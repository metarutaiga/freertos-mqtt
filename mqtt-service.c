/*
 * Copyright (c) 2014, Stephen Robinson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 *  are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/timers.h>
#include <esp_log.h>

#include <string.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/socket.h>

#include "mqtt-service.h"

#define TAG __FILE_NAME__

typedef struct mqtt_state_t
{
  ip_addr_t address;
  uint16_t port;
  int auto_reconnect;
  mqtt_connect_info_t* connect_info;

  void(*calling_process)(mqtt_event_data_t* event_data);
  int tcp_connection;

  uint8_t* in_buffer;
  uint8_t* out_buffer;
  int in_buffer_length;
  int out_buffer_length;
  uint16_t message_length;
  uint16_t message_length_read;
  mqtt_message_t* outbound_message;
  mqtt_connection_t mqtt_connection;
  uint16_t pending_msg_id;
  int pending_msg_type;

} mqtt_state_t;

typedef struct outbound_message {
  STAILQ_ENTRY(outbound_message) next;
  int message_type;
  int message_length;
  char message_data[1];
} outbound_message_t;
STAILQ_HEAD(outbound_message_list_t, outbound_message);

void mqtt_process(void* arg);

TaskHandle_t mqtt_internal IRAM_BSS_ATTR;
TaskHandle_t mqtt_external IRAM_BSS_ATTR;
mqtt_state_t mqtt_state;
int mqtt_flags IRAM_BSS_ATTR;
struct outbound_message_list_t* mqtt_outbound_message_list IRAM_BSS_ATTR;


/*********************************************************************
*
*    Public API
*
*********************************************************************/

// Initialise the MQTT client, must be called before anything else
void mqtt_init(uint8_t* in_buffer, int in_buffer_length, uint8_t* out_buffer, int out_buffer_length)
{
  mqtt_state.in_buffer = in_buffer;
  mqtt_state.in_buffer_length = in_buffer_length;
  mqtt_state.out_buffer = out_buffer;
  mqtt_state.out_buffer_length = out_buffer_length;

  mqtt_outbound_message_list = calloc(1, sizeof(struct outbound_message_list_t));
  STAILQ_INIT(mqtt_outbound_message_list);
}

// Connect to the specified server
int mqtt_connect(ip_addr_t* address, uint16_t port, int auto_reconnect, mqtt_connect_info_t* info, void(*calling_process)(mqtt_event_data_t*))
{
  if(mqtt_internal)
    return -1;

  mqtt_state.address = *address;
  mqtt_state.port = port;
  mqtt_state.auto_reconnect = auto_reconnect;
  mqtt_state.connect_info = info;
  mqtt_state.calling_process = calling_process;

  xTaskCreate(mqtt_process, "mqtt", 3072, NULL, 5, &mqtt_internal);
  mqtt_external = NULL;

  return 0;
}

// Disconnect from the server
int mqtt_disconnect(void)
{
  if(mqtt_internal)
    return -1;

  ESP_LOGI(TAG, "mqtt: exiting...");
  mqtt_flags &= ~MQTT_FLAG_READY;
  mqtt_flags |= MQTT_FLAG_EXIT;
  mqtt_external = xTaskGetCurrentTaskHandle();
  ulTaskNotifyTake(pdTRUE, 3000 / portTICK_PERIOD_MS);
  mqtt_internal = NULL;
  mqtt_external = NULL;

  return 0;
}

// Subscribe to the specified topic
int mqtt_subscribe(const char* topic)
{
  if(!mqtt_ready())
    return -1;

  ESP_LOGD(TAG, "mqtt: sending subscribe...");
  mqtt_message_t* outbound_message = mqtt_msg_subscribe(&mqtt_state.mqtt_connection,
                                                        topic, 0,
                                                        &mqtt_state.pending_msg_id);
  outbound_message_t* outbound = calloc(1, sizeof(outbound_message_t) + outbound_message->length - 1);
  outbound->message_type = MQTT_MSG_TYPE_SUBSCRIBE;
  outbound->message_length = outbound_message->length;
  memcpy(outbound->message_data, outbound_message->data, outbound_message->length);
  STAILQ_INSERT_TAIL(mqtt_outbound_message_list, outbound, next);

  return 0;
}

int mqtt_unsubscribe(const char* topic)
{
  if(!mqtt_ready())
    return -1;

  ESP_LOGD(TAG, "sending unsubscribe");
  mqtt_message_t* outbound_message = mqtt_msg_unsubscribe(&mqtt_state.mqtt_connection, topic,
                                                          &mqtt_state.pending_msg_id);
  outbound_message_t* outbound = calloc(1, sizeof(outbound_message_t) + outbound_message->length - 1);
  outbound->message_type = MQTT_MSG_TYPE_UNSUBSCRIBE;
  outbound->message_length = outbound_message->length;
  memcpy(outbound->message_data, outbound_message->data, outbound_message->length);
  STAILQ_INSERT_TAIL(mqtt_outbound_message_list, outbound, next);

  return 0;
}

// Publish the specified message
int mqtt_publish_with_length(const char* topic, const char* data, int data_length, int qos, int retain)
{
  if(!mqtt_ready())
    return -1;

  ESP_LOGD(TAG, "mqtt: sending publish...");
  mqtt_message_t* outbound_message = mqtt_msg_publish(&mqtt_state.mqtt_connection,
                                                      topic, data, data_length,
                                                      qos, retain,
                                                      &mqtt_state.pending_msg_id);
  outbound_message_t* outbound = calloc(1, sizeof(outbound_message_t) + outbound_message->length - 1);
  outbound->message_type = MQTT_MSG_TYPE_PUBLISH;
  outbound->message_length = outbound_message->length;
  memcpy(outbound->message_data, outbound_message->data, outbound_message->length);
  STAILQ_INSERT_TAIL(mqtt_outbound_message_list, outbound, next);

  return 0;
}


/***************************************************************
*
*    Internals
*
***************************************************************/

static void complete_pending(mqtt_state_t* state, int event_type)
{
  mqtt_event_data_t event_data;

  state->pending_msg_type = 0;
  mqtt_flags |= MQTT_FLAG_READY;
  event_data.type = event_type;

  state->calling_process(&event_data);
}

static void deliver_publish(mqtt_state_t* state, uint8_t* message, int length)
{
  mqtt_event_data_t event_data;

  event_data.type = MQTT_EVENT_TYPE_PUBLISH;

  event_data.topic_length = length;
  event_data.topic = mqtt_get_publish_topic(message, &event_data.topic_length);

  event_data.data_length = length;
  event_data.data = mqtt_get_publish_data(message, &event_data.data_length);

  memmove((char*)event_data.data + 1, (char*)event_data.data, event_data.data_length);
  event_data.data += 1;
  ((char*)event_data.topic)[event_data.topic_length] = '\0';
  ((char*)event_data.data)[event_data.data_length] = '\0';

  state->calling_process(&event_data);
}

static void deliver_publish_continuation(mqtt_state_t* state, uint16_t offset, uint8_t* buffer, uint16_t length)
{
  mqtt_event_data_t event_data;

  event_data.type = MQTT_EVENT_TYPE_PUBLISH_CONTINUATION;
  event_data.topic_length = 0;
  event_data.topic = NULL;
  event_data.data_length = length;
  event_data.data_offset = offset;
  event_data.data = (char*)buffer;
  ((char*)event_data.data)[event_data.data_length] = '\0';

  state->calling_process(&event_data);
}

static void handle_mqtt_connection(mqtt_state_t* state)
{
  uint8_t msg_type;
  uint8_t msg_qos;
  uint16_t msg_id;

  // Initialise and send CONNECT message
  mqtt_msg_init(&state->mqtt_connection, state->out_buffer, state->out_buffer_length);
  state->outbound_message = mqtt_msg_connect(&state->mqtt_connection, state->connect_info);
  lwip_send(state->tcp_connection, state->outbound_message->data, state->outbound_message->length, 0);
  state->outbound_message = NULL;

  // Wait for CONACK message
  if(lwip_recv(state->tcp_connection, state->in_buffer, state->in_buffer_length, 0) < 2)
    return;
  if(mqtt_get_type(state->in_buffer) != MQTT_MSG_TYPE_CONNACK)
    return;

  // Tell the client we're connected
  mqtt_flags |= MQTT_FLAG_CONNECTED;
  complete_pending(state, MQTT_EVENT_TYPE_CONNECTED);
  while(1)
  {
    // Wait for something to happen: 
    //   new incoming data, 
    //   new outgoing data, 
    //   keep alive timer expired
    while(1)
    {
      char c;
      if(state->outbound_message != NULL)
        break;
      if(STAILQ_FIRST(mqtt_outbound_message_list))
        break;
      if(lwip_recv(state->tcp_connection, &c, sizeof(c), MSG_PEEK | MSG_DONTWAIT) != -1)
        break;
      if(errno != EAGAIN)
        return;
      vTaskDelay(100 / portTICK_PERIOD_MS);
    }

    // If there's a new message waiting to go out, then send it
    if(state->outbound_message != NULL)
    {
      lwip_send(state->tcp_connection, state->outbound_message->data, state->outbound_message->length, 0);
      state->outbound_message = NULL;

      // If it was a PUBLISH message with QoS-0 then tell the client it's done
      if(state->pending_msg_type == MQTT_MSG_TYPE_PUBLISH && state->pending_msg_id == 0)
        complete_pending(state, MQTT_EVENT_TYPE_PUBLISHED);

      continue;
    }

    if(STAILQ_FIRST(mqtt_outbound_message_list))
    {
      outbound_message_t* outbound = STAILQ_FIRST(mqtt_outbound_message_list);
      STAILQ_REMOVE_HEAD(mqtt_outbound_message_list, next);

      lwip_send(state->tcp_connection, outbound->message_data, outbound->message_length, 0);
      free(outbound);
      continue;
    }

    // If we get here we must have woken for new incoming data, 
    // read and process it.
    int16_t len = lwip_recv(state->tcp_connection, state->in_buffer, 5, 0);
    if(len < 2)
      return;

    state->message_length_read = len;
    state->message_length = mqtt_get_total_length(state->in_buffer, state->message_length_read);
    if(state->message_length != state->message_length_read)
    {
      if(state->message_length > state->in_buffer_length)
        return;
      len = lwip_recv(state->tcp_connection, state->in_buffer + len, state->message_length - len, 0);
      if(len <= 0)
        return;
      state->message_length_read += len;
    }

    msg_type = mqtt_get_type(state->in_buffer);
    msg_qos  = mqtt_get_qos(state->in_buffer);
    msg_id   = mqtt_get_id(state->in_buffer, state->in_buffer_length);
    switch(msg_type)
    {
      case MQTT_MSG_TYPE_SUBACK:
        if(state->pending_msg_type == MQTT_MSG_TYPE_SUBSCRIBE && state->pending_msg_id == msg_id)
          complete_pending(state, MQTT_EVENT_TYPE_SUBSCRIBED);
        break;
      case MQTT_MSG_TYPE_UNSUBACK:
        if(state->pending_msg_type == MQTT_MSG_TYPE_UNSUBSCRIBE && state->pending_msg_id == msg_id)
          complete_pending(state, MQTT_EVENT_TYPE_UNSUBSCRIBED);
        break;
      case MQTT_MSG_TYPE_PUBLISH:
        if(msg_qos == 1)
          state->outbound_message = mqtt_msg_puback(&state->mqtt_connection, msg_id);
        else if(msg_qos == 2)
          state->outbound_message = mqtt_msg_pubrec(&state->mqtt_connection, msg_id);

        deliver_publish(state, state->in_buffer, state->message_length_read);
        break;
      case MQTT_MSG_TYPE_PUBACK:
        if(state->pending_msg_type == MQTT_MSG_TYPE_PUBLISH && state->pending_msg_id == msg_id)
          complete_pending(state, MQTT_EVENT_TYPE_PUBLISHED);
        break;
      case MQTT_MSG_TYPE_PUBREC:
        state->outbound_message = mqtt_msg_pubrel(&state->mqtt_connection, msg_id);
        break;
      case MQTT_MSG_TYPE_PUBREL:
        state->outbound_message = mqtt_msg_pubcomp(&state->mqtt_connection, msg_id);
        break;
      case MQTT_MSG_TYPE_PUBCOMP:
        if(state->pending_msg_type == MQTT_MSG_TYPE_PUBLISH && state->pending_msg_id == msg_id)
          complete_pending(state, MQTT_EVENT_TYPE_PUBLISHED);
        break;
      case MQTT_MSG_TYPE_PINGREQ:
        state->outbound_message = mqtt_msg_pingresp(&state->mqtt_connection);
        break;
      case MQTT_MSG_TYPE_PINGRESP:
        // Ignore
        break;
    }

    // NOTE: this is done down here and not in the switch case above
    //       because the PSOCK_READBUF_LEN() won't work inside a switch
    //       statement due to the way protothreads resume.
    if(msg_type == MQTT_MSG_TYPE_PUBLISH)
    {
      uint16_t len;

      // adjust message_length and message_length_read so that
      // they only account for the publish data and not the rest of the 
      // message, this is done so that the offset passed with the
      // continuation event is the offset within the publish data and
      // not the offset within the message as a whole.
      len = state->message_length_read;
      mqtt_get_publish_data(state->in_buffer, &len);
      len = state->message_length_read - len;
      state->message_length -= len;
      state->message_length_read -= len;

      while(state->message_length_read < state->message_length)
      {
        ssize_t len = lwip_recv(state->tcp_connection, state->in_buffer, state->message_length - state->message_length_read, 0);
        if(len <= 0)
          return;
        deliver_publish_continuation(state, state->message_length_read, state->in_buffer, len);
        state->message_length_read += len;
      }
    }
  }
}

void mqtt_process(void* arg)
{
  mqtt_event_data_t event_data;

  while(1)
  {
    ESP_LOGI(TAG, "mqtt: connecting...");
    mqtt_state.tcp_connection = lwip_socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if(mqtt_state.tcp_connection >= 0)
    {
      struct sockaddr_in sockaddr = {};
      sockaddr.sin_len = sizeof(sockaddr);
      sockaddr.sin_family = AF_INET;
      sockaddr.sin_port = htons(mqtt_state.port);
      sockaddr.sin_addr.s_addr = mqtt_state.address.u_addr.ip4.addr;
      if(lwip_connect(mqtt_state.tcp_connection, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0)
      {
        lwip_close(mqtt_state.tcp_connection);
        mqtt_state.tcp_connection = -1;
      }
    }

    if(mqtt_state.tcp_connection < 0)
    {
      ESP_LOGE(TAG, "mqtt: connect failed");
    }
    else
    {
      ESP_LOGI(TAG, "mqtt: connected");

      handle_mqtt_connection(&mqtt_state);
      lwip_close(mqtt_state.tcp_connection);
      mqtt_state.tcp_connection = -1;

      if(mqtt_flags & MQTT_FLAG_EXIT)
      {
        event_data.type = MQTT_EVENT_TYPE_EXITED;
        mqtt_state.calling_process(&event_data);
        xTaskNotifyGive(mqtt_external);
        vTaskDelete(NULL);
      }

      event_data.type = MQTT_EVENT_TYPE_DISCONNECTED;
      mqtt_state.calling_process(&event_data);
      ESP_LOGE(TAG, "mqtt: lost connection: %s", "closed");
    }
    if(!mqtt_state.auto_reconnect)
      break;
    vTaskDelay(1000 / portTICK_PERIOD_MS);
  }

  event_data.type = MQTT_EVENT_TYPE_EXITED;
  mqtt_state.calling_process(&event_data);
  vTaskDelete(NULL);
}
