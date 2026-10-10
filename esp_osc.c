#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>

#include <esp_log.h>

#include "esp_osc.h"

#define TAG "esp-osc"

#define ESP_OSC_MAX_PACKET_SIZE 1024
#define ESP_OSC_QUEUE_LENGTH 16

typedef struct {
  size_t len;
  uint8_t data[ESP_OSC_MAX_PACKET_SIZE];
} esp_osc_packet_t;

static QueueHandle_t rx_queue = NULL;


static void esp_osc_receive_task(void *arg) {
  esp_osc_client_t *client = (esp_osc_client_t *)arg;

  for (;;) {
    // receive message
    // ESP_LOGI(TAG, "Waiting for UDP packet...");
    ssize_t ret = recvfrom(client->socket, client->rbuf, client->len, 0, NULL, NULL);
    // ESP_LOGI(TAG, "RX len=%d", (int)ret);
    if (ret < 0) {
      int err = errno;  // Save it immediately
        // ESP_LOGE(TAG, "recvfrom() failed: errno=%d (%s)", err, strerror(err));
      if (err != EAGAIN && err != EWOULDBLOCK) {
        ESP_LOGE(TAG, "recvfrom() failed: errno=%d (%s)", err, strerror(err));
      }
    } else if (ret > 0) {
      // prepare packet
      esp_osc_packet_t packet = {0};
      packet.len = ret;

      // copy packet into queue
      if (ret <= ESP_OSC_MAX_PACKET_SIZE) {
        memcpy(packet.data, client->rbuf, ret);

        // queue packet
        if (xQueueSend(rx_queue, &packet, 10) != pdTRUE) {
          ESP_LOGW(TAG, "RX queue full, dropping packet");
        }
      } else {
        ESP_LOGE(TAG, "discard too long message (%d)", ret);
      }
    }
  }
}


bool esp_osc_init(esp_osc_client_t *client, uint16_t buf_len, uint16_t port) {
  // free existing memory
  if (client->sbuf != NULL) {
    free(client->sbuf);
  }
  if (client->rbuf != NULL) {
    free(client->rbuf);
  }

  // allocate memory
  client->sbuf = malloc(buf_len);
  client->rbuf = malloc(buf_len);
  client->len = buf_len;

  // close existing socket
  if (client->socket != 0) {
    close(client->socket);
    client->socket = 0;
  }

  // create socket
  client->socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (client->socket <= 0) {
    ESP_LOGE(TAG, "failed to create socket (%d)", errno);
    client->socket = 0;
    return false;
  }
  
  int opt = 1;
  // Allow address reuse (prevents "Address in use" errors on reconnect)
  setsockopt(client->socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  // Allow broadcast reception (good practice for 192.168.1.255)
  setsockopt(client->socket, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));

  // bind socket if port is available
  if (port > 0) {
    struct sockaddr_in addr = {0};
    addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (bind(client->socket, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
      ESP_LOGE(TAG, "failed to bind socket (%d)", errno);
      return false;
    }
  }

  // create RX queue
  rx_queue = xQueueCreate(ESP_OSC_QUEUE_LENGTH, sizeof(esp_osc_packet_t));
  if (rx_queue == NULL) {
    ESP_LOGE(TAG, "failed to create RX queue");
    return false;
  }

  // create RX task
  if (xTaskCreate(esp_osc_receive_task, "esp_osc_rx", 4096, client, 5, NULL) != pdPASS) {
    ESP_LOGE(TAG, "failed to create RX task");
    return false;
  }

  return true;
}


esp_osc_target_t esp_osc_target(const char *address, uint16_t port) {
  // prepare target
  esp_osc_target_t target = {0};
  target.addr.sin_addr.s_addr = inet_addr(address);
  target.addr.sin_family = AF_INET;
  target.addr.sin_port = htons(port);

  return target;
}


bool esp_osc_send(esp_osc_client_t *client, esp_osc_target_t *target, const char *topic, const char *format, ...) {
  // send message
  va_list args;
  va_start(args, format);
  bool ret = esp_osc_send_v(client, target, topic, format, args);
  va_end(args);

  return ret;
}


bool esp_osc_send_v(esp_osc_client_t *client, esp_osc_target_t *target, const char *topic, const char *format,
                    va_list args) {
  // prepare message
  uint32_t length = tosc_vwrite((char *)client->sbuf, client->len, topic, format, args);

  // send message
  if (sendto(client->socket, client->sbuf, length, 0, (struct sockaddr *)&target->addr, sizeof(target->addr)) < 0) {
    int err = errno;  // Save it immediately
    ESP_LOGE(TAG, "sendto() failed: errno=%d (%s)", err, strerror(err));
    return false;
  }

  return true;
}


bool esp_osc_receive(esp_osc_client_t *client, esp_osc_callback_t callback) {
  // prepare values
  esp_osc_value_t values[32];

  for (;;) {
    esp_osc_packet_t packet;

    // receive message from queue
    if (xQueueReceive(rx_queue, &packet, portMAX_DELAY) != pdTRUE) {
      ESP_LOGI(TAG, "DEQUEUE error len=%u", (unsigned)packet.len);
      return false;
    } else {
      // ESP_LOGI(TAG, "DEQUEUED len=%u", (unsigned)packet.len);
    }

    // check bundle
    if (tosc_isBundle((char *)packet.data)) {
      ESP_LOGE(TAG, "discard unsupported bundle");
      continue;
    }

    // parse message
    tosc_message msg = {0};
    int res = tosc_parseMessage(&msg, (char *)packet.data, packet.len);
    if (res < 0) {
      ESP_LOGE(TAG, "failed to parse message (%d)", res);
      continue;
    }

    // get format
    char *fmt = tosc_getFormat(&msg);

    // get size
    size_t size = strlen(fmt);
    if (size > sizeof(values) / sizeof(values[0])) {
      ESP_LOGE(TAG, "message has too many values (%d)", size);
      continue;
    }

    // parse values
    for (size_t i = 0; i < size; i++) {
      switch (fmt[i]) {
        case 'i':
          values[i].i = tosc_getNextInt32(&msg);
          break;
        case 'h':
          values[i].h = tosc_getNextInt64(&msg);
          break;
        case 'f':
          values[i].f = tosc_getNextFloat(&msg);
          break;
        case 'd':
          values[i].d = tosc_getNextDouble(&msg);
          break;
        case 's':
          values[i].s = tosc_getNextString(&msg);
          break;
        case 'b':
          tosc_getNextBlob(&msg, &values[i].b, &values[i].bl);
          break;
      }
    }

    // call callback
    // ESP_LOGI(TAG, "CALLBACK address=%s", tosc_getAddress(&msg));
    if (!callback(tosc_getAddress(&msg), fmt, values)) {
      return false;
    }
  }

  return true;
}
