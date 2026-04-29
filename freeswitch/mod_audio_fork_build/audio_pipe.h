/*
 * audio_pipe.h — API pública do pipeline full-duplex
 */
#pragma once

#ifdef __cplusplus
#include <cstdint>
#else
#include <stdint.h>
#include <stdbool.h>
/* Em C não há typedef automático para struct — criamos explicitamente */
typedef struct AudioPipe AudioPipe;
#endif

#ifdef __cplusplus
extern "C" {
struct AudioPipe;
#endif

typedef enum {
    AP_EVENT_CONNECTED    = 0,
    AP_EVENT_DISCONNECTED = 1,
} AudioPipeEvent;

typedef void (*AudioPipeEventCallback)(AudioPipe* ap, AudioPipeEvent ev, void* user_data);

/* Ciclo de vida */
AudioPipe* ap_create(const char* url, AudioPipeEventCallback cb, void* user_data);
void       ap_destroy(AudioPipe* ap);
void       ap_service(AudioPipe* ap, int timeout_ms);

/* Media bug callbacks (chamados pelo FS thread) */
void ap_on_rx_frame(AudioPipe* ap, const int16_t* pcm8, int samples8);
void ap_on_tx_frame(AudioPipe* ap, int16_t* pcm8_out, int samples8);

/* Estado */
bool ap_is_connected(const AudioPipe* ap);

#ifdef __cplusplus
}
#endif