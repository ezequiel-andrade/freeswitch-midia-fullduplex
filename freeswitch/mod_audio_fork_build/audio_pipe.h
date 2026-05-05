/*
 * audio_pipe.h — API pública do pipeline full-duplex
 *
 * MELHORIAS APLICADAS:
 *   [M1] Acumulador de fragmentos WS no receive path (rx_frag_buf)
 *   [M2] Reconexão automática com exponential backoff
 *   [M3] sq_pop com cópia local antes do lws_write (elimina exposição de ponteiro interno)
 *   [M4] CNG (Comfort Noise Generation) quando ring TX está vazio
 *   [M5] Métricas de produção: underruns, drops, reconexões
 *   [M6] ap_destroy com flush WS CLOSE correto (loop até wsi=nullptr)
 *   [M7] Jitter buffer adaptativo (ajusta TX_JITTER_FRAMES por underrun count)
 */
#pragma once

#ifdef __cplusplus
#include <cstdint>
#else
#include <stdint.h>
#include <stdbool.h>
typedef struct AudioPipe AudioPipe;
#endif

#ifdef __cplusplus
extern "C" {
struct AudioPipe;
#endif

typedef enum {
    AP_EVENT_CONNECTED    = 0,
    AP_EVENT_DISCONNECTED = 1,
    AP_EVENT_RECONNECTING = 2,   /* [M2] emitido antes de cada tentativa de reconexão */
} AudioPipeEvent;

typedef void (*AudioPipeEventCallback)(AudioPipe* ap, AudioPipeEvent ev, void* user_data);

/* ── Métricas de produção [M5] ─────────────────────────────────────────────── */
typedef struct {
    uint64_t tx_underruns;     /* pop() sem dados disponíveis → silêncio/CNG emitido */
    uint64_t tx_ring_drops;    /* push() descartado por ring cheio                   */
    uint64_t sq_drops;         /* sq_push() descartado por send queue cheia (backpressure RX) */
    uint64_t ws_reconnects;    /* número de reconexões automáticas realizadas         */
    uint64_t rx_frag_bytes;    /* bytes acumulados no fragmento WS (diagnóstico)      */
} AudioPipeStats;

/* ── Ciclo de vida ─────────────────────────────────────────────────────────── */

/*
 * ap_create — cria e conecta o pipeline.
 *
 * @url       URL WebSocket: "ws://host:port/path" ou "wss://host:port/path"
 * @cb        callback de evento (AP_EVENT_CONNECTED, DISCONNECTED, RECONNECTING)
 * @user_data ponteiro opaco passado ao callback
 * @max_reconnect_attempts  0 = sem reconexão (comportamento original);
 *                          >0 = número máximo de tentativas antes de desistir
 *                               e emitir AP_EVENT_DISCONNECTED permanente.
 *                          Backoff: 100ms → 200ms → 400ms → ... → 5000ms (cap).
 */
AudioPipe* ap_create(const char* url,
                     AudioPipeEventCallback cb,
                     void* user_data,
                     int max_reconnect_attempts);

void ap_destroy(AudioPipe* ap);
void ap_service(AudioPipe* ap, int timeout_ms);

/* ── Media bug callbacks (chamados pelo FS thread) ─────────────────────────── */
/* Invariante: ap_on_rx_frame e ap_on_tx_frame são SEMPRE chamados do mesmo
 * thread RTP — nunca de outro contexto. Documentado para prevenir refatorações
 * incorretas. */
void ap_on_rx_frame(AudioPipe* ap, const int16_t* pcm8, int samples8);
void ap_on_tx_frame(AudioPipe* ap, int16_t* pcm8_out, int samples8);

/* ── Estado e diagnóstico ──────────────────────────────────────────────────── */
bool            ap_is_connected(const AudioPipe* ap);
AudioPipeStats  ap_get_stats(const AudioPipe* ap);    /* [M5] snapshot atômico */
void            ap_reset_stats(AudioPipe* ap);         /* [M5] zera contadores  */

#ifdef __cplusplus
}
#endif
