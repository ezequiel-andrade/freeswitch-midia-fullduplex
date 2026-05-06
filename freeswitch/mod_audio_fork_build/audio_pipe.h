/*
 * audio_pipe.h — API pública do pipeline full-duplex
 *
 * MELHORIAS ANTERIORES (mantidas):
 *   [M1] Acumulador de fragmentos WS no receive path (rx_frag_buf)
 *   [M2] Reconexão automática com exponential backoff
 *   [M3] sq_pop_copy — cópia local antes do lws_write
 *   [M4] CNG (Comfort Noise Generation) quando ring TX está vazio
 *   [M5] Métricas de produção: underruns, drops, reconexões
 *   [M6] ap_destroy com flush WS CLOSE correto
 *   [M7] Jitter buffer adaptativo
 *
 * MELHORIAS NOVAS:
 *   [N1] Metadata inicial: frame JSON enviado logo após ESTABLISHED, antes do
 *        início do streaming de áudio. Permite que o bot identifique a sessão
 *        sem precisar codificar UUID/direction no path da URL.
 *
 *   [N2] Headers HTTP customizáveis no handshake WebSocket.
 *        Implementado via LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER +
 *        lws_add_http_header_by_name() — padrão documentado pelo maintainer
 *        Andy Green (LWS mailing list Dec/2019, issue #1488, #2948).
 *        Permite Authorization: Bearer, X-Tenant-ID, etc.
 *
 *   [N3] SpeexDSP resampler no path RX (8 kHz → 16 kHz).
 *        Substitui upsample linear manual por speex_resampler_process_int()
 *        com qualidade 4. API: speex.org/docs/manual/speex-manual/node7.html
 *        Requer: libspeexdsp-dev.
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
    AP_EVENT_RECONNECTING = 2,
} AudioPipeEvent;

typedef void (*AudioPipeEventCallback)(AudioPipe* ap, AudioPipeEvent ev, void* user_data);

/* ── Métricas de produção [M5] ─────────────────────────────────────────────── */
typedef struct {
    uint64_t tx_underruns;
    uint64_t tx_ring_drops;
    uint64_t sq_drops;
    uint64_t ws_reconnects;
    uint64_t rx_frag_bytes;
} AudioPipeStats;

/*
 * AudioPipeConfig — configuração opcional de ap_create().
 * Inicialize com zeros/NULL para comportamento padrão.
 *
 * [N1] metadata_json
 *      String JSON enviada como primeiro frame WS (LWS_WRITE_TEXT) imediatamente
 *      após ESTABLISHED, antes de qualquer frame de áudio. Exemplo:
 *        {"uuid":"abc-123","direction":"inbound","sample_rate":8000,"caller":"+55..."}
 *      NULL = sem metadata (comportamento original).
 *      O ponteiro deve permanecer válido até ESTABLISHED.
 *
 * [N2] extra_headers
 *      Headers HTTP adicionais no handshake WS Upgrade, um por linha, terminados
 *      com '\n'. Não use CRLF — o LWS adiciona os separadores internamente.
 *      Exemplo: "Authorization: Bearer eyJhbGc...\nX-Tenant-ID: acme\n"
 *      NULL = sem headers extras (comportamento original).
 *      O ponteiro deve permanecer válido até ESTABLISHED.
 *      Implementação: LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER +
 *      lws_add_http_header_by_name() conforme documentação oficial do LWS.
 *
 * [M2] max_reconnect_attempts
 *       0 = sem reconexão (padrão)
 *      >0 = N tentativas com backoff exponencial 100ms→5000ms
 *      -1 = reconexão infinita
 */
typedef struct {
    const char* metadata_json;
    const char* extra_headers;
    int         max_reconnect_attempts;
} AudioPipeConfig;

/* ── Ciclo de vida ─────────────────────────────────────────────────────────── */

/*
 * ap_create — cria e conecta o pipeline.
 * cfg pode ser NULL (usa defaults: sem metadata, sem headers extras, sem reconexão).
 */
AudioPipe* ap_create(const char*            url,
                     AudioPipeEventCallback cb,
                     void*                  user_data,
                     const AudioPipeConfig* cfg);

void ap_destroy(AudioPipe* ap);
void ap_service(AudioPipe* ap, int timeout_ms);
void ap_cancel_service(AudioPipe* ap);
void ap_set_closing(AudioPipe* ap);
/* ── Media bug callbacks — INVARIANTE: chamados APENAS do thread RTP ────────── */
void ap_on_rx_frame(AudioPipe* ap, const int16_t* pcm8, int samples8);
void ap_on_tx_frame(AudioPipe* ap, int16_t* pcm8_out, int samples8);

/* ── Estado e diagnóstico ──────────────────────────────────────────────────── */
bool           ap_is_connected(const AudioPipe* ap);
AudioPipeStats ap_get_stats(const AudioPipe* ap);
void           ap_reset_stats(AudioPipe* ap);

#ifdef __cplusplus
}
#endif