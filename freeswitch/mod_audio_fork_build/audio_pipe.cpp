/*
 * audio_pipe.cpp — Full-Duplex WebSocket Audio Pipeline
 * ======================================================
 *
 * FIXES ORIGINAIS (mantidos):
 *   [FIX #2] TxRingBuffer::push() — tail avança com módulo (era sem módulo → OOB).
 *   [FIX #4] sq_push() — lws_cancel_service() + WAIT_CANCELLED em vez de
 *            lws_callback_on_writable() cross-thread (undefined behavior no LWS).
 *   [FIX #5] tx_ready e ring resetados em CLOSED/CONNECTION_ERROR.
 *   [ARCH]   TX path @ 8 kHz nativo (Cartesia). RX path upsample linear 8→16 kHz.
 *
 * MELHORIAS NOVAS:
 *   [M1] Acumulador de fragmentos WS no receive path.
 *        O protocolo WS não garante alinhamento a FS_FRAME_BYTES. Frames TCP
 *        segmentados (ex: Nagle desabilitado, MTU 1400) chegam como chunks
 *        menores. O acumulador rx_frag_buf junta fragmentos até completar
 *        FS_FRAME_BYTES antes de fazer push no ring, eliminando dropout
 *        periódico que antes era descartado silenciosamente.
 *
 *   [M2] Reconexão automática com exponential backoff.
 *        Ao detectar CLOSED/CONNECTION_ERROR, agenda reconexão com delay
 *        inicial de 100ms, dobrado a cada tentativa, cap de 5000ms.
 *        Configurável via max_reconnect_attempts em ap_create().
 *        Durante reconexão: TX emite silêncio/CNG, RX é descartado.
 *        AP_EVENT_RECONNECTING emitido antes de cada tentativa.
 *
 *   [M3] sq_pop com cópia local antes do lws_write.
 *        Antes: sq_pop() avançava o tail e expunha ponteiro para interior do
 *        slot (que poderia ser reutilizado se lws_write() falhasse parcialmente).
 *        Agora: sq_pop_copy() copia o payload para buffer local antes de avançar
 *        o tail. lws_write() opera sobre o buffer local — sem exposição de ponteiro.
 *
 *   [M4] CNG (Comfort Noise Generator) no TX path.
 *        Zeros puros (silêncio digital) em PCMU viram 0xFF repetido — alguns
 *        endpoints interpretam como erro de codec. CNG gera ruído branco de baixa
 *        amplitude (~-60 dBFS) usando LCG de 32 bits (custo: 1 multiply/add por
 *        amostra, sem dependência de stdlib, sem alocação). Aplicado apenas quando
 *        tx_ready=false ou ring vazio.
 *
 *   [M5] Métricas de produção via AudioPipeStats.
 *        Contadores atômicos: tx_underruns, tx_ring_drops, sq_drops,
 *        ws_reconnects, rx_frag_bytes. Acessíveis via ap_get_stats() sem lock.
 *
 *   [M6] ap_destroy com flush WS CLOSE correto.
 *        Antes: único lws_service(ctx, 0) após set_timeout — pode não completar
 *        o handshake WS CLOSE. Agora: loop de até 5 iterações × 5ms até wsi=nullptr
 *        (indicador de CLOSE completo). Bot recebe WS close frame limpo.
 *
 *   [M7] Jitter buffer adaptativo.
 *        TX_JITTER_FRAMES começa em 1 (20ms). Se underruns acumulam (>3 em janela
 *        de 1s = 50 frames), incrementa para 2 (40ms). Se estabiliza por 10s sem
 *        underruns, volta para 1. Balanço entre latência e qualidade de áudio.
 *
 * Dependências: libwebsockets ≥ 4.x, C++14
 */

#include "audio_pipe.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <libwebsockets.h>
#include <string>

/* ─────────────────────────────────────────────────────────────────────────────
 * Constantes de áudio
 * ──────────────────────────────────────────────────────────────────────────── */
static constexpr int FS_SAMPLE_RATE    = 8000;
static constexpr int BOT_SAMPLE_RATE   = 16000;
static constexpr int RESAMPLE_RATIO    = BOT_SAMPLE_RATE / FS_SAMPLE_RATE;  // 2

static constexpr int FRAME_MS          = 20;
static constexpr int FS_FRAME_SAMPLES  = FS_SAMPLE_RATE  * FRAME_MS / 1000;  // 160
static constexpr int FS_FRAME_BYTES    = FS_FRAME_SAMPLES * 2;               // 320

static constexpr int BOT_FRAME_SAMPLES = BOT_SAMPLE_RATE * FRAME_MS / 1000;  // 320
static constexpr int BOT_FRAME_BYTES   = BOT_FRAME_SAMPLES * 2;              // 640

/* Ring buffer TX: 400ms de headroom para burst do bot */
static constexpr int TX_RING_FRAMES    = 20;
static constexpr int TX_RING_SAMPLES   = TX_RING_FRAMES * FS_FRAME_SAMPLES;  // 3200 int16

/* [M7] Jitter buffer adaptativo: limites e parâmetros de janela */
static constexpr int TX_JITTER_MIN     = 1;   /* 20ms — latência mínima */
static constexpr int TX_JITTER_MAX     = 4;   /* 80ms — cap conservador */
/* Janela de 1s = 50 frames @ 20ms. Se >3 underruns nessa janela → aumenta jitter. */
static constexpr int JITTER_ADAPT_WINDOW  = 50;   /* frames */
static constexpr int JITTER_UNDERRUN_THR  = 3;    /* underruns por janela para aumentar */
static constexpr int JITTER_STABLE_WINDOW = 500;  /* frames sem underrun para diminuir */

/* LWS padding */
static constexpr int AP_LWS_PRE  = LWS_PRE;

/* [M2] Reconexão: backoff inicial 100ms, fator 2, cap 5000ms */
static constexpr int RECONNECT_BASE_MS   = 100;
static constexpr int RECONNECT_MAX_MS    = 5000;

/* [M4] CNG: amplitude ~-60 dBFS ≈ 1/1000 de full-scale (32767) ≈ 33 */
static constexpr int16_t CNG_AMPLITUDE   = 33;

/* [M6] ap_destroy: máximo de iterações de flush WS CLOSE */
static constexpr int DESTROY_FLUSH_ITERS = 5;
static constexpr int DESTROY_FLUSH_MS    = 5;

/* ─────────────────────────────────────────────────────────────────────────────
 * [M4] CNG — Comfort Noise Generator
 *
 * Ruído branco de baixa amplitude usando LCG de 32 bits.
 * Sem stdlib, sem alocação, sem dependência de estado global.
 * Custo: 1 multiply + 1 add por amostra.
 * Amplitude: CNG_AMPLITUDE (~-60 dBFS) — inaudível mas não silêncio digital.
 * ──────────────────────────────────────────────────────────────────────────── */
static uint32_t cng_state = 0x12345678u;

static inline int16_t cng_next_sample() {
    cng_state = cng_state * 1664525u + 1013904223u;  // Knuth LCG
    /* Usa os bits [16:31] que têm período completo */
    int32_t s = static_cast<int32_t>(cng_state >> 16) - 32768;
    /* Escala para CNG_AMPLITUDE: s ∈ [-32768, 32767] → [-CNG_AMPLITUDE, CNG_AMPLITUDE] */
    return static_cast<int16_t>((s * CNG_AMPLITUDE) >> 15);
}

static void fill_cng(int16_t* dst, int n) {
    for (int i = 0; i < n; ++i)
        dst[i] = cng_next_sample();
}

/* ─────────────────────────────────────────────────────────────────────────────
 * Resample — RX path: 8 kHz → 16 kHz (interpolação linear)
 * ──────────────────────────────────────────────────────────────────────────── */
static void upsample_8_to_16(const int16_t* src, int src_n, int16_t* dst) {
    for (int i = 0; i < src_n - 1; ++i) {
        dst[i * 2]     = src[i];
        dst[i * 2 + 1] = static_cast<int16_t>(
            (static_cast<int32_t>(src[i]) + static_cast<int32_t>(src[i + 1])) / 2);
    }
    dst[(src_n - 1) * 2]     = src[src_n - 1];
    dst[(src_n - 1) * 2 + 1] = src[src_n - 1];
}

/* ─────────────────────────────────────────────────────────────────────────────
 * TxRingBuffer — SPSC lock-free
 *
 * Produtor: LWS thread (LWS_CALLBACK_CLIENT_RECEIVE)
 * Consumidor: RTP thread (bug_callback WRITE_REPLACE)
 *
 * [FIX #2] tail avança com módulo.
 * [M5]     drop counter integrado.
 * Política de overflow: drop-newest (apenas push() modifica head; pop() modifica
 * tail) — elimina race condition da política drop-oldest anterior.
 * ──────────────────────────────────────────────────────────────────────────── */
struct TxRingBuffer {
    int16_t          buf[TX_RING_SAMPLES];
    std::atomic<int> head{0};
    std::atomic<int> tail{0};
    std::atomic<int> count{0};

    /* [M5] contador de drops — modificado apenas pelo produtor (LWS thread) */
    std::atomic<uint64_t> drops{0};

    TxRingBuffer() { std::memset(buf, 0, sizeof(buf)); }

    /* Produz n amostras. Drop-newest se cheio — não modifica tail. */
    void push(const int16_t* src, int n) {
        for (int i = 0; i < n; ++i) {
            if (count.load(std::memory_order_acquire) >= TX_RING_SAMPLES) {
                drops.fetch_add(n - i, std::memory_order_relaxed);
                return;
            }
            int h = head.load(std::memory_order_relaxed);
            buf[h] = src[i];
            head.store((h + 1) % TX_RING_SAMPLES, std::memory_order_release);
            count.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    /* Consome exatamente n amostras. Preenche CNG [M4] se insuficiente. */
    int pop(int16_t* dst, int n) {
        int avail = count.load(std::memory_order_acquire);
        int read  = std::min(avail, n);
        int t     = tail.load(std::memory_order_relaxed);

        for (int i = 0; i < read; ++i) {
            dst[i] = buf[t];
            t = (t + 1) % TX_RING_SAMPLES;
        }
        tail.store(t, std::memory_order_release);
        count.fetch_sub(read, std::memory_order_acq_rel);

        int underrun_samples = n - read;
        if (underrun_samples > 0)
            fill_cng(dst + read, underrun_samples);  /* [M4] CNG em vez de zeros */

        return underrun_samples;  /* retorna 0 se sem underrun */
    }

    void reset() {
        head.store(0, std::memory_order_release);
        tail.store(0, std::memory_order_release);
        count.store(0, std::memory_order_release);
        std::memset(buf, 0, sizeof(buf));
    }

    int available() const { return count.load(std::memory_order_acquire); }
};

/* ─────────────────────────────────────────────────────────────────────────────
 * AudioPipe — estado por sessão
 * ──────────────────────────────────────────────────────────────────────────── */
struct AudioPipe {
    /* ── WebSocket ─────────────────────────────────────────────────────────── */
    lws_context*      lws_ctx   = nullptr;
    lws*              wsi       = nullptr;
    std::string       url;
    std::atomic<bool> connected{false};
    bool              closing   = false;

    /* ── [M2] Reconexão com exponential backoff ─────────────────────────────
     * Ciclo de vida da reconexão (no LWS thread):
     *   1. CLOSED/ERROR → reconnect_pending=true, reconnect_at_ms=now+backoff
     *   2. ap_service() → lws_service() → LWS_CALLBACK_EVENT_WAIT_CANCELLED
     *      (ou qualquer callback que verifica reconnect_pending + timer)
     *   3. Timer expirado → lws_client_connect_via_info() → novo wsi
     *   4. ESTABLISHED → reconnect_attempts=0, backoff resetado
     * ──────────────────────────────────────────────────────────────────────── */
    bool          reconnect_pending   = false;
    int           reconnect_attempts  = 0;
    int           max_reconnect_attempts = 0;  /* 0 = sem reconexão */
    uint64_t      reconnect_at_ms     = 0;     /* timestamp alvo em ms */
    int           reconnect_backoff_ms = RECONNECT_BASE_MS;

    /* ── TX (bot → FS RTP) — 8 kHz nativo ──────────────────────────────────── */
    TxRingBuffer  tx_ring;
    bool          tx_ready         = false;

    /* [M7] Jitter buffer adaptativo */
    int           tx_jitter_frames = TX_JITTER_MIN;
    int           jitter_frame_counter = 0;   /* frames desde início da janela atual */
    int           jitter_underrun_count = 0;  /* underruns na janela atual */
    int           jitter_stable_counter = 0;  /* frames sem underrun consecutivos */

    /* ── [M1] Acumulador de fragmentos WS no receive path ───────────────────
     * Fragmentos WS menores que FS_FRAME_BYTES são acumulados aqui até
     * completar um frame completo. Elimina dropout por desalinhamento TCP.
     * ──────────────────────────────────────────────────────────────────────── */
    uint8_t rx_frag_buf[FS_FRAME_BYTES * 4];  /* headroom para 4 frames de burst */
    int     rx_frag_fill = 0;

    /* ── RX accumulator (FS → bot) — 8 kHz → 16 kHz ────────────────────────
     * INVARIANTE: acessado APENAS pelo thread RTP (ap_on_rx_frame).
     *             Documentado explicitamente — refatorações que chamem
     *             ap_on_rx_frame de outro contexto quebram este invariante.
     * ──────────────────────────────────────────────────────────────────────── */
    int16_t rx_acc[FS_FRAME_SAMPLES];
    int     rx_acc_fill = 0;

    /* ── Send queue SPSC — RX path: RTP thread → LWS thread ────────────────
     * [M3] sq_pop_copy(): copia payload para buffer local ANTES de avançar tail.
     *      Elimina exposição de ponteiro para slot que pode ser reutilizado.
     * [FIX #4] sq_push() usa lws_cancel_service() para wake-up thread-safe.
     * ──────────────────────────────────────────────────────────────────────── */
    struct TxMsg {
        uint8_t data[AP_LWS_PRE + BOT_FRAME_BYTES];
        int     len;
    };
    static constexpr int SEND_QUEUE_SIZE = 16;  /* 16 × 20ms = 320ms buffer RX */
    TxMsg            send_queue[SEND_QUEUE_SIZE];
    std::atomic<int> sq_head{0};
    std::atomic<int> sq_tail{0};
    std::atomic<int> sq_count{0};

    /* [M5] Métricas — atômicas para leitura sem lock de ap_get_stats() */
    std::atomic<uint64_t> stat_tx_underruns{0};
    std::atomic<uint64_t> stat_sq_drops{0};
    std::atomic<uint64_t> stat_ws_reconnects{0};
    std::atomic<uint64_t> stat_rx_frag_bytes{0};

    /* ── Callbacks ──────────────────────────────────────────────────────────── */
    AudioPipeEventCallback event_cb  = nullptr;
    void*                  user_data = nullptr;

    /* ── sq_push: RTP thread → enfileira frame RX para envio WS ──────────── */
    bool sq_push(const uint8_t* payload, int len) {
        if (sq_count.load(std::memory_order_acquire) >= SEND_QUEUE_SIZE) {
            stat_sq_drops.fetch_add(1, std::memory_order_relaxed);  /* [M5] */
            return false;
        }
        int h = sq_head.load(std::memory_order_relaxed);
        TxMsg& m = send_queue[h];
        std::memcpy(m.data + AP_LWS_PRE, payload, len);
        m.len = len;
        sq_head.store((h + 1) % SEND_QUEUE_SIZE, std::memory_order_release);
        sq_count.fetch_add(1, std::memory_order_acq_rel);

        /* [FIX #4] Wake-up thread-safe */
        if (lws_ctx) lws_cancel_service(lws_ctx);
        return true;
    }

    /* ── [M3] sq_pop_copy: LWS thread — copia para buf local antes de avançar tail
     *
     * Antes (sq_pop): retornava ponteiro para m.data ANTES de avançar tail.
     *   Se lws_write() falhasse e sq_pop() fosse chamado novamente, o slot
     *   poderia ser reutilizado pelo produtor enquanto lws_write() ainda usava
     *   o ponteiro. Race condition sutil em condições de erro de rede.
     *
     * Agora (sq_pop_copy): copia payload para buf[LWS_PRE...] fornecido pelo
     *   chamador, avança tail APÓS a cópia. Chamador chama lws_write() sobre
     *   seu próprio buffer — sem exposição de ponteiro interno.
     *
     * @buf      buffer do chamador com pelo menos AP_LWS_PRE + BOT_FRAME_BYTES
     * @out_len  tamanho do payload (excluindo AP_LWS_PRE)
     * @return   true se havia item na fila
     * ──────────────────────────────────────────────────────────────────────── */
    bool sq_pop_copy(uint8_t* buf, int* out_len) {
        if (sq_count.load(std::memory_order_acquire) == 0) return false;
        int t = sq_tail.load(std::memory_order_relaxed);
        TxMsg& m = send_queue[t];
        /* Copia payload (sem LWS_PRE) para posição LWS_PRE do buffer do chamador */
        std::memcpy(buf + AP_LWS_PRE, m.data + AP_LWS_PRE, m.len);
        *out_len = m.len;
        /* Tail avança APÓS a cópia — slot seguro para reuso pelo produtor */
        sq_tail.store((t + 1) % SEND_QUEUE_SIZE, std::memory_order_release);
        sq_count.fetch_sub(1, std::memory_order_acq_rel);
        return true;
    }

    /* ── [M2] Helpers de reconexão ──────────────────────────────────────────── */

    /* Retorna timestamp em ms desde epoch (monotônico). */
    static uint64_t now_ms() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000u +
               static_cast<uint64_t>(ts.tv_nsec) / 1000000u;
    }

    /* Agenda reconexão com backoff atual e dobra para a próxima tentativa. */
    void schedule_reconnect() {
        reconnect_at_ms    = now_ms() + static_cast<uint64_t>(reconnect_backoff_ms);
        reconnect_pending  = true;
        reconnect_backoff_ms = std::min(reconnect_backoff_ms * 2, RECONNECT_MAX_MS);
    }

    /* Reseta estado de reconexão após ESTABLISHED bem-sucedido. */
    void reset_reconnect() {
        reconnect_pending    = false;
        reconnect_attempts   = 0;
        reconnect_backoff_ms = RECONNECT_BASE_MS;
        reconnect_at_ms      = 0;
    }

    /* [M7] Atualiza jitter buffer adaptativo. Chamado por ap_on_tx_frame()
     * no thread RTP a cada pop(). underrun=true se pop() teve underrun.
     *
     * Lógica:
     *   - A cada JITTER_ADAPT_WINDOW frames, avalia underrun_count.
     *   - Se underrun_count > JITTER_UNDERRUN_THR → tx_jitter_frames++ (até MAX).
     *   - Se jitter_stable_counter > JITTER_STABLE_WINDOW → tx_jitter_frames-- (até MIN).
     *   - tx_ready é reavaliado com o novo threshold.
     */
    void update_jitter(bool underrun) {
        jitter_frame_counter++;
        if (underrun) {
            jitter_underrun_count++;
            jitter_stable_counter = 0;
        } else {
            jitter_stable_counter++;
        }

        /* Avalia janela */
        if (jitter_frame_counter >= JITTER_ADAPT_WINDOW) {
            if (jitter_underrun_count > JITTER_UNDERRUN_THR &&
                tx_jitter_frames < TX_JITTER_MAX) {
                tx_jitter_frames++;
                /* Reseta tx_ready para forçar reenchimento com novo threshold */
                tx_ready = false;
            }
            jitter_underrun_count = 0;
            jitter_frame_counter  = 0;
        }

        /* Janela de estabilidade */
        if (jitter_stable_counter >= JITTER_STABLE_WINDOW &&
            tx_jitter_frames > TX_JITTER_MIN) {
            tx_jitter_frames--;
            jitter_stable_counter = 0;
        }
    }
};

/* ─────────────────────────────────────────────────────────────────────────────
 * [M2] Tenta reconectar: cria novo wsi no lws_ctx existente.
 * Chamado pelo LWS thread dentro de check_reconnect() — seguro.
 * ──────────────────────────────────────────────────────────────────────────── */
static bool do_reconnect(AudioPipe* ap) {
    if (!ap || !ap->lws_ctx) return false;

    char host[128] = {}, path[256] = {};
    int  port   = 9998;
    bool is_ssl = (ap->url.substr(0, 3) == "wss");
    const char* u     = ap->url.c_str() + (is_ssl ? 6 : 5);
    const char* slash = strchr(u, '/');
    const char* colon = strchr(u, ':');

    if (colon && (!slash || colon < slash)) {
        int hlen = static_cast<int>(colon - u);
        strncpy(host, u, std::min(hlen, 127));
        port = atoi(colon + 1);
    } else {
        int hlen = slash ? static_cast<int>(slash - u) : static_cast<int>(strlen(u));
        strncpy(host, u, std::min(hlen, 127));
    }
    strncpy(path, slash ? slash : "/", 255);

    static const lws_protocols proto_arr[] = {
        { "audio.bot", nullptr, 0, 0 },  /* callback setado no contexto */
        { nullptr, nullptr, 0, 0 }
    };

    lws_client_connect_info ci{};
    ci.context        = ap->lws_ctx;
    ci.address        = host;
    ci.port           = port;
    ci.path           = path;
    ci.host           = host;
    ci.origin         = host;
    ci.protocol       = "audio.bot";
    ci.ssl_connection = is_ssl ? LCCSCF_USE_SSL : 0;
    ci.userdata       = ap;

    ap->wsi = lws_client_connect_via_info(&ci);
    return ap->wsi != nullptr;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * [M2] check_reconnect — chamado em callbacks LWS para verificar timer.
 * Seguro: executado dentro do LWS event loop.
 * ──────────────────────────────────────────────────────────────────────────── */
static void check_reconnect(AudioPipe* ap) {
    if (!ap || !ap->reconnect_pending || ap->closing) return;
    if (ap->wsi) return;  /* ainda conectado ou conectando */

    uint64_t now = AudioPipe::now_ms();
    if (now < ap->reconnect_at_ms) return;  /* backoff ainda ativo */

    /* Verifica limite de tentativas */
    if (ap->max_reconnect_attempts > 0 &&
        ap->reconnect_attempts >= ap->max_reconnect_attempts) {
        ap->reconnect_pending = false;
        /* Desistiu — emite DISCONNECTED final */
        if (ap->event_cb)
            ap->event_cb(ap, AP_EVENT_DISCONNECTED, ap->user_data);
        return;
    }

    ap->reconnect_attempts++;
    ap->stat_ws_reconnects.fetch_add(1, std::memory_order_relaxed);  /* [M5] */

    if (ap->event_cb)
        ap->event_cb(ap, AP_EVENT_RECONNECTING, ap->user_data);

    if (!do_reconnect(ap)) {
        /* Falha na tentativa — agenda próxima com backoff */
        ap->schedule_reconnect();
    }
    /* Se do_reconnect ok, wsi≠nullptr. Aguarda ESTABLISHED ou CONNECTION_ERROR. */
}

/* ─────────────────────────────────────────────────────────────────────────────
 * LWS callback
 * ──────────────────────────────────────────────────────────────────────────── */
static int lws_callback(struct lws* wsi, enum lws_callback_reasons reason,
                        void* user, void* in, size_t len)
{
    AudioPipe* ap = reinterpret_cast<AudioPipe*>(lws_wsi_user(wsi));

    switch (reason) {

    /* ── Conexão estabelecida ─────────────────────────────────────────────── */
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        ap->connected.store(true, std::memory_order_release);
        ap->reset_reconnect();  /* [M2] */
        if (ap->event_cb) ap->event_cb(ap, AP_EVENT_CONNECTED, ap->user_data);
        lws_callback_on_writable(wsi);  /* seguro: dentro do LWS callback */
        break;

    /* ── Desconexão / erro ────────────────────────────────────────────────── */
    case LWS_CALLBACK_CLIENT_CLOSED:
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        if (ap) {
            ap->connected.store(false, std::memory_order_release);
            ap->wsi      = nullptr;

            /* [FIX #5] Reseta ring e tx_ready */
            ap->tx_ready = false;
            ap->tx_ring.reset();
            ap->rx_frag_fill = 0;  /* [M1] descarta fragmento parcial acumulado */

            /* [M2] Agenda reconexão se configurado e não estamos encerrando */
            if (!ap->closing && ap->max_reconnect_attempts != 0) {
                ap->schedule_reconnect();
                /* Não emite DISCONNECTED ainda — será emitido se esgotar tentativas */
            } else {
                /* Sem reconexão configurada: comportamento original */
                if (ap->event_cb)
                    ap->event_cb(ap, AP_EVENT_DISCONNECTED, ap->user_data);
            }
        }
        break;

    /* ── [FIX #4] Wake-up pelo thread RTP via lws_cancel_service() ──────── */
    case LWS_CALLBACK_EVENT_WAIT_CANCELLED: {
        AudioPipe* cap = reinterpret_cast<AudioPipe*>(
            lws_context_user(lws_get_context(wsi)));
        if (!cap) break;

        /* [M2] Aproveita o wake-up para verificar timer de reconexão */
        check_reconnect(cap);

        if (cap->wsi && !cap->closing &&
            cap->sq_count.load(std::memory_order_acquire) > 0) {
            lws_callback_on_writable(cap->wsi);
        }
        break;
    }

    /* ── Envio de frame RX (FS → bot / Deepgram STT) ────────────────────── */
    case LWS_CALLBACK_CLIENT_WRITEABLE: {
        if (!ap || ap->closing) break;

        /*
         * [M3] Buffer local com LWS_PRE + payload.
         *      sq_pop_copy() copia antes de avançar tail — sem exposição de
         *      ponteiro para slot interno da send queue.
         */
        uint8_t local_buf[AP_LWS_PRE + BOT_FRAME_BYTES];
        int     plen = 0;

        if (!ap->sq_pop_copy(local_buf, &plen)) break;

        int rc = lws_write(wsi,
                           local_buf + AP_LWS_PRE,
                           static_cast<size_t>(plen),
                           LWS_WRITE_BINARY);

        if (rc < 0) {
            /* Erro de escrita — LWS fecha a conexão; CLOSED será disparado. */
            break;
        }

        /* Reagenda se há mais frames na fila */
        if (ap->sq_count.load(std::memory_order_acquire) > 0)
            lws_callback_on_writable(wsi);
        break;
    }

    /* ── Recebimento de áudio TX (bot → FS RTP) ──────────────────────────── */
    case LWS_CALLBACK_CLIENT_RECEIVE: {
        /*
         * TX path: recebe PCM binário @ 8 kHz do bot (Cartesia).
         *
         * [M1] Acumulador de fragmentos WS.
         *      O WS não garante que cada mensagem tem exatamente FS_FRAME_BYTES.
         *      TCP pode entregar em pedaços menores (segmentação, Nagle, etc.).
         *      Acumulamos em rx_frag_buf até ter FS_FRAME_BYTES, depois push.
         *      Fragmento residual é mantido para a próxima mensagem.
         */
        if (!ap || !in || len == 0) break;

        const uint8_t* p   = reinterpret_cast<const uint8_t*>(in);
        int            rem = static_cast<int>(len);

        ap->stat_rx_frag_bytes.fetch_add(len, std::memory_order_relaxed);  /* [M5] */

        /* Drena: junta fragmento pendente + dados novos, processa frames completos */
        while (rem > 0) {
            /* Copia o máximo possível para rx_frag_buf */
            int space = static_cast<int>(sizeof(ap->rx_frag_buf)) - ap->rx_frag_fill;
            int copy  = std::min(rem, space);
            if (copy <= 0) {
                /* rx_frag_buf cheio sem completar frame — situação anômala:
                 * descarta acumulado e recomeça com dados atuais */
                ap->rx_frag_fill = 0;
                continue;
            }
            std::memcpy(ap->rx_frag_buf + ap->rx_frag_fill, p, copy);
            ap->rx_frag_fill += copy;
            p   += copy;
            rem -= copy;

            /* Processa todos os frames completos acumulados */
            while (ap->rx_frag_fill >= FS_FRAME_BYTES) {
                const int16_t* src8 = reinterpret_cast<const int16_t*>(ap->rx_frag_buf);
                ap->tx_ring.push(src8, FS_FRAME_SAMPLES);

                /* Desloca fragmento residual para o início */
                int residual = ap->rx_frag_fill - FS_FRAME_BYTES;
                if (residual > 0)
                    std::memmove(ap->rx_frag_buf,
                                 ap->rx_frag_buf + FS_FRAME_BYTES,
                                 residual);
                ap->rx_frag_fill = residual;
            }
        }

        /* [M7] Jitter buffer adaptativo: considera TX pronto ao atingir threshold */
        if (!ap->tx_ready &&
            ap->tx_ring.available() >= ap->tx_jitter_frames * FS_FRAME_SAMPLES) {
            ap->tx_ready = true;
        }
        break;
    }

    default:
        break;
    }
    return 0;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * API pública
 * ──────────────────────────────────────────────────────────────────────────── */

static const lws_protocols protocols[] = {
    { "audio.bot", lws_callback, 0, 0 },
    { nullptr, nullptr, 0, 0 }
};

AudioPipe* ap_create(const char* url,
                     AudioPipeEventCallback cb,
                     void* user_data,
                     int max_reconnect_attempts)
{
    auto* ap      = new AudioPipe();
    ap->url       = url;
    ap->event_cb  = cb;
    ap->user_data = user_data;
    ap->max_reconnect_attempts = max_reconnect_attempts;  /* [M2] */

    lws_context_creation_info info{};
    info.port      = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options   = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.ka_time     = 10;
    info.ka_probes   = 3;
    info.ka_interval = 5;
    info.user        = ap;  /* [FIX #4] acessível em WAIT_CANCELLED */

    ap->lws_ctx = lws_create_context(&info);
    if (!ap->lws_ctx) {
        delete ap;
        return nullptr;
    }

    /* Parse URL e conecta */
    char host[128] = {}, path[256] = {};
    int  port   = 9998;
    bool is_ssl = (ap->url.substr(0, 3) == "wss");
    const char* u     = ap->url.c_str() + (is_ssl ? 6 : 5);
    const char* slash = strchr(u, '/');
    const char* colon = strchr(u, ':');

    if (colon && (!slash || colon < slash)) {
        int hlen = static_cast<int>(colon - u);
        strncpy(host, u, std::min(hlen, 127));
        host[std::min(hlen, 127)] = '\0';
        port = atoi(colon + 1);
    } else {
        int hlen = slash ? static_cast<int>(slash - u) : static_cast<int>(strlen(u));
        strncpy(host, u, std::min(hlen, 127));
        host[std::min(hlen, 127)] = '\0';
    }
    strncpy(path, slash ? slash : "/", 255);

    lws_client_connect_info ci{};
    ci.context        = ap->lws_ctx;
    ci.address        = host;
    ci.port           = port;
    ci.path           = path;
    ci.host           = host;
    ci.origin         = host;
    ci.protocol       = protocols[0].name;
    ci.ssl_connection = is_ssl ? LCCSCF_USE_SSL : 0;
    ci.userdata       = ap;

    ap->wsi = lws_client_connect_via_info(&ci);
    if (!ap->wsi) {
        lws_context_destroy(ap->lws_ctx);
        delete ap;
        return nullptr;
    }

    return ap;
}

/*
 * [M6] ap_destroy — flush WS CLOSE correto.
 *
 * Antes: único lws_service(ctx, 0) pode não ser suficiente para completar
 * o handshake WS CLOSE → bot recebe TCP RST em vez de WS close frame.
 *
 * Agora: lws_set_timeout() agenda close. Loop de até DESTROY_FLUSH_ITERS × 5ms
 * ou até wsi=nullptr (CLOSE completo detectado via CLIENT_CLOSED callback que
 * seta wsi=nullptr). Garante WS close frame limpo para o bot.
 */
void ap_destroy(AudioPipe* ap) {
    if (!ap) return;
    ap->closing = true;

    if (ap->wsi) {
        lws_set_timeout(ap->wsi, PENDING_TIMEOUT_CLOSE_SEND, LWS_TO_KILL_ASYNC);
        /* Loop de flush — chamado no dialplan thread após join da LWS thread */
        for (int i = 0; i < DESTROY_FLUSH_ITERS && ap->wsi; ++i)
            lws_service(ap->lws_ctx, DESTROY_FLUSH_MS);
    }

    if (ap->lws_ctx) {
        lws_context_destroy(ap->lws_ctx);
        ap->lws_ctx = nullptr;
    }
    delete ap;
}

void ap_service(AudioPipe* ap, int timeout_ms) {
    if (ap && ap->lws_ctx && !ap->closing)
        lws_service(ap->lws_ctx, timeout_ms);
}

/*
 * ap_on_rx_frame — RTP thread → enfileira frame RX para envio WS ao bot.
 * INVARIANTE: chamado APENAS do thread RTP. Ver nota em AudioPipe::rx_acc.
 */
void ap_on_rx_frame(AudioPipe* ap, const int16_t* pcm8, int samples8) {
    if (!ap || !ap->connected.load(std::memory_order_acquire) || ap->closing) return;

    int written = 0;
    while (written < samples8) {
        int room  = FS_FRAME_SAMPLES - ap->rx_acc_fill;
        int avail = samples8 - written;
        int copy  = std::min(room, avail);

        std::memcpy(ap->rx_acc + ap->rx_acc_fill, pcm8 + written, copy * sizeof(int16_t));
        ap->rx_acc_fill += copy;
        written         += copy;

        if (ap->rx_acc_fill == FS_FRAME_SAMPLES) {
            int16_t up[BOT_FRAME_SAMPLES];
            upsample_8_to_16(ap->rx_acc, FS_FRAME_SAMPLES, up);
            ap->sq_push(reinterpret_cast<const uint8_t*>(up), BOT_FRAME_BYTES);
            ap->rx_acc_fill = 0;
        }
    }
}

/*
 * ap_on_tx_frame — RTP thread → injeta frame TX do bot no RTP do usuário.
 * INVARIANTE: chamado APENAS do thread RTP.
 *
 * [M4] CNG quando ring vazio (via TxRingBuffer::pop retornando underrun > 0).
 * [M7] Jitter adaptativo via update_jitter().
 */
void ap_on_tx_frame(AudioPipe* ap, int16_t* pcm8_out, int samples8) {
    if (!ap || !ap->tx_ready) {
        fill_cng(pcm8_out, samples8);  /* [M4] CNG em vez de zeros */
        /* tx_ready=false: não conta como underrun para o jitter adaptativo */
        return;
    }
    int underrun = ap->tx_ring.pop(pcm8_out, samples8);
    if (underrun > 0) {
        ap->stat_tx_underruns.fetch_add(1, std::memory_order_relaxed);  /* [M5] */
    }
    ap->update_jitter(underrun > 0);  /* [M7] */
}

bool ap_is_connected(const AudioPipe* ap) {
    return ap && ap->connected.load(std::memory_order_acquire);
}

/* [M5] Snapshot atômico das métricas — sem lock, sem parar threads. */
AudioPipeStats ap_get_stats(const AudioPipe* ap) {
    AudioPipeStats s{};
    if (!ap) return s;
    s.tx_underruns  = ap->stat_tx_underruns.load(std::memory_order_relaxed);
    s.tx_ring_drops = ap->tx_ring.drops.load(std::memory_order_relaxed);
    s.sq_drops      = ap->stat_sq_drops.load(std::memory_order_relaxed);
    s.ws_reconnects = ap->stat_ws_reconnects.load(std::memory_order_relaxed);
    s.rx_frag_bytes = ap->stat_rx_frag_bytes.load(std::memory_order_relaxed);
    return s;
}

void ap_reset_stats(AudioPipe* ap) {
    if (!ap) return;
    ap->stat_tx_underruns.store(0, std::memory_order_relaxed);
    ap->tx_ring.drops.store(0, std::memory_order_relaxed);
    ap->stat_sq_drops.store(0, std::memory_order_relaxed);
    ap->stat_ws_reconnects.store(0, std::memory_order_relaxed);
    ap->stat_rx_frag_bytes.store(0, std::memory_order_relaxed);
}
