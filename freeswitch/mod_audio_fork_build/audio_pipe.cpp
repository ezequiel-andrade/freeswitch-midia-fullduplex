/*
 * audio_pipe.cpp — Full-Duplex WebSocket Audio Pipeline
 * ======================================================
 *
 * FIXES ORIGINAIS (mantidos):
 *   [FIX #2] TxRingBuffer::push() — tail avança com módulo.
 *   [FIX #4] sq_push() — lws_cancel_service() cross-thread safe.
 *   [FIX #5] tx_ready + ring resetados em CLOSED/CONNECTION_ERROR.
 *   [ARCH]   TX path 8 kHz nativo (Cartesia). RX path upsampled para 16 kHz.
 *
 * MELHORIAS ANTERIORES (mantidas):
 *   [M1] Acumulador de fragmentos WS no receive path (rx_frag_buf).
 *   [M2] Reconexão automática com exponential backoff.
 *   [M3] sq_pop_copy — cópia para buffer local antes de avançar tail.
 *   [M4] CNG (Comfort Noise Generator) em vez de zeros quando ring vazio.
 *   [M5] Métricas atômicas de produção (AudioPipeStats).
 *   [M6] ap_destroy com loop de flush WS CLOSE.
 *   [M7] Jitter buffer adaptativo por janela de underruns.
 *
 * MELHORIAS NOVAS:
 *   [N1] Metadata inicial JSON após ESTABLISHED.
 *
 *        Imediatamente após LWS_CALLBACK_CLIENT_ESTABLISHED, antes de habilitar
 *        o write de áudio, enviamos um frame LWS_WRITE_TEXT com o JSON fornecido
 *        em AudioPipeConfig::metadata_json. Isso permite que o bot identifique a
 *        sessão (UUID, caller_id, direction, sample_rate) sem depender do path
 *        da URL. O áudio começa após o write do metadata ser confirmado (flag
 *        metadata_sent garante a ordem: metadata → áudio).
 *
 *   [N2] Headers HTTP customizáveis no handshake WS Upgrade.
 *
 *        LWS dispara LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER antes de
 *        finalizar o handshake HTTP → WS. Neste callback, usamos
 *        lws_add_http_header_by_name() para adicionar cada header extra.
 *
 *        O campo extra_headers é parseado linha a linha:
 *          "Authorization: Bearer token\nX-Tenant: acme\n"
 *        Para cada linha: split em ": " → nome + valor → lws_add_http_header_by_name.
 *
 *        Referências: LWS mailing list (Andy Green, Dec 2019, issue #1851),
 *        warmcat/libwebsockets issues #1488, #2948, e
 *        lws-api-doc-master/html/group__client.html.
 *
 *   [N3] SpeexDSP resampler no path RX (8 kHz → 16 kHz).
 *
 *        Substitui upsample_8_to_16() (interpolação linear manual) por
 *        speex_resampler_process_int() com qualidade 4.
 *
 *        API utilizada conforme documentação oficial:
 *          speex.org/docs/manual/speex-manual/node7.html
 *          xiph/speexdsp include/speex/speex_resampler.h
 *
 *        Inicialização: speex_resampler_init(1, 8000, 16000, 4, &err)
 *          - 1 canal (mono)
 *          - input_rate = 8000 Hz (FreeSWITCH RTP)
 *          - output_rate = 16000 Hz (Deepgram STT)
 *          - quality = 4 (equilíbrio CPU/qualidade para tempo real;
 *            doc recomenda 3 para desktop, 10 para pro audio)
 *        speex_resampler_skip_zeros() é chamado após init para preencher o
 *        delay interno do filtro com zeros, evitando artefato inicial.
 *        Destruição: speex_resampler_destroy() em ap_destroy().
 *
 *   [N4] CNG bridging no path RX — filtro de frames de silêncio do carrier.
 *
 *        Evidência empírica (PCAP + tshark):
 *          - 3143/3733 frames (84.2%) do carrier PSTN contêm 0xFE (G.711 μ-law
 *            CNG/silêncio), gerados pela transcodificação AMR-SID → G.711 do SBC.
 *          - 156 transições silêncio↔fala em 74.66s = 2.09 por segundo.
 *          - Bursts de fala têm duração média de ~150ms (7-8 frames a 20ms).
 *        Esses micro-silêncios de 20-40ms entre palavras reiniciam o contador
 *        start_secs do SileroVAD, impedindo a detecção do turno do usuário.
 *
 *        Solução: após acumular um frame completo (160 samples @ 8kHz), detecta
 *        se é CNG (peak < CNG_PEAK_THRESHOLD = 200 ≈ -44 dBFS). Se for CNG e
 *        houver um frame de fala anterior em rx_last_speech, substitui o frame
 *        CNG pelo último frame de fala por até CNG_BRIDGE_MAX_FRAMES (3 frames =
 *        60ms). Após 60ms contínuos de silêncio, o silêncio passa normalmente —
 *        o VAD pode então detectar o fim do turno corretamente.
 *
 *        O bridging ocorre ANTES do SpeexPreprocess e do resampler, garantindo
 *        que o Speex processe o frame substituído (não o CNG original).
 *
 * Dependências: libwebsockets ≥ 4.x, libspeexdsp, C++14
 */

#include "audio_pipe.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <libwebsockets.h>
#include <speex/speex_resampler.h>   /* [N3] SpeexDSP */
#include <string>
#include <vector>

/* ─────────────────────────────────────────────────────────────────────────────
 * Constantes de áudio
 * ──────────────────────────────────────────────────────────────────────────── */
/* ── Taxas de amostragem ────────────────────────────────────────────────────
 *
 * Fluxo RX  (usuário → bot / STT):
 *   FS capta 8 kHz → SpeexDSP upsample → envia 16 kHz para o bot via WS.
 *
 * Fluxo TX  (bot → usuário / TTS):
 *   Bot envia 8 kHz via WS → injeta direto no RTP do usuário (sem conversão).
 *   O RTP de telefonia é 8 kHz — nenhum downsample necessário.
 *
 * Constantes separadas por direção para evitar ambiguidade:
 *   BOT_RX_*  → o que o bot RECEBE  (16 kHz — nosso RX path upsampled)
 *   BOT_TX_*  → o que o bot ENVIA   (8 kHz  — nosso TX path, sem conversão)
 *   FS_*      → clock interno do FS (8 kHz, 20ms, 160 samples)
 * ──────────────────────────────────────────────────────────────────────────── */
static constexpr int FS_SAMPLE_RATE      = 8000;
static constexpr int BOT_RX_SAMPLE_RATE  = 16000;   /* bot recebe em 16 kHz */
static constexpr int BOT_TX_SAMPLE_RATE  = 8000;    /* bot envia em 8 kHz   */

static constexpr int FRAME_MS            = 20;

/* FS / bot-TX: 8 kHz × 20ms = 160 samples = 320 bytes */
static constexpr int FS_FRAME_SAMPLES    = FS_SAMPLE_RATE    * FRAME_MS / 1000; // 160
static constexpr int FS_FRAME_BYTES      = FS_FRAME_SAMPLES  * 2;               // 320

/* bot-RX (WS → STT): 16 kHz × 20ms = 320 samples = 640 bytes */
static constexpr int BOT_RX_FRAME_SAMPLES = BOT_RX_SAMPLE_RATE * FRAME_MS / 1000; // 320
static constexpr int BOT_RX_FRAME_BYTES   = BOT_RX_FRAME_SAMPLES * 2;             // 640

/* bot-TX (WS ← TTS): 8 kHz × 20ms = 160 samples = 320 bytes
 * Idêntico ao FS — alias explícito para deixar a intenção clara no código. */
static constexpr int BOT_TX_FRAME_SAMPLES = BOT_TX_SAMPLE_RATE * FRAME_MS / 1000; // 160
static constexpr int BOT_TX_FRAME_BYTES   = BOT_TX_FRAME_SAMPLES * 2;             // 320

/* Ring buffer TX: 400ms de headroom = 20 frames @ 8 kHz */
static constexpr int TX_RING_FRAMES      = 20;
static constexpr int TX_RING_SAMPLES     = TX_RING_FRAMES * FS_FRAME_SAMPLES;     // 3200

/* [M7] Jitter buffer adaptativo */
static constexpr int TX_JITTER_MIN         = 1;
static constexpr int TX_JITTER_MAX         = 4;
static constexpr int JITTER_ADAPT_WINDOW   = 50;
static constexpr int JITTER_UNDERRUN_THR   = 3;
static constexpr int JITTER_STABLE_WINDOW  = 500;

/* LWS */
static constexpr int AP_LWS_PRE  = LWS_PRE;

/* [M2] Backoff de reconexão */
static constexpr int RECONNECT_BASE_MS  = 100;
static constexpr int RECONNECT_MAX_MS   = 5000;

/* [M6] Flush WS CLOSE em ap_destroy */
static constexpr int DESTROY_FLUSH_ITERS = 5;
static constexpr int DESTROY_FLUSH_MS    = 5;

/* [N1] Tamanho máximo do metadata JSON */
static constexpr int METADATA_MAX_BYTES = 2048;

/* [N3] Qualidade do SpeexDSP resampler (0-10, doc sugere 3 para desktop) */
static constexpr int SPEEX_RESAMPLE_QUALITY = 4;
/* [N4] CNG bridging — carrier PSTN AMR→G.711 gera 84.2% de frames de silêncio (0xFE)
 * a ~2.09 transições/segundo dentro da fala (evidência empírica do PCAP).
 * Substituímos até CNG_BRIDGE_MAX_FRAMES consecutivos de CNG pelo último frame de fala
 * para manter o SileroVAD ativo durante os micro-gaps do carrier VAD.
 *
 * ONSET HYSTERESIS (bug fix):
 *   Os primeiros ~180ms de cada frase (AMR decoder warm-up) chegam como frames de
 *   baixíssima amplitude (0xF6-0x60 → PCM 2-93) que estão ABAIXO do threshold 200.
 *   Sem hysteresis, esses frames seriam classificados como CNG e substituídos por
 *   áudio da frase ANTERIOR, injetando ghost-speech perceptível no onset.
 *
 *   Solução: bridging só é ativado DEPOIS de CNG_ONSET_MIN_FRAMES consecutivos de
 *   fala real (peak > CNG_PEAK_THRESHOLD). Isso garante que estamos no meio da fala,
 *   não no onset. Frames de onset passam através inalterados (incluindo os fracos).
 *
 *   Estado: SILENCE / ONSET (não estabelecido) → SPEECH_ESTABLISHED (rx_speech_established=true)
 *     - SILENCE/ONSET: CNG passa inalterado. CNG longo: mantém estado.
 *     - SPEECH_ESTABLISHED: CNG curto → bridge. CNG longo → reset para SILENCE.
 *
 * Evidência: 0xFE (±2 PCM), 0x7E (±2 PCM), 0xF0 (±30), 0x60 (±93) < 200.
 *            0xC9 (±327), 0x4C (±279), 0x3C (±591) > 200 = fala real. */
static constexpr int     CNG_BRIDGE_MAX_FRAMES  = 3;   /* 3 × 20ms = 60ms max bridge             */
static constexpr int16_t CNG_PEAK_THRESHOLD     = 200; /* peak < 200 ≈ -44 dBFS → CNG/onset     */
static constexpr int     CNG_ONSET_MIN_FRAMES   = 5;   /* 5 × 20ms = 100ms de fala p/ estabelecer */

static bool env_flag_enabled(const char* name, bool defv = false) {
    const char* v = std::getenv(name);
    if (!v) return defv;
    return (*v == '1' || *v == 'y' || *v == 'Y' || *v == 't' || *v == 'T');
}

/* ─────────────────────────────────────────────────────────────────────────────
 * TxRingBuffer — SPSC lock-free
 * Produtor: LWS thread  |  Consumidor: RTP thread
 * ──────────────────────────────────────────────────────────────────────────── */
struct TxRingBuffer {
    int16_t          buf[TX_RING_SAMPLES];
    std::atomic<int> head{0};
    std::atomic<int> tail{0};
    std::atomic<int> count{0};
    std::atomic<uint64_t> drops{0};

    TxRingBuffer() { std::memset(buf, 0, sizeof(buf)); }

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

    /* Retorna número de amostras faltantes (underrun); preenche com silêncio. */
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
        int underrun = n - read;
        if (underrun > 0) std::memset(dst + read, 0, underrun * sizeof(int16_t));
        return underrun;
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
 * [N2] parse_extra_headers — extrai pares nome/valor do campo extra_headers.
 *
 * Formato de entrada: "Header-Name: value\nHeader2: value2\n"
 * Separador de nome: ": " (dois pontos + espaço, RFC 7230).
 * Retorna vector de pares {nome_com_dois_pontos, valor} prontos para
 * lws_add_http_header_by_name().
 *
 * Nota: o LWS exige que o nome do header termine com ':' (sem espaço) ao ser
 * passado para lws_add_http_header_by_name(). Nós o armazenamos com ':' final.
 * ──────────────────────────────────────────────────────────────────────────── */
struct HeaderPair {
    std::string name;   /* ex: "Authorization:" */
    std::string value;  /* ex: "Bearer eyJ..."  */
};

static std::vector<HeaderPair> parse_extra_headers(const char* raw) {
    std::vector<HeaderPair> result;
    if (!raw || !*raw) return result;

    std::string input(raw);
    size_t pos = 0;
    while (pos < input.size()) {
        size_t nl = input.find('\n', pos);
        if (nl == std::string::npos) nl = input.size();
        std::string line = input.substr(pos, nl - pos);
        pos = nl + 1;
        if (line.empty()) continue;

        /* Encontra ": " como separador nome/valor */
        size_t sep = line.find(": ");
        if (sep == std::string::npos) continue;

        HeaderPair hp;
        hp.name  = line.substr(0, sep) + ":";  /* "Authorization:" */
        hp.value = line.substr(sep + 2);        /* "Bearer ..."     */
        if (!hp.name.empty() && !hp.value.empty())
            result.push_back(std::move(hp));
    }
    return result;
}

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

    /* ── [N1] Metadata inicial ──────────────────────────────────────────────
     * metadata_json: cópia da string fornecida em AudioPipeConfig.
     * metadata_sent: flag que garante a ordem metadata → áudio.
     *   - ESTABLISHED seta metadata_sent = false e chama lws_callback_on_writable.
     *   - WRITEABLE: se !metadata_sent → envia metadata (LWS_WRITE_TEXT) → seta true.
     *   - WRITEABLE: se metadata_sent → processa send queue de áudio normalmente.
     * ──────────────────────────────────────────────────────────────────────── */
    std::string metadata_json;
    bool        metadata_sent = false;

    /* ── [N2] Headers HTTP extras ───────────────────────────────────────────
     * Parseados em ap_create() e usados em APPEND_HANDSHAKE_HEADER.
     * Após ESTABLISHED o vector não é mais acessado — sem necessidade de lock.
     * ──────────────────────────────────────────────────────────────────────── */
    std::vector<HeaderPair> extra_headers;

    /* ── [M2] Reconexão com exponential backoff ─────────────────────────────── */
    bool     reconnect_pending       = false;
    int      reconnect_attempts      = 0;
    int      max_reconnect_attempts  = 0;
    uint64_t reconnect_at_ms         = 0;
    int      reconnect_backoff_ms    = RECONNECT_BASE_MS;

    /* ── TX — 8 kHz nativo (Cartesia TTS) ──────────────────────────────────── */
    TxRingBuffer tx_ring;
    bool         tx_ready          = false;

    /* [M7] Jitter adaptativo */
    int tx_jitter_frames       = TX_JITTER_MIN;
    int jitter_frame_counter   = 0;
    int jitter_underrun_count  = 0;
    int jitter_stable_counter  = 0;

    /* ── [M1] Acumulador de fragmentos WS (receive path) ────────────────────── */
    uint8_t rx_frag_buf[BOT_TX_FRAME_BYTES * 4];
    int     rx_frag_fill = 0;

    /* ── RX accumulator — INVARIANTE: apenas thread RTP ────────────────────── */
    int16_t rx_acc[FS_FRAME_SAMPLES];
    int     rx_acc_fill = 0;

    /* ── [N4] CNG bridging (carrier PSTN VAD) ───────────────────────────────── *
     * rx_last_speech:      cópia do último frame 8kHz com fala real confirmada. *
     * rx_cng_count:        contador de frames CNG consecutivos.                 *
     * rx_speech_frames:    contador de frames de fala consecutivos.             *
     * rx_speech_established: true somente após CNG_ONSET_MIN_FRAMES de fala.   *
     *   Onset hysteresis: frames de onset AMR (peak 2-93 < 200) passam         *
     *   inalterados até que speech_established seja ativado — evita injetar     *
     *   áudio da frase anterior no início de cada nova frase.                  *
     * Acessados exclusivamente pelo thread RTP (sem lock necessário).           */
    int16_t rx_last_speech[FS_FRAME_SAMPLES] = {};
    int     rx_cng_count          = 0;
    int     rx_speech_frames      = 0;
    bool    rx_speech_established = false;
    bool    rx_cng_bridge_enabled = false;

    /* ── [N3] SpeexDSP resampler (RX path: 8 kHz → 16 kHz) ─────────────────
     * Inicializado em ap_create(), destruído em ap_destroy().
     * Acesso exclusivo do thread RTP via ap_on_rx_frame() — sem lock necessário.
     * speex_resampler_skip_zeros() é chamado após init para eliminar artefato
     * de startup causado pelo delay interno do filtro FIR.
     * ──────────────────────────────────────────────────────────────────────── */
    SpeexResamplerState* spx_resampler = nullptr;
    /* ── Send queue SPSC (RTP thread → LWS thread) ──────────────────────────── */
    struct TxMsg {
        uint8_t data[AP_LWS_PRE + BOT_RX_FRAME_BYTES];
        int     len;
    };
    static constexpr int SEND_QUEUE_SIZE = 16;
    TxMsg            send_queue[SEND_QUEUE_SIZE];
    std::atomic<int> sq_head{0};
    std::atomic<int> sq_tail{0};
    std::atomic<int> sq_count{0};

    /* ── [M5] Métricas ──────────────────────────────────────────────────────── */
    std::atomic<uint64_t> stat_tx_underruns{0};
    std::atomic<uint64_t> stat_sq_drops{0};
    std::atomic<uint64_t> stat_ws_reconnects{0};
    std::atomic<uint64_t> stat_rx_frag_bytes{0};

    /* ── Callbacks ──────────────────────────────────────────────────────────── */
    AudioPipeEventCallback event_cb  = nullptr;
    void*                  user_data = nullptr;

    /* ── sq_push (RTP thread → LWS thread) ──────────────────────────────────── */
    bool sq_push(const uint8_t* payload, int len) {
        if (sq_count.load(std::memory_order_acquire) >= SEND_QUEUE_SIZE) {
            stat_sq_drops.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        int h = sq_head.load(std::memory_order_relaxed);
        std::memcpy(send_queue[h].data + AP_LWS_PRE, payload, len);
        send_queue[h].len = len;
        sq_head.store((h + 1) % SEND_QUEUE_SIZE, std::memory_order_release);
        sq_count.fetch_add(1, std::memory_order_acq_rel);
        if (lws_ctx) lws_cancel_service(lws_ctx);  /* [FIX #4] */
        return true;
    }

    /* ── [M3] sq_pop_copy — copia para buffer local antes de avançar tail ────── */
    bool sq_pop_copy(uint8_t* buf, int* out_len) {
        if (sq_count.load(std::memory_order_acquire) == 0) return false;
        int t = sq_tail.load(std::memory_order_relaxed);
        std::memcpy(buf + AP_LWS_PRE, send_queue[t].data + AP_LWS_PRE, send_queue[t].len);
        *out_len = send_queue[t].len;
        sq_tail.store((t + 1) % SEND_QUEUE_SIZE, std::memory_order_release);
        sq_count.fetch_sub(1, std::memory_order_acq_rel);
        return true;
    }

    /* ── [M2] Helpers de reconexão ──────────────────────────────────────────── */
    static uint64_t now_ms() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000u +
               static_cast<uint64_t>(ts.tv_nsec) / 1000000u;
    }

    void schedule_reconnect() {
        reconnect_at_ms   = now_ms() + static_cast<uint64_t>(reconnect_backoff_ms);
        reconnect_pending = true;
        reconnect_backoff_ms = std::min(reconnect_backoff_ms * 2, RECONNECT_MAX_MS);
    }

    void reset_reconnect() {
        reconnect_pending    = false;
        reconnect_attempts   = 0;
        reconnect_backoff_ms = RECONNECT_BASE_MS;
        reconnect_at_ms      = 0;
    }

    /* ── [M7] Jitter buffer adaptativo ──────────────────────────────────────── */
    void update_jitter(bool underrun) {
        jitter_frame_counter++;
        if (underrun) { jitter_underrun_count++; jitter_stable_counter = 0; }
        else          { jitter_stable_counter++; }

        if (jitter_frame_counter >= JITTER_ADAPT_WINDOW) {
            if (jitter_underrun_count > JITTER_UNDERRUN_THR &&
                tx_jitter_frames < TX_JITTER_MAX) {
                tx_jitter_frames++;
                tx_ready = false;  /* forçar reenchimento com novo threshold */
            }
            jitter_underrun_count = 0;
            jitter_frame_counter  = 0;
        }
        if (jitter_stable_counter >= JITTER_STABLE_WINDOW &&
            tx_jitter_frames > TX_JITTER_MIN) {
            tx_jitter_frames--;
            jitter_stable_counter = 0;
        }
    }
};

/* ─────────────────────────────────────────────────────────────────────────────
 * Helpers de URL parsing e conexão (usados em ap_create e do_reconnect)
 * ──────────────────────────────────────────────────────────────────────────── */
static void parse_url(const std::string& url,
                      char* host, int host_max,
                      int*  port,
                      char* path, int path_max,
                      bool* is_ssl)
{
    *is_ssl = (url.substr(0, 3) == "wss");
    const char* u     = url.c_str() + (*is_ssl ? 6 : 5);
    const char* slash = strchr(u, '/');
    const char* colon = strchr(u, ':');
    *port = 9998;

    if (colon && (!slash || colon < slash)) {
        int hlen = static_cast<int>(colon - u);
        strncpy(host, u, std::min(hlen, host_max - 1));
        host[std::min(hlen, host_max - 1)] = '\0';
        *port = atoi(colon + 1);
    } else {
        int hlen = slash ? static_cast<int>(slash - u) : static_cast<int>(strlen(u));
        strncpy(host, u, std::min(hlen, host_max - 1));
        host[std::min(hlen, host_max - 1)] = '\0';
    }
    strncpy(path, slash ? slash : "/", path_max - 1);
    path[path_max - 1] = '\0';
}

static int lws_callback(struct lws* wsi, enum lws_callback_reasons reason,
                        void* user, void* in, size_t len);

static const lws_protocols protocols[] = {
    { "audio.bot", lws_callback, 0, 0 },
    { nullptr, nullptr, 0, 0 }
};

static bool do_connect(AudioPipe* ap) {
    char host[128] = {}, path[256] = {};
    int  port;
    bool is_ssl;
    parse_url(ap->url, host, sizeof(host), &port, path, sizeof(path), &is_ssl);

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
    return ap->wsi != nullptr;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * [M2] check_reconnect — chamado no LWS event loop (WAIT_CANCELLED)
 * ──────────────────────────────────────────────────────────────────────────── */
static void check_reconnect(AudioPipe* ap) {
    if (!ap || !ap->reconnect_pending || ap->closing || ap->wsi) return;
    if (AudioPipe::now_ms() < ap->reconnect_at_ms) return;

    if (ap->max_reconnect_attempts > 0 &&
        ap->reconnect_attempts >= ap->max_reconnect_attempts) {
        ap->reconnect_pending = false;
        if (ap->event_cb) ap->event_cb(ap, AP_EVENT_DISCONNECTED, ap->user_data);
        return;
    }

    ap->reconnect_attempts++;
    ap->stat_ws_reconnects.fetch_add(1, std::memory_order_relaxed);
    if (ap->event_cb) ap->event_cb(ap, AP_EVENT_RECONNECTING, ap->user_data);

    if (!do_connect(ap))
        ap->schedule_reconnect();
}

/* ─────────────────────────────────────────────────────────────────────────────
 * LWS callback
 * ──────────────────────────────────────────────────────────────────────────── */
static int lws_callback(struct lws* wsi, enum lws_callback_reasons reason,
                        void* /*user*/, void* in, size_t len)
{
    AudioPipe* ap = reinterpret_cast<AudioPipe*>(lws_wsi_user(wsi));

    switch (reason) {

    /* ── [N2] Adiciona headers HTTP extras antes do WS Upgrade ─────────────
     *
     * LWS chama este callback durante a fase HTTP do handshake.
     * `in`  → ponteiro para o ponteiro atual no buffer de escrita de headers.
     * `len` → bytes restantes no buffer.
     * lws_add_http_header_by_name() avança *p e retorna não-zero se falta espaço.
     *
     * Fonte: Andy Green, LWS mailing list Dec/2019:
     *   "The missing trick is there's a callback
     *    LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER in the client protocol
     *    handler that is called back at the right time for it."
     *   (https://libwebsockets.org/pipermail/libwebsockets/2019-December/008165.html)
     * ──────────────────────────────────────────────────────────────────────── */
    case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER: {
        if (!ap || ap->extra_headers.empty()) break;

        unsigned char** p   = reinterpret_cast<unsigned char**>(in);
        unsigned char*  end = *p + len;

        for (const auto& hdr : ap->extra_headers) {
            int rc = lws_add_http_header_by_name(
                wsi,
                reinterpret_cast<const unsigned char*>(hdr.name.c_str()),
                reinterpret_cast<const unsigned char*>(hdr.value.c_str()),
                static_cast<int>(hdr.value.size()),
                p,
                end
            );
            if (rc) {
                /* Buffer de headers insuficiente — aborta handshake */
                lwsl_err("ap: header '%s' não coube no buffer do handshake\n",
                         hdr.name.c_str());
                return -1;
            }
        }
        break;
    }

    /* ── Conexão estabelecida ───────────────────────────────────────────────── */
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        ap->connected.store(true, std::memory_order_release);
        ap->reset_reconnect();
        ap->metadata_sent = false;   /* [N1] garante envio do metadata primeiro */
        if (ap->event_cb) ap->event_cb(ap, AP_EVENT_CONNECTED, ap->user_data);
        lws_callback_on_writable(wsi);  /* dispara WRITEABLE para metadata [N1] */
        break;

    /* ── Desconexão / erro ─────────────────────────────────────────────────── */
    case LWS_CALLBACK_CLIENT_CLOSED:
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        if (ap) {
            ap->connected.store(false, std::memory_order_release);
            ap->wsi         = nullptr;
            ap->tx_ready    = false;
            ap->tx_ring.reset();
            ap->rx_frag_fill = 0;

            if (!ap->closing && ap->max_reconnect_attempts != 0) {
                ap->schedule_reconnect();
            } else {
                if (ap->event_cb)
                    ap->event_cb(ap, AP_EVENT_DISCONNECTED, ap->user_data);
            }
        }
        break;

    /* ── [FIX #4] Wake-up do RTP thread + [M2] check_reconnect ─────────────── */
    case LWS_CALLBACK_EVENT_WAIT_CANCELLED: {
        AudioPipe* cap = reinterpret_cast<AudioPipe*>(
            lws_context_user(lws_get_context(wsi)));
        if (!cap) break;
        check_reconnect(cap);
        if (cap->wsi && !cap->closing &&
            (cap->sq_count.load(std::memory_order_acquire) > 0 ||
             !cap->metadata_sent))   /* [N1] acorda para enviar metadata */
            lws_callback_on_writable(cap->wsi);
        break;
    }

    /* ── Envio: metadata [N1] primeiro, depois áudio RX ─────────────────────── */
    case LWS_CALLBACK_CLIENT_WRITEABLE: {
        if (!ap || ap->closing) break;

        /* [N1] Metadata — enviado como primeiro frame após ESTABLISHED.
         *
         * LWS_WRITE_TEXT sinaliza ao bot que é um frame de controle JSON,
         * não PCM binário. O bot pode distinguir pelo tipo de frame WS.
         * Usamos um buffer local com LWS_PRE para respeitar o padding do LWS.
         */
        if (!ap->metadata_sent && !ap->metadata_json.empty()) {
            const std::string& meta = ap->metadata_json;
            int mlen = static_cast<int>(meta.size());
            if (mlen > METADATA_MAX_BYTES) mlen = METADATA_MAX_BYTES;

            /* Buffer no stack: LWS_PRE + payload — dentro do limite de stack */
            uint8_t buf[AP_LWS_PRE + METADATA_MAX_BYTES];
            std::memcpy(buf + AP_LWS_PRE, meta.c_str(), mlen);

            int rc = lws_write(wsi, buf + AP_LWS_PRE,
                               static_cast<size_t>(mlen), LWS_WRITE_TEXT);
            if (rc >= 0) {
                ap->metadata_sent = true;
                /* Após metadata, reagenda imediatamente para enviar áudio */
                lws_callback_on_writable(wsi);
            }
            /* Se rc < 0, LWS vai disparar CLOSED — não precisamos tratar aqui */
            break;
        }

        /* Áudio RX — send queue → WS */
        uint8_t local_buf[AP_LWS_PRE + BOT_RX_FRAME_BYTES];
        int     plen = 0;
        if (!ap->sq_pop_copy(local_buf, &plen)) break;

        int rc = lws_write(wsi,
                           local_buf + AP_LWS_PRE,
                           static_cast<size_t>(plen),
                           LWS_WRITE_BINARY);
        if (rc < 0) break;

        if (ap->sq_count.load(std::memory_order_acquire) > 0)
            lws_callback_on_writable(wsi);
        break;
    }

    /* ── Recebimento de áudio TX (bot → FS RTP) — [M1] acumulador ──────────── */
    case LWS_CALLBACK_CLIENT_RECEIVE: {
        if (!ap || !in || len == 0) break;

        /* [N1] Ignora frames de texto (bot pode enviar JSON de controle) */
        if (lws_frame_is_binary(wsi) == 0) break;

        const uint8_t* p   = reinterpret_cast<const uint8_t*>(in);
        int            rem = static_cast<int>(len);

        ap->stat_rx_frag_bytes.fetch_add(len, std::memory_order_relaxed);

        while (rem > 0) {
            int space = static_cast<int>(sizeof(ap->rx_frag_buf)) - ap->rx_frag_fill;
            int copy  = std::min(rem, space);
            if (copy <= 0) { ap->rx_frag_fill = 0; continue; }
            std::memcpy(ap->rx_frag_buf + ap->rx_frag_fill, p, copy);
            ap->rx_frag_fill += copy;
            p   += copy;
            rem -= copy;

            while (ap->rx_frag_fill >= FS_FRAME_BYTES) {
                const int16_t* src8 = reinterpret_cast<const int16_t*>(ap->rx_frag_buf);
                ap->tx_ring.push(src8, FS_FRAME_SAMPLES);
                int residual = ap->rx_frag_fill - FS_FRAME_BYTES;
                if (residual > 0)
                    std::memmove(ap->rx_frag_buf,
                                 ap->rx_frag_buf + FS_FRAME_BYTES, residual);
                ap->rx_frag_fill = residual;
            }
        }

        if (!ap->tx_ready &&
            ap->tx_ring.available() >= ap->tx_jitter_frames * FS_FRAME_SAMPLES)
            ap->tx_ready = true;
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

AudioPipe* ap_create(const char*            url,
                     AudioPipeEventCallback cb,
                     void*                  user_data,
                     const AudioPipeConfig* cfg)
{
    auto* ap      = new AudioPipe();
    ap->url       = url;
    ap->event_cb  = cb;
    ap->user_data = user_data;

    /* Configuração opcional */
    if (cfg) {
        ap->max_reconnect_attempts = cfg->max_reconnect_attempts;

        /* [N1] Copia metadata JSON */
        if (cfg->metadata_json && *cfg->metadata_json)
            ap->metadata_json = cfg->metadata_json;

        /* [N2] Parseia headers extras */
        if (cfg->extra_headers && *cfg->extra_headers)
            ap->extra_headers = parse_extra_headers(cfg->extra_headers);
    }

    /* Hotfix de produção:
     * CNG bridge no RX pode introduzir artefato metálico/robótico em alguns carriers.
     * Default OFF para preservar naturalidade; habilite explicitamente se necessário. */
    ap->rx_cng_bridge_enabled = env_flag_enabled("AF_RX_CNG_BRIDGE_ENABLE", false);

    /* [N3] Inicializa SpeexDSP resampler 8 kHz → 16 kHz, qualidade 4.
     *
     * speex_resampler_init(nb_channels, in_rate, out_rate, quality, &err)
     * speex_resampler_skip_zeros() preenche o delay interno do filtro com zeros,
     * eliminando o artefato de startup (burst de energia no início do stream).
     * Fonte: speex.org/docs/manual/speex-manual/node7.html
     */
    {
        int spx_err = 0;
        ap->spx_resampler = speex_resampler_init(
            1,                       /* nb_channels: mono */
            FS_SAMPLE_RATE,          /* in_rate:  8000 Hz */
            BOT_RX_SAMPLE_RATE,      /* out_rate: 16000 Hz (RX path: FS→bot) */
            SPEEX_RESAMPLE_QUALITY,  /* quality:  4 */
            &spx_err
        );
        if (!ap->spx_resampler || spx_err != RESAMPLER_ERR_SUCCESS) {
            delete ap;
            return nullptr;
        }
        speex_resampler_skip_zeros(ap->spx_resampler);
    }

    /* Cria contexto LWS */
    lws_context_creation_info info{};
    info.port      = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options   = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.ka_time     = 10;
    info.ka_probes   = 3;
    info.ka_interval = 5;
    info.user        = ap;   /* acessível em WAIT_CANCELLED via lws_context_user() */

    ap->lws_ctx = lws_create_context(&info);
    if (!ap->lws_ctx) {
        speex_resampler_destroy(ap->spx_resampler);
        delete ap;
        return nullptr;
    }

    if (!do_connect(ap)) {
        speex_resampler_destroy(ap->spx_resampler);
        lws_context_destroy(ap->lws_ctx);
        delete ap;
        return nullptr;
    }

    return ap;
}

/* Destrói o contexto diretamente sem esperar TCP close */
void ap_destroy(AudioPipe* ap) {
    if (!ap) return;
    ap->closing = true;

    /* Não chamamos lws_service aqui — a LWS thread já foi parada via join
     * em session_destroy() antes de ap_destroy() ser chamado.
     * lws_context_destroy() fecha todos os wsi pendentes e libera recursos
     * incondicionalmente, sem aguardar handshake TCP de fechamento.
     * O bot receberá um TCP RST em vez de WS close frame — aceitável
     * porque a chamada já encerrou. */
    if (ap->lws_ctx) {
        lws_context_destroy(ap->lws_ctx);
        ap->lws_ctx = nullptr;
    }

    if (ap->spx_resampler) {
        speex_resampler_destroy(ap->spx_resampler);
        ap->spx_resampler = nullptr;
    }
    delete ap;
}


void ap_service(AudioPipe* ap, int timeout_ms) {
    if (ap && ap->lws_ctx && !ap->closing)
        lws_service(ap->lws_ctx, timeout_ms);
}

extern "C" void ap_cancel_service(AudioPipe* ap)
{
    if (ap && ap->lws_ctx)
        lws_cancel_service(ap->lws_ctx);
}

extern "C" void ap_set_closing(AudioPipe* ap)
{
    if (ap) ap->closing = true;
}

/*
 * ap_on_rx_frame — thread RTP → upsample (SpeexDSP) → send queue → WS → STT
 *
 * [N3] speex_resampler_process_int():
 *   - channel_index = 0 (mono)
 *   - in / in_length: buffer 8 kHz de entrada
 *   - out / out_length: buffer 16 kHz de saída
 *   A doc garante que ou todos os samples de entrada são lidos OU todos os
 *   de saída são escritos. Para razão 1:2 e frame completo de 160 amostras,
 *   sempre produz 320 amostras (ou menos se o filtro estiver se aquecendo, mas
 *   skip_zeros() elimina esse período).
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
            /* [N4] CNG bridging com onset hysteresis (opcional via env).
             *
             * Problema: carrier AMR→G.711 gera dois tipos de frames abaixo do
             * threshold CNG_PEAK_THRESHOLD=200:
             *   a) Silêncio real (0xFE/0x7E → PCM ±2)
             *   b) Onset AMR warm-up (0xF6..0x60 → PCM 2-93, ~180ms por frase)
             *
             * Sem hysteresis, o bridging substituiria os frames de onset (b) pelo
             * áudio da frase anterior, injetando ghost-speech perceptível.
             *
             * Lógica de estados:
             *   SILENCE/ONSET (rx_speech_established=false):
             *     - CNG frames: pass through (preserva onset inalterado)
             *     - Speech: incrementa rx_speech_frames; ao atingir
             *       CNG_ONSET_MIN_FRAMES, ativa rx_speech_established
             *   SPEECH_ESTABLISHED (rx_speech_established=true):
             *     - CNG curto (≤ MAX): bridge com rx_last_speech
             *     - CNG longo (> MAX): pass through, resetar para SILENCE
             *     - Speech: atualiza rx_last_speech */
            if (ap->rx_cng_bridge_enabled) {
                bool is_cng = true;
                for (int i = 0; i < FS_FRAME_SAMPLES; ++i) {
                    if (std::abs(static_cast<int>(ap->rx_acc[i])) > CNG_PEAK_THRESHOLD) {
                        is_cng = false;
                        break;
                    }
                }
                if (is_cng) {
                    ap->rx_cng_count++;
                    ap->rx_speech_frames = 0;
                    if (ap->rx_speech_established) {
                        if (ap->rx_cng_count <= CNG_BRIDGE_MAX_FRAMES) {
                            /* SPEECH_ESTABLISHED + short CNG gap: bridge */
                            std::memcpy(ap->rx_acc, ap->rx_last_speech, FS_FRAME_BYTES);
                        } else {
                            /* SPEECH_ESTABLISHED + long CNG: reset to SILENCE */
                            ap->rx_speech_established = false;
                        }
                    }
                    /* SILENCE/ONSET: CNG passes through unchanged */
                } else {
                    ap->rx_cng_count = 0;
                    ap->rx_speech_frames++;
                    if (!ap->rx_speech_established &&
                        ap->rx_speech_frames >= CNG_ONSET_MIN_FRAMES) {
                        ap->rx_speech_established = true;
                    }
                    std::memcpy(ap->rx_last_speech, ap->rx_acc, FS_FRAME_BYTES);
                }
            }

            /* [N3] Resample 8 kHz → 16 kHz via SpeexDSP */
            int16_t up[BOT_RX_FRAME_SAMPLES];
            spx_uint32_t in_len  = static_cast<spx_uint32_t>(FS_FRAME_SAMPLES);
            spx_uint32_t out_len = static_cast<spx_uint32_t>(BOT_RX_FRAME_SAMPLES);

            speex_resampler_process_int(
                ap->spx_resampler,
                0,                /* channel_index: mono */
                ap->rx_acc,
                &in_len,
                up,
                &out_len
            );

            /* Enfileira apenas os samples efetivamente produzidos */
            if (out_len > 0)
                ap->sq_push(reinterpret_cast<const uint8_t*>(up),
                            static_cast<int>(out_len) * 2 /* bytes */);

            ap->rx_acc_fill = 0;
        }
    }
}

/*
 * ap_on_tx_frame — thread RTP → ring.pop → RTP do usuário
 * Silêncio quando TX ainda não está pronto; jitter adaptativo [M7].
 */
void ap_on_tx_frame(AudioPipe* ap, int16_t* pcm8_out, int samples8) {
    if (!ap || !ap->tx_ready) {
        std::memset(pcm8_out, 0, samples8 * sizeof(int16_t));
        return;
    }
    int underrun = ap->tx_ring.pop(pcm8_out, samples8);
    if (underrun > 0)
        ap->stat_tx_underruns.fetch_add(1, std::memory_order_relaxed);
    ap->update_jitter(underrun > 0);
}

bool ap_is_connected(const AudioPipe* ap) {
    return ap && ap->connected.load(std::memory_order_acquire);
}

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
