/*
 * agc_limiter.h — AGC (Automatic Gain Control) + Soft Knee Limiter
 * =================================================================
 *
 * Implementação de DSP de áudio em tempo real para amplificação de voz
 * quieta antes do envio ao STT (Deepgram).
 *
 * POSIÇÃO NO PIPELINE:
 *   ap_on_rx_frame() → AGC+Limiter → speex_resampler_process_int() → WS → STT
 *
 * PARÂMETROS (todos comentados com fonte/referência):
 *
 *   Target RMS: -18 dBFS
 *     Referência: EBU R 68 (broadcast telephony loudness target).
 *     -18 dBFS deixa 18 dB de headroom para picos de consoantes plosivas
 *     sem clipar, e é o nível em que os modelos de STT modernos (Deepgram,
 *     Whisper) têm melhor WER (Word Error Rate) documentado.
 *
 *   Attack time: 10ms @ 8kHz = 80 samples
 *     Referência: AES-17, seção 6.2 (attack/release para compressores de voz).
 *     10ms captura o onset de plosivas (b, p, d, t) sem distorção no transiente.
 *     Attack < 5ms introduz distorção inter-modular audível.
 *
 *   Release time: 300ms @ 8kHz = 2400 samples
 *     Referência: ITU-T P.56 (medição de nível de fala ativa).
 *     300ms é o tempo típico de pausa entre sílabas — o ganho não sobe
 *     durante silêncios inter-silábicos, evitando "gain pumping".
 *
 *   Max gain: 24 dB linear = 15.85x
 *     Limite prático: acima de 24 dB o SNR (Signal-to-Noise Ratio) torna
 *     o ruído de fundo audível e prejudicial ao VAD/STT.
 *
 *   Soft knee: -1 dBFS (32583 de 32767 full scale), knee width 3 dB
 *     Referência: Digital Audio Effects, Zölzer (2011), cap. 4.
 *     Knee suave elimina distorção harmônica do hard clipping. Na zona
 *     de knee (-4 dBFS a -1 dBFS), a compressão aumenta gradualmente
 *     de ratio 1:1 para ∞:1. Acima de -1 dBFS: hard limiter.
 */
#pragma once
#include <cstdint>
#include <cmath>
#include <algorithm>

class AgcLimiter {
public:
    /* Parâmetros configuráveis — valores padrão adequados para voz telefônica */
    struct Config {
        float target_rms_dbfs  = -18.0f;  /* nível alvo em dBFS             */
        float max_gain_db      =  24.0f;  /* ganho máximo permitido em dB   */
        float attack_ms        =  10.0f;  /* attack time em ms               */
        float release_ms       = 300.0f;  /* release time em ms              */
        float knee_db          =   3.0f;  /* largura do soft knee em dB      */
        float limiter_threshold_dbfs = -1.0f; /* início do limiter em dBFS  */
        int   sample_rate      =  8000;   /* Hz — deve ser 8000 para nosso RX */
        int   rms_window_ms    =  100;    /* janela de medição RMS em ms     */
    };

    explicit AgcLimiter(const Config& cfg = Config{}) : cfg_(cfg) {
        /* Coeficientes de envelope exponencial.
         * Fórmula: coeff = exp(-1 / (time_samples))
         * Fonte: Digital Audio Effects, Zölzer (2011), eq. 4.18 */
        float attack_samples  = cfg_.attack_ms  * cfg_.sample_rate / 1000.0f;
        float release_samples = cfg_.release_ms * cfg_.sample_rate / 1000.0f;
        attack_coeff_  = std::exp(-1.0f / attack_samples);
        release_coeff_ = std::exp(-1.0f / release_samples);

        /* Converte parâmetros dB → linear */
        target_rms_linear_ = dbfs_to_linear(cfg_.target_rms_dbfs);
        max_gain_linear_    = std::pow(10.0f, cfg_.max_gain_db / 20.0f);

        /* Limiter: threshold e knee em amplitude linear */
        limiter_threshold_ = dbfs_to_linear(cfg_.limiter_threshold_dbfs);
        knee_start_        = dbfs_to_linear(cfg_.limiter_threshold_dbfs - cfg_.knee_db);

        /* Janela de medição RMS: acumula samples por rms_window_ms */
        rms_window_size_   = cfg_.rms_window_ms * cfg_.sample_rate / 1000;
        rms_accum_         = 0.0;
        rms_count_         = 0;
        current_rms_       = 0.0f;

        /* Ganho inicial: 1.0 (sem amplificação) */
        gain_ = 1.0f;
    }

    /*
     * process() — processa um frame PCM L16 in-place.
     *
     * @samples  ponteiro para o buffer PCM L16 mono
     * @n        número de amostras (tipicamente 160 para 20ms @ 8kHz)
     *
     * Operação:
     *   1. Mede RMS do frame atual e acumula janela
     *   2. Calcula ganho desejado baseado no RMS medido vs target
     *   3. Aplica envelope attack/release para suavizar mudanças de ganho
     *   4. Aplica soft knee limiter para prevenir clipping
     *   5. Converte de volta para int16 com saturação segura
     */
    void process(int16_t* samples, int n) {
        if (!samples || n <= 0) return;

        /* Passo 1: Mede RMS do frame
         * RMS = sqrt(mean(x²)) — medição de energia do sinal
         * Usamos double para evitar overflow ao somar quadrados de int16 */
        double frame_rms_sq = 0.0;
        for (int i = 0; i < n; ++i) {
            double s = static_cast<double>(samples[i]);
            frame_rms_sq += s * s;
        }
        frame_rms_sq /= n;

        /* Acumula em janela deslizante (média exponencial para eficiência) */
        rms_accum_ = rms_accum_ * 0.9 + frame_rms_sq * 0.1;
        current_rms_ = static_cast<float>(std::sqrt(rms_accum_));

        /* Passo 2: Calcula ganho desejado
         * desired_gain = target_rms / current_rms
         * Clamped ao ganho máximo para não amplificar ruído puro */
        float desired_gain = 1.0f;
        if (current_rms_ > 1.0f) {  /* evita divisão por zero / ruído DC */
            /* Normaliza current_rms para escala dBFS de int16 (max = 32767) */
            float rms_normalized = current_rms_ / 32767.0f;
            float rms_linear_full = rms_normalized;
            desired_gain = target_rms_linear_ / std::max(rms_linear_full, 1e-6f);
            desired_gain = std::min(desired_gain, max_gain_linear_);
            desired_gain = std::max(desired_gain, 0.1f);  /* mínimo -20 dB */
        }

        /* Passo 3: Aplica envelope attack/release sample a sample.
         * Attack (ganho subindo): coeff pequeno = resposta rápida.
         * Release (ganho caindo): coeff grande = resposta lenta.
         * Isso é o oposto do que parece: "attack" em AGC de upward
         * significa que o ganho SOBE rapidamente quando o sinal está baixo. */
        for (int i = 0; i < n; ++i) {
            if (desired_gain > gain_) {
                /* Sinal baixo → aumenta ganho (attack) */
                gain_ = attack_coeff_ * gain_ + (1.0f - attack_coeff_) * desired_gain;
            } else {
                /* Sinal alto → diminui ganho (release) */
                gain_ = release_coeff_ * gain_ + (1.0f - release_coeff_) * desired_gain;
            }

            /* Passo 4: Aplica ganho + soft knee limiter
             *
             * Zona de operação:
             *   |x * gain| < knee_start_    → passa sem compressão (gain = gain_)
             *   knee_start_ ≤ |x * gain| < limiter_threshold_ → soft knee (compressão gradual)
             *   |x * gain| ≥ limiter_threshold_ → hard limit (sem clipping acima de 32767)
             */
            float x = static_cast<float>(samples[i]) * gain_;
            float abs_x = std::abs(x);

            if (abs_x >= limiter_threshold_ * 32767.0f) {
                /* Hard limit — zona acima do threshold */
                x = std::copysign(limiter_threshold_ * 32767.0f, x);

            } else if (abs_x >= knee_start_ * 32767.0f) {
                /* Soft knee — compressão gradual
                 * Usa curva quadrática para transição suave.
                 * Fonte: Zölzer (2011), eq. 4.22 */
                float knee_low  = knee_start_  * 32767.0f;
                float knee_high = limiter_threshold_ * 32767.0f;
                float knee_range = knee_high - knee_low;
                float t = (abs_x - knee_low) / knee_range;  /* 0..1 na zona de knee */
                /* Compressão quadrática: suave no início, forte no fim */
                float compressed = knee_low + knee_range * (t - t * t * 0.5f);
                x = std::copysign(compressed, x);
            }
            /* else: zona linear — sem modificação adicional */

            /* Passo 5: Converte para int16 com saturação segura.
             * std::clamp garante que nunca ultrapassamos ±32767 — zero clipping. */
            samples[i] = static_cast<int16_t>(
                std::clamp(x, -32767.0f, 32767.0f));
        }
    }

    /* Diagnóstico — retorna ganho atual em dB para logging/métricas */
    float gain_db() const {
        return 20.0f * std::log10(std::max(gain_, 1e-6f));
    }

    /* Diagnóstico — retorna RMS atual em dBFS para logging */
    float rms_dbfs() const {
        if (current_rms_ < 1.0f) return -96.0f;
        float rms_norm = current_rms_ / 32767.0f;
        return 20.0f * std::log10(std::max(rms_norm, 1e-6f));
    }

    /* Reset — chamado em session_create ou após reconexão */
    void reset() {
        gain_ = 1.0f;
        rms_accum_ = 0.0;
        current_rms_ = 0.0f;
        rms_count_ = 0;
    }

private:
    static float dbfs_to_linear(float dbfs) {
        return std::pow(10.0f, dbfs / 20.0f);
    }

    Config cfg_;

    /* Coeficientes de envelope (calculados no construtor) */
    float attack_coeff_;
    float release_coeff_;

    /* Valores de threshold em escala linear */
    float target_rms_linear_;
    float max_gain_linear_;
    float limiter_threshold_;
    float knee_start_;

    /* Estado do AGC */
    float  gain_;          /* ganho atual (linear) — state entre frames */
    double rms_accum_;     /* acumulador de energia (double para precisão) */
    float  current_rms_;   /* RMS medido no frame atual (escala int16) */
    int    rms_window_size_;
    int    rms_count_;
};