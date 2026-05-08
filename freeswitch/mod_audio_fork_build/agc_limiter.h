#pragma once
/*
 * agc_limiter.h — Automatic Gain Control + Peak Limiter
 * =======================================================
 * PCM L16 Mono 8kHz, frames de 20ms (160 amostras)
 *
 * Dois estágios:
 *   1. AGC  — ganho lento (attack 50ms, release 300ms)
 *   2. Peak Limiter — attack instantâneo, release suave (50ms)
 *                     garante zero clipping em qualquer condição
 */

#include <algorithm>
#include <cmath>
#include <cstdint>

class AgcLimiter {
public:
    struct Config {
        float target_rms_dbfs   = -18.0f;
        float max_gain_db       =  24.0f;
        float attack_ms         =  50.0f;
        float release_ms        = 300.0f;
        float limiter_threshold = -1.0f;
        int   sample_rate       =  8000;
    };

    AgcLimiter() : AgcLimiter(Config{}) {}

    explicit AgcLimiter(const Config& cfg) : cfg_(cfg) {
        float peak_linear      = std::pow(10.0f, cfg_.target_rms_dbfs / 20.0f) * 32767.0f;
        target_rms_linear_     = peak_linear / std::sqrt(2.0f);
        max_gain_linear_       = std::pow(10.0f, cfg_.max_gain_db / 20.0f);
        limiter_threshold_lin_ = std::pow(10.0f, cfg_.limiter_threshold / 20.0f) * 32767.0f;

        float frame_ms = 1000.0f * 160.0f / cfg_.sample_rate;
        attack_coef_   = std::exp(-frame_ms / cfg_.attack_ms);
        release_coef_  = std::exp(-frame_ms / cfg_.release_ms);

        /* Limiter release: 50ms sample-a-sample */
        float sample_ms       = 1000.0f / cfg_.sample_rate;
        limiter_release_coef_ = std::exp(-sample_ms / 50.0f);

        reset();
    }

    void process(int16_t* buf, int n_samples) {
        if (n_samples <= 0) return;
        static constexpr int FRAME = 160;
        int offset = 0;
        while (offset < n_samples) {
            int chunk = std::min(FRAME, n_samples - offset);
            process_chunk(buf + offset, chunk);
            offset += chunk;
        }
    }

    float gain_db() const {
        if (current_gain_ <= 0.0f) return -96.0f;
        return 20.0f * std::log10(current_gain_);
    }

    void reset() {
        current_gain_ = 1.0f;
        limiter_gain_ = 1.0f;
    }

private:
    Config cfg_;
    float current_gain_          = 1.0f;
    float limiter_gain_          = 1.0f;

    float target_rms_linear_     = 0.0f;
    float max_gain_linear_       = 0.0f;
    float attack_coef_           = 0.0f;
    float release_coef_          = 0.0f;
    float limiter_threshold_lin_ = 0.0f;
    float limiter_release_coef_  = 0.0f;

    void process_chunk(int16_t* buf, int n) {
        /* ── Estágio 1: AGC (por frame) ── */
        float rms = measure_rms(buf, n);
        if (rms > 1.0f) {
            float desired = target_rms_linear_ / rms;
            desired = std::min(desired, max_gain_linear_);
            desired = std::max(desired, 1.0f / max_gain_linear_);
            float coef = (desired < current_gain_) ? attack_coef_ : release_coef_;
            current_gain_ = current_gain_ * coef + desired * (1.0f - coef);
        }

        /* ── Estágio 2: Peak Limiter (sample a sample) ── */
        for (int i = 0; i < n; ++i) {
            float s = buf[i] * current_gain_;

            float peak = std::abs(s);
            if (peak > limiter_threshold_lin_) {
                /* Attack instantâneo: aplica ganho mínimo necessário imediatamente */
                float needed = limiter_threshold_lin_ / peak;
                if (needed < limiter_gain_)
                    limiter_gain_ = needed;
            } else {
                /* Release suave quando sinal volta ao normal */
                limiter_gain_ = limiter_gain_ * limiter_release_coef_
                                + 1.0f * (1.0f - limiter_release_coef_);
                if (limiter_gain_ > 1.0f) limiter_gain_ = 1.0f;
            }

            s *= limiter_gain_;
            s = std::clamp(s, -32767.0f, 32767.0f);
            buf[i] = static_cast<int16_t>(s);
        }
    }

    static float measure_rms(const int16_t* buf, int n) {
        if (n == 0) return 0.0f;
        double sum = 0.0;
        for (int i = 0; i < n; ++i)
            sum += static_cast<double>(buf[i]) * buf[i];
        return static_cast<float>(std::sqrt(sum / n));
    }
};