/*
 * agc_wrapper.cpp — Implementação C++ da wrapper C para AgcLimiter
 * =================================================================
 * Compilar junto com mod_audio_fork como:
 *   g++ -c agc_wrapper.cpp -o agc_wrapper.o
 * E linkar: gcc ... agc_wrapper.o -lstdc++
 */
#include "agc_wrapper.h"
#include "agc_limiter.h"

struct AgcHandle {
    AgcLimiter agc;
};

extern "C" {

AgcHandle *agc_create(void) {
    AgcLimiter::Config cfg;
    cfg.target_rms_dbfs = -18.0f;
    cfg.max_gain_db     =  24.0f;
    cfg.attack_ms       =  50.0f;
    cfg.release_ms      = 300.0f;
    cfg.sample_rate     =  8000;  /* FreeSWITCH usa 8kHz internamente */
    return new AgcHandle{ AgcLimiter(cfg) };
}

void agc_process(AgcHandle *h, int16_t *buf, int n_samples) {
    if (!h) return;
    h->agc.process(buf, n_samples);
}

float agc_gain_db(AgcHandle *h) {
    if (!h) return 0.0f;
    return h->agc.gain_db();
}

void agc_reset(AgcHandle *h) {
    if (!h) return;
    h->agc.reset();
}

void agc_destroy(AgcHandle *h) {
    delete h;
}

} /* extern "C" */
