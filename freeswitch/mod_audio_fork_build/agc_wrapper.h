/*
 * agc_wrapper.h — Interface C para o AgcLimiter (C++)
 * =====================================================
 * Inclua este header no mod_audio_fork.c (C puro).
 * Compile agc_wrapper.cpp separadamente com g++.
 */
#ifndef AGC_WRAPPER_H
#define AGC_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Handle opaco — uma instância por sessão/uuid */
typedef struct AgcHandle AgcHandle;

/* Cria uma instância do AGC com parâmetros padrão:
 *   target: -18 dBFS, max_gain: 24 dB, attack: 50ms, release: 300ms */
AgcHandle *agc_create(void);

/* Processa 'n_samples' amostras L16 in-place (amplifica/limita) */
void agc_process(AgcHandle *h, int16_t *buf, int n_samples);

/* Retorna ganho atual em dB (para logging/debug) */
float agc_gain_db(AgcHandle *h);

/* Reseta estado interno */
void agc_reset(AgcHandle *h);

/* Destrói instância */
void agc_destroy(AgcHandle *h);

#ifdef __cplusplus
}
#endif

#endif /* AGC_WRAPPER_H */
