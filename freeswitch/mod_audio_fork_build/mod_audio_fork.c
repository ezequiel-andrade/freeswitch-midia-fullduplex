/*
 * mod_audio_fork.c — Full-Duplex Audio Fork (read_frame loop como clock de mídia)
 * ======================================================================
 *
 * FIXES ORIGINAIS (mantidos):
 *   [FIX #1] Dialplan usa app "audio_fork" (não API uuid_audio_fork).
 *   [FIX #3] mod_audio_fork_shutdown(): join de todas as threads LWS antes
 *            de destruir o pool do módulo.
 *
 * MELHORIAS NOVAS:
 *   [M2] Reconexão automática configurável via parâmetro max_reconnects.
 *        Dialplan: <action application="audio_fork"
 *                    data="ws://127.0.0.1:8000/${uuid} max_reconnects=5"/>
 *        0 = sem reconexão (comportamento original, default).
 *        -1 = reconexão infinita.
 *        N  = até N tentativas antes de hangup.
 *
 *   [M5] API de métricas: uuid_audio_fork_stats <uuid>
 *        Retorna JSON com tx_underruns, tx_ring_drops, sq_drops,
 *        ws_reconnects, rx_frag_bytes — úteis para monitoramento Prometheus.
 *
 *   [AP_EVENT_RECONNECTING] Novo evento: loga tentativa de reconexão sem hangup.
 *
 * ARQUITETURA DE THREADS:
 *
 *   Thread 1 — Dialplan (app audio_fork):
 *     switch_core_session_read_frame() loop → clock de mídia RTP
 *     → READ_REPLACE e WRITE_REPLACE a cada 20ms
 *
 *   Thread 2 — LWS service (por sessão):
 *     ap_service() em loop de 5ms
 *     → acorda por lws_cancel_service() quando há frames RX
 *     → verifica timer de reconexão em WAIT_CANCELLED [M2]
 *
 *   Thread RTP (FreeSWITCH core):
 *     READ_REPLACE  → ap_on_rx_frame() → upsample → sq_push → WS → STT
 *     WRITE_REPLACE → ap_on_tx_frame() → ring.pop → RTP → usuário
 *
 * DIALPLAN:
 *   Sem reconexão (original):
 *     <action application="audio_fork" data="ws://127.0.0.1:8000/${uuid}"/>
 *
 *   Com reconexão (até 10 tentativas):
 *     <action application="audio_fork"
 *       data="ws://127.0.0.1:8000/${uuid} max_reconnects=10"/>
 *
 *   Reconexão infinita:
 *     <action application="audio_fork"
 *       data="ws://127.0.0.1:8000/${uuid} max_reconnects=-1"/>
 *
 * API auxiliar:
 *   uuid_audio_fork_stop  <uuid>         — encerra fork
 *   uuid_audio_fork_stats <uuid>         — JSON com métricas [M5]
 */

#include <switch.h>
#include "audio_pipe.h"

SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_audio_fork_shutdown);
SWITCH_MODULE_LOAD_FUNCTION(mod_audio_fork_load);
SWITCH_MODULE_DEFINITION(mod_audio_fork, mod_audio_fork_load,
                         mod_audio_fork_shutdown, NULL);

/* ─────────────────────────────────────────────────────────────────────────────
 * Constantes
 * ──────────────────────────────────────────────────────────────────────────── */
#define FS_FRAME_SAMPLES      160   /* 20ms @ 8kHz */
#define DEFAULT_MAX_RECONNECTS  0   /* 0 = sem reconexão (comportamento original) */

/* ─────────────────────────────────────────────────────────────────────────────
 * Estrutura de sessão
 * ──────────────────────────────────────────────────────────────────────────── */
typedef struct {
    char                  uuid[SWITCH_UUID_FORMATTED_LENGTH + 1];
    AudioPipe            *ap;
    switch_media_bug_t   *bug;
    switch_thread_t      *svc_thread;
    switch_atomic_t       running;
    switch_mutex_t       *mutex;
    switch_memory_pool_t *pool;
} fork_session_t;

static switch_hash_t         *sessions_hash  = NULL;
static switch_mutex_t        *sessions_mutex = NULL;
static switch_memory_pool_t  *module_pool    = NULL;

/* ─────────────────────────────────────────────────────────────────────────────
 * Thread LWS service
 * ──────────────────────────────────────────────────────────────────────────── */
static void *SWITCH_THREAD_FUNC lws_service_thread(switch_thread_t *t, void *obj)
{
    fork_session_t *fs = (fork_session_t *)obj;
    (void)t;

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] lws_service thread iniciado\n", fs->uuid);

    while (switch_atomic_read(&fs->running)) {
        ap_service(fs->ap, 5);
    }

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] lws_service thread encerrado\n", fs->uuid);
    return NULL;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * Event handler do AudioPipe
 * ──────────────────────────────────────────────────────────────────────────── */
static void ap_event_handler(AudioPipe *ap, AudioPipeEvent ev, void *user_data)
{
    fork_session_t *fs = (fork_session_t *)user_data;
    (void)ap;

    switch (ev) {

    case AP_EVENT_CONNECTED:
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                          "[%s] WebSocket conectado ao bot\n", fs->uuid);
        break;

    /* [M2] Reconexão em andamento — apenas loga, não faz hangup */
    case AP_EVENT_RECONNECTING:
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                          "[%s] WebSocket desconectado — tentando reconexão...\n",
                          fs->uuid);
        break;

    /* DISCONNECTED final: sem reconexão configurada OU tentativas esgotadas */
    case AP_EVENT_DISCONNECTED:
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] WebSocket encerrado — finalizando chamada\n",
                      fs->uuid);
    {
        switch_core_session_t *sw = switch_core_session_locate(fs->uuid);
        if (sw) {
            switch_channel_t *ch = switch_core_session_get_channel(sw);
            if (switch_channel_ready(ch)) {
                switch_channel_hangup(ch, SWITCH_CAUSE_DESTINATION_OUT_OF_ORDER);
            }
            switch_core_session_rwunlock(sw);
        }
    }
    break;
    }
}

/* ─────────────────────────────────────────────────────────────────────────────
 * Media Bug Callback — chamado pelo thread RTP, NUNCA bloqueia
 * ──────────────────────────────────────────────────────────────────────────── */
static switch_bool_t bug_callback(switch_media_bug_t *bug,
                                  void *user_data,
                                  switch_abc_type_t type)
{
    fork_session_t *fs = (fork_session_t *)user_data;
    switch_frame_t *frame;

    switch (type) {

    case SWITCH_ABC_TYPE_INIT:
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                          "[%s] Media bug inicializado (full-duplex)\n", fs->uuid);
        break;

    case SWITCH_ABC_TYPE_CLOSE:
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                          "[%s] Media bug fechado\n", fs->uuid);
        break;

    /* ── RX: voz do usuário → bot (STT) ──────────────────────────────────── */
    case SWITCH_ABC_TYPE_READ_REPLACE:
        frame = switch_core_media_bug_get_read_replace_frame(bug);
        if (!frame || !frame->data || frame->datalen == 0) break;
        if (!ap_is_connected(fs->ap)) break;
        ap_on_rx_frame(fs->ap,
                       (const int16_t *)frame->data,
                       (int)(frame->datalen / sizeof(int16_t)));
        break;

    /* ── TX: TTS do bot → RTP do usuário ─────────────────────────────────── */
    case SWITCH_ABC_TYPE_WRITE_REPLACE: {
        frame = switch_core_media_bug_get_write_replace_frame(bug);
        if (!frame || !frame->data) break;
        ap_on_tx_frame(fs->ap, (int16_t *)frame->data, FS_FRAME_SAMPLES);
        frame->datalen = FS_FRAME_SAMPLES * sizeof(int16_t);
        frame->samples = FS_FRAME_SAMPLES;
        switch_core_media_bug_set_write_replace_frame(bug, frame);
        break;
    }

    default:
        break;
    }

    return SWITCH_TRUE;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * parse_url_and_reconnects — separa URL do parâmetro max_reconnects
 *
 * Entrada:  "ws://host:port/path max_reconnects=10"
 * Saída:    url_out = "ws://host:port/path"  (copiado para buf_url)
 *           max_reconnects_out = 10
 *
 * Compatibilidade: se não houver max_reconnects=, comportamento original
 * (max_reconnects=0, sem reconexão).
 * ──────────────────────────────────────────────────────────────────────────── */
static void parse_url_and_reconnects(const char *data,
                                     char *buf_url, int buf_url_len,
                                     int *max_reconnects_out)
{
    *max_reconnects_out = DEFAULT_MAX_RECONNECTS;

    /* Copia dados para buffer mutável */
    char tmp[1024];
    strncpy(tmp, data, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    /* Procura por " max_reconnects=" */
    char *param = strstr(tmp, " max_reconnects=");
    if (param) {
        *param = '\0';  /* trunca URL no espaço */
        *max_reconnects_out = atoi(param + 16);  /* strlen(" max_reconnects=") = 16 */
    }

    strncpy(buf_url, tmp, buf_url_len - 1);
    buf_url[buf_url_len - 1] = '\0';
}

/* ─────────────────────────────────────────────────────────────────────────────
 * session_create
 * ──────────────────────────────────────────────────────────────────────────── */
static fork_session_t *session_create(switch_core_session_t *sw_session,
                                      const char *ws_url,
                                      int max_reconnects)
{
    const char *uuid = switch_core_session_get_uuid(sw_session);
    switch_channel_t *channel = switch_core_session_get_channel(sw_session);

    switch_memory_pool_t *pool;
    switch_core_new_memory_pool(&pool);

    fork_session_t *fs = (fork_session_t *)switch_core_alloc(pool, sizeof(*fs));
    memset(fs, 0, sizeof(*fs));
    strncpy(fs->uuid, uuid, SWITCH_UUID_FORMATTED_LENGTH);
    fs->pool = pool;
    switch_atomic_set(&fs->running, 1);
    switch_mutex_init(&fs->mutex, SWITCH_MUTEX_NESTED, pool);

    /* [N1] Campos fixos do FreeSWITCH */
    const char *caller = switch_channel_get_variable(channel, "caller_id_number");
    const char *dest   = switch_channel_get_variable(channel, "destination_number");
    const char *dir    = switch_channel_direction(channel) == SWITCH_CALL_DIRECTION_INBOUND
                           ? "inbound" : "outbound";

    /* Monta JSON base com campos fixos */
    char metadata_json[2048];
    int pos = switch_snprintf(
        metadata_json, sizeof(metadata_json),
        "{\"uuid\":\"%s\",\"direction\":\"%s\",\"caller\":\"%s\","
        "\"destination\":\"%s\",\"sip_sample_rate\":8000,\"ws_sample_rate\":16000",
        uuid    ? uuid   : "",
        dir,
        caller  ? caller : "",
        dest    ? dest   : ""
    );

    /* [N1] Campos dinâmicos: variáveis de canal com prefixo "af_meta_" */
    switch_event_t *vars = NULL;
    if (switch_channel_get_variables(channel, &vars) == SWITCH_STATUS_SUCCESS && vars) {
        switch_event_header_t *h;
        for (h = vars->headers; h && pos < (int)sizeof(metadata_json) - 64; h = h->next) {
            if (!h->name || strncmp(h->name, "af_meta_", 8) != 0) continue;
            pos += switch_snprintf(metadata_json + pos,
                                   sizeof(metadata_json) - pos,
                                   ",\"%s\":\"%s\"",
                                   h->name + 8,           /* remove prefixo */
                                   h->value ? h->value : "");
        }
        switch_event_destroy(&vars);
    }

    /* Fecha o JSON */
    if (pos < (int)sizeof(metadata_json) - 2)
        metadata_json[pos++] = '}';
    metadata_json[pos] = '\0';

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] metadata_json=%s\n", uuid, metadata_json);

    /* [N2] Headers HTTP extras via variável de canal "af_extra_headers" */
    const char *extra_headers = switch_channel_get_variable(channel, "af_extra_headers");

    /* [N1/N2/M2] monta config para ap_create */
    AudioPipeConfig cfg = {0};
    cfg.metadata_json          = metadata_json;
    cfg.extra_headers          = extra_headers;   /* NULL se não configurado */
    cfg.max_reconnect_attempts = max_reconnects;

    fs->ap = ap_create(ws_url, ap_event_handler, fs, &cfg);
    if (!fs->ap) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                          "[%s] ap_create falhou para %s\n", uuid, ws_url);
        switch_core_destroy_memory_pool(&pool);
        return NULL;
    }

    switch_mutex_lock(sessions_mutex);
    switch_core_hash_insert(sessions_hash, uuid, fs);
    switch_mutex_unlock(sessions_mutex);

    switch_threadattr_t *ta;
    switch_threadattr_create(&ta, pool);
    switch_threadattr_detach_set(ta, 0);  /* joinable */
    switch_thread_create(&fs->svc_thread, ta, lws_service_thread, fs, pool);

    switch_status_t st = switch_core_media_bug_add(
        sw_session,
        "audio_fork_fullduplex",
        NULL,
        bug_callback,
        fs,
        0,
        SMBF_READ_REPLACE | SMBF_WRITE_REPLACE | SMBF_NO_PAUSE,
        &fs->bug
    );

    if (st != SWITCH_STATUS_SUCCESS) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                          "[%s] switch_core_media_bug_add falhou\n", uuid);
        switch_atomic_set(&fs->running, 0);
        if (fs->svc_thread) {
            switch_status_t ignored;
            switch_thread_join(&ignored, fs->svc_thread);
        }
        ap_destroy(fs->ap);
        switch_mutex_lock(sessions_mutex);
        switch_core_hash_delete(sessions_hash, uuid);
        switch_mutex_unlock(sessions_mutex);
        switch_core_destroy_memory_pool(&pool);
        return NULL;
    }

    return fs;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * session_destroy — ordem crítica: join LWS → remove bug → destroy ap → pool
 * ──────────────────────────────────────────────────────────────────────────── */
static void session_destroy(fork_session_t *fs, switch_core_session_t *sw_session)
{
    const char *uuid = fs->uuid;

    /* 1. Sinaliza parada para a LWS thread */
    switch_atomic_set(&fs->running, 0); /* ← para o loop da LWS thread */

    /* 2. Acorda a LWS thread IMEDIATAMENTE via lws_cancel_service().
     *
     * Sem isso, a thread fica bloqueada até o próximo timeout de ap_service()
     * (5ms por iteração), mas na prática o LWS pode estar aguardando eventos
     * de rede internos (ex: TCP FIN do bot, timeout de keepalive) que podem
     * demorar segundos — causando o teardown lento de 15s observado nos logs.
     *
     * lws_cancel_service() é thread-safe: usa um pipe/eventfd interno do LWS
     * para acordar o select/epoll da thread de serviço imediatamente.
     * A thread acorda, entra em LWS_CALLBACK_EVENT_WAIT_CANCELLED, checa
     * switch_atomic_read(&fs->running) == 0 via ap_service(), e sai do loop.
     *
     * Acesso a fs->ap->lws_ctx é seguro aqui porque:
     *   - fs->ap só é destruído no passo 5, após o join
     *   - lws_ctx só é destruído dentro de ap_destroy() — ainda não ocorreu
     */
    ap_set_closing(fs->ap);              /* closing=true ANTES do cancel */
    ap_cancel_service(fs->ap);           /* acorda a thread imediatamente */

    /* 3. Aguarda a LWS thread encerrar (join) */
    if (fs->svc_thread) {
        switch_status_t ignored;
        switch_thread_join(&ignored, fs->svc_thread);
        fs->svc_thread = NULL;
    }

    /* 4. Remove media bug (para callbacks do thread RTP) */
    if (sw_session && fs->bug) {
        switch_core_media_bug_remove(sw_session, &fs->bug);
        fs->bug = NULL;
    }

    /* 5. Remove da tabela global */
    switch_mutex_lock(sessions_mutex);
    switch_core_hash_delete(sessions_hash, uuid);
    switch_mutex_unlock(sessions_mutex);

    /* 6. Destrói AudioPipe ([M6] flush WS CLOSE correto) */
    ap_destroy(fs->ap);
    fs->ap = NULL;

    /* 7. Pool por último */
    switch_core_destroy_memory_pool(&fs->pool);

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] Sessão destruída\n", uuid);
}

/* ─────────────────────────────────────────────────────────────────────────────
 * APLICAÇÃO DE DIALPLAN — audio_fork
 * ──────────────────────────────────────────────────────────────────────────── */
SWITCH_STANDARD_APP(audio_fork_app)
{
    switch_channel_t *channel = switch_core_session_get_channel(session);
    const char       *uuid    = switch_core_session_get_uuid(session);

    if (!data || !*(char *)data) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                          "[%s] audio_fork: ws-url não fornecida\n", uuid);
        return;
    }

    /* Verifica fork duplicado */
    switch_mutex_lock(sessions_mutex);
    int already = (switch_core_hash_find(sessions_hash, uuid) != NULL);
    switch_mutex_unlock(sessions_mutex);

    if (already) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                          "[%s] audio_fork: fork já ativo\n", uuid);
        return;
    }

    /* [M2] Parse URL e max_reconnects do parâmetro data */
    char    ws_url[1024];
    int     max_reconnects = DEFAULT_MAX_RECONNECTS;
    parse_url_and_reconnects((const char *)data, ws_url, sizeof(ws_url), &max_reconnects);

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] audio_fork iniciando → %s (max_reconnects=%d)\n",
                      uuid, ws_url, max_reconnects);

    fork_session_t *fs = session_create(session, ws_url, max_reconnects);
    if (!fs) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                          "[%s] audio_fork: falha ao criar sessão\n", uuid);
        return;
    }

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] audio_fork: entrando no read_frame loop\n", uuid);

    /* ════════════════════════════════════════════════════════════════════════
     * CLOCK DE MÍDIA — Loop canônico
     * ════════════════════════════════════════════════════════════════════════ */
    uint8_t        write_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
    switch_frame_t write_frame = {0};
    write_frame.data    = write_buf;
    write_frame.buflen  = SWITCH_RECOMMENDED_BUFFER_SIZE;
    write_frame.datalen = FS_FRAME_SAMPLES * sizeof(int16_t);
    write_frame.samples = FS_FRAME_SAMPLES;
    write_frame.codec   = switch_core_session_get_write_codec(session);

    switch_frame_t *read_frame;
    while (switch_channel_ready(channel)) {
        switch_status_t st = switch_core_session_read_frame(
            session, &read_frame, SWITCH_IO_FLAG_NONE, 0);

        if (!SWITCH_READ_ACCEPTABLE(st)) break;

        /* [FIX: Linphone re-INVITE] Refresha codec a cada iteração */
        write_frame.codec   = switch_core_session_get_write_codec(session);
        write_frame.datalen = FS_FRAME_SAMPLES * sizeof(int16_t);
        write_frame.samples = FS_FRAME_SAMPLES;
        memset(write_buf, 0, write_frame.datalen);
        switch_core_session_write_frame(session, &write_frame, SWITCH_IO_FLAG_NONE, 0);
    }

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "[%s] audio_fork: loop encerrado, teardown\n", uuid);

    session_destroy(fs, session);
}

/* ─────────────────────────────────────────────────────────────────────────────
 * API: uuid_audio_fork_stop <uuid>
 * ──────────────────────────────────────────────────────────────────────────── */
SWITCH_STANDARD_API(uuid_audio_fork_stop_function)
{
    if (!cmd || !*cmd) {
        stream->write_function(stream, "-ERR uso: uuid_audio_fork_stop <uuid>\n");
        return SWITCH_STATUS_SUCCESS;
    }

    switch_mutex_lock(sessions_mutex);
    fork_session_t *fs = (fork_session_t *)switch_core_hash_find(sessions_hash, cmd);
    switch_mutex_unlock(sessions_mutex);

    if (!fs) {
        stream->write_function(stream, "-ERR fork não encontrado para %s\n", cmd);
        return SWITCH_STATUS_SUCCESS;
    }

    switch_core_session_t *sw_session = switch_core_session_locate(cmd);
    if (sw_session) {
        switch_channel_hangup(switch_core_session_get_channel(sw_session),
                              SWITCH_CAUSE_NORMAL_CLEARING);
        switch_core_session_rwunlock(sw_session);
    }

    stream->write_function(stream, "+OK sinal de stop enviado para %s\n", cmd);
    return SWITCH_STATUS_SUCCESS;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * [M5] API: uuid_audio_fork_stats <uuid>
 *
 * Retorna snapshot JSON das métricas do AudioPipe para a sessão indicada.
 * Útil para monitoramento externo (Prometheus, Grafana, alertas).
 *
 * Saída:
 *   +OK {"uuid":"...","tx_underruns":N,"tx_ring_drops":N,"sq_drops":N,
 *        "ws_reconnects":N,"rx_frag_bytes":N,"jitter_frames":N}
 * ──────────────────────────────────────────────────────────────────────────── */
SWITCH_STANDARD_API(uuid_audio_fork_stats_function)
{
    if (!cmd || !*cmd) {
        stream->write_function(stream, "-ERR uso: uuid_audio_fork_stats <uuid>\n");
        return SWITCH_STATUS_SUCCESS;
    }

    switch_mutex_lock(sessions_mutex);
    fork_session_t *fs = (fork_session_t *)switch_core_hash_find(sessions_hash, cmd);
    switch_mutex_unlock(sessions_mutex);

    if (!fs || !fs->ap) {
        stream->write_function(stream, "-ERR sessão não encontrada: %s\n", cmd);
        return SWITCH_STATUS_SUCCESS;
    }

    AudioPipeStats s = ap_get_stats(fs->ap);
    stream->write_function(stream,
        "+OK {\"uuid\":\"%s\","
        "\"connected\":%s,"
        "\"tx_underruns\":%" SWITCH_UINT64_T_FMT ","
        "\"tx_ring_drops\":%" SWITCH_UINT64_T_FMT ","
        "\"sq_drops\":%" SWITCH_UINT64_T_FMT ","
        "\"ws_reconnects\":%" SWITCH_UINT64_T_FMT ","
        "\"rx_frag_bytes\":%" SWITCH_UINT64_T_FMT "}\n",
        fs->uuid,
        ap_is_connected(fs->ap) ? "true" : "false",
        (uint64_t)s.tx_underruns,
        (uint64_t)s.tx_ring_drops,
        (uint64_t)s.sq_drops,
        (uint64_t)s.ws_reconnects,
        (uint64_t)s.rx_frag_bytes
    );

    return SWITCH_STATUS_SUCCESS;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * Lifecycle
 * ──────────────────────────────────────────────────────────────────────────── */
SWITCH_MODULE_LOAD_FUNCTION(mod_audio_fork_load)
{
    *module_interface = switch_loadable_module_create_module_interface(pool, modname);

    switch_core_new_memory_pool(&module_pool);
    switch_mutex_init(&sessions_mutex, SWITCH_MUTEX_NESTED, module_pool);
    switch_core_hash_init(&sessions_hash);

    switch_application_interface_t *app_interface;
    SWITCH_ADD_APP(app_interface,
                   "audio_fork",
                   "Full-duplex audio fork via WebSocket (read_frame loop)",
                   "audio_fork <ws-url> [max_reconnects=N]",
                   audio_fork_app,
                   "<ws-url> [max_reconnects=N]",
                   SAF_NONE);

    switch_api_interface_t *api_interface;
    SWITCH_ADD_API(api_interface,
                   "uuid_audio_fork_stop",
                   "Stop audio fork for a channel",
                   uuid_audio_fork_stop_function,
                   "<uuid>");

    SWITCH_ADD_API(api_interface,
                   "uuid_audio_fork_stats",
                   "Get audio fork metrics for a channel (JSON)",
                   uuid_audio_fork_stats_function,
                   "<uuid>");

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "mod_audio_fork (full-duplex, read_frame loop) carregado\n");
    return SWITCH_STATUS_SUCCESS;
}

/* ─────────────────────────────────────────────────────────────────────────────
 * [FIX #3] mod_audio_fork_shutdown
 * ──────────────────────────────────────────────────────────────────────────── */
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_audio_fork_shutdown)
{
#define MAX_SESSIONS_SHUTDOWN 512
    fork_session_t *to_stop[MAX_SESSIONS_SHUTDOWN];
    int             nstop = 0;

    switch_mutex_lock(sessions_mutex);
    for (switch_hash_index_t *hi = switch_core_hash_first(sessions_hash);
         hi;
         hi = switch_core_hash_next(&hi))
    {
        void *val = NULL;
        switch_core_hash_this(hi, NULL, NULL, &val);
        if (val && nstop < MAX_SESSIONS_SHUTDOWN)
            to_stop[nstop++] = (fork_session_t *)val;
    }
    switch_mutex_unlock(sessions_mutex);

    for (int i = 0; i < nstop; i++) {
        fork_session_t *fs = to_stop[i];
        switch_atomic_set(&fs->running, 0);
        if (fs->svc_thread) {
            switch_status_t ignored;
            switch_thread_join(&ignored, fs->svc_thread);
            fs->svc_thread = NULL;
        }
    }

    switch_mutex_lock(sessions_mutex);
    switch_core_hash_destroy(&sessions_hash);
    switch_mutex_unlock(sessions_mutex);

    switch_core_destroy_memory_pool(&module_pool);

    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                      "mod_audio_fork (full-duplex) descarregado\n");
    return SWITCH_STATUS_SUCCESS;
}
