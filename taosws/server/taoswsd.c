/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "cjson/cJSON.h"
#include "libwebsockets.h"
#include "taos.h"
#include "taoserror.h"

#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SAFE_FREE(x) do {      \
  if (x) {                     \
    free(x);                   \
    x = NULL;                  \
  }                            \
} while (0)

#define HDR_FMT "%s[%d]:%s():"
#define HDR_VAL basename((char*)__FILE__), __LINE__, __func__

#define D(fmt, ...) lwsl_notice(HDR_FMT fmt "\n", HDR_VAL, ##__VA_ARGS__)
#define A(sm, fmt, ...) do {                               \
  if (sm) break;                                           \
  lwsl_err(HDR_FMT fmt "\n", HDR_VAL, ##__VA_ARGS__);      \
  abort();                                                 \
} while (0)

#define taosws_wsi_respond_err(wsi, session, fmt, ...) do {                    \
  char _buf[4096];                                                             \
  int _n = snprintf(_buf, sizeof(_buf), HDR_FMT fmt, HDR_VAL, ##__VA_ARGS__);  \
  if (_n<=0) break;                                                            \
  taosws_wsi_respond_err_impl(wsi, session, _buf, _n);                         \
} while (0)

#define taosws_wsi_respond_ok(wsi, session, fmt, ...) do {                     \
  char _buf[4096];                                                             \
  int _n = snprintf(_buf, sizeof(_buf), HDR_FMT fmt, HDR_VAL, ##__VA_ARGS__);  \
  if (_n<=0) break;                                                            \
  taosws_wsi_respond_ok_impl(wsi, session, _buf, _n);                          \
} while (0)

typedef struct taosws_session_s                taosws_session_t;
typedef struct taosws_task_s                   taosws_task_t;
typedef void (*task_routine)(taosws_task_t *task);

typedef enum taosws_task_state_e {
  TASK_STATE_NONE,
  TASK_STATE_PENDING,
  TASK_STATE_WORKING,
  TASK_STATE_WORKING_DONE,
  TASK_STATE_DONE,
} taosws_task_state_t;

struct taosws_task_s {
  lws_dll2_t                     node;

  taosws_session_t              *session;

  task_routine                   on_work;
  task_routine                   on_done;
  task_routine                   on_release;

  taosws_task_state_t            state;
};

typedef struct taosws_queue_tasks_s            taosws_queue_tasks_t;
struct taosws_queue_tasks_s {
  lws_dll2_owner_t     tasks;
};

typedef struct taosws_vhost_data_s             taosws_vhost_data_t;
typedef struct taosws_queue_workers_s          taosws_queue_workers_t;
struct taosws_queue_workers_s {
  taosws_vhost_data_t      *vhd;

  pthread_mutex_t           mutex;
  pthread_cond_t            cond;

  pthread_t                *workers;
  size_t                    cap;
  size_t                    nr;

  taosws_queue_tasks_t      pendings;
  taosws_queue_tasks_t      done;

  volatile uint8_t     stop;
};

struct taosws_vhost_data_s {
  struct lws_context         *context;

  taosws_queue_workers_t      workers;
};

typedef struct foo_task_s              foo_task_t;
struct foo_task_s {
  taosws_task_t             task;

  int                       secs_to_sleep;
};

typedef struct taos_conn_task_s        taos_conn_task_t;
struct taos_conn_task_s {
  taosws_task_t             task;

  TAOS                     *taos;
  int                       e;
  char                      errstr[1024];
};

struct taosws_session_s {
  struct lws            *wsi;

  char                 **buffers;
  size_t                 cap;
  size_t                 nr;

  char                  *recv_buf;
  size_t                 recv_cap;
  size_t                 recv_nr;

  char                  *ip;
  char                  *user;
  char                  *password;
  char                  *db;
  uint16_t               port;

  union {
    foo_task_t           foo_task;
    taos_conn_task_t     conn_task;
  };
  taosws_task_t         *task;

  TAOS                  *taos;

  uint8_t                init:1;
  uint8_t                terminating:1;
};

static const char* _lws_callback_reason_name(enum lws_callback_reasons reason)
{
#define CASE(x) case x: return #x
  switch (reason) {
    CASE(LWS_CALLBACK_PROTOCOL_INIT);
    CASE(LWS_CALLBACK_PROTOCOL_DESTROY);
    CASE(LWS_CALLBACK_WSI_CREATE);
    CASE(LWS_CALLBACK_WSI_DESTROY);
    CASE(LWS_CALLBACK_WSI_TX_CREDIT_GET);
    CASE(LWS_CALLBACK_OPENSSL_LOAD_EXTRA_CLIENT_VERIFY_CERTS);
    CASE(LWS_CALLBACK_OPENSSL_LOAD_EXTRA_SERVER_VERIFY_CERTS);
    CASE(LWS_CALLBACK_OPENSSL_PERFORM_CLIENT_CERT_VERIFICATION);
    CASE(LWS_CALLBACK_OPENSSL_CONTEXT_REQUIRES_PRIVATE_KEY);
    CASE(LWS_CALLBACK_SSL_INFO);
    CASE(LWS_CALLBACK_OPENSSL_PERFORM_SERVER_CERT_VERIFICATION);
    CASE(LWS_CALLBACK_SERVER_NEW_CLIENT_INSTANTIATED);
    CASE(LWS_CALLBACK_HTTP);
    CASE(LWS_CALLBACK_HTTP_BODY);
    CASE(LWS_CALLBACK_HTTP_BODY_COMPLETION);
    CASE(LWS_CALLBACK_HTTP_FILE_COMPLETION);
    CASE(LWS_CALLBACK_HTTP_WRITEABLE);
    CASE(LWS_CALLBACK_CLOSED_HTTP);
    CASE(LWS_CALLBACK_FILTER_HTTP_CONNECTION);
    CASE(LWS_CALLBACK_ADD_HEADERS);
    CASE(LWS_CALLBACK_VERIFY_BASIC_AUTHORIZATION);
    CASE(LWS_CALLBACK_CHECK_ACCESS_RIGHTS);
    CASE(LWS_CALLBACK_PROCESS_HTML);
    CASE(LWS_CALLBACK_HTTP_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_HTTP_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_HTTP_CONFIRM_UPGRADE);
    CASE(LWS_CALLBACK_ESTABLISHED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_CLOSED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_RECEIVE_CLIENT_HTTP_READ);
    CASE(LWS_CALLBACK_RECEIVE_CLIENT_HTTP);
    CASE(LWS_CALLBACK_COMPLETED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_CLIENT_HTTP_WRITEABLE);
    CASE(LWS_CALLBACK_CLIENT_HTTP_REDIRECT);
    CASE(LWS_CALLBACK_CLIENT_HTTP_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_CLIENT_HTTP_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_ESTABLISHED);
    CASE(LWS_CALLBACK_CLOSED);
    CASE(LWS_CALLBACK_SERVER_WRITEABLE);
    CASE(LWS_CALLBACK_RECEIVE);
    CASE(LWS_CALLBACK_RECEIVE_PONG);
    CASE(LWS_CALLBACK_WS_PEER_INITIATED_CLOSE);
    CASE(LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION);
    CASE(LWS_CALLBACK_CONFIRM_EXTENSION_OKAY);
    CASE(LWS_CALLBACK_WS_SERVER_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_WS_SERVER_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_CLIENT_CONNECTION_ERROR);
    CASE(LWS_CALLBACK_CLIENT_FILTER_PRE_ESTABLISH);
    CASE(LWS_CALLBACK_CLIENT_ESTABLISHED);
    CASE(LWS_CALLBACK_CLIENT_CLOSED);
    CASE(LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER);
    CASE(LWS_CALLBACK_CLIENT_RECEIVE);
    CASE(LWS_CALLBACK_CLIENT_RECEIVE_PONG);
    CASE(LWS_CALLBACK_CLIENT_WRITEABLE);
    CASE(LWS_CALLBACK_CLIENT_CONFIRM_EXTENSION_SUPPORTED);
    CASE(LWS_CALLBACK_WS_EXT_DEFAULTS);
    CASE(LWS_CALLBACK_FILTER_NETWORK_CONNECTION);
    CASE(LWS_CALLBACK_WS_CLIENT_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_WS_CLIENT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_GET_THREAD_ID);
    CASE(LWS_CALLBACK_ADD_POLL_FD);
    CASE(LWS_CALLBACK_DEL_POLL_FD);
    CASE(LWS_CALLBACK_CHANGE_MODE_POLL_FD);
    CASE(LWS_CALLBACK_LOCK_POLL);
    CASE(LWS_CALLBACK_UNLOCK_POLL);
    CASE(LWS_CALLBACK_CGI);
    CASE(LWS_CALLBACK_CGI_TERMINATED);
    CASE(LWS_CALLBACK_CGI_STDIN_DATA);
    CASE(LWS_CALLBACK_CGI_STDIN_COMPLETED);
    CASE(LWS_CALLBACK_CGI_PROCESS_ATTACH);
    CASE(LWS_CALLBACK_SESSION_INFO);
    CASE(LWS_CALLBACK_GS_EVENT);
    CASE(LWS_CALLBACK_HTTP_PMO);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_RX);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_RX);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_CLOSE);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_CLOSE);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_ADOPT);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_ADOPT);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_RX);
    CASE(LWS_CALLBACK_RAW_CLOSE);
    CASE(LWS_CALLBACK_RAW_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_ADOPT);
    CASE(LWS_CALLBACK_RAW_CONNECTED);
    CASE(LWS_CALLBACK_RAW_SKT_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_SKT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_ADOPT_FILE);
    CASE(LWS_CALLBACK_RAW_RX_FILE);
    CASE(LWS_CALLBACK_RAW_WRITEABLE_FILE);
    CASE(LWS_CALLBACK_RAW_CLOSE_FILE);
    CASE(LWS_CALLBACK_RAW_FILE_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_FILE_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_TIMER);
    CASE(LWS_CALLBACK_EVENT_WAIT_CANCELLED);
    CASE(LWS_CALLBACK_CHILD_CLOSING);
    CASE(LWS_CALLBACK_CONNECTING);
    CASE(LWS_CALLBACK_VHOST_CERT_AGING);
    CASE(LWS_CALLBACK_VHOST_CERT_UPDATE);
    CASE(LWS_CALLBACK_MQTT_NEW_CLIENT_INSTANTIATED);
    CASE(LWS_CALLBACK_MQTT_IDLE);
    CASE(LWS_CALLBACK_MQTT_CLIENT_ESTABLISHED);
    CASE(LWS_CALLBACK_MQTT_SUBSCRIBED);
    CASE(LWS_CALLBACK_MQTT_CLIENT_WRITEABLE);
    CASE(LWS_CALLBACK_MQTT_CLIENT_RX);
    CASE(LWS_CALLBACK_MQTT_UNSUBSCRIBED);
    CASE(LWS_CALLBACK_MQTT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_MQTT_CLIENT_CLOSED);
    CASE(LWS_CALLBACK_MQTT_ACK);
    CASE(LWS_CALLBACK_MQTT_RESEND);
    CASE(LWS_CALLBACK_MQTT_UNSUBSCRIBE_TIMEOUT);
    CASE(LWS_CALLBACK_MQTT_SHADOW_TIMEOUT);
    CASE(LWS_CALLBACK_USER);
    default:
      return "LWS_CALLBACK_unknown";
  }
#undef CASE
}

static void taosws_queue_tasks_append(taosws_queue_tasks_t *tasks, taosws_task_t *task)
{
  lws_dll2_add_tail(&task->node, &tasks->tasks);
}

static taosws_task_t* taosws_queue_tasks_remove(taosws_queue_tasks_t *tasks)
{
  lws_dll2_t *p = lws_dll2_get_head(&tasks->tasks);
  if (!p) return NULL;
  lws_dll2_remove(p);

  return lws_container_of(p, taosws_task_t, node);
}

static volatile int interrupted = 0;

static void task_on_release(taosws_task_t *task)
{
  A(task->state == TASK_STATE_DONE, "internal logic error");

  task->on_release(task);
  task->state = TASK_STATE_NONE;
  task->session->task = NULL;
}

static void _worker_routine(taosws_queue_workers_t *workers)
{
  while (!interrupted) {
    pthread_cond_wait(&workers->cond, &workers->mutex);
    if (workers->stop) break;
    // TODO:

    taosws_task_t *task = taosws_queue_tasks_remove(&workers->pendings);
    if (!task) continue;

    A(task->state == TASK_STATE_PENDING, "internal logic error");
    task->state = TASK_STATE_WORKING;

    pthread_mutex_unlock(&workers->mutex);

    task->on_work(task);

    pthread_mutex_lock(&workers->mutex);

    if (!interrupted) {
      A(task->state == TASK_STATE_WORKING, "internal logic error");
      task->state = TASK_STATE_WORKING_DONE;
      taosws_queue_tasks_append(&workers->done, task);
    } else {
      A(task->state == TASK_STATE_WORKING, "internal logic error");
      task->state = TASK_STATE_DONE;

      task_on_release(task);
    }

    A(workers, "internal logic error");
    A(workers->vhd, "internal logic error");
    A(workers->vhd->context, "internal logic error");

    lws_cancel_service(workers->vhd->context);
  }

  while (1) {
    taosws_task_t *task = taosws_queue_tasks_remove(&workers->pendings);
    if (!task) break;

    A(task->state == TASK_STATE_PENDING, "internal logic error");
    task->state = TASK_STATE_DONE;

    struct lws *wsi = task->session->wsi;

    task_on_release(task);

    lws_rx_flow_control(wsi, 1);
    lws_callback_on_writable(wsi);
  }
}

static void* worker_routine(void *arg)
{
  taosws_queue_workers_t *workers = (taosws_queue_workers_t*)arg;

  pthread_mutex_lock(&workers->mutex);

  _worker_routine(workers);

  pthread_mutex_unlock(&workers->mutex);

  return NULL;
}

static void taosws_queue_workers_init(taosws_queue_workers_t *workers, size_t cap)
{
  int r = 0;

  if (!workers) return;

  if (pthread_mutex_init(&workers->mutex, NULL)) {
    A(0, "pthread_mutex_init failed");
  }
  if (pthread_cond_init(&workers->cond, NULL)) {
    A(0, "pthread_cond_init failed");
  }

  workers->cap = cap;
  workers->workers = (pthread_t*)calloc(workers->cap, sizeof(*workers->workers));
  A(workers->workers, "out of memory");

  for (size_t i=0; i<workers->cap; ++i) {
    r = pthread_create(workers->workers + i, NULL, worker_routine, workers);
    if (r) break;
    workers->nr += 1;
  }

  A(workers->nr, "out of memory");
}

static void taosws_queue_workers_stop(taosws_queue_workers_t *workers)
{
  if (!workers) return;
  if (workers->stop) return;

  workers->stop     = 1;

  // pthread_mutex_lock(&workers->mutex);
  for (size_t i=0; i<workers->nr; ++i) {
    pthread_cond_signal(&workers->cond);
  }
  // pthread_mutex_unlock(&workers->mutex);

  for (size_t i=0; i<workers->nr; ++i) {
    pthread_join(workers->workers[i], NULL);
  }

  SAFE_FREE(workers->workers);
}

static int _post_task(taosws_queue_workers_t *workers, taosws_task_t *task)
{
  if (workers->stop) return -1;

  A(task->state == TASK_STATE_NONE, "internal logic error");
  task->state = TASK_STATE_PENDING;

  taosws_queue_tasks_append(&workers->pendings, task);

  pthread_cond_signal(&workers->cond);

  return 0;
}

static int taosws_queue_workers_post_task(taosws_queue_workers_t *workers, taosws_task_t *task)
{
  int r = 0;
  pthread_mutex_lock(&workers->mutex);
  r = _post_task(workers, task);
  pthread_mutex_unlock(&workers->mutex);

  return r ? -1 : 0;
}

static void _taosws_queue_workers_wakeup_done_tasks(taosws_queue_workers_t *workers)
{
  while (1) {
    taosws_task_t *task = taosws_queue_tasks_remove(&workers->done);
    if (!task) break;

    A(task->state == TASK_STATE_WORKING_DONE, "internal logic error");
    task->state = TASK_STATE_DONE;

    task->on_done(task);

    lws_rx_flow_control(task->session->wsi, 1);
    lws_callback_on_writable(task->session->wsi);

    task_on_release(task);
  }
}

static void taosws_queue_workers_wakeup_done_tasks(taosws_queue_workers_t *workers)
{
  pthread_mutex_lock(&workers->mutex);
  _taosws_queue_workers_wakeup_done_tasks(workers);
  pthread_mutex_unlock(&workers->mutex);
}

static void taosws_vhost_data_init(taosws_vhost_data_t *vhd, struct lws *wsi)
{
  if (!vhd) return;

  vhd->context = lws_get_context(wsi);
  vhd->workers.vhd = vhd;

  taosws_queue_workers_init(&vhd->workers, 4);

  return;
}

static void taosws_vhost_data_release(taosws_vhost_data_t *vhd)
{
  if (!vhd) return;

  taosws_queue_workers_stop(&vhd->workers);

  return;
}

static int taosws_vhost_data_post_task(taosws_vhost_data_t *vhd, taosws_task_t *task)
{
  return taosws_queue_workers_post_task(&vhd->workers, task); 
}

static void taosws_vhost_data_wakeup_done_tasks(taosws_vhost_data_t *vhd)
{
  taosws_queue_workers_wakeup_done_tasks(&vhd->workers);
}

static int taosws_session_append_buf(taosws_session_t *session, const char *buf)
{
  if (!session) return 0;

  if (session->nr >= session->cap) {
    size_t cap = (session->nr + 1 + 15) / 16 * 16;
    char **buffers = (char**)realloc(session->buffers, cap * sizeof(*buffers));
    if (!buffers) {
      D("out of memory");
      return -1;
    }
    session->buffers = buffers;
    session->cap     = cap;
  }

  session->buffers[session->nr] = malloc(LWS_PRE + strlen(buf) + 1);
  if (!session->buffers[session->nr]) {
    D("out of memory");
    return -1;
  }

  strcpy(session->buffers[session->nr] + LWS_PRE, buf);
  session->nr += 1;

  return 0;
}

static int taosws_wsi_respond_json(struct lws *wsi, taosws_session_t *session, cJSON *json)
{
  do {
    char buf[4096]; buf[0] = '\0';
    if (!cJSON_PrintPreallocated(json, buf, sizeof(buf), 0)) {
      D("buffer is too small");
      break;
    }
    if (taosws_session_append_buf(session, buf)) break;

    lws_callback_on_writable(wsi);
    return 0;
  } while (0);

  return -1;
}

static int _gen_respond(int e, const char *errmsg, size_t len, cJSON **resp)
{
  A(errmsg[len] == '\0', "internal logic error");

  cJSON *json = cJSON_CreateObject();
  if (!json) {
    D("out of memory");
    return -1;
  }

  do {
    cJSON *v = cJSON_CreateObject();
    if (!v) {
      D("out of memory");
      break;
    }

    cJSON_AddItemToObject(json, "resp", v);

    cJSON_AddNumberToObject(v, "err", e);
    cJSON_AddStringToObject(v, "errmsg", errmsg);

    *resp = json;
    return 0;
  } while (0);

  if (json) cJSON_Delete(json);
  return -1;
}

static int taosws_wsi_respond_err_impl(struct lws *wsi, taosws_session_t *session, const char *errmsg, size_t len)
{
  (void)session;

  int r = 0;

  lwsl_err("%.*s\n", (int)len, errmsg);

  cJSON *json = NULL;

  r = _gen_respond(1, errmsg, len, &json);
  if (r) return -1;

  r = taosws_wsi_respond_json(wsi, session, json);

  cJSON_Delete(json);

  return r ? -1 : 0;
}

static int taosws_wsi_respond_ok_impl(struct lws *wsi, taosws_session_t *session, const char *msg, size_t len)
{
  (void)len;

  int r = 0;

  cJSON *json = NULL;

  r = _gen_respond(0, msg, len, &json);
  if (r) return -1;

  r = taosws_wsi_respond_json(wsi, session, json);

  cJSON_Delete(json);

  return r ? -1 : 0;
}

static int taosws_session_init(taosws_session_t *session, struct lws *wsi)
{
  if (!session || session->init) return 0;
  session->wsi = wsi;
  session->init = 1;

  char buf[1024]; buf[0] = '\0'; // FIXME: big enough?

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "port", buf, sizeof(buf)-1) >=0 ) {
    int bytes = 0;
    int port = 0;
    int n = sscanf(buf, "%d%n", &port, &bytes);
    if (n == 0 || buf[bytes] || port < 0 || port > UINT16_MAX) {
      taosws_wsi_respond_err(session->wsi, session, "%s", "geturlarg `?port` failure");
      return -1;
    }
    session->port = (uint16_t)port;
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "ip", buf, sizeof(buf)-1) >= 0 && buf[0]) {
    session->ip = strdup(buf);
    if (!session->ip) {
      taosws_wsi_respond_err(session->wsi, session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "user", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->user = strdup(buf);
    if (!session->user) {
      taosws_wsi_respond_err(session->wsi, session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "password", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->password = strdup(buf);
    if (!session->password) {
      taosws_wsi_respond_err(session->wsi, session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "db", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->db = strdup(buf);
    if (!session->db) {
      taosws_wsi_respond_err(session->wsi, session, "%s", "out of memory");
      return -1;
    }
  }

  taosws_wsi_respond_ok(session->wsi, session, "ip:%s;user:%s;password:%s;db:%s;port:%d", session->ip, session->user, session->password, session->db, session->port);

  return 0;
}

static void taosws_session_release_taos(taosws_session_t *session)
{
  if (!session) return;

  if (session->taos) {
    D("taos closing ...");
    taos_close(session->taos);
    D("taos closed");
    session->taos = NULL;
  }
}

static void taosws_session_release(taosws_session_t *session)
{
  if (!session) return;

  taosws_session_release_taos(session);

  for (size_t i=0; i<session->nr; ++i) {
    SAFE_FREE(session->buffers[i]);
  }
  SAFE_FREE(session->buffers);
  session->cap = 0;
  session->nr = 0;

  SAFE_FREE(session->recv_buf);
  session->recv_cap = 0;
  session->recv_nr = 0;

  SAFE_FREE(session->ip);
  SAFE_FREE(session->user);
  SAFE_FREE(session->password);
  SAFE_FREE(session->db);
  session->port = 0;

  return;
}

static void taos_conn_task_on_work(taosws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;

  taosws_session_t *session = task->session;

  taos_conn_task->errstr[0] = '\0';

  D("taos connecting(ip:%s;user:%s;password:%s;db:%s;port:%d...", session->ip, session->user, session->password, session->db, session->port);
  taos_conn_task->taos = taos_connect(session->ip, session->user, session->password, session->db, session->port);
  if (!taos_conn_task->taos) {
    taos_conn_task->e = taos_errno(NULL);
    const char *es = taos_errstr(NULL);
    snprintf(taos_conn_task->errstr, sizeof(taos_conn_task->errstr), "failure:[taosc][%d]%s", taos_conn_task->e, es);
    return;
  }
  D("taos connecting done");

  return;
}

static void taos_conn_task_on_done(taosws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;
  taosws_session_t *session = task->session;

  if (!taos_conn_task->taos) {
    taosws_wsi_respond_err(session->wsi, session, "%s", taos_conn_task->errstr);
    return;
  }

  session->taos = taos_conn_task->taos;
  taos_conn_task->taos = NULL;
  taosws_wsi_respond_ok(session->wsi, session, "taos_connect success");
}

static void taos_conn_task_on_release(taosws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;
  if (taos_conn_task->taos) {
    D("taos closing ...");
    A(0, "internal logic error");
    taos_close(taos_conn_task->taos);
    D("taos closed");
    taos_conn_task->taos = NULL;
  }
}

static int taosws_session_open_connect(taosws_session_t *session)
{
  struct lws *wsi = session->wsi;

  if (session->taos) {
    taosws_wsi_respond_err(wsi, session, "already connected");
    return -1;
  }

  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  taos_conn_task_t *task = &session->conn_task;
  session->task = &task->task;

  task->task.session = session;
  task->task.on_work = taos_conn_task_on_work;
  task->task.on_done = taos_conn_task_on_done;
  task->task.on_release = taos_conn_task_on_release;

  if (taosws_vhost_data_post_task(vhd, &task->task)) {
    taosws_wsi_respond_err(wsi, session, "out of memory");
    task->task.on_release(session->task);
    task->task.state = TASK_STATE_NONE;
    session->task = NULL;
    return -1;
  }

  lws_rx_flow_control(wsi, 0);

  return 0;
}

static int taosws_session_close_connect(taosws_session_t *session)
{
  struct lws *wsi = session->wsi;

  if (!session->taos) {
    taosws_wsi_respond_ok(wsi, session, "not connected or already closed");
    return 0;
  }

  taosws_session_release_taos(session);

  taosws_wsi_respond_ok(wsi, session, "taos_close success");

  return 0;
}

static struct lws_context *global_context = NULL;
static taosws_vhost_data_t *global_vhd = NULL;

static void taosws_wsi_on_wait_cancel(struct lws *wsi, taosws_session_t *session)
{
  (void)session;

  A(wsi, "internal logic error");
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  A(vhost, "internal logic error");
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  A(protocols, "internal logic error");
  taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  A(vhd, "internal logic error");
  A(vhd == global_vhd, "internal logic error");

  taosws_vhost_data_wakeup_done_tasks(global_vhd);
}

static int taosws_on_protocol_init(struct lws *wsi)
{
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);

  taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_zalloc(vhost, protocols, sizeof(*vhd));
  if (!vhd) {
    D("out of memory");
    return -1;
  }

  taosws_vhost_data_init(vhd, wsi);

  global_vhd = vhd;
  return 0;
}

static int taosws_on_protocol_destroy(struct lws *wsi)
{
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  taosws_vhost_data_release(vhd);

  return 0;
}

static int taosws_wsi_on_established(struct lws *wsi, taosws_session_t *session, void *in, size_t len)
{
  (void)in;
  (void)len;

  char buf[1024]; buf[0] = '\0';
  lws_hdr_copy(wsi, buf, sizeof(buf), WSI_TOKEN_GET_URI);
  if (strcmp(buf, "/taosws")) {
    taosws_wsi_respond_err(wsi, session, "URI:expected /taosws, but got ==%s==", buf);
    return -1;
  }
  if (taosws_session_init(session, wsi)) {
    session->terminating = 1;
  }

  return 0;
}

static int _cjson_get_string(cJSON *json, const char *name, const char *def, const char **val)
{
  if (!cJSON_IsObject(json)) return -1;

  cJSON *v = cJSON_GetObjectItem(json, name);
  if (!v) {
    *val = def;
    return 0;
  }

  if (!cJSON_IsString(v)) return -1;
  *val = cJSON_GetStringValue(v);
  return 0;
}

static int _cjson_get_number(cJSON *json, const char *name, const double def, double *val)
{
  if (!cJSON_IsObject(json)) return -1;

  cJSON *v = cJSON_GetObjectItem(json, name);
  if (!v) {
    *val = def;
    return 0;
  }

  if (!cJSON_IsNumber(v)) return -1;
  *val = cJSON_GetNumberValue(v);
  return 0;
}

static int taosws_wsi_on_recv_req_conn(struct lws *wsi, taosws_session_t *session)
{
  (void)wsi;
  (void)_cjson_get_string;

  if (taosws_session_open_connect(session)) return -1;

  return 0;
}

static int taosws_wsi_on_recv_req_conn_close(struct lws *wsi, taosws_session_t *session)
{
  (void)wsi;

  taosws_session_close_connect(session);

  return 0;
}

static void foo_task_on_work(taosws_task_t *task)
{
  foo_task_t *foo_task = (foo_task_t*)task;
  D("sleeping %d secs...", foo_task->secs_to_sleep);
  sleep(foo_task->secs_to_sleep);
  D("sleeping %d secs done", foo_task->secs_to_sleep);
}

static void foo_task_on_done(taosws_task_t *task)
{
  taosws_session_t *session = task->session;
  taosws_wsi_respond_ok(session->wsi, session, "success");
}

static void foo_task_on_release(taosws_task_t *task)
{
  foo_task_t *foo_task = (foo_task_t*)task;
  (void)foo_task;
}

static int taosws_session_foo(taosws_session_t *session, int secs_to_sleep)
{
  struct lws *wsi = session->wsi;

  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  foo_task_t *task = &session->foo_task;
  session->task = &task->task;

  task->secs_to_sleep = secs_to_sleep;

  task->task.session = session;
  task->task.on_work = foo_task_on_work;
  task->task.on_done = foo_task_on_done;
  task->task.on_release = foo_task_on_release;

  if (taosws_vhost_data_post_task(vhd, &task->task)) {
    taosws_wsi_respond_err(wsi, session, "out of memory");
    task->task.on_release(session->task);
    task->task.state = TASK_STATE_NONE;
    session->task = NULL;
    return -1;
  }

  lws_rx_flow_control(wsi, 0);

  return 0;
}

static int taosws_wsi_on_recv_req_foo_args(struct lws *wsi, taosws_session_t *session, cJSON *args)
{
  double secs_to_sleep    = 0;

  if (_cjson_get_number(args, "secs_to_sleep", 0., &secs_to_sleep)) {
    taosws_wsi_respond_err(wsi, session, "`req::args::secs_to_sleep` not found in json packet");
    return -1;
  }

  if (secs_to_sleep < 0) {
    taosws_wsi_respond_err(wsi, session, "`req::args::secs_to_sleep:%f` invalid", secs_to_sleep);
    return -1;
  }

  if (taosws_session_foo(session, (int)secs_to_sleep)) return -1;

  return 0;
}

static int taosws_wsi_on_recv_req(struct lws *wsi, taosws_session_t *session, cJSON *req)
{
  cJSON *name = cJSON_GetObjectItem(req, "name");
  if (!name) {
    taosws_wsi_respond_err(wsi, session, "`req::name` not found in json packet");
    return -1;
  }
  const char *s_name = cJSON_GetStringValue(name);
  if (!s_name) {
    taosws_wsi_respond_err(wsi, session, "`req::name` is not string in json packet");
    return -1;
  }

  if (strcmp(s_name, "conn") == 0) {
    A(session->task == NULL, "internal logic error");

    return taosws_wsi_on_recv_req_conn(wsi, session);
  }

  if (strcmp(s_name, "close") == 0) {
    return taosws_wsi_on_recv_req_conn_close(wsi, session);
  }

  if (strcmp(s_name, "foo") == 0) {
    A(session->task == NULL, "internal logic error");

    cJSON *args = cJSON_GetObjectItem(req, "args");
    if (!args) {
      taosws_wsi_respond_err(wsi, session, "`req::args` not found in json packet");
      return -1;
    }

    return taosws_wsi_on_recv_req_foo_args(wsi, session, args);
  }

  taosws_wsi_respond_err(wsi, session, "unknown `req::name` = `%s` in json packet", s_name);
  return -1;
}

static int taosws_wsi_on_recv(struct lws *wsi, taosws_session_t *session, void *in, size_t len)
{
  int r = 0;

  if (session->terminating) return 0;

  if (interrupted) {
    taosws_wsi_respond_err(wsi, session, "service is shutting down");
    return -1;
  }

  if (lws_is_first_fragment(wsi)) {
    session->recv_nr = 0;
  }

  if (session->recv_nr + len >= session->recv_cap) {
    size_t cap = (session->recv_nr + len + 1 + 255) / 256 * 256;
    char *buf = (char*)realloc(session->recv_buf, cap);
    if (!buf) {
      taosws_wsi_respond_err(wsi, session, "out of memory");
      return -1;
    }
    session->recv_buf = buf;
    session->recv_cap = cap;
  }

  memcpy(session->recv_buf + session->recv_nr, in, len);
  session->recv_nr += len;

  if (!lws_is_final_fragment(wsi)) return 0;

  cJSON *json = cJSON_ParseWithLength(session->recv_buf, session->recv_nr);
  if (!json) {
    const char *s = cJSON_GetErrorPtr();
    if (s) {
      taosws_wsi_respond_err(wsi, session, "json parsing `%.*s` failure:`%.*s`", (int)len, (const char*)in, (int)(((const char*)in) + len - s), s);
    } else {
      taosws_wsi_respond_err(wsi, session, "json parsing `%.*s` failure", (int)len, (const char*)in);
    }
    return -1;
  }

  cJSON *req = cJSON_GetObjectItem(json, "req");
  if (!req) {
    taosws_wsi_respond_err(wsi, session, "`req` not found in json packet");
    cJSON_Delete(json);
    return -1;
  }

  r =  taosws_wsi_on_recv_req(wsi, session, req);

  cJSON_Delete(json);

  return r ? -1 : 0;
}

static int taosws_wsi_on_writeable(struct lws *wsi, taosws_session_t *session)
{
  if (session->nr > 0) {
    char *buf = session->buffers[0] + LWS_PRE;
    lws_write(wsi, (unsigned char*)buf, strlen(buf), LWS_WRITE_TEXT);
    lws_callback_on_writable(wsi);
    SAFE_FREE(session->buffers[0]);
    for (size_t i=0; i<session->nr; ++i) {
      if (i+1 < session->nr) {
        session->buffers[i] = session->buffers[i+1];
      } else {
        session->buffers[i] = NULL;
      }
    }
    session->nr -= 1;
    return 0;
  }

  if (session->terminating) lws_set_timeout(wsi, 0, LWS_TO_KILL_ASYNC);

  return 0;
}

static int taosws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
  (void)wsi;
  (void)reason;
  (void)user;
  (void)in;
  (void)len;

#define DC(fmt, ...) D("wsi:%p;user:%p;global_context:%p;%s", wsi, user, global_context, _lws_callback_reason_name(reason))

  taosws_session_t *session = (taosws_session_t*)user;
  // struct lws_vhost *vhost = lws_get_vhost(wsi);
  // const struct lws_protocols *protocols = lws_get_protocol(wsi);
  // taosws_vhost_data_t *vhd = (taosws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);

  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT:
      DC("");
      if (taosws_on_protocol_init(wsi)) return -1;
      break;

    case LWS_CALLBACK_PROTOCOL_DESTROY:
      DC("");
      if (taosws_on_protocol_destroy(wsi)) return -1;
      break;

    case LWS_CALLBACK_ESTABLISHED:
      DC("");
      if (taosws_wsi_on_established(wsi, session, in, len)) return -1;
      break;

    case LWS_CALLBACK_CLOSED:
      DC("");
      taosws_session_release((taosws_session_t*)user);
      break;

    case LWS_CALLBACK_CONFIRM_EXTENSION_OKAY:
      DC("");
      break;

    case LWS_CALLBACK_SERVER_WRITEABLE:
      DC("");
      taosws_wsi_on_writeable(wsi, session);
      break;

    case LWS_CALLBACK_RECEIVE:
      DC("");
      if (taosws_wsi_on_recv(wsi, session, in, len)) return -1;
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      DC("");
      taosws_wsi_on_wait_cancel(wsi, session);
      break;

    case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
      DC("");
      break;

    default:
      DC("");
      break;
  }

#undef DC

  return 0;
}

static int run(int argc, char **argv)
{
  (void)argc;
  (void)argv;

  // Create the WebSocket protocol
  static struct lws_protocols protocols[] = {
    {
      "http-only",
      lws_callback_http_dummy,
      0, 0, 0, NULL, 0
    },{
      "taosws-protocol", // Protocol name, should match the WebSocket protocol in the frontend code
      taosws_callback, // Callback function pointer
      sizeof(struct taosws_session_s), // Size of data for each session (connection)
      0, // No additional protocol parameters
      0, NULL, 0
    },
    LWS_PROTOCOL_LIST_TERM
  };

  // Create the WebSocket context
  struct lws_context_creation_info info = {
    .port = 3001, // Listening port number
    .protocols = protocols, // Protocol list
  };

  struct lws_context *context = lws_create_context(&info);

  // Check if WebSocket context creation was successful
  if (!context) {
    printf("Failed to create WebSocket context.\n");
    return -1;
  }

  global_context = context;

  // Enter the loop and wait for WebSocket connections
  while (!interrupted) {
    lws_service(context, 0);
  }

  if (global_vhd) {
    taosws_queue_workers_stop(&global_vhd->workers);
  }

  // Clean up and close the WebSocket context
  lws_context_destroy(context);

  return 0;
}

void sigint_handler(int sig)
{
  (void)sig;

	interrupted = 1;
  lws_cancel_service(global_context);
}

int main(int argc, char **argv)
{
  int r = 0;

	signal(SIGINT, sigint_handler);
	signal(SIGHUP, sigint_handler);

  taos_init();
  r = run(argc, argv);
  taos_cleanup();

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

