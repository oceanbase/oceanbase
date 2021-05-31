/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "io/easy_io.h"
#include "io/easy_baseth_pool.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"
#include "io/easy_request.h"
#include "io/easy_file.h"
#include "io/easy_client.h"
#include "io/easy_socket.h"
#include "util/easy_mod_stat.h"
#include <sched.h>

char* easy_version = "LIBEASY VERSION: 1.1.22 OceanBase Embedded Edition, BUILD: " __DATE__ " " __TIME__;
easy_io_t easy_io_var = {NULL};
easy_atomic_t easy_io_list_lock = 0;
easy_list_t easy_io_list_var = EASY_LIST_HEAD_INIT(easy_io_list_var);

static void* easy_io_on_thread_start(void* args);
static void easy_io_on_uthread_start(void* args);
static void easy_io_on_uthread_evstart(void* args);
static void easy_io_uthread_invoke(struct ev_loop* loop);
static void easy_io_thread_destroy(easy_io_thread_t* ioth);
static void easy_io_stat_process(struct ev_loop* loop, ev_timer* w, int revents);
static void easy_io_print_status(easy_io_t* eio);
static void easy_signal_handler(int sig);
static void easy_listen_close(easy_listen_t* l);

easy_io_t* easy_eio_create(easy_io_t* eio, int io_thread_count)
{
  easy_io_thread_t* ioth;
  easy_thread_pool_t* tp;
  easy_pool_t* pool;
  int v;

  {
    static int show_version = 1;
    if (show_version) {
      show_version = 0;
      easy_info_log("%s \n", easy_version);
    }
  }
  if (eio != NULL && eio->pool != NULL) {
    return eio;
  }

  if (io_thread_count <= 0 || io_thread_count > EASY_MAX_THREAD_CNT) {
    io_thread_count = sysconf(_SC_NPROCESSORS_CONF);
  }

  if ((pool = easy_pool_create(0)) == NULL) {
    return NULL;
  }

  if (eio == NULL && (eio = (easy_io_t*)easy_pool_alloc(pool, sizeof(easy_io_t))) == NULL) {
    easy_pool_destroy(pool);
    return NULL;
  }

  memset(eio, 0, sizeof(easy_io_t));
  eio->pool = pool;
  eio->io_thread_count = io_thread_count;
  eio->start_time = ev_time();
  easy_list_init(&eio->thread_pool_list);
  ev_set_allocator(realloc_lowlevel);

  // create summary buffer
  eio->eio_summary = easy_summary_create();

  tp = easy_baseth_pool_create(eio, io_thread_count, sizeof(easy_io_thread_t));

  if (tp == NULL) {
    goto error_exit;
  }

  eio->io_thread_pool = tp;
  eio->tcp_nodelay = 1;
  eio->no_delayack = 1;
  eio->use_accept4 = 1;
  eio->tcp_defer_accept = 1;
  eio->do_signal = 1;
  eio->tcp_keepalive = 1;  // open tcp_keepAlive
  eio->send_qlen = EASY_CONN_DOING_REQ_CNT * 16;
  eio->support_ipv6 = easy_socket_support_ipv6();
  eio->listen_backlog = 1024;
  eio->ssl = NULL;
  eio->ssl_rwlock_.ref_cnt = 0;
  eio->ssl_rwlock_.wait_write = 0;
  eio->keepalive_enabled = 0;
#ifdef HAVE_RECVMMSG
  eio->recv_vlen = 8;
#endif

  easy_thread_pool_for_each(ioth, tp, 0)
  {
    easy_list_init(&ioth->connected_list);
    v = offsetof(easy_client_t, client_list_node);
    ioth->client_list = easy_hash_create(pool, EASY_MAX_CLIENT_CNT / io_thread_count, v);
    ioth->client_array = easy_array_create(sizeof(easy_client_t));

    easy_list_init(&ioth->conn_list);
    easy_list_init(&ioth->session_list);
    easy_list_init(&ioth->request_list);

    ev_timer_init(&ioth->listen_watcher, easy_connection_on_listen, 0.0, 0.1);
    ioth->listen_watcher.data = ioth;
    ioth->iot = 1;

    // base thread init
    easy_baseth_init(ioth, tp, easy_io_on_thread_start, easy_connection_on_wakeup);
  }

  signal(SIGPIPE, SIG_IGN);

  // add to easy_io_list_var
  easy_spin_lock(&easy_io_list_lock);
  easy_list_add_tail(&eio->eio_list_node, &easy_io_list_var);
  easy_spin_unlock(&easy_io_list_lock);

  return eio;
error_exit:
  easy_eio_destroy(eio);
  return NULL;
}

void easy_eio_destroy(easy_io_t* eio)
{
  easy_pool_t* pool;
  easy_io_thread_t* ioth;
  easy_thread_pool_t* tp;
  easy_listen_t* l;

  if (eio == NULL)
    return;

  easy_spin_lock(&easy_io_list_lock);

  eio->stoped = 1;

  if (eio->eio_list_node.prev)
    easy_list_del(&eio->eio_list_node);

  easy_spin_unlock(&easy_io_list_lock);

  // close listen
  for (l = eio->listen; l; l = l->next) {
    easy_listen_close(l);
  }

  for (l = eio->listenadd; l; l = l->next) {
    easy_listen_close(l);
  }

  // destroy io_thread
  if (eio->io_thread_pool) {
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0)
    {
      easy_io_thread_destroy(ioth);
    }
  }

  // destroy baseth pool
  easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node)
  {
    easy_baseth_pool_destroy(tp);
  }

  easy_summary_destroy(eio->eio_summary);
  pool = eio->pool;
  memset(eio, 0, sizeof(easy_io_t));
  easy_pool_destroy(pool);

  easy_debug_log("easy_eio_destroy, eio=%p\n", eio);
}

int easy_eio_start(easy_io_t* eio)
{
  easy_baseth_t* th;
  easy_thread_pool_t* tp;

  if (eio == NULL || eio->pool == NULL) {
    return EASY_ERROR;
  }

  if (eio->started) {
    return EASY_ABORT;
  }

  if (eio->tcp_nodelay) {
    eio->tcp_cork = 0;
    eio->no_delayack = 0;
  }

  if (eio->do_signal) {
    struct sigaction sigact;
    memset(&sigact, 0, sizeof(struct sigaction));
    sigact.sa_handler = easy_signal_handler;
    sigemptyset(&sigact.sa_mask);
    sigaction(39, &sigact, NULL);
    sigact.sa_flags = SA_RESETHAND;
    sigaction(SIGINT, &sigact, NULL);
    sigaction(SIGTERM, &sigact, NULL);
  }

  easy_spin_lock(&eio->lock);
  easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node)
  {
    easy_thread_pool_for_each(th, tp, 0)
    {
      pthread_create(&(th->tid), NULL, th->on_start, (void*)th);
    }
    easy_baseth_pool_monitor(tp);
  }
  eio->started = 1;
  easy_spin_unlock(&eio->lock);

  return EASY_OK;
}

int easy_eio_wait(easy_io_t* eio)
{
  easy_baseth_t* th;
  easy_thread_pool_t* tp;

  easy_spin_lock(&eio->lock);
  easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node)
  {
    easy_spin_unlock(&eio->lock);
    easy_thread_pool_for_each(th, tp, 0)
    {
      if (th->tid && pthread_join(th->tid, NULL) == EDEADLK) {
        easy_fatal_log("easy_io_wait fatal, eio=%p, tid=%lx\n", eio, th->tid);
        abort();
      }

      th->tid = 0;
    }
    easy_spin_lock(&eio->lock);
    easy_info_log("easy_io_wait join monitor, tp=0x%lx tid=%lx\n", tp, tp->monitor_tid);
    if (tp->monitor_tid && pthread_join(tp->monitor_tid, NULL) == EDEADLK) {
      easy_fatal_log("easy_io_wait fatal, eio=%p, tid=%lx\n", eio, tp->monitor_tid);
      abort();
    }

    tp->monitor_tid = 0;
  }
  easy_spin_unlock(&eio->lock);

  easy_debug_log("easy_io_wait exit, eio=%p\n", eio);

  return EASY_OK;
}

int easy_eio_shutdown(easy_io_t* eio)
{
  easy_thread_pool_t *tp, *tp1;

  if (eio == NULL || eio->shutdown) {
    return EASY_ERROR;
  }

  easy_debug_log("easy_eio_shutdown exit, eio=%p\n", eio);
  eio->shutdown = 1;

  easy_list_for_each_entry_safe(tp, tp1, &eio->thread_pool_list, list_node)
  {
    easy_baseth_pool_on_wakeup(tp);
  }
  easy_debug_log("easy_eio_shutdown exit, eio=%p %s\n", eio, easy_version);

  return EASY_OK;
}

int easy_eio_stop(easy_io_t* eio)
{
  easy_thread_pool_t *tp, *tp1;

  if (eio == NULL || eio->stoped) {
    return EASY_ERROR;
  }

  easy_debug_log("easy_eio_stop exit, eio=%p\n", eio);
  eio->stoped = 1;
  easy_list_for_each_entry_safe(tp, tp1, &eio->thread_pool_list, list_node)
  {
    tp->stoped = 1;
    easy_baseth_pool_on_wakeup(tp);
  }
  easy_debug_log("easy_eio_stop exit, eio=%p %s\n", eio, easy_version);

  return EASY_OK;
}

/**
 * set keepalive_enabled of eio.
 */
void easy_eio_set_keepalive(easy_io_t* eio, int keepalive_enabled)
{
  eio->keepalive_enabled = keepalive_enabled;
}

struct ev_loop* easy_eio_thread_loop(easy_io_t* eio, int index)
{
  easy_io_thread_t* ioth;
  ioth = (easy_io_thread_t*)easy_thread_pool_index(eio->io_thread_pool, index);
  return (ioth ? ioth->loop : NULL);
}

void easy_eio_stat_watcher_start(
    easy_io_t* eio, ev_timer* stat_watcher, double interval, easy_io_stat_t* iostat, easy_io_stat_process_pt* process)
{
  easy_io_thread_t* ioth;

  memset(iostat, 0, sizeof(easy_io_stat_t));
  iostat->last_cnt = 0;
  iostat->last_time = eio->start_time;
  iostat->process = process;
  iostat->eio = eio;

  ioth = (easy_io_thread_t*)easy_thread_pool_index(eio->io_thread_pool, 0);
  ev_timer_init(stat_watcher, easy_io_stat_process, 0., interval);
  stat_watcher->data = iostat;
  ev_timer_start(ioth->loop, stat_watcher);
  easy_baseth_on_wakeup(ioth);
}

void easy_eio_set_uthread_start(easy_io_t* eio, easy_io_uthread_start_pt* on_utstart, void* args)
{
  easy_io_thread_t* ioth;

  eio->uthread_enable = 1;
  easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0)
  {
    ioth->on_utstart = on_utstart;
    ioth->ut_args = args;
  }
}
//////////////////////////////////////////////////////////////////////////////
static void* easy_io_on_thread_start(void* args)
{
  easy_listen_t* l;
  easy_io_thread_t* ioth;
  easy_io_t* eio;

  ioth = (easy_io_thread_t*)args;
  easy_baseth_self = (easy_baseth_t*)args;
  eio = ioth->eio;

  if (eio->block_thread_signal) {
    pthread_sigmask(SIG_BLOCK, &eio->block_thread_sigset, NULL);
  }

  // sched_setaffinity
  if (eio->affinity_enable) {
    static easy_atomic_t cpuid = -1;
    int cpunum = sysconf(_SC_NPROCESSORS_CONF);
    cpu_set_t mask;
    int idx = (easy_atomic_add_return(&cpuid, 1) & 0x7fffffff) % cpunum;
    CPU_ZERO(&mask);
    CPU_SET(idx, &mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
      easy_error_log("sched_setaffinity error: %d (%s), cpuid=%d\n", errno, strerror(errno), cpuid);
    }
  }

  if (eio->listen) {
    int ts = (eio->listen_all || eio->io_thread_count == 1);

    for (l = eio->listen; l; l = l->next) {
      if (l->reuseport || ts) {
        if (ntohs(l->addr.port) >= 1024) {
          easy_connection_reuseport(eio, l, ioth->idx);
        }
        ev_io_start(ioth->loop, &l->read_watcher[ioth->idx]);
      } else {
        if (ntohs(l->addr.port) >= 1024) {
          ev_timer_start(ioth->loop, &ioth->listen_watcher);
        }
      }
    }
  }

  if (eio->uthread_enable) {
    easy_uthread_control_t control;
    ev_set_invoke_pending_cb(ioth->loop, easy_io_uthread_invoke);
    easy_uthread_init(&control);
    easy_uthread_create(easy_io_on_uthread_evstart, ioth->loop, 256 * 1024);

    if (ioth->on_utstart) {
      easy_uthread_create(easy_io_on_uthread_start, ioth, EASY_UTHREAD_STACK);
      easy_baseth_on_wakeup(ioth);
    }

    easy_uthread_scheduler();
    easy_uthread_destroy();
  } else {
    if (ioth->on_utstart)
      ioth->on_utstart(ioth->ut_args);

    ev_run(ioth->loop, 0);
  }

  easy_baseth_self = NULL;

  easy_debug_log("pthread exit: %lx\n", pthread_self());

  return (void*)NULL;
}

static void easy_io_on_uthread_start(void* args)
{
  easy_io_thread_t* ioth = (easy_io_thread_t*)args;

  if (ioth->on_utstart) {
    (ioth->on_utstart)(ioth->ut_args);
  }
}
static void easy_io_on_uthread_evstart(void* args)
{
  ev_run((struct ev_loop*)args, 0);
}

static void easy_io_thread_destroy(easy_io_thread_t* ioth)
{
  easy_connection_t *c, *c1;
  easy_session_t *s, *s1;

  // session at ioth
  easy_spin_lock(&ioth->thread_lock);
  easy_list_for_each_entry_safe(s, s1, &ioth->session_list, session_list_node)
  {
    easy_list_del(&s->session_list_node);

    if (s->status && s->pool) {
      easy_pool_destroy(s->pool);
    }
  }
  // connection at ioth
  easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node)
  {
    EASY_CONNECTION_DESTROY(c, "thread_destroy");
  }
  // foreach connected_list
  easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node)
  {
    EASY_CONNECTION_DESTROY(c, "thread_destroy");
  }
  easy_spin_unlock(&ioth->thread_lock);

  easy_array_destroy(ioth->client_array);
}

static void easy_io_stat_process(struct ev_loop* loop, ev_timer* w, int revents)
{
  easy_io_stat_t* iostat;
  ev_tstamp last_time, t1, t2;
  int64_t last_cnt;
  easy_io_thread_t* ioth;
  easy_io_t* eio;

  iostat = (easy_io_stat_t*)w->data;
  eio = iostat->eio;

  last_time = ev_now(loop);
  last_cnt = 0;
  int ql = 0;
  easy_connection_t* c;
  easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0)
  {
    last_cnt += ioth->done_request_count;
    easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node)
    {
      ql += c->con_summary->doing_request_count;
    }
  }

  t1 = last_time - iostat->last_time;
  t2 = last_time - eio->start_time;

  iostat->last_speed = (last_cnt - iostat->last_cnt) / t1;
  iostat->total_speed = last_cnt / t2;

  iostat->last_cnt = last_cnt;
  iostat->last_time = last_time;

  if (iostat->process == NULL) {
    easy_info_log("cnt: %" PRId64 ", speed: %.2f, total_speed: %.2f, ql:%d\n",
        iostat->last_cnt,
        iostat->last_speed,
        iostat->total_speed,
        ql);
  } else {
    (iostat->process)(iostat);
  }
}

static void easy_signal_handler(int sig)
{
  easy_io_t *eio, *e1;

  if (easy_trylock(&easy_io_list_lock) == 0) {
    return;
  }

  if (sig == SIGINT || sig == SIGTERM) {
    easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node)
    {
      easy_eio_stop(eio);
    }
  } else if (sig == 39) {
    easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node)
    {
      easy_io_print_status(eio);
    }
  }

  easy_unlock(&easy_io_list_lock);
}

static void easy_io_uthread_invoke(struct ev_loop* loop)
{
  easy_baseth_t* th = (easy_baseth_t*)ev_userdata(loop);

  if (th->eio->stoped) {
    ev_break(loop, EVBREAK_ALL);
    easy_uthread_stop();
    return;
  }

  ev_invoke_pending(loop);

  while (easy_uthread_yield() > 0)
    ;
}

static void easy_io_print_status(easy_io_t* eio)
{
  easy_connection_t* c;
  easy_io_thread_t* ioth;

  // foreach connected_list
  easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0)
  {
    easy_info_log("thread:%d, doing: %d, done: %" PRIdFAST32 "\n",
        ioth->idx,
        ioth->doing_request_count,
        ioth->done_request_count);
    easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node)
    {
      easy_info_log("%d %s => doing: %d, done:%" PRIdFAST32 "\n",
          ioth->idx,
          easy_connection_str(c),
          c->con_summary->doing_request_count,
          c->con_summary->done_request_count);
    }
  }
}

static void easy_listen_close(easy_listen_t* l)
{
  int i;

  if (l->reuseport) {
    for (i = 0; i < l->bind_port_cnt; i++) {
      if (l->read_watcher[i].fd != l->fd) {
        easy_socket_set_linger(l->read_watcher[i].fd, 0);
        easy_safe_close(l->read_watcher[i].fd);
      }
    }
  }

  easy_socket_set_linger(l->fd, 0);
  easy_safe_close(l->fd);
}
