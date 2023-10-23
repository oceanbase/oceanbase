#include <pthread.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include "io/easy_io_struct.h"
#include "io/easy_log.h"
#include "io/easy_baseth_pool.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"

__thread easy_baseth_t  *easy_baseth_self;
static void easy_baseth_pool_invoke(struct ev_loop *loop);
static void easy_baseth_pool_invoke_debug(struct ev_loop *loop);
static int easy_monitor_interval = 100;
static const int64_t easy_monitor_signal = 34;

int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
pthread_t ob_pthread_get_pth(void *ptr);
void ob_set_thread_name(const char* type);
int64_t ob_update_loop_ts();
void ob_usleep(const useconds_t v);
/**
 * start
 */
void *easy_baseth_on_start(void *args)
{
    easy_baseth_t           *th;
    easy_io_t               *eio;
    th = (easy_baseth_t *) args;
    easy_baseth_self = th;
    eio = th->eio;

    if (eio->block_thread_signal)
        pthread_sigmask(SIG_BLOCK, &eio->block_thread_sigset, NULL);

    ev_run(th->loop, 0);
    easy_baseth_self = NULL;

    easy_debug_log("pthread exit: %lx.\n", pthread_self());

    return (void *)NULL;
}

/**
 * wakeup
 */
void easy_baseth_on_wakeup(void *args)
{
    easy_baseth_t           *th = (easy_baseth_t *)args;

    easy_spin_lock(&th->thread_lock);
    ev_async_fsend(th->loop, &th->thread_watcher);
    easy_spin_unlock(&th->thread_lock);
}

void easy_baseth_init(void *args, easy_thread_pool_t *tp,
                      easy_baseth_on_start_pt *start, easy_baseth_on_wakeup_pt *wakeup)
{
    easy_baseth_t           *th = (easy_baseth_t *)args;
    th->idx = (((char *)(th)) - (&(tp)->data[0])) / (tp)->member_size;
    th->on_start = start;

    th->loop = ev_loop_new(0);
    th->thread_lock = 0;
    th->lastrun = 0.0;

    ev_async_init (&th->thread_watcher, wakeup);
    th->thread_watcher.data = th;
    ev_async_start (th->loop, &th->thread_watcher);

    ev_set_userdata(th->loop, th);

    if (tp->monitor_tid) {
        ev_set_invoke_pending_cb(th->loop, easy_baseth_pool_invoke_debug);
    } else {
        ev_set_invoke_pending_cb(th->loop, easy_baseth_pool_invoke);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * 创建一个thread pool
 */
easy_thread_pool_t *easy_baseth_pool_create(easy_io_t *eio, int thread_count, int member_size)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;
    int                     size;

    size = sizeof(easy_thread_pool_t) + member_size * thread_count;

    if ((tp = (easy_thread_pool_t *) easy_pool_calloc(eio->pool, size)) == NULL)
        return NULL;

    tp->thread_count = thread_count;
    tp->member_size = member_size;
    tp->last = &tp->data[0] + member_size * thread_count;
    easy_list_add_tail(&tp->list_node, &eio->thread_pool_list);
    easy_thread_pool_for_each(th, tp, 0) {
        th->eio = eio;
    }

    // tp->ratelimit_thread = (easy_io_thread_t *)th;
    // start monitor
    const char *ptr = getenv("easy_thread_monitor");

    if (ptr) {
        easy_monitor_interval = atoi(ptr);
    }

    return tp;
}

/**
 * wakeup pool
 */
void easy_baseth_pool_on_wakeup(easy_thread_pool_t *tp)
{
    easy_baseth_t           *th;
    easy_thread_pool_for_each(th, tp, 0) {
        easy_baseth_on_wakeup(th);
    }
}

/**
 * destroy pool
 */
void easy_baseth_pool_destroy(easy_thread_pool_t *tp)
{
    easy_baseth_t           *th;
    easy_thread_pool_for_each(th, tp, 0) {
        ev_loop_destroy(th->loop);
    }
}

static void easy_baseth_pool_wakeup_session(easy_baseth_t *th)
{
    if (th->iot == 0)
        return;

    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;
    easy_io_thread_t        *ioth = (easy_io_thread_t *) th;

    // session at ioth
    easy_spin_lock(&ioth->thread_lock);

    easy_list_for_each_entry_safe(s, s1, &ioth->session_list, session_list_node) {
        if (s->status == 0 || s->status == EASY_CONNECT_SEND) {
            easy_warn_log("session fail due to io thread exit %p", s);
            easy_list_del(&s->session_list_node);
            easy_session_process(s, 0, EASY_STOP);
        }
    }
    // connection at ioth
    easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
        easy_connection_wakeup_session(c, EASY_DISCONNECT);
    }
    // foreach connected_list
    easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
        easy_connection_wakeup_session(c, EASY_DISCONNECT);
    }
    easy_spin_unlock(&ioth->thread_lock);
}


/**
 * 判断是否退出
 */
static void easy_baseth_pool_invoke(struct ev_loop *loop)
{
    easy_baseth_t           *th = (easy_baseth_t *) ev_userdata (loop);
    easy_connection_t       *c, *c1;
    easy_io_thread_t        *ioth;
    easy_listen_t           *l;

    th->lastrun = ev_now(loop);
    if (th->user_process) (*th->user_process)(th);

    ev_invoke_pending(loop);

    if (th->eio->shutdown && th->iot == 1) {
        ioth = (easy_io_thread_t *) ev_userdata (loop);

        if (ioth->eio->listen) {
            int ts = (ioth->eio->listen_all || ioth->eio->io_thread_count == 1);

            for (l = ioth->eio->listen; l; l = l->next) {
                if (l->reuseport || ts) {
                    ev_io_stop(loop, &l->read_watcher[ioth->idx]);
                } else {
                    ev_timer_stop (loop, &ioth->listen_watcher);
                }
            }
        }
        // connection at ioth
        easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
            shutdown(c->fd, SHUT_RD);
            EASY_CONNECTION_DESTROY(c, "close conn_list in ev_invoke");
        }
        // foreach connected_list
        easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
            shutdown(c->fd, SHUT_RD);
            EASY_CONNECTION_DESTROY(c, "close connected_list in ev_invoke");
        }
    }

    th->lastrun = 0.0;

    if (th->eio->stoped) {
        easy_baseth_pool_wakeup_session(th);
        ev_break(loop, EVBREAK_ALL);
        easy_debug_log("ev_break: eio=%p\n", th->eio);
    }
}

void easy_baseth_pool_invoke_debug(struct ev_loop *loop)
{
    ev_tstamp st = ev_time();
    easy_baseth_pool_invoke(loop);
    ev_tstamp et = ev_time();

    if (et - st > easy_monitor_interval / 1000.0) {
        easy_warn_log("EASY SLOW: start: %f end: %f cost: %f", st, et, et - st);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
static void *easy_baseth_pool_monitor_func(void *args)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp = (easy_thread_pool_t *) args;
    int64_t                 loopcnts[tp->thread_count];
    int64_t                 slowcnts[tp->thread_count];

    ob_set_thread_name("EasyBasethPoolMonitor");

    memset(loopcnts, 0, sizeof(loopcnts));
    memset(slowcnts, 0, sizeof(slowcnts));
    const int64_t us = easy_monitor_interval * 1000L;
    const double sec = easy_monitor_interval / 1000.0;
    easy_info_log("monitor us :%ld sec :%f", us, sec);

    while(tp->stoped == 0) {
        ob_update_loop_ts();
        ob_usleep(us);
        ev_tstamp now = ev_time();
        easy_thread_pool_for_each(th, tp, 0) {
            ev_tstamp last = th->lastrun;
            int id = ev_loop_count(th->loop);

            if (loopcnts[th->idx] != id) {
                loopcnts[th->idx] = id;
                slowcnts[th->idx] = 0;
            }

            if (last > 0 && now - last > sec) {
                slowcnts[th->idx] ++;

                if (slowcnts[th->idx] < 10) {
                  //pthread_kill(th->tid, easy_monitor_signal);
                }

                if (EASY_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                    easy_warn_log("EASY SLOW: thread: %lx, lastrun: %f cost: %f loop:%d, slowcnt: %ld",
                        ob_pthread_get_pth(th->tid), last, now - last, id, slowcnts[th->idx]);
                }
            }
        }
    }
    easy_info_log("easy monitor thread stopped");
    return NULL;
}

static void easy_baseth_pool_sighand(int sig, siginfo_t *sinfo, void *ucontext)
{
    int saved_errno = errno;
    {
        ev_tstamp last = 0.0;
        int lid = 0;

        if (easy_baseth_self) {
            last = easy_baseth_self->lastrun;
            lid = ev_loop_count(easy_baseth_self->loop);
        }

        void *array[25];
        int i, idx = 0;
        char _buffer_stack_[512];

        int n = ob_backtrace_c(array, 25);

        if (n > 2) {
            for (i = 2; i < n; i++) idx += lnprintf(idx + _buffer_stack_, 20, "%p ", array[i]);

            _buffer_stack_[idx] = '\0';
            easy_warn_log("EASY SLOW STACK:%f %d %f => %s", last, lid, ev_time() - last, _buffer_stack_);
        }
    }

    errno = saved_errno;
}

void easy_baseth_pool_monitor(easy_thread_pool_t *tp)
{
    int              rc, err;
    struct sigaction sa;

    if (tp->monitor_tid) {
        return;
    } else if (easy_monitor_interval > 10) {
      sa.sa_sigaction = easy_baseth_pool_sighand;
      sa.sa_flags = SA_RESTART | SA_SIGINFO;
      sigemptyset(&sa.sa_mask);
      rc = sigaction(easy_monitor_signal, &sa, NULL);

      err = ob_pthread_create(&tp->monitor_tid, easy_baseth_pool_monitor_func, tp);
      if (err != 0) {
        tp->monitor_tid = NULL;
        easy_error_log("sigaction: %d, monitor_tid: %p, err:%d, errno:%d", rc, tp->monitor_tid, err, errno);
      } else {
        easy_info_log("monitor thread created, tp=0x%lx tid=%p\n", tp, tp->monitor_tid);
      }
    }
}
