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

// 全局变量
char                    *easy_version = "LIBEASY VERSION: 1.1.22 OceanBase Embedded Edition, BUILD: " __DATE__ " " __TIME__;
easy_io_t               easy_io_var = {NULL};
easy_atomic_t           easy_io_list_lock = 0;
easy_list_t             easy_io_list_var = EASY_LIST_HEAD_INIT(easy_io_list_var);

static easy_ratelimitor_t global_ratelimitor = {
    .inited                  = 0,
    .rl_lock                 = 0,
    .s2r_map_version_latest  = 0,
    .s2r_map_version_cur     = 0,
    .ratelimit_enabled       = 0,
    .stat_period             = 1.0, // 1s by default
    .s2r_hmap_rwlock = EASY_SPINRWLOCK_INITIALIZER,
};

static void *easy_io_on_thread_start(void *args);
static void easy_io_on_uthread_start(void *args);
static void easy_io_on_uthread_evstart(void *args);
static void easy_io_uthread_invoke(struct ev_loop *loop);
static void easy_io_thread_destroy(easy_io_thread_t *ioth);
static void easy_io_stat_process(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_io_print_status(easy_io_t *eio);
static void easy_signal_handler(int sig);
static void easy_listen_close(easy_listen_t *l);

int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
/**
 * 初始化easy_io
 */
easy_io_t *easy_eio_create(easy_io_t *eio, int io_thread_count)
{
    easy_io_thread_t        *ioth;
    easy_thread_pool_t      *tp;
    easy_pool_t             *pool;
    int                     v;
    int                     i;
    int                     ioth_idx;

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
        io_thread_count = EASY_MAX_THREAD_CNT;
    }

    if ((pool = easy_pool_create(0)) == NULL) {
        return NULL;
    }

    // 分配空间
    if (eio == NULL && (eio = (easy_io_t *)easy_pool_alloc(pool, sizeof(easy_io_t))) == NULL) {
        easy_pool_destroy(pool);
        return NULL;
    }

    // 初始化
    memset(eio, 0, sizeof(easy_io_t));
    eio->pool = pool;
    eio->io_thread_count = io_thread_count;
    eio->start_time = ev_time();
    easy_list_init(&eio->thread_pool_list);
    ev_set_allocator(realloc_lowlevel);

    //create summary buffer
    eio->eio_summary = easy_summary_create();

    // 创建IO线程池
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
    eio->send_qlen = EASY_CONN_DOING_REQ_CNT;
    eio->support_ipv6 = easy_socket_support_ipv6();
    eio->listen_backlog = 1024;
    eio->ssl = NULL;
    eio->ssl_rwlock_.ref_cnt = 0;
    eio->ssl_rwlock_.wait_write = 0;
    eio->keepalive_enabled = 0;
#ifdef HAVE_RECVMMSG
    eio->recv_vlen = 8;
#endif

    // 初始化线程池
    ioth_idx = 0;
    easy_thread_pool_for_each(ioth, tp, 0) {
        easy_list_init(&ioth->connected_list);
        v = offsetof(easy_client_t, client_list_node);
        ioth->client_list = easy_hash_create(pool, EASY_MAX_CLIENT_CNT / io_thread_count, v);
        ioth->client_array = easy_array_create(sizeof(easy_client_t));

        // 起异步事件
        easy_list_init(&ioth->conn_list);
        easy_list_init(&ioth->session_list);
        easy_list_init(&ioth->request_list);

        ev_timer_init(&ioth->listen_watcher, easy_connection_on_listen, 0.0, 0.1);
        ioth->listen_watcher.data = ioth;
        ioth->iot = 1;
        ioth->idx = ioth_idx++;

        // base thread init
        easy_info_log("init ioth(%p), idx(%d).\n", ioth, ioth->idx);
        tp->ratelimit_thread = ioth;
        easy_baseth_init(ioth, tp, easy_io_on_thread_start, easy_connection_on_wakeup);
    }

    {
        easy_spin_lock(&(global_ratelimitor.rl_lock));
        if (global_ratelimitor.inited == 0) {
            global_ratelimitor.cpu_freq          = easy_get_cpu_mhz(1) * 1000 * 1000;
            global_ratelimitor.cycles_per_period = (uint64_t) global_ratelimitor.cpu_freq * global_ratelimitor.stat_period;
            global_ratelimitor.cycles_per_stride = global_ratelimitor.cycles_per_period / RL_RECORD_RING_SIZE;

            easy_list_init(&global_ratelimitor.ready_queue);

            v = offsetof(easy_region_ratelimitor_t, hash_list_node);
            global_ratelimitor.s2r_hmap     = easy_hash_create(pool, EASY_MAX_CLIENT_CNT / io_thread_count, v);
            global_ratelimitor.region_array = easy_array_create(sizeof(easy_region_ratelimitor_hmap_node_t));
            for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
                memset(&(global_ratelimitor.region_rlmtrs[i]), 0, sizeof(easy_region_ratelimitor_t));
                global_ratelimitor.region_rlmtrs[i].region_id = -1;
            }
        }
        global_ratelimitor.inited = 1;
        easy_info_log("global_ratelimitor inited, stat_period(%lf), cpu_freq(%lf), cycles_per_period(%ld)",
                global_ratelimitor.stat_period, global_ratelimitor.cpu_freq, global_ratelimitor.cycles_per_period);
        easy_spin_unlock(&(global_ratelimitor.rl_lock));

        eio->easy_rlmtr = &global_ratelimitor;
    }

    // 屏蔽掉SIGPIPE
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


/**
 * 把一个easy_io删除掉
 */
void easy_eio_destroy(easy_io_t *eio)
{
    easy_pool_t             *pool;
    easy_io_thread_t        *ioth;
    easy_thread_pool_t      *tp;
    easy_listen_t           *l;

    if (eio == NULL)
        return;

    // 从easy_io_list_var去掉
    easy_spin_lock(&easy_io_list_lock);

    eio->stoped = 1;

    if (eio->eio_list_node.prev) easy_list_del(&eio->eio_list_node);

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
        easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
            easy_io_thread_destroy(ioth);
        }
    }

    // destroy baseth pool
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_baseth_pool_destroy(tp);
    }

    easy_summary_destroy(eio->eio_summary);
    pool = eio->pool;
    memset(eio, 0, sizeof(easy_io_t));
    easy_pool_destroy(pool);

    easy_debug_log("easy_eio_destroy, eio=%p\n", eio);
}

/**
 * 开始easy_io, 第一个线程用于listen, 后面的线程用于处理
 */
int easy_eio_start(easy_io_t *eio)
{
    int ret = EASY_OK;
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;

    // 没初始化pool
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
        struct sigaction        sigact;
        memset(&sigact, 0, sizeof(struct sigaction));
        sigact.sa_handler = easy_signal_handler;
        sigemptyset(&sigact.sa_mask);
        sigaction(39, &sigact, NULL);
        sigact.sa_flags = SA_RESETHAND;
        sigaction(SIGINT, &sigact, NULL);
        sigaction(SIGTERM, &sigact, NULL);
    }

    // 起线程
    easy_spin_lock(&eio->lock);
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_thread_pool_for_each(th, tp, 0) {
            int err = 0;
            if ((err = ob_pthread_create(&(th->tid), th->on_start, (void *)th))) {
                ret = EASY_ERROR;
                th->tid = NULL;
                easy_error_log("easy_io_start, pthread_create error: %d(%d), idx: %d", err, errno, th->idx);
            }
        }

        easy_baseth_pool_monitor(tp);
    }

    if (EASY_OK == ret) {
        eio->started = 1;
    } else {
        eio->started = 0;
    }
    easy_spin_unlock(&eio->lock);

    return ret;
}

/**
 * 等待easy_io
 */
int easy_eio_wait(easy_io_t *eio)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;
    // 等待thread
    easy_spin_lock(&eio->lock);
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_spin_unlock(&eio->lock);
        easy_thread_pool_for_each(th, tp, 0) {
            ob_pthread_join(th->tid);
            th->tid = NULL;
        }
        easy_spin_lock(&eio->lock);
        easy_info_log("easy_io_wait join monitor, tp=0x%lx tid=%p\n", tp, tp->monitor_tid);
        ob_pthread_join(tp->monitor_tid);
        tp->monitor_tid = NULL;

    }
    easy_spin_unlock(&eio->lock);

    easy_debug_log("easy_io_wait exit, eio=%p\n", eio);

    return EASY_OK;
}

int easy_eio_shutdown(easy_io_t *eio)
{
    easy_thread_pool_t      *tp, *tp1;

    if (eio == NULL || eio->shutdown) {
        return EASY_ERROR;
    }

    easy_debug_log("easy_eio_shutdown exit, eio=%p\n", eio);
    eio->shutdown = 1;
    // 让thread停止
    easy_list_for_each_entry_safe(tp, tp1, &eio->thread_pool_list, list_node) {
        easy_baseth_pool_on_wakeup(tp);
    }
    easy_debug_log("easy_eio_shutdown exit, eio=%p %s\n", eio, easy_version);

    return EASY_OK;
}

/**
 * 停止easy_io
 */
int easy_eio_stop(easy_io_t *eio)
{
    easy_thread_pool_t      *tp, *tp1;

    if (eio == NULL || eio->stoped) {
        return EASY_ERROR;
    }

    easy_debug_log("easy_eio_stop exit, eio=%p\n", eio);
    eio->stoped = 1;
    // 让thread停止
    easy_list_for_each_entry_safe(tp, tp1, &eio->thread_pool_list, list_node) {
        tp->stoped = 1;
        easy_baseth_pool_on_wakeup(tp);
    }
    easy_debug_log("easy_eio_stop exit, eio=%p %s\n", eio, easy_version);

    return EASY_OK;
}

/**
 * set keepalive_enabled of eio.
 */
void easy_eio_set_keepalive(easy_io_t *eio, int keepalive_enabled)
{
    eio->keepalive_enabled = keepalive_enabled;
}

easy_ratelimitor_t* get_global_ratelimitor()
{
    return &global_ratelimitor;
}

void easy_eio_region_rlmtr_init(easy_region_ratelimitor_t *region_rlmtr, const char* region)
{
    if ((region_rlmtr == NULL) || (region == NULL)) {
        easy_error_log("wrong parameters, region_rlmtr(%p), region(%p).", region_rlmtr, region);
    } else {
        memset(region_rlmtr, 0, sizeof(*region_rlmtr));
        region_rlmtr->region_id = 0;
        easy_list_init(&region_rlmtr->region_rlmtr_list_node);
        easy_list_init(&region_rlmtr->ready_queue_node);
        easy_list_init(&region_rlmtr->fg_req_queue);
        easy_list_init(&region_rlmtr->fg_rsp_queue);
        easy_list_init(&region_rlmtr->bg_req_queue);
        easy_list_init(&region_rlmtr->bg_rsp_queue);

        snprintf(region_rlmtr->region_name, RL_MAX_REGION_NAME_LEN, "%s", region);
        ev_init(&region_rlmtr->tx_trigger, easy_connection_on_send_rlmtr);
        region_rlmtr->tx_trigger.data = region_rlmtr;
    }
}


void easy_eio_region_rlmtr_fini(easy_region_ratelimitor_t *region_rlmtr)
{
    if (region_rlmtr == NULL) {
        easy_error_log("wrong parameters, region_rlmtr(%p).", region_rlmtr);
    } else {
        region_rlmtr->region_id = -1;
    }
}


void easy_eio_rm_region(easy_io_t *eio, int region_id)
{
    int i;
    struct easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_region_ratelimitor_t *region_rlmtr = NULL;

    if (eio == NULL) {
        easy_error_log("wrong parameters, eio(%p).", eio);
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
            if (easy_rlmtr->region_rlmtrs[i].region_id == region_id) {
                region_rlmtr = &(easy_rlmtr->region_rlmtrs[i]);
                region_rlmtr->region_id = -1;
            }
        }

        easy_spinrwlock_wrlock(&easy_rlmtr->s2r_hmap_rwlock);
        easy_rlmtr->s2r_map_version_latest++;
        easy_hash_clear(easy_rlmtr->s2r_hmap);

        easy_array_destroy(easy_rlmtr->region_array);
        easy_rlmtr->region_array = easy_array_create(sizeof(easy_region_ratelimitor_hmap_node_t));
        easy_spinrwlock_unlock(&easy_rlmtr->s2r_hmap_rwlock);
    }
    return;
}


void easy_eio_set_region_max_bw(easy_io_t *eio, int64_t max_bw, const char *region)
{
    int i;
    struct easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_region_ratelimitor_t *region_rlmtr = NULL;

    if ((eio == NULL) || (region == NULL)) {
        easy_error_log("wrong parameters, eio(%p), region(%p).", eio, region);
        goto out;
    }

    easy_info_log("easy_eio_set_region_max_bw, region(%s), max_bw(%ld).", region, max_bw);
    easy_rlmtr = eio->easy_rlmtr;
    for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
        if (easy_rlmtr->region_rlmtrs[i].region_id != -1) {
            if (0 == strncmp(easy_rlmtr->region_rlmtrs[i].region_name, region, RL_MAX_REGION_NAME_LEN)) {
                region_rlmtr = &(easy_rlmtr->region_rlmtrs[i]);
                easy_debug_log("Found region ratelimitor, i(%d), region(%s), region_rlmtr(%p).\n",
                        i, region, region_rlmtr);
                break;
            }
        }
    }

    if (region_rlmtr == NULL) {
        for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
            if (easy_rlmtr->region_rlmtrs[i].region_id == -1) {
                region_rlmtr = &(easy_rlmtr->region_rlmtrs[i]);
                easy_eio_region_rlmtr_init(region_rlmtr, region);
                easy_debug_log("Found empty region ratelimitor, i(%d), region(%s), region_rlmtr(%p), ready_queue_empty(%d).\n",
                        i, region, region_rlmtr, easy_list_empty(&region_rlmtr->ready_queue_node));
                break;
            }
        }
    }

    if (region_rlmtr == NULL) {
        easy_debug_log("Not found region ratelimitor, region(%s).\n", region);
    } else {
        region_rlmtr->max_bw = max_bw;
    }

out:
    return;
}


int easy_eio_set_rlmtr_thread(easy_io_t *eio)
{
    int ret = EASY_OK;
    easy_ratelimitor_t* easy_rlmtr = NULL;

    if (eio == NULL) {
        ret = EASY_ERROR;
        easy_error_log("wrong parameters, eio(%p).", eio);
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        easy_rlmtr->ratelimit_thread = eio->io_thread_pool->ratelimit_thread;
        easy_info_log("set ratelimit thread, ioth(%p, %d).\n",
                easy_rlmtr->ratelimit_thread, easy_rlmtr->ratelimit_thread->idx);
    }
    return ret;
}


void easy_eio_set_ratelimit_stat_period(easy_io_t *eio, int64_t stat_period)
{
    easy_ratelimitor_t* easy_rlmtr = NULL;

    if (eio == NULL) {
        easy_error_log("wrong parameters, eio(%p).", eio);
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        easy_rlmtr->stat_period = stat_period / (1000 * 1000);
        easy_rlmtr->cycles_per_period = (uint64_t) easy_rlmtr->cpu_freq * easy_rlmtr->stat_period;
        easy_rlmtr->cycles_per_stride = easy_rlmtr->cycles_per_period / RL_RECORD_RING_SIZE;
        easy_debug_log("easy_eio_set_ratelimit_stat_period, stat_period(%lf), cpu_freq(%lf), "
                "cycles_per_period(%ld), cycles_per_stride(%ld)",
                easy_rlmtr->stat_period, easy_rlmtr->cpu_freq,
                easy_rlmtr->cycles_per_period, easy_rlmtr->cycles_per_stride);
    }
    return;
}


void easy_eio_set_ratelimit_enable(easy_io_t *eio, int easy_ratelimit_enabled)
{
    easy_ratelimitor_t* easy_rlmtr = NULL;

    if (eio == NULL) {
        easy_error_log("wrong parameters, eio(%p).", eio);
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        easy_rlmtr->ratelimit_enabled = easy_ratelimit_enabled;
        if (easy_ratelimit_enabled) {
            easy_info_log("easy ratelimit enabled.");
        } else {
            easy_info_log("easy ratelimit disabled.");
        }
    }
    return;
}


int easy_eio_set_s2r_map_cb(easy_io_t *eio, easy_io_update_s2r_map_pt *update_s2r_map_cb,
        void *update_s2r_map_cb_args)
{
    int ret = EASY_OK;
    easy_ratelimitor_t* easy_rlmtr = NULL;

    if ((eio == NULL) || (update_s2r_map_cb == NULL) || (update_s2r_map_cb_args == NULL)) {
        easy_error_log("wrong parameters, eio(%p), update_s2r_map_cb(%p), update_s2r_map_cb_args(%p).",
                eio, update_s2r_map_cb, update_s2r_map_cb_args);
        ret = EASY_ERROR;
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        easy_rlmtr->update_s2r_map_cb      = update_s2r_map_cb;
        easy_rlmtr->update_s2r_map_cb_args = update_s2r_map_cb_args;
        easy_info_log("easy sets server2region map callback.");
    }
    return ret;
}


void easy_eio_s2r_map_changed(easy_io_t *eio)
{
    easy_ratelimitor_t* easy_rlmtr = NULL;

    if (eio == NULL) {
        easy_error_log("wrong parameters, eio(%p).", eio);
    } else {
        easy_rlmtr = eio->easy_rlmtr;
        easy_spinrwlock_wrlock(&easy_rlmtr->s2r_hmap_rwlock);
        easy_rlmtr->s2r_map_version_latest++;
        easy_spinrwlock_unlock(&easy_rlmtr->s2r_hmap_rwlock);
        easy_info_log("easy s2r map version changed, s2r_map_version_latest(%ld).",
                easy_rlmtr->s2r_map_version_latest);
    }
    return;
}


int easy_eio_get_region_rlmtr(easy_addr_t *addr, easy_region_ratelimitor_t **rlmtr)
{
    int ret = EASY_OK;
    uint64_t key;
    easy_ratelimitor_t* easy_rlmtr = NULL;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;
    easy_rlmtr = ioth->eio->easy_rlmtr;
    char dest_addr[32];

    if ((addr == NULL) || (rlmtr == NULL)) {
        easy_error_log("wrong parameters, addr(%p), rlmtr(%p).", addr, rlmtr);
        ret = EASY_ERROR;
        goto out;
    }

    easy_debug_log("easy_eio_get_region_rlmtr, addr(%s), s2r_map_version_cur(%ld), s2r_map_version_latest(%ld)",
            easy_inet_addr_to_str(addr, dest_addr, 32), easy_rlmtr->s2r_map_version_cur,
            easy_rlmtr->s2r_map_version_latest);

    if (easy_rlmtr->s2r_map_version_cur != easy_rlmtr->s2r_map_version_latest) {
        easy_spinrwlock_wrlock(&easy_rlmtr->s2r_hmap_rwlock);
        if (easy_rlmtr->s2r_map_version_cur != easy_rlmtr->s2r_map_version_latest) {
            easy_hash_clear(easy_rlmtr->s2r_hmap);
            easy_array_destroy(easy_rlmtr->region_array);
            easy_rlmtr->region_array = easy_array_create(sizeof(easy_region_ratelimitor_hmap_node_t));
            ret = easy_rlmtr->update_s2r_map_cb(easy_rlmtr->update_s2r_map_cb_args);
            if (ret != EASY_OK) {
                easy_error_log("Failed to update server2region map, addr(%s).",
                        easy_inet_addr_to_str(addr, dest_addr, 32));
                goto out;
            }
            easy_rlmtr->s2r_map_version_cur = easy_rlmtr->s2r_map_version_latest;
        }
        easy_spinrwlock_unlock(&easy_rlmtr->s2r_hmap_rwlock);
    }

    key = addr->u.addr;
    // key = (key << 32) | addr->port;
    easy_spinrwlock_rdlock(&easy_rlmtr->s2r_hmap_rwlock);
    *rlmtr = (easy_region_ratelimitor_t *)easy_hash_find(easy_rlmtr->s2r_hmap, key);
    if (*rlmtr == NULL) {
        ret = EASY_ERROR;
        easy_debug_log("Not found region ratelimitor, addr(%s), key(%lx), region_rlmtr(%p)",
                       easy_inet_addr_to_str(addr, dest_addr, 32), key, *rlmtr);
    } else {
        easy_debug_log("easy_hash_find, addr(%s), key(%lx), region_rlmtr(%p)",
                       easy_inet_addr_to_str(addr, dest_addr, 32), key, *rlmtr);
    }
out:
    easy_spinrwlock_unlock(&easy_rlmtr->s2r_hmap_rwlock);
    return ret;
}


void easy_eio_set_s2r_map(easy_io_t *eio, easy_addr_t *addr, char *region)
{
    int i;
    uint64_t key;
    easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_region_ratelimitor_t *region_rlmtr = NULL;
    // easy_region_ratelimitor_hmap_node_t *region_node = NULL;
    char dest_addr[32];

    if ((eio == NULL) || (addr == NULL) || (region == NULL)) {
        easy_error_log("wrong parameters, eio(%p), addr(%p), update_s2r_map_cb_args(%p).",
                eio, addr, region);
        goto out;
    }

    easy_info_log("easy_eio_set_s2r_map, addr(%s), region(%s)",
            easy_inet_addr_to_str(addr, dest_addr, 32), region);

    easy_rlmtr = eio->easy_rlmtr;
    key = addr->u.addr;
    // key = (key << 32) | addr->port;
    region_rlmtr = (easy_region_ratelimitor_t *)easy_hash_find(easy_rlmtr->s2r_hmap, key);
    if (region_rlmtr) {
        easy_info_log("Duplicted server2region mapping, addr(%s), region(%s)",
                easy_inet_addr_to_str(addr, dest_addr, 32), region);
        goto out;
    }

    for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
        region_rlmtr = &easy_rlmtr->region_rlmtrs[i];
        if (region_rlmtr->region_id != -1) {
            if (0 == strncmp(region_rlmtr->region_name, region, RL_MAX_REGION_NAME_LEN)) {
                // region_node = easy_array_alloc(easy_rlmtr->region_array);
                // region_node->region_rlmtr = region_rlmtr;
                easy_debug_log("easy adds server2region hash map, i(%d), addr(%s), key(%lx), region(%s), region_rlmtr(%p)",
                        i, easy_inet_addr_to_str(addr, dest_addr, 32), key, region, region_rlmtr);
                easy_hash_add(easy_rlmtr->s2r_hmap, key, &region_rlmtr->hash_list_node);
                if (easy_list_empty(&region_rlmtr->ready_queue_node)) {
                    easy_debug_log("ready_queue_node is empty, i(%d), addr(%s), key(%lx), region(%s), region_rlmtr(%p)",
                            i, easy_inet_addr_to_str(addr, dest_addr, 32), key, region, region_rlmtr);
                } else {
                    easy_debug_log("ready_queue_node is not empty, i(%d), addr(%s), key(%lx), region(%s), region_rlmtr(%p)",
                            i, easy_inet_addr_to_str(addr, dest_addr, 32), key, region, region_rlmtr);
                }
                break;
            }
        }
    }
out:
    return;
}


int easy_eio_get_region_bw(easy_io_t *eio, const char* region_name, int64_t *bw, int64_t *max_bw)
{
    int ret = EASY_OK;
    struct easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_region_ratelimitor_t *region_rlmtr = NULL;
    uint64_t ring_idx, record_idx, first_stride_idx;
    uint64_t cycles, cycles_latest, cycles_per_period;
    uint64_t tx_bytes, tx_bytes_latest;
    // easy_io_thread_t *ioth = EASY_IOTH_SELF;
    int i;

    if ((eio == NULL) || (region_name == NULL) || (bw == NULL) || (max_bw == NULL)) {
        easy_error_log("wrong parameters, eio(%p), region_name(%p), bw(%p), max_bw(%p).",
                eio, region_name, bw, max_bw);
        ret = EASY_ERROR;
        goto out;
    }

    easy_rlmtr = eio->easy_rlmtr;
    for (i = 0; i < RL_MAX_REGION_COUNT; i++) {
        if (easy_rlmtr->region_rlmtrs[i].region_id != -1) {
            if (0 == strncmp(easy_rlmtr->region_rlmtrs[i].region_name, region_name, RL_MAX_REGION_NAME_LEN)) {
                region_rlmtr = &(easy_rlmtr->region_rlmtrs[i]);
                break;
            }
        }
    }

    if (region_rlmtr == NULL) {
        easy_info_log("Not found region ratelimitor, region_name(%s).\n", region_name);
        ret = EASY_ERROR;
        goto out;
    }

    tx_bytes          = 0;
    cycles            = 0;
    record_idx        = region_rlmtr->record_count;
    tx_bytes_latest   = region_rlmtr->tx_bytes;
    cycles_latest     = easy_get_cycles();
    cycles_per_period = easy_rlmtr->cycles_per_period;

    if (region_rlmtr->record_count < (RL_RECORD_RING_SIZE - 1)) {
        first_stride_idx = 0;
    } else {
        first_stride_idx = region_rlmtr->record_count - (RL_RECORD_RING_SIZE - 1);
    }

    while (record_idx >= first_stride_idx) {
        ring_idx = record_idx & (RL_RECORD_SLOTS - 1);
        cycles   = cycles_latest   - region_rlmtr->record_ring[ring_idx].cycles;
        tx_bytes = tx_bytes_latest - region_rlmtr->record_ring[ring_idx].rx_bytes;
        easy_debug_log("easy_eio_get_region_bw, record_idx(%lx), cycles(%ld, %ld), tx_bytes(%ld, %ld).\n",
                record_idx, cycles, region_rlmtr->record_ring[ring_idx].cycles, tx_bytes, region_rlmtr->record_ring[ring_idx].rx_bytes);

        if (cycles > cycles_per_period) {
            break;
        }

        if (record_idx == 0) {
            break;
        }

        record_idx--;
    }

    *bw = (tx_bytes * easy_rlmtr->cpu_freq) / cycles;
    *max_bw = region_rlmtr->max_bw;
    easy_info_log("easy_eio_get_region_bw, cycles_per_period(%ld), record_count(%ld), region_name(%s), bw(%ld), max_bw(%ld).\n",
            cycles_per_period, region_rlmtr->record_count, region_name, *bw, *max_bw);

out:
    return ret;
}


int easy_eio_is_region_speeding(easy_region_ratelimitor_t *region_rlmtr, int do_stop)
{
    int ret = 0;
    uint64_t ring_idx, record_idx, first_stride_idx;
    uint64_t cycles, cycles_now, cycles_latest, cycles_per_period;
    uint64_t tx_bytes, tx_bytes_latest, tx_bytes_per_period;
    uint64_t restart_strides;
    easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;

    if (region_rlmtr == NULL) {
        easy_error_log("wrong parameters, region_rlmtr(%p).", region_rlmtr);
        goto out;
    }

    if (region_rlmtr->tx_trigger_armed) {
        ret = 1;
        goto out;
    }

    easy_rlmtr      = ioth->eio->easy_rlmtr;
    record_idx      = region_rlmtr->record_count;
    tx_bytes_latest = region_rlmtr->tx_bytes;
    cycles_latest   = cycles_now = easy_get_cycles();
    tx_bytes_per_period = region_rlmtr->max_bw * easy_rlmtr->stat_period; // Todo: perf tune.
    cycles_per_period   = easy_rlmtr->cycles_per_period;
    if (region_rlmtr->record_count < (RL_RECORD_RING_SIZE - 1)) {
        first_stride_idx = 0;
    } else {
        first_stride_idx = region_rlmtr->record_count - (RL_RECORD_RING_SIZE - 1);
    }

    while (record_idx >= first_stride_idx) {
        ring_idx = record_idx & (RL_RECORD_SLOTS - 1);
        cycles   = cycles_latest   - region_rlmtr->record_ring[ring_idx].cycles;
        tx_bytes = tx_bytes_latest - region_rlmtr->record_ring[ring_idx].rx_bytes;

        if (cycles > cycles_per_period) {
            break;
        }

        if (tx_bytes > tx_bytes_per_period) {
            ret = 1;
            break;
        }

        if (record_idx == 0) {
            break;
        }

        record_idx--;
    }

    easy_debug_log("easy_eio_is_region_speeding, region_rlmtr(%s), record_idx(%ld), first_stride_idx(%ld),"
            "tx_bytes_per_period(%ld), cycles_per_period(%ld), ret(%d), do_stop(%d), ioth(%d), recv/disp(%ld, %ld).\n",
            region_rlmtr->region_name, region_rlmtr->record_count, first_stride_idx, tx_bytes_per_period,
            cycles_per_period, ret, do_stop, ioth->idx, region_rlmtr->received_req_count, region_rlmtr->dispatched_req_count);
    if (ret && do_stop) {
        region_rlmtr->tx_trigger_armed = 1;
        easy_debug_log("rearm tx_trigger of region ratelimit, region_rlmtr(%p), repeat(%lf).",
                region_rlmtr, region_rlmtr->tx_trigger.repeat);
        restart_strides = RL_RECORD_RING_SIZE - (region_rlmtr->record_count - record_idx);
        region_rlmtr->tx_trigger.repeat = easy_rlmtr->stat_period * restart_strides / RL_RECORD_RING_SIZE;
        easy_info_log("rearm tx_trigger of region ratelimit, region_rlmtr(%s), repeat(%lf), record_count(%ld), record_idx(%ld),"
                ".",
                region_rlmtr->region_name, region_rlmtr->tx_trigger.repeat, region_rlmtr->record_count, record_idx);
        ev_timer_again (ioth->loop, &region_rlmtr->tx_trigger);
    }

out:
    return ret;
}


void easy_eio_record_region_speed(easy_region_ratelimitor_t *region_rlmtr)
{
    uint64_t ring_idx;
    uint64_t cycles, cycles_now, cycles_per_stride;
    easy_ratelimitor_t *easy_rlmtr = NULL;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;
    int do_record = 0;

    if (region_rlmtr == NULL) {
        easy_error_log("wrong parameters, region_rlmtr(%p).", region_rlmtr);
        goto out;
    }

    easy_rlmtr = ioth->eio->easy_rlmtr;
    cycles_per_stride = easy_rlmtr->cycles_per_stride;

    ring_idx   = region_rlmtr->record_count & (RL_RECORD_SLOTS - 1);
    cycles     = region_rlmtr->record_ring[ring_idx].cycles;
    cycles_now = easy_get_cycles();

    if ((region_rlmtr->record_count == 0) && (cycles == 0)) {
        do_record = 1;
    } else if (cycles_now > (cycles + cycles_per_stride)) {
        region_rlmtr->record_count++;
        ring_idx = region_rlmtr->record_count & (RL_RECORD_SLOTS - 1);
        do_record = 1;
    }

    easy_debug_log("easy_eio_record_region_speed, region_rlmtr(%p, %s), record_count(%ld), cycles_now(%ld)"
            ", cycles(%ld), cycles + cycles_per_stride(%ld), cycles_per_stride(%ld), cycles_per_period(%ld), do_record(%d).",
            region_rlmtr, region_rlmtr->region_name, region_rlmtr->record_count, cycles_now, cycles, (cycles + cycles_per_stride), cycles_per_stride, 
            easy_rlmtr->cycles_per_period, do_record);

    if (do_record) {
        region_rlmtr->record_ring[ring_idx].cycles   = cycles_now;
        region_rlmtr->record_ring[ring_idx].rx_bytes = region_rlmtr->tx_bytes;
        easy_debug_log("easy_eio_record_region_speed, region_rlmtr(%p), record_count(%ld), cycles(%ld), tx_bytes(%ld).",
                region_rlmtr, region_rlmtr->record_count, cycles_now, cycles, region_rlmtr->tx_bytes);
    }

out:
    return;
}


/**
 * 取线程的ev_loop,  要在easy_io_init后调用
 */
struct ev_loop *easy_eio_thread_loop(easy_io_t *eio, int index)
{
    easy_io_thread_t        *ioth;
    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, index);
    return (ioth ? ioth->loop : NULL);
}

/**
 * 起处理速度定时器
 */
void easy_eio_stat_watcher_start(easy_io_t *eio, ev_timer *stat_watcher, double interval,
                                 easy_io_stat_t *iostat, easy_io_stat_process_pt *process)
{
    easy_io_thread_t        *ioth;

    memset(iostat, 0, sizeof(easy_io_stat_t));
    iostat->last_cnt = 0;
    iostat->last_time = eio->start_time;
    iostat->process = process;
    iostat->eio = eio;

    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, 0);
    ev_timer_init(stat_watcher, easy_io_stat_process, 0., interval);
    stat_watcher->data = iostat;
    ev_timer_start(ioth->loop, stat_watcher);
    easy_baseth_on_wakeup(ioth);
}

/**
 * 设置用户态线程开始
 */
void easy_eio_set_uthread_start(easy_io_t *eio, easy_io_uthread_start_pt *on_utstart, void *args)
{
    easy_io_thread_t        *ioth;

    eio->uthread_enable = 1;
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        ioth->on_utstart = on_utstart;
        ioth->ut_args = args;
    }
}
//////////////////////////////////////////////////////////////////////////////
/**
 * IO线程的回调程序
 */
static void *easy_io_on_thread_start(void *args)
{
    easy_listen_t           *l;
    easy_io_thread_t        *ioth;
    easy_io_t               *eio;

    ioth = (easy_io_thread_t *) args;
    easy_baseth_self = (easy_baseth_t *) args;
    eio = ioth->eio;

    if (eio->block_thread_signal) {
        pthread_sigmask(SIG_BLOCK, &eio->block_thread_sigset, NULL);
    }

    // sched_setaffinity
    if (eio->affinity_enable) {
        static easy_atomic_t    cpuid = -1;
        int                     cpunum = sysconf(_SC_NPROCESSORS_CONF);
        cpu_set_t               mask;
        int                     idx = (easy_atomic_add_return(&cpuid, 1) & 0x7fffffff) % cpunum;
        CPU_ZERO(&mask);
        CPU_SET(idx, &mask);

        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            easy_error_log("sched_setaffinity error: %d (%s), cpuid=%d\n", errno, strerror(errno), cpuid);
        }
    }

    // 有listen
    if (eio->listen) {
        // 监听切换timer
        int                     ts = (eio->listen_all || eio->io_thread_count == 1);

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

    // 允许使用用户线程
    if (eio->uthread_enable) {
        easy_uthread_control_t  control;
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
        if (ioth->on_utstart) ioth->on_utstart(ioth->ut_args);

        ev_run(ioth->loop, 0);
    }

    easy_baseth_self = NULL;

    easy_debug_log("pthread exit: %lx\n", pthread_self());

    return (void *)NULL;
}


/**
 * 用户态线程(uthread)的io函数
 */
static void easy_io_on_uthread_start(void *args)
{
    easy_io_thread_t        *ioth = (easy_io_thread_t *)args;

    if (ioth->on_utstart) {
        (ioth->on_utstart)(ioth->ut_args);
    }
}


static void easy_io_on_uthread_evstart(void *args)
{
    ev_run((struct ev_loop *)args, 0);
}


/**
 * 把io_thread_t释放掉
 */
static void easy_io_thread_destroy(easy_io_thread_t *ioth)
{
    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;

    // session at ioth
    easy_spin_lock(&ioth->thread_lock);
    easy_list_for_each_entry_safe(s, s1, &ioth->session_list, session_list_node) {
        easy_list_del(&s->session_list_node);

        if (s->status && s->pool) {
            easy_pool_destroy(s->pool);
        }
    }
    // connection at ioth
    easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
        EASY_CONNECTION_DESTROY(c, "thread_destroy");
    }
    // foreach connected_list
    easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
        EASY_CONNECTION_DESTROY(c, "thread_destroy");
    }
    easy_spin_unlock(&ioth->thread_lock);

    easy_array_destroy(ioth->client_array);
}

/**
 * 统计处理函数
 */
static void easy_io_stat_process(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_io_stat_t          *iostat;
    ev_tstamp               last_time, t1, t2;
    int64_t                 last_cnt;
    easy_io_thread_t        *ioth;
    easy_io_t               *eio;

    iostat = (easy_io_stat_t *)w->data;
    eio = iostat->eio;

    // 统计当前值
    last_time = ev_now(loop);
    last_cnt = 0;
    int                     ql = 0;
    easy_connection_t       *c;
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        last_cnt += (ioth->tx_done_request_count + ioth->rx_done_request_count);
        easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node) {
            ql += c->con_summary->doing_request_count;
        }
    }

    t1 = last_time - iostat->last_time;
    t2 = last_time - eio->start_time;

    // 保存起来
    iostat->last_speed = (last_cnt - iostat->last_cnt) / t1;
    iostat->total_speed = last_cnt / t2;

    iostat->last_cnt = last_cnt;
    iostat->last_time = last_time;

    if (iostat->process == NULL) {
        easy_info_log("cnt: %" PRId64 ", speed: %.2f, total_speed: %.2f, ql:%d\n",
                      iostat->last_cnt, iostat->last_speed, iostat->total_speed, ql);
    } else {
        (iostat->process)(iostat);
    }
}

static void easy_signal_handler(int sig)
{
    easy_io_t               *eio, *e1;

    if (easy_trylock(&easy_io_list_lock) == 0) {
        return;
    }

    if (sig == SIGINT || sig == SIGTERM) {
        easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node) {
            easy_eio_stop(eio);
        }
    } else if (sig == 39) {
        easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node) {
            easy_io_print_status(eio);
        }
    }

    easy_unlock(&easy_io_list_lock);
}

// uthread的处理函数
static void easy_io_uthread_invoke(struct ev_loop *loop)
{
    easy_baseth_t           *th = (easy_baseth_t *) ev_userdata(loop);

    // 是否退出
    if (th->eio->stoped) {
        ev_break(loop, EVBREAK_ALL);
        easy_uthread_stop();
        return;
    }

    ev_invoke_pending(loop);

    while (easy_uthread_yield() > 0);
}

/**
 * 打出connection信息
 */
static void easy_io_print_status(easy_io_t *eio)
{
    easy_connection_t       *c;
    easy_io_thread_t        *ioth;

    // foreach connected_list
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        easy_info_log("thread:%d, doing: %d/%d, done: %" PRIu64 "/%" PRIu64 "\n", ioth->idx,
                      ioth->tx_doing_request_count, ioth->rx_doing_request_count,
                      ioth->tx_done_request_count, ioth->rx_done_request_count);
        easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node) {
            easy_info_log("%d %s => doing: %d, done:%" PRIu64 "\n", ioth->idx, easy_connection_str(c),
                          c->con_summary->doing_request_count, c->con_summary->done_request_count);
        }
    }
}

static void easy_listen_close(easy_listen_t *l)
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
