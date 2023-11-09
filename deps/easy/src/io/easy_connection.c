#include "util/easy_string.h"
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include "io/easy_io.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"
#include "io/easy_request.h"
#include "io/easy_file.h"
#include "io/easy_client.h"
#include "io/easy_socket.h"
#include "io/easy_ssl.h"
#include "io/easy_log.h"
#include "packet/http/easy_http_handler.h"
#include "util/easy_util.h"
#include "io/easy_maccept.h"
#include "io/easy_negotiation.h"
#include "util/easy_mod_stat.h"


static void easy_switch_listen(void *data);
static easy_connection_t *easy_connection_new();
static void easy_connection_on_timeout_session(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_connection_on_timeout_conn(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_connection_on_pause(struct ev_loop *loop, ev_timer *w, int revents);
static int easy_connection_redispatch_thread(easy_connection_t *c);
static void easy_connection_evio_start(easy_connection_t *c);
static int easy_connection_do_request(easy_message_t *m);
static int easy_connection_do_response(easy_message_t *m);
static int easy_connection_send_response(easy_list_t *request_list);

static easy_message_t *easy_connection_recycle_message(easy_message_t *m);
static easy_connection_t *easy_connection_do_connect(easy_client_t *client, int afd, int is_ssl_for_test);
static easy_connection_t *easy_connection_do_client(easy_session_t *s);
static void easy_connection_autoconn(easy_connection_t *c);
static int easy_connection_process_request(easy_connection_t *c, easy_list_t *list);
static int easy_connection_sendsocket(easy_connection_t *c);
static int easy_connection_listen_dispatch(easy_io_t *eio, easy_addr_t addr, easy_listen_t *listen);
static void easy_connection_listen_watcher(easy_session_t *s);
static int easy_connection_accept_one(struct ev_loop *loop, ev_io *w);
static int easy_connection_do_accept_one(struct ev_loop *loop, ev_io *w, easy_listen_simple_t* listen, int fd, easy_addr_t addr);
static int easy_connection_checkself(easy_connection_t *c);
static void easy_connection_dump_slow_request(easy_connection_t *c);
static void easy_session_on_write_success(easy_session_t *s);
static int easy_connection_send_rlmtr(easy_io_thread_t *ioth);

#define CONN_DESTROY_LOG(msg) easy_error_log("easy_destroy_conn: %s %s", msg, easy_connection_str(c))
#define SERVER_PROCESS(c, r) ({ EASY_STAT_TIME_GUARD(ev_server_process_count, ev_server_process_time); (c->handler->process(r)); })
inline int64_t current_time()
{
    struct timeval t;
    if (gettimeofday(&t, NULL) < 0) {
        easy_error_log("get time of day failed");
    }
    return ((t.tv_sec) * 1000000 + t.tv_usec);
}

__thread easy_hash_t* thread_local_send_queue;

/**
 * 增加监听端口, 要在easy_io_start开始调用
 *
 * @param host  机器名或IP, 或NULL
 * @param port  端口号
 *
 * @return      如果成功返回easy_listen_t对象, 否则返回NULL
 */
easy_listen_t *easy_connection_add_listen(easy_io_t *eio,
        const char *host, int port, easy_io_handler_pt *handler)
{
    return easy_add_listen(eio, host, port, handler, NULL);
}

easy_listen_t *easy_connection_listen_addr(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler)
{
    int                     udp = ((handler && handler->is_udp) ? 1 : 0);
    return easy_add_listen_addr(eio, addr, handler, udp, NULL);
}

easy_listen_t *easy_add_listen(easy_io_t *eio, const char *host, int port,
                               easy_io_handler_pt *handler, void *args)
{
    easy_addr_t             address;
    int                     udp;

    udp = ((handler && handler->is_udp) ? 1 : 0);

    if (host == NULL) {
        if (eio->support_ipv6) {
            host = "[]";
        }
    } else if (memcmp(host, "unix:", 5) == 0) {
        address = easy_inet_str_to_addr(NULL, UINT16_MAX);
        return easy_add_listen_addr(eio, address, handler, 0, (void*)(host + 5));
    } else if (memcmp(host, "udp:", 4) == 0 || memcmp(host, "tcp:", 4) == 0) {
        udp = (*host == 'u');
        host += 4;
    }

    if ((address = easy_inet_str_to_addr(host, port)).family == 0) {
        easy_trace_log("error addr: host=%s, port=%d.\n", host, port);
        return NULL;
    }

    return easy_add_listen_addr(eio, address, handler, udp, args);
}

static int easy_connection_maccept_one(struct ev_loop *loop, ev_io *w)
{
    int                     fd;
    easy_listen_simple_t    *listen;

    listen = (easy_listen_simple_t *)w->data;

    if (read(w->fd, &fd, sizeof(fd)) != sizeof(fd)) {
        return EASY_AGAIN;
    }
    return easy_connection_do_accept_one(loop, w, listen, fd, easy_inet_getpeername(fd));
}


static void easy_connection_on_maccept(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_listen_simple_t    *listen;
    int                     cnt;

    listen = (easy_listen_simple_t *)w->data;
    if (listen->accept_count > 0) {
        cnt = listen->accept_count;
    } else {
        cnt = (listen->reuseport ? 32 : 5);
    }

    do {
        if (easy_connection_maccept_one(loop, w) < 0) {
            break;
        }

        cnt--;
    } while (cnt > 0);
}

/**
 * 通过easy_addr_t创建easy_listen_t
 */
easy_listen_t *easy_add_listen_addr(easy_io_t *eio, easy_addr_t addr,
                                    easy_io_handler_pt *handler, int udp, void *args)
{
    int                     i, size, cnt, fd;
    int                     flags = (eio->tcp_defer_accept ? EASY_FLAGS_DEFERACCEPT : 0);
    char                    buffer[32];
    easy_listen_t           *l;

    if (eio->pool == NULL) {
        easy_error_log("easy_connection_add_listen failure: eio->started=%d, eio->pool=%p\n",
                eio->started, eio->pool);
        return NULL;
    }

    // alloc memory
    cnt = eio->io_thread_count;
    size = cnt * sizeof(ev_io);
    size += sizeof(easy_listen_t);

    if ((l = (easy_listen_t *) easy_pool_calloc(eio->pool, size)) == NULL) {
        easy_error_log("easy_pool_calloc failure: eio->pool=%p, size=%d\n",
                eio->pool, size);
        return NULL;
    }

    // 打开监听
    l->addr = addr;
    l->handler = handler;

    if (eio->no_reuseport == 0) {
        flags |= EASY_FLAGS_NOLISTEN;
    }

    uint16_t port = ntohs(addr.port);
    if (port < 1024) {
        fd = -1;
        flags |= EASY_FLAGS_REUSEPORT;
    } else if (port >= UINT16_MAX) {
        if ((fd = easy_unix_domain_listen((char*)args, eio->listen_backlog)) < 0) {
            easy_error_log("easy_socket_listen unix domain failure: addr=%s\n", (char*)args);
            return NULL;
        }
    } else if ((fd = easy_socket_listen(udp, &l->addr, &flags, eio->listen_backlog)) < 0) {
        easy_error_log("easy_socket_listen failure: host=%s\n", easy_inet_addr_to_str(&l->addr, buffer, 32));
        return NULL;
    } else if (udp == 0 && eio->tcp_keepalive == 1) {
        // open tcp_keepalive
        if (easy_socket_set_opt(fd, SO_KEEPALIVE, 1)) {
            easy_error_log("set SO_KEEPALIVE error: %d, fd=%d\n", errno, fd);
        } else {
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPIDLE, eio->tcp_keepidle));
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPINTVL, eio->tcp_keepintvl));
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPCNT, eio->tcp_keepcnt));
        }
    }

    // 初始化
    for (i = 0; i < cnt; i++) {
        if (udp) {
            ev_io_init(&l->read_watcher[i], easy_connection_on_udpread, fd, EV_READ | EV_CLEANUP);
        } else {

            ev_io_init(&l->read_watcher[i], easy_connection_on_accept, fd, EV_READ | EV_CLEANUP);
        }

        ev_set_priority(&l->read_watcher[i], EV_MAXPRI);
        l->read_watcher[i].data = l;
    }

    if (eio->no_reuseport == 0) {
        l->reuseport = (flags & EASY_FLAGS_REUSEPORT) ? 1 : 0;
    }

    l->fd = fd;
    l->accept_count = eio->accept_count;

    if (!l->reuseport) {
        easy_info_log("easy_socket_listen: host=%s, fd=%d", easy_inet_addr_to_str(&addr, buffer, 32), fd);
    }

    if (eio->started) {
        if (l->reuseport) {
            for (i = 0; i < eio->io_thread_count; i++) {
                addr.cidx = i;
                easy_connection_listen_dispatch(eio, addr, l);
            }

            i = 50;

            while (l->bind_port_cnt < eio->io_thread_count && i-- > 0) {
                usleep(1000);
            }
        } else {
            addr.cidx = 0;
            easy_connection_listen_dispatch(eio, addr, l);
        }
    } else {
        l->next = eio->listen;
        eio->listen = l;
    }

    return l;
}

void easy_connection_on_accept_pipefd(struct ev_loop *loop, ev_io *w, int revents)
{
    int conn_fd = -1;
    easy_listen_simple_t *listen = NULL;
    int n = 0;
    int ret = -1;
    uint8_t index = 0;
    struct sockaddr_storage addr;
    socklen_t addr_len = 0;
    easy_addr_t             eaddr;
    easy_addr_t             e_peer_addr;
    char dest_addr[32];
    char src_addr[32];
    easy_baseth_t *ioth = easy_baseth_self;
    easy_io_t *eio = ioth->eio;

    addr_len = sizeof(addr);
    listen = (easy_listen_simple_t *) w->data;
    index = (uint8_t)ioth->idx;

    memset(&addr, 0, sizeof(addr));
    memset(&eaddr, 0, sizeof(eaddr));
    memset(&e_peer_addr, 0, sizeof(e_peer_addr));
    memset(dest_addr, 0, sizeof(dest_addr));
    memset(src_addr, 0, sizeof(src_addr));

    while ((n = read(w->fd, &conn_fd, sizeof(conn_fd))) < 0 && errno == EINTR);

    if (n != sizeof(conn_fd)) {
        easy_error_log("read connect fd error:%d!", errno);
        return;
    }

    easy_consume_negotiation_msg(conn_fd, eio);

    if (getsockname(conn_fd, (struct sockaddr *)&addr, &addr_len) == 0) {
        easy_inet_atoe(&addr, &eaddr);
        memset(&addr, 0, sizeof(addr));

        if (getpeername(conn_fd, (struct sockaddr *)&addr, &addr_len) == 0) {
            easy_inet_atoe(&addr, &e_peer_addr);
            easy_info_log("Accepted connect request, server addr (%s), client addr(%s), my eio magic(%#llx), thread index:%hhu.",
            easy_inet_addr_to_str(&eaddr, dest_addr, 32), easy_inet_addr_to_str(&e_peer_addr, src_addr, 32), eio->magic, index);
        } else {
            easy_error_log("getpeername failed! fd:%d, errno:%d", conn_fd, errno);
            return;
        }

        if ((ret = easy_connection_do_accept_one(loop, w, listen, conn_fd, e_peer_addr)) < 0) {
            easy_error_log("easy_connection_do_accept_one, ret:%d", ret);
            return;
        }
    } else {
        easy_error_log("get socket name failed! fd:%d, errno:%d", conn_fd, errno);
        return;
    }

}

easy_listen_t *easy_add_pipefd_listen_for_connection(easy_io_t *eio, easy_io_handler_pt *handler,
                                                    easy_io_threads_pipefd_pool_t *pipefd_pool)
{
    int cnt = 0;
    int size = 0;
    int i = 0;
    easy_listen_t *l = NULL;

    cnt = eio->io_thread_count;
    size = cnt * sizeof(ev_io);
    size += sizeof(easy_listen_t);

    if ((l = (easy_listen_t *)easy_pool_calloc(eio->pool, size)) == NULL) {
         easy_error_log("easy_pool_calloc failure: eio->pool=%p, size=%d",
                eio->pool, size);
        return NULL;
    }

    l->handler = handler;

    for (i = 0; i < cnt; i++) {
        ev_io_init(&l->read_watcher[i], easy_connection_on_accept_pipefd, pipefd_pool->pipefd[i], EV_READ | EV_CLEANUP);
        easy_info_log("register success! eio:%#llx, thread index:%d, piperdfd:%d", eio->magic, i, pipefd_pool->pipefd[i]);
        ev_set_priority(&l->read_watcher[i], EV_MAXPRI);
        l->read_watcher[i].data = l;
    }

    l->accept_count = eio->accept_count;
    l->reuseport = 1;
    l->addr.port = 0; //in thread func, do not generate new sockfd, just listen on pipe fd
    l->next = eio->listen;
    eio->listen = l;

    return l;
}
/**
 * session_timeout
 */
void easy_connection_wakeup_session(easy_connection_t *c, int err)
{
    easy_session_t          *s, *sn;
    easy_request_t          *r, *rn;

    if (c->type == EASY_TYPE_CLIENT && c->send_queue) {
        easy_list_for_each_entry_safe(s, sn, &(c->client_session_list), send_queue_list) {
            easy_hash_del(c->send_queue, s->packet_id);
            easy_session_process(s, 1, err);
        }
        easy_list_init(&c->client_session_list);
    }

    /*
     *
     * if (c->type == EASY_TYPE_CLIENT) {
     *     return;
     * } else {
     *     if (c->conn_has_error == 0 || easy_list_empty(&c->server_session_list) {
     *         return;
     *     }
     * }
     *
     */
    if (c->type != EASY_TYPE_SERVER || c->conn_has_error == 0 ||
            easy_list_empty(&c->server_session_list)) {
        return;
    }

    /*
     * Wakeup the easy_requests with retcode == EASY_AGAIN, which are returned by
     * upper-layer modules on server side.
     */
    easy_list_for_each_entry_safe(r, rn, &c->server_session_list, request_list_node) {
        if (r->waiting == 0) {
            continue;
        }

        easy_list_del(&r->request_list_node);
        // SERVER_PROCESS(c, r);
        {
            EASY_STAT_TIME_GUARD(ev_server_process_count, ev_server_process_time);
            c->handler->process(r);
        }
    }
}

/**
 * destroy掉easy_connection_t对象
 *
 * @param c - easy_connection_t对象
 */
void easy_connection_destroy(easy_connection_t *c, const char* msg)
{
    easy_message_t          *m, *m2;
    easy_io_t               *eio;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    void *ev_timer_pending_addr;
    int ev_timer_pending;

    if ((c->pool->ref == 0) || (c->destroy_retry_count < MAX_LOG_RETRY_COUNT)) {
        easy_info_log("Destroying connection, conn(%s), reason(%s).", easy_connection_str(c), msg);
    }

    // wake up the armed sessions.
    if (c->keepalive_failed) {
        easy_connection_wakeup_session(c, EASY_KEEPALIVE_ERROR);
    } else {
        easy_connection_wakeup_session(c, EASY_DISCONNECT);
    }

    // disconnect
    eio = c->ioth->eio;
    if (c->status != EASY_CONN_CLOSE && c->handler->on_disconnect) {
        (c->handler->on_disconnect)(c);
    }

    // refcount
    if (likely(eio->stoped == 0)) {
        if (c->status != EASY_CONN_CLOSE && c->pool->ref > 0) {
            ev_io_stop(c->loop, &c->read_watcher);
            ev_io_stop(c->loop, &c->write_watcher);

            if (c->pool->ref > 0) {
                ev_timer_set(&c->timeout_watcher, 0.0, 0.5);
                ev_timer_again(c->loop, &c->timeout_watcher);
            }
        }

        if (c->status != EASY_CONN_CLOSE) {
            c->last_time = ev_now(c->loop);
            c->status = EASY_CONN_CLOSE;
        }

        if (c->pool->ref > 0) {
            if ((c->destroy_retry_count < MAX_LOG_RETRY_COUNT) || (0 == (c->destroy_retry_count & 0x3FF))) {
                easy_info_log("Destroying connection not done due to pool->ref(%d), retry(%d), conn(%p).",
                        c->pool->ref, c->destroy_retry_count, c);
            }

            c->destroy_retry_count++;
            return;
        }
    }

    if (1 == eio->stoped && c->pool->ref > 0) {
        easy_info_log("stop process, directly return!eio->shutdown(%d), eio->stoped(%d), ref_count(%d)",
                        eio->shutdown, eio->stoped, c->pool->ref);
        return;
    }

    // release message
    if (easy_list_empty(&c->output) == 0) {
        easy_debug_log("Clearing output buffer chain, conn(%p), ioth(%d, %d).", c, ioth->idx, c->ioth->idx);
        easy_buf_chain_clear(&c->output);
    }

    easy_list_for_each_entry_safe(m, m2, &c->message_list, message_list_node) {
        if (eio->stoped) {
            m->pool->ref = 1;
        }

        easy_message_destroy(m, 1);
    }
    easy_list_init(&c->message_list);
    ev_io_stop(c->loop, &c->read_watcher);
    ev_io_stop(c->loop, &c->write_watcher);
    ev_timer_stop(c->loop, &c->timeout_watcher);
    ev_timer_pending_addr = ev_watch_pending_addr;
    ev_timer_pending      = ev_watch_pending;
    ev_timer_stop(c->loop, &c->pause_watcher);

    //clean summary
    easy_summary_destroy_node(c->fd, eio->eio_summary);

    // close
    if (c->fd >= 0) {
        if (!c->read_eof) {
            char buf[EASY_POOL_PAGE_SIZE];
            while (read(c->fd, buf, EASY_POOL_PAGE_SIZE) > 0);
        }

        easy_info_log("Socket closed, fd(%d), conn(%s), ev_is_pending(%d), ev_is_active(%d),"
                " ev_timer_pending_addr(%p), ev_timer_pending(%d), timeout_watcher(%p).",
                c->fd, easy_connection_str(c), ev_is_pending(&c->timeout_watcher),
                ev_is_active(&c->timeout_watcher), ev_timer_pending_addr,
                ev_timer_pending, &c->timeout_watcher);
        close(c->fd);
        c->fd = -1;
        ev_timer_init(&c->timeout_watcher, NULL, 0., 0.);
    }

    // autoreconn
    if (c->auto_reconn && eio->stoped == 0) {
        c->status = EASY_CONN_AUTO_CONN;
        double t = c->reconn_time / 1000.0 * (1 << c->reconn_fail);

        if (t > 30) {
            t = 30;
        }

        if (c->reconn_fail < 16) {
            c->reconn_fail++;
        }

        easy_info_log("start autoreconn, conn(%s), reconn_time(%f), reconn_fail(%d).\n", easy_connection_str(c), t, c->reconn_fail);
        ev_timer_set(&c->timeout_watcher, 0.0, t);
        ev_timer_again(c->loop, &c->timeout_watcher);
        c->armed_ack_timeout = -1;
        return;
    }

    easy_list_del(&c->conn_list_node);
    easy_list_del(&c->group_list_node);
    if (c->type == EASY_TYPE_SERVER) {
        easy_atomic32_add_return(&c->ioth->rx_doing_request_count, -c->doing_request_count);
        easy_atomic32_dec(&c->ioth->rx_conn_count);
    } else {
        easy_atomic32_add_return(&c->ioth->tx_doing_request_count, -c->doing_request_count);
        easy_atomic32_dec(&c->ioth->tx_conn_count);
    }

    if (c->client) {
        c->client->c = NULL;
    }

    if (eio->stoped) {
        c->pool->ref = 0;
    }

    // SSL
    easy_ssl_connection_destroy(c);
    if (c->handler && c->handler->on_close) {
        (c->handler->on_close)(c);
    }

#ifdef EASY_DEBUG_MAGIC
    c->magic++;
#endif
#if 0
    if (c->ratelimit_enabled) {
        rl_class = &(c->rx_ratelimitor->rl_classes[c->location_class]);
        if (c->is_rl_active) {
            easy_atomic32_dec(&(rl_class->rl_groups[c->location_group].active_conn_count));
        }
        easy_atomic32_dec(&(rl_class->rl_groups[c->location_group].conn_count));
    }
#endif

    easy_pool_destroy(c->pool);
}

/**
 * connect参数设置
 */
easy_session_t *easy_connection_connect_init(easy_session_t *s,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags,
        char *servername)
{
    easy_pool_t             *pool = NULL;

    if (!s) {
        s = easy_session_create(0);
        pool = s->pool;
    }

    memset(s, 0, sizeof(easy_session_t));
    s->pool = pool;
    s->status = EASY_CONNECT_ADDR;
    s->thread_ptr = (void *)handler;
    s->timeout = conn_timeout;
    s->r.args = args;
    s->packet_id = flags;

    if (servername) {
        s->r.user_data = servername;
    }

    return s;
}

/**
 * 异步连接
 */
int easy_connection_connect(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags)
{
    easy_session_t *s = easy_connection_connect_init(NULL, handler, conn_timeout, args, flags, NULL);
    return easy_connection_connect_ex(eio, addr, s);
}

int easy_connection_connect_ex(easy_io_t *eio, easy_addr_t addr, easy_session_t *s)
{
    int ret;

    if (addr.family == 0 || s == NULL) {
        return EASY_ERROR;
    }

    ret = easy_client_dispatch(eio, addr, s);
    if (ret != EASY_OK) {
        easy_session_destroy(s);
    }

    return ret;
}

/**
 * 同步连接
 */
easy_connection_t *easy_connection_connect_thread(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags)
{
    easy_session_t s, *ps;

    if (addr.family == 0) {
        return NULL;
    }

    ps = easy_connection_connect_init(&s, handler, conn_timeout, args, flags, NULL);
    return easy_connection_connect_thread_ex(addr, ps);
}

easy_connection_t *easy_connection_connect_thread_ex(easy_addr_t addr, easy_session_t *s)
{
    if (addr.family == 0 || s == NULL) {
        return NULL;
    }

    s->addr = addr;
    return easy_connection_do_client(s);
}

/**
 * 断开连接
 */
int easy_connection_disconnect(easy_io_t *eio, easy_addr_t addr)
{
    int ret;
    easy_session_t *s;

    if (addr.family == 0) {
        return EASY_ERROR;
    }

    s = easy_session_create(0);
    s->status = EASY_DISCONNECT_ADDR;

    if ((ret = easy_client_dispatch(eio, addr, s)) != EASY_OK) {
        easy_session_destroy(s);
    }

    return ret;
}

int easy_connection_disconnect_thread(easy_io_t *eio, easy_addr_t addr)
{
    easy_session_t s;

    if (addr.family == 0) {
        return EASY_ERROR;
    }

    memset(&s, 0, sizeof(easy_session_t));
    s.status = EASY_DISCONNECT_ADDR;
    s.addr = addr;
    easy_connection_do_client(&s);
    return EASY_OK;
}

int easy_connection_session_build(easy_session_t *s)
{
    double                  t;
    easy_connection_t       *c = s->c;

    if (c->type != EASY_TYPE_CLIENT) {
        if (s->type != EASY_TYPE_KEEPALIVE_SESSION) {
            easy_info_log("Only keepalive session can be built on server side, session type(%d), conn(%s).",
                    s->type, easy_connection_str(c));
            return EASY_ERROR;
        }
    }

    if (!s->cleanup) {
        s->cleanup = c->handler->cleanup;
    }

    s->now = ev_now(c->loop);
    if (s->type == EASY_TYPE_KEEPALIVE_SESSION) {
        /*
         * To be simple, encoding is done in new_keepalive_packet callback.
         * We just need to set on_write_success of easy session here.
         */
        s->on_write_success = easy_session_on_write_success;
    } else if ((s->unneed_response && s->type == EASY_TYPE_SESSION) ||
            (s->unneed_response && s->type == EASY_TYPE_RL_SESSION)) {
        // encode
        (c->handler->encode)(&s->r, s->r.opacket);
        s->on_write_success = easy_session_on_write_success;
        if (unlikely(s->enable_trace)) {
            easy_info_log("unneed response, session=%p", s);
        }
        if (s->type == EASY_TYPE_RL_SESSION) {
            if (c->ratelimit_enabled == 0) {
                c->ratelimit_enabled = 1;
                easy_info_log("Ratelimit enabled on client side, session(%p), conn(%s).",
                        s, easy_connection_str(c));
            }
        }
    } else {
        // 得到packet_id
        s->packet_id = easy_connection_get_packet_id(c, s->r.opacket, 0);
        s->r.packet_id = s->packet_id;
        // encode
        (c->handler->encode)(&s->r, s->r.opacket);

        s->timeout_watcher.data = s;
        easy_hash_add(c->send_queue, s->packet_id, &s->send_queue_hash);
        easy_list_add_tail(&s->send_queue_list, &c->client_session_list);
        easy_atomic_inc(&c->pool->ref);

#ifdef EASY_DEBUG_DOING
        EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.",
                c->doing_request_count, easy_connection_str(c), &s->r, s->r.uuid);
#endif

        c->doing_request_count++;
        c->con_summary->doing_request_count++;

        if (s->type == EASY_TYPE_RL_SESSION) {
            if (c->ratelimit_enabled == 0) {
                c->ratelimit_enabled = 1;
                easy_info_log("Ratelimit enabled on client side, session(%p), conn(%s).",
                        s, easy_connection_str(c));
            }
        }

        // 加入c->session_list
        t = (s->timeout ? s->timeout : EASY_CLIENT_DEFAULT_TIMEOUT) / 1000.0;
        ev_timer_init(&s->timeout_watcher, easy_connection_on_timeout_session, t, 0.0);
        ev_timer_start(c->loop, &s->timeout_watcher);
    }

    return EASY_OK;
}

/**
 * 发送到c上, 只允许本io线程调用
 */
int easy_connection_send_session(easy_connection_t *c, easy_session_t *s)
{
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    if (ioth == NULL || ioth->iot == 0 || ioth->eio->stoped) {
        if (ioth == NULL) {
            easy_info_log("ioth is NULL.");
        } else {
            easy_info_log("ioth is wrong, ioth->iot(%d).", ioth->iot);
        }
        return EASY_ERROR;
    }

    if (s->type != EASY_TYPE_KEEPALIVE_SESSION) {
        if (unlikely(ioth->eio->checkdrc == 0 &&
                (ioth->tx_doing_request_count >= EASY_IOTH_DOING_REQ_CNT) && s->status)) {
            easy_error_log("%p, ioth->tx_doing_request_count: %d, EASY_IOTH_DOING_REQ_CNT: %d\n",
                    ioth, ioth->tx_doing_request_count, EASY_IOTH_DOING_REQ_CNT);
            return EASY_ERROR;
        }
    }

    s->c = c;
    if (s->callback == NULL) {
        s->callback = c->handler->process;
    }

    /*
     * Currently, except keepalive, no other modules uses easy_connection_send_session.
     */
    if (s->type != EASY_TYPE_KEEPALIVE_SESSION) {
        easy_atomic32_inc(&ioth->tx_doing_request_count);
    }

    if (easy_connection_session_build(s) != EASY_OK) {
        return EASY_ERROR;
    }

    return easy_connection_sendsocket(c);
}

/**
 * 把数据发送到c上, 只允许本io线程调用
 */
int easy_connection_send_session_data(easy_connection_t *c, easy_session_t *s)
{
    s->c = c;

    // encode
    (c->handler->encode)(&s->r, s->r.opacket);

    return easy_connection_sendsocket(c);
}

/**
 * accept 事件处理
 */
void easy_connection_on_accept(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_listen_simple_t    *listen;
    int                     cnt;

    listen = (easy_listen_simple_t *) w->data;
    if (listen->accept_count > 0) {
        cnt = listen->accept_count;
    } else {
        cnt = (listen->reuseport ? 32 : 5);
    }

    do {
        if (easy_connection_accept_one(loop, w) < 0) {
            break;
        }

        cnt--;
    } while (cnt > 0);
}

static void easy_connection_set_keepalive(easy_connection_t* c)
{
    easy_io_t* eio = c->ioth->eio;
    int fd = c->fd;

    if (eio->tcp_keepalive == 1) {
        if (easy_socket_set_opt(fd, SO_KEEPALIVE, 1)) {
        } else {
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPIDLE, eio->tcp_keepidle));
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPINTVL, eio->tcp_keepintvl));
            easy_ignore(easy_socket_set_tcpopt(fd, TCP_KEEPCNT, eio->tcp_keepcnt));
        }
    }
}

static void easy_connection_rearm_failure_detection_timer(easy_connection_t* c)
{
    static __thread int64_t rearm_count = 0;
    easy_io_t* eio = c->ioth->eio;
    int64_t ack_timeout = eio->ack_timeout ? eio->ack_timeout : 60000;
    double repeat;

    if (eio->ack_timeout == c->armed_ack_timeout) {
        return;
    }

    if (ack_timeout > 0 || c->handler->on_idle) {
        /*
         * By default, idle time is 60,000ms.
         */
        repeat = easy_max(1.0, c->idle_time / 3000.0);
        if (repeat > (ack_timeout / 3000.0)) {
            /*
             * Timer repeats for 3 times in each ack_timeout interval.
             */
            repeat = ack_timeout / 3000.0;
        }
        ev_timer_stop(c->loop, &c->timeout_watcher);
        ev_timer_set(&c->timeout_watcher, (rearm_count++ % 3000) / 1000.0, repeat);
        ev_timer_start(c->loop, &c->timeout_watcher);
        easy_info_log("Rearm timer on connection, conn(%s), detect_interval(%lf), ack_timeout(%ld), "
                "rearm_count(%ld).", easy_connection_str(c), repeat, ack_timeout, rearm_count);
    }
    easy_connection_set_keepalive(c);
    c->armed_ack_timeout = eio->ack_timeout;
}

static int easy_connection_accept_one(struct ev_loop *loop, ev_io *w)
{
    int                     fd;
    easy_listen_simple_t    *listen;
    struct sockaddr_storage addr;
    socklen_t               addr_len;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    char                    dest_addr[32];
    easy_addr_t             eaddr;

    listen = (easy_listen_simple_t *) w->data;
    addr_len = sizeof(addr);

#if HAVE_ACCEPT4
    if (ioth->eio->use_accept4) {
        if ((fd = accept4(w->fd, (struct sockaddr *)&addr, &addr_len, SOCK_NONBLOCK)) < 0) {
            if (errno == ENOSYS) {
                ioth->eio->use_accept4 = 0;
            }
            if ((fd = accept(w->fd, (struct sockaddr *)&addr, &addr_len)) < 0) {
                easy_error_log("Failed to do accept(HAVE_ACCEPT4 defined).\n");
                return EASY_ERROR;
            }
        }
    } else
#endif
    {
        if ((fd = accept(w->fd, (struct sockaddr *)&addr, &addr_len)) < 0) {
            easy_error_log("Failed to do accept.\n");
            return EASY_ERROR;
        }
    }

    easy_inet_atoe(&addr, &eaddr);
    easy_info_log("Accepted connect request from client(%s).",
        easy_inet_addr_to_str(&eaddr, dest_addr, 32)); // easy_debug_log ??
    return easy_connection_do_accept_one(loop, w, listen, fd, eaddr);
}

static int easy_connection_do_accept_one(struct ev_loop *loop,
        ev_io* w, easy_listen_simple_t* listen, int fd, easy_addr_t addr)
{
    static easy_atomic_t    easy_accept_sequence = 0;
    easy_connection_t       *c;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    easy_socket_non_blocking(fd);
    // 为新连接创建一个easy_connection_t对象
    if ((c = easy_connection_new()) == NULL) {
        easy_error_log("Failed to do easy_connection_new. closing socket fd(%d).", fd);
        close(fd);
        return EASY_ERROR;
    }

    if (ioth->eio->tcp_nodelay) {
        easy_ignore(easy_socket_set_tcpopt(fd, TCP_NODELAY, 1));
    }

    // 初始化
    c->fd = fd;
    c->type = EASY_TYPE_SERVER;
    c->handler = listen->handler;
    easy_debug_log("easy_connection_do_accept_one, get_packet_id(%p), ioth->idx(%d).",
            c->handler->get_packet_id, ioth->idx);
    c->evdata = w->data;
    c->addr = addr;
    c->seq = easy_atomic_add_return(&easy_accept_sequence, 1);

    // 事件初始化
    ev_io_init(&c->read_watcher, easy_connection_on_readable, fd, EV_READ);
    ev_io_init(&c->write_watcher, easy_connection_on_writable, fd, EV_WRITE);
    ev_init(&c->timeout_watcher, easy_connection_on_timeout_conn);
    c->read_watcher.data = c;
    c->write_watcher.data = c;
    c->timeout_watcher.data = c;
    c->ioth = ioth;
    c->loop = loop;
    c->start_time = ev_now(ioth->loop);
    c->ssl_sm_ = (c->handler->is_ssl ? SSM_USE_SSL : SSM_NONE);

    /*
     * Initialize keepalive-related fields.
     */
    c->local_magic_ver = CURENT_MAGIC_VERSION;
    c->peer_magic_ver  = LEGACY_MAGIC_VERSION;
    c->magic_ver       = LEGACY_MAGIC_VERSION;
    c->last_tx_tstamp  = ev_now(c->loop);
    c->last_rx_tstamp  = ev_now(c->loop);

    //server locate
    c->con_summary = easy_summary_locate_node(c->fd, c->ioth->eio->eio_summary, listen->hidden_sum);

    // on connect
    if (c->handler->on_connect && (c->handler->on_connect)(c) == EASY_ERROR) {
        easy_error_log("Failed to do on_connect. closing socket fd(%d).", fd);
        easy_pool_destroy(c->pool);
        close(fd);
        return EASY_ERROR;
    }

    /*
     * For rpc connection with is_ssl_opt == false, ssl_connection is created here.
     *
     * For mysql connection with is_ssl_opt == ture, ssl_connection will be created
     * after 1st request is handled.
     */
    if (c->handler->is_ssl
        && !c->handler->is_ssl_opt
        && SSM_USE_SSL == c->ssl_sm_
        && NULL == c->sc
        && NULL != ioth->eio->ssl) {

        easy_info_log("easy_connection_do_accept_one %s\n", easy_connection_str(c));

        int need_destory = 0;
        easy_debug_log("accept from %s, with ssl\n", easy_connection_str(c));
        if (EASY_OK != easy_spinrwlock_rdlock(&(ioth->eio->ssl_rwlock_))) {
            easy_error_log("easy_spinrwlock_rdlock failed %s, ref_cnt %ld, wait_write %ld\n",
                    easy_connection_str(c), ioth->eio->ssl_rwlock_.ref_cnt,
                    ioth->eio->ssl_rwlock_.wait_write);
            need_destory = 1;
        } else {
            if (EASY_OK != easy_ssl_connection_create(ioth->eio->ssl->server_ctx, c)) {
                easy_error_log("easy_ssl_connection_create failed %s, cb: %p\n",
                        easy_connection_str(c), ev_cb(&c->read_watcher));
                need_destory = 1;
            }
            c->ssl_sm_  = SSM_NONE;
            // set read callback
            ev_set_cb(&c->read_watcher, easy_ssl_connection_handshake);

            if (EASY_OK != easy_spinrwlock_unlock(&(ioth->eio->ssl_rwlock_))) {
                easy_error_log("easy_spinrwlock_unlock failed %s, ref_cnt %ld, wait_write %ld\n",
                        easy_connection_str(c), ioth->eio->ssl_rwlock_.ref_cnt,
                        ioth->eio->ssl_rwlock_.wait_write);
                need_destory = 1;
            }
        }

        if (need_destory) {
            easy_info_log("Socket closed, fd(%d), conn(%s).", fd, easy_connection_str(c));
            easy_pool_destroy(c->pool);
            close(fd);
            return EASY_ERROR;
        }
    }

    // start idle
    easy_connection_rearm_failure_detection_timer(c);

    // 让出来给其他的线程
    //if (listen->is_simple == 0 && listen->reuseport == 0 && ioth->eio->listen_all == 0)
    //    easy_switch_listen(listen);

    // start read
    easy_list_add_tail(&c->conn_list_node, &c->ioth->connected_list);
    easy_atomic32_inc(&c->ioth->rx_conn_count);
    c->event_status = EASY_EVENT_READ;

    if (ioth->eio->tcp_defer_accept) {
        (ev_cb(&c->read_watcher))(loop, &c->read_watcher, 0);
    } else {
        easy_connection_evio_start(c);
    }

    easy_info_log("Connection established on server side, conn(%s), cb(%p).\n",
            easy_connection_str(c), ev_cb(&c->read_watcher));
    return EASY_OK;
}

static void easy_switch_listen(void *data)
{
    easy_listen_t           *listen = (easy_listen_t *)data;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;

    if (ioth->eio->listen_all == 0 && listen->old_ioth == NULL && listen->curr_ioth == ioth) {
        listen->old = listen->cur;
        listen->curr_ioth = NULL;
        listen->old_ioth = ioth;
        ioth->listen_watcher.repeat = 0.5;
        ev_timer_again(ioth->loop, &ioth->listen_watcher);
        easy_unlock(&listen->listen_lock);
    }
}

static void easy_connection_evio_start(easy_connection_t *c)
{
    if ((c->event_status & EASY_EVENT_READ)) {
        ev_io_start(c->loop, &c->read_watcher);
    }

    if ((c->event_status & EASY_EVENT_WRITE)) {
        ev_io_start(c->loop, &c->write_watcher);
    }

    if ((c->event_status & EASY_EVENT_TIMEOUT)) {
        ev_timer_start(c->loop, &c->timeout_watcher);
    }

    c->event_status = 0;
}

/**
 * 为了均衡，切换到其他线程
 */
static int easy_connection_redispatch_thread(easy_connection_t *c)
{
    easy_io_thread_t        *ioth;

    // 处理了８次以下, 或者有读写没完, 不能切换
    if (!c->need_redispatch || easy_list_empty(&c->message_list) == 0 || easy_list_empty(&c->output) == 0) {
        return EASY_AGAIN;
    }

    // 选择一新的ioth
    ioth = (easy_io_thread_t *)easy_thread_pool_hash(EASY_IOTH_SELF->eio->io_thread_pool, c->seq);
    return easy_connection_dispatch_to_thread(c, ioth);
}

/**
 * 把c直接给指定ioth
 */
int easy_connection_dispatch_to_thread(easy_connection_t *c, easy_io_thread_t *ioth)
{
    int                     status = EASY_EVENT_READ;
    int                     doing = c->doing_request_count;

    c->need_redispatch = 0;
    if (ioth == c->ioth) {
        return EASY_AGAIN;
    }

    easy_list_del(&c->conn_list_node);
    if (ev_is_active(&c->write_watcher)) {
        status |= EASY_EVENT_WRITE;
    }
    if (ev_is_active(&c->timeout_watcher)) {
        status |= EASY_EVENT_TIMEOUT;
    }

    if (status & EASY_EVENT_WRITE) {
        ev_io_stop(c->loop, &c->read_watcher);
    } else {
        ev_io_stop_ctrl(c->loop, &c->read_watcher);
    }

    ev_io_stop_ctrl(c->loop, &c->write_watcher);
    ev_timer_stop(c->loop, &c->timeout_watcher);

    // request_list
    if (c->type == EASY_TYPE_SERVER) {
        if (c->rx_request_queue) {
            easy_list_t *request = (easy_list_t *)c->rx_request_queue;
            easy_list_join(request, &c->server_session_list);
            c->rx_request_queue = NULL;
        }
        easy_atomic32_dec(&c->ioth->rx_conn_count);
        easy_atomic32_inc(&ioth->rx_conn_count);

        easy_atomic32_add_return(&c->ioth->rx_doing_request_count, -doing);
        easy_atomic32_add_return(&ioth->rx_doing_request_count, doing);
    } else {
        easy_atomic32_dec(&c->ioth->tx_conn_count);
        easy_atomic32_inc(&ioth->tx_conn_count);

        easy_atomic32_add_return(&c->ioth->tx_doing_request_count, -doing);
        easy_atomic32_add_return(&ioth->tx_doing_request_count, doing);
    }

    easy_info_log("%s redispatch %p to %p, cnt:%d\n", easy_connection_str(c), c->ioth, ioth, doing);
    c->event_status |= status;
    c->ioth = ioth;
    c->loop = ioth->loop;

    // wakeup
    easy_spin_lock(&ioth->thread_lock);
    easy_list_add_tail(&c->conn_list_node, &ioth->conn_list);
    easy_spin_unlock(&ioth->thread_lock);
    ev_async_send(ioth->loop, &ioth->thread_watcher);

    return EASY_ASYNC;
}

/**
 * 切换listen
 */
void easy_connection_on_listen(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_listen_t               *l;
    easy_io_thread_t            *ioth;
    ioth = (easy_io_thread_t *) w->data;
    easy_io_t                   *eio = ioth->eio;

    if (eio->listenadd) {
        easy_spin_lock(&eio->lock);
        eio->listenadd->next = eio->listen;
        eio->listen = eio->listenadd;
        eio->listenadd = NULL;
        easy_spin_unlock(&eio->lock);
    }

    // 对每一个listen
    for (l = eio->listen; l; l = l->next) {
        if (l->reuseport) {
            continue;
        }

        // trylock一下
        if (easy_trylock(&l->listen_lock)) {
            // 是自己
            if (l->old_ioth == ioth) {
                l->old_ioth = NULL;
                l->curr_ioth = ioth;
            } else {
                l->cur = ((l->cur + 1) & 1);
                ev_io_start(ioth->loop, &l->read_watcher[l->cur]);
                l->curr_ioth = ioth;
                ioth->listen_watcher.repeat = 60.;
                ev_timer_again(ioth->loop, &ioth->listen_watcher);
            }
        } else if (l->curr_ioth && l->old_ioth == ioth) {
            ev_io_stop(ioth->loop, &l->read_watcher[l->old]);
            l->old_ioth = NULL;
        }
    }
}

/**
 * conn事件处理
 */
void easy_connection_on_wakeup(struct ev_loop *loop, ev_async *w, int revents)
{
    EASY_TIME_GUARD();
    easy_connection_t       *c, *c2;
    easy_io_thread_t        *ioth;
    easy_list_t             conn_list;
    easy_list_t             session_list;
    easy_list_t             request_list;

    ioth = (easy_io_thread_t *)w->data;

    // 取回list
    easy_spin_lock(&ioth->thread_lock);
    easy_list_movelist(&ioth->conn_list, &conn_list);
    easy_list_movelist(&ioth->session_list, &session_list);
    easy_list_movelist(&ioth->request_list, &request_list);
    easy_spin_unlock(&ioth->thread_lock);

    // foreach
    easy_list_for_each_entry_safe(c, c2, &conn_list, conn_list_node) {
        c->loop = loop;
        c->start_time = ev_now(ioth->loop);
        easy_connection_evio_start(c);

        if (c->handler->on_redispatch) {
            c->handler->on_redispatch(c);
        }
    }
    easy_list_join(&conn_list, &ioth->connected_list);

    easy_connection_send_session_list(&session_list);
    easy_connection_send_response(&request_list);
    easy_connection_send_rlmtr(ioth);
}


static int easy_connection_update_rlmtr(easy_connection_t *conn, ssize_t size)
{
    int ret = EASY_OK;
    easy_region_ratelimitor_t *region_rlmtr;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;

    if (ioth->eio->easy_rlmtr->ratelimit_enabled && (!conn->ratelimit_enabled)) {
        ret = easy_eio_get_region_rlmtr(&conn->addr, &region_rlmtr);
        if (ret == EASY_OK) {
            easy_atomic_add(&region_rlmtr->tx_bytes, size);
        }
        easy_debug_log("easy_connection_update_rlmtr, update rate limit TX bytes, conn(%s), ret(%d).\n",
                easy_connection_str(conn), ret);
    }

    return ret;
}


static int easy_connection_dispatch_response(easy_region_ratelimitor_t *region_rlmtr, easy_list_t *list, int *is_speeding)
{
    int ret = EASY_OK;
    easy_io_thread_t *ioth;
    easy_request_t *cur, *next;
    easy_connection_t *conn;
    easy_message_t *msg;
    uint64_t tx_bytes_old, tx_bytes_per_period;
    easy_ratelimitor_t *easy_rlmtr = NULL;

    easy_rlmtr = (EASY_IOTH_SELF)->eio->easy_rlmtr;
    tx_bytes_per_period = region_rlmtr->max_bw * easy_rlmtr->stat_period;
    tx_bytes_per_period >>= 1;
    tx_bytes_old = region_rlmtr->tx_bytes;

    easy_debug_log("easy_connection_dispatch_response.");
    easy_list_for_each_entry_safe(cur, next, list, request_list_node) {
        msg = (easy_message_t *)cur->ms;
        conn = msg->c;

        *is_speeding = easy_eio_is_region_speeding(region_rlmtr, 1);
        if (*is_speeding) {
            break;
        } else {
            ioth = conn->ioth;
            cur->redispatched = 1;
            easy_list_del(&cur->request_list_node);
            easy_debug_log(" easy_connection_dispatch_response, cur(%p).", cur);
            easy_spin_lock(&ioth->thread_lock);
            easy_list_add_tail(&cur->request_list_node, &ioth->request_list);
            easy_spin_unlock(&ioth->thread_lock);

            region_rlmtr->dispatched_req_count++;
            easy_atomic_add(&region_rlmtr->tx_bytes, cur->opacket_size);
            easy_eio_record_region_speed(region_rlmtr);
            ev_async_send(ioth->loop, &ioth->thread_watcher);
        }

        if ((region_rlmtr->tx_bytes - tx_bytes_old) > tx_bytes_per_period) {
            ret = EASY_AGAIN;
            break;
        }
    }
    easy_debug_log("easy_connection_dispatch_response done.");
    return ret;
}


static int easy_connection_dispatch_session(easy_region_ratelimitor_t *region_rlmtr, easy_list_t *list, int *is_speeding)
{
    int ret = EASY_OK;
    easy_io_thread_t *ioth;
    easy_session_t *cur, *next;
    uint64_t tx_bytes_old, tx_bytes_per_period;
    easy_ratelimitor_t *easy_rlmtr = NULL;

    ioth = EASY_IOTH_SELF;
    easy_rlmtr = ioth->eio->easy_rlmtr;
    tx_bytes_per_period = region_rlmtr->max_bw * easy_rlmtr->stat_period;
    tx_bytes_per_period >>= 1;
    tx_bytes_old = region_rlmtr->tx_bytes;

    easy_debug_log("easy_connection_dispatch_session, ioth(%p), ioth->idx(%d).", ioth, ioth->idx);
    easy_list_for_each_entry_safe(cur, next, list, session_list_node) {
        easy_debug_log("easy_connection_dispatch_session, ioth(%d), ret(%d), cur(%p).", ioth->idx, ret, cur);
        *is_speeding = easy_eio_is_region_speeding(region_rlmtr, 1);
        if (*is_speeding) {
            easy_debug_log("easy_connection_dispatch_session, region_rlmtr is speeding, region_rlmtr(%p).", region_rlmtr);
            break;
        } else {
            cur->redispatched = 1;
            easy_list_del(&cur->session_list_node);
            easy_debug_log("redispatch session to IO thread, sess(%p).", cur);
            easy_spin_lock(&ioth->thread_lock);
            easy_list_add_tail(&cur->session_list_node, &ioth->session_list);
            easy_spin_unlock(&ioth->thread_lock);

            region_rlmtr->dispatched_req_count++;
            easy_atomic_add(&region_rlmtr->tx_bytes, (cur->opacket_size + EASY_TOTAL_HEADER_SIZE));
            easy_eio_record_region_speed(region_rlmtr);
            }

        if ((region_rlmtr->tx_bytes - tx_bytes_old) > tx_bytes_per_period) {
            ret = EASY_AGAIN;
            break;
        }
    }

    easy_debug_log("easy_connection_dispatch_session done, ioth(%d).", ioth->idx);
    return ret;
}


static int easy_connection_send_rlmtr(easy_io_thread_t *ioth)
{
    int ret = EASY_OK;
    int is_speeding = 0;
    int do_wakeup = 0;
    uint64_t dispatched = 0;
    easy_ratelimitor_t* easy_rlmtr;
    easy_region_ratelimitor_t *cur, *next;
    easy_rlmtr = ioth->eio->easy_rlmtr;

    if (ioth != easy_rlmtr->ratelimit_thread) {
        return ret;
    }
    easy_debug_log("easy_connection_send_rlmtr, ioth(%p), ioth->idx(%d), ratelimit_thread(%p, %d).",
            ioth, ioth->idx, easy_rlmtr->ratelimit_thread, easy_rlmtr->ratelimit_thread->idx);

    easy_list_for_each_entry_safe(cur, next, &easy_rlmtr->ready_queue, ready_queue_node) {
        if (EASY_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            easy_info_log("easy_connection_send_rlmtr, ioth(%d), cur(%p), cur->tx_trigger_armed(%d), recv/disp(%ld, %ld).",
                    ioth->idx, cur, cur->tx_trigger_armed, cur->received_req_count, cur->dispatched_req_count);
        }

        easy_eio_record_region_speed(cur);
        if (cur->tx_trigger_armed) {
            continue;
        }

        easy_debug_log("easy_connection_dispatch_response fg, ioth(%d), cur(%p), recv/disp(%ld, %ld).",
                ioth->idx, cur, cur->received_req_count, cur->dispatched_req_count);
        ret = easy_connection_dispatch_response(cur, &cur->fg_rsp_queue, &is_speeding);
        if ((ret != EASY_OK) || is_speeding)  {
            continue;
        }

        easy_debug_log("easy_connection_dispatch_session fg, ioth(%d), cur(%p), recv/disp(%ld, %ld).",
                ioth->idx, cur, cur->received_req_count, cur->dispatched_req_count);
        dispatched = cur->dispatched_req_count;
        ret = easy_connection_dispatch_session(cur, &cur->fg_req_queue, &is_speeding);
        if (dispatched != cur->dispatched_req_count) {
            do_wakeup = 1;
        }
        if ((ret != EASY_OK) || is_speeding)  {
            continue;
        }

        easy_debug_log("easy_connection_dispatch_response bg, ioth(%d), cur(%p), recv/disp(%ld, %ld).",
                ioth->idx, cur, cur->received_req_count, cur->dispatched_req_count);
        ret = easy_connection_dispatch_response(cur, &cur->bg_rsp_queue, &is_speeding);
        if ((ret != EASY_OK) || is_speeding)  {
            continue;
        }

        easy_debug_log("easy_connection_dispatch_session bg, ioth(%d), cur(%p), recv/disp(%ld, %ld).",
                ioth->idx, cur, cur->received_req_count, cur->dispatched_req_count);
        dispatched = cur->dispatched_req_count;
        ret = easy_connection_dispatch_session(cur, &cur->bg_req_queue, &is_speeding);
        if (dispatched != cur->dispatched_req_count) {
            do_wakeup = 1;
        }
        if ((ret != EASY_OK) || is_speeding)  {
            continue;
        }

        easy_debug_log("easy_connection_send_rlmtr, remove ratelimitor from ready_queue, ioth(%d), cur(%p), recv/disp(%ld, %ld).",
                ioth->idx, cur, cur->received_req_count, cur->dispatched_req_count);
        easy_list_del(&cur->ready_queue_node);
    }

    if (do_wakeup) {
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    easy_debug_log("easy_connection_send_rlmtr done, ioth(%p), ioth->idx(%d).\n", ioth, ioth->idx);
    return 0;
}


static int easy_connection_forward_session_to_rlmtr(easy_session_t *sess)
{
    int ret;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;
    easy_ratelimitor_t* easy_rlmtr = NULL;
    easy_region_ratelimitor_t *region_rlmtr;

    easy_rlmtr = ioth->eio->easy_rlmtr;
    ret = easy_eio_get_region_rlmtr(&sess->addr, &region_rlmtr);
    if (ret == EASY_OK) {
        if (sess->is_bg_flow) {
            easy_list_add_tail(&sess->session_list_node, &region_rlmtr->bg_req_queue);
        } else {
            easy_debug_log("add session to fg_req_queue, sess(%p), region_rlmtr(%p), ready_queue_empty(%d).\n",
                    sess, region_rlmtr, easy_list_empty(&region_rlmtr->ready_queue_node));
            easy_list_add_tail(&sess->session_list_node, &region_rlmtr->fg_req_queue);
        }

        region_rlmtr->received_req_count++;
        if (easy_list_empty(&region_rlmtr->ready_queue_node)) {
            easy_debug_log("add region_rlmtr to ready_queue, region_rlmtr(%p).\n", region_rlmtr);
            easy_list_add_tail(&region_rlmtr->ready_queue_node, &easy_rlmtr->ready_queue);
        }
    }

    return ret;
}


static int easy_connection_forward_request_to_rlmtr(easy_request_t *req)
{
    int ret;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;
    easy_connection_t         *conn;
    easy_ratelimitor_t        *easy_rlmtr;
    easy_region_ratelimitor_t *region_rlmtr;

    easy_rlmtr =ioth->eio->easy_rlmtr;
    conn = req->ms->c;
    ret = easy_eio_get_region_rlmtr(&conn->addr, &region_rlmtr);

    if (ret == EASY_OK) {
        easy_debug_log("easy_connection_forward_request_to_rlmtr, region_rlmtr(%p), req(%p).", region_rlmtr, req);
        if (req->is_bg_flow) {
            easy_list_add_tail(&req->request_list_node, &region_rlmtr->bg_rsp_queue);
        } else {
            easy_list_add_tail(&req->request_list_node, &region_rlmtr->fg_rsp_queue);
        }

        region_rlmtr->received_req_count++;
        if (easy_list_empty(&region_rlmtr->ready_queue_node)) {
            easy_debug_log("add region_rlmtr to ready_queue, region_rlmtr(%p).", region_rlmtr);
            easy_list_add_tail(&region_rlmtr->ready_queue_node, &easy_rlmtr->ready_queue);
        }
    }

    return ret;
}


void easy_connection_on_send_rlmtr(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_region_ratelimitor_t *region_rlmtr;
    easy_io_thread_t *ioth = EASY_IOTH_SELF;

    region_rlmtr = (easy_region_ratelimitor_t *)w->data;
    if (EASY_REACH_TIME_INTERVAL(1 * 1000 * 1000 * 60)) {
        easy_info_log("easy_connection_on_send_rlmtr, region_rlmtr(%p), ioth->idx(%d).",
                region_rlmtr, ioth->idx);
    }
    /*
     * Timer will expire 50 years later.
     */
    region_rlmtr->tx_trigger_armed = 0;
    region_rlmtr->tx_trigger.repeat = 50 * 365 * 24 * 3600 * 1.;
    ev_timer_again (ioth->loop, &region_rlmtr->tx_trigger);
    ev_async_send(ioth->loop, &ioth->thread_watcher);
    return;
}

void __mod_stat_cleanup(void *data)
{
  **(mod_stat_t***)data = NULL;
}

/**
 * read事件处理
 */
void easy_connection_on_readable(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    easy_message_t          *m;
    ssize_t                 n;
    int                     pending;

    c = (easy_connection_t *)w->data;
    mod_stat_t **mod_stat __attribute__((cleanup(__mod_stat_cleanup))) = &easy_cur_mod_stat;
    *mod_stat = c->pool->mod_stat;

    EASY_TIME_GUARD();
    assert(c->fd == w->fd);

    // 防止请求过多
    if (unlikely(c->type == EASY_TYPE_SERVER &&
                 (c->doing_request_count > EASY_CONN_DOING_REQ_CNT || c->ioth->rx_doing_request_count > EASY_IOTH_DOING_REQ_CNT))) {
        if (c->ioth->eio->checkdrc == 0) {
            CONN_DESTROY_LOG("too many doing request");
            goto error_exit;
        }
    }

    // 最后的请求, 如果数据没完, 需要继续读
    m = easy_list_get_last(&c->message_list, easy_message_t, message_list_node);

    // 第一次读或者上次读完整了, 重新建一个easy_message_t
    if (m == NULL || m->status != EASY_MESG_READ_AGAIN) {
        if ((m = easy_message_create(c)) == NULL) {
            CONN_DESTROY_LOG("easy_message_create failure");
            goto error_exit;
        }
    }

    const int MAX_SSL_REQ_PKT_SIZE = 36;
    int use_max_ssl_req_pkt_size = 0;
    do {
        /*
         * Check buffer message buffer size.
         */
        pending = 0;
        if (SSM_BEFORE_FIRST_PKT == c->ssl_sm_
            && c->handler->is_ssl
            && c->handler->is_ssl_opt
            && c->type == EASY_TYPE_SERVER) {
            m->next_read_len = MAX_SSL_REQ_PKT_SIZE;
            use_max_ssl_req_pkt_size = 1;
        }
        if (easy_buf_check_read_space(m->pool, m->input, m->next_read_len) != EASY_OK) {
            easy_error_log("Failed to do easy_buf_check_read_space, m=%p, len=%d\n",
                    m, m->next_read_len);
            goto error_exit;
        }

        /*
         * Read data from socket.
         */
        if ((n = (c->read)(c, m->input->last, m->next_read_len, &pending)) <= 0) {
            if (n == EASY_AGAIN) {
                easy_connection_evio_start(c);
                return;
            }

            c->conn_has_error = 0;
            if (n == 0) {
                easy_info_log("Read eof from connection. Socket may have been shut down, conn(%s).",
                        easy_connection_str(c));
                if (c->type == EASY_TYPE_CLIENT) {
                    if (EASY_ERROR == easy_connection_do_response(m)) {
                        easy_error_log("Failed to do response on client, conn(%p).", c);
                    }
                }
                c->status = EASY_CONN_CLOSE_BY_PEER;
            } else {
                c->conn_has_error = 1;
                easy_info_log("Failed to read from connection(%p), n(%d).", c, n);
            }

            goto error_exit;
        }

        c->last_rx_tstamp = ev_now(loop);
        if (unlikely(easy_log_level >= EASY_LOG_DEBUG)) {
            if (easy_log_level == EASY_LOG_DEBUG) {
                easy_debug_log("%s read: %d", easy_connection_str(c), n);
            } else {
                char btmp[128];
                easy_trace_log("%s read: %d => %s",
                        easy_connection_str(c), n, easy_string_tohex(m->input->last, n, btmp, 128));
            }
        }

        if (use_max_ssl_req_pkt_size) {
            c->ssl_sm_ = SSM_AFTER_FIRST_PKT;
        }
        m->input->last += n;
        c->read_eof = (n < m->next_read_len);
        c->con_summary->in_byte += n;
        c->recv_bytes += n;
    } while (pending);

    c->last_time = ev_now(loop);
    c->reconn_fail = 0;

    if (c->read_eof == 0 && c->first_msglen == EASY_FIRST_MSGLEN) {
        c->first_msglen = EASY_IO_BUFFER_SIZE;
        m->next_read_len = c->first_msglen;
    }

    if (c->type == EASY_TYPE_CLIENT) {
        if (EASY_ERROR == easy_connection_do_response(m)) {
            easy_warn_log("Easy client failed to handle response on connection(%s).\n",
                    easy_connection_str(c));
            goto error_exit;
        }
    } else {
        if (EASY_ERROR == easy_connection_do_request(m)) {
            easy_warn_log("Easy server failed to handle request on connection(%s).\n",
                    easy_connection_str(c));
            goto error_exit;
        }

#if 0
        if (c->ratelimit_enabled) {
            easy_connection_do_ratelimit(c);
        }
#endif
    }

    return;

error_exit:
    // easy_info_log("easy_connection_on_readable error_exit %s", easy_connection_str(c));
    EASY_CONNECTION_DESTROY(c, "on_readable");
}

/**
 * 处理响应
 */
static int easy_connection_do_response(easy_message_t *m)
{
    easy_connection_t       *c;
    easy_session_t          *s;
    uint64_t                packet_id;
    int                     i, cnt, left;
    void                    *packet;
    easy_list_t             list;
    double                  now;

    c = m->c;

    // 处理buf
    cnt = 0;
    left = 0;
    easy_list_init(&list);
    now = ev_now(easy_baseth_self->loop);

    while (m->input->pos < m->input->last) {
        if ((packet = (c->handler->decode)(m)) == NULL) {
            if (m->status == EASY_MESG_SKIP) {
                m->status = EASY_OK;
                m->enable_trace = 1;
                easy_debug_log("client recieved keepalive, m(%p), conn(%s)\n",
                        m, easy_connection_str(c));
                continue;
            }

            if (m->status != EASY_ERROR) {
                // quickack
                if (EASY_IOTH_SELF->eio->no_delayack && m->next_read_len < EASY_MSS) {
                    easy_ignore(easy_socket_set_tcpopt(c->fd, TCP_QUICKACK, 1));
                }
                break;
            }

            easy_warn_log("decode error, %s\n", easy_connection_str(c));
            return EASY_ERROR;
        }

        cnt++;
        packet_id = easy_connection_get_packet_id(c, packet, 1);
        s = (easy_session_t *)easy_hash_del(c->send_queue, packet_id);
        if (s) {
            easy_list_del(&s->send_queue_list);
        }

        if (s == NULL) {
            // 需要cleanup
            if (c->handler->cleanup)
                (c->handler->cleanup)(NULL, packet);

            if (EASY_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                easy_warn_log("Not found session, packet_id(%ld), conn(%s)\n",
                        packet_id, easy_connection_str(c));
            }
            continue;
        }

        s->r.client_read_time = current_time();
        // process
        EASY_IOTH_SELF->tx_done_request_count++;
        s->r.ipacket = packet;              // in

        if (s->async) {                     // message延后释放
            m->async = s->async;
            easy_atomic_inc(&m->pool->ref);
            s->r.request_list_node.next = (easy_list_t *)m;
        }

        // stop timer
        ev_timer_stop(c->loop, &s->timeout_watcher);
        easy_list_del(&s->session_list_node);
        easy_request_client_done(&s->r);
        easy_atomic_dec(&c->pool->ref);

        s->r.client_end_time = current_time();
        if (s->is_trace_time) {
            if (c->handler->set_trace_info) {
                c->handler->set_trace_info(&(s->r), packet);
                check_easy_request_rt(s);
            } else {
                easy_error_log("set trace time NULL");
            }
        }

        if (c->handler->batch_process) {
            if (s->now) {
                s->now = now - s->now;
            }

            easy_list_add_tail(&s->session_list_node, &list);

            if (++left >= 32) {
                (c->handler->batch_process)((easy_message_t *)&list);
                left = 0;
            }
        } else if (easy_session_process(s, 0, EASY_OK) == EASY_ERROR) {
            easy_warn_log("easy_session_process error, fd=%d, s=%p\n", c->fd, s);
            return EASY_ERROR;
        }
    }

    // batch process
    if (cnt) {
        m->recycle_cnt++;
    }

    if (left > 0) {
        (c->handler->batch_process)((easy_message_t *)&list);
    }

    // close
    if (c->wait_close && c->pool->ref == 0) {
        c->wait_close = 0;
        return EASY_ERROR;
    }

    // send new packet
    if (c->handler->new_packet) {
        left = (EASY_CONN_DOING_REQ_CNT / 2) - c->doing_request_count;

        if (c->ioth->tx_doing_request_count > 0) {
            left = easy_min(left, (EASY_IOTH_DOING_REQ_CNT / 2) - c->ioth->tx_doing_request_count);
        }

        if (left > 0) {
            ev_io_start(c->loop, &c->write_watcher);
            left = easy_min(cnt, left);

            for (i = 0; i < left; i++) {
                if ((c->handler->new_packet)(c) == EASY_ERROR)
                    return EASY_ERROR;
            }
        }
    }

    if ((m = easy_connection_recycle_message(m)) == NULL) {
        easy_warn_log("easy_connection_recycle_message error, fd=%d, m=%p\n", c->fd, m);
        return EASY_ERROR;
    }

    // status, message 没读完
    if (m->input->pos < m->input->last) {
        m->status = EASY_MESG_READ_AGAIN;
    } else {
        easy_message_destroy(m, 1);
    }

    return EASY_OK;
}

/**
 * 处理请求
 */
static int easy_connection_do_request(easy_message_t *m)
{
    easy_connection_t       *c;
    void                    *packet;
    easy_request_t          *r;
    int                     cnt, ret;

    cnt = 0;
    c = m->c;

    // 处理buf, decode
    while (m->input->pos < m->input->last) {
        if ((packet = (c->handler->decode)(m)) == NULL) {
            if (m->status == EASY_MESG_SKIP) {
                /*
                 * Change m->status to EASY_MESG_SKIP in
                 * ObRpcProtocolProcessor::resolve_packet_type
                 */
                m->status = EASY_OK;
                m->enable_trace = 1;
                easy_debug_log("Server recieved keepalive, m(%p), conn(%s)\n",
                        m, easy_connection_str(c));
                continue;
            }

            if (m->status != EASY_ERROR) {
                /*
                 * Only part of message data is received here. Send ACK to TCP TX side
                 * by setting TCP_QUICKACK, so TX side can provoke next sending ASAP.
                 */
                if (EASY_IOTH_SELF->eio->no_delayack && m->next_read_len < EASY_MSS) {
                    easy_ignore(easy_socket_set_tcpopt(c->fd, TCP_QUICKACK, 1));
                }
                break;
            }

            easy_warn_log("decode error, %s m=%p, cnt=%d\n",
                    easy_connection_str(c), m, cnt);
            c->doing_request_count += cnt;
            easy_atomic32_add(&c->ioth->rx_doing_request_count, cnt);
            return EASY_ERROR;
        }

        // new request
        r = (easy_request_t *)easy_pool_calloc(m->pool, sizeof(easy_request_t));
        if (r == NULL) {
            easy_error_log("easy_pool_calloc failure, %s, m: %p\n",
                    easy_connection_str(c), m);
            c->doing_request_count += cnt;
            easy_atomic32_add(&c->ioth->rx_doing_request_count, cnt);
            return EASY_ERROR;
        }

#ifdef EASY_DEBUG_MAGIC
        r->magic = EASY_DEBUG_MAGIC_REQUEST;
#endif
        r->ms = (easy_message_session_t *)m;
        r->ipacket = packet;    //进来的数据包
        r->start_time = ev_now(c->loop);
        r->request_arrival_time = current_time();
        // r->packet_id = easy_connection_get_packet_id(c, packet, 1);
        if (c->handler->set_trace_info) {
            c->handler->set_trace_info(r, packet);
        }
         // add m->request_list
        easy_list_add_tail(&r->request_list_node, &m->request_list);
        easy_list_add_tail(&r->all_node, &m->all_list);
        cnt++;

#ifdef EASY_DEBUG_DOING
        r->uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
        EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.", c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif
    }

    if (cnt) {
        m->request_list_count += cnt;
        c->doing_request_count += cnt;
        c->con_summary->doing_request_count += cnt;
        easy_atomic32_add(&c->ioth->rx_doing_request_count, cnt);
        m->recycle_cnt++;
    }

    if ((m = easy_connection_recycle_message(m)) == NULL) {
        return EASY_ERROR;
    }

    m->status = ((m->input->pos < m->input->last) ? EASY_MESG_READ_AGAIN : EASY_OK);
    if (c->handler->batch_process) {
        (c->handler->batch_process)(m);
    }

    if (m->enable_trace) {
        easy_debug_log("Server recieved keepalive, m(%p), "
                "request_list_count(%d), cnt(%d), ref(%ld), pos(%p), last(%p).\n",
                m, m->request_list_count, cnt, m->pool->ref, m->input->pos, m->input->last);
    }

    if ((m->request_list_count == 0) && (m->status != EASY_MESG_READ_AGAIN)) {
        /*
         * If there is only one received keepalive packet in message, no request is
         * bound to the message and the message will be leaked. We destroy it here.
         */
        if (m->enable_trace) {
            easy_debug_log("Destroying message for keepalive.");
        }
        easy_message_destroy(m, 1);
    } else {
        ret = easy_connection_process_request(c, &m->request_list);
        if (ret != EASY_OK) {
            return ret;
        }
    }

    if (SSM_USE_SSL == c->ssl_sm_
        && c->handler->is_ssl
        && c->handler->is_ssl_opt
        && NULL == c->sc
        && NULL != c->ioth->eio->ssl
       ) {
        easy_info_log("easy_connection_do_request ssl %s\n", easy_connection_str(c));

        int need_destory = 0;
        if (EASY_OK != easy_spinrwlock_rdlock(&(c->ioth->eio->ssl_rwlock_))) {
            easy_error_log("easy_spinrwlock_rdlock failed %s, ref_cnt %ld, wait_write %ld\n",
                    easy_connection_str(c),
                    c->ioth->eio->ssl_rwlock_.ref_cnt, c->ioth->eio->ssl_rwlock_.wait_write);
            need_destory = 1;
        } else {
            if (EASY_OK != easy_ssl_connection_create(c->ioth->eio->ssl->server_ctx, c)) {
                easy_error_log("easy_ssl_connection_create failed %s, cb: %p\n",
                        easy_connection_str(c), ev_cb(&c->read_watcher));
                need_destory = 1;
            }
            c->ssl_sm_ = SSM_NONE;
            // set read callback
            ev_set_cb(&c->read_watcher, easy_ssl_connection_handshake);

            if (EASY_OK != easy_spinrwlock_unlock(&(c->ioth->eio->ssl_rwlock_))) {
                easy_error_log("easy_spinrwlock_unlock failed %s, ref_cnt %ld, wait_write %ld\n",
                        easy_connection_str(c),
                        c->ioth->eio->ssl_rwlock_.ref_cnt, c->ioth->eio->ssl_rwlock_.wait_write);
                need_destory = 1;
            } else {
                easy_connection_evio_start(c);
            }
        }

        if (need_destory) {
            easy_pool_destroy(c->pool);
            easy_info_log("Socket closed, fd(%d), conn(%s).", c->fd, easy_connection_str(c));
            close(c->fd);
            return EASY_ERROR;
        }
    } else {
        // 加入监听
        if (c->event_status == EASY_EVENT_READ && !c->wait_close) {
            easy_connection_evio_start(c);
        }

        easy_connection_redispatch_thread(c);
    }

    return EASY_OK;
}


/**
 * write事件处理
 */
void easy_connection_on_writable(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_connection_t       *c;
    int                     ret;

    c = (easy_connection_t *)w->data;
    EASY_TIME_GUARD();
    assert(c->fd == w->fd);

    /* if not openssl, start negotiation here, or do negotiation in easy_ssl_client_handshake */
    if (c->type == EASY_TYPE_CLIENT && !(c->client->is_ssl)) {
        if (c->is_negotiated == 0) {
            if ((ret = easy_send_negotiate_message(c)) == EASY_OK) {
                c->is_negotiated = 1;
            } else {
                easy_info_log("send negotiate msg failed!(%s), retry!", easy_connection_str(c));
                goto error_exit;
            }
        }
    }

    // wait client time
    if (c->wcs > 0.0) {
        c->wait_client_time += (ev_now(c->loop) - c->wcs);
        c->wcs = 0.0;
    }

    if ((ret = easy_connection_write_socket(c)) == EASY_ABORT) {
        easy_warn_log("Failed to write(on_writable), conn(%s)",
                easy_connection_str(c));
        goto error_exit;
    }

    // 没数据可发, 把write停掉
    if (easy_list_empty(&c->output)) {
        if (easy_connection_redispatch_thread(c) == EASY_ASYNC) {
            return;
        }

        ev_io_stop(c->loop, &c->write_watcher);
    }

    // client
    if (c->type == EASY_TYPE_CLIENT) {
        // connected.
        if (c->status == EASY_CONN_CONNECTING) {
            EASY_TIME_GUARD();
            c->status = EASY_CONN_OK;
            ev_io_start(c->loop, &c->read_watcher);
            ev_timer_set(&c->timeout_watcher, 0.0, 0.5);
            ev_timer_again(c->loop, &c->timeout_watcher);
            c->armed_ack_timeout = -1;

            if (easy_socket_error(c->fd) != 0) {
                c->conn_has_error = 1;
                easy_error_log("Failed to establish connection(%s)",
                        easy_connection_str(c));
                goto error_exit;
            }

            // on connect
            if (c->handler->on_connect && (c->handler->on_connect)(c) == EASY_ERROR) {
                easy_error_log("Failed to do on_connect on connection(%s)",
                        easy_connection_str(c));
                goto error_exit;
            }

            c->last_tx_tstamp  = ev_now(loop);
            c->last_rx_tstamp  = ev_now(loop);
            easy_info_log("Connection established on client side, conn(%s).\n",
                    easy_connection_str(c));
        }

        // send new packet
        if (c->handler->new_packet && ret == EASY_OK && c->doing_request_count < (EASY_CONN_DOING_REQ_CNT / 2)) {
            if ((c->handler->new_packet)(c) == EASY_ERROR) {
                CONN_DESTROY_LOG("new_packet callback fail");
                goto error_exit;
            }
        }

    } else {
        if (easy_list_empty(&c->output) && (!easy_list_empty(&c->server_session_list))) {
            /*
             * Handle the easy_requests with retcode == EASY_AGAIN returned by upper-layer moduels.
             * Will wakeup the server threads waiting on the client_wait of easy_requests.
             */
            ret = easy_connection_process_request(c, &c->server_session_list);
            if (ret == EASY_ERROR) {
                CONN_DESTROY_LOG("process_request fail");
                goto error_exit;
            } else if (ret == EASY_ASYNC) {
                return;
            }

            if (!(easy_list_empty(&c->output))) {
                ev_io_start(c->loop, &c->write_watcher);
            }
        }
    }

    return;
error_exit:
    EASY_CONNECTION_DESTROY(c, "on_writable");
}

/**
 * 对timeout的处理message
 */
static void easy_connection_on_timeout_session(struct ev_loop *loop, ev_timer *w, int revents)
{
    static int now = 0;
    easy_connection_t *c;
    easy_session_t *s;

    s = (easy_session_t *)w->data;
    c = s->c;

    EASY_TIME_GUARD();
    if ((now != (int)ev_now(loop)) && (s->error == 0)) {
        easy_info_log("Session has timed out, session(%p), time(%fs), packet_id(%" PRIu64 "),"
                " pcode(%d), trace_id(" OB_TRACE_ID_FORMAT "), conn(%s).",
                s, ev_now(loop) - s->now, s->packet_id,
                s->r.pcode, s->r.trace_id[0], s->r.trace_id[1], easy_connection_str(c));
        now = (int)ev_now(loop);
    }

    // process
    easy_hash_del(c->send_queue, s->packet_id);
    easy_list_del(&s->send_queue_list);
    s->packet_id = 0;

    if (easy_session_process_keep_connection_resilient(s, 1, EASY_TIMEOUT) == EASY_ERROR) {
        easy_error_log("Failed to proccess session (keep resilient), s(%p), conn(%s).",
                s, easy_connection_str(c));
        EASY_CONNECTION_DESTROY(c, "on_timeout_session");
    }
}

static int easy_connection_update_ack_bytes_and_time(easy_connection_t* c, ev_tstamp now)
{
    int ret;
    int64_t qlen = 0;

    EASY_TIME_GUARD();

    ret = ioctl(c->fd, TIOCOUTQ, &qlen);
    if (0 != ret) {
        easy_error_log("Failed to do TIOCOUTQ ioctl on connection(%s), errno(%d), strerror(%s).",
                easy_connection_str(c), errno, strerror(errno));
    } else {
        if (qlen <= 0) {
            c->ack_wait_start = 0;
            c->ack_bytes = c->send_bytes;
        } else {
            int64_t ack_bytes = c->send_bytes - qlen;
            if (c->ack_wait_start <= 0 || ack_bytes != c->ack_bytes) {
                c->ack_bytes = ack_bytes;
                c->ack_wait_start = now;
            }
        }
    }

    return ret;
}


int easy_connection_dump_request(easy_connection_t *conn, easy_request_t *r, double hold_time)
{
    int ret = EASY_OK;
    if (conn == NULL || r == NULL) {
        easy_warn_log("conn should not be NULL");
        ret = EASY_ERROR;
        goto out;
    }

    if (r->protocol == EASY_REQQUEST_TYPE_RPC) {
        easy_warn_log("easy_reqeust hold by upper-layer for too much time. "
                "req(%p), timeout_warn_count(%lu), protocol(RPC), pcode(%d), time(%lf), packet_id(%lu), "
                "trace_id(" OB_TRACE_ID_FORMAT "), trace_point(%d), bt(%s).",
                r, r->timeout_warn_count, r->pcode, hold_time, r->packet_id,
                r->trace_id[0], r->trace_id[1], r->trace_point, r->trace_bt);
    } else if (r->protocol == EASY_REQQUEST_TYPE_SQL) {
        easy_warn_log("easy_reqeust hold by upper-layer for too much time. "
                "req(%p), timeout_warn_count(%lu), protocol(SQL), time(%lf), session_id(%ld), "
                "trace_point(%d), bt(%s).",
                r, r->timeout_warn_count, hold_time, r->session_id, r->trace_point, r->trace_bt);
    } else {
        easy_warn_log("wong protocol(%d).", r->protocol);
        ret = EASY_ERROR;
    }

out:
    return ret;
}


int easy_connection_check_rx_request(easy_connection_t *conn, double now)
{
    int ret = EASY_OK;
    easy_message_t *m, *mn;
    easy_request_t *r, *rn;
    double hold_time;
    uint64_t timeout_req_count = 0;

    if (conn == NULL) {
        easy_warn_log("conn should not be NULL");
        ret = EASY_ERROR;
        goto err;
    }

    easy_list_for_each_entry_safe(m, mn, &conn->message_list, message_list_node) {
        easy_list_for_each_entry_safe(r, rn, &m->all_list, all_node) {
            hold_time = now - r->start_time;
            if (hold_time > 9.0) {
                if ((r->timeout_warn_count < MAX_REQUEST_TIMEOUT_WARN_COUNT) ||
                        (0 == (r->timeout_warn_count & (r->timeout_warn_count - 1))) ||
                        (0 == (r->timeout_warn_count & 0x3ff))) {
                    easy_connection_dump_request(conn, r, hold_time);
                }
                r->timeout_warn_count++;
                timeout_req_count++;
            } else {
                goto out;
            }
        }
    }

out:
    if (timeout_req_count > 0) {
        if (EASY_REACH_TIME_INTERVAL(9 * 1000 * 1000)) {
            easy_warn_log("Totally %ld easy_reqeusts hold by upper-layer more than 9s.",
                    timeout_req_count);
        }
    }

err:
    return ret;
}


/**
 * 对timeout的处理connection
 */
static void easy_connection_on_timeout_conn(struct ev_loop *loop, ev_timer *w, int revents)
{
    int ret;
    double t, now;
    uint32_t ack_timeout_ms;
    easy_connection_t *conn;
    easy_io_t *eio;
    void *ev_timer_pending_addr = ev_watch_pending_addr;
    int ev_timer_pending = ev_watch_pending;

    conn = (easy_connection_t *)w->data;
    EASY_TIME_GUARD();

    if (conn->status == EASY_CONN_AUTO_CONN) {
        easy_connection_autoconn(conn);
    } else if (conn->status != EASY_CONN_OK) { // EASY_CONN_CLOSE, EASY_CONN_CONNECTING
        conn->conn_has_error = 1;

        if (conn->destroy_retry_count < MAX_LOG_RETRY_COUNT) {
            easy_info_log("Connection not in ESTABLISHED state, conn(%s), status(%d), ref(%d), time(%fs).",
                    easy_connection_str(conn), conn->status, conn->pool->ref, (ev_now(loop) - conn->last_time));
        }

        if (conn->status == EASY_CONN_CLOSE && conn->ioth->eio->force_destroy_second > 0) {
            now = ev_now(loop) - conn->ioth->eio->force_destroy_second;
            if (conn->last_time < now) {
                if (0 == conn->ioth->eio->no_force_destroy) {
                    conn->pool->ref = 0;
                } else if (1 == conn->ioth->eio->no_force_destroy && 0 == conn->slow_request_dumped) {
                    if (EASY_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                        easy_connection_dump_slow_request(conn);
                    }
                }
            }
        }

        /*
         * TODO:
         *  If the upper-layer holds requests and does not release them back to easy,
         *  here will be dead loop and logs will be pinted endlessly.
         */
        goto destroy_conn;
    } else { // EASY_CONN_OK
        now = ev_now(loop);
        if (conn->life_idle) {
            t = now - easy_max(conn->start_time, conn->last_time);
        } else {
            t = now - conn->last_time;
        }

        if (EASY_OK != easy_connection_check_rx_request(conn, now)) {
            easy_error_log("Failed to check armed RX request, conn(%s).", easy_connection_str(conn));
        }

        if (conn->handler->on_idle && (conn->idle_time / 1000.0 < t)) {
            // on_idle
            ret = (conn->handler->on_idle)(conn);
            if (ret == EASY_ABORT) {
                return;
            }

            if (ret == EASY_ERROR) {
                easy_error_log("Easy on_idle failed, and will destroy connection(%s), errno(%d).",
                        easy_connection_str(conn), errno);
                goto destroy_conn;
            }
        }

        eio = conn->ioth->eio;
        if (conn->keepalive_enabled != eio->keepalive_enabled) {
            if (conn->keepalive_enabled == 0) {
                conn->tx_keepalive_cnt = 0;
                conn->rx_keepalive_cnt = 0;
            }
            conn->keepalive_enabled = eio->keepalive_enabled;

            /*
             * reset last_rx_tstamp when keepalive just enabled, to
             * avoid failure.
             */
            conn->last_rx_tstamp  = ev_now(conn->loop);
        }

        ack_timeout_ms = eio->ack_timeout;
        if ((conn->keepalive_enabled) && (ack_timeout_ms > 0) &&
                (conn->magic_ver >= MIN_MAGIC_VERSION_KEEPALIVE)) {
            if (now > (conn->last_rx_tstamp + ack_timeout_ms / 1000 - 0.1)) {
                easy_warn_log("Easy keepalive failed, and will destroy connection, time(%fs), conn(%s).",
                        (now - conn->last_rx_tstamp), easy_connection_str(conn));
                conn->conn_has_error = 1;
                conn->keepalive_failed = 1;
                goto destroy_conn;
            }

            /*
             * At the begining when the connection established, client initiatively
             * sends 3 keepalive packets to server to trigger keepalive process.
             */
            if ((conn->tx_keepalive_cnt < 3) ||
                    (now > (conn->last_tx_tstamp + ack_timeout_ms / 3000 - 0.1))) {
                if (conn->handler->new_keepalive_packet) {
                    ret = (conn->handler->new_keepalive_packet)(conn);
                    if (ret != EASY_OK) {
                       easy_warn_log("Failed to send keepalive on connection(%p).", conn);
                        if (ret ==  EASY_ABORT) {
                            /*
                             * conn has already been destroyed when new_keepalive_packet returns EASY_ABORT.
                             */
                             goto exit;
                        } else {
                            goto destroy_conn;
                        }
                    } else {
                        conn->tx_keepalive_cnt++;
                        easy_debug_log("Keepalive message sent, conn(%s)\n",
                                easy_connection_str(conn));
                    }
                }
            } else {
                easy_debug_log("No need to send keepalive message, conn(%s)\n",
                        easy_connection_str(conn));
            }
        }

        if (ack_timeout_ms > 0) {
            ret = easy_connection_update_ack_bytes_and_time(conn, now);
            if (ret == 0) {
                if (conn->ack_wait_start > 0 &&
                        (now > conn->ack_wait_start + ack_timeout_ms / 1000.0 - 0.1)) {
                    easy_error_log("Failed to do check_ack_timeout, and will destroy connection(%s).",
                            easy_connection_str(conn));
                    conn->conn_has_error = 1;
                    goto destroy_conn;
                }
            } else {
                easy_info_log("Unexpected problem, conn(%s), ev_is_pending(%d), ev_is_active(%d),"
                        " ev_timer_pending_addr(%p), ev_timer_pending(%d), timeout_watcher(%p).",
                        easy_connection_str(conn), ev_is_pending(&conn->timeout_watcher),
                        ev_is_active(&conn->timeout_watcher), ev_timer_pending_addr,
                        ev_timer_pending, &conn->timeout_watcher);
                /*
                 * we do not go to destory conn, but to let system crash when the issue happens.
                 */
                // conn->conn_has_error = 1;
                // goto destroy_conn;
            }
        }
        easy_connection_rearm_failure_detection_timer(conn);
    }

    return;

destroy_conn:
    EASY_CONNECTION_DESTROY(conn, "on_timeout_conn");
exit:
    return;
}


/**
 * 把connection上的output的buffer写到socket上
 *
 * @param c - easy_connection_t对象
 * @return  - EASY_ABORT 网络断开
 *            EASY_AGAIN 没写完,需要继续写
 *            EASY_OK    写完了
 */
int easy_connection_write_socket(easy_connection_t *c)
{
    ssize_t ret;

    // 空的直接返回
    if (easy_list_empty(&c->output)) {
        return EASY_OK;
    }

    // 加塞
    if (EASY_IOTH_SELF->eio->tcp_cork && c->tcp_cork_flag == 0) {
        if (easy_socket_set_tcpopt(c->fd, TCP_CORK, 1)) {
        } else {
            c->tcp_cork_flag = 1;
        }
    }

    ret = (c->write)(c, &c->output);
    if (ret > 0) {
        c->con_summary->out_byte += ret;
        c->send_bytes += ret;
        c->last_tx_tstamp = ev_now(c->loop);
        easy_connection_update_rlmtr(c, ret);
    }
    if (ret == EASY_ERROR) {
        easy_warn_log("Failed to write on connection(%p), ret(%d).\n", c, ret);
        c->conn_has_error = 1;
        ev_io_stop(c->loop, &c->write_watcher);
        return EASY_ABORT;
    }

    c->last_time = ev_now(c->loop);
    ret = easy_connection_write_again(c);

    return ret;
}

// 判断write again
int easy_connection_write_again(easy_connection_t *c)
{
    // 还有没写出去, 起写事件
    if (!easy_list_empty(&c->output)) {
        if (c->handler->sending_data) {
            c->handler->sending_data(c);
        }

        c->wcs = ev_now(c->loop);
        ev_io_start(c->loop, &c->write_watcher);
        return EASY_AGAIN;
    } else if (c->handler->send_data_done) {
        return c->handler->send_data_done(c);
    } else {
        if (c->type == EASY_TYPE_SERVER) {
            if (c->wait_close && easy_list_empty(&c->server_session_list)) {// 需要关闭掉
                easy_info_log("Shutting down socket with SHUT_WR, fd(%d), conn(%s).\n",
                        c->fd, easy_connection_str(c));
                c->wait_close = 0;
                shutdown(c->fd, SHUT_WR);
                return EASY_ABORT;
            }
        }

        // tcp_cork
        if (EASY_IOTH_SELF->eio->tcp_cork && c->tcp_cork_flag) {
            if (easy_socket_set_tcpopt(c->fd, TCP_CORK, 0)) {
            } else {
                c->tcp_cork_flag = 0;
            }
        }
    }

    return EASY_OK;
}

/**
 * 得到packet的id
 */
uint64_t easy_connection_get_packet_id(easy_connection_t *c, void *packet, int flag)
{
    uint64_t                packet_id = 0;

    if (c->handler->get_packet_id) {
        packet_id = (c->handler->get_packet_id)(c, packet);
    } else {
        if (c->send_queue) {
            packet_id = c->send_queue->seqno;
            if (flag) {
                packet_id -= c->send_queue->count;
            }

            packet_id <<= 16;
            packet_id |= (c->fd & 0xffff);
        }
    }

    return packet_id;
}

/**
 * new 出一个connection_t对象
 */
static easy_connection_t *easy_connection_new()
{
    easy_pool_t             *pool;
    easy_connection_t       *c;

    // 为connection建pool
    if ((pool = easy_pool_create(0)) == NULL)
        return NULL;

    // 创建easy_connection_t对象
    c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));

    if (c == NULL)
        goto error_exit;

    // 初始化
#ifdef EASY_DEBUG_MAGIC
    c->magic = EASY_DEBUG_MAGIC_CONNECT;
#endif

    c->pool = pool;
    c->reconn_time = 100;
    c->idle_time = 60000;
    c->first_msglen = EASY_FIRST_MSGLEN; // 1Kbyte
    c->default_msglen = EASY_IO_BUFFER_SIZE; // 16Kbyte
    c->read = easy_socket_read;
    c->write = easy_socket_write;
    c->fd = -1;
    c->armed_ack_timeout = -1;
    c->tls_version_option = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;

    easy_list_init(&c->message_list);
    easy_list_init(&c->server_session_list);
    easy_list_init(&c->conn_list_node);
    easy_list_init(&c->group_list_node);
    // easy_list_init(&c->rlmtr_list_node);
    easy_list_init(&c->output);
    easy_list_init(&c->client_session_list);
    ev_init(&c->pause_watcher, easy_connection_on_pause);

    return c;
error_exit:
    easy_pool_destroy(pool);
    return NULL;
}

/**
 * server回复
 */
static int easy_connection_send_response(easy_list_t *request_list)
{
    int64_t send_count = 0;
    easy_request_t              *r, *rn;
    easy_message_t              *m;
    easy_connection_t           *c = NULL, *nc = NULL;
    easy_list_t                 wlist = EASY_LIST_HEAD_INIT(wlist);
    easy_list_t                 flist = EASY_LIST_HEAD_INIT(flist);
    int                         ret;
    easy_io_thread_t            *ioth = EASY_IOTH_SELF, *dst_ioth;
    char                        dest_addr[32];

    EASY_TIME_GUARD();

    // encode
    easy_list_for_each_entry_safe(r, rn, request_list, request_list_node) {
        send_count++;
        easy_list_del(&r->request_list_node);
        m = (easy_message_t *)r->ms;
        c = m->c;

        if (c == NULL) {
            easy_error_log("easy_connection_send_response, reqeust(%p), conn(%s), ioth(%d), m(%p).",
                    r, easy_connection_str(c), ioth->idx, m);
            goto error_exit;
        }

        if (r->ratelimit_enabled) {
            if (!r->redispatched) {
                if (!c->ratelimit_enabled) {
                    c->ratelimit_enabled = 1;
                    easy_info_log("Ratelimit enabled on server side, reqeust(%p), conn(%s).",
                            r, easy_connection_str(c));
                }

                /*
                 * For response, no mather whether ioth->eio->easy_rlmtr->ratelimit_enabled
                 * is true, it should be forwarded to region ratelimitor. The response is
                 * currently not handled by the ioth that m->c is bound to.
                 */
                easy_debug_log("forward request to region ratelimitor, addr(%s), req(%p), ms(%p), ioth->idx(%d), req->pkt_id(%lx), conn(%s).\n",
                        easy_inet_addr_to_str(&(c->addr), dest_addr, 32), r, m, ioth->idx, (r->packet_id >> 16), easy_connection_str(c));
                ret = easy_connection_forward_request_to_rlmtr(r);
                if (ret != EASY_OK) {
                    /*
                     * When region ratelimitor is not found, the response should be directly forwarded to its
                     * dst ioth, instead of region ratelimitor.
                     */
                    dst_ioth = r->ms->c->ioth;
                    easy_spin_lock(&dst_ioth->thread_lock);
                    r->redispatched = 1;
                    easy_list_add_tail(&r->request_list_node, &dst_ioth->request_list);
                    easy_spin_unlock(&dst_ioth->thread_lock);
                    easy_debug_log("forward request to dst_ioth, addr(%s), req(%p), ms(%p), ioth->idx(%d), req->pkt_id(%lx), "
                            "all_node->prev(%p), all_node->next(%p)\n",
                            easy_inet_addr_to_str(&(c->addr), dest_addr, 32), r, m, dst_ioth->idx, (r->packet_id >> 16),
                            r->all_node.prev, r->all_node.next);
                    ev_async_send(dst_ioth->loop, &dst_ioth->thread_watcher);
                }
                // r->redispatched = 1;
                continue;
            } else {
                easy_debug_log("response redispatched, addr(%s), req(%p), ioth->idx(%d), req->pkt_id(%lx).\n",
                        easy_inet_addr_to_str(&(c->addr), dest_addr, 32), r, ioth->idx, (r->packet_id >> 16));
            }
        } else {
            // assert(!c->ratelimit_enabled);
        }

        // 从其他进程返回后
        ret = EASY_ERROR;

        /*
         * m->pool->ref and c->pool->ref are increased by easy_request_sleeping
         * which is called in ObRpcHandler::process and ObMySQLHandler::process.
         * We decrease them here.
         */
        easy_atomic_dec(&m->pool->ref);

        if (r->retcode != EASY_ERROR) {
            if (r->opacket == NULL && r->retcode == EASY_AGAIN) {
                SERVER_PROCESS(c, r);
            } else if ((ret = easy_connection_request_done(r)) == EASY_OK) {
                if (easy_list_empty(&c->group_list_node)) {
                    easy_list_add_tail(&c->group_list_node, &wlist);
                }
            }
        } else { // 如果出错
            easy_list_del(&c->group_list_node);
            easy_list_add_tail(&c->group_list_node, &flist);
        }

        // 引用计数
        easy_atomic_dec(&c->pool->ref);

        // message是否也不在使用了
        if (ret == EASY_OK && m->request_list_count == 0 && m->status != EASY_MESG_READ_AGAIN) {
            easy_message_destroy(m, 1);
        }
    }

    // failure request, close connection
    easy_list_for_each_entry_safe(c, nc, &flist, group_list_node) {
        easy_list_del(&c->group_list_node);
        EASY_CONNECTION_DESTROY(c, "handle fail list");
    }

    // foreach write socket
    easy_list_for_each_entry_safe(c, nc, &wlist, group_list_node) {
        easy_list_del(&c->group_list_node);
        if (easy_connection_write_socket(c) == EASY_ABORT) {
            EASY_CONNECTION_DESTROY(c, "write fail in send_response");
        } else if (c->type == EASY_TYPE_SERVER) {
            easy_connection_redispatch_thread(c);
        }
    }

    return EASY_OK;
error_exit:
    return EASY_ABORT;
}

int easy_connection_send_session_list(easy_list_t *list)
{
    int ret;
    int64_t send_count = 0;
    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;
    easy_list_t             wlist = EASY_LIST_HEAD_INIT(wlist);
    easy_list_t             session_list = EASY_LIST_HEAD_INIT(session_list);
    int                     status;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    char                    dest_addr[32];

    EASY_TIME_GUARD();

    // foreach encode
    easy_list_for_each_entry_safe(s, s1, list, session_list_node) {
        send_count++;
        easy_list_del(&s->session_list_node);

        s->r.client_send_time = current_time();
        // write buffer
        if (unlikely(s->type >= EASY_TYPE_WBUFFER)) {
            c = s->c;

            if (s->type == EASY_TYPE_WBUFFER) {
                easy_list_add_tail(&((easy_buf_t *)(((easy_message_session_t *)s) + 1))->node, &c->output);
                if (easy_list_empty(&c->group_list_node)) {
                    easy_list_add_tail(&c->group_list_node, &wlist);
                }

            } else if (s->type == EASY_TYPE_PAUSE) {
                easy_connection_pause(c, s->align);
                easy_session_destroy(s);
            } else if (s->type == EASY_TYPE_LISTEN) {
                easy_connection_listen_watcher(s);
                easy_session_destroy(s);
            }

            continue;
        }

        if (s->ratelimit_enabled) {
            if (s->redispatched) {
                easy_debug_log("session redispatched, addr(%s), sess(%p), ioth->idx(%d), session->pkt_id(%lx).\n",
                        easy_inet_addr_to_str(&(s->addr), dest_addr, 32), s, ioth->idx, (s->packet_id >> 16));
            }
            if (!s->redispatched && (ioth->eio->easy_rlmtr->ratelimit_enabled)) {
                easy_debug_log("forwarding session to ratelimitor, sess(%p), addr(%s), ioth->idx(%d).\n",
                        s, easy_inet_addr_to_str(&(s->addr), dest_addr, 32), ioth->idx);
                if (ioth != ioth->eio->easy_rlmtr->ratelimit_thread) {
                    easy_warn_log("sessions with ratelimit enabled should not be handled by this ioth, sess(%p), addr(%s), ioth->idx(%d).\n",
                        s, easy_inet_addr_to_str(&(s->addr), dest_addr, 32), ioth->idx);
                } else {
                    ret = easy_connection_forward_session_to_rlmtr(s);
                    if (ret == EASY_OK) {
                        continue;
                    }
                }
            }
            s->redispatched = 1;
        } else {
            easy_debug_log("session ratelimit_enabled not enabled, sess(%p).\n", s);
        }

        // connect, disconnect
        status = s->status;

        s->r.client_connect_time = current_time();
        if ((c = easy_connection_do_client(s)) == NULL || (status & 0x02)) {
            continue;
        }

        // build session
        s->c = c;
        if (easy_connection_session_build(s) == EASY_OK) {
            if (easy_list_empty(&s->c->group_list_node)) {
                easy_list_add_tail(&s->c->group_list_node, &wlist);
            }
            easy_list_add_tail(&s->write_list_node, &session_list);
        }
    }

    easy_list_for_each_entry_safe(s, s1, &session_list, write_list_node) {
        s->r.client_write_time = current_time();
    }

    // foreach
    easy_list_for_each_entry_safe(c, c1, &wlist, group_list_node) {
        easy_list_del(&c->group_list_node);
        easy_connection_sendsocket(c);
    }

    return EASY_OK;
}


static easy_message_t *easy_connection_recycle_message(easy_message_t *m)
{
    easy_message_t          *newm;
    int                     len, olen;

    len = (m->input->last - m->input->pos);
    if (m->recycle_cnt < 16 || len == 0) {
        return m;
    }

    /*
     * When easy_message is used for >= 16 times and there is incomplete packet,
     * easy creates a new message.
     */
    // 增加default_message_len大小
    olen = m->c->first_msglen;
    m->c->first_msglen = easy_max(len, olen);
    newm = easy_message_create(m->c);
    m->c->first_msglen = olen;
    if (newm == NULL) {
        return NULL;
    }

    // 把旧的移到新的上面
    memcpy(newm->input->pos, m->input->pos, len);
    newm->input->last += len;
    newm->status = EASY_MESG_READ_AGAIN;
    m->input->pos = m->input->last;

    // 删除之前的message
    if (m->request_list_count == 0) {
        /*
         * If no pending request on the message, the message will be destroyed here.
         * Otherwise, the message will be destroyed after all the reqeusts have been
         * processed.
         */
        easy_message_destroy(m, 1);
        return newm;
    } else {
        m->status = EASY_OK;
        return m;
    }
}

/**
 * 连接到addrv
 */
static easy_connection_t *easy_connection_do_connect(easy_client_t *client, int fd, int is_ssl_for_test)
{
    struct sockaddr_storage addr;
    int                     v;
    easy_connection_t       *c;
    double                  t;

    // 建立一个connection
    if ((c = easy_connection_new()) == NULL) {
        easy_error_log("new connect failure.\n");
        return NULL;
    } else {
        easy_debug_log("easy_connection_new, conn(%p).", c);
    }

    memset(&addr, 0, sizeof(addr));
    easy_inet_etoa(&client->addr, &addr);

    if (fd < 0 && (fd = socket(addr.ss_family, SOCK_STREAM, 0)) < 0) {
        easy_error_log("socket failure, errno(%d), strerror(%s).\n", errno, strerror(errno));
        goto error_exit;
    }

    c->fd = fd;
    c->type = EASY_TYPE_CLIENT;
    c->handler = client->handler;
    c->addr = client->addr;
    c->client = client;
    c->local_magic_ver = CURENT_MAGIC_VERSION;
    c->peer_magic_ver  = LEGACY_MAGIC_VERSION;
    c->magic_ver       = CURENT_MAGIC_VERSION; /* Client triggers keepalive. */

    easy_socket_non_blocking(fd);

    // 连接
    if (EASY_IOTH_SELF->eio->tcp_nodelay && addr.ss_family != AF_UNIX) {
        easy_ignore(easy_socket_set_tcpopt(fd, TCP_NODELAY, 1));
    }

    size_t sock_addr_len = addr.ss_family != AF_UNIX ? sizeof(addr) : sizeof(struct sockaddr_un);
    if (connect(fd, (struct sockaddr *)&addr, sock_addr_len) < 0) {
        if (errno != EINPROGRESS) {
            easy_error_log("Failed to do connect, conn(%s), errno(%d), strerror(%s).\n",
                    easy_connection_str(c), errno, strerror(errno));
            goto error_exit;
        }

        c->status = EASY_CONN_CONNECTING;
    } else {
        easy_info_log("Connection established, conn(%s).\n", easy_connection_str(c));
        c->status = EASY_CONN_OK;
    }

    // self connect self
    if (easy_connection_checkself(c) == EASY_ERROR) {
        goto error_exit;
    }

    v = offsetof(easy_session_t, send_queue_hash);
    c->send_queue = thread_local_send_queue ? : (thread_local_send_queue = easy_hash_create_without_pool(EASY_IOTH_SELF->eio->send_qlen, v));

    if (c->send_queue == NULL) {
        easy_error_log("easy_hash_create failure.");
        goto error_exit;
    }

    // 初始化事件
    ev_io_init(&c->read_watcher, easy_connection_on_readable, fd, EV_READ);
    ev_io_init(&c->write_watcher, easy_connection_on_writable, fd, EV_WRITE);
    t = (client->timeout ? client->timeout : EASY_CLIENT_DEFAULT_TIMEOUT) / 1000.0;
    if (EASY_IOTH_SELF->eio->conn_timeout  > 0) {
        t = EASY_IOTH_SELF->eio->conn_timeout / 1000.0;
    }
    ev_timer_init(&c->timeout_watcher, easy_connection_on_timeout_conn, t, 0.0);

    c->read_watcher.data = c;
    c->write_watcher.data = c;
    c->timeout_watcher.data = c;

    // event_status
    if (c->status == EASY_CONN_CONNECTING)
        v = (EASY_EVENT_TIMEOUT | EASY_EVENT_WRITE);
    else
        v = (EASY_EVENT_TIMEOUT | EASY_EVENT_READ);

    c->event_status = v;
    c->ioth = EASY_IOTH_SELF;
    c->loop = c->ioth->loop;
    c->ssl_sm_ = SSM_NONE;

    if (c->status == EASY_CONN_OK) {
        c->last_tx_tstamp = ev_now(c->loop);
        c->last_rx_tstamp = ev_now(c->loop);
    }

    if (client->is_ssl && c->sc == NULL) {
        //send ssl req pkt first, just for test !!!!!!
        if (1 == is_ssl_for_test) {
            char buff[sizeof(easy_http_packet_t)];
            easy_http_packet_t *packet = (easy_http_packet_t *)buff;
            memset(buff, 0, sizeof(easy_http_packet_t));
            packet->is_raw_header = 1;
            write(fd, buff, sizeof(easy_http_packet_t));
        }

        // set write callback
        ev_set_cb(&c->write_watcher, easy_ssl_client_handshake);

        if (c->status != EASY_CONN_CONNECTING) {
            easy_ssl_client_do_handshake(c);
        }
    }

    //locate node
    c->con_summary = easy_summary_locate_node(c->fd, c->ioth->eio->eio_summary, 0);

    // 加入
    easy_list_add_tail(&c->conn_list_node, &c->ioth->connected_list);
    easy_atomic32_inc(&c->ioth->tx_conn_count);
    easy_connection_evio_start(c);
    return c;

error_exit:
    if (fd >= 0) {
        easy_info_log("Socket closed, fd(%d).", fd);
        close(fd);
    }
    easy_pool_destroy(c->pool);
    return NULL;
}

static easy_connection_t *easy_connection_do_client(easy_session_t *s)
{
    int errcode = 0;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    easy_connection_t       *c = NULL;
    easy_client_t           *client;
    int                     error = 0;
    int                     afd = -1;
    int                     is_ssl_for_test  = 0;

    // hashcode
    client = (easy_client_t *)easy_client_list_find(ioth->client_list, &s->addr);

    // 正常的session
    if (likely(s->status == 0)) {
        c = (client && client->ref ? client->c : NULL);

        if (s->callback == NULL && c && c->handler) {
            s->callback = c->handler->process;
        }

        if (unlikely(c == NULL || ioth->eio->stoped)) {
            s->error = 1;
            c = NULL;
            easy_session_process(s, 0, EASY_STOP);
        }

        return c;
        // 连接指令
    } else if ((s->status & 0x01) != 0) {
        if (client == NULL) {
            if ((client = (easy_client_t *)easy_array_alloc(ioth->client_array)) == NULL) {
                error = 1;
                s->error = 1;
                errcode = EASY_ALLOC_FAIL;
                goto error_exit;
            }

            memset(client, 0, sizeof(easy_client_t));
            client->addr = s->addr;
            client->handler = (easy_io_handler_pt *)s->thread_ptr;
            client->timeout = (int)s->timeout;
            client->user_data = s->r.args;
            client->is_ssl = 0;

            if (s->r.reserved > 0) {
                afd = s->r.reserved;
            }

            if (ioth->eio->ssl && (s->packet_id & EASY_CONNECT_SSL)) {
                if (s->packet_id & EASY_CONNECT_SSL_OPT_TEST) {
                    is_ssl_for_test = 1;
                }
                //ob already use user_data
                if (!(s->packet_id & EASY_CONNECT_SSL_OB)) {
                    client->server_name = s->r.user_data;
                }
                client->is_ssl = 1;
            }

            s->thread_ptr = NULL;
            easy_client_list_add(ioth->client_list, &client->addr, &client->client_list_node);
        } else {
            client->timeout = (int)s->timeout;
            if (client->c == NULL && ioth->eio->ssl && (s->packet_id & EASY_CONNECT_SSL)) {
                if (s->packet_id & EASY_CONNECT_SSL_OPT_TEST) {
                    is_ssl_for_test = 1;
                }
                //ob already use user_data
                if (!(s->packet_id & EASY_CONNECT_SSL_OB)) {
                    client->server_name = s->r.user_data;
                }
                client->is_ssl = 1;
            }
        }


        if (client->c == NULL) {
            client->c = easy_connection_do_connect(client, afd, is_ssl_for_test);
            if (client->c == NULL) {
                error = 1;
                s->error = 1;
                errcode = EASY_CONNECT_FAIL;

                easy_error_log("Failed to do connect, conn(%s).\n", easy_connection_str(c));
                goto error_exit;
            } else {
                easy_info_log("Establishing connection(%s), s(%p), is_ssl(%d), ioth(%d).",
                        easy_connection_str(client->c), s, NULL != client ? client->is_ssl : -1, ioth->idx);
            }
        }

        c = client->c;
        if (ioth->eio->auto_reconn && (s->packet_id & EASY_CONNECT_AUTOCONN)) {
            c->auto_reconn = 1;
        }

        if (s->status != EASY_CONNECT_SEND) {
            client->ref++;
        }

        // 断开指令
    } else if (client && --client->ref == 0) {
        if ((c = client->c)) {
            c->wait_close = 1;
            c->client = NULL;

            /*
             * Potetial bug: what will happen if ref != 0?
             */
            if (c->wait_close && c->pool->ref == 0) {
                EASY_CONNECTION_DESTROY(c, "ref reach 0 in do_client");
            }
        }
        easy_info_log("disconnect succ '%s', %d\n",
                easy_connection_str(c), client->is_ssl);

        easy_hash_del_node(&client->client_list_node);
        easy_array_free(ioth->client_array, client);
    }

error_exit:

    if (s->pool && (s->status & 0x02)) {
        easy_pool_destroy(s->pool);
    } else if (error) {
        easy_session_process(s, 0, errcode);
    }

    return c;
}

int easy_connection_request_done(easy_request_t *r)
{
    easy_connection_t       *c;
    easy_message_t          *m;
    int                     retcode = r->retcode;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    char                    dest_addr[32];

    m = (easy_message_t *)r->ms;
    c = m->c;

    easy_debug_log(" easy_connection_request_done, addr(%s), req(%p), m(%p), m->ref(%ld), ioth->idx(%d), req->pkt_id(%lx), conn(%s), "
            "all_node->prev(%p), all_node->next(%p).",
            easy_inet_addr_to_str(&(c->addr), dest_addr, 32), r, m, m->pool->ref, ioth->idx, (r->packet_id >> 16),
            easy_connection_str(c), r->all_node.prev, r->all_node.next);

    // encode
    if (r->opacket) {
        r->server_send_time = current_time();
        if ((c->handler->encode)(r, r->opacket) != EASY_OK) {
            return EASY_ERROR;
        }

        easy_request_set_cleanup(r, &c->output);
        if (retcode == EASY_AGAIN) { // 当write_socket写完
            r->again_count++;
            easy_list_add_tail(&r->request_list_node, &c->server_session_list);
            ev_io_start(c->loop, &c->write_watcher);
            r->opacket = NULL;
        }
    }

    // retcode
    if (retcode == EASY_OK && r->status != EASY_REQUEST_DONE) {
        // 计算
        easy_debug_log("easy_connection_request_done EASY_REQUEST_DONE, addr(%s), req(%p), m(%p), m->ref(%ld),"
                " ioth->idx(%d), req->pkt_id(%lx), conn(%s), all_node->prev(%p), all_node->next(%p).",
                easy_inet_addr_to_str(&(c->addr), dest_addr, 32), r, m, m->pool->ref, ioth->idx, (r->packet_id >> 16), easy_connection_str(c),
                r->all_node.prev, r->all_node.next);

        r->status = EASY_REQUEST_DONE;
        assert(m->request_list_count > 0);
        m->request_list_count--;
        c->con_summary->done_request_count++;

        // 设置redispatch
        if (c->type == EASY_TYPE_SERVER && EASY_IOTH_SELF->eio->no_redispatch == 0) {
            if (unlikely((c->con_summary->done_request_count & 0xff) == 32)) {
                c->need_redispatch = 1;
            }
        }
    }

    return EASY_OK;
}

static void easy_connection_autoconn(easy_connection_t *c)
{
    int                         fd;
    struct sockaddr_storage     addr;

    c->status = EASY_CONN_CLOSE;

    if (c->client == NULL)
        return;

    memset(&addr, 0, sizeof(addr));
    easy_inet_etoa(&c->addr, &addr);

    if ((fd = socket(addr.ss_family, SOCK_STREAM, 0)) < 0) {
        easy_error_log("socket failure, errno(%d), strerror(%s).\n", errno, strerror(errno));
        goto error_exit;
    }

    easy_socket_non_blocking(fd);

    if (EASY_IOTH_SELF->eio->tcp_nodelay && addr.ss_family != AF_UNIX) {
        easy_ignore(easy_socket_set_tcpopt(fd, TCP_NODELAY, 1));
    }

    size_t sock_addr_len = addr.ss_family != AF_UNIX ? sizeof(addr) : sizeof(struct sockaddr_un);
    if (connect(fd, (struct sockaddr *)&addr, sock_addr_len) < 0) {
        if (errno != EINPROGRESS) {
            easy_error_log("Failed to do connect, conn(%s), errno(%d), strerror(%s).\n",
                    easy_connection_str(c), errno, strerror(errno));
            close(fd);
            return;
        }

        c->status = EASY_CONN_CONNECTING;
    } else {
        c->status = EASY_CONN_OK;
    }

    // self connect self
    c->fd = fd;

    if (easy_connection_checkself(c) == EASY_ERROR) {
        CONN_DESTROY_LOG("checkself");
        goto error_exit;
    }

    // 初始化
    c->conn_has_error = 0;
    ev_io_set(&c->read_watcher, fd, EV_READ);
    ev_io_set(&c->write_watcher, fd, EV_WRITE);

    if (c->status == EASY_CONN_CONNECTING)
        c->event_status = (EASY_EVENT_TIMEOUT | EASY_EVENT_WRITE);
    else
        c->event_status = (EASY_EVENT_TIMEOUT | EASY_EVENT_READ);

    easy_debug_log("reconnect to '%s' start\n", easy_connection_str(c));

    easy_connection_evio_start(c);
    return;
error_exit:
    easy_info_log("Socket closed, fd(%d).", c->fd);
    easy_safe_close(c->fd);
    c->auto_reconn = 0;
    EASY_CONNECTION_DESTROY(c, "autoconn");
}

easy_addr_t* easy_connection_get_local_addr(easy_connection_t* c, easy_addr_t* eaddr)
{
    socklen_t len;
    struct sockaddr_storage addr;
    len = sizeof(addr);
    memset(eaddr, 0, sizeof(*eaddr));

    if (getsockname(c->fd, (struct sockaddr *) &addr, &len) == 0) {
        easy_inet_atoe(&addr, eaddr);
    }
    return eaddr;
}

char *easy_connection_str(easy_connection_t *c)
{
    static __thread char buffer[192];
    char local_addr[64], dest_addr[64];
    easy_addr_t local_eaddr;

    if (!c) {
        return "null";
    }

    easy_connection_get_local_addr(c, &local_eaddr);
    lnprintf(buffer, 192, "%s_%s_%d_%p tp=%d t=%ld-%ld s=%d r=%d io=%ld/%ld sq=%ld",
        easy_inet_addr_to_str(&local_eaddr, local_addr, sizeof(local_addr)),
        easy_inet_addr_to_str(&c->addr, dest_addr, sizeof(dest_addr)), c->fd, c, c->type,
        (int64_t)(1000000LL * c->start_time), (int64_t)(1000000LL * c->last_time),
        c->status, c->doing_request_count, c->recv_bytes, c->send_bytes, c->ack_bytes);
    return buffer;
}

static int easy_connection_process_request(easy_connection_t *c, easy_list_t *list)
{
    easy_request_t          *r, *rn;
    easy_message_t          *m = NULL;
    easy_list_t             request_list;
    int                     ret, cnt = 0;
    int                     max = (EASY_IOTH_SELF->eio->tcp_nodelay ? 4 : 128);

    assert(c->type == EASY_TYPE_SERVER);
    easy_list_movelist(list, &request_list);
    c->rx_request_queue = &request_list;

    easy_list_for_each_entry_safe(r, rn, &request_list, request_list_node) {
        m = (easy_message_t *)r->ms;
        easy_list_del(&r->request_list_node);
        EASY_IOTH_SELF->rx_done_request_count++;

        {
            EASY_STAT_TIME_GUARD(ev_server_process_count, ev_server_process_time);
            ret = (c->handler->process(r));
        }
        if (ret == EASY_ABORT || ret == EASY_ASYNC || ret == EASY_ERROR) {
            goto error_exit;
        } else if (ret != EASY_OK) {
            /*
             * If no exception, ret should always be EASY_AGAIN. The response
             * will be sent back by easy_connection_send_response after the
             * request is handled.
             */
            continue;
        }

        if (easy_connection_request_done(r) == EASY_OK) {
            cnt++;
        }

        // write to socket
        if (cnt >= max) {
            cnt = 0;
            if ((ret = easy_connection_write_socket(c)) == EASY_ABORT) {
                goto error_exit;
            }
        }

        // check request count
        if (m->request_list_count == 0 && m->status != EASY_MESG_READ_AGAIN) {
            easy_message_destroy(m, 1);
        }
    }
    c->rx_request_queue = NULL;

    // 所有的request都有reply了,一起才响应
    if (easy_connection_write_socket(c) == EASY_ABORT) {
        return EASY_ERROR;
    }

    return EASY_OK;
error_exit:
    c->rx_request_queue = NULL;

    if (ret != EASY_ASYNC) {
        easy_list_for_each_entry_safe(r, rn, &request_list, request_list_node) {
            easy_list_del(&r->request_list_node);
        }
        ret = EASY_ERROR;
    }

    return ret;
}

/**
 * 使用reuseport
 */
void easy_connection_reuseport(easy_io_t *eio, easy_listen_t *l, int idx)
{
    char                    buffer[32];
    int                     fd, udp;
    int                     flags = (eio->tcp_defer_accept ? EASY_FLAGS_DEFERACCEPT : 0);

    if (!l->reuseport) {
        return;
    }

    flags |= EASY_FLAGS_SREUSEPORT;
    udp = (l->handler && l->handler->is_udp ? 1 : 0);

    if ((fd = easy_socket_listen(udp, &l->addr, &flags, eio->listen_backlog)) < 0) {
        easy_error_log("easy_socket_listen failure: host=%s\n",
                easy_inet_addr_to_str(&l->addr, buffer, 32));
        return;
    }

    if (easy_atomic_add_return(&l->bind_port_cnt, 1) == eio->io_thread_count) {
        easy_safe_close(l->fd);
        l->fd = fd;
        easy_info_log("easy_socket_listen: host=%s, fd=%d, reuseport=%d",
                easy_inet_addr_to_str(&l->addr, buffer, 32), fd, l->reuseport);
    }

    // 初始化
    if (udp) {
        ev_io_init(&l->read_watcher[idx], easy_connection_on_udpread, fd, EV_READ | EV_CLEANUP);
    } else {
        ev_io_init(&l->read_watcher[idx], easy_connection_on_accept, fd, EV_READ | EV_CLEANUP);
    }
}

/**
 * udp read
 */
void easy_connection_on_udpread(struct ev_loop *loop, ev_io *w, int revents)
{
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    int                     blen, size, n;
    easy_listen_simple_t    *listen;
    struct sockaddr_storage *addr;
    socklen_t               addr_len;
    easy_connection_t       *c;
    easy_message_t          *m;
    easy_request_t          *r;
    easy_pool_t             *pool;
    easy_buf_t              *input;
    void                    *packet;
#ifdef HAVE_RECVMMSG
    int                     i;
    struct iovec            *iovec;
    struct mmsghdr          *hdr;
#endif

    listen = (easy_listen_simple_t *) w->data;
    addr_len = sizeof(struct sockaddr_storage);

    // 让出来给其他的线程
    if (listen->is_simple == 0 && listen->reuseport == 0 && ioth->eio->listen_all == 0)
        easy_switch_listen(listen);

    // 为connection建pool
    blen = 4096;
#ifdef HAVE_RECVMMSG
    size = sizeof(easy_connection_t) + sizeof(easy_message_t) + sizeof(easy_buf_t) + (blen +
            sizeof(struct sockaddr_storage) + sizeof(struct mmsghdr) + sizeof(struct iovec)) * ioth->eio->recv_vlen;
#else
    size = sizeof(easy_connection_t) + sizeof(easy_message_t) + blen + sizeof(easy_buf_t) + sizeof(struct sockaddr_storage);
#endif

    if ((pool = easy_pool_create(size)) == NULL)
        return;

    // 创建easy_connection_t对象
    c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));
    m = (easy_message_t *) easy_pool_calloc(pool, sizeof(easy_message_t));
#ifdef HAVE_RECVMMSG
    input = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t));
    iovec = (struct iovec *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct iovec)));
    hdr = (struct mmsghdr *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct mmsghdr)));
    addr = (struct sockaddr_storage *)easy_pool_calloc(pool, ioth->eio->recv_vlen * (sizeof(struct sockaddr_storage)));
#else
    input = easy_buf_create(pool, blen);
    addr = (struct sockaddr_storage *)easy_pool_calloc(pool, sizeof(struct sockaddr_storage));
#endif

    if (c == NULL || m == NULL || input == NULL || addr == NULL
#ifdef HAVE_RECVMMSG
        || iovec == NULL || hdr == NULL
#endif
       ) {
        easy_error_log("easy_pool_calloc failure\n");
        goto error_exit;
    }

#ifdef HAVE_RECVMMSG

    for (i = 0; i < ioth->eio->recv_vlen; i++) {
        iovec[i].iov_base = (char *)easy_pool_alloc(pool, blen);
        iovec[i].iov_len = blen;
        hdr[i].msg_hdr.msg_iov = &iovec[i];
        hdr[i].msg_hdr.msg_iovlen = 1;
        hdr[i].msg_hdr.msg_name = (struct sockaddr *)(addr + i);
        hdr[i].msg_hdr.msg_namelen = addr_len;
    }

    if (ioth->eio->recv_vlen == 1) {
        n = recvfrom(w->fd, iovec[0].iov_base, blen, 0, (struct sockaddr *)addr, &addr_len);

        if (n <= 0) {
            goto error_exit;
        }

        hdr[0].msg_len = n;
        n = 1;
    } else {
        n = recvmmsg(w->fd, hdr, ioth->eio->recv_vlen, MSG_DONTWAIT, NULL);
    }

#else
    // recvfrom
    n = recvfrom(w->fd, input->last, blen, 0, (struct sockaddr *)addr, &addr_len);
#endif

    if (n <= 0) {
        goto error_exit;
    }

    c->pool = pool;
    c->loop = loop;
    c->handler = listen->handler;
    c->fd = w->fd;
    easy_list_init(&c->output);
    m->c = c;
    m->pool = pool;
    input->args = pool;
    easy_list_init(&input->node);
    m->input = input;

#ifdef HAVE_RECVMMSG

    for (i = 0; i < n; i++) {
        input->pos = iovec[i].iov_base;
        input->last = input->pos + hdr[i].msg_len;

        easy_inet_atoe(addr + i, &c->addr);
        m->user_data = NULL;
#else
    easy_inet_atoe(addr, &c->addr);
    input->last += n;
#endif

        if ((packet = (c->handler->decode)(m)) == NULL) {
            goto error_exit;
        }

        r = (easy_request_t *)easy_pool_calloc(m->pool, sizeof(easy_request_t));

        if (r == NULL) {
            goto error_exit;
        }

#ifdef EASY_DEBUG_DOING
        r->uuid = easy_atomic_add_return(&easy_debug_uuid, 1);
        EASY_PRINT_BT("doing_request_count_inc:%d,c:%s,r:%p,%ld.",
                c->doing_request_count, easy_connection_str(c), r, r->uuid);
#endif

        r->ms = (easy_message_session_t *)m;
        r->ipacket = packet;    //进来的数据包

        // process
        SERVER_PROCESS(c, r);

        // encode
        if (r->opacket) {
            (c->handler->encode)(r, r->opacket);
            if (easy_socket_usend(c, &c->output) < 0) {
                easy_error_log("easy_socket_usend error, conn(%s), err:%d", easy_connection_str(c), errno);
                goto error_exit;
            }
            r->opacket = NULL;
        }

#ifdef HAVE_RECVMMSG
    }

#endif

error_exit:
    easy_pool_destroy(pool);
}


static int easy_connection_sendsocket(easy_connection_t *c)
{
    // 等下次写出去
    if (c->status != EASY_CONN_OK || ev_is_active(&c->write_watcher)) {
        return EASY_OK;
    }

    // 写出到socket
    if (easy_connection_write_socket(c) == EASY_ABORT) {
        EASY_CONNECTION_DESTROY(c, "sendsocket");
        return EASY_ABORT;
    }

    return EASY_OK;
}

/**
 * listen dispatch
 */
static int easy_connection_listen_dispatch(easy_io_t *eio, easy_addr_t addr, easy_listen_t *listen)
{
    int                     ret;
    easy_session_t          *s;

    s = easy_connection_connect_init(NULL, (easy_io_handler_pt *)listen, 0, NULL, 0, NULL);
    s->type = EASY_TYPE_LISTEN;

    // add dispatch.
    if ((ret = easy_client_dispatch(eio, addr, s)) != EASY_OK)
        easy_session_destroy(s);

    return ret;
}

static void easy_connection_listen_watcher(easy_session_t *s)
{
    easy_listen_t           *l = (easy_listen_t *)s->thread_ptr;
    easy_io_thread_t        *ioth = EASY_IOTH_SELF;
    easy_io_t               *eio = ioth->eio;
    char                    buffer[32];

    if (eio->listen_all || eio->io_thread_count == 1 || l->reuseport) {
        easy_connection_reuseport(eio, l, ioth->idx);
        ev_io_start(ioth->loop, &l->read_watcher[ioth->idx]);
    } else {
        ev_timer_start(ioth->loop, &ioth->listen_watcher);
    }

    easy_spin_lock(&eio->lock);
    l->next = eio->listenadd;
    eio->listenadd = l;
    easy_spin_unlock(&eio->lock);

    easy_debug_log("add listen: %s\n", easy_inet_addr_to_str(&l->addr, buffer, 32));
}

int easy_connection_destroy_dispatch(easy_connection_t *c)
{
    easy_info_log("Shutting down socket with SHUT_RD, fd(%d), conn(%s), lbt(%s).\n",
        c->fd, easy_connection_str(c), easy_lbt());
    shutdown(c->fd, SHUT_RD);
    return 0;
}

static int easy_connection_checkself(easy_connection_t *c)
{
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    // self connect self
    if (c->addr.family == AF_INET && getsockname(c->fd, (struct sockaddr *)&addr, &len) == 0) {
        if (addr.sin_port == c->addr.port && addr.sin_addr.s_addr == c->addr.u.addr) {
            easy_error_log("connect to %s failure, self connect self\n", easy_connection_str(c));
            return EASY_ERROR;
        }
    }

    return EASY_OK;
}

static void easy_connection_dump_slow_request(easy_connection_t *c)
{
    easy_message_t *m, *mn;
    easy_request_t *r, *rn;
    char                    btmp[128];
    easy_warn_log("start dump request on connection %s", easy_connection_str(c));
    easy_list_for_each_entry_safe(m, mn, &c->message_list, message_list_node) {
        easy_warn_log("start dump request on message %p", m);
        easy_list_for_each_entry_safe(r, rn, &m->all_list, all_node) {
            easy_warn_log("dump request r->retcode=%d", r->retcode);
            easy_warn_log("dump request r->ipacket=%s", easy_string_tohex(r->ipacket, 32, btmp, 128));
        }
        easy_warn_log("end dump request on message %p", m);
    }
    easy_warn_log("end dump request on connection %s", easy_connection_str(c));
    c->slow_request_dumped = 1;
}

static void easy_session_on_write_success(easy_session_t *s)
{
    if (s) {
        if (unlikely(s->enable_trace)) {
            easy_debug_log("on write success, session=%p", s);
        }
        easy_connection_t *c = s->c;

        if (s->type != EASY_TYPE_KEEPALIVE_SESSION) {
            easy_atomic32_dec(&c->ioth->tx_doing_request_count);
        }
        easy_session_destroy(s);
    }
}

static void easy_connection_cleanup_buffer(easy_buf_t *b, void *args)
{
    easy_pool_t *pool = (easy_pool_t *) args;
    easy_pool_destroy(pool);
}

/**
 * 发送数据
 */
int easy_connection_write_buffer(easy_connection_t *c, const char *data, int len)
{
    easy_buf_t             *b;
    easy_io_thread_t       *ioth;
    easy_pool_t            *pool = NULL;
    easy_message_session_t *ms;

    if (c->status == EASY_CONN_CLOSE) {
        goto error_exit;
    }

    // pool create
    ioth = c->ioth;

    if (len == -1) {
        len = strlen(data);
    }

    if ((pool = easy_pool_create(len + 512)) == NULL) {
        goto error_exit;
    }

    // self thread
    if (ioth == (easy_io_thread_t *)easy_baseth_self) {
        if ((b = easy_buf_create(pool, len)) == NULL) {
            goto error_exit;
        }

        b->last = easy_memcpy(b->last, data, len);
        easy_list_add_tail(&b->node, &c->output);
        easy_buf_set_cleanup(b, easy_connection_cleanup_buffer, pool);
        return easy_connection_write_socket(c);
    } else {
        // easy_message_session_t + easy_buf_t + data
        ms = easy_pool_alloc(pool, sizeof(easy_message_session_t) + sizeof(easy_buf_t) + len);

        if (ms == NULL) {
            goto error_exit;
        }

        b = (easy_buf_t *)(ms + 1);
        memcpy((char *)(b + 1), data, len);
        easy_buf_set_data(pool, b, (char *)(b + 1), len);
        easy_buf_set_cleanup(b, easy_connection_cleanup_buffer, pool);
        ms->type = EASY_TYPE_WBUFFER;
        ms->c = c;
        ms->pool = pool;

        easy_spin_lock(&ioth->thread_lock);
        easy_list_add_tail(&ms->list_node, &ioth->session_list);
        easy_spin_unlock(&ioth->thread_lock);
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    return EASY_OK;
error_exit:

    if (pool) easy_pool_destroy(pool);

    return EASY_ERROR;
}

static void easy_connection_on_pause(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_connection_t *c = (easy_connection_t *)w->data;

    if (c->status == EASY_CONN_OK) {
        ev_io_start(c->loop, &c->read_watcher);
    }
}

int easy_connection_pause(easy_connection_t *c, int ms)
{
    easy_io_thread_t *ioth = c->ioth;

    if (ioth == (easy_io_thread_t *)easy_baseth_self) {
        ev_io_stop(c->loop, &c->read_watcher);

        if (ev_is_active(&c->pause_watcher)) {
            ev_timer_stop(c->loop, &c->pause_watcher);
        }

        ev_timer_set(&c->pause_watcher, ms / 1000.0, 0.0);
        c->pause_watcher.data = c;
        ev_timer_start(c->loop, &c->pause_watcher);
    } else {
        easy_pool_t *pool = easy_pool_create(0);
        if (NULL == pool) {
            easy_error_log("easy_pool_create failed! alloc size(%d), conn(%s)", 0, easy_connection_str(c));
            return EASY_ERROR;
        }
        easy_message_session_t *s = easy_pool_alloc(pool, sizeof(easy_message_session_t));
        if (NULL == s) {
            if (NULL != pool) {
                easy_pool_destroy(pool);
            }
            easy_error_log("easy_pool_alloc failed! alloc size(%d, conn(%s))", sizeof(easy_message_session_t), easy_connection_str(c));
            return EASY_ERROR;
        }
        s->type = EASY_TYPE_PAUSE;
        s->align = ms;
        s->c = c;
        s->pool = pool;

        easy_spin_lock(&ioth->thread_lock);
        easy_list_add_tail(&s->list_node, &ioth->session_list);
        easy_spin_unlock(&ioth->thread_lock);
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    return EASY_OK;
}
