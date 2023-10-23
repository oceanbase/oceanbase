#include "io/easy_io.h"
#include "io/easy_client.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"
#include "io/easy_ssl.h"

static int easy_client_uthread_wakeup_conn(easy_connection_t *c);
static int easy_client_uthread_wakeup_session(easy_request_t *r);

int ob_pthread_cond_wait(pthread_cond_t *__restrict __cond,
                         pthread_mutex_t *__restrict __mutex);
/**
 * 把session发送到addr上
 */
int easy_client_dispatch(easy_io_t *eio, easy_addr_t addr, easy_session_t *s)
{
    easy_io_thread_t        *ioth;
    uint64_t                index;
    int                     ret;
    int                     issend;

    if (unlikely(!eio->started)) {
        easy_warn_log("easy_io_dispatch is failure: easy not started\n");
        return EASY_ERROR;
    } else if (unlikely(eio->stoped)) {
        easy_error_log("easy_io_dispatch is failure: easy stopped\n");
        return EASY_ERROR;
    }

    index = (addr.cidx < 256 ? addr.cidx : easy_hash_code(&addr, sizeof(easy_addr_t), 7));
    ioth = (easy_io_thread_t *)easy_thread_pool_hash(eio->io_thread_pool, index);
    issend = (s->status == 0 || s->status == EASY_CONNECT_SEND);

    if (unlikely(ioth->eio->checkdrc == 0 && ioth->tx_doing_request_count >= EASY_IOTH_DOING_REQ_CNT && issend)) {
        static int              lastlog = 0;

        if (lastlog != time(NULL)) {
            lastlog = time(NULL);
            easy_error_log("ioth->tx_doing_request_count: %d, EASY_IOTH_DOING_REQ_CNT: %d\n",
                           ioth->tx_doing_request_count, EASY_IOTH_DOING_REQ_CNT);
        }

        return EASY_ERROR;
    }

    s->async = 1;
    s->addr = addr;

    if (issend) {
        easy_atomic32_inc(&ioth->tx_doing_request_count);
    }

    char                    buffer[32];
    easy_debug_log("send to %s, status=%d", easy_inet_addr_to_str(&s->addr, buffer, 32), s->status);

    // dispatch
    ret = EASY_OK;
    easy_spin_lock(&ioth->thread_lock);

    if (likely(eio->stoped == 0)) {
        easy_list_add_tail(&s->session_list_node, &ioth->session_list);
    } else {
        easy_error_log("eio stoped.");
        ret = EASY_ERROR;
    }

    easy_spin_unlock(&ioth->thread_lock);

    if (ret == EASY_OK) {
        ev_async_send(ioth->loop, &ioth->thread_watcher);
    }

    return ret;
}

/**
 * thread发送packet的时候用, 同步, 等待返回结果
 */
void *easy_client_send(easy_io_t *eio, easy_addr_t addr, easy_session_t *s)
{
    int                     ret;
    easy_client_wait_t      wobj;

    easy_client_wait_init(&wobj);
    easy_session_set_wobj(s, &wobj);
    s->callback = easy_client_wait_process;

    if ((ret = easy_client_dispatch(eio, addr, s)) == EASY_ERROR) {
        s->error = EASY_DISPATCH_ERROR;
        easy_warn_log("easy_session_dispatch failed: %d\n", ret);
        return NULL;
    }
//    easy_info_log("easy_client_send 1, c = %s, %p, %p, %p\n", easy_connection_str(s->c), &wobj.cond, s, s->callback);

    easy_client_wait(&wobj, 1);
    pthread_cond_destroy(&wobj.cond);
    pthread_mutex_destroy(&wobj.mutex);
    return s->r.ipacket;
}

// init
void easy_client_wait_init(easy_client_wait_t *w)
{
    w->done_count = 0;
    w->status = EASY_CONN_OK;
    easy_list_init(&w->next_list);
    easy_list_init(&w->session_list);
    pthread_mutex_init(&w->mutex, NULL);
    pthread_cond_init(&w->cond, NULL);
}

void easy_client_wait_cleanup(easy_client_wait_t *w)
{
    easy_session_t          *s, *s2;
    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->mutex);
    easy_list_for_each_entry_safe(s, s2, &w->session_list, session_list_node) {
        easy_session_destroy(s);
    }
}
void easy_client_wait_wakeup(easy_client_wait_t *w)
{
    pthread_mutex_lock(&w->mutex);
    w->done_count ++;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->mutex);
}
void easy_client_wait_wakeup_request(easy_request_t *r)
{
    if (r->client_wait) {
        easy_atomic_inc(&r->ms->c->pool->ref);
        easy_atomic_inc(&r->ms->pool->ref);
        easy_client_wait_wakeup(r->client_wait);
    }
}
void easy_client_wait(easy_client_wait_t *w, int count)
{
    pthread_mutex_lock(&w->mutex);

    while (w->done_count < count) {
        ob_pthread_cond_wait(&w->cond, &w->mutex);
    }

    pthread_mutex_unlock(&w->mutex);

    if (easy_list_empty(&w->next_list))
        return;

    // next
    easy_list_t             *list = &w->next_list;
    easy_session_t          *s, *sn;
    int                     cnt = 0;

    easy_list_for_each_entry_safe(s, sn, list, session_list_node) {
        w = (easy_client_wait_t *)s->r.request_list_node.prev;

        easy_list_del(&s->session_list_node);
        easy_list_add_tail(&s->session_list_node, &w->session_list);

        if (++ cnt >= 2) {
            easy_list_movelist(list, &w->next_list);
            easy_client_wait_wakeup(w);
            break;
        } else {
            easy_client_wait_wakeup(w);
        }
    }
}

int easy_client_wait_process(easy_request_t *r)
{
    easy_client_wait_t      *w = (easy_client_wait_t *)r->request_list_node.prev;
    easy_session_t          *s = (easy_session_t *)r->ms;
//    easy_info_log("easy_client_wait_process, c = %s, %p, %p\n", easy_connection_str(s->c), s, &w->cond);

    pthread_mutex_lock(&w->mutex);
    easy_list_add_tail(&s->session_list_node, &w->session_list);
    w->done_count ++;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->mutex);

    return EASY_OK;
}

int easy_client_wait_batch_process(easy_message_t *m)
{
    easy_list_t             *list = (easy_list_t *) m;
    easy_session_t          *s;
    easy_client_wait_t      *w;

    s = easy_list_get_first(list, easy_session_t, session_list_node);
    w = (easy_client_wait_t *)s->r.request_list_node.prev;

    easy_list_del(&s->session_list_node);
    easy_list_add_tail(&s->session_list_node, &w->session_list);

    easy_list_movelist(list, &w->next_list);
    easy_client_wait_wakeup(w);
    return EASY_OK;
}

/*
int easy_client_wait_on_connect(easy_connection_t *c)
{
    easy_client_wait_t      *w;

    if ((w = (easy_client_wait_t *)c->user_data))
        easy_client_wait_wakeup(w);

    return EASY_OK;
}
*/

// add addr
int easy_client_list_add(easy_hash_t *table, easy_addr_t *addr, easy_hash_list_t *list)
{
    uint64_t                n;
    easy_hash_list_t        *first;

    n = easy_hash_code(addr, sizeof(easy_addr_t), 5);
    n &= table->mask;

    // init
    list->key = (long)(void *)addr;
    table->count ++;
    table->seqno ++;

    // add to list
    first = table->buckets[n];
    list->next = first;

    if (first) first->pprev = &list->next;

    table->buckets[n] = (easy_hash_list_t *)list;
    list->pprev = &(table->buckets[n]);

    return EASY_OK;
}

void *easy_client_list_find(easy_hash_t *table, easy_addr_t *addr)
{
    uint64_t                n;
    int                     lookup = 0;
    easy_hash_list_t        *list;
    easy_addr_t             *baddr;

    n = easy_hash_code(addr, sizeof(easy_addr_t), 5);
    n &= table->mask;
    list = table->buckets[n];

    // foreach
    while (list) {
        baddr = (easy_addr_t *)(long)list->key;

        if (memcmp(baddr, addr, sizeof(easy_addr_t)) == 0) {
            if (lookup > 100) easy_warn_log("lookup: %d", lookup);

            return ((char *)list - table->offset);
        }

        list = list->next;
        lookup ++;
    }

    if (lookup > 100) easy_warn_log("lookup: %d", lookup);

    return NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// uthread wait;
int easy_client_uthread_wait_conn(easy_connection_t *c)
{
    if (c->status == EASY_CONN_OK)
        return EASY_OK;

    if ((c->uthread = easy_uthread_current()) == NULL) {
        return EASY_ERROR;
    } else {
        easy_uthread_switch();
        return easy_uthread_get_errcode();
    }
}

int easy_client_uthread_wait_session(easy_session_t *s)
{
    if ((s->thread_ptr = easy_uthread_current()) == NULL) {
        return EASY_ERROR;
    } else {
        easy_uthread_switch();
        return easy_uthread_get_errcode();
    }
}

void easy_client_uthread_set_handler(easy_io_handler_pt *handler)
{
    handler->on_connect = easy_client_uthread_wakeup_conn;
    handler->on_disconnect = easy_client_uthread_wakeup_conn;
    handler->process = easy_client_uthread_wakeup_session;
    handler->is_uthread = 1;
}
// io_handler
static int easy_client_uthread_wakeup_conn(easy_connection_t *c)
{
    if (c->uthread) {
        easy_uthread_ready(c->uthread);

        easy_uthread_set_errcode(c->uthread, (c->conn_has_error ? EASY_ERROR : EASY_OK));
        c->uthread = NULL;
    }

    return EASY_OK;
}
static int easy_client_uthread_wakeup_session(easy_request_t *r)
{
    easy_session_t          *s;
    easy_uthread_t          *uth;

    s = (easy_session_t *) r->ms;

    if (s->type != EASY_TYPE_SESSION) {
        easy_fatal_log("session error, r->ms->type: %d\n", s->type);
        return EASY_ERROR;
    }

    if (s->thread_ptr) {
        uth = (easy_uthread_t *)s->thread_ptr;
        easy_uthread_ready(uth);
        easy_uthread_set_errcode(uth, (s->c->conn_has_error ? EASY_ERROR : EASY_OK));
        s->thread_ptr = NULL;
    }

    return EASY_OK;
}

