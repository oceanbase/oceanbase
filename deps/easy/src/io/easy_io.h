#ifndef EASY_IO_H_
#define EASY_IO_H_

#include "easy_define.h"

#include <unistd.h>
#include <pthread.h>
#include "io/easy_io_struct.h"
#include "io/easy_log.h"
#include "io/easy_summary.h"
/**
 * IO文件头
 */

EASY_CPP_START

// 接口函数
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_io_t
extern easy_io_t           *easy_eio_create(easy_io_t *eio, int io_thread_count);
extern int                  easy_eio_start(easy_io_t *eio);
extern int                  easy_eio_wait(easy_io_t *eio);
extern int                  easy_eio_stop(easy_io_t *eio);
extern int                  easy_eio_shutdown(easy_io_t *eio);
extern void                 easy_eio_destroy(easy_io_t *eio);
extern void                 easy_eio_set_keepalive(easy_io_t *eio, int keepalive_enabled);
extern easy_ratelimitor_t*  get_global_ratelimitor();
extern void                 easy_eio_set_region_max_bw(easy_io_t *eio, int64_t max_bw, const char *region);
extern int                  easy_eio_get_region_bw(easy_io_t *eio, const char* region, int64_t *bw, int64_t *max_bw);
extern void                 easy_eio_s2r_map_changed(easy_io_t *eio);
extern int                  easy_eio_get_region_rlmtr(easy_addr_t *addr, easy_region_ratelimitor_t **rlmtr);
extern int                  easy_eio_is_region_speeding(easy_region_ratelimitor_t *region_rlmtr, int do_stop);
extern void                 easy_eio_record_region_speed(easy_region_ratelimitor_t *region_rlmtr);
extern int                  easy_eio_set_rlmtr_thread(easy_io_t *eio);
extern int                  easy_eio_set_s2r_map_cb(easy_io_t *eio, easy_io_update_s2r_map_pt *update_s2r_map_cb,
                                    void *get_region_id_args);
extern void                 easy_eio_set_s2r_map(easy_io_t *eio, easy_addr_t *addr, char *region);
extern void                 easy_eio_set_ratelimit_stat_period(easy_io_t *eio, int64_t stat_period);
extern void                 easy_eio_set_ratelimit_enable(easy_io_t *eio, int easy_ratelimit_enabled);
extern void                 easy_eio_set_uthread_start(easy_io_t *eio, easy_io_uthread_start_pt *on_utstart, void *args);
extern struct ev_loop      *easy_eio_thread_loop(easy_io_t *eio, int index);
extern void                 easy_eio_stat_watcher_start(easy_io_t *eio, ev_timer *stat_watcher,
        double interval, easy_io_stat_t *iostat, easy_io_stat_process_pt *process);

///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_connection_t
extern easy_listen_t       *easy_connection_add_listen(easy_io_t *eio, const char *host, int port, easy_io_handler_pt *handler);
extern easy_connection_t   *easy_connection_connect_thread(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags);
extern int                  easy_connection_connect(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int conn_timeout, void *args, int flags);
extern int                  easy_connection_disconnect(easy_io_t *eio, easy_addr_t addr);
extern int                  easy_connection_disconnect_thread(easy_io_t *eio, easy_addr_t addr);
extern int                  easy_connection_send_session(easy_connection_t *c, easy_session_t *s);
extern int                  easy_connection_send_session_data(easy_connection_t *c, easy_session_t *s);
extern char                 *easy_connection_str(easy_connection_t *c);
extern int                  easy_connection_dispatch_to_thread(easy_connection_t *c, easy_io_thread_t *ioth);

extern easy_session_t       *easy_connection_connect_init(easy_session_t *s, easy_io_handler_pt *handler,
        int conn_timeout, void *args, int flags, char *servername);
extern easy_connection_t    *easy_connection_connect_thread_ex(easy_addr_t addr, easy_session_t *s);
extern int                  easy_connection_connect_ex(easy_io_t *eio, easy_addr_t addr, easy_session_t *s);
extern int                  easy_connection_destroy_dispatch(easy_connection_t *c);
extern easy_listen_t        *easy_add_listen(easy_io_t *eio, const char *host, int port,
        easy_io_handler_pt *handler, void *args);
extern easy_listen_t *easy_add_pipefd_listen_for_connection(easy_io_t *eio, easy_io_handler_pt *handler, easy_io_threads_pipefd_pool_t *pipefd_pool);
extern void easy_connection_on_accept_pipefd(struct ev_loop *loop, ev_io *w, int revents);

extern easy_listen_t        *easy_add_listen_addr(easy_io_t *eio, easy_addr_t addr,
        easy_io_handler_pt *handler, int udp, void *args);
extern int                  easy_connection_write_buffer(easy_connection_t *c, const char *data, int len);
extern int                  easy_connection_pause(easy_connection_t *c, int ms);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_session
extern easy_session_t      *easy_session_create(int64_t size);
extern void                 easy_session_destroy(void *s);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_client uthread
extern int                  easy_client_uthread_wait_conn(easy_connection_t *c);
extern int                  easy_client_uthread_wait_session(easy_session_t *s);
extern void                 easy_client_uthread_set_handler(easy_io_handler_pt *handler);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_client_wait
extern void                 easy_client_wait_init(easy_client_wait_t *w);
extern void                 easy_client_wait(easy_client_wait_t *w, int count);
extern void                 easy_client_wait_cleanup(easy_client_wait_t *w);
extern void                 easy_client_wait_wakeup(easy_client_wait_t *w);
extern void                 easy_client_wait_wakeup_request(easy_request_t *r);
extern int                  easy_client_wait_process(easy_request_t *r);

extern int                  easy_client_wait_batch_process(easy_message_t *m);
extern void                *easy_client_send(easy_io_t *eio, easy_addr_t addr, easy_session_t *s);
extern int                  easy_client_dispatch(easy_io_t *eio, easy_addr_t addr, easy_session_t *s);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_request
extern int                  easy_request_do_reply(easy_request_t *r);
extern easy_thread_pool_t  *easy_thread_pool_create(easy_io_t *eio, int cnt, easy_request_process_pt *cb, void *args);
extern easy_thread_pool_t  *easy_thread_pool_create_ex(easy_io_t *eio, int cnt, easy_baseth_on_start_pt *start,
        easy_request_process_pt *cb, void *args);
extern int                  easy_thread_pool_push(easy_thread_pool_t *tp, easy_request_t *r, uint64_t hv);
extern int                  easy_thread_pool_push_message(easy_thread_pool_t *tp, easy_message_t *m, uint64_t hv);
extern int                  easy_thread_pool_push_session(easy_thread_pool_t *tp, easy_session_t *s, uint64_t hv);
extern void                 easy_request_addbuf(easy_request_t *r, easy_buf_t *b);
extern void                 easy_request_addbuf_list(easy_request_t *r, easy_list_t *list);
extern void                 easy_request_wakeup(easy_request_t *r);
extern void                 easy_request_sleeping(easy_request_t *r);
extern void                 easy_request_sleepless(easy_request_t *r);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_file
extern easy_file_task_t     *easy_file_task_create(easy_request_t *r, int fd, int bufsize);
extern void                 easy_file_task_set(easy_file_task_t *ft, char *buffer, int64_t offset, int64_t bufsize, void *args);
extern void                 easy_file_task_reset(easy_file_task_t *ft, int type);
///////////////////////////////////////////////////////////////////////////////////////////////////
// easy_ssl
extern int                  easy_ssl_init();
extern int                  easy_ssl_cleanup();
extern easy_ssl_t          *easy_ssl_config_load(char *filename);
//extern easy_ssl_t          *easy_ssl_config_load_for_mysql(const char *ssl_ca, const char *ssl_cert, const char *ssl_key);
extern easy_ssl_ctx_t      *easy_ssl_ctx_load(easy_pool_t *pool, const char *ssl_ca,
                                              const char *ssl_cert, const char *ssl_key,
                                              const char *ssl_enc_cert, const char *ssl_enc_key,
                                              const int is_from_file, const int is_babassl,
                                              const int is_server);
extern int                  easy_ssl_ob_config_load(easy_io_t *eio, const char *ssl_ca,
                                                    const char *ssl_cert, const char *ssl_key,
                                                    const char *ssl_enc_cert, const char *ssl_enc_key,
                                                    const int is_from_file, const int is_babassl,
                                                    const int used_for_rpc);
extern int                  easy_ssl_ob_config_check(const char *ssl_ca,
                                                     const char *ssl_cert, const char *ssl_key,
                                                     const char *ssl_enc_cert, const char *ssl_enc_key,
                                                     const int is_from_file, const int is_babassl);
extern int                  easy_ssl_config_destroy(easy_ssl_t *ssl);
extern int easy_ssl_client_authenticate(easy_ssl_t *ssl, SSL *conn, const void *host, int len);

///////////////////////////////////////////////////////////////////////////////////////////////////
// define
#define EASY_IOTH_SELF ((easy_io_thread_t*) easy_baseth_self)
#define easy_session_set_args(s, a)      (s)->r.args = (void*)a
#define easy_session_set_handler(s,h,ua) (s)->thread_ptr = (void *)h; (s)->status = EASY_CONNECT_SEND; (s)->r.user_data = (void *)ua;
#define easy_session_set_timeout(s, t)   (s)->timeout = t
#define easy_request_set_wobj(r, w)      (r)->request_list_node.prev = (easy_list_t *)w
#define easy_session_set_wobj(s, w)      easy_request_set_wobj(&((s)->r), w)
#define easy_session_set_request(s, p, t, a)                \
    (s)->r.opacket = (void*) p;                             \
    (s)->r.args = (void*)a; (s)->timeout = t;
#define easy_session_packet_create(type, s, size)           \
    ((s = easy_session_create(size + sizeof(type))) ? ({    \
        memset(&((s)->data[0]), 0, sizeof(type));           \
        (s)->r.opacket = &((s)->data[0]);                   \
        (type*) &((s)->data[0]);}) : NULL)
#define easy_session_class_create(type, s, ...)             \
    ((s = easy_session_create(sizeof(type))) ? ({           \
        new(&((s)->data[0]))type(__VA_ARGS__);              \
        (s)->r.opacket = &((s)->data[0]);                   \
        (type*) &((s)->data[0]);}) : NULL)

#ifdef EASY_MULTIPLICITY
#define easy_io_create(eio, cnt)                    easy_eio_create(eio, cnt)
#define easy_io_start(eio)                          easy_eio_start(eio)
#define easy_io_wait(eio)                           easy_eio_wait(eio)
#define easy_io_stop(eio)                           easy_eio_stop(eio)
#define easy_io_shutdown(eio)                       easy_eio_shutdown(eio)
#define easy_io_destroy(eio)                        easy_eio_destroy(eio)
#define easy_io_thread_loop(a,b)                    easy_eio_thread_loop(a,b)
#define easy_io_set_uthread_start(eio,start,args)   easy_eio_set_uthread_start(eio,start,args)
#define easy_io_stat_watcher_start(a1,a2,a3,a4,a5)  easy_eio_stat_watcher_start(a1,a2,a3,a4,a5)
#define easy_io_add_listen(eio,host,port,handler)   easy_connection_add_listen(eio,host,port,handler)
#define easy_io_connect(eio,addr,handler,t,args)    easy_connection_connect(eio,addr,handler,t,args,0)
#define easy_io_connect_thread(eio,addr,h,t,args)   easy_connection_connect_thread(eio,addr,h,t,args,0)
#define easy_io_disconnect(eio,addr)                easy_connection_disconnect(eio,addr)
#define easy_io_disconnect_thread(eio,addr)         easy_connection_disconnect_thread(eio,addr)
#define easy_request_thread_create(eio,cnt,cb,args) easy_thread_pool_create(eio,cnt,cb,args)
#define easy_io_dispatch(eio,addr,s)                easy_client_dispatch(eio,addr,s)
#define easy_io_send(eio,addr,s)                    easy_client_send(eio,addr,s);
#else
#define easy_io_create(cnt)                         easy_eio_create(&easy_io_var, cnt)
#define easy_io_start()                             easy_eio_start(&easy_io_var)
#define easy_io_wait()                              easy_eio_wait(&easy_io_var)
#define easy_io_stop()                              easy_eio_stop(&easy_io_var)
#define easy_io_shutdown()                          easy_eio_shutdown(&easy_io_var)
#define easy_io_destroy()                           easy_eio_destroy(&easy_io_var)
#define easy_io_thread_loop(a)                      easy_eio_thread_loop(&easy_io_var,a)
#define easy_io_set_uthread_start(start,args)       easy_eio_set_uthread_start(&easy_io_var,start,args)
#define easy_io_stat_watcher_start(a1,a2,a3,a4)     easy_eio_stat_watcher_start(&easy_io_var,a1,a2,a3,a4)
#define easy_io_add_listen(host,port,handler)       easy_connection_add_listen(&easy_io_var,host,port,handler)
#define easy_io_connect(addr,handler,t,args)        easy_connection_connect(&easy_io_var,addr,handler,t,args,0)
#define easy_io_connect_thread(addr,h,t,args)       easy_connection_connect_thread(&easy_io_var,addr,h,t,args,0)
#define easy_io_disconnect(addr)                    easy_connection_disconnect(&easy_io_var,addr)
#define easy_io_disconnect_thread(addr)             easy_connection_disconnect_thread(&easy_io_var,addr)
#define easy_request_thread_create(cnt, cb, args)   easy_thread_pool_create(&easy_io_var, cnt, cb, args)
#define easy_io_dispatch(addr,s)                    easy_client_dispatch(&easy_io_var,addr,s)
#define easy_io_send(addr,s)                        easy_client_send(&easy_io_var,addr,s);
#endif

// 变量
extern __thread easy_baseth_t *easy_baseth_self;
extern easy_io_t        easy_io_var;
extern struct easy_tx_session_pool_t global_tx_session_pool;

EASY_CPP_END

#endif
