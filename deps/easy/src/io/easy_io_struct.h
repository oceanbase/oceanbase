#ifndef EASY_IO_STRUCT_H_
#define EASY_IO_STRUCT_H_

#include "easy_define.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>

/**
 * IO结构定义
 */

EASY_CPP_START

#define EV_STANDALONE    1
#define EV_USE_MONOTONIC 0
#include "io/ev.h"
#include "util/easy_pool.h"
#include "util/easy_buf.h"
#include "easy_list.h"
#include "easy_atomic.h"
#include "util/easy_hash.h"
#include "thread/easy_uthread.h"
#include "util/easy_inet.h"
#include "util/easy_array.h"
#include "util/easy_util.h"

#ifdef EASY_DEBUG_DOING
extern easy_atomic_t easy_debug_uuid;
#endif
///// define
#define EASY_MAX_THREAD_CNT         64
#define EASY_IOTH_DOING_REQ_CNT     65536
#define EASY_CONN_DOING_REQ_CNT     65536
#define EASY_WARN_LOG_INTERVAL      100
#define EASY_MSS                    1024
#ifndef EASY_MAX_CLIENT_CNT
#define EASY_MAX_CLIENT_CNT         65536
#endif

#define EASY_EVENT_READ             1
#define EASY_EVENT_WRITE            2
#define EASY_EVENT_TIMEOUT          4

#define EASY_TYPE_SERVER            0
#define EASY_TYPE_CLIENT            1

#define EASY_CONN_OK                0
#define EASY_CONN_CONNECTING        1
#define EASY_CONN_AUTO_CONN         2
#define EASY_CONN_CLOSE             3
#define EASY_CONN_CLOSE_BY_PEER     4

#define EASY_CLIENT_DEFAULT_TIMEOUT 4000
#define EASY_FIRST_MSGLEN           1024
#define EASY_IO_BUFFER_SIZE         (1024 * 32 - sizeof(easy_pool_t))
#define EASY_MESG_READ_AGAIN        1
#define EASY_MESG_WRITE_AGAIN       2
#define EASY_MESG_DESTROY           3
#define EASY_MESG_SKIP              4
#define EASY_REQUEST_DONE           1

#define EASY_IOV_MAX                256
#define EASY_IOV_SIZE               262144

#define EASY_FILE_READ              1
#define EASY_FILE_WRITE             2
#define EASY_FILE_SENDFILE          3
#define EASY_FILE_WILLNEED          4

// summary
#define EASY_SUMMARY_CNT            64
#define EASY_SUMMARY_LENGTH_BIT     10
#define EASY_SUMMARY_LENGTH         (1<<EASY_SUMMARY_LENGTH_BIT)
#define EASY_SUMMARY_LENGTH_MASK    (EASY_SUMMARY_LENGTH - 1)

#define EASY_DISCONNECT_ADDR        0x02
#define EASY_CONNECT_ADDR           0x03
#define EASY_CONNECT_SEND           0x05

#define EASY_CONNECT_AUTOCONN       0x01
#define EASY_CONNECT_SSL            0x02
#define EASY_CONNECT_SSL_OPT_TEST   0x04
#define EASY_CONNECT_SSL_OB         0x08

enum {
    LEGACY_MAGIC_VERSION = 0,
    CURENT_MAGIC_VERSION = 1,
    MIN_MAGIC_VERSION_KEEPALIVE = 1,
};

enum {
    MAX_LOG_RETRY_COUNT = 10,
    MAX_REQUEST_TIMEOUT_WARN_COUNT = 4,
};

enum {
    EASY_HEADER_SIZE        = 16,
    RPC_HEADER_SIZE         = 112,
    RPC_COST_TIME_SIZE      = 40,
    RPC_SEQ_NO_SIZE         = 8,
    EASY_TOTAL_HEADER_SIZE  = EASY_HEADER_SIZE + RPC_HEADER_SIZE + RPC_COST_TIME_SIZE + RPC_SEQ_NO_SIZE,
};

enum {
    RL_MAX_REGION_COUNT = 32,
    RL_MAX_REGION_NAME_LEN = 128,
    RL_RECORD_RING_SIZE = 8,
    RL_RECORD_SLOTS = 8 * 8,
};

enum {
    EASY_TYPE_MESSAGE           = 1,
    EASY_TYPE_SESSION           = 2,
    EASY_TYPE_KEEPALIVE_SESSION = 3,
    EASY_TYPE_RL_SESSION        = 4, /* Session with RateLimit enabled. */
    EASY_TYPE_WBUFFER           = 100,
    EASY_TYPE_PAUSE             = 101,
    EASY_TYPE_LISTEN            = 102,
};

enum {
    EASY_REQ_TRACE_BT_SIZE      = 256,
};

enum {
    EASY_REQQUEST_TYPE_RPC      = 0,
    EASY_REQQUEST_TYPE_SQL      = 1,
};

// async + spinlock
#define EASY_BASETH_DEFINE                          \
    easy_baseth_on_start_pt         *on_start;      \
    void                            *tid;           \
    int                             idx, iot;       \
    struct ev_loop                  *loop;          \
    ev_tstamp                       lastrun;        \
    ev_async                        thread_watcher; \
    easy_atomic_t                   thread_lock;    \
    easy_list_t                     user_list;      \
    easy_baseth_on_process_pt       *user_process;  \
    ev_timer                        user_timer;     \
    easy_io_t                       *eio;

///// typedef
typedef struct easy_io_thread_t easy_io_thread_t;
typedef struct easy_request_thread_t easy_request_thread_t;
typedef struct easy_message_t easy_message_t;
typedef struct easy_request_t easy_request_t;
typedef struct easy_connection_t easy_connection_t;
typedef struct easy_message_session_t easy_message_session_t;
typedef struct easy_session_t easy_session_t;
typedef struct easy_listen_simple_t easy_listen_simple_t;
typedef struct easy_listen_t easy_listen_t;
typedef struct easy_client_t easy_client_t;
typedef struct easy_io_t easy_io_t;
typedef struct easy_io_handler_pt easy_io_handler_pt;
typedef struct easy_io_stat_t easy_io_stat_t;
typedef struct easy_baseth_t easy_baseth_t;
typedef struct easy_thread_pool_t easy_thread_pool_t;
typedef struct easy_client_wait_t easy_client_wait_t;
typedef struct easy_file_task_t easy_file_task_t;
typedef struct easy_summary_node_t easy_summary_node_t;
typedef struct easy_summary_t easy_summary_t;
typedef struct easy_ssl_t easy_ssl_t;
typedef struct easy_ssl_ctx_t easy_ssl_ctx_t;
typedef struct easy_ssl_connection_t easy_ssl_connection_t;
typedef struct easy_ratelimitor_t        easy_ratelimitor_t;
typedef struct easy_ratelimit_record_t   easy_ratelimit_record_t;
typedef struct easy_region_ratelimitor_t easy_region_ratelimitor_t;
typedef struct easy_region_ratelimitor_hmap_node_t easy_region_ratelimitor_hmap_node_t;
typedef struct easy_io_threads_pipefd_pool_t easy_io_threads_pipefd_pool_t;
typedef int (easy_io_process_pt)(easy_request_t *r);
typedef int (easy_io_cleanup_pt)(easy_request_t *r, void *apacket);
typedef void (easy_io_write_success_pt)(easy_session_t *s);
typedef int (easy_request_process_pt)(easy_request_t *r, void *args);
typedef void (easy_io_stat_process_pt)(easy_io_stat_t *iostat);
typedef void (easy_io_uthread_start_pt)(void *args);
typedef void *(easy_baseth_on_start_pt)(void *args);
typedef void (easy_baseth_on_process_pt)(easy_baseth_t *th);
typedef void (easy_baseth_on_wakeup_pt)(struct ev_loop *loop, ev_async *w, int revents);
typedef ssize_t (easy_read_pt)(easy_connection_t *c, char *buf, size_t size, int *pending);
typedef ssize_t (easy_write_pt)(easy_connection_t *c, easy_list_t *l);
typedef void (easy_ssl_schandler_pt)(easy_connection_t *c);
typedef int (easy_io_update_s2r_map_pt)(void *args);


struct easy_io_handler_pt {
    void                    *(*decode)(easy_message_t *m);
    int                     (*encode)(easy_request_t *r, void *packet);
    easy_io_process_pt      *process;
    int                     (*batch_process)(easy_message_t *m);
    easy_io_cleanup_pt      *cleanup;
    uint64_t                (*get_packet_id)(easy_connection_t *c, void *packet);
    int                     (*on_connect)(easy_connection_t *c);
    int                     (*on_disconnect)(easy_connection_t *c);
    int                     (*new_packet)(easy_connection_t *c);
    int                     (*new_keepalive_packet)(easy_connection_t *c);
    int                     (*on_idle)(easy_connection_t *c);
    int                     (*set_data)(easy_request_t *r, const char *data, int len);
    void                    (*send_buf_done)(easy_request_t *r);
    void                    (*sending_data)(easy_connection_t *c);
    int                     (*send_data_done)(easy_connection_t *c);
    int                     (*on_redispatch)(easy_connection_t *c);
    int                     (*on_close)(easy_connection_t *c);
    void                    (*set_trace_info)(easy_request_t *r, void *packet);
    void                    *user_data, *user_data2;
    int8_t                  is_uthread, is_ssl, is_udp;
    int8_t                  is_ssl_opt;    //if is_ssl_opt is true, create ssl connection_t after first request, check whether need ssl
    //otherthan create ssl connection_t after connect succ

};


// 处理IO的线程
struct easy_io_thread_t {
    EASY_BASETH_DEFINE

    // queue
    easy_list_t             conn_list;
    easy_list_t             session_list;
    easy_list_t             request_list;

    // listen watcher
    ev_timer                listen_watcher;
    easy_io_uthread_start_pt *on_utstart;
    void                    *ut_args;

    // client list
    easy_hash_t             *client_list;
    easy_array_t            *client_array;

    // connected list
    easy_list_t             connected_list;
    easy_atomic32_t         tx_doing_request_count;
    easy_atomic32_t         rx_doing_request_count;
    uint64_t                tx_done_request_count;
    uint64_t                rx_done_request_count;
    easy_atomic32_t         tx_conn_count;
    easy_atomic32_t         rx_conn_count;
};

// 处理任务的线程
struct easy_request_thread_t {
    EASY_BASETH_DEFINE

    // queue
    int                     task_list_count;
    easy_list_t             task_list;
    easy_list_t             session_list;

    easy_request_process_pt *process;
    void                    *args;
};

// 保存client
struct easy_client_t {
    easy_addr_t             addr;
    easy_connection_t       *c;
    easy_io_handler_pt      *handler;
    SSL_SESSION             *ssl_session;
    easy_hash_list_t        client_list_node;
    void                    *user_data;
    int                     ref;
    uint32_t                timeout         : 31;
    uint32_t                is_ssl          : 1;
    char                    *server_name;
};

enum SSLStateMachine {
    SSM_NONE = 0,
    SSM_BEFORE_FIRST_PKT = 1,
    SSM_AFTER_FIRST_PKT = 2,
    SSM_USE_SSL = 3,
};

struct easy_ratelimit_record_t {
    cycles_t                cycles;
    uint64_t                rx_bytes;
};

struct easy_region_ratelimitor_t {
    int region_id;
    char region_name[RL_MAX_REGION_NAME_LEN];
    easy_hash_list_t hash_list_node;
    easy_list_t region_rlmtr_list_node;
    easy_list_t ready_queue_node;
    easy_list_t fg_rsp_queue; /* Todo: init the queues.*/
    easy_list_t fg_req_queue;
    easy_list_t bg_rsp_queue;
    easy_list_t bg_req_queue;
    ev_timer tx_trigger; // Todo: init
    int tx_trigger_armed;
    easy_atomic_t tx_bytes;
    uint64_t max_bw;
    uint64_t location_version;
    uint64_t cur_location_version;
    uint64_t record_count;
    uint64_t received_req_count;
    uint64_t dispatched_req_count;
    easy_ratelimit_record_t record_ring[RL_RECORD_SLOTS];
};

struct easy_region_ratelimitor_hmap_node_t {
    easy_hash_list_t hash_list_node;
    easy_region_ratelimitor_t *region_rlmtr;
};

struct easy_ratelimitor_t {
    easy_atomic_t           rl_lock;
    uint32_t                inited;
    float                   weight_group1;
    // uint64_t                min_rx_active_bw;
    double                  cpu_freq;
    double                  stat_period;
    uint64_t                cycles_per_stride;
    uint64_t                cycles_per_period;
    uint64_t                s2r_map_version_latest;
    uint64_t                s2r_map_version_cur;
    easy_list_t             ready_queue;
    easy_hash_t             *s2r_hmap; /* Address to region map.*/
    easy_array_t            *region_array;
    easy_spinrwlock_t       s2r_hmap_rwlock;
    int32_t                 ratelimit_enabled;
    easy_io_thread_t        *ratelimit_thread;

    void                      *get_region_id_args;
    easy_io_update_s2r_map_pt *update_s2r_map_cb;
    void                      *update_s2r_map_cb_args;
    easy_region_ratelimitor_t region_rlmtrs[RL_MAX_REGION_COUNT];
};

// 对应一个SOCKET连接
struct easy_connection_t {
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
    struct ev_loop          *loop;
    easy_pool_t             *pool;
    easy_io_thread_t        *ioth;
    easy_list_t             group_list_node;
    easy_list_t             conn_list_node;

    // file description
    uint32_t                default_msglen;
    uint32_t                first_msglen;
    int                     reconn_time, reconn_fail;
    int                     idle_time;
    int                     fd, seq;
    easy_addr_t             addr;

    ev_io                   read_watcher;
    ev_io                   write_watcher;
    ev_timer                timeout_watcher;
    ev_timer                pause_watcher;
    easy_list_t             message_list;

    easy_list_t             output;
    easy_io_handler_pt      *handler;
    easy_read_pt            *read;
    easy_write_pt           *write;
    easy_client_t           *client;
    easy_list_t             server_session_list;
    easy_hash_t             *send_queue;
    easy_list_t             *rx_request_queue;
    void                    *user_data;
    easy_uthread_t          *uthread;   //user thread

    uint32_t                status : 4;
    uint32_t                event_status : 4;
    uint32_t                type : 1;
    uint32_t                async_conn : 1;
    uint32_t                conn_has_error : 1;
    uint32_t                tcp_cork_flag : 1;
    uint32_t                wait_close : 1;
    uint32_t                need_redispatch : 1;
    uint32_t                read_eof : 1;
    uint32_t                auto_reconn : 1;
    uint32_t                life_idle : 1;
    uint32_t                slow_request_dumped: 1;
    uint32_t                ssl_sm_: 2; //instead of is_ssl_req_pkt(true/false)
    uint32_t                doing_request_count;
    uint32_t                destroy_retry_count;

    ev_tstamp               start_time, last_time;
    ev_tstamp               wait_client_time, wcs;

    easy_summary_node_t     *con_summary;  //add for summary
    easy_ssl_connection_t   *sc;
    void                    *evdata;
    easy_list_t             client_session_list;
    int64_t                 armed_ack_timeout;
    ev_tstamp               ack_wait_start;
    int64_t                 ack_bytes;
    int64_t                 send_bytes;
    int64_t                 recv_bytes;
    int64_t                 copied_buf_num;
    int64_t                 discarded_buf_num;

    int64_t                 keepalive_enabled;
    int64_t                 tx_keepalive_cnt;
    int64_t                 rx_keepalive_cnt;
    uint64_t                tls_version_option;
    ev_tstamp               last_tx_tstamp;
    ev_tstamp               last_rx_tstamp;
    uint8_t                 local_magic_ver;
    uint8_t                 peer_magic_ver;
    uint8_t                 magic_ver;
    uint8_t                 ratelimit_enabled;
	uint8_t                 is_negotiated;
    uint8_t                 keepalive_failed;
};

// Todo: add response size, ratelimit and bg/fg flags.
// ipacket放进来的包, opacket及出去的包
struct easy_request_t {
#ifdef EASY_DEBUG_DOING
    uint64_t                uuid;
#endif
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
    easy_message_session_t  *ms;

    easy_list_t             request_list_node;
    easy_list_t             rlmtr_queue_node;
    easy_list_t             all_node;
    int8_t                  retcode, status, waiting, alone;
    int                     reserved;
    ev_tstamp               start_time;
    void                    *ipacket;
    void                    *opacket;
    void                    *args;
    void                    *user_data;
    easy_client_wait_t      *client_wait;

    int64_t                  packet_id;
    int64_t                  client_start_time;
    int64_t                  client_send_time;
    int64_t                  client_connect_time;
    int64_t                  client_write_time;
    int64_t                  request_arrival_time;

    uint32_t                 pcode;
    int32_t                  arrival_push_diff;
    int32_t                  push_pop_diff;
    int32_t                  pop_process_start_diff;
    int32_t                  process_start_end_diff;
    int32_t                  process_end_response_diff;

    int64_t                  server_send_time;
    int64_t                  client_read_time;
    int64_t                  client_end_time;
    int64_t                  again_count;
    int64_t                  opacket_size;
    int8_t                   ratelimit_enabled;
    int8_t                   is_bg_flow;
    int8_t                   redispatched;
    int8_t                   protocol;
    uint64_t                 trace_id[4];
    uint64_t                 tenant_id;
    uint64_t                 session_id;
    uint64_t                 trace_point;
    uint64_t                 timeout_warn_count;
    char                     trace_bt[EASY_REQ_TRACE_BT_SIZE];
    easy_buf_t               tx_ebuf;
};

#define EASY_MESSAGE_SESSION_HEADER(name) \
    easy_list_t             name;         \
    easy_connection_t       *c;           \
    easy_pool_t             *pool;        \
    int8_t                  type;         \
    int8_t                  async;        \
    int8_t                  status;       \
    int8_t                  error;        \
    int                     align;

struct easy_message_session_t {
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
    EASY_MESSAGE_SESSION_HEADER(list_node);
};

// 用于接收, 一个或多个easy_request_t
struct easy_message_t {
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
    EASY_MESSAGE_SESSION_HEADER(message_list_node)

    easy_buf_t              *input;
    easy_list_t             request_list;
    easy_list_t             all_list;
    int                     request_list_count;
    int                     next_read_len;
    int                     recycle_cnt;

    void                    *user_data;
    int                     enable_trace;
};

//用于统计流量rt信息
struct easy_summary_t {
    int                     max_fd;
    ev_tstamp               time;
    easy_atomic_t           lock;
    easy_pool_t             *pool;
    easy_summary_node_t     *bucket[EASY_SUMMARY_CNT];
};

struct easy_summary_node_t {
    int                     fd;
    uint32_t                doing_request_count;
    uint64_t                done_request_count;
    uint64_t                in_byte, out_byte;
    ev_tstamp               rt_total;
};

// 用于发送, 只带一个easy_request_t
struct easy_session_t {
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
    EASY_MESSAGE_SESSION_HEADER(session_list_node);
    easy_list_t             write_list_node;
    easy_list_t             rlmtr_queue_node;
    ev_tstamp               timeout, now;
    ev_timer                timeout_watcher;

    easy_hash_list_t        send_queue_hash;
    easy_list_t             send_queue_list;
    easy_io_process_pt      *callback;
    easy_io_cleanup_pt      *cleanup;
    easy_io_write_success_pt *on_write_success;
    easy_addr_t             addr;
    easy_list_t             *nextb;

    uint64_t                packet_id;
    void                    *thread_ptr;
    easy_request_t          r;
    uint32_t                max_process_handler_time;
    uint32_t                buf_count;
    uint32_t                sent_buf_count;
    int8_t                  is_trace_time;
    int8_t                  unneed_response;
    int8_t                  enable_trace;
    int8_t                  ratelimit_enabled;
    int8_t                  is_bg_flow;
    int8_t                  redispatched;
    int8_t                  tx_buf_separated;
    int64_t                 opacket_size;
    void                    *tx_buf;
    char                    data[0];
};

struct easy_tx_session_pool_t {
    int                     inited;
    easy_array_t            *tx_sess_array;
    easy_atomic_t           tx_sess_array_lock;
    easy_atomic_t           session_total_count;
    easy_atomic_t           session_avail;
};

// 监听列表
#define EASY_LISTEN_HEADER                  \
    int                     fd;             \
    int8_t                  cur, old;       \
    uint8_t                 hidden_sum : 1; \
    uint8_t                 reuseport : 1;  \
    uint8_t                 is_simple : 1;  \
    easy_io_handler_pt      *handler;       \
    void                    *user_data;     \
    uint32_t                accept_count;

struct easy_listen_simple_t {
    EASY_LISTEN_HEADER;
};

struct easy_listen_t {
    EASY_LISTEN_HEADER;

    easy_addr_t             addr;
    easy_atomic_t           listen_lock;
    easy_io_thread_t        *curr_ioth;
    easy_io_thread_t        *old_ioth;
    easy_atomic_t           bind_port_cnt;

    easy_listen_t           *next;
    ev_io                   read_watcher[0];
};

// 用于统计处理速度
struct easy_io_stat_t {
    int64_t                 last_cnt;
    ev_tstamp               last_time;
    double                  last_speed;
    double                  total_speed;
    easy_io_stat_process_pt *process;
    easy_io_t               *eio;
};

// easy_io对象
struct easy_io_t {
    easy_pool_t             *pool;
    easy_list_t             eio_list_node;
    easy_atomic_t           lock;

    easy_listen_t           *listen;
    easy_listen_t           *listenadd;
    int                     io_thread_count;
    easy_thread_pool_t      *io_thread_pool;
    easy_thread_pool_t      *thread_pool;
    void                    *user_data;
    easy_list_t             thread_pool_list;

    // flags
    uint32_t                stoped : 1;
    uint32_t                started : 1;
    uint32_t                tcp_cork : 1;
    uint32_t                tcp_nodelay : 1;
    uint32_t                tcp_defer_accept : 1;
    uint32_t                listen_all : 1;
    uint32_t                uthread_enable : 1;
    uint32_t                affinity_enable : 1;
    uint32_t                no_redispatch : 1;
    uint32_t                do_signal : 1;
    uint32_t                block_thread_signal : 1;
    uint32_t                support_ipv6 : 1;
    uint32_t                checkdrc : 1;
    uint32_t                no_reuseport : 1;
    uint32_t                use_accept4 : 1;
    uint32_t                no_delayack : 1;
    uint32_t                no_force_destroy: 1;
    uint32_t                shutdown : 1;
    uint32_t                tcp_keepalive: 1;
    uint32_t                auto_reconn: 1;

    int32_t                 send_qlen;
    int32_t                 force_destroy_second; // second
    sigset_t                block_thread_sigset;
    int                     listen_backlog;
    uint32_t                recv_vlen;
    uint32_t                accept_count;
    uint32_t                tcp_keepidle;  // For keepAlive
    uint32_t                tcp_keepintvl;
    uint32_t                tcp_keepcnt;
    uint32_t                conn_timeout;
    uint32_t                ack_timeout;

    ev_tstamp               start_time;
    easy_summary_t          *eio_summary;
    easy_ssl_t              *ssl;
    easy_spinrwlock_t       ssl_rwlock_;

    int                     keepalive_enabled;
    easy_ratelimitor_t      *easy_rlmtr;
	uint64_t                magic;
    uint8_t                 negotiation_enable;
};

// base thread
struct easy_baseth_t {
    EASY_BASETH_DEFINE
};

struct easy_thread_pool_t {
    int                     thread_count;
    int                     member_size;
    int                     stoped;
    easy_atomic32_t         last_number;
    easy_list_t             list_node;
    easy_thread_pool_t      *next;
    char                    *last;
    void                    *monitor_tid;
    easy_io_thread_t        *ratelimit_thread;
    char                    data[0];
};

struct easy_client_wait_t {
    int                     done_count;
    int                     status;
    pthread_mutex_t         mutex;
    pthread_cond_t          cond;
    easy_list_t             session_list;
    easy_list_t             next_list;
};

struct easy_file_task_t {
    int                     fd;
    int                     bufsize;
    char                    *buffer;
    int64_t                 offset;
    int64_t                 count;
    easy_buf_t              *b;
    void                    *args;
};

struct easy_ssl_ctx_t {
    easy_pool_t             *pool;
    SSL_CTX                 *ctx;
    easy_list_t             list_node;
    int                     type;//true: client, false for server
    struct {
        char                    *file;
        int                     line;
        int                     prefer_server_ciphers;
        uint32_t                verify;
        uint32_t                verify_depth;
        int                     session_timeout;
        int                     session_cache;
        uint64_t                protocols;
        char                    *certificate;//server-cert
        char                    *certificate_key;//server-key
        char                    *dhparam;
        char                    *client_certificate;//root ca
        char                    *crl;
        char                    *ciphers;
        char                    *server_name;
        char                    *pass_phrase_dialog;
        int                     session_reuse;
    } conf;
};

struct easy_ssl_connection_t {
    SSL                     *connection;
    easy_ssl_schandler_pt   *handler;

    int                     last;
    uint32_t                handshaked : 1;
    uint32_t                renegotiation : 1;
    uint32_t                session_reuse : 1;
};

struct easy_ssl_t {
    easy_list_t             server_list;
    easy_pool_t             *pool;
    easy_hash_t             *server_map;
    easy_hash_t             *client_map;

    easy_ssl_ctx_t          *server_ctx;
    easy_ssl_ctx_t          *client_ctx;
};

struct easy_io_threads_pipefd_pool_t{
    int count;
    int pipefd[EASY_MAX_THREAD_CNT];
};
EASY_CPP_END

#endif
