#define EASY_MULTIPLICITY
#include "io/easy_io.h"
#include "packet/easy_simple_handler.h"
#include <easy_test.h>
#include <sys/wait.h>
#include <arpa/inet.h>

/**
 * 测试 easy_io.c
 */
typedef struct test_iocp_t {
    int                     io, file, iocheck, filecheck;
    int                     port;
    easy_io_t               *eio, *eio1;
    easy_io_handler_pt      *io_handler;
} test_iocp_t;

static void *test_realloc_1 (void *ptr, size_t size)
{
    return NULL;
}
static void *test_realloc_2 (void *ptr, size_t size)
{
    size_t                  n1;
    n1 = sizeof(easy_thread_pool_t) + sizeof(easy_io_thread_t) * EASY_MAX_THREAD_CNT;

    if (size == n1)
        return NULL;
    else
        return easy_test_realloc(ptr, size);
}
TEST_SETUP(easy_io)
{
    easy_log_level = (easy_log_level_t)0;
}
// int easy_io_create(int io_thread_count, int file_thread_count)
TEST(easy_io, create)
{
    easy_io_t               *eio, eio1 = {NULL};
    int                     i, cnt;
    int                     cpu_cnt = sysconf(_SC_NPROCESSORS_CONF);
    test_iocp_t             list[] = {
        {EASY_MAX_THREAD_CNT + 1, EASY_MAX_THREAD_CNT + 1, cpu_cnt, cpu_cnt / 2},
        { -1, EASY_MAX_THREAD_CNT + 1, cpu_cnt, cpu_cnt / 2},
        {EASY_MAX_THREAD_CNT + 1, -1, cpu_cnt, cpu_cnt / 2},
        { -1, -1, cpu_cnt, cpu_cnt / 2},
        {10, -1, 10, 5},
        { -1, 10, cpu_cnt, 10},
        {10, 0, 10, 0},
        {0, 0, cpu_cnt, 0},
    };

    // 1. init
    eio = easy_io_create(&eio1, 1);
    EXPECT_TRUE(eio != NULL);
    EXPECT_TRUE(eio->pool != NULL);
    eio = easy_io_create(&eio1, 1);
    EXPECT_TRUE(eio != NULL);
    EXPECT_TRUE(eio->pool != NULL);
    easy_io_destroy(eio);

    // 3. thread count
    cnt = sizeof(list) / sizeof(list[0]);

    for(i = 0; i < cnt; i++) {
        eio = easy_io_create(NULL, list[i].io);
        EXPECT_TRUE(eio != NULL);
        EXPECT_EQ(eio->io_thread_count, list[i].iocheck);
        easy_io_destroy(eio);
    }

    // 4.
    easy_pool_set_allocator(test_realloc_1);
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio == NULL);
    easy_io_destroy(eio);
    easy_pool_set_allocator(easy_test_realloc);

    // 5.
    easy_pool_set_allocator(test_realloc_2);
    eio = easy_io_create(NULL, EASY_MAX_THREAD_CNT);
    EXPECT_TRUE(eio == NULL);
    easy_io_destroy(eio);

    // 6.
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);
    easy_io_destroy(eio);
    easy_pool_set_allocator(easy_test_realloc);
}

// int easy_io_start(eio)
TEST(easy_io, start)
{
    int                     ret;
    easy_io_t               *eio = NULL;

    // no init
    easy_io_destroy(eio);
    EXPECT_TRUE (easy_io_start(eio) == EASY_ERROR)
    EXPECT_TRUE (easy_io_start(&easy_io_var) == EASY_ERROR)

    // 1.
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);

    ret = easy_io_start(eio);
    EXPECT_EQ(ret, EASY_OK);

    EXPECT_TRUE(eio->started);
    easy_io_stop(eio);
    EXPECT_TRUE(eio->stoped);

    easy_io_wait(eio);
    easy_io_destroy(eio);
}

static void test_stop_process(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_io_t               *eio = (easy_io_t *)w->data;
    easy_io_stop(eio);
}
// int easy_io_start(eio)
TEST(easy_io, wait)
{
    int                     ret;
    ev_timer                stop_watcher;
    easy_io_thread_t        *ioth;
    easy_listen_t           *listen __attribute__ ((unused));
    easy_io_handler_pt      io_handler;
    easy_io_t               *eio;

    // 1.
    easy_log_level = (easy_log_level_t)0;
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);

    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, 0);
    ev_timer_init (&stop_watcher, test_stop_process, 0., 1);
    stop_watcher.data = eio;
    ev_timer_start (ioth->loop, &stop_watcher);

    ret = easy_io_start(eio);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_TRUE(eio->started);

    ret = easy_io_wait(eio);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_TRUE(eio->pool != NULL);
    easy_io_destroy(eio);

    // 2.
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);

    // add listen
    memset(&io_handler, 0, sizeof(io_handler));
    listen = easy_io_add_listen(eio, NULL, 9998, &io_handler);
    listen = easy_io_add_listen(eio, NULL, 9999, &io_handler);

    // stop
    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, 0);
    ev_timer_init (&stop_watcher, test_stop_process, 0., 0.001);
    stop_watcher.data = eio;
    ev_timer_start (ioth->loop, &stop_watcher);

    ret = easy_io_start(eio);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_TRUE(eio->started);

    ret = easy_io_wait(eio);
    EXPECT_EQ(ret, EASY_OK);
    EXPECT_TRUE(eio->pool != NULL);
    easy_io_destroy(eio);
}

// int easy_io_stop(eio)
TEST(easy_io, stop)
{
    easy_io_t               *eio;

    // empty
    easy_io_stop(NULL);

    // 2.
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);
    easy_io_stop(eio);
    EXPECT_EQ(eio->stoped, 1);
    easy_io_destroy(eio);
}

// struct ev_loop *easy_io_thread_loop(int index)
TEST(easy_io, thread_loop)
{
    struct ev_loop          *loop;
    easy_io_t               *eio;

    // 1.
    eio = easy_io_create(NULL, 1);
    EXPECT_TRUE(eio != NULL);
    loop = easy_io_thread_loop(eio, 0);
    EXPECT_TRUE(loop != NULL);

    loop = easy_io_thread_loop(eio, 1);
    EXPECT_TRUE(loop == NULL);
    easy_io_destroy(eio);
}

static int test_process_server(easy_request_t *r)
{
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;
    packet = easy_simple_rnew(r, request->len);
    packet->len = request->len;
    packet->chid = request->chid;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, request->data, request->len);
    r->opacket = packet;
    usleep(10000);
    return EASY_OK;
}
static int test_process_server1(easy_request_t *r)
{
    return EASY_AGAIN;
}
static int test_process_client(easy_request_t *r)
{
    easy_io_t               *eio = (easy_io_t *)r->args;
    EXPECT_TRUE(r->opacket != NULL);

    if (eio->user_data == NULL) EXPECT_TRUE(r->ipacket != NULL);

    easy_session_destroy((easy_session_t *)r->ms);
    easy_io_stop(eio);
    return (eio->user_data == NULL) ? EASY_OK : EASY_ERROR;
}
static void *test_send_thread(void *args)
{
    int                     ret;
    easy_io_t               *eio;
    easy_simple_packet_t    *packet;
    easy_io_handler_pt      io_handler;
    easy_session_t          *s;
    easy_buf_t              *b1;
    easy_addr_t             addr;

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_process_client;
    test_iocp_t             *cp = (test_iocp_t *) args;
    eio = easy_io_create(NULL, 1);

    if (eio == NULL) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

    // start
    ret = easy_io_start(eio);

    if (ret == EASY_ERROR) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

    //connect
    addr = easy_inet_str_to_addr("localhost", cp->port);

    if (easy_io_connect(eio, addr, &io_handler, 0, NULL)) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

    packet = easy_simple_new(&s, 128);
    memset(&packet->buffer[0], 0, 124);
    packet->len = (cp->filecheck ? 0x4000001 : 124);
    packet->data = NULL;
    b1 = easy_buf_pack(s->pool, &packet->buffer[0], 124);
    easy_buf_chain_offer(&packet->list, b1);

    if (cp->filecheck) eio->user_data = eio;

    easy_session_set_args(s, eio);
    easy_session_set_timeout(s, 1000);

    if ((ret = easy_io_dispatch(eio, addr, s)) != EASY_OK) {
        easy_session_destroy(s);
        EXPECT_TRUE(0);
        goto error_exit;
    }

    EXPECT_EQ(ret, EASY_OK);

    ret = easy_io_wait(eio);

    if (ret == EASY_ERROR) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

error_exit:

    if (eio) easy_io_destroy(eio);

    easy_io_stop(cp->eio);
    return (void *)NULL;
}
void test_io_print_stat(easy_io_stat_t *eio)
{
}
static void test_uthread_start1(void *args)
{
    test_iocp_t             *cp = (test_iocp_t *) args;

    easy_simple_packet_t    *packet;
    easy_session_t          *s;
    easy_connection_t       *c;
    easy_addr_t             addr;

    addr = easy_inet_str_to_addr("localhost", cp->port);

    if ((c = easy_io_connect_thread(cp->eio1, addr, cp->io_handler, 0, NULL)) == NULL) {
        return;
    }

    if (easy_client_uthread_wait_conn(c) != EASY_OK) {
        return;
    }

    if ((packet = easy_simple_new(&s, 132)) == NULL)
        return;

    packet->data = &packet->buffer[0];
    packet->len = 124;
    memset(packet->data, 0, packet->len);

    // send
    if (easy_connection_send_session(c, s) != EASY_OK) {
        easy_session_destroy(s);
        EXPECT_TRUE(0);
        goto error_exit;
    }

    if (easy_client_uthread_wait_session(s) != EASY_OK) {
        easy_session_destroy(s);
        return;
    }

    easy_session_destroy(s);
error_exit:
    easy_io_stop(cp->eio1);
}
void *test_uthread_start(void *args)
{
    test_iocp_t             *cp = (test_iocp_t *) args;

    easy_io_t               *eio;
    easy_io_handler_pt      io_handler;
    int                     ret;

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    easy_client_uthread_set_handler(&io_handler);

    eio = easy_io_create(NULL, 1);

    if (eio == NULL) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

    cp->eio1 = eio;
    cp->io_handler = &io_handler;
    easy_eio_set_uthread_start(eio, test_uthread_start1, cp);
    // start
    ret = easy_io_start(eio);

    if (ret == EASY_ERROR) {
        EXPECT_TRUE(0);
        goto error_exit;
    }

    easy_io_wait(eio);

error_exit:

    if (eio) easy_io_destroy(eio);

    easy_io_stop(cp->eio);

    return (void *)NULL;
}
static void test_log_print(const char *message)
{
}
static void test_simple_fork()
{
    int                     i, cnt, ret, port, size __attribute__ ((unused));
    easy_io_handler_pt      io_handler;
    easy_listen_t           *listen;
    pthread_t               tid;
    easy_io_t               *eio;

    test_iocp_t             *cp, list[] = {
        {1, 1, 1, 0, 0},
        {1, 0, 0, 0, 0},
        {1, 1, 2, 0, 0},
        {2, 2, 0, 0, 0},
        {1, 1, 3, 0, 0}
    };

    port = 0;
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_process_server;
    // cnt
    cnt = sizeof(list) / sizeof(list[0]);
    size = 0;

    for(i = 0; i < cnt; i++) {
        cp = &list[i];
        easy_log_level = (easy_log_level_t)cp->port;

        port = 2011;
        eio = easy_io_create(NULL, cp->io);
        EXPECT_TRUE(eio != NULL);

        // disable uthread
        if (0 && cp->iocheck == 2) {
            easy_eio_set_uthread_start(eio, NULL, NULL);
        } else if (cp->iocheck == 3) {
            eio->listen_all = 1;
            easy_log_level = (easy_log_level_t)10;
            easy_log_set_print(test_log_print);
        }

        if ((listen = easy_io_add_listen(eio, NULL, 0, &io_handler)) == NULL) {
            EXPECT_TRUE(0);
            return;
        }
        port = ntohs(listen->addr.port);

        // start
        ret = easy_io_start(eio);
        EXPECT_EQ(ret, EASY_OK);

        ev_timer                stat_watcher;
        easy_io_stat_t          iostat;
        easy_eio_stat_watcher_start(eio, &stat_watcher, 0.05, &iostat, ((i == 1) ? test_io_print_stat : NULL));

        // send thread
        cp->port = port;
        cp->eio = eio;

        if (cp->iocheck != 1) {
            pthread_create(&tid, NULL, test_send_thread, cp);
            pthread_join(tid, NULL);
        } else {
            pthread_create(&tid, NULL, test_send_thread, cp);
            //pthread_create(&tid, NULL, test_uthread_start, cp);
            pthread_join(tid, NULL);
        }

        ret = easy_io_wait(eio);
        EXPECT_EQ(ret, EASY_OK);
        easy_io_destroy(eio);
    }

    easy_log_level = (easy_log_level_t)0;
    easy_log_set_print(easy_log_print_default);
}
// easy_io_set_uthread_start
TEST(easy_io, simple_send)
{
    pid_t                   pid1;
    int                     ret = 0;

    // child
    if ((pid1 = fork()) == 0) {
        test_simple_fork();
        exit(easy_test_retval);
    }

    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}
static void test_timeout_fork()
{
    int                     ret, port;
    easy_io_handler_pt      io_handler;
    easy_listen_t           *listen;
    pthread_t               tid;
    easy_io_t               *eio;
    test_iocp_t             cp;
    memset(&io_handler, 0, sizeof(io_handler));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_process_server1;
    port = 2011;
    eio = easy_io_create(NULL, 1);

    if ((listen = easy_io_add_listen(eio, NULL, 0, &io_handler)) == NULL) {
        EXPECT_TRUE(0);
        return;
    }
    port = ntohs(listen->addr.port);

    ret = easy_io_start(eio);
    EXPECT_EQ(ret, EASY_OK);
    cp.port = port;
    cp.eio = eio;
    cp.filecheck = 1;
    pthread_create(&tid, NULL, test_send_thread, &cp);
    pthread_join(tid, NULL);
    ret = easy_io_wait(eio);
    EXPECT_EQ(ret, EASY_OK);
    easy_io_destroy(eio);
}
TEST(easy_io, simple_timeout)
{
    pid_t                   pid;
    int                     ret = 0;

    // child
    if ((pid = fork()) == 0) {
        test_timeout_fork();
        exit(easy_test_retval);
    }

    usleep(50000);
    waitpid(pid, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}
