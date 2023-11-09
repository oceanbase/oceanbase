#include "io/easy_io.h"
#include "packet/easy_simple_handler.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <easy_test.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define TEST_CNT 3
/**
 * 测试 easy_connection.c
 */
TEST(easy_connection, listen)
{
    easy_listen_t           *l;

    easy_log_level = (easy_log_level_t)0;
    l = easy_io_add_listen(NULL, 80, NULL);
    EXPECT_TRUE(l == NULL);

    easy_io_create(1);

    l = easy_io_add_listen(NULL, 80, NULL);
    EXPECT_TRUE(l == NULL);

    easy_io_start();

    l = easy_io_add_listen(NULL, 80, NULL);
    EXPECT_TRUE(l == NULL);
    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}
///////////////////////////////////////////////////////////////////////////////////////////////////
static int test_thread_1_server_process(easy_request_t *r)
{
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;
    packet = easy_simple_rnew(r, request->len);
    packet->len = request->len;
    packet->chid = request->chid;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, request->data, request->len);
    r->opacket = packet;
    return EASY_OK;
}
static int test_thread_1_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_1_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static void test_thread_1_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_1_server_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_1_connect;
    io_handler.on_disconnect = test_thread_1_disconnect;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static easy_atomic_t    test_thread_1_client_process_send = 0;
static easy_atomic_t    test_thread_1_client_process_recv = 0;
static int test_thread_1_client_process(easy_request_t *r)
{
    EXPECT_TRUE(r->ipacket != NULL);
    EXPECT_TRUE(r->opacket != NULL);

    if (r->ipacket && r->opacket) {
        easy_simple_packet_t    *p1 = (easy_simple_packet_t *)r->ipacket;

        if (p1->len != sizeof(int) || *((uint32_t *)p1->data) != p1->chid + 1) {
            EXPECT_TRUE(0);
        }
    } else {
        EXPECT_TRUE(0);
    }

    easy_session_destroy((easy_session_t *)r->ms);
    easy_atomic_inc(&test_thread_1_client_process_recv);

    if (test_thread_1_client_process_send >= 10000 &&
            test_thread_1_client_process_send == test_thread_1_client_process_recv) {
        easy_io_stop();
    }

    return EASY_OK;
}
static int test_thread_1_new_packet(easy_connection_t *c)
{
    easy_session_t          *s;
    easy_simple_packet_t     *packet;
    static easy_atomic32_t   chid = 0;

    if (test_thread_1_client_process_send >= 10000) {
        return EASY_OK;
    }

    if((packet = easy_simple_new(&s, sizeof(int))) == NULL)
        return EASY_ERROR;

    packet->data = &packet->buffer[0];
    packet->len = sizeof(int);
    packet->chid = easy_atomic32_add_return(&chid, 1);
    *((int *)packet->data) = packet->chid + 1;

    easy_connection_send_session(c, s);
    easy_atomic_inc(&test_thread_1_client_process_send);
    return EASY_OK;
}
static void test_thread_1_client(int port)
{
    easy_addr_t             addr;
    easy_io_handler_pt      io_handler;
    int                     i;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_1_client_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.new_packet = test_thread_1_new_packet;
    io_handler.on_connect = test_thread_1_connect;
    io_handler.on_disconnect = test_thread_1_disconnect;

    test_thread_1_client_process_send = 1;

    easy_io_start();
    addr = easy_inet_str_to_addr("localhost", port);

    for(i = 0; i < 4; i++) {
        if (easy_io_connect(addr, &io_handler, 0, NULL) != EASY_OK)
            goto error_exit;
    }

    test_thread_1_client_process_send = 0;
    easy_io_wait();
error_exit:
    EXPECT_EQ(test_thread_1_client_process_send, test_thread_1_client_process_recv);
    easy_io_destroy();
}
TEST(easy_connection, thread_1)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_1_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_1_client(port);
        exit(easy_test_retval);
    }

    waitpid(pid2, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
    kill(pid1, SIGINT);
    usleep(1000);
    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
static easy_thread_pool_t *test_thread_2_server_tp;
static int              test_thread_2_server_fd = 0;
#define test_thread_2_server_text "ABCDEFGH"
static int test_thread_2_server_process(easy_request_t *r)
{
    easy_thread_pool_push(test_thread_2_server_tp, r, easy_hash_key((uint64_t)(long)r));
    return EASY_AGAIN;
}
static int test_thread_2_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_2_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_2_request_process(easy_request_t *r, void *args)
{
    easy_simple_packet_t    *packet, *request;
    easy_file_buf_t         *fb;
    request = (easy_simple_packet_t *)r->ipacket;

    if ((request->chid % 2) == 0) {
        packet = easy_simple_rnew(r, request->len);
        packet->len = request->len;
        packet->chid = request->chid;
        packet->data = &packet->buffer[0];
        memcpy(packet->data, request->data, request->len);
    } else {
        packet = easy_simple_rnew(r, 0);
        packet->chid = request->chid;
        packet->len = 8;
        // sendfile
        fb = easy_file_buf_create(r->ms->pool);
        fb->fd = test_thread_2_server_fd;
        fb->offset = 0;
        fb->count = 8;
        easy_buf_chain_offer(&packet->list, (easy_buf_t *)fb);
    }

    r->opacket = packet;
    return EASY_OK;
}
static void test_thread_2_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;
    char                    tmpn[128];

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_2_server_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_2_connect;
    io_handler.on_disconnect = test_thread_2_disconnect;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    // create request thread
    test_thread_2_server_tp = easy_request_thread_create(4, test_thread_2_request_process, NULL);

    // open file
    lnprintf(tmpn, 128, "/tmp/a_%lu_%lu\n", pthread_self(), time(NULL));
    test_thread_2_server_fd = open(tmpn, O_CREAT | O_RDWR, 0600);
    i = write(test_thread_2_server_fd, test_thread_2_server_text, 8);

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
    close(test_thread_2_server_fd);
    unlink(tmpn);
}
static void *test_thread_2_client_start(void *args)
{
    easy_session_t          *s;
    easy_simple_packet_t    *p1;
    easy_simple_packet_t    *packet;
    static easy_atomic32_t  chid = 0;
    easy_addr_t             addr;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt, total, port;

    cnt = 0;
    total = TEST_CNT;
    easy_log_level = (easy_log_level_t)0;
    port = (int)(long)io_handler->user_data;
    addr = easy_inet_str_to_addr("localhost", port);
    i = easy_io_connect(addr, io_handler, 0, NULL);
    EXPECT_TRUE(i == EASY_OK);

    for(i = 0; i < total; i++) {
        if ((packet = easy_simple_new(&s, sizeof(int))) == NULL) {
            EXPECT_TRUE(0);
            break;
        }

        packet->data = &packet->buffer[0];
        packet->len = sizeof(int);
        packet->chid = easy_atomic32_add_return(&chid, 1);
        *((int *)packet->data) = packet->chid + 1;

        easy_session_set_request(s, packet, 0, NULL);
        p1 = (easy_simple_packet_t *)easy_io_send(addr, s);

        if (p1 == NULL || (p1->len != 4 && p1->len != 8)) {
            EXPECT_TRUE(0);
        } else if (p1->len == 4 && *((uint32_t *)p1->data) != p1->chid + 1) {
            EXPECT_TRUE(0);
        } else if (p1->len == 8 && memcmp(p1->data, test_thread_2_server_text, 8) != 0) {
            EXPECT_TRUE(0);
        } else {
            cnt ++;
        }

        easy_session_destroy(s);
    }

    easy_io_disconnect(addr);

    EXPECT_EQ(cnt, total);
    return (void *)NULL;
}
static void test_thread_2_client(int port)
{
    easy_io_handler_pt      io_handler;
    pthread_t               tids[4];
    int                     i;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = easy_client_wait_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_2_connect;
    io_handler.user_data = (void *)(long)port;

    for(i = 0; i < 4; i++) {
        pthread_create(&tids[i], NULL, test_thread_2_client_start, &io_handler);
    }

    for(i = 0; i < 4; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}
TEST(easy_connection, thread_2)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_2_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_2_client(port);
        exit(easy_test_retval);
    }

    waitpid(pid2, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
    kill(pid1, SIGINT);
    usleep(1000);
    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
static int test_thread_3_server_process(easy_request_t *r)
{
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;

    packet = easy_simple_rnew(r, request->len);
    packet->len = request->len;
    packet->chid = -request->chid;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, request->data, request->len);
    r->opacket = packet;
    return EASY_OK;
}
static int test_thread_3_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_3_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static void test_thread_3_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_3_server_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_3_connect;
    io_handler.on_disconnect = test_thread_3_disconnect;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static void *test_thread_3_client_start(void *args)
{
    easy_session_t          *s;
    easy_simple_packet_t    *p1;
    easy_simple_packet_t    *packet;
    static easy_atomic32_t  chid = 0;
    easy_addr_t             addr;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt, total, port;

    cnt = 0;
    total = 10;
    easy_log_level = (easy_log_level_t)0;
    port = (int)(long)io_handler->user_data;
    addr = easy_inet_str_to_addr("localhost", port);
    i = easy_io_connect(addr, io_handler, 0, NULL);
    EXPECT_TRUE(i == EASY_OK);

    for(i = 0; i < total; i++) {
        if ((packet = easy_simple_new(&s, sizeof(int))) == NULL) {
            EXPECT_TRUE(0);
            break;
        }

        packet->data = &packet->buffer[0];
        packet->len = sizeof(int);
        packet->chid = easy_atomic32_add_return(&chid, 1);
        *((int *)packet->data) = packet->chid + 1;

        easy_session_set_request(s, packet, 10, NULL);
        p1 = (easy_simple_packet_t *) easy_io_send(addr, s);

        if (p1 == NULL) {
            cnt ++;
        }

        easy_session_destroy(s);
    }

    EXPECT_EQ(cnt, total);
    easy_io_disconnect(addr);

    // 多次发送
    /*
    char                    buffer[12];
    i = easy_atomic32_add_return(&chid, 1);
    *((uint32_t *)buffer) = sizeof(int);
    *((uint32_t *)(buffer + sizeof(int))) = i;
    *((int *)(buffer + sizeof(int) * 2)) = i + 1;
    value = 1;
    setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, (const void *) &value, sizeof(value));

    for(i = 0; i < 12; i++) {
        write(c->fd, buffer + i, 1);
        usleep(1);
    }
    */

    return (void *)NULL;
}
static void test_thread_3_client(int port)
{
    easy_io_handler_pt      io_handler;
    pthread_t               tids[4];
    int                     i;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = easy_client_wait_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_3_connect;
    io_handler.user_data = (void *)(long)port;

    for(i = 0; i < 4; i++) {
        pthread_create(&tids[i], NULL, test_thread_3_client_start, &io_handler);
    }

    for(i = 0; i < 4; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}
TEST(easy_connection, thread_3)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_3_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_3_client(port);
        exit(easy_test_retval);
    }

    waitpid(pid2, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
    kill(pid1, SIGINT);
    usleep(1000);
    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
static easy_thread_pool_t *test_thread_4_server_tp;
#define test_thread_4_server_text "ABCDEFGH"
static int test_thread_4_server_process(easy_request_t *r)
{
    easy_thread_pool_push(test_thread_4_server_tp, r, easy_hash_key((uint64_t)(long)r));
    return EASY_AGAIN;
}
static int test_thread_4_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_4_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_4_request_process(easy_request_t *r, void *args)
{
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;

    packet = easy_simple_rnew(r, request->len);
    packet->len = request->len;
    packet->chid = request->chid;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, request->data, request->len);
    r->opacket = packet;

    sleep(1);
    return EASY_OK;
}
static void test_thread_4_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_4_server_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_4_connect;
    io_handler.on_disconnect = test_thread_4_disconnect;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    // create request thread
    test_thread_4_server_tp = easy_request_thread_create(4, test_thread_4_request_process, NULL);

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static void *test_thread_4_client_start(void *args)
{
    easy_session_t          *s;
    easy_simple_packet_t    *packet;
    static easy_atomic32_t  chid = 0;
    easy_addr_t             addr;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt __attribute__ ((unused)), total, port;

    cnt = 0;
    total = TEST_CNT;
    easy_log_level = (easy_log_level_t)0;
    port = (int)(long)io_handler->user_data;
    addr = easy_inet_str_to_addr("localhost", port);
    i = easy_io_connect(addr, io_handler, 0, NULL);
    EXPECT_TRUE(i == EASY_OK);

    for(i = 0; i < total; i++) {
        if ((packet = easy_simple_new(&s, sizeof(int))) == NULL) {
            EXPECT_TRUE(0);
            break;
        }

        packet->data = &packet->buffer[0];
        packet->len = sizeof(int);
        packet->chid = easy_atomic32_add_return(&chid, 1);
        *((int *)packet->data) = packet->chid + 1;

        easy_session_set_request(s, packet, 100, NULL);

        if (easy_io_dispatch(addr, s) != EASY_OK) {
            easy_session_destroy(s);
        }
    }

    easy_io_disconnect(addr);
    return (void *)NULL;
}
static int test_thread_4_client_process(easy_request_t *r)
{
    easy_session_destroy((easy_session_t *)r->ms);
    return EASY_OK;
}
static void test_thread_4_client(int port)
{
    easy_io_handler_pt      io_handler;
    pthread_t               tids[4];
    int                     i;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = test_thread_4_client_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_4_connect;
    io_handler.user_data = (void *)(long)port;

    for(i = 0; i < 4; i++) {
        pthread_create(&tids[i], NULL, test_thread_4_client_start, &io_handler);
    }

    for(i = 0; i < 4; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}
TEST(easy_connection, thread_4)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_4_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_4_client(port);
        exit(easy_test_retval);
    }

    waitpid(pid2, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
    kill(pid1, SIGINT);
    usleep(1000);
    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}
