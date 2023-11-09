#include "io/easy_request.h"
#include "io/easy_io.h"
#include <easy_test.h>
#include "packet/easy_simple_handler.h"
#include <sys/types.h>
#include <sys/wait.h>

/**
 * 测试easy_request
 */
///////////////////////////////////////////////////////////////////////////////////////////////////
static easy_thread_pool_t *test_thread_5_server_tp;
static int test_thread_5_server_process(easy_message_t *m)
{
    static int              x = 0;

    if (x ++ % 2) {
        easy_thread_pool_push_message(test_thread_5_server_tp, m, easy_hash_key((uint64_t)(long)m));
    } else {
        easy_thread_pool_push_message(test_thread_5_server_tp, m, 0);
    }

    return EASY_AGAIN;
}
static int test_thread_5_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_5_cleanup(easy_request_t *r, void *apacket)
{
    return EASY_OK;
}
static int test_thread_5_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_5_request_process(easy_request_t *r, void *args)
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
static void test_thread_5_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.batch_process = test_thread_5_server_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_5_connect;
    io_handler.on_disconnect = test_thread_5_disconnect;
    io_handler.cleanup = test_thread_5_cleanup;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    // create request thread
    test_thread_5_server_tp = easy_request_thread_create(4, test_thread_5_request_process, NULL);

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static void *test_thread_5_client_start(void *args)
{
    easy_session_t          *s;
    easy_simple_packet_t    *p1;
    easy_simple_packet_t    *packet;
    static easy_atomic32_t  chid = 0;
    easy_addr_t             addr;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt, total, port;

    cnt = 0;
    total = 5000;
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

        p1 = (easy_simple_packet_t *) easy_io_send(addr, s);

        if (p1 == NULL || (p1->len != 4)) {
            EXPECT_TRUE(0);
        } else if (p1->len == 4 && *((uint32_t *)p1->data) != p1->chid + 1) {
            EXPECT_TRUE(0);
        } else {
            cnt ++;
        }

        easy_session_destroy(s);

        if (p1 == NULL) break;
    }

    easy_io_disconnect(addr);

    EXPECT_EQ(cnt, total);
    return (void *)NULL;
}
static void test_thread_5_client(int port)
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
    io_handler.on_connect = test_thread_5_connect;
    io_handler.cleanup = test_thread_5_cleanup;
    io_handler.user_data = (void *)(long)port;

    for(i = 0; i < 4; i++) {
        pthread_create(&tids[i], NULL, test_thread_5_client_start, &io_handler);
    }

    for(i = 0; i < 4; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}
TEST(easy_request, thread_5)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_5_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_5_client(port);
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

