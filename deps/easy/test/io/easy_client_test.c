#include "io/easy_client.h"
#include "io/easy_io.h"
#include <easy_test.h>
#include "packet/easy_simple_handler.h"
#include <sys/types.h>
#include <sys/wait.h>

///////////////////////////////////////////////////////////////////////////////////////////////////
easy_io_handler_pt      test_thread_4_client2_handler;
easy_addr_t             test_thread_4_server2_addr;
static int test_thread_4_server2_process(easy_request_t *r)
{
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;

    packet = easy_simple_rnew(r, request->len);
    packet->len = request->len;
    packet->chid = request->chid;
    packet->data = &packet->buffer[0];

    if (request->len == 4) request->data[3] ++;

    memcpy(packet->data, request->data, request->len);
    r->opacket = packet;
    return EASY_OK;
}
static int test_thread_4_server_process(easy_request_t *r)
{
    easy_connection_t       *c;
    easy_session_t          *s;
    easy_simple_packet_t    *packet, *request;
    request = (easy_simple_packet_t *)r->ipacket;

    // send
    c = easy_io_connect_thread(test_thread_4_server2_addr, &test_thread_4_client2_handler, 0, NULL);

    if (c == NULL) return EASY_ERROR;

    packet = easy_simple_new(&s, request->len);
    packet->len = request->len;
    packet->chid = request->chid + 10000;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, request->data, request->len);
    easy_session_set_args(s, r);

    if (easy_connection_send_session(c, s) == EASY_ERROR) {
        easy_session_destroy(s);
        return EASY_ERROR;
    }

    return EASY_AGAIN;
}
static int test_thread_4_client2_process(easy_request_t *r)
{
    easy_simple_packet_t    *response, *packet;
    easy_request_t          *request;
    request = (easy_request_t *)r->args;
    response = (easy_simple_packet_t *)r->ipacket;

    if (!response) {
        easy_request_do_reply(request);
        easy_session_destroy((easy_session_t *)r->ms);
        return EASY_ERROR;
    }

    packet = easy_simple_rnew(request, response->len);
    packet->len = response->len;
    packet->chid = ((easy_simple_packet_t *)request->ipacket)->chid;
    packet->data = &packet->buffer[0];
    memcpy(packet->data, response->data, response->len);
    request->opacket = packet;
    easy_request_do_reply(request);
    easy_session_destroy((easy_session_t *)r->ms);

    return EASY_OK;
}
static int test_thread_4_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_4_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static void test_thread_4_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler, io2_handler;
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
    memcpy(&io2_handler, &io_handler, sizeof(easy_io_handler_pt));
    io2_handler.process = test_thread_4_server2_process;
    memcpy(&test_thread_4_client2_handler, &io_handler, sizeof(easy_io_handler_pt));
    test_thread_4_client2_handler.process = test_thread_4_client2_process;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io2_handler)) != NULL)
            break;
    }

    test_thread_4_server2_addr = easy_inet_str_to_addr("localhost", port);
    port ++;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static void *test_thread_4_client_start(void *args)
{
    easy_session_t          *s;
    easy_simple_packet_t    *p1;
    easy_simple_packet_t    *packet;
    static easy_atomic32_t  chid = 0;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt, total, port;
    easy_client_wait_t      wobj;
    easy_addr_t             addr;

    cnt = 0;
    total = 10;
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

        easy_client_wait_init(&wobj);
        easy_session_set_wobj(s, &wobj);
        easy_io_dispatch(addr, s);
        easy_client_wait(&wobj, 1);
        p1 = (easy_simple_packet_t *) s->r.ipacket;

        if (p1 == NULL || p1->len != 4) {
            EXPECT_TRUE(0);
        } else {
            port = p1->chid + 1;
            port += 0x01000000;

            if (*((int32_t *)p1->data) != port) {
                EXPECT_TRUE(0);
            } else {
                cnt ++;
            }
        }

        easy_client_wait_cleanup(&wobj);
    }

    EXPECT_EQ(cnt, total);

    easy_io_disconnect(addr);

    EXPECT_EQ(cnt, total);
    return (void *)NULL;
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
    io_handler.process = easy_client_wait_process;
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
TEST(easy_client, thread_4)
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
void easy_client_send_timeout(int sig)
{
    EXPECT_TRUE(0);
    //exit(-1);
}
TEST(easy_client, send)
{
    easy_io_handler_pt      io_handler;
    easy_session_t          *s;
    easy_simple_packet_t    *packet;
    easy_addr_t             addr;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_simple_decode;
    io_handler.encode = easy_simple_encode;
    io_handler.process = easy_client_wait_process;
    io_handler.get_packet_id = easy_simple_packet_id;
    io_handler.on_connect = test_thread_4_connect;

    if ((packet = easy_simple_new(&s, sizeof(int))) == NULL) {
        EXPECT_TRUE(0);
    }

    packet->data = &packet->buffer[0];
    packet->len = sizeof(int);
    packet->chid = 1;
    addr = easy_inet_str_to_addr("231.0.0.1", 22);
    s->status = EASY_CONNECT_SEND;
    signal(SIGALRM, easy_client_send_timeout);
    alarm(3);
    easy_io_send(addr, s);
    easy_session_destroy(s);
    signal(SIGALRM, SIG_IGN);

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
}

