#include "io/easy_file.h"
#include "io/easy_io.h"
#include <easy_test.h>
#include "packet/http/easy_http_handler.h"
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/**
 * 测试easy_request
 */
///////////////////////////////////////////////////////////////////////////////////////////////////
#define TEST_ROOT_DIR "/tmp"
static char             test_thread_7_server_text[400];
static char             test_thread_7_filename[64];
static void test_thread_7_string_tobuf(easy_pool_t *pool, easy_list_t      *bc, char *name, easy_buf_string_t *s)
{
    if (s->len) {
        easy_buf_t              *b = easy_buf_check_write_space(pool, bc, s->len + strlen(name) + 16);
        b->last += sprintf(b->last, "%s%.*s<br>\n", name, s->len, easy_buf_string_ptr(s));
    }
}
static void test_thread_7_request_tobuf(easy_pool_t *pool, easy_list_t      *bc, easy_http_request_t *p)
{
    easy_string_pair_t      *t;
    easy_buf_t              *b;

    test_thread_7_string_tobuf(pool, bc, "path: ", &p->str_path);
    test_thread_7_string_tobuf(pool, bc, "query_sring: ", &p->str_query_string);
    test_thread_7_string_tobuf(pool, bc, "str_fragment: ", &p->str_fragment);
    test_thread_7_string_tobuf(pool, bc, "str_body: ", &p->str_body);
    // headers
    easy_list_for_each_entry(t, &p->headers_in->list, list) {
        if (t->name.len && t->value.len) {
            b = easy_buf_check_write_space(pool, bc, t->name.len + t->value.len + 16);
            b->last += sprintf(b->last, "%.*s: %.*s<br>\n",
                               t->name.len, easy_buf_string_ptr(&t->name),
                               t->value.len, easy_buf_string_ptr(&t->value));
        }
    }
}
static easy_thread_pool_t *test_thread_7_server_tp;
static int test_thread_7_request_process(easy_request_t *r, void *args)
{
    easy_file_task_t        *ft = (easy_file_task_t *) r->args;
    easy_http_request_t     *p = (easy_http_request_t *) r->ipacket;
    int                     rc = 0;
    easy_file_buf_t         *fb = NULL;

    if (p->str_query_string.len > 0 && memcmp("sendfile=y", p->str_query_string.data, 10) == 0) {
        easy_file_task_reset(ft, EASY_BUF_FILE);
        posix_fadvise(ft->fd, ft->offset, ft->bufsize, POSIX_FADV_WILLNEED);
        // set sendfile to output
        fb = (easy_file_buf_t *)ft->b;
        fb->fd = ft->fd;
        fb->offset = ft->offset;
        fb->count = ft->bufsize;
        rc = ft->bufsize;
    } else {
        easy_file_task_reset(ft, 0);
        rc = pread(ft->fd, ft->b->pos, ft->bufsize, ft->offset);
        ft->b->last = ft->b->pos + rc;
    }

    if (rc > 0) {
        easy_buf_chain_offer(&p->output, ft->b);
        r->opacket = p;
    }

    // first
    if (ft->offset == 0) {
        p->content_length = ft->count;
        p->is_raw_header = 0;
    } else {
        p->is_raw_header = 1;
    }

    // next
    ft->offset += rc;

    if (ft->offset >= ft->count || rc <= 0) {
        if (fb)
            easy_file_buf_set_close(fb);
        else
            close(ft->fd);

        return EASY_OK;
    } else if (ft->offset + ft->bufsize > ft->count) {
        ft->bufsize = ft->count - ft->offset;
    }

    return EASY_AGAIN;
}
static int test_thread_7_server_process(easy_request_t *r)
{
    easy_http_request_t         *p;
    int                         fd;
    char                        filename[128], url[128];
    easy_string_pair_t          *t, *t1, *t2;

    r->opacket = p = (easy_http_request_t *)r->ipacket;

    // 得到header
    t = easy_hash_string_get(p->headers_in, "Accept-Language", 15);
    EXPECT_TRUE(t != NULL);
    t2 = easy_hash_string_get(p->headers_in, "Accept-Language", 16);
    EXPECT_TRUE(t2 == NULL);
    t1 = easy_hash_string_del(p->headers_in, "Accept-Language", 15);
    EXPECT_EQ((long)t, (long)t1);
    t1 = easy_hash_string_del(p->headers_in, "Accept-Language", 15);
    EXPECT_TRUE(t1 == NULL);

    if (p->parser.method == HTTP_POST)
        p->str_query_string = p->str_body;

    if (p->str_query_string.len == 0) {
        test_thread_7_request_tobuf(r->ms->pool, &p->output, p);
    } else {
        // 处理文件名
        url[0] = '\0';

        if (p->str_path.len) lnprintf(url, 128, "%s", easy_buf_string_ptr(&p->str_path));

        if (easy_http_merge_path(filename, 128, TEST_ROOT_DIR, url)) {
            easy_buf_string_set(&p->status_line, EASY_HTTP_STATUS_400);
            return EASY_OK;
        }

        // 打开文件
        fd = open(filename, O_RDONLY);

        if (fd >= 0) {
            r->args = (void *)easy_file_task_create(r, fd, 0);
            easy_thread_pool_push(test_thread_7_server_tp, r, easy_hash_key((uint64_t)(long)r));
            return EASY_AGAIN;
        } else {
            easy_buf_string_set(&p->status_line, EASY_HTTP_STATUS_404);
        }
    }

    return EASY_OK;
}
static int test_thread_7_connect(easy_connection_t *c)
{
    return EASY_OK;
}
static int test_thread_7_disconnect(easy_connection_t *c)
{
    return EASY_OK;
}
static void test_thread_7_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_var.tcp_cork = 1;
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = test_thread_7_server_process;
    io_handler.on_connect = test_thread_7_connect;
    io_handler.on_disconnect = test_thread_7_disconnect;

    port = 2011;

    for(i = 0; i < 10; i++, port++) {
        if ((l = easy_io_add_listen(NULL, port, &io_handler)) != NULL)
            break;
    }

    test_thread_7_server_tp = easy_request_thread_create(2, test_thread_7_request_process, NULL);

    easy_io_start();
    i = write(fd, &port, 2);
    easy_io_wait();
    easy_io_destroy();
}
static void *test_thread_7_client_start(void *args)
{
    easy_session_t          *s;
    easy_http_packet_t      *packet;
    easy_http_request_t     *p1;
    easy_addr_t             addr;
    easy_io_handler_pt      *io_handler = (easy_io_handler_pt *)args;
    int                     i, cnt, total, port, status;

    cnt = 0;
    total = 10;
    port = (int)(long)io_handler->user_data;
    addr = easy_inet_str_to_addr("localhost", port);
    i = easy_io_connect(addr, io_handler, 0, NULL);
    EXPECT_TRUE(i == EASY_OK);

    for(i = 0; i < total; i++) {
        if ((packet = easy_session_packet_create(easy_http_packet_t, s, 0)) == NULL) {
            EXPECT_TRUE(0);
            break;
        }

        easy_list_init(&packet->output);
        status = 200;
        easy_buf_string_set(&packet->str_path, test_thread_7_filename);

        if ((i % 3) == 1) {
            easy_buf_string_set(&packet->str_query_string, "sendfile=y");
        } else if ((i % 3) == 2) {
            easy_buf_string_set(&packet->str_query_string, "sendfile=n#abc");
        }

        if (i % 2) packet->method = HTTP_POST;

        packet->headers_out = easy_header_create_table(s->pool);
        easy_http_add_header(s->pool, packet->headers_out, "Host", "localhost");
        easy_http_add_header(s->pool, packet->headers_out, "Connection", "keep-alive");
        easy_http_add_header(s->pool, packet->headers_out, "Accept", "image/jpeg");
        easy_http_add_header(s->pool, packet->headers_out, "User-Agent", "Mozilla/4.0");
        easy_http_add_header(s->pool, packet->headers_out, "Accept-Language", "zh-CN");

        p1 = (easy_http_request_t *) easy_io_send(addr, s);

        cnt ++;

        if (p1 == NULL) {
            EXPECT_TRUE(0);
            cnt --;
        } else {
            EXPECT_EQ(p1->parser.status_code, status);

            if ((i % 3) && (p1->str_body.len == 0 || strncmp(easy_buf_string_ptr(&p1->str_body),
                            test_thread_7_server_text, strlen(test_thread_7_server_text)))) {
                EXPECT_TRUE(0);
                cnt --;
            }
        }

        easy_session_destroy(s);

        if (p1 == NULL) break;
    }

    easy_io_disconnect(addr);

    //    EXPECT_EQ(cnt, total);
    return (void *)NULL;
}
static void test_thread_7_client(int port)
{
    easy_io_handler_pt      io_handler;
    pthread_t               tids[4];
    char                    fullname[128];
    int                     i, fd;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_var.tcp_cork = 1;
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_http_client_on_decode;
    io_handler.encode = easy_http_client_on_encode;
    io_handler.process = easy_client_wait_process;
    io_handler.on_connect = test_thread_7_connect;
    io_handler.user_data = (void *)(long)port;

    memset(test_thread_7_server_text, 'A', 400);
    test_thread_7_server_text[399] = '\0';
    lnprintf(test_thread_7_filename, 64, "/b_%lu_%lu", pthread_self(), time(NULL));
    lnprintf(fullname, 128, "%s%s", TEST_ROOT_DIR, test_thread_7_filename);
    fd = open(fullname, O_CREAT | O_RDWR, 0600);
    i = write(fd, test_thread_7_server_text, strlen(test_thread_7_server_text));
    close(fd);

    for(i = 0; i < 1; i++) {
        pthread_create(&tids[i], NULL, test_thread_7_client_start, &io_handler);
    }

    for(i = 0; i < 1; i++) {
        pthread_join(tids[i], NULL);
    }

    easy_io_stop();
    easy_io_wait();
    easy_io_destroy();
    unlink(fullname);
}
TEST(easy_file, thread_7)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_7_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_7_client(port);
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

