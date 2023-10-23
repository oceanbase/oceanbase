#include "io/easy_io.h"
#include <easy_test.h>
#include "util/easy_time.h"
#include "packet/http/easy_http_handler.h"
#include "packet/http/http_parser.h"
#include "io/easy_connection.h"
#include <sys/types.h>
#include <sys/wait.h>

static void *test_realloc (void *ptr, size_t size)
{
    return NULL;
}
/**
 * 测试 easy_http_handler
 */
///////////////////////////////////////////////////////////////////////////////////////////////////
TEST(easy_http_handler, create)
{
    easy_http_packet_t      *packet;
    easy_pool_t             *pool;
    char                    *value, *key;
    int                     i;
    char                    data[16];

    pool = easy_pool_create(1024);
    packet = easy_http_packet_create(pool);

    // add header
    for(i = 0; i < 128; i++) {
        lnprintf(data, 16, "key%d", i);
        key = easy_pool_strdup(pool, data);
        lnprintf(data, 16, "value%d", i);
        value = easy_pool_strdup(pool, data);
        easy_http_add_header(pool, packet->headers_out, key, value);
    }

    // get header
    for(i = 0; i < 128; i++) {
        lnprintf(data, 16, "key%d", i);
        value = easy_http_get_header(packet->headers_out, data);
        lnprintf(data, 16, "value%d", i);
        EXPECT_TRUE(memcmp(value, data, strlen(data)) == 0);
    }

    // del header
    for(i = 0; i < 128; i++) {
        if (i % 2)
            lnprintf(data, 16, "key%d", i);
        else
            lnprintf(data, 16, "Key%d", i);

        value = easy_http_del_header(packet->headers_out, data);
        lnprintf(data, 16, "value%d", i);

        if (value) {
            EXPECT_TRUE(memcmp(value, data, strlen(data)) == 0);
        } else {
            EXPECT_TRUE(0);
        }
    }

    // header again
    for(i = 0; i < 128; i++) {
        lnprintf(data, 16, "key%d", i);
        value = easy_http_del_header(packet->headers_out, data);
        EXPECT_TRUE(value == NULL);
        value = easy_http_get_header(packet->headers_out, data);
        EXPECT_TRUE(value == NULL);
    }

    easy_pool_destroy(pool);
}

TEST(easy_http_handler, urldecode)
{
    char                    buffer[32];
    int                     len;

    memcpy(buffer, "%41%41c", 8);
    len = easy_url_decode(buffer, strlen(buffer));
    EXPECT_EQ(len, 3);
    EXPECT_TRUE(memcmp(buffer, "AAc", len) == 0);
}

TEST(easy_http_handler, string_end)
{
    easy_pool_t             *pool;
    easy_http_request_t     p;
    easy_string_pair_t      header;
    char                    buffer[2];

    // easy_http_request_t
    memset(&p, 0, sizeof(easy_http_request_t));
    memset(buffer, 'A', 2);

    p.str_fragment.len = 1;
    p.str_fragment.data --;
    buffer[1] = '\0';
    easy_buf_string_set(&header.name, buffer);
    easy_buf_string_set(&header.value, buffer);
    pool = easy_pool_create(1024);
    p.headers_in = easy_header_create_table(pool);
    easy_hash_string_add(p.headers_in, &header);

    easy_pool_destroy(pool);
}

TEST(easy_http_hander, decode)
{
    easy_pool_t             *pool;
    easy_message_t          *m;
    int                     size;
    void                    *ptr;

    // failure
    easy_log_level = (easy_log_level_t)0;
    pool = easy_pool_create(0);
    m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    size = pool->end - pool->last - sizeof(easy_buf_t);
    size = ((size < 0) ? EASY_POOL_ALIGNMENT : size);
    m->input = easy_buf_create(pool, size);
    m->pool = pool;

    easy_pool_set_allocator(test_realloc);
    ptr = easy_http_server_on_decode(m);
    easy_pool_set_allocator(easy_test_realloc);
    EXPECT_TRUE(ptr == NULL);

    // null string
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr == NULL);

    // failure http
    size = sprintf(m->input->pos, "XXX http://1.taobao.com/ HTTP/1.1");
    m->input->last = m->input->pos + size;
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status == EASY_ERROR);

    // parse
    m->user_data = NULL;
    m->status = 0;
    m->c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));
    size = sprintf(m->input->pos, "GET http://1.taobao.com/淘宝/a.html?a=y&b=c#test");
    m->input->last = m->input->pos + size;
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, " HTTP/1.1\r\nHost: x");
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, "yz.taobao");
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, ".com\r\nConnection: close\r\n\r\n");
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);
    EXPECT_TRUE(m->c->wait_close == 1);

    if (ptr) {
        easy_http_request_t     *p = (easy_http_request_t *) ptr;
        char                    *value = easy_http_get_header(p->headers_in, "Host");
        EXPECT_TRUE(memcmp(value, "xyz.taobao.com", 14) == 0);

        if (p->str_host.len > 0)
            EXPECT_TRUE(strncmp((char *)p->str_host.data, "1.taobao.com", p->str_host.len) == 0);
    }

    easy_pool_destroy(pool);
}

TEST(easy_http_hander, server_on_encode)
{
    easy_pool_t             *pool;
    easy_request_t          *r;
    easy_http_request_t     *hr;
    easy_buf_t              *b, *b1;

    easy_log_level = (easy_log_level_t)0;

    // raw header
    {
        pool = easy_pool_create(0);
        r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
        hr = (easy_http_request_t *)easy_pool_calloc(pool, sizeof(easy_http_request_t));
        r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
        r->ms->pool = pool;
        r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
        easy_list_init(&r->ms->c->output);
        easy_list_init(&hr->output);
        b = easy_buf_create(pool, 1);
        hr->is_raw_header = 1;
        easy_list_add_tail(&b->node, &hr->output);
        easy_http_server_on_encode(r, hr);
        b1 = easy_list_get_first(&r->ms->c->output, easy_buf_t, node);
        EXPECT_TRUE(b1 == b);
        easy_pool_destroy(pool);
    }

    // status line
    {
        pool = easy_pool_create(0);
        r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
        hr = (easy_http_request_t *)easy_pool_calloc(pool, sizeof(easy_http_request_t));
        r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
        r->ms->pool = pool;
        r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
        easy_list_init(&r->ms->c->output);
        easy_list_init(&hr->output);
        easy_buf_string_set(&hr->status_line, EASY_HTTP_STATUS_400);
        hr->headers_out = easy_header_create_table(pool);
        easy_http_add_header(pool, hr->headers_out, "Host", "a.taobao.com");
        hr->parser.http_major = hr->parser.http_minor = 1;

        easy_http_server_on_encode(r, hr);
        b = easy_list_get_first(&r->ms->c->output, easy_buf_t, node);
        EXPECT_TRUE(memcmp(b->pos, "HTTP/1.1 400 Bad Request\r\nHost: a.taobao.com\r\n", 41) == 0);
        easy_pool_destroy(pool);
    }
}

TEST(easy_http_hander, client_on_encode)
{
    easy_pool_t             *pool;
    easy_request_t          *r;
    easy_http_packet_t      *hr;
    easy_buf_t              *b, *b1;
    char                    *ptr;

    easy_log_level = (easy_log_level_t)0;

    // raw header
    {
        pool = easy_pool_create(0);
        r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
        hr = (easy_http_packet_t *)easy_pool_calloc(pool, sizeof(easy_http_packet_t));
        r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
        r->ms->pool = pool;
        r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
        easy_list_init(&r->ms->c->output);
        easy_list_init(&hr->output);
        b = easy_buf_create(pool, 1);
        hr->is_raw_header = 1;
        easy_list_add_tail(&b->node, &hr->output);
        easy_http_client_on_encode(r, hr);
        b1 = easy_list_get_first(&r->ms->c->output, easy_buf_t, node);
        EXPECT_TRUE(b1 == b);
        easy_pool_destroy(pool);
    }

    // post
    {
        pool = easy_pool_create(0);
        r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
        hr = (easy_http_packet_t *)easy_pool_calloc(pool, sizeof(easy_http_packet_t));
        r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
        r->ms->pool = pool;
        r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
        easy_list_init(&r->ms->c->output);
        easy_list_init(&hr->output);

        hr->method = HTTP_POST;
        hr->keep_alive = 1;
        hr->str_path.data = NULL;
        easy_buf_string_set(&hr->str_query_string, "a=y&c=d");
        hr->headers_out = easy_header_create_table(pool);
        easy_http_add_header(pool, hr->headers_out, "Server", "apache");

        easy_http_client_on_encode(r, hr);
        b = easy_list_get_first(&r->ms->c->output, easy_buf_t, node);
        ptr = "POST / HTTP/1.1\r\nServer: apache\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\na=y&c=d";

        EXPECT_TRUE(memcmp(b->pos, ptr, strlen(ptr)) == 0);
        easy_pool_destroy(pool);
    }

    // get
    {
        pool = easy_pool_create(0);
        r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
        hr = (easy_http_packet_t *)easy_pool_calloc(pool, sizeof(easy_http_packet_t));
        r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
        r->ms->pool = pool;
        r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
        easy_list_init(&r->ms->c->output);
        easy_list_init(&hr->output);

        hr->str_path.data = NULL;
        easy_buf_string_set(&hr->str_query_string, "a=y&c=d");
        hr->headers_out = easy_header_create_table(pool);
        easy_http_add_header(pool, hr->headers_out, "Server", "apache");

        easy_http_client_on_encode(r, hr);
        b = easy_list_get_first(&r->ms->c->output, easy_buf_t, node);

        EXPECT_TRUE(memcmp(b->pos, "GET /?a=y&c=d HTTP/1.1\r\nServer: apache\r\n", 40) == 0);
        easy_pool_destroy(pool);
    }
}

TEST(easy_http_hander, client_on_decode)
{
    easy_pool_t             *pool;
    easy_message_t          *m;
    int                     size;
    void                    *ptr;
    easy_io_handler_pt      handler;

    easy_http_handler_init(&handler, NULL);

    // failure
    easy_log_level = (easy_log_level_t)0;
    pool = easy_pool_create(0);
    m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    size = pool->end - pool->last - sizeof(easy_buf_t);
    size = ((size < 0) ? EASY_POOL_ALIGNMENT : size);
    m->input = easy_buf_create(pool, size);
    m->pool = pool;

    easy_pool_set_allocator(test_realloc);
    ptr = easy_http_client_on_decode(m);
    easy_pool_set_allocator(easy_test_realloc);
    EXPECT_TRUE(ptr == NULL);

    // null string
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr == NULL);

    // failure http
    size = sprintf(m->input->pos, "XXX http://1.taobao.com/ HTTP/1.1");
    m->input->last = m->input->pos + size;
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status == EASY_ERROR);

    // parse
    m->user_data = NULL;
    m->status = 0;
    m->c = (easy_connection_t *) easy_pool_calloc(pool, sizeof(easy_connection_t));
    m->c->handler = &handler;
    size = sprintf(m->input->pos, "HTTP/1.1 200");
    m->input->last = m->input->pos + size;
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, " OK\r\nHost: x");
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, "yz.taobao");
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr == NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);

    m->input->last += sprintf(m->input->last, ".com\r\nConnection: close\r\nContent-Length: 2\r\n\r\nab");
    ptr = easy_http_client_on_decode(m);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_TRUE(m->status != EASY_ERROR);
    EXPECT_TRUE(m->c->wait_close == 1);

    if (ptr) {
        easy_http_request_t     *p = (easy_http_request_t *) ptr;
        char                    *value = easy_http_get_header(p->headers_in, "Host");
        EXPECT_TRUE(memcmp(value, "xyz.taobao.com", 14) == 0);
    }

    easy_pool_destroy(pool);
}

TEST(easy_http_hander, init)
{
    easy_io_handler_pt      handler;
    easy_http_handler_init(&handler, NULL);
    EXPECT_TRUE(handler.decode == easy_http_server_on_decode);
    EXPECT_TRUE(handler.encode = easy_http_server_on_encode);
}

//int easy_url_decode(char *str, int len)
TEST(easy_http_hander, url_decode)
{
    char                    data[128], data1[28];
    strcpy(data, "HTTP://a.taobao.com/%26%RR%0");
    strcpy(data1, "HTTP://a.taobao.com/&%RR%0");
    int                     n = easy_url_decode(data, strlen(data));
    EXPECT_TRUE(n > 0);
    EXPECT_TRUE(strcmp(data, data1) == 0);
}

//int easy_http_merge_path(char *newpath, int len, const char *rootpath, const char *addpath)
TEST(easy_http_hander, merge_path)
{
    /*
    char                    newpath[128];
    memset(newpath, 0, sizeof(newpath));
    int                     n;
    n = easy_http_merge_path(newpath, 128, "/root", "..");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_NE(n, EASY_OK);
    n = easy_http_merge_path(newpath, 128, "/root", "../");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_NE(n, EASY_OK);
    n = easy_http_merge_path(newpath, 128, "/root", "aa/..");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_EQ(n, EASY_OK);
    n = easy_http_merge_path(newpath, 128, "/root", "aa/../");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_EQ(n, EASY_OK);
    n = easy_http_merge_path(newpath, 128, "/root", "aa/../.");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_EQ(n, EASY_OK);
    n = easy_http_merge_path(newpath, 128, "/root", "aa/../..");
    fprintf(stderr, "%s\n", newpath);
    EXPECT_NE(n, EASY_OK);
    */
}

//int easy_http_request_printf(easy_http_request_t *r, const char *fmt, ...)
TEST(easy_http_hander, printf)
{
    easy_pool_t             *pool;
    easy_http_request_t     r;
    easy_buf_t              *b;

    pool = easy_pool_create(0);
    r.m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    r.m->pool = pool;
    easy_list_init(&r.output);
    easy_http_request_printf(&r, "a: %04d, s: %s", 123, "test");

    b = easy_list_get_first(&r.output, easy_buf_t, node);
    char                    *str = "a: 0123, s: test";
    EXPECT_TRUE(strncmp(b->pos, str, strlen(str)) == 0);

    easy_pool_destroy(pool);
}

TEST(easy_http_hander, args)
{
    easy_pool_t             *pool;
    easy_http_request_t     *p;

    easy_header_set_case_sensitive(0);
    pool = easy_pool_create(0);
    p = (easy_http_request_t *)easy_pool_calloc(pool, sizeof(easy_http_request_t));
    memset(p, 0, sizeof(easy_http_request_t));
    p->m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    p->m->pool = pool;
    easy_buf_string_set(&p->str_query_string, "a=b&c=d&x==&&=&Test=abc&a");

    char                    *x = easy_http_get_args(p, "test");
    EXPECT_TRUE(x != NULL);

    if (x != NULL) EXPECT_TRUE(strcmp(x, "abc") == 0);

    easy_pool_destroy(pool);
}

static int http_fetch_set_data(easy_request_t *r, const char *data, int len)
{
    easy_buf_t              *b = r->args;

    memcpy(b->last, data, len);
    b->last += len;

    return EASY_OK;
}

TEST(easy_http_hander, server_on_encode_chunked)
{
    easy_pool_t             *pool;
    easy_request_t          *r;
    easy_http_request_t     *hr;
    easy_buf_t              *b;
    easy_message_t          *m;
    easy_session_t          *s;
    easy_io_handler_pt      io_handler;

    // init
    easy_log_level = (easy_log_level_t)0;
    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.set_data = (void *)http_fetch_set_data;

    pool = easy_pool_create(0);
    r = (easy_request_t *)easy_pool_calloc(pool, sizeof(easy_request_t));
    hr = (easy_http_request_t *)easy_pool_calloc(pool, sizeof(easy_http_request_t));
    r->ms = (easy_message_session_t *)easy_pool_calloc(pool, sizeof(easy_message_session_t));
    r->ms->pool = pool;
    r->ms->c = (easy_connection_t *)easy_pool_calloc(pool, sizeof(easy_connection_t));
    easy_list_init(&r->ms->c->output);
    easy_list_init(&hr->output);
    r->ms->c->handler = &io_handler;
    r->ms->c->send_queue = easy_hash_create(pool, 32, offsetof(easy_session_t, send_queue_hash));

    // encode chunked
    easy_buf_string_set(&hr->status_line, EASY_HTTP_STATUS_200);
    hr->headers_out = easy_header_create_table(pool);
    easy_http_add_header(pool, hr->headers_out, "hOst", "a.taobao.com");
    easy_http_add_header(pool, hr->headers_out, "Transfer-Encoding", "chunked");
    hr->parser.http_major = hr->parser.http_minor = 1;
    hr->send_chunk = 1;
    r->retcode = EASY_AGAIN;

    b = easy_buf_create(pool, 32);
    b->last = easy_strcpy(b->last, "test server on");
    easy_list_add_tail(&b->node, &hr->output);
    easy_http_server_on_encode(r, hr);
    hr->is_raw_header = 1;

    b = easy_buf_create(pool, 32);
    b->last = easy_strcpy(b->last, " encode ");
    easy_list_add_tail(&b->node, &hr->output);
    easy_http_server_on_encode(r, hr);

    b = easy_buf_create(pool, 32);
    b->last = easy_strcpy(b->last, "chunked");
    easy_list_add_tail(&b->node, &hr->output);
    r->retcode = EASY_OK;
    easy_http_server_on_encode(r, hr);

    // output encode
    m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    m->pool = pool;
    m->c = r->ms->c;
    m->input = easy_buf_create(pool, 1024);
    easy_list_for_each_entry(b, &r->ms->c->output, node) {
        m->input->last += lnprintf(m->input->last, 1024, "%.*s", (int)(b->last - b->pos), b->pos);
    }

    // decode chunked
    easy_header_set_case_sensitive(2);
    s = easy_session_create(512);
    s->r.args = easy_buf_create(pool, 1024);
    s->packet_id = easy_connection_get_packet_id(m->c, NULL, 0);
    easy_hash_dlist_add(m->c->send_queue, s->packet_id, &s->send_queue_hash, &s->send_queue_list);
    hr = easy_http_client_on_decode(m);

    if (hr) {
        char                    *value = easy_http_get_header(hr->headers_in, "Host");
        EXPECT_TRUE(value);
    }

    b = s->r.args;
    EXPECT_TRUE(hr);
    EXPECT_TRUE(m->status == EASY_OK);
    EXPECT_TRUE(memcmp(b->pos, "test server on encode chunked", strlen("test server on encode chunked")) == 0);
    easy_header_set_case_sensitive(0);

    easy_session_destroy(s);
    easy_pool_destroy(pool);
}

easy_http_request_t  *easy_test_query_string_decode(easy_message_t *m, char *query_str)
{
    easy_http_request_t     *ptr;
    int                     size;

    // parse
    m->input = easy_buf_create(m->pool, 4096);
    m->user_data = NULL;
    m->status = 0;
    m->c = (easy_connection_t *) easy_pool_calloc(m->pool, sizeof(easy_connection_t));
    size = sprintf(m->input->pos, "GET http://1.taobao.com/淘宝/a.html%s HTTP/1.1\r\nHost: x.taobao.com\r\n\r\n", query_str);
    m->input->last = m->input->pos + size;
    ptr = easy_http_server_on_decode(m);
    EXPECT_TRUE(ptr);
    EXPECT_TRUE(m->status != EASY_ERROR);
    EXPECT_TRUE(m->c->wait_close == 0);

    return ptr;
}

#define easy_test_query_string_match(p, query_str) do { \
        EXPECT_TRUE(p); \
        if (p == NULL)  break; \
        EXPECT_EQ(strlen(query_str), p->str_query_string.len); \
        if (p->str_query_string.len > 0) \
            EXPECT_TRUE(strncmp((char *)p->str_query_string.data, query_str, p->str_query_string.len) == 0); \
    } while(0)

TEST(easy_http_hander, decode1)
{
    easy_pool_t             *pool;
    easy_message_t          *m;
    easy_http_request_t     *ptr;

    // failure
    easy_log_level = (easy_log_level_t)0;
    pool = easy_pool_create(0);
    m = (easy_message_t *)easy_pool_calloc(pool, sizeof(easy_message_t));
    m->pool = pool;

    {
        // 不带问号
        char                    *query_str = "a=y&b=c";
        char                    *dst_query_str = "";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        //  一个问号 ?a
        char                    *query_str = "?a=y&b=c";
        char                    *dst_query_str = query_str + 1;

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 一个问号不带参数?
        char                    *query_str = "?";
        char                    *dst_query_str = "";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 两个问号
        char                    *query_str = "??a=y&b=c";
        char                    *dst_query_str = query_str + 1;

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 两个问号不带参数
        char                    *query_str = "??";
        char                    *dst_query_str = "?";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 3个问号不带参数
        char                    *query_str = "???";
        char                    *dst_query_str = query_str + 1;

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 问号中间有字符
        char                    *query_str = "?a=y&b=c?111";
        char                    *dst_query_str = "a=y&b=c?111";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 问号中间有字符，不带参数
        char                    *query_str = "?a=y&b=c?";
        char                    *dst_query_str = "a=y&b=c?";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 井号后面没有问号
        char                    *query_str = "?a=y&b=c#abc";
        char                    *dst_query_str = "a=y&b=c";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 井号后面有问号
        char                    *query_str = "?a=y&b=c#abc??111";
        char                    *dst_query_str = "a=y&b=c";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 问号后面是井号井号后面有问号
        char                    *query_str = "?#abc??111";
        char                    *dst_query_str = "";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 井号后面有问号
        char                    *query_str = "#?abc??111";
        char                    *dst_query_str = "";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }

    {
        // 井号后面有问号
        char                    *query_str = "#222?abc??111";
        char                    *dst_query_str = "";

        ptr = easy_test_query_string_decode(m, query_str);

        easy_test_query_string_match(ptr, dst_query_str);
    }
    easy_pool_destroy(pool);
}

static http_parser_settings test_easy_http_settings = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
};

TEST(easy_http_handler, parser)
{
    char                    buffer[1024];
    int                     size, n, i;
    int                     errnum = 0;
    int                     oknum = 0;
    int                     cnt = 10000 * 200;

    strcpy(buffer, "GET /index.php/0000000000/index.php/0000000000.html HTTP/1.0\r\nUser-Agent: ApacheBench/2.0.40-dev\r\nHost: test.taobao.com\r\nAccept: */*\r\n");
    size = strlen(buffer);

    int64_t                 t1 = easy_time_now();

    for(i = 0; i < cnt; i++) {
        http_parser             parser;
        http_parser_init(&parser, HTTP_REQUEST);
        n = http_parser_execute(&parser, &test_easy_http_settings, buffer, size);

        if (http_parser_has_error(&parser) || n < 0) {
            errnum ++;
        } else {
            oknum ++;
        }
    }

    int64_t                 t2 = easy_time_now();

    fprintf(stderr, "t: %" PRId64 ", speed: %.2f\n", (t2 - t1), cnt * 1000000.0 / (t2 - t1));
    EXPECT_TRUE(errnum == 0);
    EXPECT_TRUE(oknum == cnt);
}

///////////////////////////////////////////////////////////////////////////////////////////////
static easy_thread_pool_t *test_thread_101_server_tp;
static easy_list_t test_thread_101_list = EASY_LIST_HEAD_INIT(test_thread_101_list);
static int test_thread_101_server_process(easy_request_t *r)
{
    easy_thread_pool_push(test_thread_101_server_tp, r, easy_hash_key((uint64_t)(long)r));
    return EASY_AGAIN;
}
static int test_thread_101_request_process(easy_request_t *r, void *args)
{
    easy_list_add_tail(&r->request_list_node, &test_thread_101_list);

    easy_http_request_t *p;
    p = (easy_http_request_t *)r->ipacket;

    if (p->str_path.len != 5 || memcmp(p->str_path.data, "/done", 5) != 0) {
        return EASY_ABORT;
    }

    // reply
    easy_request_t *n1, *n2;
    easy_list_for_each_entry_safe(n1, n2, &test_thread_101_list, request_list_node) {
        easy_list_del(&n1->request_list_node);
        p = (easy_http_request_t *)n1->ipacket;
        n1->opacket = p;
        easy_http_request_printf(p, "%s", p->str_path.data);
        easy_request_wakeup(n1);
    }
    easy_list_init(&test_thread_101_list);

    return EASY_ABORT;
}
static void test_thread_101_server(int fd)
{
    easy_listen_t           *l;
    easy_io_handler_pt      io_handler;
    int                     i, port;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = test_thread_101_server_process;

    test_thread_101_server_tp = easy_request_thread_create(1, test_thread_101_request_process, NULL);
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
static int test_thread_101_disconnect(easy_connection_t *c)
{
    easy_io_stop();
    return EASY_OK;
}
static int test_thread_101_connect(easy_connection_t *c)
{
    int retv = EASY_OK;
    char buffer[65536];
    char text[20000];
    int i, ret, idx = 0;
    memset(text, 'A', sizeof(text));
    text[sizeof(text) - 1] = '\0';
    text[2] = '=';

    for(i = 0; i < 3; i++) {
        idx += lnprintf(buffer + idx, 30000,
                        "POST /request_%d HTTP/1.1\r\nContent-Length: %d\r\n"
                        "Content-Type: application/x-www-form-urlencoded\r\n"
                        "Host: test.com\r\nConnection: keep-alive\r\n\r\n%s",
                        i, (int)strlen(text), text);
    }

    idx += lnprintf(buffer + idx, 4000,
                    "GET /done HTTP/1.1\r\n"
                    "Host: test.com\r\n"
                    "Connection: keep-alive\r\n\r\n");
    buffer[idx] = '\0';

    char *p = buffer;

    while(idx > 0) {
        i = easy_min(idx, 1000);
        ret = write(c->fd, p, i);

        if (ret > 0) {
            p += ret;
            idx -= ret;
        } else if (errno != EAGAIN && errno != EINTR) {
            easy_error_log("write failure: %d:%s\n", errno, strerror(errno));
            retv = EASY_ERROR;
            break;
        }

        usleep(100);
    }

    return retv;
}
static int test_thread_101_client_process(easy_request_t *r)
{
    return EASY_OK;
}
static void test_thread_101_client(int port)
{
    easy_io_handler_pt      io_handler;

    easy_log_level = (easy_log_level_t)0;
    easy_io_create(4);
    easy_io_start();

    memset(&io_handler, 0, sizeof(easy_io_handler_pt));
    io_handler.decode = easy_http_server_on_decode;
    io_handler.encode = easy_http_server_on_encode;
    io_handler.process = test_thread_101_client_process;
    io_handler.on_connect = test_thread_101_connect;
    io_handler.on_disconnect = test_thread_101_disconnect;

    // connect
    easy_addr_t addr = easy_inet_str_to_addr("localhost", port);
    int i = easy_io_connect(addr, &io_handler, 0, NULL);
    EXPECT_TRUE(i == EASY_OK);

    easy_io_wait();
    easy_io_destroy();
}
TEST(easy_http_hander, thread_101)
{
    int                     pfd[2];
    pid_t                   pid1, pid2;
    int                     ret = 0;
    int                     port = 0;

    if (pipe(pfd) == -1) return;

    // server
    if ((pid1 = fork()) == 0) {
        close(pfd[0]);
        test_thread_101_server(pfd[1]);
        close(pfd[1]);
        exit(easy_test_retval);
    } else {
        close(pfd[1]);
        ret = read(pfd[0], &port, 2);
        close(pfd[0]);
    }

    // client
    if ((pid2 = fork()) == 0) {
        test_thread_101_client(port);
        exit(easy_test_retval);
    }

    waitpid(pid2, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
    kill(pid1, SIGINT);
    usleep(1000);
    waitpid(pid1, &ret, 0);
    EXPECT_EQ(WEXITSTATUS(ret), 0);
}
