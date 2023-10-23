#include <ctype.h>
#include "io/easy_connection.h"
#include "packet/http/easy_http_handler.h"

static int easy_http_request_message_begin(http_parser *parser);
static int easy_http_request_headers_complete(http_parser *parser);
static int easy_http_request_message_complete(http_parser *parser);
static int easy_http_request_on_path(http_parser *, const char *, size_t);
static int easy_http_request_on_proto(http_parser *, const char *, size_t);
static int easy_http_request_on_query_string(http_parser *, const char *, size_t);
static int easy_http_request_on_fragment(http_parser *, const char *, size_t);
static int easy_http_request_on_body(http_parser *, const char *, size_t);
static int easy_http_request_on_host(http_parser *, const char *, size_t);
static int easy_http_request_on_header_field (http_parser *, const char *, size_t);
static int easy_http_request_on_header_value (http_parser *, const char *, size_t);
static void easy_http_string_end(easy_buf_string_t *s);
static void easy_http_header_string_end(easy_http_request_t *p);
static void easy_http_parse_args(easy_http_request_t *p);
static void easy_http_add_args(easy_http_request_t *p, char *p1, char *p2, char *p3);
static int easy_http_encode_chunk(easy_request_t *r, int64_t chunk_length);
static int easy_header_case_sensitive = 0;
static int easy_http_max_header_size = HTTP_MAX_HEADER_SIZE;

/**
 * easy http request settings
 */
http_parser_settings easy_http_request_settings = {
    easy_http_request_message_begin,
    easy_http_request_on_path,
    easy_http_request_on_query_string,
    NULL,
    easy_http_request_on_proto,
    easy_http_request_on_host,
    easy_http_request_on_fragment,
    easy_http_request_on_header_field,
    easy_http_request_on_header_value,
    easy_http_request_headers_complete,
    easy_http_request_on_body,
    easy_http_request_message_complete
};
http_parser_settings easy_http_response_settings = {
    easy_http_request_message_begin,
    NULL, NULL, NULL, NULL, NULL, NULL,
    easy_http_request_on_header_field,
    easy_http_request_on_header_value,
    easy_http_request_headers_complete,
    easy_http_request_on_body,
    easy_http_request_message_complete
};

static void easy_http_string_end(easy_buf_string_t *s)
{
    char                    *ptr;

    if (s->len && (ptr = s->data + s->len)) {
        *ptr = '\0';
    }
}

/**
 * 把header对加入到table中
 */
void easy_http_add_header(easy_pool_t *pool, easy_hash_string_t *table,
                          const char *name, const char *value)
{
    easy_string_pair_t      *header;

    header = (easy_string_pair_t *)easy_pool_alloc(pool, sizeof(easy_string_pair_t));
    easy_buf_string_set(&header->name, name);
    easy_buf_string_set(&header->value, value);

    easy_hash_string_add(table, header);
}

/**
 * 删除header
 */
char *easy_http_del_header(easy_hash_string_t *table, const char *name)
{
    easy_string_pair_t      *t;

    if ((t = easy_hash_string_del(table, name, strlen(name))) != NULL)
        return easy_buf_string_ptr(&t->value);

    return NULL;
}

/**
 * 得到一header的value
 */
char *easy_http_get_header(easy_hash_string_t *table, const char *name)
{
    easy_string_pair_t      *t;

    if ((t = easy_hash_string_get(table, name, strlen(name))) != NULL)
        return easy_buf_string_ptr(&t->value);

    return NULL;
}

/**
 * 得到args的参数
 */
char *easy_http_get_args(easy_http_request_t *p, const char *name)
{
    if (p->args_parsed == 0) {
        easy_http_parse_args(p);
    }

    if (p->args_table) {
        return easy_http_get_header(p->args_table, name);
    }

    return NULL;
}

// http parser callback
static int easy_http_request_message_begin(http_parser *parser)
{
    easy_http_request_t     *p = (easy_http_request_t *) parser->data;
    p->message_begin_called = 1;
    return 0;
}
static int easy_http_request_headers_complete(http_parser *parser)
{
    easy_http_request_t     *p = (easy_http_request_t *) parser->data;
    p->header_complete_called = 1;
    p->content_length = 0;
    easy_http_header_string_end(p);

    if (p->m->c->send_queue && p->m->c->send_queue->flags == HTTP_HEAD)
        return 1;
    else
        return 0;
}
static int easy_http_request_message_complete(http_parser *parser)
{
    easy_http_request_t     *p = (easy_http_request_t *) parser->data;
    p->message_complete_called = 1;
    return 1;
}

#define EASY_HTTP_REQUEST_CB_DEFINE(name)                                       \
    static int easy_http_request_on_##name(http_parser *parser,                 \
                                           const char *value, size_t len)  {    \
        easy_http_request_t     *p;                                             \
        p = (easy_http_request_t*) parser->data;                                \
        easy_buf_string_append(&p->str_##name, value, len);                     \
        return 0;                                                               \
    }

EASY_HTTP_REQUEST_CB_DEFINE(path);
EASY_HTTP_REQUEST_CB_DEFINE(proto);
EASY_HTTP_REQUEST_CB_DEFINE(query_string);
EASY_HTTP_REQUEST_CB_DEFINE(fragment);
EASY_HTTP_REQUEST_CB_DEFINE(host);

static int easy_http_request_on_body(http_parser *parser, const char *value, size_t len)
{
    easy_http_request_t     *p;
    easy_connection_t       *c;
    uint64_t                packet_id;
    easy_session_t          *s;

    p = (easy_http_request_t *) parser->data;
    c = p->m->c;

    if (c->handler->set_data) {
        packet_id = easy_connection_get_packet_id(c, NULL, 1);
        s = (easy_session_t *) easy_hash_find(c->send_queue, packet_id);

        if (s == NULL) {
            easy_warn_log("%s, packet_id=%ld\n", easy_connection_str(c), packet_id);
            return 1;
        }

        // set_data
        ev_timer_again(c->loop, &s->timeout_watcher);
        s->r.ipacket = p;
        p->content_length += len;
        (*c->handler->set_data)(&s->r, value, len);
    } else {
        easy_buf_string_append(&p->str_body, value, len);
    }

    return 0;
}

static int easy_http_request_on_header_field (http_parser *parser, const char *value, size_t len)
{
    easy_http_request_t         *p;

    p = (easy_http_request_t *) parser->data;

    // new name
    if (p->last_was_value) {
        if (p->last_header == NULL || p->last_header == p->end_header) {
            p->last_header = (easy_string_pair_t *) easy_pool_calloc(p->m->pool, sizeof(easy_string_pair_t) * 4);
            p->end_header = p->last_header + 3;
        } else {
            p->last_header ++;
        }
    }

    easy_buf_string_append(&p->last_header->name, value, len);
    p->last_was_value = 0;
    return 0;
}

static int easy_http_request_on_header_value (http_parser *parser, const char *value, size_t len)
{
    easy_http_request_t         *p;

    p = (easy_http_request_t *) parser->data;

    if (p->headers_in->count > EASY_HTTP_HDR_MAX_SIZE)
        return 1;

    // new value
    if (!p->last_was_value) {
        if (easy_header_case_sensitive == 2)
            easy_string_capitalize(p->last_header->name.data, p->last_header->name.len);

        easy_hash_string_add(p->headers_in, p->last_header);
    }

    easy_buf_string_append(&p->last_header->value, value, len);
    p->last_was_value = 1;
    return 0;
}

/**
 * 新创一个easy_http_request_t
 */
int easy_http_request_create (easy_message_t *m, enum http_parser_type type)
{
    easy_http_request_t     *p;

    if ((p = (easy_http_request_t *) easy_pool_calloc(m->pool, sizeof(easy_http_request_t))) == NULL)
        return EASY_ERROR;

    http_parser_init(&p->parser, type);
    p->parser.data = p;
    p->last_was_value = 1;
    p->m = m;
    p->content_length = -1;
    m->user_data = p;
    easy_list_init(&p->output);
    p->headers_in = easy_header_create_table(m->pool);
    p->headers_out = easy_header_create_table(m->pool);

    return EASY_OK;
}

/**
 * easy io callback
 * 用于server端
 */
void *easy_http_server_on_decode(easy_message_t *m)
{
    easy_http_request_t     *p;
    char                    *plast;
    int                     n, size, hcc;

    // create http request
    if (m->user_data == NULL && easy_http_request_create(m, HTTP_REQUEST) == EASY_ERROR) {
        easy_error_log("easy_http_request_create failure\n");
        m->status = EASY_ERROR;
        return NULL;
    }

    p = (easy_http_request_t *) m->user_data;
    plast = m->input->pos + p->parsed_byte;

    if ((size = m->input->last - plast) <= 0) {
        return NULL;
    }

    hcc = p->header_complete_called;
    n = http_parser_execute(&p->parser, &easy_http_request_settings, plast, size);

    if (http_parser_has_error(&p->parser) || n < 0) {
        m->status = EASY_ERROR;
        return NULL;
    }

    p->parsed_byte += n;

    // 没读完
    if (p->header_complete_called == 0) {
        if (p->parsed_byte > easy_http_max_header_size)
            m->status = EASY_ERROR;

        return NULL;
    }

    if (p->message_complete_called == 0) {
        if (m->c->handler->set_data && hcc) {
            p->parsed_byte -= n;
            m->input->last -= n;
        }

        m->next_read_len = EASY_IO_BUFFER_SIZE;
        return NULL;
    }

    p->content_length += p->str_body.len;
    m->input->pos += (p->parsed_byte + 1);
    m->user_data = NULL;

    if (http_should_keep_alive(&p->parser) == 0) {
        m->c->wait_close = 1;
        p->wait_close = 1;
    } else if ((p->parser.flags & 2)) {
        p->keep_alive = 1;
    }

    return p;
}

/**
 * 响应的时候encode, 用于server端
 */
int easy_http_server_on_encode(easy_request_t *r, void *data)
{
    easy_http_request_t     *p;
    easy_buf_t              *b;
    easy_string_pair_t      *header;
    int                     size;
    int                     chunk_length;

    p = (easy_http_request_t *)data;
    chunk_length = 0;

    if (p->is_raw_header == 0) {
        // header length
        if (p->status_line.len == 0)
            easy_buf_string_set(&p->status_line, EASY_HTTP_STATUS_200);

        if (p->content_type.len == 0)
            easy_buf_string_set(&p->content_type, "text/html");

        size = 128 + p->status_line.len + p->content_type.len;
        // headers
        size += p->headers_out->count * 4;
        easy_list_for_each_entry(header, &p->headers_out->list, list) {
            size += header->name.len;
            size += header->value.len;
        }

        // "Transfer-Encoding: chunked\r\n"
        if (p->send_chunk) {
            size += sizeof("Transfer-Encoding: chunked\r\n");
        } else if (p->content_length <= 0) {
            p->content_length = easy_buf_list_len(&p->output);
        }

        // concat headers
        if ((b = easy_buf_create(r->ms->pool, size)) == NULL)
            return EASY_ERROR;

        // status line
        b->last = easy_const_strcpy(b->last, "HTTP/");
        (*b->last++) = p->parser.http_major + '0';
        (*b->last++) = '.';
        (*b->last++) = p->parser.http_minor + '0';
        (*b->last++) = ' ';
        b->last = easy_memcpy(b->last, p->status_line.data, p->status_line.len);
        b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);

        // headers
        easy_list_for_each_entry(header, &p->headers_out->list, list) {
            b->last = easy_memcpy(b->last, header->name.data, header->name.len);
            b->last = easy_const_strcpy(b->last, ": ");
            b->last = easy_memcpy(b->last, header->value.data, header->value.len);
            b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);
        }

        if (!p->del_default_header) {
            // Content-Type, Content-Length
            b->last = easy_const_strcpy(b->last, "Content-Type: ");
            b->last = easy_memcpy(b->last, p->content_type.data, p->content_type.len);

            if (p->send_chunk) {
                b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF "Transfer-Encoding: chunked");
            } else if (p->content_length >= 0) {
                b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF "Content-Length: ");
                b->last = easy_num_to_str(b->last, 32, p->content_length);
            }

            b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);

            // keep alive
            if (p->wait_close) {
                b->last = easy_const_strcpy(b->last, "Connection: close"EASY_HTTP_CRLF);
            } else if (p->keep_alive) {
                b->last = easy_const_strcpy(b->last, "Connection: keep-alive"EASY_HTTP_CRLF);
            }
        }

        // header end
        b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);

        easy_request_addbuf(r, b);
    }

    // chunk
    if (p->send_chunk) {
        chunk_length = easy_buf_list_len(&p->output);

        if (chunk_length > 0) {
            easy_http_encode_chunk(r, chunk_length);
            easy_request_addbuf_list(r, &p->output);
            easy_http_encode_chunk(r, ((r->retcode == EASY_OK) ? -2 : -1));
        }
    } else {
        easy_request_addbuf_list(r, &p->output);
    }

    return EASY_OK;
}

/**
 * 请求的时候encode, 用于client端
 */
int easy_http_client_on_encode(easy_request_t *r, void *data)
{
    easy_http_packet_t      *p;
    easy_buf_t              *b;
    easy_string_pair_t      *header;
    int                     size, length;

    p = (easy_http_packet_t *)data;

    if (r->ms->c->send_queue) {
        r->ms->c->send_queue->flags = p->method;
    }

    if (p->is_raw_header == 0) {
        length = (p->method == HTTP_POST) ? p->str_query_string.len : 0;
        size = 128 + p->str_path.len + p->str_query_string.len;
        // headers
        size += p->headers_out->count * 4;
        easy_list_for_each_entry(header, &p->headers_out->list, list) {
            size += header->name.len;
            size += header->value.len;
        }

        // concat headers
        if ((b = easy_buf_create(r->ms->pool, size)) == NULL)
            return EASY_ERROR;

        // request line
        if (p->method == HTTP_POST)
            b->last = easy_const_strcpy(b->last, "POST ");
        else if (p->method == HTTP_HEAD)
            b->last = easy_const_strcpy(b->last, "HEAD ");
        else
            b->last = easy_const_strcpy(b->last, "GET ");

        if (p->str_path.data)
            b->last = easy_memcpy(b->last, p->str_path.data, p->str_path.len);
        else
            (*b->last++) = '/';

        // query string
        if (p->method != HTTP_POST && p->str_query_string.data) {
            (*b->last++) = '?';
            b->last = easy_memcpy(b->last, p->str_query_string.data, p->str_query_string.len);
        }

        b->last = easy_const_strcpy(b->last, " HTTP/1.1"EASY_HTTP_CRLF);

        // headers
        easy_list_for_each_entry(header, &p->headers_out->list, list) {
            b->last = easy_memcpy(b->last, header->name.data, header->name.len);
            b->last = easy_const_strcpy(b->last, ": ");
            b->last = easy_memcpy(b->last, header->value.data, header->value.len);
            b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);
        }

        if (!p->del_default_header) {
            // Content-Length
            if (p->method == HTTP_POST) {
                b->last = easy_const_strcpy(b->last, "Content-Type: "
                                            "application/x-www-form-urlencoded"
                                            EASY_HTTP_CRLF "Content-Length: ");
                b->last = easy_num_to_str(b->last, 32, length);
                b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);
            }

            if (p->keep_alive) {
                b->last = easy_const_strcpy(b->last, "Connection: keep-alive"EASY_HTTP_CRLF);
            }
        }

        // header end
        b->last = easy_const_strcpy(b->last, EASY_HTTP_CRLF);

        // post data
        if (length > 0 && p->str_query_string.data) {
            b->last = easy_memcpy(b->last, p->str_query_string.data, p->str_query_string.len);
        }

        easy_request_addbuf(r, b);
    }

    easy_request_addbuf_list(r, &p->output);

    return EASY_OK;
}

void *easy_http_client_on_decode(easy_message_t *m)
{
    easy_http_request_t     *p;
    char                    *plast;
    int                     n, size, hcc;

    // create http request
    if (m->user_data == NULL && easy_http_request_create(m, HTTP_RESPONSE) == EASY_ERROR) {
        easy_error_log("easy_http_request_create failure\n");
        m->status = EASY_ERROR;
        return NULL;
    }

    p = (easy_http_request_t *) m->user_data;
    plast = m->input->pos + p->parsed_byte;

    if ((size = m->input->last - plast) < 0) {
        return NULL;
    }

    hcc = p->header_complete_called;
    n = http_parser_execute(&p->parser, &easy_http_response_settings, plast, size);

    if (http_parser_has_error(&p->parser) || n < 0) {
        m->status = EASY_ERROR;
        return NULL;
    }

    p->parsed_byte += n;

    // 没读完
    if (p->header_complete_called == 0) {
        if (p->parsed_byte > easy_http_max_header_size)
            m->status = EASY_ERROR;

        return NULL;
    }

    if (p->message_complete_called == 0) {
        if (m->c->handler->set_data && hcc) {
            p->parsed_byte -= n;
            m->input->last -= n;
        }

        m->next_read_len = EASY_IO_BUFFER_SIZE;
        return NULL;
    }

    m->input->pos += (p->parsed_byte + 1);
    m->user_data = NULL;

    if (http_should_keep_alive(&p->parser) == 0) {
        m->c->wait_close = 1;
        p->wait_close = 1;
    }

    return p;
}

/**
 * 新创一个easy_http_request_t
 */
easy_http_packet_t *easy_http_packet_create (easy_pool_t *pool)
{
    easy_http_packet_t      *p;

    p = (easy_http_packet_t *) easy_pool_calloc(pool, sizeof(easy_http_packet_t));
    p->headers_out = easy_header_create_table(pool);
    return p;
}

/**
 * 初始化handler
 */
void easy_http_handler_init(easy_io_handler_pt *handler, easy_io_process_pt *process)
{
    memset(handler, 0, sizeof(easy_io_handler_pt));
    handler->decode = easy_http_server_on_decode;
    handler->encode = easy_http_server_on_encode;
    handler->process = process;
}

//////////////////////////////////////////////////////////////////////////////////////
static inline int easy_htoi(char *ch)
{
    int                     digit;
    digit = ((ch[0] >= 'A') ? ((ch[0] & 0xdf) - 'A') + 10 : (ch[0] - '0'));
    digit                   *= 16;
    digit += ((ch[1] >= 'A') ? ((ch[1] & 0xdf) - 'A') + 10 : (ch[1] - '0'));
    return digit;
}

int easy_url_decode(char *str, int len)
{
    char                    *dest = str;
    char                    *data = str;

    while (len--) {
        if (*data == '%' && len >= 2 && isxdigit((int) * (data + 1))
                && isxdigit((int) * (data + 2))) {
            *dest = (char) easy_htoi(data + 1);
            data += 2;
            len -= 2;
        } else {
            *dest = *data;
        }

        data++;
        dest++;
    }

    *dest = '\0';
    return dest - str;
}

/**
 * 把两个path merge起来
 */
int easy_http_merge_path(char *newpath, int len, const char *rootpath, const char *addpath)
{
    const char              *p;
    char                    *u, *ue;
    int                     state, size;

    u = newpath;
    ue = u + len - 1;

    if ((size = strlen(rootpath)) >= len)
        return EASY_ERROR;

    memcpy(newpath, rootpath, size);
    newpath += size;
    u = newpath;
    ue = u + (len - size) - 1;

    if (ue > u && size > 0 && *(u - 1) != '/' && *addpath != '/') *u++ = '/';

    // addpath
    p = addpath;
    state = 0;

    while(*p) {
        if (u == ue || u < newpath)
            return EASY_ERROR;

        *u ++ = *p;

        if (*p == '/') {
            if (state) u -= state;

            if (state == 5) {
                while(u >= newpath) {
                    if(*u == '/') {
                        u++;
                        break;
                    }

                    u --;
                }
            }

            state = 1;
        } else if (state && *p == '.')
            state = (state == 5 ? 0 : (state == 2 ? 5 : 2));
        else
            state = 0;

        p ++;
    }

    *u = '\0';
    return EASY_OK;
}

int easy_http_request_printf(easy_http_request_t *r, const char *fmt, ...)
{
    int                     len;
    char                    buffer[EASY_POOL_PAGE_SIZE];
    easy_buf_t              *b;

    va_list                 args;
    va_start(args, fmt);
    len = easy_vsnprintf(buffer, EASY_POOL_PAGE_SIZE, fmt, args);
    va_end(args);
    b = easy_buf_check_write_space(r->m->pool, &r->output, len);
    memcpy(b->last, buffer, len);
    b->last += len;
    return len;
}

/**
 * add args to args_table
 */
static void easy_http_add_args(easy_http_request_t *p, char *p1, char *p2, char *p3)
{
    easy_string_pair_t      *header;

    if (p2 <= p1 && p3 <= p2 + 1)
        return;

    header = (easy_string_pair_t *)easy_pool_alloc(p->m->pool, sizeof(easy_string_pair_t));
    header->name.data = p1;
    header->name.len = p2 - p1;
    header->value.data = ++ p2;
    header->value.len = p3 - p2;
    easy_hash_string_add(p->args_table, header);
}

/**
 * parse query string
 */
static void easy_http_parse_args(easy_http_request_t *p)
{
    char                    *p1, *p2, *ptr;

    p->args_parsed = 1;

    if (p->str_query_string.len == 0)
        return;

    // dup
    ptr = (char *)easy_pool_alloc(p->m->pool, p->str_query_string.len + 1);
    memcpy(ptr, p->str_query_string.data, p->str_query_string.len);
    ptr[p->str_query_string.len] = '\0';

    if (p->args_table == NULL)
        p->args_table = easy_header_create_table(p->m->pool);

    // parser
    p2 = p1 = ptr;

    while(*ptr && p->args_table->count < EASY_HTTP_HDR_MAX_SIZE) {
        if (*ptr == '&') {
            *ptr = '\0';
            easy_http_add_args(p, p1, p2, ptr);
            p2 = p1 = ptr + 1;
        } else if (*ptr == '=') {
            *ptr = '\0';
            p2 = ptr;
        }

        ptr ++;
    }

    easy_http_add_args(p, p1, p2, ptr);
}

/**
 * 把header上的string用0结尾
 */
static void easy_http_header_string_end(easy_http_request_t *p)
{
    easy_string_pair_t      *t;

    // 把string结尾加0
    easy_http_string_end(&p->str_path);
    easy_http_string_end(&p->str_query_string);
    easy_http_string_end(&p->str_fragment);
    easy_list_for_each_entry(t, &p->headers_in->list, list) {
        easy_http_string_end(&t->name);
        easy_http_string_end(&t->value);
    }
}

static int easy_http_encode_chunk(easy_request_t *r, int64_t chunk_length)
{
    easy_buf_t              *b;

    if ((b = easy_buf_create(r->ms->pool, 32)) == NULL)
        return EASY_ERROR;

    if (chunk_length >= 0) {
        b->last += lnprintf(b->last, b->end - b->last, "%lx"EASY_HTTP_CRLF, (long)chunk_length);
    } else if (chunk_length == -2) {
        b->last += lnprintf(b->last, b->end - b->last, EASY_HTTP_CRLF"0"EASY_HTTP_CRLF EASY_HTTP_CRLF);
    } else {
        b->last += lnprintf(b->last, b->end - b->last, EASY_HTTP_CRLF);
    }

    easy_request_addbuf(r, b);

    return EASY_OK;
}

void easy_header_set_case_sensitive(int value)
{
    easy_header_case_sensitive = value;
}

easy_hash_string_t *easy_header_create_table(easy_pool_t *pool)
{
    return easy_hash_string_create(pool, EASY_HTTP_HDR_MAX_SIZE, (easy_header_case_sensitive ? 0 : 1));
}

void easy_header_set_max_size(int value)
{
    easy_http_max_header_size = value;
}

