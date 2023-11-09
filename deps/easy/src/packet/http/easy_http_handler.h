#ifndef EASY_HTTP_HANDLER_H_
#define EASY_HTTP_HANDLER_H_

#include "easy_define.h"
#include "util/easy_string.h"

/**
 * 对ＨＴＴＰ的流解析
 */
EASY_CPP_START

#include "packet/http/http_parser.h"
#include "io/easy_io.h"

#define EASY_HTTP_HDR_MAX_SIZE          128
#define EASY_HTTP_CRLF                  "\r\n"

// http status code
#define EASY_HTTP_STATUS_200            "200 OK"
#define EASY_HTTP_STATUS_201            "201 Created"
#define EASY_HTTP_STATUS_202            "202 Accepted"
#define EASY_HTTP_STATUS_204            "204 No Content"
#define EASY_HTTP_STATUS_206            "206 Partial Content"
#define EASY_HTTP_STATUS_301            "301 Moved Permanently"
#define EASY_HTTP_STATUS_302            "302 Moved Temporarily"
#define EASY_HTTP_STATUS_303            "303 See Other"
#define EASY_HTTP_STATUS_304            "304 Not Modified"
#define EASY_HTTP_STATUS_400            "400 Bad Request"
#define EASY_HTTP_STATUS_401            "401 Unauthorized"
#define EASY_HTTP_STATUS_402            "402 Payment Required"
#define EASY_HTTP_STATUS_403            "403 Forbidden"
#define EASY_HTTP_STATUS_404            "404 Not Found"
#define EASY_HTTP_STATUS_405            "405 Not Allowed"
#define EASY_HTTP_STATUS_406            "406 Not Acceptable"
#define EASY_HTTP_STATUS_408            "408 Request Time-out"
#define EASY_HTTP_STATUS_409            "409 Conflict"
#define EASY_HTTP_STATUS_410            "410 Gone"
#define EASY_HTTP_STATUS_411            "411 Length Required"
#define EASY_HTTP_STATUS_412            "412 Precondition Failed"
#define EASY_HTTP_STATUS_413            "413 Request Entity Too Large"
#define EASY_HTTP_STATUS_415            "415 Unsupported Media Type"
#define EASY_HTTP_STATUS_416            "416 Requested Range Not Satisfiable"
#define EASY_HTTP_STATUS_500            "500 Internal Server Error"
#define EASY_HTTP_STATUS_501            "501 Method Not Implemented"
#define EASY_HTTP_STATUS_502            "502 Bad Gateway"
#define EASY_HTTP_STATUS_503            "503 Service Temporarily Unavailable"
#define EASY_HTTP_STATUS_504            "504 Gateway Time-out"
#define EASY_HTTP_STATUS_507            "507 Insufficient Storage"

typedef struct easy_http_request_t      easy_http_request_t;
typedef struct easy_http_packet_t       easy_http_packet_t;

struct easy_http_request_t {
    easy_message_t          *m;
    http_parser             parser;

    easy_buf_string_t       str_path;
    easy_buf_string_t       str_query_string;
    easy_buf_string_t       str_fragment;
    easy_buf_string_t       str_body;
    easy_buf_string_t       str_proto;
    easy_buf_string_t       str_host;

    easy_hash_string_t      *headers_in;
    easy_hash_string_t      *headers_out;
    easy_hash_string_t      *args_table;
    easy_string_pair_t      *last_header, *end_header;

    // response
    easy_buf_string_t       status_line;
    easy_list_t             output;
    easy_buf_string_t       content_type;
    int64_t                 content_length;

    // flags
    unsigned int            message_begin_called : 1;
    unsigned int            header_complete_called : 1;
    unsigned int            message_complete_called : 1;
    unsigned int            last_was_value : 1;
    unsigned int            is_raw_header : 1;
    unsigned int            args_parsed : 1;
    unsigned int            wait_close : 1;
    unsigned int            keep_alive : 1;
    unsigned int            send_chunk : 1;
    unsigned int            del_default_header : 1;

    int                     parsed_byte;
    void                    *user_data;
};

struct easy_http_packet_t {
    easy_buf_string_t       str_query_string;
    easy_buf_string_t       str_path;
    easy_hash_string_t      *headers_out;
    easy_list_t             output;

    enum http_method        method;

    unsigned int            is_raw_header : 1;
    unsigned int            keep_alive : 1;
    unsigned int            del_default_header : 1;
};

void easy_http_add_header(easy_pool_t *pool, easy_hash_string_t *table, const char *name, const char *value);
char *easy_http_del_header(easy_hash_string_t *table, const char *name);
char *easy_http_get_header(easy_hash_string_t *table, const char *name);

void easy_http_handler_init(easy_io_handler_pt *handler, easy_io_process_pt *process);
void *easy_http_server_on_decode(easy_message_t *m);
int easy_http_server_on_encode(easy_request_t *r, void *data);
void *easy_http_client_on_decode(easy_message_t *m);
int easy_http_client_on_encode(easy_request_t *r, void *data);
easy_http_packet_t *easy_http_packet_create (easy_pool_t *pool);

int easy_url_decode(char *str, int len);
int easy_http_merge_path(char *newpath, int len, const char *rootpath, const char *addpath);
int easy_http_request_printf(easy_http_request_t *r, const char *fmt, ...);
char *easy_http_get_args(easy_http_request_t *p, const char *name);
void easy_header_set_case_sensitive(int value);
void easy_header_set_max_size(int value);
easy_hash_string_t *easy_header_create_table(easy_pool_t *pool);
int easy_http_request_create (easy_message_t *m, enum http_parser_type type);

extern http_parser_settings easy_http_request_settings;
extern http_parser_settings easy_http_response_settings;

EASY_CPP_END

#endif
