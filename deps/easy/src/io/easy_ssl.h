/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef EASY_SSL_H_
#define EASY_SSL_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

EASY_CPP_START

#define EASY_SSL_SCACHE_BUILTIN 0
#define EASY_SSL_SCACHE_OFF 1
typedef struct easy_ssl_ctx_server_t {
  easy_hash_list_t node;
  char* server_name;
  easy_ssl_ctx_t* ss;
} easy_ssl_ctx_server_t;

typedef struct easy_ssl_pass_phrase_dialog_t {
  char* type;
  char* server_name;
} easy_ssl_pass_phrase_dialog_t;

#define easy_ssl_get_connection(s) SSL_get_ex_data((SSL*)s, easy_ssl_connection_index)
extern int easy_ssl_connection_index;

// function
int easy_ssl_connection_create(easy_ssl_ctx_t* ssl, easy_connection_t* c);
int easy_ssl_connection_destroy(easy_connection_t* c);
void easy_ssl_connection_handshake(struct ev_loop* loop, ev_io* w, int revents);
int easy_ssl_client_do_handshake(easy_connection_t* c);
void easy_ssl_client_handshake(struct ev_loop* loop, ev_io* w, int revents);

EASY_CPP_END

#endif
