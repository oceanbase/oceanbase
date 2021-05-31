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

#ifndef EASY_MESSAGE_H_
#define EASY_MESSAGE_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

/**
 * receive message
 */

EASY_CPP_START

easy_message_t* easy_message_create(easy_connection_t* c);
easy_message_t* easy_message_create_nlist(easy_connection_t* c);
int easy_message_destroy(easy_message_t* m, int del);
int easy_session_process(easy_session_t* s, int stop, int err);
int easy_session_process_keep_connection_resilient(easy_session_t* s, int stop, int err);

EASY_CPP_END

#endif
