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

#ifndef EASY_REQUEST_H_
#define EASY_REQUEST_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

EASY_CPP_START

void easy_request_server_done(easy_request_t* r);
void easy_request_client_done(easy_request_t* r);
void easy_request_set_cleanup(easy_request_t* r, easy_list_t* output);
void check_easy_request_rt(easy_session_t* s);

EASY_CPP_END

#endif
