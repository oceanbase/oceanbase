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

#ifndef EASY_CLIENT_H_
#define EASY_CLIENT_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

EASY_CPP_START

void* easy_client_list_find(easy_hash_t* table, easy_addr_t* addr);
int easy_client_list_add(easy_hash_t* table, easy_addr_t* addr, easy_hash_list_t* list);

EASY_CPP_END

#endif
