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

#ifndef EASY_INET_H_
#define EASY_INET_H_

#include "easy_define.h"

EASY_CPP_START

extern char* easy_inet_addr_to_str(easy_addr_t* addr, char* buffer, int len);
extern easy_addr_t easy_inet_str_to_addr(const char* host, int port);
extern int easy_inet_parse_host(easy_addr_t* address, const char* host, int port);
extern int easy_inet_is_ipaddr(const char* host);
extern int easy_inet_hostaddr(uint64_t* address, int size, int local);
extern easy_addr_t easy_inet_add_port(easy_addr_t* addr, int diff);
extern easy_addr_t easy_inet_getpeername(int s);
extern void easy_inet_atoe(void* a, easy_addr_t* e);
extern void easy_inet_etoa(easy_addr_t* e, void* a);
extern int easy_inet_myip(easy_addr_t* addr);

EASY_CPP_END

#endif
