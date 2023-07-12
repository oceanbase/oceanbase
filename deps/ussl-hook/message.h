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

#ifndef USSL_HOOK_MESSAGE_H
#define USSL_HOOK_MESSAGE_H

#include <stdint.h>

#define NEGOTIATION_MAGIC 0xbeef0312
#define MAX_EXTRA_INFO_LEN 20
#define USSL_BUF_LEN 256

typedef struct negotiation_head_t
{
  uint32_t magic;
  uint32_t version;
  uint32_t len;
} negotiation_head_t;

typedef struct negotiation_message_t
{
  int type;
  uint64_t client_gid;
} negotiation_message_t;

int send_negotiation_message(int fd, const char *b, int sz);

#endif // USSL_HOOK_MESSAGE_H