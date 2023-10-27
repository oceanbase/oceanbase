/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

void fixed_queue_init(fixed_queue_t* q, void* buf, int64_t bytes)
{
  q->push = 0;
  q->pop = 0;
  q->data = (void**)buf;
  q->capacity = bytes/sizeof(void*);
  memset(buf, 0, bytes);
}

extern int fixed_queue_push(fixed_queue_t* q, void* p);
extern void* fixed_queue_pop(fixed_queue_t* q);
