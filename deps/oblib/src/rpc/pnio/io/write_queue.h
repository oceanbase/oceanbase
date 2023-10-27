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

#define BUCKET_SIZE    1024
typedef struct write_queue_t {
  dqueue_t queue;
  int64_t pos;
  int64_t cnt;
  int64_t sz;
  int16_t categ_count_bucket[BUCKET_SIZE];
} write_queue_t;

extern void wq_init(write_queue_t* wq);
extern void wq_push(write_queue_t* wq, dlink_t* l);
extern int wq_flush(sock_t* s, write_queue_t* wq, dlink_t** old_head);
extern int wq_delete(write_queue_t* wq, dlink_t* l);
