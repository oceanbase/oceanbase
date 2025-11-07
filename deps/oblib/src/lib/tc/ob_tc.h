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

#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <cstring>
const int MAX_SHARED_DEVICE_LIMIT_COUNT = 2;
struct TCLink
{
  TCLink(TCLink* n): next_(n) {}
  TCLink* next_;
};

class TCRequestOwner
{
public:
  virtual bool is_canceled() const = 0;
};

struct TCRequest
{
  TCRequest(TCRequestOwner *owner): link_(NULL), qid_(-1), bytes_(0), start_ns_(0), storage_key_(0), norm_bytes_(0), owner_(owner) {
  }
  TCRequest(int qid, int64_t bytes): link_(NULL), qid_(qid), bytes_(bytes), start_ns_(0), storage_key_(0), norm_bytes_(0), owner_(NULL) {}
  ~TCRequest() {}
  bool is_canceled() const { return (NULL != owner_) && owner_->is_canceled(); }
  TCLink link_;
  int qid_;
  int64_t bytes_;
  int64_t start_ns_;
  uint64_t storage_key_;
  int64_t norm_bytes_; // for local device
  TCRequestOwner *owner_;
};

class ITCHandler
{
public:
  ITCHandler() {}
  virtual ~ITCHandler() {}
  virtual int handle(TCRequest* req) = 0;
};

enum QD_TYPE { QDISC_ROOT, QDISC_BUFFER_QUEUE, QDISC_WEIGHTED_QUEUE, QDISC_QUEUE_END, TCLIMIT_BYTES, TCLIMIT_COUNT };
int init_qdtable();
int tclimit_create(int type, const char* name, uint64_t storage_key = 0);
void tclimit_destroy(int limiter_id);
int tclimit_set_limit(int limiter_id, int64_t limit);
int tclimit_get_limit(int limiter_id, int64_t &limit);
int qdisc_create(int type, int parent, const char* name);
void qdisc_destroy(int qid);
int qsched_set_handler(int root, ITCHandler* handler);
int qsched_start(int root, int n_thread);
int qsched_stop(int root);
int qsched_wait(int root);
int qsched_submit(int root, TCRequest* req, uint32_t chan_id);
int qdisc_set_weight(int qid, int64_t weight); // default weight, limit, reserve
int qdisc_set_limit(int qid, int64_t limit);
int qdisc_set_reserve(int qid, int64_t limit);
int qdisc_add_limit(int qid, int limiter_id);
int qdisc_del_limit(int qid, int limiter_id);
int qdisc_add_reserve(int qid, int limiter_id);
int qdisc_del_reserve(int qid, int limiter_id);
