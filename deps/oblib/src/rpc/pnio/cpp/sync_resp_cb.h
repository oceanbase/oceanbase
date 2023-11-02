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
#include "ready_flag.h"
class SyncRespCallback: public IRespHandler
{
public:
  SyncRespCallback(IMemPool* pool): IRespHandler(pool), buf_(NULL), sz_(0) {}
  virtual ~SyncRespCallback() {}
  static SyncRespCallback* create() {
    IMemPool* pool = RpcMemPool::create(0);
    SyncRespCallback* cb = (typeof(cb))pool->alloc(sizeof(*cb));
    return new(cb)SyncRespCallback(pool);
  }
  int handle_resp(int io_err, const char* buf, int64_t sz) {
    if (NULL != buf && NULL != (buf_ = (char*)alloc(sz))) {
      memcpy(buf_, buf, sz);
      sz_ = sz;
    }
    ready_.set_ready();
    return 0;
  }
  const char* wait(int64_t& sz) {
    ready_.wait_ready();
    sz = sz_;
    return buf_;
  }
private:
  RespReadyFlag ready_;
  char* buf_;
  int64_t sz_;
};
