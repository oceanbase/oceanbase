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

#ifndef OCEANBASE_SIGNAL_WORKER_H_
#define OCEANBASE_SIGNAL_WORKER_H_

#include "lib/thread/thread_pool.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/signal/ob_signal_utils.h"

namespace oceanbase
{
namespace common
{

struct ObSigRequest
{
  constexpr static uint64_t MAGIC = 0xebeb12344321bebe;
  ObSigRequest()
    : magic_(MAGIC),
      fd_{-1, -1},
      fd2_{-1, -1},
      code_(INVALID_LELVEL),
      ref_cnt_(0),
      exclude_tid_(-1),
      trace_id_()
  {}
  bool check_magic() { return MAGIC == magic_; }
  int64_t inc_and_fetch_ref() { return ATOMIC_AAF(&ref_cnt_, 1); }
  int64_t dec_and_fetch_ref() { return ATOMIC_AAF(&ref_cnt_, -1); }
  int64_t fetch_ref() { return ATOMIC_LOAD(&ref_cnt_); }

  uint64_t magic_;
  int fd_[2];
  int fd2_[2];
  int code_;
  int64_t ref_cnt_;
  int64_t exclude_tid_;
  DTraceId trace_id_;
};

class ObSignalWorker : public lib::ThreadPool
{
public:
  ObSignalWorker();
  ~ObSignalWorker();
  void run1() override;
  int start() override;
  void stop() override;
  void wait() override;
};

class ObSigHandlerCtx;
class ObSigHandler
{
public:
  void handle(ObSigHandlerCtx &ctx);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SIGNAL_WORKER_H_
