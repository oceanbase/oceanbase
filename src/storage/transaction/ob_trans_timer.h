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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_TIMER_
#define OCEANBASE_TRANSACTION_OB_TRANS_TIMER_

#include <stdint.h>
#include "common/ob_partition_key.h"
#include "ob_time_wheel.h"
#include "ob_trans_define.h"

namespace oceanbase {

namespace common {
class ObTimeWheel;
class ObTimeWheelTask;
class ObPartitionKey;
}  // namespace common

namespace transaction {
class ObITimeoutTask;
class ObTransTimeoutTask;
class ObITransCtxMgr;
class ObTransService;
}  // namespace transaction

namespace transaction {

class ObITimeoutTask : public common::ObTimeWheelTask {
public:
  ObITimeoutTask() : is_registered_(false), is_running_(false), delay_(0)
  {}
  virtual ~ObITimeoutTask()
  {}
  void reset()
  {
    is_registered_ = false;
    is_running_ = false;
    delay_ = 0;
    common::ObTimeWheelTask::reset();
  }

public:
  void set_registered(const bool is_registered)
  {
    is_registered_ = is_registered;
  }
  bool is_registered() const
  {
    return is_registered_;
  }
  void set_running(const bool is_running)
  {
    is_running_ = is_running;
  }
  bool is_running() const
  {
    return is_running_;
  }
  void set_delay(const int64_t delay)
  {
    delay_ = delay;
  }

protected:
  bool is_registered_;
  bool is_running_;
  int64_t delay_;
};

class ObITransTimer {
public:
  ObITransTimer()
  {}
  virtual ~ObITransTimer()
  {}
  virtual int init() = 0;
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;

public:
  virtual int register_timeout_task(ObITimeoutTask& task, const int64_t delay) = 0;
  virtual int unregister_timeout_task(ObITimeoutTask& task) = 0;
};

class ObTransTimeoutTask : public ObITimeoutTask {
public:
  ObTransTimeoutTask() : is_inited_(false), ctx_(NULL)
  {}
  virtual ~ObTransTimeoutTask()
  {}
  int init(ObTransCtx* ctx);
  void destroy();
  void reset();

public:
  void runTimerTask();
  uint64_t hash() const;

public:
  TO_STRING_KV(K_(is_inited), K_(is_registered), K_(is_running), K_(delay), KP_(ctx), K_(bucket_idx), K_(run_ticket),
      K_(is_scheduled), KP_(prev), KP_(next));

private:
  bool is_inited_;
  ObTransCtx* ctx_;
};

class ObPrepareChangingLeaderTask : public ObITimeoutTask {
public:
  ObPrepareChangingLeaderTask()
      : is_inited_(false), expected_ts_(0), txs_(NULL), pkey_(), proposal_leader_(), round_(1), cnt_(0)
  {}
  ~ObPrepareChangingLeaderTask()
  {
    destroy();
  }
  int init(const int64_t expected_ts, ObTransService* txs, const common::ObPartitionKey& pkey,
      const common::ObAddr& proposal_leader, const int64_t round, const int64_t cnt);
  void reset()
  {
    is_inited_ = false;
    expected_ts_ = 0;
    txs_ = NULL;
    pkey_.reset();
    proposal_leader_.reset();
    round_ = 1;
    cnt_ = 0;
  }
  void destroy()
  {
    reset();
  }

public:
  void runTimerTask();
  uint64_t hash() const;
  int run();
  TO_STRING_KV(K_(is_inited), K_(expected_ts), KP_(txs), K_(pkey), K_(proposal_leader), K_(round), K_(cnt));

private:
  bool is_inited_;
  int64_t expected_ts_;
  ObTransService* txs_;
  common::ObPartitionKey pkey_;
  common::ObAddr proposal_leader_;
  int64_t round_;
  int64_t cnt_;
};

class ObTransTimer : public ObITransTimer {
public:
  ObTransTimer() : is_inited_(false), is_running_(false)
  {}
  virtual ~ObTransTimer()
  {}
  virtual int init();
  virtual int start();
  virtual int stop();
  virtual int wait();
  virtual void destroy();

public:
  virtual int register_timeout_task(ObITimeoutTask& task, const int64_t delay);
  virtual int unregister_timeout_task(ObITimeoutTask& task);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransTimer);

protected:
  // schedule timeout task precision. us
  static const int64_t TRANS_TIMEOUT_TASK_PRECISION_US = 5000;
  static const int64_t THREAD_NUM = 4;

  bool is_inited_;
  bool is_running_;
  common::ObTimeWheel tw_;
};

class ObDupTableLeaseTimer : public ObTransTimer {
public:
  ObDupTableLeaseTimer()
  {}
  virtual ~ObDupTableLeaseTimer()
  {}
  int init();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseTimer);
};

}  // namespace transaction
}  // namespace oceanbase

#endif
