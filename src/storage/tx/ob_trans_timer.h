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
#include "ob_time_wheel.h"

namespace oceanbase
{

namespace common
{
class ObTimeWheel;
class ObTimeWheelTask;
}

namespace transaction
{
class ObTransService;
class ObTransCtx;
class ObTxDesc;
class ObTransService;

class ObITimeoutTask : public common::ObTimeWheelTask
{
public:
  ObITimeoutTask() : is_registered_(false), is_running_(false), delay_(0) {}
  virtual ~ObITimeoutTask() {}
  void reset()
  {
    is_registered_ = false;
    is_running_ = false;
    delay_ = 0;
    common::ObTimeWheelTask::reset();
  }
public:
  void set_registered(const bool is_registered) { is_registered_ = is_registered; }
  bool is_registered() const { return is_registered_; }
  void set_running(const bool is_running) { is_running_ = is_running; }
  bool is_running() const { return is_running_; }
  void set_delay(const int64_t delay) { delay_ = delay; }
  int64_t get_delay() const { return delay_; }
protected:
  bool is_registered_;
  bool is_running_;
  int64_t delay_;
};

class ObITransTimer
{
public:
  ObITransTimer() {}
  virtual ~ObITransTimer() {}
  virtual int init(const char *timer_name) = 0;
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;
public:
  virtual int register_timeout_task(ObITimeoutTask &task, const int64_t delay) = 0;
  virtual int unregister_timeout_task(ObITimeoutTask &task) = 0;
};

class ObTransTimeoutTask : public ObITimeoutTask
{
public:
  ObTransTimeoutTask() :is_inited_(false), ctx_(NULL) {}
  virtual ~ObTransTimeoutTask() {}
  int init(ObTransCtx *ctx);
  void destroy();
  void reset();
public:
  void runTimerTask();
  uint64_t hash() const;
public:
  TO_STRING_KV(K_(is_inited), K_(is_registered), K_(is_running), K_(delay), KP_(ctx),
      K_(bucket_idx), K_(run_ticket), K_(is_scheduled), KP_(prev), KP_(next));
private:
  bool is_inited_;
  ObTransCtx *ctx_;
};

class ObTxTimeoutTask : public ObITimeoutTask
{
public:
  ObTxTimeoutTask() :is_inited_(false), tx_desc_(NULL), txs_(NULL) {}
  virtual ~ObTxTimeoutTask() {}
  int init(ObTxDesc *tx_desc, ObTransService* txs);
  void reset();
public:
  void runTimerTask();
  uint64_t hash() const;
public:
  TO_STRING_KV(K_(is_inited), K_(is_registered), K_(is_running), K_(delay), KP_(tx_desc),
      K_(bucket_idx), K_(run_ticket), K_(is_scheduled), KP_(prev), KP_(next));
private:
  bool is_inited_;
  ObTxDesc *tx_desc_;
  ObTransService *txs_;
};

class ObTransTimer : public ObITransTimer
{
public:
  ObTransTimer() : is_inited_(false), is_running_(false) {}
  virtual ~ObTransTimer() {}
  virtual int init(const char *timer_name);
  virtual int start();
  virtual int stop();
  virtual int wait();
  virtual void destroy();
public:
  virtual int register_timeout_task(ObITimeoutTask &task, const int64_t delay);
  virtual int unregister_timeout_task(ObITimeoutTask &task);
private:
  int64_t get_thread_num_() { return common::max(sysconf(_SC_NPROCESSORS_ONLN) / 24, 2); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransTimer);
protected:
  // schedule timeout task precision. us
  static const int64_t TRANS_TIMEOUT_TASK_PRECISION_US = 100 * 1000L;

  bool is_inited_;
  bool is_running_;
  common::ObTimeWheel tw_;
};

class ObDupTableLeaseTimer : public ObTransTimer
{
public:
  ObDupTableLeaseTimer() {}
  virtual ~ObDupTableLeaseTimer() {}
  int init();
private:
  static const int64_t DUP_TABLE_TIMEOUT_TASK_PRECISION_US = 3 * 1000 * 1000L;
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseTimer);
};

} // transaction
} // oceanbase

#endif
