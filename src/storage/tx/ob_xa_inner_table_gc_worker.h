/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_XA_INNNER_TABLE_GC_WORKER_
#define OCEANBASE_TRANSACTION_OB_XA_INNNER_TABLE_GC_WORKER_

#include "share/ob_thread_pool.h"

namespace oceanbase
{

namespace transaction
{

class ObXAService;

class ObXAInnerTableGCWorker : public share::ObThreadPool
{
public:
  ObXAInnerTableGCWorker() : is_inited_(false), xa_service_(NULL) {}
  ~ObXAInnerTableGCWorker() { destroy(); }
  int init(ObXAService *txs);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  virtual void run1() override;
private:
  bool is_inited_;
  ObXAService *xa_service_;
  int64_t max_gc_cost_time_; //default is GC_INTERVAL
private:
  const static int64_t GC_INTERVAL = 10 * 1000 * 1000; // 10s
};

}//transaction

}//oceanbase

#endif
