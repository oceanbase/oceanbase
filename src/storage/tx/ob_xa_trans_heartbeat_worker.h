/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_
#define OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_

#include "share/ob_thread_pool.h"         // ObThreadPool
#include "lib/thread/ob_thread_name.h"
namespace oceanbase
{

namespace transaction
{
class ObXAService;

class ObXATransHeartbeatWorker : public share::ObThreadPool
{
public:
  ObXATransHeartbeatWorker() : is_inited_(false) {}
  ~ObXATransHeartbeatWorker() { destroy(); }
  int init(ObXAService *xa_service);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  virtual void run1() override;
private:
  bool is_inited_;
  ObXAService *xa_service_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_
