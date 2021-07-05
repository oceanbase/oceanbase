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

#ifndef OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_
#define OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_

#include "share/ob_thread_pool.h"  // ObThreadPool

namespace oceanbase {

namespace transaction {
class ObTransService;

class ObXATransHeartbeatWorker : public share::ObThreadPool {
public:
  ObXATransHeartbeatWorker() : is_inited_(false), is_running_(false)
  {}
  ~ObXATransHeartbeatWorker()
  {
    destroy();
  }
  int init(ObTransService* trans_service);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  virtual void run1() override;

private:
  bool is_inited_;
  bool is_running_;
  ObTransService* trans_service_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_XA_TRANS_HEARTBEAT_WORKER_
