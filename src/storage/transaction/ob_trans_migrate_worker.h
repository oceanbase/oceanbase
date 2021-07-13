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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_MIGRATE_WORKER_
#define OCEANBASE_TRANSACTION_OB_TRANS_MIGRATE_WORKER_

#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {

namespace transaction {

class ObTransMigrateWorker : public lib::TGTaskHandler {
public:
  ObTransMigrateWorker() : is_inited_(false), is_running_(false)
  {}
  ~ObTransMigrateWorker()
  {
    destroy();
  }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  int push(void* task);

public:
  void handle(void* task);

private:
  bool is_inited_;
  bool is_running_;
};

}  // namespace transaction
}  // namespace oceanbase
#endif  // OCEANBASE_TRANSACTION_OB_TRANS_MIGRATE_WORKER_
