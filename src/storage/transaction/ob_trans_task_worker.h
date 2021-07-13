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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_TASK_WORKER_
#define OCEANBASE_TRANSACTION_OB_TRANS_TASK_WORKER_

#include "lib/thread/thread_mgr_interface.h"
#include "ob_trans_define.h"

namespace oceanbase {

namespace transaction {

class BigTransCallbackTask : public ObTransTask {
public:
  BigTransCallbackTask()
  {
    reset();
  }
  ~BigTransCallbackTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const common::ObPartitionKey& partition, const int64_t log_type, const int64_t log_id,
      const int64_t timestamp, ObTransCtx* ctx);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  int64_t get_log_type() const
  {
    return log_type_;
  }
  int64_t get_log_id() const
  {
    return log_id_;
  }
  int64_t get_log_timestamp() const
  {
    return log_timestamp_;
  }
  ObTransCtx* get_trans_ctx()
  {
    return ctx_;
  }
  bool is_valid() const;
  TO_STRING_KV(KP(this), K_(partition), K_(log_type), K_(log_id), K_(log_timestamp));

private:
  common::ObPartitionKey partition_;
  int64_t log_type_;
  int64_t log_id_;
  int64_t log_timestamp_;
  ObTransCtx* ctx_;
};

class ObTransTaskWorker : public lib::TGTaskHandler {
public:
  ObTransTaskWorker() : is_inited_(false), is_running_(false)
  {}
  ~ObTransTaskWorker()
  {
    destroy();
  }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

public:
  void handle(void* task);
  int submit_big_trans_callback_task(const common::ObPartitionKey& partition, const int64_t log_type,
      const uint64_t log_id, const int64_t submit_timestamp, ObTransCtx* ctx);

private:
  bool is_inited_;
  bool is_running_;
  DISALLOW_COPY_AND_ASSIGN(ObTransTaskWorker);
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_TASK_WORKER_
