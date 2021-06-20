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

#ifndef OCEANBASE_TRANSACTION_ELR_TASK_H_
#define OCEANBASE_TRANSACTION_ELR_TASK_H_

#include "ob_trans_define.h"

namespace oceanbase {

namespace transaction {
class WaitTransEndTask : public ObTransTask {
public:
  WaitTransEndTask()
  {
    reset();
  }
  virtual ~WaitTransEndTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t task_type, const common::ObPartitionKey& partition, const ObTransID& trans_id);
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  TO_STRING_KV(K_(partition), K_(trans_id));

private:
  ObTransID trans_id_;
  common::ObPartitionKey partition_;
  DISALLOW_COPY_AND_ASSIGN(WaitTransEndTask);
};

class CallbackTransTask : public ObTransTask {
public:
  CallbackTransTask() : ObTransTask(ObTransRetryTaskType::UNKNOWN)
  {
    reset();
  }
  virtual ~CallbackTransTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t callback_type, const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const ObTransID& prev_trans_id, const int status);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  const ObTransID& get_prev_trans_id() const
  {
    return prev_trans_id_;
  }
  int get_status() const
  {
    return status_;
  }
  TO_STRING_KV(K_(partition), K_(trans_id), K_(prev_trans_id), K_(status));

private:
  common::ObPartitionKey partition_;
  ObTransID trans_id_;
  ObTransID prev_trans_id_;
  int status_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif
