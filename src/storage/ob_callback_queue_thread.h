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

#ifndef OCEANBASE_STORAGE_OB_CALLBACK_QUEUE_THREAD_
#define OCEANBASE_STORAGE_OB_CALLBACK_QUEUE_THREAD_

#include "common/ob_partition_key.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/cpu/ob_cpu_topology.h"

#include "clog/ob_partition_log_service.h"

namespace oceanbase {
namespace storage {
enum ObCbTaskType {
  LEADER_REVOKE_TASK = 0,
  LEADER_TAKEOVER_TASK,
  LEADER_TAKEOVER_BOTTOM_TASK,
  LEADER_ACTIVE_TASK,
  FREEZE_LOG_TASK,
  OFFLINE_PARTITION_TASK,
};

class LeaderActiveArg {
public:
  LeaderActiveArg() : is_elected_by_changing_leader_(false)
  {}
  ~LeaderActiveArg()
  {}
  LeaderActiveArg& operator=(const LeaderActiveArg& arg)
  {
    is_elected_by_changing_leader_ = arg.is_elected_by_changing_leader_;
    return *this;
  }
  TO_STRING_KV(K_(is_elected_by_changing_leader));

public:
  bool is_elected_by_changing_leader_;
};

struct ObCbTask {
  ObCbTaskType task_type_;
  common::ObPartitionKey pkey_;
  common::ObRole role_;
  bool succeed_;
  int64_t retry_cnt_;
  int ret_code_;
  LeaderActiveArg leader_active_arg_;
  bool large_cb_;

  ObCbTask()
      : task_type_(),
        pkey_(),
        role_(common::INVALID_ROLE),
        succeed_(true),
        retry_cnt_(0),
        ret_code_(OB_SUCCESS),
        leader_active_arg_(),
        large_cb_(false)
  {}
  ~ObCbTask()
  {}

  bool is_valid() const
  {
    return pkey_.is_valid() && retry_cnt_ >= 0;
  }
  TO_STRING_KV("task_type", task_type_, N_KEY, pkey_, "role", role_, "succeed", succeed_, "ret_code", ret_code_,
      "retry_count", retry_cnt_, "leader_active_arg", leader_active_arg_, "large_cb", large_cb_);
};

class ObCallbackQueueThread : public lib::TGTaskHandler {
public:
  static int64_t QUEUE_THREAD_NUM;
  static const int64_t MINI_MODE_QUEUE_THREAD_NUM = 2;
  ObCallbackQueueThread();
  virtual ~ObCallbackQueueThread();

public:
  virtual int init(ObPartitionService* partition_service, int tg_id);
  virtual int push(const ObCbTask* task);
  virtual void handle(void* task);
  virtual void destroy();
  int get_tg_id() const
  {
    return tg_id_;
  }

private:
  int get_task(ObCbTask*& task);
  void free_task(ObCbTask* task);

private:
  bool inited_;
  ObPartitionService* partition_service_;
  common::ObFixedQueue<ObCbTask> free_queue_;
  ObCbTask* tasks_;
  int tg_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCallbackQueueThread);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_CALLBACK_QUEUE_THREAD_
