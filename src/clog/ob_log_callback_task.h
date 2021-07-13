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

#ifndef OCEANBASE_CLOG_OB_LOG_CALLBACK_TASK_
#define OCEANBASE_CLOG_OB_LOG_CALLBACK_TASK_

#include "lib/allocator/ob_allocator.h"
#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "common/ob_i_callback.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
enum CallbackTaskType {
  CLOG_UNKNOWN_CB,
  CLOG_BATCH_CB,
  CLOG_MEMBER_CHANGE_SUCCESS_CB,
  CLOG_POP_TASK_CB,
};

class ObLogCallbackTask {
public:
  ObLogCallbackTask()
  {
    reset();
  }
  ~ObLogCallbackTask()
  {}

public:
  int init(const CallbackTaskType& task_type, const common::ObPartitionKey& partition_key);
  void reset();
  CallbackTaskType get_cb_task_type() const
  {
    return task_type_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  TO_STRING_KV(K_(task_type), K_(partition_key));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogCallbackTask);

private:
  CallbackTaskType task_type_;
  common::ObPartitionKey partition_key_;
};

class ObGeneralCallbackTask : public ObLogCallbackTask {
public:
  ObGeneralCallbackTask()
  {
    reset();
  }
  ~ObGeneralCallbackTask()
  {}
  int init(common::ObICallback* callback);
  void reset();
  int handle_callback();

private:
  DISALLOW_COPY_AND_ASSIGN(ObGeneralCallbackTask);

public:
  common::ObICallback* callback_;
  int64_t before_push_cb_ts_;
};
typedef ObGeneralCallbackTask ObBatchCallbackTask;

class ObMemberChangeCallbackTask : public ObLogCallbackTask {
public:
  ObMemberChangeCallbackTask()
  {
    reset();
  }
  ~ObMemberChangeCallbackTask()
  {}

public:
  int init(const CallbackTaskType& task_type, const common::ObPartitionKey& partition_key,
      const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
      const common::ObProposalID& ms_proposal_id);
  void reset();
  ObLogType get_log_type() const
  {
    return log_type_;
  }
  uint64_t get_ms_log_id() const
  {
    return ms_log_id_;
  }
  int64_t get_mc_timestamp() const
  {
    return mc_timestamp_;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  const common::ObMemberList& get_prev_member_list() const
  {
    return prev_member_list_;
  }
  const common::ObMemberList& get_curr_member_list() const
  {
    return curr_member_list_;
  }
  common::ObProposalID get_ms_proposal_id() const
  {
    return ms_proposal_id_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemberChangeCallbackTask);

private:
  ObLogType log_type_;
  uint64_t ms_log_id_;
  int64_t mc_timestamp_;
  int64_t replica_num_;
  common::ObMemberList prev_member_list_;
  common::ObMemberList curr_member_list_;
  common::ObProposalID ms_proposal_id_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CALLBACK_TASK_
