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

#ifndef OCEANBASE_REPLAYENGINE_OB_I_LOG_REPLAY_ENGINE_
#define OCEANBASE_REPLAYENGINE_OB_I_LOG_REPLAY_ENGINE_

#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace transaction {
class ObTransService;
}
namespace storage {
class ObPartitionService;
class ObReplayStatus;
class ObReplayTask;
class ObIPartitionGroup;
}  // namespace storage
namespace replayengine {
class ObILogReplayEngine {
public:
  ObILogReplayEngine()
  {}
  virtual ~ObILogReplayEngine()
  {}

public:
  struct ObLogReplayEngineConfig {
    int64_t total_limit_;
    int64_t hold_limit_;
    int64_t page_size_;
    bool is_valid() const
    {
      return (total_limit_ >= 0) && (hold_limit_ >= 0) && (page_size_ >= 0);
    }
    TO_STRING_KV(K(total_limit_), K(hold_limit_), K(page_size_));
  };

public:
  virtual int init(transaction::ObTransService* trans_replay_service, storage::ObPartitionService* partition_service,
      const ObLogReplayEngineConfig& config) = 0;
  virtual void destroy() = 0;
  // submit_replay_log_task_sequentially:Submit the replay tasks in sequence, the log_id of the
  // (N+1)th submission must
  // be 1 greater than the log_id of the Nth submission
  // @param[in] need_replay:Whether the log needs to be replayed, the member change log and the
  // transaction log of D replica and log replica do not need to be replayed
  // @param[in] pkey:pkey of PG
  // @param[in] log_id:🔐id of submitted log
  // @param[in] log_ts:submit_timestamp of log
  // @retval OB_SUCCESS : The task is submitted successfully
  // @retval OB_NOT_INIT: ObLogReplayEngine has not been inited
  // @retval OB_INVALID_ARGUMENT : the arguments are not invalid
  // @retval OB_EAGAIN: the replay task needs to be resubmit:only logs those do not need to replay
  // will return OB_EAGAIN
  // When need_replay is false, the function will return success only if the current sequence logs
  // are submitted to the queue of the replay engine
  // @retval OB_ALLOCATE_MEMORY_FAILED : no memory in replay_engine mod
  // @retval OB_ERR_UNEXPECTED  unexpected error
  // @retval other error ret code

  virtual int submit_replay_log_task_sequentially(const common::ObPartitionKey& pkey, const uint64_t log_id,
      const int64_t log_ts, const bool need_replay, const clog::ObLogType log_type,
      const int64_t next_replay_log_ts) = 0;
  // submit_replay_log_task_by_restore: interface for restoring to  submit log into replay engine
  // @param[in] pkey:pkey of PG
  // @param[in] log_id:🔐the largest log id that restoring need to replay
  // @param[in] log_ts:submit_timestamp of log
  // @retval OB_SUCCESS : The task is submitted successfully
  // @retval OB_NOT_INIT: ObLogReplayEngine has not been inited
  // @retval OB_INVALID_ARGUMENT : the arguments are not invalid
  // @retval OB_ALLOCATE_MEMORY_FAILED : no memory in replay_engine mod
  // @retval OB_ERR_UNEXPECTED  unexpected error
  // @retval other error ret code
  virtual int submit_replay_log_task_by_restore(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts) = 0;
  virtual int add_partition(const common::ObPartitionKey& pkey) = 0;
  virtual int remove_partition(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup* partition) = 0;
  virtual int remove_partition(const common::ObPartitionKey& pkey) = 0;
  virtual int reset_partition(const common::ObPartitionKey& pkey) = 0;
  virtual int set_need_filter_trans_log(const common::ObPartitionKey& pkey, const bool need_filter) = 0;

  virtual int is_replay_finished(const common::ObPartitionKey& partition_key, bool& replay_finish) const = 0;
  virtual int is_submit_finished(const common::ObPartitionKey& partition_key, bool& submit_finish) const = 0;
  virtual int check_can_receive_log(const common::ObPartitionKey& pkey, bool& can_receive_log) const = 0;
  virtual int get_pending_submit_task_count(
      const common::ObPartitionKey& partition_key, int64_t& pending_count) const = 0;

  virtual int is_tenant_out_of_memory(const common::ObPartitionKey& partition_key, bool& is_out_of_mem) = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;

  virtual int submit_task_into_queue(storage::ObReplayTask* task_queue) = 0;
};
}  // namespace replayengine
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_I_LOG_REPLAY_ENGINE_
