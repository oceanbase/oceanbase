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

#ifndef OCEANBASE_CLOG_OB_LOG_REPLAY_ENGINE_WRAPPER_
#define OCEANBASE_CLOG_OB_LOG_REPLAY_ENGINE_WRAPPER_

#include "storage/replayengine/ob_i_log_replay_engine.h"
#include "ob_log_replay_engine_wrapper.h"

namespace oceanbase {
namespace clog {
class ObLogReplayEngineWrapper {
public:
  ObLogReplayEngineWrapper() : is_inited_(false), log_replay_engine_(NULL)
  {}
  virtual ~ObLogReplayEngineWrapper()
  {}

public:
  virtual int init(replayengine::ObILogReplayEngine* log_replay_engine);
  virtual int is_replay_finished(const common::ObPartitionKey& partition_key, bool& is_finished) const;

  virtual int is_submit_finished(const common::ObPartitionKey& partition_key, bool& is_finished) const;
  virtual int check_can_receive_log(const common::ObPartitionKey& partition_key, bool& can_receive_log) const;
  virtual int get_pending_submit_task_count(const common::ObPartitionKey& partition_key, int64_t& pending_count) const;
  virtual int submit_replay_log_task_sequentially(const common::ObPartitionKey& pkey, const uint64_t log_id,
      const int64_t log_ts, const bool need_replay, const clog::ObLogType log_type, const int64_t next_replay_log_ts);
  virtual int submit_replay_log_task_by_restore(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts);
  virtual int reset_partition(const common::ObPartitionKey& partition_key);
  virtual int is_tenant_out_of_memory(const common::ObPartitionKey& partition_key, bool& is_out_of_mem);

private:
  bool is_inited_;
  replayengine::ObILogReplayEngine* log_replay_engine_;

  DISALLOW_COPY_AND_ASSIGN(ObLogReplayEngineWrapper);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_REPLAY_ENGINE_WRAPPER_
