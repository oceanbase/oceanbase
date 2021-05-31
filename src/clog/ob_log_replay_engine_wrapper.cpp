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

#include "ob_log_replay_engine_wrapper.h"
#include "ob_log_entry.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObLogReplayEngineWrapper::init(replayengine::ObILogReplayEngine* log_replay_engine)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_replay_engine)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_replay_engine_ = log_replay_engine;
    is_inited_ = true;
  }
  return ret;
}

int ObLogReplayEngineWrapper::submit_replay_log_task_sequentially(const ObPartitionKey& pkey, const uint64_t log_id,
    const int64_t log_ts, const bool need_replay, const clog::ObLogType log_type, const int64_t next_replay_log_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_replay_engine_->submit_replay_log_task_sequentially(
        pkey, log_id, log_ts, need_replay, log_type, next_replay_log_ts);
  }
  return ret;
}

int ObLogReplayEngineWrapper::submit_replay_log_task_by_restore(
    const ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_replay_engine_->submit_replay_log_task_by_restore(pkey, log_id, log_ts);
  }
  return ret;
}

int ObLogReplayEngineWrapper::is_replay_finished(const ObPartitionKey& partition_key, bool& is_finished) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    is_finished = false;
    ret = log_replay_engine_->is_replay_finished(partition_key, is_finished);
  }
  return ret;
}

int ObLogReplayEngineWrapper::is_submit_finished(const ObPartitionKey& partition_key, bool& is_finished) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    is_finished = false;
    ret = log_replay_engine_->is_submit_finished(partition_key, is_finished);
  }
  return ret;
}

int ObLogReplayEngineWrapper::check_can_receive_log(
    const common::ObPartitionKey& partition_key, bool& can_receive_log) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_replay_engine_->check_can_receive_log(partition_key, can_receive_log);
  }
  return ret;
}

int ObLogReplayEngineWrapper::is_tenant_out_of_memory(
    const common::ObPartitionKey& partition_key, bool& is_tenant_out_of_mem)
{
  return log_replay_engine_->is_tenant_out_of_memory(partition_key, is_tenant_out_of_mem);
}

int ObLogReplayEngineWrapper::get_pending_submit_task_count(
    const common::ObPartitionKey& partition_key, int64_t& pending_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_replay_engine_->get_pending_submit_task_count(partition_key, pending_count);
  }
  return ret;
}

int ObLogReplayEngineWrapper::reset_partition(const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == log_replay_engine_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_replay_engine_->reset_partition(partition_key);
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
