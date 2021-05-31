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

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_partition_split.h"
#include "storage/ob_partition_service.h"
#include "clog/ob_log_define.h"
#include "share/ob_partition_modify.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace clog;
using namespace rootserver;

namespace storage {
OB_SERIALIZE_MEMBER(ObPartitionSplitState, state_);
OB_SERIALIZE_MEMBER(ObPartitionSplitSourceLog, schema_version_, spp_, slave_read_ts_);
OB_SERIALIZE_MEMBER(ObPartitionSplitDestLog, split_version_, schema_version_, source_log_id_, source_log_ts_, spp_);
OB_SERIALIZE_MEMBER(ObPartitionSplitInfo, schema_version_, partition_pair_, split_type_, split_version_, source_log_id_,
    source_log_ts_, receive_split_ts_);

int ObPartitionSplitSourceLog::init(
    const int64_t schema_version, const ObSplitPartitionPair& spp, const int64_t slave_read_ts)
{
  int ret = OB_SUCCESS;
  if (0 >= schema_version || !spp.is_valid() || slave_read_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K(spp), K(slave_read_ts));
  } else if (OB_FAIL(spp_.assign(spp))) {
    STORAGE_LOG(WARN, "assign split partition pair failed", K(ret));
  } else {
    schema_version_ = schema_version;
    slave_read_ts_ = slave_read_ts;
  }
  return ret;
}

bool ObPartitionSplitSourceLog::is_valid() const
{
  return 0 < schema_version_ && slave_read_ts_ >= 0 && spp_.is_valid();
}

int ObPartitionSplitSourceLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else if (OB_FAIL(spp_.replace_tenant_id(new_tenant_id))) {
    STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(new_tenant_id));
  } else {
  }
  return ret;
}

int ObPartitionSplitDestLog::init(const int64_t split_version, const int64_t schema_version,
    const int64_t source_log_id, const int64_t source_log_ts, const ObSplitPartitionPair& spp)
{
  int ret = OB_SUCCESS;
  if (0 >= split_version || 0 >= schema_version || !is_valid_log_id(source_log_id) || 0 >= source_log_ts ||
      !spp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(split_version),
        K(schema_version),
        K(source_log_id),
        K(source_log_ts),
        K(spp));
  } else if (OB_FAIL(spp_.assign(spp))) {
    STORAGE_LOG(WARN, "assign split pair partition failed", K(ret));
  } else {
    split_version_ = split_version;
    schema_version_ = schema_version;
    source_log_id_ = source_log_id;
    source_log_ts_ = source_log_ts;
  }
  return ret;
}

bool ObPartitionSplitDestLog::is_valid() const
{
  return 0 < split_version_ && 0 < schema_version_ && is_valid_log_id(source_log_id_) && 0 < source_log_ts_ &&
         spp_.is_valid();
}

int ObPartitionSplitDestLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id));
  } else if (OB_FAIL(spp_.replace_tenant_id(new_tenant_id))) {
    STORAGE_LOG(WARN, "replace_tenant_id failed", K(ret), K(new_tenant_id));
  } else {
  }
  return ret;
}

int ObPartitionSplitState::init(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
  }
  STORAGE_LOG(INFO, "partition split state init", K(ret), K(pkey));
  return ret;
}

int ObPartitionSplitState::set_partition_key(const common::ObPartitionKey& pkey)
{
  pkey_ = pkey;
  return OB_SUCCESS;
}

int ObPartitionSplitState::switch_state(const ObPartitionSplitAction& action)
{
  int ret = OB_SUCCESS;
  static const ObPartitionSplitStateEnum N = UNKNOWN_SPLIT_STATE;
  static const ObPartitionSplitStateEnum STATE_MAP[MAX_SPLIT_STATE][MAX_SPLIT_ACTION] = {
      // LEADER_INIT
      {FOLLOWER_INIT, N, SPLIT_START, N, N, N, N, N, N, LEADER_WAIT_SPLIT, N, N, N, N},
      // FOLLOWER_INIT
      {FOLLOWER_INIT,
          LEADER_INIT,
          N,
          N,
          N,
          N,
          N,
          N,
          TABLE_REFERENCE_SUCCESS,
          FOLLOWER_WAIT_SPLIT,
          N,
          N,
          FOLLOWER_INIT,
          N},
      // SPLIT_START
      {FOLLOWER_INIT, N, SPLIT_START, SPLIT_TRANS_CLEAR, N, N, N, N, N, N, N, N, N, N},
      // SPLIT_TRANS_CLEAR
      {FOLLOWER_INIT, N, N, SPLIT_TRANS_CLEAR, SPLIT_SOURCE_LOGGING, N, N, N, N, N, N, N, N, N},
      // SPLIT_SOURCE_LOGGING
      {FOLLOWER_INIT, N, SPLIT_SOURCE_LOGGING, N, N, LEADER_SPLIT_SOURCE_LOG, N, N, N, N, N, N, N, N},
      // LEADER_SPLIT_SOURCE_LOG
      {FOLLOWER_SPLIT_SOURCE_LOG, N, LEADER_SPLIT_SOURCE_LOG, N, N, N, SHUTDOWN_SUCCESS, N, N, N, N, N, N, N},
      // FOLLOWER_SPLIT_SOURCE_LOG
      {N, LEADER_SPLIT_SOURCE_LOG, N, N, N, N, SHUTDOWN_SUCCESS, N, TABLE_REFERENCE_SUCCESS, N, N, N, N, N},
      // SHUTDOWN_SUCCESS
      {SHUTDOWN_SUCCESS,
          SHUTDOWN_SUCCESS,
          N,
          N,
          N,
          N,
          SHUTDOWN_SUCCESS,
          TABLE_REFERENCE_SUCCESS,
          TABLE_REFERENCE_SUCCESS,
          N,
          N,
          N,
          N,
          N},
      // TABLE_REFERENCE_SUCCESS
      {TABLE_REFERENCE_SUCCESS,
          TABLE_REFERENCE_SUCCESS,
          N,
          N,
          N,
          N,
          N,
          TABLE_REFERENCE_SUCCESS,
          TABLE_REFERENCE_SUCCESS,
          N,
          N,
          N,
          N,
          N},

      // LEADER_WAIT_SPLIT
      {FOLLOWER_WAIT_SPLIT, N, N, N, N, N, N, N, N, N, SPLIT_DEST_LOGGING, N, N, N},
      // FOLLOWER_WAIT_SPLIT
      {N, LEADER_WAIT_SPLIT, N, N, N, N, N, N, N, N, N, N, FOLLOWER_LOGICAL_SPLIT_SUCCESS, N},
      // SPLIT_DEST_LOGGING
      {FOLLOWER_WAIT_SPLIT, N, N, N, N, N, N, N, N, N, SPLIT_DEST_LOGGING, LEADER_LOGICAL_SPLIT_SUCCESS, N, N},
      // LEADER_LOGICAL_SPLIT_SUCCESS
      {FOLLOWER_LOGICAL_SPLIT_SUCCESS, N, N, N, N, N, N, N, N, N, N, N, N, LEADER_INIT},
      // FOLLOWER_LOGICAL_SPLIT_SUCCESS
      {N, LEADER_LOGICAL_SPLIT_SUCCESS, N, N, N, N, N, N, N, N, N, N, FOLLOWER_LOGICAL_SPLIT_SUCCESS, FOLLOWER_INIT},

  };
  if (!is_valid_split_action(action)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(action));
  } else {
    const ObPartitionSplitStateEnum next_state = STATE_MAP[state_][action];
    if (N == next_state) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      save_state_ = state_;
      state_ = next_state;
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "switch split state failed",
        K(ret),
        K_(pkey),
        "state",
        to_state_str(state_),
        "save_state",
        to_state_str(save_state_),
        "action",
        to_action_str(action));
  } else {
    STORAGE_LOG(INFO,
        "switch split state success",
        K_(pkey),
        "state",
        to_state_str(state_),
        "save_state",
        to_state_str(save_state_),
        "action",
        to_action_str(action));
  }
  return ret;
}

int ObPartitionSplitState::restore_state()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(
      INFO, "restore split state", K_(pkey), "state", to_state_str(state_), "save_state", to_state_str(save_state_));
  state_ = save_state_;
  save_state_ = UNKNOWN_SPLIT_STATE;
  return ret;
}

int ObPartitionSplitState::set_state(const ObPartitionSplitStateEnum state)
{
  int ret = OB_SUCCESS;
  if (!is_valid_split_state(state)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(state));
  } else {
    save_state_ = state_;
    state_ = state;
    STORAGE_LOG(INFO,
        "set split state success",
        K_(pkey),
        "state",
        to_state_str(state_),
        "save_state",
        to_state_str(save_state_));
  }
  return ret;
}

int ObSplitLogCb::init(ObPartitionService* ps, const ObStorageLogType log_type)
{
  int ret = OB_SUCCESS;
  if (NULL == ps || (OB_LOG_SPLIT_SOURCE_PARTITION != log_type && OB_LOG_SPLIT_DEST_PARTITION != log_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ps), K(log_type));
  } else {
    partition_service_ = ps;
    log_type_ = log_type;
  }
  return ret;
}

int ObSplitLogCb::on_success(const ObPartitionKey& pkey, const ObLogType log_type, const uint64_t log_id,
    const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_type);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || !is_valid_log_id(log_id) || 0 >= version) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (NULL == partition_service_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, partition service is NULL", K(ret));
  } else if (OB_LOG_SPLIT_SOURCE_PARTITION == log_type_) {
    ret = partition_service_->sync_split_source_log_success(pkey, log_id, version);
  } else if (OB_LOG_SPLIT_DEST_PARTITION == log_type_) {
    ret = partition_service_->sync_split_dest_log_success(pkey);
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "illegal log type", K(ret));
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "split log callback failed", K(ret), K(pkey), K(log_id), K(version));
  } else {
    STORAGE_LOG(INFO, "split log on_success callback success", K(pkey), K(log_id), K(version));
  }
  ObSplitLogCbFactory::release(this);
  return ret;
}

int ObSplitLogCb::on_finished(const ObPartitionKey& pkey, const uint64_t log_id)
{
  STORAGE_LOG(INFO, "split log on_finished callback success", K(pkey), K(log_id));
  ObSplitLogCbFactory::release(this);
  return OB_SUCCESS;
}

ObSplitLogCb* ObSplitLogCbFactory::alloc()
{
  return op_reclaim_alloc(ObSplitLogCb);
}

void ObSplitLogCbFactory::release(ObSplitLogCb* cb)
{
  if (NULL != cb) {
    op_reclaim_free(cb);
    cb = NULL;
  }
}

int ObPartitionSplitInfo::init(const int64_t schema_version, const ObSplitPartitionPair& spp, const int64_t split_type)
{
  int ret = OB_SUCCESS;
  if (schema_version_ > 0) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), K(*this));
  } else if (0 >= schema_version || !spp.is_valid() || !is_valid_split_type(split_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K(spp), K(split_type));
  } else if (OB_FAIL(partition_pair_.assign(spp))) {
    STORAGE_LOG(WARN, "assign partition pair failed", K(ret));
  } else {
    schema_version_ = schema_version;
    split_type_ = split_type;
    receive_split_ts_ = ObTimeUtility::current_time();
  }
  STORAGE_LOG(INFO, "init split info", K(ret), K(schema_version), K(*this));
  return ret;
}

void ObPartitionSplitInfo::reset()
{
  STORAGE_LOG(DEBUG, "reset split info", K(*this));
  schema_version_ = 0;
  partition_pair_.reset();
  split_type_ = UNKNWON_SPLIT_TYPE;
  split_version_ = OB_INVALID_ID;
  source_log_id_ = OB_INVALID_ID;
  source_log_ts_ = 0;
  receive_split_ts_ = 0;
}

int ObPartitionSplitInfo::assign(const ObPartitionSplitInfo& spi)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_pair_.assign(spi.partition_pair_))) {
    STORAGE_LOG(WARN, "assign partition pair failed", K(ret));
  } else {
    schema_version_ = spi.schema_version_;
    split_type_ = spi.split_type_;
    split_version_ = spi.split_version_;
    source_log_id_ = spi.source_log_id_;
    source_log_ts_ = spi.source_log_ts_;
    receive_split_ts_ = spi.receive_split_ts_;
  }
  return ret;
}

int ObPartitionSplitInfo::set(const int64_t schema_version, const ObSplitPartitionPair& spp, const int64_t split_type)
{
  int ret = OB_SUCCESS;
  if (schema_version_ > 0) {
    STORAGE_LOG(INFO, "rewrite split info", K(*this), K(schema_version), K(spp), K(split_type));
  }
  if (OB_FAIL(partition_pair_.assign(spp))) {
    STORAGE_LOG(WARN, "assign partition pair failed", K(ret));
  } else {
    schema_version_ = schema_version;
    split_type_ = split_type;
  }
  return ret;
}

bool ObPartitionSplitInfo::is_valid() const
{
  return schema_version_ > 0 && partition_pair_.is_valid() && split_type_ != UNKNWON_SPLIT_TYPE;
}

}  // namespace storage
}  // namespace oceanbase
