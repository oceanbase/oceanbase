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

#include "ob_trans_stat.h"

namespace oceanbase
{
using namespace common;

namespace transaction
{

void ObTransStat::reset()
{
  is_inited_ = false;
  addr_.reset();
  trans_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_exiting_ = false;
  is_readonly_ = false;
  has_decided_ = false;
  is_dirty_ = false;
  active_memstore_version_.reset();
  trans_param_.reset();
  ctx_create_time_ = -1;
  expired_time_ = -1;
  refer_ = -1;
  sql_no_ = 0;
  state_ = static_cast<int64_t>(ObTxState::UNKNOWN);
  session_id_ = 0;
  proxy_session_id_ = 0;
  trans_type_ = TransType::UNKNOWN_TRANS;
  part_trans_action_ = ObPartTransAction::UNKNOWN;
  lock_for_read_retry_count_ = 0;
  ctx_addr_ = 0;
  prev_trans_arr_.reset();
  next_trans_arr_.reset();
  prev_trans_commit_count_ = 0;
  ctx_id_ = 0;
  pending_log_size_ = 0;
  flushed_log_size_ = 0;
}

int ObTransStat::init(const ObAddr &addr, const ObTransID &trans_id,
    const uint64_t tenant_id, const bool is_exiting, const bool is_readonly,
    const bool has_decided, const bool is_dirty,
    const ObStartTransParam &trans_param, const int64_t ctx_create_time,
    const int64_t expired_time, const int64_t refer, const int64_t sql_no,
    const int64_t state, const uint32_t session_id, const uint64_t proxy_session_id,
    const int trans_type, const int64_t part_trans_action, const uint64_t lock_for_read_retry_count,
    const int64_t ctx_addr, const ObElrTransInfoArray &prev_trans_arr,
    const ObElrTransInfoArray &next_trans_arr, int32_t prev_trans_commit_count,
    uint32_t ctx_id, const int64_t pending_log_size, const int64_t flushed_log_size)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransStat init twice");
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    addr_ = addr;
    trans_id_ = trans_id;
    tenant_id_ = tenant_id;
    is_exiting_ = is_exiting;
    is_readonly_ = is_readonly;
    has_decided_ = has_decided;
    is_dirty_ = is_dirty;
    active_memstore_version_ = ObVersion(2);
    trans_param_ = trans_param;
    ctx_create_time_ = ctx_create_time;
    expired_time_ = expired_time;
    refer_ = refer;
    sql_no_ = sql_no;
    state_ = state;
    session_id_ = session_id;
    proxy_session_id_ = proxy_session_id;
    trans_type_ = trans_type;
    part_trans_action_ = part_trans_action;
    lock_for_read_retry_count_ = lock_for_read_retry_count;
    ctx_addr_ = ctx_addr;
    prev_trans_arr_ = prev_trans_arr;
    next_trans_arr_ = next_trans_arr;
    prev_trans_commit_count_ = prev_trans_commit_count;
    ctx_id_ = ctx_id;
    pending_log_size_ = pending_log_size;
    flushed_log_size_ = flushed_log_size;
  }

  return ret;
}

int ObTransLockStat::init(const common::ObAddr &addr,
                      uint64_t tenant_id,
                      const ObMemtableKeyInfo &memtable_key,
                      uint32_t session_id,
                      uint64_t proxy_session_id,
                      const ObTransID &trans_id,
                      int64_t ctx_create_time,
                      int64_t expired_time)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransLockStat init twice");
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    addr_ = addr;
    tenant_id_ = tenant_id;
    memtable_key_ = memtable_key;
    session_id_ = session_id;
    proxy_session_id_ = proxy_session_id;
    trans_id_ = trans_id;
    ctx_create_time_ = ctx_create_time;
    expired_time_ = expired_time;
  }

  return ret;
}

void ObTransLockStat::reset()
{
  is_inited_ = false;
  addr_.reset();
  tenant_id_ = 0;
  memtable_key_.reset();
  session_id_ = 0;
  proxy_session_id_ = 0;
  trans_id_.reset();
  ctx_create_time_ = 0;
  expired_time_ = 0;
}

int ObDuplicatePartitionStat::init(const common::ObAddr &addr,
                                   const uint64_t cur_log_id,
                                   const bool is_master,
                                   const DupTableLeaseInfoHashMap &hashmap)
{
  int ret = OB_SUCCESS;
  addr_ = addr;
  cur_log_id_ = cur_log_id;
  is_master_ = is_master;
  UNUSED(hashmap);
  return ret;
}

void ObDuplicatePartitionStat::reset()
{
  addr_.reset();
  cur_log_id_ = 0;
  is_master_ = false;
  lease_list_.reset();
}

} // transaction
} // oceanbase
