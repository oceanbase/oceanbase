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

#include "ob_tx_stat.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace transaction
{

void ObTxStat::reset()
{
  is_inited_ = false;
  addr_.reset();
  tx_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  has_decided_ = false;
  ls_id_.reset();
  participants_.reset();
  tx_ctx_create_time_ = -1;
  tx_expired_time_ = -1;
  ref_cnt_ = -1;
  last_op_sn_ = 0;
  pending_write_ = 0;
  state_ = static_cast<int64_t>(ObTxState::UNKNOWN);
  tx_type_ = TransType::UNKNOWN_TRANS;
  part_tx_action_ = ObPartTransAction::UNKNOWN;
  tx_ctx_addr_ = (void*)0;
  pending_log_size_ = 0;
  flushed_log_size_ = 0;
  role_state_ = -1;  // RoleState::INVALID
  session_id_ = 0;
  scheduler_addr_.reset();
  is_exiting_ = false;
}
int ObTxStat::init(const common::ObAddr &addr, const ObTransID &tx_id,
                   const uint64_t tenant_id,  const bool has_decided,
                   const share::ObLSID &ls_id, const share::ObLSArray &participants,
                   const int64_t tx_ctx_create_time, const int64_t tx_expired_time,
                   const int64_t ref_cnt, const int64_t last_op_sn,
                   const int64_t pending_write, const int64_t state,
                   const int tx_type, const int64_t part_tx_action,
                   const void* const tx_ctx_addr,
                   const int64_t pending_log_size, const int64_t flushed_log_size,
                   const int64_t role_state,
                   const int64_t session_id, const common::ObAddr &scheduler,
                   const bool is_exiting)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "ObTxStat init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_SUCCESS != (ret = participants_.assign(participants))) {
    TRANS_LOG(WARN, "ls array assign error", KR(ret), K(participants));
  } else {
    is_inited_ = true;
    addr_ = addr;
    tx_id_ = tx_id;
    tenant_id_ = tenant_id;
    has_decided_ = has_decided;
    ls_id_ = ls_id;
    tx_ctx_create_time_ = tx_ctx_create_time;
    tx_expired_time_ = tx_expired_time;
    ref_cnt_ = ref_cnt;
    last_op_sn_ = last_op_sn;
    pending_write_ = pending_write;
    state_ = state;
    tx_type_ = tx_type;
    part_tx_action_ = part_tx_action;
    tx_ctx_addr_ = tx_ctx_addr;
    pending_log_size_ = pending_log_size;
    flushed_log_size_ = flushed_log_size;
    role_state_ = role_state;
    session_id_ = session_id;
    scheduler_addr_ = scheduler;
    is_exiting_ = is_exiting;
  }
  return ret;
}

int ObTxLockStat::init(const common::ObAddr &addr,
                      uint64_t tenant_id,
                      const share::ObLSID &ls_id,
                      const ObMemtableKeyInfo &memtable_key_info,
                      uint32_t session_id,
                      uint64_t proxy_session_id,
                      const ObTransID &tx_id,
                      int64_t tx_ctx_create_time,
                      int64_t tx_expired_time)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTxLockStat init twice");
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    addr_ = addr;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    memtable_key_info_ = memtable_key_info;
    session_id_ = session_id;
    proxy_session_id_ = proxy_session_id;
    tx_id_ = tx_id;
    tx_ctx_create_time_ = tx_ctx_create_time;
    tx_expired_time_ = tx_expired_time;
  }

  return ret;
}

void ObTxLockStat::reset()
{
  is_inited_ = false;
  addr_.reset();
  tenant_id_ = 0;
  ls_id_.reset();
  memtable_key_info_.reset();
  session_id_ = 0;
  proxy_session_id_ = 0;
  tx_id_.reset();
  tx_ctx_create_time_ = 0;
  tx_expired_time_ = 0;
}

} // transaction
} // oceanbase
