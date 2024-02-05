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
  xid_.reset();
  coord_.reset();
  last_request_ts_ = OB_INVALID_TIMESTAMP;
  busy_cbs_cnt_ = 0;
  replay_completeness_ = -1;
  serial_final_scn_.reset();
  callback_list_stats_.reset();
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
                   const bool is_exiting, const ObXATransID &xid,
                   const share::ObLSID &coord, const int64_t last_request_ts,
                   SCN start_scn, SCN end_scn, SCN rec_scn, bool transfer_blocking,
                   const int busy_cbs_cnt,
                   int replay_completeness,
                   share::SCN serial_final_scn)
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
    xid_ = xid;
    if (part_tx_action == ObPartTransAction::COMMIT && !coord.is_valid()) {
      coord_ = ls_id;
    } else {
      coord_ = coord;
    }
    last_request_ts_ = last_request_ts;
    start_scn_ = start_scn;
    end_scn_ = end_scn;
    rec_scn_ = rec_scn;
    transfer_blocking_ = transfer_blocking;
    busy_cbs_cnt_ = busy_cbs_cnt;
    replay_completeness_ = replay_completeness;
    serial_final_scn_ = serial_final_scn;
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

int ObTxSchedulerStat::init(const uint64_t tenant_id,
                            const common::ObAddr &addr,
                            const uint32_t sess_id,
                            const ObTransID &tx_id,
                            const int64_t state,
                            const int64_t cluster_id,
                            const ObXATransID &xid,
                            const share::ObLSID &coord_id,
                            const ObTxPartList &parts,
                            const ObTxIsolationLevel &isolation,
                            const share::SCN &snapshot_version,
                            const ObTxAccessMode &access_mode,
                            const uint64_t op_sn,
                            const uint64_t flag,
                            const int64_t active_ts,
                            const int64_t expire_ts,
                            const int64_t timeout_us,
                            const int32_t ref_cnt,
                            const void* const tx_desc_addr,
                            const ObTxSavePointList &savepoints,
                            const int16_t abort_cause,
                            const bool can_elr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "ObTxSchedulerStat init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(parts_.assign(parts))) {
    TRANS_LOG(WARN, "parts assign error", KR(ret), K(parts));
  } else if (OB_FAIL(get_valid_savepoints(savepoints))) {
    TRANS_LOG(WARN, "savepoints assign error", KR(ret), K(savepoints));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    addr_ = addr;
    sess_id_ = sess_id;
    tx_id_ = tx_id;
    state_ = state;
    cluster_id_ = cluster_id;
    xid_ = xid;
    coord_id_ = coord_id;
    isolation_ = isolation;
    snapshot_version_ = snapshot_version;
    access_mode_ = access_mode;
    op_sn_ = op_sn;
    flag_ = flag;
    active_ts_ = active_ts;
    expire_ts_ = expire_ts;
    timeout_us_ = timeout_us;
    ref_cnt_ = ref_cnt;
    tx_desc_addr_ = tx_desc_addr;
    abort_cause_ = abort_cause;
    can_elr_ = can_elr;
  }
  return ret;
}

void ObTxSchedulerStat::reset()
{
  is_inited_ = false;
  tenant_id_ = 0;
  addr_.reset();
  sess_id_ = 0;
  tx_id_.reset();
  state_ = 0;
  cluster_id_ = -1;
  xid_.reset();
  coord_id_.reset();
  parts_.reset();
  isolation_ = ObTxIsolationLevel::INVALID;
  snapshot_version_.reset();
  access_mode_ = ObTxAccessMode::INVL;
  op_sn_ = -1;
  flag_ = 0;
  active_ts_ = -1;
  expire_ts_ = -1;
  timeout_us_ = -1;
  ref_cnt_ = -1;
  tx_desc_addr_ = (void*)0;
  savepoints_.reset();
  abort_cause_ = 0;
  can_elr_ = false;
}

int64_t ObTxSchedulerStat::get_parts_str(char* buf, const int64_t buf_len)
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int i = 0; i < parts_.count(); i++) {
    J_OBJ_START();
    J_KV("id", parts_.at(i).id_.id());
    J_COMMA();
    J_KV("addr", parts_.at(i).addr_);
    J_COMMA();
    J_KV("epoch", parts_.at(i).epoch_);
    J_OBJ_END();
    if (i < parts_.count() - 1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  return pos;
}

int ObTxSchedulerStat::get_valid_savepoints(const ObTxSavePointList &savepoints)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < savepoints.count(); i++) {
    if (savepoints.at(i).is_savepoint()) {
      if (OB_FAIL(savepoints_.push_back(savepoints.at(i)))) {
        TRANS_LOG(WARN, "failed to push into savepoints array", KR(ret));
      }
    }
  }
  return ret;
}

} // transaction
} // oceanbase
