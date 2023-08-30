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

#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_service.h"
#include "storage/high_availability/ob_ls_block_tx_service.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace storage
{

ObLSBlockTxService::ObLSBlockTxService()
  : is_inited_(false),
    mutex_(),
    cur_seq_(SCN::min_scn()),
    ls_(NULL)
{
}

ObLSBlockTxService::~ObLSBlockTxService()
{
}

int ObLSBlockTxService::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "block tx service is inited", K(ret), KP(ls));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ls));
  } else {
    ls_ = ls;
    cur_seq_.set_min();
    is_inited_ = true;
    STORAGE_LOG(INFO, "success to init block tx service", K(ret), KP(ls), "ls_id", ls_->get_ls_id(), KP(this));
  }
  return ret;
}

void ObLSBlockTxService::destroy()
{
  is_inited_ = false;
  cur_seq_.reset();
  ls_ = NULL;
}

void ObLSBlockTxService::switch_to_follower_forcedly()
{
  ObMutexGuard mutex_guard(mutex_);
  ls_->get_tx_svr()->unblock_normal();
  STORAGE_LOG(INFO, "[BLOCK_TX]switch to follower finish");
}

// TODO(yangyi.yyy): switch to leader should fetch new gts
int ObLSBlockTxService::switch_to_leader()
{
  ObMutexGuard mutex_guard(mutex_);
  ls_->get_tx_svr()->unblock_normal();
  STORAGE_LOG(INFO, "[BLOCK_TX]switch to leader finish");
  return OB_SUCCESS;
}

int ObLSBlockTxService::switch_to_follower_gracefully()
{
  ObMutexGuard mutex_guard(mutex_);
  ls_->get_tx_svr()->unblock_normal();
  STORAGE_LOG(INFO, "[BLOCK_TX]switch to follower gracefully");
  return OB_SUCCESS;
}

int ObLSBlockTxService::resume_leader()
{
  ObMutexGuard mutex_guard(mutex_);
  STORAGE_LOG(INFO, "[BLOCK_TX]resume leader finish");
  return OB_SUCCESS;
}

int ObLSBlockTxService::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  UNUSEDx(buffer, nbytes, lsn, scn);
  return OB_SUCCESS;
}

int ObLSBlockTxService::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObLSBlockTxService::ha_block_tx(const share::SCN &new_seq)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_is_leader_())) {
    STORAGE_LOG(WARN, "failed to check is leader", K(ret));
  } else if (OB_FAIL(check_seq_(new_seq))) {
    STORAGE_LOG(WARN, "failed to check seq", K(ret));
  } else if (OB_FAIL(ls_->get_tx_svr()->block_normal())) {
    STORAGE_LOG(WARN, "failed to block tx", K(ret));
  } else {
    SERVER_EVENT_ADD("transfer", "ha_block_tx",
                     "tenant_id", MTL_ID(),
                     "ls_id", ls_->get_ls_id(),
                     "cur_seq", new_seq,
                     "prev_seq", cur_seq_);
    update_seq_(new_seq);
    STORAGE_LOG(INFO, "success to block tx", K(ret), KPC_(ls), K(new_seq));
  }
  return ret;
}

int ObLSBlockTxService::ha_kill_tx(const share::SCN &new_seq)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_is_leader_())) {
    STORAGE_LOG(WARN, "failed to check is leader", K(ret));
  } else if (OB_FAIL(check_seq_(new_seq))) {
    STORAGE_LOG(WARN, "failed to check seq", K(ret));
  } else if (OB_FAIL(ls_->kill_all_tx(true/*gracefully*/))) {
    STORAGE_LOG(WARN, "failed to kill all tx", K(ret), KPC_(ls));
  } else {
    SERVER_EVENT_ADD("transfer", "ha_kill_tx",
                     "tenant_id", MTL_ID(),
                     "ls_id", ls_->get_ls_id(),
                     "cur_seq", new_seq,
                     "prev_seq", cur_seq_);
    update_seq_(new_seq);
    STORAGE_LOG(INFO, "success to kill all tx", K(ret), KPC_(ls), K(new_seq));
  }
  return ret;
}

int ObLSBlockTxService::ha_unblock_tx(const share::SCN &new_seq)
{
  int ret = OB_SUCCESS;
  ObMutexGuard mutex_guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (new_seq != cur_seq_) {
    ret = OB_SEQUENCE_NOT_MATCH;
    STORAGE_LOG(WARN, "unblock tx get unexpected sequence", K(ret), K(new_seq), K(cur_seq_));
  } else if (OB_FAIL(ls_->get_tx_svr()->unblock_normal())) {
    if (OB_STATE_NOT_MATCH == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "tx is already unblock", KPC_(ls), K(new_seq));
    } else {
      STORAGE_LOG(WARN, "failed to unblock normal", K(ret), K(new_seq));
    }
  } else {
    SERVER_EVENT_ADD("transfer", "ha_unblock_tx",
                     "tenant_id", MTL_ID(),
                     "ls_id", ls_->get_ls_id(),
                     "cur_seq", new_seq,
                     "prev_seq", cur_seq_);
    update_seq_(new_seq);
    STORAGE_LOG(INFO, "success to unblock tx", K(ret), KPC_(ls), K(new_seq));
  }
  return ret;
}

int ObLSBlockTxService::check_is_leader_()
{
  int ret =  OB_SUCCESS;
  ObRole role;
  int64_t proposal_id = 0;
  if (OB_FAIL(ls_->get_log_handler()->get_role(role, proposal_id))) {
    STORAGE_LOG(WARN, "failed to get role", K(ret), KPC(ls_));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
    STORAGE_LOG(WARN, "ls is not leader, can not block tx", K(ret), K(role));
  }
  return ret;
}

int ObLSBlockTxService::check_seq_(const share::SCN &new_seq)
{
  int ret = OB_SUCCESS;
  if (cur_seq_.is_min()) {
    // do not check
  } else if (new_seq < cur_seq_) {
    ret = OB_SEQUENCE_TOO_SMALL;
    STORAGE_LOG(WARN, "seq is too SMALL", K(ret), K(new_seq), K(cur_seq_));
  }
  STORAGE_LOG(INFO, "compare seq", K(ret), K(new_seq), K(cur_seq_));
  return ret;
}

int ObLSBlockTxService::update_seq_(const share::SCN &new_seq)
{
  int ret = OB_SUCCESS;
  if (!new_seq.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "seq is not valid", K(ret), K(new_seq), K(cur_seq_));
  } else if (new_seq == cur_seq_) {
    //do nothing
  } else if (new_seq < cur_seq_) {
    STORAGE_LOG(WARN, "seq is too old", K(ret), K(new_seq), K(cur_seq_));
  } else {
    cur_seq_ = new_seq;
  }
  return ret;
}

}
}
