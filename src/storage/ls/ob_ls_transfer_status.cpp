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
#include "storage/ls/ob_ls_transfer_status.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::transaction;

int ObLSTransferStatus::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLSTransferStatus init twice", K(ret), K(is_inited_));
  } else {
    ls_ = ls;
    is_inited_ = true;
    STORAGE_LOG(INFO, "ObLSTransferStatus init success", K(*this));
  }
  return ret;
}

void ObLSTransferStatus::reset()
{
  is_inited_ = false;
  ls_ = nullptr;
  transfer_tx_id_.reset();
  transfer_task_id_ = 0;
  transfer_prepare_op_ = false;
  transfer_prepare_scn_.reset();
  move_tx_op_ = false;
  move_tx_scn_.reset();
}

void ObLSTransferStatus::reset_prepare_op()
{
  transfer_prepare_op_ = false;
  transfer_prepare_scn_.reset();
  if (is_finished()) {
    transfer_tx_id_.reset();
    transfer_task_id_ = 0;
  }
}

void ObLSTransferStatus::reset_move_tx_op()
{
  move_tx_op_ = false;
  move_tx_scn_.reset();
  if (is_finished()) {
    transfer_tx_id_.reset();
    transfer_task_id_ = 0;
  }
}

bool ObLSTransferStatus::is_finished()
{
  return !transfer_prepare_op_ && !move_tx_op_;
}

int ObLSTransferStatus::online()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSTransferStatus not init", K(ret), K(*this));
  } else {
    STORAGE_LOG(INFO, "ObLSTransferStatus online", K(*this));
  }
  return ret;
}

int ObLSTransferStatus::offline()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  reset_prepare_op();
  reset_move_tx_op();
  STORAGE_LOG(INFO, "ObLSTransferStatus offline", K(*this));
  return ret;
}

int ObLSTransferStatus::update_status(const transaction::ObTransID tx_id,
                                      const int64_t task_id,
                                      const share::SCN op_scn,
                                      const transaction::NotifyType op_type,
                                      const transaction::ObTxDataSourceType mds_type)
{
  int ret = OB_SUCCESS;
  bool is_follower = false;
  int64_t proposal_id = 0;
  common::ObRole ls_role = common::ObRole::INVALID_ROLE;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSTransferStatus not init", K(ret), K(*this));
  } else if (!tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx_id is invalid", K(ret), K(*this));
  } else if (op_type != NotifyType::REGISTER_SUCC && op_type != NotifyType::ON_ABORT && !op_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "op_scn is invalid", K(ret), K(*this));
  // mds frame not pass replay flag, get it from log handler
  } else if (OB_FAIL(ls_->get_log_handler()->get_role(ls_role, proposal_id))) {
    STORAGE_LOG(WARN, "get ls role fail", K(ret), K(*this));
  } else if (ObTxDataSourceType::TRANSFER_DEST_PREPARE != mds_type &&
      ObTxDataSourceType::TRANSFER_MOVE_TX_CTX != mds_type) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mds_type", K(ret), K(*this), K(mds_type));
  } else if (common::ObRole::FOLLOWER == ls_role) {
    is_follower = true;
  }
  if (OB_SUCC(ret)) {
    ObSpinLockGuard guard(lock_);
    if (is_follower) {
      if (OB_FAIL(replay_status_inner_(tx_id, task_id, op_scn, op_type, mds_type))) {
        STORAGE_LOG(WARN, "update transfer status", KR(ret), K(*this), K(tx_id), K(task_id));
      }
    } else {
      if (OB_FAIL(update_status_inner_(tx_id, task_id, op_scn, op_type, mds_type))) {
        STORAGE_LOG(WARN, "update transfer status", KR(ret), K(*this), K(tx_id), K(task_id));
      }
    }
    FLOG_INFO("update_transfer_status", K(ret), K(tx_id), K(task_id), K(op_scn), K(op_type), K(mds_type), K(*this));
  }
  return ret;
}

int ObLSTransferStatus::update_status_inner_(const transaction::ObTransID tx_id,
                                             const int64_t task_id,
                                             const share::SCN op_scn,
                                             const transaction::NotifyType op_type,
                                             const transaction::ObTxDataSourceType mds_type)
{
  int ret = OB_SUCCESS;
  // leader
  if (!transfer_tx_id_.is_valid() || transfer_tx_id_ == tx_id) {
    if (NotifyType::ON_COMMIT == op_type || NotifyType::ON_ABORT == op_type) {
      if (ObTxDataSourceType::TRANSFER_DEST_PREPARE == mds_type) {
        enable_upper_trans_calculation_(op_scn);
        reset_prepare_op();
      } else if (ObTxDataSourceType::TRANSFER_MOVE_TX_CTX == mds_type) {
        reset_move_tx_op();
      }
    } else {
      transfer_tx_id_ = tx_id;
      transfer_task_id_ = task_id;
      if (ObTxDataSourceType::TRANSFER_DEST_PREPARE == mds_type) {
        transfer_prepare_op_ = true;
        transfer_prepare_scn_ = op_scn;
        disable_upper_trans_calculation_();
      } else if (ObTxDataSourceType::TRANSFER_MOVE_TX_CTX == mds_type) {
        move_tx_op_ = true;
        move_tx_scn_ = op_scn;
      }
    }
  } else if (NotifyType::ON_ABORT == op_type) {
    TRANS_LOG(WARN, "has unfinish tx status when transfer abort can skip", K(*this), K(tx_id), K(task_id));
  } else if (NotifyType::ON_COMMIT == op_type) {
    TRANS_LOG(ERROR, "has unfinish tx status when transfer commit", K(*this), K(tx_id), K(task_id));
  } else {
    ret = OB_OP_NOT_ALLOW;
    TRANS_LOG(WARN, "has unfinish tx status", KR(ret), K(*this), K(tx_id), K(task_id));
  }
  return ret;
}

int ObLSTransferStatus::replay_status_inner_(const transaction::ObTransID tx_id,
                                             const int64_t task_id,
                                             const share::SCN op_scn,
                                             const transaction::NotifyType op_type,
                                             const transaction::ObTxDataSourceType mds_type)
{
  int ret = OB_SUCCESS;
  // follower replay filter
  if (ObTxDataSourceType::TRANSFER_DEST_PREPARE == mds_type) {
    if (!transfer_prepare_scn_.is_valid() || transfer_prepare_scn_ < op_scn) {
      if (NotifyType::ON_COMMIT == op_type || NotifyType::ON_ABORT == op_type) {
        enable_upper_trans_calculation_(op_scn);
        reset_prepare_op();
      } else {
        transfer_tx_id_ = tx_id;
        transfer_task_id_ = task_id;
        transfer_prepare_op_ = true;
        transfer_prepare_scn_ = op_scn;
        disable_upper_trans_calculation_();
      }
    }
  } else if (ObTxDataSourceType::TRANSFER_MOVE_TX_CTX == mds_type) {
    if (!move_tx_scn_.is_valid() || move_tx_scn_ < op_scn) {
      if (NotifyType::ON_COMMIT == op_type || NotifyType::ON_ABORT == op_type) {
        reset_move_tx_op();
      } else {
        transfer_tx_id_ = tx_id;
        transfer_task_id_ = task_id;
        move_tx_op_ = true;
        move_tx_scn_ = op_scn;
      }
    }
  }
  return ret;
}

int ObLSTransferStatus::get_transfer_prepare_status(
  bool &enable,
  share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSTransferStatus not init", K(ret), K(*this));
  } else {
    ObSpinLockGuard guard(lock_);
    enable = transfer_prepare_op_;
    scn = transfer_prepare_scn_;
  }
  return ret;
}

int ObLSTransferStatus::enable_upper_trans_calculation_(const share::SCN op_scn)
{
  int ret = OB_SUCCESS;
  ObTxTableGuard guard;
  ObTxTable *tx_table = nullptr;

  if (OB_FAIL(ls_->get_tx_table_guard(guard))) {
    TRANS_LOG(WARN, "failed to get tx table", K(ret));
  } else if (OB_UNLIKELY(!guard.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx table guard is invalid", K(ret), K(guard));
  } else if (OB_ISNULL(tx_table = guard.get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx data table in tx table is nullptr.", K(ret));
  } else {
    tx_table->enable_upper_trans_calculation(op_scn);
    TRANS_LOG(INFO, "enable upper trans calculation", KPC(ls_), K(guard), KPC(this));
  }

  return ret;
}

int ObLSTransferStatus::disable_upper_trans_calculation_()
{
  int ret = OB_SUCCESS;
  ObTxTableGuard guard;
  ObTxTable *tx_table = nullptr;

  if (OB_FAIL(ls_->get_tx_table_guard(guard))) {
    TRANS_LOG(WARN, "failed to get tx table", K(ret));
  } else if (OB_UNLIKELY(!guard.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx table guard is invalid", K(ret), K(guard));
  } else if (OB_ISNULL(tx_table = guard.get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx table is nullptr.", K(ret));
  } else {
    (void)tx_table->disable_upper_trans_calculation();
    TRANS_LOG(INFO, "disable upper trans calculation", KPC(ls_), K(guard), KPC(this));
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
