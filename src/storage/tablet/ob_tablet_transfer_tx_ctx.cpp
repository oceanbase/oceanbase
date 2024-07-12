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

#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_transfer_tx_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace transaction;

OB_SERIALIZE_MEMBER(CollectTxCtxInfo, src_ls_id_, dest_ls_id_, task_id_, transfer_epoch_, transfer_scn_, args_);
OB_SERIALIZE_MEMBER(ObTxCtxMoveArg, tx_id_, epoch_, session_id_, tx_state_, trans_version_, prepare_version_, commit_version_, cluster_id_, cluster_version_, scheduler_, tx_expired_time_, xid_, last_seq_no_, max_submitted_seq_no_, tx_start_scn_, tx_end_scn_, is_sub2pc_, happened_before_, table_lock_info_);
OB_SERIALIZE_MEMBER(ObTransferDestPrepareInfo, task_id_, src_ls_id_, dest_ls_id_);
OB_SERIALIZE_MEMBER(ObTransferMoveTxParam, src_ls_id_, transfer_epoch_, transfer_scn_, op_scn_, op_type_, is_replay_, is_incomplete_replay_);

int CollectTxCtxInfo::assign(const CollectTxCtxInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(args_.assign(other.args_))) {
    LOG_WARN("collect tx ctx info assign failed", KR(ret), K(other));
  } else {
    src_ls_id_ = other.src_ls_id_;
    dest_ls_id_ = other.dest_ls_id_;
    task_id_ = other.task_id_;
    transfer_epoch_ = other.transfer_epoch_;
    transfer_scn_ = other.transfer_scn_;
  }
  return ret;
}

void ObTransferMoveTxParam::reset()
{
  src_ls_id_.reset();
  transfer_epoch_ = 0;
  transfer_scn_.reset();
  op_scn_.reset();
  op_type_ = NotifyType::UNKNOWN;
  is_replay_ = false;
  is_incomplete_replay_ = false;
}

void ObTransferOutTxParam::reset()
{
  except_tx_id_ = 0;
  data_end_scn_.reset();
  op_scn_.reset();
  op_type_ = NotifyType::UNKNOWN;
  is_replay_ = false;
  dest_ls_id_.reset();
  transfer_epoch_ = 0;
  move_tx_ids_ = nullptr;
}

void ObTransferOutTxCtx::reset()
{
  do_transfer_block_ = false;
  src_ls_id_.reset();
  dest_ls_id_.reset();
  data_end_scn_.reset();
  transfer_scn_.reset();
  transfer_epoch_ = 0;
  filter_tx_need_transfer_ = false;
  move_tx_ids_.reset();
}

bool ObTransferOutTxCtx::is_valid()
{
  return do_transfer_block_ &&
    src_ls_id_.is_valid() &&
    dest_ls_id_.is_valid() &&
    data_end_scn_.is_valid() &&
    transfer_scn_.is_valid() &&
    transfer_epoch_ > 0;
}

int ObTransferOutTxCtx::assign(const ObTransferOutTxCtx &other)
{
  int ret = OB_SUCCESS;
  const mds::MdsCtx &mds_ctx = static_cast<const mds::MdsCtx&>(other);
  if (OB_FAIL(MdsCtx::assign(mds_ctx))) {
    LOG_WARN("transfer out tx ctx assign failed", KR(ret), K(other));
  } else if (OB_FAIL(move_tx_ids_.assign(other.move_tx_ids_))) {
    LOG_WARN("assign array failed", KR(ret));
  } else {
    do_transfer_block_ = other.do_transfer_block_;
    src_ls_id_ = other.src_ls_id_;
    dest_ls_id_ = other.dest_ls_id_;
    data_end_scn_ = other.data_end_scn_;
    transfer_scn_ = other.transfer_scn_;
    transfer_epoch_ = other.transfer_epoch_;
    filter_tx_need_transfer_ = other.filter_tx_need_transfer_;
  }
  return ret;
}

int ObTransferOutTxCtx::record_transfer_block_op(const share::ObLSID src_ls_id,
                                                 const share::ObLSID dest_ls_id,
                                                 const share::SCN data_end_scn,
                                                 int64_t transfer_epoch,
                                                 bool is_replay,
                                                 bool filter_tx_need_transfer,
                                                 ObIArray<ObTransID> &move_tx_ids)
{
  int ret = OB_SUCCESS;
  if (!is_replay && do_transfer_block_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx do_transfer_block unexpectd", KR(ret), KP(this));
  } else if (OB_FAIL(move_tx_ids_.assign(move_tx_ids))) {
    LOG_WARN("assgin array failed", KR(ret));
  } else {
    src_ls_id_ = src_ls_id;
    dest_ls_id_ = dest_ls_id;
    data_end_scn_ = data_end_scn;
    transfer_epoch_ = transfer_epoch;
    do_transfer_block_ = true;
    filter_tx_need_transfer_ = filter_tx_need_transfer;
  }
  return ret;
}

void ObTransferOutTxCtx::on_redo(const share::SCN &redo_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_out_tx on_redo", K(redo_scn), K(tx_id), KP(this), KPC(this));
  mds::MdsCtx::on_redo(redo_scn);
  transfer_scn_ = redo_scn;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  int64_t active_tx_count = 0;
  int64_t block_tx_count = 0;
  ObTransferOutTxParam param;
  param.except_tx_id_ = get_writer().writer_id_;
  param.data_end_scn_ = data_end_scn_;
  param.op_scn_ = redo_scn;
  param.op_type_ = transaction::NotifyType::ON_REDO;
  param.is_replay_ = false;
  param.dest_ls_id_ = dest_ls_id_;
  param.transfer_epoch_ = transfer_epoch_;
  if (filter_tx_need_transfer_) {
    param.move_tx_ids_ = &move_tx_ids_;
  } else {
    param.move_tx_ids_ = nullptr;
  }

  while (true) {
    int ret = OB_SUCCESS;
    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer out tx ctx invalid state", KR(ret), K(tx_id), KP(this), KPC(this));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(src_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(tx_id), KP(this));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), KP(this), KP(ls));
    } else if (!empty_tx() && OB_FAIL(ls->transfer_out_tx_op(param, active_tx_count, block_tx_count))) {
      LOG_WARN("transfer out tx failed", KR(ret), K(tx_id), KP(this));
    }
    if (OB_FAIL(ret)) {
      ob_usleep(10 * 1000);
    } else {
      break;
    }
  }
}

void ObTransferOutTxCtx::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_out_tx on_commit", K(commit_version), K(commit_scn), K(tx_id), KP(this), KPC(this));
  int ret = OB_SUCCESS;
  mds::MdsCtx::on_commit(commit_version, commit_scn);
  ObTransferOutTxParam param;
  param.except_tx_id_ = get_writer().writer_id_;
  param.data_end_scn_ = data_end_scn_;
  param.op_scn_ = commit_scn;
  param.op_type_ = transaction::NotifyType::ON_COMMIT;
  param.is_replay_ = false;
  param.dest_ls_id_ = dest_ls_id_;
  param.transfer_epoch_ = transfer_epoch_;
  if (filter_tx_need_transfer_) {
    param.move_tx_ids_ = &move_tx_ids_;
  } else {
    param.move_tx_ids_ = nullptr;
  }
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    int64_t active_tx_count = 0;
    int64_t op_tx_count = 0;
    int64_t start_time = ObTimeUtility::current_time();

    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer out tx ctx invalid state", KR(ret), K(tx_id), KP(this), KPC(this));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(src_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(writer_), KP(this));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), KP(this));
    } else if (!empty_tx() && OB_FAIL(ls->transfer_out_tx_op(param, active_tx_count, op_tx_count))) {
      LOG_WARN("transfer out tx op failed", KR(ret), K(tx_id), KP(this));
    } else {
      int64_t end_time = ObTimeUtility::current_time();
      LOG_INFO("transfer out tx op commit", KR(ret), KP(this),
          K(active_tx_count), K(op_tx_count), "cost", end_time - start_time);
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferOutTxCtx on_commit fail", KR(ret), K(commit_version), K(commit_scn), K(tx_id), KPC(this));
      }
    }
  }
}

void ObTransferOutTxCtx::on_abort(const share::SCN &abort_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_out_tx on_abort", K(abort_scn), K(tx_id), KP(this), KPC(this));
  mds::MdsCtx::on_abort(abort_scn);
  ObTransferOutTxParam param;
  param.except_tx_id_ = get_writer().writer_id_;
  param.data_end_scn_ = data_end_scn_;
  param.op_scn_ = abort_scn;
  param.op_type_ = transaction::NotifyType::ON_ABORT;
  param.is_replay_ = false;
  param.dest_ls_id_ = dest_ls_id_;
  param.transfer_epoch_ = transfer_epoch_;
  if (filter_tx_need_transfer_) {
    param.move_tx_ids_ = &move_tx_ids_;
  } else {
    param.move_tx_ids_ = nullptr;
  }
  if (do_transfer_block_) {
    while (true) {
      int ret = OB_SUCCESS;
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      int64_t active_tx_count = 0;
      int64_t op_tx_count = 0;

      if (!src_ls_id_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transfer out tx ctx invalid state", KR(ret), K(tx_id), KPC(this));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(src_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
        LOG_WARN("fail to get ls", KR(ret), K(tx_id), KP(this));
      } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", KR(ret), KP(this));
      } else if (!empty_tx() && OB_FAIL(ls->transfer_out_tx_op(param, active_tx_count, op_tx_count))) {
        LOG_WARN("transfer out tx op failed", KR(ret), K(tx_id), KP(this));
      }
      if (OB_SUCC(ret)) {
        break;
      } else {
        ob_usleep(10 * 1000);
        if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
          LOG_ERROR("ObTransferOutTxCtx on_abort fail", KR(ret), K(abort_scn), K(tx_id), KPC(this));
        }
      }
    }
  }
}

int ObStartTransferMoveTxHelper::on_register(const char* buf, const int64_t len, mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  SCN op_scn;
  ObTransferMoveTxCtx &transfer_move_tx_ctx = static_cast<ObTransferMoveTxCtx&>(ctx);
  CollectTxCtxInfo &collect_tx_info = transfer_move_tx_ctx.get_collect_tx_info();
  transaction::ObTransID tx_id = transfer_move_tx_ctx.get_writer().writer_id_;
  bool start_modify = false;
  LOG_INFO("TransferMoveTx on_register", K(tx_id));

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register move tx get invalid argument", KR(ret), KP(buf), K(len));
  } else if (collect_tx_info.is_valid() || transfer_move_tx_ctx.get_op_scn().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx state is valid before register", KR(ret), K(transfer_move_tx_ctx));
  } else if (CLICK_FAIL(collect_tx_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize collect tx ctx info", KR(ret), K(len), K(pos));
  } else if (!collect_tx_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register collect_tx_info is valid", KR(ret), K(collect_tx_info));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(collect_tx_info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(transfer_move_tx_ctx));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(transfer_move_tx_ctx), KP(ls));
  } else if (FALSE_IT(start_modify = true)) {
  } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, SCN(),
          NotifyType::REGISTER_SUCC, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
    LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
  } else {
    int64_t start_time = ObTimeUtil::current_time();
    ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                        collect_tx_info.transfer_epoch_,
                                        collect_tx_info.transfer_scn_,
                                        op_scn,
                                        transaction::NotifyType::REGISTER_SUCC,
                                        false,
                                        false);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
        LOG_WARN("move tx op failed", KR(ret), K(tx_id), K(transfer_move_tx_ctx));
      } else {
        break;
      }
      if (ObTimeUtil::current_time() - start_time > 5 * 1000 * 1000) {
        break;
      } else if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(10 * 1000);
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (start_modify && OB_TMP_FAIL(clean(ls, tx_id, collect_tx_info))) {
      LOG_ERROR("TransferMoveTx clean failed", K(tmp_ret), K(tx_id));
    }
  }
  LOG_INFO("[TRANSFER] TransferMoveTx on_register", KR(ret), K(len), K(tx_id),
          "tx_count", collect_tx_info.args_.count(), KP(&transfer_move_tx_ctx), K(collect_tx_info));
  return ret;
}


int ObStartTransferMoveTxHelper::clean(ObLS *ls, transaction::ObTransID tx_id, CollectTxCtxInfo &collect_tx_info)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                      collect_tx_info.transfer_epoch_,
                                      collect_tx_info.transfer_scn_,
                                      SCN(),
                                      transaction::NotifyType::ON_ABORT,
                                      false,
                                      false);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, SCN(),
          NotifyType::ON_ABORT, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
      LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
    } else if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
      LOG_WARN("move tx op failed", KR(ret), K(tx_id));
    } else {
      break;
    }
    if (OB_FAIL(ret)) {
      int64_t cost = ObTimeUtil::current_time() - start_time;
      if (cost > 500 * 1000) {
        LOG_WARN("move_tx clean tool long time", KR(ret), K(ls->get_ls_id()), K(tx_id), K(cost));
      }
      // retry
      ret = OB_SUCCESS;
      ob_usleep(10 * 1000);
    }
  }
  return ret;
}

int ObStartTransferMoveTxHelper::on_replay(const char* buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTransferMoveTxCtx &transfer_move_tx_ctx = static_cast<ObTransferMoveTxCtx&>(ctx);
  CollectTxCtxInfo &collect_tx_info = transfer_move_tx_ctx.get_collect_tx_info();
  transaction::ObTransID tx_id = transfer_move_tx_ctx.get_writer().writer_id_;
  LOG_INFO("TransferMoveTx on_replay", K(tx_id), KP(&collect_tx_info), K(collect_tx_info));

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("move tx get invalid argument", KR(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(transfer_move_tx_ctx.get_collect_tx_info().deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize collect tx ctx info", KR(ret), K(len), K(pos));
  } else if (!transfer_move_tx_ctx.get_collect_tx_info().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collect_tx_info is valid", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(collect_tx_info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(collect_tx_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(collect_tx_info), KP(ls));
  } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, scn,
          NotifyType::ON_REDO, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
    LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
  } else {
    ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                        collect_tx_info.transfer_epoch_,
                                        collect_tx_info.transfer_scn_,
                                        scn,
                                        transaction::NotifyType::ON_REDO,
                                        true,
                                        transfer_move_tx_ctx.is_incomplete_replay());
    if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
      LOG_WARN("move tx ctx failed", KR(ret), K(collect_tx_info));
    } else {
      LOG_INFO("[TRANSFER] TransferMoveTx on_replay", KR(ret), K(tx_id));
    }
  }
  return ret;
}

ObTransferMoveTxCtx::ObTransferMoveTxCtx()
  : writer_(), op_scn_(), collect_tx_info_()
{}

void ObTransferMoveTxCtx::reset()
{
  op_scn_.reset();
  collect_tx_info_.reset();
}

void ObTransferMoveTxCtx::set_writer(const mds::MdsWriter &writer)
{
  writer_.writer_type_ = writer.writer_type_;
  writer_.writer_id_ = writer.writer_id_;
}

const mds::MdsWriter ObTransferMoveTxCtx::get_writer() const { return writer_; }

int ObTransferMoveTxCtx::assign(const ObTransferMoveTxCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collect_tx_info_.assign(other.collect_tx_info_))) {
    LOG_WARN("move_tx_ctx assign failed", KR(ret), K(other));
  } else {
    writer_ = other.writer_;
    op_scn_ = other.op_scn_;
  }
  return ret;
}

void ObTransferMoveTxCtx::on_redo(const share::SCN &redo_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("move_tx_ctx on_redo", K(redo_scn), K(tx_id), KP(this));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    CollectTxCtxInfo &collect_tx_info = collect_tx_info_;
    if (!collect_tx_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("TRANSFER collect_tx_info is invalid", KR(ret), K(collect_tx_info), K(op_scn_), K(writer_), KP(this));
    } else if ((!op_scn_.is_valid() || op_scn_ < redo_scn) && FALSE_IT(op_scn_ = redo_scn)) {
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(collect_tx_info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(writer_), K(collect_tx_info), KP(this));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(collect_tx_info), KP(ls));
    } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, redo_scn,
          NotifyType::REGISTER_SUCC, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
      LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
    } else {
      ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                          collect_tx_info.transfer_epoch_,
                                          collect_tx_info.transfer_scn_,
                                          redo_scn,
                                          transaction::NotifyType::ON_REDO,
                                          false,
                                          is_incomplete_replay());
      if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
        LOG_WARN("move tx ctx failed", KR(ret), K(collect_tx_info), K(tx_id), KP(this), K(redo_scn));
      } else {
        LOG_INFO("[TRANSFER] move_tx_ctx", KR(ret), K(redo_scn), K(tx_id), KP(this));
      }
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferMoveTxCtx on_redo fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}

void ObTransferMoveTxCtx::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("move_tx_ctx on_commit", K(commit_version), K(commit_scn), K(tx_id), KP(this));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    CollectTxCtxInfo &collect_tx_info = collect_tx_info_;
    if (!collect_tx_info.is_valid() || !op_scn_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("TRANSFER collect_tx_info is invalid", KR(ret), K(collect_tx_info), K(op_scn_));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(collect_tx_info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(collect_tx_info));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(collect_tx_info), KP(ls));
    } else {
      ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                          collect_tx_info.transfer_epoch_,
                                          collect_tx_info.transfer_scn_,
                                          commit_scn,
                                          transaction::NotifyType::ON_COMMIT,
                                          false,
                                          is_incomplete_replay());
      if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
        LOG_WARN("move tx ctx failed", KR(ret), K(collect_tx_info), K(commit_scn));
      } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, commit_scn,
          NotifyType::ON_COMMIT, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
        LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
      } else {
        LOG_INFO("[TRANSFER] move_tx_ctx", KR(ret), K(commit_version), K(commit_scn), K(writer_), KP(this));
      }
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferMoveTxCtx on_commit fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}

void ObTransferMoveTxCtx::on_abort(const share::SCN &abort_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("move_tx_ctx on_abort", K(abort_scn), K(writer_), KP(this));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    CollectTxCtxInfo &collect_tx_info = collect_tx_info_;
    if (!collect_tx_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("TRANSFER collect_tx_info is invalid", KR(ret), K(collect_tx_info), K(op_scn_), K(abort_scn));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(collect_tx_info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(collect_tx_info));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(collect_tx_info), KP(ls));
    } else {
      ObTransferMoveTxParam move_tx_param(collect_tx_info.src_ls_id_,
                                          collect_tx_info.transfer_epoch_,
                                          collect_tx_info.transfer_scn_,
                                          abort_scn,
                                          transaction::NotifyType::ON_ABORT,
                                          false,
                                          is_incomplete_replay());
      if (OB_FAIL(ls->move_tx_op(move_tx_param, collect_tx_info.args_))) {
        LOG_WARN("move tx ctx failed", KR(ret), K(collect_tx_info), K(abort_scn));
      } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, collect_tx_info.task_id_, abort_scn,
          NotifyType::ON_ABORT, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX))) {
        LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
      } else {
        LOG_INFO("[TRANSFER] move_tx_ctx", KR(ret), K(writer_), KP(this), K(abort_scn));
      }
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferMoveTxCtx on_abort fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}

int ObStartTransferDestPrepareHelper::on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObTransferDestPrepareTxCtx &user_ctx = static_cast<ObTransferDestPrepareTxCtx&>(ctx);
  ObTransferDestPrepareInfo &info = user_ctx.get_info();
  transaction::ObTransID tx_id = user_ctx.get_writer().writer_id_;
  LOG_INFO("transfer_dest_prepare register", K(tx_id));

  if (OB_FAIL(info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize transfer dest prepare info", KR(ret), K(len), K(pos));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer_dest_prepare invalid param", KR(ret), K(info));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(info), KP(ls));
  } else if (OB_FAIL(ls->get_transfer_status().update_status(tx_id, info.task_id_, SCN(),
          NotifyType::REGISTER_SUCC, ObTxDataSourceType::TRANSFER_DEST_PREPARE))) {
    LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
  }
  return ret;
}

int ObStartTransferDestPrepareHelper::on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObTransferDestPrepareTxCtx &user_ctx = static_cast<ObTransferDestPrepareTxCtx&>(ctx);
  ObTransferDestPrepareInfo &info = user_ctx.get_info();
  transaction::ObTransID tx_id = user_ctx.get_writer().writer_id_;
  LOG_INFO("transfer_dest_prepare on_replay", K(tx_id), K(scn));

  if (OB_FAIL(info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize transfer dest prepare info", KR(ret), K(len), K(pos));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer_dest_prepare invalid param", KR(ret), K(info));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(info));
  } else if (OB_FAIL(ls_handle.get_ls()->get_transfer_status().update_status(tx_id, info.task_id_, scn,
          NotifyType::ON_REDO, ObTxDataSourceType::TRANSFER_DEST_PREPARE))) {
    LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
  }
  return ret;
}

void ObTransferDestPrepareTxCtx::reset()
{
  op_scn_.reset();
  transfer_dest_prepare_info_.reset();
}

int ObTransferDestPrepareInfo::assign(const ObTransferDestPrepareInfo& other)
{
  int ret = OB_SUCCESS;
  task_id_ = other.task_id_;
  src_ls_id_ = other.src_ls_id_;
  dest_ls_id_ = other.dest_ls_id_;
  return ret;
}

int ObTransferDestPrepareTxCtx::assign(const ObTransferDestPrepareTxCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_dest_prepare_info_.assign(other.transfer_dest_prepare_info_))) {
    LOG_WARN("transfer dest prepare info assign failed", KR(ret), K(other));
  } else {
    writer_ = other.writer_;
    op_scn_ = other.op_scn_;
  }
  return ret;
}

void ObTransferDestPrepareTxCtx::set_writer(const mds::MdsWriter &writer)
{
  writer_.writer_type_ = writer.writer_type_;
  writer_.writer_id_ = writer.writer_id_;
}

const mds::MdsWriter ObTransferDestPrepareTxCtx::get_writer() const { return writer_; }

void ObTransferDestPrepareTxCtx::on_redo(const share::SCN &redo_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_dest_prepare on_redo", K(tx_id), K(this), K(redo_scn));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTransferDestPrepareInfo &info = get_info();
    if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer dest prepare info is invalid", KR(ret), K(tx_id), KP(this), KPC(this));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(tx_id), K(transfer_dest_prepare_info_));
    } else if ((!op_scn_.is_valid() || op_scn_ < redo_scn) && FALSE_IT(op_scn_ = redo_scn)) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_transfer_status().update_status(tx_id, info.task_id_, redo_scn,
            NotifyType::ON_REDO, ObTxDataSourceType::TRANSFER_DEST_PREPARE))) {
      LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferDestPrepareTxCtx on_redo fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}

// TODO we could recover dest_ls weak_read_ts advance before on_commit just after move_tx_ctx
void ObTransferDestPrepareTxCtx::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_dest_prepare on_commit", K(tx_id), K(this), K(commit_scn));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTransferDestPrepareInfo &info = get_info();
    if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer dest prepare info is invalid", KR(ret), K(tx_id), KP(this), KPC(this));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(info.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(tx_id), K(transfer_dest_prepare_info_));
    } else if (OB_FAIL(ls_handle.get_ls()->get_transfer_status().update_status(tx_id, info.task_id_, commit_scn,
            NotifyType::ON_COMMIT, ObTxDataSourceType::TRANSFER_DEST_PREPARE))) {
      LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferDestPrepareTxCtx on_commit fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}

void ObTransferDestPrepareTxCtx::on_abort(const share::SCN &abort_scn)
{
  transaction::ObTransID tx_id = writer_.writer_id_;
  LOG_INFO("transfer_dest_prepare on_abort", K(tx_id), K(this), K(abort_scn));
  while (true) {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTransferDestPrepareInfo &info = get_info();
    if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer dest prepare info is invalid", KR(ret), K(tx_id), KP(this), KPC(this));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(transfer_dest_prepare_info_.dest_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(transfer_dest_prepare_info_), K(tx_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_transfer_status().update_status(tx_id, info.task_id_, abort_scn,
            NotifyType::ON_ABORT, ObTxDataSourceType::TRANSFER_DEST_PREPARE))) {
      LOG_WARN("update transfer status failed", KR(ret), K(tx_id));
    }
    if (OB_SUCC(ret)) {
      break;
    } else {
      ob_usleep(10 * 1000);
      if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
        LOG_ERROR("ObTransferDestPrepareTxCtx on_abort fail", KR(ret), K(tx_id), KP(this));
      }
    }
  }
}


} // end storage
} // end oceanbase
