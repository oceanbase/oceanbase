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

#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle

namespace oceanbase
{
using namespace share;
namespace transaction
{

int ObITxLogAdapter::block_confirm_with_dup_tablet_change_snapshot(
    share::SCN &dup_tablet_change_snapshot)
{
  dup_tablet_change_snapshot.set_invalid();
  return OB_SUCCESS;
}

int ObITxLogAdapter::unblock_confirm_with_prepare_scn(const share::SCN &dup_tablet_change_snapshot,
                                                      const share::SCN &prepare_scn)
{
  UNUSED(dup_tablet_change_snapshot);
  UNUSED(prepare_scn);
  return OB_SUCCESS;
}

int ObITxLogAdapter::check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                                              bool &is_dup_tablet,
                                              const share::SCN &base_snapshot,
                                              const share::SCN &redo_scn)
{
  UNUSED(tablet_id);
  UNUSED(redo_scn);
  UNUSED(base_snapshot);
  is_dup_tablet = false;
  return OB_SUCCESS;
}

int ObITxLogAdapter::check_dup_tablet_readable(const ObTabletID &tablet_id,
                                               const share::SCN &read_snapshot,
                                               const bool read_from_leader,
                                               const share::SCN &max_replayed_scn,
                                               bool &readable)
{
  UNUSED(tablet_id);
  UNUSED(read_snapshot);
  UNUSED(read_from_leader);
  UNUSED(max_replayed_scn);
  readable = false;
  return OB_SUCCESS;
}

int ObITxLogAdapter::check_redo_sync_completed(const ObTransID &tx_id,
                                               const share::SCN &redo_completed_scn,
                                               bool &redo_sync_finish,
                                               share::SCN &total_max_read_version)
{
  UNUSED(tx_id);
  UNUSED(redo_completed_scn);
  redo_sync_finish = false;
  total_max_read_version.set_invalid();
  return OB_SUCCESS;
}

int ObITxLogAdapter::get_committing_dup_trx_cnt(int64_t &dup_trx_cnt)
{
  int ret = OB_SUCCESS;

  dup_trx_cnt = 0;

  return ret;
}

int ObITxLogAdapter::add_commiting_dup_trx(const ObTransID &tx_id)
{
  UNUSED(tx_id);
  return OB_SUCCESS;
}

int ObITxLogAdapter::remove_commiting_dup_trx(const ObTransID &tx_id)
{
  UNUSED(tx_id);
  return OB_SUCCESS;
}

void ObLSTxLogAdapter::reset()
{
  log_handler_ = nullptr;
  dup_table_ls_handler_ = nullptr;
  tx_table_ = nullptr;
}

int ObLSTxLogAdapter::init(ObITxLogParam *param, ObTxTable *tx_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_NOT_NULL(log_handler_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", KR(ret), KP(param), KP(log_handler_));
  } else {
    ObTxPalfParam *palf_param = static_cast<ObTxPalfParam *>(param);
    log_handler_ = palf_param->get_log_handler();
    dup_table_ls_handler_ = palf_param->get_dup_table_ls_handler();
    tx_table_ = tx_table;
  }
  return ret;
}

int ObLSTxLogAdapter::submit_log(const char *buf,
                                 const int64_t size,
                                 const SCN &base_scn,
                                 ObTxBaseLogCb *cb,
                                 const bool need_nonblock)
{
  int ret = OB_SUCCESS;
  palf::LSN lsn;
  SCN scn;

  if (NULL == buf || 0 >= size || OB_ISNULL(cb) || !base_scn.is_valid() ||
      base_scn.convert_to_ts() > ObTimeUtility::current_time() + 86400000000L) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size), K(base_scn), KP(cb));
  } else if (OB_ISNULL(log_handler_) || !log_handler_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(log_handler_));
  } else if (OB_FAIL(log_handler_->append(buf, size, base_scn, need_nonblock, cb, lsn, scn))) {
    TRANS_LOG(WARN, "append log to palf failed", K(ret), KP(log_handler_), KP(buf), K(size), K(base_scn),
              K(need_nonblock));
  } else {
    cb->set_base_ts(base_scn);
    cb->set_lsn(lsn);
    cb->set_log_ts(scn);
    cb->set_submit_ts(ObTimeUtility::current_time());
    ObTransStatistic::get_instance().add_clog_submit_count(MTL_ID(), 1);
    ObTransStatistic::get_instance().add_trans_log_total_size(MTL_ID(), size);
  }
  TRANS_LOG(DEBUG, "ObLSTxLogAdapter::submit_ls_log", KR(ret), KP(cb));

  return ret;
}

int ObLSTxLogAdapter::get_role(bool &is_leader, int64_t &epoch)
{
  int ret = OB_SUCCESS;

  ObRole role = INVALID_ROLE;
  if (OB_ISNULL(log_handler_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(log_handler_));
  } else if (OB_FAIL(log_handler_->get_role(role, epoch))) {
    if (ret == OB_NOT_INIT || ret == OB_NOT_RUNNING) {
      ret = OB_SUCCESS;
      is_leader = false;
    } else {
      TRANS_LOG(WARN, "get role failed", K(ret));
    }
  } else if (LEADER == role) {
    is_leader = true;
  } else {
    is_leader = false;
  }

  return ret;
}

int ObLSTxLogAdapter::get_max_decided_scn(SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_handler_) || !log_handler_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(log_handler_));
  } else {
    ret = log_handler_->get_max_decided_scn(scn);
  }
  return ret;
}

int ObLSTxLogAdapter::get_append_mode_initial_scn(share::SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_handler_) || !log_handler_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(log_handler_));
  } else {
    ret = log_handler_->get_append_mode_initial_scn(ref_scn);
  }
  return ret;
}

int ObLSTxLogAdapter::block_confirm_with_dup_tablet_change_snapshot(
    share::SCN &dup_tablet_change_snapshot)
{
  int ret = OB_SUCCESS;

  dup_tablet_change_snapshot.set_invalid();

  return ret;
}

int ObLSTxLogAdapter::unblock_confirm_with_prepare_scn(const share::SCN &dup_tablet_change_snapshot,
                                                       const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObLSTxLogAdapter::check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                                               bool &is_dup_tablet,
                                               const share::SCN &base_snapshot,
                                               const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "invalid dup table ls handler", K(ret));
  } else if (OB_FAIL(dup_table_ls_handler_->check_dup_tablet_in_redo(tablet_id, is_dup_tablet,
                                                                     base_snapshot, redo_scn))) {
    DUP_TABLE_LOG(WARN, "check dup tablet readable failed", K(ret));
  }

  return ret;
}

int ObLSTxLogAdapter::check_dup_tablet_readable(const ObTabletID &tablet_id,
                                                const share::SCN &read_snapshot,
                                                const bool read_from_leader,
                                                const share::SCN &max_replayed_scn,
                                                bool &readable)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "invalid dup table ls handler", K(ret));
  } else if (OB_FAIL(dup_table_ls_handler_->check_dup_tablet_readable(
                 tablet_id, read_snapshot, read_from_leader, max_replayed_scn, readable))) {
    DUP_TABLE_LOG(WARN, "check dup tablet readable failed", K(ret));
  }
  return ret;
}

int ObLSTxLogAdapter::check_redo_sync_completed(const ObTransID &tx_id,
                                                const share::SCN &redo_completed_scn,
                                                bool &redo_sync_finish,
                                                share::SCN &total_max_read_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "invalid dup table ls handler", K(ret));
  } else if (OB_FAIL(dup_table_ls_handler_->check_redo_sync_completed(
                 tx_id, redo_completed_scn, redo_sync_finish, total_max_read_version))) {
    DUP_TABLE_LOG(WARN, "check redo sync completed failed", K(ret));
  }

  return ret;
}

bool ObLSTxLogAdapter::is_dup_table_lease_valid()
{
  bool is_follower_lease = false;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    DUP_TABLE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid dup table ls handler");
  } else {
    is_follower_lease = dup_table_ls_handler_->is_dup_table_lease_valid();
  }

  return is_follower_lease;
}

bool ObLSTxLogAdapter::has_dup_tablet()
{
  bool has_dup = false;
  if (OB_ISNULL(dup_table_ls_handler_)) {
    has_dup = false;
  } else {
    has_dup = dup_table_ls_handler_->has_dup_tablet();
  }
  return has_dup;
}

int ObLSTxLogAdapter::get_committing_dup_trx_cnt(int64_t &dup_trx_cnt)
{
  int ret = OB_SUCCESS;

  dup_trx_cnt = 0;
  if (OB_ISNULL(dup_table_ls_handler_) || OB_ISNULL(tx_table_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(dup_table_ls_handler_), KP(tx_table_));
  } else {
    dup_trx_cnt = dup_table_ls_handler_->get_committing_dup_trx_cnt();
    if (0 == dup_trx_cnt) {
      ObTxTableGuard tx_table_guard;
      if (OB_FAIL(tx_table_->get_tx_table_guard(tx_table_guard))) {
        TRANS_LOG(WARN, "get tx table guard failed", K(ret), K(dup_trx_cnt), K(tx_table_guard));
      } else if (tx_table_guard.check_ls_offline()) {
        ret = OB_LS_OFFLINE;
        TRANS_LOG(WARN, "The ls has been offline", K(ret), K(dup_trx_cnt), K(tx_table_guard));
      }
    }
  }
  return dup_trx_cnt;
}

int ObLSTxLogAdapter::add_commiting_dup_trx(const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dup table ls handler", KP(dup_table_ls_handler_), K(tx_id),
              KPC(log_handler_));
  } else if (OB_FAIL(dup_table_ls_handler_->add_commiting_dup_trx(tx_id))) {
    TRANS_LOG(WARN, "add commiting dup trx failed", K(ret), K(tx_id));
  }

  return ret;
}

int ObLSTxLogAdapter::remove_commiting_dup_trx(const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dup_table_ls_handler_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dup table ls handler", KP(dup_table_ls_handler_), K(tx_id),
              KPC(log_handler_));
  } else if (OB_FAIL(dup_table_ls_handler_->remove_commiting_dup_trx(tx_id))) {
    TRANS_LOG(WARN, "remove commiting dup trx failed", K(ret), K(tx_id));
  }

  return ret;
}

}
}
