/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_wrs_handler.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace clog;
using namespace share;

namespace storage
{
int ObLSWRSHandler::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLSWRSHandler init twice", K(ret), K(is_inited_));
  } else {
    is_enabled_ = false;
    ls_weak_read_ts_.set_min();
    is_inited_ = true;
    ls_id_ = ls_id;
    STORAGE_LOG(INFO, "ObLSWRSHandler init success", K(*this));
  }

  return ret;
}

void ObLSWRSHandler::reset()
{
  is_inited_ = false;
  // set weak read ts to 0
  ls_weak_read_ts_.set_min();
  is_enabled_ = false;
  ls_id_.reset();
}

int ObLSWRSHandler::offline()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  is_enabled_ = false;
  // set weak read ts to 1
  ls_weak_read_ts_.set_base();
  STORAGE_LOG(INFO, "weak read handler disabled", K(*this));
  return ret;
}

int ObLSWRSHandler::online()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSWRSHandler not init", K(ret), K(*this));
  } else {
    ObSpinLockGuard guard(lock_);
    is_enabled_ = true;
    STORAGE_LOG(INFO, "weak read handler enabled", K(*this));
  }
  return ret;
}

int ObLSWRSHandler::generate_ls_weak_read_snapshot_version(ObLS &ls,
                                                           bool &need_skip,
                                                           bool &is_user_ls,
                                                           SCN &ret_wrs_version,
                                                           const int64_t max_stale_time,
                                                           const bool need_print)
{
  int ret = OB_SUCCESS;
  SCN wrs_scn;
  SCN gts_scn;
  SCN min_log_service_scn;
  SCN min_tx_service_scn;
  SCN end_scn;
  need_skip = false;

  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSWRSHandler not init", K(ret), K(is_inited_), K(ls));
  } else if (!is_enabled_) {
    need_skip = true;
    if (need_print || REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "weak read handler not enabled", K(*this));
    }
  } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), NULL, gts_scn))) {
    TRANS_LOG(WARN, "get gts scn error", K(ret), K(max_stale_time), K(*this));
  } else if (OB_FAIL(generate_weak_read_timestamp_(ls, max_stale_time, wrs_scn, min_log_service_scn, min_tx_service_scn, end_scn))) {
    need_skip = true;
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "fail to generate weak read timestamp", KR(ret), K(max_stale_time));
    }
    ret = OB_SUCCESS;
  } else {
    const bool in_transfer_prepare = ls.get_transfer_status().get_transfer_prepare_enable();
    int64_t snapshot_version_barrier = gts_scn.convert_to_ts() - max_stale_time;
    if (wrs_scn.convert_to_ts() <= snapshot_version_barrier) {
      // rule out these ls to avoid too old weak read timestamp
      need_skip = true;
      if (!in_transfer_prepare && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        // only print log when not in transfer status
        WRS_LOG(INFO, "wead read timestamp is too old", K(wrs_scn),
                                                        K(gts_scn),
                                                        K(max_stale_time),
                                                        K(*this));
      }
    } else {
      need_skip = false;
    }

    uint64_t log_service_delta = gts_scn >= min_log_service_scn ? gts_scn.convert_to_ts(true) - min_log_service_scn.convert_to_ts(true) : 0;
    uint64_t tx_service_delta = gts_scn >= min_tx_service_scn ? gts_scn.convert_to_ts(true) - min_tx_service_scn.convert_to_ts(true) : 0;
    uint64_t end_scn_delta = gts_scn >= end_scn ? gts_scn.convert_to_ts(true) - end_scn.convert_to_ts(true) : 0;
    if (need_print) {
      WRS_LOG(INFO, "[WRS] generate ls weak read timestamp", "tenant_id",
              MTL_ID(), "ls_id", ls.get_ls_id(),
              K(gts_scn), K(wrs_scn), K(end_scn), K(min_log_service_scn), K(min_tx_service_scn),
              "wrs_delta", gts_scn.convert_to_ts(true) - wrs_scn.convert_to_ts(true),
              K(end_scn_delta), K(log_service_delta), K(tx_service_delta),
              K(max_stale_time),
               K(in_transfer_prepare));
      // print keep alive info
      ls.get_keep_alive_ls_handler()->print_stat_info();
    } else {
      WRS_LOG(TRACE, "[WRS] generate ls weak read timestamp", "tenant_id",
              MTL_ID(), "ls_id", ls.get_ls_id(),
              K(gts_scn), K(wrs_scn), K(end_scn), K(min_log_service_scn), K(min_tx_service_scn),
              "wrs_delta", gts_scn.convert_to_ts(true) - wrs_scn.convert_to_ts(true),
              K(end_scn_delta), K(log_service_delta), K(tx_service_delta),
              K(max_stale_time),
              K(in_transfer_prepare));
    }
  }

  // put check transfer_prepare after generate wrs
  if (OB_SUCC(ret) && ls.get_transfer_status().get_transfer_prepare_enable()) {
    wrs_scn.reset();
    need_skip = true;
    if (need_print || REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      WRS_LOG(INFO, "ls in transfer status, skip it for generate wrs",
         "tenant_id", MTL_ID(), "ls_id", ls.get_ls_id(), K(*this));
    }
  }

  // check replica type
  if (OB_SUCC(ret) && false == need_skip) {
    if (ls_id_.is_sys_ls()) {
      is_user_ls = false;
    } else {
      is_user_ls = true;
    }
  }

  // update weak read timestamp
  if (OB_SUCC(ret)) {
    if (!need_skip) {
      ret_wrs_version = wrs_scn;
    }
  }

  if (wrs_scn.is_valid()) {
    // Update timestamp forcedly no matter how current ls leaves behind or not;
    ls_weak_read_ts_.inc_update(wrs_scn);
  }

  return ret;
}

int ObLSWRSHandler::generate_weak_read_timestamp_(ObLS &ls,
   const int64_t max_stale_time,
   SCN &wrs_scn,
   SCN &min_log_service_scn,
   SCN &min_tx_service_scn,
   SCN &end_scn)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();

  //the order of apply service、trx should not be changed
  if (OB_FAIL(ls.get_max_decided_scn(min_log_service_scn))) {
    if (OB_STATE_NOT_MATCH == ret) {
      // print one log per minute
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        STORAGE_LOG(WARN, "get_max_decided_log_ts_ns error", K(ret), K(ls_id));
      }
    } else {
      STORAGE_LOG(WARN, "get_max_decided_log_ts_ns error", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(ls.get_end_scn(end_scn))) {
    STORAGE_LOG(WARN, "get_end_scn error", K(ret), K(ls_id));
  } else if (OB_FAIL(ls.get_tx_svr()
                       ->get_trans_service()
                       ->get_ls_min_uncommit_prepare_version(ls_id,
                                                             min_tx_service_scn))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K(ls_id));
      }
    } else {
      STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K(ls_id));
    }
  } else {
    wrs_scn = SCN::min(min_log_service_scn, min_tx_service_scn);
  }

  return ret;
}

}
}
