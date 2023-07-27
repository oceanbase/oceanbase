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

#include "storage/tx/wrs/ob_ls_wrs_handler.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_service.h"

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
    is_enabled_ = true;
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
                                                           SCN &wrs_version,
                                                           const int64_t max_stale_time)
{
  int ret = OB_SUCCESS;
  SCN timestamp;
  SCN gts_scn;
  need_skip = false;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLSWRSHandler not init", K(ret), K(is_inited_), K(ls));
  } else if (!is_enabled_) {
    // do nothing
    need_skip = true;
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "weak read handler not enabled", K(*this));
    }
  } else if (OB_FAIL(generate_weak_read_timestamp_(ls, max_stale_time, timestamp))) {
    STORAGE_LOG(DEBUG, "fail to generate weak read timestamp", KR(ret), K(max_stale_time));
    need_skip = true;
    ret = OB_SUCCESS;
  } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), NULL, gts_scn))) {
    TRANS_LOG(WARN, "get gts scn error", K(ret), K(max_stale_time), K(*this));
  } else if (OB_FAIL(ls.get_migration_status(status))
                  || ObMigrationStatus::OB_MIGRATION_STATUS_NONE == status ) {
    // check the weak read timestamp of the migrated ls
    if (timestamp.convert_to_ts() > gts_scn.convert_to_ts() - 500 * 1000) {
      STORAGE_LOG(TRACE, "ls received the latest log", K(timestamp));
      // clog chases within 500ms, then clear the mark
      need_skip = false;
    } else {
      need_skip = true;
    }
  } else {
    int64_t snapshot_version_barrier = gts_scn.convert_to_ts() - max_stale_time;
    if (timestamp.convert_to_ts() <= snapshot_version_barrier) {
      // rule out these ls to avoid too old weak read timestamp
      need_skip = true;
    } else {
      need_skip = false;
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
      wrs_version = timestamp;
    }
    // Update timestamp forcedly no matter how current ls leaves behind or not;
    ls_weak_read_ts_.inc_update(timestamp);
  }

  return ret;
}

int ObLSWRSHandler::generate_weak_read_timestamp_(ObLS &ls, const int64_t max_stale_time, SCN &timestamp)
{
  int ret = OB_SUCCESS;
  SCN min_log_service_scn, min_tx_service_ts;
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
  } else if (OB_FAIL(ls.get_tx_svr()
                       ->get_trans_service()
                       ->get_ls_min_uncommit_prepare_version(ls_id,
                                                             min_tx_service_ts))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K(ls_id));
      }
    } else {
      STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K(ls_id));
    }
  } else {
    timestamp = SCN::min(min_log_service_scn, min_tx_service_ts);
    const int64_t current_us = ObClockGenerator::getClock();
    if (current_us - timestamp.convert_to_ts() > 3000 * 1000L /*3s*/
        || REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      TRANS_LOG(INFO, "get wrs ts", K(ls_id),
                                    "delta", current_us - timestamp.convert_to_ts(),
                                    "log_service_ts", min_log_service_scn.convert_to_ts(),
                                    "min_tx_service_ts", min_tx_service_ts.convert_to_ts(),
                                    K(timestamp));
      // print keep alive info
      ls.get_keep_alive_ls_handler()->print_stat_info();
    }
  }

  return ret;
}

}
}
