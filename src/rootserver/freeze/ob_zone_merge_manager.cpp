/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS_COMPACTION


#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "ob_zone_merge_manager.h"
#include "ob_major_freeze_util.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_freeze_info_proxy.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::palf;

ObZoneMergeManagerBase::ObZoneMergeManagerBase()
  : lock_(ObLatchIds::ZONE_MERGE_MANAGER_READ_LOCK),
    is_inited_(false), is_loaded_(false),
    tenant_id_(common::OB_INVALID_ID), global_merge_info_(),
    proxy_(NULL)
{}

int ObZoneMergeManagerBase::init(const uint64_t tenant_id, ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = &proxy;
    is_inited_ = true;
    is_loaded_ = false;
  }
  return ret;
}

int ObZoneMergeManagerBase::reload()
{
  int ret = OB_SUCCESS;

  LOG_INFO("start to reload zone_merge_mgr", K_(tenant_id), K_(is_loaded), K_(global_merge_info));
  HEAP_VAR(ObGlobalMergeInfo, global_merge_info) {
    global_merge_info.tenant_id_ = tenant_id_;

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*proxy_, tenant_id_,
                          global_merge_info, true/*print_sql*/))) {
      LOG_WARN("fail to get global merge info", KR(ret), K_(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(reset_merge_info())) {
        LOG_WARN("fail to reset merge info", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(global_merge_info_.assign(global_merge_info))) {
        LOG_WARN("fail to assign", KR(ret), K(global_merge_info));
      }
    }

    if (OB_SUCC(ret)) {
      is_loaded_ = true;
      FLOG_INFO("succ to reload zone merge manager", K_(global_merge_info));
    } else {
      LOG_WARN("fail to reload zone merge manager", KR(ret));
    }
  }
  return ret;
}

int ObZoneMergeManagerBase::try_reload()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (is_loaded_) {
    if (TC_REACH_TIME_INTERVAL(5 * 60 * 1000 * 1000)) { // 5min
      FLOG_INFO("zone_merge_mgr is already loaded", K_(tenant_id), K_(global_merge_info));
    }
  } else if (OB_FAIL(reload())) {
    LOG_WARN("fail to reload", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObZoneMergeManagerBase::reset_merge_info()
{
  global_merge_info_.reset();
  is_loaded_ = false;
  FLOG_INFO("reset merge info", K_(tenant_id), K_(global_merge_info));
  return OB_SUCCESS;
}

int ObZoneMergeManagerBase::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !is_loaded_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner_stat_error", K_(is_inited), K_(is_loaded), KR(ret));
  }
  return ret;
}

int ObZoneMergeManagerBase::check_freeze_service_epoch(
    ObMySQLTransaction &trans,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int64_t persistent_epoch = -1;
  if (expected_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expected_epoch));
  } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(trans, tenant_id_,
             ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, persistent_epoch))) {
    LOG_WARN("fail to select freeze_service_epoch for update", KR(ret), K_(tenant_id));
  } else if (persistent_epoch != expected_epoch) {
    ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
    LOG_WARN("freeze service epoch mismatch", KR(ret), K(expected_epoch), K(persistent_epoch));
  }
  return ret;
}

void ObZoneMergeManagerBase::handle_trans_stat(
    ObMySQLTransaction &trans,
    int &ret)
{
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN_RET(tmp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
}

int ObZoneMergeManagerBase::get_snapshot(
    ObGlobalMergeInfo &global_merge_info)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  global_merge_info.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(global_merge_info.assign(global_merge_info_))) {
    LOG_WARN("fail to assign", KR(ret), K_(global_merge_info));
  }
  return ret;
}

int ObZoneMergeManagerBase::suspend_merge(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const bool is_suspend = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(suspend_or_resume_zone_merge(is_suspend, expected_epoch))) {
    LOG_WARN("fail to suspend merge", KR(ret), K_(tenant_id), K(is_suspend), K(expected_epoch));
  }
  return ret;
}

int ObZoneMergeManagerBase::resume_merge(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const bool is_suspend = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(suspend_or_resume_zone_merge(is_suspend, expected_epoch))) {
    LOG_WARN("fail to resume merge", KR(ret), K_(tenant_id), K(is_suspend), K(expected_epoch));
  }
  return ret;
}

int ObZoneMergeManagerBase::set_merge_status(
    const ObGlobalMergeInfo::MergeErrorType error_type,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  if ((error_type >= ObGlobalMergeInfo::MergeErrorType::ERROR_TYPE_MAX)
      || (error_type < ObGlobalMergeInfo::MergeErrorType::NONE_ERROR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(error_type));
  } else {
    ObMySQLTransaction trans;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    int64_t is_merge_error = 1;
    if (error_type == ObGlobalMergeInfo::MergeErrorType::NONE_ERROR) {
      is_merge_error = 0;
    }

    FREEZE_TIME_GUARD;
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
      LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
    } else {
      ObGlobalMergeInfo tmp_global_info;
      if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
      } else {
        tmp_global_info.is_merge_error_.set_val(is_merge_error, true);
        tmp_global_info.set_error_type(error_type, true);

        FREEZE_TIME_GUARD;
        if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_,
            tmp_global_info))) {
          LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
        }

        handle_trans_stat(trans, ret);

        if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
          LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
        } else {
          LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succ to set merge status", K_(tenant_id), K(error_type), K(global_merge_info_.is_merge_error_));
      ROOTSERVICE_EVENT_ADD("daily_merge", "set_merge_error", K_(tenant_id), K(is_merge_error), K(error_type));
    }
  }
  return ret;
}

int ObZoneMergeManagerBase::check_need_broadcast(
    const SCN &frozen_scn,
    bool &need_broadcast)
{
  int ret = OB_SUCCESS;
  need_broadcast = false;
  if (OB_UNLIKELY(!frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if ((global_merge_info_.frozen_scn() < frozen_scn)
             && GCONF.enable_major_freeze) { // require enable_major_freeze = true
    need_broadcast = true;
  }
  return ret;
}

int ObZoneMergeManagerBase::set_global_freeze_info(
    const SCN &frozen_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);

  bool need_broadcast = false;
  if (OB_FAIL(check_need_broadcast(frozen_scn, need_broadcast))) {
    LOG_WARN("fail to check_need_broadcast", KR(ret), K_(tenant_id), K(frozen_scn));
  } else if (!need_broadcast) {
    LOG_INFO("no need set global freeze info", K(frozen_scn), K_(global_merge_info));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
    LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
  } else {
    ObGlobalMergeInfo tmp_global_info;
    if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
      LOG_WARN("fail to assign global merge info", KR(ret), K_(tenant_id));
    } else {
      tmp_global_info.frozen_scn_.set_scn(frozen_scn, true);
      tmp_global_info.set_merge_mode(ObGlobalMergeInfo::MergeMode::MERGE_MODE_TENANT, true); // stop window compaction when invoking tenant major freeze
      if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_,
          tmp_global_info))) {
        LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
      }

      handle_trans_stat(trans, ret);

      if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
      } else {
        LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
      }
    }
  }

  LOG_INFO("finish set global freeze info", KR(ret), K_(tenant_id), K(frozen_scn), K(need_broadcast));
  return ret;
}

int ObZoneMergeManagerBase::set_window_compaction_info(
    const ObWindowCompactionParam &param,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  const ObGlobalMergeInfo::MergeStatus origin_merge_status =
      global_merge_info_.merge_status();
  const ObGlobalMergeInfo::MergeMode origin_merge_mode =
      global_merge_info_.merge_mode();
  bool has_set = false;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(param));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE != origin_merge_status) {
    ret = OB_MAJOR_FREEZE_NOT_FINISHED;
    LOG_WARN("cannot do window compaction now, need wait current major_freeze finish", KR(ret), K_(tenant_id), K(global_merge_info_));
  } else if (OB_UNLIKELY(!GCONF.enable_major_freeze)) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major freeze is disabled, no need to set window compaction info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
    LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
  } else {
    ObGlobalMergeInfo tmp_global_info;
    if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
      LOG_WARN("fail to assign global merge info", KR(ret), K_(tenant_id));
    } else {
      if (param.with_start_ts_) {
        tmp_global_info.merge_start_time_.set_val(param.window_start_time_us_, true);
      } else {
        tmp_global_info.merge_start_time_.set_val(ObTimeUtility::current_time(), true);
      }
      tmp_global_info.set_merge_mode(ObGlobalMergeInfo::MergeMode::MERGE_MODE_WINDOW, true);
      tmp_global_info.set_merge_status(
          ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MERGING, true);
      if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_, tmp_global_info))) {
        LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
      }

      handle_trans_stat(trans, ret);

      if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
      } else {
        has_set = true;
        LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
        ROOTSERVICE_EVENT_ADD("window_compaction", "set_window_start_ts", K_(tenant_id), K(param));
      }
    }
  }

  LOG_INFO("finish set window compaction info", KR(ret), K_(tenant_id), K(has_set), K(origin_merge_status),
           K(origin_merge_mode), K(param));
  return ret;
}

int ObZoneMergeManagerBase::finish_window_compaction(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObGlobalMergeInfo tmp_global_info;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!global_merge_info_.is_window_merge_mode() || global_merge_info_.is_idle_status())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("window compaction is already finished", KR(ret), K_(tenant_id), K_(global_merge_info));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
    LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
  } else if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
    LOG_WARN("fail to assign global merge info", KR(ret), K_(tenant_id));
  } else if (FALSE_IT(tmp_global_info.set_merge_status(
                 ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE,
                 true))) {
  } else if (OB_FAIL(
                 ObGlobalMergeTableOperator::update_partial_global_merge_info(
                     trans, tenant_id_, tmp_global_info))) {
    LOG_WARN("fail to update partial global merge info", KR(ret),
             K(tmp_global_info));
  }

  handle_trans_stat(trans, ret);

  if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
    LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
  } else {
    LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
  }
  LOG_INFO("finish finish_window_compaction", KR(ret), K_(tenant_id), K(expected_epoch));
  return ret;
}

int ObZoneMergeManagerBase::get_global_broadcast_scn(SCN &global_broadcast_scn) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_broadcast_scn = global_merge_info_.global_broadcast_scn();
  }
  return ret;
}

int ObZoneMergeManagerBase::get_global_last_merged_scn(SCN &global_last_merged_scn) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_last_merged_scn =  global_merge_info_.last_merged_scn();
  }
  return ret;
}

int ObZoneMergeManagerBase::get_global_merge_status(ObGlobalMergeInfo::MergeStatus &global_merge_status) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_merge_status = global_merge_info_.merge_status();
  }
  return ret;
}

int ObZoneMergeManagerBase::get_global_last_merged_time(int64_t &global_last_merged_time) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_last_merged_time = global_merge_info_.last_merged_time_.get_value();
  }
  return ret;
}

int ObZoneMergeManagerBase::get_global_merge_start_time(int64_t &global_merge_start_time) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_merge_start_time = global_merge_info_.merge_start_time_.get_value();
  }
  return ret;
}

int ObZoneMergeManagerBase::get_global_merge_mode(ObGlobalMergeInfo::MergeMode &global_merge_mode) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    global_merge_mode = global_merge_info_.merge_mode();
  }
  return ret;
}

int ObZoneMergeManagerBase::generate_next_global_broadcast_scn(
    const int64_t expected_epoch,
    SCN &next_scn)
{
  int ret = OB_SUCCESS;
  FREEZE_TIME_GUARD;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (global_merge_info_.is_merge_error()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be is_merge_error", KR(ret), K_(global_merge_info));
  } else if (global_merge_info_.last_merged_scn() < global_merge_info_.global_broadcast_scn()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not merged yet", "last_merged_scn", global_merge_info_.last_merged_scn(),
             "global_broadcast_scn", global_merge_info_.global_broadcast_scn(), KR(ret),
             K_(tenant_id));
  } else if (global_merge_info_.last_merged_scn() > global_merge_info_.global_broadcast_scn()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("last_merged_scn must not larger than global_broadcast_scn", KR(ret),
              K_(tenant_id), "last_merged_scn", global_merge_info_.last_merged_scn(),
              "global_broadcast_scn", global_merge_info_.global_broadcast_scn());
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
    LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
  } else {
    ObGlobalMergeInfo tmp_global_info;
    ObZoneMergeInfo tmp_zone_info;
    tmp_zone_info.tenant_id_ = tenant_id_;
    // zone_ is intentionally left unset: update_tenant_all_zone_merge_info uses only
    // tenant_id as PK and updates all zone rows for this tenant in one SQL statement.
    if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
      LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
    } else {
      if (global_merge_info_.global_broadcast_scn() < global_merge_info_.frozen_scn()) {
        // only when global_broadcast_scn is less than global frozen_scn, we can use
        // frozen_scn to start major_freeze
        next_scn = global_merge_info_.frozen_scn();
        tmp_global_info.global_broadcast_scn_.set_scn(next_scn, true);
        const int64_t cur_time = ObTimeUtility::current_time();
        tmp_global_info.merge_start_time_.set_val(cur_time, true);
        tmp_zone_info.merge_start_time_.set_val(cur_time, true);
      } else if (global_merge_info_.global_broadcast_scn() == global_merge_info_.frozen_scn()) {
        next_scn = global_merge_info_.global_broadcast_scn();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global_broadcast_scn must not larger than global frozen_scn", KR(ret),
          K_(global_merge_info));
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("next global_broadcast_scn", K_(tenant_id), K(next_scn), K(tmp_global_info));

        tmp_global_info.set_merge_status(
            ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MERGING,
            true);
        tmp_global_info.set_merge_mode(
            ObGlobalMergeInfo::MergeMode::MERGE_MODE_TENANT,
            true); // stop window compaction when invoking tenant major freeze
        tmp_zone_info.set_merge_status(
            ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MERGING,
            true);
        tmp_zone_info.is_merging_.set_val(1, true);
        tmp_zone_info.broadcast_scn_.set_scn(next_scn, true);
        tmp_zone_info.frozen_scn_.set_scn(global_merge_info_.frozen_scn(), true);

        FREEZE_TIME_GUARD;
        if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_,
            tmp_global_info))) {
          LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
        } else if (OB_FAIL(ObZoneMergeTableOperator::update_tenant_all_zone_merge_info(trans, tenant_id_, tmp_zone_info))) {
          LOG_WARN("fail to update all zones merge info", KR(ret), K_(tenant_id), K(tmp_zone_info));
        }
      }

      handle_trans_stat(trans, ret);

      if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
      } else {
        LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
      }
    }
  }

  return ret;
}

// if all zones finished merge & checksum checking, we may need to update global merge info and all zone merge infos
int ObZoneMergeManagerBase::try_update_global_last_merged_scn(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else {
    // here, we don't check is_merge_error, cuz merge & chcksum already finished.
    // we need to do some update work at last. is_merge_error will be caught in next round
    const int64_t cur_time = ObTimeUtility::current_time();
    if (global_merge_info_.is_in_merge()) {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
        LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
      } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
        LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
      } else {
        // after all zones finished merge, update global merge info and all zone merge infos
        ObGlobalMergeInfo tmp_global_info;
        if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
          LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
        } else {
          const int64_t cur_time = ObTimeUtility::current_time();
          tmp_global_info.last_merged_time_.set_val(cur_time, true);
          tmp_global_info.last_merged_scn_.set_scn(global_merge_info_.global_broadcast_scn(), true);
          tmp_global_info.set_merge_status(
              ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE,
              true);

          // zone_ is intentionally left unset: update_tenant_all_zone_merge_info uses only
          // tenant_id as PK and updates all zone rows for this tenant in one SQL statement.
          // broadcast_scn_/frozen_scn_ are NOT updated here; they were already set
          // when the merge started in generate_next_global_broadcast_scn.
          ObZoneMergeInfo tmp_zone_info;
          tmp_zone_info.tenant_id_ = tenant_id_;
          tmp_zone_info.is_merging_.set_val(0, true);
          tmp_zone_info.last_merged_scn_.set_scn(global_merge_info_.global_broadcast_scn(), true);
          tmp_zone_info.last_merged_time_.set_val(cur_time, true);
          tmp_zone_info.set_merge_status(
              ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE,
              true);
          tmp_zone_info.all_merged_scn_.set_scn(global_merge_info_.global_broadcast_scn(), true);

          FREEZE_TIME_GUARD;
          if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_,
              tmp_global_info))) {
            LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::update_tenant_all_zone_merge_info(trans, tenant_id_, tmp_zone_info))) {
            LOG_WARN("fail to update all zones merge info", KR(ret), K_(tenant_id), K(tmp_zone_info));
          }

          handle_trans_stat(trans, ret);

          if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
            LOG_WARN("fail to assign global merge info", KR(ret), K_(tenant_id), K(tmp_global_info));
          } else {
            LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
          }
        }
      }
    }
  }
  return ret;
}

// after finishing merge(before checksum checking), update global merge info
int ObZoneMergeManagerBase::update_global_merge_info_after_merge(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (global_merge_info_.is_in_verifying_status()) {
    LOG_INFO("already in verifying status, no need to update global merge status again", K_(tenant_id),
             "global merge status", global_merge_info_.merge_status_);
  } else if (global_merge_info_.is_merge_error()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("should not update global merge status, cuz is_merge_error is true", KR(ret), K_(global_merge_info));
  } else {
    if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
      LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
    } else {
      ObGlobalMergeInfo tmp_global_info;
      if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
      } else {
        tmp_global_info.set_merge_status(
            ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_VERIFYING,
            true);
        if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_,
            tmp_global_info))) {
          LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
        }

        handle_trans_stat(trans, ret);

        if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
          LOG_WARN("fail to assign global merge info", KR(ret), K_(tenant_id), K(tmp_global_info));
        } else {
          LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
        }
      }
    }
  }
  return ret;
}

int ObZoneMergeManagerBase::try_update_zone_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  // 1. get zone_list of current tenant from __all_tenant when previous_locality is empty
  ObArray<ObZone> zone_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_tenant_zone_list(zone_list))) {
    LOG_WARN("fail to get tenant zone list", KR(ret), K_(tenant_id));
  } else if (zone_list.count() > 0) {
    ObMySQLTransaction trans;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
      LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
    } else if (OB_FAIL(
            ObZoneMergeTableOperator::sync_zone_merge_info_with_zone_list(
                trans, tenant_id_, zone_list))) {
      LOG_WARN("fail to sync tenant zone merge info by zone_list", KR(ret), K_(tenant_id), K(zone_list));
    }
    handle_trans_stat(trans, ret);
  }

  return ret;
}

int ObZoneMergeManagerBase::adjust_global_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObFreezeInfo max_frozen_status;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  SCN min_compaction_scn;
  SCN max_frozen_scn;
  // 1. get min{compaction_scn} of all tablets in __all_tablet_meta_table
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_min_compaction_scn(tenant_id_, min_compaction_scn))) {
    LOG_WARN("fail to get min_compaction_scn", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(min_compaction_scn < SCN::base_scn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected min_compaction_scn", KR(ret), K_(tenant_id), K(min_compaction_scn));
  } else if (min_compaction_scn == SCN::base_scn()) {
    // do nothing. no need to adjust global_merge_info
  } else if (min_compaction_scn > SCN::base_scn()) {
    /*  case 1 : min{compaction_scn} is a medium scn
     *  return max{frozen_scn} which is smaller than or equal to curr medium scn from __all_freeze_info
     *  case 2 : min{compaction_scn} is a tenant major scn
     *  max{frozen_scn} must be equal to min{compaction_scn}, return max{frozen_scn}
     */
    if (OB_FAIL(freeze_info_proxy.get_max_frozen_scn_smaller_or_equal_than(*proxy_,
                min_compaction_scn, max_frozen_scn))) {
      LOG_WARN("fail to get max frozen_scn smaller than or equal to min_compaction_scn", KR(ret),
               K_(tenant_id), K(min_compaction_scn));
    } else if (max_frozen_scn < SCN::base_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max_frozen_scn", KR(ret), K_(tenant_id), K(max_frozen_scn));
    } else if (max_frozen_scn == SCN::base_scn()) {
      // do nothing. no need to adjust global_merge_info
    } else if (max_frozen_scn > SCN::base_scn()) {
      // 3. if max{frozen_scn} > 1, update __all_merge_info and global_merge_info with max{frozen_scn}
      if (OB_FAIL(inner_adjust_global_merge_info(max_frozen_scn, expected_epoch))) {
        LOG_WARN("fail to inner adjust global merge info", KR(ret), K_(tenant_id), K(max_frozen_scn));
      }
    }
  }
  FLOG_INFO("finish to adjust global merge info", K_(tenant_id), K(min_compaction_scn), K(max_frozen_scn), K_(global_merge_info));
  return ret;
}

int ObZoneMergeManagerBase::suspend_or_resume_zone_merge(
    const bool is_suspend,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);

  if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
    LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
  } else {
    ObGlobalMergeInfo tmp_global_info;
    if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
      LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
    } else {
      tmp_global_info.suspend_merging_.set_val(is_suspend, true);
      if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_, tmp_global_info))) {
        LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
      }

      handle_trans_stat(trans, ret);

      if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K(tmp_global_info));
      } else {
        LOG_INFO("succ to update global merge info", K_(tenant_id), "latest global merge_info", tmp_global_info);
      }
    }
  }

  return ret;
}

int ObZoneMergeManagerBase::get_tenant_zone_list(common::ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND previous_locality = ''",
          OB_ALL_TENANT_TNAME, tenant_id_))) {
        LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) { // result is empty
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next", KR(ret), K_(tenant_id), K(sql));
        }
      } else {
        int64_t tmp_real_str_len = 0; // used to fill output argument
        SMART_VAR(char[MAX_ZONE_LIST_LENGTH], zone_list_str) {
          zone_list_str[0] = '\0';
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone_list", zone_list_str,
                                    MAX_ZONE_LIST_LENGTH, tmp_real_str_len);
          if (FAILEDx(str2zone_list(zone_list_str, zone_list))) {
            LOG_WARN("fail to str2zone_list", KR(ret), K(zone_list_str));
          }
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ret)) {
        //nothing todo
      } else if (OB_ITER_END != (tmp_ret = result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  return ret;
}

int ObZoneMergeManagerBase::str2zone_list(
    const char *str,
    ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  zone_list.reuse();
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is null", KP(str), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ";", &save_ptr);
      if (NULL != item_str) {
        if (OB_FAIL(zone_list.push_back(ObZone(item_str)))) {
          LOG_WARN("fail to push_back", KR(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObZoneMergeManagerBase::inner_adjust_global_merge_info(
    const SCN &frozen_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!frozen_scn.is_valid() || expected_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(frozen_scn), K(expected_epoch));
  } else {
    // 1. adjust global_merge_info in memory to control the frozen_scn of the next major compaction.
    // 2. adjust global_merge_info in table for background thread to update report_scn.
    //
    // Note that, here not only adjust last_merged_scn, but also adjust global_broadcast_scn and
    // frozen_scn. So as to avoid error in ObMajorMergeScheduler::do_work(), which works based on
    // these global_merge_info in memory.
    ObMySQLTransaction trans;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(check_freeze_service_epoch(trans, expected_epoch))) {
      LOG_WARN("fail to check freeze_service_epoch", KR(ret), K(expected_epoch));
    } else {
      ObGlobalMergeInfo tmp_global_info;
      if (OB_FAIL(tmp_global_info.assign_value(global_merge_info_))) {
        LOG_WARN("fail to assign global merge info", KR(ret), K_(global_merge_info));
      } else {
        tmp_global_info.frozen_scn_.set_scn(frozen_scn, true);
        tmp_global_info.global_broadcast_scn_.set_scn(frozen_scn, true);
        tmp_global_info.last_merged_scn_.set_scn(frozen_scn, true);
        if (OB_FAIL(ObGlobalMergeTableOperator::update_partial_global_merge_info(trans, tenant_id_, tmp_global_info))) {
          LOG_WARN("fail to update partial global merge info", KR(ret), K(tmp_global_info));
        }
        handle_trans_stat(trans, ret);
        if (FAILEDx(global_merge_info_.assign_value(tmp_global_info))) {
          LOG_WARN("fail to assign global_merge_info", KR(ret), K(tmp_global_info), K_(global_merge_info));
        } else {
          LOG_INFO("succ to update global_merge_info", K_(tenant_id), K(tmp_global_info), K_(global_merge_info));
        }
      }
    }
  }
  return ret;
}

// only used for copying data to/from shadow_
int ObZoneMergeManagerBase::copy_infos(
    ObZoneMergeManagerBase &dest,
    const ObZoneMergeManagerBase &src)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest.global_merge_info_.assign(src.global_merge_info_))) {
    LOG_WARN("fail to assign", KR(ret), "info", src.global_merge_info_);
  } else {
    dest.is_inited_ = src.is_inited_;
    dest.is_loaded_ = src.is_loaded_;
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
ObZoneMergeManager::ObZoneMergeMgrGuard::ObZoneMergeMgrGuard(
    const SpinRWLock &lock,
    ObZoneMergeManagerBase &zone_merge_mgr,
    ObZoneMergeManagerBase &shadow,
    int &ret)
    :  lock_(const_cast<SpinRWLock &>(lock)), zone_merge_mgr_(zone_merge_mgr),
       shadow_(shadow), ret_(ret)
{
  SpinRLockGuard copy_guard(lock_);
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(OB_SUCCESS !=
      (tmp_ret = ObZoneMergeManager::copy_infos(shadow_, zone_merge_mgr_)))) {
    LOG_WARN("fail to copy to zone_merge_mgr shadow", K(tmp_ret), K_(ret));
  }
  if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
    ret_ = tmp_ret;
  }
}

ObZoneMergeManager::ObZoneMergeMgrGuard::~ObZoneMergeMgrGuard()
{
  SpinWLockGuard copy_guard(lock_);
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(OB_SUCCESS !=
      (tmp_ret = ObZoneMergeManager::copy_infos(zone_merge_mgr_, shadow_)))) {
    LOG_WARN_RET(tmp_ret, "fail to copy from zone_merge_mgr shadow", K(tmp_ret), K_(ret));
  }
  if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
    ret_ = tmp_ret;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
ObZoneMergeManager::ObZoneMergeManager()
  : write_lock_(ObLatchIds::ZONE_MERGE_MANAGER_WRITE_LOCK), shadow_()
{}

ObZoneMergeManager::~ObZoneMergeManager()
{}

int ObZoneMergeManager::init(const uint64_t tenant_id, ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObZoneMergeManagerBase::init(tenant_id, proxy))) {
    LOG_WARN("fail to init zone_merge_manager_base", KR(ret), K(tenant_id));
  } else if (OB_FAIL(shadow_.init(tenant_id, proxy))) {
    LOG_WARN("fail to init zone_merge_mgr_base shadow_", KR(ret), K(tenant_id));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
