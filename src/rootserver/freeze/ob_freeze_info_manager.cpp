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

#define USING_LOG_PREFIX RS

#include "rootserver/freeze/ob_freeze_info_manager.h"

#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_root_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/utility.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_global_stat_proxy.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_snapshot_table_proxy.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/wrs/ob_weak_read_util.h"
#include "share/ob_server_table_operator.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace palf;

namespace rootserver
{
/****************************** ObFreezeInfo ******************************/
int ObFreezeInfo::assign(const ObFreezeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    frozen_statuses_.reset();
    if (OB_FAIL(frozen_statuses_.assign(other.frozen_statuses_))) {
      LOG_WARN("fail to assign", KR(ret), K(other));
    } else {
      latest_snapshot_gc_scn_ = other.latest_snapshot_gc_scn_;
    }
  }
  return ret;
}

int ObFreezeInfo::get_latest_frozen_scn(SCN &frozen_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    frozen_scn =  frozen_statuses_.at(frozen_statuses_.count() - 1).frozen_scn_;
  }
  return ret;
}

int ObFreezeInfo::get_min_freeze_info_greater_than(
    const SCN &frozen_scn,
    share::ObSimpleFrozenStatus &frozen_status) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    int64_t idx = -1;
    int64_t cache_count = frozen_statuses_.count();
    SCN max_cache_frozen_scn = SCN::min_scn();
    for (int64_t i = 0; i < cache_count; i++) {
      max_cache_frozen_scn = ((frozen_statuses_.at(i).frozen_scn_ > max_cache_frozen_scn)
                              ? frozen_statuses_.at(i).frozen_scn_ : max_cache_frozen_scn);
      if (frozen_statuses_.at(i).frozen_scn_ > frozen_scn) {
        idx = i;
        break;
      }
    }

    if (idx >= 0) {
      frozen_status = frozen_statuses_.at(idx);
    } else {
      if (max_cache_frozen_scn == frozen_scn) {
        LOG_TRACE("no more larger frozen_scn", K(frozen_scn), K_(frozen_statuses));
      } else if (max_cache_frozen_scn < frozen_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max cached frozen_scn should not less than frozen_scn", KR(ret), K(frozen_scn),
          K(max_cache_frozen_scn), K_(frozen_statuses));
      }
    }
  }
  return ret;
}

int ObFreezeInfo::get_frozen_status(
    const SCN &frozen_scn,
    share::ObSimpleFrozenStatus &frozen_status) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid freeze info", KR(ret), KPC(this));
  } else {
    int64_t idx = -1;
    for (int64_t i = 0; i < frozen_statuses_.count(); i++) {
      if (frozen_statuses_.at(i).frozen_scn_ == frozen_scn) {
        idx = i;
        break;
      }
    }

    if (idx >= 0) {
      frozen_status = frozen_statuses_.at(idx);
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not found freeze_info", KR(ret), KPC(this), K(frozen_scn));
    }
  }
  return ret;
}

/****************************** ObFreezeInfoManager ******************************/
int ObFreezeInfoManager::init(
    uint64_t tenant_id,
    common::ObMySQLProxy &proxy,
    ObZoneMergeManager &merge_info_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &proxy;
    merge_info_mgr_ = &merge_info_mgr;
    is_inited_ = true;
  }

  return ret;
}

int ObFreezeInfoManager::reload()
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  ObFreezeInfo freeze_info;
  if (OB_FAIL(inner_reload(freeze_info))) {
    LOG_WARN("fail to reload freeze info", KR(ret));
  } else if (!freeze_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid freeze info", KR(ret), K(freeze_info));
  } else if (OB_FAIL(freeze_info_.assign(freeze_info))) {
    LOG_WARN("fail to set freeze info, need reload", KR(ret));
    freeze_info_.set_invalid();
  } else {
    LOG_INFO("succ to reload freeze info manager", K_(tenant_id), K(freeze_info));
  }
  return ret;
}

int ObFreezeInfoManager::try_reload()
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (freeze_info_.is_valid()) {
  } else if (OB_FAIL(reload())) {
    LOG_WARN("fail to reload", KR(ret));
  }
  return ret;
}

int ObFreezeInfoManager::get_global_last_merged_scn(SCN &global_last_merged_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("merge info mgr is null", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_info_mgr_->get_global_last_merged_scn(global_last_merged_scn))) {
    LOG_WARN("fail to get global_last_merged_scn", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoManager::get_global_broadcast_scn(SCN &global_broadcast_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("merge info mgr is null", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_info_mgr_->get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get global_broadcast_scn", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoManager::adjust_global_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("merge info mgr is null", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->adjust_global_merge_info(expected_epoch))) {
    LOG_WARN("fail to adjust global merge info", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObFreezeInfoManager::reset_freeze_info()
{
  freeze_info_.set_invalid();
}

// reload will acquire latest freeze info from __all_freeze_info.
int ObFreezeInfoManager::inner_reload(ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  freeze_info.reset();

  ObSEArray<ObSimpleFrozenStatus, 4> simple_frozen_statuses;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  SCN global_broadcast_scn;

  if (OB_FAIL(merge_info_mgr_->try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_info_mgr_->get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get broadcast version", KR(ret), K_(tenant_id));
  // 1. get snapshot_gc_scn
  } else if (OB_FAIL(ObGlobalStatProxy::get_snapshot_gc_scn(
             *sql_proxy_, tenant_id_, freeze_info.latest_snapshot_gc_scn_))) {
    LOG_WARN("fail to select for update snapshot_gc_scn", KR(ret), K_(tenant_id));
  // 2. acquire freeze info in same trans, ensure we can get the latest freeze info
  } else if (OB_FAIL(freeze_info_proxy.get_freeze_info_larger_or_equal_than(
             *sql_proxy_, global_broadcast_scn, simple_frozen_statuses))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(global_broadcast_scn));
  } else if (OB_UNLIKELY(simple_frozen_statuses.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid frozen status", KR(ret), K(global_broadcast_scn));
  } else {
    std::sort(simple_frozen_statuses.begin(), simple_frozen_statuses.end(),
              [](const ObSimpleFrozenStatus &a, const ObSimpleFrozenStatus &b)
                 { return a.frozen_scn_ < b.frozen_scn_; });
  }

  if (FAILEDx(freeze_info.frozen_statuses_.assign(simple_frozen_statuses))) {
    LOG_WARN("fail to assign", KR(ret), K(simple_frozen_statuses));
  } else {
    LOG_INFO("inner load succ", K(freeze_info));
  }

  if (OB_FAIL(ret)) {
    freeze_info.set_invalid();
  }

  return ret;
}

int ObFreezeInfoManager::set_freeze_info()
{
  int ret = OB_SUCCESS;
  SCN new_frozen_scn;
  SCN remote_snapshot_gc_scn;
  ObSimpleFrozenStatus frozen_status;

  ObRecursiveMutexGuard guard(lock_);

  // not to check newest schema_version
  int64_t fake_schema_version = 1000;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
    // freeze get_schema_version need interactive with ddl trans but don't use gen_new_schema_version so no need check_in_rs
    // freeze disable check_newest_schema so not to use schema_guard
    ObDDLSQLTransaction trans(GCTX.schema_service_, false/*need_end_signal*/, false/*stash*/, false/*parallel*/, false/*check_in_rs*/, false/*check_newest_schema*/);

    // In 'ddl_sql_transaction.start()', it implements the semantics of 'lock_all_ddl_operation'.
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_, fake_schema_version))) {
      if ((OB_TRANS_TIMEOUT == ret) || (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret)) {
        ret = OB_EAGAIN; // in order to try launch major freeze again, set ret = OB_EAGAIN here
        LOG_WARN("ddl conflict, will try to launch major freeze again", KR(ret), K_(tenant_id));
      } else {
        LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(fake_schema_version));
      }
    // 1. lock snapshot_gc_ts in __all_global_stat
    } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(
              trans, tenant_id_, remote_snapshot_gc_scn))) {
      LOG_WARN("fail to select for update", KR(ret));
    } else {
      int64_t schema_version_in_frozen_ts = 0;
      uint64_t data_version = 0;

      // 2. generate new frozen_scn
      if (OB_FAIL(generate_frozen_scn(freeze_info_, remote_snapshot_gc_scn, new_frozen_scn))) {
        LOG_WARN("fail to generate frozen timestamp", KR(ret));
      // 3. get schema_version at frozen_scn
      } else if (OB_FAIL(get_schema_version(new_frozen_scn, schema_version_in_frozen_ts))) {
        LOG_WARN("fail to get schema version", KR(ret), K(new_frozen_scn));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
      } else {
        frozen_status.frozen_scn_ = new_frozen_scn;
        frozen_status.schema_version_ = schema_version_in_frozen_ts;
        frozen_status.data_version_ = data_version;

        // 4. insert freeze info
        if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, frozen_status))) {
          LOG_WARN("fail to set freeze info", KR(ret), K(frozen_status), K_(tenant_id));
        }
      }
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        ret = ((OB_SUCC(ret)) ? tmp_ret : ret);
        LOG_WARN("fail to end trans", "is_commit", OB_SUCCESS == ret,  KR(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_.frozen_statuses_.push_back(frozen_status))) {
      LOG_WARN("fail to push back", KR(ret), K(frozen_status));
    } else {
      LOG_INFO("succ to set new freeze_info", K_(tenant_id), K_(freeze_info));
    }
  }

  if (OB_FAIL(ret)) {
    freeze_info_.set_invalid();
  }

  LOG_INFO("finish set freeze info", KR(ret), K(frozen_status), K_(tenant_id));
  ROOTSERVICE_EVENT_ADD("root_service", "root_major_freeze", K_(tenant_id),
                        K(ret), "new_frozen_scn", new_frozen_scn.get_val_for_inner_table_field());
  return ret;
}

int ObFreezeInfoManager::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;;
    LOG_WARN("inner stat error", KR(ret));
  } else if ((nullptr == sql_proxy_) || (nullptr == merge_info_mgr_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), KP_(sql_proxy), KP_(merge_info_mgr));
  } else if (OB_FAIL(try_reload())) {
    LOG_INFO("fail to reload", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoManager::get_schema_version(
    const SCN &frozen_scn,
    int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  ObSchemaService *server_schema_service = nullptr;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_ISNULL(server_schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_schema_service is null", KR(ret));
  } else {
    ObRefreshSchemaStatus status;
    status.tenant_id_ = tenant_id_;
    // TODO snapshot_timestamp_ should be SCN
    status.snapshot_timestamp_ = frozen_scn.get_val_for_inner_table_field();

    if (OB_FAIL(server_schema_service->fetch_schema_version(status, *sql_proxy_, schema_version))) {
      LOG_WARN("fail to fetch schema version", KR(ret), K(status));
    }
  }

  return ret;
}

int ObFreezeInfoManager::generate_frozen_scn(
    const ObFreezeInfo &freeze_info,
    const SCN &snapshot_gc_scn,
    SCN &new_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  ObSnapshotInfo snapshot_info;

  // build index or backup will acquire snapshot,
  // so should make sure frozen_scn will be greater max snapshot_ts.
  if (OB_FAIL(snapshot_proxy.get_max_snapshot_info(*sql_proxy_, tenant_id_, snapshot_info))) {
   if (OB_ENTRY_NOT_EXIST == ret) {
     // no acquired snapshot
     ret = OB_SUCCESS;
   } else {
     LOG_WARN("fail to get max snapshot info", KR(ret));
   }
  }

  SCN tmp_frozen_scn;
  SCN local_max_frozen_scn;
  uint64_t cur_min_data_version = 0;

  ObSimpleFrozenStatus max_frozen_status;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  if (FAILEDx(freeze_info_proxy.get_max_freeze_info(*sql_proxy_, max_frozen_status))) {
    LOG_WARN("fail to get freeze info with max frozen_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info.get_latest_frozen_scn(local_max_frozen_scn))) {
    LOG_WARN("fail to get latest frozen_scn", KR(ret), K(freeze_info));
  } else if (max_frozen_status.frozen_scn_ != local_max_frozen_scn) {
    // after new leader updates epoch and reloads freeze_info, old leader generates one new
    // frozen_scn and can add it into __all_freeze_info (cuz not checking epoch)
    //
    if (local_max_frozen_scn < max_frozen_status.frozen_scn_) {
      ret = OB_EAGAIN;
      LOG_WARN("max frozen_scn in cache is smaller than max frozen_scn in table, will try again",
               KR(ret), K(local_max_frozen_scn), K(max_frozen_status));
    } else { // local_max_frozen_scn > max_frozen_status.frozen_scn_
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max frozen_scn in cache is larger than max frozen_scn in table", KR(ret),
               K(local_max_frozen_scn), K(max_frozen_status));
    }
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, cur_min_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
  } else if (cur_min_data_version < max_frozen_status.data_version_) {
    // do not allow data_version of freeze_info rollback
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major freeze is not allowed now, please check and upgrade observer", KR(ret),
             K_(tenant_id), K(cur_min_data_version), "last_min_data_version",
             max_frozen_status.data_version_);
  } else if (OB_FAIL(get_gts(tmp_frozen_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if ((tmp_frozen_scn <= snapshot_gc_scn)
             || (tmp_frozen_scn <= local_max_frozen_scn)
             || (tmp_frozen_scn <= snapshot_info.snapshot_scn_)) {
    // current time from gts must be greater than old ts
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid frozen_timestmap", KR(ret), K(snapshot_gc_scn),
              K(tmp_frozen_scn), K(local_max_frozen_scn), K(snapshot_info));
  } else {
    new_frozen_scn = tmp_frozen_scn;
  }

  return ret;
}

int ObFreezeInfoManager::get_freeze_info(
    const SCN &frozen_scn,
    share::ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (SCN::base_scn() == frozen_scn) {
    frozen_status.frozen_scn_ = SCN::base_scn();
  } else if (OB_FAIL(freeze_info_.get_frozen_status(frozen_scn, frozen_status))) {
    LOG_WARN("fail to get frozen status", KR(ret), K(frozen_scn), K_(freeze_info));
  }
  return ret;
}

int ObFreezeInfoManager::get_local_latest_frozen_scn(SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(freeze_info_.get_latest_frozen_scn(frozen_scn))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K_(freeze_info));
  }

  return ret;
}

int ObFreezeInfoManager::renew_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;

  SCN cur_snapshot_gc_scn;
  SCN new_snapshot_gc_scn;
  int64_t affected_rows = 0;
  ObMySQLTransaction trans;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner error", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_,
      cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  }
  // no need to minus max_stale_time_for_weak_consistency since 4.1, because the collection of
  // multi-version data no longer depends on snapshot_gc_scn since 4.1
  else if (OB_FAIL(get_gts(new_snapshot_gc_scn))) {
    LOG_WARN("fail to get_gts", KR(ret));
  } else if ((new_snapshot_gc_scn <= freeze_info_.latest_snapshot_gc_scn_)
             || (cur_snapshot_gc_scn >= new_snapshot_gc_scn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snaptshot gc time", KR(ret), K(cur_snapshot_gc_scn), K(new_snapshot_gc_scn),
      K(freeze_info_.latest_snapshot_gc_scn_));
  } else if (OB_FAIL(ObGlobalStatProxy::update_snapshot_gc_scn(trans, tenant_id_, new_snapshot_gc_scn,
      affected_rows))) {
    LOG_WARN("fail to update snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", KR(ret), K(affected_rows));
  } else if (OB_FAIL(set_local_snapshot_gc_scn(new_snapshot_gc_scn))) {
    LOG_WARN("fail to set latest snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = ((OB_SUCC(ret)) ? tmp_ret : ret);
      LOG_WARN("fail to end trans", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
    }
  }

  if (OB_FAIL(ret)) {
    freeze_info_.set_invalid();
  } else {
    LOG_INFO("succ to renew snapshot_gc_scn", K(new_snapshot_gc_scn), K_(tenant_id));
  }

  return ret;
}

int ObFreezeInfoManager::get_gts(SCN &gts_scn) const
{
  int ret = OB_SUCCESS;
  bool is_external_consistent = true;
  const int64_t timeout_us = 10 * 1000 * 1000;
  if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id_, timeout_us, gts_scn, is_external_consistent))) {
    LOG_WARN("fail to get ts sync", K(ret), K_(tenant_id), K(timeout_us));
  } else if (!is_external_consistent) { // only suppport gts in 4.0
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot freeze without gts", KR(ret), K_(tenant_id));
  }

  return ret;
}

int ObFreezeInfoManager::set_local_snapshot_gc_scn(const SCN &new_scn)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (!freeze_info_.is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    freeze_info_.latest_snapshot_gc_scn_ = new_scn;
  }
  return ret;
}

int ObFreezeInfoManager::try_gc_freeze_info()
{
  ObRecursiveMutexGuard guard(lock_);
  int ret = OB_SUCCESS;

  const int64_t MAX_KEEP_INTERVAL_NS =  30 * 24 * 60 * 60 * 1000L * 1000L * 1000L; // 30 day
  const int64_t MIN_REMAINED_VERSION_COUNT = 32;
  SCN cur_gts_scn;
  SCN min_frozen_scn;
  if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get_gts", KR(ret));
  } else {
    min_frozen_scn = SCN::minus(cur_gts_scn, MAX_KEEP_INTERVAL_NS);
  }

  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  ObMySQLTransaction trans;
  ObArray<ObSimpleFrozenStatus> all_frozen_status;
  SCN cur_snapshot_gc_scn;

  if (FAILEDx(check_inner_stat())) {
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_, cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_proxy.get_all_freeze_info(trans, all_frozen_status))) {
    LOG_WARN("fail to get all freeze info", KR(ret), K_(tenant_id));
  } else {
    const int64_t frozen_status_cnt = all_frozen_status.count();
    if (frozen_status_cnt > MIN_REMAINED_VERSION_COUNT) {
      int64_t reserved_idx = frozen_status_cnt - MIN_REMAINED_VERSION_COUNT - 1;
      const SCN &tmp_frozen_scn = all_frozen_status.at(reserved_idx).frozen_scn_;

      min_frozen_scn = MIN(min_frozen_scn, tmp_frozen_scn);
      if (OB_FAIL(freeze_info_proxy.batch_delete(trans, min_frozen_scn))) {
        LOG_WARN("fail to batch delete freeze info", KR(ret), K(min_frozen_scn));
      } else {
        // reload will later
        freeze_info_.set_invalid();
        LOG_INFO("succ to batch delete freeze info", K_(tenant_id), K(min_frozen_scn));
      }
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = ((OB_SUCC(ret)) ? tmp_ret : ret);
      LOG_WARN("fail to end trans", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
    }
  }
  return ret;
}

int ObFreezeInfoManager::try_update_zone_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->try_update_zone_merge_info(expected_epoch))) {
    LOG_WARN("fail to try update zone_merge_info", KR(ret), K_(tenant_id), K(expected_epoch));
  }
  return ret;
}

int ObFreezeInfoManager::check_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;
  SCN cur_gts_scn;
  int64_t delay = 0;
  int64_t start_service_time = -1;
  int64_t total_service_time = -1;

  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get_gts", KR(ret));
  } else {
    const SCN &snapshot_gc_scn = freeze_info_.latest_snapshot_gc_scn_;
    if (snapshot_gc_scn > cur_gts_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to check snapshot_gc_scn, snapshot_gc_scn is larger than cur_gts_scn",
               KR(ret), K(snapshot_gc_scn), K(cur_gts_scn), K_(tenant_id));
    } else {
      const int64_t delay = (!snapshot_gc_scn.is_valid_and_not_min()) ? 0 :
          (ObTimeUtility::current_time() - snapshot_gc_scn.convert_to_ts());

      if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        if (delay > SNAPSHOT_GC_TS_ERROR) {
          // In order to avoid LOG_ERROR when the tenant reloads old snapshot_gc_scn due to the
          // cluster restarted. LOG_ERROR should satisfy two additional conditions:
          // 1. start_service_time > 0. start_service_time is initialized to 0 when observer starts.
          // Then it will be updated to the time when observer starts through heartbeat, which is
          // scheduled every 2 seconds.
          // 2. total_service_time > SNAPSHOT_GC_TS_ERROR.
          ObServerTableOperator st_operator;
          if (OB_FAIL(st_operator.init(sql_proxy_))) {
            LOG_WARN("fail to init server table operator", K(ret), K_(tenant_id));
          } else if (OB_FAIL(st_operator.get_start_service_time(GCONF.self_addr_, start_service_time))) {
            LOG_WARN("fail to get start service time", KR(ret), K_(tenant_id));
          } else if (FALSE_IT(total_service_time = ObTimeUtility::current_time() - start_service_time)) {
          } else if ((start_service_time > 0) && (total_service_time > SNAPSHOT_GC_TS_ERROR)) {
            LOG_ERROR("rs_monitor_check : snapshot_gc_ts delay for a long time",
                      K(snapshot_gc_scn), K(delay), K_(tenant_id), K(start_service_time),
                      K(total_service_time));
          }
        } else if (delay > SNAPSHOT_GC_TS_WARN) {
          LOG_WARN("rs_monitor_check : snapshot_gc_ts delay for a long time",
                  K(snapshot_gc_scn), K(delay), K_(tenant_id));
        }
      }
    }
  }

  return ret;
}

int ObFreezeInfoManager::get_min_freeze_info_to_broadcast(
    ObSimpleFrozenStatus &frozen_status) const
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  SCN global_last_merged_scn;
  if (OB_FAIL(get_global_last_merged_scn(global_last_merged_scn))) {
    LOG_WARN("fail to get global_last_merged_version", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_.get_min_freeze_info_greater_than(
             global_last_merged_scn, frozen_status))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(global_last_merged_scn));
  }

  return ret;
}

int ObFreezeInfoManager::get_min_freeze_info(ObSimpleFrozenStatus &frozen_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(get_min_freeze_info_to_broadcast(frozen_status))) {
    LOG_WARN("fail to get freeze info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoManager::check_need_broadcast(bool &need_broadcast)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObSimpleFrozenStatus frozen_status;
  if (OB_FAIL(get_min_freeze_info(frozen_status))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (frozen_status.is_valid()) {
    if (OB_FAIL(merge_info_mgr_->check_need_broadcast(frozen_status.frozen_scn_, need_broadcast))) {
      LOG_WARN("fail to check need broadcast", KR(ret), K_(tenant_id), K(frozen_status));
    }
  }
  return ret;
}

int ObFreezeInfoManager::broadcast_freeze_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObSimpleFrozenStatus frozen_status;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_min_freeze_info(frozen_status))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (frozen_status.is_valid()) {
    if (OB_FAIL(merge_info_mgr_->set_global_freeze_info(frozen_status.frozen_scn_, expected_epoch))) {
      LOG_WARN("fail to set global freeze info", KR(ret), K_(tenant_id), K(frozen_status), K(expected_epoch));
    }
  }
  return ret;
}

} // rootserver
} // oceanbase
