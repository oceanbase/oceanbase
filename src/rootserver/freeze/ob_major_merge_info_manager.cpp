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

#include "rootserver/freeze/ob_major_merge_info_manager.h"

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

/****************************** ObMajorMergeInfoManager ******************************/
int ObMajorMergeInfoManager::init(
    uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init zone_merge_mgr", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init freeze_info_mgr", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

int ObMajorMergeInfoManager::try_reload()
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (freeze_info_mgr_.is_valid()) {
    // do nothing
  } else if (OB_FAIL(reload())) {
    LOG_WARN("fail to reload", KR(ret));
  }
  return ret;
}

int ObMajorMergeInfoManager::reload(const bool reload_zone_merge_info)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  SCN global_broadcast_scn;
  if (reload_zone_merge_info && OB_FAIL(zone_merge_mgr_.reload())) {
    LOG_WARN("fail to reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (!reload_zone_merge_info && OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get broadcast version", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.reload(global_broadcast_scn))) {
    LOG_WARN("fail to update freeze info", KR(ret), K_(tenant_id), K(global_broadcast_scn));
  } else {
    LOG_INFO("succ to reload merge info manager", K_(tenant_id));
  }
  return ret;
}

// add freeze info to inner_table
int ObMajorMergeInfoManager::set_freeze_info()
{
  int ret = OB_SUCCESS;
  SCN new_frozen_scn;
  ObRecursiveMutexGuard guard(lock_);

  const int64_t fake_schema_version = 1000;
  SCN remote_snapshot_gc_scn;
  ObFreezeInfo freeze_info;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
    // freeze get_schema_version need interactive with ddl trans but don't use gen_new_schema_version so no need check_in_rs
    // freeze disable check_newest_schema so not to use schema_guard
    ObDDLSQLTransaction trans(GCTX.schema_service_, false/*need_end_signal*/, false/*stash*/, false/*parallel*/, false/*check_in_rs*/, false/*check_newest_schema*/);

    // In 'ddl_sql_transaction.start()', it implements the semantics of 'lock_all_ddl_operation'.
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_, fake_schema_version))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(fake_schema_version));
    // 1. lock snapshot_gc_ts in __all_global_stat
    } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(
              trans, tenant_id_, remote_snapshot_gc_scn))) {
      LOG_WARN("fail to select for update", KR(ret));
    } else {
      int64_t schema_version_in_frozen_ts = 0;
      uint64_t data_version = 0;

      // 2. generate new frozen_scn
      if (OB_FAIL(generate_frozen_scn(remote_snapshot_gc_scn, new_frozen_scn))) {
        LOG_WARN("fail to generate frozen timestamp", KR(ret));
      // 3. get schema_version at frozen_scn
      } else if (OB_FAIL(get_schema_version(new_frozen_scn, schema_version_in_frozen_ts))) {
        LOG_WARN("fail to get schema version", KR(ret), K(new_frozen_scn));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
      } else {
        freeze_info.frozen_scn_ = new_frozen_scn;
        freeze_info.schema_version_ = schema_version_in_frozen_ts;
        freeze_info.data_version_ = data_version;

        // 4. insert freeze info
        if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, freeze_info))) {
          LOG_WARN("fail to set freeze info", KR(ret), K(freeze_info), K_(tenant_id));
        }
      }
    }

    ret = trans.handle_trans_in_the_end(ret);
  }

  if (FAILEDx(freeze_info_mgr_.add_freeze_info(freeze_info))) {
    LOG_WARN("fail to push back", KR(ret), K(freeze_info));
  }

  if (OB_FAIL(ret)) {
    freeze_info_mgr_.reset_freeze_info(); // reload freeze info on the next fetch
  }

  LOG_INFO("finish set freeze info", KR(ret), K(freeze_info), K_(tenant_id));
  ROOTSERVICE_EVENT_ADD("root_service", "root_major_freeze", K_(tenant_id),
                        K(ret), "new_frozen_scn", new_frozen_scn.get_val_for_inner_table_field());
  return ret;
}

// lock guarded by caller
int ObMajorMergeInfoManager::generate_frozen_scn(
    const SCN &snapshot_gc_scn,
    SCN &new_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  ObSnapshotInfo snapshot_info;

  // build index or backup will acquire snapshot,
  // so should make sure frozen_scn will be greater max snapshot_ts.
  if (OB_FAIL(snapshot_proxy.get_max_snapshot_info(*GCTX.sql_proxy_, tenant_id_, snapshot_info))) {
   if (OB_ENTRY_NOT_EXIST == ret) {
     // no acquired snapshot
     ret = OB_SUCCESS;
   } else {
     LOG_WARN("fail to get max snapshot info", KR(ret));
   }
  }

  SCN tmp_frozen_scn;
  share::ObFreezeInfo latest_frozen_status;
  SCN local_max_frozen_scn;
  uint64_t cur_min_data_version = 0;

  ObFreezeInfo max_frozen_status;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  if (FAILEDx(freeze_info_proxy.get_max_freeze_info(*GCTX.sql_proxy_, max_frozen_status))) {
    LOG_WARN("fail to get freeze info with max frozen_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.get_latest_freeze_info(latest_frozen_status))) {
    LOG_WARN("fail to get latest frozen_scn", KR(ret), K(latest_frozen_status));
  } else if (FALSE_IT(local_max_frozen_scn = latest_frozen_status.frozen_scn_)) {
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

int ObMajorMergeInfoManager::get_schema_version(
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

    if (OB_FAIL(server_schema_service->fetch_schema_version(status, *GCTX.sql_proxy_, schema_version))) {
      LOG_WARN("fail to fetch schema version", KR(ret), K(status));
    }
  }

  return ret;
}

int ObMajorMergeInfoManager::get_freeze_info(
    const SCN &frozen_scn,
    share::ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  if (SCN::base_scn() == frozen_scn) {
    freeze_info.frozen_scn_ = SCN::base_scn();
  } else {
    ObRecursiveMutexGuard guard(lock_);
    if (OB_FAIL(try_reload())) {
      LOG_WARN("fail to check inner stat", KR(ret));
    } else if (OB_FAIL(freeze_info_mgr_.get_freeze_info(frozen_scn, freeze_info))) {
      LOG_WARN("fail to get frozen status", KR(ret), K(frozen_scn), K(freeze_info));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::get_local_latest_frozen_scn(SCN &frozen_scn)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  share::ObFreezeInfo latest_freeze_info;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.get_latest_freeze_info(latest_freeze_info))) {
    LOG_WARN("inner stat error", KR(ret), K(latest_freeze_info));
  } else {
    frozen_scn = latest_freeze_info.frozen_scn_;
  }
  return ret;
}

int ObMajorMergeInfoManager::renew_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;

  SCN cur_snapshot_gc_scn;
  SCN latest_snapshot_gc_scn;
  SCN new_snapshot_gc_scn;
  int64_t affected_rows = 0;
  ObMySQLTransaction trans;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(try_reload())) {
    LOG_WARN("inner error", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_,
      cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  }
  // no need to minus max_stale_time_for_weak_consistency since 4.1, because the collection of
  // multi-version data no longer depends on snapshot_gc_scn since 4.1
  else if (OB_FAIL(get_gts(new_snapshot_gc_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if (FALSE_IT(latest_snapshot_gc_scn = freeze_info_mgr_.get_snapshot_gc_scn())) {
  } else if ((new_snapshot_gc_scn <= latest_snapshot_gc_scn)
             || (cur_snapshot_gc_scn >= new_snapshot_gc_scn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snaptshot gc time", KR(ret), K(cur_snapshot_gc_scn), K(new_snapshot_gc_scn),
      K(latest_snapshot_gc_scn));
  } else if (OB_FAIL(ObGlobalStatProxy::update_snapshot_gc_scn(trans, tenant_id_, new_snapshot_gc_scn,
      affected_rows))) {
    LOG_WARN("fail to update snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", KR(ret), K(affected_rows));
  } else if (OB_FAIL(freeze_info_mgr_.update_snapshot_gc_scn(new_snapshot_gc_scn))) {
    LOG_WARN("fail to update snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  }

  ret = trans.handle_trans_in_the_end(ret);

  if (OB_FAIL(ret)) {
    freeze_info_mgr_.reset_freeze_info();
  }
  LOG_INFO("renew snapshot_gc_scn", K(ret), K(new_snapshot_gc_scn), K_(tenant_id));

  return ret;
}

int ObMajorMergeInfoManager::try_gc_freeze_info()
{
  ObRecursiveMutexGuard guard(lock_);
  int ret = OB_SUCCESS;

  const int64_t MAX_KEEP_INTERVAL_NS =  30 * 24 * 60 * 60 * 1000L * 1000L * 1000L; // 30 day
  const int64_t MIN_REMAINED_VERSION_COUNT = 32;
  SCN cur_gts_scn;
  SCN min_frozen_scn;
  if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else {
    min_frozen_scn = SCN::minus(cur_gts_scn, MAX_KEEP_INTERVAL_NS);
  }

  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  ObMySQLTransaction trans;
  ObArray<ObFreezeInfo> all_freeze_info;
  SCN cur_snapshot_gc_scn;

  if (FAILEDx(try_reload())) {
    LOG_WARN("fail to try reload", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_, cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_proxy.get_all_freeze_info(trans, all_freeze_info))) {
    LOG_WARN("fail to get all freeze info", KR(ret), K_(tenant_id));
  } else {
    const int64_t freeze_info_cnt = all_freeze_info.count();
    if (freeze_info_cnt > MIN_REMAINED_VERSION_COUNT) {
      int64_t reserved_idx = freeze_info_cnt - MIN_REMAINED_VERSION_COUNT - 1;
      const SCN &tmp_frozen_scn = all_freeze_info.at(reserved_idx).frozen_scn_;

      min_frozen_scn = MIN(min_frozen_scn, tmp_frozen_scn);
      if (OB_FAIL(freeze_info_proxy.batch_delete(trans, min_frozen_scn))) {
        LOG_WARN("fail to batch delete freeze info", KR(ret), K(min_frozen_scn));
      } else {
        // reload will later
        freeze_info_mgr_.reset_freeze_info();
        LOG_INFO("succ to batch delete freeze info", K_(tenant_id), K(min_frozen_scn));
      }
    }
  }

  ret = trans.handle_trans_in_the_end(ret);
  return ret;
}

int ObMajorMergeInfoManager::try_update_zone_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.try_update_zone_merge_info(expected_epoch))) {
    LOG_WARN("fail to try update zone_merge_info", KR(ret), K_(tenant_id), K(expected_epoch));
  }
  return ret;
}

int ObMajorMergeInfoManager::check_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;
  SCN cur_gts_scn;
  int64_t delay = 0;
  int64_t start_service_time = -1;
  int64_t total_service_time = -1;
  SCN snapshot_gc_scn;

  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if (FALSE_IT(snapshot_gc_scn = freeze_info_mgr_.get_snapshot_gc_scn())) {
  } else {
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
          if (OB_FAIL(st_operator.init(GCTX.sql_proxy_))) {
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

int ObMajorMergeInfoManager::inner_get_min_freeze_info(ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  SCN global_last_merged_scn;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_global_last_merged_scn(global_last_merged_scn))) {
    LOG_WARN("fail to get global_last_merged_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.get_min_freeze_info_greater_than(
             global_last_merged_scn, freeze_info))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(global_last_merged_scn));
  }
  return ret;
}

int ObMajorMergeInfoManager::check_need_broadcast(bool &need_broadcast)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObFreezeInfo freeze_info;
  if (OB_FAIL(inner_get_min_freeze_info(freeze_info))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (freeze_info.is_valid()) {
    if (OB_FAIL(zone_merge_mgr_.check_need_broadcast(freeze_info.frozen_scn_, need_broadcast))) {
      LOG_WARN("fail to check need broadcast", KR(ret), K_(tenant_id), K(freeze_info));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::broadcast_freeze_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObFreezeInfo freeze_info;
  if (OB_FAIL(inner_get_min_freeze_info(freeze_info))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (freeze_info.is_valid()) {
    if (OB_FAIL(zone_merge_mgr_.set_global_freeze_info(freeze_info.frozen_scn_, expected_epoch))) {
      LOG_WARN("fail to set global freeze info", KR(ret), K_(tenant_id), K(freeze_info), K(expected_epoch));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::adjust_global_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge info mgr not inited", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.adjust_global_merge_info(expected_epoch))) {
    LOG_WARN("fail to adjust global merge info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObMajorMergeInfoManager::get_gts(SCN &gts_scn) const
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


} // rootserver
} // oceanbase
