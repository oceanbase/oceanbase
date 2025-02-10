/**
 * Copyright (c) 2023 OceanBase
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

#include "observer/ob_server_struct.h"
#include "rootserver/mview/ob_mview_push_snapshot_task.h"
#include "share/schema/ob_mview_info.h"
#include "share/ob_global_stat_proxy.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "share/backup/ob_backup_data_table_operator.h"

namespace oceanbase {
namespace rootserver {

ObMViewPushSnapshotTask::ObMViewPushSnapshotTask()
  : is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObMViewPushSnapshotTask::~ObMViewPushSnapshotTask() {}

int ObMViewPushSnapshotTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewPushSnapshotTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObMViewPushSnapshotTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewPushSnapshotTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_PUSH_SNAPSHOT_INTERVAL, true /*repeat*/))) {
      LOG_WARN("fail to schedule ObMViewPushSnapshotTask", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewPushSnapshotTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewPushSnapshotTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObMViewPushSnapshotTask::wait() { wait_task(); }

void ObMViewPushSnapshotTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);
  bool need_schedule = false;
  ObMySQLTransaction trans;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewPushSnapshotTask not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop_)) {

  } else if (OB_FAIL(need_schedule_major_refresh_mv_task(tenant_id_, need_schedule))) {
    LOG_WARN("fail to check need schedule major refresh mv task", KR(ret), K(tenant_id_));
  } else if (!need_schedule) {

  } else if (OB_UNLIKELY(OB_ISNULL(sql_proxy) || OB_ISNULL(mgr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy or mgr is null", KR(ret), K(sql_proxy), K(mgr));
  } else if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id_));
  } else {
    share::ObGlobalStatProxy stat_proxy(trans, tenant_id_);
    share::SCN major_refresh_mv_merge_scn;
    const int64_t snapshot_for_tx = mgr->get_min_reserved_snapshot_for_tx();
    share::SCN min_refresh_scn;
    share::ObSnapshotTableProxy snapshot_proxy;
    const bool select_for_update = true;
    uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    bool space_danger = false;
    ObArray<share::ObBackupJobAttr> backup_jobs;
    // we query the major_refresh_mv_merge_scn in __all_core_table to conflict with the backup
    // process.
    if (OB_FAIL(stat_proxy.get_major_refresh_mv_merge_scn(select_for_update,
                                                          major_refresh_mv_merge_scn))) {
      LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(!major_refresh_mv_merge_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major_refresh_mv_merge_scn is invalid", KR(ret), K(tenant_id_),
                K(major_refresh_mv_merge_scn));
    } // to ensure the concurrent query won't return the OB_SNAPSHOT_DISCARDED error,
      // we use snapshot_for_tx to get the min refresh scn in __all_mview.
    else if (OB_FAIL(ObMViewInfo::get_min_major_refresh_mview_scn(
                 trans, tenant_id_, snapshot_for_tx, min_refresh_scn))) {
      // the tenant has no major refresh mview
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get min major_refresh_mview_scn", KR(ret), K(tenant_id_),
                  K(snapshot_for_tx));
      }
    } else if (OB_FAIL(share::ObBackupJobOperator::get_jobs(
                   *sql_proxy, meta_tenant_id, false /*select for update*/, backup_jobs))) {
      LOG_WARN("failed to get backup jobs", K(ret), K(tenant_id_));
    } else if (!backup_jobs.empty() && OB_FAIL(check_space_occupy_(space_danger))) {
      LOG_WARN("backup jobs exist, check space occupy failed", KR(ret), K(tenant_id_));
    } else if (!backup_jobs.empty() && !space_danger) {
      LOG_INFO("backup jobs exist, space is not in danger just skip push snapshot", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(snapshot_proxy.push_snapshot_for_major_refresh_mv(trans, tenant_id_,
                                                                         min_refresh_scn))) {
      LOG_WARN("fail to push snapshot for major refresh mv", KR(ret), K(tenant_id_),
                K(min_refresh_scn));
    } else {
      LOG_INFO("[MAJ_REF_MV] successfully push major refresh mview snapshot", K(tenant_id_),
                K(snapshot_for_tx), K(min_refresh_scn));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
}

int ObMViewPushSnapshotTask::check_space_occupy_(bool &space_danger)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  space_danger = false;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign("select CAST(max(DATA_DISK_IN_USE/DATA_DISK_ALLOCATED)*100 as SIGNED) occupy from oceanbase.gv$ob_servers"))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret));
    } else {
      int64_t occupy = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "occupy", occupy, int64_t);
      int64_t upper_bound = GCONF._datafile_usage_upper_bound_percentage;
      LOG_INFO("check_space_occupy", KR(ret), K(occupy), K(upper_bound), K(sql));
      if (OB_SUCC(ret)) {
        if (occupy >= GCONF._datafile_usage_upper_bound_percentage) {
          space_danger = true;
          LOG_ERROR("space in danger", K(occupy), K(upper_bound));
        }
      }
    }
  }

  return ret;
}


} // namespace rootserver
} // namespace oceanbase
