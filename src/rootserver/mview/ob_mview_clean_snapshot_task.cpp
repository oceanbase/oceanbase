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
#include "rootserver/mview/ob_mview_clean_snapshot_task.h"
#include "share/schema/ob_mview_info.h"
#include "share/ob_global_stat_proxy.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"

namespace oceanbase {
namespace rootserver {

ObMViewCleanSnapshotTask::ObMViewCleanSnapshotTask()
  : is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObMViewCleanSnapshotTask::~ObMViewCleanSnapshotTask() {}

int ObMViewCleanSnapshotTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewCleanSnapshotTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObMViewCleanSnapshotTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewCleanSnapshotTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_CLEAN_SNAPSHOT_INTERVAL, true /*repeat*/))) {
      LOG_WARN("fail to schedule ObMViewCleanSnapshotTask", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewCleanSnapshotTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewCleanSnapshotTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObMViewCleanSnapshotTask::wait() { wait_task(); }

// This task is used to clean records in __all_acquired_snapshot for the following conditions:
// 1. when a mv is dropped, the snapshot record of its container table should be removed
// 2. when failure occurs in the creation process of a mv, the snapshot record may have been
// created, but the mv is not created successfully, so the snapshot record should be removed
// 3. when multiple mv referring to the same base table, there will be multiple snapshot record for
// the base table, but only the record with smallest scn is necessary, others can be removed
// 4. when all mv of a base table are dropped, the snapshot record of the base table should be
// removed
void ObMViewCleanSnapshotTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);
  bool need_schedule = false;
  ObMySQLTransaction trans;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewCleanSnapshotTask not init", KR(ret), KP(this));
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
    share::ObSnapshotTableProxy snapshot_proxy;
    const bool select_for_update = true;
    ObSEArray<share::ObSnapshotInfo, 4> snapshots;
    bool mview_in_creation = false;
    if (OB_FAIL(stat_proxy.get_major_refresh_mv_merge_scn(select_for_update,
                                                          major_refresh_mv_merge_scn))) {
      LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(!major_refresh_mv_merge_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major_refresh_mv_merge_scn is invalid", KR(ret), K(tenant_id_),
               K(major_refresh_mv_merge_scn));
    } else if (OB_FAIL(snapshot_proxy.get_all_snapshots(
                   trans, tenant_id_, share::SNAPSHOT_FOR_MAJOR_REFRESH_MV, snapshots))) {
      LOG_WARN("fail to get snapshots", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObMViewInfo::contains_major_refresh_mview_in_creation(
                   trans, tenant_id_, mview_in_creation))) {
      LOG_WARN("fail to check if mv is in creation", KR(ret), K(tenant_id_));
    } else if (mview_in_creation) {
      // when a mview is being created, its snapshot may be added but the dependency is not
      // added yet, which can cause cleaning the snapshot by mistake. so we just skip this round.
      LOG_INFO("mview is being created, skip clean task", KR(ret), K(tenant_id_));
    } else {
      uint64_t last_tablet_id = 0;
      // considering the task is scheduled infrequently, performance should not be a concern here,
      // so we use a simple loop to remove the snapshots for now.
      for (int64_t i = 0; OB_SUCC(ret) && i < snapshots.count(); ++i) {
        const share::ObSnapshotInfo &snapshot = snapshots.at(i);
        uint64_t table_id = 0;
        ObSEArray<uint64_t, 4> relevent_mv_tables;
        bool is_container_table = false;
        bool need_remove_snapshot = false;
        if (OB_UNLIKELY(!snapshot.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snapshot is invalid", KR(ret), K(snapshot));
        } else if (snapshot.tablet_id_ == last_tablet_id) {
          // the snapshots list is sorted by tablet_id, snapshot_scn,
          // so if the tablet_id is the same as the last one, we can safely remove this snapshot.
          need_remove_snapshot = true;
          LOG_INFO("redundant snapshot, remove snapshot", KR(ret), K(tenant_id_), K(snapshot));
        } else if (FALSE_IT(last_tablet_id = snapshot.tablet_id_)) {
        } else if (OB_FAIL(get_table_id_(trans, snapshot.tablet_id_, table_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            need_remove_snapshot = true;
            LOG_INFO("table is dropped, remove snapshot", KR(ret), K(tenant_id_), K(snapshot));
          } else {
            LOG_WARN("fail to get table id", KR(ret), K(snapshot));
          }
        } else if (OB_FAIL(is_mv_container_table_(table_id, is_container_table))) {
          LOG_WARN("fail to check is mv container table", KR(ret), K(tenant_id_), K(table_id));
        } else if (is_container_table) {
          // do nothing
        } else if (OB_FAIL(sql::ObMVDepUtils::get_referring_mv_of_base_table(
                       trans, tenant_id_, table_id, relevent_mv_tables))) {
          LOG_WARN("fail to get referring mv of base table", KR(ret), K(tenant_id_), K(table_id));
        } else if (relevent_mv_tables.empty()) {
          need_remove_snapshot = true;
          LOG_INFO("no relevant mv, remove snapshot", KR(ret), K(tenant_id_), K(snapshot));
        }
        if (OB_FAIL(ret) || !need_remove_snapshot) {
          // do nothing
        } else if (OB_FAIL(snapshot_proxy.remove_snapshot(trans, tenant_id_, snapshot))) {
          LOG_WARN("fail to remove snapshot", KR(ret), K(tenant_id_), K(snapshot));
        } else {
          LOG_INFO("[MAJ_REF_MV] successfully remove snapshot", KR(ret), K(tenant_id_), K(snapshot));
        }
      }
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

int ObMViewCleanSnapshotTask::get_table_id_(ObISQLClient &sql_client, const uint64_t tablet_id,
                                           uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT table_id FROM %s WHERE tablet_id = %ld",
                               share::OB_ALL_TABLET_TO_LS_TNAME, tablet_id))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
    }
  }

  return ret;
}

int ObMViewCleanSnapshotTask::is_mv_container_table_(const uint64_t table_id, bool &is_container)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  is_container = false;

  if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tenant_id_), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("table schema is null", KR(ret), K(tenant_id_), K(table_id));
  } else {
    is_container = table_schema->mv_major_refresh();
  }

  return ret;
}


} // namespace rootserver
} // namespace oceanbase
