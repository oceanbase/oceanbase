/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_mview_update_cache_task.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"

namespace oceanbase
{
namespace rootserver
{

ObMviewUpdateCacheTask::ObMviewUpdateCacheTask()
    : is_inited_(false),
      is_stop_(false),
      in_sched_(false)
{
}
ObMviewUpdateCacheTask::~ObMviewUpdateCacheTask()
{
  clean_up();
}

int ObMviewUpdateCacheTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMviewUpdateCacheTask init twice", KR(ret), KPC(this));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMviewUpdateCacheTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMviewUpdateCacheTask not init", KR(ret), KPC(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(TaskDelay, true /*repeat*/))) {
      LOG_WARN("fail to schedule update mview cache task", KR(ret));
    } else {
      in_sched_ = true;
      LOG_INFO("ObMviewUpdateCacheTask started", KR(ret), KPC(this));
    }
  }
  return ret;
}

void ObMviewUpdateCacheTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMviewUpdateCacheTask::wait() { wait_task(); }
void ObMviewUpdateCacheTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  clean_up();
}

void ObMviewUpdateCacheTask::clean_up()
{
  is_inited_ = false;
  is_stop_ = false;
  in_sched_ = false;
}

int ObMviewUpdateCacheTask::get_mview_refresh_scn_sql(const int refresh_mode,
                                                      ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT CAST(MVIEW_ID AS UNSIGNED) AS MVIEW_ID, \
                              LAST_REFRESH_SCN, \
                              CAST(REFRESH_MODE AS UNSIGNED) AS REFRESH_MODE \
                              FROM `%s`.`%s` \
                              WHERE TENANT_ID = 0 and REFRESH_MODE = %d",
                            OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME, refresh_mode))) {
    LOG_WARN("fail to get sql", KR(ret), K(sql));
  }
  return ret;
}

void ObMviewUpdateCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  share::SCN read_snapshot(share::SCN::min_scn());
  share::SCN mview_refresh_snapshot;
  const uint64_t tenant_id = MTL_ID();
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  rootserver::ObMViewMaintenanceService *mview_maintenance_service =
                                      MTL(rootserver::ObMViewMaintenanceService*);
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(mview_maintenance_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null or ObMViewMaintenanceService is null",
             KR(ret), KP(sql_proxy), KP(mview_maintenance_service));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else {
    const int64_t refresh_mode = (int64_t)ObMVRefreshMode::MAJOR_COMPACTION;
    ObSEArray<uint64_t, 2> mview_ids;
    ObSEArray<uint64_t, 2> mview_refresh_scns;
    ObSEArray<uint64_t, 2> mview_refresh_modes;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(get_mview_refresh_scn_sql(refresh_mode, sql))) {
        LOG_WARN("failed to get last refresh scn sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res,
                                         tenant_id,
                                         sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(extract_sql_result(res.get_result(),
                                            mview_ids,
                                            mview_refresh_scns,
                                            mview_refresh_modes))) {
        LOG_WARN("fail to extract sql result", K(ret), K(sql), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(mview_maintenance_service->update_mview_refresh_info_cache(mview_ids,
                                                                                  mview_refresh_scns,
                                                                                  mview_refresh_modes))){
      LOG_WARN("fail to update mview refresh info cache", K(ret),
               K(mview_ids), K(mview_refresh_scns), K(mview_refresh_modes), K(tenant_id));
    }
  }
}

int ObMviewUpdateCacheTask::extract_sql_result(sqlclient::ObMySQLResult *mysql_result,
                                               ObIArray<uint64_t> &mview_ids,
                                               ObIArray<uint64_t> &last_refresh_scns,
                                               ObIArray<uint64_t> &mview_refresh_modes)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mysql_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result is null", K(ret), KP(mysql_result));
  } else {
    ObSEArray<uint64_t, 2> res_ids;
    ObSEArray<uint64_t, 2> res_scns;
    ObSEArray<uint64_t, 2> refresh_modes;
    const int64_t col_idx0 = 0;
    const int64_t col_idx1 = 1;
    const int64_t col_idx2 = 2;
    while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
      uint64_t mview_id = OB_INVALID_ID;
      uint64_t last_refresh_scn = OB_INVALID_SCN_VAL;
      uint64_t refresh_mode = (uint64_t)ObMVRefreshMode::MAX;
      if (OB_FAIL(mysql_result->get_uint(col_idx0, mview_id))
          || OB_FAIL(mysql_result->get_uint(col_idx1, last_refresh_scn))
          || OB_FAIL(mysql_result->get_uint(col_idx2, refresh_mode))) {
        LOG_WARN("fail to get int/uint value", K(ret));
      } else if (OB_FAIL(res_ids.push_back(mview_id))
                  || OB_FAIL(res_scns.push_back(last_refresh_scn))
                  || OB_FAIL(refresh_modes.push_back(refresh_mode))) {
        LOG_WARN("fail to push back array", K(ret));
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret || OB_ITER_END == ret)) {
      if((OB_FAIL(mview_ids.assign(res_ids)) ||
          OB_FAIL(last_refresh_scns.assign(res_scns)) ||
          OB_FAIL(mview_refresh_modes.assign(refresh_modes)))) {
        LOG_WARN("fail to assign array", K(ret));
      }
    }
  }
  return ret;
}

}
}
