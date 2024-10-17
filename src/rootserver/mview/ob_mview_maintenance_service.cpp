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

#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_struct.h" // ObMvRefreshMode

namespace oceanbase
{
namespace rootserver
{
using namespace common;
/**
 * ObMViewMaintenanceService
 */

ObMViewMaintenanceService::ObMViewMaintenanceService() : is_inited_(false) {}

ObMViewMaintenanceService::~ObMViewMaintenanceService() {}

int ObMViewMaintenanceService::mtl_init(ObMViewMaintenanceService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(service));
  } else if (OB_FAIL(service->init())) {
    LOG_WARN("fail to init mview maintenance service", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t bucket_num = 64;
  ObMemAttr attr(tenant_id, "MvRefreshInfo");
  ObMemAttr cache_attr(tenant_id, "MvCacheTask");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewMaintenanceService init twice", KR(ret), KP(this));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(mlog_maintenance_task_.init())) {
      LOG_WARN("fail to init mlog maintenance task", KR(ret));
    } else if (OB_FAIL(mview_maintenance_task_.init())) {
      LOG_WARN("fail to init mview maintenance task", KR(ret));
    } else if (OB_FAIL(mvref_stats_maintenance_task_.init())) {
      LOG_WARN("fail to init mvref stats maintenance task", KR(ret));
    } else if (OB_FAIL(mview_push_refresh_scn_task_.init())) {
      LOG_WARN("fail to init mview push refresh scn task", KR(ret));
    } else if (OB_FAIL(mview_push_snapshot_task_.init())) {
      LOG_WARN("fail to init mview push snapshot task", KR(ret));
    } else if (OB_FAIL(replica_safe_check_task_.init())) {
      LOG_WARN("fail to init mvref stats maintenance task", KR(ret));
    } else if (OB_FAIL(collect_mv_merge_info_task_.init())) {
      LOG_WARN("collect mv merge info task init failed", KR(ret));
    } else if (OB_FAIL(mview_clean_snapshot_task_.init())) {
      LOG_WARN("fail to init mview clean snapshot task", KR(ret));
    } else if (OB_FAIL(mview_update_cache_task_.init())) {
      LOG_WARN("fail to init mview update cache task", KR(ret));
    } else if (OB_FAIL(mview_refresh_info_map_.create(bucket_num, attr))) {
      LOG_WARN("fail to create mview refresh info map", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMViewMaintenanceService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    // do nothing
  }
  return ret;
}

void ObMViewMaintenanceService::stop()
{
  mlog_maintenance_task_.stop();
  mview_maintenance_task_.stop();
  mvref_stats_maintenance_task_.stop();
  mview_push_refresh_scn_task_.stop();
  mview_push_snapshot_task_.stop();
  replica_safe_check_task_.stop();
  collect_mv_merge_info_task_.stop();
  mview_clean_snapshot_task_.stop();
}

void ObMViewMaintenanceService::wait()
{
  mlog_maintenance_task_.wait();
  mview_maintenance_task_.wait();
  mvref_stats_maintenance_task_.wait();
  mview_push_refresh_scn_task_.wait();
  mview_push_snapshot_task_.wait();
  replica_safe_check_task_.wait();
  collect_mv_merge_info_task_.wait();
  mview_clean_snapshot_task_.wait();
  mview_update_cache_task_.wait();
}

void ObMViewMaintenanceService::destroy()
{
  is_inited_ = false;
  mlog_maintenance_task_.destroy();
  mview_maintenance_task_.destroy();
  mvref_stats_maintenance_task_.destroy();
  mview_push_refresh_scn_task_.destroy();
  mview_push_snapshot_task_.destroy();
  replica_safe_check_task_.destroy();
  collect_mv_merge_info_task_.destroy();
  mview_clean_snapshot_task_.destroy();
  mview_update_cache_task_.destroy();
  mview_refresh_info_map_.destroy();
}

int ObMViewMaintenanceService::inner_switch_to_leader()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t start_time_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(mlog_maintenance_task_.start())) {
      LOG_WARN("fail to start mlog maintenance task", KR(ret));
    } else if (OB_FAIL(mview_maintenance_task_.start())) {
      LOG_WARN("fail to start mview maintenance task", KR(ret));
    } else if (OB_FAIL(mvref_stats_maintenance_task_.start())) {
      LOG_WARN("fail to start mvref stats maintenance task", KR(ret));
    } else if (OB_FAIL(mview_push_refresh_scn_task_.start())) {
      LOG_WARN("fail to start mview push refresh scn task", KR(ret));
    } else if (OB_FAIL(mview_push_snapshot_task_.start())) {
      LOG_WARN("fail to start mview push snapshot task", KR(ret));
    } else if (OB_FAIL(replica_safe_check_task_.start())) {
      LOG_WARN("fail to start mvref stats maintenance task", KR(ret));
    } else if (OB_FAIL(collect_mv_merge_info_task_.start())) {
      LOG_WARN("collect mv merge info task start failed", KR(ret));
    } else if (OB_FAIL(mview_clean_snapshot_task_.start())) {
      LOG_WARN("fail to start mview clean snapshot task", KR(ret));
    } else if (OB_FAIL(mview_update_cache_task_.start())) {
      LOG_WARN("fail to start mview update cache task", KR(ret));
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("mview_maintenance: switch_to_leader", KR(ret), K(tenant_id), K(cost_us));
  return ret;
}

int ObMViewMaintenanceService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t start_time_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    // start update cache task in follower
    if (OB_FAIL(mview_update_cache_task_.start())) {
      LOG_WARN("fail to start mview update cache task", KR(ret));
    }
    stop();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("mview_maintenance: switch_to_follower", KR(ret), K(tenant_id), K(cost_us));
  return ret;
}

void ObMViewMaintenanceService::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
}

int ObMViewMaintenanceService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::extract_sql_result(sqlclient::ObMySQLResult *mysql_result,
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

int ObMViewMaintenanceService::update_mview_refresh_info_cache(
        const ObIArray<uint64_t> &mview_ids,
        const ObIArray<uint64_t> &mview_refresh_scns,
        const ObIArray<uint64_t> &mview_refresh_modes,
        ObMviewRefreshInfoMap &mview_refresh_info_map) {
  int ret = OB_SUCCESS;
  int update_cache_cnt = 0;
  const int invalid_refresh_scn = 0;
  ARRAY_FOREACH_X(mview_ids, idx, cnt, OB_SUCC(ret)) {
    RefreshInfo new_refresh_info;
    if (mview_refresh_scns.at(idx) == invalid_refresh_scn) {
      // skip update invalid scn in cache
    } else if (mview_refresh_modes.at(idx) == (uint64_t)ObMVRefreshMode::MAJOR_COMPACTION) {
      new_refresh_info.refresh_scn_ = mview_refresh_scns.at(idx);
      new_refresh_info.refresh_ts_ = ObTimeUtility::fast_current_time();
      new_refresh_info.expired_ts_ = new_refresh_info.refresh_ts_ +
                                     ObMViewMaintenanceService::CacheValidInterval;
      if (OB_FAIL(mview_refresh_info_map.set_refactored(mview_ids.at(idx), new_refresh_info, 1/*overwrite*/))) {
        LOG_WARN("fail to set refresh info", KR(ret), K(idx), K(mview_ids.at(idx)));
      }
      update_cache_cnt += 1;
      // for debug
      LOG_INFO("update mview refresh info", K(ret), K(mview_refresh_scns.at(idx)),
              K(update_cache_cnt));
    }
  }
  return ret;
}

int ObMViewMaintenanceService::
    get_mview_last_refresh_info(const ObIArray<uint64_t> &src_mview_ids,
                                ObMySQLProxy *sql_proxy,
                                const uint64_t tenant_id,
                                const share::SCN &scn,
                                ObIArray<uint64_t> &mview_ids,
                                ObIArray<uint64_t> &last_refresh_scns,
                                ObIArray<uint64_t> &mview_refresh_modes)
{
  int ret = OB_SUCCESS;
  mview_ids.reuse();
  last_refresh_scns.reuse();
  mview_refresh_modes.reuse();
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(sql::ObExprLastRefreshScn::
                  get_last_refresh_scn_sql(scn, src_mview_ids, sql))) {
        LOG_WARN("failed to get last refresh scn sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_proxy->read(res,
                                         tenant_id,
                                         sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(extract_sql_result(res.get_result(),
                                            mview_ids,
                                            last_refresh_scns,
                                            mview_refresh_modes))) {
        LOG_WARN("failt to extract sql result", K(ret), K(sql), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObMViewMaintenanceService::fetch_mv_refresh_scns(
                            const ObIArray<uint64_t> &src_mview_ids,
                            const share::SCN &read_snapshot,
                            ObIArray<uint64_t> &mview_ids,
                            ObIArray<uint64_t> &mview_refresh_scns,
                            uint64_t &not_hit_count)
{
  int ret = OB_SUCCESS;
  if (src_mview_ids.empty()) {
    // do nothing
  } else if (!read_snapshot.is_valid()) {
    not_hit_count += 1;
  } else {
    set_last_request_ts(ObTimeUtility::fast_current_time());
    ARRAY_FOREACH_X(src_mview_ids, idx, cnt, OB_SUCC(ret) && 0 == not_hit_count) {
      RefreshInfo refresh_info;
      if (OB_FAIL(mview_refresh_info_map_.get_refactored(src_mview_ids.at(idx), refresh_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          not_hit_count += 1;
        }
        LOG_WARN("fail to get refresh info", KR(ret), K(idx), K(src_mview_ids.at(idx)));
      } else {
        if (refresh_info.hit_cache(read_snapshot)) {
          if (OB_FAIL(mview_refresh_scns.push_back(refresh_info.refresh_scn_))) {
            LOG_WARN("fail to push back refresh scns", KR(ret), K(idx), K(src_mview_ids.at(idx)));
          }
        } else {
          not_hit_count += 1;
        }
      }
    }
  }
  return ret;
}

int ObMViewMaintenanceService::get_mview_refresh_info(const ObIArray<uint64_t> &src_mview_ids,
                                                      ObMySQLProxy *sql_proxy,
                                                      const share::SCN &read_snapshot,
                                                      ObIArray<uint64_t> &mview_ids,
                                                      ObIArray<uint64_t> &mview_refresh_scns)
{
  int ret = OB_SUCCESS;
  uint64_t not_hit_count = 0;
  const uint64_t tenant_id = MTL_ID();
  ObSEArray<uint64_t, 2> refresh_modes;
  ObSEArray<uint64_t, 2> refresh_scns;
  if (!is_inited_ || !mview_refresh_info_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret),
             K(is_inited_), K(mview_refresh_info_map_.created()));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (src_mview_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(fetch_mv_refresh_scns(src_mview_ids, read_snapshot,
                                           mview_ids, refresh_scns, not_hit_count))){
    LOG_WARN("fail to fetch mv refresh scns", KR(ret), K(tenant_id), K(src_mview_ids));
  }
  if (OB_FAIL(ret)) {
  } else if (not_hit_count == 0) {
    if (OB_FAIL(mview_ids.assign(src_mview_ids)) ||
        OB_FAIL(mview_refresh_scns.assign(refresh_scns))) {
      LOG_WARN("fail to assign mview ids or mview refresh scns", K(ret));
    }
  } else {
    mview_refresh_scns.reuse();
    if (OB_FAIL(get_mview_last_refresh_info(src_mview_ids,
                                            sql_proxy,
                                            tenant_id,
                                            read_snapshot,
                                            mview_ids,
                                            mview_refresh_scns,
                                            refresh_modes))) {
      LOG_WARN("fail to get mview last refresh info", K(ret), K(src_mview_ids), K(tenant_id));
    } else if (OB_FAIL(ObMViewMaintenanceService::
                        update_mview_refresh_info_cache(mview_ids,
                                                        mview_refresh_scns,
                                                        refresh_modes,
                                                        mview_refresh_info_map_))) {
      LOG_WARN("fail to update mview refresh info cache", K(ret), K(tenant_id));
    }
  }
  // for debug
  LOG_INFO("use mview refresh info cache",
            K(src_mview_ids), K(mview_ids),
            K(tenant_id), K(not_hit_count),
            K(mview_refresh_scns), K(read_snapshot));
  return ret;
}
} // namespace rootserver
} // namespace oceanbase
