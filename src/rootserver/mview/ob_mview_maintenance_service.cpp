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
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

/**
 * ObMViewMaintenanceService
 */

ObMViewMaintenanceService::ObMViewMaintenanceService() : is_inited_(false),
                                                         mview_refresh_info_timestamp_(0),
                                                         mview_mds_timestamp_(0),
                                                         proposal_id_(0)
  {}

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
  ObMemAttr attr(tenant_id, "MViewService");
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
    } else if (OB_FAIL(mview_mds_task_.init())) {
      LOG_WARN("fail to init mview mds task", KR(ret));
    } else if (OB_FAIL(mview_refresh_info_cache_.create(bucket_num, attr))) {
      LOG_WARN("fail to create mview refresh info cache", KR(ret));
    } else if (OB_FAIL(mview_mds_map_.create(bucket_num, attr))) {
      LOG_WARN("fail to create mview mds map", KR(ret));
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
    } else if (OB_FAIL(pending_task_manager_.init())) {
      LOG_WARN("fail to init pending task manager", KR(ret));
#endif
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
  } else if (!is_meta_tenant(MTL_ID()) && OB_FAIL(mview_update_cache_task_.start())) { // run on every tenant server
    LOG_WARN("fail to start mview update cache task", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObMViewMaintenanceService::stop()
{
  sys_ls_task_stop_();
  mview_update_cache_task_.stop();
}

void ObMViewMaintenanceService::sys_ls_task_stop_()
{
  mlog_maintenance_task_.stop();
  mview_maintenance_task_.stop();
  mvref_stats_maintenance_task_.stop();
  mview_push_refresh_scn_task_.stop();
  mview_push_snapshot_task_.stop();
  replica_safe_check_task_.stop();
  collect_mv_merge_info_task_.stop();
  mview_clean_snapshot_task_.stop();
  mview_mds_task_.stop();
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  pending_task_manager_.stop();
#endif
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
  mview_mds_task_.wait();
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  pending_task_manager_.wait();
#endif
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
  mview_refresh_info_cache_.destroy();
  mview_mds_task_.destroy();
  mview_mds_map_.destroy();
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  pending_task_manager_.destroy();
#endif
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
    common::ObRole role;
    int64_t proposal_id;
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
    } else if (OB_FAIL(mview_mds_task_.start())) {
      LOG_WARN("fail to start mview mds task", KR(ret));
    } else if (OB_FAIL(mview_mds_task_.update_mview_mds_op())) {
      LOG_WARN("fail to update mview mds op", KR(ret));
    } else if (OB_FAIL(MTL(logservice::ObLogService *)->
                       get_palf_role(share::SYS_LS, role, proposal_id))) {
      LOG_WARN("fail to get palf role", KR(ret), K(role), K(proposal_id));
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
    } else if (OB_FAIL(pending_task_manager_.start())) {
      LOG_WARN("fail to start pending task manager", KR(ret));
#endif
    } else {
      proposal_id_ = proposal_id;
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
  } else if (FALSE_IT(sys_ls_task_stop_())) {
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

int ObMViewMaintenanceService::update_mview_refresh_info_cache(
        const ObIArray<uint64_t> &mview_ids,
        const ObIArray<uint64_t> &mview_refresh_scns,
        const ObIArray<uint64_t> &mview_refresh_modes) {
  int ret = OB_SUCCESS;
  int update_cache_cnt = 0;
  int64_t start_ts = ObTimeUtility::current_time();
  hash::ObHashSet<uint64_t> update_set;
  ObSEArray<uint64_t, 1> del_mview_id;
  if (OB_FAIL(update_set.create(10))) {
    LOG_WARN("init update set failed", KR(ret));
  } else {
    ARRAY_FOREACH_X(mview_ids, idx, cnt, OB_SUCC(ret)) {
      if (mview_refresh_scns.at(idx) > 0 && mview_refresh_modes.at(idx) == (uint64_t)ObMVRefreshMode::MAJOR_COMPACTION) {
        MViewRefreshInfo cache_info;
        bool need_update = true;
        if (OB_FAIL(update_set.set_refactored(mview_ids.at(idx)))) {
          LOG_WARN("fail to set mview_id", KR(ret), K(idx), K(mview_ids.at(idx)));
        } else if (OB_FAIL(mview_refresh_info_cache_.get_refactored(mview_ids.at(idx), cache_info))) {
          if (OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
          }
        } else if (mview_refresh_scns.at(idx) == cache_info.refresh_scn_) {
         need_update = false;
        }
        if (OB_SUCC(ret) && need_update) {
          MViewRefreshInfo new_refresh_info;
          new_refresh_info.refresh_scn_ = mview_refresh_scns.at(idx);
          if (OB_FAIL(mview_refresh_info_cache_.set_refactored(mview_ids.at(idx), new_refresh_info, 1/*overwrite*/))) {
            LOG_WARN("fail to set refresh info", KR(ret), K(idx), K(mview_ids.at(idx)));
          } else {
            update_cache_cnt++;
          }
        }
      }
    }
  }
  // remove deleted mview_id in cache
  if (OB_SUCC(ret) && mview_refresh_info_cache_.size() != update_set.size()) {
    for (MViewRefreshInfoCache::iterator it = mview_refresh_info_cache_.begin();OB_SUCC(ret) && it != mview_refresh_info_cache_.end(); it++) {
      if (OB_FAIL(update_set.exist_refactored(it->first))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST) {
          if (OB_FAIL(del_mview_id.push_back(it->first))) {
            LOG_WARN("del_mview_id push failed", KR(ret), K(it->first));
          }
        } else {
          LOG_WARN("check mview_id failed", KR(ret), K(it->first));
        }
      }
    }
    for (int64_t idx = 0; idx < del_mview_id.count() && OB_SUCC(ret); idx++) {
      if (OB_FAIL(mview_refresh_info_cache_.erase_refactored(del_mview_id.at(idx))))  {
        LOG_WARN("erash mview failed", KR(ret), K(del_mview_id.at(idx)));
      }
    }
  }
  // update timestamp
  if (OB_SUCC(ret)) {
    mview_refresh_info_timestamp_ = start_ts;
  }
  int64_t end_ts = ObTimeUtility::current_time();
  LOG_INFO("update mview refresh info", K(ret), K(mview_ids), K(mview_refresh_scns), K(update_cache_cnt), K(del_mview_id),
      "cost", end_ts - start_ts);
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
  uint64_t data_version = 0;
  if (OB_ISNULL(sql_proxy) || tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(get_mview_last_refresh_info_sql_(scn, src_mview_ids, tenant_id, sql))) {
        LOG_WARN("failed to get last refresh scn sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_proxy->read(res,
                                         tenant_id,
                                         sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_FAIL(mview_update_cache_task_.extract_sql_result(res.get_result(),
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
                            bool &hit_cache)
{
  int ret = OB_SUCCESS;
  hit_cache = false;
  if (src_mview_ids.empty()) {
    // do nothing
  } else if (!read_snapshot.is_valid()) {
  } else if (ObTimeUtil::current_time() - mview_refresh_info_timestamp_ > CacheValidInterval) {
    // cache expired
  } else {
    int64_t succ_cnt = 0;
    ARRAY_FOREACH_X(src_mview_ids, idx, cnt, OB_SUCC(ret)) {
      MViewRefreshInfo refresh_info;
      if (OB_FAIL(mview_refresh_info_cache_.get_refactored(src_mview_ids.at(idx), refresh_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("fail to get refresh info", KR(ret), K(idx), K(src_mview_ids.at(idx)));
      } else {
        if (read_snapshot.get_val_for_tx() >= refresh_info.refresh_scn_) {
          if (OB_FAIL(mview_refresh_scns.push_back(refresh_info.refresh_scn_))) {
            LOG_WARN("fail to push back refresh scns", KR(ret), K(idx), K(src_mview_ids.at(idx)));
          } else {
            succ_cnt++;
          }
        } else {
          break;
        }
      }
      if (OB_SUCC(ret) && src_mview_ids.count() == succ_cnt) {
        hit_cache = true;
      }
    }
  }
  return ret;
}

int ObMViewMaintenanceService::get_mview_last_refresh_info_sql_(
                               const share::SCN &scn,
                               const ObIArray<uint64_t> &mview_ids,
                               const uint64_t tenant_id,
                               ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString mview_id_array;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(mview_ids.empty() || tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect error", K(ret), K(mview_ids), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get min data version", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(mview_ids.count() > 100)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("more than 100 different materialized view id used in last_refresh_scn", K(ret), K(mview_ids.count()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "more than 100 different materialized view id used in last_refresh_scn is");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < mview_ids.count(); ++i) {
      if (OB_FAIL(mview_id_array.append_fmt(0 == i ? "%ld" : ",%ld", mview_ids.at(i)))) {
        LOG_WARN("fail to append fmt", KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (data_version < MOCK_DATA_VERSION_4_3_5_3 || (data_version >= DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_2_0)) {
    if (OB_FAIL(sql.assign_fmt("SELECT CAST(MVIEW_ID AS UNSIGNED) AS MVIEW_ID, \
                                LAST_REFRESH_SCN, \
                                CAST(REFRESH_MODE AS UNSIGNED) AS REFRESH_MODE \
                                FROM `%s`.`%s`", OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME))){
      LOG_WARN("fail to assign sql", K(ret));
    }
  } else if ((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0) || data_version >= DATA_VERSION_4_4_2_0) {
    if (OB_FAIL(sql.assign_fmt("SELECT CAST(MVIEW_ID AS UNSIGNED) AS MVIEW_ID, \
                                CASE WHEN IS_SYNCED = FALSE OR REFRESH_MODE = 4 THEN LAST_REFRESH_SCN \
                                ELSE DATA_SYNC_SCN \
                                END AS LAST_REFRESH_SCN, \
                                CAST(REFRESH_MODE AS UNSIGNED) AS REFRESH_MODE \
                                FROM `%s`.`%s`", OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME))){
      LOG_WARN("fail to assign sql", K(ret));
    }
  }
  // append as of snapshot and filter info
  if (OB_FAIL(ret)) {
  } else if (scn.is_valid() &&
             OB_FAIL(sql.append_fmt(" AS OF SNAPSHOT %ld", scn.get_val_for_sql()))) {
    LOG_WARN("fail to append sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = 0 AND MVIEW_ID IN (%.*s)",
                     (int)mview_id_array.length(), mview_id_array.ptr()))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  LOG_INFO("get last refresh info sql", K(ret), K(sql));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_GET_WRONG_CACHE);
int ObMViewMaintenanceService::get_mview_refresh_info(const ObIArray<uint64_t> &src_mview_ids,
                                                      ObMySQLProxy *sql_proxy,
                                                      const share::SCN &read_snapshot,
                                                      ObIArray<uint64_t> &mview_ids,
                                                      ObIArray<uint64_t> &mview_refresh_scns)
{
  int ret = OB_SUCCESS;
  bool hit_cache = false;
  const uint64_t tenant_id = MTL_ID();
  ObSEArray<uint64_t, 2> refresh_modes;
  ObSEArray<uint64_t, 2> refresh_scns;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(sql_proxy));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (src_mview_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(fetch_mv_refresh_scns(src_mview_ids, read_snapshot,
                                           mview_ids, refresh_scns, hit_cache))){
    LOG_WARN("fail to fetch mv refresh scns", KR(ret), K(tenant_id), K(src_mview_ids));
  }
  if (OB_FAIL(ret)) {
  } else if (hit_cache) {
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
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(ERRSIM_GET_WRONG_CACHE)) {
    LOG_WARN("errsim get wrong cache", K(ret), K(mview_refresh_scns));
    const uint64_t two_day_ns = 172800000000000; // 2 * 24 * 60 * 60 * 1000 * 1000 * 1000;
    for (int idx = 0; idx < mview_refresh_scns.count(); idx++) {
      if (mview_refresh_scns.at(idx) > two_day_ns) {
        mview_refresh_scns.at(idx) -= two_day_ns;
      }
      LOG_INFO("reduce mview refresh scn", K(idx), K(mview_refresh_scns.at(idx)));
    }
    ret = OB_SUCCESS;
  }
#endif
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    LOG_INFO("get_mview_refresh_info", K(ret), K(src_mview_ids), K(mview_ids),
            K(tenant_id), K(hit_cache),
            K(mview_refresh_scns), K(read_snapshot));
  }
  return ret;
}

int ObMViewMaintenanceService::get_min_mview_mds_snapshot(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  GetMinMVMdsSnapshotFunctor get_min_func(scn);
  if (OB_FAIL(mview_mds_map_.foreach_refactored(get_min_func))) {
    LOG_WARN("fail to get min mview mds snapshot", K(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::get_min_mview_pending_task_snapshot(share::SCN &scn)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  if (OB_FAIL(pending_task_manager_.get_min_pending_task_snapshot(scn))) {
    LOG_WARN("fail to get min mview pending task snapshot", K(ret));
  }
#else
  scn.set_invalid();
#endif
  return ret;
}

// read from __all_mview_dep and filter those table not mview
// for example: all tables are mviews
// A
// ├── B
// │   └── D
// ├── C
// └── D
// E ── F
int ObMViewMaintenanceService::check_nested_mview_mds_exists(
                               const uint64_t refresh_id,
                               const share::SCN &target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (refresh_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_id));
  } else {
    CheckMVMdsExistFunctor check_exist_func(exist, refresh_id, target_data_sync_scn);
    if (OB_FAIL(mview_mds_map_.foreach_refactored(check_exist_func))) {
      LOG_WARN("fail to foreach mview mds map", K(ret));
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret) && !exist) {
    ret = OB_EAGAIN;
    LOG_WARN("nested mview mds not exist", K(ret), K(refresh_id),
             K(target_data_sync_scn), K(mview_mds_map_.size()));
  }
  // for debug
  LOG_INFO("check nested mview mds exists", K(ret), K(refresh_id),
           K(target_data_sync_scn));
  return ret;
}

int ObMViewMaintenanceService::get_min_target_data_sync_scn(
                               const uint64_t mview_id,
                               share::SCN &target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  target_data_sync_scn.reset();
  if (mview_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(mview_id));
  } else {
    GetMVMinTargetDataSyncScnFunctor get_min_fun(mview_id, target_data_sync_scn);
    if (OB_FAIL(mview_mds_map_.foreach_refactored(get_min_fun))) {
      LOG_WARN("fail to foreach mview mds map", K(ret));
    }
  }
  LOG_DEBUG("get min target scn", K(ret), K(mview_id), K(target_data_sync_scn));
  return ret;
}

int ObMViewMaintenanceService::GetMinMVMdsSnapshotFunctor::
     operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv)
{
  int ret = OB_SUCCESS;
  share::SCN scn;
  if (mv_mds_kv.second.read_snapshot_ > 0 && (!scn.is_valid() || mv_mds_kv.second.read_snapshot_ < scn.get_val_for_tx())) {
    scn.convert_for_tx(mv_mds_kv.second.read_snapshot_);
  }
  if (mv_mds_kv.second.mview_op_type_ == MVIEW_OP_TYPE::NESTED_SYNC_REFRESH) {
    if (mv_mds_kv.second.target_data_sync_scn_.is_valid()) {
      if (!scn.is_valid() || (mv_mds_kv.second.target_data_sync_scn_ < scn)) {
        scn = mv_mds_kv.second.target_data_sync_scn_;
      }
    }
  }
  if (scn.is_valid() && (!scn_.is_valid() || scn < scn_)) {
    scn_ = scn;
  }
  return ret;
}

int ObMViewMaintenanceService::CheckMVMdsExistFunctor::
     operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv)
{
  int ret = OB_SUCCESS;
  exist_ = false;
  if (mv_mds_kv.second.mview_op_type_ == storage::MVIEW_OP_TYPE::NESTED_SYNC_REFRESH) {
    LOG_DEBUG("check nested mview mds exists", K(ret), K(mv_mds_kv.second));
    if (mv_mds_kv.second.refresh_id_ == refresh_id_) {
      if (!target_data_sync_scn_.is_valid()) {
        exist_ = true;
      } else if (mv_mds_kv.second.target_data_sync_scn_ == target_data_sync_scn_) {
        exist_ = true;
      }
    }
    if (exist_) {
      // to break foreach loop
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObMViewMaintenanceService::GetMVMinTargetDataSyncScnFunctor::
     operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv)
{
  int ret = OB_SUCCESS;
  if (mv_mds_kv.second.mview_op_type_ == storage::MVIEW_OP_TYPE::NESTED_SYNC_REFRESH) {
    bool exist_mview = false;
    ARRAY_FOREACH(mv_mds_kv.second.nested_mview_lists_, idx) {
      const uint64_t nested_mview_id = mv_mds_kv.second.nested_mview_lists_.at(idx);
      if (nested_mview_id == mview_id_) {
        exist_mview = true;
        break;
      }
    }
    if (exist_mview) {
      if (mv_mds_kv.second.target_data_sync_scn_.is_valid()) {
        if (!target_data_sync_scn_.is_valid() ||
            mv_mds_kv.second.target_data_sync_scn_ < target_data_sync_scn_) {
          target_data_sync_scn_ = mv_mds_kv.second.target_data_sync_scn_;
        }
      } else {
        ret = OB_EAGAIN;
        target_data_sync_scn_.reset();
        LOG_INFO("exist nested data sync refresh with invalid target scn",
                  K(ret), K(mview_id_), K(target_data_sync_scn_), K(mv_mds_kv.first), K(mv_mds_kv.second));
      }
    }
  }
  return ret;
}


} // namespace rootserver
} // namespace oceanbase
