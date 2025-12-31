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

#define USING_LOG_PREFIX SHARE

#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ash/ob_ash_refresh_task.h"
#include "share/ash/ob_active_sess_hist_task.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "share/wr/ob_wr_task.h"
#include "share/wr/ob_wr_service.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_srv_network_frame.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::common::sqlclient;

// refresh would consume time up to 10s
// if refresh time is above this threshold, refresh exection is too slow
constexpr int64_t ASH_REFESH_TIME = 10 * 1000L * 1000L;  // 10s
// we should snapshot ahead if current speed is SPEED_THRESHOLD times larger than expect speed
constexpr int SPEED_THRESHOLD = 3;
// ahead snapshot will be triggered only if the time remaining until the next scheduled snapshot is greater than SNAPSHOT_THRESHOLD
constexpr int SNAPSHOT_THRESHOLD = 180 * 1000L * 1000L;  // 180s
// free slots threshold when snapshot cannot be triggered
constexpr double FREE_SLOTS_THRESHOLD = 0.5;
// free slots threshold for ash buffer must preserved
constexpr double FREE_SLOTS_THRESHOLD_FOR_SNAPSHOT_AHEAD = 0.3;


ObAshRefreshTask &ObAshRefreshTask::get_instance()
{
  static ObAshRefreshTask the_one;
  return the_one;
}

int ObAshRefreshTask::start()
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ASH_REFRESH_INTERVAL < SNAPSHOT_THRESHOLD, "ASH_REFRESH_INTERVAL should be less than SNAPSHOT_THRESHOLD");
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer,
                                 *this,
                                 ASH_REFRESH_INTERVAL,
                                 true /* repeat */))) {
    LOG_WARN("fail define timer schedule", K(ret));
  } else {
    if (OB_FAIL(wr_proxy_.init(GCTX.net_frame_->get_req_transport()))) {
      LOG_WARN("failed to init wr proxy", K(ret));
    }
    LOG_INFO("AshRefresh init OK");
    last_scheduled_snapshot_time_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

void ObAshRefreshTask::runTimerTask()
{
  if (0 == prev_write_pos_) {
  } else if (GCONF._ob_ash_disk_write_enable) {
    int64_t task_timeout_ts = ObTimeUtility::current_time() + ASH_REFRESH_INTERVAL - ESTIMATE_PS_RESERVE_TIME;
    THIS_WORKER.set_timeout_ts(task_timeout_ts);
    int ret = OB_SUCCESS;
    common::ObTimeGuard time_guard(__func__, ASH_REFESH_TIME);
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObSqlString sql;
    int64_t read_pos = 0;
    int64_t last_snapshot_end_time = 0;
    int64_t snapshot_flag = 0;
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    char svr_ip[common::OB_IP_STR_BUFF];
    int32_t svr_port;
    GCONF.self_addr_.ip_to_string(svr_ip, sizeof(svr_ip));
    svr_port = GCONF.self_addr_.get_port();

    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      ObMySQLResult *result = nullptr;
      if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ time_to_usec(END_INTERVAL_TIME), snap_flag FROM %s where "
                                        "snap_id=%ld and tenant_id=%ld",
                    OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME, LAST_SNAPSHOT_RECORD_SNAP_ID, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to format sql", KR(ret));
      } else if (OB_FAIL(
                    sql_proxy->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // no record in __wr_snapshot table. this is the first time we take snapshot in this
          // cluster or user delete this record.
          last_snapshot_end_time = last_scheduled_snapshot_time_;
          ret = OB_SUCCESS;
          LOG_WARN("last snapshot end time doesn't exist in this cluster", K(tenant_id));
        }
      } else if (OB_FAIL(result->get_int(0L, last_snapshot_end_time))) {
          LOG_WARN("get column fail", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_int(1L, snapshot_flag))) {
          LOG_WARN("get column fail", KR(ret), K(sql));
      }

      if (OB_SUCC(ret)) {
        res.reuse();
        sql.reuse();
        if (OB_ISNULL(sql_proxy)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
        } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ min(sample_id)+1 from %s where "
                                        "svr_ip='%s' and svr_port=%d and sample_time>usec_to_time(%ld)",
                      OB_V_OB_ACTIVE_SESSION_HISTORY_TNAME, svr_ip, svr_port, last_snapshot_end_time))) {
          LOG_WARN("failed to format sql", KR(ret));
        } else if (OB_FAIL(
                      GCTX.sql_proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get mysql result", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get next row", KR(ret));
        } else if (OB_FAIL(result->get_int(0L, read_pos))) {
            LOG_WARN("get column fail", KR(ret), K(sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObActiveSessHistList::get_instance().set_read_pos(read_pos);
      if (static_cast<ObWrSnapshotFlag>(snapshot_flag) == ObWrSnapshotFlag::LAST_SCHEDULED_SNAPSHOT) {
        last_scheduled_snapshot_time_ = last_snapshot_end_time;
      }
      bool need_do_ash_take_ahead = require_snapshot_ahead();
      LOG_INFO("start do ash refresh task", K(prev_write_pos_), K(prev_sched_time_),
                                      K(ObActiveSessHistList::get_instance().write_pos()),
                                      K(ObActiveSessHistList::get_instance().free_slots_num()),
                                      K(last_scheduled_snapshot_time_),
                                      K(last_snapshot_end_time),
                                      K(need_do_ash_take_ahead));
      // send rpc
      if (need_do_ash_take_ahead) {
        int64_t timeout = task_timeout_ts - ObTimeUtility::current_time();
        ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        ObArray<uint64_t> all_tenants;
        ObAddr leader;
        if (OB_FAIL(schema_service->get_tenant_ids(all_tenants))) {
          LOG_WARN("failed to get all tenant ids", KR(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < all_tenants.size(); i++) {
            const uint64_t tenant_id = all_tenants.at(i);
            if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
              if (check_tenant_can_do_wr_task(tenant_id)) {
                ObWrCreateSnapshotArg arg(
                      tenant_id, LAST_SNAPSHOT_RECORD_SNAP_ID, last_snapshot_end_time, ObTimeUtility::current_time(), task_timeout_ts);
                if (OB_FAIL(GCTX.location_service_->get_leader(
                      GCONF.cluster_id, tenant_id, share::SYS_LS, false /*force_renew*/, leader))) {
                  LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
                } else if (OB_FAIL(wr_proxy_.to(leader)
                                    .by(tenant_id)
                                    .timeout(timeout)
                                    .group_id(share::OBCG_WR)
                                    .wr_async_take_snapshot(arg, nullptr))) {
                  LOG_WARN("failed to send async snapshot task", KR(ret), K(tenant_id));
                } else {
                  LOG_DEBUG("executing snapshot task for tenant", K(tenant_id), K(leader), K(timeout),
                      K(arg));
                }
              }
            } else {
              LOG_DEBUG("Only system and user tenants support WR diagnostics", K(ret), K(tenant_id));
            }
            if (OB_FAIL(ret)) {
              ret = OB_SUCCESS;
              LOG_WARN(
                  "wr snapshot ahead failed for current tenant, procede to next tenant", K(tenant_id));
            }
          }
        }
      }
    }
  }
  prev_write_pos_ = ObActiveSessHistList::get_instance().write_pos();
  prev_sched_time_ = ObTimeUtility::current_time();
}

ERRSIM_POINT_DEF(EN_FORCE_ENABLE_SNAPSHOT_AHEAD);

bool ObAshRefreshTask::require_snapshot_ahead()
{
  bool bret = false;
  ObActiveSessHistList::get_instance().set_pre_check_snapshot_time(ObTimeUtility::current_time());
  int64_t write_pos = ObActiveSessHistList::get_instance().write_pos();
  int64_t free_slots_num = ObActiveSessHistList::get_instance().free_slots_num();
  int64_t scheduled_snapshot_interval = GCTX.wr_service_->get_snapshot_interval(false) * 60 * 1000L * 1000L;
  int64_t next_scheduled_snapshot_interval = scheduled_snapshot_interval - (ObTimeUtility::current_time() - last_scheduled_snapshot_time_);
  if (next_scheduled_snapshot_interval <= 0 || next_scheduled_snapshot_interval > scheduled_snapshot_interval) {
    // last scheduled snapshot time update may lose or no last scheduled snapshot or this machine's clock is behind
    // cluster pressure may be heavy
    LOG_WARN_RET(OB_INVALID_TIMESTAMP, "unexpected next scheduled snapshot interval");
    //可能由于重启或者切主导致无法实现第一次定时落盘，此时当ash buffer使用率超过FREE_SLOTS_THRESHOLD时，强制启用快照提前
    if (free_slots_num < ObActiveSessHistList::get_instance().size() * FREE_SLOTS_THRESHOLD) {
      bret = true;
      LOG_INFO("force enable snapshot ahead because of free slots num is too small");
    } else {
      bret = false;
    }
  } else {
    double cur_speed = 1.0 * (write_pos - prev_write_pos_) / ((ObTimeUtility::current_time() - prev_sched_time_) / 1000 / 1000);
    double expect_speed = 1.0 * free_slots_num / (next_scheduled_snapshot_interval / 1000 / 1000);
    bret = (cur_speed >= SPEED_THRESHOLD * expect_speed ||
            free_slots_num < ObActiveSessHistList::get_instance().size() * FREE_SLOTS_THRESHOLD_FOR_SNAPSHOT_AHEAD) &&
           (next_scheduled_snapshot_interval > SNAPSHOT_THRESHOLD);
  }
  if (EN_FORCE_ENABLE_SNAPSHOT_AHEAD) {
    bret = true;
    LOG_INFO("force enable snapshot ahead");
  }
  ObActiveSessHistList &ash_list = ObActiveSessHistList::get_instance();
  const int64_t read_pos = ash_list.read_pos();
  const int64_t size = ash_list.size();
  if (write_pos >= read_pos &&
      write_pos <= read_pos + size) {
    FLOG_WARN_RET(OB_SUCCESS, "check need snapshot ahead, write pos in range", K(bret),
                 K(prev_write_pos_), K(write_pos),
                 K(read_pos),
                 K(size),
                 K(SPEED_THRESHOLD),
                 K(next_scheduled_snapshot_interval),
                 K(SNAPSHOT_THRESHOLD));
  } else {
    FLOG_WARN_RET(OB_ERR_UNEXPECTED, "check need snapshot ahead, write pos out of range", K(bret),
                 K(prev_write_pos_), K(write_pos),
                 K(read_pos),
                 K(size),
                 K(SPEED_THRESHOLD),
                 K(next_scheduled_snapshot_interval),
                 K(SNAPSHOT_THRESHOLD));
  }
  return bret;
}

bool ObAshRefreshTask::check_tenant_can_do_wr_task(uint64_t tenant_id)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  uint64_t data_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_3_5_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.3.5, workload repository not supported");
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
  } else if (tenant_schema->is_normal()) {
    bret = true;
  } else {
    LOG_WARN("tenant status abnormal for wr", K(tenant_schema->get_status()), K(tenant_id));
  }
  return bret;
}
