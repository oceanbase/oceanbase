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

#include "ob_sql_stat_dump_task.h"
#include "share/wr/ob_wr_task.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_srv_network_frame.h"

#define USING_LOG_PREFIX SQL

namespace oceanbase {
namespace share {

ObSqlStatDumpTask &ObSqlStatDumpTask::get_instance() {
  static ObSqlStatDumpTask the_one;
  return the_one;
}

int ObSqlStatDumpTask::init() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(wr_proxy_.init(GCTX.net_frame_->get_req_transport()))) {
    LOG_WARN("failed to init wr proxy", K(ret));
  } else {
    sqlstat_interval_ = ObSqlStatDumpTask::REFRESH_INTERVAL;
    is_inited_ = true;
  }
  LOG_INFO("sql stat dump task init", K(ret));
  return ret;
}

void ObSqlStatDumpTask::cancel_current_task() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql stat dump task not init", K(ret));
  } else {
    TG_CANCEL(lib::TGDefIDs::ServerGTimer, *this);
    LOG_INFO("sql stat dump task cancelled", K(ObSqlStatDumpTask::REFRESH_INTERVAL), K_(sqlstat_interval));
  }
}

int ObSqlStatDumpTask::schedule_one_task(int64_t interval_us) {
  int ret = OB_SUCCESS;
  if (interval_us == 0) {
    interval_us = sqlstat_interval_;
  } else {
    sqlstat_interval_ = interval_us;
  }
  LOG_INFO("schedule sql stat dump task", K(interval_us), K_(sqlstat_interval), K(lbt()), K(this));
  return TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, *this, interval_us, false /* repeat */);
}

int ObSqlStatDumpTask::modify_sqlstat_interval(int64_t minutes) {
  int ret = OB_SUCCESS;
  if (minutes < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid value range, sqlstat_interval can not be less than 1.", K(ret));
  } else if (FALSE_IT(cancel_current_task())) {
  } else if (OB_FAIL(schedule_one_task(minutes * 60 * 1000 * 1000))) {
    LOG_WARN("failed to schedule sql stat dump task after modify sqlstat interval", K(ret));
  }
  return ret;
}

int64_t ObSqlStatDumpTask::get_sql_stat_interval(bool is_lazy_load) {
  int ret = OB_SUCCESS;
  int64_t interval = 0;
  if (is_lazy_load) {
    interval = sqlstat_interval_;
  } else {
    if (OB_FAIL(WorkloadRepositoryTask::fetch_interval_num_from_wr_control(interval, "sqlstat_interval"))) {
      interval = ObSqlStatDumpTask::REFRESH_INTERVAL;
      LOG_WARN("failed to fetch sqlstat interval from wr control", K(ret));
    } else if (interval == 0) {
      interval = ObSqlStatDumpTask::REFRESH_INTERVAL;
      LOG_WARN("sqlstat interval is 0, set to default value", K(interval));
    } else {
      //convert sqlstat_interval from minutes to us
      interval = interval * 60 * 1000 * 1000;
      sqlstat_interval_ = interval;
    }
  }
  return interval;
}
void ObSqlStatDumpTask::runTimerTask() {
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t sqlstat_interval_us = get_sql_stat_interval(false/*is_lazy_load*/);
  LOG_INFO("run sql stat dump task", K(ObSqlStatDumpTask::REFRESH_INTERVAL), K_(sqlstat_interval), K(sqlstat_interval_us));
  int64_t task_timeout_ts = ObTimeUtility::current_time() + sqlstat_interval_us - ESTIMATE_PS_RESERVE_TIME;
  THIS_WORKER.set_timeout_ts(task_timeout_ts);
  ObSqlString sql;
  char svr_ip[common::OB_IP_STR_BUFF];
  int32_t svr_port;
  GCONF.self_addr_.ip_to_string(svr_ip, sizeof(svr_ip));
  svr_port = GCONF.self_addr_.get_port();
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
        if (share::WorkloadRepositoryTask::check_tenant_can_do_wr_task(tenant_id)) {
          ObWrCreateSnapshotArg arg(
            tenant_id, SQL_STAT_SNAPSHOT_AHEAD_SNAP_ID, INVALID_LAST_SNAPSHOT_BEGIN_TIME, INVALID_LAST_SNAPSHOT_END_TIME, task_timeout_ts);
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
            LOG_DEBUG("executing sqlstat snapshot task for tenant", K(tenant_id), K(leader), K(timeout),
                K(arg));
          }
        } else {
          LOG_DEBUG("tenant cannot do wr task", K(tenant_id));
        }
      } else {
        LOG_DEBUG("Only system and user tenants support WR diagnostics", K(ret), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
        ret = OB_SUCCESS;
        LOG_WARN("wr sqlstat snapshot ahead failed for current tenant, procede to next tenant", K(tenant_id));
      }
    }
  }
  int64_t next_interval = start_time + sqlstat_interval_us - ObTimeUtility::current_time();
  if (next_interval < 0) {
    next_interval = 0;
  }
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, *this, next_interval, false /* repeat */))) {
    LOG_ERROR("failed to schedule sql stat dump task", K(ret), K(next_interval), K_(sqlstat_interval), K(sqlstat_interval_us));
  }
}

} // namespace sql
} // namespace oceanbase