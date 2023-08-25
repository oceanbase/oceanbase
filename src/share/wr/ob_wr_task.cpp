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

#define USING_LOG_PREFIX WR

#include "deps/oblib/src/lib/thread/thread_mgr.h"
#include "lib/oblog/ob_log.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/wr/ob_wr_task.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_errno.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_server_struct.h"
#include "lib/string/ob_sql_string.h"  // ObSqlString
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "share/location_cache/ob_location_service.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
namespace share
{

#define WR_SNAP_ID_SEQNENCE_NAME "OB_WORKLOAD_REPOSITORY_SNAP_ID_SEQNENCE"

WorkloadRepositoryTask::WorkloadRepositoryTask()
    : wr_proxy_(),
      snapshot_interval_(DEFAULT_SNAPSHOT_INTERVAL),
      tg_id_(-1),
      timeout_ts_(0),
      is_running_task_(false)
{}

int WorkloadRepositoryTask::schedule_one_task(int64_t interval)
{
  if (interval == 0) {
    // default interval is configed by snapshot_interval_
    interval = snapshot_interval_ * 60 * 1000L * 1000L;
  }
  return TG_SCHEDULE(tg_id_, *this, interval, false /*not repeat*/);
}

int WorkloadRepositoryTask::modify_snapshot_interval(int64_t minutes)
{
  int ret = OB_SUCCESS;
  if (minutes < 10) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sanpshot interval below 10 minutes is not allowed.", KR(ret), K(minutes));
  } else if (FALSE_IT(cancel_current_task())) {
  } else if (FALSE_IT(snapshot_interval_ = minutes)) {
  } else if (OB_FAIL(schedule_one_task())) {
    LOG_WARN("failed to schedule wr snapshot task after modify snapshot interval", K(ret));
  }

  return ret;
}

int WorkloadRepositoryTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wr_proxy_.init(GCTX.net_frame_->get_req_transport()))) {
    LOG_WARN("failed to init wr proxy", K(ret));
  }
  return ret;
}

int WorkloadRepositoryTask::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE(lib::TGDefIDs::WR_TIMER_THREAD, tg_id_))) {
    LOG_WARN("start wr timer task failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to start wr task", K(ret));
  } else {
    LOG_INFO("init wr task thread finished", K_(tg_id));
  }
  return ret;
}

void WorkloadRepositoryTask::stop()
{
  TG_STOP(tg_id_);
}

void WorkloadRepositoryTask::wait()
{
  TG_WAIT(tg_id_);
}

void WorkloadRepositoryTask::destroy()
{
  TG_DESTROY(tg_id_);
}

void WorkloadRepositoryTask::cancel_current_task()
{
  TG_CANCEL(tg_id_, *this);
}

// execute a deletion task every six snapshots completed
void WorkloadRepositoryTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  WR_STAT_GUARD(WR_SCHEDULAR);
  is_running_task_ = true;

  // current snapshot task must stop when next task is time to schedule.
  timeout_ts_ = common::ObTimeUtility::current_time() + snapshot_interval_ * 60 * 1000L * 1000L;
  int64_t snap_id = 0;
  LOG_INFO("start to take wr snapshot", KPC(this));

  // take one snapshot
  if (OB_SUCC(ret) && OB_FAIL(do_snapshot(false /*is_user_submit*/, wr_proxy_, snap_id))) {
    LOG_WARN("failed to take wr snapshot", KR(ret), KPC(this));
  }
  // delete expired snapshot
  if (OB_SUCC(ret) && 0 == snap_id % OB_WR_DELETE_SNAPSHOT_FREQUENCY) {
    if (OB_FAIL(do_delete_snapshot())) {
      LOG_WARN("failed to delete wr snapshot", K(ret));
    }
  }
  // dispatch next wr snapshot task(not repeat).
  int64_t interval = timeout_ts_ - common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(interval < 0)) {
    LOG_INFO(
        "already timeout wr snapshot interval", K(timeout_ts_), K(interval), K_(snapshot_interval));
    interval = 0;
  }
  // should dispatch next wr snapshot task regardless of errors.
  if (OB_FAIL(schedule_one_task(interval))) {
    LOG_WARN("failed to schedule wr snapshot task after modify snapshot interval", K(ret));
  }

  is_running_task_ = false;
}

constexpr int64_t WR_SNAPSHOT_CONTROLLER_TIME_GUARD = 1 * 60 * 1000 * 1000;

int WorkloadRepositoryTask::do_snapshot(
    const bool is_user_submit, obrpc::ObWrRpcProxy &wr_proxy, int64_t &snap_id)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  common::ObTimeGuard time_guard(__func__, WR_SNAPSHOT_CONTROLLER_TIME_GUARD);
  ObAddr leader;
  int64_t begin_interval_time = 0;  // in us
  int64_t end_interval_time = 0;    // in us
  int64_t ps_timeout_timestamp = 0;
  int64_t task_timeout_ts = 0;
  int64_t timeout = 0;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObArray<uint64_t> all_tenants;
  if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in upgrade, can not do wr snapshot", KR(ret));
  } else if (OB_FAIL(get_ps_timeout_timestamp(ps_timeout_timestamp))) {
    LOG_WARN("failed to get ps timeout timestamp", K(ret));
  } else if (FALSE_IT(timeout = ps_timeout_timestamp - common::ObTimeUtility::current_time() -
                                WR_SNAPSHOT_CONTROLLER_TIME_GUARD)) {
  } else if (FALSE_IT(task_timeout_ts = ps_timeout_timestamp - WR_SNAPSHOT_CONTROLLER_TIME_GUARD)) {
  } else if (OB_FAIL(get_next_snapshot_id(snap_id))) {
    LOG_WARN("failed to get next snapshot id", KR(ret));
  } else if (OB_FAIL(get_begin_interval_time(begin_interval_time))) {
    LOG_WARN("failed to get previous begin interval time", K(ret));
  } else if (FALSE_IT(end_interval_time = common::ObTimeUtility::current_time())) {
  } else if (timeout < 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("wr snapshot task already timeout", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_ids(all_tenants))) {
    LOG_WARN("failed to get all tenant ids", KR(ret));
  } else {
    if (OB_UNLIKELY(0 == begin_interval_time)) {
      // first time to do snapshot, begin_interval_time equals end_interval_time
      // FIXME: if all nodes of a cluster down for quiet long time, how to determine interval time?
      // TODO(roland.qk): Need to look at Oracle's behavior after restart
      begin_interval_time = end_interval_time;
      LOG_INFO("first time to take wr snapshot, set ", K(begin_interval_time));
    }
    for (int i = 0; OB_SUCC(ret) && i < all_tenants.size(); i++) {
      const uint64_t cur_tenant_id = all_tenants.at(i);
      if (is_sys_tenant(cur_tenant_id) || is_user_tenant(cur_tenant_id)) {
        if (check_tenant_can_do_wr_task(cur_tenant_id)) {
          ObWrCreateSnapshotArg arg(
              cur_tenant_id, snap_id, begin_interval_time, end_interval_time, task_timeout_ts);
          if (OB_FAIL(GCTX.location_service_->get_leader(
                  GCONF.cluster_id, cur_tenant_id, share::SYS_LS, false /*force_renew*/, leader))) {
            LOG_WARN("fail to get ls locaiton leader", KR(ret), K(cur_tenant_id));
          } else if (OB_FAIL(setup_tenant_snapshot_info(snap_id, cur_tenant_id, GCONF.cluster_id,
                        leader, begin_interval_time, end_interval_time,
                        is_user_submit ? ObWrSnapshotFlag::MANUAL : ObWrSnapshotFlag::SCHEDULE))) {
            LOG_WARN(
                "failed to setup snapshot info", KR(ret), K(snap_id), K(cur_tenant_id), K(leader));
          } else if (OB_FAIL(wr_proxy.to(leader)
                                .by(cur_tenant_id)
                                .timeout(timeout)
                                .group_id(share::OBCG_WR)
                                .wr_async_take_snapshot(arg, nullptr))) {
            LOG_WARN("failed to send async snapshot task", KR(ret), K(cur_tenant_id));
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(modify_tenant_snapshot_status_and_startup_time(snap_id, cur_tenant_id,
                    GCONF.cluster_id, leader, 0, ObWrSnapshotStatus::FAILED))) {
              LOG_WARN("failed to modify snapshot info status", KR(tmp_ret), K(cur_tenant_id));
            } else if (is_sys_tenant(cur_tenant_id)) {
              if (OB_TMP_FAIL(WorkloadRepositoryTask::update_snap_info_in_wr_control(
                      cur_tenant_id, snap_id, end_interval_time))) {
                LOG_WARN("failed to update wr control info", KR(tmp_ret), K(cur_tenant_id));
              }
            }
          } else {
            LOG_DEBUG("executing snapshot task for tenant", K(cur_tenant_id), K(leader), K(timeout),
                K(arg));
          }
          if (OB_FAIL(ret)) {
            save_ret = ret;
            ret = OB_SUCCESS;
            LOG_WARN(
                "wr snapshot failed for current tenant, procede to next tenant", K(cur_tenant_id));
          }
        }
      } else {
        LOG_DEBUG("Only system and user tenants support WR diagnostics", K(ret), K(cur_tenant_id));
      }
    }
  }
  ret = COVER_SUCC(save_ret);
  return ret;
}

int WorkloadRepositoryTask::do_delete_snapshot()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard(__func__, WR_SNAPSHOT_CONTROLLER_TIME_GUARD);
  int64_t ps_timeout_timestamp = 0;
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObArray<uint64_t> all_tenant_ids;
  int64_t retention = 0;
  if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location_service_ or schema_service is nullptr", K(ret), KP(GCTX.location_service_),
        KP(schema_service));
  } else if (OB_FAIL(get_ps_timeout_timestamp(ps_timeout_timestamp))) {
    LOG_WARN("failed to get ps timeout timestamp", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_ids(all_tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret));
  } else if (OB_FAIL(fetch_retention_usec_from_wr_control(retention))) {
    LOG_WARN("failed to fetch retention num from wr_control", K(ret));
  } else {
    int64_t timeout = ps_timeout_timestamp - common::ObTimeUtility::current_time() -
                      WR_SNAPSHOT_CONTROLLER_TIME_GUARD;
    int64_t task_timeout_ts = ps_timeout_timestamp - WR_SNAPSHOT_CONTROLLER_TIME_GUARD;
    int64_t end_ts_of_retention = common::ObTimeUtility::current_time() - retention;
    int save_ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < all_tenant_ids.size(); i++) {
      const uint64_t tenant_id = all_tenant_ids.at(i);
      if (OB_FAIL(do_delete_single_tenant_snapshot(
              tenant_id, cluster_id, end_ts_of_retention, task_timeout_ts, timeout, wr_proxy_))) {
        LOG_WARN("failed to do delete single tenant snapshot", K(ret), K(tenant_id), K(cluster_id),
            K(task_timeout_ts), K(end_ts_of_retention), K(timeout));
        save_ret = ret;
        ret = OB_SUCCESS;
      }
    }  // end for
    ret = COVER_SUCC(save_ret);
  }
  return ret;
}

int WorkloadRepositoryTask::get_next_snapshot_id(int64_t &snap_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fetch_snapshot_id_sequence_nextval(snap_id))) {
    if (OB_OBJECT_NAME_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("wr snapshot id sequence not exist, create it now");
      if (OB_FAIL(create_snapshot_id_sequence())) {
        LOG_WARN("failed to create wr snap_id sequence.", KR(ret));
      } else if (OB_FAIL(fetch_snapshot_id_sequence_nextval(snap_id))) {
        LOG_WARN("failed to fetch wr snapshot id after create sequence", KR(ret));
      }
    } else {
      LOG_WARN("failed to fetch wr snapshot id", KR(ret));
    }
  }
  return ret;
}

int WorkloadRepositoryTask::create_snapshot_id_sequence()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  int64_t affected_rows = 0;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("CREATE SEQUENCE %s START WITH 1 INCREMENT BY 1 "
                                    "MINVALUE 1 MAXVALUE 9223372036854775807 NOCACHE ORDER CYCLE",
                 WR_SNAP_ID_SEQNENCE_NAME))) {
    LOG_WARN("failed to assign create sequence sql string", KR(ret));
  } else if (OB_FAIL(sql_proxy->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to create wr snap_id sequence", KR(ret), K(sql));
  }
  return ret;
}

int WorkloadRepositoryTask::fetch_snapshot_id_sequence_nextval(int64_t &snap_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  ObObj snap_id_obj;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT /*+ WORKLOAD_REPOSITORY */ %s.nextval as snap_id FROM DUAL", WR_SNAP_ID_SEQNENCE_NAME))) {
      LOG_WARN("failed to assign create sequence sql string", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to fetch next snap_id sequence", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->get_obj((int64_t)0, snap_id_obj))) {
      LOG_WARN("failed to get snap_id obj", KR(ret));
    } else if (snap_id_obj.get_number().cast_to_int64(snap_id)) {
      LOG_WARN("failed to get snap_id value", KR(ret));
    }
  }
  return ret;
}

int WorkloadRepositoryTask::get_begin_interval_time(int64_t &begin_interval_time)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ time_to_usec(END_INTERVAL_TIME) FROM %s where "
                                      "tenant_id=%ld order by snap_id desc limit 1",
                   OB_WR_SNAPSHOT_TNAME, OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to format sql", KR(ret));
    } else if (OB_FAIL(
                   GCTX.sql_proxy_->read(res, gen_meta_tenant_id(OB_SYS_TENANT_ID), sql.ptr()))) {
      LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        // no record in __wr_snapshot table. this is the first time we take snapshot in this
        // cluster.
        ret = OB_SUCCESS;
        begin_interval_time = 0;
        LOG_INFO("first time to take wr snapshot in this cluster", K(begin_interval_time));
      } else {
        LOG_WARN("get next result failed", KR(ret), K(sql));
      }
    } else if (OB_FAIL(result->get_int(0L, begin_interval_time))) {
      LOG_WARN("get column fail", KR(ret), K(sql));
    }
  }
  return ret;
}

int WorkloadRepositoryTask::setup_tenant_snapshot_info(int64_t snap_id, uint64_t tenant_id,
    int64_t cluster_id, const ObAddr &addr, int64_t begin_interval_time, int64_t end_interval_time,
    ObWrSnapshotFlag snap_flag)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml_splicer;
  char svr_ip[OB_IP_STR_BUFF];
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!addr.ip_to_string(svr_ip, OB_IP_STR_BUFF))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert target addr to string", KR(ret), K(addr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column(K(tenant_id)))) {
    LOG_WARN("failed to add tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(dml_splicer.add_pk_column(K(cluster_id)))) {
    LOG_WARN("failed to add column cluster_id", KR(ret), K(cluster_id));
  } else if (OB_FAIL(dml_splicer.add_pk_column(K(snap_id)))) {
    LOG_WARN("failed to add column SNAP_ID", KR(ret), K(snap_id));
  } else if (OB_FAIL(dml_splicer.add_pk_column(K(svr_ip)))) {
    LOG_WARN("failed to add column svr_ip", KR(ret), K(svr_ip), K(addr));
  } else if (OB_FAIL(dml_splicer.add_pk_column("svr_port", addr.get_port()))) {
    LOG_WARN("failed to add column svr_port", KR(ret), K(addr));
  } else if (OB_FAIL(dml_splicer.add_time_column(K(begin_interval_time)))) {
    LOG_WARN("failed to add column begin_interval_time", KR(ret), K(begin_interval_time));
  } else if (OB_FAIL(dml_splicer.add_time_column(K(end_interval_time)))) {
    LOG_WARN("failed to add column end_interval_time", KR(ret), K(end_interval_time));
  } else if (OB_FAIL(dml_splicer.add_column("snap_flag", snap_flag))) {
    LOG_WARN("failed to add column snap_flag", KR(ret), K(snap_flag));
  } else if (OB_FAIL(dml_splicer.add_column(true, "startup_time"))) {
    LOG_WARN("failed to add column snap_flag", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_column("status", ObWrSnapshotStatus::PROCESSING))) {
    LOG_WARN("failed to add column snap_flag", KR(ret));
  } else if (OB_FAIL(dml_splicer.finish_row())) {
    LOG_WARN("failed to finish row", KR(ret));
  } else if (OB_FAIL(dml_splicer.splice_batch_insert_sql(OB_WR_SNAPSHOT_TNAME, sql))) {
    LOG_WARN("failed to generate snapshot insertion sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
                 GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write snapshot_info", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id)));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
  } else {
    LOG_DEBUG("success to insert snapshot info", K(sql));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_WR_SNAPSHOT_MODIFY_SNAPSHOT_STATUS);

int WorkloadRepositoryTask::modify_tenant_snapshot_status_and_startup_time(int64_t snap_id,
    uint64_t tenant_id, int64_t cluster_id, const ObAddr &addr, int64_t startup_time,
    ObWrSnapshotStatus status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char svr_ip[OB_IP_STR_BUFF];
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(ERRSIM_WR_SNAPSHOT_MODIFY_SNAPSHOT_STATUS)) {
    ret = ERRSIM_WR_SNAPSHOT_MODIFY_SNAPSHOT_STATUS;
    LOG_WARN("errsim in modify snapshot status", KR(ret));
  } else if (OB_UNLIKELY(!addr.ip_to_string(svr_ip, OB_IP_STR_BUFF))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert target addr to string", KR(ret), K(addr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("update /*+ WORKLOAD_REPOSITORY */ %s set status=%ld, startup_time=usec_to_time(%ld) "
                                    "where tenant_id=%ld and cluster_id=%ld and "
                                    "snap_id=%ld and svr_ip='%s' and svr_port=%d",
                 OB_WR_SNAPSHOT_TNAME, status, startup_time, tenant_id, cluster_id, snap_id, svr_ip,
                 addr.get_port()))) {
    LOG_WARN("failed to format update snapshot info sql", KR(ret));
  } else if (OB_FAIL(
                 GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write snapshot_info", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id)));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int WorkloadRepositoryTask::fetch_to_delete_snap_id_list_by_time(const int64_t end_ts_of_retention,
    const uint64_t tenant_id, const int64_t cluster_id, const ObAddr &server_addr,
    common::ObIArray<int64_t> &to_delete_snap_ids)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  char svr_ip[OB_IP_STR_BUFF];
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!server_addr.ip_to_string(svr_ip, OB_IP_STR_BUFF))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert target addr to string", KR(ret), K(server_addr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(
              sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ snap_id from %s where tenant_id=%ld and cluster_id=%ld and "
                             "end_interval_time <= usec_to_time(%ld)"
                             "LIMIT %ld",
                  OB_WR_SNAPSHOT_TNAME, tenant_id, cluster_id,
                  end_ts_of_retention, WR_FETCH_TO_DELETE_SNAP_MAX_NUM))) {
        LOG_WARN("failed to assign fetch snap_id query string", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to fetch snap_id query", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            int64_t snap_id;
            EXTRACT_INT_FIELD_MYSQL(*result, "snap_id", snap_id, int64_t);
            if (OB_SUCC(ret)) {
              if (OB_FAIL(to_delete_snap_ids.push_back(snap_id))) {
                LOG_WARN("failed to push back into to_delete_snap_ids", K(ret), K(snap_id));
              }
            }
          }
        }  // end while
      }
    }
  }
  return ret;
}

int WorkloadRepositoryTask::fetch_to_delete_snap_id_list_by_range(const int64_t low_snap_id,
    const int64_t high_snap_id, const uint64_t tenant_id, const int64_t cluster_id,
    const ObAddr &server_addr, common::ObIArray<int64_t> &to_delete_snap_ids)
{
  int ret = OB_SUCCESS;

  ObSqlString sql;
  char svr_ip[OB_IP_STR_BUFF];
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!server_addr.ip_to_string(svr_ip, OB_IP_STR_BUFF))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert target addr to string", KR(ret), K(server_addr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(
              sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ snap_id from %s where tenant_id=%ld and cluster_id=%ld and "
                             "snap_id between %ld and %ld ",   // User manually delete without limit
                  OB_WR_SNAPSHOT_TNAME, tenant_id, cluster_id,
                  low_snap_id, high_snap_id))) {
        LOG_WARN("failed to assign fetch snap_id query string", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to fetch snap_id query", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            int64_t snap_id;
            EXTRACT_INT_FIELD_MYSQL(*result, "snap_id", snap_id, int64_t);
            if (OB_SUCC(ret)) {
              if (OB_FAIL(to_delete_snap_ids.push_back(snap_id))) {
                LOG_WARN("failed to push back into to_delete_snap_ids", K(ret), K(snap_id));
              }
            }
          }
        }  // end while
      }
    }
  }
  return ret;
}

int WorkloadRepositoryTask::modify_snap_info_status_to_deleted(const uint64_t tenant_id,
    const int64_t cluster_id, const ObAddr &server_addr,
    const common::ObIArray<int64_t> &to_delete_snap_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char svr_ip[OB_IP_STR_BUFF];
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);

  if (to_delete_snap_ids.empty()) {
    // do nothing
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else if (OB_UNLIKELY(!server_addr.ip_to_string(svr_ip, OB_IP_STR_BUFF))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert target addr to string", KR(ret), K(server_addr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < to_delete_snap_ids.count(); ++i) {
      // start of batch
      if (0 == i % WR_MODIFY_BATCH_SIZE) {
        if (FALSE_IT(sql.reset())) {
        } else if (OB_FAIL(sql.assign_fmt("update /*+ WORKLOAD_REPOSITORY */ %s set status=%ld "
                                          "where tenant_id=%ld and cluster_id=%ld and "
                                          "snap_id in (",
                       OB_WR_SNAPSHOT_TNAME, ObWrSnapshotStatus::DELETED, tenant_id, cluster_id))) {
          LOG_WARN("failed to format update snapshot info sql", KR(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t snap_id = to_delete_snap_ids.at(i);
        if (OB_FAIL(sql.append_fmt(
                "%s '%lu'", (i % WR_MODIFY_BATCH_SIZE == 0) ? " " : ", ", snap_id))) {
          LOG_WARN("sql append failed", K(ret));
        }
      }

      // end of batch
      if (OB_SUCC(ret)) {
        if (to_delete_snap_ids.count() - 1 == i ||
            WR_MODIFY_BATCH_SIZE - 1 == i % WR_MODIFY_BATCH_SIZE) {
          if (OB_FAIL(sql.append_fmt(")"))) {
            LOG_WARN("sql append failed", K(ret));
          } else if (OB_FAIL(GCTX.sql_proxy_->write(
                         gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
            LOG_WARN("failed to update snapshot_info", KR(ret), K(sql),
                K(gen_meta_tenant_id(tenant_id)));
          } else if (affected_rows < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(sql));
          }
        }
      }
    }  // end for
  }
  return ret;
}

int WorkloadRepositoryTask::check_all_tenant_last_snapshot_task_finished(bool &is_all_finished)
{
  int ret = OB_SUCCESS;
  int64_t snap_id;
  if (OB_FAIL(get_last_snap_id(snap_id))) {
    LOG_WARN("failed to get last snap id", KR(ret));
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      is_all_finished = true;
      LOG_INFO("no wr snapshot records before");
    }
  } else if (OB_FAIL(check_snapshot_task_finished_for_snap_id(snap_id, is_all_finished))) {
    LOG_WARN("failed to get last snapshot status", KR(ret), K(snap_id));
  } else if (!is_all_finished) {
    int tmp_ret = OB_SUCCESS;
    int64_t task_begin_ts;
    if (OB_TMP_FAIL(get_last_snapshot_task_begin_ts(snap_id, task_begin_ts))) {
      LOG_WARN("failed to last snapshot task begin time", KR(tmp_ret));
    } else if (task_begin_ts + DEFAULT_SNAPSHOT_INTERVAL * 60 * 1000L * 1000L <=
               common::ObTimeUtility::current_time()) {
      LOG_INFO("previous task not finished but timeout", K(snap_id));
      is_all_finished = true;
    }
  }
  return ret;
}

int WorkloadRepositoryTask::get_last_snapshot_task_begin_ts(int64_t snap_id, int64_t &task_begin_ts)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  ObSqlString sql;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ time_to_usec(END_INTERVAL_TIME) FROM %s where "
                                 "cluster_id=%ld and snap_id=%ld limit 1",
              OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME, cluster_id, snap_id))) {
        LOG_WARN("failed to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_INFO(
              "target snap_id didn't have any records", KR(ret), K(snap_id), K(cluster_id), K(sql));
        }
      } else if (OB_FAIL(result->get_int(0L, task_begin_ts))) {
        LOG_WARN("get column fail", KR(ret), K(sql));
      } else {
        OB_ASSERT(result->next() == OB_ITER_END);
      }
    }
  }
  return ret;
}

int WorkloadRepositoryTask::check_snapshot_task_finished_for_snap_id(
    int64_t snap_id, bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = true;
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObArray<uint64_t> all_tenants;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_ids(all_tenants))) {
    LOG_WARN("failed to get all tenant ids", KR(ret));
  }
  for (int i = 0; is_finished && OB_SUCC(ret) && i < all_tenants.size(); i++) {
    const uint64_t tenant_id = all_tenants.at(i);
    if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
      if (check_tenant_can_do_wr_task(tenant_id)) {
        ObSqlString sql;
        SMART_VAR(ObISQLClient::ReadResult, res)
        {
          ObMySQLResult *result = nullptr;
          if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ status FROM %s where "
                                    "cluster_id=%ld and snap_id=%ld",
                  OB_WR_SNAPSHOT_TNAME, cluster_id, snap_id))) {
            LOG_WARN("failed to format sql", KR(ret));
          } else if (OB_FAIL(GCTX.sql_proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
            LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get mysql result", KR(ret), K(sql));
          } else if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("target snap_id didn't have any records, could be newly created tenant",
                  K(snap_id), K(tenant_id), K(cluster_id), K(sql));
            }
          } else {
            int64_t snap_status = 0;
            EXTRACT_INT_FIELD_MYSQL(*result, "status", snap_status, int64_t);
            if (OB_SUCC(ret)) {
              switch (static_cast<ObWrSnapshotStatus>(snap_status)) {
                case ObWrSnapshotStatus::DELETED: {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("wr snapshot been marked deleted", KR(ret), K(snap_id), K(tenant_id));
                  break;
                }
                case ObWrSnapshotStatus::FAILED: {
                  LOG_WARN("wr snapshot has failed task", K(snap_id), K(tenant_id));
                  break;
                }
                case ObWrSnapshotStatus::PROCESSING: {
                  is_finished = false;
                  LOG_DEBUG("wr snapshot still has unfinished tasks", K(snap_id), K(tenant_id));
                  break;
                }
                case ObWrSnapshotStatus::SUCCESS: {
                  LOG_DEBUG("wr snapshot success", K(snap_id), K(tenant_id));
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unknow wr snapshot status", K(snap_id), K(cluster_id), K(tenant_id),
                      K(snap_status));
                }
              }
            }
            OB_ASSERT(result->next() == OB_ITER_END);
          }
        }
      }
    }
  }
  return ret;
}

int WorkloadRepositoryTask::get_last_snap_id(int64_t &snap_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ snap_id FROM %s"
                                      " order by snap_id desc limit 1",
                   OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME))) {
      LOG_WARN("failed to format sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("failed to fetch latest snapshot id", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_EMPTY_RESULT;
        LOG_WARN("no record in __wr_snapshot table", KR(ret), K(sql));
      } else {
        LOG_WARN("get next result failed", KR(ret), K(sql));
      }
    } else {
      EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(*result, "snap_id", snap_id);
    }
  }
  return ret;
}

int WorkloadRepositoryTask::do_delete_single_tenant_snapshot(const uint64_t tenant_id,
    const int64_t cluster_id, const int64_t end_ts_of_retention, const int64_t task_timeout_ts,
    const int64_t rpc_timeout, const obrpc::ObWrRpcProxy &wr_proxy)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    if (check_tenant_can_do_wr_task(tenant_id)) {
      ObAddr leader;
      ObWrPurgeSnapshotArg arg(tenant_id, task_timeout_ts);
      if (OB_FAIL(GCTX.location_service_->get_leader(
              cluster_id, tenant_id, share::SYS_LS, false /*force_renew*/, leader))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
      } else if (OB_FAIL(fetch_to_delete_snap_id_list_by_time(end_ts_of_retention, tenant_id,
                    cluster_id, leader, arg.get_to_delete_snap_ids()))) {
        LOG_WARN("failed to fetch to delete snap id lsit by time", K(ret), K(end_ts_of_retention),
            K(tenant_id), K(cluster_id), K(leader));
      } else if (OB_UNLIKELY(arg.get_to_delete_snap_ids().empty())) {
        LOG_INFO("there are currently no snapshots to delete", K(ret), K(end_ts_of_retention),
            K(tenant_id), K(cluster_id), K(leader));
      } else if (OB_FAIL(wr_proxy.to(leader)
                            .by(tenant_id)
                            .group_id(share::OBCG_WR)
                            .timeout(rpc_timeout)
                            .wr_async_purge_snapshot(arg, nullptr))) {
        LOG_WARN("failed to send async snapshot task", KR(ret), K(tenant_id), K(arg));
      }
    }
  } else {
    LOG_DEBUG("Only system and user tenants support WR diagnostics", K(ret), K(tenant_id));
  }
  return ret;
}

int WorkloadRepositoryTask::do_delete_single_tenant_snapshot(const uint64_t tenant_id,
    const int64_t cluster_id, const int64_t low_snap_id, const int64_t high_snap_id,
    const int64_t task_timeout_ts, const int64_t rpc_timeout, const obrpc::ObWrRpcProxy &wr_proxy)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    if (check_tenant_can_do_wr_task(tenant_id)) {
      ObAddr leader;
      ObWrPurgeSnapshotArg arg(tenant_id, task_timeout_ts);
      if (OB_FAIL(GCTX.location_service_->get_leader(
              cluster_id, tenant_id, share::SYS_LS, false /*force_renew*/, leader))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
      } else if (OB_FAIL(fetch_to_delete_snap_id_list_by_range(low_snap_id, high_snap_id, tenant_id,
                    cluster_id, leader, arg.get_to_delete_snap_ids()))) {
        LOG_WARN("failed to fetch to delete snap id lsit by time", K(ret), K(low_snap_id),
            K(high_snap_id), K(tenant_id), K(cluster_id), K(leader));
      } else if (OB_UNLIKELY(arg.get_to_delete_snap_ids().empty())) {
        LOG_INFO("there are currently no snapshots to delete", K(ret), K(low_snap_id),
            K(high_snap_id), K(tenant_id), K(cluster_id), K(leader));
      } else if (OB_FAIL(wr_proxy.to(leader)
                            .by(tenant_id)
                            .group_id(share::OBCG_WR)
                            .timeout(rpc_timeout)
                            .wr_async_purge_snapshot(arg, nullptr))) {
        LOG_WARN("failed to send async snapshot task", KR(ret), K(tenant_id), K(arg));
      }
    }
  } else {
    LOG_DEBUG("Only system and user tenants support WR diagnostics", K(ret), K(tenant_id));
  }
  return ret;
}

int WorkloadRepositoryTask::get_ps_timeout_timestamp(int64_t &ps_timeout_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t interval = 0;
  if (OB_FAIL(fetch_interval_num_from_wr_control(interval))) {
    LOG_WARN("failed to fetch interval num from wr control", K(ret));
  } else if (OB_UNLIKELY(interval == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("interval is invalid number", K(ret), K(interval));
  } else {
    ps_timeout_timestamp = common::ObTimeUtility::current_time() + interval * 1000L * 1000L -
                           ESTIMATE_PS_RESERVE_TIME;
  }
  return ret;
}

int WorkloadRepositoryTask::mins_to_duration(const int64_t mins, char *string)
{
  int ret = OB_SUCCESS;
  int64_t days = mins / (24 * 60);
  int64_t remaining_mins = mins % (24 * 60);
  int64_t hours = remaining_mins / 60;
  int64_t minutes = remaining_mins % 60;
  sprintf(string, "+%ld %02ld:%02ld:00", days, hours, minutes);
  return ret;
}

int WorkloadRepositoryTask::update_wr_control(const char *time_num_col_name, const int64_t mins,
    const char *time_col_name, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char duration_chars[64];
  int64_t secs = mins * 60L;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(WorkloadRepositoryTask::mins_to_duration(mins, duration_chars))) {
    LOG_WARN("failed to conver mins to duration", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("update /*+ WORKLOAD_REPOSITORY */ %s set %s=%ld, %s='%s' "
                                    "where tenant_id=%ld",
                 OB_WR_CONTROL_TNAME, time_num_col_name, secs, time_col_name, duration_chars,
                 tenant_id))) {
    LOG_WARN("failed to format update snapshot info sql", KR(ret));
  } else if (OB_FAIL(
                 GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write snapshot_info", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id)));
  } else if (affected_rows != 1) {
    LOG_TRACE("affected rows is not 1", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int WorkloadRepositoryTask::fetch_retention_usec_from_wr_control(int64_t &retention)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ retention_num FROM %s where tenant_id = %ld",
                   OB_WR_CONTROL_TNAME, tenant_id))) {
      LOG_WARN("failed to assign create sequence sql string", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to fetch next snap_id sequence", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        // no record in __wr_control table.
        ret = OB_SUCCESS;
        // retention in us
        retention = WorkloadRepositoryTask::DEFAULT_SNAPSHOT_RETENTION * 60 * 1000L * 1000L;
        ObSqlString wr_control_sql_string;
        int64_t affected_rows = 0;
        if (OB_FAIL(get_init_wr_control_sql_string(OB_SYS_TENANT_ID, wr_control_sql_string))) {
          LOG_WARN("failed to get init wr control sql string", K(wr_control_sql_string));
        } else if (OB_FAIL(sql_proxy->write(gen_meta_tenant_id(tenant_id), wr_control_sql_string.ptr(), affected_rows))) {
          LOG_WARN("failed to insert default value into wr_control", KR(ret), K(wr_control_sql_string));
        } else if (1 != affected_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected affected_rows", K(affected_rows));
        }
        LOG_INFO("no record in __wr_control table, set default value", K(retention));
      } else {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "retention_num", retention, int64_t);
      if (OB_SUCC(ret)) {
        retention = retention * 1000L * 1000L;
        LOG_TRACE("current retention in us for wr", K(retention));
      }
    }
  }
  return ret;
}

int WorkloadRepositoryTask::fetch_interval_num_from_wr_control(int64_t &interval)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ WORKLOAD_REPOSITORY */ snapint_num FROM %s where tenant_id = %ld",
                   OB_WR_CONTROL_TNAME, tenant_id))) {
      LOG_WARN("failed to assign create sequence sql string", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to fetch next snap_id sequence", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        // no record in __wr_control table.
        ret = OB_SUCCESS;
        // interval in second
        interval = WorkloadRepositoryTask::DEFAULT_SNAPSHOT_INTERVAL * 60;
        ObSqlString wr_control_sql_string;
        int64_t affected_rows = 0;
        if (OB_FAIL(get_init_wr_control_sql_string(OB_SYS_TENANT_ID, wr_control_sql_string))) {
          LOG_WARN("failed to get init wr control sql string", K(wr_control_sql_string));
        } else if (OB_FAIL(sql_proxy->write(gen_meta_tenant_id(tenant_id), wr_control_sql_string.ptr(), affected_rows))) {
          LOG_WARN("failed to insert default value into wr_control", KR(ret), K(wr_control_sql_string));
        } else if (1 != affected_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected affected_rows", K(affected_rows));
        }
        LOG_INFO("no record in __wr_control table, set default value", K(interval));
      } else {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "snapint_num", interval, int64_t);
    }
  }
  return ret;
}
int WorkloadRepositoryTask::update_snap_info_in_wr_control(
    const int64_t tenant_id, const int64_t snap_id, const int64_t end_interval_time)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tmp_interval = 0;
  if (OB_FAIL(fetch_interval_num_from_wr_control(tmp_interval))) {
    LOG_WARN("failed to fetch interval num from wr control", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE /*+ WORKLOAD_REPOSITORY */ %s set most_recent_snap_id = %ld, "
                             "most_recent_snap_time = usec_to_time(%ld) where tenant_id = %ld",
          OB_WR_CONTROL_TNAME, snap_id, end_interval_time, tenant_id))) {
    LOG_WARN("sql assign failed", K(ret));
  } else if (OB_FAIL(
                 GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(gen_meta_tenant_id(tenant_id)), K(sql));
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows", KR(ret), K(affected_rows));
  }
  return ret;
}
int WorkloadRepositoryTask::get_init_wr_control_sql_string(
  const int64_t tenant_id,
  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t default_snap_interval_mins = WorkloadRepositoryTask::DEFAULT_SNAPSHOT_INTERVAL;
  char default_interval_char[OB_MAX_TIME_STR_LENGTH] = "";
  int64_t default_snap_retention_mins = WorkloadRepositoryTask::DEFAULT_SNAPSHOT_RETENTION;
  char default_retention_char[OB_MAX_TIME_STR_LENGTH] = "";
  int64_t default_topnsql = 30;
  if (OB_FAIL(WorkloadRepositoryTask::mins_to_duration(default_snap_interval_mins,
                                                       default_interval_char))) {
    LOG_WARN("failed to transform mins to duration string",K(ret), K(default_snap_interval_mins));
  } else if (OB_FAIL(WorkloadRepositoryTask::mins_to_duration(default_snap_retention_mins,
                                                              default_retention_char))) {
    LOG_WARN("failed to transform mins to duration string",K(ret), K(default_snap_retention_mins));
  } else if (OB_FAIL(sql.assign_fmt("INSERT IGNORE /*+ WORKLOAD_REPOSITORY use_plan_cache(none) */ INTO %s (tenant_id,  "
      "snap_interval, snapint_num, retention, retention_num, topnsql) VALUES",
        OB_WR_CONTROL_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("(%ld, '%s', %ld, '%s', %ld, %ld)", tenant_id,
                                      default_interval_char, default_snap_interval_mins*60L,
                                      default_retention_char, default_snap_retention_mins*60L,
                                      default_topnsql))) {
    LOG_WARN("sql append failed", K(ret));
  }
  return ret;
}

bool WorkloadRepositoryTask::check_tenant_can_do_wr_task(uint64_t tenant_id)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  uint64_t data_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
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

}  // end of namespace share
}  // end of namespace oceanbase
