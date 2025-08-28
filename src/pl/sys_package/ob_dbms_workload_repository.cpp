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

#define USING_LOG_PREFIX PL

#include "ob_dbms_workload_repository.h"
#include "observer/ob_srv_network_frame.h"  // ObSrvNetworkFrame
#include "share/wr/ob_wr_task.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/ob_version.h"
#include <sys/utsname.h>
#include "src/pl/ob_pl.h"
#include "lib/file/ob_string_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace share::schema;
using namespace common::sqlclient;
class ObSrvNetworkFrame;
namespace pl
{
#define TIME_WINDOWS_NUMS 4

ObDbmsWorkloadRepository::SqlIdArray::SqlIdArray()
{
  str_[0] = '\0';
}

ObDbmsWorkloadRepository::SqlIdArray::SqlIdArray(const char *str)
{
  STRNCPY(str_, str, OB_MAX_SQL_ID_LENGTH + 1);
  str_[OB_MAX_SQL_ID_LENGTH] = '\0';
}

ObDbmsWorkloadRepository::SqlIdArray::SqlIdArray(const SqlIdArray &other)
{
  STRNCPY(str_, other.str_, OB_MAX_SQL_ID_LENGTH + 1);
  str_[OB_MAX_SQL_ID_LENGTH] = '\0';
}

bool ObDbmsWorkloadRepository::SqlIdArray::operator==(const SqlIdArray& other) const
{
  return STRNCMP(str_, other.str_, OB_MAX_SQL_ID_LENGTH + 1) == 0;
}
int ObDbmsWorkloadRepository::create_snapshot(
    ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  WR_STAT_GUARD(WR_USER_SUBMIT_SNAPSHOT);
  UNUSED(params);
  UNUSED(result);
  ObAddr leader;
  int64_t snap_id = 0;
  obrpc::ObWrRpcProxy wr_proxy;
  int64_t cluster_id = GCONF.cluster_id;
  static const int64_t SLEEP_INTERVAL_US = 1 * 1000L * 1000L;  // 1s
  int64_t timeout_ts =
      common::ObTimeUtility::current_time() + WorkloadRepositoryTask::WR_MIN_SNAPSHOT_INTERVAL;
  ObWrUserSubmitSnapArg user_submit_snap_arg(timeout_ts);
  ObWrUserSubmitSnapResp user_submit_snapshot_resp;

  uint64_t data_version = OB_INVALID_VERSION;

  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (!is_sys_tenant(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("only sys tenant can create snapshot", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "sys tenant");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
  } else if (OB_FAIL(GCTX.location_service_->get_leader(
                 cluster_id, OB_SYS_TENANT_ID, share::SYS_LS, false /*force_renew*/, leader))) {
    LOG_WARN("fail to get ls locaiton leader", KR(ret), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(wr_proxy.init(GCTX.net_frame_->get_req_transport()))) {
    LOG_WARN("failed to init wr proxy", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    do {
      if (OB_UNLIKELY(common::ObTimeUtility::current_time() >= timeout_ts)) {
        ret = OB_TIMEOUT;
        LOG_WARN("wr snapshot task already timeout", K(ret));
      } else if (OB_FAIL(wr_proxy.to(leader)
                             .by(OB_SYS_TENANT_ID)
                             .group_id(share::OBCG_WR)
                             .timeout(WorkloadRepositoryTask::WR_MIN_SNAPSHOT_INTERVAL)
                             .wr_sync_user_submit_snapshot_task(
                                 user_submit_snap_arg, user_submit_snapshot_resp))) {
        if (OB_NEED_RETRY == ret) {
          ob_usleep(SLEEP_INTERVAL_US);
        } else {
          LOG_WARN("failed to send sync snapshot task", KR(ret), K(OB_SYS_TENANT_ID),
              K(user_submit_snapshot_resp));
        }
      }
      // if there are some snapshot tasks in processing, we need to retry dispatching snapshot task.
    } while (OB_NEED_RETRY == ret);

    if (OB_SUCC(ret)) {
      int64_t snap_id = user_submit_snapshot_resp.get_snap_id();
      bool is_all_finished = false;
      // wait until snapshot task finished or timeout.
      while (OB_SUCC(ret) && !is_all_finished) {
        if (OB_UNLIKELY(common::ObTimeUtility::current_time() >= timeout_ts)) {
          ret = OB_TIMEOUT;
          LOG_WARN("wr snapshot task already timeout", K(ret));
        } else if (OB_FAIL(WorkloadRepositoryTask::check_snapshot_task_finished_for_snap_id(
                       snap_id, is_all_finished))) {
          LOG_WARN("failed to check all tenants' last snapshot status", K(ret), K(is_all_finished));
        } else if (!is_all_finished) {
          ob_usleep(SLEEP_INTERVAL_US);
        }
      }
      if (OB_SUCC(ret)) {
        // if snapshot task finished, check its belonging snapshot status.
        bool is_all_success = false;
        if (OB_UNLIKELY(false == is_all_finished)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user create wr snapshot not finish", KR(ret), K(snap_id));
        } else if (OB_FAIL(check_snapshot_task_success_for_snap_id(snap_id, is_all_success))) {
          LOG_WARN("failed to check wr snapshot status", KR(ret), K(snap_id));
        } else {
          if (OB_UNLIKELY(is_all_success == false)) {
            ret = OB_ERROR;
            LOG_WARN("user submit wr snapshot failed but error code cannot be retrieved", K(ret),
                K(snap_id));
          } else {
            LOG_DEBUG("all wr task success", K(snap_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::check_snapshot_task_success_for_snap_id(
    int64_t snap_id, bool &is_all_success)
{
  int ret = OB_SUCCESS;
  is_all_success = true;
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS CNT FROM %s where "
                                  "cluster_id=%ld and snap_id=%ld and status!=%ld",
              OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME, cluster_id, snap_id, ObWrSnapshotStatus::SUCCESS))) {
        LOG_WARN("failed to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed read __wr_snapshot records",K(snap_id), K(cluster_id));
      } else {
        int64_t cnt = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "CNT", cnt, int64_t);
        if (OB_SUCC(ret)) {
          if (cnt > 0) {
            is_all_success = false;
          } else if (cnt == 0) {
            is_all_success = true;
          } else if (OB_UNLIKELY(cnt < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wr snapshot records count unexpected", K(snap_id), K(cluster_id), K(cnt));
          }
        }
        OB_ASSERT(result->next() == OB_ITER_END);
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::drop_snapshot_range(
    ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(2 != params.count())) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("parameters number is wrong", K(ret), K(params.count()));
  } else if (params.at(0).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("first parameters is null", K(ret), K(params.at(0)));
  } else if (params.at(1).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("second parameters is null", K(ret), K(params.at(1)));
  } else if (!is_sys_tenant(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("only sys tenant can drop snapshot range", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "sys tenant");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
  } else {
    obrpc::ObWrRpcProxy wr_proxy;
    uint64_t tenant_id = OB_SYS_TENANT_ID;
    int64_t low_snap_id = params.at(0).get_int();
    int64_t high_snap_id = params.at(1).get_int();
    int64_t cluster_id = GCONF.cluster_id;
    int64_t task_timeout_ts =
        ObTimeUtility::current_time() + WorkloadRepositoryTask::WR_USER_DEL_TASK_TIMEOUT;
    ObArray<uint64_t> all_tenant_ids;

    ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is nullptr", K(ret));
    } else if (OB_FAIL(schema_service->get_tenant_ids(all_tenant_ids))) {
      LOG_WARN("failed to get all tenant_ids", KR(ret));
    } else if (OB_FAIL(wr_proxy.init(GCTX.net_frame_->get_req_transport()))) {
      LOG_WARN("failed to init wr proxy", K(ret));
    } else {
      int save_ret = OB_SUCCESS;
      for (int i = 0; OB_SUCC(ret) && i < all_tenant_ids.size(); i++) {
        const uint64_t tenant_id = all_tenant_ids.at(i);
        if (OB_UNLIKELY(task_timeout_ts < ObTimeUtility::current_time())) {
          ret = OB_TIMEOUT;
          LOG_WARN("wr purge timeout", KR(ret), K(task_timeout_ts));
        } else if (OB_FAIL(share::WorkloadRepositoryTask::do_delete_single_tenant_snapshot(
                       tenant_id, cluster_id, low_snap_id, high_snap_id, task_timeout_ts,
                       WorkloadRepositoryTask::WR_USER_DEL_TASK_TIMEOUT, wr_proxy))) {
          LOG_WARN("failed to do delete single tenant snapshot", K(ret), K(tenant_id),
              K(cluster_id), K(task_timeout_ts), K(low_snap_id), K(high_snap_id));
          save_ret = ret;
          ret = OB_SUCCESS;
        }
      }  // end for
      ret = COVER_SUCC(save_ret);
    }
    // After the asynchronous task has been submitted, check whether all tasks have been executed
    if (OB_SUCC(ret)) {
      static const int64_t SLEEP_INTERVAL_US = 1 * 1000L * 1000L;  // 1s
      bool is_all_finished = false;
      // wait until snapshot task finished or timeout.
      while (OB_SUCC(ret) && !is_all_finished) {
        if (OB_UNLIKELY(common::ObTimeUtility::current_time() >= task_timeout_ts)) {
          ret = OB_TIMEOUT;
          LOG_WARN("wr snapshot task already timeout", K(ret));
        } else if (check_drop_task_success_for_snap_id_range(low_snap_id, high_snap_id, is_all_finished)) {
          LOG_WARN("failed to check drop task state ", K(low_snap_id), K(high_snap_id), K(is_all_finished));
        } else if (!is_all_finished) {
          ob_usleep(SLEEP_INTERVAL_US);
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::check_drop_task_success_for_snap_id_range(
      const int64_t low_snap_id, const int64_t high_snap_id, bool &is_all_success)
{
  int ret = OB_SUCCESS;
  is_all_success = true;
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS CNT FROM %s where "
                                  "cluster_id=%ld and snap_id between %ld and %ld and (status=%ld or status=%ld)",
              OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME, cluster_id, low_snap_id, high_snap_id, ObWrSnapshotStatus::DELETED, ObWrSnapshotStatus::SUCCESS))) {
        LOG_WARN("failed to format sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to fetch snapshot info", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to read __wr_snapshot records",K(low_snap_id), K(high_snap_id), K(cluster_id));
      } else {
        int64_t cnt = -1;
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
        if (OB_SUCC(ret)) {
          if (cnt >= 1) {
            is_all_success = false;
          } else if (OB_UNLIKELY(cnt < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count of record lower than zero, unexcepted!", K(low_snap_id), K(high_snap_id), K(cluster_id),
                K(cnt));
          }
        }
        OB_ASSERT(result->next() == OB_ITER_END);
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::modify_snapshot_settings(
    ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  obrpc::ObWrRpcProxy wr_proxy;
  int64_t cluster_id = GCONF.cluster_id;
  uint64_t data_version = OB_INVALID_VERSION;

  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (!is_sys_tenant(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("only sys tenant can create snapshot", K(ret));
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "sys tenant");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
  } else if (OB_UNLIKELY(3 != params.count())) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("parameters number is wrong", K(ret), K(params.count()));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(
                 cluster_id, OB_SYS_TENANT_ID, share::SYS_LS, false /*force_renew*/, leader))) {
    LOG_WARN("fail to get ls locaiton leader", KR(ret), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(wr_proxy.init(GCTX.net_frame_->get_req_transport()))) {
    LOG_WARN("failed to init wr proxy", K(ret));
  } else {
    int64_t retention = 0;
    if (OB_SUCC(ret)) {
      if (params.at(0).is_null()) {
        retention = -1;  // null
      } else {
        retention = params.at(0).get_int();
        if (0 == retention) {
          retention = 110 * 365 * 24 * 60L;  // 110 years
        } else if (retention < 1440 || retention > 100 * 365 * 24 * 60L) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid value range, retention needs to be between 1440 and 52560000.", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              "Invalid value range, retention needs to be between 1440 and 52560000.");
        }
      }
    }
    int64_t interval = 0;
    if (OB_SUCC(ret)) {
      if (params.at(1).is_null()) {
        interval = -1;  // null
      } else {
        interval = params.at(1).get_int();
        if (0 == interval) {
          interval = 110 * 365 * 24 * 60L;  // 110 years
        } else if (interval < 10 || interval > 1 * 365 * 24 * 60L) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid value range, interval needs to be between 10 and 525600.", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              "Invalid value range, interval needs to be between 10 and 525600.");
        }
      }
    }

    int64_t topnsql = 0;
    if (OB_SUCC(ret)) {
      if (params.at(2).is_null()) {
        topnsql = -1;  // null
      } else {
        topnsql = params.at(2).get_int();
        if (topnsql < 30 || topnsql > 50000) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid value range, topnsql needs to be between 30 and 50000.", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              "Invalid value range, topnsql needs to be between 30 and 50000.");
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObWrUserModifySettingsArg wr_user_modify_settings_arg(
          ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), retention, interval, topnsql);
      if (OB_FAIL(wr_proxy.to(leader)
                      .by(OB_SYS_TENANT_ID)
                      .group_id(share::OBCG_WR)
                      .timeout(WR_USER_CREATE_SNAP_RPC_TIMEOUT)
                      .wr_sync_user_modify_settings_task(wr_user_modify_settings_arg))) {
        LOG_WARN("failed to send sync modify settings task", KR(ret), K(OB_SYS_TENANT_ID));
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::generate_ash_report_text(
    ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  AshReportParams ash_report_params(ctx.exec_ctx_->get_my_session()->get_timezone_info());
  uint64_t data_version = OB_INVALID_VERSION;
  ObStringBuffer buff(&ctx.exec_ctx_->get_allocator());
  if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
  } else if (OB_FAIL(process_ash_report_params(data_version, params, ash_report_params))) {
    LOG_WARN("failed to process ash report params", K(ret), K(data_version), K(params));
  } else {
    ash_report_params.cur_tenant_id = ctx.exec_ctx_->get_my_session()->get_effective_tenant_id();
    if (OB_FAIL(print_ash_report_header(ash_report_params, buff))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }

    // calc ASH_BEGIN_TIME and ASH_END_TIME
    // FIXME:The ash records on different nodes are different.
    //The start and end times queried here are the earliest and latest start and end times on all nodes,
    //which is equivalent to extending the time of ash records on some nodes
    int64_t ash_begin_time = 0;
    int64_t ash_end_time = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_ash_begin_and_end_time(ash_report_params))) {
        LOG_WARN("failed to get ash begin time and end time", K(ret));
      }
    }
    if (OB_SUCC(ret)) {

      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
      } else if ( data_version >= DATA_VERSION_4_3_5_2 &&  OB_FAIL(get_wr_begin_and_end_time(ash_report_params))) {
        LOG_WARN("failed to get wr begin time and end time", K(ret));
      }
    }
    // calc num_samples
    int64_t num_samples = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_ash_num_samples(ash_report_params, num_samples))) {
        LOG_WARN("failed to get num_samples", K(ret));
      }
    }
    int64_t wr_num_samples = 0;
    if (OB_SUCC(ret)) {
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
      } else if (data_version >= DATA_VERSION_4_3_5_2 &&  OB_FAIL(get_wr_num_samples(ash_report_params, wr_num_samples))) {
        LOG_WARN("failed to get wr_num_samples", K(ret));
      }
    }

    // print ash summary info
    bool no_data = false;  // if no data, will just print ash summary info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_ash_summary_info(ash_report_params, buff, no_data))) {
        LOG_WARN("failed to print ash summary info", K(ret));
      }
    }
    num_samples = num_samples + 10 * wr_num_samples;
    // print other infos
    if (OB_SUCC(ret) && !no_data) {
      if (OB_FAIL(print_ash_top_active_tenants(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top active tenants", K(ret));
      } else if (OB_FAIL(print_ash_top_node_load(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_group(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_foreground_db_time(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_background_db_time(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_sessions(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_io_bandwidth(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top io bandwidth", K(ret));
      } else if (OB_FAIL(print_top_blocking_session(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top blocking session", K(ret));
      } else if (OB_FAIL(print_ash_top_latches(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_db_object(ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top db object", K(ret));
      } else if (OB_FAIL(print_ash_activity_over_time(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_execution_phase(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_io_event(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top io time", K(ret));
      } else if (OB_FAIL(print_top_sql_command_type(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_with_top_wait_events(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_with_top_operator(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_plsql(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_text(
                     ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_report_end(ash_report_params, buff))) {
        LOG_WARN("failed to ash report end", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObTextStringResult text_res(lib::is_oracle_mode()? ObLongTextType : ObTextType, true, &ctx.exec_ctx_->get_allocator());
      if (FALSE_IT(
              result.set_collation_type(ctx.exec_ctx_->get_my_session()->get_nls_collation()))) {
      } else if (OB_FAIL(text_res.init(buff.length()))) {
        LOG_WARN("Failed to init text res", K(ret), K(buff.length()));
      } else if (OB_FAIL(text_res.append(buff.string()))) {
        LOG_WARN("Failed to append str to text res", K(ret), K(text_res), K(buff));
      } else {
        ObString lob_str;
        text_res.get_result_buffer(lob_str);
        OX(result.set_lob_value(lib::is_oracle_mode()? ObLongTextType : ObTextType, lob_str.ptr(), lob_str.length()));
        OX(result.set_has_lob_header());
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::lpad(
    const char *src, const int64_t size, const char *pad, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (size <= strlen(src)) {
    if (OB_FAIL(buff.append(src))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < (size - strlen(src)); i++) {
      if (OB_FAIL(buff.append(pad))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buff.append(src))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::format_row(const AshReportParams &ash_report_params,
                                         const AshRowItem &ash_row,
                                         const char *pad,
                                         const char *sep,
                                         ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buff.append(sep))) {
    LOG_WARN("failed to append string into buff", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ash_row.column_size_; i++) {
    const AshColumnItem &column = ash_row.columns_[i];
    if (OB_FAIL(lpad(column.column_content_, column.column_width_, pad, buff))) {
      LOG_WARN("failed to calc lpad ", K(i), K(ret));
    } else if (OB_FAIL(buff.append(sep))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else if (column.href_sql_id_) {
      SqlIdArray sql_id(column.column_content_);
      if (OB_FAIL(add_var_to_array_no_dup(ash_report_params.top_sql_ids_, sql_id))) {
        LOG_WARN("append sql id array failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(buff.append("\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::get_ash_bound_sql_time(const AshReportParams &ash_report_params,
                                                     char *start_time_buf, int64_t start_buf_len,
                                                     char *end_time_buf, int64_t end_buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t time_buf_len = 128;
  int64_t time_buf_pos = 0;
  int64_t ash_begin_time = ash_report_params.wr_begin_time > 0 ? ash_report_params.wr_begin_time : ash_report_params.ash_begin_time;
  int64_t ash_end_time = ash_report_params.ash_end_time > 0 ? ash_report_params.ash_end_time : ash_report_params.wr_end_time;
  char ash_begin_time_buf[time_buf_len] = "";
  char ash_end_time_buf[time_buf_len] = "";
  if (OB_FAIL(usec_to_string(ash_report_params.tz_info,
                             ash_begin_time,
                             ash_begin_time_buf,
                             time_buf_len,
                             time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(time_buf_pos = 0)) {
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info,
                                    ash_end_time,
                                    ash_end_time_buf,
                                    time_buf_len,
                                    time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (is_oracle_mode()) {
    snprintf(start_time_buf, start_buf_len, "TO_DATE('%s')", ash_begin_time_buf);
    snprintf(end_time_buf, end_buf_len, "TO_DATE('%s')", ash_end_time_buf);
  } else {
    snprintf(start_time_buf, start_buf_len, "TIMESTAMP'%s'", ash_begin_time_buf);
    snprintf(end_time_buf, end_buf_len, "TIMESTAMP'%s'", ash_end_time_buf);
  }
  return ret;
}

int ObDbmsWorkloadRepository::usec_to_string(const common::ObTimeZoneInfo *tz_info,
    const int64_t usec, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTime time;
  if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(usec, tz_info, time))) {
    LOG_WARN("failed to usec to ob time", K(ret), K(usec));
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(time,
                 lib::is_oracle_mode() ? DT_TYPE_ORACLE_TIMESTAMP : DT_TYPE_DATETIME, 0 /*scale*/,
                 buf, buf_len, pos, true /*with_delim*/))) {
    LOG_WARN("fail to change time to string", K(ret), K(time), K(pos));
  }
  return ret;
}

const char *mysql_table = " FROM oceanbase.GV$ACTIVE_SESSION_HISTORY ASH";
const char *wr_mysql_sys_table = " FROM oceanbase.CDB_WR_ACTIVE_SESSION_HISTORY WR LEFT JOIN oceanbase.CDB_WR_EVENT_NAME WR_EVENT_NAME ON WR.EVENT_ID = WR_EVENT_NAME.EVENT_ID AND WR.TENANT_ID = WR_EVENT_NAME.TENANT_ID";
const char *wr_mysql_tenant_table = " FROM oceanbase.DBA_WR_ACTIVE_SESSION_HISTORY WR LEFT JOIN oceanbase.DBA_WR_EVENT_NAME WR_EVENT_NAME ON WR.EVENT_ID = WR_EVENT_NAME.EVENT_ID";
const char *wr_mysql_tenant_table_421 = " FROM oceanbase.DBA_WR_ACTIVE_SESSION_HISTORY WR";
const char *wr_mysql_sys_table_421 = " FROM oceanbase.CDB_WR_ACTIVE_SESSION_HISTORY WR";
const char *oracle_table = " FROM SYS.GV$ACTIVE_SESSION_HISTORY ASH";
const char *wr_oracle_table = " FROM SYS.DBA_WR_ACTIVE_SESSION_HISTORY WR LEFT JOIN SYS.DBA_WR_EVENT_NAME WR_EVENT_NAME ON WR.EVENT_ID = WR_EVENT_NAME.EVENT_ID";
const char *wr_oracle_table_421 = " FROM SYS.DBA_WR_ACTIVE_SESSION_HISTORY WR";

const char *ASH_VIEW_SQL_4352 =
 "SELECT"
 "  ASH.TIME_MODEL AS TIME_MODEL,"
 "  ASH.SVR_IP AS SVR_IP,"
 "  ASH.SVR_PORT AS SVR_PORT,"
 "  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
 "  ASH.CON_ID AS TENANT_ID,"
 "  ASH.USER_ID AS USER_ID,"
 "  ASH.SESSION_ID AS SESSION_ID,"
 "  ASH.SESSION_TYPE AS SESSION_TYPE,"
 "  ASH.SQL_ID AS SQL_ID,"
 "  ASH.PLAN_ID AS PLAN_ID,"
 "  ASH.TRACE_ID AS TRACE_ID,"
 // EVENT
 "  %s"
 "  ASH.EVENT_NO AS EVENT_NO,"
 "  ASH.EVENT_ID AS EVENT_ID,"
 "  ASH.P1 AS P1,"
 "  ASH.P1TEXT AS P1TEXT,"
 "  ASH.P2 AS P2,"
 "  ASH.P2TEXT AS P2TEXT,"
 "  ASH.P3 AS P3,"
 "  ASH.P3TEXT AS P3TEXT,"
 // WAIT_CLASS
 "  %s"
 "  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
 "  ASH.TIME_WAITED AS TIME_WAITED,"
 "  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
 "  ASH.GROUP_ID AS GROUP_ID,"
 "  ASH.PLAN_HASH AS PLAN_HASH,"
 "  ASH.THREAD_ID AS THREAD_ID,"
 "  ASH.STMT_TYPE AS STMT_TYPE,"
 "  ASH.PROGRAM AS PROGRAM,"
 "  ASH.MODULE AS MODULE,"
 "  ASH.ACTION AS ACTION,"
 "  ASH.CLIENT_ID AS CLIENT_ID,"
 "  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
 "  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
 "  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
 "  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
 "  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
 "  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
 "  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
 "  ASH.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,"
 "  ASH.TABLET_ID AS TABLET_ID,"
 "  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
 "  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
 "  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
 "  ASH.PROXY_SID AS PROXY_SID, "
 "  ASH.TX_ID AS TX_ID, "
 "  ASH.DELTA_READ_IO_REQUESTS AS DELTA_READ_IO_REQUESTS,"
 "  ASH.DELTA_READ_IO_BYTES AS DELTA_READ_IO_BYTES,"
 "  ASH.DELTA_WRITE_IO_REQUESTS AS DELTA_WRITE_IO_REQUESTS,"
 "  ASH.DELTA_WRITE_IO_BYTES AS DELTA_WRITE_IO_BYTES,"
 "  1 AS COUNT_WEIGHT"
 // FROM which table
 " %s"
 " WHERE sample_time between '%.*s' and '%.*s'";
 
 const char *WR_VIEW_SQL_4352 =
 "SELECT"
 " WR.TIME_MODEL AS TIME_MODEL,"
 " WR.SVR_IP AS SVR_IP,"
 " WR.SVR_PORT AS SVR_PORT,"
 " WR.SAMPLE_TIME AS SAMPLE_TIME,"
 " WR.TENANT_ID AS TENANT_ID,"
 " WR.USER_ID AS USER_ID,"
 " WR.SESSION_ID AS SESSION_ID,"
 // SESSION_TYPE
 " %s"
 " WR.SQL_ID AS SQL_ID,"
 " WR.PLAN_ID AS PLAN_ID,"
 " WR.TRACE_ID AS TRACE_ID,"
 // EVENT
 " %s"
 " WR.EVENT_NO AS EVENT_NO,"
 " WR.EVENT_ID AS EVENT_ID,"
 " WR.P1 AS P1,"
 " WR_EVENT_NAME.PARAMETER1 AS P1TEXT,"
 " WR.P2 AS P2,"
 " WR_EVENT_NAME.PARAMETER2 AS P2TEXT,"
 " WR.P3 AS P3,"
 " WR_EVENT_NAME.PARAMETER3 AS P3TEXT,"
 // WAIT_CLASS
 " %s"
 " WR_EVENT_NAME.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
 " WR.TIME_WAITED AS TIME_WAITED,"
 " WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
 " WR.GROUP_ID AS GROUP_ID,"
 " WR.PLAN_HASH AS PLAN_HASH,"
 " WR.THREAD_ID AS THREAD_ID,"
 " WR.STMT_TYPE AS STMT_TYPE,"
 " WR.PROGRAM AS PROGRAM,"
 " WR.MODULE AS MODULE,"
 " WR.ACTION AS ACTION,"
 " WR.CLIENT_ID AS CLIENT_ID,"
 " WR.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
 " WR.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
 " WR.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
 " WR.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
 " WR.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
 " WR.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
 " WR.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
 " WR.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,"
 " WR.TABLET_ID AS TABLET_ID,"
 " WR.TM_DELTA_TIME AS TM_DELTA_TIME, "
 " WR.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
 " WR.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
 " WR.PROXY_SID AS PROXY_SID, "
 " WR.TX_ID AS TX_ID, "
 " WR.DELTA_READ_IO_REQUESTS AS DELTA_READ_IO_REQUESTS,"
 " WR.DELTA_READ_IO_BYTES AS DELTA_READ_IO_BYTES,"
 " WR.DELTA_WRITE_IO_REQUESTS AS DELTA_WRITE_IO_REQUESTS,"
 " WR.DELTA_WRITE_IO_BYTES AS DELTA_WRITE_IO_BYTES,"
 " 10 AS COUNT_WEIGHT"
 // FROM which table
 " %s"
 " WHERE sample_time between '%.*s' and '%.*s'";

 const char *ASH_VIEW_SQL_4350 = 
 "SELECT"
 "  ASH.SVR_IP AS SVR_IP,"
 "  ASH.SVR_PORT AS SVR_PORT,"
 "  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
 "  ASH.CON_ID AS TENANT_ID,"
 "  ASH.USER_ID AS USER_ID,"
 "  ASH.SESSION_ID AS SESSION_ID,"
 "  ASH.SESSION_TYPE AS SESSION_TYPE,"
 "  ASH.SQL_ID AS SQL_ID,"
 "  ASH.PLAN_ID AS PLAN_ID,"
 "  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
 "  %s"
 "  ASH.EVENT_NO AS EVENT_NO,"
 "  ASH.EVENT_ID AS EVENT_ID,"
 "  ASH.P1 AS P1,"
 "  ASH.P1TEXT AS P1TEXT,"
 "  ASH.P2 AS P2,"
 "  ASH.P2TEXT AS P2TEXT,"
 "  ASH.P3 AS P3,"
 "  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
 "  %s"
 "  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
 "  ASH.TIME_WAITED AS TIME_WAITED,"
 "  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
 "  ASH.GROUP_ID AS GROUP_ID,"
 "  ASH.PLAN_HASH AS PLAN_HASH,"
 "  ASH.THREAD_ID AS THREAD_ID,"
 "  ASH.STMT_TYPE AS STMT_TYPE,"
 "  ASH.PROGRAM AS PROGRAM,"
 "  ASH.MODULE AS MODULE,"
 "  ASH.ACTION AS ACTION,"
 "  ASH.CLIENT_ID AS CLIENT_ID,"
 "  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
 "  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
 "  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
 "  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
 "  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
 "  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
 "  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
 "  ASH.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,"
 "  ASH.TABLET_ID AS TABLET_ID,"
 "  ASH.TIME_MODEL AS TIME_MODEL,"
 "  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
 "  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
 "  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
 "  ASH.PROXY_SID AS PROXY_SID, "
 "  NULL AS TX_ID, "
 "  NULL AS DELTA_READ_IO_REQUESTS,"
 "  NULL AS DELTA_READ_IO_BYTES,"
 "  NULL AS DELTA_WRITE_IO_REQUESTS,"
 "  NULL AS DELTA_WRITE_IO_BYTES,"
 "  1 AS COUNT_WEIGHT "
 // FROM which table
 " %s "
 " WHERE sample_time between '%.*s' and '%.*s'";

 
const char *ASH_VIEW_SQL_425 =
"SELECT"
"  ASH.TIME_MODEL AS TIME_MODEL,"
"  ASH.SVR_IP AS SVR_IP,"
"  ASH.SVR_PORT AS SVR_PORT,"
"  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
"  ASH.CON_ID AS TENANT_ID,"
"  ASH.USER_ID AS USER_ID,"
"  ASH.SESSION_ID AS SESSION_ID,"
"  ASH.SESSION_TYPE AS SESSION_TYPE,"
"  ASH.SQL_ID AS SQL_ID,"
"  ASH.PLAN_ID AS PLAN_ID,"
"  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
"  %s"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
"  %s"
"  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
"  ASH.TIME_WAITED AS TIME_WAITED,"
"  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
"  ASH.GROUP_ID AS GROUP_ID,"
"  ASH.PLAN_HASH AS PLAN_HASH,"
"  ASH.THREAD_ID AS THREAD_ID,"
"  ASH.STMT_TYPE AS STMT_TYPE,"
"  ASH.PROGRAM AS PROGRAM,"
"  ASH.MODULE AS MODULE,"
"  ASH.ACTION AS ACTION,"
"  ASH.CLIENT_ID AS CLIENT_ID,"
"  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
"  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
"  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
"  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
"  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
"  ASH.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,"
"  ASH.TABLET_ID AS TABLET_ID,"
"  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
"  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
"  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
"  ASH.PROXY_SID AS PROXY_SID, "
"  ASH.TX_ID AS TX_ID, "
"  ASH.DELTA_READ_IO_REQUESTS AS DELTA_READ_IO_REQUESTS,"
"  ASH.DELTA_READ_IO_BYTES AS DELTA_READ_IO_BYTES,"
"  ASH.DELTA_WRITE_IO_REQUESTS AS DELTA_WRITE_IO_REQUESTS,"
"  ASH.DELTA_WRITE_IO_BYTES AS DELTA_WRITE_IO_BYTES,"
"  1 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *WR_VIEW_SQL_425 =
"SELECT"
" WR.TIME_MODEL AS TIME_MODEL,"
" WR.SVR_IP AS SVR_IP,"
" WR.SVR_PORT AS SVR_PORT,"
" WR.SAMPLE_TIME AS SAMPLE_TIME,"
" WR.TENANT_ID AS TENANT_ID,"
" WR.USER_ID AS USER_ID,"
" WR.SESSION_ID AS SESSION_ID,"
// SESSION_TYPE
" %s"
" WR.SQL_ID AS SQL_ID,"
" WR.PLAN_ID AS PLAN_ID,"
" WR.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
" WR.EVENT_NO AS EVENT_NO,"
" WR.EVENT_ID AS EVENT_ID,"
" WR.P1 AS P1,"
" WR_EVENT_NAME.PARAMETER1 AS P1TEXT,"
" WR.P2 AS P2,"
" WR_EVENT_NAME.PARAMETER2 AS P2TEXT,"
" WR.P3 AS P3,"
" WR_EVENT_NAME.PARAMETER3 AS P3TEXT,"
// WAIT_CLASS
" %s"
" WR_EVENT_NAME.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
" WR.TIME_WAITED AS TIME_WAITED,"
" WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
" WR.GROUP_ID AS GROUP_ID,"
" WR.PLAN_HASH AS PLAN_HASH,"
" WR.THREAD_ID AS THREAD_ID,"
" WR.STMT_TYPE AS STMT_TYPE,"
" WR.PROGRAM AS PROGRAM,"
" WR.MODULE AS MODULE,"
" WR.ACTION AS ACTION,"
" WR.CLIENT_ID AS CLIENT_ID,"
" WR.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
" WR.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
" WR.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
" WR.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
" WR.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
" WR.IN_SQL_EXECUTION AS IN_SQL_EXECUTION, "
" WR.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION, "
" WR.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION, "
" WR.BLOCKING_SESSION_ID AS BLOCKING_SESSION_ID,"
" WR.TABLET_ID AS TABLET_ID,"
" WR.TM_DELTA_TIME AS TM_DELTA_TIME, "
" WR.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
" WR.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
" WR.PROXY_SID AS PROXY_SID, "
" WR.TX_ID AS TX_ID, "
" WR.DELTA_READ_IO_REQUESTS AS DELTA_READ_IO_REQUESTS,"
" WR.DELTA_READ_IO_BYTES AS DELTA_READ_IO_BYTES,"
" WR.DELTA_WRITE_IO_REQUESTS AS DELTA_WRITE_IO_REQUESTS,"
" WR.DELTA_WRITE_IO_BYTES AS DELTA_WRITE_IO_BYTES,"
" 10 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *ASH_VIEW_SQL_424 =
"SELECT"
"  ASH.TIME_MODEL AS TIME_MODEL,"
"  ASH.SVR_IP AS SVR_IP,"
"  ASH.SVR_PORT AS SVR_PORT,"
"  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
"  ASH.CON_ID AS TENANT_ID,"
"  ASH.USER_ID AS USER_ID,"
"  ASH.SESSION_ID AS SESSION_ID,"
"  ASH.SESSION_TYPE AS SESSION_TYPE,"
"  ASH.SQL_ID AS SQL_ID,"
"  ASH.PLAN_ID AS PLAN_ID,"
"  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
"  %s"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
" %s"
"  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
"  ASH.TIME_WAITED AS TIME_WAITED,"
"  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
"  ASH.GROUP_ID AS GROUP_ID,"
"  ASH.PLAN_HASH AS PLAN_HASH,"
"  ASH.THREAD_ID AS THREAD_ID,"
"  ASH.STMT_TYPE AS STMT_TYPE,"
"  ASH.PROGRAM AS PROGRAM,"
"  ASH.MODULE AS MODULE,"
"  ASH.ACTION AS ACTION,"
"  ASH.CLIENT_ID AS CLIENT_ID,"
"  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
"  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
"  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
"  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
"  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
"  ASH.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
"  ASH.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
"  ASH.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
"  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
"  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
"  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
"  NULL AS PROXY_SID, "
"  NULL AS TX_ID, "
"  NULL AS DELTA_READ_IO_REQUESTS,"
"  NULL AS DELTA_READ_IO_BYTES,"
"  NULL AS DELTA_WRITE_IO_REQUESTS,"
"  NULL AS DELTA_WRITE_IO_BYTES,"
"  1 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *WR_VIEW_SQL_424 =
"SELECT"
" WR.TIME_MODEL AS TIME_MODEL,"
" WR.SVR_IP AS SVR_IP,"
" WR.SVR_PORT AS SVR_PORT,"
" WR.SAMPLE_TIME AS SAMPLE_TIME,"
" WR.TENANT_ID AS TENANT_ID,"
" WR.USER_ID AS USER_ID,"
" WR.SESSION_ID AS SESSION_ID,"
// SESSION_TYPE
" %s"
" WR.SQL_ID AS SQL_ID,"
" WR.PLAN_ID AS PLAN_ID,"
" WR.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
" WR.EVENT_NO AS EVENT_NO,"
" WR.EVENT_ID AS EVENT_ID,"
" WR.P1 AS P1,"
" WR_EVENT_NAME.PARAMETER1 AS P1TEXT,"
" WR.P2 AS P2,"
" WR_EVENT_NAME.PARAMETER2 AS P2TEXT,"
" WR.P3 AS P3,"
" WR_EVENT_NAME.PARAMETER3 AS P3TEXT,"
// WAIT_CLASS
" %s"
" WR_EVENT_NAME.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
" WR.TIME_WAITED AS TIME_WAITED,"
" WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
" WR.GROUP_ID AS GROUP_ID,"
" WR.PLAN_HASH AS PLAN_HASH,"
" WR.THREAD_ID AS THREAD_ID,"
" WR.STMT_TYPE AS STMT_TYPE,"
" WR.PROGRAM AS PROGRAM,"
" WR.MODULE AS MODULE,"
" WR.ACTION AS ACTION,"
" WR.CLIENT_ID AS CLIENT_ID,"
" WR.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
" WR.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
" WR.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
" WR.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
" WR.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
" WR.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
" WR.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
" WR.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
" WR.TM_DELTA_TIME AS TM_DELTA_TIME, "
" WR.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
" WR.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
" NULL AS PROXY_SID, "
" NULL AS TX_ID, "
" NULL AS DELTA_READ_IO_REQUESTS,"
" NULL AS DELTA_READ_IO_BYTES,"
" NULL AS DELTA_WRITE_IO_REQUESTS,"
" NULL AS DELTA_WRITE_IO_BYTES,"
" 10 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *ASH_VIEW_SQL_423 =
"SELECT"
"  ASH.SVR_IP AS SVR_IP,"
"  ASH.SVR_PORT AS SVR_PORT,"
"  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
"  ASH.CON_ID AS TENANT_ID,"
"  ASH.USER_ID AS USER_ID,"
"  ASH.SESSION_ID AS SESSION_ID,"
"  ASH.SESSION_TYPE AS SESSION_TYPE,"
"  ASH.SQL_ID AS SQL_ID,"
"  ASH.PLAN_ID AS PLAN_ID,"
"  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
" %s"
"  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
"  ASH.TIME_WAITED AS TIME_WAITED,"
"  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
"  ASH.GROUP_ID AS GROUP_ID,"
"  NULL AS PLAN_HASH,"
"  NULL AS THREAD_ID,"
"  NULL AS STMT_TYPE,"
"  ASH.PROGRAM AS PROGRAM,"
"  ASH.MODULE AS MODULE,"
"  ASH.ACTION AS ACTION,"
"  ASH.CLIENT_ID AS CLIENT_ID,"
"  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
"  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
"  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
"  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
"  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
"  ASH.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
"  ASH.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
"  ASH.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
"  0 AS TIME_MODEL,"
"  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
"  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
"  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
"  NULL AS PROXY_SID, "
"  NULL AS TX_ID, "
"  NULL AS DELTA_READ_IO_REQUESTS,"
"  NULL AS DELTA_READ_IO_BYTES,"
"  NULL AS DELTA_WRITE_IO_REQUESTS,"
"  NULL AS DELTA_WRITE_IO_BYTES,"
"  1 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *WR_VIEW_SQL_423 =
"SELECT"
" WR.SVR_IP AS SVR_IP,"
" WR.SVR_PORT AS SVR_PORT,"
" WR.SAMPLE_TIME AS SAMPLE_TIME,"
" WR.TENANT_ID AS TENANT_ID,"
" WR.USER_ID AS USER_ID,"
" WR.SESSION_ID AS SESSION_ID,"
// SESSION_TYPE
" %s"
" WR.SQL_ID AS SQL_ID,"
" WR.PLAN_ID AS PLAN_ID,"
" WR.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
" WR.EVENT_NO AS EVENT_NO,"
" WR.EVENT_ID AS EVENT_ID,"
" WR.P1 AS P1,"
" WR_EVENT_NAME.PARAMETER1 AS P1TEXT,"
" WR.P2 AS P2,"
" WR_EVENT_NAME.PARAMETER2 AS P2TEXT,"
" WR.P3 AS P3,"
" WR_EVENT_NAME.PARAMETER3 AS P3TEXT,"
// WAIT_CLASS
" %s"
" WR_EVENT_NAME.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
" WR.TIME_WAITED AS TIME_WAITED,"
" WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
" WR.GROUP_ID AS GROUP_ID,"
" NULL AS PLAN_HASH,"
" NULL AS THREAD_ID,"
" NULL AS STMT_TYPE,"
" WR.PROGRAM AS PROGRAM,"
" WR.MODULE AS MODULE,"
" WR.ACTION AS ACTION,"
" WR.CLIENT_ID AS CLIENT_ID,"
" WR.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
" WR.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
" WR.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
" WR.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
" WR.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
" WR.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
" WR.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
" WR.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
" 0 AS TIME_MODEL,"
" WR.TM_DELTA_TIME AS TM_DELTA_TIME, "
" WR.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
" WR.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
" NULL AS PROXY_SID, "
" NULL AS TX_ID, "
" NULL AS DELTA_READ_IO_REQUESTS,"
" NULL AS DELTA_READ_IO_BYTES,"
" NULL AS DELTA_WRITE_IO_REQUESTS,"
" NULL AS DELTA_WRITE_IO_BYTES,"
" 10 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *ASH_VIEW_SQL_422 =
"SELECT"
"  ASH.SVR_IP AS SVR_IP,"
"  ASH.SVR_PORT AS SVR_PORT,"
"  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
"  ASH.CON_ID AS TENANT_ID,"
"  ASH.USER_ID AS USER_ID,"
"  ASH.SESSION_ID AS SESSION_ID,"
"  ASH.SESSION_TYPE AS SESSION_TYPE,"
"  ASH.SQL_ID AS SQL_ID,"
"  ASH.PLAN_ID AS PLAN_ID,"
"  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
" %s"
"  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
"  ASH.TIME_WAITED AS TIME_WAITED,"
"  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
"  NULL AS GROUP_ID,"
"  NULL AS PLAN_HASH,"
"  NULL AS THREAD_ID,"
"  NULL AS STMT_TYPE,"
"  ASH.PROGRAM AS PROGRAM,"
"  ASH.MODULE AS MODULE,"
"  ASH.ACTION AS ACTION,"
"  ASH.CLIENT_ID AS CLIENT_ID,"
"  ASH.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
"  ASH.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
"  ASH.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
"  ASH.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
"  ASH.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
"  ASH.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
"  ASH.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
"  ASH.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
"  ASH.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
"  0 AS TIME_MODEL,"
"  ASH.TM_DELTA_TIME AS TM_DELTA_TIME, "
"  ASH.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
"  ASH.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
"  NULL AS PROXY_SID, "
"  NULL AS TX_ID, "
"  NULL AS DELTA_READ_IO_REQUESTS,"
"  NULL AS DELTA_READ_IO_BYTES,"
"  NULL AS DELTA_WRITE_IO_REQUESTS,"
"  NULL AS DELTA_WRITE_IO_BYTES,"
"  1 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *WR_VIEW_SQL_422 =
"SELECT"
" WR.SVR_IP AS SVR_IP,"
" WR.SVR_PORT AS SVR_PORT,"
" WR.SAMPLE_TIME AS SAMPLE_TIME,"
" WR.TENANT_ID AS TENANT_ID,"
" WR.USER_ID AS USER_ID,"
" WR.SESSION_ID AS SESSION_ID,"
// SESSION_TYPE
" %s"
" WR.SQL_ID AS SQL_ID,"
" WR.PLAN_ID AS PLAN_ID,"
" WR.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
" WR.EVENT_NO AS EVENT_NO,"
" WR.EVENT_ID AS EVENT_ID,"
" WR.P1 AS P1,"
" WR_EVENT_NAME.PARAMETER1 AS P1TEXT,"
" WR.P2 AS P2,"
" WR_EVENT_NAME.PARAMETER2 AS P2TEXT,"
" WR.P3 AS P3,"
" WR_EVENT_NAME.PARAMETER3 AS P3TEXT,"
// WAIT_CLASS
" %s"
" WR_EVENT_NAME.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
" WR.TIME_WAITED AS TIME_WAITED,"
" WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
" NULL AS GROUP_ID,"
" NULL AS PLAN_HASH,"
" NULL AS THREAD_ID,"
" NULL AS STMT_TYPE,"
" WR.PROGRAM AS PROGRAM,"
" WR.MODULE AS MODULE,"
" WR.ACTION AS ACTION,"
" WR.CLIENT_ID AS CLIENT_ID,"
" WR.TOP_LEVEL_SQL_ID AS TOP_LEVEL_SQL_ID,"
" WR.PLSQL_ENTRY_OBJECT_ID AS PLSQL_ENTRY_OBJECT_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_ID AS PLSQL_ENTRY_SUBPROGRAM_ID,"
" WR.PLSQL_ENTRY_SUBPROGRAM_NAME AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
" WR.PLSQL_OBJECT_ID AS PLSQL_OBJECT_ID,"
" WR.PLSQL_SUBPROGRAM_ID AS PLSQL_SUBPROGRAM_ID,"
" WR.PLSQL_SUBPROGRAM_NAME AS PLSQL_SUBPROGRAM_NAME,"
" WR.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
" WR.IN_PLSQL_COMPILATION AS IN_PLSQL_COMPILATION,"
" WR.IN_PLSQL_EXECUTION AS IN_PLSQL_EXECUTION,"
" 0 AS TIME_MODEL,"
" WR.TM_DELTA_TIME AS TM_DELTA_TIME, "
" WR.TM_DELTA_CPU_TIME AS TM_DELTA_CPU_TIME, "
" WR.TM_DELTA_DB_TIME AS TM_DELTA_DB_TIME, "
" NULL AS PROXY_SID,"
" NULL AS TX_ID, "
" NULL AS DELTA_READ_IO_REQUESTS,"
" NULL AS DELTA_READ_IO_BYTES,"
" NULL AS DELTA_WRITE_IO_REQUESTS,"
" NULL AS DELTA_WRITE_IO_BYTES,"
" 10 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *ASH_VIEW_SQL_421 =
"SELECT"
"  ASH.SVR_IP AS SVR_IP,"
"  ASH.SVR_PORT AS SVR_PORT,"
"  ASH.SAMPLE_TIME AS SAMPLE_TIME,"
"  ASH.CON_ID AS TENANT_ID,"
"  ASH.USER_ID AS USER_ID,"
"  ASH.SESSION_ID AS SESSION_ID,"
"  ASH.SESSION_TYPE AS SESSION_TYPE,"
"  ASH.SQL_ID AS SQL_ID,"
"  ASH.PLAN_ID AS PLAN_ID,"
"  ASH.TRACE_ID AS TRACE_ID,"
// EVENT
" %s"
"  ASH.EVENT_NO AS EVENT_NO,"
"  0 AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
// WAIT_CLASS
" %s"
"  ASH.WAIT_CLASS_ID AS WAIT_CLASS_ID,"
"  ASH.TIME_WAITED AS TIME_WAITED,"
"  ASH.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
"  NULL AS GROUP_ID,"
"  NULL AS PLAN_HASH,"
"  NULL AS THREAD_ID,"
"  NULL AS STMT_TYPE,"
"  '' AS PROGRAM,"
"  ASH.MODULE AS MODULE,"
"  ASH.ACTION AS ACTION,"
"  ASH.CLIENT_ID AS CLIENT_ID,"
"  NULL AS TOP_LEVEL_SQL_ID,"
"  NULL AS PLSQL_ENTRY_OBJECT_ID,"
"  NULL AS PLSQL_ENTRY_SUBPROGRAM_ID,"
"  NULL AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
"  NULL AS PLSQL_OBJECT_ID,"
"  NULL AS PLSQL_SUBPROGRAM_ID,"
"  NULL AS PLSQL_SUBPROGRAM_NAME,"
"  ASH.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
"  NULL AS IN_PLSQL_COMPILATION,"
"  NULL AS IN_PLSQL_EXECUTION,"
"  0 AS TIME_MODEL,"
"  NULL AS TM_DELTA_TIME, "
"  NULL AS TM_DELTA_CPU_TIME, "
"  NULL AS TM_DELTA_DB_TIME, "
"  NULL AS PROXY_SID, "
"  NULL AS TX_ID, "
"  NULL AS DELTA_READ_IO_REQUESTS,"
"  NULL AS DELTA_READ_IO_BYTES,"
"  NULL AS DELTA_WRITE_IO_REQUESTS,"
"  NULL AS DELTA_WRITE_IO_BYTES,"
"  1 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

const char *WR_VIEW_SQL_421 =
"SELECT"
" WR.SVR_IP AS SVR_IP,"
" WR.SVR_PORT AS SVR_PORT,"
" WR.SAMPLE_TIME AS SAMPLE_TIME,"
" WR.TENANT_ID AS TENANT_ID,"
" WR.USER_ID AS USER_ID,"
" WR.SESSION_ID AS SESSION_ID,"
// SESSION_TYPE
" %s"
" WR.SQL_ID AS SQL_ID,"
" WR.PLAN_ID AS PLAN_ID,"
" WR.TRACE_ID AS TRACE_ID,"
" NULL AS EVENT,"
" WR.EVENT_NO AS EVENT_NO,"
" 0 AS EVENT_ID,"
" WR.P1 AS P1,"
" NULL AS P1TEXT,"
" WR.P2 AS P2,"
" NULL AS P2TEXT,"
" WR.P3 AS P3,"
" NULL AS P3TEXT,"
" NULL AS WAIT_CLASS,"
" NULL AS WAIT_CLASS_ID,"
" WR.TIME_WAITED AS TIME_WAITED,"
" WR.SQL_PLAN_LINE_ID AS SQL_PLAN_LINE_ID,"
" NULL AS GROUP_ID,"
" NULL AS PLAN_HASH,"
" NULL AS THREAD_ID,"
" NULL AS STMT_TYPE,"
" '' AS PROGRAM,"
" WR.MODULE AS MODULE,"
" WR.ACTION AS ACTION,"
" WR.CLIENT_ID AS CLIENT_ID,"
" NULL AS TOP_LEVEL_SQL_ID,"
" NULL AS PLSQL_ENTRY_OBJECT_ID,"
" NULL AS PLSQL_ENTRY_SUBPROGRAM_ID,"
" NULL AS PLSQL_ENTRY_SUBPROGRAM_NAME,"
" NULL AS PLSQL_OBJECT_ID,"
" NULL AS PLSQL_SUBPROGRAM_ID,"
" NULL AS PLSQL_SUBPROGRAM_NAME,"
" WR.IN_SQL_EXECUTION AS IN_SQL_EXECUTION,"
" NULL AS IN_PLSQL_COMPILATION,"
" NULL AS IN_PLSQL_EXECUTION,"
" 0 AS TIME_MODEL,"
" NULL AS TM_DELTA_TIME, "
" NULL AS TM_DELTA_CPU_TIME, "
" NULL AS TM_DELTA_DB_TIME, "
" NULL AS PROXY_SID, "
" NULL AS TX_ID, "
" NULL AS DELTA_READ_IO_REQUESTS,"
" NULL AS DELTA_READ_IO_BYTES,"
" NULL AS DELTA_WRITE_IO_REQUESTS,"
" NULL AS DELTA_WRITE_IO_BYTES,"
" 10 AS COUNT_WEIGHT"
// FROM which table
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

#define EXTRACT_INT_FIELD_FOR_ASH(result, column_name, field, type)              \
  if (OB_SUCC(ret)) {                                                            \
    ObObjMeta _col_type;                                                         \
    if (OB_FAIL((result).get_type(column_name, _col_type))) {                    \
      LOG_WARN("get column type from result failed", K(ret), K(column_name));    \
    } else if (_col_type.is_number() || _col_type.is_decimal_int()) {            \
      EXTRACT_INT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field , type); \
    } else {                                                                     \
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);        \
    }                                                                            \
  }

#define EXTRACT_INT_FIELD_FOR_ASH_STR(result, column_name, field, type)         \
  type field = 0;                                                               \
  char field##_char[64] = "";                                                   \
  if (OB_SUCC(ret)) {                                                           \
    ObObjMeta col_type;                                                         \
    if (OB_FAIL((result).get_type(column_name, col_type))) {                    \
      LOG_WARN("get column type from result failed", K(ret), K(column_name));   \
    } else if (col_type.is_number() || col_type.is_decimal_int()) {             \
      EXTRACT_INT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field, type); \
    } else {                                                                    \
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);       \
    }                                                                           \
    sprintf(field##_char, "%ld", field);                                        \
  }

#define EXTRACT_UINT_FIELD_FOR_ASH(result, column_name, field, type)                                       \
  if (OB_SUCC(ret)) {                                                                                      \
    ObObjMeta _col_type;                                                                                   \
    if (OB_FAIL((result).get_type(column_name, _col_type))) {                                              \
      LOG_WARN("get column type from result failed", K(ret), K(column_name));                              \
    } else if (_col_type.is_unumber() || _col_type.is_number() || _col_type.is_decimal_int()) {            \
      EXTRACT_UINT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field , type);                          \
    } else {                                                                                               \
      EXTRACT_UINT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);                                 \
    }                                                                                                      \
  }

#define EXTRACT_UINT_FIELD_FOR_ASH_STR(result, column_name, field, type)                                  \
  type field = 0;                                                                                         \
  char field##_char[64] = "";                                                                             \
  if (OB_SUCC(ret)) {                                                                                     \
    ObObjMeta col_type;                                                                                   \
    if (OB_FAIL((result).get_type(column_name, col_type))) {                                              \
      LOG_WARN("get column type from result failed", K(ret), K(column_name));                             \
    } else if (col_type.is_unumber() || col_type.is_number() || col_type.is_decimal_int()) {              \
      EXTRACT_UINT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field, type);                          \
    } else {                                                                                              \
      EXTRACT_UINT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);                                \
    }                                                                                                     \
    sprintf(field##_char, "%lu", field);                                                                  \
  }

#define ASH_FIELD_CHAR(field) field##_char

int ObDbmsWorkloadRepository::append_fmt_ash_view_sql(
    const AshReportParams &ash_report_params, ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  const int64_t time_buf_len = 128;
  int64_t time_buf_pos = 0;
  char ash_begin_time_buf[time_buf_len];
  char ash_end_time_buf[time_buf_len];
  char port_buf[time_buf_len];
  uint64_t data_version = 0;
  const char* ash_view_ptr = nullptr;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else {
    if (data_version < DATA_VERSION_4_3_0_0) {
      // v4.2
      if (data_version >= MOCK_DATA_VERSION_4_2_5_0) {
        ash_view_ptr = ASH_VIEW_SQL_425;
      } else if (data_version >= MOCK_DATA_VERSION_4_2_4_0) {
        ash_view_ptr = ASH_VIEW_SQL_424;
      } else if (data_version == MOCK_DATA_VERSION_4_2_3_0) {
        ash_view_ptr = ASH_VIEW_SQL_423;
      } else if (data_version == DATA_VERSION_4_2_2_0) {
        ash_view_ptr = ASH_VIEW_SQL_422;
      } else {
        ash_view_ptr = ASH_VIEW_SQL_421;
      }
    } else {
      // v4.3
      if (data_version >= DATA_VERSION_4_3_5_2) {
        ash_view_ptr = ASH_VIEW_SQL_4352;
      } else if (data_version >= DATA_VERSION_4_3_5_0) {
        ash_view_ptr = ASH_VIEW_SQL_4350;
      } else {
        ash_view_ptr = ASH_VIEW_SQL_421;
      }
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ash_view_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ash view ptr is nullptr", K(ret), K(ash_view_ptr));
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_begin_time,
                 ash_begin_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(time_buf_pos = 0)) {
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_end_time,
                 ash_end_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(sprintf(port_buf, "%ld", ash_report_params.port))) {
  } else if (OB_FAIL(sql_string.append_fmt(ash_view_ptr,
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT,"
                                       : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT,",
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS,"
                                       : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS,",
                 lib::is_oracle_mode() ? oracle_table : mysql_table,
                 static_cast<int>(time_buf_pos),
                 ash_begin_time_buf,
                 static_cast<int>(time_buf_pos),
                 ash_end_time_buf))) {
    LOG_WARN("failed to assign query string", K(ret));
  } else {
    if (ash_report_params.svr_ip != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.SVR_IP = '%.*s'",
              ash_report_params.svr_ip.length(), ash_report_params.svr_ip.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.port != -1) {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.SVR_PORT = '%lu'", ash_report_params.port))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.sql_id != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.SQL_ID = '%.*s'",
              ash_report_params.sql_id.length(), ash_report_params.sql_id.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.trace_id != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.TRACE_ID = '%.*s'",
              ash_report_params.trace_id.length(), ash_report_params.trace_id.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.wait_class != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.WAIT_CLASS = '%.*s'",
              ash_report_params.wait_class.length(), ash_report_params.wait_class.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.tenant_id > 0) {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.CON_ID = '%lu'", ash_report_params.tenant_id))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::append_time_model_view_sql(ObSqlString &sql_string,
                                                         const char *select_lists,
                                                         const ObArrayWrap<const char*> &timemodel_columns,
                                                         const ObArrayWrap<int32_t> &timemodel_fields,
                                                         const char *source_table,
                                                         bool with_sum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_table) || strlen(source_table) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source table is nullptr", KR(ret), K(source_table));
  } else if (OB_UNLIKELY(timemodel_columns.count() != timemodel_fields.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timemodel arguements is invalid", KR(ret), K(timemodel_columns), K(timemodel_fields));
  } else if (OB_FAIL(sql_string.append("SELECT "))) {
    LOG_WARN("append sql string failed", KR(ret));
  } else if (select_lists != nullptr && OB_FAIL(sql_string.append(select_lists))) {
    LOG_WARN("append select lists string failed", KR(ret), K(select_lists));
  } else if (select_lists != nullptr && OB_FAIL(sql_string.append(", "))) {
    LOG_WARN("append sql string failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < timemodel_columns.count(); ++i) {
    const char *timemodel_column = timemodel_columns.at(i);
    int64_t timemodel_flag = (1 << timemodel_fields.at(i));
    if (i > 0 && OB_FAIL(sql_string.append(", "))) {
      LOG_WARN("append sql string failed", KR(ret));
    } else if (with_sum && OB_FAIL(sql_string.append("SUM("))) {
      LOG_WARN("append sql string failed", KR(ret));
    } else if (lib::is_oracle_mode() &&
      OB_FAIL(sql_string.append_fmt("CASE WHEN BITAND(TIME_MODEL, %ld) = 0 THEN 0 ELSE COUNT_WEIGHT END", timemodel_flag))) {
      LOG_WARN("append timemodel flag failed", KR(ret));
    } else if (!lib::is_oracle_mode() &&
      OB_FAIL(sql_string.append_fmt("CASE WHEN (time_model & %ld) = 0 THEN 0 ELSE COUNT_WEIGHT END", timemodel_flag))) {
      LOG_WARN("append timemodel flag failed", KR(ret));
    } else if (with_sum && OB_FAIL(sql_string.append(")"))) {
      LOG_WARN("append sql string failed", KR(ret));
    } else if (OB_FAIL(sql_string.append_fmt(" AS %s", timemodel_column))) {
      LOG_WARN("append timemodel column failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_single_table = is_single_identifier(source_table);
    if (is_single_table) {
      if (OB_FAIL(sql_string.append_fmt(" FROM %s tmp_timemodel WHERE time_model > 0", source_table))) {
        LOG_WARN("append timemodel source table failed", KR(ret));
      }
    } else {
      //is inner view
      if (OB_FAIL(sql_string.append_fmt(" FROM (%s) tmp_timemodel WHERE time_model > 0", source_table))) {
        LOG_WARN("append timemodel source table failed", KR(ret));
      }
    }
  }
  return ret;
}

bool ObDbmsWorkloadRepository::is_single_identifier(const char *sql_str)
{
  bool is_single = true;
  bool found_space = false;
  if (sql_str == NULL || *sql_str == '\0') {
    is_single = false;
  }
  for (int i = 0; is_single && sql_str[i] != '\0'; i++) {
    if (isspace((unsigned char)sql_str[i])) {
      if (found_space) {
        is_single = false;
      } else {
        found_space = true;
      }
    }
  }
  return is_single;
}

int ObDbmsWorkloadRepository::unpivot_time_model_column_sql(ObSqlString &sql_string,
                                                            const char *select_lists,
                                                            const ObArrayWrap<const char*> &timemodel_columns,
                                                            const char *source_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source table is nullptr", KR(ret));
  } else if (OB_FAIL(sql_string.append("SELECT "))) {
    LOG_WARN("append sql string failed", KR(ret));
  } else if (select_lists != nullptr && OB_FAIL(sql_string.append(select_lists))) {
    LOG_WARN("append select lists string failed", KR(ret), K(select_lists));
  } else if (select_lists != nullptr && OB_FAIL(sql_string.append(", "))) {
    LOG_WARN("append sql string failed", KR(ret));
  } else if (OB_FAIL(sql_string.append("CASE rn_ "))) {
    LOG_WARN("append sql string failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < timemodel_columns.count(); ++i) {
    const char *timemodel_column = timemodel_columns.at(i);
    int64_t rn = i + 1;
    if (OB_FAIL(sql_string.append_fmt("WHEN %ld THEN '%s' ", rn, timemodel_column))) {
      LOG_WARN("append timemodel column failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_string.append("END AS phase_name, "))) {
      LOG_WARN("append sql string failed", KR(ret));
    } else if (OB_FAIL(sql_string.append("CASE rn_ "))) {
      LOG_WARN("append sql string failed", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < timemodel_columns.count(); ++i) {
    const char *timemodel_column = timemodel_columns.at(i);
    int64_t rn = i + 1;
    if (OB_FAIL(sql_string.append_fmt("WHEN %ld THEN %s ", rn, timemodel_column))) {
      LOG_WARN("append timemodel column failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_string.append("END AS phase_cnt "))) {
      LOG_WARN("append sql string failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_single_table = is_single_identifier(source_table);
    if (is_single_table) {
      if (OB_FAIL(sql_string.append_fmt("FROM %s tmp_tm, ", source_table))) {
        LOG_WARN("append timemodel source table failed", KR(ret));
      }
    } else {
      //is inner view
      if (OB_FAIL(sql_string.append_fmt("FROM (%s) tmp_tm, ", source_table))) {
        LOG_WARN("append timemodel source table failed", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_string.append_fmt("(SELECT ROW_NUMBER() OVER(ORDER BY NULL) AS rn_ "
                                      "FROM table(generator(%ld))) t_", timemodel_columns.count()))) {
      LOG_WARN("append sql string failed", KR(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::append_fmt_wr_view_sql(
    const AshReportParams &ash_report_params, ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  const int64_t time_buf_len = 128;
  int64_t time_buf_pos = 0;
  char wr_begin_time_buf[time_buf_len];
  char wr_end_time_buf[time_buf_len];
  char port_buf[time_buf_len];
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.wr_begin_time,
                 wr_begin_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(time_buf_pos = 0)) {
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.wr_end_time,
                 wr_end_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(sprintf(port_buf, "%ld", ash_report_params.port))) {
  } else if ((data_version < DATA_VERSION_4_2_2_0) &&
             OB_FAIL(sql_string.append_fmt(WR_VIEW_SQL_421,
                 lib::is_oracle_mode()
                     ? " CAST(DECODE(SESSION_TYPE, 0, 'FOREGROUND', 'BACKGROUND') AS VARCHAR2(10)) AS SESSION_TYPE,"
                     : " CAST(IF (SESSION_TYPE = 0, 'FOREGROUND', 'BACKGROUND') AS CHAR(10)) AS SESSION_TYPE,",
                 lib::is_oracle_mode()                                 ? wr_oracle_table_421
                 : ash_report_params.cur_tenant_id == OB_SYS_TENANT_ID ? wr_mysql_sys_table_421
                                                                       : wr_mysql_tenant_table_421,
                 static_cast<int>(time_buf_pos),
                 wr_begin_time_buf,
                 static_cast<int>(time_buf_pos),
                 wr_end_time_buf))) {
    LOG_WARN("failed to assign query string", K(ret));
  } else if ((data_version >= DATA_VERSION_4_2_2_0) && (data_version < DATA_VERSION_4_3_0_0) &&
             OB_FAIL(sql_string.append_fmt(data_version >= MOCK_DATA_VERSION_4_2_5_0   ? WR_VIEW_SQL_425
                                           : data_version >= MOCK_DATA_VERSION_4_2_4_0 ? WR_VIEW_SQL_424
                                           : data_version >= MOCK_DATA_VERSION_4_2_3_0 ? WR_VIEW_SQL_423
                                                                                  : WR_VIEW_SQL_422,
                 lib::is_oracle_mode()
                     ? " CAST(DECODE(SESSION_TYPE, 0, 'FOREGROUND', 'BACKGROUND') AS VARCHAR2(10)) AS SESSION_TYPE,"
                     : " CAST(IF (SESSION_TYPE = 0, 'FOREGROUND', 'BACKGROUND') AS CHAR(10)) AS SESSION_TYPE,",
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT_NAME) AS VARCHAR2(64)) AS EVENT,"
                                       : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT_NAME) AS CHAR(64)) AS EVENT,",
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS,"
                                       : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS,",
                 lib::is_oracle_mode()                                 ? wr_oracle_table
                 : ash_report_params.cur_tenant_id == OB_SYS_TENANT_ID ? wr_mysql_sys_table
                                                                       : wr_mysql_tenant_table,
                 static_cast<int>(time_buf_pos),
                 wr_begin_time_buf,
                 static_cast<int>(time_buf_pos),
                 wr_end_time_buf))) {
    LOG_WARN("failed to assign query string", K(ret));
  } else if ((data_version >= DATA_VERSION_4_3_5_2) && OB_FAIL(sql_string.append_fmt(WR_VIEW_SQL_4352, 
                 lib::is_oracle_mode()
                     ? " CAST(DECODE(SESSION_TYPE, 0, 'FOREGROUND', 'BACKGROUND') AS VARCHAR2(10)) AS SESSION_TYPE,"
                     : " CAST(IF (SESSION_TYPE = 0, 'FOREGROUND', 'BACKGROUND') AS CHAR(10)) AS SESSION_TYPE,",
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT_NAME) AS VARCHAR2(64)) AS EVENT,"
                                       : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT_NAME) AS CHAR(64)) AS EVENT,",
                 lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS,"
                                       : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS,",
                 lib::is_oracle_mode()                                 ? wr_oracle_table
                 : ash_report_params.cur_tenant_id == OB_SYS_TENANT_ID ? wr_mysql_sys_table
                                                                       : wr_mysql_tenant_table,
                 static_cast<int>(time_buf_pos),
                 wr_begin_time_buf,
                 static_cast<int>(time_buf_pos),
                 wr_end_time_buf))) {
    LOG_WARN("failed to assign query string", K(ret));
  } else {
    if (ash_report_params.svr_ip != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND WR.SVR_IP = '%.*s'",
              ash_report_params.svr_ip.length(), ash_report_params.svr_ip.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.port != -1) {
      if (OB_FAIL(sql_string.append_fmt(" AND WR.SVR_PORT = '%lu'", ash_report_params.port))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.sql_id != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND WR.SQL_ID = '%.*s'",
              ash_report_params.sql_id.length(), ash_report_params.sql_id.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.trace_id != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND WR.TRACE_ID = '%.*s'",
              ash_report_params.trace_id.length(), ash_report_params.trace_id.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.wait_class != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND WR_EVENT_NAME.WAIT_CLASS = '%.*s'",
              ash_report_params.wait_class.length(), ash_report_params.wait_class.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.tenant_id > 0) {
      if (OB_FAIL(sql_string.append_fmt(" AND WR.TENANT_ID = '%lu'", ash_report_params.tenant_id))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
  }
  return ret;
}

//FIXME The function min(sample_time) converts sample_time from milliseconds to seconds.
//When min(sample_time) and max(sample_time) are converted and fall within the same second,
//even though there are start and end times for the ash or wr,
//the query for the number of WR samples returns 0 because the start and end times are in the same second.
int ObDbmsWorkloadRepository::get_ash_begin_and_end_time(AshReportParams &ash_report_params)
{
  int64_t ash_begin_time = 0;
  int64_t ash_end_time = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append("SELECT MIN(SAMPLE_TIME) AS ASH_BEGIN_TIME, MAX(SAMPLE_TIME)"
                                    " AS ASH_END_TIME  FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(") top_event  GROUP BY svr_ip, svr_port ORDER BY ASH_BEGIN_TIME DESC %s",
                         lib::is_oracle_mode() ? "FETCH FIRST 1 ROW ONLY" : "LIMIT 1"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            if (OB_FAIL(result->get_timestamp("ASH_BEGIN_TIME", nullptr, ash_begin_time))) {
              if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
                ret = OB_SUCCESS;
                ash_begin_time = 0;
              } else {
                LOG_WARN("failed to get timestamp", K(ret));
              }
            } else if (OB_FAIL(result->get_timestamp("ASH_END_TIME", nullptr, ash_end_time))) {
              if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
                ret = OB_SUCCESS;
                ash_end_time = 0;
              } else {
                LOG_WARN("failed to get timestamp", K(ret));
              }
            }
          }
        }  // end while
      }
    }
  }
  if (OB_SUCC(ret)) {
    ash_report_params.ash_begin_time = ash_begin_time;
    ash_report_params.ash_end_time = ash_end_time;
  } else {
    LOG_WARN("failed to get ash begin and end time", K(ret));
  }
  return ret;
}

//FIXME The function min(sample_time) converts sample_time from milliseconds to seconds.
//When min(sample_time) and max(sample_time) are converted and fall within the same second,
//even though there are start and end times for the ash or wr,
//the query for the number of WR samples returns 0 because the start and end times are in the same second.
int ObDbmsWorkloadRepository::get_wr_begin_and_end_time(AshReportParams &ash_report_params)
{
  int ret = OB_SUCCESS;
  int64_t wr_begin_time = 0;
  int64_t wr_end_time = 0;
  ash_report_params.wr_begin_time = ash_report_params.user_input_ash_begin_time;
  ash_report_params.wr_end_time = ash_report_params.ash_begin_time > ash_report_params.user_input_ash_begin_time
                                  ? ash_report_params.ash_begin_time - 1
                                  : ash_report_params.user_input_ash_end_time;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append("SELECT MIN(SAMPLE_TIME) AS WR_BEGIN_TIME, MAX(SAMPLE_TIME)"
                                    " AS WR_END_TIME  FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt wr view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event "))) {
        LOG_WARN("append sql failed", K(ret));
      }
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            if (OB_FAIL(result->get_timestamp("WR_BEGIN_TIME", nullptr, wr_begin_time))) {
              if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
                ret = OB_SUCCESS;
                wr_begin_time = 0;
              } else {
                LOG_WARN("failed to get timestamp", K(ret));
              }
            } else if (OB_FAIL(result->get_timestamp("WR_END_TIME", nullptr, wr_end_time))) {
              if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
                ret = OB_SUCCESS;
                wr_end_time = 0;
              } else {
                LOG_WARN("failed to get timestamp", K(ret));
              }
            }
          }
        }  // end while
      }
    }
  }
  if (OB_SUCC(ret)) {
    ash_report_params.wr_begin_time = wr_begin_time;
    ash_report_params.wr_end_time = wr_end_time;
  } else {
    LOG_WARN("failed to get wr begin and end time", K(ret));
  }
  if (ash_report_params.wr_end_time == 0 && ash_report_params.ash_end_time == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("There are no sampling points in the input time period", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "There are no sampling points in the input time period");
  }
  return ret;
}

int ObDbmsWorkloadRepository::get_ash_num_samples(
    AshReportParams &ash_report_params, int64_t &num_samples)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append_fmt(
              "SELECT COUNT(1) AS NUM_SAMPLES FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            EXTRACT_INT_FIELD_FOR_ASH(*result, "NUM_SAMPLES", num_samples, int64_t);
          }
        }  // end while
      }
    }
  }

  if (OB_SUCC(ret)) {
    ash_report_params.ash_num_samples = num_samples;
  } else {
    LOG_WARN("failed to get ash num samples", K(ret));
  }
  return ret;
}

int ObDbmsWorkloadRepository::get_wr_num_samples(
    AshReportParams &ash_report_params, int64_t &wr_num_samples)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append_fmt(
              "SELECT COUNT(1) AS WR_NUM_SAMPLES FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt wr view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            EXTRACT_INT_FIELD_FOR_ASH(*result, "WR_NUM_SAMPLES", wr_num_samples, int64_t);
          }
        }  // end while
      }
    }
  }
  if (OB_SUCC(ret)) {
    ash_report_params.wr_num_samples = wr_num_samples;
  } else {
    LOG_WARN("failed to get wr num samples", K(ret));
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_summary_info(
    const AshReportParams &ash_report_params, ObStringBuffer &buff, bool &no_data)
{
  int ret = OB_SUCCESS;
  int64_t dur_elapsed_time = ash_report_params.get_elapsed_time() / 1000000LL;
  int64_t num_samples = ash_report_params.ash_num_samples + 10 * ash_report_params.wr_num_samples;
  no_data = false;
  const int64_t time_buf_len = 128;
  const int64_t data_source_len = 64;
  int64_t l_btime_buf_pos = 0;
  int64_t l_etime_buf_pos = 0;
  int64_t ash_begin_time_buf_pos = 0;
  int64_t ash_end_time_buf_pos = 0;
  int64_t wr_begin_time_buf_pos = 0;
  int64_t wr_end_time_buf_pos = 0;
  char l_btime_buf[time_buf_len] = "";
  char l_etime_buf[time_buf_len] = "";
  char ash_begin_time_buf[time_buf_len] = "";
  char ash_end_time_buf[time_buf_len] = "";
  char wr_begin_time_buf[time_buf_len] = "";
  char wr_end_time_buf[time_buf_len] = "";
  char ash_data_source_buf[data_source_len] = "";
  char wr_data_source_buf[data_source_len] = "";
  double avg_active_sess = static_cast<double>(num_samples) / dur_elapsed_time;
  avg_active_sess = round(avg_active_sess * 100) / 100;  // round to two decimal places
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<pre class=\"ash_html\">"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  HEAP_VAR(ObSqlString, temp_string)
  {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.user_input_ash_begin_time, l_btime_buf, time_buf_len, l_btime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.user_input_ash_end_time, l_etime_buf, time_buf_len,l_etime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    }
    if (ash_report_params.ash_begin_time <= 0 && ash_report_params.ash_end_time <= 0) {
      ash_begin_time_buf[0] = '\0';
      ash_begin_time_buf_pos = 0;
      ash_end_time_buf[0] = '\0';
      ash_end_time_buf_pos = 0;
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_begin_time,
                   ash_begin_time_buf, time_buf_len, ash_begin_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_end_time,
                   ash_end_time_buf, time_buf_len, ash_end_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else {
      strncpy(ash_data_source_buf, lib::is_oracle_mode() ? "SYS.GV$ACTIVE_SESSION_HISTORY" : "oceanbase.GV$ACTIVE_SESSION_HISTORY", data_source_len);
    }
    if (OB_FAIL(ret)) {
    } else if (ash_report_params.wr_begin_time <= 0 && ash_report_params.wr_end_time <= 0) {
      wr_begin_time_buf[0] = '\0';
      wr_begin_time_buf_pos = 0;
      wr_end_time_buf[0] = '\0';
      wr_end_time_buf_pos = 0;
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.wr_begin_time,
                   wr_begin_time_buf, time_buf_len, wr_begin_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.wr_end_time,
                   wr_end_time_buf, time_buf_len, wr_end_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else {
      if (lib::is_oracle_mode()) {
        strncpy(wr_data_source_buf, "SYS.DBA_WR_ACTIVE_SESSION_HISTORY", data_source_len);
      } else {
        strncpy(wr_data_source_buf, ash_report_params.tenant_id == OB_SYS_TENANT_ID
                                      ?  "oceanbase.CDB_WR_ACTIVE_SESSION_HISTORY"
                                      : "oceanbase.DBA_WR_ACTIVE_SESSION_HISTORY", data_source_len);
      }
    }

    // OS info
    struct utsname uts;
    if (0 != ::uname(&uts)) {
      ret = OB_ERR_SYS;
      LOG_WARN("call uname failed");
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   temp_string.append_fmt("           Cluster Name: %s \n", GCONF.cluster.str()))) {
      LOG_WARN("failed to assign Cluster Name string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt(
                   "       Observer Version: %s (%s) \n", PACKAGE_STRING, build_version()))) {
      LOG_WARN("failed to assign Observer Version string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("  Operation System Info: %s(%s)_%s \n", uts.sysname,
                   uts.release, uts.machine))) {
      LOG_WARN("failed to assign Operation System Info string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("  User Input Begin Time: %.*s \n",
                   static_cast<int>(l_btime_buf_pos), l_btime_buf))) {
      LOG_WARN("failed to assign Sample Begin string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("    User Input End Time: %.*s \n",
                   static_cast<int>(l_etime_buf_pos), l_etime_buf))) {
      LOG_WARN("failed to assign Sample End string", K(ret));
    } else if (ash_report_params.ash_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt(
                   "Ash Analysis Begin Time: %.*s \n", static_cast<int>(ash_begin_time_buf_pos), ash_begin_time_buf))) {
      LOG_WARN("failed to assign Ash Analysis Begin Time string", K(ret));
    } else if (ash_report_params.ash_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt(
                   "  Ash Analysis End Time: %.*s \n", static_cast<int>(ash_end_time_buf_pos), ash_end_time_buf))) {
      LOG_WARN("failed to assign Ash Analysis End Time string", K(ret));
    } else if (ash_report_params.wr_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt(
                   " Wr Analysis Begin Time: %.*s \n", static_cast<int>(wr_begin_time_buf_pos), wr_begin_time_buf))) {
      LOG_WARN("failed to assign Wr Analysis Begin Time string", K(ret));
    } else if (ash_report_params.wr_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt(
                   "   Wr Analysis End Time: %.*s \n", static_cast<int>(wr_end_time_buf_pos), wr_end_time_buf))) {
      LOG_WARN("failed to assign Ash Analysis End Time string", K(ret));
    } else if (ash_report_params.ash_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt(
                   "        Ash Data Source: %.*s \n", static_cast<int>(sizeof(ash_data_source_buf)), ash_data_source_buf))) {
      LOG_WARN("failed to assign Ash Data Source string", K(ret));
    } else if (ash_report_params.wr_end_time > 0 &&
          OB_FAIL(temp_string.append_fmt(
                   "         Wr Data Source: %.*s \n", static_cast<int>(sizeof(wr_data_source_buf)), wr_data_source_buf))) {
      LOG_WARN("failed to assign Wr Data Source string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("           Elapsed Time: %ld \n", dur_elapsed_time))) {
      LOG_WARN("failed to assign Elapsed Time string", K(ret));
    } else if (ash_report_params.ash_end_time > 0 &&
               OB_FAIL(temp_string.append_fmt("      Ash Num of Sample: %ld \n", ash_report_params.ash_num_samples))) {
      LOG_WARN("failed to assign Ash Num of Sample string", K(ret));
    } else if (ash_report_params.wr_end_time > 0 && OB_FAIL(temp_string.append_fmt("       Wr Num of Sample: %ld \n",
                                                        ash_report_params.wr_num_samples * 10))) {
      LOG_WARN("failed to assign Wr Num of Sample string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("Average Active Sessions: %.2f \n", avg_active_sess))) {
      LOG_WARN("failed to assign Average Active Sessions string", K(ret));
    } else if (OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
      LOG_WARN("failed to push string into buff", K(ret));
    } else {
      if (dur_elapsed_time <= 0) {
        dur_elapsed_time = 1;
      }
      if (num_samples <= 0) {
        num_samples = 1;
        no_data = true;
      }
    }
  }
  if (OB_SUCC(ret) && ash_report_params.is_html) {
    if (OB_FAIL(buff.append("</pre>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else if (OB_FAIL(buff.append("<ul id=110></ul>"))) {  // hyperlink list for sections
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

inline void calc_ratio(int64_t dividend, int64_t divisor, char *ratio_char)
{
  if (divisor != 0) {
    double ratio = static_cast<double>(dividend) / divisor;
    ratio = round(1000 * 100 * ratio) / 1000;
    sprintf(ratio_char, "%.2f%%", ratio);
  }
}

inline void calc_avg_active_sessions(int64_t dividend, int64_t divisor, char *ratio_char)
{
  if (divisor != 0) {
    double ratio = static_cast<double>(dividend) / divisor;
    ratio = round(1000 * ratio) / 1000;
    sprintf(ratio_char, "%.2f", ratio);
  }
}

inline void calc_avg_active_sessions(int64_t dividend, double divisor, char *ratio_char)
{
  if (abs(divisor) > 1e-6) {
    double ratio = static_cast<double>(dividend) / divisor;
    ratio = round(1000 * ratio) / 1000;
    sprintf(ratio_char, "%.2f", ratio);
  }
}

int ObDbmsWorkloadRepository::append_fmt_ash_wr_view_sql(const AshReportParams &ash_report_params, ObSqlString &sql_string) {
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(sql_string.append("SELECT * FROM ("))) {
    LOG_WARN("failed to append fmt ash wr view sql", K(ret));
  } else if (ash_report_params.ash_end_time > 0 && 
            OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
    LOG_WARN("failed to append fmt ash view sql", K(ret));
  } else if (data_version >= DATA_VERSION_4_3_5_2 && ash_report_params.ash_end_time > 0 && 
             ash_report_params.wr_end_time > 0 && 
             OB_FAIL(sql_string.append(" UNION ALL "))) {
    LOG_WARN("failed to union all", K(ret));
  } else if (data_version >= DATA_VERSION_4_3_5_2 &&  ash_report_params.wr_end_time > 0 && 
             OB_FAIL(append_fmt_wr_view_sql(ash_report_params, sql_string))) {
    LOG_WARN("failed to fmt wr view sql", K(ret));
  } else if (OB_FAIL(sql_string.append(") WHERE 1 = 1  "))) {
    LOG_WARN("failed to append fmt ash wr view sql", K(ret));
  }
  return ret;
}
int ObDbmsWorkloadRepository::print_ash_top_active_tenants(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {"Top Active Tenants",
      "this section lists top active tenant information",
      "Total Samples: num of records during ash report analysis time period",
      "Wait Event Samples: num of records when session is on wait event",
      "On CPU Samples: num of records when session is on cpu",
      "Avg Active Sessions: average active sessions during ash report analysis time period",
      "% Activity: activity(cpu + wait) percentage for given tenant",
      "Equivalent Client Load: equivalent client average active sessions during ash report analysis time period"};
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 8;
    const int64_t column_widths[column_size] = {64, 12, 18, 23, 19, 20, 11, 24};
    AshColumnItem column_headers[column_size] = {"Tenant Name", "Session Type", "Total Samples",
        "Wait Event Samples", "On CPU Samples", "Avg Active Sessions", "% Activity", "Equivalent Client Load"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      //If the ratio between client total time and db time is below the minimum activity level,
      //we consider that particular client session to be inactive.
      //In this case, when calculating the load for that client session,
      //only the db time passing through that session will be recorded,
      //without taking into account any idle time within it.
      //see it:SUM(CASE WHEN (DB_CNT < 0.1*DELTA_TIME) THEN DB_CNT ELSE DELTA_TIME END) as TOTAL_TIME
      //min_active_ratio=0.1
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "SELECT TENANT_ID, SESSION_TYPE, SUM(db_cnt) AS CNT, SUM(cpu_cnt) AS CPU_CNT, "
                  "SUM(wait_cnt) AS WAIT_CNT, "
                  "SUM(CASE WHEN (db_cnt < 0.1 * delta_time) THEN db_cnt ELSE delta_time END) AS TOTAL_TIME "
          "FROM ("
            "SELECT tenant_id, session_type, SUM(count_weight) AS db_cnt, "
                    "SUM(CASE WHEN event_no = 0 THEN count_weight ELSE 0 END) AS cpu_cnt, "
                    "SUM(CASE WHEN event_no = 0 THEN 0 ELSE count_weight END) AS wait_cnt, "
                    "%s AS delta_time "
            "FROM (",
        lib::is_oracle_mode() ? "(ROUND((CAST(MAX(sample_time) AS DATE) - CAST(MIN(sample_time) AS DATE)) * 86400) + 1)"
                              : "CAST((UNIX_TIMESTAMP(MAX(sample_time)) - UNIX_TIMESTAMP(MIN(sample_time)) + 1) AS SIGNED)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
            ") tmp_ash "
            "GROUP BY tenant_id, proxy_sid, session_type"
          ") session_load "
          "GROUP BY tenant_id, session_type "
          "ORDER BY cnt DESC"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            uint64_t tenant_id = 0;
            char tenant_name[64] = "";
            int64_t total_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TENANT_ID", tenant_id, uint64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_TIME", total_time, int64_t);

            if (OB_SUCC(ret)) {
              ObSchemaGetterGuard schema_guard;
              const ObSimpleTenantSchema *tenant_info = nullptr;
              if (OB_ISNULL(GCTX.schema_service_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("schema service is nullptr", K(ret));
              } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
                LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
              } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
                LOG_WARN("get tenant info failed", K(ret), K(tenant_id));
              }
              ret = OB_SUCCESS;
              if (OB_NOT_NULL(tenant_info)) {
                snprintf(tenant_name, 64, "%s", tenant_info->get_tenant_name());
                tenant_name[63] = '\0';
              } else {
                snprintf(tenant_name, 63, "tenant:%ld", tenant_id);
              }
            }

            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(cnt,
                ash_report_params.get_elapsed_time() / 1000000,
                avg_active_sessions_char);

            char client_load_char[64] = "-";
            if (0 == strcasecmp(session_type_char, "FOREGROUND")) {
              calc_avg_active_sessions(total_time,
                  ash_report_params.get_elapsed_time() / 1000000,
                  client_load_char);
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[column_size] = {tenant_name, session_type_char,
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  avg_active_sessions_char, activity_radio_char, client_load_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_node_load(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
      "Top Node Load",
      "this section lists top node measured by DB time",
      "IP: OceanBase instance svr_ip",
      "Port: OceanBase instance svr_port",
      "Total Samples: num of records during ash report analysis time period",
      "Wait Event Samples: num of records when session is on wait event",
      "On CPU Samples: num of records when session is on cpu",
      "Avg Active Sessions: average active sessions during ash report analysis time period",
      "% Activity: activity(cpu + wait) percentage for given tenant",
      "Equivalent Client Load: equivalent client average active sessions during ash report analysis time period"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))){
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 9;
    const int64_t column_widths[column_size] = {16, 7, 12, 18, 23, 19, 20, 11, 24};
    AshColumnItem column_headers[column_size] = {"IP", "Port", "Session Type", "Total Samples",
        "Wait Event Samples", "On CPU Samples", "Avg Active Sessions", "% Activity", "Equivalent Client Load"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      //If the ratio between client total time and db time is below the minimum activity level,
      //we consider that particular client session to be inactive.
      //In this case, when calculating the load for that client session,
      //only the db time passing through that session will be recorded,
      //without taking into account any idle time within it.
      //see it:SUM(CASE WHEN (0.1 * TM_DELTA_TIME > 1000000) THEN 1000000 ELSE TM_DELTA_TIME END)
      //min_active_ratio=0.1
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "SELECT SVR_IP, SVR_PORT, SESSION_TYPE, SUM(count_weight) AS CNT, "
            "SUM(CASE WHEN event_no = 0 THEN count_weight ELSE 0 END) AS CPU_CNT, "
            "SUM(CASE WHEN event_no = 0 THEN 0 ELSE count_weight END) AS WAIT_CNT, "
            "SUM((CASE WHEN (0.1 * TM_DELTA_TIME > 1000000) THEN 1000000 ELSE TM_DELTA_TIME END) * count_weight) AS TOTAL_TIME "
          "FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash and wr view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") te "
          "GROUP BY te.svr_ip, te.svr_port, te.session_type "
          "ORDER BY cnt DESC"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char ip_char[64] = "";
            int64_t total_time = 0;
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SVR_IP", ip_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SVR_PORT", port, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_TIME", total_time, int64_t);
            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(cnt,
                ash_report_params.get_elapsed_time() / 1000000,
                avg_active_sessions_char);

            char client_load_char[64] = "-";
            if (0 == strcasecmp(session_type_char, "FOREGROUND")) {
              calc_avg_active_sessions(total_time,
                  ash_report_params.get_elapsed_time(),
                  client_load_char);
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {ip_char, ASH_FIELD_CHAR(port), session_type_char,
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  avg_active_sessions_char, activity_radio_char, client_load_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_foreground_db_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char* contents[] = {
    "Top Foreground DB Time",
    "This section lists top foreground db time categorized by event",
    "Event Name: comprise wait event and on cpu event",
    "Event Samples: num of sampled session activity records",
    "% Activity: activity percentage for given event",
    "Avg Active Sessions: average active sessions during ash report analysis time period"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))){
    LOG_WARN("failed to print section header and explaination");
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {64, 64, 20, 13, 20, 11};
    AshColumnItem column_headers[column_size] = {
        "Node", "Event Name", "Wait Class", "Event Samples", "Avg Active Sessions", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "WITH session_data AS ("
            "SELECT %s AS event, %s AS wait_class, SUM(count_weight) AS event_time, %s AS node "
            "FROM (",
            lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'ON CPU', event) AS VARCHAR2(64))"
                                  : "CAST(IF (event_no = 0, 'ON CPU', event) AS CHAR(64))", //%s AS event
            lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'NULL', wait_class) AS VARCHAR2(64))"
                                  : "CAST(IF (event_no = 0, 'NULL', wait_class) AS CHAR(64))", //%s AS wait_class
            lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)"))) { //%s AS node
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                   " and session_type = 'FOREGROUND'"
            ") ta "
            "GROUP BY ta.svr_ip, ta.svr_port, ta.event_no, ta.event, ta.wait_class "
            "ORDER BY event_time DESC %s"
          "), ",
          lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //ORDER BY event_time DESC %s
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_event AS ("
            "SELECT node, event, wait_class, event_time, "
                    "ROW_NUMBER() OVER (PARTITION BY node ORDER BY event_time DESC) AS event_rank "
            "FROM session_data"
          "), "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_node AS ("
            "SELECT node, SUM(event_time) AS node_time "
            "FROM session_data "
            "GROUP BY node"
          ") "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "SELECT CASE WHEN te.event_rank = 1 THEN te.node ELSE '' END AS NODE, "
                  "te.event AS EVENT, te.wait_class AS WAIT_CLASS, te.event_time AS EVENT_TIME "
          "FROM top_event te JOIN top_node tn ON te.node = tn.node "
          "ORDER BY tn.node_time DESC, te.event_rank"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "NODE", node_char, 64, tmp_real_str_len);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_TIME", event_time, int64_t);
            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(event_time,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time + 
                 ash_report_params.wr_end_time - ash_report_params.wr_begin_time) / 1000000, avg_active_sessions_char);

            char event_radio_char[64] = "";
            calc_ratio(event_time, num_samples, event_radio_char);

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  node_char, event_char, wait_class_char, ASH_FIELD_CHAR(event_time),
                  avg_active_sessions_char, event_radio_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

#define EXTRACT_ASH_EXEC_PHASE(phase)                                                             \
  int64_t phase##_cnt = 0;                                                                        \
  EXTRACT_INT_FIELD_FOR_ASH(*result, #phase, phase##_cnt, int64_t);                               \
  if (OB_FAIL(ret)) {                                                                             \
  } else if (OB_FAIL(                                                                             \
                 phase_array.push_back(std::pair<const char *, int64_t>(#phase, phase##_cnt)))) { \
    LOG_WARN("failed to push ##phase pair into phase array", K(ret));                             \
  }

//when adding a row in tm_columns, the bit flag in tm_flags should be set
const char *tm_columns[] = {"IN_PARSE",
                            "IN_PL_PARSE",
                            "IN_PLAN_CACHE",
                            "IN_SQL_OPTIMIZE",
                            "IN_SQL_EXECUTION",
                            "IN_PX_EXECUTION",
                            "IN_SEQUENCE_LOAD",
                            "IN_COMMITTING",
                            "IN_STORAGE_READ",
                            "IN_STORAGE_WRITE",
                            "IN_REMOTE_DAS_EXECUTION",
                            "IN_PLSQL_COMPILATION",
                            "IN_PLSQL_EXECUTION",
                            "IN_FILTER_ROWS",
                            "IN_RPC_ENCODE",
                            "IN_RPC_DECODE",
                            "IN_CONNECTION_MGR",
                            "IN_CHECK_ROW_CONFLICTION",
                            "IN_DEADLOCK_ROW_REGISTER",
                            "IN_CHECK_TX_STATUS",
                            "IN_RESOLVE",
                            "IN_REWRITE",
                            "IN_DUPLICATE_CONFLICT_RESOLVE",
                            "IN_FOREIGN_KEY_CASCADING",
                            "IN_EXTRACT_QUERY_RANGE"};
int32_t tm_flags[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};

int ObDbmsWorkloadRepository::print_ash_top_execution_phase(
    const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top Execution Phase",
    "this section lists top phases of execution, such as SQL, PL/SQL, STORAGE, etc.",
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))){
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {12, 40, 20, 11, 40, 11};
    AshColumnItem column_headers[column_size] = {
        "Session Type", "Phase of Execution", "Active Samples", "% Activity", "SQL_ID", "% SQL_ID"};
    column_headers[0].merge_cell_ = true; //merge empty cell with session type
    ObArrayWrap<const char*> tm_cols_wrap(tm_columns, ARRAYSIZEOF(tm_columns));
    ObArrayWrap<int32_t> tm_flags_wrap(tm_flags, ARRAYSIZEOF(tm_columns));
    HEAP_VARS_3((ObISQLClient::ReadResult, res), (ObSqlString, sql_string), (ObSqlString, tm_view))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "WITH session_data AS ("
          "SELECT session_type,sql_id,trace_id,time_model,count_weight,svr_ip, svr_port, session_id "
          "FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") tmp_ash "
        "), "))) {
        LOG_WARN("append ash view sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        " session_data_fixup AS ( "
          " SELECT session_type,sql_id,trace_id,time_model,count_weight FROM ( "
              " SELECT "
                " session_type, "
                " FIRST_VALUE ( sql_id ) IGNORE NULLS OVER ( PARTITION BY svr_ip, svr_port, session_id, trace_id ) AS sql_id, "
                " trace_id, "
                " time_model, "
                " count_weight "
              " FROM session_data "
            " ) WHERE time_model > 0"
          " ) "))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "SELECT SQL_ID, SESSION_TYPE, PHASE_NAME, SQL_PHASE_SAMPLES, TOTAL_PHASE_SAMPLES, "
                  "ROW_NUMBER() OVER(PARTITION BY session_type ORDER BY total_phase_samples DESC) AS PHASE_RANK "
          "FROM ("
            "SELECT sql_id, session_type, phase_name, phase_cnt AS sql_phase_samples, "
                    "SUM(phase_cnt) OVER(PARTITION BY session_type, phase_name ORDER BY phase_cnt ASC) AS total_phase_samples, "
                    "ROW_NUMBER() OVER(PARTITION BY session_type, phase_name ORDER BY phase_cnt DESC) AS sql_rank "
            "FROM ("))) {
        LOG_WARN("append ash sql failed", K(ret));
      } else if (OB_FAIL(append_time_model_view_sql(
        tm_view,"sql_id, session_type", tm_cols_wrap, tm_flags_wrap, "session_data_fixup", true))) {
        LOG_WARN("append time model view sql failed", K(ret));
      } else if (OB_FAIL(tm_view.append(" GROUP BY sql_id, session_type"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(unpivot_time_model_column_sql(sql_string, "sql_id, session_type", tm_cols_wrap, tm_view.ptr()))) {
        LOG_WARN("append unpivot timemodel column sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
            ") unpivot_phase WHERE phase_cnt > 0"
          ") all_phase "
          "WHERE sql_rank = 1 "
          "ORDER BY SESSION_TYPE DESC, PHASE_RANK ASC"))) {
        LOG_WARN("unpivot time model column sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            tmp_real_str_len = 0;
            char phase_name_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "PHASE_NAME", phase_name_char, 64, tmp_real_str_len);

            tmp_real_str_len = 0;
            char sql_id_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "SQL_ID", sql_id_char, 64, tmp_real_str_len);

            int64_t sql_phase_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SQL_PHASE_SAMPLES", sql_phase_samples, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_PHASE_SAMPLES", total_phase_samples, int64_t);
            int64_t phase_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "PHASE_RANK", phase_rank, int64_t);
            if (phase_rank > 1) {
              session_type_char[0] = '\0';
            }

            char sample_radio_char[64] = "";
            calc_ratio(total_phase_samples, num_samples, sample_radio_char);
            char sql_radio_char[64] = "";
            calc_ratio(sql_phase_samples, num_samples, sql_radio_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  session_type_char, phase_name_char, ASH_FIELD_CHAR(total_phase_samples), sample_radio_char,
                  sql_id_char, sql_radio_char};
              if (column_content[4].column_content_[0] != '\0') {
                //mark SQL_ID column
                column_content[4].href_sql_id_ = true;
              }
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

#undef EXTRACT_ASH_EXEC_PHASE

int ObDbmsWorkloadRepository::print_ash_background_db_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top Background DB Time",
    "this section lists top DB Time for background sessions",
    "Program: process name for background sessions",
    "Event Name: comprise wait event and on cpu event",
    "Event Samples: num of sampled session activity records",
    "% Activity: activity percentage for given event",
    "Avg Active Sessions: average active sessions during ash report analysis time period"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 9;
    const int64_t column_widths[column_size] = {64, 65, 64, 64, 64, 20, 13, 11, 20};
    AshColumnItem column_headers[column_size] = {
        "Node", "Program", "Module", "Action", "Event Name", "Wait Class", "Event Samples", " % Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "WITH session_data AS ("
            "SELECT %s AS event, %s AS wait_class, SUM(count_weight) AS event_time, %s AS node, "
                    "CASE WHEN program IS NULL THEN 'UNDEFINED' ELSE program END AS program, "
                    "CASE WHEN module IS NULL THEN 'UNDEFINED' ELSE module END AS module, "
                    "CASE WHEN action IS NULL THEN 'UNDEFINED' ELSE action END AS action "
            "FROM (",
            lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'ON CPU', event) AS VARCHAR2(64))"
                                  : "CAST(IF (event_no = 0, 'ON CPU', event) AS CHAR(64))", //%s AS event
            lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'NULL', wait_class) AS VARCHAR2(64))"
                                  : "CAST(IF (event_no = 0, 'NULL', wait_class) AS CHAR(64))", //%s AS wait_class
            lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)"))) { //%s AS node
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                   " and session_type='BACKGROUND'"
            ") ta "
            "GROUP BY ta.svr_ip, ta.svr_port, ta.program, ta.module, ta.action, ta.event_no, ta.event, ta.wait_class "
            "ORDER BY event_time DESC %s"
          "), ",
          lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //ORDER BY event_time DESC %s
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_event AS ("
            "SELECT node, event, wait_class, event_time, program, module, action, "
                    "ROW_NUMBER() OVER (PARTITION BY node ORDER BY event_time DESC) AS event_rank "
            "FROM session_data"
          "), "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_node AS ("
            "SELECT node, SUM(event_time) AS node_time "
            "FROM session_data "
            "GROUP BY node"
          ") "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "SELECT CASE WHEN te.event_rank = 1 THEN te.node ELSE '' END AS NODE, "
                  "te.event AS EVENT, te.wait_class AS WAIT_CLASS, te.event_time AS EVENT_TIME, "
                  "te.program AS PROGRAM, te.module AS MODULE, te.action AS ACTION "
          "FROM top_event te JOIN top_node tn ON te.node = tn.node "
          "ORDER BY tn.node_time DESC, te.event_rank"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "NODE", node_char, 64, tmp_real_str_len);
            char program_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "PROGRAM", program_char, 64, tmp_real_str_len);
            char module_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "MODULE", module_char, 64, tmp_real_str_len);
            char action_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "ACTION", action_char, 64, tmp_real_str_len);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_TIME", event_time, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(event_time, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(event_time,
                ash_report_params.get_elapsed_time() / 1000000,
                avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  node_char, program_char, module_char, action_char,
                  event_char, wait_class_char,
                  ASH_FIELD_CHAR(event_time), event_radio_char, avg_active_sessions_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_sessions(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {"Top Sessions",
      "this section lists top Active Sessions with the largest wait event and SQL_ID",
      "Session ID: user session id",
      "% Activity: represents the load on the database caused by this session",
      "Avg Active Sessions: average active sessions during ash report analysis time period",
      "Event Name: comprise wait event and on cpu event",
      "% Event: represents the activity load of the event on the database",
      "% SQL ID: represents the activity load of the event on the database",
      "Sql Executions: represents the execution count of the SQL_ID"};
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 12;
    const int64_t column_widths[column_size] = {32, 20, 64, 11, 20, 64, 20, 11, 40, 20, 11, 20};
    AshColumnItem column_headers[column_size] = {"Node", "Session ID", "Program", "% Activity", "Avg Active Sessions",
        "Event Name", "Wait Class", "% Event", "SQL ID", "Plan Hash", "% SQL ID", "Sql Executions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "WITH session_data AS ("
                "SELECT svr_ip, svr_port, session_id, %s AS event, %s AS wait_class, "
                "event_no, sql_id, plan_hash, trace_id, program, count_weight "
              "FROM (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(event_no, 0, 'ON CPU', event) AS VARCHAR2(64))"
                  : "CAST(IF (event_no = 0, 'ON CPU', event) AS CHAR(64))",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(event_no, 0, 'NULL', wait_class) AS VARCHAR2(64))"
                  : "CAST(IF (event_no = 0, 'NULL', wait_class) AS CHAR(64))"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and session_type='FOREGROUND') tmp_ash), "))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
              "top_session AS ("
                "SELECT svr_ip, svr_port, session_id, SUM(count_weight) AS total_sample_count, program "
                "FROM session_data "
                "GROUP BY svr_ip, svr_port, session_id, program "
                "ORDER BY total_sample_count DESC %s), ",
                  lib::is_oracle_mode() ? "FETCH FIRST 15 ROWS ONLY" : "LIMIT 15"))) {
        LOG_WARN("append top session topic text failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
              "top_event AS ("
              "SELECT svr_ip, svr_port, session_id, event_no, event, wait_class, event_sample_count "
                "FROM ("
                  "SELECT svr_ip, svr_port, session_id, event_no, event, wait_class, SUM(count_weight) AS event_sample_count, "
                          "ROW_NUMBER() OVER (PARTITION BY svr_ip, svr_port, session_id ORDER BY SUM(count_weight) DESC) AS event_rank "
                  "FROM session_data "
                  "WHERE (svr_ip, svr_port, session_id) IN (SELECT svr_ip, svr_port, session_id FROM top_session) "
                  "GROUP BY svr_ip, svr_port, session_id, event_no, event, wait_class"
                ") event_data "
                "WHERE event_rank = 1"
              "), "))) {
        LOG_WARN("append top event topic text failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
              "sql_data AS ("
                "SELECT svr_ip, svr_port, session_id, event_no, sql_id, plan_hash, "
                        "SUM(SUM(count_weight)) OVER (PARTITION BY svr_ip, svr_port, session_id, event_no, sql_id ORDER BY SUM(count_weight)) AS sql_sample_count, "
                        "COUNT(DISTINCT trace_id) AS sql_exec_count "
                "FROM ( "
                        " SELECT svr_ip, svr_port, session_id, event_no, count_weight, trace_id, "
                        " FIRST_VALUE ( sql_id ) IGNORE NULLS OVER ( PARTITION BY svr_ip, svr_port, session_id, trace_id ) AS sql_id, "
                        " FIRST_VALUE ( plan_hash ) IGNORE NULLS OVER ( PARTITION BY svr_ip, svr_port, session_id, trace_id ) AS plan_hash "
                        " from session_data "
                      " ) "
                "WHERE (svr_ip, svr_port, session_id, event_no) IN (SELECT svr_ip, svr_port, session_id, event_no FROM top_event) "
                "GROUP BY svr_ip, svr_port, session_id, event_no, sql_id, plan_hash"
              "), "))) {
        LOG_WARN("append sql_data text failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
              "top_sql AS ("
                "SELECT svr_ip, svr_port, session_id, sql_id, plan_hash, sql_sample_count, sql_exec_count "
                "FROM ("
                  "SELECT svr_ip, svr_port, session_id, sql_id, plan_hash, sql_sample_count, sql_exec_count, "
                          "ROW_NUMBER() OVER (PARTITION BY svr_ip, svr_port, session_id, event_no ORDER BY sql_sample_count DESC) AS sql_rank "
                  "FROM sql_data"
                ") tmp_sql "
                "WHERE sql_rank = 1 AND sql_id IS NOT NULL AND LENGTH(sql_id) > 0"
              ") "))) {
        LOG_WARN("append top sql topic text failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
              "SELECT %s AS NODE, ts.session_id AS SESSION_ID, "
                      "ts.total_sample_count AS TOTAL_SAMPLE_COUNT, "
                      "ts.program AS PROGRAM, "
                      "te.event AS EVENT, "
                      "te.event_sample_count AS EVENT_SAMPLE_COUNT, "
                      "te.wait_class AS WAIT_CLASS, "
                      "tsq.sql_id AS SQL_ID, "
                      "tsq.plan_hash AS PLAN_HASH, "
                      "tsq.sql_sample_count AS SQL_SAMPLE_COUNT, "
                      "tsq.sql_exec_count AS SQL_EXEC_COUNT "
              "FROM top_session ts "
              "LEFT JOIN top_event te ON ts.session_id = te.session_id and ts.svr_ip = te.svr_ip and ts.svr_port = te.svr_port "
              "LEFT JOIN top_sql tsq ON ts.session_id = tsq.session_id and ts.svr_ip = tsq.svr_ip and ts.svr_port = tsq.svr_port "
              "ORDER BY ts.total_sample_count DESC",
              lib::is_oracle_mode() ? "ts.svr_ip||':'||ts.svr_port" : "CONCAT(ts.svr_ip, ':', ts.svr_port)"//%s AS node
              ))) {
        LOG_WARN("append the main topic failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "NODE", node_char, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SESSION_ID", session_id, int64_t);
            int64_t total_sample_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_SAMPLE_COUNT", total_sample_cnt, int64_t);
            char program_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "PROGRAM", program_char, 64, tmp_real_str_len);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            int64_t event_sample_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "EVENT_SAMPLE_COUNT", event_sample_cnt, int64_t);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);
            char sql_id[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, int64_t);
            int64_t sql_sample_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SQL_SAMPLE_COUNT", sql_sample_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SQL_EXEC_COUNT", sql_exec_cnt, int64_t);

            char active_radio_char[64] = "";
            calc_ratio(total_sample_cnt, num_samples, active_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(total_sample_cnt,
                ash_report_params.get_elapsed_time() / 1000000, avg_active_sessions_char);

            char event_percentage[64] = "";
            calc_ratio(event_sample_cnt, num_samples, event_percentage);

            char sql_percentage[64] = "";
            calc_ratio(sql_sample_cnt, num_samples, sql_percentage);

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  node_char, ASH_FIELD_CHAR(session_id), program_char, active_radio_char, avg_active_sessions_char,
                  event_char, wait_class_char, event_percentage,
                  sql_id, ASH_FIELD_CHAR(plan_hash), sql_percentage, ASH_FIELD_CHAR(sql_exec_cnt)};
              if (column_content[7].column_content_[0] != '\0') {
                //mark SQL_ID
                column_content[7].href_sql_id_ = true;
              }
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_group(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top Groups",
    "this section lists top resource consumer groups",
    "Group Name: resource consumer group name",
    "Group Samples: num of sampled session activity records in the current resource group",
    "% Activity: activity percentage for given event resource group",
    "Avg Active Sessions: average active sessions during ash report analysis time period"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 11;
    const int64_t column_widths[column_size] = {64, 128, 13, 11, 64, 11, 32, 11, 32, 11, 20};
    AshColumnItem column_headers[column_size] = {
        "Node", "Group Name", "Group Samples", "% Activity", "Program", "% Program", "Module", "% Module", "Action", "% Action", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header( ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "WITH session_data AS ("
          "SELECT svr_ip, svr_port, tenant_id, group_id, sample_time, count_weight, "
                  "CASE WHEN program IS NULL THEN 'UNDEFINED' ELSE program END AS program, "
                  "CASE WHEN module IS NULL THEN 'UNDEFINED' ELSE module END AS module, "
                  "CASE WHEN action IS NULL THEN 'UNDEFINED' ELSE action END AS action "
          "FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") ash "
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "top_action AS ("
          "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, "
                  "action_time, module_time, program_time, group_time, node_time "
          "FROM ("
            "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, "
                    "SUM(count_weight) AS action_time, "
                    "SUM(SUM(count_weight)) OVER ("
                      "PARTITION BY svr_ip, svr_port, tenant_id, group_id, program, module "
                      "ORDER BY NULL "
                      "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                    ") AS module_time, "
                    "SUM(SUM(count_weight)) OVER("
                      "PARTITION BY svr_ip, svr_port, tenant_id, group_id, program "
                      "ORDER BY NULL "
                      "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                    ") AS program_time, "
                    "SUM(SUM(count_weight)) OVER("
                      "PARTITION BY svr_ip, svr_port, tenant_id, group_id "
                      "ORDER BY NULL "
                      "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                    ") AS group_time, "
                    "SUM(SUM(count_weight)) OVER("
                      "PARTITION BY svr_ip, svr_port "
                      "ORDER BY NULL "
                      "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                    ") AS node_time "
            "FROM session_data "
            "GROUP BY svr_ip, svr_port, tenant_id, group_id, program, module, action"
          ") tmp_action "
          "WHERE action_time >= %ld "
          "ORDER BY action_time DESC %s"
        ") ",
        num_samples / 100, //WHERE action_time >= %ld, drop %action < 1%
        lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //ORDER BY action_time DESC %s
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "SELECT SVR_IP, SVR_PORT, NODE_TIME, "
                "ROW_NUMBER() OVER ("
                  "PARTITION BY svr_ip, svr_port "
                  "ORDER BY row_order"
                ") AS GROUP_RANK, "
                "TENANT_ID, GROUP_ID, GROUP_TIME, "
                "ROW_NUMBER() OVER ("
                  "PARTITION BY svr_ip, svr_port, tenant_id, group_id "
                  "ORDER BY row_order"
                ") AS PROGRAM_RANK, "
                "PROGRAM, PROGRAM_TIME, "
                "ROW_NUMBER() OVER ("
                  "PARTITION BY svr_ip, svr_port, tenant_id, group_id, program "
                  "ORDER BY row_order"
                ") AS MODULE_RANK, "
                "MODULE, MODULE_TIME, "
                "ROW_NUMBER() OVER ("
                  "PARTITION BY svr_ip, svr_port, tenant_id, group_id, program, module "
                  "ORDER BY row_order"
                ") AS ACTION_RANK, "
                "ACTION, ACTION_TIME "
        "FROM ("
          "SELECT svr_ip, svr_port, node_time, tenant_id, group_id, group_time, "
                  "program, program_time, module, module_time, action, action_time, "
                  "ROW_NUMBER() OVER("
                    "ORDER BY node_time DESC, group_time DESC, "
                              "program_time DESC, module_time DESC, "
                              "action_time DESC"
                  ") AS row_order "
          "FROM top_action"
        ") ta "
        "ORDER BY row_order"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
      } else {
        bool got_data = false;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            got_data = true;
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            char svr_ip_char[32] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SVR_IP", svr_ip_char, 32, tmp_real_str_len);
            int64_t svr_port = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SVR_PORT", svr_port, int64_t);
            snprintf(node_char, 64, "%s:%ld", svr_ip_char, svr_port);
            uint64_t tenant_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TENANT_ID", tenant_id, uint64_t);
            uint64_t group_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "GROUP_ID", group_id, uint64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "GROUP_TIME", group_time, int64_t);
            int64_t group_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "GROUP_RANK", group_rank, int64_t);
            int64_t program_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "PROGRAM_RANK", program_rank, int64_t);
            int64_t module_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "MODULE_RANK", module_rank, int64_t);
            int64_t action_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "ACTION_RANK", action_rank, int64_t);
            tmp_real_str_len = 0;
            char program_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "PROGRAM", program_char, 64, tmp_real_str_len);
            int64_t program_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "PROGRAM_TIME", program_time, int64_t);
            tmp_real_str_len = 0;
            char module_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "MODULE", module_char, 64, tmp_real_str_len);
            int64_t module_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "MODULE_TIME", module_time, int64_t);
            tmp_real_str_len = 0;
            char action_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ACTION", action_char, 64, tmp_real_str_len);
            int64_t action_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "ACTION_TIME", action_time, int64_t);

            uint64_t action_key = 0;
            if (OB_SUCC(ret)) {
              action_key = murmurhash(svr_ip_char, strlen(svr_ip_char), action_key);
              action_key = murmurhash(&svr_port, sizeof(svr_port), action_key);
              action_key = murmurhash(&tenant_id, sizeof(tenant_id), action_key);
              action_key = murmurhash(&group_id, sizeof(group_id), action_key);
              action_key = murmurhash(program_char, strlen(program_char), action_key);
              action_key = murmurhash(module_char, strlen(module_char), action_key);
              action_key = murmurhash(action_char, strlen(action_char), action_key);
            }

            if (group_rank > 1) {
              node_char[0] = '\0';
            }

            char group_path[128] = "";
            char group_radio_char[64] = "";

            if (OB_SUCC(ret) && 1 == program_rank) {
              if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("GCTX.cgroup_ctrl_ is nullptr", K(ret));
              } else if(OB_FAIL(GCTX.cgroup_ctrl_->get_group_path(group_path, 128, tenant_id, group_id))) {
                LOG_WARN("get group path failed", K(ret), K(tenant_id), K(group_id));
              } else {
                group_path[127] = '\0';
              }
              if (OB_SUCCESS != ret) {
                ret = OB_SUCCESS;
                snprintf(group_path, 128, "tenant:%ld/group:%ld", tenant_id, group_id);
              }
              calc_ratio(group_time, num_samples, group_radio_char);
            }
            if (program_rank > 1) {
              group_time = 0;
              ASH_FIELD_CHAR(group_time)[0] = '\0';
              group_path[0] = '\0';
            }

            char program_radio_char[64] = "";
            if (1 == module_rank) {
              calc_ratio(program_time, num_samples, program_radio_char);
            } else {
              program_char[0] = '\0';
            }

            char module_radio_char[64] = "";
            if (1 == action_rank) {
              calc_ratio(module_time, num_samples, module_radio_char);
            } else {
              module_char[0] = '\0';
            }

            char action_radio_char[64] = "";
            calc_ratio(action_time, num_samples, action_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(
                action_time, ash_report_params.get_elapsed_time() / 1000000, avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[column_size] = {
                  node_char,
                  group_path, ASH_FIELD_CHAR(group_time), group_radio_char,
                  program_char, program_radio_char,
                  module_char, module_radio_char,
                  action_char, action_radio_char,
                  AshColumnItem(action_key, avg_active_sessions_char)};
              column_content[10].foreign_section_name_ = "Top Groups";
              column_content[10].foreign_table_index_ = 2; //link with the secondary table
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
        if (OB_SUCC(ret) && got_data) {
          if (OB_FAIL(print_action_activity_over_time(ash_report_params, num_samples, buff))) {
            LOG_WARN("print action activity over time failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_action_activity_over_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {28, 13, 128, 20, 11, 20};
    AshColumnItem column_headers[column_size] = {
      "Slot Begin Time", "Slot Count", "Action Key",
      "Action Samples", "% Activity", "Avg Active Sessions"
    };
    column_headers[2].is_hidden_ = true;
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      const int64_t time_buf_len = 128;
      int64_t time_buf_pos = 0;
      char min_sample_time[time_buf_len] = "";
      char max_sample_time[time_buf_len] = "";
      const int DEFAULT_INTERVAL_TIME = get_active_time_window(ash_report_params.get_elapsed_time());
      AshColumnHeader headers(column_size, column_headers, column_widths);
      //By default, this table is not displayed.
      //Its display is triggered by a mouse click event in the HTML.
      headers.hide_table_ = true;
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(get_ash_bound_sql_time(ash_report_params,
                                                min_sample_time,
                                                time_buf_len,
                                                max_sample_time,
                                                time_buf_len))) {
        LOG_WARN("get ash bound sql time failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "WITH session_data AS ("
            "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, count_weight, "
                    "slot_begin_time, slot_end_time, %s AS interval_time "
            "FROM ("
              "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, count_weight, "
                      "CASE WHEN slot_begin_time < min_sample_time THEN min_sample_time ELSE slot_begin_time END AS slot_begin_time, "
                      "CASE WHEN slot_end_time > max_sample_time THEN max_sample_time ELSE slot_end_time END AS slot_end_time "
              "FROM ("
                "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, count_weight, "
                        "%s AS slot_begin_time, "
                        "%s AS slot_end_time, "
                        "%s AS min_sample_time, %s AS max_sample_time "
                "FROM ("
                  "SELECT %d AS interval_time, "
                          "svr_ip, svr_port, tenant_id, group_id, count_weight, "
                          "CASE WHEN program IS NULL THEN 'UNDEFINED' ELSE program END AS program, "
                          "CASE WHEN module IS NULL THEN 'UNDEFINED' ELSE module END AS module, "
                          "CASE WHEN action IS NULL THEN 'UNDEFINED' ELSE action END AS action, "
                          "%s AS utc_seconds "
                  "FROM (",
          lib::is_oracle_mode() ? "ROUND((slot_end_time - slot_begin_time) * 86400)"
                                : "UNIX_TIMESTAMP(slot_end_time) - UNIX_TIMESTAMP(slot_begin_time)", //%s AS interval_time
          lib::is_oracle_mode() ? "TO_DATE('1970-01-01') + (utc_seconds - MOD(utc_seconds, interval_time)) / 86400"
                                : "FROM_UNIXTIME(utc_seconds - (utc_seconds % interval_time))", //%s AS slot_begin_time
          lib::is_oracle_mode() ? "TO_DATE('1970-01-01') + (utc_seconds + interval_time - MOD(utc_seconds, interval_time)) / 86400"
                                : "FROM_UNIXTIME(utc_seconds + interval_time - (utc_seconds % interval_time))", //%s AS slot_end_time
          min_sample_time, max_sample_time, //%s AS min_sample_time, %s AS max_sample_time
          DEFAULT_INTERVAL_TIME, //%d AS interval_time
          lib::is_oracle_mode() ? "ROUND((CAST(sample_time AS DATE) - DATE '1970-01-01') * 86400)"
                                : "CAST(UNIX_TIMESTAMP(sample_time) AS SIGNED)") //%s AS utc_seconds
        )) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                  ") ta"
                ") tt"
              ") tg"
            ") ti"
          "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_action AS ("
            "SELECT slot_begin_time, interval_time, "
                    "svr_ip, svr_port, tenant_id, group_id, program, module, action, "
                    "SUM(count_weight) AS action_time, "
                    "ROW_NUMBER() OVER (PARTITION BY slot_begin_time ORDER BY SUM(count_weight) DESC) AS action_rank "
            "FROM session_data sd "
            "GROUP BY sd.svr_ip, sd.svr_port, sd.tenant_id, sd.group_id, sd.program, sd.module, "
                      "sd.action, sd.slot_begin_time, sd.interval_time "
          "), "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "action_key AS ("
            "SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action, "
                    "SUM(action_time) AS cnt "
            "FROM top_action "
            "GROUP BY svr_ip, svr_port, tenant_id, group_id, program, module, action "
            "HAVING SUM(action_time) >= %ld "
            "ORDER BY cnt DESC %s"
          "), ",
          num_samples / 100, //HAVING SUM(count_weight) >= %ld, drop %action < 1%
          lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //ORDER BY cnt DESC %s
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_slot AS ("
            "SELECT slot_begin_time, interval_time, SUM(action_time) AS slot_time "
            "FROM top_action ta "
            "GROUP BY ta.slot_begin_time, ta.interval_time"
          ")"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "SELECT "
            "CAST(ts.slot_begin_time AS CHAR(19)) AS SLOT_BEGIN_TIME, "
            "ts.interval_time AS INTERVAL_TIME, "
            "ts.slot_time AS SLOT_TIME, "
            "ta.svr_ip AS SVR_IP, ta.svr_port AS SVR_PORT, "
            "ta.tenant_id AS TENANT_ID, ta.group_id AS GROUP_ID, "
            "ta.program AS PROGRAM, ta.module AS MODULE, ta.action AS ACTION, "
            "ta.action_time AS ACTION_TIME, ta.action_rank AS ACTION_RANK "
          "FROM "
            "top_action ta JOIN top_slot ts "
              "ON ta.slot_begin_time = ts.slot_begin_time AND ta.interval_time = ts.interval_time "
          "WHERE (ta.svr_ip, ta.svr_port, ta.tenant_id, ta.group_id, ta.program, ta.module, ta.action) IN "
                  "(SELECT svr_ip, svr_port, tenant_id, group_id, program, module, action FROM action_key) "
          "ORDER BY ts.slot_begin_time, ts.interval_time, ta.action_rank"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char slot_begin_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SLOT_BEGIN_TIME", slot_begin_char, 64, tmp_real_str_len);
            int64_t interval_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "INTERVAL_TIME", interval_time, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SLOT_TIME", slot_count, int64_t);
            char svr_ip_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SVR_IP", svr_ip_char, 64, tmp_real_str_len);
            int64_t svr_port = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SVR_PORT", svr_port, int64_t);
            int64_t tenant_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TENANT_ID", tenant_id, int64_t);
            int64_t group_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "GROUP_ID", group_id, int64_t);
            char program_char[ASH_PROGRAM_STR_LEN + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "PROGRAM", program_char, ASH_PROGRAM_STR_LEN, tmp_real_str_len);
            char module_char[ASH_MODULE_STR_LEN + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "MODULE", module_char, ASH_MODULE_STR_LEN, tmp_real_str_len);
            char action_char[ASH_ACTION_STR_LEN + 3] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ACTION", action_char, ASH_ACTION_STR_LEN+1, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "ACTION_TIME", action_samples, int64_t);
            int64_t action_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "ACTION_RANK", action_rank, int64_t);

            uint64_t row_id = 0;
            if (OB_SUCC(ret)) {
              row_id = murmurhash(svr_ip_char, strlen(svr_ip_char), row_id);
              row_id = murmurhash(&svr_port, sizeof(svr_port), row_id);
              row_id = murmurhash(&tenant_id, sizeof(tenant_id), row_id);
              row_id = murmurhash(&group_id, sizeof(group_id), row_id);
              row_id = murmurhash(program_char, strlen(program_char), row_id);
              row_id = murmurhash(module_char, strlen(module_char), row_id);
              row_id = murmurhash(action_char, strlen(action_char), row_id);
            }

            if (OB_SUCC(ret)) {
              snprintf(slot_begin_char + strlen(slot_begin_char),
                       65 - strlen(slot_begin_char),
                       "(+%lds)", interval_time);
            }
            if (action_rank > 1 && !ash_report_params.is_html) {
              slot_begin_char[0] = '\0';
              ASH_FIELD_CHAR(slot_count)[0] = '\0';
            }
            char action_key[128] = "";
            if (OB_SUCC(ret)) {
              char group_path[128] = "";
              if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("GCTX.cgroup_ctrl_ is nullptr", K(ret));
              } else if(OB_FAIL(GCTX.cgroup_ctrl_->get_group_path(group_path, 128, tenant_id, group_id, ""))) {
                LOG_WARN("get group path failed", K(ret), K(tenant_id), K(group_id));
              } else {
                group_path[127] = '\0';
              }
              if (OB_SUCCESS != ret) {
                ret = OB_SUCCESS;
                snprintf(group_path, 128, "tenant:%ld/group:%ld", tenant_id, group_id);
              }
              snprintf(action_key, 128, "%s:%ld-%s-%s-%s-%s",
                       svr_ip_char, svr_port, group_path, program_char, module_char, action_char);
            }

            char action_radio_char[64] = "";
            calc_ratio(action_samples, slot_count, action_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(action_samples, interval_time, avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  slot_begin_char, ASH_FIELD_CHAR(slot_count), action_key,
                  ASH_FIELD_CHAR(action_samples), action_radio_char, avg_active_sessions_char};
              AshRowItem ash_row(column_size, row_id, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_latches(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top Latchs",
    "this section lists top latches",
    "Latch Wait Event: event that waiting for latch",
    "Event Samples: num of sampled session activity records",
    "% Activity: activity percentage for given event",
    "Avg Active Sessions: average active sessions during ash report analysis time period"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 4;
    const int64_t column_widths[column_size] = {64, 13, 11, 20};
    AshColumnItem column_headers[column_size] = {
        "Latch Wait Event", "Event Samples", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT %s AS EVENT, SUM(count_weight) AS CNT "
        "FROM (",
        lib::is_oracle_mode() ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64))"
                              : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64))"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
        ") ta "
        "WHERE ta.event like 'latch: %' "
        "GROUP BY ta.event_no, ta.event, ta.wait_class "
        "HAVING SUM(count_weight) > 1 "
        "ORDER BY cnt DESC "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "%s", lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(cnt, (ash_report_params.get_elapsed_time()) / 1000000, avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  event_char, ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int32_t ObDbmsWorkloadRepository::get_active_time_window(int64_t elapsed_time)
{
  int64_t elapsed_seconds = elapsed_time / 1000000;
  int time_window = elapsed_seconds / 10;
  if (time_window < 10) {
    //The minimum interval is 10 seconds.
    time_window = 10;
  } else if (time_window > 60) {
    int interval_minutes = static_cast<int>(round(static_cast<double>(time_window) / 60));
    time_window = interval_minutes * 60;
  }
  return time_window;
}

int ObDbmsWorkloadRepository::print_ash_activity_over_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {"Activity Over Time",
      "this section lists time slot information during the analysis period.",
      "The numbers in parentheses represent the time (in seconds) that has passed since the beginning and end stages.",
      "Slot Begin Time: current slot's begin time. current slot end with next slot begin time.",
      "Event Name: comprise wait event and on cpu event.",
      "Event Samples: num of sampled session activity records.",
      "% Activity: activity percentage for given event.",
      "Avg Active Sessions: average active sessions during ash report analysis time period."};
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 7;
    const int64_t column_widths[column_size] = {28, 13, 64, 20, 13, 11, 20};
    AshColumnItem column_headers[column_size] = {"Slot Begin Time", "Slot Count", "Event Name", "Wait Class",
        "Event Samples", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      const int64_t time_buf_len = 128;
      int64_t time_buf_pos = 0;
      char min_sample_time[time_buf_len] = "";
      char max_sample_time[time_buf_len] = "";
      int64_t query_time = ash_report_params.get_elapsed_time();
      int time_window = get_active_time_window(query_time);
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(get_ash_bound_sql_time(ash_report_params,
                                                min_sample_time,
                                                time_buf_len,
                                                max_sample_time,
                                                time_buf_len))) {
        LOG_WARN("get ash bound sql time failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "WITH session_data AS ("
            "SELECT event_no, event, wait_class, count_weight, slot_begin_time, slot_end_time, %s AS interval_time "
            "FROM ("
              "SELECT event_no, event, wait_class, count_weight, "
                      "CASE WHEN slot_begin_time < min_sample_time THEN min_sample_time ELSE slot_begin_time END AS slot_begin_time, "
                      "CASE WHEN slot_end_time > max_sample_time THEN max_sample_time ELSE slot_end_time END AS slot_end_time "
              "FROM ("
                "SELECT event_no, event, wait_class, count_weight, "
                        "%s AS slot_begin_time, "
                        "%s AS slot_end_time, "
                        "%s AS min_sample_time, %s AS max_sample_time "
                "FROM ("
                  "SELECT event_no, count_weight, %d AS interval_time, "
                          "CASE WHEN event_no = 0 THEN 'ON CPU' ELSE event END AS event, "
                          "CASE WHEN event_no = 0 THEN 'NULL' ELSE wait_class END AS wait_class, "
                          "%s AS utc_seconds "
                  "FROM (",
          lib::is_oracle_mode() ? "ROUND((slot_end_time - slot_begin_time) * 86400)"
                                : "UNIX_TIMESTAMP(slot_end_time) - UNIX_TIMESTAMP(slot_begin_time)", //%s AS interval_time
          lib::is_oracle_mode() ? "TO_DATE('1970-01-01') + (utc_seconds - MOD(utc_seconds, interval_time)) / 86400"
                                : "FROM_UNIXTIME(utc_seconds - (utc_seconds % interval_time))", //%s AS slot_begin_time
          lib::is_oracle_mode() ? "TO_DATE('1970-01-01') + (utc_seconds + interval_time - MOD(utc_seconds, interval_time)) / 86400"
                                : "FROM_UNIXTIME(utc_seconds + interval_time - (utc_seconds % interval_time))", //%s AS slot_end_time
          min_sample_time, max_sample_time, //%s AS min_sample_time, %s AS max_sample_time
          time_window, //%d AS interval_time
          lib::is_oracle_mode() ? "ROUND((CAST(sample_time AS DATE) - DATE '1970-01-01') * 86400)"
                                : "CAST(UNIX_TIMESTAMP(sample_time) AS SIGNED)") //%s AS utc_seconds
        )) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                  ") ta"
                ") tt"
              ") tg"
            ") ti"
          "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_event AS ("
            "SELECT slot_begin_time, interval_time, event, wait_class, "
                    "SUM(count_weight) AS event_time, "
                    "ROW_NUMBER() OVER (PARTITION BY slot_begin_time ORDER BY SUM(count_weight) DESC) AS event_rank "
            "FROM session_data sd "
            "GROUP BY sd.event, sd.wait_class, sd.slot_begin_time, sd.interval_time "
          "), "))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
          "top_slot AS ("
            "SELECT slot_begin_time, interval_time, SUM(event_time) AS slot_time "
            "FROM top_event te "
            "GROUP BY te.slot_begin_time, te.interval_time"
          ")"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          "SELECT "
            "CAST(ts.slot_begin_time AS CHAR(19)) AS SLOT_BEGIN_TIME, "
            "ts.interval_time AS INTERVAL_TIME, "
            "ts.slot_time AS SLOT_TIME, "
            "te.event AS EVENT, "
            "te.wait_class AS WAIT_CLASS, "
            "te.event_time AS EVENT_TIME, "
            "te.event_rank AS EVENT_RANK "
            "FROM "
              "top_event te JOIN top_slot ts "
                "ON te.slot_begin_time = ts.slot_begin_time AND te.interval_time = ts.interval_time "
            "WHERE te.event_rank <= 5 "
            "ORDER BY ts.slot_begin_time, ts.interval_time, te.event_rank "
            "%s", lib::is_oracle_mode() ? "FETCH FIRST 50 ROWS ONLY" : "LIMIT 50"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char slot_begin_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SLOT_BEGIN_TIME", slot_begin_char, 64, tmp_real_str_len);
            int64_t interval_time = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "INTERVAL_TIME", interval_time, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SLOT_TIME", slot_count, int64_t);
            char event_char[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_TIME", event_samples, int64_t);
            int64_t event_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "EVENT_RANK", event_rank, int64_t);

            if (event_rank > 1) {
              slot_begin_char[0] = '\0';
              ASH_FIELD_CHAR(slot_count)[0] = '\0';
            } else if (interval_time < time_window) {
              snprintf(slot_begin_char + strlen(slot_begin_char),
                       65 - strlen(slot_begin_char),
                       "(+%lds)", interval_time);
            }

            char event_radio_char[64] = "";
            calc_ratio(event_samples, slot_count, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(event_samples, interval_time, avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  slot_begin_char, ASH_FIELD_CHAR(slot_count),
                  event_char, wait_class_char,
                  ASH_FIELD_CHAR(event_samples), event_radio_char, avg_active_sessions_char};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
            with_color = !with_color;
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_db_time(
    const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top SQL with Top DB Time",
    "This Section lists the SQL statements that accounted for the highest percentages of sampled session activity.",
    "Plan Hash: Numeric representation of the current SQL plan.",
    "Total Samples: num of records during ash report analysis time period",
    "Wait Event Samples: num of records when session is on wait event",
    "On CPU Samples: num of records when session is on cpu",
    "% Activity: activity percentage for given event"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 7;
    const int64_t column_widths[column_size] = {40, 20, 18, 23, 19, 12, 64};
    AshColumnItem column_headers[column_size] = {"SQL ID", "Plan Hash", "Total Samples",
        "Wait Event Samples", "On CPU Samples", "% Activity", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append("SELECT SQL_ID, PLAN_HASH, QUERY_SQL, CPU_CNT, "
                                           "WAIT_CNT, CNT FROM (SELECT ash.*, "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(pc.QUERY_SQL, CHR(10), '''')), 0, 55) QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(QUERY_SQL, CHAR(10), '''')), 1, 55) AS QUERY_SQL  "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "FROM (SELECT SQL_ID, PLAN_HASH, %s, %s FROM (",
                     lib::is_oracle_mode()
                         ? "sum(decode(event_no, 0, COUNT_WEIGHT, 0)) as cpu_cnt, "
                           "sum(decode(event_no, 0, 0, COUNT_WEIGHT)) as wait_cnt"
                         : "cast(sum(if(event_no = 0, COUNT_WEIGHT, 0)) as signed integer) as cpu_cnt, "
                           "cast(sum(if(event_no = 0, 0, COUNT_WEIGHT)) as signed integer) as wait_cnt",
                     lib::is_oracle_mode() ? "SUM(COUNT_WEIGHT) AS CNT"
                                    : "CAST(SUM(COUNT_WEIGHT) as signed integer) AS CNT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and plan_hash is not null) top_event GROUP BY SQL_ID, "
                                           "PLAN_HASH having sum(count_weight) > 1) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(
                     sql_string.append_fmt("LEFT JOIN %s pc ON ash.sql_id = pc.sql_id AND "
                                           "ash.plan_hash = pc.plan_hash ORDER BY cnt DESC) v1 ",
                         lib::is_oracle_mode()
                             ? "(SELECT * FROM (SELECT SQL_ID, PLAN_HASH, QUERY_SQL, ROW_NUMBER() "
                               "OVER (PARTITION BY SQL_ID, PLAN_HASH ORDER BY SQL_ID, PLAN_HASH) AS N FROM "
                               "SYS.GV$OB_SQLSTAT) WHERE N = 1)"
                             : "(SELECT distinct SQL_ID, PLAN_HASH, QUERY_SQL "
                               "FROM oceanbase.GV$OB_SQLSTAT)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 30 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 30 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char sql_id[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 64, tmp_real_str_len);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {sql_id, ASH_FIELD_CHAR(plan_hash),
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  activity_radio_char, query_sql};
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_wait_events(
    const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top SQL with Top Events",
    "This Section lists the SQL statements that accounted for the highest percentages event.",
    "Plan Hash: Numeric representation of the current SQL plan",
    "Active Samples: num of samples for top current SQL",
    "% Activity: activity percentage for given SQL ID",
    "Sampled Executions: represents the number of times the current SQL execution has been sampled",
    "Top Event: top event name for current SQL plan",
    "% Event: activity percentage for current SQL plan",
    "Top Operator/ExecPhase: top operator name or execution phase for current event",
    "% Operator/ExecPhase: activity percentage for given operator",
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 10;
    const int64_t column_widths[column_size] = {40, 20, 20, 14, 20, 64, 14, 128, 24, 64};
    AshColumnItem column_headers[column_size] = {
      "SQL ID", "Plan Hash", "Active Samples", "% Activity", "Sampled Executions",
      "Top Event", "% Event", "Top Operator/ExecPhase", "% Operator/ExecPhase", "SQL Text"
    };
    ObArrayWrap<const char*> tm_cols_wrap(tm_columns, ARRAYSIZEOF(tm_columns));
    ObArrayWrap<int32_t> tm_flags_wrap(tm_flags, ARRAYSIZEOF(tm_columns));
    const uint64_t request_tenant_id = MTL_ID();
    HEAP_VARS_3((ObISQLClient::ReadResult, res), (ObSqlString, sql_string), (ObSqlString, tm_view))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "WITH session_data AS ("
          "SELECT sql_id, svr_ip, svr_port, session_id, trace_id, "
                  " plan_hash, sql_plan_line_id, event_no, %s AS event, time_model, count_weight, action "
          "FROM (",
        lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'ON CPU', event) AS VARCHAR2(64))"
                              : "CAST(IF (event_no = 0, 'ON CPU', event) AS CHAR(64)) "))) {
        LOG_WARN("append sql string failed", KR(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") tmp_ash), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          " session_data_fixup AS ( "
              " SELECT "
                  " sql_id, CASE WHEN (plan_hash IS NULL) THEN 0 ELSE plan_hash END AS plan_hash, "
                  " sql_plan_line_id, event_no, event, trace_id, time_model, count_weight  "
              " FROM ( "
                  " ( SELECT /*+use_hash(t2)*/ "
                      " FIRST_VALUE ( t1.sql_id ) IGNORE NULLS OVER ( PARTITION BY t1.svr_ip, t1.svr_port, t1.session_id, t1.trace_id ) AS sql_id, "
                      " FIRST_VALUE ( t1.plan_hash ) IGNORE NULLS OVER ( PARTITION BY t1.svr_ip, t1.svr_port, t1.session_id, t1.trace_id ) AS plan_hash, "
                      "t1.sql_plan_line_id, "
                      "t1.event_no, "
                      "t1.event, "
                      "t1.trace_id, "
                      "t1.time_model, "
                      "t1.count_weight, "
                      "t1.action"
                  " FROM "
                      " session_data t1"
                      "  JOIN ( "
                          " SELECT DISTINCT session_id, trace_id "
                          " FROM session_data  user_ash "
                          " WHERE %s > 0 AND session_id IS NOT NULL AND trace_id IS NOT NULL "
                      " ) t2 ON t1.session_id = t2.session_id AND t1.trace_id = t2.trace_id "
                  " ) UNION ALL ("
                  " SELECT /*+use_hash(t2)*/ "
                      " t1.sql_id, "
                      " t1.plan_hash, "
                      " t1.sql_plan_line_id, "
                      " t1.event_no, "
                      " t1.event, "
                      " t1.trace_id, "
                      " t1.time_model, "
                      " t1.count_weight, "
                      " t1.action "
                    " FROM "
                      " session_data t1 "
                      " LEFT JOIN ( "
                        " SELECT DISTINCT session_id, trace_id "
                        " FROM session_data  user_ash "
                        " WHERE %s > 0 AND session_id IS NOT NULL AND trace_id IS NOT NULL "
                        " ) t2 ON t1.session_id = t2.session_id AND t1.trace_id = t2.trace_id "
                    " WHERE t2.session_id IS NULL AND t2.trace_id IS NULL "
                  " ) ) "
              " WHERE sql_id is not null "
          " ), ",
          lib::is_oracle_mode() ? "BITAND(time_model, 3178509) "
                              : "(time_model & 3178509) ",
          lib::is_oracle_mode() ? "BITAND(time_model, 3178509) " 
                              : "(time_model & 3178509) "))) {
        LOG_WARN("failed to append sql string", K(ret)); 
      } else if (OB_FAIL(append_time_model_view_sql(
        tm_view,"sql_id, plan_hash, event_no", tm_cols_wrap, tm_flags_wrap, "session_data_fixup", true))) {
        LOG_WARN("append time model view sql failed", K(ret));
      } else if (OB_FAIL(tm_view.append(" GROUP BY sql_id, plan_hash, event_no"))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_exec_phase AS ("
          "SELECT sql_id, plan_hash, event_no, phase_name, phase_cnt "
          "FROM ("
            "SELECT sql_id, plan_hash, event_no, phase_name, phase_cnt, "
                    "ROW_NUMBER() OVER(PARTITION BY sql_id, plan_hash, event_no ORDER BY phase_cnt DESC) AS phase_rank "
                    "FROM ("))) {
        LOG_WARN("append sql string failed", K(ret));
      } else if (OB_FAIL(unpivot_time_model_column_sql(sql_string, "sql_id, plan_hash, event_no", tm_cols_wrap, tm_view.ptr()))) {
        LOG_WARN("append unpivot timemodel column sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(") tphr WHERE phase_cnt > 0) tep WHERE tep.phase_rank = 1), "))) {
        LOG_WARN("unpivot time model column sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_operator AS ("
          "SELECT sql_id, plan_hash, sql_plan_line_id, event_no, event, "
                  "SUM(count_weight) AS total_operator_samples, "
                  "ROW_NUMBER() OVER (PARTITION BY sd.sql_id, sd.plan_hash, sd.event_no ORDER BY SUM(count_weight) DESC) AS operator_rank "
          "FROM session_data_fixup sd "
          "GROUP BY sd.sql_id, sd.plan_hash, sd.sql_plan_line_id, sd.event_no, sd.event"
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_event AS ("
          "SELECT sql_id, plan_hash, event_no, event, "
                  "SUM(total_operator_samples) AS total_event_samples, "
                  "ROW_NUMBER() OVER (PARTITION BY sql_id, plan_hash "
                                     "ORDER BY SUM(total_operator_samples) DESC) AS event_rank "
          "FROM top_operator "
          "GROUP BY sql_id, plan_hash, event_no, event), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "top_plan AS ("
          "SELECT sql_id, plan_hash, total_plan_samples, exec_cnt, "
                 "ROW_NUMBER() OVER (ORDER BY total_plan_samples DESC) AS plan_rank "
          "FROM ( "
            "SELECT sql_id, plan_hash, SUM(count_weight) AS total_plan_samples, COUNT(DISTINCT trace_id) AS exec_cnt "
            "FROM session_data_fixup "
            "GROUP BY sql_id, plan_hash HAVING SUM(count_weight) >= %ld "
            "ORDER BY total_plan_samples DESC %s"
          ") "
        "), ",
          num_samples / 100, //HAVING SUM(count_weight) >=%ld, drop %% Activity < 1%
          lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //DESC %s
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_ash AS ("
          "SELECT sd.sql_id AS sql_id, sd.plan_hash AS plan_hash, "
            "sd.sql_plan_line_id AS sql_plan_line_id, "
            "sd.total_operator_samples AS total_operator_samples, "
            "te.event_no AS event_no, "
            "te.event AS event, "
            "te.total_event_samples AS total_event_samples, "
            "te.event_rank AS event_rank, "
            "tp.total_plan_samples AS total_plan_samples, "
            "tp.exec_cnt AS total_exec_cnt, "
            "tp.plan_rank AS plan_rank "
          "FROM top_operator sd "
                "JOIN top_event te "
                      "ON sd.sql_id = te.sql_id AND sd.plan_hash = te.plan_hash AND sd.event_no = te.event_no "
                "JOIN top_plan tp "
                      "ON sd.sql_id = tp.sql_id AND sd.plan_hash = tp.plan_hash "
          "WHERE te.event_rank <= 5 AND sd.operator_rank <= 1), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_text AS ( "
          "SELECT sql_id, %s AS query_sql FROM %s where query_sql %s "
          "UNION ALL "
          "SELECT sql_id, %s AS query_sql FROM %s where query_sql %s "
        "), ",
        lib::is_oracle_mode() ? "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHR(10), '''')), 0, 55) AS VARCHAR2(64))" : "query_sql",
        lib::is_oracle_mode() ? "sys.gv$ob_sqlstat " : "oceanbase.gv$ob_sqlstat ",
        lib::is_oracle_mode() ? "is not NULL " : "!= ''" ,
        lib::is_oracle_mode() ? "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHR(10), '''')), 0, 55) AS VARCHAR2(64))" : "query_sql",
        lib::is_oracle_mode() ? "sys.DBA_WR_SQLTEXT "
                              : is_sys_tenant(request_tenant_id) ? " oceanbase.CDB_WR_SQLTEXT " 
                                                                 : " oceanbase.DBA_WR_SQLTEXT ",
        lib::is_oracle_mode() ? "is not NULL " : "!= ''" 
      ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_plan AS ( "
          "SELECT sql_id, plan_hash, id, operator, object_alias FROM %s "
          "UNION ALL "
          "SELECT sql_id, plan_hash, id, operator, object_alias FROM %s "
        ")",
        lib::is_oracle_mode() ? "sys.gv$ob_sql_plan " : "oceanbase.gv$ob_sql_plan ",
        lib::is_oracle_mode() ? "sys.DBA_WR_SQL_PLAN "
                              : is_sys_tenant(request_tenant_id) ? " oceanbase.CDB_WR_SQL_PLAN " 
                                                                 : " oceanbase.DBA_WR_SQL_PLAN "
      ))){
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT CASE WHEN ash.event_rank > 1 THEN NULL ELSE ash.sql_id END AS SQL_ID, "
                "CASE WHEN ash.event_rank > 1 THEN NULL ELSE ash.plan_hash END AS PLAN_HASH, "
                "ash.event AS EVENT, "
                "ash.event_rank AS EVENT_RANK, "
                "ash.total_event_samples AS TOTAL_EVENT_SAMPLES, "
                "CASE WHEN ash.sql_plan_line_id IS NULL THEN tep.phase_name ELSE sp.operator END AS OPERATOR, "
                "CASE WHEN ash.sql_plan_line_id IS NULL THEN tep.phase_cnt "
                      "ELSE ash.total_operator_samples END AS TOTAL_OPERATOR_SAMPLES, "
                "ash.total_plan_samples AS TOTAL_PLAN_SAMPLES, "
                "CASE WHEN ash.event_rank > 1 THEN NULL ELSE ash.total_exec_cnt END AS TOTAL_EXEC_CNT, "
                "CASE WHEN ash.event_rank > 1 THEN NULL ELSE pc.query_sql END AS QUERY_SQL "
        "FROM top_ash ash "
              "LEFT JOIN ("
                   "SELECT DISTINCT sql_id, %s AS query_sql "
                   "FROM sql_text) pc "
              "ON ash.sql_id = pc.sql_id "
              "LEFT JOIN ("
                   "SELECT DISTINCT plan_hash, sql_id, id, "
                   "CASE WHEN object_alias = '' OR object_alias IS NULL THEN %s ELSE %s END AS operator  "
                   "FROM sql_plan) sp "
              "ON ash.sql_id = sp.sql_id AND ash.plan_hash = sp.plan_hash AND ash.sql_plan_line_id = sp.id "
              "LEFT JOIN top_exec_phase tep "
              "ON ash.sql_id = tep.sql_id AND ash.plan_hash = tep.plan_hash AND ash.event_no = tep.event_no "
        "ORDER BY ash.plan_rank ASC, ash.event_rank ASC",
        lib::is_oracle_mode() ? "query_sql" //plan_hash, %s AS query_sql
                              : "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHAR(10), '''')), 1, 55) AS CHAR(64))",
        lib::is_oracle_mode() ? "id || ':' || operator" : "CONCAT(id, ':', operator)", //CASE WHEN object_alias...THEN %s ...
        lib::is_oracle_mode() ? "id || ':' || operator || ':' || object_alias" : "CONCAT(id, ':', operator, ':', object_alias)" //CASE WHEN object_alias...ELSE %s END
        ))) { //FROM %s) sp
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char sql_id[64] = "";
            uint64_t event_rank = 0;
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_PLAN_SAMPLES", total_plan_samples, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_EXEC_CNT", total_exec_cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "EVENT_RANK", event_rank, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(total_plan_samples, num_samples, activity_radio_char);
            if (event_rank > 1) {
              //When event_rank > 1,
              //plan_hash and total_plan_samples correspond to default value strings,
              //cleanup meaningless results.
              ASH_FIELD_CHAR(total_plan_samples)[0] = '\0';
              total_plan_samples = 0;
              ASH_FIELD_CHAR(total_exec_cnt)[0] = '\0';
              total_exec_cnt = 0;
              ASH_FIELD_CHAR(plan_hash)[0] = '\0';
              plan_hash = 0;
              activity_radio_char[0] = '\0';
            }

            tmp_real_str_len = 0;
            char operator_char[256] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "OPERATOR", operator_char, 255, tmp_real_str_len);
            if (tmp_real_str_len <= 0) {
              snprintf(operator_char, 256, "UNDEFINED");
            }
            int64_t total_operator_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_OPERATOR_SAMPLES", total_operator_samples, int64_t);
            char operator_radio_char[64] = "";
            calc_ratio(total_operator_samples, num_samples, operator_radio_char);

            tmp_real_str_len = 0;
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "EVENT", event_char, 64, tmp_real_str_len);
            int64_t total_event_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_EVENT_SAMPLES", total_event_samples, int64_t);
            char event_radio_char[64] = "";
            calc_ratio(total_event_samples, num_samples, event_radio_char);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 64, tmp_real_str_len);
            if (event_rank <= 1 && tmp_real_str_len <= 0) {
              snprintf(query_sql, 256, "UNDEFINED");
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  sql_id, ASH_FIELD_CHAR(plan_hash), ASH_FIELD_CHAR(total_plan_samples), activity_radio_char,
                  ASH_FIELD_CHAR(total_exec_cnt),
                  event_char, event_radio_char,
                  operator_char, operator_radio_char,
                  query_sql};
              if (column_content[0].column_content_[0] != '\0'
                  && 0 != strcasecmp(column_content[0].column_content_, "UNDEFINED")
                  && column_content[9].column_content_[0] != '\0'
                  && 0 != strcasecmp(column_content[9].column_content_, "UNDEFINED")) {
                column_content[0].href_sql_id_ = true;
              }
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_operator(
    const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top SQL with Top Operator",
    "This Section lists the SQL statements that accounted for the highest percentages of sampled session activity with sql operator",
    "Plan Hash: Numeric representation of the current SQL plan",
    "Active Samples: num of samples for top current SQL",
    "% Activity: activity percentage for given SQL ID",
    "Sampled Executions: represents the number of times the current SQL execution has been sampled",
    "Top Operator: top operator name for current SQL plan",
    "% Operator: activity percentage for given operator",
    "Top Event: top event name for current operator",
    "% Event: activity percentage for given event"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 10;
    const int64_t column_widths[column_size] = {40, 20, 14, 14, 20, 128, 14, 64, 14, 64};
    AshColumnItem column_headers[column_size] = {
      "SQL ID", "Plan Hash", "Active Samples", "% Activity", "Sampled Executions",
      "Top Operator", "% Operator", "Top Event", "% Event", "SQL Text"
    };
    const uint64_t request_tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "WITH session_data AS ("
          "SELECT sql_id, plan_hash, sql_plan_line_id, event_no, %s AS event, trace_id, count_weight, svr_ip, svr_port,session_id "
          "FROM (",
        lib::is_oracle_mode() ? "CAST(DECODE(event_no, 0, 'ON CPU', event) AS VARCHAR2(64))"
                              : "CAST(IF (event_no = 0, 'ON CPU', event) AS CHAR(64)) "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") t where sql_plan_line_id IS NOT NULL ), " ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "top_event AS ("
          "SELECT sql_id, plan_hash, sql_plan_line_id, event_no, event, "
                  "SUM(count_weight) AS total_event_samples, "
                  "ROW_NUMBER() OVER (PARTITION BY sql_id, plan_hash, sql_plan_line_id ORDER BY SUM(count_weight) DESC) AS event_rank "
          "FROM session_data sd "
          "GROUP BY sd.sql_id, sd.plan_hash, sd.sql_plan_line_id, sd.event_no, sd.event"
        "), "))) {
        LOG_WARN("append top event string failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_operator AS ("
          "SELECT sql_id, plan_hash, sql_plan_line_id, "
                  "SUM(total_event_samples) AS total_operator_samples, "
                  "ROW_NUMBER() OVER (PARTITION BY sql_id, plan_hash "
                                     "ORDER BY SUM(total_event_samples) DESC) AS operator_rank "
          "FROM top_event "
          "GROUP BY sql_id, plan_hash, sql_plan_line_id"
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "top_plan AS ("
          "SELECT sql_id, plan_hash, total_plan_samples, total_exec_cnt, "
                "ROW_NUMBER() OVER (ORDER BY total_plan_samples DESC) AS plan_rank "
          "FROM ( "
            "SELECT sql_id, plan_hash, SUM(count_weight) AS total_plan_samples, COUNT(DISTINCT trace_id) AS total_exec_cnt "
            "FROM session_data "
            "GROUP BY sql_id, plan_hash HAVING SUM(count_weight) >= %ld "
            "ORDER BY total_plan_samples DESC %s"
          ") "
        "), ",
        num_samples / 100, //HAVING SUM(count_weight) >= %ld, drop %Activity < 1%
        lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"))) { //DESC %s
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "top_ash AS ("
          "SELECT te.sql_id AS sql_id, te.plan_hash AS plan_hash, "
            "te.sql_plan_line_id AS sql_plan_line_id, "
            "te.event AS event, "
            "te.total_event_samples AS total_event_samples, "
            "tsp.total_operator_samples AS total_operator_samples, "
            "tsp.operator_rank AS operator_rank, "
            "tp.total_plan_samples AS total_plan_samples, "
            "tp.total_exec_cnt AS total_exec_cnt, "
            "tp.plan_rank AS plan_rank "
          "FROM top_event te "
                "JOIN top_operator tsp "
                      "ON te.sql_id = tsp.sql_id AND te.plan_hash = tsp.plan_hash AND te.sql_plan_line_id = tsp.sql_plan_line_id "
                "JOIN top_plan tp "
                      "ON te.sql_id = tp.sql_id AND te.plan_hash = tp.plan_hash "
          "WHERE tsp.operator_rank <= 5 AND te.event_rank <= 1"
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_text AS ( "
          "SELECT sql_id, %s AS query_sql FROM %s where query_sql %s "
          "UNION ALL "
          "SELECT sql_id, %s AS query_sql FROM %s where query_sql %s "
        "), ",
        lib::is_oracle_mode() ? "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHR(10), '''')), 0, 55) AS VARCHAR2(64))" : "query_sql",
        lib::is_oracle_mode() ? "sys.gv$ob_sqlstat " : "oceanbase.gv$ob_sqlstat ",
        lib::is_oracle_mode() ? "is not NULL " : "!= ''" ,
        lib::is_oracle_mode() ? "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHR(10), '''')), 0, 55) AS VARCHAR2(64))" : "query_sql",
        lib::is_oracle_mode() ? "sys.DBA_WR_SQLTEXT "
                              : is_sys_tenant(request_tenant_id) ? " oceanbase.CDB_WR_SQLTEXT " 
                                                                 : " oceanbase.DBA_WR_SQLTEXT ",
        lib::is_oracle_mode() ? "is not NULL " : "!= ''" 
      ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_plan AS ( "
          "SELECT sql_id, plan_hash, id, operator, object_alias FROM %s "
          "UNION ALL "
          "SELECT sql_id, plan_hash, id, operator, object_alias FROM %s "
        ")",
        lib::is_oracle_mode() ? "sys.gv$ob_sql_plan " : "oceanbase.gv$ob_sql_plan ",
        lib::is_oracle_mode() ? "sys.DBA_WR_SQL_PLAN "
                              : is_sys_tenant(request_tenant_id) ? " oceanbase.CDB_WR_SQL_PLAN " 
                                                                 : " oceanbase.DBA_WR_SQL_PLAN "
      ))){
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT CASE WHEN ash.operator_rank > 1 THEN NULL ELSE ash.sql_id END AS SQL_ID, "
                "CASE WHEN ash.operator_rank > 1 THEN NULL ELSE ash.plan_hash END AS PLAN_HASH, "
                "ash.event AS EVENT, "
                "ash.operator_rank AS OPERATOR_RANK, "
                "ash.total_event_samples AS TOTAL_EVENT_SAMPLES, "
                "ash.total_operator_samples AS TOTAL_OPERATOR_SAMPLES, "
                "ash.total_plan_samples AS TOTAL_PLAN_SAMPLES, "
                "CASE WHEN ash.operator_rank > 1 THEN NULL ELSE ash.total_exec_cnt END AS TOTAL_EXEC_CNT, "
                "sp.operator AS OPERATOR, "
                "CASE WHEN ash.operator_rank > 1 THEN NULL ELSE pc.query_sql END AS QUERY_SQL "
        "FROM top_ash ash "
              "LEFT JOIN ("
                   "SELECT DISTINCT sql_id, %s AS query_sql "
                   "FROM sql_text) pc "
              "ON ash.sql_id = pc.sql_id "
              "LEFT JOIN ("
                   "SELECT DISTINCT plan_hash, sql_id, id, "
                   "CASE WHEN object_alias = '' OR object_alias IS NULL THEN %s ELSE %s END AS operator "
                   "FROM sql_plan) sp "
              "ON ash.sql_id = sp.sql_id AND ash.plan_hash = sp.plan_hash AND ash.sql_plan_line_id = sp.id "
        "ORDER BY ash.plan_rank ASC, ash.operator_rank ASC",
        lib::is_oracle_mode() ? "query_sql " //plan_hash, %s AS query_sql
                              : "CAST(SUBSTR(TRIM(REPLACE(query_sql, CHAR(10), '''')), 1, 55) AS CHAR(64))",
        lib::is_oracle_mode() ? "id || ':' || operator" : "CONCAT(id, ':', operator)", //CASE WHEN object_alias...THEN %s ...
        lib::is_oracle_mode() ? "id || ':' || operator || ':' || object_alias" : "CONCAT(id, ':', operator, ':', object_alias)" //CASE WHEN object_alias...ELSE %s END
        ))) { //FROM %s) sp
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char sql_id[64] = "";
            uint64_t operator_rank = 0;
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_PLAN_SAMPLES", total_plan_samples, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_EXEC_CNT", total_exec_cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "OPERATOR_RANK", operator_rank, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(total_plan_samples, num_samples, activity_radio_char);
            if (operator_rank > 1) {
              //When operator_rank > 1,
              //plan_hash and total_plan_samples correspond to default value strings,
              //cleanup meaningless results.
              ASH_FIELD_CHAR(total_plan_samples)[0] = '\0';
              total_plan_samples = 0;
              ASH_FIELD_CHAR(total_exec_cnt)[0] = '\0';
              total_exec_cnt = 0;
              ASH_FIELD_CHAR(plan_hash)[0] = '\0';
              plan_hash = 0;
              activity_radio_char[0] = '\0';
            }

            tmp_real_str_len = 0;
            char operator_char[256] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "OPERATOR", operator_char, 255, tmp_real_str_len);
            if (tmp_real_str_len <= 0) {
              snprintf(operator_char, 256, "UNDEFINED");
            }
            int64_t total_operator_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_OPERATOR_SAMPLES", total_operator_samples, int64_t);
            char operator_radio_char[64] = "";
            calc_ratio(total_operator_samples, num_samples, operator_radio_char);

            tmp_real_str_len = 0;
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "EVENT", event_char, 64, tmp_real_str_len);
            int64_t total_event_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "TOTAL_EVENT_SAMPLES", total_event_samples, int64_t);
            char event_radio_char[64] = "";
            calc_ratio(total_event_samples, num_samples, event_radio_char);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 64, tmp_real_str_len);
            if (operator_rank <= 1 && tmp_real_str_len <= 0) {
              snprintf(query_sql, 256, "UNDEFINED");
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                  sql_id, ASH_FIELD_CHAR(plan_hash), ASH_FIELD_CHAR(total_plan_samples), activity_radio_char, ASH_FIELD_CHAR(total_exec_cnt),
                  operator_char, operator_radio_char,
                  event_char, event_radio_char,
                  query_sql};
              if (column_content[0].column_content_[0] != '\0'
                  && 0 != strcasecmp(column_content[0].column_content_, "UNDEFINED")
                  && column_content[9].column_content_[0] != '\0'
                  && 0 != strcasecmp(column_content[9].column_content_, "UNDEFINED")) {
                column_content[0].href_sql_id_ = true;
              }
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_command_type(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top SQL Statement Types",
    "this section lists top sql statement type.",
    "SQL Statement Type: SQL statement types such as SELECT or UPDATE",
    "Total Samples: num of records during ash report analysis time period",
    "% Event: activity percentage of records when session is on wait event",
    "% On CPU: activity percentage of records when session is on cpu",
    "% Activity: activity(cpu + wait) percentage for given tenant",
    "Node: the server address where the statement was executed"
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 8;
    const int64_t column_widths[column_size] = {45, 21, 21, 21, 64, 21, 21, 21};
    AshColumnItem column_headers[column_size] = {
      "SQL Statement Type", "Total Samples", "% Activity", "Sampled Executions",
      "Node", "% Node", "% On CPU", "% Event"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "WITH session_data AS ("
          "SELECT sql_id, stmt_type, event_no, count_weight, svr_ip, svr_port, trace_id,session_id, time_model "
          "FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") ash_wr"
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
          " session_data_fixup AS ( "
            " SELECT sql_id, stmt_type, event_no, count_weight, svr_ip, svr_port, trace_id FROM "
              " ( SELECT /*+use_hash(t2)*/ "
                " FIRST_VALUE ( t1.sql_id ) IGNORE NULLS OVER ( PARTITION BY t1.svr_ip, t1.svr_port, t1.session_id, t1.trace_id ) AS sql_id, "
                " t1.stmt_type as stmt_type, "
                " t1.event_no as event_no, "
                " t1.count_weight as count_weight, "
                " t1.svr_ip AS svr_ip, "
                " t1.svr_port AS svr_port, "
                " t1.trace_id as trace_id "
              " FROM "
              " session_data t1"
              "  JOIN ( "
                  " SELECT DISTINCT session_id, trace_id "
                  " FROM session_data  user_ash "
                  " WHERE %s > 0 AND session_id IS NOT NULL AND trace_id IS NOT NULL "
                " ) t2 ON t1.session_id = t2.session_id AND t1.trace_id = t2.trace_id )"
              " UNION ALL "
              " ( SELECT /*+use_hash(t2)*/ "
                " t1.sql_id, "
                " t1.stmt_type, "
                " t1.event_no, "
                " t1.count_weight, "
                " t1.svr_ip, "
                " t1.svr_port, "
                " t1.trace_id "
              " FROM "
                " session_data t1 "
                " LEFT JOIN ( "
                  " SELECT DISTINCT session_id, trace_id "
                  " FROM session_data  user_ash "
                  " WHERE %s > 0 AND session_id IS NOT NULL AND trace_id IS NOT NULL "
                  " ) t2 ON t1.session_id = t2.session_id AND t1.trace_id = t2.trace_id "
              " WHERE t2.session_id IS NULL AND t2.trace_id IS NULL )"
            " ), ",
            lib::is_oracle_mode() ? "BITAND(time_model, 3178509) "
                                : "(time_model & 3178509) ",
            lib::is_oracle_mode() ? "BITAND(time_model, 3178509) " 
                      : "(time_model & 3178509) "))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "sql2stmt AS ("
          "SELECT sql_id, stmt_type "
          "FROM ("
            "SELECT sql_id, stmt_type, "
                    "ROW_NUMBER() OVER ("
                      "PARTITION BY sql_id "
                      "ORDER BY SUM(count_weight) DESC"
                    ") AS stmt_rank "
            "FROM session_data_fixup sd "
            "WHERE sql_id IS NOT NULL AND stmt_type IS NOT NULL "
            "GROUP BY sd.sql_id, sd.stmt_type "
          ") tmp_sql "
          "WHERE stmt_rank = 1"
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "stmt_info AS ("
          "SELECT s2s.stmt_type AS stmt_type, sd.event_no AS event_no, "
                  "sd.count_weight AS count_weight, sd.trace_id AS trace_id, "
                  "%s AS node "
          "FROM session_data_fixup sd "
          "LEFT JOIN sql2stmt s2s "
            "ON sd.sql_id = s2s.sql_id "
        ") ",
        lib::is_oracle_mode() ? "sd.svr_ip||':'||sd.svr_port" : "CONCAT(sd.svr_ip, ':', sd.svr_port)" //%s AS node
        ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT STMT_TYPE, TOTAL_SAMPLES, SAMPLED_EXECUTIONS, "
                "NODE, NODE_SAMPLES, NODE_RANK, "
                "CPU_SAMPLES, WAIT_SAMPLES "
        "FROM ("
          "SELECT stmt_type, node, "
                  "SUM(count_weight) AS node_samples, "
                  "COUNT(DISTINCT trace_id) AS sampled_executions, "
                  "SUM(CASE WHEN event_no = 0 THEN count_weight ELSE 0 END) AS cpu_samples, "
                  "SUM(CASE WHEN event_no = 0 THEN 0 ELSE count_weight END) AS wait_samples, "
                  "SUM(SUM(count_weight)) OVER("
                    "PARTITION BY stmt_type "
                    "ORDER BY NULL "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                  ") AS total_samples, "
                  "ROW_NUMBER() OVER(PARTITION BY stmt_type ORDER BY SUM(count_weight) DESC) AS node_rank "
          "FROM stmt_info si "
          "GROUP BY stmt_type, node "
          "ORDER BY node_samples DESC, node_rank ASC %s"
        ") top_stmt"
        " ORDER BY total_samples DESC, node_rank ASC",
        lib::is_oracle_mode() ? "FETCH FIRST 15 ROWS ONLY" : "LIMIT 15"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            int64_t stmt_type = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "STMT_TYPE", stmt_type, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_SAMPLES", total_samples, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SAMPLED_EXECUTIONS", sampled_executions, int64_t);
            char node_addr[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "NODE", node_addr, 64, tmp_real_str_len);
            int64_t node_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "NODE_SAMPLES", node_samples, int64_t);
            uint64_t node_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "NODE_RANK", node_rank, int64_t);
            int64_t cpu_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "CPU_SAMPLES", cpu_samples, int64_t);
            int64_t wait_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "WAIT_SAMPLES", wait_samples, int64_t);


            char activity_radio_char[64] = "";
            const char *stmt_type_str =
              ObResolverUtils::get_stmt_type_string(static_cast<stmt::StmtType>(stmt_type)).ptr();
            calc_ratio(total_samples, num_samples, activity_radio_char);
            if (node_rank > 1) {
              stmt_type_str = "";
              ASH_FIELD_CHAR(total_samples)[0] = '\0';
              ASH_FIELD_CHAR(sampled_executions)[0] = '\0';
              activity_radio_char[0] = '\0';
            }
            char node_radio_char[64] = "";
            char cpu_radio_char[64] = "";
            char wait_radio_char[64] = "";
            calc_ratio(node_samples, num_samples, node_radio_char);
            calc_ratio(cpu_samples, num_samples, cpu_radio_char);
            calc_ratio(wait_samples, num_samples, wait_radio_char);

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                stmt_type_str, ASH_FIELD_CHAR(total_samples), activity_radio_char, ASH_FIELD_CHAR(sampled_executions),
                node_addr, node_radio_char, cpu_radio_char, wait_radio_char
              };
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_plsql(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contens[] = {
    "Top PL/SQL Procedures",
    "\"PL/SQL Entry Subprogram\" represents the application's top-level entry-point(procedure, function, trigger, package initialization) into PL/SQL.",
    "\"PL/SQL Current Subprogram\" is the pl/sql subprogram being executed at the point of sampling. "
                                  "If the value is \"SQL\", "
                                  "it represents the percentage of time spent executing SQL for the particular plsql entry subprogram.",
    "\"PL/SQL Entry Subprogram\" represents the application's top-level subprogram name"
  };
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (data_version < DATA_VERSION_4_2_2_0 || 
              (DATA_VERSION_4_3_0_0 <= data_version && data_version < DATA_VERSION_4_3_5_0)) {
    // do not support.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contens, 4))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 4;
    const int64_t column_widths[column_size] = {60, 20, 60, 20};
    AshColumnItem column_headers[column_size] = {
        "PLSQL Entry Subprogram", "% Activity", "PLSQL Current Subprogram", "% Current"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "WITH session_data AS ("
                     "SELECT plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "plsql_entry_subprogram_name, "
                     "plsql_object_id, plsql_subprogram_id, plsql_subprogram_name, "
                     "%s"
                     "time_model, count_weight "
                     "FROM (",
                     lib::is_oracle_mode()
                         ? "CAST(CASE WHEN BITAND(TIME_MODEL , 2048) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PLSQL_COMPILATION, "
                         "CAST(CASE WHEN BITAND(TIME_MODEL , 4096) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_PLSQL_EXECUTION, "
                         "CAST(CASE WHEN BITAND(TIME_MODEL , 16) > 0 THEN 'Y' ELSE 'N' END  AS VARCHAR2(1)) AS IN_SQL_EXECUTION, "
                         : "CAST(CASE WHEN (TIME_MODEL & 2048) > 0 THEN 'Y' ELSE 'N' END AS CHAR(1)) AS IN_PLSQL_COMPILATION, "
                         "CAST(CASE WHEN (TIME_MODEL & 4096) > 0 THEN 'Y' ELSE 'N' END AS CHAR(1)) AS IN_PLSQL_EXECUTION, "
                         "CAST(CASE WHEN (TIME_MODEL & 16) > 0 THEN 'Y' ELSE 'N' END AS CHAR(1)) AS IN_SQL_EXECUTION, "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") ash_wr "
                                           "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "top_plsql_entry AS ("
                     "SELECT plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "plsql_entry_subprogram_name, "
                     "SUM(count_weight) AS entry_cnt "
                     "FROM session_data sd "
                     "GROUP BY sd.plsql_entry_object_id, sd.plsql_entry_subprogram_id, "
                     "sd.plsql_entry_subprogram_name "
                     "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "top_pl_subprogram AS ("
                     "SELECT plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "plsql_object_id, plsql_subprogram_id, plsql_subprogram_name, "
                     "SUM(count_weight) AS subprogram_cnt "
                     "FROM session_data sd "
                     "WHERE plsql_object_id > 0 "
                     "AND (IN_PLSQL_COMPILATION = 'Y' OR IN_PLSQL_EXECUTION = 'Y') "
                     "AND (%s) "
                     "GROUP BY plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "plsql_object_id, plsql_subprogram_id, plsql_subprogram_name "
                     "), ",
                     lib::is_oracle_mode()
                         ? "BITAND(time_model, 4096) > 0 OR BITAND(time_model, 2048) > 0"
                         : "(time_model & 4096) > 0 OR (time_model & 2048) > 0"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "top_sql_subprogram AS ("
                     "SELECT plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "NULL AS plsql_object_id, NULL AS plsql_subprogram_id, 'SQL' AS "
                     "plsql_subprogram_name, "
                     "SUM(count_weight) AS subprogram_cnt "
                     "FROM session_data sd "
                     "WHERE IN_PLSQL_COMPILATION = 'N' AND IN_PLSQL_EXECUTION = 'N' AND "
                     "IN_SQL_EXECUTION = 'Y' "
                     "AND (%s) "
                     "GROUP BY plsql_entry_object_id, plsql_entry_subprogram_id "
                     "), ",
                     lib::is_oracle_mode() ? "BITAND(time_model, 16) > 0 AND BITAND(time_model, "
                                             "4096) = 0 AND BITAND(time_model, 2048) = 0"
                                           : "(time_model & 16) > 0 AND (time_model & 4096) = 0 "
                                             "AND (time_model & 2048) = 0"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "top_subprogram AS ("
                     "SELECT plsql_entry_object_id, plsql_entry_subprogram_id, "
                     "plsql_object_id, plsql_subprogram_id, plsql_subprogram_name, subprogram_cnt, "
                     "ROW_NUMBER() OVER (PARTITION BY plsql_entry_object_id, "
                     "plsql_entry_subprogram_id ORDER BY subprogram_cnt DESC) AS subprogram_rank "
                     "FROM ("
                     "SELECT * FROM top_pl_subprogram "
                     "UNION ALL "
                     "SELECT * FROM top_sql_subprogram "
                     ") tmp_subprogram "
                     "ORDER BY subprogram_cnt DESC %s "
                     "), ",
                     lib::is_oracle_mode() ? "FETCH FIRST 50 ROWS ONLY" : "LIMIT 50"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt("pl_object AS ("
                                               "SELECT db.database_name AS owner, r.routine_name "
                                               "AS object_name, r.routine_id AS object_id "
                                               "FROM %s db, %s r "
                                               "WHERE r.database_id = db.database_id "
                                               "UNION ALL "
                                               "SELECT db.database_name AS owner, p.package_name "
                                               "AS object_name, p.package_id AS object_id "
                                               "FROM %s db, %s p "
                                               "WHERE p.database_id = db.database_id "
                                               "UNION ALL "
                                               "SELECT db.database_name AS owner, t.type_name AS "
                                               "object_name, t.type_id AS object_id "
                                               "FROM %s db, %s t "
                                               "WHERE t.database_id = db.database_id "
                                               "UNION ALL "
                                               "SELECT db.database_name AS owner, trg.trigger_name "
                                               "AS object_name, trg.trigger_id AS object_id "
                                               "FROM %s db, %s trg "
                                               "WHERE trg.database_id = db.database_id "
                                               "%s"
                                               ") ",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT"
                                           : "oceanbase.__all_routine",  // FROM %s db, %s r
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT"
                                           : "oceanbase.__all_package",  // FROM %s db, %s p
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_TYPE_REAL_AGENT"
                                           : "oceanbase.__all_type",  // FROM %s db, %s t
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode()
                         ? "SYS.ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT"
                         : "oceanbase.__all_tenant_trigger",  // FROM %s db, %s trg
                     lib::is_oracle_mode() ? "UNION ALL "
                                             "SELECT db.database_name AS owner, ps.package_name AS "
                                             "object_name, ps.package_id AS object_id "
                                             "FROM SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT db, "
                                             "SYS.ALL_VIRTUAL_PACKAGE_SYS_AGENT ps "
                                             "WHERE ps.database_id = db.database_id "
                                           : ""  // oracle mode from db, sp
                     ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "SELECT %s AS ENTRY_OBJECT_OWNER, %s AS ENTRY_OBJECT_NAME, "
              "te.plsql_entry_subprogram_name AS ENTRY_SUBPROGRAM_NAME, "
              "te.plsql_entry_object_id AS ENTRY_OBJECT_ID, "
              "te.plsql_entry_subprogram_id AS ENTRY_SUBPROGRAM_ID, "
              "te.entry_cnt AS ENTRY_CNT, "
              "%s AS SUB_OBJECT_OWNER, %s AS SUB_OBJECT_NAME, "
              "ts.plsql_subprogram_name AS SUBPROGRAM_NAME, "
              "ts.plsql_object_id AS SUB_OBJECT_ID, "
              "ts.plsql_subprogram_id AS SUB_PROGRAM_ID, "
              "ts.subprogram_cnt AS SUBPROGRAM_CNT, "
              "ts.subprogram_rank AS SUBPROGRAM_RANK "
              "FROM top_plsql_entry te "
              "LEFT JOIN pl_object obj "
              "ON te.plsql_entry_object_id = obj.object_id "
              "JOIN top_subprogram ts "
              "ON ts.plsql_entry_object_id = te.plsql_entry_object_id "
              "AND ts.plsql_entry_subprogram_id = te.plsql_entry_subprogram_id "
              "LEFT JOIN pl_object sub_obj "
              "ON ts.plsql_object_id = sub_obj.object_id "
              "ORDER BY te.entry_cnt DESC, ts.subprogram_rank",
              lib::is_oracle_mode()
                  ? "obj.owner"
                  : "nvl(obj.owner, 'oceanbase')",  // sys package under MySQL mode cannot obtain
                                                    // the owner name
              lib::is_oracle_mode()
                  ? "obj.object_name"
                  : "nvl(obj.object_name, mysql_proc_info(te.plsql_entry_object_id, 'name'))",
              lib::is_oracle_mode() ? "sub_obj.owner" : "nvl(sub_obj.owner, 'oceanbase')",
              lib::is_oracle_mode()
                  ? "sub_obj.object_name"
                  : "nvl(sub_obj.object_name, mysql_proc_info(ts.plsql_object_id, 'name'))"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char entry_obj_owner[common::OB_MAX_DATABASE_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ENTRY_OBJECT_OWNER", entry_obj_owner,
                common::OB_MAX_DATABASE_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char entry_obj_name[common::OB_MAX_ROUTINE_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "ENTRY_OBJECT_NAME", entry_obj_name, common::OB_MAX_ROUTINE_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char entry_subprogram_name[common::OB_MAX_ROUTINE_NAME_LENGTH + 1] = "\0";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ENTRY_SUBPROGRAM_NAME", entry_subprogram_name,
                common::OB_MAX_ROUTINE_NAME_LENGTH, tmp_real_str_len);

            int64_t entry_object_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "ENTRY_OBJECT_ID", entry_object_id, int64_t);

            int64_t entry_subprogram_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "ENTRY_SUBPROGRAM_ID", entry_subprogram_id, int64_t);
            int64_t event_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "ENTRY_CNT", event_cnt, int64_t);

            char sub_obj_owner[common::OB_MAX_DATABASE_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SUB_OBJECT_OWNER", sub_obj_owner,
                common::OB_MAX_DATABASE_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char sub_obj_name[common::OB_MAX_ROUTINE_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "SUB_OBJECT_NAME", sub_obj_name, common::OB_MAX_ROUTINE_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char subprogram_name[common::OB_MAX_ROUTINE_NAME_LENGTH + 1] = "\0";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SUBPROGRAM_NAME", subprogram_name,
                common::OB_MAX_ROUTINE_NAME_LENGTH, tmp_real_str_len);

            int64_t sub_object_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SUB_OBJECT_ID", sub_object_id, int64_t);

            int64_t sub_program_id = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SUB_PROGRAM_ID", sub_program_id, int64_t);
            int64_t subprogram_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SUBPROGRAM_CNT", subprogram_cnt, int64_t);
            uint64_t subprogram_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "SUBPROGRAM_RANK", subprogram_rank, uint64_t);

            char entry_activity[20] = "";
            calc_ratio(event_cnt, num_samples, entry_activity);
            char current_activity[20] = "";
            calc_ratio(subprogram_cnt, num_samples, current_activity);

            char entry_name[common::OB_MAX_ROUTINE_NAME_LENGTH * 3] = "";
            if (subprogram_rank > 1) {
              entry_name[0] = '\0';
              entry_activity[0] = '\0';
            } else if ('\0' == entry_subprogram_name[0]) {
              sprintf(entry_name, "UNKNOWN_PLSQL_ID<%ld, %ld>", entry_object_id, entry_subprogram_id);
            } else if ('\0' == entry_obj_name[0]) {
              snprintf(entry_name, sizeof(entry_name), "%s", entry_subprogram_name);
            } else {
              snprintf(entry_name, sizeof(entry_name), "%s.%s.%s",
                       entry_obj_owner, entry_obj_name, entry_subprogram_name);
            }

            char current_name[common::OB_MAX_ROUTINE_NAME_LENGTH * 3] = "";
            if ('\0' == sub_obj_name[0]) {
              sprintf(current_name, "UNKNOWN_PLSQL_ID<%ld, %ld>", sub_object_id, sub_program_id);
            } else if ('\0' == subprogram_name[0]) {
              snprintf(current_name, sizeof(current_name), "%s", subprogram_name);
            } else {
              snprintf(current_name, sizeof(current_name), "%s.%s.%s",
                       sub_obj_owner, sub_obj_name, subprogram_name);
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {entry_name, entry_activity, current_name, current_activity};
              AshRowItem ash_row(column_size, column_content, column_widths, true);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_text(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const int64_t column_size = 2;
  const int64_t column_widths[column_size] = {64, 64};
  AshColumnItem column_headers[column_size] = {"SQL ID", "SQL Text"};
  AshColumnHeader headers(column_size, column_headers, column_widths);

  bool with_color = true;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Complete List of SQL Text"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      const ObArray<SqlIdArray> &top_sql_ids = ash_report_params.top_sql_ids_;
      if (OB_FAIL(sql_string.append_fmt(
        "SELECT sql_id AS SQL_ID, SUBSTR(query_sql, 1, 4000) AS QUERY_SQL "
        "FROM ( "
          "SELECT sql_id, query_sql, elapsed_time_total, "
                  "ROW_NUMBER() OVER(PARTITION BY sql_id ORDER BY NULL) AS sql_rank "
          "FROM %s "
          "WHERE query_sql IS NOT NULL ",
        lib::is_oracle_mode() ? "SYS.GV$OB_SQLSTAT" : "oceanbase.gv$ob_sqlstat"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!top_sql_ids.empty()) {
        if (OB_FAIL(sql_string.append(
            " AND sql_id IN ("))) {
          LOG_WARN("append sql string failed", K(ret));
        }
      }
      for (int i = 0; OB_SUCC(ret) && i < top_sql_ids.count(); i ++) {
        const char *str_fmt = (i > 0) ? ",'%s'" : "'%s'";
        if (OB_FAIL(sql_string.append_fmt(str_fmt, top_sql_ids.at(i).get_str()))) {
          LOG_WARN("append sql string failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !top_sql_ids.empty()) {
        if (OB_FAIL(sql_string.append(") "))) {
          LOG_WARN("append sql string failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql_string.append(
        ") tmp_sql "
        "WHERE sql_rank = 1 "
        "ORDER BY elapsed_time_total DESC"))) {
        LOG_WARN("append sql string failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
      } else {
        if (OB_SUCC(ret) && ash_report_params.is_html) {
          if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            int64_t tmp_real_str_len = 0;
            char sql_id[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

            HEAP_VAR(char[4005], sql_text)
            {
              sql_text[0] = '\0';
              EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                  *result, "QUERY_SQL", sql_text, 4000, tmp_real_str_len);
              if (!ash_report_params.is_html) {
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(buff.append("  SQL ID: "))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                } else if (OB_FAIL(buff.append(sql_id))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                } else if (OB_FAIL(buff.append("\n"))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                } else if (OB_FAIL(buff.append("SQL Text: "))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                } else if (OB_FAIL(buff.append(sql_text))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                } else if (OB_FAIL(buff.append("\n"))) {
                  LOG_WARN("failed to push string into buff", K(ret));
                }
              } else {
                AshColumnItem column_content[] = {sql_id, sql_text};
                AshRowItem ash_row(column_size, column_content, column_widths, with_color);
                if (OB_FAIL(print_sqltext_section_column_row(ash_report_params, buff, ash_row))) {
                  LOG_WARN("failed to format row", K(ret));
                } else {
                  with_color = !with_color;
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(buff.append("\n"))) {
                LOG_WARN("failed to push string into buff", K(ret));
              }
            }
          }
        }  // end while
      }
    }
    if (OB_SUCC(ret) && ash_report_params.is_html) {
      if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      }
    }
  }
  return ret;
}

inline int cast_otimestamp_mydatetime(const ObObj &in, int64_t &out,
    const ObDbmsWorkloadRepository::AshReportParams &ash_report_params)
{
  int ret = OB_SUCCESS;
  int64_t dt_value = 0;
  int64_t utc_value = 0;
  if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(
          in.get_type(), in.get_otimestamp_value(), ash_report_params.tz_info, dt_value))) {
    LOG_WARN("fail to timestamp_tz_to_timestamp", K(ret), K(in));
  } else if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(
                 dt_value, ash_report_params.tz_info, utc_value))) {
    LOG_WARN("failed to convert datetime to timestamp", K(ret));
  } else {
    out = utc_value;
  }
  return ret;
}

int ObDbmsWorkloadRepository::process_ash_report_params(
    const uint64_t data_version, const sql::ParamStore &params, AshReportParams &ash_report_params)
{
  int ret = OB_SUCCESS;
  if (data_version < DATA_VERSION_4_3_0_0) {
    // v4.2
    if ((5 == params.count() && data_version < MOCK_DATA_VERSION_4_2_3_0) ||
        (7 == params.count() && data_version >= MOCK_DATA_VERSION_4_2_3_0 && data_version < MOCK_DATA_VERSION_4_2_4_0) ||
        (9 == params.count() && data_version >= MOCK_DATA_VERSION_4_2_4_0)) {
      //do nothing
    } else {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("parameters number is wrong", K(ret), K(params.count()), K(data_version));
    }
  } else {
    // v4.3
    if ((5 == params.count() && data_version < DATA_VERSION_4_3_5_0) ||
        (9 == params.count() && data_version >= DATA_VERSION_4_3_5_0)) {
      //do nothing
    } else {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("parameters number is wrong", K(ret), K(params.count()), K(data_version));
    }
  }

  if (OB_SUCC(ret)) {
    if (params.at(0).is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("first parameters is null", K(ret), K(params.at(0)));
    } else if (params.at(1).is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("second parameters is null", K(ret), K(params.at(1)));
    } else if (OB_FAIL(params.at(2).get_string(ash_report_params.sql_id))) {
      LOG_WARN("failed to get sql id from params", K(ret), K(params.at(2)));
    } else if (OB_FAIL(params.at(3).get_string(ash_report_params.trace_id))) {
      LOG_WARN("failed to get trace id from params", K(ret), K(params.at(3)));
    } else if (OB_FAIL(params.at(4).get_string(ash_report_params.wait_class))) {
      LOG_WARN("failed to get wait class from params", K(ret), K(params.at(4)));
    } else {

      if (lib::is_oracle_mode()) {
        if (OB_FAIL(cast_otimestamp_mydatetime(
                params.at(0), ash_report_params.ash_begin_time, ash_report_params))) {
          LOG_WARN("failed to convert otimestamp", K(ret));
        } else if (OB_FAIL(cast_otimestamp_mydatetime(
                       params.at(1), ash_report_params.ash_end_time, ash_report_params))) {
          LOG_WARN("failed to convert otimestamp", K(ret));
        }
      } else {
        ash_report_params.ash_begin_time = params.at(0).get_timestamp();
        ash_report_params.ash_end_time = params.at(1).get_timestamp();
      }

      ash_report_params.user_input_ash_begin_time = ash_report_params.ash_begin_time;
      ash_report_params.user_input_ash_end_time = ash_report_params.ash_end_time;

      if (5 < params.count()) {
        if (OB_FAIL(params.at(5).get_string(ash_report_params.svr_ip))) {
          LOG_WARN("failed to get svr_ip from params", K(ret), K(params.at(5)));
        }
        if (OB_SUCC(ret) && !params.at(6).is_null()) {
          if (lib::is_oracle_mode()) {
            if (!params.at(6).get_number().is_valid_int64(ash_report_params.port)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cast svr prot to int_64 fail", K(ret));
            }
          } else {
            ash_report_params.port = params.at(6).get_int();
          }
        } else {
          ash_report_params.port = -1;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (7 < params.count()) {
        ObString tmp;
        if (OB_FAIL(params.at(8).get_string(tmp))) {
          LOG_WARN("failed to get report_type from params", K(ret), K(params.at(8)));
        } else if (0 == strncmp("text", tmp.ptr(), 4)) {
          ash_report_params.is_html = false;
        } else {
          ash_report_params.is_html = true;
        }
        if (OB_SUCC(ret)) {
          if (OB_SUCC(ret) && !params.at(7).is_null()) {
            if (lib::is_oracle_mode()) {
              if (!params.at(7).get_number().is_valid_int64(ash_report_params.tenant_id)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("cast svr prot to int_64 fail", K(ret));
              }
            } else {
              ash_report_params.tenant_id = params.at(7).get_int();
            }

            if (MTL_ID() != OB_SYS_TENANT_ID && ash_report_params.tenant_id != MTL_ID()) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("User tenants cannot view other tenants' ASH Reports", KR(ret), K(ash_report_params.tenant_id), K(MTL_ID()));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "User tenants cannot view other tenants' ASH Reports");
            }
          } else {
            ash_report_params.tenant_id = 0;
          }
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("parameters number is wrong", K(ret), K(params.count()), K(data_version),
        K(DATA_VERSION_4_2_2_0));
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_report_header(
    const AshReportParams &ash_report_params, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append(
        "<!DOCTYPE html><html lang=\"en\"><head><title>ASH Report</title> \
        <style type=\"text/css\"> \
        body.ash_html {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black; background:rgb(246, 248, 251);} \
        pre.ash_html  {font:8pt Courier;color:black; background:rgb(246, 248, 251);} \
        pre_sqltext.ash_html  {white-space: pre-wrap;} \
        h1.ash_html   {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:rgb(246, 248, 251);border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;} \
        h2.ash_html   {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:rgb(246, 248, 251);margin-top:4pt; margin-bottom:0pt;} \
        h3.ash_html {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:rgb(246, 248, 251);margin-top:4pt; margin-bottom:0pt;} \
        li.ash_html {font: 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:rgb(246, 248, 251);} \
        th.ash_htmlnobg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:rgb(246, 248, 251);padding-left:4px; padding-right:4px;padding-bottom:2px} \
        th.ash_htmlbg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px} \
        td.ash_htmlnc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;} \
        td.ash_htmlc    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;} \
        td.ash_htmlnclb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-left: thin solid black;} \
        td.ash_htmlncbb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-left: thin solid black;border-right: thin solid black;} \
        td.ash_htmlncrb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-right: thin solid black;} \
        td.ash_htmlcrb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-right: thin solid black;} \
        td.ash_htmlclb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-left: thin solid black;} \
        td.ash_htmlcbb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-left: thin solid black;border-right: thin solid black;} \
        a.ash_html {font:bold 8pt Arial,Helvetica,sans-serif;color:#663300; vertical-align:top;margin-top:0pt; margin-bottom:0pt;} \
        td.ash_htmlnct {font:8pt Arial,Helvetica,Geneva,sans-serif;border-top: thin solid black;color:black;background:rgb(246, 248, 251);vertical-align:top;} \
        td.ash_htmlct   {font:8pt Arial,Helvetica,Geneva,sans-serif;border-top: thin solid black;color:black;background:#FFFFCC; vertical-align:top;} \
        td.ash_htmlnclbt  {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-top: thin solid black;border-left: thin solid black;} \
        td.ash_htmlncbbt  {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-left: thin solid black;border-right: thin solid black;border-top: thin solid black;} \
        td.ash_htmlncrbt {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:rgb(246, 248, 251);vertical-align:top;border-top: thin solid black;border-right: thin solid black;} \
        td.ash_htmlcrbt     {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-right: thin solid black;} \
        td.ash_htmlclbt     {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-left: thin solid black;} \
        td.ash_htmlcbbt   {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-left: thin solid black;border-right: thin solid black;} \
        table.tdiff {  border_collapse: collapse; } \
        table.tscl {width: 600;} \
        table.tscl tbody, table.tscl thead { display: block; } \
        table.tscl thead tr th {height: 12px;line-height: 12px;} \
        table.tscl tbody { height: 100px;overflow-y: auto; overflow-x: hidden;} \
        table.tscl tbody td, thead th {width: 200;} \
        .hidden   {position:absolute;left:-10000px;top:auto;width:1px;height:1px;overflow:hidden;} \
        .pad   {margin-left:17px;} \
        .doublepad {margin-left:34px;} \
        .hide-column { \
          display: none; \
        } \
        </style></head>\n \
        <script> \
          function setSectionList() { \
            var list = document.getElementById(110); \
            const section_headers = document.querySelectorAll('h2'); \
            section_headers.forEach(ele => { \
              let str = ele.innerText.replaceAll('<br>',''); \
              let li = document.createElement('li'); \
              let a = document.createElement('a'); \
              a.classList.add('ash_html'); \
              a.setAttribute('href', str); \
              a.innerText = str; \
              li.innerHTML = '<a class = \"ash_html\" href = \"#' + str + '\">' + str + '</a>'; \
              li.classList.add('ash_html'); \
              list.appendChild(li); \
            }); \
          } \
          function mergeEmptyCells() { \
            const tables = document.querySelectorAll('table'); \
            tables.forEach(table => { \
              const rows = table.rows; \
              const calcSpanNum = (rowIndex, colIndex) => { \
                let num = 0; \
                for (let i = rowIndex + 1; i < rows.length; i++) { \
                  if (rows[i].cells[colIndex].innerText.trim() === '') { \
                    num++; \
                  } else { \
                    break; \
                  } \
                } \
                return num; \
              }; \
              let mergeColumns = new Set(); \
              if (table.classList.contains('merge_cell')) { \
                for (let j = 0; j < rows[0].cells.length; j++ ) { \
                  mergeColumns.add(j); \
                } \
              } else { \
                const headers = rows[0].getElementsByTagName('th'); \
                for (let j = 0; j < headers.length; j++) { \
                  if (headers[j].classList.contains('merge_cell')) { \
                    mergeColumns.add(j); \
                  } \
                } \
              } \
              for (let i = 1; i < rows.length; i++) { \
                for (let j = 0; j < rows[i].cells.length; j++) { \
                  if (!mergeColumns.has(j)) continue; \
                  /*跳过不需要合并的列*/ \
                  let currentCell = rows[i].cells[j]; \
                  let spanNum = calcSpanNum(i, j); \
                  /*tbody 第一行的空单元格不要清除*/ \
                  if (i > 1 && currentCell.innerText.trim() === '') { \
                    currentCell.className = 'blank-cell'; \
                    continue; \
                  } \
                  if (spanNum) { \
                    currentCell.rowSpan = spanNum + 1; \
                  } \
                } \
              } \
            }); \
            document.querySelectorAll('.blank-cell').forEach(el => el.parentNode.removeChild(el)); \
          } \
          function hideColumn() { \
            const tables = document.querySelectorAll('table'); \
            tables.forEach(table => { \
              const rows = table.rows; \
              const headers = rows[0].getElementsByTagName('th'); \
              let hiddenColumns = new Set(); \
              for (let i = 0; i < headers.length; i++) { \
                if (headers[i].classList.contains('hide-column')) { \
                  hiddenColumns.add(i); \
                } \
              } \
              for (let i = 1; i < rows.length; i++) { \
                hiddenColumns.forEach(colIndex => { \
                  rows[i].cells[colIndex].classList.add('hide-column'); \
                }); \
              } \
            }); \
          } \
          document.addEventListener('DOMContentLoaded', function () { \
            setSectionList(); \
            mergeEmptyCells(); /*合并空单元格*/ \
            hideColumn(); \
          }); \
        </script> \
        <body class=\"ash_html\"> \
        <h1 class=\"ash_html\"> \
        ASH Report \
        </h1>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    if (OB_FAIL(buff.append("\nASH Report\n\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_header(
    const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str)
{
  int ret = OB_SUCCESS;
  ash_report_params.table_cnt_ = 0;
  ash_report_params.section_name_ = str;
  SMART_VAR(ObSqlString, temp_string)
  {
    if (ash_report_params.is_html) {
      if (OB_FAIL(temp_string.append_fmt("<a class=\"ash_html\" name='%s'></a>\
          <h2 class=\"ash_html\">%s<br></h2>",
              str, str))) {
        LOG_WARN("failed to format string", K(ret));
      }
    } else {
      if (OB_FAIL(temp_string.append_fmt("\n%s:\n", str))) {
        LOG_WARN("failed to format string", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
      LOG_WARN("failed to push string into buff", K(ret));
    }
  }
  return ret;
}

uint64_t ObDbmsWorkloadRepository::get_section_table_id(const char *section_name, int64_t table_index)
{
  uint64_t table_id = murmurhash(section_name, strlen(section_name), 0);
  table_id = murmurhash(&table_index, sizeof(table_index), table_id);
  return table_id;
}

int ObDbmsWorkloadRepository::print_section_column_header(const AshReportParams &ash_report_params,
                                                          ObStringBuffer &buff,
                                                          const AshColumnHeader &column_header)
{
  int ret = OB_SUCCESS;
  ash_report_params.table_cnt_ ++;
  if (ash_report_params.is_html) {
    SMART_VARS_2((ObSqlString, temp_string), (ObSqlString, class_list))
    {
      uint64_t table_id = get_section_table_id(ash_report_params.section_name_, ash_report_params.table_cnt_);
      if (OB_FAIL(class_list.append("normal"))) {
        LOG_WARN("append class name failed", K(ret));
      } else if (column_header.merge_columns_ && OB_FAIL(class_list.append(" merge_cell"))) {
        LOG_WARN("append class name failed", K(ret));
      } else if (column_header.hide_table_ && OB_FAIL(class_list.append(" hide-table"))) {
        LOG_WARN("append class name failed", K(ret));
      } else if (OB_FAIL(temp_string.append_fmt("<table id=\"%lu\" class=\"%s\" border=\"1\"><tr>",
                                                table_id, class_list.ptr()))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
      for (int i = 0; i < column_header.column_size_ && OB_SUCC(ret); i++) {
        class_list.reuse();
        const AshColumnItem &column = column_header.columns_[i];
        if (OB_FAIL(class_list.append("ash_htmlbg"))) {
          LOG_WARN("append class name failed", K(ret));
        } else if (column.merge_cell_ && OB_FAIL(class_list.append(" merge_cell"))) {
          LOG_WARN("append class name failed", K(ret));
        } else if (column.is_hidden_ && OB_FAIL(class_list.append(" hide-column"))) {
          LOG_WARN("append class name failed", K(ret));
        } else if (OB_FAIL(temp_string.append_fmt(
                "<th class=\"%s\" scope=\"col\">%s</th>",
                class_list.ptr(), column.column_content_))) {
          LOG_WARN("failed to format string", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
          LOG_WARN("failed to push string into buff", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buff.append("</tr>"))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
    }
  } else {
    AshRowItem ash_row(column_header.column_size_, column_header.columns_, false);
    if (OB_FAIL(print_text_table_frame(column_header, buff))) {
      LOG_WARN("failed to format row", K(ret));
    } else if (OB_FAIL(format_row(ash_report_params, ash_row, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    } else if (OB_FAIL(print_text_table_frame(column_header, buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_column_row(const AshReportParams &ash_report_params,
                                                       ObStringBuffer &buff,
                                                       const AshRowItem &ash_row)
{
  int ret = OB_SUCCESS;
  const char *color_class = ash_row.with_color_ ? "ash_htmlc" : "ash_htmlnc";
  if (ash_report_params.is_html) {
    SMART_VAR(ObSqlString, temp_string)
    {
      char id2char[40] = "";
      if (ash_row.data_row_id_ > 0) {
        snprintf(id2char, sizeof(id2char), " data-row-id='%lu'", ash_row.data_row_id_);
      }
      if (OB_FAIL(temp_string.append_fmt("<tr%s>", id2char))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
      for (int i = 0; i < ash_row.column_size_ && OB_SUCC(ret); i++) {
        const AshColumnItem &column = ash_row.columns_[i];
        const char *column_content = ash_row.columns_[i].column_content_;
        if (column.href_sql_id_) {
          SqlIdArray sql_id(column_content);
          if (OB_FAIL(temp_string.append_fmt("<td%s class='%s'><a class='%s' href='#%s'>%s</a></td>",
                                             0 == i ? " scope=\"row\"" : "", //<td%s, print <td scope=row
                                             color_class, //<td class='%s'>
                                             0 == i ? "ash_html" : "awr", //<a class='%s'
                                             column_content, //href='#%s'
                                             column_content))) { //%s</a></td>
            LOG_WARN("failed to format string", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(ash_report_params.top_sql_ids_, sql_id))) {
            LOG_WARN("append sql id array failed", K(ret));
          }
        } else if (column.foreign_key_ > 0) {
          if (OB_ISNULL(ash_row.columns_[i].foreign_section_name_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("foreign section name is invalid", K(ret));
          } else {
            uint64_t table_id = get_section_table_id(column.foreign_section_name_, column.foreign_table_index_);
            if (OB_FAIL(temp_string.append_fmt("<td%s class='%s actions' data-action-params='tableIds=%lu&rowIds=%lu'>%s</td>",
                                               0 == i ? " scope=\"row\"" : "", //<td%s, print <td scope=row
                                               color_class, //<td class='%s actions'>
                                               table_id, //tableIds=%lu
                                               column.foreign_key_, //rowIds=%lu
                                               column_content))) { //%s</a></td>
              LOG_WARN("failed to format string", K(ret));
            }
          }
        } else {
          if (OB_FAIL(temp_string.append_fmt("<td%s class='%s'>%s</td>",
                                             0 == i ? " scope=\"row\"" : "", //<td%s, print <td scope=row
                                             color_class, //<td class='%s'>
                                             column_content))) { //%s</a></td>
            LOG_WARN("failed to format string", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(temp_string.append("</tr>\n"))) {
          LOG_WARN("failed to append string into buff", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
        LOG_WARN("failed to push string into buff", K(ret));
      }
    }
  } else {
    if (OB_FAIL(format_row(ash_report_params, ash_row, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_sqltext_section_column_row(const AshReportParams &ash_report_params,
                                                               ObStringBuffer &buff,
                                                               const AshRowItem &ash_row)
{
  int ret = OB_SUCCESS;
  const char *color_class = ash_row.with_color_ ? "ash_htmlc" : "ash_htmlnc";
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<tr>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else {
      SMART_VAR(ObSqlString, temp_string)
      {
        for (int i = 0; i < ash_row.column_size_ && OB_SUCC(ret); i++) {
          const char *column_content = ash_row.columns_[i].column_content_;
          if (i == 0) {
            if (OB_FAIL(temp_string.append_fmt(
                    "<td scope=\"row\" class='%s'><a class=\"ash_html\" name=\"%s\"></a>%s</td>\n",
                    color_class, column_content, column_content))) {
              LOG_WARN("failed to format string", K(ret));
            }
          } else {
            if (OB_FAIL(temp_string.append_fmt(
                    "<td class='%s'>%s</td>\n", color_class, column_content))) {
              LOG_WARN("failed to format string", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
            LOG_WARN("failed to push string into buff", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buff.append("</tr>\n"))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
    }
  } else {
    if (OB_FAIL(format_row(ash_report_params, ash_row, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_column_end(const AshReportParams &ash_report_params, ObStringBuffer &buff, const AshColumnHeader &column_header)
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("</table><p />\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    if (OB_FAIL(print_text_table_frame(column_header, buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_report_end(
    const AshReportParams &ash_report_params, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {

    if (OB_FAIL(buff.append(
      "End of Report \
      <script> \
        (() => {\
          const styles = document.createElement('style');\
          styles.innerHTML = `\
            .actions {\
              position: relative;\
              cursor: pointer;\
            }\
            .actions:hover {\
              color: #00e;\
            }\
            .actions:hover::before {\
              content: 'view detail';\
              position: absolute;\
              left: 0;\
              top: 0;\
              transform: translateY(-120%);\
              background: rgba(0 0 0/.6);\
              color: #fff;\
              font-size: 12px;\
              padding: 2px;\
              line-height: 1.4;\
            }\
            .mask.actions {\
              cursor: default;\
            }\
            .mask.actions:hover {\
              color: currentColor;\
            }\
            .drawer {\
              position: fixed;\
              display: flex;\
              flex-direction: column;\
              right: 0;\
              top: 0;\
              bottom: 0;\
              width: 100%;\
              max-width: 1024px;\
              background: #fff;\
              box-shadow: 0 0 10px rgba(0 0 0/0.15);\
            }\
            .mask {\
              position: fixed;\
              left: 0;\
              right: 0;\
              top: 0;\
              bottom: 0;\
              z-index: 100;\
              overflow: hidden;\
              box-sizing: border-box;\
            }\
            .drawer {\
              animation: slide-in .3s ease-in;\
            }\
            .drawer header {\
              padding: 24px;\
              margin-right: 100px;\
              box-sizing: border-box;\
            }\
            .drawer main {\
              padding: 24px;\
              box-sizing: border-box;\
              font-weight: normal;\
              overflow: auto;\
              overscroll-behavior: none;\
            }\
            .drawer main > section {\
              margin: 10px 0 20px;\
            }\
            .drawer .close {\
              position: absolute;\
              right: 10px;\
              top: 6px;\
              -webkit-appearance: none;\
              border: none;\
              background: transparent;\
              cursor: pointer;\
              padding: 4px;\
              line-height: 1.6;\
              color: #00e;\
            }\
            .hide-table {\
              height: 0;\
              overflow: hidden;\
              position: absolute;\
              visibility: hidden;\
            }\
            @keyframes slide-in {\
              from {\
                transform: translateX(100%);\
              }\
              to {\
                transform: translateX(0);\
              }\
            }\
        `;\
          document.head.appendChild(styles);\
        \
          const findTableById = id => [...document.querySelectorAll('table[id]')].filter(t => t.id === id.trim())[0] || null;\
        \
          const html = `\
        <div class=\"mask actions\" data-action-params=\"actionType=close\" style=\"display:none;\">\
          <div class=\"drawer\">\
            <button class=\"actions close\" data-action-params=\"actionType=close\">[close]</button>\
        <header>{title}</header>\
            <main>content</main>\
          </div>\
        </div>\
        `;\
          const attach = document.createElement('div');\
          attach.innerHTML = html;\
          document.body.appendChild(attach);\
        \
          document.body.addEventListener('click', e => {\
            const hasAction = e.target.classList.contains('actions');\
            if (!hasAction) {\
              return;\
            }\
            if (!e.target.dataset.actionParams) {\
              return;\
            }\
            const params = new URLSearchParams(e.target.dataset.actionParams);\
            e.preventDefault();\
            e.stopPropagation();\
            const actionType = params.get('actionType') || 'view';\
            const title = params.get('title') || '';\
            const width = params.get('width');\
            const tableIds = (params.get('tableIds') || '').split(',').filter(e => e);\
            const rowIds = (params.get('rowIds') || '').split(',').filter(e => e);\
            if (actionType === 'close') {\
              attach.querySelector('.mask').style.display = 'none';\
              return;\
            }\
        \
            attach.querySelector('.mask').style.display = 'block';\
            attach.querySelector('.mask .drawer > header').style.display = title ? 'block' : 'none';\
            if (title) {\
              attach.querySelector('.mask .drawer > header').innerHTML = attach.querySelector('.mask .drawer > header').innerHTML.replace('{title}', title);\
            }\
        \
            htmlString = [];\
            const tableSize = [];\
            tableIds.forEach(tid => {\
              const t = findTableById(tid);\
              if (!t) {\
                throw new Error('[JS ERR] table id no find.');\
              }\
              tableSize.push(t.getBoundingClientRect().width);\
              htmlString.push(`<section>${t.outerHTML.replaceAll('data-action-params', 'data-none')\
                .replaceAll('hide-table', '')\
                }</section>`);\
            });\
            attach.querySelector('.mask .drawer').style.cssText = `max-width:${width || (Math.max(...tableSize) || 0) + 60}px`;\
            attach.querySelector('.mask .drawer > main').innerHTML = htmlString.join('');\
        \
            if (rowIds.length) {\
              document.querySelectorAll('.mask .drawer > main > section > table > tbody > tr').forEach((row, i) => {\
                if (row.querySelectorAll('th').length) {\
                  return;\
                }\
                row.style.display = rowIds.includes((row.dataset.rowId || '').trim()) ? 'table-row' : 'none';\
              });\
            }\
          }, false);\
          document.addEventListener('keydown', function(event) {\
            if (event.keyCode === 27) {\
              document.querySelector('.mask').style.display = 'none';\
            }\
          });\
        })();\
      </script> \
      </body></html>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_text_table_frame(const AshColumnHeader &column_header, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buff.append("+"))) {
    LOG_WARN("failed to append string into buff", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_header.column_size_; i++) {
    if (OB_FAIL(lpad("-", column_header.columns_[i].column_width_, "-", buff))) {
      LOG_WARN("failed to calc lpad ", K(i), K(ret));
    } else if (OB_FAIL(buff.append("+"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(buff.append("\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_explaination_begin(
    const AshReportParams &ash_report_params, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<ul>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    // do noting
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_explaination_end(
    const AshReportParams &ash_report_params, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("</ul>\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    // do noting
  }
  return ret;
}

int ObDbmsWorkloadRepository::insert_section_explaination_line(
    const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObSqlString, temp_string)
  {
    if (ash_report_params.is_html) {
      if (OB_FAIL(temp_string.append_fmt(
              "<li class='ash_html'>%s</li>\n", str))) {
        LOG_WARN("failed to format string", K(ret));
      }
    } else {
      if (OB_FAIL(temp_string.append_fmt("  - %s\n", str))) {
        LOG_WARN("failed to format string", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buff.append(temp_string.ptr(), temp_string.length()))) {
        LOG_WARN("failed to push string into buff", K(ret));
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_header_and_explaination(
    const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *content[], int64_t content_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if(content_size < 1) {
    LOG_WARN("content_size is invalid", K(content_size));
  } else if(OB_FAIL(print_section_header(ash_report_params, buff, content[0]))) {
    LOG_WARN("failed to print section header", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to print section explaination begin", K(ret));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < content_size; i++) {
      if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff, content[i]))) {
        LOG_WARN("failed to insert section explaination line", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to print section explaination end", K(ret));
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_blocking_session(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top Blocking Sessions",
    "Blocking session activity percentages are calculated with respect to waits on enqueues, latches and \"buffer busy\" only.",
    "'% Activity' represents the load on the database caused by a particular blocking session.",
    "'# Avg Active Sessions' shows the number of ASH samples in which the blocking session was found active."
  };
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, 4))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 11;
    const int64_t column_widths[column_size] = {21, 20, 20, 64, 21, 40, 64, 20, 20, 40, 20};
    AshColumnItem column_headers[column_size] = {
      "Blocking Session ID", "% Activity", "Avg Active Sessions",
      "Holder User", "Holder TX ID", "Holder SQL ID",
      "Event Caused", "% Event", "XIDs",
      "Top Waitting SQL ID" , "% SQL ID"
    };
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
        "WITH session_data AS("
          "SELECT blocking_session_id, event, p1 AS holder_tx_id, p2 AS holder_data_seq, "
                  "tx_id, sql_id, count_weight, trace_id, event_id,  session_id "
          "FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
          ") tmp_ash"
          " where blocking_session_id != 0 and blocking_session_id is not null and event_id = 14003 "
        "), "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "top_waitting_event AS ("
          "SELECT blocking_session_id, event, holder_tx_id, holder_data_seq, sql_id, sql_samples, "
                  "ROW_NUMBER() OVER("
                    "PARTITION BY blocking_session_id, holder_tx_id, holder_data_seq, event "
                    "ORDER BY sql_samples DESC"
                  ") AS sql_rank, "
                  "SUM(tx_cnt) OVER("
                    "PARTITION BY blocking_session_id, holder_tx_id, holder_data_seq, event "
                    "ORDER BY sql_samples DESC "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                  ") AS tx_cnt, "
                  "SUM(sql_samples) OVER("
                    "PARTITION BY blocking_session_id, holder_tx_id, holder_data_seq, event "
                    "ORDER BY sql_samples DESC "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                  ") AS event_samples, "
                  "ROW_NUMBER() OVER("
                    "PARTITION BY blocking_session_id "
                    "ORDER BY sql_samples DESC"
                  ") AS session_rank, "
                  "SUM(sql_samples) OVER("
                    "PARTITION BY blocking_session_id "
                    "ORDER BY sql_samples DESC "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
                  ") AS session_samples, "
                  "ROW_NUMBER() OVER(ORDER BY NULL) AS row_id "
          "FROM ("
            "SELECT blocking_session_id, event, event_id, holder_tx_id, holder_data_seq, sql_id, "
                    "COUNT(DISTINCT tx_id) AS tx_cnt, "
                    "SUM(count_weight) AS sql_samples "
            "FROM ( "
              " SELECT blocking_session_id, event, event_id, holder_tx_id, holder_data_seq, "
              " FIRST_VALUE ( sql_id ) IGNORE NULLS OVER ( PARTITION BY  session_id, trace_id ) AS sql_id, "
              " count_weight , tx_id "
              " from session_data"
              ") sd "
            " where blocking_session_id != 0 and blocking_session_id is not null and event_id = 14003 "
            "GROUP BY blocking_session_id, event, event_id, holder_tx_id, holder_data_seq, sql_id"
          " ) top_waitting_sql "
          "ORDER BY sql_samples DESC %s"
        ")",
          lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30" //ORDER BY sql_samples DESC %s
        ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT BLOCKING_SESSION_ID, EVENT, HOLDER_USER_NAME, "
                "HOLDER_TX_ID, HOLDER_SQL_ID, WAITTING_SQL_ID, "
                "TX_CNT, SQL_SAMPLES, EVENT_SAMPLES, SESSION_SAMPLES, "
                "SQL_RANK, SESSION_RANK "
        "FROM ("
          "SELECT we.blocking_session_id AS blocking_session_id, "
                  "we.event AS event, "
                  "sa.user_name AS holder_user_name, "
                  "we.holder_tx_id AS holder_tx_id, "
                  "sa.sql_id AS holder_sql_id, "
                  "we.sql_id AS waitting_sql_id, "
                  "we.tx_cnt AS tx_cnt, "
                  "we.sql_samples AS sql_samples, "
                  "we.sql_rank AS sql_rank, "
                  "we.event_samples AS event_samples, "
                  "we.session_rank AS session_rank, "
                  "we.session_samples AS session_samples, "
                  "ROW_NUMBER() OVER (PARTITION BY we.row_id ORDER BY sa.seq_num DESC) AS holder_sql_rank "
          "FROM top_waitting_event we "
          "LEFT JOIN %s sa "
            "ON sa.tx_id = we.holder_tx_id AND sa.seq_num <= we.holder_data_seq"
        ") t "
        "WHERE t.holder_sql_rank = 1 "
        "ORDER BY t.session_samples DESC, t.event_samples DESC, t.sql_rank ASC",
        lib::is_oracle_mode() ? "SYS.GV$OB_SQL_AUDIT" : "oceanbase.gv$ob_sql_audit"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "BLOCKING_SESSION_ID", blocking_session_id, int64_t);
            char event_char[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char holder_user_name[129] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "HOLDER_USER_NAME", holder_user_name, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "HOLDER_TX_ID", holder_tx_id, int64_t);
            char holder_sql_id[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "HOLDER_SQL_ID", holder_sql_id, 64, tmp_real_str_len);
            char waitting_sql_id[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "WAITTING_SQL_ID", waitting_sql_id, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TX_CNT", tx_cnt, int64_t);
            int64_t sql_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SQL_SAMPLES", sql_samples, int64_t);
            int64_t event_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "EVENT_SAMPLES", event_samples, int64_t);
            int64_t session_samples = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "SESSION_SAMPLES", session_samples, int64_t);
            uint64_t sql_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "SQL_RANK", sql_rank, uint64_t);
            uint64_t session_rank = 0;
            EXTRACT_UINT_FIELD_FOR_ASH(*result, "SESSION_RANK", session_rank, uint64_t);

            char session_radio_char[32] = "";
            calc_ratio(session_samples, num_samples, session_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(session_samples,
                                     ash_report_params.get_elapsed_time() / 1000000,
                                     avg_active_sessions_char);
            if (session_rank > 1) {
              ASH_FIELD_CHAR(blocking_session_id)[0] = '\0';
              session_radio_char[0] = '\0';
              avg_active_sessions_char[0] = '\0';
            }

            char event_radio_char[64] = "";
            calc_ratio(event_samples, num_samples, event_radio_char);
            bool link_holder_sql_id = false;
            if (sql_rank <= 1) {
              if (holder_user_name[0] == '\0') {
                STRNCPY(holder_user_name, "UNKNOWN_USER", sizeof(holder_user_name));
              }
              if (holder_sql_id[0] == '\0') {
                STRNCPY(holder_sql_id, "UNKNOWN_SQL_ID", sizeof(holder_sql_id));
              } else {
                link_holder_sql_id = true;
              }
            }
            if (sql_rank > 1) {
              holder_user_name[0] = '\0';
              ASH_FIELD_CHAR(holder_tx_id)[0] = '\0';
              holder_sql_id[0] = '\0';
              event_char[0] = '\0';
              event_radio_char[0] = '\0';
            }

            char waitting_sql_radio_char[64] = "";
            calc_ratio(sql_samples, num_samples, waitting_sql_radio_char);
            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                ASH_FIELD_CHAR(blocking_session_id), session_radio_char, avg_active_sessions_char,
                holder_user_name, ASH_FIELD_CHAR(holder_tx_id), holder_sql_id,
                event_char, event_radio_char, ASH_FIELD_CHAR(tx_cnt),
                waitting_sql_id, waitting_sql_radio_char
              };
              column_content[5].href_sql_id_ = link_holder_sql_id;
              if (column_content[9].column_content_[0] != '\0') {
                column_content[9].href_sql_id_ = true;
              }
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_db_object(const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top DB Objects"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "With respect to Application, Cluster, User I/O and buffer busy waits only."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Object ID: tablet_id of the database table."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time period."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity:the load on the database caused by accessing the databsae object."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Execution Count:represents how many executions involve this object"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event:When accessing the object, the corresponding Top waiting events (up to 5 are printed)."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Event:The percentage of the corresponding Top wait events when accessing the object (up to 5 events are printed)."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "SQL ID:the SQL ID with the highest load when accessing the object and in the wait event."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "%SQL ID/Module:The percentage of SQL ID or Module with the highest load in this wait event when accessing the object."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Object Name (Type/ Partition Name):the name and type/partition_name of the database object being accessed"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 11;
    const int64_t column_widths[column_size] = {25, 20, 20, 20, 20, 20, 64, 20, 64, 20, 128};
    AshColumnItem column_headers[column_size] = {
      "Node Address","Tenant ID", "Object ID", "Avg Active Sessions", "% Activity", "Execution Count",
      "Event", "% Event", "SQL ID", "%SQL ID/Module", "Object Name (Type/Partition Name)"
    };
    HEAP_VARS_3((ObISQLClient::ReadResult, res), (ObSqlString, sql_string), (ObSqlString, tmp_concat_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                " WITH session_data AS ( "
                    " SELECT "
                        " svr_ip, "
                        " svr_port, "
                        " tenant_id, "
                        " tablet_id, "
                        " (CASE WHEN event_no=0 THEN 'ON CPU' ELSE event END) AS event, "
                        " (CASE WHEN sql_id is null or sql_id = '' THEN %s ELSE sql_id END) AS sql_id_or_module, "
                        " count_weight, "
                        " trace_id, "
                        " session_id "
                    "  FROM ( "
                        " SELECT * FROM ( ",
                  lib::is_oracle_mode()? 
                    " module || '.' || action " :
                    " CONCAT(module, '.', action)"))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                    ") ash_wr "
                    " ) WHERE tablet_id IS NOT NULL  "
                " ), "))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_string.append(
                " all_object_sql AS ( "
                  " SELECT "
                      " svr_ip, "
                      " svr_port, "
                      " tenant_id, "
                      " tablet_id, "
                      " event, "
                      " sql_id_or_module, "
                      " SUM(count_weight) AS sql_time, "
                      " ROW_NUMBER() OVER ( "
                          " PARTITION BY svr_ip, svr_port, tenant_id, tablet_id, event "
                          " ORDER BY NULL "
                      " ) AS sql_rank, "
                      " SUM(SUM(count_weight)) OVER ( "
                          " PARTITION BY svr_ip, svr_port, tenant_id, tablet_id, event "
                          " ORDER BY NULL "
                          " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
                      " ) AS event_time, "
                      " SUM(SUM(count_weight)) OVER ( "
                          " PARTITION BY svr_ip, svr_port, tenant_id, tablet_id "
                          " ORDER BY NULL "
                          " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
                      " ) AS object_time, "
                      " SUM(SUM(count_weight)) OVER ( "
                          " PARTITION BY svr_ip, svr_port "
                          " ORDER BY NULL "
                          " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
                      " ) AS node_time, "
                      " SUM(COUNT(DISTINCT trace_id)) OVER ( "
                          " PARTITION BY svr_ip, svr_port, tenant_id, tablet_id "
                          " ORDER BY NULL "
                          " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
                      " ) AS exec_cnt "
                  " FROM session_data sd "
                  " GROUP BY svr_ip, svr_port, tenant_id, tablet_id, event, sql_id_or_module "
                " ), "
                " top_object_event AS ( "
                  " SELECT "
                      " svr_ip, "
                      " svr_port, "
                      " tenant_id, "
                      " tablet_id, "
                      " event, "
                      " sql_id_or_module, "
                      " sql_time, "
                      " event_time, "
                      " object_time, "
                      " node_time, "
                      " exec_cnt, "
                      " ROW_NUMBER() OVER ( "
                          " ORDER BY node_time DESC, object_time DESC, event_time DESC, sql_time DESC "
                      " ) AS display_rank "
                  " FROM ( "
                      " SELECT "
                          " svr_ip, "
                          " svr_port, "
                          " tenant_id, "
                          " tablet_id, "
                          " event, "
                          " sql_id_or_module, "
                          " sql_time, "
                          " event_time, "
                          " object_time, "
                          " node_time, "
                          " exec_cnt, "
                          " DENSE_RANK() OVER ( "
                              " ORDER BY object_time DESC, node_time DESC, tenant_id, tablet_id, svr_ip, svr_port "
                          " ) AS limit_rank "
                      " FROM all_object_sql "
                      " WHERE sql_rank = 1 "
                  " ) tmp_event "
                  " WHERE limit_rank <= 30 "
              " ), "
              " top_time_rank AS ( "
                  " SELECT "
                      " svr_ip, "
                      " svr_port, "
                      " tenant_id, "
                      " tablet_id, "
                      " event, "
                      " sql_id_or_module, "
                      " sql_time, "
                      " event_time, "
                      " object_time, "
                      " node_time, "
                      " exec_cnt, "
                      " display_rank, "
                      " ROW_NUMBER() OVER ( "
                          " PARTITION BY svr_ip, svr_port "
                          " ORDER BY display_rank "
                      " ) AS object_rank, "
                      " ROW_NUMBER() OVER ( "
                          " PARTITION BY svr_ip, svr_port, tenant_id, tablet_id "
                          " ORDER BY display_rank "
                      " ) AS event_rank "
                  " FROM top_object_event toe "
              " ), "))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
            " table_name_info AS ( "
                " SELECT DISTINCT "
                  " %s, "
                  " table_id, "
                  " tablet_id, "
                  " data_table_id, "
                  " database_name, "
                  " table_name, "
                  " table_type, "
                  " partition_name, "
                  " subpartition_name, "
                  " index_name "
                " FROM %s "
                " UNION "
                " SELECT DISTINCT "
                    " A.tenant_id as tenant_id, "
                    " A.table_id as table_id, "
                    " A.table_id as tablet_id, "
                    " CASE "
                      " WHEN A.data_table_id in (0) THEN NULL "
                      " ELSE A.data_table_id "
                    " END as data_table_id, "
                    " D.database_name, "
                    " A.table_name, "
                    " CASE WHEN A.TABLE_TYPE IN (0,2) THEN 'SYSTEM TABLE' "
                      " WHEN A.TABLE_TYPE IN (3,6,8,9) THEN 'USER TABLE' "
                      " WHEN A.TABLE_TYPE IN (5) THEN 'INDEX'  "
                      " WHEN A.TABLE_TYPE IN (12,13) THEN 'LOB AUX TABLE' "
                      "  ELSE NULL "
                    " END AS table_type, "
                    " 'NULL' as partition_name, "
                    " 'NULL' as subpartition_name, "
                    " A.table_name as index_name "
                "  FROM %s A JOIN %s D ON A.DATABASE_ID = D.DATABASE_ID "
                " WHERE table_id BETWEEN 10000 AND 20000 "
            " ),",
          is_sys_tenant(request_tenant_id)? " tenant_id " : " EFFECTIVE_TENANT_ID() as tenant_id ",
          /*dba_ob_table_locations*/
          lib::is_oracle_mode()? " SYS.DBA_OB_TABLE_LOCATIONS ": is_sys_tenant(request_tenant_id)? " oceanbase.CDB_OB_TABLE_LOCATIONS " : " oceanbase.DBA_OB_TABLE_LOCATIONS ",
          /*all_virtual_table*/
          lib::is_oracle_mode()? " SYS.ALL_VIRTUAL_TABLE_REAL_AGENT ": " oceanbase.__ALL_VIRTUAL_TABLE ",
          /*all_virtual_database*/
          lib::is_oracle_mode()? " SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT ": is_sys_tenant(request_tenant_id)? " oceanbase.__ALL_VIRTUAL_DATABASE " : " oceanbase.__ALL_DATABASE "
          ))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(tmp_concat_string.append(
        " ( database_name || '.' ||  CASE WHEN table_type IN ('INDEX', 'LOB AUX TABLE') THEN data_table_name ELSE table_name END || "
        " CASE "
            " WHEN table_type = 'INDEX' THEN '.' || index_name "
            " WHEN table_type = 'LOB AUX TABLE' THEN '.lob_aux_table' "
            " ELSE '' "
        " END || "
        " CASE "
            " WHEN subpartition_name <> 'NULL' THEN '(' || subpartition_name || ')' "
            " WHEN partition_name <> 'NULL' THEN '(' || partition_name || ')' "
            " ELSE '' "
        " END )  "
      ))) {
        LOG_WARN("failed to append tmp concat string", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(tmp_concat_string.append(
        " CONCAT( "
            " database_name, '.',  "
            " CASE WHEN table_type IN ('INDEX', 'LOB AUX TABLE') THEN data_table_name ELSE table_name END, "
            " CASE "
                " WHEN table_type = 'INDEX' THEN CONCAT('.', index_name) "
                " WHEN table_type = 'LOB AUX TABLE' THEN '.lob_aux_table' "
                " ELSE '' "
            " END, "
            " CASE "
                " WHEN subpartition_name <> 'NULL' THEN CONCAT('(', subpartition_name, ')') "
                " WHEN partition_name <> 'NULL' THEN CONCAT('(', partition_name, ')') "
                " ELSE '' "
            " END "
        " )"
      ))) {
        LOG_WARN("failed to append tmp concat string", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
            " tablet_info AS ( "
              " SELECT "
              " tenant_id, "
              " %s AS object_name, "
              " tablet_id "
              " FROM ( "
                " SELECT DISTINCT  "
                    " v1.tenant_id AS tenant_id, "
                    " v1.tablet_id AS tablet_id, "
                    " v1.database_name AS database_name, "
                    " v1.table_name AS table_name, "
                    " v1.table_type AS table_type, "
                    " v1.partition_name AS partition_name, "
                    " v1.subpartition_name AS subpartition_name, "
                    " v1.index_name AS index_name, "
                    " v2.table_name AS data_table_name "
                " FROM ( "
                    " SELECT DISTINCT "
                        " tenant_id, "
                        " tablet_id, "
                        " data_table_id, "
                        " database_name, "
                        " table_name, "
                        " table_type, "
                        " partition_name, "
                        " subpartition_name, "
                        " index_name "
                    " FROM table_name_info "
                    " WHERE (tenant_id, tablet_id) IN (SELECT DISTINCT tenant_id, tablet_id FROM top_object_event)"
                  " ) v1 "
                  " LEFT JOIN table_name_info v2 ON v1.tenant_id = v2.tenant_id and v1.data_table_id = v2.table_id "
                " ) tmp_tablet_info "
            " ) ", tmp_concat_string.ptr()))) { //%s AS object_name
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
              " SELECT "
                  " %s AS NODE_ADDR, "
                  " tr.object_rank AS OBJECT_RANK, "
                  " tr.tenant_id AS TENANT_ID, "
                  " tr.tablet_id AS TABLET_ID, "
                  " tr.object_time AS OBJECT_TIME, "
                  " tr.exec_cnt AS EXEC_CNT, "
                  " tr.event_rank AS EVENT_RANK, "
                  " tr.event AS EVENT, "
                  " tr.sql_id_or_module AS SQL_ID_OR_MODULE, "
                  " ti.object_name AS OBJECT_NAME, "
                  " tr.event_time AS EVENT_TIME, "
                  " tr.sql_time AS SQL_TIME"
              " FROM top_time_rank tr "
              " LEFT JOIN tablet_info ti ON tr.tenant_id = ti.tenant_id and tr.tablet_id = ti.tablet_id "
              " WHERE tr.event_rank <= 5 "
              " ORDER BY display_rank ASC; ",
              lib::is_oracle_mode() ? "tr.svr_ip || ':' || tr.svr_port " : "CONCAT(tr.svr_ip, ':', tr.svr_port)" ))) {
        LOG_WARN("failed to append sql string", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_addr[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "NODE_ADDR", node_addr, 64, tmp_real_str_len);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "OBJECT_RANK", object_rank, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TENANT_ID", tenant_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TABLET_ID", tablet_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "OBJECT_TIME", object_time, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EXEC_CNT", exec_cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "EVENT_RANK", event_rank, int64_t);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char sql_id_or_module[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID_OR_MODULE", sql_id_or_module, 64, tmp_real_str_len);
            char object_name[256] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "OBJECT_NAME", object_name, 255, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_TIME", event_time, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SQL_TIME", sql_time, int64_t);

            char avg_active_sessions_char[64] = "";
            calc_avg_active_sessions(object_time, ash_report_params.get_elapsed_time() / 1000000,
                avg_active_sessions_char);
            char object_time_radio_char[64] = "";
            calc_ratio(object_time, num_samples, object_time_radio_char);
            char event_radio_char[64] = "";
            calc_ratio(event_time, num_samples, event_radio_char);
            char sql_radio_char[64] = "";
            calc_ratio(sql_time, num_samples, sql_radio_char);

            if (OB_SUCC(ret) && exec_cnt <= 1) {
              int tmp_ret = OB_SUCCESS;
              if (object_name[0] == '\0' && request_tenant_id != OB_SYS_TENANT_ID && tablet_id <= 1000) {
                int64_t meta_tenant_id = gen_meta_tenant_id(request_tenant_id);
                if (OB_TMP_FAIL(get_table_name_by_id(meta_tenant_id, tablet_id, object_name, 256 /*object_name_len*/))) {
                  snprintf(object_name, 256, "UNKNOWN_OBJECT <%ld, %ld>", meta_tenant_id, tablet_id);
                  LOG_WARN("failed to get table name by id", K(ret), K(meta_tenant_id), K(tablet_id));
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (object_rank > 1) {
                node_addr[0] = '\0';
              }
              if (event_rank > 1) {
                tenant_id = 0;
                ASH_FIELD_CHAR(tenant_id)[0] = '\0';
                tablet_id = 0;
                ASH_FIELD_CHAR(tablet_id)[0] = '\0';
                avg_active_sessions_char[0] = '\0';
                object_time_radio_char[0] = '\0';
                exec_cnt = 0;
                ASH_FIELD_CHAR(exec_cnt)[0] = '\0';
                object_name[0] = '\0';
              }
            }

            if (OB_SUCC(ret)) {
              AshColumnItem column_content[] = {
                node_addr, ASH_FIELD_CHAR(tenant_id), ASH_FIELD_CHAR(tablet_id), avg_active_sessions_char,
                object_time_radio_char, ASH_FIELD_CHAR(exec_cnt), event_char,
                event_radio_char, sql_id_or_module, sql_radio_char, object_name
              };
              column_content[7].href_sql_id_ = true;
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::get_table_name_by_id(uint64_t tenant_id, uint64_t object_id, char *object_name, const size_t object_name_len)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_info = nullptr;
  const share::schema::ObSimpleTableSchemaV2 *simple_schema = nullptr;
  const ObSimpleDatabaseSchema *database_schema = NULL;
  uint64_t table_id = 0;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is nullptr", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, object_id, simple_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(object_id));
  } else if (OB_ISNULL(simple_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id,
                                                      simple_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("fail to get_database_schema", KPC(simple_schema), K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(object_id), K(ret));
  } else {
    snprintf(object_name, object_name_len, "%s.%s", database_schema->get_database_name(), simple_schema->get_table_name());
  }
  return ret;
}


int ObDbmsWorkloadRepository::print_ash_top_io_bandwidth(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top IO Bandwidth",
    "This section lists the top io bandwidth from the perspective of SQL and background tasks",
    "Node: The IP address and port number of the observer",
    "Program Module Action / SQL ID: Program Module Action represent the type of background tasks, which is only effective in background tasks. And SQL ID is only effective in foreground tasks",
    "Plan Hash: Numeric representation of the current SQL plan, which is only effective in foreground tasks.",
    "IOPS: Average IO requests executed per second",
    "IO Size: The total read or write size(MB) of background task or SQL",
    "IO Bandwidth: Average IO size(MB) consumed per second",
    "Type: read or write",
    "Object ID: The tablet_id with the highest read or write bytes",
    "% Object ID: The ratio of the tablet_id with the highest read or write bytes to the total bytes of tablet_ids in entire background task or SQL",
  };
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    // do not support.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push ash top io header into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy = lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const uint64_t column_size  = 9;
    const int64_t column_widths[column_size] = {21, 96, 20, 8, 16, 20, 20, 10, 12};
    AshColumnItem column_headers[column_size] = {"Node", "Program Module Action / SQL ID", "Plan Hash", "Type",  
    "IOPS", "IO Size(MB)", "IO Bandwidth(MB/s)", "Object ID", "% Object ID"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to print column header", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "WITH session_data AS ("
            "SELECT svr_ip, svr_port, program, module, action, sql_id, plan_hash, delta_read_io_requests, "
                   "delta_read_io_bytes, delta_write_io_requests, delta_write_io_bytes, tablet_id, "
                   "count_weight, tm_delta_time "
            "FROM ( "
                ))) {
          LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash wr view sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
            "AND (delta_read_io_requests > 0 or delta_write_io_requests > 0))), "))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_data AS ("
            "SELECT svr_ip, svr_port, NULL AS program, NULL AS module, NULL AS action, "
                    "sql_id, plan_hash, io_count, io_bytes, type, tablet_id, "
                    "ROW_NUMBER() OVER(PARTITION BY svr_ip, svr_port, sql_id, plan_hash, "
                                      "type ORDER BY io_bytes DESC) AS rank,"
                    "SUM(io_count) OVER(PARTITION BY svr_ip, svr_port, sql_id, "
                                        "plan_hash, type) AS total_io_counts, "
                    "SUM(io_bytes) OVER(PARTITION BY svr_ip, svr_port, "
                                        "sql_id, plan_hash, type) AS total_io_bytes, "
                    "SUM(tablet_tm_delta_time) OVER(PARTITION BY svr_ip, svr_port, "
                                        "sql_id, plan_hash, type) AS total_delta_time "
            "FROM ( "))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
                "SELECT svr_ip, svr_port, program, module, action, sql_id, plan_hash, "
                        "CASE rn_ WHEN 1 THEN read_count WHEN 2 THEN write_count END AS io_count, "
                        "CASE rn_ WHEN 1 THEN read_bytes WHEN 2 THEN write_bytes END AS io_bytes, "
                        "CASE rn_ WHEN 1 THEN 'disk_read' WHEN 2 THEN 'disk_write' END AS type, "
                        "tablet_id, tablet_tm_delta_time "
                "FROM  "))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
                      "(SELECT svr_ip, svr_port, NULL AS program, NULL AS module, NULL AS action, "
                              "sql_id, plan_hash, "
                              "SUM(delta_read_io_requests * count_weight) AS read_count, "
                              "SUM(delta_read_io_bytes * count_weight) AS read_bytes, "
                              "SUM(delta_write_io_requests * count_weight) AS write_count, "
                              "SUM(delta_write_io_bytes * count_weight) AS write_bytes, tablet_id, "
                              "SUM(tm_delta_time * count_weight) AS tablet_tm_delta_time "
                      "FROM session_data sd "
                      "WHERE sql_id IS NOT NULL AND plan_hash IS NOT NULL "
                      "GROUP BY svr_ip, svr_port, sql_id, plan_hash, tablet_id) ,"
                      "(SELECT ROW_NUMBER() OVER(ORDER BY NULL) AS rn_ FROM table(generator(2)))"
                  ") WHERE io_count > 0),"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "module_data AS ( "
           "SELECT svr_ip, svr_port, program, module, action, sql_id, plan_hash, io_count, "
                  "io_bytes, type, tablet_id, "
                  "ROW_NUMBER() OVER(PARTITION BY svr_ip, svr_port, program, module, action, "
                                    "type ORDER BY io_bytes DESC) AS rank, "
                  "SUM(io_count) OVER(PARTITION BY svr_ip, svr_port, program, module, "
                                      "action, type) AS total_io_counts, "
                  "SUM(io_bytes) OVER(PARTITION BY svr_ip, svr_port, program, "
                                      "module, action, type) AS total_io_bytes, "
                  "SUM(tablet_tm_delta_time) OVER(PARTITION BY svr_ip, svr_port, program, "
                                      "module, action, type) AS total_delta_time "
           "FROM ( "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
                "SELECT svr_ip, svr_port, program, module, action, sql_id, plan_hash, "
                        "CASE rn_ WHEN 1 THEN read_count WHEN 2 THEN write_count END AS io_count, "
                        "CASE rn_ WHEN 1 THEN read_bytes WHEN 2 THEN write_bytes END AS io_bytes, "
                        "CASE rn_ WHEN 1 THEN 'disk_read' WHEN 2 THEN 'disk_write' END AS type, "
                        "tablet_id, tablet_tm_delta_time "
                "FROM "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
                      "(SELECT svr_ip, svr_port, program, module, action, NULL AS sql_id, NULL AS plan_hash, "
                              "SUM(delta_read_io_requests * count_weight) AS read_count, "
                              "SUM(delta_read_io_bytes * count_weight) AS read_bytes, "
                              "SUM(delta_write_io_requests * count_weight) AS write_count, "
                              "SUM(delta_write_io_bytes * count_weight) AS write_bytes, tablet_id, "
                              "SUM(tm_delta_time * count_weight) AS tablet_tm_delta_time "
                      "FROM session_data sd "
                      "WHERE sql_id IS NULL AND plan_hash IS NULL "
                      "GROUP BY svr_ip, svr_port, program, module, action, tablet_id), "
                      "(SELECT ROW_NUMBER() OVER(ORDER BY NULL) AS rn_ FROM table(generator(2))) "
                  ") WHERE io_count > 0)"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT * FROM ("
                "SELECT %s AS NODE, PROGRAM, MODULE, ACTION, SQL_ID, PLAN_HASH, IO_COUNT, IO_BYTES, TOTAL_DELTA_TIME, "
                        "TYPE, TABLET_ID, TOTAL_IO_COUNTS, TOTAL_IO_BYTES "
                "FROM sql_data WHERE RANK=1 "
                "UNION ALL "
                "SELECT %s AS NODE, PROGRAM, MODULE, ACTION, SQL_ID, PLAN_HASH, IO_COUNT, IO_BYTES, TOTAL_DELTA_TIME, "
                        "TYPE, TABLET_ID, TOTAL_IO_COUNTS, TOTAL_IO_BYTES "
                "FROM module_data WHERE RANK=1 "
        ") ORDER BY TOTAL_IO_BYTES / TOTAL_DELTA_TIME DESC %s",
        lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)",
        lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)",
        lib::is_oracle_mode() ? "FETCH FIRST 50 ROW ONLY" : "LIMIT 50"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("Failed to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "NODE", node_char, 64, tmp_real_str_len);
            char program_char[ASH_PROGRAM_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "PROGRAM", program_char, ASH_PROGRAM_STR_LEN, tmp_real_str_len);
            char module_char[ASH_MODULE_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "MODULE", module_char, ASH_MODULE_STR_LEN, tmp_real_str_len);
            char action_char[ASH_ACTION_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ACTION", action_char, ASH_ACTION_STR_LEN, tmp_real_str_len);
            char sql_id_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id_char, 64, tmp_real_str_len);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TABLET_ID", tablet_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_IO_COUNTS", total_io_counts, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "IO_BYTES", io_bytes, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_IO_BYTES", total_io_bytes, int64_t);
            char type_char[32] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "TYPE", type_char, 32, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TOTAL_DELTA_TIME", total_delta_time, int64_t);

            char total_io_mega_byte[64] = "0";
            calc_avg_active_sessions(total_io_bytes, 1024.0 * 1024.0, total_io_mega_byte);
            char tablet_ratio_char[64] = "-";
            calc_ratio(io_bytes, total_io_bytes, tablet_ratio_char);
            char io_ps_char[64] = "0";
            char io_bandwidth_char[64] = "0";
            if (total_delta_time > 0) {
              calc_avg_active_sessions(total_io_counts, static_cast<double>(total_delta_time) / 1000000.0, io_ps_char);
              calc_avg_active_sessions(total_io_bytes, static_cast<double>(total_delta_time) / 1000000.0 * 1024 * 1024,
                                       io_bandwidth_char); //convert Byte to MB
            } else {
              LOG_WARN("total_delta_time is 0", K(node_char), K(program_char), K(module_char), 
                       K(action_char), K(sql_id_char), K(plan_hash));
            }

            char program_module_action_char[ASH_PROGRAM_STR_LEN + ASH_MODULE_STR_LEN + ASH_ACTION_STR_LEN + 3] = "";
            if (OB_SUCC(ret) && module_char[0] != '\0') {
              snprintf(program_module_action_char, ASH_PROGRAM_STR_LEN + ASH_MODULE_STR_LEN + ASH_ACTION_STR_LEN + 3, 
                      "%s/%s/%s", program_char, module_char, action_char);
              AshColumnItem column_content[] = {
                node_char, program_module_action_char, plan_hash_char, type_char, io_ps_char, total_io_mega_byte,
                io_bandwidth_char, tablet_id_char, tablet_ratio_char
              };
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            } else if (OB_SUCC(ret) && sql_id_char[0] != '\0') {
              AshColumnItem column_content[] = {
                node_char, sql_id_char, plan_hash_char, type_char, io_ps_char, total_io_mega_byte, 
                io_bandwidth_char, tablet_id_char, tablet_ratio_char
              };
              column_content[1].href_sql_id_ = true;
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_io_event(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const char *contents[] = {
    "Top IO Events",
    "This section lists the top io event from the perspective of SQL and background tasks",
    "Node: The IP address and port number of the observer",
    "Program Module Action / SQL ID: Program Module Action represent the type of background tasks, which is only "
                                    "effective in background tasks. And SQL ID is only effective in foreground tasks",
    "Plan Hash: Numeric representation of the current SQL plan, which is only effective in foreground tasks",
    "IO Event Samples: The total samples of io wait event during task execution.",
    "%IO Event Samples: The proportion of total io wait event samples among all sampled points",
    "Top Event: The top io wait event name.",
    "IO Type: The type of io wait event.",
    "% EVENT: activity percentage for the io wait event.",
    "Enqueue Time: The total time(in seconds) spent queuing for I/O requests.",
    "Device Time: The total time (in seconds) spent executing I/O requests on the device.",
    "Callback Time: The total time (in seconds) spent executing callbacks for I/O requests.",
  };
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    // do not support.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header_and_explaination(ash_report_params, buff, contents, ARRAYSIZEOF(contents)))) {
    LOG_WARN("failed to push ash top io header into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy = lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const uint64_t column_size  = 11;
    const int64_t column_widths[column_size] = {21, 96, 20, 20, 20, 10, 20, 10, 20, 20, 20};
    AshColumnItem column_headers[column_size] = {
      "Node", "Program Module Action / SQL ID", "Plan Hash", "IO Event Samples", "%IO Event Samples", 
      "Top Event", "IO Type", "% EVENT", "Enqueue Time(S)", "Device Time(S)", "Callback Time(S)"
    };
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      AshColumnHeader headers(column_size, column_headers, column_widths, true);
      if (OB_FAIL(print_section_column_header(ash_report_params, buff, headers))) {
        LOG_WARN("failed to print column header", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
        "WITH session_data AS ( "
            " SELECT svr_ip, svr_port, program, module, action, sql_id, plan_hash, "
                    "p1, p2, p3, count_weight, event_id, time_waited "
            "FROM ( "
                ))) {
          LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(append_fmt_ash_wr_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash wr view sql", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(" AND event_id BETWEEN 10000 AND 12000 )), "))) {
        LOG_WARN("Failed to append top io time sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "event_name AS ( "
          "SELECT event_id, display_name as event_name, wait_class as type "
          "FROM %s "
        "), ",
        lib::is_oracle_mode() ? "sys.V$EVENT_NAME" : "oceanbase.V$EVENT_NAME"
      ))) {
        LOG_WARN("Failed to append top io time sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_event_data AS ( "
          "SELECT svr_ip, svr_port, NULL AS program, NULL AS module, NULL AS action, "
                 "sql_id, plan_hash, sd.event_id AS event_id, "
                 "SUM(p1) AS  event_sum_p1, "
                 "SUM(p2) AS  event_sum_p2, "
                 "SUM(p3) AS  event_sum_p3, "
                 "SUM(time_waited) AS  event_sum_time_waited, "
                 "SUM(count_weight) AS event_count, "
                 "ROW_NUMBER() OVER (PARTITION BY svr_ip, svr_port, sql_id, plan_hash "
                                    "ORDER BY SUM(count_weight) DESC) AS event_rank " 
          "FROM session_data sd "
          "LEFT JOIN event_name en ON sd.event_id = en.event_id "
          "WHERE sql_id IS NOT NULL AND plan_hash IS NOT NULL AND (type='USER_IO' or type='SYSTEM_IO') "
          "GROUP BY svr_ip, svr_port, sql_id, plan_hash, sd.event_id "
        "), "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_data AS ( "
          "SELECT svr_ip, svr_port, NULL AS program, NULL AS module, NULL AS action, sql_id, plan_hash, "
                 "sum(event_sum_p1) AS sql_sum_p1, "
                 "sum(event_sum_p2) AS sql_sum_p2, "
                 "sum(event_sum_p3) AS sql_sum_p3, "
                 "sum(event_sum_time_waited) AS sql_sum_time_waited, "
                 "sum(event_count) AS sql_count, ROW_NUMBER() OVER (ORDER BY sum(event_count) DESC) AS sql_rank " 
          "FROM sql_event_data "
          "GROUP BY svr_ip, svr_port, sql_id, plan_hash "
        "), "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "sql_all_data AS ( "
          "SELECT sed.svr_ip, sed.svr_port, sed.program, sed.module, sed.action, sed.sql_id, sed.plan_hash, "
                 "sed.event_id, sed.event_sum_p1, sed.event_sum_p2, sed.event_sum_p3, sed.event_sum_time_waited, "
                 "sed.event_count, sed.event_rank, sd.sql_sum_p1, sd.sql_sum_p2, sd.sql_sum_p3, "
                 "sd.sql_sum_time_waited, sd.sql_count, sd.sql_rank, en.event_name, en.type " 
          "FROM sql_event_data sed " 
          "LEFT JOIN sql_data sd "
            "ON sed.svr_ip = sd.svr_ip "
            "AND sed.svr_port = sd.svr_port "
            "AND sed.sql_id = sd.sql_id "
            "AND sed.plan_hash = sd.plan_hash "
          "LEFT JOIN event_name en "
            "ON sed.event_id = en.event_id "
          "WHERE sql_rank <= 30 AND event_rank <= 3 "
       "), "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "module_event_data AS ( "
          "SELECT svr_ip, svr_port, program, module, action, "
                 "NULL AS sql_id, NULL AS plan_hash, sd.event_id as event_id, "
                 "SUM(p1) AS  event_sum_p1, "
                 "SUM(p2) AS  event_sum_p2, "
                 "SUM(p3) AS  event_sum_p3, "
                 "SUM(time_waited) AS  event_sum_time_waited, "
                 "SUM(count_weight) AS event_count, "
                 "ROW_NUMBER() OVER (PARTITION BY svr_ip, svr_port, program, module, action "
                                     "ORDER BY SUM(count_weight) DESC) AS event_rank "
          "FROM session_data sd "
          "LEFT JOIN event_name en ON sd.event_id = en.event_id " 
          "WHERE sql_id IS NULL "
                "AND plan_hash IS NULL "
                "AND program IS NOT NULL "
                "AND module IS NOT NULL "
                "AND (type='USER_IO' or type='SYSTEM_IO') "
          "GROUP BY svr_ip, svr_port, program, module, action, sd.event_id "
         "),"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "module_data AS ( "
          "SELECT svr_ip, svr_port, program, module, action, NULL AS sql_id, NULL AS plan_hash, "
                  "sum(event_sum_p1) AS module_sum_p1, "
                  "sum(event_sum_p2) AS module_sum_p2, "
                  "sum(event_sum_p3) AS module_sum_p3, "
                  "sum(event_sum_time_waited) AS module_sum_time_waited, "
                  "sum(event_count) AS module_count, "
                  "ROW_NUMBER() OVER (ORDER BY sum(event_count) DESC) AS module_rank " 
          "FROM module_event_data "
          "GROUP BY svr_ip, svr_port, program, module, action "
      "),"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "module_all_data AS ( "
          "SELECT med.svr_ip, med.svr_port, med.program, med.module, med.action, med.sql_id, "
                 "med.plan_hash, med.event_id, med.event_sum_p1, med.event_sum_p2, med.event_sum_p3, "
                 "med.event_sum_time_waited, med.event_count, med.event_rank, md.module_sum_p1, md.module_sum_p2, "
                 "md.module_sum_p3, md.module_sum_time_waited, md.module_count, md.module_rank, en.event_name, en.type "
          "FROM module_event_data med "
          "LEFT JOIN module_data md "
            "ON med.svr_ip = md.svr_ip "
            "AND med.svr_port = md.svr_port "
            "AND med.program = md.program "
            "AND med.module = md.module "
            "AND med.action = md.action "
          "LEFT JOIN event_name en "
            "ON med.event_id = en.event_id "
          "WHERE module_rank <= 30 AND event_rank <= 3 "
      ") "
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_string.append_fmt(
        "SELECT * "
        "FROM( "
            "SELECT "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE %s END AS NODE, "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE sad.program END AS PROGRAM, "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE sad.module END AS MODULE, "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE sad.action END AS ACTION, "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE sad.sql_id END AS SQL_ID, "
              "CASE WHEN sad.event_rank > 1 THEN NULL ELSE sad.plan_hash END AS PLAN_HASH,"
              "sad.event_sum_p1 AS EVENT_SUM_P1, "
              "sad.event_sum_p2 AS EVENT_SUM_P2, "
              "sad.event_sum_p3 AS EVENT_SUM_P3, "
              "sad.event_sum_time_waited AS EVENT_SUM_TIME_WAITED, "
              "sad.event_count AS  EVENT_COUNT, "
              "sad.event_rank AS EVENT_RANK, "
              "sad.sql_sum_p1 AS ROW_SUM_P1, "
              "sad.sql_sum_p2 AS ROW_SUM_P2, "
              "sad.sql_sum_p3 AS ROW_SUM_P3, "
              "sad.sql_count AS ROW_COUNT, "
              "sad.sql_rank AS ROW_RANK, "
              "sad.event_name AS EVENT_NAME, "
              "sad.type AS TYPE "
            "FROM sql_all_data sad " 
            "UNION ALL "
            "SELECT "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE %s END AS NODE, "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE mad.program END AS PROGRAM, "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE mad.module END AS MODULE, "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE mad.action END AS ACTION, "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE mad.sql_id END AS SQL_ID, "
              "CASE WHEN mad.event_rank > 1 THEN NULL ELSE mad.plan_hash END AS PLAN_HASH, "
              "mad.event_sum_p1 AS EVENT_SUM_P1, "
              "mad.event_sum_p2 AS EVENT_SUM_P2, "
              "mad.event_sum_p3 AS EVENT_SUM_P3, "
              "mad.event_sum_time_waited AS EVENT_SUM_TIME_WAITED,"
              "mad.event_count AS  EVENT_COUNT, "
              "mad.event_rank AS EVENT_RANK, "
              "mad.module_sum_p1 AS ROW_SUM_P1, "
              "mad.module_sum_p2 AS ROW_SUM_P2, "
              "mad.module_sum_p3 AS ROW_SUM_P3, "
              "mad.module_count AS ROW_COUNT, "
              "mad.module_rank AS ROW_RANK, "
              "mad.event_name AS EVENT_NAME, "
              "mad.type AS TYPE "
            "FROM module_all_data mad "
        ") ORDER BY ROW_COUNT DESC, ROW_RANK ASC, EVENT_RANK ASC %s ",
        lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)",
        lib::is_oracle_mode() ? "svr_ip || ':' || svr_port" : "CONCAT(svr_ip, ':', svr_port)",
        lib::is_oracle_mode() ? "FETCH FIRST 30 ROWS ONLY" : "LIMIT 30"
      ))) {
        LOG_WARN("Failed to append top io sql");
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("Failed to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
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
            int64_t tmp_real_str_len = 0;
            char node_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "NODE", node_char, 64, tmp_real_str_len);
            char program_char[ASH_PROGRAM_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "PROGRAM", program_char, ASH_PROGRAM_STR_LEN, tmp_real_str_len);
            char module_char[ASH_MODULE_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "MODULE", module_char, ASH_MODULE_STR_LEN, tmp_real_str_len);
            char action_char[ASH_ACTION_STR_LEN] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "ACTION", action_char, ASH_ACTION_STR_LEN, tmp_real_str_len);
            char sql_id_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "SQL_ID", sql_id_char, 64, tmp_real_str_len);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "ROW_COUNT", row_count, int64_t);
            char event_name_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "EVENT_NAME", event_name_char, 64, tmp_real_str_len);
            char type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(*result, "TYPE", type_char, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_COUNT", event_count, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "EVENT_RANK", event_rank, uint64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_SUM_P1", event_sum_p1, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_SUM_P2", event_sum_p2, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_SUM_P3", event_sum_p3, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "EVENT_SUM_TIME_WAITED", event_sum_time_waited, int64_t);

            if (event_rank > 1) {
              row_count = 0;
              row_count_char[0] = '\0';
            }

            char row_ratio_char[64] = "";
            calc_ratio(row_count, num_samples, row_ratio_char);

            char event_ratio_char[64] = "";
            calc_ratio(event_count, num_samples, event_ratio_char);
            
            //ash采样到一个等待事件可以认为这个等待事件花费了1秒
            //对于时长t > 1秒等待事件，ash采样n次就可以认为这个等待事件花费了n秒
            //对于时长t < 1秒的等待事件，ash采样到这个事件的概率只有t，因此也可以认为采样一次这个事件花费了1秒
            //IO等待事件大多数很短，对于p1,p2,p3它只记录了一次等待事件三个阶段消耗的时间，因此而忽略了采样到等待事件的概率远小于1
            //因此这里计算p1,p2,p3相对于整个等待事件的比例，然后利用等待事件的时长估算p1,p2,p3的时长
            char event_sum_p1_s_char[64] = "";
            calc_avg_active_sessions(event_sum_p1 * event_count, event_sum_time_waited, event_sum_p1_s_char);

            char event_sum_p2_s_char[64] = "";
            calc_avg_active_sessions(event_sum_p2 * event_count, event_sum_time_waited, event_sum_p2_s_char);

            char event_sum_p3_s_char[64] = "";
            calc_avg_active_sessions(event_sum_p3 * event_count, event_sum_time_waited, event_sum_p3_s_char);

            char program_module_action_char[ASH_PROGRAM_STR_LEN + ASH_MODULE_STR_LEN + ASH_ACTION_STR_LEN + 3] = "";
            if (OB_SUCC(ret) && module_char[0] != '\0') {
              snprintf(program_module_action_char, ASH_PROGRAM_STR_LEN + ASH_MODULE_STR_LEN + ASH_ACTION_STR_LEN + 3, 
                      "%s/%s/%s", program_char, module_char, action_char);
              AshColumnItem column_content[] = {
                node_char, program_module_action_char, plan_hash_char, row_count_char, row_ratio_char, event_name_char,
                type_char, event_ratio_char, event_sum_p1_s_char, event_sum_p2_s_char, event_sum_p3_s_char
              };
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            } else if (OB_SUCC(ret) && sql_id_char[0] != '\0') {
              AshColumnItem column_content[] = {
                node_char, sql_id_char, plan_hash_char, row_count_char, row_ratio_char, event_name_char,
                type_char, event_ratio_char, event_sum_p1_s_char, event_sum_p2_s_char, event_sum_p3_s_char
              };
              column_content[1].href_sql_id_ = true;
              AshRowItem ash_row(column_size, column_content, column_widths, with_color);
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, ash_row))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(print_section_column_end(ash_report_params, buff, headers))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace pl
}  // namespace oceanbase
