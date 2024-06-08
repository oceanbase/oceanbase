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

#include "observer/ob_srv_network_frame.h"  // ObSrvNetworkFrame
#include "observer/ob_server_struct.h"
#include "share/wr/ob_wr_task.h"
#include "share/wr/ob_wr_rpc_proxy.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "pl/sys_package/ob_dbms_workload_repository.h"
#include "lib/string/ob_strings.h"
#include "share/ob_time_utility2.h"
#include <cstring>
#include <stdio.h>
#include "share/ob_lob_access_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "share/ob_version.h"
#include <sys/utsname.h>

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace share::schema;
class ObSrvNetworkFrame;
namespace pl
{
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
      ObMySQLResult *result = nullptr;
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
      ObMySQLResult *result = nullptr;
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

    // calc ASH_BEGIN_TIME and ASH_END_TIME
    int64_t ash_begin_time = 0;
    int64_t ash_end_time = 0;
    if (OB_FAIL(print_ash_report_header(ash_report_params, buff))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_ash_begin_and_end_time(ash_report_params, ash_begin_time, ash_end_time))) {
        LOG_WARN("failed to get ash begin time and end time", K(ret));
      } else {
        ash_report_params.ash_begin_time = ash_begin_time;
        ash_report_params.ash_end_time = ash_end_time;
      }
    }

    // calc num_samples and num_events
    int64_t num_samples = 0;
    int64_t num_events = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_ash_num_samples_and_events(ash_report_params, num_samples, num_events))) {
        LOG_WARN("failed to get num_events and num_samples", K(ret));
      }
    }

    // print ash summary info
    int64_t dur_elapsed_time =
        (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000LL;
    bool no_data = false;  // if no data, will just print ash summary info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_ash_summary_info(ash_report_params, ash_report_params.user_input_ash_begin_time,
              ash_report_params.user_input_ash_end_time, dur_elapsed_time, num_samples, num_events, buff,
              no_data))) {
        LOG_WARN("failed to print ash summary info", K(ret));
      }
    }

    // print other infos
    if (OB_SUCC(ret) && !no_data) {
      if (OB_FAIL(print_ash_top_active_tenants(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top active tenants", K(ret));
      } else if (OB_FAIL(print_ash_top_node_load(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_foreground_db_time(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_execution_phase(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_background_db_time(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_sessions(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_group(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_top_latches(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_ash_activity_over_time(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_command_type(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_with_top_db_time(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_with_top_wait_events(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_with_top_operator(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_plsql(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top node load", K(ret));
      } else if (OB_FAIL(print_top_sql_text(
                     ash_report_params, num_samples, num_events, buff))) {
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

int ObDbmsWorkloadRepository::format_row(const int64_t column_size, const char *column_contents[],
    const int64_t column_widths[], const char *pad, const char *sep, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buff.append(sep))) {
    LOG_WARN("failed to append string into buff", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_size; i++) {
    if (OB_FAIL(lpad(column_contents[i], column_widths[i], pad, buff))) {
      LOG_WARN("failed to calc lpad ", K(i), K(ret));
    } else if (OB_FAIL(buff.append(sep))) {
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

const char *oracle_table = " FROM SYS.GV$ACTIVE_SESSION_HISTORY ASH";

const char *ASH_VIEW_SQL_424 =
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
"  ASH.EVENT AS EVENT,"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
"  ASH.WAIT_CLASS AS WAIT_CLASS,"
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
"  ASH.TIME_MODEL AS TIME_MODEL"
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
"  ASH.EVENT AS EVENT,"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
"  ASH.WAIT_CLASS AS WAIT_CLASS,"
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
"  0 AS TIME_MODEL"
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
"  ASH.EVENT AS EVENT,"
"  ASH.EVENT_NO AS EVENT_NO,"
"  ASH.EVENT_ID AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
"  ASH.WAIT_CLASS AS WAIT_CLASS,"
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
"  0 AS TIME_MODEL"
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
"  ASH.EVENT AS EVENT,"
"  ASH.EVENT_NO AS EVENT_NO,"
"  0 AS EVENT_ID,"
"  ASH.P1 AS P1,"
"  ASH.P1TEXT AS P1TEXT,"
"  ASH.P2 AS P2,"
"  ASH.P2TEXT AS P2TEXT,"
"  ASH.P3 AS P3,"
"  ASH.P3TEXT AS P3TEXT,"
"  ASH.WAIT_CLASS AS WAIT_CLASS,"
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
"  0 AS TIME_MODEL"
" %s"
" WHERE sample_time between '%.*s' and '%.*s'";

#define EXTRACT_INT_FIELD_FOR_ASH(result, column_name, field, type) \
  if (OB_SUCC(ret)) { \
    if (lib::is_oracle_mode()) {\
      EXTRACT_INT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field , type); \
    } else { \
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type); \
    } \
  }

#define EXTRACT_INT_FIELD_FOR_ASH_STR(result, column_name, field, type)         \
  type field = 0;                                                               \
  char field##_char[64] = "";                                                   \
  if (OB_SUCC(ret)) {                                                           \
    if (lib::is_oracle_mode()) {                                                \
      EXTRACT_INT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field, type); \
    } else {                                                                    \
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);       \
    }                                                                           \
    sprintf(field##_char, "%ld", field);                                        \
  }

#define EXTRACT_UINT_FIELD_FOR_ASH_STR(result, column_name, field, type)         \
  type field = 0;                                                                \
  char field##_char[64] = "";                                                    \
  if (OB_SUCC(ret)) {                                                            \
    if (lib::is_oracle_mode()) {                                                 \
      EXTRACT_UINT_FIELD_FROM_NUMBER_SKIP_RET(result, column_name, field, type); \
    } else {                                                                     \
      EXTRACT_UINT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type);       \
    }                                                                            \
    sprintf(field##_char, "%lu", field);                                         \
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
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_begin_time,
                 ash_begin_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(time_buf_pos = 0)) {
  } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, ash_report_params.ash_end_time,
                 ash_end_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(sprintf(port_buf, "%ld", ash_report_params.port))) {
  } else if (OB_FAIL(sql_string.append_fmt(data_version >= DATA_VERSION_4_2_4_0   ? ASH_VIEW_SQL_424
                                           : data_version == DATA_VERSION_4_2_3_0 ? ASH_VIEW_SQL_423
                                           : data_version == DATA_VERSION_4_2_2_0
                                               ? ASH_VIEW_SQL_422
                                               : ASH_VIEW_SQL_421,
                 lib::is_oracle_mode() ? oracle_table : mysql_table, static_cast<int>(time_buf_pos),
                 ash_begin_time_buf, static_cast<int>(time_buf_pos), ash_end_time_buf))) {
    LOG_WARN("failed to assign query string", K(ret));
  } else {
    if (ash_report_params.svr_ip != "") {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.SVR_IP = '%.*s'",
              ash_report_params.svr_ip.length(), ash_report_params.svr_ip.ptr()))) {
        LOG_WARN("failed to assign query string", K(ret));
      }
    }
    if (ash_report_params.port != -1) {
      if (OB_FAIL(sql_string.append_fmt(" AND ASH.PORT = '%lu'", ash_report_params.port))) {
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

int ObDbmsWorkloadRepository::get_ash_begin_and_end_time(
    const AshReportParams &ash_report_params, int64_t &ash_begin_time, int64_t &ash_end_time)
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
      if (OB_FAIL(sql_string.append("SELECT MIN(SAMPLE_TIME) AS ASH_BEGIN_TIME, MAX(SAMPLE_TIME)"
                                    " AS ASH_END_TIME  FROM   ("))) {
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
  return ret;
}

int ObDbmsWorkloadRepository::get_ash_num_samples_and_events(
    const AshReportParams &ash_report_params, int64_t &num_samples, int64_t &num_events)
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
              "SELECT COUNT(1) AS NUM_SAMPLES, COUNT(1) AS NUM_EVENTS FROM   ("))) {
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
            EXTRACT_INT_FIELD_FOR_ASH(*result, "NUM_EVENTS", num_events, int64_t);
          }
        }  // end while
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_summary_info(const AshReportParams &ash_report_params,
    const int64_t l_btime, const int64_t l_etime, int64_t &dur_elapsed_time, int64_t &num_samples,
    int64_t &num_events, ObStringBuffer &buff, bool &no_data)
{
  int ret = OB_SUCCESS;
  no_data = false;
  const int64_t time_buf_len = 128;
  int64_t l_btime_buf_pos = 0;
  int64_t l_etime_buf_pos = 0;
  int64_t ash_begin_time_buf_pos = 0;
  int64_t ash_end_time_buf_pos = 0;
  char l_btime_buf[time_buf_len] = "";
  char l_etime_buf[time_buf_len] = "";
  char ash_begin_time_buf[time_buf_len] = "";
  char ash_end_time_buf[time_buf_len] = "";
  double avg_active_sess = static_cast<double>(num_samples) / dur_elapsed_time;
  avg_active_sess = round(avg_active_sess * 100) / 100;  // round to two decimal places
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<pre class=\"ash_html\">"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  HEAP_VAR(ObSqlString, temp_string)
  {
    if (OB_FAIL(usec_to_string(
            ash_report_params.tz_info, l_btime, l_btime_buf, time_buf_len, l_btime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(ash_report_params.tz_info, l_etime, l_etime_buf, time_buf_len,
                   l_etime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (ash_report_params.ash_begin_time <= 0 && ash_report_params.ash_end_time <= 0) {
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
    } else if (OB_FAIL(temp_string.append_fmt("    Analysis Begin Time: %.*s \n",
                   static_cast<int>(ash_begin_time_buf_pos), ash_begin_time_buf))) {
      LOG_WARN("failed to assign Analysis Begin Time string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("      Analysis End Time: %.*s \n",
                   static_cast<int>(ash_end_time_buf_pos), ash_end_time_buf))) {
      LOG_WARN("failed to assign Analysis End Time string", K(ret));
    } else if (OB_FAIL(
                   temp_string.append_fmt("           Elapsed Time: %ld \n", dur_elapsed_time))) {
      LOG_WARN("failed to assign Elapsed Time string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("          Num of Sample: %ld \n", num_samples))) {
      LOG_WARN("failed to assign Num of Sample string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("          Num of Events: %ld \n", num_events))) {
      LOG_WARN("failed to assign Num of Events string", K(ret));
    } else if (OB_FAIL(
                   temp_string.append_fmt("Average Active Sessions: %.2f \n", avg_active_sess))) {
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
      if (num_events <= 0) {
        num_events = 1;
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
  double ratio = static_cast<double>(dividend) / divisor;
  ratio = round(1000 * 100 * ratio) / 1000;
  sprintf(ratio_char, "%.2f%%", ratio);
}

inline void calc_avg_avtive_sessions(int64_t dividend, int64_t divisor, char *ratio_char)
{
  double ratio = static_cast<double>(dividend) / divisor;
  ratio = round(1000 * ratio) / 1000;
  sprintf(ratio_char, "%.2f", ratio);
}

int ObDbmsWorkloadRepository::print_ash_top_active_tenants(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Active Tenants"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top active tenant information"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Total Count: num of records during ash report analysis time period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Wait Event Count: num of records when session is on wait event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "On CPU Count: num of records when session is on cpu"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "% Activity: activity(cpu + wait) percentage for given tenant"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 7;
    const int64_t column_widths[column_size] = {9, 12, 18, 23, 19, 20, 11};
    const char *column_headers[column_size] = {"Tenant ID", "Session Type", "Total Count",
        "Wait Event Count", "On CPU Count", "Avg Active Sessions", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "SELECT TENANT_ID, SESSION_TYPE, COUNT(*) AS CNT, %s FROM (",
                     lib::is_oracle_mode()
                         ? "sum(decode(event_no, 0, 1, 0)) as CPU_CNT, "
                           "sum(decode(event_no, 0, 0, 1)) as WAIT_CNT"
                         : "cast(sum(if(event_no = 0, 1, 0)) as signed integer) as CPU_CNT, "
                           "cast(sum(if(event_no = 0, 0, 1)) as signed integer) as WAIT_CNT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event  GROUP BY tenant_id, session_type ORDER BY cnt DESC"))) {
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
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "TENANT_ID", tenant_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);

            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {ASH_FIELD_CHAR(tenant_id), session_type_char,
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  avg_active_sessions_char, activity_radio_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_node_load(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Node Load"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top node measured by DB time"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "IP: OceanBase instance svr_ip"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Port: OceanBase instance svr_port"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Total Count: num of records during ash report analysis time period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Wait Event Count: num of records when session is on wait event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "On CPU Count: num of records when session is on cpu"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "% Activity: activity(cpu + wait) percentage for given tenant"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 8;
    const int64_t column_widths[column_size] = {16, 7, 12, 18, 23, 19, 20, 11};
    const char *column_headers[column_size] = {"IP", "Port", "Session Type", "Total Count",
        "Wait Event Count", "On CPU Count", "Avg Active Sessions", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "SELECT SVR_IP, SVR_PORT, SESSION_TYPE, COUNT(*) AS CNT, %s FROM (",
                     lib::is_oracle_mode()
                         ? "sum(decode(event_no, 0, 1, 0)) as CPU_CNT, "
                           "sum(decode(event_no, 0, 0, 1)) as WAIT_CNT"
                         : "cast(sum(if(event_no = 0, 1, 0)) as signed integer) as CPU_CNT, "
                           "cast(sum(if(event_no = 0, 0, 1)) as signed integer) as WAIT_CNT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event  GROUP BY svr_ip, svr_port, session_type ORDER BY cnt DESC"))) {
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
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SVR_IP", ip_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SVR_PORT", port, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);
            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {ip_char, ASH_FIELD_CHAR(port), session_type_char,
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  avg_active_sessions_char, activity_radio_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_foreground_db_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Foreground DB Time"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "this section lists top foreground db time categorized by event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Name: comprise wait event and on cpu event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 5;
    const int64_t column_widths[column_size] = {64, 20, 13, 20, 11};
    const char *column_headers[column_size] = {
        "Event Name", "Wait Class", "Event Count", "Avg Active Sessions", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt("SELECT * FROM (SELECT %s, %s, count(*) as CNT FROM (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                  : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS"
                  : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and session_type='FOREGROUND') top_event GROUP BY "
                                           "EVENT_NO, WAIT_CLASS, EVENT ORDER BY cnt DESC)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {event_char, wait_class_char, ASH_FIELD_CHAR(cnt),
                  avg_active_sessions_char, event_radio_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
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

int ObDbmsWorkloadRepository::print_ash_top_execution_phase(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Execution Phase"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "this section lists top phases of execution, such as SQL, PL/SQL, STORAGE, "
                 "etc."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 5;
    const int64_t column_widths[column_size] = {12, 40, 10, 11, 20};
    const char *column_headers[column_size] = {
        "Session Type", "Phase of Execution", "Count", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt("SELECT SESSION_TYPE, %s COUNT(*) AS CNT FROM (",
              lib::is_oracle_mode()
                  ? "SUM(CASE WHEN BITAND(TIME_MODEL, 1) = 0 THEN 0 ELSE 1 END) AS IN_PARSE, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 2) = 0 THEN 0 ELSE 1 END) AS IN_PL_PARSE, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 4) = 0 THEN 0 ELSE 1 END) AS IN_PLAN_CACHE, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 8) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_SQL_OPTIMIZE, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 16) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_SQL_EXECUTION, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 32) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_PX_EXECUTION, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 64) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_SEQUENCE_LOAD, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 128) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_COMMITTING, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 256) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_STORAGE_READ, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 512) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_STORAGE_WRITE, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 1024) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_REMOTE_DAS_EXECUTION, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 8192) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_FILTER_ROWS, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 2048) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_PLSQL_COMPILATION, "
                    "SUM(CASE WHEN BITAND(TIME_MODEL, 4096) = 0 THEN 0 ELSE 1 END) AS "
                    "IN_PLSQL_EXECUTION, "
                  : "CAST(SUM(CASE WHEN (time_model & 1) = 0 THEN 0 ELSE 1 "
                    "END)AS SIGNED INTEGER) IN_PARSE, "
                    "CAST(SUM(CASE WHEN (time_model & 2) = 0 THEN 0 ELSE 1 "
                    "END)AS SIGNED INTEGER) IN_PL_PARSE, "
                    "CAST(SUM(CASE WHEN (time_model & 4) = 0 THEN 0 ELSE 1 "
                    "END)AS SIGNED INTEGER) IN_PLAN_CACHE, "
                    "CAST(SUM(CASE WHEN (time_model & 8) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_SQL_OPTIMIZE, "
                    "CAST(SUM(CASE WHEN (time_model & 16) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_SQL_EXECUTION, "
                    "CAST(SUM(CASE WHEN (time_model & 32) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_PX_EXECUTION, "
                    "CAST(SUM(CASE WHEN (time_model & 64) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_SEQUENCE_LOAD, "
                    "CAST(SUM(CASE WHEN (time_model & 128) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_COMMITTING, "
                    "CAST(SUM(CASE WHEN (time_model & 256) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_STORAGE_READ, "
                    "CAST(SUM(CASE WHEN (time_model & 512) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_STORAGE_WRITE, "
                    "CAST(SUM(CASE WHEN (time_model & 1024) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_REMOTE_DAS_EXECUTION, "
                    "CAST(SUM(CASE WHEN (time_model & 8192) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_FILTER_ROWS, "
                    "CAST(SUM(CASE WHEN (time_model & 2048) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_PLSQL_COMPILATION, "
                    "CAST(SUM(CASE WHEN (time_model & 4096) = 0 THEN 0 ELSE "
                    "1 END)AS SIGNED INTEGER) IN_PLSQL_EXECUTION, "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(
                     sql_string.append(") top_event GROUP BY session_type ORDER BY cnt DESC"))) {
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
            ObArray<std::pair<const char *, int64_t>> phase_array;
            char session_type_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "SESSION_TYPE", session_type_char, 64, tmp_real_str_len);

            EXTRACT_ASH_EXEC_PHASE(IN_PARSE)
            EXTRACT_ASH_EXEC_PHASE(IN_PL_PARSE)
            EXTRACT_ASH_EXEC_PHASE(IN_PLAN_CACHE)
            EXTRACT_ASH_EXEC_PHASE(IN_SQL_OPTIMIZE)
            EXTRACT_ASH_EXEC_PHASE(IN_SQL_EXECUTION)
            EXTRACT_ASH_EXEC_PHASE(IN_PX_EXECUTION)
            EXTRACT_ASH_EXEC_PHASE(IN_SEQUENCE_LOAD)
            EXTRACT_ASH_EXEC_PHASE(IN_COMMITTING)
            EXTRACT_ASH_EXEC_PHASE(IN_STORAGE_READ)
            EXTRACT_ASH_EXEC_PHASE(IN_STORAGE_WRITE)
            EXTRACT_ASH_EXEC_PHASE(IN_REMOTE_DAS_EXECUTION)
            EXTRACT_ASH_EXEC_PHASE(IN_PLSQL_COMPILATION)
            EXTRACT_ASH_EXEC_PHASE(IN_PLSQL_EXECUTION)
            EXTRACT_ASH_EXEC_PHASE(IN_FILTER_ROWS)

            std::sort(phase_array.begin(), phase_array.end(), phase_cmp_func);
            for (int64_t i = 0; OB_SUCC(ret) && i < phase_array.count(); i++) {
              const char *phase_name = phase_array.at(i).first;
              int64_t phase_cnt = phase_array.at(i).second;
              if (0 == phase_cnt) {
                continue;
              }
              char sample_radio_char[64] = "";
              calc_ratio(phase_cnt, num_samples, sample_radio_char);

              char phase_cnt_char[64] = "";
              sprintf(phase_cnt_char, "%ld", phase_cnt);

              char avg_active_sessions_char[64] = "";
              calc_avg_avtive_sessions(phase_cnt,
                  (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                  avg_active_sessions_char);

              if (OB_SUCC(ret)) {
                const char *column_content[] = {session_type_char, phase_name, phase_cnt_char,
                    sample_radio_char, avg_active_sessions_char};
                if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                        column_content, column_widths, with_color))) {
                  LOG_WARN("failed to format row", K(ret));
                }
                with_color = !with_color;
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
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
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Background DB Time"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "this section lists top DB Time for background sessions"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Program: process name for background sessions"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Name: comprise wait event and on cpu event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {65, 64, 20, 13, 11, 20};
    const char *column_headers[column_size] = {
        "Program", "Event Name", "Wait Class", "Event Count", " % Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "SELECT * FROM (SELECT PROGRAM, %s, %s, COUNT(*) AS CNT FROM (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                  : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS"
                  : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and session_type='BACKGROUND') top_event GROUP BY "
                                           "program, EVENT_NO, EVENT, WAIT_CLASS "
                                           "having count(*) > 1 ORDER BY cnt DESC)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            char program_char[64 + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "PROGRAM", program_char, 64, tmp_real_str_len);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {program_char, event_char, wait_class_char,
                  ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_sessions(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Sessions"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top Sessions"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Session ID: user session id"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Name: comprise wait event and on cpu event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 7;
    const int64_t column_widths[column_size] = {20, 64, 64, 20, 13, 11, 20};
    const char *column_headers[column_size] = {"Session ID", "Program", "Event Name", "Wait Class",
        "Event Count", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "SELECT * FROM (SELECT SESSION_ID, PROGRAM, %s, %s, COUNT(*) AS CNT FROM (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                  : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS"
                  : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     " and session_type='FOREGROUND') top_event GROUP BY session_id, "
                     "program, EVENT_NO, EVENT, WAIT_CLASS "
                     "having count(*) > 1 ORDER BY cnt DESC)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            char program_char[64 + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "PROGRAM", program_char, 64, tmp_real_str_len);
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "SESSION_ID", session_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {ASH_FIELD_CHAR(session_id), program_char, event_char,
                  wait_class_char, ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_group(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Groups"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top resource consumer groups"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Group ID: resource consumer group id"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Name: comprise wait event and on cpu event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {10, 64, 20, 13, 11, 20};
    const char *column_headers[column_size] = {
        "Group ID", "Event Name", "Wait Class", "Event Count", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "SELECT * FROM (SELECT GROUP_ID, %s, %s, COUNT(*) AS CNT FROM (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                  : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS WAIT_CLASS"
                  : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS WAIT_CLASS"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(
                     sql_string.append(") top_event GROUP BY group_id, EVENT_NO, EVENT, WAIT_CLASS "
                                       "having count(*) > 1 ORDER BY cnt DESC)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 200 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 200 "))) {
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
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "GROUP_ID", group_id, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {ASH_FIELD_CHAR(group_id), event_char, wait_class_char,
                  ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_latches(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top Latchs"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top latches"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Latch Wait Event: event that waiting for latch"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 4;
    const int64_t column_widths[column_size] = {64, 13, 11, 20};
    const char *column_headers[column_size] = {
        "Latch Wait Event", "Event Count", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt("SELECT * FROM (SELECT %s, COUNT(*) AS CNT FROM (",
                     lib::is_oracle_mode()
                         ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                         : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event where event like 'latch: %' GROUP BY EVENT_NO, EVENT, WAIT_CLASS "
                     "having count(*) > 1 ORDER BY cnt DESC)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {
                  event_char, ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_activity_over_time(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Activity Over Time"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "this section lists time slot information during the analysis period."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Slot Begin Time: current slot's begin time. current slot end with next slot begin "
                 "time."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Name: comprise wait event and on cpu event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Event Count: num of sampled session activity records"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {28, 64, 20, 13, 11, 20};
    const char *column_headers[column_size] = {"Slot Begin Time", "Event Name", "Wait Class",
        "Event Count", "% Activity", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "select * from (select TIMEKEY, EVENT, WAIT_CLASS, CNT, ROW_NUMBER() OVER (PARTITION "
              "BY TIMEKEY ORDER BY cnt DESC) AS n from (SELECT TIMEKEY, %s, %s, COUNT(*) AS CNT "
              "FROM (SELECT %s as TIMEKEY, EVENT_NO, EVENT, WAIT_CLASS from (",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS event"
                  : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS event",
              lib::is_oracle_mode()
                  ? "CAST(DECODE(EVENT_NO, 0, 'NULL', WAIT_CLASS) AS VARCHAR2(64)) AS wait_class"
                  : "CAST(IF (EVENT_NO = 0, 'NULL', WAIT_CLASS) AS CHAR(64)) AS wait_class",
              lib::is_oracle_mode()
                  ? "cast(to_date('19700101', 'YYYYMMDD') + (cast((cast(sample_time as DATE) - "
                    "DATE '1970-01-01') * 86400 as integer) - "
                    "MOD(cast((cast(sample_time as DATE) - DATE '1970-01-01') * 86400 as integer), "
                    "(5 * 60))) / 86400 as VARCHAR2(64))"
                  : "cast(from_unixtime(unix_timestamp(sample_time) - "
                    "unix_timestamp(sample_time) % (5 * 60)) as char(64))" /*5 minutes slot*/
              ))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(
                     sql_string.append(")) "
                                       "top_event GROUP BY timekey, EVENT_NO, EVENT, "
                                       "WAIT_CLASS having count(*) > 1 ORDER BY timekey)) where n "
                                       "<= 10"))) {  // top 10 row for each slot
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" and ROWNUM <= 500 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 500 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, request_tenant_id, sql_string.ptr()))) {
        LOG_WARN("falied to execute sql", KR(ret), K(request_tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(request_tenant_id), K(sql_string));
      } else {
        char prev_timekey_char[65] = "";
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
            char timekey_char[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "TIMEKEY", timekey_char, 64, tmp_real_str_len);
            char event_char[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event_char, 64, tmp_real_str_len);
            char wait_class_char[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class_char, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);

            char event_radio_char[64] = "";
            calc_ratio(cnt, num_samples, event_radio_char);

            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt, 5 * 60, /*5 mins*/
                avg_active_sessions_char);
            if (0 == strncmp(prev_timekey_char, timekey_char, 65)) {
              timekey_char[0] = '\0';
            } else {
              MEMCPY(prev_timekey_char, timekey_char, sizeof(prev_timekey_char));
            }
            if (OB_SUCC(ret)) {
              const char *column_content[] = {timekey_char, event_char, wait_class_char,
                  ASH_FIELD_CHAR(cnt), event_radio_char, avg_active_sessions_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_db_time(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top SQL with Top DB Time"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "This Section lists the SQL statements that accounted for the highest percentages "
                 "of sampled session activity."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Plan Hash: Numeric representation of the current SQL plan."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Total Count: num of records during ash report analysis time period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Wait Event Count: num of records when session is on wait event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "On CPU Count: num of records when session is on cpu"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 7;
    const int64_t column_widths[column_size] = {40, 20, 18, 23, 19, 12, 64};
    const char *column_headers[column_size] = {"SQL ID", "Plan Hash", "Total Count",
        "Wait Event Count", "On CPU Count", "% Activity", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
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
                     "FROM (SELECT SQL_ID, PLAN_HASH, %s, COUNT(*) AS cnt FROM (",
                     lib::is_oracle_mode()
                         ? "sum(decode(event_no, 0, 1, 0)) as cpu_cnt, "
                           "sum(decode(event_no, 0, 0, 1)) as wait_cnt"
                         : "cast(sum(if(event_no = 0, 1, 0)) as signed integer) as cpu_cnt, "
                           "cast(sum(if(event_no = 0, 0, 1)) as signed integer) as wait_cnt"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and plan_hash is not null) top_event GROUP BY SQL_ID, "
                                           "PLAN_HASH having count(*) > 1) ash "))) {
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
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 63, tmp_real_str_len);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {sql_id, ASH_FIELD_CHAR(plan_hash),
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  activity_radio_char, query_sql};
              if (OB_FAIL(print_sql_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color, 0, 6))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_wait_events(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top SQL with Top Events"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "This Section lists the SQL statements that accounted for the highest percentages "
                 "event."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Plan Hash: Numeric representation of the current SQL plan"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event: top event for current SQL plan"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Event Count: num of samples for top event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {40, 20, 64, 14, 12, 64};
    const char *column_headers[column_size] = {
        "SQL ID", "Plan Hash", "Event", "Event Count", "% Activity", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(
                     sql_string.append_fmt("SELECT SQL_ID, PLAN_HASH, QUERY_SQL, EVENT, CNT from "
                                           "(SELECT SQL_ID, PLAN_HASH, "
                                           "QUERY_SQL, EVENT, CNT, ROW_NUMBER() OVER (PARTITION "
                                           "BY PLAN_HASH, EVENT ORDER BY CNT DESC) AS n FROM "
                                           "(SELECT ash.SQL_ID, ash.PLAN_HASH, cnt, "
                                           "%s, ",
                         lib::is_oracle_mode()
                             ? "CAST(DECODE(EVENT_NO, 0, 'ON CPU', EVENT) AS VARCHAR2(64)) AS EVENT"
                             : "CAST(IF (EVENT_NO = 0, 'ON CPU', EVENT) AS CHAR(64)) AS EVENT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(pc.QUERY_SQL, CHR(10), '''')), 0, 55) QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(QUERY_SQL, CHAR(10), '''')), 1, 55) AS QUERY_SQL  "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("FROM (SELECT SQL_ID, PLAN_HASH, "
                                           "event, event_no, COUNT(*) AS cnt FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(
                     sql_string.append(" and plan_hash is not null) top_event GROUP BY SQL_ID, "
                                       "PLAN_HASH, event_no, event having count(*) > 1) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(
                     sql_string.append_fmt("LEFT JOIN %s pc ON ash.sql_id = pc.sql_id AND "
                                           "ash.plan_hash = pc.plan_hash ORDER BY cnt DESC) v1) ",
                         lib::is_oracle_mode()
                             ? "(SELECT * FROM (SELECT SQL_ID, PLAN_HASH, QUERY_SQL, ROW_NUMBER() "
                               "OVER (PARTITION BY SQL_ID, PLAN_HASH ORDER BY SQL_ID, PLAN_HASH) AS N FROM "
                               "SYS.GV$OB_SQLSTAT) WHERE N = 1)"
                             : "(SELECT distinct SQL_ID, PLAN_HASH, QUERY_SQL "
                               "FROM oceanbase.GV$OB_SQLSTAT)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(" WHERE n <= 1 and ROWNUM <= 100 order by cnt desc "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(" WHERE n <= 1 order by cnt desc LIMIT 100 "))) {
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
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);

            tmp_real_str_len = 0;
            char event_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "EVENT", event_char, 63, tmp_real_str_len);
            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 63, tmp_real_str_len);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {sql_id, ASH_FIELD_CHAR(plan_hash), event_char,
                  ASH_FIELD_CHAR(cnt), activity_radio_char, query_sql};
              if (OB_FAIL(print_sql_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color, 0, 5))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_with_top_operator(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top SQL with Top Operator"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "This Section lists the SQL statements that accounted for the highest percentages "
                 "of sampled session activity with sql operator"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Plan Hash: Numeric representation of the current SQL plan"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Operator: top operator name for current SQL plan"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "Count: num of samples for top current SQL operator"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "% Activity: activity percentage for given event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {40, 20, 128, 14, 14, 64};
    const char *column_headers[column_size] = {
        "SQL ID", "Plan Hash", "Operator", "Count", "% Activity", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "SELECT SQL_ID, PLAN_HASH, QUERY_SQL, OPERATOR, CNT FROM (SELECT SQL_ID, "
                     "PLAN_HASH, "
                     "QUERY_SQL, OPERATOR, GROUP_CNT, CNT, ROW_NUMBER() OVER (PARTITION "
                     "BY SQL_ID, PLAN_HASH, SQL_PLAN_LINE_ID ORDER BY CNT DESC) AS n FROM (SELECT "
                     "ASH.SQL_ID, ASH.PLAN_HASH, ASH.SQL_PLAN_LINE_ID, SP.OPERATOR, CNT, SUM(CNT) "
                     "OVER(PARTITION by ASH.SQL_ID, ASH.PLAN_HASH) AS GROUP_CNT,"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(pc.QUERY_SQL, CHR(10), '''')), 0, 55) QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " SUBSTR(TRIM(REPLACE(QUERY_SQL, CHAR(10), '''')), 1, 55) AS QUERY_SQL  "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("FROM (SELECT SQL_ID, PLAN_HASH, "
                                           "sql_plan_line_id, COUNT(*) AS cnt FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(
                     sql_string.append(" and plan_hash is not null) top_event GROUP BY SQL_ID, "
                                       "PLAN_HASH, sql_plan_line_id having count(*) > 1) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (
          OB_FAIL(sql_string.append_fmt(
              "LEFT JOIN %s pc ON ash.sql_id = pc.sql_id AND "
              "ash.plan_hash = pc.plan_hash LEFT JOIN %s sp ON ash.sql_id=sp.sql_id AND "
              "ash.plan_hash = sp.plan_hash AND ash.sql_plan_line_id = sp.id) v1) ",
              lib::is_oracle_mode()
                  ? "(SELECT * FROM (SELECT SQL_ID, PLAN_HASH, QUERY_SQL, ROW_NUMBER() "
                    "OVER (PARTITION BY SQL_ID, PLAN_HASH ORDER BY SQL_ID, PLAN_HASH) AS N FROM "
                    "SYS.GV$OB_SQLSTAT) WHERE N = 1)"
                  : "(SELECT distinct SQL_ID, PLAN_HASH, QUERY_SQL FROM "
                    "oceanbase.GV$OB_SQLSTAT)",
              lib::is_oracle_mode()
                  ? "(SELECT * FROM (SELECT SQL_ID, PLAN_HASH, ID, OPERATOR, ROW_NUMBER() "
                    "OVER (PARTITION BY SQL_ID, PLAN_HASH, ID, OPERATOR ORDER BY SQL_ID, PLAN_HASH) AS N FROM "
                    "SYS.GV$OB_SQL_PLAN) WHERE N = 1)"
                  : "(select distinct plan_hash, sql_id, id, operator "
                    "from oceanbase.__ALL_VIRTUAL_SQL_PLAN)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(" WHERE n <= 5 and ROWNUM <= 100 ORDER BY GROUP_CNT "
                                           "DESC, SQL_ID DESC, CNT DESC "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() &&
                 OB_FAIL(sql_string.append(
                     " WHERE n <= 5 ORDER BY GROUP_CNT DESC, SQL_ID DESC, CNT DESC LIMIT 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
      } else {
        LOG_INFO("[roland_debug]", K(sql_string));
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
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_UINT_FIELD_FOR_ASH_STR(*result, "PLAN_HASH", plan_hash, uint64_t);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);

            tmp_real_str_len = 0;
            char operator_char[256] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "OPERATOR", operator_char, 255, tmp_real_str_len);
            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 63, tmp_real_str_len);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {sql_id, ASH_FIELD_CHAR(plan_hash), operator_char,
                  ASH_FIELD_CHAR(cnt), activity_radio_char, query_sql};
              if (OB_FAIL(print_sql_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color, 0, 5))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_command_type(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top SQL Statement Types"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "this section lists top sql statement type."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "SQL Statement Type: SQL statement types such as SELECT or UPDATE"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Total Count: num of records during ash report analysis time period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Wait Event Count: num of records when session is on wait event"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(
                 ash_report_params, buff, "On CPU Count: num of records when session is on cpu"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "Avg Active Sessions: average active sessions during ash report analysis time "
                 "period"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "% Activity: activity(cpu + wait) percentage for given tenant"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    bool with_color = true;
    const uint64_t request_tenant_id = MTL_ID();
    const int64_t column_size = 6;
    const int64_t column_widths[column_size] = {45, 18, 23, 19, 20, 11};
    const char *column_headers[column_size] = {"SQL Statement Type", "Total Count",
        "Wait Event Count", "On CPU Count", "Avg Active Sessions", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt("SELECT STMT_TYPE, COUNT(*) AS CNT, %s FROM (",
                     lib::is_oracle_mode()
                         ? "sum(decode(event_no, 0, 1, 0)) as CPU_CNT, "
                           "sum(decode(event_no, 0, 0, 1)) as WAIT_CNT"
                         : "cast(sum(if(event_no = 0, 1, 0)) as signed integer) as CPU_CNT, "
                           "cast(sum(if(event_no = 0, 0, 1)) as signed integer) as WAIT_CNT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and STMT_TYPE is not null) top_event  GROUP BY "
                                           "STMT_TYPE ORDER BY cnt DESC"))) {
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
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "STMT_TYPE", stmt_type, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CNT", cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "CPU_CNT", cpu_cnt, int64_t);
            EXTRACT_INT_FIELD_FOR_ASH_STR(*result, "WAIT_CNT", wait_cnt, int64_t);

            char activity_radio_char[64] = "";
            calc_ratio(cnt, num_samples, activity_radio_char);
            char avg_active_sessions_char[64] = "";
            calc_avg_avtive_sessions(cnt,
                (ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000,
                avg_active_sessions_char);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {
                  ObResolverUtils::get_stmt_type_string(static_cast<stmt::StmtType>(stmt_type))
                      .ptr(),
                  ASH_FIELD_CHAR(cnt), ASH_FIELD_CHAR(wait_cnt), ASH_FIELD_CHAR(cpu_cnt),
                  avg_active_sessions_char, activity_radio_char};
              if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                      column_content, column_widths, with_color))) {
                LOG_WARN("failed to format row", K(ret));
              }
              with_color = !with_color;
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_plsql(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get_min_data_version failed", K(ret), K(MTL_ID()));
  } else if (data_version < DATA_VERSION_4_2_2_0) {
    // do not support.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(print_section_header(ash_report_params, buff, "Top PL/SQL Procedures"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_begin(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "\"PL/SQL Entry Subprogram\" represents the application's top-level "
                 "entry-point(procedure, function, trigger, package initialization) into "
                 "PL/SQL."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "\"PL/SQL Current Subprogram\" is the pl/sql subprogram being executed at the "
                 "point of sampling . If the value is \"SQL\", it represents the percentage of "
                 "time spent executing SQL for the particular plsql entry subprogram."))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(insert_section_explaination_line(ash_report_params, buff,
                 "\"PL/SQL Entry Subprogram\" represents the application's top-level subprogram "
                 "name"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(print_section_explaination_end(ash_report_params, buff))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(GCTX.sql_proxy_)));
    ObCommonSqlProxy *sql_proxy =
        lib::is_oracle_mode() ? &oracle_proxy : static_cast<ObCommonSqlProxy *>(GCTX.sql_proxy_);
    const uint64_t tenant_id = MTL_ID();
    const int64_t column_size = 3;
    const int64_t column_widths[column_size] = {60, 60, 20};
    const char *column_headers[column_size] = {
        "PLSQL Entry Subprogram", "PLSQL Current Subprogram", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(print_section_column_header(
              ash_report_params, buff, column_size, column_headers, column_widths))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "SELECT OBJ_OWNER, OBJ_NAME, PLSQL_ENTRY_SUBPROGRAM_NAME, "
                     "PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, ENTRY_CNT "
                     "FROM (SELECT ash.*, obj.OWNER OBJ_OWNER, obj.OBJECT_NAME OBJ_NAME "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "FROM (SELECT PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, "
                     "PLSQL_ENTRY_SUBPROGRAM_NAME, COUNT(1) AS ENTRY_CNT FROM (SELECT * FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") WHERE PLSQL_ENTRY_OBJECT_ID > 0 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event GROUP BY PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, "
                     "PLSQL_ENTRY_SUBPROGRAM_NAME) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "LEFT JOIN (select database_name owner, routine_name object_name, routine_id "
                     "object_id from %s db, %s r where r.database_id = db.database_id "
                     "union select database_name owner, package_name object_name, package_id "
                     "object_id from %s db, %s p where p.database_id = db.database_id) obj "
                     " ON ash.plsql_entry_object_id = obj.object_id "
                     "ORDER BY ENTRY_CNT DESC) v1 ",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT"
                                           : "oceanbase.__all_routine",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                           : "oceanbase.__all_database",
                     lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT"
                                           : "oceanbase.__all_package"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
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
            char obj_owner[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "OBJ_OWNER", obj_owner,
                common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char obj_name[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "OBJ_NAME", obj_name, common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

            tmp_real_str_len = 0;
            char subpro_name[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "\0";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "PLSQL_ENTRY_SUBPROGRAM_NAME", subpro_name,
                common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

            int64_t plsql_entry_object_id = 0;
            char plsql_entry_object_id_char[64] = "";
            EXTRACT_INT_FIELD_FOR_ASH(
                *result, "PLSQL_ENTRY_OBJECT_ID", plsql_entry_object_id, int64_t);
            sprintf(plsql_entry_object_id_char, "%ld", plsql_entry_object_id);

            int64_t plsql_entry_subprogram_id = 0;
            char plsql_entry_subprogram_id_char[64] = "";
            EXTRACT_INT_FIELD_FOR_ASH(
                *result, "PLSQL_ENTRY_SUBPROGRAM_ID", plsql_entry_subprogram_id, int64_t);
            sprintf(plsql_entry_subprogram_id_char, "%ld", plsql_entry_subprogram_id);

            int64_t event_cnt = 0;
            EXTRACT_INT_FIELD_FOR_ASH(*result, "ENTRY_CNT", event_cnt, int64_t);

            char entry_activity[64] = "";
            double entry_act = static_cast<double>(event_cnt) / num_events;
            entry_act = round(100 * 100 * entry_act) / 100;
            sprintf(entry_activity, "%.2f%%", entry_act);

            char pl_name[common::OB_MAX_ASH_PL_NAME_LENGTH * 3] = "";
            if ('\0' == subpro_name[0]) {
              sprintf(pl_name, "%s.%s", obj_owner, obj_name);
            } else {
              sprintf(pl_name, "%s.%s.%s", obj_owner, obj_name, subpro_name);
            }

            if (OB_SUCC(ret)) {
              const char *column_content[] = {pl_name, "-", entry_activity};
              if (OB_FAIL(print_section_column_row(
                      ash_report_params, buff, column_size, column_content, column_widths, true))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              HEAP_VARS_2((ObISQLClient::ReadResult, sub_res), (ObSqlString, sub_sql_string))
              {
                ObMySQLResult *sub_result = nullptr;
                if (OB_FAIL(sub_sql_string.append(
                        "SELECT OBJ_OWNER, OBJ_NAME, PLSQL_SUBPROGRAM_NAME, PLSQL_OBJECT_ID, "
                        "PLSQL_SUBPROGRAM_ID, SUB_CNT "
                        "FROM (SELECT ash.*, obj.OWNER OBJ_OWNER, obj.OBJECT_NAME OBJ_NAME "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(
                               " FROM (SELECT PLSQL_OBJECT_ID, PLSQL_SUBPROGRAM_ID, "
                               "PLSQL_SUBPROGRAM_NAME, COUNT(1) AS SUB_CNT FROM (SELECT * FROM "
                               "("))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sub_sql_string))) {
                  LOG_WARN("failed to append fmt ash view sql", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(") WHERE PLSQL_ENTRY_OBJECT_ID = "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(plsql_entry_object_id_char))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(" AND PLSQL_ENTRY_SUBPROGRAM_ID = "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(plsql_entry_subprogram_id_char))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (!lib::is_oracle_mode() &&
                           OB_FAIL(sql_string.append(" AND PLSQL_OBJECT_ID > 0 AND ((time_model & "
                                                     "4096) > 0 OR (time_model & 2048) > 0 ) "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (lib::is_oracle_mode() &&
                           OB_FAIL(
                               sql_string.append(" AND PLSQL_OBJECT_ID > 0 AND (BITAND(TIME_MODEL, "
                                                 "4096) > 0 OR BITAND(TIME_MODEL, 2048) > 0) "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append(
                               ") top_event GROUP BY PLSQL_OBJECT_ID, PLSQL_SUBPROGRAM_ID, "
                               "PLSQL_SUBPROGRAM_NAME) ash "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sub_sql_string.append_fmt(
                               " LEFT JOIN "
                               " (select database_name owner, routine_name object_name, routine_id "
                               "object_id from %s db, %s r where r.database_id = db.database_id "
                               "union select database_name owner, package_name object_name, "
                               "package_id object_id from %s db, %s p where p.database_id = "
                               "db.database_id) obj "
                               " ON ash.plsql_object_id = obj.object_id ORDER BY SUB_CNT DESC) v1 ",
                               lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                                     : "oceanbase.__all_database",
                               lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT"
                                                     : "oceanbase.__all_routine",
                               lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT"
                                                     : "oceanbase.__all_database",
                               lib::is_oracle_mode() ? "SYS.ALL_VIRTUAL_PACKAGE_REAL_AGENT"
                                                     : "oceanbase.__all_package"))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (lib::is_oracle_mode() &&
                           OB_FAIL(sql_string.append(" WHERE ROWNUM <= 100 "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
                  LOG_WARN("append sql failed", K(ret));
                } else if (OB_FAIL(sql_proxy->read(sub_res, tenant_id, sub_sql_string.ptr()))) {
                  LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
                } else if (OB_ISNULL(sub_result = sub_res.get_result())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
                } else {
                  while (OB_SUCC(ret)) {
                    if (OB_FAIL(sub_result->next())) {
                      if (OB_ITER_END == ret) {
                        ret = OB_SUCCESS;
                        break;
                      } else {
                        LOG_WARN("fail to get next row", KR(ret));
                      }
                    } else {
                      int64_t tmp_real_str_len = 0;
                      char sub_obj_owner[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "\0";
                      EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*sub_result, "OBJ_OWNER", sub_obj_owner,
                          common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

                      tmp_real_str_len = 0;
                      char sub_obj_name[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "\0";
                      EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*sub_result, "OBJ_NAME", sub_obj_name,
                          common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

                      tmp_real_str_len = 0;
                      char subpro_name[common::OB_MAX_ASH_PL_NAME_LENGTH + 1] = "\0";
                      EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*sub_result, "PLSQL_SUBPROGRAM_NAME",
                          subpro_name, common::OB_MAX_ASH_PL_NAME_LENGTH, tmp_real_str_len);

                      int64_t plsql_subprogram_id = 0;
                      char plsql_subprogram_id_char[64] = "";
                      EXTRACT_INT_FIELD_FOR_ASH(
                          *sub_result, "PLSQL_SUBPROGRAM_ID", plsql_subprogram_id, int64_t);
                      sprintf(plsql_subprogram_id_char, "%ld", plsql_subprogram_id);

                      int64_t sub_event_cnt = 0;
                      EXTRACT_INT_FIELD_FOR_ASH(*sub_result, "SUB_CNT", sub_event_cnt, int64_t);

                      char sub_activity[64] = "";
                      double sub_act = static_cast<double>(sub_event_cnt) / num_events;
                      sub_act = round(100 * 100 * sub_act) / 100;
                      sprintf(sub_activity, "%.2f%%", sub_act);

                      char sub_pl_name[common::OB_MAX_ASH_PL_NAME_LENGTH * 3 + 1] = "\0";
                      if ('\0' == subpro_name[0]) {
                        sprintf(sub_pl_name, "%s.%s", obj_owner, obj_name);
                      } else {
                        sprintf(sub_pl_name, "%s.%s.%s", obj_owner, obj_name, subpro_name);
                      }

                      if (OB_SUCC(ret) && sub_event_cnt > 0) {
                        const char *column_content[] = {"-", sub_pl_name, sub_activity};
                        if (OB_FAIL(print_section_column_row(ash_report_params, buff, column_size,
                                column_content, column_widths, true))) {
                          LOG_WARN("failed to format row", K(ret));
                        }
                      }
                    }
                  }
                  if (OB_SUCC(ret)) {
                    sub_sql_string.reset();
                    if (OB_FAIL(sub_sql_string.append(" SELECT COUNT(1) AS SQL_CNT FROM ("))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (OB_FAIL(
                                   append_fmt_ash_view_sql(ash_report_params, sub_sql_string))) {
                      LOG_WARN("failed to append fmt ash view sql", K(ret));
                    } else if (OB_FAIL(sub_sql_string.append(") WHERE PLSQL_ENTRY_OBJECT_ID = "))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (OB_FAIL(sub_sql_string.append(plsql_entry_object_id_char))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (OB_FAIL(
                                   sub_sql_string.append(" AND PLSQL_ENTRY_SUBPROGRAM_ID = "))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (OB_FAIL(sub_sql_string.append(plsql_entry_subprogram_id_char))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (lib::is_oracle_mode() &&
                               OB_FAIL(
                                   sql_string.append(" AND (time_model & 16) > 0 AND (time_model & "
                                                     "4096) = 0 AND (time_model & 2048) = 0 "))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (!lib::is_oracle_mode() &&
                               OB_FAIL(sql_string.append(
                                   " AND BITAND(TIME_MODEL, 16) > 0 AND BITAND(TIME_MODEL, 4096) = "
                                   "0 AND BITAND(TIME_MODEL, 4096) = 0 "))) {
                      LOG_WARN("append sql failed", K(ret));
                    } else if (OB_FAIL(sql_proxy->read(sub_res, tenant_id, sub_sql_string.ptr()))) {
                      LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
                    } else if (OB_ISNULL(sub_result = sub_res.get_result())) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
                    } else {
                      if (OB_FAIL(sub_result->next())) {
                        if (OB_ITER_END == ret) {
                          ret = OB_SUCCESS;
                          break;
                        } else {
                          LOG_WARN("fail to get next row", KR(ret));
                        }
                      } else {
                        int64_t sql_event_cnt = 0;
                        EXTRACT_INT_FIELD_FOR_ASH(*sub_result, "SQL_CNT", sql_event_cnt, int64_t);

                        if (sql_event_cnt > 0) {
                          char sql_activity[64] = "";
                          double sql_act = static_cast<double>(sql_event_cnt) / num_events;
                          sql_act = round(100 * 100 * sql_act) / 100;
                          sprintf(sql_activity, "%.2f%%", sql_act);
                          if (OB_SUCC(ret)) {
                            const char *column_content[] = {"-", "SQL", sql_activity};
                            if (OB_FAIL(print_section_column_row(ash_report_params, buff,
                                    column_size, column_content, column_widths, true))) {
                              LOG_WARN("failed to format row", K(ret));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(
                  print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_top_sql_text(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  const int64_t column_size = 2;
  const int64_t column_widths[column_size] = {64, 64};
  const char *column_headers[column_size] = {"SQL ID", "SQL Text"};

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
      if (OB_FAIL(sql_string.append("SELECT SQL_ID, SUBSTR(QUERY_SQL, 1 ,4000) AS QUERY_SQL FROM "
                                    "(SELECT pc.SQL_ID SQL_ID, pc.QUERY_SQL QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("FROM (SELECT SQL_ID, COUNT(*) AS CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" and sql_id is not null) top_event GROUP BY SQL_ID "
                                           "ORDER BY CNT DESC) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt("LEFT JOIN %s pc ON ash.sql_id = pc.sql_id ORDER BY "
                                               "cnt DESC) WHERE QUERY_SQL is not null ",
                     lib::is_oracle_mode()
                         ? "(SELECT * FROM (SELECT SQL_ID, QUERY_SQL, ROW_NUMBER() "
                               "OVER (PARTITION BY SQL_ID ORDER BY SQL_ID) AS N FROM "
                               "SYS.GV$OB_SQLSTAT) WHERE N = 1)"
                         : "(SELECT distinct SQL_ID, QUERY_SQL FROM oceanbase.GV$OB_SQLSTAT)"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(sql_string.append(" AND ROWNUM <= 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (!lib::is_oracle_mode() && OB_FAIL(sql_string.append(" LIMIT 100 "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(tenant_id), K(sql_string));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql_string));
      } else {
        if (OB_SUCC(ret) && ash_report_params.is_html) {
          if (OB_FAIL(print_section_column_header(
                  ash_report_params, buff, column_size, column_headers, column_widths))) {
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
            char sql_id[65] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SQL_ID", sql_id, 64, tmp_real_str_len);

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
                const char *column_content[] = {sql_id, sql_text};
                if (OB_FAIL(print_sqltext_section_column_row(ash_report_params, buff, column_size,
                        column_content, column_widths, with_color))) {
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
      if (OB_FAIL(print_section_column_end(ash_report_params, buff, column_size, column_widths))) {
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
  if ((5 == params.count() && data_version < DATA_VERSION_4_2_3_0) ||
      (7 == params.count() && data_version >= DATA_VERSION_4_2_3_0 &&
          data_version < DATA_VERSION_4_2_4_0) ||
      (9 == params.count() && data_version >= DATA_VERSION_4_2_4_0)) {
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

      if (7 >= params.count() && data_version >= DATA_VERSION_4_2_3_0) {
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
      } else if (9 == params.count() && data_version >= DATA_VERSION_4_2_4_0) {
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
    if (OB_FAIL(buff.append("<!DOCTYPE html><html lang=\"en\"><head><title>ASH Report</title> \
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
          document.addEventListener('DOMContentLoaded', setSectionList); \
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

int ObDbmsWorkloadRepository::print_section_column_header(const AshReportParams &ash_report_params,
    ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
    const int64_t column_widths[])
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<table border=\"1\"><tr>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else {
      SMART_VAR(ObSqlString, temp_string)
      {
        for (int i = 0; i < column_size && OB_SUCC(ret); i++) {
          if (OB_FAIL(temp_string.append_fmt(
                  "<th class=\"ash_htmlbg\" scope=\"col\">%s</th>", column_contents[i]))) {
            LOG_WARN("failed to format string", K(ret));
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
      if (OB_FAIL(buff.append("</tr>"))) {
        LOG_WARN("failed to append string into buff", K(ret));
      }
    }
  } else {
    if (OB_FAIL(print_text_table_frame(column_size, column_widths, buff))) {
      LOG_WARN("failed to format row", K(ret));
    } else if (OB_FAIL(format_row(column_size, column_contents, column_widths, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    } else if (OB_FAIL(print_text_table_frame(column_size, column_widths, buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_section_column_row(const AshReportParams &ash_report_params,
    ObStringBuffer &buff, const int64_t column_size, const char *column_contents[], const int64_t column_widths[], bool with_color)
{
  int ret = OB_SUCCESS;
  const char *color_class = with_color ? "ash_htmlc" : "ash_htmlnc";
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<tr>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else {
      SMART_VAR(ObSqlString, temp_string)
      {
        for (int i = 0; i < column_size && OB_SUCC(ret); i++) {
          if (i == 0) {
            if (OB_FAIL(temp_string.append_fmt("<td scope=\"row\" class='%s'>%s</td>",
                    color_class, column_contents[i]))) {
              LOG_WARN("failed to format string", K(ret));
            }
          } else {
            if (OB_FAIL(temp_string.append_fmt("<td class='%s'>%s</td>",
                  color_class, column_contents[i]))) {
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
    if (OB_FAIL(format_row(
                    column_size, column_contents, column_widths, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_sql_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], bool with_color, int sql_id_column, int query_column)
{
  int ret = OB_SUCCESS;
  const char *color_class = with_color ? "ash_htmlc" : "ash_htmlnc";
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<tr>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else {
      SMART_VAR(ObSqlString, temp_string)
      {
        for (int i = 0; i < column_size && OB_SUCC(ret); i++) {
          if (column_contents[query_column][0] != '\0' && i == sql_id_column) {
            if (i == 0) {
              if (OB_FAIL(temp_string.append_fmt("<td scope=\"row\" class='%s'><a class='ash_html' "
                                                 "href='#%s'>%s</a></td>",
                      color_class, column_contents[i], column_contents[i]))) {
                LOG_WARN("failed to format string", K(ret));
              }
            } else {
              if (OB_FAIL(
                      temp_string.append_fmt("<td class='%s'><a class='awr' href='#%s'>%s</a></td>",
                          color_class, column_contents[i], column_contents[i]))) {
                LOG_WARN("failed to format string", K(ret));
              }
            }
          } else {
            if (i == 0) {
              if (OB_FAIL(temp_string.append_fmt(
                      "<td scope=\"row\" class='%s'>%s</td>", color_class, column_contents[i]))) {
                LOG_WARN("failed to format string", K(ret));
              }
            } else {
              if (OB_FAIL(temp_string.append_fmt(
                      "<td class='%s'>%s</td>", color_class, column_contents[i]))) {
                LOG_WARN("failed to format string", K(ret));
              }
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
    if (OB_FAIL(format_row(
                    column_size, column_contents, column_widths, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_sqltext_section_column_row(const AshReportParams &ash_report_params,
    ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
    const int64_t column_widths[], bool with_color)
{
  int ret = OB_SUCCESS;
  const char *color_class = with_color ? "ash_htmlc" : "ash_htmlnc";
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("<tr>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    } else {
      SMART_VAR(ObSqlString, temp_string)
      {
        for (int i = 0; i < column_size && OB_SUCC(ret); i++) {
          if (i == 0) {
            if (OB_FAIL(temp_string.append_fmt(
                    "<td scope=\"row\" class='%s'><a class=\"ash_html\" name=\"%s\"></a>%s</td>\n",
                    color_class, column_contents[i], column_contents[i]))) {
              LOG_WARN("failed to format string", K(ret));
            }
          } else {
            if (OB_FAIL(temp_string.append_fmt(
                    "<td class='%s'>%s</td>\n", color_class, column_contents[i]))) {
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
    if (OB_FAIL(format_row(
                    column_size, column_contents, column_widths, " ", "|", buff))) {
      LOG_WARN("failed to format row", K(ret));
    }
  }
  return ret;
}


int ObDbmsWorkloadRepository::print_section_column_end(const AshReportParams &ash_report_params, ObStringBuffer &buff, const int64_t column_size, const int64_t column_widths[])
{
  int ret = OB_SUCCESS;
  if (ash_report_params.is_html) {
    if (OB_FAIL(buff.append("</table><p />\n"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  } else {
    if (OB_FAIL(print_text_table_frame(column_size, column_widths, buff))) {
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
    if (OB_FAIL(buff.append("End of Report</body></html>"))) {
      LOG_WARN("failed to append string into buff", K(ret));
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_text_table_frame(const int64_t column_size, const int64_t column_widths[], ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buff.append("+"))) {
    LOG_WARN("failed to append string into buff", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_size; i++) {
    if (OB_FAIL(lpad("-", column_widths[i], "-", buff))) {
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

}  // namespace pl
}  // namespace oceanbase
