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
          continue;
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
          continue;
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
      if (OB_FAIL(sql.assign_fmt("SELECT count(*) as cnt FROM %s where "
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
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
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
          continue;
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
      if (OB_FAIL(sql.assign_fmt("SELECT count(*) as cnt FROM %s where "
                                  "cluster_id=%ld and snap_id between %ld and %ld and status=%ld",
              OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME, cluster_id, low_snap_id, high_snap_id, ObWrSnapshotStatus::DELETED))) {
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
  } else if (OB_UNLIKELY(2 != params.count())) {
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

    if (OB_SUCC(ret)) {
      ObWrUserModifySettingsArg wr_user_modify_settings_arg(
          ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), retention, interval);
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
  AshReportParams ash_report_params;
  uint64_t data_version = OB_INVALID_VERSION;
  ObStringBuffer buff(&ctx.exec_ctx_->get_allocator());
  if (OB_UNLIKELY(5 != params.count())) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("parameters number is wrong", K(ret), K(params.count()));
  } else if (params.at(0).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("first parameters is null", K(ret), K(params.at(0)));
  } else if (params.at(1).is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("second parameters is null", K(ret), K(params.at(1)));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get min data_version failed", KR(ret), K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is too low for wr", K(ctx.exec_ctx_->get_my_session()->get_effective_tenant_id()), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2.1, workload repository not supported");
  } else if (OB_FAIL(params.at(2).get_string(ash_report_params.sql_id))) {
    LOG_WARN("failed to get sql id from params", K(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_string(ash_report_params.trace_id))) {
    LOG_WARN("failed to get trace id from params", K(ret), K(params.at(3)));
  } else if (OB_FAIL(params.at(4).get_string(ash_report_params.wait_class))) {
    LOG_WARN("failed to get wait class from params", K(ret), K(params.at(4)));
  } else if (OB_FAIL(buff.append("\n# ASH Report\n\n"))) {  // print header
    LOG_WARN("failed to append string into buff", K(ret));
  } else {
    ash_report_params.ash_begin_time = params.at(0).get_datetime();
    ash_report_params.ash_end_time = params.at(1).get_datetime();

    // calc ASH_BEGIN_TIME and ASH_END_TIME
    int64_t ash_begin_time = 0;
    int64_t ash_end_time = 0;
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
      if (OB_FAIL(print_ash_summary_info(ash_report_params, params.at(0).get_datetime(),
              params.at(1).get_datetime(), dur_elapsed_time, num_samples, num_events, buff,
              no_data))) {
        LOG_WARN("failed to print ash summary info", K(ret));
      }
    }

    // print other infos
    if (OB_SUCC(ret) && !no_data) {
      if (OB_FAIL(
              print_ash_top_user_event_info(ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top user event info", K(ret));
      } else if (OB_FAIL(print_ash_top_events_and_value(
                     ash_report_params, num_samples, num_events, buff))) {
        LOG_WARN("failed to print ash top event and p1/p2/p3 value", K(ret));
      } else if (OB_FAIL(print_ash_top_exec_phase(
                     ash_report_params, num_samples, dur_elapsed_time, buff))) {
        LOG_WARN("failed to print ash top phase of execution", K(ret));
      } else if (OB_FAIL(print_ash_top_sql_with_event(ash_report_params, num_events, buff))) {
        LOG_WARN("failed to print ash top sql event info", K(ret));
      } else if (OB_FAIL(
                     print_ash_top_sql_with_blocking_event(ash_report_params, num_events, buff))) {
        LOG_WARN("failed to print ash top sql with blocking event info", K(ret));
      } else if (OB_FAIL(print_ash_sql_text_list(ash_report_params, buff))) {
        LOG_WARN("failed to print ash sql text list", K(ret));
      } else if (OB_FAIL(print_ash_top_session_info(
                     ash_report_params, num_samples, num_events, dur_elapsed_time, buff))) {
        LOG_WARN("failed to print ash top session info", K(ret));
      } else if (OB_FAIL(print_ash_top_blocking_session_info(
                     ash_report_params, num_samples, num_events, dur_elapsed_time, buff))) {
        LOG_WARN("failed to print ash top blocking session info", K(ret));
      } else if (OB_FAIL(print_ash_top_latches_info(ash_report_params, num_samples, buff))) {
        LOG_WARN("failed to print ash sql top latches info", K(ret));
      } else if (OB_FAIL(print_ash_node_load(ash_report_params, buff))) {
        LOG_WARN("failed to print ash node load", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObTextStringResult text_res(ObTextType, true, &ctx.exec_ctx_->get_allocator());
      if (FALSE_IT(
              result.set_collation_type(ctx.exec_ctx_->get_my_session()->get_nls_collation()))) {
      } else if (OB_FAIL(text_res.init(buff.length()))) {
        LOG_WARN("Failed to init text res", K(ret), K(buff.length()));
      } else if (OB_FAIL(text_res.append(buff.string()))) {
        LOG_WARN("Failed to append str to text res", K(ret), K(text_res), K(buff));
      } else {
        ObString lob_str;
        text_res.get_result_buffer(lob_str);
        OX(result.set_lob_value(ObTextType, lob_str.ptr(), lob_str.length()));
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

int ObDbmsWorkloadRepository::usec_to_string(
    const int64_t usec, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTime time;
  if (OB_FAIL(ObTimeConverter::usec_to_ob_time(usec, time))) {
    LOG_WARN("failed to usec to ob time", K(ret), K(usec));
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(
                 time, DT_TYPE_DATETIME, 0 /*scale*/, buf, buf_len, pos, true /*with_delim*/))) {
    LOG_WARN("fail to change time to string", K(ret), K(time), K(pos));
  }
  return ret;
}
const char *NULL_CHAR = "";
const int NULL_CHAR_LENGTH = 0;
const char *FILTER_EVENT_STR =
    "1";
const char *ASH_VIEW_SQL =
    "SELECT * FROM ( SELECT a.sample_id, a.sample_time,  a.svr_ip, "
    " a.svr_port,  a.con_id,  a.user_id,  a.session_id,  a.session_type, a.session_state,  "
    "a.sql_id,  a.plan_id,"
    " a.trace_id, IF(a.event='' OR a.event is NULL, 'CPU + Wait for CPU', a.event) as event,  "
    "nvl(a.event_no, 1) "
    " as event_no, a.p1, a.p1text,  a.p2, a.p2text,  a.p3, a.p3text,  "
    " IF(a.wait_class = '' OR a.wait_class is NULL, 'CPU', a.wait_class) as wait_class, "
    " nvl(a.wait_class_id, 9999) as wait_class_id,  a.time_waited,  a.sql_plan_line_id,  "
    "a.in_parse, "
    " a.in_pl_parse,  a.in_plan_cache,  a.in_sql_optimize,  a.in_sql_execution,  "
    "a.in_px_execution,  a.in_sequence_load, a.in_committing,  a.in_storage_read, "
    "a.in_storage_write, a.in_remote_das_execution, a.module, a.action, a.client_id  FROM GV$ACTIVE_SESSION_HISTORY a "
    " WHERE 1=1 and a.sample_time between '%.*s' and '%.*s' "
    ") unified_ash WHERE sample_time between '%.*s'  and '%.*s' "
    " AND ('%.*s' = ''  OR sql_id like '%.*s') "
    " AND ('%.*s' = '' OR trace_id like '%.*s') "
    " AND ('%.*s' = ''  OR wait_class like '%.*s') "
    " AND ('%.*s' = '' OR module like '%.*s') "
    " AND ('%.*s' = ''  OR action like '%.*s') "
    " AND ('%.*s' = ''  OR client_id like '%.*s')";

int ObDbmsWorkloadRepository::append_fmt_ash_view_sql(
    const AshReportParams &ash_report_params, ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  const int64_t time_buf_len = 128;
  int64_t time_buf_pos = 0;
  char ash_begin_time_buf[time_buf_len];
  char ash_end_time_buf[time_buf_len];
  if (OB_FAIL(usec_to_string(
          ash_report_params.ash_begin_time, ash_begin_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (FALSE_IT(time_buf_pos = 0)) {
  } else if (OB_FAIL(usec_to_string(
                 ash_report_params.ash_end_time, ash_end_time_buf, time_buf_len, time_buf_pos))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
  } else if (OB_FAIL(sql_string.append_fmt(ASH_VIEW_SQL, static_cast<int>(time_buf_pos),
                 ash_begin_time_buf, static_cast<int>(time_buf_pos), ash_end_time_buf,
                 static_cast<int>(time_buf_pos), ash_begin_time_buf, static_cast<int>(time_buf_pos),
                 ash_end_time_buf, ash_report_params.sql_id.length(),
                 ash_report_params.sql_id.ptr(), ash_report_params.sql_id.length(),
                 ash_report_params.sql_id.ptr(), ash_report_params.trace_id.length(),
                 ash_report_params.trace_id.ptr(), ash_report_params.trace_id.length(),
                 ash_report_params.trace_id.ptr(), ash_report_params.wait_class.length(),
                 ash_report_params.wait_class.ptr(), ash_report_params.wait_class.length(),
                 ash_report_params.wait_class.ptr(), NULL_CHAR_LENGTH, NULL_CHAR, NULL_CHAR_LENGTH,
                 NULL_CHAR, NULL_CHAR_LENGTH, NULL_CHAR, NULL_CHAR_LENGTH, NULL_CHAR,
                 NULL_CHAR_LENGTH, NULL_CHAR, NULL_CHAR_LENGTH, NULL_CHAR))) {
    LOG_WARN("failed to assign query string", K(ret));
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
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
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
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            EXTRACT_DATETIME_FIELD_MYSQL_SKIP_RET(
                *result, "ASH_BEGIN_TIME", ash_begin_time, int64_t);
            EXTRACT_DATETIME_FIELD_MYSQL_SKIP_RET(*result, "ASH_END_TIME", ash_end_time, int64_t);
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
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append("SELECT COUNT(1) AS NUM_SAMPLES, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(")AS SIGNED INTEGER) AS NUM_EVENTS FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "NUM_SAMPLES", num_samples, int64_t);
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "NUM_EVENTS", num_events, int64_t);
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
  char l_btime_buf[time_buf_len];
  char l_etime_buf[time_buf_len];
  char ash_begin_time_buf[time_buf_len];
  char ash_end_time_buf[time_buf_len];
  double avg_active_sess = static_cast<double>(num_samples) / dur_elapsed_time;
  avg_active_sess = round(avg_active_sess * 100) / 100;  // round to two decimal places
  HEAP_VAR(ObSqlString, temp_string)
  {
    if (OB_FAIL(usec_to_string(l_btime, l_btime_buf, time_buf_len, l_btime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(l_etime, l_etime_buf, time_buf_len, l_etime_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (ash_report_params.ash_begin_time <= 0 && ash_report_params.ash_end_time <= 0) {
      ash_begin_time_buf[0] = '\0';
      ash_begin_time_buf_pos = 0;
      ash_end_time_buf[0] = '\0';
      ash_end_time_buf_pos = 0;
    } else if (OB_FAIL(usec_to_string(ash_report_params.ash_begin_time, ash_begin_time_buf,
                   time_buf_len, ash_begin_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    } else if (OB_FAIL(usec_to_string(ash_report_params.ash_end_time, ash_end_time_buf,
                   time_buf_len, ash_end_time_buf_pos))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(temp_string.append("----\n"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("           Sample Begin: %.*s \n",
                   static_cast<int>(l_btime_buf_pos), l_btime_buf))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("             Sample End: %.*s \n",
                   static_cast<int>(l_etime_buf_pos), l_etime_buf))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append("             ----------\n"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("    Analysis Begin Time: %.*s \n",
                   static_cast<int>(ash_begin_time_buf_pos), ash_begin_time_buf))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("      Analysis End Time: %.*s \n",
                   static_cast<int>(ash_end_time_buf_pos), ash_end_time_buf))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(
                   temp_string.append_fmt("           Elapsed Time: %ld \n", dur_elapsed_time))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("          Num of Sample: %ld \n", num_samples))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append_fmt("          Num of Events: %ld \n", num_events))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(
                   temp_string.append_fmt("Average Active Sessions: %.2f \n", avg_active_sess))) {
      LOG_WARN("failed to assign query string", K(ret));
    } else if (OB_FAIL(temp_string.append("----\n"))) {
      LOG_WARN("append sql failed", K(ret));
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
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_user_event_info(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top User Events:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    const char *table_top[] = {"-", "-", "-"};
    const int64_t column_widths[] = {40, 20, 9};
    const char *column_headers[] = {"Event", "WAIT_CLASS", "% Event"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     3 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append("SELECT /*+ MONITOR */ EVENT,  WAIT_CLASS, COUNT(1)"
                                           " EVENT_CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event GROUP BY EVENT, WAIT_CLASS ORDER BY EVENT_CNT DESC"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            int64_t event_cnt = 0;
            char event[64] = "";
            char wait_class[64] = "";
            char event_radio_char[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "WAIT_CLASS", wait_class, 64, tmp_real_str_len);
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(10000 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {event, wait_class, event_radio_char};
              if (OB_FAIL(format_row(
                      3 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_events_and_value(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top Events P1/P2/P3 Value:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    const char *table_top[] = {"-", "-", "-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {40, 10, 12, 50, 20, 20, 20};
    const char *column_headers[] = {"Event", "% Event", "% Activity", "Max P1/P2/P3", "Parameter 1",
        "Parameter 2", "Parameter 3"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     7 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append("SELECT * FROM (SELECT EVENT, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ")AS SIGNED INTEGER) EVENT_CNT, CAST(COUNT(1) AS SIGNED INTEGER)"
                     " SAMPLE_CNT, MAX(P1) P1, MAX(P2) P2, "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "MAX(P3) P3, MAX(P1TEXT) P1TEXT, MAX(P2TEXT) P2TEXT, MAX(P3TEXT)"
                     " P3TEXT FROM   ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event  GROUP BY EVENT, WAIT_CLASS ORDER BY 2 DESC) LIMIT 10"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);

            int64_t event_cnt = 0;
            char event_radio_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(10000 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            int64_t sample_cnt = 0;
            char sample_radio_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SAMPLE_CNT", sample_cnt, int64_t);
            double sample_radio = static_cast<double>(sample_cnt) / num_samples;
            sample_radio = round(1000 * 100 * sample_radio) / 1000;
            sprintf(sample_radio_char, "%.3f%%", sample_radio);

            int64_t p1 = 0;
            int64_t p2 = 0;
            int64_t p3 = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "P1", p1, int64_t);
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "P2", p2, int64_t);
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "P3", p3, int64_t);
            char p1p2p3_char[OB_MAX_WAIT_EVENT_PARAM_LENGTH * 3] = "";
            sprintf(p1p2p3_char, "%ld, %ld, %ld", p1, p2, p3);

            char p1_text[OB_MAX_WAIT_EVENT_PARAM_LENGTH] = " ";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "P1TEXT", p1_text, OB_MAX_WAIT_EVENT_PARAM_LENGTH, tmp_real_str_len);
            char p2_text[OB_MAX_WAIT_EVENT_PARAM_LENGTH] = " ";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "P2TEXT", p2_text, OB_MAX_WAIT_EVENT_PARAM_LENGTH, tmp_real_str_len);
            char p3_text[OB_MAX_WAIT_EVENT_PARAM_LENGTH] = " ";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "P3TEXT", p3_text, OB_MAX_WAIT_EVENT_PARAM_LENGTH, tmp_real_str_len);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {event, event_radio_char, sample_radio_char,
                  p1p2p3_char, p1_text, p2_text, p3_text};
              if (OB_FAIL(format_row(
                      7 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_exec_phase(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t dur_elapsed_time, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top Phase of Execution:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    const char *table_top[] = {"-", "-", "-", "-"};
    const int64_t column_widths[] = {40, 12, 14, 40};
    const char *column_headers[] = {
        "Phase of Execution", "% Activity", "Sample Count", "Avg Active Sessions"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(4 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     4 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(4 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append("SELECT "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("CAST(SUM(CASE WHEN  IN_PARSE = 'N' THEN 0 ELSE 1 "
                                           "END)AS SIGNED INTEGER) IN_PARSE, "
                                           "CAST(SUM(CASE WHEN  IN_PL_PARSE = 'N' THEN 0 ELSE 1 "
                                           "END)AS SIGNED INTEGER) IN_PL_PARSE, "
                                           "CAST(SUM(CASE WHEN  IN_PLAN_CACHE = 'N' THEN 0 ELSE 1 "
                                           "END)AS SIGNED INTEGER) IN_PLAN_CACHE, "
                                           "CAST(SUM(CASE WHEN  IN_SQL_OPTIMIZE = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_SQL_OPTIMIZE, "
                                           "CAST(SUM(CASE WHEN  IN_SQL_EXECUTION = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_SQL_EXECUTION, "
                                           "CAST(SUM(CASE WHEN  IN_PX_EXECUTION = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_PX_EXECUTION, "
                                           "CAST(SUM(CASE WHEN  IN_SEQUENCE_LOAD = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_SEQUENCE_LOAD, "
                                           "CAST(SUM(CASE WHEN  IN_COMMITTING = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_COMMITTING, "
                                           "CAST(SUM(CASE WHEN  IN_STORAGE_READ = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_STORAGE_READ, "
                                           "CAST(SUM(CASE WHEN  IN_STORAGE_WRITE = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_STORAGE_WRITE, "
                                           "CAST(SUM(CASE WHEN  IN_REMOTE_DAS_EXECUTION = 'N' THEN 0 ELSE "
                                           "1 END)AS SIGNED INTEGER) IN_REMOTE_DAS_EXECUTION "
                                           " FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            ObArray<std::pair<const char *, int64_t>> phase_array;

            int64_t in_parse_cnt = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_PARSE", in_parse_cnt, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(
                           std::pair<const char *, int64_t>("IN_PARSE", in_parse_cnt)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_pl_parse_cnt = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_PL_PARSE", in_pl_parse_cnt, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(
                           std::pair<const char *, int64_t>("IN_PL_PARSE", in_pl_parse_cnt)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_plan_cache = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_PLAN_CACHE", in_plan_cache, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(
                           std::pair<const char *, int64_t>("IN_PLAN_CACHE", in_plan_cache)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_sql_optimize = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_SQL_OPTIMIZE", in_sql_optimize, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(
                           std::pair<const char *, int64_t>("IN_SQL_OPTIMIZE", in_sql_optimize)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_sql_execution = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(
                *result, "IN_SQL_EXECUTION", in_sql_execution, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char *, int64_t>(
                           "IN_SQL_EXECUTION", in_sql_execution)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_px_execution = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_PX_EXECUTION", in_px_execution, int64_t);
            if (OB_FAIL(ret)) {

            } else if (OB_FAIL(phase_array.push_back(
                           std::pair<const char *, int64_t>("IN_PX_EXECUTION", in_px_execution)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_sequence_load = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(
                *result, "IN_SEQUENCE_LOAD", in_sequence_load, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char *, int64_t>(
                           "IN_SEQUENCE_LOAD", in_sequence_load)))) {
              LOG_WARN("failed to push pair into phase array", K(ret));
            }

            int64_t in_committing = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_COMMITTING", in_committing, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char*, int64_t>("IN_COMMITTING", in_committing)))) {
              LOG_WARN("failed to push pair into phase array",K(ret));
            }

            int64_t in_storage_read = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_STORAGE_READ", in_storage_read, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char*, int64_t>("IN_STORAGE_READ", in_storage_read)))) {
              LOG_WARN("failed to push pair into phase array",K(ret));
            }

            int64_t in_storage_write = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_STORAGE_WRITE", in_storage_write, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char*, int64_t>("IN_STORAGE_WRITE", in_storage_write)))) {
              LOG_WARN("failed to push pair into phase array",K(ret));
            }

            int64_t in_das_remote_exec = 0;
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IN_REMOTE_DAS_EXECUTION", in_das_remote_exec, int64_t);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(phase_array.push_back(std::pair<const char*, int64_t>("IN_REMOTE_DAS_EXECUTION", in_das_remote_exec)))) {
              LOG_WARN("failed to push pair into phase array",K(ret));
            }

            lib::ob_sort(phase_array.begin(), phase_array.end(), phase_cmp_func);
            for (int64_t i = 0; OB_SUCC(ret) && i < phase_array.count(); i++) {
              const char *phase_name = phase_array.at(i).first;
              int64_t phase_cnt = phase_array.at(i).second;
              char sample_radio_char[64] = "";
              double sample_radio = static_cast<double>(phase_cnt) / num_samples;
              sample_radio = round(1000 * 100 * sample_radio) / 1000;
              sprintf(sample_radio_char, "%.3f%%", sample_radio);

              char phase_cnt_char[64] = "";
              sprintf(phase_cnt_char, "%ld", phase_cnt);

              char phase_avg_time_char[64] = "";
              double phase_avg_time = static_cast<double>(phase_cnt) / dur_elapsed_time;
              phase_avg_time = round(1000 * 100 * phase_avg_time) / 1000;
              sprintf(phase_avg_time_char, "%.3f%%", phase_avg_time);

              const char *column_content[] = {
                  phase_name, sample_radio_char, phase_cnt_char, phase_avg_time_char};
              if (OB_FAIL(format_row(
                      4 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(4 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_sql_with_event(
    const AshReportParams &ash_report_params, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top SQL with Top Events\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - All events included.\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - Empty ''SQL Text'' if it is PL/SQL query\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();

    const char *table_top[] = {"-", "-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {40, 12, 25, 40, 12, 60};
    const char *column_headers[] = {
        "SQL ID", "PLAN ID", "Sampled # of Executions", "Event", "% Event", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     6 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "SELECT SQL_ID, PLAN_ID, EVENT_CNT, EVENT, QUERY_SQL FROM (SELECT ash.*, "
                     "SUBSTR(TRIM(REPLACE(QUERY_SQL, CHAR(10), '''')), 1, 55) AS QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("FROM (SELECT SQL_ID, PLAN_ID, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(") AS SIGNED INTEGER) EVENT_CNT, EVENT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event GROUP BY SQL_ID, PLAN_ID, EVENT) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "LEFT JOIN GV$OB_PLAN_CACHE_PLAN_STAT pc ON ash.sql_id = pc.sql_id AND "
                     "ash.plan_id = pc.plan_id ORDER BY EVENT_CNT DESC) v1 LIMIT 100"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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

            int64_t plan_id = 0;
            char plan_id_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "PLAN_ID", plan_id, int64_t);
            sprintf(plan_id_char, "%ld", plan_id);

            int64_t event_cnt = 0;
            char event_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            sprintf(event_cnt_char, "%ld", event_cnt);

            tmp_real_str_len = 0;
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);

            char event_radio_char[64] = "";
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(100 * 100 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 63, tmp_real_str_len);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {
                  sql_id, plan_id_char, event_cnt_char, event, event_radio_char, query_sql};
              if (OB_FAIL(format_row(
                      6 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_sql_with_blocking_event(
    const AshReportParams &ash_report_params, const int64_t num_events, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top SQL with Top Blocking Events\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - Empty result if no event other than On CPU sampled\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - Empty ''SQL Text'' if it is PL/SQL query\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();

    const char *table_top[] = {"-", "-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {40, 12, 25, 40, 12, 60};
    const char *column_headers[] = {
        "SQL ID", "PLAN ID", "Sampled # of Executions", "Event", "% Event", "SQL Text"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     6 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(
                     sql_string.append("SELECT SQL_ID, PLAN_ID, EVENT_CNT, EVENT, QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append("FROM (SELECT ash.*, SUBSTR(TRIM(REPLACE(QUERY_SQL, "
                                           "CHAR(10), '''')), 1, 55) AS QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(" FROM (SELECT SQL_ID, PLAN_ID, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(") AS SIGNED INTEGER) EVENT_CNT, EVENT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event WHERE wait_class_id != 100 GROUP BY "
                                           "SQL_ID, PLAN_ID, EVENT) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "LEFT JOIN GV$OB_PLAN_CACHE_PLAN_STAT pc ON ash.sql_id = pc.sql_id AND "
                     "ash.plan_id = pc.plan_id ORDER BY EVENT_CNT DESC) LIMIT 100"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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

            int64_t plan_id = 0;
            char plan_id_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "PLAN_ID", plan_id, int64_t);
            sprintf(plan_id_char, "%ld", plan_id);

            int64_t event_cnt = 0;
            char event_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            sprintf(event_cnt_char, "%ld", event_cnt);

            tmp_real_str_len = 0;
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);

            char event_radio_char[64] = "";
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(100 * 100 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            tmp_real_str_len = 0;
            char query_sql[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                *result, "QUERY_SQL", query_sql, 63, tmp_real_str_len);
            if (OB_SUCC(ret)) {
              const char *column_content[] = {
                  sql_id, plan_id_char, event_cnt_char, event, event_radio_char, query_sql};
              if (OB_FAIL(format_row(
                      6 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(6 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_sql_text_list(
    const AshReportParams &ash_report_params, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Complete List of SQL Text:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_string.append(
              "SELECT SQL_ID, PLAN_ID, SUBSTR(QUERY_SQL, 1 ,4000) AS QUERY_SQL FROM "
              "(SELECT pc.SQL_ID SQL_ID, pc.PLAN_ID, pc.QUERY_SQL QUERY_SQL "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "FROM (SELECT SQL_ID, PLAN_ID, COUNT(1) EVENT_CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(") top_event GROUP BY SQL_ID, PLAN_ID, EVENT) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "LEFT JOIN GV$OB_PLAN_CACHE_PLAN_STAT pc ON ash.sql_id = pc.sql_id AND "
                     "ash.plan_id = pc.plan_id ORDER BY EVENT_CNT DESC) WHERE QUERY_SQL IS NOT "
                     "NULL "
                     "LIMIT 100"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(buff.append("  SQL ID: "))) {
              LOG_WARN("failed to push string into buff", K(ret));
            } else if (OB_FAIL(buff.append(sql_id))) {
              LOG_WARN("failed to push string into buff", K(ret));
            } else if (OB_FAIL(buff.append("\n"))) {
              LOG_WARN("failed to push string into buff", K(ret));
            }

            int64_t plan_id = 0;
            char plan_id_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "PLAN_ID", plan_id, int64_t);
            sprintf(plan_id_char, "%ld", plan_id);

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(buff.append(" PLAN ID: "))) {
              LOG_WARN("failed to push string into buff", K(ret));
            } else if (OB_FAIL(buff.append(plan_id_char))) {
              LOG_WARN("failed to push string into buff", K(ret));
            } else if (OB_FAIL(buff.append("\n"))) {
              LOG_WARN("failed to push string into buff", K(ret));
            }

            HEAP_VAR(char[4005], sql_text)
            {
              sql_text[0] = '\0';
              EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET_AND_TRUNCATION(
                  *result, "QUERY_SQL", sql_text, 4000, tmp_real_str_len);
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(buff.append("SQL Text: "))) {
                LOG_WARN("failed to push string into buff", K(ret));
              } else if (OB_FAIL(buff.append(sql_text))) {
                LOG_WARN("failed to push string into buff", K(ret));
              } else if (OB_FAIL(buff.append("\n"))) {
                LOG_WARN("failed to push string into buff", K(ret));
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
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_session_info(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, const int64_t dur_elapsed_time,
    ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top Sessions:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - ''# Samples Active'' shows the number of ASH samples in which "
                                 "the blocking session was found active.\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();

    const char *table_top[] = {"-", "-", "-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {20, 22, 40, 12, 30, 20, 30};
    const char *column_headers[] = {
        "Sid", "% Activity", "Event", "Event Count", "% Event", "User", "# Samples Active"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     7 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "SELECT  SESSION_ID, EVENT, EVENT_CNT, SAMPLE_CNT, user_name USER_NAME "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     " FROM (SELECT * FROM (SELECT SESSION_ID, USER_ID, EVENT, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ")AS SIGNED INTEGER) EVENT_CNT, COUNT(1) SAMPLE_CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ") top_event  GROUP BY SESSION_ID, USER_ID, EVENT "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "ORDER BY SAMPLE_CNT DESC) LIMIT 100) ash  "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     " LEFT JOIN oceanbase.__all_user u ON u.USER_ID = ash.USER_ID ORDER BY SAMPLE_CNT DESC"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            int64_t session_id = 0;
            char session_id_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SESSION_ID", session_id, int64_t);
            sprintf(session_id_char, "%ld", session_id);

            int64_t sample_cnt = 0;
            char sample_radio_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SAMPLE_CNT", sample_cnt, int64_t);
            double sample_radio = static_cast<double>(sample_cnt) / num_samples;
            sample_radio = round(100 * 100 * sample_radio) / 100;
            sprintf(sample_radio_char, "%.2f%%", sample_radio);

            int64_t tmp_real_str_len = 0;
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);

            int64_t event_cnt = 0;
            char event_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            sprintf(event_cnt_char, "%ld", event_cnt);
            char event_radio_char[64] = "";
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(100 * 100 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            tmp_real_str_len = 0;
            char user_name[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "USER_NAME", user_name, 64, tmp_real_str_len);

            char sample_active_char[64] = "";
            double sample_active_radio = static_cast<double>(event_cnt) / dur_elapsed_time;
            sample_active_radio = round(100 * 100 * sample_active_radio) / 100;
            sprintf(sample_active_char, "%ld/%ld[%.2f%%]", event_cnt, dur_elapsed_time,
                sample_active_radio);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {session_id_char, sample_radio_char, event,
                  event_cnt_char, event_radio_char, user_name, sample_active_char};
              if (OB_FAIL(format_row(
                      7 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_blocking_session_info(
    const AshReportParams &ash_report_params, const int64_t num_samples, const int64_t num_events,
    const int64_t dur_elapsed_time, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top Blocking Sessions:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - Blocking session activity percentages are calculated with "
                                 "respect to waits on latches and locks only. \n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append(" - ''# Samples Active'' shows the number of ASH samples in which "
                                 "the session was found waiting"
                                 " for that particular event. The percentage shown in this column "
                                 "is calculated with respect to wall time.\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();
    const char *table_top[] = {"-", "-", "-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {20, 22, 40, 12, 30, 20, 30};
    const char *column_headers[] = {"Blocking Sid", "% Activity", "Event Caused", "Event Count",
        "% Event", "User", "# Samples Active"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     7 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "SELECT  SESSION_ID, EVENT, EVENT_CNT, SAMPLE_CNT, user_name USER_NAME "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     " FROM (SELECT * FROM (SELECT SESSION_ID, USER_ID, EVENT, CAST(SUM("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(FILTER_EVENT_STR))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     ")AS SIGNED INTEGER) AS EVENT_CNT, COUNT(1) SAMPLE_CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" ) top_event  WHERE wait_class_id != 100 GROUP BY "
                                           "SESSION_ID, USER_ID, EVENT "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt(
                     "ORDER BY SAMPLE_CNT DESC) LIMIT 100) ash "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     " LEFT JOIN oceanbase.__all_user u ON u.USER_ID = ash.USER_ID ORDER BY SAMPLE_CNT DESC"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            int64_t session_id = 0;
            char session_id_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SESSION_ID", session_id, int64_t);
            sprintf(session_id_char, "%ld", session_id);

            int64_t sample_cnt = 0;
            char sample_radio_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SAMPLE_CNT", sample_cnt, int64_t);
            double sample_radio = static_cast<double>(sample_cnt) / num_samples;
            sample_radio = round(100 * 100 * sample_radio) / 100;
            sprintf(sample_radio_char, "%.2f%%", sample_radio);

            int64_t tmp_real_str_len = 0;
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);

            int64_t event_cnt = 0;
            char event_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "EVENT_CNT", event_cnt, int64_t);
            sprintf(event_cnt_char, "%ld", event_cnt);
            char event_radio_char[64] = "";
            double event_radio = static_cast<double>(event_cnt) / num_events;
            event_radio = round(100 * 100 * event_radio) / 100;
            sprintf(event_radio_char, "%.2f%%", event_radio);

            tmp_real_str_len = 0;
            char user_name[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
                *result, "USER_NAME", user_name, 64, tmp_real_str_len);

            char sample_active_char[64] = "";
            double sample_active_radio = static_cast<double>(event_cnt) / dur_elapsed_time;
            sample_active_radio = round(100 * 100 * sample_active_radio) / 100;
            sprintf(sample_active_char, "%ld/%ld[%.2f%%]", event_cnt, dur_elapsed_time,
                sample_active_radio);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {session_id_char, sample_radio_char, event,
                  event_cnt_char, event_radio_char, user_name, sample_active_char};
              if (OB_FAIL(format_row(
                      7 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(7 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsWorkloadRepository::print_ash_top_latches_info(
    const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Top latches:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();

    const char *table_top[] = {"-", "-", "-"};
    const int64_t column_widths[] = {40, 20, 20};
    const char *column_headers[] = {"Latch", "Sampled Count", "% Activity"};
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string))
    {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(
                     3 /*column_size*/, column_headers, column_widths, " ", "|", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append(
                     "SELECT * FROM (SELECT EVENT, COUNT(1) SAMPLE_CNT FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" ) top_event  WHERE wait_class_id = 104 AND"
                                           " SUBSTR(event, 1, 6) = 'latch:' "))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_string.append_fmt("GROUP BY EVENT "
                                               "ORDER BY SAMPLE_CNT DESC) LIMIT 100"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id),
            K(sql_string));
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
            char event[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "EVENT", event, 64, tmp_real_str_len);
            int64_t sample_cnt = 0;
            char sample_cnt_char[64] = "";
            char sample_radio_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SAMPLE_CNT", sample_cnt, int64_t);
            sprintf(sample_cnt_char, "%ld", sample_cnt);

            double sample_radio = static_cast<double>(sample_cnt) / num_samples;
            sample_radio = round(100 * 100 * sample_radio) / 100;
            sprintf(sample_radio_char, "%.2f%%", sample_radio);

            if (OB_SUCC(ret)) {
              const char *column_content[] = {event, sample_cnt_char, sample_radio_char};
              if (OB_FAIL(format_row(
                      3 /*column_size*/, column_content, column_widths, " ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        }  // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(3 /*column_size*/, table_top, column_widths, "-", "+", buff))) {
            LOG_WARN("failed to format row", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int ObDbmsWorkloadRepository::print_ash_node_load(
  const AshReportParams &ash_report_params,
  ObStringBuffer &buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", K(ret));
  } else if (OB_FAIL(buff.append("\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else if (OB_FAIL(buff.append("## Node Load:\n"))) {
    LOG_WARN("failed to push string into buff", K(ret));
  } else {
    common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    const uint64_t tenant_id = MTL_ID();

    const char* table_top[] = {"-", "-", "-", "-", "-"};
    const int64_t column_widths[] = {40, 20, 20, 20, 30};
    const char* column_headers[] = {"SVR IP", "SVR PORT", "Sampled Count", "Idle Wait Count", "Load"};

    double duration = static_cast<double>(ash_report_params.ash_end_time - ash_report_params.ash_begin_time) / 1000000;
    HEAP_VARS_2((ObISQLClient::ReadResult, res), (ObSqlString, sql_string)) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(format_row(5/*column_size*/, table_top,column_widths,"-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(5/*column_size*/, column_headers,column_widths," ", "|",buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(format_row(5/*column_size*/, table_top,column_widths,"-", "+", buff))) {
        LOG_WARN("failed to format row", K(ret));
      } else if (OB_FAIL(sql_string.append("SELECT SVR_IP, SVR_PORT, count(1) AS SAMPLE_CNT , "
                                           " CAST(SUM(CASE WHEN wait_class_id = 106 "
                                           " THEN 1 ELSE 0 END) AS SIGNED INTEGER) AS IDLE_WAIT_CNT"
                                           " FROM ("))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(append_fmt_ash_view_sql(ash_report_params, sql_string))) {
        LOG_WARN("failed to append fmt ash view sql", K(ret));
      } else if (OB_FAIL(sql_string.append(" ) top_event  GROUP BY SVR_IP, SVR_PORT"))) {
        LOG_WARN("append sql failed", K(ret));
      } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql_string.ptr()))) {
        LOG_WARN("failed to fetch ash begin time and ash end time", KR(ret), K(tenant_id), K(sql_string));
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
            char svr_ip[64] = "";
            EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(*result, "SVR_IP", svr_ip, 64, tmp_real_str_len);

            int64_t svr_port = 0;
            char svr_port_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SVR_PORT", svr_port, int64_t);
            sprintf(svr_port_char, "%ld", svr_port);

            int64_t sample_cnt = 0;
            char sample_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "SAMPLE_CNT", sample_cnt, int64_t);
            sprintf(sample_cnt_char, "%ld", sample_cnt);

            int64_t idle_cnt = 0;
            char idle_cnt_char[64] = "";
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "IDLE_WAIT_CNT", idle_cnt, int64_t);
            sprintf(idle_cnt_char, "%ld", idle_cnt);

            double load = static_cast<double>(sample_cnt-idle_cnt) / duration;
            char load_char[64] = "";
            sprintf(load_char, "%.2f", load);

            if (OB_SUCC(ret)) {
              const char* column_content[] = {svr_ip, svr_port_char, sample_cnt_char, idle_cnt_char, load_char};
              if (OB_FAIL(format_row(5/*column_size*/, column_content, column_widths," ", "|", buff))) {
                LOG_WARN("failed to format row", K(ret));
              }
            }
          }
        } // end while

        if (OB_SUCC(ret)) {
          if (OB_FAIL(format_row(5/*column_size*/, table_top,column_widths,"-", "+", buff))) {
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