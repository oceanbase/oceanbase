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

#include "share/wr/ob_wr_snapshot_rpc_processor.h"
#include "share/wr/ob_wr_collector.h"
#include "share/wr/ob_wr_task.h"
#include "share/wr/ob_wr_service.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "observer/ob_srv_network_frame.h"
namespace oceanbase
{
namespace share
{

DEF_TO_STRING(ObWrSnapshotArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(task_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObWrSnapshotArg, task_type_);

DEF_TO_STRING(ObWrCreateSnapshotArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("WR_snapshot_arg");
  J_COLON();
  pos += ObWrSnapshotArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(tenant_id), K_(snap_id), K_(snapshot_begin_time), K_(snapshot_end_time), K_(timeout_ts));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObWrCreateSnapshotArg, ObWrSnapshotArg), tenant_id_, snap_id_,
    snapshot_begin_time_, snapshot_end_time_, timeout_ts_);

DEF_TO_STRING(ObWrPurgeSnapshotArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("WR_snapshot_arg");
  J_COLON();
  pos += ObWrSnapshotArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(tenant_id), K_(to_delete_snap_ids), K_(timeout_ts));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(
    (ObWrPurgeSnapshotArg, ObWrSnapshotArg), tenant_id_, to_delete_snap_ids_, timeout_ts_);

DEF_TO_STRING(ObWrUserSubmitSnapArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("WR_user_submit_snapshot_arg");
  J_COLON();
  pos += ObWrSnapshotArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(timeout_ts));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER((ObWrUserSubmitSnapArg, ObWrSnapshotArg), timeout_ts_);

OB_SERIALIZE_MEMBER(ObWrUserSubmitSnapResp, snap_id_);

DEF_TO_STRING(ObWrUserModifySettingsArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("WR_user_modify_settings_arg");
  J_COLON();
  pos += ObWrSnapshotArg::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(tenant_id), K_(retention), K_(interval), K_(topnsql));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(
    (ObWrUserModifySettingsArg, ObWrSnapshotArg), tenant_id_, retention_, interval_, topnsql_);

template <obrpc::ObRpcPacketCode pcode>
int ObWrBaseSnapshotTaskP<pcode>::init()
{
  return OB_SUCCESS;
}
template <obrpc::ObRpcPacketCode pcode>
int ObWrBaseSnapshotTaskP<pcode>::before_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(RpcProcessor::before_process())) {
    LOG_WARN("do rpc processor before_process failed", K(ret));
  }
  // TODO:set memory ctx if needed.
  return ret;
}

#define SLOW_WR_SNAPSHOT_WATERMARK 60 * 1000 * 1000  // 1min
template <obrpc::ObRpcPacketCode pcode>
int ObWrBaseSnapshotTaskP<pcode>::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t elapsed_time =
      common::ObTimeUtility::current_time() - RpcProcessor::get_receive_timestamp();
  if (OB_FAIL(RpcProcessor::after_process(error_code))) {
    LOG_WARN("do after_process failed", K(ret));
  } else if (elapsed_time >= SLOW_WR_SNAPSHOT_WATERMARK) {
    // TODO: define wr slow snapshot config.
    // slow wr snapshot task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow wr snapshot rpc process]");
  }
  return OB_SUCCESS;
}

template <obrpc::ObRpcPacketCode pcode>
void ObWrBaseSnapshotTaskP<pcode>::cleanup()
{
  RpcProcessor::cleanup();
}

int ObWrAsyncSnapshotTaskP::process()
{
  int ret = OB_SUCCESS;
  WR_STAT_GUARD(WR_SNAPSHOT);
  ObWrSnapshotArg &arg = ObWrAsyncSnapshotTaskP::arg_;
  LOG_DEBUG("wr snapshot task", K(MTL_ID()), K(arg));
  // gather inner table data
  if (WrTaskType::TAKE_SNAPSHOT == arg.get_task_type()) {
    ObWrSnapshotStatus status;
    ObWrCreateSnapshotArg &snapshot_arg = static_cast<ObWrCreateSnapshotArg &>(arg);
    int64_t origin_worker_timeout = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(snapshot_arg.get_timeout_ts());
    ObWrCollector collector(snapshot_arg.get_snap_id(), snapshot_arg.get_snapshot_begin_time(),
        snapshot_arg.get_snapshot_end_time(), snapshot_arg.get_timeout_ts());
    if (OB_UNLIKELY(MTL_ID() != snapshot_arg.get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wr snapshot task tenant_id mismatch!", K(MTL_ID()), K(snapshot_arg));
    } else if (OB_FAIL(collector.collect())) {
      LOG_WARN("failed to collect wr data", K(ret), K(snapshot_arg), K(MTL_ID()));
    }
    // update snapshot info
    if (OB_FAIL(ret)) {
      status = ObWrSnapshotStatus::FAILED;
    } else {
      status = ObWrSnapshotStatus::SUCCESS;
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(WorkloadRepositoryTask::modify_tenant_snapshot_status_and_startup_time(
            snapshot_arg.get_snap_id(), snapshot_arg.get_tenant_id(), GCONF.cluster_id,
            GCONF.self_addr_, GCTX.start_service_time_, status))) {
      LOG_WARN("failed to modify snapshot info", KR(tmp_ret), K(snapshot_arg), K(status));
    } else if (is_sys_tenant(snapshot_arg.get_tenant_id())) {
      if (OB_TMP_FAIL(WorkloadRepositoryTask::update_snap_info_in_wr_control(snapshot_arg.get_tenant_id(),
              snapshot_arg.get_snap_id(), snapshot_arg.get_snapshot_end_time()))) {
        LOG_WARN("failed to update wr control info", KR(tmp_ret), K(snapshot_arg));
      }
    }
    ret = COVER_SUCC(tmp_ret);
    THIS_WORKER.set_timeout_ts(origin_worker_timeout);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong op type", KR(ret), K(arg.get_task_type()));
  }
  return ret;
}
int ObWrAsyncPurgeSnapshotTaskP::process()
{
  int ret = OB_SUCCESS;
  WR_STAT_GUARD(WR_PURGE);
  ObWrSnapshotArg &arg = ObWrAsyncPurgeSnapshotTaskP::arg_;
  LOG_DEBUG("wr snapshot task", K(MTL_ID()), K(arg));
  // gather inner table data
  if (WrTaskType::PURGE == arg.get_task_type()) {
    ObWrSnapshotStatus status;
    ObWrPurgeSnapshotArg &purge_arg = static_cast<ObWrPurgeSnapshotArg &>(arg);
    int64_t origin_worker_timeout = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(purge_arg.get_timeout_ts());

    ObWrDeleter deleter(purge_arg);
    if (OB_UNLIKELY(MTL_ID() != purge_arg.get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wr snapshot task tenant_id mismatch!", K(MTL_ID()), K(purge_arg));
    } else if (OB_FAIL(deleter.do_delete())) {
      LOG_WARN("failed to delete wr data", K(ret), K(purge_arg), K(MTL_ID()));
    }
    THIS_WORKER.set_timeout_ts(origin_worker_timeout);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong op type", KR(ret), K(arg.get_task_type()));
  }
  return ret;
}

int ObWrSyncUserSubmitSnapshotTaskP::process()
{
  int ret = OB_SUCCESS;
  WR_STAT_GUARD(WR_SNAPSHOT);
  ObWrSnapshotArg &arg = ObWrSyncUserSubmitSnapshotTaskP::arg_;
  ObWrUserSubmitSnapResp &resp = ObWrSyncUserSubmitSnapshotTaskP::result_;
  LOG_DEBUG("user submit a wr take snapshot task", K(MTL_ID()), K(arg));
  if (OB_ISNULL(GCTX.wr_service_) || OB_ISNULL(GCTX.net_frame_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "wr service  or net frame is nullptr", K(ret), K(GCTX.net_frame_), K(GCTX.wr_service_));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in upgrade, can not do wr snapshot", KR(ret));
  } else if (GCTX.wr_service_->is_running_task()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("now, timer task is running, need to retry", K(ret));
  } else if (WrTaskType::USER_SNAPSHOT == arg.get_task_type()) {
    ObWrUserSubmitSnapArg &user_submit_arg = static_cast<ObWrUserSubmitSnapArg &>(arg);
    int64_t origin_worker_timeout = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(user_submit_arg.get_timeout_ts());
    bool is_all_finished = false;
    int64_t snap_id = 0;
    obrpc::ObWrRpcProxy wr_proxy;
    int64_t timeout_ts = user_submit_arg.get_timeout_ts();
    int64_t next_wr_task_ts = common::ObTimeUtility::current_time() +
                              GCTX.wr_service_->get_snapshot_interval() * 60 * 1000L * 1000L;
    if (OB_FAIL(WorkloadRepositoryTask::check_all_tenant_last_snapshot_task_finished(
            is_all_finished))) {
      LOG_WARN("failed to check all tenants` last snapshot status", K(ret), K(is_all_finished));
    } else if (!is_all_finished) {
      ret = OB_NEED_RETRY;
      LOG_WARN("now, timer task is running, need to retry", K(ret));
    } else if (OB_FAIL(GCTX.wr_service_->cancel_current_task())) {
      LOG_WARN("failed to cancel current task", K(ret));
    } else if (OB_FAIL(wr_proxy.init(GCTX.net_frame_->get_req_transport()))) {
      int tmp_ret = schedule_next_wr_task(next_wr_task_ts);
      LOG_WARN("failed to init wr proxy", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(GCTX.wr_service_->get_wr_timer_task().do_snapshot(
                   true /*is_user_submit*/, wr_proxy, snap_id))) {
      int tmp_ret = schedule_next_wr_task(next_wr_task_ts);
      LOG_WARN("failed to take snapshot", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(schedule_next_wr_task(next_wr_task_ts))) {
      LOG_WARN("failed to schedule next wr task", KR(ret), K(next_wr_task_ts));
    } else {
      resp.set_snap_id(snap_id);
    }
    THIS_WORKER.set_timeout_ts(origin_worker_timeout);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong op type", KR(ret), K(arg.get_task_type()));
  }
  return ret;
}

int ObWrSyncUserSubmitSnapshotTaskP::schedule_next_wr_task(int64_t next_wr_task_ts)
{
  int ret = OB_SUCCESS;
  // dispatch next wr snapshot task(not repeat).
  int64_t interval = next_wr_task_ts - common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(interval < 0)) {
    LOG_WARN("already timeout wr snapshot interval", K(next_wr_task_ts), K(interval),
        K(GCTX.wr_service_->get_snapshot_interval()));
    interval = 0;
  }
  if (OB_FAIL(GCTX.wr_service_->schedule_new_task(interval))) {
    LOG_WARN("failed to schedule  new wr snapshot task", K(ret));
  }
  return ret;
}

const char *RETENTION_NUM_COLUMN_NAME = "retention_num";
const char *RETENTION_COLUMN_NAME = "retention";
const char *INTERVAL_NUM_COLUMN_NAME = "snapint_num";
const char *INTERVAL_COLUMN_NAME = "snap_interval";

int ObWrSyncUserModifySettingsTaskP::process()
{
  int ret = OB_SUCCESS;
  int64_t tmp_interval = 0;
  ObWrUserModifySettingsArg &arg = ObWrSyncUserModifySettingsTaskP::arg_;
  LOG_DEBUG("user submit modify snapshot settings task", K(MTL_ID()), K(arg));
  if (OB_ISNULL(GCTX.wr_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wr service is nullptr", K(ret),K(GCTX.wr_service_));
  } else if (OB_FAIL(WorkloadRepositoryTask::fetch_interval_num_from_wr_control(tmp_interval))) {
    LOG_WARN("failed to fetch interval num from wr control", K(ret));
  } else if (WrTaskType::USER_MODIFY_SETTINGS == arg.get_task_type()) {
    if (OB_SUCC(ret)) {
      if (-1 == arg.get_retention()) {
        // keep the value of retention , do nothing
      } else if (OB_FAIL(WorkloadRepositoryTask::update_wr_control(RETENTION_NUM_COLUMN_NAME,
                     arg.get_retention(), RETENTION_COLUMN_NAME, arg.get_tenant_id()))) {
        LOG_WARN("failed to update wr control retention", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (-1 == arg.get_interval()) {
        // keep the value of interval , do nothing
      } else if (OB_FAIL(WorkloadRepositoryTask::update_wr_control(INTERVAL_NUM_COLUMN_NAME,
                     arg.get_interval(), INTERVAL_COLUMN_NAME, arg.get_tenant_id()))) {
        LOG_WARN("failed to update wr control interval", K(ret));
      } else if (OB_FAIL(GCTX.wr_service_->get_wr_timer_task().modify_snapshot_interval(
                     arg.get_interval()))) {
        LOG_WARN("failed to take snapshot", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error type of task", K(ret), K(GCTX.net_frame_), K(GCTX.wr_service_));
  }
  return ret;
}

}  // end of namespace share
}  // end of namespace oceanbase
