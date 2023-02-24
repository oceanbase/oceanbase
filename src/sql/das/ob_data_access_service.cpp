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

#define USING_LOG_PREFIX SQL_DAS
#include "observer/ob_srv_network_frame.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_extra_data.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_das_utils.h"
#include "sql/ob_phy_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/tx/ob_trans_service.h"
namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace common;
using namespace transaction;
namespace sql
{
ObDataAccessService &ObDataAccessService::get_instance()
{
  static ObDataAccessService instance;
  return instance;
}

int ObDataAccessService::mtl_init(ObDataAccessService *&das)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  auto req_transport = net_frame->get_req_transport();
  if (OB_FAIL(das->id_cache_.init(self, req_transport))) {
    LOG_ERROR("init das id service failed", K(ret));
  } else if (OB_FAIL(das->init(net_frame->get_req_transport(), self))) {
    LOG_ERROR("init data access service failed", K(ret));
  }
  return ret;
}

void ObDataAccessService::mtl_destroy(ObDataAccessService *&das)
{
  if (das != nullptr) {
    das->~ObDataAccessService();
    oceanbase::common::ob_delete(das);
    das = nullptr;
  }
}

int ObDataAccessService::init(rpc::frame::ObReqTransport *transport, const ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(das_rpc_proxy_.init(transport))) {
    LOG_WARN("init das rpc proxy failed", K(ret));
  } else if (OB_FAIL(task_result_mgr_.init())) {
    LOG_WARN("init das task result manager failed", KR(ret));
  } else {
    ctrl_addr_ = self_addr;
  }
  return ret;
}

int ObDataAccessService::execute_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(das_ref.is_execute_directly())) {
    NG_TRACE(do_local_das_task_begin);
    FLTSpanGuard(do_local_das_task);
    if (OB_FAIL(task_op.start_das_task())) {
      LOG_WARN("start das task failed", K(ret), K(task_op));
    }
    NG_TRACE(do_local_das_task_end);
  } else {
    ret = execute_dist_das_task(das_ref, task_op);
    task_op.errcode_ = ret;
  }
  OB_ASSERT(task_op.errcode_ == ret);
  if (OB_FAIL(ret) && GCONF._enable_partition_level_retry && task_op.can_part_retry()) {
    //only fast select can be retry with partition level
    int tmp_ret = retry_das_task(das_ref, task_op);
    if (OB_SUCCESS == tmp_ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to retry das task", K(tmp_ret));
    }
  }
  return ret;
}

int ObDataAccessService::get_das_task_id(int64_t &das_id)
{
  int ret = OB_SUCCESS;
  NG_TRACE(get_das_id_begin);
  FLTSpanGuard(get_das_id);
  const int MAX_RETRY_TIMES = 50;
  int64_t tmp_das_id = 0;
  bool force_renew = false;
  int64_t total_sleep_time = 0;
  int64_t cur_sleep_time = 1000; // 1ms
  int64_t max_sleep_time = ObDASIDCache::OB_DAS_ID_RPC_TIMEOUT_MIN * 2; // 200ms
  do {
    if (OB_SUCC(id_cache_.get_das_id(tmp_das_id, force_renew))) {
    } else if (OB_EAGAIN == ret) {
      if (total_sleep_time >= max_sleep_time) {
        // TODO chenxuan change error code
        ret = OB_GTI_NOT_READY;
        LOG_WARN("get das id not ready", K(ret), K(total_sleep_time), K(max_sleep_time));
      } else {
        force_renew = true;
        ob_usleep(cur_sleep_time);
        total_sleep_time += cur_sleep_time;
        cur_sleep_time = cur_sleep_time * 2;
      }
    } else {
      LOG_WARN("get das id failed", K(ret));
    }
  } while (OB_EAGAIN == ret);
  if (OB_SUCC(ret)) {
    das_id = tmp_das_id;
  }
  NG_TRACE(get_das_id_end);
  return ret;
}

OB_NOINLINE int ObDataAccessService::execute_dist_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = das_ref.get_exec_ctx();
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObDASTaskArg task_arg;
  if (OB_FAIL(task_arg.add_task_op(&task_op))) {
    LOG_WARN("failed to add das task op", K(ret), K(task_op));
  } else {
    task_arg.set_timeout_ts(session->get_query_timeout_ts());
    task_arg.set_ctrl_svr(ctrl_addr_);
    task_arg.get_runner_svr() = task_op.tablet_loc_->server_;
    if (task_arg.is_local_task()) {
      if (OB_FAIL(do_local_das_task(das_ref, task_arg))) {
        LOG_WARN("do local das task failed", K(ret), K(task_op));
      }
    } else if (OB_FAIL(do_remote_das_task(das_ref, task_arg))) {
      LOG_WARN("do remote das task failed", K(ret));
    }
  }
  return ret;
}

int ObDataAccessService::clear_task_exec_env(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  UNUSED(das_ref);
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_op.end_das_task())) {
    LOG_WARN("end das task failed", K(ret));
  }
  return ret;
}

int ObDataAccessService::refresh_partition_location(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = das_ref.get_exec_ctx();
  ObDASBaseRtDef *das_rtdef = task_op.get_rtdef();
  ObDASTableLoc *table_loc = das_rtdef->table_loc_;
  ObDASTabletLoc *tablet_loc = const_cast<ObDASTabletLoc*>(task_op.get_tablet_loc());
  if (OB_SUCC(DAS_CTX(exec_ctx).refresh_tablet_loc(*tablet_loc))) {
    task_op.set_ls_id(tablet_loc->ls_id_);
  }
  LOG_INFO("LOCATION: refresh tablet cache", K(ret), KPC(tablet_loc), KPC(tablet_loc));
  return ret;
}

int ObDataAccessService::retry_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = task_op.errcode_;
  while (!is_virtual_table(task_op.get_ref_table_id()) &&
         (is_master_changed_error(ret) || is_partition_change_error(ret) || OB_REPLICA_NOT_READABLE == ret)) {
    if (!can_fast_fail(task_op)) {
      task_op.in_part_retry_ = true;
      das_ref.get_exec_ctx().get_my_session()->set_session_in_retry(true, ret);
      if (OB_FAIL(clear_task_exec_env(das_ref, task_op))) {
        LOG_WARN("clear task execution environment", K(ret));
      } else if (OB_FAIL(das_ref.get_exec_ctx().check_status())) {
        LOG_WARN("query is timeout, terminate retry", K(ret));
      } else if (OB_FAIL(refresh_partition_location(das_ref, task_op))) {
        LOG_WARN("refresh partition location failed", K(ret));
      } else if (OB_FAIL(execute_dist_das_task(das_ref, task_op))) {
        LOG_WARN("execute dist das task failed", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}


bool ObDataAccessService::can_fast_fail(const ObIDASTaskOp &task_op) const
{
  bool bret = false;
  int ret = OB_SUCCESS;  // no need to pass ret outside.
  const common::ObTableID &table_id = IS_DAS_DML_OP(task_op)
      ? static_cast<const ObDASDMLBaseCtDef *>(task_op.get_ctdef())->table_id_
      : static_cast<const ObDASScanCtDef *>(task_op.get_ctdef())->ref_table_id_;
  int64_t schema_version = IS_DAS_DML_OP(task_op)
      ? static_cast<const ObDASDMLBaseCtDef *>(task_op.get_ctdef())->schema_version_
      : static_cast<const ObDASScanCtDef *>(task_op.get_ctdef())->schema_version_;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    LOG_ERROR("invalid schema service", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    // tenant could be deleted
    bret = true;
    LOG_WARN("get tenant schema guard fail", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    bret = true;
    LOG_WARN("table not exist, fast fail das task");
  } else if (table_schema->get_schema_version() != schema_version) {
    bret = true;
    LOG_WARN("schema version changed, fast fail das task", "current schema version",
             table_schema->get_schema_version(), "query schema version", schema_version);
  }
  return bret;
}

int ObDataAccessService::end_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_op.end_das_task())) {
    LOG_WARN("end das task failed", K(ret), KPC(task_op.get_tablet_loc()));
  }
  return ret;
}

int ObDataAccessService::rescan_das_task(ObDASRef &das_ref, ObDASScanOp &scan_op)
{
  int ret = OB_SUCCESS;
  NG_TRACE(rescan_das_task_begin);
  FLTSpanGuard(rescan_das_task);
  if (scan_op.is_local_task()) {
    if (OB_FAIL(scan_op.rescan())) {
      LOG_WARN("rescan das task failed", K(ret));
    }
  } else if (OB_FAIL(execute_dist_das_task(das_ref, scan_op))) {
    LOG_WARN("execute dist das task failed", K(ret));
  }
  OB_ASSERT(scan_op.errcode_ == ret);
  if (OB_FAIL(ret) && GCONF._enable_partition_level_retry && scan_op.can_part_retry()) {
    //only fast select can be retry with partition level
    int tmp_ret = retry_das_task(das_ref, scan_op);
    if (OB_SUCCESS == tmp_ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to retry das task", K(tmp_ret));
    }
  }
  NG_TRACE(rescan_das_task_end);
  return ret;
}

int ObDataAccessService::do_local_das_task(ObDASRef &das_ref, ObDASTaskArg &task_arg)
{
  UNUSED(das_ref);
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to do local das task", K(task_arg));
  ObIDASTaskOp *task_op = task_arg.get_task_op();
  NG_TRACE(do_local_das_task_begin);
  FLTSpanGuard(do_local_das_task);
  if (OB_FAIL(task_op->start_das_task())) {
    LOG_WARN("start local das task failed", K(ret));
  }
  NG_TRACE(do_local_das_task_end);
  return ret;
}

int ObDataAccessService::do_remote_das_task(ObDASRef &das_ref, ObDASTaskArg &task_arg)
{
  int ret = OB_SUCCESS;
  void *resp_buf = nullptr;
  NG_TRACE(do_remote_das_task_begin);
  FLTSpanGuard(do_remote_das_task);
  ObSQLSessionInfo *session = das_ref.get_exec_ctx().get_my_session();
  ObPhysicalPlanCtx *plan_ctx = das_ref.get_exec_ctx().get_physical_plan_ctx();
  int64_t timeout = plan_ctx->get_timeout_timestamp() - ObTimeUtility::current_time();
  uint64_t tenant_id = session->get_rpc_tenant_id();
  ObIDASTaskOp *task_op = task_arg.get_task_op();
  ObIDASTaskResult *op_result = nullptr;
  ObDASExtraData *extra_result = nullptr;
  ObDASRemoteInfo remote_info;
  remote_info.exec_ctx_ = &das_ref.get_exec_ctx();
  remote_info.frame_info_ = das_ref.get_expr_frame_info();
  remote_info.trans_desc_ = session->get_tx_desc();
  remote_info.snapshot_ = *task_op->get_snapshot();
  remote_info.need_tx_ = (remote_info.trans_desc_ != nullptr);
  task_arg.set_remote_info(&remote_info);
  ObDASRemoteInfo::get_remote_info() = &remote_info;

  LOG_DEBUG("begin to do remote das task", K(task_arg));
  SMART_VAR(ObDASTaskResp, task_resp) {
    if (NULL != (op_result = task_op->get_op_result())) {
      if (OB_FAIL(op_result->reuse())) {
        LOG_WARN("reuse task result failed", K(ret));
      }
    } else {
      if (OB_FAIL(das_ref.get_das_factory().create_das_task_result(task_op->get_type(), op_result))) {
        LOG_WARN("create das task result failed", K(ret));
      } else if (OB_FAIL(op_result->init(*task_op))) {
        LOG_WARN("init task result failed", K(ret));
      } else {
        task_op->set_op_result(op_result);
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(collect_das_task_info(task_arg, remote_info))) {
      LOG_WARN("collect das task info failed", K(ret));
    } else if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("das is timeout", K(ret), K(plan_ctx->get_timeout_timestamp()), K(timeout));
    } else if (OB_FAIL(task_resp.add_op_result(op_result))) {
      LOG_WARN("failed to add op result", K(ret));
    } else if (OB_FAIL(das_rpc_proxy_
                    .to(task_arg.get_runner_svr())
                    .by(tenant_id)
                    .timeout(timeout)
                    .remote_sync_access(task_arg, task_resp))) {
      LOG_WARN("rpc remote sync access failed", K(ret), K(task_arg));
      // RPC fail, add task's LSID to trans_result
      // indicate some transaction participant may touched
      session->get_trans_result().add_touched_ls(task_op->get_ls_id());
    } else {
      ObDASUtils::log_user_error_and_warn(task_resp.get_rcode());
      if (OB_FAIL(task_resp.get_err_code())) {
        LOG_WARN("error occurring in remote das task", K(ret), K(task_arg));
      } else if (OB_FAIL(task_op->decode_task_result(op_result))) {
        LOG_WARN("decode das task result failed", K(ret));
      } else if (task_resp.has_more()
                  && OB_FAIL(setup_extra_result(das_ref, task_resp,
                  task_op, extra_result))) {
        LOG_WARN("setup extra result failed", KR(ret));
      } else if (task_resp.has_more() && OB_FAIL(op_result->link_extra_result(*extra_result))) {
        LOG_WARN("link extra result failed", K(ret));
      }
      if (OB_NOT_NULL(session->get_tx_desc())) {
        int tmp_ret = MTL(transaction::ObTransService*)
          ->add_tx_exec_result(*session->get_tx_desc(),
                                task_resp.get_trans_result());
        if (tmp_ret != OB_SUCCESS) {
          LOG_WARN("merge response partition failed", K(ret), K(tmp_ret), K(task_resp));
        }
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  NG_TRACE_EXT(do_remote_das_task_end, Y(ret), OB_ID(addr), task_arg.get_runner_svr());
  return ret;
}

int ObDataAccessService::collect_das_task_info(ObDASTaskArg &task_arg, ObDASRemoteInfo &remote_info)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = task_arg.get_task_op();
  if (task_op->get_ctdef() != nullptr) {
    remote_info.has_expr_ |= task_op->get_ctdef()->has_expr();
    remote_info.need_calc_expr_ |= task_op->get_ctdef()->has_pdfilter_or_calc_expr();
    remote_info.need_calc_udf_ |= task_op->get_ctdef()->has_pl_udf();
    if (OB_FAIL(add_var_to_array_no_dup(remote_info.ctdefs_, task_op->get_ctdef()))) {
      LOG_WARN("store remote ctdef failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && task_op->get_rtdef() != nullptr) {
    if (OB_FAIL(add_var_to_array_no_dup(remote_info.rtdefs_, task_op->get_rtdef()))) {
      LOG_WARN("store remote rtdef failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_array_no_dup(remote_info.ctdefs_, task_op->get_related_ctdefs()))) {
      LOG_WARN("append task op related ctdefs to remote info failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(remote_info.rtdefs_, task_op->get_related_rtdefs()))) {
      LOG_WARN("append task op related rtdefs to remote info failed", K(ret));
    }
  }
  return ret;
}

int ObDataAccessService::setup_extra_result(ObDASRef &das_ref,
                                            ObDASTaskResp &task_resp,
                                            ObIDASTaskOp *task_op,
                                            ObDASExtraData *&extra_result)
{
  int ret = OB_SUCCESS;
  extra_result = NULL;
  ObPhysicalPlanCtx *plan_ctx = das_ref.get_exec_ctx().get_physical_plan_ctx();
  int64_t timeout_ts = plan_ctx->get_timeout_timestamp();
  if (OB_UNLIKELY(!task_resp.has_more())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should not set up extra result", KR(ret), K(task_resp.has_more()));
  } else if (OB_FAIL(das_ref.get_das_factory().create_das_extra_data(extra_result))) {
    LOG_WARN("create das extra data failed", KR(ret));
  } else if (OB_FAIL(extra_result->init(task_op->get_task_id(),
                                        timeout_ts,
                                        task_resp.get_runner_svr(),
                                        GCTX.net_frame_->get_req_transport()))) {
    LOG_WARN("init extra data failed", KR(ret));
  } else {
    extra_result->set_has_more(true);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
