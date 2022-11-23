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
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/ob_trans_service.h"
namespace oceanbase
{
namespace sql
{
int ObDASSyncAccessP::init()
{
  int ret = OB_SUCCESS;
  ObDASTaskArg &task = arg_;
  ObDASSyncAccessP::get_das_factory() = &das_factory_;
  das_remote_info_.exec_ctx_ = &exec_ctx_;
  das_remote_info_.frame_info_ = &frame_info_;
  task.set_remote_info(&das_remote_info_);
  ObDASRemoteInfo::get_remote_info() = &das_remote_info_;
  return ret;
}

int ObDASSyncAccessP::before_process()
{
  int ret = OB_SUCCESS;
  ObDASTaskArg &task = arg_;
  ObDASTaskResp &task_resp = result_;
  ObIDASTaskResult *task_result = nullptr;
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = task.get_task_op()->get_tenant_id();
  mem_attr.label_ = "DASRpcPCtx";
  exec_ctx_.get_allocator().set_attr(mem_attr);
  ObDASTaskFactory *das_factory = ObDASSyncAccessP::get_das_factory();
  if (OB_ISNULL(das_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das factory is not inited", K(ret));
  } else if (OB_FAIL(ObDASSyncRpcProcessor::before_process())) {
    LOG_WARN("do rpc processor before_process failed", K(ret));
  } else if (das_remote_info_.need_calc_udf_ &&
      OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(das_factory->create_das_task_result(task.get_task_op()->get_type(),
                                                         task_result))) {
    LOG_WARN("create das task result failed", K(ret), K(task));
  } else if (OB_FAIL(task_result->init(*task.get_task_op()))) {
    LOG_WARN("init task result failed", K(ret), KPC(task_result), KPC(task.get_task_op()));
  } else if (OB_FAIL(task_resp.add_op_result(task_result))) {
    LOG_WARN("failed to add das op result", K(ret), K(*task_result));
  } else {
    exec_ctx_.get_sql_ctx()->schema_guard_ = &schema_guard_;
  }
  return ret;
}

int ObDASSyncAccessP::process()
{
  int ret = OB_SUCCESS;
  NG_TRACE(das_rpc_process_begin);
  FLTSpanGuard(das_rpc_process);
  ObDASTaskArg &task = arg_;
  ObDASTaskResp &task_resp = result_;
  ObIDASTaskOp *task_op = task.get_task_op();
  ObIDASTaskResult *task_result = task_resp.get_op_result();
  bool has_more = false;
  ObDASOpType task_type = DAS_OP_INVALID;
  //regardless of the success of the task execution, the fllowing meta info must be set
  task_result->set_task_id(task_op->get_task_id());
  task_resp.set_ctrl_svr(task.get_ctrl_svr());
  task_resp.set_runner_svr(task.get_runner_svr());
  if (OB_ISNULL(task_op) || OB_ISNULL(task_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task op is nullptr", K(ret), K(task_op), K(task_result));
  } else if (OB_FAIL(task_op->start_das_task())) {
    LOG_WARN("start das task failed", K(ret));
  } else if (OB_FAIL(task_op->fill_task_result(*task_result, has_more))) {
    LOG_WARN("fill task result to controller failed", K(ret));
  } else if (OB_UNLIKELY(has_more) && OB_FAIL(task_op->fill_extra_result())) {
    LOG_WARN("fill extra result to controller failed", KR(ret));
  } else {
    task_type = task_op->get_type();
    task_resp.set_has_more(has_more);
    ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
    if (wb != nullptr) {
      //ignore the errcode of storing warning msg
      (void)task_resp.store_warning_msg(*wb);
    }
  }
  //因为end_task还有可能失败，需要通过RPC将end_task的返回值带回到scheduler上
  if (OB_NOT_NULL(task_op)) {
    int tmp_ret = task_op->end_das_task();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end das task failed", K(ret), K(tmp_ret), K(task));
    }
    ret = COVER_SUCC(tmp_ret);
    if (OB_NOT_NULL(task_op->get_trans_desc())) {
      tmp_ret = MTL(transaction::ObTransService*)
        ->get_tx_exec_result(*task_op->get_trans_desc(),
                            task_resp.get_trans_result());
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("get trans exec result failed", K(ret), K(task));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_SUCCESS != ret && is_schema_error(ret)) {
      ret = GSCHEMASERVICE.is_schema_error_need_retry(NULL, task_op->get_tenant_id()) ?
            OB_ERR_REMOTE_SCHEMA_NOT_FULL : OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
    }
    task_resp.set_err_code(ret);
    if (OB_SUCCESS != ret) {
      task_resp.store_err_msg(ob_get_tsi_err_msg(ret));
      LOG_WARN("process das sync access task failed", K(ret),
              K(task.get_ctrl_svr()), K(task.get_runner_svr()));
    }
  }
  LOG_DEBUG("process das sync access task", K(ret), K(task), KPC(task_result), K(has_more));
  NG_TRACE_EXT(das_rpc_process_end, OB_ID(type), task_type);
  return OB_SUCCESS;
}

int ObDASSyncAccessP::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
  if (OB_FAIL(ObDASSyncRpcProcessor::after_process(error_code))) {
    LOG_WARN("do das sync base rpc process failed", K(ret));
  } else if (elapsed_time >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    //slow das task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow das rpc process]");
  }
  //执行相关的错误信息不用传递给RPC框架，RPC框架不处理具体的RPC执行错误信息，始终返回OB_SUCCESS
  return OB_SUCCESS;
}

void ObDASSyncAccessP::cleanup()
{
  ObActiveSessionGuard::setup_default_ash();
  das_factory_.cleanup();
  ObDASSyncAccessP::get_das_factory() = nullptr;
  if (das_remote_info_.trans_desc_ != nullptr) {
    MTL(transaction::ObTransService*)->release_tx(*das_remote_info_.trans_desc_);
    das_remote_info_.trans_desc_ = nullptr;
  }
  ObDASSyncRpcProcessor::cleanup();
}

int ObDASSyncFetchP::process()
{
  int ret = OB_SUCCESS;
  NG_TRACE(fetch_das_result_process_begin);
  FLTSpanGuard(fetch_das_result_process);
  ObDASDataFetchReq &req = arg_;
  ObDASDataFetchRes &res = result_;
  ObDataAccessService *das = NULL;
  const uint64_t tenant_id = req.get_tenant_id();
  const int64_t task_id = req.get_task_id();
  ObChunkDatumStore &datum_store = res.get_datum_store();
  bool has_more = false;
  if (tenant_id != MTL_ID()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong tenant id", KR(ret), K(req));
  } else if (OB_FAIL(res.init(tenant_id, task_id))) {
    LOG_WARN("init res failed", KR(ret), K(req));
  } else if (OB_ISNULL(das = MTL(ObDataAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das is null", KR(ret), KP(das));
  } else if (OB_FAIL(das->get_task_res_mgr().iterator_task_result(task_id,
                                                                  datum_store,
                                                                  has_more))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // After server reboot, the hash map containing task results was gone.
      // We need to retry for such cases.
      LOG_WARN("task result was gone due to server reboot, will retry", KR(ret), K(res));
      ret = OB_RPC_SEND_ERROR;
    } else {
      LOG_WARN("get task result failed", KR(ret), K(res));
    }
  } else {
    res.set_has_more(has_more);
  }
  NG_TRACE(fetch_das_result_process_end);
  return ret;
}

int ObDASSyncFetchP::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
  if (OB_FAIL(ObDASSyncFetchResRpcProcessor::after_process(error_code))) {
    LOG_WARN("do das sync base rpc process failed", K(ret));
  } else if (elapsed_time >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    //slow das task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow das rpc process]");
  }
  //执行相关的错误信息不用传递给RPC框架，RPC框架不处理具体的RPC执行错误信息，始终返回OB_SUCCESS
  return OB_SUCCESS;
}

int ObDASAsyncEraseP::process()
{
  int ret = OB_SUCCESS;
  ObDASDataEraseReq &req = arg_;
  ObDataAccessService *das = NULL;
  const uint64_t tenant_id = req.get_tenant_id();
  const int64_t task_id = req.get_task_id();
  if (OB_ISNULL(das = MTL(ObDataAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das is null", KR(ret), KP(das));
  } else if (OB_FAIL(das->get_task_res_mgr().erase_task_result(task_id))) {
    LOG_WARN("erase task result failed", KR(ret), K(task_id));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
