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
#include "lib/signal/ob_signal_struct.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/ob_trans_service.h"
#include "observer/ob_srv_network_frame.h"
#include "lib/ash/ob_active_session_guard.h"

namespace oceanbase
{
namespace sql
{
template<obrpc::ObRpcPacketCode pcode>
int ObDASBaseAccessP<pcode>::init()
{
  int ret = OB_SUCCESS;
  ObDASTaskArg &task = RpcProcessor::arg_;
  ObDASBaseAccessP<pcode>::get_das_factory() = &das_factory_;
  das_remote_info_.exec_ctx_ = &exec_ctx_;
  das_remote_info_.frame_info_ = &frame_info_;
  task.set_remote_info(&das_remote_info_);
  ObDASRemoteInfo::get_remote_info() = &das_remote_info_;
  return ret;
}

template<obrpc::ObRpcPacketCode pcode>
int ObDASBaseAccessP<pcode>::before_process()
{
  int ret = OB_SUCCESS;
  ObDASTaskArg &task = RpcProcessor::arg_;
  ObDASTaskResp &task_resp = RpcProcessor::result_;
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = task.get_task_op()->get_tenant_id();
  mem_attr.label_ = "DASRpcPCtx";
  exec_ctx_.get_allocator().set_attr(mem_attr);
  ObActiveSessionGuard::setup_thread_local_ash();
  ObActiveSessionGuard::get_stat().in_das_remote_exec_ = true;
  ObActiveSessionGuard::get_stat().tenant_id_ = task.get_task_op()->get_tenant_id();
  ObActiveSessionGuard::get_stat().trace_id_ = *ObCurTraceId::get_trace_id();
  ObActiveSessionGuard::get_stat().user_id_ = das_remote_info_.user_id_;
  ObActiveSessionGuard::get_stat().session_id_ = das_remote_info_.session_id_;
  ObActiveSessionGuard::get_stat().plan_id_ = das_remote_info_.plan_id_;
  MEMCPY(ObActiveSessionGuard::get_stat().sql_id_, das_remote_info_.sql_id_,
      min(sizeof(ObActiveSessionGuard::get_stat().sql_id_), sizeof(das_remote_info_.sql_id_)));
  if (OB_FAIL(RpcProcessor::before_process())) {
    LOG_WARN("do rpc processor before_process failed", K(ret));
  } else if (das_remote_info_.need_calc_expr_ &&
      OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    exec_ctx_.get_sql_ctx()->schema_guard_ = &schema_guard_;
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_DAS_SIMULATE_EXTRA_RESULT_MEMORY_LIMIT);

template<obrpc::ObRpcPacketCode pcode>
int ObDASBaseAccessP<pcode>::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("DAS base access remote process", K_(RpcProcessor::arg));
  ObDASTaskArg &task = RpcProcessor::arg_;
  ObDASTaskResp &task_resp = RpcProcessor::result_;
  SQL_INFO_GUARD(ObString("DAS REMOTE PROCESS"), task.get_remote_info()->sql_id_);
  const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops = task.get_task_ops();
  common::ObSEArray<ObIDASTaskResult*, 2> &task_results = task_resp.get_op_results();
  ObDASTaskFactory *das_factory = ObDASBaseAccessP<pcode>::get_das_factory();
  ObIDASTaskResult *op_result = nullptr;
  ObIDASTaskOp *task_op = nullptr;
  bool has_more = false;
  int64_t memory_limit = das::OB_DAS_MAX_PACKET_SIZE;
#ifdef ERRSIM
  if (EN_DAS_SIMULATE_EXTRA_RESULT_MEMORY_LIMIT) {
    memory_limit = -EN_DAS_SIMULATE_EXTRA_RESULT_MEMORY_LIMIT;
    LOG_INFO("das simulate extra result memory limit", K(memory_limit));
  }
#endif
  //regardless of the success of the task execution, the following meta info must be set
  task_resp.set_ctrl_svr(task.get_ctrl_svr());
  task_resp.set_runner_svr(task.get_runner_svr());
  if (task_ops.count() == 0 || task_results.count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task op unexpected", K(ret), K(task_ops), K(task_results));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < task_ops.count(); i++) {
      task_op = task_ops.at(i);
      if (OB_FAIL(das_factory->create_das_task_result(task_op->get_type(), op_result))) {
        LOG_WARN("create das task result failed", K(ret));
      } else if (OB_FAIL(task_resp.add_op_result(op_result))) {
        LOG_WARN("failed to add op result", K(ret));
      } else if (OB_FAIL(op_result->init(*task_op, CURRENT_CONTEXT->get_arena_allocator()))) {
        LOG_WARN("failed to init op result", K(ret));
      } else if (FALSE_IT(op_result->set_task_id(task_op->get_task_id()))) {
      } else if (OB_FAIL(task_op->start_das_task())) {
        LOG_WARN("start das task failed", K(ret));
      } else if (OB_FAIL(task_op->fill_task_result(*task_results.at(i), has_more, memory_limit))) {
        LOG_WARN("fill task result to controller failed", K(ret));
      } else if (OB_UNLIKELY(has_more) && OB_FAIL(task_op->fill_extra_result())) {
        LOG_WARN("fill extra result to controller failed", KR(ret));
      } else {
        task_resp.set_has_more(has_more);
        ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
        if (wb != nullptr) {
          //ignore the errcode of storing warning msg
          (void)task_resp.store_warning_msg(*wb);
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(op_result)) {
        // accessing failed das task result is undefined behavior.
        op_result->reuse();
      }
      //因为end_task还有可能失败，需要通过RPC将end_task的返回值带回到scheduler上
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
        LOG_WARN("process das access task failed", K(ret),
                K(task.get_ctrl_svr()), K(task.get_runner_svr()));
      }
      if (has_more || memory_limit < 0) {
        /**
         * das serialized execution.
         * If current resp buffer is overflow. We would reply result
         * directly. following un-executed tasks would be executed
         * later remotely.
         *
         * the insert op won't set has_more flag, but if it exceed the
         * threshold of memory_limit, we should reply anyway.
         */
        LOG_DEBUG("reply das result due to memory limit exceeded.",
            K(has_more), K(memory_limit), K(i), K(task_ops.count()));
        break;
      }
      LOG_DEBUG("process das base access task", K(ret), KPC(task_op), KPC(op_result), K(has_more));
    }
  }
  return OB_SUCCESS;
}

template<obrpc::ObRpcPacketCode pcode>
int ObDASBaseAccessP<pcode>::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  const int64_t elapsed_time = common::ObTimeUtility::current_time() - RpcProcessor::get_receive_timestamp();
  if (OB_FAIL(RpcProcessor::after_process(error_code))) {
    LOG_WARN("do das base rpc process failed", K(ret));
  } else if (elapsed_time >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    //slow das task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow das rpc process]");
  }
  //执行相关的错误信息不用传递给RPC框架，RPC框架不处理具体的RPC执行错误信息，始终返回OB_SUCCESS
  return OB_SUCCESS;
}

template<obrpc::ObRpcPacketCode pcode>
void ObDASBaseAccessP<pcode>::cleanup()
{
  ObActiveSessionGuard::get_stat().reuse();
  ObActiveSessionGuard::setup_default_ash();
  das_factory_.cleanup();
  ObDASBaseAccessP<pcode>::get_das_factory() = nullptr;
  if (das_remote_info_.trans_desc_ != nullptr) {
    MTL(transaction::ObTransService*)->release_tx(*das_remote_info_.trans_desc_);
    das_remote_info_.trans_desc_ = nullptr;
  }
  RpcProcessor::cleanup();
}

int ObDASSyncAccessP::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("DAS sync access remote process", K_(arg));
  FLTSpanGuard(das_sync_rpc_process);
  if (OB_FAIL(ObDASSyncRpcProcessor::process())) {
    LOG_WARN("failed to process das sync rpc", K(ret));
  }
  return OB_SUCCESS;
}

int ObDASAsyncAccessP::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("DAS async access remote process", K_(arg));
  FLTSpanGuard(das_async_rpc_process);
  if (OB_FAIL(ObDASAsyncRpcProcessor::process())) {
    LOG_WARN("failed to process das async rpc", K(ret));
  }
  return OB_SUCCESS;
}

void ObRpcDasAsyncAccessCallBack::on_timeout()
{
  int ret = OB_TIMEOUT;
  int64_t current_ts = ObTimeUtility::current_time();
  int64_t timeout_ts = context_->get_timeout_ts();
  // ESTIMATE_PS_RESERVE_TIME = 100 * 1000
  if (timeout_ts - current_ts > 100 * 1000) {
    LOG_DEBUG("rpc return OB_TIMEOUT before actual timeout, change error code to OB_RPC_CONNECT_ERROR", KR(ret),
              K(timeout_ts), K(current_ts));
    ret = OB_RPC_CONNECT_ERROR;
  }
  LOG_WARN("das async task timeout", KR(ret), K(get_task_ops()));
  result_.set_err_code(ret);
  result_.get_op_results().reuse();
  context_->get_das_ref().inc_concurrency_limit_with_signal();
}

void ObRpcDasAsyncAccessCallBack::on_invalid()
{
  int ret = OB_SUCCESS;
  // a valid packet on protocol level, but can't decode it.
  LOG_WARN("das async task invalid", K(get_task_ops()));
  result_.set_err_code(OB_INVALID_ERROR);
  result_.get_op_results().reuse();
  context_->get_das_ref().inc_concurrency_limit_with_signal();
}

void ObRpcDasAsyncAccessCallBack::set_args(const Request &arg)
{
  UNUSED(arg);
}

int ObRpcDasAsyncAccessCallBack::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("DAS async access callback process", K_(result));
  if (OB_FAIL(get_rcode())) {
    result_.set_err_code(get_rcode());
    // we need to clear op results because they are not decoded from das async rpc due to rpc error.
    result_.get_op_results().reuse();
    LOG_WARN("das async rpc execution failed", K(get_rcode()), K_(result));
  }
  context_->get_das_ref().inc_concurrency_limit_with_signal();
  return ret;
}

oceanbase::rpc::frame::ObReqTransport::AsyncCB *ObRpcDasAsyncAccessCallBack::clone(
    const oceanbase::rpc::frame::SPAlloc &alloc) const {
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB * const>(this));
}

int ObDasAsyncRpcCallBackContext::init(const ObMemAttr &attr)
{
  alloc_.set_attr(attr);
  return task_ops_.get_copy_assign_ret();
};

int ObDASSyncFetchP::process()
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(fetch_das_result_process);
  ObDASDataFetchReq &req = arg_;
  ObDASDataFetchRes &res = result_;
  ObDataAccessService *das = NULL;
  const uint64_t tenant_id = req.get_tenant_id();
  const int64_t task_id = req.get_task_id();
  if (tenant_id != MTL_ID()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong tenant id", KR(ret), K(req));
  } else if (OB_FAIL(res.init(tenant_id, task_id))) {
    LOG_WARN("init res failed", KR(ret), K(req));
  } else if (OB_ISNULL(das = MTL(ObDataAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das is null", KR(ret), KP(das));
  } else if (OB_FAIL(das->get_task_res_mgr().iterator_task_result(res))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // After server reboot, the hash map containing task results was gone.
      // We need to retry for such cases.
      LOG_WARN("task result was gone due to server reboot, will retry", KR(ret), K(res));
      ret = OB_RPC_SEND_ERROR;
    } else {
      LOG_WARN("get task result failed", KR(ret), K(res));
    }
  }
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
