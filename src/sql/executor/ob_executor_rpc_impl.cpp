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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_executor_rpc_impl.h"
#include "share/ob_worker.h"
#include "share/ob_cluster_version.h"
#include "sql/ob_sql_context.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/executor/ob_remote_executor_processor.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

int ObExecutorRpcImpl::init(obrpc::ObExecutorRpcProxy* rpc_proxy, obrpc::ObBatchRpc* batch_rpc)
{
  int ret = OB_SUCCESS;
  proxy_ = rpc_proxy;
  batch_rpc_ = batch_rpc;
  return ret;
}

int ObExecutorRpcImpl::mini_task_execute(ObExecutorRpcCtx& rpc_ctx, ObMiniTask& task, ObMiniTaskResult& result)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  const ObAddr& svr = task.get_runner_server();
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
  obrpc::ObExecutorRpcProxy to_proxy = proxy_->to(svr);
  if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
  } else if (OB_FAIL(to_proxy.by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).mini_task_execute(task, result))) {
    LOG_WARN("rpc task_execute fail", K(ret), K(tenant_id), K(svr), K(timeout), K(timeout_timestamp));
    const obrpc::ObRpcResultCode& rcode = to_proxy.get_result_code();
    if (OB_LIKELY(OB_SUCCESS != rcode.rcode_)) {
      FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
    }
    deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
  }
  return ret;
}

int ObExecutorRpcImpl::mini_task_submit(ObExecutorRpcCtx& rpc_ctx, ObMiniTask& task)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  ObCurTraceId::TraceId* cur_trace_id = NULL;
  const ObAddr& svr = task.get_runner_server();
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy_ is NULL", K(ret), K(proxy_));
  } else if (OB_UNLIKELY(!rpc_ctx.min_cluster_version_is_valid())) {  // only local execution
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min cluster version is invalid", K(ret), K(rpc_ctx.get_min_cluster_version()));
  } else if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current trace id is NULL", K(ret));
  } else if (OB_ISNULL(rpc_ctx.get_ap_mini_task_mgr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ap mini task mgr is null", K(ret), K(rpc_ctx.get_ap_mini_task_mgr()));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_submit timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else {
      ObRpcAPMiniDistExecuteCB task_submit_cb(
          rpc_ctx.get_ap_mini_task_mgr(), task.get_ob_task_id(), *cur_trace_id, svr, timeout_timestamp);
      if (OB_FAIL(proxy_->to(svr)
                      .by(tenant_id)
                      .as(OB_SYS_TENANT_ID)
                      .timeout(timeout)
                      .ap_mini_task_submit(task, &task_submit_cb))) {
        LOG_WARN("rpc ap mini task_submit fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      }
      if (OB_FAIL(ret)) {
        deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
      }
    }
  }
  return ret;
}

int ObExecutorRpcImpl::ping_sql_task(ObExecutorPingRpcCtx& ping_ctx, ObPingSqlTask& ping_task)
{
  int ret = OB_SUCCESS;
  const ObAddr& exec_svr = ping_task.exec_svr_;
  uint64_t tenant_id = THIS_WORKER.get_rpc_tenant() > 0 ? THIS_WORKER.get_rpc_tenant() : ping_ctx.get_rpc_tenant_id();
  int64_t wait_timeout = ping_ctx.get_wait_timeout();
  ObRpcAPPingSqlTaskCB task_cb(ping_task.task_id_);
  OV(OB_NOT_NULL(proxy_));
  switch (ping_task.task_id_.get_task_type()) {
    case ET_DIST_TASK:
      OZ(task_cb.set_dist_task_mgr(ping_ctx.get_dist_task_mgr()));
      break;
    case ET_MINI_TASK:
      OZ(task_cb.set_mini_task_mgr(ping_ctx.get_mini_task_mgr()));
      break;
    default:
      break;
  }
  OZ(proxy_->to(exec_svr)
          .by(tenant_id)
          .as(OB_SYS_TENANT_ID)
          .timeout(wait_timeout)
          .ap_ping_sql_task(ping_task, &task_cb),
      exec_svr,
      tenant_id,
      wait_timeout,
      ping_task);
  return ret;
}

int ObExecutorRpcImpl::task_execute(ObExecutorRpcCtx& rpc_ctx, ObTask& task, const common::ObAddr& svr,
    RemoteExecuteStreamHandle& handler, bool& has_sent_task, bool& has_transfer_err)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  has_sent_task = false;
  has_transfer_err = false;
  handler.set_task_id(task.get_ob_task_id());
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  RemoteStreamHandle& real_handler = handler.get_remote_stream_handle();
  RemoteStreamHandle::MyHandle& h = real_handler.get_handle();
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_) || OB_ISNULL(real_handler.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not init", K(ret), K_(proxy), "result", real_handler.get_result());
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    obrpc::ObExecutorRpcProxy to_proxy = proxy_->to(svr);
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (FALSE_IT(has_sent_task = true)) {
    } else if (OB_FAIL(to_proxy.by(tenant_id)
                           .as(OB_SYS_TENANT_ID)
                           .timeout(timeout)
                           .task_execute(task, *real_handler.get_result(), h))) {
      LOG_WARN("rpc task_execute fail", K(ret), K(tenant_id), K(svr), K(timeout), K(timeout_timestamp));
      // rcode.rcode_ will be set in ObRpcProcessor<T>::part_response() of remote server,
      // and return to local server from remote server. so:
      // 1. if we get OB_SUCCESS from rcode.rcode_ here, transfer process must has error,
      //    such as network error or crash of remote server.
      // 2. if we get some error from rcode.rcode_ here, transfer process must has no error,
      //    otherwise we can not get rcode.rcode_ from remote server.
      const obrpc::ObRpcResultCode& rcode = to_proxy.get_result_code();
      if (OB_LIKELY(OB_SUCCESS != rcode.rcode_)) {
        FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
      } else {
        has_transfer_err = true;
      }
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  handler.set_result_code(ret);
  return ret;
}

int ObExecutorRpcImpl::task_execute_v2(ObExecutorRpcCtx& rpc_ctx, ObRemoteTask& task, const common::ObAddr& svr,
    RemoteExecuteStreamHandle& handler, bool& has_sent_task, bool& has_transfer_err)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  has_sent_task = false;
  has_transfer_err = false;
  handler.set_task_id(task.get_task_id());
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  RemoteStreamHandleV2& real_handler = handler.get_remote_stream_handle_v2();
  RemoteStreamHandleV2::MyHandle& h = real_handler.get_handle();
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_) || OB_ISNULL(real_handler.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not init", K(ret), K_(proxy), "result", real_handler.get_result());
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    obrpc::ObExecutorRpcProxy to_proxy = proxy_->to(svr);
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (FALSE_IT(has_sent_task = true)) {
    } else if (OB_FAIL(to_proxy.by(tenant_id)
                           .as(OB_SYS_TENANT_ID)
                           .timeout(timeout)
                           .remote_task_execute(task, *real_handler.get_result(), h))) {
      LOG_WARN("rpc task_execute fail", K(ret), K(tenant_id), K(svr), K(timeout), K(timeout_timestamp));
      // rcode.rcode_ will be set in ObRpcProcessor<T>::part_response() of remote server,
      // and return to local server from remote server. so:
      // 1. if we get OB_SUCCESS from rcode.rcode_ here, transfer process must has error,
      //    such as network error or crash of remote server.
      // 2. if we get some error from rcode.rcode_ here, transfer process must has no error,
      //    otherwise we can not get rcode.rcode_ from remote server.
      const obrpc::ObRpcResultCode& rcode = to_proxy.get_result_code();
      if (OB_LIKELY(OB_SUCCESS != rcode.rcode_)) {
        FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
      } else {
        has_transfer_err = true;
      }
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  handler.set_result_code(ret);
  return ret;
}

int ObExecutorRpcImpl::remote_task_submit(
    ObExecutorRpcCtx& rpc_ctx, ObRemoteTask& task, const ObAddr& svr, bool& has_sent_task)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  ObCurTraceId::TraceId* cur_trace_id = NULL;
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  has_sent_task = false;
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret), K(proxy_));
  } else if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else {
      has_sent_task = true;
      if (OB_FAIL(
              proxy_->to(svr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).remote_task_submit(task, nullptr))) {
        LOG_WARN("rpc task_submit fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      }
      if (OB_FAIL(ret)) {
        deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
      }
    }
  }
  return ret;
}

int ObExecutorRpcImpl::remote_post_result_async(
    ObExecutorRpcCtx& rpc_ctx, ObRemoteResult& remote_result, const ObAddr& svr, bool& has_sent_result)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  ObCurTraceId::TraceId* cur_trace_id = NULL;
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  has_sent_result = false;
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret), K(proxy_));
  } else if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else {
      has_sent_result = true;
      if (OB_FAIL(proxy_->to(svr)
                      .by(tenant_id)
                      .as(OB_SYS_TENANT_ID)
                      .timeout(timeout)
                      .remote_post_result(remote_result, nullptr))) {
        LOG_WARN("rpc task_submit fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      }
    }
  }
  return ret;
}

int ObExecutorRpcImpl::task_submit(
    ObExecutorRpcCtx& rpc_ctx, ObTask& task, const common::ObAddr& svr, const TransResult* trans_result) const
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  ObCurTraceId::TraceId* cur_trace_id = NULL;
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret), K(proxy_));
  } else if (OB_ISNULL(trans_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_result is NULL", K(ret));
  } else if (OB_UNLIKELY(!rpc_ctx.min_cluster_version_is_valid())) {  // only local execution
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("min cluster version is invalid", K(ret), K(rpc_ctx.get_min_cluster_version()));
  } else if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trace id is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else {
      ObRpcAPDistExecuteCB task_submit_cb(
          task.get_runner_server(), task.get_ob_task_id(), *cur_trace_id, timeout_timestamp);
      task.set_max_sql_no(trans_result->get_max_sql_no());
      if (OB_FAIL(proxy_->to(svr)
                      .by(tenant_id)
                      .as(OB_SYS_TENANT_ID)
                      .timeout(timeout)
                      .ap_task_submit(task, &task_submit_cb))) {
        LOG_WARN("rpc task_submit fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      }
      if (OB_FAIL(ret)) {
        deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
      }
    }
  }
  return ret;
}

/*
 * kill task and wait
 * */
int ObExecutorRpcImpl::task_kill(ObExecutorRpcCtx& rpc_ctx, const ObTaskID& task_id, const common::ObAddr& svr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(proxy_->to(svr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).task_kill(task_id))) {
      LOG_WARN("rpc task_kill fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  return ret;
}

/*
 * task complete, wakeup scheduler
 * */
int ObExecutorRpcImpl::task_complete(ObExecutorRpcCtx& rpc_ctx, ObTaskCompleteEvent& task, const common::ObAddr& svr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(proxy_->to(svr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).task_complete(task))) {
      LOG_WARN("rpc task_complete fail", K(ret), K(tenant_id), K(svr), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  return ret;
}

int ObExecutorRpcImpl::task_notify_fetch(ObExecutorRpcCtx& rpc_ctx, ObTaskEvent& task_event, const common::ObAddr& svr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    proxy_->set_server(svr);
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(
                   proxy_->to(svr).by(tenant_id).as(OB_SYS_TENANT_ID).timeout(timeout).task_notify_fetch(task_event))) {
      LOG_WARN("rpc task_notify_fetch fail", K(ret), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  return ret;
}

int ObExecutorRpcImpl::task_fetch_result(ObExecutorRpcCtx& rpc_ctx, const ObSliceID& ob_slice_id,
    const common::ObAddr& svr, FetchResultStreamHandle& handler)
{
  handler.set_task_id(ob_slice_id.get_ob_task_id());
  OB_ASSERT(NULL != handler.get_result());
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(proxy_->to(svr)
                           .by(tenant_id)
                           .as(OB_SYS_TENANT_ID)
                           .timeout(timeout)
                           .task_fetch_result(ob_slice_id, *handler.get_result(), handler.get_handle()))) {
      LOG_WARN(
          "rpc task_notify_fetch fail", K(ret), K(ob_slice_id), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  handler.set_result_code(ret);
  return ret;
}

int ObExecutorRpcImpl::task_fetch_interm_result(ObExecutorRpcCtx& rpc_ctx, const ObSliceID& ob_slice_id,
    const common::ObAddr& svr, FetchIntermResultStreamHandle& handler)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_) || OB_ISNULL(handler.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy or handler result is null", K(ret), K(proxy_));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(proxy_->to(svr)
                           .by(tenant_id)
                           .as(OB_SYS_TENANT_ID)
                           .timeout(timeout)
                           .task_fetch_interm_result(ob_slice_id, *handler.get_result(), handler.get_handle()))) {
      LOG_WARN(
          "rpc task_notify_fetch fail", K(ret), K(ob_slice_id), K(svr), K(tenant_id), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  handler.set_result_code(ret);
  return ret;
}

int ObExecutorRpcImpl::fetch_interm_result_item(ObExecutorRpcCtx& rpc_ctx, const common::ObAddr& dst,
    const ObSliceID& slice_id, const int64_t fetch_index, ObFetchIntermResultItemRes& res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }
  ObFetchIntermResultItemArg arg;
  arg.slice_id_ = slice_id;
  arg.index_ = fetch_index;
  const int64_t timeout = rpc_ctx.get_timeout_timestamp() - ObTimeUtility::current_time();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy is null", K(ret), K(proxy_));
  } else if (timeout < 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout", K(ret), K(dst), K(rpc_ctx.get_timeout_timestamp()));
  } else if (OB_FAIL(proxy_->to(dst)
                         .by(tenant_id)
                         .as(OB_SYS_TENANT_ID)
                         .timeout(timeout)
                         .fetch_interm_result_item(arg, res))) {
    LOG_WARN("fetch interm result interm failed", K(ret), K(dst), K(arg));
  }
  return ret;
}

int ObExecutorRpcImpl::close_result(ObExecutorRpcCtx& rpc_ctx, ObSliceID& ob_slice_id, const common::ObAddr& svr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("proxy_ is NULL", K(ret));
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc", K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (OB_FAIL(proxy_->to(svr)
                           .by(tenant_id)
                           .as(OB_SYS_TENANT_ID)
                           .timeout(timeout)
                           .close_result(ob_slice_id, NULL))) {
      LOG_WARN("rpc close_result fail", K(ret), K(svr), K(tenant_id), K(ob_slice_id), K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  return ret;
}

void ObExecutorRpcImpl::deal_with_rpc_timeout_err(ObExecutorRpcCtx& rpc_ctx, int& err, const ObAddr& dist_server) const
{
  if (OB_TIMEOUT == err) {
    int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_timestamp - cur_timestamp > 0) {
      LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
                "change error code to OB_CONNECT_ERROR",
          K(err),
          K(timeout_timestamp),
          K(cur_timestamp));
      ObQueryRetryInfo* retry_info = rpc_ctx.get_retry_info_for_update();
      if (NULL != retry_info) {
        int a_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (a_ret = retry_info->add_invalid_server_distinctly(dist_server)))) {
          LOG_WARN("fail to add invalid server distinctly", K(a_ret), K(dist_server));
        } else {
          // LOG_INFO("YZFDEBUG add invalid server distinctly", K(a_ret), K(dist_server), "p",
          // &retry_info->get_invalid_servers());
        }
      }
      err = OB_RPC_CONNECT_ERROR;
    } else {
      LOG_DEBUG("rpc return OB_TIMEOUT, and it is actually timeout, "
                "do not change error code",
          K(err),
          K(timeout_timestamp),
          K(cur_timestamp));
      ObQueryRetryInfo* retry_info = rpc_ctx.get_retry_info_for_update();
      if (NULL != retry_info) {
        retry_info->set_is_rpc_timeout(true);
      }
    }
  }
}

int ObExecutorRpcCtx::check_status() const
{
  int ret = OB_SUCCESS;
  int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
  if (cur_timestamp > timeout_timestamp_) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(cur_timestamp), K(timeout_timestamp_), K(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (session_->is_terminate(ret)) {
    LOG_WARN("execution was terminated", K(ret));
  }
  return ret;
}

int ObExecutorRpcImpl::get_sql_batch_req_type(int64_t execution_id) const
{
  int type = 0;
  if (execution_id & 0x1) {
    type = obrpc::SQL_BATCH_REQ_NODELAY1;
  } else {
    type = obrpc::SQL_BATCH_REQ_NODELAY2;
  }
  return type;
}

int ObExecutorRpcImpl::remote_task_batch_submit(const uint64_t tenant_id, const ObAddr& server,
    const int64_t cluster_id, const ObRemoteTask& task, bool& has_sent_task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey fake_pkey;
  int batch_req_type = get_sql_batch_req_type(task.get_task_id().get_execution_id());
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("executor rpc not init", K(ret));
  } else if (OB_FAIL(batch_rpc_->post(
                 tenant_id, server, cluster_id, batch_req_type, OB_SQL_REMOTE_TASK_TYPE, fake_pkey, task))) {
    LOG_WARN("post batch rpc failed", K(ret));
  } else {
    has_sent_task = true;
  }
  return ret;
}

int ObExecutorRpcImpl::remote_batch_post_result(const uint64_t tenant_id, const common::ObAddr& server,
    const int64_t cluster_id, const ObRemoteResult& result, bool& has_sent_result)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey fake_pkey;
  int batch_req_type = get_sql_batch_req_type(result.get_task_id().get_execution_id());
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("executor rpc not init", K(ret));
  } else if (OB_FAIL(batch_rpc_->post(
                 tenant_id, server, cluster_id, batch_req_type, OB_SQL_REMOTE_RESULT_TYPE, fake_pkey, result))) {
    LOG_WARN("post batch rpc failed", K(ret));
  } else {
    has_sent_result = true;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
