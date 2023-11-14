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
#include "lib/worker.h"
#include "share/ob_cluster_version.h"
#include "sql/ob_sql_context.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/executor/ob_remote_executor_processor.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

int ObExecutorRpcImpl::init(obrpc::ObExecutorRpcProxy *rpc_proxy, obrpc::ObBatchRpc *batch_rpc)
{
  int ret = OB_SUCCESS;
  proxy_ = rpc_proxy;
  batch_rpc_ = batch_rpc;
  return ret;
}

int ObExecutorRpcImpl::task_execute(ObExecutorRpcCtx &rpc_ctx,
                                    ObTask &task,
                                    const common::ObAddr &svr,
                                    RemoteExecuteStreamHandle &handler,
                                    bool &has_sent_task,
                                    bool &has_transfer_err)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  has_sent_task = false;
  has_transfer_err = false;
  handler.set_task_id(task.get_ob_task_id());
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  RemoteStreamHandle &real_handler = handler.get_remote_stream_handle();
  RemoteStreamHandle::MyHandle &h = real_handler.get_handle();
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  const int32_t group_id = rpc_ctx.get_group_id();
  if (OB_ISNULL(proxy_) || OB_ISNULL(real_handler.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not init", K(ret), K_(proxy), "result", real_handler.get_result());
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    obrpc::ObExecutorRpcProxy to_proxy = proxy_->to(svr);
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc",
               K(ret),
               K(svr),
               K(timeout),
               K(timeout_timestamp));
    } else if (FALSE_IT(has_sent_task = true)) {
    } else if (OB_FAIL(to_proxy
                       .by(tenant_id)
                       .timeout(timeout)
                       .group_id(group_id)
                       .task_execute(task, *real_handler.get_result(), h))) {
      LOG_WARN("rpc task_execute fail",
               K(ret),
               K(tenant_id),
               K(svr),
               K(timeout),
               K(timeout_timestamp));
      // rcode.rcode_ will be set in ObRpcProcessor<T>::part_response() of remote server,
      // and return to local server from remote server. so:
      // 1. if we get OB_SUCCESS from rcode.rcode_ here, transfer process must has error,
      //    such as network error or crash of remote server.
      // 2. if we get some error from rcode.rcode_ here, transfer process must has no error,
      //    otherwise we can not get rcode.rcode_ from remote server.
      const obrpc::ObRpcResultCode &rcode = to_proxy.get_result_code();
      if (OB_LIKELY(OB_SUCCESS != rcode.rcode_)) {
        FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
      } else if (OB_RPC_SEND_ERROR == ret || OB_RPC_POST_ERROR == ret) {
        // these two error means the request hasn't been sent out to network
        // either because the server is in blacklist or network link breaks
        has_sent_task = false;
      } else {
        has_transfer_err = true;
      }
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  handler.set_result_code(ret);
  return ret;
}

int ObExecutorRpcImpl::task_execute_v2(ObExecutorRpcCtx &rpc_ctx,
                                       ObRemoteTask &task,
                                       const common::ObAddr &svr,
                                       RemoteExecuteStreamHandle &handler,
                                       bool &has_sent_task,
                                       bool &has_transfer_err)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = rpc_ctx.get_rpc_tenant_id();

  has_sent_task = false;
  has_transfer_err = false;
  handler.set_task_id(task.get_task_id());
  if (THIS_WORKER.get_rpc_tenant() > 0) {
    tenant_id = THIS_WORKER.get_rpc_tenant();
  }

  RemoteStreamHandleV2 &real_handler = handler.get_remote_stream_handle_v2();
  RemoteStreamHandleV2::MyHandle &h = real_handler.get_handle();
  int64_t timeout_timestamp = rpc_ctx.get_timeout_timestamp();
  const int32_t group_id = rpc_ctx.get_group_id();
  if (OB_ISNULL(proxy_) || OB_ISNULL(real_handler.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not init", K(ret), K_(proxy), "result", real_handler.get_result());
  } else {
    int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
    obrpc::ObExecutorRpcProxy to_proxy = proxy_->to(svr);
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("task_execute timeout before rpc",
               K(ret), K(svr), K(timeout), K(timeout_timestamp));
    } else if (FALSE_IT(has_sent_task = true)) {
    } else if (OB_FAIL(to_proxy
                       .by(tenant_id)
                       .timeout(timeout)
                       .group_id(group_id)
                       .remote_task_execute(task, *real_handler.get_result(), h))) {
      LOG_WARN("rpc task_execute fail",
               K(ret), K(tenant_id), K(svr), K(timeout), K(timeout_timestamp));
      // rcode.rcode_ will be set in ObRpcProcessor<T>::part_response() of remote server,
      // and return to local server from remote server. so:
      // 1. if we get OB_SUCCESS from rcode.rcode_ here, transfer process must has error,
      //    such as network error or crash of remote server.
      // 2. if we get some error from rcode.rcode_ here, transfer process must has no error,
      //    otherwise we can not get rcode.rcode_ from remote server.
      const obrpc::ObRpcResultCode &rcode = to_proxy.get_result_code();
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

/*
 * 发送杀死一个task的命令并阻塞等待对端返回执行状态
 * */
int ObExecutorRpcImpl::task_kill(
    ObExecutorRpcCtx &rpc_ctx,
    const ObTaskID &task_id,
    const common::ObAddr &svr)
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
      LOG_WARN("task_execute timeout before rpc",
               K(ret),
               K(svr),
               K(timeout),
               K(timeout_timestamp));
    } else if (OB_FAIL(proxy_
                       ->to(svr)
                       .by(tenant_id)
                       .timeout(timeout)
                       .task_kill(task_id))) {
      LOG_WARN("rpc task_kill fail", K(ret), K(svr), K(tenant_id),
               K(timeout), K(timeout_timestamp));
      deal_with_rpc_timeout_err(rpc_ctx, ret, svr);
    }
  }
  return ret;
}

void ObExecutorRpcImpl::deal_with_rpc_timeout_err(ObExecutorRpcCtx &rpc_ctx,
                                                  int &err,
                                                  const ObAddr &dist_server) const
{
  if (OB_TIMEOUT == err) {
    int64_t timeout_timestamp = rpc_ctx.get_ps_timeout_timestamp();
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_timestamp - cur_timestamp > 0) {
      LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
                "change error code to OB_CONNECT_ERROR", K(err),
                K(timeout_timestamp), K(cur_timestamp));
      err = OB_RPC_CONNECT_ERROR;
    } else {
      LOG_DEBUG("rpc return OB_TIMEOUT, and it is actually timeout, "
                "do not change error code", K(err),
                K(timeout_timestamp), K(cur_timestamp));
      ObQueryRetryInfo *retry_info = rpc_ctx.get_retry_info_for_update();
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

int ObExecutorRpcImpl::remote_task_batch_submit(const uint64_t tenant_id,
                                                const ObAddr &server,
                                                const int64_t cluster_id,
                                                const ObRemoteTask &task,
                                                bool &has_sent_task)
{
  // 多个租户共用同一个系统租户的聚合线程
  int ret = OB_SUCCESS;
  int batch_req_type = get_sql_batch_req_type(task.get_task_id().get_execution_id());
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("executor rpc not init", K(ret));
  } else if (OB_FAIL(batch_rpc_->post(tenant_id,
                                      server, cluster_id,
                                      batch_req_type, OB_SQL_REMOTE_TASK_TYPE,
                                      task))) {
    LOG_WARN("post batch rpc failed", K(ret));
  } else {
    has_sent_task = true;
  }
  return ret;
}

int ObExecutorRpcImpl::remote_batch_post_result(const uint64_t tenant_id,
                                                const common::ObAddr &server,
                                                const int64_t cluster_id,
                                                const ObRemoteResult &result,
                                                bool &has_sent_result)
{
  // 多个租户共用同一个系统租户的聚合线程
  int ret = OB_SUCCESS;
  int batch_req_type = get_sql_batch_req_type(result.get_task_id().get_execution_id());
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("executor rpc not init", K(ret));
  } else if (OB_FAIL(batch_rpc_->post(tenant_id,
                                      server, cluster_id,
                                      batch_req_type, OB_SQL_REMOTE_RESULT_TYPE,
                                      result))) {
    LOG_WARN("post batch rpc failed", K(ret));
  } else {
    has_sent_result = true;
  }
  return ret;
}
}/* ns sql*/
}/* ns oceanbase */
