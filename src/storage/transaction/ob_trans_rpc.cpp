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

#include "ob_trans_rpc.h"
#include "share/ob_errno.h"
#include "share/ob_cluster_version.h"
#include "lib/oblog/ob_log.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "ob_trans_service.h"
#include "ob_trans_msg_type.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {

using namespace common;
using namespace transaction;
using namespace obrpc;
using namespace storage;

namespace transaction {
int refresh_location_cache(ObTransService* trans_service, const ObPartitionKey& partition, const bool need_clear_cache)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_service) || !partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", KP(trans_service), K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(trans_service->refresh_location_cache(partition, need_clear_cache))) {
    TRANS_LOG(WARN, "refresh location cache error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int handle_trans_msg_callback(ObTransService* trans_service, const ObPartitionKey& partition, const ObTransID& trans_id,
    const int64_t msg_type, const int status, const int64_t task_type, const ObAddr& addr, const int64_t sql_no,
    const int64_t request_id)
{
  int ret = OB_SUCCESS;
  transaction::TransRpcTask* task = NULL;

  // skip verify sql_no
  if (OB_ISNULL(trans_service) || !partition.is_valid() || !trans_id.is_valid() || !addr.is_valid() ||
      !ObTransMsgTypeChecker::is_valid_msg_type(msg_type) || !transaction::ObTransRetryTaskType::is_valid(task_type)) {
    TRANS_LOG(WARN,
        "invalid argument",
        KP(trans_service),
        K(partition),
        K(trans_id),
        K(msg_type),
        K(status),
        K(task_type),
        K(addr),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_ISNULL(task = TransRpcTaskFactory::alloc())) {
      TRANS_LOG(ERROR, "alloc trans rpc task error", KP(task));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(task->init(partition, trans_id, msg_type, status, task_type, addr, sql_no, request_id))) {
        TRANS_LOG(
            WARN, "TransRpcTask init error", K(partition), K(trans_id), K(msg_type), "rcode", status, K(task_type));
      } else if (OB_FAIL(trans_service->push(task))) {
        TRANS_LOG(WARN, "transaction service push task errro", KR(ret), "rpc_task", *task);
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        TransRpcTaskFactory::release(task);
        task = NULL;
      }
    }
  }

  return ret;
}

}  // namespace transaction

namespace obrpc {
OB_SERIALIZE_MEMBER(ObTransRpcResult, status_, send_timestamp_);

void ObTransRpcResult::reset()
{
  status_ = OB_SUCCESS;
  send_timestamp_ = 0L;
}

void ObTransRpcResult::init(const int status, const int64_t timestamp)
{
  status_ = status;
  send_timestamp_ = timestamp;
}

int ObTransP::process()
{
  int ret = OB_SUCCESS;
  ObTransService* trans_service = partition_service_->get_trans_service();
  static const int64_t STATISTICS_INTERVAL_US = 10000000;
  static RLOCAL(int64_t, total_rt);
  static RLOCAL(int64_t, total_process);
  const int64_t run_ts = get_run_timestamp();

  if (OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN, "get transaction service error", KP(trans_service));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = trans_service->handle_trans_msg(arg_, result_);
    const int64_t cur_ts = ObTimeUtility::current_time();
    total_rt = total_rt + (cur_ts - run_ts);
    total_process++;
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "handle transaction message error", KR(ret), "msg", arg_);
    }
  }
  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
    TRANS_LOG(INFO,
        "transaction rpc statistics",
        "total_rt",
        (int64_t)total_rt,
        "total_process",
        (int64_t)total_process,
        "avg_rt",
        total_rt / (total_process + 1));
    total_rt = 0;
    total_process = 0;
  }

  // always return OB_SUCCESS, so rpc can send result_ back
  return OB_SUCCESS;
}

int ObTransRespP::process()
{
  int ret = OB_SUCCESS;
  ObTransService* trans_service = partition_service_->get_trans_service();
  if (OB_ISNULL(trans_service)) {
    TRANS_LOG(WARN, "get transaction service error", KP(trans_service));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCCESS != (ret = trans_service->handle_trans_response(arg_))) {
    // TRANS_LOG(WARN, "handle trans response error", KR(ret), "msg", arg_);
  } else {
    TRANS_LOG(DEBUG, "handle trans response message success", "msg", arg_);
  }

  return ret;
}

}  // namespace obrpc

namespace transaction {
int ObTransRpc::init(
    ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const ObAddr& self, obrpc::ObBatchRpc* rpc)
{
  UNUSED(rpc);
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransRpc inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(rpc_proxy) || OB_ISNULL(trans_service) || !self.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", KP(rpc_proxy), KP(trans_service), K(self));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = trans_cb_.init(trans_service))) {
    TRANS_LOG(WARN, "transaction callback init error", KR(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    trans_service_ = trans_service;
    self_ = self;
    last_stat_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
    TRANS_LOG(INFO, "transaction rpc inited success");
  }
  return ret;
}

int ObTransRpc::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransRpc is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransRpc is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTransRpc start success");
  }

  return ret;
}

void ObTransRpc::stop()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransRpc is not inited");
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTransRpc already has been stopped");
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObTransRpc stop success");
  }
}

void ObTransRpc::wait()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransRpc is not inited");
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransRpc is already running");
  } else {
    TRANS_LOG(INFO, "ObTransRpc wait success");
  }
}

void ObTransRpc::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
    rpc_proxy_ = NULL;
    trans_service_ = NULL;
    self_.reset();
    TRANS_LOG(INFO, "transaction rpc destroyed");
  }
}

int ObTransRpc::post_trans_msg(
    const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
#ifdef TRANS_ERROR
  const int64_t random = ObRandom::rand(1, 100);
  if (0 == random % 20) {
    // mock package drop: 5%
    TRANS_LOG(INFO, "post trans msg failed for random error (discard msg)", K(tenant_id), K(server), K(msg));
    return ret;
  } else if (0 == random % 50) {
    usleep(100000);
    TRANS_LOG(INFO, "post trans msg failed for random error (delayed msg)", K(tenant_id), K(server), K(msg));
  } else {
    // do nothing
  }
#endif

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransRpc not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransRpc is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!server.is_valid()) ||
             OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(server), K(msg), K(msg_type));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rpc_proxy_->to(server)
                         .by(tenant_id)
                         .timeout(GCONF._ob_trans_rpc_timeout)
                         .post_trans_msg(msg, &trans_cb_))) {
    TRANS_LOG(WARN, "post transaction message error", KR(ret), K(msg));
  } else {
    total_trans_msg_count_++;
    statistics_();
    TRANS_LOG(DEBUG, "post transaction message success", K(msg));
  }

  return ret;
}

int ObTransRpc::post_trans_msg(
    const uint64_t tenant_id, const ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type)
{
  UNUSED(tenant_id);
  UNUSED(server);
  UNUSED(msg);
  UNUSED(msg_type);
  return OB_NOT_SUPPORTED;
}

int ObTransRpc::post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  TransRpcTask* task = NULL;
#ifdef TRANS_ERROR
  const int64_t random = ObRandom::rand(1, 100);
  if (0 == random % 20) {
    // mock package drop: 5%
    TRANS_LOG(INFO, "post trans msg failed for random error (discard msg)", K(tenant_id), K(server), K(msg));
    return ret;
  } else if (0 == random % 50) {
    TRANS_LOG(INFO, "post trans msg failed for random error (delayed msg)", K(tenant_id), K(server), K(msg));
  } else {
    // do nothing
  }
#endif

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransRpc not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransRpc is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!server.is_valid()) ||
             OB_UNLIKELY(!msg.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), "sender", server, K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else if (self_ == server) {
    if (NULL == (task = TransRpcTaskFactory::alloc())) {
      TRANS_LOG(ERROR, "alloc trans rpc task error", KP(task));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(task->init(msg, ObTransRetryTaskType::ERROR_MSG_TASK))) {
        TRANS_LOG(WARN, "TransRpcTask init error", K(msg));
      } else if (OB_FAIL(trans_service_->push(task))) {
        TRANS_LOG(WARN, "transaction service push task errro", KR(ret), "rpc_task", *task);
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        TransRpcTaskFactory::release(task);
        task = NULL;
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_trans_resp_msg(msg, NULL))) {
    TRANS_LOG(WARN, "post transaction response message error", KR(ret), K(msg));
  } else {
    total_trans_resp_msg_count_++;
    TRANS_LOG(DEBUG, "post transaction response message success", K(msg));
  }

  return ret;
}

void ObTransRpc::statistics_()
{
  const int64_t cur_ts = ObTimeUtility::current_time();
  if (cur_ts - last_stat_ts_ > STAT_INTERVAL) {
    TRANS_LOG(INFO, "rpc statistics", K_(total_trans_msg_count), K_(total_trans_resp_msg_count));
    total_trans_msg_count_ = 0;
    total_trans_resp_msg_count_ = 0;
    last_stat_ts_ = cur_ts;
  }
}

void TransRpcTask::reset()
{
  trans_id_.reset();
  msg_type_ = OB_TRANS_MSG_UNKNOWN;
  status_ = OB_SUCCESS;
  msg_.reset();
  task_type_ = ObTransRetryTaskType::UNKNOWN;
  addr_.reset();
  request_id_ = OB_INVALID_TIMESTAMP;
  sql_no_ = -1;
  task_timeout_ = INT64_MAX / 2;
  ObTransTask::reset();
}

int TransRpcTask::init(const ObTransMsg& msg, const int64_t task_type)
{
  int ret = OB_SUCCESS;

  if (!msg.is_valid() || !ObTransRetryTaskType::is_valid(task_type)) {
    TRANS_LOG(WARN, "invalid argument", K(msg), K(task_type));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(msg), K(task_type));
  } else {
    msg_ = msg;
  }

  return ret;
}

int TransRpcTask::init(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t msg_type,
    const int64_t request_id, const int64_t task_type, const int64_t sql_no, const int64_t task_timeout)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() || !ObTransMsgTypeChecker::is_valid_msg_type(msg_type) ||
      request_id <= 0 || !ObTransRetryTaskType::is_valid(task_type) || sql_no <= 0 || task_timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(partition),
        K(trans_id),
        K(msg_type),
        K(request_id),
        K(task_type),
        K(sql_no),
        K(task_timeout));
  } else if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(trans_id), K(task_type));
  } else {
    partition_ = partition;
    trans_id_ = trans_id;
    msg_type_ = msg_type;
    request_id_ = request_id;
    sql_no_ = sql_no;
    task_timeout_ = task_timeout;
  }

  return ret;
}

int TransRpcTask::init(const ObPartitionKey& partition, const ObTransID& trans_id, const int64_t msg_type,
    const int status, const int64_t task_type, const ObAddr& addr, const int64_t sql_no, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  // skip verify seq_no
  if (!partition.is_valid() || !trans_id.is_valid() || !ObTransMsgTypeChecker::is_valid_msg_type(msg_type) ||
      !ObTransRetryTaskType::is_valid(task_type) || !addr.is_valid()) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(partition),
        K(trans_id),
        K(msg_type),
        K(status),
        K(task_type),
        K(addr),
        K(sql_no),
        K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(trans_id), K(task_type));
  } else {
    partition_ = partition;
    trans_id_ = trans_id;
    msg_type_ = msg_type;
    status_ = status;
    addr_ = addr;
    sql_no_ = sql_no;
    request_id_ = request_id;
  }

  return ret;
}

int ObTransBatchRpc::init(
    obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const ObAddr& self, obrpc::ObBatchRpc* batch_rpc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trx_rpc_.init(rpc_proxy, trans_service, self, batch_rpc))) {
    TRANS_LOG(WARN, "trx rpc init failed", K(ret));
  } else {
    batch_rpc_ = batch_rpc;
  }
  return ret;
}

int ObTransBatchRpc::start()
{
  return trx_rpc_.start();
}

void ObTransBatchRpc::stop()
{
  trx_rpc_.stop();
}

void ObTransBatchRpc::wait()
{
  trx_rpc_.wait();
}

void ObTransBatchRpc::destroy()
{
  trx_rpc_.destroy();
  batch_rpc_ = NULL;
}

int ObTransBatchRpc::post_trans_msg(
    const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (ObTransMsgTypeChecker::is_2pc_msg_type(msg_type)) {
    const int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    if (OB_FAIL(batch_rpc_->post(
            tenant_id, server, dst_cluster_id, obrpc::TRX_BATCH_REQ_NODELAY, msg_type, msg.get_receiver(), msg))) {
      TRANS_LOG(WARN, "post msg failed", K(ret));
    }
  } else {
    if (OB_FAIL(trx_rpc_.post_trans_msg(tenant_id, server, msg, msg_type))) {
      TRANS_LOG(WARN, "post msg failed", K(ret));
    }
  }
  return ret;
}

int ObTransBatchRpc::post_trans_msg(
    const uint64_t tenant_id, const ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (OB_ISNULL(batch_rpc_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (ObTransMsgType2Checker::is_valid_msg_type(msg_type)) {
    const int64_t new_msg_type = msg_type + OB_TRX_NEW_MSG_TYPE_BASE;
    const int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    if (OB_FAIL(batch_rpc_->post(
            tenant_id, server, dst_cluster_id, obrpc::TRX_BATCH_REQ_NODELAY, new_msg_type, msg.receiver_, msg))) {
      TRANS_LOG(WARN, "post msg failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObTransBatchRpc::post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg)
{
  return trx_rpc_.post_trans_resp_msg(tenant_id, server, msg);
}

}  // namespace transaction

}  // namespace oceanbase
