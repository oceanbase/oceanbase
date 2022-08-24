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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_rpc_processor.h"
#include "ob_px_sub_coord.h"
#include "ob_px_task_process.h"
#include "ob_px_admission.h"
#include "ob_px_sqc_handler.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "storage/memtable/ob_lock_wait_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObInitSqcP::init()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler* sqc_handler = nullptr;
  if (OB_ISNULL(sqc_handler = ObPxSqcHandler::get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get sqc handler", K(ret));
  } else if (OB_FAIL(sqc_handler->init())) {
    LOG_WARN("Failed to init sqc handler", K(ret));
  } else {
    arg_.sqc_handler_ = sqc_handler;
  }
  return ret;
}

void ObInitSqcP::destroy()
{
  obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_ASYNC_INIT_SQC> >::destroy();
  if (OB_NOT_NULL(arg_.sqc_handler_)) {
    ObPxSqcHandler::release_handler(arg_.sqc_handler_);
  }
}

int ObInitSqcP::process()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive dfo", K_(arg));
  ObPxSqcHandler* sqc_handler = arg_.sqc_handler_;

  if (OB_NOT_NULL(sqc_handler)) {
    ObPxRpcInitSqcArgs& arg = sqc_handler->get_sqc_init_arg();
    SET_INTERRUPTABLE(arg.sqc_.get_interrupt_id().px_interrupt_id_);
    unregister_interrupt_ = true;
  }

  if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler can't be nullptr", K(ret));
  } else if (OB_FAIL(sqc_handler->init_env())) {
    LOG_WARN("Failed to init sqc env", K(ret));
  } else if (OB_FAIL(sqc_handler->pre_acquire_px_worker(result_.reserved_thread_count_))) {
    LOG_WARN("Failed to pre acquire px worker", K(ret));
  } else if (result_.reserved_thread_count_ <= 0) {
    ret = OB_ERR_INSUFFICIENT_PX_WORKER;
    LOG_WARN("Worker thread res not enough", K_(result));
  } else if (OB_FAIL(sqc_handler->get_partitions_info(result_.partitions_info_))) {
    LOG_WARN("Failed to get partition info", K(ret));
  } else if (OB_FAIL(sqc_handler->link_qc_sqc_channel())) {
    LOG_WARN("Failed to link qc sqc channel", K(ret));
  } else {
    /*do nothing*/
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(sqc_handler)) {
    if (unregister_interrupt_) {
      ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
      UNSET_INTERRUPTABLE(arg.sqc_.get_interrupt_id().px_interrupt_id_);
      unregister_interrupt_ = false;
    }
    ObPxSqcHandler::release_handler(sqc_handler);
    arg_.sqc_handler_ = nullptr;
  }

  if (OB_SUCCESS != ret && is_schema_error(ret)) {
    ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
  }
  result_.rc_ = ret;
  // return value by result_.rc_
  return OB_SUCCESS;
}

int ObInitSqcP::startup_normal_sqc(ObPxSqcHandler& sqc_handler)
{
  int ret = OB_SUCCESS;
  int64_t dispatched_worker_count = 0;
  bool all_finish = false;
  ObSQLSessionInfo* session = sqc_handler.get_exec_ctx().get_my_session();
  ObPxSubCoord& sub_coord = sqc_handler.get_sub_coord();
  const int64_t rpc_worker = 1;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObPxRpcInitSqcArgs& arg = sqc_handler.get_sqc_init_arg();
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->set_peer_addr(arg.sqc_.get_qc_addr());
    if (OB_FAIL(session->store_query_string(ObString::make_string("PX SUB COORDINATOR")))) {
      LOG_WARN("store query string to session failed", K(ret));
    } else if (OB_FAIL(sub_coord.pre_process())) {
      LOG_WARN("fail process sqc", K(arg), K(ret));
    } else if (OB_FAIL(sub_coord.try_start_tasks(dispatched_worker_count))) {
      LOG_WARN("Notity all dispatched worker to exit", K(ret), K(dispatched_worker_count));
      sub_coord.notify_dispatched_task_exit(dispatched_worker_count);
      LOG_WARN("All dispatched worker exit", K(ret), K(dispatched_worker_count));
    } else {
      sqc_handler.get_notifier().wait_all_worker_start();
      sqc_handler.check_interrupt();
      sqc_handler.worker_end_hook();
    }
  }
  return ret;
}

int ObInitSqcP::after_process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = nullptr;
  ObPxSqcHandler* sqc_handler = arg_.sqc_handler_;
  bool no_need_startup_normal_sqc = (OB_SUCCESS != result_.rc_);
  if (no_need_startup_normal_sqc) {
  } else if (OB_ISNULL(sqc_handler = arg_.sqc_handler_) || !sqc_handler->valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid sqc handler", K(ret), KPC(sqc_handler));
  } else if (OB_ISNULL(session = sqc_handler->get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Session can't be null", K(ret));
  } else {
    share::CompatModeGuard g(session->get_compatibility_mode() == ORACLE_MODE ? share::ObWorker::CompatMode::ORACLE
                                                                              : share::ObWorker::CompatMode::MYSQL);

    sqc_handler->set_tenant_id(sqc_handler->get_exec_ctx().get_my_session()->get_effective_tenant_id());
    ObPxRpcInitSqcArgs& arg = sqc_handler->get_sqc_init_arg();
    LOG_TRACE(
        "process dfo", K(arg), K(session->get_compatibility_mode()), K(sqc_handler->get_reserved_px_thread_count()));
    ret = startup_normal_sqc(*sqc_handler);
  }

  if (!no_need_startup_normal_sqc) {
    if (unregister_interrupt_) {
      if (OB_ISNULL(sqc_handler = arg_.sqc_handler_)
          || !sqc_handler->valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid sqc handler", K(ret), KPC(sqc_handler));
      } else {
        ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
        UNSET_INTERRUPTABLE(arg.sqc_.get_interrupt_id().px_interrupt_id_);
      }
    }
    if (OB_NOT_NULL(sqc_handler) && OB_SUCCESS == sqc_handler->get_end_ret()) {
      sqc_handler->set_end_ret(ret);
    }
    ObPxSqcHandler::release_handler(sqc_handler);
    arg_.sqc_handler_ = nullptr;
  }

  return ret;
}

// need remov
int ObInitTaskP::init()
{
  return OB_NOT_SUPPORTED;
}

int ObInitTaskP::process()
{
  return OB_NOT_SUPPORTED;
}

int ObInitTaskP::after_process()
{
  return OB_NOT_SUPPORTED;
}

void ObFastInitSqcReportQCMessageCall::operator()(hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry)
{
  UNUSED(entry);
  if (OB_NOT_NULL(sqc_)) {
    sqc_->set_need_report(false);
  }
}

int ObInitFastSqcP::init()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler* sqc_handler = nullptr;
  if (OB_ISNULL(sqc_handler = ObPxSqcHandler::get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get sqc handler", K(ret));
  } else if (OB_FAIL(sqc_handler->init())) {
    LOG_WARN("Failed to init sqc handler", K(ret));
  } else {
    arg_.sqc_handler_ = sqc_handler;
    arg_.sqc_handler_->reset_reference_count();  // reset sqc handler ref count
  }
  return ret;
}

void ObInitFastSqcP::destroy()
{
  obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_FAST_INIT_SQC> >::destroy();
  if (OB_NOT_NULL(arg_.sqc_handler_)) {
    ObPxSqcHandler::release_handler(arg_.sqc_handler_);
  }
}

int ObInitFastSqcP::process()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive dfo", K_(arg));
  ObPxSqcHandler* sqc_handler = arg_.sqc_handler_;
  ObSQLSessionInfo* session = nullptr;
  if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler can't be nullptr", K(ret));
  } else if (OB_FAIL(sqc_handler->init_env())) {
    LOG_WARN("Failed to init sqc env", K(ret));
  } else if (OB_ISNULL(sqc_handler = arg_.sqc_handler_) || !sqc_handler->valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid sqc handler", K(ret), KPC(sqc_handler));
  } else if (OB_ISNULL(session = sqc_handler->get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Session can't be null", K(ret));
  } else if (OB_FAIL(sqc_handler->link_qc_sqc_channel())) {
    LOG_WARN("fail to link qc sqc channel", K(ret));
  } else {
    ObPxRpcInitSqcArgs& arg = sqc_handler->get_sqc_init_arg();
    arg.sqc_.set_task_count(1);
    arg.sqc_.set_rpc_worker(true);
    ObPxInterruptGuard px_int_guard(arg.sqc_.get_interrupt_id().px_interrupt_id_);
    share::CompatModeGuard g(session->get_compatibility_mode() == ORACLE_MODE ? share::ObWorker::CompatMode::ORACLE
                                                                              : share::ObWorker::CompatMode::MYSQL);
    sqc_handler->set_tenant_id(session->get_effective_tenant_id());
    LOG_TRACE(
        "process dfo", K(arg), K(session->get_compatibility_mode()), K(sqc_handler->get_reserved_px_thread_count()));
    if (OB_FAIL(startup_normal_sqc(*sqc_handler))) {
      LOG_WARN("fail to startup normal sqc", K(ret));
    }
  }

  if (OB_SUCCESS != ret && is_schema_error(ret)) {
    ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
  }

  if (OB_NOT_NULL(sqc_handler)) {
    sqc_handler->set_end_ret(ret);
    if (sqc_handler->has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
      ret = OB_SUCCESS;
    }
    sqc_handler->reset_reference_count();
    ObPxSqcHandler::release_handler(sqc_handler);
    arg_.sqc_handler_ = nullptr;
  }
  return ret;
}

int ObInitFastSqcP::startup_normal_sqc(ObPxSqcHandler& sqc_handler)
{
  int ret = OB_SUCCESS;
  int64_t dispatched_worker_count = 0;
  ObSQLSessionInfo* session = sqc_handler.get_exec_ctx().get_my_session();
  ObPxSubCoord& sub_coord = sqc_handler.get_sub_coord();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObPxRpcInitSqcArgs& arg = sqc_handler.get_sqc_init_arg();
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->set_peer_addr(arg.sqc_.get_qc_addr());
    if (OB_FAIL(session->store_query_string(ObString::make_string("PX SUB COORDINATOR")))) {
      LOG_WARN("store query string to session failed", K(ret));
    } else if (OB_FAIL(sub_coord.pre_process())) {
      LOG_WARN("fail process sqc", K(arg), K(ret));
    } else if (OB_FAIL(sub_coord.try_start_tasks(dispatched_worker_count, true))) {
      LOG_WARN("fail to start tasks", K(ret));
    }
  }

  return ret;
}

void ObFastInitSqcCB::on_timeout()
{
  int ret = OB_TIMEOUT;
  ret = deal_with_rpc_timeout_err_safely();
  interrupt_qc(ret);
}

int ObFastInitSqcCB::process()
{
  int ret = rcode_.rcode_;
  if (OB_FAIL(ret)) {
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_ts_ - cur_timestamp > 0) {
      interrupt_qc(ret);
      LOG_WARN("init fast sqc cb async interrupt qc", K_(trace_id), K(addr_), K(timeout_ts_), K(interrupt_id_), K(ret));
    } else {
      LOG_WARN("init fast sqc cb async timeout", K_(trace_id), K(addr_), K(timeout_ts_), K(cur_timestamp), K(ret));
    }
  }
  return ret;
}

int ObFastInitSqcCB::deal_with_rpc_timeout_err_safely()

{
  int ret = OB_SUCCESS;
  ObDealWithRpcTimeoutCall call(addr_, retry_info_, timeout_ts_, trace_id_);
  call.ret_ = OB_TIMEOUT;
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  if (OB_NOT_NULL(manager)) {
    if (OB_FAIL(manager->get_map().atomic_refactored(interrupt_id_, call))) {
      LOG_WARN("fail to deal with rpc timeout call", K(interrupt_id_));
    }
  }
  return call.ret_;
}

void ObFastInitSqcCB::interrupt_qc(int err)
{
  int ret = OB_SUCCESS;
  ObGlobalInterruptManager* manager = ObGlobalInterruptManager::getInstance();
  if (OB_NOT_NULL(manager)) {
    ObFastInitSqcReportQCMessageCall call(sqc_);
    if (OB_FAIL(manager->get_map().atomic_refactored(interrupt_id_, call))) {
      LOG_WARN("fail to set need report", K(interrupt_id_));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObInterruptCode int_code(err, GETTID(), GCTX.self_addr_, "RPC ABORT PX");
      if (OB_SUCCESS != (tmp_ret = manager->interrupt(interrupt_id_, int_code))) {
        LOG_WARN("fail to send interrupt message", K_(trace_id), K(tmp_ret), K(int_code), K(interrupt_id_));
      }
    }
  }
}

void ObDealWithRpcTimeoutCall::deal_with_rpc_timeout_err()
{
  if (OB_TIMEOUT == ret_) {
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_ts_ - cur_timestamp > 100 * 1000) {
      LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
                "change error code to OB_CONNECT_ERROR",
          K(ret_),
          K(timeout_ts_),
          K(cur_timestamp));
      if (NULL != retry_info_) {
        int a_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (a_ret = retry_info_->add_invalid_server_distinctly(addr_)))) {
          LOG_WARN("fail to add invalid server distinctly", K_(trace_id), K(a_ret), K_(addr));
        }
      }
      ret_ = OB_RPC_CONNECT_ERROR;
    } else {
      LOG_DEBUG("rpc return OB_TIMEOUT, and it is actually timeout, "
                "do not change error code",
          K(ret_),
          K(timeout_ts_),
          K(cur_timestamp));
      if (NULL != retry_info_) {
        retry_info_->set_is_rpc_timeout(true);
      }
    }
  }
}

void ObDealWithRpcTimeoutCall::operator()(hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode*>& entry)
{
  UNUSED(entry);
  deal_with_rpc_timeout_err();
}
