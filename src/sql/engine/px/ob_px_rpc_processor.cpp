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
#include "lib/signal/ob_signal_struct.h"
#include "lib/ash/ob_active_session_guard.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "sql/engine/px/ob_px_target_mgr.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "share/detect/ob_detect_callback.h"
#include "share/detect/ob_detect_manager_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObInitSqcP::init()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *sqc_handler = nullptr;
  if (OB_ISNULL(sqc_handler = ObPxSqcHandler::get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sqc handler", K(ret));
  } else if (OB_FAIL(sqc_handler->init())) {
    LOG_WARN("Failed to init sqc handler", K(ret));
    sqc_handler->reset();
    op_reclaim_free(sqc_handler);
  } else {
    arg_.sqc_handler_ = sqc_handler;
    arg_.sqc_handler_->reset_reference_count(); //设置sqc_handler的引用计数为1.
  }
  return ret;
}

void ObInitSqcP::destroy()
{
  obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_ASYNC_INIT_SQC> >::destroy();
  /**
   * 如果经历了after process这个流程，arg_.sqc_handler_会被置为空。
   * 如果这里arg_.sqc_handler_不为空，则意味着，init以后，没有进行
   * after process这个流程，那么就应当由自己来做释放。
   */
  if (OB_NOT_NULL(arg_.sqc_handler_)) {
    int report_ret = OB_SUCCESS;
    ObPxSqcHandler::release_handler(arg_.sqc_handler_, report_ret);
  }
  ObActiveSessionGuard::setup_default_ash();
}

int ObInitSqcP::process()
{
  ObActiveSessionGuard::get_stat().in_px_execution_ = true;
  int ret = OB_SUCCESS;
  LOG_TRACE("receive dfo", K_(arg));
  ObPxSqcHandler *sqc_handler = arg_.sqc_handler_;
  result_.sqc_order_gi_tasks_ = true;
  /**
   * 只要能进process，after process一定会被调用，所以可以用中断覆盖整个
   * SQC的生命周期。
   */
  if (OB_NOT_NULL(sqc_handler)) {
    ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
    if (OB_FAIL(SET_INTERRUPTABLE(arg.sqc_.get_interrupt_id().px_interrupt_id_))) {
      LOG_WARN("sqc failed to SET_INTERRUPTABLE");
    } else {
      unregister_interrupt_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler can't be nullptr", K(ret));
  } else if (OB_FAIL(sqc_handler->init_env())) {
    LOG_WARN("Failed to init sqc env", K(ret));
  } else if (OB_FAIL(sqc_handler->pre_acquire_px_worker(result_.reserved_thread_count_))) {
    LOG_WARN("Failed to pre acquire px worker", K(ret));
  } else if (OB_FAIL(pre_setup_op_input(*sqc_handler))) {
    LOG_WARN("pre setup op input failed", K(ret));
  } else if (OB_FAIL(sqc_handler->thread_count_auto_scaling(result_.reserved_thread_count_))) {
    LOG_WARN("fail to do thread auto scaling", K(ret), K(result_.reserved_thread_count_));
  } else if (result_.reserved_thread_count_ <= 0) {
    ret = OB_ERR_INSUFFICIENT_PX_WORKER;
    LOG_WARN("Worker thread res not enough", K_(result));
  } else if (OB_FAIL(sqc_handler->link_qc_sqc_channel())) {
    LOG_WARN("Failed to link qc sqc channel", K(ret));
  } else if (sqc_handler->get_phy_plan().is_enable_px_fast_reclaim() &&
      OB_FAIL(ObDetectManagerUtils::sqc_register_into_dm(sqc_handler, sqc_handler->get_sqc_init_arg().sqc_))) {
    LOG_WARN("[DM] sqc failed to register_into_dm");
  } else {
    /*do nothing*/
  }

#ifdef ERRSIM
  int ecode = EventTable::EN_PX_SQC_INIT_PROCESS_FAILED;
  if (OB_SUCCESS != ecode && OB_SUCC(ret)) {
    LOG_WARN("match sqc execute errism", K(ecode));
    ret = ecode;
  }
#endif

  if (OB_FAIL(ret) && OB_NOT_NULL(sqc_handler)) {
    if (unregister_interrupt_) {
      ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
      UNSET_INTERRUPTABLE(arg.sqc_.get_interrupt_id().px_interrupt_id_);
      unregister_interrupt_ = false;
    }

    //
    if (is_schema_error(ret)) {
      ObInterruptUtil::update_schema_error_code(&(sqc_handler->get_exec_ctx()), ret);
    }

    int report_ret = OB_SUCCESS;
    // DO NOT use sqc_handler after release_handler!!!
    ObPxSqcHandler::release_handler(sqc_handler, report_ret);
    arg_.sqc_handler_ = nullptr;
  }

  // 非rpc框架的错误内容设置到response消息中
  // rpc框架的错误码在process中返回OB_SUCCESS
  result_.rc_ = ret;
  // 异步逻辑处理接口，总返回OB_SUCCESS，逻辑处理中的错误码
  // 通过result_.rc_返回
  return OB_SUCCESS;
}

int ObInitSqcP::pre_setup_op_input(ObPxSqcHandler &sqc_handler)
{
  int ret = OB_SUCCESS;
  ObPxSubCoord &sub_coord = sqc_handler.get_sub_coord();
  ObExecContext *ctx = sqc_handler.get_sqc_init_arg().exec_ctx_;
  ObOpSpec *root = sqc_handler.get_sqc_init_arg().op_spec_root_;
  ObPxSqcMeta &sqc = sqc_handler.get_sqc_init_arg().sqc_;
  sub_coord.set_is_single_tsc_leaf_dfo(sqc.is_single_tsc_leaf_dfo());
  CK(OB_NOT_NULL(ctx) && OB_NOT_NULL(root));
  if (sqc.is_single_tsc_leaf_dfo() &&
      OB_FAIL(sub_coord.rebuild_sqc_access_table_locations())) {
    LOG_WARN("fail to rebuild sqc access location", K(ret));
  } else if (OB_FAIL(sub_coord.pre_setup_op_input(*ctx, *root, sub_coord.get_sqc_ctx(),
      sqc.get_access_table_locations(),
      sqc.get_access_table_location_keys()))) {
    LOG_WARN("pre_setup_op_input failed", K(ret));
  }
  return ret;
}

int ObInitSqcP::startup_normal_sqc(ObPxSqcHandler &sqc_handler)
{
  int ret = OB_SUCCESS;
  int64_t dispatched_worker_count = 0;
  ObSQLSessionInfo *session = sqc_handler.get_exec_ctx().get_my_session();
  ObPxSubCoord &sub_coord = sqc_handler.get_sub_coord();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObPxRpcInitSqcArgs &arg = sqc_handler.get_sqc_init_arg();
    SQL_INFO_GUARD(arg.sqc_.get_monitoring_info().cur_sql_, session->get_cur_sql_id());
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->set_current_trace_id(ObCurTraceId::get_trace_id());
    session->set_peer_addr(arg.sqc_.get_qc_addr());
    if (OB_FAIL(session->store_query_string(ObString::make_string("PX SUB COORDINATOR")))) {
      LOG_WARN("store query string to session failed", K(ret));
    } else if (OB_FAIL(sub_coord.pre_process())) {
      LOG_WARN("fail process sqc", K(arg), K(ret));
    } else if (OB_FAIL(sub_coord.try_start_tasks(dispatched_worker_count))) {
      /**
       * 启动部分worker失败的时候，我们这里主动将已经启动的worker中断掉。
       * 这个操作是阻塞的，中断成功后，后续直接释放sqc handler。
       */
      LOG_WARN("Notity all dispatched worker to exit", K(ret), K(dispatched_worker_count));
      sub_coord.notify_dispatched_task_exit(dispatched_worker_count);
      LOG_WARN("All dispatched worker exit", K(ret), K(dispatched_worker_count));
    } else {
      sqc_handler.get_notifier().wait_all_worker_start();
      /**
       * 检查中断，如果自己这边已收到中断，传递给各个worker，避免worker掉中断。
       * process流程一旦结束，sqc就可能收到中断，但是此时worker不一定注册了中断，
       * 所以这是sqc需要将中断传递给各个worker。
       */
      sqc_handler.check_interrupt();
      sqc_handler.worker_end_hook();
    }
  }
  return ret;
}

int ObInitSqcP::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  UNUSED(error_code);
  ObSQLSessionInfo *session = nullptr;
  ObPxSqcHandler *sqc_handler = arg_.sqc_handler_;
  bool no_need_startup_normal_sqc = (OB_SUCCESS != result_.rc_);
  if (no_need_startup_normal_sqc) {
    /**
     *  rc_不等于OB_SUCCESS，不再进行sqc的流程，直接在最后面去进行sqc的释放。
     */
  } else if (OB_ISNULL(sqc_handler = arg_.sqc_handler_)
             || !sqc_handler->valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid sqc handler", K(ret), KPC(sqc_handler));
  } else if (OB_ISNULL(session = sqc_handler->get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Session can't be null", K(ret));
  } else {
    lib::CompatModeGuard g(session->get_compatibility_mode() == ORACLE_MODE ?
        lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);

    sqc_handler->set_tenant_id(sqc_handler->get_exec_ctx().get_my_session()->get_effective_tenant_id());
    ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
    /**
     * 根据 arg_ 参数来获取本机线程，并执行 task
     */
    LOG_TRACE("process dfo", K(arg), K(session->get_compatibility_mode()), K(sqc_handler->get_reserved_px_thread_count()));
    ret = startup_normal_sqc(*sqc_handler);
    session->set_session_sleep();
  }

  ObActiveSessionGuard::get_stat().in_px_execution_ = false;
  /**
   * 此处需要清理中断，并把分配的线程数和handler释放.
   * worker正常启动后，此时它的引用计数被更新成了
   * worker数量＋rpc，release_handler会做减掉一个引用计数，最后一个引用计数的人
   * 会真正的对sqc handler进行释放。
   * 最后一个工作线程，需要释放内存;
   */
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
    int report_ret = OB_SUCCESS;
    ObPxSqcHandler::release_handler(sqc_handler, report_ret);
    arg_.sqc_handler_ = nullptr;
  }
  return ret;
}

// 已经未使用了，后续移除
int ObInitTaskP::init()
{
  return OB_NOT_SUPPORTED;
}

int ObInitTaskP::process()
{
  // 根据 arg_ 参数来获取本机线程，并执行 task
  return OB_NOT_SUPPORTED;
}

int ObInitTaskP::after_process(int error_code)
{
  UNUSED(error_code);
  return OB_NOT_SUPPORTED;
}

void ObFastInitSqcReportQCMessageCall::operator()(hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry)
{
  UNUSED(entry);
  if (OB_NOT_NULL(sqc_)) {
    if (sqc_->is_ignore_vtable_error() && err_ != OB_SUCCESS
        && ObVirtualTableErrorWhitelist::should_ignore_vtable_error(err_)) {
      // 当该SQC是虚拟表查询时, 调度RPC失败时需要忽略错误结果.
      // 并mock一个sqc finsh msg发送给正在轮询消息的PX算子
      // 此操作已确认是线程安全的.
      mock_sqc_finish_msg();
    } else {
      sqc_->set_need_report(false);
      if (need_set_not_alive_) {
        sqc_->set_server_not_alive(true);
      }
    }
  }
}

int ObFastInitSqcReportQCMessageCall::mock_sqc_finish_msg()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sqc_)) {
    dtl::ObDtlBasicChannel *ch = reinterpret_cast<dtl::ObDtlBasicChannel *>(
        sqc_->get_qc_channel());
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ch is unexpected", K(ret));
    } else {
      MTL_SWITCH(ch->get_tenant_id()) {
        ObPxFinishSqcResultMsg finish_msg;
        finish_msg.rc_ = err_;
        finish_msg.dfo_id_ = sqc_->get_dfo_id();
        finish_msg.sqc_id_ = sqc_->get_sqc_id();
        dtl::ObDtlMsgHeader header;
        header.nbody_ = static_cast<int32_t>(finish_msg.get_serialize_size());
        header.type_ = static_cast<int16_t>(finish_msg.get_type());
        int64_t need_size = header.get_serialize_size() + finish_msg.get_serialize_size();
        dtl::ObDtlLinkedBuffer *buffer = nullptr;
        if (OB_ISNULL(buffer = ch->alloc_buf(need_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buffer failed", K(ret));
        } else {
          auto buf = buffer->buf();
          auto size = buffer->size();
          auto &pos = buffer->pos();
          buffer->set_data_msg(false);
          buffer->timeout_ts() = timeout_ts_;
          buffer->set_msg_type(dtl::ObDtlMsgType::FINISH_SQC_RESULT);
          const bool inc_recv_buf_cnt = false;
          if (OB_FAIL(common::serialization::encode(buf, size, pos, header))) {
            LOG_WARN("fail to encode buffer", K(ret));
          } else if (OB_FAIL(common::serialization::encode(buf, size, pos, finish_msg))) {
            LOG_WARN("serialize RPC channel message fail", K(ret));
          } else if (FALSE_IT(buffer->size() = pos)) {
          } else if (FALSE_IT(pos = 0)) {
          } else if (FALSE_IT(buffer->tenant_id() = ch->get_tenant_id())) {
          } else if (OB_FAIL(ch->attach(buffer, inc_recv_buf_cnt))) {
            LOG_WARN("fail to feedup buffer", K(ret));
          } else if (FALSE_IT(ch->free_buffer_count())) {
          } else {
            need_interrupt_ = false;
          }
        }
        if (NULL != buffer) {
          ch->free_buffer_count();
        }
      }
    }
  }
  return ret;
}
// ObInitFastSqcP相关函数.
int ObInitFastSqcP::init()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *sqc_handler = nullptr;
 if (OB_ISNULL(sqc_handler = ObPxSqcHandler::get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sqc handler", K(ret));
  } else if (OB_FAIL(sqc_handler->init())) {
    LOG_WARN("Failed to init sqc handler", K(ret));
    sqc_handler->reset();
    op_reclaim_free(sqc_handler);
  } else {
    arg_.sqc_handler_ = sqc_handler;
    arg_.sqc_handler_->reset_reference_count(); //设置sqc_handler的引用计数为1.
  }
  return ret;
}

void ObInitFastSqcP::destroy()
{
  obrpc::ObRpcProcessor<obrpc::ObPxRpcProxy::ObRpc<obrpc::OB_PX_FAST_INIT_SQC> >::destroy();
  /**
   * 如果经历了after process这个流程，arg_.sqc_handler_会被置为空。
   * 如果这里arg_.sqc_handler_不为空，则意味着，init以后，没有进行
   * after process这个流程，那么就应当由自己来做释放。
   */
  if (OB_NOT_NULL(arg_.sqc_handler_)) {
    int report_ret = OB_SUCCESS;
    ObPxSqcHandler::release_handler(arg_.sqc_handler_, report_ret);
  }
  ObActiveSessionGuard::setup_default_ash();
}

int ObInitFastSqcP::process()
{
  ObActiveSessionGuard::get_stat().in_sql_execution_ = true;
  int ret = OB_SUCCESS;
  LOG_TRACE("receive dfo", K_(arg));
  ObPxSqcHandler *sqc_handler = arg_.sqc_handler_;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler can't be nullptr", K(ret));
  } else if (OB_FAIL(sqc_handler->init_env())) {
    LOG_WARN("Failed to init sqc env", K(ret));
  } else if (OB_ISNULL(sqc_handler = arg_.sqc_handler_)
             || !sqc_handler->valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid sqc handler", K(ret), KPC(sqc_handler));
  } else if (OB_FAIL(OB_E(EventTable::EN_PX_SQC_EXECUTE_FAILED) OB_SUCCESS)) {
    LOG_WARN("match sqc execute errism", K(ret));
  } else if (OB_ISNULL(session = sqc_handler->get_exec_ctx().get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Session can't be null", K(ret));
  } else if (OB_FAIL(sqc_handler->link_qc_sqc_channel())) {
    LOG_WARN("fail to link qc sqc channel", K(ret));
  } else if (sqc_handler->get_phy_plan().is_enable_px_fast_reclaim() &&
      OB_FAIL(ObDetectManagerUtils::sqc_register_into_dm(sqc_handler, sqc_handler->get_sqc_init_arg().sqc_))) {
    LOG_WARN("[DM] sqc failed to register_into_dm");
  } else {
    ObPxRpcInitSqcArgs &arg = sqc_handler->get_sqc_init_arg();
    arg.sqc_.set_task_count(1);
    ObPxInterruptGuard px_int_guard(arg.sqc_.get_interrupt_id().px_interrupt_id_);
    if (OB_FAIL(px_int_guard.get_interrupt_reg_ret())) {
      LOG_WARN("fast sqc failed to SET_INTERRUPTABLE");
    } else {
      lib::CompatModeGuard g(session->get_compatibility_mode() == ORACLE_MODE ?
      lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
      sqc_handler->set_tenant_id(session->get_effective_tenant_id());
      LOG_TRACE("process dfo",
                K(arg),
                K(session->get_compatibility_mode()),
                K(sqc_handler->get_reserved_px_thread_count()));
      if (OB_FAIL(startup_normal_sqc(*sqc_handler))) {
        LOG_WARN("fail to startup normal sqc", K(ret));
      }
    }
  }

  //
  if (OB_SUCCESS != ret && is_schema_error(ret) && OB_NOT_NULL(sqc_handler)) {
    ObInterruptUtil::update_schema_error_code(&(sqc_handler->get_exec_ctx()), ret);
  }

  ObActiveSessionGuard::get_stat().in_sql_execution_ = false;
  if (OB_NOT_NULL(sqc_handler)) {
    // link channel之前或者link过程可能会失败.
    // 如果sqc和qc没有link, 由response将 ret 通知给px.
    // 如果sqc和qc已经link, 由dtl msg report通知px.
    sqc_handler->set_end_ret(ret);
    if (sqc_handler->has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
      ret = OB_SUCCESS;
    }
    sqc_handler->reset_reference_count();
    int report_ret = OB_SUCCESS;
    ObPxSqcHandler::release_handler(sqc_handler, report_ret);
    ret = OB_SUCCESS == ret ? report_ret : ret;
    arg_.sqc_handler_ = nullptr;
  }
  return ret;
}

int ObInitFastSqcP::startup_normal_sqc(ObPxSqcHandler &sqc_handler)
{
  int ret = OB_SUCCESS;
  int64_t dispatched_worker_count = 0;
  ObSQLSessionInfo *session = sqc_handler.get_exec_ctx().get_my_session();
  ObPxSubCoord &sub_coord = sqc_handler.get_sub_coord();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObPxRpcInitSqcArgs &arg = sqc_handler.get_sqc_init_arg();
    SQL_INFO_GUARD(arg.sqc_.get_monitoring_info().cur_sql_, session->get_cur_sql_id());
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

void ObFastInitSqcCB::log_warn_sqc_fail(int ret)
{
  // Do not change the follow log about px_obdiag_sqc_addr, becacue it will use in obdiag tool
  LOG_WARN("init fast sqc cb async interrupt qc", K_(trace_id), K(timeout_ts_), K(interrupt_id_),
           K(ret), "px_obdiag_sqc_addr", addr_);
}

int ObFastInitSqcCB::process()
{
  //
  int ret = rcode_.rcode_;
  if (OB_FAIL(ret)) {
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_ts_ - cur_timestamp > 0) {
      interrupt_qc(ret);
      log_warn_sqc_fail(ret);
    } else {
      LOG_WARN("init fast sqc cb async timeout", K_(trace_id),
               K(addr_), K(timeout_ts_), K(cur_timestamp), K(ret));
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
  ObGlobalInterruptManager *manager = ObGlobalInterruptManager::getInstance();
  if (OB_NOT_NULL(manager)) {
    // if we are sure init_sqc msg is not sent to sqc successfully, we don't have to set sqc not alive.
    bool init_sqc_not_send_out = (get_error() == EASY_TIMEOUT_NOT_SENT_OUT
                                 || get_error() == EASY_DISCONNECT_NOT_SENT_OUT);
    const bool need_set_not_alive = !init_sqc_not_send_out;
    ObFastInitSqcReportQCMessageCall call(sqc_, err, timeout_ts_, need_set_not_alive);
    if (OB_FAIL(manager->get_map().atomic_refactored(interrupt_id_, call))) {
      LOG_WARN("fail to set need report", K(interrupt_id_));
    } else if (!call.need_interrupt_) {
      /* do nothing*/
      LOG_WARN("ignore virtual table error,no need interrupt qc", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObInterruptCode int_code(err,
                               GETTID(),
                               GCTX.self_addr(),
                               "RPC ABORT PX");
      if (OB_SUCCESS != (tmp_ret = manager->interrupt(interrupt_id_, int_code))) {
        LOG_WARN("fail to send interrupt message", K_(trace_id),
          K(tmp_ret), K(int_code), K(interrupt_id_));
      }
    }
  }
}

void ObDealWithRpcTimeoutCall::deal_with_rpc_timeout_err()
{
  if (OB_TIMEOUT == ret_) {
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    // 由于存在时间精度不一致导致的时间差, 这里需要满足大于100ms才认为不是超时.
    // 一个容错的处理.
    if (timeout_ts_ - cur_timestamp > 100 * 1000) {
      LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
                "change error code to OB_CONNECT_ERROR", K(ret_),
                K(timeout_ts_), K(cur_timestamp));
      ret_ = OB_RPC_CONNECT_ERROR;
    } else {
      LOG_DEBUG("rpc return OB_TIMEOUT, and it is actually timeout, "
                "do not change error code", K(ret_),
                K(timeout_ts_), K(cur_timestamp));
      if (NULL != retry_info_) {
        retry_info_->set_is_rpc_timeout(true);
      }
    }
  }
}

void ObDealWithRpcTimeoutCall::operator() (hash::HashMapPair<ObInterruptibleTaskID,
      ObInterruptCheckerNode *> &entry)
{
  UNUSED(entry);
  deal_with_rpc_timeout_err();
}

int ObPxTenantTargetMonitorP::init()
{
  return OB_SUCCESS;
}

void ObPxTenantTargetMonitorP::destroy()
{

}

// leader 接收各个 follower 的资源汇报，并将 leader 看到的最新视图作为结果返回给 follower
int ObPxTenantTargetMonitorP::process()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("px_target_request", 100000);
  const uint64_t tenant_id = arg_.get_tenant_id();
  const uint64_t follower_version = arg_.get_version();
  // server id of the leader that the follower sync with previously.
  const uint64_t prev_leader_server_id = ObPxTenantTargetMonitor::get_server_id(follower_version);
  const uint64_t leader_server_id  = GCTX.server_id_;
  bool is_leader;
  uint64_t leader_version;
  result_.set_tenant_id(tenant_id);
  if (OB_FAIL(OB_PX_TARGET_MGR.is_leader(tenant_id, is_leader))) {
    LOG_ERROR("get is_leader failed", K(ret), K(tenant_id));
  } else if (!is_leader) {
    result_.set_status(MONITOR_NOT_MASTER);
  } else if (arg_.need_refresh_all_ || prev_leader_server_id != leader_server_id) {
    if (OB_FAIL(OB_PX_TARGET_MGR.reset_leader_statistics(tenant_id))) {
      LOG_ERROR("reset leader statistics failed", K(ret));
    } else if (OB_FAIL(OB_PX_TARGET_MGR.get_version(tenant_id, leader_version))) {
      LOG_WARN("get master_version failed", K(ret), K(tenant_id));
    } else {
      result_.set_status(MONITOR_VERSION_NOT_MATCH);
      result_.set_version(leader_version);
      LOG_INFO("need refresh all", K(tenant_id), K(arg_.need_refresh_all_),
               K(follower_version), K(prev_leader_server_id), K(leader_server_id));
    }
  } else if (OB_FAIL(OB_PX_TARGET_MGR.get_version(tenant_id, leader_version))) {
    LOG_WARN("get master_version failed", K(ret), K(tenant_id));
  } else {
    result_.set_version(leader_version);
    if (follower_version != leader_version) {
      result_.set_status(MONITOR_VERSION_NOT_MATCH);
    } else {
      result_.set_status(MONITOR_READY);
      for (int i = 0; OB_SUCC(ret) && i < arg_.addr_target_array_.count(); i++) {
        ObAddr &server = arg_.addr_target_array_.at(i).addr_;
        int64_t peer_used_inc = arg_.addr_target_array_.at(i).target_;
        if (OB_FAIL(OB_PX_TARGET_MGR.update_peer_target_used(tenant_id, server, peer_used_inc, leader_version))) {
          LOG_WARN("set thread count failed", K(ret), K(tenant_id), K(server), K(peer_used_inc));
        }
      }

      // A simple and rude exception handling, re-statistics
      if (OB_FAIL(ret)) {
        int tem_ret = OB_SUCCESS;
        if ((tem_ret = OB_PX_TARGET_MGR.reset_leader_statistics(tenant_id)) != OB_SUCCESS) {
          LOG_ERROR("reset statistics failed", K(tem_ret), K(tenant_id), K(leader_version));
        } else {
          LOG_INFO("reset statistics succeed", K(tenant_id), K(leader_version));
        }
      } else {
        ObPxGlobalResGather gather(result_);
        if (OB_FAIL(OB_PX_TARGET_MGR.gather_global_target_usage(tenant_id, gather))) {
          LOG_WARN("get global thread count failed", K(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObPxCleanDtlIntermResP::process()
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey key;
  int64_t batch_size = 0 == arg_.batch_size_ ? 1 : arg_.batch_size_;
  for (int64_t i = 0; i < arg_.info_.count(); i++) {
    ObPxCleanDtlIntermResInfo &info = arg_.info_.at(i);
    for (int64_t task_id = 0; task_id < info.task_count_; task_id++) {
      ObPxTaskChSet ch_set;
      if (OB_FAIL(ObDtlChannelUtil::get_receive_dtl_channel_set(info.sqc_id_, task_id,
            info.ch_total_info_, ch_set))) {
        LOG_WARN("get receive dtl channel set failed", K(ret));
      } else {
        LOG_TRACE("ObPxCleanDtlIntermResP process", K(i), K(arg_.batch_size_), K(info), K(task_id), K(ch_set));
        for (int64_t ch_idx = 0; ch_idx < ch_set.count(); ch_idx++) {
          key.channel_id_ = ch_set.get_ch_info_set().at(ch_idx).chid_;
          for (int64_t batch_id = 0; batch_id < batch_size && OB_SUCC(ret); batch_id++) {
            key.batch_id_= batch_id;
            if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager*)->erase_interm_result_info(key))) {
              if (OB_HASH_NOT_EXIST == ret) {
                // interm result is written from batch_id = 0 to batch_size,
                // if some errors happen when batch_id = i, no interm result of batch_id > i will be written.
                // so if erase failed, just break and continue to erase interm result of next channel.
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("fail to release recieve internal result", K(ret), K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
