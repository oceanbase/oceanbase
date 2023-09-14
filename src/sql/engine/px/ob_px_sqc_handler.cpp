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
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/optimizer/ob_storage_estimator.h"
#include "sql/ob_sql_trans_control.h"
#include "storage/tx/ob_trans_service.h"
#include "share/detect/ob_detect_manager_utils.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
using namespace common;
namespace sql
{


int ObPxWorkNotifier::wait_all_worker_start()
{
  int ret = OB_SUCCESS;
  const int64_t wait_us = 30000;
  uint32_t wait_key = cond_.get_key();
  /**
   * 这个信号量可能存在丢信号的情况；
   * 如果丢了信号，则wait一段时间，
   * 如果未丢信号量，则是直接被唤醒。
   */
  bool is_interrupted = false;
  int64_t cnt = 1;
  while (start_worker_count_ != expect_worker_count_ && !is_interrupted) {
    cond_.wait(wait_key, wait_us);
    // check status after at most 1 second.
    if (0 == (cnt++ % 32)) {
      is_interrupted = IS_INTERRUPTED();
    }
  }
  return ret;
}

void ObPxWorkNotifier::worker_start(int64_t tid)
{
  int64_t start_worker_count = ATOMIC_AAF(&start_worker_count_, 1);
  if (start_worker_count == expect_worker_count_) {
    // notify rpc worker to exit
    cond_.signal();
  }
  tid_array_[start_worker_count - 1] = tid;
}

void ObPxWorkNotifier::worker_end(bool &all_worker_finish)
{
  all_worker_finish = false;
  /**
   * 不应在ATOMIC_AAF执行后再访问任何内存，因为堆上的数据可能已经被回收了。
   * 故这里提前做了记录。
   */
  const int64_t rpc_thread = 1;
  int64_t expect_end_count = expect_worker_count_ + rpc_thread;
  int64_t finish_worker_count = ATOMIC_AAF(&finish_worker_count_, 1);
  if (finish_worker_count == expect_end_count) {
    all_worker_finish = true;
  }
}

int ObPxWorkNotifier::set_expect_worker_count(int64_t worker_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tid_array_.prepare_allocate(worker_count))) {
    LOG_WARN("failed to prepare allocate worker", K(ret));
  } else {
    expect_worker_count_ = worker_count;
  }
  return ret;
}

int ObPxSqcHandler::worker_end_hook() {
  int ret = OB_SUCCESS;
  bool all_finish = false;
  notifier_->worker_end(all_finish);
  if (all_finish) {
    LOG_TRACE("all sqc finished, begin sqc end process");
    if (OB_FAIL(sub_coord_->end_process())) {
      LOG_WARN("failed to end sqc", K(ret));
    }
  }
  end_ret_ = ret;
  return ret;
}

int ObPxSqcHandler::pre_acquire_px_worker(int64_t &reserved_thread_count)
{
  int ret = OB_SUCCESS;
  int64_t max_thread_count = sqc_init_args_->sqc_.get_max_task_count();
  int64_t min_thread_count = sqc_init_args_->sqc_.get_min_task_count();
    // 提前在租户中预留线程数，用于 px worker 执行
  ObPxSubAdmission::acquire(max_thread_count, min_thread_count, reserved_px_thread_count_);
  reserved_px_thread_count_ = reserved_px_thread_count_ < min_thread_count ? 0 : reserved_px_thread_count_;
  if (OB_FAIL(notifier_->set_expect_worker_count(reserved_px_thread_count_))) {
    LOG_WARN("failed to set expect worker count", K(ret), K(reserved_px_thread_count_));
  } else {
    sqc_init_args_->sqc_.set_task_count(reserved_px_thread_count_);
    reserved_thread_count = reserved_px_thread_count_;
  }
  if (OB_SUCC(ret)) {
    if (reserved_px_thread_count_ < max_thread_count &&
        reserved_px_thread_count_ >= min_thread_count) {
      LOG_INFO("Downgrade px thread allocation",
              K_(reserved_px_thread_count),
              K(max_thread_count),
              K(min_thread_count),
              K(reserved_thread_count),
              K(sqc_init_args_));
    }
    /**
     * sqc handler的引用计数，1为rpc线程, 此时只有rpc线程引用.
     */
    reference_count_ = 1;
    LOG_TRACE("SQC acquire px worker", K(max_thread_count), K(min_thread_count),
        K(reserved_px_thread_count_), K(reserved_thread_count));
  }
  return ret;
}

ObPxSqcHandler *ObPxSqcHandler::get_sqc_handler()
{
  return op_reclaim_alloc(ObPxSqcHandler);
}

void ObPxSqcHandler::release_handler(ObPxSqcHandler *sqc_handler, int &report_ret)
{
  bool all_released = false;
  if (OB_ISNULL(sqc_handler)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "Get null sqc handler", K(sqc_handler));
  } else if (FALSE_IT(sqc_handler->release(all_released))) {
  } else if (all_released) {
    IGNORE_RETURN sqc_handler->check_rf_leak();
    IGNORE_RETURN sqc_handler->destroy_sqc(report_ret);
    sqc_handler->reset();
    op_reclaim_free(sqc_handler);
  }
}

void ObPxSqcHandler::check_rf_leak()
{
  IGNORE_RETURN sub_coord_->destroy_shared_rf_msgs();
}

int ObPxSqcHandler::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  reserved_px_thread_count_ = 0;
  rpc_level_ = THIS_WORKER.get_curr_request_level();
  void *buf = nullptr;
  observer::ObGlobalContext &gctx = GCTX;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "SqcHandlerParam")
    .set_parallel(4)
    .set_properties(lib::ALLOC_THREAD_SAFE);
  ObIAllocator *allocator = nullptr;
  if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned", K(ret));
  } else {
    allocator = &mem_context_->get_arena_allocator();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPxWorkNotifier)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc px nofitier", K(ret));
  } else if (FALSE_IT(notifier_ = new(buf) ObPxWorkNotifier())) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObDesExecContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc des execontext", K(ret));
  } else if (FALSE_IT(exec_ctx_ = new(buf) ObDesExecContext(*allocator, gctx.session_mgr_))) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPhysicalPlan)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc physical plan", K(ret));
  } else if (FALSE_IT(des_phy_plan_ = new(buf) ObPhysicalPlan(mem_context_))) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPxRpcInitSqcArgs)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc sqc init args", K(ret));
  } else if (FALSE_IT(sqc_init_args_ = new(buf) ObPxRpcInitSqcArgs())) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPxSubCoord)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc des px sub coord", K(ret));
  } else if (FALSE_IT(sub_coord_ = new(buf) ObPxSubCoord(gctx, *sqc_init_args_))) {
  } else {
    exec_ctx_->set_sqc_handler(this);
  }
  return ret;
}

// The factory will invoke this function.
void ObPxSqcHandler::reset()
{
  tenant_id_ = UINT64_MAX;
  reserved_px_thread_count_ = 0;
  process_flags_ = 0;
  end_ret_ = OB_SUCCESS;
  reference_count_ = 1;
  call_dtor(sub_coord_);
  call_dtor(sqc_init_args_);
  call_dtor(des_phy_plan_);
  call_dtor(exec_ctx_);
  call_dtor(notifier_);
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

int ObPxSqcHandler::copy_sqc_init_arg(int64_t &pos, const char *data_buf, int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = nullptr;
  if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler need to be inited", K(ret));
  } else {
    allocator = &mem_context_->get_arena_allocator();
    WITH_CONTEXT(mem_context_) {
      sqc_init_args_->set_deserialize_param(*exec_ctx_, *des_phy_plan_, allocator);
      if (OB_FAIL(sqc_init_args_->do_deserialize(pos, data_buf, data_len))) {
        LOG_WARN("Failed to deserialize", K(ret));
      }
      sqc_init_args_->sqc_handler_ = this;
    }
  }
  return ret;
}

int ObPxSqcHandler::init_env()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = nullptr;
  if (OB_ISNULL(sqc_init_args_->exec_ctx_)
      || OB_ISNULL(sqc_init_args_->op_spec_root_)
      || OB_ISNULL(sqc_init_args_->des_phy_plan_)
      || OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*sqc_init_args_->exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc args should not be NULL", K(ret));
  } else if (OB_FAIL(sub_coord_->init_exec_env(*sqc_init_args_->exec_ctx_))) {
    LOG_WARN("Failed to init env", K(ret));
  } else {
    init_flt_content();
  }
  return ret;
}

void ObPxSqcHandler::init_flt_content()
{
  if (OBTRACE->is_inited()) {
    flt_ctx_.trace_id_ = OBTRACE->get_trace_id();
    flt_ctx_.span_id_ = OBTRACE->get_root_span_id();
    flt_ctx_.policy_ = OBTRACE->get_policy();
  }
  LOG_TRACE("init flt rpc content", K(flt_ctx_), K(OBTRACE->get_trace_id()));
}

bool ObPxSqcHandler::need_rollback()
{
  bool bret = (end_ret_ == OB_SUCCESS ? false : true);
  if (OB_NOT_NULL(sub_coord_)) {
    auto &tasks = sub_coord_->get_sqc_ctx().get_tasks();
    for (int64_t i = 0; !bret && i < tasks.count(); ++i) {
      ObPxTask &task = tasks.at(i);
      // TASK_DEFAULT_RET_VALUE means task not start
      // OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER means dtl peer channel not exist
      // OB_GOT_SIGNAL_ABORTING means get a interrupt
      if (OB_SUCCESS != task.get_result() &&
          ObPxTask::TASK_DEFAULT_RET_VALUE != task.get_result() &&
          OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER != task.get_result() &&
          OB_GOT_SIGNAL_ABORTING != task.get_result()) {
        bret = true;
      }
    }
  }
  return bret;
}

bool ObPxSqcHandler::all_task_success()
{
  bool bret = OB_SUCCESS == end_ret_;
  if (bret && OB_NOT_NULL(sub_coord_)) {
    auto &tasks = sub_coord_->get_sqc_ctx().get_tasks();
    for (int64_t i = 0; bret && i < tasks.count(); ++i) {
      ObPxTask &task = tasks.at(i);
      bret = OB_SUCCESS == task.get_result();
      if (OB_SUCCESS != task.get_result()) {
        LOG_INFO("px task not succ", K(task.get_result()), K(i));
      }
    }
  }
  return bret;
}

int ObPxSqcHandler::destroy_sqc(int &report_ret)
{
  int ret = OB_SUCCESS;
  int end_ret = OB_SUCCESS;
  report_ret = OB_SUCCESS;
  sub_coord_->destroy_first_buffer_cache();
  // end_ret_记录的错误时SQC end process的时候发生的错误，该时刻语句已经执行完成，收尾工作发生了
  // 问题。相比起事务的错误码，收尾的错误码优先级更低。
  end_ret = end_ret_;

  // clean up ddl context if needed
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = sub_coord_->end_ddl(all_task_success()))) {
    LOG_WARN("end ddl failed", K(tmp_ret));
    end_ret = OB_SUCCESS == end_ret ? tmp_ret : end_ret;
  }
  if (OB_NOT_NULL(des_phy_plan_) && des_phy_plan_->is_enable_px_fast_reclaim()) {
    (void) ObDetectManagerUtils::sqc_unregister_check_item_from_dm(
        sqc_init_args_->sqc_.get_px_detectable_ids().qc_detectable_id_, node_sequence_id_);
  }
  if (has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
    /**
     * sqc-qc通道的连接是rpc中process的最后一步，如果link成功就会有这个flag。
     * 那也就是意味着一旦有这个flag，rpc就会正常返回，qc就会认为sqc正常启动了，
     * qc会在自己的信息中记录该sqc正常启动了了。qc现在的结束是同步结束，每一个
     * 被标记启动的sqc都必须在自己正常或者异常结束的时候，报告给qc自己已经结束
     * 了。有任何一个标记启动的sqc不report，qc都会一直等待直到超时。
     */
    if (OB_FAIL(sub_coord_->report_sqc_finish(end_ret))) {
      LOG_WARN("fail report sqc to qc", K(ret), K(end_ret_));
      report_ret = ret;
    }
    if (OB_NOT_NULL(des_phy_plan_) && des_phy_plan_->is_enable_px_fast_reclaim()) {
      (void) ObDetectManagerUtils::sqc_unregister_detectable_id_from_dm(
          sqc_init_args_->sqc_.get_px_detectable_ids().sqc_detectable_id_);
    }
    ObPxSqcMeta &sqc = sqc_init_args_->sqc_;
    LOG_TRACE("sqc send report to qc", K(sqc));
  }
  if (has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
    get_sqc_ctx().sqc_proxy_.destroy();
    ObPxSqcMeta &sqc = sqc_init_args_->sqc_;
    dtl::ObDtlChannelInfo &ci = sqc.get_sqc_channel_info();
    dtl::ObDtlChannel *ch = sqc.get_sqc_channel();
    // 这里的ch是可能为0的，如果是正常的执行，
    // 它会在ObPxSQCProxy的unlink_sqc_qc_channel里面，将ch置为0.
    // 没有走正常收尾流程时则依赖于这里来进行释放.
    if (OB_NOT_NULL(ch) && OB_FAIL(dtl::ObDtlChannelGroup::unlink_channel(ci))) {
      LOG_WARN("Failed to unlink channel", K(ret));
    }
  }
  return ret;
}

int ObPxSqcHandler::link_qc_sqc_channel()
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta &sqc = sqc_init_args_->sqc_;
  dtl::ObDtlChannelInfo &ci = sqc.get_sqc_channel_info();
  dtl::ObDtlChannel *ch = NULL;
  if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch))) {
    LOG_WARN("Failed to link qc-sqc channel", K(ci), K(ret));
  } else {
    ch->set_sqc_owner();
    ch->set_thread_id(GETTID());
    sqc.set_sqc_channel(ch);
    add_flag(OB_SQC_HANDLER_QC_SQC_LINKED);
    OZ(get_sqc_ctx().sqc_proxy_.init());
  }
  return ret;
}

void ObPxSqcHandler::check_interrupt()
{
  if (OB_UNLIKELY(IS_INTERRUPTED())) {
    has_interrupted_ = true;
    // 中断错误处理
    ObInterruptCode code = GET_INTERRUPT_CODE();
    int ret = code.code_;
    if (OB_NOT_NULL(sqc_init_args_)) {
      LOG_WARN("sqc interrupted", K(ret), K(code), K(sqc_init_args_->sqc_));
      ObPxSqcMeta &sqc = sqc_init_args_->sqc_;
      ObInterruptUtil::interrupt_tasks(sqc, ret);
    }
  }
}

int ObPxSqcHandler::thread_count_auto_scaling(int64_t &reserved_px_thread_count)
{
  int ret = OB_SUCCESS;
  /* strategy 1
   * if single table cannot be divided into much ranges,
   * the max thread cnt should be less than ranges cnt.
   */
  int64_t range_cnt = 0;
  ObGranulePump &pump = sub_coord_->get_sqc_ctx().gi_pump_;
  int64_t temp_cnt = reserved_px_thread_count;
  if (reserved_px_thread_count <= 1 || !sqc_init_args_->sqc_.is_single_tsc_leaf_dfo()) {
  } else if (OB_ISNULL(sub_coord_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subcoord is null", K(ret));
  } else {
    ObGranulePump &pump = sub_coord_->get_sqc_ctx().gi_pump_;
    if (OB_FAIL(pump.get_first_tsc_range_cnt(range_cnt))) {
      LOG_WARN("fail to get first tsc range cnt", K(ret));
    } else if (0 == range_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range cnt equal 0", K(ret));
    } else {
      reserved_px_thread_count = min(reserved_px_thread_count, range_cnt);
      reserved_px_thread_count_ = reserved_px_thread_count;
      if (temp_cnt > reserved_px_thread_count) {
        LOG_TRACE("sqc px worker auto-scaling worked", K(temp_cnt), K(range_cnt), K(reserved_px_thread_count));
      }
      if (OB_FAIL(notifier_->set_expect_worker_count(reserved_px_thread_count))) {
        LOG_WARN("failed to set expect worker count", K(ret), K(reserved_px_thread_count_));
      } else {
        sqc_init_args_->sqc_.set_task_count(reserved_px_thread_count);
      }
    }
  }
  return ret;
}

} // sql
} // oceanbase
