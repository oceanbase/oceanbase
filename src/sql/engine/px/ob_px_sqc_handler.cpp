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

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase {
using namespace common;
namespace sql {

REGISTER_CREATOR(ObPxSqcHandlerFactory, ObPxSqcHandler, ObPxSqcHandler, 0);

int ObPxWorkNotifier::wait_all_worker_start()
{
  int ret = OB_SUCCESS;
  const int64_t wait_us = 30000;
  uint32_t wait_key = cond_.get_key();
  while (start_worker_count_ != expect_worker_count_) {
    cond_.wait(wait_key, wait_us);
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

void ObPxWorkNotifier::worker_end(bool& all_worker_finish)
{
  all_worker_finish = false;
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

int ObPxSqcHandler::worker_end_hook()
{
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

int ObPxSqcHandler::pre_acquire_px_worker(int64_t& reserved_thread_count)
{
  int ret = OB_SUCCESS;
  int64_t max_thread_count = sqc_init_args_->sqc_.get_max_task_count();
  int64_t min_thread_count = sqc_init_args_->sqc_.get_min_task_count();
  const int64_t rpc_worker_count = 1;
  const bool rpc_worker = sqc_init_args_->sqc_.is_rpc_worker();
  if (rpc_worker) {
    // single dfo scheduling
    reserved_px_thread_count_ = 0;
    if (OB_FAIL(notifier_->set_expect_worker_count(reserved_px_thread_count_))) {
      LOG_WARN("failed to set expect worker count", K(ret), K(reserved_px_thread_count_));
    } else {
      reserved_thread_count = rpc_worker_count;
      sqc_init_args_->sqc_.set_task_count(rpc_worker_count);
    }
  } else {
    // acquire tenant thread for px worker
    ObPxSubAdmission::acquire(max_thread_count, min_thread_count, reserved_px_thread_count_);
    reserved_px_thread_count_ = reserved_px_thread_count_ < min_thread_count ? 0 : reserved_px_thread_count_;
    if (OB_FAIL(notifier_->set_expect_worker_count(reserved_px_thread_count_))) {
      LOG_WARN("failed to set expect worker count", K(ret), K(reserved_px_thread_count_));
    } else {
      sqc_init_args_->sqc_.set_task_count(reserved_px_thread_count_);
      reserved_thread_count = reserved_px_thread_count_;
    }
  }
  if (OB_SUCC(ret)) {
    if (!rpc_worker && reserved_px_thread_count_ < max_thread_count && reserved_px_thread_count_ >= min_thread_count) {
      LOG_INFO("Downgrade px thread allocation",
          K_(reserved_px_thread_count),
          K(max_thread_count),
          K(min_thread_count),
          K(reserved_thread_count),
          K(sqc_init_args_));
    }
    /**
     * sqc handler reference, 1 for rpc thread
     */
    reference_count_ = 1;
    LOG_TRACE("SQC acquire px worker",
        K(max_thread_count),
        K(min_thread_count),
        K(rpc_worker),
        K(reserved_px_thread_count_),
        K(reserved_thread_count));
  }
  return ret;
}

ObPxSqcHandler* ObPxSqcHandler::get_sqc_handler()
{
  ObPxSqcHandler* sqc_handler = nullptr;
  auto sqc_handler_factory = ObPxSqcHandlerTCFactory::get_instance();
  if (OB_ISNULL(sqc_handler_factory)) {
    LOG_ERROR("Failed to get sqc handler factory instance");
  } else if (OB_ISNULL(sqc_handler = sqc_handler_factory->get(0))) {
    LOG_ERROR("Failed to get sqc handler");
  } else {
    LOG_TRACE("SQC Mgr Generate a new sqc handler", K(sqc_handler));
  }
  return sqc_handler;
}

void ObPxSqcHandler::release_handler(ObPxSqcHandler* sqc_handler)
{
  auto sqc_handler_factory = ObPxSqcHandlerTCFactory::get_instance();
  bool all_released = false;
  if (OB_ISNULL(sqc_handler_factory) || OB_ISNULL(sqc_handler)) {
    LOG_ERROR("Get sqc handler factory instance failed", K(sqc_handler_factory), K(sqc_handler));
  } else if (FALSE_IT(sqc_handler->release(all_released))) {
  } else if (all_released) {
    bool rpc_worker = sqc_handler->sqc_init_args_->sqc_.is_rpc_worker();
    IGNORE_RETURN sqc_handler->destroy_sqc();
    LOG_TRACE("SQC Mgr Recycle a sqc handler", K(rpc_worker), K(sqc_handler));
    sqc_handler_factory->put(sqc_handler);
    sqc_handler = nullptr;
  }
}

int ObPxSqcHandler::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = UINT64_MAX;
  reserved_px_thread_count_ = 0;
  void* buf = nullptr;
  observer::ObGlobalContext& gctx = GCTX;
  lib::ContextParam param;
  param.set_mem_attr(lib::current_tenant_id(), ObModIds::OB_SQL_SQC_HANDLER)
      .set_parallel(4)
      .set_properties(lib::ALLOC_THREAD_SAFE);
  ObIAllocator* allocator = nullptr;
  if (OB_FAIL(ROOT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
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
  } else if (FALSE_IT(notifier_ = new (buf) ObPxWorkNotifier())) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObDesExecContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc des execontext", K(ret));
  } else if (FALSE_IT(exec_ctx_ = new (buf) ObDesExecContext(*allocator, gctx.session_mgr_))) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPhysicalPlan)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc physical plan", K(ret));
  } else if (FALSE_IT(des_phy_plan_ = new (buf) ObPhysicalPlan(*mem_context_))) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPxRpcInitSqcArgs)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc sqc init args", K(ret));
  } else if (FALSE_IT(sqc_init_args_ = new (buf) ObPxRpcInitSqcArgs())) {
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObPxSubCoord)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc des px sub coord", K(ret));
  } else if (FALSE_IT(sub_coord_ = new (buf) ObPxSubCoord(gctx, *sqc_init_args_))) {
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

int ObPxSqcHandler::copy_sqc_init_arg(int64_t& pos, const char* data_buf, int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObIAllocator* allocator = nullptr;
  if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sqc handler need to be inited", K(ret));
  } else {
    allocator = &mem_context_->get_arena_allocator();
    sqc_init_args_->set_deserialize_param(*exec_ctx_, *des_phy_plan_, allocator);
    if (OB_FAIL(sqc_init_args_->do_deserialize(pos, data_buf, data_len))) {
      LOG_WARN("Failed to deserialize", K(ret));
    }
    sqc_init_args_->sqc_handler_ = this;
  }
  return ret;
}

int ObPxSqcHandler::init_env()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = nullptr;
  if (OB_ISNULL(sqc_init_args_->exec_ctx_) ||
      (OB_ISNULL(sqc_init_args_->op_root_) && OB_ISNULL(sqc_init_args_->op_spec_root_)) ||
      OB_ISNULL(sqc_init_args_->des_phy_plan_) ||
      OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*sqc_init_args_->exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc args should not be NULL", K(ret));
  } else if (OB_FAIL(sub_coord_->init_exec_env(*sqc_init_args_->exec_ctx_))) {
    LOG_WARN("Failed to init env", K(ret));
  } else if (OB_FAIL(sub_coord_->start_participants(*sqc_init_args_))) {
    LOG_WARN("Failed to start participants", K(ret));
  } else {
    add_flag(OB_SQC_HANDLER_TRAN_STARTED);
  }
  return ret;
}

bool ObPxSqcHandler::need_rollback()
{
  bool bret = (end_ret_ == OB_SUCCESS ? false : true);
  if (OB_NOT_NULL(sub_coord_)) {
    auto& tasks = sub_coord_->get_sqc_ctx().get_tasks();
    for (int64_t i = 0; !bret && i < tasks.count(); ++i) {
      ObPxTask& task = tasks.at(i);
      // TASK_DEFAULT_RET_VALUE means task not start
      // OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER means dtl peer channel not exist
      // OB_GOT_SIGNAL_ABORTING means get a interrupt
      if (OB_SUCCESS != task.get_result() && ObPxTask::TASK_DEFAULT_RET_VALUE != task.get_result() &&
          OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER != task.get_result() &&
          OB_GOT_SIGNAL_ABORTING != task.get_result()) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObPxSqcHandler::destroy_sqc()
{
  int ret = OB_SUCCESS;
  int end_ret = OB_SUCCESS;
  sub_coord_->destroy_first_buffer_cache();
  if (has_flag(OB_SQC_HANDLER_TRAN_STARTED)) {
    if (OB_SUCCESS != (end_ret = sub_coord_->end_participants(*sqc_init_args_, need_rollback()))) {
      LOG_WARN("Failed to end participants", K(end_ret));
    }
  }
  if (OB_SUCCESS == end_ret) {
    end_ret = end_ret_;
  }

  if (has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
    // sqc master report execution complete if sqc-qc channel linked, or QC will wait to timeout.
    ObPxSqcMeta& sqc = sqc_init_args_->sqc_;
    dtl::ObDtlChannelInfo& ci = sqc.get_sqc_channel_info();
    dtl::ObDtlChannel* ch = sqc.get_sqc_channel();
    if (OB_FAIL(sub_coord_->report_sqc_finish(end_ret))) {
      LOG_WARN("fail report sqc to qc", K(ret));
    }
    LOG_TRACE("sqc send report to qc", K(sqc));
  }
  if (has_flag(OB_SQC_HANDLER_QC_SQC_LINKED)) {
    ObPxSqcMeta& sqc = sqc_init_args_->sqc_;
    dtl::ObDtlChannelInfo& ci = sqc.get_sqc_channel_info();
    dtl::ObDtlChannel* ch = sqc.get_sqc_channel();
    if (OB_NOT_NULL(ch) && OB_FAIL(dtl::ObDtlChannelGroup::unlink_channel(ci))) {
      LOG_WARN("Failed to unlink channel", K(ret));
    }
  }
  return ret;
}

int ObPxSqcHandler::link_qc_sqc_channel()
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta& sqc = sqc_init_args_->sqc_;
  dtl::ObDtlChannelInfo& ci = sqc.get_sqc_channel_info();
  dtl::ObDtlChannel* ch = NULL;
  if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch))) {
    LOG_WARN("Failed to link qc-sqc channel", K(ci), K(ret));
  } else {
    sqc.set_sqc_channel(ch);
    add_flag(OB_SQC_HANDLER_QC_SQC_LINKED);
  }
  return ret;
}

int ObPxSqcHandler::get_partitions_info(ObIArray<ObPxPartitionInfo>& partitions_info)
{
  int ret = OB_SUCCESS;
  ObIArray<const ObTableScan*>& scan_ops = sqc_init_args_->scan_ops_;
  common::ObSEArray<common::ObPartitionArray, 4> partition_keys_array;
  auto part_service = exec_ctx_->get_task_exec_ctx().get_partition_service();
  const bool non_partition_granule = false;
  partitions_info.reset();
  if (!scan_ops.empty() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
    int64_t each_table_part_count =
        (0 == scan_ops.count() ? 0 : sqc_init_args_->sqc_.get_access_table_locations().count() / scan_ops.count());
    int64_t tsc_location_idx = 0;
    if (OB_FAIL(ObPxPartitionLocationUtil::get_all_tables_partitions(scan_ops.count(),
            sqc_init_args_->sqc_.get_access_table_locations(),
            tsc_location_idx,
            partition_keys_array,
            each_table_part_count))) {
      LOG_WARN("Get all table scan's partition failed", K(ret));
    } else if (scan_ops.count() != partition_keys_array.count()) {
      // TODO: partition wise insert will reach here
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected argument", K(ret), K(scan_ops.count()), K(partition_keys_array.count()));
    }

    ARRAY_FOREACH(scan_ops, idx)
    {
      const ObTableScan* tsc = scan_ops.at(idx);
      ObSEArray<ObNewRange, 8> scan_ranges;
      if (is_virtual_table(tsc->get_scan_key_id())) {
        common::ObPartitionArray& partition_array = partition_keys_array.at(idx);
        CK(partition_array.count() == 1);
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_array.count(); ++i) {
          LOG_DEBUG("one virtual partition", K(idx), K(i));
          ObPxPartitionInfo partition_info;
          partition_info.logical_row_count_ = 1;
          partition_info.physical_row_count_ = 1;
          partition_info.partition_key_ = partition_keys_array.at(idx).at(i);
          if (OB_FAIL(partitions_info.push_back(partition_info))) {
            LOG_WARN("Failed to push back partition info", K(ret));
          }
        }
      } else if (OB_FAIL(ObGranuleSplitter::get_query_range(*sqc_init_args_->exec_ctx_,
                     tsc->get_query_range(),
                     scan_ranges,
                     tsc->get_scan_key_id(),
                     non_partition_granule))) {
        LOG_WARN("Failed to get scan range", K(ret));
      } else {
        // init common param
        storage::ObTableScanParam param;
        param.schema_version_ = tsc->get_schema_version();
        param.scan_flag_.flag_ = tsc->get_flags();
        if (OB_FAIL(param.column_ids_.assign(tsc->get_output_column_ids()))) {
          LOG_WARN("Failed to assign column ids", K(ret));
        }
        common::ObPartitionArray& partition_array = partition_keys_array.at(idx);
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_array.count(); ++i) {
          ObSimpleBatch batch;
          SQLScanRange range;
          SQLScanRangeArray range_array;
          obrpc::ObEstPartResElement est_res;
          param.pkey_ = partition_keys_array.at(idx).at(i);
          param.index_id_ = tsc->get_scan_key_id();
          if (OB_FAIL(ObOptEstCost::construct_scan_range_batch(scan_ranges, batch, range, range_array))) {
            LOG_WARN("Failed to construct scan range", K(ret));
          } else if (OB_FAIL(ObOptEstCost::storage_estimate_rowcount(
                         part_service, param, batch, scan_ranges.at(0).get_start_key().get_obj_cnt(), est_res))) {
            LOG_WARN("Failed to calculate result", K(ret));
          } else {
            LOG_DEBUG("one partition", K(est_res), K(i), K(param), K(batch.is_valid()));
            ObPxPartitionInfo partition_info;
            partition_info.logical_row_count_ = est_res.logical_row_count_;
            partition_info.physical_row_count_ = est_res.physical_row_count_;
            partition_info.partition_key_ = partition_keys_array.at(idx).at(i);
            if (OB_FAIL(partitions_info.push_back(partition_info))) {
              LOG_WARN("Failed to push back partition info", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sub_coord_->set_partitions_info(partitions_info))) {
      LOG_WARN("Failed to set partitions info", K(ret));
    }
    LOG_DEBUG("sqc handler get all partition rows info", K(partitions_info));
  }
  return ret;
}

void ObPxSqcHandler::check_interrupt()
{
  if (OB_UNLIKELY(IS_INTERRUPTED())) {
    ObInterruptCode code = GET_INTERRUPT_CODE();
    int ret = code.code_;
    if (OB_NOT_NULL(sqc_init_args_)) {
      LOG_WARN("sqc interrupted", K(ret), K(code), K(sqc_init_args_->sqc_));
      ObPxSqcMeta& sqc = sqc_init_args_->sqc_;
      ObInterruptUtil::interrupt_tasks(sqc, ret);
    }
  }
}

}  // namespace sql
}  // namespace oceanbase
