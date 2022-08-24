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
#include "sql/ob_sql.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_mod_define.h"
#include "common/ob_smart_call.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/table/ob_mv_table_scan.h"
#include "sql/engine/table/ob_row_sample_scan.h"
#include "sql/engine/table/ob_block_sample_scan.h"
#include "sql/engine/table/ob_table_scan_with_checksum.h"
#include "sql/engine/table/ob_table_row_store.h"
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/dml/ob_table_delete.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/dml/ob_table_replace.h"
#include "sql/engine/dml/ob_table_merge.h"
#include "sql/engine/dml/ob_table_update_returning.h"
#include "sql/engine/dml/ob_table_delete_returning.h"
#include "sql/engine/dml/ob_table_insert_returning.h"
#include "sql/engine/dml/ob_multi_table_merge.h"
#include "sql/engine/dml/ob_table_append_local_sort_data.h"
#include "sql/engine/dml/ob_table_append_sstable.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher.h"
#include "sql/engine/dml/ob_table_lock.h"
#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit.h"
#include "sql/engine/px/exchange/ob_px_reduce_transmit.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_px_merge_sort_receive.h"
#include "sql/engine/px/ob_px_fifo_coord.h"
#include "sql/engine/px/ob_px_merge_sort_coord.h"
#include "sql/engine/px/ob_light_granule_iterator.h"
#include "sql/engine/pdml/ob_px_multi_part_insert.h"
#include "sql/engine/pdml/ob_px_multi_part_delete.h"
#include "sql/engine/pdml/ob_px_multi_part_update.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_direct_transmit.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/ob_query_exec_ctx_mgr.h"
#include "lib/profile/ob_perf_event.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "ob_operator.h"
#include "observer/ob_server.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace sql {

int ObOpKitStore::init(ObIAllocator& alloc, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (NULL == (kits_ = static_cast<ObOperatorKit*>(alloc.alloc(size * sizeof(kits_[0]))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    memset(kits_, 0, size * sizeof(kits_[0]));
    size_ = size;
  }
  LOG_TRACE("trace init kit store", K(ret), K(size));
  return ret;
}

void ObOpKitStore::destroy()
{
  if (NULL != kits_) {
    for (int64_t i = 0; i < size_; i++) {
      ObOperatorKit& kit = kits_[i];
      if (NULL != kit.op_) {
        kit.op_->destroy();
      }
      if (NULL != kit.input_) {
        kit.input_->~ObOpInput();
      }
    }
  }
}

ObExecContext::ObExecContext(ObIAllocator& allocator)
    : sche_allocator_(ObModIds::OB_SQL_EXECUTOR, OB_MALLOC_NORMAL_BLOCK_SIZE),
      inner_allocator_(),
      allocator_(allocator),
      phy_op_size_(0),
      phy_op_ctx_store_(NULL),
      phy_op_input_store_(NULL),
      phy_plan_ctx_(NULL),
      expr_op_size_(0),
      expr_op_ctx_store_(NULL),
      scheduler_thread_ctx_(sche_allocator_),
      task_executor_ctx_(*this),
      my_session_(NULL),
      plan_cache_manager_(NULL),
      sql_proxy_(NULL),
      virtual_table_ctx_(),
      session_mgr_(NULL),
      exec_stat_collector_(),
      stmt_factory_(NULL),
      expr_factory_(NULL),
      outline_params_wrapper_(NULL),
      execution_id_(OB_INVALID_ID),
      has_non_trivial_expr_op_ctx_(false),
      sql_ctx_(NULL),
      need_disconnect_(true),
      part_row_map_manager_(),
      need_change_timeout_ret_(false),
      row_id_list_array_(),
      total_row_count_(0),
      is_evolution_(false),
      reusable_interm_result_(false),
      is_async_end_trans_(false),
      trans_state_(),
      gi_task_map_(nullptr),
      gi_restart_(false),
      udf_ctx_mgr_(nullptr),
      output_row_(NULL),
      field_columns_(NULL),
      is_direct_local_plan_(false),
      restart_plan_(false),
      sqc_handler_(nullptr),
      frames_(NULL),
      frame_cnt_(0),
      op_kit_store_(),
      eval_res_mem_(NULL),
      eval_tmp_mem_(NULL),
      eval_ctx_(NULL),
      query_exec_ctx_(nullptr),
      temp_ctx_(),
      gi_pruning_info_(),
      sched_info_(),
      convert_allocator_(nullptr),
      root_op_(NULL),
      pwj_map_(nullptr),
      calc_type_(CALC_NORMAL),
      fixed_id_(OB_INVALID_ID),
      expr_partition_id_(OB_INVALID_ID),
      iters_(256, allocator),
      check_status_times_(0)
{}

ObExecContext::ObExecContext()
    : sche_allocator_(ObModIds::OB_SQL_EXECUTOR, OB_MALLOC_NORMAL_BLOCK_SIZE),
      inner_allocator_(ObModIds::OB_SQL_EXEC_CONTEXT, OB_MALLOC_NORMAL_BLOCK_SIZE * 8),
      allocator_(inner_allocator_),
      phy_op_size_(0),
      phy_op_ctx_store_(NULL),
      phy_op_input_store_(NULL),
      phy_plan_ctx_(NULL),
      expr_op_size_(0),
      expr_op_ctx_store_(NULL),
      scheduler_thread_ctx_(sche_allocator_),
      task_executor_ctx_(*this),
      my_session_(NULL),
      plan_cache_manager_(NULL),
      sql_proxy_(NULL),
      virtual_table_ctx_(),
      session_mgr_(NULL),
      exec_stat_collector_(),
      stmt_factory_(NULL),
      expr_factory_(NULL),
      outline_params_wrapper_(NULL),
      execution_id_(OB_INVALID_ID),
      interrupt_id_(0),
      has_non_trivial_expr_op_ctx_(false),
      sql_ctx_(NULL),
      need_disconnect_(true),
      part_row_map_manager_(),
      need_change_timeout_ret_(false),
      row_id_list_array_(),
      total_row_count_(0),
      is_evolution_(false),
      reusable_interm_result_(false),
      is_async_end_trans_(false),
      trans_state_(),
      gi_task_map_(nullptr),
      gi_restart_(false),
      udf_ctx_mgr_(nullptr),
      output_row_(NULL),
      field_columns_(NULL),
      is_direct_local_plan_(false),
      restart_plan_(false),
      sqc_handler_(nullptr),
      frames_(NULL),
      frame_cnt_(0),
      eval_res_mem_(NULL),
      eval_tmp_mem_(NULL),
      eval_ctx_(NULL),
      query_exec_ctx_(nullptr),
      temp_ctx_(),
      gi_pruning_info_(),
      sched_info_(),
      convert_allocator_(nullptr),
      root_op_(NULL),
      pwj_map_(nullptr),
      calc_type_(CALC_NORMAL),
      fixed_id_(OB_INVALID_ID),
      expr_partition_id_(OB_INVALID_ID),
      iters_(256, allocator_),
      check_status_times_(0)
{}

ObExecContext::~ObExecContext()
{
  row_id_list_array_.reset();
  destroy_eval_ctx();
  reset_op_ctx();
  // For background threads, you need to call destructor
  if (NULL != phy_plan_ctx_ && !THIS_WORKER.has_req_flag()) {
    phy_plan_ctx_->~ObPhysicalPlanCtx();
  }
  phy_plan_ctx_ = NULL;
  // destory gi task info map
  if (OB_NOT_NULL(gi_task_map_)) {
    gi_task_map_->destroy();
    gi_task_map_ = NULL;
  }
  if (OB_NOT_NULL(udf_ctx_mgr_)) {
    udf_ctx_mgr_->~ObUdfCtxMgr();
    udf_ctx_mgr_ = NULL;
  }
  if (OB_NOT_NULL(pwj_map_)) {
    pwj_map_->destroy();
    pwj_map_ = NULL;
  }
  clean_resolve_ctx();
  sqc_handler_ = nullptr;
  if (OB_LIKELY(NULL != convert_allocator_)) {
    DESTROY_CONTEXT(convert_allocator_);
    convert_allocator_ = NULL;
  }
  iters_.reset();
}

void ObExecContext::clean_resolve_ctx()
{
  if (OB_NOT_NULL(expr_factory_)) {
    expr_factory_->~ObRawExprFactory();
    expr_factory_ = nullptr;
  }
  if (OB_NOT_NULL(stmt_factory_)) {
    stmt_factory_->~ObStmtFactory();
    stmt_factory_ = nullptr;
  }
  sql_ctx_ = nullptr;
}

int ObExecContext::push_back_iter(common::ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iters_.push_back(iter))) {
    LOG_WARN("failed to push back iter", K(ret));
  }
  return ret;
}

int ObExecContext::remove_iter(common::ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
    if (iters_.at(i) == iter) {
      if (OB_FAIL(iters_.remove(i))) {
        LOG_WARN("failed to remove iter", K(ret), K(i));
      } else {
        break;
      }
    }
  }
  return ret;
}

uint64_t ObExecContext::get_ser_version() const
{
  return GET_UNIS_CLUSTER_VERSION() < CLUSTER_VERSION_2250 ? SER_VERSION_0 : SER_VERSION_1;
}

void ObExecContext::reset_op_ctx()
{
  // The destruction of exec context is a normal operation,
  // and the calling members are not displayed for members which will not cause memory leaks
  if (phy_op_ctx_store_ != NULL) {
    void** it = phy_op_ctx_store_;
    void** it_end = &phy_op_ctx_store_[phy_op_size_];
    for (; it != it_end; ++it) {
      if (NULL != (*it)) {
        (static_cast<ObPhyOperator::ObPhyOperatorCtx*>(*it))->destroy();
      }
    }
  }
  if (phy_op_input_store_ != NULL) {
    ObIPhyOperatorInput** it = phy_op_input_store_;
    ObIPhyOperatorInput** it_end = &phy_op_input_store_[phy_op_size_];
    for (; it != it_end; ++it) {
      if (NULL != (*it)) {
        (*it)->~ObIPhyOperatorInput();
      }
    }
  }
  reset_expr_op();

  op_kit_store_.destroy();
}

void ObExecContext::reset_op_env()
{
  reset_op_ctx();
  op_kit_store_.reset();
  phy_op_size_ = 0;
  expr_op_size_ = 0;
  gi_restart_ = false;
  restart_plan_ = false;
  output_row_ = NULL;
  field_columns_ = NULL;
  if (OB_NOT_NULL(gi_task_map_)) {
    if (gi_task_map_->created()) {
      gi_task_map_->clear();
    }
  }
  if (OB_NOT_NULL(udf_ctx_mgr_)) {
    udf_ctx_mgr_->reset();
  }
}

int ObExecContext::init_phy_op(const uint64_t phy_op_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(phy_op_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to initialize", K(phy_op_size));
  } else if (OB_UNLIKELY(phy_op_size_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init exec ctx twice", K_(phy_op_size));
  } else if (NULL == my_session_) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info not set", K(ret));
  } else {
    phy_op_size_ = phy_op_size;
    if (my_session_->use_static_typing_engine()) {
      if (OB_FAIL(op_kit_store_.init(allocator_, phy_op_size))) {
        LOG_WARN("init operator kit store failed", K(ret));
      }
    } else {
      int64_t ctx_store_size = static_cast<int64_t>(phy_op_size * sizeof(void*));
      int64_t input_store_size = static_cast<int64_t>(phy_op_size * sizeof(ObIPhyOperatorInput*));
      if (OB_UNLIKELY(NULL == (phy_op_ctx_store_ = static_cast<void**>(allocator_.alloc(ctx_store_size))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc phy_op_ctx_store_ memory", K(ctx_store_size));
      } else if (OB_UNLIKELY(NULL == (phy_op_input_store_ =
                                             static_cast<ObIPhyOperatorInput**>(allocator_.alloc(input_store_size))))) {
        phy_op_ctx_store_ = NULL;
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc phy_op_input_store_ memory", K(input_store_size));
      } else {
        // initialize phy_op_ctx_store_ and phy_op_input_store_
        MEMSET(phy_op_ctx_store_, 0, ctx_store_size);
        MEMSET(phy_op_input_store_, 0, input_store_size);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(gi_task_map_)) {
      // Do nothing.
    } else if (gi_task_map_->created()) {
      // Do nothing. If this map has been created, it means this plan is trying to reopen.
    } else if (OB_FAIL(gi_task_map_->create(
                   PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
                   ObModIds::OB_SQL_PX))) {
      LOG_WARN("create gi task map failed", K(ret));
    }
  }
  return ret;
}

int ObExecContext::init_expr_op(uint64_t expr_op_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr_op_size_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init exec ctx twice", K(ret), K_(expr_op_size));
  } else if (expr_op_size > 0) {
    int64_t ctx_store_size = static_cast<int64_t>(expr_op_size * sizeof(ObExprOperatorCtx*));
    if (OB_ISNULL(expr_op_ctx_store_ = static_cast<ObExprOperatorCtx**>(allocator_.alloc(ctx_store_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc expr_op_ctx_store_ memory", K(ret), K(ctx_store_size));
    } else {
      expr_op_size_ = expr_op_size;
      MEMSET(expr_op_ctx_store_, 0, ctx_store_size);
    }
  }
  return ret;
}

void ObExecContext::reset_expr_op()
{
  if (expr_op_ctx_store_ != NULL) {
    ObExprOperatorCtx** it = expr_op_ctx_store_;
    ObExprOperatorCtx** it_end = &expr_op_ctx_store_[expr_op_size_];
    for (; it != it_end; ++it) {
      if (NULL != (*it)) {
        (*it)->~ObExprOperatorCtx();
      }
    }
    has_non_trivial_expr_op_ctx_ = false;
    expr_op_ctx_store_ = NULL;
    expr_op_size_ = 0;
  }
}

int ObExecContext::init_eval_ctx()
{
  int ret = OB_SUCCESS;
  if (NULL == eval_ctx_) {
    CK(NULL == eval_res_mem_);
    CK(NULL == eval_tmp_mem_);
    CK(NULL != my_session_);

    lib::MemoryContext current_context =
        (query_exec_ctx_ != nullptr ? query_exec_ctx_->get_mem_context() : CURRENT_CONTEXT);
    WITH_CONTEXT(current_context)
    {
      lib::ContextParam param;
      param.set_properties(!use_remote_sql() ? lib::USE_TL_PAGE_OPTIONAL : lib::DEFAULT_PROPERTIES)
          .set_mem_attr(my_session_->get_effective_tenant_id(),
              common::ObModIds::OB_SQL_EXPR_CALC,
              common::ObCtxIds::EXECUTE_CTX_ID)
          .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);

      void* mem = NULL;
      if (OB_ISNULL(mem = allocator_.alloc(sizeof(*eval_ctx_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(eval_res_mem_, param))) {
        LOG_WARN("create memory entity failed", K(ret));
        eval_res_mem_ = NULL;
      } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(eval_tmp_mem_, param))) {
        LOG_WARN("create memory entity failed", K(ret));
        eval_tmp_mem_ = NULL;
      } else {
        eval_ctx_ =
            new (mem) ObEvalCtx(*this, eval_res_mem_->get_arena_allocator(), eval_tmp_mem_->get_arena_allocator());
      }

      if (OB_FAIL(ret)) {
        if (NULL != mem) {
          allocator_.free(mem);
          mem = NULL;
          if (NULL != eval_res_mem_) {
            DESTROY_CONTEXT(eval_res_mem_);
            eval_res_mem_ = NULL;
          }
          if (NULL != eval_tmp_mem_) {
            DESTROY_CONTEXT(eval_tmp_mem_);
            eval_tmp_mem_ = NULL;
          }
        }
      }
    }
  } else {
    // set frames to eval ctx
    eval_ctx_->frames_ = frames_;
  }
  return ret;
}

void ObExecContext::destroy_eval_ctx()
{
  if (NULL != eval_ctx_) {
    eval_ctx_->~ObEvalCtx();
    allocator_.free(eval_ctx_);
    eval_ctx_ = NULL;
  }
  if (NULL != eval_res_mem_) {
    DESTROY_CONTEXT(eval_res_mem_);
    eval_res_mem_ = NULL;
  }
  if (NULL != eval_tmp_mem_) {
    DESTROY_CONTEXT(eval_tmp_mem_);
    eval_tmp_mem_ = NULL;
  }
}

int ObExecContext::set_phy_op_ctx_ptr(uint64_t index, void* phy_op)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index >= phy_op_size_ || NULL == phy_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(phy_op_size), K(phy_op), K(index));
  } else {
    phy_op_ctx_store_[index] = phy_op;
  }
  return ret;
}

void* ObExecContext::get_phy_op_ctx_ptr(uint64_t index) const
{
  void* ret = NULL;

  if (NULL != phy_op_ctx_store_ && index < phy_op_size_) {
    ret = phy_op_ctx_store_[index];
  }
  return ret;
}

ObIAllocator& ObExecContext::get_sche_allocator()
{
  return sche_allocator_;
}

ObIAllocator& ObExecContext::get_allocator()
{
  return allocator_;
}

int ObExecContext::create_phy_op_ctx(uint64_t phy_op_id, int64_t nbyte, const ObPhyOperatorType type, void*& op_ctx)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;

  op_ctx = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == phy_op_id || nbyte <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(phy_op_id), K(nbyte));
  } else if (OB_UNLIKELY(NULL != get_phy_op_ctx_ptr(phy_op_id))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical operator context has been created", K(phy_op_id), K(nbyte));
  } else if (OB_UNLIKELY(NULL == (ptr = allocator_.alloc(nbyte)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(nbyte), "operator type", ob_phy_operator_type_str(type));
  } else if (OB_FAIL(set_phy_op_ctx_ptr(phy_op_id, ptr))) {
    LOG_WARN("set physical operator context to store failed",
        K(ret),
        K(phy_op_id),
        "op type",
        ob_phy_operator_type_str(type));
    if (ptr != NULL) {
      allocator_.free(ptr);
      ptr = NULL;
    }
  } else {
    op_ctx = ptr;
    LOG_DEBUG("succ to create_phy_op_ctx", K(ret), K(phy_op_id), "op type", ob_phy_operator_type_str(type));
  }
  return ret;
}

int ObExecContext::create_expr_op_ctx(uint64_t op_id, int64_t op_ctx_size, void*& op_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op_id >= expr_op_size_ || op_ctx_size <= 0 || OB_ISNULL(expr_op_ctx_store_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_id), K(op_ctx_size), K(expr_op_ctx_store_));
  } else if (OB_UNLIKELY(NULL != get_expr_op_ctx(op_id))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expr operator context has been created", K(op_id));
  } else if (OB_ISNULL(op_ctx = allocator_.alloc(op_ctx_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), K(op_id), K(op_ctx_size));
  } else {
    expr_op_ctx_store_[op_id] = static_cast<ObExprOperatorCtx*>(op_ctx);
    has_non_trivial_expr_op_ctx_ = true;
  }
  return ret;
}

void* ObExecContext::get_expr_op_ctx(uint64_t op_id)
{
  return (OB_LIKELY(op_id < expr_op_size_) && !OB_ISNULL(expr_op_ctx_store_)) ? expr_op_ctx_store_[op_id] : NULL;
}

int ObExecContext::create_phy_op_input(uint64_t phy_op_id, ObPhyOperatorType type, ObIPhyOperatorInput*& op_input)
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input_param = NULL;
  op_input = NULL;
  if (OB_UNLIKELY(phy_op_id >= phy_op_size_) || OB_ISNULL(phy_op_input_store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(phy_op_id), K_(phy_op_size), K_(phy_op_input_store));
  } else if (OB_UNLIKELY(NULL != phy_op_input_store_[phy_op_id])) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical operator context has been created", K(phy_op_id), K_(phy_op_size), K(type));
  } else if (OB_UNLIKELY(NULL == (input_param = alloc_operator_input_by_type(type)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc op input by type", K(phy_op_id), K(type));
  } else {
    input_param->set_deserialize_allocator(&allocator_);
    phy_op_input_store_[phy_op_id] = input_param;
    op_input = input_param;
  }
  return ret;
}

ObIPhyOperatorInput* ObExecContext::get_phy_op_input(uint64_t phy_op_id) const
{
  ObIPhyOperatorInput* input_param = NULL;
  if (phy_op_id < phy_op_size_ && phy_op_input_store_ != NULL) {
    input_param = phy_op_input_store_[phy_op_id];
  }
  return input_param;
}

int ObExecContext::create_physical_plan_ctx()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* local_plan_ctx = NULL;
  if (OB_UNLIKELY(phy_plan_ctx_ != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("phy_plan_ctx_ is not null");
  } else if (OB_UNLIKELY(NULL == (local_plan_ctx = static_cast<ObPhysicalPlanCtx*>(
                                      allocator_.alloc(sizeof(ObPhysicalPlanCtx)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to create physical_plan_ctx");
  } else {
    phy_plan_ctx_ = new (local_plan_ctx) ObPhysicalPlanCtx(allocator_);
    phy_plan_ctx_->set_exec_ctx(this);
  }
  return ret;
}

int ObExecContext::merge_scheduler_info()
{
  int ret = OB_SUCCESS;
  /**
   * merge trans result first, otherwise maybe only part of transaction will be rollbacked
   * when something else failed.
   */
  if (OB_FAIL(merge_final_trans_result())) {
    LOG_WARN("merge final trans result failed", K(ret));
  } else if (OB_FAIL(merge_scheduler_retry_info())) {
    LOG_WARN("fail to merge scheduler retry info", K(ret));
  } else if (OB_FAIL(merge_last_failed_partitions())) {
    LOG_WARN("fail to merge last failed partitions", K(ret));
  }
  return ret;
}

int ObExecContext::merge_final_trans_result()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = get_my_session();
  CK(OB_NOT_NULL(session));
  OZ(session->get_trans_result().merge_result(scheduler_thread_ctx_.get_trans_result()),
      scheduler_thread_ctx_.get_trans_result());
  scheduler_thread_ctx_.clear_trans_result();
  return ret;
}

int ObExecContext::merge_scheduler_retry_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(my_session_));
  } else if (OB_FAIL(
                 my_session_->get_retry_info_for_update().merge(scheduler_thread_ctx_.get_scheduler_retry_info()))) {
    LOG_WARN("fail to merge scheduler thread retry info into main thread retry info",
        K(ret),
        K(my_session_->get_retry_info()),
        K(scheduler_thread_ctx_.get_scheduler_retry_info()));
  }
  return ret;
}

int ObExecContext::merge_last_failed_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_executor_ctx_.merge_last_failed_partitions())) {
    LOG_WARN("fail to merge failed partitions", K(ret));
  }
  return ret;
}

ObStmtFactory* ObExecContext::get_stmt_factory()
{
  if (OB_ISNULL(stmt_factory_)) {
    if (OB_ISNULL(stmt_factory_ = OB_NEWx(ObStmtFactory, (&allocator_), allocator_))) {
      LOG_WARN("fail to create log plan factory", K(stmt_factory_));
    }
  } else {
    // do nothing
  }
  return stmt_factory_;
}

ObRawExprFactory* ObExecContext::get_expr_factory()
{
  if (OB_ISNULL(expr_factory_)) {
    if (OB_ISNULL(expr_factory_ = OB_NEWx(ObRawExprFactory, (&allocator_), allocator_))) {
      LOG_WARN("fail to create log plan factory", K(expr_factory_));
    }
  } else {
    // do nothing
  }
  return expr_factory_;
}

int ObExecContext::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan ctx is null");
  } else if (phy_plan_ctx_->is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret));
  } else if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (my_session_->is_terminate(ret)) {
    LOG_WARN("execution was terminated", K(ret));
  } else if (OB_FAIL(release_table_ref())) {
    LOG_WARN("failed to refresh table on demand", K(ret));
  } else if (IS_INTERRUPTED()) {
    ObInterruptCode& ic = GET_INTERRUPT_CODE();
    ret = ic.code_;
    LOG_WARN("px execution was interrupted", K(ic), K(ret));
  } else {
    if (share::ObWorker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
      ret = OB_KILLED_BY_THROTTLING;
    }
  }
  return ret;
}

int ObExecContext::fast_check_status(const int64_t n)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((check_status_times_++ & n) == n)) {
    ret = check_status();
  }
  return ret;
}

uint64_t ObExecContext::get_min_cluster_version() const
{
  return task_executor_ctx_.get_min_cluster_version();
}

void ObExecContext::set_addr(common::ObAddr addr)
{
  UNUSED(addr);
}

const common::ObAddr& ObExecContext::get_addr() const
{
  return MYADDR;
}

int ObExecContext::get_gi_task_map(GIPrepareTaskMap*& gi_task_map)
{
  int ret = OB_SUCCESS;
  gi_task_map = nullptr;
  if (nullptr == gi_task_map_) {
    void* buf = inner_allocator_.alloc(sizeof(GIPrepareTaskMap));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else if (FALSE_IT(gi_task_map_ = new (buf) GIPrepareTaskMap())) {
    } else if (OB_FAIL(gi_task_map_->create(
                   PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
                   ObModIds::OB_SQL_PX))) {
      LOG_WARN("Failed to create gi task map", K(ret));
    } else {
      gi_task_map = gi_task_map_;
    }
  } else {
    gi_task_map = gi_task_map_;
  }
  return ret;
}

int ObExecContext::get_convert_charset_allocator(ObArenaAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = NULL;
  if (OB_ISNULL(convert_allocator_)) {
    if (OB_ISNULL(my_session_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("session is null", K(ret));
    } else {
      lib::ContextParam param;
      param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
          .set_mem_attr(my_session_->get_effective_tenant_id(),
              common::ObModIds::OB_SQL_EXPR_CALC,
              common::ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(convert_allocator_, param))) {
        SQL_ENG_LOG(WARN, "create entity failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    allocator = &convert_allocator_->get_arena_allocator();
  }

  return ret;
}

int ObExecContext::get_udf_ctx_mgr(ObUdfCtxMgr*& udf_ctx_mgr)
{
  int ret = OB_SUCCESS;
  udf_ctx_mgr = nullptr;
  if (OB_ISNULL(udf_ctx_mgr_)) {
    void* buf = inner_allocator_.alloc(sizeof(ObUdfCtxMgr));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else {
      udf_ctx_mgr_ = new (buf) ObUdfCtxMgr();
      udf_ctx_mgr = udf_ctx_mgr_;
    }
  } else {
    udf_ctx_mgr = udf_ctx_mgr_;
  }
  return ret;
}

int ObExecContext::add_temp_table_interm_result_ids(
    int64_t temp_table_id, int64_t sqc_id, const ObIArray<uint64_t>& ids)
{
  int ret = OB_SUCCESS;
  bool is_existed = false;
  ObIArray<ObSqlTempTableCtx>& temp_ctx = get_temp_table_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < temp_ctx.count(); i++) {
    if (temp_table_id == temp_ctx.at(i).temp_table_id_) {
      for (int64_t j = 0; OB_SUCC(ret) && j < temp_ctx.at(i).temp_table_infos_.count(); j++) {
        ObTempTableSqcInfo& temp_info = temp_ctx.at(i).temp_table_infos_.at(j);
        if (sqc_id == temp_info.sqc_id_) {
          if (OB_FAIL(temp_info.interm_result_ids_.assign(ids))) {
            LOG_WARN("failed to assign to interm resuld ids.", K(ret));
          } else { /*do nothing.*/
          }
        } else { /*do nothing.*/
        }
      }
      is_existed = true;
    }
  }
  return ret;
}

int ObExecContext::init_physical_plan_ctx(const ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  int64_t foreign_key_checks = 0;
  if (OB_ISNULL(phy_plan_ctx_) || OB_ISNULL(my_session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical_plan or ctx is NULL", K_(phy_plan_ctx), K_(my_session), K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(my_session_->get_foreign_key_checks(foreign_key_checks))) {
    LOG_WARN("failed to get foreign_key_checks", K(ret));
  } else {
    int64_t start_time = my_session_->get_query_start_time();
    int64_t plan_timeout = 0;
    const ObQueryHint& query_hint = plan.get_query_hint();
    ObConsistencyLevel consistency = INVALID_CONSISTENCY;
    my_session_->set_cur_phy_plan(const_cast<ObPhysicalPlan*>(&plan));
    if (OB_UNLIKELY(query_hint.query_timeout_ > 0)) {
      plan_timeout = query_hint.query_timeout_;
    } else {
      if (OB_FAIL(my_session_->get_query_timeout(plan_timeout))) {
        LOG_WARN("fail to get query timeout", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!plan.is_remote_plan() || GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250) {
        // From the 2250 version, remote sql will be sent to the remote for execution,
        // and the data will not be touched locally, so there is no need to allocate parameter space
        if (OB_FAIL(phy_plan_ctx_->reserve_param_space(plan.get_param_count()))) {
          LOG_WARN("reserve param space failed", K(ret), K(plan.get_param_count()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (stmt::T_SELECT == plan.get_stmt_type()) {
        if (OB_UNLIKELY(query_hint.read_consistency_ != INVALID_CONSISTENCY)) {
          consistency = query_hint.read_consistency_;
        } else {
          consistency = my_session_->get_consistency_level();
        }
      } else {
        consistency = STRONG;
      }
      phy_plan_ctx_->set_consistency_level(consistency);
      phy_plan_ctx_->set_timeout_timestamp(start_time + plan_timeout);
      phy_plan_ctx_->set_phy_plan(&plan);
      phy_plan_ctx_->set_ignore_stmt(plan.is_ignore());
      phy_plan_ctx_->set_foreign_key_checks(0 != foreign_key_checks);
      phy_plan_ctx_->set_table_row_count_list_capacity(plan.get_access_table_num());
      THIS_WORKER.set_timeout_ts(phy_plan_ctx_->get_timeout_timestamp());
    }
  }
  return ret;
}

int ObExecContext::serialize_operator_input(char*& buf, int64_t buf_len, int64_t& pos, int32_t& real_input_count) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input_param = NULL;
  // only after the serialization of physical operator input, we can know the real input count
  // so skip the length of int32_t to serialize real_input_count and serialize input param first
  pos += serialization::encoded_length_i32(real_input_count);
  if (OB_NOT_NULL(phy_plan_ctx_) && !phy_plan_ctx_->is_new_engine()) {
    if (OB_ISNULL(root_op_)) {
      for (int64_t index = 0; OB_SUCC(ret) && index < phy_op_size_; ++index) {
        if (NULL != (input_param = static_cast<ObIPhyOperatorInput*>(phy_op_input_store_[index])) &&
            input_param->need_serialized()) {
          OB_UNIS_ENCODE(index);                                                 // serialize index
          OB_UNIS_ENCODE(static_cast<int64_t>(input_param->get_phy_op_type()));  // serialize operator type
          OB_UNIS_ENCODE(*input_param);                                          // serialize input parameter
          if (OB_SUCC(ret)) {
            ++real_input_count;
          }
        }
      }
    } else if (OB_FAIL(serialize_operator_input_recursively(root_op_, buf, buf_len, pos, real_input_count, false))) {
      LOG_WARN("fail to serialize operator input recursively", K(ret), K(root_op_));
    }
  }
  return ret;
}

int ObExecContext::serialize_operator_input_recursively(const ObPhyOperator* op, char*& buf, int64_t buf_len,
    int64_t& pos, int32_t& real_input_count, bool is_full_tree) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    /*do nothing*/
  } else {
    ObIPhyOperatorInput* input_param = NULL;
    int64_t index = op->get_id();
    if (NULL != (input_param = static_cast<ObIPhyOperatorInput*>(phy_op_input_store_[index])) &&
        input_param->need_serialized()) {
      OB_UNIS_ENCODE(index);                                                 // serialize index
      OB_UNIS_ENCODE(static_cast<int64_t>(input_param->get_phy_op_type()));  // serialize operator type
      OB_UNIS_ENCODE(*input_param);                                          // serialize input parameter
      if (OB_SUCC(ret)) {
        ++real_input_count;
      }
    }
    if (!is_full_tree && IS_PX_COORD(op->get_type())) {
      is_full_tree = true;
    }
    if (OB_SUCC(ret) && (is_full_tree || !IS_PX_RECEIVE(op->get_type()))) {
      for (int i = 0; i < op->get_child_num() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(SMART_CALL(serialize_operator_input_recursively(
                op->get_child(i), buf, buf_len, pos, real_input_count, is_full_tree)))) {
          LOG_WARN("fail to serialize operator input recursively", K(ret), K(op));
        }
      }
    }
  }
  return ret;
}

int ObExecContext::serialize_operator_input_len_recursively(
    const ObPhyOperator* op, int64_t& len, bool is_full_tree) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    /*do nothing*/
  } else {
    ObIPhyOperatorInput* input_param = NULL;
    int64_t index = op->get_id();
    if (NULL != (input_param = static_cast<ObIPhyOperatorInput*>(phy_op_input_store_[index])) &&
        input_param->need_serialized()) {
      OB_UNIS_ADD_LEN(index);                                                 // serialize index
      OB_UNIS_ADD_LEN(static_cast<int64_t>(input_param->get_phy_op_type()));  // serialize operator type
      OB_UNIS_ADD_LEN(*input_param);                                          // serialize input parameter
    }
    if (!is_full_tree && IS_PX_COORD(op->get_type())) {
      is_full_tree = true;
    }
    if (is_full_tree || !IS_PX_RECEIVE(op->get_type())) {
      for (int i = 0; i < op->get_child_num() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(SMART_CALL(serialize_operator_input_len_recursively(op->get_child(i), len, is_full_tree)))) {
          LOG_WARN("fail to serialize operator input recursively", K(ret), K(op));
        }
      }
    }
  }
  return ret;
}

int ObExecContext::reset_one_row_id_list(const common::ObIArray<int64_t>* row_id_list)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(row_id_list));
  if (OB_SUCC(ret)) {
    row_id_list_array_.reset();
    total_row_count_ = 0;
    OZ(row_id_list_array_.push_back(row_id_list));
    total_row_count_ += row_id_list->count();
  }
  return ret;
}

int ObExecContext::add_row_id_list(const common::ObIArray<int64_t>* row_id_list)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(row_id_list));
  if (OB_SUCC(ret)) {
    OZ(row_id_list_array_.push_back(row_id_list));
    total_row_count_ += row_id_list->count();
  }
  return ret;
}

int ObExecContext::get_pwj_map(PWJPartitionIdMap*& pwj_map)
{
  int ret = OB_SUCCESS;
  pwj_map = nullptr;
  if (nullptr == pwj_map_) {
    void* buf = inner_allocator_.alloc(sizeof(PWJPartitionIdMap));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else if (FALSE_IT(pwj_map_ = new (buf) PWJPartitionIdMap())) {
    } else if (OB_FAIL(pwj_map_->create(
                   PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
                   ObModIds::OB_SQL_PX))) {
      LOG_WARN("Failed to create gi task map", K(ret));
    } else {
      pwj_map = pwj_map_;
    }
  } else {
    pwj_map = pwj_map_;
  }
  return ret;
}

// Currently, there are some limitations
// Only iterator in ITER_END can be released because of memory allocation problem
// iterator in merge sort join cannot work properly,
// because iterator may not go to end
int ObExecContext::release_table_ref()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
    if (OB_FAIL(iters_.at(i)->release_table_ref())) {
      LOG_WARN("failed to release table ref", K(ret), K(i));
    } else {
      LOG_DEBUG("succ to release_table_ref");
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();
  int32_t real_input_count = 0;   // real serialized input param count
  int64_t input_start_pos = pos;  // the position of the starting serialization

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is invalid",
        K_(phy_op_size),
        K_(phy_op_ctx_store),
        K_(phy_op_input_store),
        K_(phy_plan_ctx),
        K_(my_session),
        K_(plan_cache_manager),
        K(ret));
  } else {
    phy_plan_ctx_->set_expr_op_size(expr_op_size_);
    if (ser_version == SER_VERSION_1) {
      OB_UNIS_ENCODE(my_session_->get_login_tenant_id());
    }
    OB_UNIS_ENCODE(phy_op_size_);
    OB_UNIS_ENCODE(*phy_plan_ctx_);
    OB_UNIS_ENCODE(*my_session_);
    input_start_pos = pos;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_operator_input(buf, buf_len, pos, real_input_count))) {
        LOG_WARN("fail serialize operator input", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, input_start_pos, real_input_count))) {
        LOG_WARN("encode int32_t", K(buf_len), K(input_start_pos), K(real_input_count));
      }
    }
    OB_UNIS_ENCODE(task_executor_ctx_);
    //    OB_UNIS_ENCODE(execution_id_);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExecContext)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  LOG_WARN("not supported", K(ret));
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObExecContext)
{
  int64_t len = 0;
  uint64_t ser_version = get_ser_version();
  int32_t real_input_count = 0;
  ObIPhyOperatorInput* input_param = NULL;

  if (is_valid()) {
    phy_plan_ctx_->set_expr_op_size(expr_op_size_);
    if (ser_version == SER_VERSION_1) {
      OB_UNIS_ADD_LEN(my_session_->get_login_tenant_id());
    }
    OB_UNIS_ADD_LEN(phy_op_size_);
    OB_UNIS_ADD_LEN(*phy_plan_ctx_);
    OB_UNIS_ADD_LEN(*my_session_);
    len += serialization::encoded_length_i32(real_input_count);
    if (!phy_plan_ctx_->is_new_engine()) {
      if (OB_ISNULL(root_op_)) {
        for (int64_t index = 0; index < phy_op_size_; ++index) {
          if (NULL != (input_param = phy_op_input_store_[index]) && input_param->need_serialized()) {
            int64_t op_type = static_cast<int64_t>(input_param->get_phy_op_type());
            OB_UNIS_ADD_LEN(index);
            OB_UNIS_ADD_LEN(op_type);
            OB_UNIS_ADD_LEN(*input_param);
          }
        }
      } else {
        serialize_operator_input_len_recursively(root_op_, len, false);
      }
    }
    OB_UNIS_ADD_LEN(task_executor_ctx_);
    //    OB_UNIS_ADD_LEN(execution_id_);
  }
  return len;
}

ObIPhyOperatorInput* ObExecContext::alloc_operator_input_by_type(ObPhyOperatorType type)
{
#define REGISTER_BEGIN \
  if (0) {}
#define REGISTER_PHY_OPERATOR_INPUT(input_class, op_type)                     \
  else if (op_type == type)                                                   \
  {                                                                           \
    void* ptr = NULL;                                                         \
    if (OB_UNLIKELY(NULL == (ptr = allocator_.alloc(sizeof(input_class))))) { \
      LOG_WARN("alloc operator input failed",                                 \
          "input_class_name",                                                 \
          (#input_class),                                                     \
          "operator_type",                                                    \
          ob_phy_operator_type_str(type));                                    \
    } else {                                                                  \
      input_param = new (ptr) input_class();                                  \
    }                                                                         \
  }
#define REGISTER_END                                                       \
  else                                                                     \
  {                                                                        \
    LOG_WARN("invalid operator type", "type", static_cast<int64_t>(type)); \
  }

  ObIPhyOperatorInput* input_param = NULL;
  REGISTER_BEGIN
  //@e.g. if physical operator ObMergeGroupBy has input named ObMergeGrouByInput,
  // register it like this
  // REGISTER_PHY_OPERATOR_INPUT(ObMergeGroupByInput, PHY_MERGE_GROUP_BY)
  REGISTER_PHY_OPERATOR_INPUT(ObDistributedTransmitInput, PHY_DISTRIBUTED_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObDirectTransmitInput, PHY_DIRECT_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObFifoReceiveInput, PHY_FIFO_RECEIVE)
  //  REGISTER_PHY_OPERATOR_INPUT(ObRootReceiveInput, PHY_ROOT_RECEIVE)
  REGISTER_PHY_OPERATOR_INPUT(ObDistributedReceiveInput, PHY_DISTRIBUTED_RECEIVE)
  REGISTER_PHY_OPERATOR_INPUT(ObDirectReceiveInput, PHY_DIRECT_RECEIVE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableScanInput, PHY_TABLE_SCAN)
  REGISTER_PHY_OPERATOR_INPUT(ObTableScanInput, PHY_FAKE_CTE_TABLE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableScanInput, PHY_TABLE_SCAN_WITH_DOMAIN_INDEX)
  REGISTER_PHY_OPERATOR_INPUT(ObMVTableScanInput, PHY_MV_TABLE_SCAN)
  REGISTER_PHY_OPERATOR_INPUT(ObRowSampleScanInput, PHY_ROW_SAMPLE_SCAN)
  REGISTER_PHY_OPERATOR_INPUT(ObBlockSampleScanInput, PHY_BLOCK_SAMPLE_SCAN)
  REGISTER_PHY_OPERATOR_INPUT(ObRootTransmitInput, PHY_ROOT_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObTableUpdateInput, PHY_UPDATE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableDeleteInput, PHY_DELETE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableInsertInput, PHY_INSERT)
  REGISTER_PHY_OPERATOR_INPUT(ObTableReplaceInput, PHY_REPLACE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableInsertUpInput, PHY_INSERT_ON_DUP)
  REGISTER_PHY_OPERATOR_INPUT(ObTableMergeInput, PHY_MERGE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableInsertReturningInput, PHY_INSERT_RETURNING)
  REGISTER_PHY_OPERATOR_INPUT(ObTableDeleteReturningInput, PHY_DELETE_RETURNING)
  REGISTER_PHY_OPERATOR_INPUT(ObTableScanWithChecksumInput, PHY_TABLE_SCAN_WITH_CHECKSUM)
  REGISTER_PHY_OPERATOR_INPUT(ObTableAppendLocalSortDataInput, PHY_APPEND_LOCAL_SORT_DATA)
  REGISTER_PHY_OPERATOR_INPUT(ObTableAppendSSTableInput, PHY_APPEND_SSTABLE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableUpdateReturningInput, PHY_UPDATE_RETURNING)
  // REGISTER_PHY_OPERATOR_INPUT(ObTableReplaceReturningInput, PHY_REPLACE_RETURNING)
  // REGISTER_PHY_OPERATOR_INPUT(ObTableInsertUpReturningInput, PHY_INSERT_ON_DUP_RETURNING)
  REGISTER_PHY_OPERATOR_INPUT(ObGIInput, PHY_GRANULE_ITERATOR)
  REGISTER_PHY_OPERATOR_INPUT(ObDistributedTransmitInput, PHY_DETERMINATE_TASK_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObPxFifoReceiveInput, PHY_PX_FIFO_RECEIVE)
  REGISTER_PHY_OPERATOR_INPUT(ObPxMergeSortReceiveInput, PHY_PX_MERGE_SORT_RECEIVE)
  REGISTER_PHY_OPERATOR_INPUT(ObPxDistTransmitInput, PHY_PX_DIST_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObPxRepartTransmitInput, PHY_PX_REPART_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObPxReduceTransmitInput, PHY_PX_REDUCE_TRANSMIT)
  REGISTER_PHY_OPERATOR_INPUT(ObPxFifoCoordInput, PHY_PX_FIFO_COORD)
  REGISTER_PHY_OPERATOR_INPUT(ObPxMergeSortCoordInput, PHY_PX_MERGE_SORT_COORD)
  REGISTER_PHY_OPERATOR_INPUT(ObMultiPartTableScanInput, PHY_MULTI_PART_TABLE_SCAN)
  REGISTER_PHY_OPERATOR_INPUT(ObTableRowStoreInput, PHY_TABLE_ROW_STORE)
  REGISTER_PHY_OPERATOR_INPUT(ObTCRFetcherInput, PHY_TABLE_CONFLICT_ROW_FETCHER)
  REGISTER_PHY_OPERATOR_INPUT(ObMultiTableMergeInput, PHY_MULTI_TABLE_MERGE)
  REGISTER_PHY_OPERATOR_INPUT(ObLGIInput, PHY_LIGHT_GRANULE_ITERATOR)
  REGISTER_PHY_OPERATOR_INPUT(ObPxMultiPartDeleteInput, PHY_PX_MULTI_PART_DELETE)
  REGISTER_PHY_OPERATOR_INPUT(ObPxMultiPartInsertInput, PHY_PX_MULTI_PART_INSERT)
  REGISTER_PHY_OPERATOR_INPUT(ObPxMultiPartUpdateInput, PHY_PX_MULTI_PART_UPDATE)
  REGISTER_PHY_OPERATOR_INPUT(ObTableLockInput, PHY_LOCK)
  REGISTER_PHY_OPERATOR_INPUT(ObTempTableAccessInput, PHY_TEMP_TABLE_ACCESS)
  REGISTER_PHY_OPERATOR_INPUT(ObTempTableInsertInput, PHY_TEMP_TABLE_INSERT)
  REGISTER_PHY_OPERATOR_INPUT(ObMaterialInput, PHY_MATERIAL)
  REGISTER_PHY_OPERATOR_INPUT(ObExprValuesInput, PHY_EXPR_VALUES)
  REGISTER_END
  return input_param;

#undef REGISTER_END
#undef REGISTER_PHY_OPERATOR_INPUT
#undef REGISTER_BEGIN
}
}  // namespace sql
}  // namespace oceanbase
