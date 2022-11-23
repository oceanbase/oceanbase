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

#include "ob_subplan_filter_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

bool DatumRow::operator==(const DatumRow &other) const
{
  bool cmp = false;
  if (cnt_ != other.cnt_) {
    cmp = false;
  } else {
    for (int64_t i = 0; cmp && i < cnt_; ++i) {
      cmp = ObDatum::binary_equal(elems_[i], other.elems_[i]);
    }
  }
  return cmp;
}

uint64_t DatumRow::hash(uint64_t seed) const
{
  uint64_t hash_val = seed;
  for (int64_t i = 0; i < cnt_; ++i) {
    hash_val = murmurhash(elems_[i].ptr_, elems_[i].len_, hash_val);
  }
  return hash_val;
}

ObSubQueryIterator::ObSubQueryIterator(ObOperator &op)
    : op_(op),
    onetime_plan_(false),
    init_plan_(false),
    inited_(false),
    eval_ctx_(op.get_eval_ctx()),
    iter_brs_(NULL),
    batch_size_(0),
    batch_row_pos_(0),
    iter_end_(false)
{
}

int ObSubQueryIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  bool is_from_store = init_plan_ && inited_;
  if (is_from_store) {
    ret = store_it_.get_next_row(get_output(), op_.get_eval_ctx());
  } else {
    ret = op_.get_next_row();
  }

  return ret;
}

int ObSubQueryIterator::rewind(const bool reset_onetime_plan /* = false */)
{
  //根据subplan filter的语义，reset row iterator，其它的成员保持不变
  int ret = OB_SUCCESS;
  if (onetime_plan_ && !reset_onetime_plan) {
    // for onetime expr
  } else if (init_plan_) {
    // for init plan
    if (OB_FAIL(store_.begin(store_it_))) {
      LOG_WARN("failed to rewind iterator", K(ret));
    }
  } else {
    if (OB_FAIL(op_.rescan())) {
      LOG_WARN("failed to do rescan", K(ret));
    }
  }
  iter_end_ = false;
  //for vectorize mode, SPF iter may have a stored batch to process
  //should reset them in rewind() 
  iter_brs_ = NULL;
  batch_size_ = 0;
  batch_row_pos_ = 0;
  return ret;
}

void ObSubQueryIterator::reuse()
{
  inited_ = false;
  store_it_.reset();
  store_.reset();
  iter_brs_ = NULL;
  batch_size_ = 0;
  batch_row_pos_ = 0;
  iter_end_ = false;
}

//TODO 移到对应的expr， 设置一个标记确保只计算一次
int ObSubQueryIterator::prepare_init_plan()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (!store_.is_inited()) {
      // TODO bin.lb: use auto memory management
      OZ(store_.init(1L << 20,  // 1MB memory limit
                     GET_MY_SESSION(op_.get_exec_ctx())->get_effective_tenant_id()));
      OZ(store_.alloc_dir_id());
    }
    if (op_.is_vectorized()) {
      const ObBatchRows *iter_brs = NULL;
      do {
        int64_t stored_rows_count = 0;
        if (OB_FAIL(op_.get_next_batch(op_.get_spec().max_batch_size_, iter_brs))) {
          LOG_WARN("fail to get next batch", K(ret), K(op_.get_spec().max_batch_size_));
        } else if (OB_FAIL(store_.add_batch(get_output(), op_.get_eval_ctx(), *iter_brs->skip_,
                                            iter_brs->size_, stored_rows_count))) {
          LOG_WARN("fail to add batch", K(ret), K(*iter_brs));
        }
      } while (OB_SUCC(ret) && !iter_brs->end_);
      if (OB_SUCC(ret)) {
        inited_ = true;
        OZ(store_.finish_add_row());
        OZ(store_it_.init(&store_));
      }
    } else {
      while (OB_SUCC(ret) && OB_SUCC(get_next_row())) {
        OZ(store_.add_row(get_output(), &op_.get_eval_ctx()));
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        inited_ = true;
        OZ(store_.finish_add_row());
        OZ(store_it_.init(&store_));
      }
    }
  }
  return ret;
}

int ObSubQueryIterator::init_mem_entity()
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(ObMemAttr(op_.get_exec_ctx().get_my_session()->get_effective_tenant_id(),
        "SqlSQIterator",
        ObCtxIds::WORK_AREA));
  param.set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_entity_, param))) {
    LOG_WARN("fail to create entity", K(ret));
  } else if (OB_ISNULL(mem_entity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create entity ", K(ret));
  }
  return ret;
}

int ObSubQueryIterator::init_probe_row(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  probe_row_.cnt_ = cnt;
  if (OB_ISNULL(probe_row_.elems_ =
      static_cast<ObDatum *>(op_.get_exec_ctx().get_allocator().alloc(cnt * sizeof(ObDatum))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init probe row", K(ret));
  }
  return ret;
}

int ObSubQueryIterator::get_arena_allocator(common::ObIAllocator *&alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_entity_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mem_entity is not init", K(ret));
  } else {
    alloc = &mem_entity_->get_arena_allocator();
  }
  return ret;
}

int ObSubQueryIterator::get_curr_probe_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *pctx = nullptr;
  const ObSubPlanFilterSpec &spec = static_cast<const ObSubPlanFilterSpec &> (parent_->get_spec());
  if (OB_ISNULL(probe_row_.elems_) || probe_row_.cnt_ != spec.exec_param_array_[get_iter_id() - 1/*ignore child 0*/].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("probe_row is not init", K(ret), K(probe_row_.elems_),
                                      K(probe_row_.cnt_), K(get_iter_id()));
  } else if (OB_ISNULL(pctx = op_.get_exec_ctx().get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param store is not init", K(ret));
  } else {
    // put exec param into probe_row
    for (int64_t i = 0; i < probe_row_.cnt_; ++i) {
      probe_row_.elems_[i] = spec.exec_param_array_[get_iter_id() - 1/*ignore child 0*/][i]
                             ->locate_expr_datum(op_.get_eval_ctx());
    }
  }
  return ret;
}

int ObSubQueryIterator::get_refactored(ObDatum &out)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hashmap_.get_refactored(probe_row_, out))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to find in hashmap", K(ret));
    }
  }
  return ret;
}

int ObSubQueryIterator::set_refactored(const DatumRow &row,
                                                       const ObDatum &result,
                                                       const int64_t deep_copy_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hashmap_.set_refactored(row, result))) {
    LOG_WARN("failed to add to hashmap", K(ret));
  } else {
    memory_used_ += deep_copy_size;
  }
  return ret;
}

int ObSubQueryIterator::reset_hash_map()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(mem_entity_)) {
    mem_entity_->reuse();
  }
  memory_used_ = 0;
  if (hashmap_.created() && OB_FAIL(hashmap_.reuse())) {
    LOG_WARN("failed to reuse hash map", K(ret));
  }
  return ret;
}

ObSubPlanFilterSpec::ObSubPlanFilterSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    rescan_params_(alloc),
    onetime_exprs_(alloc),
    init_plan_idxs_(ModulePageAllocator(alloc)),
    one_time_idxs_(ModulePageAllocator(alloc)),
    update_set_(alloc),
    exec_param_array_(alloc),
    exec_param_idxs_inited_(false),
    enable_px_batch_rescans_(alloc),
    enable_das_batch_rescans_(false),
    filter_exprs_(alloc),
    output_exprs_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObSubPlanFilterSpec, ObOpSpec),
                    rescan_params_,
                    onetime_exprs_,
                    init_plan_idxs_,
                    one_time_idxs_,
                    update_set_,
                    exec_param_array_,
                    exec_param_idxs_inited_,
                    enable_px_batch_rescans_,
                    enable_das_batch_rescans_,
                    filter_exprs_,
                    output_exprs_);

DEF_TO_STRING(ObSubPlanFilterSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(rescan_params),
       K_(onetime_exprs),
       K_(init_plan_idxs),
       K_(one_time_idxs),
       K_(update_set),
       K_(exec_param_idxs_inited));
  J_OBJ_END();
  return pos;
}

ObSubPlanFilterOp::ObSubPlanFilterOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    update_set_mem_(NULL),
    iter_end_(false),
    enable_left_px_batch_(false),
    enable_left_das_batch_(false),
    left_rows_(),
    left_rows_iter_(),
    last_store_row_(),
    save_last_row_(false),
    is_left_end_(false),
    batch_rescan_ctl_(),
    cur_params_(),
    cur_param_idxs_(),
    cur_param_expr_idxs_(),
    last_store_row_mem_(NULL)
{
}

ObSubPlanFilterOp::~ObSubPlanFilterOp()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
  destroy_px_batch_rescan_status();
}

void ObSubPlanFilterOp::destroy_subplan_iters()
{
  FOREACH_CNT(it, subplan_iters_) {
    if (NULL != *it) {
      (*it)->~Iterator();
      *it = NULL;
    }
  }
  subplan_iters_.reset();
}

void ObSubPlanFilterOp::destroy_px_batch_rescan_status()
{
  left_rows_iter_.reset();
  last_store_row_.reset();
  left_rows_.reset();
  batch_rescan_ctl_.reset();
  cur_params_.reset();
  cur_param_expr_idxs_.reset();
  cur_param_idxs_.reset();
}

void ObSubPlanFilterOp::destroy()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
  destroy_px_batch_rescan_status();
  ObOperator::destroy();
}



//SPF has its own rescan
int ObSubPlanFilterOp::rescan()
{
  int ret = OB_SUCCESS;
  brs_.end_ = false;
  iter_end_ = false;
  clear_evaluated_flag();
  set_param_null();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to inner rescan", K(ret));
  }

  if (OB_SUCC(ret) && enable_left_px_batch_) {
    left_rows_.reset();
    left_rows_iter_.reset();
    batch_rescan_ctl_.reuse();
    is_left_end_ = false;
    cur_params_.reset();
    cur_param_idxs_.reset();
    cur_param_expr_idxs_.reset();
    save_last_row_ = false;
    last_store_row_.reset();
    brs_holder_.reset();
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("rescan child operator failed", K(ret),
               "op", op_name(), "child", children_[i]->op_name());
    }
  }
  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    Iterator *iter = subplan_iters_.at(i - 1);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan_iter is null", K(ret));
    } else if (MY_SPEC.init_plan_idxs_.has_member(i)) {
      iter->reuse();
      if (OB_FAIL(iter->prepare_init_plan())) {
        LOG_WARN("prepare init plan failed", K(ret), K(i));
      }
    } else if (OB_FAIL(iter->reset_hash_map())) {
      LOG_WARN("failed to reset hash map", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_onetime_exprs())) {
      LOG_WARN("prepare onetime exprs failed", K(ret));
    } else if (OB_FAIL(child_->rescan())) {
      LOG_WARN("failed to do rescan", K(ret));
    } else {
      startup_passed_ = spec_.startup_filters_.empty();
    }
  }
  need_init_before_get_row_ = false;
#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}
//SPF has its own switch iterator

int ObSubPlanFilterOp::fill_cur_row_rescan_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else if (batch_rescan_ctl_.cur_idx_ >= batch_rescan_ctl_.params_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx is unexpected", K(ret),
             K(batch_rescan_ctl_.cur_idx_), K(batch_rescan_ctl_.params_.get_count()));
  } else {
    common::ObIArray<common::ObObjParam>& params =
        batch_rescan_ctl_.params_.get_one_batch_params(batch_rescan_ctl_.cur_idx_);
    int64_t param_cnt = params.count();
    int64_t idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      idx = batch_rescan_ctl_.params_.get_param_idx(i);
      plan_ctx->get_param_store_for_update().at(idx) = params.at(i);
    }
  }
  OZ(prepare_rescan_params(false));
  return ret;
}

int ObSubPlanFilterOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to inner switch iterator", K(ret));
  } else if (OB_FAIL(child_->switch_iterator())) {
    //TODO: 目前只支持对非相关子查询做多组迭代器切换，只切换主表
    if (OB_ITER_END != ret) {
      LOG_WARN("swtich child operator iterator failed", K(ret));
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObSubPlanFilterOp::inner_open()
{
  int ret = OB_SUCCESS;
  CK(child_cnt_ >= 2);
  CK(child_cnt_ == MY_SPEC.enable_px_batch_rescans_.count() ||
     0 == MY_SPEC.enable_px_batch_rescans_.count());
  if (OB_SUCC(ret)) {
    //在subplan filter中，第一个child是对外输出的主表，后面的child都是subquery，
    //subquery的结果需要参与表达式计算，所以为每个subquery生成一个row_iterator
    OZ(subplan_iters_.prepare_allocate(child_cnt_ - 1));
    //TODO 移动到后面
    if (MY_SPEC.exec_param_idxs_inited_ && child_cnt_ - 1 != MY_SPEC.exec_param_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param idx array is unexpected", K(ret), K(MY_SPEC.exec_param_array_.count()));
    }
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      void *ptr = ctx_.get_allocator().alloc(sizeof(Iterator));
      Iterator *&iter = subplan_iters_.at(i - 1);
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc subplan iterator failed", K(ret), "size", sizeof(Iterator));
      } else {
        iter = new(ptr) Iterator(*children_[i]);
        iter->set_iter_id(i);
        iter->set_parent(this);
        if (MY_SPEC.init_plan_idxs_.has_member(i)) {
          iter->set_init_plan();
          //init plan 移到get_next_row之后
        } else if (MY_SPEC.one_time_idxs_.has_member(i)) {
          iter->set_onetime_plan();
        } else if (!MY_SPEC.enable_px_batch_rescans_.empty() &&
            MY_SPEC.enable_px_batch_rescans_.at(i)) {
          enable_left_px_batch_ = true;
        }
        enable_left_das_batch_ = MY_SPEC.enable_das_batch_rescans_;
        if (!MY_SPEC.exec_param_idxs_inited_) {
          //unittest or old version, do not init hashmap
        } else if (OB_FAIL(iter->init_mem_entity())) {
          LOG_WARN("failed to init mem_entity", K(ret));
        } else if (MY_SPEC.exec_param_array_[i - 1].count() > 0) {
          //min of buckets is 16,
          //max will not exceed card of left_child and HASH_MAP_MEMORY_LIMIT/ObObj
          if (OB_FAIL(iter->init_hashmap(max(
                  16/*hard code*/, min(get_child(0)->get_spec().get_rows(),
                      iter->HASH_MAP_MEMORY_LIMIT / static_cast<int64_t>(sizeof(ObDatum))))))) {
            LOG_WARN("failed to init hash map for idx", K(i), K(ret));
          } else if (OB_FAIL(iter->init_probe_row(MY_SPEC.exec_param_array_[i - 1].count()))) {
            LOG_WARN("failed to init probe row", K(ret));
          }
        }
      }
    }
  }
  if (enable_left_px_batch_ && OB_ISNULL(last_store_row_mem_)) {
    ObSQLSessionInfo *session = ctx_.get_my_session();
    uint64_t tenant_id =session->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id,
                       "ObSBFCache",
                       ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(last_store_row_mem_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(last_store_row_mem_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(left_rows_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
      LOG_WARN("init row store failed", K(ret));
    } else {
      left_rows_.set_allocator(last_store_row_mem_->get_malloc_allocator());
    }
  }
  if (OB_SUCC(ret) && is_vectorized()) {
    if (OB_FAIL(brs_holder_.init(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("init brs_holder_ failed", K(ret));
    }
  }
  return ret;
}

int ObSubPlanFilterOp::inner_close()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
  return OB_SUCCESS;
}

int ObSubPlanFilterOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_next_row())) {
      LOG_WARN("fail to get left next row", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!MY_SPEC.update_set_.empty()) {
      OZ(handle_update_set());
    }
  }
  if (OB_ITER_END == ret) {
    set_param_null();
  }
  return ret;
}
int ObSubPlanFilterOp::handle_next_row()
{
  int ret = OB_SUCCESS;
  if (need_init_before_get_row_) {
    OZ(prepare_onetime_exprs());
  }
  if (OB_FAIL(ret)) {
  } else if (enable_left_px_batch_) {
    bool has_row = false;
    int batch_count = PX_RESCAN_BATCH_ROW_COUNT;
    if (left_rows_iter_.is_valid() && left_rows_iter_.has_next()) {
      batch_rescan_ctl_.cur_idx_++;
    } else if (is_left_end_) {
      ret = OB_ITER_END;
    } else {
      batch_rescan_ctl_.reuse();
      left_rows_iter_.reset();
      left_rows_.reset();
      last_store_row_mem_->get_arena_allocator().reset();
      if (OB_ISNULL(last_store_row_.get_store_row())) {
        if (save_last_row_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: store row is null", K(ret));
        } else if (OB_FAIL(last_store_row_.init(
            last_store_row_mem_->get_malloc_allocator(), child_->get_spec().output_.count()))) {
          LOG_WARN("failed to init right last row", K(ret));
        }
      } else if (save_last_row_) {
          // restore expr datum to original value
          if (OB_FAIL(last_store_row_.restore(child_->get_spec().output_, eval_ctx_))) {
          LOG_WARN("failed to restore left row", K(ret));
        }
      }
      save_last_row_ = false;
      set_param_null();
      while (OB_SUCC(ret) && batch_count--) {
        clear_evaluated_flag();
        set_param_null();
        if (OB_FAIL(child_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            is_left_end_ = true;
          }
        } else if (OB_FAIL(left_rows_.add_row(child_->get_spec().output_, &eval_ctx_))) {
          LOG_WARN("fail to add row", K(ret));
        } else if (OB_FAIL(prepare_rescan_params(true))) {
          LOG_WARN("fail to prepare rescan params", K(ret));
        } else {
          has_row = true;
        }
      }
      if (OB_SUCC(ret)) {
        // back expr datum to last_store_row
        if (OB_FAIL(last_store_row_.shadow_copy(child_->get_spec().output_, eval_ctx_))) {
          LOG_WARN("failed to shadow copy last left row", K(ret));
        } else {
          save_last_row_ = true;
        }
      }

      if (OB_SUCC(ret) || (has_row && OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        OZ(left_rows_.finish_add_row(false));
        OZ(left_rows_.begin(left_rows_iter_));
      }
    }
    if (OB_SUCC(ret)) {
      clear_evaluated_flag();
      // fetch datum from left_row_iter_ instead of child operator
      if (OB_FAIL(left_rows_iter_.get_next_row(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("Failed to get next row", K(ret));
      } else {
        OZ(fill_cur_row_rescan_param());
      }
    }
  } else if (FALSE_IT(clear_evaluated_flag())) {
  } else if (FALSE_IT(set_param_null())) {
  } else if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    }
  } else if (OB_FAIL(prepare_rescan_params(false))) {
    LOG_WARN("fail to prepare rescan params", K(ret));
  }

  if (OB_SUCC(ret) && need_init_before_get_row_) {
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      Iterator *&iter = subplan_iters_.at(i - 1);
      if (MY_SPEC.init_plan_idxs_.has_member(i)) {
        OZ(iter->prepare_init_plan());
      }
    }
    need_init_before_get_row_ = false;
  }
  return ret;
}

int ObSubPlanFilterOp::handle_next_batch_with_px_rescan(const int64_t op_max_batch_size)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = NULL;
  bool stop_fetch = false;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  uint64_t left_rows_total_cnt = 0;
  if (left_rows_iter_.is_valid() && left_rows_iter_.has_next()) {
    // fetch data from left store
  } else {
    // 1. material data from child into left_rows_
    // 2. prepare batch rescan params
    batch_rescan_ctl_.reuse();
    left_rows_.reset();
    left_rows_iter_.reset();
    (void) brs_holder_.restore();
    while (OB_SUCC(ret) && continue_fetching(left_rows_total_cnt, stop_fetch)) {
      set_param_null();
      clear_evaluated_flag();
      int64_t store_row_cnt = -1;
      if (OB_FAIL(child_->get_next_batch(op_max_batch_size, child_brs))) {
        LOG_WARN("fail to get next batch", K(ret));
      } else if (OB_FAIL(left_rows_.add_batch(child_->get_spec().output_, eval_ctx_,
                          *child_brs->skip_, child_brs->size_, store_row_cnt))) {
        LOG_WARN("fail to add expr datums to left_rows_", K(ret));
      } else {
        stop_fetch = child_brs->end_;
        left_rows_total_cnt += store_row_cnt;
        guard.set_batch_size(child_brs->size_);
        clear_evaluated_flag();
        // prepare px batch rescan parameter
        for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < child_brs->size_; l_idx++) {
          if (child_brs->skip_->exist(l_idx)) { continue; }
          guard.set_batch_idx(l_idx);
          if (OB_FAIL(prepare_rescan_params(true))) {
            LOG_WARN("prepare rescan params failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!child_brs->end_) {
        // backup child datums into brs_holder_
        OZ(brs_holder_.save(MY_SPEC.max_batch_size_));
      }

      if (OB_FAIL(left_rows_.finish_add_row(false))) {
        LOG_WARN("prepare rescan params failed", K(ret));
      } else if (OB_FAIL(left_rows_.begin(left_rows_iter_))) {
        LOG_WARN("prepare rescan params failed", K(ret));
      }
      if (left_rows_total_cnt != left_rows_.get_row_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left_rows row cnt is unexpectd", K(ret));
      }
    }
  }

  // fetch data from masterized left_rows(ChunkDatumStore) and do filtering
  if (OB_SUCC(ret)) {
    int64_t rows_fetched = 0;
    clear_evaluated_flag();
    if (OB_FAIL(left_rows_iter_.get_next_batch(child_->get_spec().output_,
               eval_ctx_, op_max_batch_size, rows_fetched))) {
      if (OB_ITER_END == ret) {
        brs_.size_ = rows_fetched;
        brs_.end_ = true;
        iter_end_ = true;
        OB_ASSERT(0 == brs_.size_);
        OB_ASSERT(0 == left_rows_total_cnt);
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("left_rows_iter_.get_next_batch failed", K(ret));
      }
    } else {
      // Note:  rows are fetched from left_rows(ChunkDatumStore), so there is no
      //        no skip row.
      //        Do not change brs_.skip_
      brs_.size_ = rows_fetched;
      left_rows_total_cnt -= rows_fetched; // debug only
      guard.set_batch_size(brs_.size_);
      for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < brs_.size_; l_idx++) {
        guard.set_batch_idx(l_idx);
        if (OB_FAIL(fill_cur_row_rescan_param())) {
          LOG_WARN("prepare rescan params failed", K(ret));
        } else {
          if (need_init_before_get_row_) {
            for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
              Iterator *&iter = subplan_iters_.at(i - 1);
              if (MY_SPEC.init_plan_idxs_.has_member(i)) {
                OZ(iter->prepare_init_plan());
              }
            }
            need_init_before_get_row_ = false;
          }
        }
        if (OB_SUCC(ret))  {
          bool filtered = false;
          if (OB_FAIL(filter_row(eval_ctx_, MY_SPEC.filter_exprs_, filtered))) {
            LOG_WARN("fail to filter row", K(ret));
          } else if (filtered) {
            brs_.skip_->set(l_idx);
          } else {
            ObDatum *datum = NULL;
            FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
              if (OB_FAIL((*e)->eval(eval_ctx_, datum))) {
                LOG_WARN("expr evaluate failed", K(ret), K(*e));
              }
            }
          }
          batch_rescan_ctl_.cur_idx_++;
        }
      } // for end
      LOG_DEBUG("show batch_rescan_ctl_ info ", K(batch_rescan_ctl_),
               K(rows_fetched), K(left_rows_total_cnt));
    }
  }
  FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
    (*e)->get_eval_info(eval_ctx_).projected_ = true;
  }

  return ret;
}

int ObSubPlanFilterOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t op_max_batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (need_init_before_get_row_) {
    OZ(prepare_onetime_exprs());
  }
  //从主表中获取一行数据
  clear_evaluated_flag();
  if (enable_left_px_batch_) {
    if (OB_FAIL(handle_next_batch_with_px_rescan(op_max_batch_size))) {
      LOG_WARN("handle_next_batch_with_px_rescan failed", K(ret));
    }
  } else {
    if (iter_end_) {
      brs_.size_ = 0;
      brs_.end_ = true;
    }
    while (OB_SUCC(ret) && !iter_end_) {
      const ObBatchRows *child_brs = NULL;
      set_param_null();
      if (OB_FAIL(child_->get_next_batch(op_max_batch_size, child_brs))) {
        LOG_WARN("fail to get next batch", K(ret));
      } else if (child_brs->end_) {
        iter_end_ = true;
      }
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_size(child_brs->size_);
      brs_.size_ = child_brs->size_;
      bool all_filtered = true;
      brs_.skip_->deep_copy(*child_brs->skip_, child_brs->size_);
      clear_evaluated_flag();

      for (int64_t l_idx = 0; OB_SUCC(ret) && l_idx < child_brs->size_; l_idx++) {
        if (child_brs->skip_->exist(l_idx)) { continue; }
        guard.set_batch_idx(l_idx);
        if (OB_FAIL(prepare_rescan_params(false))) {
          LOG_WARN("prepare rescan params failed", K(ret));
        } else {
          if (need_init_before_get_row_) {
            for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
              Iterator *&iter = subplan_iters_.at(i - 1);
              if (MY_SPEC.init_plan_idxs_.has_member(i)) {
                OZ(iter->prepare_init_plan());
              }
            }
            need_init_before_get_row_ = false;
          }
        }
        if (OB_SUCC(ret))  {
          bool filtered = false;
          if (OB_FAIL(filter_row(eval_ctx_, MY_SPEC.filter_exprs_, filtered))) {
            LOG_WARN("fail to filter row", K(ret));
          } else if (filtered) {
            brs_.skip_->set(l_idx);
          } else {
            all_filtered = false;
            ObDatum *datum = NULL;
            FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
              if (OB_FAIL((*e)->eval(eval_ctx_, datum))) {
                LOG_WARN("expr evaluate failed", K(ret), K(*e));
              }
            }
          }
        }
      } // for end
      if (OB_SUCC(ret) && all_filtered) {
        reset_batchrows();
        continue;
      }
      FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
      break;
    }
  }

  if (OB_SUCC(ret) && iter_end_) {
    set_param_null();
  }
  return ret;
}

int ObSubPlanFilterOp::prepare_rescan_params(bool need_save)
{
  int ret = OB_SUCCESS;
  ObObjParam *param = NULL;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  CK(OB_NOT_NULL(plan_ctx));
  cur_params_.reset();
  cur_param_idxs_.reset();
  cur_param_expr_idxs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.rescan_params_.count(); ++i) {
    if (OB_FAIL(MY_SPEC.rescan_params_.at(i).set_dynamic_param(eval_ctx_, param))) {
      LOG_WARN("fail to set dynamic param", K(ret));
    } else if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is null", K(ret));
    } else if (need_save) {
      ObObjParam copy_result;
      int64_t expr_idx = 0;
      LOG_DEBUG("prepare_rescan_params", KPC(param), K(i));
      OZ(batch_rescan_ctl_.params_.deep_copy_param(*param, copy_result));
      OZ(cur_params_.push_back(copy_result));
      OZ(cur_param_idxs_.push_back(MY_SPEC.rescan_params_.at(i).param_idx_));
      CK(OB_NOT_NULL(plan_ctx->get_phy_plan()));
      OZ(plan_ctx->get_phy_plan()->get_expr_frame_info().get_expr_idx_in_frame(
          MY_SPEC.rescan_params_.at(i).dst_, expr_idx));
      OZ(cur_param_expr_idxs_.push_back(expr_idx));
    }
  }
  if (OB_SUCC(ret) && need_save) {
    batch_rescan_ctl_.param_version_ += 1;
    OZ(batch_rescan_ctl_.params_.append_batch_rescan_param(
            cur_param_idxs_, cur_params_, cur_param_expr_idxs_));
  }
  return ret;
}

int ObSubPlanFilterOp::prepare_onetime_exprs()
{
  int ret = OB_SUCCESS;
  if (is_vectorized()) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(MY_SPEC.max_batch_size_);
    ret = prepare_onetime_exprs_inner();
  } else {
    // TODO qubin.qb: make eval_ctx_ NOT shared among operators
    // Child operator would set global eval_ctx.batch_size_ to 1 if its
    // parent is non-vectorized, however, onetime expr is calculated ahead of
    // fetching child rows. which leaves its child no chance to to explicitly
    // set eval_ctx.batch_size_ Therefore, set batchsize_ 1 explicitly no matter
    // is vectorization is enabled.
    eval_ctx_.set_batch_size(1);
    eval_ctx_.set_batch_idx(0);
    ret = prepare_onetime_exprs_inner();
  }
  return ret;
}

int ObSubPlanFilterOp::prepare_onetime_exprs_inner()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDatum copyed;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.onetime_exprs_.count(); ++i) {
    const ObDynamicParamSetter &setter = MY_SPEC.onetime_exprs_.at(i);
    ObDatum *datum = NULL;
    if (OB_FAIL(setter.src_->eval(eval_ctx_, datum))) {
      LOG_WARN("expression evaluate failed", K(ret));
    } else if (OB_FAIL(copyed.deep_copy(*datum, ctx_.get_allocator()))) {
      LOG_WARN("datum deep copy failed", K(ret));
    } else if (OB_FAIL(setter.update_dynamic_param(eval_ctx_, copyed))) {
      LOG_WARN("update dynamic param store failed", K(ret));
    }
  }
  return ret;
}

int ObSubPlanFilterOp::handle_update_set()
{
  int ret = OB_SUCCESS;
  const int64_t extra_size = 0;
  if (NULL == update_set_mem_) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
                       "SubplanFilterOp", ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(update_set_mem_, param))) {
      LOG_WARN("create memory entity failed", K(ret));
    }
  } else {
    update_set_mem_->get_arena_allocator().reuse();
  }

  if (OB_SUCC(ret)) {
    ObChunkDatumStore::LastStoredRow row_val(update_set_mem_->get_arena_allocator());
    Iterator *iter = NULL;
    subplan_iters_to_check_.reset();
    int64_t update_set_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < subplan_iters_.count(); ++i) {
      if (OB_ISNULL(iter = subplan_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null iterator", K(ret));
      } else if (OB_FAIL(iter->rewind())) {
        LOG_WARN("fail to rewind", K(ret));
      } else if (OB_SUCC(iter->get_next_row())) {
        if (OB_FAIL(subplan_iters_to_check_.push_back(iter))) {
          LOG_WARN("fail to push back. ", K(ret));
        } else {
          update_set_pos += iter->get_output().count();
        }
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row. ", K(ret));
      } else {  // set null for no row subplan iterator
        ret = OB_SUCCESS;
        int64_t j = update_set_pos;
        ObExpr *expr = NULL;
        update_set_pos += iter->get_output().count();
        if (OB_UNLIKELY(update_set_pos > MY_SPEC.update_set_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected update set count. ", K(ret), K(update_set_pos), K(MY_SPEC.update_set_.count()));
        }
        for (; OB_SUCC(ret) && j < update_set_pos; ++j) {
          if (OB_ISNULL(expr = MY_SPEC.update_set_.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null expr", K(ret), K(j), K(MY_SPEC.update_set_));
          } else {
            expr->locate_expr_datum(eval_ctx_).set_null();
            expr->set_evaluated_projected(eval_ctx_);
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(MY_SPEC.update_set_.count() != update_set_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected update set count. ", K(ret), K(update_set_pos), K(MY_SPEC.update_set_.count()));
    } else if (OB_FAIL(row_val.save_store_row(MY_SPEC.update_set_, eval_ctx_, extra_size))) {
      LOG_WARN("deep copy row failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subplan_iters_to_check_.count(); ++i) {
        if (OB_UNLIKELY(OB_SUCCESS == (ret = subplan_iters_to_check_.at(i)->get_next_row()))) {
          ret = OB_ERR_MORE_THAN_ONE_ROW;
          LOG_WARN("subquery too many rows", K(ret));
        } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row. ", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(row_val.store_row_->to_expr(MY_SPEC.update_set_, eval_ctx_))) {
        LOG_WARN("failed to get expr from chunck datum store. ", K(ret));
      }
    }
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
