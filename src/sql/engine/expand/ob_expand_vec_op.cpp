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

#include "ob_expand_vec_op.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/basic/ob_vector_result_holder.h"

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObExpandVecSpec::DupExprPair, org_expr_, dup_expr_);
OB_SERIALIZE_MEMBER((ObExpandVecSpec, ObOpSpec), expand_exprs_, gby_exprs_, grouping_id_expr_,
                    dup_expr_pairs_, group_set_exprs_, pruned_groupby_exprs_);

int ObExpandVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init())) {
    LOG_WARN("init operator failed", K(ret));
  }
  return ret;
}

int ObExpandVecOp::init()
{
  int ret = OB_SUCCESS;
  void *holder_buf = nullptr;
  if (MY_SPEC.use_rich_format_) {
    holder_buf = allocator_.alloc(sizeof(ObVectorsResultHolder));
    if (OB_ISNULL(holder_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      vec_holder_ = new(holder_buf) ObVectorsResultHolder();
      if (OB_FAIL(vec_holder_->init(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("init result holder failed", K(ret));
      }
    }
  } else {
    holder_buf = allocator_.alloc(sizeof(ObBatchResultHolder));
    if (OB_ISNULL(holder_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      datum_holder_ = new (holder_buf) ObBatchResultHolder();
      if (OB_FAIL(datum_holder_->init(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("init batch result holder failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    reset_status();
    LOG_TRACE("expand open", K(MY_SPEC.expand_exprs_), K(MY_SPEC.gby_exprs_));
  }
  return ret;
}

int ObExpandVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  destroy();
  return ret;
}

int ObExpandVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("inner rescan failed", K(ret));
  }
  if (MY_SPEC.use_rich_format_) {
    vec_holder_->reset();
  } else {
    datum_holder_->reset();
  }
  if (OB_SUCC(ret)) {
    reset_status();
  }
  return ret;
}

int ObExpandVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool do_output = false;
  while (OB_SUCC(ret) && !do_output) {
    switch (dup_status_) {
    case (DupStatus::END): {
      brs_.end_ = true;
      brs_.size_ = 0;
      do_output = true;
      break;
    }
    case DupStatus::Init: {
      const ObBatchRows *child_brs = nullptr;
      if (OB_FAIL(get_next_batch_from_child(MIN(MY_SPEC.max_batch_size_, max_row_cnt), child_brs))) {
        LOG_WARN("get next batch from child failed", K(ret));
      } else if (child_brs->end_
                 && (0 == child_brs->size_
                     || child_brs->size_ == child_brs->skip_->accumulate_bit_cnt(child_brs->size_))) {
        dup_status_ = DupStatus::END;
        brs_.end_ = true;
        brs_.size_ = 0;
        do_output = true;
      } else if (OB_FAIL(backup_child_input(child_brs))) {
        LOG_WARN("backup child input nulls failed", K(ret));
      } else if (OB_FAIL(duplicate_rollup_exprs())) {
        LOG_WARN("duplicate rollup exprs failed", K(ret));
      }
      break;
    }
    case DupStatus::ORIG_ALL: {
      if (OB_FAIL(setup_grouping_id())) {
        LOG_WARN("setup grouping id failed", K(ret));
      } else {
        copy_child_brs();
        do_output = true;
      }
      break;
    }
    case DupStatus::DUP_PARTIAL: {
      if (OB_FAIL(do_dup_partial())) {
        LOG_WARN("duplicate partial input failed", K(ret));
      } else if (OB_FAIL(setup_grouping_id())) {
        LOG_WARN("set grouping id failed", K(ret));
      } else {
        do_output = true;
      }
      break;
    }
    }
    next_status();
  }
  if (OB_SUCC(ret)) {
    clear_evaluated_flags();
  }
  if (OB_SUCC(ret) && dup_status_ == DupStatus::END) {
    if (OB_FAIL(restore_child_input())) {
      LOG_WARN("restore child input failed", K(ret));
    }
  }
  return ret;
}

int ObExpandVecOp::do_dup_partial()
{
  int ret = OB_SUCCESS;
  // if following condition matches, copy current batch of result
  //
  // in oracle, group by rollup(c1, 1, c2)
  // `1` is a remove const expr, and rollup result just as group by (c1, 1, NULL)
  //
  // if rollup expr is a const expr, just cp current batch (both mysql & oracle)
  //
  // `group by c1, c2, rollup (c1, c2)`, result of rollup(c1) is same as group by (c1, NULL)
  //
  // in oracle group by roll(c1, c1, c2), rollup result of last c1 is same as group by (c1, c1, NULL)
  // rollup result of first c1 is same as group by (NULL, NULL, NULL)
  bool is_real_static_const =
    MY_SPEC.expand_exprs_.at(expr_iter_idx_)->type_ == T_FUN_SYS_REMOVE_CONST
    && MY_SPEC.expand_exprs_.at(expr_iter_idx_)->args_[0]->is_static_const_;
  if ((lib::is_oracle_mode() && is_real_static_const)
      || MY_SPEC.expand_exprs_.at(expr_iter_idx_)->is_const_expr()
      || has_exist_in_array(MY_SPEC.gby_exprs_, MY_SPEC.expand_exprs_.at(expr_iter_idx_))
      || exists_dup_expr(expr_iter_idx_)) {
    // do nothing
  } else if (MY_SPEC.use_rich_format_) {
    ObExpr *null_expr = MY_SPEC.expand_exprs_.at(expr_iter_idx_);
    if (OB_FAIL(null_expr->init_vector_for_write(eval_ctx_, null_expr->get_default_res_format(),
                                                 child_input_size_))) {
      LOG_WARN("init vector failed", K(ret));
    } else {
      null_expr->get_nulls(eval_ctx_).set_all(child_input_size_);
      null_expr->get_vector(eval_ctx_)->set_has_null();
    }
  } else {
    ObExpr *null_expr = MY_SPEC.expand_exprs_.at(expr_iter_idx_);
    ObDatumVector null_vec = null_expr->locate_expr_datumvector(eval_ctx_);
    for (int i = 0; i < child_input_size_; i++) {
      if (child_input_skip_->at(i)) {
      } else {
        null_vec.at(i)->set_null();
      }
    }
  }
  if (OB_SUCC(ret)) {
    copy_child_brs();
  }
  return ret;
}

int ObExpandVecOp::get_next_batch_from_child(int64_t max_row_cnt, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  bool stop = false;
  if (OB_FAIL(restore_child_input())) {
    LOG_WARN("restore child input nulls failed", K(ret));
  }
  while (!stop && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      LOG_WARN("get child next batch failed", K(ret));
    } else if (child_brs->end_) {
      stop = true;
    } else {
      stop = (child_brs->skip_->accumulate_bit_cnt(child_brs->size_) != child_brs->size_);
    }
  }
  return ret;
}

int ObExpandVecOp::backup_child_input(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  child_input_size_ = child_brs->size_;
  child_input_skip_ = child_brs->skip_;
  child_all_rows_active_ = child_brs->all_rows_active_;
  if (OB_UNLIKELY(child_brs->size_ <= 0)) {
  } else if (MY_SPEC.use_rich_format_) {
    if (OB_FAIL(vec_holder_->save(child_brs->size_))) {
      LOG_WARN("save vec result failed", K(ret));
    }
  } else {
    if (OB_FAIL(datum_holder_->save(child_brs->size_))) {
      LOG_WARN("save result failed", K(ret));
    }
  }
  return ret;
}

int ObExpandVecOp::restore_child_input()
{
  int ret = OB_SUCCESS;
  if (child_input_size_ > 0) {
    if (MY_SPEC.use_rich_format_) {
      if (OB_FAIL(vec_holder_->restore())) {
        LOG_WARN("restore vec results failed", K(ret));
      }
    } else if (OB_FAIL(datum_holder_->restore())) {
      LOG_WARN("restore results failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      child_input_size_ = 0;
      child_all_rows_active_ = false;
      child_input_skip_ = nullptr;
    }
  }
  return ret;
}

int ObExpandVecOp::setup_grouping_id()
{
  using id_vec_type = ObFixedLengthFormat<int64_t>;
  int ret = OB_SUCCESS;
  ObExpr *grouping_id = MY_SPEC.grouping_id_expr_;
  int64_t seq = MY_SPEC.expand_exprs_.count() - expr_iter_idx_;
  if (OB_UNLIKELY(child_input_size_ <= 0)) {
  } else if (MY_SPEC.use_rich_format_) {
    if (OB_UNLIKELY(grouping_id->get_vec_value_tc() != VEC_TC_INTEGER)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vec tc", K(ret), K(grouping_id->get_vec_value_tc()));
    } else if (OB_FAIL(grouping_id->init_vector_for_write(eval_ctx_, VEC_FIXED, child_input_size_))) {
      LOG_WARN("init vector failed", K(ret));
    } else {
      id_vec_type *ids = static_cast<id_vec_type *>(grouping_id->get_vector(eval_ctx_));
      for (int i = 0; i < child_input_size_; i++) {
        if (child_input_skip_->at(i)) {
        } else {
          ids->set_int(i, seq);
        }
      }
    }
  } else {
    ObDatum *datums = grouping_id->locate_datums_for_update(eval_ctx_, child_input_size_);
    for (int i = 0; i < child_input_size_; i++) {
      if (child_input_skip_->at(i)) {
      } else {
        datums[i].set_int(seq);
      }
    }
  }
  if (OB_SUCC(ret) && child_input_size_ > 0) {
    grouping_id->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

template<>
int ObExpandVecOp::duplicate_expr<VEC_FIXED>(ObExpr *from, ObExpr *to)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(to->get_default_res_format() != VEC_FIXED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid format", K(ret));
  } else if (OB_FAIL(to->init_vector_for_write(eval_ctx_, VEC_FIXED, brs_.size_))) {
    LOG_WARN("init vector failed", K(ret));
  } else {
    ObIVector *from_vec = from->get_vector(eval_ctx_);
    ObIVector *to_vec = to->get_vector(eval_ctx_);
    // copy nulls
    copy_bitmap_based_nulls(from_vec, to_vec);
    // copy datas
    ObFixedLengthBase *from_data = static_cast<ObFixedLengthBase *>(from_vec);
    ObFixedLengthBase *to_data = static_cast<ObFixedLengthBase *>(to_vec);
    MEMCPY(to_data->get_data(), from_data->get_data(), brs_.size_ * from_data->get_length());
  }
  return ret;
}

template<>
int ObExpandVecOp::duplicate_expr<VEC_DISCRETE>(ObExpr *from, ObExpr *to)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(to->get_default_res_format() != VEC_DISCRETE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid format", K(ret));
  } else if (OB_FAIL(to->init_vector_for_write(eval_ctx_, VEC_DISCRETE, brs_.size_))) {
    LOG_WARN("init vector failed", K(ret));
  } else {
    ObIVector *from_vec = from->get_vector(eval_ctx_);
    ObIVector *to_vec = to->get_vector(eval_ctx_);
    // copy nulls
    copy_bitmap_based_nulls(from_vec, to_vec);
    // copy datas, shallow copy
    ObDiscreteBase *from_data = static_cast<ObDiscreteBase *>(from_vec);
    ObDiscreteBase *to_data = static_cast<ObDiscreteBase *>(to_vec);
    for (int i = 0; i < brs_.size_; i++) {
      to_data->get_ptrs()[i] = from_data->get_ptrs()[i];
      to_data->get_lens()[i] = from_data->get_lens()[i];
    }
  }
  return ret;
}

template<>
int ObExpandVecOp::duplicate_expr<VEC_CONTINUOUS>(ObExpr *from, ObExpr *to)
{// continuous to discrete
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(to->get_default_res_format() != VEC_DISCRETE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid format", K(ret));
  } else if (OB_FAIL(to->init_vector_for_write(eval_ctx_, VEC_DISCRETE, brs_.size_))) {
    LOG_WARN("init vector failed", K(ret));
  } else {
    ObIVector *from_vec = from->get_vector(eval_ctx_);
    ObIVector *to_vec = to->get_vector(eval_ctx_);
    // copy nulls
    copy_bitmap_based_nulls(from_vec, to_vec);
    // shallow copy data
    ObContinuousFormat *from_data = static_cast<ObContinuousFormat *>(from_vec);
    ObDiscreteFormat *to_data = static_cast<ObDiscreteFormat *>(to_vec);
    ObBitmapNullVectorBase *from_nulls = static_cast<ObBitmapNullVectorBase *>(from_vec);
    for (int i = 0; i < brs_.size_; i++) {
      if (brs_.skip_->at(i)) {
        to_data->set_payload_shallow(i, nullptr, 0);
      } else if (from_nulls->is_null(i)) {
        to_data->set_null(i);
      } else {
        to_data->set_payload_shallow(i, from_data->get_payload(i), from_data->get_length(i));
      }
    }
  }
  return ret;
}

template<typename uni_vector>
int ObExpandVecOp::duplicate_expr_from_uniform(uni_vector *from, ObExpr *to)
{
  int ret = OB_SUCCESS;
  VectorFormat fmt = to->get_format(eval_ctx_);
  ObIVector *to_vec = to->get_vector(eval_ctx_);
  const char *payload = nullptr;
  int32_t len = 0;
  switch (fmt) {
  case (VEC_FIXED): {
    ObFixedLengthBase *to_data = static_cast<ObFixedLengthBase *>(to_vec);
    ObBitmapNullVectorBase *to_nulls = static_cast<ObBitmapNullVectorBase *>(to_vec);
    for (int i = 0; i < brs_.size_; i++) {
      if (brs_.skip_->at(i)) {
      } else if (from->is_null(i)) {
        to_nulls->set_null(i);
      } else if (FALSE_IT(from->get_payload(i, payload, len))) {
      } else {
        OB_ASSERT(len == to_data->get_length());
        MEMCPY(to_data->get_data() + i * len, payload,len);
      }
    }
    break;
  }
  case (VEC_DISCRETE): {
    ObDiscreteFormat *to_data = static_cast<ObDiscreteFormat *>(to_vec);
    for (int i = 0; i < brs_.size_; i++) {
      if (brs_.skip_->at(i)) {
      } else if (from->is_null(i)) {
        to_data->set_null(i);
      } else if (FALSE_IT(from->get_payload(i, payload, len))) {
      } else {
        to_data->set_payload_shallow(i, payload, len);
      }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid format", K(ret), K(fmt));
  }
  }
  return ret;
}
template<>
int ObExpandVecOp::duplicate_expr<VEC_UNIFORM>(ObExpr *from, ObExpr *to)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to->init_vector_for_write(eval_ctx_, to->get_default_res_format(), brs_.size_))) {
    LOG_WARN("init vector failed", K(ret));
  } else {
    ObIVector *from_vec = from->get_vector(eval_ctx_);
    if (OB_FAIL(duplicate_expr_from_uniform(static_cast<ObUniformFormat<false> *>(from_vec), to))) {
      LOG_WARN("duplicate expr failed", K(ret));
    }
  }
  return ret;
}

template<>
int ObExpandVecOp::duplicate_expr<VEC_UNIFORM_CONST>(ObExpr *from, ObExpr *to)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to->init_vector_for_write(eval_ctx_, to->get_default_res_format(), brs_.size_))) {
    LOG_WARN("init vector failed", K(ret));
  } else {
    ObIVector *from_vec = from->get_vector(eval_ctx_);
    if (OB_FAIL(duplicate_expr_from_uniform(static_cast<ObUniformFormat<true> *>(from_vec), to))) {
      LOG_WARN("duplicate expr failed", K(ret));
    }
  }
  return ret;
}

int ObExpandVecOp::duplicate_rollup_exprs()
{
#define DO_DUP(fmt)                                                                                \
  case fmt: {                                                                                      \
    ret = duplicate_expr<fmt>(org_expr, dup_expr);                                                 \
  } break

  int ret = OB_SUCCESS;
  copy_child_brs();
  if (MY_SPEC.use_rich_format_) {
    for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.dup_expr_pairs_.count(); i++) {
      ObExpr *org_expr = MY_SPEC.dup_expr_pairs_.at(i).org_expr_;
      ObExpr *dup_expr = MY_SPEC.dup_expr_pairs_.at(i).dup_expr_;
      if (OB_FAIL(org_expr->eval_vector(eval_ctx_, brs_))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        VectorFormat fmt = org_expr->get_format(eval_ctx_);
        switch (fmt) {
          LST_DO_CODE(DO_DUP, VEC_FIXED, VEC_DISCRETE, VEC_CONTINUOUS, VEC_UNIFORM,
                      VEC_UNIFORM_CONST);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid format", K(ret), K(fmt));
        }
        }
      }
      if (OB_SUCC(ret)) { dup_expr->set_evaluated_projected(eval_ctx_); }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.dup_expr_pairs_.count(); i++) {
      ObExpr *org_expr = MY_SPEC.dup_expr_pairs_.at(i).org_expr_;
      ObExpr *dup_expr = MY_SPEC.dup_expr_pairs_.at(i).dup_expr_;
      if (OB_FAIL(org_expr->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        ObDatum *to_datums = dup_expr->locate_datums_for_update(eval_ctx_, brs_.size_);
        ObDatumVector src_datums = org_expr->locate_expr_datumvector(eval_ctx_);
        for (int j = 0; j < brs_.size_; j++) {
          if (brs_.skip_->at(j)) {
          } else {
            to_datums[j] = *src_datums.at(j);
          }
        }
      }
      if (OB_SUCC(ret)) { dup_expr->set_evaluated_projected(eval_ctx_); }
    }
  }
  return ret;
}

void ObExpandVecOp::destroy()
{
  expr_iter_idx_ = -1;
  dup_status_ = DupStatus::Init;
  if (MY_SPEC.use_rich_format_) {
    if (vec_holder_ != nullptr) {
      vec_holder_->reset();
      vec_holder_ = nullptr;
    }
  } else if (datum_holder_ != nullptr) {
    datum_holder_->reset();
    datum_holder_ = nullptr;
  }
  child_input_size_ = 0;
  child_input_skip_ = nullptr;
  child_all_rows_active_ = false;
  allocator_.reset();
}

void ObExpandVecOp::next_status()
{
  switch(dup_status_) {
  case DupStatus::Init: {
    dup_status_ = DupStatus::ORIG_ALL;
    expr_iter_idx_ = MY_SPEC.expand_exprs_.count();
    break;
  }
  case DupStatus::ORIG_ALL: {
    dup_status_ = DupStatus::DUP_PARTIAL;
    expr_iter_idx_--;
    break;
  }
  case DupStatus::DUP_PARTIAL: {
    expr_iter_idx_--;
    if (expr_iter_idx_ < 0) {
      dup_status_ = DupStatus::Init;
    }
    break;
  }
  case DupStatus::END: {
    break;
  }
  }
}

void ObExpandVecOp::clear_evaluated_flags()
{
  // we don't clear evaluated flags of expand exprs and duplicate exprs
  // expand exprs do not need re-calculation, just set null flags
  // duplicate exprs are copied once, and do not changed ever since
  for (int i = 0; i < eval_infos_.count(); i++) {
    bool is_expand_eval_info = false;
    bool is_dup_expr_eval_info = false;
    for (int j = 0; !is_expand_eval_info && j < MY_SPEC.expand_exprs_.count(); j++) {
      is_expand_eval_info = eval_infos_.at(i) == &(MY_SPEC.expand_exprs_.at(j)->get_eval_info(eval_ctx_));
    }
    for (int j = 0; !is_dup_expr_eval_info && j < MY_SPEC.dup_expr_pairs_.count(); j++) {
      is_dup_expr_eval_info =
        (eval_infos_.at(i) == &(MY_SPEC.dup_expr_pairs_.at(j).dup_expr_->get_eval_info(eval_ctx_)));
    }
    if (!is_dup_expr_eval_info && !is_expand_eval_info) {
      eval_infos_.at(i)->clear_evaluated_flag();
    }
  }
}
} // end sql
} // end oceanbase