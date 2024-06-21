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

#include "sql/engine/set/ob_hash_set_vec_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashSetVecSpec::ObHashSetVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    set_exprs_(alloc),
    sort_collations_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObHashSetVecSpec, ObOpSpec), set_exprs_, sort_collations_);

ObHashSetVecOp::ObHashSetVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
  first_get_left_(true),
  has_got_part_(false),
  profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
  sql_mem_processor_(profile_, op_monitor_info_),
  hp_infras_(),
  hash_values_for_batch_(nullptr),
  need_init_(true),
  left_brs_(nullptr),
  mem_context_(nullptr)
{
}

int ObHashSetVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left or right is null", K(ret), K(left_), K(right_));
  } else if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("failed to init mem context", K(ret));
  }
  return ret;
}

void ObHashSetVecOp::reset()
{
  first_get_left_ = true;
  has_got_part_ = false;
  left_brs_ = nullptr;
  hp_infras_.reset();
}

int ObHashSetVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  } else {
    reset();
  }
  sql_mem_processor_.unregister_profile();
  return ret;
}

int ObHashSetVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    reset();
  }
  return ret;
}

void ObHashSetVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  hp_infras_.destroy();
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  ObOperator::destroy();
}

int ObHashSetVecOp::get_left_batch(const int64_t batch_size, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  if (first_get_left_) {
    CK(OB_NOT_NULL(left_brs_));
    child_brs = left_brs_;
    first_get_left_ = false;
  } else {
    if (OB_FAIL(left_->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("failed to get batch from child", K(ret));
    }
  }
  return ret;
}
// use for HashExcept, HashIntersect
int ObHashSetVecOp::build_hash_table_from_left_batch(bool from_child, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (!from_child) {
    if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to open cur part", K(ret));
    } else if (OB_FAIL(hp_infras_.resize(
        hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  ctx_.get_my_session()->get_effective_tenant_id(),
                  hp_infras_.get_cur_part_file_size(InputSide::LEFT),
                  spec_.type_,
                  spec_.id_,
                  &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    }
  }
  hp_infras_.switch_left();
  ObBitVector *output_vec = nullptr;
  while (OB_SUCC(ret)) {
    if (from_child) {
      const ObBatchRows *left_brs = nullptr;
      if (OB_FAIL(get_left_batch(batch_size, left_brs))) {
        LOG_WARN("failed to get left batch", K(ret));
      } else if (left_brs->end_ && 0 == left_brs->size_) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(convert_vector(left_->get_spec().output_,
                            static_cast<const ObHashSetVecSpec &>
                            (get_spec()).set_exprs_,
                            left_brs))) {
         LOG_WARN("failed to convert vector", K(ret));
      } else if (OB_FAIL(hp_infras_.calc_hash_value_for_batch(
                        static_cast<const ObHashSetVecSpec &>
                            (get_spec()).set_exprs_,
                        *left_brs,
                        hash_values_for_batch_))) {
        LOG_WARN("failed to calc hash value for batch", K(ret));
      } else if (OB_FAIL(hp_infras_.insert_row_for_batch(static_cast<const ObHashSetVecSpec &>
                                          (get_spec()).set_exprs_, hash_values_for_batch_,
                                            left_brs->size_, left_brs->skip_, output_vec))) {
        LOG_WARN("failed to insert row for batch", K(ret));
      }
    } else {
      int64_t read_rows = 0;
      if (OB_FAIL(hp_infras_.get_left_next_batch(static_cast<const ObHashSetVecSpec &>
                        (get_spec()).set_exprs_, batch_size, read_rows, hash_values_for_batch_))) {
        LOG_WARN("failed to get left next batch", K(ret));
      } else if (OB_FAIL(hp_infras_.insert_row_for_batch(static_cast<const ObHashSetVecSpec &>
                                                      (get_spec()).set_exprs_,
                                                      hash_values_for_batch_,
                                                      read_rows, nullptr, output_vec))) {
        LOG_WARN("failed to insert row", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status exit", K(ret));
    }
  } //end of while
  if (OB_ITER_END == ret) {
    if (OB_FAIL(hp_infras_.finish_insert_row())) {
      LOG_WARN("failed to finish insert", K(ret));
    } else if (!from_child && OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to close cur part", K(ret));
    }
  }
  return ret;
}

int ObHashSetVecOp::init_hash_partition_infras()
{
  int ret = OB_SUCCESS;
  int64_t est_rows = get_spec().rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, get_spec().px_est_size_factor_, est_rows, est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  ctx_.get_my_session()->get_effective_tenant_id(),
                  est_rows * get_spec().width_,
                  get_spec().type_,
                  get_spec().id_,
                  &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (OB_FAIL(hp_infras_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                                     GCONF.is_sql_operator_dump_enabled(),
                                     true, true, 1, (get_spec()).max_batch_size_,
                                     static_cast<const ObHashSetVecSpec &>(get_spec()).set_exprs_,
                                     &sql_mem_processor_, (get_spec()).compress_type_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    const ObHashSetVecSpec &spec = static_cast<const ObHashSetVecSpec&>(get_spec());
    int64_t est_bucket_num = hp_infras_.est_bucket_count(est_rows, get_spec().width_);
    hp_infras_.set_io_event_observer(&io_event_observer_);
    if (OB_FAIL(hp_infras_.set_funcs(&spec.sort_collations_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(hp_infras_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(hp_infras_.init_hash_table(est_bucket_num))) {
      LOG_WARN("failed to init hash table", K(ret));
    }
  }
  return ret;
}

int ObHashSetVecOp::init_hash_partition_infras_for_batch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_hash_partition_infras())) {
    LOG_WARN("failed to init hash partition infra", K(ret));
  } else if (need_init_) {
    need_init_ = false;
    int64_t batch_size = get_spec().max_batch_size_;
    if (OB_FAIL(hp_infras_.init_my_skip(batch_size))) {
      LOG_WARN("failed to init my_skip", K(ret));
    } else if (OB_ISNULL(hash_values_for_batch_
                        = static_cast<uint64_t *> (ctx_.get_allocator().alloc(batch_size * sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init hash values for batch", K(ret), K(batch_size));
    }
  }
  return ret;
}

int ObHashSetVecOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (NULL == mem_context_) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
        "ObHashSetRows",
        ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    }
  }
  return ret;
}

int ObHashSetVecOp::convert_vector(const common::ObIArray<ObExpr*> &src_exprs,
                               const common::ObIArray<ObExpr*> &dst_exprs,
                               const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  if (child_brs->end_ && 0 == child_brs->size_) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_exprs.count(); i++) {
      ObExpr *from = src_exprs.at(i);
      ObExpr *to = dst_exprs.at(i);
      if (OB_FAIL(from->eval_vector(eval_ctx_, *child_brs))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx_);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx_);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx_))->get_datums();
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, child_brs->size_));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx_));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < child_brs->size_ && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx_));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx_);
          if (src != dst) {
            MEMCPY(dst, src, child_brs->size_ * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, child_brs->size_));
        } else {
          to_vec_header = from_vec_header;
        }
        // init eval info
        if (OB_SUCC(ret)) {
          to->get_eval_info(eval_ctx_).cnt_ = child_brs->size_;
          to->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
