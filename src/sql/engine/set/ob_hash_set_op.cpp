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

#include "sql/engine/set/ob_hash_set_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashSetSpec::ObHashSetSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObSetSpec(alloc, type),
    hash_funcs_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObHashSetSpec, ObSetSpec), hash_funcs_);

ObHashSetOp::ObHashSetOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
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

int ObHashSetOp::inner_open()
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

void ObHashSetOp::reset()
{
  first_get_left_ = true;
  has_got_part_ = false;
  left_brs_ = nullptr;
  hp_infras_.reset();
}

int ObHashSetOp::inner_close()
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

int ObHashSetOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    reset();
  }
  return ret;
}

void ObHashSetOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  hp_infras_.~ObHashPartInfrastructure();
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  ObOperator::destroy();
}

int ObHashSetOp::is_left_has_row(bool &left_has_row)
{
  int ret = OB_SUCCESS;
  left_has_row = true;
  if (OB_FAIL(left_->get_next_row())) {
    if (OB_ITER_END == ret) {
      left_has_row = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next row from left op", K(ret));
    }
  }
  return ret;
}

int ObHashSetOp::get_left_row()
{
  int ret = OB_SUCCESS;
  if (first_get_left_) {
    // left_已经get_next_row
    first_get_left_ = false;
  } else {
    if (OB_FAIL(left_->get_next_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("child operator get next row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObHashSetOp::get_left_batch(const int64_t batch_size, const ObBatchRows *&child_brs)
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

int ObHashSetOp::get_right_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(right_->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("child operator get next row failed", K(ret));
    }
  }
  return ret;
}

int ObHashSetOp::build_hash_table(bool from_child)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  bool inserted = false;
  if (!from_child) {
    if (OB_FAIL(hp_infras_.open_cur_part(InputSide::RIGHT))) {
      LOG_WARN("failed to open cur part", K(ret));
    } else if (OB_FAIL(hp_infras_.resize(
        hp_infras_.get_cur_part_row_cnt(InputSide::RIGHT)))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  ctx_.get_my_session()->get_effective_tenant_id(),
                  hp_infras_.get_cur_part_file_size(InputSide::RIGHT),
                  spec_.type_,
                  spec_.id_,
                  &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    }
  }
  // 切换到right，使得dump逻辑到right partition
  hp_infras_.switch_right();
  bool has_exists = false;
  while (OB_SUCC(ret)) {
    if (from_child) {
      if (OB_FAIL(right_->get_next_row())) {
      } else if (OB_FAIL(hp_infras_.insert_row(right_->get_spec().output_, has_exists, inserted))) {
        LOG_WARN("failed to insert row", K(ret));
      }
    } else {
      if (OB_FAIL(hp_infras_.get_right_next_row(store_row, get_spec().output_))) {
      } else if (OB_FAIL(hp_infras_.insert_row(static_cast<const ObSetSpec &> 
                                               (get_spec()).set_exprs_, has_exists, inserted))) {
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
      LOG_WARN("failed to finish insert row", K(ret));
    } else if (!from_child && OB_FAIL(hp_infras_.close_cur_part(InputSide::RIGHT))) {
      LOG_WARN("failed to close cur part", K(ret));
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHashSetOp::build_hash_table_from_left(bool from_child)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  bool inserted = false;
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
  // 切换到left，使得dump逻辑到left partition
  hp_infras_.switch_left();
  bool has_exists = false;
  while (OB_SUCC(ret)) {
    if (from_child) {
      if (OB_FAIL(get_left_row())) {
      } else if (OB_FAIL(hp_infras_.insert_row(left_->get_spec().output_, has_exists, inserted))) {
        LOG_WARN("failed to insert row", K(ret));
      }
    } else {
      if (OB_FAIL(hp_infras_.get_left_next_row(store_row, static_cast<const ObSetSpec &>
                                                             (get_spec()).set_exprs_))) {
      } else if (OB_FAIL(hp_infras_.insert_row(static_cast<const ObSetSpec &>
                                               (get_spec()).set_exprs_, has_exists, inserted))) {
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
      LOG_WARN("failed to finish insert row", K(ret));
    } else if (!from_child && OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to close cur part", K(ret));
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHashSetOp::build_hash_table_from_left_batch(bool from_child, const int64_t batch_size)
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
      } else if (hp_infras_.calc_hash_value_for_batch(left_->get_spec().output_, left_brs->size_, 
                                                      left_brs->skip_, hash_values_for_batch_)) {
        LOG_WARN("failed to calc hash value for batch", K(ret));
      } else if (OB_FAIL(hp_infras_.insert_row_for_batch(left_->get_spec().output_, hash_values_for_batch_, 
                                                         left_brs->size_, left_brs->skip_, output_vec))) {
        LOG_WARN("failed to insert row for batch", K(ret));
      }                                          
    } else {
      int64_t read_rows = 0;
      if (OB_FAIL(hp_infras_.get_left_next_batch(static_cast<const ObSetSpec &>
                        (get_spec()).set_exprs_, batch_size, read_rows, hash_values_for_batch_))) {
        LOG_WARN("failed to get left next batch", K(ret));
      } else if (OB_FAIL(hp_infras_.insert_row_for_batch(static_cast<const ObSetSpec &>
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

int ObHashSetOp::init_hash_partition_infras()
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
                                     true, true, 2, &sql_mem_processor_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    const ObHashSetSpec &spec = static_cast<const ObHashSetSpec&>(get_spec());
    int64_t est_bucket_num = hp_infras_.est_bucket_count(est_rows, get_spec().width_);
    hp_infras_.set_io_event_observer(&io_event_observer_);
    if (OB_FAIL(hp_infras_.set_funcs(&spec.hash_funcs_, &spec.sort_collations_,
        &spec.sort_cmp_funs_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(hp_infras_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(hp_infras_.init_hash_table(est_bucket_num))) {
      LOG_WARN("failed to init hash table", K(ret));
    }
  }
  return ret;
}


int ObHashSetOp::convert_row(
  const common::ObIArray<ObExpr*> &src_exprs, const common::ObIArray<ObExpr*> &dst_exprs)
{
  int ret = OB_SUCCESS;
  if (dst_exprs.count() != src_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
      K(dst_exprs.count()));
  } else {
    ObDatum *src_datum = nullptr;
    for (uint32_t i = 0; i < dst_exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(src_exprs.at(i)->eval(eval_ctx_, src_datum))) {
        LOG_WARN("failed to eval expr", K(ret), K(i));
      } else {
        dst_exprs.at(i)->locate_expr_datum(eval_ctx_) = *src_datum;
        dst_exprs.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
    // LOG_TRACE("trace convert row", K(ret), K(dst_exprs.count()), K(src_exprs.count()));
  }
  return ret;
}

int ObHashSetOp::convert_batch(const common::ObIArray<ObExpr*> &src_exprs, 
                               const common::ObIArray<ObExpr*> &dst_exprs,
                               const int64_t batch_size,
                               const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  if (0 == batch_size) {
  } else if (dst_exprs.count() != src_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
      K(dst_exprs.count()));
  } else {
    for (uint32_t i = 0; i < dst_exprs.count(); ++i) {
      ObDatum *dst = dst_exprs.at(i)->locate_batch_datums(eval_ctx_);
      if (!src_exprs.at(i)->is_batch_result()) {
        ObDatum &src = src_exprs.at(i)->locate_expr_datum(eval_ctx_, 0);
        for (int64_t j = 0; j < batch_size; ++j) {
          if (skip.at(j)) {
            continue;
          }
          dst_exprs.at(i)->locate_expr_datum(eval_ctx_, j) = src;
        }
      } else {
        ObDatum *src = src_exprs.at(i)->locate_batch_datums(eval_ctx_);
        MEMCPY(dst, src, sizeof(ObDatum) * batch_size);
      }
      dst_exprs.at(i)->set_evaluated_projected(eval_ctx_);
    } // end for
  }
  return ret;
}

int ObHashSetOp::init_hash_partition_infras_for_batch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_hash_partition_infras())) {
    LOG_WARN("failed to init hash partition infra", K(ret));
  } else if (need_init_) {
    need_init_ = false;
    int64_t batch_size = get_spec().max_batch_size_;
    if (OB_FAIL(hp_infras_.init_my_skip(batch_size))) {
      LOG_WARN("failed to init my_skip", K(ret));
    } else if (OB_FAIL(hp_infras_.init_items(batch_size))) {
      LOG_WARN("failed to init items", K(ret));
    } else if (OB_FAIL(hp_infras_.init_distinct_map(batch_size))) {
      LOG_WARN("failed to init disitnct map", K(ret));
    } else if (OB_ISNULL(hash_values_for_batch_
                        = static_cast<uint64_t *> (ctx_.get_allocator().alloc(batch_size * sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init hash values for batch", K(ret), K(batch_size));
    }
  }
  return ret;
}

int ObHashSetOp::init_mem_context()
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

} // end namespace sql
} // end namespace oceanbase
