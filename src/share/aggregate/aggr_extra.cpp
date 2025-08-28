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
#define USING_LOG_PREFIX SQL

#include "share/aggregate/aggr_extra.h"
#include "share/stat/ob_hybrid_hist_estimator.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{


void HashBasedDistinctVecExtraResult::reuse()
{
  if (nullptr != hp_infras_ && hp_infras_mgr_->is_inited()) {
    hp_infras_mgr_->free_one_hp_infras(hp_infras_);
  }
  status_flags_ = 0;
  try_check_tick_ = 0;
  hp_infras_ = nullptr;
  brs_holder_.reset();
  VecExtraResult::reuse();
}

int HashBasedDistinctVecExtraResult::rewind()
{
  int ret = OB_SUCCESS;
  if (nullptr != hp_infras_ && need_rewind_) {
    if (OB_FAIL(hp_infras_->rewind())) {
      LOG_WARN("rewind iterator failed", K(ret));
    } else {
      got_row_ = false;
    }
  }
  LOG_DEBUG("extra result rewind");
  return ret;
}

HashBasedDistinctVecExtraResult::~HashBasedDistinctVecExtraResult()
{
  reuse();
  if (nullptr != hash_values_for_batch_) {
    alloc_.free(hash_values_for_batch_);
    hash_values_for_batch_ = nullptr;
  }
  if (nullptr != my_skip_) {
    alloc_.free(my_skip_);
    my_skip_ = nullptr;
  }
  brs_holder_.destroy();
  hp_infras_ = nullptr;
  aggr_info_ = nullptr;
  hp_infras_mgr_ = nullptr;
}

int HashBasedDistinctVecExtraResult::init_my_skip(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  void *data = nullptr;
  if (OB_ISNULL(data = alloc_.alloc(ObBitVector::memory_size(batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init bit vector", K(ret));
  } else {
    my_skip_ = to_bit_vector(data);
    my_skip_->reset(batch_size);
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::init_vector_default(ObEvalCtx &ctx, const int64_t size)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < aggr_info_->param_exprs_.count(); i++) {
    const ObExpr *expr = aggr_info_->param_exprs_.at(i);
    const VectorHeader &header = expr->get_vector_header(ctx);
    if (VEC_INVALID != header.format_) {
      // do nothing
    } else if (OB_FAIL(expr->init_vector_default(ctx, size))) {
      LOG_WARN("failed to init vector default", K(ret));
    }
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::init_hp_infras()
{
  int ret = OB_SUCCESS;
  if (inited_hp_infras_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(hp_infras_mgr_->init_one_hp_infras(need_rewind_,
                                                        &aggr_info_->distinct_collations_,
                                                        aggr_info_->param_exprs_, hp_infras_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    inited_hp_infras_ = true;
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::init_distinct_set(
    const ObAggrInfo &aggr_info, const bool need_rewind,
    ObHashPartInfrasVecMgr &hp_infras_mgr, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  hp_infras_mgr_ = &hp_infras_mgr;
  aggr_info_ = &aggr_info;
  need_rewind_ = need_rewind;
  max_batch_size_ = eval_ctx.max_batch_size_;
  const int64_t tenant_id = eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (!hp_infras_mgr.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash part infras group not initialized", K(ret));
  } else if (eval_ctx.max_batch_size_ > 0) {
    if (OB_ISNULL(hash_values_for_batch_
                  = static_cast<uint64_t *> (alloc_.alloc(eval_ctx.max_batch_size_ * sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init hash values for batch", K(ret), K(eval_ctx.max_batch_size_));
    } else if (OB_FAIL(init_my_skip(eval_ctx.max_batch_size_))) {
      LOG_WARN("failed to init my skip", K(ret), K(eval_ctx.max_batch_size_));
    } else if (OB_FAIL(brs_holder_.init(aggr_info.param_exprs_, eval_ctx))) {
      LOG_WARN("failed to init result holder", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != hash_values_for_batch_) {
      alloc_.free(hash_values_for_batch_);
      hash_values_for_batch_ = nullptr;
    }
    if (nullptr != my_skip_) {
      alloc_.free(my_skip_);
      my_skip_ = nullptr;
    }
  } else {
    is_inited_ = true;
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::insert_row_for_batch(const common::ObIArray<ObExpr *> &exprs,
                                                          const int64_t end_pos,
                                                          const ObBitVector *skip /* nullptr */,
                                                          const int64_t start_pos /* 0 */)
{
  int ret = OB_SUCCESS;
  ObBitVector *output_vec = nullptr;
  if (OB_ISNULL(my_skip_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_skip_ is not init", K(ret), K(my_skip_));
  } else if (nullptr == skip) {
    my_skip_->reset(end_pos);
  } else if (start_pos > 0) {
    my_skip_->deep_copy(*skip, end_pos);
    my_skip_->set_all(static_cast<int64_t>(0), start_pos);
    if (end_pos < max_batch_size_) {
      my_skip_->set_all(end_pos, max_batch_size_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!inited_hp_infras_ && OB_FAIL(init_hp_infras())) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else if (OB_FAIL(hp_infras_->calc_hash_value_for_batch(
               exprs, (start_pos > 0 || nullptr == skip) ? *my_skip_ : *skip, end_pos, false,
               hash_values_for_batch_))) {
    LOG_WARN("failed to calc hash values batch", K(ret));
  } else if (OB_FAIL(hp_infras_->insert_row_for_batch(
               exprs, hash_values_for_batch_, end_pos,
               (start_pos > 0 || nullptr == skip) ? my_skip_ : skip, output_vec))) {
    LOG_WARN("failed to insert batch rows", K(ret));
  } else {
    // int64_t got_rows = end_pos - output_vec->accumulate_bit_cnt(end_pos);
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::build_distinct_data_for_batch(
    const common::ObIArray<ObExpr*> &exprs,
    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = -1;
  try_check_tick_ = 0;
  ObBitVector *output_vec = nullptr;
  while (OB_SUCC(ret)) {
    ret = hp_infras_->get_left_next_batch(exprs, batch_size, read_rows, hash_values_for_batch_);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(hp_infras_->finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (OB_FAIL(hp_infras_->close_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to close cur part", K(ret));
      } else {
        LOG_TRACE("trace break out of the loop");
        break;
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get left next batch", K(ret));
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_->insert_row_for_batch(exprs,
                                                        hash_values_for_batch_,
                                                        read_rows,
                                                        nullptr,
                                                        output_vec))) {
      LOG_WARN("failed to insert batch rows, dump", K(ret));
    }
  }
  return ret;
}

int HashBasedDistinctVecExtraResult::get_next_unique_hash_table_batch(
    const common::ObIArray<ObExpr *> &exprs,
    const int64_t max_row_cnt,
    int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (!got_row_) {
    if (!inited_hp_infras_ && OB_FAIL(init_hp_infras())) {
      LOG_WARN("failed to init hash partition infrastructure", K(ret));
    } else if (OB_FAIL(hp_infras_->finish_insert_row())) {
      LOG_WARN("failed to finish to insert row", K(ret));
    } else if (OB_FAIL(hp_infras_->open_hash_table_part())) {
      LOG_WARN("failed to open hash table part", K(ret));
    } else {
      got_row_ = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hp_infras_->get_next_hash_table_batch(exprs,
                                                           max_row_cnt,
                                                           read_rows,
                                                           nullptr))) {
    if (OB_ITER_END == ret) {
      if (OB_FAIL(hp_infras_->end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras_->start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(hp_infras_->get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get dumped partitions", K(ret));
        }
      } else if (OB_FAIL(hp_infras_->open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(hp_infras_->resize(
          hp_infras_->get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else if (OB_FAIL(build_distinct_data_for_batch(exprs, max_row_cnt))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to build distinct data", K(ret));
      } else if (OB_FAIL(hp_infras_->open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_next_unique_hash_table_batch(exprs, max_row_cnt, read_rows)))) {
        LOG_WARN("failed to get next unique hash table batch", K(ret));
      }
    } else {
      LOG_WARN("failed to get next batch in hash table", K(ret));
    }
  }
  return ret;
}

int64_t VecExtraResult::to_string(char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited));
  J_OBJ_END();
  return pos;
}

int64_t HashBasedDistinctVecExtraResult::to_string(char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(got_row));
  J_KV(K_(need_rewind));
  if (nullptr != hp_infras_) {
    J_KV(KP_(hp_infras));
  }
  J_OBJ_END();
  return pos;
}

int DataStoreVecExtraResult::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                                       const sql::EvalBound &bound, const sql::ObBitVector &skip,
                                       const uint16_t selector[], const int64_t size,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(need_sort_)) {
    ObBatchRows brs =
      ObBatchRows(const_cast<ObBitVector &>(skip), size, bound.get_all_rows_active());
    ret = sort_->add_batch(brs, selector, size);
    if (OB_SUCC(ret) && need_count_) {
      int64_t diff = size;
      if (!bound.get_all_rows_active()) {
        for (int64_t i = 0; i < size; ++i) {
          if (skip.at(selector[i])) {
            --diff;
          }
        }
      }
      sort_count_ += diff;
    }
  } else {
    if (OB_SUCC(ret)) {
      ret = store_->add_batch(exprs, eval_ctx, selector, bound, skip, size);
    }
  }

  return ret;
}

int DataStoreVecExtraResult::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx,
                                       const sql::EvalBound &bound, const sql::ObBitVector &skip,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (pvt_skip == nullptr) {
    char *skip_buf = nullptr;
    int skip_size = ObBitVector::memory_size(eval_ctx.max_batch_size_);
    if (OB_ISNULL(skip_buf = (char *)alloc_.alloc(skip_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      pvt_skip = to_bit_vector(skip_buf);
      pvt_skip->reset(eval_ctx.max_batch_size_);
    }
  }

  if (OB_SUCC(ret)) {
    pvt_skip->set_all(static_cast<int64_t>(0), eval_ctx.max_batch_size_);
    pvt_skip->deep_copy(skip, bound.start(), bound.end());

    ObBatchRows brs = ObBatchRows(const_cast<ObBitVector &>(*pvt_skip), bound.batch_size(),
                                  pvt_skip->accumulate_bit_cnt(bound.batch_size()) == 0);

    if (need_sort_) {
      bool need_dump = true;
      ret = sort_->add_batch(brs, need_dump);
      if (OB_SUCC(ret) && need_count_) {
        int64_t diff = bound.range_size();
        if (!bound.get_all_rows_active()) {
          diff -= skip.accumulate_bit_cnt(bound);
        }
        sort_count_ += diff;
      }
    } else {
      int64_t size = bound.range_size();
      ret = store_->add_batch(exprs, eval_ctx, brs, size);
    }
  }

  return ret;
}

int DataStoreVecExtraResult::rewind()
{
  int ret = OB_SUCCESS;

  if (need_sort_) {
    ret = sort_->rewind();
  } else {
    ret = store_->begin(*vec_result_iter_);
  }

  return ret;
}

int DataStoreVecExtraResult::prepare_for_eval()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(need_sort_)) {
    ret = sort_->sort();
  } else {
    ret = store_->begin(*vec_result_iter_);
  }
  return ret;
}

int DataStoreVecExtraResult::get_next_batch(ObEvalCtx &ctx, const common::ObIArray<ObExpr *> &exprs,
                                            int64_t &read_rows)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(need_sort_)) {
    ret = sort_->get_next_batch(ctx.max_batch_size_, read_rows);
  } else {
    ret = vec_result_iter_->get_next_batch(exprs, ctx, ctx.max_batch_size_, read_rows, nullptr);
  }
  return ret;
}

int DataStoreVecExtraResult::init_data_set(ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx,
                                           ObMonitorNode *op_monitor_info,
                                           ObIOEventObserver *io_event_observer_,
                                           ObIAllocator &allocator, bool need_rewind)
{
  int ret = OB_SUCCESS;
  
  if (data_store_inited_) {
    ret = OB_INIT_TWICE;
    SQL_LOG(WARN, "inited", K(data_store_inited_), K(ret));
  } else if (need_sort_) {
    ObSortVecOpContext context;
    context.tenant_id_ = eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();

    // Get the sort key exprs
    OB_ASSERT(aggr_info.sort_collations_.count() > 0);

    ObExprPtrIArray &param_expr = aggr_info.param_exprs_;
    ObSortCollations &sort_collations_ = aggr_info.sort_collations_;

    void *sort_key_buf = nullptr;
    void *cur_collation_buf = nullptr;

    ExprFixedArray *sort_key = nullptr;
    ObSortCollations *cur_collation = nullptr;
    ExprFixedArray *addon_keys = nullptr;

    if (OB_ISNULL(sort_key_buf = allocator.alloc(sizeof(ExprFixedArray)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(sort_key = new (sort_key_buf) ExprFixedArray(allocator))) {
    } else if (OB_ISNULL(cur_collation_buf = allocator.alloc(sizeof(ObSortCollations)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(cur_collation = new (cur_collation_buf) ObSortCollations(allocator))) {
    } else if (OB_FAIL(sort_key->init(sort_collations_.count()))) {
      SQL_LOG(WARN, "failed to init", K(ret));
    } else if (OB_FAIL(cur_collation->init(sort_collations_.count()))) {
      SQL_LOG(WARN, "failed to init", K(ret));
    }
    for (int i = 0; i < sort_collations_.count() && OB_SUCC(ret); i++) {
      ObExpr *cur_expr = param_expr.at(sort_collations_.at(i).field_idx_);
      if (is_contain(*sort_key, cur_expr)) {
      } else if (OB_FAIL(sort_key->push_back(cur_expr))) {
        SQL_LOG(WARN, "failed to push back", K(ret));
      } else if (OB_FAIL(cur_collation->push_back(sort_collations_.at(i)))) {
        SQL_LOG(WARN, "failed to push back", K(ret));
      } else {
        int last_idx = cur_collation->count() - 1;
        cur_collation->at(last_idx).field_idx_ = last_idx;
      }
    }

    if (OB_SUCC(ret) && sort_key->count() < param_expr.count()) {
      context.has_addon_ = true;
      void *addon_keys_buf = nullptr;
      int init_size = param_expr.count() - sort_key->count();
      if (OB_ISNULL(addon_keys_buf = allocator.alloc(sizeof(ExprFixedArray)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else if (FALSE_IT(addon_keys = new (addon_keys_buf) ExprFixedArray(allocator))) {
      } else if (OB_FAIL(addon_keys->init(init_size))) {
        SQL_LOG(WARN, "failed to init", K(ret));
      } else {
        for (int i = 0; i < param_expr.count() && OB_SUCC(ret) && addon_keys->count() < init_size;
             i++) {
          if (!is_contain(*sort_key, param_expr.at(i))
              && OB_FAIL(addon_keys->push_back(param_expr.at(i)))) {
            SQL_LOG(WARN, "failed to push back", K(ret));
          }
        }
      }
    }
    context.sk_exprs_ = sort_key;
    context.addon_exprs_ = addon_keys;
    context.sk_collations_ = cur_collation;
    context.eval_ctx_ = &eval_ctx;
    context.exec_ctx_ = &eval_ctx.exec_ctx_;
    context.need_rewind_ = need_rewind;

    void *sort_buf = nullptr;

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sort_buf = allocator.alloc(sizeof(ObSortVecOpProvider)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else if (FALSE_IT(sort_ = new (sort_buf) ObSortVecOpProvider(*op_monitor_info))) {
      } else if (OB_FAIL(sort_->init(context))) {
        LOG_WARN("failed to init sort", K(ret));
      } else {
        sort_->set_operator_type(op_monitor_info->get_operator_type());
        sort_->set_operator_id(op_monitor_info->get_op_id());
        sort_->set_io_event_observer(io_event_observer_);
      }
    }
  } else {
    void *store_buf = nullptr;
    ObMemAttr attr(eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                   ObModIds::OB_SQL_AGGR_FUN_GROUP_CONCAT, ObCtxIds::WORK_AREA);
    if (OB_ISNULL(store_buf = allocator.alloc(sizeof(ObTempRowStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (OB_FALSE_IT(store_ = new (store_buf) ObTempRowStore()))  {
    } else if (OB_FAIL(store_->init(aggr_info.param_exprs_, eval_ctx.max_batch_size_, attr, INT64_MAX,
                                    true, 0, ObCompressorType::NONE_COMPRESSOR))) {
      LOG_WARN("init temp row store failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(data_store_brs_holder_.init(aggr_info.param_exprs_, eval_ctx))) {
      LOG_WARN("failed to init result holder", K(ret));
    } else {
      data_store_inited_ = true;
    }
  }
  return ret;
}

void DataStoreVecExtraResult::reuse()
{
  data_store_brs_holder_.reset();
}

DataStoreVecExtraResult::~DataStoreVecExtraResult()
{
  reuse();
  if (need_sort_) {
    if (sort_ != nullptr) { 
      sort_->reset();
    }
  } else {
    store_->reset();
  }
  data_store_brs_holder_.destroy();
  sort_ = nullptr;
  if (pvt_skip != nullptr) {
    alloc_.free(pvt_skip);
    pvt_skip = nullptr;
  }
}

TopFreHistVecExtraResult::~TopFreHistVecExtraResult()
{

}

static int get_param_int_val(ObExpr *expr, ObDatum *datum, int64_t &val)
{
  int ret = OB_SUCCESS;
  if (expr->obj_meta_.is_integer_type()) {
    val = datum->get_int();
  } else if (expr->obj_meta_.is_decimal_int()) {
    ret = ObExprUtil::trunc_decint2int64(datum->get_decimal_int(), datum->get_int_bytes(),
                                         expr->datum_meta_.scale_, val);
  } else if (expr->obj_meta_.is_number()) {
    ret = ObExprUtil::trunc_num2int64(*datum, val);
  }
  return ret;
}

int TopFreHistVecExtraResult::init_topk_fre_histogram_item(ObIAllocator &allocator,
                                                           ObAggrInfo &aggr_info, 
                                                           ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  constexpr int64_t MAX_BATCH_SIZE = 256;
  if (OB_UNLIKELY(T_FUN_TOP_FRE_HIST != aggr_info.get_expr_type()) ||
      OB_ISNULL(aggr_info.window_size_param_expr_) ||
      OB_ISNULL(aggr_info.item_size_param_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_info));
  } else {
    ObDatum *window_size_result = NULL;
    ObDatum *item_size_result = NULL;
    ObDatum *max_disuse_cnt_result = NULL;
    if (OB_UNLIKELY(!aggr_info.window_size_param_expr_->obj_meta_.is_numeric_type() ||
                    !aggr_info.item_size_param_expr_->obj_meta_.is_numeric_type() ||
                    (aggr_info.max_disuse_param_expr_ != NULL &&
                     !aggr_info.max_disuse_param_expr_->obj_meta_.is_numeric_type()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr node is null", K(ret), KPC(aggr_info.window_size_param_expr_),
                                    KPC(aggr_info.item_size_param_expr_),
                                    KPC(aggr_info.max_disuse_param_expr_));
    } else if (OB_FAIL(aggr_info.window_size_param_expr_->eval(eval_ctx, window_size_result)) ||
               OB_FAIL(aggr_info.item_size_param_expr_->eval(eval_ctx, item_size_result)) ||
               (aggr_info.max_disuse_param_expr_ != NULL &&
                OB_FAIL(aggr_info.max_disuse_param_expr_->eval(eval_ctx, max_disuse_cnt_result)))) {
      LOG_WARN("eval failed", K(ret));
    } else if (OB_ISNULL(window_size_result) ||
               OB_ISNULL(item_size_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(window_size_result), K(item_size_result));
    } else if (OB_FAIL(get_param_int_val(aggr_info.window_size_param_expr_, window_size_result,
                                         window_size_))
               || OB_FAIL(get_param_int_val(aggr_info.item_size_param_expr_,
                                            item_size_result, item_size_))
               || (aggr_info.max_disuse_param_expr_ != NULL
                   && OB_FAIL(get_param_int_val(aggr_info.max_disuse_param_expr_,
                                                max_disuse_cnt_result, max_disuse_cnt_)))) {
      LOG_WARN("failed to get int param val", K(*window_size_result), K(window_size_),
                                              K(*item_size_result), K(item_size_),
                                              KPC(max_disuse_cnt_result), K(max_disuse_cnt_), K(ret));
    } else if (OB_FALSE_IT(tmp_batch_cap_ = MAX_BATCH_SIZE)) {
    } else if (OB_ISNULL(tmp_batch_payloads_ = (const char**)allocator.alloc(sizeof(const char*) * tmp_batch_cap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_ISNULL(tmp_batch_payload_lens_ = (int32_t*) allocator.alloc(sizeof(int32_t) * tmp_batch_cap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_ISNULL(tmp_batch_hash_vals_ = (uint64_t*) allocator.alloc(sizeof(uint64_t) * tmp_batch_cap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      tmp_batch_idx_ = 0;
      is_topk_hist_need_des_row_ = aggr_info.is_need_deserialize_row_;
      set_topk_fre_histogram_item();
      LOG_TRACE("succeed to init topk fre histogram item", K(window_size_), K(item_size_),
                                          K(aggr_info.is_need_deserialize_row_), K(max_disuse_cnt_));
    }
  }
  return ret;
}

void TopFreHistVecExtraResult::reuse()
{
  topk_fre_hist_.~ObTopKFrequencyHistograms();
  new (&topk_fre_hist_) ObTopKFrequencyHistograms();
  set_topk_fre_histogram_item();
  tmp_batch_idx_ = 0;
}

int TopFreHistVecExtraResult::rewind()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("unsupported top fre hist in window function", K(ret));
  return ret;
}

void TopFreHistVecExtraResult::set_topk_fre_histogram_item()
{
  topk_fre_hist_.set_window_size(window_size_);
  topk_fre_hist_.set_item_size(item_size_);
  topk_fre_hist_.set_is_topk_hist_need_des_row(is_topk_hist_need_des_row_);
  topk_fre_hist_.set_max_disuse_cnt(max_disuse_cnt_);
}

int HybridHistVecExtraResult::init_data_set(ObIAllocator &allocator,
                                            ObAggrInfo &aggr_info, 
                                            ObEvalCtx &eval_ctx,
                                            ObIOEventObserver *io_event_observer)
{
  int ret = OB_SUCCESS;
  ObDatum *bucket_num_result = NULL;
  void *row_store_buf = nullptr;
  int64_t tenant_id = eval_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
  ObMemAttr attr(tenant_id, "HybirdHist", ObCtxIds::WORK_AREA);
  lib::ContextParam param;
  param.set_mem_attr(tenant_id, "HybirdHist", ObCtxIds::WORK_AREA)
       .set_properties(lib::USE_TL_PAGE_OPTIONAL);

  if (OB_ISNULL(aggr_info.bucket_num_param_expr_) ||
     OB_UNLIKELY(!aggr_info.bucket_num_param_expr_->obj_meta_.is_numeric_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(aggr_info.param_exprs_.count()),
                                     K(aggr_info.bucket_num_param_expr_));
  } else if (OB_FAIL(aggr_info.bucket_num_param_expr_->eval(eval_ctx, bucket_num_result))) {
    LOG_WARN("eval failed", K(ret));
  } else if (OB_ISNULL(bucket_num_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(bucket_num_result));
  } else if (OB_FAIL(get_param_int_val(aggr_info.bucket_num_param_expr_, 
                                       bucket_num_result,
                                       bucket_num_))) {
    LOG_WARN("failed to get int param val", K(ret), K(bucket_num_result));
  } else if (OB_FAIL(init_batch_vector(allocator, eval_ctx.max_batch_size_))) {
    LOG_WARN("failed to init batch vector", K(ret));
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create entity failed");
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned");
  } else if (OB_FAIL(store_.init(aggr_info.param_exprs_, 
                                 eval_ctx.max_batch_size_, 
                                 attr, 
                                 0,
                                 true, 
                                 sizeof(BucketDesc), 
                                 ObCompressorType::NONE_COMPRESSOR))) {
    LOG_WARN("init temp row store failed", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                             tenant_id, 0, 
                                             op_monitor_info_.get_operator_type(), 
                                             0, &eval_ctx.exec_ctx_))) {
    LOG_WARN("failed to init sql memory manager processor", K(ret));
  } else {
    store_.set_allocator(mem_context_->get_malloc_allocator());
    store_.set_callback(&sql_mem_processor_);
    store_.set_io_event_observer(io_event_observer);
    store_.set_dir_id(sql_mem_processor_.get_dir_id());
    data_store_inited_ = true;
    LOG_TRACE("succeed to init hybrid histogram item", K(bucket_num_));
  }
  return ret;
}

int HybridHistVecExtraResult::prepare_for_eval()
{
  int ret = OB_SUCCESS;
  if (prev_row_ != NULL) {
    fill_row_desc();
    BucketDesc *desc = reinterpret_cast<BucketDesc*>(
                  prev_row_->get_extra_payload(store_.get_row_meta()));
    *desc = batch_bucket_desc_[0];
    prev_row_ = NULL;
  }
  return ret;
}

int HybridHistVecExtraResult::compute_hybrid_hist_result(
                                 int64_t max_batch_size,
                                 const ObObjMeta &obj_meta,
                                 ObIAllocator &allocator,
                                 ObHybridHistograms &histogram)
{
  int ret = OB_SUCCESS;
  ObTempRowStore::Iterator vec_result_iter;
  int64_t bucket_size = -1;
  bool dynamic_size = false;
  int64_t dynamic_step = 0;
  int64_t bucket_rows = 0;
  int64_t ep_num = 0;
  int64_t un_pop_count = 0;
  int64_t un_pop_bucket = 0;
  if (num_distinct_ == 0) {
    // do nothing
  } else if (OB_FAIL(store_.begin(vec_result_iter))) {
    LOG_WARN("failed to read temp store", K(ret));
  } else {
    int64_t row_index = 0;
    constexpr int64_t MAX_BATCH_SIZE = 256;
    max_batch_size = min(MAX_BATCH_SIZE, max_batch_size);
    const ObCompactRow *rows[MAX_BATCH_SIZE];
    BucketDesc *desc = NULL;
    int64_t ep_count = 0;
    bool is_pop = false;
    while (OB_SUCC(ret)) {
      int64_t read_rows = 0;
      if (OB_FAIL(vec_result_iter.get_next_batch(max_batch_size, read_rows, rows))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          SQL_LOG(WARN, "get row from temp store failed", K(ret));
        }
        break;
      } else if (read_rows <= 0) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "read unexpected zero rows", K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < read_rows; ++i) {
          if (OB_UNLIKELY(row_index == 0)) {
            if (num_distinct_ <= bucket_num_ + 2) {
              bucket_size = 1;
            } else if (bucket_num_ <= pop_count_) {
              bucket_size = total_count_ / bucket_num_;
            } else {
              dynamic_size = true;
              // first bucket always contain only one values. following code will handle first value is 
              // popular value or not.
              desc = reinterpret_cast<BucketDesc*>(rows[i]->get_extra_payload(store_.get_row_meta()));
              if (desc->is_pop_ || bucket_num_ == pop_count_ + 1) {
                bucket_size = (total_count_ - pop_freq_) / (bucket_num_ - pop_count_);
              } else {
                bucket_size = (total_count_ - pop_freq_ - desc->ep_count_) / (bucket_num_ - pop_count_ - 1);
              }
            }
          } 
          
          desc = reinterpret_cast<BucketDesc*>(rows[i]->get_extra_payload(store_.get_row_meta()));
          ep_count = desc->ep_count_;
          is_pop = desc->is_pop_;
          bucket_rows += ep_count;
          ep_num += ep_count;
          if (!is_pop) {
            un_pop_count += ep_count;
          }

          if (bucket_rows > bucket_size || 
              0 == row_index || 
              num_distinct_ - 1 == row_index) {
            bucket_rows = 0;
            ObObj ep_val;
            ObDatum datum = rows[i]->get_datum(store_.get_row_meta(), 0);
            if (OB_FAIL(datum.to_obj(ep_val, obj_meta))) {
              LOG_WARN("failed to obj", K(ret));
            } else if (OB_FAIL(ob_write_obj(allocator, ep_val, ep_val))) {
              LOG_WARN("failed to write obj", K(ret), K(ep_val));
            } else {
              ObHistBucket bkt(ep_val, ep_count, ep_num);
              if (!is_pop) {
                ++un_pop_bucket;
              }
              if (OB_FAIL(histogram.add_hist_bucket(bkt))) {
                LOG_WARN("failed add hist bucket", K(ret));
              }
            }
            if (dynamic_size && bucket_num_ > pop_count_ + un_pop_bucket) {
              bucket_size = (total_count_ - pop_freq_ - un_pop_count)
                            / (bucket_num_ - pop_count_ - un_pop_bucket);
            }
          }
          ++row_index;
        }
      }
    }
    vec_result_iter.reset();
    if (OB_SUCC(ret)) {
      histogram.set_total_count(total_count_);
      histogram.set_num_distinct(num_distinct_);
      histogram.set_pop_count(pop_count_);
      histogram.set_pop_freq(pop_freq_);
    }
  }
  return ret;
}

int HybridHistVecExtraResult::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  HybridHistVecAvailableMemChecker max_available_rowcnt_checker(store_.get_row_cnt_in_memory());
  HybridHistVecAvailableMemChecker max_available_mem_checker(sql_mem_processor_.get_data_size());
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      max_available_rowcnt_checker,
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            max_available_mem_checker,
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(store_.dump(false))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      sql_mem_processor_.reset();
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump", K(sql_mem_processor_.get_data_size()),
        K(store_.get_row_cnt_in_memory()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int HybridHistVecExtraResult::init_batch_vector(ObIAllocator &allocator, int max_batch_size)
{
  int ret = OB_SUCCESS;
  void *vector_buf = NULL;
  max_batch_size = min(max_batch_size, MAX_BATCH_SIZE);
  const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
  const int64_t lens_size = sizeof(int32_t) * max_batch_size;
  const int64_t ptrs_size = sizeof(char *) * max_batch_size;
  const int64_t bucket_desc_size = sizeof(BucketDesc) * max_batch_size;
  ObBitVector *nulls = nullptr;
  int32_t *lens = nullptr;
  char **ptrs = nullptr;
  if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
  } else if (OB_ISNULL(lens = static_cast<int32_t *>(allocator.alloc(lens_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(lens_size));
  } else if (OB_ISNULL(ptrs = static_cast<char **>(allocator.alloc(ptrs_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
  } else if (OB_ISNULL(batch_bucket_desc_ = 
                        static_cast<BucketDesc*>(allocator.alloc(bucket_desc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
  } else if (OB_ISNULL(vector_buf = allocator.alloc(sizeof(RTVectorType<VEC_DISCRETE, VEC_TC_STRING>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
  } else {
    nulls->reset(max_batch_size);
    MEMSET(ptrs, 0, ptrs_size);
    MEMSET(lens, 0, lens_size);
    batch_vector_ = new(vector_buf) RTVectorType<VEC_DISCRETE, VEC_TC_STRING>(lens, ptrs, nulls);
    batch_idx_ = 0;
    max_batch_size_ = max_batch_size;
  } 
  return ret;
}

int HybridHistVecExtraResult::flush_batch_rows(bool need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(batch_idx_ > 0)) {
    if (OB_NOT_NULL(prev_row_)) {
        BucketDesc *desc = reinterpret_cast<BucketDesc*>(
           prev_row_->get_extra_payload(store_.get_row_meta()));
        *desc = batch_bucket_desc_[0];
    }
    if (need_dump) {
      if (OB_FAIL(process_dump())) {
        SQL_LOG(WARN, "failed to process dump", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(batch_idx_ > MAX_BATCH_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected batch idx", K(ret));
      } else {
        SMART_VARS_2((uint16_t[MAX_BATCH_SIZE], selector), 
                     (ObCompactRow*[MAX_BATCH_SIZE], rows)) {
          ObSEArray<ObIVector*, 1> vecs;
          for (uint16_t i = 0; i < batch_idx_; ++i) {
            selector[i] = i;
          }
          MEMSET(rows, 0, batch_idx_);
          if (OB_FAIL(vecs.push_back(batch_vector_))) {
            LOG_WARN("failed to push back batch vector", K(ret));
          } else if (OB_FAIL(store_.add_batch(vecs, selector, batch_idx_, rows))) {
            LOG_WARN("failed to add batch to store", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < batch_idx_ - 1; ++i) {
              if (OB_ISNULL(rows[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else {
                BucketDesc *desc = reinterpret_cast<BucketDesc*>(
                  rows[i]->get_extra_payload(store_.get_row_meta()));
                *desc = batch_bucket_desc_[i + 1];
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(rows[batch_idx_ - 1])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else {
                prev_row_ = rows[batch_idx_ - 1];
                prev_row_->get_cell_payload(store_.get_row_meta(), 0, prev_payload_, prev_len_);
                batch_idx_ = 0;
              }
            }
          }
        }
      }
      
    }
  }
  return ret;
}

} // namespace aggregate
} // namespace share
} // namespace oceanbase
