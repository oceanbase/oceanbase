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

} // end aggregate
} // end share
} // end oceanbase
