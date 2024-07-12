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

#include "sql/engine/join/hash_join/join_hash_table.h"

namespace oceanbase
{
namespace sql
{

// TODO shengle support more normalized hash table
bool JoinHashTable::use_normalized_ht(JoinTableCtx &hjt_ctx)
{
  bool ret = false;
  ObJoinType join_type = hjt_ctx.join_type_;
  if (hjt_ctx.probe_opt_
      && (INNER_JOIN == join_type || LEFT_SEMI_JOIN == join_type
          || RIGHT_SEMI_JOIN == join_type)
      && !hjt_ctx.contain_ns_equal_
      && hjt_ctx.build_keys_->count() <= 2) {
    ret = true;
    for (int64_t i = 0; ret && i < hjt_ctx.build_keys_->count(); i++) {
      if (!ob_is_integer_type(hjt_ctx.build_keys_->at(i)->datum_meta_.type_)
          || 8 != hjt_ctx.build_keys_->at(i)->res_buf_len_) {
        ret = false;
      }
    }
  }
  LOG_DEBUG("use norimalized hash table", K(ret));

  return ret;
}

int JoinHashTable::init(JoinTableCtx &hjt_ctx, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  bool use_normalized = use_normalized_ht(hjt_ctx);
  LOG_DEBUG("init join hash table", K(use_normalized), K(hjt_ctx.is_shared_),
            K(hjt_ctx.build_keys_->count()));
  if (hjt_ctx.is_shared_) {
    if (use_normalized) {
      if (1 == hjt_ctx.build_keys_->count()) {
        //hash_table_ = OB_NEWx(NormalizedSharedInt64Table, (&allocator));
        hash_table_ = OB_NEWx(GenericSharedHashTable, (&allocator));
      } else if (2 == hjt_ctx.build_keys_->count()) {
        //hash_table_ = OB_NEWx(NormalizedSharedInt128Table, (&allocator));
        hash_table_ = OB_NEWx(GenericSharedHashTable, (&allocator));
      }
    } else {
      hash_table_ = OB_NEWx(GenericSharedHashTable, (&allocator));
    }
  } else {
    if (use_normalized) {
      if (1 == hjt_ctx.build_keys_->count()) {
        hash_table_ = OB_NEWx(NormalizedInt64Table, (&allocator));
      } else if (2 == hjt_ctx.build_keys_->count()) {
        hash_table_ = OB_NEWx(NormalizedInt128Table, (&allocator));
      }
    } else {
      hash_table_ = OB_NEWx(GenericTable, (&allocator));
    }
  }
  if (NULL == hash_table_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new hash table", K(ret));
  } else if (OB_FAIL(hash_table_->init(allocator, hjt_ctx.max_batch_size_))) {
    LOG_WARN("alloc bucket array failed", K(ret));
  }
  return ret;
}

int JoinHashTable::init_mcv(JoinTableCtx &hjt_ctx, ObIAllocator &allocator, 
                            uint64_t *mcv_hash_vals, int64_t &n_mcv) 
{
  int ret = OB_SUCCESS;
  bool use_normalized = use_normalized_ht(hjt_ctx);
  LOG_DEBUG("init mcv join hash table", K(hjt_ctx.is_shared_),
            K(hjt_ctx.build_keys_->count()));
  if (hjt_ctx.is_shared_) {
    mcv_hash_table_ = OB_NEWx(GenericSharedMcvHashTable, (&allocator));
  } else {
    if (use_normalized) {
      if (1 == hjt_ctx.build_keys_->count()) {
        mcv_hash_table_ = OB_NEWx(NormalizedInt64McvTable, (&allocator));
      } else if (2 == hjt_ctx.build_keys_->count()) {
        mcv_hash_table_ = OB_NEWx(NormalizedInt128McvTable, (&allocator));
      }
    } else {
      mcv_hash_table_ = OB_NEWx(GenericMcvTable, (&allocator));
    }
  }

  if (NULL == mcv_hash_table_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new hash table", K(ret));
  } else if (OB_FAIL(ret = mcv_hash_table_->init(allocator, hjt_ctx.max_batch_size_))) {
    LOG_WARN("alloc bucket array failed", K(ret));
  } else {
    static const int64_t RATIO_OF_BUCKETS = 2;
    int64_t n_bucket = next_pow2(n_mcv * RATIO_OF_BUCKETS);
    if (OB_FAIL(ret = mcv_hash_table_->build_prepare(n_mcv, n_bucket))) {
      LOG_WARN("mcv_hash_table build prepare failed ", K(n_mcv), K(n_bucket));
    } else {
      int64_t used_buckets = 0;
      int64_t collisions = 0;
      if (OB_FAIL(ret = mcv_hash_table_->init_mcv_bucket_hash(hjt_ctx, mcv_hash_vals, n_mcv, 
                                                                used_buckets, collisions))) {
        LOG_WARN("mcv_hash_table init hash_val failed ", K(ret));
      }
      mcv_hash_table_->set_diag_info(used_buckets, collisions);
    }
  }
  return ret;
}

int JoinHashTable::get_mcv_hash_exist(JoinTableCtx &hjt_ctx, uint64_t hash_val, bool &exist)
{
  return mcv_hash_table_->get_hash_exist(hjt_ctx, hash_val, exist);
}

int JoinHashTable::get_mcv_hash_exist_batch(JoinTableCtx &hjt_ctx, int64_t size, uint64_t *hash_vals, 
                                            uint16_t *selector, uint16_t selector_cnt)
{
  return mcv_hash_table_->get_hash_exist_batch(hjt_ctx, hash_vals, size, selector, selector_cnt);
}

int JoinHashTable::build_prepare(JoinTableCtx &ctx, int64_t row_count, int64_t bucket_count) {
  ctx.reuse();
  return hash_table_->build_prepare(row_count, bucket_count);
}

int JoinHashTable::build(bool is_mcv, JoinPartitionRowIter &iter, JoinTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t used_buckets = 0;
  int64_t collisions = 0;
  while (OB_SUCC(ret)) {
    int64_t read_size = 0;
    if (OB_FAIL(iter.get_next_batch(ctx.stored_rows_,
                                    ctx.max_batch_size_,
                                    read_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else if (0 == read_size) {
      ret = OB_ITER_END;
      break;
    } else if (!is_mcv && OB_FAIL(hash_table_->insert_batch(ctx,
            const_cast<ObHJStoredRow **>(ctx.stored_rows_), read_size, used_buckets, collisions))) {
      LOG_WARN("fail to insert batch", K(ret));
    } else if (is_mcv && OB_FAIL(mcv_hash_table_->insert_batch(ctx,
            const_cast<ObHJStoredRow **>(ctx.stored_rows_), read_size, used_buckets, collisions))) {
      LOG_WARN("fail to insert mcv batch", K(ret));
    }
    LOG_DEBUG("build hash join table", K(read_size), K(ret));
  }
  if (!is_mcv) {
    hash_table_->set_diag_info(used_buckets, collisions);
  } else {
    mcv_hash_table_->set_diag_info(used_buckets, collisions);
    if (OB_FAIL(mcv_hash_table_->finish_build_mcv(ctx))) {
      LOG_WARN("fail to finsh build mcv", K(ret));
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

// init probe key
// hash_table probe prepare
//   * prefect bucket for probe active rows
//   * init matched buckets array for probe active rows
int JoinHashTable::probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (0 < output_info.mcv_selector_cnt_
      && OB_FAIL(mcv_hash_table_->probe_prepare(ctx, 
                                                 output_info.mcv_selector_, 
                                                 output_info.mcv_selector_cnt_))) {
    LOG_WARN("fail to prepare mcv batch");
  } else if (OB_FAIL(hash_table_->probe_prepare(ctx,
                                                output_info.selector_,
                                                output_info.selector_cnt_))) {
    LOG_WARN("fail to prepare batch");
  }
  return ret;
}

int JoinHashTable::probe_batch(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (0 < output_info.mcv_selector_cnt_ 
      && OB_FAIL(mcv_hash_table_->probe_batch(
                                    ctx, output_info, 
                                    output_info.mcv_selector_, 
                                    output_info.mcv_selector_cnt_, 
                                    ctx.mcv_cur_items_, 
                                    0))) {
    LOG_WARN("fail to probe mcv batch");
  } else if (OB_FAIL(hash_table_->probe_batch(
                                    ctx, output_info, 
                                    output_info.selector_, 
                                    output_info.selector_cnt_, 
                                    ctx.cur_items_, 
                                    output_info.mcv_selector_cnt_))) {
    LOG_WARN("fail to probe batch");
  }
  output_info.first_probe_ = false;

  return ret;
}

int JoinHashTable::project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (0 < output_info.mcv_selector_cnt_ 
      && OB_FAIL(mcv_hash_table_->project_matched_rows(
                                    ctx,
                                    output_info.left_result_rows_,
                                    output_info.mcv_selector_, 
                                    output_info.mcv_selector_cnt_))) {
    LOG_WARN("fail to probe mcv batch");
  } else if (OB_FAIL(hash_table_->project_matched_rows(
                                    ctx,
                                    output_info.left_result_rows_,
                                    output_info.selector_, 
                                    output_info.selector_cnt_))) {
    LOG_WARN("fail to probe batch");
  }
  return ret;
};

int JoinHashTable::get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  output_info.selector_cnt_ = 0;
  output_info.mcv_selector_cnt_ = 0;
  ret = hash_table_->get_unmatched_rows(ctx,
                                        output_info.left_result_rows_, 
                                        ctx.cur_bkid_,
                                        ctx.cur_tuple_,
                                        output_info.selector_cnt_);
  if (ctx.is_probe_skew_ && OB_ITER_END == ret && 0 == output_info.selector_cnt_) {
    ret = mcv_hash_table_->get_unmatched_rows(ctx, 
                                              output_info.left_result_rows_, 
                                              ctx.mcv_cur_bkid_, 
                                              ctx.mcv_cur_tuple_, 
                                              output_info.mcv_selector_cnt_);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
