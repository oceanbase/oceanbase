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
    if (use_normalized ) {
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
    if (use_normalized ) {
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

int JoinHashTable::build_prepare(JoinTableCtx &ctx, int64_t row_count, int64_t bucket_count) {
  ctx.reuse();
  return hash_table_->build_prepare(row_count, bucket_count);
}

int JoinHashTable::build(JoinPartitionRowIter &iter, JoinTableCtx &ctx) {
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
    } else if (OB_FAIL(hash_table_->insert_batch(ctx,
            const_cast<ObHJStoredRow **>(ctx.stored_rows_), read_size, used_buckets, collisions))) {
      LOG_WARN("fail to insert batch", K(ret));
    }
    LOG_DEBUG("build hash join table", K(read_size), K(ret));
  }
  hash_table_->set_diag_info(used_buckets, collisions);

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

// init probe key
// hash_table probe prepare
//   * prefect bucket for probe active rows
//   * init matched buckets array for probe active rows
int JoinHashTable::probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) {
  return hash_table_->probe_prepare(ctx, output_info);
}

int JoinHashTable::probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) {
  return hash_table_->probe_batch(ctx, output_info);
}

int JoinHashTable::get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  return hash_table_->get_unmatched_rows(ctx, output_info);
}

} // end namespace sql
} // end namespace oceanbase
