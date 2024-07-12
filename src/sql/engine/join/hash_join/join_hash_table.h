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

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_JOIN_HASH_TABLE_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_JOIN_HASH_TABLE_H_

#include "sql/engine/join/hash_join/hash_table.h"

namespace oceanbase
{
namespace sql
{

class JoinHashTable {
public:
  JoinHashTable() : hash_table_(NULL), mcv_hash_table_(NULL)
  {}
  int init(JoinTableCtx &hjt_ctx, ObIAllocator &allocator);
  int init_mcv(JoinTableCtx &hjt_ctx, ObIAllocator &allocator, 
                           uint64_t *mcv_hash_vals, int64_t &n_mcv);
  bool use_normalized_ht(JoinTableCtx &hjt_ctx);
  int build_prepare(JoinTableCtx &ctx, int64_t row_count, int64_t bucket_count);
  int build(bool is_mcv, JoinPartitionRowIter &iter, JoinTableCtx &jt_ctx);
  int get_mcv_hash_exist(JoinTableCtx &hjt_ctx, uint64_t hash_val, bool &exist);
  int get_mcv_hash_exist_batch(JoinTableCtx &hjt_ctx, int64_t size, uint64_t *hash_vals, 
                               uint16_t *selector, uint16_t selector_cnt);
  int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info);
  int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info);
  int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info);
  int get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info);
  void reset() {
    if (NULL != hash_table_) {
      hash_table_->reset();
    }
    if (NULL != mcv_hash_table_) {
      mcv_hash_table_->reset();
    }
  };
  int64_t get_mem_used() const {
    return hash_table_->get_mem_used() + 
           (mcv_hash_table_ == NULL ? 0 : mcv_hash_table_->get_mem_used());
  }
  void free(ObIAllocator *allocator) {
    if (NULL != hash_table_) {
      hash_table_->free(allocator);
      allocator->free(hash_table_);
      hash_table_ = NULL;
    }
    if (NULL != mcv_hash_table_) {
      mcv_hash_table_->free(allocator);
      allocator->free(mcv_hash_table_);
      mcv_hash_table_ = NULL;
    }
  }
  int64_t get_one_bucket_size() const { return hash_table_->get_one_bucket_size(); }
  int64_t get_normalized_key_size() const { return hash_table_->get_normalized_key_size(); }

  int64_t get_row_count() { return hash_table_->get_row_count(); };
  int64_t get_used_buckets() { return hash_table_->get_used_buckets(); }
  int64_t get_nbuckets() { return hash_table_->get_nbuckets(); }
  int64_t get_collisions() { return hash_table_->get_collisions(); }

private:
  IHashTable *hash_table_;
  IHashTable *mcv_hash_table_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_JOIN_HASH_TABLE_H_*/
