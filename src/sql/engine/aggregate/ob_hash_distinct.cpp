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
#include "sql/engine/aggregate/ob_hash_distinct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "common/row/ob_row.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;

namespace sql {

ObHashDistinct::ObHashDistinctCtx::ObHashDistinctCtx(ObExecContext& exec_ctx)
    : ObPhyOperatorCtx(exec_ctx),
      mem_limit_(0),
      part_count_(0),
      part_mem_limit_(0),
      estimated_mem_usage_(0),
      bucket_bits_(0),
      part_mask_(0),
      bucket_mask_(0),
      part_row_stores_(NULL),
      cur_it_part_(0),
      // it_in_mem_(true),
      hash_tab_(NULL),
      ha_row_store_(NULL),
      ha_is_full_(false),
      bkt_created_(false),
      child_finished_(false),
      state_(HDState::SCAN_CHILD),
      part_level_(0),
      cur_rows_(0),
      pre_mem_used_(0),
      profile_(nullptr),
      sql_mem_processor_(nullptr),
      top_ctx_(nullptr),
      is_top_ctx_(false),
      op_type_(PHY_INVALID),
      op_id_(UINT64_MAX),
      first_read_part_(true)
{
  enable_sql_dumped_ = GCONF.is_sql_operator_dump_enabled() && !(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250);
}

void ObHashDistinct::ObHashDistinctCtx::reset()
{
  reuse();
  mem_limit_ = 0;
  if (OB_NOT_NULL(mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

void ObHashDistinct::ObHashDistinctCtx::reuse()
{
  int ret = OB_SUCCESS;
  part_it_.reset();
  DLIST_FOREACH_REMOVESAFE(node, sub_list_)
  {
    sub_list_.remove(node);
    LOG_TRACE("remove sub ctx", K(node), K(node->data_), K(node->parent_));
    ObHashDistinctCtx* sub_ctx = node->data_;
    sub_ctx->reset();
    sub_ctx->~ObHashDistinctCtx();
    mem_context_->free(sub_ctx);
    mem_context_->free(node);
  }
  if (NULL != part_row_stores_) {
    for (int64_t i = 0; i < part_count_; i++) {
      part_row_stores_[i].reset();
      part_row_stores_[i].~ObChunkRowStore();
    }
    mem_context_->free(part_row_stores_);
    part_row_stores_ = NULL;
  }

  if (NULL != ha_row_store_) {
    ha_row_store_->reset();
    ha_row_store_->~ObChunkRowStore();
    mem_context_->free(ha_row_store_);
    ha_row_store_ = NULL;
  }

  if (hash_tab_ != NULL) {
    hash_tab_->reset();
    if (hash_tab_->mem_context_ == mem_context_) {
      mem_context_->free(hash_tab_);
    }
    hash_tab_ = NULL;
  }
  part_count_ = 0;
  part_mem_limit_ = 0;
  estimated_mem_usage_ = 0;
  bucket_bits_ = 0;
  part_mask_ = 0;
  bucket_mask_ = 0;
  cur_it_part_ = 0;
  // it_in_mem_ = true;
  ha_is_full_ = false;
  bkt_created_ = false;
  child_finished_ = false;
  state_ = HDState::SCAN_CHILD;
  cur_rows_ = 0;
  top_ctx_ = nullptr;
  first_read_part_ = true;
  // only destroy profile if top ctx
  if (is_top_ctx_) {
    destroy_sql_mem_profile();
  }
  profile_ = nullptr;
  sql_mem_processor_ = nullptr;
}

int ObHashDistinct::ObHashDistinctCtx::init_sql_mem_profile(
    ObIAllocator* allocator, int64_t max_mem_size, int64_t cache_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect status: mem_context is null", K(ret));
  } else if (OB_ISNULL(profile_)) {
    if (OB_NOT_NULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect status: sql mem processor is not null", K(ret));
    } else {
      void* buf1 = mem_context_->get_malloc_allocator().alloc(sizeof(ObSqlWorkAreaProfile));
      void* buf2 = mem_context_->get_malloc_allocator().alloc(sizeof(ObSqlMemMgrProcessor));
      if (OB_ISNULL(buf1) || OB_ISNULL(buf2)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      }
      if (OB_SUCC(ret)) {
        profile_ = new (buf1) ObSqlWorkAreaProfile(ObSqlWorkAreaType::HASH_WORK_AREA);
        sql_mem_processor_ = new (buf2) ObSqlMemMgrProcessor(*profile_);
        if (OB_FAIL(sql_mem_processor_->init(allocator,
                exec_ctx_.get_my_session()->get_effective_tenant_id(),
                cache_size,
                op_type_,
                op_id_,
                &exec_ctx_))) {
          LOG_WARN("failed to init profile", K(ret), K(max_mem_size), K(cache_size));
        }
      } else {
        if (OB_NOT_NULL(buf1)) {
          mem_context_->get_malloc_allocator().free(buf1);
        }
        if (OB_NOT_NULL(buf2)) {
          mem_context_->get_malloc_allocator().free(buf2);
        }
      }
    }
  } else {
    if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect status: sql mem processor is null", K(ret));
    } else {
      if (OB_FAIL(sql_mem_processor_->init(allocator,
              exec_ctx_.get_my_session()->get_effective_tenant_id(),
              cache_size,
              op_type_,
              op_id_,
              &exec_ctx_))) {
        LOG_WARN("failed to init profile", K(ret), K(max_mem_size), K(cache_size));
      }
    }
  }
  return ret;
}

void ObHashDistinct::ObHashDistinctCtx::destroy_sql_mem_profile()
{
  if (OB_NOT_NULL(sql_mem_processor_)) {
    if (OB_ISNULL(mem_context_)) {
      LOG_ERROR("mem entity is null", K(lbt()));
    } else {
      sql_mem_processor_->unregister_profile();
      sql_mem_processor_->destroy();
      mem_context_->get_malloc_allocator().free(sql_mem_processor_);
      sql_mem_processor_ = nullptr;
    }
  }
  if (OB_NOT_NULL(profile_)) {
    if (OB_ISNULL(mem_context_)) {
      LOG_ERROR("mem entity is null", K(lbt()));
    } else {
      mem_context_->get_malloc_allocator().free(profile_);
      profile_ = nullptr;
    }
  }
}

int ObHashDistinct::ObHashDistinctCtx::estimate_memory_usage(int64_t input, int64_t mem_limit, int64_t& ha_mem_limit)
{
  int ret = OB_SUCCESS;
  const int64_t part_size = ObChunkRowStore::BLOCK_SIZE + sizeof(ObChunkRowStore);
  const int64_t min_dump_mem = part_size * MIN_PART_COUNT;
  if (!ha_is_full_) {
    if (mem_limit < min_dump_mem) {
      part_count_ = MIN_PART_COUNT;
    } else {
      // reserve 1/8 for security
      part_count_ = input / (std::max(mem_limit * 7 / 8, 1L));
      part_count_ = next_pow2(part_count_);
      part_count_ = std::min(std::max(part_count_, (int64_t)MIN_PART_COUNT), (int64_t)MAX_PART_COUNT);
    }
  }
  const int64_t dump_mem = part_count_ * part_size;
  ha_mem_limit = std::max(mem_limit - dump_mem, dump_mem) * 7 / 8;
  part_mem_limit_ = ObChunkRowStore::BLOCK_SIZE;
  estimated_mem_usage_ = input;
  profile_->set_basic_info(input, input, input);
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::estimate_memory_usage_for_partition(
    int64_t input, int64_t mem_limit, int64_t& ha_mem_limit)
{
  int ret = OB_SUCCESS;
  // save 12% mem for safety
  mem_limit *= 0.88;

  if (input <= mem_limit * 0.88) {
    ha_mem_limit = mem_limit * 0.88;
    part_mem_limit_ = ObChunkRowStore::BLOCK_SIZE;
    part_count_ = 0;
    estimated_mem_usage_ = input;
  } else if (estimate_memory_usage(input, mem_limit, ha_mem_limit)) {
    LOG_WARN("mem limit too small for partition", K(ret), K(input), K(mem_limit));
  }

  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::init_hash_table(
    const ObSQLSessionInfo* session, int64_t bucket_size, int64_t mem_limit)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  char* rs_buf = NULL;
  int64_t size_buf_init = 0;
  if (bucket_size > 0) {
    size_buf_init = ObHashDistinctCtx::HashTable::BUCKET_BUF_SIZE * sizeof(ObHashDistinctCtx::HashRow);
    ObMemAttr attr;
    attr.label_ = "InitHashTable";
    if (OB_ISNULL(buf = static_cast<char*>(mem_context_->allocf(sizeof(ObHashDistinctCtx::HashTable), attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc buffer", K(ret));
    } else {
      hash_tab_ = new (buf) ObHashDistinctCtx::HashTable;
      hash_tab_->mem_context_ = mem_context_;
      if (OB_FAIL(hash_tab_->buckets_.init(bucket_size))) {
        LOG_WARN("Failed to alloc buffer", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(hash_tab_->mem_context_->allocp(size_buf_init)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc buffer", K(ret));
      } else if (OB_FAIL(hash_tab_->buckets_buf_.prepare_allocate(1))) {
        LOG_WARN("Failed to alloc buffer for hashrow", K(ret));
      } else {
        hash_tab_->buf_cnt_ = ObHashDistinctCtx::HashTable::BUCKET_BUF_SIZE;
        hash_tab_->buckets_buf_.at(0) = new (buf) ObHashDistinct::ObHashDistinctCtx::HashRow[hash_tab_->buf_cnt_];
        hash_tab_->cur_ = 0;
        hash_tab_->nbuckets_ = bucket_size;
      }
    }
  }
  if (OB_SUCC(ret) && 0 < mem_limit) {
    ObMemAttr attr;
    attr.label_ = "InitHashTable";
    if (OB_ISNULL(rs_buf = static_cast<char*>(mem_context_->allocf(sizeof(ObChunkRowStore), attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc buffer", K(ret));
    } else {
      ha_row_store_ = new (rs_buf) ObChunkRowStore(&mem_context_->get_malloc_allocator());
      if (OB_FAIL(ha_row_store_->init(0,
              session->get_effective_tenant_id(),
              ObCtxIds::WORK_AREA,
              ObModIds::OB_SQL_HASH_DIST_ROW_STORE,
              false,
              ObChunkRowStore::FULL,
              sizeof(uint64_t)))) {
        LOG_WARN("Failed to init ha_row_store_", K(ret));
      } else {
        ha_row_store_->set_dir_id(sql_mem_processor_->get_dir_id());
        ha_row_store_->set_callback(sql_mem_processor_);
        LOG_TRACE("init hash row store", K(ha_row_store_), K(mem_limit));
      }
    }
  } else {
    ha_row_store_ = NULL;
  }
  LOG_TRACE("hash distinct init hash table", K(ret), K(bucket_size), K(size_buf_init), K(mem_limit));
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::init_partitions_info(ObSQLSessionInfo* session)
{
  int ret = OB_SUCCESS;
  int64 size_part = part_count_ * sizeof(ObChunkRowStore);
  char* buf_part = NULL;
  ObMemAttr attr;
  attr.label_ = "InitPartInfo";
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ret));
  } else if (OB_ISNULL(buf_part = static_cast<char*>(mem_context_->allocf(size_part, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc buffer", K(ret));
  } else {
    part_row_stores_ = new (buf_part) ObChunkRowStore;
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
      ObChunkRowStore* part_row_store = new (buf_part) ObChunkRowStore(&mem_context_->get_malloc_allocator());
      if (OB_FAIL(part_row_store->init(1,
              session->get_effective_tenant_id(),
              ObCtxIds::WORK_AREA,
              ObModIds::OB_SQL_CHUNK_ROW_STORE,
              true,
              ObChunkRowStore::FULL,
              sizeof(uint64_t)))) {
        LOG_WARN("fail to create hash group buckets", K(ret));
      } else {
        part_row_store->set_dir_id(sql_mem_processor_->get_dir_id());
        part_row_store->set_callback(sql_mem_processor_);
      }
      buf_part += sizeof(ObChunkRowStore);
    }
    LOG_TRACE(
        "trace start partition dump", K(ret), K(part_count_), K(pre_mem_used_), K(pre_mem_used_ + get_mem_used()));
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::set_hash_info(int64_t part_count, int64_t n_bucket, int64_t level)
{
  int ret = OB_SUCCESS;
  if (!is2n(n_bucket)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket num", K(n_bucket));
  } else {
    bucket_bits_ = log2(n_bucket);
    bucket_mask_ = n_bucket - 1;
    part_mask_ = (next_pow2(part_count - 1) - 1) << (bucket_bits_ + level);
    //      LOG_WARN("Molly", K(n_bucket), K_(part_mask), K_(bucket_mask),K_(bucket_bits), K(level));
  }
  return ret;
}

inline bool ObHashDistinct::ObHashDistinctCtx::equal_with_hashrow(
    const ObHashDistinct* dist, const common::ObNewRow* row, uint64_t hash_val, const void* other) const
{
  bool are_equal = true;
  if (!OB_ISNULL(other)) {
    const HashRow* other_row = static_cast<const HashRow*>(other);
    if (hash_val != *(static_cast<int64_t*>(other_row->stored_row_->get_extra_payload()))) {
      are_equal = false;
    } else {
      int64_t nth_cell = 0;
      for (int64_t idx = 0; are_equal && idx < dist->distinct_columns_.count(); ++idx) {
        if (NULL == row->projector_ || 0 == row->projector_size_) {
          nth_cell = dist->distinct_columns_.at(idx).index_;
        } else {
          nth_cell = row->projector_[dist->distinct_columns_.at(idx).index_];
        }
        const ObObj& cell = other_row->stored_row_->cells()[nth_cell];
        if (!cell.is_equal(row->cells_[nth_cell], dist->distinct_columns_.at(idx).cs_type_)) {
          are_equal = false;
        }
      }
    }
  } else {
    are_equal = true;
  }
  return are_equal;
}

bool ObHashDistinct::ObHashDistinctCtx::in_hash_tab(
    const ObHashDistinct* dist, const common::ObNewRow* row, const uint64_t hash_val, const uint64_t bucket_idx) const
{
  bool has_value = false;
  ObHashDistinctCtx::HashRow* cur = hash_tab_->buckets_.at(bucket_idx);

  while (!has_value && cur != NULL) {
    if (equal_with_hashrow(dist, row, hash_val, cur)) {
      has_value = true;
    }
    cur = cur->next_tuple_;
  }

  return has_value;
}

int ObHashDistinct::ObHashDistinctCtx::insert_hash_tab(const ObChunkRowStore::StoredRow* sr, const uint64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  HashRow* cur = hash_tab_->buckets_.at(bucket_idx);
  HashRow* new_row = NULL;
  int64_t nth_buf = hash_tab_->cur_ >> HashTable::BUCKET_SHIFT;
  int64_t nth_slot = hash_tab_->cur_ & HashTable::BUCKET_MASK;
  if (hash_tab_->cur_ < hash_tab_->buf_cnt_) {
    new_row = &hash_tab_->buckets_buf_.at(nth_buf)[nth_slot];
  } else {
    char* buf = NULL;
    int64_t size_buf = HashTable::BUCKET_BUF_SIZE * sizeof(ObHashDistinctCtx::HashRow);
    if (nth_slot != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(ret), K_(hash_tab), K(nth_slot), K(nth_buf));
    } else if (OB_FAIL(hash_tab_->buckets_buf_.prepare_allocate(nth_buf + 1))) {
      LOG_WARN("Failed to alloc buffer", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(hash_tab_->mem_context_->allocp(size_buf)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc buffer", K(ret));
    } else {
      hash_tab_->buf_cnt_ += HashTable::BUCKET_BUF_SIZE;
      hash_tab_->buckets_buf_.at(nth_buf) = new (buf) HashRow[HashTable::BUCKET_BUF_SIZE];
      new_row = &hash_tab_->buckets_buf_.at(nth_buf)[0];
    }
  }
  if (OB_UNLIKELY(OB_ISNULL(new_row))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed for ha", K(ret), KP(new_row), K(hash_tab_));
  } else {
    new_row->next_tuple_ = cur;
    new_row->stored_row_ = sr;
    hash_tab_->buckets_.at(bucket_idx) = new_row;
    hash_tab_->cur_++;
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::add_row(const ObNewRow* row, ObChunkRowStore::StoredRow** sr, bool& ha_is_full)
{
  int ret = OB_SUCCESS;
  int64_t mem_used = pre_mem_used_ + get_mem_used();
  int64_t data_size = get_data_size();
  if (data_size > sql_mem_processor_->get_data_ratio() * sql_mem_processor_->get_mem_bound() || ha_is_full) {
    ha_is_full = false;
    calc_data_ratio();
    if (OB_FAIL(sql_mem_processor_->extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t mem_size) {
              UNUSED(mem_size);
              return data_size > sql_mem_processor_->get_data_ratio() * sql_mem_processor_->get_mem_bound();
            },
            ha_is_full,
            mem_used,
            16))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    } else if (OB_FAIL(update_used_mem_bound(mem_used))) {
      LOG_WARN("failed to update used mem bound", K(ret));
    }
    LOG_TRACE("need dump",
        K(data_size),
        K(mem_used),
        K(ha_is_full),
        K(profile_->get_expect_size()),
        K(profile_->get_cache_size()),
        K(sql_mem_processor_->get_data_ratio()));
  }
  *sr = nullptr;
  if (OB_FAIL(ret)) {
  } else if (ha_is_full && !enable_sql_dumped_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("exceed memory limit", K(ret));
  } else if (!ha_is_full && OB_FAIL(ha_row_store_->add_row(*row, sr))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    LOG_WARN("failed to add row", K(ret), K(sql_mem_processor_->get_data_ratio()), K(mem_used));
  }
  return ret;
}

void ObHashDistinct::ObHashDistinctCtx::calc_data_ratio()
{
  int64_t mem_data = pre_mem_used_ + get_mem_used();
  int64_t data_size = get_data_size();
  if (0 == data_size) {
    sql_mem_processor_->set_data_ratio(0.6);
  } else {
    if (data_size > mem_data) {
      sql_mem_processor_->set_data_ratio(0.6);
      LOG_TRACE("unexpected status: data size exceed total size", K(data_size), K(mem_data));
    } else {
      double data_ratio = data_size * 1.0 / mem_data;
      if (data_size >= mem_data) {
        sql_mem_processor_->set_data_ratio(0.8);
      } else {
        sql_mem_processor_->set_data_ratio(data_size * 1.0 / mem_data);
      }
      LOG_TRACE("calc data ratio", K(data_size), K(mem_data), K(data_ratio));
    }
  }
}

int ObHashDistinct::ObHashDistinctCtx::update_used_mem_bound(int64_t mem_used)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sql_mem_processor_)) {
    int64_t hash_mem_bound = 0;
    if (OB_FAIL(sql_mem_processor_->update_used_mem_size(mem_used))) {
      LOG_WARN("failed to update used mem size", K(ret));
    } else if (OB_FAIL(
                   estimate_memory_usage(profile_->get_input_size(), profile_->get_expect_size(), hash_mem_bound))) {
      LOG_WARN("failed to estimate memory usage", K(ret), K(profile_->get_expect_size()));
    } else {
      calc_data_ratio();
      LOG_TRACE("hash distinct update used memory size",
          K(hash_mem_bound),
          K(profile_->get_expect_size()),
          K(profile_->get_cache_size()),
          K(sql_mem_processor_->get_data_ratio()),
          K(part_count_));
    }
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::update_mem_status_periodically()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
          &mem_context_->get_malloc_allocator(), [&](int64_t cur_cnt) { return cur_rows_ > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(cur_rows_));
  } else if (updated) {
    if (OB_FAIL(update_used_mem_bound(pre_mem_used_ + get_mem_used()))) {
      LOG_WARN("failed to update used mem bound", K(ret));
    } else {
      LOG_TRACE("trace update mem status",
          K(cur_rows_),
          K(pre_mem_used_),
          K(get_mem_used()),
          K(sql_mem_processor_->get_data_ratio()));
    }
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::process_rows_from_child(
    const ObHashDistinct* dist, const ObNewRow*& row, bool& got_distinct_row)
{
  int ret = OB_SUCCESS;
  const ObNewRow* cur_row = NULL;
  uint64_t hash_value = 0;
  got_distinct_row = false;

  if (!child_finished_) {
    while (OB_SUCC(ret) && !got_distinct_row) {
      ++cur_rows_;
      if (OB_FAIL(dist->child_op_->get_next_row(exec_ctx_, cur_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("child operator get next row failed", K(ret));
        } else {
          child_finished_ = true;
          hash_tab_->reuse();
          if (!dist->is_block_mode_) {
            ha_row_store_->reuse();
          }
          LOG_TRACE("iter end", K(ret), K(ha_is_full_), K(op_id_));
        }
      } else if (OB_ISNULL(cur_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cur_row is null", K(ret));
      } else if (!ha_is_full_ && OB_FAIL(update_mem_status_periodically())) {
        LOG_WARN("failed to update max usable memory size periodically", K(ret), K(cur_rows_));
      } else if (OB_FAIL(dist->get_hash_value(cur_row, hash_value))) {
        LOG_WARN("get hash_value failed", K(ret));
      } else {
        int64_t bucket_idx = get_bucket_idx(hash_value);
        if (!in_hash_tab(dist, cur_row, hash_value, bucket_idx)) {
          ObChunkRowStore::StoredRow* sr = nullptr;
          if (!ha_is_full_) {
            if (OB_FAIL(add_row(cur_row, &sr, ha_is_full_))) {
              LOG_WARN("failed to add row", K(ret));
            } else if (ha_is_full_) {
              sql_mem_processor_->set_number_pass(part_level_ + 1);
            } else if (OB_FAIL(insert_hash_tab(sr, bucket_idx))) {
              LOG_WARN("failed to insert ha", K(ret));
            } else {
              *(static_cast<int64_t*>(sr->get_extra_payload())) = hash_value;
              if (!dist->is_block_mode_) {
                got_distinct_row = true;
                row = cur_row;
              }
            }
          }
          // put into partitions
          if (ha_is_full_ && !got_distinct_row && OB_SUCC(ret)) {
            if (OB_UNLIKELY(OB_ISNULL(part_row_stores_) && OB_FAIL(init_partitions_info(exec_ctx_.get_my_session())))) {
              LOG_WARN("fail to create hash group buckets", K(ret));
            } else {
              int64_t part_idx = get_part_idx(hash_value);
              if (OB_FAIL(part_row_stores_[part_idx].add_row(*cur_row, &sr))) {
                LOG_WARN("add row to ChunkRowStore failed", K(ret), K(bucket_idx), K(part_idx));
              } else {
                *(static_cast<int64_t*>(sr->get_extra_payload())) = hash_value;
              }
            }
          }
        }
      }  // end of while
    }
  } else {
    ret = OB_ITER_END;
    LOG_TRACE("get next when child ends", K(ret), K(dist->is_block_mode_));
  }
  return ret;
}

int64_t ObHashDistinct::ObHashDistinctCtx::get_total_based_mem_used()
{
  int ret = OB_SUCCESS;
  int64_t mem_used = 0;
  if (OB_NOT_NULL(top_ctx_)) {
    DLIST_FOREACH(node, top_ctx_->sub_list_)
    {
      ObHashDistinctCtx* ctx = node->data_;
      if (OB_NOT_NULL(ctx) && ctx != this && OB_NOT_NULL(ctx->mem_context_)) {
        mem_used += ctx->get_mem_used();
      }
    }
    mem_used += top_ctx_->get_mem_used();
  } else {
    mem_used += get_mem_used();
  }
  return mem_used;
}

int ObHashDistinct::ObHashDistinctCtx::open_cur_partition(ObChunkRowStore* row_store)
{
  int ret = OB_SUCCESS;
  int chunk_size = (NULL == ha_row_store_) ? row_store->get_file_size()
                                           : (ha_row_store_->get_mem_limit() > OB_DEFAULT_MACRO_BLOCK_SIZE
                                                     ? OB_DEFAULT_MACRO_BLOCK_SIZE
                                                     : ha_row_store_->get_mem_limit());
  if (OB_FAIL(row_store->begin(part_it_, chunk_size))) {
    LOG_WARN("begin iterator for crs failed", K(ret), K(chunk_size), K_(ha_row_store));
  } else {
    LOG_TRACE("begin row store iterator", K(chunk_size), K(cur_it_part_));
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::process_one_partition(const ObHashDistinct* dist, ObChunkRowStore* row_store,
    ObSQLSessionInfo* my_session, const ObNewRow*& row, bool& got_distinct_row)
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  uint64_t hash_value = 0;
  const ObChunkRowStore::StoredRow* sr;
  got_distinct_row = false;
  while (OB_SUCC(ret) && !got_distinct_row) {
    ++cur_rows_;
    if (OB_FAIL(part_it_.get_next_row(sr))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("child operator get next row failed", K(ret));
      } else {
        child_finished_ = true;
        part_it_.reset();
        hash_tab_->reuse();
        if (ha_row_store_ != NULL) {
          ha_row_store_->reuse();
        }
        row_store->reset();
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = sql_mem_processor_->update_used_mem_size(pre_mem_used_ + get_mem_used()))) {
          LOG_WARN("failed to update used mem size", K(ret), K(tmp_ret));
        }
      }
    } else if (OB_ISNULL(sr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null", K(ret));
    } else {
      hash_value = *(static_cast<int64_t*>(sr->get_extra_payload()));
      int64_t bucket_idx = get_bucket_idx(hash_value);
      if (OB_FAIL(!ha_is_full_ && update_mem_status_periodically())) {
        LOG_WARN("failed to update max usable memory size periodically", K(ret), K(cur_rows_));
      } else if (OB_FAIL(part_it_.convert_to_row(sr, cur_row))) {
        LOG_WARN("failed to convert row", K(ret), K(cur_rows_));
      } else if (!in_hash_tab(dist, cur_row, hash_value, bucket_idx)) {
        ObChunkRowStore::StoredRow* new_sr = nullptr;
        if (!ha_is_full_) {
          if (OB_NOT_NULL(ha_row_store_)) {
            if (OB_FAIL(add_row(cur_row, &new_sr, ha_is_full_))) {
              LOG_WARN("failed to add row", K(ret));
            } else if (ha_is_full_) {
              sql_mem_processor_->set_number_pass(part_level_ + 1);
            } else if (OB_SUCC(insert_hash_tab(new_sr, bucket_idx))) {
              got_distinct_row = true;
              row = cur_row;
              *(static_cast<int64_t*>(new_sr->get_extra_payload())) = hash_value;
            }
          } else {
            if (OB_SUCC(insert_hash_tab(sr, bucket_idx))) {
              got_distinct_row = true;
              row = cur_row;
            }
          }
        }
        // put into partitions
        if (ha_is_full_ && !got_distinct_row && OB_SUCC(ret)) {
          if (OB_UNLIKELY(OB_ISNULL(part_row_stores_) && OB_FAIL(init_partitions_info(my_session)))) {
            LOG_WARN("fail to create hash group buckets", K(ret));
          } else {
            int64_t part_idx = get_part_idx(hash_value);
            if (OB_FAIL(part_row_stores_[part_idx].copy_row(sr))) {
              LOG_WARN("add row to ChunkRowStore failed", K(ret), K(bucket_idx), K(part_idx));
            }
          }
        }
      } else {
      }
    }
  }  // end of while
  return ret;
}

// get next dumped partition
int ObHashDistinct::ObHashDistinctCtx::get_next_partition(ObHashDistinctCtx* top_ctx, PartitionLinkNode*& link_node)
{
  ObChunkRowStore* store;
  int64_t cur_part = cur_it_part_;
  int ret = OB_SUCCESS;
  bool has_next_rs = false;
  link_node = nullptr;
  if (NULL != part_row_stores_) {
    while (!has_next_rs && (cur_part < part_count_)) {
      store = &part_row_stores_[cur_part];
      if (store->is_inited() && store->has_dumped()) {
        has_next_rs = true;
        LOG_TRACE("trace get partition", K(ret), K(store->get_row_cnt()), K(cur_part), K(op_id_));
        break;
      }
      cur_part++;
    }
  }
  if (has_next_rs) {
    ObMemAttr attr;
    attr.label_ = "HashDistCtx";
    char* buf = static_cast<char*>(top_ctx->mem_context_->allocf(sizeof(ObHashDistinctCtx), attr));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc buffer", K(ret));
    } else {
      attr.label_ = "PartLinkNode";
      ObHashDistinctCtx* new_ctx = new (buf) ObHashDistinctCtx(this->exec_ctx_);
      if (OB_FAIL(new_ctx->assign_sub_ctx(this, store, top_ctx))) {
        LOG_WARN("fail to assign sub ctx", K(ret), K(store), K(this));
      } else if (OB_ISNULL(buf = static_cast<char*>(top_ctx->mem_context_->allocf(sizeof(PartitionLinkNode), attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc buffer", K(ret));
      } else {
        link_node = new (buf) PartitionLinkNode(new_ctx, this);
        top_ctx->sub_list_.add_first(link_node);
        cur_it_part_ = cur_part;
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(new_ctx)) {
        new_ctx->reset();
        top_ctx->mem_context_->free(new_ctx);
        new_ctx = nullptr;
      }
    }
  } else {
    ret = OB_ITER_END;
    LOG_TRACE("no more data in partitions", K(part_level_), K(this), K(top_ctx));
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::update_used_mem_size()
{
  return (nullptr == sql_mem_processor_) ? OB_SUCCESS : sql_mem_processor_->update_used_mem_size(pre_mem_used_);
}

int ObHashDistinct::ObHashDistinctCtx::out_put_ha_rows(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row;
  if (OB_FAIL(part_it_.get_next_row(cur_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get stored row from partition", K(ret));
    } else {
      part_it_.reset();
      ha_row_store_->reuse();
    }
  } else {
    row = cur_row;
  }
  return ret;
}
// store row and set refacotred
int ObHashDistinct::ObHashDistinctCtx::get_next_mem_partition()
{
  int ret = OB_ITER_END;
  if (part_row_stores_ != NULL) {
    ObChunkRowStore* store = nullptr;
    while (cur_it_part_ < part_count_) {
      store = &part_row_stores_[cur_it_part_];
      if (store->is_inited() && !store->has_dumped()) {
        if (OB_FAIL(store->begin(part_it_))) {
          LOG_WARN("begin iterator for crs failed", K(ret));
        } else {
          LOG_TRACE("scan part", K(cur_it_part_));
          ret = OB_SUCCESS;
        }
        break;
      }
      cur_it_part_++;
    }
  }
  if (OB_ITER_END == ret) {
    // it_in_mem_ = false;
    cur_it_part_ = 0;
    hash_tab_->reuse();
    ha_row_store_->reuse();
    first_read_part_ = true;
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::get_dist_row_from_in_mem_partitions(
    const ObHashDistinct* dist, const ObNewRow*& row, bool& got_distinct_row)
{
  int ret = OB_SUCCESS;
  const ObChunkRowStore::StoredRow* sr = NULL;
  ObNewRow* cur_row = NULL;
  uint64_t hash_value = 0;
  uint64_t bucket_idx = 0;
  ObChunkRowStore* store;
  if (first_read_part_) {
    first_read_part_ = false;
    if (OB_FAIL(get_next_mem_partition())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next partition in memory", K(ret));
      }
    }
  }
  got_distinct_row = false;
  while (!got_distinct_row && OB_SUCC(ret)) {
    // block: process child -> child iter end -> return all row(ha_row_store) -> process in mem partition
    // non block: process child -> ret one row -> child iter end ->process in mem partiton
    while (!got_distinct_row && OB_SUCC(ret)) {
      if (OB_FAIL(part_it_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get stored row from partition", K(ret));
        } else {
          LOG_DEBUG("partition in-mem finished:", K(cur_it_part_));
          part_it_.reset();
          part_row_stores_[cur_it_part_].reset();
          hash_tab_->reuse();
          cur_it_part_++;
          if (OB_FAIL(get_next_mem_partition())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next partition in memory", K(ret));
            }
          }
        }
      } else {
        hash_value = *(static_cast<int64_t*>(sr->get_extra_payload()));
        bucket_idx = get_bucket_idx(hash_value);
        if (OB_FAIL(part_it_.convert_to_row(sr, cur_row))) {
          LOG_WARN("failed to convert row", K(ret));
        } else if (!in_hash_tab(dist, cur_row, hash_value, bucket_idx)) {
          if (OB_FAIL(insert_hash_tab(sr, bucket_idx))) {
            LOG_WARN("failed to insert ha", K(ret));
          } else {
            got_distinct_row = true;
            row = cur_row;
          }
        }
      }
    }
  }
  return ret;
}

int ObHashDistinct::ObHashDistinctCtx::clean_up_partitions()
{
  int ret = OB_SUCCESS;
  if (part_row_stores_ != NULL) {
    int64_t total_dump_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; i++) {
      if (OB_FAIL(part_row_stores_[i].finish_add_row())) {
        LOG_WARN("Failed to finish partition", K(ret), K(this), K(i));
      } else {
        total_dump_row_cnt += part_row_stores_[i].get_row_cnt();
        LOG_TRACE("finished", K(i), K(part_row_stores_[i].get_row_cnt()));
      }
    }
    LOG_TRACE("dump total row count", K(total_dump_row_cnt), K(op_id_));
  }
  return ret;
}

inline int64_t ObHashDistinct::ObHashDistinctCtx::get_partitions_mem_usage()
{
  int64_t mem_used = 0;
  for (int64_t i = 0; i < part_count_; i++) {
    mem_used += part_row_stores_[i].get_mem_hold();
  }
  return mem_used;
}

int ObHashDistinct::ObHashDistinctCtx::assign_sub_ctx(
    ObHashDistinctCtx* parent, ObChunkRowStore* parent_row_stores, ObHashDistinctCtx* top_ctx)
{
  int ret = OB_SUCCESS;
  profile_ = top_ctx->profile_;
  sql_mem_processor_ = top_ctx->sql_mem_processor_;
  op_type_ = top_ctx->op_type_;
  op_id_ = top_ctx->op_id_;
  enable_sql_dumped_ = top_ctx->enable_sql_dumped_;
  top_ctx_ = top_ctx;
  pre_mem_used_ = get_total_based_mem_used();
  estimated_mem_usage_ = parent_row_stores->get_file_size();
  // ha_row_store_ = parent->ha_row_store_;
  part_level_ = parent->part_level_ + 1;
  bkt_created_ = parent->bkt_created_;
  lib::ContextParam param;
  param
      .set_mem_attr(parent->exec_ctx_.get_my_session()->get_effective_tenant_id(),
          ObModIds::OB_SQL_HASH_DIST,
          ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("fail to create entity", K(ret));
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create entity ", K(ret));
  } else {
    int64_t ha_mem = 0;
    int64_t mem_limit = parent->mem_limit_;
    mem_limit_ = parent->mem_limit_;
    if (0 == mem_limit) {
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(ObSqlWorkAreaType::HASH_WORK_AREA,
              parent->exec_ctx_.get_my_session()->get_effective_tenant_id(),
              mem_limit))) {
        LOG_WARN("failed to get workarea size", K(ret));
      } else if (OB_FAIL(init_sql_mem_profile(
                     &mem_context_->get_malloc_allocator(), mem_limit, pre_mem_used_ * 2 + estimated_mem_usage_))) {
        LOG_WARN("failed to init sql mem profile", K(ret));
      } else if (OB_ISNULL(sql_mem_processor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect status: sql mem processor is null", K(ret));
      } else {
        mem_limit = sql_mem_processor_->get_mem_bound();
      }
      mem_limit -= pre_mem_used_;
    } else {
      // set mem limit for test
      mem_limit -= parent->mem_context_->used();
    }
    if (OB_FAIL(ret)) {
    } else if (mem_limit >= estimated_mem_usage_) {
      // all in mem, no need to alloc sub CRS
      ha_row_store_ = NULL;
      part_count_ = 1;
    } else if (OB_FAIL(estimate_memory_usage_for_partition(estimated_mem_usage_, mem_limit, ha_mem))) {
      LOG_WARN("assign sub ctx failed", K(ret), K_(estimated_mem_usage), K_(mem_limit), K(ha_mem));
    }
    int64_t bucket_num = 0;
    // if the parent's hash_tab_ is big enough for cur partition, reuse it
    if (parent->hash_tab_->nbuckets_ == MAX_BUCKET_COUNT ||
        parent->hash_tab_->nbuckets_ >= parent_row_stores->get_row_cnt()) {
      hash_tab_ = parent->hash_tab_;
      hash_tab_->reuse();
      LOG_DEBUG("reuse hash table buckets", K_(hash_tab_->nbuckets), K(parent_row_stores->get_row_cnt()));
    } else {
      get_bucket_size(
          parent_row_stores->get_row_cnt(), mem_limit, 1 - sql_mem_processor_->get_data_ratio(), bucket_num);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_hash_table(parent->exec_ctx_.get_my_session(), bucket_num, ha_mem))) {
      LOG_WARN("init hash failed", K(ret), K_(estimated_mem_usage), K_(mem_limit), K(ha_mem));
    } else if (OB_FAIL(set_hash_info(part_count_, hash_tab_->nbuckets_, part_level_))) {
      LOG_WARN("Failed to set hash info", K(ret), K_(part_count), K_(hash_tab_->nbuckets));
    }
    LOG_TRACE("trace create new partition ctx", K(ret), K(part_level_), K(this));
  }
  // exec_ctx_ = parent->exec_ctx_;
  return ret;
}

ObHashDistinct::ObHashDistinct(common::ObIAllocator& alloc) : ObDistinct(alloc), mem_limit_(0)
{}

ObHashDistinct::~ObHashDistinct()
{}

void ObHashDistinct::reset()
{
  ObDistinct::reset();
}

void ObHashDistinct::reuse()
{
  ObDistinct::reuse();
}

int ObHashDistinct::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObHashDistinctCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
    lib::MemoryContext* mem_context = nullptr;
    lib::ContextParam param;
    param.set_mem_attr(ctx.get_my_session()->get_effective_tenant_id(), ObModIds::OB_SQL_HASH_DIST, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (OB_ISNULL(mem_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create entity ", K(ret));
    } else {
      static_cast<ObHashDistinctCtx*>(op_ctx)->mem_context_ = mem_context;
      static_cast<ObHashDistinctCtx*>(op_ctx)->mem_limit_ = mem_limit_;
      static_cast<ObHashDistinctCtx*>(op_ctx)->is_top_ctx_ = true;
      static_cast<ObHashDistinctCtx*>(op_ctx)->op_type_ = get_type();
      static_cast<ObHashDistinctCtx*>(op_ctx)->op_id_ = get_id();
      LOG_TRACE("distinct block mode", K(is_block_mode_), K(get_id()));
    }
  }
  return ret;
}

int ObHashDistinct::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

// mem_need for further memory management use
int ObHashDistinct::estimate_memory_usage(
    ObHashDistinctCtx* hash_ctx, int64_t memory_limit, int64_t& ha_mem, int64_t& mem_need) const
{
  int ret = OB_SUCCESS;
  int64_t input = 0;
  int64_t mem_limit = memory_limit;
  int64_t input_rows = child_op_->get_rows();
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
          &hash_ctx->exec_ctx_, px_est_size_factor_, child_op_->get_rows(), input_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  }
  input = input_rows * child_op_->get_width();
  input += ceil(input / ObChunkRowStore::BLOCK_SIZE) * sizeof(ObChunkRowStore);
  input += input_rows * sizeof(ObChunkRowStore::StoredRow);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hash_ctx->estimate_memory_usage(input, mem_limit, ha_mem))) {
  } else if (input > mem_limit) {
    mem_need = (ha_mem * 2) / 0.88;
  } else {
    mem_need = input / 0.88;
  }

  return ret;
}

int ObHashDistinct::get_bucket_size(
    int64_t est_bucket, int64_t max_bound_size, double extra_ratio, int64_t& bucket_size)
{
  int ret = OB_SUCCESS;
  static_assert(MIN_BUCKET_COUNT < MAX_BUCKET_COUNT, "MIN_BUCKET_COUNT >= MAX_BUCKET_COUNT");
  bucket_size = estimate_mem_size_by_rows(est_bucket);
  int64_t max_bucket_size = max_bound_size * extra_ratio;
  if (0 < max_bucket_size) {
    int64_t bucket_mem_size = bucket_size * sizeof(ObHashDistinctCtx::HashRow*);
    while (bucket_mem_size > max_bucket_size) {
      bucket_mem_size >>= 1;
    }
    bucket_size = bucket_mem_size / sizeof(ObHashDistinctCtx::HashRow*);
  } else {
    bucket_size = MIN_BUCKET_COUNT;
  }
  if (bucket_size < MIN_BUCKET_COUNT) {
    bucket_size = MIN_BUCKET_COUNT;
  } else if (bucket_size > MAX_BUCKET_COUNT) {
    bucket_size = MAX_BUCKET_COUNT;
  } else {
  }  // do nothing
  LOG_TRACE("trace hash distinct bucket size",
      K(max_bucket_size),
      K(bucket_size),
      K(max_bound_size),
      K(extra_ratio),
      K(est_bucket));
  return ret;
}

int ObHashDistinct::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashDistinctCtx* distinct_ctx = NULL;

  if (OB_ISNULL(distinct_ctx = GET_PHY_OPERATOR_CTX(ObHashDistinctCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct ctx is null");
  } else if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    distinct_ctx->reuse();
  }
  return ret;
}

int ObHashDistinct::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashDistinctCtx* distinct_ctx = NULL;
  if (OB_ISNULL(distinct_ctx = GET_PHY_OPERATOR_CTX(ObHashDistinctCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    distinct_ctx->reset();
  }
  return ret;
}

int ObHashDistinct::get_hash_value(const ObNewRow* row, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = 0;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < distinct_columns_.count(); ++idx) {
    const ObObj& cell = row->get_cell(distinct_columns_.at(idx).index_);
    if (cell.is_null()) {
      hash_value = cell.hash(hash_value);
    } else if (cell.is_string_type()) {
      hash_value = cell.varchar_hash(distinct_columns_.at(idx).cs_type_, hash_value);
    } else {
      hash_value = cell.hash(hash_value);
    }
  }
  return ret;
}

// only once
int ObHashDistinct::init_ha(const ObSQLSessionInfo* session, ObHashDistinctCtx* hash_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = 0;
  int64_t hash_tab_mem = 0;
  int64_t estimated_mem_usage = 0;
  int64_t mem_limit = mem_limit_;
  int64_t row_count = get_rows();
  if (0 == mem_limit_ && OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(ObSqlWorkAreaType::HASH_WORK_AREA,
                             hash_ctx->exec_ctx_.get_my_session()->get_effective_tenant_id(),
                             mem_limit))) {
    LOG_WARN("failed to get workarea size", K(ret));
  } else if (OB_FAIL(
                 ObPxEstimateSizeUtil::get_px_size(&hash_ctx->exec_ctx_, px_est_size_factor_, get_rows(), row_count))) {
    LOG_WARN("failed to get px size", K(ret));
  }
  int64_t extra_mem_size = estimate_mem_size_by_rows(row_count) + ObChunkRowStore::BLOCK_SIZE;
  int64_t tmp_estimated_mem_size = extra_mem_size + row_count * get_width();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hash_ctx->init_sql_mem_profile(
                 &hash_ctx->mem_context_->get_malloc_allocator(), mem_limit, tmp_estimated_mem_size))) {
    LOG_WARN("failed to init sql mem profile", K(ret));
  } else if (OB_ISNULL(hash_ctx->sql_mem_processor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect status: sql mem processor is null", K(ret));
  } else {
    mem_limit = hash_ctx->sql_mem_processor_->get_mem_bound();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(estimate_memory_usage(hash_ctx, mem_limit, hash_tab_mem, estimated_mem_usage))) {
    LOG_WARN("Failed to get memory usage", K(ret));
  } else if (OB_FAIL(
                 get_bucket_size(row_count, mem_limit, extra_mem_size * 1. / tmp_estimated_mem_size, bucket_size))) {
    LOG_WARN("Failed to get bucket size", K(ret));
  } else if (OB_FAIL(hash_ctx->init_hash_table(session, bucket_size, hash_tab_mem))) {
    LOG_WARN("Failed to init hash table Ha", K(ret));
  } else if (OB_FAIL(hash_ctx->set_hash_info(hash_ctx->part_count_, bucket_size, hash_ctx->part_level_))) {
    LOG_WARN("Failed to set hash info", K(ret));
  } else {
    hash_ctx->bkt_created_ = true;
  }
  return ret;
}

// If bkt_created_ false, create bucket fist.
// If not matched int hash buckets, store in bucket and return this row.
// If matched, get next row until get row not matched.
int ObHashDistinct::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashDistinctCtx* hash_ctx = NULL;
  ObHashDistinctCtx* top_hash_ctx = NULL;
  ObHashDistinctCtx::PartitionLinkNode* node = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op is null");
  } else if (OB_ISNULL(top_hash_ctx = hash_ctx = GET_PHY_OPERATOR_CTX(ObHashDistinctCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (!hash_ctx->bkt_created_ && OB_FAIL(init_ha(ctx.get_my_session(), hash_ctx))) {
    LOG_WARN("Failed to init ha", K(ret));
  } else {
    bool break_out = false;
    if (hash_ctx->get_cur_sub_partition(top_hash_ctx, node)) {
      hash_ctx = node->data_;
    }
    while (!break_out && OB_SUCC(ret)) {
      switch (hash_ctx->state_) {
        case ObHashDistinctCtx::HDState::SCAN_CHILD:
          if (OB_FAIL(hash_ctx->process_rows_from_child(this, row, break_out))) {
            if (OB_ITER_END == ret) {
              if (NULL != hash_ctx->part_row_stores_) {
                ret = OB_SUCCESS;
                if (OB_FAIL(hash_ctx->clean_up_partitions())) {
                  LOG_WARN("Failed to finish partition", K(ret));
                } else {
                  hash_ctx->state_ = ObHashDistinctCtx::HDState::PROCESS_IN_MEM;
                }
              }
              if (is_block_mode_ && hash_ctx->ha_row_store_ != NULL && hash_ctx->ha_row_store_->get_row_cnt() > 0) {
                ret = OB_SUCCESS;
                hash_ctx->state_ = ObHashDistinctCtx::HDState::OUTPUT_HA;
                if (OB_FAIL(hash_ctx->ha_row_store_->begin(hash_ctx->part_it_))) {
                  LOG_WARN("begin iterator for crs failed", K(ret));
                }
              }
              LOG_TRACE("trace distinct process child iter end",
                  K(ret),
                  K(is_block_mode_),
                  K(hash_ctx->ha_row_store_->get_row_cnt()),
                  K(get_id()));
              if (break_out) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected state:child iter end and got dist row", K(ret), K(break_out));
              }
            }
          } else if (!break_out) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected state:return from child but no dist row", K(ret), K(break_out));
          }
          break;
        case ObHashDistinctCtx::HDState::OUTPUT_HA:
          if (OB_FAIL(hash_ctx->out_put_ha_rows(row))) {
            if (OB_ITER_END == ret) {
              if (NULL != hash_ctx->part_row_stores_) {
                ret = OB_SUCCESS;
                hash_ctx->state_ = ObHashDistinctCtx::HDState::PROCESS_IN_MEM;
              }
            }
          } else {
            break_out = true;
          }
          break;
        case ObHashDistinctCtx::HDState::PROCESS_IN_MEM:
          if (OB_FAIL(hash_ctx->get_dist_row_from_in_mem_partitions(this, row, break_out))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              hash_ctx->state_ = ObHashDistinctCtx::HDState::GET_PARTITION;
              if (break_out) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected state:PROCESS_IN_MEM end and got dist row", K(ret), K(break_out));
              }
            }
          } else if (!break_out) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected state:PROCESS_IN_MEM out but no dist row", K(ret), K(break_out));
          }
          break;
        case ObHashDistinctCtx::HDState::GET_PARTITION:
          if (OB_FAIL(hash_ctx->get_next_partition(top_hash_ctx, node))) {
            if (OB_ITER_END == ret) {
              if (OB_UNLIKELY(break_out)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected state:GET_PARTITION end and got next part", K(ret), K(break_out));
              } else if (top_hash_ctx != hash_ctx) {
                if (OB_ISNULL(node)) {
                  if (hash_ctx->get_cur_sub_partition(top_hash_ctx, node)) {
                    LOG_TRACE("get current node", K(node), K(node->data_), K(node->parent_));
                  }
                }
                if (OB_ISNULL(node)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected status: node is null", K(ret));
                } else if (node->data_ != hash_ctx) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected status: node is not match with hash ctx",
                      K(node),
                      K(node->data_),
                      K(node->parent_),
                      K(hash_ctx),
                      K(ret),
                      K(hash_ctx->part_level_));
                } else {
                  ret = OB_SUCCESS;
                  top_hash_ctx->sub_list_.remove(node);
                }
                if (OB_FAIL(ret)) {
                } else if (OB_UNLIKELY(OB_ISNULL(node->parent_) || OB_ISNULL(node->data_))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN(
                      "unexpected state:GET_PARTITION got invalid node", K(ret), K(node), K_(top_hash_ctx->sub_list));
                } else {
                  if (OB_NOT_NULL(node->data_->ha_row_store_) && node->data_->ha_row_store_->is_inited()) {
                    node->data_->ha_row_store_->reset();
                  }
                  for (int i = 0; i < node->data_->part_count_; i++) {
                    if (node->data_->part_row_stores_[i].is_inited()) {
                      LOG_WARN("Molly unexpected unresetted rs", K(i), K(node->data_));
                      node->data_->part_row_stores_[i].reset();
                    }
                  }
                  LOG_TRACE("trace finish partition",
                      K(hash_ctx),
                      K(hash_ctx->part_level_),
                      K(node->parent_),
                      K(top_hash_ctx),
                      K(node->parent_->state_),
                      K(node->parent_->part_level_),
                      K(top_hash_ctx->part_level_));
                  hash_ctx->reset();
                  node->data_->~ObHashDistinctCtx();
                  hash_ctx = node->parent_;
                  top_hash_ctx->mem_context_->free(node->data_);
                  top_hash_ctx->mem_context_->free(node);
                  node = nullptr;
                  LOG_TRACE("trace finish partition and start new partition",
                      K(hash_ctx->part_level_),
                      K(hash_ctx->state_),
                      K(hash_ctx),
                      K(top_hash_ctx));
                  IGNORE_RETURN hash_ctx->update_used_mem_size();
                  if (hash_ctx != top_hash_ctx) {
                    hash_ctx->get_cur_sub_partition(top_hash_ctx, node);
                    if (OB_ISNULL(node)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("unexpected status: node is not match hash ctx", K(node), K(ret));
                    } else if (node->data_ != hash_ctx ||
                               ObHashDistinctCtx::HDState::GET_PARTITION != hash_ctx->state_) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("unexpected status: node is not match hash ctx",
                          K(node),
                          K(node->data_),
                          K(hash_ctx),
                          K(hash_ctx->state_),
                          K(ret));
                    }
                  }
                }
              }
            }
          } else {
            if (NULL == node || NULL == node->data_ || NULL == node->parent_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected state during SCAN_PARTITION", K(ret), K(node));
            } else {
              hash_ctx = node->data_;
              hash_ctx->state_ = ObHashDistinctCtx::HDState::SCAN_PARTITION;
              if (OB_FAIL(
                      hash_ctx->open_cur_partition(&node->parent_->part_row_stores_[node->parent_->cur_it_part_]))) {
                LOG_WARN("failed to open current partition", K(ret));
              }
              LOG_TRACE("GET_PARTITION",
                  K(node),
                  K(hash_ctx),
                  K(node->parent_->cur_it_part_),
                  K_(node->parent),
                  K(node->data_),
                  K(node->parent_->state_),
                  K(top_hash_ctx->sub_list_.get_size()));
            }
          }
          break;
        case ObHashDistinctCtx::HDState::SCAN_PARTITION:
          if (OB_FAIL(hash_ctx->process_one_partition(this,
                  &node->parent_->part_row_stores_[node->parent_->cur_it_part_],
                  ctx.get_my_session(),
                  row,
                  break_out))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              if (hash_ctx != top_hash_ctx && (OB_ISNULL(node) || node->data_ != hash_ctx)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected status: current node is not current ctx", K(ret), K(node->data_), K(hash_ctx));
              } else if (NULL != node->data_->part_row_stores_) {
                if (OB_FAIL(hash_ctx->clean_up_partitions())) {
                  LOG_WARN("Failed to finish partition", K(ret));
                } else {
                  hash_ctx->state_ = ObHashDistinctCtx::HDState::PROCESS_IN_MEM;
                  LOG_TRACE("PROCESS_IN_MEM", K(hash_ctx), K(node->data_), K(node->parent_), K(node->parent_->state_));
                }
              } else {
                top_hash_ctx->sub_list_.remove(node);
                LOG_TRACE("REMOVE_PARTITION",
                    K(node),
                    K(hash_ctx),
                    K(node->data_),
                    K(node->parent_),
                    K(node->parent_->state_),
                    K(hash_ctx->part_level_),
                    K(top_hash_ctx->sub_list_.get_size()),
                    K(get_id()));
                hash_ctx->reset();
                node->data_->~ObHashDistinctCtx();
                hash_ctx = node->parent_;
                top_hash_ctx->mem_context_->free(node->data_);
                top_hash_ctx->mem_context_->free(node);
                hash_ctx->state_ = ObHashDistinctCtx::HDState::GET_PARTITION;
                node = nullptr;
                IGNORE_RETURN hash_ctx->update_used_mem_size();
              }
              if (break_out) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected state:child iter end and got dist row", K(ret), K(break_out));
              }
            } else {
              LOG_WARN("failed to process partition", K(node->parent_->cur_it_part_));
            }
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state", K_(hash_ctx->state));
          break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(copy_cur_row(*top_hash_ctx, row))) {
    LOG_WARN("copy current row failed", K(ret));
  }

  return ret;
}

int64_t ObHashDistinct::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_DISTINCT, distinct_columns_);
  return pos;
}

OB_SERIALIZE_MEMBER((ObHashDistinct, ObDistinct));

}  // namespace sql
}  // namespace oceanbase
