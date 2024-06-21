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

#include "ob_hp_infras_vec_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;


//////////////////// start ObIHashPartInfrastructure //////////////////
ObIHashPartInfrastructure::~ObIHashPartInfrastructure()
{
  destroy();
}

int ObIHashPartInfrastructure::init(
  uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part, int64_t ways,
  int64_t max_batch_size, const common::ObIArray<ObExpr*> &exprs,
  ObSqlMemMgrProcessor *sql_mem_processor, const common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = mem_context_->allocp(sizeof(ObArenaAllocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    tenant_id_ = tenant_id;
    enable_sql_dumped_ = enable_sql_dumped;
    unique_ = unique;
    need_pre_part_ = need_pre_part;
    exprs_ = &exprs;
    max_batch_size_ = max_batch_size;
    if (1 == ways) {
      ways_ = InputWays::ONE;
    } else if (2 == ways) {
      ways_ = InputWays::TWO;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "Invalid Argument", K(ret), K(ways));
    }
    arena_alloc_ = new (buf) ObArenaAllocator(mem_context_->get_malloc_allocator());
    arena_alloc_->set_label("HashPartInfra");
    alloc_ = &mem_context_->get_malloc_allocator();
    sql_mem_processor_ = sql_mem_processor;
    compressor_type_ = compressor_type;
    init_part_func_ = &ObIHashPartInfrastructure::init_default_part;
    insert_row_func_ = &ObIHashPartInfrastructure::direct_insert_row;
    part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
    vector_ptrs_.set_allocator(alloc_);
  }
  return ret;
}

void ObIHashPartInfrastructure::clean_cur_dumping_partitions()
{
  if (OB_NOT_NULL(left_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_; ++i) {
      if (OB_NOT_NULL(left_dumped_parts_[i])) {
        left_dumped_parts_[i]->~ObIntraPartition();
        alloc_->free(left_dumped_parts_[i]);
        left_dumped_parts_[i] = nullptr;
      }
    }
    alloc_->free(left_dumped_parts_);
    left_dumped_parts_ = nullptr;
  }

  if (OB_NOT_NULL(right_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_; ++i) {
      if (OB_NOT_NULL(right_dumped_parts_[i])) {
        right_dumped_parts_[i]->~ObIntraPartition();
        alloc_->free(right_dumped_parts_[i]);
        right_dumped_parts_[i] = nullptr;
      }
    }
    alloc_->free(right_dumped_parts_);
    right_dumped_parts_ = nullptr;
  }
}

void ObIHashPartInfrastructure::clean_dumped_partitions()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_REMOVESAFE_X(node, left_part_list_, OB_SUCC(ret)) {
    ObIntraPartition *part = node;
    ObIntraPartition *tmp_part = left_part_list_.remove(part);
    if (tmp_part != part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexpected status: part it not match", K(ret), K(part), K(tmp_part));
    } else if (OB_FAIL(left_part_map_.erase_refactored(part->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(part->part_key_));
    } else if (part != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexepcted status: part is not match", K(ret), K(part), K(tmp_part));
    }
    part->~ObIntraPartition();
    alloc_->free(part);
    part = nullptr;
  }
  DLIST_FOREACH_REMOVESAFE_X(node, right_part_list_, OB_SUCC(ret)) {
    ObIntraPartition *part = node;
    ObIntraPartition *tmp_part = right_part_list_.remove(part);
    if (tmp_part != part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexpected status: part it not match", K(ret), K(part), K(tmp_part));
    } else if (OB_FAIL(right_part_map_.erase_refactored(part->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(part->part_key_));
    } else if (part != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexepcted status: part is not match", K(ret), K(part), K(tmp_part));
    }
    part->~ObIntraPartition();
    alloc_->free(part);
    part = nullptr;
  }
  left_part_list_.reset();
  right_part_list_.reset();
  left_part_map_.destroy();
  right_part_map_.destroy();
  has_create_part_map_ = false;
}

void ObIHashPartInfrastructure::destroy_cur_parts()
{
  if (OB_NOT_NULL(cur_left_part_)) {
    cur_left_part_->~ObIntraPartition();
    alloc_->free(cur_left_part_);
    cur_left_part_ = nullptr;
  }
  if (OB_NOT_NULL(cur_right_part_)) {
    cur_right_part_->~ObIntraPartition();
    alloc_->free(cur_right_part_);
    cur_right_part_ = nullptr;
  }
}

void ObIHashPartInfrastructure::destroy()
{
  reset();
  arena_alloc_ = nullptr;
  left_part_map_.destroy();
  right_part_map_.destroy();
  vector_ptrs_.destroy();
  if (OB_NOT_NULL(mem_context_)) {
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(my_skip_);
      my_skip_ = nullptr;
    }
  }
}

void ObIHashPartInfrastructure::reset()
{
  left_row_store_iter_.reset();
  right_row_store_iter_.reset();
  hash_table_row_store_iter_.reset();
  destroy_cur_parts();
  clean_cur_dumping_partitions();
  clean_dumped_partitions();
  preprocess_part_.store_.destroy();
  left_part_map_.clear();
  right_part_map_.clear();
  sort_collations_ = nullptr;
  cur_left_part_ = nullptr;
  cur_right_part_ = nullptr;
  left_dumped_parts_ = nullptr;
  right_dumped_parts_ = nullptr;
  cur_dumped_parts_ = nullptr;
  cur_part_start_id_ = 0;
  start_round_ = false;
  cur_side_ = InputSide::LEFT;
  has_cur_part_dumped_ = false;
  est_part_cnt_ = INT64_MAX;
  cur_level_ = 0;
  part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
  period_row_cnt_ = 0;
  left_part_cur_id_ = 0;
  right_part_cur_id_ = 0;
  io_event_observer_ = nullptr;
  is_inited_vec_ = false;
  if (OB_NOT_NULL(arena_alloc_)) {
    arena_alloc_->reset();
  }
}

int ObIHashPartInfrastructure::init_default_part(
  ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part) || OB_ISNULL(exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: part or exprs is null", K(ret));
  } else {
    part->part_key_.nth_way_ = InputSide::LEFT == cur_side_ ? 0 : 1;
    part->part_key_.part_shift_ = part_shift_ + delta_shift;
    part->part_key_.level_ = cur_level_ + 1;
    part->part_key_.nth_part_ = nth_part;
    ObMemAttr attr(tenant_id_, "HashPartInfra", ObCtxIds::WORK_AREA);
    if (OB_FAIL(part->store_.init(*exprs_, max_batch_size_, attr, limit, true, /*enable_dump*/
                                  ObHashPartItem::get_extra_size(), compressor_type_))) {
      SQL_ENG_LOG(WARN, "failed to init row store", K(ret));
    } else if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sql_mem_processor_ is null", K(ret));
    } else {
      part->store_.set_dir_id(sql_mem_processor_->get_dir_id());
      part->store_.set_allocator(*alloc_);
      part->store_.set_callback(sql_mem_processor_);
      part->store_.set_io_event_observer(io_event_observer_);
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::start_round()
{
  int ret = OB_SUCCESS;
  if (start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "current rount is not finish", K(ret));
  } else {
    if (need_pre_part_) {
      if (OB_FAIL((this->*init_part_func_)(&preprocess_part_, 0, INT64_MAX, 0))) {
        SQL_ENG_LOG(WARN, "failed to init preprocess part", K(ret));
      }
    }
    cur_left_part_ = nullptr;
    cur_right_part_ = nullptr;
    cur_part_start_id_ = max(left_part_cur_id_, right_part_cur_id_);
    start_round_ = true;
    cur_side_ = InputSide::LEFT;
    has_cur_part_dumped_ = false;
    est_part_cnt_ = INT64_MAX;
    period_row_cnt_ = 0;
  }
  return ret;
}

int ObIHashPartInfrastructure::append_dumped_parts(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition **dumped_parts = nullptr;
  if (InputSide::LEFT == input_side) {
    dumped_parts = left_dumped_parts_;
  } else {
    dumped_parts = right_dumped_parts_;
  }
  if (OB_NOT_NULL(dumped_parts)) {
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      if (dumped_parts[i]->store_.has_dumped()) {
        if (InputSide::LEFT == input_side) {
          if (OB_FAIL(left_part_map_.set_refactored(dumped_parts[i]->part_key_, dumped_parts[i]))) {
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i),
              K(dumped_parts[i]->part_key_));
          } else {
            left_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        } else {
          if (OB_FAIL(right_part_map_.set_refactored(
              dumped_parts[i]->part_key_, dumped_parts[i]))) {
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i),
              K(dumped_parts[i]->part_key_));
          } else {
            right_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        }
      } else {
        if (0 != dumped_parts[i]->store_.get_row_cnt_in_memory()
            || 0 != dumped_parts[i]->store_.get_row_cnt_on_disk()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is not empty", K(ret));
        } else {
          dumped_parts[i]->~ObIntraPartition();
          alloc_->free(dumped_parts[i]);
          dumped_parts[i] = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (InputSide::LEFT == input_side) {
        alloc_->free(left_dumped_parts_);
        left_dumped_parts_ = nullptr;
      } else {
        alloc_->free(right_dumped_parts_);
        right_dumped_parts_ = nullptr;
      }
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::append_all_dump_parts()
{
  int ret = OB_SUCCESS;
  if (nullptr != left_dumped_parts_ && nullptr != right_dumped_parts_) {
    if (left_part_cur_id_ != right_part_cur_id_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: part id is not match", K(ret),
        K(cur_part_start_id_), K(left_part_cur_id_), K(right_part_cur_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_dumped_parts(InputSide::LEFT))) {
    SQL_ENG_LOG(WARN, "failed to append dumped parts", K(ret));
  } else if (OB_FAIL(append_dumped_parts(InputSide::RIGHT))) {
    SQL_ENG_LOG(WARN, "failed to append dumped parts", K(ret));
  } else {
    left_dumped_parts_ = nullptr;
    right_dumped_parts_ = nullptr;
  }
  return ret;
}

int ObIHashPartInfrastructure::end_round()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_left_part_) || OB_NOT_NULL(cur_right_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "cur left or right part is not null", K(ret),
      K(cur_left_part_), K(cur_right_part_));
  } else if (OB_FAIL(append_all_dump_parts())) {
    SQL_ENG_LOG(WARN, "failed to append all dumped parts", K(ret));
  } else {
    left_row_store_iter_.reset();
    right_row_store_iter_.reset();
    hash_table_row_store_iter_.reset();
    preprocess_part_.store_.reset();
    start_round_ = false;
    cur_left_part_ = nullptr;
    cur_right_part_ = nullptr;
    cur_dumped_parts_ = nullptr;
    period_row_cnt_ = 0;
    if (OB_NOT_NULL(arena_alloc_)) {
      arena_alloc_->reset();
    }
  }
  return ret;
}

// 暂时没有需求，不实现
// for material
int ObIHashPartInfrastructure::direct_insert_row(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  UNUSED(exprs);
  UNUSED(exists);
  UNUSED(inserted);
  return OB_NOT_SUPPORTED;
}

// 暂时没有需求，不实现
// for hash join
int ObIHashPartInfrastructure::insert_row_with_hash_table(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  UNUSED(exprs);
  UNUSED(exists);
  UNUSED(inserted);
  return OB_NOT_SUPPORTED;
}

// support M: max_memory_sie
//         P: part_cnt
//         DS: data_size (DS = M - P * SS - ES)
//         SS: slot size, 64K
//         ES: extra_size, like hashtable and so on
// optimal equation:
//            f(x) = P * P * DS, denote "one pass" can process max data size
// constraint:
//            P * SS <= DS => P * SS * 2 <= (M - ES) < M, so Part memory size is less than 1/2 M
// we solve the optimal solution
void ObIHashPartInfrastructure::est_partition_count()
{
  static const int64_t MAX_PART_CNT = 128;
  static const int64_t MIN_PART_CNT = 8;
  int64_t max_mem_size = sql_mem_processor_->get_mem_bound();
  int64_t es = get_mem_used() - sql_mem_processor_->get_data_size();
  int64_t tmp_part_cnt = next_pow2((max_mem_size - es) / 2 / BLOCK_SIZE);
  est_part_cnt_ = tmp_part_cnt = (tmp_part_cnt > MAX_PART_CNT) ? MAX_PART_CNT : tmp_part_cnt;
  int64_t ds = max_mem_size - tmp_part_cnt * BLOCK_SIZE - es;
  int64_t max_f = tmp_part_cnt * tmp_part_cnt * ds;
  int64_t tmp_max_f = 0;
  while (tmp_part_cnt > 0) {
    if (ds >= tmp_part_cnt * BLOCK_SIZE && max_f > tmp_max_f) {
      est_part_cnt_ = tmp_part_cnt;
    }
    tmp_part_cnt >>= 1;
    ds = max_mem_size - tmp_part_cnt * BLOCK_SIZE - es;
    tmp_max_f = tmp_part_cnt * tmp_part_cnt * ds;
  }
  est_part_cnt_ = est_part_cnt_ < MIN_PART_CNT ? MIN_PART_CNT : est_part_cnt_;
}

int64_t ObIHashPartInfrastructure::est_bucket_count(
  const int64_t rows,
  const int64_t width,
  const int64_t min_bucket_cnt,
  const int64_t max_bucket_cnt)
{
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  const int64_t bkt_size = get_each_bucket_size();
  int64_t est_bucket_mem_size = next_pow2(rows) * bkt_size;
  int64_t est_data_mem_size = rows * width;
  int64_t max_remain_mem_size = std::max(0l, sql_mem_processor_->get_mem_bound() - est_part_cnt_ * BLOCK_SIZE);
  int64_t est_bucket_num = rows;
  while (est_bucket_mem_size + est_data_mem_size > max_remain_mem_size && est_bucket_num > 0) {
    est_bucket_num >>= 1;
    est_bucket_mem_size = next_pow2(est_bucket_num) * bkt_size;
    est_data_mem_size = est_bucket_num * width;
  }
  est_bucket_num = est_bucket_num < min_bucket_cnt ? min_bucket_cnt :
                    (est_bucket_num > max_bucket_cnt ? max_bucket_cnt : est_bucket_num);
  sql_mem_processor_->get_profile().set_basic_info(rows, width * rows, est_bucket_num);
  return est_bucket_num;
}

int ObIHashPartInfrastructure::
insert_batch_on_partitions(const common::ObIArray<ObExpr *> &exprs,
                           const ObBitVector &skip,
                           const int64_t batch_size,
                           uint64_t *hash_values)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_dumped_parts_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is null", K(ret));
  } else {
    if (!is_inited_vec_) {
      for (int64_t i = 0; i < exprs.count(); ++i) {
        vector_ptrs_.at(i) = exprs.at(i)->get_vector(*eval_ctx_);
      }
      is_inited_vec_ = true;
    }
    ret = ObBitVector::flip_foreach(skip, batch_size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        uint16_t batch_idx = idx;
        ObCompactRow *srow = nullptr;
        int64_t part_idx = get_part_idx(hash_values[idx]);
        if (OB_FAIL(cur_dumped_parts_[part_idx]->store_.add_batch(vector_ptrs_, &batch_idx, 1, &srow))) {
          SQL_ENG_LOG(WARN, "failed to add row", K(ret));
        } else {
          ObHashPartItem *store_row = static_cast<ObHashPartItem *>(srow);
          store_row->set_hash_value(hash_values[idx], cur_dumped_parts_[part_idx]->store_.get_row_meta());
          store_row->set_is_match(cur_dumped_parts_[part_idx]->store_.get_row_meta(), false);
        }
        return ret;
      }
    );
  }

  return ret;
}

int ObIHashPartInfrastructure::create_dumped_partitions(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  if (sizeof(uint64_t) * CHAR_BIT <= part_shift_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "too deep part level", K(ret), K(part_shift_));
  } else if (!has_create_part_map_) {
    has_create_part_map_ = true;
    if (OB_FAIL(left_part_map_.create(
        512, "HashInfraOp", "HashInfraOp", tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    } else if (OB_FAIL(right_part_map_.create(
        512, "HashInfraOp", "HashInfraOp", tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    }
  }
  has_cur_part_dumped_ = true;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cur_dumped_parts_ = static_cast<ObIntraPartition**>(
      alloc_->alloc(sizeof(ObIntraPartition*) * est_part_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    sql_mem_processor_->set_number_pass(cur_level_ + 1);
    MEMSET(cur_dumped_parts_, 0, sizeof(ObIntraPartition*) * est_part_cnt_);
    int32_t delta_shift = min(__builtin_ctz(est_part_cnt_), 8);
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      void *mem = alloc_->alloc(sizeof(ObIntraPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        cur_dumped_parts_[i] = new (mem) ObIntraPartition();
        ObIntraPartition *part = cur_dumped_parts_[i];
        if (OB_FAIL((this->*init_part_func_)(part, cur_part_start_id_ + i, 1, delta_shift))) {
          SQL_ENG_LOG(WARN, "failed to create part", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < est_part_cnt_; ++i) {
        ObIntraPartition *part = cur_dumped_parts_[i];
        if (OB_NOT_NULL(part)) {
          part->~ObIntraPartition();
          alloc_->free(part);
          cur_dumped_parts_[i] = nullptr;
        }
      }
      alloc_->free(cur_dumped_parts_);
      cur_dumped_parts_ = nullptr;
    }
  }
  if (OB_SUCC(ret)) {
    if (InputSide::LEFT == input_side) {
      if (OB_NOT_NULL(left_dumped_parts_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unexpected status: left is dumped", K(ret));
      } else {
        left_dumped_parts_ = cur_dumped_parts_;
        left_part_cur_id_ = cur_part_start_id_ + est_part_cnt_;
        SQL_ENG_LOG(TRACE, "left is dumped", K(ret));
      }
    } else {
      if (OB_NOT_NULL(right_dumped_parts_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unexpected status: right is dumped", K(ret));
      } else {
        right_dumped_parts_ = cur_dumped_parts_;
        right_part_cur_id_ = cur_part_start_id_ + est_part_cnt_;
        SQL_ENG_LOG(TRACE, "right is dumped", K(ret));
      }
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::calc_hash_value_for_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const ObBatchRows &brs,
  uint64_t *hash_values_for_batch,
  int64_t start_idx,
  uint64_t *hash_vals)
{
  int ret = OB_SUCCESS;
  uint64_t default_hash_value = DEFAULT_PART_HASH_VALUE;
  if (OB_ISNULL(sort_collations_)
      || OB_ISNULL(eval_ctx_)
      || OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash func or sort collation or hash values vector not init",
                K(sort_collations_), K(brs),
                K(hash_values_for_batch), K(ret));
  } else if (0 == brs.size_) {
    //do nothing
  } else if (0 != sort_collations_->count()) {
    //from child op, need eval
    for (int64_t j = start_idx; OB_SUCC(ret) && j < sort_collations_->count(); ++j) {
      const int64_t idx = sort_collations_->at(j).field_idx_;
      if (OB_FAIL(exprs.at(idx)->eval_vector(*eval_ctx_, brs))) {
        SQL_ENG_LOG(WARN, "failed to eval batch", K(ret), K(j));
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != hash_vals) {
        MEMCPY(hash_values_for_batch, hash_vals,  sizeof(uint64_t) * brs.size_);
      }
      for (int64_t j = start_idx; OB_SUCC(ret) && j < sort_collations_->count(); ++j) {
        bool is_batch_seed = (0 != j);
        const int64_t idx = sort_collations_->at(j).field_idx_;
        ObExpr *expr = exprs.at(idx);
        ObIVector *col_vec = expr->get_vector(*eval_ctx_);
        if (OB_FAIL(col_vec->murmur_hash_v3(*expr, hash_values_for_batch,
                                            *brs.skip_,
                                            EvalBound(brs.size_, brs.all_rows_active_),
                                            is_batch_seed ? hash_values_for_batch : &default_hash_value,
                                            is_batch_seed))) {
          SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::update_mem_status_periodically()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
                    alloc_,
                    [&](int64_t cur_cnt){ return period_row_cnt_ > cur_cnt; },
                    updated))) {
    SQL_ENG_LOG(WARN, "failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    //no error no will return , do not check
    sql_mem_processor_->update_used_mem_size(get_mem_used());
    est_partition_count();
  }
  return ret;
}

int ObIHashPartInfrastructure::
do_insert_batch_with_unique_hash_table(const common::ObIArray<ObExpr *> &exprs,
                                       uint64_t *hash_values_for_batch,
                                       const int64_t batch_size,
                                       const ObBitVector *skip,
                                       ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_skip_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "my_skip_ or eval_ctx_ is not init", K(ret), K(my_skip_), K(eval_ctx_));
  } else if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is nit init", K(ret));
  } else {
    my_skip_->reset(batch_size);
    output_vec = my_skip_;
    if (!has_cur_part_dumped_) {
      bool dummy_is_block = false; // unused
      bool dummy_full_by_pass = false; // unused
      if (OB_FAIL(set_distinct_batch(exprs, hash_values_for_batch, batch_size,
                                           skip, *my_skip_))) {
        SQL_ENG_LOG(WARN, "failed to set distinct values into hash table", K(ret));
      } else if (OB_FAIL(update_mem_status_periodically())) {
        SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
      } else if (OB_FAIL(process_dump(dummy_is_block, dummy_full_by_pass))) {
        SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
      }
    } else if (OB_FAIL(probe_batch(hash_values_for_batch, batch_size,
                                   skip, *my_skip_))) {
      SQL_ENG_LOG(WARN, "failed to probe distinct values for batch", K(ret));
    } else if (OB_FAIL(insert_batch_on_partitions(exprs, *my_skip_,
                                            batch_size, hash_values_for_batch))) {
      SQL_ENG_LOG(WARN, "failed to insert batch on partitions", K(ret));
    } else if (FALSE_IT(my_skip_->set_all(batch_size))) {
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::
do_insert_batch_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr *> &exprs,
                                               uint64_t *hash_values_for_batch,
                                               const int64_t batch_size,
                                               const ObBitVector *skip,
                                               bool is_block,
                                               bool can_insert,
                                               int64_t &exists,
                                               bool &full_by_pass,
                                               ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  exists = 0;
  if (OB_ISNULL(my_skip_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "my_skip_ or eval_ctx_ is not init", K(ret), K(my_skip_), K(eval_ctx_));
  } else if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is nit init", K(ret));
  } else {
    my_skip_->reset(batch_size);
    output_vec = my_skip_;
    if (!has_cur_part_dumped_) {
      if (!can_insert) {
       if (OB_FAIL(probe_batch(hash_values_for_batch, batch_size,skip, *my_skip_))) {
         SQL_ENG_LOG(WARN, "failed to probe batch for pass by", K(ret));
       } else {
         int64_t init_skip_cnt = nullptr == skip ? 0 : skip->accumulate_bit_cnt(batch_size);
         exists = (batch_size - init_skip_cnt)
                      - (batch_size - my_skip_->accumulate_bit_cnt(batch_size));
       }
      } else {
        if (OB_FAIL(set_distinct_batch(exprs, hash_values_for_batch, batch_size,
                                            skip, *my_skip_))) {
          SQL_ENG_LOG(WARN, "failed to set distinct values into hash table", K(ret));
        } else if (OB_FAIL(update_mem_status_periodically())) {
          SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
        } else if (OB_FAIL(process_dump(is_block, full_by_pass))) {
          SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
        }
      }
    } else if (OB_FAIL(probe_batch(hash_values_for_batch, batch_size,
                                   skip, *my_skip_))) {
      SQL_ENG_LOG(WARN, "failed to probe distinct values for batch", K(ret));
    } else if (OB_FAIL(insert_batch_on_partitions(exprs, *my_skip_,
                                            batch_size, hash_values_for_batch))) {
      SQL_ENG_LOG(WARN, "failed to insert batch on partitions", K(ret));
    } else if (FALSE_IT(my_skip_->set_all(batch_size))) {
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::
insert_row_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                     uint64_t *hash_values_for_batch,
                     const int64_t batch_size,
                     const ObBitVector *skip,
                     ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  if (OB_FAIL(do_insert_batch_with_unique_hash_table(batch_exprs,
                                                  hash_values_for_batch,
                                                  batch_size,
                                                  skip,
                                                  output_vec))) {
    SQL_ENG_LOG(WARN, "failed to insert batch", K(ret));
  }
  return ret;
}

int ObIHashPartInfrastructure::finish_insert_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      SQL_ENG_LOG(TRACE, "trace dumped partition",
        K(cur_dumped_parts_[i]->store_.get_row_cnt_in_memory()),
        K(cur_dumped_parts_[i]->store_.get_row_cnt_on_disk()),
        K(i), K(est_part_cnt_), K(cur_dumped_parts_[i]->part_key_));
      if (OB_FAIL(cur_dumped_parts_[i]->store_.dump(true))) {
        SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
      } else if (OB_FAIL(cur_dumped_parts_[i]->store_.finish_add_row(true))) {
        SQL_ENG_LOG(WARN, "failed to finish add row", K(ret));
      }
    }
    cur_dumped_parts_ = nullptr;
  }
  return ret;
}

int ObIHashPartInfrastructure::get_next_left_partition()
{
  int ret = OB_SUCCESS;
  cur_left_part_ = left_part_list_.remove_last();
  if (OB_NOT_NULL(cur_left_part_)) {
    ObIntraPartition *tmp_part = nullptr;
    if (OB_FAIL(left_part_map_.erase_refactored(cur_left_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_left_part_->part_key_));
    } else if (cur_left_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match", K(ret),
        K(cur_left_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIHashPartInfrastructure::get_next_right_partition()
{
  int ret = OB_SUCCESS;
  SQL_ENG_LOG(TRACE, "trace right part count", K(right_part_list_.get_size()));
  cur_right_part_ = right_part_list_.remove_last();
  if (OB_NOT_NULL(cur_right_part_)) {
    ObIntraPartition *tmp_part = nullptr;
    if (OB_FAIL(right_part_map_.erase_refactored(cur_right_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_right_part_->part_key_));
    } else if (cur_right_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match",
        K(ret), K(cur_right_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIHashPartInfrastructure::get_cur_matched_partition(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition *part = nullptr;
  ObIntraPartition *matched_part = nullptr;
  if (InputSide::LEFT == input_side) {
    part = cur_left_part_;
  } else {
    part = cur_right_part_;
  }
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpect status: part is null", K(ret));
  } else if (part->part_key_.is_left()) {
    ObIntraPartition *tmp_part = nullptr;
    ObIntraPartKey part_key = part->part_key_;
    part_key.set_right();
    if (OB_FAIL(right_part_map_.erase_refactored(part_key, &tmp_part))) {
    } else {
      matched_part = tmp_part;
      right_part_list_.remove(tmp_part);
    }
  } else {
    ObIntraPartition *tmp_part = nullptr;
    ObIntraPartKey part_key = part->part_key_;
    part_key.set_left();
    if (OB_FAIL(left_part_map_.erase_refactored(part_key, &tmp_part))) {
    } else {
      matched_part = tmp_part;
      left_part_list_.remove(tmp_part);
    }
  }
  if (OB_SUCC(ret) && nullptr != matched_part) {
    if (InputSide::LEFT == input_side) {
      cur_right_part_ = matched_part;
    } else {
      cur_left_part_ = matched_part;
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::open_hash_table_part()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_table_row_store_iter_.init(&preprocess_part_.store_))) {
    SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
  }
  return ret;
}

// close仅仅关闭iterator不会清理数据
int ObIHashPartInfrastructure::close_hash_table_part()
{
  int ret = OB_SUCCESS;
  hash_table_row_store_iter_.reset();
  return ret;
}

int ObIHashPartInfrastructure::open_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_)
      || (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur part is null", K(ret), K(input_side));
  } else if (InputSide::LEFT == input_side) {
    if (OB_FAIL(left_row_store_iter_.init(&cur_left_part_->store_))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_left_part_->part_key_.level_;
      part_shift_ = cur_left_part_->part_key_.part_shift_;
      SQL_ENG_LOG(TRACE, "trace open left part", K(ret), K(cur_left_part_->part_key_),
        K(cur_left_part_->store_.get_row_cnt_in_memory()),
        K(cur_left_part_->store_.get_row_cnt_on_disk()));
    }
  } else if (InputSide::RIGHT == input_side) {
    if (OB_FAIL(right_row_store_iter_.init(&cur_right_part_->store_))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_right_part_->part_key_.level_;
      part_shift_ = cur_right_part_->part_key_.part_shift_;
      SQL_ENG_LOG(TRACE, "trace open right part", K(ret), K(cur_right_part_->part_key_),
        K(cur_right_part_->store_.get_row_cnt_in_memory()),
        K(cur_right_part_->store_.get_row_cnt_on_disk()));
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::close_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  has_cur_part_dumped_ = false;
  ObIntraPartition *tmp_part = nullptr;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_)
      || (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur part is null", K(ret), K(input_side));
  } else if (InputSide::LEFT == input_side) {
    left_row_store_iter_.reset();
    tmp_part = cur_left_part_;
    cur_left_part_ = nullptr;
  } else if (InputSide::RIGHT == input_side) {
    right_row_store_iter_.reset();
    tmp_part = cur_right_part_;
    cur_right_part_ = nullptr;
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(tmp_part)) {
    tmp_part->~ObIntraPartition();
    alloc_->free(tmp_part);
    tmp_part = nullptr;
  }
  return ret;
}

int ObIHashPartInfrastructure::get_next_partition(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (InputSide::LEFT == input_side) {
    switch_left();
    if (OB_NOT_NULL(cur_left_part_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: cur partition is not null", K(ret));
    }
  } else {
    switch_right();
    if (OB_NOT_NULL(cur_right_part_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: cur partition is not null", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_create_part_map_) {
    // hash map is not created, so it can't dumped
    ret = OB_ITER_END;
  } else if (is_left()) {
    if (OB_FAIL(get_next_left_partition())) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "failed to get next left partition");
      }
    } else if (OB_ISNULL(cur_left_part_)
        || InputSide::LEFT != cur_left_part_->part_key_.nth_way_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: current part is wrong", K(ret), K(cur_left_part_));
    } else {
      cur_side_ = InputSide::LEFT;
    }
  } else {
    if (OB_FAIL(get_next_right_partition())) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "failed to get next right partition");
      }
    } else if (OB_ISNULL(cur_right_part_)
        || InputSide::RIGHT != cur_right_part_->part_key_.nth_way_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: current part is wrong", K(ret), K(cur_right_part_));
    } else {
      cur_side_ = InputSide::RIGHT;
    }
  }
  return ret;
}

// 按照input_side获取下一组的partition pair<left, right>
int ObIHashPartInfrastructure::get_next_pair_partition(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  // 这里暂时按照input_side去拿分区，其实如果需要拿最近添加的partition，应该拿left和right中最近加入的分区
  // 即可以取list中两者最后一个分区进行对比，哪个最近加入，获取哪个
  if (OB_FAIL(get_next_partition(input_side))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "failed to get next partition", K(ret));
    }
  } else if (OB_FAIL(get_cur_matched_partition(input_side))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "failed to get next partition", K(ret));
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::get_left_next_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows,
  uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *store_rows[max_row_cnt];
  if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is not init", K(ret));
  } else if (OB_ISNULL(cur_left_part_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_left_part_));
  } else if (OB_FAIL(left_row_store_iter_.get_next_batch(exprs,
                                                         *eval_ctx_,
                                                         max_row_cnt,
                                                         read_rows,
                                                         &store_rows[0]))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next batch", K(ret));
    }
  }
  //we need to precalcucate the hash values for batch, if not iter_end
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < read_rows; ++i) {
      const ObHashPartItem *sr = static_cast<const ObHashPartItem *> (store_rows[i]);
      hash_values_for_batch[i] = sr->get_hash_value(preprocess_part_.store_.get_row_meta());
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::get_right_next_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_right_part_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_right_part_));
  } else if (OB_FAIL(right_row_store_iter_.get_next_batch(exprs, *eval_ctx_, max_row_cnt, read_rows))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::get_next_hash_table_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows,
  const ObCompactRow **store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "eval ctx is nullptr", K(ret));
  } else if (OB_FAIL(hash_table_row_store_iter_.get_next_batch(exprs,
                                                               *eval_ctx_,
                                                               max_row_cnt,
                                                               read_rows,
                                                               store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next batch", K(ret));
    }
  }
  return ret;
}

int ObIHashPartInfrastructure::process_dump(bool is_block, bool &full_by_pass)
{
  int ret = OB_SUCCESS;
  bool dumped = false;
  if (need_dump()) {
    if (OB_FAIL(sql_mem_processor_->extend_max_memory_size(
        alloc_,
        [&](int64_t max_memory_size)
        { UNUSED(max_memory_size); return need_dump(); },
        dumped, sql_mem_processor_->get_data_size()))) {
      SQL_ENG_LOG(WARN, "failed to extend max memory size", K(ret));
    } else if (dumped) {
      full_by_pass = true;
      if (!is_push_down_ || is_block) {
        if (enable_sql_dumped_) {
          has_cur_part_dumped_ = true;
          if (OB_FAIL(create_dumped_partitions(cur_side_))) {
            SQL_ENG_LOG(WARN, "failed to create dumped partitions", K(ret), K(est_part_cnt_));
          }
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "hash partitioning is out of memory", K(ret), K(get_mem_used()));
        }
      } else {
        // for unblock distinct, if by_pass, return row drictly
      }
    }
  }
  return ret;
}
//////////////////// end ObIHashPartInfrastructure //////////////////

//////////////////// start ObHashPartInfrastructureVecImpl //////////////////
#define HP_INFRAS_STATUS_CHECK                                                                     \
  int ret = OB_SUCCESS;                                                                            \
  if (OB_FAIL(check_status())) {                                                                   \
    LOG_WARN("failed to check status", K(ret));                                                    \
  } else

void ObHashPartInfrastructureVecImpl::reset()
{
  if (nullptr != hp_infras_) {
    hp_infras_->reset();
  }
  is_inited_ = false;
}

void ObHashPartInfrastructureVecImpl::destroy()
{
  if (nullptr != hp_infras_) {
    hp_infras_->destroy();
    if (OB_NOT_NULL(mem_context_)) {
      if (OB_NOT_NULL(alloc_)) {
        hp_infras_->~ObIHashPartInfrastructure();
        alloc_->free(hp_infras_);
        hp_infras_ = nullptr;
        alloc_ = nullptr;
      }
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
  is_inited_ = false;
}

int ObHashPartInfrastructureVecImpl::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    void *buf = nullptr;
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, "HashPartInfra",
                    common::ObCtxIds::WORK_AREA)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else {
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::decide_hp_infras_type(const common::ObIArray<ObExpr*> &exprs,
                                                           BucketType &bkt_type,
                                                           uint64_t &payload_len)
{
  int ret = OB_SUCCESS;
  bool use_general_type = false;
  int64_t exprs_cnt = exprs.count();
  payload_len = 0;
  for (int64_t i = 0; !use_general_type && i < exprs.count(); ++i) {
    ObObjType meta_type = exprs.at(i)->datum_meta_.type_;
    const ObObjDatumMapType datum_map_type = ObDatum::get_obj_datum_map_type(meta_type);
    switch (datum_map_type) {
    case OBJ_DATUM_8BYTE_DATA:
      payload_len += 8;
      break;
    case OBJ_DATUM_4BYTE_DATA:
      payload_len += 4;
      break;
    case OBJ_DATUM_1BYTE_DATA:
      payload_len += 1;
      break;
    default:
      use_general_type = true;
      break;
    }
  }
  if (use_general_type) {
    payload_len = 0;
    bkt_type = ObHashPartInfrastructureVecImpl::TYPE_GENERAL;
  } else {
    int32_t row_fixed_size = RowMeta::get_row_fixed_size(exprs_cnt, payload_len, ObHashPartItem::get_extra_size());
    switch(static_cast<int64_t>(std::ceil(static_cast<double>(row_fixed_size) / 8))) {
      case 1 ... 6:
        bkt_type = ObHashPartInfrastructureVecImpl::BYTE_TYPE_48;
        break;
      case 7:
        bkt_type = ObHashPartInfrastructureVecImpl::BYTE_TYPE_56;
        break;
      case 8:
        bkt_type = ObHashPartInfrastructureVecImpl::BYTE_TYPE_64;
        break;
      default:
        bkt_type = ObHashPartInfrastructureVecImpl::TYPE_GENERAL;
        break;
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::init_hp_infras(const int64_t tenant_id,
                                                    const common::ObIArray<ObExpr *> &exprs,
                                                    ObIHashPartInfrastructure *&hp_infras)
{
  int ret = OB_SUCCESS;
  uint64_t payload_len = 0;
  if (hp_infras_ != nullptr) {
    // do nothing
  } else if (OB_FAIL(decide_hp_infras_type(exprs, bkt_type_, payload_len))) {
    SQL_ENG_LOG(WARN, "failed to decide hash part infras type", K(ret), K(bkt_type_),
                K(payload_len));
  } else {
    //TODO open inline bkt when compact row is optimal
    bkt_type_ = TYPE_GENERAL;
    switch (bkt_type_) {
    case TYPE_GENERAL:
      bkt_size_ = sizeof(HPInfrasBktGeneral);
      if (OB_FAIL(alloc_hp_infras_impl_instance<HPInfrasBktGeneral>(tenant_id, hp_infras_))) {
        SQL_ENG_LOG(WARN, "failed to alloc hash part infras instance", K(ret), K(bkt_type_),
                    K(payload_len));
      }
      break;
    case BYTE_TYPE_48:
      bkt_size_ = sizeof(HPInfrasFixedBktByte48);
      if (OB_FAIL(alloc_hp_infras_impl_instance<HPInfrasFixedBktByte48>(tenant_id, hp_infras_))) {
        SQL_ENG_LOG(WARN, "failed to alloc hash part infras instance", K(ret), K(bkt_type_),
                    K(payload_len));
      }
      break;
    case BYTE_TYPE_56:
      bkt_size_ = sizeof(HPInfrasFixedBktByte56);
      if (OB_FAIL(alloc_hp_infras_impl_instance<HPInfrasFixedBktByte56>(tenant_id, hp_infras_))) {
        SQL_ENG_LOG(WARN, "failed to alloc hash part infras instance", K(ret), K(bkt_type_),
                    K(payload_len));
      }
      break;
    case BYTE_TYPE_64:
      bkt_size_ = sizeof(HPInfrasFixedBktByte64);
      if (OB_FAIL(alloc_hp_infras_impl_instance<HPInfrasFixedBktByte64>(tenant_id, hp_infras_))) {
        SQL_ENG_LOG(WARN, "failed to alloc hash part infras instance", K(ret), K(bkt_type_),
                    K(payload_len));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unknown type", K(ret), K(bkt_type_));
      break;
    }
  }
  return ret;
}

int64_t ObHashPartInfrastructureVecImpl::get_bucket_size() const
{
  return bkt_size_;
}

int ObHashPartInfrastructureVecImpl::init(uint64_t tenant_id,
  bool enable_sql_dumped, bool unique, bool need_pre_part,
  int64_t ways, int64_t max_batch_size, const common::ObIArray<ObExpr*> &exprs,
  ObSqlMemMgrProcessor *sql_mem_processor, const common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(init_mem_context(tenant_id))) {
    LOG_WARN("failed to init mem context", K(ret), K(tenant_id));
  } else if (OB_FAIL(init_hp_infras(tenant_id, exprs, hp_infras_))) {
    LOG_WARN("failed to init hash part infras instance", K(ret));
  } else if (OB_FAIL(hp_infras_->init(tenant_id, enable_sql_dumped, unique,
      need_pre_part, ways, max_batch_size, exprs, sql_mem_processor, compressor_type))) {
    LOG_WARN("failed to init hash part infras", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObHashPartInfrastructureVecImpl::set_io_event_observer(ObIOEventObserver *observer)
{
  HP_INFRAS_STATUS_CHECK
  {
    hp_infras_->set_io_event_observer(observer);
  }
}

void ObHashPartInfrastructureVecImpl::set_push_down()
{
  HP_INFRAS_STATUS_CHECK
  {
    hp_infras_->set_push_down();
  }
}

int64_t ObHashPartInfrastructureVecImpl::est_bucket_count(
  const int64_t rows,
  const int64_t width,
  const int64_t min_bucket_cnt,
  const int64_t max_bucket_cnt)
{
  int64_t est_bucket_cnt = 0;
  HP_INFRAS_STATUS_CHECK
  {
    est_bucket_cnt = hp_infras_->est_bucket_count(rows, width, min_bucket_cnt, max_bucket_cnt);
  }
  return est_bucket_cnt;
}

int ObHashPartInfrastructureVecImpl::set_funcs(
  const common::ObIArray<ObSortFieldCollation> *sort_collations,
  ObEvalCtx *eval_ctx)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->set_funcs(sort_collations, eval_ctx))) {
      LOG_WARN("failed to set funcs", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::start_round()
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->start_round())) {
      LOG_WARN("failed to start round", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::end_round()
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->end_round())) {
      LOG_WARN("failed to end round", K(ret));
    }
  }
  return ret;
}

void ObHashPartInfrastructureVecImpl::switch_left()
{
  hp_infras_->switch_left();
}

bool ObHashPartInfrastructureVecImpl::has_cur_part(InputSide input_side)
{
  return hp_infras_->has_cur_part(input_side);
}

int ObHashPartInfrastructureVecImpl::get_right_next_batch(
                           const common::ObIArray<ObExpr *> &exprs,
                           const int64_t max_row_cnt,
                           int64_t &read_rows)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->get_right_next_batch(exprs, max_row_cnt, read_rows))) {
      LOG_WARN("failed to end round", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::exists_batch(
                  const common::ObIArray<ObExpr*> &exprs,
                  const ObBatchRows &brs, ObBitVector *skip,
                  uint64_t *hash_values_for_batch)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->exists_batch(exprs, brs, skip, hash_values_for_batch))) {
      LOG_WARN("failed to end round", K(ret));
    }
  }
  return ret;
}

const RowMeta &ObHashPartInfrastructureVecImpl::get_hash_store_row_meta() const
{
  return hp_infras_->get_hash_store_row_meta();
}

void ObHashPartInfrastructureVecImpl::switch_right()
{
  hp_infras_->switch_right();
}

int ObHashPartInfrastructureVecImpl::init_hash_table(int64_t bucket_cnt,
                                                     int64_t min_bucket,
                                                     int64_t max_bucket)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->init_hash_table(bucket_cnt, min_bucket, max_bucket))) {
      LOG_WARN("failed to init hash table", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::init_my_skip(const int64_t batch_size)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->init_my_skip(batch_size))) {
      LOG_WARN("failed to init my skip", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::calc_hash_value_for_batch(
  const common::ObIArray<ObExpr *> &batch_exprs,
  const ObBatchRows &child_brs,
  uint64_t *hash_values_for_batch,
  int64_t start_idx,
  uint64_t *hash_vals)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->calc_hash_value_for_batch(batch_exprs, child_brs, hash_values_for_batch,
                                                      start_idx, hash_vals))) {
      LOG_WARN("failed to calc hash value for batch", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::finish_insert_row()
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->finish_insert_row())) {
      LOG_WARN("failed to finish insert row", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::open_cur_part(InputSide input_side)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->open_cur_part(input_side))) {
      LOG_WARN("failed to open current part", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::close_cur_part(InputSide input_side)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->close_cur_part(input_side))) {
      LOG_WARN("failed to close current part", K(ret));
    }
  }
  return ret;
}

int64_t ObHashPartInfrastructureVecImpl::get_cur_part_row_cnt(InputSide input_side)
{
  int64_t row_cnt = 0;
  HP_INFRAS_STATUS_CHECK
  {
    row_cnt = hp_infras_->get_cur_part_row_cnt(input_side);
  }
  return row_cnt;
}

int64_t ObHashPartInfrastructureVecImpl::get_cur_part_file_size(InputSide input_side)
{
  int64_t part_file_size = 0;
  HP_INFRAS_STATUS_CHECK
  {
    part_file_size = hp_infras_->get_cur_part_file_size(input_side);
  }
  return part_file_size;
}

int ObHashPartInfrastructureVecImpl::get_next_pair_partition(InputSide input_side)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->get_next_pair_partition(input_side))) {
      LOG_WARN("failed to get next pair partition", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::get_next_partition(InputSide input_side)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->get_next_partition(input_side))) {
      LOG_WARN("failed to get next partition", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::get_left_next_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows,
  uint64_t *hash_values_for_batch)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->get_left_next_batch(exprs, max_row_cnt,
                                                read_rows, hash_values_for_batch))) {
      LOG_WARN("failed to get left next batch", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::get_next_hash_table_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows,
  const ObCompactRow **store_row)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->get_next_hash_table_batch(exprs, max_row_cnt, read_rows, store_row))) {
      LOG_WARN("failed to get next hash table batch", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::resize(int64_t bucket_cnt)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->resize(bucket_cnt))) {
      LOG_WARN("failed to resize", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::insert_row_for_batch(
  const common::ObIArray<ObExpr *> &batch_exprs,
  uint64_t *hash_values_for_batch,
  const int64_t batch_size,
  const ObBitVector *skip,
  ObBitVector *&output_vec)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->insert_row_for_batch(batch_exprs, hash_values_for_batch,
                                                 batch_size, skip, output_vec))) {
      LOG_WARN("failed to insert row for batch", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::do_insert_batch_with_unique_hash_table_by_pass(
  const common::ObIArray<ObExpr *> &exprs,
  uint64_t *hash_values_for_batch,
  const int64_t batch_size,
  const ObBitVector *skip,
  bool is_block,
  bool can_insert,
  int64_t &exists,
  bool &full_by_pass,
  ObBitVector *&output_vec)
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->do_insert_batch_with_unique_hash_table_by_pass(
          exprs, hash_values_for_batch, batch_size, skip, is_block, can_insert, exists,
          full_by_pass, output_vec))) {
      LOG_WARN("failed to do insert batch with unique hash table by pass", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrastructureVecImpl::open_hash_table_part()
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->open_hash_table_part())) {
      LOG_WARN("failed to open hash table part", K(ret));
    }
  }
  return ret;
}

int64_t ObHashPartInfrastructureVecImpl::get_hash_bucket_num()
{
  int64_t bucket_num = 0;
  HP_INFRAS_STATUS_CHECK
  {
    bucket_num = hp_infras_->get_hash_bucket_num();
  }
  return bucket_num;
}

int64_t ObHashPartInfrastructureVecImpl::get_bucket_num() const
{
  int64_t bucket_num = 0;
  HP_INFRAS_STATUS_CHECK
  {
    bucket_num = hp_infras_->get_bucket_num();
  }
  return bucket_num;
}

void ObHashPartInfrastructureVecImpl::reset_hash_table_for_by_pass()
{
  if (nullptr != hp_infras_) {
    hp_infras_->reset_hash_table_for_by_pass();
  }
}

int64_t ObHashPartInfrastructureVecImpl::get_hash_table_size() const
{
  int64_t ht_size = 0;
  HP_INFRAS_STATUS_CHECK
  {
    ht_size = hp_infras_->get_hash_table_size();
  }
  return ht_size;
}

int ObHashPartInfrastructureVecImpl::extend_hash_table_l3()
{
  HP_INFRAS_STATUS_CHECK
  {
    if (OB_FAIL(hp_infras_->extend_hash_table_l3())) {
      LOG_WARN("failed to extend hash table l3", K(ret));
    }
  }
  return ret;
}

bool ObHashPartInfrastructureVecImpl::hash_table_full()
{
  bool ht_full = false;
  HP_INFRAS_STATUS_CHECK
  {
    ht_full = hp_infras_->hash_table_full();
  }
  return ht_full;
}

int64_t ObHashPartInfrastructureVecImpl::get_hash_store_mem_used() const
{
  int64_t ht_mem_used = 0;
  HP_INFRAS_STATUS_CHECK
  {
    ht_mem_used = hp_infras_->get_hash_store_mem_used();
  }
  return ht_mem_used;
}

void ObHashPartInfrastructureVecImpl::destroy_my_skip()
{
  if (nullptr != hp_infras_) {
    hp_infras_->destroy_my_skip();
  }
}

int64_t ObHashPartInfrastructureVecImpl::estimate_total_count() const
{
  int64_t est_total_cnt = 0;
  HP_INFRAS_STATUS_CHECK
  {
    est_total_cnt = hp_infras_->estimate_total_count();
  }
  return est_total_cnt;
}
//////////////////// end ObHashPartInfrastructureVecImpl //////////////////
