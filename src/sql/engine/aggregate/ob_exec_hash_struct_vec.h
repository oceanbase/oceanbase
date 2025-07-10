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

#ifndef SQL_ENGINE_AGGREGATE_OB_HASH_STRUCT_VEC
#define SQL_ENGINE_AGGREGATE_OB_HASH_STRUCT_VEC
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "common/row/ob_row_store.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_temp_row_store.h"
// #include "share/aggregate/processor.h"

// 本文件中有许多模版函数原本被定义在CPP中，【因为UNITY合并编译单元的作用，而通过了编译，但模版代码的实现需要在头文件中定义】，因此关闭UNITY后导致observer无法通过编译
// 为解决关闭UNITY后的编译问题，将其挪至头文件中
// 但本函数使用了OZ、CK宏，这两个宏内部的log打印使用了LOG_WARN，要求必须定义USING_LOG_PREFIX
// 由于这里是头文件，这将导致非常棘手的问题：
// 1. 如果在本头文件之前没有定义USING_LOG_PREFIX，则必须重新定义USING_LOG_PREFIX（但宏被定义在头文件中将造成污染）
// 2. 如果是在本文件中新定义的USING_LOG_PREFIX，则需要被清理掉，防止污染被传播到其他.h以及cpp中
// 因此这里判断USING_LOG_PREFIX是否已定义，若已定义则放弃重新定义（这意味着日志并不总是被以“SQL_RESV”标识打印），同时也定义特殊标识
// 若发现定义特殊标识，则在预处理过程中执行宏清理动作
// 整个逻辑相当trick，是为了尽量少的修改代码逻辑，代码owner后续需要整改这里的逻辑
#ifndef USING_LOG_PREFIX
#define MARK_MACRO_DEFINED_BY_OB_EXEC_HASH_STRUCT_VEC_H
#define USING_LOG_PREFIX SQL_RESV
#endif

namespace oceanbase
{
namespace share
{
namespace aggregate
{
using AggrRowPtr = char *; // #include "src/share/aggregate/agg_ctx.h" // this will result recursive include
}
}
namespace common{
class ObNewRow;
class ObRowStore;
}

namespace sql
{

class ObGbyBloomFilterVec;
struct BatchAggrRowsTable
{
  static const int64_t MAX_REORDER_GROUPS = 64;
  BatchAggrRowsTable() : is_valid_(true), aggr_rows_(nullptr),
                         selectors_(nullptr), selectors_item_cnt_(nullptr),
                         max_batch_size_(0) {}
  int init(int64_t max_batch_size, ObIAllocator &alloc/*arena allocator*/);
  void reuse()
  {
    if (is_valid_) {
      if (nullptr != selectors_item_cnt_) {
        memset(selectors_item_cnt_, 0, sizeof(uint16_t) * MAX_REORDER_GROUPS);
      }
      if (nullptr != aggr_rows_) {
        memset(aggr_rows_, 0, sizeof(char *) * MAX_REORDER_GROUPS);
      }
    }
  }
  void reset() {
    is_valid_ = true;
    reuse();
  }
  bool is_valid() const { return is_valid_; }
  void set_invalid() { is_valid_ = false; }
  // when is_valid_ = false, do not record aggr ptr info, only reset true when inner_rescan
  bool is_valid_;
  char **aggr_rows_;
  uint16_t **selectors_;
  uint16_t *selectors_item_cnt_;
  uint16_t max_batch_size_;
};

enum ObGroupRowBucketType {
  INLINE = 0,
  OUTLINE,
};

enum BucketState {
  EMPTY = 0,
  OCCUPYED = 1,
  VALID = 2,
  MAX = 3
};

/*
|        |extra: aggr row    |       |
               compat row
*/
class ObGroupRowItemVec : public ObCompactRow
{
public:
  ObGroupRowItemVec() : ObCompactRow() {}
  ~ObGroupRowItemVec() {}
  char *get_aggr_row(const RowMeta &row_meta)
  {
    return static_cast<char *>(this->get_extra_payload(row_meta));
  }
  void inc_hit_cnt() { header_.cnt_++; }
  void init_hit_cnt(uint32_t cnt) { header_.cnt_ = cnt; }
  uint32_t get_hit_cnt() const { return header_.cnt_; }
  const ObCompactRow *get_groupby_row() const { return this; }
};

static_assert(sizeof(ObGroupRowItemVec) == sizeof(RowHeader),
              "do not add any member in ObCompactRow!");

struct ObGroupRowBucketBase
{
  static const int64_t HASH_VAL_BIT = 56;
  static const int64_t BKT_SEQ_BIT = 6;
  static const int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  static const int64_t BKT_SEQ_MASK = UINT64_MAX >> (64 - BKT_SEQ_BIT);
  ObGroupRowBucketBase() : info_(0) {}
  uint64_t get_hash() const { return hash_; }
  void set_hash(uint64_t hash_val) { hash_ = hash_val; }
  void set_empty() { bkt_state_ = static_cast<uint64_t> (BucketState::EMPTY); }
  void set_occupyed() { bkt_state_ = static_cast<uint64_t> (BucketState::OCCUPYED); }
  void set_valid() { bkt_state_ = static_cast<uint64_t> (BucketState::VALID); }
  bool check_hash(uint64_t hash_val) const
  { return hash_ == (HASH_VAL_MASK & hash_val); }
  bool is_valid() const { return bkt_state_ == static_cast<uint64_t> (BucketState::VALID); }
  bool is_occupyed() const { return bkt_state_ == static_cast<uint64_t> (BucketState::OCCUPYED); }
  bool is_empty() const { return bkt_state_ == static_cast<uint64_t> (BucketState::EMPTY); }
  uint64_t get_bkt_seq() const { return bkt_seq_; }
  void set_bkt_seq(uint64_t seq)
  { bkt_seq_ = (seq & BKT_SEQ_MASK); }
protected:
  union {
    struct {
      uint64_t hash_ : 56;
      uint64_t bkt_seq_ : 6; //up to BatchAggrRowsTable::MAX_REORDER_GROUPS
      uint64_t bkt_state_ : 2;
    };
    uint64_t info_;
  };
};

struct ObGroupRowBucketInline : public ObGroupRowBucketBase
{
  using CompactRowItem = ObGroupRowItemVec;
  using RowItemType = ObGroupRowItemVec;
  static const ObGroupRowBucketType TYPE = ObGroupRowBucketType::INLINE;
  static const int64_t INLINE_SIZE = 0; // FIXME: change to 64 byte
  ObGroupRowBucketInline() : ObGroupRowBucketBase() {}
  ~ObGroupRowBucketInline() {}
  CompactRowItem &get_item() { return item_; }
  void set_item(CompactRowItem &item)
  {
    if (item.get_row_size() > INLINE_SIZE) {
      ob_abort();
    }
    MEMCPY(buffer_, reinterpret_cast<char *> (&item), item.get_row_size());
  }
  OB_INLINE ObGroupRowBucketInline &operator =(const ObGroupRowBucketInline &other)
  {
    info_ = other.info_;
    MEMCPY(buffer_, other.buffer_, INLINE_SIZE);
    return *this;
  }
  TO_STRING_KV(K(info_));
  union {
    CompactRowItem item_;
    char buffer_[INLINE_SIZE];
  };
};

struct ObGroupRowBucket : public ObGroupRowBucketBase
{
  using CompactRowItem = ObGroupRowItemVec;
  using RowItemType = ObGroupRowItemVec;
  static const ObGroupRowBucketType TYPE = ObGroupRowBucketType::OUTLINE;
  ObGroupRowBucket() : ObGroupRowBucketBase(), item_(nullptr) {}
  CompactRowItem &get_item() { return *item_; }
  void set_item(CompactRowItem &item) { item_ = &item; }
  // keep trivial constructor make ObSegmentArray use memset to construct arrays.
  TO_STRING_KV(K(info_));
  CompactRowItem *item_;
};

class ShortStringAggregator
{
public:
  static const int32_t HASH_VAL_FOR_EMPTY_STRING = 256;
  static const int32_t HASH_VAL_FOR_NULL = 257;
  static const int32_t MAX_BKT_CNT_ONE_COL = 258;
  ShortStringAggregator(ObTempRowStore &group_store) : alloc_(nullptr), inited_(false), is_valid_(false), array_size_(0),
  groupby_cnt_(0), groupby_values_(nullptr), grs_array_(NULL), max_batch_size_(0), aggr_row_size_(0), size_(0), group_store_(group_store),
  new_row_selector_(), new_row_selector_cnt_(0), vector_ptrs_(),
  srows_(nullptr), ser_num_array_(nullptr) {}
  int process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                    const ObBatchRows &child_brs,
                    ObIAllocator &alloc,
                    ObEvalCtx &ctx,
                    char **batch_old_rows,
                    char **batch_new_rows,
                    int64_t &agg_row_cnt,
                    int64_t &agg_group_cnt,
                    BatchAggrRowsTable *batch_aggr_rows,
                    bool &has_new_row,
                    uint16_t *new_row_pos,
                    uint16_t *old_row_pos,
                    int64_t &new_row_cnt,
                    int64_t &old_row_cnt)
  {
    int ret = OB_SUCCESS;
    new_row_selector_cnt_ = 0;
    has_new_row = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
      if (nullptr == gby_exprs.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get invalid ptr", K(ret));
      } else {
        vector_ptrs_.at(i) = gby_exprs.at(i)->get_vector(ctx);
      }
    }
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < gby_exprs.count(); ++col_idx) {
      switch (gby_exprs.at(col_idx)->get_format(ctx)) {
        case VEC_UNIFORM : {
          ObUniformBase *vec = static_cast<ObUniformBase *> (gby_exprs.at(col_idx)->get_vector(ctx));
          for (int64_t i = 0; i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i)) {
              continue;
            }
            int64_t idx = i + col_idx * max_batch_size_;
            if (vec->get_datums()[i].is_null()) {
              groupby_values_[idx] = HASH_VAL_FOR_NULL;
            } else if (1 != vec->get_datums()[i].len_) {
              groupby_values_[idx] = HASH_VAL_FOR_EMPTY_STRING;
            } else {
              groupby_values_[idx] = static_cast<int32_t>(static_cast<uint8_t>(*vec->get_datums()[i].ptr_));
            }
          }
          break;
        }
        case VEC_UNIFORM_CONST : {
          ObUniformBase *vec = static_cast<ObUniformBase *>
                                (gby_exprs.at(col_idx)->get_vector(ctx));
          for (int64_t i = 0; i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i)) {
              continue;
            }
            int64_t idx = i + col_idx * max_batch_size_;
            if (vec->get_datums()[0].is_null()) {
              groupby_values_[idx] = HASH_VAL_FOR_NULL;
            } else if (1 != vec->get_datums()[0].len_) {
              groupby_values_[idx] = HASH_VAL_FOR_EMPTY_STRING;
            } else {
              groupby_values_[idx]
                  = static_cast<int32_t>(static_cast<uint8_t>(*vec->get_datums()[0].ptr_));
            }
          }
          break;
        }
        case VEC_CONTINUOUS: {
          ObContinuousBase *vec = static_cast<ObContinuousBase *> (gby_exprs.at(col_idx)->get_vector(ctx));
          for (int64_t i = 0; i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i)) {
              continue;
            }
            int64_t idx = i + col_idx * max_batch_size_;
            if (vec->get_nulls()->at(i)) {
              groupby_values_[idx] = HASH_VAL_FOR_NULL;
            } else if (1 != vec->get_offsets()[i + 1] - vec->get_offsets()[i]) {
              groupby_values_[idx] = HASH_VAL_FOR_EMPTY_STRING;
            } else {
              groupby_values_[idx] = static_cast<int32_t>(static_cast<uint8_t>(*(vec->get_data() + vec->get_offsets()[i])));
            }
          }
          break;
        }
        case VEC_DISCRETE : {
          ObDiscreteBase *vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(ctx));
          for (int64_t i = 0; i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i)) {
              continue;
            }
            int64_t idx = i + col_idx * max_batch_size_;
            if (vec->get_nulls()->at(i)) {
              groupby_values_[idx] = HASH_VAL_FOR_NULL;
            } else if (1 != vec->get_lens()[i]) {
              groupby_values_[idx] = HASH_VAL_FOR_EMPTY_STRING;
            } else {
              groupby_values_[idx] = static_cast<int32_t>(static_cast<uint8_t>(*vec->get_ptrs()[i]));
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "get invalid format", K(ret), K(col_idx), K(gby_exprs.at(col_idx)->get_format(ctx)));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; ++i) {
        if (child_brs.skip_->at(i)) {
          continue;
        }
        ++agg_row_cnt;
        int64_t idx = 1 == groupby_cnt_ ? groupby_values_[i]
                                          : groupby_values_[i]
                                            + MAX_BKT_CNT_ONE_COL * groupby_values_[max_batch_size_ + i];
        if (OB_ISNULL(grs_array_[idx])) {
          ObCompactRow *srow = nullptr;
          uint16_t row_idx = static_cast<uint16_t> (i);
          if (OB_FAIL(group_store_.add_batch(vector_ptrs_,
                                            &row_idx,
                                            1,
                                            &srow))) {
            SQL_ENG_LOG(WARN, "failed to add new rows", K(ret));
          } else {
            ObGroupRowItemVec *new_item = static_cast<ObGroupRowItemVec *> (&srow[0]);
            batch_new_rows[i] = new_item->get_aggr_row(group_store_.get_row_meta());
            new_row_pos[new_row_cnt++] = i;
            grs_array_[idx] = batch_new_rows[i];
            ser_num_array_[idx] = size_;
            CK (OB_NOT_NULL(batch_new_rows[i]));
            has_new_row = true;
            ++size_;
            ++agg_group_cnt;
          }
        } else {
          batch_old_rows[i] = grs_array_[idx];
          old_row_pos[old_row_cnt++] = i;
          if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
            if (size_ > BatchAggrRowsTable::MAX_REORDER_GROUPS) {
              batch_aggr_rows->set_invalid();
            } else {
              int64_t ser_num = ser_num_array_[idx];
              batch_aggr_rows->aggr_rows_[ser_num] = batch_old_rows[i];
              batch_aggr_rows->selectors_[ser_num][batch_aggr_rows->selectors_item_cnt_[ser_num]++] = i;
            }
          }
        }
      }
    }
    return ret;
  }

  int check_batch_length(const common::ObIArray<ObExpr *> &gby_exprs,
                         const ObBatchRows &child_brs,
                         const bool *is_dumped,
                         uint64_t *hash_values,
                         ObEvalCtx &eval_ctx)
  {
    int ret = OB_SUCCESS;
    for (int64_t col_idx = 0; OB_SUCC(ret) && is_valid() && col_idx < gby_exprs.count(); ++col_idx) {
      switch (gby_exprs.at(col_idx)->get_format(eval_ctx)) {
        case VEC_UNIFORM_CONST :
        case VEC_UNIFORM : {
          ObUniformBase *vec = static_cast<ObUniformBase *> (gby_exprs.at(col_idx)->get_vector(eval_ctx));
          bool is_const = VEC_UNIFORM_CONST == gby_exprs.at(col_idx)->get_format(eval_ctx);
          for (int64_t i = 0; is_valid() && i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i) || is_dumped[i]) {
              continue;
            }
            if (vec->get_datums()[is_const ? i : 0].len_ > 1) {
              set_invalid();
            }
          }
          break;
        }
        case VEC_CONTINUOUS : {
          ObContinuousBase *vec = static_cast<ObContinuousBase *> (gby_exprs.at(col_idx)->get_vector(eval_ctx));
          for (int64_t i = 0; is_valid() && i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i) || is_dumped[i]) {
              continue;
            }
            if (vec->get_offsets()[i + 1] - vec->get_offsets()[i] > 1) {
              set_invalid();
            }
          }
          break;
        }
        case VEC_DISCRETE : {
          ObDiscreteBase *vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(eval_ctx));
          for (int64_t i = 0; is_valid() && i < child_brs.size_; ++i) {
            if (child_brs.skip_->at(i) || is_dumped[i]) {
              continue;
            }
            if (vec->get_lens()[i] > 1) {
              set_invalid();
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "get invalid format", K(ret), K(col_idx), K(gby_exprs.at(col_idx)->get_format(eval_ctx)));
      }
    }
    if (OB_SUCC(ret) && !is_valid()) {
      //need fall back, recalc hash value firstly
      OZ (fallback_calc_hash_value_batch(gby_exprs, child_brs, eval_ctx, hash_values));
    }
    return ret;
  }

  int init(ObIAllocator &allocator, ObEvalCtx &eval_ctx, const ObIArray<ObExpr*> &group_exprs, const int64_t aggr_row_size)
  {
    int ret = OB_SUCCESS;
    vector_ptrs_.set_allocator(&allocator);
    new_row_selector_.set_allocator(&allocator);
    if (OB_UNLIKELY(group_exprs.count() > 2 || 0 == group_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "only support group by one or two exprs", K(ret));
    } else if (FALSE_IT(array_size_ = 1 == group_exprs.count()
                        ? MAX_BKT_CNT_ONE_COL : MAX_BKT_CNT_ONE_COL * MAX_BKT_CNT_ONE_COL)) {
    } else if (OB_ISNULL(grs_array_ = static_cast<char **>(allocator.alloc(
                                                  sizeof(char *) * array_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate group row array memory failed", K(ret));
    } else if (OB_ISNULL(ser_num_array_ = static_cast<int8_t *> (allocator.alloc(
                                                  sizeof(int8_t) * array_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate ser num array memory failed", K(ret));
    } else if (OB_ISNULL(groupby_values_ = static_cast<int32_t *> (allocator.alloc(sizeof(int32_t)
                                                                   * eval_ctx.max_batch_size_
                                                                   * group_exprs.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate group hash value array memory failed", K(ret));
    } else if (OB_ISNULL(srows_ = static_cast<ObCompactRow **> (allocator.alloc(sizeof(ObCompactRow *) * eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    } else if (OB_FAIL(new_row_selector_.prepare_allocate(eval_ctx.max_batch_size_))) {
      SQL_ENG_LOG(WARN, "failed to init selector", K(ret));
    } else if (OB_FAIL(vector_ptrs_.prepare_allocate(group_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to init vector ptrs", K(ret));
    } else {
      alloc_ = &allocator;
      groupby_cnt_ = group_exprs.count();
      inited_ = true;
      is_valid_ = true;
      max_batch_size_ = eval_ctx.max_batch_size_;
      aggr_row_size_ = aggr_row_size;
      MEMSET(groupby_values_, 0, sizeof(int32_t) * eval_ctx.max_batch_size_ * group_exprs.count());
      MEMSET(grs_array_, 0, sizeof(char *) * array_size_);
      MEMSET(ser_num_array_, 0, sizeof(int8_t) * array_size_);
    }
    if (OB_FAIL(ret)) {
      if (nullptr != grs_array_) {
        allocator.free(grs_array_);
        grs_array_ = nullptr;
      }
      if (nullptr != ser_num_array_) {
        allocator.free(ser_num_array_);
        ser_num_array_ = nullptr;
      }
      if (nullptr != groupby_values_) {
        allocator.free(groupby_values_);
        groupby_values_ = nullptr;
      }
      if (nullptr != srows_) {
        allocator.free(srows_);
        srows_ = nullptr;
      }
    }
    return ret;
  }

  void reuse()
  {
    size_ = 0;
    MEMSET(groupby_values_, 0, sizeof(int32_t) * max_batch_size_ * groupby_cnt_);
    MEMSET(grs_array_, 0, sizeof(char *) * array_size_);
    MEMSET(ser_num_array_, 0, sizeof(int8_t) * array_size_);
  }

  void destroy()
  {
    vector_ptrs_.destroy();
    new_row_selector_.destroy();
    if (nullptr != alloc_) {
      if (OB_NOT_NULL(groupby_values_)) {
        alloc_->free(groupby_values_);
        groupby_values_ = nullptr;
      }
      if (OB_NOT_NULL(grs_array_)) {
        alloc_->free(grs_array_);
        grs_array_ = nullptr;
      }
      if (OB_NOT_NULL(ser_num_array_)) {
        alloc_->free(ser_num_array_);
        ser_num_array_ = nullptr;
      }
      if (nullptr != srows_) {
        alloc_->free(srows_);
        srows_ = nullptr;
      }
      alloc_ = nullptr;
    }
  }
  bool is_valid() const { return is_valid_; }
  int64_t size() const { return size_; }
  void set_invalid() { is_valid_ = false; }
  static int fallback_calc_hash_value_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                            const ObBatchRows &child_brs,
                                            ObEvalCtx &eval_ctx,
                                            uint64_t *hash_values);
private:
  ObIAllocator *alloc_;
  bool inited_;
  bool is_valid_;
  int64_t array_size_;
  int64_t groupby_cnt_;
  int32_t *groupby_values_;
  char **grs_array_;
  int64_t max_batch_size_;
  int64_t aggr_row_size_;
  int64_t size_;
  ObTempRowStore &group_store_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> new_row_selector_;
  int64_t new_row_selector_cnt_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> vector_ptrs_;
  ObCompactRow **srows_;
  int8_t *ser_num_array_;
};

// Auto extended hash table, extend to double buckets size if hash table is quarter filled.
template <typename GroupRowBucket>
class ObExtendHashTableVec
{
public:
  using RowItemType = typename GroupRowBucket::RowItemType;
  using StoreRowFunc = std::function<int(const IVectorPtrs &vectors,
                                         const uint16_t selector[],
                                         const int64_t size,
                                         ObCompactRow **stored_rows)>;
  const static int64_t INITIAL_SIZE = 128;
  const static int64_t SIZE_BUCKET_SCALE = 2;
  const static int64_t MAX_MEM_PERCENT = 40;

  const static int64_t SKEW_HEAP_SIZE = 15;
  const static int64_t SKEW_ITEM_CNT_TOLERANCE = 64;
  constexpr static const float SKEW_POPULAR_MIN_RATIO = 0.01;

  using BucketArray = common::ObSegmentArray<GroupRowBucket,
                                             OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                             common::ModulePageAllocator,
                                             false,
                                             ObSEArray<GroupRowBucket *, 64, common::ModulePageAllocator, false>,
                                             true>;

  ObExtendHashTableVec(int64_t tenant_id)
    : is_inited_vec_(false),
      hash_expr_cnt_(0),
      initial_bucket_num_(0),
      size_(0),
      buckets_(NULL),
      allocator_("ExtendHTBucket", tenant_id, ObCtxIds::WORK_AREA),
      item_alloc_("SqlGbyItem",
                  common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                  tenant_id,
                  ObCtxIds::WORK_AREA),
      group_store_(),
      tenant_id_(tenant_id),
      gby_exprs_(nullptr),
      eval_ctx_(nullptr),
      vector_ptrs_(),
      iter_(*this, group_store_.get_row_meta()),
      probe_cnt_(0),
      max_batch_size_(0),
      locate_buckets_(nullptr),
      new_row_selector_(),
      old_row_selector_(),
      col_has_null_(),
      new_row_selector_cnt_(0),
      old_row_selector_cnt_(0),
      change_valid_idx_(),
      change_valid_idx_cnt_(0),
      srows_(nullptr),
      op_id_(-1),
      sstr_aggr_(group_store_)
  {
  }
  ~ObExtendHashTableVec() { destroy(); }
  int64_t get_probe_cnt() const { return probe_cnt_; }
  class Iterator {
  public:
    Iterator(ObExtendHashTableVec<GroupRowBucket> &hash_set, const RowMeta &meta) : hash_set_(hash_set), meta_(meta),
                                                                              scan_cnt_(0), curr_bkt_idx_(0),
                                                                              curr_item_(nullptr), store_iter_(),
                                                                              inited_(false), iter_end_(false) {}
    void reset()
    {
      scan_cnt_ = 0;
      curr_bkt_idx_ = 0;
      curr_item_ = nullptr;
      inited_ = false;
      iter_end_ = false;
      store_iter_.reset();
    }
    int get_next_batch_from_table(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows)
    {
      int ret = OB_SUCCESS;
      read_rows = 0;
      int64_t read_idx = 0;
      if (OB_ISNULL(rows)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "param is nullptr", K(ret), KP(rows));
      } else if (iter_end_) {
      } else if (OB_UNLIKELY(!inited_)) {
        inited_ = true;
        curr_bkt_idx_ = 0;
        scan_cnt_ = 0;
        while (curr_bkt_idx_ < hash_set_.buckets_->count() && !hash_set_.buckets_->at(curr_bkt_idx_).is_valid()) {
          ++curr_bkt_idx_;
        }
        if (curr_bkt_idx_ < hash_set_.buckets_->count()) {
          rows[read_idx] = &hash_set_.buckets_->at(curr_bkt_idx_).get_item();
          curr_item_ = const_cast<ObGroupRowItemVec *> (static_cast<const ObGroupRowItemVec *> (rows[read_idx]));
          ++scan_cnt_;
          ++read_rows;
          ++read_idx;
        } else {
          iter_end_ = true;
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = read_idx; !iter_end_ && i < max_rows; ++i) {
          do {
            ++curr_bkt_idx_;
            if (curr_bkt_idx_ >= hash_set_.buckets_->count()) {
              iter_end_ = true;
            }
          } while(!iter_end_ && !hash_set_.buckets_->at(curr_bkt_idx_).is_valid());
          if (!iter_end_) {
            rows[i] = &hash_set_.buckets_->at(curr_bkt_idx_).get_item();
            curr_item_ = const_cast<ObGroupRowItemVec *> (static_cast<const ObGroupRowItemVec *> (rows[i]));
            ++scan_cnt_;
            ++read_rows;
          }
        }
        if (iter_end_ && scan_cnt_ != hash_set_.size()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "scan cnt is not match", K(ret), K(read_rows), K(scan_cnt_),
                                                     K(hash_set_.size()), K(curr_bkt_idx_),
                                                     K(hash_set_.get_bucket_num()));
        } else if (!iter_end_ && read_rows != max_rows) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "read rows is not match", K(ret), K(read_rows), K(max_rows));
        }
      }
      return ret;
    }
    int get_next_batch_from_store(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows)
    {
      int ret = OB_SUCCESS;
      if (!inited_) {
        OZ (store_iter_.init(&hash_set_.group_store_));
        inited_ = true;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(store_iter_.get_next_batch(max_rows, read_rows, rows))) {
        if (ret == OB_ITER_END && read_rows == 0) {
          // for hash table size is zero
          // overwrite ret
          ret = OB_SUCCESS;
          iter_end_ = true;
        } else {
          SQL_ENG_LOG(WARN, "failed to get batch", K(ret), K(read_rows), K(max_rows));
        }
      } else {
        scan_cnt_ += read_rows;
        iter_end_ = (read_rows == 0);
      }
      return ret;
    }
    int get_next_batch_from_store(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows,
        common::ObArray<std::pair<const ObCompactRow *, int32_t>> &popular_array_temp,
      uint64_t &total_load_rows, int64_t probe_cnt)
    {
      int ret = OB_SUCCESS;
      if (!inited_) {
        OZ (store_iter_.init(&hash_set_.group_store_));
        inited_ = true;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(store_iter_.get_next_batch(max_rows, read_rows, rows))) {
        if (ret == OB_ITER_END && read_rows == 0) {
          // for hash table size is zero
          // overwrite ret
          ret = OB_SUCCESS;
          iter_end_ = true;
        } else {
          SQL_ENG_LOG(WARN, "failed to get batch", K(ret), K(read_rows), K(max_rows));
        }
      } else {
        if (read_rows > 0) {
          // insert {CompactRow*, cnt} of item in this batch to top_n_cnt heap
          for (int i = 0; OB_SUCC(ret) && i < read_rows; i++) {
            const ObGroupRowItemVec* item_ptr = static_cast<ObGroupRowItemVec*>(const_cast<ObCompactRow*>(rows[i]));
            if (OB_ISNULL(item_ptr)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_LOG(WARN, "invalid compact row", K(ret), K(i));
            } else {
              uint32_t cnt = item_ptr->get_hit_cnt();
              total_load_rows += cnt;
              if (cnt > max(SKEW_ITEM_CNT_TOLERANCE, (int) (probe_cnt * 0.01 + 1))) {
                if (popular_array_temp.count() <= SKEW_HEAP_SIZE - 1) {
                  if (OB_FAIL(popular_array_temp.push_back(std::make_pair(const_cast<ObCompactRow*>(rows[i]), cnt)))) {
                    SQL_ENG_LOG(WARN, "popular array temp push back failed", K(ret));
                  } else if (popular_array_temp.count() == SKEW_HEAP_SIZE) {
                    // Create a small top heap based on the number of occurrences of the element cnt
                    std::make_heap(popular_array_temp.begin(), popular_array_temp.end(),
                                  group_row_items_greater);
                  }
                } else {
                  if (cnt > popular_array_temp.at(0).second) {
                    std::pop_heap(popular_array_temp.begin(), popular_array_temp.end(),
                                  group_row_items_greater);
                    popular_array_temp.at(SKEW_HEAP_SIZE - 1) = std::make_pair(const_cast<ObCompactRow*>(rows[i]), cnt);
                    std::push_heap(popular_array_temp.begin(), popular_array_temp.end(),
                                  group_row_items_greater);
                  }
                }
              }
            }
          }
        }
        scan_cnt_ += read_rows;
        iter_end_ = (read_rows == 0);
      }
      return ret;
    }
  private:
    ObExtendHashTableVec<GroupRowBucket> &hash_set_;
    const RowMeta &meta_;
    int64_t scan_cnt_;
    int64_t curr_bkt_idx_;
    ObGroupRowItemVec *curr_item_;
    ObTempRowStore::Iterator store_iter_;
    bool inited_;
    bool iter_end_;
  };
  friend class ObExtendHashTableVec::Iterator;
  static int calc_extra_size(int64_t aggr_row_size) { return aggr_row_size; }
  bool is_inited() const { return sstr_aggr_.is_valid() || NULL != buckets_; }

  int init(ObIAllocator *allocator,
          lib::ObMemAttr &mem_attr,
          const common::ObIArray<ObExpr *> &gby_exprs,
          const int64_t hash_expr_cnt,
          ObEvalCtx *eval_ctx,
          int64_t max_batch_size,
          bool nullable,
          bool all_int64,
          int64_t op_id,
          bool use_sstr_aggr,
          int64_t aggr_row_size,
          int64_t initial_size);
  int append_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                   const ObBatchRows &child_brs,
                   const bool *is_dumped,
                   const uint64_t *hash_values,
                   const common::ObIArray<int64_t> &lengths,
                   char **batch_new_rows,
                   int64_t &agg_group_cnt,
                   bool need_reinit_vectors = false);
  int set_unique_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                       const ObBatchRows &child_brs,
                       const uint64_t *hash_values,
                       char **batch_new_rows);
  int process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                    const ObBatchRows &child_brs,
                    const bool *is_dumped,
                    const uint64_t *hash_values,
                    const common::ObIArray<int64_t> &lengths,
                    const bool can_append_batch,
                    const ObGbyBloomFilterVec *bloom_filter,
                    char **batch_old_rows,
                    char **batch_new_rows,
                    int64_t &agg_row_cnt,
                    int64_t &agg_group_cnt,
                    BatchAggrRowsTable *batch_aggr_rows,
                    bool need_reinit_vectors,
                    uint16_t *new_row_pos,
                    uint16_t *old_row_pos,
                    int64_t &new_row_cnt,
                    int64_t &old_row_cnt);
  template <bool ALL_ROWS_ACTIVE>
  int inner_process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                          const ObBatchRows &child_brs,
                          const bool *is_dumped,
                          const uint64_t *hash_values,
                          const common::ObIArray<int64_t> &lengths,
                          const bool can_append_batch,
                          const ObGbyBloomFilterVec *bloom_filter,
                          char **batch_old_rows,
                          char **batch_new_rows,
                          int64_t &agg_row_cnt,
                          int64_t &agg_group_cnt,
                          BatchAggrRowsTable *batch_aggr_rows,
                          bool need_reinit_vectors,
                          const bool probe_by_col,
                          const int64_t start_idx,
                          int64_t &processed_idx,
                          uint16_t *new_row_pos,
                          uint16_t *old_row_pos,
                          int64_t &new_row_cnt,
                          int64_t &old_row_cnt);
  int inner_process_batch(const RowMeta &row_meta,
                          const int64_t batch_size,
                          const ObBitVector *child_skip,
                          ObBitVector &my_skip,
                          uint64_t *hash_values,
                          StoreRowFunc sf,
                          const bool probe_by_col,
                          const int64_t start_idx,
                          int64_t &processed_idx);
  int inner_process_column(const common::ObIArray<ObExpr *> &gby_exprs,
                           const RowMeta &row_meta,
                           const int64_t col_idx,
                           bool &need_fallback);
  int inner_process_column_not_null(const common::ObIArray<ObExpr *> &gby_exprs,
                                    const RowMeta &row_meta,
                                    const int64_t col_idx,
                                    bool &need_fallback);

  static bool group_row_items_greater(const std::pair<const ObCompactRow*, int32_t> &a,
                                      const std::pair<const ObCompactRow*, int32_t> &b);

  int process_popular_value_batch(ObBatchRows *result_brs,
                                  const common::ObIArray<ObExpr *> &exprs,
                                  const common::ObIArray<int64_t> &lengths,
                                  uint64_t *hash_vals,
                                  int64_t dop,
                                  uint64_t &by_pass_rows,
                                  const uint64_t check_valid_threshold,
                                  int64_t &agg_row_cnt,
                                  int64_t &agg_group_cnt,
                                  char **batch_old_rows,
                                  char **batch_new_rows,
                                  common::hash::ObHashMap<uint64_t, uint64_t,
                                  hash::NoPthreadDefendMode> *popular_map);

  int check_popular_values_validity(uint64_t &by_pass_rows,
                                    const uint64_t check_valid_threshold,
                                    int64_t dop,
                                    common::hash::ObHashMap<uint64_t, uint64_t,
                                    hash::NoPthreadDefendMode> *popular_map);
  void prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const;
  // Link item to hash table, extend buckets if needed.
  // (Do not check item is exist or not)
  int64_t size() const { return sstr_aggr_.is_valid() ? sstr_aggr_.size() : size_; }
  int init_group_store(int64_t dir_id, ObSqlMemoryCallback *callback, ObIAllocator &alloc,
                       ObIOEventObserver *observer, int64_t max_batch_size, int64_t agg_row_size)
  {
    group_store_.set_dir_id(dir_id);
    group_store_.set_callback(callback);
    group_store_.set_allocator(alloc);
    group_store_.set_io_event_observer(observer);
    ObMemAttr attr(tenant_id_, ObModIds::OB_HASH_NODE_GROUP_ROWS, ObCtxIds::WORK_AREA);
    int64_t extra_size = calc_extra_size(agg_row_size);
    return group_store_.init(*gby_exprs_, max_batch_size, attr, 0/*mem_limit*/,
                             false/* enable_dump*/, extra_size, NONE_COMPRESSOR);
  }

  int get_next_batch(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows,
    common::ObArray<std::pair<const ObCompactRow *, int32_t>> &popular_array_temp,
    uint64_t &total_load_rows, bool get_top_n_item)
  {
    int ret = OB_SUCCESS;
    if (ObGroupRowBucketType::INLINE == GroupRowBucket::TYPE) {
      ret = iter_.get_next_batch_from_table(rows, max_rows, read_rows);
    } else if (get_top_n_item) {
      ret = iter_.get_next_batch_from_store(rows, max_rows, read_rows, popular_array_temp, total_load_rows,
        probe_cnt_);
    } else {
      ret = iter_.get_next_batch_from_store(rows, max_rows, read_rows);
    }
    return ret;
  }

  const RowMeta &get_row_meta() const { return group_store_.get_row_meta(); }

  void reset_group_store()
  {
    return group_store_.reset();
  }

  void reuse()
  {
    int ret = common::OB_SUCCESS;
    if (nullptr != buckets_) {
      int64_t bucket_num = get_bucket_num();
      buckets_->reuse();
      if (OB_FAIL(buckets_->init(bucket_num))) {
        SQL_ENG_LOG(ERROR, "resize bucket array failed", K(size_), K(bucket_num), K(get_bucket_num()));
      }
    }
    if (col_has_null_.count() > 0) {
      MEMSET(&col_has_null_.at(0), 0, col_has_null_.count());
    }
    size_ = 0;
    group_store_.reuse();
    iter_.reset();
    if (sstr_aggr_.is_valid()) {
      sstr_aggr_.reuse();
    }
    item_alloc_.reset_remain_one_page();
    probe_cnt_ = 0;
  }

  int resize(ObIAllocator *allocator, int64_t bucket_num);

  void destroy()
  {
    if (NULL != buckets_) {
      buckets_->destroy();
      allocator_.free(buckets_);
      buckets_ = NULL;
    }
    if (NULL != locate_buckets_) {
      allocator_.free(locate_buckets_);
      locate_buckets_ = nullptr;
    }
    if (nullptr != srows_) {
      allocator_.free(srows_);
      srows_ = nullptr;
    }
    sstr_aggr_.destroy();
    vector_ptrs_.destroy();
    new_row_selector_.destroy();
    old_row_selector_.destroy();
    col_has_null_.destroy();
    change_valid_idx_.destroy();
    size_ = 0;
    initial_bucket_num_ = 0;
    item_alloc_.reset();
    group_store_.~ObTempRowStore();
    allocator_.set_allocator(nullptr);
  }
  int64_t mem_used() const
  {
    return NULL == buckets_ ? 0 : buckets_->mem_used();
  }

  inline int64_t get_bucket_num() const
  {
    return NULL == buckets_ ? 0 : buckets_->count();
  }
  inline int64_t get_bucket_size() const
  {
    return sizeof(GroupRowBucket);
  }
  inline bool is_sstr_aggr_valid() const
  {
    return sstr_aggr_.is_valid();
  }
  template <typename CB>
  int foreach_bucket_hash(CB &cb) const
  {
    int ret = common::OB_SUCCESS;
    if (sstr_aggr_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sstr aggr can not foreach bkt", K(ret));
    } else if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid null buckets", K(ret), K(buckets_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_bucket_num(); i++) {
      if (buckets_->at(i).is_valid() && OB_FAIL(cb(buckets_->at(i).get_hash()))) {
        SQL_ENG_LOG(WARN, "call back failed", K(ret));
      }
    }
    return ret;
  }
  template <typename CB>
  int foreach(CB &cb) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid null buckets", K(ret), K(buckets_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_bucket_num(); i++) {
      if (OB_FAIL(cb(buckets_->at(i)))) {
        SQL_ENG_LOG(WARN, "call back failed", K(ret));
      }
    }
    return ret;
  }
  int set_distinct_batch(const RowMeta &row_meta,
                         const int64_t batch_size,
                         const ObBitVector *child_skip,
                         ObBitVector &my_skip,
                         uint64_t *hash_values,
                         StoreRowFunc sf);
  int inner_process_batch(const RowMeta &row_meta,
                          const int64_t batch_size,
                          const ObBitVector *child_skip,
                          ObBitVector &my_skip,
                          uint64_t *hash_values,
                          StoreRowFunc sf);
  int get(const RowMeta &row_meta,
          const int64_t batch_idx,
          uint64_t hash_val,
          const RowItemType *&item);
  typedef int (ObExtendHashTableVec::*CMP_FUNC)(const RowMeta &row_meta, const ObCompactRow &left, const int64_t right_idx, bool &result) const;
  int likely_equal_nullable(const RowMeta &row_meta,
                            const ObCompactRow &left,
                            const int64_t right_idx,
                            bool &result) const;
  int extend(const int64_t new_bucket_num);
  const BucketArray *get_buckets() const { return buckets_; }
protected:
  // Locate the bucket with the same hash value, or empty bucket if not found.
  // The returned empty bucket is the insert position for the %hash_val
  OB_INLINE const GroupRowBucket &locate_next_bucket(const BucketArray &buckets,
                                                     const uint64_t hash_val,
                                                     int64_t &curr_pos) const
  {
    const GroupRowBucket *bucket = nullptr;
    const int64_t cnt = buckets.count();
    uint64_t mask_hash = (hash_val & ObGroupRowBucketBase::HASH_VAL_MASK);
    if (OB_LIKELY(curr_pos < 0)) {
      // relocate bkt
      curr_pos = mask_hash & (cnt - 1);
      bucket = &buckets.at(curr_pos);
    } else {
      bucket = &buckets.at((++curr_pos) & (cnt - 1));
    }
    while (!bucket->check_hash(mask_hash) && bucket->is_valid()) {
      bucket = &buckets.at((++curr_pos) & (cnt - 1));
    }
    return *bucket;
  }
  OB_NOINLINE void locate_batch_buckets(const BucketArray &buckets,
                                        const uint64_t *hash_values,
                                        const int64_t batch_size)
  {
    const int64_t cnt = buckets.count();
    for (int64_t i = 0; i < batch_size; ++i) {
      if (i < batch_size - 8) {
        __builtin_prefetch((&buckets_->at((ObGroupRowBucketBase::HASH_VAL_MASK & hash_values[i + 8]) & (cnt - 1))),
                          0/* read */, 2 /*high temp locality*/);
      }
      int64_t curr_pos = (hash_values[i] & ObGroupRowBucketBase::HASH_VAL_MASK & (cnt - 1));
      locate_buckets_[i] = const_cast <GroupRowBucket *> (&buckets.at(curr_pos));
      while (!locate_buckets_[i]->check_hash(hash_values[i] & ObGroupRowBucketBase::HASH_VAL_MASK)
              && locate_buckets_[i]->is_valid()) {
        locate_buckets_[i] = const_cast <GroupRowBucket *> (&buckets.at((++curr_pos) & (cnt - 1)));
      }
    }
  }
  // used for extend
  OB_INLINE const GroupRowBucket &locate_empty_bucket(const BucketArray &buckets,
                                                      const uint64_t hash_val) const
  {
    const int64_t cnt = buckets.count();
    uint64_t mask_hash = (hash_val & ObGroupRowBucketBase::HASH_VAL_MASK);
    int64_t curr_pos = mask_hash & (cnt - 1);
    const GroupRowBucket * bucket = &buckets.at(curr_pos);
    while (bucket->is_valid()) {
      bucket = &buckets.at((++curr_pos) & (cnt - 1));
    }
    return *bucket;
  }

protected:
  DISALLOW_COPY_AND_ASSIGN(ObExtendHashTableVec);
  int extend();
protected:
  lib::ObMemAttr mem_attr_;
  bool is_inited_vec_;
  int64_t hash_expr_cnt_;
  int64_t initial_bucket_num_;
  int64_t size_;
  BucketArray *buckets_;
  common::ModulePageAllocator allocator_;
  ObArenaAllocator item_alloc_;
  ObTempRowStore group_store_;
  int64_t tenant_id_;
  const common::ObIArray<ObExpr *> *gby_exprs_;
  ObEvalCtx *eval_ctx_;
  static const int64_t HASH_BUCKET_PREFETCH_MAGIC_NUM = 4 * 1024;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> vector_ptrs_;
  Iterator iter_;
  int64_t probe_cnt_;
  int64_t max_batch_size_;
  GroupRowBucket **locate_buckets_;
  common::ObFixedArray<uint16_t, common::ObIAllocator> new_row_selector_;
  common::ObFixedArray<uint16_t, common::ObIAllocator> old_row_selector_;
  common::ObFixedArray<bool, common::ObIAllocator> col_has_null_;
  int64_t new_row_selector_cnt_;
  int64_t old_row_selector_cnt_;
  common::ObFixedArray<uint16_t, common::ObIAllocator> change_valid_idx_;
  int64_t change_valid_idx_cnt_;
  ObCompactRow **srows_;
  int64_t op_id_;
  ShortStringAggregator sstr_aggr_;
};

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::resize(ObIAllocator *allocator, int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (bucket_num < get_bucket_num() / 2) {
    if (NULL != buckets_) {
      buckets_->destroy();
      allocator_.free(buckets_);
      buckets_ = NULL;
    }
    size_ = 0;
    initial_bucket_num_ = 0;
    item_alloc_.reset();
    group_store_.reset();
    if (bucket_num < 2) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret));
    } else {
      void *buckets_buf = NULL;
      if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        is_inited_vec_ = false;
        buckets_ = new(buckets_buf)BucketArray(allocator_);
        buckets_->set_tenant_id(tenant_id_);
        initial_bucket_num_ = common::next_pow2(bucket_num * SIZE_BUCKET_SCALE);
        SQL_ENG_LOG(DEBUG, "debug bucket num", K(ret), K(buckets_->count()), K(initial_bucket_num_));
        size_ = 0;
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(extend())) {
        SQL_ENG_LOG(WARN, "extend failed", K(ret));
      }
    }
  } else {
    reuse();
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::extend(const int64_t new_bucket_num)
{
  int ret = common::OB_SUCCESS;
  int64_t pre_bucket_num = get_bucket_num();
  if (new_bucket_num <= pre_bucket_num) {
    // do nothing
  } else {
    iter_.reset();
    BucketArray *new_buckets = NULL;
    void *buckets_buf = NULL;
    if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      new_buckets = new(buckets_buf)BucketArray(allocator_);
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
    } else if (OB_FAIL(new_buckets->init(new_bucket_num))) {
      SQL_ENG_LOG(WARN, "resize bucket array failed", K(ret), K(new_bucket_num));
    } else {
      const int64_t size = get_bucket_num();
      for (int64_t i = 0; i < size; i++) {
        const GroupRowBucket &old = buckets_->at(i);
        if (old.is_valid()) {
          const_cast<GroupRowBucket &>(locate_empty_bucket(*new_buckets, old.get_hash())) = old;
        } else if (old.is_occupyed()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "extend is prepare allocated", K(old.get_hash()));
        }
      }
      buckets_->destroy();
      allocator_.free(buckets_);

      buckets_ = new_buckets;
      buckets_->set_tenant_id(tenant_id_);
    }
    if (OB_FAIL(ret)) {
      if (buckets_ == new_buckets) {
        SQL_ENG_LOG(ERROR, "unexpected status: failed allocate new bucket", K(ret));
      } else if (nullptr != new_buckets) {
        new_buckets->destroy();
        allocator_.free(new_buckets);
        new_buckets = nullptr;
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::extend()
{
  int ret = common::OB_SUCCESS;
  int64_t pre_bucket_num = get_bucket_num();
  int64_t new_bucket_num = 0 == pre_bucket_num ?
                          (0 == initial_bucket_num_ ? INITIAL_SIZE : initial_bucket_num_)
                          : pre_bucket_num * 2;
  SQL_ENG_LOG(DEBUG, "extend hash table", K(ret), K(new_bucket_num), K(initial_bucket_num_),
              K(pre_bucket_num));
  if (OB_FAIL(extend(new_bucket_num))) {
    SQL_ENG_LOG(WARN, "failed to extend hash table", K(ret));
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::get(const RowMeta &row_meta,
                                              const int64_t batch_idx,
                                              uint64_t hash_val,
                                              const RowItemType *&item)
{
  int ret = OB_SUCCESS;
  bool result = false;
  item = nullptr;
  GroupRowBucket *bucket = nullptr;
  if (OB_UNLIKELY(NULL == buckets_)) {
    // do nothing
  } else {
    int64_t curr_pos = -1;
    bool find_bkt = false;
    while (OB_SUCC(ret) && !find_bkt) {
      bucket = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_, hash_val, curr_pos));
      if (!bucket->is_valid()) {
        find_bkt = true;
      } else {
        RowItemType *it = &(bucket->get_item());
        if (OB_FAIL(likely_equal_nullable(row_meta, static_cast<ObCompactRow&>(*it), batch_idx, result))) {
          SQL_ENG_LOG(WARN, "failed to cmp", K(ret));
        } else if (result) {
          item = it;
          find_bkt = true;
        }
      }
    }
  }
  return ret;
}

//
// We use 8 bit for one element and 2 hash functions, false positive probability is:
// (1/8 + 1/8 - 1/8 * 1/8)^2 ~= 5.5%, good enough for us.
//
// To reuse the hash value (hash_val) of hash table && partition, we restrict bit count (bit_cnt)
// to power of 2, and construct bloom filter hashes (h1, h2) from hash_val:
//   h1: return hash_val directly, only the low log2(bit_cnt) will be used.
//   h2: bswap high 32 bit of hash_val, then right shift log2(bit_cnt) bits.
//       bswap is needed here because the low bit of hight 32 bit may be used for partition, they
//       are the same in one partition.
//
class ObGbyBloomFilterVec
{
public:
  explicit ObGbyBloomFilterVec(const ModulePageAllocator &alloc)
    : bits_(alloc), cnt_(0), h2_shift_(0)
  {
  }

  int init(const int64_t size, int64_t ratio = 8)
  {
    int ret = common::OB_SUCCESS;
    if (size <= 0 || ratio <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid bit cnt", K(ret), K(size), K(ratio));
    } else {
      cnt_ = next_pow2(ratio * size);
      if (OB_FAIL(bits_.reserve(cnt_))) {
        SQL_ENG_LOG(WARN, "bit set reserve failed", K(ret));
      } else {
        h2_shift_ = sizeof(cnt_) * CHAR_BIT - __builtin_clzl(cnt_);
      }
    }
    return ret;
  }
  void reuse()
  {
    bits_.reuse();
    cnt_ = 0;
    h2_shift_ = 0;
  }
  void reset()
  {
    bits_.reset();
    cnt_ = 0;
    h2_shift_ = 0;
  }
private:
  inline uint64_t h1(const uint64_t hash_val) const
  {
    return hash_val;
  }

  inline uint64_t h2(const uint64_t hash_val) const
  {
#ifdef OB_LITTLE_ENDIAN
    const static int64_t idx = 1;
#else
    const static int64_t idx = 0;
#endif
    uint64_t v = hash_val;
    reinterpret_cast<uint32_t *>(&v)[idx]
        = __builtin_bswap32(reinterpret_cast<const uint32_t *>(&hash_val)[idx]);
    return v >> h2_shift_;
  }

public:
  int set(const uint64_t hash_val)
  {
    int ret = common::OB_SUCCESS;
    if (0 == cnt_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalied cnt", K(ret), K(cnt_));
    } else if (OB_FAIL(bits_.add_member(h1(hash_val) & (cnt_ - 1)))
        || OB_FAIL(bits_.add_member(h2(hash_val) & (cnt_ - 1)))) {
      SQL_ENG_LOG(WARN, "bit set add member failed", K(ret), K(cnt_));
    }
    return ret;
  }

  OB_INLINE bool exist(const uint64_t hash_val) const
  {
    return bits_.has_member(h1(hash_val) & (cnt_ - 1))
        && bits_.has_member(h2(hash_val) & (cnt_ - 1));
  }

private:
  ObSegmentBitSet<common::OB_MALLOC_MIDDLE_BLOCK_SIZE> bits_;
  int64_t cnt_; // power of 2
  int64_t h2_shift_;
};

ObCompactRow &call_processor_get_groupby_stored_row_to_avoid_cycle_include(const RowMeta &row_meta,
                                                                           const share::aggregate::AggrRowPtr agg_row);

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::set_unique_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                           const ObBatchRows &child_brs,
                                                           const uint64_t *hash_values,
                                                           char **batch_new_rows)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_UNLIKELY((size_ + child_brs.size_)
                                                      * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    int64_t pre_bkt_num = get_bucket_num();
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else if (get_bucket_num() <= pre_bkt_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; ++i) {
    if (nullptr == batch_new_rows[i]) {
      continue;
    }
    ObCompactRow &srow = call_processor_get_groupby_stored_row_to_avoid_cycle_include(group_store_.get_row_meta(), batch_new_rows[i]);
    GroupRowBucket *bucket = const_cast<GroupRowBucket *> (&locate_empty_bucket(*buckets_, hash_values[i]));
    bucket->set_hash(hash_values[i]);
    bucket->set_valid();
    bucket->set_bkt_seq(size_);
    bucket->set_item(static_cast<ObGroupRowItemVec &> (srow));
    size_ += 1;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::append_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                       const ObBatchRows &child_brs,
                                                       const bool *is_dumped,
                                                       const uint64_t *hash_values,
                                                       const common::ObIArray<int64_t> &lengths,
                                                       char **batch_new_rows,
                                                       int64_t &agg_group_cnt,
                                                       bool need_reinit_vectors)
{
  int ret = OB_SUCCESS;
  if (!is_inited_vec_ || need_reinit_vectors) {
    for (int64_t i = 0; i < gby_exprs.count(); ++i) {
      if (nullptr == gby_exprs.at(i)) {
        vector_ptrs_.at(i) = nullptr;
      } else {
        vector_ptrs_.at(i) = gby_exprs.at(i)->get_vector(*eval_ctx_);
      }
    }
    is_inited_vec_ = true;
  }
  char *get_row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(group_store_.add_batch(vector_ptrs_,
                     &new_row_selector_.at(0),
                     new_row_selector_cnt_,
                     srows_,
                     &lengths))) {
    LOG_WARN("failed to add batch rows", K(ret));
  } else {
    agg_group_cnt += new_row_selector_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_row_selector_cnt_; ++i) {
      GroupRowBucket *curr_bkt = locate_buckets_[new_row_selector_.at(i)];
      ObCompactRow *curr_row = srows_[i];
      if (OB_ISNULL(curr_bkt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get locate bucket", K(ret), K(new_row_selector_.at(i)));
      } else {
        curr_bkt->set_hash(hash_values[new_row_selector_.at(i)]);
        curr_bkt->set_valid();
        curr_bkt->set_bkt_seq(size_ + i);
        curr_bkt->set_item(static_cast<ObGroupRowItemVec &> (*curr_row));
        curr_bkt->get_item().init_hit_cnt(1);
        get_row = curr_bkt->get_item().get_aggr_row(group_store_.get_row_meta());
        CK (OB_NOT_NULL(get_row));
      }
      LOG_DEBUG("append new row", "new row",
           CompactRow2STR(group_store_.get_row_meta(), *curr_row, &gby_exprs));
      batch_new_rows[new_row_selector_.at(i)] = get_row;
    }
    size_ += new_row_selector_cnt_;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                        const ObBatchRows &child_brs,
                                                        const bool *is_dumped,
                                                        const uint64_t *hash_values,
                                                        const common::ObIArray<int64_t> &lengths,
                                                        const bool can_append_batch,
                                                        const ObGbyBloomFilterVec *bloom_filter,
                                                        char **batch_old_rows,
                                                        char **batch_new_rows,
                                                        int64_t &agg_row_cnt,
                                                        int64_t &agg_group_cnt,
                                                        BatchAggrRowsTable *batch_aggr_rows,
                                                        bool need_reinit_vectors,
                                                        uint16_t *new_row_pos,
                                                        uint16_t *old_row_pos,
                                                        int64_t &new_row_cnt,
                                                        int64_t &old_row_cnt)
{
  int ret = OB_SUCCESS;
  new_row_cnt = 0;
  old_row_cnt = 0;
  if (sstr_aggr_.is_valid()
      && OB_FAIL(sstr_aggr_.check_batch_length(gby_exprs, child_brs, is_dumped,
                                               const_cast<uint64_t *> (hash_values), *eval_ctx_))) {
    LOG_WARN("failed to check batch length", K(ret));
  } else if (!sstr_aggr_.is_valid()) {
    new_row_selector_cnt_ = 0;
    // extend bucket to hold whole batch
    while (OB_SUCC(ret) && OB_UNLIKELY((size_ + child_brs.size_)
                                                        * SIZE_BUCKET_SCALE >= get_bucket_num())) {
      int64_t pre_bkt_num = get_bucket_num();
      if (OB_FAIL(extend())) {
        SQL_ENG_LOG(WARN, "extend failed", K(ret));
      } else if (get_bucket_num() <= pre_bkt_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(locate_buckets_)) {
      if (OB_ISNULL(locate_buckets_ = static_cast<GroupRowBucket **> (allocator_.alloc(sizeof(GroupRowBucket *) * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(srows_)) {
      if (OB_ISNULL(srows_ = static_cast<ObCompactRow **> (allocator_.alloc(sizeof(ObCompactRow *) * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t start_idx = 0;
      int64_t processed_idx = 0;
      bool all_rows_active = child_brs.all_rows_active_ && can_append_batch && nullptr == bloom_filter;
      #ifdef ENABLE_DEBUG_LOG
      if (all_rows_active && 0 != child_brs.skip_->accumulate_bit_cnt(child_brs.size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to check all rows active", K(op_id_), K(ret), K(lbt()),
                  K(child_brs.skip_->accumulate_bit_cnt(child_brs.size_)));
        return ret;
      }
      #endif
      if (all_rows_active ? OB_FAIL(inner_process_batch<true>(gby_exprs, child_brs, is_dumped,
                                      hash_values, lengths, can_append_batch,
                                      bloom_filter, batch_old_rows, batch_new_rows,
                                      agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                      need_reinit_vectors, true, start_idx, processed_idx,
                                      new_row_pos, old_row_pos, new_row_cnt, old_row_cnt))
                          : OB_FAIL(inner_process_batch<false>(gby_exprs, child_brs, is_dumped,
                                      hash_values, lengths, can_append_batch,
                                      bloom_filter, batch_old_rows, batch_new_rows,
                                      agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                      need_reinit_vectors, true, start_idx, processed_idx,
                                      new_row_pos, old_row_pos, new_row_cnt, old_row_cnt))) {
        LOG_WARN("failed to process batch", K(ret));
      } else if (processed_idx < child_brs.size_
                 && OB_FAIL(inner_process_batch<false>(gby_exprs, child_brs, is_dumped,
                                                hash_values, lengths, can_append_batch,
                                                bloom_filter, batch_old_rows, batch_new_rows,
                                                agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                                need_reinit_vectors, false, processed_idx, processed_idx,
                                                new_row_pos, old_row_pos, new_row_cnt, old_row_cnt))) {
        LOG_WARN("failed to process batch fallback", K(ret));
      }
    }
  } else {
    bool has_new_row = false;
    if (OB_FAIL(sstr_aggr_.process_batch(gby_exprs, child_brs, item_alloc_,
                                   *eval_ctx_, batch_old_rows, batch_new_rows,
                                   agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                   has_new_row, new_row_pos, old_row_pos,
                                   new_row_cnt, old_row_cnt))) {
      LOG_WARN("failed to process batch", K(ret));
    } else if (has_new_row) {
      if (OB_FAIL(ShortStringAggregator::fallback_calc_hash_value_batch(gby_exprs, child_brs, *eval_ctx_,
                                                 const_cast<uint64_t *> (hash_values)))) {
        LOG_WARN("failed to calc hash values", K(ret));
      } else if (OB_FAIL(set_unique_batch(gby_exprs, child_brs, hash_values, batch_new_rows))) {
        LOG_WARN("failed to set unique batch", K(ret));
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
bool ObExtendHashTableVec<GroupRowBucket>::group_row_items_greater(
  const std::pair<const ObCompactRow*, int32_t> &a,
  const std::pair<const ObCompactRow*, int32_t> &b)
{
  return a.second > b.second;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::process_popular_value_batch(ObBatchRows *result_brs,
                                      const common::ObIArray<ObExpr *> &exprs,
                                      const common::ObIArray<int64_t> &lengths,
                                      uint64_t *hash_vals,
                                      int64_t dop,
                                      uint64_t &by_pass_rows,
                                      const uint64_t check_valid_threshold,
                                      int64_t &agg_row_cnt,
                                      int64_t &agg_group_cnt,
                                      char **batch_old_rows,
                                      char **batch_new_rows,
                                      common::hash::ObHashMap<uint64_t, uint64_t,
                                      hash::NoPthreadDefendMode> *popular_map)
{
  int ret = OB_SUCCESS;
  uint64_t cnt_value;
  uint64_t mask_hash;
  bool result = false;
  bool find_bkt = false;
  bool can_add_row = false;
  GroupRowBucket *now_bucket = nullptr;
  ObGroupRowItemVec *exist_curr_gr_item = NULL;
  if (check_valid_threshold <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected check_valid_threshold value", K(check_valid_threshold));
  }
  // extend bucket to hold whole batch
  while (OB_SUCC(ret) && OB_UNLIKELY((size_ + result_brs->size_)
                                                      * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    int64_t pre_bkt_num = get_bucket_num();
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else if (get_bucket_num() <= pre_bkt_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
    }
  }
  for (int64_t i = 0; i < exprs.count(); ++i) {
    if (nullptr == exprs.at(i)) {
      vector_ptrs_.at(i) = nullptr;
    } else {
      vector_ptrs_.at(i) = exprs.at(i)->get_vector(*eval_ctx_);
    }
  }

  for (int64_t i=0; OB_SUCC(ret) && i < result_brs->size_; i++) {
    if (result_brs->skip_->at(i)) {
      continue;
    }
    by_pass_rows++;
    mask_hash = (hash_vals[i] & ObGroupRowBucketBase::HASH_VAL_MASK);
    if (popular_map->size() == 0) {
    } else if (popular_map->get_refactored(mask_hash, cnt_value) != OB_HASH_NOT_EXIST) {
      result = false;
      exist_curr_gr_item = nullptr;
      now_bucket = nullptr;
      can_add_row = false;
      if (OB_UNLIKELY(NULL == buckets_)) {
        // do nothing
      } else {
        int64_t curr_pos = -1;
        find_bkt = false;
        while (OB_SUCC(ret) && !find_bkt) {
          now_bucket = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_,
                                                      hash_vals[i], curr_pos));
          if (OB_UNLIKELY(now_bucket->is_occupyed())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wrong bucket state, is occupyed", K(ret));
          } else if (now_bucket->is_empty()) {
            find_bkt = true;
            can_add_row = true;
          } else {
            RowItemType *it = &(now_bucket->get_item());
            if (OB_FAIL(likely_equal_nullable(group_store_.get_row_meta(),
                                    static_cast<ObCompactRow&>(*it), i, result))) {
              LOG_WARN("failed to cmp", K(ret));
            } else if (result) {
              exist_curr_gr_item = it;
              find_bkt = true;
            }
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("locate curr item fail", K(ret), K(i));
        } else if (exist_curr_gr_item != NULL) {
          // old row
          ++probe_cnt_;
          ++agg_row_cnt;
          exist_curr_gr_item->inc_hit_cnt();
          batch_old_rows[i] = exist_curr_gr_item->get_aggr_row(group_store_.get_row_meta());
          CK(OB_NOT_NULL(batch_old_rows[i]));
        } else if (can_add_row) {
          // new row, add item
          size_++;
          ++probe_cnt_;
          ++agg_row_cnt;
          new_row_selector_cnt_ = 0;
          new_row_selector_.at(new_row_selector_cnt_++) = i;
          if (OB_FAIL(group_store_.add_batch(vector_ptrs_, &new_row_selector_.at(0),
                     new_row_selector_cnt_, srows_, &lengths))) {
            LOG_WARN("failed to add row", K(ret));
          }
          ++agg_group_cnt;
          now_bucket->set_hash(hash_vals[i]);
          now_bucket->set_valid();
          now_bucket->set_item(static_cast<ObGroupRowItemVec &> (*(srows_[0])));
          now_bucket->get_item().init_hit_cnt(1);
          batch_old_rows[i] = now_bucket->get_item().get_aggr_row(group_store_.get_row_meta());
          CK(OB_NOT_NULL(batch_old_rows[i]));
        } else {
        }
      }
    } else {
    }

    if (OB_FAIL(ret)) {
    } else if (0 != (by_pass_rows % (check_valid_threshold))) {
    } else if (popular_map->size() > 0 &&
      OB_FAIL(check_popular_values_validity(by_pass_rows,
      check_valid_threshold, dop, popular_map))) {
      LOG_WARN("check popular values validity failed", K(ret), K(by_pass_rows),
        K(check_valid_threshold), K(dop));
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::check_popular_values_validity(uint64_t &by_pass_rows,
                                    const uint64_t check_valid_threshold, int64_t dop,
                                    common::hash::ObHashMap<uint64_t, uint64_t,
                                    hash::NoPthreadDefendMode> *popular_map)
{
  int ret = OB_SUCCESS;
  bool has_valid_popular_value = false;
  int size = size_;

  for (int64_t i = 0; OB_SUCC(ret) && i < get_bucket_num(); i++) {
    if (buckets_->at(i).is_valid()) {
      ObGroupRowItemVec &item = (buckets_->at(i).get_item());
      if ((check_valid_threshold != item.get_hit_cnt())
          && (((item.get_hit_cnt() * dop * 1.0) / (check_valid_threshold - item.get_hit_cnt()))
              > SKEW_POPULAR_MIN_RATIO)) {
        has_valid_popular_value = true;
      }
      item.init_hit_cnt(0);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!has_valid_popular_value) {
    popular_map->reuse();
    LOG_DEBUG("no has_valid_popular_value, reuse popular_map!", K(has_valid_popular_value), K(popular_map->size()));
  }
  return ret;
}

template <typename GroupRowBucket>
template <bool ALL_ROWS_ACTIVE>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                              const ObBatchRows &child_brs,
                                                              const bool *is_dumped,
                                                              const uint64_t *hash_values,
                                                              const common::ObIArray<int64_t> &lengths,
                                                              const bool can_append_batch,
                                                              const ObGbyBloomFilterVec *bloom_filter,
                                                              char **batch_old_rows,
                                                              char **batch_new_rows,
                                                              int64_t &agg_row_cnt,
                                                              int64_t &agg_group_cnt,
                                                              BatchAggrRowsTable *batch_aggr_rows,
                                                              bool need_reinit_vectors,
                                                              const bool probe_by_col,
                                                              const int64_t start_idx,
                                                              int64_t &processed_idx,
                                                              uint16_t *new_row_pos,
                                                              uint16_t *old_row_pos,
                                                              int64_t &new_row_cnt,
                                                              int64_t &old_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t curr_idx = start_idx;
  bool need_fallback = false;
  const int64_t cnt = buckets_->count();
  if (ALL_ROWS_ACTIVE) {
    locate_batch_buckets(*buckets_, hash_values, child_brs.size_);
  }
  while (OB_SUCC(ret) && !need_fallback && curr_idx < child_brs.size_) {
    bool batch_duplicate = false;
    new_row_selector_cnt_ = 0;
    old_row_selector_cnt_ = 0;
    for (; OB_SUCC(ret) && curr_idx < child_brs.size_; ++curr_idx) {
      if (!ALL_ROWS_ACTIVE
          && (child_brs.skip_->at(curr_idx)
              || is_dumped[curr_idx]
              || (nullptr != bloom_filter
                  && !bloom_filter->exist(ObGroupRowBucketBase::HASH_VAL_MASK & hash_values[curr_idx])))) {
        continue;
      }
      int64_t curr_pos = -1;
      bool find_bkt = false;
      while (OB_SUCC(ret) && !find_bkt) {
        if (!ALL_ROWS_ACTIVE) {
          if (curr_idx + 8 < child_brs.size_) {
            __builtin_prefetch((&buckets_->at((ObGroupRowBucketBase::HASH_VAL_MASK & hash_values[curr_idx + 8]) & (cnt - 1))),
                          0/* read */, 2 /*high temp locality*/);
          }
          locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_, hash_values[curr_idx], curr_pos));
        }
        if (locate_buckets_[curr_idx]->is_valid()) {
          if (ALL_ROWS_ACTIVE || probe_by_col) {
            old_row_selector_.at(old_row_selector_cnt_++) = curr_idx;
            __builtin_prefetch(&locate_buckets_[curr_idx]->get_item(), 0, 2);
            find_bkt = true;
          } else {
            bool result = true;
            ObGroupRowItemVec *it = &locate_buckets_[curr_idx]->get_item();
            for (int64_t i = 0; OB_SUCC(ret) && result && i < gby_exprs.count(); ++i) {
              bool null_equal = (nullptr == gby_exprs.at(i));
              if (!null_equal) {
                ObIVector *r_vec = gby_exprs.at(i)->get_vector(*eval_ctx_);
                const bool l_isnull = it->is_null(i);
                const bool r_isnull = r_vec->is_null(curr_idx);
                if (l_isnull != r_isnull) {
                  result = false;
                } else if (l_isnull && r_isnull) {
                  result = true;
                } else {
                  const int64_t l_len = it->get_length(group_store_.get_row_meta(), i);
                  const int64_t r_len = r_vec->get_length(curr_idx);
                  if (l_len == r_len
                      && 0 == memcmp(it->get_cell_payload(group_store_.get_row_meta(), i),
                                  r_vec->get_payload(curr_idx),
                                  r_len)) {
                    result = true;
                  } else {
                    int cmp_res = 0;
                    if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(i), curr_idx, false,
                                                    it->get_cell_payload(group_store_.get_row_meta(), i),
                                                    l_len, cmp_res))) {
                      LOG_WARN("failed to cmp left and right", K(ret));
                    } else {
                      result = (0 == cmp_res);
                    }
                  }
                }
              }
            }
            if (OB_SUCC(ret) && result) {
              ++probe_cnt_;
              ++agg_row_cnt;
              it->inc_hit_cnt();
              batch_old_rows[curr_idx] = it->get_aggr_row(group_store_.get_row_meta());
              old_row_pos[old_row_cnt++] = curr_idx;
              if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
                if (size_ > BatchAggrRowsTable::MAX_REORDER_GROUPS) {
                  batch_aggr_rows->set_invalid();
                } else {
                  int64_t ser_num = locate_buckets_[curr_idx]->get_bkt_seq();
                  batch_aggr_rows->aggr_rows_[ser_num] = batch_old_rows[curr_idx];
                  batch_aggr_rows->selectors_[ser_num][batch_aggr_rows->selectors_item_cnt_[ser_num]++] = curr_idx;
                }
              }
              find_bkt = true;
            }
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          find_bkt = true;
        } else if (ALL_ROWS_ACTIVE || can_append_batch) {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
          ++probe_cnt_;
          ++agg_row_cnt;
          find_bkt = true;
        } else {
          find_bkt = true;
        }
      }
      if (batch_duplicate) {
        break;
      }
    }
    if (OB_SUCC(ret)
        && (ALL_ROWS_ACTIVE || probe_by_col)
        && old_row_selector_cnt_ > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < gby_exprs.count(); ++i) {
        if (nullptr == gby_exprs.at(i)) {
          //3 stage null equal
          continue;
        }
        col_has_null_.at(i) |= gby_exprs.at(i)->get_vector(*eval_ctx_)->has_null();
        if (OB_FAIL(col_has_null_.at(i) ?
                    inner_process_column(gby_exprs, group_store_.get_row_meta(), i, need_fallback)
                    : inner_process_column_not_null(gby_exprs, group_store_.get_row_meta(), i, need_fallback))) {
          LOG_WARN("failed to process column", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (!need_fallback) {
          probe_cnt_ += old_row_selector_cnt_;
          agg_row_cnt += old_row_selector_cnt_;
          if (batch_aggr_rows && batch_aggr_rows->is_valid() && size_ > BatchAggrRowsTable::MAX_REORDER_GROUPS) {
            batch_aggr_rows->set_invalid();
          }
          if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
            for (int64_t i = 0; i < old_row_selector_cnt_; ++i) {
              const int64_t idx = old_row_selector_.at(i);
              batch_old_rows[idx] = (static_cast<GroupRowBucket *> (locate_buckets_[idx]))->get_item().get_aggr_row(group_store_.get_row_meta());
              old_row_pos[old_row_cnt++] = idx;
              (static_cast<GroupRowBucket *> (locate_buckets_[idx]))->get_item().inc_hit_cnt();
              int64_t ser_num = locate_buckets_[idx]->get_bkt_seq();
              batch_aggr_rows->aggr_rows_[ser_num] = batch_old_rows[idx];
              batch_aggr_rows->selectors_[ser_num][batch_aggr_rows->selectors_item_cnt_[ser_num]++] = idx;
            }
          } else {
            for (int64_t i = 0; i < old_row_selector_cnt_; ++i) {
              const int64_t idx = old_row_selector_.at(i);
              (static_cast<GroupRowBucket *> (locate_buckets_[idx]))->get_item().inc_hit_cnt();
              batch_old_rows[idx] = (static_cast<GroupRowBucket *> (locate_buckets_[idx]))->get_item().get_aggr_row(group_store_.get_row_meta());
              old_row_pos[old_row_cnt++] = idx;
            }
          }
        } else {
          //reset occupyed bkt and stat
          for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
            locate_buckets_[new_row_selector_.at(i)]->set_empty();
          }
          probe_cnt_ -= new_row_selector_cnt_;
          agg_row_cnt -= new_row_selector_cnt_;
          continue;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if ((ALL_ROWS_ACTIVE || can_append_batch)
          && new_row_selector_cnt_ > 0
          && OB_FAIL(append_batch(gby_exprs, child_brs, is_dumped, hash_values,
                              lengths, batch_new_rows, agg_group_cnt,
                              need_reinit_vectors))) {
        LOG_WARN("failed to append batch", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < new_row_selector_cnt_; ++i) {
          new_row_pos[new_row_cnt++] = new_row_selector_.at(i);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
          if (nullptr == gby_exprs.at(i)) {
            //3 stage null equal
            continue;
          }
          col_has_null_.at(i) |= gby_exprs.at(i)->get_vector(*eval_ctx_)->has_null();
        }
        new_row_selector_cnt_ = 0;
      }
    }
    processed_idx = curr_idx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_column(const common::ObIArray<ObExpr *> &gby_exprs,
                                                               const RowMeta &row_meta,
                                                               const int64_t col_idx,
                                                               bool &need_fallback)
{
  int ret = OB_SUCCESS;
  switch (gby_exprs.at(col_idx)->get_format(*eval_ctx_)) {
    case VEC_FIXED : {
      ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      int64_t r_len = r_vec->get_length();
      if (row_meta.fixed_expr_reordered()) {
        const int64_t offset = row_meta.get_fixed_cell_offset(col_idx);
        if (r_len == 8) {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            const bool l_isnull = it->is_null(col_idx);
            const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
            if (l_isnull != r_isnull) {
              need_fallback = true;
            } else if (l_isnull && r_isnull) {
            } else {
              need_fallback = (*(reinterpret_cast<int64_t *> (r_vec->get_data() + r_len * curr_idx))
                                  != *(reinterpret_cast<const int64_t *> (it->get_fixed_cell_payload(offset))));
            }
          }
        } else {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            const bool l_isnull = it->is_null(col_idx);
            const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
            if (l_isnull != r_isnull) {
              need_fallback = true;
            } else if (l_isnull && r_isnull) {
            } else {
              need_fallback = (0 != memcmp(it->get_fixed_cell_payload(offset),
                            r_vec->get_data() + r_len * curr_idx,
                            r_len));
            }
          }
        }
      } else {
        for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
          const int64_t curr_idx = old_row_selector_.at(i);
          ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
          const bool l_isnull = it->is_null(col_idx);
          const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
           if (l_isnull != r_isnull) {
            need_fallback = true;
          } else if (l_isnull && r_isnull) {
          } else {
            need_fallback = (0 != memcmp(it->get_cell_payload(row_meta, col_idx),
                            r_vec->get_data() + r_len * curr_idx,
                            r_len));
          }
        }
      }
      break;
    }
    case VEC_DISCRETE : {
      ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const bool l_isnull = it->is_null(col_idx);
        const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
        if (l_isnull != r_isnull) {
          need_fallback = true;
        } else if (l_isnull && r_isnull) {
        } else {
          const int64_t r_len = r_vec->get_lens()[curr_idx];
          const int64_t l_len = it->get_length(row_meta, col_idx);
          if (r_len == l_len
              && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_ptrs()[curr_idx],
                          r_len)) {
          } else {
            int cmp_res = 0;
            if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                            it->get_cell_payload(row_meta, col_idx),
                                            l_len, cmp_res))) {
              LOG_WARN("failed to cmp left and right", K(ret));
            } else {
              need_fallback = static_cast<bool> (cmp_res);
            }
          }
        }
      }
      break;
    }
    case VEC_CONTINUOUS :
    case VEC_UNIFORM :
    case VEC_UNIFORM_CONST : {
      ObIVector *r_vec = gby_exprs.at(col_idx)->get_vector(*eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const bool l_isnull = it->is_null(col_idx);
        const bool r_isnull = r_vec->is_null(curr_idx);
        if (l_isnull != r_isnull) {
          need_fallback = true;
        } else if (l_isnull && r_isnull) {
        } else {
          const int64_t l_len = it->get_length(row_meta, col_idx);
          const int64_t r_len = r_vec->get_length(curr_idx);
          if (l_len == r_len
              && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_payload(curr_idx),
                          r_len)) {
          } else {
            int cmp_res = 0;
            if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                            it->get_cell_payload(row_meta, col_idx),
                                            l_len, cmp_res))) {
              LOG_WARN("failed to cmp left and right", K(ret));
            } else {
              need_fallback = static_cast<bool> (cmp_res);
            }
          }
        }
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data format", K(ret), K(gby_exprs.at(col_idx)->get_format(*eval_ctx_)));
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_column_not_null(const common::ObIArray<ObExpr *> &gby_exprs,
                                                                        const RowMeta &row_meta,
                                                                        const int64_t col_idx,
                                                                        bool &need_fallback)
{
  int ret = OB_SUCCESS;
  switch (gby_exprs.at(col_idx)->get_format(*eval_ctx_)) {
    case VEC_FIXED : {
      ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      int64_t r_len = r_vec->get_length();
      if (row_meta.fixed_expr_reordered()) {
        const int64_t offset = row_meta.get_fixed_cell_offset(col_idx);
        if (r_len == 8) {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            need_fallback = (*(reinterpret_cast<int64_t *> (r_vec->get_data() + r_len * curr_idx))
                                != *(reinterpret_cast<const int64_t *> (it->get_fixed_cell_payload(offset))));
          }
        } else {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            need_fallback = (0 != memcmp(it->get_fixed_cell_payload(offset),
                          r_vec->get_data() + r_len * curr_idx,
                          r_len));
          }
        }
      } else {
        for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
          const int64_t curr_idx = old_row_selector_.at(i);
          ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
          need_fallback = (0 != memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_data() + r_len * curr_idx,
                          r_len));
        }
      }
      break;
    }
    case VEC_DISCRETE : {
      ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      const int64_t real_col_idx = row_meta.fixed_expr_reordered()
                                  ? (row_meta.project_idx(col_idx) - row_meta.fixed_cnt_) : col_idx;
      const int64_t len_off = row_meta.var_offsets_off_ + sizeof(int32_t) * real_col_idx;
      const int64_t var_off = row_meta.var_data_off_;
      for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const int64_t r_len = r_vec->get_lens()[curr_idx];
        const int32_t off1 = *reinterpret_cast<int32_t *>(it->payload() + len_off + sizeof(int32_t));
        const int32_t off2 = *reinterpret_cast<int32_t *>(it->payload() + len_off);
        int32_t l_len = off1 - off2;
        const char *var_data = it->payload() + var_off;
        if (l_len != r_len
            || 0 != memcmp(var_data + off2, r_vec->get_ptrs()[curr_idx], r_len)) {
          need_fallback = true;
        }
      }
      break;
    }
    case VEC_CONTINUOUS :
    case VEC_UNIFORM :
    case VEC_UNIFORM_CONST : {
      ObIVector *r_vec = gby_exprs.at(col_idx)->get_vector(*eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const int64_t l_len = it->get_length(row_meta, col_idx);
        const int64_t r_len = r_vec->get_length(curr_idx);
        if (l_len == r_len
            && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                        r_vec->get_payload(curr_idx),
                        r_len)) {
        } else {
          int cmp_res = 0;
          if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                          it->get_cell_payload(row_meta, col_idx),
                                          l_len, cmp_res))) {
            LOG_WARN("failed to cmp left and right", K(ret));
          } else {
            need_fallback = static_cast<bool> (cmp_res);
          }
        }
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data format", K(ret), K(gby_exprs.at(col_idx)->get_format(*eval_ctx_)));
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::init(ObIAllocator *allocator,
                                               lib::ObMemAttr &mem_attr,
                                               const ObIArray<ObExpr *> &gby_exprs,
                                               const int64_t hash_expr_cnt,
                                               ObEvalCtx *eval_ctx,
                                               int64_t max_batch_size,
                                               bool nullable,
                                               bool all_int64,
                                               int64_t op_id,
                                               bool use_sstr_aggr,
                                               int64_t aggr_row_size,
                                               int64_t initial_size)
{
  int ret = OB_SUCCESS;
  if (initial_size < 2) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(initial_size));
  } else {
    mem_attr_ = mem_attr;
    hash_expr_cnt_ = hash_expr_cnt;
    allocator_.set_allocator(allocator);
    allocator_.set_label(mem_attr.label_);
    max_batch_size_ = max_batch_size;
    void *buckets_buf = NULL;
    op_id_ = op_id;
    vector_ptrs_.set_allocator(allocator);
    new_row_selector_.set_allocator(allocator);
    old_row_selector_.set_allocator(allocator);
    col_has_null_.set_allocator(allocator);
    change_valid_idx_.set_allocator(allocator);
    if (use_sstr_aggr && OB_FAIL(sstr_aggr_.init(allocator_, *eval_ctx, gby_exprs, aggr_row_size))) {
      LOG_WARN("failed to init short string aggr", K(ret));
    } else if (OB_FAIL(vector_ptrs_.prepare_allocate(gby_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to alloc ptrs", K(ret));
    } else if (OB_FAIL(new_row_selector_.prepare_allocate(max_batch_size))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_FAIL(old_row_selector_.prepare_allocate(max_batch_size))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_FAIL(col_has_null_.prepare_allocate(gby_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      if (col_has_null_.count() > 0) {
        MEMSET(&col_has_null_.at(0), 0, col_has_null_.count());
      }
      buckets_ = new(buckets_buf)BucketArray(allocator_);
      buckets_->set_tenant_id(tenant_id_);
      initial_bucket_num_ = common::next_pow2(initial_size * SIZE_BUCKET_SCALE);
      SQL_ENG_LOG(DEBUG, "debug bucket num", K(ret), K(buckets_->count()), K(initial_bucket_num_));
      size_ = 0;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_vec_ = false;
    gby_exprs_ = &gby_exprs;
    eval_ctx_ = eval_ctx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::set_distinct_batch(const RowMeta &row_meta,
                                                             const int64_t batch_size,
                                                             const ObBitVector *child_skip,
                                                             ObBitVector &my_skip,
                                                             uint64_t *hash_values,
                                                             StoreRowFunc sf)
{
  int ret = OB_SUCCESS;
  new_row_selector_cnt_ = 0;
  int64_t real_batch_size =
    (nullptr != child_skip) ? batch_size - child_skip->accumulate_bit_cnt(batch_size) : batch_size;
  if (OB_UNLIKELY((size_ + real_batch_size) * SIZE_BUCKET_SCALE > get_bucket_num())) {
    int64_t new_bucket_num = common::next_pow2((size_ + real_batch_size) * SIZE_BUCKET_SCALE);
    int64_t pre_bkt_num = get_bucket_num();
    if (OB_FAIL(extend(new_bucket_num))) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else if (get_bucket_num() <= pre_bkt_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(locate_buckets_)) {
    if (OB_ISNULL(locate_buckets_ = static_cast<GroupRowBucket **> (allocator_.alloc(sizeof(GroupRowBucket *) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(srows_)) {
    if (OB_ISNULL(srows_ = static_cast<ObCompactRow **> (allocator_.alloc(sizeof(ObCompactRow *) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    }
  }
  if (OB_SUCC(ret) && !is_inited_vec_) {
    for (int64_t i = 0; i < gby_exprs_->count(); ++i) {
      vector_ptrs_.at(i) = gby_exprs_->at(i)->get_vector(*eval_ctx_);
    }
    is_inited_vec_ = true;
  }

  if (OB_SUCC(ret)) {
    const int64_t start_idx = 0;
    int64_t processed_idx = 0;
    if (OB_FAIL(inner_process_batch(row_meta, batch_size, child_skip,
                                    my_skip, hash_values, sf,
                                    true, start_idx, processed_idx))) {
      LOG_WARN("failed to process batch", K(ret));
    } else if (processed_idx < batch_size
                && OB_FAIL(inner_process_batch(row_meta, batch_size, child_skip,
                                               my_skip, hash_values, sf,
                                               false, processed_idx, processed_idx))) {
      LOG_WARN("failed to process batch fallback", K(ret));
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_batch(const RowMeta &row_meta,
                                                              const int64_t batch_size,
                                                              const ObBitVector *child_skip,
                                                              ObBitVector &my_skip,
                                                              uint64_t *hash_values,
                                                              StoreRowFunc sf,
                                                              const bool probe_by_col,
                                                              const int64_t start_idx,
                                                              int64_t &processed_idx)
{
  int ret = OB_SUCCESS;
  int64_t curr_idx = start_idx;
  bool need_fallback = false;
  while (OB_SUCC(ret) && !need_fallback && curr_idx < batch_size) {
    bool batch_duplicate = false;
    new_row_selector_cnt_ = 0;
    old_row_selector_cnt_ = 0;
    for (; OB_SUCC(ret) && curr_idx < batch_size; ++curr_idx) {
      if (OB_NOT_NULL(child_skip) && child_skip->at(curr_idx)) {
        my_skip.set(curr_idx);
        continue;
      }
      int64_t curr_pos = -1;
      bool find_bkt = false;
      while (OB_SUCC(ret) && !find_bkt) {
        locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_, hash_values[curr_idx], curr_pos));
        if (locate_buckets_[curr_idx]->is_valid()) {
          if (probe_by_col) {
            old_row_selector_.at(old_row_selector_cnt_++) = curr_idx;
            __builtin_prefetch(&locate_buckets_[curr_idx]->get_item(),
                          0, 2);
            find_bkt = true;
          } else {
            bool result = true;
            RowItemType *it = &locate_buckets_[curr_idx]->get_item();
            for (int64_t i = 0; OB_SUCC(ret) && result && i < hash_expr_cnt_; ++i) {
              ObIVector *r_vec = gby_exprs_->at(i)->get_vector(*eval_ctx_);
              const bool l_isnull = it->is_null(i);
              const bool r_isnull = r_vec->is_null(curr_idx);
              if (l_isnull != r_isnull) {
                result = false;
              } else if (l_isnull && r_isnull) {
                result = true;
              } else {
                const int64_t l_len = it->get_length(row_meta, i);
                const int64_t r_len = r_vec->get_length(curr_idx);
                if (l_len == r_len && (0 == memcmp(it->get_cell_payload(row_meta, i),
                                                  r_vec->get_payload(curr_idx),
                                                  l_len))) {
                  result = true;
                } else {
                  int cmp_res = 0;
                  if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), curr_idx, false,
                                                  it->get_cell_payload(row_meta, i),
                                                  l_len, cmp_res))) {
                    LOG_WARN("failed to cmp left and right", K(ret));
                  } else {
                    result = (0 == cmp_res);
                  }
                }
              }
            }
            if (OB_SUCC(ret) && result) {
              my_skip.set(curr_idx);
              find_bkt = true;
            }
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          find_bkt = true;
        } else {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
          find_bkt = true;
        }
      }
      if (batch_duplicate) {
        break;
      }
    }
    if (OB_SUCC(ret)
      && probe_by_col
      && old_row_selector_cnt_ > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < hash_expr_cnt_; ++i) {
        col_has_null_.at(i) |= gby_exprs_->at(i)->get_vector(*eval_ctx_)->has_null();
        if (col_has_null_.at(i) ?
            OB_FAIL(inner_process_column(*gby_exprs_, row_meta, i, need_fallback))
            : OB_FAIL(inner_process_column_not_null(*gby_exprs_, row_meta, i, need_fallback))) {
          LOG_WARN("failed to process column", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!need_fallback) {
        for (int64_t i = 0; i < old_row_selector_cnt_; ++i) {
          const int64_t idx = old_row_selector_.at(i);
          my_skip.set(idx);
        }
      } else {
        //reset occupyed bkt and stat
        for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
          locate_buckets_[new_row_selector_.at(i)]->set_empty();
        }
        continue;
      }
    }
    if (OB_FAIL(ret) || 0 == new_row_selector_cnt_) {
    } else if (OB_FAIL(sf(vector_ptrs_, &new_row_selector_.at(0), new_row_selector_cnt_, srows_))) {
      LOG_WARN("failed to append batch", K(ret));
    } else {
      for (int64_t i = 0; i < hash_expr_cnt_; ++i) {
        col_has_null_.at(i) |= gby_exprs_->at(i)->get_vector(*eval_ctx_)->has_null();
      }
      for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
        int64_t idx = new_row_selector_.at(i);
        locate_buckets_[idx]->set_hash(hash_values[idx]);
        locate_buckets_[idx]->set_valid();
        locate_buckets_[idx]->set_item(static_cast<RowItemType&> (*srows_[i]));
        ++size_;
      }
      new_row_selector_cnt_ = 0;
    }
    processed_idx = curr_idx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::likely_equal_nullable(const RowMeta &row_meta,
                                                                const ObCompactRow &left_row,
                                                                const int64_t right_idx,
                                                                bool &result) const
{
  // All rows is in one group, when no group by columns (group by const expr),
  // so the default %result is true
  int ret = OB_SUCCESS;
  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && result && i < hash_expr_cnt_; ++i) {
    bool null_equal = (nullptr == gby_exprs_->at(i));
    if (!null_equal) {
      ObIVector *r_vec = gby_exprs_->at(i)->get_vector(*eval_ctx_);
      const bool l_isnull = left_row.is_null(i);
      const bool r_isnull = r_vec->is_null(right_idx);
      if (l_isnull != r_isnull) {
        result = false;
      } else if (l_isnull && r_isnull) {
        result = true;
      } else {
        const int64_t l_len = left_row.get_length(row_meta, i);
        const int64_t r_len = r_vec->get_length(right_idx);
        if (l_len == r_len && (0 == memcmp(left_row.get_cell_payload(row_meta, i),
                                           r_vec->get_payload(right_idx),
                                           l_len))) {
          result = true;
        } else {
          int cmp_res = 0;
          if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), right_idx, false,
                                           left_row.get_cell_payload(row_meta, i),
                                           l_len, cmp_res))) {
            LOG_WARN("failed to cmp left and right", K(ret));
          } else {
            result = (0 == cmp_res);
          }
        }
      }
    } else {
      result = true;
    }
  }
  return ret;
}

template <typename GroupRowBucket>
void ObExtendHashTableVec<GroupRowBucket>::prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const
{
  if (!sstr_aggr_.is_valid()) {
    int64_t mask = get_bucket_num() - 1;
    for(int64_t i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      }
      __builtin_prefetch((&buckets_->at((ObGroupRowBucketBase::HASH_VAL_MASK & hash_vals[i]) & mask)),
                          0/* read */, 2 /*high temp locality*/);
    }
  }
}

}//ns sql
}//ns oceanbase

#ifdef MARK_MACRO_DEFINED_BY_OB_EXEC_HASH_STRUCT_VEC_H
#undef USING_LOG_PREFIX
#endif

#endif