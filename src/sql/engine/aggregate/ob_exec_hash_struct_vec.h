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

namespace oceanbase
{
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
                    bool &has_new_row)
  {
    int ret = OB_SUCCESS;
    new_row_selector_cnt_ = 0;
    has_new_row = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
      if (nullptr == gby_exprs.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid ptr", K(ret));
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
          LOG_WARN("get invalid format", K(ret), K(col_idx), K(gby_exprs.at(col_idx)->get_format(ctx)));
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
            LOG_WARN("failed to add new rows", K(ret));
          } else {
            ObGroupRowItemVec *new_item = static_cast<ObGroupRowItemVec *> (&srow[0]);
            batch_new_rows[i] = new_item->get_aggr_row(group_store_.get_row_meta());
            grs_array_[idx] = batch_new_rows[i];
            ser_num_array_[idx] = size_;
            CK (OB_NOT_NULL(batch_new_rows[i]));
            has_new_row = true;
            ++size_;
            ++agg_group_cnt;
          }
        } else {
          batch_old_rows[i] = grs_array_[idx];
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
          LOG_WARN("get invalid format", K(ret), K(col_idx), K(gby_exprs.at(col_idx)->get_format(eval_ctx)));
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
      LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    } else if (OB_FAIL(new_row_selector_.prepare_allocate(eval_ctx.max_batch_size_))) {
      LOG_WARN("failed to init selector", K(ret));
    } else if (OB_FAIL(vector_ptrs_.prepare_allocate(group_exprs.count()))) {
      LOG_WARN("failed to init vector ptrs", K(ret));
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

  using BucketArray = common::ObSegmentArray<GroupRowBucket,
                                             OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                             common::ModulePageAllocator,
                                             false,
                                             ObSEArray<GroupRowBucket *, 64, common::ModulePageAllocator, false>,
                                             true>;

  ObExtendHashTableVec(int64_t tenant_id)
    : is_inited_vec_(false),
      auto_extend_(false),
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
      locate_bucket_(nullptr),
      iter_(*this, group_store_.get_row_meta()),
      probe_cnt_(0),
      max_batch_size_(0),
      locate_buckets_(nullptr),
      new_row_selector_(),
      old_row_selector_(),
      col_has_null_(),
      new_row_selector_cnt_(0),
      old_row_selector_cnt_(),
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
        SQL_ENG_LOG(WARN, "failed to get batch", K(ret), K(read_rows), K(max_rows));
      } else {
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
          int64_t initial_size,
          bool auto_extend);
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
                    bool need_reinit_vectors);
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
                          int64_t &processed_idx);
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

  int get_next_batch(const ObCompactRow **rows, const int64_t max_rows, int64_t &read_rows)
  {
    int ret = OB_SUCCESS;
    if (ObGroupRowBucketType::INLINE == GroupRowBucket::TYPE) {
      ret = iter_.get_next_batch_from_table(rows, max_rows, read_rows);
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
    group_store_.reset();
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
      LOG_WARN("sstr aggr can not foreach bkt", K(ret));
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
  bool auto_extend_;
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
  GroupRowBucket *locate_bucket_;
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
          LOG_WARN("extend is prepare allocated", K(old.get_hash()));
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
          LOG_WARN("failed to cmp", K(ret));
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

}//ns sql
}//ns oceanbase

#endif
