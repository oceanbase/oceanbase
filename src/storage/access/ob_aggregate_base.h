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

#ifndef OCEANBASE_STORAGE_OB_AGGREGATE_BASE_H_
#define OCEANBASE_STORAGE_OB_AGGREGATE_BASE_H_

#include <stdint.h>
#include "share/aggregate/agg_ctx.h"
#include "share/ob_compute_property.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "share/aggregate/agg_reuse_cell.h"

namespace oceanbase
{
namespace common
{
class ObBitmap;
}
namespace blocksstable
{
class ObIMicroBlockReader;
struct ObMicroIndexInfo;
}
namespace storage
{
#define USE_GROUP_BY_MAX_DISTINCT_CNT 16384
#define USE_GROUP_BY_BUF_BLOCK_SIZE 256
#define USE_GROUP_BY_BUF_MAX_BLOCK_CNT USE_GROUP_BY_MAX_DISTINCT_CNT / USE_GROUP_BY_BUF_BLOCK_SIZE
static const lib::ObLabel pd_agg_label = "PD_AGGREGATE";

enum ObPDAggType
{
  PD_COUNT = 0,
  PD_MIN,
  PD_MAX,
  PD_SUM,
  PD_FIRST_ROW,
  PD_HLL,
  PD_SUM_OP_SIZE,
  PD_RB_BUILD,
  PD_STR_PREFIX_MIN,
  PD_STR_PREFIX_MAX,
  PD_COUNT_SUM,
  PD_RB_AND,
  PD_RB_OR,
  PD_MAX_TYPE
};

enum FillDatumType
{
  NULL_DATUM,
  ZERO_DATUM
};

struct ObPushdownRowIdCtx
{
public:
  ObPushdownRowIdCtx()
    : row_ids_(nullptr),
      row_cap_(0),
      begin_(-1),
      end_(-1),
      bound_row_id_(OB_INVALID_CS_ROW_ID),
      is_reverse_(false)
  {}
  ObPushdownRowIdCtx(const int32_t *row_ids, int64_t row_cap)
    : row_ids_(row_ids),
      row_cap_(row_cap),
      begin_(-1),
      end_(-1),
      bound_row_id_(OB_INVALID_CS_ROW_ID)
  {
    is_reverse_ = row_cap_ > 1 && row_ids_[1] < row_ids_[0];
  }
  void reuse()
  {
    row_ids_ = nullptr;
    row_cap_ = 0;
    begin_ = -1;
    end_ = -1;
    bound_row_id_ = OB_INVALID_CS_ROW_ID;
    is_reverse_ = false;
  }
  OB_INLINE bool is_valid() const
  {
    bool is_valid_bound = (OB_INVALID_CS_ROW_ID == bound_row_id_
                           || (is_reverse_ && bound_row_id_ <= begin_)
                           || (!is_reverse_ && bound_row_id_ >= end_));
    return (nullptr != row_ids_ && row_cap_ > 0)
        || (nullptr == row_ids_ && begin_ >= 0 && end_ >= begin_ && is_valid_bound);
  }
  OB_INLINE int64_t get_row_count() const
  {
    return row_ids_ == nullptr ? (end_ - begin_ + 1) : row_cap_;
  }
  OB_INLINE int64_t get_row_id(const int64_t idx) const
  {
    int64_t row_id = 0;
    if (row_ids_ != nullptr) {
      row_id = is_reverse_ ? row_ids_[row_cap_ - 1 - idx] : row_ids_[idx];
    } else {
      row_id = begin_ + idx;
    }
    return row_id;
  }
  TO_STRING_KV(KP_(row_ids), K_(row_cap), K_(begin), K_(end), K_(bound_row_id), K_(is_reverse));
  /**
   * if row_ids_ != nullptr, row_ids_ and row_cap_ are valid.
   * if row_ids_ == nullptr, begin_ and end_ are valid. ==> row_ids: [begin_, end_]
   */
  const int32_t *row_ids_;
  int64_t row_cap_;
  int64_t begin_;
  int64_t end_;
  int64_t bound_row_id_; // row_id boundary reached in one pushdown decoder scan.
  bool is_reverse_;
};

// Common interface classes for aggregate pushdown in vectorization 1.0 and 2.0
class ObAggCellBase
{
public:
  ObAggCellBase(common::ObIAllocator &allocator, const share::ObAggrParamProperty &param_prop);
  virtual ~ObAggCellBase() {}
  virtual void reset();
  virtual void reuse();
  /**
    * agg_row_idx:
    *  Effective when group by pushdown
    * agg_batch_size:
    *  Effective when aggregate with expr pushdown,
    *  It indicates the batch size when aggregate batch single rows, 0 means default batch size.
    *  When it is not 1, the last batch must be aggregated explicitly by the invoker.
    */
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0,
      const int64_t agg_batch_size = 0) = 0;
  virtual ObObjType get_obj_type() const = 0;
  int reserve_bitmap(const int64_t size);
  OB_INLINE bool is_assigned_to_group_by_processor() const
  { return is_assigned_to_group_by_processor_; }
  OB_INLINE void set_assigned_to_group_by_processor(bool is_assigned)
  { is_assigned_to_group_by_processor_ = is_assigned; }
  OB_INLINE const ObDatum &get_result_datum() const { return result_datum_; };
  OB_INLINE ObPDAggType get_type() const { return agg_type_; }
  OB_INLINE bool is_min_agg() const
  {
    return (PD_MIN == agg_type_ && is_monotonic_asc()) || (PD_MAX == agg_type_ && is_monotonic_desc());
  }
  OB_INLINE bool is_max_agg() const
  {
    return (PD_MAX == agg_type_ && is_monotonic_asc()) || (PD_MIN == agg_type_ && is_monotonic_desc());
  }
  OB_INLINE ObBitmap &get_bitmap() { return *bitmap_; };
  OB_INLINE void set_ignore_eval_index_info(const bool ignore_eval_index_info) { ignore_eval_index_info_ = ignore_eval_index_info; }
  VIRTUAL_TO_STRING_KV(K_(agg_type), K_(is_inited), K_(param_prop), KP_(agg_row_reader), K_(ignore_eval_index_info),
    K_(result_datum), K_(skip_index_datum), K_(skip_index_datum_is_prefix), K_(is_assigned_to_group_by_processor), KPC_(bitmap));
protected:
  OB_INLINE bool is_monotonic_asc() const { return share::Monotonicity::ASC == param_prop_.mono_; }
  OB_INLINE bool is_monotonic_desc() const { return share::Monotonicity::DESC == param_prop_.mono_; }
  OB_INLINE bool is_monotonic() const { return is_monotonic_asc() || is_monotonic_desc(); }
  OB_INLINE bool is_param_null_prop() const { return param_prop_.is_null_prop_; }
protected:
  ObBitmap *bitmap_;
  blocksstable::ObAggRowReader *agg_row_reader_;
  blocksstable::ObStorageDatum result_datum_;
  blocksstable::ObStorageDatum skip_index_datum_;
  common::ObIAllocator &allocator_;
  ObPDAggType agg_type_;
  share::ObAggrParamProperty param_prop_;
  bool is_assigned_to_group_by_processor_;
  bool skip_index_datum_is_prefix_;
  bool ignore_eval_index_info_;
  bool is_inited_;
};

class ObAggGroupBase
{
public:
  virtual ~ObAggGroupBase() {}
  virtual bool is_vec() const = 0;
  virtual bool check_finished() const = 0;
  virtual int eval(blocksstable::ObStorageDatum &datum, const int64_t row_count) = 0;
  virtual int eval_batch(
      const ObTableIterParam *iter_param,
      const ObTableAccessContext *context,
      const int32_t col_offset,
      blocksstable::ObIMicroBlockReader *reader,
      const ObPushdownRowIdCtx &pd_row_id_ctx) = 0;
  virtual int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, const int32_t col_index, bool &can_agg) = 0;
  virtual int fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObAggStoreBase
{
public:
  virtual ~ObAggStoreBase() {}
  virtual int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg) = 0;
  virtual int fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg) = 0;
  virtual int collect_aggregated_result() = 0;
  virtual int set_ignore_eval_index_info(const bool ignore_eval_index_info) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

// for normal group by pushdown
// some helpful data buffers，the inner memory is discrete and allocated as need
template<typename T>
class ObGroupByExtendableBuf
{
public:
  ObGroupByExtendableBuf(
      T *basic_data,
      const int32_t basic_size,
      const int32_t item_size,
      common::ObIAllocator &allocator);
  ~ObGroupByExtendableBuf() { reset(); };
  void reset();
  int reserve(const int32_t size);
  void fill_items(const T item);
  void fill_datum_items(const FillDatumType type);
  OB_INLINE int get_item(const int32_t pos, T *&item);
  OB_INLINE T &at(const int32_t pos);
  OB_INLINE int32_t get_capacity() { return capacity_; }
  OB_INLINE int32_t get_basic_count() const { return basic_count_; }
  OB_INLINE T *get_basic_buf() { return basic_data_; }
  OB_INLINE bool is_use_extra_data() const { return capacity_ > basic_count_; }
  OB_INLINE void set_item_size(const int32_t item_size) { OB_ASSERT(0 == extra_block_count_); item_size_ = item_size; }
  TO_STRING_KV(K_(capacity), KP_(basic_data), K_(basic_count), K_(extra_block_count), K_(item_size));
private:
  struct BufBlock
  {
    union {
      T *data_;
      ObAggDatumBuf *datum_buf_;
    };
  };
  int alloc_bufblock(BufBlock *&block);
  void free_bufblock(BufBlock *&block);
  void fill_datums(ObDatum *datums, const int32_t count, const FillDatumType datum_type);
  int32_t capacity_;
  int32_t basic_count_;
  T *basic_data_;
  BufBlock *extra_blocks_[USE_GROUP_BY_BUF_MAX_BLOCK_CNT];
  int32_t extra_block_count_;
  int32_t item_size_;
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGroupByExtendableBuf);
};

class ObGroupByCellBase
{
public:
  ObGroupByCellBase(const int64_t batch_size, common::ObIAllocator &allocator);
  virtual ~ObGroupByCellBase();
  virtual void reset();
  virtual void reuse();
  virtual int init(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx) = 0;
  virtual int init_for_single_row(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx) = 0;
  virtual int eval_batch(
      common::ObDatum *datums,
      const int64_t count,
      const int32_t agg_idx,
      const bool is_group_by_col = false,
      const bool is_default_datum = false,
      const uint32_t ref_offset = 0) = 0;
  virtual int copy_output_row(const int64_t batch_idx, const ObTableIterParam &iter_param) = 0;
  virtual int copy_output_rows(const int64_t batch_idx, const ObTableIterParam &iter_param) = 0;
  virtual int copy_single_output_row(const ObTableIterParam &iter_param, sql::ObEvalCtx &ctx) = 0;
  virtual int collect_result() = 0;
  virtual int add_distinct_null_value() = 0;
  virtual int extract_distinct() = 0;
  virtual int output_extra_group_by_result(int64_t &count, const ObTableIterParam &iter_param) = 0;
  virtual int pad_column_in_group_by(const int64_t row_cap) = 0;
  virtual int assign_agg_cells(const sql::ObExpr *col_expr, common::ObIArray<int32_t> &agg_idxs) = 0;
  virtual int clear_agg_cell_assign_status() = 0;
  virtual int check_distinct_and_ref_valid();
  virtual int clear_evaluated_infos() { return OB_SUCCESS; }
  OB_INLINE int64_t get_batch_size() const { return batch_size_; }
  OB_INLINE int32_t get_group_by_col_offset() const { return group_by_col_offset_; }
  OB_INLINE const share::schema::ObColumnParam *get_group_by_col_param() const { return group_by_col_param_; }
  OB_INLINE ObObjDatumMapType get_obj_datum_map_type() const {return group_by_col_expr_->obj_datum_map_; }
  OB_INLINE virtual bool is_exceed_sql_batch() const { return true; }
  virtual common::ObDatum *get_group_by_col_datums_to_fill() = 0;
  virtual const char **get_cell_datas() = 0;
  virtual common::ObDatum *get_group_by_col_datums() const = 0;
  OB_INLINE int64_t get_ref_cnt() const { return ref_cnt_; }
  OB_INLINE void set_ref_cnt(const int64_t ref_cnt) { ref_cnt_ = ref_cnt; }
  OB_INLINE uint32_t *get_refs_buf() { return refs_buf_; }
  OB_INLINE virtual bool need_read_reference() const = 0;
  OB_INLINE virtual bool need_do_aggregate() const = 0;
  OB_INLINE int64_t get_distinct_cnt() const { return distinct_cnt_; }
  OB_INLINE void set_distinct_cnt(const int64_t distinct_cnt) { distinct_cnt_ = distinct_cnt; }
  OB_INLINE bool need_extract_distinct() const { return need_extract_distinct_; }
  OB_INLINE bool is_processing() const { return is_processing_; }
  OB_INLINE void set_is_processing(const bool is_processing)
  {
    is_processing_ = is_processing;
    projected_cnt_ = 0;
  }
  OB_INLINE void set_row_capacity(const int64_t row_capacity) { row_capacity_ = row_capacity; }
  // When decide_use_group_by() return true, only can refresh table when the whole micro block is scanned.
  OB_INLINE bool can_refresh() const { return !is_exceed_sql_batch() || (projected_cnt_ >= distinct_cnt_); }
  template <typename T>
  int decide_use_group_by(const int64_t row_cnt, const int64_t read_cnt, const int64_t distinct_cnt, const T *bitmap, bool &use_group_by)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObGroupByCellVec is not inited", K(ret), K_(is_inited));
    } else {
      const bool is_valid_bitmap = nullptr != bitmap && !bitmap->is_all_true();
      use_group_by = row_capacity_ == batch_size_ &&
                    read_cnt * USE_GROUP_BY_READ_CNT_FACTOR > row_cnt &&
                    distinct_cnt < USE_GROUP_BY_MAX_DISTINCT_CNT &&
                    distinct_cnt < row_cnt * USE_GROUP_BY_DISTINCT_RATIO &&
                    (!is_valid_bitmap ||
                      bitmap->popcnt() * USE_GROUP_BY_FILTER_FACTOR > bitmap->size());
      if (use_group_by) {
        if ((is_valid_bitmap || read_cnt < row_cnt) && OB_FAIL(prepare_tmp_group_by_buf(distinct_cnt + 1))) {
          STORAGE_LOG(WARN, "Failed to init extra info", K(ret));
        } else if (OB_FAIL(reserve_group_by_buf(distinct_cnt + 1))) {
          STORAGE_LOG(WARN, "Failed to prepare group by datum buf", K(ret));
        }
      }
      STORAGE_LOG(TRACE, "[GROUP BY PUSHDOWN]", K(ret), K(row_cnt), K(read_cnt), K(distinct_cnt), K(is_valid_bitmap), K(use_group_by),
          K_(batch_size), K_(row_capacity),
          "popcnt", is_valid_bitmap ? bitmap->popcnt() : 0,
          "size", is_valid_bitmap ? bitmap->size() : 0);
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV(K_(is_inited),
                       K_(batch_size),
                       K_(row_capacity),
                       K_(distinct_cnt),
                       K_(ref_cnt),
                       K_(projected_cnt),
                       KP_(group_by_col_expr),
                       KPC_(group_by_col_param),
                       K_(group_by_col_offset),
                       K_(need_extract_distinct),
                       K_(is_processing),
                       K_(distinct_projector_buf),
                       K(ObArrayWrap<uint32_t>(refs_buf_, ref_cnt_)));
protected:
  virtual int prepare_tmp_group_by_buf(const int64_t size) = 0;
  virtual int reserve_group_by_buf(const int64_t size) = 0;
  static const int64_t DEFAULT_AGG_CELL_CNT = 2;
  static const int64_t USE_GROUP_BY_READ_CNT_FACTOR = 2;
  static constexpr double USE_GROUP_BY_DISTINCT_RATIO = 0.5;
  static const int64_t USE_GROUP_BY_FILTER_FACTOR = 2;
  int64_t batch_size_;
  int64_t row_capacity_;
  int64_t distinct_cnt_;
  int64_t ref_cnt_;
  int64_t projected_cnt_;
  uint32_t *refs_buf_;
  sql::ObExpr *group_by_col_expr_;
  const share::schema::ObColumnParam *group_by_col_param_;
  ObGroupByExtendableBuf<int16_t> *distinct_projector_buf_;
  common::ObArenaAllocator padding_allocator_;
  common::ObIAllocator &allocator_;
  int32_t group_by_col_offset_;
  bool need_extract_distinct_;
  bool is_processing_;
  bool is_inited_;
};

struct ObColOffsetMap
{
  ObColOffsetMap() : col_offset_(-1), agg_idx_(-1) {}
  ObColOffsetMap(const int64_t col_offset, const int64_t agg_idx)
    : col_offset_(col_offset),
      agg_idx_(agg_idx)
  {}
  bool operator < (const ObColOffsetMap &map)
  {
    return col_offset_ < map.col_offset_;
  }
  TO_STRING_KV(K_(col_offset), K_(agg_idx));
  int32_t col_offset_;
  int64_t agg_idx_;
};

struct ObPushdownAggContext
{
public:
  ObPushdownAggContext(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      sql::ObBitVector *skip_bit,
      common::ObIAllocator &allocator);
  virtual ~ObPushdownAggContext();
  void reset();
  void reuse_batch();
  int init(const ObTableAccessParam &param, const int64_t row_count);
  int prepare_aggregate_rows(const int64_t row_count);
  TO_STRING_KV(K_(agg_infos), K_(cols_offset_map), KP_(rows), K_(row_meta), K_(batch_rows), K_(agg_row_num));
private:
  int init_agg_infos(const ObTableAccessParam &param);
  OB_INLINE void setup_agg_row(share::aggregate::AggrRowPtr row, const int32_t row_size);
public:
  common::ObFixedArray<ObAggrInfo, common::ObIAllocator> agg_infos_;
  common::ObFixedArray<ObColOffsetMap, common::ObIAllocator> cols_offset_map_;
  share::aggregate::RuntimeContext agg_ctx_;
  ObCompactRow **rows_;
  RowMeta row_meta_;
  ObBatchRows batch_rows_;
  int64_t agg_row_num_;
  common::ObIAllocator &allocator_;
  common::ObArenaAllocator row_allocator_;
  aggregate::ReuseAggCellMgr reuse_aggrow_mgr_;
};

class ObAggDatumBuf
{
public:
  ObAggDatumBuf(common::ObIAllocator &allocator);
  ~ObAggDatumBuf() { reset(); };
  int init(const int64_t size, const bool need_cell_data_ptr, const int64_t datum_size);
  void reset();
  void reuse();
  OB_INLINE int64_t get_size() const { return size_; }
  OB_INLINE void set_size(const int64_t size) { size_ = size; }
  OB_INLINE int64_t get_capacity() { return capacity_; }
  OB_INLINE common::ObDatum *get_datums() { return datums_; }
  OB_INLINE const char **get_cell_datas() { return cell_data_ptrs_; }
  static int new_agg_datum_buf(
      const int64_t size,
      const bool need_cell_data_ptr,
      common::ObIAllocator &allocator,
      ObAggDatumBuf *&datum_buf,
      const int64_t datum_size = common::OBJ_DATUM_NUMBER_RES_SIZE);
  TO_STRING_KV(K_(size), K_(capacity), K_(datum_size), KP_(datums), KP_(buf), KP_(cell_data_ptrs));
private:
  int64_t size_;
  int64_t capacity_;
  int64_t datum_size_;
  common::ObDatum *datums_;
  char *buf_;
  const char **cell_data_ptrs_;
  common::ObIAllocator &allocator_;
};

// for normal group by pushdown
// store the distinct of group by column, should be continuous ObDatums
class ObAggGroupByDatumBuf
{
public:
  ObAggGroupByDatumBuf(
      common::ObDatum *basic_data,
      const int32_t basic_size,
      const int32_t datum_size,
      common::ObIAllocator &allocator);
  ~ObAggGroupByDatumBuf() { reset(); }
  void reset();
  int reserve(const int32_t size);
  void fill_datums(const FillDatumType datum_type);
  OB_INLINE int32_t get_capacity() const { return capacity_; }
  OB_INLINE common::ObDatum *get_sql_result_datums() { return sql_result_datums_; }
  OB_INLINE common::ObDatum *get_extra_result_datums() { return nullptr == result_datum_buf_ ? nullptr : result_datum_buf_->get_datums(); }
  OB_INLINE common::ObDatum *get_group_by_datums() const { return is_use_extra_buf() ? result_datum_buf_->get_datums() : sql_result_datums_; }
  OB_INLINE const char **get_group_by_cell_datas() const { return is_use_extra_buf() ? result_datum_buf_->get_cell_datas() : nullptr; }
  OB_INLINE bool is_use_extra_buf() const { return capacity_ > sql_datums_cnt_; }
  OB_INLINE int64_t get_extra_buf_size() const {return nullptr == result_datum_buf_ ? 0 : result_datum_buf_->get_size(); }
  TO_STRING_KV(K_(capacity), K_(sql_datums_cnt), KP_(sql_result_datums), KP_(result_datum_buf), K_(datum_size));
private:
  int32_t capacity_;
  int32_t sql_datums_cnt_;
  common::ObDatum *sql_result_datums_; // agg expr's datums
  ObAggDatumBuf *result_datum_buf_; // used in case that the count of distinct values exceed sql batch size
  int32_t datum_size_;
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAggGroupByDatumBuf);
};

template<typename T>
ObGroupByExtendableBuf<T>::ObGroupByExtendableBuf(
    T *basic_data,
    const int32_t basic_size,
    const int32_t item_size,
    common::ObIAllocator &allocator)
    : capacity_(basic_size),
      basic_count_(basic_size),
      basic_data_(basic_data),
      extra_block_count_(0),
      item_size_(item_size),
      allocator_(allocator)
{
  MEMSET(extra_blocks_, 0, sizeof(extra_blocks_));
}

template<typename T>
void ObGroupByExtendableBuf<T>::reset()
{
  capacity_ = 0;
  basic_data_ = nullptr;
  basic_count_ = 0;
  if (extra_block_count_ > 0) {
    for (int64_t i = 0; i < extra_block_count_; ++i) {
      free_bufblock(extra_blocks_[i]);
    }
  }
  extra_block_count_ = 0;
  item_size_ = 0;
}


template<typename T>
int ObGroupByExtendableBuf<T>::reserve(const int32_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0 || size > USE_GROUP_BY_MAX_DISTINCT_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Unexpected size", K(ret), K(size));
  } else {
    capacity_ = MAX(basic_count_, size);
    int32_t cur_capacity = basic_count_ + extra_block_count_ * USE_GROUP_BY_BUF_BLOCK_SIZE;
    if (capacity_ > cur_capacity) {
      int32_t required_block_cnt = ceil((double)(size - cur_capacity) / USE_GROUP_BY_BUF_BLOCK_SIZE);
      for (int64_t i = 0; OB_SUCC(ret) && i < required_block_cnt; ++i) {
        if (OB_FAIL(alloc_bufblock(extra_blocks_[extra_block_count_]))) {
          STORAGE_LOG(WARN, "Failed to allock buf block", K(ret));
        } else {
          extra_block_count_++;
        }
      }
    }
  }
  return ret;
}

template<typename T>
void ObGroupByExtendableBuf<T>::fill_items(const T item)
{
  if (capacity_ <= basic_count_) {
    MEMSET(basic_data_, item, item_size_ * capacity_);
  } else {
    if (basic_count_ > 0) {
      MEMSET(basic_data_, item, item_size_ * basic_count_);
    }
    const int32_t used_block_cnt = ceil((double)(capacity_ - basic_count_) / USE_GROUP_BY_BUF_BLOCK_SIZE);
    for (int64_t i = 0; i < used_block_cnt; ++i) {
      if (i < used_block_cnt - 1) {
         MEMSET(extra_blocks_[i]->data_, item, item_size_ * USE_GROUP_BY_BUF_BLOCK_SIZE);
      } else {
        const int32_t remain_cnt = capacity_ - basic_count_ - (used_block_cnt - 1) * USE_GROUP_BY_BUF_BLOCK_SIZE;
        MEMSET(extra_blocks_[i]->data_, item, item_size_ * remain_cnt);
      }
    }
  }
}

template<typename T>
void ObGroupByExtendableBuf<T>::fill_datum_items(const FillDatumType type)
{
  if (capacity_ <= basic_count_) {
    fill_datums(basic_data_, capacity_, type);
  } else {
    if (basic_count_ > 0) {
      fill_datums(basic_data_, basic_count_, type);
    }
    const int32_t used_block_cnt = ceil((double)(capacity_ - basic_count_) / USE_GROUP_BY_BUF_BLOCK_SIZE);
    for (int64_t i = 0; i < used_block_cnt; ++i) {
      if (i < used_block_cnt - 1) {
        fill_datums(extra_blocks_[i]->datum_buf_->get_datums(), USE_GROUP_BY_BUF_BLOCK_SIZE, type);
      } else {
        const int32_t remain_cnt = capacity_ - basic_count_ - (used_block_cnt - 1) * USE_GROUP_BY_BUF_BLOCK_SIZE;
        fill_datums(extra_blocks_[i]->datum_buf_->get_datums(), remain_cnt, type);
      }
    }
  }
}

template<typename T>
int ObGroupByExtendableBuf<T>::alloc_bufblock(BufBlock *&block)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(BufBlock)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
  } else if(FALSE_IT(block = new (buf) BufBlock())) {
  } else if (OB_ISNULL(buf = allocator_.alloc(item_size_ * USE_GROUP_BY_BUF_BLOCK_SIZE))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
  } else {
    block->data_ =reinterpret_cast<T*>(buf);
  }
  return ret;
}

template<>
OB_INLINE int ObGroupByExtendableBuf<ObDatum>::alloc_bufblock(BufBlock *&block)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(BufBlock)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
  } else if(FALSE_IT(block = new (buf) BufBlock())) {
  } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(
      USE_GROUP_BY_BUF_BLOCK_SIZE, false, allocator_, block->datum_buf_, item_size_))) {
    STORAGE_LOG(WARN, "Failed to alloc agg datum buf", K(ret));
  }
  return ret;
}

template<typename T>
void ObGroupByExtendableBuf<T>::free_bufblock(BufBlock *&block)
{
  if (nullptr != block) {
    if (nullptr != block->data_) {
      allocator_.free(block->data_);
    }
    allocator_.free(block);
    block = nullptr;
  }
}

template<>
OB_INLINE void ObGroupByExtendableBuf<ObDatum>::free_bufblock(BufBlock *&block)
{
  if (nullptr != block) {
    if (nullptr != block->datum_buf_) {
      block->datum_buf_->~ObAggDatumBuf();
      allocator_.free(block->datum_buf_);
    }
    allocator_.free(block);
    block = nullptr;
  }
}

template<typename T>
void ObGroupByExtendableBuf<T>::fill_datums(ObDatum *datums, const int32_t count, const FillDatumType datum_type)
{
  if (FillDatumType::NULL_DATUM == datum_type) {
    for (int64_t i = 0; i < count; ++i) {
      datums[i].set_null();
    }
  } else if (FillDatumType::ZERO_DATUM == datum_type) {
    for (int64_t i = 0; i < count; ++i) {
      datums[i].set_int(0);
    }
  }
}

template<typename T>
int ObGroupByExtendableBuf<T>::get_item(const int32_t pos, T *&item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos >= get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(pos), KPC(this));
  } else {
    item = &at(pos);
  }
  return ret;
}

template<typename T>
OB_INLINE T& ObGroupByExtendableBuf<T>::at(const int32_t pos)
{
  OB_ASSERT(pos < get_capacity());
  if (pos < basic_count_) {
    return basic_data_[pos];
  } else {
    const int32_t block_idx = (pos - basic_count_) / USE_GROUP_BY_BUF_BLOCK_SIZE;
    const int32_t data_offset = pos - basic_count_ - block_idx * USE_GROUP_BY_BUF_BLOCK_SIZE;
    return extra_blocks_[block_idx]->data_[data_offset];
  }
}

template<>
OB_INLINE ObDatum& ObGroupByExtendableBuf<ObDatum>::at(const int32_t pos)
{
  OB_ASSERT(pos < get_capacity());
  if (pos < basic_count_) {
    return basic_data_[pos];
  } else {
    const int32_t block_idx = (pos - basic_count_) / USE_GROUP_BY_BUF_BLOCK_SIZE;
    const int32_t data_offset = pos - basic_count_ - block_idx * USE_GROUP_BY_BUF_BLOCK_SIZE;
    return extra_blocks_[block_idx]->datum_buf_->get_datums()[data_offset];
  }
}

template<typename DATA_TYPE, typename BUF_TYPE>
int new_group_by_buf(
    DATA_TYPE *basic_data,
    const int32_t basic_size,
    const int32_t item_size,
    common::ObIAllocator &allocator,
    BUF_TYPE *&group_by_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_size <= 0 || item_size > common::OBJ_DATUM_DECIMALINT_MAX_RES_SIZE ||
      (nullptr != basic_data && basic_size <= 0) ||
      nullptr == basic_data && basic_size > 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(basic_data), K(basic_size));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(BUF_TYPE)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret));
    } else {
      group_by_buf = new (buf) BUF_TYPE(basic_data, basic_size, item_size, allocator);
    }
  }
  return ret;
}

template<typename BUF_TYPE>
void free_group_by_buf(common::ObIAllocator &allocator, BUF_TYPE *&group_by_buf)
{
  if (nullptr != group_by_buf) {
    group_by_buf->~BUF_TYPE();
    allocator.free(group_by_buf);
    group_by_buf = nullptr;
  }
}

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_AGGREGATE_BASE_H_ */
