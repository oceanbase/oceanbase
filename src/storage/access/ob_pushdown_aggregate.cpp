/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_pushdown_aggregate.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/aggregate/ob_aggregate_util.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/lob/ob_lob_manager.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObAggDatumBuf::ObAggDatumBuf(common::ObIAllocator &allocator)
  : size_(0), datum_size_(0), datums_(nullptr), buf_(nullptr), cell_data_ptrs_(nullptr), allocator_(allocator)
{
}

int ObAggDatumBuf::init(const int64_t size, const bool need_cell_data_ptr, const int64_t datum_size)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(size <= 0 || datum_size <= 0 || datum_size > common::OBJ_DATUM_DECIMALINT_MAX_RES_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(size), K(datum_size));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDatum) * size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc datum buf", K(ret), K(size));
  } else if (FALSE_IT(datums_ = new (buf) ObDatum[size])) {
  } else if (OB_ISNULL(buf = allocator_.alloc(datum_size * size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc datum buf", K(ret), K(size));
  } else if (FALSE_IT(buf_ = static_cast<char*>(buf))) {
  } else if (need_cell_data_ptr) {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(char*) * size))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc cell data ptrs", K(ret), K(size));
    } else {
      cell_data_ptrs_ = static_cast<const char**> (buf);
    }
  }
  if (OB_SUCC(ret)) {
    size_ = size;
    datum_size_ = datum_size;
    reuse();
  } else {
    reset();
  }
  return ret;
}

void ObAggDatumBuf::reset()
{
  if (OB_NOT_NULL(datums_)) {
    allocator_.free(datums_);
    datums_ = nullptr;
  }
  if (OB_NOT_NULL(buf_)) {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  if (OB_NOT_NULL(cell_data_ptrs_)) {
    allocator_.free(cell_data_ptrs_);
    cell_data_ptrs_ = nullptr;
  }
  size_ = 0;
  datum_size_ = 0;
}

void ObAggDatumBuf::reuse()
{
  for(int64_t i = 0; i < size_; ++i) {
    datums_[i].pack_ = 0;
    datums_[i].ptr_ = buf_ + i * datum_size_;
  }
}

int ObAggDatumBuf::new_agg_datum_buf(
    const int64_t size,
    const bool need_cell_data_ptr,
    common::ObIAllocator &allocator,
    ObAggDatumBuf *&datum_buf,
    const int64_t datum_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == datum_buf) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObAggDatumBuf)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc agg datum buffer", K(ret));
    } else if (FALSE_IT(datum_buf = new (buf) ObAggDatumBuf(allocator))) {
    } else if (OB_FAIL(datum_buf->init(size, need_cell_data_ptr, datum_size))) {
      LOG_WARN("Failed to init agg datum buf", K(ret));
    }
  }
  return ret;
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
    LOG_WARN("Invalid argument", K(ret), KP(basic_data), K(basic_size));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(BUF_TYPE)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret));
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

ObAggGroupByDatumBuf::ObAggGroupByDatumBuf(
    common::ObDatum *basic_data,
    const int32_t basic_size,
    const int32_t datum_size,
    common::ObIAllocator &allocator)
    : capacity_(basic_size),
      sql_datums_cnt_(basic_size),
      sql_result_datums_(basic_data),
      result_datum_buf_(nullptr),
      datum_size_(datum_size),
      allocator_(allocator)
{
}

void ObAggGroupByDatumBuf::reset()
{
  capacity_ = 0;
  sql_result_datums_ = nullptr;
  sql_datums_cnt_ = 0;
  if (nullptr != result_datum_buf_) {
    result_datum_buf_->reset();
    allocator_.free(result_datum_buf_);
  }
  result_datum_buf_ = nullptr;
  datum_size_ = 0;
}

int ObAggGroupByDatumBuf::reserve(const int32_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0 || size > USE_GROUP_BY_MAX_DISTINCT_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected size", K(ret), K(size));
  } else {
    capacity_ = MAX(sql_datums_cnt_, size);
    if (is_use_extra_buf()) {
      if (OB_ISNULL(result_datum_buf_)) {
        if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(USE_GROUP_BY_MAX_DISTINCT_CNT,
            true, allocator_, result_datum_buf_, datum_size_))) {
          LOG_WARN("Failed to alloc agg datum buf", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObAggGroupByDatumBuf::fill_datums(const FillDatumType datum_type)
{
  common::ObDatum *datums = get_group_by_datums();
  if (FillDatumType::NULL_DATUM == datum_type) {
    for (int64_t i = 0; i < capacity_; ++i) {
      datums[i].set_null();
    }
  } else if (FillDatumType::ZERO_DATUM == datum_type) {
    for (int64_t i = 0; i < capacity_; ++i) {
      datums[i].set_int(0);
    }
  }
}

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
    LOG_WARN("Unexpected size", K(ret), K(size));
  } else {
    capacity_ = MAX(basic_count_, size);
    int32_t cur_capacity = basic_count_ + extra_block_count_ * USE_GROUP_BY_BUF_BLOCK_SIZE;
    if (capacity_ > cur_capacity) {
      int32_t required_block_cnt = ceil((double)(size - cur_capacity) / USE_GROUP_BY_BUF_BLOCK_SIZE);
      for (int64_t i = 0; OB_SUCC(ret) && i < required_block_cnt; ++i) {
        if (OB_FAIL(alloc_bufblock(extra_blocks_[extra_block_count_]))) {
          LOG_WARN("Failed to allock buf block", K(ret));
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
    LOG_WARN("Failed to alloc memory", K(ret));
  } else if(FALSE_IT(block = new (buf) BufBlock())) {
  } else if (OB_ISNULL(buf = allocator_.alloc(item_size_ * USE_GROUP_BY_BUF_BLOCK_SIZE))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory", K(ret));
  } else {
    block->data_ =reinterpret_cast<T*>(buf);
  }
  return ret;
}

template<>
int ObGroupByExtendableBuf<ObDatum>::alloc_bufblock(BufBlock *&block)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(BufBlock)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory", K(ret));
  } else if(FALSE_IT(block = new (buf) BufBlock())) {
  } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(
      USE_GROUP_BY_BUF_BLOCK_SIZE, false, allocator_, block->datum_buf_, item_size_))) {
    LOG_WARN("Failed to alloc agg datum buf", K(ret));
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
void ObGroupByExtendableBuf<ObDatum>::free_bufblock(BufBlock *&block)
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

ObAggCell::ObAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : agg_type_(ObPDAggType::PD_MAX_TYPE),
      basic_info_(basic_info),
      result_datum_(),
      def_datum_(),
      skip_index_datum_(),
      allocator_(allocator),
      is_lob_col_(false),
      aggregated_(false),
      agg_datum_buf_(nullptr),
      agg_row_reader_(nullptr),
      col_datums_(nullptr),
      group_by_result_datum_buf_(nullptr),
      bitmap_(nullptr),
      group_by_result_cnt_(0),
      is_assigned_to_group_by_processor_(false),
      padding_allocator_("ObStorageAgg", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  if (basic_info_.col_param_ != nullptr) {
    is_lob_col_ = basic_info_.col_param_->get_meta_type().is_lob_storage();
  }
}

void ObAggCell::reset()
{
  agg_type_ = ObPDAggType::PD_MAX_TYPE;
  basic_info_.reset();
  result_datum_.reset();
  skip_index_datum_.reset();
  is_lob_col_ = false;
  aggregated_ = false;
  if (nullptr != agg_datum_buf_) {
    agg_datum_buf_->reset();
    allocator_.free(agg_datum_buf_);
    agg_datum_buf_ = nullptr;
  }
  if (nullptr != agg_row_reader_) {
    agg_row_reader_->reset();
    allocator_.free(agg_row_reader_);
    agg_row_reader_ = nullptr;
  }
  if (nullptr != bitmap_) {
    allocator_.free(bitmap_);
    bitmap_ = nullptr;
  }
  col_datums_ = nullptr;
  free_group_by_buf(allocator_, group_by_result_datum_buf_);
  group_by_result_cnt_ = 0;
  is_assigned_to_group_by_processor_ = false;
  padding_allocator_.reset();
}

void ObAggCell::reuse()
{
  aggregated_ = false;
  result_datum_.reuse();
  result_datum_.set_null();
  skip_index_datum_.reuse();
  skip_index_datum_.set_null();
  group_by_result_cnt_ = 0;
  if (nullptr != bitmap_) {
    bitmap_->reuse();
  }
  if (nullptr != col_datums_) {
    for (int64_t i = 0; i < basic_info_.batch_size_; ++i) {
      col_datums_[i].set_null();
    }
  }
  padding_allocator_.reuse();
}

void ObAggCell::clear_group_by_info()
{
  if (nullptr != group_by_result_datum_buf_) {
    if (PD_COUNT != agg_type_) {
      group_by_result_datum_buf_->fill_datum_items(FillDatumType::NULL_DATUM);
    } else {
      group_by_result_datum_buf_->fill_datum_items(FillDatumType::ZERO_DATUM);
    }
  }
}

int ObAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (is_group_by) {
    if (PD_FIRST_ROW != agg_type_) {
      common::ObDatum *result_datums = basic_info_.agg_expr_->locate_batch_datums(*eval_ctx);
      if (OB_ISNULL(result_datums)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null agg datums", K(ret));
      } else if (OB_FAIL(new_group_by_buf(result_datums, basic_info_.batch_size_,
          common::OBJ_DATUM_NUMBER_RES_SIZE, allocator_, group_by_result_datum_buf_))) {
        LOG_WARN("Failed to new buf", K(ret));
      } else if (OB_UNLIKELY(nullptr == basic_info_.agg_expr_->args_ ||
          nullptr == basic_info_.agg_expr_->args_[0] ||
          basic_info_.agg_expr_->args_[0]->type_ != T_REF_COLUMN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("args is invalid", K(ret), KPC(basic_info_.agg_expr_));
      } else if (OB_ISNULL(col_datums_ = basic_info_.agg_expr_->args_[0]->locate_batch_datums(*eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null col datums", K(ret));
      }
    } else if (OB_ISNULL(col_datums_ = basic_info_.agg_expr_->locate_batch_datums(*eval_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null col datums", K(ret));
    }
  }
  return ret;
}

int ObAggCell::eval_micro_block(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null col param", K(ret));
  } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(basic_info_.batch_size_, true, allocator_, agg_datum_buf_))) {
    LOG_WARN("Failed to alloc agg datum buf", K(ret));
  } else if (OB_FAIL(reader->get_aggregate_result(iter_param, context, col_offset, *basic_info_.col_param_, row_ids, row_count, *agg_datum_buf_, *this))) {
    LOG_WARN("Failed to get aggregate result", K(ret));
  }
  return ret;
}

int ObAggCell::eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret), K(index_info));
  } else if (OB_UNLIKELY(skip_index_datum_.is_null())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected skip index datum is null", K(ret), K(index_info));
  } else if (OB_FAIL(eval(skip_index_datum_))) {
    LOG_WARN("Failed to process datum", K(ret), K(skip_index_datum_), KPC(this));
  }
  return ret;
}

int ObAggCell::copy_output_row(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset >= group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    const common::ObDatum &datum = col_datums_[datum_offset];
    if (OB_FAIL(result_datums[datum_offset].from_storage_datum(
        datum, basic_info_.agg_expr_->obj_datum_map_))) {
      LOG_WARN("Failed to clone datum", K(ret), K(datum), K(basic_info_.agg_expr_->obj_datum_map_));
    }
  }
  return ret;
}

int ObAggCell::copy_output_rows(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset > group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_offset; i++) {
      // Although one batch of rows will be returned to SQL layer directly
      // it's needed to use 'from_storage_datum' to avoid overrided prevous memory of datum
      // as the ptr of datum may be not redirected to SQL memory and will be overrided in following 'eval_batch_in_group_by'
      if (OB_FAIL(result_datums[i].from_storage_datum(
          col_datums_[i], basic_info_.agg_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to clone datum", K(ret), K(i), K(col_datums_[i]), K(basic_info_.agg_expr_->obj_datum_map_));
      }
    }
  }
  return ret;
}

int ObAggCell::copy_single_output_row(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.agg_expr_->args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null args of agg expr", K(ret), KPC(basic_info_.agg_expr_));
  } else {
    sql::ObDatum &datum = basic_info_.agg_expr_->args_[0]->locate_expr_datum(ctx);
    common::ObDatum &result_datum = basic_info_.agg_expr_->locate_datum_for_write(ctx);
    if (OB_FAIL(result_datum.from_storage_datum(datum, basic_info_.agg_expr_->obj_datum_map_))) {
      LOG_WARN("Failed to clone datum", K(ret), K(datum), K(basic_info_.agg_expr_->obj_datum_map_));
    }
  }
  return ret;
}

int ObAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObDatum &result = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  if (OB_FAIL(fill_default_if_need(result_datum_))) {
    LOG_WARN("Failed to fill default", K(ret), KPC(this));
  } else if (OB_FAIL(result.from_storage_datum(result_datum_, basic_info_.agg_expr_->obj_datum_map_))) {
    LOG_WARN("Failed to from storage datum", K(ret), K(result_datum_), K(result), KPC(this));
  } else {
    sql::ObEvalInfo &eval_info = basic_info_.agg_expr_->get_eval_info(ctx);
    eval_info.evaluated_ = true;
    LOG_DEBUG("collect_result", K(result), KPC(this));
  }
  return ret;
}

int ObAggCell::collect_batch_result_in_group_by(const int64_t distinct_cnt)
{
  UNUSED(distinct_cnt);
  return OB_SUCCESS;
}

int ObAggCell::reserve_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_by_result_datum_buf_->reserve(size))) {
    LOG_WARN("Failed to prepare extra buf", K(ret));
  } else {
    clear_group_by_info();
  }
  return ret;
}

int ObAggCell::output_extra_group_by_result(const int64_t start, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start < group_by_result_datum_buf_->get_basic_count() ||
      start + count > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected argument, should be not exceed the buf size",
      K(ret), K(start), K(count), KPC(group_by_result_datum_buf_));
  } else if (OB_UNLIKELY(!group_by_result_datum_buf_->is_use_extra_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum *sql_result_datums = group_by_result_datum_buf_->get_basic_buf();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(sql_result_datums[i].from_storage_datum(
          group_by_result_datum_buf_->at(start + i), basic_info_.agg_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to output extra buf", K(ret));
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(start), K(count),
        K(common::ObArrayWrap<common::ObDatum>(sql_result_datums, count)));
  }
  return ret;
}

int ObAggCell::pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator)
{
  return OB_SUCCESS;
}

int ObAggCell::reserve_bitmap(const int64_t count)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(bitmap_)) {
    if (OB_FAIL(bitmap_->reserve(count))) {
      LOG_WARN("Failed to reserve bitmap", K(ret));
    } else {
      bitmap_->reuse(); // all false
    }
  } else {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory for bitmap", K(ret));
    } else if (FALSE_IT(bitmap_ = new (buf) ObBitmap(allocator_))) {
    } else if (OB_FAIL(bitmap_->init(count))) { // all false
      LOG_WARN("Failed to init bitmap", K(ret));
    }
  }
  return ret;
}

int ObAggCell::prepare_def_datum()
{
  int ret = OB_SUCCESS;
  if (def_datum_.is_nop()) {
    def_datum_.reuse();
    const ObObj &def_cell = basic_info_.col_param_->get_orig_default_value();
    if (!def_cell.is_nop_value()) {
      if (OB_FAIL(def_datum_.from_obj_enhance(def_cell))) {
        STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
      } else if (OB_FAIL(pad_column_if_need(def_datum_, allocator_, false))) {
        LOG_WARN("Failed to pad default datum", K(ret), K_(basic_info), K_(def_datum));
      } else if (def_cell.is_lob_storage() && !def_cell.is_null()) {
        // lob def value must have no lob header when not null, should add lob header for default value
        ObString data = def_datum_.get_string();
        ObString out;
        if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, data, out))) {
          LOG_WARN("failed to fill lob header for column.", K(ret), K(def_cell), K(data));
        } else {
          def_datum_.set_string(out);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, virtual column is not supported", K(ret), K(basic_info_.col_offset_));
    }
  }
  return ret;
}

int ObAggCell::fill_default_if_need(blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, col param is null", K(ret), K(basic_info_.col_offset_));
  } else if (datum.is_nop()) {
    if (OB_FAIL(prepare_def_datum())) {
      LOG_WARN("failed to prepare default datum", K(ret));
    } else {
      datum.reuse();
      const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(basic_info_.col_param_->get_meta_type().get_type());
      if (OB_FAIL(datum.from_storage_datum(def_datum_, map_type))) {
        LOG_WARN("Failed to from storage datum", K(ret), K(def_datum_), K(datum), K(*this));
      }
    }
  }
  return ret;
}

int ObAggCell::pad_column_if_need(blocksstable::ObStorageDatum &datum, common::ObIAllocator &padding_allocator, bool alloc_need_reuse)
{
  int ret = OB_SUCCESS;
  if (alloc_need_reuse){
    padding_allocator.reuse();
  }
  if (OB_ISNULL(basic_info_.col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, col param is null", K(ret), K(basic_info_.col_offset_));
  } else if (!basic_info_.need_padding()) {
  } else if (OB_FAIL(pad_column(basic_info_.col_param_->get_meta_type(), basic_info_.col_param_->get_accuracy(), padding_allocator, datum))) {
    LOG_WARN("Fail to pad column", K(ret), K(basic_info_.col_offset_), KPC(this));
  }
  return ret;
}

int ObAggCell::deep_copy_datum(const blocksstable::ObStorageDatum &src, common::ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  if (src.is_null() || src.is_nop()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected datum", K(ret), K(src));
  } else {
    if (!result_datum_.is_local_buf()) {
      tmp_alloc.reuse();
    }
    if (OB_FAIL(result_datum_.deep_copy(src, tmp_alloc))) {
      LOG_WARN("Failed to deep copy", K(ret), K(src), K(result_datum_));
    }
  }
  return ret;
}

int ObAggCell::can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg, bool &can_agg)
{
  int ret = OB_SUCCESS;
  if (index_info.has_agg_data() && can_use_index_info()) {
    if (OB_FAIL(read_agg_datum(index_info, is_cg))) {
      LOG_WARN("fail to read agg datum", K(ret), K(is_cg), K(index_info));
    } else {
      can_agg = !skip_index_datum_.is_null();
    }
  } else {
    can_agg = false;
  }
  return ret;
}

int ObAggCell::read_agg_datum(
    const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (nullptr == agg_row_reader_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(blocksstable::ObAggRowReader)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc agg row reader", K(ret));
    } else {
      agg_row_reader_ = new (buf) blocksstable::ObAggRowReader();
    }
  }
  if (OB_SUCC(ret)) {
    skip_index_datum_.reuse();
    skip_index_datum_.set_null();
    blocksstable::ObSkipIndexColMeta meta;
    // TODO: @luhaopeng.lhp fix col_index in cg, use 0 temporarily
    meta.col_idx_ = is_cg ? 0 : static_cast<uint32_t>(basic_info_.col_index_);
    switch (agg_type_) {
      case ObPDAggType::PD_SUM_OP_SIZE:
      case ObPDAggType::PD_COUNT: {
        meta.col_type_ = blocksstable::SK_IDX_NULL_COUNT;
        break;
      }
      case ObPDAggType::PD_MIN: {
        meta.col_type_ = blocksstable::SK_IDX_MIN;
        break;
      }
      case ObPDAggType::PD_MAX: {
        meta.col_type_ = blocksstable::SK_IDX_MAX;
        break;
      }
      case ObPDAggType::PD_SUM: {
        meta.col_type_ = blocksstable::SK_IDX_SUM;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected agg type", K(agg_type_));
      }
    }
    agg_row_reader_->reset();
    if (OB_FAIL(agg_row_reader_->init(index_info.agg_row_buf_, index_info.agg_buf_size_))) {
      LOG_WARN("Fail to init aggregate row reader", K(ret));
    } else if (OB_FAIL(agg_row_reader_->read(meta, skip_index_datum_))) {
      LOG_WARN("Failed read aggregate row", K(ret), K(meta));
    }
  }
  return ret;
}

struct ObDummyNumber
{
public:
  ObDummyNumber(int &cons_ret)
  {
    int ret = OB_SUCCESS;
    common::ObDataBuffer local_alloc0(number_local_buff0_, common::number::ObNumber::MAX_BYTE_LEN );
    common::ObDataBuffer local_alloc1(number_local_buff1_, common::number::ObNumber::MAX_BYTE_LEN );
    if (OB_FAIL(number_zero_.from(static_cast<int64_t>(0), local_alloc0))) {
      LOG_WARN("Failed to cons number from int", K(ret));
    } else if (OB_FAIL(number_one_.from(static_cast<int64_t>(1), local_alloc1))) {
      LOG_WARN("Failed to cons number from int", K(ret));
    }
    cons_ret = ret;
  }
  ~ObDummyNumber() {}
  static char number_local_buff0_[common::number::ObNumber::MAX_BYTE_LEN];
  static char number_local_buff1_[common::number::ObNumber::MAX_BYTE_LEN];
  static common::number::ObNumber number_zero_;
  static common::number::ObNumber number_one_;
};

char ObDummyNumber::number_local_buff0_[common::number::ObNumber::MAX_BYTE_LEN];
char ObDummyNumber::number_local_buff1_[common::number::ObNumber::MAX_BYTE_LEN];
common::number::ObNumber ObDummyNumber::number_zero_;
common::number::ObNumber ObDummyNumber::number_one_;

ObCountAggCell::ObCountAggCell(
    const ObAggCellBasicInfo &basic_info,
    common::ObIAllocator &allocator,
    bool exclude_null)
    : ObAggCell(basic_info, allocator),
      exclude_null_(exclude_null),
      row_count_(0)
{
  agg_type_ = ObPDAggType::PD_COUNT;
}

void ObCountAggCell::reset()
{
  ObAggCell::reset();
  exclude_null_ = false;
  row_count_ = 0;
}

void ObCountAggCell::reuse()
{
  ObAggCell::reuse();
  row_count_ = 0;
}

int ObCountAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (exclude_null_) {
    if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
      LOG_WARN("Failed to init agg cell", K(ret));
    }
  } else if (is_group_by) {
    common::ObDatum *result_datums = basic_info_.agg_expr_->locate_batch_datums(*eval_ctx);
    if (OB_ISNULL(result_datums)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null agg datums", K(ret));
    } else if (OB_FAIL(new_group_by_buf(result_datums, basic_info_.batch_size_,
        common::OBJ_DATUM_NUMBER_RES_SIZE, allocator_, group_by_result_datum_buf_))) {
      LOG_WARN("Failed to new buf", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_group_by && lib::is_oracle_mode()) {
    static ObDummyNumber dummy(ret);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to cons number from int", K(ret));
    }
  }
  return ret;
}

int ObCountAggCell::eval(blocksstable::ObStorageDatum &datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(ret), K(row_count));
  } else if (!exclude_null_) {
    row_count_ += row_count;
  } else if (OB_FAIL(fill_default_if_need(datum))) {
    LOG_WARN("Failed to fill default", K(ret), KPC(this));
  } else if (!datum.is_null()) {
    row_count_ += row_count;
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("after count row", K(ret), K(row_count_));
  return ret;
}

int ObCountAggCell::eval_batch(const common::ObDatum *datums, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(ret), KP(datums), K(row_count));
  } else if (OB_UNLIKELY(exclude_null_ || nullptr != datums)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not read data", K(ret), K(exclude_null_), KP(datums));
  } else {
    row_count_ += row_count;
    aggregated_ = true;
  }
  LOG_DEBUG("after count batch", K(ret), K(row_count), K(row_count_));
  return ret;
}

int ObCountAggCell::eval_micro_block(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (!exclude_null_) {
    row_count_ += row_count;
  } else {
    int64_t valid_row_count = 0;
    if (OB_ISNULL(row_ids)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, row_ids is null", K(ret), KPC(this), K(row_count));
    } else if (OB_FAIL(reader->get_row_count(col_offset, row_ids, row_count, false, basic_info_.col_param_, valid_row_count))) {
      LOG_WARN("Failed to get row count from micro block decoder", K(ret), KPC(this), K(row_count));
    } else {
      row_count_ += valid_row_count;
    }
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("eval_micro_block", K(ret), K(row_count), K(row_count_));
  return ret;
}

int ObCountAggCell::eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret));
  } else if (!exclude_null_) {
    row_count_ += index_info.get_row_count();
  } else if (OB_UNLIKELY(skip_index_datum_.is_null())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected skip index datum is null", K(ret), K(index_info));
  } else {
    row_count_ += index_info.get_row_count() - skip_index_datum_.get_int();
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("eval_index_info", K(ret), K(index_info.get_row_count()), K(row_count_));
  return ret;
}

template<bool EXCLUDE_NULL, bool GROUP_BY_COL>
void eval_batch_for_count(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    ObGroupByExtendableBuf<ObDatum> *result_datums)
{}

template<>
void eval_batch_for_count<false, false>(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    ObGroupByExtendableBuf<ObDatum> *result_datums)
{
  for (int64_t i = 0; i < count; ++i) {
    common::ObDatum &result_datum = result_datums->at(refs[i]);
    result_datum.set_int(result_datum.get_int() + 1);
  }
}

template<>
void eval_batch_for_count<true, false>(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    ObGroupByExtendableBuf<ObDatum> *result_datums)
{
  for (int64_t i = 0; i < count; ++i) {
    common::ObDatum &result_datum = result_datums->at(refs[i]);
    if (!datums[i].is_null()) {
      result_datum.set_int(result_datum.get_int() + 1);
    }
  }
}

template<>
void eval_batch_for_count<true, true>(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    ObGroupByExtendableBuf<ObDatum> *result_datums)
{
  for (int64_t i = 0; i < count; ++i) {
    common::ObDatum &result_datum = result_datums->at(refs[i]);
    if (!datums[refs[i]].is_null()) {
      result_datum.set_int(result_datum.get_int() + 1);
    }
  }
}

typedef void (*eval_batch_for_count_func) (
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    ObGroupByExtendableBuf<ObDatum> *result_datums);
static eval_batch_for_count_func EVAL_BATCH_COUNT_FUNCS[2][2] = {
  {eval_batch_for_count<false, false>, eval_batch_for_count<false, false>},
  {eval_batch_for_count<true, false>, eval_batch_for_count<true, true>}
};

int ObCountAggCell::eval_batch_in_group_by(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_default_datum)) {
    const bool should_inc = !exclude_null_ || !datums[0].is_null();
    if (should_inc) {
      for (int64_t i = 0; i < count; ++i) {
        common::ObDatum &result_datum = group_by_result_datum_buf_->at(refs[i]);
        result_datum.set_int(result_datum.get_int() + 1);
      }
    }
  } else {
    eval_batch_for_count_func eval_func = EVAL_BATCH_COUNT_FUNCS[exclude_null_][is_group_by_col];
    eval_func(datums, count, refs, group_by_result_datum_buf_);
  }
  return ret;
}

int ObCountAggCell::copy_output_row(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset >= group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum &result_datum = group_by_result_datum_buf_->at(datum_offset);
    if (exclude_null_) {
      const common::ObDatum &datum = col_datums_[datum_offset];
      if (!datum.is_null()) {
        lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_one_) : result_datum.set_int(1);
      } else {
        lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_zero_) : result_datum.set_int(0);
      }
    } else {
      lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_one_) : result_datum.set_int(1);
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(datum_offset), K(result_datum));
  }
  return ret;
}

int ObCountAggCell::copy_output_rows(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset > group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    if (exclude_null_) {
      if (lib::is_oracle_mode()) {
        for (int64_t i = 0; i < datum_offset; i++) {
          col_datums_[i].is_null() ? result_datums[i].set_number(ObDummyNumber::number_zero_) :
              result_datums[i].set_number(ObDummyNumber::number_one_);
        }
      } else {
        for (int64_t i = 0; i < datum_offset; i++) {
          col_datums_[i].is_null() ? result_datums[i].set_int(0) : result_datums[i].set_int(1);
        }
      }
    } else if (lib::is_oracle_mode()) {
      for (int64_t i = 0; i < datum_offset; i++) {
        result_datums[i].set_number(ObDummyNumber::number_one_);
      }
    } else {
      for (int64_t i = 0; i < datum_offset; i++) {
        result_datums[i].set_int(1);
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(datum_offset),
        K(common::ObArrayWrap<common::ObDatum>(col_datums_, datum_offset)),
        K(common::ObArrayWrap<common::ObDatum>(result_datums, datum_offset)));
  }
  return ret;
}

int ObCountAggCell::copy_single_output_row(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  if (exclude_null_) {
    if (OB_ISNULL(basic_info_.agg_expr_->args_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null args of agg expr", K(ret), KPC(basic_info_.agg_expr_));
    } else {
      sql::ObDatum &datum = basic_info_.agg_expr_->args_[0]->locate_expr_datum(ctx);
      if (!datum.is_null()) {
        lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_one_) : result_datum.set_int(1);
      } else {
        lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_zero_) : result_datum.set_int(0);
      }
    }
  } else {
    lib::is_oracle_mode() ? result_datum.set_number(ObDummyNumber::number_one_) : result_datum.set_int(1);
  }
  return ret;
}

int ObCountAggCell::collect_batch_result_in_group_by(const int64_t distinct_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(distinct_cnt > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(distinct_cnt), KPC(group_by_result_datum_buf_));
  } else if (lib::is_oracle_mode()) {
    common::number::ObNumber result_num;
    char local_buf[common::number::ObNumber::MAX_BYTE_LEN];
    common::ObDataBuffer local_alloc(local_buf, common::number::ObNumber::MAX_BYTE_LEN);
    for (int64_t i = 0; OB_SUCC(ret) && i < distinct_cnt; ++i) {
      local_alloc.free();
      common::ObDatum &result_datum = group_by_result_datum_buf_->at(i);
      const int64_t row_count = result_datum.get_int();
      if (OB_FAIL(result_num.from(row_count, local_alloc))) {
        LOG_WARN("Failed to cons number from int", K(ret), K(row_count));
      } else {
        result_datum.set_number(result_num);
      }
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(distinct_cnt));
  return ret;
}

int ObCountAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObDatum &result = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  sql::ObEvalInfo &eval_info = basic_info_.agg_expr_->get_eval_info(ctx);
  if (lib::is_oracle_mode()) {
    common::number::ObNumber result_num;
    char local_buff[common::number::ObNumber::MAX_BYTE_LEN];
    common::ObDataBuffer local_alloc(local_buff, common::number::ObNumber::MAX_BYTE_LEN);
    if (OB_FAIL(result_num.from(row_count_, local_alloc))) {
      LOG_WARN("Failed to cons number from int", K(ret), K(row_count_));
    } else {
      result.set_number(result_num);
      eval_info.evaluated_ = true;
    }
  } else {
    result.set_int(row_count_);
    eval_info.evaluated_ = true;
  }
  LOG_DEBUG("collect_result", K(result), KPC(this));
  return ret;
}

ObMinAggCell::ObMinAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : ObAggCell(basic_info, allocator),
      group_by_ref_array_(nullptr),
      datum_allocator_("ObStorageAgg", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  agg_type_ =ObPDAggType::PD_MIN;
  result_datum_.set_null();
}

void ObMinAggCell::reset()
{
  ObAggCell::reset();
  if (nullptr != group_by_ref_array_) {
    allocator_.free(group_by_ref_array_);
    group_by_ref_array_ = nullptr;
  }
  datum_allocator_.reset();
}

void ObMinAggCell::reuse()
{
  ObAggCell::reuse();
  datum_allocator_.reuse();
}

int ObMinAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.agg_expr_->basic_funcs_) ||
      OB_ISNULL(basic_info_.agg_expr_->basic_funcs_->null_first_cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected agg expr", K(ret), K(basic_info_.agg_expr_));
  } else if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else {
    cmp_fun_ = basic_info_.agg_expr_->basic_funcs_->null_first_cmp_;
  }
  return ret;
}

int ObMinAggCell::eval(blocksstable::ObStorageDatum &storage_datum, const int64_t row_count)
{
  UNUSED(row_count);
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (OB_FAIL(fill_default_if_need(storage_datum))) {
    LOG_WARN("Failed to fill default", K(ret), K(storage_datum), K(*this));
  } else if (storage_datum.is_null()) {
  } else if (OB_FAIL(pad_column_if_need(storage_datum, padding_allocator_))) {
    LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(storage_datum));
  } else if (result_datum_.is_null()) {
    if (OB_FAIL(result_datum_.deep_copy(storage_datum, datum_allocator_))) {
      LOG_WARN("Failed to deep copy datum", K(ret), K(storage_datum), K(basic_info_.col_offset_));
    }
  } else if (OB_FAIL(cmp_fun_(result_datum_, storage_datum, cmp_ret))) {
      LOG_WARN("Failed to compare", K(ret), K(result_datum_), K(storage_datum));
  } else if (cmp_ret > 0 && OB_FAIL(deep_copy_datum(storage_datum, datum_allocator_))) {
    LOG_WARN("Failed to deep copy datum", K(ret), K(storage_datum), K(result_datum_), K(basic_info_.col_offset_));
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  return ret;
}

int ObMinAggCell::eval_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), KP(datums), K(count));
  } else {
    int cmp_ret = 0;
    blocksstable::ObStorageDatum tmp_min_datum;
    tmp_min_datum.set_null();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const common::ObDatum &datum = datums[i];
      if (datum.is_null()) {
      } else if (tmp_min_datum.is_null()) {
        tmp_min_datum.shallow_copy_from_datum(datum);
      } else if (OB_FAIL(cmp_fun_(tmp_min_datum, datum, cmp_ret))) {
          LOG_WARN("Failed to compare", K(ret), K(tmp_min_datum), K(datum));
      } else if (cmp_ret > 0) {
        tmp_min_datum.shallow_copy_from_datum(datum);
      }
    }
    if (OB_FAIL(ret) || tmp_min_datum.is_null()) {
    } else if (result_datum_.is_null()) {
      if (OB_FAIL(deep_copy_datum(tmp_min_datum, datum_allocator_))) {
        LOG_WARN("Failed to deep copy datum", K(ret), K(tmp_min_datum), K(result_datum_), K(basic_info_.col_offset_));
      }
    } else if (OB_FAIL(cmp_fun_(result_datum_, tmp_min_datum, cmp_ret))) {
      LOG_WARN("Failed to compare", K(ret), K(result_datum_), K(tmp_min_datum));
    } else if (cmp_ret > 0) {
      if (OB_FAIL(deep_copy_datum(tmp_min_datum, datum_allocator_))) {
        LOG_WARN("Failed to deep copy datum", K(ret), K(tmp_min_datum), K(result_datum_), K(basic_info_.col_offset_));
      }
    }
    if (OB_SUCC(ret)) {
      aggregated_ = true;
    }
  }
  return ret;
}

int ObMinAggCell::eval_batch_in_group_by(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count > basic_info_.batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(count), K(basic_info_.batch_size_));
  } else if (OB_UNLIKELY(nullptr == cmp_fun_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), KP(cmp_fun_));
  } else {
    const bool read_distinct_val = is_group_by_col || is_default_datum;
    const bool need_deep_copy = !read_distinct_val && OBJ_DATUM_STRING == basic_info_.agg_expr_->obj_datum_map_;
    if (need_deep_copy && nullptr == group_by_ref_array_) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(uint32_t) * basic_info_.batch_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory", K(ret), K(basic_info_.batch_size_));
      } else {
        group_by_ref_array_ = static_cast<uint32_t*>(buf);
        MEMSET(group_by_ref_array_, -1, basic_info_.batch_size_);
      }
    }
    int64_t updated_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const uint32_t distinct_ref = refs[i];
      const uint32_t dictinct_datum_offset = is_default_datum ? 0 : refs[i];
      const common::ObDatum &datum = read_distinct_val ? datums[dictinct_datum_offset] : datums[i];
      if (datum.is_null()) {
      } else {
        common::ObDatum &result_datum = group_by_result_datum_buf_->at(distinct_ref);
        int cmp_ret = 0;
        if (!result_datum.is_null() && OB_FAIL(cmp_fun_(result_datum, datum, cmp_ret))) {
          LOG_WARN("Failed to cmp", K(ret), K(result_datum), K(datum));
        } else if (result_datum.is_null() || cmp_ret > 0) {
          // This function may be invoked for many times in one micro block,
          // so instead of '=', it's needed to use 'from_storage_datum' to avoid overrided prevous memory of datum
          if (OB_FAIL(result_datum.from_storage_datum(datum, basic_info_.agg_expr_->obj_datum_map_))) {
            LOG_WARN("Failed to clone datum", K(ret), K(datum), K(basic_info_.agg_expr_->obj_datum_map_));
          } else if (need_deep_copy) {
            group_by_ref_array_[updated_cnt++] = distinct_ref;
          }
        }
      }
    }
    // deep copy updated distinct values
    // as there is no guarantee of the validness of the previous memory in next round
    if (OB_FAIL(ret)) {
    } else if (updated_cnt > 0) {
      lib::ob_sort(group_by_ref_array_, group_by_ref_array_ + updated_cnt);
      int64_t last_ref_cnt = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < updated_cnt; ++i) {
        if (-1 == group_by_ref_array_[i]) {
        } else if (last_ref_cnt == group_by_ref_array_[i]) {
          group_by_ref_array_[i] = -1;
        } else {
          common::ObDatum &result_datum = group_by_result_datum_buf_->at(group_by_ref_array_[i]);
          if (OB_FAIL(result_datum.deep_copy(result_datum, datum_allocator_))) {
            LOG_WARN("Failed to deep copy distinct datum", K(ret));
          } else {
            last_ref_cnt = group_by_ref_array_[i];
            group_by_ref_array_[i] = -1;
          }
        }
      }
    }
  }
  return ret;
}

int ObMinAggCell::pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  common::ObDatum *sql_result_datums = group_by_result_datum_buf_->get_basic_buf();
  if (basic_info_.need_padding() &&
      OB_FAIL(storage::pad_on_datums(
              basic_info_.col_param_->get_accuracy(),
              basic_info_.col_param_->get_meta_type().get_collation_type(),
              allocator,
              row_cap,
              sql_result_datums))) {
    LOG_WARN("Failed to pad aggregate column in group by", K(ret), KPC(this));
  }
  return ret;
}

ObMaxAggCell::ObMaxAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : ObAggCell(basic_info, allocator),
      group_by_ref_array_(nullptr),
      datum_allocator_("ObStorageAgg", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  agg_type_ = ObPDAggType::PD_MAX;
  result_datum_.set_null();
}

void ObMaxAggCell::reset()
{
  ObAggCell::reset();
  if (nullptr != group_by_ref_array_) {
    allocator_.free(group_by_ref_array_);
    group_by_ref_array_ = nullptr;
  }
  datum_allocator_.reset();
}

void ObMaxAggCell::reuse()
{
  ObAggCell::reuse();
  datum_allocator_.reuse();
}

int ObMaxAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.agg_expr_->basic_funcs_) ||
      OB_ISNULL(basic_info_.agg_expr_->basic_funcs_->null_first_cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected agg expr", K(ret), K(basic_info_.agg_expr_));
  } else if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else {
    cmp_fun_ = basic_info_.agg_expr_->basic_funcs_->null_first_cmp_;
  }
  return ret;
}

int ObMaxAggCell::eval(blocksstable::ObStorageDatum &storage_datum, const int64_t row_count)
{
  UNUSED(row_count);
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (OB_FAIL(fill_default_if_need(storage_datum))) {
    LOG_WARN("Failed to fill default", K(ret), K(storage_datum), K(*this));
  } else if (storage_datum.is_null()) {
  } else if (OB_FAIL(pad_column_if_need(storage_datum, padding_allocator_))) {
    LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(storage_datum));
  } else if (result_datum_.is_null()) {
    if (OB_FAIL(result_datum_.deep_copy(storage_datum, datum_allocator_))) {
      LOG_WARN("Failed to deep copy datum", K(ret), K(storage_datum), K(basic_info_.col_offset_));
    }
  } else if (OB_FAIL(cmp_fun_(result_datum_, storage_datum, cmp_ret))) {
    LOG_WARN("Failed to compare", K(ret), K(result_datum_), K(storage_datum));
  } else if (cmp_ret < 0 && OB_FAIL(deep_copy_datum(storage_datum, datum_allocator_))) {
    LOG_WARN("Failed to deep copy datum", K(ret), K(storage_datum), K(result_datum_), K(basic_info_.col_offset_));
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  return ret;
}

int ObMaxAggCell::eval_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), KP(datums), K(count));
  } else {
    int cmp_ret = 0;
    blocksstable::ObStorageDatum tmp_min_datum;
    tmp_min_datum.set_null();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const common::ObDatum &datum = datums[i];
      if (datum.is_null()) {
      } else if (tmp_min_datum.is_null()) {
        tmp_min_datum.shallow_copy_from_datum(datum);
      } else if (OB_FAIL(cmp_fun_(tmp_min_datum, datum, cmp_ret))) {
          LOG_WARN("Failed to compare", K(ret), K(tmp_min_datum), K(datum));
      } else if (cmp_ret < 0) {
        tmp_min_datum.shallow_copy_from_datum(datum);
      }
    }
    if (OB_FAIL(ret) || tmp_min_datum.is_null()) {
    } else if (result_datum_.is_null()) {
      if (OB_FAIL(deep_copy_datum(tmp_min_datum, datum_allocator_))) {
        LOG_WARN("Failed to deep copy datum", K(ret), K(tmp_min_datum), K(result_datum_), K(basic_info_.col_offset_));
      }
    } else if (OB_FAIL(cmp_fun_(result_datum_, tmp_min_datum, cmp_ret))) {
      LOG_WARN("Failed to compare", K(ret), K(result_datum_), K(tmp_min_datum));
    } else if (cmp_ret < 0) {
      if (OB_FAIL(deep_copy_datum(tmp_min_datum, datum_allocator_))) {
        LOG_WARN("Failed to deep copy datum", K(ret), K(tmp_min_datum), K(result_datum_), K(basic_info_.col_offset_));
      }
    }
    if (OB_SUCC(ret)) {
      aggregated_ = true;
    }
  }
  return ret;
}

int ObMaxAggCell::eval_batch_in_group_by(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count > basic_info_.batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(count), K(basic_info_.batch_size_));
  } else if (OB_UNLIKELY(nullptr == cmp_fun_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), KP(cmp_fun_));
  } else {
    const bool read_distinct_val = is_group_by_col || is_default_datum;
    const bool need_deep_copy = !read_distinct_val && OBJ_DATUM_STRING == basic_info_.agg_expr_->obj_datum_map_;
    if (need_deep_copy && nullptr == group_by_ref_array_) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(uint32_t) * basic_info_.batch_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory", K(ret), K(basic_info_.batch_size_));
      } else {
        group_by_ref_array_ = static_cast<uint32_t*>(buf);
        MEMSET(group_by_ref_array_, -1, basic_info_.batch_size_);
      }
    }
    int64_t updated_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const uint32_t distinct_ref = refs[i];
      const uint32_t dictinct_datum_offset = is_default_datum ? 0 : refs[i];
      const common::ObDatum &datum = read_distinct_val ? datums[dictinct_datum_offset] : datums[i];
      if (datum.is_null()) {
      } else {
        common::ObDatum &result_datum = group_by_result_datum_buf_->at(distinct_ref);
        int cmp_ret = 0;
        if (!result_datum.is_null() && OB_FAIL(cmp_fun_(result_datum, datum, cmp_ret))) {
          LOG_WARN("Failed to cmp", K(ret), K(result_datum), K(datum));
        } else if (result_datum.is_null() || cmp_ret < 0) {
          if (OB_FAIL(result_datum.from_storage_datum(datum, basic_info_.agg_expr_->obj_datum_map_))) {
            LOG_WARN("Failed to clone datum", K(ret), K(datum), K(basic_info_.agg_expr_->obj_datum_map_));
          } else if (need_deep_copy) {
            group_by_ref_array_[updated_cnt++] = distinct_ref;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (updated_cnt > 0) {
      lib::ob_sort(group_by_ref_array_, group_by_ref_array_ + updated_cnt);
      int64_t last_ref_cnt = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < updated_cnt; ++i) {
        if (-1 == group_by_ref_array_[i]) {
        } else if (last_ref_cnt == group_by_ref_array_[i]) {
          group_by_ref_array_[i] = -1;
        } else {
          common::ObDatum &result_datum = group_by_result_datum_buf_->at(group_by_ref_array_[i]);
          if (OB_FAIL(result_datum.deep_copy(result_datum, datum_allocator_))) {
            LOG_WARN("Failed to deep copy distinct datum", K(ret));
          } else {
            last_ref_cnt = group_by_ref_array_[i];
            group_by_ref_array_[i] = -1;
          }
        }
      }
    }
  }
  return ret;
}

int ObMaxAggCell::pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  common::ObDatum *sql_result_datums = group_by_result_datum_buf_->get_basic_buf();
  if (basic_info_.need_padding() &&
      OB_FAIL(storage::pad_on_datums(
              basic_info_.col_param_->get_accuracy(),
              basic_info_.col_param_->get_meta_type().get_collation_type(),
              allocator,
              row_cap,
              sql_result_datums))) {
    LOG_WARN("Failed to pad aggregate column in group by", K(ret), KPC(this));
  }
  return ret;
}

ObHyperLogLogAggCell::ObHyperLogLogAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : ObAggCell(basic_info, allocator),
      hash_func_(nullptr),
      ndv_calculator_(),
      def_hash_value_(0)
{
  agg_type_ = ObPDAggType::PD_HLL;
}

void ObHyperLogLogAggCell::reset()
{
  hash_func_ = nullptr;
  if (OB_ISNULL(ndv_calculator_)) {
    ndv_calculator_->destroy();
    allocator_.free(ndv_calculator_);
    ndv_calculator_ = nullptr;
  }
  def_hash_value_ = 0;
  ObAggCell::reset();
}

void ObHyperLogLogAggCell::reuse()
{
  ObAggCell::reuse();
  if (OB_ISNULL(ndv_calculator_)) {
    ndv_calculator_->reuse();
  }
}

int ObHyperLogLogAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(basic_info_.agg_expr_->args_) ||
      OB_ISNULL(basic_info_.agg_expr_->args_[0]) ||
      OB_ISNULL(basic_info_.agg_expr_->args_[0]->basic_funcs_) ||
      OB_ISNULL(basic_info_.agg_expr_->args_[0]->basic_funcs_->murmur_hash_) ||
      OB_UNLIKELY(T_REF_COLUMN != basic_info_.agg_expr_->args_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected agg expr", K(ret), K(basic_info_.agg_expr_));
  } else if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObHyperLogLogCalculator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for hyperloglog calculator", K(ret));
  } else {
    ndv_calculator_ = new (buf) ObHyperLogLogCalculator();
    if (OB_FAIL(ndv_calculator_->init(&allocator_, LLC_BUCKET_BITS))) {
      LOG_WARN("Failed to init ndv calculator", K(ret));
    } else {
      hash_func_ = basic_info_.agg_expr_->args_[0]->basic_funcs_->murmur_hash_;
    }
  }

  if (OB_SUCC(ret) && nullptr != basic_info_.col_param_ &&
        !(basic_info_.col_param_->get_orig_default_value().is_nop_value())) {
    uint64_t hash_value = 0;
    if (OB_FAIL(prepare_def_datum())) {
      LOG_WARN("Failed to prepare default datum", K(ret));
    } else if (def_datum_.is_null()) {
    } else if (OB_FAIL(hash_func_(def_datum_, hash_value, hash_value))) {
      LOG_WARN("Failed to do hash", K(ret));
    } else {
      def_hash_value_ = hash_value;
    }
  }
  return ret;
}

// fuse padding datums in expr, storage_datum is not padding
int ObHyperLogLogAggCell::eval(blocksstable::ObStorageDatum &storage_datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0; // same as ObAggregateProcessor llc hash_value
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (storage_datum.is_nop()) {
    if (!def_datum_.is_null()) {
      ndv_calculator_->set(def_hash_value_);
    }
  } else if (storage_datum.is_null()) {
    // ndv does not consider null
  } else if (OB_FAIL(pad_column_if_need(storage_datum, padding_allocator_))) {
    LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(storage_datum));
  } else if (OB_FAIL(hash_func_(storage_datum, hash_value, hash_value))) {
    LOG_WARN("Failed to do hash", K(ret));
  } else {
    ndv_calculator_->set(hash_value);
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  return ret;
}

// eval_batch() is invoked by ObAggCell::eval_micro_block()
// Like Min/Max aggregate, does not have the capability for batch processing now.
int ObHyperLogLogAggCell::eval_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(ret), KP(datums), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      uint64_t hash_value = 0; // same as ObAggregateProcessor llc hash_value
      const common::ObDatum &datum = datums[i];
      if (datum.is_null()) {
        // ndv does not consider null
      } else if (OB_FAIL(hash_func_(datum, hash_value, hash_value))) {
        LOG_WARN("Failed to do hash", K(ret));
      } else {
        ndv_calculator_->set(hash_value);
      }
    }
    if (OB_SUCC(ret)) {
      aggregated_ = true;
    }
  }
  LOG_DEBUG("after set ndv hash batch", K(count), K_(ndv_calculator));
  return ret;
}

int ObHyperLogLogAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObDatum &result = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  sql::ObEvalInfo &eval_info = basic_info_.agg_expr_->get_eval_info(ctx);
  result.set_string(ndv_calculator_->get_buckets(), ndv_calculator_->get_bucket_num());
  eval_info.evaluated_ = true;
  LOG_DEBUG("collect result", K_(ndv_calculator), K(result), KPC(this));
  return ret;
}

ObSumOpSizeAggCell::ObSumOpSizeAggCell(
    const ObAggCellBasicInfo &basic_info,
    common::ObIAllocator &allocator,
    const bool exclude_null)
    : ObAggCell(basic_info, allocator),
      op_size_(0),
      def_op_size_(0),
      total_size_(0),
      exclude_null_(exclude_null)
{
  agg_type_ = ObPDAggType::PD_SUM_OP_SIZE;
}

void ObSumOpSizeAggCell::reset()
{
  op_size_ = 0;
  def_op_size_ = 0;
  total_size_ = 0;
  exclude_null_ = false;
  ObAggCell::reset();
}

void ObSumOpSizeAggCell::reuse()
{
  ObAggCell::reuse();
  total_size_ = 0;
}

int ObSumOpSizeAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else if (OB_FAIL(set_op_size())) {
    LOG_WARN("Failed to get op size", K(ret));
  } else if (nullptr != basic_info_.col_param_ &&
             !(basic_info_.col_param_->get_orig_default_value().is_nop_value())) {
    if (OB_FAIL(prepare_def_datum())) {
      LOG_WARN("Failed to prepare default datum", K(ret));
    } else if (OB_FAIL(get_datum_op_size(def_datum_, def_op_size_))) {
      LOG_WARN("Failed to get default datum length", K(ret), K_(def_datum));
    }
  }
  return ret;
}

int ObSumOpSizeAggCell::set_op_size()
{
  int ret = OB_SUCCESS;
  ObObjDatumMapType type = OBJ_DATUM_MAPPING_MAX;
  if (OB_ISNULL(basic_info_.agg_expr_->args_) ||
      OB_ISNULL(basic_info_.agg_expr_->args_[0]) ||
      OB_UNLIKELY(T_REF_COLUMN != basic_info_.agg_expr_->args_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is null", K(ret), KPC(basic_info_.agg_expr_));
  } else if (FALSE_IT(type = basic_info_.agg_expr_->args_[0]->obj_datum_map_)) {
  } else if (OB_UNLIKELY(type >= common::OBJ_DATUM_MAPPING_MAX || type <= common::OBJ_DATUM_NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type", K(ret), K(type));
  } else if (is_fixed_length_type()) {
    switch (type) {
      case common::OBJ_DATUM_8BYTE_DATA : {
        op_size_ = sizeof(ObDatum) + 8;
        break;
      }
      case common::OBJ_DATUM_4BYTE_DATA : {
        op_size_ = sizeof(ObDatum) + 4;
        break;
      }
      case common::OBJ_DATUM_1BYTE_DATA : {
        op_size_ = sizeof(ObDatum) + 1;
        break;
      }
      case common::OBJ_DATUM_4BYTE_LEN_DATA : {
        op_size_ = sizeof(ObDatum) + 12;
        break;
      }
      case common::OBJ_DATUM_2BYTE_LEN_DATA : {
        op_size_ = sizeof(ObDatum) + 10;
        break;
      }
      case common::OBJ_DATUM_FULL : {
        op_size_ = sizeof(ObDatum) + 16;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected fixed length type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObSumOpSizeAggCell::get_datum_op_size(const ObDatum &datum, int64_t &length)
{
  int ret = OB_SUCCESS;
  if (!is_lob_col() || datum.is_null()) {
    length = sizeof(ObDatum) + datum.len_;
  } else {
    ObLobLocatorV2 locator(datum.get_string(), basic_info_.agg_expr_->args_[0]->obj_meta_.has_lob_header());
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      LOG_WARN("Failed to get lob data byte len", K(ret), K(locator));
    } else {
      length = sizeof(ObDatum) + lob_data_byte_len;
    }
  }
  return ret;
}

int ObSumOpSizeAggCell::eval(blocksstable::ObStorageDatum &storage_datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (storage_datum.is_nop()) {
    total_size_ += def_op_size_ * row_count;
  } else if (OB_FAIL(pad_column_if_need(storage_datum, padding_allocator_))) {
    LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(storage_datum));
  } else if (OB_FAIL(get_datum_op_size(storage_datum, length))) {
    LOG_WARN("Failed to get datum length", K(ret), K(storage_datum));
  } else {
    total_size_ += length * row_count; // row_count is not always 1
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("after sum op size", K(ret), K(length), K_(total_size), K_(op_size), K_(def_op_size));
  return ret;
}

// eval_batch() is invoked by ObAggCell::eval_micro_block()
// Like Min/Max aggregate, does not have the capability for batch processing now.
int ObSumOpSizeAggCell::eval_batch(const common::ObDatum *datums, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  if (OB_UNLIKELY(nullptr == datums || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), KP(datums), K(row_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      // consider lob
      const ObDatum datum = datums[i];
      if (OB_FAIL(get_datum_op_size(datum, length))) {
        LOG_WARN("Failed to get datum length", K(ret), K(datum));
      } else {
        total_size_ += length;
      }
    }
    if (OB_SUCC(ret)) {
      aggregated_ = true;
    }
  }
  LOG_DEBUG("after sum op size batch", K(ret), K(row_count), K_(op_size), K_(total_size));
  return ret;
}

int ObSumOpSizeAggCell::eval_micro_block(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  if (need_access_data()) {
    if (is_fixed_length_type()) {
      int64_t valid_row_count = 0;
      if (OB_ISNULL(row_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected, row_ids is null", K(ret), KPC(this), K(row_count));
      } else if (OB_FAIL(reader->get_row_count(col_offset, row_ids, row_count, false, basic_info_.col_param_, valid_row_count))) {
        LOG_WARN("Failed to get row count from micro block decoder", K(ret), KPC(this), K(row_count));
      } else {
        total_size_ += (row_count - valid_row_count) * sizeof(ObDatum) + valid_row_count * op_size_;
      }
    } else if (OB_FAIL(ObAggCell::eval_micro_block(iter_param, context, col_offset, reader, row_ids, row_count))) {
      LOG_WARN("Failed to eval micro block", K(ret));
    }
  } else {
    total_size_ += row_count * op_size_;
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("eval micro block in sum op size", K(ret), K(row_count), K_(exclude_null), K_(op_size), K_(total_size));
  return ret;
}

int ObSumOpSizeAggCell::eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  // consider the judge condition
  if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret));
  } else if (!exclude_null_) {
    total_size_ += index_info.get_row_count() * op_size_;
  } else if (OB_UNLIKELY(skip_index_datum_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected skip index datum, is null", K(ret), K(index_info));
  } else {
    int64_t null_count = skip_index_datum_.get_int();
    total_size_ += (index_info.get_row_count() - null_count) * op_size_ + null_count * sizeof(ObDatum);
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("eval_index_info", K(ret), K(index_info.get_row_count()), K_(op_size), K_(total_size));
  return ret;
}

int ObSumOpSizeAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObDatum &result = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  sql::ObEvalInfo &eval_info = basic_info_.agg_expr_->get_eval_info(ctx);
  if (lib::is_oracle_mode()) {
    common::number::ObNumber result_num;
    char local_buff[common::number::ObNumber::MAX_BYTE_LEN];
    common::ObDataBuffer local_alloc(local_buff, common::number::ObNumber::MAX_BYTE_LEN);
    if (OB_FAIL(result_num.from(total_size_, local_alloc))) {
      LOG_WARN("Failed to cons number from uint", K(ret), K_(total_size));
    } else {
      result.set_number(result_num);
      eval_info.evaluated_ = true;
    }
  } else {
    result.set_uint(total_size_);
    eval_info.evaluated_ = true;
  }
  LOG_DEBUG("collect result", K(result), KPC(this));
 return ret;
}

ObSumAggCell::ObSumAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : ObAggCell(basic_info, allocator),
      obj_tc_(ObNullTC),
      sum_use_int_flag_(false),
      is_sum_use_temp_buf_(false),
      num_int_(0),
      sum_use_int_flag_buf_(nullptr),
      num_buf_(nullptr),
      eval_func_(nullptr),
      eval_batch_func_(nullptr),
      copy_datum_func_(nullptr),
      cast_datum_(),
      sum_temp_buffer_(nullptr),
      cast_temp_buffer_(nullptr)
{
  agg_type_ = ObPDAggType::PD_SUM;
  result_datum_.set_null();
}

void ObSumAggCell::reset()
{
  obj_tc_ = ObNullTC;
  sum_use_int_flag_ = false;
  num_int_ = 0;
  free_group_by_buf(allocator_, sum_use_int_flag_buf_);
  free_group_by_buf(allocator_, num_int_buf_);
  eval_func_ = nullptr;
  eval_batch_func_ = nullptr;
  copy_datum_func_ = nullptr;
  if (is_sum_use_temp_buf_) {
    if (nullptr != sum_temp_buffer_) {
      allocator_.free(sum_temp_buffer_);
      sum_temp_buffer_ = nullptr;
    }
    if (nullptr != cast_temp_buffer_) {
      allocator_.free(cast_temp_buffer_);
      cast_temp_buffer_ = nullptr;
    }
    is_sum_use_temp_buf_ = false;
  }
  cast_datum_.reset();
  ObAggCell::reset();
}

void ObSumAggCell::reuse()
{
  ObAggCell::reuse();
  if (is_sum_use_temp_buf_) {
    result_datum_.ptr_ = sum_temp_buffer_;
    cast_datum_.ptr_ = cast_temp_buffer_;
  }
  sum_use_int_flag_ = false;
  num_int_ = 0;
  // reset_aggregate_info();
}

void ObSumAggCell::clear_group_by_info()
{
  if (nullptr != sum_use_int_flag_buf_) {
    sum_use_int_flag_buf_->fill_items(false);
  }
  if (nullptr != num_int_buf_) {
    num_int_buf_->fill_items(0);
  }
}

void ObSumAggCell::reset_aggregate_info()
{
  if (nullptr != group_by_result_datum_buf_) {
    ObDatum *datums = group_by_result_datum_buf_->get_basic_buf();
    for (int64_t i = 0; i < group_by_result_datum_buf_->get_basic_count(); ++i) {
      datums[i].set_null();
    }
    if (sum_use_int_flag_buf_->get_basic_count() > 0) {
      MEMSET(sum_use_int_flag_buf_->get_basic_buf(), false, sizeof(bool) * sum_use_int_flag_buf_->get_basic_count());
    }
    if (num_int_buf_->get_basic_count() > 0) {
      MEMSET(num_int_buf_->get_basic_buf(), 0, sizeof(int64_t) * num_int_buf_->get_basic_count());
    }
  }
}

int ObSumAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == basic_info_.agg_expr_->args_ ||
      nullptr == basic_info_.agg_expr_->args_[0] ||
      T_REF_COLUMN != basic_info_.agg_expr_->args_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args is NULL", K(ret), KPC(basic_info_.agg_expr_));
  } else if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else {
    obj_tc_ = ob_obj_type_class(basic_info_.agg_expr_->args_[0]->datum_meta_.type_);
    const ObObjTypeClass res_tc = ob_obj_type_class(basic_info_.agg_expr_->datum_meta_.type_);
    const int16_t precision = basic_info_.agg_expr_->datum_meta_.precision_;
    eval_skip_index_func_ = nullptr;
    switch (obj_tc_) {
      case ObObjTypeClass::ObIntTC: {
        switch (res_tc) {
          case ObObjTypeClass::ObNumberTC: {
            eval_func_ = &ObSumAggCell::eval_int<number::ObNumber>;
            eval_batch_func_ = &ObSumAggCell::eval_int_batch<number::ObNumber>;
            copy_datum_func_ = &ObSumAggCell::copy_int_to_number;
            eval_skip_index_func_ = &ObSumAggCell::eval_number;
            break;
          }
          case ObObjTypeClass::ObDecimalIntTC: {
            if (OB_UNLIKELY(precision < OB_DECIMAL_LONGLONG_DIGITS ||
                              precision > MAX_PRECISION_DECIMAL_INT_256)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected precision", K(ret), K(precision));
            } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
              eval_func_ = &ObSumAggCell::eval_int<int128_t>;
              eval_batch_func_ = &ObSumAggCell::eval_int_batch<int128_t>;
              copy_datum_func_ = &ObSumAggCell::copy_int_to_decimal_int<int128_t>;
            } else {
              eval_func_ = &ObSumAggCell::eval_int<int256_t>;
              eval_batch_func_ = &ObSumAggCell::eval_int_batch<int256_t>;
              copy_datum_func_ = &ObSumAggCell::copy_int_to_decimal_int<int256_t>;
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected type", K(ret), K(obj_tc_));
            break;
          }
        }
        break;
      }
      case ObObjTypeClass::ObUIntTC: {
        switch (res_tc) {
          case ObObjTypeClass::ObNumberTC: {
            eval_func_ = &ObSumAggCell::eval_uint<number::ObNumber>;
            eval_batch_func_ = &ObSumAggCell::eval_uint_batch<number::ObNumber>;
            copy_datum_func_ = &ObSumAggCell::copy_uint_to_number;
            eval_skip_index_func_ = &ObSumAggCell::eval_number;
            break;
          }
          case ObObjTypeClass::ObDecimalIntTC: {
            if (OB_UNLIKELY(precision < OB_DECIMAL_LONGLONG_DIGITS ||
                              precision > MAX_PRECISION_DECIMAL_INT_256)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected precision", K(ret), K(precision));
            } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
              eval_func_ = &ObSumAggCell::eval_uint<int128_t>;
              eval_batch_func_ = &ObSumAggCell::eval_uint_batch<int128_t>;
              copy_datum_func_ = &ObSumAggCell::copy_uint_to_decimal_int<int128_t>;
            } else {
              eval_func_ = &ObSumAggCell::eval_uint<int256_t>;
              eval_batch_func_ = &ObSumAggCell::eval_uint_batch<int256_t>;
              copy_datum_func_ = &ObSumAggCell::copy_uint_to_decimal_int<int256_t>;
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected type", K(ret), K(obj_tc_));
            break;
          }
        }
        break;
      }
      case ObObjTypeClass::ObFloatTC: {
        eval_func_ = &ObSumAggCell::eval_float;
        eval_batch_func_ = &ObSumAggCell::eval_float_batch;
        copy_datum_func_ = &ObSumAggCell::copy_float;
        eval_skip_index_func_ = &ObSumAggCell::eval_float;
        break;
      }
      case ObObjTypeClass::ObDoubleTC: {
        eval_func_ = &ObSumAggCell::eval_double;
        eval_batch_func_ = &ObSumAggCell::eval_double_batch;
        copy_datum_func_ = &ObSumAggCell::copy_double;
        eval_skip_index_func_ = &ObSumAggCell::eval_double;
        break;
      }
      case ObObjTypeClass::ObNumberTC: {
        eval_func_ = &ObSumAggCell::eval_number;
        eval_batch_func_ = &ObSumAggCell::eval_number_batch;
        copy_datum_func_ = &ObSumAggCell::copy_number;
        eval_skip_index_func_ = &ObSumAggCell::eval_number;
        break;
      }
      case ObObjTypeClass::ObDecimalIntTC: {
        ret = ObSumAggCell::init_decimal_int_func();
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret), K(obj_tc_));
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (eval_skip_index_func_ == nullptr && OB_FAIL(init_eval_skip_index_func_for_decimal())) {
    LOG_WARN("fail to init eval with skip index for decimal", K(ret));
  } else if (is_group_by) {
    const int64_t datum_size = is_sum_use_temp_buf_ ? common::OBJ_DATUM_DECIMALINT_MAX_RES_SIZE : common::OBJ_DATUM_NUMBER_RES_SIZE;
    group_by_result_datum_buf_->set_item_size(datum_size);
    if (ObObjTypeClass::ObIntTC == obj_tc_ || ObObjTypeClass::ObUIntTC == obj_tc_) {
      if (OB_FAIL(new_group_by_buf((bool*)nullptr, 0, sizeof(bool), allocator_, sum_use_int_flag_buf_))) {
        LOG_WARN("Failed to new buf", K(ret));
      } else if (OB_FAIL(sum_use_int_flag_buf_->reserve(basic_info_.batch_size_))) {
        LOG_WARN("Failed to reserve buf", K(ret));
      } else if (OB_FAIL(new_group_by_buf((int64_t*)(nullptr), 0, sizeof(int64_t), allocator_, num_int_buf_))) {
        LOG_WARN("Failed to new buf", K(ret));
      } else if (OB_FAIL(num_int_buf_->reserve(basic_info_.batch_size_))) {
        LOG_WARN("Failed to new buf", K(ret));
      }
    }
  }
  return ret;
}

int ObSumAggCell::init_decimal_int_func()
{
  int ret = OB_SUCCESS;
// The sum aggregation function will increase the precision by 22. Based on this rule,
// we can get the mapping of each input to output:
//   a. int32_t [0 - 9] => result precision range [22 - 31], which always be int128_t.
//   b. int64_t [10 - 18] => result P range [32, 40], which can be int128_t or int256_t.
//   c. int128_t [19 - 38] => result P range [41, 60], which always be int256.
//   d. int256_t [39 - 76] => result P range [61, 98], which can be int256_t or int512_t.
  const static ObSumEvalAggFuncType AGG_FUNCS[][3] = {
    // int32_t
    {
      &ObSumAggCell::eval_decimal_int<int128_t, int32_t>,
      nullptr,
      &ObSumAggCell::eval_decimal_int_number<int32_t>
    },
    // int64_t
    {
      &ObSumAggCell::eval_decimal_int<int128_t, int64_t>,
      &ObSumAggCell::eval_decimal_int<int256_t, int64_t>,
      &ObSumAggCell::eval_decimal_int_number<int64_t>
    },
    // int128_t
    {
      &ObSumAggCell::eval_decimal_int<int256_t, int128_t>,
      nullptr,
      &ObSumAggCell::eval_decimal_int_number<int128_t>
    },
    // int256_t
    {
      &ObSumAggCell::eval_decimal_int<int256_t, int256_t>,
      &ObSumAggCell::eval_decimal_int<int512_t, int256_t>,
      &ObSumAggCell::eval_decimal_int_number<int256_t>
    }
  };

  const static ObSumCopyDatumFuncType COPY_FUNCS[][3] = {
    // int32_t
    {
      &ObSumAggCell::copy_decimal_int<int128_t, int32_t>,
      nullptr,
      &ObSumAggCell::copy_decimal_int_to_number<int32_t>
    },
    // int64_t
    {
      &ObSumAggCell::copy_decimal_int<int128_t, int64_t>,
      &ObSumAggCell::copy_decimal_int<int256_t, int64_t>,
      &ObSumAggCell::copy_decimal_int_to_number<int64_t>
    },
    // int128_t
    {
      &ObSumAggCell::copy_decimal_int<int256_t, int128_t>,
      nullptr,
      &ObSumAggCell::copy_decimal_int_to_number<int128_t>
    },
    // int256_t
    {
      &ObSumAggCell::copy_decimal_int<int256_t, int256_t>,
      &ObSumAggCell::copy_decimal_int<int512_t, int256_t>,
      &ObSumAggCell::copy_decimal_int_to_number<int256_t>
    }
  };

  const static ObSumEvalBatchAggFuncType AGG_BATCH_FUNCS[][3][2] = {
    // int32_t
    {
      { &ObSumAggCell::eval_decimal_int_batch<int128_t, int32_t, int32_t>,
        &ObSumAggCell::eval_decimal_int_batch<int128_t, int64_t, int32_t> },
      { nullptr, nullptr,},
      { &ObSumAggCell::eval_decimal_int_number_batch<int32_t, int32_t>,
        &ObSumAggCell::eval_decimal_int_number_batch<int64_t, int32_t> }
    },
    // int64_t
    {
      { &ObSumAggCell::eval_decimal_int_batch<int128_t, int64_t, int64_t>,
        &ObSumAggCell::eval_decimal_int_batch<int128_t, int128_t, int64_t> },
      { &ObSumAggCell::eval_decimal_int_batch<int256_t, int64_t, int64_t>,
        &ObSumAggCell::eval_decimal_int_batch<int256_t, int128_t, int64_t> },
      { &ObSumAggCell::eval_decimal_int_number_batch<int64_t, int64_t>,
        &ObSumAggCell::eval_decimal_int_number_batch<int128_t, int64_t> }
    },
    // int128_t
    {
      { &ObSumAggCell::eval_decimal_int_batch<int256_t, int128_t, int128_t>,
        &ObSumAggCell::eval_decimal_int_batch<int256_t, int256_t, int128_t> },
      { nullptr, nullptr,},
      { &ObSumAggCell::eval_decimal_int_number_batch<int128_t, int128_t>,
        &ObSumAggCell::eval_decimal_int_number_batch<int256_t, int128_t> }
    },
    // int256_t
    {
      { &ObSumAggCell::eval_decimal_int_batch<int256_t, int256_t, int256_t>, nullptr },
      { &ObSumAggCell::eval_decimal_int_batch<int512_t, int256_t, int256_t>, nullptr },
      { &ObSumAggCell::eval_decimal_int_number_batch<int256_t, int256_t>, nullptr }
    }
  };

  const ObObjTypeClass res_tc = ob_obj_type_class(basic_info_.agg_expr_->datum_meta_.type_);
  const int16_t arg_prec = basic_info_.agg_expr_->args_[0]->datum_meta_.precision_;
  const int16_t res_prec = basic_info_.agg_expr_->datum_meta_.precision_;
  const int16_t buffer_prec = get_max_decimalint_precision(arg_prec) - arg_prec;
  const int arg_type = static_cast<int>(get_decimalint_type(arg_prec));
  const bool need_cast = (buffer_prec < MAX_PRECISION_DECIMAL_INT_64) &&
                           get_scale_factor<int64_t>(buffer_prec) < basic_info_.batch_size_;
  if (OB_UNLIKELY(arg_type > 3 || arg_type < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected arg type", K(ret), K(arg_type));
  } else if (ObObjTypeClass::ObNumberTC == res_tc) {
    eval_func_ = AGG_FUNCS[arg_type][2];
    eval_batch_func_ = AGG_BATCH_FUNCS[arg_type][2][need_cast];
    copy_datum_func_ = COPY_FUNCS[arg_type][2];
    eval_skip_index_func_ = &ObSumAggCell::eval_number;
  } else {
    const int res_type = static_cast<int>(get_decimalint_type(res_prec));
    int res_func_idx = 0;
    if (OB_UNLIKELY(res_prec < arg_prec || res_prec - arg_prec > OB_DECIMAL_LONGLONG_DIGITS)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected precision", K(ret), K(arg_prec), K(res_prec));
    } else {
      switch (arg_type) {
        case DECIMAL_INT_32:
        case DECIMAL_INT_128:
          res_func_idx = 0;
          break;
        case DECIMAL_INT_64:
          res_func_idx = res_type == DECIMAL_INT_128 ? 0 : 1;
          break;
        case DECIMAL_INT_256:
          res_func_idx = res_type == DECIMAL_INT_256 ? 0 : 1;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCC(ret)) {
        eval_func_ = AGG_FUNCS[arg_type][res_func_idx];
        eval_batch_func_ = AGG_BATCH_FUNCS[arg_type][res_func_idx][need_cast];
        copy_datum_func_ = COPY_FUNCS[arg_type][res_func_idx];
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(eval_func_) || OB_ISNULL(eval_batch_func_) || OB_ISNULL(copy_datum_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("function does not init", K(ret), K(*this));
    } else if (res_prec > MAX_PRECISION_DECIMAL_INT_256) {
      // Special scenarios: ObStorageDatum can only store 40 bytes, and int512 has 64 bytes,
      // so need to allocate temporary memory to store result
      is_sum_use_temp_buf_ = true;
      if (OB_ISNULL(sum_temp_buffer_ = static_cast<char*>(allocator_.alloc(sizeof(int512_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else if (OB_ISNULL(cast_temp_buffer_ = static_cast<char*>(allocator_.alloc(sizeof(int512_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        result_datum_.ptr_ = sum_temp_buffer_;
        cast_datum_.ptr_ = cast_temp_buffer_;
      }

      if (OB_FAIL(ret)) {
        if (nullptr != sum_temp_buffer_) {
          allocator_.free(sum_temp_buffer_);
          sum_temp_buffer_ = nullptr;
        }
        if (nullptr != cast_temp_buffer_) {
          allocator_.free(cast_temp_buffer_);
          cast_temp_buffer_ = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObSumAggCell::eval(blocksstable::ObStorageDatum &datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (OB_FAIL(fill_default_if_need(datum))) {
    LOG_WARN("Failed to fill default", K(ret), K(datum), KPC(this));
  } else if (datum.is_null()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL((this->*eval_func_)(datum, DEFAULT_DATUM_OFFSET))) {
        LOG_WARN("Fail to eval", K(ret), K(obj_tc_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    aggregated_ = true;
  }
  LOG_DEBUG("after process rows", KPC(this));
  return ret;
}

int ObSumAggCell::eval_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(ret), K(count));
  } else if (OB_FAIL((this->*eval_batch_func_)(datums, count))) {
    LOG_WARN("Failed to eval batch", K(ret));
  } else {
    aggregated_ = true;
  }
  return ret;
}

int ObSumAggCell::eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (!is_cg && (!index_info.can_blockscan(is_lob_col()) || index_info.is_left_border() || index_info.is_right_border())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, the micro index info must can blockscan and not border", K(ret), K(index_info));
  } else if (OB_UNLIKELY(skip_index_datum_.is_null())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected skip index datum is null", K(ret), K(index_info));
  } else if (OB_FAIL((this->*eval_skip_index_func_)(skip_index_datum_, DEFAULT_DATUM_OFFSET))) {
    LOG_WARN("Fail to eval", K(ret), K(obj_tc_));
  } else {
    aggregated_ = true;
    LOG_DEBUG("[SKIP INDEX] sum agg eval_index_info", K(ret), K(skip_index_datum_),
        KPC(this), K(index_info), K(is_cg));
  }
  return ret;
}

int ObSumAggCell::eval_batch_in_group_by(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count <= 0 || nullptr == refs || distinct_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(datums), K(count), KP(refs), K(distinct_cnt));
  } else {
    const bool read_distinct_val = is_group_by_col || is_default_datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const uint32_t dictinct_datum_offset = is_default_datum ? 0 : refs[i];
      const common::ObDatum &datum = read_distinct_val ? datums[dictinct_datum_offset] : datums[i];
      if (datum.is_null()) {
      } else if (OB_FAIL((this->*eval_func_)(datum, refs[i]))) {
        LOG_WARN("Fail to eval", K(ret), K(obj_tc_));
      }
    }
  }
  return ret;
}

bool ObSumAggCell::can_use_index_info() const
{
  bool res = false;
  res = nullptr != basic_info_.col_param_ && basic_info_.col_param_->get_meta_type().is_numeric_type();
  return res;
}

int ObSumAggCell::copy_output_row(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset >= group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    const common::ObDatum &datum = col_datums_[datum_offset];
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    if (datum.is_null()) {
      result_datums[datum_offset].set_null();
    } else if (OB_FAIL((this->*copy_datum_func_)(datum, result_datums[datum_offset]))) {
      LOG_WARN("Failed to copy output datum", K(ret));
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(datum_offset), K(datum), K(result_datums[datum_offset]));
  }
  return ret;
}

int ObSumAggCell::copy_output_rows(const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset > group_by_result_datum_buf_->get_basic_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(datum_offset), KPC(group_by_result_datum_buf_));
  } else {
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_offset; ++i) {
      const common::ObDatum &datum = col_datums_[i];
      if (datum.is_null()) {
        result_datums[i].set_null();
      } else if (OB_FAIL((this->*copy_datum_func_)(datum, result_datums[i]))) {
        LOG_WARN("Failed to copy output datum", K(ret));
      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(datum_offset),
        K(common::ObArrayWrap<common::ObDatum>(col_datums_, datum_offset)),
        K(common::ObArrayWrap<common::ObDatum>(result_datums, datum_offset)));
  }
  return ret;
}

int ObSumAggCell::copy_single_output_row(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_info_.agg_expr_->args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null args of agg expr", K(ret), KPC(basic_info_.agg_expr_));
  } else {
    sql::ObDatum &datum = basic_info_.agg_expr_->args_[0]->locate_expr_datum(ctx);
    common::ObDatum &result_datum = basic_info_.agg_expr_->locate_datum_for_write(ctx);
    if (datum.is_null()) {
      result_datum.set_null();
    } else if (OB_FAIL((this->*copy_datum_func_)(datum, result_datum))) {
      LOG_WARN("Failed to copy output datum", K(ret));
    }
  }
  return ret;
}

int ObSumAggCell::copy_int_to_number(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber nmb;
  if (OB_FAIL(nmb.from(datum.get_int(), tmp_alloc))) {
    LOG_WARN("create number from int failed", K(ret));
  } else {
    result_datum.set_number(nmb);
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::copy_int_to_decimal_int(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    result_datum.set_null();
  } else {
    DATUM_TO_DECIMAL_INT(result_datum, RES_T) = datum.get_int();
    result_datum.pack_ = sizeof(RES_T);
  }
  return ret;
}

int ObSumAggCell::copy_uint_to_number(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  sql::ObNumStackAllocator<1> tmp_alloc;
  common::number::ObNumber nmb;
  if (OB_FAIL(nmb.from(datum.get_uint(), tmp_alloc))) {
    LOG_WARN("create number from int failed", K(ret));
  } else {
    result_datum.set_number(nmb);
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::copy_uint_to_decimal_int(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  DATUM_TO_DECIMAL_INT(result_datum, RES_T) = datum.get_uint();
  result_datum.pack_ = sizeof(RES_T);
  return ret;
}

int ObSumAggCell::copy_float(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  result_datum.set_float(datum.get_float());
  return ret;
}

int ObSumAggCell::copy_double(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  result_datum.set_double(datum.get_double());
  return ret;
}

int ObSumAggCell::copy_number(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  result_datum.set_number(datum.get_number());
  return ret;
}

template<typename RES_T, typename ARG_T>
int ObSumAggCell::copy_decimal_int(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  DATUM_TO_DECIMAL_INT(result_datum, RES_T) = DATUM_TO_CONST_DECIMAL_INT(datum, ARG_T);
  result_datum.pack_ = sizeof(RES_T);
  return ret;
}

template<typename ARG_T>
int ObSumAggCell::copy_decimal_int_to_number(const ObDatum &datum, ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  sql::ObNumStackAllocator<2> tmp_alloc;
  number::ObNumber right_nmb;
  if (OB_FAIL(wide::to_number(DATUM_TO_CONST_DECIMAL_INT(datum, ARG_T),
                              child_scale(), tmp_alloc, right_nmb))) {
    LOG_WARN("fail to cast decimal int to number", K(ret));
  } else {
    result_datum.set_number(right_nmb);
  }
  return ret;
}

int ObSumAggCell::collect_batch_result_in_group_by(const int64_t distinct_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(distinct_cnt > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(distinct_cnt), KPC(group_by_result_datum_buf_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < distinct_cnt; ++i) {
      if (OB_FAIL(collect_result_in_group_by(i))) {
        LOG_WARN("Failed to collect result in group by", K(ret));
      }
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(distinct_cnt));
  return ret;
}

int ObSumAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObDatum &result = basic_info_.agg_expr_->locate_datum_for_write(ctx);
  sql::ObEvalInfo &eval_info = basic_info_.agg_expr_->get_eval_info(ctx);
  if (!sum_use_int_flag_) {
    if (OB_FAIL(ObAggCell::collect_result(ctx))) {
      LOG_WARN("Failed to collect_result", K(ret), KPC(this));
    }
  } else if (ObIntTC != obj_tc_ && ObUIntTC != obj_tc_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected type class", K(ret), K(obj_tc_));
  } else if (ob_is_decimal_int(basic_info_.agg_expr_->datum_meta_.type_)) {
    int128_t right_nmb = 0;
    if (ObIntTC == obj_tc_) {
      right_nmb = num_int_;
    } else {
      right_nmb = num_uint_;
    }
    if (OB_FAIL(collect_result_to_decimal_int(right_nmb, result_datum_, result))) {
      LOG_WARN("Failed to collect result to decimal int", K(ret));
    } else {
      eval_info.evaluated_ = true;
    }
  } else {
    sql::ObNumStackAllocator<2> tmp_alloc;
    common::number::ObNumber right_nmb;
    const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
    if (ObIntTC == obj_tc_) {
      if (OB_FAIL(right_nmb.from(num_int_, tmp_alloc))) {
        LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(obj_tc_));
      }
    } else if (OB_FAIL(right_nmb.from(num_uint_, tmp_alloc))) {
      LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(obj_tc_));
    }
    if (OB_SUCC(ret)) {
      if (result_datum_.is_null()) {
        result.set_number(right_nmb);
      } else {
        common::number::ObNumber left_nmb(result_datum_.get_number());
        common::number::ObNumber result_nmb;
        if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, strict_mode))) {
          LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          result.set_number(result_nmb);
        }
      }
      eval_info.evaluated_ = true;
    }
  }
  LOG_DEBUG("collect_result", K(result), KPC(this));
  return ret;
}

template<typename RES_T>
int ObSumAggCell::eval_int(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  char buf_alloc[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  if (OB_FAIL(eval_int_inner<RES_T>(datum, allocator, datum_offset))) {
    LOG_WARN("Failed to eval int", K(ret));
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::eval_uint(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  char buf_alloc[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  if (OB_FAIL(eval_uint_inner<RES_T>(datum, allocator, datum_offset))) {
    LOG_WARN("Failed to eval uint", K(ret));
  }
  return ret;
}

int ObSumAggCell::eval_float(const common::ObDatum &datum, const int32_t datum_offset)
{
  return eval_float_inner(datum, datum_offset);
}

int ObSumAggCell::eval_double(const common::ObDatum &datum, const int32_t datum_offset)
{
  return eval_double_inner(datum, datum_offset);
}

int ObSumAggCell::eval_number(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
  if (datum.is_null()) {
  } else if (result_datum.is_null()) {
    result_datum.set_number(datum.get_number());
  } else {
    common::number::ObNumber left_nmb(result_datum.get_number());
    common::number::ObNumber right_nmb(datum.get_number());
    char buf_alloc[common::number::ObNumber::MAX_CALC_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_CALC_BYTE_LEN);
    const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
    common::number::ObNumber result_nmb;
    if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
      LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
    } else {
      result_datum.set_number(result_nmb);
    }
  }
  return ret;
}

int ObSumAggCell::init_eval_skip_index_func_for_decimal()
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass res_tc = ob_obj_type_class(basic_info_.agg_expr_->datum_meta_.type_);
  const int16_t res_prec = basic_info_.agg_expr_->datum_meta_.precision_;
  const int res_type = static_cast<int>(get_decimalint_type(res_prec));
  switch (res_type) {
    case DECIMAL_INT_128:
      eval_skip_index_func_ = &ObSumAggCell::eval_number_decimal_int<int128_t>;
      break;
    case DECIMAL_INT_256:
      eval_skip_index_func_ = &ObSumAggCell::eval_number_decimal_int<int256_t>;
      break;
    case DECIMAL_INT_512:
      eval_skip_index_func_ = &ObSumAggCell::eval_number_decimal_int<int512_t>;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect result decimal type", K(ret),
          K(res_type), K(basic_info_.agg_expr_->datum_meta_));
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::eval_number_decimal_int(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  //cast number to decimal
  int16_t out_scale = basic_info_.agg_expr_->datum_meta_.scale_;
  ObDecimalIntBuilder tmp_alloc;
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes = 0;
  const number::ObNumber nmb(datum.get_number());
  const ObScale in_scale = nmb.get_scale();
  const ObPrecision out_prec = basic_info_.agg_expr_->datum_meta_.precision_;
  int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
  if (OB_FAIL(wide::from_number(nmb, tmp_alloc, in_scale, decint, int_bytes))) {
    LOG_WARN("from_number failed", K(ret), K(out_scale));
  } else if (sql::ObDatumCast::need_scale_decimalint(in_scale, int_bytes, out_scale, out_bytes)) {
    // upcasting
    ObDecimalIntBuilder res_val;
    if (OB_FAIL(sql::ObDatumCast::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, out_prec,
                                        basic_info_.agg_expr_->extra_, res_val))) {
      LOG_WARN("scale decimal int failed", K(ret), K(in_scale), K(out_scale));
    } else {
      cast_datum_.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  } else {
    cast_datum_.set_decimal_int(decint, int_bytes);
  }
  if (FAILEDx((eval_decimal_int<RES_T, RES_T>(cast_datum_, datum_offset)))) {
    LOG_WARN(" fail to eval decimal int", K(ret));
  }
  return ret;
}

template<typename RES_T, typename ARG_T>
int ObSumAggCell::eval_decimal_int(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
  if (datum.is_null()) {
  } else if (result_datum.is_null()) {
    DATUM_TO_DECIMAL_INT(result_datum, RES_T) = DATUM_TO_CONST_DECIMAL_INT(datum, ARG_T);
    result_datum.pack_ = sizeof(RES_T);
  } else {
    DATUM_TO_DECIMAL_INT(result_datum, RES_T) += DATUM_TO_CONST_DECIMAL_INT(datum, ARG_T);
  }
  return ret;
}

template<typename ARG_T>
int ObSumAggCell::eval_decimal_int_number(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
  if (!datum.is_null()) {
    sql::ObNumStackAllocator<2> tmp_alloc;
    number::ObNumber right_nmb;
    if (OB_FAIL(wide::to_number(DATUM_TO_CONST_DECIMAL_INT(datum, ARG_T),
                                child_scale(), tmp_alloc, right_nmb))) {
      LOG_WARN("fail to cast decimal int to number", K(ret));
    } else if (result_datum.is_null()) {
      result_datum.set_number(right_nmb);
    } else {
      number::ObNumber left_nmb(result_datum.get_number());
      number::ObNumber result_nmb;
      if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, false))) {
        LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
      } else {
        result_datum.set_number(result_nmb);
      }
    }
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::eval_int_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  char buf_alloc[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(eval_int_inner<RES_T>(datums[i], allocator))) {
      LOG_WARN("Failed to eval int", K(ret));
    }
  }
  return ret;
}

template<typename RES_T>
int ObSumAggCell::eval_uint_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  char buf_alloc[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator(buf_alloc, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(eval_uint_inner<RES_T>(datums[i], allocator))) {
      LOG_WARN("Failed to eval uint", K(ret));
    }
  }
  return ret;
}

int ObSumAggCell::eval_float_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(eval_float_inner(datums[i]))) {
      LOG_WARN("Failed to eval float", K(ret));
    }
  }
  return ret;
}

int ObSumAggCell::eval_double_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(eval_double_inner(datums[i]))) {
      LOG_WARN("Failed to eval float", K(ret));
    }
  }
  return ret;
}

int ObSumAggCell::eval_number_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  common::number::ObNumber result_nmb;
  char buf_alloc1[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  char buf_alloc2[common::number::ObNumber::MAX_CALC_BYTE_LEN];
  uint32_t sum_digits_buf[common::number::ObNumber::OB_CALC_BUFFER_SIZE];
  MEMSET(sum_digits_buf, 0, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  ObDataBuffer allocator1(buf_alloc1, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  ObDataBuffer allocator2(buf_alloc2, common::number::ObNumber::MAX_CALC_BYTE_LEN);
  bool all_skip = true;
  if (!result_datum_.is_null()) {
      result_nmb.assign(result_datum_.get_number_desc().desc_,
                        const_cast<uint32_t *>(result_datum_.get_number_digits()));
  }
  ObAggSource src(datums);
  ObAggSelector selector(count);
  if (OB_FAIL(sql::number_accumulator(src, allocator1, allocator2, result_nmb,
                                      sum_digits_buf, all_skip, selector))) {
    LOG_WARN("number add failed", K(ret), K(result_nmb));
  } else if (!all_skip) {
    result_datum_.set_number(result_nmb);
  }
  LOG_DEBUG("number result", K(result_nmb));
  return ret;
}

template<typename RES_T, typename CALC_T, typename ARG_T>
int ObSumAggCell::eval_decimal_int_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  CALC_T accum = 0;
  bool accumulated = false;
  for (int64_t i = 0; i < count; ++i) {
    if (datums[i].is_null()) {
      continue;
    }
    accum += DATUM_TO_CONST_DECIMAL_INT(datums[i], ARG_T);
    accumulated = true;
  }
  if (accumulated) {
    if (result_datum_.is_null()) {
      DATUM_TO_DECIMAL_INT(result_datum_, RES_T) = accum;
      result_datum_.pack_ = sizeof(RES_T);
    } else {
      DATUM_TO_DECIMAL_INT(result_datum_, RES_T) += accum;
    }
  }
  return ret;
}

template<typename CALC_T, typename ARG_T>
int ObSumAggCell::eval_decimal_int_number_batch(const common::ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  CALC_T accum = 0;
  bool accumulated = false;
  for (int64_t i = 0; i < count; ++i) {
    if (datums[i].is_null()) {
      continue;
    }
    accum += DATUM_TO_CONST_DECIMAL_INT(datums[i], ARG_T);
    accumulated = true;
  }
  if (accumulated) {
    sql::ObNumStackAllocator<2> tmp_alloc;
    number::ObNumber right_nmb;
    if (OB_FAIL(wide::to_number(accum, child_scale(), tmp_alloc, right_nmb))) {
      LOG_WARN("fail to cast decimal int to number", K(ret));
    } else if (result_datum_.is_null()) {
      result_datum_.set_number(right_nmb);
    } else {
      number::ObNumber left_nmb(result_datum_.get_number());
      number::ObNumber result_nmb;
      if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, false))) {
        LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
      } else {
        result_datum_.set_number(result_nmb);
      }
    }
  }
  return ret;
}

int ObSumAggCell::collect_result_in_group_by(const int64_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_offset > group_by_result_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected datum offset", K(ret), K(datum_offset), K(group_by_result_cnt_));
  } else if (ObObjTypeClass::ObIntTC != obj_tc_ && ObObjTypeClass::ObUIntTC != obj_tc_) {
  } else if (ob_is_decimal_int(basic_info_.agg_expr_->datum_meta_.type_)) {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    if (sum_use_int_flag_buf_->at(datum_offset)) {
      int128_t right_nmb = 0;
      if (ObIntTC == obj_tc_) {
        right_nmb = num_int_buf_->at(datum_offset);
      } else {
        right_nmb = num_uint_buf_->at(datum_offset);
      }
      if (OB_FAIL(collect_result_to_decimal_int(right_nmb, result_datum, result_datum))) {
        LOG_WARN("Failed to collect result to decimal int", K(ret));
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(result_datum));
    }
  } else {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    if (sum_use_int_flag_buf_->at(datum_offset)) {
      sql::ObNumStackAllocator<2> tmp_alloc;
      common::number::ObNumber right_nmb;
      const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
      if (ObIntTC == obj_tc_) {
        int64_t *num = nullptr;
        if (OB_FAIL(right_nmb.from(num_int_buf_->at(datum_offset), tmp_alloc))) {
          LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(obj_tc_));
        }
      } else {
        uint64_t *num = nullptr;
        if (OB_FAIL(right_nmb.from(num_uint_buf_->at(datum_offset), tmp_alloc))) {
          LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(obj_tc_));
        }
      }
      if (OB_SUCC(ret)) {
        if (result_datum.is_null()) {
          result_datum.set_number(right_nmb);
        } else {
          common::number::ObNumber left_nmb(result_datum.get_number());
          common::number::ObNumber result_nmb;
          if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, strict_mode))) {
            LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
          } else {
            result_datum.set_number(result_nmb);
          }
        }
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(right_nmb), K(result_datum));
    }
  }
  return ret;
}

int ObSumAggCell::reserve_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_by_result_datum_buf_->reserve(size))) {
    LOG_WARN("Failed to prepare extra buf", K(ret));
  } else if (FALSE_IT(ObAggCell::clear_group_by_info())) {
  } else if (nullptr != sum_use_int_flag_buf_) {
    if (OB_FAIL(sum_use_int_flag_buf_->reserve(size))) {
      LOG_WARN("Failed to reserve flags", K(ret));
    } else if (OB_FAIL(num_int_buf_->reserve(size))) {
      LOG_WARN("Failed to prepare buf", K(ret));
    } else {
      clear_group_by_info();
    }
  }
  return ret;
}

int ObSumAggCell::output_extra_group_by_result(const int64_t start, const int64_t count)
{
  int ret = OB_SUCCESS;
  common::ObDatum *sql_result_datums = group_by_result_datum_buf_->get_basic_buf();
  if (OB_UNLIKELY(start < group_by_result_datum_buf_->get_basic_count() ||
      start + count > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected argument, should be not exceed the buf size",
      K(ret), K(start), K(count), KPC(group_by_result_datum_buf_));
  } else if (ob_is_decimal_int(basic_info_.agg_expr_->datum_meta_.type_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      common::ObDatum &result_datum = group_by_result_datum_buf_->at(start + i);
      if (result_datum.is_null()) {
        sql_result_datums[i].set_null();
      } else if (OB_FAIL((this->*copy_datum_func_)(result_datum, sql_result_datums[i]))) {
        LOG_WARN("Failed to copy decimal int datum", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      common::ObDatum &result_datum = group_by_result_datum_buf_->at(start + i);
      if (OB_FAIL(sql_result_datums[i].from_storage_datum(
          result_datum, basic_info_.agg_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to output extra buf", K(ret));
      }
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(start), K(count), K(ObArrayWrap<common::ObDatum>(sql_result_datums, count)));
  return ret;
}

int ObSumAggCell::collect_result_to_decimal_int(
    const int128_t &right_nmb,
    const common::ObDatum &datum,
    common::ObDatum &result)
{
  int ret = OB_SUCCESS;
  if (basic_info_.agg_expr_->datum_meta_.precision_ <= MAX_PRECISION_DECIMAL_INT_128) {
    if (datum.is_null()) {
      DATUM_TO_DECIMAL_INT(result, int128_t) = right_nmb;
    } else {
      DATUM_TO_DECIMAL_INT(result, int128_t) =
        DATUM_TO_CONST_DECIMAL_INT(datum, int128_t) + right_nmb;
    }
    result.pack_ = sizeof(int128_t);
  } else { // int256;
    if (datum.is_null()) {
      DATUM_TO_DECIMAL_INT(result, int256_t) = right_nmb;
    } else {
      DATUM_TO_DECIMAL_INT(result, int256_t) =
        DATUM_TO_CONST_DECIMAL_INT(datum, int256_t) + right_nmb;
    }
    result.pack_ = sizeof(int256_t);
  }
  return ret;
}

ObFirstRowAggCell::ObFirstRowAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator)
    : ObAggCell(basic_info, allocator),
      is_determined_value_(false),
      aggregated_flag_cnt_(0),
      aggregated_flag_buf_(),
      datum_allocator_("ObStorageAgg", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  agg_type_ = ObPDAggType::PD_FIRST_ROW;
}

void ObFirstRowAggCell::reset()
{
  is_determined_value_ = false;
  aggregated_flag_cnt_ = 0;
  free_group_by_buf(allocator_, aggregated_flag_buf_);
  if (nullptr != agg_datum_buf_) {
    agg_datum_buf_->reset();
    allocator_.free(agg_datum_buf_);
    agg_datum_buf_ = nullptr;
  }
  datum_allocator_.reset();
  ObAggCell::reset();
}

void ObFirstRowAggCell::reuse()
{
  ObAggCell::reuse();
  aggregated_flag_cnt_ = 0;
  datum_allocator_.reuse();
  if (is_determined_value_) {
    set_determined_value();
  }
}

void ObFirstRowAggCell::clear_group_by_info()
{
  if (nullptr != aggregated_flag_buf_) {
    aggregated_flag_buf_->fill_items(false);
  }
}

int ObFirstRowAggCell::init(const bool is_group_by, sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAggCell::init(is_group_by, eval_ctx))) {
    LOG_WARN("Failed to init agg cell", K(ret));
  } else if (is_group_by) {
    if (OB_FAIL(new_group_by_buf((bool*)nullptr, 0, sizeof(bool), allocator_, aggregated_flag_buf_))) {
      LOG_WARN("Failed to new buf", K(ret));
    } else if (OB_FAIL(aggregated_flag_buf_->reserve(basic_info_.batch_size_))) {
      LOG_WARN("Failed to reserve buf", K(ret));
    } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(basic_info_.batch_size_, false, allocator_, agg_datum_buf_))) {
      LOG_WARN("Failed to alloc agg datum buf", K(ret));
    } else if (OB_FAIL(new_group_by_buf(agg_datum_buf_->get_datums(), basic_info_.batch_size_,
        common::OBJ_DATUM_NUMBER_RES_SIZE, allocator_, group_by_result_datum_buf_))) {
      LOG_WARN("Failed to new buf", K(ret));
    }
  }
  return ret;
}

int ObFirstRowAggCell::eval(blocksstable::ObStorageDatum &datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_count));
  } else if (!aggregated_) {
    if (OB_FAIL(fill_default_if_need(datum))) {
      LOG_WARN("Failed to fill default", K(ret), KPC(this));
    } else if (OB_FAIL(pad_column_if_need(datum, padding_allocator_))) {
      LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(datum));
    } else if (OB_FAIL(result_datum_.deep_copy(datum, datum_allocator_))) {
      LOG_WARN("Failed to deep copy datum", K(ret), K(datum));
    } else {
      aggregated_ = true;
    }
  }
  return ret;
}

int ObFirstRowAggCell::eval_micro_block(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    if (OB_UNLIKELY(nullptr == row_ids || 0 == row_count || nullptr == basic_info_.col_param_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), KP(row_ids), K(row_count), K(basic_info_.col_param_));
    } else {
      blocksstable::ObStorageDatum datum;
      if (OB_FAIL(reader->get_column_datum(iter_param, context, *basic_info_.col_param_, col_offset, row_ids[0], datum))) {
        LOG_WARN("Failed to get first datum", K(ret), K(col_offset), K(row_ids[0]), K(row_count));
      } else if (OB_FAIL(fill_default_if_need(datum))) {
        LOG_WARN("Failed to fill default", K(ret), KPC(this));
      } else if (OB_FAIL(pad_column_if_need(datum, padding_allocator_))) {
        LOG_WARN("Failed to pad column", K(ret), K_(basic_info), K(datum));
      } else if (OB_FAIL(result_datum_.deep_copy(datum, datum_allocator_))) {
        LOG_WARN("Failed to deep copy datum", K(ret), K(datum));
      } else {
        aggregated_ = true;
      }
    }
  }
  return ret;
}

int ObFirstRowAggCell::eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  UNUSEDx(index_info, is_cg);
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, must be aggregated in single/batch rows", K(ret));
  }
  return ret;
}

int ObFirstRowAggCell::can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg, bool &can_agg)
{
  int ret = OB_SUCCESS;
  can_agg = can_use_index_info();
  return ret;
}

int ObFirstRowAggCell::eval_batch_in_group_by(
    const common::ObDatum *datums,
    const int64_t count,
    const uint32_t *refs,
    const int64_t distinct_cnt,
    const bool is_group_by_col,
    const bool is_default_datum)
{
  int ret = OB_SUCCESS;
  const bool read_distinct_val = is_group_by_col || is_default_datum;
  const bool need_deep_copy = !read_distinct_val && OBJ_DATUM_STRING == basic_info_.agg_expr_->obj_datum_map_;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const uint32_t distinct_ref = refs[i];
    const uint32_t dictinct_datum_offset = is_default_datum ? 0 : refs[i];
    const common::ObDatum &datum = read_distinct_val ? datums[dictinct_datum_offset] : datums[i];
    bool &aggregated_flag = aggregated_flag_buf_->at(distinct_ref);
    ObDatum &result_datum = group_by_result_datum_buf_->at(distinct_ref);
    if (aggregated_flag) {
    } else if (datum.is_null()) {
    } else if (need_deep_copy) {
      if (OB_FAIL(result_datum.deep_copy(datum, datum_allocator_))) {
        LOG_WARN("Failed to clone datum", K(ret), K(datum));
      } else {
        aggregated_flag_cnt_++;
        aggregated_flag = true;
      }
    } else if (OB_FAIL(result_datum.from_storage_datum(datum, basic_info_.agg_expr_->obj_datum_map_))) {
      LOG_WARN("Failed to clone datum", K(ret), K(datum), K(basic_info_.agg_expr_->obj_datum_map_));
    } else {
      aggregated_flag_cnt_++;
      aggregated_flag = true;
    }
    if (OB_SUCC(ret) && aggregated_flag_cnt_ == distinct_cnt) {
      // this micro block is finished, no need do eval any more
      aggregated_ = true;
    }
  }
  return ret;
}

int ObFirstRowAggCell::collect_result(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!aggregated_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected, must be aggregated in single/batch row", K(ret));
  } else if (OB_FAIL(ObAggCell::collect_result(ctx))) {
    LOG_WARN("Failed to collect_result", K(ret), KPC(this));
  }
  return ret;
}

int ObFirstRowAggCell::collect_batch_result_in_group_by(const int64_t distinct_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(distinct_cnt > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(distinct_cnt), KPC(group_by_result_datum_buf_));
  } else {
    const int collected_cnt = MIN(distinct_cnt, basic_info_.batch_size_);
    common::ObDatum *result_datums = group_by_result_datum_buf_->get_basic_buf();
    for (int64_t i = 0; OB_SUCC(ret) && i < collected_cnt; ++i) {
      if (OB_FAIL(col_datums_[i].from_storage_datum(result_datums[i], basic_info_.agg_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to clone datum", K(ret), K(result_datums[i]), K(basic_info_.agg_expr_->obj_datum_map_));
      }
    }
  }
  return ret;
}

int ObFirstRowAggCell::reserve_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAggCell::reserve_group_by_buf(size))) {
    LOG_WARN("Failed to prepare extra group by buf", K(ret));
  } else if (nullptr != aggregated_flag_buf_) {
    if (OB_FAIL(aggregated_flag_buf_->reserve(size))) {
      LOG_WARN("Failed to reserve flag", K(ret));
    } else {
      clear_group_by_info();
    }
  }
  return ret;
}

int ObFirstRowAggCell::output_extra_group_by_result(const int64_t start, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start < group_by_result_datum_buf_->get_basic_count() ||
      start + count > group_by_result_datum_buf_->get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected argument, should be not exceed the buf size",
      K(ret), K(start), K(count), KPC(group_by_result_datum_buf_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      common::ObDatum &result_datum = group_by_result_datum_buf_->at(start + i);
      if (OB_FAIL(col_datums_[i].from_storage_datum(result_datum, basic_info_.agg_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to clone datum", K(ret), K(result_datum), K(basic_info_.agg_expr_->obj_datum_map_));
      }
    }
  }
  return ret;
}

int ObPDAggFactory::alloc_cell(
    const ObAggCellBasicInfo &basic_info,
    common::ObIArray<ObAggCell*> &agg_cells,
    const bool exclude_null,
    const bool is_group_by,
    sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!basic_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(basic_info));
  } else {
    void *buf = nullptr;
    ObAggCell *cell = nullptr;
    switch (basic_info.agg_expr_->type_) {
      case T_REF_COLUMN: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObFirstRowAggCell))) ||
            OB_ISNULL(cell = new(buf) ObFirstRowAggCell(basic_info, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memroy for agg cell", K(ret));
        }
        break;
      }
      case T_FUN_COUNT: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCountAggCell))) ||
            OB_ISNULL(cell = new(buf) ObCountAggCell(basic_info, allocator_, exclude_null))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memroy for agg cell", K(ret));
        }
        break;
      }
      case T_FUN_MIN: {
        const ObDatumCmpFuncType cmp_fun = basic_info.agg_expr_->basic_funcs_->null_first_cmp_;
        if (OB_ISNULL(cmp_fun)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func is NULL", K(ret), KPC(basic_info.agg_expr_));
        } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMinAggCell))) ||
            OB_ISNULL(cell = new(buf) ObMinAggCell(basic_info, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memroy for agg cell", K(ret));
        }
        break;
      }
      case T_FUN_MAX: {
        const ObDatumCmpFuncType cmp_fun = basic_info.agg_expr_->basic_funcs_->null_first_cmp_;
        if (OB_ISNULL(cmp_fun)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func is NULL", K(ret), KPC(basic_info.agg_expr_));
        } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMaxAggCell))) ||
            OB_ISNULL(cell = new(buf) ObMaxAggCell(basic_info, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memroy for agg cell", K(ret));
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObHyperLogLogAggCell))) ||
            OB_ISNULL(cell = new(buf) ObHyperLogLogAggCell(basic_info, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memory for agg cell", K(ret));
        }
        break;
      }
      case T_FUN_SUM_OPNSIZE: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSumOpSizeAggCell))) ||
            OB_ISNULL(cell = new(buf) ObSumOpSizeAggCell(basic_info, allocator_, exclude_null))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memory for agg cell", K(ret));
         }
         break;
       }
      case T_FUN_SUM: {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSumAggCell))) ||
            OB_ISNULL(cell = new(buf) ObSumAggCell(basic_info, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to alloc memroy for agg cell", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Agg is not supported", K(ret), K(basic_info.agg_expr_->type_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cell->init(is_group_by, eval_ctx))) {
      LOG_WARN("Failed to init agg cell", K(ret));
    } else if (OB_FAIL(agg_cells.push_back(cell))) {
      LOG_WARN("Failed to push back agg cell", K(ret));
    }
  }
  return ret;
}

void ObPDAggFactory::release(common::ObIArray<ObAggCell*> &agg_cells)
{
  for (int64_t i = 0; i < agg_cells.count(); ++i) {
    if (OB_NOT_NULL(agg_cells.at(i))) {
      agg_cells.at(i)->reset();
      allocator_.free(agg_cells.at(i));
    }
  }
  agg_cells.reset();
}

ObGroupByCell::ObGroupByCell(const int64_t batch_size, common::ObIAllocator &allocator)
  : batch_size_(batch_size),
    row_capacity_(batch_size),
    group_by_col_offset_(-1),
    group_by_col_param_(nullptr),
    group_by_col_expr_(nullptr),
    group_by_col_datum_buf_(nullptr),
    tmp_group_by_datum_buf_(nullptr),
    agg_cells_(),
    distinct_cnt_(0),
    ref_cnt_(0),
    refs_buf_(nullptr),
    need_extract_distinct_(false),
    distinct_projector_buf_(nullptr),
    agg_datum_buf_(nullptr),
    is_processing_(false),
    projected_cnt_(0),
    agg_cell_factory_(allocator),
    padding_allocator_("PDGroupByPad", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    allocator_(allocator)
{
}

void ObGroupByCell::reset()
{
  batch_size_ = 0;
  row_capacity_ = 0;
  group_by_col_offset_ = -1;
  group_by_col_param_ = nullptr;
  group_by_col_expr_ = nullptr;
  agg_cell_factory_.release(agg_cells_);
  distinct_cnt_ = 0;
  ref_cnt_ = 0;
  free_group_by_buf(allocator_, group_by_col_datum_buf_);
  free_group_by_buf(allocator_, tmp_group_by_datum_buf_);
  if (nullptr != refs_buf_) {
    allocator_.free(refs_buf_);
    refs_buf_ = nullptr;
  }
  need_extract_distinct_ = false;
  free_group_by_buf(allocator_, distinct_projector_buf_);
  if (nullptr != agg_datum_buf_) {
    agg_datum_buf_->reset();
    allocator_.free(agg_datum_buf_);
    agg_datum_buf_ = nullptr;
  }
  padding_allocator_.reset();
  is_processing_ = false;
  projected_cnt_ = 0;
}

void ObGroupByCell::reuse()
{
  distinct_cnt_ = 0;
  ref_cnt_ = 0;
  for (int64_t i = 0; i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->reuse();
  }
  need_extract_distinct_ = false;
  if (nullptr != distinct_projector_buf_) {
    distinct_projector_buf_->fill_items(-1);
  }
  padding_allocator_.reuse();
  is_processing_ = false;
  projected_cnt_ = 0;
}

int ObGroupByCell::init(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
  if (OB_UNLIKELY(nullptr == param.iter_param_.group_by_cols_project_ ||
                  0 == param.iter_param_.group_by_cols_project_->count() ||
                  nullptr == out_cols_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param.iter_param_));
  } else {
    common::ObDatum *group_by_col_datums = nullptr;
    group_by_col_offset_ = param.iter_param_.group_by_cols_project_->at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < param.output_exprs_->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == param.output_exprs_->at(i)->type_) {
        LOG_TRACE("Group by pushdown in batch nlj", K(ret));
        continue;
      } else if (nullptr == param.output_sel_mask_ || param.output_sel_mask_->at(i)) {
        int32_t col_offset = param.iter_param_.out_cols_project_->at(i);
        int32_t col_index = param.iter_param_.read_info_->get_columns_index().at(col_offset);
        const share::schema::ObColumnParam *col_param = out_cols_param->at(col_offset);
        sql::ObExpr *expr = param.output_exprs_->at(i);
        ObAggCellBasicInfo basic_info(col_offset, col_index, col_param, expr,
                                      batch_size_, is_pad_char_to_full_length(context.sql_mode_));
        if (group_by_col_offset_ == col_offset) {
          group_by_col_datums = expr->locate_batch_datums(eval_ctx);
          if (OB_ISNULL(group_by_col_datums)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected null datums", K(ret), K(i), KPC(expr));
          } else if (OB_FAIL(new_group_by_buf(group_by_col_datums, batch_size_,
              common::OBJ_DATUM_NUMBER_RES_SIZE, allocator_, group_by_col_datum_buf_))) {
            LOG_WARN("Failed to new buf", K(ret));
          } else {
            const common::ObObjMeta &obj_meta = col_param->get_meta_type();
            if (is_pad_char_to_full_length(context.sql_mode_) && obj_meta.is_fixed_len_char_type()) {
              group_by_col_param_ = col_param;
            }
            group_by_col_expr_ = expr;
          }
        } else if (OB_FAIL(agg_cell_factory_.alloc_cell(basic_info, agg_cells_, false, true, &eval_ctx))) {
          LOG_WARN("Failed to alloc agg cell", K(ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(group_by_col_datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected group by col datums", K(ret), K(param));
    } else if (OB_FAIL(init_agg_cells(param, context, eval_ctx, false))) {
      LOG_WARN("Failed to init agg_cells", K(ret));
    } else {
      if (agg_cells_.count() > 2) {
        lib::ob_sort(agg_cells_.begin(), agg_cells_.end(),
                  [](ObAggCell *a, ObAggCell *b) { return a->get_col_offset() < b->get_col_offset(); });
      }
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(uint32_t) * batch_size_))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory", K(ret));
      } else {
        refs_buf_ = reinterpret_cast<uint32_t*>(buf);
      }
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), KPC(this));
  return ret;
}

int ObGroupByCell::init_for_single_row(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == param.iter_param_.group_by_cols_project_ ||
                  0 == param.iter_param_.group_by_cols_project_->count() ||
                  nullptr ==  param.iter_param_.get_col_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param.iter_param_));
  } else if (OB_FAIL(init_agg_cells(param, context, eval_ctx, true))) {
    LOG_WARN("Failed to init agg_cells", K(ret));
  }
  return ret;
}

int ObGroupByCell::eval_batch(
    const common::ObDatum *datums,
    const int64_t count,
    const int32_t agg_idx,
    const bool is_group_by_col,
    const bool is_default_datum,
    const uint32_t ref_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(agg_idx >= agg_cells_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(agg_idx), K(agg_cells_.count()));
  } else if (OB_UNLIKELY(0 == distinct_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state, not load distinct yet", K(ret));
  } else if (agg_cells_.at(agg_idx)->finished()) {
  } else if (OB_FAIL(agg_cells_.at(agg_idx)->eval_batch_in_group_by(
      datums, count, refs_buf_ + ref_offset, distinct_cnt_, is_group_by_col, is_default_datum))) {
    LOG_WARN("Failed to eval batch with in group by", K(ret));
  } else {
    agg_cells_.at(agg_idx)->set_group_by_result_cnt(distinct_cnt_);
  }
  return ret;
}

int ObGroupByCell::copy_output_row(const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  // just shallow copy output datum to agg
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->set_group_by_result_cnt(batch_idx);
    if (OB_FAIL(agg_cells_.at(i)->copy_output_row(batch_idx - 1))) {
      LOG_WARN("Failed to copy output row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_distinct_cnt(batch_idx);
  }
  return ret;
}

int ObGroupByCell::copy_output_rows(const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  // just shallow copy output datums to agg
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->set_group_by_result_cnt(batch_idx);
    if (OB_FAIL(agg_cells_.at(i)->copy_output_rows(batch_idx))) {
      LOG_WARN("Failed to copy output rows", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    set_distinct_cnt(batch_idx);
  }
  return ret;
}

int ObGroupByCell::copy_single_output_row(sql::ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  // just shallow copy output datum to agg
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->copy_single_output_row(ctx))) {
      LOG_WARN("Failed to copy output row", K(ret));
    }
  }
  return ret;
}

int ObGroupByCell::collect_result()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->collect_batch_result_in_group_by(distinct_cnt_);
  }
  return ret;
}

int ObGroupByCell::add_distinct_null_value()
{
  int ret = OB_SUCCESS;
  if (distinct_cnt_ + 1 > group_by_col_datum_buf_->get_capacity()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected distinct cnt", K(ret), K(distinct_cnt_), K(batch_size_), KPC(group_by_col_datum_buf_));
  } else {
    common::ObDatum *datums = get_group_by_col_datums_to_fill();
    datums[distinct_cnt_].set_null();
    distinct_cnt_++;
  }
  return ret;
}

int ObGroupByCell::prepare_tmp_group_by_buf()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(distinct_projector_buf_)) {
    if (OB_FAIL(new_group_by_buf((int16_t*)nullptr, 0, sizeof(int16_t), allocator_, distinct_projector_buf_))) {
      LOG_WARN("Failed to new buf", K(ret));
    } else if (OB_FAIL(distinct_projector_buf_->reserve(batch_size_))) {
      LOG_WARN("Failed to new buf", K(ret));
    } else if (OB_FAIL(ObAggDatumBuf::new_agg_datum_buf(batch_size_, false, allocator_, agg_datum_buf_))) {
      LOG_WARN("Failed to alloc agg datum buf", K(ret));
    } else if (OB_FAIL(new_group_by_buf(agg_datum_buf_->get_datums(), batch_size_,
        common::OBJ_DATUM_NUMBER_RES_SIZE, allocator_, tmp_group_by_datum_buf_))) {
      LOG_WARN("Failed to new buf", K(ret));
    } else {
      distinct_projector_buf_->fill_items(-1);
    }
  }
  if (OB_SUCC(ret)) {
    need_extract_distinct_ = true;
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(need_extract_distinct_), K(batch_size_));
  return ret;
}

int ObGroupByCell::reserve_group_by_buf(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_by_col_datum_buf_->reserve(size))) {
    LOG_WARN("Failed to reserve buf", K(ret));
  } else if (nullptr != distinct_projector_buf_) {
    if (OB_FAIL(distinct_projector_buf_->reserve(size))) {
       LOG_WARN("Failed to reserve buf", K(ret));
    } else if (FALSE_IT(distinct_projector_buf_->fill_items(-1))) {
    } else if (OB_FAIL(tmp_group_by_datum_buf_->reserve(size))) {
      LOG_WARN("Failed to reserve buf", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->reserve_group_by_buf(size))) {
      LOG_WARN("Failed to prepare extra buf", K(ret), K(i), KPC(agg_cells_.at(i)));
    }
  }
  return ret;
}

int ObGroupByCell::output_extra_group_by_result(int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(!group_by_col_datum_buf_->is_use_extra_buf())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), KPC(group_by_col_datum_buf_));
  } else if (OB_UNLIKELY(0 == projected_cnt_ && row_capacity_ != batch_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row_capacity, must be equal with batch_size at first", K(ret), K(row_capacity_), K(batch_size_));
  } else if (projected_cnt_ >= distinct_cnt_) {
    ret = OB_ITER_END;
  } else {
    count = MIN(row_capacity_, distinct_cnt_ - projected_cnt_);
    common::ObDatum *result_datum = nullptr;
    common::ObDatum *sql_result_datums = group_by_col_datum_buf_->get_sql_result_datums();
    common::ObDatum *extra_result_datums = group_by_col_datum_buf_->get_extra_result_datums();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(sql_result_datums[i].from_storage_datum(
          extra_result_datums[projected_cnt_ + i], group_by_col_expr_->obj_datum_map_))) {
        LOG_WARN("Failed to output extra buf", K(ret));
      }
    }
    // do nothing for the first batch, the aggregated values is already in the sql datum
    if (OB_SUCC(ret) && projected_cnt_ >= batch_size_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
        if (OB_FAIL(agg_cells_.at(i)->output_extra_group_by_result(projected_cnt_, count))) {
          LOG_WARN("Failed to prepare extra buf", K(ret), K(i), KPC(agg_cells_.at(i)));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    projected_cnt_ += count;
    if (projected_cnt_ >= distinct_cnt_) {
      ret = OB_ITER_END;
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(count), K(projected_cnt_), K(distinct_cnt_), K(row_capacity_));
  return ret;
}

int ObGroupByCell::pad_column_in_group_by(const int64_t row_cap)
{
  int ret = OB_SUCCESS;
  common::ObDatum *sql_result_datums = group_by_col_datum_buf_->get_sql_result_datums();
  if (nullptr != group_by_col_param_ &&
      group_by_col_param_->get_meta_type().is_fixed_len_char_type() &&
      OB_FAIL(storage::pad_on_datums(
              group_by_col_param_->get_accuracy(),
              group_by_col_param_->get_meta_type().get_collation_type(),
              padding_allocator_,
              row_cap,
              sql_result_datums))) {
    LOG_WARN("Failed to pad group by column", K(ret), K(row_cap), KPC_(group_by_col_param));
  }
  for (int i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    if (OB_FAIL(agg_cells_.at(i)->pad_column_in_group_by(row_cap, padding_allocator_))) {
      LOG_WARN("Failed to pad column for aggregate datums", K(ret), K(row_cap));
    }
  }
  return ret;
}

int ObGroupByCell::extract_distinct()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(ref_cnt_));
  } else {
    common::ObDatum *group_by_col_datums = group_by_col_datum_buf_->get_group_by_datums();
    common::ObDatum *tmp_group_by_datums = tmp_group_by_datum_buf_->get_group_by_datums();
    for (int64_t i = 0; OB_SUCC(ret) && i < ref_cnt_; ++i) {
      uint32_t &ref = refs_buf_[i];
      if (OB_UNLIKELY(ref >= group_by_col_datum_buf_->get_capacity())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected ref", K(ret), K(ref), K(batch_size_));
      } else {
        int16_t &distinct_projector = distinct_projector_buf_->at(ref);
        if (-1 == distinct_projector) {
          // distinct val is not extracted yet
          if (OB_FAIL(group_by_col_datums[distinct_cnt_].from_storage_datum(tmp_group_by_datums[ref], group_by_col_expr_->obj_datum_map_))) {
            LOG_WARN("Failed to clone datum", K(ret), K(tmp_group_by_datums[ref]), K(group_by_col_expr_->obj_datum_map_));
          } else {
            distinct_projector = distinct_cnt_;
            ref = distinct_cnt_;
            distinct_cnt_++;
          }
        } else {
          // distinct val is already extracted
          ref = distinct_projector;
        }

      }
    }
    LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(ref_cnt_), K(distinct_cnt_));
  }
  return ret;
}

int ObGroupByCell::assign_agg_cells(const sql::ObExpr *col_expr, common::ObIArray<int32_t> &agg_idxs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCell *agg_cell = agg_cells_.at(i);
    if (agg_cell->is_assigned_to_group_by_processor()) {
    } else if ((ObPDAggType::PD_FIRST_ROW == agg_cell->get_type() && col_expr == agg_cell->get_agg_expr()) ||
        (ObPDAggType::PD_COUNT == agg_cell->get_type() && !agg_cell->need_access_data()) ||
        (ObPDAggType::PD_FIRST_ROW != agg_cell->get_type() &&col_expr == agg_cell->get_agg_expr()->args_[0])) {
      if (OB_FAIL(agg_idxs.push_back(i))) {
        LOG_WARN("Failed to push back", K(ret));
      } else {
        agg_cell->set_assigned_to_group_by_processor();
      }
    }
  }
  return ret;
}

int ObGroupByCell::check_distinct_and_ref_valid()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ref_cnt_ <= 0 || distinct_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state", K(ret), K(ref_cnt_), K(distinct_cnt_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ref_cnt_; ++i) {
    if (OB_UNLIKELY(refs_buf_[i] >= distinct_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected ref", K(ret), K(i), K(ref_cnt_), K(distinct_cnt_), K(ObArrayWrap<uint32_t>(refs_buf_, ref_cnt_)));
    }
  }
  return ret;
}

int ObGroupByCell::init_uniform_header(
    const sql::ObExprPtrIArray *output_exprs,
    const sql::ObExprPtrIArray *agg_exprs,
    sql::ObEvalCtx &eval_ctx,
    const bool init_output)
{
  int ret = OB_SUCCESS;
  if (init_output && OB_FAIL(init_exprs_uniform_header(output_exprs, eval_ctx, eval_ctx.max_batch_size_))) {
    LOG_WARN("Failed to init uniform header for output exprs", K(ret));
  } else if (OB_FAIL(init_exprs_uniform_header(agg_exprs, eval_ctx, eval_ctx.max_batch_size_))) {
    LOG_WARN("Failed to init uniform header for agg exprs", K(ret));
  }
  return ret;
}

int ObGroupByCell::init_agg_cells(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx, const bool is_for_single_row)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < param.aggregate_exprs_->count(); ++i) {
    int32_t col_offset = param.iter_param_.agg_cols_project_->at(i);
    int32_t col_index = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? -1 : param.iter_param_.read_info_->get_columns_index().at(col_offset);
    const share::schema::ObColumnParam *col_param = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : out_cols_param->at(col_offset);
    sql::ObExpr *agg_expr = param.aggregate_exprs_->at(i);
    bool exclude_null = false;
    if (T_FUN_COUNT == agg_expr->type_) {
      if (OB_COUNT_AGG_PD_COLUMN_ID != col_offset) {
        exclude_null = col_param->is_nullable_for_write();
      }
    }
    ObAggCellBasicInfo basic_info(col_offset, col_index, col_param, agg_expr,
                                  batch_size_, is_pad_char_to_full_length(context.sql_mode_));
    if (OB_FAIL(agg_cell_factory_.alloc_cell(basic_info, agg_cells_, exclude_null, !is_for_single_row, &eval_ctx))) {
      LOG_WARN("Failed to alloc agg cell", K(ret), K(i));
    }
  }
  return ret;
}

int64_t ObGroupByCell::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(batch_size),
       K_(row_capacity),
       K_(group_by_col_offset),
       KP_(group_by_col_expr),
       K_(agg_cells),
       K_(distinct_cnt),
       K_(ref_cnt),
       K_(is_processing),
       K_(projected_cnt),
       KPC_(group_by_col_datum_buf));
  J_COMMA();
  J_KV(K(ObArrayWrap<uint32_t>(refs_buf_, ref_cnt_)));
  J_OBJ_END();
  return pos;
}

}
}
