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

#define USING_LOG_PREFIX STORAGE
#include "ob_aggregate_base.h"

namespace oceanbase
{
namespace storage
{
ObAggCellBase::ObAggCellBase(common::ObIAllocator &allocator)
  : bitmap_(nullptr),
    agg_row_reader_(nullptr),
    result_datum_(),
    skip_index_datum_(),
    allocator_(allocator),
    agg_type_(PD_MAX_TYPE),
    is_assigned_to_group_by_processor_(false),
    is_inited_(false)
{
}

void ObAggCellBase::reset()
{
  if (nullptr != bitmap_) {
    bitmap_->~ObBitmap();
    allocator_.free(bitmap_);
    bitmap_ = nullptr;
  }
  if (nullptr != agg_row_reader_) {
    agg_row_reader_->~ObAggRowReader();
    allocator_.free(agg_row_reader_);
    agg_row_reader_ = nullptr;
  }
  result_datum_.reset();
  skip_index_datum_.reset();
  is_assigned_to_group_by_processor_ = false;
  agg_type_ = PD_MAX_TYPE;
  is_inited_ = false;
}

void ObAggCellBase::reuse()
{
  if (nullptr != bitmap_) {
    bitmap_->reuse();
  }
  result_datum_.reuse();
  result_datum_.set_null();
  skip_index_datum_.reuse();
  skip_index_datum_.set_null();
}

int ObAggCellBase::reserve_bitmap(const int64_t count)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggCellVec not inited", K(ret));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid count", K(ret), K(count));
  } else if (OB_NOT_NULL(bitmap_)) {
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

ObGroupByCellBase::ObGroupByCellBase(const int64_t batch_size, common::ObIAllocator &allocator)
  : batch_size_(batch_size),
    row_capacity_(batch_size),
    distinct_cnt_(0),
    ref_cnt_(0),
    projected_cnt_(0),
    refs_buf_(nullptr),
    group_by_col_expr_(nullptr),
    group_by_col_param_(nullptr),
    distinct_projector_buf_(nullptr),
    padding_allocator_("GroupByPad", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    allocator_(allocator),
    group_by_col_offset_(-1),
    need_extract_distinct_(false),
    is_processing_(false),
    is_inited_(false)
{
}

ObGroupByCellBase::~ObGroupByCellBase()
{
  reset();
}

void ObGroupByCellBase::reset()
{
  batch_size_ = 0;
  row_capacity_ = 0;
  group_by_col_offset_ = -1;
  group_by_col_expr_ = nullptr;
  distinct_cnt_ = 0;
  ref_cnt_ = 0;
  if (nullptr != refs_buf_) {
    allocator_.free(refs_buf_);
    refs_buf_ = nullptr;
  }
  need_extract_distinct_ = false;
  free_group_by_buf(allocator_, distinct_projector_buf_);
  padding_allocator_.reset();
  is_processing_ = false;
  projected_cnt_ = 0;
  is_inited_ = false;
}

void ObGroupByCellBase::reuse()
{
  distinct_cnt_ = 0;
  ref_cnt_ = 0;
  need_extract_distinct_ = false;
  if (nullptr != distinct_projector_buf_) {
    distinct_projector_buf_->fill_items(-1);
  }
  padding_allocator_.reuse();
  is_processing_ = false;
  projected_cnt_ = 0;
}

int ObGroupByCellBase::check_distinct_and_ref_valid()
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

ObPushdownAggContext::ObPushdownAggContext(
  const int64_t batch_size,
  sql::ObEvalCtx &eval_ctx,
  sql::ObBitVector *skip_bit,
  common::ObIAllocator &allocator)
  : agg_infos_(allocator),
    cols_offset_map_(allocator),
    agg_ctx_(eval_ctx, MTL_ID(), agg_infos_, pd_agg_label),
    rows_(nullptr),
    row_meta_(&allocator),
    batch_rows_(),
    agg_row_num_(0),
    allocator_(allocator),
    row_allocator_("PDAggRow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  batch_rows_.skip_ = skip_bit;
  batch_rows_.size_ = batch_size;
  batch_rows_.set_all_rows_active(true);
}

ObPushdownAggContext::~ObPushdownAggContext()
{
  reset();
}

void ObPushdownAggContext::reset()
{
  agg_infos_.reset();
  cols_offset_map_.reset();
  agg_ctx_.destroy();
  if (OB_NOT_NULL(rows_)) {
    allocator_.free(rows_);
  }
  row_meta_.reset();
  agg_row_num_ = 0;
  row_allocator_.reset();
}

// If ObGroupByCellBase::decide_use_group_by() return false, only one batch aggregate rows need to be reused.
// If ObGroupByCellBase::decide_use_group_by() return true, reuse other aggregate rows as needed in prepare_aggregate_rows().
void ObPushdownAggContext::reuse_batch()
{
  const int64_t batch_size = MIN(batch_rows_.size_, agg_row_num_);
  for (int i = 0; i < batch_size; ++i) {
    share::aggregate::AggrRowPtr agg_row = (char *)rows_[i]->get_extra_payload(row_meta_);
    setup_agg_row(agg_row, agg_ctx_.row_meta().row_size_);
  }
}

int ObPushdownAggContext::init(const ObTableAccessParam &param, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (nullptr == param.aggregate_exprs_ || param.aggregate_exprs_->count() == 0) {
    // no aggregate
  } else if (OB_FAIL(init_agg_infos(param))) {
    LOG_WARN("Failed to init aggr infos", K(ret));
  } else if (OB_FAIL(agg_ctx_.init_row_meta(agg_ctx_.aggr_infos_, allocator_))) {
    LOG_WARN("Failed to init row meta for agg ctx", K(ret));
  } else {
    ObSEArray<ObExpr *, 1> mock_exprs;
    row_meta_.reset();
    if (OB_FAIL(row_meta_.init(mock_exprs, agg_ctx_.row_meta().row_size_))) {
      LOG_WARN("init row meta failed", K(ret));
    } else if (OB_FAIL(prepare_aggregate_rows(row_count))) {
      LOG_WARN("Failed to prepare aggregate rows", K(ret), K(row_count));
    }
  }
  return ret;
}

int ObPushdownAggContext::init_agg_infos(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  const sql::ObExprPtrIArray *agg_exprs = param.aggregate_exprs_;
  if (OB_ISNULL(agg_exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null aggr exprs", K(ret), KP(agg_exprs));
  } else if (OB_FAIL(agg_infos_.init(agg_exprs->count()))) {
    LOG_WARN("Failed to init aggr infos array", K(ret), K(agg_exprs->count()));
  } else if (OB_FAIL(cols_offset_map_.init(agg_exprs->count()))) {
    LOG_WARN("Failed to init column offset array", K(ret), K(agg_exprs->count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs->count(); ++i) {
      ObAggrInfo agg_info(allocator_);
      const int32_t col_offset = param.iter_param_.agg_cols_project_->at(i);
      ObExpr *agg_expr = agg_exprs->at(i);
      if (OB_UNLIKELY(nullptr == agg_expr || agg_expr->arg_cnt_ > 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalie aggr expr", K(ret), KPC(agg_expr));
      } else if (OB_FAIL(agg_info.param_exprs_.init(agg_expr->arg_cnt_))) {
        LOG_WARN("Failed to init param exprs array", K(ret));
      } else {
        agg_info.real_aggr_type_ = agg_expr->type_;
        agg_info.expr_ = agg_expr;
        if (agg_expr->arg_cnt_ > 0 && OB_FAIL(agg_info.param_exprs_.push_back(agg_expr->args_[0]))) {
          LOG_WARN("Failed to push back expr", K(ret), KPC(agg_expr));
        } else if (OB_FAIL(agg_infos_.push_back(agg_info))) {
          LOG_WARN("Failed to push bach agg_info", K(ret), K(i), K(agg_info));
        } else if (OB_FAIL(cols_offset_map_.push_back(ObColOffsetMap(col_offset, i)))) {
          LOG_WARN("Failed to push back col_offset map", K(ret), K(i), K(col_offset));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ob_sort(cols_offset_map_.begin(), cols_offset_map_.end());
    }
  }
  return ret;
}

int ObPushdownAggContext::prepare_aggregate_rows(const int64_t count)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  void *buf = nullptr;
  if (OB_UNLIKELY(count > USE_GROUP_BY_MAX_DISTINCT_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected agg row count", K(ret), K(count));
  } else if (count > agg_row_num_) {
    const int64_t row_count = MIN(MAX(count, 2 * agg_row_num_), USE_GROUP_BY_MAX_DISTINCT_CNT);
    if (nullptr != rows_) {
      row_allocator_.reuse();
      rows_ = nullptr;
    }
    row_size = row_meta_.get_row_fixed_size() + agg_ctx_.row_meta().row_size_;
    if (OB_ISNULL(buf = row_allocator_.alloc((sizeof(ObCompactRow *) + row_size) * row_count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      rows_ = static_cast<ObCompactRow **>(buf);
      char *row_buf = static_cast<char *>(buf) + sizeof(ObCompactRow *) * row_count;
      for (int i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        rows_[i] = new (row_buf + i * row_size) ObCompactRow();
        rows_[i]->init(row_meta_);
        share::aggregate::AggrRowPtr agg_row = (char *)rows_[i]->get_extra_payload(row_meta_);
        setup_agg_row(agg_row, agg_ctx_.row_meta().row_size_);
      }
      agg_row_num_ = row_count;
    }
  } else {
    for (int i = batch_rows_.size_; i < count; ++i) {
      share::aggregate::AggrRowPtr agg_row = (char *)rows_[i]->get_extra_payload(row_meta_);
      setup_agg_row(agg_row, agg_ctx_.row_meta().row_size_);
    }
  }
  return ret;
}

OB_INLINE void ObPushdownAggContext::setup_agg_row(share::aggregate::AggrRowPtr row, const int32_t row_size)
{
  OB_ASSERT(row_size == agg_ctx_.row_meta().row_size_);
  MEMSET(row, 0, row_size);
  for (int col_id = 0; col_id < agg_ctx_.aggr_infos_.count(); col_id++) {
    if (agg_ctx_.aggr_infos_.at(col_id).get_expr_type() != T_FUN_COUNT &&
        agg_ctx_.aggr_infos_.at(col_id).get_expr_type() != T_FUN_SUM_OPNSIZE) {
      ObDatumMeta &res_meta = agg_ctx_.aggr_infos_.at(col_id).expr_->datum_meta_;
      VecValueTypeClass res_tc = get_vec_value_tc(res_meta.type_, res_meta.scale_, res_meta.precision_);
      char *cell = nullptr;
      int32_t cell_len = 0;
      agg_ctx_.row_meta().locate_cell_payload(col_id, row, cell, cell_len);
      // oracle mode use ObNumber as result type for count aggregation
      // we use int64_t as result type for count aggregation in aggregate row
      // and cast int64_t to ObNumber during `collect_group_result`
      if (res_tc == VEC_TC_NUMBER) {
        ObNumberDesc &d = *reinterpret_cast<ObNumberDesc *>(cell);
        // set zero number
        d.len_ = 0;
        d.sign_ = number::ObNumber::POSITIVE;
        d.exp_ = 0;
      } else if (res_tc == VEC_TC_FLOAT) {
        *reinterpret_cast<float *>(cell) = float();
      } else if (res_tc == VEC_TC_DOUBLE || res_tc == VEC_TC_FIXED_DOUBLE) {
        *reinterpret_cast<double *>(cell) = double();
      }
    }
  }
}

ObAggDatumBuf::ObAggDatumBuf(common::ObIAllocator &allocator)
  : size_(0), capacity_(0), datum_size_(0), datums_(nullptr), buf_(nullptr), cell_data_ptrs_(nullptr), allocator_(allocator)
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
    capacity_ = size;
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
  capacity_ = 0;
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
  int64_t new_size = size;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid size", K(ret), K(size));
  } else if (nullptr != datum_buf) {
    if (size > datum_buf->get_capacity()) {
      new_size = MAX(size, 2 * datum_buf->get_capacity());
      allocator.reuse();
      datum_buf = nullptr;
    } else {
      datum_buf->set_size(size);
      datum_buf->reuse();
    }
  }
  if (OB_SUCC(ret) && nullptr == datum_buf) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObAggDatumBuf)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc agg datum buffer", K(ret));
    } else if (FALSE_IT(datum_buf = new (buf) ObAggDatumBuf(allocator))) {
    } else if (OB_FAIL(datum_buf->init(new_size, need_cell_data_ptr, datum_size))) {
      LOG_WARN("Failed to init agg datum buf", K(ret));
    }
  }
  return ret;
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

}
}
