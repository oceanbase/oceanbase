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

#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "ob_aggregated_store_vec.h"

namespace oceanbase
{
namespace storage
{
ObAggGroupVec::ObAggGroupVec()
  : agg_cells_(),
    col_param_(nullptr),
    project_expr_(nullptr),
    col_offset_(-1),
    col_index_(-1),
    agg_type_flag_(),
    need_access_data_(false),
    need_get_row_ids_(false)
{
  agg_cells_.set_attr(ObMemAttr(MTL_ID(), "PDAggStore"));
}

ObAggGroupVec::ObAggGroupVec(ObColumnParam* col_param, sql::ObExpr* project_expr,
                             const int32_t col_offset, const int32_t col_index)
  : agg_cells_(),
    col_param_(col_param),
    project_expr_(project_expr),
    col_offset_(col_offset),
    col_index_(col_index),
    agg_type_flag_(),
    need_access_data_(false),
    need_get_row_ids_(false)
{
  agg_cells_.set_attr(ObMemAttr(MTL_ID(), "PDAggStore"));
}

ObAggGroupVec::~ObAggGroupVec()
{
  agg_cells_.reset();
}

void ObAggGroupVec::reuse()
{
  for (int64_t i = 0; i < agg_cells_.count(); ++i) {
    agg_cells_.at(i)->reuse();
  }
}

int ObAggGroupVec::eval(blocksstable::ObStorageDatum &datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_FAIL(agg_cell->eval(datum, row_count))) {
      LOG_WARN("Failed to aggregate one datum", K(ret), K(datum), K(row_count));
    }
  }
  return ret;
}

int ObAggGroupVec::eval_batch(
    const ObTableIterParam *iter_param,
    const ObTableAccessContext *context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t *row_ids,
    const int64_t row_count,
    const bool reserve_memory)
{
  UNUSEDx(iter_param, context);
  int ret = OB_SUCCESS;
  if (nullptr != reader && reserve_memory) {
    reader->reserve_reader_memory(true); // hold memory before aggregation finished
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_FAIL(agg_cell->eval_batch(reader, col_offset, row_ids, row_count))) {
      LOG_WARN("Failed to aggregate batch rows", K(ret), K(row_count));
    }
  }
  return ret;
}

int ObAggGroupVec::fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_FAIL(agg_cell->eval_index_info(index_info, is_cg))) {
      LOG_WARN("Failed to eval index info", K(ret), K(i), K(index_info), K(is_cg), KPC(this));
    }
  }
  return ret;
}

int ObAggGroupVec::collect_result()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_FAIL(agg_cell->collect_result(true/*fill_output*/))) {
      LOG_WARN("Failed to collect results in aggr cell", K(ret));
    }
  }
  return ret;
}

int ObAggGroupVec::can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, const int32_t col_index, bool &can_agg)
{
  int ret = OB_SUCCESS;
  can_agg = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_agg && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_FAIL(agg_cell->can_use_index_info(index_info, col_index, can_agg))) {
      LOG_WARN("Failed to collect results in aggr cell", K(ret));
    }
  }
  return ret;
}

OB_INLINE int ObAggGroupVec::set_agg_type_flag(const ObPDAggType agg_type)
{
  int ret = OB_SUCCESS;
  switch (agg_type) {
    case PD_COUNT : {
      agg_type_flag_.set_count_flag(true);
      break;
    }
    case PD_MIN:
    case PD_MAX : {
      agg_type_flag_.set_minmax_flag(true);
      break;
    }
    case PD_SUM : {
      agg_type_flag_.set_sum_flag(true);
      break;
    }
    case PD_RB_BUILD: {
      agg_type_flag_.set_has_rb_build_agg(true);
      break;
    }
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected aggregate type", K(ret), K(agg_type));
    }
  }
  return ret;
}

ObAggregatedStoreVec::ObAggregatedStoreVec(
    const int64_t batch_size,
    sql::ObEvalCtx &eval_ctx,
    ObTableAccessContext &context,
    sql::ObBitVector *skip_bit)
      : ObVectorStore(batch_size, eval_ctx, context, skip_bit),
        pd_agg_ctx_(batch_size, eval_ctx, skip_bit, *context.stmt_allocator_),
        agg_groups_(),
        pd_agg_factory_(*context.stmt_allocator_),
        allocator_(*context.stmt_allocator_),
        need_access_data_(false),
        need_get_row_ids_(false)
{
  agg_groups_.set_attr(ObMemAttr(MTL_ID(), "PDAggStore"));
  is_vec2_ = true;
}

ObAggregatedStoreVec::~ObAggregatedStoreVec()
{
  reset();
}

void ObAggregatedStoreVec::reset()
{
  ObVectorStore::reset();
  pd_agg_ctx_.reset();
  release_agg_group();
  if (col_mask_set_.created()) {
    col_mask_set_.destroy();
  }
  need_access_data_ = false;
  need_get_row_ids_ = false;
}

void ObAggregatedStoreVec::release_agg_group()
{
  for (int64_t i = 0; i < agg_groups_.count(); ++i) {
    ObAggGroupVec *agg_group = agg_groups_.at(i);
    if (OB_NOT_NULL(agg_group)) {
      pd_agg_factory_.release(agg_group->agg_cells_);
      agg_group->~ObAggGroupVec();
      allocator_.free(agg_group);
    }
  }
  agg_groups_.reset();
}

void ObAggregatedStoreVec::reuse()
{
  ObVectorStore::reuse();
  iter_end_flag_ = IterEndState::PROCESSING;
  count_ = 0;
}

int ObAggregatedStoreVec::reuse_capacity(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0 || capacity > batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(capacity), K(batch_size_));
  } else {
    for (int64_t i = 0; i < agg_groups_.count(); ++i) {
      agg_groups_.at(i)->reuse();
    }
    pd_agg_ctx_.reuse_batch();
    count_ = 0;
    row_capacity_ = capacity;
    eval_ctx_.reuse(capacity);
  }
  return ret;
}

int ObAggregatedStoreVec::init(const ObTableAccessParam &param, common::hash::ObHashSet<int32_t> *agg_col_mask)
{
  UNUSED(agg_col_mask);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.output_exprs_) ||
      OB_ISNULL(param.iter_param_.out_cols_project_) ||
      OB_ISNULL(param.aggregate_exprs_) ||
      OB_ISNULL(param.iter_param_.agg_cols_project_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate pushdown expr and projector", K(ret), K(param.output_exprs_),
        K(param.iter_param_.out_cols_project_),
        K(param.aggregate_exprs_), K(param.iter_param_.agg_cols_project_));
  } else if (param.output_exprs_->count() != param.iter_param_.out_cols_project_->count() ||
      param.aggregate_exprs_->count() != param.iter_param_.agg_cols_project_->count() ||
      param.aggregate_exprs_->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate count", K(ret), K(param.output_exprs_->count()),
        K(param.iter_param_.out_cols_project_->count()),
        K(param.aggregate_exprs_->count()), K(param.iter_param_.agg_cols_project_->count()));
  } else if (OB_FAIL(pd_agg_ctx_.init(param, 1))) {
    LOG_WARN("Failed to init pushdown agg ctx", K(ret), K(param));
  } else if (OB_FAIL(init_agg_groups(param))) {
    LOG_WARN("Failed to init aggregate groups", K(ret));
  } else if (OB_FAIL(check_agg_store_valid())) {
    LOG_WARN("Invalid aggregate store status", K(ret));
  } else if (OB_FAIL(ObVectorStore::init(param, &col_mask_set_))) {
    LOG_WARN("Failed to init ObVectorStore", K(ret));
  }
  return ret;
}

int ObAggregatedStoreVec::init_agg_groups(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObAggGroupVec *agg_group = nullptr;
  ObAggCellVec *agg_cell = nullptr;
  int32_t pre_offset = -1;
  const ObIArray<ObColOffsetMap> &cols_offset_map = pd_agg_ctx_.cols_offset_map_;
  const ObIArray<ObAggrInfo> &agg_infos = pd_agg_ctx_.agg_infos_;
  for (int64_t i = 0; OB_SUCC(ret) && i < cols_offset_map.count(); ++i) {
    const int32_t col_offset = cols_offset_map.at(i).col_offset_;
    const int64_t agg_idx = cols_offset_map.at(i).agg_idx_;
    const int32_t col_index = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? -1 : param.iter_param_.read_info_->get_columns_index().at(col_offset);
    share::schema::ObColumnParam *col_param = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : param.iter_param_.get_col_params()->at(col_offset);
    if (i == 0 || (col_offset != pre_offset && OB_COUNT_AGG_PD_COLUMN_ID != col_offset)) {
      sql::ObExpr *project_expr = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : agg_infos.at(agg_idx).param_exprs_[0];
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAggGroupVec))) ||
          OB_ISNULL(agg_group = new (buf) ObAggGroupVec(col_param, project_expr, col_offset, col_index))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Faile to alloc memory for ObAggrGroupVec", K(ret));
      } else if (OB_FAIL(agg_groups_.push_back(agg_group))) {
        LOG_WARN("Failed to push aggr_group", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool exclude_null = false;
      const ObExpr *agg_expr = agg_infos.at(agg_idx).expr_;
      if (OB_ISNULL(agg_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Unexpected null agg expr", K(ret));
      } else if (T_FUN_COUNT == agg_expr->type_ || T_FUN_SUM_OPNSIZE == agg_expr->type_) {
        if (OB_COUNT_AGG_PD_COLUMN_ID != col_offset) {
          exclude_null = col_param->is_nullable_for_write();
        }
      }
      ObAggCellVecBasicInfo basic_info(pd_agg_ctx_.agg_ctx_, pd_agg_ctx_.rows_, pd_agg_ctx_.row_meta_, pd_agg_ctx_.batch_rows_,
                                       col_offset, col_param, is_pad_char_to_full_length(context_.sql_mode_));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pd_agg_factory_.alloc_cell(basic_info, agg_cell, agg_idx, exclude_null))) {
        LOG_WARN("Failed to alloc aggregate cell", K(ret));
      } else if (OB_COUNT_AGG_PD_COLUMN_ID == col_offset) { // COUNT(*) is added to the first agg_group
        ObAggGroupVec *first_agg_group = nullptr;
        if (OB_ISNULL(first_agg_group = agg_groups_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null agg group", K(ret));
        } else if (OB_FAIL(first_agg_group->agg_cells_.push_back(agg_cell))) {
          LOG_WARN("Failed to push agg cell", K(ret));
        }
      } else if (OB_FAIL(agg_group->agg_cells_.push_back(agg_cell))) {
        LOG_WARN("Failed to push agg cell", K(ret));
      } else {
        pre_offset = col_offset;
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::check_agg_store_valid()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), common::ObModIds::OB_HASH_BUCKET);
  if (OB_FAIL(col_mask_set_.create(agg_groups_.count() * 2, attr))) {
    LOG_WARN("Failed to create mask column set", K(ret), K(agg_groups_.count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
    ObAggGroupVec *agg_group = agg_groups_.at(i);
    for (int j = 0; OB_SUCC(ret) && j < agg_group->agg_cells_.count(); ++j) {
      const ObAggCellVec *agg_cell = agg_group->agg_cells_.at(j);
      const ObPDAggType agg_type = agg_cell->get_type();
      agg_group->need_access_data_ |= agg_cell->need_access_data();
      agg_group->need_get_row_ids_ |= agg_cell->need_get_row_ids();
      if (OB_FAIL(agg_group->set_agg_type_flag(agg_type))) {
        LOG_WARN("Failed to set agg_type_flag", K(ret), K(agg_type));
      }
    }
    if (OB_UNLIKELY(agg_group->need_access_data_ && !agg_group->need_get_row_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid aggregate store status", K(ret), K(i), KPC(agg_group));
    } else if (!agg_group->need_access_data_
               && OB_FAIL(col_mask_set_.set_refactored(agg_group->col_offset_, 0 /*deduplicated*/))) {
      LOG_WARN("Failed to add column offset", K(ret), K(i), KPC(agg_group));
    } else {
      need_access_data_ |= agg_group->need_access_data_;
      need_get_row_ids_ |= agg_group->need_get_row_ids_;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(need_access_data_ && !need_get_row_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid aggregate store status", K(ret), K_(need_access_data), K_(need_get_row_ids));
  }
  return ret;
}

int ObAggregatedStoreVec::can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else {
    can_agg = filter_is_null() && index_info.can_blockscan(iter_param_->has_lob_column_out()) &&
              !index_info.is_left_border() && !index_info.is_right_border();
    for (int64_t i = 0; OB_SUCC(ret) && can_agg && i < agg_groups_.count(); ++i) {
      ObAggGroupVec *agg_group = agg_groups_.at(i);
      if (OB_ISNULL(agg_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
      } else if (OB_FAIL(agg_group->can_use_index_info(index_info, agg_group->col_index_, can_agg))) {
        LOG_WARN("Failed to judge whether agg_group can agg index info", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else {
    set_aggregated_in_prefetch();
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
      ObAggGroupVec *agg_group = agg_groups_.at(i);
      if (OB_ISNULL(agg_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
      } else if (OB_FAIL(agg_group->fill_index_info(index_info, is_cg))) {
        LOG_WARN("Failed to fill index info in aggr_group", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::fill_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockRowScanner &scanner,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  UNUSED(group_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else {
    const bool is_reverse = begin_index > end_index;
    int64_t covered_row_count = is_reverse ? begin_index - end_index : end_index - begin_index;
    // if should check null or not whole block is covered
    // must get valid rows
    bool need_get_row_ids = false;
    int64_t micro_row_count = 0;
    blocksstable::ObIMicroBlockReader *reader = scanner.get_reader();
    if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null reader", K(ret), K(reader));
    } else if (FALSE_IT(reader->reserve_reader_memory(false))) {
    } else if (OB_FAIL(reader->get_row_count(micro_row_count))) {
      LOG_WARN("Failed to get micro row count", K(ret));
    } else if (!need_access_data_) {
      if (need_get_row_ids_ || micro_row_count != covered_row_count) {
        if (OB_FAIL(get_row_ids(reader, begin_index, end_index, count_, false, res))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(end_index));
          }
        }
      } else {
        count_ = nullptr == res.bitmap_ ? covered_row_count : res.bitmap_->popcnt();
        begin_index = end_index;
      }
    } else if (OB_FAIL(ObVectorStore::fill_output_rows(0/*no use*/,
                                                       scanner,
                                                       begin_index,
                                                       end_index,
                                                       res,
                                                       false/*not set_end()*/,
                                                       !filter_is_null()))) {
      LOG_WARN("Failed to project rows in aggregate pushdown", K(ret), K(begin_index), K(end_index), K(res));
    }
    if (OB_SUCC(ret) && OB_FAIL(do_aggregate(reader, need_access_data_))) {
      LOG_WARN("Failed to aggregate rows", K(ret), KP(reader));
    }
  }
  return ret;
}

int ObAggregatedStoreVec::fill_rows(const int64_t group_idx, const int64_t row_count)
{
  UNUSEDx(group_idx, row_count);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else {
    reset_after_aggregate();
  }
  return ret;
}

int ObAggregatedStoreVec::fill_row(blocksstable::ObDatumRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else if (OB_UNLIKELY(count_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected full aggregated store", K(ret), K_(count));
  } else {
    count_++;
    eval_ctx_.set_batch_idx(count_);
    if (OB_FAIL(do_aggregate(nullptr/*reader*/, false/*reserve_memory*/))) {
      LOG_WARN("Failed to aggregate rows", K(ret));
    }
  }
  return ret;
}

int ObAggregatedStoreVec::do_aggregate(blocksstable::ObIMicroBlockReader *reader, const bool reserve_memory)
{
  int ret = OB_SUCCESS;
  if (count_ > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
      ObAggGroupVec *agg_group = agg_groups_.at(i);
      if (OB_ISNULL(agg_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
      } else if (OB_FAIL(agg_group->eval_batch(nullptr/*iter_param*/,
                                               nullptr/*context*/,
                                               agg_group->col_offset_,
                                               reader,
                                               row_ids_,
                                               count_,
                                               reserve_memory))) {
        LOG_WARN("Failed to eval batch", K(ret), KPC(agg_group), K_(need_access_data), K_(need_get_row_ids));
      }
    }
    if (OB_SUCC(ret)) {
      reset_after_aggregate();
    }
  }
  return ret;
}

int ObAggregatedStoreVec::collect_aggregated_result()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else if (OB_FAIL(storage::init_exprs_vector_header(iter_param_->aggregate_exprs_,
                                                       eval_ctx_,
                                                       eval_ctx_.max_batch_size_))) {
    LOG_WARN("Failed to init vector", K(ret), K_(eval_ctx_.max_batch_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
      ObAggGroupVec *agg_group = agg_groups_.at(i);
      if (OB_ISNULL(agg_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
      } else if (OB_FAIL(agg_group->collect_result())) {
        LOG_WARN("Failed to collect results in aggr group", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::get_agg_group(const sql::ObExpr *expr, ObAggGroupVec *&agg_group)
{
  int ret = OB_SUCCESS;
  agg_group = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregatedStoreVec is not inited", K(ret), K(*this));
  } else if (nullptr == expr) {
    // COUNT(*)
    if (OB_UNLIKELY(agg_groups_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpect agg groups count", K(ret), K(agg_groups_.count()));
    } else {
      agg_group = agg_groups_.at(0);
    }
  } else {
    for (int64_t i = 0; i < agg_groups_.count(); ++i) {
      ObAggGroupVec *cur_group = agg_groups_.at(i);
      if (cur_group->project_expr_ == expr) {
        agg_group = cur_group;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(agg_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null agg group", K(ret), KP(expr));
  }
  return ret;
}

} /* namespace stroage */
} /* namespace oceanbase */
