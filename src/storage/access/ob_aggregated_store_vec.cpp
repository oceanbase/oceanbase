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
    agg_row_id_(OB_INVALID_CS_ROW_ID),
    col_offset_(-1),
    col_index_(-1),
    agg_type_flag_(),
    need_access_data_(false),
    need_get_row_ids_(false),
    has_aggr_with_expr_(false)
{
  agg_cells_.set_attr(ObMemAttr(MTL_ID(), "PDAggStore"));
}

ObAggGroupVec::ObAggGroupVec(ObColumnParam* col_param,
                             const int32_t col_offset,
                             const int32_t col_index)
  : agg_cells_(),
    col_param_(col_param),
    project_expr_(nullptr),
    agg_row_id_(OB_INVALID_CS_ROW_ID),
    col_offset_(col_offset),
    col_index_(col_index),
    agg_type_flag_(),
    need_access_data_(false),
    need_get_row_ids_(false),
    has_aggr_with_expr_(false)
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
  agg_row_id_ = OB_INVALID_CS_ROW_ID;
}

int ObAggGroupVec::eval(blocksstable::ObStorageDatum &datum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), K(i), KP(agg_cell));
    } else if (OB_FAIL(agg_cell->eval(datum, row_count))) {
      LOG_WARN("Failed to aggregate one datum", K(ret), K(i), K(datum), K(row_count));
    } else if (OB_FAIL(agg_cell->aggregate_batch_single_rows(0/*agg_row_idx*/))) {
      LOG_WARN("Failed to aggregate batch single rows", K(ret), K(i), KPC(agg_cell));
    }
  }
  return ret;
}

int ObAggGroupVec::eval_batch(
    const ObTableIterParam *iter_param,
    const ObTableAccessContext *context,
    const int32_t col_offset,
    blocksstable::ObIMicroBlockReader *reader,
    const ObPushdownRowIdCtx &pd_row_id_ctx,
    const bool reserve_memory)
{
  UNUSEDx(iter_param, context);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_offset < 0 || !pd_row_id_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(col_offset), K(pd_row_id_ctx));
  } else if (nullptr != reader && reserve_memory) {
    reader->reserve_reader_memory(true); // hold memory before aggregation finished
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(clear_evaluated_infos())) {
    LOG_WARN("Failed to clear evaluated infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), K(i), KP(agg_cell));
    } else if (agg_cell->is_agg_finish(pd_row_id_ctx)) {
      LOG_DEBUG("aggregate has been pushdown to decoder", K(pd_row_id_ctx), K(agg_cell->get_agg_row_id()));
    } else if (OB_FAIL(agg_cell->eval_batch(reader, col_offset, pd_row_id_ctx.row_ids_, pd_row_id_ctx.get_row_count()))) {
      LOG_WARN("Failed to aggregate batch rows", K(ret), K(pd_row_id_ctx));
    }
  }
  return ret;
}

int ObAggGroupVec::fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), K(i), KP(agg_cell));
    } else if (OB_FAIL(agg_cell->eval_index_info(index_info, is_cg))) {
      LOG_WARN("Failed to eval index info", K(ret), K(i), K(index_info), K(is_cg), KPC(this));
    }
  }
  return ret;
}

int ObAggGroupVec::agg_pushdown_decoder(
    blocksstable::ObIMicroBlockReader *reader,
    const int32_t col_offset,
    const ObPushdownRowIdCtx &pd_row_id_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == reader || col_offset < 0 || !pd_row_id_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", KP(reader), K(col_offset), K(pd_row_id_ctx));
  } else if (OB_UNLIKELY(is_agg_finish(pd_row_id_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected status", K_(agg_row_id), K(pd_row_id_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
      ObAggCellVec *agg_cell = agg_cells_.at(i);
      if (OB_ISNULL(agg_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null agg cell", K(ret), K(i), KP(agg_cell));
      } else if (OB_FAIL(agg_cell->agg_pushdown_decoder(reader, col_offset, pd_row_id_ctx))) {
        LOG_WARN("Failed to pushdown aggregate to decoder", K(ret), K(i), K(col_offset), KP(reader), K(pd_row_id_ctx));
      } else if (!pd_row_id_ctx.is_reverse_) {
        agg_row_id_ = i == 0 ? agg_cell->get_agg_row_id() : MIN(agg_row_id_, agg_cell->get_agg_row_id());
      } else {
        agg_row_id_ = i == 0 ? agg_cell->get_agg_row_id() : MAX(agg_row_id_, agg_cell->get_agg_row_id());
      }
    }
  }
  return ret;
}

int ObAggGroupVec::collect_result()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), K(i), KP(agg_cell));
    } else if (OB_FAIL(agg_cell->collect_result(true/*fill_output*/))) {
      LOG_WARN("Failed to collect results in agg cell", K(ret), K(i), KPC(agg_cell));
    }
  }
  return ret;
}

int ObAggGroupVec::reset_agg_row_id()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), KP(agg_cell));
    } else {
      agg_cell->reset_agg_row_id();
    }
  }
  agg_row_id_ = OB_INVALID_CS_ROW_ID;
  return ret;
}

int ObAggGroupVec::clear_evaluated_infos()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells_.count(); ++i) {
    ObAggCellVec *agg_cell = agg_cells_.at(i);
    if (OB_ISNULL(agg_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null agg cell", K(ret), KP(agg_cell));
    } else {
      agg_cell->clear_evaluated_infos();
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

#define SET_AGG_FLAG(op_type, flag_type)         \
  case (op_type) : {                             \
    agg_type_flag_.set_##flag_type##_flag(true); \
    break;                                       \
  }

OB_INLINE int ObAggGroupVec::set_agg_type_flag(const ObPDAggType agg_type)
{
  int ret = OB_SUCCESS;
  switch (agg_type) {
    SET_AGG_FLAG(PD_COUNT, count)
    SET_AGG_FLAG(PD_MIN, minmax)
    SET_AGG_FLAG(PD_MAX, minmax)
    SET_AGG_FLAG(PD_STR_PREFIX_MIN, minmax)
    SET_AGG_FLAG(PD_STR_PREFIX_MAX, minmax)
    SET_AGG_FLAG(PD_SUM, sum)
    SET_AGG_FLAG(PD_HLL, hll)
    SET_AGG_FLAG(PD_SUM_OP_SIZE, sum_op_nsize)
    SET_AGG_FLAG(PD_RB_AND, rb_build)
    SET_AGG_FLAG(PD_RB_OR, rb_build)
    SET_AGG_FLAG(PD_RB_BUILD, rb_build)
    SET_AGG_FLAG(PD_COUNT_SUM, count_sum)
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected aggregate type", K(ret), K(agg_type));
    }
  }
  return ret;
}
#undef SET_AGG_FLAG

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
        need_get_row_ids_(false),
        has_aggr_with_expr_(false)
{
  agg_groups_.set_attr(ObMemAttr(MTL_ID(), "PDAggStore"));
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
  has_aggr_with_expr_ = false;
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
  sql::ObExpr *project_expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < cols_offset_map.count(); ++i) {
    const int32_t col_offset = cols_offset_map.at(i).col_offset_;
    const int64_t agg_idx = cols_offset_map.at(i).agg_idx_;
    const int32_t col_index = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? -1 : param.iter_param_.read_info_->get_columns_index().at(col_offset);
    share::schema::ObColumnParam *col_param = OB_COUNT_AGG_PD_COLUMN_ID == col_offset ? nullptr : param.iter_param_.get_col_params()->at(col_offset);
    if (i == 0 || (col_offset != pre_offset && OB_COUNT_AGG_PD_COLUMN_ID != col_offset)) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAggGroupVec))) ||
          OB_ISNULL(agg_group = new (buf) ObAggGroupVec(col_param, col_offset, col_index))) {
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
      } else if (OB_FAIL(pd_agg_factory_.alloc_cell(basic_info, agg_idx, param, exclude_null, agg_cell))) {
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
      if (OB_ISNULL(agg_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null agg cell", K(ret), KP(agg_cell));
      } else if (OB_FAIL(agg_group->set_agg_type_flag(agg_cell->get_type()))) {
        LOG_WARN("Failed to set agg_type_flag", K(ret), K(agg_cell->get_type()));
      } else if (j == 0) { // COUNT(*) is the last aggregate in on agg group
        agg_group->project_expr_ = agg_cell->get_project_expr();
      } else if (OB_UNLIKELY(nullptr != agg_cell->get_project_expr() && agg_group->project_expr_ != agg_cell->get_project_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect different project expr in one agg group", K(ret), KPC(agg_group), KPC(agg_cell));
      }
      if (OB_SUCC(ret)) {
        agg_group->need_access_data_ |= agg_cell->need_access_data();
        agg_group->need_get_row_ids_ |= agg_cell->need_get_row_ids();
        agg_group->has_aggr_with_expr_ |= agg_cell->is_aggr_with_expr();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(agg_group->need_access_data_ && !agg_group->need_get_row_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid aggregate store status", K(ret), K(i), KPC(agg_group));
    } else if (!agg_group->need_access_data_
               && OB_FAIL(col_mask_set_.set_refactored(agg_group->col_offset_, 0 /*deduplicated*/))) {
      LOG_WARN("Failed to add column offset", K(ret), K(i), KPC(agg_group));
    } else {
      need_access_data_ |= agg_group->need_access_data_;
      need_get_row_ids_ |= agg_group->need_get_row_ids_;
      has_aggr_with_expr_ |= agg_group->has_aggr_with_expr_;
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
    // pre agg info will not be generated for lob out row
    can_agg = filter_is_null() && index_info.can_blockscan() &&
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
  blocksstable::ObIMicroBlockReader *reader = scanner.get_reader();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null reader", K(ret), KP(reader));
  } else {
    bool all_agg_pd_decoder = false;
    reader->reserve_reader_memory(false);
    if (OB_FAIL(agg_pushdown_decoder(reader, begin_index, end_index, res, all_agg_pd_decoder))) {
      LOG_WARN("Failed to push aggregate to decoder", K(ret), KP(reader));
    } else if (all_agg_pd_decoder) {
      reset_after_aggregate();
      LOG_DEBUG("all aggregate in one micro block has been pushdown to decoder",
        K(ret), K(begin_index), K(end_index), KP(reader), KPC(res.bitmap_));
    // TODO @wenye: only fill column whose aggregate is not pushdown to decoder.
    } else if (OB_FAIL(fill_output_rows(scanner, begin_index, end_index, res))) {
      LOG_WARN("Failed to project rows in aggregate pushdown", K(ret), K(begin_index), K(end_index), K(res));
    } else {
      const bool is_reverse = begin_index > end_index;
      const int64_t bound_row_id = is_reverse ? begin_index + 1 : begin_index - 1;
      if (OB_FAIL(do_aggregate(reader, need_access_data_, bound_row_id))) {
        LOG_WARN("Failed to aggregate rows", K(ret), KP(reader));
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::agg_pushdown_decoder(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res,
    bool &all_agg_pd_decoder)
{
  int ret = OB_SUCCESS;
  ObPushdownRowIdCtx pd_row_id_ctx;
  const bool is_reverse = begin_index > end_index;
  const int64_t covered_row_count = is_reverse ? begin_index - end_index : end_index - begin_index;
  all_agg_pd_decoder = false;
  if (OB_UNLIKELY(0 != count_)) {
    // defense code: data cross fuse and micro block is banned
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate store count", K(ret), K(count_),
      K(begin_index), K(end_index), K(res), KPC(this), KP(reader));
  } else if (!need_get_row_ids_) {
    int64_t micro_row_count = 0;
    if (OB_FAIL(reader->get_row_count(micro_row_count))) {
      LOG_WARN("Failed to get micro row count", K(ret));
    } else if (covered_row_count == micro_row_count) {
      pd_row_id_ctx.begin_ = 0;
      pd_row_id_ctx.end_ = nullptr == res.bitmap_ ? covered_row_count - 1 : res.bitmap_->popcnt() - 1;
      pd_row_id_ctx.bound_row_id_ = pd_row_id_ctx.end_;
      if (pd_row_id_ctx.get_row_count() == 0) {
      } else if (OB_FAIL(agg_groups_pushdown_decoder(reader, pd_row_id_ctx, all_agg_pd_decoder))) {
        LOG_WARN("fail to pushdown aggregate to decoder", K(ret), K(pd_row_id_ctx), KP(reader));
      }
    }
  } else if (nullptr == res.bitmap_ || res.bitmap_->is_all_true()) {
    pd_row_id_ctx.begin_ = is_reverse ? end_index + 1 : begin_index;
    pd_row_id_ctx.end_ = is_reverse ? begin_index : end_index - 1;
    pd_row_id_ctx.bound_row_id_ = is_reverse ? end_index + 1 : end_index - 1;
    pd_row_id_ctx.is_reverse_ = is_reverse;
    if (OB_FAIL(agg_groups_pushdown_decoder(reader, pd_row_id_ctx, all_agg_pd_decoder))) {
      LOG_WARN("fail to pushdown aggregate to decoder", K(ret), K(pd_row_id_ctx), KP(reader));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (all_agg_pd_decoder) {
    count_ = nullptr == res.bitmap_ ? covered_row_count : res.bitmap_->popcnt();
    begin_index = end_index;
    LOG_TRACE("all aggregate has been pushdown to decoder", K(ret), K(pd_row_id_ctx), KPC(res.bitmap_), K_(count));
  } else if (OB_FAIL(get_row_ids(reader, begin_index, end_index, count_, true, res))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get row ids", K(ret), K(begin_index), K(end_index));
    }
  } else if (0 == count_) {
    // skip if no rows selected
  } else {
    pd_row_id_ctx.row_ids_ = row_ids_;
    pd_row_id_ctx.row_cap_ = count_;
    pd_row_id_ctx.bound_row_id_ = is_reverse ? begin_index + 1 : begin_index - 1;
    pd_row_id_ctx.is_reverse_ = is_reverse;
    if (OB_FAIL(agg_groups_pushdown_decoder(reader, pd_row_id_ctx, all_agg_pd_decoder))) {
      LOG_WARN("fail to pushdown aggregate to decoder", K(ret), K(pd_row_id_ctx), KP(reader));
    }
  }
  return ret;
}

int ObAggregatedStoreVec::agg_groups_pushdown_decoder(
    blocksstable::ObIMicroBlockReader *reader,
    const ObPushdownRowIdCtx &pd_row_id_ctx,
    bool &all_agg_pd_decoder)
{
  int ret = OB_SUCCESS;
  all_agg_pd_decoder = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
    ObAggGroupVec *agg_group = agg_groups_.at(i);
    if (OB_ISNULL(agg_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
    } else if (agg_group->is_agg_finish(pd_row_id_ctx)) {
      LOG_DEBUG("all aggregate in one agg group has been pushdown to decoder", K(ret), K(i), K(pd_row_id_ctx), KPC(agg_group));
    } else if (OB_FAIL(agg_group->agg_pushdown_decoder(reader, agg_group->col_offset_, pd_row_id_ctx))) {
      LOG_WARN("fail pushdown aggregate to decoder", K(ret), K(i), KPC(agg_group), K(pd_row_id_ctx));
    } else if (agg_group->is_agg_finish(pd_row_id_ctx)) {
      LOG_DEBUG("all aggregate in one agg group has been pushdown to decoder", K(ret), K(i), K(pd_row_id_ctx), KPC(agg_group));
    } else {
      all_agg_pd_decoder = false;
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
  } else if (OB_UNLIKELY(count_ >= row_capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected full aggregated store", K(ret), K_(count), K_(row_capacity));
  } else {
    count_++;
    eval_ctx_.set_batch_idx(count_);
    if (count_ >= row_capacity_) {
       if (OB_FAIL(do_aggregate(nullptr/*reader*/, false/*reserve_memory*/))) {
        LOG_WARN("Failed to aggregate rows", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregatedStoreVec::fill_output_rows(
    blocksstable::ObIMicroBlockRowScanner &scanner,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  if (0 == count_) {
  } else if (iter_param_->use_new_format()) {
    if (OB_FAIL(scanner.get_rows_for_rich_format(cols_projector_,
                                                  col_params_,
                                                  row_ids_,
                                                  count_,
                                                  0,
                                                  cell_data_ptrs_,
                                                  len_array_,
                                                  exprs_,
                                                  &default_datums_,
                                                  is_pad_char_to_full_length(context_.sql_mode_),
                                                  !filter_is_null() || has_aggr_with_expr_))) {
      LOG_WARN("Failed to get rows for rich format", K(ret));
    }
  } else if (OB_FAIL(scanner.get_rows_for_old_format(cols_projector_,
                                                      col_params_,
                                                      row_ids_,
                                                      count_,
                                                      0,
                                                      cell_data_ptrs_,
                                                      exprs_,
                                                      datum_infos_,
                                                      &default_datums_,
                                                      is_pad_char_to_full_length(context_.sql_mode_)))) {
    LOG_WARN("Failed to get rows for old format", K(ret));
  }
  if (OB_SUCC(ret)) {
    eval_ctx_.set_batch_idx(count_);
    if (OB_UNLIKELY(IterEndState::LIMIT_ITER_END == iter_end_flag_)) {
      ret = OB_ITER_END;
    }
    EVENT_ADD(ObStatEventIds::SSSTORE_READ_ROW_COUNT, count_);
  }
  LOG_TRACE("[Vectorized] aggregate store copy rows", K(ret),
            K(begin_index), K(end_index), K_(count), K(res),
            "row_ids", common::ObArrayWrap<const int32_t>(row_ids_, count_),
            KPC(this));
  return ret;
}

int ObAggregatedStoreVec::do_aggregate(
    blocksstable::ObIMicroBlockReader *reader,
    const bool reserve_memory,
    const int64_t bound_row_id/*OB_INVALID_CS_ROW_ID*/)
{
  int ret = OB_SUCCESS;
  if (count_ > 0) {
    ObPushdownRowIdCtx pd_row_id_ctx(row_ids_, count_);
    pd_row_id_ctx.bound_row_id_ = bound_row_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
      ObAggGroupVec *agg_group = agg_groups_.at(i);
      if (OB_ISNULL(agg_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
      } else if (OB_FAIL(agg_group->eval_batch(nullptr/*iter_param*/,
                                               nullptr/*context*/,
                                               agg_group->col_offset_,
                                               reader,
                                               pd_row_id_ctx,
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
  const int64_t batch_size = MAX(eval_ctx_.max_batch_size_, 1);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAggregateStoreVec is not inited", K(ret));
  } else if (OB_FAIL(storage::init_exprs_vector_header(iter_param_->aggregate_exprs_,
                                                       eval_ctx_,
                                                       batch_size))) {
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

int ObAggregatedStoreVec::reset_agg_row_id()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_groups_.count(); ++i) {
    ObAggGroupVec *agg_group = agg_groups_.at(i);
    if (OB_ISNULL(agg_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null aggregate group", K(ret), KP(agg_group));
    } else if (OB_FAIL(agg_group->reset_agg_row_id())){
      LOG_WARN("Failed to reset agg_row_id in agg group", K(ret));
    }
  }
  return ret;
}

} /* namespace stroage */
} /* namespace oceanbase */
