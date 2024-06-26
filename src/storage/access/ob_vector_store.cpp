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
#include "ob_vector_store.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/access/ob_pushdown_aggregate.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObVectorStore::ObVectorStore(
    const int64_t batch_size,
    sql::ObEvalCtx &eval_ctx,
    ObTableAccessContext &context)
    : ObBlockBatchedRowStore(batch_size, eval_ctx, context),
    count_(0),
    exprs_(*context_.stmt_allocator_),
    cols_projector_(*context_.stmt_allocator_),
    datum_infos_(*context_.stmt_allocator_),
    col_params_(*context_.stmt_allocator_),
    group_idx_expr_(nullptr),
    default_row_(),
    group_by_cell_(nullptr),
    iter_param_(nullptr)
  {}

ObVectorStore::~ObVectorStore()
{
  reset();
}

void ObVectorStore::reset()
{
  ObBlockBatchedRowStore::reset();
  count_ = 0;
  exprs_.reset();
  cols_projector_.reset();
  datum_infos_.reset();
  col_params_.reset();
  group_idx_expr_ = nullptr;
  iter_param_ = nullptr;
  default_row_.reset();
  if (nullptr != group_by_cell_) {
    group_by_cell_->~ObGroupByCell();
    context_.stmt_allocator_->free(group_by_cell_);
    group_by_cell_ = nullptr;
  }
}

int ObVectorStore::init(const ObTableAccessParam &param)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("vector store init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(nullptr == param.output_exprs_ ||
                         nullptr == param.iter_param_.out_cols_project_ ||
                         nullptr == param.iter_param_.get_col_params())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid init param", K(ret), KP(param.output_exprs_),
             K(param.iter_param_));
  } else if (OB_FAIL(ObBlockBatchedRowStore::init(param))) {
    LOG_WARN("fail to init ObBlockBatchedRowStore", K(ret));
  } else {
    int64_t expr_count = param.output_exprs_->count();
    const share::schema::ObColumnParam *col_param = nullptr;
    const common::ObIArray<int32_t>& out_cols_projector = *param.iter_param_.out_cols_project_;
    const common::ObIArray<bool> *output_sel_mask = param.output_sel_mask_;
    const common::ObIArray<share::schema::ObColumnParam *> *out_cols_param = param.iter_param_.get_col_params();
    const bool use_new_format = param.iter_param_.use_new_format();
    if (OB_FAIL(exprs_.init(expr_count))) {
      LOG_WARN("Failed to init exprs", K(ret));
    } else if (OB_FAIL(default_row_.init(*context_.stmt_allocator_, out_cols_param->count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(out_cols_param->count()));
    } else if (OB_FAIL(cols_projector_.init(expr_count))) {
      LOG_WARN("Failed to init cols", K(ret));
    } else if (OB_FAIL(datum_infos_.init(expr_count))) {
      LOG_WARN("Failed to init datum infos", K(ret), K(expr_count));
    } else if (OB_FAIL(col_params_.init(expr_count))) {
      LOG_WARN("Fail to init col params", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; i++) {
      common::ObDatum *datums = nullptr;
      sql::ObExpr *expr = nullptr;
      if (param.iter_param_.need_fill_group_idx() &&
          T_PSEUDO_GROUP_ID == param.output_exprs_->at(i)->type_) {
        group_idx_expr_ = param.output_exprs_->at(i);
      } else if (nullptr == output_sel_mask || output_sel_mask->at(i)) {
        if (OB_FAIL(exprs_.push_back(param.output_exprs_->at(i)))) {
          LOG_WARN("fail to push back expr", K(ret));
        } else if (OB_FAIL(cols_projector_.push_back(out_cols_projector.at(i)))) {
          LOG_WARN("fail to push back col", K(ret));
        } else if (OB_ISNULL(expr = param.output_exprs_->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null expr", K(ret), K(i), K(param.output_exprs_));
        } else if (OB_ISNULL(datums = expr->locate_batch_datums(eval_ctx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null datums", K(ret), K(i), KPC(expr));
        } else if (OB_UNLIKELY(!use_new_format &&
                               !(expr->is_variable_res_buf()) &&
                               datums->ptr_ != eval_ctx_.frames_[expr->frame_idx_] + expr->res_buf_off_)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("Unexpected sql expr datum buffer", K(ret), KP(datums->ptr_), K(eval_ctx_), KPC(expr));
        } else if (OB_FAIL(datum_infos_.push_back(blocksstable::ObSqlDatumInfo(datums, param.output_exprs_->at(i))))) {
          LOG_WARN("fail to push back datum", K(ret), K(datums));
        }

        if (OB_FAIL(ret)) {
        } else if (out_cols_projector.at(i) >= out_cols_param->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column id", K(ret), K(out_cols_projector.at(i)), K(out_cols_param->count()));
        } else {
          col_param = nullptr;
          const common::ObObjMeta &obj_meta = out_cols_param->at(out_cols_projector.at(i))->get_meta_type();
          if (is_pad_char_to_full_length(context_.sql_mode_) && obj_meta.is_fixed_len_char_type()) {
            col_param = out_cols_param->at(out_cols_projector.at(i));
          } else if (obj_meta.is_lob_storage() || obj_meta.is_decimal_int()) {
            col_param = out_cols_param->at(out_cols_projector.at(i));
          }
          ObObj def_cell(out_cols_param->at(out_cols_projector.at(i))->get_orig_default_value());
          if (def_cell.is_nop_value()) {
            default_row_.storage_datums_[col_params_.count()].set_nop();
          } else if (OB_FAIL(default_row_.storage_datums_[col_params_.count()].from_obj(def_cell, expr->obj_datum_map_))) {
            LOG_WARN("convert obj to datum failed", K(ret), K(col_params_.count()), K(def_cell));
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(col_params_.push_back(col_param))) {
            LOG_WARN("failed to push back col param", K(ret));
          }
        }
      }
    }
    default_row_.count_ = col_params_.count();
  }
  if (OB_SUCC(ret) && param.iter_param_.enable_pd_group_by()) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = context_.stmt_allocator_->alloc(sizeof(ObGroupByCell)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc datum buf", K(ret));
    } else if (FALSE_IT(group_by_cell_ = new (buf) ObGroupByCell(batch_size_, *context_.stmt_allocator_))) {
    } else if (OB_FAIL(group_by_cell_->init(param, context_, eval_ctx_))) {
      LOG_WARN("Failed to init group by cell", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  } else {
    iter_param_ = &(param.iter_param_);
  }
  return ret;
}

int ObVectorStore::reuse_capacity(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBlockBatchedRowStore::reuse_capacity(capacity))) {
    LOG_WARN("Fail to reuse capacity", K(ret), K(capacity));
  } else if (nullptr != group_by_cell_) {
    if (!group_by_cell_->is_processing()) {
      group_by_cell_->reuse();
    }
    group_by_cell_->set_row_capacity(capacity);
  }
  if (OB_SUCC(ret)) {
    count_ = 0;
  }
  return ret;
}

// called after deep copy
int ObVectorStore::fill_row(blocksstable::ObDatumRow &row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ >= row_capacity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect full vector store", K(ret), K(count_));
  } else {
    count_++;
    eval_ctx_.set_batch_idx(count_);
    if (count_ >= row_capacity_) {
      set_end();
    }
    if (nullptr != group_by_cell_) {
      if (OB_FAIL(group_by_cell_->copy_output_row(count_))) {
        LOG_WARN("Failed to copy output row row", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorStore::fill_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockRowScanner *scanner,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  bool can_group_by = false;
  blocksstable::ObIMicroBlockReader *reader = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector store is not inited", K(ret));
  } else if (OB_ISNULL(scanner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null scanner", K(ret));
  } else if (OB_UNLIKELY((nullptr == scanner) ||
                         (0 != count_ && nullptr == group_by_cell_))) {
    // defense code: data cross fuse and micro block is banned
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected vector store count", K(ret), KP(scanner), K_(count), KP_(group_by_cell));
  } else if (FALSE_IT(reader = scanner->get_reader())) {
  } else if (OB_FAIL(check_can_group_by(reader, begin_index, end_index, res, can_group_by))) {
    LOG_WARN("Failed to checkout pushdown group by", K(ret));
  } else if (can_group_by) {
    if (OB_FAIL(fill_group_by_rows(group_idx, reader, begin_index, end_index, res))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to fill group by rows", K(ret));
      }
    }
  } else if (OB_FAIL(fill_output_rows(group_idx, scanner, begin_index, end_index, res))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to fill output rows", K(ret));
    }
  } else if (nullptr != group_by_cell_) {
    if (iter_param_->use_uniform_format() &&
        OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx_, false))) {
      LOG_WARN("Failed to init uniform header", K(ret));
    } else if (OB_FAIL(group_by_cell_->copy_output_rows(count_))) {
      LOG_WARN("Failed to copy output rows", K(ret));
    }
  }
  return ret;
}

// shallow copy
int ObVectorStore::fill_output_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockRowScanner *scanner,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  int64_t row_capacity = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector store is not inited", K(ret));
  } else if (0 != count_) {
    // defense code: data cross fuse and micro block is banned
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected vector store count", K(ret), K(count_));
  } else if (OB_FAIL(get_row_ids(scanner->get_reader(), begin_index, end_index, row_capacity, true, res))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get row ids", K(ret), K(begin_index), K(end_index));
    }
  } else if (0 == row_capacity) {
    // skip if no rows selected
  } else if (iter_param_->use_new_format()) {
    if (OB_FAIL(scanner->get_rows_for_rich_format(cols_projector_,
                                                  col_params_,
                                                  row_ids_,
                                                  row_capacity,
                                                  0,
                                                  cell_data_ptrs_,
                                                  len_array_,
                                                  exprs_,
                                                  &default_row_))) {
      LOG_WARN("Failed to get rows for rich format", K(ret));
    }
  } else if (OB_FAIL(scanner->get_rows_for_old_format(cols_projector_,
                                                      col_params_,
                                                      row_ids_,
                                                      row_capacity,
                                                      0,
                                                      cell_data_ptrs_,
                                                      exprs_,
                                                      datum_infos_,
                                                      &default_row_))) {
    LOG_WARN("Failed to get rows for old format", K(ret));
  }
  if (OB_SUCC(ret)) {
    count_ = row_capacity;
    eval_ctx_.set_batch_idx(count_);
    // todo: support data cross microblocks in vectorized
    set_end();
    if (OB_FAIL(fill_group_idx(group_idx))) {
      LOG_WARN("Failed to fill group idx", K(ret));
    } else {
      if (OB_UNLIKELY(IterEndState::LIMIT_ITER_END == iter_end_flag_)) {
        ret = OB_ITER_END;
      }
      EVENT_ADD(ObStatEventIds::SSSTORE_READ_ROW_COUNT, row_capacity);
    }
  }
  LOG_TRACE("[Vectorized] vector store copy rows", K(ret),
            K(begin_index), K(end_index), K(row_capacity), K(res),
            "row_ids", common::ObArrayWrap<const int32_t>(row_ids_, row_capacity),
            KPC(this));
  return ret;
}

int ObVectorStore::fill_group_by_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  int64_t output_cnt = 0;
  bool already_init_uniform = false;
  if (!group_by_cell_->is_processing()) {
    if (iter_param_->use_uniform_format() &&
        OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx_))) {
      LOG_WARN("Failed to init uniform header", K(ret));
    } else if (OB_FAIL(do_group_by(group_idx, reader, begin_index, end_index, res))) {
      LOG_WARN("Failed to do group by", K(ret));
    } else if (!group_by_cell_->is_exceed_sql_batch()) {
      output_cnt = group_by_cell_->get_distinct_cnt();
    } else {
      group_by_cell_->reset_projected_cnt();
      group_by_cell_->set_is_processing(true);
      already_init_uniform = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (group_by_cell_->is_processing()) {
    if (!already_init_uniform &&
        iter_param_->use_uniform_format() &&
        OB_FAIL(group_by_cell_->init_uniform_header(iter_param_->output_exprs_, iter_param_->aggregate_exprs_, eval_ctx_))) {
      LOG_WARN("Failed to init uniform header", K(ret));
    } else if (OB_FAIL(group_by_cell_->output_extra_group_by_result(output_cnt))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        group_by_cell_->set_is_processing(false);
      } else {
        LOG_WARN("Failed to fill rows", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(group_by_cell_->pad_column_in_group_by(output_cnt))) {
    LOG_WARN("Failed to pad column in group by", K(ret), K(output_cnt));
  } else {
    count_ = output_cnt;
    eval_ctx_.set_batch_idx(count_);
    if (OB_FAIL(fill_group_idx(group_idx))) {
      LOG_WARN("Failed to fill group idx", K(ret));
    } else {
      EVENT_ADD(ObStatEventIds::SSSTORE_READ_ROW_COUNT, count_);
      set_end();
      if (!group_by_cell_->is_processing()) {
        begin_index = end_index;
      }
    }
  }
  return ret;
}

int ObVectorStore::check_can_group_by(
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    const ObFilterResult &res,
    bool &can_group_by)
{
  int ret = OB_SUCCESS;
  can_group_by = false;
  if (nullptr == group_by_cell_) {
  } else if (group_by_cell_->is_processing()) {
    can_group_by = true;
  } else {
    int64_t micro_row_count = 0;
    if (OB_FAIL(reader->get_row_count(micro_row_count))) {
      LOG_WARN("Failed to get micro row count", K(ret));
    } else {
      int64_t distinct_cnt = 0;
      const int64_t covered_row_count = end_index - begin_index;
      if (OB_FAIL(reader->get_distinct_count(group_by_cell_->get_group_by_col_offset(), distinct_cnt))) {
        if (OB_UNLIKELY(OB_NOT_SUPPORTED != ret)) {
          LOG_WARN("Failed to get distinct cnt", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(group_by_cell_->decide_use_group_by(
          micro_row_count, covered_row_count, distinct_cnt, res.bitmap_, can_group_by))) {
        LOG_WARN("Failed to decide use group by", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorStore::do_group_by(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockReader *reader,
    int64_t begin_index,
    const int64_t end_index,
    const ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  int64_t row_capacity = 0;
  blocksstable::ObIMicroBlockDecoder *decoder = static_cast<blocksstable::ObIMicroBlockDecoder*>(reader);
  const int32_t group_by_col_offset = group_by_cell_->get_group_by_col_offset();
  const char **cell_data = group_by_cell_->get_cell_datas();
  if (OB_FAIL(decoder->read_distinct(group_by_col_offset,
      nullptr == cell_data ? cell_data_ptrs_ : cell_data, *group_by_cell_))) {
    LOG_WARN("Failed to read distinct", K(ret));
  } else if (group_by_cell_->need_read_reference()) {
    const bool need_extract_distinct = group_by_cell_->need_extract_distinct();
    const bool need_do_aggregate = group_by_cell_->need_do_aggregate();
    if (need_extract_distinct) {
      group_by_cell_->set_distinct_cnt(0);
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_row_ids(reader, begin_index, end_index, row_capacity, false, res))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get row ids", K(ret), K(begin_index), K(end_index));
        }
      } else if (0 == row_capacity) {
      } else if (OB_FAIL(decoder->read_reference(group_by_col_offset, row_ids_, row_capacity, *group_by_cell_))) {
        LOG_WARN("Failed to read reference", K(ret));
      } else if (need_extract_distinct && OB_FAIL(group_by_cell_->extract_distinct())) {
        LOG_WARN("Failed to extract distinct", K(ret));
      } else if (need_do_aggregate) {
        if (OB_FAIL(group_by_cell_->check_distinct_and_ref_valid())) {
          LOG_WARN("Failed to check valid", K(ret));
        } else if (OB_FAIL(decoder->get_group_by_aggregate_result(row_ids_, cell_data_ptrs_, row_capacity, *group_by_cell_))) {
          LOG_WARN("Failed to get aggregate result", K(ret));
        }
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(row_capacity), KPC(group_by_cell_));
    }
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Unexpected ret, should be OB_ITER_END", K(ret));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(group_by_cell_->collect_result())) {
      LOG_WARN("Failed to collect result", K(ret));
    }
  }
  return ret;
}

int ObVectorStore::fill_rows(const int64_t group_idx, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector store is not inited", K(ret));
  } else if (0 != count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected vector store count", K(ret), K(count_));
  } else {
    count_ = row_count;
    eval_ctx_.set_batch_idx(count_);
    if (OB_FAIL(fill_group_idx(group_idx))) {
      LOG_WARN("Failed to fill group idx", K(ret));
    } else {
      set_end();
      EVENT_ADD(ObStatEventIds::SSSTORE_READ_ROW_COUNT, row_count);
    }
  }
  return ret;
}

int ObVectorStore::fill_group_idx(const int64_t group_idx)
{
  int ret = OB_SUCCESS;
  if (nullptr != group_idx_expr_) {
    if (iter_param_->op_->enable_rich_format_ && OB_FAIL(init_expr_vector_header(*group_idx_expr_, eval_ctx_, count_))) {
      LOG_WARN("Failed to init expr", K(ret));
    } else {
      ObDatum *group_idx_datums = group_idx_expr_->locate_batch_datums(eval_ctx_);
      for (int64_t i = 0; i < count_; ++i) {
        group_idx_datums[i].set_int(group_idx);
      }
    }
  }
  return ret;
}

int64_t ObVectorStore::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(exprs),
       K_(default_row),
       K_(cols_projector),
       K_(count),
       K_(batch_size),
       K_(row_capacity),
       K_(iter_end_flag),
       K_(eval_ctx));
  if (nullptr != iter_param_ && 0 < count_) {
    if (iter_param_->use_new_format()) {
      sql::EvalBound bound(count_, true);
      for (int64_t i = 0; i < datum_infos_.count(); i++) {
        J_KV(K(i));
        J_COLON();
        J_KV("new format datums",
             sql::ToStrVectorHeader(*datum_infos_.at(i).expr_,
                                    iter_param_->op_->get_eval_ctx(),
                                    nullptr,
                                    bound));
      }
    } else {
      for (int64_t i = 0; i < datum_infos_.count(); i++) {
        J_KV(K(i));
        J_COLON();
        J_KV("datums", (ObArrayWrap<ObDatum>(datum_infos_.at(i).datum_ptr_, count_)));
      }
    }
  }
  J_OBJ_END();
  return pos;
}

}
}
