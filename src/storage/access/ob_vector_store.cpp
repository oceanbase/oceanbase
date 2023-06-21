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
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"

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
    datums_(*context_.stmt_allocator_),
    col_params_(*context_.stmt_allocator_),
    map_types_(*context_.stmt_allocator_),
    group_idx_expr_(nullptr),
    default_row_()
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
  datums_.reset();
  col_params_.reset();
  map_types_.reset();
  group_idx_expr_ = nullptr;
  default_row_.reset();
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
    if (OB_FAIL(exprs_.init(expr_count))) {
      LOG_WARN("Failed to init exprs", K(ret));
    } else if (OB_FAIL(default_row_.init(*context_.stmt_allocator_, out_cols_param->count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(out_cols_param->count()));
    } else if (OB_FAIL(cols_projector_.init(expr_count))) {
      LOG_WARN("Failed to init cols", K(ret));
    } else if (OB_FAIL(datums_.init(expr_count))) {
      LOG_WARN("Failed to init datums", K(ret));
    } else if (OB_FAIL(col_params_.init(expr_count))) {
      LOG_WARN("Fail to init col params", K(ret));
    } else if (OB_FAIL(map_types_.init(expr_count))) {
      LOG_WARN("Fail to init map types", K(ret));
    } else if (OB_FAIL(row_buf_.init(*context_.stmt_allocator_, param.iter_param_.get_max_out_col_cnt()))) {
      LOG_WARN("Fail to init datum ro types", K(ret));
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
          STORAGE_LOG(WARN, "Unexpected null datums", K(ret), K(i), KPC(expr));
        } else if (OB_UNLIKELY(!(expr->is_variable_res_buf())
                   && datums->ptr_ != eval_ctx_.frames_[expr->frame_idx_] + expr->res_buf_off_)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "Unexpected sql expr datum buffer", K(ret), KP(datums->ptr_), K(eval_ctx_), KPC(expr));
        } else if (OB_FAIL(datums_.push_back(datums))) {
          LOG_WARN("fail to push back datum", K(ret), K(datums));
        } else if (out_cols_projector.at(i) >= out_cols_param->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column id", K(ret), K(out_cols_projector.at(i)), K(out_cols_param->count()));
        } else {
          col_param = nullptr;
          if (is_pad_char_to_full_length(context_.sql_mode_) &&
              out_cols_param->at(out_cols_projector.at(i))->get_meta_type().is_fixed_len_char_type()) {
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
          } else if (OB_FAIL(map_types_.push_back(param.output_exprs_->at(i)->obj_datum_map_))) {
            LOG_WARN("failed to push back map type", K(ret));
          }
        }
      }
    }
    default_row_.count_ = col_params_.count();
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObVectorStore::reuse_capacity(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBlockBatchedRowStore::reuse_capacity(capacity))) {
    LOG_WARN("Fail to reuse capacity", K(ret), K(capacity));
  } else {
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
  }
  return ret;
}

// shallow copy
int ObVectorStore::fill_rows(
    const int64_t group_idx,
    blocksstable::ObIMicroBlockReader *reader,
    int64_t &begin_index,
    const int64_t end_index,
    const common::ObBitmap *bitmap)
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
  } else if (OB_FAIL(get_row_ids(reader, begin_index, end_index, row_capacity, true, bitmap))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get row ids", K(ret), K(begin_index), K(end_index));
    }
  } else if (0 == row_capacity) {
    // skip if no rows selected
  } else if (blocksstable::ObIMicroBlockReader::Decoder == reader->get_type()) {
    blocksstable::ObMicroBlockDecoder *block_decoder = static_cast<blocksstable::ObMicroBlockDecoder*>(reader);
    if (OB_FAIL(block_decoder->get_rows(cols_projector_, col_params_, row_ids_, cell_data_ptrs_, row_capacity, datums_))) {
      LOG_WARN("fail to copy rows", K(ret), K(cols_projector_), K(row_capacity),
              "row_ids", common::ObArrayWrap<const int64_t>(row_ids_, row_capacity));
    }
  } else {
    blocksstable::ObMicroBlockReader *block_reader = static_cast<blocksstable::ObMicroBlockReader*>(reader);
    if (OB_FAIL(block_reader->get_rows(cols_projector_, col_params_, map_types_, default_row_,
                                       row_ids_, row_capacity, row_buf_, datums_, exprs_, eval_ctx_))) {
      LOG_WARN("fail to copy rows", K(ret), K(cols_projector_), K(row_capacity),
               "row_ids", common::ObArrayWrap<const int64_t>(row_ids_, row_capacity));
    }
  }
  if (OB_SUCC(ret)) {
    count_ = row_capacity;
    eval_ctx_.set_batch_idx(count_);
    // todo: support data cross microblocks in vectorized
    set_end();
    fill_group_idx(group_idx);
    if (OB_UNLIKELY(IterEndState::LIMIT_ITER_END == iter_end_flag_)) {
      ret = OB_ITER_END;
    }
    EVENT_ADD(ObStatEventIds::SSSTORE_READ_ROW_COUNT, row_capacity);
  }
  LOG_TRACE("[Vectorized] vector store copy rows", K(ret),
            K(begin_index), K(end_index), K(row_capacity), KP(bitmap),
            "row_ids", common::ObArrayWrap<const int64_t>(row_ids_, row_capacity),
            KPC(this));
  return ret;
}

void ObVectorStore::fill_group_idx(const int64_t group_idx)
{
  if (nullptr != group_idx_expr_) {
    ObDatum *group_idx_datums = group_idx_expr_->locate_batch_datums(eval_ctx_);
    for (int64_t i = 0; i < count_; ++i) {
      group_idx_datums[i].set_int(group_idx);
    }
  }
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
  for (int64_t i = 0; i < datums_.count(); i++) {
    J_KV(K(i));
    J_COLON();
    J_KV(K(ObArrayWrap<ObDatum>(datums_.at(i), count_)));
  }
  J_OBJ_END();
  return pos;
}

}
}
