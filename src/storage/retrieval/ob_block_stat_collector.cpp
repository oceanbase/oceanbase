/**
 * Copyright (c) 2025 OceanBase
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

#include "sql/engine/expr/ob_expr_bm25.h"
#include "ob_block_stat_collector.h"

namespace oceanbase
{
namespace storage
{

ObBM25MaxScoreParamCollector::ObBM25MaxScoreParamCollector()
: token_freq_data_idx_(-1),
  token_freq_agg_idx_(-1),
  doc_len_data_idx_(-1),
  doc_len_agg_idx_(-1),
  curr_max_score_(0),
  token_freq_res_(nullptr),
  doc_len_res_(nullptr),
  is_inited_(false)
{}

void ObBM25MaxScoreParamCollector::reset()
{
  token_freq_data_idx_ = -1;
  token_freq_agg_idx_ = -1;
  doc_len_data_idx_ = -1;
  doc_len_agg_idx_ = -1;
  curr_max_score_ = 0;
  token_freq_res_ = nullptr;
  doc_len_res_ = nullptr;
  is_inited_ = false;
}

void ObBM25MaxScoreParamCollector::reuse()
{
  if (nullptr != token_freq_res_) {
    token_freq_res_->reuse();
    token_freq_res_->set_null();
  }
  if (nullptr != doc_len_res_) {
    doc_len_res_->reuse();
    doc_len_res_->set_null();
  }
  curr_max_score_ = 0;
}

int ObBM25MaxScoreParamCollector::init(
  const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
  const ObIArray<uint32_t> &stat_projectors,
  const ObIArray<ObColDesc> &col_descs,
  blocksstable::ObDatumRow &result_row,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(stat_cols.count() != stat_projectors.count() || stat_cols.count() != result_row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stat_cols), K(stat_projectors), K(result_row));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < stat_cols.count(); ++i) {
    const blocksstable::ObSkipIndexColType col_type = stat_cols.at(i).get_col_type();
    if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ) {
      if (OB_UNLIKELY(token_freq_agg_idx_ != -1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected duplicate token freq column", K(ret), K(stat_cols));
      } else if (OB_UNLIKELY(!col_descs.at(i).col_type_.is_uint64())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected token freq column type", K(ret), K(i), K(stat_cols), K(col_descs));
      } else {
        token_freq_agg_idx_ = i;
        token_freq_data_idx_ = stat_projectors.at(i);
        token_freq_res_ = &result_row.storage_datums_[i];
      }
    } else if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN) {
      if (OB_UNLIKELY(doc_len_agg_idx_ != -1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected duplicate doc length column", K(ret), K(stat_cols));
      } else if (OB_UNLIKELY(!col_descs.at(i).col_type_.is_uint64())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected doc length column type", K(ret), K(i), K(stat_cols), K(col_descs));
      } else {
        doc_len_agg_idx_ = i;
        doc_len_data_idx_ = stat_projectors.at(i);
        doc_len_res_ = &result_row.storage_datums_[i];
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(doc_len_agg_idx_ == -1 || token_freq_agg_idx_ == -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected agg idxes", K(ret), K(doc_len_agg_idx_), K(token_freq_agg_idx_));
  } else {
    token_freq_res_->reuse();
    token_freq_res_->set_null();
    doc_len_res_->reuse();
    doc_len_res_->set_null();
    is_inited_ = true;
  }
  return ret;
}

int ObBM25MaxScoreParamCollector::collect_data_row(const blocksstable::ObDatumRow &data_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(token_freq_data_idx_ < 0
      || doc_len_data_idx_ < 0
      || token_freq_data_idx_ >= data_row.get_column_count()
      || doc_len_data_idx_ >= data_row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data idxes", K(ret), K(token_freq_data_idx_), K(doc_len_data_idx_), K(data_row.get_column_count()));
  } else {
    const uint64_t token_freq = data_row.storage_datums_[token_freq_data_idx_].get_uint();
    const uint64_t doc_len = data_row.storage_datums_[doc_len_data_idx_].get_uint();
    const double norm_len = static_cast<double>(doc_len);
    const double tf_score = sql::ObExprBM25::doc_token_weight(token_freq, norm_len);
    if (tf_score > curr_max_score_) {
      token_freq_res_->set_uint(token_freq);
      doc_len_res_->set_uint(doc_len);
      curr_max_score_ = tf_score;
    }
  }
  return ret;
}

int ObBM25MaxScoreParamCollector::collect_agg_row(const blocksstable::ObDatumRow &agg_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(token_freq_agg_idx_ >= agg_row.get_column_count()
      || doc_len_agg_idx_ >= agg_row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected agg idxes", K(ret), K(token_freq_agg_idx_), K(doc_len_agg_idx_), K(agg_row.get_column_count()));
  } else {
    const uint64_t token_freq = agg_row.storage_datums_[token_freq_agg_idx_].get_uint();
    const uint64_t doc_len = agg_row.storage_datums_[doc_len_agg_idx_].get_uint();
    const double norm_len = static_cast<double>(doc_len);
    const double tf_score = sql::ObExprBM25::doc_token_weight(token_freq, norm_len);
    if (tf_score > curr_max_score_) {
      token_freq_res_->set_uint(token_freq);
      doc_len_res_->set_uint(doc_len);
      curr_max_score_ = tf_score;
    }
  }
  return ret;
}

ObBlockStatCollector::ObBlockStatCollector()
: loose_min_collectors_(),
  min_agg_row_projs_(),
  min_data_row_projs_(),
  loose_max_collectors_(),
  max_agg_row_projs_(),
  max_data_row_projs_(),
  bm25_collector_(),
  result_row_(),
  result_tmp_allocator_(),
  projector_(nullptr),
  has_bm25_(false),
  is_inited_(false)
{}

void ObBlockStatCollector::reset()
{
  loose_min_collectors_.reset();
  min_agg_row_projs_.reset();
  min_data_row_projs_.reset();
  loose_max_collectors_.reset();
  max_agg_row_projs_.reset();
  max_data_row_projs_.reset();
  bm25_collector_.reset();
  result_row_.reset();
  result_tmp_allocator_.reset();
  projector_ = nullptr;
  has_bm25_ = false;
  is_inited_ = false;
}

void ObBlockStatCollector::reuse()
{
  for (int64_t i = 0; i < loose_min_collectors_.count(); ++i) {
    loose_min_collectors_.at(i).reuse();
  }
  for (int64_t i = 0; i < loose_max_collectors_.count(); ++i) {
    loose_max_collectors_.at(i).reuse();
  }
  bm25_collector_.reuse();
  result_tmp_allocator_.reuse();
}

int ObBlockStatCollector::init(
  const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
  const ObIArray<uint32_t> &stat_projectors,
  const ObIArray<ObColDesc> &col_descs,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_FAIL(result_row_.init(stat_cols.count()))) {
    LOG_WARN("failed to init result row", K(ret), K(stat_cols));
  } else if (OB_FAIL(init_collectors(stat_cols, stat_projectors, col_descs, allocator))) {
    LOG_WARN("failed to init collector", K(ret), K(stat_cols));
  } else {
    result_tmp_allocator_.set_attr(ObMemAttr(MTL_ID(), "BlkStatTmpRes"));
    projector_ = &stat_cols;
    is_inited_ = true;
  }
  return ret;
}

int ObBlockStatCollector::collect_data_row(const blocksstable::ObDatumRow &data_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(collect_row(data_row, min_data_row_projs_, loose_min_collectors_))) {
    LOG_WARN("failed to collect min stats", K(ret), K(data_row));
  } else if (OB_FAIL(collect_row(data_row, max_data_row_projs_, loose_max_collectors_))) {
    LOG_WARN("failed to collect max stats", K(ret), K(data_row));
  } else if (has_bm25_ && OB_FAIL(bm25_collector_.collect_data_row(data_row))) {
    LOG_WARN("failed to collect bm25 stats", K(ret), K(data_row));
  }
  return ret;
}

int ObBlockStatCollector::collect_agg_row(const blocksstable::ObDatumRow &agg_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(collect_row(agg_row, min_agg_row_projs_, loose_min_collectors_))) {
    LOG_WARN("failed to collect min stats", K(ret), K(agg_row));
  } else if (OB_FAIL(collect_row(agg_row, max_agg_row_projs_, loose_max_collectors_))) {
    LOG_WARN("failed to collect max stats", K(ret), K(agg_row));
  } else if (has_bm25_ && OB_FAIL(bm25_collector_.collect_agg_row(agg_row))) {
    LOG_WARN("failed to collect bm25 stats", K(ret), K(agg_row));
  }
  return ret;
}

int ObBlockStatCollector::get_result_row(const blocksstable::ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    result_row = &result_row_;
  }
  return ret;
}


#define INIT_FIXED_ARRAY(array, count, allocator) \
if (OB_SUCC(ret)) { \
  array.set_allocator(&allocator); \
  if (OB_FAIL(array.init(count))) { \
    LOG_WARN("failed to init array", K(ret), K(count)); \
  } else if (OB_FAIL(array.prepare_allocate(count))) { \
    LOG_WARN("failed to prepare allocate array", K(ret), K(count)); \
  }\
}


int ObBlockStatCollector::init_collectors(
  const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
  const ObIArray<uint32_t> &stat_projectors,
  const ObIArray<ObColDesc> &col_descs,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  loose_min_collectors_.set_allocator(&allocator);
  loose_max_collectors_.set_allocator(&allocator);
  int64_t min_count = 0, max_count = 0;
  bool has_bm25_token_freq_param = false;
  bool has_bm25_doc_len_param = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_cols.count(); ++i) {
    const blocksstable::ObSkipIndexColType col_type = stat_cols.at(i).get_col_type();
    if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_MIN) {
      ++min_count;
    } else if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_MAX) {
      ++max_count;
    } else if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ) {
      has_bm25_token_freq_param = true;
    } else if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN) {
      has_bm25_doc_len_param = true;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported col stat type", K(ret), K(col_type), K(i), K(stat_cols));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(has_bm25_token_freq_param != has_bm25_doc_len_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bm25 max score param not supported", K(ret), K(stat_cols));
  } else if (!has_bm25_token_freq_param || !has_bm25_doc_len_param) {
    has_bm25_ = false;
  } else if (OB_FAIL(bm25_collector_.init(stat_cols, stat_projectors, col_descs, result_row_, allocator))) {
    LOG_WARN("failed to init bm25 collector", K(ret), K(stat_cols));
  } else {
    has_bm25_ = true;
  }

  INIT_FIXED_ARRAY(loose_min_collectors_, min_count, allocator);
  INIT_FIXED_ARRAY(min_agg_row_projs_, min_count, allocator);
  INIT_FIXED_ARRAY(min_data_row_projs_, min_count, allocator);
  INIT_FIXED_ARRAY(loose_max_collectors_, max_count, allocator);
  INIT_FIXED_ARRAY(max_agg_row_projs_, max_count, allocator);
  INIT_FIXED_ARRAY(max_data_row_projs_, max_count, allocator);

  int64_t min_idx = 0, max_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_cols.count(); ++i) {
    const blocksstable::ObSkipIndexColType col_type = stat_cols.at(i).get_col_type();
    const int64_t stat_col_proj = stat_projectors.at(i);
    const ObColDesc &col_desc = col_descs.at(stat_col_proj);
    blocksstable::ObStorageDatum &result_datum = result_row_.storage_datums_[i];
    if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_MIN) {
      ObLooseMinStatCollector &collector = loose_min_collectors_.at(min_idx);
      if (OB_FAIL(collector.init(col_desc, result_datum, result_tmp_allocator_))) {
        LOG_WARN("failed to init loose_min_collector", K(ret));
      } else {
        min_agg_row_projs_.at(min_idx) = i;
        min_data_row_projs_.at(min_idx) = stat_col_proj;
        ++min_idx;
      }
    } else if (col_type == blocksstable::ObSkipIndexColType::SK_IDX_MAX) {
      ObLooseMaxStatCollector &collector = loose_max_collectors_.at(max_idx);
      if (OB_FAIL(collector.init(col_desc, result_datum, result_tmp_allocator_))) {
        LOG_WARN("failed to init loose_max_collector", K(ret));
      } else {
        max_agg_row_projs_.at(max_idx) = i;
        max_data_row_projs_.at(max_idx) = stat_col_proj;
        ++max_idx;
      }
    }
  }
  return ret;
}

#undef INIT_FIXED_ARRAY


} // namespace storage
} // namespace oceanbase
