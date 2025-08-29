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

#include "ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{

ObBlockMaxScoreIterParam::ObBlockMaxScoreIterParam()
  : stat_cols_(),
    stat_projectors_(),
    min_domain_id_col_idx_(0),
    max_domain_id_col_idx_(0),
    token_freq_col_idx_(0),
    doc_length_col_idx_(0),
    score_col_idx_(0),
    domain_id_idx_in_rowkey_(0),
    dim_col_idx_in_rowkey_(0),
    domain_id_obj_meta_(),
    dim_obj_meta_(),
    scan_allocator_(nullptr),
    ranking_type_(OB_MAX_MAX_SCORE_RANKING_TYPE)
{
}

int ObBlockMaxScoreIterParam::init(const ObDASIRScanCtDef &ir_ctdef, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  stat_cols_.set_allocator(&alloc);
  stat_projectors_.set_allocator(&alloc);
  const ObTextBlockMaxSpec &block_max_spec = ir_ctdef.block_max_spec_;
  if (OB_UNLIKELY(!ir_ctdef.is_block_scan_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ir_ctdef));
  } else if (OB_FAIL(stat_cols_.init(block_max_spec.col_types_.count()))) {
    LOG_WARN("failed to init stat cols", K(ret));
  } else if (OB_FAIL(stat_projectors_.init(block_max_spec.col_types_.count()))) {
    LOG_WARN("failed to init stat projectors", K(ret));
  } else {
    // some column index hard coded here, generate it on code generation if inverted index schema may change
    min_domain_id_col_idx_ = block_max_spec.min_id_idx_;
    max_domain_id_col_idx_ = block_max_spec.max_id_idx_;
    token_freq_col_idx_ = block_max_spec.token_freq_idx_;
    doc_length_col_idx_ = block_max_spec.doc_length_idx_;
    dim_col_idx_in_rowkey_ = 0;
    domain_id_idx_in_rowkey_ = 1;
    domain_id_obj_meta_ = ir_ctdef.inv_scan_domain_id_col_->obj_meta_;
    dim_obj_meta_ = ir_ctdef.token_col_->obj_meta_;
    scan_allocator_ = &alloc;
    ranking_type_ = ObMaxScoreRankingType::OB_MAX_SCORE_BM_25;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_max_spec.col_types_.count(); ++i) {
      ObSkipIndexColMeta col_meta(block_max_spec.col_store_idxes_.at(i), block_max_spec.col_types_.at(i));
      if (OB_FAIL(stat_cols_.push_back(col_meta))) {
        LOG_WARN("failed to push back stat col", K(ret));
      } else if (OB_FAIL(stat_projectors_.push_back(block_max_spec.scan_col_proj_.at(i)))) {
        LOG_WARN("failed to push back stat projector", K(ret));
      }
    }
  }
  return ret;
}

int ObBlockMaxScoreIterParam::init(const ObDASVecAuxScanCtDef &vec_aux_ctdef, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  stat_cols_.set_allocator(&alloc);
  stat_projectors_.set_allocator(&alloc);
  const ObSPIVBlockMaxSpec &block_max_spec = vec_aux_ctdef.block_max_spec_;
  if (OB_FAIL(stat_cols_.init(block_max_spec.col_types_.count()))) {
    LOG_WARN("failed to init stat cols", K(ret));
  } else if (OB_FAIL(stat_projectors_.init(block_max_spec.col_types_.count()))) {
    LOG_WARN("failed to init stat projectors", K(ret));
  } else {
    min_domain_id_col_idx_ = block_max_spec.min_id_idx_;
    max_domain_id_col_idx_ = block_max_spec.max_id_idx_;
    score_col_idx_ = block_max_spec.value_idx_;
    dim_col_idx_in_rowkey_ = 0;
    domain_id_idx_in_rowkey_ = 1;
    domain_id_obj_meta_ = vec_aux_ctdef.spiv_scan_docid_col_->obj_meta_;
    dim_obj_meta_.set_uint32();
    scan_allocator_ = &alloc;
    ranking_type_ = ObMaxScoreRankingType::OB_MAX_SCORE_INNER_PRODUCT;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_max_spec.col_types_.count(); ++i) {
      ObSkipIndexColMeta col_meta(block_max_spec.col_store_idxes_.at(i), block_max_spec.col_types_.at(i));
      if (OB_FAIL(stat_cols_.push_back(col_meta))) {
        LOG_WARN("failed to push back stat col", K(ret));
      } else if (OB_FAIL(stat_projectors_.push_back(block_max_spec.scan_col_proj_.at(i)))) {
        LOG_WARN("failed to push back stat projector", K(ret));
      }
    }
  }
  return ret;
}

void ObBlockMaxScoreIterParam::reset()
{
  stat_cols_.reset();
  stat_projectors_.reset();
  min_domain_id_col_idx_ = 0;
  max_domain_id_col_idx_ = 0;
  token_freq_col_idx_ = 0;
  doc_length_col_idx_ = 0;
  domain_id_idx_in_rowkey_ = 0;
  domain_id_obj_meta_.reset();
  dim_obj_meta_.reset();
  scan_allocator_ = nullptr;
  ranking_type_ = OB_MAX_MAX_SCORE_RANKING_TYPE;
}

bool ObBlockMaxScoreIterParam::is_valid() const
{
  const bool stat_valid = stat_cols_.count() == stat_projectors_.count();
  const bool idx_valid = min_domain_id_col_idx_ >= 0 && max_domain_id_col_idx_ >= 0 && domain_id_idx_in_rowkey_ >= 0;
  return stat_valid && idx_valid && domain_id_obj_meta_.is_valid();
}

ObMaxScoreTuple::ObMaxScoreTuple()
  : max_score_(0),
    min_domain_id_(nullptr),
    max_domain_id_(nullptr)
{
}

void ObMaxScoreTuple::reset()
{
  max_score_ = 0;
  min_domain_id_ = nullptr;
  max_domain_id_ = nullptr;
}

ObBlockMaxScoreIterator::ObBlockMaxScoreIterator()
  : max_score_tuple_(),
    stat_iter_(),
    advance_rowkey_(),
    advance_doc_id_(),
    block_max_scan_param_(nullptr),
    block_stat_scan_param_(),
    domain_id_cmp_(),
    dim_cmp_(nullptr),
    scorer_buf_(),
    scorer_(nullptr),
    calc_max_score_(nullptr),
    scan_dim_datum_(),
    has_been_advanced_(false),
    is_inited_(false)
{
}

void ObBlockMaxScoreIterator::reset()
{
  max_score_tuple_.reset();
  stat_iter_.reset();
  advance_rowkey_.reset();
  advance_doc_id_.reset();
  block_max_scan_param_ = nullptr;
  block_stat_scan_param_.reset();
  domain_id_cmp_.reset();
  dim_cmp_ = nullptr;
  if (nullptr != scorer_) {
    scorer_->~ObIBlockMaxScoreCalc();
    scorer_ = nullptr;
  }
  calc_max_score_ = nullptr;
  scan_dim_datum_.reuse();
  has_been_advanced_ = false;
  is_inited_ = false;
}

void ObBlockMaxScoreIterator::reuse()
{
  reset();
  // TODO: reuse and rescan
}

int ObBlockMaxScoreIterator::get_next(const ObMaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *agg_row = nullptr;
  const ObDatumRowkey *endkey = nullptr;
  max_score_tuple_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(stat_iter_.get_next(agg_row, endkey))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next", K(ret));
    }
  } else if (OB_ISNULL(agg_row) || OB_ISNULL(endkey)
      || OB_UNLIKELY(agg_row->get_column_count() != block_max_scan_param_->stat_cols_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(agg_row), KP(endkey), KPC_(block_max_scan_param));
  } else if (OB_FAIL(calc_domain_id_range(*agg_row, *endkey))) {
    LOG_WARN("fail to calc doc id range", K(ret));
  } else if (OB_FAIL(calc_max_score_(*agg_row, *scorer_, max_score_tuple_.max_score_))) {
    LOG_WARN("fail to calc max score", K(ret));
  } else {
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObBlockMaxScoreIterator::get_curr_max_score_tuple(const ObMaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!max_score_tuple_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid max score tuple", K(ret));
  } else {
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObBlockMaxScoreIterator::advance_to(const ObDatum &domain_id, const bool inclusive)
{
  int ret = OB_SUCCESS;

  ObStorageDatum &rowkey_domain_id_datum = advance_rowkey_.datums_[block_max_scan_param_->domain_id_idx_in_rowkey_];
  bool id_in_range = false;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(advance_doc_id_.from_datum(domain_id))) {
    LOG_WARN("fail to deep copy domain id", K(ret), K(domain_id));
  } else if (!max_score_tuple_.is_valid()) {
    id_in_range = false;
  } else if (OB_FAIL(domain_id_cmp_.compare(domain_id, *max_score_tuple_.max_domain_id_, cmp_ret))) {
    LOG_WARN("fail to compare domain id", K(ret), K(domain_id), K_(advance_doc_id));
  } else if (cmp_ret > 0 || (cmp_ret == 0 && !inclusive)) {
    id_in_range = false;
  } else {
    id_in_range = true;
  }

  const ObMaxScoreTuple *next_max_score_tuple = nullptr;
  if (OB_FAIL(ret)) {
  } else if (id_in_range) {
    max_score_tuple_.min_domain_id_ = &advance_doc_id_.get_datum();
    has_been_advanced_ = true;
  } else if (FALSE_IT(rowkey_domain_id_datum.shallow_copy_from_datum(advance_doc_id_.get_datum()))) {
  } else if (OB_FAIL(stat_iter_.advance_to(advance_rowkey_, inclusive))) {
    LOG_WARN("fail to advance to", K(ret), K_(advance_rowkey), K(inclusive));
  } else if (OB_FAIL(get_next(next_max_score_tuple))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next", K(ret));
    }
  } else {
    has_been_advanced_ = true;
  }


  return ret;
}

int ObBlockMaxScoreIterator::inner_init(
    const ObBlockMaxScoreIterParam &block_max_scan_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!block_max_scan_param.is_valid() || !scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_max_scan_param), K(scan_param));
  } else if (OB_FAIL(init_cmp_funcs(block_max_scan_param))) {
    LOG_WARN("fail to init domain id cmp", K(ret));
  } else if (OB_FAIL(init_advance_rowkey(block_max_scan_param, scan_param))) {
    LOG_WARN("fail to init advance rowkey", K(ret));
  } else if (OB_FAIL(block_stat_scan_param_.init(
      block_max_scan_param.stat_cols_,
      block_max_scan_param.stat_projectors_,
      scan_param))) {
    LOG_WARN("fail to init block stat scan param", K(ret));
  } else if (OB_FAIL(MTL(ObAccessService *)->scan_block_stat(block_stat_scan_param_, stat_iter_))) {
    LOG_WARN("fail to scan block stat", K(ret));
  } else {
    const int64_t dim_rowkey_idx = block_max_scan_param.dim_col_idx_in_rowkey_;
    const ObObj &scan_dim_obj = block_stat_scan_param_.get_scan_param()->key_ranges_.at(0).end_key_.get_obj_ptr()[dim_rowkey_idx];
    scan_dim_datum_.reuse();
    if (OB_FAIL(scan_dim_datum_.from_obj(scan_dim_obj))) {
      LOG_WARN("fail to convert to datum", K(ret), K(scan_dim_obj));
    } else {
      switch (block_max_scan_param.ranking_type_) {
      case OB_MAX_SCORE_BM_25:
        calc_max_score_ = calc_max_score_bm25;
        break;
      case OB_MAX_SCORE_INNER_PRODUCT:
        calc_max_score_ = calc_max_score_inner_product;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectedranking type", K(ret), K(block_max_scan_param.ranking_type_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    block_max_scan_param_ = &block_max_scan_param;
  }
  return ret;
}

int ObBlockMaxScoreIterator::init_advance_rowkey(
    const ObBlockMaxScoreIterParam &iter_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_param.scan_allocator_)
    || OB_UNLIKELY(1 != scan_param.key_ranges_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(iter_param), K(scan_param));
  } else {
    const ObRowkey &start_key = scan_param.key_ranges_.at(0).start_key_;
    if (OB_FAIL(advance_rowkey_.from_rowkey(start_key, *iter_param.scan_allocator_))) {
      LOG_WARN("failed to convert to advance rowkey", K(ret));
    }
  }
  return ret;
}

int ObBlockMaxScoreIterator::init_cmp_funcs(const ObBlockMaxScoreIterParam &block_max_scan_param)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &id_obj_meta = block_max_scan_param.domain_id_obj_meta_;
  const ObObjMeta &dim_obj_meta = block_max_scan_param.dim_obj_meta_;
  sql::ObExprBasicFuncs *dim_basic_funcs = ObDatumFuncs::get_basic_func(
      dim_obj_meta.get_type(), dim_obj_meta.get_collation_type());
  if (OB_FAIL(domain_id_cmp_.init(id_obj_meta))) {
    LOG_WARN("fail to init domain id cmp", K(ret));
  } else if (OB_ISNULL(dim_basic_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(id_obj_meta), K(dim_obj_meta));
  } else {
    dim_cmp_ = dim_basic_funcs->null_first_cmp_;
  }

  return ret;
}

int ObBlockMaxScoreIterator::calc_domain_id_range(const ObDatumRow &agg_row, const ObDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  const int64_t min_idx = block_max_scan_param_->min_domain_id_col_idx_;
  const int64_t max_idx = block_max_scan_param_->max_domain_id_col_idx_;
  const int64_t id_rowkey_idx = block_max_scan_param_->domain_id_idx_in_rowkey_;
  const int64_t dim_rowkey_idx = block_max_scan_param_->dim_col_idx_in_rowkey_;
  int cmp_ret = 0;
  if (OB_UNLIKELY(min_idx >= agg_row.get_column_count()
      || max_idx >= agg_row.get_column_count()
      || id_rowkey_idx >= endkey.get_datum_cnt()
      || dim_rowkey_idx >= endkey.get_datum_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_idx), K(max_idx), K(id_rowkey_idx),
        K(dim_rowkey_idx), K(agg_row), K(endkey));
  } else {
    const ObDatum &min_datum = agg_row.storage_datums_[min_idx];
    const ObDatum &max_datum = agg_row.storage_datums_[max_idx];
    const ObDatum &rowkey_id_datum = endkey.get_datum(id_rowkey_idx);
    const ObDatum &rowkey_dim_datum = endkey.get_datum(dim_rowkey_idx);
    if (OB_UNLIKELY(min_datum.is_null() || max_datum.is_null() || rowkey_id_datum.is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null domain id datum", K(ret), K(min_datum), K(max_datum), K(rowkey_id_datum));
    } else {
      // Min datum in last iterated range might not aligned with the upper bound of max score.
      // but since we scan doc id in ascending order, we can use current iter doc id datum to refine the min doc id semantic.

      if (!has_been_advanced_) {
        max_score_tuple_.min_domain_id_ = &min_datum;
      } else if (OB_FAIL(domain_id_cmp_.compare(min_datum, advance_doc_id_.get_datum(), cmp_ret))) {
        LOG_WARN("fail to compare domain id", K(ret), K(agg_row), K_(advance_doc_id));
      } else {
        max_score_tuple_.min_domain_id_ = cmp_ret >= 0 ? &min_datum : &advance_doc_id_.get_datum();
      }

      if (OB_FAIL(ret)) {
      } else {
        // Since micro block is not divided by dimension boundary, max_datum in first statistic range
        // might comes from other dimension and not aligned with the data range covered by upper bound of max score.
        const ObObj &scan_dim_obj = block_stat_scan_param_.get_scan_param()->key_ranges_.at(0).end_key_.get_obj_ptr()[dim_rowkey_idx];
        ObStorageDatum scan_dim_datum;
        if (OB_FAIL(scan_dim_datum.from_obj(scan_dim_obj))) {
          LOG_WARN("fail to convert to datum", K(ret), K(scan_dim_obj));
        } else if (OB_UNLIKELY(rowkey_dim_datum.is_ext() || scan_dim_datum.is_ext())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ext datum for dimension", K(ret), K(rowkey_dim_datum), K(scan_dim_datum));
        } else if (OB_FAIL(dim_cmp_(rowkey_dim_datum, scan_dim_datum, cmp_ret))) {
          LOG_WARN("fail to compare dim", K(ret), K(scan_dim_obj), K(rowkey_dim_datum));
        } else if (OB_UNLIKELY(cmp_ret < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected endkey dimension smaller than scan dimension",
              K(ret), K(cmp_ret), K(rowkey_dim_datum), K(scan_dim_datum));
        } else {
          if (cmp_ret > 0) {
            // reached the end of the dimension
            max_score_tuple_.max_domain_id_ = &max_datum;
          } else {
            // use id from iterated end key as a safe boundary
            max_score_tuple_.max_domain_id_ = &rowkey_id_datum;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase