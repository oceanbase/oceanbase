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

#ifndef OB_BLOCK_MAX_ITER_H_
#define OB_BLOCK_MAX_ITER_H_

#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "ob_block_stat_iter.h"
#include "ob_sparse_retrieval_util.h"

namespace oceanbase
{
namespace storage
{

enum ObMaxScoreRankingType : uint8_t
{
  OB_MAX_SCORE_BM_25 = 0,
  OB_MAX_SCORE_INNER_PRODUCT,
  OB_MAX_MAX_SCORE_RANKING_TYPE,
};

struct ObBlockMaxBM25RankingParam
{
  ObBlockMaxBM25RankingParam()
    : doc_freq_(0), total_doc_cnt_(0), avg_doc_token_cnt_(0.0), token_freq_col_idx_(0), doc_length_col_idx_(0) {}
  ~ObBlockMaxBM25RankingParam() = default;
  int64_t doc_freq_;
  int64_t total_doc_cnt_;
  double avg_doc_token_cnt_;
  int64_t token_freq_col_idx_;
  int64_t doc_length_col_idx_;
  TO_STRING_KV(K_(doc_freq), K_(total_doc_cnt), K_(avg_doc_token_cnt), K_(token_freq_col_idx), K_(doc_length_col_idx));
};

struct ObBlockMaxIPRankingParam
{
  int64_t score_col_idx_;
  double query_value_;
  TO_STRING_KV(K_(score_col_idx));
};

struct ObBlockMaxScoreIterParam
{
  ObBlockMaxScoreIterParam();
  ~ObBlockMaxScoreIterParam() = default;

  int init(const ObDASIRScanCtDef &ir_ctdef, ObIAllocator &alloc);
  int init(const ObDASVecAuxScanCtDef &vec_aux_ctdef, ObIAllocator &alloc);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(stat_cols), K_(stat_projectors), K_(min_domain_id_col_idx),
      K_(max_domain_id_col_idx), K_(domain_id_idx_in_rowkey), K_(dim_col_idx_in_rowkey),
      K_(domain_id_obj_meta), K_(dim_obj_meta), K_(ranking_type));
  ObFixedArray<ObSkipIndexColMeta, ObIAllocator> stat_cols_;
  ObFixedArray<uint32_t, ObIAllocator> stat_projectors_;
  int64_t min_domain_id_col_idx_;
  int64_t max_domain_id_col_idx_;
  int64_t token_freq_col_idx_;
  int64_t doc_length_col_idx_;
  int64_t score_col_idx_;
  int64_t domain_id_idx_in_rowkey_;
  int64_t dim_col_idx_in_rowkey_;
  ObObjMeta domain_id_obj_meta_;
  ObObjMeta dim_obj_meta_;
  ObIAllocator *scan_allocator_; // should reuse on reuse / rescan
  ObMaxScoreRankingType ranking_type_;
};

class ObMaxScoreTuple
{
public:
  ObMaxScoreTuple();
  ~ObMaxScoreTuple() = default;
  void reset();
  bool is_valid() const { return nullptr != min_domain_id_ && nullptr != max_domain_id_; }
  double max_score_;
  const ObDatum *min_domain_id_;
  const ObDatum *max_domain_id_;
};

struct ObIBlockMaxScoreCalc
{
public:
  ObIBlockMaxScoreCalc() = default;
  virtual ~ObIBlockMaxScoreCalc() = default;
  virtual int calc_max_score(const ObDatumRow &agg_row, double &max_score) = 0;
};

template<typename RankingParam>
struct ObBlockMaxScoreCalc final : public ObIBlockMaxScoreCalc
{
public:
  ObBlockMaxScoreCalc(const RankingParam &ranking_param)
  : ObIBlockMaxScoreCalc(),
    ranking_param_(ranking_param)
  {}
  virtual ~ObBlockMaxScoreCalc() = default;
  inline int calc_max_score(const ObDatumRow &agg_row, double &max_score) override;
private:
  const RankingParam &ranking_param_;
};

template<>
inline int ObBlockMaxScoreCalc<ObBlockMaxBM25RankingParam>::calc_max_score(const ObDatumRow &agg_row, double &max_score)
{
  int ret = OB_SUCCESS;
  const ObDatum &max_token_freq_datum = agg_row.storage_datums_[ranking_param_.token_freq_col_idx_];
  const ObDatum &min_doc_length_datum = agg_row.storage_datums_[ranking_param_.doc_length_col_idx_];
  if (OB_UNLIKELY(max_token_freq_datum.is_null() || min_doc_length_datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null value", K(ret), K(max_token_freq_datum), K(min_doc_length_datum));
  } else {
    max_score = sql::ObExprBM25::eval(
        max_token_freq_datum.get_int(),
        min_doc_length_datum.get_int(),
        ranking_param_.doc_freq_,
        ranking_param_.total_doc_cnt_,
        ranking_param_.avg_doc_token_cnt_);
  }
  return ret;
}

template<>
inline int ObBlockMaxScoreCalc<ObBlockMaxIPRankingParam>::calc_max_score(const ObDatumRow &agg_row, double &max_score)
{
  int ret = OB_SUCCESS;
  const ObDatum &score_datum = agg_row.storage_datums_[ranking_param_.score_col_idx_];
  if (OB_UNLIKELY(score_datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null score", K(ret), K(score_datum));
  } else {
    max_score = score_datum.get_float() * ranking_param_.query_value_;
  }
  return ret;
}


// Iterator to scan inverted index meta data and generate Block Max MaxScore triplet
class ObBlockMaxScoreIterator
{
public:
  ObBlockMaxScoreIterator();
  virtual ~ObBlockMaxScoreIterator() { reset(); }
  void reset();
  void reuse();
  template<typename RankingParam>
  int init(
      const RankingParam &ranking_param,
      const ObBlockMaxScoreIterParam &block_max_scan_param,
      ObTableScanParam &scan_param);
  /*
   * Return tuple that describes max score upper bound between min/max domain id
   * for a sparse retrieval dimension query.
   */
  int get_next(const ObMaxScoreTuple *&max_score_tuple);
  int get_curr_max_score_tuple(const ObMaxScoreTuple *&max_score_tuple);
  int advance_to(const ObDatum &domin_id, const bool inclusive);
private:
  // TableScanParam as input param currently for:
  // 1. reuse essential input parameter for accessing data in tablet
  // 2. There is no skip index on memtable, need to scan corresponding column
  int inner_init(
      const ObBlockMaxScoreIterParam &block_max_scan_param,
      ObTableScanParam &scan_param);
  int init_advance_rowkey(const ObBlockMaxScoreIterParam &iter_param, ObTableScanParam &scan_param);
  int init_cmp_funcs(const ObBlockMaxScoreIterParam &block_max_scan_param);
  int calc_domain_id_range(const ObDatumRow &agg_row, const ObDatumRowkey &endkey);
private:
  typedef int (*CalcMaxScoreFunc)(const ObDatumRow &agg_row, ObIBlockMaxScoreCalc &scorer, double &max_score);
  static constexpr int64_t MAX_SCORE_CALC_BUF_SIZE = std::max(
      sizeof(ObBlockMaxScoreCalc<ObBlockMaxBM25RankingParam>),
      sizeof(ObBlockMaxScoreCalc<ObBlockMaxIPRankingParam>));
  static int calc_max_score_bm25(const ObDatumRow &agg_row, ObIBlockMaxScoreCalc &scorer, double &max_score)
  {
    return static_cast<ObBlockMaxScoreCalc<ObBlockMaxBM25RankingParam>*>(&scorer)->calc_max_score(agg_row, max_score);
  }
  static int calc_max_score_inner_product(const ObDatumRow &agg_row, ObIBlockMaxScoreCalc &scorer, double &max_score)
  {
    return static_cast<ObBlockMaxScoreCalc<ObBlockMaxIPRankingParam>*>(&scorer)->calc_max_score(agg_row, max_score);
  }
  template<typename RankingParam>
  inline int check_ranking_param(const RankingParam &ranking_param, const ObBlockMaxScoreIterParam &block_max_scan_param);
private:
  ObMaxScoreTuple max_score_tuple_;
  ObBlockStatIterator stat_iter_;
  ObDatumRowkey advance_rowkey_;
  ObDocIdExt advance_doc_id_;
  const ObBlockMaxScoreIterParam *block_max_scan_param_;
  ObBlockStatScanParam block_stat_scan_param_;
  ObDomainIdCmp domain_id_cmp_;
  ObDatumCmpFuncType dim_cmp_;
  char scorer_buf_[MAX_SCORE_CALC_BUF_SIZE];
  ObIBlockMaxScoreCalc *scorer_;
  CalcMaxScoreFunc calc_max_score_;
  ObStorageDatum scan_dim_datum_;
  bool first_tuple_iterated_;
  bool has_been_advanced_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBlockMaxScoreIterator);
};

template<typename RankingParam>
int ObBlockMaxScoreIterator::init(
    const RankingParam &ranking_param,
    const ObBlockMaxScoreIterParam &block_max_scan_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(block_max_scan_param, scan_param))) {
    STORAGE_LOG(WARN, "fail to init", K(ret));
  } else if (OB_FAIL(check_ranking_param(ranking_param, block_max_scan_param))) {
    STORAGE_LOG(WARN, "fail to check ranking param", K(ret));
  } else if (OB_ISNULL(scorer_ = new (scorer_buf_) ObBlockMaxScoreCalc(ranking_param))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

template<typename RankingParam>
int ObBlockMaxScoreIterator::check_ranking_param(
    const RankingParam &ranking_param,
    const ObBlockMaxScoreIterParam &block_max_scan_param)
{
  // unsupported ranking type
  return OB_NOT_SUPPORTED;
}

template<>
inline int ObBlockMaxScoreIterator::check_ranking_param(
    const ObBlockMaxBM25RankingParam &ranking_param,
    const ObBlockMaxScoreIterParam &block_max_scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranking_param.token_freq_col_idx_ >= block_max_scan_param.stat_cols_.count()
      || ranking_param.doc_length_col_idx_ >= block_max_scan_param.stat_cols_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(ranking_param), K(block_max_scan_param));
  }
  return ret;
}

template<>
inline int ObBlockMaxScoreIterator::check_ranking_param(
    const ObBlockMaxIPRankingParam &ranking_param,
    const ObBlockMaxScoreIterParam &block_max_scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranking_param.score_col_idx_ >= block_max_scan_param.stat_cols_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(ranking_param), K(block_max_scan_param));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif
