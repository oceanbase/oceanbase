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

#ifndef OB_BLOCK_STAT_COLLECTOR_H_
#define OB_BLOCK_STAT_COLLECTOR_H_

#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"


namespace oceanbase
{
namespace storage
{

class ObIColBlockStatCollector
{
public:
  ObIColBlockStatCollector() {}
  virtual ~ObIColBlockStatCollector() {}
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int init(
      const ObColDesc &col_desc,
      blocksstable::ObStorageDatum &result_datum,
      ObIAllocator &result_alloc) = 0;
  virtual int collect(const ObDatum &datum) = 0;
};

// Aggregator to perform loose min-max aggregation based on data / skip index pre-agg result,
// regardless of multi-version data caused by dmls, It will try to calculate an
// approximate upper bound of max value and lower bound of min value.
template<blocksstable::ObSkipIndexColType AGG_TYPE>
class ObLooseMinMaxStatCollector final : public ObIColBlockStatCollector
{
public:
  ObLooseMinMaxStatCollector();
  virtual ~ObLooseMinMaxStatCollector() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(
      const ObColDesc &col_descs,
      blocksstable::ObStorageDatum &result_datum,
      ObIAllocator &result_alloc) override;
  virtual int collect(const ObDatum &datum) override;
  TO_STRING_KV(KPC_(agg_result), K(AGG_TYPE));
private:
  int update_agg_result(const ObDatum &datum);
  inline bool need_update_result(int cmp_ret);
private:
  blocksstable::ObStorageDatum *agg_result_;
  ObDatumCmpFuncType cmp_func_;
  ObIAllocator *allocator_;
};

typedef ObLooseMinMaxStatCollector<blocksstable::ObSkipIndexColType::SK_IDX_MIN> ObLooseMinStatCollector;
typedef ObLooseMinMaxStatCollector<blocksstable::ObSkipIndexColType::SK_IDX_MAX> ObLooseMaxStatCollector;

class ObBM25MaxScoreParamCollector
{
public:
  ObBM25MaxScoreParamCollector();
  virtual ~ObBM25MaxScoreParamCollector() { reset(); }
  void reset();
  void reuse();
  int init(
      const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
      const ObIArray<uint32_t> &stat_projectors,
      const ObIArray<ObColDesc> &col_descs,
      blocksstable::ObDatumRow &result_row,
      ObIAllocator &allocator);
  int collect_data_row(const blocksstable::ObDatumRow &data_row);
  int collect_agg_row(const blocksstable::ObDatumRow &agg_row);
private:
  uint32_t token_freq_data_idx_;
  uint32_t token_freq_agg_idx_;
  uint32_t doc_len_data_idx_;
  uint32_t doc_len_agg_idx_;
  double curr_max_score_;
  blocksstable::ObStorageDatum *token_freq_res_;
  blocksstable::ObStorageDatum *doc_len_res_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBM25MaxScoreParamCollector);
};

class ObBlockStatCollector
{
public:
  ObBlockStatCollector();
  virtual ~ObBlockStatCollector() { reset(); }
  void reset();
  void reuse();
  int init(
      const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
      const ObIArray<uint32_t> &stat_projectors,
      const ObIArray<ObColDesc> &col_descs,
      ObIAllocator &allocator);
  int collect_data_row(const blocksstable::ObDatumRow &data_row);
  int collect_agg_row(const blocksstable::ObDatumRow &agg_row);
  int get_result_row(const blocksstable::ObDatumRow *&result_row);
private:
  int init_collectors(
      const ObIArray<blocksstable::ObSkipIndexColMeta> &stat_cols,
      const ObIArray<uint32_t> &stat_projectors,
      const ObIArray<ObColDesc> &col_descs,
      ObIAllocator &allocator);
  template <typename CollectorType>
  int collect_row(
      const blocksstable::ObDatumRow &row,
      const ObIArray<uint32_t> &proejctors,
      ObFixedArray<CollectorType, ObIAllocator> &collectors);
private:
  typedef ObFixedArray<ObLooseMinStatCollector, ObIAllocator> LooseMinCollectorArray;
  typedef ObFixedArray<ObLooseMaxStatCollector, ObIAllocator> LooseMaxCollectorArray;
  typedef ObFixedArray<uint32_t, ObIAllocator> ColumnProjectorArray;

  LooseMinCollectorArray loose_min_collectors_;
  ColumnProjectorArray min_agg_row_projs_;
  ColumnProjectorArray min_data_row_projs_;
  LooseMaxCollectorArray loose_max_collectors_;
  ColumnProjectorArray max_agg_row_projs_;
  ColumnProjectorArray max_data_row_projs_;
  ObBM25MaxScoreParamCollector bm25_collector_;
  blocksstable::ObDatumRow result_row_;
  ObArenaAllocator result_tmp_allocator_;
  const ObIArray<blocksstable::ObSkipIndexColMeta> *projector_;
  bool has_bm25_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBlockStatCollector);
};



/*--------------------------- ObLooseMinMaxStatCollector definitions -----------------------------*/
template<blocksstable::ObSkipIndexColType AGG_TYPE>
ObLooseMinMaxStatCollector<AGG_TYPE>::ObLooseMinMaxStatCollector()
  : agg_result_(),
    cmp_func_(nullptr),
    allocator_(nullptr)
{
}

template<blocksstable::ObSkipIndexColType AGG_TYPE>
void ObLooseMinMaxStatCollector<AGG_TYPE>::reset()
{
  agg_result_ = nullptr;
  cmp_func_ = nullptr;
  allocator_ = nullptr;
}

template<blocksstable::ObSkipIndexColType AGG_TYPE>
void ObLooseMinMaxStatCollector<AGG_TYPE>::reuse()
{
  if (nullptr != agg_result_) {
    agg_result_->reuse();
    agg_result_->set_null();
  }
}


template<blocksstable::ObSkipIndexColType AGG_TYPE>
int ObLooseMinMaxStatCollector<AGG_TYPE>::init(
    const ObColDesc &col_desc,
    blocksstable::ObStorageDatum &result_datum,
    ObIAllocator &result_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(agg_result_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "Init twice", K(ret));
  } else {
    allocator_ = &result_alloc;
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type());
    if (blocksstable::ObSkipIndexColType::SK_IDX_MAX == AGG_TYPE) {
      cmp_func_ = basic_funcs->null_first_cmp_;
    } else if (blocksstable::ObSkipIndexColType::SK_IDX_MIN == AGG_TYPE) {
      cmp_func_ = basic_funcs->null_last_cmp_;
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "Unsupported aggregation type", K(ret), K(AGG_TYPE));
    }
    if (OB_SUCC(ret)) {
      agg_result_ = &result_datum;
      agg_result_->reuse();
      agg_result_->set_null();
    }
  }
  return ret;
}

template<blocksstable::ObSkipIndexColType AGG_TYPE>
int ObLooseMinMaxStatCollector<AGG_TYPE>::collect(const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  int cmp_res = 0;
  if (OB_ISNULL(agg_result_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(cmp_func_(datum, *agg_result_, cmp_res))) {
    STORAGE_LOG(WARN, "failed to compare datum", K(ret), K(datum), KPC_(agg_result));
  } else if (!need_update_result(cmp_res)) {
    // skip
  } else if (OB_FAIL(update_agg_result(datum))) {
    STORAGE_LOG(WARN, "failed to update agg result", K(ret), K(datum), K(cmp_res));
  }
  return ret;
}

template<>
inline bool ObLooseMinMaxStatCollector<blocksstable::ObSkipIndexColType::SK_IDX_MIN>::need_update_result(int cmp_ret)
{
  return cmp_ret < 0;
}

template<>
inline bool ObLooseMinMaxStatCollector<blocksstable::ObSkipIndexColType::SK_IDX_MAX>::need_update_result(int cmp_ret)
{
  return cmp_ret > 0;
}

template<blocksstable::ObSkipIndexColType AGG_TYPE>
int ObLooseMinMaxStatCollector<AGG_TYPE>::update_agg_result(const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null datum as agg result", K(ret), K(datum));
  } else {
    agg_result_->reuse();
    if (datum.len_ < common::OBJ_DATUM_NUMBER_RES_SIZE) {
      char *copy_buf = agg_result_->buf_;
      const int64_t max_copy_size = common::OBJ_DATUM_NUMBER_RES_SIZE;
      int64_t copy_pos = 0;
      if (OB_FAIL(agg_result_->ObDatum::deep_copy(datum, copy_buf, max_copy_size, copy_pos))) {
        STORAGE_LOG(WARN, "fail to deep copy datum", K(ret), K(datum));
      }
    } else if (OB_FAIL(agg_result_->ObDatum::deep_copy(datum, *allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy datum", K(ret), K(datum));
    }
  }
  return ret;
}

/*---------------------------- ObBlockStatCollector definitions ----------------------------------*/
template <typename CollectorType>
int ObBlockStatCollector::collect_row(
    const blocksstable::ObDatumRow &row,
    const ObIArray<uint32_t> &proejctors,
    ObFixedArray<CollectorType, ObIAllocator> &collectors)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < proejctors.count(); ++i) {
    const uint32_t proj = proejctors.at(i);
    const blocksstable::ObStorageDatum &datum = row.storage_datums_[proj];
    CollectorType &collector = collectors.at(i);
    if (OB_FAIL(collector.collect(datum))) {
      STORAGE_LOG(WARN, "failed to collect datum stat", K(i), K(proj), K(datum));
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase

#endif
