/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_AGGREGATOR_
#define OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_AGGREGATOR_

#include "share/schema/ob_table_param.h"
#include "ob_index_block_util.h"
#include "ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIMicroBlockWriter;
class ObIDatumIter;
class ObIColAggregator
{
public:
  ObIColAggregator() : col_desc_(), result_(nullptr), can_aggregate_(true) {}
  virtual ~ObIColAggregator() {}

  virtual int init(const ObColDesc &col_desc, ObStorageDatum &result) = 0;
  virtual void reset() = 0;
  virtual void reuse();
  virtual int eval(const ObStorageDatum &datum, const bool is_data) = 0;
  virtual int eval(ObIDatumIter &datum_iter) = 0;
  virtual int get_result(const ObStorageDatum *&result) = 0;
  VIRTUAL_TO_STRING_KV(K_(can_aggregate));

  void set_not_aggregate() { can_aggregate_ = false; }
  inline ObColDesc get_col_decs() const { return col_desc_; }
protected:
  int inner_init(const ObColDesc &col_desc, ObStorageDatum &result);
  static int copy_agg_datum(const ObDatum &src, ObDatum &dst);
  static bool need_set_not_aggregate(const ObObjType type, const ObDatum &datum)
  {
    // exceeds maximum length or contain out row column, can not keep aggregate
    return (!datum.is_null() && datum.len_ > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH) ||
           (is_lob_storage(type) && !datum.is_null() && !datum.get_lob_data().in_row_);
  }
protected:
  ObColDesc col_desc_;
  ObStorageDatum *result_;
  bool can_aggregate_;
};

class ObColNullCountAggregator final : public ObIColAggregator
{
public:
  ObColNullCountAggregator() : null_count_(0) {}
  virtual ~ObColNullCountAggregator() {}

  int init(const ObColDesc &col_desc, ObStorageDatum &result) override;
  void reset() override { new (this) ObColNullCountAggregator(); }
  void reuse() override;
  int eval(const ObStorageDatum &datum, const bool is_data) override;
  int eval(ObIDatumIter &datum_iter) override;
  int get_result(const ObStorageDatum *&result) override;
private:
  int64_t null_count_;
  DISALLOW_COPY_AND_ASSIGN(ObColNullCountAggregator);
};

class ObColMaxAggregator final : public ObIColAggregator
{
public:
  ObColMaxAggregator() : cmp_func_(nullptr) {}
  virtual ~ObColMaxAggregator() {}

  int init(const ObColDesc &col_desc, ObStorageDatum &result) override;
  void reset() override { new (this) ObColMaxAggregator(); }
  void reuse() override;
  int eval(const ObStorageDatum &datum, const bool is_data) override;
  int eval(ObIDatumIter &datum_iter) override;
  int get_result(const ObStorageDatum *&result) override;
private:
  common::ObDatumCmpFuncType cmp_func_;
  DISALLOW_COPY_AND_ASSIGN(ObColMaxAggregator);
};

class ObColMinAggregator final : public ObIColAggregator
{
public:
  ObColMinAggregator() : cmp_func_(nullptr) {}
  virtual ~ObColMinAggregator() {}

  int init(const ObColDesc &col_desc, ObStorageDatum &result) override;
  void reset() override { new (this) ObColMinAggregator(); }
  void reuse() override;
  int eval(const ObStorageDatum &datum, const bool is_data) override;
  int eval(ObIDatumIter &datum_iter) override;
  int get_result(const ObStorageDatum *&result) override;
private:
  common::ObDatumCmpFuncType cmp_func_;
  DISALLOW_COPY_AND_ASSIGN(ObColMinAggregator);
};

class ObColSumAggregator final : public ObIColAggregator
{
typedef int (ObColSumAggregator::*ObColSumAggEvalFuncCType)(const common::ObDatum &datum);
public:
  ObColSumAggregator() : eval_func_(nullptr) {}
  virtual ~ObColSumAggregator() {}
  int init(const ObColDesc &col_desc, ObStorageDatum &result) override;
  void reset() override { new (this) ObColSumAggregator(); }
  void reuse() override;
  int eval(const ObStorageDatum &datum, const bool is_data) override;
  int eval(ObIDatumIter &datum_iter) override;
  int get_result(const ObStorageDatum *&result) override;
private:
  // eval
  int choose_eval_func(const bool is_data);
  int inner_eval_number(const number::ObNumber &nmb);
  int eval_int_number(const common::ObDatum &datum);
  int eval_uint_number(const common::ObDatum &datum);
  int eval_decimal_int_number(const common::ObDatum &datum);
  int eval_number(const common::ObDatum &datum);
  int eval_float(const common::ObDatum &datum);
  int eval_double(const common::ObDatum &datum);

  // eval float

private:
  ObColSumAggEvalFuncCType eval_func_;
  DISALLOW_COPY_AND_ASSIGN(ObColSumAggregator);
};

template <typename T, int64_t MAX_COUNT, int64_t BLOCK_SIZE>
class ObPodFix2dArray;
class ObEncodingHashTable;
class ObDictEncodingHashTable;

struct ObMicroDataPreAggParam
{
  ObMicroDataPreAggParam() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }
  bool use_encoding_ht() const { return is_pax_encoding_ && nullptr != encoding_ht_; }
  bool use_cs_encoding_ht() const { return is_cs_encoding_ && nullptr != cs_encoding_ht_; }
  TO_STRING_KV(KP_(col_datums), KP_(encoding_ht), K_(null_cnt), K_(min_integer), K_(max_integer),
      K_(is_integer_aggregated), K_(is_cs_encoding), K_(is_pax_encoding));

  const ObPodFix2dArray<ObDatum, 1 << 20, common::OB_MALLOC_NORMAL_BLOCK_SIZE> *col_datums_;
  union {
    const ObEncodingHashTable *encoding_ht_;
    const ObDictEncodingHashTable *cs_encoding_ht_;
  };
  uint64_t null_cnt_;
  uint64_t min_integer_;
  uint64_t max_integer_;
  bool is_integer_aggregated_;
  bool is_cs_encoding_;
  bool is_pax_encoding_;
};

class ObISkipIndexAggregator
{
public:
  ObISkipIndexAggregator();
  virtual ~ObISkipIndexAggregator() { reset(); }

  void reset();
  void reuse(); // clear aggregated result

  int init(
      const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
      const ObIArray<ObColDesc> &full_col_descs,
      ObDatumRow &agg_result,
      ObIAllocator &allocator);

  // Aggregate with datum row
  virtual int eval(const ObDatumRow &datum_row) = 0;
  // Aggregate with serialized agg row
  int eval(const char *buf, const int64_t buf_size, const int64_t row_count);
  // Generate aggregated row for serialization
  int get_aggregated_row(const ObDatumRow *&aggregated_row);
  int64_t get_max_agg_size() { return max_agg_size_; }
  TO_STRING_KV(K_(col_aggs), KPC(agg_result_), K_(max_agg_size), K_(need_aggregate), K_(is_inited));
private:
  int calc_max_agg_size(
      const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
      const ObIArray<ObColDesc> &full_col_descs);
  int init_col_aggregators(
      const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
      const ObIArray<ObColDesc> &full_col_descs,
      ObIAllocator &allocator);
  template<typename T>
  int init_col_aggregator(
      const ObColDesc &col_desc,
      ObStorageDatum &result_datum,
      ObIAllocator &allocator);
protected:
  ObIAllocator *allocator_;
  common::ObFixedArray<ObIColAggregator *, common::ObIAllocator> col_aggs_;
  ObDatumRow *agg_result_;
  const ObIArray<ObSkipIndexColMeta> *full_agg_metas_;
  const ObIArray<ObColDesc> *full_col_descs_;
  ObAggRowReader agg_row_reader_;
  int64_t max_agg_size_;
  bool need_aggregate_;
  bool evaluated_;
  bool is_inited_;
};

class ObSkipIndexIndexAggregator final : public ObISkipIndexAggregator
{
public:
  ObSkipIndexIndexAggregator();
  virtual ~ObSkipIndexIndexAggregator() {}

  virtual int eval(const ObDatumRow &datum_row) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSkipIndexIndexAggregator);
};

class ObSkipIndexDataAggregator final : public ObISkipIndexAggregator
{
public:
  ObSkipIndexDataAggregator();
  virtual ~ObSkipIndexDataAggregator() {}

  virtual int eval(const ObDatumRow &datum_row) override;
  int eval(const ObIMicroBlockWriter &data_micro_writer);
private:
  bool can_agg_with_dict(const ObSkipIndexColType idx_type)
  {
    return ObSkipIndexColType::SK_IDX_MIN == idx_type || ObSkipIndexColType::SK_IDX_MAX == idx_type;
  }
  bool can_use_pre_agg_integer(const ObSkipIndexColMeta &col_meta);
  int do_col_agg_with_pre_agg_integer(
      const int64_t agg_idx,
      const ObSkipIndexColMeta &col_meta,
      const ObMicroDataPreAggParam &agg_param);
  template<typename IterParamType>
  int do_col_agg(const int64_t agg_idx, const IterParamType &iter_param);
  DISALLOW_COPY_AND_ASSIGN(ObSkipIndexDataAggregator);
};

struct ObAggregateInfo final
{
public:
  ObAggregateInfo();
  ~ObAggregateInfo();
  void reset();
  void eval(const ObIndexBlockRowDesc &row_desc);
  void get_agg_result(ObIndexBlockRowDesc &row_desc) const;

  TO_STRING_KV(K_(row_count), K_(row_count_delta), K_(max_merged_trans_version),
      K_(macro_block_count), K_(micro_block_count), K_(can_mark_deletion),
      K_(contain_uncommitted_row), K_(has_string_out_row), K_(has_lob_out_row),
      K_(is_last_row_last_flag));
public:
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  bool can_mark_deletion_;
  bool contain_uncommitted_row_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool is_last_row_last_flag_;
};


struct ObIndexRowAggInfo final
{
public:
  ObIndexRowAggInfo();
  ~ObIndexRowAggInfo();
  void reset();
  bool is_valid() const { return (need_data_aggregate_ && aggregated_row_.is_valid()) || (!need_data_aggregate_ && !aggregated_row_.is_valid()); }
  TO_STRING_KV(K_(aggregated_row), K_(aggregate_info), K_(need_data_aggregate));
public:
  ObDatumRow aggregated_row_;
  ObAggregateInfo aggregate_info_;
  bool need_data_aggregate_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexRowAggInfo);
};

class ObIndexBlockAggregator final
{
public:
  ObIndexBlockAggregator();
  ~ObIndexBlockAggregator() {}
  void reset();
  void reuse();
  int init(const ObDataStoreDesc &store_desc, ObIAllocator &allocator);
  int eval(const ObIndexBlockRowDesc &row_desc);
  int get_index_agg_result(ObIndexBlockRowDesc &row_desc);
  int get_index_row_agg_info(ObIndexRowAggInfo &index_row_agg_info, ObIAllocator &allocator);
  inline bool need_data_aggregate() const { return need_data_aggregate_ && !has_reused_null_agg_in_this_micro_block_; };
  inline const ObDatumRow& get_aggregated_row() const { return aggregated_row_; };
  inline int64_t get_max_agg_size() { return skip_index_aggregator_.get_max_agg_size(); }
  inline int64_t get_row_count() const { return aggregate_info_.row_count_; }
  inline bool contain_uncommitted_row() const { return aggregate_info_.contain_uncommitted_row_; }
  inline bool is_last_row_last_flag() const { return aggregate_info_.is_last_row_last_flag_; }
  inline int64_t get_max_merged_trans_version() const { return aggregate_info_.max_merged_trans_version_; }
  TO_STRING_KV(K_(skip_index_aggregator), K_(aggregated_row), K_(aggregate_info),
      K_(need_data_aggregate), K_(has_reused_null_agg_in_this_micro_block), K_(is_inited));
private:
  ObSkipIndexIndexAggregator skip_index_aggregator_;
  ObDatumRow aggregated_row_;
  ObAggregateInfo aggregate_info_;
  bool need_data_aggregate_;
  bool has_reused_null_agg_in_this_micro_block_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_AGGREGATOR_
