// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_ROWSCN_FILTER_H_
#define OB_STORAGE_COMPACTION_ROWSCN_FILTER_H_

#include "storage/compaction/ob_i_compaction_filter.h"
namespace oceanbase
{
namespace compaction
{

class ObRowscnFilter : public ObICompactionFilter
{
public:
  ObRowscnFilter()
    : ObICompactionFilter(),
      is_inited_(false),
      filter_val_(0),
      filter_col_idx_(0),
      filter_type_(FILTER_TYPE_MAX)
  {
  }
  ~ObRowscnFilter() {}
  int init(
    const int64_t filter_val,
    const int64_t filter_col_idx,
    const CompactionFilterType filter_type);
  OB_INLINE void reset()
  {
    filter_val_ = 0;
    filter_col_idx_ = 0;
    is_inited_ = false;
  }
  virtual CompactionFilterType get_filter_type() const override { return ROWSCN_FILTER; }
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) const override;
  virtual int get_filter_op(
    blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
    ObBlockOp &op) const override;
  virtual int64_t get_trans_version_col_idx() const override { return filter_col_idx_; }
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  INHERIT_TO_STRING_KV("ObRowscnFilter", ObICompactionFilter, K_(filter_val),
      K_(filter_col_idx), "filter_type", ObICompactionFilter::get_filter_type_str(filter_type_));
private:
  bool is_inited_;
  int64_t filter_val_;
  int64_t filter_col_idx_;
  CompactionFilterType filter_type_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_ROWSCN_FILTER_H_
