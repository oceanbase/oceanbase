// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
#define OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
#include "storage/compaction/ob_i_compaction_filter.h"
#include "storage/compaction/ob_mds_filter_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"

namespace oceanbase
{
namespace compaction
{
class ObMdsInfoCompactionFilter final : public ObICompactionFilter
{
public:
  ObMdsInfoCompactionFilter()
    : ObICompactionFilter(),
      allocator_("MdsCompaFit", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      truncate_filter_(allocator_),
      ttl_filter_(allocator_),
      schema_rowkey_cnt_(0),
      is_inited_(false)
  {}
  virtual ~ObMdsInfoCompactionFilter() { reset(); }
  void reset()
  {
    is_inited_ = false;
  }
  int init(
    common::ObIAllocator &allocator,
    const ObTabletID &tablet_id,
    const ObStorageSchema *schema,
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const storage::ObMdsInfoDistinctMgr &mds_info_mgr);
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const override;
  virtual CompactionFilterType get_filter_type() const override { return MDS_IN_MEDIUM_INFO; }
  virtual int get_filter_op(
    blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
    ObBlockOp &op) const override;
  virtual int64_t get_trans_version_col_idx() const override
  { return schema_rowkey_cnt_; }
  INHERIT_TO_STRING_KV("ObMdsInfoCompactionFilter", ObICompactionFilter, K_(truncate_filter), K_(ttl_filter));
private:
  ObArenaAllocator allocator_;
  storage::ObTruncatePartitionFilter truncate_filter_;
  storage::ObTTLFilter ttl_filter_;
  int64_t schema_rowkey_cnt_;
  bool is_inited_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
