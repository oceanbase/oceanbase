//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
#define OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
#include "storage/compaction/ob_i_compaction_filter.h"
#include "storage/compaction/ob_mds_filter_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
namespace oceanbase
{
namespace storage
{
struct ObMdsInfoDistinctMgr;
}
namespace compaction
{
class ObMdsInfoCompactionFilter final : public ObICompactionFilter
{
public:
  ObMdsInfoCompactionFilter()
    : ObICompactionFilter(),
      truncate_filter_(),
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
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const storage::ObMdsInfoDistinctMgr &truncate_info_mgr);
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) override;
  virtual CompactionFilterType get_filter_type() const override { return MDS_IN_MEDIUM_INFO; }
  INHERIT_TO_STRING_KV("ObMdsInfoCompactionFilter", ObICompactionFilter, K_(truncate_filter));
private:
  storage::ObTruncatePartitionFilter truncate_filter_;
  bool is_inited_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MDS_INFO_COMPACTION_FILTER_H_
