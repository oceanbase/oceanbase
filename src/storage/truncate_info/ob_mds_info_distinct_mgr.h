//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_DISTINCT_MGR_H_
#define OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_DISTINCT_MGR_H_
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
struct ObVersionRange;
}
namespace compaction
{
struct ObMdsFilterInfo;
}
namespace storage
{
class ObTablet;
struct ObMdsInfoDistinctMgr final
{
  ObMdsInfoDistinctMgr();
  ~ObMdsInfoDistinctMgr() { reset(); }
  int init(
    common::ObArenaAllocator &allocator,
    storage::ObTablet &tablet,
    const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const common::ObVersionRange &read_version_range,
    const bool for_access);
  bool is_valid() const { return is_inited_; }
  bool empty() const { return distinct_array_.empty(); }
  void clear() {
    array_.reset();
    distinct_array_.reuse();
  }
  void reset()
  {
    is_inited_ = false;
    array_.reset();
    distinct_array_.reset();
  }
  int fill_mds_filter_info(
    common::ObIAllocator &allocator,
    compaction::ObMdsFilterInfo &mds_filter_info) const;
  int check_mds_filter_info(
    const compaction::ObMdsFilterInfo &mds_filter_info);
  int get_distinct_truncate_info_array(
    ObTruncateInfoArray &distinct_array) const;
  const ObIArray<ObTruncateInfo *> &get_distinct_truncate_info_array() const { return distinct_array_; }
  bool operator ==(const ObMdsInfoDistinctMgr &other) const = delete;
  TO_STRING_KV(K_(array), "distinct_array_cnt", distinct_array_.count(), K_(distinct_array));
private:
  int read_split_truncate_info_array(
    const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const common::ObVersionRange &read_version_range,
    const bool for_access);
  int build_distinct_array(const ObVersionRange &read_version_range, const bool for_access);
private:
  ObTruncateInfoArray array_;
  ObSEArray<ObTruncateInfo *, 4> distinct_array_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMdsInfoDistinctMgr);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_DISTINCT_MGR_H_
