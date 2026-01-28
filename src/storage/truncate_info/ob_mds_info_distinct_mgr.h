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
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_kv_cache.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"

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

template <typename MDSInfo, typename MDSInfoArray, typename MDSInfoCacheKey>
class ObMdsInfoDistinctMgrImpl final
{
public:
  using MDSInfoCacheUtil = ObMDSInfoKVCacheUtil<MDSInfoCacheKey, MDSInfo>;

public:
  ObMdsInfoDistinctMgrImpl();

  ~ObMdsInfoDistinctMgrImpl() { reset(); }

  int init(ObArenaAllocator &allocator,
           ObTablet &tablet,
           const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
           const ObVersionRange &read_version_range,
           const bool for_access);

  OB_INLINE bool is_valid() const { return is_inited_; }

  OB_INLINE bool empty() const { return distinct_array_.empty(); }
  OB_INLINE int64_t get_newest_commit_version() const { return newest_commit_version_; }

  OB_INLINE void clear()
  {
    array_.reset();
    distinct_array_.reuse();
  }

  OB_INLINE void reset()
  {
    is_inited_ = false;
    array_.reset();
    distinct_array_.reset();
  }

  int fill_mds_filter_info(ObIAllocator &allocator,
                           compaction::ObMdsFilterInfo &mds_filter_info) const;

  int check_mds_filter_info(const compaction::ObMdsFilterInfo &mds_filter_info);

  int get_distinct_mds_info_array(MDSInfoArray &distinct_array) const;

  const ObIArray<MDSInfo *> &get_distinct_mds_info_array() const { return distinct_array_; }

  bool operator==(const ObMdsInfoDistinctMgrImpl &other) const = delete;

  TO_STRING_KV(K_(newest_commit_version), K_(array), "distinct_array_cnt", distinct_array_.count(), K_(distinct_array));

private:
  int read_split_mds_info_array(const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                                const ObVersionRange &read_version_range,
                                const bool for_access);

  int build_distinct_array(const ObVersionRange &read_version_range, const bool for_access);

  static int need_replace(const ObTruncateInfo &exist_info, const ObTruncateInfo &input_info, bool &equal, bool &replace);
  static int need_replace(const ObTTLFilterInfo &exist_info, const ObTTLFilterInfo &input_info, bool &equal, bool &replace);

private:
  MDSInfoArray array_;
  ObSEArray<MDSInfo *, 4> distinct_array_;
  int64_t newest_commit_version_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMdsInfoDistinctMgrImpl);
};

using ObTruncateInfoDistinctMgr
    = ObMdsInfoDistinctMgrImpl<ObTruncateInfo, ObTruncateInfoArray, ObTruncateInfoCacheKey>;
using ObTTLFilterInfoDistinctMgr
    = ObMdsInfoDistinctMgrImpl<ObTTLFilterInfo, ObTTLFilterInfoArray, ObTTLFilterInfoCacheKey>;

class ObMdsInfoDistinctMgr
{
public:
  ObMdsInfoDistinctMgr() : truncate_info_distinct_mgr_(), ttl_filter_info_distinct_mgr_() {}
  ~ObMdsInfoDistinctMgr() = default;

  bool is_empty() const { return truncate_info_distinct_mgr_.empty() && ttl_filter_info_distinct_mgr_.empty(); }
  int64_t get_newest_commit_version() const
  { return MAX(truncate_info_distinct_mgr_.get_newest_commit_version(), ttl_filter_info_distinct_mgr_.get_newest_commit_version()); }
  int init(ObArenaAllocator &allocator,
           ObTablet &tablet,
           const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
           const ObVersionRange &read_version_range,
           const bool for_access)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(truncate_info_distinct_mgr_.init(
            allocator, tablet, split_extra_tablet_handles, read_version_range, for_access))) {
      STORAGE_LOG(WARN, "fail to init truncate info distinct mgr", K(ret));
    } else if (OB_FAIL(ttl_filter_info_distinct_mgr_.init(allocator,
                                                          tablet,
                                                          split_extra_tablet_handles,
                                                          read_version_range,
                                                          for_access))) {
      STORAGE_LOG(WARN, "fail to init ttl filter info distinct mgr", K(ret));
    }
    return ret;
  }

  int fill_mds_filter_info(ObIAllocator &allocator,
                           compaction::ObMdsFilterInfo &mds_filter_info) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(truncate_info_distinct_mgr_.fill_mds_filter_info(allocator, mds_filter_info))) {
      STORAGE_LOG(WARN, "fail to fill truncate info mds filter info", K(ret));
    } else if (OB_FAIL(ttl_filter_info_distinct_mgr_.fill_mds_filter_info(allocator, mds_filter_info))) {
      STORAGE_LOG(WARN, "fail to fill ttl filter info mds filter info", K(ret));
    }
    return ret;
  }

  int check_mds_filter_info(const compaction::ObMdsFilterInfo &mds_filter_info)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(truncate_info_distinct_mgr_.check_mds_filter_info(mds_filter_info))) {
      STORAGE_LOG(WARN, "fail to check truncate info mds filter info", K(ret));
    } else if (OB_FAIL(ttl_filter_info_distinct_mgr_.check_mds_filter_info(mds_filter_info))) {
      STORAGE_LOG(WARN, "fail to check ttl filter info mds filter info", K(ret));
    }
    return ret;
  }

  const ObTruncateInfoDistinctMgr &get_truncate_info_distinct_mgr() const
  {
    return truncate_info_distinct_mgr_;
  }

  const ObTTLFilterInfoDistinctMgr &get_ttl_filter_info_distinct_mgr() const
  {
    return ttl_filter_info_distinct_mgr_;
  }

  TO_STRING_KV(K_(truncate_info_distinct_mgr), K_(ttl_filter_info_distinct_mgr));

private:
  ObTruncateInfoDistinctMgr truncate_info_distinct_mgr_;
  ObTTLFilterInfoDistinctMgr ttl_filter_info_distinct_mgr_;
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_DISTINCT_MGR_H_
