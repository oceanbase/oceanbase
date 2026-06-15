/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_ACCESS_MDS_FILTER_MGR_H
#define OB_STORAGE_ACCESS_MDS_FILTER_MGR_H

#include "storage/compaction_ttl/ob_base_version_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "storage/blocksstable/ob_sstable.h"

namespace oceanbase
{
namespace storage
{

class ObMDSFilterFlags
{
public:
  enum MDSFilterType : uint32_t
  {
    BASE_VERSION_FILTER = 0x1,
    TRUNCATE_FILTER = 0x2,
    TTL_FILTER = 0x4
  };

  ObMDSFilterFlags() : flags_(0) {}
  ~ObMDSFilterFlags() = default;

  OB_INLINE bool operator==(const ObMDSFilterFlags &other) const { return flags_ == other.flags_; }
  OB_INLINE bool operator!=(const ObMDSFilterFlags &other) const { return flags_ != other.flags_; }
  OB_INLINE bool is_empty() const { return flags_ == 0; }
  OB_INLINE bool has_type(const MDSFilterType type) const { return flags_ & type; }
  OB_INLINE ObMDSFilterFlags &reset()
  {
    flags_ = 0;
    return *this;
  }
  OB_INLINE ObMDSFilterFlags &add_type(const MDSFilterType type)
  {
    flags_ |= type;
    return *this;
  }
  OB_INLINE ObMDSFilterFlags &add_type_if(const bool condition, const MDSFilterType type)
  {
    if (condition) {
      flags_ |= type;
    }
    return *this;
  }

  TO_STRING_KV(K(flags_));

  uint32_t flags_;
};

class ObMDSFilterMgr
{
public:
  using MDSFilterType = ObMDSFilterFlags::MDSFilterType;

public:
  ObMDSFilterMgr(ObIAllocator &outer_allocator)
      : outer_allocator_(outer_allocator),
        filter_allocator_("MDSFilterMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        and_filter_node_(nullptr),
        and_filter_executor_(nullptr),
        truncate_partition_filter_(nullptr),
        ttl_filter_(nullptr),
        base_version_filter_(nullptr),
        build_filter_flags_()
  {
  }

  ~ObMDSFilterMgr();

  OB_INLINE ObIAllocator &get_outer_allocator() const { return outer_allocator_; }
  OB_INLINE ObArenaAllocator &get_filter_allocator() { return filter_allocator_; }
  OB_INLINE ObTruncatePartitionFilter *get_truncate_partition_filter() const
  {
    return build_filter_flags_.has_type(MDSFilterType::TRUNCATE_FILTER) ? truncate_partition_filter_ : nullptr;
  }
  OB_INLINE ObTTLFilter *get_ttl_filter() const
  {
    return build_filter_flags_.has_type(MDSFilterType::TTL_FILTER) ? ttl_filter_ : nullptr;
  }
  OB_INLINE ObBaseVersionFilter *get_base_version_filter() const
  {
    return build_filter_flags_.has_type(MDSFilterType::BASE_VERSION_FILTER) ? base_version_filter_ : nullptr;
  }

  OB_INLINE int build_filters(ObTablet &tablet,
                              const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                              const ObITableReadInfo &read_info,
                              const ObVersionRange &read_version_range,
                              const ObMDSFilterFlags &mds_filter_flags);

  OB_INLINE int filter(const ObDatumRow &row, bool &filtered, bool check_filter = true, bool check_version = false) const;

  // There will be a sstable level skip-index row in ObSSTableMeta after 4.5.1.
  // We can use it to filter a whole sstable before construct query iterators.
  OB_INLINE int do_sstable_level_filter(const ObSSTable &table, bool &is_filtered) const;

  int check_filter_row_complete(const ObDatumRow &row, bool &complete) const;

  int combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter);

  OB_INLINE bool is_empty() const { return build_filter_flags_.is_empty(); }
  OB_INLINE void set_should_combine_filters_flag(const ObMDSFilterFlags &mds_filter_flags)
  {
    build_filter_flags_ = mds_filter_flags;
  }
  OB_INLINE void clear_should_combine_filters_flag()
  {
    build_filter_flags_.reset();
  }

  TO_STRING_KV(KPC(truncate_partition_filter_),
               KPC(ttl_filter_),
               KPC(base_version_filter_),
               K(build_filter_flags_),
               K(and_filter_executor_));

private:
  // the mds filter mgr is always allocated by outer allocator in query path, and the same as other filter-wrapper.
  // we don't allocate mds filter by this allocator, because we need to reuse them for whole sql-stmt and never release them.
  // Therefore, a dedicated allocator is better. (In DML path, this allocator will be an FIFO allocator)
  ObIAllocator &outer_allocator_;

  // this allocator is used to allocate any mds-related filter
  ObArenaAllocator filter_allocator_;

  // and filter is used to combine sql pushdown filter and mds filter
  sql::ObPushdownFilterNode *and_filter_node_;
  sql::ObPushdownFilterExecutor *and_filter_executor_;

  ObTruncatePartitionFilter *truncate_partition_filter_;
  ObTTLFilter *ttl_filter_;
  ObBaseVersionFilter *base_version_filter_;

  // The build_filters_flag_ means which mds filter should be combined into and filter.
  ObMDSFilterFlags build_filter_flags_;
};

class ObMDSFilterMgrFactory
{
public:
  static int build_mds_filter_mgr(ObTablet &tablet,
                                  const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                                  const ObITableReadInfo *read_info,
                                  const ObVersionRange &read_version_range,
                                  ObIAllocator &outer_allocator,
                                  ObMDSFilterMgr *&mds_filter_mgr,
                                  const ObMDSFilterFlags &mds_filter_flags);

  static void destroy_mds_filter_mgr(ObMDSFilterMgr *&mds_filter_mgr);
};

OB_INLINE int ObMDSFilterMgr::filter(const ObDatumRow &row,
                                     bool &filtered,
                                     bool check_filter,
                                     bool check_version) const
{
  int ret = OB_SUCCESS;

  filtered = false;

  if (OB_SUCC(ret) && check_filter && !filtered && get_truncate_partition_filter() != nullptr) {
    if (OB_FAIL(truncate_partition_filter_->filter(row, filtered))) {
      STORAGE_LOG(WARN, "failed to filter truncate partition filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && check_filter && !filtered && get_ttl_filter() != nullptr) {
    if (OB_FAIL(ttl_filter_->filter(row, filtered))) {
      STORAGE_LOG(WARN, "failed to filter ttl filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && check_version && !filtered && get_base_version_filter() != nullptr) {
    if (OB_FAIL(base_version_filter_->filter(row, filtered))) {
      STORAGE_LOG(WARN, "failed to filter base version filter", K(ret));
    }
  }

  return ret;
}

OB_INLINE int ObMDSFilterMgr::do_sstable_level_filter(const ObSSTable &table,
                                                      bool &is_filtered) const
{
  int ret = OB_SUCCESS;

  is_filtered = false;

  const ObTTLFilter *ttl_filter = get_ttl_filter();
  if (ttl_filter != nullptr) {
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(table.get_meta(meta_handle))) {
      STORAGE_LOG(WARN, "failed to get sstable meta", KR(ret), K(table));
    } else {
      ObAggRowCachedReader agg_row_cached_reader;
      sql::ObBoolMask fal_desc;
      ObSkipIndexExtraParam extra_param(
          /* min_scn */   meta_handle.get_sstable_meta().get_min_merged_trans_version(),
          /* max_scn */   meta_handle.get_sstable_meta().get_upper_trans_version(),
          /* row_count */ meta_handle.get_sstable_meta().get_row_count()
      );
      if (meta_handle.get_sstable_meta().has_sstable_skip_index()
          && OB_FAIL(agg_row_cached_reader.init(meta_handle.get_sstable_meta().get_sstable_skip_index_buf(), meta_handle.get_sstable_meta().get_sstable_skip_index_size()))) {
        STORAGE_LOG(WARN, "failed to init agg row cached reader", KR(ret), K(table));
      } else if (OB_FAIL(ttl_filter->skip_index_filter(agg_row_cached_reader, fal_desc, extra_param))) {
        STORAGE_LOG(WARN, "failed to do skip index ttl filter", KR(ret), K(table));
      } else if (fal_desc.is_always_false()) {
        is_filtered = true;
      }

      // TODO(menglan): below log is used for feature test & debug, should remove it after merge
      STORAGE_LOG(TRACE, "do sstable level ttl filter", K(extra_param), K(fal_desc), K(table));
    }
  }

  return ret;
}

}
}

#endif
