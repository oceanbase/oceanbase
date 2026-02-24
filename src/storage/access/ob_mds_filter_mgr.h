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

#ifndef OB_STORAGE_ACCESS_MDS_FILTER_MGR_H
#define OB_STORAGE_ACCESS_MDS_FILTER_MGR_H

#include "storage/compaction_ttl/ob_base_version_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "sql/engine/ob_exec_context.h"

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
  ObMDSFilterMgr(ObIAllocator *outer_allocator)
      : outer_allocator_(outer_allocator),
        filter_allocator_("MDSFilterMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        exec_ctx_(filter_allocator_),
        eval_ctx_(exec_ctx_),
        expr_spec_(filter_allocator_),
        op_(eval_ctx_, expr_spec_),
        filter_factory_(&filter_allocator_),
        and_filter_node_(nullptr),
        and_filter_executor_(nullptr),
        and_filter_flags_(),
        sql_pd_filter_in_final_(nullptr),
        final_root_filter_(nullptr),
        truncate_partition_filter_(nullptr),
        ttl_filter_(nullptr),
        base_version_filter_(nullptr),
        build_filter_flags_(),
        combined_child_count_(-1)
  {
  }

  ~ObMDSFilterMgr();

  OB_INLINE ObIAllocator *get_outer_allocator() const { return outer_allocator_; }
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
                              const ObIArray<ObColDesc> &cols_desc,
                              const ObIArray<ObColumnParam *> *cols_param,
                              const ObVersionRange &read_version_range,
                              const ObMDSFilterFlags &mds_filter_flags);

  OB_INLINE int filter(const ObDatumRow &row, bool &filtered, bool check_filter = true, bool check_version = false) const;

  // There will be a sstable level skip-index row in ObSSTableMeta after 4.5.1.
  // We can use it to filter a whole sstable before construct query iterators.
  OB_INLINE int do_sstable_level_filter(const ObSSTable &table, bool &is_filtered) const;

  int check_filter_row_complete(const ObDatumRow &row, bool &complete) const;

  int combine_to_filter_tree_with_check(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter);

  OB_INLINE void set_uncombined()
  {
    combined_child_count_ = -1;
  }

  OB_INLINE bool is_empty() const { return build_filter_flags_.is_empty(); }
  OB_INLINE void set_should_combine_filters_flag(const ObMDSFilterFlags &mds_filter_flags)
  {
    build_filter_flags_ = mds_filter_flags;
  }
  OB_INLINE void clear_should_combine_filters_flag()
  {
    build_filter_flags_.reset();
  }

  OB_INLINE sql::ObPushdownFilterFactory &get_filter_factory() { return filter_factory_; }
  OB_INLINE sql::ObPushdownOperator &get_pushdown_operator() { return op_; }

  TO_STRING_KV(KPC(truncate_partition_filter_),
               KPC(ttl_filter_),
               KPC(base_version_filter_),
               K(build_filter_flags_),
               K(and_filter_flags_),
               K(and_filter_executor_),
               K(combined_child_count_),
               K(final_root_filter_),
               K(sql_pd_filter_in_final_));

private:
  int combine_to_filter_tree_without_check(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter);

private:
  // the mds filter mgr is always allocated by outer allocator in query path, and the same as other filter
  // in compaction path, this is nullptr
  ObIAllocator *outer_allocator_;

  // this allocator is used to allocate and filter
  ObArenaAllocator filter_allocator_;

  // below struct is shared by all mds filter(ttl filter, base version filter, truncate filter)
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObPushdownExprSpec expr_spec_;
  sql::ObPushdownOperator op_;
  sql::ObPushdownFilterFactory filter_factory_;

  // and filter is used to combine sql pushdown filter and mds filter
  //   - and_filter_flags: used to indicate which mds filter has combined in and filter
  sql::ObPushdownFilterNode *and_filter_node_;
  sql::ObPushdownFilterExecutor *and_filter_executor_;
  ObMDSFilterFlags and_filter_flags_;

  // sql_pd_filter_in_final_: the sql pushdown filter in final root filter (which is the sql_pushdown_filter_ in last combine function)
  // final_root_filter_: the final root filter after combine all filters (it maybe and_filter_executor_ or sql_pushdown_filter_ or single mds filter)
  sql::ObPushdownFilterExecutor *sql_pd_filter_in_final_;
  sql::ObPushdownFilterExecutor *final_root_filter_;

  ObTruncatePartitionFilter *truncate_partition_filter_;
  ObTTLFilter *ttl_filter_;
  ObBaseVersionFilter *base_version_filter_;

  // The build_filters_flag_ means which mds filter should be combined into and filter.
  // In rescan path (change to next partition), it's not the same as and_filter_flags_ (which indicate the last partition's use).
  ObMDSFilterFlags build_filter_flags_;
  int32_t combined_child_count_; // negative means not combined, positive means combined
};

class ObMDSFilterMgrFactory
{
public:
  static int build_mds_filter_mgr(ObTablet &tablet,
                                  const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                                  const ObIArray<ObColDesc> &cols_desc,
                                  const ObIArray<ObColumnParam *> *cols_param,
                                  const ObVersionRange &read_version_range,
                                  ObIAllocator *outer_allocator,
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

  // We can do more pushdown filter check here. Now, we only check the ttl filter
  is_filtered = false;
  const ObTTLFilter *ttl_filter = get_ttl_filter();
  if (OB_NOT_NULL(ttl_filter)) {
    ObStorageDatum datum;
    datum.set_int(table.get_upper_trans_version());
    if (OB_FAIL(ttl_filter->rowscn_filter(datum, is_filtered))) {
      STORAGE_LOG(WARN, "failed to filter by rowscn", KR(ret), K(datum), K(is_filtered));
    }
  }

  return ret;
}
}
}

#endif
