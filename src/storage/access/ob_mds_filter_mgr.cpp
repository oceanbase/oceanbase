/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_mds_filter_mgr.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

#define RELEASE_FILTER_PTR_WHEN_FAILED(type, ptr)                                                  \
  if (nullptr != ptr) {                                                                            \
    ptr->~type();                                                                                  \
    ptr = nullptr;                                                                                 \
  }

ObMDSFilterMgr::~ObMDSFilterMgr()
{
  if (nullptr != and_filter_executor_) {
    // should not destruct its children as they are not alloced here
    //                          AND
    // root_filter(not here) truncate_filter ttl_filter base_version_filter
    and_filter_executor_->set_childs(0, nullptr);
    and_filter_executor_->~ObPushdownFilterExecutor();
    and_filter_executor_ = nullptr;
  }
  if (nullptr != and_filter_node_) {
    and_filter_node_->~ObPushdownFilterNode();
    and_filter_node_ = nullptr;
  }
  if (truncate_partition_filter_ != nullptr) {
    truncate_partition_filter_->~ObTruncatePartitionFilter();
  }
  if (ttl_filter_ != nullptr) {
    ttl_filter_->~ObTTLFilter();
  }
  if (base_version_filter_ != nullptr) {
    base_version_filter_->~ObBaseVersionFilter();
  }

  // Don't need to free them because they are allocated by self's ArenaAllocator
}

OB_INLINE int ObMDSFilterMgr::build_filters(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObITableReadInfo &read_info,
    const ObVersionRange &read_version_range,
    const ObMDSFilterFlags &mds_filter_flags)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::TRUNCATE_FILTER)) {
    if (OB_FAIL(ObTruncatePartitionFilterFactory::build_truncate_partition_filter(
            tablet,
            split_extra_tablet_handles,
            read_info.get_columns_desc(),
            read_info.get_columns(),
            read_version_range,
            filter_allocator_,
            truncate_partition_filter_))) {
      LOG_WARN("failed to build truncate partition filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::TTL_FILTER)) {
    if (OB_FAIL(ObTTLFilterFactory::build_ttl_filter(tablet,
                                                     split_extra_tablet_handles,
                                                     read_info,
                                                     read_version_range,
                                                     filter_allocator_,
                                                     ttl_filter_))) {
      LOG_WARN("failed to build ttl filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::BASE_VERSION_FILTER)) {
    if (OB_FAIL(ObBaseVersionFilterFactory::build_base_version_filter(
            tablet, read_version_range, filter_allocator_, base_version_filter_))) {
      LOG_WARN("failed to build base version filter", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // Determine which filter should be combined
    // valid condiction:
    //    + filter pointer is not nullptr
    //    + build_filter_flags_ is set (which means we should check mds filter info)
    //    + filter is not empty (which means mds filter info is not empty)
    build_filter_flags_
        .reset()
        .add_type_if(truncate_partition_filter_ != nullptr
                         && mds_filter_flags.has_type(MDSFilterType::TRUNCATE_FILTER)
                         && truncate_partition_filter_->is_normal_filter(),
                     MDSFilterType::TRUNCATE_FILTER)
        .add_type_if(ttl_filter_ != nullptr
                         && mds_filter_flags.has_type(MDSFilterType::TTL_FILTER)
                         && !ttl_filter_->is_empty(),
                     MDSFilterType::TTL_FILTER)
        .add_type_if(base_version_filter_ != nullptr
                         && mds_filter_flags.has_type(MDSFilterType::BASE_VERSION_FILTER),
                     MDSFilterType::BASE_VERSION_FILTER);
  } else {
    clear_should_combine_filters_flag();
  }

  return ret;
}

int ObMDSFilterMgr::check_filter_row_complete(const ObDatumRow &row, bool &complete) const
{
  int ret = OB_SUCCESS;

  complete = true;

  if (OB_SUCC(ret) && complete && get_truncate_partition_filter() != nullptr) {
    if (OB_FAIL(truncate_partition_filter_->check_filter_row_complete(row, complete))) {
      LOG_WARN("failed to check filter row complete", K(ret));
    }
  }

  if (OB_SUCC(ret) && complete && get_ttl_filter() != nullptr) {
    if (OB_FAIL(ttl_filter_->check_filter_row_complete(row, complete))) {
      LOG_WARN("failed to check filter row complete", K(ret));
    }
  }

  if (OB_SUCC(ret) && complete && get_base_version_filter() != nullptr) {
    if (OB_FAIL(base_version_filter_->check_filter_row_complete(row, complete))) {
      LOG_WARN("failed to check filter row complete", K(ret));
    }
  }

  return ret;
}

int ObMDSFilterMgr::combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter)
{
  int ret = OB_SUCCESS;

  // the whole reuse procedure:
  //
  // ┌──────────────────────────────────────────────────────────────────────────┐
  // │                                               ┌────────┐                 │
  // │                                               │Truncate│◄──┐  ◄──────────┼─────┐
  // │ access_context->init()                        └────────┘   │             │     │
  // │                                               ┌────────┐   │             │     │
  // │ access_context->prepare_mds_filter() ───────► │  TTL   │◄──┤  ◄──────────┼─────┤
  // │                                               └────────┘   │             │     │
  // │                                               ┌────────┐   │             │     │
  // │                                               │Base Ver│◄──┤  ◄──────────┼─────┤
  // │                                               └────────┘   │             │     │
  // │                                                            │             │     │
  // │                                               ┌─────────┐  │  ┌────────┐ │     │
  // │ access_context->combine_filter()────────────► │   AND   ├──┴─►│ query  │ │     │
  // │                                               └─────────┘     └────────┘ │     │
  // └──────────────────────────────────────────────────────────────────────────┘     │
  //                                                                                  │
  // ┌───────────────────────────────────────────────────────────────────────────┐    │
  // │ MultipleMerge->switch_param/switch_table;                                 │    │
  // │                                                                           │    │
  // │                                       will directly reuse and change them │    │
  // │ access_context->prepare_mds_filter() ─────────────────────────────────────┼────┘
  // │                                                                           |
  // │                                                                           │
  // │ access_context->combine_filter() ────────────────►  rebuild               │
  // │                                                                           │
  // └───────────────────────────────────────────────────────────────────────────┘

  //
  // There are three cases when reuse (combined_child_count_ >= 0):
  //
  // 1. table rescan (just change range)
  //   - it will directly call multiple_merge.open() to avoid read mds again
  // 2. table rescan (change partition, like switch_param)
  //   - the mds filter info will be re-read
  //   - we recombine filters because mds filter type maybe change
  // 3. table refresh
  //   - the mds filter info will be re-read
  //   - we recombine filters because mds filter type maybe change

  // Step 1. add all filter into one array
  constexpr int64_t MAX_CHILD_CNT = 4;
  ObSEArray<sql::ObPushdownFilterExecutor *, MAX_CHILD_CNT> wait_for_combine;

  if (sql_pushdown_filter && OB_FAIL(wait_for_combine.push_back(sql_pushdown_filter))) {
    LOG_WARN("Failed to push back root filter", K(ret));
  } else if (get_base_version_filter() != nullptr && OB_FAIL(wait_for_combine.push_back(static_cast<sql::ObPushdownFilterExecutor *>(base_version_filter_->get_base_version_filter_executor())))) {
    LOG_WARN("Failed to push back base version filter", K(ret));
  } else if (get_ttl_filter() != nullptr && OB_FAIL(wait_for_combine.push_back(static_cast<sql::ObPushdownFilterExecutor *>(ttl_filter_->get_ttl_filter_executor())))) {
    LOG_WARN("Failed to push back ttl filter", K(ret));
  } else if (get_truncate_partition_filter() != nullptr && OB_FAIL(wait_for_combine.push_back(static_cast<sql::ObPushdownFilterExecutor *>(truncate_partition_filter_->get_truncate_filter_executor())))) {
    LOG_WARN("Failed to push back truncate filter", K(ret));
  }

  // Step 2. build And filter tree
  int64_t n_child = wait_for_combine.count();

  if (OB_FAIL(ret)) {
  } else {
    switch (n_child) {
    case 0:
      root_filter = nullptr;
      break;
    case 1:
      root_filter = wait_for_combine.at(0);
      break;
    default: {
      if (nullptr == and_filter_node_) {
        sql::ObPushdownFilterFactory filter_factory(&filter_allocator_);
        if (OB_FAIL(filter_factory.alloc(sql::PushdownFilterType::AND_FILTER, MAX_CHILD_CNT, and_filter_node_))) {
          LOG_WARN("Failed to alloc pushdown and filter node", K(ret));
        } else if (OB_FAIL(filter_factory.alloc<false>(
                       sql::PushdownExecutorType::AND_FILTER_EXECUTOR,
                       MAX_CHILD_CNT, // we alloc the maximum size of childs to avoid realloc when child count changes
                       *and_filter_node_,
                       and_filter_executor_,
                       wait_for_combine.at(0)->get_op()))) {
          LOG_WARN("Failed to alloc pushdown and filter executor", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < n_child; ++i) {
          and_filter_executor_->set_child(i, wait_for_combine.at(i));
        }
        and_filter_executor_->set_child_count(n_child);
        root_filter = and_filter_executor_;
      }

      if (OB_FAIL(ret)) {
        RELEASE_FILTER_PTR_WHEN_FAILED(ObPushdownFilterNode, and_filter_node_);
        RELEASE_FILTER_PTR_WHEN_FAILED(ObPushdownFilterExecutor, and_filter_executor_);
      }

      break;
    }
    }

    if (OB_SUCC(ret)) {
      LOG_TRACE("[MDS INFO] combine with mds filter tree",
                K(build_filter_flags_),
                K(n_child),
                KP(root_filter),
                KP(sql_pushdown_filter),
                KPC(truncate_partition_filter_),
                KPC(ttl_filter_),
                KPC(base_version_filter_));
    }
  }

  return ret;
}

// TODO(menglan): Too many parameters, maybe should use a unified struct to pass parameter
int ObMDSFilterMgrFactory::build_mds_filter_mgr(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObITableReadInfo *read_info,
    const ObVersionRange &read_version_range,
    ObIAllocator &outer_allocator,
    ObMDSFilterMgr *&mds_filter_mgr,
    const ObMDSFilterFlags &mds_filter_flags)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(read_info == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(read_info));
  } else if (mds_filter_flags.is_empty()) {
    if (nullptr != mds_filter_mgr) {
      mds_filter_mgr->set_should_combine_filters_flag(mds_filter_flags);
    }
  } else {
    if (nullptr == mds_filter_mgr) {
      if (OB_ISNULL(mds_filter_mgr = OB_NEWx(ObMDSFilterMgr, &outer_allocator, outer_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc mds filter mgr", KR(ret));
      }
    }

    if (FAILEDx(mds_filter_mgr->build_filters(tablet,
                                              split_extra_tablet_handles,
                                              *read_info,
                                              read_version_range,
                                              mds_filter_flags))) {
      LOG_WARN("Failed to build filters", KR(ret));
    }

    if (OB_FAIL(ret)) {
      OB_DELETEx(ObMDSFilterMgr, &outer_allocator, mds_filter_mgr);
      mds_filter_mgr = nullptr;
    }
  }

  return ret;
}

void ObMDSFilterMgrFactory::destroy_mds_filter_mgr(ObMDSFilterMgr *&mds_filter_mgr)
{
  if (nullptr != mds_filter_mgr) {
    ObIAllocator &outer_allocator = mds_filter_mgr->get_outer_allocator();
    mds_filter_mgr->~ObMDSFilterMgr();
    outer_allocator.free(mds_filter_mgr);
    mds_filter_mgr = nullptr;
  }
}

}
}
