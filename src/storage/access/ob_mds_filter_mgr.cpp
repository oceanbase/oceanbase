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
    ObTruncatePartitionFilterFactory::destroy_truncate_partition_filter(truncate_partition_filter_);
  }
  if (ttl_filter_ != nullptr) {
    ObTTLFilterFactory::destroy_ttl_filter(ttl_filter_);
  }
  if (base_version_filter_ != nullptr) {
    ObBaseVersionFilterFactory::destroy_base_version_filter(base_version_filter_);
  }
}

OB_INLINE int ObMDSFilterMgr::build_filters(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObVersionRange &read_version_range,
    const ObMDSFilterFlags &mds_filter_flags)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::TRUNCATE_FILTER)) {
    if (OB_FAIL(ObTruncatePartitionFilterFactory::build_truncate_partition_filter(
            tablet,
            split_extra_tablet_handles,
            cols_desc,
            cols_param,
            read_version_range,
            *this,
            truncate_partition_filter_))) {
      LOG_WARN("failed to build truncate partition filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::TTL_FILTER)) {
    if (OB_FAIL(ObTTLFilterFactory::build_ttl_filter(tablet,
                                                     split_extra_tablet_handles,
                                                     cols_desc,
                                                     cols_param,
                                                     read_version_range,
                                                     *this,
                                                     ttl_filter_))) {
      LOG_WARN("failed to build ttl filter", K(ret));
    }
  }

  if (OB_SUCC(ret) && mds_filter_flags.has_type(MDSFilterType::BASE_VERSION_FILTER)) {
    if (OB_FAIL(ObBaseVersionFilterFactory::build_base_version_filter(
            tablet, read_version_range, *this, base_version_filter_))) {
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

int ObMDSFilterMgr::combine_to_filter_tree_with_check(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter)
{
  int ret = OB_SUCCESS;

  // the reuse logic makes combine complex, but it's worth to reuse truncate filter which has too
  // complex filter tree struct.
  //
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
  // │                                  has_combined                             │
  // │ access_context->combine_filter() ──────────────-►  skip                   │
  // │                                  set_uncombined                           │
  // │                                 ────────────────►  rebuild                │
  // │                                                                           │
  // └───────────────────────────────────────────────────────────────────────────┘


  //
  // There are three cases when reuse (combined_child_count_ >= 0):
  //
  // 1. table rescan (just change range)
  //   - it will directly call multiple_merge.open() to avoid read mds again
  // 2. table rescan (change partition, like switch_param)
  //   - the mds filter info must be re-read
  //   - we must recombine filters because mds filter type maybe change
  // 3. table refresh
  //   - the mds filter info must be re-read
  //   - we must recombine filters because mds filter type maybe change

  if (combined_child_count_ >= 0 && sql_pd_filter_in_final_ == sql_pushdown_filter && and_filter_flags_ == build_filter_flags_) {
    //! To make thing simple, we must ensure that sql pushdown filter and mds_filter is the same as last combine,
    //! Otherwise, we should re-combine all filters
    root_filter = final_root_filter_;
  } else if (OB_FAIL(combine_to_filter_tree_without_check(root_filter, sql_pushdown_filter))) {
    LOG_WARN("failed to combine to filter tree", K(ret), KPC(root_filter));
  }

  return ret;
}


int ObMDSFilterMgr::combine_to_filter_tree_without_check(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter)
{
  int ret = OB_SUCCESS;

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
      // only child count not changed, we can reuse the and filter node and executor
      if (nullptr == and_filter_node_ || and_filter_node_->n_child_ != n_child) {
        if (nullptr != and_filter_node_) {
          // notice that the child lifetime is not same as the and filter node and executor
          and_filter_executor_->set_childs(0, nullptr);
          and_filter_executor_->~ObPushdownFilterExecutor();
          and_filter_node_->~ObPushdownFilterNode();
          and_filter_node_ = nullptr;
          and_filter_executor_ = nullptr;
        }

        if (OB_FAIL(filter_factory_.alloc(
                sql::PushdownFilterType::AND_FILTER, n_child, and_filter_node_))) {
          LOG_WARN("Failed to alloc pushdown and filter node", K(ret));
        } else if (OB_FAIL(filter_factory_.alloc(sql::PushdownExecutorType::AND_FILTER_EXECUTOR,
                                                 n_child,
                                                 *and_filter_node_,
                                                 and_filter_executor_,
                                                 op_))) {
          LOG_WARN("Failed to alloc pushdown and filter executor", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < n_child; ++i) {
          and_filter_executor_->set_child(i, wait_for_combine.at(i));
        }
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
      // TODO(menglan): for debug, make it to trace level
      LOG_TRACE("[MDS INFO] combine with mds filter tree",
                K(and_filter_flags_),
                K(build_filter_flags_),
                K(combined_child_count_),
                K(n_child),
                KP(final_root_filter_),
                KP(root_filter),
                KP(sql_pd_filter_in_final_),
                KP(sql_pushdown_filter),
                KPC(truncate_partition_filter_),
                KPC(ttl_filter_),
                KPC(base_version_filter_));

      and_filter_flags_ = build_filter_flags_;
      combined_child_count_ = n_child;
      final_root_filter_ = root_filter;
      sql_pd_filter_in_final_ = sql_pushdown_filter;
    }
  }

  return ret;
}

int ObMDSFilterMgrFactory::build_mds_filter_mgr(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObVersionRange &read_version_range,
    ObIAllocator *outer_allocator,
    ObMDSFilterMgr *&mds_filter_mgr,
    const ObMDSFilterFlags &mds_filter_flags)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(outer_allocator == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(outer_allocator));
  } else if (mds_filter_flags.is_empty()) {
    if (nullptr != mds_filter_mgr) {
      mds_filter_mgr->set_uncombined();
      mds_filter_mgr->set_should_combine_filters_flag(mds_filter_flags);
    }
  } else {
    if (nullptr == mds_filter_mgr) {
      if (OB_ISNULL(mds_filter_mgr = OB_NEWx(ObMDSFilterMgr, outer_allocator, outer_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc mds filter mgr", KR(ret));
      }
    }

    if (FAILEDx(mds_filter_mgr->build_filters(tablet,
                                              split_extra_tablet_handles,
                                              cols_desc,
                                              cols_param,
                                              read_version_range,
                                              mds_filter_flags))) {
      LOG_WARN("Failed to build filters", KR(ret));
    }

    if (OB_FAIL(ret)) {
      OB_DELETEx(ObMDSFilterMgr, outer_allocator, mds_filter_mgr);
      mds_filter_mgr = nullptr;
    }
  }

  return ret;
}

void ObMDSFilterMgrFactory::destroy_mds_filter_mgr(ObMDSFilterMgr *&mds_filter_mgr)
{
  if (nullptr != mds_filter_mgr) {
    ObIAllocator *outer_allocator = mds_filter_mgr->get_outer_allocator();
    mds_filter_mgr->~ObMDSFilterMgr();
    if (OB_NOT_NULL(outer_allocator)) {
      outer_allocator->free(mds_filter_mgr);
    } else {
      // can't return error here, because it maybe used in class destructor
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("destroy_mds_filter_mgr must used with build_mds_filter_mgr. but outer_allocator is nullptr, which is unexpected", KR(ret), KPC(mds_filter_mgr), K(lbt()));
    }
    mds_filter_mgr = nullptr;
  }
}

}
}
