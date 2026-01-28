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

#include "ob_ttl_filter.h"
#include "storage/access/ob_mds_filter_mgr.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{

using namespace sql;

namespace storage
{

ObTTLFilter::~ObTTLFilter()
{
  if (nullptr != single_node_) {
    single_node_->~ObTTLWhiteFilterNode();
  }
  if (nullptr != single_executor_) {
    single_executor_->~ObTTLWhiteFilterExecutor();
  }
  if (nullptr != ttl_filter_node_) {
    ttl_filter_node_->~ObTTLAndFilterNode();
  }
  if (nullptr != ttl_filter_executor_) {
    ttl_filter_executor_->~ObTTLAndFilterExecutor();
  }
}

int ObTTLFilter::init(ObTablet &tablet,
                      const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                      const ObIArray<ObColDesc> &cols_desc,
                      const ObIArray<ObColumnParam *> *cols_param,
                      const ObVersionRange &read_version_range)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret), KPC(this));
  } else if (OB_FAIL(ttl_filter_info_mgr_.init(ttl_info_allocator_,
                                               tablet,
                                               split_extra_tablet_handles,
                                               read_version_range,
                                               true))) {
    LOG_WARN("Fail to init ttl filter info mgr", KR(ret), K(tablet), K(read_version_range));
  } else if (OB_FAIL(init(tablet.get_rowkey_read_info().get_schema_rowkey_count(),
                          cols_desc,
                          cols_param,
                          ttl_filter_info_mgr_))) {
    LOG_WARN("Fail to init ttl filter", KR(ret), K(tablet), K(read_version_range));
  } else {
    schema_rowkey_cnt_ = tablet.get_rowkey_read_info().get_schema_rowkey_count();
    is_inited_ = true;
  }

  return ret;
}

void ObTTLFilter::reuse()
{
  ttl_filter_info_mgr_.reset();
  ttl_filter_info_array_.reset();

  use_single_ttl_filter_ = false;
  if (OB_NOT_NULL(ttl_filter_executor_)) {
    ttl_filter_executor_->reuse();
  }
  ttl_info_allocator_.reuse();

  filter_col_idx_array_.reset();
  filter_val_.reset();
  schema_rowkey_cnt_ = -1;
}

int ObTTLFilter::switch_info(ObTablet &tablet,
                             const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                             const ObIArray<ObColDesc> &cols_desc,
                             const ObIArray<ObColumnParam *> *cols_param,
                             const ObVersionRange &read_version_range)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", KR(ret));
  } else if (OB_FAIL(ttl_filter_info_mgr_.init(ttl_info_allocator_,
                                               tablet,
                                               split_extra_tablet_handles,
                                               read_version_range,
                                               true))) {
    LOG_WARN("Fail to init ttl filter info mgr", KR(ret), K(tablet), K(read_version_range));
  } else if (OB_FAIL(ttl_filter_info_array_.init_for_first_creation(ttl_info_allocator_))) {
    LOG_WARN("Fail to init ttl filter info array", KR(ret));
  } else if (OB_FAIL(ttl_filter_info_mgr_.get_distinct_mds_info_array(ttl_filter_info_array_))) {
    LOG_WARN("Fail to read ttl filter info array", KR(ret), K(ttl_filter_info_mgr_));
  } else if (ttl_filter_info_array_.count() == 1) {
    use_single_ttl_filter_ = true;
    if (OB_ISNULL(single_executor_)) {
      if (!ttl_filter_info_array_.empty()
          && OB_FAIL(init_ttl_filter(cols_desc, cols_param, ttl_filter_info_array_))) {
        LOG_WARN("Fail to init ttl filter", KR(ret), K(ttl_filter_info_array_));
      }
    } else {
      if (OB_FAIL(single_executor_->switch_info(
              *ttl_filter_info_array_.at(0) /* not null check in below get_array */, cols_desc))) {
        LOG_WARN("Fail to switch ttl filter", KR(ret), K(ttl_filter_info_array_));
      }
    }
  } else {
    use_single_ttl_filter_ = false;
    if (OB_ISNULL(ttl_filter_executor_)) {
      if (!ttl_filter_info_array_.empty()
          && OB_FAIL(init_ttl_filter(cols_desc, cols_param, ttl_filter_info_array_))) {
        LOG_WARN("Fail to init ttl filter", KR(ret), K(ttl_filter_info_array_));
      }
    } else {
      ttl_filter_executor_->reuse();
      if (!ttl_filter_info_array_.empty()
          && OB_FAIL(ttl_filter_executor_->switch_info(
              mds_filter_mgr_.get_filter_factory(), cols_desc, ttl_filter_info_array_))) {
        LOG_WARN("Fail to switch ttl filter", KR(ret), K(ttl_filter_info_array_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_filter_helper_arrays(ttl_filter_info_array_))) {
    LOG_WARN("Fail to build filter helper arrays", KR(ret));
  } else {
    schema_rowkey_cnt_ = tablet.get_rowkey_read_info().get_schema_rowkey_count();
  }

  return ret;
}

int ObTTLFilter::init(const int64_t schema_rowkey_cnt,
                      const ObIArray<ObColDesc> &cols_desc,
                      const ObIArray<ObColumnParam *> *cols_param,
                      const ObTTLFilterInfoDistinctMgr &mds_info_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_UNLIKELY(!mds_info_mgr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid mds info", KR(ret), K(mds_info_mgr));
  } else if (OB_FAIL(ttl_filter_info_array_.init_for_first_creation(ttl_info_allocator_))) {
    LOG_WARN("Fail to init ttl filter info array", KR(ret));
  } else if (OB_FAIL(mds_info_mgr.get_distinct_mds_info_array(ttl_filter_info_array_))) {
    LOG_WARN("Fail to read ttl filter info array", KR(ret));
  } else if (!ttl_filter_info_array_.empty() && OB_FAIL(init_ttl_filter(cols_desc, cols_param, ttl_filter_info_array_))) {
    LOG_WARN("Fail to init ttl filter", KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_filter_helper_arrays(ttl_filter_info_array_))) {
    LOG_WARN("Fail to build filter col idx array", KR(ret));
  } else {
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    is_inited_ = true;
  }

  return ret;
}

int ObTTLFilter::init_ttl_filter(const ObIArray<ObColDesc> &cols_desc,
                                 const ObIArray<ObColumnParam *> *cols_param,
                                 const ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterNode *node = nullptr;
  ObPushdownFilterExecutor *executor = nullptr;
  ObPushdownFilterFactory &filter_factory = mds_filter_mgr_.get_filter_factory();
  ObPushdownOperator &op = mds_filter_mgr_.get_pushdown_operator();

  if (ttl_filter_info_array_.count() == 1) {
    use_single_ttl_filter_ = true;

    if (OB_UNLIKELY(single_node_ != nullptr || single_executor_ != nullptr || ttl_filter_info_array.at(0) == nullptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected state, single node or executor should be nullptr", KR(ret), K(single_node_), K(single_executor_), K(ttl_filter_info_array));
    } else if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_WHITE_FILTER, 0, node))) {
      LOG_WARN("Fail to alloc ttl white filter node", KR(ret));
    } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR, 0, *node, executor, op))) {
      LOG_WARN("Fail to alloc ttl white filter executor", KR(ret));
    } else if (OB_FALSE_IT(single_node_ = static_cast<ObTTLWhiteFilterNode *>(node))) {
    } else if (OB_FALSE_IT(single_executor_ = static_cast<ObTTLWhiteFilterExecutor *>(executor))) {
    } else if (OB_FAIL(single_executor_->init(*ttl_filter_info_array.at(0), cols_desc))) {
      LOG_WARN("Fail to init ttl white filter executor", KR(ret));
    }

  } else {
    use_single_ttl_filter_ = false;

    if (OB_UNLIKELY(ttl_filter_node_ != nullptr || ttl_filter_executor_ != nullptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected state, ttl node or executor should be nullptr", KR(ret));
    } else if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_AND_FILTER, 0, node))) {
      LOG_WARN("Fail to alloc ttl filter node", KR(ret));
    } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TTL_AND_FILTER_EXECUTOR, 0, *node, executor, op))) {
      LOG_WARN("Fail to alloc ttl filter executor", KR(ret));
    } else if (OB_FALSE_IT(ttl_filter_node_ = static_cast<ObTTLAndFilterNode *>(node))) {
    } else if (OB_FALSE_IT(ttl_filter_executor_ = static_cast<ObTTLAndFilterExecutor *>(executor))) {
    } else if (OB_FAIL(ttl_filter_executor_->init(filter_factory, cols_desc, cols_param, ttl_filter_info_array))) {
      LOG_WARN("Fail to init ttl filter executor", KR(ret));
    }

  }

  return ret;
}

int ObTTLFilter::build_filter_helper_arrays(const ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  filter_col_idx_array_.reset();
  filter_val_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ttl_filter_info_array.count(); ++i) {
    if (OB_FAIL(filter_col_idx_array_.push_back(ttl_filter_info_array.at(i)->ttl_filter_col_idx_))) {
      LOG_WARN("Fail to push back filter col idx", KR(ret), K(ttl_filter_info_array));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(filter_val_.init(ttl_filter_info_array))) {
    LOG_WARN("Fail to init filter val", KR(ret), K(filter_val_));
  }

  return ret;
}

int ObTTLFilter::check_filter_row_complete(const ObDatumRow &row, bool &complete) const
{
  int ret = OB_SUCCESS;

  complete = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_col_idx_array_.count(); ++i) {
    int64_t filter_col_idx = filter_col_idx_array_.at(i);
    if (OB_UNLIKELY(filter_col_idx < 0 || filter_col_idx >= row.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected state", K(ret), K(filter_col_idx), K(row));
    } else if (row.storage_datums_[filter_col_idx].is_nop()) {
      complete = false;
      break;
    }
  }

  return ret;
}

int ObTTLFilter::init_ttl_filter_for_unittest(const int64_t schema_rowkey_cnt,
                                              const ObIArray<ObColDesc> &cols_desc,
                                              const ObIArray<ObColumnParam *> *cols_param,
                                              const ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_FAIL(ttl_filter_info_array_.init_for_first_creation(ttl_info_allocator_))) {
    LOG_WARN("Fail to init ttl filter info array", KR(ret));
  } else if (OB_FAIL(ttl_filter_info_array_.assign(ttl_info_allocator_, ttl_filter_info_array))) {
    LOG_WARN("Fail to assign ttl filter info array", KR(ret));
  } else if (OB_FAIL(init_ttl_filter(cols_desc, cols_param, ttl_filter_info_array))) {
    LOG_WARN("Fail to init ttl filter", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTTLFilterFactory::build_ttl_filter(ObTablet &tablet,
                                         const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                                         const ObIArray<ObColDesc> &cols_desc,
                                         const ObIArray<ObColumnParam *> *cols_param,
                                         const ObVersionRange &read_version_range,
                                         ObMDSFilterMgr &mds_filter_mgr,
                                         ObTTLFilter *&ttl_filter)
{
  int ret = OB_SUCCESS;

  bool contain_ttl_info = true;
  ObIAllocator *outer_allocator = mds_filter_mgr.get_outer_allocator();

  if (OB_UNLIKELY(nullptr == outer_allocator || !read_version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(outer_allocator), K(read_version_range));
  } else if (FALSE_IT(tablet.check_ttl_info_state(read_version_range, contain_ttl_info))) {
  } else if (!contain_ttl_info) {
    // if ttl_filter is not inited, we need to skip build ttl filter
    // if ttl_filter is inited, we should clear it (reuse for next time use)
    if (nullptr != ttl_filter) {
      ttl_filter->reuse();
    }
  } else if (nullptr == ttl_filter) {
    if (OB_ISNULL(ttl_filter = OB_NEWx(ObTTLFilter, outer_allocator, mds_filter_mgr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc ttl filter", KR(ret));
    } else if (OB_FAIL(ttl_filter->init(tablet,
                                        split_extra_tablet_handles,
                                        cols_desc,
                                        cols_param,
                                        read_version_range))) {
      LOG_WARN("Fail to init ttl filter", KR(ret));
    }

    if (OB_FAIL(ret) && nullptr != ttl_filter) {
      ttl_filter->~ObTTLFilter();
      outer_allocator->free(ttl_filter);
      ttl_filter = nullptr;
    }
  } else {
    if (OB_FALSE_IT(ttl_filter->reuse())) {
    } else if (OB_FAIL(ttl_filter->switch_info(tablet,
                                               split_extra_tablet_handles,
                                               cols_desc,
                                               cols_param,
                                               read_version_range))) {
      LOG_WARN("Fail to switch ttl filter", KR(ret));
    }
  }

  return ret;
}

void ObTTLFilterFactory::destroy_ttl_filter(ObTTLFilter *&ttl_filter)
{
  if (nullptr != ttl_filter) {
    ObIAllocator *outer_allocator = ttl_filter->get_mds_filter_mgr().get_outer_allocator();
    ttl_filter->~ObTTLFilter();
    if (OB_NOT_NULL(outer_allocator)) {
      outer_allocator->free(ttl_filter);
    } else {
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("destroy_ttl_filter must used with build_ttl_filter. but outer_allocator is nullptr, which is unexpected", KR(ret), KPC(ttl_filter));
    }
    ttl_filter = nullptr;
  }
}

} // namespace storage
} // namespace oceanbase