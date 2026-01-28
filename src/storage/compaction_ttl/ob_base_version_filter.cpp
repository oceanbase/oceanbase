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

#include "ob_base_version_filter.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/access/ob_mds_filter_mgr.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

ObBaseVersionFilter::~ObBaseVersionFilter()
{
  if (nullptr != base_version_filter_node_) {
    base_version_filter_node_->~ObBaseVersionFilterNode();
    base_version_filter_node_ = nullptr;
  }
  if (nullptr != base_version_filter_executor_) {
    base_version_filter_executor_->~ObBaseVersionFilterExecutor();
    base_version_filter_executor_ = nullptr;
  }
}

int ObBaseVersionFilter::init_filter_executor(const int64_t schema_rowkey_cnt, const int64_t base_version)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterNode *node = nullptr;
  ObPushdownFilterExecutor *executor = nullptr;
  ObPushdownFilterFactory &filter_factory = mds_filter_mgr_.get_filter_factory();
  ObPushdownOperator &op = mds_filter_mgr_.get_pushdown_operator();

  if (OB_UNLIKELY(base_version_filter_node_ != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Base version filter node already inited", KR(ret));
  } else if (OB_FAIL(filter_factory.alloc(PushdownFilterType::BASE_VERSION_FILTER, 0, node))) {
    LOG_WARN("Fail to alloc base version filter node", KR(ret));
  } else if (OB_FAIL(filter_factory.alloc(
                 PushdownExecutorType::BASE_VERSION_FILTER_EXECUTOR, 0, *node, executor, op))) {
    LOG_WARN("Fail to alloc base version filter executor", KR(ret));
  } else if (OB_FALSE_IT(base_version_filter_node_ = static_cast<sql::ObBaseVersionFilterNode *>(node))) {
  } else if (OB_FALSE_IT(base_version_filter_executor_ = static_cast<sql::ObBaseVersionFilterExecutor *>(executor))) {
  } else if (OB_FAIL(base_version_filter_executor_->init(schema_rowkey_cnt, base_version))) {
    LOG_WARN("Fail to init base version filter executor", KR(ret), K(schema_rowkey_cnt), K(base_version));
  }

  return ret;
}

int ObBaseVersionFilter::init(ObTablet &tablet, const ObVersionRange &read_version_range)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_FAIL(init_filter_executor(tablet.get_rowkey_read_info().get_schema_rowkey_count(),
                                          read_version_range.base_version_))) {
    LOG_WARN("Fail to init filter executor", KR(ret));
  } else {
    schema_rowkey_cnt_ = tablet.get_rowkey_read_info().get_schema_rowkey_count();
    is_inited_ = true;
  }

  return ret;
}

void ObBaseVersionFilter::reuse()
{
  schema_rowkey_cnt_ = -1;
}

int ObBaseVersionFilter::switch_info(ObTablet &tablet, const ObVersionRange &read_version_range)
{
  int ret = OB_SUCCESS;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (FALSE_IT(schema_rowkey_cnt_ = tablet.get_rowkey_read_info().get_schema_rowkey_count())) {
  } else if (base_version_filter_node_ != nullptr) {
    if (OB_FAIL(base_version_filter_executor_->switch_info(schema_rowkey_cnt_, read_version_range.base_version_))) {
      LOG_WARN("Fail to switch info", KR(ret));
    }
  } else {
    if (OB_FAIL(init_filter_executor(schema_rowkey_cnt_, read_version_range.base_version_))) {
      LOG_WARN("Fail to init filter executor", KR(ret));
    }
  }

  return ret;
}

int ObBaseVersionFilter::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (OB_UNLIKELY(row.count_ <= schema_rowkey_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row count", K(ret), K(row.count_), K(schema_rowkey_cnt_));
  } else if (OB_FAIL(base_version_filter_executor_->filter(row, filtered))) {
    LOG_WARN("Fail to filter", KR(ret), K(row), KPC(this));
  }

  return ret;
}

int ObBaseVersionFilter::check_filter_row_complete(const blocksstable::ObDatumRow &row, bool &complete) const
{
  int ret = OB_SUCCESS;

  complete = true;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (OB_UNLIKELY(row.count_ <= schema_rowkey_cnt_)) {
    complete = false;
  } else if (OB_UNLIKELY(row.storage_datums_[schema_rowkey_cnt_].is_null()
                         || row.storage_datums_[schema_rowkey_cnt_].is_nop())) {
    complete = false;
  }

  return ret;
}

int ObBaseVersionFilter::init_base_version_filter_for_unittest(const int64_t schema_rowkey_cnt,
                                                               const int64_t base_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_FAIL(init_filter_executor(schema_rowkey_cnt, base_version))) {
    LOG_WARN("Fail to init filter executor", KR(ret));
  } else {
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    is_inited_ = true;
  }

  return ret;
}

int ObBaseVersionFilterFactory::build_base_version_filter(
    ObTablet &tablet,
    const common::ObVersionRange &read_version_range,
    ObMDSFilterMgr &mds_filter_mgr,
    ObBaseVersionFilter *&base_version_filter)
{
  int ret = OB_SUCCESS;

  ObIAllocator *outer_allocator = mds_filter_mgr.get_outer_allocator();

  if (OB_UNLIKELY(nullptr == outer_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(outer_allocator));
  } else if (nullptr == base_version_filter) {
    if (OB_ISNULL(base_version_filter = OB_NEWx(ObBaseVersionFilter, outer_allocator, mds_filter_mgr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc base version filter", KR(ret));
    } else if (OB_FAIL(base_version_filter->init(tablet, read_version_range))) {
      LOG_WARN("Fail to init base version filter", KR(ret));
    }

    if (OB_FAIL(ret) && nullptr != base_version_filter) {
      base_version_filter->~ObBaseVersionFilter();
      outer_allocator->free(base_version_filter);
      base_version_filter = nullptr;
    }

  } else {
    if (OB_FALSE_IT(base_version_filter->reuse())) {
    } else if (OB_FAIL(base_version_filter->switch_info(tablet, read_version_range))) {
      LOG_WARN("Fail to switch info", KR(ret));
    }
  }

  return ret;
}

void ObBaseVersionFilterFactory::destroy_base_version_filter(
    ObBaseVersionFilter *&base_version_filter)
{
  if (nullptr != base_version_filter) {
    ObIAllocator *outer_allocator = base_version_filter->get_mds_filter_mgr().get_outer_allocator();
    base_version_filter->~ObBaseVersionFilter();
    if (OB_NOT_NULL(outer_allocator)) {
      outer_allocator->free(base_version_filter);
    } else {
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("destroy_base_version_filter must used with build_base_version_filter. but outer_allocator is nullptr, which is unexpected", KR(ret), KPC(base_version_filter));
    }
    base_version_filter = nullptr;
  }
}
}
}