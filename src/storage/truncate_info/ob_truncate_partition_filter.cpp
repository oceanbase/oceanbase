/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_truncate_partition_filter.h"
#include "share/schema/ob_list_row_values.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/access/ob_mds_filter_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;

namespace storage
{

ObTruncatePartitionFilter::ObTruncatePartitionFilter(ObMDSFilterMgr &mds_filter_mgr)
  : mds_filter_mgr_(mds_filter_mgr),
    is_inited_(false),
    filter_type_(ObTruncateFilterType::FILTER_TYPE_MAX),
    schema_rowkey_cnt_(-1),
    truncate_info_allocator_("TruncateInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    mds_info_mgr_(),
    truncate_info_array_(),
    truncate_filter_node_(nullptr),
    truncate_filter_executor_(nullptr),
    ref_column_idxs_()
{
  ref_column_idxs_.set_attr(ObMemAttr(MTL_ID(), "TruncateFilter"));
}

ObTruncatePartitionFilter::~ObTruncatePartitionFilter()
{
  if (nullptr != truncate_filter_executor_) {
    truncate_filter_executor_->~ObTruncateAndFilterExecutor();
    truncate_filter_executor_ = nullptr;
  }
  if (nullptr != truncate_filter_node_) {
    truncate_filter_node_->~ObPushdownFilterNode();
    truncate_filter_node_ = nullptr;
  }
}

int ObTruncatePartitionFilter::init(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObVersionRange &read_version_range,
    const bool has_truncate_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (FALSE_IT(schema_rowkey_cnt_ = tablet.get_rowkey_read_info().get_schema_rowkey_count())) {
  } else if (!has_truncate_info) {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
    is_inited_ = true;
  } else if (OB_FAIL(mds_info_mgr_.init(truncate_info_allocator_, tablet, split_extra_tablet_handles, read_version_range, true/*for_access*/))) {
    LOG_WARN("failed to init mds filter info mgr", KR(ret), K(read_version_range));
  } else if (mds_info_mgr_.empty()) {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
    is_inited_ = true;
  } else if (OB_FAIL(init(schema_rowkey_cnt_, cols_desc, cols_param, mds_info_mgr_))) {
    LOG_WARN("failed to init", K(ret));
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(tablet), K(cols_desc), K(read_version_range), KPC(this));
  return ret;
}

int ObTruncatePartitionFilter::init(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObTruncateInfoDistinctMgr &mds_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(mds_info_mgr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty mds info", K(ret));
  } else if (OB_FAIL(truncate_info_array_.init_for_first_creation(truncate_info_allocator_))) {
    LOG_WARN("failed to init truncate info array", KR(ret));
  } else if (OB_FAIL(mds_info_mgr.get_distinct_mds_info_array(truncate_info_array_))) {
    LOG_WARN("failed to read truncate info array", K(ret));
  } else if (OB_FAIL(init_truncate_filter(schema_rowkey_cnt, cols_desc, cols_param, truncate_info_array_))) {
    LOG_WARN("failed to init truncate filter", K(ret));
  } else {
    filter_type_ = ObTruncateFilterType::NORMAL_FILTER;
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(schema_rowkey_cnt), K(cols_desc), K(mds_info_mgr), KPC(this));
  return ret;
}

int ObTruncatePartitionFilter::switch_info(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObVersionRange &read_version_range,
    const bool has_truncate_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_truncate_info) {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
  } else if (OB_FAIL(mds_info_mgr_.init(truncate_info_allocator_, tablet, split_extra_tablet_handles, read_version_range, true/*for_access*/))) {
    LOG_WARN("failed to init mds filter info mgr", KR(ret), K(read_version_range));
  } else if (mds_info_mgr_.empty()) {
    filter_type_ = ObTruncateFilterType::EMPTY_FILTER;
  } else if (OB_FAIL(truncate_info_array_.init_for_first_creation(truncate_info_allocator_))) {
    LOG_WARN("failed to init truncate info array", KR(ret));
  } else if (OB_FAIL(mds_info_mgr_.get_distinct_mds_info_array(truncate_info_array_))) {
    LOG_WARN("failed to read truncate info array", K(ret));
  } else if (OB_UNLIKELY(schema_rowkey_cnt_ != tablet.get_rowkey_read_info().get_schema_rowkey_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not equal schema rowkey cnt", K(ret), K_(schema_rowkey_cnt), K(tablet.get_rowkey_read_info().get_schema_rowkey_count()));
  } else if (OB_ISNULL(truncate_filter_executor_)) {
    if (OB_FAIL(init_truncate_filter(schema_rowkey_cnt_, cols_desc, cols_param, truncate_info_array_))) {
      LOG_WARN("failed to init truncate filter", K(ret));
    } else {
      filter_type_ = ObTruncateFilterType::NORMAL_FILTER;
    }
  } else if (OB_FAIL(truncate_filter_executor_->switch_info(mds_filter_mgr_.get_filter_factory(), schema_rowkey_cnt_, cols_desc, truncate_info_array_))) {
    LOG_WARN("failed to init truncate filter executor", K(ret));
  } else {
    filter_type_ = ObTruncateFilterType::NORMAL_FILTER;
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(tablet), K(cols_desc), K(read_version_range), KPC(this));
  return ret;
}

void ObTruncatePartitionFilter::reuse()
{
  filter_type_ = ObTruncateFilterType::FILTER_TYPE_MAX;
  truncate_info_array_.reset();
  mds_info_mgr_.reset();
  truncate_info_allocator_.reuse();
  if (OB_NOT_NULL(truncate_filter_executor_)) {
    truncate_filter_executor_->reuse();
  }
}

int ObTruncatePartitionFilter::filter(const ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;
  filtered = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ObTruncateFilterType::NORMAL_FILTER == filter_type_) {
    if (OB_FAIL(do_normal_filter(row, filtered))) {
      LOG_WARN("failed to do normal filter", K(ret), K(row));
    }
  } else if (ObTruncateFilterType::EMPTY_FILTER == filter_type_) {
    LOG_DEBUG("[TRUNCATE INFO] filter is empty after rescan", K(*this));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state", K(ret), K_(filter_type), KPC(this));
  }
  LOG_DEBUG("[TRUNCATE INFO] filter single row", K(ret), K(row), K(filtered), KPC(this));
  return ret;
}

int ObTruncatePartitionFilter::do_normal_filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.row_flag_.is_delete() || row.row_flag_.is_lock())) {
    filtered = false;
  } else if (OB_FAIL(truncate_filter_executor_->filter(row, filtered))) {
    LOG_WARN("failed to filter", K(ret));
  }
  return ret;
}

int ObTruncatePartitionFilter::check_filter_row_complete(const blocksstable::ObDatumRow &row, bool &complete) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_normal_filter())) {
    complete = true;
  } else if (OB_UNLIKELY(ref_column_idxs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref column idx is unexpected null", KR(ret), K_(ref_column_idxs));
  } else {
    complete = true;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < ref_column_idxs_.count() && complete; ++idx) {
      const uint64_t ref_idx = ref_column_idxs_[idx];
      if (OB_UNLIKELY(ref_idx < 0 || ref_idx >= row.get_column_count()) ) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid data", KR(ret), K(idx), K(ref_idx), K(row));
      } else if (row.storage_datums_[ref_idx].is_nop()) {
        complete = false;
      }
    }
  }
  return ret;
}

int ObTruncatePartitionFilter::init_truncate_filter(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObTruncateInfoArray &array)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterExecutor *executor = nullptr;
  ObPushdownFilterFactory &filter_factory = mds_filter_mgr_.get_filter_factory();
  ObPushdownOperator &op = mds_filter_mgr_.get_pushdown_operator();

  if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TRUNCATE_AND_FILTER, 0, truncate_filter_node_))) {
    LOG_WARN("failed to alloc truncate filter node", K(ret));
  } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TRUNCATE_AND_FILTER_EXECUTOR,
                                           0, *truncate_filter_node_, executor, op))) {
    LOG_WARN("failed to alloc truncate filter executor", K(ret));
  } else if (FALSE_IT(truncate_filter_executor_ = static_cast<ObTruncateAndFilterExecutor*>(executor))) {
  } else if (OB_FAIL(truncate_filter_executor_->init(filter_factory, schema_rowkey_cnt, cols_desc, cols_param, array))) {
    LOG_WARN("failed to init truncate filter executor", K(ret));
  } else if (OB_FAIL(init_column_idxs(array))) {
    LOG_WARN("failed to init column idxs", KR(ret), K(array));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTruncatePartitionFilter::init_column_idxs(const ObTruncateInfoArray &array)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < array.count(); ++idx) {
    if (OB_ISNULL(array.at(idx))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid data", KR(ret), K(idx), KPC(array.at(idx)));
    } else if (0 == idx && OB_FAIL(init_column_idxs(array.at(idx)->truncate_part_.part_key_idxs_))) {
      LOG_WARN("failed to init column idx", KR(ret), K(idx), KPC(array.at(idx)));
    } else if (array.at(idx)->is_sub_part_) {
      if (OB_FAIL(init_column_idxs(array.at(idx)->truncate_subpart_.part_key_idxs_))) {
        LOG_WARN("failed to init column idx", KR(ret), K(idx), KPC(array.at(idx)));
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObTruncatePartitionFilter::init_column_idxs(const ObPartKeyIdxArray &key_idx_array)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < key_idx_array.count(); ++idx) {
    if (OB_FAIL(ref_column_idxs_.push_back(key_idx_array.at(idx)))) {
      LOG_WARN("failed to push back part key idx", KR(ret), K(idx), K(key_idx_array));
    }
  }
  return ret;
}

int ObTruncatePartitionFilterFactory::build_truncate_partition_filter(
    ObTablet &tablet,
    const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObVersionRange &read_version_range,
    ObMDSFilterMgr &mds_filter_mgr,
    ObTruncatePartitionFilter *&truncate_part_filter)
{
  int ret = OB_SUCCESS;
  bool has_truncate_info = false;
  ObIAllocator *outer_allocator = mds_filter_mgr.get_outer_allocator();

  if (OB_UNLIKELY(nullptr == outer_allocator || !read_version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(tablet), KP(outer_allocator), K(read_version_range));
  } else if (FALSE_IT(tablet.check_truncate_info_state(read_version_range, has_truncate_info))) {
  } else if (!has_truncate_info) {
    LOG_DEBUG("[TRUNCATE INFO] do not need read truncate info", K(ret), K(read_version_range), KP(truncate_part_filter));
    if (OB_UNLIKELY(nullptr != truncate_part_filter)) {
      truncate_part_filter->reuse();
      truncate_part_filter->set_empty();
    }
  } else if (nullptr == truncate_part_filter) {
    if (OB_ISNULL(truncate_part_filter = OB_NEWx(ObTruncatePartitionFilter, outer_allocator, mds_filter_mgr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_FAIL(truncate_part_filter->init(tablet, split_extra_tablet_handles, cols_desc, cols_param, read_version_range,
                                                  has_truncate_info))) {
      LOG_WARN("failed to init filter wrapper", K(ret));
    }
    if (OB_FAIL(ret) && nullptr != truncate_part_filter) {
      truncate_part_filter->~ObTruncatePartitionFilter();
      outer_allocator->free(truncate_part_filter);
      truncate_part_filter = nullptr;
    }
  } else if (FALSE_IT(truncate_part_filter->reuse())) {
  } else if (OB_FAIL(truncate_part_filter->switch_info(tablet, split_extra_tablet_handles, cols_desc, cols_param, read_version_range,
                                                       has_truncate_info))) {
    LOG_WARN("failed to switch info", K(ret));
  }
  return ret;
}

void ObTruncatePartitionFilterFactory::destroy_truncate_partition_filter(ObTruncatePartitionFilter *&truncate_part_filter)
{
  if (nullptr != truncate_part_filter) {
    ObIAllocator *outer_allocator = truncate_part_filter->get_mds_filter_mgr().get_outer_allocator();
    truncate_part_filter->~ObTruncatePartitionFilter();
    if (OB_NOT_NULL(outer_allocator)) {
      outer_allocator->free(truncate_part_filter);
    } else {
      int ret = OB_ERR_UNEXPECTED;
      LOG_WARN("destroy_truncate_partition_filter must used with build_truncate_partition_filter. but outer_allocator is nullptr, which is unexpected", KR(ret), KPC(truncate_part_filter));
    }
    truncate_part_filter = nullptr;
  }
}

}
}
