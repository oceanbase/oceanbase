/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_ttl_filter.h"

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
  if (nullptr != single_or_node_) {
    single_or_node_->~ObTTLOrFilterNode();
  }
  if (nullptr != single_or_executor_) {
    single_or_executor_->~ObTTLOrFilterExecutor();
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
                      const ObTTLFilterInitParams &init_params,
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
                          init_params,
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

  filter_mode_ = ObTTLFilterMode::EMPTY;
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
                             const ObTTLFilterInitParams &init_params,
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
  } else if (OB_FAIL(init_ttl_filter(tablet.get_rowkey_read_info().get_schema_rowkey_count(), init_params, ttl_filter_info_array_))) {
    LOG_WARN("Fail to init ttl filter", KR(ret));
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
                      const ObTTLFilterInitParams &init_params,
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
  } else if (OB_FAIL(init_ttl_filter(schema_rowkey_cnt, init_params, ttl_filter_info_array_))) {
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

int ObTTLFilter::calc_filter_mode(const ObTTLFilterInfoArray &ttl_filter_info_array,
                                  ObTTLFilterMode &filter_mode)
{
  int ret = OB_SUCCESS;

  if (ttl_filter_info_array.empty()) {
    filter_mode = ObTTLFilterMode::EMPTY;
  } else if (ttl_filter_info_array.count() == 1) {
    const ObTTLFilterInfo *ttl_filter_info = ttl_filter_info_array.at(0);
    if (OB_ISNULL(ttl_filter_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null ttl filter info", KR(ret));
    } else if (ttl_filter_info->is_rowscn_filter()) {
      filter_mode = ObTTLFilterMode::SINGLE_WHITE;
    } else {
      filter_mode = ObTTLFilterMode::SINGLE_OR;
    }
  } else {
    filter_mode = ObTTLFilterMode::AND_MULTI;
  }

  return ret;
}

template <PushdownFilterType NODE_TYPE_VAL,
          PushdownExecutorType EXECUTOR_TYPE_VAL,
          int64_t CHILD_COUNT,
          typename EXECUTOR_TYPE,
          typename NODE_TYPE,
          typename TTL_FILTER_INFOS>
int ObTTLFilter::init_or_switch_filter_executor(const TTL_FILTER_INFOS &ttl_filter_info,
                                                const ObTTLFilterInitParams &init_params,
                                                const int64_t schema_rowkey_cnt,
                                                EXECUTOR_TYPE &ret_executor,
                                                NODE_TYPE &ret_node)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterNode *node = nullptr;
  ObPushdownFilterExecutor *executor = nullptr;
  ObPushdownFilterFactory filter_factory(&filter_allocator_);

  if (ret_executor != nullptr) {
    if (OB_FAIL(ret_executor->switch_info(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))) {
      LOG_WARN("Fail to switch ttl filter", KR(ret), K(ttl_filter_info), K(init_params.cols_desc_), K(schema_rowkey_cnt));
    }
  } else {
    if (OB_FAIL(filter_factory.alloc(NODE_TYPE_VAL, CHILD_COUNT, node))) {
      LOG_WARN("Fail to alloc ttl white filter node", KR(ret), K(NODE_TYPE_VAL), K(CHILD_COUNT));
    } else if (OB_FAIL(filter_factory.alloc(EXECUTOR_TYPE_VAL, CHILD_COUNT, *node, executor))) {
      LOG_WARN("Fail to alloc ttl white filter executor", KR(ret), K(EXECUTOR_TYPE_VAL), K(CHILD_COUNT));
    } else {
      for (int64_t i = 0; i < CHILD_COUNT; ++i) {
        executor->set_child(i, nullptr);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(static_cast<EXECUTOR_TYPE>(executor)->init(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))) {
      LOG_WARN("Fail to init ttl white filter executor", KR(ret), K(EXECUTOR_TYPE_VAL), K(CHILD_COUNT));
    } else {
      ret_executor = static_cast<EXECUTOR_TYPE>(executor);
      ret_node = static_cast<NODE_TYPE>(node);
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(node)) {
        node->~ObPushdownFilterNode();
        node = nullptr;
      }
      if (OB_NOT_NULL(executor)) {
        executor->~ObPushdownFilterExecutor();
        executor = nullptr;
      }
    }
  }

  return ret;
}

int ObTTLFilter::init_ttl_filter(const int64_t schema_rowkey_cnt,
                                 const ObTTLFilterInitParams &init_params,
                                 const ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(calc_filter_mode(ttl_filter_info_array, filter_mode_))) {
    LOG_WARN("Fail to calc filter mode", KR(ret), K(ttl_filter_info_array));
  } else {
    // Don't need to release other type executor which maybe reused for next partition.
    // All of them will be released in destructor.
    switch (filter_mode_) {
      case ObTTLFilterMode::SINGLE_WHITE:
        if (OB_FAIL((init_or_switch_filter_executor<PushdownFilterType::TTL_WHITE_FILTER,
                                                    PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR>(
            *ttl_filter_info_array.at(0),
            init_params,
            schema_rowkey_cnt,
            single_executor_,
            single_node_)))) {
          LOG_WARN("Fail to init or switch single ttl filter", KR(ret), K(ttl_filter_info_array), K(init_params.cols_desc_), K(schema_rowkey_cnt));
        }
        break;
      case ObTTLFilterMode::SINGLE_OR:
        if (OB_FAIL((init_or_switch_filter_executor<PushdownFilterType::TTL_OR_FILTER,
                                                    PushdownExecutorType::TTL_OR_FILTER_EXECUTOR,
                                                    ObTTLOrFilterExecutor::CHILD_COUNT>(
            *ttl_filter_info_array.at(0),
            init_params,
            schema_rowkey_cnt,
            single_or_executor_,
            single_or_node_)))) {
          LOG_WARN("Fail to init or switch single or filter", KR(ret), K(ttl_filter_info_array), K(init_params.cols_desc_), K(schema_rowkey_cnt));
        }
        break;
      case ObTTLFilterMode::AND_MULTI:
        if (OB_FAIL((init_or_switch_filter_executor<PushdownFilterType::TTL_AND_FILTER,
                                                    PushdownExecutorType::TTL_AND_FILTER_EXECUTOR>(
            ttl_filter_info_array,
            init_params,
            schema_rowkey_cnt,
            ttl_filter_executor_,
            ttl_filter_node_)))) {
          LOG_WARN("Fail to init or switch multi ttl filter", KR(ret), K(ttl_filter_info_array), K(init_params.cols_desc_), K(schema_rowkey_cnt));
        }
        break;
      case ObTTLFilterMode::EMPTY:
        ret = OB_SUCCESS;
        break;
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

int ObTTLFilter::skip_index_filter(blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
                                   sql::ObBoolMask &fal_desc,
                                   const ObSkipIndexExtraParam &extra_param) const
{
  int ret = OB_SUCCESS;
  fal_desc.set_uncertain();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", KR(ret));
  } else {
    switch (filter_mode_) {
      case ObTTLFilterMode::SINGLE_WHITE:
        ret = single_executor_->skip_index_filter(agg_row_cached_reader, fal_desc, extra_param);
        break;
      case ObTTLFilterMode::SINGLE_OR:
        ret = single_or_executor_->skip_index_filter(agg_row_cached_reader, fal_desc, extra_param);
        break;
      case ObTTLFilterMode::AND_MULTI:
        ret = ttl_filter_executor_->skip_index_filter(agg_row_cached_reader, fal_desc, extra_param);
        break;
      case ObTTLFilterMode::EMPTY:
        fal_desc.set_always_true();
        break;
    }

    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "Failed to skip index filter", KR(ret), K_(filter_mode));
    }
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
  } else if (OB_FAIL(init_ttl_filter(schema_rowkey_cnt, ObTTLFilterInitParams(cols_desc), ttl_filter_info_array))) {
    LOG_WARN("Fail to init ttl filter", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTTLFilterFactory::build_ttl_filter(ObTablet &tablet,
                                         const ObIArray<ObTabletHandle> *split_extra_tablet_handles,
                                         const ObITableReadInfo &read_info,
                                         const ObVersionRange &read_version_range,
                                         ObArenaAllocator &filter_allocator,
                                         ObTTLFilter *&ttl_filter)
{
  int ret = OB_SUCCESS;

  bool contain_ttl_info = true;
  const ObTTLFilterInitParams init_params(read_info.get_columns_desc(),
                                          &read_info.get_columns_index(),
                                          read_info.get_columns());

  if (OB_UNLIKELY(!read_version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(read_version_range));
  } else if (FALSE_IT(tablet.check_ttl_info_state(read_version_range, contain_ttl_info))) {
  } else if (!contain_ttl_info) {
    // if ttl_filter is not inited, we need to skip build ttl filter
    // if ttl_filter is inited, we should clear it (reuse for next time use)
    if (nullptr != ttl_filter) {
      ttl_filter->reuse();
    }
  } else if (nullptr == ttl_filter) {
    if (OB_ISNULL(ttl_filter = OB_NEWx(ObTTLFilter, &filter_allocator, filter_allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc ttl filter", KR(ret));
    } else if (OB_FAIL(ttl_filter->init(tablet,
                                        split_extra_tablet_handles,
                                        init_params,
                                        read_version_range))) {
      LOG_WARN("Fail to init ttl filter", KR(ret));
    }

    if (OB_FAIL(ret) && nullptr != ttl_filter) {
      ttl_filter->~ObTTLFilter();
      ttl_filter = nullptr;
    }
  } else {
    if (OB_FALSE_IT(ttl_filter->reuse())) {
    } else if (OB_FAIL(ttl_filter->switch_info(tablet,
                                               split_extra_tablet_handles,
                                               init_params,
                                               read_version_range))) {
      LOG_WARN("Fail to switch ttl filter", KR(ret));
    }
  }

  return ret;
}


} // namespace storage
} // namespace oceanbase