/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_ttl_filter_struct.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace sql
{

using namespace blocksstable;

int ObTTLWhiteFilterNode::set_op_type(const ObRawExpr &)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("TTL white filter node should use another func to set", K(ret));
  return ret;
}

int ObTTLWhiteFilterNode::get_filter_val_meta(common::ObObjMeta &obj_meta) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!obj_meta_.is_valid() || obj_meta_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid obj meta", KR(ret), K_(obj_meta));
  } else {
    obj_meta = obj_meta_;
  }

  return ret;
}

ObObjType ObTTLWhiteFilterNode::get_filter_arg_obj_type(int64_t) const
{
  return obj_meta_.get_type();
}

int ObTTLWhiteFilterExecutor::inner_init(const ObTTLFilterInfo &ttl_filter_info,
                                         const ObTTLFilterInitParams &init_params,
                                         const int64_t schema_rowkey_cnt,
                                         const FilterType executor_filter_type)
{
  int ret = OB_SUCCESS;

  const ObIArray<ObColDesc> &col_descs = init_params.cols_desc_;
  const ObColumnIndexArray *col_index_array = init_params.column_index_array_;

  // The col_idx in TTLFilterInfo is the ttl column index of full column in storage order.
  // But at this time, what we need is the index of ttl column in ReadInfo order
  // TODO(menglan): Too confuse of (col_index, read_info.cols_index_array, col_offset, output_project...)
  //                Maybe we need add some type information to distinguish them and refactor read_info struct
  int64_t ttl_col_offset = col_index_array ? col_index_array->get_array_idx_by_value(ttl_filter_info.ttl_filter_col_idx_)
                                           : ttl_filter_info.ttl_filter_col_idx_; // this branch is for compaction use, where col_offset is always equal to col_idx

  if (OB_UNLIKELY(!ttl_filter_info.is_valid()
                  || ttl_col_offset < 0
                  || col_descs.count() <= max(ttl_col_offset, schema_rowkey_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid ttl filter info", KR(ret), K(ttl_filter_info), K(col_descs), K(schema_rowkey_cnt), K(ttl_col_offset), K(init_params));
  } else {
    ObTTLWhiteFilterNode &node = static_cast<ObTTLWhiteFilterNode &>(get_filter_node());

    // set col index and type
    switch (executor_filter_type) {
      case FilterType::COMMIT_VERSION_FILTER:
        col_offset_ = schema_rowkey_cnt;
        col_idx_ = schema_rowkey_cnt;
        col_type_ = ObTTLFilterColType::ROWSCN;
        original_filter_val_ = ttl_filter_info.commit_version_;
        filter_type_ = FilterType::COMMIT_VERSION_FILTER;
        break;
      case FilterType::TTL_FILTER:
        col_offset_ = ttl_col_offset;
        col_idx_ = ttl_filter_info.ttl_filter_col_idx_;
        col_type_ = ttl_filter_info.ttl_filter_col_type_;
        original_filter_val_ = ttl_filter_info.ttl_filter_value_;
        filter_type_ = FilterType::TTL_FILTER;
        break;
      case FilterType::IS_NULL_FILTER:
        col_offset_ = ttl_col_offset;
        col_idx_ = ttl_filter_info.ttl_filter_col_idx_;
        col_type_ = ttl_filter_info.ttl_filter_col_type_;
        original_filter_val_ = ttl_filter_info.ttl_filter_value_;
        filter_type_ = FilterType::IS_NULL_FILTER;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl white filter type", KR(ret), K(executor_filter_type));
        break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(node.obj_meta_.set_meta(col_descs.at(col_offset_).col_type_))) {
    } else if (OB_FALSE_IT(col_obj_meta_.set_meta(col_descs.at(col_offset_).col_type_))) {
    } else if (OB_FALSE_IT(cmp_func_ = get_datum_cmp_func(node.obj_meta_, node.obj_meta_))) {
    } else if (FilterType::IS_NULL_FILTER == filter_type_) {
      cache_datum_.reuse();
      node.set_op_type(WHITE_OP_NU);
    } else {
      // set value
      cache_datum_.reuse();
      switch (col_type_) {
        case ObTTLFilterColType::ROWSCN:
          cache_datum_.set_int(original_filter_val_);
          node.set_op_type(WHITE_OP_GT); // Caller will ensure the compare value is positive.
          break;
        case ObTTLFilterColType::INT64:
          cache_datum_.set_int(-original_filter_val_);
          node.set_op_type(WHITE_OP_LT); // Different from rowscn filter, the value is negative from user's perspective.
          break;
        case ObTTLFilterColType::TIMESTAMP:
          cache_datum_.set_timestamp(original_filter_val_);
          node.set_op_type(WHITE_OP_GT);
          break;
        case ObTTLFilterColType::MYSQL_DATETIME:
          cache_datum_.set_mysql_datetime(original_filter_val_);
          node.set_op_type(WHITE_OP_GT);
          break;
        case ObTTLFilterColType::MYSQL_DATE:
          cache_datum_.set_mysql_date(original_filter_val_);
          node.set_op_type(WHITE_OP_GT);
          break;
        case ObTTLFilterColType::ORACLE_DATE:
          cache_datum_.set_datetime(original_filter_val_);
          node.set_op_type(WHITE_OP_GT);
          break;
        case ObTTLFilterColType::TIMESTAMP_NANO:
        case ObTTLFilterColType::TIMESTAMP_LTZ: {
          common::ObOTimestampData ot_data;
          ot_data.time_us_ = original_filter_val_ / 1000L; // ns to us
          ot_data.time_ctx_.set_tail_nsec(original_filter_val_ % 1000L);
          cache_datum_.set_otimestamp_tiny(ot_data);
          node.set_op_type(WHITE_OP_GT);
          break;
        }
        case ObTTLFilterColType::TIMESTAMP_TZ: {
          common::ObOTimestampData ot_data;
          ot_data.time_us_ = original_filter_val_ / 1000L; // ns to us
          ot_data.time_ctx_.set_tail_nsec(original_filter_val_ % 1000L);
          cache_datum_.set_otimestamp(ot_data);
          node.set_op_type(WHITE_OP_GT);
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected obj meta, should error in table_param or dml_param", KR(ret), K(node.obj_meta_), K(ttl_filter_info));
      };
    }

    LOG_TRACE("init ttl filter executor", KR(ret), K(cache_datum_), K(col_descs), K(node.obj_meta_), K(ttl_filter_info), K(filter_type_));
  }

  // push cache value into datum params(pushdown interface needed)
  // col_ids_, col_offsets_, col_params_, default_datums_ (micro block pushdown / skip index)
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(datum_params_.clear())) {
  } else if (OB_FAIL(datum_params_.push_back(cache_datum_))) {
    LOG_WARN("Failed to push back datum", KR(ret), K(cache_datum_));
  } else if (OB_FALSE_IT(filter_.col_ids_.clear())) {
  } else if (OB_FAIL(filter_.col_ids_.push_back(col_descs.at(col_offset_).col_id_))) {
    LOG_WARN("Failed to push back col idx", KR(ret), K(col_offset_), K(col_descs.at(col_offset_)));
  } else if (OB_FALSE_IT(col_params_.clear())) {
  } else if (OB_FAIL(col_params_.push_back(nullptr))) {
    LOG_WARN("Failed to push back col params", KR(ret), K(col_offset_));
  } else if (OB_FALSE_IT(default_datums_.clear())) {
  } else if (OB_FAIL(default_datums_.prepare_allocate(1))) {
    LOG_WARN("Failed to push back default datum", KR(ret), K(cache_datum_));
  } else if (OB_FALSE_IT(default_datums_.at(0).reuse())) {
  } else if (col_type_ != ObTTLFilterColType::ROWSCN) {
    const ObObj *default_value = nullptr;

    if (init_params.column_params_ != nullptr) {
      default_value = &init_params.column_params_->at(col_offset_)->get_orig_default_value();
    } else if (init_params.schema_ != nullptr) {
      const ObStorageColumnSchema *col_schema = init_params.schema_->get_column_schema(col_descs.at(col_offset_).col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null column schema", KR(ret), K(col_descs.at(col_offset_)), KPC(init_params.schema_));
      } else {
        default_value = &col_schema->get_orig_default_value();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (default_value && OB_FAIL(default_datums_.at(0).from_obj(*default_value))) {
      LOG_WARN("Failed to push back default datum", KR(ret), KPC(default_value));
    }
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::switch_info(ObPushdownFilterFactory &filter_factory,
                                          const ObTTLFilterInfo &ttl_filter_info,
                                          const ObTTLFilterInitParams &init_params,
                                          const int64_t schema_rowkey_cnt,
                                          const FilterType executor_filter_type)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL filter executor not inited", KR(ret));
  } else if (OB_FAIL(inner_init(ttl_filter_info, init_params, schema_rowkey_cnt, executor_filter_type))) {
    LOG_WARN("Failed to inner init", KR(ret), K(ttl_filter_info), K(init_params.cols_desc_));
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::init(ObPushdownFilterFactory &filter_factory,
                                   const ObTTLFilterInfo &ttl_filter_info,
                                   const ObTTLFilterInitParams &init_params,
                                   const int64_t schema_rowkey_cnt,
                                   const FilterType executor_filter_type)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("TTL filter executor already inited", KR(ret));
  } else if (OB_FAIL(datum_params_.init(1))) { // ttl column is always 1, now
    LOG_WARN("Failed to init datum params", KR(ret));
  } else if (OB_FAIL(filter_.col_ids_.init(1))) {
    LOG_WARN("Failed to init col ids", KR(ret));
  } else if (OB_FAIL(col_params_.init(1))) {
    LOG_WARN("Failed to init col params", KR(ret));
  } else if (OB_FAIL(default_datums_.init(1))) {
    LOG_WARN("Failed to init default datums", KR(ret));
  } else if (OB_FAIL(inner_init(ttl_filter_info, init_params, schema_rowkey_cnt, executor_filter_type))) {
    LOG_WARN("Failed to inner init", KR(ret), K(ttl_filter_info), K(init_params.cols_desc_));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::inner_filter(const ObStorageDatum &datum,
                                           bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (is_filter_always_true()) {
    // always true means nothing is filtered
    filtered = false;
  } else if (is_filter_always_false()) {
    // always false means everything is filtered
    filtered = true;
  } else if (OB_UNLIKELY(datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected datum", KR(ret), K(datum));
  } else {
    ObStorageDatum shallow_datum;
    shallow_datum.shallow_copy_from_datum(datum);

    if (col_type_ == ObTTLFilterColType::ROWSCN && datum.get_int() < 0) {
      // if this column is ora_rowscn, we should make the value > 0 to meet the filter logic
      shallow_datum.reuse();
      shallow_datum.set_int(-datum.get_int());
    }

    if (OB_FAIL(ObIMicroBlockReader::filter_white_filter(*this, shallow_datum, filtered))) {
      LOG_WARN("Failed to filter white filter", KR(ret), K(datum), KPC(this));
    }
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::skip_index_filter(
    ObAggRowCachedReader &agg_row_cached_reader,
    sql::ObBoolMask &fal_desc,
    const ObSkipIndexExtraParam &extra_param) const
{
  int ret = OB_SUCCESS;

  fal_desc.set_uncertain();

  ObMinMaxFilterParam param;
  ObObjMeta obj_meta;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL filter executor not inited", KR(ret));
  } else if (OB_FAIL(get_filter_node().get_filter_val_meta(obj_meta))) {
    LOG_WARN("Failed to get filter val meta", KR(ret));
  } else if (OB_FAIL(ObSkipIndexFilterExecutor::read_aggregate_data(
                 agg_row_cached_reader,
                 col_idx_,
                 obj_meta,
                 is_padding_mode_,
                 param,
                 nullptr,
                 nullptr,
                 col_type_ == ObTTLFilterColType::ROWSCN,
                 extra_param))) {
    LOG_WARN("Failed to read skip index agg data", KR(ret), K_(col_idx));
  } else if (OB_FAIL(ObSkipIndexFilterExecutor::apply_filter_on_min_max(param, *this, fal_desc, extra_param.get_mock_row_count()))) {
    LOG_WARN("Failed to apply filter on min max", K(ret));
  }

  return ret;
}

ObTTLOrFilterExecutor::ObTTLOrFilterExecutor(ObIAllocator &alloc,
                                             ObTTLOrFilterNode &filter,
                                             ObPushdownOperator *op)
    : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::TTL_OR_FILTER_EXECUTOR),
      filter_(filter), is_inited_(false)
{
}

int ObTTLOrFilterExecutor::init(ObPushdownFilterFactory &filter_factory,
                                const ObTTLFilterInfo &ttl_filter_info,
                                const ObTTLFilterInitParams &init_params,
                                const int64_t schema_rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("TTL or filter executor already inited", KR(ret));
  } else if (ttl_filter_info.is_rowscn_filter()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter info type, shouldn't be rowscn filter", KR(ret), K(ttl_filter_info));
  } else {
    ObPushdownFilterNode *nodes[CHILD_COUNT] = {nullptr, nullptr, nullptr};
    ObPushdownFilterExecutor *executors[CHILD_COUNT] = {nullptr, nullptr, nullptr};

    for (int64_t i = 0; OB_SUCC(ret) && i < CHILD_COUNT; ++i) {
      if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_WHITE_FILTER, 0, nodes[i]))) {
        LOG_WARN("failed to alloc node", K(ret));
      } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR, 0, *nodes[i], executors[i]))) {
        LOG_WARN("failed to alloc executor", K(ret));
      } else if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(executors[i])->init(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt, CHILD_FILTER_TYPES[i]))) {
        LOG_WARN("failed to init", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < CHILD_COUNT; ++i) {
        if (OB_NOT_NULL(nodes[i])) {
          nodes[i]->~ObPushdownFilterNode();
          nodes[i] = nullptr;
        }
        if (OB_NOT_NULL(executors[i])) {
          executors[i]->~ObPushdownFilterExecutor();
          executors[i] = nullptr;
        }
      }
    } else {
      for (int64_t i = 0; i < CHILD_COUNT; ++i) {
        set_child(i, executors[i]);
      }
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTTLOrFilterExecutor::switch_info(ObPushdownFilterFactory &filter_factory,
                                       const ObTTLFilterInfo &ttl_filter_info,
                                       const ObTTLFilterInitParams &init_params,
                                       const int64_t schema_rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL or filter executor not inited", KR(ret));
  } else if (ttl_filter_info.is_rowscn_filter()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter info type, shouldn't be rowscn filter", KR(ret), K(ttl_filter_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < CHILD_COUNT; ++i) {
      ObPushdownFilterExecutor *executor = nullptr;
      if (OB_FAIL(get_child(i, executor))) {
        LOG_WARN("failed to get child", K(ret));
      } else if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(executor)->switch_info(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt, CHILD_FILTER_TYPES[i]))) {
        LOG_WARN("failed to switch info", K(ret));
      }
    }
  }

  return ret;
}


int ObTTLOrFilterExecutor::filter(const ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  filtered = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL or filter executor not inited", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < n_child_ && filtered; ++i) {
      ObPushdownFilterExecutor *child = childs_[i];
      if (OB_UNLIKELY(nullptr == child || !child->is_ttl_filter_node())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl executor type", K(ret), K(i), KPC(child));
      } else if (child->is_filter_always_true()) {
        filtered = false;
      } else if (child->is_filter_always_false()) {
        filtered = true;
      } else if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(child)->filter(row, filtered))) {
        LOG_WARN("failed to filter", K(ret), K(i), KPC(child));
      }
    }
  }

  return ret;
}

int ObTTLOrFilterExecutor::skip_index_filter(
    ObAggRowCachedReader &agg_row_cached_reader,
    sql::ObBoolMask &fal_desc,
    const ObSkipIndexExtraParam &extra_param) const
{
  int ret = OB_SUCCESS;
  fal_desc.set_always_false();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL or filter executor not inited", KR(ret));
  } else {
    // OrFilter condition = child1.condition OR child2.condition
    for (int64_t i = 0; OB_SUCC(ret) && i < n_child_; ++i) {
      ObPushdownFilterExecutor *child = childs_[i];
      sql::ObBoolMask child_desc;
      if (OB_UNLIKELY(nullptr == child || !child->is_ttl_filter_node())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl executor type", K(ret), K(i), KPC(child));
      } else if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(child)->skip_index_filter(agg_row_cached_reader, child_desc, extra_param))) {
        LOG_WARN("Failed to filter by skip index datum", K(ret), K(i));
      } else {
        fal_desc = fal_desc | child_desc;
        if (fal_desc.is_always_true()) {
          break;
        }
      }
    }
  }

  return ret;
}

ObTTLAndFilterExecutor::ObTTLAndFilterExecutor(ObIAllocator &alloc,
                                               ObTTLAndFilterNode &filter,
                                               ObPushdownOperator *op)
    : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::TTL_AND_FILTER_EXECUTOR),
      is_inited_(false), valid_ttl_filter_cnt_(0), filter_(filter)
{
  ObMemAttr mem_attr(MTL_ID(), "TTLExe");
  ttl_filters_.set_attr(mem_attr);
  ttl_filters_buffer_.set_attr(mem_attr);
}

ObTTLAndFilterExecutor::~ObTTLAndFilterExecutor()
{
#define RELEASE_TTL_FILTERS(filters)                                                               \
  for (int64_t i = 0; i < filters.count(); ++i) {                                                  \
    if (OB_NOT_NULL(filters.at(i))) {                                                              \
      filters.at(i)->~ObPushdownFilterExecutor();                                                  \
    }                                                                                              \
  }

  RELEASE_TTL_FILTERS(ttl_filters_);
  RELEASE_TTL_FILTERS(ttl_filters_buffer_);
  // childs_ is a shallow copy of ttl_filters_ (set by mock_children_array_for_skip_index),
  // clear it to prevent base class destructor from double-destroying the same children.
  set_childs(0, nullptr);
#undef RELEASE_TTL_FILTERS
}

int ObTTLAndFilterExecutor::init(ObPushdownFilterFactory &filter_factory,
                                 const ObTTLFilterInfoArray &ttl_filter_info_array,
                                 const ObTTLFilterInitParams &init_params,
                                 const int64_t schema_rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("TTL filter executor already inited", KR(ret));
  } else if (OB_UNLIKELY(init_params.cols_desc_.count() <= 0 || ttl_filter_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(init_params.cols_desc_), K(ttl_filter_info_array));
  } else {
    ObPushdownFilterExecutor *single_ttl_filter = nullptr;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < ttl_filter_info_array.count(); ++idx) {
      single_ttl_filter = nullptr;
      const ObTTLFilterInfo *ttl_filter_info = ttl_filter_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == ttl_filter_info || !ttl_filter_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid ttl filter info", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(build_ttl_filter(*ttl_filter_info, init_params, schema_rowkey_cnt, filter_factory, single_ttl_filter))) {
        LOG_WARN("Failed to build ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_ISNULL(single_ttl_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null single ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(ttl_filters_.push_back(single_ttl_filter))) {
        LOG_WARN("Failed to push back", KR(ret));
        if (OB_NOT_NULL(single_ttl_filter)) {
          single_ttl_filter->~ObPushdownFilterExecutor();
          single_ttl_filter = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      mock_children_array_for_skip_index();
      valid_ttl_filter_cnt_ = ttl_filter_info_array.count();
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::switch_info(ObPushdownFilterFactory &filter_factory,
                                        const ObTTLFilterInfoArray &ttl_filter_info_array,
                                        const ObTTLFilterInitParams &init_params,
                                        const int64_t schema_rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL and filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(ttl_filter_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(init_params.cols_desc_), K(ttl_filter_info_array));
  } else if (OB_FAIL(inner_reuse())) {
    LOG_WARN("Failed to inner reuse", KR(ret));
  } else {
    ObPushdownFilterExecutor *single_ttl_filter = nullptr;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < ttl_filter_info_array.count(); ++idx) {
      single_ttl_filter = nullptr;
      const ObTTLFilterInfo *ttl_filter_info = ttl_filter_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == ttl_filter_info || !ttl_filter_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid ttl filter info", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(try_use_cached_filter(filter_factory, *ttl_filter_info, init_params, schema_rowkey_cnt, single_ttl_filter))) {
        LOG_WARN("Failed to try use cached filter", KR(ret));
      } else if (nullptr == single_ttl_filter
                 && OB_FAIL(build_ttl_filter(*ttl_filter_info, init_params, schema_rowkey_cnt, filter_factory, single_ttl_filter))) {
        LOG_WARN("Failed to build ttl filter", KR(ret));
      } else if (OB_ISNULL(single_ttl_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null single ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(ttl_filters_.push_back(single_ttl_filter))) {
        LOG_WARN("Failed to push back", KR(ret));
        if (OB_NOT_NULL(single_ttl_filter)) {
          single_ttl_filter->~ObPushdownFilterExecutor();
          single_ttl_filter = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      mock_children_array_for_skip_index();
      valid_ttl_filter_cnt_ = ttl_filter_info_array.count();
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::inner_reuse()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < ttl_filters_.count(); ++i) {
    ObPushdownFilterExecutor *filter = ttl_filters_.at(i);
    if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter", K(ret), K(i));
    } else if (OB_FAIL(ttl_filters_buffer_.push_back(filter))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ttl_filters_.reuse();
    valid_ttl_filter_cnt_ = 0;
  }

  return ret;
}

int ObTTLAndFilterExecutor::try_use_cached_filter(
    ObPushdownFilterFactory &filter_factory,
    const ObTTLFilterInfo &ttl_filter_info,
    const ObTTLFilterInitParams &init_params,
    const int64_t schema_rowkey_cnt,
    ObPushdownFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;

  filter = nullptr;

  for (int64_t i = 0; OB_SUCC(ret) && i < ttl_filters_buffer_.count(); ++i) {
    ObPushdownFilterExecutor *candidate = ttl_filters_buffer_.at(i);
    if (OB_ISNULL(candidate)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter", K(ret), K(i));
    } else if (ttl_filter_info.is_rowscn_filter()
                   ? candidate->get_type() == PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR
                   : candidate->get_type() == PushdownExecutorType::TTL_OR_FILTER_EXECUTOR) {
      if (OB_FAIL(ttl_filters_buffer_.remove(i))) {
        LOG_WARN("failed to remove filter from buffer", K(ret), K(i));
      } else {
        filter = candidate;
        if (filter->get_type() == PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR
                ? OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(filter)->switch_info(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))
                : OB_FAIL(static_cast<ObTTLOrFilterExecutor *>(filter)->switch_info(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))) {
          LOG_WARN("failed to switch info", K(ret), KPC(filter));
        }
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(filter)) {
      filter->~ObPushdownFilterExecutor();
      filter = nullptr;
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::build_ttl_filter(
    const ObTTLFilterInfo &ttl_filter_info,
    const ObTTLFilterInitParams &init_params,
    const int64_t schema_rowkey_cnt,
    ObPushdownFilterFactory &filter_factory,
    ObPushdownFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterNode *node = nullptr;
  ObPushdownFilterExecutor *executor = nullptr;

  if (ttl_filter_info.is_rowscn_filter()) {
    if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_WHITE_FILTER, 0, node))) {
      LOG_WARN("failed to alloc node", K(ret));
    } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR, 0, *node, executor))) {
      LOG_WARN("failed to alloc executor", K(ret));
    } else if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(executor)->init(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))) {
      LOG_WARN("failed to init", K(ret));
    }
  } else {
    if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_OR_FILTER, ObTTLOrFilterExecutor::CHILD_COUNT, node))) {
      LOG_WARN("failed to alloc node", K(ret));
    } else if (OB_FAIL(filter_factory.alloc(PushdownExecutorType::TTL_OR_FILTER_EXECUTOR, ObTTLOrFilterExecutor::CHILD_COUNT, *node, executor))) {
      LOG_WARN("failed to alloc executor", K(ret));
    } else if (OB_FAIL(static_cast<ObTTLOrFilterExecutor *>(executor)->init(filter_factory, ttl_filter_info, init_params, schema_rowkey_cnt))) {
      LOG_WARN("failed to init", K(ret));
    }
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
  } else {
    filter = executor;
  }

  return ret;
}

int ObTTLAndFilterExecutor::filter(const ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL and filter executor not inited", KR(ret));
  } else {
    filtered = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_ttl_filter_cnt_ && !filtered; ++i) {
      if (ttl_filters_.at(i)->get_type() == PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR) {
        if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(ttl_filters_.at(i))->filter(row, filtered))) {
          LOG_WARN("failed to filter ttl filter", K(ret), K(i), KPC(ttl_filters_.at(i)));
        }
      } else if (ttl_filters_.at(i)->get_type() == PushdownExecutorType::TTL_OR_FILTER_EXECUTOR) {
        if (OB_FAIL(static_cast<ObTTLOrFilterExecutor *>(ttl_filters_.at(i))->filter(row, filtered))) {
          LOG_WARN("failed to filter ttl filter", K(ret), K(i), KPC(ttl_filters_.at(i)));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl filter type", K(ret), K(i), KPC(ttl_filters_.at(i)));
      }
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::skip_index_filter(ObAggRowCachedReader &agg_row_cached_reader,
                                              sql::ObBoolMask &fal_desc,
                                              const ObSkipIndexExtraParam &extra_param) const
{
  int ret = OB_SUCCESS;
  fal_desc.set_always_true();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL and filter executor not inited", KR(ret));
  } else {
    // AndFilter condition = child1.condition AND child2.condition
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_ttl_filter_cnt_; ++i) {
      sql::ObBoolMask child_desc;
      ObPushdownFilterExecutor *child = ttl_filters_.at(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", K(ret), K(i));
      } else if (child->get_type() == PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR) {
        if (OB_FAIL(static_cast<ObTTLWhiteFilterExecutor *>(child)->skip_index_filter(agg_row_cached_reader, child_desc, extra_param))) {
          LOG_WARN("Failed to filter by skip index datum", K(ret), K(i));
        }
      } else if (child->get_type() == PushdownExecutorType::TTL_OR_FILTER_EXECUTOR) {
        if (OB_FAIL(static_cast<ObTTLOrFilterExecutor *>(child)->skip_index_filter(agg_row_cached_reader, child_desc, extra_param))) {
          LOG_WARN("Failed to filter by skip index datum", K(ret), K(i));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl filter type", K(ret), K(i), KPC(child));
      }

      if (OB_SUCC(ret)) {
        fal_desc = fal_desc & child_desc;
        if (fal_desc.is_always_false()) {
          break;
        }
      }
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::execute_logic_filter(PushdownFilterInfo &filter_info,
                                                 ObIMicroBlockRowScanner *micro_scanner,
                                                 const bool use_vectorize,
                                                 ObBitmap &result)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < valid_ttl_filter_cnt_; i++) {
    const ObBitmap *child_result = nullptr;
    if (OB_ISNULL(ttl_filters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child filter", K(ret), K(i));
    } else if (OB_FAIL(ttl_filters_.at(i)->execute(this, filter_info, micro_scanner, use_vectorize))) {
      LOG_WARN("failed to filter micro block", K(ret), K(i), KP(ttl_filters_.at(i)));
    } else if (OB_ISNULL(child_result = ttl_filters_.at(i)->get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected get null filter bitmap", K(ret));
    } else if (OB_FAIL(result.bit_and(*child_result))) {
      LOG_WARN("failed to merge result bitmap", K(ret), KP(child_result));
    } else if (result.is_all_false()) {
      break;
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
