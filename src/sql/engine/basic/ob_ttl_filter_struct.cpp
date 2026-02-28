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

#include "ob_ttl_filter_struct.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace sql
{

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

int ObTTLWhiteFilterExecutor::inner_init(const storage::ObTTLFilterInfo &ttl_filter_info,
                                          const ObIArray<share::schema::ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ttl_filter_info.is_valid()
                  || col_descs.count() <= ttl_filter_info.ttl_filter_col_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid ttl filter info", KR(ret), K(ttl_filter_info), K(col_descs));
  } else {
    ObTTLWhiteFilterNode &node = static_cast<ObTTLWhiteFilterNode &>(get_filter_node());

    // set col index and type
    col_idx_ = ttl_filter_info.ttl_filter_col_idx_;
    node.obj_meta_.set_meta(col_descs.at(col_idx_).col_type_);
    col_obj_meta_.set_meta(col_descs.at(col_idx_).col_type_);
    is_ora_rowscn_ = ttl_filter_info.ttl_filter_col_type_ == storage::ObTTLFilterInfo::ObTTLFilterColType::ROWSCN;

    // set cmp func
    cmp_func_ = get_datum_cmp_func(node.obj_meta_, node.obj_meta_);

    // set value
    cache_datum_.reuse();
    if (node.obj_meta_.is_int()) {
      cache_datum_.set_int(ttl_filter_info.ttl_filter_value_);
    } else if (node.obj_meta_.is_timestamp()) {
      cache_datum_.set_timestamp(ttl_filter_info.ttl_filter_value_);
    } else if (node.obj_meta_.is_datetime()) {
      cache_datum_.set_datetime(ttl_filter_info.ttl_filter_value_);
    } else if (node.obj_meta_.is_date()) {
      cache_datum_.set_date(ttl_filter_info.ttl_filter_value_);
    } else if (node.obj_meta_.is_unsigned_integer()) {
      cache_datum_.set_int(ttl_filter_info.ttl_filter_value_);
    } else if (node.obj_meta_.is_mysql_datetime()) {
      cache_datum_.set_mysql_datetime(ttl_filter_info.ttl_filter_value_);
    } else {
      // TODO(menglan): support other types like timestamp, datetime, etc.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected obj meta, should error in table_param or dml_param", KR(ret), K(node.obj_meta_), K(ttl_filter_info));
    }

    LOG_TRACE("init ttl filter executor", KR(ret), K(cache_datum_), K(col_descs), K(node.obj_meta_), K(ttl_filter_info));
  }

  // push cache value into datum params(pushdown interface needed)
  // col_ids_, col_params_(skip index interface needed)
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(datum_params_.clear())) {
  } else if (OB_FAIL(datum_params_.push_back(cache_datum_))) {
    LOG_WARN("Failed to push back datum", KR(ret), K(cache_datum_));
  } else if (OB_FALSE_IT(filter_.col_ids_.clear())) {
  } else if (OB_FAIL(filter_.col_ids_.push_back(col_descs.at(col_idx_).col_id_))) {
    LOG_WARN("Failed to push back col idx", KR(ret), K(col_idx_), K(col_descs.at(col_idx_)));
  } else if (OB_FALSE_IT(col_params_.clear())) {
  } else if (OB_FAIL(col_params_.push_back(nullptr))) {
    LOG_WARN("Failed to push back col params", KR(ret), K(col_idx_));
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::switch_info(const storage::ObTTLFilterInfo &ttl_filter_info,
                                          const ObIArray<share::schema::ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL filter executor not inited", KR(ret));
  } else if (OB_FAIL(inner_init(ttl_filter_info, col_descs))) {
    LOG_WARN("Failed to inner init", KR(ret), K(ttl_filter_info), K(col_descs));
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::init(const storage::ObTTLFilterInfo &ttl_filter_info,
                                   const ObIArray<share::schema::ObColDesc> &col_descs)
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
  } else if (OB_FAIL(inner_init(ttl_filter_info, col_descs))) {
    LOG_WARN("Failed to inner init", KR(ret), K(ttl_filter_info), K(col_descs));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTTLWhiteFilterExecutor::inner_filter(const blocksstable::ObStorageDatum &datum,
                                           bool &filtered) const
{
  int ret = OB_SUCCESS;

  blocksstable::ObStorageDatum shallow_datum;
  shallow_datum.shallow_copy_from_datum(datum);

  if (is_filter_always_true()) {
    // always true means nothing is filtered
    filtered = false;
  } else if (is_filter_always_false()) {
    // always false means everything is filtered
    filtered = true;
  } else if (OB_UNLIKELY(datum.is_nop() || datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected datum", KR(ret), K(datum));
  } else if (is_ora_rowscn_ && datum.get_int() < 0) {
    // if this column is ora_rowscn, we should make the value > 0 to meet the filter logic
    shallow_datum.reuse();
    shallow_datum.set_int(-datum.get_int());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(blocksstable::ObIMicroBlockReader::filter_white_filter(
                 *this, shallow_datum, filtered))) {
    LOG_WARN("Failed to filter white filter", KR(ret), K(datum), KPC(this));
  }

  return ret;
}

ObTTLAndFilterExecutor::ObTTLAndFilterExecutor(ObIAllocator &alloc,
                                               ObTTLAndFilterNode &filter,
                                               ObPushdownOperator &op)
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
      filters.at(i)->~ObTTLWhiteFilterExecutor();                                                  \
    }                                                                                              \
  }

  RELEASE_TTL_FILTERS(ttl_filters_);
  RELEASE_TTL_FILTERS(ttl_filters_buffer_);
  // not need to release children_array_, because it's just a shallow copy of ttl_filters_
#undef RELEASE_TTL_FILTERS
}

int ObTTLAndFilterExecutor::mock_children_array_for_skip_index()
{
  int ret = OB_SUCCESS;

  // set childs array just like a normal filter executor (skip index interface needed)
  // notice that we can't use reinterpret_cast here, because the childs_ array ObPushdownFilterExecutor**
  // but ttl_filters_ array is ObTTLWhiteFilterExecutor**

  children_array_.reuse();
  set_childs(0, nullptr);

  int64_t n_child = ttl_filters_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < n_child; i++) {
    ObPushdownFilterExecutor *child = ttl_filters_.at(i);
    if (OB_FAIL(children_array_.push_back(child))) {
      LOG_WARN("Failed to push back", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    set_childs(n_child, children_array_.get_data());
  }

  return ret;
}

int ObTTLAndFilterExecutor::init(ObPushdownFilterFactory &filter_factory,
                                 const common::ObIArray<share::schema::ObColDesc> &cols_desc,
                                 const common::ObIArray<share::schema::ObColumnParam *> *cols_param,
                                 const storage::ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("TTL filter executor already inited", KR(ret));
  } else if (OB_UNLIKELY(cols_desc.count() <= 0 || ttl_filter_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(cols_desc), K(ttl_filter_info_array));
  } else {
    ObTTLWhiteFilterExecutor *single_ttl_filter = nullptr;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < ttl_filter_info_array.count(); ++idx) {
      single_ttl_filter = nullptr;
      const ObTTLFilterInfo *ttl_filter_info = ttl_filter_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == ttl_filter_info || !ttl_filter_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid ttl filter info", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(build_ttl_filter(cols_desc, *ttl_filter_info, filter_factory, single_ttl_filter))) {
        LOG_WARN("Failed to build ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_ISNULL(single_ttl_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null single ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(ttl_filters_.push_back(single_ttl_filter))) {
        LOG_WARN("Failed to push back", KR(ret));
        if (OB_NOT_NULL(single_ttl_filter)) {
          single_ttl_filter->~ObTTLWhiteFilterExecutor();
          single_ttl_filter = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mock_children_array_for_skip_index())) {
      LOG_WARN("Failed to mock children array for skip index", KR(ret));
    } else {
      valid_ttl_filter_cnt_ = ttl_filter_info_array.count();
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::switch_info(ObPushdownFilterFactory &filter_factory,
                                        const common::ObIArray<share::schema::ObColDesc> &cols_desc,
                                        const storage::ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL and filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(ttl_filter_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(cols_desc), K(ttl_filter_info_array));
  } else if (OB_FAIL(inner_reuse())) {
    LOG_WARN("Failed to inner reuse", KR(ret));
  } else {
    ObTTLWhiteFilterExecutor *single_ttl_filter = nullptr;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < ttl_filter_info_array.count(); ++idx) {
      single_ttl_filter = nullptr;
      const ObTTLFilterInfo *ttl_filter_info = ttl_filter_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == ttl_filter_info || !ttl_filter_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid ttl filter info", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(try_use_cached_filter(cols_desc, *ttl_filter_info, single_ttl_filter))) {
        LOG_WARN("Failed to try use cached filter", KR(ret));
      } else if (nullptr == single_ttl_filter
                 && OB_FAIL(build_ttl_filter(cols_desc, *ttl_filter_info, filter_factory, single_ttl_filter))) {
        LOG_WARN("Failed to build ttl filter", KR(ret));
      } else if (OB_ISNULL(single_ttl_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null single ttl filter", KR(ret), K(idx), KPC(ttl_filter_info));
      } else if (OB_FAIL(ttl_filters_.push_back(single_ttl_filter))) {
        LOG_WARN("Failed to push back", KR(ret));
        if (OB_NOT_NULL(single_ttl_filter)) {
          single_ttl_filter->~ObTTLWhiteFilterExecutor();
          single_ttl_filter = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mock_children_array_for_skip_index())) {
      LOG_WARN("Failed to mock children array for skip index", KR(ret));
    } else {
      valid_ttl_filter_cnt_ = ttl_filter_info_array.count();
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::inner_reuse()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < ttl_filters_.count(); ++i) {
    ObTTLWhiteFilterExecutor *filter = ttl_filters_.at(i);
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
    const common::ObIArray<share::schema::ObColDesc> &cols_desc,
    const storage::ObTTLFilterInfo &ttl_filter_info,
    ObTTLWhiteFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;

  filter = nullptr;
  ObIArray<ObTTLWhiteFilterExecutor *> &filter_buffer = ttl_filters_buffer_;
  if (!filter_buffer.empty()) {
    if (OB_FAIL(filter_buffer.pop_back(filter))) {
      LOG_WARN("failed to pop back", K(ret));
    } else if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter", K(ret));
    } else if (OB_FAIL(filter->switch_info(ttl_filter_info, cols_desc))) {
      LOG_WARN("failed to switch info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(filter)) {
      filter->~ObTTLWhiteFilterExecutor();
      filter = nullptr;
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::build_ttl_filter(
    const common::ObIArray<share::schema::ObColDesc> &cols_desc,
    const storage::ObTTLFilterInfo &ttl_filter_info,
    ObPushdownFilterFactory &filter_factory,
    ObTTLWhiteFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;

  ObPushdownFilterNode *node = nullptr;
  ObPushdownFilterExecutor *executor = nullptr;

  if (OB_FAIL(filter_factory.alloc(PushdownFilterType::TTL_WHITE_FILTER, 0, node))) {
    LOG_WARN("failed to alloc node", K(ret));
  } else if (OB_FAIL(filter_factory.alloc(
                 PushdownExecutorType::TTL_WHITE_FILTER_EXECUTOR, 0, *node, executor, op_))) {
    LOG_WARN("failed to alloc executor", K(ret));
  } else if (OB_FALSE_IT(filter = static_cast<ObTTLWhiteFilterExecutor *>(executor))) {
  } else if (OB_FAIL(filter->init(ttl_filter_info, cols_desc))) {
    LOG_WARN("failed to init", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(node)) {
      node->~ObPushdownFilterNode();
      node = nullptr;
    }
    if (OB_NOT_NULL(filter)) {
      filter->~ObTTLWhiteFilterExecutor();
      filter = nullptr;
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("TTL and filter executor not inited", KR(ret));
  } else {
    filtered = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_ttl_filter_cnt_ && !filtered; ++i) {
      if (OB_FAIL(ttl_filters_.at(i)->filter(row, filtered))) {
        LOG_WARN("failed to filter ttl filter", K(ret), K(i), KPC(ttl_filters_.at(i)));
      }
    }
  }

  return ret;
}

int ObTTLAndFilterExecutor::execute_logic_filter(
    PushdownFilterInfo &filter_info,
    blocksstable::ObIMicroBlockRowScanner *micro_scanner,
    const bool use_vectorize,
    common::ObBitmap &result)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < valid_ttl_filter_cnt_; i++) {
    const ObBitmap *child_result = nullptr;
    if (OB_ISNULL(ttl_filters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child filter", K(ret), K(i));
    } else if (OB_FAIL(
                   ttl_filters_.at(i)->execute(this, filter_info, micro_scanner, use_vectorize))) {
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