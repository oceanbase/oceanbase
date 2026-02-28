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

#include "storage/blocksstable/ob_datum_row.h"
#include "ob_base_version_filter_struct.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace sql
{

int ObBaseVersionFilterNode::set_op_type(const ObRawExpr &)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("Base version filter node should not call this function", K(ret));
  return ret;
}

int ObBaseVersionFilterNode::get_filter_val_meta(common::ObObjMeta &obj_meta) const
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

ObObjType ObBaseVersionFilterNode::get_filter_arg_obj_type(int64_t) const
{
  return obj_meta_.get_type();
}

int ObBaseVersionFilterExecutor::inner_init(const int64_t schema_rowkey_cnt, const int64_t base_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(schema_rowkey_cnt <= 0 || base_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid schema rowkey cnt or base version",
             KR(ret),
             K(schema_rowkey_cnt),
             K(base_version));
  } else {
    ObBaseVersionFilterNode &node = static_cast<ObBaseVersionFilterNode &>(get_filter_node());

    // set col index
    col_idx_ = schema_rowkey_cnt;
    col_obj_meta_.set_int();

    // set value
    cache_datum_.reuse();
    cache_datum_.set_int(base_version);
  }

  // push cache value into datum params(pushdown interface needed)
  // col_ids_, col_params_(skip index interface needed)
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(datum_params_.clear())) {
  } else if (OB_FAIL(datum_params_.push_back(cache_datum_))) {
    LOG_WARN("Failed to push back datum", KR(ret), K(cache_datum_));
  } else if (OB_FALSE_IT(filter_.col_ids_.clear())) {
  } else if (OB_FAIL(filter_.col_ids_.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
    LOG_WARN("Failed to push back col idx", KR(ret), K(col_idx_));
  } else if (OB_FALSE_IT(col_params_.clear())) {
  } else if (OB_FAIL(col_params_.push_back(nullptr))) {
    LOG_WARN("Failed to push back col params", KR(ret));
  }

  return ret;
}

int ObBaseVersionFilterExecutor::switch_info(const int64_t schema_rowkey_cnt, const int64_t base_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Base version filter executor not inited", KR(ret));
  } else if (OB_FAIL(inner_init(schema_rowkey_cnt, base_version))) {
    LOG_WARN("Failed to inner init", KR(ret), K(schema_rowkey_cnt), K(base_version));
  }

  return ret;
}

int ObBaseVersionFilterExecutor::init(const int64_t schema_rowkey_cnt, const int64_t base_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Base version filter executor already inited", KR(ret));
  } else if (OB_FAIL(datum_params_.init(1))) { // base version column is always 1
    LOG_WARN("Failed to init datum params", KR(ret));
  } else if (OB_FAIL(filter_.col_ids_.init(1))) {
    LOG_WARN("Failed to init col ids", KR(ret));
  } else if (OB_FAIL(col_params_.init(1))) {
    LOG_WARN("Failed to init col params", KR(ret));
  } else {
    // the type won't change, so we can get the cmp func just once
    ObBaseVersionFilterNode &node = static_cast<ObBaseVersionFilterNode &>(get_filter_node());
    cmp_func_ = storage::get_datum_cmp_func(node.obj_meta_, node.obj_meta_);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_init(schema_rowkey_cnt, base_version))) {
    LOG_WARN("Failed to inner init", KR(ret), K(schema_rowkey_cnt), K(base_version));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObBaseVersionFilterExecutor::filter(const blocksstable::ObDatumRow &row, bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Base version filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(row.count_ <= col_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected col idx", KR(ret), K(col_idx_), K(row.count_));
  } else if (OB_FAIL(inner_filter(row.storage_datums_[col_idx_], filtered))) {
    LOG_WARN("Failed to inner filter", KR(ret), K(row), KPC(this));
  }

  return ret;
}

int ObBaseVersionFilterExecutor::filter(const blocksstable::ObStorageDatum *datums,
                                        int64_t count,
                                        bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Base version filter executor not inited", KR(ret));
  } else if (OB_UNLIKELY(count == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected count", KR(ret), K(count));
  } else if (OB_FAIL(inner_filter(datums[0], filtered))) {
    LOG_WARN("Failed to inner filter", KR(ret), K(datums[0]), KPC(this));
  }

  return ret;
}

int ObBaseVersionFilterExecutor::inner_filter(const blocksstable::ObStorageDatum &datum,
                                              bool &filtered) const
{
  int ret = OB_SUCCESS;

  if (is_filter_always_true()) {
    // always true means nothing is filtered
    filtered = false;
  } else if (is_filter_always_false()) {
    // always false means everything is filtered
    filtered = true;
  } else if (OB_UNLIKELY(datum.is_nop() || datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected datum", KR(ret), K(datum));
  } else {
    filtered = abs(datum.get_int()) <= abs(cache_datum_.get_int());
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase