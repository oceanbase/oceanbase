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

#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadInsertTableRowIteratorParam
 */

ObDirectLoadInsertTableRowIteratorParam::ObDirectLoadInsertTableRowIteratorParam()
  : lob_column_cnt_(0),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    cmp_funcs_(nullptr),
    column_stat_array_(nullptr),
    lob_builder_(nullptr),
    is_heap_table_(false),
    online_opt_stat_gather_(false),
    px_mode_(false)
{
}

ObDirectLoadInsertTableRowIteratorParam::~ObDirectLoadInsertTableRowIteratorParam()
{
}

/**
 * ObDirectLoadInsertTableRowIterator
 */

ObDirectLoadInsertTableRowIterator::ObDirectLoadInsertTableRowIterator()
: lob_allocator_(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObDirectLoadInsertTableRowIterator::~ObDirectLoadInsertTableRowIterator()
{
}

int ObDirectLoadInsertTableRowIterator::inner_init(
  const ObDirectLoadInsertTableRowIteratorParam &param)
{
  int ret = OB_SUCCESS;
  param_ = param;
  return ret;
}

int ObDirectLoadInsertTableRowIterator::get_next_row(const blocksstable::ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  ObDatumRow *datum_row = nullptr;
  if (OB_FAIL(inner_get_next_row(datum_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to do inner get next row", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (param_.online_opt_stat_gather_ && OB_FAIL(collect_obj(*datum_row))) {
      LOG_WARN("fail to collect obj", KR(ret));
    }
  }
  if (OB_SUCC(ret) && param_.lob_column_cnt_ > 0) {
    lob_allocator_.reuse();
    if (OB_FAIL(handle_lob(*datum_row))) {
      LOG_WARN("fail to handle lob", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result_row = datum_row;
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::collect_obj(const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (param_.is_heap_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.table_data_desc_.column_count_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt + 1];
      const common::ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i + 1).get_cmp_func();
      const ObColDesc &col_desc = param_.col_descs_->at(i + 1);
      ObOptOSGColumnStat *col_stat = param_.column_stat_array_->at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(
              col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.table_data_desc_.rowkey_column_num_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i];
      const common::ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
      const ObColDesc &col_desc = param_.col_descs_->at(i);
      ObOptOSGColumnStat *col_stat = param_.column_stat_array_->at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(
              col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
    for (int64_t i = param_.table_data_desc_.rowkey_column_num_;
         OB_SUCC(ret) && i < param_.table_data_desc_.column_count_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt];
      const common::ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
      const ObColDesc &col_desc = param_.col_descs_->at(i);
      ObOptOSGColumnStat *col_stat = param_.column_stat_array_->at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(
              col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::handle_lob(blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_.lob_builder_->append_lob(lob_allocator_, datum_row))) {
    LOG_WARN("fail to append lob", KR(ret), K(param_.tablet_id_), K(datum_row));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
