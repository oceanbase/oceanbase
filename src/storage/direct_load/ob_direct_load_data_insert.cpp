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

#include "storage/direct_load/ob_direct_load_data_insert.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace sql;

/**
 * ObDirectLoadSSTableScanMergeParam
 */

ObDirectLoadDataInsertParam::ObDirectLoadDataInsertParam()
  : store_column_count_(0),
    datum_utils_(nullptr),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadDataInsertParam::~ObDirectLoadDataInsertParam()
{
}

bool ObDirectLoadDataInsertParam::is_valid() const
{
  return tablet_id_.is_valid() && store_column_count_ > 0 && table_data_desc_.is_valid() &&
         nullptr != datum_utils_ && nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadDataInsert
 */

ObDirectLoadDataInsert::ObDirectLoadDataInsert()
  : load_iter_(nullptr), is_inited_(false)
{
}

ObDirectLoadDataInsert::~ObDirectLoadDataInsert()
{
}

int ObDirectLoadDataInsert::init(
    const ObDirectLoadDataInsertParam &param,
    ObIStoreRowIterator *load_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDataInsert init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || nullptr == load_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), KP(load_iter));
  } else {
    param_ = param;
    load_iter_ = load_iter;
    is_inited_ = true;
  }

  return ret;
}

int ObDirectLoadDataInsert::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_FAIL(load_iter_->get_next_row(datum_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", KR(ret));
    }
  } else if (OB_UNLIKELY(datum_row->count_ != param_.store_column_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), K(datum_row->count_), K(param_.store_column_count_));
  } else if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(*datum_row))) {
    LOG_WARN("fail to handle insert row", KR(ret), KPC(datum_row));
  }

  return ret;
}

/**
 * ObDirectLoadSSTableDataInsert
 */

ObDirectLoadSSTableDataInsert::ObDirectLoadSSTableDataInsert()
  : is_inited_(false)
{
}

ObDirectLoadSSTableDataInsert::~ObDirectLoadSSTableDataInsert()
{
}

int ObDirectLoadSSTableDataInsert::init(
    const ObDirectLoadDataInsertParam &param,
    const ObIArray<ObDirectLoadSSTable *> &sstable_array,
    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid()) || !range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(range));
  } else {
    ObDirectLoadSSTableScanMergeParam scan_merge_param;
    scan_merge_param.tablet_id_ = param.tablet_id_;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = param.dml_row_handler_;
    if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, range))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(data_insert_.init(param, &scan_merge_))) {
      LOG_WARN("fail to init data insert", KR(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadSSTableDataInsert::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    ret = data_insert_.get_next_row(datum_row);
  }

  return ret;
}

/**
 * ObDirectLoadMultipleSSTableDataInsert
 */

ObDirectLoadMultipleSSTableDataInsert::ObDirectLoadMultipleSSTableDataInsert()
  : is_inited_(false)
{
}

ObDirectLoadMultipleSSTableDataInsert::~ObDirectLoadMultipleSSTableDataInsert()
{
}

int ObDirectLoadMultipleSSTableDataInsert::init(
    const ObDirectLoadDataInsertParam &param,
    const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid()) || !range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(range));
  } else {
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = param.dml_row_handler_;
    if (OB_FAIL(range_.assign(param.tablet_id_, range))) {
      LOG_WARN("fail to assign range", KR(ret));
    } else if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, range_))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(data_insert_.init(param, &scan_merge_))) {
      LOG_WARN("fail to init data insert", KR(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadMultipleSSTableDataInsert::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    ret = data_insert_.get_next_row(datum_row);
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
