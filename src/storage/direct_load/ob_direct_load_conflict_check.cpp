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

#include "storage/direct_load/ob_direct_load_conflict_check.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace sql;

/**
 * ObDirectLoadConflictCheckParam
 */

ObDirectLoadConflictCheckParam::ObDirectLoadConflictCheckParam()
  : origin_table_(nullptr),
    range_(nullptr),
    col_descs_(nullptr),
    datum_utils_(nullptr),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadConflictCheckParam::~ObDirectLoadConflictCheckParam()
{
}

bool ObDirectLoadConflictCheckParam::is_valid() const
{
  return tablet_id_.is_valid() &&
         table_data_desc_.is_valid() &&
         nullptr != origin_table_ &&
         nullptr != range_ && range_->is_valid() &&
         nullptr != col_descs_ &&
         nullptr != datum_utils_ &&
         nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadConflictCheck
 */

ObDirectLoadConflictCheck::ObDirectLoadConflictCheck()
  : allocator_("TLD_CfltCheck"),
    range_allocator_("TLD_RCfltCheck"),
    load_iter_(nullptr),
    origin_scanner_(nullptr),
    origin_row_(nullptr),
    origin_iter_is_end_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  range_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadConflictCheck::~ObDirectLoadConflictCheck()
{
  if (origin_scanner_ != nullptr) {
    origin_scanner_->~ObDirectLoadOriginTableScanner();
    allocator_.free(origin_scanner_);
    origin_scanner_ = nullptr;
  }
}

int ObDirectLoadConflictCheck::init(
    const ObDirectLoadConflictCheckParam &param,
    ObDirectLoadIStoreRowIterator *load_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || nullptr == load_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), KP(load_iter));
  } else {
    param_ = param;
    load_iter_ = load_iter;
    new_range_ = *param_.range_;
    if (OB_FAIL(param_.origin_table_->scan(*param_.range_, allocator_, origin_scanner_, true/*skip_read_lob*/))) {
      LOG_WARN("fail to scan origin table", KR(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadConflictCheck::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    while (OB_SUCC(ret) && nullptr == result_row) {
      const ObDirectLoadDatumRow *datum_row = nullptr;
      if (OB_FAIL(load_iter_->get_next_row(datum_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next row", KR(ret));
        }
      } else if (OB_UNLIKELY(datum_row->is_delete_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected delete row", KR(ret), KPC(datum_row));
      } else if (OB_FAIL(handle_get_next_row_finish(datum_row, result_row))) {
        LOG_WARN("fail to handle get next row finish", KR(ret), KPC(datum_row));
      }
    }
  }

  return ret;
}

int ObDirectLoadConflictCheck::handle_get_next_row_finish(
    const ObDirectLoadDatumRow *load_row,
    const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  int64_t skip_count = 0;
  while (OB_SUCC(ret) && !origin_iter_is_end_) {
    if (origin_row_ == nullptr) {
      if (OB_FAIL(origin_scanner_->get_next_row(origin_row_))) {
        if (ret == OB_ITER_END) {
          origin_iter_is_end_ = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next row", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret) && origin_row_ != nullptr) {
      if (OB_FAIL(compare(*load_row, *origin_row_, cmp_ret))) {
        LOG_WARN("fail to compare", KR(ret), K(skip_count));
      } else {
        if (cmp_ret <= 0) {
          break;
        } else {
          skip_count++;
          origin_row_ = nullptr;
          if (skip_count == SKIP_THESHOLD) {
            skip_count = 0;
            if (OB_FAIL(reopen_origin_scanner(load_row))) {
              LOG_WARN("fail to reopen origin scanner", KR(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr != origin_row_ && cmp_ret == 0) {
      // 有冲突
      if (OB_FAIL(param_.dml_row_handler_->handle_update_row(param_.tablet_id_, *origin_row_, *load_row, result_row))) {
        LOG_WARN("fail to handle update row", KR(ret), KP(origin_row_), KP(load_row));
      } else if (result_row == origin_row_) {
        // 不吐出origin_row_
        result_row = nullptr;
      }
      origin_row_ = nullptr;
    } else {
      // 无冲突
      result_row = load_row;
      if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(param_.tablet_id_, *result_row))) {
        LOG_WARN("fail to handle insert row", KR(ret), KP(result_row));
      }
    }
  }
  return ret;
}

int ObDirectLoadConflictCheck::compare(
    const ObDirectLoadDatumRow &first_row,
    const ObDirectLoadDatumRow &second_row,
    int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey first_key(first_row.storage_datums_, param_.table_data_desc_.rowkey_column_num_);
  ObDatumRowkey second_key(second_row.storage_datums_, param_.table_data_desc_.rowkey_column_num_);
  if (OB_FAIL(first_key.compare(second_key, *param_.datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare", KR(ret));
  }

  return ret;
}

int ObDirectLoadConflictCheck::reopen_origin_scanner(const ObDirectLoadDatumRow *datum_row)
{
  int ret = OB_SUCCESS;
  range_allocator_.reuse();
  ObDatumRowkey start_key(datum_row->storage_datums_, param_.table_data_desc_.rowkey_column_num_);
  if (OB_FAIL(start_key.deep_copy(new_range_.start_key_, range_allocator_))) {
    LOG_WARN("fail to copy start_key", KR(ret));
  } else if (OB_FAIL(new_range_.start_key_.prepare_memtable_readable(*param_.col_descs_, range_allocator_))) {
    LOG_WARN("fail to prepare_memtable_readable", KR(ret));
  } else {
    new_range_.set_left_closed();
    if (OB_FAIL(origin_scanner_->open(new_range_))) {
      LOG_WARN("fail to open origin scanner", KR(ret), K(new_range_));
    }
  }

  return ret;
}

/**
 * ObDirectLoadSSTableConflictCheck
 */

ObDirectLoadSSTableConflictCheck::ObDirectLoadSSTableConflictCheck()
  : is_inited_(false)
{
}

ObDirectLoadSSTableConflictCheck::~ObDirectLoadSSTableConflictCheck()
{
}

int ObDirectLoadSSTableConflictCheck::init(
    const ObDirectLoadConflictCheckParam &param,
    const ObDirectLoadTableHandleArray &sstable_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else {
    ObDirectLoadSSTableScanMergeParam scan_merge_param;
    scan_merge_param.tablet_id_ = param.tablet_id_;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = param.dml_row_handler_;
    if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, *param.range_))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(conflict_check_.init(param, &scan_merge_))) {
      LOG_WARN("fail to init conflict_check_", KR(ret));
    } else {
      // set parent params
      row_flag_ = param.table_data_desc_.row_flag_;
      column_count_ = param.table_data_desc_.column_count_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadSSTableConflictCheck::get_next_row(const ObDirectLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    ret = conflict_check_.get_next_row(datum_row);
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableConflictCheck
 */

ObDirectLoadMultipleSSTableConflictCheck::ObDirectLoadMultipleSSTableConflictCheck()
  : is_inited_(false)
{
}

ObDirectLoadMultipleSSTableConflictCheck::~ObDirectLoadMultipleSSTableConflictCheck()
{
}

int ObDirectLoadMultipleSSTableConflictCheck::init(
    const ObDirectLoadConflictCheckParam &param,
    const ObDirectLoadTableHandleArray &sstable_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (OB_FAIL(range_.assign(param.tablet_id_, *param.range_))) {
    LOG_WARN("fail to assign range", KR(ret));
  } else {
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = param.dml_row_handler_;
    if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, range_))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(conflict_check_.init(param, &scan_merge_))) {
      LOG_WARN("fail to init conflict_check_", KR(ret));
    } else {
      // set parent params
      row_flag_ = param.table_data_desc_.row_flag_;
      column_count_ = param.table_data_desc_.column_count_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadMultipleSSTableConflictCheck::get_next_row(const ObDirectLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    ret = conflict_check_.get_next_row(datum_row);
  }

  return ret;
}



} // namespace storage
} // namespace oceanbase
