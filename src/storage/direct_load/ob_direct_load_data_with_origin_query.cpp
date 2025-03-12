/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_data_with_origin_query.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

ObDirectLoadDataWithOriginQueryParam::ObDirectLoadDataWithOriginQueryParam()
  : tablet_id_(),
    rowkey_count_(0),
    store_column_count_(0),
    origin_table_(nullptr),
    col_descs_(nullptr),
    datum_utils_(nullptr),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadDataWithOriginQueryParam::~ObDirectLoadDataWithOriginQueryParam() {}

bool ObDirectLoadDataWithOriginQueryParam::is_valid() const
{
  return tablet_id_.is_valid() && rowkey_count_ > 0 && store_column_count_ > 0 &&
         nullptr != origin_table_ && nullptr != col_descs_ && nullptr != datum_utils_ &&
         nullptr != dml_row_handler_;
}

/**
 * ObDirectLoadDataWithOriginQuery
 */

ObDirectLoadDataWithOriginQuery::ObDirectLoadDataWithOriginQuery()
  : allocator_("TLD_DRF"),
    rowkey_allocator_("TLD_DRF"),
    load_iter_(nullptr),
    origin_getter_(nullptr),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  rowkey_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadDataWithOriginQuery::~ObDirectLoadDataWithOriginQuery()
{
  if (nullptr != origin_getter_) {
    origin_getter_->~ObDirectLoadOriginTableGetter();
    allocator_.free(origin_getter_);
    origin_getter_ = nullptr;
  }
}

int ObDirectLoadDataWithOriginQuery::init(const ObDirectLoadDataWithOriginQueryParam &param,
                                          ObDirectLoadIStoreRowIterator *load_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDataWithOriginQuery init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || nullptr == load_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param), KP(load_iter));
  } else {
    param_ = param;
    load_iter_ = load_iter;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadDataWithOriginQuery::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDataWithOriginQuery not init", KR(ret), K(is_inited_));
  } else {
    const ObDirectLoadDatumRow *datum_row = nullptr;
    const ObDirectLoadDatumRow *full_row = nullptr;
    if (OB_FAIL(load_iter_->get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_UNLIKELY(datum_row->count_ != param_.table_data_desc_.column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), KPC(datum_row), K(param_.table_data_desc_));
    } else if (OB_FAIL(query_full_row(*datum_row, full_row))) {
      LOG_WARN("fail to query full row", KR(ret));
    } else if (OB_UNLIKELY(full_row->count_ != param_.store_column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", KR(ret), KPC(full_row), K(param_.store_column_count_));
    } else {
      datum_row_.seq_no_ = datum_row->seq_no_;
      datum_row_.is_delete_ = datum_row->is_delete_;
      datum_row_.storage_datums_ = full_row->storage_datums_;
      datum_row_.count_ = full_row->count_;
      result_row = &datum_row_;
      if (result_row->is_delete_) {
        if (OB_FAIL(param_.dml_row_handler_->handle_delete_row(param_.tablet_id_, *result_row))) {
          LOG_WARN("fail to handle delete row", KR(ret));
        }
      } else {
        if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(param_.tablet_id_, *result_row))) {
          LOG_WARN("fail to handle insert row", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadDataWithOriginQuery::query_full_row(const ObDirectLoadDatumRow &rowkey_row,
                                                    const ObDirectLoadDatumRow *&full_row)
{
  int ret = OB_SUCCESS;
  full_row = nullptr;
  rowkey_allocator_.reuse();
  if (OB_FAIL(rowkey_.assign(rowkey_row.storage_datums_, param_.rowkey_count_))) {
    LOG_WARN("fail to assign rowkey", KR(ret), K(rowkey_row), K(param_));
  } else if (OB_FAIL(rowkey_.prepare_memtable_readable(*param_.col_descs_, rowkey_allocator_))) {
    LOG_WARN("fail to prepare_memtable_readable", KR(ret));
  } else {
    if (nullptr == origin_getter_) {
      if (OB_FAIL(param_.origin_table_->get(rowkey_, allocator_, origin_getter_, true/*skip_read_lob*/))) {
        LOG_WARN("fail to get origin table", KR(ret));
      }
    } else {
      if (OB_FAIL(origin_getter_->open(rowkey_))) {
        LOG_WARN("fail to open origin getter", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(origin_getter_->get_next_row(full_row))) {
      LOG_WARN("fail to get next row", KR(ret));
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected full row not found", KR(ret), K(rowkey_row));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableDataWithOriginQuery
 */

ObDirectLoadMultipleSSTableDataWithOriginQuery::ObDirectLoadMultipleSSTableDataWithOriginQuery()
  : is_inited_(false)
{
}

ObDirectLoadMultipleSSTableDataWithOriginQuery::~ObDirectLoadMultipleSSTableDataWithOriginQuery() {}

int ObDirectLoadMultipleSSTableDataWithOriginQuery::init(const ObDirectLoadDataWithOriginQueryParam &param,
                                                         const ObDirectLoadTableHandleArray &sstable_array,
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
    // scan_merge只会处理冲突行, 先复用param.dml_row_handler_
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = param.dml_row_handler_;
    if (OB_FAIL(range_.assign(param.tablet_id_, range))) {
      LOG_WARN("fail to assign range", KR(ret));
    } else if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, range_))) {
      LOG_WARN("fail to init scan merge", KR(ret));
    } else if (OB_FAIL(data_delete_.init(param, &scan_merge_))) {
      LOG_WARN("fail to init data insert", KR(ret));
    } else {
      // set parent params
      row_flag_ = param.table_data_desc_.row_flag_;
      column_count_ = param.store_column_count_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableDataWithOriginQuery::get_next_row(const ObDirectLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    ret = data_delete_.get_next_row(datum_row);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase