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

#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;

ObDirectLoadLobMetaIterParam::ObDirectLoadLobMetaIterParam()
  : datum_utils_(nullptr), col_descs_(nullptr), dml_row_handler_(nullptr)
{
}

ObDirectLoadLobMetaIterParam::~ObDirectLoadLobMetaIterParam() {}

bool ObDirectLoadLobMetaIterParam::is_valid() const
{
  return tablet_id_.is_valid() &&
         table_data_desc_.is_valid() &&
         OB_NOT_NULL(datum_utils_) && datum_utils_->is_valid() &&
         OB_NOT_NULL(col_descs_) &&
         OB_NOT_NULL(dml_row_handler_);
}

ObDirectLoadLobMetaRowIter::ObDirectLoadLobMetaRowIter()
  : allocator_("TLD_LobRowIter"),
    origin_table_(nullptr),
    origin_scanner_(nullptr),
    range_allocator_("TLD_LobRange"),
    lob_id_row_(nullptr),
    lob_id_row_cnt_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  range_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadLobMetaRowIter::~ObDirectLoadLobMetaRowIter()
{
  if (nullptr != origin_scanner_) {
    origin_scanner_->~ObDirectLoadOriginTableScanner();
    allocator_.free(origin_scanner_);
    origin_scanner_ = nullptr;
  }
}

int ObDirectLoadLobMetaRowIter::init(const ObDirectLoadLobMetaIterParam &param,
                                     ObDirectLoadOriginTable &origin_table,
                                     const ObDirectLoadTableHandleArray &sstable_array,
                                     const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("get not inited error", K(ret));
  } else {
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = &conflict_handler_;
    if (OB_FAIL(init_range())) {
      LOG_WARN("fail to init range", KR(ret));
    } else if (OB_FAIL(scan_range_.assign(param.tablet_id_, range))) {
      LOG_WARN("fail to assign range", KR(ret));
    } else if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, scan_range_))) {
      LOG_WARN("fail to init scan merge ", KR(ret));
    } else {
      param_ = param;
      origin_table_ = &origin_table;
      // set parent params
      column_count_ = ObLobMetaUtil::LOB_META_COLUMN_CNT;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadLobMetaRowIter::init_range()
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datums = nullptr;
  const int64_t count = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT * 2;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageDatum) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else {
    datums = new (buf) ObStorageDatum[count];
    range_.start_key_.assign(datums, ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT);
    range_.end_key_.assign(datums + ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT,
                           ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT);
    range_.start_key_.datums_[ObLobMetaUtil::SEQ_ID_COL_ID].set_min();
    range_.end_key_.datums_[ObLobMetaUtil::SEQ_ID_COL_ID].set_max();
    range_.set_left_open();
    range_.set_right_open();
  }
  return ret;
}

int ObDirectLoadLobMetaRowIter::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("get not inited error", K(ret));
  } else {
    if (nullptr == origin_scanner_) {
      if (OB_FAIL(switch_next_lob_id())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to switch next lob id", KR(ret));
        }
      }
    }
    while (OB_SUCC(ret) && nullptr == result_row) {
      const ObDirectLoadDatumRow *datum_row = nullptr;
      if (OB_FAIL(origin_scanner_->get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_lob_id())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next lob id", KR(ret));
            }
          }
        }
      } else {
        datum_row_.storage_datums_ = datum_row->storage_datums_;
        datum_row_.count_ = datum_row->count_;
        result_row = &datum_row_;
        ++lob_id_row_cnt_;
        if (datum_row_.is_delete_) {
          if (OB_FAIL(param_.dml_row_handler_->handle_delete_row(param_.tablet_id_, datum_row_))) {
            LOG_WARN("fail to handle delete row", KR(ret));
          }
        } else {
          if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(param_.tablet_id_, datum_row_))) {
            LOG_WARN("fail to handle insert row", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadLobMetaRowIter::switch_next_lob_id()
{
  int ret = OB_SUCCESS;
  // lob_id都是outrow的, 必须能扫到行
  if (OB_UNLIKELY(nullptr != lob_id_row_ && lob_id_row_cnt_ == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob id row cnt", KR(ret), KPC(lob_id_row_));
  } else if (FALSE_IT(lob_id_row_cnt_ = 0)) {
  } else if (OB_FAIL(scan_merge_.get_next_row(lob_id_row_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next lob", K(ret));
    }
  } else if (OB_UNLIKELY(lob_id_row_->count_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row count", KR(ret), KPC(lob_id_row_));
  } else {
    datum_row_.seq_no_ = lob_id_row_->seq_no_;
    datum_row_.is_delete_ = lob_id_row_->is_delete_;
    range_allocator_.reuse();
    range_.start_key_.datums_[ObLobMetaUtil::LOB_ID_COL_ID] = lob_id_row_->storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID];
    range_.end_key_.datums_[ObLobMetaUtil::LOB_ID_COL_ID] = lob_id_row_->storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID];
    if (OB_FAIL(
          range_.start_key_.prepare_memtable_readable(*param_.col_descs_, range_allocator_))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range_));
    } else if (OB_FAIL(
                 range_.end_key_.prepare_memtable_readable(*param_.col_descs_, range_allocator_))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range_));
    } else {
      if (nullptr == origin_scanner_) {
        if (OB_FAIL(origin_table_->scan(range_, allocator_, origin_scanner_, true /*skip_read_lob*/))) {
          LOG_WARN("fail to scan origin table", KR(ret));
        }
      } else {
        if (OB_FAIL(origin_scanner_->open(range_))) {
          LOG_WARN("fail to open origin scanner", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
