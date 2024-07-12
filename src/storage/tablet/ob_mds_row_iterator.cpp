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

#include "storage/tablet/ob_mds_row_iterator.h"

#include "lib/ob_errno.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/access/ob_single_merge.h"
#include "storage/access/ob_multiple_get_merge.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/multi_data_source/adapter_define/mds_dump_kv_wrapper.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObMdsRowIterator::ObMdsRowIterator()
  : ObNewRowIterator(ObNewRowIterator::IterType::ObTableScanIterator),
    is_inited_(false),
    access_param_(),
    access_ctx_(),
    get_table_param_(),
    table_scan_range_(),
    table_scan_param_(nullptr),
    multiple_merge_(nullptr)
{
}

ObMdsRowIterator::~ObMdsRowIterator()
{
  reset();
}

int ObMdsRowIterator::init(
    ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle,
    ObStoreCtx &store_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scan_param.is_mds_query_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not mds query request", K(ret), K(scan_param));
  } else {
    const ObRowkeyReadInfo *rowkey_read_info = ObMdsSchemaHelper::get_instance().get_rowkey_read_info();
    common::ObVersionRange version_range;
    version_range.multi_version_start_ = 0;
    version_range.base_version_ = 0;
    version_range.snapshot_version_ = scan_param.fb_snapshot_.get_val_for_tx();

    if (OB_UNLIKELY(!version_range.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid version range", K(ret), K(version_range));
    } else if (OB_FAIL(access_param_.init(scan_param, nullptr/*tablet_handle*/, rowkey_read_info))) {
      LOG_WARN("fail to init access param", K(ret), K(scan_param));
    } else if (OB_FAIL(access_ctx_.init(scan_param, store_ctx, version_range, nullptr/*cached_iter_node*/))) {
      LOG_WARN("fail to init access ctx", K(ret), K(scan_param), K(store_ctx), K(version_range));
    } else if (OB_FAIL(init_get_table_param(scan_param, tablet_handle))) {
      LOG_WARN("fail to init get table param", K(ret), K(scan_param));
    } else if (OB_FAIL(table_scan_range_.init(scan_param))) {
      LOG_WARN("fail to init table scan range", K(ret), K(scan_param));
    } else if (OB_FAIL(init_and_open_iter(scan_param))) {
      LOG_WARN("fail to init and open iter", K(ret));
    } else {
      table_scan_param_ = &scan_param;
      is_inited_ = true;
      LOG_DEBUG("succeed to init mds row iterator", K(ret),
          "ls_id", tablet_handle.get_obj()->get_ls_id(),
          "tablet_id", tablet_handle.get_obj()->get_tablet_id(),
          K(scan_param));
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

void ObMdsRowIterator::reset()
{
  if (nullptr != multiple_merge_) {
    multiple_merge_->~ObMultipleMerge();
    multiple_merge_ = nullptr;
  }
  access_param_.reset();
  access_ctx_.reset();
  get_table_param_.reset();
  table_scan_range_.reset();
  table_scan_param_ = nullptr;
  is_inited_ = false;
}

int ObMdsRowIterator::get_next_row(ObNewRow *&row)
{
  return OB_NOT_IMPLEMENT;
}

int ObMdsRowIterator::get_next_row()
{
  blocksstable::ObDatumRow *row = nullptr;
  return get_next_row(row);
}

int ObMdsRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(multiple_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple merge is null", K(ret), KP_(multiple_merge));
  } else if (OB_FAIL(multiple_merge_->get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next rows", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObMdsRowIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(multiple_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple merge is null", K(ret), KP_(multiple_merge));
  } else if (OB_FAIL(multiple_merge_->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObMdsRowIterator::get_next_mds_kv(common::ObIAllocator &allocator, mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;

  blocksstable::ObDatumRow *row = nullptr;
  if (OB_FAIL(get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret), KP(row), KPC(this));
  } else if (OB_FAIL(convert(allocator, *row, kv))) {
    LOG_WARN("fail to convert datum row to mds dump kv", K(ret), KPC(row));
  }

  return ret;
}

int ObMdsRowIterator::convert(
    common::ObIAllocator &allocator,
    const blocksstable::ObDatumRow &row,
    mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  mds::MdsDumpKVStorageAdapter adapter;

  // TODO(@bowen.gbw): avoid memory copy
  if (OB_FAIL(adapter.convert_from_mds_row(row))) {
    LOG_WARN("fail to convert from mds row", K(ret), K(row));
  } else if (OB_FAIL(kv.convert_from_adapter(allocator, adapter))) {
    LOG_WARN("fail to convert from adapter", K(ret), K(adapter));
  } else {
    LOG_DEBUG("succeed to convert row to mds dump kv", K(ret), K(kv));
  }

  return ret;
}

int ObMdsRowIterator::init_get_table_param(
    const ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(tablet_handle))) {
    LOG_WARN("fail to set tablet handle", K(ret), K(tablet_handle));
  } else {
    get_table_param_.frozen_version_ = scan_param.frozen_version_;
    get_table_param_.sample_info_ = scan_param.sample_info_;
  }
  return ret;
}

int ObMdsRowIterator::init_and_open_iter(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(!table_scan_range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan range is invalid", K(ret), K_(table_scan_range));
  } else if (table_scan_range_.is_get()) {
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys = table_scan_range_.get_rowkeys();
    if (rowkeys.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkeys is empty", K(ret), K(rowkeys));
    } else if (rowkeys.count() == 1 && OB_FAIL(init_and_open_single_get_merge(scan_param))) {
      LOG_WARN("fail to init and open single get merge", K(ret), K(scan_param));
    } else if (rowkeys.count() > 1 && OB_FAIL(init_and_open_multiple_get_merge(scan_param))) {
      LOG_WARN("fail to init and open multiple get merge", K(ret), K(scan_param));
    }
  } else if (table_scan_range_.is_scan()) {
    const common::ObIArray<blocksstable::ObDatumRange> &datum_ranges = table_scan_range_.get_ranges();
    if (datum_ranges.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum ranges is empty", K(ret), K(datum_ranges));
    } else if (datum_ranges.count() == 1 && OB_FAIL(init_and_open_multiple_scan_merge(scan_param))) {
      LOG_WARN("fail to init and open multiple scan merge", K(ret), K(scan_param));
    } else if (datum_ranges.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multiple datum ranges for mds is not supported", K(ret), K(datum_ranges));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan range is not GET or SCAN", K(ret), K_(table_scan_range));
  }

  return ret;
}

int ObMdsRowIterator::init_and_open_single_get_merge(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  void *buf = scan_param.allocator_->alloc(sizeof(ObSingleMerge));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObSingleMerge));
  } else {
    ObSingleMerge *single_merge = new (buf) ObSingleMerge();
    const blocksstable::ObDatumRowkey &rowkey = table_scan_range_.get_rowkeys().at(0);
    if (OB_FAIL(single_merge->init(access_param_, access_ctx_, get_table_param_))) {
      LOG_WARN("fail to init single merge", K(ret));
    } else if (OB_FAIL(single_merge->open(rowkey))) {
      LOG_WARN("fail to open single merge", K(ret), K(rowkey));
    } else {
      multiple_merge_ = single_merge;
    }

    if (OB_FAIL(ret)) {
      single_merge->~ObSingleMerge();
      scan_param.allocator_->free(single_merge);
      single_merge = nullptr;
    }
  }

  return ret;
}

int ObMdsRowIterator::init_and_open_multiple_get_merge(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  void *buf = scan_param.allocator_->alloc(sizeof(ObMultipleGetMerge));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObMultipleGetMerge));
  } else {
    ObMultipleGetMerge *multiple_get_merge = new (buf) ObMultipleGetMerge();
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys = table_scan_range_.get_rowkeys();
    if (OB_FAIL(multiple_get_merge->init(access_param_, access_ctx_, get_table_param_))) {
      LOG_WARN("fail to init multiple get merge", K(ret));
    } else if (OB_FAIL(multiple_get_merge->open(rowkeys))) {
      LOG_WARN("fail to open multiple get merge", K(ret), K(rowkeys));
    } else {
      multiple_merge_ = multiple_get_merge;
    }

    if (OB_FAIL(ret)) {
      multiple_get_merge->~ObMultipleGetMerge();
      scan_param.allocator_->free(multiple_get_merge);
      multiple_get_merge = nullptr;
    }
  }

  return ret;
}

int ObMdsRowIterator::init_and_open_multiple_scan_merge(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  void *buf = scan_param.allocator_->alloc(sizeof(ObMultipleScanMerge));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObMultipleScanMerge));
  } else {
    ObMultipleScanMerge *multiple_scan_merge = new (buf) ObMultipleScanMerge();
    const blocksstable::ObDatumRange &datum_range = table_scan_range_.get_ranges().at(0);
    if (OB_FAIL(multiple_scan_merge->init(access_param_, access_ctx_, get_table_param_))) {
      LOG_WARN("fail to init multiple scan merge", K(ret));
    } else if (OB_FAIL(multiple_scan_merge->open(datum_range))) {
      LOG_WARN("fail to open multiple scan merge", K(ret), K(datum_range));
    } else {
      multiple_merge_ = multiple_scan_merge;
    }

    if (OB_FAIL(ret)) {
      multiple_scan_merge->~ObMultipleScanMerge();
      scan_param.allocator_->free(multiple_scan_merge);
      multiple_scan_merge = nullptr;
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
