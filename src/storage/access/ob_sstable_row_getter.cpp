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
#include "ob_sstable_row_getter.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
ObSSTableRowGetter::~ObSSTableRowGetter()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
}

void ObSSTableRowGetter::reset()
{
  ObStoreRowIterator::reset();
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
  is_opened_ = false;
  has_fetched_ = false;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  rowkey_ = nullptr;
  prefetcher_.reset();
}

void ObSSTableRowGetter::reuse()
{
  ObStoreRowIterator::reuse();
  is_opened_ = false;
  has_fetched_ = false;
  prefetcher_.reuse();
}

int ObSSTableRowGetter::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowGetter has been opened", K(ret));
  } else if (OB_UNLIKELY(nullptr == query_range ||
                         nullptr == table ||
                         !table->is_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObSSTableRowGetter", K(ret), KP(query_range), KP(table));
  } else {
    sstable_ = static_cast<ObSSTable *>(table);
    rowkey_ = static_cast<const blocksstable::ObDatumRowkey *>(query_range);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    read_handle_.rowkey_ = rowkey_;
    read_handle_.range_idx_ = 0;
    read_handle_.is_get_ = true;
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(
                  type_, *sstable_, iter_param, access_ctx, query_range))) {
        LOG_WARN("fail to init prefetcher", K(ret));
      }
    } else if (OB_FAIL(prefetcher_.switch_context(
        type_, *sstable_, iter_param.get_read_info()->get_datum_utils(), access_ctx, query_range))) {
      LOG_WARN("fail to switch context for prefetcher, ", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.single_prefetch(read_handle_))) {
        LOG_WARN("ObSSTableRowGetter prefetch failed ", K(ret));
      } else {
        is_opened_ = true;
      }
    }
  }

  if (OB_UNLIKELY(!is_opened_)) {
    reset();
  }
  return ret;
}

int ObSSTableRowGetter::inner_get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSSTableRowGetter has not been opened", K(ret));
  } else if (has_fetched_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fetch_row(read_handle_, store_row))) {
    if (OB_ITER_END == ret) {
      has_fetched_ = true;
    } else {
      LOG_WARN("Fail to fetch row", K(ret));
    }
  } else if (nullptr != store_row) {
    ObDatumRow &datum_row = *const_cast<ObDatumRow *>(store_row);
    if (!store_row->row_flag_.is_not_exist() &&
        iter_param_->need_scn_ &&
        OB_FAIL(set_row_scn(*iter_param_, *sstable_, store_row))) {
      LOG_WARN("failed to set row scn", K(ret));
    }
    EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
    LOG_DEBUG("inner get next row", KPC(store_row), KPC(rowkey_));
  }
  return ret;
}

int ObSSTableRowGetter::fetch_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (nullptr == micro_getter_) {
    if (nullptr == (micro_getter_ = OB_NEWx(ObMicroBlockRowGetter, access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate micro block row getter", K(ret));
    } else if (OB_FAIL(micro_getter_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro block row getter", K(ret));
    }
  } else if (OB_FAIL(micro_getter_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("Fail to switch context", K(ret));
  }
  LOG_DEBUG("start to fetch row", KPC(rowkey_), K(read_handle));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_getter_->get_row(
              read_handle,
              store_row,
              macro_block_reader_))) {
    LOG_WARN("Fail to get row", K(ret));
  } else {
    has_fetched_ = true;
  }
  return ret;
}

}
}
