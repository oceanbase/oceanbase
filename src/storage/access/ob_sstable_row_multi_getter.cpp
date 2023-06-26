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
#include "ob_sstable_row_multi_getter.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
namespace storage {

ObSSTableRowMultiGetter::~ObSSTableRowMultiGetter()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
}

void ObSSTableRowMultiGetter::reset()
{
  ObStoreRowIterator::reset();
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_getter_, ObMicroBlockRowGetter);
  is_opened_ = false;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  prefetcher_.reset();
}

void ObSSTableRowMultiGetter::reuse()
{
  ObStoreRowIterator::reuse();
  is_opened_ = false;
  prefetcher_.reuse();
}

int ObSSTableRowMultiGetter::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObStoreRowIterator has been opened", K(ret));
  } else if (OB_UNLIKELY(nullptr == query_range ||
                         nullptr == table ||
                         !table->is_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObSSTableRowMultiGetter", K(ret), KP(query_range), KP(table));
  } else {
    sstable_ = static_cast<ObSSTable *>(table);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(
                  type_, *sstable_, iter_param, access_ctx, query_range))) {
        LOG_WARN("fail to init prefetcher, ", K(ret));
      }
    } else if (OB_FAIL(prefetcher_.switch_context(
        type_, *sstable_, iter_param.get_read_info()->get_datum_utils(), access_ctx, query_range))) {
      LOG_WARN("fail to switch context for prefetcher, ", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.multi_prefetch())) {
        LOG_WARN("Fail to prefetch data", K(ret));
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

int ObSSTableRowMultiGetter::inner_get_next_row(const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSSTableRowMultiGetter has not been opened", K(ret), KP(this));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.multi_prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      } else if (prefetcher_.fetch_rowkey_idx_ >= prefetcher_.prefetch_rowkey_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end())) {
          ret = OB_ITER_END;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
        }
      } else if (!prefetcher_.current_read_handle().cur_prefetch_end_) {
        continue;
      } else if (OB_FAIL(fetch_row(prefetcher_.current_read_handle(), store_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          prefetcher_.mark_cur_rowkey_fetched(prefetcher_.current_read_handle());
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("Fail to fetch row", K(ret));
        }
      } else {
        prefetcher_.mark_cur_rowkey_fetched(prefetcher_.current_read_handle());
        break;
      }
    }

    if (OB_SUCC(ret) && nullptr != store_row) {
      ObDatumRow &datum_row = *const_cast<ObDatumRow *>(store_row);
      if (!store_row->row_flag_.is_not_exist() &&
          iter_param_->need_scn_ &&
          OB_FAIL(set_row_scn(*iter_param_, *sstable_, store_row))) {
        LOG_WARN("failed to set row scn", K(ret));
      }
      EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
      LOG_DEBUG("inner get next row", K(*store_row));
    }
  }
  return ret;
}

int ObSSTableRowMultiGetter::fetch_row(ObSSTableReadHandle &read_handle, const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (nullptr == micro_getter_) {
    if (nullptr == (micro_getter_ = OB_NEWx(ObMicroBlockRowGetter, access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate micro block getter", K(ret));
    } else if (OB_FAIL(micro_getter_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro block row getter", K(ret));
    }
    //switch context each row due to the cache will be disabled if too many rows getted
  } else if (OB_FAIL(micro_getter_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
    STORAGE_LOG(WARN, "Fail to switch context", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_getter_->get_row(
              read_handle,
              store_row,
              macro_block_reader_))) {
    LOG_WARN("Fail to get row", K(ret));
  }
  return ret;
}

}
}
