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

#include "ob_sstable_row_multi_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

ObSSTableRowMultiScanner::ObSSTableRowMultiScanner()
    : has_prefetched_(false), prefetch_range_idx_(0), orig_ranges_(NULL)
{}

ObSSTableRowMultiScanner::~ObSSTableRowMultiScanner()
{}

void ObSSTableRowMultiScanner::reset()
{
  ObSSTableRowScanner::reset();
  has_prefetched_ = false;
  prefetch_range_idx_ = 0;
  orig_ranges_ = NULL;
}

void ObSSTableRowMultiScanner::reuse()
{
  ObSSTableRowScanner::reuse();
  has_prefetched_ = false;
  prefetch_range_idx_ = 0;
  orig_ranges_ = NULL;
}

int ObSSTableRowMultiScanner::get_handle_cnt(
    const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL == query_range) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else {
    read_handle_cnt = MULTISCAN_READ_HANDLE_CNT;
    micro_handle_cnt = MULTISCAN_MICRO_HANDLE_CNT;
  }
  return ret;
}

int ObSSTableRowMultiScanner::prefetch_read_handle(ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableSkipRangeCtx* skip_ctx = NULL;

  if (!has_prefetched_) {
    prefetch_range_idx_ = 0;
    has_prefetched_ = true;
  }

  while (OB_SUCC(ret)) {
    if (prefetch_range_idx_ >= ranges_->count() || prefetch_range_idx_ < 0) {
      ret = OB_ITER_END;
      STORAGE_LOG(DEBUG, "prefetch range idx reaches end");
    } else {
      const ObExtStoreRange& range = ranges_->at(prefetch_range_idx_);
      STORAGE_LOG(DEBUG, "next range", K(range), K(prefetch_range_idx_), KP(this));
      if (range.is_single_rowkey()) {
        // get
        if (OB_FAIL(get_read_handle(range.get_ext_start_key(), read_handle))) {
          STORAGE_LOG(WARN, "Fail to get read handle, ", K(ret), K(range), K_(prefetch_range_idx));
        } else {
          read_handle.range_idx_ = prefetch_range_idx_;
          prefetch_range_idx_++;
          if (OB_FAIL(get_skip_range_ctx(read_handle, INT64_MAX, skip_ctx))) {
            STORAGE_LOG(WARN, "fail to get skip range ctx", K(ret));
          } else if (OB_UNLIKELY(nullptr != skip_ctx && skip_ctx->need_skip_)) {
            STORAGE_LOG(DEBUG, "skip current get range", K(ret), K(read_handle), K(*skip_ctx));
          } else {
            STORAGE_LOG(DEBUG, "get next read handle", K(read_handle), K(range));
            break;
          }
        }
      } else {
        // scan
        if (OB_FAIL(prefetch_range(prefetch_range_idx_, ranges_->at(prefetch_range_idx_), read_handle))) {
          if (OB_ITER_END == ret) {
            has_find_macro_ = false;
            prefetch_range_idx_++;
            ret = OB_SUCCESS;
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowMultiScanner::fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (0 != read_handle.is_get_) {
    if (OB_FAIL(get_row(read_handle, store_row))) {
      STORAGE_LOG(WARN, "Fail to get row, ", K(ret));
    }
  } else {
    if (OB_FAIL(ObSSTableRowScanner::fetch_row(read_handle, store_row))) {
      if (OB_ITER_END != ret && OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "Fail to scan row, ", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableRowMultiScanner::skip_range(
    int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange* new_range = NULL;
  if (OB_UNLIKELY(range_idx < 0 || range_idx >= ranges_->count() || NULL == gap_key)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx), KP(gap_key));
  } else if (OB_FAIL(generate_new_range(range_idx, *gap_key, include_gap_key, ranges_->at(range_idx), new_range))) {
    STORAGE_LOG(WARN, "fail to get skip range", K(ret));
  } else if (NULL != new_range) {
    if (OB_FAIL(skip_range_impl(range_idx, ranges_->at(range_idx), *new_range))) {
      STORAGE_LOG(WARN, "fail to skip range", K(ret), K(range_idx));
    }
  }
  return ret;
}

int ObSSTableRowMultiScanner::get_range_count(const void* query_range, int64_t& range_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "query range is null", K(ret));
  } else {
    const common::ObIArray<common::ObExtStoreRange>* ranges =
        static_cast<const common::ObIArray<common::ObExtStoreRange>*>(query_range);
    range_count = ranges->count();
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
