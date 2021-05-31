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

#include "ob_sstable_row_multi_getter.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;

namespace oceanbase {
namespace storage {

ObSSTableRowMultiGetter::ObSSTableRowMultiGetter() : ObSSTableRowGetter(), prefetch_idx_(0)
{}

ObSSTableRowMultiGetter::~ObSSTableRowMultiGetter()
{}

int ObSSTableRowMultiGetter::get_handle_cnt(
    const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL == query_range) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else {
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys =
        reinterpret_cast<const common::ObIArray<common::ObExtStoreRowkey>*>(query_range);
    read_handle_cnt = min(rowkeys->count(), MULTIGET_READ_HANDLE_CNT);
    micro_handle_cnt = min(rowkeys->count(), MULTIGET_MICRO_HANDLE_CNT);
  }
  return ret;
}

int ObSSTableRowMultiGetter::prefetch_read_handle(ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableSkipRangeCtx* skip_ctx = NULL;
  if (!has_prefetched_) {
    prefetch_idx_ = 0;
    has_prefetched_ = true;
  }

  while (OB_SUCC(ret)) {
    if (prefetch_idx_ >= rowkeys_->count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(get_read_handle(rowkeys_->at(prefetch_idx_), read_handle))) {
      STORAGE_LOG(WARN, "Fail to get read handle, ", K(ret), K(prefetch_idx_), K(rowkeys_->at(prefetch_idx_)));
    } else {
      read_handle.range_idx_ = prefetch_idx_;
      prefetch_idx_++;
      if (OB_FAIL(get_skip_range_ctx(read_handle, INT64_MAX, skip_ctx))) {
        STORAGE_LOG(WARN, "fail to get skip range ctx", K(ret));
      } else if (OB_UNLIKELY(nullptr != skip_ctx && skip_ctx->need_skip_)) {
        STORAGE_LOG(DEBUG, "skip current get range", K(ret), K(read_handle), K(*skip_ctx));
      } else {
        STORAGE_LOG(DEBUG, "get next read handle", K(read_handle), K(rowkeys_->at(prefetch_idx_ - 1)));
        break;
      }
    }
  }
  return ret;
}

void ObSSTableRowMultiGetter::reset()
{
  ObSSTableRowGetter::reset();
  prefetch_idx_ = 0;
}

void ObSSTableRowMultiGetter::reuse()
{
  ObSSTableRowGetter::reuse();
  prefetch_idx_ = 0;
}

int ObSSTableRowMultiGetter::get_range_count(const void* query_range, int64_t& range_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "query range is null", K(ret), KP(query_range));
  } else {
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys =
        static_cast<const common::ObIArray<common::ObExtStoreRowkey>*>(query_range);
    range_count = rowkeys->count();
  }
  return ret;
}

int ObSSTableRowMultiGetter::skip_range(
    int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0 || nullptr == gap_key || !include_gap_key)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx), KP(gap_key), K(include_gap_key));
  } else {
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys =
        static_cast<const common::ObIArray<common::ObExtStoreRowkey>*>(query_range_);
    const common::ObStoreRowkey* rowkey = &rowkeys->at(range_idx).get_store_rowkey();
    if (gap_key != rowkey) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid gap rowkey", K(ret), K(*gap_key), K(*rowkey), KP(gap_key), KP(rowkey));
    } else {
      skip_ctx_.reset();
      skip_ctx_.range_idx_ = range_idx;
      skip_ctx_.macro_idx_ = 0;
    }
  }
  return ret;
}

int ObSSTableRowMultiGetter::get_skip_range_ctx(
    ObSSTableReadHandle& read_handle, const int64_t cur_micro_idx, ObSSTableSkipRangeCtx*& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(cur_micro_idx);
  ctx = nullptr;
  if (read_handle.range_idx_ < skip_ctx_.range_idx_) {
    skip_ctx_.need_skip_ = true;
    ctx = &skip_ctx_;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
