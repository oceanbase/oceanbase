/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/parquet/ob_parquet_page_buffer.h"

#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{

ObCoalescedBuffer::ObCoalescedBuffer(int64_t file_size, int64_t offset, int64_t length)
    : file_size_(file_size), offset_(offset), length_(length),
      alloc_(common::ObMemAttr(MTL_ID(), "CoalescedBuffer"))
{
  // int64_t end = offset_ + length_;
  // offset_ = (offset_ / DEFAULT_MACRO_BLOCK_SIZE) * DEFAULT_MACRO_BLOCK_SIZE;
  // // 末尾向上取整，且不超过文件的 file_size
  // end = std::min((end + DEFAULT_MACRO_BLOCK_SIZE - 1) / DEFAULT_MACRO_BLOCK_SIZE * DEFAULT_MACRO_BLOCK_SIZE, file_size_);
  // length_ = end - offset_;
}

ObCoalescedBuffer::~ObCoalescedBuffer()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf_)) {
    if (!has_waited_) {
      if (OB_FAIL(io_handle_.wait())) {
        LOG_WARN("io_handle wait failed", K(ret));
      }
    }
    alloc_.free(buf_);
    buf_ = NULL;
  }
}

int ObCoalescedBuffer::alloc_buffer()
{
  int ret = OB_SUCCESS;
  if (NULL != buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer already allocated", K(ret), K(buf_), K(length_));
  } else if (OB_ISNULL(buf_ = (char *)alloc_.alloc(length_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("buffer allocate failed", K(ret));
  }
  return ret;
}

int ObCoalescedBuffer::wait(ObLakeTableParquetPageMgrMetrics &metrics)
{
  int ret = OB_SUCCESS;
  if (has_waited_) {
    // do nothing
  } else {
    int64_t start_ns = ObTimeUtility::current_time_ns();
    if (OB_FAIL(io_handle_.wait())) {
      LOG_WARN("io_handle wait failed", K(ret));
    } else {
      has_waited_ = true;
      int64_t cost_ns = ObTimeUtility::current_time_ns() - start_ns;
      metrics.total_io_wait_time_ns_ += cost_ns;
      metrics.max_io_wait_time_ns_ = MAX(metrics.max_io_wait_time_ns_, cost_ns);
    }
  }
  return ret;
}

int64_t ObCoalescedBuffer::end() const
{
  return offset_ + length_;
}

int ObParquetCachedPageBuffer::get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                                                std::shared_ptr<arrow::Buffer> &buffer,
                                                bool &is_cache_hit,
                                                bool &is_decompressed)
{
  UNUSED(metrics);
  int ret = OB_SUCCESS;
  ObExternalDataPageCacheValue *value = cache_handle_.value_;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached data is null");
  } else if (OB_UNLIKELY(!value->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached data is invalid");
  } else {
    is_cache_hit = true;
    is_decompressed = value->is_decompressed();
    buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t *>(value->get_buffer()),
                                             value->get_valid_data_size());
  }
  return ret;
}

ObParquetPageBufferSlice::ObParquetPageBufferSlice(
    ObCoalescedBuffer *coalesced_buffer,
    int64_t offset,
    int64_t length)
    : coalesced_buffer_(coalesced_buffer), offset_(offset), length_(length)
{
}

ObParquetPageBufferSlice::~ObParquetPageBufferSlice()
{
  if (OB_NOT_NULL(coalesced_buffer_)) {
    coalesced_buffer_->ref_counts_--;
  }
}

int ObParquetPageBufferSlice::get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
  std::shared_ptr<arrow::Buffer> &buffer, bool &is_cache_hit, bool &is_decompressed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(coalesced_buffer_) || OB_ISNULL(coalesced_buffer_->buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coalesced buffer is null");
  } else if (OB_UNLIKELY((offset_ + length_) > coalesced_buffer_->length_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid offset/length", K(offset_), K(length_));
  } else if (OB_FAIL(coalesced_buffer_->wait(metrics))) {
    LOG_WARN("coalesced buffer wait failed", K(ret));
  } else {
    is_cache_hit = false;
    is_decompressed = false;
    buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<uint8_t *>(coalesced_buffer_->buf_ + offset_),
        length_);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
