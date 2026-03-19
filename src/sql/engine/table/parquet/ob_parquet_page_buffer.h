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

#ifndef OCEANBASE_OB_PARQUET_PAGE_BUFFER_H
#define OCEANBASE_OB_PARQUET_PAGE_BUFFER_H
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/table/ob_external_data_page_cache.h"
#include "sql/engine/table/ob_external_file_access.h"

#include <arrow/buffer.h>

namespace oceanbase
{
namespace sql
{

class ObCoalescedBuffer
{
public:
  ObCoalescedBuffer(int64_t file_size, int64_t offset, int64_t length);
  ~ObCoalescedBuffer();
  int alloc_buffer();
  int64_t end() const;
  int wait(ObLakeTableParquetPageMgrMetrics &metrics);
  char *buf_ = NULL;
  int64_t file_size_;
  int64_t offset_;
  int64_t length_;
  bool has_waited_ = false;  // 判断数据是否已经被 wait 过了，即 buf 是否已经被填充完毕
  int64_t ref_counts_ = 0; // 被多少个 ObParquetPageBufferSlice 引用
  ObExternalFileReadHandle io_handle_;

private:
  common::ObMalloc alloc_;
};

class ObParquetPageBufferBase
{
public:
  ObParquetPageBufferBase() = default;
  virtual ~ObParquetPageBufferBase() = default;
  virtual int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                               std::shared_ptr<arrow::Buffer> &buffer,
                               bool &is_cache_hit,
                               bool &is_decompressed) = 0;
  TO_STRING_EMPTY();
};

class ObParquetCachedPageBuffer final : public ObParquetPageBufferBase
{
public:
  ObParquetCachedPageBuffer() = default;
  virtual ~ObParquetCachedPageBuffer() = default;
  int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                       std::shared_ptr<arrow::Buffer> &buffer,
                       bool &is_cache_hit,
                       bool &is_decompressed);
  ObExternalDataPageCacheValueHandle cache_handle_;
};

class ObParquetPageBufferSlice final : public ObParquetPageBufferBase
{
public:
  ObParquetPageBufferSlice(ObCoalescedBuffer *coalesced_buffer, int64_t offset, int64_t length);
  virtual ~ObParquetPageBufferSlice();
  int get_arrow_buffer(ObLakeTableParquetPageMgrMetrics &metrics,
                       std::shared_ptr<arrow::Buffer> &buffer,
                       bool &is_cache_hit,
                       bool &is_decompressed);

  ObCoalescedBuffer *coalesced_buffer_;
  int64_t offset_; // offset_ in coalesced_buffer_
  int64_t length_; // length_ in coalesced_buffer_
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_OB_PARQUET_PAGE_BUFFER_H
