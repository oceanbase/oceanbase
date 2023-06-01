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

#include "ob_zstd_compressor.h"
#include "lib/ob_errno.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "ob_zstd_wrapper.h"

using namespace oceanbase;
using namespace common;
using namespace zstd;


static void *ob_zstd_malloc(void *opaque, size_t size)
{
  void *buf = NULL;
  if (NULL != opaque) {
    ObZstdCtxAllocator *allocator = reinterpret_cast<ObZstdCtxAllocator*> (opaque);
    buf = allocator->alloc(size);
  }
  return buf;
}

static void ob_zstd_free(void *opaque, void *address)
{
  if (NULL != opaque) {
    ObZstdCtxAllocator *allocator = reinterpret_cast<ObZstdCtxAllocator*> (opaque);
    allocator->free(address);
  }
}

/**
 * ------------------------------ObZstdCtxAllocator---------------------
 */
ObZstdCtxAllocator::ObZstdCtxAllocator(int64_t tenant_id)
  : allocator_(ObModIds::OB_COMPRESSOR, OB_MALLOC_BIG_BLOCK_SIZE,
               tenant_id)
{
}

ObZstdCtxAllocator::~ObZstdCtxAllocator()
{
}

void* ObZstdCtxAllocator::alloc(size_t size)
{
  return allocator_.alloc(size);
}

void ObZstdCtxAllocator::free(void *addr)
{
  allocator_.free(addr);
}

void ObZstdCtxAllocator::reuse()
{
  allocator_.reuse();
}

void ObZstdCtxAllocator::reset()
{
  allocator_.reset();
}

/**
 * ----------------------------ObZstdCompressor---------------------------
 */
int ObZstdCompressor::compress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  size_t compress_ret_size = 0;
  ObZstdCtxAllocator &zstd_allocator = ObZstdCtxAllocator::get_thread_local_instance();
  OB_ZSTD_customMem zstd_mem = {ob_zstd_malloc, ob_zstd_free, &zstd_allocator};
  dst_data_size = 0;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ",
        K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else if (OB_FAIL(ObZstdWrapper::compress(zstd_mem,
                                          src_buffer,
                                          static_cast<size_t>(src_data_size),
                                          dst_buffer,
                                          static_cast<size_t>(dst_buffer_size),
                                          compress_ret_size))) {
    LIB_LOG(WARN, "failed to compress zstd", K(ret), K(compress_ret_size),
        KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else {
    dst_data_size = compress_ret_size;
  }

  zstd_allocator.reuse();
  return ret;
}

int ObZstdCompressor::decompress(const char *src_buffer,
                                 const int64_t src_data_size,
                                 char *dst_buffer,
                                 const int64_t dst_buffer_size,
                                 int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  size_t decompress_ret_size = 0;
  ObZstdCtxAllocator &zstd_allocator = ObZstdCtxAllocator::get_thread_local_instance();
  OB_ZSTD_customMem zstd_mem = {ob_zstd_malloc, ob_zstd_free, &zstd_allocator};
  dst_data_size = 0;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (OB_FAIL(ObZstdWrapper::decompress(zstd_mem,
                                              src_buffer,
                                              src_data_size,
                                              dst_buffer,
                                              dst_buffer_size,
                                              decompress_ret_size))) {
    LIB_LOG(WARN, "failed to decompress zstd", K(ret), K(decompress_ret_size),
        KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else {
    dst_data_size = decompress_ret_size;
  }
  zstd_allocator.reuse();
  return ret;
}

void ObZstdCompressor::reset_mem()
{
  ObZstdCtxAllocator &zstd_allocator = ObZstdCtxAllocator::get_thread_local_instance();
  zstd_allocator.reset();
}

const char *ObZstdCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::ZSTD_COMPRESSOR];
}

ObCompressorType ObZstdCompressor::get_compressor_type() const
{
  return ObCompressorType::ZSTD_COMPRESSOR;
}

int ObZstdCompressor::get_max_overflow_size(const int64_t src_data_size,
                                            int64_t &max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = (src_data_size >> 7) + 512 + 12;
  }
  return ret;
}

