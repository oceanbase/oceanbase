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

#include "ob_zstd_stream_compressor.h"
#include "lib/ob_errno.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "ob_zstd_wrapper.h"


using namespace oceanbase;
using namespace common;
using namespace zstd;

static void *ob_zstd_stream_malloc(void *opaque, size_t size)
{
  void *buf = NULL;
  if (NULL != opaque) {
    ObZstdStreamCtxAllocator *allocator = reinterpret_cast<ObZstdStreamCtxAllocator*> (opaque);
    buf = allocator->alloc(size);
  }
  return buf;
}

static void ob_zstd_stream_free(void *opaque, void *address)
{
  if (NULL != opaque) {
    ObZstdStreamCtxAllocator *allocator = reinterpret_cast<ObZstdStreamCtxAllocator*> (opaque);
    allocator->free(address);
  }
}

/**
 * ------------------------------ObZstdStreamCtxAllocator---------------------
 */
ObZstdStreamCtxAllocator::ObZstdStreamCtxAllocator()
  : allocator_(ObModIds::OB_STREAM_COMPRESSOR, OB_SERVER_TENANT_ID)
{

}

ObZstdStreamCtxAllocator::~ObZstdStreamCtxAllocator()
{
}

void* ObZstdStreamCtxAllocator::alloc(size_t size)
{
  return allocator_.alloc(size);
}

void ObZstdStreamCtxAllocator::free(void *addr)
{
  allocator_.free(addr);
}

/**
 * ------------------------------ObZstdStreamCompressor---------------------
 */
const char *ObZstdStreamCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::STREAM_ZSTD_COMPRESSOR];
}

ObCompressorType ObZstdStreamCompressor::get_compressor_type() const
{
  return ObCompressorType::STREAM_ZSTD_COMPRESSOR;
}

int ObZstdStreamCompressor::create_compress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  ctx = NULL;

  ObZstdStreamCtxAllocator &zstd_allocator = ObZstdStreamCtxAllocator::get_thread_local_instance();
  OB_ZSTD_customMem zstd_mem = {ob_zstd_stream_malloc, ob_zstd_stream_free, &zstd_allocator};
  if (OB_FAIL(ObZstdWrapper::create_cctx(zstd_mem, ctx))) {
    LIB_LOG(WARN, "failed to create cctx", K(ret));
  }
  return ret;
}

int ObZstdStreamCompressor::reset_compress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else if (OB_FAIL(free_compress_ctx(ctx))) {
    LIB_LOG(WARN, "failed to free compress ctx ", K(ret));
  } else if (OB_FAIL(create_compress_ctx(ctx))) {
    LIB_LOG(WARN, "failed to create compress ctx ", K(ret));
  } else {/*do nothing*/}
  return ret;
}


int ObZstdStreamCompressor::free_compress_ctx(void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else {
    ObZstdWrapper::free_cctx(ctx);
  }
  return ret;
}

// a block is considered not compressible enough,  compressed_size will be zero
int ObZstdStreamCompressor::stream_compress(void *ctx, const char *src, const int64_t src_size,
                                            char *dest, const int64_t dest_capacity, int64_t &dest_size)
{
  int ret = OB_SUCCESS;
  int64_t bound_size = 0;
  size_t compressed_size = 0;
  dest_size = 0;

  if (OB_ISNULL(ctx)
      || OB_ISNULL(src)
      || OB_ISNULL(dest)
      || OB_UNLIKELY(src_size <= 0)
      || OB_UNLIKELY(dest_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument,", KP(ctx), KP(src), K(src_size), KP(dest), K(dest_capacity), K(ret));
  } else if (OB_FAIL(get_compress_bound_size(src_size, bound_size))) {
    LIB_LOG(WARN, "faile to get compress bound size,", KP(src), K(src_size), KP(dest), K(dest_capacity), K(ret));
  } else if (OB_UNLIKELY(dest_capacity < bound_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dest buffer not enough", KP(src), K(src_size), KP(dest), K(dest_capacity), K(bound_size), K(ret));
  } else if (OB_FAIL(ObZstdWrapper::compress_block(ctx, src, src_size, dest, dest_capacity, compressed_size))) {
    LIB_LOG(WARN, "failed to compress block", K(ret), KP(src), K(src_size), KP(dest), K(dest_capacity), K(compressed_size));
  } else {
    dest_size = compressed_size;
  }
  return ret;
}

int ObZstdStreamCompressor::create_decompress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  ObZstdStreamCtxAllocator &zstd_allocator = ObZstdStreamCtxAllocator::get_thread_local_instance();
  OB_ZSTD_customMem zstd_mem = {ob_zstd_stream_malloc, ob_zstd_stream_free, &zstd_allocator};
  ctx = NULL;

  if (OB_FAIL(ObZstdWrapper::create_dctx(zstd_mem, ctx))) {
    LIB_LOG(WARN, "failed to create dctx", K(ret));
  }
  return ret;
}

int ObZstdStreamCompressor::reset_decompress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else if (OB_FAIL(free_decompress_ctx(ctx))) {
    LIB_LOG(WARN, "failed to free decompress ctx ", K(ret));
  } else if (OB_FAIL(create_decompress_ctx(ctx))) {
    LIB_LOG(WARN, "failed to create decompress ctx ", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObZstdStreamCompressor::free_decompress_ctx(void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else {
    ObZstdWrapper::free_dctx(ctx);
  }
  return ret;
}

int ObZstdStreamCompressor::stream_decompress(void *ctx, const char *src, const int64_t src_size,
                                              char *dest, const int64_t dest_capacity, int64_t &dest_size)
{
  int ret = OB_SUCCESS;
  size_t decompressed_size = 0;
  dest_size = 0;

  if (OB_ISNULL(ctx)
      || OB_ISNULL(src)
      || OB_ISNULL(dest)
      || OB_UNLIKELY(src_size <= 0)
      || OB_UNLIKELY(dest_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument", KP(ctx), KP(src), K(src_size), KP(dest), K(dest_capacity), K(ret));
  } else if (OB_FAIL(ObZstdWrapper::decompress_block(ctx, src, src_size, dest, dest_capacity, decompressed_size))) {
    LIB_LOG(WARN, "failed to decompress block", K(ret), KP(src), K(src_size), KP(dest), K(dest_capacity), K(decompressed_size));
  } else {
    dest_size = decompressed_size;
  }
  return ret;
}

int ObZstdStreamCompressor::get_compress_bound_size(const int64_t src_size, int64_t &bound_size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= src_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument,", K(src_size), K(ret));
  } else {
    bound_size = ObZstdWrapper::compress_bound(src_size);
  }
  return ret;
}

int ObZstdStreamCompressor::insert_uncompressed_block(void *ctx, const void *block, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(block) || OB_UNLIKELY(0 >= block_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", KP(ctx), KP(block), K(block_size), K(ret));
  } else if (OB_FAIL(ObZstdWrapper::insert_block(ctx, block, block_size))) {
    LIB_LOG(WARN, "failed to insert block", K(ret), KP(ctx), KP(block), K(block_size));
  }
  return ret;
}
