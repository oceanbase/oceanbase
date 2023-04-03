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

#include "ob_zstd_wrapper.h"
#include <stdio.h>

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd_src/zstd.h"

using namespace oceanbase;
using namespace common;
using namespace zstd_1_3_8;

constexpr int OB_SUCCESS                             = 0;
constexpr int OB_INVALID_ARGUMENT                    = -4002;
constexpr int OB_ALLOCATE_MEMORY_FAILED              = -4013;
constexpr int OB_ERR_COMPRESS_DECOMPRESS_DATA        = -4257;
constexpr int OB_IO_ERROR                            = -4009;
constexpr int OB_ZSTD_VERSION_138                    = 138;

static const int OB_ZSTD_COMPRESS_LEVEL = 1;

int ObZstdWrapper::compress(
    OB_ZSTD_customMem &ob_zstd_mem,
    const char *src_buffer,
    const size_t src_data_size,
    char *dst_buffer,
    const size_t dst_buffer_size,
    size_t &compress_ret_size)
{
  int ret = OB_SUCCESS;
  ZSTD_CCtx *zstd_cctx = NULL;
  ZSTD_customMem zstd_mem;
  int zstd_version = 0;
  zstd_mem.customAlloc = ob_zstd_mem.customAlloc;
  zstd_mem.customFree = ob_zstd_mem.customFree;
  zstd_mem.opaque = ob_zstd_mem.opaque;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (zstd_cctx = ZSTD_createCCtx_advanced(zstd_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    compress_ret_size = ZSTD_compressCCtx(zstd_cctx,
                                          dst_buffer,
                                          dst_buffer_size,
                                          src_buffer,
                                          src_data_size,
                                          OB_ZSTD_COMPRESS_LEVEL,
                                          &zstd_version);
    if (0 != ZSTD_isError(compress_ret_size)) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    } else if (OB_ZSTD_VERSION_138 != zstd_version) {
      ret = OB_IO_ERROR;
    }
  }

  if (NULL != zstd_cctx) {
    ZSTD_freeCCtx(zstd_cctx);
    zstd_cctx = NULL;
  }
  return ret;

}

int ObZstdWrapper::decompress(
    OB_ZSTD_customMem &ob_zstd_mem,
    const char *src_buffer,
    const size_t src_data_size,
    char *dst_buffer,
    const size_t dst_buffer_size,
    size_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  ZSTD_DCtx *zstd_dctx = NULL;
  ZSTD_customMem zstd_mem;
  int zstd_version = 0;
  zstd_mem.customAlloc = ob_zstd_mem.customAlloc;
  zstd_mem.customFree = ob_zstd_mem.customFree;
  zstd_mem.opaque = ob_zstd_mem.opaque;
  dst_data_size = 0;


  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (zstd_dctx = ZSTD_createDCtx_advanced(zstd_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    dst_data_size = ZSTD_decompressDCtx(zstd_dctx,
                                              dst_buffer,
                                              dst_buffer_size,
                                              src_buffer,
                                              src_data_size,
                                              &zstd_version);
    if (0 != ZSTD_isError(dst_data_size)) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    } else if (OB_ZSTD_VERSION_138 != zstd_version) {
      ret = OB_IO_ERROR;
    }
  }

  if (NULL != zstd_dctx) {
    ZSTD_freeDCtx(zstd_dctx);
    zstd_dctx = NULL;
  }
  return ret;
}


int ObZstdWrapper::create_cctx(OB_ZSTD_customMem &ob_zstd_mem, void *&ctx)
{
  int ret = OB_SUCCESS;
  size_t ret_code = 0;
  ZSTD_CCtx *cctx = NULL;
  ZSTD_customMem zstd_mem;
  zstd_mem.customAlloc = ob_zstd_mem.customAlloc;
  zstd_mem.customFree = ob_zstd_mem.customFree;
  zstd_mem.opaque = ob_zstd_mem.opaque;

  ctx = NULL;

  if (NULL == (cctx = ZSTD_createCCtx_advanced(zstd_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (0 != ZSTD_isError(ret_code = ZSTD_compressBegin(cctx, OB_ZSTD_COMPRESS_LEVEL))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    ZSTD_freeCCtx(cctx);
    cctx = NULL;
  } else {
    ctx = cctx;
  }
  return ret;
}


void ObZstdWrapper::free_cctx(void *&ctx)
{
  ZSTD_CCtx *cctx = static_cast<ZSTD_CCtx *>(ctx);
  ZSTD_freeCCtx(cctx);
}

int ObZstdWrapper::compress_block(void *ctx, const char *src, const size_t src_size,
    char *dest, const size_t dest_capacity, size_t &compressed_size)
{
  int ret = OB_SUCCESS;
  ZSTD_CCtx *cctx = static_cast<ZSTD_CCtx *>(ctx);
  compressed_size = 0;

  if (NULL == ctx || NULL == src || NULL == dest) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    compressed_size = ZSTD_compressBlock(cctx, dest, dest_capacity, src, src_size);
    if (0 != ZSTD_isError(compressed_size)) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    }
  }

  return ret;
}

int ObZstdWrapper::create_dctx(OB_ZSTD_customMem &ob_zstd_mem, void *&ctx)
{
  int ret = OB_SUCCESS;
  size_t ret_code = 0;
  ZSTD_DCtx *dctx = NULL;
  ZSTD_customMem zstd_mem;
  zstd_mem.customAlloc = ob_zstd_mem.customAlloc;
  zstd_mem.customFree = ob_zstd_mem.customFree;
  zstd_mem.opaque = ob_zstd_mem.opaque;
  ctx = NULL;

  if (NULL == (dctx = ZSTD_createDCtx_advanced(zstd_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (ZSTD_isError(ret_code = ZSTD_decompressBegin(dctx))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    ZSTD_freeDCtx(dctx);
    dctx = NULL;
  } else {
    ctx = dctx;
  }
  return ret;
}

void ObZstdWrapper::free_dctx(void *&ctx)
{
  ZSTD_DCtx *dctx = static_cast<ZSTD_DCtx *>(ctx);
  ZSTD_freeDCtx(dctx);
}

int ObZstdWrapper::decompress_block(void *ctx, const char *src, const size_t src_size,
    char *dest, const size_t dest_capacity, size_t &decompressed_size)
{
  int ret = OB_SUCCESS;
  decompressed_size = 0;

  if (NULL == ctx
      || NULL == src
      || NULL == dest
      || src_size <= 0
      || dest_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ZSTD_DCtx *dctx = static_cast<ZSTD_DCtx *>(ctx);
    decompressed_size = ZSTD_decompressBlock(dctx, dest, dest_capacity, src, src_size);
    if (0 != ZSTD_isError(decompressed_size)) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    }
  }
  return ret;

}

size_t ObZstdWrapper::compress_bound(const size_t src_size)
{
  return ZSTD_compressBound(src_size);
}

int ObZstdWrapper::insert_block(void *ctx, const void *block, const size_t block_size)
{
  int ret = OB_SUCCESS;
  if (NULL == ctx || NULL == block || 0 >= block_size) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ZSTD_DCtx *dctx = static_cast<ZSTD_DCtx *>(ctx);
    ZSTD_insertBlock(dctx, block, block_size);
  }
  return ret;
}




