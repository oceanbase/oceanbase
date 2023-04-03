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

#include "lib/compress/lz4/ob_lz4_stream_compressor.h"
#include "lib/ob_errno.h"
#include "lib/compress/lz4/lz4_wrapper.h"

using namespace oceanbase::lib::lz4_191;

namespace oceanbase
{
namespace common
{
const char *ObLZ4StreamCompressor191::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::STREAM_LZ4_COMPRESSOR];
}

ObCompressorType ObLZ4StreamCompressor191::get_compressor_type() const
{
  return ObCompressorType::STREAM_LZ4_COMPRESSOR;
}

int ObLZ4StreamCompressor191::create_compress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx = LZ4_createStream())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "failed to create_compress_ctx", K(ret));
  }
  return ret;
}

int ObLZ4StreamCompressor191::free_compress_ctx(void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else {
    LZ4_stream_t *lz4_ctx
        = static_cast<LZ4_stream_t *>(ctx);
    LZ4_freeStream(lz4_ctx);
  }
  return ret;
}

int ObLZ4StreamCompressor191::reset_compress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else {
    LZ4_stream_t *lz4_ctx
        = static_cast<LZ4_stream_t *>(ctx);
    LZ4_resetStream(lz4_ctx);
  }
  return ret;
}

int ObLZ4StreamCompressor191::stream_compress(void *ctx, const char *src, const int64_t src_size,
                                           char *dest, const int64_t dest_size, int64_t &compressed_size)
{
  int ret = OB_SUCCESS;
  int32_t acceleration = 0;
  int64_t bound_size = 0;
  if (OB_ISNULL(ctx)
      || OB_ISNULL(src)
      || OB_ISNULL(dest)
      || OB_UNLIKELY(!is_valid_original_data_length(src_size))
      || OB_UNLIKELY(dest_size <= 0)
      || OB_UNLIKELY(dest_size > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument,", KP(src), K(src_size), KP(dest), K(dest_size), K(ret));
  } else if (OB_FAIL(get_compress_bound_size(src_size, bound_size))) {
    LIB_LOG(WARN, "failed to get compress bound size,", KP(src), K(src_size), KP(dest), K(dest_size), K(ret));
  } else if (OB_UNLIKELY(dest_size < bound_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer is not enough", K(src_size), K(bound_size), K(dest_size), K(ret));
  } else {
    LZ4_stream_t *lz4_ctx
        = static_cast<LZ4_stream_t *>(ctx);
    if (OB_UNLIKELY(0 >= (compressed_size = LZ4_compress_fast_continue(lz4_ctx, src, dest, static_cast<int32_t>(src_size), static_cast<int32_t>(dest_size), acceleration)))) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LIB_LOG(WARN, "fail to compress data by LZ4_compress_fast_continue",
              KP(src), K(src_size), KP(dest), K(dest_size), K(compressed_size), K(ret));
    }
  }
  return ret;
}

int ObLZ4StreamCompressor191::create_decompress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx = LZ4_createStreamDecode())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "got decompress stream ctx is NULL", K(ret));
  }
  return ret;
}

int ObLZ4StreamCompressor191::reset_decompress_ctx(void *&ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLZ4StreamCompressor191::free_decompress_ctx(void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid ctx is NULL ", K(ret));
  } else {
    //this func always returns 0
    LZ4_streamDecode_t *lz4_ctx
        = static_cast<LZ4_streamDecode_t *>(ctx);
    LZ4_freeStreamDecode(lz4_ctx);
  }
  return ret;
}

int ObLZ4StreamCompressor191::stream_decompress(void *ctx, const char *src, const int64_t src_size,
                                             char *dest, const int64_t max_decompressed_size,
                                             int64_t &decompressed_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)
      || OB_ISNULL(src)
      || OB_ISNULL(dest)
      || OB_UNLIKELY(src_size <= 0)
      || OB_UNLIKELY(src_size > INT32_MAX)
      || OB_UNLIKELY(max_decompressed_size <= 0)
      || OB_UNLIKELY(max_decompressed_size > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument,", KP(src), K(src_size), KP(dest), K(max_decompressed_size), K(ret));
  } else {
    LZ4_streamDecode_t *decompress_ctx
        = static_cast<LZ4_streamDecode_t *>(ctx);
    if (OB_UNLIKELY(0 >= (decompressed_size = LZ4_decompress_safe_continue(decompress_ctx, src, dest, static_cast<int32_t>(src_size), static_cast<int32_t>(max_decompressed_size))))) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LIB_LOG(WARN, "fail to decompress data by LZ4_decompress_safe_continue",
              KP(src), KP(dest), K(src_size), K(max_decompressed_size), K(decompressed_size), K(ret));
    }
  }
  return ret;
}

int ObLZ4StreamCompressor191::get_compress_bound_size(const int64_t src_data_size, int64_t &bound_size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_original_data_length(src_data_size))) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(src_data_size), K(ret));
  } else {
    bound_size = LZ4_compressBound(static_cast<int32_t>(src_data_size));
  }
  return ret;
}

int ObLZ4StreamCompressor191::insert_uncompressed_block(void *dctx, const void *block, const int64_t block_size)
{
  int ret = OB_ERR_UNEXPECTED;
  LIB_LOG(ERROR, "lz4 should not invoke this func", KP(dctx), KP(block), K(block_size), K(ret));
  return ret;
}

bool ObLZ4StreamCompressor191::is_valid_original_data_length(int64_t origin_data_len) const
{
  return (origin_data_len <= LZ4_MAX_INPUT_SIZE && origin_data_len > 0);
}


}//namespace common
}//namesoace oceanbase
