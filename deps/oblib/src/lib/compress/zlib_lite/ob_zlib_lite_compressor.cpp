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

#include "ob_zlib_lite_compressor.h"
#include "zlib_lite_src/codec_deflate_qpl.h"
#include "zlib_lite_src/deflate.h"
#include "zlib_lite_src/zlib.h"
#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{
/*zlib_lite supports two algorithms. On the platform that supports qpl, the qpl compression algorithm will be used, 
otherwise the zlib algorithm will be used.*/

ObZlibLiteCompressor::ObZlibLiteCompressor()
{
}

ObZlibLiteCompressor::~ObZlibLiteCompressor()
{
}

void *qpl_allocate(int64_t size)
{
  return ob_malloc(size, ObModIds::OB_COMPRESSOR);
}

void qpl_deallocate(void *ptr)
{
  return ob_free(ptr);
}

int ObZlibLiteCompressor::init()
{
  int ret = OB_SUCCESS;
#ifdef ENABLE_QPL_COMPRESSION
  QplAllocator allocator;
  allocator.allocate = qpl_allocate;
  allocator.deallocate = qpl_deallocate;
  int qpl_ret = qpl_init(allocator);
  if (0 != qpl_ret) {
    ret = OB_ERROR;
  }
#endif
  return ret;
}

void ObZlibLiteCompressor::deinit()
{
#ifdef ENABLE_QPL_COMPRESSION
  qpl_deinit();
#endif
}

int ObZlibLiteCompressor::zlib_compress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  int ret = OB_SUCCESS; // just for log
  z_stream stream;
  int err = Z_OK;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
#ifdef MAXSEG_64K
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != source_len) return Z_BUF_ERROR;
#endif
  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;
  stream.opaque = (voidpf)0;

  if ((uLong)stream.avail_out != *dest_len) {
    err = Z_BUF_ERROR;
  } else if (Z_OK != (err = deflateInit2_(&stream, compress_level, Z_DEFLATED, window_bits, DEF_MEM_LEVEL,
                                          Z_DEFAULT_STRATEGY, ZLIB_VERSION, (int)sizeof(z_stream)))) {
    LIB_LOG(WARN, "deflateInit2_ failed", K(err));
  } else if (Z_STREAM_END != (err = deflate(&stream, Z_FINISH))) {
    deflateEnd(&stream);
    err = (err == Z_OK ? Z_BUF_ERROR : err);
  } else {
    *dest_len = stream.total_out;
    err = deflateEnd(&stream);
  }
  return err;
}

int ObZlibLiteCompressor::zlib_decompress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  int ret = OB_SUCCESS; // just for log
  z_stream stream;
  int err = Z_OK;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;
  
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != source_len || (uLong)stream.avail_out != *dest_len) {
    err = Z_BUF_ERROR;
  } else if (Z_OK != (err = inflateInit2_(&stream, window_bits, ZLIB_VERSION,
                                          (int)sizeof(z_stream)))) {
    LIB_LOG(WARN, "inflateInit2_ failed", K(err));
  } else {
    err = inflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
      inflateEnd(&stream);
      if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0)) {
        err = Z_DATA_ERROR;
      }
    } else {
      *dest_len = stream.total_out;
      err = inflateEnd(&stream);
    }
  }
  return err;
}

int ObZlibLiteCompressor::zlib_lite_compress(const char* src_buffer,
                                             const int64_t src_data_size,
                                             char* dst_buffer,
                                             const int64_t dst_buffer_size)
{
#ifdef ENABLE_QPL_COMPRESSION
  return qpl_compress(src_buffer, dst_buffer, static_cast<int>(src_data_size), static_cast<int>(dst_buffer_size));
#else
  int ret = OB_SUCCESS;
  int zlib_errno = Z_OK;
  int64_t compress_ret_size = dst_buffer_size;
  zlib_errno = zlib_compress(reinterpret_cast<Bytef*>(dst_buffer),
                             reinterpret_cast<uLongf*>(&compress_ret_size),
                             reinterpret_cast<const Bytef*>(src_buffer),
                             static_cast<uLong>(src_data_size));
  if (OB_UNLIKELY(Z_OK != zlib_errno)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "Compress data by zlib in zlib_lite algorithm faild, ", K(ret), K(zlib_errno));
    return -1;
  } 
    
  return compress_ret_size;
#endif
}

int ObZlibLiteCompressor::zlib_lite_decompress(const char* src_buffer,
                                               const int64_t src_data_size,
                                               char* dst_buffer,
                                               const int64_t dst_buffer_size)
{
#ifdef ENABLE_QPL_COMPRESSION
  return qpl_decompress(src_buffer, dst_buffer, static_cast<int>(src_data_size), static_cast<int>(dst_buffer_size));
#else
  int ret = OB_SUCCESS;
  int64_t decompress_ret_size = dst_buffer_size;
  int zlib_errno = Z_OK;
  zlib_errno = zlib_decompress(reinterpret_cast<Bytef*>(dst_buffer),
                          reinterpret_cast<uLongf*>(&decompress_ret_size),
                          reinterpret_cast<const Byte*>(src_buffer),
                          static_cast<uLong>(src_data_size));
  if (OB_UNLIKELY(Z_OK != zlib_errno)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "Decompress data by zlib in zlib_lite algorithm faild, ",K(ret), K(zlib_errno));
    return -1;
  }
  return decompress_ret_size;
#endif
}

int ObZlibLiteCompressor::compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
            "invalid compress argument, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            KP(dst_buffer),
            K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ", K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else if (0 >= (dst_data_size = zlib_lite_compress(
                       src_buffer, src_data_size, dst_buffer, dst_buffer_size))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
           "fail to compress data by zlib_lite_compress, ",
           K(ret),
           KP(src_buffer),
           K(src_data_size),
           K(dst_buffer_size),
           K(dst_data_size));
  }

  return ret;
}

int ObZlibLiteCompressor::decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
            "invalid decompress argument, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            KP(dst_buffer),
            K(dst_buffer_size));
  } else if (0 >= (dst_data_size = zlib_lite_decompress(
                       src_buffer, src_data_size, dst_buffer, dst_buffer_size))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
            "fail to decompress by zlib_lite_decompress, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            K(dst_buffer_size),
            K(dst_data_size));
  }

  return ret;
}

int ObZlibLiteCompressor::get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = (src_data_size >> 12) + (src_data_size >> 14) + (src_data_size >> 25 ) + 13;
  }
  return ret;
}

const char* ObZlibLiteCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::ZLIB_LITE_COMPRESSOR];
}

ObCompressorType ObZlibLiteCompressor::get_compressor_type() const
{
  return ObCompressorType::ZLIB_LITE_COMPRESSOR;
}

} // namespace ZLIB_LITE
} // namespace common
} // namespace oceanbase
