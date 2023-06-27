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
#include "zlib_lite_src/CodecDeflateQpl.h"
#include "zlib_lite_src/deflate.h"
#include "zlib_lite_src/zlib.h"
#include "lib/ob_errno.h"

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

int ObZlibLiteCompressor::zlib_compress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  z_stream stream;
  int err = -1;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
#ifdef MAXSEG_64K
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != sourceLen) return Z_BUF_ERROR;
#endif
  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  if ((uLong)stream.avail_out != *dest_len) return Z_BUF_ERROR;

  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;
  stream.opaque = (voidpf)0;

  err = deflateInit2_(&stream, compress_level, Z_DEFLATED, window_bits, DEF_MEM_LEVEL,
                      Z_DEFAULT_STRATEGY, ZLIB_VERSION, (int)sizeof(z_stream));
  if (err != Z_OK) return err;

  err = deflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    deflateEnd(&stream);
    return err == Z_OK ? Z_BUF_ERROR : err;
  }
  *dest_len = stream.total_out;

  err = deflateEnd(&stream);
  return err;
}

int ObZlibLiteCompressor::zlib_decompress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  z_stream stream;
  int err = -1;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != source_len) return Z_BUF_ERROR;

  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  if ((uLong)stream.avail_out != *dest_len) return Z_BUF_ERROR;

  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;

  err = inflateInit2_(&stream, window_bits, ZLIB_VERSION, (int)sizeof(z_stream));
  if (err != Z_OK) return err;

  err = inflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
      inflateEnd(&stream);
      if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0))
          return Z_DATA_ERROR;
      return err;
  }
  *dest_len = stream.total_out;

  err = inflateEnd(&stream);
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
