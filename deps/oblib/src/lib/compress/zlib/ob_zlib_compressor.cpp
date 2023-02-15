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

#include "ob_zlib_compressor.h"
#include "zlib_src/deflate.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{

int ObZlibCompressor::fast_level0_compress(unsigned char *dest, unsigned long *destLen,
                                           const unsigned char *source, unsigned long sourceLen)
{
  int err = Z_OK;
#ifdef MAXSEG_64K
  /* Check for source > 64K on 16-bit machine: */
  if (sourceLen >= UINT16_MAX || (*destLen) >= UINT16_MAX) {
    err = Z_BUF_ERROR;
  } else
#endif
  static const uLong ZLIB_HEADER_LEN = 2;
  static const uLong DEFLATE_HEADER_LEN = 5;
  static const uLong ZLIB_TAILER_LEN = 4;
  const uLong MAX_DEFLATE_BLOCK_SIZE = (1 << 16);//64k
  const uLong MAX_DEFLATE_BLOCK_BODY_SIZE[3] = {
      MAX_DEFLATE_BLOCK_SIZE - DEFLATE_HEADER_LEN,
      (MAX_DEFLATE_BLOCK_SIZE >> 1) + DEFLATE_HEADER_LEN,
      (MAX_DEFLATE_BLOCK_SIZE >> 1)
  };//65531,32773,32768

  /* Check for source > 4G UINT32_MAX */
  if (sourceLen >= UINT32_MAX
      || (*destLen) >= (UINT32_MAX + ZLIB_HEADER_LEN + ZLIB_TAILER_LEN + sourceLen
                        + DEFLATE_HEADER_LEN * (sourceLen / MAX_DEFLATE_BLOCK_BODY_SIZE[2] + 1))) {
    err = Z_BUF_ERROR;
  } else {
    const unsigned char *orig_dest = dest;
    //https://tools.ietf.org/html/rfc1950
    //https://tools.ietf.org/html/rfc1951
    //1. fill zilb header, 2B
    Byte zlib_header[ZLIB_HEADER_LEN] = {0x78, 0x01};
    MEMCPY(dest, zlib_header, ZLIB_HEADER_LEN);
    dest += ZLIB_HEADER_LEN;

    //2. fill zilb body: deflate header + body
    Byte deflate_header[DEFLATE_HEADER_LEN] = {0x00,};
    uLong max_dist_size = (1<<15) - MIN_LOOKAHEAD;
    uLong max_deflate_size = 0;
    uLong step = 0;
    uLong adler = adler32(0L, Z_NULL, 0);
    for (uLong idx = 0, block_cnt = 0; idx < sourceLen; idx += step, block_cnt++) {
      step = sourceLen - idx;
      max_deflate_size = MAX_DEFLATE_BLOCK_BODY_SIZE[(block_cnt > 2 ? 2 : block_cnt)];
      if (step > max_deflate_size) {
        step = max_deflate_size;
      }
      if (step < max_dist_size) {
        deflate_header[0] = (Byte)(0x01);
      }
      deflate_header[1] = (Byte)(step & 0x000000ff);
      deflate_header[2] = (Byte)((step & 0x0000ff00) >> 8);
      deflate_header[3] = (Byte)((~step) & 0x000000ff);
      deflate_header[4] = (Byte)(((~step) & 0x0000ff00) >> 8);

      MEMCPY(dest, deflate_header, DEFLATE_HEADER_LEN);
      dest += DEFLATE_HEADER_LEN;
      MEMCPY(dest, source + idx, step);
      dest += step;
      adler = adler32(adler, source + idx, static_cast<uint32_t>(step));
    }
    if (step >= max_dist_size) {
      //last step is zero
      deflate_header[0] = (Byte)(0x01);
      deflate_header[1] = (Byte)(0x00);
      deflate_header[2] = (Byte)(0x00);
      deflate_header[3] = (Byte)(0xff);
      deflate_header[4] = (Byte)(0xff);
      MEMCPY(dest, deflate_header, DEFLATE_HEADER_LEN);
      dest += DEFLATE_HEADER_LEN;
    }

    //3. fill zlib tailer, 4B
    Byte zlib_tailer[ZLIB_TAILER_LEN] = {0};
    zlib_tailer[0] = (Byte)((adler & 0xff000000) >> 24);
    zlib_tailer[1] = (Byte)((adler & 0x00ff0000) >> 16);
    zlib_tailer[2] = (Byte)((adler & 0x0000ff00) >> 8);
    zlib_tailer[3] = (Byte)((adler & 0x000000ff));
    MEMCPY(dest, zlib_tailer, ZLIB_TAILER_LEN);
    dest += ZLIB_TAILER_LEN;
    *destLen = (dest - orig_dest);
  }
  return err;
}

int ObZlibCompressor::compress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int zlib_errno = Z_OK;
  int64_t compress_ret_size = dst_buffer_size;
  int64_t max_overflow_size = 0;
  if ( NULL == src_buffer
       || 0 >= src_data_size
       || NULL == dst_buffer
       || 0 >= dst_buffer_size ) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ",
        K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else {
    if (0 == compress_level_) {
      zlib_errno = fast_level0_compress(reinterpret_cast<Bytef*>(dst_buffer),
                                        reinterpret_cast<uLongf*>(&compress_ret_size),
                                        reinterpret_cast<const Bytef*>(src_buffer),
                                        static_cast<uLong>(src_data_size));
    } else {
      zlib_errno = compress2(reinterpret_cast<Bytef*>(dst_buffer),
                             reinterpret_cast<uLongf*>(&compress_ret_size),
                             reinterpret_cast<const Bytef*>(src_buffer),
                             static_cast<uLong>(src_data_size),
                             static_cast<int>(compress_level_));
    }

    if (OB_UNLIKELY(Z_OK != zlib_errno)) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LIB_LOG(WARN, "fail to compress data by zlib, ",
          K(ret), "zlib_errno", zlib_errno, KP(src_buffer), K(src_data_size), K_(compress_level));
    } else {
      dst_data_size = compress_ret_size;
    }
  }
  return ret;
}

int ObZlibCompressor::decompress(const char *src_buffer,
			                           const int64_t src_data_size,
			                           char *dst_buffer,
			                           const int64_t dst_buffer_size,
			                           int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int zlib_errno = Z_OK;
  int64_t decompress_ret_size = dst_buffer_size;
  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (Z_OK != (zlib_errno = ::uncompress(reinterpret_cast<Bytef*>(dst_buffer),
                                                reinterpret_cast<uLongf*>(&decompress_ret_size),
                                                reinterpret_cast<const Byte*>(src_buffer),
                                                static_cast<uLong>(src_data_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to decompress data by zlib, ",
        K(ret), "zlib_errno", zlib_errno, KP(src_buffer), K(src_data_size));
  } else {
    dst_data_size = decompress_ret_size;
  }

  return ret;
}

int ObZlibCompressor::set_compress_level(const int64_t compress_level)
{
  int ret = OB_SUCCESS;

  if (compress_level < -1 || compress_level > 9) {
     ret = OB_INVALID_ARGUMENT;
     LIB_LOG(WARN, "invalid argument, ", K(ret), K(compress_level));
  } else {
     compress_level_ = compress_level;
  }
  return ret;
}
const char *ObZlibCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::ZLIB_COMPRESSOR];
}

ObCompressorType ObZlibCompressor::get_compressor_type() const
{
  return ObCompressorType::ZLIB_COMPRESSOR;
}

int ObZlibCompressor::get_max_overflow_size(const int64_t src_data_size,
                                            int64_t &max_overflow_size) const
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

}//namespace common
}//namespace oceanbase
