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

#include "lib/compress/lz4/ob_lz4_compressor.h"
#include "lib/ob_errno.h"
#include "lib/compress/lz4/lz4_wrapper.h"

using namespace oceanbase::lib::lz4_191;

namespace oceanbase
{
namespace common
{
int ObLZ4Compressor191::compress(
    const char *src_buffer,
    const int64_t src_data_size,
    char *dst_buffer,
    const int64_t dst_buffer_size,
    int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
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
  } else if (0 >= (dst_data_size = LZ4_compress_default(src_buffer,
                                                        dst_buffer,
                                                        static_cast<int>(src_data_size),
                                                        static_cast<int>(dst_buffer_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to compress data by LZ4_compress, ",
        K(ret), KP(src_buffer), K(src_data_size), K(dst_buffer_size), K(dst_data_size));
  }

  return ret;
}

int ObLZ4Compressor191::decompress(
    const char *src_buffer,
    const int64_t src_data_size,
    char *dst_buffer,
    const int64_t dst_buffer_size,
    int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (0 >= (dst_data_size = LZ4_decompress_safe(src_buffer,
                                                      dst_buffer,
                                                      static_cast<int>(src_data_size),
                                                      static_cast<int>(dst_buffer_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to decompress by LZ4_uncompress, ",
        K(ret), KP(src_buffer), K(src_data_size), K(dst_buffer_size), K(dst_data_size));
  }

  return ret;
}

int ObLZ4Compressor191::get_max_overflow_size(
    const int64_t src_data_size, int64_t &max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = src_data_size / 255 + 16;
  }
  return ret;
}

const char *ObLZ4Compressor191::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::LZ4_191_COMPRESSOR];
}

ObCompressorType ObLZ4Compressor191::get_compressor_type() const
{
  return ObCompressorType::LZ4_191_COMPRESSOR;
}

}//namespace common
}//namesoace oceanbase
