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

#ifndef OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
#define OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
#include "lib/compress/ob_compressor.h"
#include <zlib.h>

namespace oceanbase
{
namespace common
{
class ObZlibCompressor : public ObCompressor
{
public:
  explicit ObZlibCompressor(int64_t compress_level = 6) : compress_level_(compress_level) {}
  virtual ~ObZlibCompressor() {}
  virtual int compress(const char *src_buffer,
                       const int64_t src_data_size,
                       char *dst_buffer,
                       const int64_t dst_buffer_size,
                       int64_t &dst_data_size);

  virtual int decompress(const char *src_buffer,
                         const int64_t src_data_size,
                         char *dst_buffer,
                         const int64_t dst_buffer_size,
                         int64_t &dst_data_size);
  int set_compress_level(const int64_t compress_level);
  virtual const char *get_compressor_name() const;
  virtual ObCompressorType get_compressor_type() const;
  virtual int get_max_overflow_size(const int64_t src_data_size,
                                    int64_t &max_overflow_size) const;

  int fast_level0_compress(unsigned char *dest, unsigned long *destLen,
                           const unsigned char *source, unsigned long sourceLen);

private:
  int64_t compress_level_;
};

}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
