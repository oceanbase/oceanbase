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

#ifndef OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace common
{
class ObSnappyCompressor : public ObCompressor
{
public:
  ObSnappyCompressor() {}
  virtual ~ObSnappyCompressor() {}
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
  virtual const char *get_compressor_name() const;
  virtual ObCompressorType get_compressor_type() const;
  virtual int get_max_overflow_size(const int64_t src_data_size,
                            int64_t &max_overflow_size) const;
};

}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMON_COMPRESS_SNAPPY_COMPRESSOR_H_
