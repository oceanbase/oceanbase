/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_COMPRESS_NONE_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_NONE_COMPRESSOR_H_
#include "lib/compress/ob_compressor.h"

//#define COMPRESSOR_NAME "none"
namespace oceanbase
{
namespace common
{
class ObNoneCompressor : public ObCompressor
{
public:
  ObNoneCompressor() {}
  virtual ~ObNoneCompressor() {}
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

} //namespace common
} //namespace oceanbase
#endif // OCEANBASE_COMMON_COMPRESS_NONE_COMPRESSOR_H_
