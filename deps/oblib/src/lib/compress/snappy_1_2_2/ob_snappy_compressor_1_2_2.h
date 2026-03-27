/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_COMPRESS_SNAPPY_1_2_2_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_SNAPPY_1_2_2_COMPRESSOR_H_
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace common
{
namespace snappy_1_2_2
{

class __attribute__((visibility("default"))) ObSnappyCompressor1_2_2 : public ObCompressor
{
public:
  explicit ObSnappyCompressor1_2_2(ObIAllocator &allocator)
    : allocator_(allocator)
  {}
  virtual ~ObSnappyCompressor1_2_2() {}
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
  int get_uncompressed_length(const char *compressed,
                              const int64_t compressed_length,
                              size_t &uncompressed_length);
private:
  ObIAllocator &allocator_;
};
}
}
}

#endif //OCEANBASE_COMMON_COMPRESS_SNAPPY_1_2_2_COMPRESSOR_1_2_2_H_