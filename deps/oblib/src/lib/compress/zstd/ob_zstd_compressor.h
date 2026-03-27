/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_COMPRESS_ZSTD_COMPRESSOR_
#define OCEANBASE_COMMON_COMPRESS_ZSTD_COMPRESSOR_
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace common
{

namespace zstd
{

class ObZstdCompressor : public ObCompressor
{
public:
  explicit ObZstdCompressor(ObIAllocator &allocator)
    : allocator_(allocator) {}
  virtual ~ObZstdCompressor() {}
  int compress(const char *src_buffer,
               const int64_t src_data_size,
               char *dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size) override;
  int decompress(const char *src_buffer,
                 const int64_t src_data_size,
                 char *dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size) override;
  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;
  int get_max_overflow_size(const int64_t src_data_size,
                                   int64_t &max_overflow_size) const;
private:
  ObIAllocator &allocator_;

};
} // namespace zstd
} //namespace common
} //namespace oceanbase
#endif //OCEANBASE_COMMON_COMPRESS_ZSTD_COMPRESSOR_
