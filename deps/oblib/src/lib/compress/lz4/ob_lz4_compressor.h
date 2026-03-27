/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_COMPRESS_LZ4_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_LZ4_COMPRESSOR_H_

#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace common
{
#define OB_PUBLIC_API __attribute__ ((visibility ("default")))

class OB_PUBLIC_API ObLZ4Compressor : public ObCompressor
{
public:
  int compress(const char* src_buffer,
               const int64_t src_data_size,
               char* dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size);

  int decompress(const char* src_buffer,
                 const int64_t src_data_size,
                 char* dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size);

  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;

  int get_max_overflow_size(const int64_t src_data_size,
                            int64_t &max_overflow_size) const;
};

class OB_PUBLIC_API ObLZ4Compressor191 : public ObCompressor
{
public:
  int compress(const char* src_buffer,
               const int64_t src_data_size,
               char* dst_buffer,
               const int64_t dst_buffer_size,
               int64_t &dst_data_size);

  int decompress(const char* src_buffer,
                 const int64_t src_data_size,
                 char* dst_buffer,
                 const int64_t dst_buffer_size,
                 int64_t &dst_data_size);

  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;

  int get_max_overflow_size(const int64_t src_data_size,
                            int64_t &max_overflow_size) const;
};

#undef OB_PUBLIC_API

}//namespace common
}//namespace oceanabse
#endif
