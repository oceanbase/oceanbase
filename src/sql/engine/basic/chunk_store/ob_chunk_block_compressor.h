/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_CHUNK_BLOCK_COMPRESSOR_H_
#define OCEANBASE_BASIC_OB_CHUNK_BLOCK_COMPRESSOR_H_

#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace sql
{

class ObChunkBlockCompressor final
{
public:
  ObChunkBlockCompressor();
  virtual ~ObChunkBlockCompressor();
  void reset();
  int init(const ObCompressorType type);
  int compress(const char *in, const int64_t in_size, const int64_t max_comp_size,
               char *out, int64_t &out_size);
  int decompress(const char *in, const int64_t in_size, const int64_t uncomp_size,
      char *out, int64_t &out_size);
  ObCompressorType get_compressor_type() const { return compressor_type_; }

  int calc_need_size(int64_t in_size, int64_t &need_size);
private:
  common::ObCompressorType compressor_type_;
  common::ObCompressor *compressor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif
