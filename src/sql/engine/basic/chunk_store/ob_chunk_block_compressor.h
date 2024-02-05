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
