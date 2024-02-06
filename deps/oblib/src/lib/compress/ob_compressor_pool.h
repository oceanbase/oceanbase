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

#ifndef OB_COMPRESSOR_POOL_H_
#define OB_COMPRESSOR_POOL_H_

#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_stream_compressor.h"
#include "none/ob_none_compressor.h"
#include "lz4/ob_lz4_compressor.h"
#include "snappy/ob_snappy_compressor.h"
#include "zlib/ob_zlib_compressor.h"
#include "lz4/ob_lz4_stream_compressor.h"
#include "zstd/ob_zstd_compressor.h"
#include "zstd/ob_zstd_stream_compressor.h"
#include "zstd_1_3_8/ob_zstd_compressor_1_3_8.h"
#include "zstd_1_3_8/ob_zstd_stream_compressor_1_3_8.h"

namespace oceanbase
{
namespace common
{

class ObCompressorPool
{
public:
  static ObCompressorPool &get_instance();
  int get_compressor(const char *compressor_name, ObCompressor *&compressor);
  int get_compressor(const ObCompressorType &compressor_type, ObCompressor *&compressor);
  int get_compressor_type(const char *compressor_name, ObCompressorType &compressor_type) const;
  int get_compressor_type(const ObString &compressor_name, ObCompressorType &compressor_type) const;

  int get_stream_compressor(const char *compressor_name, ObStreamCompressor *&stream_compressor);
  int get_stream_compressor(const ObCompressorType &compressor_type, ObStreamCompressor *&stream_compressor);
  static bool need_common_compress(const ObCompressorType &compressor_type)
  {
    return (need_compress(compressor_type) && (!need_stream_compress(compressor_type)));
  }
  static bool need_stream_compress(const ObCompressorType &compressor_type)
  {
    return (STREAM_LZ4_COMPRESSOR == compressor_type
        || STREAM_ZSTD_COMPRESSOR == compressor_type
        || STREAM_ZSTD_1_3_8_COMPRESSOR == compressor_type);
  }

  static bool need_compress(const ObCompressorType &compressor_type)
  {
    return ((INVALID_COMPRESSOR != compressor_type) && (NONE_COMPRESSOR != compressor_type));
  }
  int get_max_overflow_size(const int64_t src_data_size, int64_t &max_overflow_size);
private:
  ObCompressorPool();
  virtual ~ObCompressorPool() {}

  ObNoneCompressor none_compressor;
  ObLZ4Compressor lz4_compressor;
  ObLZ4Compressor191 lz4_compressor_1_9_1;
  ObSnappyCompressor snappy_compressor;
  ObZlibCompressor zlib_compressor;
  zstd::ObZstdCompressor zstd_compressor;
  zstd_1_3_8::ObZstdCompressor_1_3_8 zstd_compressor_1_3_8;

  //stream compressor
  ObLZ4StreamCompressor lz4_stream_compressor;
  zstd::ObZstdStreamCompressor zstd_stream_compressor;
  zstd_1_3_8::ObZstdStreamCompressor_1_3_8 zstd_stream_compressor_1_3_8;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_COMPRESSOR_POOL_H_ */
