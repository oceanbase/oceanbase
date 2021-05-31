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

#ifndef OB_COMPRESSOR_UTIL_H_
#define OB_COMPRESSOR_UTIL_H_

namespace oceanbase {
namespace common {

enum ObCompressorType {
  INVALID_COMPRESSOR = 0,
  NONE_COMPRESSOR = 1,
  LZ4_COMPRESSOR = 2,
  SNAPPY_COMPRESSOR = 3,
  ZLIB_COMPRESSOR = 4,
  ZSTD_COMPRESSOR = 5,
  STREAM_LZ4_COMPRESSOR = 6,   // used for clog rpc compress
  STREAM_ZSTD_COMPRESSOR = 7,  // used for clog rpc compress
  ZSTD_1_3_8_COMPRESSOR = 8,
  STREAM_ZSTD_1_3_8_COMPRESSOR = 9,  // used for clog rpc compress
  LZ4_191_COMPRESSOR = 10,
};

const char* const compress_funcs[] = {
    "lz4_1.0",
    "none",
    "snappy_1.0",
    "zlib_1.0",
    "zstd_1.0",
    "zstd_1.3.8",
    "lz4_1.9.1",
};

const char* const perf_compress_funcs[] = {
    "none",
    "lz4_1.0",
    "zstd_1.0",
    "zstd_1.3.8",
};

const char* const batch_rpc_compress_funcs[] = {
    "lz4_1.0",
    "none",
    "snappy_1.0",
    "zlib_1.0",
    "zstd_1.0",
    "stream_lz4_1.0",
    "stream_zstd_1.0",
    "zstd_1.3.8",
    "stream_zstd_1.3.8",
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_COMPRESSOR_UTIL_H_ */
