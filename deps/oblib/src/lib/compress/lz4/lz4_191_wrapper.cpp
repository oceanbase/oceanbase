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

#include "lz4_src/lz4_191.h"
#include "lz4_wrapper.h"

namespace oceanbase {
namespace lib {
namespace lz4_191 {
LZ4_compress_default_FUNC LZ4_compress_default = &::LZ4_compress_default;
LZ4_decompress_safe_FUNC LZ4_decompress_safe = &::LZ4_decompress_safe;
LZ4_createStream_FUNC LZ4_createStream = &::LZ4_createStream;
LZ4_freeStream_FUNC LZ4_freeStream = &::LZ4_freeStream;
LZ4_resetStream_FUNC LZ4_resetStream = &::LZ4_resetStream;
LZ4_compress_fast_continue_FUNC LZ4_compress_fast_continue = &::LZ4_compress_fast_continue;
LZ4_createStreamDecode_FUNC LZ4_createStreamDecode = &::LZ4_createStreamDecode;
LZ4_freeStreamDecode_FUNC LZ4_freeStreamDecode = &::LZ4_freeStreamDecode;
LZ4_decompress_safe_continue_FUNC LZ4_decompress_safe_continue = &::LZ4_decompress_safe_continue;
LZ4_compressBound_FUNC LZ4_compressBound = &::LZ4_compressBound;
}  // lz4_191
}  // lib
}  // oceanbase
