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

#ifndef LZ4_171_WRAPPER_H
#define LZ4_171_WRAPPER_H


#ifndef LZ4_VERSION_NUMBER
struct LZ4_stream_t;
struct LZ4_streamDecode_t;
#endif

// copied from lz4.h
#define LZ4_MAX_INPUT_SIZE        0x7E000000   /* 2 113 929 216 bytes */

#define OB_PUBLIC_API __attribute__ ((visibility ("default")))

namespace oceanbase {
namespace lib {
namespace lz4_171 {

using LZ4_compress_default_FUNC = int(*)(const char* source, char* dest, int sourceSize, int maxDestSize);
OB_PUBLIC_API extern LZ4_compress_default_FUNC LZ4_compress_default;

using LZ4_decompress_safe_FUNC = int(*)(const char* source, char* dest, int compressedSize, int maxDecompressedSize);
OB_PUBLIC_API extern LZ4_decompress_safe_FUNC LZ4_decompress_safe;

using LZ4_createStream_FUNC = LZ4_stream_t*(*)(void);
OB_PUBLIC_API extern LZ4_createStream_FUNC LZ4_createStream;

using LZ4_freeStream_FUNC = int(*)(LZ4_stream_t* streamPtr);
OB_PUBLIC_API extern LZ4_freeStream_FUNC LZ4_freeStream;

using LZ4_resetStream_FUNC = void(*)(LZ4_stream_t* streamPtr);
OB_PUBLIC_API extern LZ4_resetStream_FUNC LZ4_resetStream;

using LZ4_compress_fast_continue_FUNC = int(*)(LZ4_stream_t* streamPtr, const char* src, char* dst, int srcSize, int maxDstSize, int acceleration);
OB_PUBLIC_API extern LZ4_compress_fast_continue_FUNC LZ4_compress_fast_continue;

using LZ4_createStreamDecode_FUNC = LZ4_streamDecode_t*(*)(void);
OB_PUBLIC_API extern LZ4_createStreamDecode_FUNC LZ4_createStreamDecode;

using LZ4_freeStreamDecode_FUNC = int(*)(LZ4_streamDecode_t* LZ4_stream);
OB_PUBLIC_API extern LZ4_freeStreamDecode_FUNC LZ4_freeStreamDecode;

using LZ4_decompress_safe_continue_FUNC = int(*)(LZ4_streamDecode_t* LZ4_streamDecode, const char* source, char* dest, int compressedSize, int maxDecompressedSize);
OB_PUBLIC_API extern LZ4_decompress_safe_continue_FUNC LZ4_decompress_safe_continue;

using LZ4_compressBound_FUNC = int(*)(int inputSize);
OB_PUBLIC_API extern LZ4_compressBound_FUNC LZ4_compressBound;

}  // lz4_171

namespace lz4_191 {

using LZ4_compress_default_FUNC = int(*)(const char* source, char* dest, int sourceSize, int maxDestSize);
OB_PUBLIC_API extern LZ4_compress_default_FUNC LZ4_compress_default;

using LZ4_decompress_safe_FUNC = int(*)(const char* source, char* dest, int compressedSize, int maxDecompressedSize);
OB_PUBLIC_API extern LZ4_decompress_safe_FUNC LZ4_decompress_safe;

using LZ4_createStream_FUNC = LZ4_stream_t*(*)(void);
OB_PUBLIC_API extern LZ4_createStream_FUNC LZ4_createStream;

using LZ4_freeStream_FUNC = int(*)(LZ4_stream_t* streamPtr);
OB_PUBLIC_API extern LZ4_freeStream_FUNC LZ4_freeStream;

using LZ4_resetStream_FUNC = void(*)(LZ4_stream_t* streamPtr);
OB_PUBLIC_API extern LZ4_resetStream_FUNC LZ4_resetStream;

using LZ4_compress_fast_continue_FUNC = int(*)(LZ4_stream_t* streamPtr, const char* src, char* dst, int srcSize, int maxDstSize, int acceleration);
OB_PUBLIC_API extern LZ4_compress_fast_continue_FUNC LZ4_compress_fast_continue;

using LZ4_createStreamDecode_FUNC = LZ4_streamDecode_t*(*)(void);
OB_PUBLIC_API extern LZ4_createStreamDecode_FUNC LZ4_createStreamDecode;

using LZ4_freeStreamDecode_FUNC = int(*)(LZ4_streamDecode_t* LZ4_stream);
OB_PUBLIC_API extern LZ4_freeStreamDecode_FUNC LZ4_freeStreamDecode;

using LZ4_decompress_safe_continue_FUNC = int(*)(LZ4_streamDecode_t* LZ4_streamDecode, const char* source, char* dest, int compressedSize, int maxDecompressedSize);
OB_PUBLIC_API extern LZ4_decompress_safe_continue_FUNC LZ4_decompress_safe_continue;

using LZ4_compressBound_FUNC = int(*)(int inputSize);
OB_PUBLIC_API extern LZ4_compressBound_FUNC LZ4_compressBound;

}  // lz4_191

#undef OB_PUBLIC_API

}  // lib
}  // oceanbase

#endif /* LZ4_171_WRAPPER_H */
