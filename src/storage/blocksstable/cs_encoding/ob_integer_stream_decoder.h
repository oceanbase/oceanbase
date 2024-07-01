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

#ifndef OCEANBASE_INTEGER_STREAM_DECODER_H_
#define OCEANBASE_INTEGER_STREAM_DECODER_H_

#include "ob_stream_encoding_struct.h"
#include "ob_column_encoding_struct.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_simd_fixed_pfor.h"
#include "lib/codec/ob_double_delta_zigzag_rle.h"
#include "lib/codec/ob_delta_zigzag_rle.h"
#include "lib/codec/ob_delta_zigzag_pfor.h"
#include "lib/codec/ob_double_delta_zigzag_pfor.h"
#include "lib/codec/ob_universal_compression.h"
#include "lib/codec/ob_xor_fixed_pfor.h"
#include "lib/codec/ob_tiered_codec.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

namespace oceanbase
{
namespace blocksstable
{
typedef void (*ConvertUnitToDatumFunc)(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums);

extern  ObMultiDimArray_T<ConvertUnitToDatumFunc,
    4, ObRefStoreWidthV::MAX_WIDTH_V, 4, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2> convert_uint_to_datum_funcs;


class ObIntegerStreamDecoder
{
public:
  static int build_decoder_ctx(const ObStreamData &data,
                                const int64_t count,
                                const ObCompressorType type,
                                ObIntegerStreamDecoderCtx &ctx,
                                uint16_t &stream_meta_len);

  static int transform_to_raw_array(
      const ObStreamData &data, ObIntegerStreamDecoderCtx &ctx, char *raw_arr_buf, ObIAllocator &alloc);

  OB_INLINE static int decimal_datum_assign(
      common::ObDatum &datum,
      uint8_t precision_width,
      uint64_t value,
      const bool safe)
  {
    int ret = OB_SUCCESS;
    switch(precision_width) {
      case ObIntegerStream::UintWidth::UW_4_BYTE : {
        *(int32_t*)datum.ptr_ = (int64_t)(value);
        datum.pack_ = sizeof(int32_t);
        break;
      }
      case ObIntegerStream::UintWidth::UW_8_BYTE : {
        *(int64_t*)datum.ptr_ = (int64_t)(value);
        datum.pack_ = sizeof(int64_t);
        break;
      }
      case ObIntegerStream::UintWidth::UW_16_BYTE : {
        *(int128_t*)datum.ptr_ = (int64_t)(value);
        datum.pack_ = sizeof(int128_t);
        break;
      }
      case ObIntegerStream::UintWidth::UW_32_BYTE : {
        *(int256_t*)datum.ptr_ = (int64_t)(value);
        datum.pack_ = sizeof(int256_t);
        break;
      }
      case ObIntegerStream::UintWidth::UW_64_BYTE : {
        *(int512_t*)datum.ptr_ = (int64_t)(value);
        datum.pack_ = sizeof(int512_t);
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected precison width", K(ret), K(precision_width));
        if (!safe) {
          ob_abort();
        }
        break;
      }
    }
    return ret;
  }

private:
  // decode all, but do not add base, maybe modify ObIntegerStreamDecoderCtx.
  template<typename UintType>
  static int decode_all_except_base(const ObStreamData &data,
                                    ObIntegerStreamDecoderCtx &ctx,
                                    UintType *uint_arr,
                                    ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(decode_body<UintType>(data, ctx, uint_arr, alloc))) {
      STORAGE_LOG(WARN, "fail to encode to int", KR(ret), K(sizeof(UintType)));
    } else {
      ctx.meta_.set_raw_encoding();
    }
    return ret;
  }

  static int decode_stream_meta(const ObStreamData &data,
                                ObIntegerStreamDecoderCtx &ctx,
                                uint16_t &stream_meta_len);

  template<typename T>
  /*OB_INLINE*/ static int do_decode(
      ObCodec &codec,
      const ObStreamData &data,
      const ObIntegerStreamDecoderCtx &ctx,
      T *&uint_arr)
  {
    int ret = common::OB_SUCCESS;
    uint64_t out_pos = 0;
    uint64_t out_len = ctx.count_ * sizeof(T);
    const char *in = data.buf_;
    uint64_t in_len = data.len_;
    char *out = reinterpret_cast<char *>(uint_arr);
    uint64_t uint_count = ctx.count_;
    uint64_t in_pos = 0;

    codec.set_uint_bytes(sizeof(T));
    if (OB_FAIL(codec.decode(in, in_len, in_pos, uint_count, out, out_len, out_pos))) {
      STORAGE_LOG(WARN, "fail to deocde array", K(in_len), K(uint_count), K(out_len), KR(ret));
    }
    return ret;
  };

  template<typename T>
  OB_INLINE static int decode_body(
      const ObStreamData &data,
      const ObIntegerStreamDecoderCtx &ctx,
      T *&int_arr,
      ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    switch (ctx.meta_.get_encoding_type()) {
      case ObIntegerStream::EncodingType::RAW : {
        ObSimpleBitPacking codec;
        codec.set_uint_packing_bits(ctx.meta_.get_uint_width_size() * CHAR_BIT);
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN,"fail to do raw decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::SIMD_FIXEDPFOR: {
        ObCompositeCodec<ObSIMDFixedPFor, ObSimpleBitPacking> codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN, "fail to do simd fixedpfor decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_RLE: {
        ObDoubleDeltaZigzagRle codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN, "fail to do double delta zigzig rle decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_PFOR: {
        ObDoubleDeltaZigzagPFor codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN, "fail to do double delta zigzag pfor deocde", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DELTA_ZIGZAG_RLE: {
        ObDeltaZigzagRle codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN, "fail to do delta zigzag pfor decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DELTA_ZIGZAG_PFOR: {
        ObDeltaZigzagPFor codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN, "fail to do delta zigzag pfor decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::UNIVERSAL_COMPRESS : {
        ObUniversalCompression codec;
        codec.set_compressor_type(ctx.compressor_type_);
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN,"fail to do universal compression decode", KR(ret), K(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::XOR_FIXED_PFOR : {
        ObXorFixedPfor codec;
        if (OB_FAIL((do_decode<T>(codec, data, ctx, int_arr)))) {
          STORAGE_LOG(WARN,"fail to do universal compression decode", KR(ret), K(ctx));
        }
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,"unexpected encoding type", KR(ret), K(ctx));
        break;
      }
    }
    return ret;
  }
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_INTEGER_STREAM_DECODER_H_
