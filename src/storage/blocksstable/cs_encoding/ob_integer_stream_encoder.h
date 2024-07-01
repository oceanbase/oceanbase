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

#ifndef OCEANBASE_INTEGER_STREAM_ENCODER_H_
#define OCEANBASE_INTEGER_STREAM_ENCODER_H_

#include "share/ob_define.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/cs_encoding/ob_column_datum_iter.h"
#include "lib/codec/ob_fast_delta.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_simd_fixed_pfor.h"
#include "lib/codec/ob_double_delta_zigzag_rle.h"
#include "lib/codec/ob_double_delta_zigzag_pfor.h"
#include "lib/codec/ob_delta_zigzag_rle.h"
#include "lib/codec/ob_delta_zigzag_pfor.h"
#include "lib/codec/ob_xor_fixed_pfor.h"
#include "lib/codec/ob_universal_compression.h"
#include "lib/codec/ob_tiered_codec.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "ob_cs_encoding_util.h"
#include "ob_stream_encoding_struct.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

// 1. support UINT8, UINT16, UINT32 and UINT64
// 2. all data must be positive, or must set base_value
// 3. if null_replaced_value < 0, make sure use_base and null_replaced_value > base_value
class ObIntegerStreamEncoder
{
public:
  ObIntegerStreamEncoder() : ctx_(nullptr) {}

  int encode(ObIntegerStreamEncoderCtx &ctx, ObIDatumIter &datum_iter, ObMicroBufferWriter &writer);

  template <typename T>
  int encode(ObIntegerStreamEncoderCtx &ctx, T *int_arr, const int64_t arr_len, ObMicroBufferWriter &writer)
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(int_arr) || (arr_len <= 0) || (!ctx.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KP(int_arr), K(arr_len), K(ctx), KR(ret));
    } else {
      ctx_ = &ctx;
      // gcc will use SIMD
      if (ctx_->meta_.is_use_base()) {
        for (int64_t i = 0; (i < arr_len); i++) {
          int_arr[i] -= (T)ctx_->meta_.base_value_;
        }
      }
      if (OB_FAIL(inner_encode<T>(int_arr, arr_len, writer))) {
        STORAGE_LOG(WARN, "fail to ineer encode", KPC(ctx_), K(arr_len));
      }
    }

    return ret;
  }


  void reuse() {}

private:
  int encode_stream_meta(ObMicroBufferWriter &buf_writer);

  template<typename T>
  int do_encode(ObCodec &codec, const T *in_arr, uint64_t arr_count, ObMicroBufferWriter &buf_writer, const bool is_detected)
  {
    int ret = OB_SUCCESS;

    uint64_t remain_len = buf_writer.remain_buffer_size();
    uint64_t out_buf_len = remain_len;
    char *out = buf_writer.current();
    uint64_t out_pos = 0;

    codec.set_uint_bytes(sizeof(T));

    const char *in = reinterpret_cast<const char *>(in_arr);
    uint64_t in_len = arr_count * sizeof(T);
    if (OB_FAIL(codec.encode(in, in_len, out, out_buf_len, out_pos))) {
      STORAGE_LOG(WARN, "fail to encode array", KR(ret), K(arr_count),
                  K(out_pos), K(out_buf_len), K(buf_writer), K(codec));
    } else {
      int64_t char_len = out_pos;

      STORAGE_LOG(DEBUG, "after encoding integer stream", K(ctx_), K(arr_count),
                  "type", ObIntegerStream::get_encoding_type_name(ctx_->meta_.get_encoding_type()),
                  "ratio", ((0 == out_pos) ? 0 : (in_len * 100 / out_pos)),
                  K(is_detected),
                  "orig_len", in_len,
                  "encoded_len", out_pos,
                  K(sizeof(T)),
                  "uint width", ctx_->meta_.width_,
                  "attr", ctx_->meta_.attr_);
      if (OB_FAIL(buf_writer.advance(char_len))) {
        STORAGE_LOG(ERROR, "unexpected out_len", KR(ret), K(char_len), K(remain_len), K(buf_writer));
        abort();
      }
    }
    return ret;
  };

  template<class T>
  int do_codec_encode(const T *uint_arr, const uint64_t arr_count, ObMicroBufferWriter &buf_writer, const bool is_detected)
  {
    int ret = OB_SUCCESS;
    switch (ctx_->meta_.get_encoding_type()) {
      case ObIntegerStream::EncodingType::RAW : {
        // uint align
        ObSimpleBitPacking codec;
        codec.set_uint_packing_bits(ctx_->meta_.get_uint_width_size() * CHAR_BIT);
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do raw encode", KR(ret), KPC_(ctx));
        }
        break;
      }
      case ObIntegerStream::EncodingType::SIMD_FIXEDPFOR: {
        ObCompositeCodec<ObSIMDFixedPFor, ObSimpleBitPacking> codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do simd fixedpfor encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_RLE: {
        ObDoubleDeltaZigzagRle codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do double delta zigzig rle encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_PFOR: {
        ObDoubleDeltaZigzagPFor codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do double delta zigzag pfor encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DELTA_ZIGZAG_RLE: {
        ObDeltaZigzagRle codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do delta zigzag pfor encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::DELTA_ZIGZAG_PFOR: {
        ObDeltaZigzagPFor codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do delta zigzag pfor encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::XOR_FIXED_PFOR: {
        ObXorFixedPfor codec;
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do delta zigzag pfor encode", KR(ret), KPC(ctx_));
        }
        break;
      }
      case ObIntegerStream::EncodingType::UNIVERSAL_COMPRESS: {
        ObUniversalCompression codec;
        codec.set_allocator(*ctx_->info_.allocator_);
        codec.set_compressor_type(ctx_->info_.compressor_type_);
        if (OB_FAIL((do_encode<T>(codec, uint_arr, arr_count, buf_writer, is_detected)))) {
          STORAGE_LOG(WARN, "fail to do universal compression", KR(ret), KPC(ctx_));
        }
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected encoding type", KR(ret), KPC(ctx_));
        break;
      }
    }
    return ret;
  }

  template<typename T>
  int choose_stream_codec(const void *int_arr, const int64_t arr_count, ObMicroBufferWriter &buf_writer)
  {
    int ret = OB_SUCCESS;
    bool is_previous_used = false;

    if (OB_ISNULL(int_arr)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret));
    } else if (!ctx_->meta_.is_unspecified_encoding()) {
      if (ctx_->meta_.is_universal_compress_encoding()) {
        if (ctx_->info_.compressor_type_ == ObCompressorType::INVALID_COMPRESSOR ||
            ctx_->info_.compressor_type_ == ObCompressorType::NONE_COMPRESSOR) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "universal compress encoding not supported due to compressor_type not set",
              K(ret), KPC(ctx_));
        }
      }
    } else if (arr_count < ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD) {
      ctx_->meta_.set_raw_encoding();
    } else if (OB_FAIL(ctx_->try_use_previous_encoding(is_previous_used))) {
      STORAGE_LOG(WARN, "fail to try_use_previous_encoding", K(ret), KPC(ctx_));
    } else if (is_previous_used) {
      STORAGE_LOG(DEBUG, "use previous encoding", K(ret), KPC(ctx_));
    } else if (OB_FAIL(dectect_candidate_codec((T*)int_arr, arr_count, buf_writer))) {
      STORAGE_LOG(WARN, "fail to choose_codec_from_candidate", K(ret));
    }
    return ret;
  }

  template<typename T>
  int inner_encode(const void* int_arr, const int64_t arr_count, ObMicroBufferWriter &buf_writer)
  {
    int ret = OB_SUCCESS;
    const int64_t orig_pos = buf_writer.length();
    const int64_t raw_encoding_len = sizeof(T) * arr_count;
    int64_t curr_encoding_len = 0;
    const int64_t test_seed = 0;
    const int64_t remain_size = buf_writer.remain_buffer_size();
    bool need_encode_with_raw = false;
    if (OB_UNLIKELY(remain_size < raw_encoding_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "remain size is not enough", KR(ret), K(remain_size), K(raw_encoding_len));
    } else if (test_seed > 0) { // just for test
      int64_t type = test_seed % (ObIntegerStream::EncodingType::MAX_TYPE - 1) + 1;
      ctx_->meta_.set_encoding_type((ObIntegerStream::EncodingType)type);
      if (OB_FAIL(encode_stream_meta(buf_writer))) {
        STORAGE_LOG(WARN,"fail to encode_stream_header", KR(ret));
      } else if (OB_FAIL(do_codec_encode<T>((T*)int_arr, arr_count, buf_writer, false))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          STORAGE_LOG(WARN, "fail to do codec encode", KR(ret), KPC(ctx_), K(arr_count));
        } else {
          ret = OB_SUCCESS;
          need_encode_with_raw = true;
        }
      }
    } else if (OB_FAIL(choose_stream_codec<T>(int_arr, arr_count, buf_writer))) {
      STORAGE_LOG(WARN, "fail to choose stream codec", KR(ret), KP(int_arr), K(arr_count));
    } else if (OB_FAIL(encode_stream_meta(buf_writer))) {
      STORAGE_LOG(WARN, "fail to encode_stream_header", KR(ret));
    } else {
      const int64_t data_start_pos = buf_writer.length();
      if (OB_FAIL(do_codec_encode<T>((T*)int_arr, arr_count, buf_writer, false))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          STORAGE_LOG(WARN, "fail to do codec encode", KR(ret), KPC(ctx_), K(arr_count));
        } else {
          ret = OB_SUCCESS;
          need_encode_with_raw = true;
        }
      } else if ((curr_encoding_len = buf_writer.length() - data_start_pos) > raw_encoding_len) {
        need_encode_with_raw = true;
      }
    }

    if (OB_SUCC(ret) && need_encode_with_raw) {
      STORAGE_LOG(INFO, "need encode with raw", K(remain_size), K(raw_encoding_len), K(curr_encoding_len), K(ctx_->meta_));
      if (OB_UNLIKELY(ObIntegerStream::EncodingType::RAW == ctx_->meta_.type_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "the previously choosed type must be not RAW", KR(ret), KPC(ctx_));
      } else if (OB_FAIL(buf_writer.set_length(orig_pos))) {
        STORAGE_LOG(WARN, "fail to set pos", KR(ret), K(orig_pos), K(buf_writer));
      } else {
        ctx_->meta_.set_raw_encoding();
        if (OB_FAIL(encode_stream_meta(buf_writer))) {
          STORAGE_LOG(WARN,"fail to encode_stream_header", KR(ret));
        } else if (OB_FAIL(do_codec_encode<T>((T*)int_arr, arr_count, buf_writer, false))) {
          STORAGE_LOG(WARN,"fail to do codec encode", KR(ret), KPC(ctx_), K(arr_count));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // if failed, reset writer buffer's pos
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = buf_writer.set_length(orig_pos))) {
        STORAGE_LOG(WARN,"fail to set pos", KR(ret), KR(tmp_ret), K(orig_pos), K(buf_writer));
      }
    }

    return ret;
  }

  struct ObCodecCost
  {
    ObCodecCost() { memset(this, 0, sizeof(*this)); }
    inline bool operator<(const ObCodecCost &rhs) const
    {
      return space_cost_ < rhs.space_cost_;
    }

    TO_STRING_KV(K_(type), "name", ObIntegerStream::get_encoding_type_name(type_),
                 K_(time_cost_us), K_(space_cost), K_(percent_to_best));
    ObIntegerStream::EncodingType type_;
    int64_t time_cost_us_;
    int64_t space_cost_;
    int64_t percent_to_best_;
  };

  template<typename T>
  int dectect_candidate_codec(const T *int_arr, const int64_t arr_count,
                              ObMicroBufferWriter &buf_writer)
  {
    static constexpr int64_t sample_ratio = 25; // sample ratio 25%
    static constexpr int64_t min_sample_count = 1024;
    int ret = OB_SUCCESS;

    ObIntegerStream::EncodingType candidate_list[ObIntegerStream::EncodingType::MAX_TYPE];
    int32_t candidate_count = 0;
    build_candidate_codec_list<T>(candidate_list, candidate_count);

    // RAW encoding can't be disabled
    int32_t raw_encoding_idx = candidate_count;
    candidate_list[raw_encoding_idx] = ObIntegerStream::EncodingType::RAW;
    candidate_count += 1;

    if (candidate_count == 1) {
      ctx_->meta_.set_encoding_type(candidate_list[0]);
      STORAGE_LOG(INFO, "only one codec enabled", K(ret), "type", candidate_list[0]);
    } else {
      int64_t sample_count = 0;
      if (arr_count < min_sample_count) {
        sample_count = arr_count;
      } else {
        sample_count = std::max(arr_count * sample_ratio / 100, min_sample_count);
      }
      const int64_t orig_pos = buf_writer.length();
      const int64_t remain_size = buf_writer.remain_buffer_size();
      const int64_t raw_encoding_len = sizeof(T) * sample_count;

      if (OB_UNLIKELY(remain_size < raw_encoding_len)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "remain size is not enough", KR(ret), K(remain_size), K(raw_encoding_len));
      } else {
        int64_t start_time_us = 0;
        int64_t end_time_us = 0;
        ObCodecCost cost_arr[candidate_count];
        cost_arr[raw_encoding_idx].type_ = ObIntegerStream::EncodingType::RAW;
        cost_arr[raw_encoding_idx].time_cost_us_ = 0;
        cost_arr[raw_encoding_idx].space_cost_ = raw_encoding_len;

        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < raw_encoding_idx; i++) {
          ctx_->meta_.set_encoding_type(candidate_list[i]);
          start_time_us = ObTimeUtility::current_time();
          if (OB_FAIL(do_codec_encode<T>(int_arr, sample_count, buf_writer, true))) {
            if (OB_BUF_NOT_ENOUGH != ret) {
              STORAGE_LOG(WARN,"fail to do codec encode", KR(ret), KPC(ctx_), K(sample_count),
                        K(candidate_count), K(candidate_list[i]), K(i));
            } else {
              // if buf not enough, set space_cost to INT32_MAX which must be large than raw_encoding_len,
              // so this type will not be choosed, because we choose most space-saving encoding type.
              ret = OB_SUCCESS;
              cost_arr[i].type_ = candidate_list[i];
              cost_arr[i].time_cost_us_ = 0;
              cost_arr[i].space_cost_ = INT32_MAX;
            }
          } else {
            cost_arr[i].type_ = candidate_list[i];
            cost_arr[i].time_cost_us_ = ObTimeUtility::current_time() - start_time_us;
            cost_arr[i].space_cost_ = buf_writer.length() - orig_pos;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(buf_writer.set_length(orig_pos))) {
              STORAGE_LOG(WARN,"fail to set pos", K(ret), K(orig_pos));
            }
          } else if (OB_SUCCESS != (tmp_ret = buf_writer.set_length(orig_pos))) {
            STORAGE_LOG(WARN,"fail to set pos", K(ret), KR(tmp_ret), K(orig_pos));
          }
        }

        if (OB_SUCC(ret)) {
          lib::ob_sort(cost_arr, cost_arr + candidate_count);
          // use most space-saving encoding type
          const ObCodecCost &best_codec = cost_arr[0];
          for (int64_t i = 0; i < candidate_count; i++) {
            cost_arr[i].percent_to_best_ = cost_arr[i].space_cost_ * 100 / best_codec.space_cost_;
          }
          ctx_->meta_.set_encoding_type(best_codec.type_);

          STORAGE_LOG(INFO, "detect codec",
                      "best_codec", ObIntegerStream::get_encoding_type_name(best_codec.type_),
                      "best_codec_space_cost", best_codec.space_cost_,
                      "cost_list", ObArrayWrap<ObCodecCost>(cost_arr, candidate_count),
                      KPC(ctx_), K(arr_count), K(sample_count));
        }
      }
    }

    return ret;
  }

  template<typename T>
  void build_candidate_codec_list(ObIntegerStream::EncodingType *list, int32_t &count)
  {
    count = 0;
    if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::SIMD_FIXEDPFOR)) {
      list[count++] = ObIntegerStream::EncodingType::SIMD_FIXEDPFOR;
    }
    if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_RLE)) {
      list[count++] = ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_RLE;
    }
    if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_PFOR)) {
      list[count++] = ObIntegerStream::EncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
    }
    if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::DELTA_ZIGZAG_RLE)) {
      list[count++] = ObIntegerStream::EncodingType::DELTA_ZIGZAG_RLE;
    }
    if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::DELTA_ZIGZAG_PFOR)) {
      list[count++] = ObIntegerStream::EncodingType::DELTA_ZIGZAG_PFOR;
    }

    if (ctx_->info_.is_monotonic_inc_) {
      // no other
    } else {
      if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::XOR_FIXED_PFOR)) {
        list[count++] = ObIntegerStream::EncodingType::XOR_FIXED_PFOR;
      }
      if (ctx_->info_.encoding_opt_->is_enabled(ObIntegerStream::EncodingType::UNIVERSAL_COMPRESS)
        && (ctx_->info_.compressor_type_ != ObCompressorType::INVALID_COMPRESSOR)
        && (ctx_->info_.compressor_type_ != ObCompressorType::NONE_COMPRESSOR)) {
        list[count++] = ObIntegerStream::EncodingType::UNIVERSAL_COMPRESS;
      }
    }
  }

private:
  ObIntegerStreamEncoderCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObIntegerStreamEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_INTEGER_STREAM_ENCODER_H_
