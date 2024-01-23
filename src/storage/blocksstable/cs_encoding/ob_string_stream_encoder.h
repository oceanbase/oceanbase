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

#ifndef OCEANBASE_STRING_STREAM_ENCODER_H_
#define OCEANBASE_STRING_STREAM_ENCODER_H_

#include "ob_stream_encoding_struct.h"
#include "lib/compress/ob_compress_util.h"
#include "ob_column_datum_iter.h"
#include "ob_integer_stream_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObStringStreamEncoder
{
public:
  ObStringStreamEncoder()
    : ctx_(nullptr), int_ctx_(),
      offset_arr_(nullptr), offset_arr_count_(0),
      byte_arr_(nullptr), writer_(nullptr)
 {}

  int encode(ObStringStreamEncoderCtx &ctx,
             ObIDatumIter &iter,
             ObMicroBufferWriter &writer,
             ObMicroBufferWriter *all_string_writer,
             common::ObIArray<uint32_t> &stream_offset_arr);
  const ObIntegerStreamEncoderCtx &get_offset_encoder_ctx() const { return int_ctx_; }

  void reuse()
  {
    int_ctx_.reset();
    byte_arr_ = nullptr;
    offset_arr_ = nullptr;
  }

private:
  int convert_datum_to_stream_(ObIDatumIter &iter);
  template<typename T>
  int do_convert_datum_to_stream_(ObIDatumIter &iter);
  int encode_byte_stream_(common::ObIArray<uint32_t> &stream_offset_arr);
  int encode_offset_stream_(common::ObIArray<uint32_t> &stream_offset_arr);
  template<typename T>
  int do_encode_offset_stream_(common::ObIArray<uint32_t> &stream_offset_arr);

private:
  ObStringStreamEncoderCtx *ctx_;
  ObIntegerStreamEncoderCtx int_ctx_;
  void *offset_arr_;
  uint32_t offset_arr_count_;
  char *byte_arr_;
  ObMicroBufferWriter *writer_;
  ObMicroBufferWriter *all_string_writer_;
};

template<typename T>
int ObStringStreamEncoder::do_convert_datum_to_stream_(ObIDatumIter &iter)
{
  int ret = OB_SUCCESS;
  const bool is_fixed_len = ctx_->meta_.is_fixed_len_string();
  const uint32_t umcompress_len = ctx_->meta_.uncompressed_len_;

  if (!is_fixed_len) {
    offset_arr_count_ = (uint32_t)(iter.size());
    int64_t offset_arr_len = offset_arr_count_ * sizeof(T);
    if (umcompress_len + offset_arr_len > all_string_writer_->remain()) {
      // remain size is not enough to hold umcompress_len and offset_array_len, which is possible
      // if the fixed-length data use variable-length encoding due to existing many null values.
      if (OB_FAIL(all_string_writer_->ensure_space(umcompress_len))) {
        STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(umcompress_len), K(offset_arr_len));
      } else {
        byte_arr_ = all_string_writer_->current();
        if (OB_ISNULL(offset_arr_ = ctx_->info_.allocator_->alloc(offset_arr_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to malloc", K(ret), K(umcompress_len), K(offset_arr_len));
        }
      }
    } else if (OB_FAIL(all_string_writer_->ensure_space(umcompress_len + offset_arr_len))) {
      STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(umcompress_len), K(offset_arr_len));
    } else {
      byte_arr_ = all_string_writer_->current();
      offset_arr_ = byte_arr_ + umcompress_len;
    }
  } else {
    if (umcompress_len != 0 && OB_FAIL(all_string_writer_->ensure_space(umcompress_len))) {
      STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(umcompress_len));
    } else {
      byte_arr_ = all_string_writer_->current();
    }
  }
  int64_t pos = 0;
  int64_t tmp_len = 0;
  const ObDatum *datum = nullptr;
  int64_t i = 0;
  T *offset_arr = static_cast<T*>(offset_arr_);
  while (OB_SUCC(ret) && OB_SUCC(iter.get_next(datum))) {
    if (datum->is_null()) {
      if (is_fixed_len) {
        // if fixed len, fill 0 for placeholders
        tmp_len = ctx_->meta_.get_fixed_string_len();
        if (OB_UNLIKELY((pos + tmp_len) > umcompress_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough", KR(ret), K(pos), K(umcompress_len), K(tmp_len));
        } else {
          MEMSET(byte_arr_ + pos, 0, tmp_len);
          pos += tmp_len;
        }
      }
    } else {
      tmp_len = datum->len_;
      if (OB_UNLIKELY((pos + tmp_len) > umcompress_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buf not enough", KR(ret), K(pos), K(umcompress_len), K(tmp_len));
      } else {
        MEMCPY(byte_arr_ + pos, datum->ptr_, tmp_len);
        pos += tmp_len;
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_fixed_len) {
        offset_arr[i] = pos;
      }
    }
    i++;
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(i != iter.size() || pos != umcompress_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected datum count and bytes len", K(ret), K(i), K(iter.size()), K(pos), K(umcompress_len));
    } else if (OB_FAIL(all_string_writer_->advance(umcompress_len))) {
      STORAGE_LOG(WARN, "fail to advance all_string_writer_", K(ret), K(umcompress_len));
    }
  }

  return ret;
}

template<typename T>
int ObStringStreamEncoder::do_encode_offset_stream_(ObIArray<uint32_t> &stream_offset_arr)
{
  int ret = OB_SUCCESS;
  if (ctx_->meta_.is_fixed_len_string()) {
    // fixed len str no need encode offset str
  } else {
    ObIntegerStreamEncoder int_encoder;
    T *offset_arr = static_cast<T*>(offset_arr_);
    uint64_t end_offset = offset_arr[offset_arr_count_ - 1];
    if (OB_UNLIKELY(ctx_->meta_.uncompressed_len_ != end_offset)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "max string offset must equal to total_bytes_len_", K(ret), K(end_offset), KPC_(ctx));
    } else if (OB_FAIL(int_ctx_.build_offset_array_stream_meta(
        end_offset, ctx_->info_.raw_encoding_str_offset_))) {
      STORAGE_LOG(WARN, "fail to build_offset_array_stream_meta", KR(ret));
    } else if (OB_FAIL(int_ctx_.build_stream_encoder_info(
                                false/*has_null*/,
                                true/*monotonic inc*/,
                                ctx_->info_.encoding_opt_,
                                ctx_->info_.previous_encoding_, ctx_->info_.int_stream_idx_,
                                ctx_->info_.compressor_type_,
                                ctx_->info_.allocator_))) {
      STORAGE_LOG(WARN, "fail to build_stream_encoder_info", K(ret));
    } else if (OB_FAIL(int_encoder.encode(int_ctx_, offset_arr, offset_arr_count_, *writer_))) {
      STORAGE_LOG(WARN, "fail to encode string offset", KR(ret));
    } else if (OB_FAIL(stream_offset_arr.push_back((uint32_t)writer_->length()))) {
      STORAGE_LOG(WARN, "fail to push back", KPC(writer_), KR(ret));
    }
  }

  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_STRING_STREAM_ENCODER_H_
