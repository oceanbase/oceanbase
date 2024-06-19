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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/cs_encoding/ob_integer_stream_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_integer_stream_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_cs_decoding_util.h"
#include "lib/codec/ob_fast_delta.h"
#include <iostream>
#include <random>

namespace oceanbase
{
namespace blocksstable
{

class TestIntegerStream : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}

  TestIntegerStream() : tenant_ctx_(500) {
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  virtual ~TestIntegerStream() {}

  enum Monotonicity
  {
    RANDOM = 0,
    STRICT_INCREMENT = 1,
    STRICT_DECREMENT = 2,
    EQUAL = 3,
  };

  template<class T>
  void generate_data(T *&int_arr, const int64_t size, T min, T max, Monotonicity mon) {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<T> distribution(min, max);
    int_arr = reinterpret_cast<T *>(allocator_.alloc(sizeof(T) * size));

    T first_value = distribution(rng);
    T second_value = distribution(rng);
    T delta = ((second_value > 0) ? second_value : (0 - second_value));
    delta = delta%3 + 1;
    for (int64_t i = 0; i < size; i++) {
      if (RANDOM == mon) {
        int_arr[i] = distribution(rng);
      } else if (STRICT_INCREMENT == mon) {
        int_arr[i] = first_value + i * delta;
      } else if (STRICT_DECREMENT == mon) {
        int_arr[i] = first_value - i * delta;
      } else if (EQUAL == mon) {
        int_arr[i] = first_value;
      }

      LOG_TRACE("generate_data", K(i), K(int_arr[i]), K(min), K(max));
    }
  }

  template<class T>
  void generate_datums(ObColDatums *datums, const int64_t size, bool has_null, T min, T max, Monotonicity mon)
  {
    uint64_t *datusms_content_ptr = reinterpret_cast<uint64_t *>(allocator_.alloc(sizeof(uint64_t) * size));
    memset(datusms_content_ptr, 0,  sizeof(uint64_t) * size);
    T *data = nullptr;
    generate_data<T>(data, size, min, max, mon);

    for (int64_t i = 0; i < size; i++) {
      ObDatum datum;
      datum.uint_ = (datusms_content_ptr + i);
      *(datusms_content_ptr + i) = data[i];

      if (has_null && (i % 5 == 0)) {
        datum.set_null();
      } else {
        datum.pack_ = sizeof(T);
      }
      LOG_DEBUG("generate_datum", K(datum), K(i));
      ASSERT_EQ(OB_SUCCESS, datums->push_back(datum));
    }
  }

  void generate_bitmap(char *bitmap, ObColDatums *datums)
  {
    for (int64_t i = 0; i < datums->count(); i++) {
      if (datums->at(i).is_null()) {
        bitmap[i/8] |= (1 << (7 - i%8));
      }
    }
  }

  void buid_raw_integer_stream_data(const ObStreamData &stream_data,
                                    const int64_t count,
                                    const ObCompressorType type,
                                    ObIntegerStreamDecoderCtx &decode_ctx,
                                    ObStreamData &raw_stream_data)
  {
    uint16_t stream_meta_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObIntegerStreamDecoder::build_decoder_ctx(
        stream_data, count,type, decode_ctx, stream_meta_len));
    ObStreamData stream_data2(stream_data.buf_ + stream_meta_len, stream_data.len_ - stream_meta_len);
    const uint32_t width_size = decode_ctx.meta_.get_uint_width_size();
    uint32_t array_buf_size = width_size * decode_ctx.count_;
    char *array_buf = (char*)allocator_.alloc(array_buf_size);
    ASSERT_EQ(OB_SUCCESS, ObIntegerStreamDecoder::transform_to_raw_array(stream_data2, decode_ctx, array_buf, allocator_));
    raw_stream_data.set(array_buf, array_buf_size);
  }

  template<int32_t WIDTH_TAG>
  static void do_decode_raw_array(const ObStreamData &data,
                    const ObIntegerStreamDecoderCtx &ctx,
                    const int32_t *row_ids,
                    const int64_t row_count,
                    char *out_buf)
  {
    typedef typename ObCSEncodingStoreTypeInference<WIDTH_TAG>::Type StoreIntType;
    const StoreIntType *orig_arr = reinterpret_cast<const StoreIntType *>(data.buf_);
    StoreIntType *int_arr = reinterpret_cast<StoreIntType *>(out_buf);
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value();
    for (int64_t i = 0; i < row_count; i++) {
      int_arr[i] = orig_arr[row_ids[i]] + base;
    }
  }

  static void decode_raw_array(const ObStreamData &data,
                    const ObIntegerStreamDecoderCtx &ctx,
                    const int32_t *row_ids,
                    const int64_t row_count,
                    char *out_buf)
  {
    const uint32_t width = ctx.meta_.width_;
    switch(width) {
      case ObIntegerStream::UW_1_BYTE :
        do_decode_raw_array<ObIntegerStream::UW_1_BYTE>(data, ctx, row_ids, row_count, out_buf);
        break;
      case ObIntegerStream::UW_2_BYTE :
        do_decode_raw_array<ObIntegerStream::UW_2_BYTE>(data, ctx, row_ids, row_count, out_buf);
        break;
      case ObIntegerStream::UW_4_BYTE :
        do_decode_raw_array<ObIntegerStream::UW_4_BYTE>(data, ctx, row_ids, row_count, out_buf);
        break;
      case ObIntegerStream::UW_8_BYTE :
        do_decode_raw_array<ObIntegerStream::UW_8_BYTE>(data, ctx, row_ids, row_count, out_buf);
        break;
      default :
        ob_abort();
    }
  }


  template<class T>
  void test_and_check_int_encoding(
      int64_t size,
      const ObIntegerStream::EncodingType type,
      uint8_t attribute,
      T min,
      T max)
  {
    LOG_INFO("test_and_check_int_encoding", K(size), K(type), K(attribute));
    ObIntegerStreamEncoderCtx ctx;
    ObCSEncodingOpt encoding_opt;
    ObArenaAllocator alloctor;
    const ObCompressorType compress_type =  ObCompressorType::ZSTD_1_3_8_COMPRESSOR;

    ctx.meta_.set_4_byte_width();
    ctx.meta_.type_ = type;
    ctx.meta_.attr_ = attribute;
    if (ctx.meta_.is_use_base()) {
      ctx.meta_.set_base_value(min);
    }
    ctx.build_stream_encoder_info(false, false, &encoding_opt, nullptr, -1, compress_type, &alloctor);

    ObIntegerStreamEncoder encoder;
    uint32_t *data = nullptr;
    generate_data<T>(data, size, min, max, RANDOM);
    uint32_t *orig_data = new uint32_t[size];
    memcpy(orig_data, data, size * sizeof(uint32_t));
    ObMicroBufferWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(OB_DEFAULT_MACRO_BLOCK_SIZE, OB_DEFAULT_MACRO_BLOCK_SIZE));
    ASSERT_EQ(OB_SUCCESS, encoder.encode(ctx, data, size, writer));

    const char *stream_start = writer.data();
    int64_t stream_len = writer.length();
    ObStreamData stream_data(stream_start, stream_len);
    ObIntegerStreamDecoderCtx decode_ctx;
    ObStreamData raw_stream_data;
    buid_raw_integer_stream_data(stream_data, size, compress_type, decode_ctx, raw_stream_data);

    const uint32_t width_size = decode_ctx.meta_.get_uint_width_size();
    uint32_t array_buf_size = width_size * decode_ctx.count_;
    char *dst_array_buf = (char*)allocator_.alloc(array_buf_size);

    // test batch decode
    int64_t row_id_count = size;
    int32_t *row_ids = new int32_t[row_id_count];
    for (int32_t i = 0; i < row_id_count; i++) {
      row_ids[i] = i;
    }

    decode_raw_array(raw_stream_data, decode_ctx, row_ids, row_id_count, dst_array_buf);
    for (int64_t i = 0; i < row_id_count; i++) {
      ASSERT_EQ(orig_data[i], *(uint32_t*)(dst_array_buf + i * width_size));
    }

    // test disorder batch decode
    int64_t random_idx = ObTimeUtility::current_time()%size;
    for (int64_t i = 0; i < size; i++) {
      row_ids[i] = (i + random_idx) % size;
      if (i%7 == 0) {
        row_ids[i] = random_idx; //duplicate
      }
    }
    decode_raw_array(raw_stream_data, decode_ctx, row_ids, row_id_count, dst_array_buf);
    for (int64_t i = 0; i < size; i++) {
      if (orig_data[row_ids[i]] != *(uint32_t*)(dst_array_buf + i * width_size)) {
        LOG_INFO("missmatch", K(i), K(random_idx), K(row_ids[i]),
            K(*(uint32_t*)(dst_array_buf + i * width_size)), K(orig_data[row_ids[i]]));
        ::abort();
      }
    }
    delete[] orig_data;
    orig_data = nullptr;
  }

  template<class T>
  void test_and_check_int_datums_all_encoding_type(
      uint8_t attribute,
      bool has_null,
      T min,
      T max,
      T null_replace_value,
      ObIntegerStream::UintWidth actual_uint_width,
      bool use_nullbitmap,
      int64_t loop,
      Monotonicity mon,
      bool use_null_replace_ref)
  {
    ObIntegerStream::EncodingType type = ObIntegerStream::RAW;
    for (int64_t m = 1; m < ObIntegerStream::EncodingType::MAX_TYPE; m++) {
      int64_t random = ObTimeUtility::current_time()%13 + 1 + 1;
      for (int64_t i = 1; i < 100000; i=i * random) {
        type = (ObIntegerStream::EncodingType)(m);
        LOG_INFO("round", K(i), K(m), K(type), K(loop), K(mon), K(min), K(max));
        test_and_check_int_datums<T>(i, type, attribute, has_null, min, max,
                                     null_replace_value, actual_uint_width,
                                     use_nullbitmap, loop, mon, use_null_replace_ref);
      }
    }
  }

  template<class T>
  void test_and_check_int_datums(
      int64_t size,
      const ObIntegerStream::EncodingType type,
      uint8_t attribute,
      bool has_null,
      T min,
      T max,
      T null_replace_value,
      ObIntegerStream::UintWidth actual_uint_width,
      bool use_nullbitmap,
      int64_t loop,
      Monotonicity mon,
      bool use_null_replace_ref)
  {
    LOG_INFO("test_and_check_datums_encoding", K(size), K(type), K(attribute), K(null_replace_value),
        K(loop), K(sizeof(T)), K(min), K(max), K(mon), K(use_null_replace_ref), K(actual_uint_width));
    ObIntegerStreamEncoderCtx ctx;
    ObCSEncodingOpt encoding_opt;
    ObArenaAllocator allocator;
    const ObCompressorType compress_type =  ObCompressorType::ZSTD_1_3_8_COMPRESSOR;
    ctx.meta_.width_ = actual_uint_width;
    ctx.meta_.type_ = type;
    ctx.meta_.attr_ = attribute;
    if (ctx.meta_.is_use_base()) {
      if (ctx.meta_.is_use_null_replace_value()) {
        if (null_replace_value < min) {
          ctx.meta_.set_base_value(null_replace_value);
        } else {
          ctx.meta_.set_base_value(min);
        }
      } else {
        ctx.meta_.set_base_value(min);
      }
    }
    if (ctx.meta_.is_use_null_replace_value()) {
      ctx.meta_.set_null_replaced_value(null_replace_value);
    }
    ctx.build_stream_encoder_info(has_null, false, &encoding_opt, nullptr, -1, compress_type, &allocator);

    ObIntegerStreamEncoder encoder;
    ObColDatums *datums = new ObColDatums(allocator);
    datums->reserve(1 << 20);
    generate_datums<T>(datums, size, has_null, min, max, mon);
    int64_t bitmap_size = pad8(size);
    char *bitmap = new char[bitmap_size];
    memset(bitmap, 0, bitmap_size);
    generate_bitmap(bitmap, datums);

    ObMicroBufferWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(OB_DEFAULT_MACRO_BLOCK_SIZE, OB_DEFAULT_MACRO_BLOCK_SIZE));

    ObColumnDatumIter iter(*datums);
    ASSERT_EQ(OB_SUCCESS, encoder.encode(ctx, iter, writer));

    const char *stream_start = writer.data();
    int64_t stream_len = writer.length();
    ObStreamData data(stream_start, stream_len);

    ObIntegerStreamDecoderCtx decode_ctx;
    ObStreamData raw_stream_data;
    buid_raw_integer_stream_data(data, size, compress_type, decode_ctx, raw_stream_data);

    ObDatum *datums2 = new ObDatum[size];
    char *datums2_buf = new char[size * sizeof(uint64_t)];
    memset(datums2_buf, 0, size * sizeof(uint64_t));
    for (int64_t i = 0; i < size; i++) {
      datums2[i].ptr_ = (datums2_buf + i * sizeof(uint64_t));
    }

    // decode batch
    int32_t *row_ids = new int32_t[size];
    for (int32_t i = 0; i < size; i++) {
      row_ids[i] = i;
    }
    ObBaseColumnDecoderCtx base_ctx;
    base_ctx.allocator_ = &allocator_;
    base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL;
    base_ctx.null_desc_ = nullptr;
    if (use_nullbitmap) {
      base_ctx.null_bitmap_ = bitmap;
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP;
    }
    if (ctx.meta_.is_use_null_replace_value()) {
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED;
      base_ctx.null_replaced_value_ = ctx.meta_.null_replaced_value();
    }

    uint32_t ref_width_V = ObRefStoreWidthV::NOT_REF;
    int64_t null_replaced_ref = size;
    if (use_null_replace_ref) {
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF;
      base_ctx.null_replaced_ref_ = null_replaced_ref;
      ref_width_V = ObRefStoreWidthV::REF_IN_DATUMS;
      for (int64_t i = 0; i < size; i++) {
         // store row_id into datum.pack_
        if (datums->at(i).is_null()) {
          datums2[i].pack_ = null_replaced_ref;
        } else {
          datums2[i].pack_ = i;
        }
      }
    }

    ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
        [decode_ctx.meta_.width_]
        [ref_width_V]
        [get_width_tag_map()[sizeof(T)]]  /*datum_width_V*/
        [base_ctx.null_flag_]
        [decode_ctx.meta_.is_decimal_int()];
    convert_func(base_ctx, raw_stream_data.buf_, decode_ctx, nullptr, row_ids, size, datums2);

    for (int64_t i = 0; i < size; i++) {
      if (!ObDatum::binary_equal(datums->at(row_ids[i]), datums2[i])) {
        LOG_INFO("missmatch", K(datums->at(row_ids[i])), K(datums2[i]), K(i));
        ::abort();
      }
    }

    // disorder batch
    uint32_t *ref_arr = new uint32_t[size];
    memset(datums2_buf, 0, size * sizeof(uint64_t));
    for (int64_t i = 0; i < size; i++) {
      datums2[i].reset();
      datums2[i].ptr_ = (datums2_buf + i * sizeof(uint64_t));
    }
    int64_t random_idx = ObTimeUtility::current_time() % size;
    int32_t row_id = 0;
    for (int64_t i = 0; i < size; i++) {
      ref_arr[i] = i;
      row_id = (i + random_idx) % size;
      row_ids[i] = row_id;
      if (use_null_replace_ref && datums->at(row_id).is_null()) {
        datums2[i].pack_ = size;
      } else {
        datums2[i].pack_ = row_id;
      }
      if (i%7 == 0) {
        row_ids[i] = random_idx; //duplicate
        if (use_null_replace_ref && datums->at(random_idx).is_null()) {
          datums2[i].pack_ = size;
        } else {
          datums2[i].pack_ = random_idx;
        }
      }
    }
    convert_func = convert_uint_to_datum_funcs
        [decode_ctx.meta_.width_]
        [ref_width_V]
        [get_width_tag_map()[sizeof(T)]]  /*datum_width_V*/
        [base_ctx.null_flag_]
        [decode_ctx.meta_.is_decimal_int()];
    convert_func(base_ctx, raw_stream_data.buf_, decode_ctx, nullptr, row_ids, size, datums2);

    for (int64_t i = 0; i < size; i++) {
      bool is_equal = false;
      ObDatum *tmp_datum = &datums->at(row_ids[i]);
      is_equal = ObDatum::binary_equal(*tmp_datum, datums2[i]);
      if (!is_equal) {
        for (int64_t j = 0; j < size; j++) {
          LOG_INFO("compare", K(datums->at(j)), K(datums2[j]), K(j), K(row_ids[j]));
        }
        LOG_INFO("missmatch", KPC(tmp_datum), K(datums2[i]), K(i), K(random_idx), K(row_ids[i]), K(size));
        ::abort();
      }
    }

    //  disorder batch with using ref array
    memset(datums2_buf, 0, size * sizeof(uint64_t));
    if (use_null_replace_ref) {
      convert_func = convert_uint_to_datum_funcs
          [decode_ctx.meta_.width_]
          [ObRefStoreWidthV::REF_4_BYTE]
          [get_width_tag_map()[sizeof(T)]]  /*datum_width_V*/
          [base_ctx.null_flag_]
          [decode_ctx.meta_.is_decimal_int()];
      convert_func(base_ctx, raw_stream_data.buf_, decode_ctx, (char*)ref_arr, row_ids, size, datums2);
    }


    delete []ref_arr;
    delete []row_ids;
    row_ids = nullptr;
    delete []datums2_buf;
    datums2_buf = nullptr;
    delete []datums2;
    datums2 = nullptr;
    delete datums;
    datums = nullptr;
  }

protected:
  ObArenaAllocator allocator_;
  share::ObTenantBase tenant_ctx_;
};

TEST_F(TestIntegerStream, test_uint32_encoding)
{
  ObIntegerStream::EncodingType type = ObIntegerStream::RAW;
  uint8_t attribute = 0;
  for (int64_t j = 0; j < 2; j++) {
    if (0 == j) {
      attribute = ObIntegerStream::Attribute::USE_NONE;
    } else if (1 == j) {
      attribute = ObIntegerStream::Attribute::USE_BASE;
    }
    for (int64_t i = 1; i < 1000000; i=i * (i + 1)) {
      LOG_INFO("round", K(j), K(i));
      type = ObIntegerStream::RAW;
      test_and_check_int_encoding<uint32_t>(i, type, attribute, 2, UINT32_MAX);
      type = ObIntegerStream::SIMD_FIXEDPFOR;
      test_and_check_int_encoding<uint32_t>(i, type, attribute, 2, UINT32_MAX);
    }
  }
}

#define DECLEAR_PARAM \
  ObIntegerStream::EncodingType type = ObIntegerStream::RAW; \
  uint8_t attribute = 0; \
  bool has_null = false; \
  uint64_t null_replaced_value = 1; \
  bool null_replaced_rowid = false; \
  bool use_nullbitmap = false; \
  uint64_t min = 2; \
  uint64_t max = UINT64_MAX; \
  uint64_t int_min = 2; \
  uint64_t int_max = INT64_MAX; \
  Monotonicity mon = RANDOM; \
  ObIntegerStream::UintWidth width = ObIntegerStream::UW_1_BYTE;

TEST_F(TestIntegerStream, test_datums_encoding)

{
  for (int64_t j = 0; j < 4; j++) {
    DECLEAR_PARAM;

    if (0 == j) {
      attribute = ObIntegerStream::Attribute::USE_NONE;
    } else if (1 == j) {
      attribute = ObIntegerStream::Attribute::USE_BASE;
    } else if (2 == j) {
      mon = STRICT_INCREMENT;
    } else if (3 == j) {
      attribute = ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      has_null = true;
    }

    test_and_check_int_datums_all_encoding_type<uint64_t>(
        attribute, has_null, min, max, null_replaced_value, ObIntegerStream::UW_8_BYTE,
        use_nullbitmap, j, mon, null_replaced_rowid);
  }
}

TEST_F(TestIntegerStream, test_negative_int_datums)
{
  for (int64_t j = 0; j < 8; j ++) {
    DECLEAR_PARAM;

    if (0 == j) {
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      has_null = true;
      int_min = INT64_MIN + 1;
      int_max = INT64_MAX - 1;
      null_replaced_value = INT64_MIN;
      width = ObIntegerStream::UW_8_BYTE;
    } else if (1 == j) {
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      has_null = true;
      int_min = INT32_MIN;
      int_max = INT32_MAX - 1;
      null_replaced_value = INT32_MAX;
      width = ObIntegerStream::UW_4_BYTE;
    } else if (2 == j) {
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      has_null = true;
      int_min = INT16_MIN + 1;
      int_max = INT16_MAX - 1;
      null_replaced_value = INT16_MIN;
      width = ObIntegerStream::UW_2_BYTE;
    } else if (3 == j) {
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      has_null = true;
      int_min = INT8_MIN;
      int_max = INT8_MAX - 1;
      null_replaced_value = INT8_MAX;
      width = ObIntegerStream::UW_1_BYTE;
    } else if (4 == j) {
      use_nullbitmap = true;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      int_min = INT64_MIN;
      int_max = INT64_MAX;
      width = ObIntegerStream::UW_8_BYTE;
    } else if (5 == j) {
      use_nullbitmap = true;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      int_min = INT32_MIN;
      int_max = INT32_MAX;
      width = ObIntegerStream::UW_4_BYTE;
    } else if (6 == j) {
      use_nullbitmap = true;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      int_min = INT16_MIN;
      int_max = INT16_MAX;
      width = ObIntegerStream::UW_2_BYTE;
    } else if (7 == j) {
      use_nullbitmap = true;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
      int_min = INT8_MIN;
      int_max = INT8_MAX;
      width = ObIntegerStream::UW_1_BYTE;
    }

    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
  }
}


TEST_F(TestIntegerStream, test_all_zero)
{
  for (int64_t j = 0; j < 3; j ++) {
    DECLEAR_PARAM;
    int_min = 0;
    int_max = 0;
    if (0 == j) {
      has_null = false;
      use_nullbitmap = false;
      attribute = 0;
    } else if (1 == j) {
      has_null = true;
      use_nullbitmap = true;
    } else if (2 == j) {
      has_null = true;
      use_nullbitmap = false;
      attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
      null_replaced_value = 2;
    }

    width = ObIntegerStream::UW_1_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    width = ObIntegerStream::UW_2_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    width = ObIntegerStream::UW_4_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    width = ObIntegerStream::UW_8_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
  }
}


TEST_F(TestIntegerStream, test_all_min_max)
{
  for (int64_t j = 0; j < 2; j ++) {
    DECLEAR_PARAM;

    if (0 == j) {
      has_null = false;
      use_nullbitmap = false;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
    } else if (1 == j) {
      has_null = true;
      use_nullbitmap = true;
      attribute |= ObIntegerStream::Attribute::USE_BASE;
    }

    int_min = INT8_MIN;
    int_max = INT8_MAX;
    width = ObIntegerStream::UW_1_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    int_min = INT16_MIN;
    int_max = INT16_MAX;
    width = ObIntegerStream::UW_2_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    int_min = INT32_MIN;
    int_max = INT32_MAX;
    width = ObIntegerStream::UW_4_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

    int_min = INT64_MIN;
    int_max = INT64_MAX;
    width = ObIntegerStream::UW_8_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
  }
}


TEST_F(TestIntegerStream, test_negative_inc_delta)
{
  for (int64_t j = 1; j < 2; j ++) {
    DECLEAR_PARAM

    if (0 == j) {
      int_min = INT32_MIN + 2;
      int_max = 0;
      has_null = false;
      mon = STRICT_INCREMENT;

      width = ObIntegerStream::UW_4_BYTE;
      test_and_check_int_datums_all_encoding_type<int64_t>(
          attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
    } else if (1 == j) {
      mon = STRICT_DECREMENT;
      int_max = UINT32_MAX + 1LL; // first value > UINT32_MAX
      int_min = UINT32_MAX + 1LL;

      width = ObIntegerStream::UW_8_BYTE;
      test_and_check_int_datums_all_encoding_type<int64_t>(
          attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
    }
  }
}

TEST_F(TestIntegerStream, test_delta)
{
  for (int64_t j = 0; j < 12; j ++) {
    DECLEAR_PARAM;
    has_null = false;

    if (0 == j) {
      mon = STRICT_INCREMENT;
      int_min = 5;
    } else if (1 == j) {
      mon = STRICT_INCREMENT;
      int_min = INT32_MIN;
    } else if (2 == j) {
      mon = STRICT_INCREMENT;
      int_min = -5;
    } else if (3 == j) {
      mon = STRICT_DECREMENT;
      int_min = INT32_MAX;
    } else if (4 == j) {
      mon = STRICT_DECREMENT;
      int_min = 5;
    } else if (5 == j) {
      mon = STRICT_DECREMENT;
      int_min = -5;
    } else if (6 == j) {
      mon = EQUAL;
      int_min = 5;
    } else if (7 == j) {
      mon = EQUAL;
      int_min = -5;
    } else if (8 == j) {
      mon = EQUAL;
      int_min = 0;
    } else if (9 == j) {
      mon = EQUAL;
      int_min = 5;
    } else if (10 == j) {
      mon = EQUAL;
      int_min = -5;
    } else if (11 == j) {
      mon = EQUAL;
      int_min = 0;
    }

    width = ObIntegerStream::UW_8_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
  }
}

TEST_F(TestIntegerStream, test_all_0_and_replace_value_negative)
{
  for (int64_t j = 0; j < 1; j ++) {
    DECLEAR_PARAM;
    attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
    attribute |= ObIntegerStream::Attribute::USE_BASE;
    int_min = 0;
    int_max = 0;
    has_null = true;
    null_replaced_value = -1;

    width = ObIntegerStream::UW_1_BYTE;
    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);
  }
}

TEST_F(TestIntegerStream, test_raw_negative_with_null)
{
  for (int64_t j = 0; j < 1; j ++) {
    DECLEAR_PARAM;
    attribute |= ObIntegerStream::Attribute::REPLACE_NULL_VALUE;
    int_min = -100;
    int_max = int_min + 100;
    has_null = true;
    null_replaced_value = -101;
    width = ObIntegerStream::UW_1_BYTE;
    attribute |= ObIntegerStream::Attribute::USE_BASE;

    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, j, mon, null_replaced_rowid);

  }
}

TEST_F(TestIntegerStream, test_null_replaced_rowid)
{
  DECLEAR_PARAM;
  int_min = -1000;
  int_max = int_min + 1000;
  width = ObIntegerStream::UW_2_BYTE;
  has_null = true;
  null_replaced_rowid = true;
  attribute |= ObIntegerStream::Attribute::USE_BASE;
  null_replaced_value = -1001;

  test_and_check_int_datums_all_encoding_type<int64_t>(
      attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, 1, mon, null_replaced_rowid);
}

TEST_F(TestIntegerStream, test_rle)
{
  for (int64_t j = 0; j < 4; j ++) {
    DECLEAR_PARAM;
    if (0 == j) {
      int_min = 10; // just one unique value
      int_max = 10;
      width = ObIntegerStream::UW_1_BYTE;
    } else if (1 == j) {
      int_min = 10000; // just two unique value
      int_max = 10001;
      width = ObIntegerStream::UW_2_BYTE;
    } else if (2 == j) {
      int_min = UINT32_MAX - 1; // just two unique value
      int_max = UINT32_MAX;
      width = ObIntegerStream::UW_4_BYTE;
    } else if (3 == j) {
      int_min = INT64_MAX - 2; // just three unique value
      int_max = INT64_MAX;
      width = ObIntegerStream::UW_8_BYTE;
    }

    test_and_check_int_datums_all_encoding_type<int64_t>(
        attribute, has_null, int_min, int_max, null_replaced_value, width, use_nullbitmap, 1, mon, null_replaced_rowid);
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_integer_stream.log*");
  OB_LOGGER.set_file_name("test_integer_stream.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
