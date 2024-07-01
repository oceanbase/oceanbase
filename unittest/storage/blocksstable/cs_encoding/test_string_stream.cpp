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
#include <iostream>
#include <random>
#define protected public
#define private public
#include "storage/blocksstable/cs_encoding/ob_string_stream_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_string_stream_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_cs_decoding_util.h"
#include "lib/codec/ob_fast_delta.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{
namespace blocksstable
{

class TestStringStream : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}

  TestStringStream() : tenant_ctx_(500)
  {
    srand(time(NULL));
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
  }
  virtual ~TestStringStream() {}

  int64_t max_count = 64<<9;

  void randstr(char *str, const int64_t len)
  {
    int i;
    for (i = 0; i < len; ++i)
    {
      switch ((rand() % 3)) {
        case 1:
          str[i] = 'A' + rand() % 26;
          break;
        case 2:
          str[i] = 'a' + rand() % 26;
          break;
        default:
          str[i] = '0' + rand() % 10;
          break;
      }
    }
  }

  void generate_datums(ObColDatums *datums, const int64_t size, bool has_null, bool is_fix_len,
      bool has_empty_string, bool all_empty, int64_t &total_len)
  {
    total_len = 0;
    for (int64_t i = 0; i < size; i++) {
      ObDatum datum;
      if (has_null && (i % 5 == 0)) {
        datum.set_null();
      } else {
        int64_t len = i%333 + 1;
        if (is_fix_len) {
          len = 5;
        }
        if ((has_empty_string && (i % 7 == 0))
            || all_empty) {
          len = 0;
        }
        char *tmp = reinterpret_cast<char *>(allocator_.alloc(len));
        datum.ptr_ = tmp;
        datum.len_ = len;
        randstr(tmp, len);
      }

      ASSERT_EQ(OB_SUCCESS, datums->push_back(datum));
      total_len += datum.len_;
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

  void test_and_check_str_datums(int64_t size, const ObCompressorType type, bool use_zero_len_as_null, bool has_null, bool is_fix_len,
      bool use_nullbitmap, bool has_empty_string, bool all_null, bool all_empty, bool half_null_half_empty, bool use_null_replaced_ref)
  {
    LOG_INFO("test_and_check_string_encoding", K(size), K(type), K(use_zero_len_as_null), K(has_null), K(is_fix_len),
        K(use_nullbitmap), K(all_null), K(half_null_half_empty), K(use_null_replaced_ref));
    ObArenaAllocator local_arena;
    ObStringStreamEncoderCtx ctx;
    ObCSEncodingOpt encoding_opt;
    bool is_use_zero_len_as_null = use_zero_len_as_null;
    int64_t fixed_len = -1;
    if (is_fix_len) {
      fixed_len = 5;
    }
    if (all_empty) {
      fixed_len = 0;
    }
    if (!(use_nullbitmap || all_empty || has_empty_string || half_null_half_empty || use_null_replaced_ref)) {
      is_use_zero_len_as_null = true;
    }
    common::ObCompressor *compressor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(type, compressor));

    ObStringStreamEncoder encoder;
    uint32_t *data = nullptr;
    ObColDatums *datums = new ObColDatums(local_arena);
    ASSERT_EQ(OB_SUCCESS, datums->resize(max_count));
    datums->reuse();
    if (half_null_half_empty) {
      all_empty = true;
    }
    int64_t total_len = 0;
    generate_datums(datums, size, has_null, is_fix_len, has_empty_string, all_empty, total_len);
    if (all_null) {
      for (int64_t i = 0; (i < datums->count()); i++) {
        total_len -= datums->at(i).len_;
        datums->at(i).set_null();
      }
    }
    if (half_null_half_empty) {
      for (int64_t i = 0; (i < datums->count()); i++) {
        if (i%2 == 0) {
          total_len -= datums->at(i).len_;
          datums->at(i).set_null();
        }
      }
    }
    if (fixed_len >= 0) {
      total_len = datums->count() * fixed_len;
    }
    ctx.build_string_stream_meta(fixed_len, is_use_zero_len_as_null, total_len);
    ctx.build_string_stream_encoder_info(type, false, &encoding_opt, nullptr, -1, &allocator_);
    int64_t bitmap_size = pad8(size);
    char *bitmap = new char[bitmap_size];
    memset(bitmap, 0, bitmap_size);
    generate_bitmap(bitmap, datums);

    ObMicroBufferWriter writer;
    ObMicroBufferWriter all_string_writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(OB_DEFAULT_MACRO_BLOCK_SIZE, OB_DEFAULT_MACRO_BLOCK_SIZE));
    ASSERT_EQ(OB_SUCCESS, all_string_writer.init(OB_DEFAULT_MACRO_BLOCK_SIZE, OB_DEFAULT_MACRO_BLOCK_SIZE));
    common::ObArray<uint32_t> offsets;

    ObColumnDatumIter iter(*datums);
    ASSERT_EQ(OB_SUCCESS, encoder.encode(ctx, iter, writer, &all_string_writer, offsets));

    ObStreamData str_data;
    ObStreamData raw_offset_data;
    // 1. decode integer_stream header
    ObIntegerStreamDecoderCtx offset_decoder_ctx;
    uint16_t meta_len = 0;
    if (!ctx.meta_.is_fixed_len_string()) {
      const char *int_stream_start = writer.data() + offsets[0];
      int64_t int_stream_len = offsets[1] - offsets[0];
      ObStreamData data(int_stream_start, int_stream_len);
      buid_raw_integer_stream_data(data, size, type, offset_decoder_ctx, raw_offset_data);
    }

    // 2. build_decoding_ctx for string stream
    ObStringStreamDecoderCtx str_decode_ctx;
    const char *str_stream_start = writer.data();
    int64_t str_stream_meta_len = offsets[0];
    ObStreamData data2(str_stream_start, str_stream_meta_len);
    uint16_t str_meta_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObStringStreamDecoder::build_decoder_ctx(data2, str_decode_ctx, str_meta_len));
    str_data.set(all_string_writer.data(), all_string_writer.length());

    // 3. decode str
    ObColDatums *datums2 = new ObColDatums(local_arena);
    ASSERT_EQ(OB_SUCCESS, datums2->resize(max_count));
    datums2->reuse();
    for (int64_t i = 0; i < size; i++) {
      ObDatum datum;
      ASSERT_EQ(OB_SUCCESS, datums2->push_back(datum));
    }

    // test batch decode
    int32_t *row_ids = new int32_t[size];
    for (int32_t i = 0; i < size; i++) {
      row_ids[i] = i;
    }
    ObDatum *datums3 = new ObDatum[size];
    char *datums2_buf = new char[size * sizeof(uint64_t)];
    memset(datums2_buf, 0, size * sizeof(uint64_t));
    for (int64_t i = 0; i < size; i++) {
      datums3[i].ptr_ = (datums2_buf + i * sizeof(uint64_t));
    }

    uint64_t *ref_arr = new uint64_t[size];
    int64_t null_replaced_ref = size;
    uint32_t ref_width_V = ObRefStoreWidthV::NOT_REF;

    ObBaseColumnDecoderCtx base_ctx;
    base_ctx.allocator_ = &allocator_;
    base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL;
    base_ctx.null_desc_ = nullptr;
    if (use_nullbitmap) {
      base_ctx.null_bitmap_ = bitmap;
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP;
    } else if (use_null_replaced_ref) {
      ref_width_V = ObRefStoreWidthV::REF_IN_DATUMS;
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF;
      base_ctx.null_replaced_ref_ = null_replaced_ref;
      for (int64_t i = 0; i < size; i++) {
        if (use_null_replaced_ref && datums->at(i).is_null()) {
          datums3[i].pack_ = size;
          ref_arr[i] = size;
        } else {
          datums3[i].pack_ = i;
          ref_arr[i] = i;
        }
      }
    } else if (fixed_len < 0 && is_use_zero_len_as_null) {
      base_ctx.null_flag_ = ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED;
    }

    const uint8_t offset_width = str_decode_ctx.meta_.is_fixed_len_string() ?
            FIX_STRING_OFFSET_WIDTH_V : offset_decoder_ctx.meta_.width_;
    ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
        [offset_width]
        [ref_width_V]
        [base_ctx.null_flag_]
        [false/*need_copy_V*/];
    convert_func(base_ctx, str_data.buf_, str_decode_ctx, raw_offset_data.buf_, nullptr, row_ids, size, datums3);

    for (int64_t i = 0; i < size; i++) {
      if (!ObDatum::binary_equal(datums->at(row_ids[i]), datums3[i])) {
        LOG_INFO("not equal", K(datums->at(row_ids[i])), K(datums3[i]), K(i), K(row_ids[i]));
        ::abort();
      }
    }

    // disorder batch decode
    for (int64_t i = 0; i < size; i++) {
      datums3[i].reset();
    }
    int64_t random_idx = ObTimeUtility::current_time()%size;
    int32_t row_id = 0;
    for (int64_t i = 0; i < size; i++) {
      row_id = (i + random_idx) % size;
      row_ids[i] = row_id;
      if (use_null_replaced_ref && datums->at(row_id).is_null()) {
        datums3[i].pack_ = size;
      } else {
        datums3[i].pack_ = row_id;
      }
      if (i%9 == 0) {
        row_ids[i] = random_idx; //duplicate
        if (use_null_replaced_ref && datums->at(random_idx).is_null()) {
          datums3[i].pack_ = size;
        } else {
          datums3[i].pack_ = random_idx;
        }
      }
    }

    convert_func = convert_string_to_datum_funcs
        [offset_width]
        [ref_width_V]
        [base_ctx.null_flag_]
        [false/*need_copy_V*/];
    convert_func(base_ctx, str_data.buf_, str_decode_ctx, raw_offset_data.buf_, nullptr, row_ids, size, datums3);

    for (int64_t i = 0; i < size; i++) {
      if (!ObDatum::binary_equal(datums->at(row_ids[i]), datums3[i])) {
        LOG_INFO("not equal", K(datums->at(row_ids[i])), K(datums3[i]), K(i), K(row_ids[i]));
        ::abort();
      }
    }

    // test batch decode with ref arr
    if (use_null_replaced_ref) {
      convert_func = convert_string_to_datum_funcs
          [offset_width]
          [ObRefStoreWidthV::REF_8_BYTE]
          [base_ctx.null_flag_]
          [false/*need_copy_V*/];
      convert_func(base_ctx, str_data.buf_, str_decode_ctx, raw_offset_data.buf_, (char*)ref_arr, row_ids, size, datums3);

      for (int64_t i = 0; i < size; i++) {
        if (!ObDatum::binary_equal(datums->at(row_ids[i]), datums3[i])) {
          LOG_INFO("not equal", K(datums->at(row_ids[i])), K(datums3[i]), K(i), K(row_ids[i]), K(ref_arr[row_ids[i]]));
          ::abort();
        }
      }
    }

    delete []ref_arr;
    delete []row_ids;
    row_ids = nullptr;
    delete []datums3;
    datums3 = nullptr;
    delete datums2;
    datums2 = nullptr;
    delete datums;
    datums = nullptr;
  }

protected:
  ObArenaAllocator allocator_;
  share::ObTenantBase tenant_ctx_;
};

TEST_F(TestStringStream, test_datums_encoding)
{
  for (int64_t j = 0; j < 13; j++) {
    common::ObCompressorType compress_type = ObCompressorType::NONE_COMPRESSOR;
    bool use_zero_len_as_null = 0;
    bool has_null = false;
    bool is_fix_len = false;
    bool use_nullbitmap = false;
    bool has_empty_string = false;
    bool all_null = false;
    bool all_empty = false;
    bool half_null_half_empty = false;
    bool use_null_replaced_ref = false;
    if (0 == j) {
      compress_type = NONE_COMPRESSOR;
    } else if (1 == j) {
      compress_type = LZ4_COMPRESSOR;
    } else if (2 == j) {
      compress_type = SNAPPY_COMPRESSOR;
      has_null = true;
      use_zero_len_as_null = true;
    } else if (3 == j) {
      compress_type = ZSTD_1_3_8_COMPRESSOR;
      is_fix_len = true;
    } else if (4 == j) {
      compress_type = SNAPPY_COMPRESSOR;
      is_fix_len = true;
      use_nullbitmap = true;
      has_null = true;
    } else if (5 == j) {
      compress_type = ZSTD_1_3_8_COMPRESSOR;
      has_null = true;
      use_nullbitmap = true;
    } else if (6 == j) {
      has_null = true;
      use_nullbitmap = true;
      has_empty_string = true;
    } else if (7 == j) {
      // all null, use null replace value
      all_null = true;
    } else if (8 == j) {
      // all empty string
      all_empty = true;
    } else if (9 == j) {
      // half null and half empty
      half_null_half_empty = true;
      use_nullbitmap = true;
    } else if (10 == j) {
      // null replace row id
      compress_type = SNAPPY_COMPRESSOR;
      use_null_replaced_ref = true;
    } else if (11 == j) {
      // null replace row id
      compress_type = SNAPPY_COMPRESSOR;
      use_null_replaced_ref = true;
      has_null = true;
    } else if (12 == j) {
      // null replace row id
      compress_type = SNAPPY_COMPRESSOR;
      use_null_replaced_ref = true;
      all_null = true;
    }

    for (int64_t i = 1; i <= max_count; i=(i * (i + j + 1))) {
      LOG_INFO("round", K(i), K(j));
      test_and_check_str_datums(i, compress_type, use_zero_len_as_null, has_null, is_fix_len, use_nullbitmap,
          has_empty_string, all_null, all_empty, half_null_half_empty, use_null_replaced_ref);
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_string_stream.log*");
  OB_LOGGER.set_file_name("test_string_stream.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
