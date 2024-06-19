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
#include "test_column_decoder.h"
#define protected public
#define private public

#define PUSHDOWN_GENERAL_TEST(x) \
            TEST_F(x, basic_filter_pushdown_op_test_eq_ne_nu_nn) { basic_filter_pushdown_eq_ne_nu_nn_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_comparison) { basic_filter_pushdown_comparison_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_in) { basic_filter_pushdown_in_op_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_bt) { basic_filter_pushdown_bt_test(); }

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestDictDecoder : public TestColumnDecoder
{
public:
  TestDictDecoder() : TestColumnDecoder(ObColumnHeader::Type::DICT) {}
  virtual ~TestDictDecoder() {}
};

class TestRLEDecoder : public TestColumnDecoder
{
public:
  TestRLEDecoder() : TestColumnDecoder(ObColumnHeader::Type::RLE) {}
  virtual ~TestRLEDecoder() {}
};

class TestIntBaseDiffDecoder : public TestColumnDecoder
{
public:
  TestIntBaseDiffDecoder() : TestColumnDecoder(ObColumnHeader::Type::INTEGER_BASE_DIFF) {}
  virtual ~TestIntBaseDiffDecoder() {}
};

class TestRetroPDDecoder : public TestColumnDecoder
{
public:
  TestRetroPDDecoder() : TestColumnDecoder(true) {}
  virtual ~TestRetroPDDecoder() {}
};

class TestHexDecoder : public TestColumnDecoder
{
public:
  TestHexDecoder() : TestColumnDecoder(ObColumnHeader::Type::HEX_PACKING) {}
  virtual ~TestHexDecoder() {}
};

class TestStringDiffDecoder : public TestColumnDecoder
{
public:
  TestStringDiffDecoder() : TestColumnDecoder(ObColumnHeader::Type::STRING_DIFF) {}
  virtual ~TestStringDiffDecoder() {}
};

class TestStringPrefixDecoder : public TestColumnDecoder
{
public:
  TestStringPrefixDecoder() : TestColumnDecoder(ObColumnHeader::Type::STRING_PREFIX) {}
  virtual ~TestStringPrefixDecoder() {}
};

class TestColumnEqualDecoder : public TestColumnDecoder
{
public:
  TestColumnEqualDecoder() : TestColumnDecoder(ObColumnHeader::Type::COLUMN_EQUAL) {}
  virtual ~TestColumnEqualDecoder() {}
};

class TestInterColumnSubstringDecoder : public TestColumnDecoder
{
public:
  TestInterColumnSubstringDecoder() : TestColumnDecoder(ObColumnHeader::Type::COLUMN_SUBSTR) {}
  virtual ~TestInterColumnSubstringDecoder() {}
};

TEST_F(TestIntBaseDiffDecoder, filter_pushdown_comaprison_neg_test)
{
  filter_pushdown_comaprison_neg_test();
}

PUSHDOWN_GENERAL_TEST(TestRetroPDDecoder);
PUSHDOWN_GENERAL_TEST(TestDictDecoder);
PUSHDOWN_GENERAL_TEST(TestRLEDecoder);
PUSHDOWN_GENERAL_TEST(TestIntBaseDiffDecoder);

TEST_F(TestHexDecoder, basic_filter_pushdown_op_test_eq_ne_nu_nn)
{
  basic_filter_pushdown_eq_ne_nu_nn_test();
}

TEST_F(TestDictDecoder, batch_decode_to_datum_condense_test)
{
  batch_decode_to_datum_test(true);
}

TEST_F(TestDictDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestDictDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestDictDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_FIXED);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestDictDecoder, batch_decode_single_var_len_dict) {
  const int64_t string_len = UINT16_MAX + 3;
  char *string_buf = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, row));
  int64_t varchar_col_idx = -1;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    if (col_descs_.at(i).col_type_.get_type() == ObVarcharType) {
      varchar_col_idx = i;
      break;
    }
  }
  ASSERT_NE(varchar_col_idx, -1);
  string_buf = static_cast<char *>(allocator_.alloc(string_len));
  ASSERT_TRUE(nullptr != string_buf);
  MEMSET(string_buf, 7, string_len);
  row.storage_datums_[varchar_col_idx].ptr_ = string_buf;
  row.storage_datums_[varchar_col_idx].pack_ = string_len;

  ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int32_t row_id = 0;
  const char *cell_data = nullptr;
  ObDatum datum;
  char datum_buf[40];
  datum.ptr_ = datum_buf;
  ASSERT_EQ(OB_SUCCESS, decoder.decoders_[varchar_col_idx].batch_decode(decoder.row_index_, &row_id, &cell_data, 1, &datum));

  ASSERT_EQ(datum.len_, string_len);
  ASSERT_EQ(0, MEMCMP(datum.ptr_, string_buf, string_len));


}

TEST_F(TestRLEDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestRLEDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestRLEDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_FIXED);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestIntBaseDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestIntBaseDiffDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestIntBaseDiffDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_FIXED);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestHexDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestHexDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestHexDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestStringDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringDiffDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestStringDiffDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestStringPrefixDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringPrefixDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestStringPrefixDecoder, cell_decode_to_datum_test_without_hex)
{
  cell_decode_to_datum_test_without_hex();
}

TEST_F(TestStringPrefixDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestColumnEqualDecoder, cell_decode_to_datum_test)
{
  cell_column_equal_decode_to_datum_test();
}

TEST_F(TestColumnEqualDecoder, col_equal_batch_decode_to_vector_test)
{
  col_equal_batch_decode_to_vector_test(VEC_FIXED);
  col_equal_batch_decode_to_vector_test(VEC_UNIFORM);
  col_equal_batch_decode_to_vector_test(VEC_DISCRETE);
  col_equal_batch_decode_to_vector_test(VEC_CONTINUOUS);
}

TEST_F(TestInterColumnSubstringDecoder, cell_decode_to_datum_test)
{
  cell_inter_column_substring_to_datum_test();
}

TEST_F(TestInterColumnSubstringDecoder, col_substr_batch_decode_to_vector_test)
{
  col_substr_batch_decode_to_vector_test(VEC_UNIFORM);
  col_substr_batch_decode_to_vector_test(VEC_DISCRETE);
  col_substr_batch_decode_to_vector_test(VEC_CONTINUOUS);
}


// TEST_F(TestDictDecoder, batch_decode_perf_test)
// {
//   batch_get_row_perf_test();
// }

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_general_column_decoder.log*");
  OB_LOGGER.set_file_name("test_general_column_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
