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
#include "test_decoder_filter_perf.h"
#include "storage/compaction/ob_compaction_memory_pool.h"

namespace oceanbase
{

namespace storage
{

// just for test, skip to use
int ObCompactionBufferWriter::ensure_space(const int64_t size)
{
  int ret = OB_SUCCESS;
  use_mem_pool_ = false;

  if (size <= 0) {
    // do nothing
  } else if (nullptr == data_) { // first alloc
    if (OB_UNLIKELY(!block_.empty())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      data_ = (char *) share::mtl_malloc(size, label_);
      pos_ = 0;
      capacity_ = size;
      block_.buffer_ = data_;
      block_.buffer_size_ = size;
      block_.type_ = ObCompactionBufferBlock::MTL_PIECE_TYPE;
    }
  } else if (capacity_ < size) {
    char *new_data = (char *) share::mtl_malloc(size, label_);
    MEMCPY(new_data, data_, pos_);
    data_ = new_data;
    capacity_ = size;
    block_.buffer_ = data_;
    block_.buffer_size_ = size;
    block_.type_ = ObCompactionBufferBlock::MTL_PIECE_TYPE;
  }
  return ret;
}

void ObCompactionBufferWriter::reset()
{
}
}

namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestPerfCmpResult : public TestDecoderFilterPerf
{
public:
  TestPerfCmpResult() {}
  virtual ~TestPerfCmpResult() {}

  virtual void SetUp();
  virtual void TearDown();
};

void TestPerfCmpResult::SetUp()
{
  TestDecoderFilterPerf::SetUp();
}

void TestPerfCmpResult::TearDown()
{
  TestDecoderFilterPerf::TearDown();
}

#define NEED_EXECUTE_CASE true

#define EXE_ROUND 1
#define IS_BIT_PACKING false
#define WITH_NULL true
#define WITHOUT_NULL false
#define WITH_WARM true

#define TEST_FILTER_DECODE_ALL_OP() \
  int64_t i = 100; \
  while (i-- > 0) { \
    basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/); \
    basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_comp_op_test(true/*is_column_store*/); \
    basic_filter_pushdown_comp_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_in_op_test(true/*is_column_store*/); \
    basic_filter_pushdown_in_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_bt_op_test(true/*is_column_store*/); \
    basic_filter_pushdown_bt_op_test(false/*is_column_store*/); \
    basic_reuse(); \
  } \
  perf_ctx_.print_report(true/*print total*/, true/*print detail*/); \

#define TEST_EXECUTE_CS_PAX_ALL_OP() \
  basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_comp_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_comp_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_in_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_in_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_bt_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_bt_op_test(true/*is_column_store*/); \
  basic_reuse(); \

#define TEST_EXECUTE_CS_PAX_ALL_OP_TR() \
  basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_comp_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_comp_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_in_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_in_op_test(false/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_bt_op_test(true/*is_column_store*/); \
  basic_reuse(); \
  basic_filter_pushdown_bt_op_test(false/*is_column_store*/); \
  basic_reuse(); \

#define TEST_FILTER_DECODE_ALL_OP_ONCE(with_warm) \
  if (with_warm) { \
    TEST_EXECUTE_CS_PAX_ALL_OP() \
    perf_ctx_.reuse(); \
  } \
  TEST_EXECUTE_CS_PAX_ALL_OP() \
  TEST_EXECUTE_CS_PAX_ALL_OP_TR() \
  perf_ctx_.print_report(true/*print total*/, true/*print detail*/); \

#define TEST_CS_FILTER_DECODE_ALL_OP() \
  int64_t i = 1; \
  while (i-- > 0) { \
    basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_comp_op_test(true/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_in_op_test(true/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_bt_op_test(true/*is_column_store*/); \
    basic_reuse(); \
  } \
  perf_ctx_.print_report(true/*print total*/, true/*print detail*/); \

#define TEST_PAX_FILTER_DECODE_ALL_OP() \
  int64_t i = 1; \
  while (i-- > 0) { \
    basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_comp_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_in_op_test(false/*is_column_store*/); \
    basic_reuse(); \
    basic_filter_pushdown_bt_op_test(false/*is_column_store*/); \
    basic_reuse(); \
  } \
  perf_ctx_.print_report(true/*print total*/, true/*print detail*/); \

// TEST_F(TestPerfCmpResult, test_raw_decoder_010)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, false/*need_compress*/, true/*need_decode*/,
//     false/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_raw_decoder_011)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, false/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_raw_decoder_111)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_raw_decoder_1110)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_raw_decoder_1111)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, true/*is_bit_packing*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_dict_decoder_010)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, false/*need_compress*/, true/*need_decode*/,
//     false/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_dict_decoder_011)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, false/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_dict_decoder_111)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_dict_decoder_1110)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_dict_decoder_1111)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, true/*is_bit_packing*/));
//   TEST_FILTER_DECODE_ALL_OP();
// }

// TEST_F(TestPerfCmpResult, test_raw_get_rows_with_null)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/, false/*check_all_type*/, 1/*round*/));
//   basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/);
//   basic_reuse();
//   basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/);
//   basic_reuse();
//   perf_ctx_.print_report(true/*print total*/, true/*print detail*/);
// }

// TEST_F(TestPerfCmpResult, test_raw_get_rows_without_null)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/, false/*check_all_type*/, 50/*round*/));
//   basic_filter_pushdown_bt_op_test(true/*is_column_store*/);
//   basic_reuse();
//   basic_filter_pushdown_bt_op_test(false/*is_column_store*/);
//   basic_reuse();
//   perf_ctx_.print_report(true/*print total*/, true/*print detail*/);
// }

// TEST_F(TestPerfCmpResult, test_dict_get_rows_with_null)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/, false/*check_all_type*/, 50/*round*/));
//   basic_filter_pushdown_eqne_nunn_op_test(true/*is_column_store*/);
//   basic_reuse();
//   basic_filter_pushdown_eqne_nunn_op_test(false/*is_column_store*/);
//   basic_reuse();
//   perf_ctx_.print_report(true/*print total*/, true/*print detail*/);
// }

// TEST_F(TestPerfCmpResult, test_dict_get_rows_without_null)
// {
//   ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
//     true/*need_cs_full_transform*/, false/*is_bit_packing*/, false/*check_all_type*/, 50/*round*/));
//   basic_filter_pushdown_bt_op_test(true/*is_column_store*/);
//   basic_reuse();
//   basic_filter_pushdown_bt_op_test(false/*is_column_store*/);
//   basic_reuse();
//   perf_ctx_.print_report(true/*print total*/, true/*print detail*/);
// }

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_f5)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      5/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_f40)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      40/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_f95)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      95/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_f100)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      100/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_t5)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      5/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_t40)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      40/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_t95)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      95/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_raw_get_rows_t100)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(true/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      100/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_f5)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      5/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_f40)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      40/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_f95)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      95/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_f100)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      100/*highest_data_pct*/, WITHOUT_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_t5)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      5/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_t40)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      40/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_t95)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      95/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

TEST_F(TestPerfCmpResult, test_general_dict_get_rows_t100)
{
  if (NEED_EXECUTE_CASE) {
    ASSERT_EQ(OB_SUCCESS, prepare(false/*is_raw_*/, true/*need_compress*/, true/*need_decode*/,
      true/*need_cs_full_transform*/, IS_BIT_PACKING, false/*check_all_type*/, EXE_ROUND/*round*/,
      100/*highest_data_pct*/, WITH_NULL));
    TEST_FILTER_DECODE_ALL_OP_ONCE(WITH_WARM);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_perf_cmp_result.log*");
  OB_LOGGER.set_file_name("test_perf_cmp_result.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  std::string filter = testing::GTEST_FLAG(filter);
  if (filter != "") {
    return RUN_ALL_TESTS();
  }
  return RUN_ALL_TESTS();
}
