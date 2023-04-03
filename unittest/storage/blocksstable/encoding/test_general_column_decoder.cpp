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

TEST_F(TestRLEDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestIntBaseDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestHexDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringPrefixDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
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
