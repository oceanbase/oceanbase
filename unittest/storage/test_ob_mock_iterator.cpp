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

#include "ob_mock_iterator.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::common::hash;
using namespace oceanbase::unittest;
namespace unittest
{
class TestObMockIteratorBuilder : public ::testing::Test
{
public:
  void SetUp()
  {
    row = NULL;
    ret_val = OB_SUCCESS;
  }
  void TearDown()
  {
    iter.reset();
    row = NULL;
  }

  ObMockIterator iter;
  const ObStoreRow *row;
  ObString vchar;
  char buf[ObMockIteratorBuilder::MAX_DATA_LENGTH];
  int ret_val;
};

TEST_F(TestObMockIteratorBuilder, get)
{
  ObStoreRow rows[32];
  ObObj objs[32];
  int64_t i = 0;
  for (i = 0; i < 32; ++i) {
    rows[i].row_val_.cells_ = &objs[i];
    rows[i].row_val_.count_ = 1;
    rows[i].row_val_.cells_[0].set_int(i);
    iter.add_row(&rows[i]);
  }

  for (i = 0; i < 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
    ASSERT_EQ(i % 32, row->row_val_.cells_[0].get_int());
  }

  iter.reset_iter();
  ASSERT_EQ(OB_SUCCESS, iter.get_row(7, row));
  ASSERT_EQ(7, row->row_val_.cells_[0].get_int());
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(0, row->row_val_.cells_[0].get_int());

  ASSERT_EQ(OB_SUCCESS, iter.get_row(31, row));
  ASSERT_EQ(31, row->row_val_.cells_[0].get_int());
  ASSERT_EQ(OB_ITER_END, iter.get_row(32, row));

  iter.reset_iter();
  ASSERT_EQ(OB_SUCCESS, iter.get_row(0, row));
  ASSERT_EQ(0, row->row_val_.cells_[0].get_int());
  ASSERT_EQ(OB_ITER_END, iter.get_row(32, row));

  iter.reset();
  ASSERT_EQ(OB_ITER_END, iter.get_row(0, row));

  ret_val = iter.add_row(NULL);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret_val);
}

TEST_F(TestObMockIteratorBuilder, from)
{
  const char *input = "int varchar\n"
                      "1   'test string'\n";
  ret_val = iter.from(input);
  ASSERT_EQ(OB_SUCCESS, ret_val);

  // row 0
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  // int
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(1, row->row_val_.cells_[0].get_int());

  // varchar
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[1].get_type());
  row->row_val_.cells_[1].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("test string", buf);

  ObMockStoreRowIterator iter1;
  ret_val = iter1.from(input);
  ASSERT_EQ(OB_SUCCESS, ret_val);
}

TEST_F(TestObMockIteratorBuilder, ext)
{
  const char *input = "int var  num  ts  int int\n"
                      "1   NULL MIN  NOP MAX 2\n";
  ret_val = iter.from(input);
  ASSERT_EQ(OB_SUCCESS, ret_val);

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_TRUE(row->row_val_.cells_[0].is_int());
  ASSERT_TRUE(row->row_val_.cells_[1].is_null());
  ASSERT_TRUE(row->row_val_.cells_[2].is_min_value());
  ASSERT_TRUE(row->row_val_.cells_[3].is_nop_value());
  ASSERT_TRUE(row->row_val_.cells_[4].is_max_value());
  ASSERT_TRUE(row->row_val_.cells_[5].is_int());
}

TEST_F(TestObMockIteratorBuilder, parse_header_error)
{
  ASSERT_EQ(OB_EMPTY_RESULT, iter.from(" "));
  ASSERT_EQ(OB_EMPTY_RESULT, iter.from("\nint\n1\n"));
  ASSERT_EQ(OB_OBJ_TYPE_ERROR, iter.from("intt\n1\n"));
  ASSERT_EQ(OB_OBJ_TYPE_ERROR, iter.from("i\n1\n"));
}

TEST_F(TestObMockIteratorBuilder, parse_int)
{
  const char *input = "int\n"
                      "0x1234abcdef\n"
                      "0xfffffffffffffff\n"
                      "07654321\n"
                      "0\n";

  ASSERT_EQ(OB_SUCCESS, iter.from(input));

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(0x1234abcdef, row->row_val_.cells_[0].get_int());

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(0xfffffffffffffff, row->row_val_.cells_[0].get_int());

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(07654321, row->row_val_.cells_[0].get_int());

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(0, row->row_val_.cells_[0].get_int());

  // error type
  const char *input2 = "int\n"
                       "0xffffffffffffffffff\n";
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, iter.from(input2));

  const char *input3 = "int\n"
                       "abc\n";
  ASSERT_EQ(OB_ERR_CAST_VARCHAR_TO_NUMBER, iter.from(input3));
}

TEST_F(TestObMockIteratorBuilder, parse_number)
{
  const char *input = "number\n"
                      "str\n";

  ASSERT_EQ(OB_INVALID_NUMERIC, iter.from(input));
}

TEST_F(TestObMockIteratorBuilder, parse_timestamp)
{
  // error input while timestamp without ''
  // in this example 11:12:12 will be handle by parse int, and 1 by timestamp
  const char *input = "timestamp int\n"
                      "2013-12-12 11:12:12 1\n";
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, iter.from(input));

  const char *input2 = "timestamp\n"
                       "'2012-22-12 11:12:12'\n";
  ASSERT_EQ(OB_INVALID_DATE_FORMAT, iter.from(input2));

  // equal to 15-01-15 00:00:00
  const char *input3 = "timestamp\n"
                       "'15-01-15'\n";
  ASSERT_EQ(OB_SUCCESS, iter.from(input3));

  const char *input4 = "timestamp\n"
                       "1234567\n";
  ASSERT_EQ(OB_INVALID_DATE_FORMAT, iter.from(input4));
}

TEST_F(TestObMockIteratorBuilder, parse_dml_and_flag)
{

  const char *input = "int dml           flag\n"
                      "1  T_DML_UNKNOWN RF_ROW_DOES_NOT_EXIST\n"
                      "1  T_DML_INSERT  RF_ROW_EXIST\n"
                      "1  T_DML_UPDATE  RF_DELETE\n"
                      "1  T_DML_DELETE  RF_DELETE\n"
                      "1  T_DML_REPLACE RF_DELETE\n";
  ASSERT_EQ(OB_SUCCESS, iter.from(input));

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_UNKNOWN, row->get_dml());
  ASSERT_EQ(+RF_ROW_DOES_NOT_EXIST, row->flag_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_INSERT, row->get_dml());
  ASSERT_EQ(+RF_ROW_EXIST, row->flag_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_UPDATE, row->get_dml());
  ASSERT_EQ(+RF_DELETE, row->flag_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_DELETE, row->get_dml());
  ASSERT_EQ(+RF_DELETE, row->flag_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_REPLACE, row->get_dml());
  ASSERT_EQ(+RF_DELETE, row->flag_);

  // test default value
  const char *input2 = "int\n"
                       "1\n";
  ASSERT_EQ(OB_SUCCESS, iter.from(input2));
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(T_DML_UNKNOWN, row->get_dml());
  ASSERT_EQ(+RF_ROW_EXIST, row->flag_);

  const char *input3 = "int dml\n"
                       "1 aa\n";
  ASSERT_EQ(OB_HASH_NOT_EXIST, iter.from(input3));

  const char *input4 = "int flag\n"
                       "1 aa\n";
  ASSERT_EQ(OB_HASH_NOT_EXIST, iter.from(input4));
}

TEST_F(TestObMockIteratorBuilder, parse_varchar)
{
  const char *input = "varchar\n"
                      "'varchar int timestamp bool'\n"
                      "'\n\t\r'\n"
                      "'\\n\\t\\r'\n";

  ASSERT_EQ(OB_SUCCESS, iter.from(input));

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("varchar int timestamp bool", buf);

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("\n\t\r", buf);

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("\n\t\r", buf);

  // change escape
  const char *input2 = "varchar\n"
                       "'#'##'\n"
                       "'\n\t\r'\n"
                       "'\\n\\t\\r'\n";

  ASSERT_EQ(OB_SUCCESS, iter.from(input2, '#'));

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("'#", buf);

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("\n\t\r", buf);

  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[0].get_type());
  row->row_val_.cells_[0].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("\\n\\t\\r", buf);

  // input single big word
  ObString input3;
  char input_buf[ObMockIteratorBuilder::MAX_DATA_LENGTH + 16];
  input3.assign_buffer(input_buf, ObMockIteratorBuilder::MAX_DATA_LENGTH + 16);
  input3.write("varchar\n", sizeof("varchar\n") - 1);
  for (int i = 0; i <= ObMockIteratorBuilder::MAX_DATA_LENGTH; i++) {
    input3.write("a", 1);
  }
  input3.write("\n", 1);
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, iter.from(input3));

  // input single big word end with escape
  input3.assign_buffer(input_buf, ObMockIteratorBuilder::MAX_DATA_LENGTH + 16);
  input3.write("varchar\n", sizeof("varchar\n") - 1);
  for (int i = 0; i < ObMockIteratorBuilder::MAX_DATA_LENGTH; i++) {
    input3.write("a", 1);
  }
  input3.write("\\r\n", 3);
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, iter.from(input3));

  // escape at the end
  const char *input4 = "varchar\n"
                       "#";
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, iter.from(input4, '#'));

}

TEST_F(TestObMockIteratorBuilder, parse)
{
  number::ObNumber nmb;
  int64_t usec = 0;
  int64_t pos = 0;
  const char *input = "int  Varchar  number timestamp                  flag         dml\n"
                      "0x1a number   1.2    '2012-12-12 01:21:12'      RF_ROW_EXIST T_DML_INSERT\n"
                      "-1   '2\\' 3' -3     '2014-05-06 12:34:56.6789' OP_DEL_ROW   T_DML_REPLACE\n";

  ASSERT_EQ(OB_SUCCESS, iter.from(input));

  // row 0
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  // int
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(0x1a, row->row_val_.cells_[0].get_int());

  // varchar
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[1].get_type());
  row->row_val_.cells_[1].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("number", buf);

  // number
  ASSERT_EQ(ObNumberType, row->row_val_.cells_[2].get_type());
  row->row_val_.cells_[2].get_number(nmb);
  ASSERT_STREQ("1.2", nmb.format());

  // timestamp
  ASSERT_TRUE(row->row_val_.cells_[3].is_timestamp());
  row->row_val_.cells_[3].get_timestamp(usec);
  pos = 0;
  ObTimeUtility::usec_format_to_str(usec, ObString::make_string("%Y-%m-%d %T"),
                                    buf, ObMockIteratorBuilder::MAX_DATA_LENGTH, pos);
  ASSERT_STREQ("2012-12-12 01:21:12", buf);

  // flag and dml
  ASSERT_EQ(+RF_ROW_EXIST, row->flag_);
  ASSERT_EQ(T_DML_INSERT, row->get_dml());

  // row 1
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  // int negative
  ASSERT_EQ(ObIntType, row->row_val_.cells_[0].get_type());
  ASSERT_EQ(-1, row->row_val_.cells_[0].get_int());

  // varchar with eacape and space
  ASSERT_EQ(ObVarcharType, row->row_val_.cells_[1].get_type());
  row->row_val_.cells_[1].get_varchar(vchar);
  vchar.to_string(buf, ObMockIteratorBuilder::MAX_DATA_LENGTH);
  ASSERT_STREQ("2' 3", buf);

  // number negative
  ASSERT_EQ(ObNumberType, row->row_val_.cells_[2].get_type());
  row->row_val_.cells_[2].get_number(nmb);
  ASSERT_STREQ("-3", nmb.format());

  // timestamp with ms
  ASSERT_TRUE(row->row_val_.cells_[3].is_timestamp());
  row->row_val_.cells_[3].get_timestamp(usec);
  pos = 0;
  ObTimeUtility::usec_format_to_str(usec, ObString::make_string("%Y-%m-%d %T.%f"),
                                    buf, ObMockIteratorBuilder::MAX_DATA_LENGTH, pos);
  ASSERT_STREQ("2014-05-06 12:34:56.678900", buf);

  // flag and dml
  ASSERT_EQ(+RF_DELETE, row->flag_);
  ASSERT_EQ(T_DML_REPLACE, row->get_dml());

}

TEST_F(TestObMockIteratorBuilder, equals)
{
  ObMockStoreRowIterator iter1;
  const char *input1 =
      "int var num\n"
      "1   2   3\n";
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));

  ObMockStoreRowIterator iter2;
  const char *input2 =
      "int var num\n"
      "1   2   4\n";
  EXPECT_EQ(OB_SUCCESS, iter2.from(input2));
  EXPECT_FALSE(iter2.equals(iter1));

  ObMockStoreRowIterator iter3;
  const char *input3 =
      "int var num\n"
      "1   2   3\n";
  EXPECT_EQ(OB_SUCCESS, iter3.from(input3));
  // rewind two iter before call
  EXPECT_FALSE(iter3.equals(iter1));

  iter1.reset_iter();
  iter3.reset_iter();
  EXPECT_TRUE(iter3.equals(iter1));

  ObMockStoreRowIterator iter4;
  const char *input4 =
      "int var   num\n"
      "1   2     3\n"
      "2   'int' 4\n";
  EXPECT_EQ(OB_SUCCESS, iter4.from(input4));
  iter1.reset_iter();
  EXPECT_FALSE(iter4.equals(iter1));
}

TEST_F(TestObMockIteratorBuilder, test_new_row)
{
  ObMockNewRowIterator iter1;
  const char *input1 =
      "int var num\n"
      "1   2   3\n";
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));

  ObMockNewRowIterator iter2;
  const char *input2 =
      "int var num\n"
      "1   2   4\n";
  EXPECT_EQ(OB_SUCCESS, iter2.from(input2));
  EXPECT_FALSE(iter2.equals(iter1));
}

TEST_F(TestObMockIteratorBuilder, test_query_row)
{
  ObMockQueryRowIterator iter1;
  const char *input1 =
      "int var num\n"
      "1   2   3\n";
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));

  ObMockQueryRowIterator iter2;
  const char *input2 =
      "int var num\n"
      "1   2   4\n";
  EXPECT_EQ(OB_SUCCESS, iter2.from(input2));
  EXPECT_FALSE(iter2.equals(iter1));
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
