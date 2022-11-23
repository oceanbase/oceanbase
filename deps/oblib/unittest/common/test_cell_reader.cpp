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

#include <gtest/gtest.h>
#include "common/cell/ob_cell_writer.h"
#include "common/cell/ob_cell_reader.h"
#include "common/object/ob_object.h"
#include "lib/number/ob_number_v2.h"
namespace oceanbase
{
using namespace common;
using namespace number;
namespace unittest
{
class TestCellReader : public ::testing::Test
{
public:
  TestCellReader();
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  void alloc(char *&ptr, const int64_t size);
  ModuleArena *get_arena() { return &arena_; }
private:
  ModulePageAllocator alloc_;
  ModuleArena arena_;
};
TestCellReader::TestCellReader()
  :alloc_(ObModIds::TEST),
   arena_(ModuleArena::DEFAULT_BIG_PAGE_SIZE, alloc_)
{
}
void TestCellReader::alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(arena_.alloc(size));
  ASSERT_TRUE(NULL != ptr);
}
TEST_F(TestCellReader, test_null)
{
  int ret = OB_SUCCESS;
  ObObj w_obj;
  const ObObj *r_obj = NULL;
  w_obj.set_null();
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  w_obj.set_null();
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append(w_obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.next_cell();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.get_cell(r_obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(w_obj == *r_obj);
}

TEST_F(TestCellReader, test_int)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[10];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[0].set_tinyint(100);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[1].set_utinyint(101);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[2].set_smallint(10000);
  ret = writer.append(w_obj[2]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[3].set_usmallint(10001);
  ret = writer.append(w_obj[3]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[4].set_mediumint(1000000);
  ret = writer.append(w_obj[4]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[5].set_umediumint(1000001);
  ret = writer.append(w_obj[5]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[6].set_int32(1000000000);
  ret = writer.append(w_obj[6]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[7].set_uint32(1000000001);
  ret = writer.append(w_obj[7]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[8].set_int(1000000000000000000);
  ret = writer.append(w_obj[8]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[9].set_uint64(1000000000000000001);
  ret = writer.append(w_obj[9]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 10; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_float_double)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[4];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[0].set_float(static_cast<float>(1.1));
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[1].set_ufloat(static_cast<float>(2.2));
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[2].set_double(3.3);
  ret = writer.append(w_obj[2]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[3].set_double(4.4);
  ret = writer.append(w_obj[3]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 4; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_number)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[2];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObNumber number;
  char *buf = NULL;
  alloc(buf, 1024);
  ModuleArena *arena = get_arena();
  sprintf(buf, "100");
  ret = number.from(buf, *arena);
  ASSERT_EQ(OB_SUCCESS, ret);
  w_obj[0].set_number(number);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  sprintf(buf, "10000");
  ret = number.from(buf, *arena);
  ASSERT_EQ(OB_SUCCESS, ret);
  w_obj[1].set_unumber(number);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 2; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_time)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[5];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[0].set_datetime(1);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[1].set_timestamp(2);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[2].set_date(3);
  ret = writer.append(w_obj[2]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[3].set_time(4);
  ret = writer.append(w_obj[3]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[4].set_year(5);
  ret = writer.append(w_obj[4]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 5; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_char)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[2];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObNumber number;
  char *buf1 = NULL;
  alloc(buf1, 1024);
  char *buf2 = NULL;
  alloc(buf2, 1024);
  ObString str;

  sprintf(buf1, "11");
  str.assign_ptr(buf1, 2);
  w_obj[0].set_varchar(str);
  w_obj[0].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  sprintf(buf2, "22");
  str.assign_ptr(buf2, 2);
  w_obj[1].set_char(str);
  w_obj[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 2; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_binary)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[2];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObNumber number;
  char *buf1 = NULL;
  alloc(buf1, 1024);
  char *buf2 = NULL;
  alloc(buf2, 1024);
  ObString str;

  sprintf(buf1, "11");
  str.assign_ptr(buf1, 2);
  w_obj[0].set_varbinary(str);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  sprintf(buf2, "22");
  str.assign_ptr(buf2, 2);
  w_obj[1].set_binary(str);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 2; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_extend)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[6];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[0].set_ext(ObActionFlag::OP_END_FLAG);
  ret = writer.append(w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[1].set_ext(ObActionFlag::OP_NOP);
  ret = writer.append(w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[2].set_ext(ObActionFlag::OP_DEL_ROW);
  ret = writer.append(w_obj[2]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[3].set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  ret = writer.append(w_obj[3]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[4].set_min_value();
  ret = writer.append(w_obj[4]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[5].set_max_value();
  ret = writer.append(w_obj[5]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, DENSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 6; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
  }
}

TEST_F(TestCellReader, test_column)
{
  int ret = OB_SUCCESS;
  ObObj w_obj[2];
  const ObObj *r_obj = NULL;
  char *w_buf = NULL;
  alloc(w_buf, 1024);
  ObCellWriter writer;
  ObCellReader reader;
  ret = writer.init(w_buf, 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[0].set_int(100);
  ret = writer.append(1, w_obj[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  w_obj[1].set_int(101);
  ret = writer.append(2, w_obj[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.init(w_buf, 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  uint64_t column_id = 0;
  for (int64_t i = 0; i < 2; ++ i) {
    ret = reader.next_cell();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = reader.get_cell(column_id, r_obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(w_obj[i] == *r_obj);
    ASSERT_EQ(i + 1, static_cast<int64_t>(column_id));
  }
}
}//end namespace unittest
}//end namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
