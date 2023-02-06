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
#include "common/object/ob_object.h"
#include "lib/number/ob_number_v2.h"
namespace oceanbase
{
using namespace common;
using namespace number;
namespace unittest
{
class TestCellWriter : public ::testing::Test
{
public:
  TestCellWriter();
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  void alloc(char *&ptr, const int64_t size);
  void check_meta(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr);
  void check_int(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const int64_t value);
  void check_double(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const double value);
  void check_number(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      ObObj &obj);
  void check_time(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const int64_t value);
  void check_char(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const int64_t value,
      const uint8_t c_type);
  void check_binary(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const int64_t value);
  void check_extend(
      const char *buf,
      const int64_t pos,
      const int64_t type,
      const int64_t attr,
      const int64_t value);
  void check_column_id(
      const char *buf,
      const int64_t pos,
      const uint32_t column_id);

  ModuleArena *get_arena() { return &arena_; }
private:
  ModulePageAllocator alloc_;
  ModuleArena arena_;
};
TestCellWriter::TestCellWriter()
  :alloc_(ObModIds::TEST),
   arena_(ModuleArena::DEFAULT_BIG_PAGE_SIZE, alloc_)
{
}
void TestCellWriter::alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(arena_.alloc(size));
  ASSERT_TRUE(NULL != ptr);
}
void TestCellWriter::check_meta(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
}
void TestCellWriter::check_int(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const int64_t value)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  switch(meta->attr_) {
    case 0: {
        const int8_t *tmp = reinterpret_cast<const int8_t*>(buf + pos + 1);
        ASSERT_EQ(*tmp, value);
        break;
      }
    case 1: {
        const int16_t *tmp = reinterpret_cast<const int16_t*>(buf + pos + 1);
        ASSERT_EQ(*tmp, value);
        break;
      }
    case 2: {
        const int32_t *tmp = reinterpret_cast<const int32_t*>(buf + pos + 1);
        ASSERT_EQ(*tmp, value);
        break;
      }
    case 3: {
        const int64_t *tmp = reinterpret_cast<const int64_t*>(buf + pos + 1);
        ASSERT_EQ(*tmp, value);
        break;
      }
    default:
      COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid attr.");
  }
}

void TestCellWriter::check_double(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const double value)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  switch (meta->type_) {
    case ObFloatType:
    case ObUFloatType:  {
      const float *tmp = reinterpret_cast<const float*>(buf + pos + 1);
      ASSERT_EQ(*tmp, static_cast<float>(value));
      break;
    }
    case ObDoubleType:
    case ObUDoubleType:{
      const double *tmp = reinterpret_cast<const double*>(buf + pos + 1);
      ASSERT_EQ(*tmp, value);
      break;
    }
  }
}

void TestCellWriter::check_time(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const int64_t value)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  switch (meta->type_) {
    case ObDateTimeType:
    case ObTimestampType:
    case ObTimeType: {
      const int64_t *tmp = reinterpret_cast<const int64_t*>(buf + pos + 1);
      ASSERT_EQ(*tmp, static_cast<int64_t>(value));
      break;
    }
    case ObDateType: {
      const int32_t *tmp = reinterpret_cast<const int32_t*>(buf + pos + 1);
      ASSERT_EQ(*tmp, static_cast<int32_t>(value));
      break;
    }
    case ObYearType: {
      const uint8_t *tmp = reinterpret_cast<const uint8_t*>(buf + pos + 1);
      ASSERT_EQ(*tmp, static_cast<uint8_t>(value));
      break;
    }
  }
}

void TestCellWriter::check_char(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const int64_t value,
    const uint8_t c_type)
{
  int ret = OB_SUCCESS;
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  char *tmp_buf = NULL;
  alloc(tmp_buf, 1024);
  const int32_t *ptr1 = NULL;
  const uint8_t *ptr2 = NULL;
  const char *ptr3 = NULL;
  switch (meta->attr_) {
    case 0:
      sprintf(tmp_buf, "%ld", value);
      ptr1 = reinterpret_cast<const int32_t*>(buf + pos + 1);
      ASSERT_EQ(*ptr1, static_cast<int32_t>(strlen(tmp_buf)));
      ptr3 = reinterpret_cast<const char*>(buf + pos + 5);
      ret = memcmp(ptr3, tmp_buf, strlen(tmp_buf));
      ASSERT_EQ(0, ret);
      break;
   case 1:
      sprintf(tmp_buf, "%ld", value);
      ptr2 = reinterpret_cast<const uint8_t*>(buf + pos + 1);
      ASSERT_EQ(*ptr2, c_type);
      ptr1 = reinterpret_cast<const int32_t*>(buf + pos + 2);
      ASSERT_EQ(*ptr1, static_cast<int32_t>(strlen(tmp_buf)));
      ptr3 = reinterpret_cast<const char*>(buf + pos + 6);
      ret = memcmp(ptr3, tmp_buf, strlen(tmp_buf));
      ASSERT_EQ(0, ret);
      break;
  }
}

void TestCellWriter::check_binary(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const int64_t value)
{
  int ret = OB_SUCCESS;
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  char *tmp_buf = NULL;
  alloc(tmp_buf, 1024);
  const int32_t *ptr1 = NULL;
  const char *ptr3 = NULL;

  sprintf(tmp_buf, "%ld", value);
  ptr1 = reinterpret_cast<const int32_t*>(buf + pos + 1);
  ASSERT_EQ(*ptr1, static_cast<int32_t>(strlen(tmp_buf)));
  ptr3 = reinterpret_cast<const char*>(buf + pos + 5);
  ret = memcmp(ptr3, tmp_buf, strlen(tmp_buf));
  ASSERT_EQ(0, ret);
}

void TestCellWriter::check_number(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    ObObj &obj)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  ObObj check_obj;
  ObNumber check_value;
  const uint32_t *desc = reinterpret_cast<const uint32_t*>(buf + pos + 1);
  ObNumber::Desc tmp_desc;
  tmp_desc.desc_ = *desc;
  tmp_desc.reserved_ = 0;
  check_value.assign(tmp_desc.desc_,
      ((0 == tmp_desc.len_) ? NULL : (uint32_t*)(buf + pos + 5)));
  if (ObNumberType == meta->type_) {
    check_obj.set_number(check_value);
  } else if (ObUNumberType == meta->type_) {
    check_obj.set_unumber(check_value);
  }
  ASSERT_TRUE(obj == check_obj);
}

void TestCellWriter::check_extend(
    const char *buf,
    const int64_t pos,
    const int64_t type,
    const int64_t attr,
    const int64_t value)
{
  const ObCellWriter::CellMeta *meta
    = reinterpret_cast<const ObCellWriter::CellMeta*>(buf + pos);
  ASSERT_EQ(type, meta->type_);
  ASSERT_EQ(attr, meta->attr_);
  const int8_t *ptr = NULL;
  switch (meta->attr_) {
    case 0:
      break;
    case 1:
      ptr = reinterpret_cast<const int8_t*>(buf + pos + 1);
      ASSERT_EQ(value, *ptr);
      break;
  }
}

void TestCellWriter::check_column_id(
    const char *buf,
    const int64_t pos,
    const uint32_t column_id)
{
  const uint32_t *ptr = reinterpret_cast<const uint32_t*>(buf + pos);
  ASSERT_EQ(column_id, *ptr);
}

TEST_F(TestCellWriter, test_null)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObObj obj;
  obj.set_null();
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(1, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(1, cell_cnt);
  check_meta(buf, 0, ObNullType, 0);
}

TEST_F(TestCellWriter, test_int)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;

  obj.set_tinyint(100);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_utinyint(101);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_smallint(10000);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_usmallint(10001);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_mediumint(1000000);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_umediumint(1000001);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_int32(1000000000);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_uint32(1000000001);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_int(1000000000000000000);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_uint64(1000000000000000001);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(48, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(10, cell_cnt);
  check_int(buf, 0, ObTinyIntType, 0, 100);
  check_int(buf, 2, ObUTinyIntType, 0, 101);
  check_int(buf, 4, ObSmallIntType, 1, 10000);
  check_int(buf, 7, ObUSmallIntType, 1, 10001);
  check_int(buf, 10, ObMediumIntType, 2, 1000000);
  check_int(buf, 15, ObUMediumIntType, 2, 1000001);
  check_int(buf, 20, ObInt32Type, 2, 1000000000);
  check_int(buf, 25, ObUInt32Type, 2, 1000000001);
  check_int(buf, 30, ObIntType, 3, 1000000000000000000);
  check_int(buf, 39, ObUInt64Type, 3, 1000000000000000001);
}

TEST_F(TestCellWriter, test_float_double)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;

  obj.set_float(static_cast<float>(1.1));
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_ufloat(static_cast<float>(2.2));
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_double(3.3);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj.set_udouble(4.4);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(28, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(4, cell_cnt);

  check_double(buf, 0, ObFloatType, 0, 1.1);
  check_double(buf, 5, ObUFloatType, 0, 2.2);
  check_double(buf, 10, ObDoubleType, 0, 3.3);
  check_double(buf, 19, ObUDoubleType, 0, 4.4);
}

TEST_F(TestCellWriter, test_number)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj1;
  ObObj obj2;

  ObNumber number1;
  ObNumber number2;
  char *buf1 = NULL;
  char *buf2 = NULL;
  alloc(buf1, 1024);
  alloc(buf2, 1024);

  ModuleArena *arena = get_arena();
  sprintf(buf1, "100");
  ret = number1.from(buf1, *arena);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj1.set_number(number1);
  ret = writer.append(obj1);
  ASSERT_EQ(OB_SUCCESS, ret);

  sprintf(buf2, "10000");
  ret = number2.from(buf2, *arena);
  ASSERT_EQ(OB_SUCCESS, ret);
  obj2.set_unumber(number2);
  ret = writer.append(obj2);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(18, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(2, cell_cnt);
  check_number(buf, 0, ObNumberType, 0, obj1);
  check_number(buf, 9, ObUNumberType, 0, obj2);
}


TEST_F(TestCellWriter, test_time)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;

  obj.set_datetime(1);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_timestamp(2);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_date(3);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_time(4);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_year(5);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(34, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(5, cell_cnt);

  check_time(buf, 0, ObDateTimeType, 0, 1);
  check_time(buf, 9, ObTimestampType, 0, 2);
  check_time(buf, 18, ObDateType, 0, 3);
  check_time(buf, 23, ObTimeType, 0, 4);
  check_time(buf, 32, ObYearType, 0, 5);
}

TEST_F(TestCellWriter, test_char)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;
  char *tmp_buf = NULL;
  alloc(tmp_buf, 1024);
  ObString str;

  sprintf(tmp_buf, "11");
  str.assign_ptr(tmp_buf, 2);
  obj.set_varchar(str);
  obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  sprintf(tmp_buf, "22");
  str.assign_ptr(tmp_buf, 2);
  obj.set_char(str);
  obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(14, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(2, cell_cnt);

  check_char(buf, 0, ObVarcharType, 0, 11, CS_TYPE_UTF8MB4_GENERAL_CI);
  check_char(buf, 7, ObCharType, 0, 22, CS_TYPE_UTF8MB4_GENERAL_CI);
}

TEST_F(TestCellWriter, test_extend)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;
  char *tmp_buf = NULL;
  alloc(tmp_buf, 1024);
  ObString str;

  obj.set_ext(ObActionFlag::OP_END_FLAG);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_ext(ObActionFlag::OP_NOP);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_ext(ObActionFlag::OP_DEL_ROW);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_min_value();
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  obj.set_max_value();
  ret = writer.append(obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(11, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(6, cell_cnt);

  check_extend(buf, 0, ObExtendType, 0, 0);
  check_extend(buf, 1, ObExtendType, 1, 0);
  check_extend(buf, 3, ObExtendType, 1, 1);
  check_extend(buf, 5, ObExtendType, 1, 2);
  check_extend(buf, 7, ObExtendType, 1, 3);
  check_extend(buf, 9, ObExtendType, 1, 4);
}

TEST_F(TestCellWriter, test_append_column)
{
  int ret = OB_SUCCESS;
  char *write_buf = NULL;
  alloc(write_buf, 2 * 1024 * 1024);
  ObCellWriter writer;
  ret = writer.init(write_buf, 2 * 1024 * 1024, SPARSE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObObj obj;
  obj.set_int(1);
  char *tmp_buf = NULL;
  alloc(tmp_buf, 1024);

  ret = writer.append(5, obj);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = writer.get_buf();
  ASSERT_TRUE(NULL != buf);
  int64_t size = writer.size();
  ASSERT_EQ(6, size);
  int64_t cell_cnt = writer.get_cell_cnt();
  ASSERT_EQ(1, cell_cnt);

  check_int(buf, 0, ObIntType, 0, 1);
  check_column_id(buf, 2, 5);
}
}//end namespace unittest
}//end namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
