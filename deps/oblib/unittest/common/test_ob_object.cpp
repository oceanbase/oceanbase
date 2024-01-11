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

#include "common/object/ob_object.h"
#include <gtest/gtest.h>
#include <fstream>
#include <locale>
#include <codecvt>
#include <cassert>
#include "lib/utility/ob_test_util.h"
using namespace oceanbase::common;

class TestObObj: public ::testing::Test
{
public:
  TestObObj();
  virtual ~TestObObj();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObObj);
protected:
  // function members
  void test_obj_serialize(ObObj &obj1, ObObj &obj2, char* buff, int64_t buff_len);
  void parse_line(const char *line,  char *res, int &res_len);
  int compare_sortkey(const ObCollationType cs_type, const char *lhs, const int lhs_len, const char *rhs, const int rhs_len);
  void test_sortkey(const char *filename);
  void test_random_sortkey();
  void gen_random_unicode_string(const int len, char *res, int &real_len);
  int random_range(const int low, const int high);
  void print_obj(const ObObj &obj);
protected:
  // data members
};

TestObObj::TestObObj()
{
}

TestObObj::~TestObObj()
{
}

void TestObObj::SetUp()
{
}

void TestObObj::TearDown()
{
}

void TestObObj::parse_line(const char *line,  char *res, int &res_len)
{
  const int64_t len = strlen(line);
  int c = 0;
  int integer_nums = 0;
  int k = 0;
  for (int64_t i = 0; i < len; ++i) {
    if (0x20 == line[i]) {
      continue;
    } else {
      ++integer_nums;
      c = 16 * c + line[i] - '0';
      if (2 == integer_nums) {
        res[k++] = static_cast<char>(c);
        //fprintf(stdout, "%X ", res[k-1]);
        c = 0;
        integer_nums = 0;
      }
    }
  }
  res[k] = '\0';
  res_len = k;
  //fprintf(stdout, "\n");
}

void TestObObj::print_obj(const ObObj &obj)
{
  const int64_t len = obj.get_val_len();
  const char *ptr = obj.get_string_ptr();
  fprintf(stdout, "type=%d ", obj.get_collation_type());
  for (int64_t i = 0; i < len; ++i) {
    fprintf(stdout, "%X ", ptr[i]);
  }
  fprintf(stdout, "\n");
}

int TestObObj::compare_sortkey(const ObCollationType cs_type, const char *lhs, const int lhs_len, const char *rhs, const int rhs_len)
{
  int cmp = 0;
  ObObj obj1;
  ObObj obj2;
  ObObj obj3;
  ObObj obj4;
  bool is_valid_collation_free1 = false;
  bool is_valid_collation_free2 = false;
  ObArenaAllocator allocator;
  int ret1 = 0;
  int ret2 = 0;
  obj1.set_varchar("");
  obj1.set_varchar_value(lhs, lhs_len);
  obj2.set_varchar("");
  obj2.set_varchar_value(rhs, rhs_len);
  obj1.set_collation_type(cs_type);
  obj2.set_collation_type(cs_type);
  ret1 = obj1.compare(obj2, cs_type);
  obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator);
  obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator);
  if (is_valid_collation_free1 && is_valid_collation_free2) {
    ret2 = obj3.compare_collation_free(obj4);
    //fprintf(stdout, "use collation free\n");
  } else {
    ret2 = obj1.compare(obj2, cs_type);
    //fprintf(stdout, "use original\n");
  }
  //print_obj(obj3);
  //print_obj(obj4);
  //fprintf(stdout, "ret1=%d ret2=%d\n", ret1, ret2);
  if (0 == ret1) {
    if (0 != ret1) {
      cmp = -1;
    }
  } else if (ret1 < 0) {
    if (ret2 >= 0) {
      cmp = -1;
    }
  } else {
    if (ret2 <= 0) {
      cmp = -1;
    }
  }
  if (0 != cmp) {
    print_obj(obj1);
    print_obj(obj2);
    print_obj(obj3);
    print_obj(obj4);
    fprintf(stdout, "ret1=%d\n", ret1);
    fprintf(stdout, "ret2=%d\n", ret2);
    fprintf(stdout, "cs_type=%d\n", cs_type);
  }
  return cmp;
}

void TestObObj::test_sortkey(const char *filename)
{
  std::ifstream if_file(filename);
  std::string prev_line;
  std::string curr_line;
  char prev_line_res[255];
  char curr_line_res[255];
  int prev_len = 0;
  int curr_len = 0;
  int cmp = 0;
  std::getline(if_file, prev_line);
  while (std::getline(if_file, curr_line)) {
    parse_line(prev_line.c_str(), prev_line_res, prev_len);
    parse_line(curr_line.c_str(), curr_line_res, curr_len);
    cmp = compare_sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, prev_line_res, prev_len, curr_line_res, curr_len);
    ASSERT_EQ(0, cmp);
    prev_line = curr_line;
  }
}

int TestObObj::random_range(const int low, const int high)
{
  return std::rand() % (high - low) + low;
}

void TestObObj::gen_random_unicode_string(const int len, char *res, int &real_len)
{
  int i = 0;
  int unicode_point = 0;
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
  for (i = 0; i < len; ) {
    const int bytes = random_range(1, 7);
    if (bytes < 4) {
      unicode_point = random_range(0, 127);
    } else if (bytes < 6) {
      unicode_point = random_range(0xFF, 0xFFFF);
    } else if (bytes < 7) {
      unicode_point = random_range(0XFFFF, 0X10FFFF);
    }
    std::string utf_str = converter.to_bytes(unicode_point);
    //fprintf(stdout, "code_point=%d\n", unicode_point);
    //fprintf(stdout, "utf8_str=%s\n", utf_str.c_str());
    for (int j = 0; j < utf_str.size(); ++j) {
      res[i++] = utf_str[j];
    }
  }
  real_len = i;
}

void TestObObj::test_random_sortkey()
{
  static const int64_t TEST_RUN = 100000;
  char lhs_buf[255];
  char rhs_buf[255];
  int lhs_len = 0;
  int lhs_len_tmp = 0;
  int rhs_len = 0;
  int rhs_len_tmp = 0;
  std::srand(static_cast<unsigned int>(std::time(0)));
  int cmp = 0;
  for (int64_t i = 0; i < TEST_RUN; ++i) {
    //fprintf(stdout, "run=%ld\n", i);
    lhs_len_tmp = random_range(6, 8);
    rhs_len_tmp = random_range(6, 8);
    gen_random_unicode_string(lhs_len_tmp, lhs_buf, lhs_len);
    gen_random_unicode_string(rhs_len_tmp, rhs_buf, rhs_len);
    cmp = compare_sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, lhs_buf, lhs_len, rhs_buf, rhs_len);
    ASSERT_EQ(0, cmp);
    cmp = compare_sortkey(CS_TYPE_UTF8MB4_BIN, lhs_buf, lhs_len, rhs_buf, rhs_len);
    ASSERT_EQ(0, cmp);
  }
}

TEST_F(TestObObj, test_variable_sortkey)
{
  test_sortkey("CollationTest/CollationTest_NON_IGNORABLE_SHORT.txt");
  test_sortkey("CollationTest/CollationTest_SHIFTED_SHORT.txt");
}

TEST_F(TestObObj, test_random_sortkey)
{
  test_random_sortkey();
}

TEST_F(TestObObj, test_sizeof)
{
  ASSERT_EQ(16U, sizeof(ObObj));
}

TEST_F(TestObObj, test_null)
{
  ObObj obj1;
  obj1.set_null();
  ASSERT_TRUE(obj1.is_null());
  ASSERT_EQ(ObNullType, obj1.get_type());
}

TEST_F(TestObObj, test_int)
{
  ObObj obj1;
  obj1.set_int(-1);
  ASSERT_TRUE(obj1.is_int());
  ASSERT_EQ(ObIntType, obj1.get_type());
  ASSERT_EQ(-1, obj1.get_int());
  int64_t v = 0;
  OK(obj1.get_int(v));
  ASSERT_EQ(-1, v);
  ASSERT_TRUE(!obj1.is_zero());
  // compare
  ObObj obj2;
  obj2.set_int(2);
  ASSERT_TRUE(obj1 < obj2);
  // print
  char buffer[64];
  ASSERT_TRUE(0 < obj1.to_string(buffer, 64));
  ASSERT_STREQ("{\"BIGINT\":-1}", buffer);
  _OB_LOG(DEBUG, "%s", S(obj1));
  int64_t pos = 0;
  OK(obj1.print_sql_literal(buffer, 64, pos));
  _OB_LOG(DEBUG, "%s", buffer);
  ASSERT_STREQ("-1", buffer);
  int64_t cur_default_value_len = 0;
  OK(obj1.print_varchar_literal(buffer, 64, cur_default_value_len, NULL));
  _OB_LOG(DEBUG, "%s", buffer);
  ASSERT_STREQ("'-1'", buffer);
  obj1.dump();
  // deep copy
  ASSERT_FALSE(obj1.need_deep_copy());
  ASSERT_EQ(0, obj1.get_deep_copy_size());
  ObObj newobj;
  pos = 0;
  OK(newobj.deep_copy(obj1, buffer, 64, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(-1, newobj.get_int());
  ASSERT_EQ(8, obj1.get_data_length());
  // checksum
  // serialize & deserialize
  newobj.reset();
  pos = 0;
  OK(obj1.serialize(buffer, 64, pos));
  ASSERT_EQ(pos, obj1.get_serialize_size());
  pos = 0;
  OK(newobj.deserialize(buffer, 64, pos));
  ASSERT_EQ(-1, newobj.get_int());
  // is xxx
  ObObj zero;
  zero.set_int(0);
  ASSERT_TRUE(zero.is_zero());
}

TEST_F(TestObObj, test_varchar)
{
  ObObj obj1;
  obj1.set_varchar("abc");
  ASSERT_TRUE(obj1.is_varchar());
  ASSERT_EQ(ObVarcharType, obj1.get_type());
  ObString str1("abc");
  ASSERT_EQ(str1, obj1.get_varchar());
  ObString v;
  OK(obj1.get_varchar(v));
  ASSERT_EQ(str1, v);
  ASSERT_TRUE(!obj1.is_zero());
  // compare
  ObObj obj2;
  obj2.set_varchar("xyz");
  ASSERT_TRUE(obj1.compare(obj2, CS_TYPE_UTF8MB4_BIN) < 0);
  // print
  char buffer[64];
  ASSERT_TRUE(0 < obj1.to_string(buffer, 64));
  const char* expected_str = "{\"VARCHAR\":\"abc\", collation:\"invalid_type\"}";
  ASSERT_STREQ(expected_str, buffer);
  _OB_LOG(DEBUG, "%s", S(obj1));
  int64_t pos = 0;
  OK(obj1.print_sql_literal(buffer, 64, pos));
  _OB_LOG(DEBUG, "%s", buffer);
  ASSERT_STREQ("'abc'", buffer);
  int64_t cur_default_value_len = 0;
  OK(obj1.print_varchar_literal(buffer, 64, cur_default_value_len));
  _OB_LOG(DEBUG, "%s", buffer);
  ASSERT_STREQ("'abc'", buffer);
  obj1.dump();
  // deep copy
  ASSERT_TRUE(obj1.need_deep_copy());
  ASSERT_EQ(3, obj1.get_deep_copy_size());
  ObObj newobj;
  pos = 0;
  OK(newobj.deep_copy(obj1, buffer, 64, pos));
  ASSERT_EQ(3, pos);
  ASSERT_EQ(str1, newobj.get_varchar());
  ASSERT_EQ(3, obj1.get_data_length());
  // checksum
  // serialize & deserialize
  newobj.reset();
  pos = 0;
  OK(obj1.serialize(buffer, 64, pos));
  ASSERT_EQ(pos, obj1.get_serialize_size());
  pos = 0;
  OK(newobj.deserialize(buffer, 64, pos));
  ASSERT_EQ(str1, newobj.get_varchar());
}

TEST_F(TestObObj, test_collation)
{
  ObObj obj1;
  ObObj obj2;
  obj1.set_varchar("abc");
  obj2.set_varchar("ABC");
  ASSERT_TRUE(0 != obj1.compare(obj2, CS_TYPE_UTF8MB4_BIN));
  ASSERT_TRUE(0 == obj1.compare(obj2, CS_TYPE_UTF8MB4_GENERAL_CI));
  obj1.set_varchar("ß");
  obj2.set_varchar("s");
  ASSERT_TRUE(0 != obj1.compare(obj2, CS_TYPE_UTF8MB4_BIN));
  ASSERT_TRUE(0 == obj1.compare(obj2, CS_TYPE_UTF8MB4_GENERAL_CI));
  obj1.set_varchar("aßbssß");
  obj2.set_varchar("asbßsß");
  ASSERT_TRUE(0 != obj1.compare(obj2, CS_TYPE_UTF8MB4_BIN));
  ASSERT_TRUE(0 == obj1.compare(obj2, CS_TYPE_UTF8MB4_GENERAL_CI));
}

TEST_F(TestObObj, test_to_collation_free_obj)
{
  ObObj obj1;
  ObObj obj2;
  ObObj obj3;
  ObObj obj4;
  bool is_valid_collation_free1 = false;
  bool is_valid_collation_free2 = false;
  ObArenaAllocator allocator;
  obj1.set_varchar("abc");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_varchar("ABC");
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));

  obj1.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 != obj3.compare_collation_free(obj4));

  obj1.set_varchar("ß");
  obj2.set_varchar("s");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));

  obj1.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 != obj3.compare_collation_free(obj4));

  obj1.set_varchar("aßbssß");
  obj2.set_varchar("asbßsß");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));

  obj1.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 != obj3.compare_collation_free(obj4));

  // NULL case
  obj1.set_varchar("");
  obj2.set_varchar("");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  fprintf(stdout, "len=%d,str=%p\n", obj1.get_val_len(), obj1.get_string_ptr());
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));

  obj1.set_varchar("");
  obj2.set_varchar(" 01");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  fprintf(stdout, "len=%d,str=%p\n", obj1.get_val_len(), obj1.get_string_ptr());
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 != obj3.compare_collation_free(obj4));

  // \0 case
  obj1.set_varchar("");
  obj2.set_varchar("\0");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  fprintf(stdout, "len=%d,str=%p\n", obj1.get_val_len(), obj1.get_string_ptr());
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));

  obj1.set_varchar("124");
  obj2.set_varchar("\0");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  fprintf(stdout, "len=%d,str=%p\n", obj1.get_val_len(), obj1.get_string_ptr());
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 != obj3.compare_collation_free(obj4));

  // The current version, no special settings, will automatically ignore the trailing spaces
  obj1.set_varchar("abD");
  obj2.set_varchar("abD ");
  obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  obj2.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, obj1.to_collation_free_obj(obj3, is_valid_collation_free1, allocator));
  ASSERT_EQ(OB_SUCCESS, obj2.to_collation_free_obj(obj4, is_valid_collation_free2, allocator));
  ASSERT_TRUE(0 == obj3.compare_collation_free(obj4));
}

TEST_F(TestObObj, test_compare_collation_free_mysqlmode)
{
  // mysql mode
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ObObj obj1;
  ObObj obj2;
  char aa[10];
  char bb[10];
  // Test the size of a\0\0 and a
  aa[0] = 'a';
  aa[1] = '\0';
  aa[2] = '\0';
  bb[0] = 'a';
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value(aa, 3);
  obj2.set_varchar_value(bb, 1);
  int cmp = obj1.compare_collation_free(obj2);
  fprintf(stdout, "%d\n", cmp);
  ASSERT_TRUE(cmp < 0);

  // Test the size of a\0\0 and a\0\0\0
  aa[0] = 'a';
  aa[1] = '\0';
  aa[2] = '\0';
  bb[0] = 'a';
  bb[1] = '\0';
  bb[2] = '\0';
  bb[3] = '\0';
  obj1.set_varchar_value(aa, 3);
  obj2.set_varchar_value(bb, 4);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // Test the size of a\1 and a
  aa[0] = 'a';
  aa[1] = char(1);
  bb[0] = 'a';
  obj1.set_varchar_value(aa, 2);
  obj2.set_varchar_value(bb, 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // Test the size of a\1 and a\0
  aa[0] = 'a';
  aa[1] = char(1);
  bb[0] = 'a';
  bb[1] = '\0';
  obj1.set_varchar_value(aa, 2);
  obj2.set_varchar_value(bb, 2);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // The current version, no special settings, will automatically ignore the trailing spaces
  obj1.set_varchar_value("abD", 3);
  obj2.set_varchar_value("abD ", 4);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(0 == cmp);

  // test NULL and varchar
  obj1.set_null();
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test varchar and null
  obj2.set_null();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test NULL and NULL
  obj1.set_null();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(0 == cmp);

  // test min and varchar
  obj1.set_min_value();
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test varchar and min
  obj2.set_min_value();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test max and varchar
  obj1.set_max_value();
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test varchar and max
  obj2.set_max_value();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test min and null
  obj1.set_min_value();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test null and min
  obj1.set_null();
  obj2.set_min_value();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test max and null
  obj1.set_max_value();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test null and max
  obj1.set_null();
  obj2.set_max_value();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);
}

TEST_F(TestObObj, test_compare_collation_free_oraclemode)
{
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  ASSERT_TRUE(oceanbase::lib::is_oracle_mode());
  ObObj obj1;
  ObObj obj2;
  char aa[10];
  char bb[10];
  // Test the size of a\0\0 and a
  aa[0] = 'a';
  aa[1] = '\0';
  aa[2] = '\0';
  bb[0] = 'a';
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value(aa, 3);
  obj2.set_varchar_value(bb, 1);
  int cmp = obj1.compare_collation_free(obj2);
  fprintf(stdout, "%d\n", cmp);
  ASSERT_TRUE(cmp > 0);

  // Test the size of a\0\0 and a\0\0\0
  aa[0] = 'a';
  aa[1] = '\0';
  aa[2] = '\0';
  bb[0] = 'a';
  bb[1] = '\0';
  bb[2] = '\0';
  bb[3] = '\0';
  obj1.set_varchar_value(aa, 3);
  obj2.set_varchar_value(bb, 4);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp <  0);

  // Test the size of a\1 and a
  aa[0] = 'a';
  aa[1] = char(1);
  bb[0] = 'a';
  obj1.set_varchar_value(aa, 2);
  obj2.set_varchar_value(bb, 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // Test the size of a\1 and a\0
  aa[0] = 'a';
  aa[1] = char(1);
  bb[0] = 'a';
  bb[1] = '\0';
  obj1.set_varchar_value(aa, 2);
  obj2.set_varchar_value(bb, 2);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // Under oracle varchar, the ones with spaces at the end are bigger 
  obj1.set_varchar_value("abD", 3);
  obj2.set_varchar_value("abD ", 4);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test NULL and varchar
  obj1.set_null();
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test varchar and null
  obj2.set_null();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test NULL and NULL
  obj1.set_null();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(0 == cmp);

  // test min and varchar
  obj1.set_min_value();
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test varchar and min
  obj2.set_min_value();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test max and varchar
  obj1.set_max_value();
  obj2.meta_.set_varchar();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test varchar and max
  obj2.set_max_value();
  obj1.meta_.set_varchar();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_varchar_value("0", 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test min and null
  obj1.set_min_value();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test null and min
  obj1.set_null();
  obj2.set_min_value();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test max and null
  obj1.set_max_value();
  obj2.set_null();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp > 0);

  // test null and max
  obj1.set_null();
  obj2.set_max_value();
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

  // test oracle char
  // Test the size of a space and a
  aa[0] = 'a';
  aa[1] = ' ';
  bb[0] = 'a';
  obj1.meta_.set_char();
  obj1.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj2.meta_.set_char();
  obj2.set_collation_type(CS_TYPE_COLLATION_FREE);
  obj1.set_char_value(aa, 2);
  obj2.set_char_value(bb, 1);
  cmp = obj1.compare_collation_free(obj2);
  fprintf(stdout, "%d\n", cmp);
  ASSERT_TRUE(cmp == 0);

  // Test the size of char a\1 and a
  aa[0] = 'a';
  aa[1] = char(1);
  bb[0] = 'a';
  obj1.set_char_value(aa, 2);
  obj2.set_char_value(bb, 1);
  cmp = obj1.compare_collation_free(obj2);
  ASSERT_TRUE(cmp < 0);

}

void TestObObj::test_obj_serialize(ObObj &obj1, ObObj &obj2, char* buff, int64_t buff_len)
{
  int64_t len = obj1.get_serialize_size();
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, obj1.serialize(buff, buff_len, pos));
  ASSERT_EQ(len, pos);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, obj2.deserialize(buff, len, pos));
  ASSERT_EQ(obj1, obj2);
}

TEST_F(TestObObj, test_serialize)
{
  ObObj obj1;
  ObObj obj2;
  static const int64_t MY_BUF_SIZE = 1024;
  char buff[MY_BUF_SIZE];
  // 1. varchar
  const char * cases[] =
      {
        "",
        "127.0.0.1:55412;127.0.0.2:55412"
      };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    obj1.set_varchar(cases[i]);
    obj1.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    test_obj_serialize(obj1, obj2, buff, MY_BUF_SIZE);
  }
  // 2. null
  obj1.set_null();
  test_obj_serialize(obj1, obj2, buff, MY_BUF_SIZE);
  // 3. null
  obj1.set_int(-1);
  test_obj_serialize(obj1, obj2, buff, MY_BUF_SIZE);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
