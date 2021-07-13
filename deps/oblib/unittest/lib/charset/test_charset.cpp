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

#include <pthread.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <codecvt>
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "gtest/gtest.h"
#include <iostream>
#include <fstream>

using namespace oceanbase::common;

#define CUR_RESULT_FILE_SUFFIX ".record"
#define STD_RESULT_FILE_SUFFIX ".result"

class TestCharset : public ::testing::Test {
public:
  TestCharset();
  virtual ~TestCharset();
  virtual void SetUp();
  virtual void TearDown();
  template <typename func>
  void for_each_utf8(func handle);

protected:
  void gen_random_unicode_string(const int len, char* res, int& real_len);
  int random_range(const int low, const int high);
};

TestCharset::TestCharset()
{}

TestCharset::~TestCharset()
{}

void TestCharset::SetUp()
{
  srand((unsigned)time(NULL));
}

void TestCharset::TearDown()
{}

int TestCharset::random_range(const int low, const int high)
{
  return std::rand() % (high - low) + low;
}

void TestCharset::gen_random_unicode_string(const int len, char* res, int& real_len)
{
  int i = 0;
  int unicode_point = 0;
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
  for (i = 0; i < len;) {
    const int bytes = random_range(1, 7);
    if (bytes < 4) {
      unicode_point = random_range(0, 127);
    } else if (bytes < 6) {
      unicode_point = random_range(0xFF, 0xFFFF);
    } else if (bytes < 7) {
      unicode_point = random_range(0XFFFF, 0X10FFFF);
    }
    std::string utf_str = converter.to_bytes(unicode_point);
    // fprintf(stdout, "code_point=%d\n", unicode_point);
    // fprintf(stdout, "utf8_str=%s\n", utf_str.c_str());
    for (int j = 0; j < utf_str.size(); ++j) {
      res[i++] = utf_str[j];
    }
  }
  real_len = i;
}

TEST_F(TestCharset, strcmp)
{
  ObString a;
  ObString b;
  int ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, a.ptr(), a.length(), b.ptr(), b.length());
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_EQ(0, ret);
  char aa[10] = "abd";
  char bb[10] = "aBd ";
  char cc[10] = " aBd";
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, aa, 3, bb, 4);
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_EQ(-1, ret);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, aa, 3, cc, 4);
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_EQ(1, ret);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_BIN, aa, 3, bb, 4);
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_TRUE(ret > 0);
  ObString c(aa);
  ObString d(bb);
  fprintf(stdout, "c:%.*s\n", c.length(), c.ptr());
  fprintf(stdout, "d:%.*s\n", d.length(), d.ptr());
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, c, d);
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_EQ(-1, ret);
  fprintf(stdout, "ret:%d\n", ret);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_BIN, c, d);
  fprintf(stdout, "ret:%d\n", ret);
  ASSERT_TRUE(ret > 0);
  ObString empty;
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, empty, d);
  ASSERT_EQ(-1, ret);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, d, empty);
  ASSERT_EQ(1, ret);
  ObString empty1;
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, empty1, empty);
  ASSERT_EQ(0, ret);
  ret = ObCharset::strcmp(CS_TYPE_UTF8MB4_BIN, empty1, empty);
  ASSERT_EQ(0, ret);
}

TEST_F(TestCharset, sortkey)
{
  char aa[10] = "abc";
  char aa1[10];
  char bb[10] = "abc ";
  char bb1[10];
  bool is_valid_unicode = false;
  size_t size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, aa, strlen(aa), aa1, 10, is_valid_unicode);
  size_t size2 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, bb, strlen(bb), bb1, 10, is_valid_unicode);
  ASSERT_NE(size1, size2);
  ASSERT_TRUE(is_valid_unicode);

  char space[10] = "  ";
  size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, space, strlen(space), aa1, 10, is_valid_unicode);
  ASSERT_EQ(size1, 2);
  ASSERT_TRUE(is_valid_unicode);

  char empty[10] = "";
  size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, empty, strlen(empty), aa1, 10, is_valid_unicode);
  ASSERT_EQ(size1, 0);
  ASSERT_TRUE(is_valid_unicode);

  char invalid[10];
  invalid[0] = char(0x10);
  invalid[1] = char(0x80);
  invalid[2] = '\0';
  size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, invalid, strlen(invalid), aa1, 10, is_valid_unicode);
  ASSERT_EQ(size1, 1);
  ASSERT_FALSE(is_valid_unicode);

  // The parameter of sortkey cannot be NULL
  // char *p = NULL;
  // size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, true, p, 0, aa1, 10);
}

TEST_F(TestCharset, casedn)
{
  char a1[14] = "Variable_name";
  char a2[14] = "Variable_NAME";
  char a3[14] = "variable_name";
  ObString y1;
  ObString y2;
  ObString y3;
  a1[13] = '1';
  a2[13] = '1';
  a3[13] = '1';
  y1.assign_ptr(a1, 14);
  y2.assign_ptr(a2, 14);
  y3.assign_ptr(a3, 14);
  fprintf(stdout, "ret:%p, %d\n", y1.ptr(), y1.length());
  size_t size1 = ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, y1);
  EXPECT_TRUE(y1 == y3);
  size_t size2 = ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, y2);
  fprintf(stdout, "y1:%.*s, y2:%.*s, y3:%.*s\n", y1.length(), y1.ptr(), y2.length(), y2.ptr(), y3.length(), y3.ptr());
  EXPECT_TRUE(y2 == y3);
  ASSERT_EQ(y1.length(), 14);
  ASSERT_EQ(y2.length(), 14);
  ASSERT_EQ(size1, 14);
  ASSERT_EQ(size2, 14);
}

TEST_F(TestCharset, case_insensitive_equal)
{
  ObString y1 = "Variable_name";
  ObString y2 = "variable_name";
  ObString y3 = "variable_name1";
  ObString y4 = "variable_name1";
  bool yy = ObCharset::case_insensitive_equal(y1, y2, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_TRUE(yy);
  yy = ObCharset::case_insensitive_equal(y2, y3, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_FALSE(yy);
  yy = ObCharset::case_insensitive_equal(y3, y4, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_TRUE(yy);
}

TEST_F(TestCharset, hash_sort)
{
  ObString s;
  uint64_t ret = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, s.ptr(), s.length(), 0);
  const char* a = "abd";
  const char* b = "aBD";
  uint64_t ret1 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, a, 3, 0, NULL);
  uint64_t ret2 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, b, 3, 0, NULL);
  fprintf(stdout, "ret:%lu, ret1:%lu, ret2:%lu\n", ret, ret1, ret2);
  uint64_t ret3 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, ObString::make_string(b));
  ASSERT_EQ(ret2, ret3);
}

TEST_F(TestCharset, case_mode_equal)
{
  ObString y1 = "Variable_name";
  ObString y2 = "variable_name";
  ObString y3 = "variable_name1";
  ObString y4 = "variable_name1";
  bool is_equal = false;
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_SENSITIVE, y1, y2);
  ASSERT_FALSE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_SENSITIVE, y1, y1);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_SENSITIVE, y3, y4);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_SENSITIVE, y1, y3);
  ASSERT_FALSE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_INSENSITIVE, y1, y2);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_INSENSITIVE, y1, y1);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_INSENSITIVE, y3, y4);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_ORIGIN_AND_INSENSITIVE, y1, y3);
  ASSERT_FALSE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_LOWERCASE_AND_INSENSITIVE, y1, y2);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_LOWERCASE_AND_INSENSITIVE, y1, y1);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_LOWERCASE_AND_INSENSITIVE, y3, y4);
  ASSERT_TRUE(is_equal);
  is_equal = ObCharset::case_mode_equal(OB_LOWERCASE_AND_INSENSITIVE, y1, y3);
  ASSERT_FALSE(is_equal);
}

TEST_F(TestCharset, well_formed_length)
{
  int ret = OB_SUCCESS;
  const char* str = "\0123";
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  int64_t well_formed_length = 0;
  int64_t str_len = 1;

  ret = ObCharset::well_formed_len(cs_type, str, str_len, well_formed_length);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(1 == well_formed_length);
  ret = ObCharset::well_formed_len(cs_type, str, 0, well_formed_length);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(0 == well_formed_length);
  ret = ObCharset::well_formed_len(cs_type, NULL, 0, well_formed_length);
  ASSERT_TRUE(OB_SUCC(ret));
  ASSERT_TRUE(0 == well_formed_length);
  ret = ObCharset::well_formed_len(cs_type, NULL, str_len, well_formed_length);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == ret);
}

TEST_F(TestCharset, test_max_byte_char_pos)
{
  int ret = OB_SUCCESS;
  const ObCollationType types[] = {CS_TYPE_BINARY, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_BIN};
  for (int64_t i = 0; OB_SUCC(ret) && i < sizeof(types) / sizeof(ObCollationType); ++i) {
    int real_len = 0;
    int64_t char_len = 0;
    char buf[25600];
    gen_random_unicode_string(25500, buf, real_len);
    std::cout << "real_len" << real_len << std::endl;
    int64_t left_bytes = real_len;
    const int64_t block_size = 16000;
    char* pos = buf;
    while (left_bytes > 0) {
      int64_t well_formed_len = 0;
      int32_t well_formed_error = 0;
      int64_t calc_char_len = 0;
      const int64_t write_bytes = std::min(left_bytes, block_size);
      const int64_t real_bytes = ObCharset::max_bytes_charpos(types[i], pos, left_bytes, write_bytes, char_len);
      std::cout << "real_bytes" << real_bytes << std::endl;
      ASSERT_TRUE(real_bytes <= 16000);
      ret = ObCharset::well_formed_len(types[i], pos, real_bytes, well_formed_len, well_formed_error);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(real_bytes, well_formed_len);
      ASSERT_EQ(0, well_formed_error);
      calc_char_len = ObCharset::strlen_char(types[i], pos, real_bytes);
      ASSERT_EQ(calc_char_len, char_len);
      left_bytes -= real_bytes;
      pos += real_bytes;
    }
  }
}

TEST_F(TestCharset, test_ascii_list_for_all_charset)
{
  const int64_t buf_len = 100;
  char buf[buf_len] = {0};

  const int64_t chunk_size = 8192;
  char chunk[chunk_size] = {0};
  ObDataBuffer allocator(chunk, chunk_size);

  ASSERT_EQ(OB_SUCCESS, ObCharsetUtils::init(allocator));

  std::cout << "ascii";
  for (int cs_i = CHARSET_INVALID; cs_i < CHARSET_MAX; ++cs_i) {
    auto charset_type = static_cast<ObCharsetType>(cs_i);
    if (!ObCharset::is_valid_charset(charset_type))
      continue;
    ObCollationType cs_type = ObCharset::get_default_collation(charset_type);
    ASSERT_TRUE(ObCharset::is_valid_collation(cs_type));
    std::cout << "\t" << ObCharset::charset_name(cs_type);
  }
  std::cout << std::endl;

  for (int ascii_wc = 0; ascii_wc <= INT8_MAX; ascii_wc++) {
    std::cout << ascii_wc;
    for (int cs_i = CHARSET_INVALID; cs_i < CHARSET_MAX; ++cs_i) {
      auto charset_type = static_cast<ObCharsetType>(cs_i);
      if (!ObCharset::is_valid_charset(charset_type))
        continue;
      ObCollationType cs_type = ObCharset::get_default_collation(charset_type);
      ASSERT_TRUE(ObCharset::is_valid_collation(cs_type));
      int64_t result_len = 0;
      ObString str = ObCharsetUtils::get_const_str(cs_type, ascii_wc);
      ASSERT_EQ(OB_SUCCESS, hex_print(str.ptr(), str.length(), buf, buf_len, result_len));
      buf[result_len] = '\0';
      std::cout << "\t" << buf;
    }

    std::cout << std::endl;
  }
}

int unicode_to_utf8(ob_wc_t c, unsigned char* utf8string)
{
  if (c <= 0x7F) {
    utf8string[0] = c;
    return 1;
  } else if (c <= 0x7FF) {
    utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);
    utf8string[1] = 0x80 | (c & 0x3F);
    return 2;
  } else if (c <= 0xFFFF) {
    utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);
    utf8string[1] = 0x80 | ((c >> 6) & 0x3F);
    utf8string[2] = 0x80 | (c & 0x3F);
    return 3;
  } else {
    utf8string[0] = 0xF0 | ((c >> 18) & 0x07);
    utf8string[1] = 0x80 | ((c >> 12) & 0x3F);
    utf8string[2] = 0x80 | ((c >> 6) & 0x3F);
    utf8string[3] = 0x80 | (c & 0x3F);
    return 4;
  }

  return 0;
}

template <typename func>
void TestCharset::for_each_utf8(func handle)
{
  char buf[4];
  ObString str(4, 0, buf);

  for (ob_wc_t wchar = 0; wchar < 0x110000; wchar++) {
    int len = unicode_to_utf8(wchar, (unsigned char*)buf);
    ASSERT_TRUE(0 != len);
    str.set_length(len);
    handle(str, wchar);
  }
}

/*
template<typename func>
void TestCharset::for_each_binary(func handle) {
  char buf[3];
  ObString str(3, 0, buf);

  //one byte
  for (unsigned char c = 0; c < 0xFF; c++) {
    str.set_length(0);
    str.write((char*)(&c), 1);
    handle(str);
  }
  //two bytes
  for (unsigned char c1 = 0; c1 < 0xFF; c1++) {
    for (unsigned char c2 = 0; c2 < 0xFF; c2++) {
      str.set_length(0);
      str.write((char*)(&c1), 1);
      str.write((char*)(&c2), 1);
      handle(str);
    }
  }
  //three bytes
  for (unsigned char c1 = 0; c1 < 0xFF; c1++) {
    for (unsigned char c2 = 0; c2 < 0xFF; c2++) {
      for (unsigned char c3 = 0; c3 < 0xFF; c3++) {
        str.set_length(0);
        str.write((char*)(&c1), 1);
        str.write((char*)(&c2), 1);
        str.write((char*)(&c3), 1);
        handle(str);
      }
    }
  }
}
*/

struct TestReusltFileGuard {
  TestReusltFileGuard(const char* test_name) : fp_(nullptr)
  {
    std::string file_path;
    file_path.append("./");
    file_path.append(test_name);
    file_path.append(CUR_RESULT_FILE_SUFFIX);
    fp_ = fopen(file_path.c_str(), "w");
  }
  ~TestReusltFileGuard()
  {
    if (nullptr != fp_) {
      fclose(fp_);
      fp_ = nullptr;
    }
  }
  FILE* get_fp()
  {
    return fp_;
  }
  FILE* fp_;
};

void compare_result(const char* test_name)
{
  std::string cur_res_file_path, std_res_file_path;
  cur_res_file_path.append("./");
  cur_res_file_path.append(test_name);
  cur_res_file_path.append(CUR_RESULT_FILE_SUFFIX);
  std_res_file_path.append("./");
  std_res_file_path.append(test_name);
  std_res_file_path.append(STD_RESULT_FILE_SUFFIX);

  std::ifstream cur_res(cur_res_file_path, std::ios::binary);
  ASSERT_TRUE(cur_res.is_open());
  std::ifstream std_res(std_res_file_path, std::ios::binary);
  ASSERT_TRUE(std_res.is_open());

  std::string cur_line;
  std::string std_line;
  int line_no = 0;
  while (std::getline(std_res, std_line)) {
    line_no++;
    ASSERT_TRUE(std::getline(cur_res, cur_line));
    if (0 != std_line.compare(cur_line)) {
      fprintf(stdout,
          "not consistent result detected at line %d:\n"
          "cur_line:%s\n"
          "std_line:%s\n",
          line_no,
          cur_line.c_str(),
          std_line.c_str());
      ASSERT_TRUE(0);
    }
  }
}

TEST_F(TestCharset, test_ismbchar_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%d\t"
          "%d\n",
          wchar,
          str.length(),
          str.ptr(),
          ObCharset::is_mbchar(CS_TYPE_UTF8MB4_BIN, str.ptr(), str.ptr() + str.length()),
          ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, str.ptr(), str.ptr() + str.length()));
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_strlen_char_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%lu\t"
          "%lu\n",
          wchar,
          str.length(),
          str.ptr(),

          ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, str.ptr(), str.length()),
          ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, str.ptr(), str.length()));
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_mb_wc_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      int32_t cur_wchar1, cur_wchar2;
      int32_t length1, length2;

      ASSERT_EQ(0, ObCharset::mb_wc(CS_TYPE_UTF8MB4_BIN, str.ptr(), str.length(), length1, cur_wchar1));
      ASSERT_EQ(0, ObCharset::mb_wc(CS_TYPE_UTF8MB4_GENERAL_CI, str.ptr(), str.length(), length2, cur_wchar2));
      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%04x\t"
          "%04x\n",
          wchar,
          str.length(),
          str.ptr(),
          cur_wchar1,
          cur_wchar2);
      ASSERT_TRUE(cur_wchar1 == wchar);
      ASSERT_TRUE(cur_wchar2 == wchar);
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_wc_mb_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {

    auto handle = [](const ObString& str, ob_wc_t wchar) -> void {
      char buf[4];
      int32_t length;
      ObString res(4, 0, buf);

      ASSERT_EQ(0, ObCharset::wc_mb(CS_TYPE_UTF8MB4_BIN, wchar, buf, 4, length));
      res.set_length(length);
      ASSERT_TRUE(0 == str.compare(res));

      ASSERT_EQ(0, ObCharset::wc_mb(CS_TYPE_UTF8MB4_GENERAL_CI, wchar, buf, 4, length));
      res.set_length(length);
      ASSERT_TRUE(0 == str.compare(res));
    };
    TestCharset::for_each_utf8(handle);
  } while (0);
}

TEST_F(TestCharset, test_caseup_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      char buf1[4];
      char buf2[4];
      int length1, length2;

      ASSERT_TRUE(
          0 < (length1 = ObCharset::caseup(CS_TYPE_UTF8MB4_BIN, const_cast<char*>(str.ptr()), str.length(), buf1, 4)));
      ASSERT_TRUE(0 < (length2 = ObCharset::caseup(
                           CS_TYPE_UTF8MB4_GENERAL_CI, const_cast<char*>(str.ptr()), str.length(), buf2, 4)));

      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%.*s\t"
          "%.*s\n",
          wchar,
          str.length(),
          str.ptr(),
          length1,
          buf1,
          length2,
          buf2);
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_casedn_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      char buf1[4];
      char buf2[4];
      int length1, length2;

      ASSERT_TRUE(
          0 < (length1 = ObCharset::casedn(CS_TYPE_UTF8MB4_BIN, const_cast<char*>(str.ptr()), str.length(), buf1, 4)));
      ASSERT_TRUE(0 < (length2 = ObCharset::casedn(
                           CS_TYPE_UTF8MB4_GENERAL_CI, const_cast<char*>(str.ptr()), str.length(), buf2, 4)));

      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%.*s\t"
          "%.*s\n",
          wchar,
          str.length(),
          str.ptr(),
          length1,
          buf1,
          length2,
          buf2);
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_sortkey_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      char buf1[4];
      char buf2[4];
      int length1, length2;
      bool is_uni1, is_uni2;

      ASSERT_TRUE(0 < (length1 = ObCharset::sortkey(
                           CS_TYPE_UTF8MB4_BIN, const_cast<char*>(str.ptr()), str.length(), buf1, 4, is_uni1)));
      ASSERT_TRUE(is_uni1);
      ASSERT_TRUE(0 < (length2 = ObCharset::sortkey(
                           CS_TYPE_UTF8MB4_GENERAL_CI, const_cast<char*>(str.ptr()), str.length(), buf2, 4, is_uni2)));
      ASSERT_TRUE(is_uni2);

      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%.*s\t"
          "%.*s\n",
          wchar,
          str.length(),
          str.ptr(),
          length1,
          buf1,
          length2,
          buf2);
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

TEST_F(TestCharset, test_hash_sort_utf8)
{
  const char* test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  ObString test_name_pure(test_name);
  test_name_pure.split_on('_');
  do {
    TestReusltFileGuard file_guard(test_name);
    ASSERT_TRUE(NULL != file_guard.get_fp());

    auto handle = [&file_guard](const ObString& str, ob_wc_t wchar) -> void {
      fprintf(file_guard.get_fp(),
          "U+%04lX\t"
          "%.*s\t"
          "%lu\t"
          "%lu\t"
          "%lu\t"
          "%lu\n",
          wchar,
          str.length(),
          str.ptr(),
          ObCharset::hash(CS_TYPE_UTF8MB4_BIN, const_cast<char*>(str.ptr()), str.length(), 0, 0, NULL),
          ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, const_cast<char*>(str.ptr()), str.length(), 0, 0, NULL),
          ObCharset::hash(CS_TYPE_UTF8MB4_BIN, const_cast<char*>(str.ptr()), str.length(), 0, 1, NULL),
          ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, const_cast<char*>(str.ptr()), str.length(), 0, 1, NULL));
    };
    fprintf(file_guard.get_fp(),
        "wchar\t"
        "str\t"
        "%.*s(UTF8MB4_BIN)\t"
        "%.*s(UTF8MB4_GENERAL_CI)\t"
        "%.*s(UTF8MB4_BIN oracle)\t"
        "%.*s(UTF8MB4_GENERAL_CI oracle)\n",
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr(),
        test_name_pure.length(),
        test_name_pure.ptr());
    TestCharset::for_each_utf8(handle);
  } while (0);

  compare_result(test_name);
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
