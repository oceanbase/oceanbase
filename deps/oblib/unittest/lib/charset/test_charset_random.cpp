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

class TestCharsetRandom : public ::testing::Test {
public:
  TestCharsetRandom();
  virtual ~TestCharsetRandom();
  virtual void SetUp();
  virtual void TearDown();
  template <typename func>
  void for_each_utf8(func handle);

protected:
  void gen_random_unicode_string(const int len, char* res, int& real_len);
  int random_range(const int low, const int high);
};

TestCharsetRandom::TestCharsetRandom()
{}

TestCharsetRandom::~TestCharsetRandom()
{}

void TestCharsetRandom::SetUp()
{
  srand((unsigned)time(NULL));
}

void TestCharsetRandom::TearDown()
{}

int TestCharsetRandom::random_range(const int low, const int high)
{
  return std::rand() % (high - low) + low;
}

void TestCharsetRandom::gen_random_unicode_string(const int len, char* res, int& real_len)
{
  int pos = 0;
  int unicode_point = 0;
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
  for (int i = 0; i < len; ++i) {
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
    MEMCPY(res + pos, &utf_str[0], utf_str.length());
    pos += utf_str.length();
  }
  real_len = pos;
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
void TestCharsetRandom::for_each_utf8(func handle)
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

TEST_F(TestCharsetRandom, test_wellformed_len_random)
{
  const int64_t max_len = 100;
  const int64_t max_random_times = 1000;
  char buf[(max_len + 10) * 4 + 1];

  for (int64_t char_len = 0; char_len <= max_len; char_len++) {
    for (int random_times = max_random_times; random_times > 0; random_times--) {
      int real_len = 0;
      int64_t well_formed_len = 0;
      gen_random_unicode_string(char_len, buf, real_len);

      // debug value
      std::string str(buf, real_len);

      // ismbchar()  - detects whether the given string is a multi-byte sequence
      do {
        bool is_mbchar_utf8 = (char_len > 0 && ((unsigned char*)buf)[0] > 0x7F);
        ASSERT_TRUE(ObCharset::is_mbchar(CS_TYPE_BINARY, buf, buf + real_len) == 0);
        ASSERT_TRUE(ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, buf, buf + real_len) == is_mbchar_utf8);
        ASSERT_TRUE(ObCharset::is_mbchar(CS_TYPE_UTF8MB4_BIN, buf, buf + real_len) == is_mbchar_utf8);
      } while (0);

      // numchars()  - returns number of characters in the given string, e.g. in SQL function CHAR_LENGTH().
      do {
        ASSERT_TRUE(ObCharset::strlen_char(CS_TYPE_BINARY, buf, real_len) == real_len);
        ASSERT_TRUE(ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len) == char_len);
        ASSERT_TRUE(ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, buf, real_len) == char_len);
      } while (0);

      // charpos()   - calculates the offset of the given position in the string.
      //               Used in SQL functions LEFT(), RIGHT(), SUBSTRING(),
      do {
        ASSERT_TRUE(ObCharset::charpos(CS_TYPE_BINARY, buf, real_len, real_len) == real_len);
        ASSERT_TRUE(ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len, char_len) == real_len);
        ASSERT_TRUE(ObCharset::charpos(CS_TYPE_UTF8MB4_BIN, buf, real_len, char_len) == real_len);
      } while (0);

      // max_bytes_charpos()   - calculates the offset of the given byte position in the string.
      do {
        int64_t char_pos = 0;
        ASSERT_TRUE(ObCharset::max_bytes_charpos(CS_TYPE_BINARY, buf, real_len, real_len, char_pos) == real_len);
        ASSERT_TRUE(char_pos == real_len);
        ASSERT_TRUE(
            ObCharset::max_bytes_charpos(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len, real_len, char_pos) == real_len);
        ASSERT_TRUE(char_pos == char_len);
        ASSERT_TRUE(ObCharset::max_bytes_charpos(CS_TYPE_UTF8MB4_BIN, buf, real_len, real_len, char_pos) == real_len);
        ASSERT_TRUE(char_pos == char_len);
      } while (0);

      // well_formed_len()
      //             - returns length of a given multi-byte string in bytes
      //               Used in INSERTs to shorten the given string so it
      //               a) is "well formed" according to the given character set
      //               b) can fit into the given data type
      do {
        ASSERT_TRUE(0 == ObCharset::well_formed_len(CS_TYPE_BINARY, buf, real_len, well_formed_len));
        ASSERT_TRUE(well_formed_len == real_len);
        ASSERT_TRUE(0 == ObCharset::well_formed_len(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len, well_formed_len));
        ASSERT_TRUE(well_formed_len == real_len);
        ASSERT_TRUE(0 == ObCharset::well_formed_len(CS_TYPE_UTF8MB4_BIN, buf, real_len, well_formed_len));
        ASSERT_TRUE(well_formed_len == real_len);
      } while (0);

      // lengthsp()  - returns the length of the given string without trailing spaces.
      do {
        int gen_space_len = random_range(0, 10);
        int ori_space_len = 0;
        while (ori_space_len < real_len && buf[real_len - ori_space_len - 1] == 0x20)
          ori_space_len++;
        MEMSET(buf + real_len, 0x20, gen_space_len);
        ASSERT_TRUE(
            ObCharset::strlen_byte_no_sp(CS_TYPE_BINARY, buf, real_len + gen_space_len) == real_len + gen_space_len);
        ASSERT_TRUE(ObCharset::strlen_byte_no_sp(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len + gen_space_len) ==
                    real_len - ori_space_len);
        ASSERT_TRUE(ObCharset::strlen_byte_no_sp(CS_TYPE_UTF8MB4_GENERAL_CI, buf, real_len + gen_space_len) ==
                    real_len - ori_space_len);
      } while (0);

      // mb_wc       - converts the left multi-byte sequence into its Unicode code.
      // wc_mb       - converts the given Unicode code into multi-byte sequence.

      // caseup      - converts the given string to lowercase using length
      // casedn      - converts the given string to lowercase using length
      // fill()     - writes the given Unicode value into the given string
      //              with the given length. Used to pad the string, usually
      //              with space character, according to the given charset.
      // String-to-number conversion routines
      // scan()    - to skip leading spaces in the given string.
      //             Used when a string value is inserted into a numeric field.

      // COLLATION HANDLER
      // strnncoll()   - compares two strings according to the given collation
      // strnncollsp() - like the above but ignores trailing spaces for PAD SPACE
      //                 collations. For NO PAD collations, identical to strnncoll.
      // strnxfrm()    - makes a sort key suitable for memcmp() corresponding
      //                 to the given string
      // like_range()  - creates a LIKE range, for optimizer
      // wildcmp()     - wildcard comparison, for LIKE
      // strcasecmp()  - 0-terminated string comparison
      // instr()       - finds the first substring appearance in the string
      // hash_sort()   - calculates hash value taking into account
      //                 the collation rules, e.g. case-insensitivity,
      //                 accent sensitivity, etc.
    }
  }
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
