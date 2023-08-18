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
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "gtest/gtest.h"
#include "unicode_map.h"
#include "common/data_buffer.h"
#include "lib/oblog/ob_log_module.h"
#define USING_LOG_PREFIX SQL

using namespace oceanbase::common;

class TestCharset: public ::testing::Test
{
public:
  TestCharset();
  virtual ~TestCharset();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void gen_random_unicode_string(const int len, char *res, int &real_len);
  int random_range(const int low, const int high);
};

TestCharset::TestCharset()
{
}

TestCharset::~TestCharset()
{
}

void TestCharset::SetUp()
{
  srand((unsigned)time(NULL ));
}

void TestCharset::TearDown()
{
}

int TestCharset::random_range(const int low, const int high)
{
  return std::rand() % (high - low) + low;
}

void TestCharset::gen_random_unicode_string(const int len, char *res, int &real_len)
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

  //std::map<int, int> charset{
    //{8,0},{28,1},{45,2}};
  std::map<int, int> charset{
    {8,0},{28,1},{45,2},{46,3},{47,4},{54,5},{55,6},{63,7},{87,8},{101,9},{216,10},{224,11},
    {248,12},{249,13},{251,14}};
  // init test_string, the order should be same as charset's second param
  // test_string.first is a valid unicode for correspond charset while the second is invalid
  // but for some charset it is all valid, like latin1, utf8
  std::vector<std::pair<std::string, std::string >> test_string;

  const char ascii_string[] = {'\x7f','\0'};
  const char non_ascii_string[] = {'\xff','\0'};
  const char gbk_string[] = { '\xc4', '\xe3', '\xba', '\xc3','\0' };//meaing is '你好'
  const char gb18030_string[] = { '\xc4', '\xe3', '\xba', '\xc3','\0' };//meaing is '你好'
  const char utf8_string[] = { '\xe4', '\xbd', '\xa0', '\xe5', '\xa5', '\xbd','\0'};//meaing is '你好'
  const char utf16_string[] = { '\x4f', '\x60', '\x59', '\x7d','\0'};//meaing is '你好'
  test_string.push_back(std::make_pair(std::string(ascii_string),std::string((non_ascii_string)))); //CS_TYPE_LATIN1_SWEDISH_CI
  test_string.push_back(std::make_pair(std::string(gbk_string),std::string((non_ascii_string)))); //CS_TYPE_GBK_CHINESE_CI
  test_string.push_back(std::make_pair(std::string(utf8_string),std::string((non_ascii_string)))); //CS_TYPE_UTF8MB4_GENERAL_CI
  test_string.push_back(std::make_pair(std::string(utf8_string),std::string((non_ascii_string)))); //CS_TYPE_UTF8MB4_BIN
  test_string.push_back(std::make_pair(std::string(ascii_string),std::string((non_ascii_string)))); //CS_TYPE_LATIN1_BIN
  test_string.push_back(std::make_pair(std::string(utf16_string),std::string((non_ascii_string)))); //CS_TYPE_UTF16_GENERAL_CI
  test_string.push_back(std::make_pair(std::string(utf16_string),std::string((non_ascii_string)))); //CS_TYPE_UTF16_BIN
  test_string.push_back(std::make_pair(std::string(ascii_string),std::string((non_ascii_string)))); //CS_TYPE_BINARY
  test_string.push_back(std::make_pair(std::string(gbk_string),std::string((non_ascii_string)))); //CS_TYPE_GBK_BIN
  test_string.push_back(std::make_pair(std::string(utf16_string),std::string((non_ascii_string)))); //CS_TYPE_UTF16_UNICODE_CI
  test_string.push_back(std::make_pair(std::string(gb18030_string),std::string((non_ascii_string)))); //CS_TYPE_GB18030_2022_BIN
  test_string.push_back(std::make_pair(std::string(utf8_string),std::string((non_ascii_string))));//CS_TYPE_UTF8MB4_UNICODE_CI
  test_string.push_back(std::make_pair(std::string(gb18030_string),std::string((non_ascii_string)))); //CS_TYPE_GB18030_CHINESE_CI
  test_string.push_back(std::make_pair(std::string(gb18030_string),std::string((non_ascii_string)))); //CS_TYPE_GB18030_BIN
  test_string.push_back(std::make_pair(std::string(gb18030_string),std::string((non_ascii_string)))); //CS_TYPE_GB18030_CHINESE_CS

  //result[0]: charset index
  //result[1],result[2]: the size and validility of the first string
  //result[3],result[4]: the size and validility of the second string
  std::vector<std::vector<int>>result{
    {0,1,1,1,1},
    {1,4,1,1,0},
    {2,6,1,0,0},
    {3,6,1,0,0},
    {4,1,1,1,1},
    {5,4,1,0,0},
    {6,4,1,0,0},
    {7,1,1,1,1},
    {8,4,1,1,1},
    {9,10,1,10,1},
    {10,4,1,1,1},
    {11,10,1,10,1},
    {12,8,1,1,0},
    {13,4,1,1,1},
    {14,8,1,1,0}
  };
  for (auto it : charset) {
    bool is_valid_collation = ObCharset::is_valid_collation(it.first);
    ASSERT_TRUE(is_valid_collation);
    const char* p1 = test_string[it.second].first.data();
    int p1_len = test_string[it.second].first.length();
    const char* p2 = test_string[it.second].second.data();
    int p2_len = test_string[it.second].second.length();
    size1 = ObCharset::sortkey((ObCollationType)it.first, p1, p1_len, aa1, 10, is_valid_unicode);
    ASSERT_TRUE(size1 == result[it.second][1]);
    ASSERT_TRUE(is_valid_unicode == result[it.second][2]);

    size1 = ObCharset::sortkey((ObCollationType)it.first, p2, p2_len, aa1, 10, is_valid_unicode);
    ASSERT_TRUE(size1 == result[it.second][3]);
    ASSERT_TRUE(is_valid_unicode == result[it.second][4]);
  }
  // The parameter of sortkey cannot be NULL
  //char *p = NULL;
  //size1 = ObCharset::sortkey(CS_TYPE_UTF8MB4_GENERAL_CI, true, p, 0, aa1, 10);
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
  fprintf(stdout, "ret:%p, %d\n", y1.ptr(), y1.length() );
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
  ObString y1= "Variable_name";
  ObString y2= "variable_name";
  ObString y3= "variable_name1";
  ObString y4= "variable_name1";
  bool yy = ObCharset::case_insensitive_equal(y1, y2, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_TRUE(yy);
  yy = ObCharset::case_insensitive_equal(y2, y3, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_FALSE(yy);
  yy = ObCharset::case_insensitive_equal(y3, y4, CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_TRUE(yy);

  yy = ObCharset::case_insensitive_equal(y1, y2, CS_TYPE_GB18030_2022_PINYIN_CI);
  ASSERT_TRUE(yy);
  yy = ObCharset::case_insensitive_equal(y2, y3, CS_TYPE_GB18030_2022_PINYIN_CI);
  ASSERT_FALSE(yy);
  yy = ObCharset::case_insensitive_equal(y3, y4, CS_TYPE_GB18030_2022_PINYIN_CI);
  ASSERT_TRUE(yy);
}

TEST_F(TestCharset, hash_sort)
{
  ObString s;
  uint64_t ret = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, s.ptr(), s.length(), 0);
  const char *a = "abd";
  const char *b = "aBD";
  uint64_t ret1 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, a, 3, 0);
  uint64_t ret2 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, b, 3, 0);
  fprintf(stdout, "ret:%lu, ret1:%lu, ret2:%lu\n", ret, ret1, ret2);
  //uint64_t ret3 = ObCharset::hash(CS_TYPE_UTF8MB4_GENERAL_CI, ObString::make_string(b));
  ASSERT_EQ(ret1, ret2);
}

TEST_F(TestCharset, case_mode_equal)
{
  ObString y1= "Variable_name";
  ObString y2= "variable_name";
  ObString y3= "variable_name1";
  ObString y4= "variable_name1";
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
  const char *str = "\0123";
  ObCollationType cs_type =  CS_TYPE_UTF8MB4_GENERAL_CI;
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
    char *pos = buf;
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

  std::cout<< "ascii";
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
    std::cout<< ascii_wc;
    for (int cs_i = CHARSET_INVALID; cs_i < CHARSET_MAX; ++cs_i) {
      auto charset_type = static_cast<ObCharsetType>(cs_i);
      if (!ObCharset::is_valid_charset(charset_type))
        continue;
      ObCollationType cs_type = ObCharset::get_default_collation(charset_type);
      ASSERT_TRUE(ObCharset::is_valid_collation(cs_type));
      int64_t result_len = 0;
      ObString str = ObCharsetUtils::get_const_str(cs_type, ascii_wc);
      ASSERT_EQ (OB_SUCCESS, hex_print(str.ptr(), str.length(), buf, buf_len, result_len));
      buf[result_len] = '\0';
      std::cout <<"\t" << buf;
    }

    std::cout << std::endl;
  }

}

TEST_F(TestCharset, test_find_gb18030_case_prob)
{
  const int buf_len = 20;
  char buf1[buf_len];
  char buf2[buf_len];
  char hex_buf1[buf_len];
  char hex_buf2[buf_len];
  int length1 = 0, length2 = 0;
  ObCollationType cs_type = CS_TYPE_GB18030_BIN;
  for (int i = 0; i < 256; i++) {
    const ObUnicaseInfoChar *info = ObCharset::get_charset(cs_type)->caseinfo->page[i];
    if (NULL != info) {
      for (int j = 0; j < 256; j++) {
        ASSERT_TRUE(OB_SUCCESS == ObCharset::wc_mb(cs_type, info[j].tolower, buf1, buf_len, length1));
        ASSERT_TRUE(OB_SUCCESS == ObCharset::wc_mb(cs_type, info[j].toupper, buf2, buf_len, length2));
        buf1[length1] = '\0';
        buf2[length2] = '\0';
        if (length1 != length2) {
          ASSERT_TRUE(OB_SUCCESS == to_hex_cstr(buf1, length1, hex_buf1, buf_len));
          ASSERT_TRUE(OB_SUCCESS == to_hex_cstr(buf2, length2, hex_buf2, buf_len));
          std::cout<< info[j].tolower <<"," << info[j].toupper << "," << hex_buf1 << "," << hex_buf2 << std::endl;
        }
      }
    }
  }
  cs_type = CS_TYPE_GB18030_2022_BIN;
  for (int i = 0; i < 256; i++) {
    const ObUnicaseInfoChar *info = ObCharset::get_charset(cs_type)->caseinfo->page[i];
    if (NULL != info) {
      for (int j = 0; j < 256; j++) {
        ASSERT_TRUE(OB_SUCCESS == ObCharset::wc_mb(cs_type, info[j].tolower, buf1, buf_len, length1));
        ASSERT_TRUE(OB_SUCCESS == ObCharset::wc_mb(cs_type, info[j].toupper, buf2, buf_len, length2));
        buf1[length1] = '\0';
        buf2[length2] = '\0';
        if (length1 != length2) {
          ASSERT_TRUE(OB_SUCCESS == to_hex_cstr(buf1, length1, hex_buf1, buf_len));
          ASSERT_TRUE(OB_SUCCESS == to_hex_cstr(buf2, length2, hex_buf2, buf_len));
          std::cout<< info[j].tolower <<"," << info[j].toupper << "," << hex_buf1 << "," << hex_buf2 << std::endl;
        }
      }
    }
  }
}

/*
TEST_F(TestCharset, test_gbk_pua)
{
  
  int64_t size = sizeof(gbk_uni_map) / sizeof(UniCodeMap);
  ASSERT_EQ(size, 23940);
  for (int i = 0; i < size; i++) {
    ASSERT_TRUE(func_gbk_uni_onechar(gbk_uni_map[i].encoding) == gbk_uni_map[i].unicode) << "i=" << i;
    ASSERT_TRUE(func_uni_gbk_onechar(gbk_uni_map[i].unicode) == gbk_uni_map[i].encoding) << "i=" << i;
  }
}
*/

TEST_F(TestCharset, test_zh_0900_as_cs)
{


  ObString str;
  char sort_key[2048];
  bool is_valid = false;

  auto print_sort_key = [&](ObCollationType coll_type) -> void {
      auto size = ObCharset::sortkey(coll_type, str.ptr(), str.length(), sort_key,
                                     sizeof(sort_key), is_valid);
      fprintf(stdout, "src=");
      for (int i = 0; i < str.length(); i++) {
        fprintf(stdout, "%02X", (unsigned char)str[i]);
      }
      fprintf(stdout, "\n");
      fprintf(stdout, "sort_key=");
      for (int i = 0; i < size; i++) {
        fprintf(stdout, "%02X", (unsigned char)sort_key[i]);
      }
      fprintf(stdout, "\n");
  };

  char buffer[2048];
  ObDataBuffer data_buffer(buffer, sizeof(buffer));

  auto convert_string = [&data_buffer](const char* input, ObCollationType dest_type) -> ObString {
    ObString output;
    ObCharset::charset_convert(data_buffer, ObString(input), CS_TYPE_UTF8MB4_BIN, dest_type, output);
    return output;
  };

  ObCollationType coll_types[] = {CS_TYPE_UTF8MB4_ZH_0900_AS_CS, CS_TYPE_GBK_ZH_0900_AS_CS,
                                 CS_TYPE_GB18030_ZH_0900_AS_CS, CS_TYPE_UTF16_ZH_0900_AS_CS,
                                 CS_TYPE_GB18030_2022_ZH_0900_AS_CS};

  for (int i = 0; i < array_elements(coll_types); i++) {
    ObCollationType coll_type = coll_types[i];
    fprintf(stdout, "## TEST_COLL=%d\n", coll_type);

    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("坝", coll_type), convert_string("弝", coll_type)) < 0);
    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("弝", coll_type), convert_string("爸", coll_type)) < 0);
    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("爸", coll_type), convert_string("跁", coll_type)) < 0);
    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("韩", coll_type), convert_string("美", coll_type)) < 0);
    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("美", coll_type), convert_string("日", coll_type)) < 0);

    str = convert_string("我们今天", coll_type);
    print_sort_key(coll_types[i]);
    str = "\xFF\xFF";
    print_sort_key(coll_types[i]);
    str = "\xef\xbf\xbd\xef\xbf\xbd";
    print_sort_key(coll_types[i]);
    str = convert_string("中", coll_type);
    print_sort_key(coll_types[i]);
  }
}

TEST_F(TestCharset, test_zh2_0900_as_cs)
{


  ObString str;
  char sort_key[2048];
  bool is_valid = false;

  auto print_sort_key = [&](ObCollationType coll_type) -> void {
      auto size = ObCharset::sortkey(coll_type, str.ptr(), str.length(), sort_key,
                                     sizeof(sort_key), is_valid);
      fprintf(stdout, "src=");
      for (int i = 0; i < str.length(); i++) {
        fprintf(stdout, "%02X", (unsigned char)str[i]);
      }
      fprintf(stdout, "\n");
      fprintf(stdout, "sort_key=");
      for (int i = 0; i < size; i++) {
        fprintf(stdout, "%02X", (unsigned char)sort_key[i]);
      }
      fprintf(stdout, "\n");
  };

  char buffer[2048];
  ObDataBuffer data_buffer(buffer, sizeof(buffer));

  auto convert_string = [&data_buffer](const char* input, ObCollationType dest_type) -> ObString {
    ObString output;
    ObCharset::charset_convert(data_buffer, ObString(input), CS_TYPE_UTF8MB4_BIN, dest_type, output);
    return output;
  };

  ObCollationType coll_types[] = {CS_TYPE_UTF8MB4_ZH2_0900_AS_CS, CS_TYPE_GB18030_2022_ZH2_0900_AS_CS};

  for (int i = 0; i < array_elements(coll_types); i++) {
    ObCollationType coll_type = coll_types[i];
    fprintf(stdout, "## TEST_COLL=%d\n", coll_type);

    ASSERT_TRUE(ObCharset::strcmp(coll_type, convert_string("一", coll_type), convert_string("二", coll_type)) < 0);

    str = convert_string("一丁丂七丄丅丆", coll_type);
    print_sort_key(coll_types[i]);


    /*
    str = convert_string("我们今天", coll_type);
    print_sort_key(coll_types[i]);
    str = "\xFF\xFF";
    print_sort_key(coll_types[i]);
    str = "\xef\xbf\xbd\xef\xbf\xbd";
    print_sort_key(coll_types[i]);
    str = convert_string("中", coll_type);
    print_sort_key(coll_types[i]);
    */
  }
}


TEST_F(TestCharset, tolower)
{
  ObArenaAllocator allocator;
  char a1[] = "Variable_name";
  char a2[] = "Variable_NAME";
  char a3[] = "variable_name";
  ObString y1;
  ObString y2;
  ObString y3;
  y1.assign_ptr(a1, strlen(a1));
  y2.assign_ptr(a2, strlen(a2));
  y3.assign_ptr(a3, strlen(a3));
  fprintf(stdout, "ret:%p, %d\n", y1.ptr(), y1.length() );
  for (int cs_i = CHARSET_INVALID; cs_i < CHARSET_MAX; ++cs_i) {
    auto charset_type = static_cast<ObCharsetType>(cs_i);
    if (!ObCharset::is_valid_charset(charset_type) || CHARSET_UTF16 == charset_type || CHARSET_BINARY == charset_type)
      continue;
    ObCollationType cs_type = ObCharset::get_default_collation(charset_type);
    ASSERT_TRUE(ObCharset::is_valid_collation(cs_type));
    const char *cs_name = ObCharset::charset_name(cs_type);

    ObString y1_res;
    ASSERT_TRUE(OB_SUCCESS == ObCharset::tolower(cs_type, y1, y1_res, allocator));
    fprintf(stdout, "charset=%s, src:%.*s, src_lower:%.*s, dst:%.*s\n", cs_name,
            y1.length(), y1.ptr(), y1_res.length(), y1_res.ptr(), y3.length(), y3.ptr());
    EXPECT_TRUE(y1_res == y3);
    ObString y2_res;
    ASSERT_TRUE(OB_SUCCESS == ObCharset::tolower(cs_type, y2, y2_res, allocator));
    fprintf(stdout, "charset=%s, src:%.*s, src_lower:%.*s, dst:%.*s\n", cs_name,
            y2.length(), y2.ptr(), y2_res.length(), y2_res.ptr(), y3.length(), y3.ptr());
    EXPECT_TRUE(y2_res == y3);
  }
}


TEST_F(TestCharset, toupper)
{
  ObArenaAllocator allocator;
  char a1[] = "Variable_name";
  char a2[] = "Variable_NAME";
  char a3[] = "VARIABLE_NAME";
  ObString y1;
  ObString y2;
  ObString y3;
  y1.assign_ptr(a1, strlen(a1));
  y2.assign_ptr(a2, strlen(a2));
  y3.assign_ptr(a3, strlen(a3));
  fprintf(stdout, "ret:%p, %d\n", y1.ptr(), y1.length() );
  for (int cs_i = CHARSET_INVALID; cs_i < CHARSET_MAX; ++cs_i) {
    auto charset_type = static_cast<ObCharsetType>(cs_i);
    if (!ObCharset::is_valid_charset(charset_type) || CHARSET_UTF16 == charset_type || CHARSET_BINARY == charset_type)
      continue;
    ObCollationType cs_type = ObCharset::get_default_collation(charset_type);
    ASSERT_TRUE(ObCharset::is_valid_collation(cs_type));
    const char *cs_name = ObCharset::charset_name(cs_type);

    ObString y1_res;
    ASSERT_TRUE(OB_SUCCESS == ObCharset::toupper(cs_type, y1, y1_res, allocator));
    fprintf(stdout, "charset=%s, src:%.*s, src_upper:%.*s, dst:%.*s\n", cs_name,
            y1.length(), y1.ptr(), y1_res.length(), y1_res.ptr(), y3.length(), y3.ptr());
    EXPECT_TRUE(y1_res == y3);
    ObString y2_res;
    ASSERT_TRUE(OB_SUCCESS == ObCharset::toupper(cs_type, y2, y2_res, allocator));
    fprintf(stdout, "charset=%s, src:%.*s, src_upper:%.*s, dst:%.*s\n", cs_name,
            y2.length(), y2.ptr(), y2_res.length(), y2_res.ptr(), y3.length(), y3.ptr());
    EXPECT_TRUE(y2_res == y3);
  }
}

static uint get_magic_gb18030_2022_uni(uint code)
{
  switch (code) {
    case 0xFE59 : return 0x9FB4;
    case 0xFE61 : return 0x9FB5;
    case 0xFE66 : return 0x9FB6;
    case 0xFE67 : return 0x9FB7;
    case 0xFE6D : return 0x9FB8;
    case 0xFE7E : return 0x9FB9;
    case 0xFE90 : return 0x9FBA;
    case 0xFEA0 : return 0x9FBB;
    case 0xA6D9 : return 0xFE10;
    case 0xA6DA : return 0xFE12;
    case 0xA6DB : return 0xFE11;
    case 0xA6DC : return 0xFE13;
    case 0xA6DD : return 0xFE14;
    case 0xA6DE : return 0xFE15;
    case 0xA6DF : return 0xFE16;
    case 0xA6EC : return 0xFE17;
    case 0xA6ED : return 0xFE18;
    case 0xA6F3 : return 0xFE19;
    case 0x82359037 : return 0xE81E;
    case 0x82359038 : return 0xE826;
    case 0x82359039 : return 0xE82B;
    case 0x82359130 : return 0xE82C;
    case 0x82359131 : return 0xE832;
    case 0x82359132 : return 0xE843;
    case 0x82359133 : return 0xE854;
    case 0x82359134 : return 0xE864;
    case 0x84318236 : return 0xE78D;
    case 0x84318238 : return 0xE78E;
    case 0x84318237 : return 0xE78F;
    case 0x84318239 : return 0xE790;
    case 0x84318330 : return 0xE791;
    case 0x84318331 : return 0xE792;
    case 0x84318332 : return 0xE793;
    case 0x84318333 : return 0xE794;
    case 0x84318334 : return 0xE795;
    case 0x84318335 : return 0xE796;
    default: return 0;
  }
}

static uint get_magic_uni_gb18030_2022(uint code)
{
  switch (code) {
    case 0x9FB4 : return 0xFE59;
    case 0x9FB5 : return 0xFE61;
    case 0x9FB6 : return 0xFE66;
    case 0x9FB7 : return 0xFE67;
    case 0x9FB8 : return 0xFE6D;
    case 0x9FB9 : return 0xFE7E;
    case 0x9FBA : return 0xFE90;
    case 0x9FBB : return 0xFEA0;
    case 0xFE10 : return 0xA6D9;
    case 0xFE12 : return 0xA6DA;
    case 0xFE11 : return 0xA6DB;
    case 0xFE13 : return 0xA6DC;
    case 0xFE14 : return 0xA6DD;
    case 0xFE15 : return 0xA6DE;
    case 0xFE16 : return 0xA6DF;
    case 0xFE17 : return 0xA6EC;
    case 0xFE18 : return 0xA6ED;
    case 0xFE19 : return 0xA6F3;
    case 0xE81E : return 0x82359037;
    case 0xE826 : return 0x82359038;
    case 0xE82B : return 0x82359039;
    case 0xE82C : return 0x82359130;
    case 0xE832 : return 0x82359131;
    case 0xE843 : return 0x82359132;
    case 0xE854 : return 0x82359133;
    case 0xE864 : return 0x82359134;
    case 0xE78D : return 0x84318236;
    case 0xE78E : return 0x84318238;
    case 0xE78F : return 0x84318237;
    case 0xE790 : return 0x84318239;
    case 0xE791 : return 0x84318330;
    case 0xE792 : return 0x84318331;
    case 0xE793 : return 0x84318332;
    case 0xE794 : return 0x84318333;
    case 0xE795 : return 0x84318334;
    case 0xE796 : return 0x84318335;
    default: return 0;
  }
}

static inline uint gb18030_chs_to_code(const uchar *src, size_t srclen) {
  uint r = 0;

  ob_charset_assert(srclen == 1 || srclen == 2 || srclen == 4);

  switch (srclen) {
    case 1:
      r = src[0];
      break;
    case 2:
      r = (src[0] << 8) + src[1];
      break;
    case 4:
      r = (src[0] << 24) + (src[1] << 16) + (src[2] << 8) + src[3];
      break;
    default:
      ob_charset_assert(0);
  }

  return r;
}

TEST_F(TestCharset, check_gb18030_2022)
{
  int ret = 0;
  uchar s[4];

  ob_charset_conv_mb_wc ob_mb_wc_gb18030_2022 = ob_charset_gb18030_2022_pinyin_ci.cset->mb_wc;
  ob_charset_conv_mb_wc ob_mb_wc_gb18030 = ob_charset_gb18030_chinese_ci.cset->mb_wc;
  ob_charset_conv_wc_mb ob_wc_mb_gb18030_2022 = ob_charset_gb18030_2022_pinyin_ci.cset->wc_mb;
  ob_charset_conv_wc_mb ob_wc_mb_gb18030 = ob_charset_gb18030_chinese_ci.cset->wc_mb;

  for (s[0] = 0x81; s[0] <= 0xFE; s[0]++) {
    for (s[1] = 0x40; s[1] <= 0xFE; s[1]++) {
      if (s[1] == 0x7F) {
        continue;
      }
      uint gb_code = gb18030_chs_to_code(s, 2);
      ob_wc_t uni_gb18030_2022;
      ob_mb_wc_gb18030_2022(NULL, &uni_gb18030_2022, s, s + 4);
      ulong target = get_magic_gb18030_2022_uni(gb_code);
      if (target == 0) {
        ob_mb_wc_gb18030(NULL, &target, s, s + 4);
      }
      ASSERT_TRUE(target = uni_gb18030_2022);
    }
  }
  for (s[0] = 0x81; s[0] <= 0xFE; s[0]++) {
    for (s[1] = 0x30; s[1] <= 0x39; s[1]++) {
      for (s[2] = 0x81; s[2] <= 0xFE; s[2]++) {
        for (s[3] = 0x30; s[3] <= 0x39; s[3]++) {
          uint gb_code = gb18030_chs_to_code(s, 4);
          ob_wc_t uni_gb18030_2022;
          ob_mb_wc_gb18030_2022(NULL, &uni_gb18030_2022, s, s + 4);
          ulong target = get_magic_gb18030_2022_uni(gb_code);
          if (target == 0) {
            ob_mb_wc_gb18030(NULL, &target, s, s + 4);
          }
          ASSERT_TRUE(target = uni_gb18030_2022);
        }
      }
    }
  }

  for (uint i=0; i <= 0x10FFFF; i ++) {
    uchar s_gb18030[4];
    uchar s_gb18030_2022[4];
    uint target = get_magic_uni_gb18030_2022(i);
    if (target == 0) {
      int len_gb18030 = ob_wc_mb_gb18030(NULL, i, s_gb18030, s_gb18030 + 4);
      target = (len_gb18030 == 0) ? 0 : gb18030_chs_to_code(s_gb18030, len_gb18030);
    }
    int len_gb18030_2022 = ob_wc_mb_gb18030_2022(NULL, i, s_gb18030_2022, s_gb18030_2022 + 4);
    uint code_gb18030_2022 = (len_gb18030_2022 == 0) ? 0 : gb18030_chs_to_code(s_gb18030_2022, len_gb18030_2022);
    ASSERT_TRUE(target == code_gb18030_2022);
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  int ret = ObCharset::init_charset();
  fprintf(stdout, "ret=%d\n", ret);
  return RUN_ALL_TESTS();
}
