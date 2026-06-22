/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstring>
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "sql/parser/ob_parser_fullwidth_converter.h"
#include "sql/parser/ob_parser_fullwidth_converter_c.h"
#include "lib/charset/ob_charset.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestFullWidthSymbols : public ::testing::Test
{
public:
  TestFullWidthSymbols() : allocator_(ObModIds::TEST) {}
  virtual ~TestFullWidthSymbols() {}
  virtual void SetUp() {}
  virtual void TearDown() { allocator_.reset(); }

protected:
  ObArenaAllocator allocator_;

  int parse_sql_for_test(const char *sql, ParseResult &parse_result)
  {
    int ret = OB_SUCCESS;
    memset(&parse_result, 0, sizeof(ParseResult));
    parse_result.malloc_pool_ = &allocator_;
    parse_result.sql_mode_ = SMO_ORACLE;
    parse_result.connection_collation_ = CS_TYPE_UTF8MB4_BIN;
    parse_result.charset_info_ = ObCharset::get_charset(CS_TYPE_UTF8MB4_BIN);
    parse_result.charset_info_oracle_db_ = parse_result.charset_info_;
    parse_result.semicolon_start_col_ = INT32_MAX;
    if (OB_SUCCESS != (ret = parse_init(&parse_result))) {
    } else {
      ret = parse_sql(&parse_result, sql, strlen(sql));
      parse_terminate(&parse_result);
    }
    return ret;
  }
};

// Test UTF8 fullwidth conversion
TEST_F(TestFullWidthSymbols, test_utf8_fullwidth_symbols_conversion)
{
  ObFullWidthSymbolConverter converter(&allocator_, CHARSET_UTF8MB4);

  // Test full-width space (U+3000)
  {
    // UTF8: E3 80 80
    const char input[] = "SELECT　""1";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT 1", out_str.ptr());
  }

  // Test full-width comma (U+FF0C)
  {
    // UTF8: EF BC 8C
    const char input[] = "SELECT 1，""2";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT 1,2", out_str.ptr());
  }

  // Test full-width parentheses (U+FF08, U+FF09)
  {
    // UTF8: EF BC 88, EF BC 89
    const char input[] = "SELECT（""1）";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT(1)", out_str.ptr());
  }

  // Test full-width operators (U+FF0B +, U+FF0D -, U+FF0A *, U+FF0F /)
  {
    const char input[] = "SELECT 1＋""2－""3＊""4／""5";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT 1+2-3*4/5", out_str.ptr());
  }

  // Test full-width comparison operators (U+FF1C <, U+FF1D =, U+FF1E >)
  {
    const char input[] = "SELECT 1＜""2＝""3＞""4";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT 1<2=3>4", out_str.ptr());
  }
}

// Test that characters in string literals are NOT converted
TEST_F(TestFullWidthSymbols, test_preserve_string_literals)
{
  ObFullWidthSymbolConverter converter(&allocator_, CHARSET_UTF8MB4);

  // Full-width space inside single-quoted string should be preserved
  {
    const char input[] = "SELECT '　'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    // String content should be preserved
    EXPECT_STREQ("SELECT '　'", out_str.ptr());
  }

  // Full-width operators inside double-quoted string should be preserved
  {
    const char input[] = "SELECT \"＋－\"";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT \"＋－\"", out_str.ptr());
  }

  // Full-width characters inside National string literal (N'...') should be preserved
  {
    const char input[] = "SELECT N'　'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT N'　'", out_str.ptr());
  }

  // Full-width characters inside lowercase National string literal (n'...') should be preserved
  {
    const char input[] = "SELECT n'　'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT n'　'", out_str.ptr());
  }

  // Full-width characters inside National Q-quote string (NQ'...') should be preserved
  {
    const char input[] = "SELECT NQ'[　]'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT NQ'[　]'", out_str.ptr());
  }

  // Full-width characters inside lowercase National Q-quote string (nq'...') should be preserved
  {
    const char input[] = "SELECT nq'{　}'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT nq'{　}'", out_str.ptr());
  }

  // Full-width characters inside National Q-quote string (nQ'...') should be preserved
  {
    const char input[] = "SELECT nQ'{　}'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT nQ'{　}'", out_str.ptr());
  }

  // Full-width characters inside National Q-quote string (Nq'...') should be preserved
  {
    const char input[] = "SELECT Nq'{　}'";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT Nq'{　}'", out_str.ptr());
  }
}

// Test that characters in comments are NOT converted
TEST_F(TestFullWidthSymbols, test_preserve_comments)
{
  ObFullWidthSymbolConverter converter(&allocator_, CHARSET_UTF8MB4);

  // SQL comment
  {
    const char input[] = "SELECT 1 -- 　 comment\nFROM dual";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    // Comment content should be preserved
    EXPECT_STREQ("SELECT 1 -- 　 comment\nFROM dual", out_str.ptr());
  }

  // C-style comment
  {
    const char input[] = "SELECT /* 　 */ 1";
    ObString in_str(strlen(input), input);
    ObString out_str;
    ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
    EXPECT_STREQ("SELECT /* 　 */ 1", out_str.ptr());
  }
}

// Test convert() reuses input pointer when no conversion is needed
TEST_F(TestFullWidthSymbols, test_convert_reuses_input_ptr_without_fullwidth_symbols)
{
  ObFullWidthSymbolConverter converter(&allocator_, CHARSET_UTF8MB4);

  const char input[] = "SELECT 1 FROM dual";
  ObString in_str(strlen(input), input);
  ObString out_str;
  ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
  EXPECT_EQ(in_str.ptr(), out_str.ptr());
  EXPECT_EQ(in_str.length(), out_str.length());
}

// Test position mapping
TEST_F(TestFullWidthSymbols, test_position_mapping)
{
  FullWidthSymbolPosMapping mapping;

  // Add some entries
  // Original: "A全角B" where 全角 is 3 bytes, converted to ASCII " "
  // Position 0: 'A' -> 'A' (no change)
  // Position 1-3: full-width space (3 bytes) -> ' ' (1 byte)
  // Position 4: 'B' -> 'B' (at position 2 after conversion)
  ASSERT_EQ(OB_SUCCESS, mapping.add_entry(1, 1, 3, 1)); // fullwidth space at original pos 1

  // Test position conversion
  EXPECT_EQ(0, mapping.conv_to_orig_pos(0, false));  // 'A' stays at 0
  EXPECT_EQ(1, mapping.conv_to_orig_pos(1, false));  // ' ' maps to start of full width at 1
  EXPECT_EQ(4, mapping.conv_to_orig_pos(2, false));  // 'B' maps to original pos 4
}

// Test convert() output with fullwidth symbols
TEST_F(TestFullWidthSymbols, test_convert_with_fullwidth_symbols)
{
  ObFullWidthSymbolConverter converter(&allocator_, CHARSET_UTF8MB4);

  const char input[] = "A　""B";
  ObString in_str(strlen(input), input);
  ObString out_str;
  ASSERT_EQ(OB_SUCCESS, converter.convert(in_str, out_str));
  EXPECT_STREQ("A B", out_str.ptr());
  EXPECT_NE(in_str.ptr(), out_str.ptr());
  EXPECT_TRUE(converter.has_conversions());
}

TEST_F(TestFullWidthSymbols, test_bridge_preprocess_and_postprocess)
{
  ParseResult parse_result;
  memset(&parse_result, 0, sizeof(ParseResult));
  parse_result.malloc_pool_ = &allocator_;
  parse_result.sql_mode_ = SMO_ORACLE;
  parse_result.connection_collation_ = CS_TYPE_UTF8MB4_BIN;

  const char input[] = "A，""B";
  const char *processed_sql = nullptr;
  int32_t processed_len = 0;
  void *full_width_sym_converter = nullptr;

  ASSERT_EQ(OB_SUCCESS,
            sql_parser_preprocess_fullwidth_symbols(&parse_result,
                                                    input,
                                                    strlen(input),
                                                    &processed_sql,
                                                    &processed_len,
                                                    &full_width_sym_converter));
  ASSERT_NE(nullptr, processed_sql);
  EXPECT_STREQ("A,B", processed_sql);
  ASSERT_NE(nullptr, full_width_sym_converter);

  // Simulate parser error position on converted SQL: column 3 points to 'B'.
  parse_result.start_col_ = 3;
  parse_result.end_col_ = 3;
  sql_parser_postprocess_fullwidth_symbols(&parse_result, full_width_sym_converter, processed_len);

  // On original SQL, 'B' is shifted by +2 bytes because ， is 3-byte full width.
  EXPECT_EQ(5, parse_result.start_col_);
  EXPECT_EQ(5, parse_result.end_col_);
}

TEST_F(TestFullWidthSymbols, test_parse_sql_error_location_uses_original_sql_position)
{
  ParseResult converted_result;
  ParseResult original_result;

  const char converted_sql[] = "SELECT 1, \\ FROM dual";
  const char original_sql[] = "SELECT 1， \\ FROM dual";

  const int converted_ret = parse_sql_for_test(converted_sql, converted_result);
  const int original_ret = parse_sql_for_test(original_sql, original_result);

  ASSERT_NE(OB_SUCCESS, converted_ret);
  ASSERT_NE(OB_SUCCESS, original_ret);
  EXPECT_EQ(converted_result.start_col_ + 2, original_result.start_col_);
  EXPECT_EQ(converted_result.end_col_ + 2, original_result.end_col_);
}

// Test PL paren level delta calculation for full-width parentheses/brackets.
TEST_F(TestFullWidthSymbols, test_get_pl_parenlevel_delta_for_fullwidth_symbols)
{
  const int coll = CS_TYPE_UTF8MB4_BIN;

  // Typical balanced full-width parens/brackets => delta 0
  {
    const char sql[] = "SELECT （1） + ［2］ FROM dual";
    const int32_t len = static_cast<int32_t>(strlen(sql));
    EXPECT_EQ(0, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, len, coll));
  }

  // One extra opening full-width parenthesis => positive delta
  {
    const char sql[] = "BEGIN （1 + 2";
    const int32_t len = static_cast<int32_t>(strlen(sql));
    EXPECT_EQ(1, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, len, coll));
  }

  // One extra closing full-width parenthesis => negative delta
  {
    const char sql[] = "BEGIN 1 + 2）";
    const int32_t len = static_cast<int32_t>(strlen(sql));
    EXPECT_EQ(-1, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, len, coll));
  }

  // Mixed full-width and ASCII parens/brackets: only full-width contributes
  {
    const char sql[] = "BEGIN (（a + [b]） + c) END";
    const int32_t len = static_cast<int32_t>(strlen(sql));
    EXPECT_EQ(0, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, len, coll));
  }

  // Range-limited scan: full SQL is balanced, subrange is not
  {
    const char sql[] = "AA（BB（CC）DD）EE";
    const int32_t len = static_cast<int32_t>(strlen(sql));
    EXPECT_EQ(0, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, len, coll));

    // Scan only "AA（BB（CC" => two opens, zero closes => +2
    const int32_t last_col = static_cast<int32_t>(strlen("AA（BB（CC"));
    EXPECT_EQ(2, get_fullwidth_parenlevel_delta_for_pl(sql, len, 1, last_col, coll));
  }
}

int main(int argc, char **argv)
{
  GCONF._enable_sql_parse_fullwidth_symbols = true;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
