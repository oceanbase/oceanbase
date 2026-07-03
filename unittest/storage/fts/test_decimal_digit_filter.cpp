/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "storage/fts/analyzer/filter/ob_decimal_digit_filter.h"
#include "unittest/storage/fts/ob_fts_filter_unittest_mock.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace storage
{

TEST(DecimalDigitFilterTest, fullwidth_and_thai_digits)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  // Fullwidth ０ (U+FF10) + Thai digit ๐ (U+0E50)
  mock.push_token(reinterpret_cast<const char *>(u8"a\uFF10\u0E50b"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  const char expect[] = {'a', '0', '0', 'b'};
  fts_filter_test_assert_token_text(tok, expect, 4);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST(DecimalDigitFilterTest, arabic_indic_digits)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  // Arabic-Indic digits ١٢٣ (U+0661–U+0663)
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"x\u0661\u0662\u0663y"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, "x123y", 5);
}

TEST(DecimalDigitFilterTest, devanagari_and_ascii_mixed)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  // Devanagari digit २ (U+0968), then ASCII "B7"
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"A\u0968" "B7"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, "A2B7", 4);
}

TEST(DecimalDigitFilterTest, ascii_unchanged)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("x12y", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "x12y", 4);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST(DecimalDigitFilterTest, skip_invalid_upstream_token)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token(nullptr, 0);
  mock.push_token("x12y", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "x12y", 4);
  ASSERT_EQ(1, tok.pos_inc_);
}

TEST(DecimalDigitFilterTest, init_wrong_type)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObTokenFilterSpec spec;
  spec.type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.init(spec, allocator));
}

TEST(DecimalDigitFilterTest, init_twice_fails)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
  ASSERT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
}

TEST(DecimalDigitFilterTest, get_next_without_upstream)
{
  common::ObArenaAllocator allocator;
  ObDecimalDigitFilter filter;
  ObDecimalDigitFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObTokenAttr tok;
  ASSERT_EQ(OB_NOT_INIT, filter.get_next_token(tok));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
