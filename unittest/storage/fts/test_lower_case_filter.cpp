/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "storage/fts/analyzer/filter/ob_lower_case_filter.h"
#include "unittest/storage/fts/ob_fts_filter_unittest_mock.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace storage
{

TEST(LowerCaseFilterTest, ascii_lower)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObLowerCaseFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("HeLLo", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "hello", 5);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST(LowerCaseFilterTest, utf8_accent_and_cjk_mix)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObLowerCaseFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"ÄB中"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, reinterpret_cast<const char *>(u8"äb中"), 6);
}

TEST(LowerCaseFilterTest, skip_invalid_upstream_token)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObLowerCaseFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token(nullptr, 0);
  mock.push_token("HeLLo", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "hello", 5);
  ASSERT_EQ(1, tok.pos_inc_);
}

TEST(LowerCaseFilterTest, init_wrong_type)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObTokenFilterSpec spec;
  spec.type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_ENGLISH_POSSESSIVE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.init(spec, allocator));
}

TEST(LowerCaseFilterTest, init_twice_fails)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObLowerCaseFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
  ASSERT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
}

TEST(LowerCaseFilterTest, get_next_without_upstream)
{
  common::ObArenaAllocator allocator;
  ObLowerCaseFilter filter;
  ObLowerCaseFilterSpec spec;
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
