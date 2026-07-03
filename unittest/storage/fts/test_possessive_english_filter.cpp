/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "storage/fts/analyzer/filter/ob_possessive_english_filter.h"
#include "storage/fts/analyzer/ob_token_stream.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

namespace oceanbase
{
namespace storage
{

namespace
{

struct MockTokenEntry final
{
  std::string text_;
  int32_t position_;
};

// Feeds a fixed sequence of tokens from owned std::string storage for testing pull chain.
class MockTokenStream final : public ObITokenStream
{
public:
  MockTokenStream() : next_idx_(0) {}

  void clear()
  {
    entries_.clear();
    next_idx_ = 0;
  }

  void push_token(const char *ascii_text, const int32_t position)
  {
    MockTokenEntry e;
    e.text_ = ascii_text != nullptr ? ascii_text : "";
    e.position_ = position;
    entries_.push_back(std::move(e));
  }

  void push_token_utf8(const char *utf8_text, const int32_t position)
  {
    MockTokenEntry e;
    e.text_ = utf8_text != nullptr ? utf8_text : "";
    e.position_ = position;
    entries_.push_back(std::move(e));
  }

  int get_next_token(ObTokenAttr &token) override
  {
    int ret = OB_SUCCESS;
    if (next_idx_ >= entries_.size()) {
      ret = OB_ITER_END;
    } else {
      const MockTokenEntry &e = entries_[next_idx_];
      token.token_ptr_ = e.text_.empty() ? nullptr : e.text_.c_str();
      token.token_len_ = static_cast<int32_t>(e.text_.size());
      token.pos_inc_ = e.position_;
      token.is_keyword_ = false;
      ++next_idx_;
    }
    return ret;
  }

  void reset() override { next_idx_ = 0; }

private:
  std::vector<MockTokenEntry> entries_;
  size_t next_idx_;
};

static void assert_token_text(const ObTokenAttr &tok, const char *expected_ascii)
{
  ASSERT_TRUE(tok.is_valid());
  ASSERT_EQ(strlen(expected_ascii), static_cast<size_t>(tok.token_len_));
  ASSERT_EQ(0, memcmp(expected_ascii, tok.token_ptr_, static_cast<size_t>(tok.token_len_)));
}

static void assert_token_utf8(const ObTokenAttr &tok, const char *expected_utf8, const int32_t expected_byte_len)
{
  ASSERT_TRUE(tok.is_valid());
  ASSERT_EQ(expected_byte_len, tok.token_len_);
  ASSERT_EQ(0, memcmp(expected_utf8, tok.token_ptr_, static_cast<size_t>(expected_byte_len)));
}

} // namespace

TEST(PossessiveEnglishFilterTest, strip_apostrophe_s)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("John's", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "John");

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST(PossessiveEnglishFilterTest, strip_apostrophe_s_uppercase_s)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("John'S", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "John");
}

TEST(PossessiveEnglishFilterTest, strip_unicode_u2019_apostrophe_s)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  // "John" + U+2019 RIGHT SINGLE QUOTATION MARK + "s"
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"John\u2019s"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, "John", 4);

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

// U+FF07 FULLWIDTH APOSTROPHE ＇ (e.g. CJK input method)
TEST(PossessiveEnglishFilterTest, strip_fullwidth_uff07_apostrophe_s)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"John\uFF07s"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, "John", 4);
}

TEST(PossessiveEnglishFilterTest, strip_unicode_u2019_trailing_plural_possessive)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"dogs\u2019"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, "dogs", 4);
}

TEST(PossessiveEnglishFilterTest, strip_trailing_fullwidth_uff07_plural)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"dogs\uFF07"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, "dogs", 4);
}

TEST(PossessiveEnglishFilterTest, strip_unicode_u2019_apostrophe_s_after_multibyte_letter)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  // "Émile" + U+2019 + "s"
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"\u00C9mile\u2019s"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, reinterpret_cast<const char *>(u8"\u00C9mile"), 6);
}

TEST(PossessiveEnglishFilterTest, unchanged_when_u2019_inside_word_not_suffix)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"rock\u2019n\u2019roll"), 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, reinterpret_cast<const char *>(u8"rock\u2019n\u2019roll"), 15);
}

TEST(PossessiveEnglishFilterTest, strip_trailing_apostrophe_plural_possessive)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("dogs'", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "dogs");
}

TEST(PossessiveEnglishFilterTest, unchanged_plain_word)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("hello", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "hello");
}

TEST(PossessiveEnglishFilterTest, unchanged_malformed_u2019_suffix_bytes)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  // Ends with incomplete U+2019 bytes (E2 80), should not be stripped.
  const char *malformed = "a\xe2\x80";
  mock.push_token_utf8(malformed, 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_utf8(tok, malformed, 3);
}

TEST(PossessiveEnglishFilterTest, strip_two_char_apostrophe_s_and_skip_empty_result)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("'s", 0);
  mock.push_token("dogs", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "dogs");
  ASSERT_EQ(1, tok.pos_inc_);
}

TEST(PossessiveEnglishFilterTest, skip_invalid_upstream_token)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token(nullptr, 0);
  mock.push_token("John's", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "John");
  ASSERT_EQ(1, tok.pos_inc_);
}

TEST(PossessiveEnglishFilterTest, empty_after_strip_folds_pos_inc)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("'s", 1);
  mock.push_token("dogs", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "dogs");
  ASSERT_EQ(2, tok.pos_inc_);
}

TEST(PossessiveEnglishFilterTest, multiple_tokens_sequence)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  MockTokenStream mock;
  mock.push_token("John's", 0);
  mock.push_token("runs", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "John");
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  assert_token_text(tok, "runs");
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST(PossessiveEnglishFilterTest, init_wrong_spec_type)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObTokenFilterSpec bad_spec;
  bad_spec.type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.init(bad_spec, allocator));
}

TEST(PossessiveEnglishFilterTest, init_twice_fails)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
  ASSERT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
}

TEST(PossessiveEnglishFilterTest, get_next_without_upstream)
{
  common::ObArenaAllocator allocator;
  ObPossessiveEnglishFilter filter;
  ObPossessiveEnglishFilterSpec spec;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObTokenAttr tok;
  ASSERT_EQ(OB_NOT_INIT, filter.get_next_token(tok));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
