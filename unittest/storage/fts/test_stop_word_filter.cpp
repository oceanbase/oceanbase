/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "storage/fts/analyzer/filter/ob_stop_word_filter.h"
#include "unittest/storage/fts/ob_fts_filter_unittest_mock.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace storage
{

class StopWordFilterTest : public ::testing::Test
{};

TEST_F(StopWordFilterTest, english_builtin_skips_the)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("the", 0);
  mock.push_token("run", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "run", 3);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, english_skipped_stopwords_fold_upstream_pos_inc)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("foo", 0);
  mock.push_token("the", 1);
  mock.push_token("bar", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "foo", 3);
  ASSERT_EQ(0, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "bar", 3);
  ASSERT_EQ(2, tok.pos_inc_);

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, english_uppercase_stopword_filtered_by_case_insensitive_collation)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("THE", 0);
  mock.push_token("news", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "news", 4);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, english_keeps_content_token)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("oceanbase", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "oceanbase", 9);
}

TEST_F(StopWordFilterTest, english_empty_language_defaults_to_english)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  // language_ LANGUAGE_INVALID -> defaults to English builtin (same as historical "_english_" preset)
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("the", 0);
  mock.push_token("cat", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "cat", 3);
}

TEST_F(StopWordFilterTest, thai_builtin_skips_static_stopword)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_THAI;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  // "ใน" is in phase-1 built-in Thai list
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"ใน"), 0);
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"ไก่"), 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, reinterpret_cast<const char *>(u8"ไก่"), 9);
}

TEST_F(StopWordFilterTest, vietnamese_language_noops_without_builtin_stopwords)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_VIETNAMESE;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"và"), 0);
  mock.push_token("hanoi", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, reinterpret_cast<const char *>(u8"và"), 3);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "hanoi", 5);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, vietnamese_noop_keeps_upstream_pos_inc)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_VIETNAMESE;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("saigon", 0);
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"và"), 1);
  mock.push_token_utf8(reinterpret_cast<const char *>(u8"không"), 1);
  mock.push_token("delta", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "saigon", 6);
  ASSERT_EQ(0, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, reinterpret_cast<const char *>(u8"và"), 3);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_utf8(tok, reinterpret_cast<const char *>(u8"không"), 6);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "delta", 5);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, vietnamese_keeps_content_token)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_VIETNAMESE;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("oceanbase", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "oceanbase", 9);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, indonesian_builtin_skips_lucene_stopword)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_INDONESIAN;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("yang", 0);
  mock.push_token("jakarta", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "jakarta", 7);
}

TEST_F(StopWordFilterTest, indonesian_uppercase_stopword_filtered_by_case_insensitive_collation)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_INDONESIAN;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("YANG", 0);
  mock.push_token("berita", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "berita", 6);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, indonesian_skipped_stopwords_fold_upstream_pos_inc)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_INDONESIAN;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("nusantara", 0);
  mock.push_token("dan", 1);
  mock.push_token("yang", 1);
  mock.push_token("digital", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "nusantara", 9);
  ASSERT_EQ(0, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "digital", 7);
  ASSERT_EQ(3, tok.pos_inc_);

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, indonesian_all_stopwords_returns_iter_end)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_INDONESIAN;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("dan", 0);
  mock.push_token("yang", 1);
  mock.push_token("tidak", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, malay_language_noops_without_builtin_stopwords)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_MALAY;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("dan", 0);
  mock.push_token("dokumen", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "dan", 3);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "dokumen", 7);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, malay_noop_keeps_uppercase_token)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_MALAY;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("DAN", 0);
  mock.push_token("dokumen", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "DAN", 3);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "dokumen", 7);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, malay_noop_keeps_upstream_pos_inc)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_MALAY;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("borneo", 0);
  mock.push_token("dan", 1);
  mock.push_token("yang", 1);
  mock.push_token("projek", 1);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "borneo", 6);
  ASSERT_EQ(0, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "dan", 3);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "yang", 4);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "projek", 6);
  ASSERT_EQ(1, tok.pos_inc_);

  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, malay_keeps_content_token)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_MALAY;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("dokumen", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "dokumen", 7);
  ASSERT_EQ(OB_ITER_END, filter.get_next_token(tok));
}

TEST_F(StopWordFilterTest, none_language_disables_filtering)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_NONE;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  ObFtsFilterTestMockTokenStream mock;
  mock.push_token("the", 0);
  filter.set_input(&mock);

  ObTokenAttr tok;
  ASSERT_EQ(OB_SUCCESS, filter.get_next_token(tok));
  fts_filter_test_assert_token_text(tok, "the", 3);
}

TEST_F(StopWordFilterTest, custom_stopword_table_not_supported_yet)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.stopword_table_ = common::ObString("db.t1");
  ASSERT_EQ(OB_NOT_SUPPORTED, filter.init(spec, allocator));
}

TEST_F(StopWordFilterTest, init_wrong_spec_type)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObTokenFilterSpec bad_spec;
  bad_spec.type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.init(bad_spec, allocator));
}

TEST_F(StopWordFilterTest, init_twice_fails)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
  spec.language_ = ObStopWordLanguageKind::LANGUAGE_ENGLISH;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
  ASSERT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
}

TEST_F(StopWordFilterTest, get_next_without_upstream)
{
  common::ObArenaAllocator allocator;
  ObStopWordFilter filter;
  ObStopWordFilterSpec spec;
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
