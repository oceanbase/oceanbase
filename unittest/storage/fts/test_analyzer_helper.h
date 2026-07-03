/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef UNITTEST_STORAGE_FTS_TEST_ANALYZER_HELPER_H_
#define UNITTEST_STORAGE_FTS_TEST_ANALYZER_HELPER_H_

#include <gtest/gtest.h>

#include <cstring>
#include <initializer_list>
#include <iostream>
#include <string>
#include <vector>

#include "lib/ob_errno.h"
#include "storage/fts/analyzer/ob_i_token_filter.h"
#include "storage/fts/analyzer/ob_i_tokenizer.h"

namespace oceanbase
{
namespace storage
{

struct MockTokenEntry
{
  std::string text_;
  int32_t pos_inc_;
  bool is_keyword_;
};

class MockTokenStream : public ObITokenStream
{
public:
  MockTokenStream()
    : entries_(),
      next_idx_(0)
  {}

  explicit MockTokenStream(std::initializer_list<const char *> items)
    : entries_(),
      next_idx_(0)
  {
    set_tokens(items);
  }

  void set_tokens(std::initializer_list<const char *> items)
  {
    entries_.clear();
    entries_.reserve(items.size());
    for (const char *item : items) {
      MockTokenEntry entry;
      entry.text_ = item == nullptr ? "" : item;
      entry.pos_inc_ = 1;
      entry.is_keyword_ = false;
      entries_.push_back(entry);
    }
    next_idx_ = 0;
  }

  void push_token(const char *item, const int32_t pos_inc, const bool is_keyword = false)
  {
    MockTokenEntry entry;
    entry.text_ = item == nullptr ? "" : item;
    entry.pos_inc_ = pos_inc;
    entry.is_keyword_ = is_keyword;
    entries_.push_back(entry);
  }

  int get_next_token(ObTokenAttr &token) override
  {
    int ret = OB_SUCCESS;
    token = ObTokenAttr();
    if (next_idx_ >= entries_.size()) {
      ret = OB_ITER_END;
    } else {
      const MockTokenEntry &entry = entries_[next_idx_++];
      token.token_ptr_ = entry.text_.empty() ? nullptr : entry.text_.data();
      token.token_len_ = static_cast<int32_t>(entry.text_.size());
      token.pos_inc_ = entry.pos_inc_;
      token.is_keyword_ = entry.is_keyword_;
    }
    return ret;
  }

  void reset() override
  {
    entries_.clear();
    next_idx_ = 0;
  }

private:
  std::vector<MockTokenEntry> entries_;
  size_t next_idx_;
};

class FTAnalyzerTestHelper
{
public:
  static void assert_tokenizer_output(const char *case_name,
                                      ObITokenizer &tokenizer,
                                      const char *text,
                                      std::initializer_list<const char *> expected,
                                      const ObCollationType coll_type = CS_TYPE_UTF8MB4_BIN)
  {
    std::vector<std::string> actual;
    const int64_t text_len = text == nullptr ? -1 : static_cast<int64_t>(std::strlen(text));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(text, text_len, coll_type))
        << "case=" << case_name;
    ASSERT_EQ(OB_SUCCESS, collect_stream_token_texts(tokenizer, actual))
        << "case=" << case_name;
    expect_token_texts_eq(case_name, actual, expected);
  }

  static void assert_token_filter_output(const char *case_name,
                                         ObITokenFilter &filter,
                                         std::initializer_list<const char *> input,
                                         std::initializer_list<const char *> expected)
  {
    std::vector<std::string> actual;
    MockTokenStream input_stream;
    input_stream.set_tokens(input);
    filter.set_input(&input_stream);
    ASSERT_EQ(OB_SUCCESS, collect_stream_token_texts(filter, actual))
        << "case=" << case_name;
    expect_token_texts_eq(case_name, actual, expected);
  }

private:
  static int collect_stream_token_texts(ObITokenStream &stream, std::vector<std::string> &tokens)
  {
    int ret = OB_SUCCESS;
    tokens.clear();
    ObTokenAttr token;
    int next_ret = stream.get_next_token(token);
    while (next_ret == OB_SUCCESS) {
      if (OB_ISNULL(token.token_ptr_) || token.token_len_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      tokens.push_back(std::string(token.token_ptr_, token.token_len_));
      next_ret = stream.get_next_token(token);
    }
    if (OB_SUCC(ret) && next_ret != OB_ITER_END) {
      ret = next_ret;
    }
    return ret;
  }

  static void expect_token_texts_eq(const char *case_name,
                                    const std::vector<std::string> &actual,
                                    std::initializer_list<const char *> expected)
  {
    if (actual.size() != expected.size()) {
      std::cout << "=== case: " << (case_name != nullptr ? case_name : "(null)") << "\n";
      for (int64_t i = 0; i < static_cast<int64_t>(actual.size()); ++i) {
        const std::string &tok = actual[static_cast<size_t>(i)];
        std::cout << "  [" << i << "] token=`" << tok << "`\n";
      }
      std::cout << std::flush;
    }
    ASSERT_EQ(expected.size(), actual.size()) << "case=" << case_name;
    size_t i = 0;
    for (const auto &exp : expected) {
      ASSERT_LT(i, actual.size()) << "case=" << case_name;
      EXPECT_EQ(std::string(exp), actual[i]) << "case=" << case_name << " token_idx=" << i;
      ++i;
    }
  }

};

} // namespace storage
} // namespace oceanbase

#endif // UNITTEST_STORAGE_FTS_TEST_ANALYZER_HELPER_H_
