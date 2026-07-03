/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_STORAGE_FTS_OB_FTS_FILTER_UNITTEST_MOCK_H_
#define OCEANBASE_UNITTEST_STORAGE_FTS_OB_FTS_FILTER_UNITTEST_MOCK_H_

#include "lib/ob_errno.h"
#include "storage/fts/analyzer/ob_token_stream.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

namespace oceanbase
{
namespace storage
{

struct ObFtsFilterTestMockTokenEntry final
{
  std::string text_;
  int32_t pos_inc_;
};

class ObFtsFilterTestMockTokenStream final : public ObITokenStream
{
public:
  ObFtsFilterTestMockTokenStream() : next_idx_(0) {}

  void clear()
  {
    entries_.clear();
    next_idx_ = 0;
  }

  void push_token(const char *utf8_text, const int32_t pos_inc)
  {
    ObFtsFilterTestMockTokenEntry e;
    e.text_ = utf8_text != nullptr ? utf8_text : "";
    e.pos_inc_ = pos_inc;
    entries_.push_back(std::move(e));
  }

  void push_token_utf8(const char *utf8_text, const int32_t pos_inc)
  {
    ObFtsFilterTestMockTokenEntry e;
    e.text_ = utf8_text != nullptr ? utf8_text : "";
    e.pos_inc_ = pos_inc;
    entries_.push_back(std::move(e));
  }

  int get_next_token(ObTokenAttr &token) override
  {
    int ret = OB_SUCCESS;
    if (next_idx_ >= entries_.size()) {
      ret = OB_ITER_END;
    } else {
      const ObFtsFilterTestMockTokenEntry &e = entries_[next_idx_];
      token.token_ptr_ = e.text_.empty() ? nullptr : e.text_.c_str();
      token.token_len_ = static_cast<int32_t>(e.text_.size());
      token.pos_inc_ = e.pos_inc_;
      token.is_keyword_ = false;
      ++next_idx_;
    }
    return ret;
  }

  void reset() override { next_idx_ = 0; }

private:
  std::vector<ObFtsFilterTestMockTokenEntry> entries_;
  size_t next_idx_;
};

inline void fts_filter_test_assert_token_text(const ObTokenAttr &tok,
                                              const char *expected_utf8,
                                              const int32_t expected_len)
{
  ASSERT_TRUE(tok.is_valid());
  ASSERT_EQ(expected_len, tok.token_len_);
  ASSERT_EQ(0, memcmp(expected_utf8, tok.token_ptr_, static_cast<size_t>(expected_len)));
}

inline void fts_filter_test_assert_token_utf8(const ObTokenAttr &tok,
                                              const char *expected_utf8,
                                              const int32_t expected_byte_len)
{
  ASSERT_TRUE(tok.is_valid());
  ASSERT_EQ(expected_byte_len, tok.token_len_);
  ASSERT_EQ(0, memcmp(expected_utf8, tok.token_ptr_, static_cast<size_t>(expected_byte_len)));
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_FTS_OB_FTS_FILTER_UNITTEST_MOCK_H_
