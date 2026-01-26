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


// Original author: liuzhenlong.lzl <> (137ca4f6dbd9)
// This file extracted StringSearcher from ob_expr_like.h/ob_expr_like.cpp to create a reusable class.

#ifndef OCEANBASE_SQL_ENGINE_EXPR_STRING_SEARCHER_
#define OCEANBASE_SQL_ENGINE_EXPR_STRING_SEARCHER_

#if defined(__x86_64__)
#include "immintrin.h"
#endif
#include "common/ob_target_specific.h"

namespace oceanbase
{
namespace common
{
OB_DECLARE_AVX2_SPECIFIC_CODE(
class ObStringSearcher {
private:
  static constexpr int AVX2_SIZE = sizeof(__m256i);

public:
  ObStringSearcher()
      : pattern_(nullptr), pattern_end_(nullptr), pattern_len_(0) {}
  int init(const char *pattern, size_t len);
  const char *get_pattern() { return pattern_; }
  const char *get_patterne_end() { return pattern_end_; }
  size_t get_pattern_length() { return pattern_len_; }

  int instr(const char *text, const char *text_end, int64_t &res, bool &find) const;
  int is_substring(const char *text, const char *text_end, bool &res) const;
  int start_with(const char *text, const char *text_end, bool &res) const;
  int end_with(const char *text, const char *text_end, bool &res) const;
  int equal(const char *text, const char *text_end, bool &res) const;
  bool memequal_opt(const char *s1, const char *s2, size_t n) const;

private:
  template <typename T>
  bool memequal_plain(const char *p1, const char *p2) const;
  template <int cnt>
  bool memequal_sse(const char *p1, const char *p2) const;

  // string to be searched for
  const char *pattern_;
  const char *pattern_end_;
  size_t pattern_len_;
  // first or last byte of `pattern_`
  uint8_t first_;
  uint8_t last_;
  // vector filled `first_` or `last_`
  __m256i vfirst_;
  __m256i vlast_;
};
)

int utf8_loc_to_locb(const char *str, size_t length, uint32_t loc, int64_t &locb);
int utf8_locb_to_loc(const char *str, size_t length, int64_t locb, uint32_t &loc);

}
}

#endif