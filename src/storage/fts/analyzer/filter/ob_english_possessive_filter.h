/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_ENGLISH_POSSESSIVE_FILTER_H_
#define OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_ENGLISH_POSSESSIVE_FILTER_H_

#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

// Token filter config: English possessive stripping only; token text is UTF-8 (utf8mb4).
struct ObEnglishPossessiveFilterSpec : public ObTokenFilterSpec
{
  ObEnglishPossessiveFilterSpec()
  {
    type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_ENGLISH_POSSESSIVE;
  }
};

// Removes English possessive suffixes: ASCII ', U+2019 (typographic apostrophe), U+FF07 (fullwidth),
// each with optional s/S, or a trailing apostrophe alone (e.g. John's, John<U+2019>s, dogs', dogs<U+FF07>).
// Run before lowercasing/stemming.
class ObEnglishPossessiveFilter : public ObITokenFilter
{
public:
  ObEnglishPossessiveFilter();
  ~ObEnglishPossessiveFilter() override;

  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;

private:
  static bool calc_stripped_len(const char *data, const int32_t len, int32_t &new_len);

  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObEnglishPossessiveFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_ENGLISH_POSSESSIVE_FILTER_H_
