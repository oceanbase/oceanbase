/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_DECIMAL_DIGIT_FILTER_H_
#define OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_DECIMAL_DIGIT_FILTER_H_

#include "lib/allocator/page_arena.h"
#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

// Token filter config: decimal-digit normalization only; token text is UTF-8 (utf8mb4).
struct ObDecimalDigitFilterSpec : public ObTokenFilterSpec
{
  ObDecimalDigitFilterSpec()
  {
    type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_DECIMAL_DIGIT;
  }
};

// Normalizes Unicode decimal digits to ASCII 0–9 (ICU u_digit); UTF-8 buffer on internal arena
// (reuse per token, reset on filter reset).
class ObDecimalDigitFilter : public ObITokenFilter
{
public:
  ObDecimalDigitFilter();
  ~ObDecimalDigitFilter() override;

  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;

private:
  bool is_inited_;
  common::ObArenaAllocator normalize_arena_;

  DISALLOW_COPY_AND_ASSIGN(ObDecimalDigitFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_DECIMAL_DIGIT_FILTER_H_
