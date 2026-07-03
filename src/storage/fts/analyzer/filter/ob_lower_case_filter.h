/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_LOWER_CASE_FILTER_H_
#define OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_LOWER_CASE_FILTER_H_

#include "lib/allocator/page_arena.h"
#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

// Token filter config: lowercase only; token text is UTF-8 (utf8mb4), no per-filter collation.
struct ObLowerCaseFilterSpec : public ObTokenFilterSpec
{
  ObLowerCaseFilterSpec()
  {
    type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE;
  }
};

// Unicode case folding via ObCharset::tolower on UTF-8; output buffers come from an internal arena
// (reuse per token, reset on filter reset) to avoid unbounded growth on long-lived upstream allocators.
class ObLowerCaseFilter : public ObITokenFilter
{
public:
  ObLowerCaseFilter();
  ~ObLowerCaseFilter() override;

  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;

private:
  bool is_inited_;
  common::ObArenaAllocator tolower_arena_;

  DISALLOW_COPY_AND_ASSIGN(ObLowerCaseFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_LOWER_CASE_FILTER_H_
