/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_CHARSET_CONVERT_FILTER_H_
#define OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_CHARSET_CONVERT_FILTER_H_

#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "storage/fts/analyzer/ob_i_token_filter.h"

namespace oceanbase
{
namespace storage
{

// Token filter that converts each token from utf8mb4_bin (analyzer pipeline working
// charset) back to the source column collation. Paired with ObUtf8mb4BinCharFilter,
// which normalises input to utf8mb4_bin at the pipeline head. This filter must be the
// LAST token filter in the pipeline so all upstream filters operate on utf8mb4 bytes.
//
// target_collation_ is left as CS_TYPE_INVALID in the spec at DDL time; the token
// stream factory patches it with the runtime source collation before init.
struct ObCharsetConvertFilterSpec : public ObTokenFilterSpec
{
  common::ObCollationType target_collation_;
  ObCharsetConvertFilterSpec()
    : ObTokenFilterSpec(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT),
      target_collation_(common::CS_TYPE_INVALID)
  {}
  VIRTUAL_TO_STRING_KV(K_(type), K_(target_collation));
};

class ObCharsetConvertFilter : public ObITokenFilter
{
public:
  ObCharsetConvertFilter();
  ~ObCharsetConvertFilter() override;

  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;

private:
  bool is_inited_;
  common::ObCollationType target_collation_;
  common::ObArenaAllocator convert_arena_;

  DISALLOW_COPY_AND_ASSIGN(ObCharsetConvertFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_ANALYZER_FILTER_OB_CHARSET_CONVERT_FILTER_H_
