/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LEGACY_CHAR_FILTER_H_
#define OCEANBASE_STORAGE_OB_LEGACY_CHAR_FILTER_H_

#include "lib/allocator/page_arena.h"
#include "storage/fts/analyzer/ob_i_char_filter.h"

namespace oceanbase
{
namespace storage
{

struct ObLegacyLowercaseCharFilterSpec : public ObCharFilterSpec
{
  common::ObCollationType coll_type_;  // collation type for tolower conversion
  ObLegacyLowercaseCharFilterSpec() : ObCharFilterSpec(), coll_type_(common::CS_TYPE_INVALID)
  { type_ = ObCharFilterType::CHAR_FILTER_TYPE_LOWERCASE_LEGACY; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(coll_type));
};

class ObLegacyLowercaseCharFilter : public ObICharFilter
{
public:
  ObLegacyLowercaseCharFilter();
  virtual ~ObLegacyLowercaseCharFilter() = default;
  virtual int init(const ObCharFilterSpec &spec, common::ObIAllocator &alloc) override;
  virtual int filter(const char *input, const int64_t input_len,
                     const char *&output, int64_t &output_len) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(coll_type));
private:
  bool is_inited_;
  common::ObCollationType coll_type_;
  // Self-owned arena reused on every filter() call. Required because the analyzer
  // (and therefore this filter) is reused across rows in DDL segment paths; sharing
  // the analyzer's scratch_alloc_ would let tolower buffers accumulate indefinitely
  // alongside the tokenizer's long-lived state (e.g. IK dict caches).
  common::ObArenaAllocator tolower_arena_;
  DISALLOW_COPY_AND_ASSIGN(ObLegacyLowercaseCharFilter);
};

struct ObUtf8mb4BinCharFilterSpec : public ObCharFilterSpec
{
  // Source collation of the original indexed text. The filter converts from this collation
  // to utf8mb4_bin before non-legacy analyzer tokenization.
  common::ObCollationType src_collation_;
  ObUtf8mb4BinCharFilterSpec()
    : ObCharFilterSpec(),
      src_collation_(common::CS_TYPE_INVALID)
  { type_ = ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(src_collation));
};

// First char filter for non-legacy analyzers. It normalizes the input bytes to utf8mb4_bin
// so downstream analyzer tokenizers can operate on a single charset/collation.
//
// Output lifetime: when charset conversion is required, filter() allocates the converted
// buffer from a self-owned arena that is reused on every filter() call. The output is valid
// until the next filter() invocation on the same instance.
class ObUtf8mb4BinCharFilter : public ObICharFilter
{
public:
  ObUtf8mb4BinCharFilter();
  virtual ~ObUtf8mb4BinCharFilter() = default;
  virtual int init(const ObCharFilterSpec &spec, common::ObIAllocator &alloc) override;
  virtual int filter(const char *input, const int64_t input_len,
                     const char *&output, int64_t &output_len) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(src_collation));
private:
  bool is_inited_;
  // Original collation captured at analyzer construction time; not passed through filter().
  common::ObCollationType src_collation_;
  // Self-owned arena reused on every filter() call. See the same rationale on
  // ObLegacyLowercaseCharFilter::tolower_arena_.
  common::ObArenaAllocator convert_arena_;
  DISALLOW_COPY_AND_ASSIGN(ObUtf8mb4BinCharFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LEGACY_CHAR_FILTER_H_
