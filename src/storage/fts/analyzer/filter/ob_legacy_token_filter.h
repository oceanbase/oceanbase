/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LEGACY_TOKEN_FILTER_H_
#define OCEANBASE_STORAGE_OB_LEGACY_TOKEN_FILTER_H_

#include "storage/fts/analyzer/ob_i_token_filter.h"
#include "storage/fts/ob_fts_stop_token_check.h"
#include "storage/fts/ob_fts_struct.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace storage
{

struct ObLegacyMinMaxTokenFilterSpec : public ObTokenFilterSpec
{
  int64_t min_token_size_;
  int64_t max_token_size_;
  common::ObCollationType coll_type_;  // collation for computing code_point_cnt via ObCharset::strlen_char
  ObLegacyMinMaxTokenFilterSpec()
    : ObTokenFilterSpec(), min_token_size_(0), max_token_size_(0), coll_type_(common::CS_TYPE_INVALID)
  { type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_MIN_MAX; }
  VIRTUAL_TO_STRING_KV(K_(type), K_(min_token_size), K_(max_token_size), K_(coll_type));
};

struct ObLegacyStopTokenFilterSpec : public ObTokenFilterSpec
{
  common::ObObjMeta token_meta_;  // carries collation type for hash/cmp/stop-checker init
  ObLegacyStopTokenFilterSpec() : ObTokenFilterSpec()
  { type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LEGACY_STOP; }
};

// Token filter that skips tokens whose code_point_cnt is outside [min, max] range.
// Used by legacy built-in parsers to enforce token length constraints.
class ObLegacyMinMaxTokenFilter : public ObITokenFilter
{
public:
  ObLegacyMinMaxTokenFilter()
    : is_inited_(false), alloc_(nullptr), coll_type_(common::CS_TYPE_INVALID),
      min_token_size_(0), max_token_size_(0) {}
  virtual ~ObLegacyMinMaxTokenFilter() = default;
  virtual int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  virtual int get_next_token(ObTokenAttr &token) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(min_token_size), K_(max_token_size));
  static constexpr int64_t MAX_CHAR_COUNT_PER_TOKEN = 1024;
private:
  bool is_out_of_range_(const int64_t code_point_cnt) const;
private:
  bool is_inited_;
  common::ObIAllocator *alloc_;
  common::ObCollationType coll_type_;
  int64_t min_token_size_;
  int64_t max_token_size_;
  DISALLOW_COPY_AND_ASSIGN(ObLegacyMinMaxTokenFilter);
};

// Token filter that skips stop words (e.g. "the", "a", "is").
// The stop token table, hash and cmp functions are initialized in init()
// from the token_meta_ carried in ObLegacyStopTokenFilterSpec.
class ObLegacyStopTokenFilter : public ObITokenFilter
{
public:
  ObLegacyStopTokenFilter()
    : is_inited_(false),
      alloc_(nullptr),
      coll_type_(common::CS_TYPE_INVALID),
      hash_func_(nullptr),
      cmp_func_(nullptr) {}
  virtual ~ObLegacyStopTokenFilter() = default;
  virtual int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  virtual int get_next_token(ObTokenAttr &token) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(coll_type));
private:
  bool is_inited_;
  common::ObIAllocator *alloc_;
  common::ObCollationType coll_type_;
  common::ObObjMeta token_meta_;
  sql::ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
  ObStopTokenChecker stop_checker_;
  DISALLOW_COPY_AND_ASSIGN(ObLegacyStopTokenFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LEGACY_TOKEN_FILTER_H_
