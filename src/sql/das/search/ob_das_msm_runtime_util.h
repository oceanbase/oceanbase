/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * DAS-layer minimum_should_match runtime utilities: ES-style spec evaluation
 * (ObDASMinShouldMatchSpec) plus RtDef helpers that combine eval datums with
 * runtime token / should / group counts (ObDASMSMRuntimeUtil).
 */

#ifndef OCEANBASE_SQL_OB_DAS_MSM_RUNTIME_UTIL_H_
#define OCEANBASE_SQL_OB_DAS_MSM_RUNTIME_UTIL_H_

#include "lib/ob_define.h"

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
struct ObDatum;
class ObIAllocator;
}  // namespace common

namespace sql
{

// DAS-runtime MSM spec evaluation (matches ES calculateMinShouldMatch behavior).
// optional_clause_count is token count, group count, or bool should clause count.
class ObDASMinShouldMatchSpec
{
public:
  static int string_calc(common::ObIAllocator &alloc,
                         const int64_t optional_clause_count,
                         const common::ObString &spec,
                         int64_t &msm_result);
  /// Pure integer MSM (same semantics as string_calc() when spec is a decimal integer string; non-recursive).
  static int int_calc(const int64_t optional_clause_count, const int64_t raw_int, int64_t &msm_out);
};

/// Helpers for DAS RtDef: map evaluated MSM datum + runtime counts to final msm count.
class ObDASMSMRuntimeUtil
{
public:
  /// DAS runtime: apply bool minimum_should_match from evaluated datum (string_calc vs int_calc).
  static int calc_bool_should_msm(
      common::ObIAllocator &alloc,
      const int64_t should_n,
      const bool is_msm_unresolved_expr,
      const common::ObDatum *msm_datum,
      int64_t &msm_out);

  /// DAS runtime: match() MSM from eval datum (string_calc vs int clamp to token_n).
  static int calc_match_msm(
      common::ObIAllocator &alloc,
      const int64_t token_n,
      const bool is_msm_unresolved_expr,
      const common::ObDatum *msm_datum,
      int64_t &msm_out);

  /// DAS runtime: multi_match / query_string MSM (string_calc then [1,n]; else int clamp [1,n]).
  static int calc_multi_or_query_string_msm(
      common::ObIAllocator &alloc,
      const int64_t n,
      const bool is_msm_unresolved_expr,
      const common::ObDatum *msm_datum,
      int64_t &msm_out);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_DAS_MSM_RUNTIME_UTIL_H_
