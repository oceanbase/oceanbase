/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_predicate_json.h
/// \brief OB-side builder for the plugin contract's predicate-tree JSON.
///
/// Builds two JSON views from the same optimizer filters:
///   - `predicate_json`: the complete safely-convertible scan predicate used
///     for plugin-side partition, file, and reader pruning.
///   - `partition_filter_json`: only whole, exactly convertible expressions
///     whose referenced columns are all partition columns.
///
/// `partition_filter_json` is a planning-proof candidate, not permission to
/// remove residual filters by itself. OB removes the corresponding raw
/// expressions only after plan_create reports partition_filter_applied=true.
/// Unsupported expressions remain in OB's filter pipeline for correctness.
///
/// A filter that OB cannot express in the contract grammar (functions, col-vs-col,
/// non-foldable literals, dynamic params) is silently left UN-pushed; OB's own
/// pushdown filter pipeline still evaluates it.
///
/// Constants are folded via `ObSQLUtils::calc_const_or_calculable_expr`; dynamic
/// params are not pushed (they evaluate at runtime). The emitted `col` node
/// carries the column's field_id (`col_idx`, informational) and NAME (`name`,
/// which the plugin resolves to its schema field index/type. The `lit` node carries the value as a string
/// (the plugin converts it to the field type of the sibling col).
///
/// Output strings are deep-copied into the caller's allocator; an empty ObString
/// means "nothing to push down" (the caller passes NULL to the plugin).

#ifndef OB_EXT_PREDICATE_JSON_H
#define OB_EXT_PREDICATE_JSON_H

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObExecContext;
class ObPushdownFilterExecutor;

namespace ext_predicate
{

/// Build the full predicate_json and its fully-convertible partition-only subset.
/// The subset is sent through partition_filter_json as a planning-proof candidate;
/// OB may remove only the corresponding raw exprs after the plugin confirms that
/// the candidate was applied by its planner. `exec_ctx` is required for const
/// folding. Returns an OB errno; on error the outputs are left empty.
int build_predicate_json_from_raw_expr(common::ObIAllocator &alloc,
                                       ObExecContext *exec_ctx,
                                       const common::ObIArray<ObRawExpr *> &filters,
                                       const common::ObIArray<uint64_t> &partition_col_ids,
                                       common::ObString &out_predicate_json,
                                       common::ObString &out_partition_filter_json,
                                       common::ObIArray<ObRawExpr *> &out_partition_filter_exprs);

/// Build a reader-time predicate JSON from the already-instantiated storage
/// filter executor. `column_ids` and `column_names` are parallel arrays mapping
/// OB column ids to plugin schema names. Filter datums must be initialized before
/// this call, so execution parameters are represented by their runtime values.
///
/// Unsupported leaves are omitted from AND, while any unsupported OR child
/// makes the whole OR unpushable. An empty output means no reader pushdown; OB
/// must still evaluate the original storage filter for correctness.
int build_predicate_json_from_pushdown_filter(
    common::ObIAllocator &alloc,
    ObPushdownFilterExecutor *filter,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<common::ObString> &column_names,
    common::ObString &out_predicate_json);

} // namespace ext_predicate
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_PREDICATE_JSON_H
