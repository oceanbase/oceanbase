/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_predicate_json.h
/// \brief OB-side builder for the plugin contract's predicate-tree JSON.
///
/// Turns optimizer-stage `ObRawExpr` filters into the two JSON strings the
/// plugin contract carries at plan_create:
///   - `partition_filter_json`: equality/IN conjuncts on partition-key columns
///     only (the plugin flattens these into `SetPartitionFilter`'s
///     `vector<map<string,string>>`). Range / IS-NULL / NE / NOT-IN on
///     partition columns is NOT pushed here — those stay in the residual
///     predicate (OB filters rows, or go to predicate_json once reader_create
///     wiring lands).
///   - `predicate_json`: the residual row predicate — every other convertible
///     filter (cmp/in/is_null/is_not_null/and/or/not) regardless of column kind.
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

/// Build a single predicate_json (the AND of every convertible top-level filter)
/// from optimizer filter exprs. OB does NOT split partition vs residual — the
/// plugin's SDK splits the one Predicate internally (e.g. paimon
/// CreatePickedFieldFilter / ExcludePredicateWithFields), mirroring the deleted
/// native paimon path which only called SetPredicate. The plan_create
/// `partition_filter_json` argument is therefore passed NULL by the caller.
/// `partition_col_ids` (OB column ids = field_id + OB_APP_MIN_COLUMN_ID of the
/// plugin-declared partition-key columns) is retained only for column tagging
/// inside emit; it no longer gates the output. `exec_ctx` is required for const
/// folding. Returns an OB errno; on error the output is left empty.
int build_predicate_json_from_raw_expr(common::ObIAllocator &alloc,
                                       ObExecContext *exec_ctx,
                                       const common::ObIArray<ObRawExpr *> &filters,
                                       const common::ObIArray<uint64_t> &partition_col_ids,
                                       common::ObString &out_predicate_json);

/// Build a reader-time predicate JSON from the already-instantiated storage
/// filter executor. `column_ids` and `column_names` are parallel arrays mapping
/// OB column ids to plugin schema names. Filter datums must be initialized before
/// this call, so execution parameters are represented by their runtime values.
///
/// Unsupported leaves are omitted from AND, while any unsupported OR child
/// makes the whole OR unpushable. An empty output means no reader pushdown; OB
/// must still evaluate the original storage filter for correctness.
/// `out_fully_converted` is true only when every node in the original tree was
/// represented in JSON; it does not by itself guarantee plugin-side filtering.
int build_predicate_json_from_pushdown_filter(
    common::ObIAllocator &alloc,
    ObPushdownFilterExecutor *filter,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<common::ObString> &column_names,
    common::ObString &out_predicate_json,
    bool &out_fully_converted);

} // namespace ext_predicate
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_PREDICATE_JSON_H
