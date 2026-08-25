/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"

#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // OB_EXT_K_* + grammar
#include "common/object/ob_object.h"  // OB_APP_MIN_COLUMN_ID, ObObj

namespace oceanbase
{
namespace sql
{
namespace ext_predicate
{
namespace
{

// Per-call context carried through the recursive emission.
struct EmitCtx
{
  common::ObIAllocator &alloc;
  ObExecContext *exec_ctx;
  const common::ObIArray<uint64_t> &partition_col_ids;
};

// Strip lossless cast and return the underlying column-ref expr, or null if
// `e` is not a column ref. Mirrors legacy `get_column_ref_without_lossless_cast`.
int get_column_ref(const ObRawExpr *e, const ObColumnRefRawExpr *&col)
{
  int ret = OB_SUCCESS;
  col = nullptr;
  const ObRawExpr *real = e;
  if (OB_ISNULL(e)) {
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(real, real))) {
    LOG_WARN("strip lossless cast failed", K(ret), KPC(e));
  } else if (real->is_column_ref_expr()) {
    col = static_cast<const ObColumnRefRawExpr *>(real);
  }
  return ret;
}

bool is_partition_col(const EmitCtx &ctx, const ObColumnRefRawExpr &col)
{
  const uint64_t col_id = col.get_column_id();
  for (int64_t i = 0; i < ctx.partition_col_ids.count(); ++i) {
    if (ctx.partition_col_ids.at(i) == col_id) { return true; }
  }
  return false;
}

// Fold a const expr to ObObj. Returns OB_SUCCESS with is_valid=false for
// non-foldable (dynamic param / non-const) — caller skips the predicate.
int fold_const(const EmitCtx &ctx, const ObRawExpr *e, common::ObObj &obj, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(e) || OB_ISNULL(ctx.exec_ctx)) {
  } else if (e->has_flag(CNT_DYNAMIC_PARAM)) {
    // exec params evaluate at runtime — not safe for pushdown.
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.exec_ctx, e, obj, is_valid,
                                                                ctx.alloc))) {
    LOG_WARN("calc const expr failed", K(ret), KPC(e));
  } else if (!is_valid || obj.is_unknown() || obj.is_nop_value()) {
    is_valid = false;
  }
  return ret;
}

// JSON-escape a string into `out`.
int json_escape(common::ObSqlString &out, const char *p, int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    char c = p[i];
    switch (c) {
      case '"':  ret = out.append("\\\""); break;
      case '\\': ret = out.append("\\\\"); break;
      case '\n': ret = out.append("\\n");  break;
      case '\r': ret = out.append("\\r");  break;
      case '\t': ret = out.append("\\t");  break;
      case '\b': ret = out.append("\\b");  break;
      case '\f': ret = out.append("\\f");  break;
      default:
        // Control chars -> \u00XX; otherwise literal.
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          ret = out.append(buf);
        } else {
          ret = out.append(&c, 1);
        }
    }
  }
  return ret;
}

int json_escape(common::ObSqlString &out, const common::ObString &s)
{
  return json_escape(out, s.ptr(), s.length());
}

// Render an ObObj's value as the lit node's `value` string (already JSON-escaped).
int obj_to_lit_value(common::ObSqlString &out, const common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (obj.is_null()) {
    // NULL literal: emit as null JSON literal value? The contract lit node is a
    // string; partition maps cannot carry NULL, and equality against
    // NULL is not meaningful. Skip (treat as non-convertible) by returning err.
    ret = OB_ERR_UNEXPECTED;
  } else if (obj.is_string_type() || obj.is_lob()) {
    const common::ObString s = obj.get_string();
    ret = json_escape(out, s);
  } else {
    char buf[OB_MAX_ROW_KEY_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(obj.print_plain_str_literal(buf, sizeof(buf), pos))) {
      LOG_WARN("print obj plain str failed", K(ret), K(obj));
    } else {
      ret = json_escape(out, ObString(pos, buf));
    }
  }
  return ret;
}

// Emit a `col` node. `partition_eligible` is AND-ed with whether this col is a
// partition column (a non-partition col disqualifies the subtree from
// partition_filter).
int emit_col(const EmitCtx &ctx, const ObColumnRefRawExpr &col, bool &partition_eligible,
             common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (!is_partition_col(ctx, col)) { partition_eligible = false; }
  const int64_t field_id =
      static_cast<int64_t>(col.get_column_id() - OB_APP_MIN_COLUMN_ID);
  if (OB_FAIL(out.append_fmt("{\"%s\":\"col\",\"%s\":%ld,\"%s\":\"",
                             OB_EXT_K_KIND, OB_EXT_K_COL_IDX,
                             static_cast<long>(field_id), OB_EXT_K_NAME))) {
  } else if (OB_FAIL(json_escape(out, col.get_column_name()))) {
  } else {
    ret = out.append("\"}");
  }
  return ret;
}

// Emit a `lit` node from a const expr (folded). Returns OB_ERR_UNEXPECTED if the
// const is not foldable / null (caller skips the whole predicate).
int emit_lit(const EmitCtx &ctx, const ObRawExpr *const_expr, common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  common::ObObj obj;
  bool is_valid = false;
  if (OB_FAIL(fold_const(ctx, const_expr, obj, is_valid))) {
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;  // non-foldable -> skip predicate
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"lit\",\"%s\":\"", OB_EXT_K_KIND, OB_EXT_K_VALUE))) {
  } else if (OB_FAIL(obj_to_lit_value(out, obj))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = out.append("\"}");
  }
  return ret;
}

// Normalize a comparison op when the col/const sides are reversed (e.g. `5 > col`
// -> `col < 5`). Mirrors legacy `normalize_cmp_op_type`.
ObItemType normalize_cmp_op(ObItemType op, bool reversed)
{
  if (!reversed) { return op; }
  switch (op) {
    case T_OP_LT: return T_OP_GT;
    case T_OP_GT: return T_OP_LT;
    case T_OP_LE: return T_OP_GE;
    case T_OP_GE: return T_OP_LE;
    default:      return op;  // eq/ne symmetric
  }
}

const char *cmp_op_name(ObItemType op)
{
  switch (op) {
    case T_OP_EQ:
    case T_OP_NSEQ: return "eq";
    case T_OP_NE:   return "ne";
    case T_OP_LT:   return "lt";
    case T_OP_LE:   return "le";
    case T_OP_GT:   return "gt";
    case T_OP_GE:   return "ge";
    default:        return nullptr;
  }
}

// Recursive emit. OB_SUCCESS + `ok=true` => `out` holds the node JSON and
// `partition_eligible` is set. OB_SUCCESS + `ok=false` => not convertible
// (skip). Error => abort.
int emit_node(const EmitCtx &ctx, const ObRawExpr *e, bool &partition_eligible,
              bool &ok, common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  partition_eligible = true;
  if (OB_ISNULL(e)) {
  } else {
    const ObItemType type = e->get_expr_type();
    switch (type) {
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_LT:
      case T_OP_LE:
      case T_OP_GT:
      case T_OP_GE:
      case T_OP_NE: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 2) {
        } else {
          const ObColumnRefRawExpr *col = nullptr;
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObRawExpr *const_expr = nullptr;
          bool reversed = false;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col != nullptr) {
            const_expr = right;
          } else if (OB_FAIL(get_column_ref(right, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(right));
          } else if (col != nullptr) {
            const_expr = left;
            reversed = true;
          }
          if (OB_SUCC(ret) && col != nullptr && const_expr != nullptr) {
            const ObItemType op = normalize_cmp_op(type, reversed);
            const char *opn = cmp_op_name(op);
            if (opn != nullptr
                && OB_FAIL(out.append_fmt("{\"%s\":\"cmp\",\"%s\":\"%s\",\"%s\":[",
                                          OB_EXT_K_KIND, OB_EXT_K_OP, opn,
                                          OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out))) {
            } else if (OB_FAIL(out.append(","))) {
            } else if (OB_FAIL(emit_lit(ctx, const_expr, out))) {
              ret = OB_SUCCESS;  // non-foldable const -> skip this cmp
            } else if (OB_FAIL(out.append("]}"))) {
            } else {
              // partition_filter only carries equality (IN is OR-of-eq, handled
              // in the T_OP_IN branch). Range / NE on a partition col is still
              // a valid residual predicate, just not partition-eligible.
              if (op != T_OP_EQ && op != T_OP_NSEQ) { partition_eligible = false; }
              ok = true;
            }
          }
        }
        break;
      }
      case T_OP_IN:
      case T_OP_NOT_IN: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 2) {
        } else {
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObColumnRefRawExpr *col = nullptr;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col == nullptr || OB_ISNULL(right)
                     || T_OP_ROW != right->get_expr_type()) {
          } else {
            const ObOpRawExpr *row_expr = static_cast<const ObOpRawExpr *>(right);
            const char *kind = (T_OP_IN == type) ? "in" : "not_in";
            // NOT_IN is never partition-eligible (can't express as equality maps).
            if (T_OP_NOT_IN == type) { partition_eligible = false; }
            if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[", OB_EXT_K_KIND, kind,
                                       OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out))) {
            } else {
              bool all_lit_ok = true;
              for (int64_t i = 0; OB_SUCC(ret) && all_lit_ok
                                  && i < row_expr->get_param_count(); ++i) {
                if (OB_FAIL(out.append(","))) {
                } else if (OB_FAIL(emit_lit(ctx, row_expr->get_param_expr(i), out))) {
                  ret = OB_SUCCESS;  // non-foldable item -> whole IN not convertible
                  all_lit_ok = false;
                }
              }
              if (OB_SUCC(ret) && all_lit_ok && OB_FAIL(out.append("]}"))) {
              } else if (OB_SUCC(ret) && all_lit_ok) {
                ok = true;
              }
            }
          }
        }
        break;
      }
      case T_OP_IS:
      case T_OP_IS_NOT: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() < 2) {
        } else {
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObColumnRefRawExpr *col = nullptr;
          common::ObObj const_obj;
          bool is_valid = false;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col == nullptr) {
          } else if (OB_FAIL(fold_const(ctx, right, const_obj, is_valid))) {
          } else if (!is_valid || !const_obj.is_null()) {
            // IS <not-null> is not the IS NULL pattern; skip.
          } else {
            const char *kind = (T_OP_IS == type) ? "is_null" : "is_not_null";
            partition_eligible = false;  // NULL can't be a partition equality map entry
            if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[", OB_EXT_K_KIND, kind,
                                       OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out))) {
            } else if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
            }
          }
        }
        break;
      }
      case T_OP_AND: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        bool any_ok = false;
        bool all_part_eligible = true;
        if (OB_FAIL(out.append_fmt("{\"%s\":\"and\",\"%s\":[", OB_EXT_K_KIND, OB_EXT_K_CHILDREN))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); ++i) {
            bool child_ok = false;
            bool child_part = true;
            common::ObSqlString child_json;
            if (OB_FAIL(emit_node(ctx, op_expr->get_param_expr(i), child_part, child_ok,
                                 child_json))) {
              LOG_WARN("emit and-child failed", K(ret), K(i));
            } else if (!child_ok) {
              // Skip non-convertible conjunct: emitting a weaker AND is safe for
              // partition_filter (prunes less) and for residual (OB filters).
            } else {
              if (any_ok && OB_FAIL(out.append(","))) {}
              if (OB_SUCC(ret) && OB_FAIL(out.append(child_json.string()))) {
              } else {
                any_ok = true;
                if (!child_part) { all_part_eligible = false; }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (!any_ok) {
              ok = false;  // nothing convertible
            } else if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
              partition_eligible = all_part_eligible;
            }
          }
        }
        break;
      }
      case T_OP_OR: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        bool all_ok = true;
        bool all_part_eligible = true;
        if (OB_FAIL(out.append_fmt("{\"%s\":\"or\",\"%s\":[", OB_EXT_K_KIND, OB_EXT_K_CHILDREN))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && all_ok
                              && i < op_expr->get_param_count(); ++i) {
            bool child_ok = false;
            bool child_part = true;
            common::ObSqlString child_json;
            if (OB_FAIL(emit_node(ctx, op_expr->get_param_expr(i), child_part, child_ok,
                                 child_json))) {
              LOG_WARN("emit or-child failed", K(ret), K(i));
            } else if (!child_ok) {
              all_ok = false;  // can't emit partial OR — drop the whole OR
            } else {
              if (i > 0 && OB_FAIL(out.append(","))) {}
              if (OB_SUCC(ret) && OB_FAIL(out.append(child_json.string()))) {
              } else {
                if (!child_part) { all_part_eligible = false; }
              }
            }
          }
          if (OB_SUCC(ret) && all_ok && op_expr->get_param_count() > 0) {
            if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
              partition_eligible = all_part_eligible;
            }
          }
        }
        break;
      }
      case T_OP_NOT: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 1) {
        } else {
          bool child_ok = false;
          bool child_part = true;
          common::ObSqlString child_json;
          if (OB_FAIL(emit_node(ctx, op_expr->get_param_expr(0), child_part, child_ok,
                               child_json))) {
          } else if (!child_ok) {
          } else if (OB_FAIL(out.append_fmt("{\"%s\":\"not\",\"%s\":[", OB_EXT_K_KIND,
                                              OB_EXT_K_CHILDREN))) {
          } else if (OB_FAIL(out.append(child_json.string()))) {
          } else if (OB_FAIL(out.append("]}"))) {
          } else {
            partition_eligible = false;  // NOT is never a partition equality conjunct
            ok = true;
          }
        }
        break;
      }
      default:
        // not convertible (functions, col-vs-col, etc.) — skip
        break;
    }
  }
  return ret;
}

// Wrap a list of node JSON strings into an AND root (or return the single node
// as-is). `parts` are already-emitted node JSON strings.
int wrap_and(const common::ObIArray<common::ObSqlString *> &parts, common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (parts.count() == 1) {
    ret = out.append(parts.at(0)->string());
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"and\",\"%s\":[", OB_EXT_K_KIND,
                                     OB_EXT_K_CHILDREN))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
      if (i > 0 && OB_FAIL(out.append(","))) {}
      if (OB_SUCC(ret) && OB_FAIL(out.append(parts.at(i)->string()))) {}
    }
    if (OB_SUCC(ret)) { ret = out.append("]}"); }
  }
  return ret;
}

struct RuntimeEmitCtx
{
  const common::ObIArray<uint64_t> &column_ids;
  const common::ObIArray<common::ObString> &column_names;
};

bool find_runtime_column(const RuntimeEmitCtx &ctx,
                         const uint64_t column_id,
                         common::ObString &column_name)
{
  bool found = false;
  for (int64_t i = 0; !found && i < ctx.column_ids.count(); ++i) {
    if (ctx.column_ids.at(i) == column_id) {
      column_name = ctx.column_names.at(i);
      found = true;
    }
  }
  return found;
}

int emit_runtime_col(const RuntimeEmitCtx &ctx,
                     const uint64_t column_id,
                     bool &ok,
                     common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  common::ObString column_name;
  if (column_id < OB_APP_MIN_COLUMN_ID
      || !find_runtime_column(ctx, column_id, column_name)) {
    // The executor can reference a pseudo/derived column with no plugin field.
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"col\",\"%s\":%ld,\"%s\":\"",
                                    OB_EXT_K_KIND, OB_EXT_K_COL_IDX,
                                    static_cast<long>(column_id - OB_APP_MIN_COLUMN_ID),
                                    OB_EXT_K_NAME))) {
  } else if (OB_FAIL(json_escape(out, column_name))) {
  } else if (OB_FAIL(out.append("\"}"))) {
  } else {
    ok = true;
  }
  return ret;
}

int emit_runtime_lit(const ObWhiteFilterExecutor &filter,
                     const int64_t datum_idx,
                     bool &ok,
                     common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  common::ObObjMeta obj_meta;
  common::ObObj obj;
  if (datum_idx < 0 || datum_idx >= datums.count()) {
  } else if (WHITE_OP_IN == filter.get_op_type()
             && OB_FAIL(filter.get_filter_node().get_filter_in_val_meta(datum_idx, obj_meta))) {
    LOG_WARN("get runtime IN literal meta failed", K(ret), K(datum_idx));
  } else {
    if (WHITE_OP_IN != filter.get_op_type()) {
      obj_meta = filter.get_param_obj_meta();
    }
    if (datums.at(datum_idx).is_null()) {
      // NULL comparisons are not represented by a lit node.
    } else if (OB_FAIL(datums.at(datum_idx).to_obj(obj, obj_meta))) {
      LOG_WARN("convert runtime predicate datum failed", K(ret), K(datum_idx), K(obj_meta));
    } else if (OB_FAIL(out.append_fmt("{\"%s\":\"lit\",\"%s\":\"",
                                      OB_EXT_K_KIND, OB_EXT_K_VALUE))) {
    } else if (OB_FAIL(obj_to_lit_value(out, obj))) {
    } else if (OB_FAIL(out.append("\"}"))) {
    } else {
      ok = true;
    }
  }
  return ret;
}

const char *white_cmp_op_name(const ObWhiteFilterOperatorType op)
{
  switch (op) {
    case WHITE_OP_EQ: return "eq";
    case WHITE_OP_NE: return "ne";
    case WHITE_OP_LT: return "lt";
    case WHITE_OP_LE: return "le";
    case WHITE_OP_GT: return "gt";
    case WHITE_OP_GE: return "ge";
    default:          return nullptr;
  }
}

int emit_runtime_cmp(const RuntimeEmitCtx &ctx,
                     const uint64_t column_id,
                     const ObWhiteFilterExecutor &filter,
                     const ObWhiteFilterOperatorType op,
                     const int64_t datum_idx,
                     bool &ok,
                     common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const char *op_name = white_cmp_op_name(op);
  bool col_ok = false;
  bool lit_ok = false;
  common::ObSqlString col_json;
  common::ObSqlString lit_json;
  if (OB_ISNULL(op_name)) {
  } else if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json))) {
  } else if (!col_ok) {
  } else if (OB_FAIL(emit_runtime_lit(filter, datum_idx, lit_ok, lit_json))) {
  } else if (!lit_ok) {
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"cmp\",\"%s\":\"%s\",\"%s\":[",
                                    OB_EXT_K_KIND, OB_EXT_K_OP, op_name,
                                    OB_EXT_K_CHILDREN))) {
  } else if (OB_FAIL(out.append(col_json.string()))) {
  } else if (OB_FAIL(out.append(","))) {
  } else if (OB_FAIL(out.append(lit_json.string()))) {
  } else if (OB_FAIL(out.append("]}"))) {
  } else {
    ok = true;
  }
  return ret;
}

int wrap_runtime_children(const char *kind,
                          const common::ObIArray<common::ObSqlString> &children,
                          common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (children.count() == 1) {
    ret = out.append(children.at(0).string());
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                                    OB_EXT_K_KIND, kind, OB_EXT_K_CHILDREN))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children.count(); ++i) {
      if (i > 0 && OB_FAIL(out.append(","))) {}
      if (OB_SUCC(ret) && OB_FAIL(out.append(children.at(i).string()))) {}
    }
    if (OB_SUCC(ret)) {
      ret = out.append("]}");
    }
  }
  return ret;
}

int emit_runtime_white_filter(const RuntimeEmitCtx &ctx,
                              ObWhiteFilterExecutor &filter,
                              bool &ok,
                              common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const common::ObIArray<uint64_t> &col_ids = filter.get_col_ids();
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (filter.is_filter_dynamic_node() || filter.is_semistruct_filter_node()
      || col_ids.count() != 1) {
    // Runtime/semistruct filters cannot be frozen into a reader created once.
  } else {
    const uint64_t column_id = col_ids.at(0);
    const ObWhiteFilterOperatorType op = filter.get_op_type();
    if (WHITE_OP_NU == op || WHITE_OP_NN == op) {
      bool col_ok = false;
      common::ObSqlString col_json;
      const char *kind = WHITE_OP_NU == op ? "is_null" : "is_not_null";
      if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json))) {
      } else if (!col_ok) {
      } else if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                                        OB_EXT_K_KIND, kind, OB_EXT_K_CHILDREN))) {
      } else if (OB_FAIL(out.append(col_json.string()))) {
      } else if (OB_FAIL(out.append("]}"))) {
      } else {
        ok = true;
      }
    } else if (WHITE_OP_IN == op) {
      bool col_ok = false;
      common::ObSqlString col_json;
      common::ObSEArray<common::ObSqlString, 8> lit_jsons;
      if (datums.empty()) {
        // Do not rely on plugins accepting an empty IN list.
      } else if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json))) {
      } else if (!col_ok) {
      } else if (OB_FAIL(lit_jsons.reserve(datums.count()))) {
      } else {
        bool all_lits_ok = true;
        for (int64_t i = 0; OB_SUCC(ret) && all_lits_ok && i < datums.count(); ++i) {
          bool lit_ok = false;
          common::ObSqlString lit_json;
          if (OB_FAIL(emit_runtime_lit(filter, i, lit_ok, lit_json))) {
          } else if (!lit_ok) {
            all_lits_ok = false;
          } else if (OB_FAIL(lit_jsons.push_back(lit_json))) {
          }
        }
        if (OB_SUCC(ret) && all_lits_ok
            && OB_FAIL(out.append_fmt("{\"%s\":\"in\",\"%s\":[",
                                      OB_EXT_K_KIND, OB_EXT_K_CHILDREN))) {
        } else if (OB_SUCC(ret) && all_lits_ok
                   && OB_FAIL(out.append(col_json.string()))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && all_lits_ok && i < lit_jsons.count(); ++i) {
            if (OB_FAIL(out.append(","))) {
            } else if (OB_FAIL(out.append(lit_jsons.at(i).string()))) {
            }
          }
          if (OB_SUCC(ret) && all_lits_ok && OB_FAIL(out.append("]}"))) {
          } else if (OB_SUCC(ret) && all_lits_ok) {
            ok = true;
          }
        }
      }
    } else if (WHITE_OP_BT == op && datums.count() == 2) {
      common::ObSEArray<common::ObSqlString, 2> children;
      bool lower_ok = false;
      bool upper_ok = false;
      common::ObSqlString lower;
      common::ObSqlString upper;
      if (OB_FAIL(emit_runtime_cmp(ctx, column_id, filter, WHITE_OP_GE, 0,
                                   lower_ok, lower))) {
      } else if (OB_FAIL(emit_runtime_cmp(ctx, column_id, filter, WHITE_OP_LE, 1,
                                          upper_ok, upper))) {
      } else if (!lower_ok || !upper_ok) {
      } else if (OB_FAIL(children.push_back(lower))) {
      } else if (OB_FAIL(children.push_back(upper))) {
      } else if (OB_FAIL(wrap_runtime_children("and", children, out))) {
      } else {
        ok = true;
      }
    } else if (datums.count() == 1) {
      ret = emit_runtime_cmp(ctx, column_id, filter, op, 0, ok, out);
    }
  }
  return ret;
}

int emit_runtime_node(const RuntimeEmitCtx &ctx,
                      ObPushdownFilterExecutor *filter,
                      bool &ok,
                      bool &fully_converted,
                      common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  fully_converted = false;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (filter->is_logic_and_node() || filter->is_logic_or_node()) {
    const bool is_and = filter->is_logic_and_node();
    bool all_ok = true;
    bool all_fully_converted = true;
    common::ObSEArray<common::ObSqlString, 4> children;
    ObPushdownFilterExecutor **child_filters = filter->get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && (is_and || all_ok)
                         && i < filter->get_child_count(); ++i) {
      bool child_ok = false;
      bool child_fully_converted = false;
      common::ObSqlString child_json;
      if (OB_ISNULL(child_filters) || OB_ISNULL(child_filters[i])) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(emit_runtime_node(ctx, child_filters[i], child_ok,
                                           child_fully_converted, child_json))) {
        LOG_WARN("emit runtime predicate child failed", K(ret), K(i));
      } else if (!child_ok) {
        all_fully_converted = false;
        if (!is_and) {
          all_ok = false;  // Partial OR could incorrectly discard rows.
        }
      } else if (OB_FAIL(children.push_back(child_json))) {
      } else if (!child_fully_converted) {
        all_fully_converted = false;
      }
    }
    if (OB_SUCC(ret) && all_ok && !children.empty()) {
      if (OB_FAIL(wrap_runtime_children(is_and ? "and" : "or", children, out))) {
      } else {
        ok = true;
        fully_converted = all_fully_converted;
      }
    }
  } else if (filter->is_filter_white_node()) {
    ret = emit_runtime_white_filter(
        ctx, *static_cast<ObWhiteFilterExecutor *>(filter), ok, out);
    fully_converted = ok;
  }
  // Black/sample/other nodes remain as OB-side residual filters.
  return ret;
}

int copy_out(common::ObIAllocator &alloc, const common::ObSqlString &s, common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (s.empty()) {
  } else {
    // The plugin ABI takes predicate JSON as `const char *` without a length.
    // Keep the ObString length exact, but make the backing buffer C-string safe.
    char *dst = static_cast<char *>(alloc.alloc(s.length() + 1));
    if (OB_ISNULL(dst)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc predicate json failed", K(ret), K(s.length()));
    } else {
      MEMCPY(dst, s.ptr(), s.length());
      dst[s.length()] = '\0';
      out.assign(dst, static_cast<int32_t>(s.length()));
    }
  }
  return ret;
}

} // namespace

int build_predicate_json_from_raw_expr(common::ObIAllocator &alloc,
                                       ObExecContext *exec_ctx,
                                       const common::ObIArray<ObRawExpr *> &filters,
                                       const common::ObIArray<uint64_t> &partition_col_ids,
                                       common::ObString &out_predicate_json)
{
  // Single predicate tree (the AND of every convertible top-level filter). We do
  // NOT split partition vs residual here anymore: paimon's SDK splits the one
  // Predicate internally (CreatePickedFieldFilter picks partition-key conjuncts
  // for partition pruning; ExcludePredicateWithFields yields the residual row
  // predicate). That mirrors the deleted native paimon path, which only ever
  // called SetPredicate and never SetPartitionFilter. OB therefore keeps zero
  // format-specific classification; the plan_create `partition_filter_json`
  // argument is left NULL (see ob_ext_file_pruner.cpp). `partition_col_ids`
  // stays in the signature only because emit_node tags columns with it; it no
  // longer gates the output. Non-convertible conjuncts are dropped (OB's own
  // pipeline still evaluates the full filter, so correctness is preserved).
  int ret = OB_SUCCESS;
  out_predicate_json.reset();
  EmitCtx ctx = {alloc, exec_ctx, partition_col_ids};
  common::ObSEArray<common::ObSqlString, 4> parts;  // every convertible top-level filter
  if (OB_FAIL(parts.reserve(filters.count()))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      const ObRawExpr *f = filters.at(i);
      bool ok = false;
      bool part_eligible = true;  // computed by emit_node; no longer used to split
      common::ObSqlString node;
      if (OB_FAIL(emit_node(ctx, f, part_eligible, ok, node))) {
        LOG_WARN("emit predicate node failed", K(ret), K(i), KPC(f));
      } else if (!ok) {
        // non-convertible — OB's own pipeline filters it.
      } else if (OB_FAIL(parts.push_back(node))) {
        LOG_WARN("push back predicate part failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !parts.empty()) {
    common::ObSqlString assembled;
    common::ObSEArray<common::ObSqlString *, 4> ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
      if (OB_FAIL(ptrs.push_back(&parts.at(i)))) {}
    }
    if (OB_SUCC(ret) && OB_FAIL(wrap_and(ptrs, assembled))) {
      LOG_WARN("assemble predicate json failed", K(ret));
    } else if (OB_FAIL(copy_out(alloc, assembled, out_predicate_json))) {
      LOG_WARN("copy predicate json failed", K(ret));
    }
  }
  return ret;
}

int build_predicate_json_from_pushdown_filter(
    common::ObIAllocator &alloc,
    ObPushdownFilterExecutor *filter,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<common::ObString> &column_names,
    common::ObString &out_predicate_json,
    bool &out_fully_converted)
{
  int ret = OB_SUCCESS;
  out_predicate_json.reset();
  out_fully_converted = false;
  if (column_ids.count() != column_names.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("runtime predicate column mapping count mismatch",
             K(ret), K(column_ids.count()), K(column_names.count()));
  } else if (OB_NOT_NULL(filter)) {
    RuntimeEmitCtx ctx = {column_ids, column_names};
    bool ok = false;
    common::ObSqlString node;
    if (OB_FAIL(emit_runtime_node(ctx, filter, ok, out_fully_converted, node))) {
      LOG_WARN("emit runtime predicate failed", K(ret));
    } else if (ok && OB_FAIL(copy_out(alloc, node, out_predicate_json))) {
      LOG_WARN("copy runtime predicate json failed", K(ret));
      out_fully_converted = false;
    } else if (!ok) {
      out_fully_converted = false;
    }
  }
  return ret;
}

} // namespace ext_predicate
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
