/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL
#include "sql/outline/ob_outline_template_matcher.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_outline_binding_rule.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/outline/ob_pattern_matcher.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/ob_parser.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/utility/ob_sort.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace sql
{

// ==================== Static Helper Functions ====================

static inline bool is_identifier_char(char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') || c == '_';
}

static int64_t ob_find_str_ci_local(const char *haystack, int64_t hlen,
                                    const char *needle, int64_t nlen,
                                    int64_t search_start = 0)
{
  if (nlen == 0 || hlen < nlen) return -1;
  for (int64_t i = search_start; i <= hlen - nlen; ++i) {
    bool match = true;
    for (int64_t j = 0; j < nlen; ++j) {
      if (::tolower(haystack[i + j]) != ::tolower(needle[j])) {
        match = false;
        break;
      }
    }
    if (match) {
      bool left_ok = (i == 0) || !is_identifier_char(haystack[i - 1]);
      bool right_ok = (i + nlen >= hlen) || !is_identifier_char(haystack[i + nlen]);
      if (left_ok && right_ok) {
        return i;
      }
    }
  }
  return -1;
}

static int ob_replace_all_ci_local(const ObString &input,
                                   const ObString &from,
                                   const ObString &to,
                                   ObSqlString &output)
{
  int ret = OB_SUCCESS;
  output.reset();
  if (from.empty()) {
    ret = output.assign(input);
  } else {
    const char *ptr = input.ptr();
    int64_t remaining = input.length();
    int64_t offset = 0;
    bool found = false;
    while (OB_SUCC(ret) && offset < remaining) {
      int64_t pos = ob_find_str_ci_local(ptr + offset, remaining - offset, from.ptr(), from.length());
      if (pos < 0) {
        if (OB_FAIL(output.append(ptr + offset, remaining - offset))) {
          LOG_WARN("fail to append tail", K(ret));
        }
        break;
      } else {
        found = true;
        if (pos > 0 && OB_FAIL(output.append(ptr + offset, pos))) {
          LOG_WARN("fail to append segment", K(ret));
        } else if (OB_FAIL(output.append(to))) {
          LOG_WARN("fail to append replacement", K(ret));
        }
        offset += pos + from.length();
      }
    }
    if (OB_SUCC(ret) && !found && output.empty()) {
      ret = output.assign(input);
    }
  }
  return ret;
}

static inline bool is_hint_subtree_root(const ParseNode *node)
{
  return OB_NOT_NULL(node)
      && (T_HINT_OPTION_LIST == node->type_
          || T_RELATION_FACTOR_IN_HINT == node->type_
          || T_RELATION_FACTOR_IN_HINT_LIST == node->type_
          || T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST == node->type_
          || T_INDEX_HINT == node->type_
          || T_INDEX_HINT_LIST == node->type_);
}

// ==================== collect_table_names_dfs ====================

int ObOutlineTemplateMatcher::collect_table_names_dfs(
    const ParseNode *node,
    ObIArray<ObTableDbName> &table_db_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // skip NULL
  } else if (is_hint_subtree_root(node)) {
    // Ignore hint-only subtrees. They may contain relation-like parse nodes,
    // but they are not real FROM/JOIN slots and must not affect ast_position.
  } else if (T_RELATION_FACTOR == node->type_) {
    // Leaf: extract db_name and table_name
    const ParseNode *db_node = node->num_child_ > 0 ? node->children_[0] : NULL;
    const ParseNode *tbl_node = node->num_child_ > 1 ? node->children_[1] : NULL;
    ObString db_name;
    ObString tbl_name;
    if (OB_NOT_NULL(tbl_node) && T_IDENT == tbl_node->type_) {
      tbl_name.assign_ptr(tbl_node->str_value_, static_cast<ObString::obstr_size_t>(tbl_node->str_len_));
    }
    if (OB_NOT_NULL(db_node) && T_IDENT == db_node->type_) {
      db_name.assign_ptr(db_node->str_value_, static_cast<ObString::obstr_size_t>(db_node->str_len_));
    }
    if (!tbl_name.empty()) {
      if (OB_FAIL(table_db_names.push_back(ObTableDbName(tbl_name, db_name)))) {
        LOG_WARN("failed to push table/db name", K(ret), K(tbl_name));
      }
    }
  } else {
    // Non-leaf: DFS all children in order
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_NOT_NULL(node->children_) && OB_NOT_NULL(node->children_[i])) {
        if (OB_FAIL(collect_table_names_dfs(node->children_[i],
                                             table_db_names))) {
          LOG_WARN("failed to collect from child node", K(ret), K(i), K(node->type_));
        }
      }
    }
  }
  return ret;
}

// ==================== has_explicit_db_prefix ====================

bool ObOutlineTemplateMatcher::has_explicit_db_prefix(const TableItem *table_item)
{
  if (OB_ISNULL(table_item)) {
    return false;
  }
  // Check database_name_ first: the resolver always sets this for db-qualified tables
  // (e.g., "dbt_db_001.orders"). This works even when node_ is not properly set,
  // such as during CREATE OUTLINE resolution.
  if (!table_item->database_name_.empty()) {
    return true;
  }
  if (OB_ISNULL(table_item->node_)) {
    return false;
  }
  const ParseNode *node = table_item->node_;
  // T_RELATION_FACTOR: children_[0] = db_name, children_[1] = table_name
  if (T_RELATION_FACTOR == node->type_ && node->num_child_ > 0) {
    const ParseNode *db_node = node->children_[0];
    return (OB_NOT_NULL(db_node) && db_node->str_len_ > 0);
  }
  // For T_ALIAS wrapping a T_RELATION_FACTOR
  if (T_ALIAS == node->type_ && node->num_child_ > 0) {
    const ParseNode *rel_node = node->children_[0];
    if (OB_NOT_NULL(rel_node) && T_RELATION_FACTOR == rel_node->type_ && rel_node->num_child_ > 0) {
      const ParseNode *db_node = rel_node->children_[0];
      return (OB_NOT_NULL(db_node) && db_node->str_len_ > 0);
    }
  }
  return false;
}

// ==================== parse-tree based template signature ====================
// New mechanism (replaces the stmt + ObSqlPrinter round-trip): locate db/table
// identifier tokens directly in the original SQL text via each ParseNode's
// sql_str_off_, overwrite them with wildcards, then fast-parameterize via
// get_outline_key. CREATE and MATCH share this worker so signatures are
// byte-symmetric without any resolve.

// A byte range in the original SQL to be overwritten by rep_.
struct ObSigReplaceSpan
{
  int64_t off_;
  int64_t len_;
  common::ObString rep_;  // "*" (db slot / table slot with db present) or "*.*" (bare table slot)
  TO_STRING_KV(K_(off), K_(len), K_(rep));
};

// Compute the raw [off,len) span of an identifier token in sql, robust to
// backtick quoting/escaping and sql_str_off_ off-by-one. Returns false if the
// span cannot be located (defensive: caller skips it).
static bool identifier_raw_span(const ObString &sql, const ParseNode *ident,
                                int64_t &off, int64_t &len)
{
  bool ok = false;
  off = 0;
  len = 0;
  if (OB_ISNULL(ident) || OB_ISNULL(ident->str_value_) || ident->str_len_ <= 0) {
    // nothing
  } else {
    const char *buf = sql.ptr();
    const int64_t total = sql.length();
    const int64_t a = ident->sql_str_off_;
    if (a < 0 || a >= total) {
      // unusable offset
    } else {
      // sql_str_off_ may point at the backtick or one past it depending on the
      // lexer rule; probe both.
      int64_t start = a;
      if (buf[a] != '`' && a > 0 && buf[a - 1] == '`') {
        start = a - 1;
      }
      if (buf[start] == '`') {
        int64_t i = start + 1;
        bool closed = false;
        while (i < total && !closed) {
          if (buf[i] == '`') {
            if (i + 1 < total && buf[i + 1] == '`') {
              i += 2;  // escaped `` inside a quoted identifier
            } else {
              closed = true;
            }
          } else {
            i += 1;
          }
        }
        if (closed) {
          off = start;
          len = i - start + 1;
          ok = true;
        }
      } else {
        // unquoted: consume identifier bytes ([A-Za-z0-9_$] or non-ascii/multibyte)
        int64_t i = a;
        while (i < total) {
          const unsigned char c = static_cast<unsigned char>(buf[i]);
          const bool id_char = (c >= 0x80)
              || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9') || c == '_' || c == '$';
          if (!id_char) { break; }
          i += 1;
        }
        if (i > a) {
          off = a;
          len = i - a;
          ok = true;
        }
      }
    }
  }
  return ok;
}

static int add_ident_span(const ObString &sql, const ParseNode *ident,
                          const ObString &rep, ObIArray<ObSigReplaceSpan> &spans)
{
  int ret = OB_SUCCESS;
  int64_t off = 0;
  int64_t len = 0;
  if (identifier_raw_span(sql, ident, off, len)) {
    ObSigReplaceSpan span;
    span.off_ = off;
    span.len_ = len;
    span.rep_ = rep;
    if (OB_FAIL(spans.push_back(span))) {
      LOG_WARN("failed to push replace span", K(ret));
    }
  } else {
    LOG_WARN("skip identifier with unusable span (defensive)",
             "off", OB_NOT_NULL(ident) ? ident->sql_str_off_ : -1);
  }
  return ret;
}

// Collect SQL text spans to wildcard for template signature generation, in a
// single DFS. Every relation-factor table (including CTE references) normalizes
// to `*`.`*`, and every column-ref table/db qualifier is wildcarded too. No name
// pre-collection is needed: aliases and CTE refs are wildcarded like any other
// qualifier, so both CREATE and MATCH transform identically (byte-symmetric), and
// the FROM-clause position guard (ast_position) does the real physical-table
// disambiguation at match time. The column name itself is never touched.
static int collect_wildcard_spans_dfs(const ObString &sql,
                                      const ParseNode *node,
                                      ObIArray<ObSigReplaceSpan> &spans)
{
  int ret = OB_SUCCESS;
  // Keep wildcarded SQL parseable by quoting the identifiers with the mode's
  // quote char: backtick for mysql, double-quote for oracle. Both CREATE and
  // MATCH run under the same mode, so the signature stays symmetric.
  const bool is_oracle = lib::is_oracle_mode();
  const ObString star = is_oracle
      ? ObString::make_string("\"*\"") : ObString::make_string("`*`");
  const ObString star_star = is_oracle
      ? ObString::make_string("\"*\".\"*\"") : ObString::make_string("`*`.`*`");
  if (OB_ISNULL(node)) {
    // skip
  } else if (is_hint_subtree_root(node)) {
    // ignore hint-only subtrees
  } else if (T_RELATION_FACTOR == node->type_) {
    const ParseNode *db_node = node->num_child_ > 0 ? node->children_[0] : NULL;
    const ParseNode *tbl_node = node->num_child_ > 1 ? node->children_[1] : NULL;
    const bool has_db = OB_NOT_NULL(db_node) && T_IDENT == db_node->type_ && db_node->str_len_ > 0;
    if (has_db) {
      if (OB_FAIL(add_ident_span(sql, db_node, star, spans))) {
        LOG_WARN("failed add relation db span", K(ret));
      } else if (OB_FAIL(add_ident_span(sql, tbl_node, star, spans))) {
        LOG_WARN("failed add relation tbl span", K(ret));
      }
    } else if (OB_NOT_NULL(tbl_node) && T_IDENT == tbl_node->type_ && tbl_node->str_len_ > 0) {
      if (OB_FAIL(add_ident_span(sql, tbl_node, star_star, spans))) {
        LOG_WARN("failed add relation bare tbl span", K(ret));
      }
    }
    // leaf: do NOT recurse into children_[2+]
  } else if (T_COLUMN_REF == node->type_) {
    const ParseNode *db_node = node->num_child_ > 0 ? node->children_[0] : NULL;
    const ParseNode *tbl_node = node->num_child_ > 1 ? node->children_[1] : NULL;
    const bool has_db = OB_NOT_NULL(db_node) && T_IDENT == db_node->type_ && db_node->str_len_ > 0;
    const bool has_tbl = OB_NOT_NULL(tbl_node) && T_IDENT == tbl_node->type_ && tbl_node->str_len_ > 0;
    if (has_db && has_tbl) {
      // db.tbl.col -> *.*.col
      if (OB_FAIL(add_ident_span(sql, db_node, star, spans))) {
        LOG_WARN("failed add col db span", K(ret));
      } else if (OB_FAIL(add_ident_span(sql, tbl_node, star, spans))) {
        LOG_WARN("failed add col tbl span", K(ret));
      }
    } else if (has_tbl) {
      // tbl.col -> *.*.col; qualifier wildcarded unconditionally (physical table
      // or alias alike -- the FROM-clause position guard disambiguates).
      if (OB_FAIL(add_ident_span(sql, tbl_node, star_star, spans))) {
        LOG_WARN("failed add col bare tbl span", K(ret));
      }
    }
    // column child (children_[2]) is never wildcarded; no recursion needed
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_NOT_NULL(node->children_) && OB_NOT_NULL(node->children_[i])) {
        if (OB_FAIL(collect_wildcard_spans_dfs(sql, node->children_[i], spans))) {
          LOG_WARN("failed wildcard span dfs", K(ret));
        }
      }
    }
  }
  return ret;
}

// Apply wildcard spans from left to right, then fast-parameterize via get_outline_key.
static int apply_spans_and_get_key(const ObString &sql,
                                   ObIArray<ObSigReplaceSpan> &spans,
                                   ObSQLSessionInfo *session,
                                   ObIAllocator &allocator,
                                   bool need_format,
                                   ObString &signature)
{
  int ret = OB_SUCCESS;
  signature.reset();
  if (spans.count() > 0) {
    lib::ob_sort(&spans.at(0), &spans.at(0) + spans.count(),
                 [](const ObSigReplaceSpan &lhs, const ObSigReplaceSpan &rhs) {
                   return lhs.off_ < rhs.off_;
                 });
  }
  ObSqlString buf;
  int64_t prev = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < spans.count(); ++i) {
    const ObSigReplaceSpan &s = spans.at(i);
    if (s.off_ < prev || s.len_ <= 0 || s.off_ + s.len_ > sql.length()) {
      // overlapping or out of range -> skip defensively
      LOG_WARN("skip out-of-range/overlapping span", K(s), K(prev), K(sql.length()));
    } else if (OB_FAIL(buf.append(sql.ptr() + prev, s.off_ - prev))) {
      LOG_WARN("append prefix failed", K(ret));
    } else if (OB_FAIL(buf.append(s.rep_))) {
      LOG_WARN("append replacement failed", K(ret));
    } else {
      prev = s.off_ + s.len_;
    }
  }
  if (OB_SUCC(ret) && prev < sql.length()) {
    if (OB_FAIL(buf.append(sql.ptr() + prev, sql.length() - prev))) {
      LOG_WARN("append tail failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObMaxConcurrentParam::FixParamStore fix_params(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    ObWrapperAllocator(&allocator));
    bool has_qm = false;
    if (OB_FAIL(ObSQLUtils::get_outline_key(allocator, session, buf.string(),
                                            signature, fix_params,
                                            FP_PARAMERIZE_AND_FILTER_HINT_MODE,
                                            has_qm, need_format))) {
      LOG_WARN("failed to get_outline_key for parse-tree signature", K(ret), K(buf.string()));
    } else {
      LOG_DEBUG("[OUTLINE] parse-tree wildcard sql", K(buf.string()), K(signature));
    }
  }
  return ret;
}

int ObOutlineTemplateMatcher::generate_template_signature_from_parse_tree(
    const ObString &sql_text,
    const ParseNode *stmt_node,
    ObSQLSessionInfo *session,
    ObIAllocator &allocator,
    bool need_format,
    ObString &template_signature)
{
  int ret = OB_SUCCESS;
  template_signature.reset();
  if (OB_ISNULL(stmt_node) || OB_ISNULL(session) || sql_text.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null param for parse-tree signature", K(ret), KP(stmt_node), KP(session),
             K(sql_text.length()));
  } else {
    ObSEArray<ObSigReplaceSpan, 16> spans;
    if (OB_FAIL(collect_wildcard_spans_dfs(sql_text, stmt_node, spans))) {
      LOG_WARN("failed to collect wildcard spans", K(ret));
    } else if (OB_FAIL(apply_spans_and_get_key(sql_text, spans, session, allocator,
                                               need_format, template_signature))) {
      LOG_WARN("failed to apply spans and get key", K(ret));
    }
    LOG_DEBUG("[OUTLINE] parse-tree template signature", K(ret), K(template_signature),
              "span_cnt", spans.count());
  }
  return ret;
}

// Thin wrapper: validate the parse result, extract the original SQL text and
// the stmt child, and delegate to generate_template_signature_from_parse_tree.
// Best-effort: returns OB_SUCCESS with an empty signature when the parse result
// is unusable (callers treat empty as "no signature").
static int generate_signature_from_parse_result(const ParseResult *parse_result,
                                                ObSQLSessionInfo *session,
                                                ObIAllocator &allocator,
                                                bool need_format,
                                                ObString &template_signature)
{
  int ret = OB_SUCCESS;
  template_signature.reset();
  if (OB_ISNULL(parse_result) || OB_ISNULL(parse_result->result_tree_)
      || parse_result->result_tree_->num_child_ <= 0
      || OB_ISNULL(parse_result->result_tree_->children_[0])) {
    // unusable parse result: leave signature empty
  } else {
    ObString sql_text(static_cast<ObString::obstr_size_t>(parse_result->input_sql_len_),
                      parse_result->input_sql_);
    ret = ObOutlineTemplateMatcher::generate_template_signature_from_parse_tree(
        sql_text, parse_result->result_tree_->children_[0], session,
        allocator, need_format, template_signature);
  }
  return ret;
}

// ==================== match_template_outline ====================

int ObOutlineTemplateMatcher::match_template_outline(
    ObPlanCacheCtx &pc_ctx,
    const ObOutlineInfo &outline_info,
    bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
  ObIAllocator &allocator = pc_ctx.allocator_;

  LOG_DEBUG("[BINDING_RULE] match_template_outline enter",
           K(outline_info.get_outline_id()), K(outline_info.get_pattern_rules_str().length()));

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const ObString &pattern_rules = outline_info.get_pattern_rules_str();
    ObOutlineBindingRule local_binding_rule;
    const ObOutlineBindingRule *binding_rule_ptr = NULL;

    if (pattern_rules.empty()) {
      LOG_TRACE("[OUTLINE] empty pattern_rules, cannot match");
    } else if (OB_FAIL(ObOutlineBindingRule::deserialize_from_json(pattern_rules, allocator, local_binding_rule))) {
      LOG_WARN("failed to deserialize pattern_rules JSON", K(ret), K(pattern_rules));
    } else {
      // Pre-parse patterns (non-fatal on failure). Regex validation runs inline
      // via ICU at match time, so no pre-compilation is needed here.
      for (int64_t i = 0; i < local_binding_rule.get_map_item_count(); ++i) {
        ObOutlineRuleMapping &m = local_binding_rule.get_map_item(i);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = m.parse_patterns())) {
          LOG_WARN("parse_patterns soft-fail", K(tmp_ret), K(i));
        }
      }
      binding_rule_ptr = &local_binding_rule;
    }

    if (OB_FAIL(ret) || OB_ISNULL(binding_rule_ptr)) {
      // Error or empty pattern_rules
    } else if (binding_rule_ptr->get_map_item_count() == 0) {
      matched = true;
      LOG_TRACE("[OUTLINE] SCOPE=TENANT without MAP, signature matched");
    } else {
      hash::ObHashMap<ObString, ObString> var_values;
      if (OB_FAIL(var_values.create(16, ObMemAttr(OB_SERVER_TENANT_ID, "PatternMatch")))) {
        LOG_WARN("failed to create var_values hashmap", K(ret));
      } else {
        ObSEArray<ObTableDbName, 16> actual_table_db_names;

        ParseResult *parse_result = pc_ctx.sql_ctx_.outline_match_parse_result_;
        if (OB_NOT_NULL(parse_result) && OB_NOT_NULL(parse_result->result_tree_) &&
            parse_result->result_tree_->num_child_ > 0 &&
            OB_NOT_NULL(parse_result->result_tree_->children_[0])) {
          ParseNode *stmt_node = parse_result->result_tree_->children_[0];
          if (OB_FAIL(collect_table_names_dfs(stmt_node, actual_table_db_names))) {
            LOG_WARN("failed to collect actual table names via DFS", K(ret));
          } else {
            LOG_TRACE("[OUTLINE] collected actual tables", K(actual_table_db_names.count()));
          }
        }

        if (OB_SUCC(ret) && actual_table_db_names.empty()) {
          matched = false;
          LOG_TRACE("[OUTLINE] fallback match (parse tree unavailable or no tables)");
        } else if (OB_SUCC(ret)) {
          bool all_matched = true;
          for (int64_t i = 0; OB_SUCC(ret) && all_matched && i < binding_rule_ptr->get_map_item_count(); ++i) {
            const ObOutlineRuleMapping &mapping = binding_rule_ptr->get_map_item(i);
            int64_t position = mapping.get_ast_position();

            ObString actual_table;
            ObString actual_db;
            if (position > 0 && position <= actual_table_db_names.count()) {
              actual_table = actual_table_db_names.at(position - 1).first;
              actual_db = actual_table_db_names.at(position - 1).second;
            }

            if (actual_db.empty() && mapping.has_db_prefix()) {
              if (OB_NOT_NULL(session)) {
                const ObString &session_db = session->get_database_name();
                if (!session_db.empty()) {
                  actual_db = session_db;
                }
              }
            }

            if (actual_table.empty()) {
              all_matched = false;
              LOG_TRACE("[OUTLINE] position mismatch", K(i), K(position),
                        K(actual_table_db_names.count()));
            } else {
              bool tbl_matched = false;
              bool db_matched = true;

              if (mapping.is_fixed_mapping()) {
                if (OB_FAIL(ObPatternMatcher::match_exact(actual_table,
                                                           mapping.get_original_table_name(),
                                                           tbl_matched))) {
                  LOG_WARN("failed exact table match", K(ret));
                } else if (!tbl_matched) {
                  all_matched = false;
                  LOG_TRACE("[OUTLINE] exact table mismatch", K(i),
                            K(actual_table), K(mapping.get_original_table_name()));
                }
              } else if (mapping.has_table_pattern_form()) {
                // Pattern view must be assembled by parse_patterns() during
                // preload; a missing view would silently false-negative every query.
                if (OB_UNLIKELY(!mapping.are_patterns_parsed())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("patterns_parsed_ not set when matching", K(ret),
                           K(actual_table), K(mapping.get_table_fixed_prefix()),
                           K(mapping.get_table_var_name()));
                } else if (OB_FAIL(ObPatternMatcher::match_with_var_info(
                    actual_table, mapping.get_table_var_info(),
                    var_values, tbl_matched))) {
                  LOG_WARN("failed to match table pattern", K(ret),
                           K(actual_table),
                           K(mapping.get_table_fixed_prefix()),
                           K(mapping.get_table_var_name()));
                }
                if (OB_FAIL(ret)) {
                  all_matched = false;
                } else if (!tbl_matched) {
                  all_matched = false;
                  LOG_TRACE("[OUTLINE] table pattern mismatch", K(i),
                            K(actual_table),
                            K(mapping.get_table_fixed_prefix()),
                            K(mapping.get_table_var_name()));
                }
              } else {
                if (OB_FAIL(ObPatternMatcher::match_exact(actual_table,
                                                           mapping.get_original_table_name(),
                                                           tbl_matched))) {
                  LOG_WARN("failed exact table match", K(ret));
                } else if (!tbl_matched) {
                  all_matched = false;
                  LOG_TRACE("[OUTLINE] exact table mismatch", K(i),
                            K(actual_table), K(mapping.get_original_table_name()));
                }
              }

              // DB pattern matching
              if (OB_SUCC(ret) && all_matched && mapping.has_db_prefix()) {
                if (mapping.is_fixed_mapping()) {
                  if (OB_FAIL(ObPatternMatcher::match_exact(actual_db,
                                                             mapping.get_original_db_name(),
                                                             db_matched))) {
                    LOG_WARN("failed exact db match", K(ret));
                  } else if (!db_matched) {
                    all_matched = false;
                    LOG_TRACE("[OUTLINE] exact db mismatch", K(i),
                              K(actual_db), K(mapping.get_original_db_name()));
                  }
                } else if (mapping.has_db_pattern_form()) {
                  if (OB_UNLIKELY(!mapping.are_patterns_parsed())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("patterns_parsed_ not set when matching", K(ret),
                             K(actual_db), K(mapping.get_db_fixed_prefix()),
                             K(mapping.get_db_var_name()));
                  } else if (OB_FAIL(ObPatternMatcher::match_with_var_info(
                      actual_db, mapping.get_db_var_info(),
                      var_values, db_matched))) {
                    LOG_WARN("failed to match db pattern", K(ret),
                             K(actual_db), K(mapping.get_db_fixed_prefix()),
                             K(mapping.get_db_var_name()));
                  }
                  if (OB_FAIL(ret)) {
                    all_matched = false;
                  } else if (!db_matched) {
                    all_matched = false;
                    LOG_TRACE("[OUTLINE] db pattern mismatch", K(i),
                              K(actual_db), K(mapping.get_db_fixed_prefix()),
                              K(mapping.get_db_var_name()));
                  }
                } else if (!mapping.get_original_db_name().empty()) {
                  if (OB_FAIL(ObPatternMatcher::match_exact(actual_db,
                                                             mapping.get_original_db_name(),
                                                             db_matched))) {
                    LOG_WARN("failed exact db match", K(ret));
                  } else if (!db_matched) {
                    all_matched = false;
                    LOG_TRACE("[OUTLINE] exact db mismatch", K(i),
                              K(actual_db), K(mapping.get_original_db_name()));
                  }
                }
              }
            }
          }

          if (OB_SUCC(ret) && all_matched) {
            matched = true;
            LOG_TRACE("[OUTLINE] template outline matched with pattern validation",
                      K(binding_rule_ptr->get_map_item_count()),
                      K(actual_table_db_names.count()),
                      K(binding_rule_ptr->get_scope()));
          }
        }
        var_values.destroy();
      }
    }
  }

  return ret;
}

// ==================== collect_hint_table_refs_dfs (static helper) ====================
// Walk parse tree DFS to find T_RELATION_FACTOR_IN_HINT nodes (confirmed table references
// in hint context) and extract their table_name/db_name. This ensures we only replace
// identifiers that are in table-reference positions, not index names or other identifiers.

static int collect_hint_table_refs_dfs(
    const ParseNode *node,
    ObIArray<ObString> &table_names,
    ObIArray<ObString> &db_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // skip
  } else if (T_RELATION_FACTOR_IN_HINT == node->type_) {
    // Found a confirmed table reference in hint context
    // child[0] = T_RELATION_FACTOR (normal_relation_factor)
    // child[1] = qb_name_option
    if (node->num_child_ >= 1 && OB_NOT_NULL(node->children_) && OB_NOT_NULL(node->children_[0])) {
      const ParseNode *rel_factor = node->children_[0];
      if (T_RELATION_FACTOR == rel_factor->type_ && rel_factor->num_child_ >= 2) {
        const ParseNode *db_node = OB_NOT_NULL(rel_factor->children_) ? rel_factor->children_[0] : NULL;
        const ParseNode *tbl_node = OB_NOT_NULL(rel_factor->children_) ? rel_factor->children_[1] : NULL;
        ObString db_name;
        ObString tbl_name;
        if (OB_NOT_NULL(tbl_node) && OB_NOT_NULL(tbl_node->str_value_) && tbl_node->str_len_ > 0) {
          tbl_name.assign_ptr(tbl_node->str_value_, static_cast<ObString::obstr_size_t>(tbl_node->str_len_));
        }
        if (OB_NOT_NULL(db_node) && OB_NOT_NULL(db_node->str_value_) && db_node->str_len_ > 0) {
          db_name.assign_ptr(db_node->str_value_, static_cast<ObString::obstr_size_t>(db_node->str_len_));
        }
        if (!tbl_name.empty()) {
          if (OB_FAIL(table_names.push_back(tbl_name))) {
            LOG_WARN("failed to push hint table name", K(ret));
          } else if (OB_FAIL(db_names.push_back(db_name))) {
            LOG_WARN("failed to push hint db name", K(ret));
          }
        }
      }
    }
    // Do NOT recurse into T_RELATION_FACTOR_IN_HINT children (already processed)
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_NOT_NULL(node->children_) && OB_NOT_NULL(node->children_[i])) {
        if (OB_FAIL(collect_hint_table_refs_dfs(node->children_[i],
                                                table_names, db_names))) {
          LOG_WARN("failed in hint table ref DFS", K(ret));
        }
      }
    }
  }
  return ret;
}

// ==================== reconstruct_outline_content ====================
// AST-guided reconstruction: parse outline content to identify table references
// via the hint parse tree, then replace only confirmed table-reference placeholders.
// Falls back to binding_rule-based string replacement if parsing fails.

int ObOutlineTemplateMatcher::reconstruct_outline_content(
    const ObString &template_content,
    const ObOutlineBindingRule &binding_rule,
    const ObIArray<ObString> &actual_table_names,
    ObIAllocator &allocator,
    ObString &result)
{
  int ret = OB_SUCCESS;
  ObSqlString working;
  ObSqlString temp;

  // Step 1: Try AST-guided approach by parsing the outline content.
  // Parse the outline content (as a hint in synthetic SQL) to find T_RELATION_FACTOR_IN_HINT
  // nodes, which identify confirmed table references (not index names or other identifiers).
  bool use_ast = false;
  ObSEArray<ObString, 8> hint_tbl_names;
  ObSEArray<ObString, 8> hint_db_names;
  ObSqlString synthetic_sql;
  ObArenaAllocator parse_allocator(ObModIds::OB_SQL_PARSER);

  if (OB_FAIL(synthetic_sql.assign_fmt("SELECT %.*s 1 FROM DUAL",
      template_content.length(), template_content.ptr()))) {
    LOG_WARN("failed to build synthetic SQL for hint parsing", K(ret));
    ret = OB_SUCCESS; // non-fatal, return original content
  } else {
    ObParser parser(parse_allocator, SMO_DEFAULT);
    ParseResult parse_result;
    memset(&parse_result, 0, sizeof(parse_result));

    int parse_ret = parser.parse(synthetic_sql.string(), parse_result, STD_MODE,
                                 false, true/*no_throw_parser_error*/);
    if (OB_SUCCESS == parse_ret && OB_NOT_NULL(parse_result.result_tree_)) {
      if (OB_FAIL(collect_hint_table_refs_dfs(parse_result.result_tree_,
                                              hint_tbl_names, hint_db_names))) {
        LOG_WARN("failed to collect hint table refs from parse tree", K(ret));
        ret = OB_SUCCESS; // non-fatal
      } else if (hint_tbl_names.count() > 0) {
        use_ast = true;
      }
    }
  }

  // Step 2: Perform replacements
  if (OB_FAIL(working.assign(template_content))) {
    LOG_WARN("failed to copy template content", K(ret));
  } else if (use_ast) {
    // AST-guided: iterate confirmed table references from hint parse tree.
    // Only replace identifiers that appear in T_RELATION_FACTOR_IN_HINT positions.
    for (int64_t i = 0; OB_SUCC(ret) && i < hint_tbl_names.count(); ++i) {
      const ObString &hint_tbl = hint_tbl_names.at(i);
      const ObString &hint_db = hint_db_names.at(i);

      // Find matching binding_rule mapping by table placeholder name
      for (int64_t j = 0; OB_SUCC(ret) && j < binding_rule.get_map_item_count(); ++j) {
        const ObOutlineRuleMapping &mapping = binding_rule.get_map_item(j);
        if (!mapping.needs_placeholder()) {
          continue;
        }

        // Match by table placeholder
        bool tb_match = false;
        if (!mapping.get_tb_placeholder().empty()) {
          tb_match = (0 == hint_tbl.case_compare(mapping.get_tb_placeholder()));
        }
        if (!tb_match) {
          continue;
        }

        // Found match - get actual table name for this position
        int64_t position = mapping.get_ast_position();
        ObString actual_table;
        if (position > 0 && position <= actual_table_names.count()) {
          actual_table = actual_table_names.at(position - 1);
        }
        if (actual_table.empty()) {
          break;
        }

        // Build search and replacement strings in quoted format
        ObSqlString search;
        ObSqlString replacement;

        if (mapping.has_db_prefix() && !hint_db.empty()) {
          if (OB_FAIL(search.assign_fmt("\"%.*s\".\"%.*s\"",
              hint_db.length(), hint_db.ptr(),
              hint_tbl.length(), hint_tbl.ptr()))) {
            LOG_WARN("failed to build search string", K(ret));
          } else if (OB_FAIL(replacement.assign_fmt("\"%.*s\".\"%.*s\"",
              mapping.get_original_db_name().length(), mapping.get_original_db_name().ptr(),
              actual_table.length(), actual_table.ptr()))) {
            LOG_WARN("failed to build replacement", K(ret));
          }
        } else {
          if (OB_FAIL(search.assign_fmt("\"%.*s\"",
              hint_tbl.length(), hint_tbl.ptr()))) {
            LOG_WARN("failed to build search string", K(ret));
          } else if (OB_FAIL(replacement.assign_fmt("\"%.*s\"",
              actual_table.length(), actual_table.ptr()))) {
            LOG_WARN("failed to build replacement", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_replace_all_ci_local(working.string(), search.string(),
                                              replacement.string(), temp))) {
            LOG_WARN("failed to replace placeholder", K(ret), K(search.string()));
          } else if (OB_FAIL(working.assign(temp.string()))) {
            LOG_WARN("failed to update working string", K(ret));
          }
        }
        break; // found match for this hint table ref
      }
    }
  } else {
    // AST approach failed (parse failed or no table refs in hint).
    // Nothing to reconstruct — return original content as-is, same as normal outline path.
    LOG_DEBUG("[OUTLINE] AST-guided reconstruction not applicable, returning original content",
              "outline_content_len", template_content.length());
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, working.string(), result))) {
      LOG_WARN("failed to deep copy reconstructed outline content", K(ret));
    }
  }

  return ret;
}

// ==================== try_match_template_outline (facade) ====================

// Scan raw SQL for "/*+" to detect hint syntax, consistent with fast parser (is_hint_begin).
static bool stmt_parse_tree_has_hint(const ParseResult *parse_result)
{
  bool has_hint = false;
  if (OB_NOT_NULL(parse_result) && OB_NOT_NULL(parse_result->input_sql_)
      && parse_result->input_sql_len_ >= 3) {
    const char *sql = parse_result->input_sql_;
    const int64_t len = parse_result->input_sql_len_;
    for (int64_t i = 0; !has_hint && i <= len - 3; ++i) {
      has_hint = (sql[i] == '/' && sql[i + 1] == '*' && sql[i + 2] == '+');
    }
  }
  return has_hint;
}

int ObOutlineTemplateMatcher::try_gen_template_signature(
    ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObSqlCtx &sql_ctx = pc_ctx.sql_ctx_;
  ObSQLSessionInfo *session = sql_ctx.session_info_;
  ParseResult *parse_result = sql_ctx.outline_match_parse_result_;
  if (OB_ISNULL(session)) {
    // missing context: leave signature empty
  } else if (session->is_real_inner_session()) {
    // skip background inner SQL (e.g. WR snapshots); use is_real_inner_session()
    // (not is_inner()) so PL-originated user SQL still gets a signature
  } else if (!sql_ctx.outline_match_template_signature_.empty()) {
    // already produced by try_match_template_outline()
  } else if (stmt_parse_tree_has_hint(parse_result)) {
    // hinted SQL never participates in matching
  } else {
    // Parse-tree text surgery on the original SQL (no resolve). Symmetric with the
    // DDL side, which re-parses its ON-clause and runs the same worker.
    ObString template_signature;
    if (OB_SUCCESS == generate_signature_from_parse_result(
            parse_result, session, pc_ctx.allocator_, false/*need_format*/,
            template_signature)
        && !template_signature.empty()) {
      sql_ctx.outline_match_template_signature_ = template_signature;
    }
  }
  LOG_DEBUG("[OUTLINE] try_gen_template_signature",
            K(sql_ctx.outline_match_template_signature_));
  return ret;
}

int ObOutlineTemplateMatcher::try_match_template_outline(
    ObPlanCacheCtx &pc_ctx,
    ObSchemaGetterGuard *schema_guard,
    ObSQLSessionInfo *session,
    uint64_t database_id,
    const ObString &signature_sql,
    ObIAllocator &allocator,
    const ObOutlineInfo *&outline_info)
{
  int ret = OB_SUCCESS;
  outline_info = NULL;

  if (OB_ISNULL(session) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session or schema_guard", K(ret));
  } else {
    // Skip matching for hinted SQL, consistent with normal outlines: template
    // signatures are AST-derived (hints stripped), so we must check explicitly.
    ParseResult *parse_result = pc_ctx.sql_ctx_.outline_match_parse_result_;
    if (stmt_parse_tree_has_hint(parse_result)) {
      LOG_DEBUG("[OUTLINE] query SQL contains user hints, skip template outline matching");
    } else {
    bool has_template = false;
    if (OB_FAIL(schema_guard->has_template_outline(session->get_effective_tenant_id(), has_template))) {
      LOG_WARN("failed to check has_template_outline", K(ret));
    } else if (!has_template) {
      LOG_DEBUG("[OUTLINE] no template outlines, skip template matching",
               K(session->get_effective_tenant_id()));
    } else {
      LOG_TRACE("[OUTLINE] checking template outlines", K(session->get_effective_tenant_id()),
                K(database_id));

      // Generate template signature via parse-tree text surgery (no resolve).
      ObString template_signature;
      int ast_ret = generate_signature_from_parse_result(
          parse_result, session, allocator, false/*need_format*/, template_signature);
      LOG_DEBUG("[OUTLINE] generated template signature", K(ast_ret), K(template_signature));
      if (OB_SUCCESS == ast_ret) {
        pc_ctx.sql_ctx_.outline_match_template_signature_ = template_signature;
      } else {
        LOG_WARN("failed to generate template signature from parse tree",
                 K(ast_ret), "original_sql_sig", signature_sql);
      }

      if (OB_SUCC(ret) && !template_signature.empty()) {
        // Try matching with current database_id
        ObArray<const ObOutlineInfo *> template_candidates;
        if (OB_FAIL(schema_guard->get_outline_infos_with_signature(
            session->get_effective_tenant_id(),
            database_id,
            template_signature,
            false,
            template_candidates))) {
          LOG_WARN("failed to get template candidates", K(ret));
        }

        // Try matching with OB_PUBLIC_SCHEMA_ID for tenant-level outlines
        if (OB_SUCC(ret) && database_id != OB_PUBLIC_SCHEMA_ID) {
          ObArray<const ObOutlineInfo *> tenant_candidates;
          if (OB_FAIL(schema_guard->get_outline_infos_with_signature(
              session->get_effective_tenant_id(),
              OB_PUBLIC_SCHEMA_ID,
              template_signature,
              false,
              tenant_candidates))) {
            LOG_WARN("failed to get tenant template candidates", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < tenant_candidates.count(); ++i) {
              if (OB_FAIL(template_candidates.push_back(tenant_candidates.at(i)))) {
                LOG_WARN("failed to append tenant candidate", K(ret));
              }
            }
          }
        }

        // Perform pattern matching for each candidate
        if (OB_SUCC(ret)) {
          LOG_TRACE("[OUTLINE] found template candidates", K(template_candidates.count()));
          for (int64_t i = 0; OB_SUCC(ret) && NULL == outline_info && i < template_candidates.count(); ++i) {
            const ObOutlineInfo *candidate = template_candidates.at(i);
            if (OB_ISNULL(candidate)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL candidate", K(ret), K(i));
            } else if (!candidate->get_pattern_rules_str().empty()) {
              bool matched = false;
              if (OB_FAIL(match_template_outline(pc_ctx, *candidate, matched))) {
                LOG_WARN("failed to match template outline", K(ret), K(i));
              } else if (matched) {
                outline_info = candidate;
                LOG_DEBUG("[OUTLINE] template outline matched",
                          K(session->get_effective_tenant_id()),
                          K(database_id),
                          "outline_id", candidate->get_outline_id(),
                          "outline_name", candidate->get_name_str());
              }
            }
          }
        }
      }
    }
    } // end of else (no user hints)
  }
  return ret;
}

// ==================== reconstruct_content_if_needed (facade) ====================

int ObOutlineTemplateMatcher::reconstruct_content_if_needed(
    ObPlanCacheCtx &pc_ctx,
    const ObOutlineInfo *outline_info,
    ObIAllocator &allocator,
    ObString &outline_content)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(outline_info) || outline_info->get_pattern_rules_str().empty()) {
    // nothing to reconstruct
  } else {
    ObOutlineBindingRule binding_rule;
    if (OB_FAIL(ObOutlineBindingRule::deserialize_from_json(outline_info->get_pattern_rules_str(),
                                          allocator, binding_rule))) {
      LOG_WARN("failed to parse pattern_rules for reconstruction", K(ret));
      ret = OB_SUCCESS;
    } else {
      ObSEArray<ObTableDbName, 16> actual_table_db_names;
      ParseResult *parse_result = pc_ctx.sql_ctx_.outline_match_parse_result_;

      if (OB_NOT_NULL(parse_result) && OB_NOT_NULL(parse_result->result_tree_) &&
          parse_result->result_tree_->num_child_ > 0 &&
          OB_NOT_NULL(parse_result->result_tree_->children_[0])) {
        ParseNode *stmt_node = parse_result->result_tree_->children_[0];
        if (OB_NOT_NULL(stmt_node)) {
          if (OB_FAIL(collect_table_names_dfs(stmt_node, actual_table_db_names))) {
            LOG_WARN("failed to collect actual table names for reconstruction", K(ret));
            ret = OB_SUCCESS;
          }
        }
      }

      // Build actual_table_names for reconstruct_outline_content
      ObSEArray<ObString, 16> actual_table_names;
      if (OB_SUCC(ret) && actual_table_db_names.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < actual_table_db_names.count(); ++i) {
          if (OB_FAIL(actual_table_names.push_back(actual_table_db_names.at(i).first))) {
            LOG_WARN("failed to extract table name from pair", K(ret));
          }
        }
      }

      // Fallback to binding rule's original names if parse tree extraction failed
      if (OB_SUCC(ret) && actual_table_names.empty()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < binding_rule.get_map_item_count(); ++i) {
          const ObOutlineRuleMapping &mapping = binding_rule.get_map_item(i);
          if (OB_FAIL(actual_table_names.push_back(mapping.get_original_table_name()))) {
            LOG_WARN("failed to push fallback table name", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && binding_rule.get_map_item_count() > 0) {
        ObString reconstructed;
        if (OB_FAIL(reconstruct_outline_content(outline_content, binding_rule,
                                                 actual_table_names, allocator,
                                                 reconstructed))) {
          LOG_WARN("failed to reconstruct outline content", K(ret));
          ret = OB_SUCCESS;
        } else {
          outline_content = reconstructed;
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
