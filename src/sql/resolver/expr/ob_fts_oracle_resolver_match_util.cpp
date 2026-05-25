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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/expr/ob_fts_oracle_resolver_match_util.h"
#include "sql/parser/sql_parser_base.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/number/ob_number_v2.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace sql
{

int ObFtsOracleMatchExprUtil::convert_contains_to_match_against(
    const ParseNode &node,
    void *malloc_pool,
    const common::ParamStore &param_list,
    ParseNode *&match_against_node)
{
  int ret = OB_SUCCESS;
  match_against_node = NULL;

  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(malloc_pool));
  } else {
    ParseNode *column_node = NULL;
    ParseNode *search_string_node = NULL;
    ParseNode *label_node = NULL;
    int64_t contains_label = -1; // -1 means no label

    // Parse parameters (validation is done inside parse_contains_params_)
    if (OB_FAIL(parse_contains_params_(node, column_node, search_string_node, label_node))) {
      LOG_WARN("failed to parse CONTAINS parameters", K(ret));
    } else if (OB_NOT_NULL(label_node)) {
      // Extract label value if present
      if (OB_FAIL(extract_contains_label_(label_node, param_list, contains_label))) {
        LOG_WARN("failed to extract CONTAINS label", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(column_node) && OB_NOT_NULL(search_string_node)) {
      // Convert column_node to T_COLUMN_REF format
      ParseNode *column_ref_node = NULL;
      if (OB_FAIL(convert_column_to_ref_(*column_node, malloc_pool, column_ref_node))) {
        LOG_WARN("failed to convert column to ref", K(ret));
      } else if (OB_NOT_NULL(column_ref_node)) {
        // Create MATCH_AGAINST node
        if (OB_FAIL(create_match_against_node_(
            malloc_pool, *column_ref_node, *search_string_node, contains_label, match_against_node))) {
          LOG_WARN("failed to create match against node", K(ret));
        }
      }
    }
  }
  return ret;
}

// Split CONTAINS argument list into column / search string / optional label parse nodes.
int ObFtsOracleMatchExprUtil::parse_contains_params_(
    const ParseNode &node,
    ParseNode *&column_node,
    ParseNode *&search_string_node,
    ParseNode *&label_node)
{
  int ret = OB_SUCCESS;
  column_node = NULL;
  search_string_node = NULL;
  label_node = NULL;

  if (OB_ISNULL(node.children_[1])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid CONTAINS parameters: missing children", K(ret));
  } else if (OB_UNLIKELY(T_EXPR_LIST != node.children_[1]->type_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid CONTAINS parameters: children_[1] is not T_EXPR_LIST", K(ret));
  } else {
    const int param_count = node.children_[1]->num_child_;
    if (param_count != 2 && param_count != 3) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, "CONTAINS", "2 or 3", static_cast<int>(param_count));
      LOG_WARN("CONTAINS requires 2 or 3 parameters", K(ret));
    } else {
      ParseNode *link_node = NULL;

      if (param_count == 3) {
      // 3 parameters: column, search_string, label
      if (OB_ISNULL(node.children_[1]->children_[0])) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid CONTAINS parameters: missing T_LINK_NODE", K(ret));
      } else {
        const int first_child_type = node.children_[1]->children_[0]->type_;
        if (first_child_type == T_LINK_NODE) {
          link_node = node.children_[1]->children_[0];
          if (OB_UNLIKELY(link_node->num_child_ != 3)) {
            ret = OB_ERR_PARAM_SIZE;
            LOG_USER_ERROR(OB_ERR_PARAM_SIZE, "CONTAINS", 3, static_cast<int>(link_node->num_child_));
            LOG_WARN("CONTAINS requires 3 parameters: column, search_string, label", K(ret));
          } else {
            column_node = link_node->children_[0];
            search_string_node = link_node->children_[1];
            label_node = link_node->children_[2];
          }
        } else {
          // Direct children (not wrapped in T_LINK_NODE)
          column_node = node.children_[1]->children_[0];
          search_string_node = node.children_[1]->children_[1];
          label_node = node.children_[1]->children_[2];
        }
      }
      } else {
        // 2 parameters: column, search_string
        if (OB_ISNULL(node.children_[1]->children_[0])) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("invalid CONTAINS parameters: missing T_LINK_NODE", K(ret));
        } else {
          const int first_child_type = node.children_[1]->children_[0]->type_;
          if (first_child_type == T_LINK_NODE) {
            link_node = node.children_[1]->children_[0];
            if (OB_UNLIKELY(link_node->num_child_ != 2)) {
              ret = OB_ERR_PARAM_SIZE;
              LOG_USER_ERROR(OB_ERR_PARAM_SIZE, "CONTAINS", 2, static_cast<int>(link_node->num_child_));
              LOG_WARN("CONTAINS requires 2 parameters: column, search_string", K(ret));
            } else {
              column_node = link_node->children_[0];
              search_string_node = link_node->children_[1];
            }
          } else {
            // Direct children (not wrapped in T_LINK_NODE)
            column_node = node.children_[1]->children_[0];
            search_string_node = node.children_[1]->children_[1];
          }
        }
      }
    }
  }
  return ret;
}

// Validate CONTAINS label literal or bound parameter as positive integer (Oracle).
int ObFtsOracleMatchExprUtil::extract_contains_label_(
    const ParseNode *label_node,
    const common::ParamStore &param_list,
    int64_t &contains_label)
{
  int ret = OB_SUCCESS;
  contains_label = -1; // -1 means no label

  if (OB_ISNULL(label_node)) {
    // No label node, label remains -1
  } else if (T_INT == label_node->type_) {
    contains_label = label_node->value_;
    LOG_DEBUG("Extracted CONTAINS label from T_INT", K(contains_label), K(label_node->value_));
    // Validate: label must be a positive integer (> 0)
    if (contains_label <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("CONTAINS label must be a positive integer", K(ret), K(contains_label));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "CONTAINS label must be a positive integer");
    }
  } else if (T_QUESTIONMARK == label_node->type_) {
    // Extract label from parameterized value
    if (OB_UNLIKELY(label_node->value_ < 0 || label_node->value_ >= param_list.count())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("CONTAINS label parameter index out of range",
              K(ret), K(label_node->value_), K(param_list.count()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS label must be a literal integer constant, others is");
    } else {
      const common::ObObjParam &param = param_list.at(label_node->value_);
      number::ObNumber num_val;
      int64_t label_val = 0;
      // Oracle integer literals are stored as ObNumberType
      if (OB_FAIL(param.get_number(num_val))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("CONTAINS label parameter is not a number type", K(ret), K(param.get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS label must be a literal integer constant, others is");
      } else if (OB_FAIL(num_val.cast_to_int64(label_val))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("CONTAINS label parameter number cannot be cast to int64", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS label must be a literal integer constant, others is");
      } else {
        contains_label = label_val;
        LOG_DEBUG("Extracted CONTAINS label", K(contains_label), K(label_val));
        // Validate: label must be a positive integer (> 0)
        if (contains_label <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("CONTAINS label must be a positive integer", K(ret), K(contains_label));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "CONTAINS label must be a positive integer");
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("CONTAINS label must be an integer literal or parameterized integer", K(ret), K(label_node->type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS label must be a literal integer constant");
  }
  return ret;
}

// Normalize column parse node to T_COLUMN_REF for MATCH_AGAINST construction.
int ObFtsOracleMatchExprUtil::convert_column_to_ref_(
    const ParseNode &column_node,
    void *malloc_pool,
    ParseNode *&column_ref_node)
{
  int ret = OB_SUCCESS;
  column_ref_node = NULL;

  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(malloc_pool));
  } else if (T_COLUMN_REF == column_node.type_) {
    column_ref_node = const_cast<ParseNode*>(&column_node);
  } else {
    // Convert T_IDENT, T_OBJ_ACCESS_REF, or other column reference types to T_COLUMN_REF
    ParseNode *col_name_node = NULL;
    if (T_IDENT == column_node.type_) {
      col_name_node = const_cast<ParseNode*>(&column_node);
    } else if (T_OBJ_ACCESS_REF == column_node.type_) {
      ParseNode *obj_node = const_cast<ParseNode*>(&column_node);
      while (OB_NOT_NULL(obj_node) && T_OBJ_ACCESS_REF == obj_node->type_
            && OB_NOT_NULL(obj_node->children_) && OB_NOT_NULL(obj_node->children_[1])) {
        obj_node = obj_node->children_[1];
      }
      if (OB_NOT_NULL(obj_node) && T_IDENT == obj_node->type_) {
        col_name_node = obj_node;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("cannot extract column name from object access ref", K(ret));
      }
    } else if (column_node.num_child_ >= 1 && OB_NOT_NULL(column_node.children_[0])) {
      // Handle cases where column_node has children
      ParseNode *first_child = column_node.children_[0];
      if (first_child->str_len_ > 0 && OB_NOT_NULL(first_child->str_value_)) {
        col_name_node = new_terminal_node(malloc_pool, T_IDENT);
        if (OB_ISNULL(col_name_node)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create T_IDENT node", K(ret));
        } else {
          col_name_node->str_value_ = first_child->str_value_;
          col_name_node->str_len_ = first_child->str_len_;
          col_name_node->value_ = first_child->value_;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column node first child has no string value", K(ret), K(column_node.type_));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected column node type", K(ret), K(column_node.type_), K(column_node.num_child_));
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(col_name_node)) {
      column_ref_node = new_non_terminal_node(malloc_pool,
                                              T_COLUMN_REF, 3,
                                              NULL,  // database_name
                                              NULL,  // table_name
                                              col_name_node);  // column_name
      if (OB_ISNULL(column_ref_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create column ref node", K(ret));
      }
    }
  }
  return ret;
}

// Build T_FUN_MATCH_AGAINST node; stash Oracle CONTAINS label in int32_values_[0] when applicable.
int ObFtsOracleMatchExprUtil::create_match_against_node_(
    void *malloc_pool,
    ParseNode &column_ref_node,
    ParseNode &search_string_node,
    const int64_t contains_label,
    ParseNode *&match_against_node)
{
  int ret = OB_SUCCESS;
  match_against_node = NULL;

  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(malloc_pool));
  } else {
    // Create column list node
    ParseNode *column_list_node = new_non_terminal_node(malloc_pool,
                                                        T_MATCH_COLUMN_LIST, 1,
                                                        &column_ref_node);
    if (OB_ISNULL(column_list_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create column list node", K(ret));
    } else {
      // Create MATCH_AGAINST node
      match_against_node = new_non_terminal_node(malloc_pool,
                                                  T_FUN_MATCH_AGAINST, 2,
                                                  column_list_node,
                                                  &search_string_node);
      if (OB_ISNULL(match_against_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create match against node", K(ret));
      } else {
        match_against_node->value_ = 4; // MATCH_PHRASE_MODE
        // Store contains_label in int32_values_[0] ONLY in Oracle mode
        // In MySQL mode, int32_values_[0] is used for parser type, so we must not overwrite it
        if (lib::is_oracle_mode()) {
          if (contains_label >= 0) {
            match_against_node->int32_values_[0] = static_cast<int32_t>(contains_label);
          } else {
            match_against_node->int32_values_[0] = -1;
          }
          LOG_DEBUG("Oracle CONTAINS: stored label in int32_values_[0]",
                    K(match_against_node->value_), K(match_against_node->int32_values_[0]));
        } else {
          // MySQL mode: do not touch int32_values_[0], it's used for parser type
          LOG_DEBUG("MySQL mode: not storing label in int32_values_[0]");
        }
      }
    }
  }
  return ret;
}

// Parser bridge: Oracle SCORE(label) -> T_FUN_ES_SCORE with validated positive label.
int ObFtsOracleMatchExprUtil::convert_score_to_es_score(
    const ParseNode &node,
    void *malloc_pool,
    const common::ParamStore &param_list,
    ParseNode *&score_node)
{
  int ret = OB_SUCCESS;
  score_node = NULL;

  if (OB_ISNULL(malloc_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(malloc_pool));
  } else if (OB_ISNULL(node.children_[1]) || OB_UNLIKELY(T_EXPR_LIST != node.children_[1]->type_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid SCORE parameters", K(ret));
  } else if (node.children_[1]->num_child_ != 1) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, "SCORE", 1, static_cast<int>(node.children_[1]->num_child_));
    LOG_WARN("SCORE requires 1 parameter: label", K(ret));
  } else {
    score_node = new_terminal_node(malloc_pool, T_FUN_ES_SCORE);
    if (OB_ISNULL(score_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create score node", K(ret));
    } else {
      // Extract label value from SCORE(label)
      ParseNode *label_node = node.children_[1]->children_[0];
      int64_t score_label = -1; // -1 means no label

      if (OB_NOT_NULL(label_node)) {
        if (T_INT == label_node->type_) {
          score_label = label_node->value_;
          LOG_DEBUG("Extracted SCORE label from T_INT", K(score_label), K(label_node->value_));
          // Validate: label must be a positive integer (> 0)
          if (score_label <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("SCORE label must be a positive integer", K(ret), K(score_label));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SCORE label must be a positive integer");
          }
        } else if (T_QUESTIONMARK == label_node->type_) {
          // Extract label from parameterized value
          // Oracle parser parameterizes integer literals as ObNumberType, so we directly use get_number()
          if (OB_UNLIKELY(label_node->value_ < 0 || label_node->value_ >= param_list.count())) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("SCORE label parameter index out of range",
                     K(ret), K(label_node->value_), K(param_list.count()));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "SCORE label must be a literal integer constant");
          } else {
            const common::ObObjParam &param = param_list.at(label_node->value_);
            number::ObNumber num_val;
            int64_t label_val = 0;
            // Oracle integer literals are stored as ObNumberType, so we use get_number() and cast to int64
            if (OB_FAIL(param.get_number(num_val))) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("SCORE label parameter is not a number type", K(ret), K(param.get_type()));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "SCORE label must be a literal integer constant");
            } else if (OB_FAIL(num_val.cast_to_int64(label_val))) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("SCORE label parameter number cannot be cast to int64", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "SCORE label must be a literal integer constant");
            } else {
              score_label = label_val;
              LOG_DEBUG("Extracted SCORE label", K(score_label), K(label_val));
              // Validate: label must be a positive integer (> 0)
              if (score_label <= 0) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("SCORE label must be a positive integer", K(ret), K(score_label));
                LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SCORE label must be a positive integer");
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("SCORE label must be an integer literal or parameterized integer", K(ret), K(label_node->type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "SCORE label must be a literal integer constant");
        }

        // Store label in score_node->value_ for later use in process_match_score
        if (OB_SUCC(ret)) {
          score_node->value_ = score_label;
        }
      } else {
        LOG_INFO("SCORE has no label node, score_label remains -1", K(score_label));
      }
    }
  }

  return ret;
}

// Set MATCH_AGAINST mode / Oracle label on raw expr after parse (phrase mode for Oracle CONTAINS).
int ObFtsOracleMatchExprUtil::validate_and_set_match_against_mode(
    const ParseNode &node,
    const int64_t data_version,
    const common::ObIArray<sql::ObMatchFunRawExpr *> *match_exprs,
    sql::ObMatchFunRawExpr &match_against)
{
  int ret = OB_SUCCESS;
  ObMatchAgainstMode mode_to_use = ObMatchAgainstMode::MAX_MATCH_AGAINST_MODE;
  bool is_oracle_contains = false;

  if (lib::is_mysql_mode()) {
    // MySQL mode: int32_values_[0] is used for parser type, not label
    // Validate the mode from node.value_
    mode_to_use = static_cast<ObMatchAgainstMode>(node.value_);
    if (data_version < DATA_VERSION_4_4_0_0 &&
        ObMatchAgainstMode::NATURAL_LANGUAGE_MODE != mode_to_use &&
        ObMatchAgainstMode::BOOLEAN_MODE != mode_to_use) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "search modes other than NATURAL_LANGUAGE_MODE or BOOLEAN_MODE");
      LOG_WARN("unsupported match against mode", K(ret), K(node.value_));
    } else if (data_version >= DATA_VERSION_4_4_0_0 &&
               ObMatchAgainstMode::NATURAL_LANGUAGE_MODE != mode_to_use &&
               ObMatchAgainstMode::BOOLEAN_MODE != mode_to_use &&
               ObMatchAgainstMode::MATCH_PHRASE_MODE != mode_to_use) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "search modes other than NATURAL_LANGUAGE_MODE, BOOLEAN_MODE or MATCH_PHRASE_MODE");
      LOG_WARN("unsupported match against mode", K(ret), K(node.value_));
    }
  } else {
    // Oracle mode: use MATCH_PHRASE_MODE
    is_oracle_contains = true;
    mode_to_use = ObMatchAgainstMode::MATCH_PHRASE_MODE;
  }

  if (OB_SUCC(ret)) {
    match_against.set_mode_flag(mode_to_use);
    // Extract label from int32_values_[0] ONLY if it's a converted Oracle CONTAINS node
    if (is_oracle_contains) {
      match_against.set_score_label(node.int32_values_[0]);
      LOG_DEBUG("Set CONTAINS label in match_against",
                K(node.int32_values_[0]),
                K(match_against.get_score_label()));
    }
  }
  return ret;
}

// Enforce distinct positive labels across multiple CONTAINS in one statement.
int ObFtsOracleMatchExprUtil::check_duplicate_contains_labels(
    const common::ObIArray<sql::ObMatchFunRawExpr *> &match_exprs)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 4> seen_labels;
  for (int64_t i = 0; OB_SUCC(ret) && i < match_exprs.count(); ++i) {
    const sql::ObMatchFunRawExpr *expr = match_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("match_expr is null", K(ret), K(i));
      break;
    }
    const int64_t label = expr->get_score_label();
    if (label < 0) {
      continue;
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < seen_labels.count(); ++j) {
      if (seen_labels.at(j) == label) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("duplicate CONTAINS labels found", K(ret), K(label));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate CONTAINS labels found in primary operator calls");
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(seen_labels.push_back(label))) {
      LOG_WARN("failed to push back label", K(ret), K(label));
    }
  }
  return ret;
}

//Case A: SCORE labels ⊆ CONTAINS labels
//Case B: CONTAINS labels ⊆ SCORE labels
//Case C: Intersection (both have labels the other doesn't)
//Handled by the following call chain:
//ObRawExprResolverImpl::process_match_score               --SQL parser creates separate ParseNode for each SCORE(label), each calls process_match_score individually
//│
//└── find_and_validate_score_match          --Therefore score_label is a single value, not an array
//    │
//    └── find_matching_contains_for_score_  --Iterates through all match_exprs via for loop [optimization deferred]
int ObFtsOracleMatchExprUtil::find_and_validate_score_match(
    const int64_t score_label,
    const common::ObIArray<sql::ObMatchFunRawExpr *> &match_exprs,
    sql::ObMatchFunRawExpr *&matched_match_expr)
{
  int ret = OB_SUCCESS;
  matched_match_expr = NULL;

  if (OB_FAIL(check_duplicate_contains_labels(match_exprs))) {
    LOG_WARN("duplicate CONTAINS labels found", K(ret));
  } else if (score_label < 0) {
    // No label specified, use first match_expr
    if (match_exprs.count() > 0) {
      matched_match_expr = match_exprs.at(0);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("No match_expr available for SCORE without label", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "No match_expr available for SCORE without label");
    }
  } else {
    // Find match_expr with matching label using utility function
    // Validation and error logging are done inside the function
    if (OB_FAIL(find_matching_contains_for_score_(
        score_label,
        match_exprs,
        matched_match_expr))) {
      LOG_WARN("failed to find matching contains for score", K(ret), K(score_label));
    }
  }

  return ret;
}

// Lookup CONTAINS match expr by score label; validates type is MATCH_AGAINST.
int ObFtsOracleMatchExprUtil::find_matching_contains_for_score_(
  const int64_t score_label,
  const common::ObIArray<sql::ObMatchFunRawExpr *> &match_exprs,
  sql::ObMatchFunRawExpr *&matched_match_expr)
{
  int ret = OB_SUCCESS;
  matched_match_expr = NULL;
  bool found = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < match_exprs.count() && !found; ++i) {
    sql::ObMatchFunRawExpr *match_expr = match_exprs.at(i);
    if (OB_ISNULL(match_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("match_expr is null", K(ret), K(i));
    } else {
      const int64_t contains_label = match_expr->get_score_label();
      // Check if this match_expr has the matching label
      if (OB_SUCC(ret) && contains_label == score_label && !found) {
        matched_match_expr = match_expr;
        found = true;
        LOG_DEBUG("Found matching CONTAINS label for SCORE", K(score_label), K(contains_label), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(matched_match_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("SCORE label does not match any CONTAINS label", K(ret), K(score_label));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "some SCORE labels do not match any CONTAINS labels");
    } else if (T_FUN_MATCH_AGAINST != matched_match_expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("The score with unsupported match type", K(ret), K(matched_match_expr->get_expr_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "The score with unsupported match type");
    }
  }

  return ret;
}

// EXPLAIN / pretty-print: emit Oracle CONTAINS(...) text from MATCH_AGAINST raw expr.
int ObFtsOracleMatchExprUtil::serialize_contains_expr(
    const sql::ObMatchFunRawExpr &match_expr,
    const sql::ExplainType type,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode() || match_expr.is_es_match()) {
    // Skip non-Oracle mode or ES match expressions
  } else if (match_expr.get_match_columns().count() == 1) {
    // Oracle mode: serialize as CONTAINS(column, 'search')
    // Single column: CONTAINS(column, 'search')
    if (OB_ISNULL(match_expr.get_match_columns().at(0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CONTAINS("))) {
      LOG_WARN("fail to databuff_printf", K(ret));
    } else if (OB_FAIL(match_expr.get_match_columns().at(0)->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("fail to get_name", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
      LOG_WARN("fail to databuff_printf", K(ret));
    } else if (OB_FAIL(match_expr.get_search_key()->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("fail to get_name", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("fail to databuff_printf", K(ret));
    } else if (EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "("))) {
        LOG_WARN("fail to databuff_printf", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", &match_expr))) {
        LOG_WARN("fail to databuff_printf", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to databuff_printf", K(ret));
      }
    }
  } else {
    // Oracle mode: serialize as CONTAINS(column1, column2, 'search')
    // Multiple columns: CONTAINS(column1, column2, 'search')
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CONTAINS("))) {
      LOG_WARN("fail to databuff_printf", K(ret));
    } else {
      // Print all column names
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < match_expr.get_match_columns().count(); ++i) {
        if (OB_ISNULL(match_expr.get_match_columns().at(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(match_expr.get_match_columns().at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < match_expr.get_match_columns().count() - 1
                   && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
          LOG_WARN("fail to databuff_printf", K(ret));
        }
      }
      // Print search key and closing parenthesis
      if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        LOG_WARN("fail to databuff_printf", K(ret));
      } else if (OB_SUCC(ret) && OB_FAIL(match_expr.get_search_key()->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name", K(ret));
      } else if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to databuff_printf", K(ret));
      } else if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "("))) {
          LOG_WARN("fail to databuff_printf", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", &match_expr))) {
          LOG_WARN("fail to databuff_printf", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          LOG_WARN("fail to databuff_printf", K(ret));
        }
      }
    }
  }
  return ret;
}

// Oracle CONTAINS/MATCH WHERE legality (resolver): per conjunct after resolve+bool; not optimizer ctdef.
int ObFtsOracleMatchExprUtil::rhs_is_literal_numeric_zero_(sql::ObRawExpr *right_expr, bool &is_zero)
{
  int ret = OB_SUCCESS;
  is_zero = false;
  sql::ObRawExpr *stripped_rhs = right_expr;
  while (OB_NOT_NULL(stripped_rhs) && stripped_rhs->get_expr_type() == T_FUN_SYS_CAST) {
    stripped_rhs = stripped_rhs->get_param_expr(0);
  }
  if (OB_NOT_NULL(stripped_rhs) && stripped_rhs->is_const_expr()) {
    sql::ObConstRawExpr *const_raw_expr = static_cast<sql::ObConstRawExpr *>(stripped_rhs);
    const common::ObObj &value = const_raw_expr->get_value();
    const common::ObObj *actual = &value;
    if (value.is_unknown()) {
      const common::ObObj &param_obj = const_raw_expr->get_param();
      if (param_obj.is_numeric_type() && param_obj.is_zero()) {
        actual = &param_obj;
      }
    }
    if (actual->is_integer_type() && actual->get_int() == 0) {
      is_zero = true;
    } else if (actual->is_number() && actual->is_zero_number()) {
      is_zero = true;
    }
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::match_relop_vs_zero_policy_(sql::ObRawExpr *cmp_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oracle compat: null comparison expr in MATCH-vs-zero policy", K(ret));
  } else {
    sql::ObRawExpr *lhs_after_strip = cmp_expr->get_param_expr(0);
    while (OB_NOT_NULL(lhs_after_strip) && lhs_after_strip->get_expr_type() == T_FUN_SYS_CAST) {
      lhs_after_strip = lhs_after_strip->get_param_expr(0);
    }
    sql::ObRawExpr *rhs_expr = cmp_expr->get_param_expr(1);
    bool rhs_zero = false;
    if (OB_ISNULL(lhs_after_strip) || !lhs_after_strip->has_flag(IS_MATCH_EXPR)) {
      // do nothing: not a MATCH-on-LHS comparison; skip Oracle relop-vs-zero check
    } else if (OB_FAIL(rhs_is_literal_numeric_zero_(rhs_expr, rhs_zero))) {
      LOG_WARN("oracle compat: failed to check RHS literal zero for MATCH comparison",
               K(ret), KPC(rhs_expr), KPC(cmp_expr));
    } else if (!rhs_zero) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS() must use '> 0', others is");
      LOG_WARN("CONTAINS() must use '> 0', others is", K(ret));
    } else if (cmp_expr->get_expr_type() != T_OP_GT) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "CONTAINS() must use '> 0', others is");
      LOG_WARN("CONTAINS() must use '> 0', others is", K(ret));
    }
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::walk_conjunct_contains_policy_(sql::ObRawExpr *cur_expr, const int32_t depth)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_expr) || depth <= 0) {
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("oracle compat: null expr in CONTAINS WHERE policy walk", K(ret));
    }
  } else if (!cur_expr->has_flag(IS_MATCH_EXPR) && !cur_expr->has_flag(CNT_MATCH_EXPR)) {
    // do nothing: no CONTAINS/MATCH on this node; no policy action in this walk
  } else if (cur_expr->get_expr_type() == T_OP_NOT) {
    sql::ObRawExpr *not_operand = cur_expr->get_param_expr(0);
    if (OB_NOT_NULL(not_operand) && not_operand->get_expr_type() == T_OP_GT) {
      sql::ObRawExpr *gt_lhs_stripped = not_operand->get_param_expr(0);
      while (OB_NOT_NULL(gt_lhs_stripped) && gt_lhs_stripped->get_expr_type() == T_FUN_SYS_CAST) {
        gt_lhs_stripped = gt_lhs_stripped->get_param_expr(0);
      }
      sql::ObRawExpr *gt_rhs_expr = not_operand->get_param_expr(1);
      bool rhs_is_literal_zero = false;
      if (OB_FAIL(rhs_is_literal_numeric_zero_(gt_rhs_expr, rhs_is_literal_zero))) {
        LOG_WARN("oracle compat: failed RHS literal-zero check under NOT+GT",
                 K(ret), KPC(gt_rhs_expr), KPC(not_operand));
      } else if (OB_NOT_NULL(gt_lhs_stripped) && gt_lhs_stripped->has_flag(IS_MATCH_EXPR)
                 && rhs_is_literal_zero) {
        // do nothing: NOT(MATCH > 0) is the allowed negated form
      } else if (OB_FAIL(match_relop_vs_zero_policy_(not_operand))) {
        LOG_WARN("oracle compat: NOT inner GT failed MATCH-vs-zero policy", K(ret), KPC(not_operand));
      } else {
        for (int32_t param_idx = 0; OB_SUCC(ret) && param_idx < not_operand->get_param_count(); ++param_idx) {
          sql::ObRawExpr *gt_child_expr = not_operand->get_param_expr(param_idx);
          if (OB_NOT_NULL(gt_child_expr)
              && (gt_child_expr->has_flag(IS_MATCH_EXPR) || gt_child_expr->has_flag(CNT_MATCH_EXPR))
              && OB_FAIL(walk_conjunct_contains_policy_(gt_child_expr, depth - 1))) {
            LOG_WARN("oracle compat: failed recursive CONTAINS policy on NOT inner child",
                     K(ret), K(param_idx), KPC(gt_child_expr));
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("oracle compat: failed CONTAINS policy under NOT+GT", K(ret), KPC(not_operand), K(depth));
      }
    } else if (OB_FAIL(walk_conjunct_contains_policy_(not_operand, depth - 1))) {
      LOG_WARN("oracle compat: failed CONTAINS policy walk under NOT", K(ret), KPC(not_operand), K(depth));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("oracle compat: failed CONTAINS policy on NOT subtree", K(ret), KPC(cur_expr));
    }
  } else {
    const ObItemType node_item_type = cur_expr->get_expr_type();
    if (node_item_type == T_OP_LT || node_item_type == T_OP_LE || node_item_type == T_OP_GT
        || node_item_type == T_OP_GE || node_item_type == T_OP_EQ || node_item_type == T_OP_NE) {
      if (OB_FAIL(match_relop_vs_zero_policy_(cur_expr))) {
        LOG_WARN("oracle compat: comparison failed MATCH-vs-zero policy", K(ret), KPC(cur_expr));
      }
    } else if (node_item_type == T_OP_AND || node_item_type == T_OP_OR) {
      for (int32_t idx = 0; OB_SUCC(ret) && idx < cur_expr->get_param_count(); ++idx) {
        sql::ObRawExpr *child_expr = cur_expr->get_param_expr(idx);
        if (OB_NOT_NULL(child_expr)
            && (child_expr->has_flag(IS_MATCH_EXPR) || child_expr->has_flag(CNT_MATCH_EXPR))
            && OB_FAIL(walk_conjunct_contains_policy_(child_expr, depth - 1))) {
          LOG_WARN("oracle compat: failed CONTAINS policy under AND/OR child", K(ret), K(idx), KPC(child_expr));
        }
      }
    } else {
      for (int32_t idx = 0; OB_SUCC(ret) && idx < cur_expr->get_param_count(); ++idx) {
        sql::ObRawExpr *child_expr = cur_expr->get_param_expr(idx);
        if (OB_NOT_NULL(child_expr)
            && (child_expr->has_flag(IS_MATCH_EXPR) || child_expr->has_flag(CNT_MATCH_EXPR))
            && OB_FAIL(walk_conjunct_contains_policy_(child_expr, depth - 1))) {
          LOG_WARN("oracle compat: failed CONTAINS policy on generic expr child", K(ret), K(idx), KPC(child_expr));
        }
      }
    }
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::check_expr_after_resolve(sql::ObRawExpr *conjunct_root)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    // skip: CONTAINS WHERE policy applies only in Oracle mode
  } else if (OB_ISNULL(conjunct_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oracle compat: null WHERE conjunct root for CONTAINS policy check", K(ret));
  } else if (OB_FAIL(walk_conjunct_contains_policy_(conjunct_root, 24))) {
    LOG_WARN("oracle compat: CONTAINS WHERE policy check failed on conjunct root", K(ret), KPC(conjunct_root));
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::attach_oracle_phrase_like_for_score(ObMatchFunRawExpr *match_expr, ObRawExpr *like_expr)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && OB_NOT_NULL(match_expr) && OB_NOT_NULL(like_expr)) {
    match_expr->set_oracle_phrase_like_expr(like_expr);
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::wrap_oracle_phrase_score_expr_if_applicable(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo *session_info,
    ObMatchFunRawExpr *matched_match_expr,
    ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  out_expr = matched_match_expr;
  if (!lib::is_oracle_mode() || OB_ISNULL(matched_match_expr)) {
    // keep out_expr as matched_match_expr
  } else if (OB_ISNULL(matched_match_expr->get_oracle_phrase_like_expr())) {
    // no phrase LIKE recorded for this match expr
  } else if (OB_FAIL(build_oracle_phrase_score_case_when_expr_(expr_factory, session_info,
                                                               matched_match_expr->get_oracle_phrase_like_expr(),
                                                               matched_match_expr, out_expr))) {
    LOG_WARN("failed to build oracle phrase score case when", K(ret));
  }
  return ret;
}

int ObFtsOracleMatchExprUtil::build_oracle_phrase_score_case_when_expr_(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo *session_info,
    ObRawExpr *phrase_like_expr,
    ObMatchFunRawExpr *match_expr,
    ObRawExpr *&case_when_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *zero_expr = NULL;
  constexpr double zero_val = 0.0;
  ObRawExpr *phrase_like_case_when_expr = NULL;
  if (OB_ISNULL(phrase_like_expr) || OB_ISNULL(match_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phrase_like_expr), KP(match_expr));
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(expr_factory, ObDoubleType, zero_val, zero_expr))) {
    LOG_WARN("build const double for oracle phrase score failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                     expr_factory, phrase_like_expr, match_expr, zero_expr, phrase_like_case_when_expr))) {
    LOG_WARN("build case when for oracle phrase score failed", K(ret));
  } else if (OB_ISNULL(phrase_like_case_when_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null case when expr after build for oracle phrase score", K(ret));
  } else if (OB_FAIL(phrase_like_case_when_expr->formalize(session_info))) {
    LOG_WARN("formalize oracle phrase score case when failed", K(ret));
  } else {
    case_when_expr = phrase_like_case_when_expr;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
