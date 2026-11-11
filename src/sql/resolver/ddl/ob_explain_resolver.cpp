/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

const int64_t ObExplainResolver::EXPLAIN_FORMAT = 0;
const int64_t ObExplainResolver::EXPLAIN_DISPLAY_OPTION = 1;
const int64_t ObExplainResolver::EXPLAIN_INTO_TABLE = 2;
const int64_t ObExplainResolver::EXPLAIN_STMT_ID = 3;
const int64_t ObExplainResolver::EXPLAIN_CHILD_STMT = 4;
const int64_t ObExplainResolver::EXPLAIN_CHILD_COUNT = 5;

const ParseNode *ObExplainResolver::get_child_stmt_node(const ParseNode *explain_node)
{
  const ParseNode *child_node = NULL;
  if (OB_NOT_NULL(explain_node)
      && T_EXPLAIN == explain_node->type_
      && explain_node->num_child_ >= EXPLAIN_CHILD_COUNT) {
    child_node = explain_node->children_[EXPLAIN_CHILD_STMT];
  }
  return child_node;
}

bool ObExplainResolver::is_supported_ddl_item_type(const ObItemType item_type)
{
  return T_ALTER_TABLE == item_type;
}

bool ObExplainResolver::is_ddl_explain(const ParseNode *explain_node)
{
  bool ret = false;
  const ParseNode *child_node = get_child_stmt_node(explain_node);
  if (OB_NOT_NULL(child_node)) {
    ret = ObStmt::is_ddl_stmt(ObResolverUtils::get_stmt_type_by_item_type(child_node->type_), true);
  }
  return ret;
}

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_ && ObExplainResolver::EXPLAIN_CHILD_COUNT == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = const_cast<ParseNode *>(ObExplainResolver::get_child_stmt_node(&parse_tree));
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (ObExplainResolver::is_supported_ddl_item_type(child_node->type_)) {
      ObResolverParams ddl_params;
      ObResolver ddl_resolver(ddl_params);
      if (OB_FAIL(ddl_params.assign(params_))) {
        LOG_WARN("fail to assign params for DDL explain", K(ret));
      } else if (OB_FAIL(ddl_resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                              *child_node,
                                              child_stmt))) {
        LOG_WARN("failed to resolve DDL child stmt for EXPLAIN", K(ret));
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolved DDL stmt is null", K(ret));
      } else {
        ObAlterTableStmt *alter_stmt = static_cast<ObAlterTableStmt *>(child_stmt);
        alter_stmt->set_is_explain(true);
        explain_stmt->set_explain_ddl_stmt(child_stmt);
        explain_stmt->set_display_opt(opt);
      }
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_MERGE == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_
                 || T_MULTI_INSERT == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolverParams params;
      ObResolver resolver(params);
      if (OB_FAIL(params.assign(params_))) {
        LOG_WARN("fail to assign params", K(ret));
      } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                          *child_node,
                                          child_stmt))) {
        LOG_WARN("failed to resolve child stmt", K(ret));
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_OBJECT_NAME_DISPLAY:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_OBJECT_NAME_DISPLAY);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !explain_stmt->is_ddl_explain()) {
      ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
      if (OB_SUCC(ret) && NULL != opt_node) {
        switch (opt_node->type_) {
        case T_PRETTY_COLOR:
          opt.with_color_ = true;
          // pass
        case T_PRETTY:
          opt.with_tree_line_ = true;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected display option", K(ret), K(opt_node->type_));
        }
      }
      if (OB_SUCC(ret)) {
        explain_stmt->set_display_opt(opt);
      }
      ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
      if (OB_SUCC(ret) && NULL != into_table_node) {
        ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
        explain_stmt->set_into_table(into_table);
      }
      ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
      if (OB_SUCC(ret) && NULL != statement_id_node) {
        ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
        explain_stmt->set_statement_id(statement_id);
      }
    }
  }

  return ret;
}
