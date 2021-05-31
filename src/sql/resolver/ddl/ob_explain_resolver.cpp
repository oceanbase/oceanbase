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
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
#define EXPLAIN_FORMAT 0
#define EXPLAIN_CHILD_STMT 1
int ObExplainResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt* explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_ && 2 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax", "type", parse_tree.type_, "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {

    ParseNode* child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt* child_stmt = NULL;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_ || T_DELETE == child_node->type_ || T_INSERT == child_node->type_ ||
                   T_MERGE == child_node->type_ || T_UPDATE == child_node->type_ || T_REPLACE == child_node->type_ ||
                   T_MULTI_INSERT == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *child_node, child_stmt))) {
        LOG_WARN("failed to resolve child stmt", K(ret));
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt*>(child_stmt));
        ParseNode* format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
            case T_OUTLINE: {
              explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            } break;
            case T_EXTENDED: {
              explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            } break;
            case T_PARTITIONS: {
              explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            } break;
            case T_TRADITIONAL: {
              explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            } break;
            case T_JSON: {
              explain_stmt->set_explain_format(EXPLAIN_JSON);
            } break;
            case T_BASIC: {
              explain_stmt->set_explain_format(EXPLAIN_BASIC);
              break;
            }
            case T_EXTENDED_NOADDR: {
              explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
              break;
            }
            case T_PLANREGRESS: {
              explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
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
  }

  return ret;
}
