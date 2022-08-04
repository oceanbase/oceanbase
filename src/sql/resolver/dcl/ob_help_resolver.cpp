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
#include "sql/resolver/dcl/ob_help_resolver.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "lib/charset/ob_charset.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_result.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

#define ROW_NUM(sql_result, row_num)                    \
  for (; OB_SUCC(sql_result->next());) {}               \
  row_num = sql_result->result_set().get_return_rows(); \
  ret = OB_SUCCESS;

ObHelpResolver::ObHelpResolver(ObResolverParams& params) : ObSelectResolver(params)
{}

ObHelpResolver::~ObHelpResolver()
{}

int ObHelpResolver::parse_and_resolve_select_sql(const ObString& select_sql)
{
  int ret = OB_SUCCESS;
  // 1. parse and resolve view defination
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member is not init", K(ret), K(session_info_), K(params_.allocator_));
  } else {
    ParseResult select_result;
    ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
    if (OB_FAIL(parser.parse(select_sql, select_result))) {
      LOG_WARN("parse select sql failed", K(select_sql), K(ret));
    } else {
      // use alias to make all columns number continued
      if (OB_ISNULL(select_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(
                     select_result.result_tree_->num_child_ != 1 || NULL == select_result.result_tree_->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid",
            K(ret),
            K(select_result.result_tree_->num_child_),
            K(select_result.result_tree_->children_));
      } else if (OB_UNLIKELY(NULL == select_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid", K(ret), "child ptr", select_result.result_tree_->children_[0]);
      } else {
        ParseNode* select_stmt_node = select_result.result_tree_->children_[0];
        if (OB_FAIL(ObSelectResolver::resolve(*select_stmt_node))) {
          LOG_WARN("resolve select in view definition failed", K(ret), K(select_stmt_node));
        }
      }
    }
  }
  return ret;
}

int ObHelpResolver::search_topic(const ParseNode& parse_tree, int64_t& topic_count, ObString& select_sql)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy* sql_proxy = GCTX.sql_proxy_;
  const char* mask = parse_tree.str_value_;
  const int len = parse_tree.str_len_;
  uint64_t real_tenant_id = session_info_->get_effective_tenant_id();
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    observer::ObInnerSQLResult* sql_result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT name from mysql.help_topic"
                               " where name like '%.*s'",
            len,
            mask))) {
      LOG_WARN("append sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy->read(res, real_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else {
      if (OB_ISNULL(sql_result = dynamic_cast<observer::ObInnerSQLResult*>(res.get_result()))) {
        LOG_WARN("result is null", K(ret));
      } else {
        if (OB_SUCC(sql_result->next())) {};
        ROW_NUM(sql_result, topic_count);
        if (topic_count == 0) {
          // do nothing
        } else if (topic_count == 1) {
          if (OB_FAIL(sql.assign_fmt("select name, description, example from mysql.help_topic"
                                     " WHERE name like '%s'",
                  mask))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        } else {
          if (OB_FAIL(sql.assign_fmt(" select name, \"N\" as is_it_category from mysql.help_topic"
                                     " where name like '%.*s'"
                                     " union all"
                                     " select name, \"Y\" as is_it_category from mysql.help_category"
                                     " where name like '%.*s' order by 2 asc , 1 asc",
                  len,
                  mask,
                  len,
                  mask))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        }
      }
    }
  }
  return ret;
}

int ObHelpResolver::search_keyword(const ParseNode& parse_tree, int64_t& topic_count, ObString& select_sql)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy* sql_proxy = GCTX.sql_proxy_;
  const char* mask = parse_tree.str_value_;
  const int len = parse_tree.str_len_;
  uint64_t real_tenant_id = session_info_->get_effective_tenant_id();
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    observer::ObInnerSQLResult* sql_result = NULL;
    if (OB_FAIL(sql.assign_fmt("select t3.name from mysql.help_keyword t1 "
                               " JOIN mysql.help_relation t2 on (t1.help_keyword_id = t2.help_keyword_id) "
                               " JOIN mysql.help_topic t3 on (t2.help_topic_id = t3.help_topic_id) "
                               " WHERE t1.name like '%.*s'",
            len,
            mask))) {
      LOG_WARN("append sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy->read(res, real_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else {
      if (OB_ISNULL(sql_result = dynamic_cast<observer::ObInnerSQLResult*>(res.get_result()))) {
        LOG_WARN("result is null", K(ret));
      } else {
        if (OB_SUCC(sql_result->next())) {};
        ROW_NUM(sql_result, topic_count);
        if (topic_count == 0) {
          // do nothing
        } else if (topic_count == 1) {
          if (OB_FAIL(sql.assign_fmt("select t3.name, t3.description, t3.example from mysql.help_keyword t1 "
                                     " JOIN mysql.help_relation t2 on (t1.help_keyword_id = t2.help_keyword_id) "
                                     " JOIN mysql.help_topic t3 on (t2.help_topic_id = t3.help_topic_id) "
                                     " WHERE t1.name like '%.*s' ",
                  len,
                  mask))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        } else {
          if (OB_FAIL(sql.assign_fmt("select t3.name as name, \"N\" as is_it_category  from mysql.help_keyword t1"
                                     " JOIN mysql.help_relation t2 on (t1.help_keyword_id = t2.help_keyword_id)"
                                     " JOIN mysql.help_topic t3 on (t2.help_topic_id = t3.help_topic_id)"
                                     " WHERE t1.name like '%.*s'"
                                     " union all"
                                     " select name, \"Y\" as is_it_category from mysql.help_category"
                                     " where name like '%.*s' order by 2 asc, 1 asc",
                  len,
                  mask,
                  len,
                  mask))) {
            LOG_WARN("fail to generate select string", K(ret));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        }
      }
    }
  }
  return ret;
}

int ObHelpResolver::search_category(const ParseNode& parse_tree, int64_t& category_count, ObString& select_sql)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy* sql_proxy = GCTX.sql_proxy_;
  const char* mask = parse_tree.str_value_;
  const int len = parse_tree.str_len_;
  ObString help_category_name;
  uint64_t real_tenant_id = session_info_->get_effective_tenant_id();
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    observer::ObInnerSQLResult* sql_result = NULL;
    if (OB_FAIL(sql.assign_fmt(
            "select help_category_id, name from mysql.help_category where name like '%.*s'", len, mask))) {
      LOG_WARN("append sql failed", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy->read(res, real_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else {
      if (OB_ISNULL(sql_result = dynamic_cast<observer::ObInnerSQLResult*>(res.get_result()))) {
        LOG_WARN("result is null", K(ret));
      } else {
        if (OB_SUCC(sql_result->next())) {
          sql_result->get_nchar("name", help_category_name);
        }
        ROW_NUM(sql_result, category_count);
        if (category_count == 0) {
          if (OB_FAIL(sql.assign_fmt(
                  "select name, \"N\" as is_it_category from mysql.help_topic where help_category_id = \"-1\""))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        } else if (category_count == 1) {
          if (OB_FAIL(
                  sql.assign_fmt(" select \"%.*s\" as source_category_name, t1.name as name, \"N\" as is_it_category "
                                 " from mysql.help_topic t1 join mysql.help_category t2"
                                 " on (t1.help_category_id = t2.help_category_id)"
                                 " where t2.name like '%.*s'"
                                 " union all"
                                 " select \"%.*s\" as source_category_name, t2.name as name, \"Y\" as is_it_category"
                                 " from mysql.help_category t1 join mysql.help_category t2"
                                 " on (t1.help_category_id = t2.parent_category_id)"
                                 " where t1.name like '%.*s'  order by 3 asc , 2 asc",
                      help_category_name.length(),
                      help_category_name.ptr(),
                      len,
                      mask,
                      help_category_name.length(),
                      help_category_name.ptr(),
                      len,
                      mask))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else if (OB_FAIL(sql_proxy->read(res, real_tenant_id, sql.ptr()))) {
            LOG_WARN("execute sql failed", K(ret), K(sql));
          } else {
            if (OB_ISNULL(sql_result = dynamic_cast<observer::ObInnerSQLResult*>(res.get_result()))) {
              LOG_WARN("result is null", K(ret));
            } else {
              ObString is_it_category;
              uint64_t row_count;
              if (OB_SUCC(sql_result->next())) {
                sql_result->get_nchar("is_it_category", is_it_category);
              }
              ROW_NUM(sql_result, row_count);
              if (row_count == 1) {
                if (is_it_category.compare("N") == 0) {
                  if (OB_FAIL(sql.assign_fmt("select t1.name, t1.description, t1.example "
                                             " from mysql.help_topic t1 join mysql.help_category t2"
                                             " on (t1.help_category_id = t2.help_category_id)"
                                             " where t2.name like '%.*s' ",
                          len,
                          mask))) { /* output the only topic */
                    LOG_WARN("append sql failed", K(ret), K(sql));
                  }
                } else {
                  if (OB_FAIL(sql.assign_fmt(" select t2.name as name, \"Y\" as is_it_category"
                                             " from mysql.help_category t1 join mysql.help_category t2"
                                             " on (t1.help_category_id = t2.parent_category_id)"
                                             " where t1.name like '%.*s' ",
                          len,
                          mask))) {
                    LOG_WARN("append sql failed", K(ret), K(sql));
                  }
                }
              }
            }
            if(OB_SUCC(ret)) select_sql.assign_ptr(sql.ptr(), sql.length());
          }
        } else {
          if (OB_FAIL(sql.assign_fmt(" select name, \"Y\" as is_it_category"
                                     " from mysql.help_category where name like '%.*s' order by name asc",
                  len,
                  mask))) {
            LOG_WARN("append sql failed", K(ret), K(sql));
          } else {
            select_sql.assign_ptr(sql.ptr(), sql.length()); 
          }
        }
      }
    }
  }

  return ret;
}

int ObHelpResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  /* transform into select_sql */
  ObString select_sql;
  const char* mask = parse_tree.str_value_;
  const int len = parse_tree.str_len_;
  if (parse_tree.type_ == T_HELP) {
    /* Inner Sql */
    int64_t topic_count, category_count;
    if (OB_FAIL(search_topic(parse_tree, topic_count, select_sql))) {
      LOG_WARN("search_topic error", K(ret));
    } else if (topic_count == 0 && OB_FAIL(search_keyword(parse_tree, topic_count, select_sql))) {
      LOG_WARN("search_keyword error", K(ret));
    } else if (topic_count == 0 && OB_FAIL(search_category(parse_tree, category_count, select_sql))) {
      LOG_WARN("search_category error", K(ret));
    }
  } else {
    LOG_WARN("ObHelpResoler err");
    ret = OB_ERR_UNEXPECTED;
  }
  if(OB_SUCC(ret) && OB_FAIL(parse_and_resolve_select_sql(select_sql))) {
    LOG_WARN("fail to parse and resolve select sql", K(ret), K(select_sql));
  }

  if (OB_LIKELY(OB_SUCCESS == ret && NULL != stmt_)) {
    if (OB_UNLIKELY(stmt::T_SELECT != stmt_->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(stmt_->get_stmt_type()));
    } else {
      ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt_);
      select_stmt->set_select_type(NOT_AFFECT_FOUND_ROWS);
      select_stmt->set_select_type(NOT_AFFECT_FOUND_ROWS);
      if (OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
        LOG_WARN("pull select stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase