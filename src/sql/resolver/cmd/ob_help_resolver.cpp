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
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "ob_help_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/cmd/ob_help_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace share;

int ObHelpResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  int topic_count = 0;
  int category_count = 0;
  int32_t size = 0;
  ObString like_pattern;
  ObHelpStmt *help_stmt = NULL;
  ObSqlString sql;
  ObString category_name;
  int count = 0;
  ObString name;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session info is not init", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_
                         || NULL == parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "parser tree is wrong", K(ret));
  } else {
    size = static_cast<int32_t>(parse_tree.children_[0]->str_len_);
    like_pattern.assign(const_cast<char *>(parse_tree.children_[0]->str_value_), size);
    if (NULL == (help_stmt = create_stmt<ObHelpStmt>())) {
      ret = OB_SQL_RESOLVER_NO_MEMORY;
      SQL_RESV_LOG(WARN, "failed to create help stmt");
    } else {
      stmt_ = help_stmt;
      if (OB_FAIL(create_topic_sql(like_pattern, sql))) {//search for topic
        SQL_RESV_LOG(WARN, "failed to create topic sql", K(sql), K(like_pattern));
      } else if (OB_FAIL(search_topic(help_stmt, sql, topic_count))) {
        SQL_RESV_LOG(WARN,"search topic failed",K(like_pattern), K(topic_count));
      } else {/* do nothing */}
      if (OB_SUCCESS == ret && 0 == topic_count) {
        if (OB_FAIL(create_keyword_sql(like_pattern, sql))) {//search for keyword
          SQL_RESV_LOG(WARN, "failed to keyword create sql", K(sql), K(like_pattern));
        } else if (OB_FAIL(search_topic(help_stmt, sql, topic_count))) {
          SQL_RESV_LOG(WARN,"search keyword failed",K(like_pattern), K(topic_count));
        } else {/* do nothing */}
      }
      if (OB_SUCCESS == ret &&  0 == topic_count) {
        if (OB_FAIL(create_category_sql(like_pattern, sql))) {//search for category
          SQL_RESV_LOG(WARN, "failed to category create sql", K(sql), K(like_pattern));
        } else if (OB_FAIL(search_category(help_stmt, sql, category_count, category_name))) {
          SQL_RESV_LOG(WARN,"search category failed",K(like_pattern), K(category_count));
        } else {
          if (1 == category_count) {//if category count is 1, then search topic and category whose parent category is category_name;
            if (OB_FAIL(help_stmt->clear_row_store())) {
              SQL_RESV_LOG(WARN, "fail to clear row store", K(ret));
            } else if (OB_FAIL(create_category_topic_sql(like_pattern, sql))) {//ceate sql
              SQL_RESV_LOG(WARN,"create category topic sql failed", K(sql), K(like_pattern));
            } else if (OB_FAIL(search_topic(help_stmt, sql, count, category_name))) {//seach topic whose parent category is category_name
              SQL_RESV_LOG(WARN,"search keyword failed",K(like_pattern), K(count));
            } else if (OB_FAIL(create_category_child_sql(like_pattern, sql))) {//create sql
              SQL_RESV_LOG(WARN,"create category child sql failed", K(sql), K(like_pattern));
            } else if (OB_FAIL(search_category(help_stmt, sql, count, name, category_name))) {//seach category  whose parent category is category_name
              SQL_RESV_LOG(WARN,"search keyword failed",K(like_pattern), K(count));
            } else {/* do nothing */}
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (1 == topic_count) {//seach topic only
          if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("name")))) {
            SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
          } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("description")))) {
            SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
          } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("example")))) {
            SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
          } else {/*do nothing*/}
        } else if (0 == topic_count) {
          if (category_count > 1) {
            if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("name")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
            } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("is_it_category")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
            } else {/*do nothing*/}
          } else {
            if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("source_category_name")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
            } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("name")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
            } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("is_it_category")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
            } else {/*do nothing*/}
            if (OB_SUCC(ret) && 0 == category_count){
              if (OB_FAIL(help_stmt->set_col_count(3))) {
                SQL_RESV_LOG(WARN, "failed to set help stmt rowstor column count");
              }
            }
          }
        } else {//topic_count > 1
          if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("name")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
          } else if (OB_FAIL(help_stmt->add_col_name(ObString::make_string("is_it_category")))) {
              SQL_RESV_LOG(WARN, "fail to add column name", K(ret));
          } else if (OB_FAIL(create_category_sql(like_pattern, sql))) {
            SQL_RESV_LOG(WARN, "failed to category create sql", K(sql), K(like_pattern));
          } else if (OB_FAIL(search_category(help_stmt, sql, category_count, name))) {
            SQL_RESV_LOG(WARN,"search category failed",K(like_pattern), K(category_count));
          } else {/* do nothing */}
        }
      }
    }
  }
  return ret;
}

int ObHelpResolver::search_topic(ObHelpStmt *help_stmt,
                                 const ObSqlString &sql,
                                 int &topic_count,
                                 const ObString &source_category)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    int row_count = 0;
    ObNewRow row;
    ObObj cells[3];
    ObString col1;
    ObString col2;
    ObString col3;
    int64_t col_count = 0;
  
    if (OB_UNLIKELY(NULL == sql_proxy_ || NULL == help_stmt)) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "parameter or class member is not init", K(ret), K(sql_proxy_), K(help_stmt));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      SQL_RESV_LOG(WARN, "execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "failed to get result", K(sql));
    } else {
      ObString last_name;
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
        if ( 0 != source_category.compare("None")) {//search topic for the category
          col_count = 3;
          if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "name", col2))) {
            SQL_REWRITE_LOG(WARN, "fail to get name column", K(ret), K(col2));
          } else {
            cells[0].set_varchar(source_category);
            cells[1].set_varchar(col2);
            cells[2].set_varchar(ObString::make_string("N"));
          }
        } else {
          if (0 == row_count) {
            col_count = 3;
            if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "name", col1))) {
              SQL_REWRITE_LOG(WARN, "fail to get name column", K(ret), K(col1));
            } else if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "description", col2))) {
              SQL_REWRITE_LOG(WARN, "fail to get description column", K(ret), K(col2));
            } else if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "example", col3))) {
              SQL_REWRITE_LOG(WARN, "fail to get example column", K(ret), K(col3));
            } else {
              cells[0].set_varchar(col1);
              cells[1].set_varchar(col2);
              cells[2].set_varchar(col3);
              last_name = col1;
            }
          } else {//if more than 1 topic is found, columns are name|is_it_category
            col_count = 2;
            if (1 == row_count && 0 == source_category.compare("None")) {//clear rowstore and add the first row again according to the columns
              if (OB_FAIL(help_stmt->clear_row_store())) {
                SQL_RESV_LOG(WARN, "fail to clear row store", K(ret));
              } else {
                cells[0].set_varchar(last_name);
                cells[1].set_varchar(ObString::make_string("N"));
                row.cells_ = cells;
                row.count_ = col_count;
                help_stmt->add_row(row);
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "name", col1))) {
              SQL_REWRITE_LOG(WARN, "fail to get name column", K(ret), K(col1));
            } else {
              cells[0].set_varchar(col1);
              cells[1].set_varchar(ObString::make_string("N"));
            }
          }
        }
        row.cells_ = cells;
        row.count_ = col_count;
        help_stmt->add_row(row);
        row_count++;
      }
      if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
        SQL_RESV_LOG(WARN,"get result failed");
      } else {
        topic_count = row_count;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObHelpResolver::search_category(ObHelpStmt *help_stmt,
                                    const ObSqlString &sql,
                                    int &category_count,
                                    ObString &category_name,
                                    const ObString &source_category)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    int row_count = 0;
    int64_t col_count = 0;
    ObObj cells[3];
    ObString name;
    ObNewRow row;
    char *str_clone = NULL;
    if (OB_UNLIKELY(NULL == sql_proxy_
                    || NULL == help_stmt
                    || NULL == allocator_)) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN,
                   "parameter or class member is not init", K(ret), K(sql_proxy_), K(help_stmt), K(allocator_));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      SQL_RESV_LOG(WARN, "execute sql failed", K(ret), K(sql));
    } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "failed to get result", K(ret), K(sql));
    } else {
      if (0 != source_category.compare("None")) {
        col_count = 3;
      } else {
        col_count = 2;
      }
      if (OB_FAIL(help_stmt->set_col_count(col_count))) {
        SQL_RESV_LOG(WARN, "failed to set help stmt rowstor column count");
      } else {
        while (OB_SUCCESS == ret && OB_SUCCESS == (ret = result->next())) {
          if (OB_FAIL(GET_COL_IGNORE_NULL(result->get_varchar, "name", name))) {
            SQL_REWRITE_LOG(WARN, "fail to get name column", K(ret), K(name));
          } else if (OB_UNLIKELY(NULL == name.ptr()
                                 || name.length() <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_REWRITE_LOG(WARN, "name from result is wrong", K(ret), K(name));
          } else {
            if (0 != source_category.compare("None")) {//search child category for the category
              cells[0].set_varchar(source_category);
              cells[1].set_varchar(name);
              cells[2].set_varchar(ObString::make_string("Y"));
            } else {
              cells[0].set_varchar(name);
              cells[1].set_varchar(ObString::make_string("Y"));
            }
            row.cells_ = cells;
            row.count_ = col_count;
            help_stmt->add_row(row);
            row_count++;
            if (OB_UNLIKELY(NULL == (str_clone = static_cast<char *>(allocator_->alloc(name.length()))))) {
              ret = OB_SQL_RESOLVER_NO_MEMORY;
              SQL_RESV_LOG(WARN, "allocate memory failed", K(ret));
            } else {
              MEMCPY(str_clone, name.ptr(), name.length());
              category_name.assign_ptr(str_clone, name.length());
            }
          }
        }
        if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
          SQL_RESV_LOG(WARN,"get result failed");
        } else {
          category_count = row_count;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObHelpResolver::create_keyword_sql(const ObString &like_pattern, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT t3.name, t3.description, t3.example "
                             "FROM mysql.%s t1 JOIN mysql.%s t2  on (t1.help_keyword_id = t2.help_keyword_id) "
                             "JOIN mysql.%s t3 on (t2.help_topic_id = t3.help_topic_id) "
                             "WHERE t1.name like '%s' ",
                             OB_HELP_KEYWORD_TNAME,
                             OB_HELP_RELATION_TNAME,
                             OB_HELP_TOPIC_TNAME,
                             like_pattern.ptr()))) {
    SQL_RESV_LOG(WARN, "assign sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObHelpResolver::create_topic_sql(const ObString &like_pattern, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT name, description, example "
                             "FROM mysql.%s WHERE name like '%s' ",
                             OB_HELP_TOPIC_TNAME,
                             like_pattern.ptr()))) {
    SQL_RESV_LOG(WARN, "assign sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObHelpResolver::create_category_sql(const ObString &like_pattern, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT name "
                             "FROM mysql.%s WHERE name like '%s' ",
                             OB_HELP_CATEGORY_TNAME,
                             like_pattern.ptr()))) {
    SQL_RESV_LOG(WARN, "assign sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObHelpResolver::create_category_topic_sql(const ObString &like_pattern, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT t1.name "
                             "FROM mysql.%s t1 JOIN mysql.%s t2 on (t1.help_category_id = t2.help_category_id ) "
                             "WHERE t2.name like '%s' ",
                             OB_HELP_TOPIC_TNAME,
                             OB_HELP_CATEGORY_TNAME,
                             like_pattern.ptr()))) {
    SQL_RESV_LOG(WARN, "assign sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObHelpResolver::create_category_child_sql(const ObString &like_pattern, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("SELECT t2.name "
                             "FROM mysql.%s t1 JOIN mysql.%s t2 on (t1.help_category_id = t2.parent_category_id ) "
                             "WHERE t1.name like '%s' ",
                             OB_HELP_CATEGORY_TNAME,
                             OB_HELP_CATEGORY_TNAME,
                             like_pattern.ptr()))) {
    SQL_RESV_LOG(WARN, "assign sql failed", K(ret), K(sql));
  }
  return ret;
}
