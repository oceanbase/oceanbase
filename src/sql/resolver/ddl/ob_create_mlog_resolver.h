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

#ifndef OB_CREATE_MLOG_RESOLVER_H_
#define OB_CREATE_MLOG_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_create_mlog_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObCreateMLogStmt;
class ObCreateMLogResolver : public ObDDLResolver
{
public:
  explicit ObCreateMLogResolver(ObResolverParams &params);
  virtual ~ObCreateMLogResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

protected:
  int fill_session_info_into_arg(const sql::ObSQLSessionInfo &session,
                                 ObCreateMLogStmt &create_mlog_stmt);
  int resolve_table_name_node(ParseNode *table_name_node,
                              ObCreateMLogStmt &create_mlog_stmt);
  int resolve_table_option_node(ParseNode *table_option_node,
                                ObCreateMLogStmt &create_mlog_stmt);
  int resolve_with_option_node(ParseNode *with_option_node,
                               ObCreateMLogStmt &create_mlog_stmt);
  int resolve_special_columns_node(ParseNode *special_columns_node,
                                   ObCreateMLogStmt &create_mlog_stmt);
  int resolve_special_column_node(ParseNode *special_column_node,
                                  ObCreateMLogStmt &create_mlog_stmt);
  int resolve_reference_columns_node(ParseNode *ref_columns_node,
                                  ObCreateMLogStmt &create_mlog_stmt);
  int resolve_reference_column_node(ParseNode *ref_column_node,
                                ObCreateMLogStmt &create_mlog_stmt);
  int resolve_new_values_node(ParseNode *new_values_node,
                              ObCreateMLogStmt &create_mlog_stmt);
  int resolve_purge_node(ParseNode *purge_node,
                         ObCreateMLogStmt &create_mlog_stmt);
  int resolve_purge_start_next_node(ParseNode *purge_start_node,
                                    ParseNode *purge_next_node,
                                    ObCreateMLogStmt &create_mlog_stmt);

private:
  bool is_column_exists(ObIArray<ObString> &column_name_array, const ObString &column_name);

private:
  enum ParameterEnum {
    ENUM_TABLE_NAME = 0,
    ENUM_OPT_TABLE_OPTIONS,
    ENUM_OPT_WITH,
    ENUM_OPT_NEW_VALUES,
    ENUM_OPT_PURGE,
    ENUM_TOTAL_COUNT
  };
  bool is_heap_table_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateMLogResolver);
};
} // sql
} // oceanbase

#endif // OB_CREATE_MLOG_RESOLVER_H_