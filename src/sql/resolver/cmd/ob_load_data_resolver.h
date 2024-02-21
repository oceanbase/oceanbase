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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_LOAD_DATA_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_CMD_OB_LOAD_DATA_RESOLVER_H
#include "lib/ob_define.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObLoadDataStmt;
struct ObDataInFileStruct;

class ObLoadDataResolver : public ObCMDResolver
{
public:
  explicit ObLoadDataResolver(ObResolverParams &params):
        ObCMDResolver(params), current_scope_(T_LOAD_DATA_SCOPE)
  {
  }
  virtual ~ObLoadDataResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
  int resolve_field_node(const ParseNode &node, const common::ObNameCaseMode case_mode,
                         ObLoadDataStmt &load_stmt);
  int resolve_user_vars_node(const ParseNode &node, ObLoadDataStmt &load_stmt);
  int resolve_field_or_var_list_node(const ParseNode &node, const common::ObNameCaseMode case_mode,
                                     ObLoadDataStmt &load_stmt);
  int resolve_empty_field_or_var_list_node(ObLoadDataStmt &load_stmt);
  int resolve_set_clause(const ParseNode &node, const common::ObNameCaseMode case_mode,
                                  ObLoadDataStmt &load_stmt);
  int resolve_each_set_node(const ParseNode &node, const common::ObNameCaseMode case_mode,
                             ObLoadDataStmt &load_stmt);
  int resolve_char_node(const ParseNode &node, int32_t &single_char);
  int resolve_string_node(const ParseNode &node, common::ObString &target_str);
  int resolve_field_list_node(const ParseNode &node, ObDataInFileStruct &data_struct_in_file);
  int resolve_line_list_node(const ParseNode &node, ObDataInFileStruct &data_struct_in_file);
  int resolve_sys_vars(common::ObIArray<ObVarInfo> &sys_vars);
  int resolve_default_func(ObRawExpr *&expr);
  int resolve_default_expr(ObRawExpr *&expr, ObLoadDataStmt &load_stmt, uint64_t column_id);
  int resolve_subquery_info(common::ObIArray<ObSubQueryInfo> &subquery_info);
  int build_column_ref_expr(ObQualifiedName &q_name, ObRawExpr *&col_expr);
  int resolve_column_ref_expr(common::ObIArray<ObQualifiedName> &columns, ObRawExpr *&expr);
  int check_if_table_exists(uint64_t tenant_id, const common::ObString &db_name,
                            const common::ObString &table_name, bool cte_table_fisrt, uint64_t& table_id);
  int validate_stmt(ObLoadDataStmt* stmt);
  int resolve_hints(const ParseNode &node);

  int resolve_filename(ObLoadDataStmt *load_stmt, ParseNode *node);
  int local_infile_enabled(bool &enabled) const;

  int check_trigger_constraint(const ObTableSchema *table_schema);
private:
  int pattern_match(const ObString& str, const ObString& pattern, bool &matched);
  bool exist_wildcard(const ObString& str);
private:
  enum ParameterEnum {
    ENUM_OPT_LOCAL = 0,
    ENUM_FILE_NAME,
    ENUM_DUPLICATE_ACTION,
    ENUM_TABLE_NAME,
    ENUM_OPT_CHARSET,
    ENUM_OPT_FIELD,
    ENUM_OPT_LINE,
    ENUM_OPT_IGNORE_ROWS,
    ENUM_OPT_FIELD_OR_VAR,
    ENUM_OPT_SET_FIELD,
    ENUM_OPT_HINT,
    ENUM_OPT_EXTENDED_OPTIONS,
    ENUM_TOTAL_COUNT
  };
  ObStmtScope current_scope_;
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataResolver);
};
}//sql
}//oceanbase
#endif
