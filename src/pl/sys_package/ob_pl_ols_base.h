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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_SA_BASE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_SA_BASE_H_
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{

namespace sql
{
class ObAlterTableStmt;
}

namespace pl
{

constexpr sql::ObValueChecker<int64_t>                 FLAG_CHECKER                  (0, 1, OB_ERR_ARGUMENT_OUT_OF_RANGE);
constexpr sql::ObValueChecker<int64_t>                 COMP_NUM_CHECKER              (0, 9999, OB_ERR_ARGUMENT_OUT_OF_RANGE);
constexpr sql::ObValueChecker<int64_t>                 LABEL_TAG_CHECKER             (0, 99999999, OB_ERR_ARGUMENT_OUT_OF_RANGE);

constexpr sql::ObValueChecker<ObString::obstr_size_t>  POLICY_NAME_CHECKER           (1, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  COLUMN_NAME_CHECKER           (1, OB_MAX_COLUMN_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  COMP_SHORT_NAME_CHECKER       (1, MAX_ORACLE_SA_COMPONENTS_SHORT_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  COMP_LONG_NAME_CHECKER        (1, MAX_ORACLE_SA_COMPONENTS_LONG_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  LABEL_TEXT_CHECKER            (1, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  ENFORCEMENT_OPTIONS_CHECKER   (0, OB_MAX_COLUMN_NAME_LENGTH, OB_INVALID_ARGUMENT);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  TABLE_NAME_CHECKER            (1, OB_MAX_TABLE_NAME_LENGTH, OB_WRONG_TABLE_NAME);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  LABEL_FUNCTION_CHECKER        (0, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_WRONG_TABLE_NAME);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  PREDICATE_CHECKER             (0, OB_MAX_ORACLE_VARCHAR_LENGTH, OB_WRONG_TABLE_NAME);
constexpr sql::ObValueChecker<ObString::obstr_size_t>  USER_NAME_CHECKER             (0, OB_MAX_USER_NAME_LENGTH, OB_WRONG_USER_NAME_LENGTH);

constexpr sql::ObPointerChecker<ObLabelSePolicySchema>      POLICY_SCHEMA_CHECKER         (OB_ERR_POLICY_STRING_NOT_FOUND);
constexpr sql::ObPointerChecker<ObLabelSeComponentSchema>   COMPONENT_SCHEMA_CHECKER      (OB_ERR_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING);
constexpr sql::ObPointerChecker<ObLabelSeLabelSchema>       LABEL_SCHEMA_CHECKER          (OB_ERR_INVALID_LABEL_STRING);
static const int64_t INVALID_OLS_VALUE = -1;
class ObOLSBase {
public:
  static int init_phy_plan_timeout(sql::ObExecContext &exec_ctx, sql::ObSQLSessionInfo &session);
  static int get_schema_guard(sql::ObExecContext &exec_ctx, share::schema::ObSchemaGetterGuard *&schema_guard);
  static int append_str_to_sqlstring(common::ObSqlString &target, const common::ObString &param);
  static int append_int_to_sqlstring(common::ObSqlString &target, const int64_t param);
  static int gen_stmt_string(common::ObSqlString &ddl_stmt_str, const common::ObString &function_name, const common::ObString &args);
  static int get_interger_from_obj_and_check(const common::ObObj &param,
                                      int64_t &comp_num,
                                      const sql::ObValueChecker<int64_t> &checker,
                                      common::ObSqlString &param_str,
                                      bool accept_null = false);
  static int get_string_from_obj_and_check(common::ObIAllocator *allocator,
                                    const common::ObObj &param,
                                    common::ObString &name,
                                    const sql::ObValueChecker<ObString::obstr_size_t> &checker,
                                    common::ObSqlString &param_str,
                                    bool accept_null = false);
  static void set_ols_func_common_result_type(sql::ObExprResType &type);
  static void set_ols_func_common_result(common::ObObj &result);

  static int send_policy_ddl_rpc(sql::ObExecContext &exec_ctx, const obrpc::ObLabelSePolicyDDLArg &ddl_arg);
  static int send_component_ddl_rpc(sql::ObExecContext &exec_ctx, const obrpc::ObLabelSeComponentDDLArg &ddl_arg);
  static int send_label_ddl_rpc(sql::ObExecContext &exec_ctx, const obrpc::ObLabelSeLabelDDLArg &ddl_arg);
  static int send_user_level_ddl_rpc(sql::ObExecContext &exec_ctx, const obrpc::ObLabelSeUserLevelDDLArg &ddl_arg);
  static int check_func_access_role(sql::ObSQLSessionInfo &session);
  static int set_session_schema_update_flag(sql::ObExecContext &exec_ctx);
  static bool need_retry_ddl(sql::ObExecContext &ctx, int &ret);
};

class ObOLSUtil {
public:
  static int adjust_column_flag(sql::ObAlterTableStmt &alter_table_stmt, bool is_add);
  static int restore_invisible_column_flag(share::schema::ObSchemaGetterGuard &schema_guard,
                                           const uint64_t tenant_id,
                                           sql::ObAlterTableStmt &alter_table_stmt);
  static int generate_alter_table_args(sql::ObExecContext &exec_ctx,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       sql::ObSQLSessionInfo &session,
                                       const common::ObString &ddl_stmt_str,
                                       sql::ObAlterTableStmt *&alter_table_stmt);
  static int exec_switch_policy_column(const common::ObString &schema_name,
                                       const common::ObString &table_name,
                                       const common::ObString &column_name,
                                       sql::ObExecContext &exec_ctx,
                                       sql::ObSQLSessionInfo &session,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       bool is_switch_on);
  static int exec_drop_policy_column(const common::ObString &schema_name,
                                     const common::ObString &table_name,
                                     const common::ObString &column_name,
                                     sql::ObExecContext &exec_ctx,
                                     sql::ObSQLSessionInfo &session);
  static int exec_add_policy_column(const common::ObString &ddl_stmt_str,
                                    const common::ObString &schema_name,
                                    const common::ObString &table_name,
                                    const common::ObString &column_name,
                                    sql::ObExecContext &exec_ctx,
                                    sql::ObSQLSessionInfo &session,
                                    share::schema::ObSchemaGetterGuard &schema_guard);
  static int label_tag_compare(int64_t tenant_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const int64_t label_tag1,
                               const int64_t label_tag2,
                               int64_t &cmp_result);

};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_SA_BASE_H_ */
