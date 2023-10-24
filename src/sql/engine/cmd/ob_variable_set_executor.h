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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_VARIABLE_SET_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_VARIABLE_SET_EXECUTOR_
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "share/ob_define.h"
#include "sql/session/ob_session_val_map.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObExprCtx;
// namespace sqlclient
// {
class ObMySQLProxy;
// }
}
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
class ObPhysicalPlanCtx;
class ObVariableSetExecutor
{
public:
  ObVariableSetExecutor();
  virtual ~ObVariableSetExecutor();
  int execute(ObExecContext &ctx, ObVariableSetStmt &stmt);
  static int calc_var_value_static_engine(
          ObVariableSetStmt::VariableSetNode &node,
          ObVariableSetStmt &stmt,
          ObExecContext &exec_ctx,
          common::ObObj &value_obj);
  static int calc_subquery_expr_value(ObExecContext &ctx,
                                      ObSQLSessionInfo *session_info,
                                      ObRawExpr *expr,
                                      common::ObObj &value_obj);
  static int execute_subquery_expr(ObExecContext &ctx,
                                   ObSQLSessionInfo *session_info,
                                   const ObSqlString &subquery_expr,
                                   common::ObObj &value_obj);
  static int check_and_convert_sys_var(ObExecContext &ctx,
                                       const share::ObSetVar &set_var,
                                       share::ObBasicSysVar &sys_var,
                                       const common::ObObj &in_val,
                                       common::ObObj &out_val,
                                       bool is_set_stmt);
  static int set_user_variable(const common::ObObj &val,
                               const common::ObString &name,
                               const common::ObExprCtx &expr_ctx);
  static int set_user_variable(const common::ObObj &val,
                               const common::ObString &name,
                               ObSQLSessionInfo *session);
  static int cast_value(ObExecContext &ctx,
                        const ObVariableSetStmt::VariableSetNode &var_node,
                        uint64_t actual_tenant_id,
                        common::ObIAllocator &calc_buf,
                        const share::ObBasicSysVar &sys_val,
                        const common::ObObj &in_val,
                        common::ObObj &out_val);
  static int switch_to_session_variable(const common::ObExprCtx &expr_ctx,
                                        const common::ObObj &value,
                                        ObSessionVariable &sess_var);
  static int switch_to_session_variable(const common::ObObj &value,
                                        ObSessionVariable &sess_var);
private:
  struct ObValidatePasswordCtx {
    ObValidatePasswordCtx() :
      expect_length_(0),
      cur_length_(0),
      cur_mixed_case_count_(0),
      cur_number_count_(0),
      cur_special_count_(0)
    {}
    int init(uint64_t tenant_id);
    int get_current_val(share::schema::ObSchemaGetterGuard &schema_guard,
                        uint64_t tenant_id,
                        share::ObSysVarClassType var_id,
                        uint64_t &val);
    int update_expect_length();

    uint64_t expect_length_;
    uint64_t cur_length_;
    uint64_t cur_mixed_case_count_;
    uint64_t cur_number_count_;
    uint64_t cur_special_count_;
  };
  int process_session_autocommit_hook(ObExecContext &exec_ctx,
                                      const common::ObObj &val);
  int process_auto_increment_hook(const ObSQLMode sql_mode,
                                  const common::ObString var_name,
                                  common::ObObj &val);
  int process_last_insert_id_hook(ObPhysicalPlanCtx *plan_ctx,
                                  const ObSQLMode sql_mode,
                                  const common::ObString var_name,
                                  common::ObObj &val);
  int update_resource_mapping_rule_version(ObMySQLProxy &sql_proxy, uint64_t tenant_id);

  int update_global_variables(ObExecContext &ctx,
                              ObDDLStmt &stmt,
                              const share::ObSetVar &set_var,
                              const common::ObObj &value_obj);
  int global_variable_timezone_formalize(ObExecContext &ctx, ObObj &val);
  int process_validate_password_hook(ObValidatePasswordCtx &ctx,
                                     const common::ObString var_name,
                                     const common::ObObj &val);
  int cascade_set_validate_password(ObExecContext &ctx,
                                    ObVariableSetStmt &stmt,
                                    common::ObIAllocator &calc_buf,
                                    common::ObMySQLProxy &sql_proxy,
                                    const ObValidatePasswordCtx &password_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObVariableSetExecutor);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_CMD_OB_VARIABLE_SET_EXECUTOR_ */
