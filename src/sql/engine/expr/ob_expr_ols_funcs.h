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
#ifndef _OB_EXPR_OLS_FUNCS_H
#define _OB_EXPR_OLS_FUNCS_H


#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/ob_sql_utils.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace sql
{
class ObAlterTableStmt;

class ObExprOLSBase {
public:
  static int init_phy_plan_timeout(ObExecContext &exec_ctx, ObSQLSessionInfo &session);
  static int get_schema_guard(ObExecContext *exec_ctx, share::schema::ObSchemaGetterGuard *&schema_guard);
  static int append_str_to_sqlstring(common::ObSqlString &target, const common::ObString &param);
  int append_int_to_sqlstring(common::ObSqlString &target, const int64_t param) const;
  int gen_stmt_string(common::ObSqlString &ddl_stmt_str, const common::ObString &function_name, const common::ObString &args) const;
  int get_interger_from_obj_and_check(const common::ObObj &param,
                                      int64_t &comp_num,
                                      const ObValueChecker<int64_t> &checker,
                                      common::ObSqlString &param_str,
                                      bool accept_null = false) const;
  static int get_string_from_obj_and_check(common::ObIAllocator *allocator,
                                    const common::ObObj &param,
                                    common::ObString &name,
                                    const ObValueChecker<ObString::obstr_size_t> &checker,
                                    common::ObSqlString &param_str,
                                    bool accept_null = false);
  void set_ols_func_common_result_type(ObExprResType &type) const;
  void set_ols_func_common_result(common::ObObj &result) const;

  int send_policy_ddl_rpc(common::ObExprCtx &expr_ctx, const obrpc::ObLabelSePolicyDDLArg &ddl_arg) const;
  int send_component_ddl_rpc(common::ObExprCtx &expr_ctx, const obrpc::ObLabelSeComponentDDLArg &ddl_arg) const;
  int send_label_ddl_rpc(common::ObExprCtx &expr_ctx, const obrpc::ObLabelSeLabelDDLArg &ddl_arg) const;
  int send_user_level_ddl_rpc(common::ObExprCtx &expr_ctx, const obrpc::ObLabelSeUserLevelDDLArg &ddl_arg) const;
  int check_func_access_role(ObSQLSessionInfo &session) const;
  int set_session_schema_update_flag(common::ObExprCtx &expr_ctx) const;
  bool need_retry_ddl(ObExecContext &ctx, int &ret) const;
};

class ObExprOLSUtil {
public:
  static int adjust_column_flag(ObAlterTableStmt &alter_table_stmt, bool is_add);
  static int restore_invisible_column_flag(share::schema::ObSchemaGetterGuard &schema_guard,
                                           const uint64_t tenant_id,
                                           ObAlterTableStmt &alter_table_stmt);
  static int generate_alter_table_args(common::ObExprCtx &expr_ctx,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       ObSQLSessionInfo &session,
                                       const common::ObString &ddl_stmt_str,
                                       ObAlterTableStmt *&alter_table_stmt);
  static int exec_switch_policy_column(const common::ObString &schema_name,
                                       const common::ObString &table_name,
                                       const common::ObString &column_name,
                                       common::ObExprCtx &expr_ctx,
                                       ObExecContext &exec_ctx,
                                       ObSQLSessionInfo &session,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       bool is_switch_on);
  static int exec_drop_policy_column(const common::ObString &schema_name,
                                     const common::ObString &table_name,
                                     const common::ObString &column_name,
                                     ObExecContext &exec_ctx,
                                     ObSQLSessionInfo &session);
  static int exec_add_policy_column(const common::ObString &ddl_stmt_str,
                                    const common::ObString &schema_name,
                                    const common::ObString &table_name,
                                    const common::ObString &column_name,
                                    ObExprCtx &expr_ctx,
                                    ObExecContext &exec_ctx,
                                    ObSQLSessionInfo &session,
                                    share::schema::ObSchemaGetterGuard &schema_guard);
  static int label_tag_compare(int64_t tenant_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const int64_t label_tag1,
                               const int64_t label_tag2,
                               int64_t &cmp_result);

};


//--------------- Functions for OLS policy --------------------

/**
 * @brief The ObExprOLSPolicyCreate class
 * PROCEDURE CREATE_POLICY (policy_name       IN VARCHAR2,
                            column_name       IN VARCHAR2 DEFAULT NULL,
                            default_options   IN VARCHAR2 DEFAULT NULL);
 */
class ObExprOLSPolicyCreate : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSPolicyCreate(common::ObIAllocator &alloc);
  virtual ~ObExprOLSPolicyCreate();
  int calc_result_type3(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        ObExprResType &type3,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSPolicyAlter class
 * PROCEDURE ALTER_POLICY (policy_name       IN  VARCHAR2,
                           default_options   IN  VARCHAR2 DEFAULT NULL);
 */
class ObExprOLSPolicyAlter : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSPolicyAlter(common::ObIAllocator &alloc);
  virtual ~ObExprOLSPolicyAlter();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSPolicyDrop class
 * PROCEDURE DROP_POLICY (policy_name IN VARCHAR2,
                          drop_column  BOOLEAN DEFAULT FALSE);
 */
class ObExprOLSPolicyDrop : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSPolicyDrop(common::ObIAllocator &alloc);
  virtual ~ObExprOLSPolicyDrop();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSPolicyDisable class
 * PROCEDURE DISABLE_POLICY (policy_name IN VARCHAR2);
 */
class ObExprOLSPolicyDisable : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSPolicyDisable(common::ObIAllocator &alloc);
  virtual ~ObExprOLSPolicyDisable();
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSPolicyEnable class
 * PROCEDURE ENABLE_POLICY (policy_name IN VARCHAR2);
 */
class ObExprOLSPolicyEnable : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSPolicyEnable(common::ObIAllocator &alloc);
  virtual ~ObExprOLSPolicyEnable();
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
};


//--------------- Functions for OLS level --------------------

/**
 * @brief The ObExprOLSLevelCreate class
 * PROCEDURE CREATE_LEVEL (policy_name IN VARCHAR2,
                           level_num         IN INTEGER,
                           short_name        IN VARCHAR2,
                           long_name         IN VARCHAR2);
 */
class ObExprOLSLevelCreate : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLevelCreate(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLevelCreate();

  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSLevelAlter class
 * PROCEDURE ALTER_LEVEL (policy_name IN VARCHAR2,
                          level_num       IN INTEGER,
                          new_short_name  IN VARCHAR2 DEFAULT NULL,
                          new_long_name   IN VARCHAR2 DEFAULT NULL);
   //TODO [label]:
   PROCEDURE ALTER_LEVEL (policy_name IN VARCHAR2,
                          short_name      IN VARCHAR2,
                          new_long_name   IN VARCHAR2);
 */
class ObExprOLSLevelAlter : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLevelAlter(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLevelAlter();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSLevelDrop class
 * PROCEDURE DROP_LEVEL (policy_name IN VARCHAR2,
                         level_num   IN INTEGER);
   //TODO: [label]
   PROCEDURE DROP_LEVEL (policy_name IN VARCHAR2,
                         short_name  IN VARCHAR2);
 */
class ObExprOLSLevelDrop : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLevelDrop(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLevelDrop();

  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
};



//--------------- Functions for OLS label --------------------

/**
 * @brief The ObExprOLSLabelCreate class
 * PROCEDURE CREATE_LABEL (
   policy_name IN VARCHAR2,
   label_tag   IN INTEGER,
   label_value IN VARCHAR2,
   data_label  IN BOOLEAN DEFAULT TRUE);
 */
class ObExprOLSLabelCreate : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelCreate(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelCreate();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSLabelAlter class
 * PROCEDURE ALTER_LABEL (
   policy_name       IN VARCHAR2,
   label_tag         IN INTEGER,
   new_label_value   IN VARCHAR2 DEFAULT NULL,
   new_data_label    IN BOOLEAN  DEFAULT NULL);

   //TODO [label]
   PROCEDURE ALTER_LABEL (
   policy_name       IN VARCHAR2,
   label_value       IN VARCHAR2,
   new_label_value   IN VARCHAR2 DEFAULT NULL,
   new_data_label    IN BOOLEAN  DEFAULT NULL);
 */
class ObExprOLSLabelAlter : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelAlter(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelAlter();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSLabelDrop class
 * PROCEDURE DROP_LABEL (
   policy_name       IN VARCHAR2,
   label_tag         IN INTEGER);

   //TODO [label]
   PROCEDURE DROP_LABEL (
   policy_name       IN VARCHAR2,
   label_value       IN VARCHAR2);
 */
class ObExprOLSLabelDrop : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelDrop(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelDrop();

  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;

};

//--------------- Functions for OLS table policy --------------------

/**
 * @brief The ObExprOLSTablePolicyApply class
 * PROCEDURE APPLY_TABLE_POLICY (policy_name       IN VARCHAR2,
                                 schema_name       IN VARCHAR2,
                                 table_name        IN VARCHAR2,
                                 table_options     IN VARCHAR2 DEFAULT NULL,
                                 label_function    IN VARCHAR2 DEFAULT NULL,
                                 predicate         IN VARCHAR2 DEFAULT NULL);
 */
class ObExprOLSTablePolicyApply : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSTablePolicyApply(common::ObIAllocator &alloc);
  virtual ~ObExprOLSTablePolicyApply();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSTablePolicyRemove class
 * PROCEDURE REMOVE_TABLE_POLICY (policy_name        IN VARCHAR2,
                                  schema_name        IN VARCHAR2,
                                  table_name         IN VARCHAR2,
                                  drop_column        IN BOOLEAN DEFAULT FALSE);
 */
class ObExprOLSTablePolicyRemove : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSTablePolicyRemove(common::ObIAllocator &alloc);
  virtual ~ObExprOLSTablePolicyRemove();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSTablePolicyApply class
 * PROCEDURE DISABLE_TABLE_POLICY (policy_name      IN VARCHAR2,
                                   schema_name      IN VARCHAR2,
                                   table_name       IN VARCHAR2);
 */
class ObExprOLSTablePolicyDisable : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSTablePolicyDisable(common::ObIAllocator &alloc);
  virtual ~ObExprOLSTablePolicyDisable();
  int calc_result_type3(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        ObExprResType &type3,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSTablePolicyEnable class
 * PROCEDURE ENABLE_TABLE_POLICY (policy_name     IN VARCHAR2,
                                  schema_name     IN VARCHAR2,
                                  table_name      IN VARCHAR2);
 */
class ObExprOLSTablePolicyEnable : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSTablePolicyEnable(common::ObIAllocator &alloc);
  virtual ~ObExprOLSTablePolicyEnable();
  int calc_result_type3(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        ObExprResType &type3,
                        common::ObExprTypeCtx &type_ctx) const;
};


//--------------- Functions for OLS schema policy --------------------
//TODO [label], alter mutiple table to add columns in one transaction


//--------------- Functions for OLS SA_USER_ADMIN -------------------->

/**
 * @brief The ObExprOLSTablePolicyRemove class
 * PROCEDURE SET_LEVELS (policy_name      IN VARCHAR2,
                         user_name        IN VARCHAR2,
                         max_level        IN VARCHAR2,
                         min_level        IN VARCHAR2 DEFAULT NULL,
                         def_level        IN VARCHAR2 DEFAULT NULL,
                         row_level        IN VARCHAR2 DEFAULT NULL);
 */
class ObExprOLSUserSetLevels : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSUserSetLevels(common::ObIAllocator &alloc);
  virtual ~ObExprOLSUserSetLevels();
  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types_array,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
};

//--------------- Functions for OLS SA_SESSION -------------------->

/**
 * @brief The ObExprOLSSessionSetLevel class
 * PROCEDURE SET_LABEL (policy_name IN VARCHAR2,
                        label IN VARCHAR2);
 */
class ObExprOLSSessionSetLabel : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSSessionSetLabel(common::ObIAllocator &alloc);
  virtual ~ObExprOLSSessionSetLabel();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
};

/**
 * @brief The ObExprOLSSessionSetRowLabel class
 * PROCEDURE SET_ROW_LABEL (policy_name IN VARCHAR2,
                            row_label IN VARCHAR2);
 */
class ObExprOLSSessionSetRowLabel : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSSessionSetRowLabel(common::ObIAllocator &alloc);
  virtual ~ObExprOLSSessionSetRowLabel();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
};
/**
 * @brief CREATE OR REPLACE PACKAGE BODY SA_SESSION AS
           PROCEDURE RESTORE_DEFAULT_LABELS (policy_name in VARCHAR2); 
 */
class ObExprOLSSessionRestoreDefaultLabels : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSSessionRestoreDefaultLabels(common::ObIAllocator &alloc);
  virtual ~ObExprOLSSessionRestoreDefaultLabels();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
};


class ObExprOLSSessionLabel : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSSessionLabel(common::ObIAllocator &alloc);
  virtual ~ObExprOLSSessionLabel();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

/* @brief
    SA_SESSION.ROW_LABEL ( 
      policy_name IN VARCHAR2)
    RETURN VARCHAR2; 
*/
class ObExprOLSSessionRowLabel : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSSessionRowLabel(common::ObIAllocator &alloc);
  virtual ~ObExprOLSSessionRowLabel();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_row_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};


//--------------- Functions for OLS CMP -------------------->
/**
 * @brief The ObExprOLSLabelCompare class
 * obj1 int
 * obj2 int
 * only compare the label tag value
 */
class ObExprOLSLabelCmpLE : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelCmpLE(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelCmpLE();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_cmple(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

class ObExprOLSLabelCheck : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelCheck(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelCheck();
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_label_check(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

class ObExprOLSLabelToChar : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSLabelToChar(common::ObIAllocator &alloc);
  virtual ~ObExprOLSLabelToChar();
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_label_to_char(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

class ObExprOLSCharToLabel : public ObFuncExprOperator, ObExprOLSBase
{
public:
  explicit ObExprOLSCharToLabel(common::ObIAllocator &alloc);
  virtual ~ObExprOLSCharToLabel();
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_char_to_label(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

}
}

#endif // _OB_EXPR_OLS_FUNCS_H
