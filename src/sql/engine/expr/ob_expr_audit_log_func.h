/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AUDIT_LOG_FUNC_H
#define OCEANBASE_SQL_OB_EXPR_AUDIT_LOG_FUNC_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprAuditLogFunc : public ObStringExprOperator
{
public:
  explicit ObExprAuditLogFunc(common::ObIAllocator &alloc,
                              ObExprOperatorType type,
                              const char *name,
                              int32_t param_num);
  virtual ~ObExprAuditLogFunc() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
protected:
  static int check_privilege(ObSQLSessionInfo &session,
                             bool &is_valid,
                             common::ObString &error_info);
  static int parse_user_name(const common::ObString &str,
                             common::ObString &user_name,
                             common::ObString &host,
                             bool &is_valid,
                             common::ObString &error_info);
  static int parse_filter_name(const common::ObString &str,
                               common::ObString &filter_name,
                               bool &is_valid,
                               common::ObString &error_info);
  static int parse_definition(const common::ObString &str,
                              common::ObString &definition,
                              bool &is_valid,
                              common::ObString &error_info);
  static int fill_res_datum(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const bool is_valid,
                            const common::ObString &error_info,
                            ObDatum &expr_datum);
private:
  int check_data_version(common::ObExprTypeCtx &type_ctx) const;
  int check_param_type(const ObExprResType &type) const;
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAuditLogFunc);
};

class ObExprAuditLogSetFilter : public ObExprAuditLogFunc
{
public:
  explicit ObExprAuditLogSetFilter(common::ObIAllocator &alloc);
  virtual ~ObExprAuditLogSetFilter() {}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_set_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAuditLogSetFilter);
};

class ObExprAuditLogRemoveFilter : public ObExprAuditLogFunc
{
public:
  explicit ObExprAuditLogRemoveFilter(common::ObIAllocator &alloc);
  virtual ~ObExprAuditLogRemoveFilter() {}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_remove_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAuditLogRemoveFilter);
};

class ObExprAuditLogSetUser : public ObExprAuditLogFunc
{
public:
  explicit ObExprAuditLogSetUser(common::ObIAllocator &alloc);
  virtual ~ObExprAuditLogSetUser() {}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_set_user(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAuditLogSetUser);
};

class ObExprAuditLogRemoveUser : public ObExprAuditLogFunc
{
public:
  explicit ObExprAuditLogRemoveUser(common::ObIAllocator &alloc);
  virtual ~ObExprAuditLogRemoveUser() {}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_remove_user(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAuditLogRemoveUser);
};

}
}
#endif
