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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_SYS_CONTEXT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_SYS_CONTEXT_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase {
namespace sql {

class ObExprSysContext : public ObFuncExprOperator {
public:
  ObExprSysContext();
  explicit ObExprSysContext(common::ObIAllocator& alloc);
  virtual ~ObExprSysContext();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& arg1, ObExprResType& arg2, common::ObExprTypeCtx& type_ctx) const override;
  virtual int calc_result2(common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2,
      common::ObExprCtx& expr_ctx) const override;

  struct UserEnvParameter {
    char name[32];
    int (*calc)(
        common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
    int (*eval)(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1, const common::ObDatum& arg2,
        ObEvalCtx& ctx);
  };
  static int calc_schemaid(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int calc_current_schemaid(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int calc_current_schema(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int calc_user(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int get_tenant_name(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int get_tenant_id(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int calc_sessionid_result(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  static int calc_ip_address(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);
  typedef int (*calc_fun)(
      common::ObObj& result, const common::ObObj& arg1, const common::ObObj& arg2, common::ObExprCtx& expr_ctx);

  static int eval_schemaid(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_current_schemaid(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_current_schema(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_user(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_tenant_name(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_tenant_id(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_sessionid(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  static int eval_ip_address(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);
  typedef int (*eval_fun)(const ObExpr& expr, common::ObDatum& res, const common::ObDatum& arg1,
      const common::ObDatum& arg2, ObEvalCtx& ctx);

  static int eval_sys_context(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);
  virtual int cg_expr(ObExprCGCtx& ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

private:
  static int get_calc_fun(const common::ObString& ns_str, const common::ObString& para_str, calc_fun& fun);
  static int get_userenv_fun(const common::ObString& para_str, calc_fun& fun);
  static UserEnvParameter userenv_parameters_[];
  static const int DEFAULT_LENGTH = 256;

  static int get_eval_fun(const common::ObString& ns_str, const common::ObString& para_str, eval_fun& fun);
  static int get_userenv_fun(const common::ObString& para_str, eval_fun& fun);
  static int uint_string(const ObExpr& expr, ObEvalCtx& ctx, uint64_t id, common::ObDatum& res);
  static int extract_ip(const common::ObString& user_at_client_ip, int64_t& start);
  DISALLOW_COPY_AND_ASSIGN(ObExprSysContext);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
