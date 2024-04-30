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
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{

class ObExprSysContext : public ObFuncExprOperator {
public:
  ObExprSysContext();
  explicit ObExprSysContext(common::ObIAllocator& alloc);
  virtual ~ObExprSysContext();
  virtual int calc_result_type2(ObExprResType& type,
                                ObExprResType& arg1,
                                ObExprResType& arg2,
                                common::ObExprTypeCtx& type_ctx) const override;
  struct UserEnvParameter
  {
    char name[32];
    int (*eval)(const ObExpr &expr, common::ObDatum &res, const common::ObDatum &arg1,
                const common::ObDatum &arg2, ObEvalCtx &ctx);
  };
  struct NLS_Lang
  {
    char language[128];
    char abbreviated[32];
  };

  static int eval_schemaid(const ObExpr &expr, common::ObDatum &res,
                           const common::ObDatum &arg1,
                           const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_current_schemaid(const ObExpr &expr, common::ObDatum &res,
                                   const common::ObDatum &arg1,
                                   const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_current_schema(const ObExpr &expr, common::ObDatum &res,
                                 const common::ObDatum &arg1,
                                 const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_user(const ObExpr &expr, common::ObDatum &res,
                       const common::ObDatum &arg1,
                       const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_tenant_name(const ObExpr &expr, common::ObDatum &res,
                              const common::ObDatum &arg1,
                              const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_tenant_id(const ObExpr &expr, common::ObDatum &res,
                            const common::ObDatum &arg1,
                            const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_sessionid(const ObExpr &expr, common::ObDatum &res,
                            const common::ObDatum &arg1,
                            const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_ip_address(const ObExpr &expr, common::ObDatum &res,
                            const common::ObDatum &arg1,
                            const common::ObDatum &arg2, ObEvalCtx &ctx);
  static int eval_curr_user_id(const ObExpr &expr, common::ObDatum &res,
                               const common::ObDatum &arg1, const common::ObDatum &arg2,
                               ObEvalCtx &ctx);
  static int eval_curr_user(const ObExpr &expr, common::ObDatum &res,
                               const common::ObDatum &arg1, const common::ObDatum &arg2,
                               ObEvalCtx &ctx);
  static int eval_instance(const ObExpr &expr, common::ObDatum &res,
                           const common::ObDatum &arg1, const common::ObDatum &arg2,
                           ObEvalCtx &ctx);
  static int eval_instance_name(const ObExpr &expr, common::ObDatum &res,
                           const common::ObDatum &arg1, const common::ObDatum &arg2,
                           ObEvalCtx &ctx);
  static int eval_language(const ObExpr &expr, common::ObDatum &res,
                           const common::ObDatum &arg1, const common::ObDatum &arg2,
                           ObEvalCtx &ctx);
  static int eval_lang(const ObExpr &expr, common::ObDatum &res,
                           const common::ObDatum &arg1, const common::ObDatum &arg2,
                           ObEvalCtx &ctx);
  static int eval_action(const ObExpr &expr, common::ObDatum &res,
                         const common::ObDatum &arg1, const common::ObDatum &arg2,
                         ObEvalCtx &ctx);
  static int eval_client_info(const ObExpr &expr, common::ObDatum &res,
                              const common::ObDatum &arg1, const common::ObDatum &arg2,
                              ObEvalCtx &ctx);
  static int eval_module(const ObExpr &expr, common::ObDatum &res,
                         const common::ObDatum &arg1, const common::ObDatum &arg2,
                         ObEvalCtx &ctx);
  static int eval_client_identifier(const ObExpr &expr, common::ObDatum &res,
                                    const common::ObDatum &arg1, const common::ObDatum &arg2,
                                    ObEvalCtx &ctx);
  static int eval_application_context(const ObExpr &expr, common::ObDatum &res,
                                      const common::ObString &arg1, const common::ObString &arg2,
                                      ObEvalCtx &ctx);
  static int eval_proxy_user(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                const ObDatum &arg2, ObEvalCtx &ctx);

  static int eval_proxy_user_id(const ObExpr &expr, ObDatum &res,
                                    const ObDatum &arg1, const ObDatum &arg2,
                                    ObEvalCtx &ctx);

  static int eval_auth_identity(const ObExpr &expr, ObDatum &res, const ObDatum &arg1,
                                const ObDatum &arg2, ObEvalCtx &ctx);
  typedef int (*eval_fun)(const ObExpr &expr, common::ObDatum &res,
                          const common::ObDatum &arg1,
                          const common::ObDatum &arg2, ObEvalCtx &ctx);

  static int eval_sys_context(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int get_schema_guard(share::schema::ObSchemaGetterGuard &schema_guard, uint64_t tenant_id);
private:
  static UserEnvParameter userenv_parameters_[];
  static NLS_Lang lang_map_[];
  static const int DEFAULT_LENGTH = 256;

  static int get_eval_fun(const common::ObString& ns_str, const common::ObString& para_str,
                          eval_fun &fun);
  static int get_userenv_fun(const common::ObString& para_str, eval_fun &fun);
  static int uint_string(const ObExpr &expr, ObEvalCtx &ctx, uint64_t id,
                         common::ObDatum &res);
  static int extract_ip(const common::ObString &user_at_client_ip, int64_t &start);
  DISALLOW_COPY_AND_ASSIGN(ObExprSysContext);
};

} // end namespace sql
} // end namespace oceanbase
#endif
