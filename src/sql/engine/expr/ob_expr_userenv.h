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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_USERENV_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_USERENV_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{

class ObExprUserEnv : public ObFuncExprOperator {
public:
  ObExprUserEnv();
  explicit ObExprUserEnv(common::ObIAllocator& alloc);
  virtual ~ObExprUserEnv();
  virtual int calc_result_type1(ObExprResType& type,
                                ObExprResType& arg1,
                                common::ObExprTypeCtx& type_ctx,
                                common::ObIArray<common::ObObj*> &arg_arrs) const override;
  struct NLS_Lang
  {
    char language[128];
    char abbreviated[32];
  };
  struct UserEnvParameter
  {
    char name[15];
    bool is_str;
    int (*eval)(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  };
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_user_env_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_schemaid_result1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_sessionid_result1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_instance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_lang(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_language(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_client_info(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  static NLS_Lang lang_map_[];
  int check_arg_valid(const common::ObObj &value, UserEnvParameter &para,
                      common::ObIAllocator *alloc) const;
  static UserEnvParameter parameters_[];
  static const int DEFAULT_LENGTH = 64;
  DISALLOW_COPY_AND_ASSIGN(ObExprUserEnv);
};

} // end namespace sql
} // end namespace oceanbase
#endif
