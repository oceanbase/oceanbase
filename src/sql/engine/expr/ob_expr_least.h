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

#ifndef _OB_SQL_EXPR_LEAST_H_
#define _OB_SQL_EXPR_LEAST_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLeastGreatest : public ObMinMaxExprOperator
{
public:
  explicit ObExprLeastGreatest(common::ObIAllocator &alloc, ObExprOperatorType type,
                                    const char *name, int32_t param_num);
  virtual ~ObExprLeastGreatest() {}

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  int calc_result_typeN_oracle(ObExprResType &type,
                        ObExprResType *types_stack,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
  int calc_result_typeN_mysql(ObExprResType &type,
                        ObExprResType *types_stack,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int cast_param(const ObExpr &src_expr, ObEvalCtx &ctx,
                        const ObDatumMeta &dst_meta,
                        const ObCastMode &cm, ObIAllocator &allocator,
                        ObDatum &res_datum);
  static int cast_result(const ObExpr &src_expr, const ObExpr &dst_expr, ObEvalCtx &ctx,
                         const ObCastMode &cm,
                         ObDatum &expr_datum);
  static int calc_mysql(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, bool least);
  static int calc_oracle(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, bool least);
  // left < right: return true, else return false.
  static inline bool cmp_integer(const ObDatum &l_datum, const bool l_is_int,
                                 const ObDatum &r_datum, const bool r_is_int)
  {
    bool ret_bool = true;
    if (l_is_int && r_is_int) {
      ret_bool = l_datum.get_int() < r_datum.get_int();
    } else if (!l_is_int && !r_is_int) {
      ret_bool = l_datum.get_uint() < r_datum.get_uint();
    } else if (l_is_int && !r_is_int) {
      ret_bool = l_datum.get_int() < r_datum.get_uint();
    } else {
      ret_bool = l_datum.get_uint() < r_datum.get_int();
    }
    return ret_bool;
  }
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprLeastGreatest);
};

class ObExprLeast : public ObExprLeastGreatest
{
public:
  explicit  ObExprLeast(common::ObIAllocator &alloc);
  virtual ~ObExprLeast() {}

  static int calc_least(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprLeast);
};


}
}
#endif /* _OB_SQL_EXPR_LEAST_H_ */
