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

#ifndef OCEANBASE_OB_SQL_EXPR_ARG_CASE_H_
#define OCEANBASE_OB_SQL_EXPR_ARG_CASE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase
{
namespace sql
{

typedef int (*ob_get_cmp_type_func) (common::ObObjType &type,
                                     const common::ObObjType &type1,
                                     const common::ObObjType &type2,
                                     const common::ObObjType &type3);

class ObExprArgCase : public ObExprOperator
{
public:
  explicit  ObExprArgCase(common::ObIAllocator &alloc);
  virtual ~ObExprArgCase();

  virtual int assign(const ObExprOperator &other);

  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos);

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_with_cast(common::ObObj &result,
                  const common::ObObj *objs_stack,
                  int64_t param_num,
                  common::ObCompareCtx &cmp_ctx,
                  common::ObCastCtx &cast_ctx,
                  const ObExprResType &res_type,
                  const ob_get_cmp_type_func get_cmp_type_func);
  static int calc_without_cast(common::ObObj &result,
                  const common::ObObj *objs_stack,
                  int64_t param_num,
                  common::ObCastCtx &cast_ctx,
                  common::ObCompareCtx &cmp_ctx,
                  const ObExprResType &res_type);
  void set_need_cast(bool need_cast) {need_cast_ = need_cast;}

  inline static int get_cmp_type(common::ObObjType &type,
                                 const common::ObObjType &type1,
                                 const common::ObObjType &type2,
                                 const common::ObObjType &type3)
  {
    UNUSED(type3);
    return ObExprResultTypeUtil::get_relational_cmp_type(type,type1,type2);
  }

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprArgCase);
private:
  bool need_cast_;
};
}
}
#endif // OCEANBASE_OB_SQL_EXPR_ARG_CASE_H_
