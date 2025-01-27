/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_META_ID_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_META_ID_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprVecIVFMetaID : public ObFuncExprOperator
{
public:
  explicit ObExprVecIVFMetaID(common::ObIAllocator &alloc);
  virtual ~ObExprVecIVFMetaID() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN;}
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;
  static int generate_meta_id(
      const ObExpr &expr,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprVecIVFMetaID);
};
}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_META_ID_H_ */
