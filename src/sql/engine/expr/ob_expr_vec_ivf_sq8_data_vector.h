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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_SQ8_DATA_VECTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_SQ8_DATA_VECTOR_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprVecIVFSQ8DataVector : public ObFuncExprOperator
{
public:
  explicit ObExprVecIVFSQ8DataVector(common::ObIAllocator &alloc);
  virtual ~ObExprVecIVFSQ8DataVector() {}
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
  static int generate_data_vector(
      const ObExpr &expr,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
  static int cal_u8_data_vector(ObIAllocator &alloc, uint32_t size, float *min_vec, float *step_vec, float *data_vec, uint8_t *&res_vec);
  virtual bool need_rt_ctx() const override { return true; }
private :
  static const int SQ_META_SIZE = 3;
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprVecIVFSQ8DataVector);
};
}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_SQ8_DATA_VECTOR_H_ */
