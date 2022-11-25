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

#ifndef DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_COLL_PRED_H_
#define DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_COLL_PRED_H_
#include "sql/engine/expr/ob_expr_multiset.h"
#include "lib/container/ob_fast_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{

namespace pl{
class ObPLCollectoin;
}
namespace sql
{
struct ObExprCollPredInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprCollPredInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);
  ObMultiSetType ms_type_;
  ObMultiSetModifier ms_modifier_;
  int64_t tz_offset_;
  ObExprResType result_type_;
};

class ObExprCollPred : public ObExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprCollPred(common::ObIAllocator &alloc);
  virtual ~ObExprCollPred();

  virtual void reset();
  int assign(const ObExprOperator &other);
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  VIRTUAL_TO_STRING_KV(N_EXPR_TYPE, get_type_name(type_),
                       N_EXPR_NAME, name_,
                       N_DIM, row_dimension_,
                       N_REAL_PARAM_NUM, real_param_num_,
                       K_(ms_type),
                       K_(ms_modifier));

  inline void set_ms_type(ObMultiSetType ms_type) { ms_type_ = ms_type; }
  inline ObMultiSetType get_ms_type() const { return ms_type_; }
  inline void set_ms_modifier(ObMultiSetModifier ms_modifier) { ms_modifier_ = ms_modifier; }
  inline ObMultiSetModifier get_ms_modifier() const { return ms_modifier_; }

  static int calc_is_submultiset(const ObObj &obj1,
                                     const ObObj &obj2,
                                     int64_t tz_offset,
                                     const ObExprCalcType calc_type,
                                     CollectionPredRes &result);
  static int compare_obj(const ObObj &obj1, const ObObj &obj2, ObCompareCtx &cmp_ctx);

  static int calc_collection_is_contained_without_null(const pl::ObPLCollection *c1,
                                                     const pl::ObPLCollection *c2,
                                                     int64_t tz_offset,
                                                     const ObExprCalcType calc_type,
                                                     CollectionPredRes &result);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_coll_pred(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCollPred);

private:
  ObMultiSetType ms_type_;
  ObMultiSetModifier ms_modifier_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_COLL_PRED_H_ */
