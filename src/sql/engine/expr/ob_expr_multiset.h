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

#ifndef DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_MULTISET_H_
#define DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_MULTISET_H_
#include "sql/engine/expr/ob_expr_operator.h"
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

struct ObExprMultiSetInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprMultiSetInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID),
        ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);
  ObMultiSetType ms_type_;
  ObMultiSetModifier ms_modifier_;
};

class ObExprMultiSet : public ObExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprMultiSet(common::ObIAllocator &alloc);
  virtual ~ObExprMultiSet();

  virtual void reset();
  int assign(const ObExprOperator &other);
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_multiset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

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

  static int calc_ms_one_distinct(common::ObIAllocator *coll_allocator,
                                   ObObj *c,
                                   int64_t count,
                                   ObObj *&data_arr,
                                   int64_t &elem_count);
private:
  static int calc_ms_union(common::ObIAllocator *coll_allocator,
                    pl::ObPLCollection *c1,
                    pl::ObPLCollection *c2,
                    ObObj *&data_arr,
                    int64_t &elem_count,
                    ObMultiSetType ms_type,
                    ObMultiSetModifier ms_modifier);
  static int calc_ms_intersect(common::ObIAllocator *coll_allocator,
                        pl::ObPLCollection *c1,
                        pl::ObPLCollection *c2,
                        ObObj *&data_arr,
                        int64_t &elem_count,
                        ObMultiSetType ms_type,
                        ObMultiSetModifier ms_modifier);
  static int calc_ms_except(common::ObIAllocator *coll_allocator,
                     pl::ObPLCollection *c1,
                     pl::ObPLCollection *c2,
                     ObObj *&data_arr,
                     int64_t &elem_count,
                     ObMultiSetType ms_type,
                     ObMultiSetModifier ms_modifier);

  static int calc_ms_impl(common::ObIAllocator *coll_allocator,
                   pl::ObPLCollection *c1,
                   pl::ObPLCollection *c2,
                   ObObj *&data_arr,
                   int64_t &elem_count,
                   ObMultiSetType ms_type,
                   ObMultiSetModifier ms_modifier);

  static int calc_ms_all_impl(common::ObIAllocator *coll_allocator,
                       pl::ObPLCollection *c1,
                       pl::ObPLCollection *c2,
                       ObObj *&data_arr,
                       int64_t &elem_count,
                       bool allow_dup,
                       ObMultiSetType ms_type,
                       ObMultiSetModifier ms_modifier);

  static int calc_ms_distinct_impl(common::ObIAllocator *coll_allocator,
                                   pl::ObPLCollection *c1,
                                   pl::ObPLCollection *c2,
                                   ObObj *&data_arr,
                                   int64_t &elem_count,
                                   ObMultiSetType ms_type,
                                   ObMultiSetModifier ms_modifier);

  DISALLOW_COPY_AND_ASSIGN(ObExprMultiSet);
private:
  ObMultiSetType ms_type_;
  ObMultiSetModifier ms_modifier_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_OBJ_ACCESS_H_ */
