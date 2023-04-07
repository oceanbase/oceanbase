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

#ifndef OCEANBASE_SQL_OB_EXPR_OBJECT_CONSTRUCT_H_
#define OCEANBASE_SQL_OB_EXPR_OBJECT_CONSTRUCT_H_

#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{

struct ObExprObjectConstructInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprObjectConstructInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type), rowsize_(0),
      udt_id_(OB_INVALID_ID), elem_types_(alloc)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);

  int64_t rowsize_;
  uint64_t udt_id_;
  common::ObFixedArray<ObExprResType, common::ObIAllocator> elem_types_;

};

class ObExprObjectConstruct : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprObjectConstruct(common::ObIAllocator &alloc);
  virtual ~ObExprObjectConstruct();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_object_construct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int check_types(const common::ObObj *objs_stack,
                         const common::ObIArray<ObExprResType> &elem_types,
                         int64_t param_num);
  static int fill_obj_stack(const ObExpr &expr, ObEvalCtx &ctx, ObObj *objs);

  static int newx(ObEvalCtx &ctx, ObObj &result, uint64_t udt_id);

  virtual void reset() {
    rowsize_ = 0;
    udt_id_ = OB_INVALID_ID;
    elem_types_.reset();
    ObFuncExprOperator::reset();
  }
  inline void set_rowsize(int64_t rowsize) { rowsize_ = rowsize; }
  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }
  inline int set_elem_types(const common::ObIArray<ObExprResType> &elem_types)
  {
    return elem_types_.assign(elem_types);
  }
private:
  int64_t rowsize_;
  uint64_t udt_id_;
  common::ObSEArray<ObExprResType, 5> elem_types_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprObjectConstruct);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_USER_DEFINED_FUNC_H_
