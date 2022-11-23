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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_GET_CURSOR_ATTR_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_GET_CURSOR_ATTR_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "pl/ob_pl_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprPLGetCursorAttr : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprPLGetCursorAttr(common::ObIAllocator &alloc);
  virtual ~ObExprPLGetCursorAttr();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_pl_get_cursor_attr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int assign(const ObExprOperator &other);
  void set_pl_get_cursor_attr_info(const pl::ObPLGetCursorAttrInfo &cursor_info)
  {
    pl_cursor_info_ = cursor_info;
  }
  const pl::ObPLGetCursorAttrInfo& get_pl_get_cursor_attr_info() const
  {
    return pl_cursor_info_;
  }
public:
  struct ExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type) {}
    virtual ~ExtraInfo() {}
    static int init_pl_cursor_info(ObIAllocator *allocator,
                                   const ObExprOperatorType type,
                                   const pl::ObPLGetCursorAttrInfo &cursor_info,
                                   ObExpr &rt_expr);
    virtual int deep_copy(common::ObIAllocator &allocator,
                          const ObExprOperatorType type,
                          ObIExprExtraInfo *&copied_info) const override;
    TO_STRING_KV(K_(pl_cursor_info));

    pl::ObPLGetCursorAttrInfo pl_cursor_info_;
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPLGetCursorAttr);
private:
  pl::ObPLGetCursorAttrInfo pl_cursor_info_;
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_GET_CURSOR_ATTR_H_ */
