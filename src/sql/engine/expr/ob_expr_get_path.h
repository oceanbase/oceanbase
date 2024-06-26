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

#ifndef OB_EXPR_GET_PATH_H
#define OB_EXPR_GET_PATH_H

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

struct ObDataAccessPathExtraInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDataAccessPathExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type)
  {}
  virtual ~ObDataAccessPathExtraInfo() {}
  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
  TO_STRING_KV(K(type_), K(data_access_path_));
  ObString data_access_path_;
};


class ObExprGetPath: public ObFuncExprOperator
{
public:
  explicit ObExprGetPath(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_GET_PATH, N_GET_PATH, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}
  virtual ~ObExprGetPath() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const
  {
    UNUSED(type1);
    UNUSED(type2);
    UNUSED(type_ctx);
    type.set_varchar();
    type.set_collation_type(CS_TYPE_BINARY);
    return common::OB_SUCCESS;
  }
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override {
    return common::OB_NOT_SUPPORTED;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGetPath);
};

}
}
#endif // OB_EXPR_GET_PATH_H
