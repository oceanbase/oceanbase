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
 * This file contains implementation for eval_sdo_relate.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SDO_RELATE_H_
#define OCEANBASE_SQL_OB_EXPR_SDO_RELATE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
typedef union ObSdoRelateMask
{
  struct {
    uint16_t anyinteract_ : 1;
    uint16_t contains_ : 1;
    uint16_t coveredby_ : 1;
    uint16_t covers_ : 1;
    uint16_t equal_ : 1;
    uint16_t on_ : 1;
    uint16_t overlapbdydisjoint_ : 1;
    uint16_t overlapbdyintersect_ : 1;
    uint16_t inside_ : 1;
    uint16_t touch_ : 1;
    uint16_t reserved_ : 6;
  };

  uint16_t flags_;
} ObSdoRelateMask;

class ObSdoRelationship
{
public:
  static constexpr char* ANYINTERACT = const_cast<char*>("ANYINTERACT");
  static constexpr char* CONTAINS = const_cast<char*>("CONTAINS");
  static constexpr char* COVEREDBY = const_cast<char*>("COVEREDBY");
  static constexpr char* COVERS = const_cast<char*>("COVERS");
  static constexpr char* EQUAL = const_cast<char*>("EQUAL");
  static constexpr char* ON = const_cast<char*>("ON");
  static constexpr char* OVERLAPBDYDISJOINT = const_cast<char*>("OVERLAPBDYDISJOINT");
  static constexpr char* OVERLAPBDYINTERSECT = const_cast<char*>("OVERLAPBDYINTERSECT");
  static constexpr char* INSIDE = const_cast<char*>("INSIDE");
  static constexpr char* TOUCH = const_cast<char*>("TOUCH");
};
class ObExprSdoRelate : public ObFuncExprOperator
{
public:
  explicit ObExprSdoRelate(common::ObIAllocator &alloc);
  virtual ~ObExprSdoRelate();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_sdo_relate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int get_params(ObExpr *param_expr, ObArenaAllocator& temp_allocator, ObEvalCtx &ctx, ObSdoRelateMask& mask);
  static int set_relate_result(ObIAllocator &res_alloc, ObDatum &res, bool result);
  DISALLOW_COPY_AND_ASSIGN(ObExprSdoRelate);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_INTERSECTS_H_