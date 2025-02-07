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
 * This file contains implementation for array_map.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_MAP
#define OCEANBASE_SQL_OB_EXPR_ARRAY_MAP

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"


namespace oceanbase
{
namespace sql
{

// used in expr.extra_
struct ObExprArrayMapInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprArrayMapInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        allocator_(alloc),
        param_exprs_(NULL),
        param_num_(0),
        param_idx_(NULL)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

public:

  // for deserialize
  common::ObIAllocator &allocator_;
  ObExpr** param_exprs_;
	int64_t	param_num_;
  uint32_t* param_idx_;
};

class ObExprArrayMap : public ObFuncExprOperator
{
public:
  explicit ObExprArrayMap(common::ObIAllocator &alloc);
  explicit ObExprArrayMap(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayMap();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_map(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:

  int get_array_map_lambda_params(const ObRawExpr *raw_expr, ObArray<uint32_t> &param_idx, int depth,
                                  ObArray<ObExpr *> &param_exprs) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayMap);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_MAP