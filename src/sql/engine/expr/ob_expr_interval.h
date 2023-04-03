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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_INTERVAL_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_INTERVAL_H_

#include <cstdint>

#include "objit/common/ob_item_type.h"
#include "ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInterval: public ObExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
	explicit ObExprInterval(common::ObIAllocator &alloc);

	virtual ~ObExprInterval() {}

	virtual int assign(const ObExprOperator &other);

  OB_INLINE void set_use_binary_search(bool use_binary_search) {use_binary_search_ = use_binary_search;}

  virtual int calc_result_typeN(ObExprResType &type,
	                                ObExprResType *types,
	                                int64_t param_num,
	                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_interval_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res);
private:
	bool use_binary_search_; //use binary search or sequential search during calc
};

} // namespace sql
} // namespace oceanbase


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_INTERVAL_H_ */
