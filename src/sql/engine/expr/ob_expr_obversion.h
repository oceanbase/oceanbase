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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprObVersion : public ObStringExprOperator {
public:
  explicit ObExprObVersion(common::ObIAllocator& alloc);
  virtual ~ObExprObVersion();
  virtual int calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_ */
