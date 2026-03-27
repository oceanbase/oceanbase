/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_LOCATE_H_
#define OB_EXPR_LOCATE_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLocate : public ObLocationExprOperator
{
public:
  explicit  ObExprLocate(common::ObIAllocator &alloc);
  virtual ~ObExprLocate();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  static const int8_t PARAM_NUM_TWO = 2;
  static const int8_t PARAM_NUM_THREE = 3;

  DISALLOW_COPY_AND_ASSIGN(ObExprLocate);
};


}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_LOCATE_H_ */
