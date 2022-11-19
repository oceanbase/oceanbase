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
private:
  static const int8_t PARAM_NUM_TWO = 2;
  static const int8_t PARAM_NUM_THREE = 3;

  DISALLOW_COPY_AND_ASSIGN(ObExprLocate);
};


}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_LOCATE_H_ */
