/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_PROMOTION_UTIL_H_
#define _OB_EXPR_PROMOTION_UTIL_H_

//#include "sql/engine/expr/ob_expr_operator.h"
#include "common/object/ob_obj_type.h"
//#include "common/expression/ob_expr_string_buf.h"
//#include "lib/timezone/ob_timezone_info.h"

namespace oceanbase
{
namespace sql
{
  class ObExprResType;
  class ObExprPromotionUtil
  {
  public:
    static int get_nvl_type(
      ObExprResType &type,
      const ObExprResType &type1,
      const ObExprResType &type2);
  private:
    static int get_calc_type(
      ObExprResType &type,
      const ObExprResType &type1,
      const ObExprResType &type2,
      const common::ObObjType map[common::ObMaxTC][common::ObMaxTC]);
};

}
}
#endif  /* _OB_EXPR_PROMOTION_UTIL_H_ */
