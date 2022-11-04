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
