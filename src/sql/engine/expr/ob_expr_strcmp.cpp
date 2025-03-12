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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_strcmp.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{


ObExprStrcmp::ObExprStrcmp(ObIAllocator &alloc)
    : ObRelationalExprOperator::ObRelationalExprOperator(alloc, T_FUN_SYS_STRCMP, N_STRCMP, 2, NOT_ROW_DIMENSION)
{
}

int ObExprStrcmp::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObCollationLevel coll_level = CS_LEVEL_EXPLICIT;
  ObExprResTypes res_types;
  if (OB_FAIL(res_types.push_back(type1))) {
    LOG_WARN("fail to push back res type", K(ret));
  } else if (OB_FAIL(res_types.push_back(type2))) {
    LOG_WARN("fail to push back res type", K(ret));
  } else if (OB_FAIL(aggregate_charsets_for_comparison(type, &res_types.at(0), 2, type_ctx))) {
    LOG_WARN("failed to aggregate_charsets_for_comparison", K(ret));
  } else {
    type.set_calc_type(ObVarcharType);
    type1.set_calc_type(ObVarcharType);
    type2.set_calc_type(ObVarcharType);
    type1.set_calc_collation(type);
    type2.set_calc_collation(type);
  }
  return ret;
}

}//end sql namespace
}//end oceanbase namespace

