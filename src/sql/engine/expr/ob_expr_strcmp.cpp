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
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast.h"

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
  UNUSED(type2);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObCollationLevel coll_level = CS_LEVEL_EXPLICIT;
  ret = ObCharset::aggregate_collation(type1.get_collation_level(),
                                       type1.get_collation_type(),
                                       type2.get_collation_level(),
                                       type2.get_collation_type(),
                                       coll_level,
                                       coll_type);
  type.set_calc_type(ObVarcharType);
  type.set_calc_collation_type(coll_type);
  type.set_calc_collation_level(coll_level);
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(coll_type);
  type2.set_calc_collation_type(coll_type);

  return ret;
}

}//end sql namespace
}//end oceanbase namespace

