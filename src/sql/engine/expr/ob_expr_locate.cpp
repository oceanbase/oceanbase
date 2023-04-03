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
#include "ob_expr_locate.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_instr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

ObExprLocate::ObExprLocate(ObIAllocator &alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_LOCATE, N_LOCATE, TWO_OR_THREE, NOT_ROW_DIMENSION) {}

ObExprLocate::~ObExprLocate()
{
  // TODO Auto-generated destructor stub
}

int ObExprLocate::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_array,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (PARAM_NUM_TWO != param_num && PARAM_NUM_THREE != param_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("locate() should have two or three arguments", K(ret));
  } else if (OB_ISNULL(types_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. types_array is null", K(ret), K(types_array));
  } else if (OB_FAIL(ObLocationExprOperator::calc_result_type2(type, types_array[0],
                                                               types_array[1], type_ctx))) {
    LOG_WARN("calc result type failed", K(ret), K(types_array[0]), K(types_array[1]));
  } else if (3 == param_num) {
    types_array[2].set_calc_type(ObIntType);
    ObCastMode cm = lib::is_oracle_mode() ? CM_NONE :
                                            CM_STRING_INTEGER_TRUNC | CM_WARN_ON_FAIL;
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | cm);
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
