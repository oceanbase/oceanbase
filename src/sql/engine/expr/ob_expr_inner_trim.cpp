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
#include "sql/engine/expr/ob_expr_inner_trim.h"
#include "sql/engine/expr/ob_expr_trim.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObExprInnerTrim::ObExprInnerTrim(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_INNER_TRIM, N_INNER_TRIM, 3, VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
  need_charset_convert_ = false;
}

ObExprInnerTrim::~ObExprInnerTrim()
{
}

inline int ObExprInnerTrim::calc_result_type3(ObExprResType &type,
                                         ObExprResType &trim_type,
                                         ObExprResType &trim_pattern,
                                         ObExprResType &text,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // %trim_type, %trim_pattern, %text are adjacent elements in the array
  CK(&trim_type + 1 == &trim_pattern);
  CK(&trim_type + 2 == &text);
  OZ(ObExprTrim::deduce_result_type(type, &trim_type, 3, type_ctx));
  LOG_DEBUG("inner trim", K(type), K(text), K(trim_pattern));

  return ret;
}

int ObExprInnerTrim::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  // inner trim seems has no difference with trim, set the trim evaluate function directly.
  rt_expr.eval_func_ = &ObExprTrim::eval_trim;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerTrim, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

}
}
