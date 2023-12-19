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
#include "sql/engine/expr/ob_expr_unhex.h"
#include <string.h>
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprUnhex::ObExprUnhex(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_UNHEX, N_UNHEX, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprUnhex::~ObExprUnhex()
{
}

int ObExprUnhex::cg_expr(ObExprCGCtx &op_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprUnhex::eval_unhex;

  return ret;
}

int ObExprUnhex::eval_unhex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool has_set_res = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("eval radian arg failed", K(ret), K(expr));
  } else if (param->is_null()) {
    res_datum.set_null();
  } else if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
    ret = ObDatumHexUtils::unhex(expr, param->get_string(), ctx, res_datum, has_set_res);
  } else { // text tc
    ObString str;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *param,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), str))) {
      LOG_WARN("failed to get real string data", K(ret));
    } else {
      ret = ObDatumHexUtils::unhex(expr, str, ctx, res_datum, has_set_res);
    }
  }
  if (OB_FAIL(ret)) {
    // when ret is OB_ERR_INVALID_HEX_NUMBER and sql_mode is not strict return null
    if(OB_ERR_INVALID_HEX_NUMBER == ret) {
      ObCastMode default_cast_mode = CM_NONE;
      const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
      ObSQLMode sql_mode = 0;
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
        LOG_WARN("get sql mode failed", K(ret));
      } else if (FALSE_IT(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                            session->is_ignore_stmt(),
                                                            sql_mode,
                                                            default_cast_mode))) {
        LOG_WARN("failed to get default cast mode", K(ret));
      } else if (CM_IS_WARN_ON_FAIL(default_cast_mode)) {
        ret = OB_SUCCESS;
        res_datum.set_null();
      } else {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("fail to eval unhex", K(ret), K(expr), K(*param));
      }
    } else {
      //ret is other error code
      LOG_WARN("fail to eval unhex", K(ret), K(expr), K(*param));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprUnhex, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
  }
  return ret;
}

}
}
