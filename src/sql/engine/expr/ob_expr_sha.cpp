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
#include "ob_expr_sha.h"

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_encryption_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

ObExprSha::ObExprSha(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SHA, N_SHA, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprSha::~ObExprSha()
{
}

int ObExprSha::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx.get_coll_type()));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));

  OZ (ObHashUtil::get_hash_output_len(OB_HASH_SH1, length));
  OX (type.set_length(length * 2));
  return ret;
}

int ObExprSha::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprSha::eval_sha;
  return ret;
}

int ObExprSha::eval_sha(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    ObString text = arg->get_string();
    ObString sha_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(ObHashUtil::hash(OB_HASH_SH1, text, alloc_guard.get_allocator(), sha_str))) {
      LOG_WARN("fail to calc sha", K(text), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                            expr_datum, false))) {
      LOG_WARN("fail to conver sha_str to hex", K(sha_str), K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSha, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

ObExprSha2::ObExprSha2(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SHA2, N_SHA2, 2, VALID_FOR_GENERATED_COL)
{
}

ObExprSha2::~ObExprSha2()
{
}

int ObExprSha2::calc_result_type2(ObExprResType &type,
                                 ObExprResType &type1,
                                 ObExprResType &type2,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  int64_t length = 0;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx.get_coll_type()));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));
  OX (type2.set_calc_type(ObIntType));
  OZ (ObHashUtil::get_hash_output_len(OB_HASH_SH512, length));
  OX (type.set_length(length * 2));
  return ret;
}

int ObExprSha2::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprSha2::eval_sha2;
  return ret;
}

int ObExprSha2::eval_sha2(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg0->is_null() || arg1->is_null()) {
    expr_datum.set_null();
  } else {
    ObString text = arg0->get_string();
    int64_t sha_bit_len = arg1->get_int();
    ObHashAlgorithm algo = OB_HASH_INVALID;
    ObString sha_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (0 == sha_bit_len) {
      sha_bit_len = 256;
    }
    if (OB_FAIL(ObHashUtil::get_sha_hash_algorightm(sha_bit_len, algo))) {
      ret = OB_SUCCESS;
      expr_datum.set_null();
      LOG_WARN("fail to get hash algorithm", K(sha_bit_len), K(ret));
    } else if (OB_FAIL(ObHashUtil::hash(algo, text, alloc_guard.get_allocator(), sha_str))) {
      LOG_WARN("fail to calc sha", K(text), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, sha_str, ctx, alloc_guard.get_allocator(),
                                            expr_datum, false))) {
      LOG_WARN("fail to convert sha_str to hex", K(sha_str), K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSha2, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
