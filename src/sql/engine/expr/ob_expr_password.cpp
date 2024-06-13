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
#include "sql/engine/expr/ob_expr_password.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/encrypt/ob_encrypted_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprPassword::ObExprPassword(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_PASSWORD, N_PASSWORD, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprPassword::~ObExprPassword()
{
}

# define SHA_PASSWORD_CHAR_LENGTH (SHA_DIGEST_LENGTH * 2 + 2)

int ObExprPassword::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  type.set_type(ObVarcharType);
  type.set_length(SHA_PASSWORD_CHAR_LENGTH);
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(CS_LEVEL_COERCIBLE);

  text.set_calc_type(ObVarcharType);
  ObExprResType tmp_type;
  OZ(ObExprOperator::aggregate_charsets_for_string_result(tmp_type, &text, 1,
                                                          type_ctx.get_coll_type()));
  OX(text.set_calc_collation_type(tmp_type.get_collation_type()));
  OX(text.set_calc_collation_level(tmp_type.get_collation_level()));
  return ret;
}

int ObExprPassword::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_password;
  return ret;
}

int ObExprPassword::eval_password(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_ERROR("evaluate parameter value failed", K(ret), K(arg));
  } else if (arg->is_null()) {
    expr_datum.set_string("", 0);
  } else if (arg->get_string().empty()) {
    expr_datum.set_string("", 0);
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    char *enc_buf = static_cast<char *>(alloc_guard.get_allocator().alloc(SHA_PASSWORD_CHAR_LENGTH));
    if (enc_buf == NULL) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      ObString tmp_str;
      ObString upp_str;
      ObString res_str;
      ObString str_in_ctx;
      tmp_str.assign_ptr(enc_buf, SHA_PASSWORD_CHAR_LENGTH);
      if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(arg->get_string(), tmp_str))) {
        LOG_WARN("encrypt password failed", K(ret));
      } else if (OB_FAIL(ObCharset::toupper(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI,
                                            tmp_str,
                                            upp_str,
                                            alloc_guard.get_allocator()))) {
        LOG_WARN("convert string to upper failed", K(ret), K(tmp_str));
      } else if (OB_FAIL(ObExprUtil::convert_string_collation(upp_str,
                                                              ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI,
                                                              res_str,
                                                              expr.datum_meta_.cs_type_,
                                                              alloc_guard.get_allocator()))) {
        LOG_WARN("convert string collation failed", K(ret), K(upp_str));
      } else if (OB_FAIL(ObExprUtil::deep_copy_str(res_str, str_in_ctx, ctx.get_expr_res_alloc()))) {
        LOG_WARN("failed to cpoy str to context", K(ret));
      } else {
        expr_datum.set_string(str_in_ctx);
        LOG_USER_WARN(OB_ERR_DEPRECATED_SYNTAX_NO_REP, "\'PASSWORD\'");
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprPassword, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_SERVER);
  }
  return ret;
}

}
}
