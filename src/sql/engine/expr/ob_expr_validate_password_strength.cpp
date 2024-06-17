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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_validate_password_strength.h"

#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

const ObValidatePasswordFunc ObExprValidatePasswordStrength::validate_funcs_[STRENGTH_MAX] =
{
  NULL, // STRENGTH_WEAK is default strength, need no validation
  ObExprValidatePasswordStrength::validate_password_lessweak,
  ObExprValidatePasswordStrength::validate_password_low,
  ObExprValidatePasswordStrength::validate_password_medium,
  ObExprValidatePasswordStrength::validate_password_strong,
};

ObExprValidatePasswordStrength::ObExprValidatePasswordStrength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_VALIDATE_PASSWORD_STRENGTH,
                         N_VALIDATE_PASSWORD_STRENGTH, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprValidatePasswordStrength::~ObExprValidatePasswordStrength()
{
}

int ObExprValidatePasswordStrength::calc_result_type1(ObExprResType &type,
                                                      ObExprResType &type1,
                                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int32();
  type.set_scale(0);
  type.set_precision(10);
  type1.set_calc_type(common::ObVarcharType);
  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

int ObExprValidatePasswordStrength::cg_expr(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_password_strength;
  return ret;
}

int ObExprValidatePasswordStrength::eval_password_strength(const ObExpr &expr,
                                                           ObEvalCtx &ctx,
                                                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *pwd_param = NULL;
  ObCollationType c_src = ObCollationType::CS_TYPE_INVALID;
  ObString password_orig;
  ObString password;
  int strength = 0;

  const ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, pwd_param))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (pwd_param->is_null()) {
    res_datum.set_null();
  } else if (FALSE_IT(password_orig = pwd_param->get_string())) {
  } else if (FALSE_IT(c_src = expr.args_[0]->obj_meta_.get_collation_type())) {
  } else if (OB_FAIL(ObCharset::charset_convert(ctx.exec_ctx_.get_allocator(),
      password_orig, c_src, ObCharset::get_system_collation(), password))) {
    LOG_WARN("fail to convert password to sys collation", K(password_orig), K(c_src), K(ret));
  } else if (OB_FAIL(calc_password_strength(password, *session, strength))) {
    LOG_WARN("fail to calc password strength", K(password), K(ret));
  } else {
    res_datum.set_int(strength);
  }

  return ret;
}

int ObExprValidatePasswordStrength::calc_password_strength(const ObString &password,
                                                           const ObBasicSessionInfo &session,
                                                           int &strength)
{
  int ret = OB_SUCCESS;
  bool passed = true;
  size_t c_len = ObCharset::strlen_char(ObCharset::get_system_collation(), password.ptr(),
                                        static_cast<int64_t>(password.length()));
  strength = 0;
  for (int i = STRENGTH_LESSWEAK; OB_SUCC(ret) && passed && i < STRENGTH_MAX; ++i) {
    if (OB_FAIL(validate_funcs_[i](password, static_cast<int64_t>(c_len), session, passed))) {
      LOG_WARN("failed to validate", K(password), K(i), K(ret));
    } else if (passed) {
      strength = i * PASSWORD_STRENGTH_MULTIPLIER;
    }
  }
  return ret;
}

int ObExprValidatePasswordStrength::validate_password_lessweak(const ObString &password,
                                                               const int64_t password_char_length,
                                                               const ObBasicSessionInfo &session,
                                                               bool &passed)
{
  int ret = OB_SUCCESS;
  int64_t check_user_name = 0;
  passed = false;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME,
                                       check_user_name))) {
    LOG_WARN("fail to get validate_password_length", K(ret));
  } else {
    passed = password_char_length >= VALID_PASSWORD_LENGTH_MIN && (0 != check_user_name ||
             !ObCharset::case_insensitive_equal(password, session.get_user_name()));
  }
  return ret;
}

int ObExprValidatePasswordStrength::validate_password_low(const ObString &password,
                                                          const int64_t password_char_length,
                                                          const ObBasicSessionInfo &session,
                                                          bool &passed)
{
  int ret = OB_SUCCESS;
  uint64_t valid_pw_len = 0;
  UNUSED(password);
  passed = false;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_LENGTH, valid_pw_len))) {
    LOG_WARN("fail to get validate_password_length", K(ret));
  } else {
    passed = password_char_length >= valid_pw_len;
  }
  return ret;
}

int ObExprValidatePasswordStrength::validate_password_medium(const ObString &password,
                                                             const int64_t password_char_length,
                                                             const ObBasicSessionInfo &session,
                                                             bool &passed)
{
  int ret = OB_SUCCESS;
  uint64_t valid_mix_case_count = 0;
  uint64_t valid_number_count = 0;
  uint64_t valid_special_count = 0;
  uint64_t lower_count = 0;
  uint64_t upper_count = 0;
  uint64_t digit_count = 0;
  uint64_t special_count = 0;
  UNUSED(password_char_length);
  passed = false;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT,
                                       valid_mix_case_count))) {
    LOG_WARN("fail to get validate_password_mixed_case_count", K(ret));
  } else if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT,
                                              valid_number_count))) {
    LOG_WARN("fail to get validate_password_number_count", K(ret));
  } else if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT,
                                              valid_special_count))) {
    LOG_WARN("fail to get validate_password_special_char_count", K(ret));
  } else {
    auto handle_char_func = [&lower_count, &upper_count, &digit_count, &special_count]
                            (ObString, int wchar) -> int {
      int ret = OB_SUCCESS;
      if (!ob_isascii(wchar)) {
        special_count++;
      } else if (islower(wchar)) {
        lower_count++;
      } else if (isupper(wchar)) {
        upper_count++;
      } else if (isdigit(wchar)) {
        digit_count++;
      } else {
        special_count++;
      }
      return ret;
    };
    OZ(ObCharsetUtils::foreach_char(password, ObCharset::get_system_collation(), handle_char_func));
  }
  if (OB_SUCC(ret)) {
    passed = lower_count >= valid_mix_case_count && upper_count >= valid_mix_case_count &&
             digit_count >= valid_number_count && special_count >= valid_special_count;
  }
  return ret;
}

int ObExprValidatePasswordStrength::validate_password_strong(const ObString &password,
                                                             const int64_t password_char_length,
                                                             const ObBasicSessionInfo &session,
                                                             bool &passed)
{
  int ret = OB_SUCCESS;
  UNUSED(password);
  UNUSED(password_char_length);
  UNUSED(session);
  // Check if the password matches the words in the dictionary file. The dictionary file is not 
  // supported in OB yet, so this validation always passed
  passed = true;
  return ret;
}

} //namespace sql
} //namespace oceanbase
