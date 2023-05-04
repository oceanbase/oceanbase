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
#include "sql/engine/expr/ob_expr_randstr.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprRandstr::ObExprRandstr(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_RANDSTR, "randstr", 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRandstr::~ObExprRandstr()
{
}

int ObExprRandstr::calc_result_type2(ObExprResType &type,
                                     ObExprResType &len,
                                     ObExprResType &seed,
                                     common::ObExprTypeCtx &type_ctx) const
{
	UNUSED(type_ctx);
	int ret = OB_SUCCESS;
	len.set_calc_type(ObIntType);
  seed.set_calc_type(ObIntType);
  if (lib::is_mysql_mode()) {
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(
                common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()), mbmaxlen))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "fail to get mbmaxlen");
    } else if (0 == mbmaxlen){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mb maxlen invalid");
    } else {
      type.set_type(ObVarcharType);
      // must divide by mbmaxlen,
      // otherwise create table as select would fail with randstr() function
      type.set_length(OB_MAX_VARCHAR_LENGTH / mbmaxlen);
    }
  } else {
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length_semantics(LS_CHAR);
    type.set_type(ObVarcharType);
    type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
  }
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
	return ret;
}

int ObExprRandstr::calc_random_str(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int64_t max_size = 0;
  if (OB_UNLIKELY(2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_size))) {
    LOG_WARN("get max length failed", K(ret));
  } else {
    int64_t rand_res = 0;
    ObDatum &len = expr.locate_param_datum(ctx, 0);
    ObDatum &seed = expr.locate_param_datum(ctx, 1);
    if (len.is_null() || seed.is_null()) {
      res_datum.set_null();
    } else if (OB_UNLIKELY(len.get_int() < 0)) {
      res_datum.set_null();
    } else if (OB_UNLIKELY(len.get_int() > max_size)) {
      res_datum.set_null();
      LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "randstr", static_cast<int>(max_size));
    } else {
      ObString output;
      ObExprStrResAlloc expr_res_alloc(expr, ctx);
      int64_t len_val = len.get_int();
      int64_t seed_val = seed.get_int();
      char *buf = static_cast<char *>(expr_res_alloc.alloc(len_val));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail alloc memory", K(ret), K(len_val));
      } else {
        static const char dict[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int64_t i = 0; i < len_val && OB_SUCC(ret); ++i) {
          // LCG, Linear Congruential Method for Generating Pseudo Random Number
          seed_val = (seed_val * 1103515245 + 12345) & ((1U << 31) - 1);
          buf[i] = dict[seed_val % (sizeof(dict) - 1)];
        }
        output.assign_ptr(buf, static_cast<int32_t>(len_val));
        res_datum.set_string(output);
      }
    }
  }
  return ret;
}

int ObExprRandstr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRandstr::calc_random_str;
  return ret;
}
} /* namespace sql */
} /* namespace oceanbase */
