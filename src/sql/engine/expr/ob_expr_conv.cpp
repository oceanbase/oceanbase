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
#include "ob_expr_conv.h"

//#include <m_string.h>
//#include "lib/regex/include/m_string.h"

#include "lib/oblog/ob_log.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

ObExprConv::ObExprConv(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_OP_CONV, N_CONV, 3, VALID_FOR_GENERATED_COL)
{
}

ObExprConv::~ObExprConv()
{
  // TODO Auto-generated destructor stub
}


/*
 * ObExprConv(str/int, base_from,
 * base_to)的语义是将第一个参数根据base_from的进制转换为base_to的进制，并以字符串的形式输出
 *
 * 框架无法对第一个参数进行转换，但可以对第二第三个参数进行转换
 */
inline int ObExprConv::calc_result_type3(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         ObExprResType &type3,
                                         common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(type_ctx.get_session()));
  if (OB_SUCC(ret)) {
    type.set_varchar();
    type.set_collation_type(get_default_collation_type(type.get_type(), type_ctx));
    type.set_collation_level(CS_LEVEL_COERCIBLE);
    type.set_length(64);
  }

  // Check ObUInt64Type to prevent repeated implicit cast
  if (type1.get_type() == ObBitType) {
    type1.set_calc_type(ObBitType);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
  } else {
    type1.set_calc_type_default_varchar();
  }
  type2.set_calc_type(ObInt32Type);
  type3.set_calc_type(ObInt32Type); // 可以是有符号数，表示type1是有符号数

  return ret;
}

int ObExprConv::cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprConv::eval_conv;

  return ret;
}

int ObExprConv::eval_conv(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_0 = NULL;
  ObDatum *param_1 = NULL;
  ObDatum *param_2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_0))
      || OB_FAIL(expr.args_[1]->eval(ctx, param_1))
      || OB_FAIL(expr.args_[2]->eval(ctx, param_2))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (param_0->is_null() || param_1->is_null() || param_2->is_null()) {
    res_datum.set_null();
  } else {
    int32_t from_base = param_1->get_int32();
    int32_t to_base = param_2->get_int32();
    int64_t dec = 0;
    if (INT32_MIN == from_base || INT32_MIN == to_base
        || abs(from_base) < MIN_BASE || abs(from_base) > MAX_BASE
        || abs(to_base) < MIN_BASE || abs(to_base) > MAX_BASE) {
      res_datum.set_null();
    } else {
      char *res_ptr = expr.get_str_res_mem(ctx, MAX_LENGTH);
      if (NULL != res_ptr) {
        if (ObBitType == expr.args_[0]->datum_meta_.type_) {
          dec = param_0->get_uint();
        } else {
          ObString str = param_0->get_string();
          int err = 0; // though err != 0 , we still use dec as the valid argument of next step.
          if (from_base < 0) {
            dec = ObCharset::strntoll(str.ptr(), str.length(), -from_base, &err);
          } else {
            dec = static_cast<int64_t>(ObCharset::strntoull(str.ptr(),
                                                            str.length(),
                                                            from_base,
                                                            &err));
          }
        }
        char *ptr = NULL;
        if (OB_ISNULL(ptr = ObCharset::lltostr(dec, res_ptr, to_base, 1))) {
          res_datum.set_null();
        } else {
          res_datum.set_string(res_ptr, static_cast<int64_t>(ptr - res_ptr));
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprConv, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
