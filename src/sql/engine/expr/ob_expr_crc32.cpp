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
#include "lib/compress/zlib/zlib.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_name_def.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_crc32.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;

namespace sql {
ObExprCrc32::ObExprCrc32(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CRC32, N_CRC32, 1, NOT_ROW_DIMENSION)
{}

int ObExprCrc32::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_precision(10);
  type.set_uint32();
  if (OB_LIKELY(type1.is_not_null())) {
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }

  if (ob_is_string_type(type1.get_type())) {
    type1.set_calc_type(type1.get_type());
    type1.set_calc_collation_type(type1.get_collation_type());
  } else {
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}

int ObExprCrc32::calc_result1(common::ObObj& obj, const common::ObObj& obj1, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj1.is_null()) {
    obj.set_null();
  } else {
    uint64_t val;
    ObString str_val = obj1.get_string();

    if (str_val.length() <= 0) {
      val = 0ULL;
    } else {
      val = crc32(0, reinterpret_cast<unsigned char*>(str_val.ptr()), str_val.length());
    }
    obj.set_uint32(val);
  }
  return ret;
}

int ObExprCrc32::calc_crc32_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  unsigned char* buf = NULL;
  ObDatum* s_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (s_datum->is_null()) {
    res_datum.set_null();
  } else {
    const ObString& str_val = s_datum->get_string();
    if (str_val.empty()) {
      res_datum.set_uint(0ULL);
    } else {
      buf = reinterpret_cast<unsigned char*>(const_cast<char*>(str_val.ptr()));
      res_datum.set_uint(crc32(0, buf, str_val.length()));
    }
  }
  return ret;
}

int ObExprCrc32::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(1 == rt_expr.arg_cnt_);
  CK(!OB_ISNULL(rt_expr.args_) && !OB_ISNULL(rt_expr.args_[0]));
  CK(ob_is_string_type(rt_expr.args_[0]->datum_meta_.type_));
  rt_expr.eval_func_ = calc_crc32_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase