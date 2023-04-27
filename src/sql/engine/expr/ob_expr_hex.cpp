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
#include <string.h>
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_hex.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprHex::ObExprHex(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_HEX, N_HEX, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprHex::~ObExprHex()
{
}

int ObExprHex::calc_result_type1(ObExprResType &type,
                                 ObExprResType &text,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  common::ObObjType param_type = text.get_type();
  const int32_t mbmaxlen = 4;
  if (ObTextType == param_type
      || ObMediumTextType == param_type
      || ObLongTextType == param_type) {
    type.set_type(ObLongTextType);
    type.set_length(OB_MAX_LONGTEXT_LENGTH / mbmaxlen);
  } else if (ObTinyTextType ==param_type) {
    type.set_type(ObTextType);
    type.set_length(OB_MAX_TEXT_LENGTH / mbmaxlen);
  } else {
    type.set_varchar();
  }
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));

  //calc length now...
  common::ObLength length = -1;
  if (ob_is_string_type(param_type) || ob_is_enum_or_set_type(param_type)) {
    length = 2 * text.get_length() + 1;
  } else if (ob_is_numeric_type(param_type) ||
             ob_is_temporal_type(param_type)) {
    length = 300;//enough !
  }
  if (type.is_varchar()) {
    if (length <= 0 || length > common::OB_MAX_VARCHAR_LENGTH) {
      length = common::OB_MAX_VARCHAR_LENGTH;
    }
    type.set_length(length);
  }

  if (ob_is_enum_or_set_type(param_type)) {
    text.set_calc_type(common::ObVarcharType);
  }

  if (OB_SUCC(ret)) {
    if (text.is_number()) {
      // accept number
    } else if (text.get_type() == ObYearType || text.is_numeric_type()) {
      text.set_calc_type(ObUInt64Type);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
    } else {
      if (!ob_is_text_tc(param_type)) {
        text.set_calc_type(ObVarcharType);
      }
      ObExprResType tmp_type;
      OZ(aggregate_charsets_for_string_result(tmp_type, &text, 1, type_ctx.get_coll_type()));
      if (OB_SUCC(ret)) {
        text.set_calc_collation_type(tmp_type.get_collation_type());
        text.set_calc_collation_level(tmp_type.get_collation_level());
      }
    }
  }

  return ret;
}
int ObExprHex::calc(ObObj &result,
                    const ObObj &text,
                    ObCastCtx &cast_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (text.get_type() == ObYearType
             || text.is_numeric_type()) {
    uint64_t uint_val = 0;
    if (OB_FAIL(ObExprHex::get_uint64(text, cast_ctx, uint_val))) {
      LOG_WARN("fail to get uint64", K(ret), K(text));
    } else if (OB_FAIL(ObHexUtils::hex_for_mysql(uint_val, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(uint_val));
    } else {}
  } else {
    ObString str;
    EXPR_GET_VARCHAR_V2(text, str);
    if (OB_FAIL(ret)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid input format. need varchar.", K(ret), K(text));
    } else if (OB_FAIL(ObHexUtils::hex(str, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(str));
    } else {}
  }
  return ret;
}

int ObExprHex::get_uint64(const ObObj &obj, ObCastCtx &cast_ctx, uint64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  if (OB_UNLIKELY(obj.is_number())) {
    if (OB_FAIL(number_uint64(obj.get_number(), out))) {
      LOG_WARN("number to uint fail", K(ret));
    }
  } else {
    EXPR_GET_UINT64_V2(obj, out);
  }
  return ret;
}

int ObExprHex::number_uint64(const number::ObNumber &num_val, uint64_t &out)
{
  int ret = OB_SUCCESS;
  int64_t tmp_int = 0;
  uint64_t tmp_uint = 0;
  number::ObNumber nmb;
  ObNumStackOnceAlloc alloc;
  if (OB_FAIL(nmb.from(num_val, alloc))) {
    LOG_WARN("deep copy failed", K(ret));
  } else if (OB_UNLIKELY(!nmb.is_integer() && OB_FAIL(nmb.round(0)))) {
    LOG_WARN("round failed", K(ret), K(nmb));
  } else if (nmb.is_valid_int64(tmp_int)) {
    out = static_cast<uint64_t>(tmp_int);
  } else if (nmb.is_valid_uint64(tmp_uint)) {
    out = tmp_uint;
  } else {
    out = UINT64_MAX;
  }
  return ret;
}

int ObExprHex::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprHex::eval_hex;
  return ret;
}

int ObExprHex::eval_hex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else {
    const ObObjType in_type = expr.args_[0]->datum_meta_.type_;
    if (ObUInt64Type == in_type || ObNumberType == in_type) {
      uint64_t val = 0;
      if (ObNumberType == in_type) {
        OZ(number_uint64(number::ObNumber(arg->get_number()), val));
      } else {
        val = arg->get_uint();
      }
      const int64_t max_len = sizeof(uint64_t) * 2 + 1;
      char *buf = expr.get_str_res_mem(ctx, max_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("get memory failed", K(ret));
      } else {
        const int len = snprintf(buf, max_len, "%lX", val);
        if (len < 0 || len > max_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snprintf failed", K(ret), K(len));
        } else {
          expr_datum.set_string(buf, len);
        }
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      if (!ob_is_text_tc(in_type)) {
        if (OB_FAIL(ObDatumHexUtils::hex(expr, arg->get_string(), ctx,
                                         alloc_guard.get_allocator(), expr_datum))) {
          LOG_WARN("to hex failed", K(ret));
        }
      } else { // text tc
        ObString str;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc_guard.get_allocator(),
                                                              *arg,
                                                              expr.args_[0]->datum_meta_,
                                                              expr.args_[0]->obj_meta_.has_lob_header(),
                                                              str))) {
          LOG_WARN("failed to get lob data", K(ret));
        } else if (OB_FAIL(ObDatumHexUtils::hex(expr, str, ctx,
                                                alloc_guard.get_allocator(), expr_datum))) {
          LOG_WARN("to hex failed", K(ret));
        }
      }
    }
  }
  return ret;
}
}
}
