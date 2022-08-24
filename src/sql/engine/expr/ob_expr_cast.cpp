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
#include "share/object/ob_obj_cast.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

namespace oceanbase {
using namespace common;
namespace sql {

ObExprCast::ObExprCast(ObIAllocator& alloc)
    : ObFuncExprOperator::ObFuncExprOperator(alloc, T_FUN_SYS_CAST, N_CAST, 2, NOT_ROW_DIMENSION)
{
  extra_serialize_ = 0;
  disable_operand_auto_cast();
}

int ObExprCast::get_cast_inttc_len(ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx,
    int32_t& res_len, int16_t& length_semantics, ObCollationType conn) const
{
  int ret = OB_SUCCESS;
  if (type1.is_literal()) {  // literal
    if (ObStringTC == type1.get_type_class()) {
      res_len = type1.get_accuracy().get_length();
      length_semantics = type1.get_length_semantics();
    } else if (OB_FAIL(ObField::get_field_mb_length(
                   type1.get_type(), type1.get_accuracy(), type1.get_collation_type(), res_len))) {
      LOG_WARN("failed to get filed mb length");
    }
  } else {
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    ObObjTypeClass tc1 = type1.get_type_class();
    int16_t scale = type1.get_accuracy().get_scale();
    if (ObDoubleTC == tc1) {
      res_len -= 1;
    } else if (ObDateTimeTC == tc1 && scale > 0) {
      res_len += scale - 1;
    } else if (OB_FAIL(get_cast_string_len(type1, type2, type_ctx, res_len, length_semantics, conn))) {
      LOG_WARN("fail to get cast string length", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// @res_len: the result length of any type be cast to string
// for column type
// such as c1(int),  cast(c1 as binary), the result length is 11.
int ObExprCast::get_cast_string_len(ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx,
    int32_t& res_len, int16_t& length_semantics, ObCollationType conn) const
{
  int ret = OB_SUCCESS;
  const ObObj& val = type1.get_param();
  if (!type1.is_literal()) {  // column
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    int16_t prec = type1.get_accuracy().get_precision();
    int16_t scale = type1.get_accuracy().get_scale();
    switch (type1.get_type()) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        int32_t prec = static_cast<int32_t>(type1.get_accuracy().get_precision());
        res_len = prec > res_len ? prec : res_len;
        break;
      }
      case ObNumberType:
      case ObUNumberType: {
        if (lib::is_oracle_mode()) {
          if (0 < prec) {
            if (0 < scale) {
              res_len = prec + 2;
            } else if (0 == scale) {
              res_len = prec + 1;
            } else {
              res_len = prec - scale;
            }
          }
        } else {
          if (0 < prec) {
            if (0 < scale) {
              res_len = prec + 2;
            } else {
              res_len = prec + 1;
            }
          }
        }
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
      case ObDateTimeType:
      case ObTimestampType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
      case ObTimeType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
      // TODO text share with varchar temporarily
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObVarcharType:
      case ObCharType:
      case ObHexStringType:
      case ObRawType:
      case ObNVarchar2Type:
      case ObNCharType:
      case ObEnumType:
      case ObSetType:
      case ObEnumInnerType:
      case ObSetInnerType:
      case ObURowIDType:
      case ObLobType:
      case ObJsonType: {
        res_len = type1.get_length();
        length_semantics = type1.get_length_semantics();
        break;
      }
      case ObBitType: {
        if (scale > 0) {
          res_len = scale;
        }
        res_len = (res_len + 7) / 8;
        break;
      }
      default: {
        break;
      }
    }
  } else if (type1.is_null()) {
    res_len = 0;  // compatible with mysql;
  } else if (OB_ISNULL(type_ctx.get_session())) {
    // calc type don't set ret, just print the log. by design.
    LOG_WARN("my_session is null");
  } else {  // literal
    ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
    ObCastMode cast_mode = CM_NONE;
    ObCollationType cast_coll_type =
        (CS_TYPE_INVALID != type2.get_collation_type()) ? type2.get_collation_type() : conn;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
    ObCastCtx cast_ctx(&oballocator, &dtc_params, 0, cast_mode, cast_coll_type);
    ObString val_str;
    EXPR_GET_VARCHAR_V2(val, val_str);
    // Note: use character size not byte
    if (OB_SUCC(ret) && NULL != val_str.ptr()) {
      int32_t len_byte = val_str.length();
      res_len = len_byte;
      length_semantics = LS_CHAR;
      if (NULL != val_str.ptr()) {
        int32_t trunc_len_byte =
            static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cast_coll_type, val_str.ptr(), len_byte));
        res_len = static_cast<int32_t>(ObCharset::strlen_char(cast_coll_type, val_str.ptr(), trunc_len_byte));
      }
      if (type1.is_numeric_type() && !type1.is_integer_type()) {
        res_len += 1;
      }
    }
  }
  return ret;
}

// this is only for engine 3.0. old engine will get cast mode from expr_ctx.
// only for explicit cast, implicit cast's cm is setup while deduce type(in type_ctx.cast_mode_)
int ObExprCast::get_explicit_cast_cm(const ObExprResType& src_type, const ObExprResType& dst_type,
    const ObSQLSessionInfo& session, const ObRawExpr& cast_raw_expr, ObCastMode& cast_mode) const
{
  int ret = OB_SUCCESS;
  cast_mode = CM_NONE;
  const bool is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_raw_expr.get_extra());
  const int32_t result_flag = src_type.get_result_flag();
  if (OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag, &session, cast_mode))) {
    LOG_WARN("get_default_cast_mode failed", K(ret));
  } else {
    const ObObjTypeClass dst_tc = ob_obj_type_class(dst_type.get_type());
    const ObObjTypeClass src_tc = ob_obj_type_class(src_type.get_type());
    if (ObDateTimeTC == dst_tc || ObDateTC == dst_tc || ObTimeTC == dst_tc) {
      cast_mode |= CM_NULL_ON_WARN;
    } else if (ob_is_int_uint(src_tc, dst_tc)) {
      cast_mode |= CM_NO_RANGE_CHECK;
    }
    if (!is_oracle_mode() && CM_IS_EXPLICIT_CAST(cast_mode)) {
      // CM_STRING_INTEGER_TRUNC is only for string to int cast in mysql mode
      if (ob_is_string_type(src_type.get_type()) &&
          (ob_is_int_tc(dst_type.get_type()) || ob_is_uint_tc(dst_type.get_type()))) {
        cast_mode |= CM_STRING_INTEGER_TRUNC;
      }
      // select cast('1e500' as decimal);  -> max_val
      // select cast('-1e500' as decimal); -> min_val
      if (ob_is_string_type(src_type.get_type()) && ob_is_number_tc(dst_type.get_type())) {
        cast_mode |= CM_SET_MIN_IF_OVERFLOW;
      }
    }
  }
  return ret;
}

int ObExprCast::check_target_type_precision_valid(const ObExprResType& type, const ObCastMode cast_mode) const
{
  int ret = OB_SUCCESS;
  if (ob_is_interval_tc(type.get_type()) && CM_IS_EXPLICIT_CAST(cast_mode)) {
    ObPrecision precision = type.get_precision();
    ObScale scale = type.get_scale();
    if (!ObIntervalScaleUtil::scale_check(precision) || !ObIntervalScaleUtil::scale_check(scale)) {
      ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      LOG_WARN("target interval type precision out of range", K(ret), K(type));
    }
  }
  return ret;
}

int ObExprCast::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType dst_type;
  ObRawExpr* cast_raw_expr = NULL;
  const sql::ObSQLSessionInfo* session = NULL;
  bool is_explicit_cast = false;
  if (OB_ISNULL(session = type_ctx.get_session()) || OB_ISNULL(cast_raw_expr = get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", K(ret), KP(session), KP(cast_raw_expr));
  } else if (OB_UNLIKELY(NOT_ROW_DIMENSION != row_dimension_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid row_dimension_", K(row_dimension_), K(ret));
  } else if (OB_FAIL(get_cast_type(type2, dst_type))) {
    LOG_WARN("get cast dest type failed", K(ret));
  } else if (OB_FAIL(check_target_type_precision_valid(dst_type, cast_raw_expr->get_extra()))) {
    LOG_WARN("check target type precision valid failed", K(ret), K(dst_type));
  } else if (OB_UNLIKELY(!cast_supported(
                 type1.get_type(), type1.get_collation_type(), dst_type.get_type(), dst_type.get_collation_type()))) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("transition does not support",
        "src",
        ob_obj_type_str(type1.get_type()),
        "dst",
        ob_obj_type_str(dst_type.get_type()));
  } else if (FALSE_IT(is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_raw_expr->get_extra()))) {
  } else if (is_explicit_cast && lib::is_oracle_mode() &&
             (ob_is_text_tc(dst_type.get_type()) || ob_is_lob_locator(dst_type.get_type()))) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("explicit cast to lob type not allowed", K(ret), K(dst_type));
  } else {
    // always cast to user requested type
    if (is_explicit_cast && !lib::is_oracle_mode() && ObCharType == dst_type.get_type()) {
      // cast(x as binary(10)), in parser,binary->T_CHAR+bianry, but, result type should be varchar, so set it.
      type.set_type(ObVarcharType);
    } else if (lib::is_mysql_mode() && ObFloatType == dst_type.get_type()) {
      // Compatible with mysql. If the precision p is not specified, produces a result of type FLOAT. 
      // If p is provided and 0 <=  p <= 24, the result is of type FLOAT. If 25 <= p <= 53, 
      // the result is of type DOUBLE. If p < 0 or p > 53, an error is returned
      // however, ob use -1 as default precision, so it is a valid value
      type.set_collation_type(dst_type.get_collation_type());
      ObPrecision float_precision = dst_type.get_precision();
      if (float_precision < -1 || float_precision > OB_MAX_DOUBLE_FLOAT_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, float_precision, "CAST", OB_MAX_DOUBLE_FLOAT_PRECISION);
      } else if (float_precision <= OB_MAX_FLOAT_PRECISION) {
        type.set_type(ObFloatType);
      } else {
        type.set_type(ObDoubleType);
      }
    } 
    else {
      type.set_type(dst_type.get_type());
      type.set_collation_type(dst_type.get_collation_type());
    }
    int16_t scale = dst_type.get_scale();
    if (is_explicit_cast && !lib::is_oracle_mode() &&
        (ObTimeType == dst_type.get_type() || ObDateTimeType == dst_type.get_type()) && scale > 6) {
      ret = OB_ERR_TOO_BIG_PRECISION;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", OB_MAX_DATETIME_PRECISION);
    }
    if (OB_SUCC(ret)) {
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      ObCollationType collation_connection = type_ctx.get_coll_type();
      ObCollationType collation_nation = session->get_nls_collation_nation();
      type1.set_calc_type(get_calc_cast_type(type1.get_type(), dst_type.get_type()));
      int32_t length = 0;
      if (ob_is_string_type(dst_type.get_type()) || ob_is_raw(dst_type.get_type()) || ob_is_json(dst_type.get_type())) {
        type.set_collation_level(is_explicit_cast ? CS_LEVEL_IMPLICIT : type1.get_collation_level());
        int32_t len = dst_type.get_length();
        int16_t length_semantics = 
            ((dst_type.is_string_or_lob_locator_type() || dst_type.is_json())
                ? dst_type.get_length_semantics()
                : (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                                           : LS_BYTE));
        if (len > 0) {  // cast(1 as char(10))
          type.set_full_length(len, length_semantics);
        } else if (OB_FAIL(get_cast_string_len(type1,
                       dst_type,
                       type_ctx,
                       len,
                       length_semantics,
                       collation_connection))) {  // cast (1 as char)
          LOG_WARN("fail to get cast string length", K(ret));
        } else {
          type.set_full_length(len, length_semantics);
        }
        if (CS_TYPE_INVALID != dst_type.get_collation_type()) {
          // cast as binary
          type.set_collation_type(dst_type.get_collation_type());
        } else {
          // use collation of current session
          type.set_collation_type(ob_is_nstring_type(dst_type.get_type()) ? collation_nation : collation_connection);
        }
      } else if (ob_is_extend(dst_type.get_type())) {
        type.set_udt_id(type2.get_udt_id());
      } else {
        type.set_length(length);
        if (ObNumberTC == dst_type.get_type_class() && 0 == dst_type.get_precision()) {
          // MySql:cast (1 as decimal(0)) = cast(1 as decimal)
          // Oracle: cast(1.4 as number) = cast(1.4 as number(-1, -1))
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObNumberType].get_precision());
        } else if (ObIntTC == dst_type.get_type_class() || ObUIntTC == dst_type.get_type_class()) {
          // for int or uint , the precision = len
          int32_t len = 0;
          int16_t length_semantics = LS_BYTE;  // unused
          if (OB_FAIL(get_cast_inttc_len(type1, dst_type, type_ctx, len, length_semantics, collation_connection))) {
            LOG_WARN("fail to get cast inttc length", K(ret));
          } else {
            len = len > OB_LITERAL_MAX_INT_LEN ? OB_LITERAL_MAX_INT_LEN : len;
            type.set_precision(static_cast<int16_t>(len));
          }
        } else if (ORACLE_MODE == compatibility_mode && ObDoubleType == dst_type.get_type()) {
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][dst_type.get_type()];
          type.set_accuracy(acc);
          type1.set_accuracy(acc);
        } else {
          type.set_precision(dst_type.get_precision());
        }
        type.set_scale(dst_type.get_scale());
      }
    }
    CK(OB_NOT_NULL(type_ctx.get_session()));
    if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
      // interval expr need OB_MYSQL_NOT_NULL_FLAG
      calc_result_flag2(type, type1, type2);
    }
  }
  if (OB_SUCC(ret) && session->use_static_typing_engine()) {
    ObCastMode explicit_cast_cm = CM_NONE;
    if (OB_FAIL(get_explicit_cast_cm(type1, dst_type, *session, *cast_raw_expr, explicit_cast_cm))) {
      LOG_WARN("set cast mode failed", K(ret));
    } else if (CM_IS_EXPLICIT_CAST(explicit_cast_cm)) {
      // cast_raw_expr.extra_ store explicit cast's cast mode
      cast_raw_expr->set_extra(explicit_cast_cm);
      // type_ctx.cast_mode_ sotre implicit cast's cast mode.
      // cannot use def cm, because it may change explicit cast behavior.
      // eg: select cast(18446744073709551615 as signed) -> -1
      //     because exprlicit case need CM_NO_RANGE_CHECK
      type_ctx.set_cast_mode(explicit_cast_cm & ~CM_EXPLICIT_CAST);
      type1.set_calc_accuracy(type.get_accuracy());

      // in engine 3.0, let implicit cast do the real cast
      bool need_extra_cast_for_src_type = false;
      bool need_extra_cast_for_dst_type = false;

      ObRawExprUtils::need_extra_cast(type1, type, need_extra_cast_for_src_type, need_extra_cast_for_dst_type);
      if (need_extra_cast_for_src_type) {
        ObExprResType src_type_utf8;
        OZ(ObRawExprUtils::setup_extra_cast_utf8_type(type1, src_type_utf8));
        OX(type1.set_calc_meta(src_type_utf8.get_obj_meta()));
      } else if (need_extra_cast_for_dst_type) {
        ObExprResType dst_type_utf8;
        OZ(ObRawExprUtils::setup_extra_cast_utf8_type(dst_type, dst_type_utf8));
        OX(type1.set_calc_meta(dst_type_utf8.get_obj_meta()));
      } else {
        bool need_warp = false;
        if (ob_is_enumset_tc(type1.get_type())) {
          // For enum/set type, need to check whether warp to string is required.
          if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(type1.get_type(), type1.get_calc_type(), false, need_warp))) {
            LOG_WARN("need_wrap_to_string failed", K(ret), K(type1));
          }
        } else if (OB_LIKELY(need_warp)) {
          // need_warp is true, no-op and keep type1's calc_type is dst_type. It will be wrapped
          // to string in ObRawExprWrapEnumSet::visit(ObSysFunRawExpr &expr) later.
        } else {
          // need_warp is false, set calc_type to type1 itself.
          type1.set_calc_meta(type1.get_obj_meta());
        }
      }
    } else {
      // no need to set cast mode, already setup while deduce type.
      //
      // implicit cast, no need to add cast again, but the ObRawExprWrapEnumSet depend on this
      // to add enum_to_str(), so we still set the calc type but skip add implicit cast in decuding.
      type1.set_calc_type(type.get_type());
      type1.set_calc_collation_type(type.get_collation_type());
      type1.set_calc_accuracy(type.get_accuracy());
    }
  }
  LOG_DEBUG("calc result type", K(type1), K(type2), K(type), K(dst_type));
  return ret;
}

int ObExprCast::get_cast_type(const ObExprResType param_type2, ObExprResType& dst_type) const
{
  int ret = OB_SUCCESS;
  if (!param_type2.is_int() && !param_type2.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type2));
  } else {
    const ObObj& param = param_type2.get_param();
    ParseNode parse_node;
    parse_node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    dst_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    dst_type.set_type(obj_type);
    if (ob_is_string_type(obj_type)) {
      // cast(x as char(10)) or cast(x as binary(10))
      dst_type.set_full_length(
          parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX], param_type2.get_accuracy().get_length_semantics());
    } else if (ob_is_raw(obj_type)) {
      dst_type.set_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
    } else if (ob_is_extend(obj_type)) {
      ret = OB_NOT_SUPPORTED;
    } else if (lib::is_mysql_mode() && ob_is_json(obj_type)) {
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
    }
    LOG_DEBUG("get_cast_type", K(dst_type), K(param_type2));
  }
  return ret;
}

int ObExprCast::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(obj2);
  int compatbility_mode = (expr_ctx.my_session_ != NULL && lib::is_oracle_mode()) ? 1 : 0;
  ObObjType src_type = obj1.get_type();
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObObj obj1_round;
  ParseNode node;
  node.value_ = obj2.get_int();
  ObObjType dest_type = static_cast<ObObjType>(node.int16_values_[0]);
  bool is_implicit_cast = (1 == extra_serialize_);
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.phy_plan_ctx_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(ObExtendType == src_type)) {
    ret = OB_NOT_SUPPORTED;
  } else if (is_implicit_cast) {
    if (OB_FAIL(do_implicit_cast(expr_ctx, CM_NONE, dest_type, obj1, result))) {
      // implicit cast just do type cast, no accuracy check, no padding or truncating.
      // need to refactor cast expr later.
      LOG_WARN("do_implicit_cast failed", K(ret));
    }
  } else if (OB_FAIL(expr_ctx.my_session_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, obj1, obj1_round))) {
    LOG_WARN("write obj failed", K(ret), K(obj1));
  } else {
    ObAccuracy accuracy;
    ObObjTypeClass src_tc = ob_obj_type_class(src_type);
    ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
    if (ObStringTC == dest_tc) {
      // parser will abort all negative number
      // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
      accuracy.set_full_length(node.int32_values_[1], result_type_.get_length_semantics(), share::is_oracle_mode());
    } else if (ObRawTC == dest_tc) {
      accuracy.set_length(node.int32_values_[1]);
    } else if (ObTextTC == dest_tc || ObLobTC == dest_tc || ObJsonTC == dest_tc) {
      // TODO texttc should use default length
      accuracy.set_length(
          node.int32_values_[1] < 0 ? ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length() : node.int32_values_[1]);
    } else if (ObIntervalTC == dest_tc) {
      if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                      !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      } else {
        ObScale scale =
            (dest_type == ObIntervalYMType)
                ? ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(node.int16_values_[3]))
                : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
                      static_cast<int8_t>(node.int16_values_[2]), static_cast<int8_t>(node.int16_values_[3]));
        accuracy.set_scale(scale);
      }
    } else {
      if (ObNumberType == dest_type && 0 == node.int16_values_[2]) {
        accuracy.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatbility_mode][ObNumberType].get_precision());
      } else {
        accuracy.set_precision(node.int16_values_[2]);
      }
      accuracy.set_scale(node.int16_values_[3]);
      if (compatbility_mode && ObDoubleType == dest_type) {
        accuracy.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatbility_mode][ObDoubleType].get_precision());
      }
    }
    LOG_DEBUG("get accuracy from result_type", K(src_type), K(dest_type), K(accuracy), K(result_type_));
    ObCastMode cast_mode = CM_EXPLICIT_CAST;
    if (share::is_mysql_mode() && ob_is_string_type(src_type) &&
        ((ob_is_int_tc(dest_type)) || ob_is_uint_tc(dest_type))) {
      cast_mode |= CM_STRING_INTEGER_TRUNC;
    }
    if (ObDateTimeTC == dest_tc || ObDateTC == dest_tc || ObTimeTC == dest_tc) {
      cast_mode |= CM_NULL_ON_WARN;
    } else if (ob_is_int_uint(src_tc, dest_tc)) {
      cast_mode |= CM_NO_RANGE_CHECK;
    } else if (lib::is_mysql_mode() && ob_is_string_type(src_type) && ob_is_number_tc(dest_type)) {
      cast_mode |= CM_SET_MIN_IF_OVERFLOW;
    }

    if (lib::is_mysql_mode() && ObNumberTC == src_tc && (ob_is_int_tc(dest_type) || ob_is_uint_tc(dest_type))) {
      if (ObNumberTC == src_tc) {
        number::ObNumber src_nmb;
        if (OB_FAIL(obj1_round.get_number(src_nmb))) {
          LOG_WARN("get nmb from src obj failed", K(ret), K(obj1_round));
        } else if (OB_FAIL(src_nmb.round(1))) {
          LOG_WARN("trunc src_nmb failed", K(ret));
        } else {
          obj1_round.set_number(src_nmb);
        }
      }
    }

    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);
    ObCastCtx cast_ctx((expr_ctx).calc_buf_,
        &dtc_params,
        (expr_ctx).phy_plan_ctx_->get_cur_time().get_datetime(),
        (expr_ctx).cast_mode_ | (cast_mode),
        result_type_.get_collation_type());
    const ObObj* res_obj = NULL;
    ObObj buf_obj1;

    bool is_bool = false;
    ObItemType item_type_obj1 = T_NULL;
    if (OB_SUCC(ret) && lib::is_mysql_mode() && (ObIntTC == src_tc || src_tc == ObStringTC)
        && ObJsonType == dest_type) {
      // mysql bool to jsonbool or enumset to json
      if (OB_FAIL(get_param_is_boolean(expr_ctx, obj1, is_bool))) {
        LOG_WARN("get src is_boolean type failed, bool may be cast as json int", K(ret), K(obj1));
      } else if (OB_FAIL(get_param_type(expr_ctx, obj1, item_type_obj1))) {
        LOG_WARN("get param type failed", K(ret), K(obj1));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_bool) {
        ret = ObObjCaster::bool_to_json(dest_type, cast_ctx, obj1_round, buf_obj1, res_obj);
      } else if (OB_UNLIKELY(item_type_obj1 == T_FUN_SET_TO_STR)
                 || OB_UNLIKELY(item_type_obj1 == T_FUN_ENUM_TO_STR)) {
        ret = ObObjCaster::enumset_to_json(dest_type, cast_ctx, obj1_round, buf_obj1, res_obj);
      } else {
        ret = ObObjCaster::to_type(dest_type, cast_ctx, obj1_round, buf_obj1, res_obj);
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("cast failed", K(ret), K(obj1_round), K(dest_type));
      } else if (OB_ISNULL(res_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null pointer", K(ret), K(obj1_round), K(dest_type));
      } else if (OB_FAIL(
          obj_accuracy_check(cast_ctx, accuracy, result_type_.get_collation_type(), *res_obj, buf_obj1, res_obj))) {
        if (ob_is_string_type(dest_type) && OB_ERR_DATA_TOO_LONG == ret) {
          if ((obj1_round.is_character_type() || obj1_round.is_clob()) && compatbility_mode) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_TRUNCATED_WRONG_VALUE;
          }
        }
        LOG_WARN("failed to check accuracy", K(obj1_round), K(dest_type), K(accuracy), K(ret),
                  K((expr_ctx).cast_mode_));
      }
    }
    if (OB_SUCC(ret)) {
      switch (res_obj->get_type()) {
        case ObIntType: {
          if (OB_LIKELY(res_obj->get_int() <= INT_MAX_VAL[ObInt32Type]) &&
              OB_LIKELY(res_obj->get_int() >= INT_MIN_VAL[ObInt32Type])) {
            buf_obj1.set_int(ObInt32Type, res_obj->get_int());
            res_obj = &buf_obj1;
          }
          break;
        }
        case ObUInt64Type: {
          if (OB_LIKELY(res_obj->get_uint64() <= UINT_MAX_VAL[ObUInt32Type])) {
            buf_obj1.set_uint(ObUInt32Type, res_obj->get_uint64());
            res_obj = &buf_obj1;
          }
          break;
        }
          // Oracle
        case ObVarcharType:
        case ObNVarchar2Type:
        case ObNCharType:
        case ObCharType: {
          // case ObNVarchar2Type:
          // case ObNCharType:
          ObObjType type = share::is_oracle_mode() ? res_obj->get_type() : ObVarcharType;
          ObLengthSemantics ls =
              share::is_oracle_mode() ? expr_ctx.my_session_->get_actual_nls_length_semantics() : LS_CHAR;

          int32_t text_length = INT32_MAX;
          ObString text;
          bool has_result = false;
          const ObObj* to_type_obj = NULL;
          ObObj tmp_out_obj;
          if (OB_FAIL(ObObjCaster::to_type(ObCharType, cast_ctx, buf_obj1, tmp_out_obj, to_type_obj))) {
            LOG_WARN("Failed to cast type", K(ret), K(buf_obj1));
          } else if (OB_ISNULL(to_type_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Failed to cast", K(ret));
          } else {
            buf_obj1 = *to_type_obj;
            buf_obj1.set_collation(result_type_);
            if (OB_FAIL(buf_obj1.get_string(text))) {
              LOG_WARN("Failed to get buf_obj1 string", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (accuracy.get_length() == DEFAULT_STR_LENGTH) {
            has_result = true;
            buf_obj1.set_string(type, text.ptr(), text.length());
            LOG_DEBUG("no change", K(ret), K(text), K(buf_obj1), K(text_length), K(accuracy.get_length()));
          } else if (OB_UNLIKELY(accuracy.get_length() < 0)) {
            ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;

            LOG_WARN("accuracy too long", K(ret), K(accuracy.get_length()));
          } else if (share::is_mysql_mode()) {
            // revise length for mysql
            // max_ap [1024, 1073741824]: max allowed packet, ref:
            // https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet
            //
            // [0, max_ap]: pad
            // (max_ap, INT32_MAX]: NULL
            // (INT32_MAX, UINT32_MAX]: DEFAULT, already convert to -1(DEFAULT_STR_LENGTH) in parser
            // (UINT32_MAX, MAX): WARN, already deal in resolver
            int64_t max_allowed_packet = 0;
            if (OB_FAIL(expr_ctx.my_session_->get_max_allowed_packet(max_allowed_packet))) {
              if (OB_ENTRY_NOT_EXIST == ret) {  // for compatibility with server before 1470
                ret = OB_SUCCESS;
                max_allowed_packet = OB_MAX_VARCHAR_LENGTH;
              } else {
                LOG_WARN("Failed to get max allow packet size", K(ret));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (accuracy.get_length() > max_allowed_packet && accuracy.get_length() <= INT32_MAX) {
              has_result = true;
              buf_obj1.set_null();
              LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "cast", static_cast<int>(max_allowed_packet));
            } else if (accuracy.get_length() == 0) {
              has_result = true;
              buf_obj1.set_string(type, NULL, 0);
            }
          } else if (share::is_oracle_mode()) {
            if (OB_UNLIKELY(accuracy.get_length() == 0)) {
              ret = OB_ERR_ZERO_LEN_COL;
              LOG_WARN("Oracle not allowed zero length", K(ret));
            } else if (OB_UNLIKELY((ObCharType == dest_type || ObNCharType == dest_type) &&
                                   OB_MAX_ORACLE_CHAR_LENGTH_BYTE < accuracy.get_length()) ||
                       OB_UNLIKELY((ObVarcharType == dest_type || ObNVarchar2Type == dest_type) &&
                                   OB_MAX_ORACLE_VARCHAR_LENGTH < accuracy.get_length())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected length", K(ret), K(dest_type), K(accuracy.get_length()));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unknown compat mode", K(ret), K(THIS_WORKER.get_compatibility_mode()));
          }

          LOG_DEBUG("length check over", K(ret), K(has_result), K(accuracy.get_length()), K(buf_obj1));

          if (!has_result && OB_SUCC(ret)) {
            bool oracle_char_byte_exceed = false;
            /**
             * get text length
             */
            if (buf_obj1.is_varbinary_or_binary()) {
              text_length = text.length();
            } else if (OB_FAIL(buf_obj1.get_char_length(accuracy, text_length, share::is_oracle_mode()))) {
              LOG_WARN("Failed to get text_length", K(ret));
            }

            /**
             * revise length semantic
             * TODO: use an interface to get semantic
             */
            if (OB_FAIL(ret)) {
            } else if (share::is_mysql_mode()) {
              ls = LS_CHAR;
            } else if (share::is_oracle_mode()) {
              ls = accuracy.get_length_semantics();
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unknown compat mode", K(THIS_WORKER.get_compatibility_mode()));
            }

            /**
             * check max length for Oracle
             */
            oracle_char_byte_exceed = false;
            if (OB_SUCC(ret) && share::is_oracle_mode() && ls == LS_CHAR) {
              if ((ObCharType == dest_type || ObNCharType == dest_type) &&
                  text.length() > OB_MAX_ORACLE_CHAR_LENGTH_BYTE) {
                oracle_char_byte_exceed = true;
              } else if ((ObVarcharType == dest_type || ObNVarchar2Type == dest_type) &&
                         text.length() > OB_MAX_ORACLE_VARCHAR_LENGTH) {
                oracle_char_byte_exceed = true;
              }
            }

            /**
             * substr/pad if needed
             */
            if (OB_FAIL(ret)) {
            } else if (OB_UNLIKELY(ls != LS_CHAR && ls != LS_BYTE)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("wrong length semantic", K(ls));
            } else if (OB_ISNULL(cast_ctx.allocator_v2_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL alloc", K(ret));
            } else if (accuracy.get_length() < text_length || oracle_char_byte_exceed) {
              /**
               * need substr
               */
              int64_t acc_len = !oracle_char_byte_exceed ? accuracy.get_length()
                                                         : ((ObVarcharType == dest_type || ObNVarchar2Type == dest_type)
                                                                   ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                                                   : OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
              int64_t char_len = 0;  // UNUSED
              int64_t size =
                  (ls == LS_BYTE || oracle_char_byte_exceed
                          ? ObCharset::max_bytes_charpos(
                                result_type_.get_collation_type(), text.ptr(), text.length(), acc_len, char_len)
                          : ObCharset::charpos(result_type_.get_collation_type(), text.ptr(), text.length(), acc_len));
              if (size == 0) {
                if (share::is_oracle_mode()) {
                  buf_obj1.set_null();
                } else if (share::is_mysql_mode()) {
                  buf_obj1.set_string(type, NULL, 0);
                }
              } else {
                char* res_ptr = static_cast<char*>(cast_ctx.allocator_v2_->alloc(size));

                if (OB_ISNULL(res_ptr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("Failed to alloc", K(ret), K(size), K(text_length), K(accuracy.get_length()), K(acc_len));
                } else {
                  MEMCPY(res_ptr, text.ptr(), size);
                  buf_obj1.set_string(type, res_ptr, static_cast<ObString::obstr_size_t>(size));
                  LOG_DEBUG(
                      "cast cut", K(text), K(buf_obj1), K(size), K(text_length), K(accuracy.get_length()), K(acc_len));
                }
              }
            } else if (accuracy.get_length() == text_length || (ObCharType != dest_type && ObNCharType != dest_type) ||
                       (share::is_mysql_mode() && res_obj->is_char() &&
                           !(SMO_PAD_CHAR_TO_FULL_LENGTH & expr_ctx.my_session_->get_sql_mode()))) {
              buf_obj1.set_string(type, text.ptr(), text.length());
              LOG_DEBUG("no change", K(ret), K(text), K(buf_obj1), K(text_length), K(accuracy.get_length()));
            } else if (accuracy.get_length() > text_length) {
              /**
               * need pad
               */
              int64_t padding_cnt = accuracy.get_length() - text_length;
              ObString padding_res;
              ObIAllocator& calc_alloc = *cast_ctx.allocator_v2_;
              if (OB_FAIL(
                      padding_char_for_cast(padding_cnt, result_type_.get_collation_type(), calc_alloc, padding_res))) {
                LOG_WARN("padding char failed", K(ret), K(padding_cnt), K(result_type_.get_collation_type()));
              } else {
                int64_t all_size = padding_res.length() + text.length();
                char* res_ptr = static_cast<char*>(cast_ctx.allocator_v2_->alloc(all_size));
                if (OB_ISNULL(res_ptr)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("allocate memory failed", K(ret));
                } else {
                  MEMMOVE(res_ptr, text.ptr(), text.length());
                  MEMMOVE(res_ptr + text.length(), padding_res.ptr(), padding_res.length());
                  buf_obj1.set_string(type, res_ptr, static_cast<ObString::obstr_size_t>(all_size));
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("never reach", K(ret), K(text_length), K(accuracy.get_length()));
            }
          }
          if (OB_SUCC(ret)) {
            res_obj = &buf_obj1;
          }
          break;
        }
        case ObDoubleType: {
          // LOG_DEBUG("after ", K(res_obj->get_double()), K(result.get_double()), K(accuracy), K(ret));
          break;
        }
        case ObFloatType: {
          // LOG_DEBUG("after ", K(res_obj->get_float()), K(result.get_float()), K(accuracy), K(ret));
          break;
        }
        case ObRawType: {
          break;
        }
        default: {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (&result != res_obj) {
        result = *res_obj;
      }
      if (!result.is_null() && ob_is_string_type(result.get_type())) {
        result.set_collation(result_type_);
      }
    }
  }

  return ret;
}

int ObExprCast::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(2 == rt_expr.arg_cnt_);
  OB_ASSERT(NULL != rt_expr.args_);
  OB_ASSERT(NULL != rt_expr.args_[0]);
  OB_ASSERT(NULL != rt_expr.args_[1]);
  ObObjType in_type = rt_expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = rt_expr.args_[0]->datum_meta_.cs_type_;
  ObObjTypeClass src_tc = ob_obj_type_class(in_type);
  ObObjType out_type = rt_expr.datum_meta_.type_;
  ObCollationType out_cs_type = rt_expr.datum_meta_.cs_type_;
  ObObjTypeClass out_tc = ob_obj_type_class(out_type);

  if (OB_UNLIKELY(ObMaxType == in_type || ObMaxType == out_type) || OB_ISNULL(op_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_type or out_type or allocator is invalid", K(ret), K(in_type), K(out_type), KP(op_cg_ctx.allocator_));
  } else {
    // setup cast mode for explicit cast.
    ObCastMode cast_mode = raw_expr.get_extra();
    if (cast_mode & CM_ZERO_FILL) {
      const ObRawExpr* src_raw_expr = NULL;
      CK(OB_NOT_NULL(src_raw_expr = raw_expr.get_param_expr(0)));
      if (OB_SUCC(ret)) {
        const ObExprResType& src_res_type = src_raw_expr->get_result_type();
        if (OB_UNLIKELY(UINT8_MAX < src_res_type.get_length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected zerofill length", K(ret), K(src_res_type.get_length()));
        } else {
          rt_expr.datum_meta_.scale_ = static_cast<int8_t>(src_res_type.get_length());
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDatumCast::choose_cast_function(
              in_type, in_cs_type, out_type, out_cs_type, cast_mode, *(op_cg_ctx.allocator_), rt_expr))) {
        LOG_WARN("choose_cast_func failed", K(ret));
      } else {
        rt_expr.extra_ = cast_mode;
      }
    }
  }
  return ret;
}

int ObExprCast::do_implicit_cast(ObExprCtx& expr_ctx, const ObCastMode& cast_mode, const ObObjType& dst_type,
    const ObObj& src_obj, ObObj& dst_obj) const
{
  int ret = OB_SUCCESS;

  EXPR_SET_CAST_CTX_MODE(expr_ctx);
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);
  ObCastCtx cast_ctx((expr_ctx).calc_buf_,
      &dtc_params,
      (expr_ctx).phy_plan_ctx_->get_cur_time().get_datetime(),
      (expr_ctx).cast_mode_ | (cast_mode),
      result_type_.get_collation_type());
  const ObObj* obj_ptr = NULL;
  ObObj tmp_obj;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObObjCaster::to_type(dst_type, cast_ctx, src_obj, tmp_obj, obj_ptr))) {
    LOG_WARN("cast failed", K(ret), K(src_obj), K(dst_type));
  } else if (OB_ISNULL(obj_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj ptr is NULL", K(ret));
  } else {
    dst_obj = *obj_ptr;
  }
  LOG_DEBUG("do_implicit_cast done", K(ret), K(dst_obj), K(src_obj), K(dst_type), K(cast_mode));
  return ret;
}

OB_DEF_SERIALIZE(ObExprCast)
{
  int ret = OB_SUCCESS;
  bool is_implicit_cast = (1 == extra_serialize_);
  if (CLUSTER_VERSION_2273 == GET_MIN_CLUSTER_VERSION()) {
    BASE_SER((ObExprCast, ObFuncExprOperator));
    OB_UNIS_ENCODE(is_implicit_cast);
  } else {
    ret = ObExprOperator::serialize_(buf, buf_len, pos);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObExprCast)
{
  int ret = OB_SUCCESS;
  if (CLUSTER_VERSION_2273 == GET_MIN_CLUSTER_VERSION()) {
    bool is_implicit_cast = false;
    BASE_DESER((ObExprCast, ObFuncExprOperator));
    OB_UNIS_DECODE(is_implicit_cast);
    extra_serialize_ = is_implicit_cast ? 1 : 0;
  } else {
    ret = ObExprOperator::deserialize_(buf, data_len, pos);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprCast)
{
  int64_t len = 0;
  if (CLUSTER_VERSION_2273 == GET_MIN_CLUSTER_VERSION()) {
    bool is_implicit_cast = false;
    BASE_ADD_LEN((ObExprCast, ObFuncExprOperator));
    OB_UNIS_ADD_LEN(is_implicit_cast);
  } else {
    len = ObExprOperator::get_serialize_size_();
  }
  return len;
}

}  // namespace sql
}  // namespace oceanbase
