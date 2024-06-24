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
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/object/ob_obj_cast.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/geo/ob_geometry_cast.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_allocator.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_resolver.h"
#include "sql/engine/expr/vector_cast/vector_cast.h"
#include "sql/engine/expr/ob_expr_util.h"

// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprCast::ObExprCast(ObIAllocator &alloc)
    : ObFuncExprOperator::ObFuncExprOperator(alloc, T_FUN_SYS_CAST,
                                             N_CAST,
                                             2,
                                             NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  extra_serialize_ = 0;
  disable_operand_auto_cast();
}

int ObExprCast::get_cast_inttc_len(ObExprResType &type1,
                                   ObExprResType &type2,
                                   ObExprTypeCtx &type_ctx,
                                   int32_t &res_len,
                                   int16_t &length_semantics,
                                   ObCollationType conn,
                                   ObCastMode cast_mode) const
{
  int ret = OB_SUCCESS;
  if (type1.is_literal()) { // literal
    if (ObStringTC == type1.get_type_class()) {
      res_len = type1.get_accuracy().get_length();
      length_semantics = type1.get_length_semantics();
    } else if (OB_FAIL(ObField::get_field_mb_length(type1.get_type(),
        type1.get_accuracy(), type1.get_collation_type(), res_len))) {
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
    } else if (OB_FAIL(get_cast_string_len(type1, type2, type_ctx, res_len, length_semantics, conn, cast_mode))) {
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
int ObExprCast::get_cast_string_len(ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx,
                                    int32_t &res_len,
                                    int16_t &length_semantics,
                                    ObCollationType conn,
                                    ObCastMode cast_mode) const
{
  int ret = OB_SUCCESS;
  const ObObj &val = type1.get_param();
  if (!type1.is_literal()) { // column
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    int16_t prec = type1.get_accuracy().get_precision();
    int16_t scale = type1.get_accuracy().get_scale();
    switch(type1.get_type()) {
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
    case ObUNumberType:
    case ObDecimalIntType: {
        if (lib::is_oracle_mode()) {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
            } else if (0 == scale){
              res_len = prec + 1;
            } else {
              res_len = prec - scale;
            }
          }
        } else {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
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
    // TODO@hanhui text share with varchar temporarily
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
    case ObLobType:
    case ObJsonType:
    case ObGeometryType: {
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
    res_len = 0;//compatible with mysql;
  } else if (OB_ISNULL(type_ctx.get_session())) {
    // calc type don't set ret, just print the log. by design.
    LOG_WARN("my_session is null");
  } else { // literal
    ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
    ObCollationType cast_coll_type = (CS_TYPE_INVALID != type2.get_collation_type())
        ? type2.get_collation_type()
        : conn;
    ObDataTypeCastParams dtc_params;
    ObTimeZoneInfoWrap tz_wrap;
    bool is_valid = false;
    ObRawExpr *raw_expr = NULL;
    if (OB_ISNULL(raw_expr = type_ctx.get_raw_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      ObCastCtx cast_ctx(&oballocator,
                        &type_ctx.get_dtc_params(),
                        0,
                        cast_mode,
                        cast_coll_type);
      ObString val_str;
      EXPR_GET_VARCHAR_V2(val, val_str);
      //这里设置的len为字符个数
      if (OB_SUCC(ret) && NULL != val_str.ptr()) {
        int32_t len_byte = val_str.length();
        res_len = len_byte;
        length_semantics = LS_CHAR;
        if (NULL != val_str.ptr()) {
          int32_t trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cast_coll_type,
              val_str.ptr(), len_byte));
          res_len = static_cast<int32_t>(ObCharset::strlen_char(cast_coll_type,
              val_str.ptr(), trunc_len_byte));
        }
        if (type1.is_numeric_type() && !type1.is_integer_type()) {
          res_len += 1;
        }
      }
    }
  }
  return ret;
}

// this is only for engine 3.0. old engine will get cast mode from expr_ctx.
// only for explicit cast, implicit cast's cm is setup while deduce type(in type_ctx.cast_mode_)
int ObExprCast::get_explicit_cast_cm(const ObExprResType &src_type,
                              const ObExprResType &dst_type,
                              const ObSQLSessionInfo &session,
                              ObSQLMode sql_mode,
                              const ObRawExpr &cast_raw_expr,
                              ObCastMode &cast_mode) const
{
  int ret = OB_SUCCESS;
  cast_mode = CM_NONE;
  const bool is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_raw_expr.get_extra());
  const int32_t result_flag = src_type.get_result_flag();
  const ObObjTypeClass dst_tc = ob_obj_type_class(dst_type.get_type());
  const ObObjTypeClass src_tc = ob_obj_type_class(src_type.get_type());
  ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag,
                                    session.get_stmt_type(),
                                    session.is_ignore_stmt(),
                                    sql_mode, cast_mode);
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
    if (!is_oracle_mode() && CM_IS_EXPLICIT_CAST(cast_mode)) {
      // CM_STRING_INTEGER_TRUNC is only for string to int cast in mysql mode
      if (ob_is_string_type(src_type.get_type()) &&
          (ob_is_int_tc(dst_type.get_type()) || ob_is_uint_tc(dst_type.get_type()))) {
        cast_mode |= CM_STRING_INTEGER_TRUNC;
      }
      // select cast('1e500' as decimal);  -> max_val
      // select cast('-1e500' as decimal); -> min_val
      if (ob_is_string_type(src_type.get_type())
          && (ob_is_number_tc(dst_type.get_type()) || ob_is_decimal_int_tc(dst_type.get_type()))) {
        cast_mode |= CM_SET_MIN_IF_OVERFLOW;
      }
      if (!is_called_in_sql() && CM_IS_WARN_ON_FAIL(cast_raw_expr.get_extra())) {
        cast_mode |= CM_WARN_ON_FAIL;
      }
    }
  }
  return ret;
}

bool ObExprCast::check_cast_allowed(const ObObjType orig_type,
                                    const ObCollationType orig_cs_type,
                                    const ObObjType expect_type,
                                    const ObCollationType expect_cs_type,
                                    const bool is_explicit_cast) const
{
  UNUSED(expect_cs_type);
  bool res = true;
  ObObjTypeClass ori_tc = ob_obj_type_class(orig_type);
  ObObjTypeClass expect_tc = ob_obj_type_class(expect_type);
  bool is_expect_lob_tc = (ObLobTC == expect_tc || ObTextTC == expect_tc);
  bool is_ori_lob_tc = (ObLobTC == ori_tc || ObTextTC == ori_tc);
  if (is_oracle_mode()) {
    if (is_explicit_cast) {
      // can't cast lob to other type except char/varchar/nchar/nvarchar2/raw. clob to raw not allowed too.
      if (is_ori_lob_tc) {
        if (expect_tc == ObJsonTC) {
          /* oracle mode, json text use lob store */
        } else if (ObStringTC == expect_tc) {
          // do nothing
        } else if (ObRawTC == expect_tc) {
          res = CS_TYPE_BINARY == orig_cs_type;
        } else {
          res = false;
        }
      }
      // any type to lob type not allowed.
      if (is_expect_lob_tc) {
        res = false;
      }
    } else {
      // BINARY FLOAT/DOUBLE not allow cast lob whether explicit
      if (is_ori_lob_tc) {
        if (expect_tc == ObFloatTC || expect_tc == ObDoubleTC) {
          res = false;
        }
      }
    }
  }
  return res;
}

int ObExprCast::calc_result_type2(ObExprResType &type,
                                  ObExprResType &type1,
                                  ObExprResType &type2,
                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType dst_type;
  ObRawExpr *cast_raw_expr = NULL;
  const sql::ObSQLSessionInfo *session = NULL;
  bool is_explicit_cast = false;
  ObCollationLevel cs_level = CS_LEVEL_INVALID;
  bool enable_decimalint = false;
  if (OB_ISNULL(session = type_ctx.get_session()) ||
      OB_ISNULL(cast_raw_expr = get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", K(ret), KP(session), KP(cast_raw_expr));
  } else if (OB_UNLIKELY(NOT_ROW_DIMENSION != row_dimension_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid row_dimension_", K(row_dimension_), K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session, enable_decimalint))) {
    LOG_WARN("fail to check_enable_decimalint", K(ret), K(session->get_effective_tenant_id()));
  } else if (OB_FAIL(get_cast_type(enable_decimalint,
                                   type2, cast_raw_expr->get_extra(), dst_type))) {
    LOG_WARN("get cast dest type failed", K(ret));
  } else if (OB_FAIL(adjust_udt_cast_type(type1, dst_type, type_ctx))) {
     LOG_WARN("adjust udt cast sub type failed", K(ret));
  } else if (OB_UNLIKELY(!cast_supported(type1.get_type(), type1.get_collation_type(),
                                        dst_type.get_type(), dst_type.get_collation_type()))) {
    if (session->is_varparams_sql_prepare()) {
      type.set_null();
      LOG_TRACE("ps prepare phase ignores type deduce error");
    } else if (const_cast<ObSQLSessionInfo *>(session)->is_pl_prepare_stage()
               && dst_type.is_geometry()
               && lib::is_oracle_mode()) {
      // oracle gis ignore ignore type deduce error in pl prepare
      type.set_geometry();
      LOG_TRACE("pl prepare phase ignores type deduce error");
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("transition does not support", "src", ob_obj_type_str(type1.get_type()),
                "dst", ob_obj_type_str(dst_type.get_type()));
    }
  } else if (FALSE_IT(is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_raw_expr->get_extra()))) {
  // check cast supported in cast_map but not support here.
  } else if (OB_FAIL(ObSQLUtils::get_cs_level_from_cast_mode(cast_raw_expr->get_extra(),
                                                             type1.get_collation_level(),
                                                             cs_level))) {
    LOG_WARN("failed to get collation level", K(ret));
  } else if (!check_cast_allowed(type1.get_type(), type1.get_collation_type(),
                                 dst_type.get_type(), dst_type.get_collation_type(),
                                 is_explicit_cast)) {
    if (session->is_varparams_sql_prepare()) {
      type.set_null();
      LOG_TRACE("ps prepare phase ignores type deduce error");
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("explicit cast to lob type not allowed", K(ret), K(dst_type));
    }
  } else {
    // always cast to user requested type
    if (is_explicit_cast && !lib::is_oracle_mode() &&
        ObCharType == dst_type.get_type()) {
      // cast(x as binary(10)), in parser,binary->T_CHAR+bianry, but, result type should be varchar, so set it.
      type.set_type(ObVarcharType);
    } else if (lib::is_mysql_mode() && ObFloatType == dst_type.get_type()) {
      // Compatible with mysql. If the precision p is not specified, produces a result of type FLOAT. 
      // If p is provided and 0 <=  p <= 24, the result is of type FLOAT. If 25 <= p <= 53, 
      // the result is of type DOUBLE. If p < 0 or p > 53, an error is returned
      // however, ob use -1 as default precision, so it is a valid value
      type.set_collation_type(dst_type.get_collation_type());
      ObPrecision float_precision = dst_type.get_precision();
      ObScale float_scale = dst_type.get_scale();
      if (OB_UNLIKELY(float_scale > OB_MAX_DOUBLE_FLOAT_SCALE)) {
        ret = OB_ERR_TOO_BIG_SCALE;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, float_scale, "CAST", OB_MAX_DOUBLE_FLOAT_SCALE);
        LOG_WARN("scale of float overflow", K(ret), K(float_scale), K(float_precision));
      } else if (float_precision < -1 ||
          (SCALE_UNKNOWN_YET == float_scale && float_precision > OB_MAX_DOUBLE_FLOAT_PRECISION)) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, float_precision, "CAST", OB_MAX_DOUBLE_FLOAT_PRECISION);
      } else if (SCALE_UNKNOWN_YET == float_scale) {
        if (float_precision <= OB_MAX_FLOAT_PRECISION) {
          type.set_type(ObFloatType);
        } else {
          type.set_type(ObDoubleType);
        }
      } else {
        type.set_type(ObFloatType);
        type.set_precision(float_precision);
        type.set_scale(float_scale);
      }
    } else if (dst_type.is_user_defined_sql_type() || dst_type.is_collection_sql_type()) {
      type.set_type(dst_type.get_type());
      type.set_subschema_id(dst_type.get_subschema_id());
    } else {
      type.set_type(dst_type.get_type());
      type.set_collation_type(dst_type.get_collation_type());
    }
    int16_t scale = dst_type.get_scale();
    if (is_explicit_cast
        && !lib::is_oracle_mode()
        && (ObTimeType == dst_type.get_type() || ObDateTimeType == dst_type.get_type())
        && scale > 6) {
      ret = OB_ERR_TOO_BIG_PRECISION;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", OB_MAX_DATETIME_PRECISION);
    }
    if (OB_SUCC(ret)) {
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      ObCollationType collation_connection = type_ctx.get_coll_type();
      ObCollationType collation_nation = session->get_nls_collation_nation();
      type1.set_calc_type(get_calc_cast_type(type1.get_type(), dst_type.get_type()));
      int32_t length = 0;
      if (ob_is_string_or_lob_type(dst_type.get_type()) || ob_is_raw(dst_type.get_type()) || ob_is_json(dst_type.get_type())
          || ob_is_geometry(dst_type.get_type())) {
        type.set_collation_level(cs_level);
        int32_t len = dst_type.get_length();
        int16_t length_semantics = ((dst_type.is_string_or_lob_locator_type() || dst_type.is_json())
            ? dst_type.get_length_semantics()
            : (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics()
                : LS_BYTE));
        if (len < 0 && !is_called_in_sql() && lib::is_oracle_mode()) {
          if (dst_type.is_char() || dst_type.is_nchar()) {
            type.set_full_length(OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE, length_semantics);
          } else if (dst_type.is_nvarchar2() || dst_type.is_varchar()) {
            type.set_full_length(OB_MAX_ORACLE_VARCHAR_LENGTH, length_semantics);
          }
        } else if (len > 0) { // cast(1 as char(10))
          type.set_full_length(len, length_semantics);
        } else if (OB_FAIL(get_cast_string_len(type1, dst_type, type_ctx, len, length_semantics,
                                               collation_connection,
                                               cast_raw_expr->get_extra()))) { // cast (1 as char)
          LOG_WARN("fail to get cast string length", K(ret));
        } else {
          type.set_full_length(len, length_semantics);
        }
        if (CS_TYPE_INVALID != dst_type.get_collation_type()) {
          // cast as binary
          type.set_collation_type(dst_type.get_collation_type());
        } else {
          // use collation of current session
          type.set_collation_type(ob_is_nstring_type(dst_type.get_type()) ?
                                  collation_nation : collation_connection);
        }
      } else if (ob_is_extend(dst_type.get_type())
                 || dst_type.is_user_defined_sql_type()
                 || dst_type.is_collection_sql_type()) {
        type.set_udt_id(type2.get_udt_id());
      } else {
        type.set_length(length);
        if ((ObNumberTC == dst_type.get_type_class() || ObDecimalIntTC == dst_type.get_type_class())
            && 0 == dst_type.get_precision()) {
          // MySql:cast (1 as decimal(0)) = cast(1 as decimal)
          // Oracle: cast(1.4 as number) = cast(1.4 as number(-1, -1))
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObNumberType].get_precision());
        } else if ((ObIntTC == dst_type.get_type_class() || ObUIntTC == dst_type.get_type_class())
                   && dst_type.get_precision() <= 0) {
          // for int or uint , the precision = len
          int32_t len = 0;
          int16_t length_semantics = LS_BYTE;//unused
          if (OB_FAIL(get_cast_inttc_len(type1, dst_type, type_ctx, len, length_semantics,
                                         collation_connection, cast_raw_expr->get_extra()))) {
            LOG_WARN("fail to get cast inttc length", K(ret));
          } else {
            len = len > OB_LITERAL_MAX_INT_LEN ? OB_LITERAL_MAX_INT_LEN : len;
            type.set_precision(static_cast<int16_t>(len));
          }
        } else if (ORACLE_MODE == compatibility_mode && ObDoubleType == dst_type.get_type()) {
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][dst_type.get_type()];
          type.set_accuracy(acc);
          if (type1.is_decimal_int()) {
            acc = type1.get_accuracy();
          }
          type1.set_accuracy(acc);
        } else if (ObYearType == dst_type.get_type()) {
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][dst_type.get_type()];
          type.set_accuracy(acc);
          if (type1.is_decimal_int() && acc.precision_ < type1.get_precision()) {
            acc.precision_ = type1.get_precision();
          }
          type1.set_accuracy(acc);
        } else {
          type.set_precision(dst_type.get_precision());
        }
        type.set_scale(dst_type.get_scale());
      }
    }
    CK(OB_NOT_NULL(type_ctx.get_session()));
    if (OB_SUCC(ret)) {
      // interval expr need NOT_NULL_FLAG
      // bug:
      calc_result_flag2(type, type1, type2);
      if (CM_IS_ADD_ZEROFILL(cast_raw_expr->get_extra())) {
        type.set_result_flag(ZEROFILL_FLAG);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObCastMode explicit_cast_cm = CM_NONE;
    if (OB_FAIL(get_explicit_cast_cm(type1, dst_type, *session,
                                     type_ctx.get_sql_mode(),
                                     *cast_raw_expr,
                                     explicit_cast_cm))) {
      LOG_WARN("set cast mode failed", K(ret));
    } else if (CM_IS_EXPLICIT_CAST(explicit_cast_cm)) {
      // cast_raw_expr.extra_ store explicit cast's cast mode
      cast_raw_expr->set_extra(explicit_cast_cm);
      // type_ctx.cast_mode_ sotre implicit cast's cast mode.
      // cannot use def cm, because it may change explicit cast behavior.
      // eg: select cast(18446744073709551615 as signed) -> -1
      //     because exprlicit case need CM_NO_RANGE_CHECK
      type_ctx.set_cast_mode(explicit_cast_cm & ~CM_EXPLICIT_CAST);
      // in engine 3.0, let implicit cast do the real cast
      bool need_extra_cast_for_src_type = false;
      bool need_extra_cast_for_dst_type = false;
      ObRawExprUtils::need_extra_cast(type1, type, need_extra_cast_for_src_type,
                                      need_extra_cast_for_dst_type);
      if (need_extra_cast_for_src_type) {
        ObExprResType src_type_utf8;
        OZ(ObRawExprUtils::setup_extra_cast_utf8_type(type1, src_type_utf8));
        OX(type1.set_calc_meta(src_type_utf8.get_obj_meta()));
      } else if (need_extra_cast_for_dst_type) {
        ObExprResType dst_type_utf8;
        OZ(ObRawExprUtils::setup_extra_cast_utf8_type(dst_type, dst_type_utf8));
        OX(type1.set_calc_meta(dst_type_utf8.get_obj_meta()));
      } else if (type1.is_ext() && type1.get_udt_id() == T_OBJ_XML
                 && dst_type.is_character_type() && is_called_in_sql()) {
        // only when cast (pl xmltype variable as chartype) in pl
        ObExprResType sql_xml_type;
        sql_xml_type.set_sql_udt(ObXMLSqlType);
        type1.set_calc_meta(sql_xml_type.get_obj_meta());
      } else {
        bool need_warp = false;
        if (ob_is_enumset_tc(type1.get_type())) {
          // For enum/set type, need to check whether warp to string is required.
          if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(type1.get_type(), type1.get_calc_type(),
                                                          false, need_warp))) {
            LOG_WARN("need_wrap_to_string failed", K(ret), K(type1));
          }
        } else if (OB_LIKELY(need_warp)) {
          // need_warp is true, no-op and keep type1's calc_type is dst_type. It will be wrapped
          // to string in ObRawExprWrapEnumSet::visit(ObSysFunRawExpr &expr) later.
        } else {
          if (ob_is_geometry_tc(dst_type.get_type())) {
            ObCastMode cast_mode = cast_raw_expr->get_extra();
            const ObObj &param = type2.get_param();
            ParseNode parse_node;
            parse_node.value_ = param.get_int();
            ObGeoType geo_type = static_cast<ObGeoType>(parse_node.int16_values_[OB_NODE_CAST_GEO_TYPE_IDX]);
            if (OB_FAIL(ObGeoCastUtils::set_geo_type_to_cast_mode(geo_type, cast_mode))) {
              LOG_WARN("fail to set geometry type to cast mode", K(ret), K(geo_type));
            } else {
              cast_raw_expr->set_extra(cast_mode);
            }
          }
          if (OB_SUCC(ret)) {
            // need_warp is false, set calc_type to type1 itself.
            type1.set_calc_meta(type1.get_obj_meta());
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (lib::is_mysql_mode() && !ob_is_numeric_type(type.get_type()) && type1.is_double()) {
          // for double type cast non-numeric type, no need set calc accuracy to dst type.
        } else if (ObDecimalIntType == type1.get_type()
                   && ob_is_decimal_int(type1.get_calc_type())) {
          // set type1's calc accuracy with param's accuracy
          type1.set_calc_accuracy(type1.get_accuracy());
        } else {
          type1.set_calc_accuracy(type.get_accuracy());
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
  LOG_DEBUG("calc result type", K(type1), K(type2), K(type), K(dst_type),
            K(type1.get_calc_accuracy()));
  return ret;
}

int ObExprCast::get_cast_type(const bool enable_decimal_int,
                              const ObExprResType param_type2,
                              const ObCastMode cast_mode,
                              ObExprResType &dst_type)
{
  int ret = OB_SUCCESS;
  if (!param_type2.is_int() && !param_type2.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type2));
  } else {
    const ObObj &param = param_type2.get_param();
    ParseNode parse_node;
    parse_node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    bool is_explicit_cast = CM_IS_EXPLICIT_CAST(cast_mode);
    dst_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    dst_type.set_type(obj_type);
    int64_t maxblen = ObCharset::CharConvertFactorNum;
    if (ob_is_lob_locator(obj_type)) {
      // cast(x as char(10)) or cast(x as binary(10))
      dst_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX], param_type2.get_accuracy().get_length_semantics());
    } else if (ob_is_string_type(obj_type)) {
      dst_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX], param_type2.get_accuracy().get_length_semantics());
      if (lib::is_mysql_mode() && is_explicit_cast && !dst_type.is_binary() && !dst_type.is_varbinary()) {
        if (dst_type.get_length() > OB_MAX_CAST_CHAR_VARCHAR_LENGTH && dst_type.get_length() <= OB_MAX_CAST_CHAR_TEXT_LENGTH) {
          dst_type.set_type(ObTextType);
          dst_type.set_length(OB_MAX_CAST_CHAR_TEXT_LENGTH);
        } else if (dst_type.get_length() > OB_MAX_CAST_CHAR_TEXT_LENGTH && dst_type.get_length() <= OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH) {
          dst_type.set_type(ObMediumTextType);
          dst_type.set_length(OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH);
        } else if (dst_type.get_length() > OB_MAX_CAST_CHAR_MEDIUMTEXT_LENGTH) {
          dst_type.set_type(ObLongTextType);
          dst_type.set_length(OB_MAX_LONGTEXT_LENGTH / maxblen);
        }
      }
    } else if (ob_is_raw(obj_type)) {
      dst_type.set_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
    } else if (ob_is_extend(obj_type)
               || ob_is_user_defined_sql_type(obj_type)
               || ob_is_collection_sql_type(obj_type)) {
      dst_type.set_udt_id(param_type2.get_udt_id());
    } else if (lib::is_mysql_mode() && ob_is_json(obj_type)) {
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (ob_is_geometry(obj_type)) {
      dst_type.set_collation_type(CS_TYPE_BINARY);
      dst_type.set_collation_level(CS_LEVEL_IMPLICIT);
    } else if (ob_is_interval_tc(obj_type)) {
      if (CM_IS_EXPLICIT_CAST(cast_mode) &&
          ((ObIntervalYMType != obj_type && !ObIntervalScaleUtil::scale_check(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX])) ||
           !ObIntervalScaleUtil::scale_check(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]))) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
        LOG_WARN("target interval type precision out of range", K(ret), K(obj_type));
      } else if (ObIntervalYMType == obj_type) {
        // interval year to month type has no precision
        dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
      } else if (ob_is_interval_ds(obj_type)) {
        //scale in day seconds type is day_scale * 10 + seconds_scale.
        dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] * 10 +
                                                parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
      } else {
        dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
        dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
      }
    } else if (ObNumberType == obj_type) {
      dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
      if (enable_decimal_int && CM_IS_EXPLICIT_CAST(cast_mode)) {
        if (is_mysql_mode()) {
          // in mysql mode, if cast is explicit, change dst type from NumberType to DecimalIntType
          dst_type.set_type(ObDecimalIntType);
        } else {
          if (is_decimal_int_accuracy_valid(dst_type.get_precision(), dst_type.get_scale())) {
            // in oracle mode, if cast is explicit and p >= s and s >= 0
            // change dest type from NumberType to DecimalIntType
            dst_type.set_type(ObDecimalIntType);
          } else {
            dst_type.set_type(ObNumberType);
          }
        }
      }
    } else {
      dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
    }
    LOG_DEBUG("get_cast_type", K(dst_type), K(param_type2));
  }
  return ret;
}

int ObExprCast::adjust_udt_cast_type(const ObExprResType &src_type,
                                     ObExprResType &dst_type,
                                     ObExprTypeCtx &type_ctx) const
{
  // set subschema id in this function for cast
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  if (src_type.is_ext()) {
    if (dst_type.is_user_defined_sql_type() || dst_type.is_collection_sql_type()) {
      // phy_plan_ctx_ may not exist during deduce,
      // save subschema mapping on sql_ctx_ before phy_plan ready?
      const uint64_t udt_type_id = src_type.get_udt_id();
      uint16_t subschema_id = ObMaxSystemUDTSqlType;

      if (udt_type_id == T_OBJ_XML) {
        subschema_id = 0;
      } else if (OB_ISNULL(exec_ctx)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("need ctx to get subschema mapping",
                 K(ret), K(src_type), K(dst_type), KP(session), KP(exec_ctx));
      } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_type_id, subschema_id))) {
        LOG_WARN("failed to get subshcema_meta_info",
                 K(ret), K(src_type), K(dst_type), K(udt_type_id));
      } else if (dst_type.get_udt_id() != src_type.get_udt_id()) {
        // not xmltype, udt id must setted in both pl types and sql types
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt id mismarch", K(ret), K(src_type), K(dst_type),
                 K(dst_type.get_udt_id()), K(src_type.get_udt_id()));
      }

      if (OB_FAIL(ret)) {
      } else if (subschema_id == ObMaxSystemUDTSqlType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cast unsupported pl udt type to sql udt type",
                 K(ret), K(src_type), K(dst_type), K(udt_type_id));
      } else {
        dst_type.set_subschema_id(subschema_id);
        dst_type.set_udt_id(udt_type_id);
      }
    } else if (dst_type.is_character_type() && !is_called_in_sql()) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type", K(ret), K(src_type), K(dst_type));
    } else if (dst_type.is_ext()) {
      // disallow PL sdo_geometry cast
      uint64_t src_udt_id = src_type.get_udt_id();
      uint64_t dst_udt_id = dst_type.get_udt_id();
      if (ObGeometryTypeCastUtil::is_sdo_geometry_udt(src_udt_id)
                && ObGeometryTypeCastUtil::is_sdo_geometry_udt(dst_udt_id)) {
        if (!ObGeometryTypeCastUtil::is_sdo_geometry_type_compatible(src_udt_id, dst_udt_id)) {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("udt id mismatch", K(ret), K(src_type), K(dst_type),
              K(src_udt_id), K(dst_udt_id));
        }
      }
    }
  } else if (src_type.is_user_defined_sql_type() || src_type.is_collection_sql_type()) {
    const uint16_t subschema_id = src_type.get_subschema_id(); // do not need subschema id ?
    uint64_t udt_id = 0;
    uint64_t src_udt_id = src_type.get_udt_id();
    uint64_t dst_udt_id = dst_type.get_udt_id();
    if (dst_type.is_ext()) {
      ObSqlUDTMeta udt_meta;
      if (subschema_id == ObXMLSqlType) {
        udt_id = T_OBJ_XML;
      } else if (OB_ISNULL(exec_ctx)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("need ctx to get subschema mapping",
                 K(ret), K(src_type), K(dst_type), KP(session), KP(exec_ctx));
      } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
        LOG_WARN("Failed to get subshcema_meta_info",
                  K(ret), K(src_type), K(dst_type), K(udt_meta.udt_id_));
      } else if (udt_meta.udt_id_ == T_OBJ_NOT_SUPPORTED) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cast unsupported sql udt type to pl udt type",
                  K(ret), K(src_type), K(dst_type), K(subschema_id));
      } else if (ObGeometryTypeCastUtil::is_sdo_geometry_udt(src_udt_id)
                && ObGeometryTypeCastUtil::is_sdo_geometry_udt(dst_udt_id)) {
        uint16_t dst_subschema_id = ObInvalidSqlType;
        if (!ObGeometryTypeCastUtil::is_sdo_geometry_type_compatible(src_udt_id, dst_udt_id)) {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("udt id mismatch", K(ret), K(src_type), K(dst_type),
              K(src_udt_id), K(dst_udt_id));
        } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(dst_udt_id, dst_subschema_id))) {
          LOG_WARN("unsupported udt id", K(ret), K(dst_subschema_id));
        } else {
          dst_type.set_type(ObUserDefinedSQLType);
          dst_type.set_subschema_id(dst_subschema_id);
          udt_id = dst_udt_id;
        }
      } else if (dst_udt_id != src_udt_id) {
        // not xmltype or sdo_geometry, udt id must setted in both pl types and sql types
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt id mismatch", K(ret), K(src_type), K(dst_type),
                 K(dst_udt_id), K(src_udt_id));
      } else {
        udt_id = udt_meta.udt_id_;
      }

      if (OB_SUCC(ret)) {
        dst_type.set_udt_id(udt_id);
      }
    } else if (dst_type.is_user_defined_sql_type()) {
      dst_type.set_subschema_id(subschema_id);
      LOG_INFO("cast from sql udt to sql udt", K(src_type), K(dst_type), K(lbt()));
    }
  } else if ((src_type.is_null() || src_type.is_character_type())
             && (dst_type.get_type() == ObUserDefinedSQLType
                 || dst_type.get_type() == ObCollectionSQLType)) {
    const uint64_t udt_type_id = dst_type.get_udt_id();
    uint16_t subschema_id = ObMaxSystemUDTSqlType;

    if (!ObObjUDTUtil::ob_is_supported_sql_udt(dst_type.get_udt_id())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported udt type for sql udt", K(ret), K(src_type), K(dst_type),
               K(dst_type.get_udt_id()), K(src_type.get_udt_id()));
    } else if (udt_type_id == T_OBJ_XML) {
      subschema_id = 0;
    } else if (OB_ISNULL(exec_ctx)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("need ctx to get subschema mapping",
                K(ret), K(src_type), K(dst_type), KP(session), KP(exec_ctx));
    } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_type_id, subschema_id))) {
      LOG_WARN("failed to get subshcema_meta_info",
                K(ret), K(src_type), K(dst_type), K(udt_type_id));
    } else { /* do nothing */ }

    if (OB_FAIL(ret)) {
    } else if (subschema_id == ObMaxSystemUDTSqlType) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cast unsupported pl udt type to sql udt type",
                K(ret), K(src_type), K(dst_type), K(udt_type_id));
    } else {
      dst_type.set_subschema_id(subschema_id);
    }
  }
  return ret;
}

int ObExprCast::get_subquery_iter(const sql::ObExpr &expr,
                                  sql::ObEvalCtx &ctx,
                                  ObExpr **&subquery_row,
                                  ObEvalCtx *&subquery_ctx,
                                  ObSubQueryIterator *&iter)
{
  int ret = OB_SUCCESS;
  iter = NULL;
  subquery_row = NULL;
  subquery_ctx = NULL;
  sql::ObDatum *subquery_datum = NULL;
  const ObExprCast::CastMultisetExtraInfo *info =
    static_cast<const ObExprCast::CastMultisetExtraInfo *>(expr.extra_info_);
  if (OB_UNLIKELY(2 != expr.arg_cnt_) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, subquery_datum))){
    LOG_WARN("expr evaluate failed", K(ret));
  } else if (OB_ISNULL(subquery_datum) || subquery_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL subquery ref info returned", K(ret));
  } else if (OB_FAIL(ObExprSubQueryRef::get_subquery_iter(
                ctx, ObExprSubQueryRef::Extra::get_info(subquery_datum->get_int()), iter))) {
    LOG_WARN("get subquery iterator failed", K(ret));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL subquery iterator", K(ret));
  } else if (OB_FAIL(iter->rewind())) {
    LOG_WARN("start iterate failed", K(ret));
  } else if (OB_ISNULL(subquery_row = &const_cast<ExprFixedArray &>(iter->get_output()).at(0))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null row", K(ret));
  } else if (OB_ISNULL(subquery_ctx = &iter->get_eval_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ctx", K(ret));
  } else if (OB_UNLIKELY(iter->get_output().count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected output column count", K(ret), K(iter->get_output().count()));
  } else if (ObNullType != iter->get_output().at(0)->datum_meta_.type_
        && iter->get_output().at(0)->datum_meta_.type_ != info->elem_type_.get_obj_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check type failed", K(ret), K(expr), KPC(iter->get_output().at(0)), K(info->elem_type_));
  }
  return ret;
}

int ObExprCast::construct_collection(const sql::ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     sql::ObDatum &res_datum,
                                     ObExpr **subquery_row,
                                     ObEvalCtx *subquery_ctx,
                                     ObSubQueryIterator *subquery_iter)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  pl::ObPLCollection *coll = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();
  const ObExprCast::CastMultisetExtraInfo *info =
    static_cast<const ObExprCast::CastMultisetExtraInfo *>(expr.extra_info_);
  if (OB_ISNULL(info) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (pl::PL_NESTED_TABLE_TYPE == info->pl_type_) {
    coll = OB_NEWx(pl::ObPLNestedTable, (&alloc), info->udt_id_);
  } else if (pl::PL_VARRAY_TYPE == info->pl_type_) {
    coll = OB_NEWx(pl::ObPLVArray, (&alloc), info->udt_id_);
    if (OB_NOT_NULL(coll)) {
      static_cast<pl::ObPLVArray*>(coll)->set_capacity(info->capacity_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected collection type to construct", K(info->type_), K(ret));
  }

  // set collection property
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(coll)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate coll", K(ret));
  } else {
    pl::ObPLINS *ns = NULL;
    const pl::ObUserDefinedType *type = NULL;
    const pl::ObCollectionType *collection_type = NULL;
    pl::ObElemDesc elem_desc;
    pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(alloc,
                                   *session,
                                   *(exec_ctx.get_sql_ctx()->schema_guard_),
                                   package_guard,
                                   *(exec_ctx.get_sql_proxy()),
                                   false);
    if (OB_FAIL(package_guard.init())) {
      LOG_WARN("failed to init package guard", K(ret));
    } else {
      ns = &resolve_ctx;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ns->get_user_type(info->udt_id_, type))) {
      LOG_WARN("failed to get user type", K(ret), K_(info->udt_id));
    } else if (OB_ISNULL(type) || OB_UNLIKELY(!type->is_collection_type())
               || OB_ISNULL(collection_type = static_cast<const pl::ObCollectionType*>(type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected type", K(ret), KPC(type));
    } else if (info->elem_type_.get_meta_type().is_ext()) {
      int64_t field_cnt = OB_INVALID_COUNT;
      elem_desc.set_obj_type(common::ObExtendType);
      if (OB_FAIL(collection_type->get_element_type().get_field_count(*ns, field_cnt))) {
        LOG_WARN("failed to get field count", K(ret));
      } else {
        elem_desc.set_field_count(field_cnt);
      }
    } else {
      elem_desc.set_data_type(info->elem_type_);
      elem_desc.set_field_count(1);
    }
    if (OB_SUCC(ret)) {
      coll->set_element_desc(elem_desc);
      coll->set_not_null(info->not_null_);
      if (OB_FAIL(fill_element(expr, ctx, res_datum, coll,
                               ns, collection_type, subquery_row,
                               subquery_ctx, subquery_iter))) {
        LOG_WARN("failed to fill element", K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObExprCast::fill_element(const sql::ObExpr &expr,
                             ObEvalCtx &ctx,
                             sql::ObDatum &res_datum,
                             pl::ObPLCollection *coll,
                             pl::ObPLINS *ns,
                             const pl::ObCollectionType *collection_type,
                             ObExpr **subquery_row,
                             ObEvalCtx *subquery_ctx,
                             ObSubQueryIterator *subquery_iter)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
#else
  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();
  ObDatum *subquery_datum = NULL;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObSEArray<ObObj, 4> data_arr;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObExprCast::CastMultisetExtraInfo *info =
    static_cast<const ObExprCast::CastMultisetExtraInfo *>(expr.extra_info_);

  // fetch the subquery result
  while (OB_SUCC(ret) && OB_SUCC(subquery_iter->get_next_row())) {
    if (OB_FAIL(subquery_row[0]->eval(*subquery_ctx, subquery_datum))) {
      LOG_WARN("failed to eval subquery", K(ret));
    } else {
      const ObDatum &d = *subquery_datum;
      ObObj v, v2;
      if (OB_FAIL(d.to_obj(v, subquery_row[0]->obj_meta_, subquery_row[0]->obj_datum_map_))) {
        LOG_WARN("failed to get obj", K(ret), K(d));
      } else if (info->not_null_) {
        if (v.is_null()) {
          ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
          LOG_WARN("not null check violated", K(ret));
        } else if (info->elem_type_.get_meta_type().is_ext()) {
          ObObjParam v1 = v;
          if (OB_FAIL(ObSPIService::spi_check_composite_not_null(&v1))) {
            LOG_WARN("failed to check not null", K(ret));
          }
        }
      }

      if (info->elem_type_.get_meta_type().is_ext() && !v.is_null()) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(alloc,
                                                 v,
                                                 v2,
                                                 true)) ;
      } else {
        OZ (deep_copy_obj(alloc, v, v2));
      }

      OZ (data_arr.push_back(v2));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  // put the subquery result into the collection
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                                      ns,
                                                      alloc,
                                                      *coll,
                                                      data_arr.count()))) {
    LOG_WARN("failed to set collection", K(ret));
  } else if (0 != data_arr.count()) {
    if (OB_ISNULL(coll->get_data()) ||
        OB_ISNULL(coll->get_allocator()) ||
        OB_ISNULL(ns) ||
        OB_ISNULL(collection_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KPC(coll), K(ns), KPC(collection_type));
    } else if (info->elem_type_.get_meta_type().is_ext()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < data_arr.count(); ++i) {
        ObObj &v = data_arr.at(i);
        if (OB_SUCC(ret)) {
          if (v.is_null()) {
            // construct a null composite obj
            ObObj new_composite;
            int64_t ptr = 0;
            int64_t init_size = OB_INVALID_SIZE;
            if (OB_FAIL(collection_type->get_element_type().newx(*coll->get_allocator(), ns, ptr))) {
              LOG_WARN("failed to new element", K(ret));
            } else if (OB_FAIL(collection_type->get_element_type().get_size(pl::PL_TYPE_INIT_SIZE,
                                                                            init_size))) {
              LOG_WARN("failed to get size", K(ret));
            } else {
              new_composite.set_extend(ptr, collection_type->get_element_type().get_type(), init_size);
              static_cast<ObObj*>(coll->get_data())[i] = new_composite;
            }
          } else {
            static_cast<ObObj*>(coll->get_data())[i] = v;
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < data_arr.count(); ++i) {
        ObObj &v = data_arr.at(i);
        if (OB_FAIL(ObSPIService::spi_pad_char_or_varchar(session,
                                                          info->elem_type_.get_obj_type(),
                                                          info->elem_type_.get_accuracy(),
                                                          coll->get_allocator(),
                                                          &v))) {
          LOG_WARN("failed to pad", K(ret));
        } else {
          static_cast<ObObj*>(coll->get_data())[i] = v;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObObj result;
    result.set_extend(reinterpret_cast<int64_t>(coll), coll->get_type());
    //Collection constructed here must be recorded and destructed at last
    if (OB_FAIL(res_datum.from_obj(result, expr.obj_datum_map_))) {
      LOG_WARN("failed to from obj", K(ret));
    } else if (OB_ISNULL(exec_ctx.get_pl_ctx()) && OB_FAIL(exec_ctx.init_pl_ctx())) {
      LOG_WARN("failed to init pl ctx", K(ret));
    } else if (OB_ISNULL(exec_ctx.get_pl_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(exec_ctx.get_pl_ctx()->add(result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }
#endif
  return ret;
}

int ObExprCast::eval_cast_multiset(const sql::ObExpr &expr,
                                   sql::ObEvalCtx &ctx,
                                   sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "eval cast multiset");
#else
  ObObj result;
  // ObObj *data_arr = NULL;
  ObExpr **subquery_row = NULL;
  ObEvalCtx *subquery_ctx = NULL;
  ObSubQueryIterator *subquery_iter = NULL;
  if (OB_FAIL(get_subquery_iter(expr, ctx, subquery_row,
                                  subquery_ctx, subquery_iter))) {
    LOG_WARN("failed to eval subquery", K(ret));
  } else if (OB_FAIL(construct_collection(expr, ctx, res_datum, subquery_row,
                                          subquery_ctx, subquery_iter))) {
    LOG_WARN("failed to construct collection", K(ret));
  }
#endif
  return ret;
}

int ObExprCast::cg_cast_multiset(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "cast multiset");
#else
  ObSchemaChecker schema_checker;
  const share::schema::ObUDTTypeInfo *dest_info = NULL;
  const int udt_id = raw_expr.get_udt_id();
  const uint64_t dest_tenant_id = pl::get_tenant_id_by_object_id(udt_id);
  const pl::ObUserDefinedType *pl_type = NULL;
  const pl::ObCollectionType *coll_type = NULL;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  if (OB_ISNULL(op_cg_ctx.schema_guard_) || OB_ISNULL(op_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(op_cg_ctx.schema_guard_), K(op_cg_ctx.session_));
  } else if (OB_FAIL(schema_checker.init(*op_cg_ctx.schema_guard_, op_cg_ctx.session_->get_sessid()))) {
    LOG_WARN("init schema checker failed", K(ret));
  } else if (OB_FAIL(schema_checker.get_udt_info(dest_tenant_id, udt_id, dest_info))) {
    LOG_WARN("failed to get udt info", K(ret));
  } else if (OB_ISNULL(dest_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null udt info", K(ret));
  } else if (OB_FAIL(dest_info->transform_to_pl_type(alloc, pl_type))) {
    LOG_WARN("failed to get pl type", K(ret));
  } else if (!pl_type->is_collection_type() ||
             OB_ISNULL(coll_type = static_cast<const pl::ObCollectionType *>(pl_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected pl type", K(ret), KPC(pl_type));
  } else {
    CastMultisetExtraInfo *info = OB_NEWx(CastMultisetExtraInfo, (&alloc), alloc, T_FUN_SYS_CAST);
    info->pl_type_ = coll_type->get_type();
    info->not_null_ = coll_type->get_element_type().get_not_null();
    if (coll_type->get_element_type().is_obj_type()) {
      CK(OB_NOT_NULL(coll_type->get_element_type().get_data_type()));
      OX(info->elem_type_ = *coll_type->get_element_type().get_data_type());
    } else {
      info->elem_type_.set_obj_type(ObExtendType);
    }
    info->capacity_ = coll_type->is_varray_type() ?
                      static_cast<const pl::ObVArrayType*>(coll_type)->get_capacity() :
                      OB_INVALID_SIZE;
    info->udt_id_ = udt_id;
    rt_expr.eval_func_ = eval_cast_multiset;
    rt_expr.eval_batch_func_ = cast_eval_arg_batch;
    rt_expr.extra_info_ = info;
  }
#endif
  return ret;
}

OB_SERIALIZE_MEMBER(ObExprCast::CastMultisetExtraInfo,
                    pl_type_, not_null_, elem_type_, capacity_, udt_id_);

int ObExprCast::CastMultisetExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                                    const ObExprOperatorType type,
                                                    ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  CastMultisetExtraInfo &other = *static_cast<CastMultisetExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    other = *this;
  }
  return ret;
}


int ObExprCast::cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(2 == rt_expr.arg_cnt_);
  OB_ASSERT(NULL != rt_expr.args_);
  OB_ASSERT(NULL != rt_expr.args_[0]);
  OB_ASSERT(NULL != rt_expr.args_[1]);
  ObObjType in_type = rt_expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = rt_expr.args_[0]->datum_meta_.cs_type_;
  ObObjType out_type = rt_expr.datum_meta_.type_;
  ObCollationType out_cs_type = rt_expr.datum_meta_.cs_type_;

  bool fast_cast_decint = false;
  ObPrecision out_prec = rt_expr.datum_meta_.precision_;
  ObScale out_scale = rt_expr.datum_meta_.scale_;
  ObPrecision in_prec = rt_expr.args_[0]->datum_meta_.precision_;
  ObScale in_scale = rt_expr.args_[0]->datum_meta_.scale_;
  if (ob_is_integer_type(in_type)) {
    in_scale = 0;
    in_prec = ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][in_type].get_precision();
  }
  // suppose we have (P1, S1) -> (P2, S2)
  // if S2 > S1 && P1 + S2 - S1 <= P2, sizeof(result_type) is wide enough to store result value
  // if S2 <= S1 && width_of_prec(P2) >= width_of_prec(P1), result type is wide enough
  if (ob_is_decimal_int_tc(out_type) && !CM_IS_CONST_TO_DECIMAL_INT(raw_expr.get_extra())
      && (ob_is_int_tc(in_type) || ob_is_uint_tc(in_type) || ob_is_decimal_int_tc(in_type))) {
    if ((out_scale > in_scale && (in_prec + out_scale - in_scale <= out_prec))
        || (out_scale <= in_scale
            && get_decimalint_type(out_prec) >= get_decimalint_type(in_prec))) {
      fast_cast_decint = true;
    }
  }
  LOG_DEBUG("fast decimal int cast", K(fast_cast_decint),
            K(in_prec), K(in_scale), K(out_prec), K(out_scale));

  if (OB_UNLIKELY(ObMaxType == in_type || ObMaxType == out_type) ||
      OB_ISNULL(op_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_type or out_type or allocator is invalid", K(ret),
             K(in_type), K(out_type), KP(op_cg_ctx.allocator_));
  } else {
    // setup cast mode for explicit cast.
    // 隐式cast的cast mode在创建cast expr时已经被设置好了，直接从raw_expr.get_extra()里拿
    ObCastMode cast_mode = raw_expr.get_extra();
    if (cast_mode & CM_ZERO_FILL) {
      // 将zerofill信息放在scale里面
      const ObRawExpr *src_raw_expr = NULL;
      CK(OB_NOT_NULL(src_raw_expr = raw_expr.get_param_expr(0)));
      if (OB_SUCC(ret)) {
        const ObExprResType &src_res_type = src_raw_expr->get_result_type();
        if (ob_is_string_or_lob_type(in_type)) {
          // do nothing, setting the zerofill length only makes sense when in_type is a numeric type
        } else if (OB_UNLIKELY(UINT_MAX8 < src_res_type.get_length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected zerofill length", K(ret), K(src_res_type.get_length()));
        } else if (ob_is_string_or_lob_type(out_type)) {
          // The zerofill information will only be used when cast to string/lob type.
          // for these types, scale is unused, so the previous design is to save child length
          // to the scale of the rt_expr.
          rt_expr.datum_meta_.scale_ = static_cast<int8_t>(src_res_type.get_length());
        }
      }
    }
    rt_expr.is_called_in_sql_ = is_called_in_sql();
    if (OB_SUCC(ret)) {
      bool just_eval_arg = false;
      const ObRawExpr *src_raw_expr = raw_expr.get_param_expr(0);
      if (OB_ISNULL(src_raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (src_raw_expr->is_multiset_expr()) {
        if (OB_FAIL(cg_cast_multiset(op_cg_ctx, raw_expr, rt_expr))) {
          LOG_WARN("failed to cg cast multiset", K(ret));
        }
      } else if (fast_cast_decint) {
        if (CM_IS_EXPLICIT_CAST(cast_mode)) {
          ObDatumCast::get_decint_cast(ob_obj_type_class(in_type), in_prec, in_scale, out_prec,
                                       out_scale, true, rt_expr.eval_batch_func_,
                                       rt_expr.eval_func_);
        } else {
          ObDatumCast::get_decint_cast(ob_obj_type_class(in_type), in_prec, in_scale, out_prec,
                                       out_scale, false, rt_expr.eval_batch_func_,
                                       rt_expr.eval_func_);
        }
        OB_ASSERT(rt_expr.eval_func_ != nullptr);
        OB_ASSERT(rt_expr.eval_batch_func_ != nullptr);
      } else {
        if (OB_FAIL(ObDatumCast::choose_cast_function(in_type, in_cs_type, out_type, out_cs_type,
                                                      cast_mode, *(op_cg_ctx.allocator_),
                                                      just_eval_arg, rt_expr))) {
          LOG_WARN("choose_cast_func failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_E(EventTable::EN_ENABLE_VECTOR_CAST) OB_SUCCESS;
        rt_expr.eval_vector_func_ =
          tmp_ret == OB_SUCCESS ?
            VectorCasterUtil::get_vector_cast(rt_expr.args_[0]->get_vec_value_tc(),
                                              rt_expr.get_vec_value_tc(), just_eval_arg,
                                              rt_expr.eval_func_, cast_mode) :
            nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.extra_ = cast_mode;
    }
  }
  return ret;
}

int ObExprCast::do_implicit_cast(ObExprCtx &expr_ctx,
                                 const ObCastMode &cast_mode,
                                 const ObObjType &dst_type,
                                 const ObObj &src_obj,
                                 ObObj &dst_obj) const
{
  int ret = OB_SUCCESS;

  EXPR_SET_CAST_CTX_MODE(expr_ctx);
  const ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);
  ObCastCtx cast_ctx((expr_ctx).calc_buf_,
                      &dtc_params,
                      (expr_ctx).phy_plan_ctx_->get_cur_time().get_datetime(),
                      (expr_ctx).cast_mode_ | (cast_mode),
                      result_type_.get_collation_type());
  const ObObj *obj_ptr = NULL;
  ObObj tmp_obj;
  if(OB_FAIL(ret)) {
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

int ObExprCast::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (exprs.count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param num", K(ret), K(exprs.count()));
  } else if (OB_ISNULL(exprs.at(0)) || OB_ISNULL(exprs.at(1)) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(exprs.at(0)), K(exprs.at(1)));
  } else {
    ObObjType src = exprs.at(0)->get_result_type().get_type();
    ObObjType dst = expr->get_result_type().get_type();
    if (ObTimeType == src && ObTimeType != dst && ob_is_temporal_type(dst)) {
      is_valid = false;
    } else {
      is_valid = true;
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprCast, raw_expr) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(raw_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else {
    ObObjType src = raw_expr->get_param_expr(0)->get_result_type().get_type();
    ObObjType dst = raw_expr->get_result_type().get_type();
    if (is_mysql_mode()) {
      SET_LOCAL_SYSVAR_CAPACITY(3);
      EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    } else {
      SET_LOCAL_SYSVAR_CAPACITY(5);
    }
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
    if (ob_is_datetime_tc(src)
        || ob_is_datetime_tc(dst)
        || ob_is_otimestampe_tc(src)
        || ob_is_otimestampe_tc(dst)) {
      EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
    }
    if (is_oracle_mode()
        && (ob_is_string_type(src)
            || ob_is_string_type(dst))
        && (ob_is_temporal_type(src)
            || ob_is_otimestamp_type(src)
            || ob_is_temporal_type(dst)
            || ob_is_otimestamp_type(dst))) {
      EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_DATE_FORMAT);
      EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_FORMAT);
      EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprCast)
{
  int ret = OB_SUCCESS;
  ret = ObExprOperator::serialize_(buf, buf_len, pos);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprCast)
{
  int ret = OB_SUCCESS;
  ret = ObExprOperator::deserialize_(buf, data_len, pos);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprCast)
{
  int64_t len = 0;
  len = ObExprOperator::get_serialize_size_();
  return len;
}

}
}

