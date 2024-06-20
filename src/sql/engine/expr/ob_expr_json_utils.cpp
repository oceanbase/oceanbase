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
 * This file is for implement of func json expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_json_param_type.h"
#include "ob_expr_json_func_helper.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "sql/ob_result_set.h"
#include "sql/ob_spi.h"
#include "ob_expr_json_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

#define CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((stmt))))))

#define GET_SESSION()                                           \
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session(); \
  if (OB_ISNULL(session)) {                                     \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("session is NULL", K(ret));                        \
  } else

int ObExprJsonQueryParamInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    ObExprJsonQueryParamInfo& other = *static_cast<ObExprJsonQueryParamInfo *>(copied_info);
    other.truncate_ = truncate_;
    other.format_json_ = format_json_;
    other.wrapper_ = wrapper_;
    other.empty_type_ = empty_type_;
    other.error_type_ = error_type_;
    other.pretty_type_ = pretty_type_;
    other.ascii_type_ = ascii_type_;
    other.scalars_type_ = scalars_type_;
    other.j_path_ = NULL;
    if (OB_FAIL(ob_write_string(allocator, path_str_, other.path_str_, true))) {
      LOG_WARN("fail to deep copy path str", K(ret));
    } else if (OB_FAIL(other.on_mismatch_.assign(on_mismatch_))) {
      LOG_WARN("fail to assign mismatch array", K(ret));
    } else if (OB_FAIL(other.on_mismatch_type_.assign(on_mismatch_type_))) {
      LOG_WARN("fail to assgin mismatch type", K(ret));
    } else if (OB_FAIL(other.parse_json_path(path_str_, other.j_path_))) {
      LOG_WARN("fail to resolve json path", K(ret));
    }
  }
  return ret;
}

int ObExprJsonQueryParamInfo::parse_json_path(ObString path_str, ObJsonPath*& j_path_)
{
  INIT_SUCC(ret);
  j_path_ = NULL;
  void* buf = allocator_.alloc(sizeof(ObJsonPath));
  if (path_str.empty()) {
    allocator_.free(buf);
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc path.", K(ret));
  } else {
    j_path_ = new (buf) ObJsonPath(path_str, &allocator_);
    if (OB_FAIL(j_path_->parse_path())) {
      LOG_WARN("wrong path expression, parse path failed or with wildcards", K(ret), K(path_str));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprJsonQueryParamInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              truncate_,
              format_json_,
              wrapper_,
              empty_type_,
              error_type_,
              pretty_type_,
              ascii_type_,
              scalars_type_,
              path_str_,
              on_mismatch_,
              on_mismatch_type_);
  return len;
}

OB_DEF_SERIALIZE(ObExprJsonQueryParamInfo)
{
  INIT_SUCC(ret);
  LST_DO_CODE(OB_UNIS_ENCODE,
              truncate_,
              format_json_,
              wrapper_,
              empty_type_,
              error_type_,
              pretty_type_,
              ascii_type_,
              scalars_type_,
              path_str_,
              on_mismatch_,
              on_mismatch_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprJsonQueryParamInfo)
{
  INIT_SUCC(ret);
  LST_DO_CODE(OB_UNIS_DECODE,
              truncate_,
              format_json_,
              wrapper_,
              empty_type_,
              error_type_,
              pretty_type_,
              ascii_type_,
              scalars_type_,
              path_str_,
              on_mismatch_,
              on_mismatch_type_);
  OZ(parse_json_path(path_str_, j_path_));
  return ret;
}

int ObJsonUtil::set_mismatch_val(ObIArray<int8_t>& val, ObIArray<int8_t>& type, int64_t& opt_val, uint32_t& pos)
{
  INIT_SUCC(ret);
  if (opt_val >= OB_JSON_ON_MISMATCH_ERROR &&
      opt_val <= OB_JSON_ON_MISMATCH_IMPLICIT) {
    pos ++;
    if (OB_FAIL(val.push_back(static_cast<int8_t>(opt_val)))) {
      LOG_WARN("mismtach add fail", K(ret));
    } else if (OB_FAIL(type.push_back(0))) {
      LOG_WARN("mismatch option add fail", K(ret));
    }
  } else if (opt_val >= OB_JSON_TYPE_MISSING_DATA &&
              opt_val <= OB_JSON_TYPE_DOT) {

    /* one mismatch val has multi mismatch type*/
    uint8_t t_value = type.at(pos);
    type.pop_back();
    switch(opt_val) {
      case OB_JSON_TYPE_MISSING_DATA :{
        t_value |= 1;
        break;
      }
      case OB_JSON_TYPE_EXTRA_DATA :{
        t_value |= 2;
        break;
      }
      case OB_JSON_TYPE_TYPE_ERROR :{
        t_value |= 4;
        break;
      }
      default :{
        break;
      }
    }
    type.push_back(t_value);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("option type error", K(opt_val), K(ret));
  }
  return ret;
}

int ObJsonUtil::init_json_path(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx,
                               const ObRawExpr* path,
                               ObExprJsonQueryParamInfo& res)
{
  INIT_SUCC(ret);
  ObObj const_data;
  bool got_data = false;
  ObExecContext *exec_ctx = op_cg_ctx.session_->get_cur_exec_ctx();
  if (OB_NOT_NULL(path)
      && (path->is_const_expr() || path->is_static_scalar_const_expr())
      && path->get_expr_type() != T_OP_GET_USER_VAR) {
    void* buf = alloc.alloc(sizeof(ObJsonPath));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc path.", K(ret));
    } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                path,
                                                                const_data,
                                                                got_data,
                                                                alloc))) {
      LOG_WARN("failed to calc offset expr", K(ret));
    } else if (!got_data || const_data.is_null()
                || !ob_is_string_type(const_data.get_type())) {
      ret = OB_ERR_INVALID_INPUT_ARGUMENT;
      LOG_WARN("fail to get int value", K(ret));
    } else {
      ObString path_str = const_data.get_string();
      res.j_path_ = new (buf) ObJsonPath(path_str, &alloc);
      if (OB_FAIL(res.j_path_->parse_path())) {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, path_str.length(), path_str.ptr());
        } else {
          ret = OB_ERR_INVALID_JSON_PATH;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
        }
        LOG_WARN("wrong path expression, parse path failed or with wildcards", K(ret), K(path_str));
      } else if (OB_FAIL(ob_write_string(alloc, path_str, res.path_str_, true))) {
        LOG_WARN("fail to deep copy path str", K(ret), K(path_str));
      }
    }
  }
  return ret;
}

int ObJsonUtil::datetime_scale_check(const ObAccuracy &accuracy,
                                     int64_t &value,
                                     bool strict)
{
  INIT_SUCC(ret);
  ObScale scale = accuracy.get_scale();

  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST",
        static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    // first check zero
    if (strict &&
        (value == ObTimeConverter::ZERO_DATE ||
        value == ObTimeConverter::ZERO_DATETIME)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("Zero datetime is invalid in json_value.", K(value));
    } else {
      int64_t temp_value = value;
      ObTimeConverter::round_datetime(scale, temp_value);
      if (strict && temp_value != value) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("Invalid input value.", K(value), K(scale));
      } else if (ObTimeConverter::is_valid_datetime(temp_value)) {
        value = temp_value;
      } else {
        ret = OB_ERR_NULL_VALUE; // set null for res
        LOG_DEBUG("Invalid datetime val, return set_null", K(temp_value));
      }
    }
  }

  return ret;
}

int ObJsonUtil::get_accuracy(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObAccuracy &accuracy,
                             ObObjType &dest_type,
                             bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObDatum *dst_type_dat = NULL;

  if (OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    is_cover_by_error = false;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, dst_type_dat))) {
    is_cover_by_error = false;
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    ret = ObJsonUtil::get_accuracy_internal(accuracy,
                                ctx,
                                dest_type,
                                dst_type_dat->get_int(),
                                expr.datum_meta_.length_semantics_);
  }
  return ret;
}

/*json cast to sql scalar*/
int ObJsonUtil::get_accuracy_internal(ObAccuracy &accuracy,
                                      ObEvalCtx& ctx,
                                      ObObjType &dest_type,
                                      const int64_t value,
                                      const ObLengthSemantics &length_semantics)
{
  INIT_SUCC(ret);
  ParseNode node;
  node.value_ = value;
  dest_type = static_cast<ObObjType>(node.int16_values_[0]);

  if (ObFloatType == dest_type) {
    // boundaries already checked in calc result type
    if (node.int16_values_[OB_NODE_CAST_N_PREC_IDX] > OB_MAX_FLOAT_PRECISION) {
      dest_type = ObDoubleType;
    }
  }
  ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
  if (ObStringTC == dest_tc) {
    // parser will abort all negative number
    // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
    accuracy.set_full_length(node.int32_values_[1], length_semantics,
                              lib::is_oracle_mode());
  } else if (ObRawTC == dest_tc) {
    accuracy.set_length(node.int32_values_[1]);
  } else if(ObTextTC == dest_tc || ObJsonTC == dest_tc) {
    accuracy.set_length(node.int32_values_[1] < 0 ?
        ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length() : node.int32_values_[1]);
  } else if (ObIntervalTC == dest_tc) {
    if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                    !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
      ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      LOG_WARN("Invalid scale.", K(ret), K(node.int16_values_[3]), K(node.int16_values_[2]));
    } else {
      ObScale scale = (dest_type == ObIntervalYMType) ?
        ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(
            static_cast<int8_t>(node.int16_values_[3]))
        : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
            static_cast<int8_t>(node.int16_values_[2]),
            static_cast<int8_t>(node.int16_values_[3]));
      accuracy.set_scale(scale);
    }
  } else {
    const ObAccuracy &def_acc =
      ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][dest_type];
    if (ObNumberType == dest_type && 0 == node.int16_values_[2]) {
      accuracy.set_precision(def_acc.get_precision());
    } else {
      accuracy.set_precision(node.int16_values_[2]);
    }
    accuracy.set_scale(node.int16_values_[3]);
    if (lib::is_oracle_mode() && ObDoubleType == dest_type) {
      accuracy.set_accuracy(def_acc.get_precision());
    }
    if (ObNumberType == dest_type
        && is_decimal_int_accuracy_valid(accuracy.get_precision(), accuracy.get_scale())) {
      bool enable_decimalint = false;
      if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type_ctx.get_session() is null", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(ctx.exec_ctx_.get_my_session(),
                                                             enable_decimalint))) {
        LOG_WARN("fail to check_enable_decimalint_type",
            K(ret), K(ctx.exec_ctx_.get_my_session()->get_effective_tenant_id()));
      } else if (enable_decimalint) {
        dest_type = ObDecimalIntType;
      }
    }
  }

  return ret;
}

int ObJsonUtil::time_scale_check(const ObAccuracy &accuracy, int64_t &value, bool strict)
{
  INIT_SUCC(ret);
  ObScale scale = accuracy.get_scale();

  if (0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL) {
    int64_t temp_value = value;
    ObTimeConverter::round_datetime(scale, temp_value);
    if (strict && temp_value != value) { // round success
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("Invalid input value.", K(value), K(scale));
    } else {
      value = temp_value;
    }
  }

  return ret;
}

static OB_INLINE int get_cast_ret(int ret)
{
  // compatibility for old ob
  if (OB_ERR_UNEXPECTED_TZ_TRANSITION == ret ||
      OB_ERR_UNKNOWN_TIME_ZONE == ret) {
    ret = OB_INVALID_DATE_VALUE;
  }

  return ret;
}

int ObJsonUtil::number_range_check(const ObAccuracy &accuracy,
                                   ObIAllocator *allocator,
                                   number::ObNumber &val,
                                   bool strict)
{
  INIT_SUCC(ret);
  ObPrecision precision = accuracy.get_precision();
  ObScale scale = accuracy.get_scale();
  const number::ObNumber *min_check_num = NULL;
  const number::ObNumber *max_check_num = NULL;
  const number::ObNumber *min_num_mysql = NULL;
  const number::ObNumber *max_num_mysql = NULL;
  bool is_finish = false;
  if (lib::is_oracle_mode()) {
    if (OB_MAX_NUMBER_PRECISION >= precision
        && precision >= OB_MIN_NUMBER_PRECISION
        && number::ObNumber::MAX_SCALE >= scale
        && scale >= number::ObNumber::MIN_SCALE) {
      min_check_num = &(ObNumberConstValue::ORACLE_CHECK_MIN[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
      max_check_num = &(ObNumberConstValue::ORACLE_CHECK_MAX[precision][scale + ObNumberConstValue::MAX_ORACLE_SCALE_DELTA]);
    } else if (ORA_NUMBER_SCALE_UNKNOWN_YET == scale
                && PRECISION_UNKNOWN_YET == precision) {
      is_finish = true;
    } else if (PRECISION_UNKNOWN_YET == precision
              && number::ObNumber::MAX_SCALE >= scale
              && scale >= number::ObNumber::MIN_SCALE) {
      number::ObNumber num;
      if (OB_FAIL(num.from(val, *allocator))) {
      } else if (OB_FAIL(num.round(scale))) {
      } else if (val.compare(num) != 0) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("input value is out of range.", K(scale), K(val));
      } else {
        is_finish = true;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
    }
  } else {
    if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
      LOG_WARN("Invalid accuracy.", K(ret), K(scale), K(precision));
    } else if (number::ObNumber::MAX_PRECISION >= precision
        && precision >= OB_MIN_DECIMAL_PRECISION
        && number::ObNumber::MAX_SCALE >= scale
        && scale >= 0) {
      min_check_num = &(ObNumberConstValue::MYSQL_CHECK_MIN[precision][scale]);
      max_check_num = &(ObNumberConstValue::MYSQL_CHECK_MAX[precision][scale]);
      min_num_mysql = &(ObNumberConstValue::MYSQL_MIN[precision][scale]);
      max_num_mysql = &(ObNumberConstValue::MYSQL_MAX[precision][scale]);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    if (OB_ISNULL(min_check_num) || OB_ISNULL(max_check_num)
        || (!lib::is_oracle_mode()
          && (OB_ISNULL(min_num_mysql) || OB_ISNULL(max_num_mysql)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min_num or max_num is null", K(ret), KPC(min_check_num), KPC(max_check_num));
    } else if (val <= *min_check_num) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        ret = OB_DATA_OUT_OF_RANGE;
      }
      LOG_WARN("val is out of min range check.", K(val), K(*min_check_num));
      is_finish = true;
    } else if (val >= *max_check_num) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        ret = OB_DATA_OUT_OF_RANGE;
      }
      LOG_WARN("val is out of max range check.", K(val), K(*max_check_num));
      is_finish = true;
    } else {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(val, tmp_alloc))) {
      } else if (OB_FAIL(num.round(scale))) {
        LOG_WARN("num.round failed", K(ret), K(scale));
      } else {
        if (strict) {
          if (num.compare(val) != 0) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_WARN("input value is out of range.", K(scale), K(val));
          } else {
            is_finish = true;
          }
        } else {
          if (OB_ISNULL(allocator)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("allocator is null", K(ret));
          } else if (OB_FAIL(val.deep_copy_v3(num, *allocator))) {
            LOG_WARN("val.deep_copy_v3 failed", K(ret), K(num));
          } else {
            is_finish = true;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected situation, res is not set", K(ret));
  }
  LOG_DEBUG("number_range_check_v2 done", K(ret), K(is_finish), K(accuracy), K(val),
            KPC(min_check_num), KPC(max_check_num));

  return ret;
}

int ObJsonUtil::set_lob_datum(common::ObIAllocator *allocator,
                              const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObObjType dst_type,
                              uint8_t ascii_type,
                              ObDatum &res)
{
  INIT_SUCC(ret);
  if (res.is_null()) { // null value jump this process
  } else {
    switch (dst_type) {
      case ObVarcharType:
      case ObRawType:
      case ObNVarchar2Type:
      case ObNCharType:
      case ObCharType:
      case ObTinyTextType:
      case ObTextType :
      case ObMediumTextType:
      case ObHexStringType:
      case ObLongTextType: {
        ObString val;
        val = res.get_string();
        ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
        if (OB_FAIL(ret)) {
        } else if (ascii_type == 0) {
          if (OB_FAIL(text_result.init(val.length()))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(text_result.append(val))) {
            LOG_WARN("failed to append realdata", K(ret), K(val), K(text_result));
          }
        } else {
          char *buf = NULL;
          int64_t buf_len = val.length() * ObCharset::MAX_MB_LEN * 2;
          int64_t reserve_len = 0;
          int32_t length = 0;

          if (OB_FAIL(text_result.init(buf_len))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(text_result.get_reserved_buffer(buf, reserve_len))) {
            LOG_WARN("fail to get reserved buffer", K(ret));
          } else if (reserve_len != buf_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get reserve len is invalid", K(ret), K(reserve_len), K(buf_len));
          } else if (OB_FAIL(ObJsonExprHelper::calc_asciistr_in_expr(val, expr.args_[0]->datum_meta_.cs_type_,
                                                                    expr.datum_meta_.cs_type_,
                                                                    buf, reserve_len, length))) {
            LOG_WARN("fail to calc unistr", K(ret));
          } else if (OB_FAIL(text_result.lseek(length, 0))) {
            LOG_WARN("text_result lseek failed", K(ret), K(text_result), K(length));
          }
        }
        if (OB_SUCC(ret)) {
          // old engine set same alloctor for wrapper, so we can use val without copy
          text_result.set_result();
        }
        break;
      }
      case ObJsonType: {
        ObString out_val;
        out_val = res.get_string();
        ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(text_result.init(out_val.length()))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(text_result.append(out_val))) {
            LOG_WARN("failed to append realdata", K(ret), K(out_val), K(text_result));
          } else {
            text_result.set_result();
          }
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObJsonUtil::bit_length_check(const ObAccuracy &accuracy,
                                 uint64_t &value)
{
  int ret = OB_SUCCESS;
  int32_t bit_len = 0;
  int32_t dst_bit_len = accuracy.get_precision();
  bit_len = ObJsonBaseUtil::get_bit_len(value);
  if(OB_UNLIKELY(bit_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit length is negative", K(ret), K(value), K(bit_len));
  } else {
    if (OB_UNLIKELY(bit_len > dst_bit_len)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("bit type length is too long", K(ret), K(bit_len),
          K(dst_bit_len), K(value));
    }
  }
  return ret;
}

// padding %padding_cnt character, we also need to convert collation type here.
// eg: select cast('abc' as nchar(100)) from dual;
//     the space must be in utf16, because dst_type is nchar
int ObJsonUtil::padding_char_for_cast(int64_t padding_cnt,
                                      const ObCollationType &padding_cs_type,
                                      ObIAllocator &alloc,
                                      ObString &padding_res)
{
  int ret = OB_SUCCESS;
  padding_res.reset();
  const ObCharsetType &cs = ObCharset::charset_type_by_coll(padding_cs_type);
  char padding_char = (CHARSET_BINARY == cs) ? OB_PADDING_BINARY : OB_PADDING_CHAR;
  int64_t padding_str_size = sizeof(padding_char) * padding_cnt;
  char *padding_str_ptr = reinterpret_cast<char*>(alloc.alloc(padding_str_size));
  if (OB_ISNULL(padding_str_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else if (CHARSET_BINARY == cs) {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    padding_res.assign_ptr(padding_str_ptr, padding_str_size);
  } else {
    MEMSET(padding_str_ptr, padding_char, padding_str_size);
    ObString padding_str(padding_str_size, padding_str_ptr);
    if (OB_FAIL(ObExprUtil::convert_string_collation(padding_str,
                                                     ObCharset::get_system_collation(),
                                                     padding_res,
                                                     padding_cs_type,
                                                     alloc))) {
      LOG_WARN("convert padding str collation faield", K(ret), K(padding_str),
                K(padding_cs_type));
    }
  }
  LOG_DEBUG("pad char done", K(ret), K(padding_cnt), K(padding_cs_type), K(padding_res));
  return ret;
}

int cast_to_null(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(j_base);
  UNUSED(accuracy);
  UNUSED(cast_param);
  UNUSED(is_type_mismatch);
  if (!cast_param.is_only_check_) {
    res.set_null();
  }
  return ret;
}

int cast_to_int(common::ObIAllocator *allocator,
                ObEvalCtx &ctx,
                ObIJsonBase *j_base,
                common::ObAccuracy &accuracy,
                ObJsonCastParam &cast_param,
                ObDatum &res,
                uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(is_type_mismatch);
  int64_t val = 0;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_int(val, true))) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SIGNED", "json_value");
    LOG_WARN("cast to int failed", K(ret), K(*j_base));
  } else if (cast_param.dst_type_ < ObIntType &&
    CAST_FAIL(int_range_check(cast_param.dst_type_, val, val))) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SIGNED", "json_value");
  } else if (!cast_param.is_only_check_) {
    if (cast_param.dst_type_ == ObIntType) {
      res.set_int(val);
    } else {
      res.set_int32(static_cast<int32_t>(val));
    }
  }
  return ret;
}

template<>
void ObJsonUtil::wrapper_set_uint(ObObjType type, uint64_t val, ObObj& obj)
{
  obj.set_uint(type, val);
}

template<>
void ObJsonUtil::wrapper_set_uint(ObObjType type, uint64_t val, ObDatum& obj)
{
  obj.set_uint(val);
}

template<>
void ObJsonUtil::wrapper_set_timestamp_tz(ObObjType type, ObOTimestampData val, ObObj& res)
{
  if (type == ObTimestampTZType) {
    res.set_timestamp_tz(val);
  } else {
    res.set_timestamp_ltz(val);
  }
}

template<>
void ObJsonUtil::wrapper_set_timestamp_tz(ObObjType type, ObOTimestampData val, ObDatum& res)
{
  if (type == ObTimestampTZType) {
    res.set_otimestamp_tz(val);
  } else {
    res.set_otimestamp_tiny(val);
  }
}

template<>
void ObJsonUtil::wrapper_set_decimal_int(const common::ObDecimalInt *decint, ObScale scale, int32_t int_bytes, ObDatum& res)
{
  res.set_decimal_int(decint, int_bytes);
}

template<>
void ObJsonUtil::wrapper_set_decimal_int(const common::ObDecimalInt *decint, ObScale scale, int32_t int_bytes, ObObj& res)
{
  res.set_decimal_int(int_bytes, scale, const_cast<common::ObDecimalInt *>(decint));
}

template<>
void ObJsonUtil::wrapper_set_string(ObObjType type, ObString& val, ObObj& obj)
{
  obj.set_string(type, val);
}

template<>
void ObJsonUtil::wrapper_set_string(ObObjType type, ObString& val, ObDatum& obj)
{
  obj.set_string(val);
}

int cast_to_uint(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(is_type_mismatch);
  uint64_t val = 0;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_uint(val, true, true))) {
    LOG_WARN("cast to uint failed", K(ret), K(*j_base));
    if (ret == OB_OPERATE_OVERFLOW) {
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "UNSIGNED", "json_value");
    }
  } else if (cast_param.dst_type_ < ObUInt64Type &&
    CAST_FAIL(uint_upper_check(cast_param.dst_type_, val))) {
    LOG_WARN("uint_upper_check failed", K(ret));
  } else if (!cast_param.is_only_check_) {
    if (cast_param.dst_type_ == ObUInt64Type) {
      ObJsonUtil::wrapper_set_uint(cast_param.dst_type_, val, res);
      res.set_uint(val);
    } else {
      res.set_uint32(static_cast<uint32_t>(val));
    }
  }

  return ret;
}

int cast_to_string(common::ObIAllocator *allocator,
                   ObEvalCtx &ctx,
                   ObIJsonBase *j_base,
                   common::ObAccuracy &accuracy,
                   ObJsonCastParam &cast_param,
                   ObDatum &res,
                   uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(ctx);
  ObString val;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ObJsonBuffer j_buf(allocator);
    if (CAST_FAIL(j_base->print(j_buf, cast_param.is_quote_, cast_param.is_pretty_))) {
      is_type_mismatch = 1;
      LOG_WARN("fail to_string as json", K(ret));
    } else {
      ObObjType in_type = ObLongTextType;
      ObString temp_str_val(j_buf.length(), j_buf.ptr());
      bool is_need_string_string_convert = ((CS_TYPE_BINARY == cast_param.dst_coll_type_)
                          || (ObCharset::charset_type_by_coll(cast_param.in_coll_type_) !=
                              ObCharset::charset_type_by_coll(cast_param.dst_coll_type_)))
                              && !(lib::is_mysql_mode() && temp_str_val.length() == 0);
      if (is_need_string_string_convert) {
        if (CS_TYPE_BINARY != cast_param.in_coll_type_
            && CS_TYPE_BINARY != cast_param.dst_coll_type_
            && (ObCharset::charset_type_by_coll(cast_param.in_coll_type_) !=
            ObCharset::charset_type_by_coll(cast_param.dst_coll_type_))) {
          char *buf = NULL;
          int64_t buf_len = (temp_str_val.length() == 0 ? 1 : temp_str_val.length()) * ObCharset::CharConvertFactorNum;
          uint32_t result_len = 0;
          buf = reinterpret_cast<char*>(allocator->alloc(buf_len));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(cast_param.in_coll_type_, temp_str_val.ptr(),
                                                        temp_str_val.length(), cast_param.dst_coll_type_, buf,
                                                        buf_len, result_len))) {
            LOG_WARN("charset convert failed", K(ret));
          } else {
            val.assign_ptr(buf, result_len);
          }
        } else {
          if (CS_TYPE_BINARY == cast_param.in_coll_type_ || CS_TYPE_BINARY == cast_param.dst_coll_type_) {
            // just copy string when in_cs_type or out_cs_type is binary
            const ObCharsetInfo *cs = NULL;
            int64_t align_offset = 0;
            if (CS_TYPE_BINARY == cast_param.in_coll_type_ && lib::is_mysql_mode()
                && (NULL != (cs = ObCharset::get_charset(cast_param.dst_coll_type_)))) {
              if (cs->mbminlen > 0 && temp_str_val.length() % cs->mbminlen != 0) {
                align_offset = cs->mbminlen - temp_str_val.length() % cs->mbminlen;
              }
            }
            int64_t len = align_offset + temp_str_val.length();
            char *buf = reinterpret_cast<char*>(allocator->alloc(len));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMMOVE(buf + align_offset, temp_str_val.ptr(), len - align_offset);
              MEMSET(buf, 0, align_offset);
              val.assign_ptr(buf, len);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("same charset should not be here, just use cast_eval_arg", K(ret),
                K(in_type), K(cast_param.dst_type_), K(cast_param.in_coll_type_), K(cast_param.dst_coll_type_));
          }
        }
      } else {
        val.assign_ptr(temp_str_val.ptr(), temp_str_val.length());
      }

      ObLengthSemantics senmactics = accuracy.get_length_semantics();
      // do str length check
      const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(
        senmactics == LS_BYTE ? CS_TYPE_BINARY : cast_param.dst_coll_type_, val.ptr(), val.length()));
      ObLength max_accuracy_len;
      if (lib::is_oracle_mode()) {
        max_accuracy_len =  (cast_param.dst_type_ == ObLongTextType) ? OB_MAX_LONGTEXT_LENGTH : accuracy.get_length();
      } else { // mysql mode
        max_accuracy_len = (ob_obj_type_class(cast_param.dst_type_) == ObTextTC)
                                ? ObAccuracy::DDL_DEFAULT_ACCURACY[cast_param.dst_type_].get_length()
                                    : accuracy.get_length();
      }
      if (max_accuracy_len > 0 && lib::is_oracle_mode()) {
        max_accuracy_len *= (senmactics == LS_BYTE ? 1 : 2);
      }

      uint32_t byte_len = 0;
      byte_len = ObCharset::charpos(senmactics == LS_BYTE ? CS_TYPE_BINARY : cast_param.dst_coll_type_, val.ptr(), str_len_char, max_accuracy_len);

      if (OB_SUCC(ret)) {
        if (max_accuracy_len == DEFAULT_STR_LENGTH) { // default string len
        } else if (cast_param.is_trunc_ && max_accuracy_len < str_len_char) {
          if (!cast_param.is_const_ && (j_base->json_type() == ObJsonNodeType::J_INT
               || j_base->json_type() == ObJsonNodeType::J_UINT
               || j_base->json_type() == ObJsonNodeType::J_BOOLEAN
               || j_base->json_type() == ObJsonNodeType::J_DOUBLE
               || j_base->json_type() == ObJsonNodeType::J_DECIMAL)) {
            ret = OB_ERR_VALUE_EXCEEDED_MAX;
          } else {
            // bugfix:
            // Q1:SELECT c1 ,jt.ww b_c1 FROM t1, json_table ( c2 columns( ww varchar2(2 char) truncate  path '$.a')) jt ;
            // Q2:SELECT c1 ,jt.ww b_c1 FROM t1, json_table ( c2 columns( ww varchar2(2 byte) truncate path '$.a')) jt;
            // should not split in the middle of char
            if (byte_len == 0) { // value has zero length
              val.assign_ptr("", 0);
            } else if (senmactics == LS_BYTE && cast_param.dst_coll_type_ != CS_TYPE_BINARY) {
              int64_t char_len; // not used
              // zero max_accuracy_len not allowed
              byte_len = ObCharset::max_bytes_charpos(cast_param.dst_coll_type_, val.ptr(), str_len_char, max_accuracy_len, char_len);
              if (byte_len == 0) { // buffer not enough for one bytes
                ret = OB_OPERATE_OVERFLOW;
              } else {
                val.assign_ptr(val.ptr(), byte_len);
              }
            } else {
              val.assign_ptr(val.ptr(), byte_len);
            }
          }
        } else if (max_accuracy_len <= 0 || str_len_char > max_accuracy_len) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "STRING", "json expr");
        }
      }
      if (OB_SUCC(ret) && ObCharType == cast_param.dst_type_ && CS_TYPE_BINARY == cast_param.dst_coll_type_) {   // binary need padding
        int64_t text_length = val.length();
        if (max_accuracy_len > text_length) {
          int64_t padding_cnt = max_accuracy_len - text_length;
          ObString padding_res;
          if (OB_FAIL(ObJsonUtil::padding_char_for_cast(padding_cnt, cast_param.dst_coll_type_, *allocator,
                                            padding_res))) {
            LOG_WARN("padding char failed", K(ret), K(padding_cnt), K(cast_param.dst_coll_type_));
          } else {
            int64_t padding_size = padding_res.length() + val.length();
            char *buf = reinterpret_cast<char*>(allocator->alloc(padding_size));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMMOVE(buf, val.ptr(), val.length());
              MEMMOVE(buf + val.length(), padding_res.ptr(), padding_res.length());
              val.assign_ptr(buf, padding_size);
            }
          }
        }
      }
      if (OB_SUCC(ret) && !cast_param.is_only_check_) {
        ObJsonUtil::wrapper_set_string(cast_param.dst_type_, val, res);
        res.set_string(val);
      }
    }
  }

  return ret;
}

bool ObJsonUtil::type_cast_to_string(ObString &json_string,
                                     common::ObIAllocator *allocator,
                                     ObEvalCtx &ctx,
                                     ObIJsonBase *j_base,
                                     common::ObAccuracy &accuracy) {
  int res = 0;
  ObDatum t_res;
  uint8_t is_type_mismatch = false;
  ObJsonCastParam cast_param(ObLongTextType, CS_TYPE_BINARY, CS_TYPE_BINARY, false);
  res = cast_to_string(allocator, ctx, j_base, accuracy, cast_param, t_res, is_type_mismatch);
  if (res == 0) {
    json_string = t_res.get_string();
  }
  return res == 0 ? true : false;
}

int cast_to_datetime(common::ObIAllocator *allocator,
                     ObEvalCtx &ctx,
                     ObIJsonBase *j_base,
                     common::ObAccuracy &accuracy,
                     ObJsonCastParam &cast_param,
                     ObDatum &res,
                     uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(cast_param);
  ObString json_string;
  int64_t val;
  GET_SESSION()
  {
    oceanbase::common::ObTimeConvertCtx cvrt_ctx(session->get_timezone_info(), false);
    if (OB_ISNULL(j_base)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("json base is null", K(ret));
    } else if (lib::is_oracle_mode()) {
      if (OB_FAIL(common_get_nls_format(session, ctx, cast_param.rt_expr_, ObDateTimeType,
                                        true,
                                        cvrt_ctx.oracle_nls_format_))) {
        LOG_WARN("common_get_nls_format failed", K(ret));
      } else if (!j_base->is_json_date(j_base->json_type())
                  && ObJsonUtil::type_cast_to_string(json_string, allocator, ctx, j_base, accuracy) && json_string.length() > 0) {
        ObJsonString json_str(json_string.ptr(),json_string.length());
        if (CAST_FAIL(json_str.to_datetime(val, &cvrt_ctx))) {
          is_type_mismatch = 1;
          LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
        }
      } else if (CAST_FAIL(j_base->to_datetime(val, &cvrt_ctx))) {
        is_type_mismatch = 1;
        LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
      }
      if (OB_SUCC(ret) && CAST_FAIL(ObJsonUtil::datetime_scale_check(accuracy, val))) {
        LOG_WARN("datetime_scale_check failed.", K(ret));
      }
    } else {
      if (CAST_FAIL(j_base->to_datetime(val, &cvrt_ctx))) {
        LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
      } else if (CAST_FAIL(ObJsonUtil::datetime_scale_check(accuracy, val))) {
        LOG_WARN("datetime_scale_check failed.", K(ret));
      }
    }
  }
  if (cast_param.is_only_check_) {
  } else if (ret == OB_ERR_NULL_VALUE) {
    res.set_null();
  } else if (OB_SUCC(ret)) {
    res.set_datetime(val);
  }
  return ret;
}

int cast_to_timstamp(common::ObIAllocator *allocator,
                     ObEvalCtx &ctx,
                     ObIJsonBase *j_base,
                     common::ObAccuracy &accuracy,
                     ObJsonCastParam &cast_param,
                     ObDatum &res,
                     uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  ObOTimestampData out_val;
  int64_t val;
  oceanbase::common::ObTimeConvertCtx cvrt_ctx(NULL, cast_param.dst_type_ == ObTimestampType);
  GET_SESSION()
  {
    if (OB_ISNULL(j_base)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("json base is null", K(ret));
    } else {
      cvrt_ctx.tz_info_ = session->get_timezone_info();
      if (lib::is_oracle_mode()) {
        if (OB_FAIL(common_get_nls_format(session, ctx, cast_param.rt_expr_, ObDateTimeType,
                                          true,
                                          cvrt_ctx.oracle_nls_format_))) {
          LOG_WARN("common_get_nls_format failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (CAST_FAIL(j_base->to_datetime(val, &cvrt_ctx))) {
      is_type_mismatch = 1;
      LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
    } else if (cast_param.dst_type_ == ObTimestampType) {
      out_val.time_us_ = val;
      out_val.time_ctx_.tail_nsec_ = 0;
    } else if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(val, cvrt_ctx.tz_info_, cast_param.dst_type_, out_val))) {
      is_type_mismatch = 1;
      LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(val), K(cast_param.dst_type_));
    }
    if (OB_SUCC(ret)) {
      ObScale scale = accuracy.get_scale();
      if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
        ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, out_val);
        if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_,
            static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
          out_val = ot_data;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid otimestamp, set it null ", K(ot_data), K(scale), "orig_date", out_val);
        }
      }
    }
    if (OB_SUCC(ret) && (!cast_param.is_only_check_)) {
      if (cast_param.dst_type_ == ObTimestampTZType) {
        res.set_otimestamp_tz(out_val);
      } else if (cast_param.dst_type_ == ObTimestampType) {
        res.set_datetime(out_val.time_us_);
      } else {
        res.set_otimestamp_tiny(out_val);
      }
    }
  }
  return ret;
}

int cast_to_date_time(common::ObIAllocator *allocator,
                      ObEvalCtx &ctx,
                      ObIJsonBase *j_base,
                      common::ObAccuracy &accuracy,
                      ObJsonCastParam &cast_param,
                      ObDatum &res,
                      uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  if (cast_param.dst_type_ == oceanbase::common::ObTimestampType)  {
    ret = cast_to_timstamp(allocator, ctx, j_base, accuracy, cast_param, res, is_type_mismatch);
  } else {
    ret = cast_to_datetime(allocator, ctx, j_base, accuracy, cast_param, res, is_type_mismatch);
  }
  return ret;
}

int cast_to_date(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(cast_param);
  UNUSED(ctx);
  UNUSED(accuracy);
  int32_t val;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (j_base->json_type() == ObJsonNodeType::J_NULL) {
    res.set_null();
  } else if (CAST_FAIL(j_base->to_date(val))) {
    is_type_mismatch = 1;
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", "json_value");
  } else if (!cast_param.is_only_check_) {
    res.set_date(val);
  }

  return ret;
}

int cast_to_time(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(is_type_mismatch);
  int64_t val = 0;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_time(val))) {
    if (ret == OB_ERR_UNEXPECTED
        && cast_param.relaxed_time_convert_) {
      if (j_base->json_type() == ObJsonNodeType::J_INT) {
        ret = OB_SUCCESS;
        int64_t in_val = j_base->get_int();
        if (OB_FAIL(ObTimeConverter::int_to_time(in_val, val))) {
          LOG_WARN("int_to_time failed", K(ret), K(in_val), K(val));
        }
      } else if (j_base->json_type() == ObJsonNodeType::J_DOUBLE) {
        // double to time, refer to: datum_cast common_double_time
        ret = OB_SUCCESS;
        double in_val = j_base->get_double();
        char buf[MAX_DOUBLE_PRINT_SIZE];
        MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
        int64_t length = ob_gcvt(in_val, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
        ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
        ObScale res_scale;
        if (CAST_FAIL(ObTimeConverter::str_to_time(str, val, &res_scale))) {
          LOG_WARN("str_to_time failed", K(ret));
        }
      } else {
        LOG_WARN("wrapper to time failed.", K(ret), K(*j_base));
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME", "json_value");
      }
    } else {
      LOG_WARN("wrapper to time failed.", K(ret), K(*j_base));
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME", "json_value");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (CAST_FAIL(ObJsonUtil::time_scale_check(accuracy, val))) {
    LOG_WARN("time_scale_check failed.", K(ret));
  } else if (!cast_param.is_only_check_) {
    res.set_time(val);
  }

  return ret;
}

int cast_to_year(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(cast_param);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(is_type_mismatch);
  // Compatible with mysql.
  // There is no year type in json binary, it is store as a full int.
  // For example, 1901 is stored as 1901, not 01.
  // in mysql 8.0, json is converted to int first, then converted to year.
  // However, json value returning as different behavior to cast expr.
  int64_t int_val;
  uint8_t val = 0;
  const uint16 min_year = 1901;
  const uint16 max_year = 2155;

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_int(int_val))) {
    LOG_WARN("wrapper to year failed.", K(ret), K(*j_base));
  } else if ((lib::is_oracle_mode() || !cast_param.relaxed_time_convert_)
              && (0 != int_val && (int_val < min_year || int_val > max_year))) {
    // different with cast, if 0 < int val < 100, do not add base year
    LOG_DEBUG("int out of year range", K(int_val));
    ret = OB_DATA_OUT_OF_RANGE;
  } else if(CAST_FAIL(ObTimeConverter::int_to_year(int_val, val))) {
    LOG_WARN("int to year failed.", K(ret), K(int_val));
  } else if (!cast_param.is_only_check_) {
    res.set_year(val);
  }

  return ret;
}

int cast_to_float(common::ObIAllocator *allocator,
                  ObEvalCtx &ctx,
                  ObIJsonBase *j_base,
                  common::ObAccuracy &accuracy,
                  ObJsonCastParam &cast_param,
                  ObDatum &res,
                  uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(is_type_mismatch);
  double tmp_val;
  float val = 0;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_double(tmp_val))) {
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
  } else {
    val = static_cast<float>(tmp_val);
    if (lib::is_mysql_mode() && CAST_FAIL(real_range_check(cast_param.dst_type_, tmp_val, val))) {
      LOG_WARN("real_range_check failed", K(ret), K(tmp_val));
    } else if (!cast_param.is_only_check_) {
      res.set_float(val);
    }
  }

  return ret;
}

int cast_to_double(common::ObIAllocator *allocator,
                  ObEvalCtx &ctx,
                  ObIJsonBase *j_base,
                  common::ObAccuracy &accuracy,
                  ObJsonCastParam &cast_param,
                  ObDatum &res,
                  uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(is_type_mismatch);
  double val = 0;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_double(val))) {
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
  } else if (ObUDoubleType == cast_param.dst_type_ && CAST_FAIL(numeric_negative_check(val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(val));
  } else if (!cast_param.is_only_check_) {
    res.set_double(val);
  }

  return ret;
}

int ObJsonUtil::cast_to_number_type(common::ObIAllocator *allocator,
                                    ObIJsonBase *j_base,
                                    common::ObAccuracy &accuracy,
                                    ObJsonCastParam &cast_param,
                                    uint8_t &is_type_mismatch,
                                    number::ObNumber &val)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_number(allocator, val))) {
    is_type_mismatch = 1;
    LOG_WARN("fail to cast json as decimal", K(ret));
  } else if (ObUNumberType == cast_param.dst_type_ && CAST_FAIL(numeric_negative_check(val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(val));
  } else if (CAST_FAIL(ObJsonUtil::number_range_check(accuracy, allocator, val))) {
    LOG_WARN("number_range_check failed", K(ret), K(val));
  }
  return ret;
}

int cast_to_number(common::ObIAllocator *allocator,
                   ObEvalCtx &ctx,
                   ObIJsonBase *j_base,
                   common::ObAccuracy &accuracy,
                   ObJsonCastParam &cast_param,
                   ObDatum &res,
                   uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(ctx);
  number::ObNumber val;
  if (OB_FAIL(ObJsonUtil::cast_to_number_type(allocator, j_base, accuracy, cast_param, is_type_mismatch, val))) {
    LOG_WARN("failed to cast to number type failed.", K(ret));
  } else if (!cast_param.is_only_check_) {
    res.set_number(val);
  }
  return ret;
}

int cast_to_decimalint(common::ObIAllocator *allocator,
                       ObEvalCtx &ctx,
                       ObIJsonBase *j_base,
                       common::ObAccuracy &accuracy,
                       ObJsonCastParam &cast_param,
                       ObDatum &res,
                       uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  // TODO:@xiaofeng.lby, modify this after support cast json into decimalint directly in json_decimalint
  number::ObNumber temp_num;
  ObDecimalInt *decint = nullptr;
  int32_t int_bytes;
  number::ObNumber val;
  if (OB_FAIL(ObJsonUtil::cast_to_number_type(allocator, j_base, accuracy, cast_param, is_type_mismatch, val))) {
    LOG_WARN("cast to number failed", K(ret));
  } else if (OB_FAIL(wide::from_number(val, *allocator, accuracy.scale_, decint, int_bytes))) {
    LOG_WARN("cast number to decimal int failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    const int len = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(accuracy.precision_);
    if (len < int_bytes) {
      res.set_null();
    } else if (len > int_bytes) {
      ObDecimalIntBuilder res_builder;
      res_builder.from(decint, int_bytes);
      res_builder.extend(len);
      res.set_decimal_int(res_builder.get_decimal_int(), res_builder.get_int_bytes());
    } else {
      res.set_decimal_int(decint, int_bytes);
    }
  }
  return ret;
}

int cast_to_bit(common::ObIAllocator *allocator,
                ObEvalCtx &ctx,
                ObIJsonBase *j_base,
                common::ObAccuracy &accuracy,
                ObJsonCastParam &cast_param,
                ObDatum &res,
                uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(accuracy);
  UNUSED(cast_param);
  UNUSED(is_type_mismatch);
  uint64_t val = 0;
  int64_t int_val;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_int(int_val))) {
    ret = OB_ERR_INVALID_JSON_VALUE_FOR_CAST;
    LOG_WARN("fail get int from json", K(ret));
  } else {
    val = static_cast<uint64_t>(int_val);
    if (OB_FAIL(ObJsonUtil::bit_length_check(accuracy, val))) {
      LOG_WARN("fail to check bit range", K(ret));
    } else if (!cast_param.is_only_check_) {
      ObJsonUtil::wrapper_set_uint(cast_param.dst_type_, val, res);
      res.set_uint(val);
    }
  }

  return ret;
}

int cast_to_json(common::ObIAllocator *allocator,
                 ObEvalCtx &ctx,
                 ObIJsonBase *j_base,
                 common::ObAccuracy &accuracy,
                 ObJsonCastParam &cast_param,
                 ObDatum &res,
                 uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(ctx);
  UNUSED(cast_param);
  ObString val;
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->get_raw_binary(val, allocator))) {
    is_type_mismatch = 1;
    LOG_WARN("failed to get raw binary", K(ret));
  } else {
    char *buf = static_cast<char *>(allocator->alloc(val.length()));
    if (OB_UNLIKELY(buf == NULL)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json array result", K(ret), K(val.length()));
    } else {
      MEMCPY(buf, val.ptr(), val.length());
      val.assign_ptr(buf, val.length());
      if (!cast_param.is_only_check_) {
        ObJsonUtil::wrapper_set_string(cast_param.dst_type_, val, res);
        res.set_string(val);
      }
    }
  }

  return ret;
}

int cast_not_expected(common::ObIAllocator *allocator,
                      ObEvalCtx &ctx,
                      ObIJsonBase *j_base,
                      common::ObAccuracy &accuracy,
                      ObJsonCastParam &cast_param,
                      ObDatum &res,
                      uint8_t &is_type_mismatch)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(allocator);
  UNUSED(ctx);
  UNUSED(j_base);
  UNUSED(accuracy);
  UNUSED(res);
  UNUSED(is_type_mismatch);
  LOG_WARN("unexpected dst_type", K(cast_param.dst_type_));
  return ret;
}

int ObJsonUtil::cast_to_res(common::ObIAllocator *allocator,
                            ObEvalCtx &ctx,
                            ObIJsonBase *j_base,
                            ObAccuracy &accuracy,
                            ObJsonCastParam &cast_param,
                            ObDatum &res,
                            uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  ObJsonUtil::ObJsonCastSqlScalar cast_func_ = get_json_cast_func(cast_param.dst_type_);
  if (OB_ISNULL(j_base)
      || (lib::is_mysql_mode() && j_base->json_type() == common::ObJsonNodeType::J_NULL)) {
    res.set_null();
  } else if (OB_ISNULL(cast_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval func can not be null", K(ret));
  } else if (OB_FAIL(((ObJsonUtil::ObJsonCastSqlScalar)(cast_func_))(allocator,
                                                   ctx, j_base, accuracy, cast_param,
                                                   res, is_type_mismatch))) {
    LOG_WARN("fail to deal json cast to sql scalar", K(ret));
  }

  LOG_DEBUG("finish cast_to_res.", K(ret), K(cast_param.dst_type_));

  return ret;
}

int ObJsonUtil::cast_json_scalar_to_sql_obj(common::ObIAllocator *allocator,
                                            ObEvalCtx& ctx,
                                            ObIJsonBase *j_base,
                                            ObCollationType collation,
                                            ObAccuracy &accuracy,
                                            ObObjType obj_type,
                                            ObScale scale,
                                            ObObj &res_obj)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator) || OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(ret));
  } else {
    bool is_use_dynamic_buffer = (ob_is_number_or_decimal_int_tc(obj_type));
    ObDatum res_datum;
    char datum_buffer[OBJ_DATUM_STRING_RES_SIZE] = {0};
    res_datum.ptr_ = datum_buffer;

    if (is_use_dynamic_buffer) {
      void* buffer = allocator->alloc(OBJ_DATUM_STRING_RES_SIZE);
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate tmp buffer failed.", K(ret));
      } else {
        res_datum.ptr_ = static_cast<char*>(buffer);
      }
    }

    ObJsonCastParam cast_param(obj_type, ObCollationType::CS_TYPE_UTF8MB4_BIN, collation, false);
    cast_param.relaxed_time_convert_ = true;
    uint8_t is_type_mismatch = false;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cast_to_res(allocator, ctx, j_base, accuracy, cast_param, res_datum, is_type_mismatch))) {
      LOG_WARN("fail to cast.", K(ret));
    } else {
      res_obj.set_type(obj_type);
      res_obj.set_collation_type(collation);
      res_obj.set_scale(scale);

      if (OB_FAIL(res_datum.to_obj(res_obj, res_obj.meta_))) {
        LOG_WARN("fail datum to obj.", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonUtil::cast_json_scalar_to_sql_obj(common::ObIAllocator *allocator,
                                            ObExecContext* exec_ctx,
                                            ObIJsonBase *j_base,
                                            ObExprResType col_res_type,
                                            ObObj &res_obj) {
  // ToDo: refine
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator) || OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null.", K(ret));
  } else {
    ObEvalCtx ctx(*exec_ctx);
    ObAccuracy temp_accuracy = col_res_type.get_accuracy();
    ret =  cast_json_scalar_to_sql_obj(allocator, ctx, j_base,
                                      col_res_type.get_collation_type(),
                                      temp_accuracy,
                                      col_res_type.get_type(),
                                      col_res_type.get_scale(),
                                      res_obj);
  }
  return ret;
}

/*
ObJsonUtil::ObJsonCastSqlDatum OB_JSON_CAST_DATUM_EXPLICIT[ObMaxTC] =
{
  // ObNullTC      = 0,    // null
  cast_to_null<ObDatum>,
  // ObIntTC       = 1,    // int8, int16, int24, int32, int64.
  cast_to_int<ObDatum>,
  // ObUIntTC      = 2,    // uint8, uint16, uint24, uint32, uint64.
  cast_to_uint<ObDatum>,
  // ObFloatTC     = 3,    // float, ufloat.
  cast_to_float<ObDatum>,
  // ObDoubleTC    = 4,    // double, udouble.
  cast_to_double<ObDatum>,
  // ObNumberTC    = 5,    // number, unumber.
  cast_to_number<ObDatum>,
  // ObDateTimeTC  = 6,    // datetime, timestamp.
  cast_to_date_time<ObDatum>,
  // ObDateTC      = 7,    // date
  cast_to_date<ObDatum>,
  // ObTimeTC      = 8,    // time
  cast_to_time<ObDatum>,
  // ObYearTC      = 9,    // year
  cast_to_year<ObDatum>,
  // ObStringTC    = 10,   // varchar, char, varbinary, binary.
  cast_to_string<ObDatum>,
  // ObExtendTC    = 11,   // extend
  cast_not_expected<ObDatum>,
  // ObUnknownTC   = 12,   // unknown
  cast_not_expected<ObDatum>,
  // ObTextTC      = 13,   // TinyText,MediumText, Text ,LongText, TinyBLOB,MediumBLOB, // BLOB ,LongBLOB
  cast_to_string<ObDatum>,
  // ObBitTC       = 14,   // bit
  cast_to_bit<ObDatum>,
  // ObEnumSetTC   = 15,   // enum, set
  cast_not_expected<ObDatum>,
  // ObEnumSetInnerTC  = 16,
  cast_not_expected<ObDatum>,
  // ObOTimestampTC    = 17, //timestamp with time zone
  cast_to_timstamp<ObDatum>,
  // ObRawTC       = 18,   // raw
  cast_to_string<ObDatum>,
  // ObIntervalTC      = 19, //oracle interval type class include interval year to month and interval day to second
  cast_not_expected<ObDatum>,
  // ObRowIDTC         = 20, // oracle rowid typeclass, includes urowid and rowid
  cast_not_expected<ObDatum>,
  // ObLobTC           = 21, //oracle lob typeclass ObLobType not use
  cast_not_expected<ObDatum>,
  // ObJsonTC          = 22, // json type class
  cast_to_json<ObDatum>,
  // ObGeometryTC      = 23, // geometry type class
  cast_not_expected<ObDatum>,
  // ObUserDefinedSQLTC = 24, // user defined type class in SQL
  cast_not_expected<ObDatum>,
};

ObJsonUtil::ObJsonCastSqlObj OB_JSON_CAST_OBJ_EXPLICIT[ObMaxTC] =
{
  // ObNullTC      = 0,    // null
  cast_to_null<ObObj>,
  // ObIntTC       = 1,    // int8, int16, int24, int32, int64.
  cast_to_int<ObObj>,
  // ObUIntTC      = 2,    // uint8, uint16, uint24, uint32, uint64.
  cast_to_uint<ObObj>,
  // ObFloatTC     = 3,    // float, ufloat.
  cast_to_float<ObObj>,
  // ObDoubleTC    = 4,    // double, udouble.
  cast_to_double<ObObj>,
  // ObNumberTC    = 5,    // number, unumber.
  cast_to_number<ObObj>,
  // ObDateTimeTC  = 6,    // datetime, timestamp.
  cast_to_date_time<ObObj>,
  // ObDateTC      = 7,    // date
  cast_to_date<ObObj>,
  // ObTimeTC      = 8,    // time
  cast_to_time<ObObj>,
  // ObYearTC      = 9,    // year
  cast_to_year<ObObj>,
  // ObStringTC    = 10,   // varchar, char, varbinary, binary.
  cast_to_string<ObObj>,
  // ObExtendTC    = 11,   // extend
  cast_not_expected<ObObj>,
  // ObUnknownTC   = 12,   // unknown
  cast_not_expected<ObObj>,
  // ObTextTC      = 13,   // TinyText,MediumText, Text ,LongText, TinyBLOB,MediumBLOB, // BLOB ,LongBLOB
  cast_to_string<ObObj>,
  // ObBitTC       = 14,   // bit
  cast_to_bit<ObObj>,
  // ObEnumSetTC   = 15,   // enum, set
  cast_not_expected<ObObj>,
  // ObEnumSetInnerTC  = 16,
  cast_not_expected<ObObj>,
  // ObOTimestampTC    = 17, //timestamp with time zone
  cast_to_timstamp<ObObj>,
  // ObRawTC       = 18,   // raw
  cast_to_string<ObObj>,
  // ObIntervalTC      = 19, //oracle interval type class include interval year to month and interval day to second
  cast_not_expected<ObObj>,
  // ObRowIDTC         = 20, // oracle rowid typeclass, includes urowid and rowid
  cast_not_expected<ObObj>,
  // ObLobTC           = 21, //oracle lob typeclass ObLobType not use
  cast_not_expected<ObObj>,
  // ObJsonTC          = 22, // json type class
  cast_to_json<ObObj>,
  // ObGeometryTC      = 23, // geometry type class
  cast_not_expected<ObObj>,
  // ObUserDefinedSQLTC = 24, // user defined type class in SQL
  cast_not_expected<ObObj>,
};
*/

ObJsonUtil::ObJsonCastSqlScalar OB_JSON_CAST_SQL_EXPLICIT[ObMaxTC] =
{
  // ObNullTC      = 0,    // null
  cast_to_null,
  // ObIntTC       = 1,    // int8, int16, int24, int32, int64.
  cast_to_int,
  // ObUIntTC      = 2,    // uint8, uint16, uint24, uint32, uint64.
  cast_to_uint,
  // ObFloatTC     = 3,    // float, ufloat.
  cast_to_float,
  // ObDoubleTC    = 4,    // double, udouble.
  cast_to_double,
  // ObNumberTC    = 5,    // number, unumber.
  cast_to_number,
  // ObDateTimeTC  = 6,    // datetime, timestamp.
  cast_to_date_time,
  // ObDateTC      = 7,    // date
  cast_to_date,
  // ObTimeTC      = 8,    // time
  cast_to_time,
  // ObYearTC      = 9,    // year
  cast_to_year,
  // ObStringTC    = 10,   // varchar, char, varbinary, binary.
  cast_to_string,
  // ObExtendTC    = 11,   // extend
  cast_not_expected,
  // ObUnknownTC   = 12,   // unknown
  cast_not_expected,
  // ObTextTC      = 13,   // TinyText,MediumText, Text ,LongText, TinyBLOB,MediumBLOB, // BLOB ,LongBLOB
  cast_to_string,
  // ObBitTC       = 14,   // bit
  cast_to_bit,
  // ObEnumSetTC   = 15,   // enum, set
  cast_not_expected,
  // ObEnumSetInnerTC  = 16,
  cast_not_expected,
  // ObOTimestampTC    = 17, //timestamp with time zone
  cast_to_timstamp,
  // ObRawTC       = 18,   // raw
  cast_to_string,
  // ObIntervalTC      = 19, //oracle interval type class include interval year to month and interval day to second
  cast_not_expected,
  // ObRowIDTC         = 20, // oracle rowid typeclass, includes urowid and rowid
  cast_not_expected,
  // ObLobTC           = 21, //oracle lob typeclass ObLobType not use
  cast_not_expected,
  // ObJsonTC          = 22, // json type class
  cast_to_json,
  // ObGeometryTC      = 23, // geometry type class
  cast_not_expected,
  // ObUserDefinedSQLTC = 24, // user defined type class in SQL
  cast_not_expected,
  // ObDecimalIntTC = 25, // decimal int class
  cast_to_decimalint,
};

#undef CAST_FAIL
#undef GET_SESSION

int ObJsonUtil::get_json_path(ObExpr* expr,
                              ObEvalCtx &ctx,
                              bool &is_null_result,
                              ObJsonParamCacheCtx *&param_ctx,
                              common::ObIAllocator &temp_allocator,
                              bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObObjType type = expr->datum_meta_.type_;
  ObJsonPathCache ctx_cache(&temp_allocator);
  ObJsonPath* j_path = nullptr;
  ObString j_path_text;
  ObJsonPathCache* path_cache = path_cache = param_ctx->get_path_cache();
  // parse json path
  if (!param_ctx->is_first_exec_ && param_ctx->is_json_path_const_
      && OB_NOT_NULL(path_cache)) {
  } else {
    type = expr->datum_meta_.type_;
    if (OB_FAIL(expr->eval(ctx, json_datum))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else if (type == ObNullType || json_datum->is_null()) {
      is_null_result = true;
    } else if (!ob_is_string_type(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(type));
    }

    if OB_SUCC(ret) {
      j_path_text = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr, ctx, temp_allocator, j_path_text, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
      } else if (j_path_text.length() == 0) { // maybe input json doc is null type
        is_null_result = true;
      } else if (OB_FAIL(ObJsonExprHelper::convert_string_collation_type(expr->datum_meta_.cs_type_,
                                                       CS_TYPE_UTF8MB4_BIN,
                                                       &ctx.exec_ctx_.get_allocator(),
                                                       j_path_text,
                                                       j_path_text))) {
        LOG_WARN("convert string memory failed", K(ret), K(j_path_text));
      }
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
    }
  }
  if (OB_SUCC(ret)) {
    if (j_path_text.empty()) {
        param_ctx->json_param_.json_path_ = nullptr;
    } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, 0, true, param_ctx->is_json_path_const_))) {
      is_cover_by_error = false;
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
        LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
      }
    } else {
      param_ctx->json_param_.json_path_ = j_path;
    }
  }
  return ret;
}

int ObJsonUtil::get_json_doc(ObExpr *expr,
                             ObEvalCtx &ctx,
                             common::ObIAllocator &allocator,
                             ObIJsonBase*& j_base,
                             bool &is_null, bool & is_cover_by_error,
                             bool relax)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObObjType val_type = expr->datum_meta_.type_;
  ObCollationType cs_type = expr->datum_meta_.cs_type_;

  bool is_oracle = lib::is_oracle_mode();

  if (OB_UNLIKELY(OB_FAIL(expr->eval(ctx, json_datum)))) {
    is_cover_by_error = false;
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObNullType || json_datum->is_null()) {
    is_null = true;
  } else if (val_type != ObJsonType && !ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (lib::is_mysql_mode() && OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else {
    ObString j_str;
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr, ctx, allocator, j_str, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (is_null) {
    } else {
      ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
      ObJsonInType expect_type = j_in_type;
      bool relax_json = (is_oracle && relax);
      uint32_t parse_flag = relax_json ? ObJsonParser::JSN_RELAXED_FLAG : 0;
      if (is_oracle && j_str.length() == 0) {
        is_null = true;
      } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, j_in_type,
                                                  expect_type, j_base, parse_flag))) {
        LOG_WARN("fail to get json base", K(ret), K(j_in_type));
        if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
          is_cover_by_error = false;
        } else if (is_oracle) {
          ret = OB_ERR_JSON_SYNTAX_ERROR;
        } else {
          ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
        }
      } else if (j_base->is_bin()) {
        // only use json doc to search
        static_cast<ObJsonBin*>(j_base)->set_seek_flag(true);
      }
    }
  }
  return ret;
}

int func_upper_json_string(ObIJsonBase*& in,
                           bool &is_null_result,
                           common::ObIAllocator *allocator,
                           uint8_t &is_type_mismatch)
{
  UNUSED(is_null_result);
  UNUSED(allocator);
  UNUSED(is_type_mismatch);
  if (((ObJsonString *)in)->get_is_null_to_str()) {
    is_null_result = true;
  }
  return OB_SUCCESS;
}

int func_conversion_fail(ObIJsonBase*& in,
                         bool &is_null_result,
                         common::ObIAllocator *allocator,
                         uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(in);
  UNUSED(allocator);
  UNUSED(is_type_mismatch);
  UNUSED(is_null_result);
  ret = OB_ERR_CONVERSION_FAIL;
  LOG_WARN("data seek fail", K(ret));
  return ret;
}

int func_str_json_null(ObIJsonBase*& in,
                       bool &is_null_result,
                       common::ObIAllocator *allocator,
                       uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  if (!in->is_real_json_null(in)) {
    ret = OB_INVALID_NUMERIC;
    is_type_mismatch = 1;
    LOG_WARN("string only function meet non-string data", K(ret));
  }
  return ret;
}

int func_num_json_null(ObIJsonBase*& in,
                       bool &is_null_result,
                       common::ObIAllocator *allocator,
                       uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(allocator);
  if (!in->is_real_json_null(in)) {
    ret = OB_INVALID_NUMERIC;
    is_type_mismatch = 1;
    LOG_WARN("number only function meet non-number data", K(ret));
  }
  return ret;
}

int set_null_result(ObIJsonBase*& in,
                    bool &is_null_result,
                    common::ObIAllocator *allocator,
                    uint8_t &is_type_mismatch)
{
  UNUSED(in);
  UNUSED(allocator);
  UNUSED(is_type_mismatch);
  is_null_result = true;
  return OB_SUCCESS;
}

int cast_succ(ObIJsonBase*& in,
              bool &is_null_result,
              common::ObIAllocator *allocator,
              uint8_t &is_type_mismatch)
{
  UNUSED(in);
  UNUSED(is_null_result);
  UNUSED(allocator);
  UNUSED(is_type_mismatch);
  return OB_SUCCESS;
}

int func_path_syntax_fail(ObIJsonBase*& in,
                          bool &is_null_result,
                          common::ObIAllocator *allocator,
                          uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  UNUSED(in);
  UNUSED(allocator);
  UNUSED(is_type_mismatch);
  UNUSED(is_null_result);
  ret = OB_ERR_JSON_PATH_SYNTAX_ERROR;
  LOG_WARN("boolean only function meet non-boolean data", K(ret));
  return ret;
}

int func_bool_json_int(ObIJsonBase*& in,
                        bool &is_null_result,
                        common::ObIAllocator *allocator,
                        uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  if (in->get_int() == 1 || in->get_int() == 0) {
    bool is_true = (in->get_int() == 1);
    ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonBoolean(is_true);
      in = tmp_ans;
    }
  } else {
    is_null_result = true;
  }
  return ret;
}

int func_bool_json_double(ObIJsonBase*& in,
                          bool &is_null_result,
                          common::ObIAllocator *allocator,
                          uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  if (in->get_double() == 1.0 || in->get_double() == 0.0) {
    bool is_true = (in->get_double() == 1.0);
    ObJsonBoolean* tmp_ans = static_cast<ObJsonBoolean*> (allocator->alloc(sizeof(ObJsonBoolean)));
    if (OB_ISNULL(tmp_ans)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
    } else {
      tmp_ans = new (tmp_ans) ObJsonBoolean(is_true);
      in = tmp_ans;
    }
  } else {
    is_null_result = true;
  }
  return ret;
}

// 17 * 30
ObJsonUtil::ObItemMethodValid OB_JSON_VALUE_ITEM_METHOD_CAST_FUNC[ObMaxItemMethod][ObMaxJsonType] =
{
  {
    // abs -> **
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // boolean - > ***
    set_null_result, // null
    set_null_result, // decimal
    func_bool_json_int, // int
    set_null_result, // uint
    func_bool_json_double, // double
    set_null_result, // string
    set_null_result, //object
    set_null_result, //array
    cast_succ, // boolean
    set_null_result, // date
    set_null_result, //time
    set_null_result, // datetime
    set_null_result, //timestamp
    set_null_result, // opaque
    cast_succ, // empty
    set_null_result, //ofloat
    set_null_result, // odouble
    set_null_result, // odeciaml
    func_bool_json_int, // oint
    set_null_result, // olong
    set_null_result, // obinary
    set_null_result, // oid
    set_null_result, //rawhex
    set_null_result, // rawid
    set_null_result, // oracledate
    set_null_result, // odate
    set_null_result, // otimestamp
    set_null_result, // otimestamptz
    set_null_result, // odaysecond
    set_null_result, // oyearmonth
  },
  {
    // bool_only -> ***
    func_path_syntax_fail, // null
    func_path_syntax_fail, // decimal
    func_path_syntax_fail, // int
    func_path_syntax_fail, // uint
    func_path_syntax_fail, // double
    func_path_syntax_fail, // string
    func_path_syntax_fail, //object
    func_path_syntax_fail, //array
    cast_succ, // boolean
    func_path_syntax_fail, // date
    func_path_syntax_fail, //time
    func_path_syntax_fail, // datetime
    func_path_syntax_fail, //timestamp
    func_path_syntax_fail, // opaque
    cast_succ, // empty
    func_path_syntax_fail, //ofloat
    func_path_syntax_fail, // odouble
    func_path_syntax_fail, // odeciaml
    func_path_syntax_fail, // oint
    func_path_syntax_fail, // olong
    func_path_syntax_fail, // obinary
    func_path_syntax_fail, // oid
    func_path_syntax_fail, //rawhex
    func_path_syntax_fail, // rawid
    func_path_syntax_fail, // oracledate
    func_path_syntax_fail, // odate
    func_path_syntax_fail, // otimestamp
    func_path_syntax_fail, // otimestamptz
    func_path_syntax_fail, // odaysecond
    func_path_syntax_fail, // oyearmonth
  },
  {
    // ceiling -> **
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // date -> ***
    func_conversion_fail, // j_null
    func_conversion_fail, // decimal
    func_conversion_fail, // int
    func_conversion_fail, // uint
    func_conversion_fail, // double
    cast_succ, // string
    func_conversion_fail, //object
    func_conversion_fail, //array
    func_conversion_fail, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    func_conversion_fail, // opaque
    func_conversion_fail, // empty
    func_conversion_fail, //ofloat
    func_conversion_fail, // odouble
    func_conversion_fail, // odeciaml
    func_conversion_fail, // oint
    func_conversion_fail, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // double -> ***
    func_conversion_fail, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    func_conversion_fail, // string
    func_conversion_fail, //object
    func_conversion_fail, //array
    func_conversion_fail, // boolean
    func_conversion_fail, // date
    func_conversion_fail, //time
    func_conversion_fail, // datetime
    func_conversion_fail, //timestamp
    func_conversion_fail, // opaque
    func_conversion_fail, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    func_conversion_fail, // obinary
    func_conversion_fail, // oid
    func_conversion_fail, //rawhex
    func_conversion_fail, // rawid
    func_conversion_fail, // oracledate
    func_conversion_fail, // odate
    func_conversion_fail, // otimestamp
    func_conversion_fail, // otimestamptz
    func_conversion_fail, // odaysecond
    func_conversion_fail, // oyearmonth
  },
  {
    // floor ->**
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // length -> **
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // lower -> ***
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    func_upper_json_string, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // number ->
    func_num_json_null, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // num_only -> ***
    func_num_json_null, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // size ->**
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // string-> ***
    func_str_json_null, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // str_only -> ***
    func_str_json_null, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // timestamp -> ***
    func_conversion_fail, // j_null
    func_conversion_fail, // decimal
    func_conversion_fail, // int
    func_conversion_fail, // uint
    func_conversion_fail, // double
    cast_succ, // string
    func_conversion_fail, //object
    func_conversion_fail, //array
    func_conversion_fail, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    func_conversion_fail, // opaque
    func_conversion_fail, // empty
    func_conversion_fail, //ofloat
    func_conversion_fail, // odouble
    func_conversion_fail, // odeciaml
    func_conversion_fail, // oint
    func_conversion_fail, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // type ->**
    cast_succ, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    cast_succ, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
  {
    // upper -> ***
    func_num_json_null, // j_null
    cast_succ, // decimal
    cast_succ, // int
    cast_succ, // uint
    cast_succ, // double
    func_upper_json_string, // string
    cast_succ, //object
    cast_succ, //array
    cast_succ, // boolean
    cast_succ, // date
    cast_succ, //time
    cast_succ, // datetime
    cast_succ, //timestamp
    cast_succ, // opaque
    cast_succ, // empty
    cast_succ, //ofloat
    cast_succ, // odouble
    cast_succ, // odeciaml
    cast_succ, // oint
    cast_succ, // olong
    cast_succ, // obinary
    cast_succ, // oid
    cast_succ, //rawhex
    cast_succ, // rawid
    cast_succ, // oracledate
    cast_succ, // odate
    cast_succ, // otimestamp
    cast_succ, // otimestamptz
    cast_succ, // odaysecond
    cast_succ, // oyearmonth
  },
};

// ItemJsonCompare

int OB_JSON_QUERY_ITEM_METHOD_NULL_OPTION[ObMaxItemMethod][ObMaxJsonType] =
{
  {
    // abs -> **
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // boolean - > ***
    1, // null
    1, // decimal
    1, // int
    1, // uint
    1, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    1, //ofloat
    1, // odouble
    1, // odeciaml
    1, // oint
    1, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // bool_only -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // ceiling -> **
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // date -> ***
    1, // null
    1, // decimal
    1, // int
    1, // uint
    1, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    1, // opaque
    1, // empty
    1, //ofloat
    1, // odouble
    1, // odeciaml
    1, // oint
    1, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // double -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    1, // date
    1, //time
    1, // datetime
    1, //timestamp
    1, // opaque
    1, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    1, // oracledate
    1, // odate
    1, // otimestamp
    1, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // floor ->**
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // length -> **
    1, // null
    1, // decimal
    1, // int
    0, // uint
    1, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    1, // date
    1, //time
    1, // datetime
    1, //timestamp
    1, // opaque
    1, // empty
    1, //ofloat
    1, // odouble
    1, // odeciaml
    1, // oint
    1, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    1, // oracledate
    1, // odate
    1, // otimestamp
    1, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // lower -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    1, //object
    1, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // number ->
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    1, // date
    1, //time
    1, // datetime
    1, //timestamp
    1, // opaque
    1, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    1, // oracledate
    1, // odate
    1, // otimestamp
    1, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // num_only -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    1, // date
    1, //time
    1, // datetime
    1, //timestamp
    1, // opaque
    1, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    1, // oracledate
    1, // odate
    1, // otimestamp
    1, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // size ->**
    0, // j_null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // string-> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    1, //object
    1, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // str_only -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    1, //object
    1, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // timestamp -> ***
    1, // null
    1, // decimal
    1, // int
    1, // uint
    1, // double
    1, // string
    1, //object
    1, //array
    1, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    1, // opaque
    1, // empty
    1, //ofloat
    1, // odouble
    1, // odeciaml
    1, // oint
    1, // olong
    1, // obinary
    1, // oid
    1, //rawhex
    1, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    1, // odaysecond
    1, // oyearmonth
  },
  {
    // type ->**
    0, // j_null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    0, //object
    0, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
  {
    // upper -> ***
    0, // null
    0, // decimal
    0, // int
    0, // uint
    0, // double
    0, // string
    1, //object
    1, //array
    0, // boolean
    0, // date
    0, //time
    0, // datetime
    0, //timestamp
    0, // opaque
    0, // empty
    0, //ofloat
    0, // odouble
    0, // odeciaml
    0, // oint
    0, // olong
    0, // obinary
    0, // oid
    0, //rawhex
    0, // rawid
    0, // oracledate
    0, // odate
    0, // otimestamp
    0, // otimestamptz
    0, // odaysecond
    0, // oyearmonth
  },
};

int ObJsonUtil::get_query_item_method_null_option(ObJsonPath* j_path,
                                               ObIJsonBase* j_base)
{
  size_t item_method = static_cast<uint8_t>(j_path->get_last_node_type());
  size_t json_type = static_cast<uint8_t>(j_base->json_type());
  int is_null_res = 0;
  // first item method pos is JPN_ABS
  if (!j_path->is_last_func()) {
    // do nothing
  } else {
    is_null_res = OB_JSON_QUERY_ITEM_METHOD_NULL_OPTION[item_method - ObJsonPathNodeType::JPN_ABS][json_type];
  }
  return is_null_res;
}

ObJsonUtil::ObItemMethodValid ObJsonUtil::get_item_method_cast_res_func(ObJsonPath* j_path,
                                              ObIJsonBase* j_base)
{
  INIT_SUCC(ret);
  size_t item_method = static_cast<uint8_t>(j_path->get_last_node_type());
  size_t json_type = static_cast<uint8_t>(j_base->json_type());
  // first item method pos is 13
  return OB_JSON_VALUE_ITEM_METHOD_CAST_FUNC[item_method - ObJsonPathNodeType::JPN_ABS][json_type];
}

/*
ObJsonUtil::ObJsonCastSqlObj ObJsonUtil::get_json_obj_cast_func(ObObjType dst_type)
{
  return OB_JSON_CAST_OBJ_EXPLICIT[OBJ_TYPE_TO_CLASS[dst_type]];
}

ObJsonUtil::ObJsonCastSqlDatum ObJsonUtil::get_json_datum_cast_func(ObObjType dst_type)
{
  return OB_JSON_CAST_DATUM_EXPLICIT[OBJ_TYPE_TO_CLASS[dst_type]];
}
*/

ObJsonUtil::ObJsonCastSqlScalar ObJsonUtil::get_json_cast_func(ObObjType dst_type)
{
  return OB_JSON_CAST_SQL_EXPLICIT[OBJ_TYPE_TO_CLASS[dst_type]];
}

bool ObJsonUtil::is_number_item_method(ObJsonPath* j_path)
{
  return j_path->get_last_node_type() == JPN_NUMBER
          || j_path->get_last_node_type() == JPN_NUM_ONLY
          || j_path->get_last_node_type() == JPN_LENGTH
          || j_path->get_last_node_type() == JPN_TYPE
          || j_path->get_last_node_type() == JPN_SIZE;
}

} // sql
} // oceanbase
