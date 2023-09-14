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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_obj_cast.h"
#include "observer/table_load/ob_table_load_time_convert.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace sql;

const ObObj ObTableLoadObjCaster::zero_obj(0);
const ObObj ObTableLoadObjCaster::null_obj(ObObjType::ObNullType);

static int pad_obj(ObTableLoadCastObjCtx &cast_obj_ctx, const ObColumnSchemaV2 *column_schema, ObObj &obj)
{
  int ret = OB_SUCCESS;
  bool is_pad = false;
  bool is_fixed_string = obj.is_fixed_len_char_type() || obj.is_binary();
  //if (lib::is_mysql_mode()) {
  //  if (is_fixed_string && (SMO_PAD_CHAR_TO_FULL_LENGTH & cast_obj_ctx.param_.sql_mode_)) {
  //    is_pad = true;
  //  }
  //} else {
  //  is_pad = is_fixed_string;
  //}
  if (is_fixed_string) {
    //Because NCHAR stores data in a format similar to Unicode, special treatment is required when padding.
    if (column_schema->get_data_type() == ObNCharType) {
      int32_t fixed_len = column_schema->get_data_length() * 2;
      if (obj.val_len_ % 2 != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The length of data for NCHAR type must be even", K(obj.val_len_));
      } else if (fixed_len > obj.val_len_) {
        char *buf = (char *)cast_obj_ctx.cast_ctx_->allocator_v2_->alloc(fixed_len);
        if (buf == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate buf", K(fixed_len), KR(ret));
        }
        if (OB_SUCC(ret)) {
          const ObCharsetType &cs = ObCharset::charset_type_by_coll(obj.get_collation_type());
          char padding_char = (CHARSET_BINARY == cs) ? OB_PADDING_BINARY : OB_PADDING_CHAR;
          MEMCPY(buf, obj.v_.ptr_, obj.val_len_);
          if (CHARSET_BINARY == cs) {
            MEMSET(buf + obj.val_len_, padding_char, fixed_len - obj.val_len_);
          } else {
            for (int i = 0; i < fixed_len - obj.val_len_; i += 2) {
              *(buf + obj.val_len_ + i) = '\0';
              *(buf + obj.val_len_ + i + 1) = padding_char;
            }
          }
          obj.v_.ptr_ = buf;
          obj.val_len_ = fixed_len;
        }
      }
    } else {
      int32_t fixed_len = column_schema->get_data_length();
      if (fixed_len > obj.val_len_) {
        char *buf = (char *)cast_obj_ctx.cast_ctx_->allocator_v2_->alloc(fixed_len);
        if (buf == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate buf", K(fixed_len), KR(ret));
        }
        if (OB_SUCC(ret)) {
          const ObCharsetType &cs = ObCharset::charset_type_by_coll(obj.get_collation_type());
          char padding_char = (CHARSET_BINARY == cs) ? OB_PADDING_BINARY : OB_PADDING_CHAR;
          MEMCPY(buf, obj.v_.ptr_, obj.val_len_);
          MEMSET(buf + obj.val_len_, padding_char, fixed_len - obj.val_len_);
          obj.v_.ptr_ = buf;
          obj.val_len_ = fixed_len;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadObjCaster::cast_obj(ObTableLoadCastObjCtx &cast_obj_ctx,
                                   const ObColumnSchemaV2 *column_schema, const ObObj &src,
                                   ObObj &dst)
{
  int ret = OB_SUCCESS;
  const ObObj *convert_src_obj = nullptr;
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  const ObAccuracy &accuracy = column_schema->get_accuracy();
  if (OB_FAIL(convert_obj(expect_type, src, convert_src_obj))) {
    LOG_WARN("fail to convert obj", KR(ret));
  }

  if (OB_SUCC(ret)) {
    if (column_schema->is_enum_or_set()) {
      if (OB_FAIL(handle_string_to_enum_set(cast_obj_ctx, column_schema, src, dst))) {
        LOG_WARN("fail to convert string to enum or set", KR(ret), K(src), K(dst));
      }
    } else {
      if (OB_FAIL(to_type(expect_type, column_schema, cast_obj_ctx, accuracy, *convert_src_obj, dst))) {
        LOG_WARN("fail to do to type", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pad_obj(cast_obj_ctx, column_schema, dst))) {
      LOG_WARN("fail to pad obj", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (cast_obj_ctx.is_need_check_ &&
                OB_FAIL(cast_obj_check(cast_obj_ctx, column_schema, dst))) {
      LOG_WARN("fail to check cast obj result", KR(ret), K(dst));
    }
  }
  return ret;
}

int ObTableLoadObjCaster::convert_obj(const ObObjType &expect_type, const ObObj &src,
                                      const ObObj *&dest)
{
  int ret = OB_SUCCESS;
  dest = &src;
  if (src.is_string_type() && !src.is_null() && lib::is_mysql_mode() && 0 == src.get_val_len() &&
      !ob_is_string_type(expect_type)) {
    dest = &zero_obj;
  } else if (src.is_string_type() && lib::is_oracle_mode() &&
             (src.is_null_oracle() || 0 == src.get_val_len())) {
    dest = &null_obj;
  }
  return ret;
}

int ObTableLoadObjCaster::handle_string_to_enum_set(ObTableLoadCastObjCtx &cast_obj_ctx,
                                                    const ObColumnSchemaV2 *column_schema,
                                                    const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  int warning = 0;
  uint64_t output_value = 0;
  const ObIArray<common::ObString> &type_infos = column_schema->get_extended_type_info();
  const ObCollationType collation_type = column_schema->get_collation_type();
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  ObCastMode cast_mode = cast_obj_ctx.cast_ctx_->cast_mode_;
  if (src.is_null()) {
    dst.set_null();
  } else if (expect_type == ObEnumType) {
    if (OB_FAIL(string_to_enum(*(cast_obj_ctx.cast_ctx_->allocator_v2_), src, collation_type,
                               cast_mode, type_infos, warning, output_value))) {
      LOG_WARN("fail to convert string to enum", KR(ret), K(src), K(type_infos), K(output_value));
    } else {
      dst.set_enum(output_value);
    }
  } else if (expect_type == ObSetType) {
    if (OB_FAIL(string_to_set(*(cast_obj_ctx.cast_ctx_->allocator_v2_), src, collation_type,
                              cast_mode, type_infos, warning, output_value))) {
      LOG_WARN("fail to convert string to set", KR(ret), K(src), K(type_infos), K(output_value));
    } else {
      dst.set_set(output_value);
    }
  }
  return ret;
}

int ObTableLoadObjCaster::string_to_enum(ObIAllocator &alloc, const ObObj &src,
                                         const ObCollationType cs_type,
                                         const common::ObCastMode cast_mode,
                                         const ObIArray<ObString> &str_values, int &warning,
                                         uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  int32_t pos = 0;
  ObString in_str;
  const ObCollationType in_cs_type = src.get_collation_type();
  const ObString orig_in_str = src.get_string();
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  int32_t no_sp_len =
    static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length()));
  ObString no_sp_val(0, static_cast<ObString::obstr_size_t>(no_sp_len), in_str.ptr());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_type(str_values, cs_type, no_sp_val, pos))) {
    LOG_WARN("fail to find type", K(str_values), K(cs_type), K(no_sp_val), K(in_str), K(pos),
             K(ret));
  } else if (OB_UNLIKELY(pos < 0)) {
    // Bug30666903: check implicit cast logic to handle number cases
    if (!in_str.is_numeric()) {
      ret = OB_ERR_DATA_TRUNCATED;
    } else {
      int err = 0;
      int64_t val_cnt = str_values.count();
      value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
      if (err != 0 || value > val_cnt) {
        value = 0;
        ret = OB_ERR_DATA_TRUNCATED;
        LOG_WARN("input value out of range", K(val_cnt), K(ret), K(err));
      }
      if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
        warning = ret;
        ret = OB_SUCCESS;
      }
    }
  } else {
    value = pos + 1; // enum start from 1
  }
  output_value = value;
  LOG_DEBUG("finish string_enum", K(ret), K(in_str), K(str_values), K(output_value), K(lbt()));
  return ret;
}

int ObTableLoadObjCaster::string_to_set(ObIAllocator &alloc, const ObObj &src,
                                        const ObCollationType cs_type,
                                        const common::ObCastMode cast_mode,
                                        const ObIArray<ObString> &str_values, int &warning,
                                        uint64_t &output_value)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObString in_str;
  const ObCollationType in_cs_type = src.get_collation_type();
  const ObString orig_in_str = src.get_string();
  OZ(ObCharset::charset_convert(alloc, orig_in_str, in_cs_type, cs_type, in_str));
  if (OB_FAIL(ret)) {
  } else if (in_str.empty()) {
    // do noting
  } else {
    bool is_last_value = false;
    const ObString &sep = ObCharsetUtils::get_const_str(cs_type, ',');
    int32_t pos = 0;
    const char *remain = in_str.ptr();
    int64_t remain_len = ObCharset::strlen_byte_no_sp(cs_type, in_str.ptr(), in_str.length());
    ObString val_str;
    do {
      pos = 0;
      const char *sep_loc = NULL;
      if (NULL == (sep_loc = static_cast<const char *>(
                     memmem(remain, remain_len, sep.ptr(), sep.length())))) {
        is_last_value = true;
        val_str.assign_ptr(remain, remain_len);
        if (OB_FAIL(find_type(str_values, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type", K(str_values), K(cs_type), K(in_str), K(pos), K(ret));
        }
      } else {
        val_str.assign_ptr(remain, sep_loc - remain);
        remain_len = remain_len - (sep_loc - remain + sep.length());
        remain = sep_loc + sep.length();
        if (OB_FAIL(find_type(str_values, cs_type, val_str, pos))) {
          LOG_WARN("fail to find type", K(str_values), K(cs_type), K(val_str), K(pos), K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(pos < 0)) { // not found
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          warning = OB_ERR_DATA_TRUNCATED;
          LOG_INFO("input value out of range, and set out value zero", K(pos), K(val_str),
                   K(in_str), K(warning));
        } else {
          ret = OB_ERR_DATA_TRUNCATED;
          LOG_WARN("data truncate", K(pos), K(val_str), K(in_str), K(ret));
        }
      } else {
        pos %= 64; // MySQL中，如果value存在重复，则value_count可以大于64
        value |= (1ULL << pos);
      }
    } while (OB_SUCC(ret) && !is_last_value);
  }

  // Bug30666903: check implicit cast logic to handle number cases
  if (in_str.is_numeric() && (OB_ERR_DATA_TRUNCATED == ret || (OB_ERR_DATA_TRUNCATED == warning &&
                                                               CM_IS_WARN_ON_FAIL(cast_mode)))) {
    int err = 0;
    value = ObCharset::strntoull(in_str.ptr(), in_str.length(), 10, &err);
    if (err == 0) {
      ret = OB_SUCCESS;
      uint32_t val_cnt = str_values.count();
      if (OB_UNLIKELY(val_cnt <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect val_cnt", K(val_cnt), K(ret));
      } else if (val_cnt >= 64) { // do nothing
      } else if (val_cnt < 64 && value > ((1ULL << val_cnt) - 1)) {
        value = 0;
        ret = OB_ERR_DATA_TRUNCATED;
        LOG_WARN("input value out of range", K(val_cnt), K(ret));
      }
      if (OB_FAIL(ret) && CM_IS_WARN_ON_FAIL(cast_mode)) {
        warning = OB_ERR_DATA_TRUNCATED;
        ret = OB_SUCCESS;
      }
    } else {
      value = 0;
    }
  }

  output_value = value;
  LOG_DEBUG("finish string_set", K(ret), K(in_str), K(str_values), K(output_value), K(lbt()));
  return ret;
}

int ObTableLoadObjCaster::to_type(const ObObjType &expect_type, const share::schema::ObColumnSchemaV2 *column_schema, ObTableLoadCastObjCtx &cast_obj_ctx,
                                  const ObAccuracy &accuracy, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx = *cast_obj_ctx.cast_ctx_;
  cast_ctx.dest_collation_ = column_schema->get_collation_type();
  const ObTableLoadTimeConverter time_cvrt = *cast_obj_ctx.time_cvrt_;
  if (src.is_null()) {
    dst.set_null();
  } else if (src.get_type_class() == ObStringTC &&
             (expect_type == ObNumberType || expect_type == ObUNumberType)) {
    ObNumberDesc d(0);
    uint32_t *digits = nullptr;
    cast_obj_ctx.number_fast_ctx_.reset();
    ObString tmp_str;
    if (OB_FAIL(src.get_varchar(tmp_str))) {
      LOG_WARN("fail to get varchar", KR(ret), K(src));
    } else if (OB_FAIL(number_fast_from(tmp_str.ptr(), tmp_str.length(), cast_ctx.allocator_v2_, d,
                                        digits, accuracy, cast_obj_ctx.number_fast_ctx_))) {
      if (ret == OB_EAGAIN) {
        if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, src, dst))) {
          LOG_WARN("fail to cast ObObj", KR(ret), K(src), K(expect_type));
        }
      } else {
        LOG_WARN("fail to cast ObObj", KR(ret), K(src), K(expect_type));
      }
    } else {
      dst.set_number(expect_type, d, digits);
    }
  } else if (src.get_type_class() == ObStringTC && expect_type == ObDateTimeType && lib::is_oracle_mode()) {
    ObCastMode cast_mode = cast_obj_ctx.cast_ctx_->cast_mode_;
    if (OB_FAIL(string_datetime_oracle(expect_type, cast_ctx, src, dst, cast_mode, time_cvrt))) {
      LOG_WARN("fail to convert string to datetime in oracle mode", KR(ret), K(src),
               K(expect_type));
    }
  } else {
    if (expect_type == ObJsonType) {
      cast_ctx.cast_mode_ |= CM_ERROR_ON_SCALE_OVER;
    }
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, src, dst))) {
      LOG_WARN("fail to cast ObObj", KR(ret), K(src), K(expect_type));
    }
  }
  return ret;
}

int ObTableLoadObjCaster::cast_obj_check(ObTableLoadCastObjCtx &cast_obj_ctx,
                                         const ObColumnSchemaV2 *column_schema, ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObObj *res_obj = &obj;
  ObCollationType collation_type = column_schema->get_collation_type();
  const ObAccuracy &accuracy = column_schema->get_accuracy();
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  bool is_fast_number = cast_obj_ctx.number_fast_ctx_.is_fast_number_;
   bool not_null_validate = (!column_schema->is_nullable() && lib::is_mysql_mode()) ||
                           (column_schema->is_not_null_enable_column() && lib::is_oracle_mode());
  if (obj.is_null() && not_null_validate && !column_schema->is_identity_column()) {
    const ObString &column_name = column_schema->get_column_name();
    ret = OB_BAD_NULL_ERROR;
    LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
  } else if (OB_FAIL(obj_collation_check(true, collation_type, *const_cast<ObObj *>(res_obj)))) {
    LOG_WARN("failed to check collation", KR(ret), K(collation_type), KPC(res_obj));
  } else if ((expect_type == ObNumberType || expect_type == ObUNumberType) && is_fast_number) {
    if (OB_FAIL(number_fast_cast_check(cast_obj_ctx.number_fast_ctx_, obj, accuracy))) {
      if (ret == OB_EAGAIN) {
        if (OB_FAIL(obj_accuracy_check(*cast_obj_ctx.cast_ctx_, accuracy, collation_type, *res_obj,
                                       obj, res_obj))) {
          LOG_WARN("failed to check accuracy", KR(ret), K(accuracy), K(collation_type),
                   KPC(res_obj));
        }
      } else {
        LOG_WARN("failed to check accuracy", KR(ret), K(obj), K(expect_type));
      }
    }
  } else if (OB_FAIL(obj_accuracy_check(*cast_obj_ctx.cast_ctx_, accuracy, collation_type, *res_obj,
                                        obj, res_obj))) {
    LOG_WARN("failed to check accuracy", KR(ret), K(accuracy), K(collation_type), KPC(res_obj));
  }
  return ret;
}

int ObTableLoadObjCaster::string_datetime_oracle(const ObObjType expect_type,
                                                 ObObjCastParams &params, const ObObj &in,
                                                 ObObj &out, const ObCastMode cast_mode,
                                                 const ObTableLoadTimeConverter &time_cvrt)
{
  int ret = OB_SUCCESS;
  ObString utf8_string;

  if (OB_UNLIKELY((ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class()) ||
                  ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input type", KR(ret), K(in), K(expect_type));
  } else if (in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid use of blob type", KR(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObExprUtil::convert_string_collation(
               in.get_string(), in.get_collation_type(), utf8_string,
               ObCharset::get_system_collation(), *params.allocator_v2_))) {
    LOG_WARN("fail to convert string collation", K(ret));
  } else {
    int64_t value = 0;
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, ObTimestampType == expect_type);
    cvrt_ctx.oracle_nls_format_ = params.dtc_params_.get_nls_format(ObDateTimeType);
    if (OB_FAIL(time_cvrt.str_to_datetime_oracle(utf8_string, cvrt_ctx, value))) {
      LOG_WARN("fail to convert str to date in oracle mode", KR(ret), K(utf8_string), K(value));
    } else if (CM_IS_ERROR_ON_SCALE_OVER(cast_mode) &&
               (value == ObTimeConverter::ZERO_DATE || value == ObTimeConverter::ZERO_DATETIME)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid date value", KR(ret), K(utf8_string));
    } else {
      out.set_datetime(value);
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
