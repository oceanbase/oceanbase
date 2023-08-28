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

#define USING_LOG_PREFIX COMMON

#include "ob_field.h"
#include "lib/timezone/ob_time_convert.h"
#include "common/ob_common_utility.h"
#include "rpc/obmysql/ob_mysql_global.h"

namespace oceanbase
{
using namespace lib;
namespace common
{

int ObParamedSelectItemCtx::deep_copy(const ObParamedSelectItemCtx &other, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator, other.paramed_cname_, paramed_cname_))) {
    LOG_WARN("failed to write stirng", K(ret));
  } else {
    param_str_offsets_ = other.param_str_offsets_;
    param_idxs_ = other.param_idxs_;
    neg_param_idxs_ = other.neg_param_idxs_;
    esc_str_flag_ = other.esc_str_flag_;
    need_check_dup_name_ = other.need_check_dup_name_;
    is_column_field_ = other.is_column_field_;
  }
  return ret;
}

int64_t ObParamedSelectItemCtx::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos,
                  " ParamedCtx: { paramed_cname: %.*s, esc_str_flag_: %d, need_check_dup_name: %d, "
                  "is_column_field_: %d}",
                  paramed_cname_.length(), paramed_cname_.ptr(),
                  esc_str_flag_, need_check_dup_name_, is_column_field_);
  return pos;
}

int64_t ObParamedSelectItemCtx::get_convert_size() const
{
  return paramed_cname_.length();
}

//only use in prepare stmt
int ObField::full_deep_copy(const ObField &other, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    LOG_WARN("null ptr");
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(ob_write_string(*allocator, other.dname_, dname_))) {
      LOG_WARN("ObStringBuf write Field.dname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.tname_, tname_))) {
      LOG_WARN("ObStringBuf write Field.tname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.org_tname_, org_tname_))) {
      LOG_WARN("ObStringBuf write Field.org_tname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.cname_, cname_))) {
      LOG_WARN("ObStringBuf write Field.cname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.org_cname_, org_cname_))) {
      LOG_WARN("ObStringBuf write Field.org_cname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.type_name_, type_name_))) {
      LOG_WARN("ObStringBuf write Field.type_name_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.type_owner_, type_owner_))) {
      LOG_WARN("ObStringBuf write Field.type_owner_ string error", K(ret));
    } else if (OB_FAIL(deep_copy_obj(*allocator, other.type_, type_))) {
      LOG_WARN("deep copy obj failed", K(ret), K(other.type_));
    } else if (OB_FAIL(deep_copy_obj(*allocator, other.default_value_, default_value_))) {
      LOG_WARN("deep copy obj failed", K(ret), K(other.default_value_));
    } else {
      accuracy_ = other.accuracy_;
      charsetnr_ = other.charsetnr_;
      flags_ = other.flags_;
      length_ = other.length_;
      is_hidden_rowid_ = other.is_hidden_rowid_;
      inout_mode_ = other.inout_mode_;
      is_paramed_select_item_ = other.is_paramed_select_item_;
      if (is_paramed_select_item_ && NULL != other.paramed_ctx_) {
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator->alloc(sizeof(ObParamedSelectItemCtx)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocator memory", K(ret));
        } else if (FALSE_IT(paramed_ctx_ = new(buf)ObParamedSelectItemCtx())) {
          // do nothing
        } else if (OB_FAIL(paramed_ctx_->deep_copy(*other.paramed_ctx_, allocator))) {
          LOG_WARN("failed to deep copy paramed ctx", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObField::deep_copy(const ObField &other, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    LOG_WARN("null ptr");
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(ob_write_string(*allocator, other.dname_, dname_))) {
      LOG_WARN("ObStringBuf write Field.dname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.tname_, tname_))) {
      LOG_WARN("ObStringBuf write Field.tname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.org_tname_, org_tname_))) {
      LOG_WARN("ObStringBuf write Field.org_tname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.cname_, cname_))) {
      LOG_WARN("ObStringBuf write Field.cname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.org_cname_, org_cname_))) {
      LOG_WARN("ObStringBuf write Field.org_cname_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.type_name_, type_name_))) {
      LOG_WARN("ObStringBuf write Field.type_name_ string error", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator, other.type_owner_, type_owner_))) {
      LOG_WARN("ObStringBuf write Field.type_name_ string error", K(ret));
    } else {
      // Attention: These two members are not deep copied.
      type_ = other.type_;
      default_value_ = other.default_value_;

      accuracy_ = other.accuracy_;
      charsetnr_ = other.charsetnr_;
      flags_ = other.flags_;
      length_ = other.length_;
      is_hidden_rowid_ = other.is_hidden_rowid_;
      inout_mode_ = other.inout_mode_;

      is_paramed_select_item_ = other.is_paramed_select_item_;
      if (is_paramed_select_item_ && NULL != other.paramed_ctx_) {
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator->alloc(sizeof(ObParamedSelectItemCtx)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocator memory", K(ret));
        } else if (FALSE_IT(paramed_ctx_ = new(buf)ObParamedSelectItemCtx())) {
          // do nothing
        } else if (OB_FAIL(paramed_ctx_->deep_copy(*other.paramed_ctx_, allocator))) {
          LOG_WARN("failed to deep copy paramed ctx", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObField, type_.meta_, type_owner_, type_name_, accuracy_, flags_, length_,
                    cname_);

//deep copy size
int64_t ObField::get_convert_size() const
{
  int64_t convert_size = sizeof(ObField);
  convert_size += dname_.length();
  convert_size += tname_.length();
  convert_size += org_tname_.length();
  convert_size += org_cname_.length();
  convert_size += cname_.length();
  if (is_paramed_select_item_ && NULL != paramed_ctx_) {
    convert_size += paramed_ctx_->get_convert_size();
  }
  convert_size += type_.get_deep_copy_size();
  convert_size += type_name_.length();
  convert_size += type_owner_.length();
  convert_size += default_value_.get_deep_copy_size();
  return convert_size;
}

int64_t ObField::to_string(char *buffer, int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buffer, len, pos,
                  "dname:%.*s, tname: %.*s, org_tname: %.*s, "
                  "cname: %.*s, org_cname: %.*s, type: %s, "
                  "type_owner: %.*s, type_name: %.*s,"
                  "charset: %hu, "
                  "decimal_scale: %hu, flags: %x, inout_mode_: %x"
                  "is_paramed_select_item: %d,"
                  "is_hidden_rowid: %d",
                  dname_.length(), dname_.ptr(),
                  tname_.length(), tname_.ptr(),
                  org_tname_.length(), org_tname_.ptr(),
                  cname_.length(), cname_.ptr(),
                  org_cname_.length(), org_cname_.ptr(),
                  to_cstring(type_),
                  type_owner_.length(), type_owner_.ptr(),
                  type_name_.length(), type_name_.ptr(),
                  charsetnr_, accuracy_.get_scale(), flags_,
                  inout_mode_,
                  is_paramed_select_item_,
                  is_hidden_rowid_);
  if (is_paramed_select_item_ && NULL != paramed_ctx_) {
    pos = paramed_ctx_->to_string(buffer + pos, len - pos) + pos;
  }
  return pos;
}



// After the constant parameterization of the projection column, the type deduction logic of the field cannot go through it again after hitting the plan.
// There is no way to get the accuracy of the result type (accuracy is always the accuracy of the first hard analysis)
// If it is select col, such as
// create table t (a int(255) zerofill); insert into t values (1);
// select lengt(a) from t;
// The result is 255, but
// select (a + a) from t;
// The result is 1, so if it is a computable expression, the length of zerofill cannot be inherited from the schema.
// To ensure safety, all accuracy-related length derivations (except character types) are replaced by the maximum value of the relevant type
// If it is a column, then you need to use the type to overturn the length
int ObField::update_field_mb_length()
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass &tc = ob_obj_type_class(type_.get_type());
  if (!is_paramed_select_item_) {
    // do nothing
  } else if (OB_ISNULL(paramed_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null argument", K(ret), K(paramed_ctx_));
  } else if (paramed_ctx_->is_column_field_) {
    // do nothing
  } else {
    switch (tc) {
    case ObIntTC:
    case ObUIntTC:
      length_ = ObAccuracy::MAX_ACCURACY[type_.get_type()].precision_;
      break;
    case ObNumberTC:
      length_ = number::ObNumber::MAX_PRECISION - number::ObNumber::MIN_SCALE;
      break;
    case ObDateTimeTC:
      length_ = DATETIME_MIN_LENGTH + OB_MAX_DATETIME_PRECISION;
      break;
    case ObOTimestampTC: {
      length_ = DATETIME_MIN_LENGTH + OB_MAX_TIMESTAMP_TZ_PRECISION;
      if (ObTimestampTZType == type_.get_type()) {
        length_ += static_cast<int32_t>(OB_MAX_TZ_ABBR_LEN + OB_MAX_TZ_NAME_LEN);
      }
      break;
    }
    case ObTimeTC:
      length_ = TIME_MIN_LENGTH + OB_MAX_DATETIME_PRECISION;
      break;
    case ObFloatTC:
      length_ = MAX_FLOAT_STR_LENGTH;
      break;
    case ObDoubleTC:
      length_ = MAX_DOUBLE_STR_LENGTH;
      break;
    case ObBitTC:
      length_ = ObAccuracy::MAX_ACCURACY[type_.get_type()].precision_;
      break;
    case ObEnumSetTC:
    case ObEnumSetInnerTC:
    case ObTextTC:
    case ObLobTC:
    case ObStringTC:
    case ObRawTC:
    case ObDateTC:
    case ObYearTC:
    case ObNullTC:
    case ObJsonTC:
    case ObGeometryTC:
      break; // do nothing
    default:
      ret = OB_ERR_UNEXPECTED;
      length_ = 0;
      break;
    }
  }
  return ret;
}

int ObField::get_field_mb_length(const ObObjType type,
                                 const ObAccuracy &accuracy,
                                 const ObCollationType charsetnr,
                                 int32_t &length)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass &tc = ob_obj_type_class(type);
  switch(tc) {
    case ObIntTC:
    case ObUIntTC:
      if (OB_LIKELY(accuracy.get_precision() > 0)) {
        length = accuracy.get_precision();
      } else {
        length = ObAccuracy::MAX_ACCURACY[type].precision_;
        //LOG_WARN("max precision is used. something bad may have happened", K(ret), K(length), K(common::lbt()));
      }
      break;
    case ObDateTC:
      length = DATE_MIN_LENGTH;
      break;
    case ObYearTC:
      length = 4; //accuracy.get_precision();
      break;
    case ObNumberTC:
      //already checked the validity of precision and scale for both oracle/mysql mode several times
      //here only do the length calculation
      if (accuracy.get_precision() >= 0 && accuracy.get_scale() >= 0) {
        if (OB_UNLIKELY(accuracy.get_precision() < accuracy.get_scale())) {
          //oracle:select cast(0.0002 as number(1, 4)) from dual; => 0.0002
          length = accuracy.get_scale() + 2;
         // ret = OB_ERR_UNEXPECTED;
         // LOG_WARN("invalid accuracy. precision is smaller than scale", K(ret), K(accuracy));
        } else {
          //for both Oracle & MySql
          length = my_decimal_precision_to_length_no_truncation(accuracy.get_precision(),
                                                                accuracy.get_scale(),
                                                                type == ObUNumberType);
        }
      } else if (accuracy.get_precision() >= 1
          && accuracy.get_scale() >= number::ObNumber::MIN_SCALE
          && accuracy.get_scale() < 0) {
        length = accuracy.get_precision() + accuracy.get_scale();
      } else if (accuracy.get_precision() == PRECISION_UNKNOWN_YET
              || accuracy.get_scale() == NUMBER_SCALE_UNKNOWN_YET) {
        //1 for point.
        //length = number::ObNumber::MAX_PRECISION + 1 + (type == ObUNumberType ? 0 : 1);
        //max length is number(65, -84) => 149
        length = number::ObNumber::MAX_PRECISION - number::ObNumber::MIN_SCALE;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. invalid precision or scale", K(ret), K(accuracy));
      }
      break;
    case ObTextTC: // TODO@hanhui texttc share with the stringtc temporarily
    case ObLobTC:
    case ObJsonTC:
    case ObGeometryTC:
    case ObStringTC: {
      // This if branch is a patch because the generation process of Operators such as CAST and CONV is not standardized.
      // As a result, length, collation, etc. are not set correctly
      if (CS_TYPE_INVALID == charsetnr || accuracy.get_length() < 0) {
        length = static_cast<ObLength>(MAX_COLUMN_VARCHAR_LENGTH);
        //LOG_WARN("negative input length. something bad may have happened", K(ret), K(accuracy), K(charsetnr), K(common::lbt()));
      } else if (is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics())) {
        length = accuracy.get_length();
      } else {
        int64_t mbmaxlen = 1;
        if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(charsetnr, mbmaxlen))) {
          LOG_WARN("fail to get mbmaxlen", K(charsetnr), K(ret));
        } else {
          if (lib::is_mysql_mode() && tc == ObTextTC) {
            // compat mysql-jdbc 8.x for judge text type by length
            length = static_cast<uint32_t>(ObAccuracy::MAX_ACCURACY[type].get_length() - 1);
          } else {
            length = static_cast<uint32_t>(accuracy.get_length() * mbmaxlen);
          }
        }
      }
      break;
    }
    case ObRawTC:
    case ObRowIDTC:
      length = accuracy.get_length();
      break;
    case ObDateTimeTC:
      length = DATETIME_MIN_LENGTH + ((accuracy.get_scale() > 0) ? (1 + accuracy.get_scale()) : 0); /* 1 represents the decimal point in 12:12:12.3333 */
      break;
    case ObOTimestampTC:
      length = DATETIME_MIN_LENGTH + ((accuracy.get_scale() > 0) ? (1 + accuracy.get_scale()) : 0); /* 1 represents the decimal point in 12:12:12.3333xxxx */
      if (ObTimestampTZType == type) {
        //for oracle timestamp tz, use max length
        length += static_cast<int32_t>(OB_MAX_TZ_ABBR_LEN + OB_MAX_TZ_NAME_LEN);
      }
      break;
    case ObIntervalTC:
      if (ObIntervalYMType == type) {
        int8_t year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(accuracy.get_scale()));
        length = 4 + year_scale;/*+99-01*/
      } else {
        int8_t day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(accuracy.get_scale()));
        int8_t fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(accuracy.get_scale()));
        length = 10 + day_scale + (fs_scale > 0 ? (1 + fs_scale) : 0);  /*+12 01:01:01.123*/
      }
      break;
    case ObTimeTC:
      /* KNOWN ISSUE:
       * 800:11:11，这时候TIME Length = 10
       * 11:11:11， 这时候TIME Length = 8
       */
      length = TIME_MIN_LENGTH + ((accuracy.get_scale() > 0) ? (1 + accuracy.get_scale()) : 0); /* 1 represents the decimal point in 12:12:12.3333 */
      break;
    case ObFloatTC:
      if (accuracy.get_precision() >= 0 && accuracy.get_scale() >= 0) {
        length = accuracy.get_precision();
      } else if (lib::is_oracle_mode()) {
        length = MAX_FLOAT_STR_LENGTH;
      } else if (accuracy.get_precision() == DEFAULT_FLOAT_PRECISION
              || accuracy.get_scale() == DEFAULT_FLOAT_SCALE) {
        length = MAX_FLOAT_STR_LENGTH;
        // When create table is not specified, the default value will be used. Affects the output of zerofill and must be set
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. invalid precision or scale", K(ret), K(accuracy));
      }
      break;
    case ObDoubleTC:
      if (accuracy.get_precision() >= 0 && accuracy.get_scale() >= 0) {
        length = accuracy.get_precision();
      } else if (lib::is_oracle_mode()) {
        length = MAX_DOUBLE_STR_LENGTH;
      } else if (accuracy.get_precision() == DEFAULT_DOUBLE_PRECISION
              || accuracy.get_scale() == DEFAULT_DOUBLE_SCALE) {
        length = MAX_DOUBLE_STR_LENGTH + 1;
        // When create table is not specified, the default value will be used. Affects the output of zerofill and must be set
        //In DML, if length is set to MAX_DOUBLE_STR_LENGTH, it will be 1 less than mysql.
        //Security considerations, here is set to MAX_DOUBLE_STR_LENGTH plus 1
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. invalid precision or scale", K(ret), K(accuracy));
      }
      break;
    case ObNullTC:
      length = 0;
      break;
    case ObBitTC:
      if (OB_LIKELY(accuracy.get_precision() > 0)) {
        length = accuracy.get_precision();
      } else {
        length = ObAccuracy::MAX_ACCURACY[type].precision_;
        //LOG_WARN("max precision is used. something bad may have happened",K(ret), K(accuracy.get_precision()), K(length));
      }
      break;
    case ObEnumSetTC:
    case ObEnumSetInnerTC:
      if (accuracy.get_length() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("length is less than zero", K(accuracy), K(type), K(ret));
      } else {
        int64_t mbmaxlen = 1;
        if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(charsetnr, mbmaxlen))) {
          LOG_WARN("fail to get mbmaxlen", K(charsetnr), K(ret));
        } else {
          length = static_cast<uint32_t>(accuracy.get_length() * mbmaxlen);
        }
      }
      break;
    case ObExtendTC:
    case ObUserDefinedSQLTC:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported get_field_mb_length for extend type", K(ret));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      length = 0; // mute
      break;
  }
  return ret;
}

}
}
