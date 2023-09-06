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
 *
 * obj2str helper
 */

#include <cstdlib>                                  // std::abs

#include "ob_obj2str_helper.h"
#include "ob_log_timezone_info_getter.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/string/ob_sql_string.h"
#include "sql/engine/expr/ob_datum_cast.h"          // padding_char_for_cast
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/geo/ob_geo_utils.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_util.h"
#endif
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_res_type_map.h"

#include "ob_log_utils.h"                           // _M_

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
const char* ObObj2strHelper::EMPTY_STRING = "";

ObObj2strHelper::ObObj2strHelper() : inited_(false),
                                     timezone_info_getter_(NULL),
                                     hbase_util_(NULL),
                                     enable_hbase_mode_(false),
                                     enable_convert_timestamp_to_unix_timestamp_(false),
                                     enable_backup_mode_(false),
                                     tenant_mgr_(NULL)
{
}

ObObj2strHelper::~ObObj2strHelper()
{
  destroy();
}

int ObObj2strHelper::init(IObCDCTimeZoneInfoGetter &timezone_info_getter,
    ObLogHbaseUtil &hbase_util,
    const bool enable_hbase_mode,
    const bool enable_convert_timestamp_to_unix_timestamp,
    const bool enable_backup_mode,
    IObLogTenantMgr &tenant_mgr)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_ob_charset_utils())) {
    OBLOG_LOG(ERROR, "failed to init ob charset util!", KR(ret));
  } else {
    timezone_info_getter_ = &timezone_info_getter;
    hbase_util_ = &hbase_util;
    enable_hbase_mode_ = enable_hbase_mode;
    enable_convert_timestamp_to_unix_timestamp_ = enable_convert_timestamp_to_unix_timestamp;
    enable_backup_mode_ = enable_backup_mode;
    tenant_mgr_ = &tenant_mgr;
    inited_ = true;
  }
  return ret;
}

int ObObj2strHelper::init_ob_charset_utils()
{
  int ret = common::OB_SUCCESS;
  lib::ObMallocAllocator *allocator = NULL;
  const lib::ObMemAttr attr(common::OB_SYS_TENANT_ID, ObModIds::OB_NUMBER);
  if (OB_FAIL(sql::ObExprTRDateFormat::init())) {
    OBLOG_LOG(ERROR, "failed to init vars in oracle trunc", KR(ret));
  } else if (OB_FAIL(sql::ObExprUuid::init())) {
    OBLOG_LOG(ERROR, "failed to init vars in uuid", KR(ret));
  } else if (OB_ISNULL(allocator = lib::ObMallocAllocator::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    OBLOG_LOG(ERROR, "allocator is null", KR(ret));
  } else if (OB_FAIL(common::ObNumberConstValue::init(*allocator, attr))) {
    OBLOG_LOG(ERROR, "failed to init ObNumberConstValue", KR(ret));
  } else if (OB_FAIL(sql::ARITH_RESULT_TYPE_ORACLE.init())) {
    OBLOG_LOG(ERROR, "failed to init ORACLE_ARITH_RESULT_TYPE", KR(ret));
  } else if (OB_FAIL(ObCharsetUtils::init(*allocator))) {
    OBLOG_LOG(ERROR, "fail to init ObCharsetUtils", KR(ret));
  }
  return ret;
}

void ObObj2strHelper::destroy()
{
  inited_ = false;
  timezone_info_getter_ = NULL;
  hbase_util_ = NULL;
  enable_hbase_mode_ = false;
  enable_convert_timestamp_to_unix_timestamp_ = false;
  enable_backup_mode_ = false;
  tenant_mgr_ = NULL;
}


//extended_type_info used for enum/set
int ObObj2strHelper::obj2str(const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t column_id,
    const common::ObObj &obj,
    common::ObString &str,
    common::ObIAllocator &allocator,
    const bool string_deep_copy,
    const common::ObIArray<common::ObString> &extended_type_info,
    const common::ObAccuracy &accuracy,
    const common::ObCollationType &collation_type,
    const ObTimeZoneInfoWrap *tz_info_wrap) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = obj.get_type();
  common::ObObjTypeClass obj_tc = common::ob_obj_type_class(obj_type);
  lib::Worker::CompatMode compat_mode = THIS_WORKER.get_compatibility_mode();

  // Configure allowed conversions: mysql timestamp column -> UTC integer time
  if (ObTimestampType == obj_type && enable_convert_timestamp_to_unix_timestamp_) {
    if (OB_FAIL(convert_mysql_timestamp_to_utc_(obj, str, allocator))) {
      OBLOG_LOG(ERROR, "convert_mysql_timestamp_to_utc_ fail", KR(ret), K(table_id), K(column_id), K(obj), K(obj_type),
          K(str));
    }
  } else if (common::ObNullTC == obj_tc) {
    str.assign_ptr(NULL, 0);
  } else if (common::ObExtendTC == obj_tc) {
    static const int64_t MAX_EXT_PRINT_LEN = 1 << 10;
    char BUFFER[MAX_EXT_PRINT_LEN];
    int64_t pos = 0;
    char *ptr = NULL;

    if (OB_FAIL(obj.print_sql_literal(BUFFER, sizeof(BUFFER), pos))
        || pos <= 0) {
      OBLOG_LOG(ERROR, "obj print_sql_literal fail", KR(ret), K(obj), K(MAX_EXT_PRINT_LEN), K(pos));
      ret = common::OB_SUCCESS == ret ? common::OB_ERR_UNEXPECTED : ret;
    } else if (NULL == (ptr = (char *)allocator.alloc(pos))) {
      OBLOG_LOG(ERROR, "allocate memory fail", "size", pos);
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      (void)MEMCPY(ptr, BUFFER, pos);
      str.assign_ptr(ptr, (int32_t)pos);
      OBLOG_LOG(DEBUG, "obj2str cast extend type", K(obj), "cast_str", str);
    }
  } else if (obj.is_xml_sql_type()) {
    if (OB_FAIL(convert_xmltype_to_text_(obj, str, allocator))) {
      OBLOG_LOG(ERROR, "convert_xmltype_to_text_ fail", KR(ret), K(table_id), K(column_id),
          K(obj), K(obj_type), K(str));
    }
  } else if (ObGeometryType == obj_type) {
    if (OB_FAIL(convert_ob_geometry_to_ewkt_(obj, str, allocator))) {
      OBLOG_LOG(ERROR, "convert_ob_geometry_to_ewkt_ fail", KR(ret), K(table_id), K(column_id),
          K(obj), K(obj_type), K(str));
    }
  // This should be before is_string_type, because for char/nchar it is also ObStringTC, so is_string_type=true
  } else if (need_padding_(compat_mode, obj)) {
    if (OB_FAIL(convert_char_obj_to_padding_obj_(compat_mode, obj, accuracy, collation_type, allocator, str))) {
      OBLOG_LOG(ERROR, "convert_char_obj_to_padding_obj_ fail", KR(ret), K(obj), K(accuracy), K(collation_type),
          K(str), K(compat_mode), "compat_mode_str", print_compat_mode(compat_mode));
    }
  } else if (obj.is_string_type()) {
    if (string_deep_copy) {
      // need deep-copy
      void *dst_buf = NULL;
      ObString src_str = obj.get_string();
      int64_t str_len = obj.get_val_len();

      if (str_len > 0) {
        if (OB_ISNULL(dst_buf = allocator.alloc(str_len))) {
          OBLOG_LOG(ERROR, "allocate memory fail", K(str_len));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(dst_buf, src_str.ptr(), src_str.length());
        }
      }

      if (OB_SUCCESS == ret) {
        str.assign_ptr(static_cast<char *>(dst_buf),
            static_cast<ObString::obstr_size_t>(str_len));
      }
    } else {
      // No deep copy required, direct reference to original string memory
      if (OB_FAIL(obj.get_string(str))) {
        OBLOG_LOG(ERROR, "get_string from ObObj fail", KR(ret), K(obj));
      } else {
        // success
      }
    }

    if (OB_SUCC(ret)) {
      // For a varchar with a default value of '', str_len=0, the empty string should be synchronised and not output as NULL
      if (0 == obj.get_val_len()) {
        str.assign_ptr(EMPTY_STRING, static_cast<ObString::obstr_size_t>(obj.get_val_len()));
      }
    }
  } else {
    common::ObObj tmp_inner_obj;
    const common::ObObj *in_obj = &obj;
    ObObjMeta inner_meta;
    inner_meta.set_collation_level(CS_LEVEL_NUMERIC);
    inner_meta.set_collation_type(CS_TYPE_BINARY);
    if (obj.is_enum() || obj.is_set()) {
      if (OB_FAIL(ObObjCaster::enumset_to_inner(inner_meta, obj, tmp_inner_obj,
                                                allocator, extended_type_info))) {
        OBLOG_LOG(ERROR, "fail to enumset_to_inner", KR(ret));
      } else {
        in_obj = &tmp_inner_obj;
      }
    }

    if (OB_SUCC(ret)) {
      common::ObObj str_obj;
      common::ObObjType target_type = common::ObMaxType;
      ObCDCTenantTimeZoneInfo *obcdc_tenant_tz_info = nullptr;

      if (OB_ISNULL(timezone_info_getter_)) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "timezone_info_getter_ is null", K(timezone_info_getter_));
      } else if (OB_FAIL(timezone_info_getter_->get_tenant_tz_info(tenant_id, obcdc_tenant_tz_info))) {
        OBLOG_LOG(ERROR, "get_tenant_tz_wrap failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(obcdc_tenant_tz_info)) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "tenant_tz_info not valid", KR(ret), K(tenant_id));
      } else {
        // obcdc need use_standard_format
        const common::ObTimeZoneInfo *tz_info = obcdc_tenant_tz_info->get_timezone_info();
        const ObDataTypeCastParams dtc_params(tz_info);
        ObObjCastParams cast_param(&allocator, &dtc_params, CM_NONE, collation_type);
        cast_param.format_number_with_limit_ = false;//here need no limit format number for libobcdc

        if (in_obj->is_bit()) {
          target_type = common::ObUInt64Type;
        } else {
          target_type = common::ObVarcharType;
        }

        if (OB_FAIL(ObObjCaster::to_type(target_type, cast_param, *in_obj, str_obj))) {
          OBLOG_LOG(ERROR, "cast obj to varchar type fail", KR(ret), KPC(in_obj), K(target_type));
          if (OB_ERR_INVALID_TIMEZONE_REGION_ID == ret) {
            // Refresh timezone until successful and convert again
            ret = OB_SUCCESS;

            if (OB_FAIL(convert_timestamp_with_timezone_data_util_succ_(target_type, cast_param,
                    *in_obj, str_obj, str, tenant_id))) {
              OBLOG_LOG(ERROR, "convert_timestamp_with_timezone_data_util_succ_ fail", KR(ret), KPC(in_obj), K(target_type));
            }
          }
        } else {
          if (in_obj->is_bit()) {
            if (OB_FAIL(convert_bit_obj_to_decimal_str_(obj, str_obj, str, allocator))) {
              OBLOG_LOG(ERROR, "convert_bit_obj_to_decimal_str_ fail", KR(ret), K(obj), K(target_type));
            }
          } else {
            if (OB_FAIL(str_obj.get_string(str))) {
              OBLOG_LOG(ERROR, "get_string from ObObj fail", KR(ret), K(str_obj));
            } else {
              // For a varchar with a default value of '', str_len=0, the empty string should be synchronised and not output as NULL
              if ((obj.is_enum() || obj.is_set()) && 0 == str_obj.get_val_len()) {
                str.assign_ptr(EMPTY_STRING, static_cast<ObString::obstr_size_t>(str_obj.get_val_len()));
              }
            }
          }
        }
      }

      // 1. hbase table T column timestamp type should be converted to positive if it is negative
      // 2. not converted in backup mode
      if (OB_SUCC(ret)) {
        bool is_hbase_table_T_column = false;

        if (obj.is_int() && enable_hbase_mode_ && ! enable_backup_mode_) {
          if (OB_ISNULL(hbase_util_)) {
            OBLOG_LOG(ERROR, "hbase_util_ is null", K(hbase_util_));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(hbase_util_->judge_hbase_T_column(table_id, column_id, is_hbase_table_T_column))) {
            OBLOG_LOG(ERROR, "hbase_util_ judge_hbase_T_column fail", KR(ret), K(table_id), K(column_id), K(is_hbase_table_T_column));
          } else if (! is_hbase_table_T_column) {
            // do nothing
          } else {
            if (OB_FAIL(convert_hbase_bit_obj_to_positive_bit_str_(obj, str_obj, str, allocator))) {
              OBLOG_LOG(ERROR, "convert_hbase_bit_obj_to_positive_bit_str_ fail", KR(ret), K(obj), K(target_type));
            }
          }
        }
        OBLOG_LOG(DEBUG, "[HBASE]", KR(ret), K(obj), K(obj_type), K(enable_hbase_mode_), K(enable_backup_mode_),
            K(str_obj), K(is_hbase_table_T_column), K(table_id));
      }
    } // OB_SUCC(ret)
  }

  // If it is a LOB, larger than 2M, do not print the contents, print the address and length
  // Avoid printing the log taking too long
  if (str.length() > 2 * _M_) {
    OBLOG_LOG(DEBUG, "obj2str", KR(ret), K(obj_type), K(obj.get_scale()), K(obj.get_meta()), K(obj_tc), K(accuracy), K(collation_type),
        KP(obj.get_string().ptr()), K(obj.get_string().length()), KP(str.ptr()), K(str.length()));
  } else {
    OBLOG_LOG(DEBUG, "obj2str", KR(ret), K(obj_type), K(obj.get_scale()), K(obj.get_meta()), K(obj_tc), K(accuracy), K(collation_type),
        K(obj), K(str), K(str.length()));
  }

  return ret;
}

int ObObj2strHelper::convert_timestamp_with_timezone_data_util_succ_(const common::ObObjType &target_type,
    common::ObObjCastParams &cast_param,
    const common::ObObj &in_obj,
    common::ObObj &str_obj,
    common::ObString &str,
    const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  bool done = false;

  if (OB_ISNULL(timezone_info_getter_)) {
    ret = OB_ERR_UNEXPECTED;
    OBLOG_LOG(ERROR, "timezone_info_getter_ is null", KR(ret), K(timezone_info_getter_));
  } else {
    while (! done && OB_SUCC(ret)) {
      if (OB_FAIL(timezone_info_getter_->refresh_tenant_timezone_info_until_succ(tenant_id))) {
        OBLOG_LOG(ERROR, "fetch_tenant_timezone_info_util_succ fail", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObObjCaster::to_type(target_type, cast_param, in_obj, str_obj))) {
        if (OB_ERR_INVALID_TIMEZONE_REGION_ID == ret) {
          OBLOG_LOG(WARN, "cast obj to varchar type fail, try again", KR(ret), K(in_obj), K(target_type));
        } else {
          OBLOG_LOG(ERROR, "cast obj to varchar type fail", KR(ret), K(in_obj), K(target_type));
        }
      } else {
        done = true;
        if (OB_FAIL(str_obj.get_string(str))) {
          OBLOG_LOG(ERROR, "get_string from ObObj fail", KR(ret), K(str_obj));
        }
      }

      if (OB_ERR_INVALID_TIMEZONE_REGION_ID == ret) {
        ret = OB_SUCCESS;
        ob_usleep(10L * 1000L);
      }
    }
  }

  return ret;
}

// bit type output decimal string
int ObObj2strHelper::convert_bit_obj_to_decimal_str_(const common::ObObj &obj,
    const common::ObObj &str_obj,
    common::ObString &str,
    common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;

  if (OB_FAIL(str_obj.get_uint64(value))) {
    OBLOG_LOG(ERROR, "get_uint64 from ObObj fail", KR(ret), K(obj), K(str_obj), K(value));
  } else {
    char buf[MAX_BIT_DECIMAL_STR_LENGTH];
    int64_t pos = 0;
    char *ptr = NULL;

    if (OB_FAIL(common::databuff_printf(buf, MAX_BIT_DECIMAL_STR_LENGTH, pos, "%lu", value))) {
      OBLOG_LOG(ERROR, "databuff_printf fail", K(pos), K(value));
    } else if (OB_ISNULL(ptr = (char *)allocator.alloc(pos))) {
      OBLOG_LOG(ERROR, "allocate memory fail", "size", pos);
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      (void)MEMCPY(ptr, buf, pos);
      str.assign_ptr(ptr, (int32_t)pos);
      OBLOG_LOG(DEBUG, "obj2str cast bit type", K(obj), "cast_str", str, K(value));
    }
  }

  return ret;
}

int ObObj2strHelper::convert_hbase_bit_obj_to_positive_bit_str_(const common::ObObj &obj,
    const common::ObObj &current_str_obj,
    common::ObString &str,
    common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);

  ObString current_str;
  const char *current_str_ptr = NULL;
  ObString::obstr_size_t current_str_len = 0;

  if (OB_FAIL(current_str_obj.get_string(current_str))) {
    OBLOG_LOG(ERROR, "get_string from ObObj fail", KR(ret), K(obj), K(current_str_obj), K(current_str));
  } else if (OB_ISNULL(current_str_ptr = current_str.ptr())) {
    OBLOG_LOG(ERROR, "current_str_ptr is null", K(obj), K(current_str_obj), K(current_str), K(current_str_ptr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    current_str_len = current_str.length();
    OBLOG_LOG(DEBUG, "[HBASE]", K(obj), K(current_str_obj), K(current_str), K(current_str_ptr), K(current_str_len));

    if ('-' == current_str_ptr[0]) {
      str.assign_ptr(current_str_ptr + 1, (int32_t)(current_str_len - 1));
    }
  }

  return ret;
}

int ObObj2strHelper::convert_mysql_timestamp_to_utc_(const common::ObObj &obj,
    common::ObString &str,
    common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t utc_time = obj.get_timestamp();
  char buf[MAX_TIMESTAMP_UTC_LONG_STR_LENGTH];
  int64_t pos = 0;
  char *ptr = NULL;
  // external output of utc integer time, compatible with mysql, splitting seconds and microseconds with a decimal point
  // Microsecond precision length of 6
  // server record UTC Timestamp with standard timezone(+0000). and obcdc will output this with
  // format "second.usec"
  const int64_t usec_mod_val = 1000000;

  if (OB_FAIL(common::databuff_printf(buf, MAX_TIMESTAMP_UTC_LONG_STR_LENGTH, pos, "%ld.%06ld",
          utc_time / usec_mod_val, std::abs(utc_time) % usec_mod_val))) {
    OBLOG_LOG(ERROR, "databuff_printf fail", K(pos), K(utc_time), K(usec_mod_val));
  } else if (OB_ISNULL(ptr = (char *)allocator.alloc(pos))) {
    OBLOG_LOG(ERROR, "allocate memory fail", "size", pos);
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    (void)MEMCPY(ptr, buf, pos);
    str.assign_ptr(ptr, (int32_t)pos);
    OBLOG_LOG(DEBUG, "obj2str cast timestamp type to utc long", K(obj), "cast_str", str, K(utc_time));
  }

  return ret;
}

int ObObj2strHelper::convert_ob_geometry_to_ewkt_(const common::ObObj &obj,
    common::ObString &str,
    common::ObIAllocator &allocator) const
{
  const ObString &wkb = obj.get_string();
  return ObGeoTypeUtil::geo_to_ewkt(wkb, str, allocator, 0);
}

int ObObj2strHelper::convert_xmltype_to_text_(
    const common::ObObj &obj,
    common::ObString &str,
    common::ObIAllocator &allocator) const
{
#ifdef OB_BUILD_ORACLE_XML
  const ObString &data = obj.get_string();
  return ObXmlUtil::xml_bin_to_text(allocator, data, str);
#else
  return OB_NOT_SUPPORTED;
#endif
}

bool ObObj2strHelper::need_padding_(const lib::Worker::CompatMode &compat_mode,
    const common::ObObj &obj) const
{
  bool bool_ret = false;

  bool_ret = (lib::Worker::CompatMode::ORACLE == compat_mode)
    && (obj.is_char() || obj.is_nchar());

  return bool_ret;
}

int ObObj2strHelper::convert_char_obj_to_padding_obj_(const lib::Worker::CompatMode &compat_mode,
    const common::ObObj &obj,
    const common::ObAccuracy &accuracy,
    const common::ObCollationType &collation_type,
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  int32_t char_len = 0;

  if (OB_FAIL(obj.get_string(str))) {
    OBLOG_LOG(ERROR, "get_string from ObObj fail", KR(ret), K(obj));
  } else if (OB_FAIL(obj.get_char_length(accuracy, char_len, lib::Worker::CompatMode::ORACLE == compat_mode))) {
    OBLOG_LOG(ERROR, "obj get_char_length fail", KR(ret), K(accuracy), K(char_len));
  } else {
    // The calculation of padding here needs to be based on char_len, not str.length
    // e.g. nchar, 'a', str,length=2, not 1
    const int64_t padding_cnt = accuracy.get_length() - char_len;
    // need pad
    if (padding_cnt > 0) {
      ObString padding_res;

      if (OB_FAIL(sql::padding_char_for_cast(padding_cnt, collation_type, allocator, padding_res))) {
        OBLOG_LOG(ERROR, "padding_char_for_cast fail", KR(ret), K(obj), K(accuracy), K(collation_type),
            K(padding_res));
      } else {
        int64_t all_size = padding_res.length() + str.length();
        char *res_ptr = static_cast<char*>(allocator.alloc(all_size));

        if (OB_ISNULL(res_ptr)) {
          OBLOG_LOG(ERROR, "allocate memory failed", KR(ret));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMMOVE(res_ptr, str.ptr(), str.length());
          MEMMOVE(res_ptr + str.length(), padding_res.ptr(), padding_res.length());
          str.assign_ptr(res_ptr, static_cast<ObString::obstr_size_t>(all_size));

          OBLOG_LOG(DEBUG, "obj2str cast char/nchar type", K(obj), "cast_str", str, "cast_str_len", str.length(),
              K(padding_cnt), K(padding_res), "padding_res_len", padding_res.length(),
              K(accuracy), K(collation_type), K(char_len));
        }
      }
    } // if (padding_cnt > 0)
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
