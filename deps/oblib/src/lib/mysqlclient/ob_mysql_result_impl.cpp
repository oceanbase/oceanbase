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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include <mysql.h>
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/time/ob_time_utility.h"
#include "common/object/ob_object.h"
#include "lib/timezone/ob_time_convert.h"
#include "common/ob_zerofill_info.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/string/ob_hex_utils_base.h"
#include "lib/mysqlclient/ob_dblink_error_trans.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
ObMySQLResultImpl::ObMySQLResultImpl(ObMySQLStatement &stmt) :
    stmt_(stmt),
    result_(NULL),
    cur_row_(NULL),
    result_column_count_(0),
    cur_row_result_lengths_(NULL),
    fields_(NULL)
{
}

ObMySQLResultImpl::~ObMySQLResultImpl()
{
  close();
  // must call close() before destroy
}

int ObMySQLResultImpl::init(bool enable_use_result)
{
  int ret = OB_SUCCESS;
  int64_t field_count = 0;
  MYSQL *stmt = NULL;
  close();
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt handler is null", K(ret));
  } else {
    if (enable_use_result) {
      result_ = mysql_use_result(stmt);
      LOG_DEBUG("enabled mysql_use_result", K(enable_use_result), K(result_), K(mysql_get_client_version()), K(mysql_get_client_info()));
    } else {
      result_ = mysql_store_result(stmt);
    }
    if (OB_ISNULL(result_)) {
      ret = -mysql_errno(stmt_.get_conn_handler());
      LOG_WARN("fail to store mysql result", "err_msg", mysql_error(stmt_.get_conn_handler()), K(ret));
      if (OB_SUCC(ret)) {
        // if we execute a ResultSet sql, but observer just return an ok packet(casued by bugs),
        // we will arrive here.
        ret = OB_ERR_SQL_CLIENT;
        LOG_WARN("fail to store mysql result, but can not get mysql errno,"
                      " maybe recieved an ok pkt, covert it to ob err", K(ret));
      }
    } else {
      result_column_count_ = static_cast<int>(mysql_num_fields(result_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_map_.create(COLUMN_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SQL_COLUMN_MAP,
                                   ObModIds::OB_HASH_NODE_SQL_COLUMN_MAP))) {
      LOG_WARN("init username hash map failed", K(ret));
    } else {
      field_count = mysql_num_fields(result_);
      fields_ = mysql_fetch_fields(result_);
      if (OB_UNLIKELY(field_count <= 0) || OB_ISNULL(fields_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mysql fields", K(ret));
      } else {
        ObString column_name_str;
        for (int64_t i = 0; i < field_count && OB_SUCC(ret); ++i) {
          if (OB_ISNULL(fields_[i].name)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected field", "field index", i, K(ret));
          } else {
            column_name_str = ObString::make_string(fields_[i].name);
            if (OB_FAIL(column_map_.set_refactored(column_name_str, i))) {
              if (OB_HASH_EXIST == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to set name to hashtable", "column index", i, K(ret));
              }
            }
          }
        } // end for
      }
    }
  }
  return ret;
}

// must using store mode
int64_t ObMySQLResultImpl::get_row_count() const
{
  return mysql_num_rows(result_);
}

int64_t ObMySQLResultImpl::get_column_count() const
{
  return result_column_count_;
}

int ObMySQLResultImpl::close()
{
  int ret = OB_SUCCESS;
  column_map_.destroy();
  if (NULL != result_) {
    mysql_free_result(result_);
    result_ = NULL;
  }
  return ret;
}

int ObMySQLResultImpl::next()
{
  int ret = OB_SUCCESS;

  // FIXME: [xiaochu] after called mysql_store_result(),
  // calling mysql_fetch_row() should never return CR_SERVER_LOST.
  // Am I right?
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("result must not be null", K(ret));
  } else if (OB_ISNULL(cur_row_ = mysql_fetch_row(result_))) {
    MYSQL *stmt_handler = stmt_.get_stmt_handler();
    if (OB_ISNULL(stmt_handler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else {
      ret = -mysql_errno(stmt_handler);
      char errmsg[256] = {0};
      const char *srcmsg = mysql_error(stmt_handler);
      MEMCPY(errmsg, srcmsg, MIN(255, STRLEN(srcmsg)));
      ObMySQLConnection *conn = stmt_.get_connection();
      if (0 == ret) {
        ret = OB_ITER_END;
      } else if (OB_ISNULL(conn)) {
        int tmp_ret = ret;
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(errmsg), K(tmp_ret), K(ret));
      } else if (OB_INVALID_ID != conn->get_dblink_id()) {
        LOG_WARN("dblink connection error", K(ret),
                                            KP(conn),
                                            K(conn->get_dblink_id()),
                                            K(conn->get_sessid()),
                                            K(conn->usable()),
                                            K(conn->ping()));
        TRANSLATE_CLIENT_ERR(ret, errmsg);
        if (ObMySQLStatement::is_need_disconnect_error(ret)) {
          conn->set_usable(false);
        }
      }
    }
  } else if (OB_ISNULL(cur_row_result_lengths_ = mysql_fetch_lengths(result_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calling is out of sync", K(ret));
  }
  return ret;
}

int ObMySQLResultImpl::get_int(const int64_t col_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty value", K(varchar_val), K(ret));
  } else {
    int64_t ret_val;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtoll(nptr, &end_ptr, 10);
    if (*nptr != '\0' && *end_ptr == '\0') {
      int_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_uint(const int64_t col_idx, uint64_t &int_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty value", K(varchar_val), K(ret));
  } else {
    uint64_t ret_val;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtoull(nptr, &end_ptr, 10);
    if ('\0' != *nptr && '\0' == *end_ptr) {
      int_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_bool(const int64_t col_idx, bool &bool_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() != 1)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid bool value", K(varchar_val), K(ret));
  } else {
    if (0 == STRNCMP("0", varchar_val.ptr(), 1)) {
      bool_val = false;
    } else if (0 == STRNCMP("1", varchar_val.ptr(), 1)) {
      bool_val = true;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid bool value", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_special_value(const common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  static int32_t min_str_len = static_cast<int32_t>(STRLEN(ObObj::MIN_OBJECT_VALUE_STR));
  static int32_t max_str_len = static_cast<int32_t>(STRLEN(ObObj::MAX_OBJECT_VALUE_STR));
  if (NULL == varchar_val.ptr() && 0 == varchar_val.length()) {
    ret = OB_ERR_NULL_VALUE;
  } else if ((varchar_val.length() == min_str_len) &&
             (0 == strncasecmp(ObObj::MIN_OBJECT_VALUE_STR, varchar_val.ptr(), min_str_len))) {
    ret = OB_ERR_MIN_VALUE;
  } else if ((varchar_val.length() == max_str_len) &&
             (0 == strncasecmp(ObObj::MAX_OBJECT_VALUE_STR, varchar_val.ptr(), max_str_len))) {
    ret = OB_ERR_MAX_VALUE;
  }
  return ret;
}

int ObMySQLResultImpl::get_varchar(const int64_t col_idx, common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_) || OB_ISNULL(cur_row_result_lengths_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check cur row or length failed", K(ret));
  } else {
    if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < result_column_count_)) {
      varchar_val.assign(cur_row_[col_idx], static_cast<int32_t>(cur_row_result_lengths_[col_idx]));
      if (OB_FAIL(get_special_value(varchar_val))) {
        if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {
          // min,max,null
        } else {
          LOG_WARN("fail to get value", K(col_idx), K(ret));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid index", K(col_idx), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_raw(const int64_t col_idx, common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_) || OB_ISNULL(cur_row_result_lengths_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check cur row or length failed", K(ret));
  } else {
    if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < result_column_count_)) {
      varchar_val.assign(cur_row_[col_idx], static_cast<int32_t>(cur_row_result_lengths_[col_idx]));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid index", K(col_idx), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_nvarchar2(const int64_t col_idx, ObString &nvarchar2_val) const
{
  return get_varchar(col_idx, nvarchar2_val);
}

int ObMySQLResultImpl::get_nchar(const int64_t col_idx, ObString &nchar) const
{
  return get_varchar(col_idx, nchar);
}

int ObMySQLResultImpl::inner_get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                                        IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty value:[%s]", K(varchar_val), K(ret));
  } else if (OB_FAIL(nmb_val.from(varchar_val.ptr(), varchar_val.length(), allocator))) {
    LOG_WARN("invalid number value", K(varchar_val), K(ret));
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int ObMySQLResultImpl::get_float(const int64_t col_idx, float &float_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty value", K(varchar_val), K(ret));
  } else {
    float ret_val = 0.0;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtof(nptr, &end_ptr);
    if ('\0' != *nptr && '\0' == *end_ptr) {
      float_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid float value", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_double(const int64_t col_idx, double &double_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty value", K(varchar_val), K(ret));
  } else {
    double ret_val = 0.0;
    const char *nptr = varchar_val.ptr();
    char *end_ptr = NULL;
    ret_val = strtod(nptr, &end_ptr);
    if ('\0' != *nptr && '\0' == *end_ptr) {
      double_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid double value", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_varchar(const char *col_name, common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_varchar(col_idx, varchar_val);
  }
  return ret;
}


int ObMySQLResultImpl::get_raw(const char *col_name, common::ObString &raw_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else if (OB_FAIL(get_raw(col_idx, raw_val))) {
    LOG_WARN("get raw failed", K(raw_val), K(ret));
  }
  return ret;
}

int ObMySQLResultImpl::get_nvarchar2(const char *col_name, ObString &nvarchar2_val) const
{
  return get_varchar(col_name, nvarchar2_val);
}

int ObMySQLResultImpl::get_nchar(const char *col_name, ObString &nchar_val) const
{
  return get_varchar(col_name, nchar_val);
}

int ObMySQLResultImpl::get_int(const char *col_name, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_int(col_idx, int_val);
  }
  return ret;
}

int ObMySQLResultImpl::get_uint(const char *col_name, uint64_t &int_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_uint(col_idx, int_val);
  }
  return ret;
}

int ObMySQLResultImpl::get_bool(const char *col_name, bool &bool_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_bool(col_idx, bool_val);
  }
  return ret;
}

int ObMySQLResultImpl::inner_get_number(const char *col_name, common::number::ObNumber &nmb_val,
                                        IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = inner_get_number(col_idx, nmb_val, allocator);
  }
  return ret;
}

int ObMySQLResultImpl::get_float(const char *col_name, float &float_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_float(col_idx, float_val);
  }
  return ret;
}

int ObMySQLResultImpl::get_double(const char *col_name, double &double_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_double(col_idx, double_val);
  }
  return ret;
}

int ObMySQLResultImpl::get_column_index(const char *col_name, int64_t &index) const
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column name is null", K(ret));
  } else {
    ObString col_name_str = ObString::make_string(col_name);
    if (OB_FAIL(column_map_.get_refactored(col_name_str, index))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
      }
      LOG_WARN("fail to get column", KCSTRING(col_name), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::print_info() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_) || OB_ISNULL(cur_row_result_lengths_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check cur row or length failed", K(ret));
  } else for (int i = 0; i < result_column_count_; ++i) {
    LOG_INFO("cell info", "cell index", i,
             "result length", cur_row_result_lengths_[i], KCSTRING(cur_row_[i]));
  }
  return ret;
}

int ObMySQLResultImpl::get_datetime(const int64_t col_idx, int64_t &datetime) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ret = varchar2datetime(varchar_val, datetime);
  }
  return ret;
}

int ObMySQLResultImpl::get_date(const int64_t col_idx, int32_t &date) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ret = ObTimeConverter::str_to_date(varchar_val, date);
  }
  return ret;
}

int ObMySQLResultImpl::ObMySQLResultImpl::get_time(const int64_t col_idx, int64_t &time) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ObScale res_scale = -1;
    ret = ObTimeConverter::str_to_time(varchar_val, time, &res_scale);
  }
  return ret;
}

int ObMySQLResultImpl::get_year(const int64_t col_idx, uint8_t &year) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ret = ObTimeConverter::str_to_year(varchar_val, year);
  }
  return ret;
}

int ObMySQLResultImpl::get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info,
    int64_t &timestamp) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    common::ObTimeConvertCtx cvrt_ctx(tz_info, true);
    ret = common::ObTimeConverter::str_to_datetime(varchar_val, cvrt_ctx, timestamp, NULL);
  }
  return ret;
}

int ObMySQLResultImpl::get_otimestamp_value(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    const common::ObObjType type, ObOTimestampData &otimestamp_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(&tz_info, false);
//  int8_t scale = -1;
//  if (OB_FAIL(ObTimeConverter::decode_otimestamp(type, varchar_val.ptr(),
//                                                 varchar_val.length(),
//                                                 cvrt_ctx, otimestamp_val, scale))) {
//    LOG_WARN("failed to decode_otimestamp", K(varchar_val), K(ret));
//  }
    ObScale scale = -1;
    cvrt_ctx.oracle_nls_format_ = (type == ObTimestampTZType) ?
                                    ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT :
                                    ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
    if (OB_FAIL(ObTimeConverter::str_to_otimestamp(varchar_val, cvrt_ctx,
                                                   type, otimestamp_val, scale))) {
      LOG_WARN("failed to str_to_otimestamp", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_timestamp_tz(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampTZType, otimestamp_val);
}

int ObMySQLResultImpl::get_timestamp_ltz(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampLTZType, otimestamp_val);
}

int ObMySQLResultImpl::get_timestamp_nano(const int64_t col_idx, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_idx, tz_info, ObTimestampNanoType, otimestamp_val);
}

int ObMySQLResultImpl::get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type, bool is_unsigned_type) const
{
  int ret = OB_SUCCESS;
  switch (mysql_type) {
    case obmysql::EMySQLFieldType::MYSQL_TYPE_NULL:
      ob_type = ObNullType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TINY:
      if (is_unsigned_type) {
        ob_type = ObUTinyIntType;
      } else {
        ob_type = ObTinyIntType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT:
      if (is_unsigned_type) {
        ob_type = ObUSmallIntType;
      } else {
        ob_type = ObSmallIntType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONG:
      if (is_unsigned_type) {
        ob_type = ObUInt32Type;
      } else {
        ob_type = ObInt32Type;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG:
    case obmysql::EMySQLFieldType::MYSQL_TYPE_INT24:
      if (is_unsigned_type) {
        ob_type = ObUInt64Type;
      } else {
        ob_type = ObIntType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT:
      if (is_unsigned_type) {
        ob_type = ObUFloatType;
      } else {
        ob_type = ObFloatType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE:
      if (is_unsigned_type) {
        ob_type = ObUDoubleType;
      } else {
        ob_type = ObDoubleType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP:
      ob_type = ObTimestampType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME:
      ob_type = ObDateTimeType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TIME:
      ob_type = ObTimeType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DATE:
      ob_type = ObDateType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR:
      ob_type = ObYearType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR:
    case obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING:
      ob_type = ObVarcharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_STRING:
      ob_type = ObCharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB:
      ob_type = ObTinyTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB:
      ob_type = ObTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB:
      ob_type = ObMediumTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB:
      ob_type = ObLongTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      ob_type = ObTimestampTZType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      ob_type = ObTimestampLTZType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      ob_type = ObTimestampNanoType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW:
      ob_type = ObRawType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL:
      if (is_unsigned_type) {
        // for decimal type , is_unsigned_type == 1 means signed
        // is_unsigned_type comes from obmysql::EMySQLFieldType::fields_ UNSIGNED_FLAG
        ob_type = ObUNumberType;
      } else {
        ob_type = ObNumberType;
      }
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT:
      ob_type = ObNumberFloatType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_JSON:
      ob_type = ObJsonType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY:
      ob_type = ObGeometryType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_BIT:
      ob_type = ObBitType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_ENUM:
      ob_type = ObEnumType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_SET:
      ob_type = ObSetType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM:
      ob_type = ObIntervalYMType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS:
      ob_type = ObIntervalDSType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2:
      ob_type = ObNVarchar2Type;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR:
      ob_type = ObNCharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX:
      ob_type = ObExtendType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID:
      ob_type = ObURowIDType;
      break;
    default:
      _OB_LOG(WARN, "unsupport MySQL type %d", mysql_type);
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObMySQLResultImpl::get_type(const int64_t col_idx, ObObjMeta &type) const
{
  int ret = OB_SUCCESS;
  ObObjType ob_type;
  if (OB_ISNULL(cur_row_) || OB_ISNULL(cur_row_result_lengths_) || OB_ISNULL(fields_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check cur row or length failed", K(ret));
  } else if (col_idx < 0 || col_idx >= result_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column idx", K(col_idx), K_(result_column_count));
  } else if (OB_FAIL(get_ob_type(ob_type, static_cast<obmysql::EMySQLFieldType>(fields_[col_idx].type),
                                 fields_[col_idx].flags & UNSIGNED_FLAG))) {
    LOG_WARN("failed to get ob type", K(ret), "mysql_type", fields_[col_idx].type);
  } else {
    type.set_type(ob_type);
    type.set_collation_type(static_cast<ObCollationType>(fields_[col_idx].charsetnr));
  }
  return ret;
}
int ObMySQLResultImpl::get_col_meta(const int64_t col_idx, bool old_max_length,
                                    oceanbase::common::ObString &name,
                                    ObDataType &data_type) const
{
  int ret = OB_SUCCESS;
  ObObjType ob_type;
  if (OB_ISNULL(fields_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check fields_ failed", K(ret));
  } else if (col_idx < 0 || col_idx >= result_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column idx", K(col_idx), K_(result_column_count));
  } else if (OB_FAIL(get_ob_type(ob_type, static_cast<obmysql::EMySQLFieldType>(fields_[col_idx].type),
                     fields_[col_idx].flags & UNSIGNED_FLAG))) {
    LOG_WARN("failed to get ob type", K(ret), "mysql_type", fields_[col_idx].type);
  } else {
#ifdef OB_BUILD_DBLINK
    int16_t precision = fields_[col_idx].precision;
#else
    int16_t precision = -1;
#endif
    int16_t scale = fields_[col_idx].decimals;
    int32_t length = fields_[col_idx].length;
    name.assign_ptr(fields_[col_idx].name, STRLEN(fields_[col_idx].name));
    data_type.meta_.set_type(ob_type);
    data_type.meta_.set_collation_type(static_cast<ObCollationType>(fields_[col_idx].charsetnr));
    //data_type.meta_.set_autoincrement(fields_[col_idx].flags & AUTO_INCREMENT_FLAG);
    data_type.set_zero_fill(fields_[col_idx].flags & ZEROFILL_FLAG);
    format_precision_scale_length(precision, scale, length,
                                  ob_type, data_type.meta_.get_collation_type(),
                                  DBLINK_DRV_OB, old_max_length);
    data_type.set_precision(precision);
    data_type.set_scale(scale);
    data_type.set_length(length);
    LOG_DEBUG("get col type from obclient", K(ob_type), K(data_type), K(ret));
  }
  return ret;
}

int ObMySQLResultImpl::get_obj(const int64_t col_idx, ObObj &obj,
                               const ObTimeZoneInfo *tz_info,
                               ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  ObObjMeta type;
  ObObjValue obj_value;
  ObString obj_str;
  if (OB_FAIL(get_type(col_idx, type))) {
    LOG_WARN("failed to get type");
  } else {
    switch(type.get_type()) {
      case ObNullType:
        obj.set_null();
        break;
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
        if (OB_SUCC(get_int(col_idx, obj_value.int64_)))
        {
          obj.set_int(obj_value.int64_);
        }
        break;
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
        if (OB_SUCC(get_uint(col_idx, obj_value.uint64_)))
        {
          obj.set_uint64(obj_value.uint64_);
        }
        break;
      case ObDoubleType:
      case ObUDoubleType:
        if (OB_SUCC(get_double(col_idx, obj_value.double_)))
        {
          obj.set_double(type.get_type(), obj_value.double_);
        }
        break;
      case ObVarcharType:
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_varchar(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      case ObCharType:
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_char(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      case ObNVarchar2Type:
        if (OB_SUCC(get_nvarchar2(col_idx, obj_str)))
        {
          obj.set_nvarchar2(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      case ObNCharType:
        if (OB_SUCC(get_nvarchar2(col_idx, obj_str)))
        {
          obj.set_nchar(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      case ObRawType:
        if (OB_SUCC(get_raw(col_idx, obj_str)))
        {
          if (obj_str.empty()) {
            obj.set_null();
          } else if (OB_ISNULL(allocator)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator is NULL", K(ret));
          } else if (OB_FAIL(ObHexUtilsBase::unhex(obj_str, *allocator, obj))) {
            LOG_WARN("fail to unhex", K(ret), K(obj_str));
          } else {
            obj.set_raw(obj.get_raw());
          }
        }
        break;
      case ObFloatType:
      case ObUFloatType:
        if (OB_SUCC(get_float(col_idx, obj_value.float_)))
        {
          obj.set_float(type.get_type(), obj_value.float_);
        }
        break;
      case ObDateTimeType:
        if (OB_SUCC(get_datetime(col_idx, obj_value.datetime_)))
        {
          obj.set_datetime(obj_value.datetime_);
        }
        break;
      case ObTimestampType:
        if (OB_ISNULL(tz_info)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tz info is NULL", K(ret));
        } else if (OB_SUCC(get_timestamp(col_idx, tz_info, obj_value.datetime_))) {
          obj.set_timestamp(obj_value.datetime_);
        }
        break;
      case ObNumberType:
      case ObUNumberType:
      case ObNumberFloatType:
        if (OB_ISNULL(allocator)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator is NULL", K(ret));
        } else {
          number::ObNumber num;
          if (OB_SUCC(get_number(col_idx, num, *allocator))) {
            obj.set_number(num);
          }
        }
        break;
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
        if (OB_ISNULL(tz_info)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tz info is NULL", K(ret));
        } else {
          ObOTimestampData tz_val;
          if (OB_SUCC(get_otimestamp_value(col_idx, *tz_info, type.get_type(), tz_val))) {
            obj.set_otimestamp_value(type.get_type(), tz_val);
          }
        }
        break;
      case ObDateType: {
        if (OB_SUCC(get_date(col_idx, obj_value.date_)))
        {
          obj.set_date(obj_value.date_);
        }
        break;
      }
      case ObTimeType: {
        if (OB_SUCC(get_time(col_idx, obj_value.time_)))
        {
          obj.set_time(obj_value.time_);
        }
        break;
      }
      case ObYearType: {
        if (OB_SUCC(get_year(col_idx, obj_value.year_)))
        {
          obj.set_year(obj_value.year_);
        }
        break;
      }
      case ObIntervalYMType: {
        ObIntervalYMValue ym_val;
        if (OB_SUCC(get_interval_ym(col_idx, ym_val))) {
          obj.set_interval_ym(ym_val);
        }
        break;
      }
      case ObIntervalDSType: {
        ObIntervalDSValue ds_val;
        if (OB_SUCC(get_interval_ds(col_idx, ds_val))) {
          obj.set_interval_ds(ds_val);
        }
        break;
      }
      case ObURowIDType: {
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          ObURowIDData rowid_data;
          if (OB_ISNULL(allocator)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator is NULL", K(ret));
          } else if (OB_FAIL(ObURowIDData::decode2urowid(obj_str.ptr(), obj_str.length(),
                                                         *allocator, rowid_data))) {
            LOG_WARN("fail to decode2urowid", K(ret), K(obj_str));
          } else {
            obj.set_urowid(rowid_data);
          }
        }
        break;
      }
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        if (lib::is_oracle_mode()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("oracle mode dblink not support lob type", K(ret), K(type.get_type()));
        } else if (OB_SUCC(get_varchar(col_idx, obj_str))) {
          obj.set_lob_value(type.get_type(), obj_str.ptr(), obj_str.length());
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      }
      case ObEnumType: {
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_enum_inner(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      }
      case ObSetType: {
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_set_inner(obj_str);
          obj.set_collation_type(type.get_collation_type());
        }
        break;
      }
      case ObGeometryType: {
        /*
        if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_geometry_value(type.get_type(), obj_str.ptr(), obj_str.length());

        }*/
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObJsonType: {
        /*if (OB_SUCC(get_varchar(col_idx, obj_str)))
        {
          obj.set_json_value(type.get_type(), obj_str.ptr(), obj_str.length());
          obj.set_collation_type(type.get_collation_type());
        }*/
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObBitType: {
        /*
        if (OB_SUCC(get_uint(col_idx, obj_value.uint64_)))//ailing to do
        {
          obj.set_bit(obj_value.uint64_);
        }*/
        ret = OB_NOT_SUPPORTED;
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported object type", "obj_type", obj.get_type(), K(ret));
        break;
    }
    if (OB_ERR_NULL_VALUE == ret) {
      obj.set_null();
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_datetime(const char *col_name, int64_t &datetime) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ret = varchar2datetime(varchar_val, datetime);
  }
  return ret;
}

int ObMySQLResultImpl::get_date(const char *col_name, int32_t &date) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ret = ObTimeConverter::str_to_date(varchar_val, date);
  }
  return ret;
}

int ObMySQLResultImpl::get_time(const char *col_name, int64_t &time) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ObScale res_scale = -1;
    ret = ObTimeConverter::str_to_time(varchar_val, time, &res_scale);
  }
  return ret;
}

int ObMySQLResultImpl::get_year(const char *col_name, uint8_t &year) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ret = ObTimeConverter::str_to_year(varchar_val, year);
  }
  return ret;
}

int ObMySQLResultImpl::get_timestamp(const char *col_name, const common::ObTimeZoneInfo *tz_info,
    int64_t &timestamp) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(tz_info, true);
    ret = ObTimeConverter::str_to_datetime(varchar_val, cvrt_ctx, timestamp, NULL);
  }
  return ret;
}

int ObMySQLResultImpl::get_otimestamp_value(const char *col_name, const common::ObTimeZoneInfo &tz_info,
    common::ObObjType type, ObOTimestampData &otimestamp_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(&tz_info, false);
    int8_t scale = -1;
    if (OB_FAIL(ObTimeConverter::decode_otimestamp(type, varchar_val.ptr(),
                                                   varchar_val.length(),
                                                   cvrt_ctx, otimestamp_val, scale))) {
      LOG_WARN("failed to decode_otimestamp", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_timestamp_tz(const char *col_name, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampTZType, otimestamp_val);
}

int ObMySQLResultImpl::get_timestamp_ltz(const char *col_name, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampLTZType, otimestamp_val);
}

int ObMySQLResultImpl::get_timestamp_nano(const char *col_name, const common::ObTimeZoneInfo &tz_info,
    common::ObOTimestampData &otimestamp_val) const
{
  return get_otimestamp_value(col_name, tz_info, ObTimestampNanoType, otimestamp_val);
}

int ObMySQLResultImpl::get_type(const char* col_name, ObObjMeta &type) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_type(col_idx, type);
  }
  return ret;
}

int ObMySQLResultImpl::get_obj(const char* col_name, ObObj &obj) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = OB_INVALID_INDEX;
  if (OB_FAIL(get_column_index(col_name, col_idx))) {
    LOG_WARN("fail to get column index", K(ret));
  } else {
    ret = get_obj(col_idx, obj);
  }
  return ret;
}

int ObMySQLResultImpl::get_interval_ym(const int64_t col_idx, ObIntervalYMValue &val) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObScale scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if (OB_FAIL(ObTimeConverter::str_to_interval_ym(varchar_val, val, scale))) {
      LOG_WARN("failed to decode interval year to month", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_interval_ds(const int64_t col_idx, ObIntervalDSValue &val) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("fail to get value", K(col_idx), K(ret));
  } else {
    ObScale scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    if (OB_FAIL(ObTimeConverter::str_to_interval_ds(varchar_val, val, scale))) {
      LOG_WARN("failed to decode interval day to second", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_interval_ym(const char *col_name, ObIntervalYMValue &val) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObScale scale = -1;
    if (OB_FAIL(ObTimeConverter::decode_interval_ym(varchar_val.ptr(), varchar_val.length(), val, scale))) {
      LOG_WARN("failed to decode interval year to month", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::get_interval_ds(const char *col_name, ObIntervalDSValue &val) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("fail to get value", KCSTRING(col_name), K(ret));
  } else {
    ObScale scale = -1;
    if (OB_FAIL(ObTimeConverter::decode_interval_ds(varchar_val.ptr(), varchar_val.length(), val, scale))) {
      LOG_WARN("failed to decode interval day to second", K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObMySQLResultImpl::inner_get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                                        common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("failed to get value", K(ret), K(col_idx));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected length of value", K(ret));
  } else if (OB_FAIL(ObURowIDData::decode2urowid(varchar_val.ptr(), varchar_val.length(),
                                                 allocator, urowid_data))) {
    LOG_WARN("failed to decode to urowid", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObMySQLResultImpl::inner_get_urowid(const char *col_name, ObURowIDData &urowid_data,
                                        common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("failed to get value", K(ret), KCSTRING(col_name));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected length of value", K(ret));
  } else if (OB_FAIL(ObURowIDData::decode2urowid(varchar_val.ptr(), varchar_val.length(),
                                                 allocator, urowid_data))) {
    LOG_WARN("failed to decode to urowid", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObMySQLResultImpl::inner_get_lob_locator(const char *col_name,
                                              common::ObLobLocator *&lob_locator,
                                              common::ObIAllocator &allocator) const
{ UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_name, varchar_val))) {
    LOG_WARN("failed to get value", K(ret), KCSTRING(col_name));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected length of value", K(ret));
  } else {
    lob_locator = static_cast<common::ObLobLocator *>((void *)varchar_val.ptr());
  }
  return ret;
}

int ObMySQLResultImpl::inner_get_lob_locator(const int64_t col_idx,
                                              common::ObLobLocator *&lob_locator,
                                              common::ObIAllocator &allocator) const
{ UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WARN("failed to get value", K(ret));
  } else if (OB_UNLIKELY(varchar_val.length() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected length of value", K(ret));
  } else {
    lob_locator = static_cast<common::ObLobLocator *>((void *)varchar_val.ptr());
  }
  return ret;
}

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
