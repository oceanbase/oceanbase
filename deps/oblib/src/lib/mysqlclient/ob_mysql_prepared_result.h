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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_RESULT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_PREPARED_RESULT__

#include <mysql.h>
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
namespace sqlclient
{
class ObMySQLPreparedStatement;
class ObMySQLPreparedResultImpl : public ObMySQLResult
{
public:
  explicit ObMySQLPreparedResultImpl(ObMySQLPreparedStatement &stmt);
  virtual ~ObMySQLPreparedResultImpl();
  /*
    implement virtual function from ObMySQLResult 
  */
  virtual int64_t get_column_count() const override { return 0; }
  virtual int close() override;
  virtual int next() override;
  virtual int get_int(const int64_t col_idx, int64_t &int_val) const override;
  virtual int get_uint(const int64_t col_idx, uint64_t &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_datetime(const int64_t col_idx, int64_t &datetime) const override { return OB_NOT_SUPPORTED; }
  virtual int get_date(const int64_t col_idx, int32_t &date) const override { return OB_NOT_SUPPORTED; }
  virtual int get_time(const int64_t col_idx, int64_t &time) const override { return OB_NOT_SUPPORTED; }
  virtual int get_year(const int64_t col_idx, uint8_t &year) const override { return OB_NOT_SUPPORTED; }
  virtual int get_bool(const int64_t col_idx, bool &bool_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const override;
  virtual int get_raw(const int64_t col_idx, common::ObString &varchar_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_float(const int64_t col_idx, float &float_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_double(const int64_t col_idx, double &double_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_otimestamp_value(const int64_t col_idx,
                                   const common::ObTimeZoneInfo &tz_info,
                                   const common::ObObjType type,
                                   common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_tz(const int64_t col_idx,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_ltz(const int64_t col_idx,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_nano(const int64_t col_idx,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
                                 virtual int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val) const { return OB_NOT_SUPPORTED; }
  virtual int get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data) const { return OB_NOT_SUPPORTED; }
  virtual int get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator) const { return OB_NOT_SUPPORTED; }
  virtual int get_type(const int64_t col_idx, ObObjMeta &type) const override { return OB_NOT_SUPPORTED; }
  virtual int get_col_meta(const int64_t col_idx, bool old_max_length,
                           oceanbase::common::ObString &name, ObObjMeta &meta,
                           int16_t &precision, int16_t &scale, int32_t &length) const override { return OB_NOT_SUPPORTED; }
  virtual int get_obj(const int64_t col_idx, ObObj &obj,
                      const common::ObTimeZoneInfo *tz_info = NULL,
                      common::ObIAllocator *allocator = NULL) const override { return OB_NOT_SUPPORTED; }
  virtual int get_interval_ym(const int64_t col_idx, common::ObIntervalYMValue &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_interval_ds(const int64_t col_idx, common::ObIntervalDSValue &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_nvarchar2(const int64_t col_idx, common::ObString &nvarchar2_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_nchar(const int64_t col_idx, common::ObString &nchar_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_int(const char *col_name, int64_t &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_uint(const char *col_name, uint64_t &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_datetime(const char *col_name, int64_t &datetime) const override { return OB_NOT_SUPPORTED; }
  virtual int get_date(const char *col_name, int32_t &date) const override { return OB_NOT_SUPPORTED; }
  virtual int get_time(const char *col_name, int64_t &time) const override { return OB_NOT_SUPPORTED; }
  virtual int get_year(const char *col_name, uint8_t &year) const override { return OB_NOT_SUPPORTED; }
  virtual int get_bool(const char *col_name, bool &bool_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_varchar(const char *col_name, common::ObString &varchar_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_raw(const char *col_name, common::ObString &raw_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_float(const char *col_name, float &float_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_double(const char *col_name, double &double_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp(const char *col_name, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_otimestamp_value(const char *col_name,
                                  const common::ObTimeZoneInfo &tz_info,
                                  const common::ObObjType type,
                                  common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_tz(const char *col_name,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_ltz(const char *col_name,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_timestamp_nano(const char *col_name,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_type(const char* col_name, ObObjMeta &type) const override { return OB_NOT_SUPPORTED; }
  virtual int get_obj(const char* col_name, ObObj &obj) const override { return OB_NOT_SUPPORTED; }
  virtual int get_interval_ym(const char *col_name, common::ObIntervalYMValue &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_interval_ds(const char *col_name, common::ObIntervalDSValue &int_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_nvarchar2(const char *col_name, common::ObString &nvarchar2_val) const override { return OB_NOT_SUPPORTED; }
  virtual int get_nchar(const char *col_name, common::ObString &nchar_val) const override { return OB_NOT_SUPPORTED; }

  int init();
  int bind_result_param();
  int bind_result(const int64_t col_idx, enum_field_types buf_type, char *out_buf, const int64_t buf_len,
                  unsigned long &res_len);
private:
  virtual int inner_get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                               IAllocator &allocator) const override { return OB_NOT_SUPPORTED; }
  virtual int inner_get_number(const char *col_name, common::number::ObNumber &nmb_val,
                               IAllocator &allocator) const override { return OB_NOT_SUPPORTED; }

  virtual int inner_get_urowid(const char *col_name, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const override { return OB_NOT_SUPPORTED; }
  virtual int inner_get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const override { return OB_NOT_SUPPORTED; }
  virtual int inner_get_lob_locator(const char *col_name, ObLobLocator *&lob_locator,
                               common::ObIAllocator &allocator) const override { return OB_NOT_SUPPORTED; }
  virtual int inner_get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
                               common::ObIAllocator &allocator) const override { return OB_NOT_SUPPORTED; }
private:
  ObMySQLPreparedStatement &stmt_;
  common::ObIAllocator &alloc_;
  int64_t result_column_count_;
  MYSQL_BIND *bind_;
};
}
}
}

#endif

