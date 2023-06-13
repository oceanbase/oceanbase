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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_RESULT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_RESULT__
#include <mysql.h>
#include "lib/mysqlclient/ob_mysql_result.h"
#include "rpc/obmysql/ob_mysql_global.h"


namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLStatement;
class ObMySQLResultImpl : public ObMySQLResult
{
  friend class ObMySQLResultWriter;
  friend class ObMySQLResultHeader;
public:
  explicit ObMySQLResultImpl(ObMySQLStatement &stmt);
  ~ObMySQLResultImpl();
  int init(bool enable_use_result = false);
  /*
   * close result
   */
  int close();
  /*
   * row count
   */
  // must using store mode after mysql_store_result()
  int64_t get_row_count(void) const;

  /*
   * move result cursor to next row
   */
  int next();
  /*
   * read int/str/TODO from result set
   * col_idx: indicate which column to read, [0, max_read_col)
   */
  int get_int(const int64_t col_idx, int64_t &int_val) const;
  int get_uint(const int64_t col_idx, uint64_t &int_val) const;
  int get_datetime(const int64_t col_idx, int64_t &datetime) const;
  int get_date(const int64_t col_idx, int32_t &date) const;
  int get_time(const int64_t col_idx, int64_t &time) const;
  int get_year(const int64_t col_idx, uint8_t &year) const;
  int get_bool(const int64_t col_idx, bool &bool_val) const;
  int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const;
  int get_raw(const int64_t col_idx, common::ObString &varchar_val) const;
  int get_float(const int64_t col_idx, float &float_val) const;
  int get_double(const int64_t col_idx, double &double_val) const;
  int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const;
  int get_otimestamp_value(const int64_t col_idx,
                           const common::ObTimeZoneInfo &tz_info,
                           const common::ObObjType type,
                           common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_tz(const int64_t col_idx,
                       const common::ObTimeZoneInfo &tz_info,
                       common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_ltz(const int64_t col_idx,
                        const common::ObTimeZoneInfo &tz_info,
                        common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_nano(const int64_t col_idx,
                         const common::ObTimeZoneInfo &tz_info,
                         common::ObOTimestampData &otimestamp_val) const;
  int get_type(const int64_t col_idx, ObObjMeta &type) const override;
  int get_col_meta(const int64_t col_idx, bool old_max_length,
                   oceanbase::common::ObString &name, ObDataType &data_type) const override;
  int get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type, bool is_unsigned_type) const;
  int get_obj(const int64_t col_idx, ObObj &obj,
              const common::ObTimeZoneInfo *tz_info = NULL,
              common::ObIAllocator *allocator = NULL) const override;
  int get_interval_ym(const int64_t col_idx, common::ObIntervalYMValue &val) const;
  int get_interval_ds(const int64_t col_idx, common::ObIntervalDSValue &val) const;
  int get_nvarchar2(const int64_t col_idx, common::ObString &nvarchar2_val) const;
  int get_nchar(const int64_t col_idx, common::ObString &nchar_val) const;

  /*
   * read int/str/TODO from result set
   * col_name: indicate which column to read
   * @return  OB_INVALID_PARAM if col_name does not exsit
   */
  int get_int(const char *col_name, int64_t &int_val) const;
  int get_uint(const char *col_name, uint64_t &int_val) const;
  int get_datetime(const char *col_name, int64_t &datetime) const;
  int get_date(const char *col_name, int32_t &date) const;
  int get_time(const char *col_name, int64_t &time) const;
  int get_year(const char *col_name, uint8_t &year) const;
  int get_bool(const char *col_name, bool &bool_val) const;
  int get_varchar(const char *col_name, common::ObString &varchar_val) const;
  int get_raw(const char *col_name, common::ObString &raw_val) const;
  int get_float(const char *col_name, float &float_val) const;
  int get_double(const char *col_name, double &double_val) const;
  int get_timestamp(const char *col_name, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const;
  int get_otimestamp_value(const char *col_name,
                           const common::ObTimeZoneInfo &tz_info,
                           const common::ObObjType type,
                           common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_tz(const char *col_name,
                       const common::ObTimeZoneInfo &tz_info,
                       common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_ltz(const char *col_name,
                        const common::ObTimeZoneInfo &tz_info,
                        common::ObOTimestampData &otimestamp_val) const;
  int get_timestamp_nano(const char *col_name,
                         const common::ObTimeZoneInfo &tz_info,
                         common::ObOTimestampData &otimestamp_val) const;
  int get_type(const char* col_name, ObObjMeta &type) const override;
  int get_obj(const char* col_name, ObObj &obj) const override;
  int get_interval_ym(const char *col_name, common::ObIntervalYMValue &val) const;
  int get_interval_ds(const char *col_name, common::ObIntervalDSValue &val) const;
  int get_nvarchar2(const char *col_name, common::ObString &nvarchar2_val) const;
  int get_nchar(const char *col_name, common::ObString &nchar_val) const;

  //debug function
  int print_info() const;
  int64_t get_column_count() const override;

private:
  int get_column_index(const char *col_name, int64_t &index) const;
  int get_special_value(const common::ObString &varchar_val) const;
  int inner_get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                       IAllocator &allocator) const;
  int inner_get_number(const char *col_name, common::number::ObNumber &nmb_val,
                       IAllocator &allocator) const;

  int inner_get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                       common::ObIAllocator &allocator) const;
  int inner_get_urowid(const char *col_name, common::ObURowIDData &urowid_data,
                       common::ObIAllocator &allocator) const;

  int inner_get_lob_locator(const int64_t col_idx, common::ObLobLocator *&lob_locator,
                       common::ObIAllocator &allocator) const;
  int inner_get_lob_locator(const char *col_name, common::ObLobLocator *&lob_locator,
                       common::ObIAllocator &allocator) const;
private:
  ObMySQLStatement &stmt_;
  MYSQL_RES *result_;
  MYSQL_ROW cur_row_;
  int result_column_count_;
  unsigned long *cur_row_result_lengths_;
  hash::ObHashMap<ObString, int64_t, hash::NoPthreadDefendMode> column_map_;
  MYSQL_FIELD *fields_;
};

}
}
}
#endif
