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

#ifndef OCEANBASE_OBSERVER_OB_INNER_SQL_RESULT_H_
#define OCEANBASE_OBSERVER_OB_INNER_SQL_RESULT_H_

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_context.h"
#include "share/rc/ob_context.h"

namespace oceanbase
{
namespace lib
{
class MemoryContext;
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{

class ObInnerSQLResult : public common::sqlclient::ObMySQLResult
{
  friend class ObInnerSQLConnection;
public:
  explicit ObInnerSQLResult(sql::ObSQLSessionInfo &session);
  virtual ~ObInnerSQLResult();
  int init();
  int init(bool has_tenant_resource);
  virtual int open();
  virtual int close();
  virtual int next();
  int force_close();

  virtual int print_info() const;

  virtual int get_int(const int64_t col_idx, int64_t &int_val) const;
  virtual int get_uint(const int64_t col_idx, uint64_t &int_val) const;
  virtual int get_datetime(const int64_t col_idx, int64_t &datetime) const;
  virtual int get_date(const int64_t col_index, int32_t &date) const;
  virtual int get_time(const int64_t col_index, int64_t &time) const;
  virtual int get_year(const int64_t col_index, uint8_t &year) const;
  virtual int get_bool(const int64_t col_idx, bool &bool_val) const;
  virtual int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const;
  virtual int get_raw(const int64_t col_idx, common::ObString &varchar_val) const;
  virtual int get_float(const int64_t col_idx, float &float_val) const;
  virtual int get_double(const int64_t col_idx, double &double_val) const;
  virtual int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info, int64_t &timestamp) const;
  virtual int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val) const override;
  virtual int get_otimestamp_value(const int64_t col_idx,
                                   const common::ObTimeZoneInfo &tz_info,
                                   const common::ObObjType type,
                                   common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_tz(const int64_t col_idx,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_ltz(const int64_t col_idx,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_nano(const int64_t col_idx,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const;
  virtual int get_type(const int64_t col_idx, ObObjMeta &type) const override;
  virtual int get_col_meta(const int64_t col_idx, bool old_max_length,
                           oceanbase::common::ObString &name, ObDataType &data_type) const override;
  virtual int64_t get_column_count() const override;
  virtual int get_obj(const int64_t col_idx, ObObj &obj,
                      const common::ObTimeZoneInfo *tz_info = NULL,
                      common::ObIAllocator *allocator = NULL) const override;
  virtual int get_interval_ym(const int64_t col_idx, ObIntervalYMValue &int_val) const;
  virtual int get_interval_ds(const int64_t col_idx, ObIntervalDSValue &int_val) const;
  virtual int get_nvarchar2(const int64_t col_idx, common::ObString &nvarchar2_val) const;
  virtual int get_nchar(const int64_t col_idx, common::ObString &nchar_val) const;

  virtual int get_int(const char *col_name, int64_t &int_val) const;
  virtual int get_uint(const char *col_name, uint64_t &int_val) const;
  virtual int get_datetime(const char *col_name, int64_t &datetime) const;
  virtual int get_date(const char *col_name, int32_t &date) const;
  virtual int get_time(const char *col_name, int64_t &time) const;
  virtual int get_year(const char *col_name, uint8_t &year) const;
  virtual int get_bool(const char *col_name, bool &bool_val) const;
  virtual int get_varchar(const char *col_name, common::ObString &varchar_val) const;
  virtual int get_raw(const char *col_name, common::ObString &raw_val) const;
  virtual int get_float(const char *col_name, float &float_val) const;
  virtual int get_double(const char *col_name, double &double_val) const;
  virtual int get_timestamp(const char *col_name, const common::ObTimeZoneInfo *tz_info, int64_t &timestamp) const;
  virtual int get_number(const char *col_name, common::number::ObNumber &nmb_val) const override;
  virtual int get_otimestamp_value(const char *col_name,
                                   const common::ObTimeZoneInfo &tz_info,
                                   const common::ObObjType type,
                                   common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_tz(const char *col_name,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_ltz(const char *col_name,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const;
  virtual int get_timestamp_nano(const char *col_name,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const;
  virtual int get_type(const char* col_name, ObObjMeta &type) const override;
  virtual int get_obj(const char* col_name, ObObj &obj) const override;

  virtual int get_interval_ym(const char *col_name, common::ObIntervalYMValue &int_val) const;
  virtual int get_interval_ds(const char *col_name, common::ObIntervalDSValue &int_val) const;
  virtual int get_nvarchar2(const char *col_name, common::ObString &nvarchar2_val) const;
  virtual int get_nchar(const char *col_name, common::ObString &nchar_val) const;

  sql::ObSqlCtx &sql_ctx() { return sql_ctx_; }

  sql::ObResultSet &result_set() { OB_ASSERT(result_set_ != nullptr); return *result_set_; }
  sql::ObRemoteResultSet &remote_result_set()
  { OB_ASSERT(remote_result_set_ != nullptr); return *remote_result_set_; }

  bool has_tenant_resource() const { return has_tenant_resource_; }
  void set_has_tenant_resource(bool has_tenant_resource)
  { has_tenant_resource_ = has_tenant_resource; }
  sql::ObResultSet *get_result_set() { return result_set_; }

  void set_execute_start_ts(int64_t ts) { execute_start_ts_ = ts; }
  int64_t get_execute_start_ts() { return execute_start_ts_; }
  void set_execute_end_ts(int64_t ts) { execute_end_ts_ = ts; }
  int64_t get_execute_end_ts() { return execute_end_ts_; }

  int get_obj(const int64_t col_idx, const common::ObObj *&obj) const;
  const ObNewRow *get_row() const { return row_; };

  void set_compat_mode(lib::Worker::CompatMode mode);
  bool is_inited() const { return is_inited_; }
  void set_is_read(const bool is_read) { is_read_ = is_read; }
private:
  virtual int inner_get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
      IAllocator &allocator) const;
  virtual int get_number_impl(const int64_t col_idx, common::number::ObNumber &nmb_val) const;

  virtual int inner_get_number(const char *col_name, common::number::ObNumber &nmb_val,
      IAllocator &allocator) const;
  virtual int get_number_impl(const char *col_name, common::number::ObNumber &nmb_val) const;

  virtual int inner_get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const;
  virtual int get_urowid_impl(const int64_t col_idx, common::ObURowIDData &urowid_data) const;

  virtual int inner_get_urowid(const char *col_name, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const;
  virtual int get_urowid_impl(const char *col_name, common::ObURowIDData &urowid_data) const;

  virtual int get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator) const;
  virtual int get_lob_locator(const char *col_name, ObLobLocator *&lob_locator) const;

  virtual int inner_get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
      common::ObIAllocator &allocator) const;
  virtual int inner_get_lob_locator(const char *col_name, ObLobLocator *&lob_locator,
      common::ObIAllocator &allocator) const;

  virtual int get_lob_locator_impl(const int64_t col_idx, ObLobLocator *&lob_locator) const;
  virtual int get_lob_locator_impl(const char *col_name, ObLobLocator *&lob_locator) const;

  // return OB_ENTRY_NOT_EXIST for column name not found.
  int find_idx(const char *col_name, int64_t &idx) const;
  int build_column_map() const;
  int inner_close();

  static inline int check_extend_value(const common::ObObj &obj);
private:
  class MemEntifyDestroyGuard
  {
  public:
    MemEntifyDestroyGuard(lib::MemoryContext &entity) : ref_(entity) {}
    ~MemEntifyDestroyGuard()
    {
      if (NULL != ref_) {
        DESTROY_CONTEXT(ref_);
        ref_ = NULL;
      }
    }
  private:
    lib::MemoryContext &ref_;
  };

  typedef common::hash::ObHashMap<
      common::ObString, int64_t, common::hash::NoPthreadDefendMode> ColumnMap;
  mutable bool column_map_created_;
  mutable bool column_indexed_;
  mutable ColumnMap column_map_;

  lib::MemoryContext mem_context_;
  // Memory of memory entity may referenced by sql_ctx_, use the guard to make
  // sure memory entity destroyed after sql_ctx_ destructed.
  MemEntifyDestroyGuard mem_context_destroy_guard_;
  sql::ObSqlCtx sql_ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  bool opened_;
  char buf_[sizeof(sql::ObResultSet)] __attribute__ ((aligned (16)));
  sql::ObSQLSessionInfo &session_;
  sql::ObResultSet *result_set_;
  sql::ObRemoteResultSet *remote_result_set_; // for inner sql with rpc transmit
  const common::ObNewRow *row_;
  int64_t execute_start_ts_;
  int64_t execute_end_ts_;
  lib::Worker::CompatMode compat_mode_;
  bool is_inited_;
  bool store_first_row_; // whether got 1 row
  bool iter_end_;
  bool is_read_; //for some write sql , do not need prefetch 1 row in open
  bool has_tenant_resource_;
  omt::ObTenant *tenant_;
  ObLDHandle handle_;

  DISALLOW_COPY_AND_ASSIGN(ObInnerSQLResult);
};

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_INNER_SQL_RESULT_H_
