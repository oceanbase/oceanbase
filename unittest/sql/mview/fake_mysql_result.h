/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Test-only fake of common::sqlclient::ObMySQLResult that returns canned rows
// for the columns the mview pending-task SQL produces. Drives tests that
// exercise ObMViewPendingTaskTableOperator::load_tasks_batch without a live
// SQL proxy.

#pragma once

#include <string.h>
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"

namespace oceanbase
{
namespace unittest
{

// Row that mirrors the 16 columns of the load_tasks_batch SELECT.
// Field order matches the array initializer convention used by the tests.
// svr_ip/svr_port were added in the session_id commit; all initializers must
// supply them explicitly (nullptr/0 for tasks that were never dispatched).
struct FakeRow
{
  // Read via get_int(name, int64_t&)
  int64_t  refresh_id;
  int64_t  mview_id;
  int64_t  seq;
  int64_t  status;
  int64_t  flags;
  int64_t  skip_cnt;
  int64_t  retry_count;
  int64_t  next_retry_ts;
  int64_t  refresh_method;
  int64_t  refresh_parallel;
  int64_t  gmt_create_ts;
  int64_t  gmt_modified_ts;
  // dep_mview_id nullable; when dep_is_null is true get_int returns OB_ERR_NULL_VALUE
  int64_t  dep_mview_id;
  bool     dep_is_null;
  // Read via get_uint(name, uint64_t&)
  uint64_t target_data_sync_scn;
  // next_retry_ts is read with EXTRACT_INT_FIELD_MYSQL_SKIP_RET; null is silently 0
  bool     next_retry_ts_is_null;
  // svr_ip/svr_port added in session_id commit (new columns on pending task table).
  // nullptr → EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET treats as NULL → svr_addr_.reset().
  const char *svr_ip;
  int64_t     svr_port;

  // Required so ObSEArray<FakeRow>::to_string() can be instantiated.
  int64_t to_string(char *, const int64_t) const { return 0; }
};

class FakeMySQLResult : public common::sqlclient::ObMySQLResult
{
public:
  FakeMySQLResult() : cursor_(-1) {}
  virtual ~FakeMySQLResult() {}

  int load_rows(const FakeRow *rows, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    rows_.reset();
    cursor_ = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(rows_.push_back(rows[i]))) {
        // propagate
      }
    }
    return ret;
  }

  void clear() { rows_.reset(); cursor_ = -1; }
  void reset_cursor() { cursor_ = -1; }

  // ---------------------------------------------------------------- core
  int64_t get_column_count() const override { return 16; }
  int close() override { return common::OB_SUCCESS; }
  int next() override
  {
    return (++cursor_ < rows_.count()) ? common::OB_SUCCESS : common::OB_ITER_END;
  }

  // ---------------------------------------------------------------- get by name (used)
  int get_int(const char *col_name, int64_t &v) const override
  {
    if (cursor_ < 0 || cursor_ >= rows_.count()) {
      return common::OB_ERR_UNEXPECTED;
    }
    const FakeRow &r = rows_.at(cursor_);
    if      (0 == strcmp(col_name, "refresh_id"))       v = r.refresh_id;
    else if (0 == strcmp(col_name, "mview_id"))         v = r.mview_id;
    else if (0 == strcmp(col_name, "seq"))              v = r.seq;
    else if (0 == strcmp(col_name, "status"))           v = r.status;
    else if (0 == strcmp(col_name, "flags"))            v = r.flags;
    else if (0 == strcmp(col_name, "skip_cnt"))         v = r.skip_cnt;
    else if (0 == strcmp(col_name, "retry_count"))      v = r.retry_count;
    else if (0 == strcmp(col_name, "next_retry_ts")) {
      if (r.next_retry_ts_is_null) return common::OB_ERR_NULL_VALUE;
      v = r.next_retry_ts;
    }
    else if (0 == strcmp(col_name, "refresh_method"))   v = r.refresh_method;
    else if (0 == strcmp(col_name, "refresh_parallel")) v = r.refresh_parallel;
    else if (0 == strcmp(col_name, "gmt_create_ts"))    v = r.gmt_create_ts;
    else if (0 == strcmp(col_name, "gmt_modified_ts"))  v = r.gmt_modified_ts;
    else if (0 == strcmp(col_name, "dep_mview_id")) {
      if (r.dep_is_null) return common::OB_ERR_NULL_VALUE;
      v = r.dep_mview_id;
    }
    else if (0 == strcmp(col_name, "svr_port")) v = r.svr_port;
    else {
      return common::OB_ERR_COLUMN_NOT_FOUND;
    }
    return common::OB_SUCCESS;
  }

  int get_uint(const char *col_name, uint64_t &v) const override
  {
    if (cursor_ < 0 || cursor_ >= rows_.count()) {
      return common::OB_ERR_UNEXPECTED;
    }
    const FakeRow &r = rows_.at(cursor_);
    if (0 == strcmp(col_name, "target_data_sync_scn")) {
      v = r.target_data_sync_scn;
      return common::OB_SUCCESS;
    }
    // Important: if a column declared as BIGINT signed (e.g. mview_id) is
    // accessed via get_uint, the real server would reject the type cast. We
    // model that here so the pre-fix code path fails fast.
    return common::OB_ERR_COLUMN_NOT_FOUND;
  }

  // ---------------------------------------------------------------- stubs (unused)
  int get_int(const int64_t, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_uint(const int64_t, uint64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_datetime(const int64_t, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_date(const int64_t, int32_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_time(const int64_t, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_year(const int64_t, uint8_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_bool(const int64_t, bool &) const override { return common::OB_NOT_SUPPORTED; }
  int get_varchar(const int64_t, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }
  int get_raw(const int64_t, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }
  int get_float(const int64_t, float &) const override { return common::OB_NOT_SUPPORTED; }
  int get_double(const int64_t, double &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp(const int64_t, const common::ObTimeZoneInfo *, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_otimestamp_value(const int64_t, const common::ObTimeZoneInfo &,
                           const common::ObObjType, common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_tz(const int64_t, const common::ObTimeZoneInfo &,
                       common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_ltz(const int64_t, const common::ObTimeZoneInfo &,
                        common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_nano(const int64_t, const common::ObTimeZoneInfo &,
                         common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_type(const int64_t, common::ObObjMeta &) const override { return common::OB_NOT_SUPPORTED; }
  int get_col_meta(const int64_t, bool, common::ObString &, common::ObDataType &) const override { return common::OB_NOT_SUPPORTED; }
  int get_obj(const int64_t, common::ObObj &, const common::ObTimeZoneInfo *,
              common::ObIAllocator *) const override { return common::OB_NOT_SUPPORTED; }
  int get_interval_ym(const int64_t, common::ObIntervalYMValue &) const override { return common::OB_NOT_SUPPORTED; }
  int get_interval_ds(const int64_t, common::ObIntervalDSValue &) const override { return common::OB_NOT_SUPPORTED; }
  int get_nvarchar2(const int64_t, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }
  int get_nchar(const int64_t, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }

  int get_datetime(const char *, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_date(const char *, int32_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_time(const char *, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_year(const char *, uint8_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_bool(const char *, bool &) const override { return common::OB_NOT_SUPPORTED; }
  int get_varchar(const char *col_name, common::ObString &v) const override
  {
    if (cursor_ < 0 || cursor_ >= rows_.count()) {
      return common::OB_ERR_UNEXPECTED;
    }
    const FakeRow &r = rows_.at(cursor_);
    if (0 == strcmp(col_name, "svr_ip")) {
      if (nullptr == r.svr_ip) return common::OB_ERR_NULL_VALUE;
      v = common::ObString(r.svr_ip);
      return common::OB_SUCCESS;
    }
    return common::OB_ERR_COLUMN_NOT_FOUND;
  }
  int get_raw(const char *, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }
  int get_float(const char *, float &) const override { return common::OB_NOT_SUPPORTED; }
  int get_double(const char *, double &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp(const char *, const common::ObTimeZoneInfo *, int64_t &) const override { return common::OB_NOT_SUPPORTED; }
  int get_otimestamp_value(const char *, const common::ObTimeZoneInfo &,
                           const common::ObObjType, common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_tz(const char *, const common::ObTimeZoneInfo &,
                       common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_ltz(const char *, const common::ObTimeZoneInfo &,
                        common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_timestamp_nano(const char *, const common::ObTimeZoneInfo &,
                         common::ObOTimestampData &) const override { return common::OB_NOT_SUPPORTED; }
  int get_type(const char *, common::ObObjMeta &) const override { return common::OB_NOT_SUPPORTED; }
  int get_obj(const char *, common::ObObj &) const override { return common::OB_NOT_SUPPORTED; }
  int get_interval_ym(const char *, common::ObIntervalYMValue &) const override { return common::OB_NOT_SUPPORTED; }
  int get_interval_ds(const char *, common::ObIntervalDSValue &) const override { return common::OB_NOT_SUPPORTED; }
  int get_nvarchar2(const char *, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }
  int get_nchar(const char *, common::ObString &) const override { return common::OB_NOT_SUPPORTED; }

private:
  int inner_get_number(const int64_t, common::number::ObNumber &, IAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }
  int inner_get_number(const char *, common::number::ObNumber &, IAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }
  int inner_get_urowid(const char *, common::ObURowIDData &, common::ObIAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }
  int inner_get_urowid(const int64_t, common::ObURowIDData &, common::ObIAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }
  int inner_get_lob_locator(const char *, common::ObLobLocator *&, common::ObIAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }
  int inner_get_lob_locator(const int64_t, common::ObLobLocator *&, common::ObIAllocator &) const override
  { return common::OB_NOT_SUPPORTED; }

  common::ObSEArray<FakeRow, 16> rows_;
  int64_t cursor_;
};

// Plug a FakeMySQLResult into ObMySQLProxy::ReadResult via placement-new on its
// internal buffer. ObISQLClient::ReadResult::create_handler<T>(args...) will
// new (buf_) T(args...) here, so this handler is created in-place and only
// holds a pointer to the externally-owned FakeMySQLResult.
class FakeResultHandler : public common::sqlclient::ObISQLResultHandler
{
public:
  explicit FakeResultHandler(FakeMySQLResult *result) : result_(result) {}
  virtual ~FakeResultHandler() {}
  common::sqlclient::ObMySQLResult *mysql_result() override { return result_; }

private:
  FakeMySQLResult *result_;
};

} // namespace unittest
} // namespace oceanbase
