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

#ifndef OCEANBASE_OBSERVER_MYSQL_OB_QUERY_RETRY_CTRL_
#define OCEANBASE_OBSERVER_MYSQL_OB_QUERY_RETRY_CTRL_

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_basic_session_info.h"
namespace oceanbase {
namespace sql {
class ObMultiStmtItem;
class ObSqlCtx;
}  // namespace sql
namespace observer {

class ObMySQLResultSet;
class ObGlobalContext;
enum ObQueryRetryType {
  RETRY_TYPE_NONE,
  RETRY_TYPE_LOCAL,
  RETRY_TYPE_PACKET,
};

class ObQueryRetryCtrl {
private:
  enum RetrySleepType {
    RETRY_SLEEP_TYPE_LINEAR,
    RETRY_SLEEP_TYPE_INDEX,
  };

public:
  ObQueryRetryCtrl();
  virtual ~ObQueryRetryCtrl();

  void test_and_save_retry_state(const ObGlobalContext& gctx, const sql::ObSqlCtx& ctx, sql::ObResultSet& result,
      int err, int& client_ret, bool force_local_retry = false);
  void clear_state_before_each_retry(sql::ObQueryRetryInfo& retry_info)
  {
    retry_type_ = RETRY_TYPE_NONE;
    retry_err_code_ = OB_SUCCESS;
    retry_info.clear_state_before_each_retry();
  }
  ObQueryRetryType get_retry_type() const
  {
    return retry_type_;
  }
  sql::ObSessionRetryStatus need_retry() const
  {
    sql::ObSessionRetryStatus ret = sql::SESS_NOT_IN_RETRY;
    if (RETRY_TYPE_NONE != retry_type_) {
      if (OB_USE_DUP_FOLLOW_AFTER_DML != retry_err_code_) {
        ret = sql::SESS_IN_RETRY;
      } else {
        ret = sql::SESS_IN_RETRY_FOR_DUP_TBL;
      }
    }
    return ret;  // RETRY_TYPE_NONE != retry_type_;
  }
  int64_t get_retry_times() const
  {
    return retry_times_;
  }
  void reset_retry_times()
  {
    retry_times_ = 0;
  }

  // tenant version
  int64_t get_tenant_global_schema_version() const
  {
    return curr_query_tenant_global_schema_version_;
  }
  void set_tenant_global_schema_version(int64_t version)
  {
    curr_query_tenant_global_schema_version_ = version;
  }
  int64_t get_tenant_local_schema_version() const
  {
    return curr_query_tenant_local_schema_version_;
  }
  void set_tenant_local_schema_version(int64_t version)
  {
    curr_query_tenant_local_schema_version_ = version;
  }
  // sys version
  int64_t get_sys_global_schema_version() const
  {
    return curr_query_sys_global_schema_version_;
  }
  void set_sys_global_schema_version(int64_t version)
  {
    curr_query_sys_global_schema_version_ = version;
  }
  int64_t get_sys_local_schema_version() const
  {
    return curr_query_sys_local_schema_version_;
  }
  void set_sys_local_schema_version(int64_t version)
  {
    curr_query_sys_local_schema_version_ = version;
  }

  static uint32_t linear_timeout_factor(uint64_t times, uint64_t threshold = 100)
  {
    return static_cast<uint32_t>((times > threshold) ? threshold : times);
  }
  static uint32_t index_timeout_factor(uint64_t times, uint64_t threshold = 7)
  {
    return static_cast<uint32_t>(1 << ((times > threshold) ? threshold : times));
  }
  void set_in_async_execute(bool in_async_execute)
  {
    in_async_execute_ = in_async_execute;
  }

private:
  void try_packet_retry(const sql::ObMultiStmtItem& multi_stmt_item);
  void log_distributed_not_supported_user_error(int err);
  void sleep_before_local_retry(ObQueryRetryCtrl::RetrySleepType retry_sleep_type, int64_t base_sleep_us,
      int64_t retry_times, int64_t timeout_timestamp);
  bool is_isolation_RR_or_SE(int32_t isolation);

public:
  // magic
  static const int64_t MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES = 5;
  static const int64_t MAX_DATA_NOT_READABLE_ERROR_LOCAL_RETRY_TIMES = 1;
  static const uint32_t WAIT_LOCAL_SCHEMA_REFRESHED_US = 1 * 1000;
  static const uint32_t WAIT_NEW_MASTER_ELECTED_US = 8 * 1000;
  static const uint32_t WAIT_RETRY_WRITE_DML_US = 1 * 1000;

private:
  /* functions */
  /* variables */
  int64_t curr_query_tenant_local_schema_version_;
  int64_t curr_query_tenant_global_schema_version_;
  int64_t curr_query_sys_local_schema_version_;
  int64_t curr_query_sys_global_schema_version_;
  int64_t retry_times_;
  ObQueryRetryType retry_type_;
  int retry_err_code_;
  bool in_async_execute_;
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObQueryRetryCtrl);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_MYSQL_OB_QUERY_RETRY_CTRL_ */
//// end of header file
