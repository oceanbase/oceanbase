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
#include "lib/container/ob_tuple.h"
#include "sql/das/ob_das_retry_ctrl.h"
namespace oceanbase
{
namespace sql
{
class ObMultiStmtItem;
struct ObSqlCtx;
class ObResultSet;
}
namespace observer
{

class ObMySQLResultSet;
struct ObGlobalContext;
enum ObQueryRetryType
{
  RETRY_TYPE_NONE, // 不重试
  RETRY_TYPE_LOCAL, // 在本线程重试
  RETRY_TYPE_PACKET, // 扔回队列中重试
};

enum RetrySleepType
{
  RETRY_SLEEP_TYPE_NONE, // no sleep
  RETRY_SLEEP_TYPE_LINEAR, // 线性重试
  RETRY_SLEEP_TYPE_INDEX, // 指数重试
};

struct ObRetryParam
{
  ObRetryParam(const sql::ObSqlCtx &ctx,
               sql::ObResultSet &result,
               sql::ObSQLSessionInfo &session,
               int64_t curr_query_tenant_local_schema_version,
               int64_t curr_query_tenant_global_schema_version,
               int64_t curr_query_sys_local_schema_version,
               int64_t curr_query_sys_global_schema_version,
               const bool force_local_retry,
               const bool is_inner_sql,
               const bool is_from_pl,
               const int64_t stmt_retry_times,
               const int64_t local_retry_times,
               const int err,
               ObQueryRetryType &retry_type,
               int &client_ret)
      : no_more_test_(false),
        force_local_retry_(force_local_retry),
        is_inner_sql_(is_inner_sql),
        is_from_pl_(is_from_pl),
        ctx_(ctx),
        result_(result),
        session_(session),
        curr_query_tenant_local_schema_version_(curr_query_tenant_local_schema_version),
        curr_query_tenant_global_schema_version_(curr_query_tenant_global_schema_version),
        curr_query_sys_local_schema_version_(curr_query_sys_local_schema_version),
        curr_query_sys_global_schema_version_(curr_query_sys_global_schema_version),
        stmt_retry_times_(stmt_retry_times),
        local_retry_times_(local_retry_times),
        err_(err),
        retry_type_(retry_type),
        client_ret_(client_ret)
  {}
  // stop testing more policy if set to TRUE
  // We use this variable to enable chaining multipy policies
  bool no_more_test_;
  // for user obmp* connection, this is set when we definately know its a local retry
  // for inner connection, force_local_retry is always true
  const bool force_local_retry_;
  // Query is executed using inner connection, such as query in PL, query issued by kernel (such as
  // schema / location refresh), query triggered by DDL
  const bool is_inner_sql_;
  // Query is part of a PL block, which is executed using inner connection
  const bool is_from_pl_;
  const sql::ObSqlCtx &ctx_;
  sql::ObResultSet &result_; // for refresh location cache
  sql::ObSQLSessionInfo &session_;
  const int64_t curr_query_tenant_local_schema_version_; // Query开始、Loc刷新前的普通租户shm ver
  const int64_t curr_query_tenant_global_schema_version_; // Query开始时的普通租户schema version
  const int64_t curr_query_sys_local_schema_version_; // Query开始、Loc刷新前的系统租户shm ver
  const int64_t curr_query_sys_global_schema_version_; // Query开始时的系统租户schema version
  const int64_t stmt_retry_times_; // statement retry times, including each retry, local or packet
                                   // note: PL block don't have a stmt_retry_times_ attribute
  const int64_t local_retry_times_; // local retry times, reset to zero when packet retry
  const int err_;
  ObQueryRetryType &retry_type_;
  int &client_ret_;
  TO_STRING_KV(K_(force_local_retry), K_(stmt_retry_times), K_(local_retry_times),
               KR(err_), K_(retry_type), K_(client_ret));
};

class ObRetryPolicy
{
public:
  ObRetryPolicy() = default;
  ~ObRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const = 0;
protected:
  void try_packet_retry(ObRetryParam &v) const;
  void sleep_before_local_retry(ObRetryParam &v,
                                RetrySleepType retry_sleep_type,
                                int64_t base_sleep_us,
                                int64_t timeout_timestamp) const;
public:
  // schema类型的错误最多在本线程重试5次。
  // 5是拍脑袋决定的
  static const int64_t MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES = 5;
  // schema, rpc
  // 1ms, schema刷新只需要一个RPC来回
  static const uint32_t WAIT_RETRY_SHORT_US = 1 * 1000;
  // leader election
  // 8ms, 选出新的主耗时是秒级别(宕机14s，主动切换2s)
  static const uint32_t WAIT_RETRY_LONG_US = 8 * 1000;
private:
  static uint32_t linear_timeout_factor(uint64_t times, uint64_t threshold = 100)
  {
    return static_cast<uint32_t>((times > threshold) ? threshold : times);
  }
  static uint32_t index_timeout_factor(uint64_t times, uint64_t threshold = 7)
  {
    return static_cast<uint32_t>(1 << ((times > threshold) ? threshold : times));
  }
};

class ObRetryObject
{
public:
  explicit ObRetryObject(ObRetryParam &v) : v_(v) {}
  ~ObRetryObject() {}
  ObRetryObject &test(const ObRetryPolicy &policy)
  {
    if (v_.no_more_test_) {
      // do nothing
    } else {
      policy.test(v_);
    }
    return *this;
  }
private:
  ObRetryParam &v_;
};

class ObQueryRetryCtrl
{
public:
  ObQueryRetryCtrl();
  virtual ~ObQueryRetryCtrl();

  // build errcode processing map, after the map was built, it is readonly.
  static int init();
  // must ensure calling destroy after all threads exit
  static void destroy();

  //本接口目前在ObMPQuery和SPI使用，SPI使用的时候必须本地重试直至超时，所以需要传入force_local_retry为true
  //force_local_retry为true时，不做try_packet_retry
  void test_and_save_retry_state(const ObGlobalContext &gctx,
                                 const sql::ObSqlCtx &ctx,
                                 sql::ObResultSet &result,
                                 int err,
                                 int &client_ret,
                                 bool force_local_retry = false,
                                 bool is_inner_sql = false,
                                 bool is_from_pl = false);
  void set_packet_retry(const int err) {
    retry_type_ = RETRY_TYPE_PACKET;
    retry_err_code_ = err;
  }
  void clear_state_before_each_retry(sql::ObQueryRetryInfo &retry_info)
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
      if (OB_USE_DUP_FOLLOW_AFTER_DML != retry_err_code_ &&
          OB_NOT_MASTER != retry_err_code_) {
        ret = sql::SESS_IN_RETRY;
      } else {
        ret = sql::SESS_IN_RETRY_FOR_DUP_TBL;
      }
    }
    return ret; //RETRY_TYPE_NONE != retry_type_;
  }
  int64_t get_retry_times() const
  {
    return retry_times_;
  }
  void reset_retry_times() { retry_times_ = 0; }

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
  static inline bool is_isolation_RR_or_SE(transaction::ObTxIsolationLevel isolation)
  {
    return (isolation == transaction::ObTxIsolationLevel::RR
            || isolation == transaction::ObTxIsolationLevel::SERIAL);
  }
  static int get_das_retry_func(int err, sql::ObDASRetryCtrl::retry_func &retry_func);
public:
  // schema类型的错误最多在本线程重试5次。
  // 5是拍脑袋决定的，之后还要看统计数据的反馈再修改。TODO qianfu.zpf
  static const int64_t MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES = 5;
  // 副本不可读类型的错误最多在本线程重试1次。
  static const int64_t MAX_DATA_NOT_READABLE_ERROR_LOCAL_RETRY_TIMES = 1;
  // 1ms, schema刷新只需要一个RPC来回
  static const uint32_t WAIT_LOCAL_SCHEMA_REFRESHED_US = 1 * 1000;
  // 8ms, 选出新的主耗时是秒级别(宕机14s，主动切换2s)
  static const uint32_t WAIT_NEW_MASTER_ELECTED_US = 8 * 1000;
  // 1ms，重试write dml等待时间
  static const uint32_t WAIT_RETRY_WRITE_DML_US = 1 * 1000;

private:
  /* functions */
  typedef void (*retry_func)(ObRetryParam &);

  // find err code processor in map_
  int get_func(int err, bool is_inner, retry_func &func);

  // default processor hook
  static void before_func(ObRetryParam &v);
  static void after_func(ObRetryParam &v);

  // various processors for error codes
  static void empty_proc(ObRetryParam &v);
  static void px_thread_not_enough_proc(ObRetryParam &v);
  static void trx_set_violation_proc(ObRetryParam &v);
  static void trx_can_not_serialize_proc(ObRetryParam &v);
  static void try_lock_row_conflict_proc(ObRetryParam &v);
  static void location_error_proc(ObRetryParam &v);
  static void nonblock_location_error_proc(ObRetryParam &v);
  static void location_error_nothing_readable_proc(ObRetryParam &v);
  static void peer_server_status_uncertain_proc(ObRetryParam &v);
  static void schema_error_proc(ObRetryParam &v);
  static void snapshot_discard_proc(ObRetryParam &v);
  static void long_wait_retry_proc(ObRetryParam &v);
  static void short_wait_retry_proc(ObRetryParam &v);
  static void force_local_retry_proc(ObRetryParam &v);
  static void batch_execute_opt_retry_proc(ObRetryParam &v);
  static void switch_consumer_group_retry_proc(ObRetryParam &v);
  static void timeout_proc(ObRetryParam &v);
  static void autoinc_cache_not_equal_retry_proc(ObRetryParam &v);


  // processors for inner SQL error codes only
  static void inner_common_schema_error_proc(ObRetryParam &v);
  static void inner_schema_error_proc(ObRetryParam &v);
  static void inner_try_lock_row_conflict_proc(ObRetryParam &v);
  static void inner_table_location_error_proc(ObRetryParam &v);
  static void inner_location_error_proc(ObRetryParam &v);
  static void inner_location_error_nothing_readable_proc(ObRetryParam &v);
  static void inner_peer_server_status_uncertain_proc(ObRetryParam &v);
  void on_close_resultset_fail_(const int err, int &client_ret);

  /* variables */
  // map_ is used to fast lookup the error code retry processor
  typedef common::ObTuple<retry_func, retry_func, sql::ObDASRetryCtrl::retry_func> RetryFuncs;
  static common::hash::ObHashMap<int, RetryFuncs, common::hash::NoPthreadDefendMode> map_;
  int64_t curr_query_tenant_local_schema_version_; // Query开始、Loc刷新前的普通租户shm ver
  int64_t curr_query_tenant_global_schema_version_; // Query开始时的普通租户schema version
  int64_t curr_query_sys_local_schema_version_; // Query开始、Loc刷新前的系统租户shm ver
  int64_t curr_query_sys_global_schema_version_; // Query开始时的系统租户schema version
  int64_t retry_times_;
  ObQueryRetryType retry_type_;
  int retry_err_code_; //记录重试时的错误码(目前用于区分复制表引起重试)
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObQueryRetryCtrl);
};

} /* observer */
} /* oceanbase */
#endif /* OCEANBASE_OBSERVER_MYSQL_OB_QUERY_RETRY_CTRL_ */
//// end of header file
