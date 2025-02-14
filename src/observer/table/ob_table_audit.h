/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_AUDIT_H_
#define OCEANBASE_OBSERVER_OB_TABLE_AUDIT_H_

#include "ob_htable_filters.h"
#include "ob_table_session_pool.h"
#include "sql/monitor/ob_exec_stat.h"
#include "share/table/ob_table.h"
#include "lib/stat/ob_diagnose_info.h"
#include "sql/ob_sql_utils.h"
#include "src/observer/mysql/obmp_base.h"
#include "observer/mysql/ob_mysql_request_manager.h"

namespace oceanbase
{
namespace table
{

struct ObTableAuditCtx
{
public:
  ObTableAuditCtx(const int32_t &retry_count, const common::ObAddr &user_client_addr, bool need_audit = true)
      : allocator_("TableAuditCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        req_buf_(nullptr),
        req_buf_len_(0),
        retry_count_(retry_count),
        user_client_addr_(user_client_addr)
  {
    need_audit_ = GCONF.enable_sql_audit && need_audit;
  }
  virtual ~ObTableAuditCtx() {};
  TO_STRING_KV(K_(need_audit),
               K(common::ObString(req_buf_len_, req_buf_)),
               K_(exec_timestamp),
               K_(retry_count),
               K_(user_client_addr));
  template<typename T>
  int generate_request_string(const T &request)
  {
    int ret = OB_SUCCESS;
    if (need_audit_) {
      //static thread_local char request_str[32L<<10];
      SMART_VAR(char[16L<<10], request_str) {
        int64_t pos = 0;
        if (OB_FAIL(databuff_print_obj(request_str, sizeof(request_str), pos, request))) {
          COMMON_LOG(WARN, "fail to format request", K(ret));
        } else {
          const int64_t request_str_len = pos;
          const int64_t buf_size = request_str_len;
          char *buf = reinterpret_cast<char *>(allocator_.alloc(request_str_len));
          if (NULL == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            COMMON_LOG(WARN, "fail to alloc request string memory", K(ret), K(buf_size));
          } else {
            MEMCPY(buf, request_str, request_str_len);
            req_buf_ = buf;
            req_buf_len_ = buf_size;
          }
        }
      }
    }
    return ret;
  }
  OB_INLINE common::ObString get_request_string() { return common::ObString(req_buf_len_, req_buf_); }
public:
  ObArenaAllocator allocator_;
  bool need_audit_;
  char *req_buf_;
  int64_t req_buf_len_;
  sql::ObExecTimestamp exec_timestamp_;
  const int32_t &retry_count_;
  const common::ObAddr &user_client_addr_;
};

template<typename T>
struct ObTableAudit
{
public:
  static const int64_t DEFUALT_STMT_BUF_SIZE = 256;
public:
  ObTableAudit(const T &op,
               const common::ObString &table_name,
               ObTableApiSessGuard &sess_guard,
               ObTableAuditCtx *audit_ctx)
      : allocator_("ObTableAudit", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        need_audit_(false),
        is_started_(false),
        request_string_(nullptr),
        request_string_len_(0),
        start_ts_(0),
        table_name_(table_name),
        op_(op),
        filter_(nullptr),
        audit_ctx_(audit_ctx),
        op_stmt_ptr_(nullptr),
        op_stmt_length_(0),
        op_stmt_pos_(0),
        sess_guard_(sess_guard)
  {
    bool is_ctx_enable_audit = OB_NOT_NULL(audit_ctx_) ? audit_ctx_->need_audit_ : true; // default enable
    need_audit_ = GCONF.enable_sql_audit && is_ctx_enable_audit;
  }
  ~ObTableAudit() {};
public:
  void start_audit(const ObTableApiCredential &credential, const common::ObString &sql_str)
  {
    if (!need_audit_) {
      // do nothing
    } else {
      start_ts_ = ObTimeUtility::fast_current_time();
      uint64_t tenant_id = credential.tenant_id_;
      uint64_t user_id = credential.user_id_;
      uint64_t database_id = credential.database_id_;

      record_.seq_ = 0;
      record_.trace_id_ = *ObCurTraceId::get_trace_id();
      record_.request_id_ = 0;
      record_.execution_id_ = 0;
      record_.session_id_ = 0;
      record_.user_client_addr_ = ObCurTraceId::get_addr();
      record_.tenant_id_ = tenant_id;
      record_.effective_tenant_id_ = tenant_id;
      record_.user_id_ = user_id;
      record_.db_id_ = database_id;

      // tenant name、user name、database name
      record_.tenant_name_ = const_cast<char *>(sess_guard_.get_tenant_name().ptr());
      record_.tenant_name_len_ = sess_guard_.get_tenant_name().length();
      record_.user_name_ = const_cast<char *>(sess_guard_.get_user_name().ptr());
      record_.user_name_len_ = sess_guard_.get_user_name().length();
      record_.db_name_ = const_cast<char *>(sess_guard_.get_database_name().ptr());
      record_.db_name_len_ = sess_guard_.get_database_name().length();

      record_.sql_cs_type_ = ObCharset::get_system_collation();
      record_.plan_id_ = 0;
      record_.partition_cnt_ = 1;
      record_.plan_type_ = sql::OB_PHY_PLAN_UNINITIALIZED;
      record_.is_executor_rpc_ = false;
      record_.is_inner_sql_ = false;
      record_.is_hit_plan_cache_ = false;
      record_.is_multi_stmt_ = false;
      record_.table_scan_ = false;
      record_.consistency_level_ = ObConsistencyLevel::STRONG;

      // request string
      record_.sql_ = const_cast<char *>(sql_str.ptr());
      record_.sql_len_ = sql_str.length();

      // stmt (not include filter)
      int ret = OB_SUCCESS;
      op_stmt_length_ = op_.get_stmt_length(table_name_) + 1; // "\0"
      if (op_stmt_length_ <= sizeof(stmt_buf_)) {
        op_stmt_ptr_ = stmt_buf_;
      } else {
        op_stmt_ptr_ = reinterpret_cast<char *>(allocator_.alloc(op_stmt_length_));
      }
      if (NULL == op_stmt_ptr_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to alloc stmt buf", K(ret), K_(op_stmt_length));
      } else if (OB_FAIL(op_.generate_stmt(table_name_, op_stmt_ptr_, op_stmt_length_, op_stmt_pos_))) {
        COMMON_LOG(WARN, "fail to generate statement", K(ret), K_(table_name),
            K_(op_stmt_length), K_(op_stmt_pos));
      } else {
        is_started_ = true;
      }
    }
  }
  void end_audit()
  {
    if (!need_audit_ || !is_started_) {
      // do nothing
    } else {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(audit_ctx_)) {
        record_.try_cnt_ = audit_ctx_->retry_count_;
        record_.user_client_addr_ = audit_ctx_->user_client_addr_;
        record_.exec_timestamp_.process_executor_ts_ = audit_ctx_->exec_timestamp_.process_executor_ts_;
        record_.exec_timestamp_.run_ts_ = audit_ctx_->exec_timestamp_.run_ts_;
        record_.exec_timestamp_.before_process_ts_ = audit_ctx_->exec_timestamp_.before_process_ts_;
        record_.exec_timestamp_.rpc_send_ts_ = audit_ctx_->exec_timestamp_.rpc_send_ts_;
        record_.exec_timestamp_.receive_ts_ = audit_ctx_->exec_timestamp_.receive_ts_;
        record_.exec_timestamp_.enter_queue_ts_ = audit_ctx_->exec_timestamp_.enter_queue_ts_;
        record_.exec_timestamp_.single_process_ts_ = audit_ctx_->exec_timestamp_.single_process_ts_;
      }
      // exec_timestamp
      record_.exec_timestamp_.exec_type_ = ExecType::RpcProcessor;
      record_.exec_timestamp_.net_t_ = 0;
      record_.exec_timestamp_.net_wait_t_ = record_.exec_timestamp_.enter_queue_ts_ - record_.exec_timestamp_.receive_ts_;
      record_.exec_timestamp_.process_executor_ts_ = start_ts_; // The start timestamp of a plan
      record_.exec_timestamp_.executor_end_ts_ = ObTimeUtility::fast_current_time(); // The end timestamp of a plan
      record_.exec_timestamp_.update_stage_time();
      // wait time
      record_.exec_record_.wait_time_end_ = total_wait_desc_.time_waited_;
      record_.exec_record_.wait_count_end_ = total_wait_desc_.total_waits_;
      if (lib::is_diagnose_info_enabled()) {
        record_.exec_record_.record_end();
        record_.update_event_stage_state();
      }

      const int64_t elapsed_time = common::ObTimeUtility::fast_current_time() - record_.exec_timestamp_.receive_ts_;
      if (elapsed_time > GCONF.trace_log_slow_query_watermark) {
        FORCE_PRINT_TRACE(THE_TRACE, "[table api][slow query]");
      }

      // sql id
      bool has_filter = (filter_ != nullptr);
      if (op_stmt_pos_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "generate operation statement failed before", K(ret), K_(op_stmt_pos));
      } else if (!has_filter) {
        common::ObString stmt(op_stmt_length_, op_stmt_ptr_);
        if (OB_FAIL(sql::ObSQLUtils::md5(stmt, record_.sql_id_, (int32_t)sizeof(record_.sql_id_)))) {
          COMMON_LOG(WARN, "fail to generate sql id", K(ret), K(stmt));
        }
      } else { // has filter
        int64_t filter_stmt_len = filter_->get_format_filter_string_length() + 1; // "\0"
        int64_t filter_stmt_pos = 0;
        int64_t stmt_capacity = op_stmt_length_ + filter_stmt_len;
        char *stmt_buf = nullptr;
        if (stmt_capacity <= sizeof(stmt_buf_)) {
          stmt_buf = stmt_buf_;
        } else {
          stmt_buf = reinterpret_cast<char *>(allocator_.alloc(stmt_capacity));
        }
        if (NULL == stmt_buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "fail to alloc stmt buf", K(ret), K(stmt_capacity));
        } else {
          MEMCPY(stmt_buf, op_stmt_ptr_, op_stmt_pos_);
          if (OB_FAIL(filter_->get_format_filter_string(stmt_buf + op_stmt_pos_,
              stmt_capacity - op_stmt_pos_, filter_stmt_pos))) {
            COMMON_LOG(WARN, "fail to get format filter string", K(ret), K_(op_stmt_pos),
                K(stmt_capacity), K(filter_stmt_pos));
          } else {
            common::ObString stmt(op_stmt_pos_ + filter_stmt_pos, stmt_buf);
            if (OB_FAIL(sql::ObSQLUtils::md5(stmt, record_.sql_id_, (int32_t)sizeof(record_.sql_id_)))) {
              COMMON_LOG(WARN, "fail to generate sql id", K(ret), K(stmt));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const bool enable_stats = false; // stats has been recorded in record_stat
        const int64_t record_limit = TABLEAPI_SESS_POOL_MGR->get_query_record_size_limit();
        MTL_SWITCH(record_.tenant_id_) {
          obmysql::ObMySQLRequestManager *req_manager = MTL(obmysql::ObMySQLRequestManager*);
          if (OB_ISNULL(req_manager)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "fail to get request manager", K(ret), K(record_.tenant_id_));
          } else if (OB_FAIL(req_manager->record_request(record_, enable_stats, record_limit))) {
            if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
              COMMON_LOG(DEBUG, "cannot allocate mem for record", K(ret));
              ret = OB_SUCCESS;
            } else {
              if (REACH_TIME_INTERVAL(100 * 1000)) { // in case logging is too frequent
                COMMON_LOG(WARN, "fail to record request info in request manager", K(ret));
              }
            }
          }
        }
      }
    }
  }
  OB_INLINE void set_tb_audit_stmt_type(sql::stmt::StmtType stmt_type) { record_.stmt_type_ = stmt_type; }
  OB_INLINE void set_tb_audit_ret_code(int ret_code) { record_.status_ = ret_code; }
  OB_INLINE void set_tb_audit_return_rows(int64_t return_rows) { record_.return_rows_ = return_rows; }
  OB_INLINE void set_tb_audit_has_table_scan(int64_t has_table_scan) { record_.table_scan_ = has_table_scan; }
  OB_INLINE void set_tb_audit_snapshot(const transaction::ObTxReadSnapshot &snapshot)
  {
    record_.trans_id_ = snapshot.core_.tx_id_.get_id();
    record_.snapshot_.version_ = snapshot.core_.version_;
    record_.snapshot_.tx_id_ = snapshot.core_.tx_id_.get_id();
    record_.snapshot_.scn_ = snapshot.core_.scn_.cast_to_int();
    record_.snapshot_.source_ = snapshot.get_source_name();
  }
  OB_INLINE void set_tb_audit_filter(const hfilter::Filter *filter) { filter_ = filter; }
public:
  ObArenaAllocator allocator_;
  bool need_audit_;
  bool is_started_;
  const char *request_string_;
  int64_t request_string_len_;
  common::ObWaitEventStat total_wait_desc_;
  sql::ObAuditRecordData record_;
  int64_t start_ts_;
  const common::ObString table_name_;
  const T &op_;
  const hfilter::Filter *filter_;
  ObTableAuditCtx *audit_ctx_;
  char stmt_buf_[DEFUALT_STMT_BUF_SIZE];
  char *op_stmt_ptr_;
  int64_t op_stmt_length_;
  int64_t op_stmt_pos_;
  ObTableApiSessGuard &sess_guard_;
};

class ObTableAuditUtils
{
public:
  static sql::stmt::StmtType get_stmt_type(ObTableOperationType::Type op_type);
};

struct ObTableAuditMultiOp
{
  static const int64_t MULTI_PREFIX_LEN = 5;
public:
  ObTableAuditMultiOp(ObTableOperationType::Type op_type, const common::ObIArray<ObTableOperation> &ops)
      : op_type_(op_type),
        ops_(ops)
  {}
  virtual ~ObTableAuditMultiOp() = default;
  int64_t get_stmt_length(const common::ObString &table_name) const;
  int generate_stmt(const common::ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const;
public:
  ObTableOperationType::Type op_type_;
  const common::ObIArray<ObTableOperation> &ops_;
};

struct ObTableAuditRedisOp
{
public:
  ObTableAuditRedisOp(const common::ObString &cmd_name)
      : cmd_name_(cmd_name)
  {}
  virtual ~ObTableAuditRedisOp() = default;
  int64_t get_stmt_length(const common::ObString &table_name) const;
  int generate_stmt(const common::ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const;
public:
  const common::ObString &cmd_name_;
};

/*
The following macro definition is used to record sql audit, how to use:
  int func()
  {
    OB_TABLE_START_AUDIT(...);
    ... // do something
    OB_TABLE_END_AUDIT(...);
  }
*/
#define OB_TABLE_END_AUDIT2(key, value)         \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    audit.end_audit();                          \
  }
#define OB_TABLE_END_AUDIT4(key, value, ...)    \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT2(__VA_ARGS__)            \
  }
#define OB_TABLE_END_AUDIT6(key, value, ...)    \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT4(__VA_ARGS__)            \
  }
#define OB_TABLE_END_AUDIT8(key, value, ...)    \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT6(__VA_ARGS__)            \
  }
#define OB_TABLE_END_AUDIT10(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT8(__VA_ARGS__)            \
  }
#define OB_TABLE_END_AUDIT12(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT10(__VA_ARGS__)           \
  }
#define OB_TABLE_END_AUDIT14(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT12(__VA_ARGS__)           \
  }
#define OB_TABLE_END_AUDIT16(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT14(__VA_ARGS__)           \
  }
#define OB_TABLE_END_AUDIT18(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT16(__VA_ARGS__)           \
  }
#define OB_TABLE_END_AUDIT20(key, value, ...)   \
  if (audit.need_audit_) {                      \
    audit.set_tb_audit_##key(value);            \
    OB_TABLE_END_AUDIT18(__VA_ARGS__)           \
  }

#define OB_TABLE_END_AUDIT_true(...) CONCAT(OB_TABLE_END_AUDIT, ARGS_NUM(__VA_ARGS__))(__VA_ARGS__)
#define OB_TABLE_END_AUDIT_false(...)
#define OB_TABLE_END_AUDIT(...) OB_TABLE_END_AUDIT_true(__VA_ARGS__)
#define OB_TABLE_END_AUDIT_SWITCH(enable, ...) OB_TABLE_END_AUDIT_##enable(__VA_ARGS__)

#define OB_TABLE_START_AUDIT_true(credential, sess_guard, table_name, audit_ctx, op)      \
  table::ObTableAudit<decltype(op)> audit(op, table_name, sess_guard, audit_ctx);         \
  common::ObMaxWaitGuard max_wait_guard(&audit.record_.exec_record_.max_wait_event_);     \
  common::ObTotalWaitGuard total_wait_guard(&audit.total_wait_desc_);                     \
  observer::ObProcessMallocCallback pmcb(0, audit.record_.request_memory_used_);          \
  lib::ObMallocCallbackGuard malloc_guard_(pmcb);                                         \
  if (audit.need_audit_ && OB_NOT_NULL(audit_ctx)) {                                      \
    if (lib::is_diagnose_info_enabled()) {                                                \
      audit.record_.exec_record_.record_start();                                          \
    }                                                                                     \
    audit.audit_ctx_ = audit_ctx;                                                         \
    audit.start_audit(credential, (audit_ctx)->get_request_string());                     \
  }

#define OB_TABLE_START_AUDIT_false(credential, sess_guard, table_name, audit_ctx, op)
#define OB_TABLE_START_AUDIT(credential, sess_guard, table_name, audit_ctx, op) \
  OB_TABLE_START_AUDIT_true(credential, sess_guard, table_name, audit_ctx, op)
#define OB_TABLE_START_AUDIT_SWITCH(enable, credential, sess_guard, table_name, audit_ctx, op) \
  OB_TABLE_START_AUDIT_##enable(credential, sess_guard, table_name, audit_ctx, op)

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_AUDIT_H_ */
