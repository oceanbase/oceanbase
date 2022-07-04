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

#ifndef _OB_OBPROXY_BASIC_SESSION_INFO_H
#define _OB_OBPROXY_BASIC_SESSION_INFO_H 1

#include "share/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "lib/lock/ob_lock_guard.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/timezone/ob_timezone_info.h"
#include "common/ob_hint.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_time_zone_info_manager.h"
#include "storage/transaction/ob_trans_define.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/client_feedback/ob_client_feedback_manager.h"
#include "sql/session/ob_session_val_map.h"
#include "sql/ob_sql_mode_manager.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_trans_util.h"
#include "share/partition_table/ob_partition_location.h"
#include "common/sql_mode/ob_sql_mode_utils.h"

namespace oceanbase {
namespace sql {
class ObPCMemPctConf;
class ObPartitionHitInfo {
public:
  ObPartitionHitInfo() : value_(true), freeze_(false)
  {}
  ~ObPartitionHitInfo()
  {}
  bool get_bool()
  {
    return value_;
  }
  void try_set_bool(bool v)
  {
    if (!freeze_)
      value_ = v;
  }
  void freeze()
  {
    freeze_ = true;
  }
  void reset()
  {
    value_ = true;
    freeze_ = false;
  }

private:
  bool value_;
  bool freeze_;
};
struct ObSessionNLSParams  // oracle nls parameters
{
  ObLengthSemantics nls_length_semantics_;
  ObCollationType nls_collation_;         // for oracle char and varchar2
  ObCollationType nls_nation_collation_;  // for oracle nchar and nvarchar2

  TO_STRING_KV(K(nls_length_semantics_), K(nls_collation_), K(nls_nation_collation_));
};

#define TZ_INFO(session) (NULL != (session) ? (session)->get_timezone_info() : NULL)

#define GET_NLS_FORMATS(session) (NULL != (session) ? (session)->get_local_nls_formats() : NULL)

#define CREATE_OBJ_PRINT_PARAM(session) (NULL != (session) ? (session)->create_obj_print_params() : ObObjPrintParams())

// flag is a single bit, but marco(e.g., IS_NO_BACKSLASH_ESCAPES) compare two 64-bits int using '&';
// if we directly assign the result to flag(single bit), only the last bit of the result is used,
// which is equal to 'flag = result & 1;'.
// So we first convert the result to bool(tmp_flag) and assign the bool to flag, which is equal to
// 'flag = result!=0;'.
#define GET_SQL_MODE_BIT(marco, sql_mode, flag) \
  do {                                          \
    bool tmp_flag = false;                      \
    marco(sql_mode, tmp_flag);                  \
    flag = tmp_flag;                            \
  } while (0)

#ifndef NDEBUG
#define CHECK_COMPATIBILITY_MODE(session)    \
  do {                                       \
    if (NULL != (session)) {                 \
      (session)->check_compatibility_mode(); \
    }                                        \
  } while (0)
#else
#define CHECK_COMPATIBILITY_MODE(session) UNUSED(session)
#endif

class ObExecContext;
class ObSysVarInPC;

enum ObSQLSessionState {
  SESSION_INIT,
  SESSION_SLEEP,
  QUERY_ACTIVE,
  QUERY_KILLED,
  SESSION_KILLED,
};

enum ObTransSpecfiedStatus {
  TRANS_SPEC_NOT_SET,
  TRANS_SPEC_SET,
  TRANS_SPEC_COMMIT,
};

enum ObSessionRetryStatus {
  SESS_NOT_IN_RETRY,          // session is not in retry process.
  SESS_IN_RETRY,              // session is retrying without duplicate table.
  SESS_IN_RETRY_FOR_DUP_TBL,  // // session is retrying with duplicate table.
};
// all system variables ars stored in sys_var_val_map_,
// frequently used variables are also stored in sys_vars_cache_ for easy access,
// pay attention to the consistency of the two copies when updating and serializing.
class ObBasicSessionInfo {
  OB_UNIS_VERSION_V(1);

public:
  // 256KB ~= 4 * OB_COMMON_MEM_BLOCK_SIZE
  static const int64_t APPROX_MEM_USAGE_PER_SESSION = 256 * 1024L;
  static const uint64_t VALID_PROXY_SESSID = 0;
  static const uint32_t INVALID_SESSID = UINT32_MAX;

  enum SafeWeakReadSnapshotSource {
    UNKOWN,
    AUTO_PLAN,
    PROXY_INPUT,
    LAST_STMT,
  };
  typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, share::ObBasicSysVar*>::AllocType,
      common::ObWrapperAllocator>
      SysVarNameValMapAllocer;
  typedef common::hash::ObHashMap<common::ObString, share::ObBasicSysVar*, common::hash::NoPthreadDefendMode,
      common::hash::hash_func<common::ObString>, common::hash::equal_to<common::ObString>, SysVarNameValMapAllocer,
      common::hash::NormalPointer, common::ObWrapperAllocator>
      SysVarNameValMap;
  typedef lib::ObLockGuard<common::ObRecursiveMutex> LockGuard;

  class TableStmtType {
    OB_UNIS_VERSION_V(1);

  public:
    TableStmtType() : table_id_(common::OB_INVALID_ID), stmt_type_(stmt::T_NONE)
    {}
    virtual ~TableStmtType()
    {}
    inline bool is_select_stmt() const
    {
      return stmt_type_ == stmt::T_SELECT;
    }
    inline bool operator==(const TableStmtType& rv) const
    {
      return table_id_ == rv.table_id_;
    }
    TO_STRING_KV(K(table_id_), K(stmt_type_));

  public:
    uint64_t table_id_;
    stmt::StmtType stmt_type_;
  };

  static const int64_t MIN_CUR_QUERY_LEN = 512;
  static const int64_t MAX_CUR_QUERY_LEN = 16 * 1024;
  static const int64_t MAX_QUERY_STRING_LEN = 64 * 1024;
  class TransFlags {
  public:
    TransFlags() : flags_(0)
    {}
    virtual ~TransFlags()
    {}
    inline void reset()
    {
      flags_ = 0;
    }
    inline void set_is_in_transaction(bool value)
    {
      set_flag(value, IS_IN_TRANSACTION);
    }
    inline void set_has_exec_write_stmt(bool value)
    {
      set_flag(value, HAS_EXEC_WRITE_STMT);
    }
    inline void set_has_set_trans_var(bool value)
    {
      set_flag(value, HAS_SET_TRANS_VAR);
    }
    inline void set_has_hold_row_lock(bool value)
    {
      set_flag(value, HAS_HOLD_ROW_LOCK);
    }
    inline void set_has_any_dml_succ(bool value)
    {
      set_flag(value, HAS_ANY_DML_SUCC);
    }
    inline void set_has_any_pl_succ(bool value)
    {
      set_flag(value, HAS_ANY_PL_SUCC);
    }
    inline void set_has_remote_plan(bool value)
    {
      set_flag(value, HAS_REMOTE_PLAN);
    }
    inline void set_has_inner_dml_write(bool value)
    {
      set_flag(value, HAS_INNER_DML_WRITE);
    }
    inline void set_need_serial_exec(bool value)
    {
      set_flag(value, NEED_SERIAL_EXEC);
    }
    inline bool is_in_transaction() const
    {
      return flags_ & IS_IN_TRANSACTION;
    }
    inline bool has_exec_write_stmt() const
    {
      return flags_ & HAS_EXEC_WRITE_STMT;
    }
    inline bool has_set_trans_var() const
    {
      return flags_ & HAS_SET_TRANS_VAR;
    }
    inline bool has_hold_row_lock() const
    {
      return flags_ & HAS_HOLD_ROW_LOCK;
    }
    inline bool has_any_dml_succ() const
    {
      return flags_ & HAS_ANY_DML_SUCC;
    }
    inline bool has_any_pl_succ() const
    {
      return flags_ & HAS_ANY_PL_SUCC;
    }
    inline bool has_remote_plan() const
    {
      return flags_ & HAS_REMOTE_PLAN;
    }
    inline bool has_inner_dml_write() const
    {
      return flags_ & HAS_INNER_DML_WRITE;
    }
    inline bool need_serial_exec() const
    {
      return flags_ & NEED_SERIAL_EXEC;
    }
    inline uint64_t get_flags() const
    {
      return flags_;
    }

  private:
    inline void set_flag(bool value, uint64_t flag)
    {
      if (value) {
        flags_ |= flag;
      } else {
        flags_ &= ~flag;
      }
      return;
    }

  private:
    static const uint64_t IS_IN_TRANSACTION = 1ULL << 0;
    static const uint64_t HAS_EXEC_WRITE_STMT = 1ULL << 1;
    static const uint64_t HAS_SET_TRANS_VAR = 1ULL << 2;  // has executed set transaction stmt.
    static const uint64_t HAS_HOLD_ROW_LOCK = 1ULL << 3;
    static const uint64_t HAS_ANY_DML_SUCC = 1ULL << 4;
    static const uint64_t HAS_ANY_PL_SUCC = 1ULL << 5;
    static const uint64_t HAS_REMOTE_PLAN = 1ULL << 6;
    static const uint64_t HAS_INNER_DML_WRITE = 1ULL << 7;
    static const uint64_t NEED_SERIAL_EXEC = 1ULL << 8;
    uint64_t flags_;
  };
  // when switching to autonomous transaction, we must switch to nested statements,
  // otherwise, the context of current statement execution may have changed after
  // switching back to the main transaction, such as:
  // so TransSavedValue should contain all members of StmtSavedValue, but some common
  // members in both classes need different operations, we decided to put those common
  // members with same operations into common base class BaseSavedValue, and leave those
  // common members with different operations in TransSavedValue or StmtSavedValue.
  // you need follow this principle when adding new members in future.
  class BaseSavedValue {
  public:
    BaseSavedValue()
    {
      reset();
    }
    inline void reset()
    {
      cur_phy_plan_ = NULL;
      cur_query_[0] = 0;
      cur_query_len_ = 0;
      total_stmt_tables_.reset();
      cur_stmt_tables_.reset();
      read_uncommited_ = false;
      base_autocommit_ = false;
      inc_autocommit_ = false;
      is_inc_autocommit_ = false;
      is_foreign_key_cascade_ = false;
      need_serial_exec_ = false;
    }

  public:
    ObPhysicalPlan* cur_phy_plan_;
    char cur_query_[MAX_CUR_QUERY_LEN];
    volatile int64_t cur_query_len_;
    common::ObSEArray<TableStmtType, 4> total_stmt_tables_;
    common::ObSEArray<TableStmtType, 2> cur_stmt_tables_;
    bool read_uncommited_;
    bool is_inc_autocommit_;
    bool inc_autocommit_;
    bool base_autocommit_;
    bool is_foreign_key_cascade_;
    bool is_foreign_key_check_exist_;
    bool need_serial_exec_;

  public:
  };
  // for switch stmt.
  class StmtSavedValue : public BaseSavedValue {
  public:
    StmtSavedValue()
    {
      reset();
    }
    inline void reset()
    {
      BaseSavedValue::reset();
      trans_desc_.reset();
      trans_result_.reset();
      cur_query_start_time_ = 0;
      in_transaction_ = false;
    }

  public:
    transaction::ObTransDesc trans_desc_;
    TransResult trans_result_;
    int64_t cur_query_start_time_;
    bool in_transaction_;
  };
  // for switch trans.
  class TransSavedValue : public BaseSavedValue {
  public:
    TransSavedValue()
    {
      reset();
    }
    void reset()
    {
      BaseSavedValue::reset();
      trans_desc_.reset();
      trans_flags_.reset();
      trans_result_.reset();
      nested_count_ = -1;
    }

  public:
    transaction::ObTransDesc trans_desc_;
    TransFlags trans_flags_;
    TransResult trans_result_;
    int64_t nested_count_;
  };

public:
  ObBasicSessionInfo();
  virtual ~ObBasicSessionInfo();

  virtual int init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator,
      const ObTZInfoMap* tz_info);
  // for test
  virtual int test_init(
      uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator);
  virtual void destroy();
  // called before put session to freelist: unlock/set invalid
  virtual void reset(bool skip_sys_var = false);
  virtual void clean_status();
  // setters
  void reset_timezone();
  int init_tenant(const common::ObString& tenant_name, const uint64_t tenant_id);
  int set_tenant(const common::ObString& tenant_name, const uint64_t tenant_id);
  int set_tenant(const common::ObString& tenant_name, const uint64_t tenant_id, char* ori_tenant_name,
      const uint64_t length, uint64_t& ori_tenant_id);
  int switch_tenant(uint64_t effective_tenant_id);
  int set_default_database(
      const common::ObString& database_name, common::ObCollationType coll_type = common::CS_TYPE_INVALID);
  int reset_default_database()
  {
    return set_default_database("");
  }
  int update_database_variables(share::schema::ObSchemaGetterGuard* schema_guard);
  int update_max_packet_size();
  int64_t get_thread_id() const
  {
    return thread_id_;
  }
  void set_thread_id(int64_t t)
  {
    thread_id_ = t;
  }
  void set_valid(const bool valid)
  {
    is_valid_ = valid;
  };
  int set_client_version(const common::ObString& client_version);
  int set_driver_version(const common::ObString& driver_version);
  void set_sql_mode(const ObSQLMode sql_mode, bool is_inc = true)
  {
    // Compatibility mode store in sql_mode_ but controlled by ob_compatibility_mode variable,
    // we can not overwrite it.
    ObSQLMode real_sql_mode =
        (sql_mode & (~ALL_SMO_COMPACT_MODE)) | (sys_vars_cache_.get_sql_mode(is_inc) & ALL_SMO_COMPACT_MODE);
    sys_vars_cache_.set_sql_mode(real_sql_mode, is_inc);
  }
  void set_compatibility_mode(const common::ObCompatibilityMode compat_mode, bool is_inc = true)
  {
    ObSQLMode sql_mode =
        ob_compatibility_mode_to_sql_mode(compat_mode) | (sys_vars_cache_.get_sql_mode(is_inc) & ~ALL_SMO_COMPACT_MODE);
    sys_vars_cache_.set_sql_mode(sql_mode, is_inc);
  }
  int update_safe_weak_read_snapshot(
      const uint64_t tenant_id, const int64_t& safe_weak_read_snapshot, const int64_t& safe_weak_read_snapshot_source);
  void set_global_vars_version(const int64_t modify_time)
  {
    global_vars_version_ = modify_time;
  }
  void set_is_deserialized()
  {
    is_deserialized_ = true;
  }
  bool get_is_deserialized()
  {
    return is_deserialized_;
  }
  void set_exec_min_cluster_version()
  {
    exec_min_cluster_version_ = GET_MIN_CLUSTER_VERSION();
  }
  uint64_t get_exec_min_cluster_version()
  {
    return exec_min_cluster_version_;
  }
  // local sys var getters
  inline ObCollationType get_local_collation_connection() const;
  inline ObCollationType get_nls_collation() const;
  inline ObCollationType get_nls_collation_nation() const;
  inline const ObString& get_ob_trace_info() const;
  bool get_local_autocommit() const;
  uint64_t get_local_auto_increment_increment() const;
  uint64_t get_local_auto_increment_offset() const;
  uint64_t get_local_last_insert_id() const;
  bool get_local_ob_enable_plan_cache() const;
  bool get_local_ob_enable_sql_audit() const;
  ObLengthSemantics get_local_nls_length_semantics() const;
  ObLengthSemantics get_actual_nls_length_semantics() const;
  int64_t get_local_ob_org_cluster_id() const;
  int64_t get_local_timestamp() const;
  const common::ObString get_local_nls_date_format() const;
  const common::ObString get_local_nls_timestamp_format() const;
  const common::ObString get_local_nls_timestamp_tz_format() const;
  const common::ObString* get_local_nls_formats() const;
  int get_local_nls_format(const ObObjType type, ObString& format_str) const;
  int64_t get_safe_weak_read_snapshot() const
  {
    return safe_weak_read_snapshot_;
  }
  int64_t get_safe_weak_read_snapshot_source() const
  {
    return weak_read_snapshot_source_;
  }
  int set_time_zone(const common::ObString& str_val, const bool is_oralce_mode, const bool need_check_valid /* true */);

  // getters
  const common::ObString get_tenant_name() const;
  uint64_t get_priv_tenant_id() const
  {
    return tenant_id_;
  }
  const common::ObString get_effective_tenant_name() const;
  uint64_t get_effective_tenant_id() const
  {
    return effective_tenant_id_;
  }
  // RPC framework use rpc_tenant_id() to deliver remote/distribute tasks.
  void set_rpc_tenant_id(uint64_t tenant_id)
  {
    rpc_tenant_id_ = tenant_id;
  }
  uint64_t get_rpc_tenant_id() const
  {
    return rpc_tenant_id_ ?: GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2210 ? effective_tenant_id_ : tenant_id_;
  }
  uint64_t get_login_tenant_id() const
  {
    return tenant_id_;
  }
  void set_login_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  bool is_tenant_changed() const
  {
    return tenant_id_ != effective_tenant_id_;
  }
  void set_autocommit(bool autocommit);
  int get_autocommit(bool& autocommit) const;
  int get_explicit_defaults_for_timestamp(bool& explicit_defaults_for_timestamp) const;
  int get_sql_auto_is_null(bool& sql_auto_is_null) const;
  int get_is_result_accurate(bool& is_result_accurate) const;
  common::ObConsistencyLevel get_consistency_level() const
  {
    return consistency_level_;
  };
  bool get_in_transaction() const
  {
    return trans_flags_.is_in_transaction();
  }
  bool is_zombie() const
  {
    return SESSION_KILLED == get_session_state();
  }
  bool is_query_killed() const
  {
    return QUERY_KILLED == get_session_state();
  }
  bool is_valid() const
  {
    return is_valid_;
  };
  uint64_t get_user_id() const
  {
    return user_id_;
  }
  bool is_auditor_user() const
  {
    return OB_ORA_AUDITOR_USER_ID == extract_pure_id(user_id_);
  };
  bool is_lbacsys_user() const
  {
    return OB_ORA_LBACSYS_USER_ID == extract_pure_id(user_id_);
  };
  bool is_oracle_sys_user() const
  {
    return OB_ORA_SYS_USER_ID == extract_pure_id(user_id_);
  };
  bool is_mysql_root_user() const
  {
    return OB_SYS_USER_ID == extract_pure_id(user_id_);
  };
  bool is_restore_user() const
  {
    return 0 == thread_data_.user_name_.case_compare(common::OB_RESTORE_USER_NAME);
  };
  bool is_proxy_sys_user() const
  {
    return OB_SYS_TENANT_ID == tenant_id_;
  };
  const common::ObString get_database_name() const;
  inline int get_database_id(uint64_t& db_id) const
  {
    db_id = database_id_;
    return common::OB_SUCCESS;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline void set_database_id(uint64_t db_id)
  {
    database_id_ = db_id;
  }
  inline const ObQueryRetryInfo& get_retry_info() const
  {
    return retry_info_;
  }
  inline ObQueryRetryInfo& get_retry_info_for_update()
  {
    return retry_info_;
  }
  inline const common::ObCurTraceId::TraceId& get_last_query_trace_id() const
  {
    return last_query_trace_id_;
  }
  int check_and_init_retry_info(const common::ObCurTraceId::TraceId& cur_trace_id, const common::ObString& sql);
  void check_and_reset_retry_info(const common::ObCurTraceId::TraceId& cur_trace_id, bool is_packet_retry);
  const common::ObLogIdLevelMap* get_log_id_level_map() const;
  const common::ObString& get_client_version() const
  {
    return client_version_;
  }
  const common::ObString& get_driver_version() const
  {
    return driver_version_;
  }
  transaction::ObTransDesc& get_trans_desc()
  {
    return trans_desc_;
  }
  const transaction::ObTransDesc& get_trans_desc() const
  {
    return trans_desc_;
  }
  TransResult& get_trans_result()
  {
    return trans_result_;
  }
  int32_t get_trans_consistency_type() const
  {
    return trans_consistency_type_;
  }
  void set_trans_consistency_type(int32_t consistency_type)
  {
    trans_consistency_type_ = consistency_type;
  }
  int get_tx_timeout(int64_t& tx_timeout) const;
  int get_query_timeout(int64_t& query_timeout) const;
  int64_t get_trx_lock_timeout() const;
  int get_sql_throttle_current_priority(int64_t& sql_throttle_current_priority);
  int get_ob_last_schema_version(int64_t& ob_last_schema_version);
  int get_pl_block_timeout(int64_t& pl_block_timeout) const;
  int get_binlog_row_image(int64_t& binlog_row_image) const;
  int get_ob_read_consistency(int64_t& ob_read_consistency) const;
  int get_sql_select_limit(int64_t& sql_select_limit) const;
  int get_ob_stmt_parallel_degree(int64_t& ob_stmt_parallel_degree) const;
  int get_ob_stmt_parallel_degree_min_max(
      int64_t& min_ob_stmt_parallel_degree, int64_t& max_ob_stmt_parallel_degree) const;
  // compatible mode of session is used for transmiting mode to switch guard, such as
  // inner sql connection. use lib::is_oracle_mode / lib::is_mysql_mode in other cases.
  // ps: check_compatibility_mode can verify if these two modes is consistent.
  ObCompatibilityMode get_compatibility_mode() const
  {
    return ob_sql_mode_to_compatibility_mode(get_sql_mode());
  }
  bool is_oracle_compatible() const
  {
    return ORACLE_MODE == get_compatibility_mode();
  }
  int check_compatibility_mode() const;
  ObSQLMode get_sql_mode() const
  {
    return sys_vars_cache_.get_sql_mode();
  }
  int get_div_precision_increment(int64_t& div_precision_increment) const;
  int get_character_set_client(common::ObCharsetType& character_set_client) const;
  int get_character_set_connection(common::ObCharsetType& character_set_connection) const;
  int get_character_set_database(common::ObCharsetType& character_set_database) const;
  int get_character_set_results(common::ObCharsetType& character_set_results) const;
  int get_character_set_server(common::ObCharsetType& character_set_server) const;
  int get_character_set_system(common::ObCharsetType& character_set_system) const;
  int get_character_set_filesystem(common::ObCharsetType& character_set_filesystem) const;
  inline int get_collation_connection(common::ObCollationType& collation_connection) const
  {
    collation_connection = get_local_collation_connection();
    return common::OB_SUCCESS;
  }
  int get_collation_database(common::ObCollationType& collation_database) const;
  int get_collation_server(common::ObCollationType& collation_server) const;
  int get_foreign_key_checks(int64_t& foreign_key_checks) const;
  int get_capture_plan_baseline(bool& v) const;
  int get_use_plan_baseline(bool& v) const;
  int get_adaptive_cursor_sharing(bool& acs) const;
  int get_nlj_batching_enabled(bool& v) const;
  int get_enable_parallel_dml(bool& v) const;
  int get_enable_parallel_query(bool& v) const;
  int get_force_parallel_query_dop(uint64_t& v) const;
  int get_force_parallel_dml_dop(uint64_t& v) const;
  int get_secure_file_priv(common::ObString& v) const;
  int get_sql_safe_updates(bool& v) const;
  int update_timezone_info();
  const common::ObTimeZoneInfo* get_timezone_info() const
  {
    return tz_info_wrap_.get_time_zone_info();
  }
  const common::ObTimeZoneInfoWrap& get_tz_info_wrap() const
  {
    return tz_info_wrap_;
  }
  inline int set_tz_info_wrap(const common::ObTimeZoneInfoWrap& other)
  {
    return tz_info_wrap_.deep_copy(other);
  }
  inline void set_nls_formats(const common::ObString* nls_formats, bool is_inc = true)
  {
    sys_vars_cache_.set_nls_date_format(nls_formats[ObNLSFormatEnum::NLS_DATE], is_inc);
    sys_vars_cache_.set_nls_timestamp_format(nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP], is_inc);
    sys_vars_cache_.set_nls_timestamp_tz_format(nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP_TZ], is_inc);
  }
  int get_influence_plan_sys_var(ObSysVarInPC& sys_vars) const;
  const common::ObString& get_sys_var_in_pc_str() const
  {
    return sys_var_in_pc_str_;
  }
  int gen_sys_var_in_pc_str();
  uint32_t get_sessid() const
  {
    return sessid_;
  }
  uint64_t get_proxy_sessid() const
  {
    return proxy_sessid_;
  }
  // for temp table or creating table
  uint64_t get_sessid_for_table() const
  {
    return is_obproxy_mode() ? get_proxy_sessid() : (is_master_session() ? get_sessid() : get_master_sessid());
  }
  uint32_t get_master_sessid() const
  {
    return master_sessid_;
  }
  common::ObString get_ssl_cipher() const
  {
    return ObString::make_string(ssl_cipher_buff_);
  }
  void set_ssl_cipher(const char* value)
  {
    const size_t min_len = std::min(sizeof(ssl_cipher_buff_) - 1, strlen(value));
    MEMCPY(ssl_cipher_buff_, value, min_len);
    ssl_cipher_buff_[min_len] = '\0';
  }
  // Master session: receiving user SQL text sessions.
  // Slave session: receiving sql plan sessions, e.g.: remote executing,
  // distribute executing sessions.
  bool is_master_session() const
  {
    return INVALID_SESSID == master_sessid_;
  }
  common::ObDSSessionActions& get_debug_sync_actions()
  {
    return debug_sync_actions_;
  }
  int64_t get_global_vars_version() const
  {
    return global_vars_version_;
  }
  int64_t get_sys_var_base_version() const
  {
    return sys_var_base_version_;
  }
  inline common::ObIArray<int64_t>& get_influence_plan_var_indexs()
  {
    return influence_plan_var_indexs_;
  }
  int64_t get_influence_plan_var_count() const
  {
    return influence_plan_var_indexs_.count();
  }
  int get_pc_mem_conf(ObPCMemPctConf& pc_mem_conf);

  int get_sql_audit_mem_conf(int64_t& mem_pct);

  /// @{ thread_data_ related: }
  int set_user(const common::ObString& user_name, const common::ObString& host_name, const uint64_t user_id);
  int set_real_client_ip(const common::ObString& client_ip);
  const common::ObString& get_user_name() const
  {
    return thread_data_.user_name_;
  }
  const common::ObString& get_host_name() const
  {
    return thread_data_.host_name_;
  }
  const common::ObString& get_client_ip() const
  {
    return thread_data_.client_ip_;
  }
  const common::ObString& get_user_at_host() const
  {
    return thread_data_.user_at_host_name_;
  }
  const common::ObString& get_user_at_client_ip() const
  {
    return thread_data_.user_at_client_ip_;
  }
  void set_conn(easy_connection_t* conn);
  easy_connection_t* get_conn() const
  {
    return thread_data_.conn_;
  }

  void set_peer_addr(common::ObAddr peer_addr)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.peer_addr_ = peer_addr;
  }
  const common::ObAddr& get_peer_addr() const
  {
    return thread_data_.peer_addr_;
  }
  const common::ObAddr& get_user_client_addr() const
  {
    return thread_data_.user_client_addr_;
  }
  common::ObAddr get_proxy_addr() const
  {
    const int32_t ip = static_cast<int32_t>((proxy_sessid_ >> 32) & 0xFFFFFFFF);
    const int32_t port = static_cast<int32_t>((proxy_sessid_ >> 16) & 0xFFFF);
    return ObAddr(ip, port);
  }
  void set_query_start_time(int64_t time)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.cur_query_start_time_ = time;
  }
  int64_t get_query_start_time() const
  {
    return thread_data_.cur_query_start_time_;
  }
  int64_t get_cur_state_start_time() const
  {
    return thread_data_.cur_state_start_time_;
  }
  void set_interactive(bool is_interactive)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.is_interactive_ = is_interactive;
  }
  bool get_interactive() const
  {
    return thread_data_.is_interactive_;
  }
  void update_last_active_time()
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.last_active_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  }
  int64_t get_last_active_time() const
  {
    return thread_data_.last_active_time_;
  }
  int set_session_state(ObSQLSessionState state);
  ObSQLSessionState get_session_state() const
  {
    return thread_data_.state_;
  }
  const char* get_session_state_str() const;
  void set_session_data(ObSQLSessionState state, obmysql::ObMySQLCmd mysql_cmd, int64_t thread_id);
  void set_mysql_cmd(obmysql::ObMySQLCmd mysql_cmd)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.mysql_cmd_ = mysql_cmd;
  }
  void set_session_in_retry(ObSessionRetryStatus is_retry)
  {
    LockGuard lock_guard(thread_data_mutex_);
    if (OB_LIKELY(SESS_NOT_IN_RETRY == is_retry ||
                  SESS_IN_RETRY_FOR_DUP_TBL != thread_data_.is_in_retry_)) {
      thread_data_.is_in_retry_ = is_retry;
    } else {
      // if the last retry is for duplicate table
      // and the SQL is retried again
      // we still keep the retry for dup table status.
      thread_data_.is_in_retry_ = SESS_IN_RETRY_FOR_DUP_TBL;
    }
  }

  void set_session_in_retry(bool is_retry, int ret)
  {
    ObSessionRetryStatus status;
    if (!is_retry) {
      status = sql::SESS_NOT_IN_RETRY;
    } else if (is_select_dup_follow_replic_err(ret)) {
      status = SESS_IN_RETRY_FOR_DUP_TBL;
    } else {
      status = SESS_IN_RETRY;
    }
    set_session_in_retry(status);
  }
  bool get_is_in_retry()
  {
    return SESS_NOT_IN_RETRY != thread_data_.is_in_retry_;
  }
  bool get_is_in_retry_for_dup_tbl()
  {
    return SESS_IN_RETRY_FOR_DUP_TBL == thread_data_.is_in_retry_;
  }
  obmysql::ObMySQLCmd get_mysql_cmd() const
  {
    return thread_data_.mysql_cmd_;
  }
  char const* get_mysql_cmd_str() const
  {
    return obmysql::get_mysql_cmd_str(thread_data_.mysql_cmd_);
  }
  int store_query_string(const common::ObString& stmt);
  void reset_query_string();
  int set_session_active(const ObString& sql, const int64_t query_receive_ts, const int64_t last_active_time_ts,
      obmysql::ObMySQLCmd cmd = obmysql::ObMySQLCmd::OB_MYSQL_COM_QUERY);
  const common::ObString get_current_query_string() const;
  uint64_t get_current_statement_id() const
  {
    return thread_data_.cur_statement_id_;
  }
  int update_session_timeout();
  int is_timeout(bool& is_timeout);
  int is_trx_idle_timeout(bool& is_timeout);
  int64_t get_wait_timeout()
  {
    return thread_data_.wait_timeout_;
  }
  int64_t get_interactive_timeout()
  {
    return thread_data_.interactive_timeout_;
  }
  int64_t get_max_packet_size()
  {
    return thread_data_.max_packet_size_;
  }
  // lock
  common::ObRecursiveMutex& get_query_lock()
  {
    return query_mutex_;
  }
  common::ObRecursiveMutex& get_thread_data_lock()
  {
    return thread_data_mutex_;
  }
  int try_lock_query()
  {
    return query_mutex_.trylock();
  }
  int try_lock_thread_data()
  {
    return thread_data_mutex_.trylock();
  }
  int unlock_query()
  {
    return query_mutex_.unlock();
  }
  int unlock_thread_data()
  {
    return thread_data_mutex_.unlock();
  }
  /// @{ system variables related:
  static int get_global_sys_variable(const ObBasicSessionInfo* session, common::ObIAllocator& calc_buf,
      const common::ObString& var_name, common::ObObj& val);
  static int get_global_sys_variable(uint64_t actual_tenant_id, common::ObIAllocator& calc_buf,
      const common::ObDataTypeCastParams& dtc_params, const common::ObString& var_name, common::ObObj& val);
  const share::ObBasicSysVar* get_sys_var(const int64_t idx) const;
  int64_t get_sys_var_count() const
  {
    return share::ObSysVarFactory::ALL_SYS_VARS_COUNT;
  }
  // load default value for system variables.
  int init_system_variables(const bool print_info_log, const bool is_sys_tenant);
  int load_default_sys_variable(const bool print_info_log, const bool is_sys_tenant);
  int update_query_sensitive_system_variable(share::schema::ObSchemaGetterGuard& schema_guard);
  int process_variable_for_tenant(const common::ObString& var, common::ObObj& val);
  int load_sys_variable(const common::ObString& name, const common::ObObj& type, const common::ObObj& value,
      const common::ObObj& min_val, const common::ObObj& max_val, const int64_t flags, bool is_from_sys_table = false);
  int load_sys_variable(const common::ObString& name, const int64_t dtype, const common::ObString& value,
      const common::ObString& min_val, const common::ObString& max_val, const int64_t flags,
      bool is_from_sys_table = false);
  // cast string values of system variables to their real type.
  int cast_sys_variable(bool is_range_value, const share::ObSysVarClassType sys_var_id, const common::ObObj& type,
      const common::ObObj& value, int64_t flags, common::ObObj& out_type, common::ObObj& out_value);

  int update_sys_variable(const common::ObString& var, const common::ObString& val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, const common::ObObj& val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, const common::ObString& val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, int64_t val);
  /// @note get system variables by id is prefered
  int update_sys_variable_by_name(const common::ObString& var, const common::ObObj& val);
  int update_sys_variable_by_name(const common::ObString& var, int64_t val);
  // int update_sys_variable(const char* const var, const common::ObString &v);
  ///@}

  int get_sys_variable(const share::ObSysVarClassType sys_var_id, common::ObObj& val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, common::ObString& val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, int64_t& val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, uint64_t& val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, share::ObBasicSysVar*& val) const;
  /// @note get system variables by id is prefered
  int get_sys_variable_by_name(const common::ObString& var, common::ObObj& val) const;
  int get_sys_variable_by_name(const common::ObString& var, share::ObBasicSysVar*& val) const;
  int get_sys_variable_by_name(const common::ObString& var, int64_t& val) const;
  ///@}

  /// check the existence of the system variable
  int sys_variable_exists(const common::ObString& var, bool& is_exist) const;

  // performance optimization for session serialization.
  typedef common::ObSEArray<share::ObSysVarClassType, 64> SysVarIds;
  class SysVarIncInfo {
    OB_UNIS_VERSION_V(1);

  public:
    SysVarIncInfo();
    virtual ~SysVarIncInfo();
    int add_sys_var_id(share::ObSysVarClassType sys_var_id);
    int get_des_sys_var_id(int64_t idx, share::ObSysVarClassType& sys_var_id) const;
    bool all_has_sys_var_id(share::ObSysVarClassType sys_var_id) const;
    int64_t all_count() const;
    const SysVarIds& get_all_sys_var_ids() const;
    int assign(const SysVarIncInfo& other);
    int reset();
    TO_STRING_KV(K(all_sys_var_ids_));

  private:
    SysVarIds all_sys_var_ids_;
  };
  int load_all_sys_vars_default();
  int load_all_sys_vars(share::schema::ObSchemaGetterGuard& schema_guard);
  int load_all_sys_vars(const share::schema::ObSysVariableSchema& sys_var_schema);
  int clean_all_sys_vars();
  SysVarIncInfo sys_var_inc_info_;

  // current executing physical plan
  ObPhysicalPlan* get_cur_phy_plan() const;
  void get_cur_sql_id(char *sql_id_buf, int64_t sql_id_buf_size) const;
  int set_cur_phy_plan(ObPhysicalPlan *cur_phy_plan);
  void reset_cur_phy_plan_to_null();

  common::ObObjType get_sys_variable_type(const common::ObString& var_name) const;
  common::ObObjType get_sys_variable_meta_type(const common::ObString& var_name) const;
  int if_aggr_pushdown_allowed(bool& aggr_pushdown_allowed) const;
  int is_transformation_enabled(bool& transformation_enabled) const;
  int is_use_trace_log(bool& use_trace_log) const;
  int is_use_transmission_checksum(bool& use_transmission_checksum) const;
  int is_select_index_enabled(bool& select_index_enabled) const;
  int get_name_case_mode(common::ObNameCaseMode& case_mode) const;
  int is_create_table_strict_mode(bool& strict_mode) const;
  int64_t get_variables_last_modify_time() const
  {
    return variables_last_modify_time_;
  }
  int get_init_connect(common::ObString& str) const;

  ///@{ user variables related:
  sql::ObSessionValMap& get_user_var_val_map()
  {
    return user_var_val_map_;
  }
  const sql::ObSessionValMap& get_user_var_val_map() const
  {
    return user_var_val_map_;
  }
  int replace_user_variable(const common::ObString& var, const ObSessionVariable& val);
  int replace_user_variables(const ObSessionValMap& user_var_map);
  int remove_user_variable(const common::ObString& var);
  int get_user_variable(const common::ObString& var, ObSessionVariable& val) const;
  int get_user_variable_value(const common::ObString& var, common::ObObj& val) const;
  int get_user_variable_meta(const common::ObString& var, common::ObObjMeta& meta) const;
  const ObSessionVariable* get_user_variable(const common::ObString& var) const;
  const common::ObObj* get_user_variable_value(const common::ObString& var) const;
  bool user_variable_exists(const common::ObString& var) const;
  /// @}

  inline static ObDataTypeCastParams create_dtc_params(const ObBasicSessionInfo* session_info)
  {
    return OB_NOT_NULL(session_info) ? session_info->get_dtc_params() : ObDataTypeCastParams();
  }

  inline ObDataTypeCastParams get_dtc_params() const
  {
    return ObDataTypeCastParams(get_timezone_info(),
        get_local_nls_formats(),
        get_nls_collation(),
        get_nls_collation_nation(),
        get_local_collation_connection());
  }

  inline ObSessionNLSParams get_session_nls_params() const
  {
    ObSessionNLSParams session_nls_params;
    session_nls_params.nls_length_semantics_ = get_actual_nls_length_semantics();
    session_nls_params.nls_collation_ = get_nls_collation();
    session_nls_params.nls_nation_collation_ = get_nls_collation_nation();
    return session_nls_params;
  }

  inline ObObjPrintParams create_obj_print_params() const
  {
    ObObjPrintParams res(get_timezone_info(), get_local_collation_connection());
    if (stmt::T_SHOW_CREATE_VIEW == stmt_type_) {
      res.is_show_create_view_ = true;
    }
    return res;
  }

  // client mode related
  void set_client_mode(const common::ObClientMode mode)
  {
    client_mode_ = mode;
  }
  bool is_java_client_mode() const
  {
    return common::OB_JAVA_CLIENT_MODE == client_mode_;
  }
  bool is_obproxy_mode() const
  {
    return common::OB_PROXY_CLIENT_MODE == client_mode_;
  }

  int64_t to_string(char* buffer, const int64_t length) const;

  static const char* source_to_string(const int64_t type)
  {
    const char* res = "UNKOWN";
    switch (type) {
      case UNKOWN:
        res = "UNKOWN";
        break;
      case AUTO_PLAN:
        res = "AUTO_PLAN";
        break;
      case PROXY_INPUT:
        res = "PROXY_INPUT";
        break;
      case LAST_STMT:
        res = "LAST_STMT";
        break;
      default:
        res = "UNKOWN";
        break;
    }
    return res;
  }

  /// @{ TRACE_SESSION_INFO related:
  struct ChangedVar {
    ChangedVar() : id_(), old_val_()
    {}
    ChangedVar(share::ObSysVarClassType id, const ObObj& val) : id_(id), old_val_(val)
    {}
    share::ObSysVarClassType id_;
    ObObj old_val_;
    TO_STRING_KV(K(id_), K(old_val_));
  };
  void reset_session_changed_info();
  bool is_already_tracked(const share::ObSysVarClassType& sys_var_id, const common::ObIArray<ChangedVar>& array) const;
  bool is_already_tracked(const common::ObString& name, const common::ObIArray<common::ObString>& array) const;
  int add_changed_sys_var(
      const share::ObSysVarClassType& sys_var_id, const common::ObObj& old_val, common::ObIArray<ChangedVar>& array);
  int add_changed_user_var(const common::ObString& name, common::ObIArray<common::ObString>& array);
  int track_sys_var(const share::ObSysVarClassType& sys_var_id, const common::ObObj& old_val);
  int track_user_var(const common::ObString& user_var);
  int is_sys_var_actully_changed(
      const share::ObSysVarClassType& sys_var_id, const common::ObObj& old_val, common::ObObj& new_val, bool& changed);
  inline bool is_sys_var_changed() const
  {
    return !changed_sys_vars_.empty();
  }
  inline bool is_user_var_changed() const
  {
    return !changed_user_vars_.empty();
  }
  inline bool is_database_changed() const
  {
    return is_database_changed_;
  }
  inline bool exist_client_feedback() const
  {
    return !feedback_manager_.is_empty();
  }
  inline bool is_session_var_changed() const
  {
    return (is_sys_var_changed() || is_user_var_changed() || exist_client_feedback());
  }
  inline bool is_session_info_changed() const
  {
    return (is_session_var_changed() || is_database_changed());
  }
  const inline common::ObIArray<ChangedVar>& get_changed_sys_var() const
  {
    return changed_sys_vars_;
  }
  const inline common::ObIArray<common::ObString>& get_changed_user_var() const
  {
    return changed_user_vars_;
  }

  inline void set_capability(const obmysql::ObMySQLCapabilityFlags cap)
  {
    capability_ = cap;
  }
  inline obmysql::ObMySQLCapabilityFlags get_capability() const
  {
    return capability_;
  }
  inline bool is_track_session_info() const
  {
    return capability_.cap_flags_.OB_CLIENT_SESSION_TRACK;
  }

  inline bool is_client_return_rowid() const
  {
    return capability_.cap_flags_.OB_CLIENT_RETURN_HIDDEN_ROWID;
  }

  inline bool is_client_use_lob_locator() const
  {
    return capability_.cap_flags_.OB_CLIENT_USE_LOB_LOCATOR;
  }

  void set_proxy_cap_flags(const obmysql::ObProxyCapabilityFlags& proxy_capability)
  {
    proxy_capability_ = proxy_capability;
  }
  obmysql::ObProxyCapabilityFlags get_proxy_cap_flags() const
  {
    return proxy_capability_;
  }
  inline bool is_abundant_feedback_support() const
  {
    return is_track_session_info() && proxy_capability_.is_abundant_feedback_support();
  }

  // TODO: as enable_transmission_checksum is global variables,
  // here we no need get_session for is_enable_transmission_checksum()
  inline bool is_enable_transmission_checksum() const
  {
    return true;
  }

  inline share::ObFeedbackManager& get_feedback_manager()
  {
    return feedback_manager_;
  }
  inline int set_follower_first_feedback(const share::ObFollowerFirstFeedbackType type);

  inline common::ObIAllocator& get_allocator()
  {
    return changed_var_pool_;
  }
  inline common::ObIAllocator& get_session_allocator()
  {
    return block_allocator_;
  }
  int set_partition_hit(const bool is_hit);
  int set_proxy_user_privilege(const int64_t user_priv_set);
  int set_proxy_capability(const uint64_t proxy_cap);
  int set_trans_specified(const bool is_spec);
  int set_safe_weak_read_snapshot_variable(const int64_t safe_snapshot);
  int set_init_connect(const common::ObString& init_sql);
  int save_trans_status();
  void reset_tx_variable();
  int32_t get_tx_isolation() const;
  bool is_isolation_serializable() const;
  void set_tx_isolation(int32_t isolation);
  bool get_tx_read_only() const;
  void set_tx_read_only(bool read_only);
  int reset_tx_variable_if_remote_trans(const ObPhyPlanType& type);
  int check_tx_read_only_privilege(const ObSqlTraits& sql_traits);
  int get_group_concat_max_len(uint64_t& group_concat_max_len) const;
  int get_ob_interm_result_mem_limit(int64_t& ob_interm_result_mem_limit) const;
  int get_ob_org_cluster_id(int64_t& ob_org_cluster_id) const;
  int get_max_allowed_packet(int64_t& max_allowed_pkt) const;
  int get_net_buffer_length(int64_t& net_buffer_len) const;
  /// @}
  int64_t get_session_info_mem_size() const { return block_allocator_.get_total_mem_size(); }
  int64_t get_sys_var_mem_size() const { return base_sys_var_alloc_.total(); }
  // no relationship with function set_partition_hit(const bool is_hit) above.
  ObPartitionHitInfo &partition_hit() { return partition_hit_; }
  bool get_err_final_partition_hit(int err_ret)
  {
    bool is_partition_hit = partition_hit().get_bool();
    if (is_proxy_refresh_location_ret(err_ret)) {
      is_partition_hit = false;
    } else if (get_is_in_retry()
               && is_proxy_refresh_location_ret(retry_info_.get_last_query_retry_err())) {
      is_partition_hit = false;
    }
    return is_partition_hit;
  };
  bool is_proxy_refresh_location_ret(int err_ret) {
    return common::OB_NOT_MASTER == err_ret;
  }
  void set_shadow(bool is_shadow) { ATOMIC_STORE(&thread_data_.is_shadow_, is_shadow); }
  bool is_shadow() { return ATOMIC_LOAD(&thread_data_.is_shadow_);  }
  uint32_t get_version() const {return version_;}
  uint32_t get_magic_num() {return magic_num_;}
  int64_t get_current_execution_id() const { return current_execution_id_; }
  void set_current_execution_id(int64_t execution_id) { current_execution_id_ = execution_id; }
  const common::ObCurTraceId::TraceId &get_last_trace_id() const { return last_trace_id_; }
  void set_last_trace_id(common::ObCurTraceId::TraceId *trace_id)
  {
    if (OB_NOT_NULL(trace_id)) { last_trace_id_ = *trace_id; }
  }
  const common::ObCurTraceId::TraceId &get_current_trace_id() const { return curr_trace_id_; }
  void set_current_trace_id(common::ObCurTraceId::TraceId *trace_id)
  {
    if (OB_NOT_NULL(trace_id)) { curr_trace_id_ = *trace_id; }
  }
  const ObString& get_app_trace_id() const
  {
    return app_trace_id_;
  }
  void set_app_trace_id(common::ObString trace_id)
  {
    app_trace_id_.assign_ptr(trace_id.ptr(), trace_id.length());
  }
  // update trace_id in sys variables and  will bing to client
  int update_last_trace_id(const ObCurTraceId::TraceId& trace_id);

  inline bool is_debug_mode() const
  {
    return true;
  }
  int set_partition_location_feedback(const share::ObFBPartitionParam& param);
  int get_auto_increment_cache_size(int64_t& auto_increment_cache_size);
  void set_curr_trans_last_stmt_end_time(int64_t t)
  {
    curr_trans_last_stmt_end_time_ = t;
  }
  int64_t get_curr_trans_last_stmt_end_time() const
  {
    return curr_trans_last_stmt_end_time_;
  }

  int use_parallel_execution(bool& v) const;

  // nested session and sql execute for foreign key.
  bool is_nested_session() const
  {
    return nested_count_ > 0;
  }
  int64_t get_nested_count() const
  {
    return nested_count_;
  }
  void set_nested_count(int64_t nested_count)
  {
    nested_count_ = nested_count;
  }
  int save_base_session(BaseSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int restore_base_session(BaseSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int save_basic_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int restore_basic_session(StmtSavedValue& saved_value);
  int begin_nested_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int end_nested_session(StmtSavedValue& saved_value);
  int begin_autonomous_session(TransSavedValue& saved_value);
  int end_autonomous_session(TransSavedValue& saved_value);
  int set_cur_stmt_tables(const common::ObIArray<ObPartitionKey>& stmt_partitions, stmt::StmtType stmt_type);
  int set_start_stmt();
  int set_end_stmt();
  bool has_start_stmt()
  {
    return nested_count_ >= 0;
  }
  bool is_fast_select() const
  {
    return trans_desc_.is_fast_select();
  }
  bool is_standalone_stmt() const
  {
    return trans_desc_.get_standalone_stmt_desc().is_valid();
  }

  bool is_server_status_in_transaction() const;

  int64_t get_read_snapshot_version()
  {
    return read_snapshot_version_;
  }
  void set_read_snapshot_version(int64_t read_snapshot_version)
  {
    read_snapshot_version_ = read_snapshot_version;
  }
  bool has_valid_read_snapshot_version() const
  {
    return read_snapshot_version_ > 0;
  }

  void set_in_transaction(bool value)
  {
    trans_flags_.set_is_in_transaction(value);
  }
  void set_has_exec_write_stmt(bool value)
  {
    trans_flags_.set_has_exec_write_stmt(value);
  }
  void set_has_set_trans_var(bool value)
  {
    trans_flags_.set_has_set_trans_var(value);
  }
  void set_has_any_dml_succ(bool value)
  {
    trans_flags_.set_has_any_dml_succ(value);
  }
  void set_has_any_pl_succ(bool value)
  {
    trans_flags_.set_has_any_pl_succ(value);
  }
  void set_has_remote_plan_in_tx(bool value)
  {
    trans_flags_.set_has_remote_plan(value);
  }
  void set_has_inner_dml_write(bool value)
  {
    trans_flags_.set_has_inner_dml_write(value);
  }
  void set_need_serial_exec(bool value)
  {
    trans_flags_.set_need_serial_exec(value);
  }
  bool is_in_transaction() const
  {
    return trans_flags_.is_in_transaction();
  }
  bool has_exec_write_stmt() const
  {
    return trans_flags_.has_exec_write_stmt();
  }
  bool has_set_trans_var() const
  {
    return trans_flags_.has_set_trans_var();
  }
  bool has_hold_row_lock() const
  {
    return trans_flags_.has_hold_row_lock();
  }
  bool has_any_dml_succ() const
  {
    return trans_flags_.has_any_dml_succ();
  }
  bool has_any_pl_succ() const
  {
    return trans_flags_.has_any_pl_succ();
  }
  bool has_remote_plan_in_tx() const
  {
    return trans_flags_.has_remote_plan();
  }
  bool has_inner_dml_write() const
  {
    return trans_flags_.has_inner_dml_write();
  }
  bool need_serial_exec() const
  {
    return trans_flags_.need_serial_exec();
  }
  uint64_t get_trans_flags() const
  {
    return trans_flags_.get_flags();
  }

  bool get_check_sys_variable()
  {
    return check_sys_variable_;
  }
  void set_check_sys_variable(bool check_sys_variable)
  {
    check_sys_variable_ = check_sys_variable;
  }
  bool is_acquire_from_pool() const
  {
    return acquire_from_pool_;
  }
  void set_acquire_from_pool(bool acquire_from_pool)
  {
    acquire_from_pool_ = acquire_from_pool;
  }
  bool can_release_to_pool() const
  {
    return release_to_pool_;
  }
  void set_release_from_pool(bool release_to_pool)
  {
    release_to_pool_ = release_to_pool;
  }
  bool is_tenant_killed()
  {
    return ATOMIC_LOAD(&is_tenant_killed_) > 0;
  }
  void set_tenant_killed()
  {
    ATOMIC_STORE(&is_tenant_killed_, 1);
  }
  bool is_use_inner_allocator() const;
  int64_t get_reused_count() const
  {
    return reused_count_;
  }
  bool is_foreign_key_cascade() const
  {
    return is_foreign_key_cascade_;
  }
  void set_foreign_key_casecade(bool value)
  {
    is_foreign_key_cascade_ = value;
  }
  bool is_foreign_key_check_exist() const
  {
    return is_foreign_key_check_exist_;
  }
  void set_foreign_key_check_exist(bool value)
  {
    is_foreign_key_check_exist_ = value;
  }
  bool reuse_cur_sql_no() const
  {
    return is_foreign_key_cascade() || is_foreign_key_check_exist();
  }
  inline void set_first_stmt_type(stmt::StmtType stmt_type)
  {
    if (stmt::T_NONE == first_stmt_type_) {
      first_stmt_type_ = stmt_type;
    }
  }
  inline void reset_first_stmt_type()
  {
    first_stmt_type_ = stmt::T_NONE;
  }
  inline stmt::StmtType get_first_stmt_type() const
  {
    return first_stmt_type_;
  }
  void set_stmt_type(stmt::StmtType stmt_type)
  {
    stmt_type_ = stmt_type;
  }
  stmt::StmtType get_stmt_type() const
  {
    return stmt_type_;
  }

  bool is_password_expired() const
  {
    return is_password_expired_;
  }
  void set_password_expired(bool value)
  {
    is_password_expired_ = value;
  }
  int load_default_sys_variable(int64_t var_idx);

  int set_session_temp_table_used(const bool is_used);
  int get_session_temp_table_used(bool& is_used) const;

protected:
  int process_session_variable(
      share::ObSysVarClassType var, const common::ObObj& value, bool is_inc, const bool check_timezone_valid = true);
  int process_session_variable_fast();
  //@brief process session log_level setting like 'all.*:info, sql.*:debug'.
  // int process_session_ob_binlog_row_image(const common::ObObj &value);
  int process_session_log_level(const common::ObObj& val);
  int process_session_sql_mode_value(const common::ObObj& value, bool is_inc);
  int process_session_compatibility_mode_value(const ObObj& value, bool is_inc);
  int process_session_time_zone_value(const common::ObObj& value, const bool check_timezone_valid);
  int process_session_overlap_time_value(const ObObj& value);
  int process_session_autocommit_value(const common::ObObj& val);
  int process_session_debug_sync(const common::ObObj& val, const bool is_global);
  // switch to nested stmt or autonomous transaction.
  int base_save_session(BaseSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int base_restore_session(BaseSavedValue& saved_value);
  int stmt_save_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int stmt_restore_session(StmtSavedValue& saved_value);
  int trans_save_session(TransSavedValue& saved_value);
  int trans_restore_session(TransSavedValue& saved_value);
  // ^^^^^^

protected:
  // because the OB_MALLOC_NORMAL_BLOCK_SIZE block in ObPool has a BlockHeader(8 Bytes),
  // so the total mem avail is (OB_MALLOC_NORMAL_BLOCK_SIZE - 8), if we divide it by (4 * 1204),
  // the last (4 * 1024 - 8) is wasted.
  // and if we divide it by (4 * 1024 - 8), almost all mem can be used.
  static const int64_t SMALL_BLOCK_SIZE = 4 * 1024LL - 8;

private:
  // these reset_xxx() functions below should be called inner only, you should
  // call reset_tx_variable() when you need reset a session object.
  void reset_tx_read_only();
  void reset_tx_isolation();
  void reset_trans_flags()
  {
    trans_flags_.reset();
  }
  void clear_trans_consistency_type()
  {
    trans_consistency_type_ = transaction::ObTransConsistencyType::UNKNOWN;
  }
  void clear_app_trace_id()
  {
    app_trace_id_.reset();
  }
  //***************

  static int change_value_for_special_sys_var(
      const common::ObString& sys_var_name, const common::ObObj& ori_val, common::ObObj& new_val);
  static int change_value_for_special_sys_var(
      const share::ObSysVarClassType sys_var_id, const common::ObObj& ori_val, common::ObObj& new_val);

  int get_int64_sys_var(const share::ObSysVarClassType sys_var_id, int64_t& int64_val) const;
  int get_uint64_sys_var(const share::ObSysVarClassType sys_var_id, uint64_t& uint64_val) const;
  int get_bool_sys_var(const share::ObSysVarClassType sys_var_id, bool& bool_val) const;
  int get_charset_sys_var(share::ObSysVarClassType sys_var_id, common::ObCharsetType& cs_type) const;
  int get_collation_sys_var(share::ObSysVarClassType sys_var_id, common::ObCollationType& coll_type) const;
  int get_string_sys_var(share::ObSysVarClassType sys_var_id, common::ObString& str) const;
  int create_sys_var(share::ObSysVarClassType sys_var_id, int64_t store_idx, share::ObBasicSysVar*& sys_var);
  //  int store_sys_var(int64_t store_idx, share::ObBasicSysVar *sys_var);
  int inner_get_sys_var(const common::ObString& sys_var_name, share::ObBasicSysVar*& sys_var) const;
  int inner_get_sys_var(const share::ObSysVarClassType sys_var_id, share::ObBasicSysVar*& sys_var) const;
  int update_session_sys_variable(ObExecContext& ctx, const common::ObString& var, const common::ObObj& val);
  int calc_need_serialize_vars(common::ObIArray<share::ObSysVarClassType>& sys_var_ids,
      common::ObIArray<common::ObString>& user_var_names, int64_t ser_version) const;
  int deep_copy_sys_variable(
      const share::ObSysVarClassType sys_var_id, const common::ObObj& src_val, common::ObObj* dest_val_ptr);
  int deep_copy_trace_id_var(const common::ObObj& src_val, common::ObObj* dest_val_ptr);
  inline int store_query_string_(const ObString& stmt);
  inline int set_session_state_(ObSQLSessionState state);

protected:
  // the following members need concurrency control
  struct MultiThreadData {
    const static int64_t DEFAULT_MAX_PACKET_SIZE = 1048576;
    MultiThreadData()
        : user_name_(),
          host_name_(),
          client_ip_(),
          user_at_host_name_(),
          user_at_client_ip_(),
          peer_addr_(),
          user_client_addr_(),
          cur_query_buf_len_(0),
          cur_query_(nullptr),
          cur_query_len_(0),
          cur_statement_id_(0),
          last_active_time_(0),
          state_(SESSION_SLEEP),
          is_interactive_(false),
          conn_(NULL),
          mysql_cmd_(obmysql::OB_MYSQL_COM_SLEEP),
          cur_query_start_time_(0),
          cur_state_start_time_(0),
          wait_timeout_(0),
          interactive_timeout_(0),
          max_packet_size_(MultiThreadData::DEFAULT_MAX_PACKET_SIZE),
          is_shadow_(false),
          is_in_retry_(SESS_NOT_IN_RETRY)
    {
      CHAR_CARRAY_INIT(database_name_);
    }
    void reset(bool begin_nested_session = false)
    {
      if (!begin_nested_session) {
        // TODO: move some thing here.
      }
      user_name_.reset();
      host_name_.reset();
      client_ip_.reset();
      user_at_host_name_.reset();
      user_at_client_ip_.reset();
      CHAR_CARRAY_INIT(database_name_);
      peer_addr_.reset();
      user_client_addr_.reset();
      if (cur_query_ != nullptr) {
        cur_query_[0] = '\0';
      }
      cur_query_len_ = 0;
      cur_statement_id_ = 0;
      last_active_time_ = 0;
      state_ = SESSION_SLEEP;
      is_interactive_ = false;
      conn_ = NULL;
      mysql_cmd_ = obmysql::OB_MYSQL_COM_SLEEP;
      cur_query_start_time_ = 0;
      cur_state_start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      wait_timeout_ = 0;
      interactive_timeout_ = 0;
      max_packet_size_ = MultiThreadData::DEFAULT_MAX_PACKET_SIZE;
      is_shadow_ = false;
      is_in_retry_ = SESS_NOT_IN_RETRY;
    }
    ~MultiThreadData()
    {}
    common::ObString user_name_;          // current user name
    common::ObString host_name_;          // current user host name
    common::ObString client_ip_;          // current user real client host name
    common::ObString user_at_host_name_;  // current user@host, for current_user()
    common::ObString user_at_client_ip_;  // current user@clientip, for user()
    char database_name_[common::OB_MAX_DATABASE_NAME_BUF_LENGTH * OB_MAX_CHAR_LEN];  // default database
    common::ObAddr peer_addr_;
    common::ObAddr user_client_addr_;
    int64_t cur_query_buf_len_;
    char* cur_query_;
    volatile int64_t cur_query_len_;
    uint64_t cur_statement_id_;
    int64_t last_active_time_;
    ObSQLSessionState state_;
    bool is_interactive_;
    easy_connection_t* conn_;  // connection of this session
    obmysql::ObMySQLCmd mysql_cmd_;
    int64_t cur_query_start_time_;
    int64_t cur_state_start_time_;
    int64_t wait_timeout_;
    int64_t interactive_timeout_;
    int64_t max_packet_size_;
    bool is_shadow_;
    ObSessionRetryStatus is_in_retry_;
  };

private:
  // cache some system variables for performance.
  class SysVarsCacheData {
    OB_UNIS_VERSION_V(1);

  public:
    SysVarsCacheData()
        : auto_increment_increment_(0),
          sql_throttle_current_priority_(100),
          ob_last_schema_version_(0),
          sql_select_limit_(0),
          auto_increment_offset_(0),
          last_insert_id_(0),
          binlog_row_image_(2),
          foreign_key_checks_(0),
          tx_read_only_(false),
          ob_enable_plan_cache_(false),
          optimizer_use_sql_plan_baselines_(false),
          optimizer_capture_sql_plan_baselines_(false),
          is_result_accurate_(false),
          _ob_use_parallel_execution_(false),
          _optimizer_adaptive_cursor_sharing_(false),
          character_set_results_(ObCharsetType::CHARSET_INVALID),
          timestamp_(0),
          tx_isolation_(transaction::ObTransIsolation::UNKNOWN),
          ob_pl_block_timeout_(0),
          autocommit_(false),
          ob_enable_trace_log_(false),
          ob_enable_sql_audit_(false),
          nls_length_semantics_(LS_BYTE),
          ob_org_cluster_id_(0),
          ob_query_timeout_(0),
          ob_trx_timeout_(0),
          collation_connection_(0),
          sql_mode_(DEFAULT_OCEANBASE_MODE),
          nls_formats_{},
          ob_trx_idle_timeout_(0),
          ob_trx_lock_timeout_(-1),
          nls_collation_(CS_TYPE_INVALID),
          nls_nation_collation_(CS_TYPE_INVALID),
          ob_trace_info_()
    {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        MEMSET(nls_formats_buf_[i], 0, MAX_NLS_FORMAT_STR_LEN);
      }
    }
    ~SysVarsCacheData()
    {}

    void reset()
    {
      auto_increment_increment_ = 0;
      sql_throttle_current_priority_ = 100;
      ob_last_schema_version_ = 0;
      sql_select_limit_ = 0;
      auto_increment_offset_ = 0;
      last_insert_id_ = 0;
      binlog_row_image_ = 2;
      foreign_key_checks_ = 0;
      tx_read_only_ = false;
      ob_enable_plan_cache_ = false;
      optimizer_use_sql_plan_baselines_ = false;
      optimizer_capture_sql_plan_baselines_ = false;
      is_result_accurate_ = false;
      _ob_use_parallel_execution_ = false;
      _optimizer_adaptive_cursor_sharing_ = false;
      character_set_results_ = ObCharsetType::CHARSET_INVALID;
      timestamp_ = 0;
      tx_isolation_ = transaction::ObTransIsolation::UNKNOWN;
      ob_pl_block_timeout_ = 0;
      autocommit_ = false;
      ob_enable_trace_log_ = false;
      ob_org_cluster_id_ = 0;
      ob_query_timeout_ = 0;
      ob_trx_timeout_ = 0;
      collation_connection_ = 0;
      ob_enable_sql_audit_ = false;
      nls_length_semantics_ = LS_BYTE;
      sql_mode_ = DEFAULT_OCEANBASE_MODE;
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        nls_formats_[i].reset();
        MEMSET(nls_formats_buf_[i], 0, MAX_NLS_FORMAT_STR_LEN);
      }
      ob_trx_idle_timeout_ = 0;
      ob_trx_lock_timeout_ = -1;
      nls_collation_ = CS_TYPE_INVALID;
      nls_nation_collation_ = CS_TYPE_INVALID;
      ob_trace_info_.reset();
    }
    void set_nls_date_format(const common::ObString& format)
    {
      set_nls_format(NLS_DATE, format);
    }
    void set_nls_timestamp_format(const common::ObString& format)
    {
      set_nls_format(NLS_TIMESTAMP, format);
    }
    void set_nls_timestamp_tz_format(const common::ObString& format)
    {
      set_nls_format(NLS_TIMESTAMP_TZ, format);
    }
    void set_nls_format(const int64_t enum_value, const common::ObString& format)
    {
      if (0 <= enum_value && enum_value < ObNLSFormatEnum::NLS_MAX) {
        if (format.empty()) {
          nls_formats_[enum_value].reset();
          MEMSET(nls_formats_buf_[enum_value], 0, MAX_NLS_FORMAT_STR_LEN);
        } else {
          MEMCPY(nls_formats_buf_[enum_value], format.ptr(), format.length());
          nls_formats_[enum_value].assign_ptr(nls_formats_buf_[enum_value], format.length());
        }
      }
    }
    const common::ObString& get_nls_date_format() const
    {
      return nls_formats_[NLS_DATE];
    }
    const common::ObString& get_nls_timestamp_format() const
    {
      return nls_formats_[NLS_TIMESTAMP];
    }
    const common::ObString& get_nls_timestamp_tz_format() const
    {
      return nls_formats_[NLS_TIMESTAMP_TZ];
    }
    void set_ob_trace_info(const common::ObString& trace_info)
    {
      if (trace_info.empty()) {
        ob_trace_info_.reset();
      } else {
        int64_t trace_len = std::min(static_cast<int64_t>(trace_info.length()), OB_TRACE_BUFFER_SIZE);
        MEMCPY(trace_info_buf_, trace_info.ptr(), trace_len);
        ob_trace_info_.assign_ptr(trace_info_buf_, trace_len);
      }
    }
    const common::ObString& get_ob_trace_info() const
    {
      return ob_trace_info_;
    }
    TO_STRING_KV(K(autocommit_), K(ob_enable_trace_log_), K(ob_enable_sql_audit_), K(nls_length_semantics_),
        K(ob_org_cluster_id_), K(ob_query_timeout_), K(ob_trx_timeout_), K(collation_connection_), K(sql_mode_),
        K(nls_formats_[0]), K(nls_formats_[1]), K(nls_formats_[2]), K(ob_trx_idle_timeout_), K(ob_trx_lock_timeout_),
        K(nls_collation_), K(nls_nation_collation_), K_(sql_throttle_current_priority), K_(ob_last_schema_version),
        K_(sql_select_limit), K_(optimizer_use_sql_plan_baselines), K_(optimizer_capture_sql_plan_baselines),
        K_(is_result_accurate), K_(_ob_use_parallel_execution), K_(character_set_results), K_(ob_pl_block_timeout));

  public:
    static const int64_t MAX_NLS_FORMAT_STR_LEN = 256;

    // need NOT serialization.
    uint64_t auto_increment_increment_;
    int64_t sql_throttle_current_priority_;
    int64_t ob_last_schema_version_;
    int64_t sql_select_limit_;
    uint64_t auto_increment_offset_;
    uint64_t last_insert_id_;
    int64_t binlog_row_image_;
    int64_t foreign_key_checks_;
    bool tx_read_only_;
    bool ob_enable_plan_cache_;
    bool optimizer_use_sql_plan_baselines_;
    bool optimizer_capture_sql_plan_baselines_;
    bool is_result_accurate_;
    bool _ob_use_parallel_execution_;
    bool _optimizer_adaptive_cursor_sharing_;
    ObCharsetType character_set_results_;

    int64_t timestamp_;
    int32_t tx_isolation_;

    int64_t ob_pl_block_timeout_;

    // need serialization.
    bool autocommit_;
    bool ob_enable_trace_log_;
    bool ob_enable_sql_audit_;
    ObLengthSemantics nls_length_semantics_;
    int64_t ob_org_cluster_id_;
    int64_t ob_query_timeout_;
    int64_t ob_trx_timeout_;
    int64_t collation_connection_;
    ObSQLMode sql_mode_;
    common::ObString nls_formats_[ObNLSFormatEnum::NLS_MAX];
    int64_t ob_trx_idle_timeout_;
    int64_t ob_trx_lock_timeout_;
    ObCollationType nls_collation_;         // for oracle char and varchar2
    ObCollationType nls_nation_collation_;  // for oracle nchar and nvarchar2
    ObString ob_trace_info_;
    char trace_info_buf_[OB_TRACE_BUFFER_SIZE];

  private:
    char nls_formats_buf_[ObNLSFormatEnum::NLS_MAX][MAX_NLS_FORMAT_STR_LEN];
  };

#define DEF_SYS_VAR_CACHE_FUNCS(SYS_VAR_TYPE, SYS_VAR_NAME)                 \
  void set_##SYS_VAR_NAME(SYS_VAR_TYPE value, bool is_inc)                  \
  {                                                                         \
    if (is_inc) {                                                           \
      inc_data_.SYS_VAR_NAME##_ = (value);                                  \
      inc_##SYS_VAR_NAME##_ = true;                                         \
    } else {                                                                \
      base_data_.SYS_VAR_NAME##_ = (value);                                 \
      if (!inc_##SYS_VAR_NAME##_) {                                         \
        inc_data_.SYS_VAR_NAME##_ = (value);                                \
      }                                                                     \
    }                                                                       \
  }                                                                         \
  const SYS_VAR_TYPE& get_##SYS_VAR_NAME() const                            \
  {                                                                         \
    return get_##SYS_VAR_NAME(inc_##SYS_VAR_NAME##_);                       \
  }                                                                         \
  const SYS_VAR_TYPE& get_##SYS_VAR_NAME(bool is_inc) const                 \
  {                                                                         \
    return is_inc ? inc_data_.SYS_VAR_NAME##_ : base_data_.SYS_VAR_NAME##_; \
  }

#define DEF_SYS_VAR_CACHE_FUNCS_STR(SYS_VAR_NAME)                                     \
  void set_##SYS_VAR_NAME(const common::ObString& value, bool is_inc)                 \
  {                                                                                   \
    if (is_inc) {                                                                     \
      inc_data_.set_##SYS_VAR_NAME(value);                                            \
      inc_##SYS_VAR_NAME##_ = true;                                                   \
    } else {                                                                          \
      base_data_.set_##SYS_VAR_NAME(value);                                           \
      if (!inc_##SYS_VAR_NAME##_) {                                                   \
        inc_data_.set_##SYS_VAR_NAME(value);                                          \
      }                                                                               \
    }                                                                                 \
  }                                                                                   \
  const common::ObString& get_##SYS_VAR_NAME() const                                  \
  {                                                                                   \
    return get_##SYS_VAR_NAME(inc_##SYS_VAR_NAME##_);                                 \
  }                                                                                   \
  const common::ObString& get_##SYS_VAR_NAME(bool is_inc) const                       \
  {                                                                                   \
    return is_inc ? inc_data_.get_##SYS_VAR_NAME() : base_data_.get_##SYS_VAR_NAME(); \
  }

  class SysVarsCache {
  public:
    SysVarsCache() : inc_flags_(0)
    {}
    ~SysVarsCache()
    {}

  public:
    void reset()
    {
      base_data_.reset();
      inc_data_.reset();
      inc_flags_ = 0;
    }
    void clean_inc()
    {
      inc_flags_ = 0;
    }
    bool is_inc_empty() const
    {
      return inc_flags_ == 0;
    }
    DEF_SYS_VAR_CACHE_FUNCS(uint64_t, auto_increment_increment);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, sql_throttle_current_priority);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_last_schema_version);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, sql_select_limit);
    DEF_SYS_VAR_CACHE_FUNCS(uint64_t, auto_increment_offset);
    DEF_SYS_VAR_CACHE_FUNCS(uint64_t, last_insert_id);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, binlog_row_image);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, foreign_key_checks);
    DEF_SYS_VAR_CACHE_FUNCS(bool, tx_read_only);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_plan_cache);
    DEF_SYS_VAR_CACHE_FUNCS(bool, optimizer_use_sql_plan_baselines);
    DEF_SYS_VAR_CACHE_FUNCS(bool, optimizer_capture_sql_plan_baselines);
    DEF_SYS_VAR_CACHE_FUNCS(bool, is_result_accurate);
    DEF_SYS_VAR_CACHE_FUNCS(bool, _ob_use_parallel_execution);
    DEF_SYS_VAR_CACHE_FUNCS(bool, _optimizer_adaptive_cursor_sharing);
    DEF_SYS_VAR_CACHE_FUNCS(ObCharsetType, character_set_results);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, timestamp);
    DEF_SYS_VAR_CACHE_FUNCS(int32_t, tx_isolation);
    DEF_SYS_VAR_CACHE_FUNCS(bool, autocommit);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_trace_log);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_sql_audit);
    DEF_SYS_VAR_CACHE_FUNCS(ObLengthSemantics, nls_length_semantics);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_org_cluster_id);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_query_timeout);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_trx_timeout);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, collation_connection);
    DEF_SYS_VAR_CACHE_FUNCS(ObSQLMode, sql_mode);
    DEF_SYS_VAR_CACHE_FUNCS_STR(nls_date_format);
    DEF_SYS_VAR_CACHE_FUNCS_STR(nls_timestamp_format);
    DEF_SYS_VAR_CACHE_FUNCS_STR(nls_timestamp_tz_format);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_trx_idle_timeout);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_trx_lock_timeout);
    DEF_SYS_VAR_CACHE_FUNCS(ObCollationType, nls_collation);
    DEF_SYS_VAR_CACHE_FUNCS(ObCollationType, nls_nation_collation);
    DEF_SYS_VAR_CACHE_FUNCS_STR(ob_trace_info);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_pl_block_timeout);
    const common::ObString* get_nls_formats() const
    {
      return inc_data_.nls_formats_;
    }
    void set_autocommit_info(bool base_value, bool inc_value, bool is_inc)
    {
      base_data_.autocommit_ = base_value;
      inc_data_.autocommit_ = inc_value;
      inc_autocommit_ = is_inc;
    }
    void get_autocommit_info(bool& base_value, bool& inc_value, bool& is_inc)
    {
      base_value = base_data_.autocommit_;
      inc_value = inc_data_.autocommit_;
      is_inc = inc_autocommit_;
    }

  public:
    SysVarsCacheData base_data_;
    SysVarsCacheData inc_data_;
    union {
      uint64_t inc_flags_;
      struct {
        bool inc_auto_increment_increment_ : 1;
        bool inc_sql_throttle_current_priority_ : 1;
        bool inc_ob_last_schema_version_ : 1;
        bool inc_sql_select_limit_ : 1;
        bool inc_auto_increment_offset_ : 1;
        bool inc_last_insert_id_ : 1;
        bool inc_binlog_row_image_ : 1;
        bool inc_foreign_key_checks_ : 1;
        bool inc_tx_read_only_ : 1;
        bool inc_ob_enable_plan_cache_ : 1;
        bool inc_optimizer_use_sql_plan_baselines_ : 1;
        bool inc_optimizer_capture_sql_plan_baselines_ : 1;
        bool inc_is_result_accurate_ : 1;
        bool inc__ob_use_parallel_execution_ : 1;
        bool inc__optimizer_adaptive_cursor_sharing_ : 1;
        bool inc_character_set_results_ : 1;
        bool inc_timestamp_ : 1;
        bool inc_tx_isolation_ : 1;
        bool inc_autocommit_ : 1;
        bool inc_ob_enable_trace_log_ : 1;
        bool inc_ob_enable_sql_audit_;
        bool inc_nls_length_semantics_ : 1;
        bool inc_ob_org_cluster_id_ : 1;
        bool inc_ob_query_timeout_ : 1;
        bool inc_ob_trx_timeout_ : 1;
        bool inc_collation_connection_ : 1;
        bool inc_sql_mode_ : 1;
        bool inc_nls_date_format_ : 1;
        bool inc_nls_timestamp_format_ : 1;
        bool inc_nls_timestamp_tz_format_ : 1;
        bool inc_ob_trx_idle_timeout_ : 1;
        bool inc_ob_trx_lock_timeout_ : 1;
        bool inc_nls_collation_ : 1;
        bool inc_nls_nation_collation_ : 1;
        bool inc_ob_trace_info_ : 1;
        bool inc_ob_pl_block_timeout_ : 1;
      };
    };
  };

private:
  int64_t get_ser_version(uint64_t tenant_id, uint64_t eff_tenant_id) const;
  static const int64_t PRE_SER_VERSION = 0;
  static const int64_t CUR_SER_VERSION = 1;

  // data structure related:
  // session mgr has a function() named for_each_session, which can loop for all
  // active sessions in other threads, so we need query_mutex_ and thread_data_mutex_
  // for concurrency control between current and other threads.
  // btw, generally speaking:
  // query_mutex_ is used for the whole session object, and
  // thread_data_mutex_ is used for thread_data_ only.
  common::ObRecursiveMutex query_mutex_;
  common::ObRecursiveMutex thread_data_mutex_;
  bool is_valid_;         // is valid session entry
  bool is_deserialized_;  // if the session is deserialized, used for free data of temp table.
  // session properties:
  char tenant_[common::OB_MAX_TENANT_NAME_LENGTH + 1];  // current tenant
  uint64_t tenant_id_;  // current tenant ID, used for privilege check and resource audit
  char effective_tenant_[common::OB_MAX_TENANT_NAME_LENGTH + 1];
  uint64_t effective_tenant_id_;  // current effective tenant ID, used for schema check
  uint64_t rpc_tenant_id_;
  bool is_changed_to_temp_tenant_;   // if tenant is changed to temp tenant for show statement
  uint64_t user_id_;                 // current user id
  common::ObString client_version_;  // current client version
  common::ObString driver_version_;  // current driver version
  uint32_t sessid_;
  uint32_t master_sessid_;
  uint32_t version_;  // sessid version
  uint64_t proxy_sessid_;
  int64_t variables_last_modify_time_;
  int64_t global_vars_version_;  // used for obproxy synchronize variables
  int64_t sys_var_base_version_;
  int64_t des_sys_var_base_version_;
  transaction::ObTransDesc trans_desc_;
  int32_t trans_consistency_type_;
  TransResult trans_result_;
  common::ObSEArray<TableStmtType, 2> total_stmt_tables_;
  common::ObSEArray<TableStmtType, 1> cur_stmt_tables_;
  char ssl_cipher_buff_[64];

protected:
  common::ObSmallBlockAllocator<> block_allocator_;
  common::ObSmallBlockAllocator<> ps_session_info_allocator_;
  common::ObStringBuf name_pool_;           // for variables names and statement names
  common::ObStringBuf base_sys_var_alloc_;  // for variables names and statement names
  TransFlags trans_flags_;

private:
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  ObSessionValMap user_var_val_map_;                                            // user variables
  share::ObBasicSysVar* sys_vars_[share::ObSysVarFactory::ALL_SYS_VARS_COUNT];  // system variables
  common::ObSEArray<int64_t, 64> influence_plan_var_indexs_;
  common::ObString sys_var_in_pc_str_;
  bool is_first_gen_;  // is first generate sys_var_in_pc_str_;
  share::ObSysVarFactory sys_var_fac_;
  char trace_id_buff_[64];  // this buffer is used for performance.

  //==============these members need serialization==============
  common::ObConsistencyLevel consistency_level_;
  ObTimeZoneInfoWrap tz_info_wrap_;
  int64_t next_tx_read_only_;
  int32_t next_tx_isolation_;
  //===============================================================

  //==============these members need NOT serialization==============
  bool log_id_level_map_valid_;
  common::ObLogIdLevelMap log_id_level_map_;
  //===============================================================

  // used for calculating which system variables need serialization,
  // set NULL after query is done.
  ObPhysicalPlan* cur_phy_plan_;
  // sql_id of cur_phy_plan_ sql
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];

  //=======================ObProxy && OCJ related============================
  obmysql::ObMySQLCapabilityFlags capability_;
  obmysql::ObProxyCapabilityFlags proxy_capability_;
  common::ObClientMode client_mode_;  // client mode, java client , obproxy or etc.
  // add by oushen, track changed session info
  common::ObSEArray<ChangedVar, 8> changed_sys_vars_;
  common::ObSEArray<common::ObString, 16> changed_user_vars_;
  common::ObArenaAllocator changed_var_pool_;  // reuse for each statement
  bool is_database_changed_;                   // is schema changed
  share::ObFeedbackManager feedback_manager_;  // feedback T-L-V
  // add, cached the flag whether transaction is sepcified
  ObTransSpecfiedStatus trans_spec_status_;
  //========================================================================

  // debug sync actions stored in session
  common::ObDSSessionActions debug_sync_actions_;
  ObPartitionHitInfo partition_hit_;
  uint32_t magic_num_;
  int64_t current_execution_id_;
  common::ObCurTraceId::TraceId last_trace_id_;
  common::ObCurTraceId::TraceId curr_trace_id_;
  common::ObString app_trace_id_;
  uint64_t database_id_;
  ObQueryRetryInfo retry_info_;
  // used to determine whether it is a retry query.
  common::ObCurTraceId::TraceId last_query_trace_id_;

protected:
  // this should be used by subclass, so need be protected
  MultiThreadData thread_data_;
  // nested session and sql execute for foreign key, trigger, pl udf, and so on.
  // inited to -1, set to 0 for main query, inc / dec for nested query.
  int64_t nested_count_;

private:
  // local cache of system variables for performance.
  SysVarsCache sys_vars_cache_;

private:
  int64_t safe_weak_read_snapshot_;
  int64_t weak_read_snapshot_source_;

  int64_t curr_trans_last_stmt_end_time_;
  int64_t read_snapshot_version_;
  bool check_sys_variable_;
  bool is_foreign_key_cascade_;
  bool is_foreign_key_check_exist_;
  bool acquire_from_pool_;
  // inited to true, set when some particular errors occur, indicates that session cannot be
  // released back to session pool.
  // so we need NOT and must NOT reset release_to_pool_ in reset().
  bool release_to_pool_;
  volatile int64_t is_tenant_killed_;  // use int64_t for ATOMIC_LOAD / ATOMIC_STORE.
  int64_t reused_count_;
  stmt::StmtType first_stmt_type_;

  // used for compatibility of serialization when multi observers with different version
  // running together, if these versions have different serialization operations, for example:
  // observer before 2.2.3 will serialize all system variables, and
  // observer of 2.2.3 or later will skip oracle only system variables in mysql mode.
  uint64_t exec_min_cluster_version_;
  stmt::StmtType stmt_type_;

private:
  // used for all_virtual_processlist.
  int64_t thread_id_;
  // indicate whether user password is expired, is set when session is established.
  // will not be changed during whole session lifetime unless user changes password
  // in this session.
  bool is_password_expired_;
};

inline const common::ObString ObBasicSessionInfo::get_current_query_string() const
{
  common::ObString str_ret;
  str_ret.assign_ptr(const_cast<char*>(thread_data_.cur_query_), static_cast<int32_t>(thread_data_.cur_query_len_));
  return str_ret;
}

inline ObCollationType ObBasicSessionInfo::get_local_collation_connection() const
{
  return static_cast<common::ObCollationType>(sys_vars_cache_.get_collation_connection());
}

inline ObCollationType ObBasicSessionInfo::get_nls_collation() const
{
  return sys_vars_cache_.get_nls_collation();
}

inline ObCollationType ObBasicSessionInfo::get_nls_collation_nation() const
{
  return sys_vars_cache_.get_nls_nation_collation();
}

inline const ObString& ObBasicSessionInfo::get_ob_trace_info() const
{
  return sys_vars_cache_.get_ob_trace_info();
}

inline bool ObBasicSessionInfo::get_local_autocommit() const
{
  return sys_vars_cache_.get_autocommit();
}

inline uint64_t ObBasicSessionInfo::get_local_auto_increment_increment() const
{
  return sys_vars_cache_.get_auto_increment_increment();
}

inline uint64_t ObBasicSessionInfo::get_local_auto_increment_offset() const
{
  return sys_vars_cache_.get_auto_increment_offset();
}

inline uint64_t ObBasicSessionInfo::get_local_last_insert_id() const
{
  return sys_vars_cache_.get_last_insert_id();
}

inline bool ObBasicSessionInfo::get_local_ob_enable_plan_cache() const
{
  return sys_vars_cache_.get_ob_enable_plan_cache();
}

inline bool ObBasicSessionInfo::get_local_ob_enable_sql_audit() const
{
  return sys_vars_cache_.get_ob_enable_sql_audit();
}

inline ObLengthSemantics ObBasicSessionInfo::get_local_nls_length_semantics() const
{
  return sys_vars_cache_.get_nls_length_semantics();
}

// oracle SYS user actual nls_length_semantics is always BYTE
inline ObLengthSemantics ObBasicSessionInfo::get_actual_nls_length_semantics() const
{
  return OB_ORA_SYS_DATABASE_ID == extract_pure_id(get_database_id()) ?
          LS_BYTE : sys_vars_cache_.get_nls_length_semantics();
}

inline int64_t ObBasicSessionInfo::get_local_ob_org_cluster_id() const
{
  return sys_vars_cache_.get_ob_org_cluster_id();
}

inline int64_t ObBasicSessionInfo::get_local_timestamp() const
{
  return sys_vars_cache_.get_timestamp();
}
inline const common::ObString ObBasicSessionInfo::get_local_nls_date_format() const
{
  return sys_vars_cache_.get_nls_date_format();
}
inline const common::ObString ObBasicSessionInfo::get_local_nls_timestamp_format() const
{
  return sys_vars_cache_.get_nls_timestamp_format();
}
inline const common::ObString ObBasicSessionInfo::get_local_nls_timestamp_tz_format() const
{
  return sys_vars_cache_.get_nls_timestamp_tz_format();
}
inline const common::ObString* ObBasicSessionInfo::get_local_nls_formats() const
{
  return sys_vars_cache_.get_nls_formats();
}

inline int ObBasicSessionInfo::get_local_nls_format(const ObObjType type, ObString& format_str) const
{
  int ret = OB_SUCCESS;
  switch (type) {
    case ObDateTimeType:
      format_str = sys_vars_cache_.get_nls_date_format();
      break;
    case ObTimestampNanoType:
    case ObTimestampLTZType:
      format_str = sys_vars_cache_.get_nls_timestamp_format();
      break;
    case ObTimestampTZType:
      format_str = sys_vars_cache_.get_nls_timestamp_tz_format();
      break;
    default:
      ret = OB_INVALID_DATE_VALUE;
      SQL_SESSION_LOG(WARN, "invalid argument. wrong type for source.", K(ret), K(type));
      break;
  }
  return ret;
}

inline int ObBasicSessionInfo::set_follower_first_feedback(const share::ObFollowerFirstFeedbackType type)
{
  INIT_SUCC(ret);
  if (is_abundant_feedback_support()) {
    if (OB_FAIL(feedback_manager_.add_follower_first_fb_info(type))) {
      SQL_SESSION_LOG(WARN, "fail to add follower first fb info", K(type), K(ret));
    }
  }
  return ret;
}

inline int ObBasicSessionInfo::set_partition_location_feedback(const share::ObFBPartitionParam& param)
{
  INIT_SUCC(ret);
  if (is_abundant_feedback_support()) {
    if (OB_FAIL(feedback_manager_.add_partition_fb_info(param))) {
      SQL_SESSION_LOG(WARN, "fail to add partition fb info", K(param), K(ret));
    }
  }
  return ret;
}

// object execute environment.
class ObExecEnv {
public:
  enum ExecEnvType {
    SQL_MODE = 0,
    CHARSET_CLIENT,
    COLLATION_CONNECTION,
    COLLATION_DATABASE,
    MAX_ENV,
  };

  static constexpr share::ObSysVarClassType ExecEnvMap[MAX_ENV + 1] = {share::SYS_VAR_SQL_MODE,
      share::SYS_VAR_CHARACTER_SET_CLIENT,
      share::SYS_VAR_COLLATION_CONNECTION,
      share::SYS_VAR_COLLATION_DATABASE,
      share::SYS_VAR_INVALID};

  ObExecEnv()
      : sql_mode_(DEFAULT_OCEANBASE_MODE),
        charset_client_(CS_TYPE_INVALID),
        collation_connection_(CS_TYPE_INVALID),
        collation_database_(CS_TYPE_INVALID)
  {}
  virtual ~ObExecEnv()
  {}

  TO_STRING_KV(K_(sql_mode), K_(charset_client), K_(collation_connection), K_(collation_database));

  void reset();

  bool operator==(const ObExecEnv& other) const;
  bool operator!=(const ObExecEnv& other) const;

  static int gen_exec_env(const ObBasicSessionInfo& session, char* buf, int64_t len, int64_t& pos);

  int init(const ObString& exec_env);
  int load(ObBasicSessionInfo& session);
  int store(ObBasicSessionInfo& session);

  ObSQLMode get_sql_mode()
  {
    return sql_mode_;
  }
  ObCharsetType get_charset_client()
  {
    return ObCharset::charset_type_by_coll(charset_client_);
  }
  ObCollationType get_collation_connection()
  {
    return collation_connection_;
  }
  ObCollationType get_collation_database()
  {
    return collation_database_;
  }

private:
  static int get_exec_env(const ObString& exec_env, ExecEnvType type, ObString& value);

private:
  ObSQLMode sql_mode_;
  ObCollationType charset_client_;
  ObCollationType collation_connection_;
  ObCollationType collation_database_;
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif /* _OB_OBPROXY_BASIC_SESSION_INFO_H */
