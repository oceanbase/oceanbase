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
#include "lib/ash/ob_active_session_guard.h"
#include "rpc/ob_sql_request_operator.h"
#include "share/ob_compatibility_control.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_time_zone_info_manager.h"
#include "share/ob_label_security.h"
#include "storage/tx/ob_trans_define.h"
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
#include "sql/monitor/flt/ob_flt_extra_info.h"
#include "sql/monitor/flt/ob_flt_utils.h"
#include "sql/parser/ob_parser_utils.h"

namespace oceanbase
{
namespace observer {
class ObSMConnection;
}
using sql::FLTControlInfo;
namespace sql
{
class ObExprRegexpSessionVariables;
class ObPCMemPctConf;
class ObPartitionHitInfo
{
public:
  ObPartitionHitInfo() : value_(true), freeze_(false) {}
  ~ObPartitionHitInfo() {}
  bool get_bool() { return value_; }
  void try_set_bool(bool v) { if (!freeze_) value_ = v; }
  void freeze() { freeze_ = true; }
  void reset() { value_ = true; freeze_ = false; }
private:
  bool value_;
  bool freeze_;
};
struct ObSessionNLSParams //oracle nls parameters
{
  ObLengthSemantics nls_length_semantics_;
  ObCollationType nls_collation_; //for oracle char and varchar2
  ObCollationType nls_nation_collation_; //for oracle nchar and nvarchar2

  TO_STRING_KV(K(nls_length_semantics_), K(nls_collation_), K(nls_nation_collation_));
};


#define TZ_INFO(session) \
  (NULL != (session) ? (session)->get_timezone_info() : NULL)

#define CREATE_OBJ_PRINT_PARAM(session) \
  (NULL != (session) ? (session)->create_obj_print_params() : ObObjPrintParams())

// flag is a single bit, but marco(e.g., IS_NO_BACKSLASH_ESCAPES) compare two 64-bits int using '&';
// if we directly assign the result to flag(single bit), only the last bit of the result is used,
// which is equal to 'flag = result & 1;'.
// So we first convert the result to bool(tmp_flag) and assign the bool to flag, which is equal to
// 'flag = result!=0;'.
#define GET_SQL_MODE_BIT(marco, sql_mode, flag) \
  do {                                          \
    bool tmp_flag=false;                        \
    marco(sql_mode, tmp_flag);                  \
    flag = tmp_flag;                            \
  } while(0)

#ifndef NDEBUG
#define CHECK_COMPATIBILITY_MODE(session) \
  do {                                    \
    if (NULL != (session)) {              \
      (session)->check_compatibility_mode(); \
    }                                     \
  } while(0)
#else
#define CHECK_COMPATIBILITY_MODE(session) UNUSED(session)
#endif


class ObExecContext;
class ObSysVarInPC;
class ObBasicSessionInfo;

enum ObDisconnectState
{
  DIS_INIT,                 // INIT
  NORMAL_QUIT,              // QUIT.
  NORMAL_KILL_SESSION,      // KILL SESSION.
  SERVER_FORCE_DISCONNECT,  // force_disconnect.
  CLIENT_FORCE_DISCONNECT,  // TCP disconnect.
};

enum ObSQLSessionState
{
  SESSION_INIT,
  SESSION_SLEEP,
  QUERY_ACTIVE,
  QUERY_KILLED,
  SESSION_KILLED,
  QUERY_DEADLOCKED,
};

enum ObTransSpecfiedStatus
{
  TRANS_SPEC_NOT_SET,
  TRANS_SPEC_SET,
  TRANS_SPEC_COMMIT,
};

enum ObSessionRetryStatus
{
  SESS_NOT_IN_RETRY,  //session没有在retry
  SESS_IN_RETRY,  //session retrying, 且和复制表无关
  SESS_IN_RETRY_FOR_DUP_TBL, //复制表相关错误引发的retry
};

/// ObBasicSessionInfo存储系统变量及其相关变量，并存储远程执行SQL task时，需要序列化到远端的状态
/// ObSQLSessionInfo存储其他状态信息，如prepared statment相关信息等
/// @note 所有的系统变量存储在sys_var_val_map_中，同时，频繁使用的变量还在本数据结构中作为独立的
/// 成员存储一份，方便访问。注意在更新系统变量和序列化的时候保持两份数据的一致。
class ObBasicSessionInfo
{
  OB_UNIS_VERSION_V(1);
public:
  // 256KB ~= 4 * OB_COMMON_MEM_BLOCK_SIZE
  static const int64_t APPROX_MEM_USAGE_PER_SESSION = 256 * 1024L;
  static const uint64_t VALID_PROXY_SESSID = 0;
  static const uint32_t INVALID_SESSID = common::INVALID_SESSID;

  enum SafeWeakReadSnapshotSource
  {
    UNKOWN,
    AUTO_PLAN,
    PROXY_INPUT,
    LAST_STMT,
  };
  typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString,
          share::ObBasicSysVar*>::AllocType, common::ObWrapperAllocator> SysVarNameValMapAllocer;
  typedef common::hash::ObHashMap<common::ObString,
                                  share::ObBasicSysVar*,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<common::ObString>,
                                  common::hash::equal_to<common::ObString>,
                                  SysVarNameValMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator> SysVarNameValMap;
  typedef lib::ObLockGuard<common::ObRecursiveMutex> LockGuard;

  class TableStmtType
  {
    OB_UNIS_VERSION_V(1);
  public:
    TableStmtType()
      : table_id_(common::OB_INVALID_ID),
        stmt_type_(stmt::T_NONE)
    {}
    TableStmtType(uint64_t table_id)
      : table_id_(table_id),
        stmt_type_(stmt::T_NONE)
    {}
    TableStmtType(uint64_t table_id, stmt::StmtType stmt_type)
      : table_id_(table_id),
        stmt_type_(stmt_type)
    {}
    virtual ~TableStmtType()
    {}
    inline bool operator==(const TableStmtType &rv) const
    {
      return table_id_ == rv.table_id_;
    }
    inline bool is_mutating() const
    {
      return stmt_type_ != stmt::T_SELECT;
    }
    inline void skip_mutating(stmt::StmtType &saved_stmt_type)
    {
      saved_stmt_type = get_stmt_type();
      set_stmt_type(stmt::T_SELECT);
    }
    inline void restore_mutating(stmt::StmtType saved_stmt_type)
    {
      set_stmt_type(saved_stmt_type);
    }
    inline stmt::StmtType get_stmt_type() const
    {
      return stmt_type_;
    }
    inline void set_stmt_type(stmt::StmtType stmt_type)
    {
      stmt_type_ = stmt_type;
    }
    TO_STRING_KV(K(table_id_),
                 K(stmt_type_));
  private:
    uint64_t table_id_;
    stmt::StmtType stmt_type_;
  };

  static const int64_t MIN_CUR_QUERY_LEN = 512;
  static const int64_t MAX_CUR_QUERY_LEN = 16 * 1024;
  static const int64_t MAX_QUERY_STRING_LEN = 64 * 1024;
  class TransFlags
  {
  public:
    TransFlags() : flags_(0), changed_(false) {}
    virtual ~TransFlags() {}
    inline void reset() { flags_ = 0; }
    inline uint64_t get_flags() const { return flags_; }
    void set_has_exec_inner_dml(bool v) { has_exec_inner_dml_ = v; }
    bool has_exec_inner_dml() const { return has_exec_inner_dml_; }
  private:
    // NOTICE:
    // after 4.1, txn support executed on multiple node
    // if use TransFlags, please add it into session_sync
    // in order to sync it to txn execution node
    union {
      uint64_t flags_;
      struct {
        // has executed dml stmt via inner connection
        // used by PL detect autonomous trasnaction missing commit or rollback
        // will not cross server, do not required to be synced
        bool has_exec_inner_dml_ : 1;
      };
    };
    bool changed_;
  };
  class SqlScopeFlags
  {
  public:
    SqlScopeFlags() : flags_(0) {}
    virtual ~SqlScopeFlags() {}
    inline void reset() { flags_ = 0; }
    inline void set_is_in_user_scope(bool value) { set_flag(value, IS_IN_USER_SCOPE); }
    inline bool is_in_user_scope() const { return flags_ & IS_IN_USER_SCOPE; }
    inline void set_flags(uint64_t value) { flags_ = value; }
    inline uint64_t get_flags() const { return flags_; }
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
    // create table as select分为create table跟insert select，
    // 这个场景不是inner_sql，增加这个标记的作用是为了区分insert select是否是由其他用户sql产生的，
    // 针对generated always as identity column，外部sql禁止做insert
    // 当前只有一个场景使用，后续可以扩展
    static const uint64_t IS_IN_USER_SCOPE = 1ULL << 0;
    uint64_t flags_;
  };
  class UserScopeGuard
  {
  public:
    UserScopeGuard(SqlScopeFlags &sql_scope_flags) : sql_scope_flags_(sql_scope_flags)
    {
      sql_scope_flags_.set_is_in_user_scope(true);
    }
    ~UserScopeGuard() { sql_scope_flags_.set_is_in_user_scope(false); }
    SqlScopeFlags &sql_scope_flags_;
  };
  // 切换自治事务一定需要切换嵌套语句，否则切回主事务后语句执行的上下文信息可能已经有变化，比如：
  //
  // 所以原则上TransSavedValue应该包含StmtSavedValue的所有属性，考虑将前者作为后者的子类，
  // 但有几个属性在两者中都存在、需要执行的操作却不同，最后决定将两者处理相同的属性抽出来放进
  // 公共基类BaseSavedValue，方便最大程度复用代码，将来新增属性时也要参考类似原则确定放在哪个类中。
  class BaseSavedValue
  {
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
      inc_autocommit_ = false;
      need_serial_exec_ = false;
    }
  public:
    // 原StmtSavedValue的属性
    ObPhysicalPlan *cur_phy_plan_;
    char cur_query_[MAX_QUERY_STRING_LEN];
    volatile int64_t cur_query_len_;
//  int64_t cur_query_start_time_;          // 用于计算事务超时时间，如果在base_save_session接口中操作
                                            // 会导致start_trans报事务超时失败，不放在基类中。
    common::ObSEArray<TableStmtType, 4> total_stmt_tables_;
    common::ObSEArray<TableStmtType, 2> cur_stmt_tables_;
//  bool in_transaction_;                   // 对应TransSavedValue的trans_flags_，不放在基类中。
    bool read_uncommited_;
    bool inc_autocommit_;
    bool need_serial_exec_;
  public:
    // 原TransSavedValue的属性
//  transaction::ObTxDesc trans_desc_;   // 两者都有trans_desc，但执行操作完全不同，不放在基类中。
//  TransFlags trans_flags_;                // 对应StmtSavedValue的in_transaction_，不放在基类中。
//  TransResult tx_result_;              // 两者都有tx_result_，但执行操作完全不同，不放在基类中。
//  int64_t nested_count_;                  // 特有属性，不放在基类中。
  };
  // for switch stmt.
  class StmtSavedValue : public BaseSavedValue
  {
  public:
    StmtSavedValue()
    {
      reset();
    }
    inline void reset()
    {
      BaseSavedValue::reset();
      tx_result_.reset();
      cur_query_start_time_ = 0;
      in_transaction_ = false;
      stmt_type_ = sql::stmt::StmtType::T_NONE;
    }
  public:
    transaction::ObTxExecResult tx_result_;
    int64_t cur_query_start_time_;
    bool in_transaction_;
    sql::stmt::StmtType stmt_type_;
  };
  // for switch trans.
  class TransSavedValue : public BaseSavedValue
  {
  public:
    TransSavedValue()
    {
      reset();
    }
    void reset()
    {
      BaseSavedValue::reset();
      tx_desc_ = NULL;
      trans_flags_.reset();
      tx_result_.reset();
      nested_count_ = -1;
      xid_.reset();
    }
  public:
    transaction::ObTxDesc *tx_desc_;
    TransFlags trans_flags_;
    transaction::ObTxExecResult tx_result_;
    int64_t nested_count_;
    transaction::ObXATransID xid_;
  };

  enum class ForceRichFormatStatus
  {
    Disable = 0,
    FORCE_ON,
    FORCE_OFF
  };

public:
  ObBasicSessionInfo(const uint64_t tenant_id);
  virtual ~ObBasicSessionInfo();

  virtual int init(uint32_t sessid, uint64_t proxy_sessid,
                   common::ObIAllocator *bucket_allocator, const ObTZInfoMap *tz_info);
  //for test
  virtual int test_init(uint32_t sessid, uint64_t proxy_sessid,
                   common::ObIAllocator *bucket_allocator);
  virtual void destroy();
  //called before put session to freelist: unlock/set invalid
  virtual void reset(bool skip_sys_var = false);
  void reset_user_var();
  void set_tenant_session_mgr(ObTenantSQLSessionMgr *tenant_session_mgr)
  {
    tenant_session_mgr_ = tenant_session_mgr;
  }
  ObTenantSQLSessionMgr *get_tenant_session_mgr() { return tenant_session_mgr_; }
  virtual void clean_status();
  //setters
  int reset_timezone();
  int init_tenant(const common::ObString &tenant_name, const uint64_t tenant_id);
  int set_tenant(const common::ObString &tenant_name, const uint64_t tenant_id);
  int set_tenant(const common::ObString &tenant_name,
                 const uint64_t tenant_id,
                 char *ori_tenant_name,
                 const uint64_t length,
                 uint64_t &ori_tenant_id);
  int switch_tenant(uint64_t effective_tenant_id);
  int switch_tenant_with_name(uint64_t effective_tenant_id, const common::ObString &tenant_name);
  int set_default_database(const common::ObString &database_name,
                           common::ObCollationType coll_type = common::CS_TYPE_INVALID);
  int reset_default_database() { return set_default_database(""); }
  int update_database_variables(share::schema::ObSchemaGetterGuard *schema_guard);
  int update_max_packet_size();
  int64_t get_thread_id() const { return thread_id_; }
  void set_thread_id(int64_t t) { thread_id_ = t; }
  void set_valid(const bool valid) {is_valid_ = valid;};
  int set_client_version(const common::ObString &client_version);
  int set_driver_version(const common::ObString &driver_version);
  int64_t get_sys_vars_encode_max_size() { return sys_vars_encode_max_size_; }
  void set_sys_vars_encode_max_size(int64_t size) { sys_vars_encode_max_size_ = size; }
  void set_sql_mode(const ObSQLMode sql_mode)
  {
    // Compatibility mode store in sql_mode_ but controlled by ob_compatibility_mode variable,
    // we can not overwrite it.
    ObSQLMode real_sql_mode = (sql_mode & (~ALL_SMO_COMPACT_MODE)) |
                              (sys_vars_cache_.get_sql_mode() & ALL_SMO_COMPACT_MODE);
    sys_vars_cache_.set_sql_mode(real_sql_mode);
  }
  void set_compatibility_mode(const common::ObCompatibilityMode compat_mode)
  {
    ObSQLMode sql_mode = ob_compatibility_mode_to_sql_mode(compat_mode) |
                         (sys_vars_cache_.get_sql_mode() & ~ALL_SMO_COMPACT_MODE);
    sys_vars_cache_.set_sql_mode(sql_mode);
  }
  void set_global_vars_version(const int64_t modify_time) { global_vars_version_ = modify_time; }
  void set_is_deserialized() { is_deserialized_ = true; }
  bool get_is_deserialized() { return is_deserialized_; }
  void set_exec_min_cluster_version() { exec_min_cluster_version_ = GET_MIN_CLUSTER_VERSION(); }
  uint64_t get_exec_min_cluster_version() const { return exec_min_cluster_version_; }
  // local sys var getters
  inline ObCollationType get_local_collation_connection() const;
  inline ObCollationType get_nls_collation() const;
  inline ObCollationType get_nls_collation_nation() const;
  inline const ObString &get_ob_trace_info() const;
  inline const ObString &get_plsql_ccflags() const;
  inline const ObString &get_iso_nls_currency() const;
  inline const ObString &get_log_row_value_option() const;
  int64_t get_default_lob_inrow_threshold() const;
  bool get_local_autocommit() const;
  uint64_t get_local_auto_increment_increment() const;
  uint64_t get_local_auto_increment_offset() const;
  uint64_t get_local_last_insert_id() const;
  bool get_local_ob_enable_pl_cache() const;
  bool get_local_ob_enable_plan_cache() const;
  bool get_local_ob_enable_sql_audit() const;
  bool get_local_cursor_sharing_mode() const;
  ObLengthSemantics get_local_nls_length_semantics() const;
  ObLengthSemantics get_actual_nls_length_semantics() const;
  int64_t get_local_ob_org_cluster_id() const;
  int64_t get_local_timestamp() const;
  const common::ObString get_local_nls_date_format() const;
  const common::ObString get_local_nls_timestamp_format() const;
  const common::ObString get_local_nls_timestamp_tz_format() const;
  int get_local_nls_format(const ObObjType type, ObString &format_str) const;
  int set_time_zone(const common::ObString &str_val, const bool is_oralce_mode,
                    const bool need_check_valid /* true */);
  void init_use_rich_format()
  {
    config_use_rich_format_ = GCONF._global_enable_rich_vector_format;
    if (!config_use_rich_format_) {
      use_rich_vector_format_ = false;
      force_rich_vector_format_ = ForceRichFormatStatus::FORCE_OFF;
    } else {
      use_rich_vector_format_ = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0
                                && sys_vars_cache_.get_enable_rich_vector_format();
      force_rich_vector_format_ = ForceRichFormatStatus::Disable;
    }
  }
  bool is_force_off_rich_format() {
    return force_rich_vector_format_ == ForceRichFormatStatus::FORCE_OFF;
  }
  bool use_rich_format() const {
    if (force_rich_vector_format_ != ForceRichFormatStatus::Disable) {
      return force_rich_vector_format_ == ForceRichFormatStatus::FORCE_ON;
    } else {
      return use_rich_vector_format_;
    }
  }

  bool config_use_rich_format() { return config_use_rich_format_; }

  bool initial_use_rich_format() const {
    return use_rich_vector_format_;
  }

  ObBasicSessionInfo::ForceRichFormatStatus get_force_rich_format_status() const
  {
    return force_rich_vector_format_;
  }

  void set_force_rich_format(ObBasicSessionInfo::ForceRichFormatStatus status)
  {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0) {
      force_rich_vector_format_ = status;
    } else {
      force_rich_vector_format_ = ForceRichFormatStatus::Disable;
    }
  }
  //getters
  const common::ObString get_tenant_name() const;
  uint64_t get_priv_tenant_id() const { return tenant_id_; }
  const common::ObString get_effective_tenant_name() const;
  // 关于各种tenant_id的使用，可参考
  uint64_t get_effective_tenant_id() const { return effective_tenant_id_; }
  // RPC framework use rpc_tenant_id() to deliver remote/distribute tasks.
  void set_rpc_tenant_id(uint64_t tenant_id) { rpc_tenant_id_ = tenant_id; }
  uint64_t get_rpc_tenant_id() const
  {
    return rpc_tenant_id_ != 0 ? rpc_tenant_id_ : effective_tenant_id_;
  }
  uint64_t get_login_tenant_id() const { return tenant_id_; }
  void set_login_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  bool is_tenant_changed() const { return tenant_id_ != effective_tenant_id_; }
  int set_autocommit(bool autocommit);
  int get_autocommit(bool &autocommit) const
  {
    autocommit = sys_vars_cache_.get_autocommit();
    return common::OB_SUCCESS;
  }
  int get_explicit_defaults_for_timestamp(bool &explicit_defaults_for_timestamp) const;
  int get_sql_auto_is_null(bool &sql_auto_is_null) const;
  int get_is_result_accurate(bool &is_result_accurate) const
  {
    is_result_accurate = sys_vars_cache_.get_is_result_accurate();
    return common::OB_SUCCESS;
  }
  common::ObIArray<uint64_t>& get_enable_role_ids() { return enable_role_ids_; }
  const common::ObIArray<uint64_t>& get_enable_role_ids() const { return enable_role_ids_; }
  int get_show_ddl_in_compat_mode(bool &show_ddl_in_compat_mode) const;
  int get_sql_quote_show_create(bool &sql_quote_show_create) const;
  common::ObConsistencyLevel get_consistency_level() const { return consistency_level_; };
  bool is_zombie() const { return SESSION_KILLED == get_session_state();}
  bool is_query_killed() const;
  bool is_valid() const { return is_valid_; };
  uint64_t get_user_id() const { return user_id_; }
  uint64_t get_proxy_user_id() const { return proxy_user_id_; }
  bool is_auditor_user() const { return is_ora_auditor_user(user_id_); };
  bool is_lbacsys_user() const { return is_ora_lbacsys_user(user_id_); };
  bool is_oracle_sys_user() const { return is_ora_sys_user(user_id_); };
  bool is_mysql_root_user() const { return is_root_user(user_id_); };
  bool is_restore_user() const { return  0 == thread_data_.user_name_.case_compare(common::OB_RESTORE_USER_NAME); };
  bool is_proxy_sys_user() const
  {
    return OB_SYS_TENANT_ID == tenant_id_;
  };
  const common::ObString get_database_name() const;
  inline int get_database_id(uint64_t &db_id) const { db_id = database_id_; return common::OB_SUCCESS; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline void set_database_id(uint64_t db_id) { database_id_ = db_id; }
  inline const ObQueryRetryInfo &get_retry_info() const { return retry_info_; }
  inline ObQueryRetryInfo &get_retry_info_for_update() { return retry_info_; }
  inline const common::ObCurTraceId::TraceId &get_last_query_trace_id() const
  { return last_query_trace_id_; }
  int check_and_init_retry_info(const common::ObCurTraceId::TraceId &cur_trace_id,
                                const common::ObString &sql);
  void check_and_reset_retry_info(const common::ObCurTraceId::TraceId &cur_trace_id,
                                  bool is_packet_retry)
  {
    // 1.如果是本地重试，到达这里的时候已经重试完了，直接重置掉session中的retry info即可；
    // 2.如果是扔回队列重试（包括大查询被扔回队列的情况），不能重置session中retry info；
    // 3.如果是不重试，这里需要重置掉session中的retry info。
    // 注意，这里是要将retry info重置到not init的状态，所以要调reset，不能调clear。
    if (!is_packet_retry) {
      retry_info_.reset();
    }
    last_query_trace_id_.set(cur_trace_id);
  }
  const common::ObLogIdLevelMap *get_log_id_level_map() const;
  const common::ObString &get_client_version() const { return client_version_; }
  const common::ObString &get_driver_version() const { return driver_version_; }
  void destory_json_pl_mngr();
  intptr_t get_json_pl_mngr();
  int get_tx_timeout(int64_t &tx_timeout) const
  {
    tx_timeout = sys_vars_cache_.get_ob_trx_timeout();
    return common::OB_SUCCESS;
  }
  int get_query_timeout(int64_t &query_timeout) const
  {
    query_timeout = sys_vars_cache_.get_ob_query_timeout();
    return common::OB_SUCCESS;
  }
  int64_t get_query_timeout_ts() const; //获取当前query超时的绝对时间
  int64_t get_trx_lock_timeout() const
  {
    return sys_vars_cache_.get_ob_trx_lock_timeout();
  }
  int64_t get_ob_max_read_stale_time() {
    return sys_vars_cache_.get_ob_max_read_stale_time();
  }
  int get_sql_throttle_current_priority(int64_t &sql_throttle_current_priority)
  {
    sql_throttle_current_priority = sys_vars_cache_.get_sql_throttle_current_priority();
    return common::OB_SUCCESS;
  }
  int get_ob_last_schema_version(int64_t &ob_last_schema_version)
  {
    ob_last_schema_version = sys_vars_cache_.get_ob_last_schema_version();
    return common::OB_SUCCESS;
  }
  int get_pl_block_timeout(int64_t &pl_block_timeout) const;
  int get_binlog_row_image(int64_t &binlog_row_image) const
  {
    binlog_row_image = sys_vars_cache_.get_binlog_row_image();
    return common::OB_SUCCESS;
  }
  int get_ob_read_consistency(int64_t &ob_read_consistency) const;
  int get_sql_select_limit(int64_t &sql_select_limit) const
  {
    sql_select_limit = sys_vars_cache_.get_sql_select_limit();
    return common::OB_SUCCESS;
  }
  // session保留compatible mode，主要用于传递mode，方便后续进行guard切换，如inner sql connection等
  // 其他需要用mode地方请尽量使用线程上的is_oracle|mysql_mode
  // 同时可以使用check_compatibility_mode来检查线程与session上的mode是否一致
  ObCompatibilityMode get_compatibility_mode() const
  {
    return ob_sql_mode_to_compatibility_mode(get_sql_mode());
  }
  bool is_oracle_compatible() const { return ORACLE_MODE == get_compatibility_mode(); }
  int check_compatibility_mode() const;
  ObSQLMode get_sql_mode() const { return sys_vars_cache_.get_sql_mode(); }
  int get_div_precision_increment(int64_t &div_precision_increment) const;
  int get_character_set_client(common::ObCharsetType &character_set_client) const;
  int get_character_set_connection(common::ObCharsetType &character_set_connection) const;
  int get_ncharacter_set_connection(common::ObCharsetType &ncharacter_set_connection) const;
  int get_character_set_database(common::ObCharsetType &character_set_database) const;
  int get_character_set_results(common::ObCharsetType &character_set_results) const;
  int get_character_set_server(common::ObCharsetType &character_set_server) const;
  int get_character_set_system(common::ObCharsetType &character_set_system) const;
  int get_character_set_filesystem(common::ObCharsetType &character_set_filesystem) const;
  inline int get_collation_connection(common::ObCollationType &collation_connection) const
  {
    collation_connection = get_local_collation_connection();
    return common::OB_SUCCESS;
  }
  int get_collation_database(common::ObCollationType &collation_database) const;
  int get_collation_server(common::ObCollationType &collation_server) const;
  int get_foreign_key_checks(int64_t &foreign_key_checks) const
  {
    foreign_key_checks = sys_vars_cache_.get_foreign_key_checks();
    return common::OB_SUCCESS;
  }
  int get_capture_plan_baseline(bool &v)  const;
  int get_default_password_lifetime(uint64_t &default_password_lifetime) const
  {
    default_password_lifetime = sys_vars_cache_.get_default_password_lifetime();
    return common::OB_SUCCESS;
  }
  int get_use_plan_baseline(bool &v) const
  {
    v = sys_vars_cache_.get_optimizer_use_sql_plan_baselines();
    return common::OB_SUCCESS;
  }
  int get_nlj_batching_enabled(bool &v) const;
  int get_optimizer_features_enable_version(uint64_t &version) const;
  int get_enable_parallel_dml(bool &v) const;
  int get_enable_parallel_query(bool &v) const;
  int get_enable_parallel_ddl(bool &v) const;
  int get_force_parallel_query_dop(uint64_t &v) const;
  int get_parallel_degree_policy_enable_auto_dop(bool &v) const;
  int get_force_parallel_dml_dop(uint64_t &v) const;
  int get_force_parallel_ddl_dop(uint64_t &v) const;
  int get_partial_rollup_pushdown(int64_t &partial_rollup) const;
  int get_distinct_agg_partial_rollup_pushdown(int64_t &partial_rollup) const;
  int get_px_shared_hash_join(bool &shared_hash_join) const;
  int get_secure_file_priv(common::ObString &v) const;
  int get_sql_safe_updates(bool &v) const;
  int get_opt_dynamic_sampling(uint64_t &v) const;
  int get_sql_notes(bool &sql_notes) const;
  int get_regexp_stack_limit(int64_t &v) const;
  int get_regexp_time_limit(int64_t &v) const;
  int get_regexp_session_vars(ObExprRegexpSessionVariables &vars) const;
  int get_activate_all_role_on_login(bool &v) const;
  int update_timezone_info();
  const common::ObTimeZoneInfo *get_timezone_info() const { return tz_info_wrap_.get_time_zone_info(); }
  const common::ObTimeZoneInfoWrap &get_tz_info_wrap() const { return tz_info_wrap_; }
  inline int set_tz_info_wrap(const common::ObTimeZoneInfoWrap &other) { return tz_info_wrap_.deep_copy(other); }
  inline void set_nls_formats(const common::ObString *nls_formats)
  {
    sys_vars_cache_.set_nls_date_format(nls_formats[ObNLSFormatEnum::NLS_DATE]);
    sys_vars_cache_.set_nls_timestamp_format(nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP]);
    sys_vars_cache_.set_nls_timestamp_tz_format(nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP_TZ]);
  }
  int get_influence_plan_sys_var(ObSysVarInPC &sys_vars) const;
  const common::ObString &get_sys_var_in_pc_str() const { return sys_var_in_pc_str_; }
  const common::ObString &get_config_in_pc_str() const { return config_in_pc_str_; }
  uint64_t get_sys_var_config_hash_val() const { return sys_var_config_hash_val_; }
  void eval_sys_var_config_hash_val();
  int gen_sys_var_in_pc_str();
  int gen_configs_in_pc_str();
  uint32_t get_sessid() const { return sessid_; }
  // Used for view or function compatibility display.
  uint32_t get_compatibility_sessid() const
  {
    return client_sessid_ == INVALID_SESSID ? sessid_ : client_sessid_;
  }
  uint32_t get_client_sessid() const { return client_sessid_; }
  inline void set_client_sessid(uint32_t client_sessid)
  {
    client_sessid_ = client_sessid;
  }
  uint64_t get_client_create_time() const { return client_create_time_; }
  inline void set_client_create_time(uint64_t client_create_time)
  {
    client_create_time_ = client_create_time;
  }
  uint64_t get_proxy_sessid() const { return proxy_sessid_; }
  uint64_t get_sessid_for_table() const { return is_obproxy_mode()? get_proxy_sessid() : (is_master_session() ? get_sessid() : get_master_sessid()); } //用于临时表、查询建表时session id获取
  uint32_t get_master_sessid() const { return master_sessid_; }
  inline const common::ObString get_sess_bt() const { return ObString::make_string(sess_bt_buff_); }
  inline int32_t get_sess_ref_cnt() const { return sess_ref_cnt_; }
  void on_get_session();
  void on_revert_session();
  common::ObString get_ssl_cipher() const { return ObString::make_string(ssl_cipher_buff_); }
  void set_ssl_cipher(const char *value)
  {
    const size_t min_len = std::min(sizeof(ssl_cipher_buff_) - 1, strlen(value));
    MEMCPY(ssl_cipher_buff_, value, min_len);
    ssl_cipher_buff_[min_len] = '\0';
  }
  // Master session: receiving user SQL text sessions.
  // Slave session: receiving sql plan sessions, e.g.: remote executing,
  // distribute executing sessions.
  bool is_master_session() const { return INVALID_SESSID == master_sessid_; }
  common::ObDSSessionActions &get_debug_sync_actions() { return debug_sync_actions_; }
  int64_t get_global_vars_version() const { return global_vars_version_; }
  inline common::ObIArray<int64_t> &get_influence_plan_var_indexs() { return influence_plan_var_indexs_; }
  int64_t get_influence_plan_var_count() const { return influence_plan_var_indexs_.count(); }
  int get_pc_mem_conf(ObPCMemPctConf &pc_mem_conf);

  int get_sql_audit_mem_conf(int64_t &mem_pct);

  /// @{ thread_data_ related: }
  int set_user(const common::ObString &user_name, const common::ObString &host_name, const uint64_t user_id);
  inline void set_proxy_user_id(const uint64_t proxy_user_id) { proxy_user_id_ = proxy_user_id; }
  int set_real_client_ip_and_port(const common::ObString &client_ip, int32_t client_addr_port);
  int set_proxy_user(const common::ObString &user_name, const common::ObString &host_name, const uint64_t user_id);
  const common::ObString &get_user_name() const { return thread_data_.user_name_;}
  const common::ObString &get_host_name() const { return thread_data_.host_name_;}
  const common::ObString &get_proxy_user_name() const { return thread_data_.proxy_user_name_;}
  const common::ObString &get_proxy_host_name() const { return thread_data_.proxy_host_name_;}
  const common::ObString &get_client_ip() const { return thread_data_.client_ip_;}
  const common::ObString &get_user_at_host() const { return thread_data_.user_at_host_name_;}
  const common::ObString &get_user_at_client_ip() const { return thread_data_.user_at_client_ip_;}
  void set_client_addr_port(const int32_t client_addr_port)
  {
    thread_data_.client_addr_port_ = client_addr_port;
  };
  int32_t get_client_addr_port() const { return thread_data_.client_addr_port_; };
  rpc::ObSqlSockDesc& get_sock_desc() { return thread_data_.sock_desc_;}
  observer::ObSMConnection *get_sm_connection();
  void set_peer_addr(common::ObAddr peer_addr)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.peer_addr_ = peer_addr;
  }
  void set_client_addr(common::ObAddr client_addr)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.client_addr_ = client_addr;
  }
  const common::ObAddr &get_peer_addr() const {return thread_data_.peer_addr_;}
  const common::ObAddr &get_client_addr() const {return thread_data_.client_addr_;}
  const common::ObAddr &get_user_client_addr() const {return thread_data_.user_client_addr_;}
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
  int64_t get_query_start_time() const { return thread_data_.cur_query_start_time_; }
  int64_t get_cur_state_start_time() const { return thread_data_.cur_state_start_time_; }
  void set_interactive(bool is_interactive)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.is_interactive_ = is_interactive;
  }
  bool get_interactive() const { return thread_data_.is_interactive_; }
  void update_last_active_time()
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.last_active_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  }
  int64_t get_last_active_time() const { return thread_data_.last_active_time_; }
  void set_disconnect_state(ObDisconnectState dis_state)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.dis_state_ = dis_state;
  }
  int set_session_state(ObSQLSessionState state);
  int check_session_status();
  ObDisconnectState get_disconnect_state() const { return thread_data_.dis_state_;}
  ObSQLSessionState get_session_state() const { return thread_data_.state_;}
  const char *get_session_state_str()const;
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
    } else if (is_select_dup_follow_replic_err(ret) ||
               OB_NOT_MASTER == ret) {
      status = SESS_IN_RETRY_FOR_DUP_TBL;
    } else {
      status = SESS_IN_RETRY;
    }
    set_session_in_retry(status);
  }
  bool get_is_in_retry() {
    return SESS_NOT_IN_RETRY != thread_data_.is_in_retry_;
  }
  bool get_is_in_retry_for_dup_tbl() {
    return SESS_IN_RETRY_FOR_DUP_TBL == thread_data_.is_in_retry_;
  }
  void set_retry_active_time(int64_t time)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.retry_active_time_ = time;
  }
  int64_t get_retry_active_time() const { return thread_data_.retry_active_time_; }
  void set_is_request_end(bool is_request_end)
  {
    LockGuard lock_guard(thread_data_mutex_);
    thread_data_.is_request_end_ = is_request_end;
  }
  bool get_is_request_end() const { return thread_data_.is_request_end_; }
  obmysql::ObMySQLCmd get_mysql_cmd() const { return thread_data_.mysql_cmd_; }
  char const *get_mysql_cmd_str() const { return obmysql::get_mysql_cmd_str(thread_data_.mysql_cmd_); }
  int store_query_string(const common::ObString &stmt);
  void reset_query_string();
  void set_session_sleep();
  // for SQL entry point
  int set_session_active(const ObString &sql,
                         const int64_t query_receive_ts,
                         const int64_t last_active_time_ts,
                         obmysql::ObMySQLCmd cmd = obmysql::ObMySQLCmd::COM_QUERY);
  // for remote / px task
  int set_session_active(const ObString &label,
                         obmysql::ObMySQLCmd cmd);
  const common::ObString get_current_query_string() const;
  uint64_t get_current_statement_id() const { return thread_data_.cur_statement_id_; }
  int update_session_timeout();
  int is_timeout(bool &is_timeout);
  int is_trx_commit_timeout(transaction::ObITxCallback *&callback, int &retcode);
  int is_trx_idle_timeout(bool &is_timeout);
  int64_t get_wait_timeout() { return thread_data_.wait_timeout_; }
  int64_t get_interactive_timeout() { return thread_data_.interactive_timeout_; }
  int64_t get_max_packet_size() {return thread_data_.max_packet_size_; }
  // lock
  common::ObRecursiveMutex &get_query_lock() { return query_mutex_; }
  common::ObRecursiveMutex &get_thread_data_lock() { return thread_data_mutex_; }
  int try_lock_query() { return query_mutex_.trylock(); }
  int try_lock_thread_data() { return thread_data_mutex_.trylock(); }
  int unlock_query() { return query_mutex_.unlock(); }
  int unlock_thread_data() { return thread_data_mutex_.unlock(); }
  /// @{ system variables related:
  static int get_global_sys_variable(const ObBasicSessionInfo *session,
                                     common::ObIAllocator &calc_buf,
                                     const common::ObString &var_name,
                                     common::ObObj &val);
  static int get_global_sys_variable(uint64_t actual_tenant_id,
                                     common::ObIAllocator &calc_buf,
                                     const common::ObDataTypeCastParams &dtc_params,
                                     const common::ObString &var_name,
                                     common::ObObj &val);
  const share::ObBasicSysVar *get_sys_var(const int64_t idx) const;
  share::ObBasicSysVar *get_sys_var(const int64_t idx);
  int64_t get_sys_var_count() const { return share::ObSysVarFactory::ALL_SYS_VARS_COUNT; }
  // deserialized scene need use base_value as baseline.
  int load_default_sys_variable(const bool print_info_log, const bool is_sys_tenant, bool is_deserialized = false);
  int load_default_configs_in_pc();
  int update_query_sensitive_system_variable(share::schema::ObSchemaGetterGuard &schema_guard);
  int process_variable_for_tenant(const common::ObString &var, common::ObObj &val);
  int load_sys_variable(common::ObIAllocator &calc_buf,
                        const common::ObString &name,
                        const common::ObObj &type,
                        const common::ObObj &value,
                        const common::ObObj &min_val,
                        const common::ObObj &max_val,
                        const int64_t flags,
                        bool is_from_sys_table);
  int load_sys_variable(common::ObIAllocator &calc_buf,
                        const common::ObString &name,
                        const int64_t dtype,
                        const common::ObString &value,
                        const common::ObString &min_val,
                        const common::ObString &max_val,
                        const int64_t flags,
                        bool is_from_sys_table);
  // 将varchar类型的value、max_val、min_val转换为相应的type类型ObObj
  int cast_sys_variable(common::ObIAllocator &calc_buf,
                        bool is_range_value,
                        const share::ObSysVarClassType sys_var_id,
                        const common::ObObj &type,
                        const common::ObObj &value,
                        int64_t flags,
                        common::ObObj &out_type,
                        common::ObObj &out_value);

  ///@{ 更新系统变量的值
  // 根据kv对更新系统变量
  int update_sys_variable(const common::ObString &var, const common::ObString &val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, const common::ObObj &val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, const common::ObString &val);
  int update_sys_variable(const share::ObSysVarClassType sys_var_id, int64_t val);
  /// @note get system variables by id is prefered
  int update_sys_variable_by_name(const common::ObString &var, const common::ObObj &val);
  int update_sys_variable_by_name(const common::ObString &var, int64_t val);
  //int update_sys_variable(const char* const var, const common::ObString &v);
  ///@}

  ///@{ 获得系统变量的值
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, common::ObObj &val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, common::ObString &val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, int64_t &val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, uint64_t &val) const;
  int get_sys_variable(const share::ObSysVarClassType sys_var_id, share::ObBasicSysVar *&val) const;
  /// @note get system variables by id is prefered
  int get_sys_variable_by_name(const common::ObString &var, common::ObObj &val) const;
  int get_sys_variable_by_name(const common::ObString &var, share::ObBasicSysVar *&val) const;
  int get_sys_variable_by_name(const common::ObString &var, int64_t &val) const;
  ///@}

  /// check the existence of the system variable
  int sys_variable_exists(const common::ObString &var, bool &is_exist) const;

  int set_client_identifier(const common::ObString &client_identifier);
  const common::ObString& get_client_identifier() const { return client_identifier_; }
  common::ObString &get_client_identifier_for_update() { return client_identifier_; }
  int init_client_identifier();
  // session序列化优化
  typedef common::ObSEArray<share::ObSysVarClassType, 64> SysVarIds;
  class SysVarIncInfo
  {
    OB_UNIS_VERSION_V(1);
  public:
    SysVarIncInfo();
    virtual ~SysVarIncInfo();
    int add_sys_var_id(share::ObSysVarClassType sys_var_id);
    int get_des_sys_var_id(int64_t idx, share::ObSysVarClassType &sys_var_id) const;
    bool all_has_sys_var_id(share::ObSysVarClassType sys_var_id) const;
    int64_t all_count() const;
    const SysVarIds &get_all_sys_var_ids() const;
    int assign(const SysVarIncInfo &other);
    int reset();
    TO_STRING_KV(K(all_sys_var_ids_));
  private:
    SysVarIds all_sys_var_ids_;
  };
  static int init_sys_vars_cache_base_values();
  int load_all_sys_vars_default();
  int load_all_sys_vars(share::schema::ObSchemaGetterGuard &schema_guard);
  int load_all_sys_vars(const share::schema::ObSysVariableSchema &sys_var_schema, bool sys_var_created);
  int clean_all_sys_vars();
  SysVarIncInfo sys_var_inc_info_;
  const ObString get_cur_sql_id() const { return ObString(sql_id_); }
  void get_cur_sql_id(char *sql_id_buf, int64_t sql_id_buf_size) const;
  void set_cur_sql_id(char *sql_id);
  int set_cur_phy_plan(ObPhysicalPlan *cur_phy_plan);
  void reset_cur_phy_plan_to_null();

  void get_flt_span_id(ObString &span_id) const;
  void get_flt_trace_id(ObString &trace_id) const;
  int set_flt_span_id(ObString span_id);
  int set_flt_trace_id(ObString trace_id);
  const ObString &get_last_flt_trace_id() const;
  int set_last_flt_trace_id(const common::ObString &trace_id);
  const ObString &get_last_flt_span_id() const;
  int set_last_flt_span_id(const common::ObString &span_id);
  bool is_row_traceformat() const { return flt_vars_.row_traceformat_; }
  void set_is_row_traceformat(bool v) { flt_vars_.row_traceformat_ = v; }
  bool is_query_trc_granuality() const { return sys_vars_cache_.get_ob_enable_trace_log()?
                                            true:flt_vars_.trc_granuality_ == ObTraceGranularity::QUERY_LEVEL; }
  void set_trc_granuality(ObTraceGranularity trc_gra) { flt_vars_.trc_granuality_ = trc_gra; }
  // @pre 系统变量存在的情况下
  // @synopsis 根据变量名，取得这个变量的类型
  // @param var_name
  // @returns
  common::ObObjType get_sys_variable_type(const common::ObString &var_name) const;
  common::ObObjType get_sys_variable_meta_type(const common::ObString &var_name) const;
  // 以下helper函数是为了方便查看某系统变量的值
  int if_aggr_pushdown_allowed(bool &aggr_pushdown_allowed) const;
  int is_transformation_enabled(bool &transformation_enabled) const;
  int get_query_rewrite_enabled(int64_t &query_rewrite_enabled) const;
  int get_query_rewrite_integrity(int64_t &query_rewrite_integrity) const;
  int is_serial_set_order_forced(bool &force_set_order, bool is_oracle_mode) const;
  int is_storage_estimation_enabled(bool &storage_estimation_enabled) const;
  bool is_use_trace_log() const
  {
    return sys_vars_cache_.get_ob_enable_trace_log();
  }
  int is_use_transmission_checksum(bool &use_transmission_checksum) const;
  int is_select_index_enabled(bool &select_index_enabled) const;
  int get_name_case_mode(common::ObNameCaseMode &case_mode) const;
  int get_init_connect(common::ObString &str) const;
  int get_locale_name(common::ObString &str) const;
  /// @}

  ///@{ user variables related:
  sql::ObSessionValMap &get_user_var_val_map() {return user_var_val_map_;}
  const sql::ObSessionValMap &get_user_var_val_map() const {return user_var_val_map_;}
  int replace_user_variable(const common::ObString &var, const ObSessionVariable &val, bool need_track = true);
  int replace_user_variables(const ObSessionValMap &user_var_map);
  int remove_user_variable(const common::ObString &var);
  int get_user_variable(const common::ObString &var, ObSessionVariable &val) const;
  int get_user_variable_value(const common::ObString &var, common::ObObj &val) const;
  int get_user_variable_meta(const common::ObString &var, common::ObObjMeta &meta) const;
  const ObSessionVariable *get_user_variable(const common::ObString &var) const;
  const common::ObObj *get_user_variable_value(const common::ObString &var) const;
  bool user_variable_exists(const common::ObString &var) const;
  inline void set_need_reset_package(bool need_reset) { need_reset_package_ = need_reset; }
  bool need_reset_package() { return need_reset_package_; }
  /// @}

  inline static ObDataTypeCastParams create_dtc_params(const ObBasicSessionInfo *session_info)
  {
    return OB_NOT_NULL(session_info) ? session_info->get_dtc_params()
                                     : ObDataTypeCastParams();
  }

  inline ObDataTypeCastParams get_dtc_params() const
  {
    return ObDataTypeCastParams(get_timezone_info(),
                                get_local_nls_date_format(),
                                get_local_nls_timestamp_format(),
                                get_local_nls_timestamp_tz_format(),
                                get_nls_collation(),
                                get_nls_collation_nation(),
                                get_local_collation_connection());
  }

  inline ObCharsets4Parser get_charsets4parser() const {
    ObCharsets4Parser charsets4parser;
    charsets4parser.string_collation_ = get_local_collation_connection();
    charsets4parser.nls_collation_ = get_nls_collation();
    return charsets4parser;
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
    res.print_origin_stmt_ = true;
    return res;
  }

  // client mode related
  void set_client_mode(const common::ObClientMode mode) { client_mode_ = mode; }
  common::ObClientMode get_client_mode() const { return client_mode_; }
  bool is_java_client_mode() const { return common::OB_JAVA_CLIENT_MODE == client_mode_; }
  bool is_obproxy_mode() const { return common::OB_PROXY_CLIENT_MODE == client_mode_; }

  int64_t to_string(char *buffer, const int64_t length) const;

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
    ChangedVar() : id_(), old_val_() {}
    ChangedVar(share::ObSysVarClassType id, const ObObj& val) :
      id_(id), old_val_(val) {}
    share::ObSysVarClassType id_;
    ObObj old_val_;   // 记录下old val, 用于比较最终的值是否变化
    TO_STRING_KV(K(id_), K(old_val_));
  };
  void reset_session_changed_info();
  bool is_already_tracked(
    const share::ObSysVarClassType &sys_var_id, const common::ObIArray<ChangedVar> &array) const;
  bool is_already_tracked(
    const common::ObString &name, const common::ObIArray<common::ObString> &array) const;
  int add_changed_sys_var(const share::ObSysVarClassType &sys_var_id, const common::ObObj &old_val,
                          common::ObIArray<ChangedVar> &array);
  int add_changed_user_var(const common::ObString &name, common::ObIArray<common::ObString> &array);
  int track_sys_var(const share::ObSysVarClassType &sys_var_id, const common::ObObj &old_val);
  int track_user_var(const common::ObString &user_var);
  int remove_changed_user_var(const common::ObString &user_var);
  int is_sys_var_actully_changed(const share::ObSysVarClassType &sys_var_id,
                                 const common::ObObj &old_val,
                                 common::ObObj &new_val,
                                 bool &changed);
  inline bool is_sys_var_changed() const { return !changed_sys_vars_.empty(); }
  inline bool is_user_var_changed() const { return !changed_user_vars_.empty(); }
  inline bool is_database_changed() const { return is_database_changed_; }
  inline bool exist_client_feedback() const { return !feedback_manager_.is_empty(); }
  inline bool is_session_var_changed() const { return (is_sys_var_changed() || is_user_var_changed() || exist_client_feedback()); }
  inline bool is_session_info_changed() const { return (is_session_var_changed() || is_database_changed()); }
  const inline common::ObIArray<ChangedVar> &get_changed_sys_var() const { return changed_sys_vars_; }
  const inline common::ObIArray<common::ObString> &get_changed_user_var() const { return changed_user_vars_; }

  inline void set_capability(const obmysql::ObMySQLCapabilityFlags cap) { capability_ = cap; }
  inline void set_client_attrbuite_capability(const uint64_t cap) { client_attribute_capability_.capability_ = cap; }
  inline obmysql::ObMySQLCapabilityFlags get_capability() const { return capability_; }
  inline bool is_track_session_info() const { return capability_.cap_flags_.OB_CLIENT_SESSION_TRACK; }

  inline bool is_client_return_rowid() const
  {
    return capability_.cap_flags_.OB_CLIENT_RETURN_HIDDEN_ROWID;
  }

  inline void set_client_return_rowid(bool flag)
  {
    capability_.cap_flags_.OB_CLIENT_RETURN_HIDDEN_ROWID = (flag ? 1 : 0);
  }

  inline bool is_client_use_lob_locator() const
  {
    return capability_.cap_flags_.OB_CLIENT_USE_LOB_LOCATOR;
  }

  inline bool is_client_support_lob_locatorv2() const
  {
    return client_attribute_capability_.cap_flags_.OB_CLIENT_CAP_OB_LOB_LOCATOR_V2;
  }

  void set_proxy_cap_flags(const obmysql::ObProxyCapabilityFlags &proxy_capability)
  {
    proxy_capability_ = proxy_capability;
  }
  obmysql::ObProxyCapabilityFlags get_proxy_cap_flags() const { return proxy_capability_; }
  inline bool is_abundant_feedback_support() const
  {
    return is_track_session_info() && proxy_capability_.is_abundant_feedback_support();
  }

  //TODO::@yuming, as enable_transmission_checksum is global variables,
  //here we no need get_session for is_enable_transmission_checksum()
  inline bool is_enable_transmission_checksum() const { return true; }

  inline share::ObFeedbackManager &get_feedback_manager () { return feedback_manager_; }
  inline int set_follower_first_feedback(const share::ObFollowerFirstFeedbackType type);

  inline common::ObIAllocator &get_allocator() { return changed_var_pool_; }
  // TODO: piece cache use this allocator for now, not property, need remove later.
  inline common::ObIAllocator &get_session_allocator() { return block_allocator_; }
  inline common::ObIAllocator &get_extra_info_alloc() { return extra_info_allocator_; }

  inline common::ObIAllocator &get_cursor_allocator() { return cursor_info_allocator_; }
  inline common::ObIAllocator &get_package_allocator() { return package_info_allocator_; }

  int set_partition_hit(const bool is_hit);
  int set_proxy_user_privilege(const int64_t user_priv_set);
  int set_proxy_capability(const uint64_t proxy_cap);
  int set_trans_specified(const bool is_spec);
  int set_init_connect(const common::ObString &init_sql);
  int save_trans_status();
  // 重置事务相关变量
  virtual void reset_tx_variable(bool reset_next_scope = true);
  transaction::ObTxIsolationLevel get_tx_isolation() const;
  bool is_isolation_serializable() const;
  void set_tx_isolation(transaction::ObTxIsolationLevel isolation);
  bool get_tx_read_only() const;
  void set_tx_read_only(const bool last_tx_read_only, const bool cur_tx_read_only);
  int reset_tx_variable_if_remote_trans(const ObPhyPlanType& type);
  int check_tx_read_only_privilege(const ObSqlTraits &sql_traits);
  int get_group_concat_max_len(uint64_t &group_concat_max_len) const;
  int get_ob_interm_result_mem_limit(int64_t &ob_interm_result_mem_limit) const;
  int get_ob_org_cluster_id(int64_t &ob_org_cluster_id) const
  {
    ob_org_cluster_id = sys_vars_cache_.get_ob_org_cluster_id();
    return common::OB_SUCCESS;
  }
  // 参数max_allowed_pkt和net_buffer_len的命名之所以不为max_allowed_packet和net_buffer_length，
  // 是为了规避lib/regex/include/mysql.h中的命名重复，使得编译能通过
  int get_max_allowed_packet(int64_t &max_allowed_pkt) const;
  int get_net_buffer_length(int64_t &net_buffer_len) const;
  /// @}
  int64_t get_session_info_mem_size() const { return block_allocator_.get_total_mem_size(); }
  ObPartitionHitInfo &partition_hit() { return partition_hit_; } // 和上面的set_partition_hit没有任何关系
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
  void set_mark_killed(bool is_mark_killed) { ATOMIC_STORE(&thread_data_.is_mark_killed_, is_mark_killed); }
  bool is_mark_killed() { return ATOMIC_LOAD(&thread_data_.is_mark_killed_);  }
  uint32_t get_magic_num() {return magic_num_;}
  int64_t get_current_execution_id() const { return current_execution_id_; }
  const common::ObCurTraceId::TraceId &get_last_trace_id() const { return last_trace_id_; }
  const common::ObCurTraceId::TraceId &get_current_trace_id() const { return curr_trace_id_; }
  uint64_t get_current_plan_id() const { return plan_id_; }
  uint64_t get_last_plan_id() const { return last_plan_id_; }
  void set_last_plan_id(uint64_t plan_id) { last_plan_id_ = plan_id; }
  void set_current_execution_id(int64_t execution_id) { current_execution_id_ = execution_id; }
  void set_last_trace_id(common::ObCurTraceId::TraceId *trace_id)
  {
    if (OB_ISNULL(trace_id)) {
    } else {
      last_trace_id_ = *trace_id;
    }
  }
  void set_current_trace_id(common::ObCurTraceId::TraceId *trace_id)
  {
    if (OB_ISNULL(trace_id)) {
    } else {
      curr_trace_id_ = *trace_id;
    }
  }
  // forbid use jit
  int get_jit_enabled_mode(ObJITEnableMode &jit_mode) const
  {
    jit_mode = ObJITEnableMode::OFF;
    return common::OB_SUCCESS;
  }

  bool get_enable_exact_mode() const
  {
    return sys_vars_cache_.get_cursor_sharing_mode() == ObCursorSharingMode::EXACT_MODE;
  }

  int64_t get_runtime_filter_type() const { return sys_vars_cache_.get_runtime_filter_type(); }
  int64_t get_runtime_filter_wait_time_ms() const { return sys_vars_cache_.get_runtime_filter_wait_time_ms(); }
  int64_t get_runtime_filter_max_in_num() const { return sys_vars_cache_.get_runtime_filter_max_in_num(); }
  int64_t get_runtime_bloom_filter_max_size() const { return sys_vars_cache_.get_runtime_bloom_filter_max_size(); }


  const ObString &get_app_trace_id() const { return app_trace_id_; }
  void set_app_trace_id(common::ObString trace_id) {
    app_trace_id_.assign_ptr(trace_id.ptr(), trace_id.length());
  }
  // update trace_id in sys variables and  will bing to client
  int update_last_trace_id(const ObCurTraceId::TraceId &trace_id);

  int set_partition_location_feedback(const share::ObFBPartitionParam &param);
  int get_auto_increment_cache_size(int64_t &auto_increment_cache_size);
  void set_curr_trans_last_stmt_end_time(int64_t t) { curr_trans_last_stmt_end_time_ = t; }
  int64_t get_curr_trans_last_stmt_end_time() const { return curr_trans_last_stmt_end_time_; }

  // for SESSION_SYNC_SYS_VAR serialize and deserialize.
  int serialize_sync_sys_vars(common::ObIArray<share::ObSysVarClassType> &sys_var_delta_ids, char *buf, const int64_t &buf_len, int64_t &pos);
  int deserialize_sync_sys_vars(int64_t &deserialize_sys_var_count, const char *buf, const int64_t &data_len, int64_t &pos, bool is_error_sync = false);
  int deserialize_sync_error_sys_vars(int64_t &deserialize_sys_var_count, const char *buf, const int64_t &data_len, int64_t &pos);
  int sync_default_sys_vars(SysVarIncInfo &tmp_sys_var_inc_info, bool &is_influence_plan_cache_sys_var);
  int get_sync_sys_vars(common::ObIArray<share::ObSysVarClassType> &sys_var_delta_ids) const;
  int get_error_sync_sys_vars(ObIArray<share::ObSysVarClassType> &sys_var_delta_ids) const;
  int get_sync_sys_vars_size(common::ObIArray<share::ObSysVarClassType> &sys_var_delta_ids, int64_t &len) const;
  bool is_sync_sys_var(share::ObSysVarClassType sys_var_id) const;
  bool is_exist_error_sync_var(share::ObSysVarClassType sys_var_id) const;
  // record session state from active to anothe state. for record total_cpu_time.
  bool is_active_state_change(ObSQLSessionState last_state, ObSQLSessionState curr_state) {
    if (last_state == QUERY_ACTIVE && curr_state != QUERY_ACTIVE) {
      return true;
    } else {
      return false;
    }
  }

  // nested session and sql execute for foreign key.
  bool is_nested_session() const { return nested_count_ > 0; }
  int64_t get_nested_count() const { return nested_count_; }
  void set_nested_count(int64_t nested_count) { nested_count_ = nested_count; }
  int save_base_session(BaseSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int restore_base_session(BaseSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int save_basic_session(StmtSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int restore_basic_session(StmtSavedValue &saved_value);
  int begin_nested_session(StmtSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int end_nested_session(StmtSavedValue &saved_value);
  int begin_autonomous_session(TransSavedValue &saved_value);
  int end_autonomous_session(TransSavedValue &saved_value);
  int init_stmt_tables();
  int merge_stmt_tables();
  int skip_mutating(uint64_t table_id, stmt::StmtType &saved_stmt_type);
  int restore_mutating(uint64_t table_id, stmt::StmtType saved_stmt_type);
  int set_start_stmt();
  int set_end_stmt();
  bool has_start_stmt() { return nested_count_ >= 0; }
  bool is_fast_select() const { return false; }

  bool is_server_status_in_transaction() const;

  bool has_explicit_start_trans() const { return tx_desc_ != NULL && tx_desc_->is_explicit(); }
  bool is_in_transaction() const { return tx_desc_ != NULL && tx_desc_->is_in_tx(); }
  bool has_active_autocommit_trans(transaction::ObTransID &trans_id);
  virtual bool is_txn_free_route_temp() const { return false; }
  bool get_in_transaction() const { return is_in_transaction(); }
  uint64_t get_trans_flags() const { return trans_flags_.get_flags(); }
  void set_has_exec_inner_dml(bool value) { trans_flags_.set_has_exec_inner_dml(value); }
  bool has_exec_inner_dml() const { return trans_flags_.has_exec_inner_dml(); }
  void set_is_in_user_scope(bool value) { sql_scope_flags_.set_is_in_user_scope(value); }
  bool is_in_user_scope() const { return sql_scope_flags_.is_in_user_scope(); }
  SqlScopeFlags &get_sql_scope_flags() { return sql_scope_flags_; }
  share::SCN get_reserved_snapshot_version() const { return reserved_read_snapshot_version_; }
  void set_reserved_snapshot_version(const share::SCN snapshot_version) { reserved_read_snapshot_version_ = snapshot_version; }
  void reset_reserved_snapshot_version() { reserved_read_snapshot_version_.reset(); }

  bool get_check_sys_variable() { return check_sys_variable_; }
  void set_check_sys_variable(bool check_sys_variable) { check_sys_variable_ = check_sys_variable; }
  bool is_acquire_from_pool() const { return acquire_from_pool_; }
  void set_acquire_from_pool(bool acquire_from_pool) { acquire_from_pool_ = acquire_from_pool; }
  bool can_release_to_pool() const { return release_to_pool_; }
  void set_release_from_pool(bool release_to_pool) { release_to_pool_ = release_to_pool; }
  bool is_tenant_killed() { return ATOMIC_LOAD(&is_tenant_killed_) > 0; }
  void set_tenant_killed() { ATOMIC_STORE(&is_tenant_killed_, 1); }
  bool is_use_inner_allocator() const;
  int64_t get_reused_count() const { return reused_count_; }
  inline void set_first_need_txn_stmt_type(stmt::StmtType stmt_type)
  {
    if (stmt::T_NONE == first_need_txn_stmt_type_) {
      first_need_txn_stmt_type_ = stmt_type;
    }
  }
  inline void reset_first_need_txn_stmt_type() { first_need_txn_stmt_type_ = stmt::T_NONE; }
  inline stmt::StmtType get_first_need_txn_stmt_type() const { return first_need_txn_stmt_type_; }
  inline void set_need_recheck_txn_readonly(bool need) { need_recheck_txn_readonly_ = need; }
  inline bool need_recheck_txn_readonly() const { return need_recheck_txn_readonly_; }
  void set_stmt_type(stmt::StmtType stmt_type) { stmt_type_ = stmt_type; }
  stmt::StmtType get_stmt_type() const { return stmt_type_; }

  int get_session_label(uint64_t policy_id, share::ObLabelSeSessionLabel &session_label) const;
  bool is_password_expired() const { return is_password_expired_; }
  void set_password_expired(bool value) { is_password_expired_ = value; }
  int64_t get_process_query_time() const { return process_query_time_; }
  void set_process_query_time(int64_t time) { process_query_time_ = time; }
  inline void set_client_sessid_support(bool is_client_sessid_support)
              { is_client_sessid_support_ = is_client_sessid_support; }
  inline bool is_client_sessid_support() { return is_client_sessid_support_; }
  inline void set_feedback_proxy_info_support(const bool is_feedback_proxy_info_support) { is_feedback_proxy_info_support_ = is_feedback_proxy_info_support; }
  inline bool is_feedback_proxy_info_support() { return is_feedback_proxy_info_support_; }
  int replace_new_session_label(uint64_t policy_id, const share::ObLabelSeSessionLabel &new_session_label);
  int set_enable_role_ids(const ObIArray<uint64_t>& role_ids);
  int load_default_sys_variable(common::ObIAllocator &allocator, int64_t var_idx);

  int set_session_temp_table_used(const bool is_used);
  int get_session_temp_table_used(bool &is_used) const;
  int get_enable_optimizer_null_aware_antijoin(bool &is_enabled) const;
  common::ActiveSessionStat &get_ash_stat() {  return ash_stat_; }
  void update_tenant_config_version(int64_t v) { cached_tenant_config_version_ = v; };
  static int check_optimizer_features_enable_valid(const ObObj &val);
  int get_compatibility_control(share::ObCompatType &compat_type) const;
  int get_compatibility_version(uint64_t &compat_version) const;
  int get_security_version(uint64_t &security_version) const;
  int check_feature_enable(const share::ObCompatFeatureType feature_type, bool &is_enable) const;
  void trace_all_sys_vars() const;
protected:
  int process_session_variable(share::ObSysVarClassType var, const common::ObObj &value,
                               const bool check_timezone_valid = true,
                               const bool is_update_sys_var = false);
  int process_session_variable_fast();
  //@brief process session log_level setting like 'all.*:info, sql.*:debug'.
  //int process_session_ob_binlog_row_image(const common::ObObj &value);
  int process_session_log_level(const common::ObObj &val);
  int process_session_sql_mode_value(const common::ObObj &value);
  int process_session_compatibility_mode_value(const ObObj &value);
  int process_session_time_zone_value(const common::ObObj &value, const bool check_timezone_valid);
  int process_session_overlap_time_value(const ObObj &value);
  int process_session_autocommit_value(const common::ObObj &val);
  int process_session_debug_sync(const common::ObObj &val, const bool is_global,
                                const bool is_update_sys_var);
  // session切换接口
  int base_save_session(BaseSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int base_restore_session(BaseSavedValue &saved_value);
  int stmt_save_session(StmtSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int stmt_restore_session(StmtSavedValue &saved_value);
  int trans_save_session(TransSavedValue &saved_value);
  int trans_restore_session(TransSavedValue &saved_value);
protected:
  // because the OB_MALLOC_NORMAL_BLOCK_SIZE block in ObPool has a BlockHeader(8 Bytes),
  // so the total mem avail is (OB_MALLOC_NORMAL_BLOCK_SIZE - 8), if we divide it by (4 * 1204),
  // the last (4 * 1024 - 8) is wasted.
  // and if we divide it by (4 * 1024 - 8), almost all mem can be used.
  static const int64_t SMALL_BLOCK_SIZE = 4 * 1024LL - 8;
private:
  //*************** reset系列函数，不允许外部调用，防止误用或漏用
  // 外部统一调用reset_tx_variable()
  void reset_tx_read_only();
  void reset_tx_isolation();
  void reset_trans_flags() { trans_flags_.reset(); }
  void clear_app_trace_id() { app_trace_id_.reset(); }
  //***************

  int dump_all_sys_vars() const;
  static int change_value_for_special_sys_var(const common::ObString &sys_var_name,
                                              const common::ObObj &ori_val,
                                              common::ObObj &new_val);
  static int change_value_for_special_sys_var(const share::ObSysVarClassType sys_var_id,
                                              const common::ObObj &ori_val,
                                              common::ObObj &new_val);
  int get_int64_sys_var(const share::ObSysVarClassType sys_var_id, int64_t &int64_val) const;
  int get_uint64_sys_var(const share::ObSysVarClassType sys_var_id, uint64_t &uint64_val) const;
  int get_bool_sys_var(const share::ObSysVarClassType sys_var_id, bool &bool_val) const;
  int get_charset_sys_var(share::ObSysVarClassType sys_var_id, common::ObCharsetType &cs_type) const;
  int get_collation_sys_var(share::ObSysVarClassType sys_var_id, common::ObCollationType &coll_type) const;
  int get_string_sys_var(share::ObSysVarClassType sys_var_id, common::ObString &str) const;
  int create_sys_var(share::ObSysVarClassType sys_var_id, int64_t store_idx, share::ObBasicSysVar *&sys_var);
//  int store_sys_var(int64_t store_idx, share::ObBasicSysVar *sys_var);
  int inner_get_sys_var(const common::ObString &sys_var_name, int64_t &store_idx, share::ObBasicSysVar *&sys_var) const;
  int inner_get_sys_var(const share::ObSysVarClassType sys_var_id, int64_t &store_idx, share::ObBasicSysVar *&sys_var) const;
  int inner_get_sys_var(const common::ObString &sys_var_name, share::ObBasicSysVar *&sys_var) const
  {
    int64_t store_idx = -1;
    return inner_get_sys_var(sys_var_name, store_idx, sys_var);
  }
  int inner_get_sys_var(const share::ObSysVarClassType sys_var_id, share::ObBasicSysVar *&sys_var) const
  {
    int64_t store_idx = -1;
    return inner_get_sys_var(sys_var_id, store_idx, sys_var);
  }
  int update_session_sys_variable(ObExecContext &ctx,
                                  const common::ObString &var,
                                  const common::ObObj &val);
  int calc_need_serialize_vars(common::ObIArray<share::ObSysVarClassType> &sys_var_ids,
                               common::ObIArray<common::ObString> &user_var_names) const;
  int deep_copy_sys_variable(share::ObBasicSysVar &sys_var,
                             const share::ObSysVarClassType sys_var_id,
                             const common::ObObj &src_val);
  int defragment_sys_variable_from(ObArray<std::pair<int64_t, ObObj>> &tmp_value);
  void defragment_sys_variable_to(ObArray<std::pair<int64_t, ObObj>> &tmp_value);
  int deep_copy_trace_id_var(const common::ObObj &src_val,
                             common::ObObj *dest_val_ptr);
  inline int store_query_string_(const ObString &stmt);
  inline int set_session_state_(ObSQLSessionState state);
  //写入系统变量的默认值, deserialized scene need use base_value as baseline.
  int init_system_variables(const bool print_info_log, const bool is_sys_tenant, bool is_deserialized = false);
protected:
  //============注意：下面的成员变量使用时，需要考虑并发控制================================
  struct MultiThreadData
  {
    const static int64_t DEFAULT_MAX_PACKET_SIZE = 1048576;
    MultiThreadData () : user_name_(),
                         host_name_(),
                         client_ip_(),
                         user_at_host_name_(),
                         user_at_client_ip_(),
                         peer_addr_(),
                         client_addr_(),
                         user_client_addr_(),
                         cur_query_buf_len_(0),
                         cur_query_(nullptr),
                         cur_query_len_(0),
                         cur_statement_id_(0),
                         last_active_time_(0),
                         dis_state_(CLIENT_FORCE_DISCONNECT),
                         state_(SESSION_SLEEP),
                         is_interactive_(false),
                         sock_desc_(),
                         mysql_cmd_(obmysql::COM_SLEEP),
                         cur_query_start_time_(0),
                         cur_state_start_time_(0),
                         wait_timeout_(0),
                         interactive_timeout_(0),
                         max_packet_size_(MultiThreadData::DEFAULT_MAX_PACKET_SIZE),
                         is_shadow_(false),
                         is_in_retry_(SESS_NOT_IN_RETRY),
                         client_addr_port_(0),
                         is_mark_killed_(false),
                         proxy_user_name_(),
                         proxy_host_name_(),
                         retry_active_time_(0),
                         is_request_end_(true)
    {
      CHAR_CARRAY_INIT(database_name_);
    }
    void reset(bool begin_nested_session = false)
    {
      if (!begin_nested_session) {
        // TODO(jiuren): move some thing here.
      }
      user_name_.reset();
      host_name_.reset();
      client_ip_.reset();
      user_at_host_name_.reset();
      user_at_client_ip_.reset();
      CHAR_CARRAY_INIT(database_name_);
      peer_addr_.reset();
      client_addr_.reset();
      user_client_addr_.reset();
      if (cur_query_ != nullptr) {
        cur_query_[0] = '\0';
      }
      cur_query_len_ = 0;
      cur_statement_id_ = 0;
      last_active_time_ = 0;
      dis_state_ = CLIENT_FORCE_DISCONNECT;
      state_ = SESSION_SLEEP;
      is_interactive_ = false;
      sock_desc_.clear_sql_session_info();
      sock_desc_.reset();
      mysql_cmd_ = obmysql::COM_SLEEP;
      cur_query_start_time_ = 0;
      cur_state_start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      wait_timeout_ = 0;
      interactive_timeout_ = 0;
      max_packet_size_ = MultiThreadData::DEFAULT_MAX_PACKET_SIZE;
      is_shadow_ = false;
      is_in_retry_ = SESS_NOT_IN_RETRY;
      client_addr_port_ = 0;
      is_mark_killed_ = false;
      proxy_user_name_.reset();
      proxy_host_name_.reset();
      retry_active_time_ = 0;
      is_request_end_ = true;
    }
    ~MultiThreadData ()
    {
    }
    common::ObString user_name_;    //current user name
    common::ObString host_name_;    //current user host name
    common::ObString client_ip_;    //current user real client host name
    common::ObString user_at_host_name_;    //current user@host, for current_user()
    common::ObString user_at_client_ip_;    //current user@clientip, for user()
    char database_name_[common::OB_MAX_DATABASE_NAME_BUF_LENGTH * OB_MAX_CHAR_LEN];  //default database
    // 假设以下场景：用户从机器 A 通过代理机器 B 发送到机器 C 上，然后再次发送到 D 上。
    // 则 user_client_addr 为 A 机器地址信息，保持不变；client_addr 为 代理机器 B 地址，保持不变。
    // peer_addr 依次为 C/D 机器地址，随着深度增加而改变；svr_addr(未此处记录)为最终值执行机器 D，保持不变。
    common::ObAddr peer_addr_;
    common::ObAddr client_addr_;
    common::ObAddr user_client_addr_;
    int64_t cur_query_buf_len_;
    char *cur_query_;
    volatile int64_t cur_query_len_;
    uint64_t cur_statement_id_;
    int64_t last_active_time_;
    ObDisconnectState dis_state_;
    ObSQLSessionState state_;
    bool is_interactive_;
    rpc::ObSqlSockDesc sock_desc_;
    obmysql::ObMySQLCmd mysql_cmd_;
    int64_t cur_query_start_time_;
    int64_t cur_state_start_time_;
    int64_t wait_timeout_;
    int64_t interactive_timeout_;
    int64_t max_packet_size_;
    bool is_shadow_;
    ObSessionRetryStatus is_in_retry_;//标识当前session是否处于query retry的状态
    int32_t client_addr_port_; // Record client address port.
    bool is_mark_killed_; // Mark the current session as delayed kill
    common::ObString proxy_user_name_;
    common::ObString proxy_host_name_;
    // In the retry scenario, record the cumulative active time except the current state,
    // and use it to count the CPU time. For example, 1. The current request status is Sleep,
    // waiting for retry, it will record the cumulative time of Active during previous execution.
    // 2. The current request status is Active, and it is retrying. It will ignore the active time
    // of the current status and record the cumulative time of Active during previous execution.
    int64_t retry_active_time_;
    bool is_request_end_; // This flag is used to distinguish whether the current request is over.
  };

public:
  //为了性能做的系统变量缓存值
  class SysVarsCacheData
  {
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
        default_password_lifetime_(0),
        tx_read_only_(false),
        ob_enable_pl_cache_(false),
        ob_enable_plan_cache_(false),
        optimizer_use_sql_plan_baselines_(false),
        optimizer_capture_sql_plan_baselines_(false),
        is_result_accurate_(false),
        ob_enable_transmission_checksum_(false),
        character_set_results_(ObCharsetType::CHARSET_INVALID),
        character_set_connection_(ObCharsetType::CHARSET_INVALID),
        ob_enable_jit_(ObJITEnableMode::OFF),
        cursor_sharing_mode_(ObCursorSharingMode::FORCE_MODE),
        timestamp_(0),
        tx_isolation_(transaction::ObTxIsolationLevel::INVALID),
        iso_nls_currency_(),
        ob_pl_block_timeout_(0),
        log_row_value_option_(),
        default_lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
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
        ob_trace_info_(),
        ob_plsql_ccflags_(),
        ob_max_read_stale_time_(0),
        runtime_filter_type_(0),
        runtime_filter_wait_time_ms_(0),
        runtime_filter_max_in_num_(0),
        runtime_bloom_filter_max_size_(INT_MAX32),
        enable_rich_vector_format_(false),
        ncharacter_set_connection_(ObCharsetType::CHARSET_INVALID),
        compat_type_(share::ObCompatType::COMPAT_MYSQL57),
        compat_version_(0),
        enable_sql_plan_monitor_(false)
    {
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        MEMSET(nls_formats_buf_[i], 0, MAX_NLS_FORMAT_STR_LEN);
      }
    }
    ~SysVarsCacheData() {}

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
      default_password_lifetime_ = 0;
      tx_read_only_ = false;
      ob_enable_pl_cache_ = false;
      ob_enable_plan_cache_ = false;
      optimizer_use_sql_plan_baselines_ = false;
      optimizer_capture_sql_plan_baselines_ = false;
      is_result_accurate_ = false;
      ob_enable_transmission_checksum_ = false;
      character_set_results_ = ObCharsetType::CHARSET_INVALID;
      character_set_connection_ = ObCharsetType::CHARSET_INVALID;
      ob_enable_jit_ = ObJITEnableMode::OFF;
      cursor_sharing_mode_ = ObCursorSharingMode::FORCE_MODE;
      timestamp_ = 0;
      tx_isolation_ = transaction::ObTxIsolationLevel::INVALID;
      ob_pl_block_timeout_ = 0;
      ob_plsql_ccflags_.reset();
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
      iso_nls_currency_.reset();
      ob_plsql_ccflags_.reset();
      log_row_value_option_.reset();
      ob_max_read_stale_time_ = 0;
      runtime_filter_type_ = 0;
      runtime_filter_wait_time_ms_ = 0;
      runtime_filter_max_in_num_ = 0;
      runtime_bloom_filter_max_size_ = INT32_MAX;
      enable_rich_vector_format_ = false;
      ncharacter_set_connection_ = ObCharsetType::CHARSET_INVALID;
      default_lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
      compat_type_ = share::ObCompatType::COMPAT_MYSQL57;
      compat_version_ = 0;
      enable_sql_plan_monitor_ = false;
    }

    inline bool operator==(const SysVarsCacheData &other) const {
      bool equal1 =  auto_increment_increment_ == other.auto_increment_increment_ &&
            sql_throttle_current_priority_ == other.sql_throttle_current_priority_ &&
            ob_last_schema_version_ == other.ob_last_schema_version_ &&
            sql_select_limit_ == other.sql_select_limit_ &&
            auto_increment_offset_ == other.auto_increment_offset_ &&
            last_insert_id_ == other.last_insert_id_ &&
            binlog_row_image_ == other.binlog_row_image_ &&
            foreign_key_checks_ == other.foreign_key_checks_ &&
            default_password_lifetime_ == other.default_password_lifetime_ &&
            tx_read_only_ == other.tx_read_only_ &&
            ob_enable_pl_cache_ == other.ob_enable_pl_cache_ &&
            ob_enable_plan_cache_ == other.ob_enable_plan_cache_ &&
            optimizer_use_sql_plan_baselines_ == other.optimizer_use_sql_plan_baselines_ &&
            optimizer_capture_sql_plan_baselines_ == other.optimizer_capture_sql_plan_baselines_ &&
            is_result_accurate_ == other.is_result_accurate_ &&
            ob_enable_transmission_checksum_ == other.ob_enable_transmission_checksum_ &&
            character_set_results_ == other.character_set_results_ &&
            character_set_connection_ == other.character_set_connection_ &&
            ob_enable_jit_ == other.ob_enable_jit_ &&
            cursor_sharing_mode_ == other.cursor_sharing_mode_ &&
            timestamp_ == other.timestamp_ &&
            tx_isolation_ == other.tx_isolation_ &&
            ob_pl_block_timeout_ == other.ob_pl_block_timeout_ &&
            ob_plsql_ccflags_ == other.ob_plsql_ccflags_ &&
            autocommit_ == other.autocommit_ &&
            ob_org_cluster_id_ == other.ob_org_cluster_id_ &&
            ob_query_timeout_ == other.ob_query_timeout_ &&
            ob_trx_timeout_ == other.ob_trx_timeout_ &&
            collation_connection_ == other.collation_connection_ &&
            ob_enable_sql_audit_ == other.ob_enable_sql_audit_ &&
            nls_length_semantics_ == other.nls_length_semantics_ &&
            sql_mode_ == other.sql_mode_ &&
            ob_trx_idle_timeout_ == other.ob_trx_idle_timeout_ &&
            ob_trx_lock_timeout_ == other.ob_trx_lock_timeout_ &&
            nls_collation_ == other.nls_collation_ &&
            nls_nation_collation_ == other.nls_nation_collation_ &&
            ob_trace_info_ == other.ob_trace_info_ &&
            iso_nls_currency_ == other.iso_nls_currency_ &&
            ob_plsql_ccflags_ == other.ob_plsql_ccflags_ &&
            log_row_value_option_ == other.log_row_value_option_ &&
            ob_max_read_stale_time_ == other.ob_max_read_stale_time_ &&
            ob_max_read_stale_time_ == other.ob_max_read_stale_time_  &&
            enable_rich_vector_format_ == other.enable_rich_vector_format_ &&
            ncharacter_set_connection_ == other.ncharacter_set_connection_ &&
            default_lob_inrow_threshold_ == other.default_lob_inrow_threshold_ &&
            compat_type_ == other.compat_type_ &&
            compat_version_ == other.compat_version_;
      bool equal2 = true;
      for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
        if (nls_formats_[i] != other.nls_formats_[i]) {
          equal2 = false;
        }
      }
      return equal1 && equal2;
    }
    void set_nls_date_format(const common::ObString &format)
    {
      set_nls_format(NLS_DATE, format);
    }
    void set_nls_timestamp_format(const common::ObString &format)
    {
      set_nls_format(NLS_TIMESTAMP, format);
    }
    void set_nls_timestamp_tz_format(const common::ObString &format)
    {
      set_nls_format(NLS_TIMESTAMP_TZ, format);
    }
    void set_nls_format(const int64_t enum_value, const common::ObString &format)
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
    void set_iso_nls_currency(const common::ObString &format)
    {
      if (format.empty()) {
        iso_nls_currency_.reset();
      } else {
        MEMCPY(iso_nls_currency_buf_, format.ptr(), format.length());
        iso_nls_currency_.assign_ptr(iso_nls_currency_buf_, format.length());
      }
    }
    const common::ObString &get_nls_date_format() const
    {
      return nls_formats_[NLS_DATE];
    }
    const common::ObString &get_nls_timestamp_format() const
    {
      return nls_formats_[NLS_TIMESTAMP];
    }
    const common::ObString &get_nls_timestamp_tz_format() const
    {
      return nls_formats_[NLS_TIMESTAMP_TZ];
    }
    void set_ob_trace_info(const common::ObString &trace_info)
    {
      if (trace_info.empty()) {
        ob_trace_info_.reset();
      } else {
        const int32_t trace_len = std::min(trace_info.length(), OB_TRACE_BUFFER_SIZE);
        MEMCPY(trace_info_buf_, trace_info.ptr(), trace_len);
        ob_trace_info_.assign_ptr(trace_info_buf_, trace_len);
      }
    }
    const common::ObString &get_ob_trace_info() const
    {
      return ob_trace_info_;
    }
    const common::ObString &get_iso_nls_currency() const
    {
      return iso_nls_currency_;
    }
    void set_plsql_ccflags(const common::ObString &plsql_ccflags)
    {
      if (plsql_ccflags.empty()) {
        ob_plsql_ccflags_.reset();
      } else {
        const int32_t ccflags_len
          = std::min(plsql_ccflags.length(), OB_TMP_BUF_SIZE_256);
        MEMCPY(plsql_ccflags_, plsql_ccflags.ptr(), ccflags_len);
        ob_plsql_ccflags_.assign_ptr(plsql_ccflags_, ccflags_len);
      }
    }
    const common::ObString &get_plsql_ccflags() const
    {
      return ob_plsql_ccflags_;
    }
    void set_log_row_value_option(const common::ObString &option)
    {
      if (option.empty()) {
        log_row_value_option_.reset();
      } else {
        MEMCPY(log_row_value_option_buf_, option.ptr(), option.length());
        log_row_value_option_.assign_ptr(log_row_value_option_buf_, option.length());
      }
    }
    void set_default_lob_inrow_threshold(const int64_t default_lob_inrow_threshold)
    {
      default_lob_inrow_threshold_ = default_lob_inrow_threshold;
    }
    const common::ObString &get_log_row_value_option() const
    {
      return log_row_value_option_;
    }
    int64_t get_default_lob_inrow_threshold() const
    {
      return default_lob_inrow_threshold_;
    }

    TO_STRING_KV(K(autocommit_), K(ob_enable_trace_log_), K(ob_enable_sql_audit_), K(nls_length_semantics_),
                 K(ob_org_cluster_id_), K(ob_query_timeout_), K(ob_trx_timeout_), K(collation_connection_),
                 K(sql_mode_), K(nls_formats_[0]), K(nls_formats_[1]), K(nls_formats_[2]),
                 K(ob_trx_idle_timeout_), K(ob_trx_lock_timeout_), K(nls_collation_), K(nls_nation_collation_),
                 K_(sql_throttle_current_priority), K_(ob_last_schema_version), K_(sql_select_limit),
                 K_(optimizer_use_sql_plan_baselines), K_(optimizer_capture_sql_plan_baselines),
                 K_(is_result_accurate), K_(character_set_results),
                 K_(character_set_connection), K_(ob_pl_block_timeout), K_(ob_plsql_ccflags),
                 K_(iso_nls_currency), K_(log_row_value_option), K_(ob_max_read_stale_time), K_(default_lob_inrow_threshold));
  public:
    static const int64_t MAX_NLS_FORMAT_STR_LEN = 256;

    //==========  不需序列化  ============
    uint64_t auto_increment_increment_;
    int64_t sql_throttle_current_priority_;
    int64_t ob_last_schema_version_;
    int64_t sql_select_limit_;
    uint64_t auto_increment_offset_;
    uint64_t last_insert_id_;
    int64_t binlog_row_image_;
    int64_t foreign_key_checks_;
    uint64_t default_password_lifetime_;
    bool tx_read_only_;
    bool ob_enable_pl_cache_;
    bool ob_enable_plan_cache_;
    bool optimizer_use_sql_plan_baselines_;
    bool optimizer_capture_sql_plan_baselines_;
    bool is_result_accurate_;
    bool ob_enable_transmission_checksum_;
    ObCharsetType character_set_results_;
    ObCharsetType character_set_connection_;
    ObJITEnableMode ob_enable_jit_;
    ObCursorSharingMode cursor_sharing_mode_;

    int64_t timestamp_;
    transaction::ObTxIsolationLevel tx_isolation_;

    common::ObString iso_nls_currency_;
    char iso_nls_currency_buf_[MAX_NLS_FORMAT_STR_LEN];
    int64_t ob_pl_block_timeout_;

    common::ObString log_row_value_option_;
    char log_row_value_option_buf_[OB_TMP_BUF_SIZE_256];
    int64_t default_lob_inrow_threshold_;

    //==========  需要序列化  ============
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
    ObCollationType nls_collation_; //for oracle char and varchar2
    ObCollationType nls_nation_collation_; //for oracle nchar and nvarchar2
    ObString ob_trace_info_; // identifier from user app, pass through system including app & db
    char trace_info_buf_[OB_TRACE_BUFFER_SIZE];
    ObString ob_plsql_ccflags_;
    char plsql_ccflags_[OB_TMP_BUF_SIZE_256];
    int64_t ob_max_read_stale_time_;
    int64_t runtime_filter_type_;
    int64_t runtime_filter_wait_time_ms_;
    int64_t runtime_filter_max_in_num_;
    int64_t runtime_bloom_filter_max_size_;
    bool enable_rich_vector_format_;
    ObCharsetType ncharacter_set_connection_;
    share::ObCompatType compat_type_;
    uint64_t compat_version_;
    // No use. Placeholder.
    bool enable_sql_plan_monitor_;
  private:
    char nls_formats_buf_[ObNLSFormatEnum::NLS_MAX][MAX_NLS_FORMAT_STR_LEN];
  };
private:
#define DEF_SYS_VAR_CACHE_FUNCS(SYS_VAR_TYPE, SYS_VAR_NAME)                           \
  void set_##SYS_VAR_NAME(SYS_VAR_TYPE value)                                         \
  {                                                                                   \
    inc_data_.SYS_VAR_NAME##_ = (value);                                              \
    inc_##SYS_VAR_NAME##_ = true;                                                     \
  }                                                                                   \
  void set_base_##SYS_VAR_NAME(SYS_VAR_TYPE value)                                    \
  {                                                                                   \
    base_data_.SYS_VAR_NAME##_ = (value);                                             \
  }                                                                                   \
  const SYS_VAR_TYPE &get_##SYS_VAR_NAME() const                                      \
  {                                                                                   \
    return get_##SYS_VAR_NAME(inc_##SYS_VAR_NAME##_);                                 \
  }                                                                                   \
  const SYS_VAR_TYPE &get_##SYS_VAR_NAME(bool is_inc) const                           \
  {                                                                                   \
    return is_inc ? inc_data_.SYS_VAR_NAME##_ : base_data_.SYS_VAR_NAME##_;           \
  }

#define DEF_SYS_VAR_CACHE_FUNCS_STR(SYS_VAR_NAME)                                     \
  void set_base_##SYS_VAR_NAME(const common::ObString &value)            \
  {                                                                                   \
    base_data_.set_##SYS_VAR_NAME(value);                                             \
  }                                                                                   \
  void set_##SYS_VAR_NAME(const common::ObString &value)                 \
  {                                                                                   \
      inc_data_.set_##SYS_VAR_NAME(value);                                            \
      inc_##SYS_VAR_NAME##_ = true;                                                   \
  }                                                                                   \
  const common::ObString &get_##SYS_VAR_NAME() const                                  \
  {                                                                                   \
    return get_##SYS_VAR_NAME(inc_##SYS_VAR_NAME##_);                                 \
  }                                                                                   \
  const common::ObString &get_##SYS_VAR_NAME(bool is_inc) const                       \
  {                                                                                   \
    return is_inc ? inc_data_.get_##SYS_VAR_NAME() : base_data_.get_##SYS_VAR_NAME(); \
  }

  class SysVarsCache
  {
  public:
    SysVarsCache()
      : inc_flags_(0)
    {}
    ~SysVarsCache()
    {}
  public:
    void reset()
    {
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
    DEF_SYS_VAR_CACHE_FUNCS(uint64_t, default_password_lifetime);
    DEF_SYS_VAR_CACHE_FUNCS(bool, tx_read_only);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_pl_cache);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_plan_cache);
    DEF_SYS_VAR_CACHE_FUNCS(bool, optimizer_use_sql_plan_baselines);
    DEF_SYS_VAR_CACHE_FUNCS(bool, optimizer_capture_sql_plan_baselines);
    DEF_SYS_VAR_CACHE_FUNCS(bool, is_result_accurate);
    DEF_SYS_VAR_CACHE_FUNCS(bool, ob_enable_transmission_checksum);
    DEF_SYS_VAR_CACHE_FUNCS(ObCharsetType, character_set_results);
    DEF_SYS_VAR_CACHE_FUNCS(ObCharsetType, character_set_connection);
    DEF_SYS_VAR_CACHE_FUNCS(ObJITEnableMode, ob_enable_jit);
    DEF_SYS_VAR_CACHE_FUNCS(ObCursorSharingMode, cursor_sharing_mode);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, timestamp);
    DEF_SYS_VAR_CACHE_FUNCS(transaction::ObTxIsolationLevel, tx_isolation);
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
    DEF_SYS_VAR_CACHE_FUNCS_STR(plsql_ccflags);
    DEF_SYS_VAR_CACHE_FUNCS_STR(iso_nls_currency);
    DEF_SYS_VAR_CACHE_FUNCS_STR(log_row_value_option);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, ob_max_read_stale_time);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, runtime_filter_type);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, runtime_filter_wait_time_ms);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, runtime_filter_max_in_num);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, runtime_bloom_filter_max_size);
    DEF_SYS_VAR_CACHE_FUNCS(bool, enable_rich_vector_format);
    DEF_SYS_VAR_CACHE_FUNCS(ObCharsetType, ncharacter_set_connection);
    DEF_SYS_VAR_CACHE_FUNCS(int64_t, default_lob_inrow_threshold);
    DEF_SYS_VAR_CACHE_FUNCS(share::ObCompatType, compat_type);
    DEF_SYS_VAR_CACHE_FUNCS(uint64_t, compat_version);
    DEF_SYS_VAR_CACHE_FUNCS(bool, enable_sql_plan_monitor);
    void set_autocommit_info(bool inc_value)
    {
      inc_data_.autocommit_ = inc_value;
      inc_autocommit_ = true;
    }
    void get_autocommit_info(bool &inc_value)
    {
      if (inc_autocommit_) {
        inc_value = inc_data_.autocommit_;
      } else {
        inc_value = base_data_.autocommit_;
      }
    }
  public:
    // base_data 是 ObSysVariables 里的 hardcode 变量值
    static SysVarsCacheData base_data_;
    SysVarsCacheData inc_data_;
    union {
      uint64_t inc_flags_;
      struct {
        bool inc_auto_increment_increment_:1;
        bool inc_sql_throttle_current_priority_:1;
        bool inc_ob_last_schema_version_:1;
        bool inc_sql_select_limit_:1;
        bool inc_auto_increment_offset_:1;
        bool inc_last_insert_id_:1;
        bool inc_binlog_row_image_:1;
        bool inc_foreign_key_checks_:1;
        bool inc_default_password_lifetime_:1;
        bool inc_tx_read_only_:1;
        bool inc_ob_enable_plan_cache_:1;
        bool inc_optimizer_use_sql_plan_baselines_:1;
        bool inc_optimizer_capture_sql_plan_baselines_:1;
        bool inc_is_result_accurate_:1;
        bool inc_ob_enable_transmission_checksum_:1;
        bool inc_character_set_results_:1;
        bool inc_character_set_connection_:1;
        bool inc_ob_enable_jit_:1;
        bool inc_cursor_sharing_mode_:1;
        bool inc_timestamp_:1;
        bool inc_tx_isolation_:1;
        bool inc_autocommit_:1;
        bool inc_ob_enable_trace_log_:1;
        bool inc_ob_enable_sql_audit_;
        bool inc_nls_length_semantics_:1;
        bool inc_ob_org_cluster_id_:1;
        bool inc_ob_query_timeout_:1;
        bool inc_ob_trx_timeout_:1;
        bool inc_collation_connection_:1;
        bool inc_sql_mode_:1;
        bool inc_nls_date_format_:1;
        bool inc_nls_timestamp_format_:1;
        bool inc_nls_timestamp_tz_format_:1;
        bool inc_ob_trx_idle_timeout_:1;
        bool inc_ob_trx_lock_timeout_:1;
        bool inc_nls_collation_:1;
        bool inc_nls_nation_collation_:1;
        bool inc_ob_trace_info_:1;
        bool inc_ob_pl_block_timeout_:1;
        bool inc_plsql_ccflags_:1;
        bool inc_iso_nls_currency_:1;
        bool inc_log_row_value_option_:1;
        bool inc_ob_max_read_stale_time_:1;
        bool inc_runtime_filter_type_:1;
        bool inc_runtime_filter_wait_time_ms_:1;
        bool inc_runtime_filter_max_in_num_:1;
        bool inc_runtime_bloom_filter_max_size_:1;
        bool inc_enable_rich_vector_format_:1;
        bool inc_ncharacter_set_connection_:1;
        bool inc_default_lob_inrow_threshold_:1;
        bool inc_ob_enable_pl_cache_:1;
        bool inc_compat_type_:1;
        bool inc_compat_version_:1;
        bool inc_enable_sql_plan_monitor_:1;
      };
    };
  };
protected:
  const uint64_t orig_tenant_id_;     // which tenant new me
private:
  static const int64_t CACHED_SYS_VAR_VERSION = 721;// a magic num
  static const int MAX_SESS_BT_BUFF_SIZE = 1024;

  ObTenantSQLSessionMgr *tenant_session_mgr_;
  // data structure related:
  common::ObRecursiveMutex query_mutex_;//互斥同一个session上的多次query请求
  common::ObRecursiveMutex thread_data_mutex_;//互斥多个线程对同一session成员的并发读写, 保护thread_data_的一致性
  bool is_valid_;  // is valid session entry
  bool is_deserialized_; //是否为反序列化得到的session, 目前仅用于临时表session释放时数据清理
  // session properties:
  char tenant_[common::OB_MAX_TENANT_NAME_LENGTH + 1];         // current tenant
  uint64_t tenant_id_;            // current tenant ID, used for privilege check and resource audit
  char effective_tenant_[common::OB_MAX_TENANT_NAME_LENGTH + 1];
  uint64_t effective_tenant_id_;            // current effective tenant ID, used for schema check
  uint64_t rpc_tenant_id_;
  bool is_changed_to_temp_tenant_;              // if tenant is changed to temp tenant for show statement
  uint64_t user_id_;              // current user id
  common::ObString client_version_;  // current client version
  common::ObString driver_version_;  // current driver version
  uint32_t sessid_;
  uint32_t master_sessid_;
  uint32_t client_sessid_;
  uint64_t client_create_time_;
  uint64_t proxy_sessid_;
  int64_t global_vars_version_; // used for obproxy synchronize variables
  int64_t sys_var_base_version_;
  uint64_t proxy_user_id_;              // current proxy user id
  /*******************************************
   * transaction ctrl relative for session
   *******************************************/
protected:
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxExecResult tx_result_; // TODO: move to QueryCtx/ExecCtx
  // reserved read snapshot version for current or previous stmt in the txn. And
  // it is used for multi-version garbage colloector to collect ative snapshot.
  // While it may be empty for the txn with ac = 1 and remote execution whose
  // snapshot version is generated from remote server(called by start_stmt). So
  // use it only query is active and version is valid.
  share::SCN reserved_read_snapshot_version_;
  transaction::ObXATransID xid_;
  bool associated_xa_; // session joined distr-xa-trans by xa-start
  int64_t cached_tenant_config_version_;
public:
  const transaction::ObXATransID &get_xid() const { return xid_; }
  transaction::ObTransID get_tx_id() const { return tx_desc_ != NULL ? tx_desc_->get_tx_id() : transaction::ObTransID(); }
  transaction::ObTxDesc /*Nullable*/ *&get_tx_desc() { return tx_desc_; }
  const transaction::ObTxDesc /*Nullable*/ *get_tx_desc() const { return tx_desc_; }
  transaction::ObTxExecResult &get_trans_result() { return tx_result_; }
  bool associated_xa() const { return associated_xa_; }
  int associate_xa(const transaction::ObXATransID &xid) { associated_xa_ = true; return xid_.set(xid); }
  void disassociate_xa() { associated_xa_ = false; xid_.reset(); }
private:
  common::ObSEArray<TableStmtType, 2> total_stmt_tables_;
  common::ObSEArray<TableStmtType, 1> cur_stmt_tables_;
  char ssl_cipher_buff_[64];
  // following 3 params are used to diagnosis session leak.
  char sess_bt_buff_[MAX_SESS_BT_BUFF_SIZE];
  int sess_bt_buff_pos_;
  int32_t sess_ref_cnt_;
  int32_t sess_ref_seq_;

protected:
  // alloc at most SMALL_BLOCK_SIZE bytes for each alloc() call.
  // free() call returns memory back to block pool
  common::ObSmallBlockAllocator<> block_allocator_;
  common::ObSmallBlockAllocator<> ps_session_info_allocator_;
  common::ObSmallBlockAllocator<> cursor_info_allocator_; // for alloc memory of PS CURSOR/SERVER REF CURSOR
  common::ObSmallBlockAllocator<> package_info_allocator_; // for alloc memory of session package state
  common::ObStringBuf name_pool_; // for variables names and statement names
  intptr_t json_pl_mngr_; // for pl json manage
  TransFlags trans_flags_;
  SqlScopeFlags sql_scope_flags_;
  bool need_reset_package_; // for dbms_session.reset_package

private:
  common::ObStringBuf base_sys_var_alloc_; // for variables names and statement names
  // Double buffer optimization
  common::ObStringBuf *inc_sys_var_alloc_[2]; // for values in sys variables update
  common::ObStringBuf inc_sys_var_alloc1_; // for values in sys variables update
  common::ObStringBuf inc_sys_var_alloc2_; // for values in sys variables update
  int32_t current_buf_index_; // for record current buf index
  // Double buffer optimization end.
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  ObSessionValMap user_var_val_map_; // user variables
  share::ObBasicSysVar *sys_vars_[share::ObSysVarFactory::ALL_SYS_VARS_COUNT]; // system variables
  common::ObSEArray<int64_t, 32> influence_plan_var_indexs_;
  common::ObString sys_var_in_pc_str_;
  // configurations that will influence execution plan
  common::ObString config_in_pc_str_;
  bool is_first_gen_; // is first generate sys_var_in_pc_str_;
  bool is_first_gen_config_; // whether is first time t o generate config_in_pc_str_
  share::ObSysVarFactory sys_var_fac_;
  char trace_id_buff_[64];//由于trace_id系统变量在出现slow query的情况下会进行更新，因此通过一个buffer来存储其内容，防止内存频繁分配
  int64_t next_frag_mem_point_; // 用于控制 sys var 内存占用的碎片整理（反复设置同一个 varchar 值会产生内存碎片）
  int64_t sys_vars_encode_max_size_;

  //==============系统变量相关的变量，需要序列化到远端==============
  common::ObConsistencyLevel consistency_level_;
  ObTimeZoneInfoWrap tz_info_wrap_;
  int64_t next_tx_read_only_;
  transaction::ObTxIsolationLevel next_tx_isolation_;
  //===============================================================

  //==============系统变量相关的变量，不需序列化到远端==============
  bool log_id_level_map_valid_;
  common::ObLogIdLevelMap log_id_level_map_;
  //===============================================================

  // 生命周期不保证，谨慎使用该指针
  ObPhysicalPlan *cur_phy_plan_;
  // sql_id of cur_phy_plan_ sql
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t plan_id_; // for ASH sampling, get current SQL's sql_id & plan_id
  uint64_t last_plan_id_;

  ObFLTVars flt_vars_;
  //=======================ObProxy && OCJ related============================
  obmysql::ObMySQLCapabilityFlags capability_;
  obmysql::ObProxyCapabilityFlags proxy_capability_;
  obmysql::ObClientAttributeCapabilityFlags client_attribute_capability_;
  common::ObClientMode client_mode_; // client mode, java client , obproxy or etc.
  // add by oushen, track changed session info
  common::ObSEArray<ChangedVar, 8> changed_sys_vars_;
  common::ObSEArray<common::ObString, 16> changed_user_vars_;
  common::ObArenaAllocator changed_var_pool_;  // reuse for each statement
  common::ObReserveArenaAllocator<256> extra_info_allocator_; // use for extra_info in 20 protocol
  bool is_database_changed_;  // is schema changed
  share::ObFeedbackManager feedback_manager_; // feedback T-L-V
  // add by gujian, cached the flag whether transaction is sepcified
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
  // 处理的上个query包的trace_id，用于判断是否为重试query的包。这里只关心query，不管其他类型的包（比如init db等）。
  common::ObCurTraceId::TraceId last_query_trace_id_;
protected:
  //this should be used by subclass, so need be protected
  MultiThreadData thread_data_;
  // nested session and sql execute for foreign key.
  int64_t nested_count_; // 初始化为-1; 当前有stmt执行为0; 发生嵌套时+1
  // Configurations that will influence execution plan.
  ObConfigInfoInPC inf_pc_configs_;
  common::ObString client_identifier_;

  //为了性能做的系统变量本地缓存值
public:
  inline const SysVarsCacheData &get_sys_var_cache_inc_data() const {
    return sys_vars_cache_.inc_data_;
  }
  inline SysVarsCacheData &get_sys_var_cache_inc_data() {
    return sys_vars_cache_.inc_data_;
  }
private:
  SysVarsCache sys_vars_cache_;
  static int fill_sys_vars_cache_base_value(
      share::ObSysVarClassType var,
      SysVarsCache &sys_vars_cache,
      const common::ObObj &val);
private:

  // @修铭 2.2版本之前用于标识只读zone场景的SHOW语句，它的值由于实现缺陷，始终为false
  // 2.2版本之后，去掉只读zone，物理备库也不再依赖该值，为此，该值不再使用
  // bool literal_query_;

  //上一条语句的结束时间
  int64_t curr_trans_last_stmt_end_time_;

  bool check_sys_variable_;
  bool acquire_from_pool_;
  // 在构造函数中初始化为true，在一些特定错误情况下被设为false，表示session不能释放回session pool。
  // 所以reset接口中不需要、并且也不能重置release_to_pool_。
  bool release_to_pool_;
  volatile int64_t is_tenant_killed_;  // use int64_t for ATOMIC_LOAD / ATOMIC_STORE.
  int64_t reused_count_;
  // type of first stmt which need transaction
  // either transactional read or transactional write
  stmt::StmtType first_need_txn_stmt_type_;
  // some Cmd like DDL will commit current transaction, and need recheck tx read only settings before run
  bool need_recheck_txn_readonly_;
  //min_cluster_version_: 记录sql执行前当前集群最小的server版本号
  //解决的问题兼容性问题:
  //   2.2.3之前的版本会序列化所有的需要序列化的系统变量,
  //   2.2.3及之后版本对于mysql模式, 不序列化ORACLE_ONLY的系统变量,
  //   下面场景会报错(问题1):
  //     222和223混跑期间, 客户端执行了一条px query 在session序列化某个系统变量时报-4016,
  //     主线程所在的server是223, sqc端所在的server是222.这个query的整个生命周期内会发生两次session的序列化和反序列化.
  //      第一次是init_sqc rpc(223上执行, 仅序列化非ORACLE_ONLY变量)
  //      第二次是在222上, sqc启worker会将所有的exec_ctx拷贝给其他worker,而拷贝的方式是序列化+反序列化.
  //      在init_sqc rpc第一次序列化时,  如果是mysql租户, 只会序列化 《需要序列化的系统变量》 以及
  //      《非ORACLE_ONLY的系统变量》,因此222的observer在反序列化后也只有这些变量.
  //      但当第二次在启worker阶段去序列化session时, 222版本没有<非 ORACLE_ONLY的系统变量>这个筛选,
  //      因此序列化时根据硬编码的系统变量只做<需要序列化>筛选, 会检测到比当前session中更多系统变量,
  //      这些变量检测到它们在当前session中为空此时就会报不符合预期报4016了.
  //
  //  为解决上面场景, 会添加版本控制, 如果是升级过程中, 当前最小版本号(用GET_MIN_CLUSTER_VERSION())不是2.2.3及以上版本,
  //  则按老的方式, 将ORACLE_ONLY的系统变量也序列化到远端机器, 可以解决上面报错的场景, 但另一个场景还会有问题:
  //
  //  问题2: 当2.2.3向2.2.2版本发送远程请求, 在计算序列化size时会计算一次需要序列化的系统变量, 由于最小版本号
  //  是2.2.2, 会将ORACLE_ONLY的系统变量也算入序列化变量中; 后面实际做序列化动作时, 也会计算一次需要序列化的系统变量,
  //  如果此时都升级到了2.2.3, 则不会将ORACLE_ONLY变量进行序列化, 此时会出现实际序列化的大小与之前计算的需要序列化的大小不一致,出现非预期行为;
  //
  //  为解决问题2, 有两种思路(思路2可用):
  //     思路1: 将需要序列化的系统变量在计算序列化size时push到一个session成员sys_var_ids_中, 下次在实际序列化不在计算需要序列化的系统变量
  //            直接利用sys_var_ids_序列化对应的系统变量, 该思路会有一个问题, 就是我们sqc启动work做session序列化时, 是并发的, 初始化sys_var_ids_
  //            会有并发问题, 因此该思路不可行;
  //     思路2: 在session上记录min_cluster_version_, 在执行前进行初始化, session求序列化size和实际序列化时, 都使用相同的最小版本号确定需要序列化哪些系统变量;
  //            确保session求序列化size和实际序列化的大小是一致的;
  //
  uint64_t exec_min_cluster_version_;
  stmt::StmtType stmt_type_;
private:
  //label security
  common::ObSEArray<share::ObLabelSeSessionLabel, 4> labels_;
  // 构造当前 session 的线程 id，用于 all_virtual_processlist 中的 THREAD_ID 字段
  // 通过本 id 可以快速对 worker 做 `pstack THREADID` 操作
  int64_t thread_id_;
  common::ActiveSessionStat ash_stat_;
  // indicate whether user password is expired, is set when session is established.
  // will not be changed during whole session lifetime unless user changes password
  // in this session.
  bool is_password_expired_;
  // timestamp of processing current query. refresh when retry.
  int64_t process_query_time_;
  int64_t last_update_tz_time_; //timestamp of last attempt to update timezone info
  bool is_client_sessid_support_; //client session id support flag
  bool is_feedback_proxy_info_support_; // to confirm whether obproxy supports feedback_proxy_info
  bool use_rich_vector_format_;
  int64_t last_refresh_schema_version_;
  // rich format specified hint, e.g. `select /*+opt_param('enable_rich_vector_format', 'true')*/ * from t`
  // force_rich_vector_format_ == FORCE_ON => use_rich_format() returns true
  // force_rich_vector_format_ == FORCE_OFF => use_rich_format() returns false
  // otherwise use_rich_format() returns use_rich_vector_format_
  ForceRichFormatStatus force_rich_vector_format_;
  // just used to plan cache key
  bool config_use_rich_format_;

  common::ObSEArray<uint64_t, 4> enable_role_ids_;
  uint64_t sys_var_config_hash_val_;
};


inline const common::ObString ObBasicSessionInfo::get_current_query_string() const
{
  common::ObString str_ret;
  str_ret.assign_ptr(const_cast<char *>(thread_data_.cur_query_), static_cast<int32_t>(thread_data_.cur_query_len_));
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

inline const ObString &ObBasicSessionInfo::get_ob_trace_info() const
{
  return sys_vars_cache_.get_ob_trace_info();
}

inline const ObString &ObBasicSessionInfo::get_plsql_ccflags() const
{
  return sys_vars_cache_.get_plsql_ccflags();
}

inline const ObString &ObBasicSessionInfo::get_iso_nls_currency() const
{
  return sys_vars_cache_.get_iso_nls_currency();
}

inline const ObString &ObBasicSessionInfo::get_log_row_value_option() const
{
  return sys_vars_cache_.get_log_row_value_option();
}

inline int64_t ObBasicSessionInfo::get_default_lob_inrow_threshold() const
{
  return sys_vars_cache_.get_default_lob_inrow_threshold();
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

inline bool ObBasicSessionInfo::get_local_ob_enable_pl_cache() const
{
  return sys_vars_cache_.get_ob_enable_pl_cache();
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

//oracle SYS user actual nls_length_semantics is always BYTE
inline ObLengthSemantics ObBasicSessionInfo::get_actual_nls_length_semantics() const
{
  return is_oracle_sys_database_id(get_database_id()) ?
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

inline int ObBasicSessionInfo::get_local_nls_format(const ObObjType type, ObString &format_str) const
{
  int ret = common::OB_SUCCESS;
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

inline int ObBasicSessionInfo::set_partition_location_feedback(const share::ObFBPartitionParam &param)
{
   INIT_SUCC(ret);
  if (is_abundant_feedback_support()) {
    if (OB_FAIL(feedback_manager_.add_partition_fb_info(param))) {
      SQL_SESSION_LOG(WARN, "fail to add partition fb info", K(param), K(ret));
    }
  }
  return ret;
}

//对象（目前仅用于PL，后续expr也照此处理）执行环境
class ObExecEnv
{
public:
  //序列化顺序
  enum ExecEnvType
  {
    SQL_MODE = 0,
    CHARSET_CLIENT,
    COLLATION_CONNECTION,
    COLLATION_DATABASE,
    PLSQL_CCFLAGS,
    PLSQL_OPTIMIZE_LEVEL,
    MAX_ENV,
  };

  static constexpr share::ObSysVarClassType ExecEnvMap[MAX_ENV + 1] = {
    share::SYS_VAR_SQL_MODE,
    share::SYS_VAR_CHARACTER_SET_CLIENT,
    share::SYS_VAR_COLLATION_CONNECTION,
    share::SYS_VAR_COLLATION_DATABASE,
    share::SYS_VAR_PLSQL_CCFLAGS,
    share::SYS_VAR_PLSQL_OPTIMIZE_LEVEL,
    share::SYS_VAR_INVALID
  };

  ObExecEnv() :
    sql_mode_(DEFAULT_OCEANBASE_MODE),
    charset_client_(CS_TYPE_INVALID),
    collation_connection_(CS_TYPE_INVALID),
    collation_database_(CS_TYPE_INVALID),
    plsql_ccflags_(),
    plsql_optimize_level_(2)  // default PLSQL_OPTIMIZE_LEVEL = 2
  { }

  virtual ~ObExecEnv() {}

  TO_STRING_KV(K_(sql_mode),
               K_(charset_client),
               K_(collation_connection),
               K_(collation_database),
               K_(plsql_ccflags),
               K_(plsql_optimize_level));

  void reset();

  bool operator==(const ObExecEnv &other) const;
  bool operator!=(const ObExecEnv &other) const;

  static int gen_exec_env(const ObBasicSessionInfo &session, char* buf, int64_t len, int64_t &pos);
  static int gen_exec_env(const share::schema::ObSysVariableSchema &sys_variable,
                          char* buf,
                          int64_t len,
                          int64_t &pos);

  int init(const ObString &exec_env);
  int load(ObBasicSessionInfo &session, ObIAllocator *alloc = NULL);
  int store(ObBasicSessionInfo &session);

  ObSQLMode get_sql_mode() const { return sql_mode_; }
  ObCharsetType get_charset_client() { return ObCharset::charset_type_by_coll(charset_client_); }
  ObCollationType get_collation_connection() { return collation_connection_; }
  ObCollationType get_collation_database() { return collation_database_; }
  ObString& get_plsql_ccflags() { return plsql_ccflags_; }

  void set_plsql_ccflags(ObString &plsql_ccflags) { plsql_ccflags_ = plsql_ccflags; }

  int64_t get_plsql_optimize_level() { return plsql_optimize_level_; }
  void set_plsql_optimize_level(int64_t level) { plsql_optimize_level_ = plsql_optimize_level_; }

private:
  ObSQLMode sql_mode_;
  ObCollationType charset_client_;
  ObCollationType collation_connection_;
  ObCollationType collation_database_;
  ObString plsql_ccflags_;
  int64_t plsql_optimize_level_;
};


}//end of namespace sql
}//end of namespace oceanbase

#endif /* _OB_OBPROXY_BASIC_SESSION_INFO_H */
