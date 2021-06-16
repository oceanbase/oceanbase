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

#ifndef OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_
#define OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_

#include "io/easy_io_struct.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "common/ob_range.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_name_def.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/time/ob_cur_time.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "lib/hash/ob_link_hashmap.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/ob_sql_config_provider.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_session_val_map.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/monitor/ob_exec_stat.h"

namespace oceanbase {
namespace observer {
class ObSqlEndTransCb;
}

namespace obmysql {
class ObMySQLRequestManager;
}  // namespace obmysql
namespace share {
class ObSequenceValue;
}
using common::ObPsStmtId;
namespace sql {
class ObResultSet;
class ObPlanCache;
class ObPsCache;
class ObPlanCacheManager;
class ObPsSessionInfo;
class ObPsStmtInfo;
class ObStmt;
class ObSQLSessionInfo;

class SessionInfoKey {
public:
  SessionInfoKey() : version_(0), sessid_(0), proxy_sessid_(0)
  {}
  SessionInfoKey(uint32_t version, uint32_t sessid, uint64_t proxy_sessid = 0)
      : version_(version), sessid_(sessid), proxy_sessid_(proxy_sessid)
  {}
  uint64_t hash() const
  {
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&version_, sizeof(version_), hash_value);
    hash_value = common::murmurhash(&sessid_, sizeof(sessid_), hash_value);
    return hash_value;
  };
  int compare(const SessionInfoKey& r)
  {
    int cmp = 0;
    if (version_ < r.version_) {
      cmp = -1;
    } else if (version_ > r.version_) {
      cmp = 1;
    } else {
      if (sessid_ < r.sessid_) {
        cmp = -1;
      } else if (sessid_ > r.sessid_) {
        cmp = 1;
      } else {
        cmp = 0;
      }
    }
    return cmp;
  }

public:
  uint32_t version_;
  uint32_t sessid_;
  uint64_t proxy_sessid_;
};

struct ObSessionStat final {
  ObSessionStat()
      : total_logical_read_(0),
        total_physical_read_(0),
        total_logical_write_(0),
        total_lock_count_(0),
        total_cpu_time_us_(0),
        total_exec_time_us_(0),
        total_alive_time_us_(0)
  {}
  void reset()
  {
    new (this) ObSessionStat();
  }

  TO_STRING_KV(K_(total_logical_read), K_(total_physical_read), K_(total_logical_write), K_(total_lock_count),
      K_(total_cpu_time_us), K_(total_exec_time_us), K_(total_alive_time_us));

  uint64_t total_logical_read_;
  uint64_t total_physical_read_;
  uint64_t total_logical_write_;
  uint64_t total_lock_count_;
  uint64_t total_cpu_time_us_;
  uint64_t total_exec_time_us_;
  uint64_t total_alive_time_us_;
};

class ObTenantCachedSchemaGuardInfo {
public:
  ObTenantCachedSchemaGuardInfo()
  {
    reset();
  }
  ~ObTenantCachedSchemaGuardInfo()
  {
    reset();
  }
  void reset();

  share::schema::ObSchemaGetterGuard& get_schema_guard()
  {
    return schema_guard_;
  }
  int refresh_tenant_schema_guard(const uint64_t tenant_id);

  void try_revert_schema_guard();

private:
  share::schema::ObSchemaGetterGuard schema_guard_;
  int64_t ref_ts_;
  uint64_t tenant_id_;
  int64_t schema_version_;
};

class ObIExtraStatusCheck : public common::ObDLinkBase<ObIExtraStatusCheck> {
public:
  virtual ~ObIExtraStatusCheck()
  {}

  virtual const char* name() const = 0;
  virtual int check() const = 0;

  class Guard {
  public:
    Guard(ObSQLSessionInfo& session, ObIExtraStatusCheck& checker);
    ~Guard();

  private:
    ObSQLSessionInfo& session_;
    ObIExtraStatusCheck& checker_;
  };
};

typedef common::hash::ObHashMap<uint64_t, share::ObSequenceValue, common::hash::NoPthreadDefendMode>
    ObSequenceCurrvalMap;
typedef common::LinkHashNode<SessionInfoKey> SessionInfoHashNode;
typedef common::LinkHashValue<SessionInfoKey> SessionInfoHashValue;
class ObSQLSessionInfo : public common::ObVersionProvider, public ObBasicSessionInfo, public SessionInfoHashValue {
  OB_UNIS_VERSION(1);

public:
  enum SessionType { INVALID_TYPE, USER_SESSION, INNER_SESSION };
  // for switch stmt.
  class StmtSavedValue : public ObBasicSessionInfo::StmtSavedValue {
  public:
    StmtSavedValue() : ObBasicSessionInfo::StmtSavedValue()
    {
      reset();
    }
    inline void reset()
    {
      ObBasicSessionInfo::StmtSavedValue::reset();
      audit_record_.reset();
      session_type_ = INVALID_TYPE;
      inner_flag_ = false;
      use_static_typing_engine_ = false;
      is_ignore_stmt_ = false;
    }

  public:
    ObAuditRecordData audit_record_;
    SessionType session_type_;
    bool inner_flag_;
    bool use_static_typing_engine_;
    bool is_ignore_stmt_;
  };

public:
  ObSQLSessionInfo();
  virtual ~ObSQLSessionInfo();

  int init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator,
      const ObTZInfoMap* tz_info = NULL, int64_t sess_create_time = 0, uint64_t tenant_id = OB_INVALID_TENANT_ID);
  // for test
  int test_init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid, common::ObIAllocator* bucket_allocator);
  void destroy(bool skip_sys_var = false);
  void reset(bool skip_sys_var);
  void clean_status();
  void set_plan_cache_manager(ObPlanCacheManager* pcm)
  {
    plan_cache_manager_ = pcm;
  }
  void set_plan_cache(ObPlanCache* cache)
  {
    plan_cache_ = cache;
  }
  void set_ps_cache(ObPsCache* cache)
  {
    ps_cache_ = cache;
  }
  const common::ObWarningBuffer& get_show_warnings_buffer() const
  {
    return show_warnings_buf_;
  }
  const common::ObWarningBuffer& get_warnings_buffer() const
  {
    return warnings_buf_;
  }
  common::ObWarningBuffer& get_warnings_buffer()
  {
    return warnings_buf_;
  }
  void reset_warnings_buf()
  {
    warnings_buf_.reset();
  }
  ObPrivSet get_user_priv_set() const
  {
    return user_priv_set_;
  }
  ObPrivSet get_db_priv_set() const
  {
    return db_priv_set_;
  }
  ObPlanCache* get_plan_cache();
  ObPsCache* get_ps_cache();
  ObPlanCacheManager* get_plan_cache_manager()
  {
    return plan_cache_manager_;
  }
  void set_user_priv_set(const ObPrivSet priv_set)
  {
    user_priv_set_ = priv_set;
  }
  void set_db_priv_set(const ObPrivSet priv_set)
  {
    db_priv_set_ = priv_set;
  }
  void set_show_warnings_buf(int error_code);
  void set_global_sessid(const int64_t global_sessid)
  {
    global_sessid_ = global_sessid;
  }
  int64_t get_global_sessid() const
  {
    return global_sessid_;
  }
  void set_read_uncommited(bool read_uncommited)
  {
    read_uncommited_ = read_uncommited;
  }
  bool get_read_uncommited() const
  {
    return read_uncommited_;
  }
  void set_version_provider(const common::ObVersionProvider* version_provider)
  {
    version_provider_ = version_provider;
  }
  const common::ObVersion get_frozen_version() const
  {
    return version_provider_->get_frozen_version();
  }
  const common::ObVersion get_merged_version() const
  {
    return version_provider_->get_merged_version();
  }
  void set_config_provider(const ObSQLConfigProvider* config_provider)
  {
    config_provider_ = config_provider;
  }
  bool is_read_only() const
  {
    return config_provider_->is_read_only();
  };
  int64_t get_nlj_cache_limit() const
  {
    return config_provider_->get_nlj_cache_limit();
  };
  bool is_terminate(int& ret) const;

  void set_curr_trans_start_time(int64_t t)
  {
    curr_trans_start_time_ = t;
  };
  int64_t get_curr_trans_start_time() const
  {
    return curr_trans_start_time_;
  };

  void set_curr_trans_last_stmt_time(int64_t t)
  {
    curr_trans_last_stmt_time_ = t;
  };
  int64_t get_curr_trans_last_stmt_time() const
  {
    return curr_trans_last_stmt_time_;
  };

  void set_sess_create_time(const int64_t t)
  {
    sess_create_time_ = t;
  };
  int64_t get_sess_create_time() const
  {
    return sess_create_time_;
  };

  void set_last_refresh_temp_table_time(const int64_t t)
  {
    last_refresh_temp_table_time_ = t;
  };
  int64_t get_last_refresh_temp_table_time() const
  {
    return last_refresh_temp_table_time_;
  };

  void set_has_temp_table_flag()
  {
    has_temp_table_flag_ = true;
  };
  bool get_has_temp_table_flag() const
  {
    return has_temp_table_flag_;
  };
  int drop_temp_tables(const bool is_sess_disconn = true);
  void refresh_temp_tables_sess_active_time();
  int drop_reused_oracle_temp_tables();
  int delete_from_oracle_temp_tables(const obrpc::ObDropTableArg& const_drop_table_arg);

  void set_trans_type(int32_t t)
  {
    trans_type_ = t;
  }
  int32_t get_trans_type() const
  {
    return trans_type_;
  }

  void get_session_priv_info(share::schema::ObSessionPrivInfo& session_priv) const;
  void set_found_rows(const int64_t count)
  {
    found_rows_ = count;
  }
  int64_t get_found_rows() const
  {
    return found_rows_;
  }
  void set_affected_rows(const int64_t count)
  {
    affected_rows_ = count;
    if (affected_rows_ > 0) {
      trans_flags_.set_has_hold_row_lock(true);
    }
  }
  int64_t get_affected_rows() const
  {
    return affected_rows_;
  }
  bool has_user_super_privilege() const;
  bool has_user_process_privilege() const;
  int check_read_only_privilege(const bool read_only, const ObSqlTraits& sql_traits);
  int check_global_read_only_privilege(const bool read_only, const ObSqlTraits& sql_traits);

  int remove_prepare(const ObString& ps_name);
  int get_prepare_id(const ObString& ps_name, ObPsStmtId& ps_id) const;
  int add_prepare(const ObString& ps_name, ObPsStmtId ps_id);
  int remove_ps_session_info(const ObPsStmtId stmt_id);
  int get_ps_session_info(const ObPsStmtId stmt_id, ObPsSessionInfo*& ps_session_info) const;

  inline void set_pl_attached_id(uint32_t id)
  {
    pl_attach_session_id_ = id;
  }
  inline uint32_t get_pl_attached_id() const
  {
    return pl_attach_session_id_;
  }

  inline common::hash::ObHashSet<common::ObString>* get_pl_sync_pkg_vars()
  {
    return pl_sync_pkg_vars_;
  }

  int replace_user_variables(const ObSessionValMap& user_var_map);
  int replace_user_variables(ObExecContext& ctx, const ObSessionValMap& user_var_map);

  inline int64_t get_last_plan_id()
  {
    return last_plan_id_;
  }
  inline void set_last_plan_id(int64_t code)
  {
    last_plan_id_ = code;
  }

  inline bool get_pl_can_retry()
  {
    return pl_can_retry_;
  }
  inline void set_pl_can_retry(bool can_retry)
  {
    pl_can_retry_ = can_retry;
  }

  inline void* get_inner_conn()
  {
    return inner_conn_;
  }
  inline void set_inner_conn(void* inner_conn)
  {
    inner_conn_ = inner_conn;
  }

  // show trace
  common::ObTraceEventRecorder* get_trace_buf();
  void clear_trace_buf();

  ObEndTransAsyncCallback& get_end_trans_cb()
  {
    return end_trans_cb_;
  }
  observer::ObSqlEndTransCb& get_mysql_end_trans_cb()
  {
    return end_trans_cb_.get_mysql_end_trans_cb();
  }
  int get_collation_type_of_names(const ObNameTypeClass type_class, common::ObCollationType& cs_type) const;
  int name_case_cmp(const common::ObString& name, const common::ObString& name_other, const ObNameTypeClass type_class,
      bool& is_equal) const;
  int kill_query();

  inline void set_inner_session()
  {
    inner_flag_ = true;
    session_type_ = INNER_SESSION;
  }
  inline void set_user_session()
  {
    inner_flag_ = false;
    session_type_ = USER_SESSION;
  }
  void set_session_type_with_flag();
  void set_session_type(SessionType session_type)
  {
    session_type_ = session_type;
  }
  inline SessionType get_session_type() const
  {
    return session_type_;
  }
  void set_early_lock_release(bool enable);
  bool get_early_lock_release() const
  {
    return enable_early_lock_release_;
  }

  bool is_inner() const
  {
    return inner_flag_;
  }
  void reset_audit_record();
  ObAuditRecordData& get_audit_record();
  const ObAuditRecordData& get_raw_audit_record() const;
  const ObAuditRecordData& get_final_audit_record(ObExecuteMode mode);
  ObSessionStat& get_session_stat()
  {
    return session_stat_;
  }
  void update_stat_from_audit_record();
  void update_alive_time_stat();
  void handle_audit_record(bool need_retry, ObExecuteMode exec_mode);

  void set_is_remote(bool is_remote)
  {
    is_remote_session_ = is_remote;
  }

  int save_session(StmtSavedValue& saved_value);
  int save_sql_session(StmtSavedValue& saved_value);
  int restore_sql_session(StmtSavedValue& saved_value);
  int restore_session(StmtSavedValue& saved_value);

  int begin_nested_session(StmtSavedValue& saved_value, bool skip_cur_stmt_tables = false);
  int end_nested_session(StmtSavedValue& saved_value);

  // sequenct.nextval() will save the result to current session.
  int get_sequence_value(uint64_t tenant_id, uint64_t seq_id, share::ObSequenceValue& value);
  int set_sequence_value(uint64_t tenant_id, uint64_t seq_id, const share::ObSequenceValue& value);

  int prepare_ps_stmt(const ObPsStmtId inner_stmt_id, const ObPsStmtInfo* stmt_info, ObPsStmtId& client_stmt_id,
      bool& already_exists, bool is_inner_sql);
  int get_inner_ps_stmt_id(ObPsStmtId cli_stmt_id, ObPsStmtId& inner_stmt_id);
  int close_ps_stmt(ObPsStmtId stmt_id);

  bool is_encrypt_tenant();

  // use static typing engine for current query
  bool use_static_typing_engine() const
  {
    return use_static_typing_engine_;
  }
  void set_use_static_typing_engine(bool use)
  {
    use_static_typing_engine_ = use;
  }

  void set_ctx_mem_context(lib::MemoryContext* ctx_mem_context)
  {
    ctx_mem_context_ = ctx_mem_context;
  }
  lib::MemoryContext* get_ctx_mem_context()
  {
    return ctx_mem_context_;
  }
  ObTenantCachedSchemaGuardInfo& get_cached_schema_guard_info()
  {
    return cached_schema_guard_info_;
  }
  int set_enable_role_array(const common::ObIArray<uint64_t>& role_id_array);
  common::ObSEArray<uint64_t, 8>& get_enable_role_array()
  {
    return enable_role_array_;
  }
  void set_in_definer_named_proc(bool in_proc)
  {
    in_definer_named_proc_ = in_proc;
  }
  bool get_in_definer_named_proc()
  {
    return in_definer_named_proc_;
  }
  bool get_prelock()
  {
    return prelock_;
  }
  void set_prelock(bool prelock)
  {
    prelock_ = prelock;
  }
  void set_proxy_version(uint64_t v)
  {
    proxy_version_ = v;
  }
  uint64_t get_proxy_version()
  {
    return proxy_version_;
  }

  void set_priv_user_id(uint64_t priv_user_id)
  {
    priv_user_id_ = priv_user_id;
  }
  uint64_t get_priv_user_id()
  {
    return (priv_user_id_ == OB_INVALID_ID) ? get_user_id() : priv_user_id_;
  }
  int64_t get_xa_end_timeout_seconds() const
  {
    return xa_end_timeout_seconds_;
  }
  void set_xa_end_timeout_seconds(int64_t seconds)
  {
    xa_end_timeout_seconds_ = seconds;
  }
  void refresh_tenant_config();
  bool is_support_external_consistent()
  {
    refresh_tenant_config();
    return is_external_consistent_;
  }
  bool is_enable_batched_multi_statement()
  {
    refresh_tenant_config();
    return enable_batched_multi_statement_;
  }
  int64_t get_tenant_sort_area_size()
  {
    refresh_tenant_config();
    return ATOMIC_LOAD(&sort_area_size_);
  }
  uint64_t get_priv_user_id_allow_invalid()
  {
    return priv_user_id_;
  }
  int get_xa_last_result() const
  {
    return xa_last_result_;
  }
  void set_xa_last_result(const int result)
  {
    xa_last_result_ = result;
  }

  int add_extra_check(ObIExtraStatusCheck& extra_check)
  {
    return extra_status_check_.add_last(&extra_check) ? common::OB_SUCCESS : common::OB_ERR_UNEXPECTED;
  }

  int del_extra_check(ObIExtraStatusCheck& extra_check)
  {
    extra_status_check_.remove(&extra_check);
    return common::OB_SUCCESS;
  }
  int get_tmp_table_size(uint64_t& size);
  int ps_use_stream_result_set(bool& use_stream);
  void set_ignore_stmt(bool v)
  {
    is_ignore_stmt_ = v;
  }
  bool is_ignore_stmt() const
  {
    return is_ignore_stmt_;
  }

private:
  int close_all_ps_stmt();

  static const int64_t MAX_STORED_PLANS_COUNT = 10240;
  static const int64_t MAX_IPADDR_LENGTH = 64;

private:
  bool is_inited_;
  // store the warning message from the most recent statement in the current session
  common::ObWarningBuffer warnings_buf_;
  common::ObWarningBuffer show_warnings_buf_;
  sql::ObEndTransAsyncCallback end_trans_cb_;
  ObAuditRecordData audit_record_;

  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;
  int64_t curr_trans_start_time_;
  int64_t curr_trans_last_stmt_time_;
  int64_t sess_create_time_;              // for cleaning temp table.
  int64_t last_refresh_temp_table_time_;  // for proxy.
  bool has_temp_table_flag_;
  bool enable_early_lock_release_;
  int32_t trans_type_;
  const common::ObVersionProvider* version_provider_;
  const ObSQLConfigProvider* config_provider_;
  ObPlanCacheManager* plan_cache_manager_;
  ObPlanCache* plan_cache_;
  ObPsCache* ps_cache_;
  int64_t found_rows_;
  int64_t affected_rows_;
  int64_t global_sessid_;
  bool read_uncommited_;
  common::ObTraceEventRecorder* trace_recorder_;
  bool inner_flag_;
  bool is_max_availability_mode_;
  typedef common::hash::ObHashMap<ObPsStmtId, ObPsSessionInfo*, common::hash::NoPthreadDefendMode> PsSessionInfoMap;
  //  typedef common::ObArray<ObPsSessionInfo *> PsSessionInfoArray;
  // PsSessionInfoArray ps_session_info_array_;
  PsSessionInfoMap ps_session_info_map_;
  typedef common::hash::ObHashMap<common::ObString, ObPsStmtId, common::hash::NoPthreadDefendMode> PsNameIdMap;
  PsNameIdMap ps_name_id_map_;
  ObPsStmtId next_client_ps_stmt_id_;
  bool is_remote_session_;
  SessionType session_type_;
  ObSequenceCurrvalMap sequence_currval_map_;

  bool pl_can_retry_;

  uint32_t pl_attach_session_id_;  // for dbms_debug.attach_session().

  int64_t last_plan_id_;  // for physical plan of show trace.

  common::hash::ObHashSet<common::ObString>* pl_sync_pkg_vars_ = NULL;

  void* inner_conn_;  // ObInnerSQLConnection * will cause .h included from each other.

  // use static typing engine for current query.
  bool use_static_typing_engine_;

  ObSessionStat session_stat_;

  lib::MemoryContext* ctx_mem_context_;
  common::ObSEArray<uint64_t, 8> enable_role_array_;
  ObTenantCachedSchemaGuardInfo cached_schema_guard_info_;
  bool in_definer_named_proc_;
  uint64_t priv_user_id_;
  int64_t xa_end_timeout_seconds_;
  bool is_external_consistent_;
  bool enable_batched_multi_statement_;
  uint64_t saved_tenant_info_;
  int64_t sort_area_size_;
  int64_t last_check_ec_ts_;
  int xa_last_result_;
  bool prelock_;
  common::ObDList<ObIExtraStatusCheck> extra_status_check_;
  uint64_t proxy_version_;
  // return different stmt id for same sql if proxy version is higher than min_proxy_version_ps_.
  uint64_t min_proxy_version_ps_;
  bool is_ignore_stmt_;  // for static engine.
};

inline ObIExtraStatusCheck::Guard::Guard(ObSQLSessionInfo& session, ObIExtraStatusCheck& checker)
    : session_(session), checker_(checker)
{
  int ret = session.add_extra_check(checker);
  if (OB_SUCCESS != ret) {
    SQL_ENG_LOG(ERROR, "add extra checker failed", K(ret));
  }
}

inline ObIExtraStatusCheck::Guard::~Guard()
{
  session_.del_extra_check(checker_);
}

inline bool ObSQLSessionInfo::is_terminate(int& ret) const
{
  bool bret = false;
  if (QUERY_KILLED == get_session_state()) {
    bret = true;
    SQL_ENG_LOG(WARN,
        "query interrupted session",
        "query",
        get_current_query_string(),
        "key",
        get_sessid(),
        "proxy_sessid",
        get_proxy_sessid());
    ret = common::OB_ERR_QUERY_INTERRUPTED;
  } else if (SESSION_KILLED == get_session_state()) {
    bret = true;
    ret = common::OB_ERR_SESSION_INTERRUPTED;
  } else if (!extra_status_check_.is_empty()) {
    DLIST_FOREACH(it, extra_status_check_)
    {
      if (OB_FAIL(it->check())) {
        SQL_ENG_LOG(WARN,
            "extra check failed",
            "check_name",
            it->name(),
            "query",
            get_current_query_string(),
            "key",
            get_sessid(),
            "proxy_sessid",
            get_proxy_sessid());
        bret = true;
        break;
      }
    }
  }
  return bret;
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_
