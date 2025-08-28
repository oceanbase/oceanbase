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

#ifndef OCEANBASE_SHARE_OB_MVIEW_ARGS_H_
#define OCEANBASE_SHARE_OB_MVIEW_ARGS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "ob_ddl_args.h"

namespace oceanbase
{
namespace obrpc
{

struct ObMVRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  share::schema::ObMVRefreshMethod refresh_method_;
  share::schema::ObMVRefreshMode refresh_mode_;
  common::ObObj start_time_;
  ObString next_time_expr_;
  ObString exec_env_;
  int64_t parallel_;
  int64_t refresh_dop_;
  share::schema::ObMVNestedRefreshMode nested_refresh_mode_;

  ObMVRefreshInfo() :
  refresh_method_(share::schema::ObMVRefreshMethod::NEVER),
  refresh_mode_(share::schema::ObMVRefreshMode::DEMAND),
  start_time_(),
  next_time_expr_(),
  exec_env_(),
  parallel_(OB_INVALID_COUNT),
  refresh_dop_(0),
  nested_refresh_mode_(share::schema::ObMVNestedRefreshMode::INDIVIDUAL) {}

  void reset() {
    refresh_method_ = share::schema::ObMVRefreshMethod::NEVER;
    refresh_mode_ = share::schema::ObMVRefreshMode::DEMAND;
    start_time_.reset();
    next_time_expr_.reset();
    exec_env_.reset();
    parallel_ = OB_INVALID_COUNT;
    refresh_dop_ = 0;
    nested_refresh_mode_ = share::schema::ObMVNestedRefreshMode::INDIVIDUAL;
  }

  bool operator == (const ObMVRefreshInfo &other) const {
    return refresh_method_ == other.refresh_method_
      && refresh_mode_ == other.refresh_mode_
      && start_time_ == other.start_time_
      && next_time_expr_ == other.next_time_expr_
      && exec_env_ == other.exec_env_
      && parallel_ == other.parallel_
      && refresh_dop_ == other.refresh_dop_
      && nested_refresh_mode_ == other.nested_refresh_mode_;
  }

  TO_STRING_KV(K_(refresh_mode),
      K_(refresh_method),
      K_(start_time),
      K_(next_time_expr),
      K_(exec_env),
      K_(parallel),
      K_(refresh_dop),
      K_(nested_refresh_mode));
};

struct ObMVRequiredColumnsInfo {
  OB_UNIS_VERSION(1);

public:
  ObMVRequiredColumnsInfo()
  : base_table_id_(OB_INVALID_ID)
  {
  }
  ObMVRequiredColumnsInfo(const uint64_t base_table_id, const ObSEArray<uint64_t, 16> &required_columns)
  {
    base_table_id_ = base_table_id;
    required_columns_.assign(required_columns);
  }
  int assign(const ObMVRequiredColumnsInfo &other);
  TO_STRING_KV(K_(base_table_id), K_(required_columns));

public:
  uint64_t base_table_id_;
  ObSEArray<uint64_t, 16> required_columns_;
};

struct ObMVAdditionalInfo {
  OB_UNIS_VERSION(1);

public:
  share::schema::ObTableSchema container_table_schema_;
  ObMVRefreshInfo mv_refresh_info_;
  ObSEArray<ObMVRequiredColumnsInfo, 8> required_columns_infos_;

  int assign(const ObMVAdditionalInfo &other);

  TO_STRING_KV(K_(container_table_schema), K_(mv_refresh_info));
};

struct ObMViewCompleteRefreshArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshArg()
    : ObDDLArg(),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      session_id_(OB_INVALID_ID),
      sql_mode_(0),
      last_refresh_scn_(),
      allocator_("MVRefDDL"),
      tz_info_(),
      tz_info_wrap_(),
      nls_formats_(),
      parent_task_id_(0),
      target_data_sync_scn_(),
      select_sql_(),
      required_columns_infos_()
  {
  }
  ~ObMViewCompleteRefreshArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewCompleteRefreshArg &other);
  INHERIT_TO_STRING_KV("ObDDLArg", ObDDLArg,
                       K_(tenant_id),
                       K_(table_id),
                       K_(session_id),
                       K_(sql_mode),
                       K_(last_refresh_scn),
                       K_(tz_info),
                       K_(tz_info_wrap),
                       "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX),
                       K_(parent_task_id),
                       K_(target_data_sync_scn),
                       K_(select_sql));
public:
  uint64_t tenant_id_;
  uint64_t table_id_; // mview table id
  uint64_t session_id_;
  ObSQLMode sql_mode_;
  share::SCN last_refresh_scn_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  int64_t parent_task_id_;
  share::SCN target_data_sync_scn_;
  ObString select_sql_;
  ObSEArray<ObMVRequiredColumnsInfo, 8> required_columns_infos_;
};

struct ObMViewCompleteRefreshRes final
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshRes() : task_id_(0), trace_id_() {}
  ~ObMViewCompleteRefreshRes() = default;
  void reset()
  {
    task_id_ = 0;
    trace_id_.reset();
  }
  int assign(const ObMViewCompleteRefreshRes &other)
  {
    if (this != &other) {
      task_id_ = other.task_id_;
      trace_id_ = other.trace_id_;
    }
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(task_id), K_(trace_id));
public:
  int64_t task_id_;
  share::ObTaskId trace_id_;
};

struct ObMViewRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMViewRefreshInfo()
    : mview_table_id_(OB_INVALID_ID),
      last_refresh_scn_(),
      refresh_scn_(),
      start_time_(OB_INVALID_TIMESTAMP),
      is_mview_complete_refresh_(false),
      mview_target_data_sync_scn_(),
      select_sql_()
  {
  }
  ~ObMViewRefreshInfo() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewRefreshInfo &other);
  TO_STRING_KV(K_(mview_table_id),
               K_(last_refresh_scn),
               K_(refresh_scn),
               K_(start_time),
               K_(is_mview_complete_refresh),
               K_(mview_target_data_sync_scn),
               K_(select_sql));
public:
  uint64_t mview_table_id_;
  share::SCN last_refresh_scn_;
  share::SCN refresh_scn_;
  int64_t start_time_;
  bool is_mview_complete_refresh_;
  share::SCN mview_target_data_sync_scn_;
  ObString select_sql_;
};

struct ObAlterMViewArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterMViewArg(): 
    exec_env_(),
    is_alter_on_query_computation_(false),
    enable_on_query_computation_(false),
    is_alter_query_rewrite_(false),
    enable_query_rewrite_(false),
    is_alter_refresh_method_(false),
    refresh_method_(share::schema::ObMVRefreshMethod::MAX),
    is_alter_refresh_dop_(false),
    refresh_dop_(0),
    is_alter_refresh_start_(false),
    start_time_(),
    is_alter_refresh_next_(false),
    next_time_expr_(),
    is_alter_nested_refresh_mode_(false),
    nested_refresh_mode_(share::schema::ObMVNestedRefreshMode::MAX)
  {
  }
  ~ObAlterMViewArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterMViewArg &other);

  TO_STRING_KV(K_(exec_env),
               K_(is_alter_on_query_computation),
               K_(enable_on_query_computation),
               K_(is_alter_query_rewrite),
               K_(enable_query_rewrite),
               K_(is_alter_refresh_method),
               K_(refresh_method),
               K_(is_alter_refresh_dop),
               K_(refresh_dop),
               K_(is_alter_refresh_start),
               K_(start_time),
               K_(is_alter_refresh_next),
               K_(next_time_expr),
               K_(is_alter_nested_refresh_mode),
               K_(nested_refresh_mode));
public:
  void set_exec_env(const ObString &exec_env)
  {
    exec_env_ = exec_env;
  }
  void set_enable_on_query_computation(bool enable)
  {
    is_alter_on_query_computation_ = true;
    enable_on_query_computation_ = enable;
  }
  void set_enable_query_rewrite(bool enable)
  {
    is_alter_query_rewrite_ = true;
    enable_query_rewrite_ = enable;
  }
  void set_refresh_method(share::schema::ObMVRefreshMethod refresh_method)
  {
    is_alter_refresh_method_ = true;
    refresh_method_ = refresh_method;
  }
  void set_refresh_dop(int64_t refresh_dop)
  {
    is_alter_refresh_dop_ = true;
    refresh_dop_ = refresh_dop;
  }
  void set_start_time(int64_t start_time)
  {
    is_alter_refresh_start_ = true;
    start_time_.set_timestamp(start_time);
  }
  void set_next_time_expr(const ObString &next_time_expr)
  {
    is_alter_refresh_next_ = true;
    next_time_expr_ = next_time_expr;
  }
  void set_alter_nested_refresh_mode(const share::schema::ObMVNestedRefreshMode nested_refresh_mode)
  {
    is_alter_nested_refresh_mode_ = true;
    nested_refresh_mode_ = nested_refresh_mode;
  }
  const ObString &get_exec_env() const { return exec_env_; }
  bool is_alter_on_query_computation() const { return is_alter_on_query_computation_; }
  bool get_enable_on_query_computation() const { return enable_on_query_computation_; }
  bool is_alter_query_rewrite() const { return is_alter_query_rewrite_; }
  bool get_enable_query_rewrite() const { return enable_query_rewrite_; }
  bool is_alter_refresh_method() const { return is_alter_refresh_method_; }
  share::schema::ObMVRefreshMethod get_refresh_method() const { return refresh_method_; }
  bool is_alter_refresh_dop() const { return is_alter_refresh_dop_; }
  int64_t get_refresh_dop() const { return refresh_dop_; }
  bool is_alter_refresh_start() const { return is_alter_refresh_start_; }
  const common::ObObj &get_start_time() const { return start_time_; }
  bool is_alter_refresh_next() const { return is_alter_refresh_next_; }
  const ObString &get_next_time_expr() const { return next_time_expr_; }
  bool is_alter_nested_refresh_mode() const { return is_alter_nested_refresh_mode_; }
  share::schema::ObMVNestedRefreshMode get_nested_refresh_mode() const { return nested_refresh_mode_; }
private:
  ObString exec_env_;
  bool is_alter_on_query_computation_;
  bool enable_on_query_computation_;
  bool is_alter_query_rewrite_;
  bool enable_query_rewrite_;
  bool is_alter_refresh_method_;
  share::schema::ObMVRefreshMethod refresh_method_;
  bool is_alter_refresh_dop_;
  int64_t refresh_dop_;
  bool is_alter_refresh_start_;
  common::ObObj start_time_;
  bool is_alter_refresh_next_;
  ObString next_time_expr_;
  bool is_alter_nested_refresh_mode_;
  share::schema::ObMVNestedRefreshMode nested_refresh_mode_;
};

struct ObAlterMLogArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterMLogArg() :
    exec_env_(),
    is_alter_table_dop_(false),
    table_dop_(0),
    is_alter_purge_start_(false),
    start_time_(),
    is_alter_purge_next_(false),
    next_time_expr_(),
    is_alter_lob_threshold_(false),
    lob_threshold_(0)
  {
  }
  ~ObAlterMLogArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterMLogArg &other);
  
  TO_STRING_KV(K_(exec_env),
               K_(is_alter_table_dop),
               K_(table_dop),
               K_(is_alter_purge_start),
               K_(start_time),
               K_(is_alter_purge_next),
               K_(next_time_expr),
               K_(is_alter_lob_threshold),
               K_(lob_threshold));
public:
  void set_exec_env(const ObString &exec_env)
  {
    exec_env_ = exec_env;
  }
  void set_table_dop(int64_t dop)
  {
    is_alter_table_dop_ = true;
    table_dop_ = dop;
  }
  void set_start_time(int64_t start_time)
  {
    is_alter_purge_start_ = true;
    start_time_.set_timestamp(start_time);
  }
  void set_next_time_expr(const ObString &next_time_expr)
  {
    is_alter_purge_next_ = true;
    next_time_expr_ = next_time_expr;
  }
  void set_lob_threshold(int64_t lob_threshold)
  {
    is_alter_lob_threshold_ = true;
    lob_threshold_ = lob_threshold;
  }
  const ObString &get_exec_env() const { return exec_env_; }
  bool is_alter_table_dop() const { return is_alter_table_dop_; }
  int64_t get_table_dop() const { return table_dop_; }
  bool is_alter_purge_start() const { return is_alter_purge_start_; }
  const common::ObObj &get_start_time() const { return start_time_; }
  bool is_alter_purge_next() const { return is_alter_purge_next_; }
  const ObString &get_next_time_expr() const { return next_time_expr_; }
  bool is_alter_lob_threshold() const { return is_alter_lob_threshold_; }
  int64_t get_lob_threshold() const { return lob_threshold_; }
private:
  ObString exec_env_;
  bool is_alter_table_dop_;
  int64_t table_dop_;
  bool is_alter_purge_start_;
  common::ObObj start_time_;
  bool is_alter_purge_next_;
  ObString next_time_expr_;
  bool is_alter_lob_threshold_;
  int64_t lob_threshold_;
};

struct ObCreateMLogArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateMLogArg()
      : ObDDLArg(),
        database_name_(),
        table_name_(),
        mlog_name_(),
        tenant_id_(OB_INVALID_TENANT_ID),
        base_table_id_(common::OB_INVALID_ID),
        mlog_table_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        with_rowid_(false),
        with_primary_key_(false),
        with_sequence_(false),
        include_new_values_(false),
        purge_options_(),
        mlog_schema_(),
        store_columns_(),
        nls_date_format_(),
        nls_timestamp_format_(),
        nls_timestamp_tz_format_(),
        sql_mode_(0),
        replace_if_exists_(false),
        create_tmp_mlog_(false)
  {

  }
  virtual ~ObCreateMLogArg() {}
  bool is_valid() const;
  int assign(const ObCreateMLogArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "failed to assign base", KR(ret));
    } else if (OB_FAIL(mlog_schema_.assign(other.mlog_schema_))) {
      SHARE_LOG(WARN, "failed to assign mlog schema", KR(ret));
    } else if (OB_FAIL(store_columns_.assign(other.store_columns_))) {
      SHARE_LOG(WARN, "failed to assign store columns", KR(ret));
    } else {
      database_name_ = other.database_name_;
      table_name_ = other.table_name_;
      mlog_name_ = other.mlog_name_;
      tenant_id_ = other.tenant_id_;
      base_table_id_ = other.base_table_id_;
      mlog_table_id_ = other.mlog_table_id_;
      session_id_ = other.session_id_;
      with_rowid_ = other.with_rowid_;
      with_primary_key_ = other.with_primary_key_;
      with_sequence_ = other.with_sequence_;
      include_new_values_ = other.include_new_values_;
      purge_options_ = other.purge_options_;
      nls_date_format_ = other.nls_date_format_;
      nls_timestamp_format_ = other.nls_timestamp_format_;
      nls_timestamp_tz_format_ = other.nls_timestamp_tz_format_;
      sql_mode_ = other.sql_mode_;
      replace_if_exists_ = other.replace_if_exists_;
      create_tmp_mlog_ = other.create_tmp_mlog_;
    }

    return ret;
  }
  void reset()
  {
    database_name_.reset();
    table_name_.reset();
    mlog_name_.reset();
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    base_table_id_ = common::OB_INVALID_ID;
    mlog_table_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    with_rowid_ = false;
    with_primary_key_ = false;
    with_sequence_ = false;
    include_new_values_ = false;
    purge_options_.reset();
    mlog_schema_.reset();
    store_columns_.reset();
    nls_date_format_.reset();
    nls_timestamp_format_.reset();
    nls_timestamp_tz_format_.reset();
    sql_mode_ = 0;
    replace_if_exists_ = false;
    create_tmp_mlog_ = false;
    ObDDLArg::reset();
  }
  DECLARE_TO_STRING;

  struct PurgeOptions {
    OB_UNIS_VERSION(1);
  public:
    PurgeOptions() : purge_mode_(share::schema::ObMLogPurgeMode::MAX)
    {
    }
    ~PurgeOptions() {}
    void reset()
    {
      purge_mode_ = share::schema::ObMLogPurgeMode::MAX;
      start_datetime_expr_.reset();
      next_datetime_expr_.reset();
      exec_env_.reset();
    }
    bool is_valid() const
    {
      return (share::schema::ObMLogPurgeMode::MAX != purge_mode_) && !exec_env_.empty();
    }
    TO_STRING_KV(K_(purge_mode),
                 K_(start_datetime_expr),
                 K_(next_datetime_expr),
                 K_(exec_env));
    share::schema::ObMLogPurgeMode purge_mode_;
    common::ObObj start_datetime_expr_;
    common::ObString next_datetime_expr_;
    common::ObString exec_env_;
  };
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString mlog_name_; // for privilege check
  uint64_t tenant_id_;
  uint64_t base_table_id_;
  uint64_t mlog_table_id_;
  uint64_t session_id_;
  bool with_rowid_;
  bool with_primary_key_;
  bool with_sequence_;
  bool include_new_values_;
  PurgeOptions purge_options_;
  share::schema::ObTableSchema mlog_schema_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> store_columns_;
  common::ObString nls_date_format_;
  common::ObString nls_timestamp_format_;
  common::ObString nls_timestamp_tz_format_;
  ObSQLMode sql_mode_;
  bool replace_if_exists_;
  bool create_tmp_mlog_;
};

struct ObCreateMLogRes
{
  OB_UNIS_VERSION(1);
public:
  ObCreateMLogRes()
      : mlog_table_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        task_id_(0)
  {}
  void reset()
  {
    mlog_table_id_ = OB_INVALID_ID;
    schema_version_ = OB_INVALID_VERSION;
    task_id_ = 0;
  }
  int assign(const ObCreateMLogRes &other) {
    int ret = common::OB_SUCCESS;
    mlog_table_id_ = other.mlog_table_id_;
    schema_version_ = other.schema_version_;
    task_id_ = other.task_id_;
    return ret;
  }
public:
  TO_STRING_KV(K_(mlog_table_id), K_(schema_version), K_(task_id));
  uint64_t mlog_table_id_;
  int64_t schema_version_;
  int64_t task_id_;
};

} // namespace obrpc
} // namespace oceanbase
#endif