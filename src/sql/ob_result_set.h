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

#ifndef OCEANBASE_SQL_OB_RESULT_SET_H
#define OCEANBASE_SQL_OB_RESULT_SET_H
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_fast_array.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_2d_array.h"
#include "lib/timezone/ob_timezone_info.h"
#include "rpc/obmysql/ob_mysql_global.h"  // for EMySQLFieldType
#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "common/ob_string_buf.h"
#include "share/ob_srv_rpc_proxy.h"
#include "common/ob_field.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/executor/ob_executor.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/executor/ob_cmd_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/ob_sql_partition_location_cache.h"

namespace oceanbase {
namespace obmysql {
class ObMySQLField;
}
namespace sql {
class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObLogPlan;
struct ObPsStoreItemValue;
class ObIEndTransCallback;
typedef common::ObFastArray<int64_t, OB_DEFAULT_SE_ARRAY_COUNT> IntFastArray;
typedef common::ObFastArray<uint64_t, OB_DEFAULT_SE_ARRAY_COUNT> UIntFastArray;
typedef common::ObFastArray<ObRawExpr*, OB_DEFAULT_SE_ARRAY_COUNT> RawExprFastArray;
// query result set
class ObResultSet {
public:
  class ExternalRetrieveInfo {
  public:
    ExternalRetrieveInfo(common::ObIAllocator& allocator)
        : allocator_(allocator),
          external_params_(allocator),
          into_exprs_(allocator),
          ref_objects_(allocator),
          route_sql_(),
          is_select_for_update_(false),
          has_hidden_rowid_(false)
    {}
    virtual ~ExternalRetrieveInfo()
    {}

    int build(ObStmt& stmt, ObSQLSessionInfo& session_info,
        common::ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>& param_info);
    int check_into_exprs(ObStmt& stmt, ObArray<ObDataType>& basic_types, ObBitSet<>& basic_into);
    const ObIArray<ObRawExpr*>& get_into_exprs() const
    {
      return into_exprs_;
    }
    int recount_dynamic_param_info(ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>& param_info);

    common::ObIAllocator& allocator_;
    common::ObFixedArray<ObRawExpr*, common::ObIAllocator> external_params_;
    common::ObFixedArray<ObRawExpr*, common::ObIAllocator> into_exprs_;
    common::ObFixedArray<share::schema::ObSchemaObjVersion, common::ObIAllocator> ref_objects_;
    ObString route_sql_;
    bool is_select_for_update_;
    bool has_hidden_rowid_;
  };

  enum PsMode {
    NO_PS = 0,  // not PS protocol
    SIMPLE_PS,  // only support the pl under mysql mode
    STD_PS,     // The standard PS, work for the oracle mode, and for the ps protocol of the mysql mode
  };

  typedef common::ObFastArray<ObPhysicalPlan*, 8> CandidatePlanArray;

public:
  explicit ObResultSet(ObSQLSessionInfo& session);
  explicit ObResultSet(ObSQLSessionInfo& session, common::ObIAllocator& allocator);
  virtual ~ObResultSet();

  static ObResultSet* alloc(ObSQLSessionInfo& session, common::ObIAllocator& allocator);
  static void free(ObResultSet* rs);
  static ObResultSet* assign(ObResultSet* other);
  /// open and execute the execution plan
  /// @note SHOULD be called for all statement even if there is no result rows
  int sync_open();
  int execute();
  int open();
  /// get the next result row
  /// @return OB_ITER_END when no more data available
  int get_next_row(const common::ObNewRow*& row);
  /// close the result set after get all the rows
  int close(bool need_retry = false);
  /// get number of rows affected by INSERT/UPDATE/DELETE
  int64_t get_affected_rows() const;
  int64_t get_return_rows() const
  {
    return return_rows_;
  }
  /// get warning count during the execution
  int64_t get_warning_count() const;
  /// get statement id
  uint64_t get_statement_id() const;
  int64_t get_sql_id() const
  {
    return sql_id_;
  };
  /// get the server's error message
  const char* get_message() const;
  /// get the server's error code
  int get_errcode() const;
  /**
   * get the row description
   * the row desc should have been valid after open() and before close()
   * @pre call open() first
   */
  int get_row_desc(const common::ObRowDesc*& row_desc) const;
  /// get the field columns
  const common::ColumnsFieldIArray* get_field_columns() const;
  const common::ParamsFieldIArray* get_param_fields() const;

  uint64_t get_field_cnt() const;

  void set_p_param_fileds(common::ParamsFieldIArray* p_param_columns)
  {
    p_param_columns_ = p_param_columns;
  }

  void set_p_column_fileds(common::ColumnsFieldIArray* p_columns_field)
  {
    p_field_columns_ = p_columns_field;
  }
  void set_exec_result(ObIExecuteResult* exec_result)
  {
    exec_result_ = exec_result;
  }
  ExternalRetrieveInfo& get_external_retrieve_info()
  {
    return external_retrieve_info_;
  }
  ObIArray<ObRawExpr*>& get_external_params();
  ObIArray<ObRawExpr*>& get_into_exprs();
  common::ObIArray<share::schema::ObSchemaObjVersion>& get_ref_objects();
  const common::ObIArray<share::schema::ObSchemaObjVersion>& get_ref_objects() const;
  ObString& get_route_sql();
  bool get_is_select_for_update();
  inline bool has_hidden_rowid();
  /// whether the result is with rows (true for SELECT statement)
  bool is_with_rows() const;
  // tell mysql if need to do async end trans
  bool need_end_trans_callback() const;
  bool need_end_trans() const;
  /// get physical plan
  // ObPhysicalPlan *get_physical_plan();
  ObPhysicalPlan*& get_physical_plan();
  /// to string
  int64_t to_string(char* buf, const int64_t buf_len) const;
  /// whether the statement is a prepared statment
  bool is_prepare_stmt() const;
  /// whether the statement is SHOW WARNINGS
  bool is_show_warnings() const;
  bool is_compound_stmt() const;
  bool is_dml_stmt(stmt::StmtType stmt_type) const;
  bool is_pl_stmt(stmt::StmtType stmt_type) const;
  stmt::StmtType get_stmt_type() const;
  stmt::StmtType get_inner_stmt_type() const;
  stmt::StmtType get_literal_stmt_type() const;
  int64_t get_query_string_id() const;
  int refresh_location_cache(bool is_nonblock);
  int check_and_nonblock_refresh_location_cache();
  bool need_execute_remote_sql_async() const
  {
    return get_exec_context().use_remote_sql() && !is_inner_result_set_;
  }

  ////////////////////////////////////////////////////////////////
  // the following methods are used by the ob_sql module internally
  /// add a field columns
  int init()
  {
    return common::OB_SUCCESS;
  }
  common::ObIAllocator& get_mem_pool()
  {
    return mem_pool_;
  }
  ObExecContext& get_exec_context()
  {
    return exec_ctx_ != nullptr ? *exec_ctx_ : inner_exec_ctx_;
  }
  void set_exec_context(ObExecContext& exec_ctx)
  {
    exec_ctx_ = &exec_ctx;
  }
  const ObExecContext& get_exec_context() const
  {
    return exec_ctx_ != nullptr ? *exec_ctx_ : inner_exec_ctx_;
  }
  int reserve_field_columns(int64_t size)
  {
    return field_columns_.reserve(size);
  };
  int reserve_param_columns(int64_t size)
  {
    return param_columns_.reserve(size);
  };
  int add_field_column(const common::ObField& field);
  int add_param_column(const common::ObField& field);
  int from_plan(const ObPhysicalPlan& phy_plan, const common::ObIArray<ObPCParam*>& raw_params);
  int to_plan(const bool is_ps_mode, ObPhysicalPlan* phy_plan);
  const common::ObString& get_statement_name() const;
  void set_statement_id(const uint64_t stmt_id);
  void set_statement_name(const common::ObString name);
  void set_message(const char* message);
  bool need_rollback(int ret, int errcode, bool is_error_ignored) const;
  void set_errcode(int code);
  void set_affected_rows(const int64_t& affected_rows);
  int set_mysql_info();
  void set_last_insert_id_session(const uint64_t last_insert_id);
  uint64_t get_last_insert_id_session();
  void set_last_insert_id_to_client(const uint64_t last_insert_id);
  uint64_t get_last_insert_id_to_client();
  void set_warning_count(const int64_t& warning_count);
  void set_physical_plan(const CacheRefHandleID ref_handle, ObPhysicalPlan* physical_plan);
  void set_cmd(ObICmd* cmd);
  bool is_end_trans_async();
  void set_end_trans_async(bool is_async);
  void set_mysql_end_trans_callback(ObIEndTransCallback* cb);
  void fields_clear();
  void set_stmt_type(stmt::StmtType stmt_type);
  void set_inner_stmt_type(stmt::StmtType stmt_type);
  void set_literal_stmt_type(stmt::StmtType stmt_type);
  void set_compound_stmt(bool compound);
  ObSQLSessionInfo& get_session()
  {
    return my_session_;
  }
  const ObSQLSessionInfo& get_session() const
  {
    return my_session_;
  };
  void set_ps_transformer_allocator(common::ObArenaAllocator* allocator);
  void set_query_string_id(int64_t query_string_id);
  void set_sql_id(int64_t sql_id)
  {
    sql_id_ = sql_id;
  };
  void set_begin_timestamp(const int64_t begin_ts);
  int start_stmt();
  int end_stmt(const bool is_rollback);
  int start_trans();
  int end_trans(const bool is_rollback);
  int start_participant();
  int end_participant(const bool is_rollback);
  void set_is_from_plan_cache(bool is_from_plan_cache)
  {
    is_from_plan_cache_ = is_from_plan_cache;
  }
  void set_is_inner_result_set(const bool v)
  {
    is_inner_result_set_ = v;
  }
  bool get_is_from_plan_cache() const
  {
    return is_from_plan_cache_;
  }
  const ObICmd* get_cmd() const
  {
    return cmd_;
  }
  ObICmd* get_cmd()
  {
    return cmd_;
  }
  void init_partition_location_cache(share::ObIPartitionLocationCache* loc_cache, common::ObAddr self_addr,
      share::schema::ObSchemaGetterGuard* schema_guard)
  {
    sql_location_cache_.init(loc_cache, self_addr, schema_guard);
    self_addr_ = self_addr;
  }
  ObSqlPartitionLocationCache& get_partition_location_cache()
  {
    return sql_location_cache_;
  }
  void set_has_top_limit(const bool has_limit);
  void set_is_calc_found_rows(const bool is_calc_found_rows);
  bool get_has_top_limit() const;
  bool is_calc_found_rows() const;
  // Determine whether an asynchronous EndTrans request has been submitted.
  // If the request has been submitted, the client needs to wait for the asynchronous response packet.
  // ref: obmp_query.cpp, ob_mysql_end_trans_callback.cpp
  bool is_async_end_trans_submitted() const
  {
    return get_exec_context().get_trans_state().is_end_trans_executed();
  }
  inline TransState& get_trans_state()
  {
    return get_exec_context().get_trans_state();
  }
  inline const TransState& get_trans_state() const
  {
    return get_exec_context().get_trans_state();
  }
  int update_last_insert_id();
  int update_is_result_accurate();
  bool is_ps_protocol() const
  {
    return STD_PS == ps_protocol_;
  };
  void set_ps_protocol()
  {
    ps_protocol_ = STD_PS;
  }
  bool is_simple_ps_protocol() const
  {
    return SIMPLE_PS == ps_protocol_;
  };
  void set_simple_ps_protocol()
  {
    ps_protocol_ = SIMPLE_PS;
  }
  int get_read_consistency(ObConsistencyLevel& consistency);
  void set_has_global_variable(bool has_global_variable)
  {
    has_global_variable_ = has_global_variable;
  }
  bool has_global_variable() const
  {
    return has_global_variable_;
  }
  void set_returning(bool is_returning)
  {
    is_returning_ = is_returning;
  }
  bool is_returning() const
  {
    return is_returning_;
  }
  void set_user_sql(bool is_user_sql)
  {
    is_user_sql_ = is_user_sql;
  }
  bool is_user_sql() const
  {
    return is_user_sql_;
  }

  // Fill parameter information with the field name
  // noted that, except for cname_, the rest of the strings are all copied from the plan.
  // The memory of cname_ is allocated by the allocator of result_set
  int construct_field_name(const common::ObIArray<ObPCParam*>& raw_params, const bool is_first_parse);

  int construct_display_field_name(
      common::ObField& field, const ObIArray<ObPCParam*>& raw_params, const bool is_first_parse);

  // The field columns in the deep copy plan are stored in the field_columns_ member
  int copy_field_columns(const ObPhysicalPlan& plan);
  bool has_implicit_cursor() const;
  int switch_implicit_cursor();
  void reset_implicit_cursor_idx()
  {
    if (get_exec_context().get_physical_plan_ctx() != nullptr)
      get_exec_context().get_physical_plan_ctx()->set_cur_stmt_id(0);
  }
  bool is_cursor_end() const;

  inline void set_ref_handle(const CacheRefHandleID handle_id)
  {
    ref_handle_id_ = handle_id;
  }
  inline CacheRefHandleID get_ref_handle()
  {
    return ref_handle_id_;
  }
  inline bool can_execute_async() const
  {
    return get_exec_context().use_remote_sql() && physical_plan_ != nullptr &&
           physical_plan_->get_location_type() != OB_PHY_PLAN_UNCERTAIN;
    // temporarily prevent the global index plan being executed asynchronously
  }
  void set_is_com_filed_list()
  {
    is_com_filed_list_ = true;
  }
  bool get_is_com_filed_list()
  {
    return is_com_filed_list_;
  }
  void set_wildcard_string(common::ObString string)
  {
    wild_str_ = string;
  }
  common::ObString& get_wildcard_string()
  {
    return wild_str_;
  }
  static void replace_lob_type(const ObSQLSessionInfo& session, const ObField& field, obmysql::ObMySQLField& mfield);

private:
  // types and constants
  static const int64_t TRANSACTION_SET_VIOLATION_MAX_RETRY = 3;

private:
  // disallow copy
  ObResultSet(const ObResultSet& other);
  ObResultSet& operator=(const ObResultSet& other);
  // function members

  int open_plan();
  int open_cmd();
  int do_open_plan(ObExecContext& ctx);
  int do_close_plan(int errcode, ObExecContext& ctx);
  bool transaction_set_violation_and_retry(int& err, int64_t& retry);
  int init_cmd_exec_context(ObExecContext& exec_ctx);
  int on_cmd_execute();
  int auto_start_plan_trans();
  int auto_end_plan_trans(int ret, bool need_retry, bool& async);

  void store_affected_rows(ObPhysicalPlanCtx& plan_ctx);
  void store_found_rows(ObPhysicalPlanCtx& plan_ctx);
  int store_last_insert_id(ObExecContext& ctx);
  int drive_pdml_query();

  // make final field name
  int make_final_field_name(char* src, int64_t len, common::ObString& field_name);
  int prepare_mock_schemas();
  int rm_mock_schemas();
  // delete useless spaces
  static int64_t remove_extra_space(char* buff, int64_t len);

protected:
  bool is_user_sql_;

private:
  // data members
  common::ObArenaAllocator inner_mem_pool_;
  common::ObIAllocator& mem_pool_;

  uint64_t statement_id_;
  int64_t sql_id_;
  int64_t affected_rows_;  // number of rows affected by INSERT/UPDATE/DELETE
  int64_t return_rows_;
  uint64_t last_insert_id_session_;
  uint64_t last_insert_id_to_client_;
  int64_t warning_count_;
  common::ObString statement_name_;
  char message_[common::MSG_SIZE];  // null terminated message string
  common::ColumnsFieldArray field_columns_;
  common::ParamsFieldArray param_columns_;

  const common::ColumnsFieldIArray* p_field_columns_;
  const common::ParamsFieldIArray* p_param_columns_;
  ObPhysicalPlan* physical_plan_;
  stmt::StmtType stmt_type_;
  // for a prepared SELECT, stmt_type_ is T_PREPARE
  // but in perf stat we want inner info, i.e. SELECT.
  stmt::StmtType inner_stmt_type_;
  stmt::StmtType literal_stmt_type_;
  char external_retrieve_info_buf_[sizeof(ExternalRetrieveInfo)];
  ExternalRetrieveInfo& external_retrieve_info_;
  bool compound_;
  bool has_global_variable_;  // there are global variables
  /*
   * ob_sql.h is the external interface of the SQL module,
   * the outside world does not need to know ObExecContext
   * Therefore, exec_ctx_ is a member of ObResultSet and is not exposed outside of SQL
   */
  int errcode_;
  ObSQLSessionInfo& my_session_;  // The session who owns this result set
  int64_t begin_timestamp_;
  ObIExecuteResult* exec_result_;
  ObICmd* cmd_;
  ObExecContext inner_exec_ctx_;
  ObExecContext* exec_ctx_;
  ObSqlPartitionLocationCache sql_location_cache_;
  bool is_from_plan_cache_;
  bool is_inner_result_set_;  // result set for inner sql execution.
  // mark whether there has limit operator
  bool has_top_limit_;
  bool is_calc_found_rows_;
  PsMode ps_protocol_;
  common::ObAddr self_addr_;
  // executor
  ObExecutor executor_;
  bool is_returning_;
  int64_t worker_count_;

  // physical plan ref handle id
  CacheRefHandleID ref_handle_id_;

  bool is_com_filed_list_;     // used to mark OB_MYSQL_COM_FIELD_LIST
  common::ObString wild_str_;  // uesd to save filed wildcard in OB_MYSQL_COM_FIELD_LIST;
};

// inline ObResultSet *ObResultSet::alloc(ObSQLSessionInfo &session, common::ObIAllocator &allocator)
//{
//  ObResultSet *rs = op_reclaim_alloc_args(ObResultSet, session, allocator);
//  if (rs != nullptr) {
//    rs->inc_ref_count();
//  }
//  return rs;
//}
//
// inline void ObResultSet::free(ObResultSet *rs)
//{
//  if (rs != nullptr) {
//    int64_t ref_count = rs->def_ref_count();
//    if (0 == ref_count) {
//      op_reclaim_free(rs);
//    }
//  }
//}
//
// inline ObResultSet *ObResultSet::assign(ObResultSet *other)
//{
//  if (other != nullptr) {
//    other->inc_ref_count();
//  }
//  return other;
//}

inline ObResultSet::ObResultSet(ObSQLSessionInfo& session, common::ObIAllocator& allocator)
    : is_user_sql_(false),
      inner_mem_pool_(),
      mem_pool_(allocator),
      statement_id_(common::OB_INVALID_ID),
      sql_id_(0),
      affected_rows_(0),
      return_rows_(0),
      last_insert_id_session_(0),
      last_insert_id_to_client_(0),
      warning_count_(0),
      statement_name_(),
      field_columns_(allocator),
      param_columns_(allocator),
      p_field_columns_(&field_columns_),
      p_param_columns_(&param_columns_),
      physical_plan_(NULL),
      stmt_type_(stmt::T_NONE),
      inner_stmt_type_(stmt::T_NONE),
      literal_stmt_type_(stmt::T_NONE),
      external_retrieve_info_(*new (external_retrieve_info_buf_) ExternalRetrieveInfo(allocator)),
      compound_(false),
      has_global_variable_(false),
      errcode_(0),
      my_session_(session),
      begin_timestamp_(0),
      exec_result_(nullptr),
      cmd_(NULL),
      inner_exec_ctx_(allocator),
      exec_ctx_(&inner_exec_ctx_),
      sql_location_cache_(),
      is_from_plan_cache_(false),
      is_inner_result_set_(false),
      has_top_limit_(false),
      is_calc_found_rows_(false),
      ps_protocol_(NO_PS),
      executor_(),
      is_returning_(false),
      worker_count_(0),
      ref_handle_id_(MAX_HANDLE),
      is_com_filed_list_(false),
      wild_str_()
{
  message_[0] = '\0';
}

inline ObResultSet::ObResultSet(ObSQLSessionInfo& session)
    : is_user_sql_(false),
      inner_mem_pool_(common::ObModIds::OB_RESULT_SET, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
      mem_pool_(inner_mem_pool_),
      statement_id_(common::OB_INVALID_ID),
      sql_id_(0),
      affected_rows_(0),
      return_rows_(0),
      last_insert_id_session_(0),
      last_insert_id_to_client_(0),
      warning_count_(0),
      statement_name_(),
      field_columns_(inner_mem_pool_),
      param_columns_(inner_mem_pool_),
      p_field_columns_(&field_columns_),
      p_param_columns_(&param_columns_),
      physical_plan_(NULL),
      stmt_type_(stmt::T_NONE),
      inner_stmt_type_(stmt::T_NONE),
      literal_stmt_type_(stmt::T_NONE),
      external_retrieve_info_(*new (external_retrieve_info_buf_) ExternalRetrieveInfo(inner_mem_pool_)),
      compound_(false),
      has_global_variable_(false),
      errcode_(0),
      my_session_(session),
      begin_timestamp_(0),
      exec_result_(nullptr),
      cmd_(NULL),
      inner_exec_ctx_(),
      exec_ctx_(&inner_exec_ctx_),
      sql_location_cache_(),
      is_from_plan_cache_(false),
      is_inner_result_set_(false),
      has_top_limit_(false),
      is_calc_found_rows_(false),
      ps_protocol_(NO_PS),
      executor_(),
      is_returning_(false),
      worker_count_(0),
      ref_handle_id_(MAX_HANDLE),
      is_com_filed_list_(false),
      wild_str_()
{
  message_[0] = '\0';
}

inline int64_t ObResultSet::get_affected_rows() const
{
  return affected_rows_;
}

inline int64_t ObResultSet::get_warning_count() const
{
  return warning_count_;
}

inline uint64_t ObResultSet::get_statement_id() const
{
  return statement_id_;
}

inline const common::ObString& ObResultSet::get_statement_name() const
{
  return statement_name_;
}

inline const char* ObResultSet::get_message() const
{
  return message_;
}

inline int ObResultSet::get_errcode() const
{
  return errcode_;
}

inline void ObResultSet::set_statement_id(const uint64_t stmt_id)
{
  statement_id_ = stmt_id;
}

inline void ObResultSet::set_message(const char* message)
{
  snprintf(message_, common::MSG_SIZE, "%s", message);
}

inline void ObResultSet::set_errcode(int code)
{
  errcode_ = code;
}

inline int ObResultSet::add_field_column(const common::ObField& field)
{
  return field_columns_.push_back(field);
}

inline int ObResultSet::add_param_column(const common::ObField& param)
{
  return param_columns_.push_back(param);
}

inline const common::ColumnsFieldIArray* ObResultSet::get_field_columns() const
{
  return p_field_columns_;
}

inline const common::ParamsFieldIArray* ObResultSet::get_param_fields() const
{
  return p_param_columns_;
}

inline ObIArray<ObRawExpr*>& ObResultSet::get_external_params()
{
  return external_retrieve_info_.external_params_;
}

inline ObIArray<ObRawExpr*>& ObResultSet::get_into_exprs()
{
  return external_retrieve_info_.into_exprs_;
}

inline const common::ObIArray<share::schema::ObSchemaObjVersion>& ObResultSet::get_ref_objects() const
{
  return external_retrieve_info_.ref_objects_;
}

inline common::ObIArray<share::schema::ObSchemaObjVersion>& ObResultSet::get_ref_objects()
{
  return external_retrieve_info_.ref_objects_;
}

inline ObString& ObResultSet::get_route_sql()
{
  return external_retrieve_info_.route_sql_;
}

inline bool ObResultSet::get_is_select_for_update()
{
  return external_retrieve_info_.is_select_for_update_;
}

inline bool ObResultSet::has_hidden_rowid()
{
  return external_retrieve_info_.has_hidden_rowid_;
}

inline bool ObResultSet::is_with_rows() const
{
  return (p_field_columns_->count() > 0 && !is_prepare_stmt());
}

inline void ObResultSet::set_affected_rows(const int64_t& affected_rows)
{
  affected_rows_ = affected_rows;
}

inline void ObResultSet::set_last_insert_id_session(const uint64_t last_insert_id)
{
  last_insert_id_session_ = last_insert_id;
}
inline uint64_t ObResultSet::get_last_insert_id_session()
{
  return last_insert_id_session_;
}

inline void ObResultSet::set_last_insert_id_to_client(const uint64_t last_insert_id)
{
  last_insert_id_to_client_ = last_insert_id;
}
inline uint64_t ObResultSet::get_last_insert_id_to_client()
{
  return last_insert_id_to_client_;
}

inline void ObResultSet::set_warning_count(const int64_t& warning_count)
{
  warning_count_ = warning_count;
}

inline int64_t ObResultSet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "stmt_type=%d ", stmt_type_);
  common::databuff_printf(buf, buf_len, pos, "is_with_rows=%c ", this->is_with_rows() ? 'Y' : 'N');
  common::databuff_printf(buf, buf_len, pos, "affected_rows=%ld ", affected_rows_);
  common::databuff_printf(buf, buf_len, pos, "warning_count=%ld ", warning_count_);
  common::databuff_printf(buf, buf_len, pos, "field_count=%ld ", field_columns_.count());
  common::databuff_printf(buf, buf_len, pos, "message=%s ", message_);
  common::databuff_printf(buf, buf_len, pos, "stmt_id=%lu ", statement_id_);
  common::databuff_printf(buf, buf_len, pos, "stmt_name=%.*s ", statement_name_.length(), statement_name_.ptr());
  common::databuff_printf(buf, buf_len, pos, "sql_id=%lu ", sql_id_);
  return pos;
}

inline void ObResultSet::set_end_trans_async(bool is_async)
{
  get_exec_context().set_end_trans_async(is_async);
}

inline bool ObResultSet::is_end_trans_async()
{
  return get_exec_context().is_end_trans_async();
}

inline void ObResultSet::set_cmd(ObICmd* cmd)
{
  cmd_ = cmd;
}

inline void ObResultSet::set_physical_plan(const CacheRefHandleID ref_handle, ObPhysicalPlan* physical_plan)
{
  physical_plan_ = physical_plan;
  if (NULL != physical_plan) {
    set_ref_handle(ref_handle);
  }
}

inline void ObResultSet::fields_clear()
{
  affected_rows_ = 0;
  return_rows_ = 0;
  warning_count_ = 0;
  message_[0] = '\0';
  field_columns_.reset();
  param_columns_.reset();
  p_field_columns_ = &field_columns_;
  p_param_columns_ = &param_columns_;
}

inline int ObResultSet::get_row_desc(const common::ObRowDesc*& row_desc) const
{
  UNUSED(row_desc);
  return common::OB_SUCCESS;
}

inline bool ObResultSet::is_prepare_stmt() const
{
  return stmt::T_PREPARE == stmt_type_;
}

inline void ObResultSet::set_stmt_type(stmt::StmtType stmt_type)
{
  stmt_type_ = stmt_type;
}

inline void ObResultSet::set_inner_stmt_type(stmt::StmtType stmt_type)
{
  inner_stmt_type_ = stmt_type;
}

inline void ObResultSet::set_compound_stmt(bool compound)
{
  compound_ = compound;
}

inline bool ObResultSet::is_show_warnings() const
{
  return stmt::T_SHOW_WARNINGS == stmt_type_;
}

inline stmt::StmtType ObResultSet::get_stmt_type() const
{
  return stmt_type_;
}

inline stmt::StmtType ObResultSet::get_inner_stmt_type() const
{
  return inner_stmt_type_;
}

inline stmt::StmtType ObResultSet::get_literal_stmt_type() const
{
  return literal_stmt_type_;
}

inline void ObResultSet::set_literal_stmt_type(stmt::StmtType stmt_type)
{
  literal_stmt_type_ = stmt_type;
}

inline bool ObResultSet::is_compound_stmt() const
{
  return compound_;
}

inline bool ObResultSet::is_dml_stmt(stmt::StmtType stmt_type) const
{
  return (stmt::T_SELECT == stmt_type || stmt::T_INSERT == stmt_type || stmt::T_REPLACE == stmt_type ||
          stmt::T_MERGE == stmt_type || stmt::T_DELETE == stmt_type || stmt::T_UPDATE == stmt_type);
}

inline bool ObResultSet::is_pl_stmt(stmt::StmtType stmt_type) const
{
  return (stmt::T_CALL_PROCEDURE == stmt_type || stmt::T_ANONYMOUS_BLOCK == stmt_type);
}

inline void ObResultSet::set_begin_timestamp(const int64_t begin_ts)
{
  begin_timestamp_ = begin_ts;
}

inline void ObResultSet::set_has_top_limit(const bool has_limit)
{
  has_top_limit_ = has_limit;
}

inline void ObResultSet::set_is_calc_found_rows(const bool is_calc_found_rows)
{
  is_calc_found_rows_ = is_calc_found_rows;
}

inline bool ObResultSet::get_has_top_limit() const
{
  return has_top_limit_;
}

inline bool ObResultSet::is_calc_found_rows() const
{
  return is_calc_found_rows_;
}

inline ObPhysicalPlan*& ObResultSet::get_physical_plan()
{
  return physical_plan_;
}

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_RESULT_SET_H */
