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

#ifndef OCEANBASE_SRC_SQL_OB_SPI_H_
#define OCEANBASE_SRC_SQL_OB_SPI_H_

#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "ob_sql_utils.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_result_set.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/dblink/ob_pl_dblink_info.h"
#endif
namespace oceanbase
{
namespace observer
{
class ObInnerSQLConnection;
class ObITimeRecord;
class ObQueryRetryCtrl;
}
using common::ObPsStmtId;

namespace pl
{
class ObDbmsCursorInfo;
}

namespace sql
{
class ObExprObjAccess;

struct ObPLSPITraceIdGuard
{
  ObPLSPITraceIdGuard(const ObString &sql,
                      const ObString &ps_sql,
                      ObSQLSessionInfo &session,
                      int &ret,
                      ObCurTraceId::TraceId *reused_trace_id = nullptr);

  ~ObPLSPITraceIdGuard();

  ObCurTraceId::TraceId origin_trace_id_;
  const ObString sql_;
  const ObString ps_sql_;
  ObSQLSessionInfo& session_;
  int &ret_;
};

struct ObSPICursor
{
  ObSPICursor(ObIAllocator &allocator, sql::ObSQLSessionInfo* session_info) :
    row_store_(), row_desc_(), allocator_(&allocator), cur_(0), fields_(allocator), complex_objs_(),
    session_info_(session_info)
  {
    row_desc_.set_tenant_id(MTL_ID());
    complex_objs_.reset();
    complex_objs_.set_tenant_id(MTL_ID());
  }

  ~ObSPICursor()
  {
    for (int64_t i = 0; i < complex_objs_.count(); ++i) {
      (void)(pl::ObUserDefinedType::destruct_obj(complex_objs_.at(i), session_info_));
    }
  }

  ObRARowStore row_store_;
  ObArray<ObDataType> row_desc_; //ObRowStore里数据自带的Meta可能是T_NULL，所以这里自备一份
  ObIAllocator* allocator_;
  int64_t cur_;
  common::ColumnsFieldArray fields_;
  ObArray<ObObj> complex_objs_;
  sql::ObSQLSessionInfo* session_info_;
};

struct ObSPIOutParams
{
  ObSPIOutParams() : has_out_param_(false), out_params_() {}

  inline void reset()
  {
    has_out_param_ = false;
    out_params_.reset();
  }

  inline int push_back(const ObObj &v)
  {
    if (!v.is_null()) {
      has_out_param_ = true;
    }
    return out_params_.push_back(v);
  }

  inline bool has_out_param() { return has_out_param_; }
  inline void set_has_out_param(bool v) { has_out_param_ = v; }

  inline ObIArray<ObObj> &get_out_params() { return out_params_; }

  TO_STRING_KV(K_(has_out_param), K_(out_params));

private:
  bool has_out_param_;
  ObSEArray<ObObj, OB_DEFAULT_SE_ARRAY_COUNT> out_params_; // 用于记录function的返回值
};

class PlMemEntifyDestroyGuard
  {
  public:
    PlMemEntifyDestroyGuard(lib::MemoryContext &entity) : ref_(entity) {}
    ~PlMemEntifyDestroyGuard()
    {
      if (NULL != ref_) {
        DESTROY_CONTEXT(ref_);
        ref_ = NULL;
      }
    }
  private:
    lib::MemoryContext &ref_;
  };

class ObSPIResultSet
{
public:
  ObSPIResultSet()
    : is_inited_(false),
      need_end_nested_stmt_(EST_NEED_NOT),
      mem_context_(nullptr),
      mem_context_destroy_guard_(mem_context_),
      allocator_(ObModIds::OB_PL_TEMP, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      result_set_(NULL),
      sql_ctx_(),
      schema_guard_(share::schema::ObSchemaMgrItem::MOD_SPI_RESULT_SET),
      orign_nested_count_(-1),
      cursor_nested_count_(-1),
      orign_session_value_(NULL),
      cursor_session_value_(NULL),
      nested_session_value_(NULL),
      out_params_(),
      exec_params_str_() {
      }
  ~ObSPIResultSet() { reset(); }
  int init(sql::ObSQLSessionInfo &session_info);
  int close_result_set();
  void reset()
  {
    //result_set_.reset();
    if (is_inited_) {
      if (result_set_ != NULL) {
        result_set_->~ObResultSet();
      }
    }
    sql_ctx_.reset();
    schema_guard_.reset();
    need_end_nested_stmt_ = EST_NEED_NOT;
    orign_nested_count_ = -1;
    cursor_nested_count_ = -1;
    if (orign_session_value_ != NULL) {
      orign_session_value_->reset();
    }
    if (cursor_session_value_ != NULL) {
      cursor_session_value_->reset();
    }
    if (nested_session_value_ != NULL) {
      nested_session_value_->reset();
    }
    orign_session_value_ = NULL;
    cursor_session_value_ = NULL;
    nested_session_value_ = NULL;
    out_params_.reset();
    exec_params_str_.reset();
    allocator_.reset();
    is_inited_ = false;
  }
  void reset_member_for_retry(sql::ObSQLSessionInfo &session_info)
  {
    if (result_set_ != NULL) {
      result_set_->~ObResultSet();
    }
    sql_ctx_.reset();
    exec_params_str_.reset();
    //allocator_.reset();
    mem_context_->get_arena_allocator().reset();
    result_set_ = new (buf_) ObResultSet(session_info, mem_context_->get_arena_allocator());
    result_set_->get_exec_context().get_task_exec_ctx().set_min_cluster_version(session_info.get_exec_min_cluster_version());
  }

  lib::MemoryContext &get_memory_ctx() { return mem_context_; }
  share::schema::ObSchemaGetterGuard &get_scheme_guard() { return schema_guard_; }
  sql::ObSqlCtx &get_sql_ctx() { return sql_ctx_; }
  ObResultSet *get_result_set() { return result_set_; }
  ObSPIOutParams &get_out_params() { return out_params_; }
  ObIAllocator &get_allocaor() { return allocator_; }
  int destruct_exec_params(ObSQLSessionInfo &session);
private:
  enum EndStmtType
  {
    EST_NEED_NOT = 0,
    EST_RESTORE_SESSION = 1,
    EST_END_NESTED_SESSION = 2
  };
  int store_session(ObSQLSessionInfo *session,
                    sql::ObSQLSessionInfo::StmtSavedValue *&value,
                    int64_t &nested_count);
  int restore_session(ObSQLSessionInfo *session,
                      sql::ObSQLSessionInfo::StmtSavedValue *value,
                      int64_t nested_count);
  int store_orign_session(ObSQLSessionInfo *session);
  int store_cursor_session(ObSQLSessionInfo *session);
  int restore_orign_session(ObSQLSessionInfo *session);
  int restore_cursor_session(ObSQLSessionInfo *session);
  int begin_nested_session(ObSQLSessionInfo &session);
  int end_nested_session(ObSQLSessionInfo &session);
  int alloc_saved_value(sql::ObSQLSessionInfo::StmtSavedValue *&session_value);
public:
  static int is_set_global_var(ObSQLSessionInfo &session, const ObString &sql, bool &has_global_variable);
  static int check_nested_stmt_legal(ObExecContext &exec_ctx, const ObString &sql, stmt::StmtType stmt_type, bool for_update = false);
  int start_trans(ObExecContext &ctx);
  int set_cursor_env(ObSQLSessionInfo &session);
  int reset_cursor_env(ObSQLSessionInfo &session);
  int start_cursor_stmt(pl::ObPLExecCtx *pl_ctx,
                        stmt::StmtType type = stmt::StmtType::T_NONE,
                        bool is_for_update = false);
  void end_cursor_stmt(pl::ObPLExecCtx *pl_ctx, int &result);
  int start_nested_stmt_if_need(pl::ObPLExecCtx *pl_ctx, const ObString &sql, stmt::StmtType stmt_type, bool for_update);
  void end_nested_stmt_if_need(pl::ObPLExecCtx *pl_ctx, int &result);
  ObString *get_exec_params_str_ptr() { return &exec_params_str_; }
private:
  bool is_inited_;
  EndStmtType need_end_nested_stmt_;
  lib::MemoryContext mem_context_;
  // Memory of memory entity may referenced by sql_ctx_, use the guard to make
  // sure memory entity destroyed after sql_ctx_ destructed.
  PlMemEntifyDestroyGuard mem_context_destroy_guard_;
  ObArenaAllocator allocator_;
  // ObIAllocator *allocator_;
  //ObMySQLProxy::MySQLResult mysql_result_;
  char buf_[sizeof(sql::ObResultSet)] __attribute__ ((aligned (16)));
  sql::ObResultSet *result_set_;
  sql::ObSqlCtx sql_ctx_; // life period follow result_set_
  share::schema::ObSchemaGetterGuard schema_guard_;
  int64_t orign_nested_count_;
  int64_t cursor_nested_count_;
  sql::ObSQLSessionInfo::StmtSavedValue *orign_session_value_;
  sql::ObSQLSessionInfo::StmtSavedValue *cursor_session_value_;
  sql::ObSQLSessionInfo::StmtSavedValue *nested_session_value_;
  ObSPIOutParams out_params_; // 用于记录function的返回值
  ObString exec_params_str_;
};

class ObSPIService
{
public:
  struct ObSPIPrepareResult
  {
    ObSPIPrepareResult() :
    type_(stmt::T_NONE),
    for_update_(false),
    has_hidden_rowid_(false),
    exec_params_(),
    into_exprs_(),
    ref_objects_(),
    route_sql_(),
    record_type_(nullptr),
    tg_timing_event_(),
    rowid_table_id_(OB_INVALID_ID),
    ps_sql_(),
    is_bulk_(false),
    has_dup_column_name_(false),
    has_link_table_(false),
    is_skip_locked_(false)
    {}
    stmt::StmtType type_; //prepare的语句类型
    bool for_update_;
    bool has_hidden_rowid_;
    ObArray<ObRawExpr *> exec_params_; //prepare语句的执行参数
    ObArray<ObRawExpr *> into_exprs_; //prepare语句的into子句变量（如果有）
    ObArray<share::schema::ObSchemaObjVersion> ref_objects_; //prepare语句涉及的关联对象
    ObString route_sql_; //从prepare语句解析出的路由字符串
    pl::ObRecordType *record_type_;
    TgTimingEvent tg_timing_event_;
    uint64_t rowid_table_id_;
    ObString ps_sql_; // sql prepare过后的参数化sql
    bool is_bulk_;
    bool has_dup_column_name_;
    bool has_link_table_;
    bool is_skip_locked_;
  };

  struct PLPrepareCtx
  {
    PLPrepareCtx(ObSQLSessionInfo &sess_info,
                  pl::ObPLBlockNS *secondary_ns,
                  bool is_dynamic_sql,
                  bool is_dbms_sql,
                  bool is_cursor)
    : sess_info_(sess_info),
      secondary_ns_(secondary_ns),
      is_dynamic_sql_(is_dynamic_sql),
      is_dbms_sql_(is_dbms_sql),
      is_cursor_(is_cursor)
    {
    }
    ObSQLSessionInfo &sess_info_;    // pl执行用到的session
    pl::ObPLBlockNS *secondary_ns_;  // sql resolve过程中用来查找是否是pl变量的名称空间
    union {
      uint16_t flag_;
      struct {
        uint16_t is_dynamic_sql_ : 1; // 标记当前执行的sql是否是动态sql
        uint16_t is_dbms_sql_ : 1;    // 标记当前执行的sql是否是dbms_sql
        uint16_t is_cursor_ : 1;      // 标记当前执行的sql是否是cursor
        uint16_t reserved_ : 13;
      };
    };
    TO_STRING_KV(KP_(secondary_ns), K_(is_dynamic_sql), K_(is_dbms_sql), K_(is_cursor));
  };

  class PLPrepareResult
  {
    public:
    PLPrepareResult() :
        mem_context_(nullptr),
        mem_context_destroy_guard_(mem_context_),
        result_set_(nullptr),
        sql_ctx_(),
        schema_guard_(share::schema::ObSchemaMgrItem::MOD_PL_PREPARE_RESULT) {}
    ~PLPrepareResult() { reset(); }
    int init(sql::ObSQLSessionInfo &session_info);
    void reset()
    {
      //result_set_.reset();
      if (result_set_ != NULL) {
        result_set_->~ObResultSet();
      }
      sql_ctx_.reset();
      schema_guard_.reset();
    }
    common::ObIAllocator *get_allocator() { return &mem_context_->get_arena_allocator(); }
  public:
    lib::MemoryContext mem_context_;
    // Memory of memory entity may referenced by sql_ctx_, use the guard to make
    // sure memory entity destroyed after sql_ctx_ destructed.
    PlMemEntifyDestroyGuard mem_context_destroy_guard_;
    char buf_[sizeof(sql::ObResultSet)] __attribute__ ((aligned (16)));
    sql::ObResultSet *result_set_;
    sql::ObSqlCtx sql_ctx_; // life period follow result_set_
    share::schema::ObSchemaGetterGuard schema_guard_;
  };

  enum ObCusorDeclareLoc {
    DECL_LOCAL, // a local cursor
    DECL_SUBPROG, // a cursor may access by subprogram
    DECL_PKG, // a cursor in pkg, ref cursor is impossible
    DECL_CLIENT, // a ref cursor used by meddleware
  };

public:
  ObSPIService() {}
  virtual ~ObSPIService() {}

  static int cast_enum_set_to_string(ObExecContext &ctx,
                                     const ObIArray<ObString> &enum_set_values,
                                     ObObjParam &src,
                                     ObObj &result);
  static int spi_calc_raw_expr(ObSQLSessionInfo *session_info,
                               ObIAllocator *allocator,
                               const ObRawExpr *rawexpr,
                               ObObj *result);
  static int spi_calc_expr(pl::ObPLExecCtx *ctx,
                           const ObSqlExpression *expr,
                           const int64_t result_idx,
                           ObObjParam *result);
  static int spi_calc_expr_at_idx(pl::ObPLExecCtx *ctx,
                                  const int64_t expr_idx,
                                  const int64_t result_idx,
                                  ObObjParam *result);

  static int spi_calc_subprogram_expr(pl::ObPLExecCtx *ctx,
                                      uint64_t package_id,
                                      uint64_t routine_id,
                                      int64_t expr_idx,
                                      ObObjParam *result);

  static int spi_calc_package_expr_v1(const pl::ObPLResolveCtx &resolve_ctx,
                                      sql::ObExecContext &exec_ctx,
                                      ObIAllocator &allocator,
                                      uint64_t package_id,
                                      int64_t expr_idx,
                                      ObObjParam *result);
  static int spi_calc_package_expr(pl::ObPLExecCtx *ctx,
                           uint64_t package_id,
                           int64_t expr_idx,
                           ObObjParam *result);
  static int spi_convert(ObSQLSessionInfo &session, ObIAllocator &allocator,
                         ObObj &src, const ObExprResType &result_type, ObObj &dst, bool ignore_fail = false);
  static int spi_convert(ObSQLSessionInfo *session, ObIAllocator *allocator,
                         ObObjParam &src, const ObExprResType &result_type, ObObjParam &result);
  static int spi_convert_objparam(pl::ObPLExecCtx *ctx, ObObjParam *src, const int64_t result_idx, ObObjParam *result, bool need_set);
  static int spi_set_package_variable(pl::ObPLExecCtx *ctx,
                             uint64_t package_id,
                             int64_t var_idx,
                             const ObObj &value,
                             bool need_deep_copy = false);
  static int spi_set_package_variable(ObExecContext *exec_ctx,
                             pl::ObPLPackageGuard *guard,
                             uint64_t package_id,
                             int64_t var_idx,
                             const ObObj &value,
                             ObIAllocator *allocator = NULL,
                             bool need_deep_copy = false);
  static int check_and_deep_copy_result(ObIAllocator &alloc,
                                        const ObObj &src,
                                        ObObj &dst);
  static int spi_set_variable_to_expr(pl::ObPLExecCtx *ctx,
                                      const int64_t expr_idx,
                                      const ObObjParam *value,
                                      bool is_default = false,
                                      bool need_copy = false);
  static int spi_set_variable(pl::ObPLExecCtx *ctx,
                              const ObSqlExpression* expr,
                              const ObObjParam *value,
                              bool is_default = false,
                              bool need_copy = false);
  static int spi_query_into_expr_idx(pl::ObPLExecCtx *ctx,
                                     const char* sql,
                                     int64_t type,
                                     const int64_t *into_exprs_idx = NULL,
                                     int64_t into_count = 0,
                                     const ObDataType *column_types = NULL,
                                     int64_t type_count = 0,
                                     const bool *exprs_not_null_flag = NULL,
                                     const int64_t *pl_integer_ranges = NULL,
                                     bool is_bulk = false,
                                     bool is_type_record = false,
                                     bool for_update = false);
  static int spi_query(pl::ObPLExecCtx *ctx,
                       const char* sql,
                       int64_t type,
                       const ObSqlExpression **into_exprs = NULL,
                       int64_t into_count = 0,
                       const ObDataType *column_types = NULL,
                       int64_t type_count = 0,
                       const bool *exprs_not_null_flag = NULL,
                       const int64_t *pl_integer_ranges = NULL,
                       bool is_bulk = false,
                       bool is_type_record = false,
                       bool for_update = false);
  static int spi_check_autonomous_trans(pl::ObPLExecCtx *ctx);
  static int spi_prepare(common::ObIAllocator &allocator,
                         ObSQLSessionInfo &session,
                         ObMySQLProxy &sql_proxy,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         sql::ObRawExprFactory &expr_factory,
                         const ObString &sql,
                         bool is_cursor,
                         pl::ObPLBlockNS *secondary_namespace,
                         ObSPIPrepareResult &prepare_result);
  static int spi_execute_with_expr_idx(pl::ObPLExecCtx *ctx,
                                       const char *ps_sql,
                                       int64_t type,
                                       const int64_t *param_exprs_idx,
                                       int64_t param_count,
                                       const int64_t *into_exprs_idx,
                                       int64_t into_count,
                                       const ObDataType *column_types,
                                       int64_t type_count,
                                       const bool *exprs_not_null_flag,
                                       const int64_t *pl_integer_ranges,
                                       bool is_bulk,
                                       bool is_forall,
                                       bool is_type_record,
                                       bool for_update);
  static int spi_execute(pl::ObPLExecCtx *ctx,
                         const char* ps_sql,
                         int64_t type,
                         const ObSqlExpression **param_exprs,
                         int64_t param_count,
                         const ObSqlExpression **into_exprs,
                         int64_t into_count,
                         const ObDataType *column_types,
                         int64_t type_count,
                         const bool *exprs_not_null_flag,
                         const int64_t *pl_integer_ranges,
                         bool is_bulk = false,
                         bool is_forall = false,
                         bool is_type_record = false,
                         bool for_update = false);

  static int spi_execute_immediate(pl::ObPLExecCtx *ctx,
                                   const int64_t sql_dix,
                                   common::ObObjParam **params,
                                   const int64_t *params_mode,
                                   int64_t param_count,
                                   const int64_t *into_exprs_idx,
                                   int64_t into_count,
                                   const ObDataType *column_types,
                                   int64_t type_count,
                                   const bool *exprs_not_null_flag,
                                   const int64_t *pl_integer_ranges,
                                   bool is_bulk = false,
                                   bool is_returning = false,
                                   bool is_type_record = false);

  static int spi_get_subprogram_cursor_info(pl::ObPLExecCtx *ctx,
                                 uint64_t package_id,
                                 uint64_t routine_id,
                                 int64_t index,
                                 pl::ObPLCursorInfo *&cursor,
                                 common::ObObjParam &param);
  static int spi_get_package_cursor_info(pl::ObPLExecCtx *ctx,
                                 uint64_t package_id,
                                 uint64_t routine_id,
                                 int64_t index,
                                 pl::ObPLCursorInfo *&cursor,
                                 common::ObObjParam &param);
  static int spi_get_cursor_info(pl::ObPLExecCtx *ctx,
                                 uint64_t package_id,
                                 uint64_t routine_id,
                                 int64_t index,
                                 pl::ObPLCursorInfo *&cursor,
                                 common::ObObjParam &param,
                                 ObCusorDeclareLoc &location);
  static int spi_get_cursor_info(pl::ObPLExecCtx *ctx,
                                 int64_t index,
                                 pl::ObPLCursorInfo *&cursor,
                                 common::ObObjParam &param);
  static int spi_cursor_alloc(common::ObIAllocator &allocator,
                              ObObj &obj);
  static int spi_cursor_init(pl::ObPLExecCtx *ctx,
                             int64_t cursor_index);
  static int cursor_open_check(pl::ObPLExecCtx *ctx,
                                   int64_t package_id,
                                   int64_t routine_id,
                                   int64_t cursor_index,
                                   pl::ObPLCursorInfo *&cursor,
                                   common::ObObjParam &obj,
                                   ObCusorDeclareLoc loc);
  static int spi_cursor_open_with_param_idx(pl::ObPLExecCtx *ctx,
                                  const char *sql,
                                  const char *ps_sql,
                                  int64_t type,
                                  bool for_update,
                                  bool has_hidden_rowid,
                                  const int64_t *sql_param_exprs,
                                  int64_t sql_param_count,
                                  uint64_t package_id,
                                  uint64_t routine_id,
                                  int64_t cursor_index,
                                  const int64_t *formal_param_idxs,
                                  const int64_t *actual_param_exprs,
                                  int64_t cursor_param_count,
                                  bool skip_locked);
  static int spi_cursor_open(pl::ObPLExecCtx *ctx,
                             const char *sql,
                             const char *ps_sql,
                             int64_t type,
                             bool for_update,
                             bool has_hidden_rowid,
                             const ObSqlExpression **sql_param_exprs,
                             int64_t sql_param_count,
                             uint64_t package_id,
                             uint64_t routine_id,
                             int64_t cursor_index,
                             const int64_t *formal_param_idxs,
                             const ObSqlExpression **actual_param_exprs,
                             int64_t cursor_param_count,
                             bool skip_locked);
  static int dbms_cursor_open(pl::ObPLExecCtx *ctx,
                              pl::ObDbmsCursorInfo &cursor,
                              const ObString &ps_sql,
                              int64_t stmt_type,
                              bool for_update,
                              bool has_hidden_rowid);
  static int spi_dynamic_open(pl::ObPLExecCtx *ctx,
                              const int64_t sql_idx,
                              const int64_t *sql_param_exprs_idx,
                              int64_t sql_param_count,
                              uint64_t package_id,
                              uint64_t routine_id,
                              int64_t cursor_index);
  static int dbms_dynamic_open(pl::ObPLExecCtx *ctx,
                               pl::ObDbmsCursorInfo &cursor,
                               bool is_dbms_sql = false);
  static int dbms_cursor_fetch(pl::ObPLExecCtx *ctx,
                              pl::ObDbmsCursorInfo &cursor,
                              bool is_server_cursor = false);
  static int spi_cursor_fetch(pl::ObPLExecCtx *ctx,
                              uint64_t package_id,
                              uint64_t routine_id,
                              int64_t cursor_index,
                              const int64_t *into_exprs,
                              int64_t into_count,
                              const ObDataType *column_types,
                              int64_t type_count,
                              const bool *exprs_not_null_flag,
                              const int64_t *pl_integer_ranges,
                              bool is_bulk,
                              int64_t limit,
                              const ObDataType *return_types,
                              int64_t return_type_count,
                              bool is_type_record = false);
  static int spi_cursor_close(pl::ObPLExecCtx *ctx,
                              uint64_t package_id,
                              uint64_t routine_id,
                              int64_t cursor_index,
                              bool ignore = false);
  static int dbms_cursor_close(ObExecContext &exec_ctx,
                               pl::ObPLCursorInfo &cursor);

  static int spi_alloc_complex_var(pl::ObPLExecCtx *ctx,
                                   int64_t type,
                                   int64_t id,
                                   int64_t var_idx,
                                   int64_t init_size,
                                   int64_t *addr);

  static int spi_extend_collection(pl::ObPLExecCtx *ctx,
                                   const int64_t collection_expr_idx,
                                   int64_t column_count,
                                   const int64_t n_expr_idx,
                                   const int64_t i_expr_idx = OB_INVALID_ID,
                                   uint64_t package_id = OB_INVALID_ID);

  static int spi_set_collection(int64_t tenant_id,
                                  const pl::ObPLINS *ns,
                                  ObIAllocator &allocator,
                                  pl::ObPLCollection &coll,
                                  int64_t n,
                                  bool extend_mode = false);

  static int spi_reset_collection(pl::ObPLCollection *coll);

  static int spi_raise_application_error(pl::ObPLExecCtx *ctx,
                                         const int64_t errcode_expr_idx,
                                         const int64_t errmsg_expr_idx);

  static int spi_process_resignal(pl::ObPLExecCtx *ctx,
                                  const int64_t errcode_expr,
                                  const int64_t errmsg_expr,
                                  const char *sql_state,
                                  int *error_code,
                                  const char *resignal_sql_state,
                                  bool is_signal);

  static int spi_delete_collection(pl::ObPLExecCtx *ctx,
                                   const int64_t collection_expr_idx,
                                   int64_t row_size,
                                   const int64_t m_expr_idx,
                                   const int64_t n_expr_idx);

  static int spi_trim_collection(pl::ObPLExecCtx *ctx,
                                   const int64_t collection_expr_idx,
                                   int64_t row_size,
                                   const int64_t n_expr_idx);

  static int acquire_spi_conn(ObMySQLProxy &sql_proxy,
                              ObSQLSessionInfo &session_info,
                              observer::ObInnerSQLConnection *&spi_conn);

  static int spi_destruct_collection(pl::ObPLExecCtx *ctx, int64_t idx);

  static int spi_init_collection(pl::ObPLExecCtx *ctx, pl::ObPLCollection *src, pl::ObPLCollection *dest, int64_t row_size, uint64_t package_id = OB_INVALID_ID);

  static int spi_sub_nestedtable(pl::ObPLExecCtx *ctx, int64_t src_idx, int64_t dst_idx, int32_t lower, int32_t upper);

#ifdef OB_BUILD_ORACLE_PL
  static int spi_extend_assoc_array(int64_t tenant_id,
                                    const pl::ObPLINS *ns,
                                    ObIAllocator &allocator,
                                    pl::ObPLAssocArray &assoc_array,
                                    int64_t n);
#endif
  static int spi_get_package_allocator(pl::ObPLExecCtx *ctx, uint64_t package_id, ObIAllocator *&allocator);

  static int spi_copy_datum(pl::ObPLExecCtx *ctx,
                            ObIAllocator *allocator,
                            ObObj *src,
                            ObObj *dest,
                            ObDataType *dest_type,
                            uint64_t package_id = OB_INVALID_ID);

  static int spi_destruct_obj(pl::ObPLExecCtx *ctx,
                              ObObj *obj);

  static int spi_build_record_type(common::ObIAllocator &allocator,
                                   ObSQLSessionInfo &session,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   const sql::ObResultSet &result_set,
                                   int64_t hidden_column_count,
                                   pl::ObRecordType *&record_type,
                                   uint64_t &rowid_table_id,
                                   pl::ObPLBlockNS *secondary_namespace,
                                   bool &has_dup_column_name);

  static int spi_construct_collection(
    pl::ObPLExecCtx *ctx, uint64_t package_id, ObObjParam *result);

  static int spi_clear_diagnostic_area(pl::ObPLExecCtx *ctx);

  static int spi_end_trans(pl::ObPLExecCtx *ctx, const char *sql, bool is_rollback);

  static int spi_pad_char_or_varchar(ObSQLSessionInfo *session_info, const ObSqlExpression *expr,
                                     ObIAllocator *allocator, ObObj *result);
  static int spi_pad_char_or_varchar(ObSQLSessionInfo *session_info, const ObRawExpr *expr,
                                     ObIAllocator *allocator, ObObj *result);

  static int spi_pad_char_or_varchar(ObSQLSessionInfo *session_info,
                                     const ObObjType &type,
                                     const ObAccuracy &accuracy,
                                     ObIAllocator *allocator, ObObj *result);

  static int spi_pad_binary(ObSQLSessionInfo *session_info,
                                          const ObAccuracy &accuracy,
                                          ObIAllocator *allocator,
                                          ObObj *result);

  static int spi_set_pl_exception_code(pl::ObPLExecCtx *ctx, int64_t code, bool is_pop_warning_buf);

  static int spi_get_pl_exception_code(pl::ObPLExecCtx *ctx, int64_t *code);

  static int spi_check_early_exit(pl::ObPLExecCtx *ctx);

  static int spi_check_exception_handler_legal(pl::ObPLExecCtx *ctx, int64_t code);

  static int spi_interface_impl(pl::ObPLExecCtx* ctx, const char *interface_name);

  static int process_function_out_result(pl::ObPLExecCtx *ctx,
                                         ObResultSet &result_set,
                                         ObIArray<ObObj> &out_params);

  static int spi_pipe_row_to_result(pl::ObPLExecCtx *ctx, ObObjParam *single_row);

  static int spi_process_nocopy_params(pl::ObPLExecCtx *ctx, int64_t local_idx);

  static int spi_set_subprogram_cursor_var(pl::ObPLExecCtx *ctx,
                                    uint64_t package_id,
                                    uint64_t routine_id,
                                    int64_t index,
                                    common::ObObjParam &param);
  static int spi_copy_ref_cursor(pl::ObPLExecCtx *ctx,
                                 ObIAllocator *allocator,
                                 ObObj *src,
                                 ObObj *dest,
                                 ObDataType *dest_type,
                                 uint64_t package_id = OB_INVALID_ID);

  static int spi_add_ref_cursor_refcount(pl::ObPLExecCtx *ctx, ObObj *cursor, int64_t addend);
  static int spi_handle_ref_cursor_refcount(pl::ObPLExecCtx *ctx,
                                      uint64_t package_id,
                                      uint64_t routine_id,
                                      int64_t index,
                                      int64_t addend);

  static int prepare_dynamic(pl::ObPLExecCtx *ctx,
                             const ObSqlExpression *sql_expr,
                             ObIAllocator &allocator,
                             bool is_returning,
                             int64_t param_cnt,
                             ObSqlString &sql_str,
                             common::ObString &ps_sql,
                             stmt::StmtType &type,
                             bool &for_update,
                             bool &hidden_rowid,
                             int64_t &into_cnt,
                             bool &skip_locked);
  static int prepare_dynamic(pl::ObPLExecCtx *ctx,
                             ObIAllocator &allocator,
                             bool is_returning,
                             bool is_dbms_sql,
                             int64_t param_cnt,
                             ObSqlString &sql_str,
                             common::ObString &ps_sql,
                             stmt::StmtType &type,
                             bool &for_update,
                             bool &hidden_rowid,
                             int64_t &into_cnt,
                             bool &skip_locked,
                             common::ColumnsFieldArray *field_list = NULL);
  static int force_refresh_schema(uint64_t tenant_id, int64_t refresh_version = OB_INVALID_VERSION);

  static int spi_update_package_change_info(
    pl::ObPLExecCtx *ctx, uint64_t package_id, uint64_t var_idx);

#ifdef OB_BUILD_ORACLE_PL
  static int spi_copy_opaque(
      pl::ObPLExecCtx *ctx, ObIAllocator *allocator,
      pl::ObPLOpaque &src, pl::ObPLOpaque *&dest, uint64_t package_id = OB_INVALID_ID);
#endif
  static int spi_check_composite_not_null(ObObjParam *v);

  static int spi_update_location(pl::ObPLExecCtx *ctx, uint64_t location);

  static int inner_open(pl::ObPLExecCtx *pl_ctx,
                        const common::ObString &sql,
                        const common::ObString &ps_sql,
                        int64_t stmt_type,
                        ParamStore &exec_params,
                        ObSPIResultSet &spi_result,
                        ObSPIOutParams &out_params);

  static void adjust_pl_status_for_xa(sql::ObExecContext &ctx, int &result);
  static int fill_cursor(ObResultSet &result_set, ObSPICursor *cursor, int64_t new_query_start_time);

  static int spi_opaque_assign_null(int64_t opaque_ptr);

  static int spi_pl_profiler_before_record(pl::ObPLExecCtx *ctx, int64_t line, int64_t level);

  static int spi_pl_profiler_after_record(pl::ObPLExecCtx *ctx, int64_t line, int64_t level);

#ifdef OB_BUILD_ORACLE_PL
  static int spi_execute_dblink(ObExecContext &exec_ctx,
                                ObIAllocator &allocator,
                                uint64_t dblink_id,
                                uint64_t package_id,
                                uint64_t proc_id,
                                ParamStore &params,
                                ObObj *result);
  static int spi_execute_dblink(ObExecContext &exec_ctx,
                                ObIAllocator &allocator,
                                const pl::ObPLDbLinkInfo *dblink_info,
                                const ObRoutineInfo *routine_info,
                                ParamStore &params,
                                ObObj *result);
  static int spi_after_execute_dblink(ObSQLSessionInfo *session,
                                      const ObRoutineInfo *routine_info,
                                      ObIAllocator &allocator,
                                      ParamStore &params,
                                      ParamStore &exec_params,
                                      ObObj *result,
                                      ObObj &tmp_result);
#endif
private:
  static int recreate_implicit_savapoint_if_need(pl::ObPLExecCtx *ctx, int &result);
  static int recreate_implicit_savapoint_if_need(sql::ObExecContext &ctx, int &result);

  static int calc_obj_access_expr(pl::ObPLExecCtx *ctx, const ObSqlExpression &expr, ObObjParam &result);

  static bool can_obj_access_expr_fast_calc(const ObSqlExpression &expr,
                                            const ObExprObjAccess *&obj_access);

  static int set_variable(pl::ObPLExecCtx *ctx,
                          const share::ObSetVar::SetScopeType scope,
                          const ObString &name,
                          const ObObjParam &value,
                          bool is_default = false);

  /**
   * @brief  只走parser不走resolver的prepare接口，供mysql模式使用
   * @param [in] allocator      - 内存分配器
   * @param [in] session  - session信息
   * @param [in] sql_proxy      - ObMySQLProxy
   * @param [in] schema_guard      - ObSchemaGetterGuard
   * @param [in] expr_factory      - 表达式工厂
   * @param [in] sql      - 待prepare的sql
   * @param [in] secondary_namespace      - 传递给sql引擎的PL名字空间
   * @param [out] prepare_result      - prepare的结果
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   * 是对spi_resolve_prepare的改写，和spi_resolve_prepare相比不同之处在于：
   * spi_resolve_prepare是通过传入PL名称空间给SQL引擎的resolver，由sql resolver进行变量解析，
   * 而本函数不调用sql resolver，自己对parser的结果进行名称解析.
   * 其主要流程是调用prepare_parse接口，获取parse的结果。其结果中包含了pl变量节点链表、依赖
   * 外部对象链表，通过resolve_local_var解析出这些变量。如果有into子句会解析出into表达式。
   */
  static int spi_parse_prepare(common::ObIAllocator &allocator,
                               ObSQLSessionInfo &session,
                               ObMySQLProxy &sql_proxy,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               sql::ObRawExprFactory &expr_factory,
                               const ObString &sql,
                               pl::ObPLBlockNS *secondary_namespace,
                               ObSPIPrepareResult &prepare_result);

  static int spi_resolve_prepare(common::ObIAllocator &allocator,
                                 ObSQLSessionInfo &session,
                                 ObMySQLProxy &sql_proxy,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 sql::ObRawExprFactory &expr_factory,
                                 const ObString &sql,
                                 bool is_cursor,
                                 pl::ObPLBlockNS *secondary_namespace,
                                 ObSPIPrepareResult &prepare_result);

  static int spi_inner_execute(pl::ObPLExecCtx *ctx,
                               const char *sql,
                               const char *ps_sql,
                               int64_t type,
                               const ObSqlExpression **param_exprs,
                               int64_t param_count,
                               const ObSqlExpression **into_exprs,
                               int64_t into_count,
                               const ObDataType *column_types,
                               int64_t type_count,
                               const bool *exprs_not_null_flag,
                               const int64_t *pl_integer_ranges,
                               int64_t is_bulk,
                               bool is_forall = false,
                               bool is_type_record = false,
                               bool for_update = false);

  static int dbms_cursor_execute(pl::ObPLExecCtx *ctx,
                                 const ObString ps_sql,
                                 stmt::StmtType stmt_type,
                                 pl::ObDbmsCursorInfo &cursor,
                                 bool is_dbms_sql);

  static int adjust_out_params(ObResultSet &result_set,
                               ObSPIOutParams &out_params);

  static int adjust_out_params(pl::ObPLExecCtx *ctx,
                               const ObSqlExpression **into_exprs,
                               int64_t into_count,
                               ObSPIOutParams &out_params);

  static int check_exist_in_into_exprs(pl::ObPLExecCtx *ctx,
                                       const ObSqlExpression **into_exprs,
                                       int64_t into_count,
                                       const ObObj &result,
                                       bool &exist);

  static int construct_exec_params(pl::ObPLExecCtx *ctx,
                                   ObIAllocator &param_allocator, //用于拷贝执行期参数
                                   const ObSqlExpression **param_exprs,
                                   int64_t param_count,
                                   const ObSqlExpression **into_exprs,
                                   int64_t into_count,
                                   ParamStore &exec_params,
                                   ObSPIOutParams &out_params,
                                   bool is_forall = false);

  static int inner_open(pl::ObPLExecCtx *ctx,
                        ObIAllocator &param_allocator, //用于拷贝执行期参数
                        const char* sql,
                        const char* ps_sql,
                        int64_t type,
                        const ObSqlExpression **param_exprs,
                        int64_t param_count,
                        const ObSqlExpression **into_exprs,
                        int64_t into_count,
                        ObSPIResultSet &spi_result,
                        ObSPIOutParams &out_params,
                        observer::ObQueryRetryCtrl *retry_ctrl = nullptr,
                        bool is_forall = false);

  static int inner_fetch(pl::ObPLExecCtx *ctx,
                         observer::ObQueryRetryCtrl &retry_ctrl,
                         ObSPIResultSet &spi_result,
                         const ObSqlExpression **into_exprs,
                         int64_t into_count,
                         const ObDataType *column_types,
                         int64_t type_count,
                         const bool *exprs_not_null_flag,
                         const int64_t *pl_integer_ranges,
                         ObIArray<ObObjParam*> *out_using_params,
                         int64_t &row_count,
                         bool is_bulk = false,
                         bool is_forall = false,
                         bool is_dynamic_sql = false,
                         ObNewRow *current_row = NULL,
                         bool has_hidden_rowid = false,
                         bool for_cursor = false,
                         int64_t limit = INT64_MAX,
                         const ObDataType *return_types = nullptr,
                         int64_t return_type_count = 0,
                         bool is_type_record = false);
    static int inner_fetch_with_retry(
                         pl::ObPLExecCtx *ctx,
                         ObSPIResultSet &spi_result,
                         const ObSqlExpression **into_exprs,
                         int64_t into_count,
                         const ObDataType *column_types,
                         int64_t type_count,
                         const bool *exprs_not_null_flag,
                         const int64_t *pl_integer_ranges,
                         int64_t &row_count,
                         ObNewRow &current_row,
                         bool has_hidden_rowid,
                         bool is_bulk,
                         bool for_cursor,
                         int64_t limit,
                         int64_t last_exec_time,
                         const ObDataType *return_types = nullptr,
                         int64_t return_type_count = 0,
                         bool is_type_record = false);

  static int convert_obj(pl::ObPLExecCtx *ctx,
                          ObCastCtx &cast_ctx,
                          bool is_strict,
                          const ObSqlExpression *result_expr,
                          const ObIArray<ObDataType> &current_type,
                          ObIArray<ObObj> &obj_array,
                          const ObDataType *trans_type,
                          int64_t trans_type_count,
                          ObIArray<ObObj> &calc_array);

  static int get_result(pl::ObPLExecCtx *ctx,
                         void *result_set,
                         bool is_streaming,
                         const ObSqlExpression **into_exprs,
                         int64_t into_count,
                         const ObDataType *column_types,
                         int64_t type_count,
                         const bool *exprs_not_null_flag,
                         const int64_t *pl_integer_ranges,
                         ObIArray<ObObjParam*> *out_using_params,
                         int64_t &row_count,
                         ObNewRow &current_row,
                         bool &can_retry,
                         bool has_hidden_rowid = false,
                         bool is_bulk = false,
                         bool is_dynamic_sql = false,
                         bool for_cursor = false,
                         bool is_forall = false,
                         int64_t limit = INT64_MAX,
                         const ObDataType *return_types = nullptr,
                         int64_t return_type_count = 0,
                         bool is_type_record = false);

  static int fetch_row(void *result_set,
                       bool is_streaming,
                       int64_t &row_count,
                       ObNewRow &row);

  static int collect_cells(pl::ObPLExecCtx &ctx,
                           ObNewRow &row,
                           const ObDataType *result_types,
                           int64_t type_count,
                           const ObIArray<ObDataType> &row_desc,
                           bool is_strict,
                           ObIArray<ObCastCtx> &cast_ctx,
                           int64_t hidden_column_count,
                           ObIArray<ObObj> &result);
  static int store_result(pl::ObPLExecCtx *ctx,
                          const ObSqlExpression *result_expr,
                          const ObDataType *result_types,
                          int64_t type_count,
                          const bool *not_null_flags,
                          const int64_t *pl_integer_ranges,
                          const ObIArray<ObDataType> &row_desc,
                          bool is_strict,
                          ObCastCtx &cast_ctx,
                          ObIArray<ObObj> &obj_array,
                          const ObDataType *return_types,
                          int64_t return_type_count,
                          bool is_type_record = false);
  static int check_and_copy_composite(ObObj &result,
                                      ObObj &src,
                                      ObIAllocator &allocator,
                                      pl::ObPLType type,
                                      uint64_t dst_udt_id);

  static int get_package_var_info_by_expr(const ObSqlExpression *expr,
                                          uint64_t &package_id,
                                          uint64_t &var_idx);
  static int store_result(pl::ObPLExecCtx *ctx,
                          ObIArray<pl::ObPLCollection*> &bulk_tables,
                          int64_t row_count,
                          int64_t column_count,
                          ObIArray<ObObj> &obj_array,
                          bool append_mode,
                          bool is_type_record);

  static int store_into_result(pl::ObPLExecCtx *ctx,
                                ObCastCtx &cast_ctx,
                                ObNewRow &cur_row,
                                const ObSqlExpression **into_exprs,
                                const ObDataType *column_types,
                                int64_t type_count,
                                int64_t into_count,
                                const bool *exprs_not_null,
                                const int64_t *pl_integer_ranges,
                                const ObDataType *return_types,
                                int64_t return_type_count,
                                int64_t actual_column_count,
                                ObIArray<ObDataType> &row_desc,
                                bool is_type_record);

  static int store_datums(ObObj &dest_addr, ObIArray<ObObj> &result,
                          ObIAllocator *alloc,  ObSQLSessionInfo *session_info,
                          bool is_schema_object);

  static int store_datum(int64_t &current_addr, const ObObj &obj, ObSQLSessionInfo *session_info);

  static const ObPostExprItem &get_last_expr_item(const ObSqlExpression &expr);

  static const ObInfixExprItem &get_first_expr_item(const ObSqlExpression &expr);

  static int get_result_type(pl::ObPLExecCtx &ctx, const ObSqlExpression &expr, ObExprResType &type);

  static ObItemType get_expression_type(const ObSqlExpression &expr);

  static const ObObj &get_const_value(const ObSqlExpression &expr);

  static bool is_question_mark_expression(const ObSqlExpression &expr);

  static bool is_const_expression(const ObSqlExpression &expr);

  static bool is_obj_access_expression(const ObSqlExpression &expr);

  static bool is_get_var_func_expression(const ObSqlExpression &expr);

  static bool is_get_package_or_subprogram_var_expression(const ObSqlExpression &expr);

  static int resolve_exec_params(const ParseResult &parse_result,
                                 ObSQLSessionInfo &session,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 sql::ObRawExprFactory &expr_factory,
                                 pl::ObPLBlockNS &secondary_namespace,
                                 ObSPIPrepareResult &prepare_result,
                                 common::ObIAllocator &allocator);

  static int resolve_into_params(const ParseResult &parse_result,
                                 ObSQLSessionInfo &session,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 sql::ObRawExprFactory &expr_factory,
                                 pl::ObPLBlockNS &secondary_namespace,
                                 ObSPIPrepareResult &prepare_result);

  static int resolve_ref_objects(const ParseResult &parse_result,
                                 ObSQLSessionInfo &session,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 ObSPIPrepareResult &prepare_result);

  static int calc_dynamic_sqlstr(
    pl::ObPLExecCtx *ctx, const ObSqlExpression *sql, ObSqlString &sqlstr);

  static int dynamic_out_params(
    common::ObIAllocator &allocator,
    ObResultSet *result, common::ObObjParam **params, int64_t param_count);

  static int cursor_close_impl(pl::ObPLExecCtx *ctx,
                                   pl::ObPLCursorInfo *cursor,
                                   bool is_refcursor,
                                   uint64_t package_id = OB_INVALID_ID,
                                   uint64_t routine_id = OB_INVALID_ID,
                                   bool ignore = false);

  static int do_cursor_fetch(pl::ObPLExecCtx *ctx,
                                     pl::ObPLCursorInfo *cursor,
                                     bool is_server_cursor,
                                     const ObSqlExpression **into_exprs,
                                     int64_t into_count,
                                     const ObDataType *column_types,
                                     int64_t type_count,
                                     const bool *exprs_not_null_flag,
                                     const int64_t *pl_integer_ranges,
                                     bool is_bulk,
                                     int64_t limit,
                                     const ObDataType *return_types = nullptr,
                                     int64_t return_type_count = 0,
                                     bool is_type_record = false);


  static int check_package_dest_and_deep_copy(pl::ObPLExecCtx &ctx,
                                    const ObSqlExpression &expr,
                                    ObIArray<ObObj> &src_array,
                                    ObIArray<ObObj> &dst_array);

  static int prepare_cursor_parameters(pl::ObPLExecCtx *ctx,
                                    ObSQLSessionInfo &session_info,
                                    uint64_t package_id,
                                    uint64_t routine_id,
                                    ObCusorDeclareLoc loc,
                                    const int64_t *formal_param_idxs,
                                    const ObSqlExpression **actual_param_exprs,
                                    int64_t cursor_param_count);
  static bool is_sql_type_into_pl(ObObj &dest_addr, ObIArray<ObObj> &obj_array);
};

struct ObPLSubPLSqlTimeGuard
{
  ObPLSubPLSqlTimeGuard(pl::ObPLExecCtx *ctx);
  ~ObPLSubPLSqlTimeGuard();
  int64_t old_sub_plsql_exec_time_;
  int64_t execute_start_;
  pl::ObPLExecState *state_;
  int64_t old_pure_sql_exec_time_;
};

}
}



#endif /* OCEANBASE_SRC_SQL_OB_SPI_H_ */
