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

namespace oceanbase
{
namespace observer
{
class ObInnerSQLConnection;
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

struct ObSPICursor
{
  ObSPICursor(ObIAllocator &allocator) :
    row_store_(), row_desc_(), allocator_(&allocator), cur_(0), fields_(allocator) {}

  ObRARowStore row_store_;
  ObArray<ObDataType> row_desc_; //ObRowStore里数据自带的Meta可能是T_NULL，所以这里自备一份
  ObIAllocator* allocator_;
  int64_t cur_;
  common::ColumnsFieldArray fields_;
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

class ObSPIResultSet
{
public:
  ObSPIResultSet()
    : need_end_nested_stmt_(EST_NEED_NOT),
      allocator_(ObModIds::OB_PL_TEMP),
      mysql_result_(),
      orign_nested_count_(-1),
      cursor_nested_count_(-1),
      orign_session_value_(NULL),
      cursor_session_value_(NULL),
      nested_session_value_(NULL),
      out_params_() {}
  ~ObSPIResultSet() { reset(); }
  void reset()
  {
    mysql_result_.reset();
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
    allocator_.reset();
  }

  ObMySQLProxy::MySQLResult &get_mysql_result() { return mysql_result_; }
  ObResultSet *get_result_set();
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
  static int check_nested_stmt_legal(ObExecContext &exec_ctx, stmt::StmtType stmt_type);
  int start_trans(ObExecContext &ctx);
  int set_cursor_env(ObSQLSessionInfo &session);
  int reset_cursor_env(ObSQLSessionInfo &session);
  int start_cursor_stmt(pl::ObPLExecCtx *pl_ctx,
                        stmt::StmtType type = stmt::StmtType::T_NONE,
                        bool is_for_update = false);
  void end_cursor_stmt(pl::ObPLExecCtx *pl_ctx, int &result);
  int start_nested_stmt_if_need(pl::ObPLExecCtx *pl_ctx, stmt::StmtType stmt_type);
  void end_nested_stmt_if_need(pl::ObPLExecCtx *pl_ctx, int &result);
private:
  EndStmtType need_end_nested_stmt_;
  ObArenaAllocator allocator_;
  // ObIAllocator *allocator_;
  ObMySQLProxy::MySQLResult mysql_result_;
  int64_t orign_nested_count_;
  int64_t cursor_nested_count_;
  sql::ObSQLSessionInfo::StmtSavedValue *orign_session_value_;
  sql::ObSQLSessionInfo::StmtSavedValue *cursor_session_value_;
  sql::ObSQLSessionInfo::StmtSavedValue *nested_session_value_;
  ObSPIOutParams out_params_; // 用于记录function的返回值
};

class ObSPIService
{
public:
  struct ObSPIPrepareResult
  {
    ObPsStmtId id_; //prepare的语句id
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
                             const ObObj &value);
  static int spi_set_package_variable(ObExecContext *exec_ctx,
                             pl::ObPLPackageGuard *guard,
                             uint64_t package_id,
                             int64_t var_idx,
                             const ObObj &value);
  static int spi_set_variable(pl::ObPLExecCtx *ctx,
                              const ObSqlExpression* expr,
                              const ObObjParam *value,
                              bool is_default = false);
  static int spi_query(pl::ObPLExecCtx *ctx,
                       const char* sql,
                       int64_t type,
                       const ObSqlExpression **into_exprs = NULL,
                       int64_t into_count = 0,
                       const ObDataType *column_types = NULL,
                       int64_t type_count = 0,
                       const bool *exprs_not_null_flag = NULL,
                       const int64_t *pl_integer_ranges = NULL,
                       bool is_bulk = false);
  static int spi_prepare(common::ObIAllocator &allocator,
                         ObSQLSessionInfo &session,
                         ObMySQLProxy &sql_proxy,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         sql::ObRawExprFactory &expr_factory,
                         const ObString &sql,
                         bool is_cursor,
                         pl::ObPLBlockNS *secondary_namespace,
                         ObSPIPrepareResult &prepare_result);
  static int spi_execute(pl::ObPLExecCtx *ctx,
                         uint64_t id,
                         int64_t type,
                         const ObSqlExpression **param_exprs,
                         int64_t param_count,
                         const ObSqlExpression **into_exprs,
                         int64_t into_count,
                         const ObDataType *column_types,
                         int64_t type_count,
                         const bool *exprs_not_null_flag,
                         const int64_t *pl_integer_rangs,
                         bool is_bulk = false,
                         bool is_forall = false);

  static int spi_execute_immediate(pl::ObPLExecCtx *ctx,
                                   const ObSqlExpression *sql,
                                   common::ObObjParam **params,
                                   const int64_t *params_mode,
                                   int64_t param_count,
                                   const ObSqlExpression **into_exprs,
                                   int64_t into_count,
                                   const ObDataType *column_types,
                                   int64_t type_count,
                                   const bool *exprs_not_null_flag,
                                   const int64_t *pl_integer_rangs,
                                   bool is_bulk = false,
                                   bool is_returning = false);

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
  static int spi_cursor_open(pl::ObPLExecCtx *ctx,
                             const char *sql,
                             uint64_t id,
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
                             int64_t cursor_param_count);
  static int dbms_cursor_open(pl::ObPLExecCtx *ctx,
                              pl::ObDbmsCursorInfo &cursor,
                              uint64_t stmt_id,
                              int64_t stmt_type,
                              bool for_update,
                              bool has_hidden_rowid);
  static int spi_dynamic_open(pl::ObPLExecCtx *ctx,
                              const ObSqlExpression *sql,
                              const ObSqlExpression **sql_param_exprs,
                              int64_t sql_param_count,
                              uint64_t package_id,
                              uint64_t routine_id,
                              int64_t cursor_index);
  static int dbms_dynamic_open(pl::ObPLExecCtx *ctx,
                               pl::ObDbmsCursorInfo &cursor);
  static int dbms_cursor_fetch(pl::ObPLExecCtx *ctx,
                              pl::ObDbmsCursorInfo &cursor,
                              bool is_server_cursor = false);
  static int spi_cursor_fetch(pl::ObPLExecCtx *ctx,
                              uint64_t package_id,
                              uint64_t routine_id,
                              int64_t cursor_index,
                              const ObSqlExpression **into_exprs,
                              int64_t into_count,
                              const ObDataType *column_types,
                              int64_t type_count,
                              const bool *exprs_not_null_flag,
                              const int64_t *pl_integer_rangs,
                              bool is_bulk,
                              int64_t limit);
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
                                   const ObSqlExpression *collection_expr,
                                   int64_t coluln_count,
                                   const ObSqlExpression *n_expr,
                                   const ObSqlExpression *i_expr = NULL,
                                   uint64_t package_id = OB_INVALID_ID);

  static int spi_set_collection(int64_t tenant_id,
                                  const pl::ObPLINS *ns,
                                  ObIAllocator &allocator,
                                  pl::ObPLCollection &coll,
                                  int64_t n,
                                  bool extend_mode = false);

  static int spi_reset_collection(pl::ObPLCollection *coll);

  static int spi_raise_application_error(pl::ObPLExecCtx *ctx,
                                         const ObSqlExpression *errcode_expr,
                                         const ObSqlExpression *errmsg_expr);

  static int spi_process_resignal(pl::ObPLExecCtx *ctx,
                                  const ObSqlExpression *errcode_expr,
                                  const ObSqlExpression *errmsg_expr,
                                  const char *sql_state,
                                  int *error_code,
                                  const char *resignal_sql_state,
                                  bool is_signal);

  static int spi_delete_collection(pl::ObPLExecCtx *ctx,
                                   const ObSqlExpression *collection_expr,
                                   int64_t row_size,
                                   const ObSqlExpression *m_expr,
                                   const ObSqlExpression *n_expr);

  static int spi_trim_collection(pl::ObPLExecCtx *ctx,
                                   const ObSqlExpression *collection_expr,
                                   int64_t row_size,
                                   const ObSqlExpression *n_expr);

  static int acquire_spi_conn(ObMySQLProxy &sql_proxy,
                              ObSQLSessionInfo &session_info,
                              observer::ObInnerSQLConnection *&spi_conn);

  static int spi_destruct_collection(pl::ObPLExecCtx *ctx, int64_t idx);

  static int spi_init_collection(pl::ObPLExecCtx *ctx, pl::ObPLCollection *src, pl::ObPLCollection *dest, int64_t row_size, uint64_t package_id = OB_INVALID_ID);

  static int spi_sub_nestedtable(pl::ObPLExecCtx *ctx, int64_t src_idx, int64_t dst_idx, int32_t lower, int32_t upper);

  static int spi_get_package_allocator(pl::ObPLExecCtx *ctx, uint64_t package_id, ObIAllocator *&allocator);

  static int spi_copy_datum(pl::ObPLExecCtx *ctx,
                            ObIAllocator *allocator,
                            ObObj *src,
                            ObObj *dest,
                            ObDataType *dest_type,
                            uint64_t package_id = OB_INVALID_ID);

  static int spi_build_record_type_by_result_set(common::ObIAllocator &allocator,
                                                 ObSQLSessionInfo &session,
                                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                                 const sql::ObResultSet &result_set,
                                                 int64_t hidden_column_count,
                                                 pl::ObRecordType *&record_type,
                                                 uint64_t &rowid_table_id);

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

  static int spi_interface_impl(pl::ObPLExecCtx* ctx, int64_t func_addr);

  static int process_function_out_result(pl::ObPLExecCtx *ctx,
                                         ObMySQLProxy::MySQLResult &mysql_result,
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
                             bool is_returning,
                             int64_t param_cnt,
                             ObSqlString &sql_str,
                             ObPsStmtId &id,
                             stmt::StmtType &type,
                             bool &for_update,
                             bool &hidden_rowid,
                             int64_t &into_cnt);
  static int prepare_dynamic(pl::ObPLExecCtx *ctx,
                             bool is_returning,
                             bool is_dbms_sql,
                             int64_t param_cnt,
                             ObSqlString &sql_str,
                             ObPsStmtId &id,
                             stmt::StmtType &type,
                             bool &for_update,
                             bool &hidden_rowid,
                             int64_t &into_cnt,
                             common::ColumnsFieldArray *field_list = NULL);
  static int force_refresh_schema(uint64_t tenant_id);

  static int spi_update_package_change_info(
    pl::ObPLExecCtx *ctx, uint64_t package_id, uint64_t var_idx);

  static int spi_check_composite_not_null(ObObjParam *v);

  static int spi_update_location(pl::ObPLExecCtx *ctx, uint64_t location);

  static int inner_open(pl::ObPLExecCtx *pl_ctx,
                        const char *sql_str,
                        uint64_t stmt_id,
                        int64_t stmt_type,
                        ParamStore &exec_params,
                        ObMySQLProxy::MySQLResult &mysql_result,
                        ObSPIOutParams &out_params,
                        bool is_forall = false,
                        int32_t array_binding_count = 0);

  static void adjust_pl_status_for_xa(sql::ObExecContext &ctx, int &result);

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
                               uint64_t id,
                               int64_t type,
                               const ObSqlExpression **param_exprs,
                               int64_t param_count,
                               const ObSqlExpression **into_exprs,
                               int64_t into_count,
                               const ObDataType *column_types,
                               int64_t type_count,
                               const bool *exprs_not_null_flag,
                               const int64_t *pl_integer_rangs,
                               int64_t is_bulk,
                               bool is_forall = false);

  static int dbms_cursor_execute(pl::ObPLExecCtx *ctx,
                                 uint64_t stmt_id,
                                 stmt::StmtType stmt_type,
                                 pl::ObDbmsCursorInfo &cursor);

  static int adjust_out_params(ObMySQLProxy::MySQLResult &mysql_result,
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
                        uint64_t id,
                        int64_t type,
                        const ObSqlExpression **param_exprs,
                        int64_t param_count,
                        const ObSqlExpression **into_exprs,
                        int64_t into_count,
                        ObMySQLProxy::MySQLResult &mysql_result,
                        ObSPIOutParams &out_params,
                        bool is_forall = false);

  static int inner_fetch(pl::ObPLExecCtx *ctx,
                         observer::ObQueryRetryCtrl &retry_ctrl,
                         sqlclient::ObMySQLResult *result_set,
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
                         int64_t limit = INT64_MAX);
    static int inner_fetch_with_retry(
                         pl::ObPLExecCtx *ctx,
                         sqlclient::ObMySQLResult *result_set,
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
                         int64_t last_exec_time);

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
                         int64_t limit = INT64_MAX);

  static int fetch_row(void *result_set,
                       bool is_streaming,
                       int64_t &row_count,
                       ObNewRow &row);

  static int collect_cells(ObNewRow &row,
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
                          ObIArray<ObObj> &obj_array);

  static int store_result(ObIArray<pl::ObPLCollection*> &bulk_tables,
                          int64_t row_count,
                          int64_t column_count,
                          const ObIArray<ObObj> &obj_array,
                          bool append_mode);

  static int store_datums(ObObj &dest_addr, const ObIArray<ObObj> &result);

  static int store_datum(int64_t &current_addr, const ObObj &obj);

  static int fill_cursor(sqlclient::ObMySQLResult *mysql_result, ObSPICursor *cursor);

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
    sqlclient::ObMySQLResult *result, common::ObObjParam **params, int64_t param_count);

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
                                     int64_t limit);

  static int check_package_dest_and_deep_copy(pl::ObPLExecCtx &ctx,
                                    const ObSqlExpression &expr,
                                    ObIArray<ObObj> &src_array,
                                    ObIArray<ObObj> &dst_array);
};

}
}



#endif /* OCEANBASE_SRC_SQL_OB_SPI_H_ */
