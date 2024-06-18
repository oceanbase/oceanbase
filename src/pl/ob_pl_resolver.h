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

#ifndef OCEANBASE_SRC_PL_OB_PL_RESOLVER_H_
#define OCEANBASE_SRC_PL_OB_PL_RESOLVER_H_

#include "ob_pl_stmt.h"
#include "parser/ob_pl_parser.h"
#include "sql/ob_spi.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_sequence_namespace_checker.h"
#include "objit/common/ob_item_type.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_warning.h"
#endif

#ifndef LOG_IN_CHECK_MODE
#define LOG_IN_CHECK_MODE(fmt, args...) \
  do {\
    if (resolve_ctx_.is_check_mode_) {\
      LOG_INFO(fmt, ##args);\
    } else {\
      LOG_WARN(fmt, ##args);\
    }\
  } while(0);
#endif

#ifdef NDEBUG
#ifndef SET_LOG_CHECK_MODE
#define SET_LOG_CHECK_MODE()                        \
  bool set_check_mode = false;                      \
  if (!IS_OB_LOG_TRACE_MODE()) {                    \
    SET_OB_LOG_TRACE_MODE();                        \
    set_check_mode = true;                          \
  }
#endif // SET_LOG_CHECK_MODE

#ifndef CANCLE_LOG_CHECK_MODE
#define CANCLE_LOG_CHECK_MODE()                     \
  if (set_check_mode) {                             \
    if (OB_FAIL(ret)) {                             \
      if (OB_LOG_NEED_TO_PRINT(DEBUG)) {            \
        PRINT_OB_LOG_TRACE_BUF(PL, DEBUG);          \
      } else {                                      \
        PRINT_OB_LOG_TRACE_BUF(PL, INFO);           \
      }                                             \
    }                                               \
    CANCLE_OB_LOG_TRACE_MODE();                     \
  }
#endif // CANCLE_LOG_CHECK_MODE
#else
#define SET_LOG_CHECK_MODE()
#define CANCLE_LOG_CHECK_MODE()
#endif

namespace oceanbase {
using sql::ObObjAccessIdent;
using sql::ObRawExprFactory;
using sql::ObSQLSessionInfo;
using sql::stmt::StmtType;
using sql::ObRawExpr;
using sql::ObConstRawExpr;
namespace pl
{
class ObPLPackageGuard;
class ObPLResolveCtx : public ObPLINS
{
public:
  ObPLResolveCtx(common::ObIAllocator &allocator,
                 sql::ObSQLSessionInfo &session_info,
                 share::schema::ObSchemaGetterGuard &schema_guard,
                 ObPLPackageGuard &package_guard,
                 common::ObMySQLProxy &sql_proxy,
                 bool is_ps,
                 bool is_check_mode = false,
                 bool is_sql_scope_ = false,
                 const ParamStore *param_list = NULL,
                 sql::ExternalParams *extern_param_info = NULL,
                 TgTimingEvent tg_timing_event = TgTimingEvent::TG_TIMING_EVENT_INVALID,
                 bool is_sync_package_var = false) :
        allocator_(allocator),
        session_info_(session_info),
        schema_guard_(schema_guard),
        package_guard_(package_guard),
        sql_proxy_(sql_proxy),
        params_(),
        is_prepare_protocol_(is_ps),
        is_check_mode_(is_check_mode),
        is_sql_scope_(is_sql_scope_),
        extern_param_info_(extern_param_info),
        is_udt_udf_ctx_(false),
        is_sync_package_var_(is_sync_package_var)
  {
    params_.param_list_ = param_list;
    params_.tg_timing_event_ = tg_timing_event;
  }
  virtual ~ObPLResolveCtx() {
  }

  virtual int get_user_type(uint64_t type_id,
                            const ObUserDefinedType *&user_type,
                            ObIAllocator *allocator = NULL) const;

  common::ObIAllocator &allocator_;
  sql::ObSQLSessionInfo &session_info_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  ObPLPackageGuard &package_guard_;
  common::ObMySQLProxy &sql_proxy_;
  sql::ObResolverParams params_;
  bool is_prepare_protocol_;
  bool is_check_mode_;
  bool is_sql_scope_; // 标识是否是由纯SQL语句过来的表达式解析请求
  sql::ExternalParams *extern_param_info_;
  bool is_udt_udf_ctx_; // indicate this context is belong to a udt udf
  bool is_sync_package_var_;
};

class ObPLMockSelfArg
{
public:
  ObPLMockSelfArg(
    const ObIArray<ObObjAccessIdx> &access_idxs,
    ObSEArray<ObRawExpr*, 4> &expr_params,
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo &session_info)
    : access_idxs_(access_idxs),
      expr_params_(expr_params),
      expr_factory_(expr_factory),
      session_info_(session_info),
      mark_only_(false),
      mocked_(false) {}
  int mock();
  ~ObPLMockSelfArg();
private:
  const ObIArray<ObObjAccessIdx> &access_idxs_;
  ObSEArray<ObRawExpr*, 4> &expr_params_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo &session_info_;
  bool mark_only_;
  bool mocked_;
};

class ObPLPackageAST;
class ObPLResolver
{
public:
  static const uint64_t PACKAGE_BLOCK_NUM_CHILD = 4;
  static const uint64_t PACKAGE_BODY_BLOCK_NUM_CHILD = 4;

  static const char *ANONYMOUS_BLOCK;
  static const char *ANONYMOUS_ARG;
  static const char *ANONYMOUS_SQL_ARG;
  static const char *ANONYMOUS_INOUT_ARG;

public:
  class HandlerAnalyzer
  {
  public:
    HandlerAnalyzer() :
      handler_stack_(),
      top_continue_(OB_INVALID_INDEX) ,
      top_notfound_level_(OB_INVALID_INDEX),
      top_warning_level_(OB_INVALID_INDEX) {}
    virtual ~HandlerAnalyzer() {}

  public:
    int set_handlers(const common::ObIArray<ObPLDeclareHandlerStmt::DeclareHandler> &handlers, int64_t level);
    int reset_handlers(int64_t level);
    int set_handler(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc, int64_t level) { return handler_stack_.push_back(ObPLDeclareHandlerStmt::DeclareHandler(level, desc)); }
    int get_handler(int64_t i, ObPLDeclareHandlerStmt::DeclareHandler &info) { return handler_stack_.at(i, info); }
    inline void reset_notfound_and_warning(int64_t level)
    {
      top_notfound_level_ = level < top_notfound_level_ ? OB_INVALID_INDEX : top_notfound_level_;
      top_warning_level_ = level < top_warning_level_ ? OB_INVALID_INDEX : top_warning_level_;
    }
    inline void set_notfound(int64_t level) { top_notfound_level_ = level; }
    inline void set_warning(int64_t level) { top_warning_level_ = level; }
    inline int64_t get_continue() { return top_continue_; }
    inline void set_continue() { top_continue_ = handler_stack_.count() - 1; }
    inline void set_continue(int64_t top_continue) { top_continue_ = top_continue; }
    inline int64_t get_stack_depth() { return handler_stack_.count(); }
    inline bool in_continue() { return OB_INVALID_INDEX != top_continue_; }
    inline bool in_notfound() { return OB_INVALID_INDEX != top_notfound_level_; }
    inline bool in_warning() { return OB_INVALID_INDEX != top_warning_level_; }

  private:
    ObArray<ObPLDeclareHandlerStmt::DeclareHandler> handler_stack_;
    int64_t top_continue_; //handler_stack_里top continue的下标
    int64_t top_notfound_level_; //第一个notfound的level，注意：这里不是handler_stack_的下标
    int64_t top_warning_level_; //第一个warning的level，注意：这里不是handler_stack_的下标
  };

public:
  ObPLResolver(common::ObIAllocator &allocator,
               sql::ObSQLSessionInfo &session_info,
               share::schema::ObSchemaGetterGuard &schema_guard,
               ObPLPackageGuard &package_guard,
               common::ObMySQLProxy &sql_proxy,
               sql::ObRawExprFactory &expr_factory,
               const ObPLBlockNS *parent_ns,
               bool is_ps,
               bool is_check_mode_ = false,
               bool is_sql_scope_ = false,
               const ParamStore *param_list = NULL,
               sql::ExternalParams *extern_param_info = NULL,
               TgTimingEvent tg_timing_event = TgTimingEvent::TG_TIMING_EVENT_INVALID) :
    resolve_ctx_(allocator,
                 session_info,
                 schema_guard,
                 package_guard,
                 sql_proxy,
                 is_ps,
                 is_check_mode_,
                 is_sql_scope_,
                 param_list,
                 extern_param_info,
                 tg_timing_event),
    external_ns_(resolve_ctx_, parent_ns),
    expr_factory_(expr_factory),
    stmt_factory_(allocator),
    current_block_(NULL),
    current_level_(OB_INVALID_INDEX),
    handler_analyzer_(),
    arg_cnt_(0),
    question_mark_cnt_(0),
    next_user_defined_exception_id_(1),
    ob_sequence_ns_checker_(resolve_ctx_.params_),
    item_type_(T_MAX) { expr_factory_.set_is_called_sql(false); }
  virtual ~ObPLResolver() {}

  enum GotoRestrictionType {
    RESTRICTION_UNKNOWN_TYPE = -1,
    RESTRICTION_NO_RESTRICT,
    RESTRICTION_JUMP_IF_LOOP,
    RESTRICTION_JUMP_OUT_EXCEPTION,
  };
  int init(ObPLFunctionAST &func_ast);
  int init_default_exprs(
    ObPLFunctionAST &func_ast, const ObIArray<share::schema::ObRoutineParam *> &params);
  int init_default_exprs(
    ObPLFunctionAST &func_ast, const ObIArray<ObPLRoutineParam *> &params);
  int init_default_expr(
    ObPLFunctionAST &func_ast, int64_t idx, const share::schema::ObIRoutineParam &param);
  int init_default_expr(ObPLFunctionAST &func_ast,
                        const share::schema::ObIRoutineParam &param,
                        const ObPLDataType &expected_type);
  int resolve(const ObStmtNodeTree *parse_tree, ObPLFunctionAST &func_ast);
  int resolve_root(const ObStmtNodeTree *parse_tree, ObPLFunctionAST &func_ast);

  int init(ObPLPackageAST &package_ast);
  int resolve(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast);
  int resolve_goto_stmts(ObPLFunctionAST &func_ast);
  int verify_goto_stmt_restriction(const ObPLStmt &goto_stmt,
                                   const ObPLStmt &dst_stmt,
                                   GotoRestrictionType &result);
  int check_goto_cursor_stmts(ObPLGotoStmt &goto_stmt,
                              const ObPLStmt &dst_stmt);
  int check_contain_goto_block(const ObPLStmt *cur_stmt,
                               const ObPLStmtBlock *goto_block,
                               bool &is_contain);
public:
  inline ObPLExternalNS &get_external_ns() { return external_ns_; }
  inline const ObPLResolveCtx &get_resolve_ctx() const { return resolve_ctx_; }
  inline ObPLBlockNS &get_current_namespace() const { return current_block_->get_namespace(); }
  inline const ObIArray<int64_t> &get_subprogram_path() const { return current_subprogram_path_; }
  inline int push_subprogram_path(int64_t idx) { return current_subprogram_path_.push_back(idx); }
  inline void pop_subprogram_path() { return current_subprogram_path_.pop_back(); }
  inline const ObIArray<ObPLStmt *> &get_goto_stmts() const { return goto_stmts_; }
  inline int push_goto_stmts(ObPLStmt *stmt) { return goto_stmts_.push_back(stmt); }
  inline void pop_goto_stmts() { goto_stmts_.pop_back(); }

public:
  /**
   * @brief 解析一个PL内部变量
   * @param [in] node      - 变量的语法节点
   * @param [in] ns  - PL的名字空间
   * @param [in] expr_factory  - 表达式工厂
   * @param [in] session_info  - session信息
   * @param [out] expr  - 解析出的表达式
   * @param [in] for_write  - 该变量是否是赋值的左值
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   */
  static int resolve_raw_expr(const ParseNode &node,
                              common::ObIAllocator &allocator,
                              ObRawExprFactory &expr_factory,
                              ObPLBlockNS &ns,
                              bool is_prepare_protocol,
                              ObRawExpr *&expr,
                              bool for_write = false);
  static int resolve_raw_expr(const ParseNode &node,
                              sql::ObResolverParams &params,
                              ObRawExpr *&expr,
                              bool for_write = false,
                              const ObPLDataType *expected_type = NULL);
  static int resolve_local_var(const ParseNode &node,
                               ObPLBlockNS &ns,
                               ObRawExprFactory &expr_factory,
                               const ObSQLSessionInfo *session_info,
                               ObSchemaGetterGuard *schema_guard,
                               ObRawExpr *&expr,
                               bool for_write = false);
  static int resolve_local_var(const ObString &var_name,
                               ObPLBlockNS &ns,
                               ObRawExprFactory &expr_factory,
                               const ObSQLSessionInfo *session_info,
                               ObSchemaGetterGuard *schema_guard,
                               ObRawExpr *&expr,
                               bool for_write = false);
  static int resolve_sp_integer_constraint(ObPLDataType &pls_type);
  static int resolve_sp_integer_type(const ParseNode *sp_data_type_node,
                                     ObPLDataType &data_type);
  static int resolve_sp_scalar_type(common::ObIAllocator &allocator,
                                    const ParseNode *sp_data_type_node,
                                    const ObString &ident_name,
                                    const sql::ObSQLSessionInfo &session_info,
                                    ObPLDataType &data_type,
                                    bool is_for_param_type = false,
                                    uint64_t package_id = OB_INVALID_ID);
  int resolve_var(sql::ObQualifiedName &q_name,
                  ObPLBlockNS &ns,
                  sql::ObRawExprFactory &expr_factory,
                  const sql::ObSQLSessionInfo *session_info,
                  ObPLCompileUnitAST &func,
                  sql::ObRawExpr *&expr,
                  bool for_write = false);
  int resolve_inner_call(const ParseNode *parse_tree, ObPLStmt *&stmt, ObPLFunctionAST &func);
  int resolve_sqlcode_or_sqlerrm(sql::ObQualifiedName &q_name,
                                 ObPLCompileUnitAST &unit_ast,
                                 sql::ObRawExpr *&expr);
  int resolve_construct(const ObQualifiedName &q_name,
                                    const ObUDFInfo &udf_info,
                                    const ObUserDefinedType &user_type,
                                    ObRawExpr *&expr);
  int resolve_construct(const sql::ObQualifiedName &q_name,
                        const sql::ObUDFInfo &udf_info,
                        ObRawExpr *&expr);
  int resolve_record_construct(const sql::ObQualifiedName &q_name,
                               const sql::ObUDFInfo &udf_info,
                               const ObUserDefinedType *user_type,
                               ObRawExpr *&expr);
  int resolve_object_construct(const sql::ObQualifiedName &q_name,
                               const sql::ObUDFInfo &udf_info,
                               const ObUserDefinedType *user_type,
                               ObRawExpr *&expr);
  int resolve_collection_construct(const sql::ObQualifiedName &q_name,
                                   const sql::ObUDFInfo &udf_info,
                                   const ObUserDefinedType *user_type,
                                   sql::ObRawExpr *&expr);
  int resolve_qualified_name(sql::ObQualifiedName &q_name,
                             ObIArray<sql::ObQualifiedName> &columns,
                             ObIArray<ObRawExpr*> &real_exprs,
                             ObPLCompileUnitAST &unit_ast,
                             sql::ObRawExpr *&expr);
  static
  int resolve_obj_access_node(const ParseNode &node,
                              ObSQLSessionInfo &session_info,
                              ObRawExprFactory &expr_factory,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObMySQLProxy &sql_proxy,
                              ObIArray<ObObjAccessIdent> &obj_access_idents,
                              ObIArray<ObObjAccessIdx>& access_idxs,
                              ObPLPackageGuard *package_guard);
  static
  int resolve_cparam_list_simple(const ParseNode &node,
                                 ObRawExprFactory &expr_factory,
                                 ObIArray<ObObjAccessIdent> &obj_access_idents);
  static
  int resolve_obj_access_idents(const ParseNode &node,
                                ObRawExprFactory &expr_factory,
                                common::ObIArray<ObObjAccessIdent> &obj_access_idents,
                                ObSQLSessionInfo &session_info);
  static
  int record_error_line(const ObStmtNodeTree *parse_tree, ObSQLSessionInfo &session_info);
  static
  int record_error_line(ObSQLSessionInfo &session_info, const int32_t line, const int32_t col);
  static
  int resolve_access_ident(const ObObjAccessIdent &access_ident, ObPLExternalNS &external_ns,
                           common::ObIArray<ObObjAccessIdx> &access_idexs);

  static
  int get_view_select_stmt(
    const ObPLResolveCtx &ctx,
    const share::schema::ObTableSchema* view_schema,
    sql::ObSelectStmt *&select_stmt);
  static
  int fill_record_type(share::schema::ObSchemaGetterGuard &schema_guard,
                       common::ObIAllocator &allocator,
                       sql::ObSelectStmt *select_stmt,
                       ObRecordType *&record_type);
  static
  int build_record_type_by_view_schema(const ObPLResolveCtx &resolve_ctx,
                                const share::schema::ObTableSchema* view_schema,
                                ObRecordType *&record_type,
                                ObIArray<ObSchemaObjVersion> *dependency_objects = NULL);
  static
  int build_record_type_by_table_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                common::ObIAllocator &allocator,
                                const share::schema::ObTableSchema* table_schema,
                                ObRecordType *&record_type,
                                bool with_rowid = false);
  static int collect_dep_info_by_view_schema(const ObPLResolveCtx &ctx,
                                             const ObTableSchema* view_schema,
                                             ObIArray<ObSchemaObjVersion> &dependency_objects);
  static int collect_dep_info_by_schema(const ObPLResolveCtx &ctx,
                                        const ObTableSchema* table_schema,
                                        ObIArray<ObSchemaObjVersion> &dependency_objects);
  static
  int build_record_type_by_schema(const ObPLResolveCtx &resolve_ctx,
                                const share::schema::ObTableSchema* table_schema,
                                ObRecordType *&record_type, bool with_rowid = false,
                                ObIArray<ObSchemaObjVersion> *dependency_objects = NULL);

  static
  int resolve_extern_type_info(bool is_row_type,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const ObSQLSessionInfo &session_info,
                               const ObIArray<ObObjAccessIdent> &access_idents,
                               ObPLExternTypeInfo *extern_type_info);
  static void pl_reset_warning_buffer()
  {
    ObWarningBuffer *buf = common::ob_get_tsi_warning_buffer();
    if (NULL != buf) {
      buf->reset();
    }
  }
  static bool is_object_not_exist_error(int ret)
  {
    return OB_ERR_SP_DOES_NOT_EXIST == ret
           || OB_ERR_SP_UNDECLARED_VAR == ret
           || OB_ERR_SP_UNDECLARED_TYPE == ret
           || OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret
           || OB_TABLE_NOT_EXIST == ret
           || OB_ERR_COLUMN_NOT_FOUND == ret
           || OB_ERR_BAD_DATABASE == ret;
  }

  static int get_subprogram_ns(
    ObPLBlockNS &current_ns, uint64_t subprogram_id, ObPLBlockNS *&subprogram_ns);
  static int get_local_variable_constraint(
    const ObPLBlockNS &ns, int64_t var_idx, bool &not_null, ObPLIntegerRange &range);

  static int restriction_on_result_cache(share::schema::ObIRoutineInfo *routine_info);
  static int resolve_sf_clause(const ObStmtNodeTree *node,
                               share::schema::ObIRoutineInfo *routine_info,
                               ObProcType &routine_type,
                               const ObPLDataType &ret_type);
  static int build_pl_integer_type(ObPLIntegerType type, ObPLDataType &data_type);
  static bool is_question_mark_value(ObRawExpr *into_expr, ObPLBlockNS *ns);
  static int set_question_mark_type(ObRawExpr *into_expr, ObPLBlockNS *ns, const ObPLDataType *type);

  static
  int build_obj_access_func_name(const ObIArray<ObObjAccessIdx> &access_idxs,
                                 ObRawExprFactory &expr_factory,
                                 const sql::ObSQLSessionInfo *session_info,
                                 ObSchemaGetterGuard *schema_guard,
                                 bool for_write,
                                 ObString &result);
  static
  int set_write_property(ObRawExpr *obj_expr,
                         ObRawExprFactory &expr_factory,
                         const ObSQLSessionInfo *session_info,
                         ObSchemaGetterGuard *schema_guard,
                         bool for_write);
  int get_caller_accessor_item(
    const ObPLStmtBlock *caller, AccessorItem &caller_item);
  int check_package_accessible(
    const ObPLStmtBlock *caller,
    share::schema::ObSchemaGetterGuard &guard,
    uint64_t package_id);
  int check_package_accessible(
    const ObPLStmtBlock *caller,
    share::schema::ObSchemaGetterGuard &guard,
    const ObPLRoutineInfo &routine_info);
  int check_routine_accessible(
    const ObPLStmtBlock *caller,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObRoutineInfo &routine_info);
  int check_package_accessible(
    AccessorItem &caller, const ObString &package_body);
  int check_routine_accessible(
    AccessorItem &caller, const ObString &routine_body);
  int check_common_accessible(
    AccessorItem &caller, ObIArray<AccessorItem> &accessors);
  int resolve_routine_accessible_by(
    const ObString source, ObIArray<AccessorItem> &result);
  int resolve_package_accessible_by(
    const ObString source, ObIArray<AccessorItem> &result);
  int resolve_accessible_by(
    const ObStmtNodeTree *accessor_list, ObIArray<AccessorItem> &result);

  static
  int resolve_sp_subtype_precision(ObSQLSessionInfo &session_info,
                                   ObIArray<ObRawExpr*>& params,
                                   const ObUserDefinedType *user_type,
                                   ObPLDataType &pl_type);
  static bool is_json_type_compatible(
    const ObUserDefinedType *left, const ObUserDefinedType *right);
  static int check_composite_compatible(const ObPLINS &ns,
    uint64_t left_type_id, uint64_t right_type_id, bool &is_compatible);

  static
  int resolve_nocopy_params(const share::schema::ObIRoutineInfo *routine_info,
                            sql::ObUDFInfo &udf_info);
  static
  int resolve_nocopy_params(const ObIArray<share::schema::ObIRoutineParam *> &formal_param_list,
                            ObPLCallStmt *call_stmt);
  static
  int resolve_nocopy_params(const ObIArray<share::schema::ObIRoutineParam *> &formal_params_list,
                            const ObIArray<const ObRawExpr *> &actual_params_list,
                            ObIArray<int64_t> &nocopy_params);
  int64_t combine_line_and_col(const ObStmtLoc &loc);
  int resolve_routine_decl_param_list(const ParseNode *param_list,
      ObPLCompileUnitAST &package_ast,
      ObPLRoutineInfo &routine_info);

  // for condition compile
  static
  int resolve_condition_compile(
    ObIAllocator &allocator,
    ObSQLSessionInfo *session_info,
    ObSchemaGetterGuard *schema_guard,
    ObPLPackageGuard *package_guard,
    ObMySQLProxy *sql_proxy,
    const ObString *exec_env,
    const ParseNode *node,
    const ParseNode *&new_node,
    bool is_inner_parse = false,
    bool is_for_trigger = false,
    bool is_for_dynamic = false,
    bool *is_include_old_new_in_trigger = NULL,
    ObPLDependencyTable *dep_table = NULL);
  int resolve_condition_compile(
    const ParseNode *node,
    const ParseNode *&new_node,
    int64_t &question_mark_count,
    bool is_inner_parse = false,
    bool is_for_trigger = false,
    bool is_for_dynamic = false,
    bool *is_include_old_new_in_trigger = NULL,
    ObPLDependencyTable *dep_table = NULL);
  int resolve_condition_compile(
    ObPLFunctionAST &unit_ast, const ParseNode &node, ObString &old_sql, ObString &new_sql);
  int resolve_preprocess_stmt(
    ObPLFunctionAST &unit_ast,
    const ParseNode &node, ObString &old_sql, ObString &new_sql, int64_t &start, int64_t &end);
  int resolve_pre_if_stmt(
    ObPLFunctionAST &unit_ast,
    const ParseNode &node, ObString &old_sql, ObString &new_sql, int64_t &start, int64_t &end);
  int resolve_pre_else_stmt(
    ObPLFunctionAST &unit_ast,
    const ParseNode &node, ObString &old_sql, ObString &new_sql, int64_t &start, int64_t &end);
  int append_sql(
    ObString &old_sql, ObString &new_sql, int64_t start, int64_t end, bool fill_blank = false);
  int resolve_and_calc_static_expr(
    ObPLFunctionAST &unit_ast, const ParseNode &node, ObObjType expect_type, ObObj &result);
  int replace_source_string(const ObString &old_sql, ParseNode *new_node);
  int replace_plsql_line(
    common::ObIAllocator &allocator, const ObStmtNodeTree *node, ObString &sql, ObString &new_sql);
  int resolve_error_stmt(ObPLFunctionAST &unit_ast, const ParseNode &node);

  int is_bool_literal_expr(const ObRawExpr *expr, bool &is_bool_literal_expr);
  int is_static_expr(const ObRawExpr *expr, bool &is_static_expr);
  int is_static_bool_expr(const ObRawExpr *expr, bool &is_static_bool_expr);
  int is_pls_literal_expr(const ObRawExpr *expr, bool &is_pls_literal_expr);
  int is_static_pls_expr(const ObRawExpr *expr, bool &is_static_pls_expr);
  int is_static_pls_or_bool_expr(const ObRawExpr *expr, bool &is_static_pls_or_bool_expr);
  int is_static_relation_expr(const ObRawExpr *expr, bool &is_static_relation_expr);
  int check_static_bool_expr(const ObRawExpr *expr, bool &is_static_bool_expr);

  static int adjust_routine_param_type(ObPLDataType &type);

  int resolve_udf_info(
    sql::ObUDFInfo &udf_info, ObIArray<ObObjAccessIdx> &access_idxs, ObPLCompileUnitAST &func, const ObIRoutineInfo *routine_info = NULL);

  int construct_name(ObString &database_name, ObString &package_name, ObString &routine_name, ObSqlString &object_name);
  static int resolve_dblink_routine(ObPLResolveCtx &resolve_ctx,
                                    const ObString &dblink_name,
                                    const ObString &db_name,
                                    const ObString &pkg_name,
                                    const ObString &routine_name,
                                    const common::ObIArray<sql::ObRawExpr *> &expr_params,
                                    const ObIRoutineInfo *&routine_info);
  static int resolve_dblink_routine_with_synonym(ObPLResolveCtx &resolve_ctx,
                                                 const uint64_t pkg_syn_id,
                                                 const ObString &cur_db_name,
                                                 const ObString &routine_name,
                                                 const common::ObIArray<sql::ObRawExpr *> &expr_params,
                                                 const ObIRoutineInfo *&routine_info);
  int resolve_dblink_type_with_synonym(const uint64_t pkg_syn_id,
                                       const ObString &type_name,
                                       ObPLCompileUnitAST &func,
                                       ObPLDataType &pl_type);
  int resolve_dblink_type(const ObString &dblink_name,
                          const ObString &db_name,
                          const ObString &pkg_name,
                          const ObString &udt_name,
                          ObPLCompileUnitAST &func,
                          ObPLDataType &pl_type);
  int resolve_dblink_udf(sql::ObQualifiedName &q_name,
                         ObRawExprFactory &expr_factory,
                         ObRawExpr *&expr,
                         ObPLCompileUnitAST &unit_ast);
  static int replace_udf_param_expr(ObObjAccessIdent &access_ident,
                             ObIArray<ObQualifiedName> &columns,
                             ObIArray<ObRawExpr*> &real_exprs);
private:
  int resolve_declare_var(const ObStmtNodeTree *parse_tree, ObPLDeclareVarStmt *stmt, ObPLFunctionAST &func_ast);
  int resolve_declare_var(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast);
  int resolve_declare_var_comm(const ObStmtNodeTree *parse_tree, ObPLDeclareVarStmt *stmt,
                               ObPLCompileUnitAST &unit_ast);
#ifdef OB_BUILD_ORACLE_PL
  int resolve_declare_user_type(const ObStmtNodeTree *parse_tree, ObPLDeclareUserTypeStmt *stmt, ObPLFunctionAST &func);
  int resolve_declare_user_type(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast);
  int resolve_declare_user_type_comm(const ObStmtNodeTree *parse_tree, ObPLDeclareUserTypeStmt *stmt,
                                     ObPLCompileUnitAST &unit_ast);
  int resolve_subtype_precision(const ObStmtNodeTree *precision_node, ObPLDataType &base_type);
  int resolve_declare_user_subtype(const ObStmtNodeTree *parse_tree,
                                   ObPLDeclareUserTypeStmt *stmt,
                                   ObPLCompileUnitAST &unit_ast);
  int resolve_ref_cursor_type(const ParseNode *node, ObPLDeclareUserTypeStmt *stmt, ObPLCompileUnitAST &unit_ast);
  int resolve_declare_collection_type(const ParseNode *type_node, ObPLDeclareUserTypeStmt *stmt, ObPLCompileUnitAST &unit_ast);
  int resolve_declare_ref_cursor(const ObStmtNodeTree *parse_tree, ObPLDeclareCursorStmt *stmt, ObPLFunctionAST &func);
#endif
  int resolve_declare_record_type(const ParseNode *type_node, ObPLDeclareUserTypeStmt *stmt, ObPLCompileUnitAST &unit_ast);
  int resolve_extern_type_info(share::schema::ObSchemaGetterGuard &schema_guard,
                               const ObSQLSessionInfo &session_info,
                               const ObIArray<ObObjAccessIdent> &access_idents,
                               ObPLExternTypeInfo *extern_type_info);
  int resolve_extern_type_info(share::schema::ObSchemaGetterGuard &guard,
                               const ObIArray<ObObjAccessIdx> &access_idxs,
                               ObPLExternTypeInfo *extern_type_info);
  int resolve_sp_row_type(const ParseNode *sp_data_type_node,
                          ObPLCompileUnitAST &func,
                          ObPLDataType &pl_type,
                          ObPLExternTypeInfo *extern_type_info = NULL,
                          bool with_rowid = false);
  int resolve_dblink_row_type_node(const ParseNode &access_node,
                                   const ParseNode &dblink_node,
                                   ObPLDataType &pl_type,
                                   bool is_row_type);
  int resolve_dblink_row_type(const ObString &db_name,
                              const ObString &table_name,
                              const ObString &col_name,
                              const ObString &dblink_name,
                              ObPLDataType &pl_type,
                              bool is_row_type);
  int resolve_dblink_row_type_with_synonym(ObPLResolveCtx &resolve_ctx,
                                           const common::ObIArray<ObObjAccessIdx> &access_idxs,
                                           ObPLDataType &pl_type,
                                           bool is_row_type);
  int resolve_dblink_type(const ParseNode *node,
                          ObPLCompileUnitAST &func,
                          ObPLDataType &pl_type);
  int resolve_sp_composite_type(const ParseNode *sp_data_type_node,
                                ObPLCompileUnitAST &func,
                                ObPLDataType &data_type,
                                ObPLExternTypeInfo *extern_type_info = NULL);
  int resolve_sp_data_type(const ParseNode *sp_data_type_node,
                           const common::ObString &ident_name,
                           ObPLCompileUnitAST &func,
                           ObPLDataType &data_type,
                           ObPLExternTypeInfo *extern_type_info = NULL,
                           bool with_rowid = false,
                           bool is_for_param_type = false);
  int resolve_assign(const ObStmtNodeTree *parse_tree, ObPLAssignStmt *stmt, ObPLFunctionAST &func);
  int resolve_if(const ObStmtNodeTree *parse_tree, ObPLIfStmt *stmt, ObPLFunctionAST &func);
  int resolve_case(const ObStmtNodeTree *parse_tree, ObPLCaseStmt *stmt, ObPLFunctionAST &func);
  int resolve_iterate(const ObStmtNodeTree *parse_tree, ObPLIterateStmt *stmt, ObPLFunctionAST &func);
  int resolve_leave(const ObStmtNodeTree *parse_tree, ObPLLeaveStmt *stmt, ObPLFunctionAST &func);
  int resolve_while(const ObStmtNodeTree *parse_tree, ObPLWhileStmt *stmt, ObPLFunctionAST &func);
  int resolve_for_loop(const ObStmtNodeTree *parse_tree, ObPLForLoopStmt *stmt, ObPLFunctionAST &func);
  int resolve_cursor_for_loop(const ObStmtNodeTree *parse_tree, ObPLCursorForLoopStmt *stmt, ObPLFunctionAST &func);
  int resolve_forall(const ObStmtNodeTree *parse_tree, ObPLForAllStmt *stmt, ObPLFunctionAST &func);
  int resolve_repeat(const ObStmtNodeTree *parse_tree, ObPLRepeatStmt *stmt, ObPLFunctionAST &func);
  int resolve_loop(const ObStmtNodeTree *parse_tree, ObPLLoopStmt *stmt, ObPLFunctionAST &func);
  int resolve_return(const ObStmtNodeTree *parse_tree, ObPLReturnStmt *stmt, ObPLFunctionAST &func);
  int resolve_sql(const ObStmtNodeTree *parse_tree, ObPLSqlStmt *stmt, ObPLFunctionAST &func);
  int resolve_execute_immediate(const ObStmtNodeTree *parse_tree, ObPLExecuteStmt *stmt, ObPLFunctionAST &func);
  int resolve_extend(const ObStmtNodeTree *parse_tree, ObPLExtendStmt *stmt, ObPLFunctionAST &func);
  int resolve_declare_cond(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast);
  int resolve_declare_cond(const ObStmtNodeTree *parse_tree,
                           ObPLDeclareCondStmt *stmt,
                           ObPLCompileUnitAST &func);
  int resolve_declare_handler(const ObStmtNodeTree *parse_tree, ObPLDeclareHandlerStmt *stmt, ObPLFunctionAST &func);
  int resolve_resignal(const ObStmtNodeTree *parse_tree, ObPLSignalStmt *stmt, ObPLFunctionAST &func);
  int resolve_signal(const ObStmtNodeTree *parse_tree, ObPLSignalStmt *stmt, ObPLFunctionAST &func);
  int resolve_call(const ObStmtNodeTree *parse_tree, ObPLCallStmt *stmt, ObPLFunctionAST &func);
  int resolve_call_param_list(const ObStmtNodeTree *parse_tree,
                              const common::ObIArray<ObPLRoutineParam *> &param_list,
                              ObPLCallStmt *stmt,
                              ObPLFunctionAST &func);
  int resolve_call_param_list(common::ObIArray<ObRawExpr*> &params,
                              const common::ObIArray<ObPLRoutineParam *> &param_list,
                              ObPLCallStmt *stmt,
                              ObPLFunctionAST &func);
  int resolve_call_param_list(const ObStmtNodeTree *parse_tree,
                              const common::ObIArray<share::schema::ObRoutineParam *> &param_list,
                              ObPLCallStmt *stmt,
                              ObPLFunctionAST &func);
  int resolve_call_param_list(common::ObIArray<ObRawExpr*> &params,
                              const common::ObIArray<share::schema::ObRoutineParam*> &param_list,
                              ObPLCallStmt *stmt,
                              ObPLFunctionAST &func);
  int resolve_declare_cursor(const ObStmtNodeTree *parse_tree, ObPLPackageAST &func);
  int resolve_declare_cursor(const ObStmtNodeTree *parse_tree,
                             ObPLDeclareCursorStmt *stmt,
                             ObPLCompileUnitAST &func);
  int resolve_cursor_actual_params(const ObStmtNodeTree *parse_tree, ObPLStmt *stmt, ObPLFunctionAST &func);
  int resolve_open(const ObStmtNodeTree *parse_tree, ObPLOpenStmt *stmt, ObPLFunctionAST &func);
  int resolve_open_for(const ObStmtNodeTree *parse_tree, ObPLOpenForStmt *stmt, ObPLFunctionAST &func);
  int resolve_fetch(const ObStmtNodeTree *parse_tree, ObPLFetchStmt *stmt, ObPLFunctionAST &func);
  int resolve_close(const ObStmtNodeTree *parse_tree, ObPLCloseStmt *stmt, ObPLFunctionAST &func);
  int resolve_null(const ObStmtNodeTree *parse_tree, ObPLNullStmt *stmt, ObPLFunctionAST &func);
  int resolve_interface(const ObStmtNodeTree *parse_tree,
                        ObPLInterfaceStmt *stmt,
                        ObPLFunctionAST &ast);
  int resolve_routine_decl(const ObStmtNodeTree *parse_tree,
                           ObPLCompileUnitAST &package_ast,
                           ObPLRoutineInfo *&routine_info,
                           bool is_udt_routine = false,
                           bool resolve_routine_def = false); // 当前是否在resolve routine define
  int resolve_routine_block(const ObStmtNodeTree *parse_tree,
                            const ObPLRoutineInfo &routine_info,
                            ObPLFunctionAST &routine_ast);
  int resolve_routine_def(const ObStmtNodeTree *parse_tree,
                          ObPLCompileUnitAST &package_ast,
                          bool is_udt_routine = false);
  int resolve_init_routine(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast);

  int resolve_pipe_row(const ObStmtNodeTree *parse_tree,
                       ObPLPipeRowStmt *stmt,
                       ObPLFunctionAST &func);
  int resolve_do(const ObStmtNodeTree *parse_tree, 
                ObPLDoStmt *stmt, 
                ObPLFunctionAST &func);
#ifdef OB_BUILD_ORACLE_PL
  int resolve_object_elem_spec_def(const ParseNode *parse_tree,
                                   ObPLCompileUnitAST &package_ast);
  int resolve_object_def(const ParseNode *parse_tree,
                         ObPLCompileUnitAST &package_ast);
  int resolve_object_elem_spec_list(const ParseNode *parse_tree,
                                    ObPLCompileUnitAST &package_ast);
  int resolve_object_constructor(const ParseNode *parse_tree,
                                 ObPLCompileUnitAST &package_ast,
                                 ObPLRoutineInfo *&routine_info);
#endif
private:
  int resolve_ident(const ParseNode *node, common::ObString &ident);
  int build_raw_expr(const ParseNode *node, ObPLCompileUnitAST &unit_ast, ObRawExpr *&expr,
                     const ObPLDataType *expected_type = NULL);
  int get_actually_pl_type(const ObPLDataType *&type);
  int set_cm_warn_on_fail(ObRawExpr *&expr);
  int analyze_expr_type(ObRawExpr *&expr,
                        ObPLCompileUnitAST &unit_ast);
  int set_udf_expr_line_number(ObRawExpr *expr, uint64_t line_number);
  int resolve_expr(const ParseNode *node, ObPLCompileUnitAST &unit_ast,
                   sql::ObRawExpr *&expr, uint64_t line_number = 0, /* where this expr called */
                   bool need_add = true, const ObPLDataType *expected_type = NULL,
                   bool is_behind_limit = false, bool is_add_bool = false);
  int resolve_columns(ObRawExpr *&expr, ObArray<sql::ObQualifiedName> &columns, ObPLCompileUnitAST &unit_ast);
  int formalize_expr(ObRawExpr &expr);
  static int formalize_expr(ObRawExpr &expr, const ObSQLSessionInfo *session_info, const ObPLINS &ns);
  int resolve_var(sql::ObQualifiedName &q_name,
                  ObPLCompileUnitAST &func,
                  sql::ObRawExpr *&expr,
                  bool for_write = false);
  int resolve_udf_without_brackets(sql::ObQualifiedName &q_name, ObPLCompileUnitAST &unit_ast, ObRawExpr *&expr);
  int make_self_symbol_expr(ObPLCompileUnitAST &func, ObRawExpr *&expr);
  int add_udt_self_argument(const ObIRoutineInfo *routine_info,
                            ObObjAccessIdent &access_ident,
                            ObIArray<ObObjAccessIdx> &access_idxs,
                            ObPLCompileUnitAST &func);
  int add_udt_self_argument(const ObIRoutineInfo *routine_info,
                            ObIArray<ObRawExpr*> &expr_params,
                            ObIArray<ObObjAccessIdx> &access_idxs,
                            ObUDFInfo *udf_info,
                            ObPLCompileUnitAST &func);
  int resolve_qualified_identifier(sql::ObQualifiedName &q_name,
                                           ObIArray<sql::ObQualifiedName> &columns,
                                           ObIArray<ObRawExpr*> &real_exprs,
                                           ObPLCompileUnitAST &unit_ast,
                                           ObRawExpr *&expr);
  int resolve_name(sql::ObQualifiedName &q_name,
                   const ObPLBlockNS &ns,
                   ObRawExprFactory &expr_factory,
                   const ObSQLSessionInfo *session_info,
                   common::ObIArray<ObObjAccessIdx> &access_idxs,
                   ObPLCompileUnitAST &func);
  int convert_pltype_to_restype(ObIAllocator &alloc,
                                              const ObPLDataType &pl_type,
                                              ObExprResType *&result_type);
  int resolve_access_ident(ObObjAccessIdent &access_ident, const ObPLBlockNS &ns,
                           ObRawExprFactory &expr_factory, const ObSQLSessionInfo *session_info,
                           ObIArray<ObObjAccessIdx> &access_idxs, ObPLCompileUnitAST &func,
                           bool is_proc = false, bool is_resolve_rowtype = false);
  int resolve_into(const ParseNode *into_node, ObPLInto &into, ObPLFunctionAST &func);
  int resolve_using(const ObStmtNodeTree *using_node,  ObIArray<InOutParam> &using_params,
                    ObPLFunctionAST &func);
  int resolve_package_stmt_list(const ObStmtNodeTree *node, ObPLStmtBlock *&block, ObPLPackageAST &package);
  int resolve_stmt_list(const ObStmtNodeTree *parse_tree, ObPLStmtBlock *&stmt, ObPLFunctionAST &func,
                        bool stop_search_label = false, bool in_handler_scope = false);
  int resolve_when(const ObStmtNodeTree *parse_tree, ObRawExpr *case_expr_var, ObPLCaseStmt *stmt, ObPLFunctionAST &func);
  int resolve_then(const ObStmtNodeTree *parse_tree,
                   ObPLFunctionAST &func,
                   ObPLDataType *data_type,
                   ObRawExpr *&expr,
                   ObPLStmtBlock *&then_block,
                   bool is_add_bool_expr = false);
  int resolve_loop_control(const ObStmtNodeTree *parse_tree, ObPLLoopControl *stmt, bool is_iterate_label, ObPLFunctionAST &func);
  int resolve_goto(const ObStmtNodeTree *parse_tree,
                   ObPLGotoStmt *stmt,
                   ObPLCompileUnitAST &unit_ast);
  int check_goto(ObPLGotoStmt *stmt);
  int resolve_handler_condition(const ObStmtNodeTree *parse_tree,
                                ObPLConditionValue &condition,
                                ObPLCompileUnitAST &func);
  int resolve_pre_condition(const ObString &name, const ObPLConditionValue **value);
  int resolve_condition_value(const ObStmtNodeTree *parse_tree, ObPLConditionValue &value,
                              const bool is_sys_db);
  int resolve_condition(const ObStmtNodeTree *parse_tree,
                        const ObPLBlockNS &ns,
                        const ObPLConditionValue **value,
                        ObPLCompileUnitAST &func);
  int resolve_condition(const common::ObString &name,
                        const ObPLBlockNS &ns,
                        const ObPLConditionValue **value);
  int resolve_cursor(ObPLCompileUnitAST &func,
                     const ObPLBlockNS &ns,
                     const ObString &db_name,
                     const ObString &package_name,
                     const ObString &cursor_name,
                     int64_t &index);
  int resolve_cursor(const ObStmtNodeTree *parse_tree,
                     const ObPLBlockNS &ns,
                     int64_t &index,
                     ObPLCompileUnitAST &func);
  int resolve_cursor(const common::ObString &name,
                     const ObPLBlockNS &ns,
                     int64_t &cursor,
                     ObPLCompileUnitAST &func,
                     bool check_mode = false,
                     bool for_external_cursor = false);
  int resolve_questionmark_cursor(const int64_t symbol_idx, 
                                  ObPLBlockNS &ns, 
                                  int64_t &cursor);
  int resolve_label(const common::ObString &name,
                    const ObPLBlockNS &ns,
                    int64_t &label,
                    bool is_iterate_label);
  int resolve_cond_loop(const ObStmtNodeTree *expr_node, const ObStmtNodeTree *body_node, ObPLCondLoop *stmt, ObPLFunctionAST &func);
  int resolve_cursor_common(const ObStmtNodeTree *name_node,
                                          const ObStmtNodeTree *type_node,
                                          ObPLCompileUnitAST &func,
                                          ObString &name,
                                          ObPLDataType &return_type);
  int resolve_cursor_formal_param(const ObStmtNodeTree *param_list,
                                            ObPLCompileUnitAST &func,
                                            ObIArray<int64_t> &params);
  int resolve_cursor_def(const ObString &cursor_name,
                          const ObStmtNodeTree *sql_node,
                          ObPLBlockNS &sql_ns,
                          ObPLDataType &cursor_type,
                          const ObIArray<int64_t> &formal_params,
                          ObPLCompileUnitAST &func,
                          int64_t &cursor_index);
  int resolve_cparams_expr(const ParseNode *param_node,
                           ObPLFunctionAST &func,
                           ObIArray<ObRawExpr*> &exprs);
  int resolve_static_sql(const ObStmtNodeTree *parse_tree, ObPLSql &static_sql, ObPLInto &static_into, bool is_cursor, ObPLFunctionAST &func);

  int resolve_cparams(ObIArray<ObRawExpr*> &exprs,
                      const common::ObIArray<share::schema::ObIRoutineParam*> &params_list,
                      ObPLStmt *stmt,
                      ObPLFunctionAST &func);
  int resolve_cparam_with_assign(ObRawExpr *expr,
                      const common::ObIArray<share::schema::ObIRoutineParam*> &params_list,
                      ObPLFunctionAST &func,
                      ObIArray<ObRawExpr*> &params,
                      ObIArray<int64_t> &expr_idx);
  int resolve_cparam_without_assign(ObRawExpr *expr,
                      const int64_t position,
                      ObPLFunctionAST &func,
                      ObIArray<ObRawExpr*> &params,
                      ObIArray<int64_t> &expr_idx);
  int check_in_param_type_legal(const share::schema::ObIRoutineParam *param_info, const ObRawExpr* param);
  int resolve_inout_param(ObRawExpr *param_expr, ObPLRoutineParamMode param_mode, int64_t &out_idx);
  int resolve_sequence_object(
    const sql::ObQualifiedName &q_name, ObPLCompileUnitAST &unit_ast, ObRawExpr *&real_ref_expr);

  int resolve_udf_pragma(const ObStmtNodeTree *parse_tree, ObPLFunctionAST &ast);
  int resolve_serially_reusable_pragma(const ObStmtNodeTree *parse_tree, ObPLPackageAST &ast);
  int resolve_restrict_references_pragma(const ObStmtNodeTree *parse_tree, ObPLPackageAST &ast);
  int resolve_interface_pragma(const ObStmtNodeTree *parse_tree, ObPLPackageAST &ast);
  int check_collection_expr_illegal(const ObRawExpr *expr, bool &is_obj_acc);

  int resolve_forall_bound_clause(const ObStmtNodeTree &parse_tree,
                                  ObPLForAllStmt *stmt,
                                  ObPLFunctionAST &func,
                                  ObPLDataType *expected_type);
  int resolve_normal_bound_clause(const ObStmtNodeTree &bound_node,
                                  ObPLForAllStmt *stmt,
                                  ObPLFunctionAST &func,
                                  ObPLDataType *expected_type);
  int resolve_values_bound_clause(const ObStmtNodeTree &values_of_node,
                                  ObPLForAllStmt *stmt,
                                  ObPLFunctionAST &func,
                                  ObPLDataType *expected_type);
  int resolve_indices_bound_clause(const ObStmtNodeTree &indices_of_node,
                                   ObPLForAllStmt *stmt,
                                   ObPLFunctionAST &func,
                                   ObPLDataType *expected_type);
  int resolve_forall_collection_node(const ObStmtNodeTree *collection_node,
                                   ObPLForAllStmt *stmt,
                                   ObPLFunctionAST &func,
                                   ObPLDataType *expected_type);
  int build_collection_property_expr(const ObString &property_name,
                                   ObIArray<ObObjAccessIdx> &access_idxs,
                                   ObPLFunctionAST &func,
                                   ObPLForAllStmt *stmt,
                                   ObPLDataType *expected_type);
  int build_forall_index_expr(ObPLForAllStmt *stmt,
                                   ObRawExprFactory &expr_factory,
                                   ObRawExpr *&expr);
  int build_collection_value_expr(ObIArray<ObObjAccessIdx> &access_idxs,
                                   ObPLFunctionAST &func,
                                   ObPLForAllStmt *stmt,
                                   ObPLDataType *expected_type);
  int resolve_forall_collection_and_check(const ObStmtNodeTree *coll_node,
                                   ObPLForAllStmt *stmt,
                                   ObPLFunctionAST &func,
                                   ObIArray<ObObjAccessIdx> &access_idxs);
private:
  int check_duplicate_condition(const ObPLDeclareHandlerStmt &stmt, const ObPLConditionValue &value,
                                bool &dup, ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc* cur_desc);
  int analyze_actual_condition_type(const ObPLConditionValue &value, ObPLConditionType &type);
#ifdef OB_BUILD_ORACLE_PL
  int check_collection_constructor(const ParseNode *node, const common::ObString &type_name, bool &is_constructor);
#endif
  int check_subprogram_variable_read_only(ObPLBlockNS &ns, uint64_t subprogram_id, int64_t var_idx);
  int check_package_variable_read_only(uint64_t package_id, uint64_t var_idx);
  int check_local_variable_read_only(
    const ObPLBlockNS &ns, uint64_t var_idx, bool is_for_inout_param = false);
  int get_const_expr_value(const ObRawExpr *expr, uint64_t &val);
  int check_variable_accessible(const ObPLBlockNS &ns,
                                const ObIArray<ObObjAccessIdx>& access_idxs,
                                bool for_write);
  int check_variable_accessible(ObRawExpr *expr, bool for_write);
  int get_subprogram_var(
    ObPLBlockNS &ns, uint64_t subprogram_id, int64_t var_idx, const ObPLVar *&var);
  static
  int make_var_from_access(const ObIArray<ObObjAccessIdx> &access_idxs,
                           ObRawExprFactory &expr_factory,
                           const sql::ObSQLSessionInfo *session_info,
                           ObSchemaGetterGuard *schema_guard,
                           const ObPLBlockNS &ns,
                           ObRawExpr *&expr,
                           bool for_write = false);
  int make_block(ObPLFunctionAST &func,
                 const ObPLStmtBlock *parent,
                 ObPLStmtBlock *&block,
                 bool explicit_block = false);
  int make_block(ObPLPackageAST &package, ObPLStmtBlock *&block);
  void set_current(ObPLStmtBlock &block)
  {
    current_block_ = &block;
    resolve_ctx_.params_.secondary_namespace_ = &block.get_namespace();
  }
  int check_and_record_stmt_type(ObPLFunctionAST &func, sql::ObSPIService::ObSPIPrepareResult &prepare_result);
  int check_declare_order(ObPLStmtType type);
  bool is_data_type_name(const ObString &ident_name);
  bool is_sqlstate_completion(const char *sql_state);
  bool is_sqlstate_valid(const char* sql_state, const int64_t &str_len);
  int resolve_obj_access_idents(const ParseNode &node,
                                common::ObIArray<ObObjAccessIdent> &obj_access_idents,
                                ObPLCompileUnitAST &func);
  int resolve_dblink_idents(const ParseNode &node,
                            common::ObIArray<ObObjAccessIdent> &obj_access_idents,
                            ObPLCompileUnitAST &func,
                            common::ObIArray<ObObjAccessIdx> &access_idexs);
  int build_collection_attribute_access(ObRawExprFactory &expr_factory,
                                        const ObSQLSessionInfo *session_info,
                                        const ObPLBlockNS &ns,
                                        ObPLCompileUnitAST &func,
                                        const ObUserDefinedType &user_type,
                                        ObIArray<ObObjAccessIdx> &access_idxs,
                                        int64_t attr_index,
                                        ObRawExpr *func_expr,
                                        ObObjAccessIdx &access_idx);
  int build_return_access(ObObjAccessIdent &access_ident,
                                            ObIArray<ObObjAccessIdx> &access_idxs,
                                            ObPLCompileUnitAST &func);
  int build_collection_access(ObObjAccessIdent &access_ident,
                                          ObIArray<ObObjAccessIdx> &access_idxs,
                                          ObPLCompileUnitAST &func,
                                          int64_t start_level = 0);
  int build_raw_expr(const ParseNode &node,
                     ObRawExpr *&expr,
                     common::ObIArray<sql::ObQualifiedName> &columns,
                     common::ObIArray<sql::ObVarInfo> &sys_vars,
                     common::ObIArray<sql::ObAggFunRawExpr*> &aggr_exprs,
                     common::ObIArray<sql::ObWinFunRawExpr*> &win_exprs,
                     common::ObIArray<sql::ObSubQueryInfo> &sub_query_info,
                     common::ObIArray<sql::ObUDFInfo> &udf_info,
                     common::ObIArray<sql::ObOpRawExpr*> &op_exprs,
                     bool is_prepare_protocol/*= false*/);
  int check_expr_result_type(const ObRawExpr *expr, ObPLIntegerType &pl_integer_type, bool &is_anonymos_arg);
  bool is_need_add_checker(const ObPLIntegerType &type, const ObRawExpr *expr);
  int add_pl_integer_checker_expr(ObRawExprFactory &expr_factory,
                                  const ObPLIntegerType &type,
                                  int32_t lower,
                                  int32_t upper,
                                  ObRawExpr *&expr);
  int add_pl_integer_checker_expr(ObRawExprFactory &expr_factory, ObRawExpr *&expr, bool &need_replace);

  int check_use_idx_illegal(ObRawExpr* expr, int64_t idx);

  int check_raw_expr_in_forall(ObRawExpr* expr,
                               int64_t idx,
                               bool &need_modify,
                               bool &can_array_binding);
  int modify_raw_expr_in_forall(ObPLForAllStmt &stmt,
                                ObPLFunctionAST &func,
                                ObPLSqlStmt &sql_stmt,
                                ObIArray<int64_t>& modify_exprs);
  int modify_raw_expr_in_forall(ObPLForAllStmt &stmt,
                                ObPLFunctionAST &func,
                                ObPLSqlStmt &sql_stmt,
                                int64_t modify_expr_id,
                                int64_t table_idx);
  int check_forall_sql_and_modify_params(ObPLForAllStmt &stmt,
                                ObPLFunctionAST &func);
  int replace_record_member_default_expr(ObRawExpr *&expr);
  int check_param_default_expr_legal(ObRawExpr *expr, bool is_subprogram_expr = true);
  int check_params_legal_in_body_routine(ObPLFunctionAST &routine_ast,
                                         const ObPLRoutineInfo *parent_routine_info,
                                         const ObPLRoutineInfo *body_routine_info);
  int check_expr_can_pre_calc(ObRawExpr *expr, bool &pre_calc);
  int transform_subquery_expr(const ParseNode *node,
                              ObRawExpr *&expr,
                              const ObPLDataType *expected_type,
                              ObPLCompileUnitAST &func);
  int try_transform_assign_to_dynamic_SQL(ObPLStmt *&old_stmt, ObPLFunctionAST &func);
  int transform_var_val_to_dynamic_SQL(int64_t sql_expr_index, int64_t into_expr_index, ObPLFunctionAST &func);
  int transform_to_new_assign_stmt(ObIArray<int64_t> &transform_array, ObPLAssignStmt *&old_stmt);

  int replace_to_const_expr_if_need(ObRawExpr *&expr);
  int build_seq_value_expr(ObRawExpr *&expr,
                           const sql::ObQualifiedName &q_name,
                           uint64_t seq_id);
  int calc_subtype_range_bound(const ObStmtNodeTree *bound_node,
                               ObPLCompileUnitAST &unit_ast,
                               int32_t &bound);
  int resolve_external_types_from_expr(ObRawExpr &expr);
  int add_external_cursor(ObPLBlockNS &ns,
                          const ObPLBlockNS *external_ns,
                          const ObPLCursor &cursor,
                          int64_t &index,
                          ObPLCompileUnitAST &func);
  int fill_schema_obj_version(share::schema::ObSchemaGetterGuard &schema_guard,
                              ObParamExternType type,
                              uint64_t obj_id,
                              ObPLExternTypeInfo &extern_type_info);
  int check_is_udt_routine(const ObObjAccessIdent &access_ident,
                           const ObPLBlockNS &ns,
                           ObIArray<ObObjAccessIdx> &access_idxs,
                           bool &is_routine);
  static int get_number_literal_value(ObRawExpr *expr, int64_t &result);
  int get_const_number_variable_literal_value(ObRawExpr *expr, int64_t &result);
  int check_assign_type(const ObPLDataType &dest_data_type, const ObRawExpr *right_expr);
  int is_return_ref_cursor_type(const ObRawExpr *expr, bool &is_ref_cursor_type);
  int replace_map_or_order_expr(uint64_t udt_id,
                                ObRawExpr *&expr,
                                ObPLCompileUnitAST &unit_ast);
  int replace_object_compare_expr(ObRawExpr *&relation_expr,
                                  ObPLCompileUnitAST &unit_ast);

  static const ObRawExpr *skip_implict_cast(const ObRawExpr *e);

  int check_subprogram(ObPLFunctionAST &func);

  int check_undeclared_var_type(ObQualifiedName &q_name);
  int check_var_type(ObString &name, uint64_t db_id, ObSchemaType &schema_type,
                     const ObSimpleTableSchemaV2 *&schema, bool find_synonym);
  int check_var_type(ObString &name1, ObString &name2, uint64_t db_id);
  inline void reset_item_type() { item_type_ = T_MAX; }
  inline void set_item_type(ObItemType item_type) { item_type_ = item_type; }

  int resolve_question_mark_node(const ObStmtNodeTree *into_node, ObRawExpr *&into_expr);

  int check_cursor_formal_params(const ObIArray<int64_t>& formal_params,
                                 ObPLCursor &cursor,
                                 bool &legal);
  int replace_cursor_formal_params(const ObIArray<int64_t> &src_formal_exprs,
                                   const ObIArray<int64_t> &dst_formal_exprs,
                                   uint64_t src_package_id,
                                   uint64_t dst_package_id,
                                   ObIArray<ObRawExpr *> &sql_params);
  int replace_cursor_formal_params(const ObIArray<int64_t> &src_formal_exprs,
                                   const ObIArray<int64_t> &dst_formal_exprs,
                                   uint64_t src_package_id,
                                   uint64_t dst_package_id,
                                   ObRawExpr &expr);
  int convert_cursor_actual_params(ObRawExpr *expr,
                                   ObPLDataType pl_data_type,
                                   ObPLFunctionAST &func,
                                   int64_t &idx);
  int check_update_column(const ObPLBlockNS &ns, const ObIArray<ObObjAccessIdx>& access_idxs);
  int get_udt_names(ObSchemaGetterGuard &schema_guard,
                    const uint64_t udt_id,
                    ObString &database_name,
                    ObString &udt_name);
  static int get_udt_database_name(ObSchemaGetterGuard &schema_guard,
                                   const uint64_t udt_id, ObString &db_name);
  static bool check_with_rowid(const ObString &routine_name,
                               bool is_for_trigger);
  static int recursive_replace_expr(ObRawExpr *expr,
                                    ObQualifiedName &qualified_name,
                                    ObRawExpr *real_expr);

  int replace_udf_param_expr(ObQualifiedName &q_name,
                             ObIArray<ObQualifiedName> &columns,
                             ObIArray<ObRawExpr*> &real_exprs);
  int get_names_by_access_ident(ObObjAccessIdent &access_ident,
                                ObIArray<ObObjAccessIdx> &access_idxs,
                                ObString &database_name,
                                ObString &package_name,
                                ObString &routine_name);
  int check_routine_callable(const ObPLBlockNS &ns,
                             ObIArray<ObObjAccessIdx> &access_idxs,
                             ObIArray<ObRawExpr*> &expr_params,
                             const ObIRoutineInfo &routine_info);
  int resolve_routine(ObObjAccessIdent &access_ident,
                      const ObPLBlockNS &ns,
                      ObIArray<ObObjAccessIdx> &access_idxs,
                      ObPLCompileUnitAST &func);

  int resolve_function(ObObjAccessIdent &access_ident,
                       ObIArray<ObObjAccessIdx> &access_idxs,
                       const ObIRoutineInfo *routine_info,
                       ObPLCompileUnitAST &func);

  int resolve_procedure(ObObjAccessIdent &access_ident,
                        ObIArray<ObObjAccessIdx> &access_idxs,
                        const ObIRoutineInfo *routine_info,
                        ObProcType routine_type);

  int resolve_construct(ObObjAccessIdent &access_ident,
                        const ObPLBlockNS &ns,
                        ObIArray<ObObjAccessIdx> &access_idxs,
                        uint64_t user_type_id,
                        ObPLCompileUnitAST &func);

  int resolve_self_element_access(ObObjAccessIdent &access_ident,
                                  const ObPLBlockNS &ns,
                                  ObIArray<ObObjAccessIdx> &access_idxs,
                                  ObPLCompileUnitAST &func);

  int build_self_access_idx(ObObjAccessIdx &self_access_idx, const ObPLBlockNS &ns);
  int build_current_access_idx(uint64_t parent_id,
                               ObObjAccessIdx &access_idx,
                               ObObjAccessIdent &access_ident,
                               const ObPLBlockNS &ns,
                               ObIArray<ObObjAccessIdx> &access_idxs,
                               ObPLCompileUnitAST &func);

  int build_collection_index_expr(ObObjAccessIdent &access_ident,
                                  ObObjAccessIdx &access_idx,
                                  const ObPLBlockNS &ns,
                                  ObIArray<ObObjAccessIdx> &access_idxs,
                                  const ObUserDefinedType &user_type,
                                  ObPLCompileUnitAST &func);

  int build_access_idx_sys_func(uint64_t parent_id, ObObjAccessIdx &access_idx);

  int resolve_composite_access(ObObjAccessIdent &access_ident,
                               ObIArray<ObObjAccessIdx> &access_idxs,
                               const ObPLBlockNS &ns,
                               ObPLCompileUnitAST &func);

  int init_udf_info_of_accessident(ObObjAccessIdent &access_ident);
  int init_udf_info_of_accessidents(ObIArray<ObObjAccessIdent> &access_ident);

  int resolve_sys_func_access(ObObjAccessIdent &access_ident,
                              ObIArray<ObObjAccessIdx> &access_idxs,
                              const ObSQLSessionInfo *session_info,
                              const ObPLBlockNS &ns);

private:
  ObPLResolveCtx resolve_ctx_;
  ObPLExternalNS external_ns_;
  sql::ObRawExprFactory expr_factory_;
  ObPLStmtFactory stmt_factory_;
  ObPLStmtBlock *current_block_;
  int64_t current_level_; // 当前的block实际层数
  HandlerAnalyzer handler_analyzer_;
  uint64_t arg_cnt_; // 对于匿名快用于表示总的question mark个数,对于routine用于表示入参个数
  uint64_t question_mark_cnt_; // 表示解析到当前语句时question_mark_cnt_数目(不包含当前语句)
  uint64_t next_user_defined_exception_id_; // 用户定义的ExceptionID, 从1开始递增
  sql::ObSequenceNamespaceChecker ob_sequence_ns_checker_; // check if an sequence is defined.
  ObArray<int64_t> current_subprogram_path_; // 当前解析到的subprogram的寻址路径
  ObArray<ObPLStmt *> goto_stmts_; // goto语句的索引，用来二次解析。
  ObItemType item_type_;
};

class ObPLSwitchDatabaseGuard
{
public:
  ObPLSwitchDatabaseGuard(sql::ObSQLSessionInfo &session_info,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          ObPLCompileUnitAST &func,
                          int &ret,
                          bool with_rowid);
  virtual ~ObPLSwitchDatabaseGuard();
private:
  int &ret_;
  sql::ObSQLSessionInfo &session_info_;
  uint64_t database_id_;
  bool need_reset_;
  ObSqlString database_name_;
};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_RESOLVER_H_ */
