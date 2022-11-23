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

#ifndef OCEANBASE_SQL_OB_SQL_
#define OCEANBASE_SQL_OB_SQL_

#include "lib/string/ob_string.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/prepare/ob_execute_stmt.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/monitor/ob_monitor_info_manager.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/executor/ob_maintain_dependency_info_task.h"

namespace test
{
class TestOptimizerUtils;
}

namespace oceanbase
{

namespace share
{
class ObRsMgr;
} // end of namespace share
namespace share
{
namespace schema
{
class ObMaxConcurrentParam;
class ObOutlineInfo;
}
}
namespace common
{
class ObOptStatManager;
}
namespace sql
{
struct ObStmtPrepareResult;
class ObIVirtualTableIteratorFactory;
struct ObSqlCtx;
class ObResultSet;

class ObPlanBaseKeyGuard
{
public:
  explicit ObPlanBaseKeyGuard(ObBaselineKey &bl_key)
    : bl_key_(bl_key),
      ori_bl_key_(bl_key) {}
  ~ObPlanBaseKeyGuard()
  {
    bl_key_ = ori_bl_key_;
  }
private:
  // disallow copy
  ObPlanBaseKeyGuard(const ObPlanBaseKeyGuard &other);
private:
  ObBaselineKey &bl_key_;
  ObBaselineKey ori_bl_key_;
};

// this class is the main interface for sql module
class ObSql
{

  static const int64_t max_error_length;
  static const int64_t SQL_MEM_SIZE_LIMIT;
public:
  /// init SQL module
  int init(common::ObOptStatManager *opt_stat_mgr,
           rpc::frame::ObReqTransport *transport,
           common::ObITabletScan *vt_partition_service,
           common::ObAddr &addr,
           share::ObRsMgr &rs_mgr);
  void destroy();
public:
  /// print statatistics of SQL module
  void stat();

public:
  //These interface are one one mapping whit MySQL request type currently

  // Interface of SQL Engine for COM_QUERY request
  //
  // Handle all COM_QUERY request from mysqlclient
  // There are two kinds of COM_QUERY request
  // 1. general query such as "select * from table_test"
  // 2. text prepare such as "prepare stmt from select * from table_test"
  //
  // @param stmt[in]       input sql string
  // @param context[in]    sql context
  // @param result[out]
  //
  // @return oceanbase error code defined in lib/ob_errno.def
  virtual int stmt_query(const common::ObString &stmt,
                         ObSqlCtx &context,
                         ObResultSet &result);
  int handle_remote_query(const ObRemoteSqlInfo &remote_sql_info,
                          ObSqlCtx &context,
                          ObExecContext &exec_ctx,
                          ObCacheObjGuard& guard);
  virtual int stmt_prepare(const common::ObString &stmt,
                           ObSqlCtx &context,
                           ObResultSet &result,
                           bool is_inner_sql = true); //判断该次调用是外部请求prepare, 还是内部sql或者pl内sql

  /** Interface of SQL Engine for COM_
   * @param stmt_id [in]
   * @param params  [in]
   * @param context [in]
   * @param result [out]
   */
  virtual int stmt_execute(const ObPsStmtId stmt_id,
                           const stmt::StmtType stmt_type,
                           const ParamStore &params,
                           ObSqlCtx &context,
                           ObResultSet &result,
                           bool is_inner_sql);

  // Interface of SQL Engine for COM_FIELD_LIST request
  //
  // @brief: list fields of the specific table with some condition
  //
  // @param table_name [in]
  // @param wild_str[in]
  // @param context [in]
  // @param result [out]
  //
  // @return oceanbase error code defined in share/ob_errno.h
  int stmt_list_field(const common::ObString &table_name,
                      const common::ObString &wild_str,
                      ObSqlCtx &context,
                      ObResultSet &result);
  // Get plan cache by tenant id
  // if not exist, create a new plan cacha object
  //
  // @param tenant_id [in]
  //
  // @return Return ObPlanCache if get/create success, else return NULL
  ObPlanCache* get_plan_cache(uint64_t tenant_id, const ObPCMemPctConf &pc_mem_conf);
  ObPsCache* get_ps_cache(const uint64_t tenant_id, const ObPCMemPctConf &pc_mem_conf);
  int get_plan_retry(ObPlanCache &plan_cache, ObPlanCacheCtx &pc_ctx, ObCacheObjGuard &guard);

  int revert_plan_cache(uint64_t tenant_id);

  ObPlanCacheManager* get_plan_cache_manager() {return &plan_cache_manager_;}

  int64_t get_execution_id() { return ATOMIC_AAF(&execution_id_, 1); }
  int64_t get_px_sequence_id() { return ATOMIC_AAF(&px_sequence_id_, 1); }
  //calculate calculable expr
  // stmt 全量 const folding.
  static int calc_pre_calculable_exprs(ObExecContext &exec_ctx,
                                       ObDMLStmt &stmt,
                                       ObPhysicalPlan &phy_plan);

  // 为了改写层 const folding 抽出来的函数
  static int calc_pre_calculable_exprs(common::ObIArray<ObHiddenColumnItem> &calculable_exprs,
                                       ObExecContext &exec_ctx,
                                       ObDMLStmt &stmt,
                                       ObPhysicalPlan &phy_plan);

  static int calc_pre_calculable_exprs(const ObDMLStmt &stmt,
                            const common::ObIArray<ObHiddenColumnItem> &calculable_exprs,
                            const bool is_ignore_stmt,
                            ObExecContext &exec_ctx,
                            ObPhysicalPlan &phy_plan,
                            const uint64_t calc_types = PRE_CALC_DEFAULT);

  // create calculable expr constraints
  static int create_expr_constraints(ObQueryCtx &query_ctx, ObExecContext &exec_ctx);
  static int create_expr_constraint(ObQueryCtx &query_ctx,
                                    ObExecContext &exec_ctx,
                                    const ObIArray<ObHiddenColumnItem> &pre_calc_exprs,
                                    const PreCalcExprExpectResult expect_result);
  //simplyfy and rewrite stmt
  static int transform_stmt(ObSqlSchemaGuard *sql_schema_guard,
                            common::ObOptStatManager *opt_stat_mgr,
                            common::ObAddr *self_addr,
                            ObPhysicalPlan *phy_plan,
                            ObExecContext &exec_ctx,
                            ObDMLStmt *&stmt);

  //optimize stmt, generate logical_plan
  static int optimize_stmt(ObOptimizer &optimizer,
                           const ObSQLSessionInfo &session_info,
                           ObDMLStmt &stmt,
                           ObLogPlan *&logical_plan);

  const common::ObAddr &get_addr() const  {return self_addr_; }

  int init_result_set(ObSqlCtx &context, ObResultSet &result_set);

public:
  //for sql test only
  friend bool compare_stmt(const char *schema_file,  const char *str1, const char *str2);
  int fill_result_set(const ObPsStmtId stmt_id,
                      const ObPsStmtInfo &stmt_info,
                      ObResultSet &result);

public:
  ObSql()
  : inited_(false),
    opt_stat_mgr_(NULL),
    transport_(NULL),
    vt_partition_service_(NULL),
    rs_mgr_(NULL),
    execution_id_(0),
    px_sequence_id_(0)
  {}

  virtual ~ObSql() { destroy(); }
  static int construct_ps_param(const ParamStore &params,
                                ObPlanCacheCtx &phy_ctx);

private:
  // disallow copy
  ObSql(const ObSql &other);
  ObSql &operator=(const ObSql &other);

private:
  int add_param_to_param_store(const ObObjParam &param,
                               ParamStore &param_store);
  int construct_param_store(const ParamStore &params,
                            ParamStore &param_store);
  int construct_ps_param_store(const ParamStore &params,
                               const ParamStore &fixed_params,
                               const ObIArray<int64_t> &fixed_param_idx,
                               ParamStore &param_store);
  int construct_param_store_from_ps_param(const ObPlanCacheCtx &phy_ctx,
                                          ParamStore &param_store);
  bool is_exist_in_fixed_param_idx(const int64_t idx,
                                   const ObIArray<int64_t> &fixed_param_idx);
  int do_real_prepare(const ObString &stmt,
                      ObSqlCtx &context,
                      ObResultSet &result,
                      bool is_inner_sql);
  /**
   * @brief  把prepare的信息加入ps cache
   * @param [in] sql      - prepare的语句
   * @param [in] stmt_type  - stmt类型
   * @param [inout] result      - prepare的结果
   * @param [out] ps_stmt_info  - 加入ps cache的结构
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   *
   * 两个add_to_ps_cache接口输入参数不同，一个使用parser的结果，供mysql模式使用；
   * 一个使用resolver的结果，供oracle模式使用
   * 其主要内容是获取prepare语句的id和type，并构造出ObPsStmtInfo，加入ps cache
   */
   int do_add_ps_cache(const ObString &sql,
                       const ObString &no_param_sql,
                       const ObIArray<ObPCParam*> &raw_params,
                       const common::ObIArray<int64_t> &fixed_param_idx,
                       int64_t param_cnt,
                       share::schema::ObSchemaGetterGuard &schema_guard,
                       stmt::StmtType stmt_type,
                       ObResultSet &result,
                       bool is_inner_sql,
                       bool is_sensitive_sql,
                       int32_t returning_into_parm_num);

  int fill_result_set(ObResultSet &result, ObSqlCtx *context, const bool is_ps_mode, ObStmt &stmt);
  int fill_select_result_set(ObResultSet &result_set, ObSqlCtx *context, const bool is_ps_mode,
                             ObCollationType collation_type, const ObString &type_name,
                             ObStmt &basic_stmt, ObField &field);
  int handle_ps_prepare(const common::ObString &stmt,
                        ObSqlCtx &context,
                        ObResultSet &result,
                        bool is_inner_sql);

  int handle_ps_execute(const ObPsStmtId stmt_id,
                        const stmt::StmtType stmt_type,
                        const ParamStore &params,
                        ObSqlCtx &context,
                        ObResultSet &result,
                        bool is_inner_sql);

  int init_execute_params_for_ab(ObIAllocator &allocator,
                                 const ParamStore &params_store,
                                 ParamStore *&params);

  int reconstruct_ps_params_store(ObIAllocator &allocator,
                                  ObSqlCtx &context,
                                  const ParamStore &params,
                                  const ParamStore &fixed_params,
                                  ObPsStmtInfo *ps_info,
                                  ParamStore &ps_param_store,
                                  ParamStore *&ps_ab_params);

  int handle_text_query(const common::ObString &stmt,
                        ObSqlCtx &context,
                        ObResultSet &result);
  int handle_physical_plan(const common::ObString &trimed_stmt,
                           ObSqlCtx &context,
                           ObResultSet &result,
                           ObPlanCacheCtx &pc_ctx,
                           const int get_plan_err,
                           bool is_psmode = false);
  // @brief  Generate 'stmt' from syntax tree
  // @param parse_result[in]     syntax tree
  // @param select_item_param_infos           select_item_param_infos from fast parser
  // @param context[in]          sql context
  // @param allocator[in]        memory allocator
  // @param result_plan[out]
  // @param result[out]        - the id of the explain plan text column
  // @retval OB_SUCCESS execute success
  // @retval OB_SOME_ERROR special errno need to handle
  int generate_stmt(ParseResult &parse_result,
                    ObPlanCacheCtx *pc_ctx,
                    ObSqlCtx &context,
                    ObIAllocator &allocator,
                    ObResultSet &result,
                    ObStmt *&basic_stmt,
                    ParseResult *outline_parse_result = NULL);

  // Generate Physical Plan for given syntax tree
  // called by handle_text_query
  // && stmt_prepare && stmt_execute when necessary
  //
  // @param parse_result[in]   syntax tree
  // @param select_item_param_infos[in]      select_item_param_infos from fast parser
  // @param context[in]        sql context
  // @param allocator[in]      memory allocator
  // @param result[out]        result set include physical plan generated
  int generate_physical_plan(ParseResult &parse_result,
                             ObPlanCacheCtx *pc_ctx,
                             ObSqlCtx &context,
                             ObResultSet &result,
                             const bool is_begin_commit_stmt,
                             const bool is_ps_mode = false,
                             ParseResult *outline_parse_result = NULL);

  //generate physical_plan
  static int code_generate(ObSqlCtx &sql_ctx,
                           ObResultSet &result,
                           ObDMLStmt *stmt,
                           share::schema::ObStmtNeedPrivs &stmt_need_privs,
                           share::schema::ObStmtOraNeedPrivs &stmt_ora_need_privs,
                           common::ObIArray<ObAuditUnit> &audit_units,
                           ObLogPlan *logical_plan,
                           ObPhysicalPlan *&phy_plan);

  int sanity_check(ObSqlCtx &context);

  int init_exec_context(const ObSqlCtx &context, ObExecContext &exec_ctx);

  int get_outline_data(ObSqlCtx &context,
                       ObPlanCacheCtx &pc_ctx,
                       const ObString &signature_sql,
                       ObOutlineState &outline_state,
                       ParseResult &outline_parse_result);

  int get_outline_data(ObPlanCacheCtx &pc_ctx,
                       const ObString &signature_sql,
                       ObOutlineState &outline_state,
                       ObString &outline_content);

  int handle_large_query(int tmp_ret,
                         ObResultSet &result,
                         bool &need_disconnect,
                         ObExecContext &exec_ctx);
  int pc_get_plan_and_fill_result(ObPlanCacheCtx &pc_ctx,
                                  ObResultSet &result_set,
                                  int &get_plan_err,
                                  bool &need_disconnect);
  int pc_get_plan(ObPlanCacheCtx &pc_ctx,
                  ObCacheObjGuard& guard,
                  int &get_plan_err,
                  bool &need_disconnect);

  int parser_and_check(const ObString &outlined_stmt,
                       ObExecContext &exec_ctx,
                       ObPlanCacheCtx &pc_ctx,
                       ParseResult &parse_result,
                       int get_plan_err,
                       bool &add_plan_to_pc,
                       bool &is_enable_transform_tree);
  int handle_parser(const ObString &outlined_stmt,
                       ObExecContext &exec_ctx,
                       ObPlanCacheCtx &pc_ctx,
                       ParseResult &parse_result,
                       int get_plan_err,
                       bool &add_plan_to_pc,
                       bool &is_enable_transform_tree);

  int check_batched_multi_stmt_after_parser(ObPlanCacheCtx &pc_ctx,
                                            ParseResult &parse_result,
                                            bool add_plan_to_pc,
                                            bool &is_valid);
  int check_batched_multi_stmt_after_resolver(ObPlanCacheCtx &pc_ctx,
                                              const ObStmt &stmt,
                                              bool &is_valid);

  int replace_const_expr(common::ObIArray<ObRawExpr*> &raw_exprs,
                         ParamStore &param_store);
  int replace_const_expr(ObRawExpr *raw_expr,
                         ParamStore &param_store);
  void generate_ps_sql_id(const ObString &raw_sql,
                          ObSqlCtx &context);
  void generate_sql_id(ObPlanCacheCtx &pc_ctx,
                           bool add_plan_to_pc,
                           ParseResult &parse_result,
                           ObString &signature_sql,
                           int err_code);
  int pc_add_plan(ObPlanCacheCtx &pc_ctx,
                  ObResultSet &result,
                  ObOutlineState &outline_state,
                  ObPlanCache *plan_cache,
                  bool& plan_added);
  //检查经过参数化的模板SQL能否被prepare
  void check_template_sql_can_be_prepare(ObPlanCacheCtx &pc_ctx, ObPhysicalPlan &plan);
  int execute_get_plan(ObPlanCache &plan_cache,
                       ObPlanCacheCtx &pc_ctx,
                       ObCacheObjGuard& guard);
  int after_get_plan(ObPlanCacheCtx &pc_ctx,
                     ObSQLSessionInfo &session,
                     ObPhysicalPlan *plan,
                     bool from_plan_cache,
                     const ParamStore *ps_params);
  int need_add_plan(const ObPlanCacheCtx &ctx,
                    ObResultSet &result,
                    bool is_enable_pc,
                    bool &need_add_plan);

  template<typename ProcessorT>
  int handle_remote_batch_req(const ObReqTimestamp &req_ts, const char* buf, int32_t size);

  int clac_fixed_param_store(const stmt::StmtType stmt_type,
                             const common::ObIArray<int64_t> &raw_params_idx,
                             const common::ObIArray<ObPCParam *> &raw_params,
                             ObIAllocator &allocator,
                             ObSQLSessionInfo &session,
                             ParamStore &fixed_param_store);

  int handle_text_execute(const ObStmt *basic_stmt, ObSqlCtx &sql_ctx, ObResultSet &result);
  int check_need_reroute(ObPlanCacheCtx &pc_ctx, ObPhysicalPlan *plan, bool &need_reroute);
  int get_first_batched_multi_stmt(ObMultiStmtItem& multi_stmt_item, ObString& sql);

  typedef hash::ObHashMap<uint64_t, ObPlanCache*> PlanCacheMap;
  friend class ::test::TestOptimizerUtils;
private:
  bool inited_;
  // BEGIN 全局单例依赖接口
  common::ObOptStatManager *opt_stat_mgr_;
  rpc::frame::ObReqTransport *transport_;
  common::ObITabletScan *vt_partition_service_;
  // END 全局单例依赖接口

  ObPlanCacheManager plan_cache_manager_;
  common::ObAddr self_addr_;
  share::ObRsMgr *rs_mgr_;
  volatile int64_t execution_id_ CACHE_ALIGNED;
  volatile uint64_t px_sequence_id_;
  ObMaintainDepInfoTaskQueue queue_;
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_OB_SQL_
