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
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/prepare/ob_execute_stmt.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/monitor/ob_monitor_info_manager.h"

namespace test {
class TestOptimizerUtils;
}

namespace oceanbase {

namespace share {
class ObRsMgr;
}  // end of namespace share
namespace share {
namespace schema {
class ObMaxConcurrentParam;
class ObOutlineInfo;
}  // namespace schema
}  // namespace share
namespace common {
class ObOptStatManager;
}
namespace sql {
struct ObStmtPrepareResult;
class ObIVirtualTableIteratorFactory;
class ObSqlCtx;
class ObResultSet;

// this class is the main interface for sql module
class ObSql {

  static const int64_t max_error_length;
  static const int64_t SQL_MEM_SIZE_LIMIT;

public:
  /// init SQL module
  int init(common::ObStatManager* stat_mgr, common::ObOptStatManager* opt_stat_mgr,
      rpc::frame::ObReqTransport* transport, storage::ObPartitionService* partition_service,
      common::ObIDataAccessService* vt_partition_service, share::ObIPartitionLocationCache* partition_loc_cache,
      common::ObAddr& addr, share::ObRsMgr& rs_mgr);
  void destroy();

public:
  /// print statatistics of SQL module
  void stat();

public:
  // These interface are one one mapping whit MySQL request type currently

  // Interface of SQL Engine for OB_MYSQL_COM_QUERY request
  //
  // Handle all OB_MYSQL_COM_QUERY request from mysqlclient
  // There are two kinds of OB_MYSQL_COM_QUERY request
  // 1. general query such as "select * from table_test"
  // 2. text prepare such as "prepare stmt from select * from table_test"
  //
  // @param stmt[in]       input sql string
  // @param context[in]    sql context
  // @param result[out]
  //
  // @return oceanbase error code defined in lib/ob_errno.def
  virtual int stmt_query(const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result);
  int handle_remote_query(const ObRemoteSqlInfo& remote_sql_info, ObSqlCtx& context, ObExecContext& exec_ctx,
      ObPhysicalPlan*& plan, CacheRefHandleID& ref_handle_id);
  int handle_batch_req(const int type, const ObReqTimestamp& req_ts, const char* buf, int32_t size);
  virtual int stmt_prepare(
      const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql = true);
  // is_inner_sql: Determine whether the call is an external request preparation,
  // or internal sql or pl internal sql

  /** Interface of SQL Engine for COM_
   * @param stmt_id [in]
   * @param params  [in]
   * @param context [in]
   * @param result [out]
   */
  virtual int stmt_execute(const ObPsStmtId stmt_id, const stmt::StmtType stmt_type, const ParamStore& params,
      ObSqlCtx& context, ObResultSet& result, bool is_inner_sql);

  // Interface of SQL Engine for OB_MYSQL_COM_FIELD_LIST request
  //
  // @brief: list fields of the specific table with some condition
  //
  // @param table_name [in]
  // @param wild_str[in]
  // @param context [in]
  // @param result [out]
  //
  // @return oceanbase error code defined in share/ob_errno.h
  int stmt_list_field(
      const common::ObString& table_name, const common::ObString& wild_str, ObSqlCtx& context, ObResultSet& result);
  // Get plan cache by tenant id
  // if not exist, create a new plan cacha object
  //
  // @param tenant_id [in]
  //
  // @return Return ObPlanCache if get/create success, else return NULL
  ObPlanCache* get_plan_cache(uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf);
  ObPsCache* get_ps_cache(const uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf);

  int revert_plan_cache(uint64_t tenant_id);

  ObPlanCacheManager* get_plan_cache_manager()
  {
    return &plan_cache_manager_;
  }

  int64_t get_execution_id()
  {
    return ATOMIC_AAF(&execution_id_, 1);
  }
  int64_t get_px_sequence_id()
  {
    return ATOMIC_AAF(&px_sequence_id_, 1);
  }
  // calculate calculable expr
  static int calc_pre_calculable_exprs(ObExecContext& exec_ctx, ObDMLStmt& stmt, ObPhysicalPlan& phy_plan);

  static int calc_pre_calculable_exprs(common::ObIArray<ObHiddenColumnItem>& calculable_exprs, ObExecContext& exec_ctx,
      ObDMLStmt& stmt, ObPhysicalPlan& phy_plan);

  static int calc_pre_calculable_exprs(const ObDMLStmt& stmt,
      const common::ObIArray<ObHiddenColumnItem>& calculable_exprs, const bool is_ignore_stmt, ObExecContext& exec_ctx,
      ObPhysicalPlan& phy_plan);

  // simplyfy and rewrite stmt
  static int transform_stmt(ObSqlSchemaGuard* sql_schema_guard,
      share::ObIPartitionLocationCache* partition_location_cache, storage::ObPartitionService* partition_service,
      common::ObStatManager* stat_mgr, common::ObOptStatManager* opt_stat_mgr, common::ObAddr* self_addr,
      int64_t merged_version, ObPhysicalPlan* phy_plan, ObExecContext& exec_ctx, ObDMLStmt*& stmt);

  // optimize stmt, generate logical_plan
  static int optimize_stmt(
      ObOptimizer& optimizer, const ObSQLSessionInfo& session_info, ObDMLStmt& stmt, ObLogPlan*& logical_plan);

  static int analyze_table_stat_version(
      share::schema::ObSchemaGetterGuard* schema_guard, common::ObOptStatManager* opt_stat_manager, ObDMLStmt& stmt);

  const common::ObAddr& get_addr() const
  {
    return self_addr_;
  }

  int init_result_set(ObSqlCtx& context, ObResultSet& result_set);

public:
  // for sql test only
  friend bool compare_stmt(const char* schema_file, const char* str1, const char* str2);
  int fill_result_set(const ObPsStmtId stmt_id, const ObPsStmtInfo& stmt_info, ObResultSet& result);

public:
  ObSql()
      : inited_(false),
        stat_mgr_(NULL),
        opt_stat_mgr_(NULL),
        transport_(NULL),
        partition_service_(NULL),
        vt_partition_service_(NULL),
        rs_mgr_(NULL),
        execution_id_(0),
        px_sequence_id_(0)
  {}

  virtual ~ObSql()
  {
    destroy();
  }

  static int construct_not_paramalize(
      const ObIArray<int64_t>& offsets, const ParamStore& param_store, ObPlanCacheCtx& plan_ctx);
  static int construct_no_check_type_params(const ObIArray<int64_t>& offsets, ParamStore& param_store);
  static int construct_ps_param(const ParamStore& params, ObPlanCacheCtx& phy_ctx);

  storage::ObPartitionService* get_partition_service() const
  {
    return partition_service_;
  }

private:
  // disallow copy
  ObSql(const ObSql& other);
  ObSql& operator=(const ObSql& other);

private:
  int construct_param_store(const ParamStore& params, ParamStore& param_store);
  int do_real_prepare(const ObString& stmt, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql);

  int do_add_ps_cache(const ObString& sql, int64_t param_cnt, share::schema::ObSchemaGetterGuard& schema_guard,
      stmt::StmtType stmt_type, ObResultSet& result, bool is_inner_sql);

  int fill_result_set(ObResultSet& result, ObSqlCtx* context, const bool is_ps_mode, ObStmt& stmt);

  int handle_ps_prepare(const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result, bool is_inner_sql);

  int handle_ps_execute(const ObPsStmtId stmt_id, const stmt::StmtType stmt_type, const ParamStore& params,
      ObSqlCtx& context, ObResultSet& result, bool is_inner_sql);

  int handle_text_query(const common::ObString& stmt, ObSqlCtx& context, ObResultSet& result);
  int handle_physical_plan(const common::ObString& trimed_stmt, ObSqlCtx& context, ObResultSet& result,
      ObPlanCacheCtx& pc_ctx, const int get_plan_err);
  // @brief  Generate 'stmt' from syntax tree
  // @param parse_result[in]     syntax tree
  // @param select_item_param_infos           select_item_param_infos from fast parser
  // @param context[in]          sql context
  // @param allocator[in]        memory allocator
  // @param result_plan[out]
  // @param result[out]        - the id of the explain plan text column
  // @retval OB_SUCCESS execute success
  // @retval OB_SOME_ERROR special errno need to handle
  int generate_stmt(ParseResult& parse_result, ObPlanCacheCtx* pc_ctx, ObSqlCtx& context, ObIAllocator& allocator,
      ObResultSet& result, ObStmt*& basic_stmt);

  // Generate Physical Plan for given syntax tree
  // called by handle_text_query
  // && stmt_prepare && stmt_execute when necessary
  //
  // @param parse_result[in]   syntax tree
  // @param select_item_param_infos[in]      select_item_param_infos from fast parser
  // @param context[in]        sql context
  // @param allocator[in]      memory allocator
  // @param result[out]        result set include physical plan generated
  int generate_physical_plan(ParseResult& parse_result, ObPlanCacheCtx* pc_ctx, ObSqlCtx& context, ObResultSet& result,
      const bool is_ps_mode = false);

  int init_baseline_info(const ObPlanCacheCtx& pc_ctx, ObPhysicalPlan* plan);

  int check_baseline(ObPhysicalPlan* plan, ObPlanCacheCtx& pc_ctx);

  // generate physical_plan
  static int code_generate(ObSqlCtx& sql_ctx, ObResultSet& result, ObDMLStmt* stmt,
      share::schema::ObStmtNeedPrivs& stmt_need_privs, share::schema::ObStmtOraNeedPrivs& stmt_ora_need_privs,
      ObLogPlan* logical_plan, ObPhysicalPlan*& phy_plan);

  int sanity_check(ObSqlCtx& context);

  int init_exec_context(const ObSqlCtx& context, ObExecutorRpcImpl& exec_rpc, obrpc::ObCommonRpcProxy& rs_proxy,
      obrpc::ObSrvRpcProxy& srv_proxy, const ObVirtualTableCtx& vt_ctx, ObExecContext& exec_ctx);

  //@brief replace and mark bool filter('1+1 and c1 = 1', '0 or c1 =1', 1+1 and 0 are considered
  // as bool filters) in stmt filters, filters include where conditions, join conditions and
  // having filters. This is used for distinguish true/false filter in plan.
  static int replace_stmt_bool_filter(ObExecContext& ctx, ObDMLStmt* stmt);

  //@brief replace bool filter in expr. Only search 'T_OP_AND' and 'T_OP_OR' expr type, and
  // replace const expr to bool(tinyint) value.
  static int replace_expr_bool_filter(ObSQLSessionInfo* session, ObRawExpr* expr, ParamStore& params);
  static int replace_joined_table_bool_filter(ObSQLSessionInfo* session, JoinedTable& joined_table, ParamStore& params);
  int transform_stmt_with_outline(ObPlanCacheCtx& pc_ctx, ObOutlineState& outline_state, common::ObString& rewrite_sql);
  int get_outline_info(ObPlanCacheCtx& pc_ctx, const ObString& outline_key,
      const share::schema::ObOutlineInfo*& outline_info, bool& outline_with_sql_id);
  int handle_large_query(int tmp_ret, ObResultSet& result, bool& need_disconnect, ObExecContext& exec_ctx);
  int handle_parallel_query(ObResultSet& result, bool& need_disconnect);
  int pc_get_plan_and_fill_result(
      ObPlanCacheCtx& pc_ctx, ObResultSet& result_set, int& get_plan_err, bool& need_disconnect);
  int pc_get_plan(ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan, int& get_plan_err, bool& need_disconnect);

  // Add a hint to the original sql, the hint comes from the outline or spm
  int rewrite_query_sql(ObPlanCacheCtx& pc_ctx, ObOutlineState& outline_state, common::ObString& rewrite_sql);

  int parser_and_check(const ObString& outlined_stmt, ObExecContext& exec_ctx, ObPlanCacheCtx& pc_ctx,
      ParseResult& parse_result, int get_plan_err, bool& add_plan_to_pc, bool& is_enable_transform_tree);
  int handle_parser(const ObString& outlined_stmt, ObExecContext& exec_ctx, ObPlanCacheCtx& pc_ctx,
      ParseResult& parse_result, int get_plan_err, bool& add_plan_to_pc, bool& is_enable_transform_tree);

  int check_batched_multi_stmt_after_parser(
      ObPlanCacheCtx& pc_ctx, ParseResult& parse_result, bool add_plan_to_pc, bool& is_valid);
  int check_batched_multi_stmt_after_resolver(ObPlanCacheCtx& pc_ctx, const ObStmt& stmt, bool& is_valid);

  int replace_const_expr(common::ObIArray<ObRawExpr*>& raw_exprs, ParamStore& param_store);
  int replace_const_expr(ObRawExpr* raw_expr, ParamStore& param_store);
  int generate_sql_id(ObPlanCacheCtx& pc_ctx, bool add_plan_to_pc);
  int pc_add_plan(ObPlanCacheCtx& pc_ctx, ObResultSet& result, ObOutlineState& outline_state, ObPlanCache* plan_cache);
  // Check whether the parameterized template SQL can be prepared
  void check_template_sql_can_be_prepare(ObPlanCacheCtx& pc_ctx, ObPhysicalPlan& plan);
  int execute_get_plan(ObPlanCache& plan_cache, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan);
  int after_get_plan(ObPlanCacheCtx& pc_ctx, ObSQLSessionInfo& session, ObPhysicalPlan* plan, bool from_plan_cache,
      const ParamStore* ps_params);
  int need_check_baseline(const ObPlanCacheCtx& pc_ctx, const ObPhysicalPlan& plan, bool& need_check);
  int need_add_plan(const ObPlanCacheCtx& ctx, ObResultSet& result, bool is_enable_pc, bool& need_add_plan);

  template <typename ProcessorT>
  int handle_remote_batch_req(const ObReqTimestamp& req_ts, const char* buf, int32_t size);

  typedef hash::ObHashMap<uint64_t, ObPlanCache*> PlanCacheMap;
  friend class ::test::TestOptimizerUtils;

private:
  bool inited_;
  // BEGIN Global singleton dependent interface
  common::ObStatManager* stat_mgr_;
  common::ObOptStatManager* opt_stat_mgr_;
  rpc::frame::ObReqTransport* transport_;
  storage::ObPartitionService* partition_service_;
  common::ObIDataAccessService* vt_partition_service_;
  // END Global singleton dependent interface

  ObPlanCacheManager plan_cache_manager_;
  common::ObAddr self_addr_;
  share::ObRsMgr* rs_mgr_;
  volatile int64_t execution_id_;
  volatile uint64_t px_sequence_id_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_OB_SQL_
