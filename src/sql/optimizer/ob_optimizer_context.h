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

#ifndef _OB_OPTIMIZER_CONTEXT_H
#define _OB_OPTIMIZER_CONTEXT_H 1
#include "share/schema/ob_schema_getter_guard.h"
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_fd_item.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
namespace sql {
//  class ObLogicalOperator;
class ObRawExprFactory;
class ObLogPlanFactory;
enum PXParallelRule { USE_PX_DEFAULT, MANUAL_HINT, SESSION_FORCE_PARALLEL, MANUAL_TABLE_DOP, MAX_OPTION };
class ObOptimizerContext {

public:
  ObOptimizerContext(ObSQLSessionInfo* session_info, ObExecContext* exec_ctx, ObSqlSchemaGuard* sql_schema_guard,
      common::ObStatManager* stat_manager, common::ObOptStatManager* opt_stat_manager,
      storage::ObPartitionService* partition_service, common::ObIAllocator& allocator,
      share::ObIPartitionLocationCache* location_cache, const ParamStore* params, common::ObAddr& addr,
      obrpc::ObSrvRpcProxy* srv_proxy, int64_t merged_version, ObQueryHint& query_hint, ObRawExprFactory& expr_factory,
      ObDMLStmt* root_stmt)
      : session_info_(session_info),
        exec_ctx_(exec_ctx),
        sql_schema_guard_(sql_schema_guard),
        stat_manager_(stat_manager),
        // TODO: modify this
        opt_stat_manager_(opt_stat_manager),
        partition_service_(partition_service),
        allocator_(allocator),
        table_location_list_(),        // declared as auto free
        table_partition_info_list_(),  // declared as auto free
        location_cache_(location_cache),
        server_(addr),
        srv_proxy_(srv_proxy),  // for `estimate storage` only
        params_(params),
        merged_version_(merged_version),
        query_hint_(query_hint),
        expr_factory_(expr_factory),
        log_plan_factory_(allocator),
        use_pdml_(false),
        parallel_(1),
        fd_item_factory_(allocator),
        enable_batch_opt_(-1),
        force_default_stat_(false),
        eval_plan_cost_(false),
        root_stmt_(root_stmt),
        px_parallel_rule_(PXParallelRule::USE_PX_DEFAULT)
  {}
  // TODO: get and set opt_stat_manager_  for test
  inline common::ObOptStatManager* get_opt_stat_manager()
  {
    return opt_stat_manager_;
  }
  inline void set_opt_stat_manager(common::ObOptStatManager* sm)
  {
    opt_stat_manager_ = sm;
  }

  virtual ~ObOptimizerContext()
  {}
  inline const ObSQLSessionInfo* get_session_info() const
  {
    return session_info_;
  }
  inline ObSQLSessionInfo* get_session_info()
  {
    return session_info_;
  }
  inline ObExecContext* get_exec_ctx() const
  {
    return exec_ctx_;
  }
  inline ObTaskExecutorCtx* get_task_exec_ctx() const
  {
    ObTaskExecutorCtx* ctx = NULL;
    if (NULL != exec_ctx_) {
      ctx = exec_ctx_->get_task_executor_ctx();
    }
    return ctx;
  }
  inline share::schema::ObSchemaGetterGuard* get_schema_guard() const
  {
    return OB_NOT_NULL(sql_schema_guard_) ? sql_schema_guard_->get_schema_guard() : NULL;
  }
  inline ObSqlSchemaGuard* get_sql_schema_guard() const
  {
    return sql_schema_guard_;
  }
  inline common::ObStatManager* get_stat_manager() const
  {
    return stat_manager_;
  }
  inline storage::ObPartitionService* get_partition_service() const
  {
    return partition_service_;
  }
  inline common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }
  int get_table_location(const uint64_t table_id, const ObTableLocation*& table_location) const
  {
    // for test
    UNUSED(table_id);
    table_location = NULL;
    int64_t N = table_location_list_.count();
    bool found = false;
    for (int64_t i = 0; !found && i < N; i++) {
      if (table_location_list_.at(i).get_table_id() == table_id) {
        table_location = &(table_location_list_.at(i));
        found = true;
      }
    }
    return common::OB_SUCCESS;
  }
  const common::ObAddr& get_local_server_addr() const
  {
    return server_;
  }
  common::ObAddr& get_local_server_addr()
  {
    return server_;
  }
  void set_local_server_ipv4_addr(const char* ip, const int32_t port)
  {
    server_.set_ip_addr(ip, port);
  }
  const obrpc::ObSrvRpcProxy* get_srv_proxy() const
  {
    return srv_proxy_;
  }
  obrpc::ObSrvRpcProxy* get_srv_proxy()
  {
    return srv_proxy_;
  }
  void set_srv_proxy(obrpc::ObSrvRpcProxy* srv_proxy)
  {
    srv_proxy_ = srv_proxy;
  }
  common::ObIArray<ObTableLocation>& get_table_location_list()
  {
    return table_location_list_;
  }
  common::ObIArray<ObTablePartitionInfo*>& get_table_partition_info_list()
  {
    return table_partition_info_list_;
  }
  share::ObIPartitionLocationCache* get_location_cache() const
  {
    return location_cache_;
  }
  inline const ParamStore* get_params()
  {
    return params_;
  }
  void set_merged_version(int64_t merged_version)
  {
    merged_version_ = merged_version;
  }
  inline int64_t get_merged_version()
  {
    return merged_version_;
  }
  inline const ObQueryHint& get_query_hint()
  {
    return query_hint_;
  }
  inline void set_query_hint(ObQueryHint& hint)
  {
    query_hint_ = hint;
  }
  inline ObRawExprFactory& get_expr_factory()
  {
    return expr_factory_;
  }
  inline ObLogPlanFactory& get_log_plan_factory()
  {
    return log_plan_factory_;
  }
  inline bool use_pdml() const
  {
    return use_pdml_;
  }
  inline int64_t get_parallel() const
  {
    return parallel_;
  }
  inline ObFdItemFactory& get_fd_item_factory()
  {
    return fd_item_factory_;
  }
  void set_parallel(int64_t parallel)
  {
    parallel_ = parallel;
  }
  void set_use_pdml(bool u)
  {
    use_pdml_ = u;
  }
  inline bool is_batched_multi_stmt()
  {
    if (NULL != exec_ctx_ && NULL != exec_ctx_->get_sql_ctx()) {
      return exec_ctx_->get_sql_ctx()->multi_stmt_item_.is_batched_multi_stmt();
    } else {
      return false;
    }
  }
  void disable_batch_rpc()
  {
    enable_batch_opt_ = 0;
  }
  bool enable_batch_rpc()
  {
    if (-1 == enable_batch_opt_) {
      if (session_info_->is_enable_batched_multi_statement()) {
        enable_batch_opt_ = 1;
      } else {
        enable_batch_opt_ = 0;
      }
    }
    return 1 == enable_batch_opt_;
  }

  bool use_default_stat() const
  {
    return force_default_stat_;
  }
  void set_use_default_stat()
  {
    force_default_stat_ = true;
  }

  void set_cost_evaluation()
  {
    eval_plan_cost_ = true;
  }
  bool is_cost_evaluation()
  {
    return eval_plan_cost_;
  }

  inline ObDMLStmt* get_root_stmt()
  {
    return root_stmt_;
  }
  inline void set_root_stmt(ObDMLStmt* stmt)
  {
    root_stmt_ = stmt;
  }
  inline void set_parallel_rule(PXParallelRule rule)
  {
    px_parallel_rule_ = rule;
  }
  const PXParallelRule& get_parallel_rule() const
  {
    return px_parallel_rule_;
  }
  inline const char* ob_px_parallel_rule_str(PXParallelRule px_parallel_ruel)
  {
    const char* ret = "USE_PX_DEFAULT";
    static const char* parallel_rule_type_to_str[] = {

        "USE_PX_DEFAULT",
        "MANUAL_HINT",
        "SESSION_FORCE_PARALLEL",
        "MANUAL_TABLE_DOP",
        "MAX_OPTION",
    };
    if (OB_LIKELY(px_parallel_ruel >= USE_PX_DEFAULT) && OB_LIKELY(px_parallel_ruel <= MAX_OPTION)) {
      ret = parallel_rule_type_to_str[px_parallel_ruel];
    }
    return ret;
  }
  inline bool use_intra_parallel() const
  {
    return parallel_ > 1;
  }

private:
  ObSQLSessionInfo* session_info_;
  ObExecContext* exec_ctx_;
  ObSqlSchemaGuard* sql_schema_guard_;
  common::ObStatManager* stat_manager_;
  common::ObOptStatManager* opt_stat_manager_;
  storage::ObPartitionService* partition_service_;
  common::ObIAllocator& allocator_;
  common::ObArray<ObTableLocation, common::ModulePageAllocator, true> table_location_list_;
  common::ObSEArray<ObTablePartitionInfo*, 2, common::ModulePageAllocator, true> table_partition_info_list_;
  share::ObIPartitionLocationCache* location_cache_;
  common::ObAddr server_;
  obrpc::ObSrvRpcProxy* srv_proxy_;
  const ParamStore* params_;
  int64_t merged_version_;
  ObQueryHint query_hint_;
  ObRawExprFactory& expr_factory_;
  ObLogPlanFactory log_plan_factory_;
  bool use_pdml_;
  int64_t parallel_;
  ObFdItemFactory fd_item_factory_;
  int enable_batch_opt_;
  bool force_default_stat_;
  bool eval_plan_cost_;
  ObDMLStmt* root_stmt_;
  PXParallelRule px_parallel_rule_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // _OB_OPTIMIZER_CONTEXT_H
