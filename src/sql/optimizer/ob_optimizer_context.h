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
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/optimizer/ob_fd_item.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
#include "sql/optimizer/ob_dynamic_sampling.h"
#include "share/config/ob_config_helper.h"


namespace oceanbase
{
namespace common
{
class ObOptStatManager;
}
namespace sql
{
//  class ObLogicalOperator;
class ObRawExprFactory;
class ObLogPlanFactory;

typedef common::ObArray<common::ObString, common::ObIAllocator &> ObPlanNotes;
//table location local index id related info
//tablet_loc_id and ref_table_id_ are used to uniquely determine
//the location identifier of the main table involved in the plan,
//and are also the key information for building association relationships.
//related_ids_ are used to record the related table_id,
//at(0) is source_table_id, and the rest are related_table_id associated with source_table_id,
//for example:
//for the scenario where the local index returns to the table,
//the source_table_id is the selected local index table_id,
//and the related_table_id is the main table table_id.
//Because the initiator of the partition is the local index,
//the tablet_id of the main table is obtained synchronously according to the tablet_id of the local index.
//for the DML operator, source_table_id is the main table table_id,
//and related_table_id is the local index table_id that needs to be maintained synchronously
struct TableLocRelInfo
{
  TableLocRelInfo()
    : table_loc_id_(common::OB_INVALID_ID),
      ref_table_id_(common::OB_INVALID_ID),
      related_ids_(),
      table_part_infos_()
  { }
  int assign(const TableLocRelInfo &other)
  {
    int ret = common::OB_SUCCESS;
    table_loc_id_ = other.table_loc_id_;
    ref_table_id_ = other.ref_table_id_;
    ret = related_ids_.assign(other.related_ids_);
    if (OB_SUCC(ret)) {
      ret = table_part_infos_.assign(other.table_part_infos_);
    }
    return ret;
  }
  TO_STRING_KV(K_(table_loc_id),
               K_(ref_table_id),
               K_(related_ids));
  common::ObTableID table_loc_id_;
  common::ObTableID ref_table_id_; //the data table object id
  common::ObArray<common::ObTableID, common::ModulePageAllocator, true> related_ids_;
  common::ObArray<ObTablePartitionInfo*, common::ModulePageAllocator, true> table_part_infos_;
};

struct AutoDOPParams {
  AutoDOPParams()
    : parallel_degree_limit_(0),
      parallel_servers_target_(0),
      unit_min_cpu_(0),
      parallel_min_scan_time_threshold_(1000)
    { }

  int64_t get_parallel_degree_limit(const int64_t server_cnt) const {
    int64_t limit = 0;
    if (0 < parallel_degree_limit_) {
      limit = parallel_degree_limit_;
    } else if (0 >= parallel_servers_target_ || 0 >= unit_min_cpu_ || 0 >= server_cnt) {
      limit = std::max(parallel_servers_target_, server_cnt * unit_min_cpu_);
    } else {
      limit = std::min(parallel_servers_target_, server_cnt * unit_min_cpu_);
    }
    return std::max(limit, 1L);
  }
  bool is_valid() { return parallel_min_scan_time_threshold_ >= 10
                          && (parallel_degree_limit_ > 0 || parallel_servers_target_ > 0
                              || unit_min_cpu_ > 0); }
  TO_STRING_KV(K_(parallel_degree_limit),
               K_(parallel_servers_target),
               K_(unit_min_cpu),
               K_(parallel_min_scan_time_threshold));
  int64_t parallel_degree_limit_;
  int64_t parallel_servers_target_;
  int64_t unit_min_cpu_;
  int64_t parallel_min_scan_time_threshold_; // auto dop threshold for table scan cost
};

struct OptSystemStat {
  OptSystemStat()
  :last_analyzed_(0),
  cpu_speed_(0),
  disk_seq_read_speed_(0),
  disk_rnd_read_speed_(0),
  network_speed_(0)
  {
  }

  inline int64_t get_last_analyzed() const { return last_analyzed_; }
  void set_last_analyzed(int64_t last_analyzed) { last_analyzed_ = last_analyzed; }

  inline int64_t get_cpu_speed() const { return cpu_speed_; }
  void set_cpu_speed(int64_t cpu_speed) { cpu_speed_ = cpu_speed; }

  inline int64_t get_disk_seq_read_speed() const { return disk_seq_read_speed_; }
  void set_disk_seq_read_speed(int64_t disk_seq_read_speed) { disk_seq_read_speed_ = disk_seq_read_speed; }

  inline int64_t get_disk_rnd_read_speed() const { return disk_rnd_read_speed_; }
  void set_disk_rnd_read_speed(int64_t disk_rnd_read_speed) { disk_rnd_read_speed_ = disk_rnd_read_speed; }

  inline int64_t get_network_speed() const { return network_speed_; }
  void set_network_speed(int64_t network_speed) { network_speed_ = network_speed; }

  TO_STRING_KV(K(last_analyzed_),
               K(cpu_speed_),
               K(disk_seq_read_speed_),
               K(disk_rnd_read_speed_),
               K(network_speed_));

private:
  int64_t last_analyzed_;
  int64_t cpu_speed_;
  int64_t disk_seq_read_speed_;
  int64_t disk_rnd_read_speed_;
  int64_t network_speed_;
};

class ObOptimizerContext
{

public:
ObOptimizerContext(ObSQLSessionInfo *session_info,
                   ObExecContext *exec_ctx,
                   ObSqlSchemaGuard *sql_schema_guard,
                   common::ObOptStatManager* opt_stat_manager,
                   common::ObIAllocator &allocator,
                   const ParamStore *params,
                   const common::ObAddr &addr,
                   obrpc::ObSrvRpcProxy *srv_proxy,
                   const ObGlobalHint &global_hint,
                   ObRawExprFactory &expr_factory,
                   ObDMLStmt *root_stmt,
                   bool is_ps_protocol,
                   ObQueryCtx *query_ctx = NULL
                   )
  : session_info_(session_info),
    exec_ctx_(exec_ctx),
    sql_schema_guard_(sql_schema_guard),
    opt_stat_manager_(opt_stat_manager),
    allocator_(allocator),
    table_location_list_(), // declared as auto free
    server_(addr),
    srv_proxy_(srv_proxy), // for `estimate storage` only
    params_(params),
    global_hint_(global_hint),
    expr_factory_(expr_factory),
    log_plan_factory_(allocator),
    force_serial_set_order_(false),
    parallel_(ObGlobalHint::UNSET_PARALLEL),
    px_parallel_rule_(PXParallelRule::USE_PX_DEFAULT),
    can_use_pdml_(false),
    max_parallel_(ObGlobalHint::UNSET_PARALLEL),
    auto_dop_params_(),
    is_online_ddl_(false),
    ddl_sample_column_count_(0),
    is_heap_table_ddl_(false),
    is_pdml_heap_table_(false),
    fd_item_factory_(allocator),
    enable_batch_opt_(-1),
    force_default_stat_(false),
    eval_plan_cost_(false),
    runtime_filter_type_(-1),
    batch_size_(0),
    root_stmt_(root_stmt),
    enable_px_batch_rescan_(-1),
    column_usage_infos_(),
    temp_table_infos_(),
    exchange_allocated_(false),
    phy_plan_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
    location_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
    local_sharding_(ObTableLocationType::OB_TBL_LOCATION_LOCAL),
    distributed_sharding_(ObTableLocationType::OB_TBL_LOCATION_DISTRIBUTED),
    match_all_sharding_(ObTableLocationType::OB_TBL_LOCATION_ALL),
    is_packed_(false),
    is_ps_protocol_(is_ps_protocol),
    expected_worker_count_(0),
    minimal_worker_count_(0),
    expected_worker_map_(),
    minimal_worker_map_(),
    all_exprs_(false),
    model_type_(ObOptEstCost::VECTOR_MODEL),
    px_object_sample_rate_(-1),
    plan_notes_(512, allocator),
    aggregation_optimization_settings_(0),
    query_ctx_(query_ctx),
    nested_sql_flags_(0),
    has_no_skip_for_update_(false),
    has_var_assign_(false),
    is_var_assign_only_in_root_stmt_(false),
    failed_ds_tab_list_(),
    has_multiple_link_stmt_(false),
    hash_join_enabled_(true),
    optimizer_sortmerge_join_enabled_(true),
    nested_loop_join_enabled_(true),
    system_stat_(),
    storage_estimation_enabled_(false),
    das_keep_order_enabled_(true),
    generate_random_plan_(false)
  { }
  inline common::ObOptStatManager *get_opt_stat_manager() { return opt_stat_manager_; }
  inline void set_opt_stat_manager(common::ObOptStatManager *sm) { opt_stat_manager_ = sm; }

  virtual ~ObOptimizerContext()
  {
    expected_worker_map_.destroy();
    minimal_worker_map_.destroy();
    log_plan_factory_.destroy();
  }
  inline const ObSQLSessionInfo *get_session_info() const { return session_info_; }
  inline ObSQLSessionInfo *get_session_info() { return session_info_; }
  inline ObExecContext *get_exec_ctx() const { return exec_ctx_; }
  inline ObQueryCtx *get_query_ctx() const { return query_ctx_; }
  inline ObTaskExecutorCtx *get_task_exec_ctx() const {
    ObTaskExecutorCtx *ctx = NULL;
    if (NULL != exec_ctx_) {
      ctx = exec_ctx_->get_task_executor_ctx();
    }
    return ctx;
  }
  inline share::schema::ObSchemaGetterGuard *get_schema_guard() const
  { return OB_NOT_NULL(sql_schema_guard_) ? sql_schema_guard_->get_schema_guard() : NULL; }
  inline ObSqlSchemaGuard *get_sql_schema_guard() const
  { return sql_schema_guard_; }
  inline common::ObIAllocator &get_allocator() { return allocator_; }
  int get_table_location(const uint64_t table_id, const ObTableLocation *&table_location) const
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
  const common::ObAddr &get_local_server_addr() const { return server_;}
  common::ObAddr &get_local_server_addr() { return server_; }
  void set_local_server_addr(const char *ip, const int32_t port) { server_.set_ip_addr(ip, port); }
  obrpc::ObSrvRpcProxy* get_srv_proxy() { return srv_proxy_; }
  void set_srv_proxy(obrpc::ObSrvRpcProxy *srv_proxy) { srv_proxy_ = srv_proxy; }
  common::ObIArray<ObTablePartitionInfo *> & get_table_partition_info() { return table_partition_infos_; }
  ObTablePartitionInfo *get_table_part_info_by_id(uint64_t table_loc_id, uint64_t ref_table_id)
  {
    ObTablePartitionInfo *table_part = nullptr;
    for (int64_t i = 0; OB_ISNULL(table_part) && i < table_partition_infos_.count(); ++i) {
      const ObDASTableLocMeta &loc_meta = table_partition_infos_.at(i)->get_table_location().get_loc_meta();
      if (table_loc_id == loc_meta.table_loc_id_ && ref_table_id == loc_meta.ref_table_id_) {
        table_part = table_partition_infos_.at(i);
      }
    }
    return table_part;
  }
  common::ObIArray<ObTableLocation> &get_table_location_list() { return table_location_list_; }
  inline const ParamStore *get_params()
  {
    return params_;
  }
  inline const ObGlobalHint &get_global_hint() { return global_hint_; }
  inline ObRawExprFactory &get_expr_factory() { return expr_factory_; }
  inline ObLogPlanFactory &get_log_plan_factory() { return log_plan_factory_; }
  inline bool can_use_pdml() const { return can_use_pdml_; }
  inline bool is_online_ddl() const { return is_online_ddl_; }
  inline int64_t get_ddl_sample_column_count() const { return ddl_sample_column_count_; }
  inline bool is_heap_table_ddl() const { return is_heap_table_ddl_; }
  inline bool is_pdml_heap_table() const { return is_pdml_heap_table_; }
  inline bool force_serial_set_order() const { return force_serial_set_order_; }
  void set_serial_set_order(bool force_serial_set_order) { force_serial_set_order_ = force_serial_set_order; }

  inline bool is_storage_estimation_enabled() const { return storage_estimation_enabled_; }
  void set_storage_estimation_enabled(bool storage_estimation_enabled) { storage_estimation_enabled_ = storage_estimation_enabled; }
  inline bool is_das_keep_order_enabled() const { return das_keep_order_enabled_; }
  void set_das_keep_order_enabled(bool das_keep_order_enabled) { das_keep_order_enabled_ = das_keep_order_enabled; }
  inline int64_t get_parallel() const { return parallel_; }
  inline int64_t get_max_parallel() const { return max_parallel_; }
  inline int64_t get_parallel_degree_limit(const int64_t server_cnt) const { return auto_dop_params_.get_parallel_degree_limit(server_cnt); }
  inline int64_t get_session_parallel_degree_limit() const { return auto_dop_params_.parallel_degree_limit_; }
  inline int64_t get_parallel_min_scan_time_threshold() const { return auto_dop_params_.parallel_min_scan_time_threshold_; }
  inline bool force_disable_parallel() const  { return px_parallel_rule_ >= PL_UDF_DAS_FORCE_SERIALIZE
                                                        && px_parallel_rule_ < MAX_OPTION; }
  inline bool is_use_table_dop() const  { return MANUAL_TABLE_DOP == px_parallel_rule_; }
  inline bool is_use_auto_dop() const { return AUTO_DOP == px_parallel_rule_; }
  inline bool is_parallel_rule_valid() const { return MAX_OPTION != px_parallel_rule_; }
  void set_parallel(int64_t parallel) { parallel_ = parallel; }
  void set_max_parallel(int64_t max_parallel) { max_parallel_ = max_parallel_ < max_parallel ? max_parallel : max_parallel_; }
  void set_auto_dop_params(const AutoDOPParams &auto_dop_params) {  auto_dop_params_ = auto_dop_params; }
  const AutoDOPParams &get_auto_dop_params() {  return auto_dop_params_; }
  void set_can_use_pdml(bool u) { can_use_pdml_ = u; }
  inline ObFdItemFactory &get_fd_item_factory() { return fd_item_factory_; }
  void set_is_online_ddl(bool flag) { is_online_ddl_ = flag; }
  void set_ddl_sample_column_count(const int64_t count) { ddl_sample_column_count_ = count; }
  void set_is_heap_table_ddl(bool flag) { is_heap_table_ddl_ = flag; }
  void set_is_pdml_heap_table(bool flag) { is_pdml_heap_table_ = flag; }
  inline void set_parallel_rule(PXParallelRule rule) { px_parallel_rule_ = rule; }
  const PXParallelRule& get_parallel_rule() const { return px_parallel_rule_; }
  inline bool is_batched_multi_stmt()
  {
    bool bret = false;
    if (NULL != exec_ctx_ && NULL != exec_ctx_->get_sql_ctx()) {
      bret = exec_ctx_->get_sql_ctx()->is_batch_params_execute();
    }
    return bret;
  }
  inline bool is_do_insert_batch_opt()
  {
    bool bret = false;
    if (NULL != exec_ctx_ && NULL != exec_ctx_->get_sql_ctx()) {
      bret = exec_ctx_->get_sql_ctx()->is_do_insert_batch_opt();
    }
    return bret;
  }
  inline bool is_strict_defensive_check()
  {
    bool bret = false;
    if (NULL != exec_ctx_ && NULL != exec_ctx_->get_sql_ctx()) {
      bret = exec_ctx_->get_sql_ctx()->get_enable_strict_defensive_check();
    }
    return bret;
  }
  void disable_batch_rpc() { enable_batch_opt_ = 0; }
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

  double enable_px_batch_rescan()
  {
    if (-1 == enable_px_batch_rescan_) {
      omt::ObTenantConfigGuard tenant_config(
            TENANT_CONF(session_info_->get_effective_tenant_id()));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        enable_px_batch_rescan_ = 0;
      } else if (tenant_config->_enable_px_batch_rescan) {
        enable_px_batch_rescan_ = 1;
      } else {
        enable_px_batch_rescan_ = 0;
      }
    }
    return enable_px_batch_rescan_;
  }

  int get_px_object_sample_rate()
  {
    if (-1 == px_object_sample_rate_) {
      omt::ObTenantConfigGuard tenant_config(
            TENANT_CONF(session_info_->get_effective_tenant_id()));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        px_object_sample_rate_ = 10;
      } else {
        px_object_sample_rate_ = tenant_config->_px_object_sampling;
      }
    }
    return px_object_sample_rate_;
  }

  bool use_default_stat() const { return force_default_stat_;}
  void set_use_default_stat() { force_default_stat_ = true; }
  void set_cost_evaluation() { eval_plan_cost_ = true; }
  bool is_cost_evaluation() { return eval_plan_cost_; }
  bool enable_runtime_filter() {
    if (0 > runtime_filter_type_) {
      get_runtime_filter_type();
    }
    return 0 != runtime_filter_type_ && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0;
  }
  bool enable_bloom_filter() {
    if (0 > runtime_filter_type_) {
      get_runtime_filter_type();
    }
    return 0 != (runtime_filter_type_ & (1 << RuntimeFilterType::BLOOM_FILTER));
  }
  bool enable_range_filter() {
    if (0 > runtime_filter_type_) {
      get_runtime_filter_type();
    }
    return 0 != (runtime_filter_type_ & (1 << RuntimeFilterType::RANGE));
  }
  bool enable_in_filter() {
    if (0 > runtime_filter_type_) {
      get_runtime_filter_type();
    }
    return 0 != (runtime_filter_type_ & (1 << RuntimeFilterType::IN));
  }
  void get_runtime_filter_type() {
    runtime_filter_type_ = session_info_->get_runtime_filter_type();
  }

  int64_t get_batch_size() const { return batch_size_; }
  void set_batch_size(const int64_t v) { batch_size_ = v; }
  inline ObDMLStmt* get_root_stmt() { return root_stmt_; }
  inline void set_root_stmt(ObDMLStmt *stmt) { root_stmt_ = stmt; }
  inline ObShardingInfo *get_local_sharding() { return &local_sharding_; }
  inline ObShardingInfo *get_match_all_sharding() { return &match_all_sharding_; }
  inline ObShardingInfo *get_distributed_sharding() { return &distributed_sharding_; }
  inline void set_exchange_allocated(bool exchange_allocated) {
    exchange_allocated_ = exchange_allocated;
  }
  inline bool get_exchange_allocated()
  {
    return exchange_allocated_;
  }
  inline void set_phy_plan_type(ObPhyPlanType phy_plan_type)
  {
    phy_plan_type_ = phy_plan_type;
  }
  inline ObPhyPlanType get_phy_plan_type() const
  {
    return phy_plan_type_;
  }
  inline void set_location_type(ObPhyPlanType location_type)
  {
    location_type_ = location_type;
  }
  inline ObPhyPlanType get_location_type() const
  {
    return location_type_;
  }
  inline void set_plan_type(ObPhyPlanType phy_plan_type,
                            ObPhyPlanType location_type,
                            bool exchange_allocated)
  {
    exchange_allocated_ = exchange_allocated;
    if (phy_plan_type == ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED ||
        phy_plan_type == ObPhyPlanType::OB_PHY_PLAN_REMOTE) {
      phy_plan_type_ = phy_plan_type;
    } else {
      phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_LOCAL;
    }
    if (location_type == ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
      location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
    } else {
      location_type_ = phy_plan_type_;
    }
  }
  ObIArray<ColumnUsageArg> &get_column_usage_infos() { return column_usage_infos_; }
  int add_temp_table(ObSqlTempTableInfo *temp_table_info) {
    return temp_table_infos_.push_back(temp_table_info);
  }
  common::ObIArray<ObSqlTempTableInfo*> &get_temp_table_infos() { return temp_table_infos_; }
  ObLogicalOperator *get_temp_table_plan(const uint64_t temp_table_id) const
  {
    ObLogicalOperator *temp_table_insert = NULL;
    bool find = false;
    for (int64_t i = 0; !find && i < temp_table_infos_.count(); i++) {
      if (NULL != temp_table_infos_.at(i) &&
          temp_table_infos_.at(i)->temp_table_id_ == temp_table_id) {
        temp_table_insert = temp_table_infos_.at(i)->table_plan_;
        find = true;
      }
    }
    return temp_table_insert;
  }
  ObLogicalOperator *get_temp_table_plan(const ObSelectStmt *table_query) const
  {
    ObLogicalOperator *temp_table_insert = NULL;
    bool find = false;
    for (int64_t i = 0; !find && i < temp_table_infos_.count(); i++) {
      if (NULL != temp_table_infos_.at(i) &&
          table_query == temp_table_infos_.at(i)->table_query_) {
        temp_table_insert = temp_table_infos_.at(i)->table_plan_;
        find = true;
      }
    }
    return temp_table_insert;
  }
  bool is_local_or_remote_plan() const
  { return OB_PHY_PLAN_REMOTE == phy_plan_type_  ||
           OB_PHY_PLAN_LOCAL == phy_plan_type_; }
  inline void set_packed(const bool is_packed) { is_packed_ = is_packed; }
  inline bool is_packed() const { return is_packed_; }
  inline void set_ps_protocol(const bool is_ps_protocol) { is_ps_protocol_ = is_ps_protocol; }
  inline bool is_ps_protocol() const { return is_ps_protocol_; }
  void set_expected_worker_count(int64_t c) { expected_worker_count_ = c; }
  int64_t get_expected_worker_count() const { return expected_worker_count_; }
  void set_minimal_worker_count(int64_t c) { minimal_worker_count_ = c; }
  int64_t get_minimal_worker_count() const { return minimal_worker_count_; }

  common::hash::ObHashMap<ObAddr, int64_t>& get_expected_worker_map() { return expected_worker_map_; }
  common::hash::ObHashMap<ObAddr, int64_t>& get_minimal_worker_map() { return minimal_worker_map_; }
  const common::hash::ObHashMap<ObAddr, int64_t>& get_expected_worker_map() const { return expected_worker_map_; }
  const common::hash::ObHashMap<ObAddr, int64_t>& get_minimal_worker_map() const { return minimal_worker_map_; }
  const ObRawExprUniqueSet &get_all_exprs() const { return all_exprs_; };
  ObRawExprUniqueSet &get_all_exprs() { return all_exprs_; };
  inline void set_cost_model_type(ObOptEstCost::MODEL_TYPE type) { model_type_ = type; }
  ObOptEstCost::MODEL_TYPE get_cost_model_type() const { return model_type_; }
  const ObPlanNotes &get_plan_notes() const { return plan_notes_; }
  void add_plan_note(const char *fmt, ...)
  {
    char buf[256];
    int64_t buf_len = 256;
    va_list args;
    va_start(args, fmt);
    int64_t pos = vsnprintf(buf, buf_len, fmt, args);
    va_end(args);
    if (pos > 0) {
      common::ObString s(pos, buf);
      add_plan_note(s);
    }
  }
  void add_plan_note(const common::ObString &str)
  {
    common::ObString s;
    int ret = common::ob_write_string(allocator_, str, s);
    if (OB_SUCC(ret)) {
      (void)  plan_notes_.push_back(s);
    }
  }
  inline void set_aggregation_optimization_settings(uint64_t settings)
  { aggregation_optimization_settings_ = settings; }
  inline uint64_t get_aggregation_optimization_settings() const
  { return aggregation_optimization_settings_; }
  inline bool force_push_down() const { return FORCE_GPD & aggregation_optimization_settings_; }
  common::ObIArray<TableLocRelInfo> &get_loc_rel_infos() { return loc_rel_infos_; }
  TableLocRelInfo *get_loc_rel_info_by_id(uint64_t table_loc_id, uint64_t ref_table_id)
  {
    TableLocRelInfo *rel_info = nullptr;
    for (int64_t i = 0; OB_ISNULL(rel_info) && i < loc_rel_infos_.count(); ++i) {
      if (loc_rel_infos_.at(i).table_loc_id_ == table_loc_id
          && loc_rel_infos_.at(i).ref_table_id_ == ref_table_id) {
        rel_info = &(loc_rel_infos_.at(i));
      }
    }
    return rel_info;
  }

  void set_in_nested_sql(bool v) { in_nested_sql_ = v; }
  bool in_nested_sql() const { return in_nested_sql_; }
  void set_has_fk(bool v) { has_fk_ = v; }
  bool has_fk() const { return has_fk_; }
  void set_has_trigger(bool v) { has_trigger_ = v; }
  bool has_trigger() const { return has_trigger_; }
  void set_has_pl_udf(bool v) { has_pl_udf_ = v; }
  bool has_pl_udf() const { return has_pl_udf_; }
  void set_has_cursor_expression(bool v) { has_cursor_expression_ = v; }
  bool has_cursor_expression() const { return has_cursor_expression_; }
  void set_has_dblink(bool v) { has_dblink_ = v; }
  bool has_dblink() const { return has_dblink_; }
  void set_has_subquery_in_function_table(bool v) { has_subquery_in_function_table_ = v; }
  bool has_subquery_in_function_table() const { return has_subquery_in_function_table_; }
  bool contain_nested_sql() const { return nested_sql_flags_ > 0; }
  //use nested sql can't in online DDL session
  bool contain_user_nested_sql() const { return nested_sql_flags_ > 0 && !is_online_ddl_; }
  inline void set_no_skip_for_update() { has_no_skip_for_update_ = true; }
  inline bool has_no_skip_for_update() const { return has_no_skip_for_update_; }
  inline bool has_var_assign() { return has_var_assign_; }
  inline void set_has_var_assign(bool v) { has_var_assign_ = v; }
  inline bool is_var_assign_only_in_root_stmt() { return is_var_assign_only_in_root_stmt_; }
  inline void set_is_var_assign_only_in_root_stmt(bool v) { is_var_assign_only_in_root_stmt_ = v; }
  common::ObIArray<ObDSFailTabInfo> &get_failed_ds_tab_list() { return failed_ds_tab_list_; }
  inline bool has_multiple_link_stmt() const { return has_multiple_link_stmt_; }
  inline void set_has_multiple_link_stmt(bool v) { has_multiple_link_stmt_ = v; }
  inline bool is_hash_join_enabled() const { return hash_join_enabled_; }
  inline void set_hash_join_enabled(bool enabled) { hash_join_enabled_ = enabled; }
  inline bool is_merge_join_enabled() const { return optimizer_sortmerge_join_enabled_; }
  inline void set_merge_join_enabled(bool enabled) { optimizer_sortmerge_join_enabled_ = enabled; }
  inline bool is_nested_join_enabled() const { return nested_loop_join_enabled_; }
  inline void set_nested_join_enabled(bool enabled) { nested_loop_join_enabled_ = enabled; }
  inline OptSystemStat& get_system_stat() { return system_stat_; }
  inline const OptSystemStat& get_system_stat() const { return system_stat_; }
  inline bool generate_random_plan() const { return generate_random_plan_; }
  inline void set_generate_random_plan(bool rand_plan) { generate_random_plan_ = rand_plan; }

private:
  ObSQLSessionInfo *session_info_;
  ObExecContext *exec_ctx_;
  ObSqlSchemaGuard *sql_schema_guard_;
  common::ObOptStatManager *opt_stat_manager_;
  common::ObIAllocator &allocator_;
  common::ObArray<ObTableLocation, common::ModulePageAllocator, true> table_location_list_;
  common::ObSEArray<ObTablePartitionInfo *, 1, common::ModulePageAllocator, true> table_partition_infos_;
  common::ObAddr server_;
  obrpc::ObSrvRpcProxy *srv_proxy_;
  const ParamStore *params_;
  const ObGlobalHint &global_hint_;
  ObRawExprFactory &expr_factory_;
  ObLogPlanFactory log_plan_factory_;
  bool force_serial_set_order_; //to keep a serial execute for set query
  int64_t parallel_;
  // 决定计划并行度的规则
  PXParallelRule px_parallel_rule_;
  bool can_use_pdml_; // can use pdml after check parallel
  int64_t max_parallel_;
  AutoDOPParams auto_dop_params_; // parameters to calc dop for Auto DOP
  bool is_online_ddl_;
  int64_t ddl_sample_column_count_;
  bool is_heap_table_ddl_; // we need to treat heap table ddl seperately
  bool is_pdml_heap_table_; // for heap table, we need to force pdml to use random distribute
  ObFdItemFactory fd_item_factory_;
  int enable_batch_opt_;
  bool force_default_stat_;
  bool eval_plan_cost_;
  // for runtime filter
  int64_t runtime_filter_type_;
  // batch row count for vectorized execution, 0 for no vectorize.
  int64_t batch_size_;
  ObDMLStmt *root_stmt_;
  int enable_px_batch_rescan_;
  common::ObSEArray<ColumnUsageArg, 16, common::ModulePageAllocator, true> column_usage_infos_;
  common::ObSEArray<ObSqlTempTableInfo*, 1, common::ModulePageAllocator, true> temp_table_infos_;
  bool exchange_allocated_;
  ObPhyPlanType phy_plan_type_;
  ObPhyPlanType location_type_;
  ObShardingInfo local_sharding_;
  ObShardingInfo distributed_sharding_;
  ObShardingInfo match_all_sharding_;
  bool is_packed_;
  bool is_ps_protocol_;
  int64_t expected_worker_count_;
  int64_t minimal_worker_count_;
  common::hash::ObHashMap<ObAddr, int64_t> expected_worker_map_;
  common::hash::ObHashMap<ObAddr, int64_t> minimal_worker_map_;
  ObRawExprUniqueSet all_exprs_;
  ObOptEstCost::MODEL_TYPE model_type_;
  double px_object_sample_rate_;
  ObPlanNotes plan_notes_;
  uint64_t aggregation_optimization_settings_; // for adaptive groupby/distinct
  ObQueryCtx *query_ctx_;
  //to record the related info about data table and local index id,
  //because the locations of data table and local index table are always bound together
  common::ObSEArray<TableLocRelInfo, 1, common::ModulePageAllocator, true> loc_rel_infos_;
  union {
    //contain_nested_sql_: whether contain trigger, foreign key, PL UDF
    //or this sql is triggered by these object
    int8_t nested_sql_flags_;
    struct {
      int8_t in_nested_sql_                    : 1; //this sql in a nested sql
      int8_t has_fk_                           : 1; //this sql has foreign key object
      int8_t has_trigger_                      : 1; //this sql has trigger object
      int8_t has_pl_udf_                       : 1; //this sql has pl user defined function
      int8_t has_subquery_in_function_table_   : 1; //this stmt has function table
      int8_t has_dblink_                       : 1; //this stmt has dblink table
      int8_t has_cursor_expression_            : 1; //this sql has cursor expression
    };
  };
  bool has_no_skip_for_update_;
  bool has_var_assign_;
  bool is_var_assign_only_in_root_stmt_;
  //record the dynamic sampling falied table list, avoid repeated dynamic sampling.
  common::ObSEArray<ObDSFailTabInfo, 1, common::ModulePageAllocator, true> failed_ds_tab_list_;
  bool has_multiple_link_stmt_;
  bool hash_join_enabled_;
  bool optimizer_sortmerge_join_enabled_;
  bool nested_loop_join_enabled_;
  OptSystemStat system_stat_;
  bool storage_estimation_enabled_;
  bool das_keep_order_enabled_;

  bool generate_random_plan_;
};
}
}

#endif // _OB_OPTIMIZER_CONTEXT_H
