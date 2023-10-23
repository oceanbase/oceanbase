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

#ifndef OCEANBASE_SQL_OB_PHYSICAL_PLAN_H
#define OCEANBASE_SQL_OB_PHYSICAL_PLAN_H
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "lib/allocator/ob_mod_define.h"
#include "common/ob_field.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/executor/ob_executor.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/monitor/ob_phy_operator_stats.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/monitor/ob_plan_info_manager.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
  class ObSchemaGetterGuard;
}
}
namespace transaction
{
  class ObEncryptMetaCache;
}
namespace sql
{
class ObTablePartitionInfo;
class ObPhyOperatorMonnitorInfo;
struct ObAuditRecordData;
class ObOpSpec;
class ObEvolutionPlan;

//class ObPhysicalPlan: public common::ObDLinkBase<ObPhysicalPlan>
typedef common::ObFixedArray<common::ObFixedArray<int64_t, common::ObIAllocator>, common::ObIAllocator> PhyRowParamMap;
typedef common::ObFixedArray<ObTableLocation, common::ObIAllocator> TableLocationFixedArray;
typedef common::ObFixedArray<ObPlanPwjConstraint, common::ObIAllocator> PlanPwjConstraintArray;
typedef common::ObFixedArray<ObDupTabConstraint, common::ObIAllocator> DupTabReplicaArray;
typedef common::ObFixedArray<transaction::ObEncryptMetaCache, common::ObIAllocator> EncryptMetaCacheArray;

//2.2.5版本之后已废弃
struct FlashBackQueryItem {
  OB_UNIS_VERSION(1);
public:
  FlashBackQueryItem()
    : table_id_(common::OB_INVALID_ID),
      time_val_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
      time_expr_(nullptr),
      type_(FLASHBACK_QUERY_ITEM_INVALID)
  {
  }

  enum FlashBackQueryItemType
  {
    FLASHBACK_QUERY_ITEM_INVALID,
    FLASHBACK_QUERY_ITEM_TIMESTAMP,
    FLASHBACK_QUERY_ITEM_SCN
  };

  DECLARE_TO_STRING;

  int64_t table_id_;
  int64_t time_val_;
  ObSqlExpression *time_expr_;
  FlashBackQueryItemType type_;
};

class ObPhysicalPlan : public ObPlanCacheObject
{
public:
  static const int32_t SLOW_QUERY_TIME = 100000; //100ms
  static const int64_t SLOW_QUERY_TIME_FOR_PLAN_EXPIRE = 5000; // 5ms
  static const int64_t SLOW_QUERY_ROW_COUNT_THRESOLD = 5000;
  static const int64_t SLOW_QUERY_SAMPLE_SIZE = 20; // smaller than ObPlanStat::MAX_SCAN_STAT_SIZE
  static const int64_t TABLE_ROW_CHANGE_THRESHOLD = 2;
  static const int64_t EXPIRED_PLAN_TABLE_ROW_THRESHOLD = 100;
  OB_UNIS_VERSION(1);
public:
  explicit ObPhysicalPlan(lib::MemoryContext &mem_context = CURRENT_CONTEXT);
  virtual ~ObPhysicalPlan();

  virtual void destroy();
  virtual void reset();
  void set_is_last_exec_succ(bool val) { stat_.is_last_exec_succ_ = val; }
  bool is_last_exec_succ() const { return stat_.is_last_exec_succ_; }
  const ObString &get_constructed_sql() const { return stat_.constructed_sql_; }
  bool temp_sql_can_prepare() const { return temp_sql_can_prepare_; }
  void set_temp_sql_can_prepare() { temp_sql_can_prepare_ = true; }
  bool with_rows() const
  { return ObStmt::is_select_stmt(stmt_type_) || is_returning() || need_drive_dml_query_; }
  int copy_common_info(ObPhysicalPlan &src);
  int extract_query_range(ObExecContext &ctx) const;
  //user var
  bool is_contains_assignment() const {return is_contains_assignment_;}
  void set_contains_assignment(bool v) {is_contains_assignment_ = v;}
  int set_vars(const common::ObIArray<ObVarInfo> &vars);
  const common::ObIArray<ObVarInfo> &get_vars() const { return vars_; }
  /**
   *
   * @param[in] table_row_count_list  计划涉及到的表的行数的数组，仅用来根据表行数的
   *                                  变化来淘汰计划
   */
  void update_plan_stat(const ObAuditRecordData &record,
                        const bool is_first,
                        const bool is_evolution,
                        const ObIArray<ObTableRowCount> *table_row_count_list);
  void update_cache_access_stat(const ObTableScanStat &scan_stat)
  {
    stat_.update_cache_stat(scan_stat);
  }
  void reset_evolution_stat()
  {
    stat_.is_evolution_ = false;
    stat_.evolution_stat_.reset();
  }
  int64_t get_evo_perf() const;
  int64_t get_cpu_time() const { return stat_.evolution_stat_.cpu_time_; }
  int64_t get_elapsed_time() const { return stat_.evolution_stat_.elapsed_time_; }
  int64_t get_executions() const { return stat_.evolution_stat_.executions_; }
  void set_evolution(bool v) { stat_.is_evolution_ = v; }
  bool get_evolution() const { return stat_.is_evolution_; }
  inline bool check_if_is_expired(const int64_t first_exec_row_count,
                                  const int64_t current_row_count) const;

  bool is_plan_unstable(const int64_t sample_count,
                        const int64_t sample_exec_row_count,
                        const int64_t sample_exec_usec);
  bool is_expired() const { return stat_.is_expired_; }
  void set_is_expired(bool expired) { stat_.is_expired_ = expired; }
  void inc_large_querys();
  void inc_delayed_large_querys();
  void inc_delayed_px_querys();
  int update_operator_stat(ObPhyOperatorMonitorInfo &info);
  bool is_need_trans() const { return is_need_trans_; }
  bool is_stmt_modify_trans() const;
  //As there's ObString in phy_hint_,need deep copy
  int set_phy_plan_hint(const ObPhyPlanHint &hint) { return phy_hint_.deep_copy(hint, allocator_); }
  ObPhyPlanHint &get_phy_plan_hint() { return phy_hint_; }
  const ObPhyPlanHint &get_phy_plan_hint() const { return phy_hint_; }

  int alloc_op_spec(
    const ObPhyOperatorType type, const int64_t child_cnt, ObOpSpec *&op, const uint64_t op_id);

  void set_location_type(ObPhyPlanType type) { location_type_ = type; }
  bool has_uncertain_local_operator() const { return OB_PHY_PLAN_UNCERTAIN == location_type_; }
  inline ObPhyPlanType get_location_type() const;
  void set_plan_type(ObPhyPlanType type) { plan_type_ = type; }
  ObPhyPlanType get_plan_type() const { return plan_type_; }
  void set_require_local_execution(bool v) { require_local_execution_ = v; }
  bool is_require_local_execution() const { return require_local_execution_; }
  void set_use_px(bool use_px) { use_px_ = use_px; }
  bool is_use_px() const { return use_px_; }
  void set_px_dop(int64_t px_dop) { px_dop_ = px_dop; }
  int64_t get_px_dop() const { return px_dop_; }
  void set_expected_worker_count(int64_t c) { stat_.expected_worker_count_ = c; }
  int64_t get_expected_worker_count() const { return stat_.expected_worker_count_; }
  void set_minimal_worker_count(int64_t c) { stat_.minimal_worker_count_ = c; }
  int64_t get_minimal_worker_count() const { return stat_.minimal_worker_count_; }
  int set_expected_worker_map(const common::hash::ObHashMap<ObAddr, int64_t> &c);
  const ObPlanStat::AddrMap& get_expected_worker_map() const;
  int set_minimal_worker_map(const common::hash::ObHashMap<ObAddr, int64_t> &c);
  const common::hash::ObHashMap<ObAddr, int64_t>& get_minimal_worker_map() const;
  int assign_worker_map(ObPlanStat::AddrMap &worker_map,
                        const common::hash::ObHashMap<ObAddr, int64_t> &c);
  const char* get_sql_id() const { return stat_.sql_id_.ptr(); }
  const ObString& get_sql_id_string() const { return stat_.sql_id_; }
  uint32_t get_next_phy_operator_id() { return next_phy_operator_id_++; }
  uint32_t get_next_phy_operator_id() const { return next_phy_operator_id_; }
  void set_next_phy_operator_id(uint32_t next_operator_id)
  { next_phy_operator_id_ = next_operator_id; }
  uint32_t get_phy_operator_size() const
  {
    return next_phy_operator_id_;
  }
  void set_next_expr_operator_id(uint32_t next_expr_id)
  { next_expr_operator_id_ = next_expr_id; }
  uint32_t get_expr_operator_size() const
  {
    return next_expr_operator_id_;
  }
  int init_operator_stats();

  inline void set_param_count(int64_t param_count) { param_count_ = param_count; }
  inline int64_t get_param_count() const { return param_count_; }

  void set_is_update_uniq_index(bool is_update_uniq_index) { is_update_uniq_index_ = is_update_uniq_index; }
  bool get_is_update_uniq_index() const { return is_update_uniq_index_; }

  void set_for_update(bool is_sfu) { is_sfu_ = is_sfu_ || is_sfu; }
  bool has_for_update() const { return is_sfu_; }
  void set_signature(uint64_t sign) { signature_ = sign; }
  uint64_t get_signature() const { return signature_; }
  void set_plan_hash_value(uint64_t v) { stat_.plan_hash_value_ = v; }
  int32_t *alloc_projector(int64_t projector_size);
  int add_table_location(const ObPhyTableLocation &table_location);
  ObExprOperatorFactory &get_expr_op_factory() { return expr_op_factory_; }
  const ObExprOperatorFactory &get_expr_op_factory() const { return expr_op_factory_; }

  int set_field_columns(const common::ColumnsFieldArray &fields);

  inline const common::ColumnsFieldArray &get_field_columns() const
  {
    return field_columns_;
  }
  int set_param_fields(const common::ParamsFieldArray &fields);
  inline const common::ParamsFieldArray &get_param_fields() const
  {
    return param_columns_;
  }
  int set_returning_param_fields(const common::ParamsFieldArray &fields);
  inline const common::ParamsFieldArray &get_returning_param_fields() const
  {
    return returning_param_columns_;
  }
  void set_literal_stmt_type(stmt::StmtType literal_stmt_type) { literal_stmt_type_ = literal_stmt_type; }
  stmt::StmtType get_literal_stmt_type() const { return literal_stmt_type_; }
  int set_autoinc_params(const common::ObIArray<share::AutoincParam> &autoinc_params);
  void set_tablet_autoinc_param(const share::ObTabletAutoincParam &tablet_autoinc_param)
  {
    tablet_autoinc_param_ = tablet_autoinc_param;
  }
  inline bool is_distributed_plan() const { return OB_PHY_PLAN_DISTRIBUTED == plan_type_; }
  inline bool is_local_plan() const { return OB_PHY_PLAN_LOCAL == plan_type_; }
  inline bool is_remote_plan() const { return OB_PHY_PLAN_REMOTE == plan_type_; }
  inline bool is_local_or_remote_plan() const { return is_local_plan() || is_remote_plan(); }
  inline bool is_select_plan() const { return ObStmt::is_select_stmt(stmt_type_); }
  inline bool is_dist_insert_or_replace_plan() const
  {
    return OB_PHY_PLAN_DISTRIBUTED == plan_type_
        && (stmt_type_ == stmt::T_INSERT || stmt_type_ == stmt::T_REPLACE);
  }
  inline common::ObIArray<share::AutoincParam> &get_autoinc_params()
  {
    return autoinc_params_;
  }
  inline share::ObTabletAutoincParam &get_tablet_autoinc_param()
  {
    return tablet_autoinc_param_;
  }
  ObSqlExpressionFactory *get_sql_expression_factory()
  { return &sql_expression_factory_; }
  const ObSqlExpressionFactory *get_sql_expression_factory() const
  { return &sql_expression_factory_; }
  void set_has_top_limit(const bool has_top_limit) { has_top_limit_ = has_top_limit; }
  bool has_top_limit() const { return has_top_limit_; }
  void set_is_wise_join(const bool is_wise_join) { is_wise_join_ = is_wise_join; }
  bool is_wise_join() const { return is_wise_join_; }
  void set_contain_table_scan(bool v) { contain_table_scan_ = v; }
  bool contain_table_scan() const { return contain_table_scan_; }
  void set_has_nested_sql(bool has_nested_sql) { has_nested_sql_ = has_nested_sql; }
  bool has_nested_sql() const { return has_nested_sql_; }
  void set_session_id(uint64_t v) { session_id_ = v; }
  uint64_t get_session_id() const { return session_id_; }
  common::ObIArray<uint64_t> &get_gtt_trans_scope_ids() { return gtt_trans_scope_ids_; }
  common::ObIArray<uint64_t> &get_gtt_session_scope_ids() { return gtt_session_scope_ids_; }
  bool is_contain_oracle_trx_level_temporary_table() const { return gtt_trans_scope_ids_.count() > 0; }
  bool is_contain_oracle_session_level_temporary_table() const { return gtt_session_scope_ids_.count() > 0; }
  bool contains_temp_table() const {return 0 != session_id_; }
  void set_returning(bool is_returning) { is_returning_ = is_returning; }
  bool is_returning() const { return is_returning_; }

  bool use_das() const { return !das_table_locations_.empty(); }
  int set_table_locations(const ObIArray<ObTablePartitionInfo *> &info,
                          share::schema::ObSchemaGetterGuard &schema_guard);
  common::ObIArray<ObTableLocation> &get_table_locations() { return table_locations_; }
  const common::ObIArray<ObTableLocation> &get_table_locations() const { return table_locations_; }
  const common::ObIArray<ObTableLocation> &get_das_table_locations() const { return das_table_locations_; }

  inline const share::schema::ObStmtNeedPrivs &get_stmt_need_privs() const { return stmt_need_privs_; }
  inline const share::schema::ObStmtOraNeedPrivs &get_stmt_ora_need_privs() const
  { return stmt_ora_need_privs_; }
  int set_stmt_need_privs(const share::schema::ObStmtNeedPrivs& stmt_need_privs);
  int set_stmt_ora_need_privs(const share::schema::ObStmtOraNeedPrivs& stmt_need_privs);
  inline const common::ObIArray<ObAuditUnit> &get_audit_units() const
  { return audit_units_; }
  int set_audit_units(const common::ObIArray<ObAuditUnit>& audit_units);
  inline int16_t get_regexp_op_count() const { return regexp_op_count_; }
  inline void set_regexp_op_count(int16_t regexp_op_count) { regexp_op_count_ = regexp_op_count; }
  inline int16_t get_like_op_count() const { return like_op_count_; }
  inline void set_like_op_count(int16_t like_op_count) { like_op_count_ = like_op_count; }
  inline int16_t get_px_exchange_out_op_count() const { return px_exchange_out_op_count_; }
  inline void set_px_exchange_out_op_count(int16_t px_exchange_out_op_count) { px_exchange_out_op_count_ = px_exchange_out_op_count; }
  inline void inc_px_exchange_out_op_count() { px_exchange_out_op_count_++; }
  inline uint32_t &get_next_expr_id() { return next_expr_operator_id_; }
  // plan id and merged version used in plan cache
  uint64_t get_plan_id() { return get_object_id(); }
  uint64_t get_plan_id() const { return get_object_id(); }
  void set_affected_last_insert_id(bool is_affect_last_insert_id);
  bool is_affected_last_insert_id() const;
  inline void set_is_affect_found_row(bool is_affect_found_row) { is_affect_found_row_ = is_affect_found_row; }
  inline bool is_affect_found_row() const { return is_affect_found_row_; }
  inline uint64_t get_plan_hash_value() const { return signature_; }
  bool is_limited_concurrent_num() const {return max_concurrent_num_ != share::schema::ObMaxConcurrentParam::UNLIMITED;}
  inline const PhyRowParamMap &get_row_param_map() const { return row_param_map_; }
  inline PhyRowParamMap &get_row_param_map() { return row_param_map_; }
  int init_params_info_str();
  inline void set_first_array_index(int64_t first_array_index) { first_array_index_ = first_array_index; }
  inline int64_t get_first_array_index() const { return first_array_index_; }
  inline void set_is_batched_multi_stmt(bool is_batched_multi_stmt) {
    is_batched_multi_stmt_ = is_batched_multi_stmt;}
  inline bool get_is_batched_multi_stmt() const { return is_batched_multi_stmt_; }
  inline void set_use_pdml(bool value) { use_pdml_ = value; }
  inline bool is_use_pdml() const { return use_pdml_; }
  inline void set_use_temp_table(bool value) { use_temp_table_ = value; }
  inline bool is_use_temp_table() const { return use_temp_table_; }
  inline void set_has_link_table(bool value) { has_link_table_ = value; }
  inline bool has_link_table() const { return has_link_table_; }
  inline void set_has_link_sfd(bool value) { has_link_sfd_ = value; }
  inline bool has_link_sfd() const { return has_link_sfd_; }
  void set_batch_size(const int64_t v) { batch_size_ = v; }
  int64_t get_batch_size() const { return batch_size_; }
  bool is_vectorized() const { return batch_size_ > 0; }
  inline void set_ddl_schema_version(const int64_t ddl_schema_version) { ddl_schema_version_ = ddl_schema_version; }
  inline int64_t get_ddl_schema_version() const { return ddl_schema_version_; }
  inline void set_ddl_table_id(const int64_t ddl_table_id) { ddl_table_id_ = ddl_table_id; }
  inline int64_t get_ddl_table_id() const { return ddl_table_id_; }
  inline void set_ddl_execution_id(const int64_t ddl_execution_id) { ddl_execution_id_ = ddl_execution_id; }
  inline int64_t get_ddl_execution_id() const { return ddl_execution_id_; }
  inline void set_ddl_task_id(const int64_t ddl_task_id) { ddl_task_id_ = ddl_task_id; }
  inline int64_t get_ddl_task_id() const { return ddl_task_id_; }
  inline void set_enable_append(const bool enable_append) { enable_append_ = enable_append; }
  inline bool get_enable_append() const { return enable_append_; }
  inline void set_append_table_id(const uint64_t append_table_id) { append_table_id_ = append_table_id; }
  inline uint64_t get_append_table_id() const { return append_table_id_; }
  void set_record_plan_info(bool v) { need_record_plan_info_ = v; }
  bool need_record_plan_info() const { return need_record_plan_info_; }
  const common::ObString &get_rule_name() const { return stat_.rule_name_; }
  inline void set_is_rewrite_sql(bool v) { stat_.is_rewrite_sql_ = v; }
  inline bool is_rewrite_sql() const { return stat_.is_rewrite_sql_; }
  inline void set_rule_version(int64_t version) { stat_.rule_version_ = version; }
  inline int64_t get_rule_version() const { return stat_.rule_version_; }
  inline void set_is_enable_udr(const bool v) { stat_.enable_udr_ = v; }
  inline bool is_enable_udr() const { return stat_.enable_udr_; }
  inline int set_rule_name(const common::ObString &rule_name)
  {
    return ob_write_string(allocator_, rule_name, stat_.rule_name_);
  }
  inline int64_t get_plan_error_cnt() { return stat_.evolution_stat_.error_cnt_; }
  inline void update_plan_error_cnt() { ATOMIC_INC(&(stat_.evolution_stat_.error_cnt_)); }

public:
  int inc_concurrent_num();
  void dec_concurrent_num();
  int set_max_concurrent_num(int64_t max_curent_num);
  int64_t get_max_concurrent_num();
  bool is_sample_time() { return 0 == stat_.execute_times_ % SAMPLE_TIMES; }
  void set_contain_index_location(bool exist) { contain_index_location_ = exist; }
  bool contain_index_location() const { return contain_index_location_; }

  virtual int64_t get_pre_expr_ref_count() const override;
  virtual void inc_pre_expr_ref_count() override;
  virtual void dec_pre_expr_ref_count() override;
  virtual int before_cache_evicted() override;
  virtual void set_pre_calc_expr_handler(PreCalcExprHandler* handler) override;
  virtual PreCalcExprHandler* get_pre_calc_expr_handler() override;

  void set_enable_plan_expiration(bool enable) { stat_.enable_plan_expiration_ = enable; }
  int64_t &get_access_table_num() { return stat_.access_table_num_; }
  int64_t get_access_table_num() const { return stat_.access_table_num_; }
  ObTableRowCount *&get_table_row_count_first_exec() { return stat_.table_row_count_first_exec_; }

  ObIArray<LocationConstraint>& get_base_constraints() { return base_constraints_; }
  const ObIArray<LocationConstraint>& get_base_constraints() const { return base_constraints_; }
  ObIArray<ObPlanPwjConstraint>& get_strict_constraints() { return strict_constrinats_; }
  const ObIArray<ObPlanPwjConstraint>& get_strict_constraints() const { return strict_constrinats_; }
  ObIArray<ObPlanPwjConstraint>& get_non_strict_constraints() { return non_strict_constrinats_; }
  const ObIArray<ObPlanPwjConstraint>& get_non_strict_constraints() const { return non_strict_constrinats_; }
  ObIArray<ObDupTabConstraint> &get_dup_table_replica_constraints() {
    return dup_table_replica_cons_;
  }
  const ObIArray<ObDupTabConstraint> &get_dup_table_replica_constraints() const {
    return dup_table_replica_cons_;
  }
  int set_location_constraints(const ObIArray<LocationConstraint> &base_constraints,
                               const ObIArray<ObPwjConstraint *> &strict_constraints,
                               const ObIArray<ObPwjConstraint *> &non_strict_constraints,
                               const ObIArray<ObDupTabConstraint> &dup_table_replica_cons);
  bool has_same_location_constraints(const ObPhysicalPlan &r) const;

  ObIArray<transaction::ObEncryptMetaCache>& get_encrypt_meta_array()
  { return encrypt_meta_array_; }
  const ObIArray<transaction::ObEncryptMetaCache>& get_encrypt_meta_array() const
  { return encrypt_meta_array_; }
  int get_encrypt_meta(const uint64_t table_id,
                       ObIArray<transaction::ObEncryptMetaCache> &metas,
                       const ObIArray<transaction::ObEncryptMetaCache> *&ret_ptr) const;
  inline bool get_is_late_materialized() const
  {
    return is_late_materialized_;
  }

  inline void set_is_late_materialized(const bool is_late_mat)
  {
    is_late_materialized_ = is_late_mat;
  }

  inline bool is_use_jit() const
  {
    return stat_.is_use_jit_;
  }

  inline void set_is_dep_base_table(bool v) { is_dep_base_table_ = v; }
  inline bool is_dep_base_table() const { return is_dep_base_table_; }

  inline void set_is_insert_select(bool v) { is_insert_select_ = v; }
  inline bool is_insert_select() const { return is_insert_select_; }
  inline void set_is_plain_insert(bool v) { is_plain_insert_ = v; }
  inline bool is_plain_insert() const { return is_plain_insert_; }
  inline bool should_add_baseline() const {
    return (ObStmt::is_dml_stmt(stmt_type_)
            && (stmt::T_INSERT != stmt_type_ || is_insert_select_)
            && (stmt::T_REPLACE != stmt_type_ || is_insert_select_));
  }
  inline bool is_plain_select() const
  {
    return stmt::T_SELECT == stmt_type_ && !has_for_update() && !contain_pl_udf_or_trigger_;
  }

  inline bool contain_paramed_column_field() const { return contain_paramed_column_field_; }
  inline ObExprFrameInfo &get_expr_frame_info() { return expr_frame_info_; }
  inline const ObExprFrameInfo &get_expr_frame_info() const { return expr_frame_info_; }

  const ObOpSpec *get_root_op_spec() const { return root_op_spec_; }
  inline bool is_link_dml_plan() {
    bool is_link_dml = false;
    if (NULL != get_root_op_spec()) {
      is_link_dml = oceanbase::sql::ObPhyOperatorType::PHY_LINK_DML == get_root_op_spec()->type_;
    }
    return is_link_dml;
  }
  void set_root_op_spec(ObOpSpec *spec) { root_op_spec_ = spec; is_new_engine_ = true; }
  inline bool need_consistent_snapshot() const { return need_consistent_snapshot_; }
  inline void set_need_consistent_snapshot(bool need_snapshot)
  { need_consistent_snapshot_ = need_snapshot; }

  void set_need_serial_exec(bool need_serial_exec) { need_serial_exec_ = need_serial_exec; }
  bool get_need_serial_exec() const { return need_serial_exec_; }

  void set_contain_pl_udf_or_trigger(bool v) { contain_pl_udf_or_trigger_ = v; }
  bool contain_pl_udf_or_trigger() { return contain_pl_udf_or_trigger_; }
  bool contain_pl_udf_or_trigger() const { return contain_pl_udf_or_trigger_; }
  void set_is_packed(const bool is_packed) { is_packed_ = is_packed; }
  bool is_packed() const { return is_packed_; }
  void set_has_instead_of_trigger(bool v) { has_instead_of_trigger_ = v;}
  bool has_instead_of_trigger() const { return has_instead_of_trigger_; }
  virtual int update_cache_obj_stat(ObILibCacheCtx &ctx);
  void calc_whether_need_trans();
  inline uint64_t get_min_cluster_version() const { return min_cluster_version_; }
  inline void set_min_cluster_version(uint64_t curr_cluster_version)
  {
    if (curr_cluster_version > min_cluster_version_) {
      min_cluster_version_ = curr_cluster_version;
    }
  }

  int set_logical_plan(ObLogicalPlanRawData &logical_plan);
  inline ObLogicalPlanRawData& get_logical_plan() { return logical_plan_; }
  inline const ObLogicalPlanRawData& get_logical_plan()const { return logical_plan_; }
  int set_feedback_info(ObExecContext &ctx);

  void set_enable_px_fast_reclaim(bool value) { is_enable_px_fast_reclaim_ = value; }
  bool is_enable_px_fast_reclaim() const { return is_enable_px_fast_reclaim_; }
public:
  static const int64_t MAX_PRINTABLE_SIZE = 2 * 1024 * 1024;
private:
  static const int64_t COMMON_OP_NUM = 16;
  static const int64_t COMMON_SUB_QUERY_NUM = 6;
  static const int64_t COMMON_BASE_TABLE_NUM = 64;
  static const int64_t COMMON_SQL_EXPR_NUM = 256;
  static const int64_t COMMON_PARAM_NUM = 12;
  static const int64_t SAMPLE_TIMES = 10;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalPlan);
private:
  ObPhyPlanHint phy_hint_; //hints for this plan
  // root operator spec for static typing engine.
  ObOpSpec *root_op_spec_;
  int64_t param_count_;
  uint64_t signature_;
  // 运行时只读数据结构
  /**
   * This is stored in order to accelerate the plan cache searching.
   * During the plan cache searching, we need to calculate the table location based
   * on the given user params, for which, we need to do the following if pre_table_locations
   * is not saved:
   *  1. resolve the statement to get the partition columns
   *  2. extract the range of the partition columns using query range and given predicates
   *  3. get the ObRawExpr of the partition expr and convert it to a postfix expression
   *  4. calculate the partition ids using the postfix expression
   *  5. inquire the location cache service to get a list of partition locations given
   *     the partition ids
   *
   *  However, with the pre table location saved, we can readily skip steps 1, 2(partially),
   *  and 3, which is good for the performance.
   */

  //for fill_result_set
  common::ColumnsFieldArray field_columns_;
  common::ParamsFieldArray param_columns_;
  common::ParamsFieldArray returning_param_columns_;
  common::ObFixedArray<share::AutoincParam, common::ObIAllocator> autoinc_params_; //auto-increment param
  share::ObTabletAutoincParam tablet_autoinc_param_;
  // for privilege check
  share::schema::ObStmtNeedPrivs stmt_need_privs_;
  share::schema::ObStmtOraNeedPrivs stmt_ora_need_privs_;
  // for security audit
  common::ObFixedArray<ObAuditUnit, common::ObIAllocator> audit_units_;
  // 涉及到的系统变量和用户变量
  common::ObFixedArray<ObVarInfo, common::ObIAllocator> vars_;
  ObSqlExpressionFactory sql_expression_factory_;
  ObExprOperatorFactory expr_op_factory_;
  stmt::StmtType literal_stmt_type_; // 含义参考ObBasicStmt中对应定义
  // 指示分布式执行器以何种调度方式执行本plan
  ObPhyPlanType plan_type_;
  //给事物使用, 指示本plan所涉及的数据的分布情况
  ObPhyPlanType location_type_;
  // 表示计划是否一定需要local的方式执行，用于处理：multi part insert（remote）+ select （local）的情况
  bool require_local_execution_; // not need serialize
  bool use_px_;
  int64_t px_dop_;
  uint32_t next_phy_operator_id_; //share val
  uint32_t next_expr_operator_id_; //share val
  // for regexp expression's compilation
  int16_t regexp_op_count_;
  // for like expression's optimization
  int16_t like_op_count_;
  // for px fast path
  int16_t px_exchange_out_op_count_;
  bool is_sfu_;
  //if the stmt  contains user variable assignment
  //such as @a:=123
  //we may need to serialize the map to remote server
  bool is_contains_assignment_;
  bool affected_last_insert_id_; //不需要序列化远端，只在本地生成执行计划和open resultset的时候需要
  bool is_affect_found_row_; //not need serialize，标记这个计划是否会影响 found_rows() 函数返回值
                             // found_rows() 细节见 https://mariadb.com/kb/en/found_rows/
  bool has_top_limit_; //not need serialize
  bool is_wise_join_; // not need serialize
  bool contain_table_scan_; //是否包含主键扫描
  bool has_nested_sql_; // 是否可能执行嵌套语句
  uint64_t  session_id_; //当计划包含临时表时记录table_schema->session_id, 用于判断计划能否重用
  bool contain_oracle_trx_level_temporary_table_; // not used
  bool contain_oracle_session_level_temporary_table_; // not used
  common::ObFixedArray<uint64_t, common::ObIAllocator> gtt_session_scope_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> gtt_trans_scope_ids_;

  //for outline use
  ObOutlineState outline_state_;
  int64_t concurrent_num_;           //plan当前的并发执行个数
  int64_t max_concurrent_num_;       //plan最大并发可执行个数, -1表示没有限制
  //for plan cache, not need serialize
  TableLocationFixedArray table_locations_; //普通表的table location，参与plan cache计划选择
  TableLocationFixedArray das_table_locations_; //DAS表的table location，用于计算DAS的分区信息

  ObString dummy_string_;  // for compatible with 3.x JIT func_ member
  PhyRowParamMap row_param_map_;
  bool is_update_uniq_index_;
  //判断该计划涉及的base table是否含有global index
  bool contain_index_location_;

  // 用于分布式计划比对
  // 基表location约束，包括TABLE_SCAN算子上的基表和INSERT算子上的基表
  ObPlanLocationConstraint base_constraints_;
  // 严格partition wise join约束，要求同一个分组内的基表分区逻辑上和物理上都相等。
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  // 如果t1, t2需要满足严格约束，则对于t1的每一个分区（分区裁剪后），都要求有一个分区定义相同的t2分区（分区裁剪后）
  // 与其在相同的物理机器上
  PlanPwjConstraintArray strict_constrinats_;
  // 非严格partition wise join约束，要求用一个分组内的基表分区物理上相等，目前仅UNION ALL会用到非严格约束
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  // 如果t1, t2需要满足非严格约束，则对于分区裁剪后t1的每一个分区，都要求有一个t2的分区与其在相同的物理机器上
  PlanPwjConstraintArray non_strict_constrinats_;
  // constraint for duplicate table to choose replica
  // dist plan will use this as (dup_tab_pos, advisor_tab_pos) pos is position in base constraint
  DupTabReplicaArray dup_table_replica_cons_;
public:
  ObExprFrameInfo expr_frame_info_;

  ObPlanStat stat_;
  ObPhyOperatorStats op_stats_;
  const int64_t MAX_BINARY_CODE_LEN = 1024 * 256; //256k
  //@todo: yuchen.wyc add a temporary member to mark whether
  //the DML statement needs to be executed through get_next_row
  bool need_drive_dml_query_;
  int64_t tx_id_; //for dblink recover xa tx
  int64_t tm_sessid_; //for dblink get connection attached on tm session
  ExprFixedArray var_init_exprs_;
private:
  bool is_returning_; //是否设置了returning

  // 标记计划是否为晚期物化计划
  bool is_late_materialized_;
  // **** for spm ****
  // 判断该计划是否依赖base table
  bool is_dep_base_table_;
  //判断该plan对应sql是否为insert into ... select ...
  bool is_insert_select_;
  // insert into values(x),(x)...(x)
  bool is_plain_insert_;
  // **** for spm end ***
  //已经废弃，兼容保留
  common::ObFixedArray<FlashBackQueryItem, common::ObIAllocator> flashback_query_items_;
  // column field数组中是否有参数化的column
  // 如果有参数化的column，每次都ob_result_set必须深拷column_fields_，并用模板构造column
  bool contain_paramed_column_field_;
  int64_t first_array_index_;
  bool need_consistent_snapshot_;
  bool is_batched_multi_stmt_;
#ifndef NDEBUG
public:
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> bit_set_;
#endif
  EncryptMetaCacheArray encrypt_meta_array_;
  int64_t is_new_engine_;
  bool use_pdml_; //is parallel dml plan
  bool use_temp_table_;
  bool has_link_table_;
  bool has_link_sfd_;
  bool need_serial_exec_;//mark if need serial execute?
  bool temp_sql_can_prepare_;
  bool is_need_trans_;
  // batch row count in vectorized execution
  int64_t batch_size_;
  bool contain_pl_udf_or_trigger_;//mark if need sync pkg variables
  int64_t ddl_schema_version_;
  int64_t ddl_table_id_;
  int64_t ddl_execution_id_;
  int64_t ddl_task_id_;
  //parallel encoding of output_expr in advance to speed up packet response
  bool is_packed_;
  bool has_instead_of_trigger_; // mask if has instead of trigger on view
  uint64_t min_cluster_version_; // record min cluster version in code gen
  bool need_record_plan_info_;
  bool enable_append_; // for APPEND hint
  uint64_t append_table_id_;
  ObLogicalPlanRawData logical_plan_;
  // for detector manager
  bool is_enable_px_fast_reclaim_;
};

inline void ObPhysicalPlan::set_affected_last_insert_id(bool affected_last_insert_id)
{
  affected_last_insert_id_ = affected_last_insert_id;
}
inline bool ObPhysicalPlan::is_affected_last_insert_id() const
{
  return affected_last_insert_id_;
}
inline int32_t *ObPhysicalPlan::alloc_projector(int64_t projector_size)
{
  return static_cast<int32_t*>(allocator_.alloc(sizeof(int32_t)*projector_size));
}

inline ObPhyPlanType ObPhysicalPlan::get_location_type() const
{
  return location_type_;
}

} //namespace sql
} //namespace oceanbase
#endif //OCEANBASE_SQL_OB_PHYSICAL_PLAN_H
