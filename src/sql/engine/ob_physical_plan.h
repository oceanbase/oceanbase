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
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator_factory.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/executor/ob_executor.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/monitor/ob_phy_operator_stats.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace sql {
class ObTablePartitionInfo;
class ObPhyOperatorMonnitorInfo;
class ObAuditRecordData;
class ObOpSpec;

// class ObPhysicalPlan: public common::ObDLinkBase<ObPhysicalPlan>
typedef common::ObFixedArray<common::ObFixedArray<int64_t, common::ObIAllocator>, common::ObIAllocator> PhyRowParamMap;
typedef common::ObFixedArray<ObTableLocation, common::ObIAllocator> TableLocationFixedArray;
typedef common::ObFixedArray<ObPlanPwjConstraint, common::ObIAllocator> PlanPwjConstraintArray;

struct FlashBackQueryItem {
  OB_UNIS_VERSION(1);

public:
  FlashBackQueryItem()
      : table_id_(common::OB_INVALID_ID),
        time_val_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
        time_expr_(nullptr),
        type_(FLASHBACK_QUERY_ITEM_INVALID)
  {}

  enum FlashBackQueryItemType {
    FLASHBACK_QUERY_ITEM_INVALID,
    FLASHBACK_QUERY_ITEM_TIMESTAMP,
    FLASHBACK_QUERY_ITEM_SCN
  };

  DECLARE_TO_STRING;

  int64_t table_id_;
  int64_t time_val_;
  ObSqlExpression* time_expr_;
  FlashBackQueryItemType type_;
};

class ObPhysicalPlan : public ObCacheObject {
public:
  static const int32_t SLOW_QUERY_TIME = 100000;                // 100ms
  static const int64_t SLOW_QUERY_TIME_FOR_PLAN_EXPIRE = 5000;  // 5ms
  static const int64_t SLOW_QUERY_ROW_COUNT_THRESOLD = 5000;
  static const int64_t SLOW_QUERY_SAMPLE_SIZE = 20;  // smaller than ObPlanStat::MAX_SCAN_STAT_SIZE
  static const int64_t TABLE_ROW_CHANGE_THRESHOLD = 2;
  static const int64_t EXPIRED_PLAN_TABLE_ROW_THRESHOLD = 100;
  OB_UNIS_VERSION(1);

public:
  explicit ObPhysicalPlan(lib::MemoryContext& mem_context = CURRENT_CONTEXT);
  virtual ~ObPhysicalPlan();

  virtual void destroy();
  virtual void reset();
  void set_is_last_open_succ(bool val)
  {
    stat_.is_last_open_succ_ = val;
  }
  bool is_last_open_succ() const
  {
    return stat_.is_last_open_succ_;
  }
  bool from_plan_baseline() const
  {
    return common::OB_INVALID_ID != stat_.bl_info_.plan_baseline_id_;
  }
  const ObString& get_constructed_sql() const
  {
    return stat_.bl_info_.key_.constructed_sql_;
  }
  bool temp_sql_can_prepare() const
  {
    return temp_sql_can_prepare_;
  }
  void set_temp_sql_can_prepare()
  {
    temp_sql_can_prepare_ = true;
  }
  bool with_rows() const
  {
    return ObStmt::is_select_stmt(stmt_type_) || is_returning();
  }
  int copy_common_info(ObPhysicalPlan& src);
  int extract_query_range(ObExecContext& ctx) const;
  // user var
  bool is_contains_assignment() const
  {
    return is_contains_assignment_;
  }
  void set_contains_assignment(bool v)
  {
    is_contains_assignment_ = v;
  }
  int set_vars(const common::ObIArray<ObVarInfo>& vars);
  const common::ObIArray<ObVarInfo>& get_vars() const
  {
    return vars_;
  }
  int add_phy_query(ObPhyOperator* phy_query);
  int store_phy_operator(ObPhyOperator* op);
  /**
   *
   * @param[in] table_row_count_list
   *    records row count of tables that refered by execution plan,
   *    this only used by evicting execution plan according to table's row count
   */
  void update_plan_stat(const ObAuditRecordData& record, const bool is_first, const bool is_evolution,
      const ObIArray<ObTableRowCount>* table_row_count_list);
  void update_cache_access_stat(const ObTableScanStat& scan_stat)
  {
    stat_.update_cache_stat(scan_stat);
  }
  int64_t get_evo_perf() const;
  void set_evolution(bool v)
  {
    stat_.is_evolution_ = v;
  }
  bool get_evolution() const
  {
    return stat_.is_evolution_;
  }
  inline bool check_if_is_expired(const int64_t first_exec_row_count, const int64_t current_row_count) const;

  bool is_plan_unstable();
  bool is_expired() const
  {
    return stat_.is_expired_;
  }
  void set_is_expired(bool expired)
  {
    stat_.is_expired_ = expired;
  }
  void inc_large_querys();
  void inc_delayed_large_querys();
  void inc_delayed_px_querys();
  int update_operator_stat(ObPhyOperatorMonitorInfo& info);
  bool is_need_trans() const
  {
    bool bool_ret = false;
    if (OB_UNLIKELY(stmt::T_EXPLAIN == stmt_type_)) {
      // false
    } else if (get_dependency_table_size() <= 0) {
      // false
    } else if (is_use_temp_table()) {
      bool_ret = true;
    } else {
      for (int64_t i = 0; i < get_dependency_table_size(); ++i) {
        if (get_dependency_table().at(i).is_base_table()) {
          const share::schema::ObSchemaObjVersion& table_version = get_dependency_table().at(i);
          uint64_t table_id = table_version.get_object_id();
          if (table_version.is_existed() &&
              (!common::is_virtual_table(table_id) || share::is_oracle_mapping_real_virtual_table(table_id)) &&
              !common::is_cte_table(table_id)) {
            bool_ret = true;
            break;
          }
        }
      }
    }
    return bool_ret;
  }

  int set_query_hint(const ObQueryHint& query_hint)
  {
    return query_hint_.deep_copy(query_hint, allocator_);
  }
  ObQueryHint& get_query_hint()
  {
    return query_hint_;
  }
  const ObQueryHint& get_query_hint() const
  {
    return query_hint_;
  }
  ObPhyOperator* get_main_query() const;
  void set_main_query(ObPhyOperator* query);
  template <typename T>
  inline int alloc_operator_by_type(
      ObPhyOperatorType phy_operator_type, T*& phy_op, const uint64_t op_id = OB_INVALID_ID);

  int alloc_op_spec(const ObPhyOperatorType type, const int64_t child_cnt, ObOpSpec*& op, const uint64_t op_id);

  void set_location_type(ObPhyPlanType type)
  {
    location_type_ = type;
  }
  bool has_uncertain_local_operator() const
  {
    return OB_PHY_PLAN_UNCERTAIN == location_type_;
  }
  inline ObPhyPlanType get_location_type() const;
  void set_plan_type(ObPhyPlanType type)
  {
    plan_type_ = type;
  }
  ObPhyPlanType get_plan_type() const
  {
    return plan_type_;
  }
  void set_require_local_execution(bool v)
  {
    require_local_execution_ = v;
  }
  bool is_require_local_execution() const
  {
    return require_local_execution_;
  }
  void set_use_px(bool use_px)
  {
    use_px_ = use_px;
  }
  bool is_use_px() const
  {
    return use_px_;
  }
  void set_px_dop(int64_t px_dop)
  {
    px_dop_ = px_dop;
  }
  int64_t get_px_dop() const
  {
    return px_dop_;
  }
  void set_expected_worker_count(int64_t c)
  {
    stat_.expected_worker_count_ = c;
  }
  int64_t get_expected_worker_count() const
  {
    return stat_.expected_worker_count_;
  }
  const char* get_sql_id() const
  {
    return stat_.sql_id_;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int64_t to_string(char* buf, const int64_t buf_len, ObPhyOperator* start_query) const;
  uint32_t get_next_phy_operator_id()
  {
    return next_phy_operator_id_++;
  }
  uint32_t get_next_phy_operator_id() const
  {
    return next_phy_operator_id_;
  }
  void set_next_phy_operator_id(uint32_t next_operator_id)
  {
    next_phy_operator_id_ = next_operator_id;
  }
  uint32_t get_phy_operator_size() const
  {
    return next_phy_operator_id_;
  }
  void set_next_expr_operator_id(uint32_t next_expr_id)
  {
    next_expr_operator_id_ = next_expr_id;
  }
  uint32_t get_expr_operator_size() const
  {
    return next_expr_operator_id_;
  }
  int init_operator_stats();

  inline void set_param_count(int64_t param_count)
  {
    param_count_ = param_count;
  }
  inline int64_t get_param_count() const
  {
    return param_count_;
  }

  void set_is_update_uniq_index(bool is_update_uniq_index)
  {
    is_update_uniq_index_ = is_update_uniq_index;
  }
  bool get_is_update_uniq_index() const
  {
    return is_update_uniq_index_;
  }

  void set_for_update(bool is_sfu)
  {
    is_sfu_ = is_sfu_ || is_sfu;
  }
  bool has_for_update() const
  {
    return is_sfu_;
  }
  void set_signature(uint64_t sign)
  {
    signature_ = sign;
  }
  uint64_t get_signature() const
  {
    return signature_;
  }
  void set_plan_hash_value(uint64_t v)
  {
    stat_.bl_info_.plan_hash_value_ = v;
  }
  int add_calculable_expr(ObSqlExpression* expr);
  int32_t* alloc_projector(int64_t projector_size);
  int add_table_location(const ObPhyTableLocation& table_location);
  ObExprOperatorFactory& get_expr_op_factory()
  {
    return expr_op_factory_;
  }
  const ObExprOperatorFactory& get_expr_op_factory() const
  {
    return expr_op_factory_;
  }

  int set_field_columns(const common::ColumnsFieldArray& fields);

  inline const common::ColumnsFieldArray& get_field_columns() const
  {
    return field_columns_;
  }
  int set_param_fields(const common::ParamsFieldArray& fields);
  inline const common::ParamsFieldArray& get_param_fields() const
  {
    return param_columns_;
  }
  void set_literal_stmt_type(stmt::StmtType literal_stmt_type)
  {
    literal_stmt_type_ = literal_stmt_type;
  }
  stmt::StmtType get_literal_stmt_type() const
  {
    return literal_stmt_type_;
  }
  int set_autoinc_params(const common::ObIArray<share::AutoincParam>& autoinc_params);
  inline bool is_distributed_plan() const
  {
    return OB_PHY_PLAN_DISTRIBUTED == plan_type_;
  }
  inline bool is_local_plan() const
  {
    return OB_PHY_PLAN_LOCAL == plan_type_;
  }
  inline bool is_remote_plan() const
  {
    return OB_PHY_PLAN_REMOTE == plan_type_;
  }
  inline bool is_local_or_remote_plan() const
  {
    return is_local_plan() || is_remote_plan();
  }
  inline bool is_select_plan() const
  {
    return ObStmt::is_select_stmt(stmt_type_);
  }
  inline bool is_dist_insert_or_replace_plan() const
  {
    return OB_PHY_PLAN_DISTRIBUTED == plan_type_ && (stmt_type_ == stmt::T_INSERT || stmt_type_ == stmt::T_REPLACE);
  }
  inline common::ObIArray<share::AutoincParam>& get_autoinc_params()
  {
    return autoinc_params_;
  }
  ObSqlExpressionFactory* get_sql_expression_factory()
  {
    return &sql_expression_factory_;
  }
  const ObSqlExpressionFactory* get_sql_expression_factory() const
  {
    return &sql_expression_factory_;
  }
  void set_has_top_limit(const bool has_top_limit)
  {
    has_top_limit_ = has_top_limit;
  }
  bool has_top_limit() const
  {
    return has_top_limit_;
  }
  void set_is_wise_join(const bool is_wise_join)
  {
    is_wise_join_ = is_wise_join;
  }
  bool is_wise_join() const
  {
    return is_wise_join_;
  }
  void set_contain_table_scan(bool v)
  {
    contain_table_scan_ = v;
  }
  bool contain_table_scan() const
  {
    return contain_table_scan_;
  }
  void set_has_nested_sql(bool has_nested_sql)
  {
    has_nested_sql_ = has_nested_sql;
  }
  bool has_nested_sql() const
  {
    return has_nested_sql_;
  }
  void set_session_id(uint64_t v)
  {
    session_id_ = v;
  }
  uint64_t get_session_id() const
  {
    return session_id_;
  }
  void set_contain_oracle_trx_level_temporary_table()
  {
    contain_oracle_trx_level_temporary_table_ = true;
  }
  bool is_contain_oracle_trx_level_temporary_table() const
  {
    return contain_oracle_trx_level_temporary_table_;
  }
  void set_contain_oracle_session_level_temporary_table()
  {
    contain_oracle_trx_level_temporary_table_ = true;
  }
  bool is_contain_oracle_session_level_temporary_table() const
  {
    return contain_oracle_trx_level_temporary_table_;
  }
  bool contains_temp_table() const
  {
    return 0 != session_id_;
  }
  void set_returning(bool is_returning)
  {
    is_returning_ = is_returning;
  }
  bool is_returning() const
  {
    return is_returning_;
  }

  int set_table_locations(const ObIArray<ObTablePartitionInfo*>& info);
  ObIArray<ObTableLocation>& get_table_locations()
  {
    return table_locations_;
  }
  const ObIArray<ObTableLocation>& get_table_locations() const
  {
    return table_locations_;
  }

  inline const share::schema::ObStmtNeedPrivs& get_stmt_need_privs() const
  {
    return stmt_need_privs_;
  }
  inline const share::schema::ObStmtOraNeedPrivs& get_stmt_ora_need_privs() const
  {
    return stmt_ora_need_privs_;
  }
  int set_stmt_need_privs(const share::schema::ObStmtNeedPrivs& stmt_need_privs);
  int set_stmt_ora_need_privs(const share::schema::ObStmtOraNeedPrivs& stmt_need_privs);
  inline int16_t get_regexp_op_count() const
  {
    return regexp_op_count_;
  }
  inline void set_regexp_op_count(int16_t regexp_op_count)
  {
    regexp_op_count_ = regexp_op_count;
  }
  inline int16_t get_like_op_count() const
  {
    return like_op_count_;
  }
  inline void set_like_op_count(int16_t like_op_count)
  {
    like_op_count_ = like_op_count;
  }
  inline int16_t get_px_exchange_out_op_count() const
  {
    return px_exchange_out_op_count_;
  }
  inline void set_px_exchange_out_op_count(int16_t px_exchange_out_op_count)
  {
    px_exchange_out_op_count_ = px_exchange_out_op_count;
  }
  inline void inc_px_exchange_out_op_count()
  {
    px_exchange_out_op_count_++;
  }
  inline uint32_t& get_next_expr_id()
  {
    return next_expr_operator_id_;
  }
  // plan id and merged version used in plan cache
  uint64_t get_plan_id()
  {
    return get_object_id();
  }
  uint64_t get_plan_id() const
  {
    return get_object_id();
  }
  void set_affected_last_insert_id(bool is_affect_last_insert_id);
  bool is_affected_last_insert_id() const;
  inline void set_is_affect_found_row(bool is_affect_found_row)
  {
    is_affect_found_row_ = is_affect_found_row;
  }
  inline bool is_affect_found_row() const
  {
    return is_affect_found_row_;
  }
  inline uint64_t get_plan_hash_value() const
  {
    return signature_;
  }
  bool is_limited_concurrent_num() const
  {
    return max_concurrent_num_ != share::schema::ObMaxConcurrentParam::UNLIMITED;
  }
  inline const PhyRowParamMap& get_row_param_map() const
  {
    return row_param_map_;
  }
  inline PhyRowParamMap& get_row_param_map()
  {
    return row_param_map_;
  }
  int init_params_info_str();
  inline void set_first_array_index(int64_t first_array_index)
  {
    first_array_index_ = first_array_index;
  }
  inline int64_t get_first_array_index() const
  {
    return first_array_index_;
  }
  inline void set_is_batched_multi_stmt(bool is_batched_multi_stmt)
  {
    is_batched_multi_stmt_ = is_batched_multi_stmt;
  }
  inline bool get_is_batched_multi_stmt() const
  {
    return is_batched_multi_stmt_;
  }
  inline void set_use_pdml(bool value)
  {
    use_pdml_ = value;
  }
  inline bool is_use_pdml()
  {
    return use_pdml_;
  }
  inline void set_use_temp_table(bool value)
  {
    use_temp_table_ = value;
  }
  inline bool is_use_temp_table() const
  {
    return use_temp_table_;
  }
  inline void set_has_link_table(bool value)
  {
    has_link_table_ = value;
  }
  inline bool has_link_table() const
  {
    return has_link_table_;
  }

public:
  static void print_tree(char* buf, const int64_t buf_len, int64_t& pos, const ObPhyOperator* op);
  int inc_concurrent_num();
  void dec_concurrent_num();
  int set_max_concurrent_num(int64_t max_curent_num);
  int64_t get_max_concurrent_num();
  bool is_sample_time()
  {
    return 0 == stat_.execute_times_ % SAMPLE_TIMES;
  }
  int generate_code();
  void set_is_contain_global_index(bool exist)
  {
    is_contain_global_index_ = exist;
  }
  bool get_is_contain_global_index() const
  {
    return is_contain_global_index_;
  }

  int64_t get_pre_expr_ref_count() const;
  void inc_pre_expr_ref_count();
  void dec_pre_expr_ref_count();
  void set_pre_calc_expr_handler(PreCalcExprHandler* handler);
  PreCalcExprHandler* get_pre_calc_expr_handler();

  void set_enable_plan_expiration(bool enable)
  {
    stat_.enable_plan_expiration_ = enable;
  }
  int64_t& get_access_table_num()
  {
    return stat_.access_table_num_;
  }
  int64_t get_access_table_num() const
  {
    return stat_.access_table_num_;
  }
  ObTableRowCount*& get_table_row_count_first_exec()
  {
    return stat_.table_row_count_first_exec_;
  }

  ObIArray<LocationConstraint>& get_base_constraints()
  {
    return base_constraints_;
  }
  const ObIArray<LocationConstraint>& get_base_constraints() const
  {
    return base_constraints_;
  }
  ObIArray<ObPlanPwjConstraint>& get_strict_constraints()
  {
    return strict_constrinats_;
  }
  const ObIArray<ObPlanPwjConstraint>& get_strict_constraints() const
  {
    return strict_constrinats_;
  }
  ObIArray<ObPlanPwjConstraint>& get_non_strict_constraints()
  {
    return non_strict_constrinats_;
  }
  const ObIArray<ObPlanPwjConstraint>& get_non_strict_constraints() const
  {
    return non_strict_constrinats_;
  }

  int set_location_constraints(const ObIArray<LocationConstraint>& base_constraints,
      const ObIArray<ObPwjConstraint*>& strict_constraints, const ObIArray<ObPwjConstraint*>& non_strict_constraints);

  inline bool get_is_late_materialized() const
  {
    return is_late_materialized_;
  }

  inline void set_is_late_materialized(const bool is_late_mat)
  {
    is_late_materialized_ = is_late_mat;
  }

  inline void set_is_dep_base_table(bool v)
  {
    is_dep_base_table_ = v;
  }
  inline bool is_dep_base_table() const
  {
    return is_dep_base_table_;
  }

  inline void set_is_insert_select(bool v)
  {
    is_insert_select_ = v;
  }
  inline bool is_insert_select() const
  {
    return is_insert_select_;
  }
  inline bool should_add_baseline() const
  {
    return (ObStmt::is_dml_stmt(stmt_type_) && (stmt::T_INSERT != stmt_type_ || is_insert_select_) &&
            (stmt::T_REPLACE != stmt_type_ || is_insert_select_));
  }

  inline bool contain_paramed_column_field() const
  {
    return contain_paramed_column_field_;
  }
  inline ObExprFrameInfo& get_expr_frame_info()
  {
    return expr_frame_info_;
  }
  inline const ObExprFrameInfo& get_expr_frame_info() const
  {
    return expr_frame_info_;
  }

  const ObOpSpec* get_root_op_spec() const
  {
    return root_op_spec_;
  }
  void set_root_op_spec(ObOpSpec* spec)
  {
    root_op_spec_ = spec;
    is_new_engine_ = true;
  }
  inline bool need_consistent_snapshot() const
  {
    return need_consistent_snapshot_;
  }
  inline void set_need_consistent_snapshot(bool need_snapshot)
  {
    need_consistent_snapshot_ = need_snapshot;
  }

  bool is_new_engine() const
  {
    return is_new_engine_;
  }
  int generate_mock_rowid_tables(share::schema::ObSchemaGetterGuard& schema_guard);

  const common::ObIArray<uint64_t>& get_mock_rowid_tables() const
  {
    return mock_rowid_tables_;
  }
  void set_need_serial_exec(bool need_serial_exec)
  {
    need_serial_exec_ = need_serial_exec;
  }
  bool get_need_serial_exec() const
  {
    return need_serial_exec_;
  }

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
  ObQueryHint query_hint_;  // hints for this plan
  // root phy operator for the old engine.
  ObPhyOperator* main_query_;
  // root operator spec for static typing engine.
  ObOpSpec* root_op_spec_;
  int64_t param_count_;
  uint64_t signature_;
  // runtime readonly data structure
  /**
   * This is stored in order to accelerate the plan cache searching.
   * During the plan cache searching, we need to calculate the table location based
   * on the given user params, for which, we need to do the following if pre_table_locations
   * is not saved:
   *  1. resolve the statement to get the partition columns
   *  2. extract the range of the partition columns using query range and given predicates
   *  3. get the ObRawExpr of the partition expr and convert it to a postfix expresssion
   *  4. calculate the partition ids using the postfix expression
   *  5. inquire the location cache servince to get a list of partition locations given
   *     the partition ids
   *
   *  However, with the pre table location saved, we can readily skip steps 1, 2(partially),
   *  and 3, which is good for the performance.
   */

  // for fill_result_set
  common::ColumnsFieldArray field_columns_;
  common::ParamsFieldArray param_columns_;
  common::ObFixedArray<share::AutoincParam, common::ObIAllocator> autoinc_params_;  // auto-increment param
  // for privilege check
  share::schema::ObStmtNeedPrivs stmt_need_privs_;
  share::schema::ObStmtOraNeedPrivs stmt_ora_need_privs_;
  // system variables and user variables
  common::ObFixedArray<ObVarInfo, common::ObIAllocator> vars_;
  ObPhyOperatorFactory op_factory_;
  ObSqlExpressionFactory sql_expression_factory_;
  ObExprOperatorFactory expr_op_factory_;
  stmt::StmtType literal_stmt_type_;  // reference: ObBasicStmt

  ObPhyPlanType plan_type_;
  ObPhyPlanType location_type_;
  bool require_local_execution_;  // not need serialize
  bool use_px_;
  int64_t px_dop_;
  uint32_t next_phy_operator_id_;   // share val
  uint32_t next_expr_operator_id_;  // share val
  // for regexp expression's compilation
  int16_t regexp_op_count_;
  // for like expression's optimization
  int16_t like_op_count_;
  // for px fast path
  int16_t px_exchange_out_op_count_;
  bool is_sfu_;
  // if the stmt  contains user variable assignment
  // such as @a:=123
  // we may need to serialize the map to remote server
  bool is_contains_assignment_;
  bool affected_last_insert_id_;  // not need serialize
  bool is_affect_found_row_;      // not need serialize
  bool has_top_limit_;            // not need serialize
  bool is_wise_join_;             // not need serialize
  bool contain_table_scan_;       // whether contains primary key scan
  bool has_nested_sql_;
  uint64_t session_id_;  // when plan has tempoary table, records table_schema->session_id
                         // so as to judge whether this plan is reusable
  bool contain_oracle_trx_level_temporary_table_;
  bool contain_oracle_session_level_temporary_table_;

  // for outline use
  ObOutlineState outline_state_;
  int64_t concurrent_num_;
  int64_t max_concurrent_num_;  // -1 represents no limit
  // for plan cache, not need serialize
  TableLocationFixedArray table_locations_;

  PhyRowParamMap row_param_map_;
  bool is_update_uniq_index_;
  bool is_contain_global_index_;

  // For dist plan, base table's location constraints including TABLE_SCAN's and INSERT's
  ObPlanLocationConstraint base_constraints_;
  PlanPwjConstraintArray strict_constrinats_;
  PlanPwjConstraintArray non_strict_constrinats_;

public:
  ObExprFrameInfo expr_frame_info_;

  ObPlanStat stat_;
  ObPhyOperatorStats op_stats_;
  const int64_t MAX_BINARY_CODE_LEN = 1024 * 256;  // 256k
  ObLoctionSensitiveHint loc_sensitive_hint_;

private:
  bool is_returning_;
  bool is_late_materialized_;
  // **** for spm ****
  bool is_dep_base_table_;
  bool is_insert_select_;
  // **** for spm end ***
  bool contain_paramed_column_field_;
  int64_t first_array_index_;
  bool need_consistent_snapshot_;
  bool is_batched_multi_stmt_;
#ifndef NDEBUG
public:
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator> bit_set_;
#endif
  int64_t is_new_engine_;
  bool use_pdml_;  // is parallel dml plan
  bool use_temp_table_;
  bool has_link_table_;
  // table with mocked rowid column
  common::ObFixedArray<uint64_t, common::ObIAllocator> mock_rowid_tables_;
  bool need_serial_exec_;  // mark if need serial execute?
  bool temp_sql_can_prepare_;
};

inline void ObPhysicalPlan::set_affected_last_insert_id(bool affected_last_insert_id)
{
  affected_last_insert_id_ = affected_last_insert_id;
}
inline bool ObPhysicalPlan::is_affected_last_insert_id() const
{
  return affected_last_insert_id_;
}
inline int32_t* ObPhysicalPlan::alloc_projector(int64_t projector_size)
{
  return static_cast<int32_t*>(allocator_.alloc(sizeof(int32_t) * projector_size));
}

inline ObPhyPlanType ObPhysicalPlan::get_location_type() const
{
  ObPhyPlanType location_type = location_type_;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000 && OB_PHY_PLAN_UNINITIALIZED == location_type) {
    location_type = plan_type_;
  }
  return location_type;
}

template <typename T>
inline int ObPhysicalPlan::alloc_operator_by_type(ObPhyOperatorType phy_operator_type, T*& phy_op, const uint64_t op_id)
{
  int ret = common::OB_SUCCESS;
  ObPhyOperator* op = NULL;
  if (OB_FAIL(op_factory_.alloc(phy_operator_type, op))) {
    OB_LOG(WARN, "fail to alloc phy_operator", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = common::OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "operator is null", K(op));
  } else {
    uint32_t tmp_op_id = UINT32_MAX;
    if (OB_INVALID_ID != op_id) {
      tmp_op_id = op_id;
    } else {
      tmp_op_id = next_phy_operator_id_++;
    }
#ifndef NDEBUG
    if (bit_set_.has_member(tmp_op_id)) {
      ret = common::OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpect status: op id exists", K(op), K(tmp_op_id));
    } else {
      bit_set_.add_member(tmp_op_id);
    }
#endif
    op->set_id(tmp_op_id);
    op->set_phy_plan(this);
    phy_op = static_cast<T*>(op);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_PHYSICAL_PLAN_H
