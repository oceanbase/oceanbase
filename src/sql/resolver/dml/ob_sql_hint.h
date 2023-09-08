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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_SQL_HINT_
#define OCEANBASE_SQL_RESOLVER_DML_OB_SQL_HINT_
#include "lib/string/ob_string.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/resolver/dml/ob_hint.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObSelectStmt;
class ObSQLSessionInfo;
struct PlanText;
struct TableItem;

struct ObHints
{
  ObHints () : stmt_id_(OB_INVALID_STMT_ID) {}
  int assign(const ObHints &other);
  int print_hints(PlanText &plan_text, bool ignore_trans_hint = false) const;

  TO_STRING_KV(K_(stmt_id), K_(qb_name), K_(hints));

  int64_t stmt_id_;
  ObString qb_name_;
  ObSEArray<ObHint*, 8, common::ModulePageAllocator, true> hints_;
  bool operator < (const ObHints &r_hints) const { return stmt_id_ < r_hints.stmt_id_; }
};

struct QbNames {
  QbNames () : stmt_type_(stmt::StmtType::T_NONE),
               is_set_stmt_(false),
               is_from_hint_(false),
               parent_name_(),
               qb_names_() {}
  void reset();
  int print_qb_names(PlanText &plan_text) const;
  TO_STRING_KV(K_(stmt_type), K_(is_set_stmt),
                K_(is_from_hint), K_(parent_name),
                K_(qb_names));

  stmt::StmtType stmt_type_;
  bool is_set_stmt_;
  bool is_from_hint_; // true: qb_names_.at(0) is from qb_name hint
  ObString parent_name_; // parent stmt qb name in transform, empty for origin stmt
  ObSEArray<ObString, 1, common::ModulePageAllocator, true> qb_names_; // used qb names for one stmt
};

struct ObQueryHint {
  ObQueryHint() { reset(); }
  void reset();

  ObGlobalHint &get_global_hint() { return global_hint_; }
  const ObGlobalHint &get_global_hint() const { return global_hint_; }
  bool has_outline_data() const { return is_valid_outline_; }
  bool has_user_def_outline() const { return user_def_outline_; }

  int set_outline_data_hints(const ObGlobalHint &global_hint,
                             const int64_t stmt_id,
                             const ObIArray<ObHint*> &hints);
  static int get_qb_name_source_hash_value(const ObString &src_qb_name,
                                           const ObIArray<uint32_t> &src_hash_val,
                                           uint32_t &hash_val);
  int append_hints(int64_t stmt_id, const ObIArray<ObHint*> &hints);
  const ObHints* get_qb_hints(const ObString &qb_name) const;
  const ObHints* get_stmt_id_hints(int64_t stmt_id) const;
  int add_stmt_id_map(const int64_t stmt_id, stmt::StmtType stmt_type);
  int set_stmt_id_map_info(const ObDMLStmt &stmt, ObString &qb_name);
  int init_query_hint(ObIAllocator *allocator, ObSQLSessionInfo *session_info, ObDMLStmt *stmt);
  int check_and_set_params_from_hint(const ObResolverParams &params, const ObDMLStmt &stmt) const;
  int check_ddl_schema_version_from_hint(const ObDMLStmt &stmt) const;
  int check_ddl_schema_version_from_hint(const ObDMLStmt &stmt,
                                         const ObDDLSchemaVersionHint& ddlSchemaVersionHint) const;
  int distribute_hint_to_orig_stmt(ObDMLStmt *stmt);
  int adjust_qb_name_for_stmt(ObIAllocator &allocator,
                              ObDMLStmt &stmt,
                              const ObString &src_qb_name,
                              const ObIArray<uint32_t> &src_hash_val,
                              int64_t *sub_num = NULL);

  int generate_orig_stmt_qb_name(ObIAllocator &allocator);
  int generate_qb_name_for_stmt(ObIAllocator &allocator,
                                const ObDMLStmt &stmt,
                                const ObString &src_qb_name,
                                const ObIArray<uint32_t> &src_hash_val,
                                ObString &qb_name,
                                int64_t *sub_num = NULL);
  int try_add_new_qb_name(ObIAllocator &allocator,
                          int64_t stmt_id,
                          const char *ptr,
                          int64_t length,
                          int64_t &cnt,
                          ObString &qb_name);
  int reset_duplicate_qb_name();
  const char *get_dml_stmt_name(stmt::StmtType stmt_type, bool is_set_stmt) const;
  int append_id_to_stmt_name(char *buf, int64_t buf_len, int64_t &pos, int64_t &id_start);
  int get_qb_name(int64_t stmt_id, ObString &qb_name) const;
  int get_qb_name_counts(const int64_t stmt_count, ObIArray<int64_t> &qb_name_counts) const;
  int recover_qb_names(const ObIArray<int64_t> &qb_name_counts, int64_t &stmt_count);
  int fill_tables(const TableItem &table, ObIArray<ObTableInHint> &hint_tables) const;
  bool is_valid_outline_transform(int64_t trans_list_loc, const ObHint *cur_hint) const;
  const ObHint *get_outline_trans_hint(int64_t pos) const
  { return pos < 0 || pos >= trans_list_.count() ? NULL : trans_list_.at(pos); }
  bool all_trans_list_valid() const
  { return outline_trans_hints_.count() == trans_list_.count(); }

  // check hint
  int get_relids_from_hint_tables(const ObDMLStmt &stmt,
                                  const ObIArray<ObTableInHint> &tables,
                                  ObRelIds &rel_ids) const;
  int get_table_bit_index_by_hint_table(const ObDMLStmt &stmt,
                                        const ObTableInHint &table,
                                        int32_t &idx) const;
  int get_table_item_by_hint_table(const ObDMLStmt &stmt,
                                   const ObTableInHint &table,
                                   TableItem *&table_item) const;
  int get_basic_table_without_index_by_hint_table(const ObDMLStmt &stmt,
                                                  const ObTableInHint &table,
                                                  TableItem *&table_item) const;
  bool has_hint_exclude_concurrent() const {  return !qb_hints_.empty() || !stmt_id_hints_.empty()
                                                     || global_hint_.has_hint_exclude_concurrent(); }

  // print hint
  int print_stmt_hint(PlanText &plan_text, const ObDMLStmt &stmt, const bool is_first_stmt_for_hint) const;
  int print_outline_data(PlanText &plan_text) const;
  int print_qb_name_hints(PlanText &plan_text) const;
  int print_qb_name_hint(PlanText &plan_text, int64_t stmt_id) const;
  int print_transform_hints(PlanText &plan_text) const;
  inline static const char *get_outline_indent(bool is_oneline)
  { //6 space, align with 'Outputs & filters'
    return is_oneline ? " " : "\n      ";
  }
  template <typename HintType>
  static int create_hint(ObIAllocator *allocator, ObItemType hint_type, HintType *&hint);
  static int create_hint_table(ObIAllocator *allocator, ObTableInHint *&table);
  static int create_leading_table(ObIAllocator *allocator, ObLeadingTable *&table);

  TO_STRING_KV(K_(global_hint),
               K_(outline_stmt_id),
               K_(qb_hints),
               K_(stmt_id_hints),
               K_(trans_list),
               K_(outline_trans_hints),
               K_(used_trans_hints));

  ObCollationType cs_type_; // used when compare table name in hint

  ObGlobalHint global_hint_;
  bool is_valid_outline_;
  bool user_def_outline_;
  int64_t outline_stmt_id_;
  ObSEArray<ObHints, 8, common::ModulePageAllocator, true> qb_hints_;  // hints with qb name
  ObSEArray<ObHints, 8, common::ModulePageAllocator, true> stmt_id_hints_; // hints without qb name, used before transform
  ObSEArray<const ObHint*, 8, common::ModulePageAllocator, true> trans_list_; // transform hints from outline data, need keep order
  ObSEArray<const ObHint*, 8, common::ModulePageAllocator, true> outline_trans_hints_; // tranform hints to generate outline data
  ObSEArray<const ObHint*, 8, common::ModulePageAllocator, true> used_trans_hints_;
  ObSEArray<QbNames, 8, common::ModulePageAllocator, true> stmt_id_map_;	//	stmt id -> qb name list, position is stmt id
  hash::ObHashMap<ObString, int64_t> qb_name_map_;	// qb name -> stmt id

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryHint);
};

template <typename HintType>
int ObQueryHint::create_hint(ObIAllocator *allocator, ObItemType hint_type, HintType *&hint)
{
  int ret = common::OB_SUCCESS;
  hint = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(allocator) || OB_UNLIKELY(T_INVALID == hint_type)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected params",K(ret), K(allocator), K(hint_type));
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(HintType)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "no more memory to create hint");
  } else {
    hint = new(ptr) HintType(hint_type);
  }
  return ret;
}

// used in embedded hint transform
struct ObStmtHint
{
  ObStmtHint() { reset(); }
  void reset();
  int assign(const ObStmtHint &other);
  bool inited() const { return NULL != query_hint_; }
  int init_stmt_hint(const ObDMLStmt &stmt,
                     const ObQueryHint &query_hint,
                     bool use_stmt_id_hints);
  int print_stmt_hint(PlanText &plan_text) const;
  const ObHint *get_normal_hint(ObItemType hint_type, int64_t *idx = NULL) const;
  ObHint *get_normal_hint(ObItemType hint_type, int64_t *idx = NULL);
  void set_query_hint(const ObQueryHint *query_hint) { query_hint_ = query_hint; }
  int replace_name_for_single_table_view(ObIAllocator *allocator,
                                         const ObDMLStmt &stmt,
                                         const TableItem &view_table);
  int set_set_stmt_hint();
  int set_simple_view_hint(const ObStmtHint *other = NULL);
  int remove_normal_hints(const ObItemType *hint_array, const int64_t num);
  int merge_stmt_hint(const ObStmtHint &other, ObHintMergePolicy policy = HINT_DOMINATED_EQUAL);
  int merge_hint(ObHint &hint, ObHintMergePolicy policy, ObIArray<ObItemType> &conflict_hints);
  int merge_normal_hint(ObHint &hint, ObHintMergePolicy policy, ObIArray<ObItemType> &conflict_hints);
  int reset_explicit_trans_hint(ObItemType hint_type);
  int get_max_table_parallel(const ObDMLStmt &stmt, int64_t &max_table_parallel) const;


  bool has_enable_hint(ObItemType hint_type) const;
  bool has_disable_hint(ObItemType hint_type) const;
  const ObHint *get_no_rewrite_hint() const { return get_normal_hint(T_NO_REWRITE); }
  bool enable_no_rewrite() const { return has_enable_hint(T_NO_REWRITE); }
  bool enable_no_pred_deduce() const { return has_enable_hint(T_NO_REWRITE) || has_enable_hint(T_NO_PRED_DEDUCE); }

  DECLARE_TO_STRING;

  const ObQueryHint *query_hint_;
  ObSEArray<ObHint*, 8, common::ModulePageAllocator, true> normal_hints_;
  ObSEArray<ObHint*, 8, common::ModulePageAllocator, true> other_opt_hints_;

private:
  int64_t get_hint_count() const {
    return normal_hints_.count() + other_opt_hints_.count();
  }
  const ObHint *get_hint_by_idx(int64_t idx) const;
  int set_hint(int64_t idx, ObHint *hint);
  DISALLOW_COPY_AND_ASSIGN(ObStmtHint);
};

struct LogJoinHint
{
  LogJoinHint() : join_tables_(),
                  local_methods_(0),
                  dist_methods_(0),
                  slave_mapping_(NULL),
                  nl_material_(NULL),
                  local_method_hints_(),
                  dist_method_hints_() {}
  int assign(const LogJoinHint &other);
  int add_join_hint(const ObJoinHint &join_hint);
  int init_log_join_hint();

  TO_STRING_KV(K_(join_tables),
               K_(local_methods),
               K_(dist_methods),
               K_(slave_mapping),
               K_(nl_material),
               K_(local_method_hints),
               K_(dist_method_hints));

  ObRelIds join_tables_;
  int64_t local_methods_;
  int64_t dist_methods_;
  const ObJoinHint *slave_mapping_;
  const ObJoinHint *nl_material_;
  ObSEArray<const ObJoinHint*, 4, common::ModulePageAllocator, true> local_method_hints_;
  ObSEArray<const ObJoinHint*, 4, common::ModulePageAllocator, true> dist_method_hints_;
};

struct LogTableHint
{
  LogTableHint() :  table_(NULL),
                    parallel_hint_(NULL),
                    use_das_hint_(NULL),
                    dynamic_sampling_hint_(NULL),
                    is_ds_hint_conflict_(false) {}
  LogTableHint(const TableItem *table) :  table_(table),
                                          parallel_hint_(NULL),
                                          use_das_hint_(NULL),
                                          dynamic_sampling_hint_(NULL),
                                          is_ds_hint_conflict_(false) {}
  int assign(const LogTableHint &other);
  int init_index_hints(ObSqlSchemaGuard &schema_guard);
  bool is_use_index_hint() const { return !index_hints_.empty() && NULL != index_hints_.at(0)
                                          && index_hints_.at(0)->is_use_index_hint(); }
  bool is_valid() const { return !index_list_.empty() || NULL != parallel_hint_
                                || NULL != use_das_hint_ || !join_filter_hints_.empty()
                                || dynamic_sampling_hint_ != NULL; }
  int get_join_filter_hint(const ObRelIds &left_tables,
                           bool part_join_filter,
                           const ObJoinFilterHint *&hint) const;
  int get_join_filter_hints(const ObRelIds &left_tables,
                            bool part_join_filter,
                            ObIArray<const ObJoinFilterHint*> &hints) const;
  int add_join_filter_hint(const ObDMLStmt &stmt,
                           const ObQueryHint &query_hint,
                           const ObJoinFilterHint &hint);
  int allowed_skip_scan(const uint64_t index_id, bool &allowed) const;

  TO_STRING_KV(K_(table), K_(index_list), K_(index_hints),
               K_(parallel_hint), K_(use_das_hint),
               K_(join_filter_hints), K_(left_tables),
               KPC(dynamic_sampling_hint_), K(is_ds_hint_conflict_));

  const TableItem *table_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> index_list_;
  common::ObSEArray<const ObIndexHint*, 4, common::ModulePageAllocator, true> index_hints_;
  const ObTableParallelHint *parallel_hint_;
  const ObIndexHint *use_das_hint_;
  ObSEArray<const ObJoinFilterHint*, 1, common::ModulePageAllocator, true> join_filter_hints_;
  ObSEArray<ObRelIds, 1, common::ModulePageAllocator, true> left_tables_; // left table relids in join filter hint
  const ObTableDynamicSamplingHint *dynamic_sampling_hint_;
  bool is_ds_hint_conflict_;
};

struct LeadingInfo {
  TO_STRING_KV(K_(table_set),
                 K_(left_table_set),
                 K_(right_table_set));

  ObRelIds table_set_;
  ObRelIds left_table_set_;
  ObRelIds right_table_set_;
};

struct JoinFilterPushdownHintInfo
{
  int check_use_join_filter(const ObDMLStmt &stmt,
                            const ObQueryHint &query_hint,
                            uint64_t filter_table_id,
                            bool part_join_filter,
                            bool &can_use,
                            const ObJoinFilterHint *&force_hint) const;
  TO_STRING_KV(K_(filter_table_id),
                 K_(join_filter_hints),
                 K_(part_join_filter_hints));

  uint64_t filter_table_id_;
  bool config_disable_;
  common::ObSEArray<const ObJoinFilterHint*, 4, common::ModulePageAllocator, true> join_filter_hints_;
  common::ObSEArray<const ObJoinFilterHint*, 4, common::ModulePageAllocator, true> part_join_filter_hints_;
};

struct LogLeadingHint
{
  void reset() {
    leading_tables_.reuse();
    leading_infos_.reuse();
    hint_ = NULL;
  }

  int init_leading_info(const ObDMLStmt &stmt,
                        const ObQueryHint &query_hint,
                        const ObHint *hint);
  int init_leading_info_from_leading_hint(const ObDMLStmt &stmt,
                                          const ObQueryHint &query_hint,
                                          const ObLeadingTable &cur_table,
                                          ObRelIds& table_set);
  int init_leading_info_from_ordered_hint(const ObDMLStmt &stmt);
  int init_leading_info_from_table(const ObDMLStmt &stmt,
                                   ObIArray<LeadingInfo> &leading_infos,
                                   TableItem *table,
                                   ObRelIds &table_set);

  TO_STRING_KV(K_(leading_tables),
               K_(leading_infos),
               K_(hint));

  ObRelIds leading_tables_;
  common::ObSEArray<LeadingInfo, 8, common::ModulePageAllocator, true> leading_infos_;
  const ObJoinOrderHint *hint_;
};

struct ObLogPlanHint
{
  ObLogPlanHint() { reset(); }
  void reset();
  int init_normal_hints(const ObIArray<ObHint*> &normal_hints);
#ifndef OB_BUILD_SPM
  int init_log_plan_hint(ObSqlSchemaGuard &schema_guard,
                         const ObDMLStmt &stmt,
                         const ObQueryHint &query_hint);
#else
  int init_log_plan_hint(ObSqlSchemaGuard &schema_guard,
                         const ObDMLStmt &stmt,
                         const ObQueryHint &query_hint,
                         const bool is_spm_evolution);
#endif
  int init_other_opt_hints(ObSqlSchemaGuard &schema_guard,
                           const ObDMLStmt &stmt,
                           const ObQueryHint &query_hint,
                           const ObIArray<ObHint*> &hints);
  int init_log_table_hints(ObSqlSchemaGuard &schema_guard);
  int init_log_join_hints();
  int add_join_filter_hint(const ObDMLStmt &stmt,
                           const ObQueryHint &query_hint,
                           const ObJoinFilterHint &join_filter_hint);
  int add_table_parallel_hint(const ObDMLStmt &stmt,
                              const ObQueryHint &query_hint,
                              const ObTableParallelHint &table_parallel_hint);
  int add_table_dynamic_sampling_hint(const ObDMLStmt &stmt,
                                      const ObQueryHint &query_hint,
                                      const ObTableDynamicSamplingHint &table_ds_hint);
  int add_index_hint(const ObDMLStmt &stmt,
                     const ObQueryHint &query_hint,
                     const ObIndexHint &index_hint);
  int add_join_hint(const ObDMLStmt &stmt,
                    const ObQueryHint &query_hint,
                    const ObJoinHint &join_hint);
  int get_log_table_hint_for_update(const ObDMLStmt &stmt,
                                    const ObQueryHint &query_hint,
                                    const ObTableInHint &table,
                                    const bool basic_table_only,
                                    LogTableHint *&log_table_hint);
  int check_status() const;
  const LogTableHint* get_log_table_hint(uint64_t table_id) const;
  const LogTableHint* get_index_hint(uint64_t table_id) const;
  int64_t get_parallel(uint64_t table_id) const;
  const ObTableDynamicSamplingHint* get_dynamic_sampling_hint(uint64_t table_id) const;
  int check_use_join_filter(uint64_t filter_table_id,
                            const ObRelIds &left_tables,
                            bool part_join_filter,
                            bool config_disable,
                            bool &can_use,
                            const ObJoinFilterHint *&force_hint) const;
  int get_pushdown_join_filter_hints(uint64_t filter_table_id,
                                     const ObRelIds &left_tables,
                                     bool config_disable,
                                     JoinFilterPushdownHintInfo& info) const;
  int check_use_das(uint64_t table_id, bool &force_das, bool &force_no_das) const;
  int check_use_skip_scan(uint64_t table_id,  uint64_t index_id,
                          bool &force_skip_scan,
                          bool &force_no_skip_scan) const;
  const LogJoinHint* get_join_hint(const ObRelIds &join_tables) const;
  const ObIArray<LogJoinHint> &get_join_hints() const { return join_hints_; }
  SetAlgo get_valid_set_algo() const;
  DistAlgo get_valid_set_dist_algo(int64_t *random_none_idx = NULL) const;
  int check_valid_set_left_branch(const ObSelectStmt *select_stmt,
                                  bool &hint_valid,
                                  bool &need_swap) const;
  const ObHint* get_normal_hint(ObItemType hint_type) const;
  bool has_enable_hint(ObItemType hint_type) const;
  bool has_disable_hint(ObItemType hint_type) const;
  bool use_join_filter(const ObRelIds &table_set) const;
  bool no_use_join_filter(const ObRelIds &table_set) const;
  int get_aggregation_info(bool &force_use_hash,
                           bool &force_use_merge,
                           bool &force_part_sort,
                           bool &force_normal_sort) const;

  bool use_late_material() const { return has_enable_hint(T_USE_LATE_MATERIALIZATION); }
  bool no_use_late_material() const { return has_disable_hint(T_USE_LATE_MATERIALIZATION); }
  bool use_hash_aggregate() const { return has_enable_hint(T_USE_HASH_AGGREGATE); }
  bool use_merge_aggregate() const { return has_disable_hint(T_USE_HASH_AGGREGATE); }
  bool pushdown_group_by() const { return has_enable_hint(T_GBY_PUSHDOWN); }
  bool no_pushdown_group_by() const { return has_disable_hint(T_GBY_PUSHDOWN); }
  bool use_hash_distinct() const { return has_enable_hint(T_USE_HASH_DISTINCT); }
  bool use_merge_distinct() const { return has_disable_hint(T_USE_HASH_DISTINCT); }
  bool pushdown_distinct() const { return has_enable_hint(T_DISTINCT_PUSHDOWN); }
  bool no_pushdown_distinct() const { return has_disable_hint(T_DISTINCT_PUSHDOWN); }
  bool use_distributed_dml() const { return has_enable_hint(T_USE_DISTRIBUTED_DML); }
  bool no_use_distributed_dml() const { return has_disable_hint(T_USE_DISTRIBUTED_DML); }

  const ObWindowDistHint *get_window_dist() const;

  TO_STRING_KV(K_(is_outline_data), K_(join_order),
               K_(table_hints), K_(join_hints),
               K_(normal_hints));

  bool is_outline_data_;
#ifdef OB_BUILD_SPM
  bool is_spm_evolution_;
#endif
  LogLeadingHint join_order_;
  common::ObSEArray<LogTableHint, 4, common::ModulePageAllocator, true> table_hints_;
  common::ObSEArray<LogJoinHint, 8, common::ModulePageAllocator, true> join_hints_;
  common::ObSEArray<const ObHint*, 8, common::ModulePageAllocator, true> normal_hints_;
};

}
}

#endif
