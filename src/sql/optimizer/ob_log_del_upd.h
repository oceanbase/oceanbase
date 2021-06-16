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

#ifndef _OB_LOG_DEL_UPD_H
#define _OB_LOG_DEL_UPD_H 1
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "ob_logical_operator.h"

namespace oceanbase {
namespace sql {

/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_COLUMNS(columns, type)                                     \
  {                                                                              \
    BUF_PRINTF(#columns "(");                                                    \
    if (columns == NULL) {                                                       \
      BUF_PRINTF("nil");                                                         \
    } else if (columns->count() == 0) {                                          \
      BUF_PRINTF("nil");                                                         \
    } else {                                                                     \
      int64_t N = columns->count();                                              \
      for (int64_t i = 0; i < N; i++) {                                          \
        BUF_PRINTF("[");                                                         \
        pos += columns->at(i).to_explain_string(buf + pos, buf_len - pos, type); \
        BUF_PRINTF("]");                                                         \
        if (i < N - 1) {                                                         \
          BUF_PRINTF(", ");                                                      \
        }                                                                        \
      }                                                                          \
    }                                                                            \
    BUF_PRINTF(")");                                                             \
  }

class ObLogDelUpd : public ObLogicalOperator {
public:
  ObLogDelUpd(ObLogPlan& plan);
  virtual ~ObLogDelUpd() = default;
  inline const common::ObIArray<TableColumns>* get_all_table_columns()
  {
    return all_table_columns_;
  }
  int add_table_columns_to_ctx(ObAllocExprContext& ctx);
  void set_all_table_columns(const common::ObIArray<TableColumns>* all_table_columns)
  {
    all_table_columns_ = all_table_columns;
    if (nullptr != all_table_columns_ && !all_table_columns_->empty() &&
        !all_table_columns_->at(0).index_dml_infos_.empty()) {
      table_columns_ = &(all_table_columns_->at(0).index_dml_infos_.at(0).column_exprs_);
    } else {
      SQL_LOG(INFO,
          "empty table_columns",
          "all_table_columns",
          all_table_columns_->empty(),
          "index_infos cnt",
          all_table_columns_->empty() ? 0 : all_table_columns_->at(0).index_dml_infos_.count());
    }
  }
  void set_lock_row_flag_expr(ObRawExpr* expr)
  {
    lock_row_flag_expr_ = expr;
  }
  ObRawExpr* get_lock_row_flag_expr() const
  {
    return lock_row_flag_expr_;
  }
  inline const common::ObIArray<ObRawExpr*>* get_check_constraint_exprs()
  {
    return check_constraint_exprs_;
  }
  void set_check_constraint_exprs(const common::ObIArray<ObRawExpr*>* check_constraint_exprs)
  {
    check_constraint_exprs_ = check_constraint_exprs;
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual uint64_t get_hash(uint64_t seed) const
  {
    return seed;
  }
  virtual uint64_t hash(uint64_t seed) const;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int reordering_project_columns() override;
  void set_ignore(bool is_ignore)
  {
    ignore_ = is_ignore;
  }
  bool is_ignore() const
  {
    return ignore_;
  }
  void set_is_returning(bool is)
  {
    is_returning_ = is;
  }
  bool is_returning() const
  {
    return is_returning_;
  }
  bool has_global_index() const
  {
    return stmt_ != NULL && static_cast<ObDelUpdStmt*>(stmt_)->has_global_index();
  }
  bool has_sequence() const
  {
    return stmt_ != NULL && static_cast<ObDelUpdStmt*>(stmt_)->has_sequence();
  }
  bool is_multi_part_dml() const
  {
    return is_multi_part_dml_;
  }
  void set_is_multi_part_dml(bool is_multi_part_dml)
  {
    is_multi_part_dml_ = is_multi_part_dml;
  }
  bool is_pdml() const
  {
    return is_pdml_;
  }
  void set_is_pdml(bool is_pdml)
  {
    is_pdml_ = is_pdml;
  }
  bool need_barrier() const
  {
    return need_barrier_;
  }
  void set_need_barrier(bool need_barrier)
  {
    need_barrier_ = need_barrier;
  }
  void set_first_dml_op(bool is_first_dml_op)
  {
    is_first_dml_op_ = is_first_dml_op;
  }
  bool is_first_dml_op() const
  {
    return is_first_dml_op_;
  }
  void set_index_maintenance(bool is_index_maintenance)
  {
    is_index_maintenance_ = is_index_maintenance;
  }
  bool is_index_maintenance() const
  {
    return is_index_maintenance_;
  }
  void set_table_location_uncertain(bool uncertain)
  {
    table_location_uncertain_ = uncertain;
  }
  bool is_table_location_uncertain() const
  {
    return table_location_uncertain_;
  }
  uint64_t get_loc_table_id() const;
  uint64_t get_index_tid() const;
  uint64_t get_table_id() const;
  const ObString get_index_table_name() const;
  bool is_update_op() const
  {
    return log_op_def::LOG_UPDATE == get_type();
  }
  int get_source_sharding_info(ObLogicalOperator* child_op, uint64_t source_table_id, ObShardingInfo*& sharding_info,
      const ObPhyTableLocationInfo*& phy_table_locaion_info);
  int check_match_source_sharding_info(ObLogicalOperator* child_op, uint64_t source_table_id, bool& is_match);
  void set_gi_above(bool is_gi_above)
  {
    gi_charged_ = is_gi_above;
  }
  bool is_gi_above() const
  {
    return gi_charged_;
  }
  int check_rowkey_distinct();
  const ObRawExpr* get_stmt_id_expr() const
  {
    return stmt_id_expr_;
  }
  inline const common::ObIArray<ObColumnRefRawExpr*>* get_table_columns() const
  {
    return table_columns_;
  }
  void set_table_columns(const common::ObIArray<ObColumnRefRawExpr*>* table_columns)
  {
    table_columns_ = table_columns;
  }
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

  ObTablePartitionInfo& get_table_partition_info()
  {
    return table_partition_info_;
  }
  const ObTablePartitionInfo& get_table_partition_info() const
  {
    return table_partition_info_;
  }
  void set_part_hint(const ObPartHint* part_hint)
  {
    part_hint_ = part_hint;
  }
  const ObPartHint* get_part_hint() const
  {
    return part_hint_;
  }
  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  int find_all_tsc(common::ObIArray<ObLogicalOperator*>& tsc_ops, ObLogicalOperator* root);
  void set_pdml_is_returning(bool v)
  {
    pdml_is_returning_ = v;
  }
  bool pdml_is_returning()
  {
    return pdml_is_returning_;
  }
  bool has_part_id_expr() const
  {
    return nullptr != pdml_partition_id_expr_;
  }
  ObRawExpr* get_partition_id_expr()
  {
    return pdml_partition_id_expr_;
  }

  virtual bool modify_multi_tables() const;
  virtual uint64_t get_target_table_id() const;
  inline ObTableLocationType get_phy_location_type() const
  {
    return table_phy_location_type_;
  }

protected:
  int generate_table_sharding_info(uint64_t loc_table_id, uint64_t ref_table_id, const ObPartHint* part_hint,
      ObTablePartitionInfo& table_partition_info, ObShardingInfo& sharding_info);
  int calculate_table_location(uint64_t loc_table_id, uint64_t ref_table_id, const ObPartHint* part_hint,
      ObTablePartitionInfo& table_partition_info);
  int alloc_partition_id_expr(ObAllocExprContext& ctx);
  int alloc_shadow_pk_column_for_pdml(ObAllocExprContext& ctx);
  virtual int need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed);
  int check_multi_table_dml_for_px(AllocExchContext& ctx, ObShardingInfo* source_sharding,
      ObShardingInfo& sharding_info, const ObPhyTableLocationInfo* phy_table_locaion_info, bool& is_needed);
  int add_all_table_assignments_to_ctx(const ObTablesAssignments* tables_assignments, ObAllocExprContext& ctx);

private:
  int get_modify_table_id(uint64_t& table_id) const;
  int allocate_exchange_post_non_pdml(AllocExchContext* ctx);
  int allocate_exchange_post_pdml(AllocExchContext* ctx);
  int check_pdml_need_exchange(AllocExchContext* ctx, ObShardingInfo& target_sharding_info, bool& need_exchange);
  int do_reordering_project_columns(ObLogicalOperator& child);

protected:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int add_exprs_to_ctx_for_pdml(
      ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, uint64_t producer_id);

  int check_multi_table_dml_for_nested_execution(bool& is_needed);
  int set_hash_dist_column_exprs(ObExchangeInfo& exch_info, uint64_t index_id) const;

protected:
  const common::ObIArray<TableColumns>* all_table_columns_;
  const common::ObIArray<ObRawExpr*>* check_constraint_exprs_;
  const common::ObIArray<ObColumnRefRawExpr*>* table_columns_;
  const ObPartHint* part_hint_;
  ObTablePartitionInfo table_partition_info_;

  ObRawExpr* stmt_id_expr_;
  ObRawExpr* lock_row_flag_expr_;  // used for update
  bool ignore_;
  bool is_returning_;
  bool is_multi_part_dml_;
  bool is_pdml_;
  bool gi_charged_;
  bool is_index_maintenance_;
  bool need_barrier_;
  bool is_first_dml_op_;
  ObTableLocationType table_phy_location_type_;
  bool table_location_uncertain_;

private:
  ObRawExpr* pdml_partition_id_expr_;
  bool pdml_is_returning_;

protected:
  bool need_alloc_part_id_expr_;
};
}  // namespace sql
}  // namespace oceanbase
#endif
