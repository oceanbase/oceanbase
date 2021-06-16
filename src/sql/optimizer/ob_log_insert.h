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

#ifndef _OB_LOG_INSERT_H
#define _OB_LOG_INSERT_H 1
#include "ob_logical_operator.h"
#include "ob_log_del_upd.h"
#include "sql/resolver/dml/ob_insert_stmt.h"

namespace oceanbase {
namespace sql {
class ObSelectLogPlan;
class ObPartIdRowMapManager;
class ObLogDupKeyChecker {
public:
  ObLogDupKeyChecker()
      : unique_index_cnt_(0),
        gui_lookup_root_(NULL),
        gui_lookup_calc_part_expr_(NULL),
        table_scan_root_(NULL),
        tsc_calc_part_expr_(NULL),
        constraint_infos_(NULL),
        gui_scan_roots_()
  {}

  void set_gui_lookup_root(ObLogicalOperator* gui_lookup_root)
  {
    gui_lookup_root_ = gui_lookup_root;
  }
  ObLogicalOperator* get_gui_lookup_root()
  {
    return gui_lookup_root_;
  }
  int add_gui_scan_root(ObLogicalOperator* gui_scan_root)
  {
    return gui_scan_roots_.push_back(gui_scan_root);
  }
  void set_table_scan_root(ObLogicalOperator* table_scan_root)
  {
    table_scan_root_ = table_scan_root;
  }
  int add_gui_scan_calc_part_exprs(ObRawExpr* expr)
  {
    return gui_scan_calc_part_exprs_.push_back(expr);
  }
  const common::ObIArray<ObRawExpr*>& get_gui_scan_calc_part_exprs() const
  {
    return gui_scan_calc_part_exprs_;
  }
  void set_gui_lookup_calc_part_expr(ObRawExpr* expr)
  {
    gui_lookup_calc_part_expr_ = expr;
  }
  ObRawExpr* get_gui_lookup_calc_part_expr() const
  {
    return gui_lookup_calc_part_expr_;
  }
  void set_tsc_calc_part_expr(ObRawExpr* expr)
  {
    tsc_calc_part_expr_ = expr;
  }
  ObRawExpr* get_tsc_calc_part_expr() const
  {
    return tsc_calc_part_expr_;
  }
  common::ObIArray<ObLogicalOperator*>& get_gui_scan_roots()
  {
    return gui_scan_roots_;
  }
  ObLogicalOperator* get_table_scan_root()
  {
    return table_scan_root_;
  }
  void set_unique_index_cnt(int64_t unique_index_cnt)
  {
    unique_index_cnt_ = unique_index_cnt;
  }
  int64_t get_unique_index_cnt() const
  {
    return unique_index_cnt_;
  }
  void set_constraint_infos(const common::ObIArray<ObUniqueConstraintInfo>* constraint_infos)
  {
    constraint_infos_ = constraint_infos;
  }
  const common::ObIArray<ObUniqueConstraintInfo>* get_constraint_infos() const
  {
    return constraint_infos_;
  }

  TO_STRING_KV(
      K_(unique_index_cnt), KPC_(gui_lookup_root), KPC_(table_scan_root), K_(gui_scan_roots), KPC_(constraint_infos));

private:
  int64_t unique_index_cnt_;
  ObLogicalOperator* gui_lookup_root_;
  ObRawExpr* gui_lookup_calc_part_expr_;
  ObLogicalOperator* table_scan_root_;
  ObRawExpr* tsc_calc_part_expr_;
  const common::ObIArray<ObUniqueConstraintInfo>* constraint_infos_;
  common::ObSEArray<ObLogicalOperator*, 8, common::ModulePageAllocator, true> gui_scan_roots_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> gui_scan_calc_part_exprs_;
};

class ObLogInsert : public ObLogDelUpd {
public:
  ObLogInsert(ObLogPlan& plan)
      : ObLogDelUpd(plan),
        is_replace_(false),
        low_priority_(false),
        high_priority_(false),
        delayed_(false),
        //        table_columns_(NULL),
        value_columns_(NULL),
        column_convert_exprs_(NULL),
        primary_keys_(NULL),
        // value_vectors_(NULL),
        insert_up_(false),
        only_one_unique_key_(false),
        tables_assignments_(NULL),
        is_insert_select_(false)
  {}

  virtual ~ObLogInsert()
  {}

  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    out = NULL;
    return common::OB_SUCCESS;
  }

  virtual int allocate_exchange_post(AllocExchContext* ctx) override;

  int allocate_exchange_post_pdml(AllocExchContext* ctx);

  int get_right_key(ObIArray<ObRawExpr*>& part_keys, const ObIArray<ObRawExpr*>& child_output_expr,
      const common::ObIArray<ObColumnRefRawExpr*>& columns, ObIArray<ObRawExpr*>& right_keys);

  virtual int inner_replace_generated_agg_expr(
      const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs) override;

  const char* get_name() const;

  int calc_cost();
  inline const common::ObIArray<ObRawExpr*>* get_column_convert_exprs() const
  {
    return column_convert_exprs_;
  }
  void set_value_columns(const common::ObIArray<ObColumnRefRawExpr*>* value_columns)
  {
    value_columns_ = value_columns;
  }
  void set_column_convert_exprs(const common::ObIArray<ObRawExpr*>* column_convert_exprs)
  {
    column_convert_exprs_ = column_convert_exprs;
  }
  void set_primary_key_ids(const common::ObIArray<uint64_t>* primary_key)
  {
    primary_keys_ = primary_key;
  }
  const common::ObIArray<uint64_t>* get_primary_key_ids()
  {
    return primary_keys_;
  }
  void set_only_one_unique_key(bool only_one)
  {
    only_one_unique_key_ = only_one;
  }
  bool is_only_one_unique_key() const
  {
    return only_one_unique_key_;
  }
  void set_replace(bool replace)
  {
    is_replace_ = replace;
  }

  bool is_replace() const
  {
    return is_replace_;
  }

  /**
   *  Add needed expr to context
   *
   *  For DELETE, we just need the rowkey columns.
   */
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  int gen_calc_part_id_expr(uint64_t table_id, uint64_t ref_table_id, ObRawExpr*& expr);
  void set_part_hint(const ObPartHint* part_hint)
  {
    part_hint_ = part_hint;
  }
  const ObPartHint* get_part_hint() const
  {
    return part_hint_;
  }
  void set_insert_up(bool insert_up)
  {
    insert_up_ = insert_up;
  }
  bool get_insert_up()
  {
    return insert_up_;
  }
  void set_tables_assignments(const ObTablesAssignments* assigns)
  {
    tables_assignments_ = assigns;
  }
  const ObTablesAssignments* get_tables_assignments() const
  {
    return tables_assignments_;
  }
  void set_is_insert_select(bool v)
  {
    is_insert_select_ = v;
  }
  bool is_insert_select() const
  {
    return is_insert_select_;
  }
  /**
   *  Get the hash value of the INSERT operator
   */
  virtual uint64_t hash(uint64_t seed) const;
  int generate_sharding_info(ObShardingInfo& target_sharding_info);
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  int calculate_table_location();

  int get_join_keys(const AllocExchContext& ctx, ObIArray<ObRawExpr*>& target_keys, ObIArray<ObRawExpr*>& source_keys);
  ObPartIdRowMapManager* get_part_row_map();
  ObLogDupKeyChecker& get_dupkey_checker()
  {
    return dupkey_checker_;
  }

  int check_if_match_partition_wise_insert(const AllocExchContext& ctx, const ObShardingInfo& target_sharding_info,
      const ObShardingInfo& source_sharding_info, bool& is_part_wise);

  virtual int extract_value_exprs();

  const ObIArray<ObRawExpr*>& get_value_exprs() const
  {
    return value_exprs_;
  }

private:
  void calc_phy_location_type();
  int set_hash_dist_column_exprs(ObExchangeInfo& exch_info, uint64_t index_tid) const;

protected:
  int add_exprs_without_column_conv(
      const common::ObIArray<ObRawExpr*>& src_exprs, common::ObIArray<ObRawExpr*>& dst_exprs);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed) override;
  bool is_table_update_part_key() const;
  bool is_table_insert_sequence_part_key() const;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);

protected:
  bool is_replace_;
  /**
   * @note These fields are added for the compatiblity of MySQL syntax and are not
   * supported at the moment.
   */
  bool low_priority_;
  bool high_priority_;
  bool delayed_;
  const common::ObIArray<ObColumnRefRawExpr*>* value_columns_;
  const common::ObIArray<ObRawExpr*>* column_convert_exprs_;
  const common::ObIArray<uint64_t>* primary_keys_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> value_exprs_;
  bool insert_up_;
  bool only_one_unique_key_;
  const ObTablesAssignments* tables_assignments_;
  ObLogDupKeyChecker dupkey_checker_;
  // for SPM Pruning
  bool is_insert_select_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
