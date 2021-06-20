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

#ifndef _OB_LOG_FOR_UPDATE_H
#define _OB_LOG_FOR_UPDATE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_del_upd.h"
namespace oceanbase {
namespace sql {
class ObLogForUpdate : public ObLogDelUpd {
public:
  ObLogForUpdate(ObLogPlan& plan);
  virtual ~ObLogForUpdate()
  {}
  virtual const char* get_name() const;
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int allocate_expr_pre(ObAllocExprContext& ctx);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  uint64_t hash(uint64_t seed) const;
  virtual int est_cost();
  virtual int compute_op_ordering();
  virtual bool modify_multi_tables() const;
  virtual uint64_t get_target_table_id() const;

  int add_for_update_table(uint64_t table_id, const ObIArray<ObColumnRefRawExpr*>& keys);
  const ObIArray<uint64_t>& get_for_update_tables() const
  {
    return lock_tables_;
  }

  void set_skip_locked(bool skip)
  {
    skip_locked_ = skip;
  }
  bool is_skip_locked() const
  {
    return skip_locked_;
  }

  void set_wait_ts(int64_t wait_ts)
  {
    wait_ts_ = wait_ts;
  }
  int64_t get_wait_ts() const
  {
    return wait_ts_;
  }

  int get_table_columns(const uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& table_cols) const;
  int get_part_columns(const uint64_t table_id, ObIArray<ObRawExpr*>& part_cols) const;
  int get_rowkey_exprs(const uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& rowkey) const;
  int create_calc_part_expr(const uint64_t table_id, ObRawExpr*& calc_part_expr);
  int get_calc_part_expr(const uint64_t table_id, ObRawExpr*& calc_part_expr) const;
  int is_rowkey_nullable(const uint64_t table_id, bool& is_nullable) const;
  int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

private:
  bool skip_locked_;
  int64_t wait_ts_;
  ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> lock_tables_;
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> calc_part_id_exprs_;
  ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> rowkeys_;
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> part_keys_;
  DISALLOW_COPY_AND_ASSIGN(ObLogForUpdate);
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // _OB_LOG_FOR_UPDATE_H
