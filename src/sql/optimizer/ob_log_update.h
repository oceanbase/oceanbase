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

#ifndef _OB_LOG_UPDATE_H
#define _OB_LOG_UPDATE_H 1
#include "ob_log_del_upd.h"

namespace oceanbase {
namespace sql {
class ObLogUpdate : public ObLogDelUpd {
public:
  ObLogUpdate(ObLogPlan& plan) : ObLogDelUpd(plan), tables_assignments_(NULL), update_set_(false)
  {}
  virtual ~ObLogUpdate()
  {}

  int calc_cost();
  void set_tables_assignments(const ObTablesAssignments* assigns)
  {
    tables_assignments_ = assigns;
  }
  const ObTablesAssignments* get_tables_assignments() const
  {
    return tables_assignments_;
  }

  virtual int copy_without_child(ObLogicalOperator*& out);
  int allocate_exchange_post(AllocExchContext* ctx);

  virtual int est_cost();

  virtual uint64_t hash(uint64_t seed) const;
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual const char* get_name() const;
  void set_update_set(bool update_set)
  {
    update_set_ = update_set;
  }
  bool is_update_set()
  {
    return update_set_;
  }

  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

  int get_update_table(ObDMLStmt* stmt, ObColumnRefRawExpr* col_expr, uint64_t& ref_id);

  virtual int need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed) override;

private:
  // MySQL only, https://dev.mysql.com/doc/refman/8.0/en/update.html
  const ObTablesAssignments* tables_assignments_;
  // update ... set (a,b) = (subquery), (d,e) = (subquery)
  bool update_set_;
  DISALLOW_COPY_AND_ASSIGN(ObLogUpdate);
};
}  // namespace sql
}  // namespace oceanbase
#endif
