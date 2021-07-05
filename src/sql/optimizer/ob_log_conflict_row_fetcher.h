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

#ifndef OBDEV_SRC_SQL_OPTIMIZER_OB_LOG_CONFLICT_ROW_FETCHER_H_
#define OBDEV_SRC_SQL_OPTIMIZER_OB_LOG_CONFLICT_ROW_FETCHER_H_
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogConflictRowFetcher : public ObLogicalOperator {
public:
  ObLogConflictRowFetcher(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        only_data_table_(false)
  {}
  virtual ~ObLogConflictRowFetcher()
  {}
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int copy_without_child(ObLogicalOperator*& out) override
  {
    return clone(out);
  }
  int add_access_expr(ObColumnRefRawExpr* access_expr)
  {
    return access_exprs_.push_back(access_expr);
  }
  const common::ObIArray<ObColumnRefRawExpr*>& get_access_exprs() const
  {
    return access_exprs_;
  }
  common::ObIArray<ObColumnRefRawExpr*>& get_access_exprs()
  {
    return access_exprs_;
  }
  const common::ObIArray<ObColumnRefRawExpr*>& get_conflict_exprs() const
  {
    return conflict_exprs_;
  }
  common::ObIArray<ObColumnRefRawExpr*>& get_conflict_exprs()
  {
    return conflict_exprs_;
  }
  uint64_t hash(uint64_t seed) const;
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  void set_index_tid(uint64_t index_tid)
  {
    index_tid_ = index_tid;
  }
  uint64_t get_index_tid()
  {
    return index_tid_;
  }
  bool get_only_data_table()
  {
    return only_data_table_;
  }
  void set_only_data_table(bool only_data_table)
  {
    only_data_table_ = only_data_table;
  }
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;
  TO_STRING_KV(K_(table_id), K_(index_tid), K_(only_data_table), K_(conflict_exprs), K_(access_exprs));

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

private:
  uint64_t table_id_;
  uint64_t index_tid_;
  bool only_data_table_;
  common::ObSEArray<ObColumnRefRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  common::ObSEArray<ObColumnRefRawExpr*, 4, common::ModulePageAllocator, true> conflict_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogConflictRowFetcher);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_OPTIMIZER_OB_LOG_CONFLICT_ROW_FETCHER_H_ */
