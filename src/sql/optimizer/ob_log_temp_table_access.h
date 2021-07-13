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

#ifndef OCEANBASE_SQL_OB_LOG_TEMP_TABLE_ACCESS_H
#define OCEANBASE_SQL_OB_LOG_TEMP_TABLE_ACCESS_H 1

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/resolver/dml/ob_sql_hint.h"

namespace oceanbase {
namespace sql {
class ObLogTempTableAccess : public ObLogicalOperator {
public:
  ObLogTempTableAccess(ObLogPlan& plan);
  virtual ~ObLogTempTableAccess();
  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    return clone(out);
  }
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int transmit_op_ordering() override;
  virtual bool is_block_op() const override
  {
    return true;
  }
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline void set_ref_table_id(uint64_t ref_table_id)
  {
    ref_table_id_ = ref_table_id;
  }
  uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }
  void set_last_access(bool is_last)
  {
    is_last_access_ = is_last;
  }
  bool is_last_access() const
  {
    return is_last_access_;
  }
  int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  inline common::ObString& get_table_name()
  {
    return temp_table_name_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_access_exprs() const
  {
    return access_exprs_;
  }
  inline common::ObIArray<ObRawExpr*>& get_access_exprs()
  {
    return access_exprs_;
  }
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTempTableAccess);

private:
  uint64_t table_id_;
  uint64_t ref_table_id_;
  common::ObString temp_table_name_;
  bool is_last_access_;  // trick part, used to release the memory
  // base columns to scan
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_TEMP_TABLE_ACCESS_H
