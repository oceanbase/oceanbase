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

#ifndef OCEANBASE_SQL_OB_LOG_TABLE_LOOKUP_H
#define OCEANBASE_SQL_OB_LOG_TABLE_LOOKUP_H

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_table_scan.h"

namespace oceanbase {
namespace sql {

class Path;
class ObLogTableLookup : public ObLogicalOperator {
public:
  ObLogTableLookup(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        table_id_(common::OB_INVALID_ID),
        ref_table_id_(common::OB_INVALID_ID),
        index_id_(common::OB_INVALID_ID),
        table_name_(),
        index_name_(),
        index_back_scan_(NULL),
        calc_part_id_expr_(NULL)
  {}
  virtual ~ObLogTableLookup()
  {}
  virtual int allocate_expr_post(ObAllocExprContext& ctx);
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int compute_property(Path* path);
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int transmit_op_ordering();
  virtual uint64_t hash(uint64_t seed) const;
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  inline void set_table_id(const uint64_t table_id)
  {
    table_id_ = table_id;
  }
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline void set_ref_table_id(const uint64_t ref_table_id)
  {
    ref_table_id_ = ref_table_id;
  }
  inline uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }
  inline void set_index_id(const uint64_t index_id)
  {
    index_id_ = index_id;
  }
  inline uint64_t get_index_id() const
  {
    return index_id_;
  }
  inline void set_table_name(common::ObString& table_name)
  {
    table_name_ = table_name;
  }
  inline const common::ObString& get_table_name() const
  {
    return table_name_;
  }
  inline void set_index_name(common::ObString& index_name)
  {
    index_name_ = index_name;
  }
  inline const common::ObString& get_index_name() const
  {
    return index_name_;
  }
  inline void set_index_back_scan(ObLogTableScan* index_back_scan)
  {
    index_back_scan_ = index_back_scan;
  }
  inline ObLogTableScan* get_index_back_scan() const
  {
    return index_back_scan_;
  }
  const ObRawExpr* get_calc_part_id_expr() const
  {
    return calc_part_id_expr_;
  }
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  int init_calc_part_id_expr();
  int replace_gen_column(ObRawExpr* part_expr, ObRawExpr*& new_part_expr);

private:
  uint64_t table_id_;
  uint64_t ref_table_id_;
  uint64_t index_id_;
  common::ObString table_name_;
  common::ObString index_name_;
  ObLogTableScan* index_back_scan_;
  ObRawExpr* calc_part_id_expr_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_OPTIMIZER_OB_LOG_TABLE_LOOKUP_H_ */
