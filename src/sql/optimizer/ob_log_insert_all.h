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

#ifndef _OB_LOG_INSERT_ALL_H
#define _OB_LOG_INSERT_ALL_H 1
#include "ob_logical_operator.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "ob_log_insert.h"

namespace oceanbase {
namespace sql {

class ObLogInsertAll : public ObLogInsert {
public:
  ObLogInsertAll(ObLogPlan& plan)
      : ObLogInsert(plan),
        is_multi_table_insert_(false),
        is_multi_insert_first_(false),
        is_multi_conditions_insert_(false),
        multi_value_columns_(NULL),
        multi_column_convert_exprs_(NULL),
        multi_insert_table_info_(NULL)
  {}

  virtual ~ObLogInsertAll()
  {}

  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int extract_value_exprs();
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs) override;

  const char* get_name() const;
  const common::ObIArray<RawExprArray>& get_multi_value_exprs() const
  {
    return multi_value_exprs_;
  }
  void set_multi_value_columns(const common::ObIArray<ColRawExprArray>* multi_value_columns)
  {
    multi_value_columns_ = multi_value_columns;
  }
  void set_multi_column_convert_exprs(const common::ObIArray<RawExprArray>* multi_col_conv_exprs)
  {
    multi_column_convert_exprs_ = multi_col_conv_exprs;
  }
  inline const common::ObIArray<RawExprArray>* get_multi_column_convert_exprs() const
  {
    return multi_column_convert_exprs_;
  }
  bool is_multi_table_insert() const
  {
    return is_multi_table_insert_;
  }
  void set_is_multi_table_insert(bool is_multi)
  {
    is_multi_table_insert_ = is_multi;
  }
  bool is_multi_insert_first()
  {
    return is_multi_insert_first_;
  }
  void set_is_multi_insert_first(bool v)
  {
    is_multi_insert_first_ = v;
  }
  bool is_multi_conditions_insert()
  {
    return is_multi_conditions_insert_;
  }
  void set_is_multi_conditions_insert(bool v)
  {
    is_multi_conditions_insert_ = v;
  }
  inline void set_multi_insert_table_info(const common::ObIArray<ObInsertTableInfo>* multi_insert_table_info)
  {
    multi_insert_table_info_ = multi_insert_table_info;
  }
  inline const common::ObIArray<ObInsertTableInfo>* get_multi_insert_table_info() const
  {
    return multi_insert_table_info_;
  }
  int add_part_hint(const ObPartHint*& part_hint)
  {
    return part_hints_.push_back(part_hint);
  }
  int get_part_hint(const ObPartHint*& part_hint, const uint64_t table_id) const;
  const common::ObIArray<const ObPartHint*>& get_part_hints() const
  {
    return part_hints_;
  }
  int remove_const_expr(const common::ObIArray<ObRawExpr*>& old_exprs, common::ObIArray<ObRawExpr*>& new_exprs) const;

protected:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed) override;
  int is_insert_table_id(uint64_t table_id, bool& is_true) const;

private:
  bool is_multi_table_insert_;
  bool is_multi_insert_first_;
  bool is_multi_conditions_insert_;
  const common::ObIArray<ColRawExprArray>* multi_value_columns_;
  common::ObSEArray<RawExprArray, 16, common::ModulePageAllocator, true> multi_value_exprs_;
  const common::ObIArray<RawExprArray>* multi_column_convert_exprs_;
  const common::ObIArray<ObInsertTableInfo>* multi_insert_table_info_;
  common::ObSEArray<const ObPartHint*, 16, common::ModulePageAllocator, true> part_hints_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
