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
 * This file contains implementation support for the log json table abstraction.
 */

#ifndef _OB_LOG_JSON_TABLE_H
#define _OB_LOG_JSON_TABLE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{
template<typename R, typename C>
class PlanVisitor;
class ObLogJsonTable : public ObLogicalOperator
{
public:
  ObLogJsonTable(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        table_id_(OB_INVALID_ID),
        value_exprs_(plan.get_allocator()),
        table_name_(),
        access_exprs_(plan.get_allocator()),
        all_cols_def_(plan.get_allocator()),
        table_type_(MulModeTableType::INVALID_TABLE_TYPE),
        namespace_arr_(plan.get_allocator()),
        column_param_default_exprs_(plan.get_allocator()),
        data_index_id_(OB_INVALID_ID),
        index_column_cnt_(0),
        inc_pk_proj_(0),
        data_table_col_ids_() {}

  virtual ~ObLogJsonTable() {}
  int add_values_expr(ObIArray<ObRawExpr*> &exprs) { return append(value_exprs_, exprs); }
  const ObIArray<ObRawExpr*> &get_value_expr() const { return value_exprs_; }
  ObIArray<ObRawExpr*> &get_value_expr() { return value_exprs_; }
  virtual uint64_t hash(uint64_t seed) const override;
  int generate_access_exprs();
  ObIArray<ObRawExpr*> &get_access_exprs() { return access_exprs_; }
  int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs);
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }

  inline common::ObString &get_table_name() { return table_name_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }

  common::ObIArray<ObJtColBaseInfo*>& get_origin_cols_def() { return all_cols_def_; }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  inline void set_table_type(MulModeTableType table_type) { table_type_ = table_type; }
  inline MulModeTableType get_table_type() { return table_type_; }
  int set_namespace_arr(ObIArray<ObString> &namespace_arr);
  int get_namespace_arr(ObIArray<ObString> &namespace_arr);
  int64_t get_ns_size() { return namespace_arr_.count(); }
  int64_t get_column_param_default_size() { return column_param_default_exprs_.count(); }
  int set_column_param_default_arr(ObIArray<ObColumnDefault> &column_param_default_exprs);
  int get_column_param_default_arr(ObIArray<ObColumnDefault> &column_param_default_exprs);
  ObColumnDefault* get_column_param_default_val(int64_t index);
  inline void set_data_index_id(uint64_t index_id) { data_index_id_ = index_id; }
  inline uint64_t get_data_index_id() const { return data_index_id_; }
  inline void set_index_column_cnt(uint64_t col_cnt) { index_column_cnt_ = col_cnt; }
  inline uint64_t get_index_column_cnt() const { return index_column_cnt_; }
  int gen_data_table_col_ids(const ObJsonTableDef &tbl_def);
  const ObIArray<uint64_t> &get_data_table_col_ids() const { return data_table_col_ids_; }
  int generate_inc_pk_proj(const ObIArray<ObRawExpr*> &exprs);
  int64_t get_inc_pk_proj() const { return inc_pk_proj_; }

private:
  uint64_t table_id_;
  ObSqlArray<ObRawExpr*> value_exprs_;
  common::ObString table_name_;
  ObSqlArray<ObRawExpr*> access_exprs_;

  ObSqlArray<ObJtColBaseInfo*> all_cols_def_;
  // table func type
  MulModeTableType table_type_;
  // xml table param
  ObSqlArray<ObString> namespace_arr_;
  // default value array
  ObSqlArray<ObColumnDefault> column_param_default_exprs_;
  uint64_t data_index_id_;
  int64_t index_column_cnt_;
  int64_t inc_pk_proj_;
  common::ObSEArray<uint64_t, 16, common::ModulePageAllocator, true> data_table_col_ids_;

  DISALLOW_COPY_AND_ASSIGN(ObLogJsonTable);
};

}
}
#endif
