/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_LOG_OPTIMIZER_STAT_GATHERING_H_
#define OB_LOG_OPTIMIZER_STAT_GATHERING_H_

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace sql
{

struct OSGShareInfo {
  OSGShareInfo() :
  table_id_(common::OB_INVALID_ID),
  calc_part_id_expr_(NULL),
  part_level_(share::schema::PARTITION_LEVEL_ZERO),
  col_conv_exprs_(),
  generated_column_exprs_(),
  column_ids_() {};
  ~OSGShareInfo()
  {
    col_conv_exprs_.reset();
    generated_column_exprs_.reset();
    column_ids_.reset();
  }

  uint64_t table_id_;
  ObRawExpr *calc_part_id_expr_;
  share::schema::ObPartitionLevel part_level_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> col_conv_exprs_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> generated_column_exprs_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> column_ids_;
};

class ObLogOptimizerStatsGathering : public ObLogicalOperator
{
public:
  ObLogOptimizerStatsGathering(ObLogPlan &plan) :
  ObLogicalOperator(plan),
  table_id_(common::OB_INVALID_ID),
  calc_part_id_expr_(NULL),
  part_level_(share::schema::PARTITION_LEVEL_ZERO),
  osg_type_(OSG_TYPE::GATHER_OSG),
  col_conv_exprs_(),
  generated_column_exprs_(),
  column_ids_()
  {}

  virtual ~ObLogOptimizerStatsGathering() = default;
  const char *get_name() const;
  virtual int est_cost() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;

  int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  inline void set_table_id(uint64_t table_id) { table_id_ = table_id; };
  inline uint64_t get_table_id() { return table_id_; };
  inline void set_osg_type(OSG_TYPE type) { osg_type_ = type; };
  inline OSG_TYPE get_osg_type () { return osg_type_; };
  inline bool is_merge_osg() {return osg_type_ == OSG_TYPE::MERGE_OSG; };
  inline bool is_gather_osg() { return osg_type_ == OSG_TYPE::GATHER_OSG; };
  inline share::schema::ObPartitionLevel get_part_level() { return part_level_; };
  inline void set_part_level(share::schema::ObPartitionLevel level) { part_level_ = level; };
  inline ObRawExpr *&get_calc_part_id_expr() { return calc_part_id_expr_; };
  int get_target_osg_id(uint64_t &target_id);
  int add_column_id(uint64_t column_id) {
    return column_ids_.push_back(column_id);
  }
  int set_col_conv_exprs(const common::ObIArray<ObRawExpr *> &col_conv_exprs) {
    return col_conv_exprs_.assign(col_conv_exprs);
  }
  int set_generated_column_exprs(const common::ObIArray<ObRawExpr *> &generated_column_exprs) {
    return generated_column_exprs_.assign(generated_column_exprs);
  }
  int set_column_ids(const common::ObIArray<uint64_t> &column_ids) {
    return column_ids_.assign(column_ids);
  }
  void set_calc_part_id_expr(ObRawExpr *calc_part_id_expr) {
    calc_part_id_expr_ = calc_part_id_expr;
  }
  common::ObIArray<ObRawExpr*>& get_col_conv_exprs() { return col_conv_exprs_; };
  common::ObIArray<ObRawExpr*>& get_generated_column_exprs() { return generated_column_exprs_; };
  common::ObIArray<uint64_t>& get_column_ids() { return column_ids_; };
private:
  int inner_get_table_schema(const ObTableSchema *&table_schema);
  int inner_get_stat_part_cnt(const ObTableSchema *table_schema, uint64_t &part_num);
  uint64_t table_id_;
  ObRawExpr *calc_part_id_expr_;
  share::schema::ObPartitionLevel part_level_;
  OSG_TYPE osg_type_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> col_conv_exprs_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> generated_column_exprs_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> column_ids_;

  DISALLOW_COPY_AND_ASSIGN(ObLogOptimizerStatsGathering);
};

}
}

#endif