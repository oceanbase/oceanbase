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

namespace oceanbase
{
namespace sql
{

class Path;
class ObLogTableLookup: public ObLogicalOperator
{
public:
  ObLogTableLookup(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        table_id_(common::OB_INVALID_ID),
        ref_table_id_(common::OB_INVALID_ID),
        index_id_(common::OB_INVALID_ID),
        table_name_(),
        index_name_(),
        scan_direction_(default_asc_direction()),
        table_partition_info_(NULL),
        use_batch_(false),
        fq_expr_(NULL),
        fq_type_(TableItem::NOT_USING),
        group_id_expr_(NULL),
        access_exprs_(),
        rowkey_exprs_(),
        calc_part_id_expr_(NULL)
  { }
  virtual ~ObLogTableLookup() { }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx);
  virtual int re_est_cost(EstimateCostInfo &param, double &card, double &cost)override;
  virtual int check_output_dependance(ObIArray<ObRawExpr *> &child_output, PPDeps &deps) override;
  virtual int compute_property(Path *path);
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int print_my_plan_annotation(char *buf,
                                       int64_t &buf_len,
                                       int64_t &pos,
                                       ExplainType type);
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  inline uint64_t get_table_id () const { return table_id_; }
  inline void set_ref_table_id(const uint64_t ref_table_id) { ref_table_id_ = ref_table_id; }
  inline uint64_t get_ref_table_id() const { return ref_table_id_; }
  inline void set_index_id(const uint64_t index_id) { index_id_ = index_id; }
  inline uint64_t get_index_id() const { return index_id_; }
  inline void set_table_name(common::ObString &table_name) { table_name_ = table_name; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_index_name(common::ObString &index_name) { index_name_ = index_name; }
  inline const common::ObString &get_index_name() const { return index_name_; }
  inline const ObRawExpr *get_group_id_expr() const { return group_id_expr_; }
  inline TableItem::FlashBackQueryType get_flashback_query_type() const {return fq_type_; }
  inline void set_flashback_query_type(TableItem::FlashBackQueryType type) { fq_type_ = type; }
  inline const ObRawExpr* get_flashback_query_expr() const { return fq_expr_; }
  inline ObRawExpr* &get_flashback_query_expr() { return fq_expr_; }
  inline void set_flashback_query_expr(ObRawExpr *expr) { fq_expr_ = expr; }
  inline ObTablePartitionInfo *get_table_partition_info() { return table_partition_info_; }
  inline const ObTablePartitionInfo *get_table_partition_info() const { return table_partition_info_; }
  inline void set_table_partition_info(ObTablePartitionInfo *table_partition_info) { table_partition_info_ = table_partition_info; }
  inline void set_use_batch(bool use_batch) { use_batch_ = use_batch; }
  inline bool use_batch() const { return use_batch_; }
  inline ObOrderDirection get_scan_direction() const { return scan_direction_; }
  inline void set_scan_direction(ObOrderDirection direction)
  {
    scan_direction_ = direction;
    common::ObIArray<OrderItem> &op_ordering = get_op_ordering();
    for (int64_t i = 0; i < op_ordering.count(); ++i) {
      op_ordering.at(i).order_type_ = scan_direction_;
    }
  }
  inline const common::ObIArray<ObRawExpr *> &get_access_exprs() const { return access_exprs_; }
  inline common::ObIArray<ObRawExpr *> &get_access_exprs() { return access_exprs_; }
  inline const ObRawExpr *get_calc_part_id_expr() const { return calc_part_id_expr_; }
  inline void set_calc_part_id_expr(ObRawExpr *calc_part_id_expr) { calc_part_id_expr_ = calc_part_id_expr; }
  inline const common::ObIArray<ObRawExpr *> &get_rowkey_exprs() const { return rowkey_exprs_; }
  int replace_gen_column(ObRawExpr *part_expr, ObRawExpr *&new_part_expr);
  int generate_access_exprs();
  int check_access_dependance(PPDeps &deps);
  int copy_part_expr_pre(CopyPartExprCtx &ctx) override;
  int do_copy_calc_part_id_expr(CopyPartExprCtx &ctx);
private:
  uint64_t table_id_;
  uint64_t ref_table_id_;
  uint64_t index_id_;
  common::ObString table_name_;
  common::ObString index_name_;
  ObOrderDirection scan_direction_;
  // table partiton locations
  ObTablePartitionInfo *table_partition_info_; //this member is not in copy_without_child,
                                               //because its used in EXCHANGE stage, and
                                               //copy_without_child used before this
  bool use_batch_;
  ObRawExpr* fq_expr_; //flashback query expr
  TableItem::FlashBackQueryType fq_type_; //flashback query type
  ObRawExpr *group_id_expr_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> rowkey_exprs_;
  ObRawExpr *calc_part_id_expr_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_OPTIMIZER_OB_LOG_TABLE_LOOKUP_H_ */
