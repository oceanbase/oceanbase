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
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{
class ObSelectLogPlan;

class ObLogInsert : public ObLogDelUpd
{
public:
  ObLogInsert(ObDelUpdLogPlan &plan)
      : ObLogDelUpd(plan),
        is_replace_(false),
        insert_up_(false),
        is_insert_select_(false),
        append_table_id_(0),
        constraint_infos_(NULL)
  {
  }

  virtual ~ObLogInsert()
  {
  }
  const char* get_name() const;
  void set_replace(bool replace)
  {
    is_replace_ = replace;
  }

  bool is_replace() const
  {
    return is_replace_;
  }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  void set_insert_up(bool insert_up)
  {
    insert_up_ = insert_up;
  }
  bool get_insert_up()
  {
    return insert_up_;
  }
  void set_is_insert_select(bool v) { is_insert_select_ = v; }
  bool is_insert_select() const { return is_insert_select_; }
  virtual bool is_single_value() const override
  {
    bool is_single_values = false;
    if (NULL != get_stmt() && get_stmt()->is_insert_stmt() &&
        static_cast<const ObInsertStmt*>(get_stmt())->is_insert_single_value()) {
      is_single_values = true;
    }

    // After rewriting the multi-line insert, the SQL statement will become a single-line insert,
    // so it is necessary to judge whether it is insert_batch_opt
    if (is_single_values && OB_NOT_NULL(get_plan())) {
      if (get_plan()->get_optimizer_context().is_do_insert_batch_opt()) {
        is_single_values = false;
      }
    }
    return is_single_values;
  }

  const ObIArray<IndexDMLInfo *> &get_replace_index_dml_infos() const
  { return index_replace_infos_; }
  ObIArray<IndexDMLInfo *> &get_replace_index_dml_infos()
  { return index_replace_infos_; }
  const ObIArray<IndexDMLInfo *> &get_insert_up_index_dml_infos() const
  { return index_upd_infos_; }
  ObIArray<IndexDMLInfo *> &get_insert_up_index_dml_infos()
  { return index_upd_infos_; }
  /**
   *  Get the hash value of the INSERT operator
   */
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int compute_plan_type() override;
  virtual int compute_sharding_info() override;
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(double child_card, double &op_cost);
  inline void set_append_table_id(const uint64_t append_table_id)
  {
    append_table_id_ = append_table_id;
  }
  inline uint64_t get_append_table_id() const { return append_table_id_; }
  void set_constraint_infos(const common::ObIArray<ObUniqueConstraintInfo> *constraint_infos)
  {
    constraint_infos_ = constraint_infos;
  }
  const common::ObIArray<ObUniqueConstraintInfo> *get_constraint_infos() const
  {
    return constraint_infos_;
  }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
protected:
  int get_constraint_info_exprs(ObIArray<ObRawExpr*> &all_exprs);
  virtual int generate_rowid_expr_for_trigger() override;
  virtual int generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int generate_multi_part_partition_id_expr() override;
protected:
  bool is_replace_;
  common::ObArray<IndexDMLInfo *, common::ModulePageAllocator, true> index_replace_infos_;
  // for insert_up update caluse
  common::ObArray<IndexDMLInfo *, common::ModulePageAllocator, true> index_upd_infos_;
  bool insert_up_; // insert on duplicate update statement
  //for SPM Pruning
  bool is_insert_select_;
  uint64_t append_table_id_;
  const common::ObIArray<ObUniqueConstraintInfo> *constraint_infos_;
};

}
}
#endif
