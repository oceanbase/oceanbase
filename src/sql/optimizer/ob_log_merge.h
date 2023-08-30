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

#ifndef _OB_LOG_MERGE_H
#define _OB_LOG_MERGE_H 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_insert.h"
namespace oceanbase
{
namespace sql
{

class ObLogMerge : public ObLogDelUpd
{
public:
  ObLogMerge(ObDelUpdLogPlan &plan)
      : ObLogDelUpd(plan),
		index_upd_infos_(),
		index_del_infos_()
  { }
  virtual ~ObLogMerge() {}
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int compute_sharding_info() override;
  const common::ObIArray<ObRawExpr *> &get_insert_condition() const;

  const common::ObIArray<ObRawExpr *> &get_update_condition() const;

  const common::ObIArray<ObRawExpr *> &get_delete_condition() const;

  const char* get_name() const;

  bool is_insert_dml_info(const IndexDMLInfo *dml_info) const;
  bool is_delete_dml_info(const IndexDMLInfo *dml_info) const;
  int get_modified_index_id(common::ObIArray<uint64_t> &index_tids);

  int assign_dml_infos(const ObIArray<IndexDMLInfo *> &index_insert_infos,
                       const ObIArray<IndexDMLInfo *> &index_update_infos,
                       const ObIArray<IndexDMLInfo *> &index_delete_infos);

  const ObIArray<IndexDMLInfo *> &get_update_infos() const { return index_upd_infos_; }
  const ObIArray<IndexDMLInfo *> &get_delete_infos() const { return index_del_infos_; }
  ObIArray<IndexDMLInfo *> &get_update_infos() { return index_upd_infos_; }
  ObIArray<IndexDMLInfo *> &get_delete_infos() { return index_del_infos_; }
  const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &get_equal_pairs() const { return equal_pairs_; }
  int set_equal_pairs(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_infos) { return equal_pairs_.assign(equal_infos); }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
protected:
  int generate_rowid_expr_for_trigger() override;
  virtual int generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs) override;
  int generate_multi_part_partition_id_expr() override;
  virtual int gen_location_constraint(void *ctx) override;
  DISALLOW_COPY_AND_ASSIGN(ObLogMerge);
private:
  ObSEArray<IndexDMLInfo *, 4, common::ModulePageAllocator, true> index_upd_infos_;
  ObSEArray<IndexDMLInfo *, 4, common::ModulePageAllocator, true> index_del_infos_;
  common::ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true> equal_pairs_;
};
}//sql
}//oceanbase

#endif
