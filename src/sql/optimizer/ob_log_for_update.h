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

#ifndef _OB_LOG_FOR_UPDATE_H
#define _OB_LOG_FOR_UPDATE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_del_upd.h"
namespace oceanbase
{
namespace sql
{
class ObLogForUpdate : public ObLogicalOperator
{
public:
  ObLogForUpdate(ObLogPlan &plan);
  virtual ~ObLogForUpdate() {}
  virtual const char *get_name() const;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  inline void set_lock_rownum(ObRawExpr *lock_rownum) { lock_rownum_ = lock_rownum; }
  inline ObRawExpr* get_lock_rownum() { return lock_rownum_; }
  virtual int est_cost();
  virtual int compute_op_ordering();
  int compute_sharding_info() override;
  int allocate_granule_pre(AllocGIContext &ctx) override;
  int allocate_granule_post(AllocGIContext &ctx) override;

  void set_skip_locked(bool skip) { skip_locked_ = skip; }
  bool is_skip_locked() const { return skip_locked_; }

  void set_wait_ts(int64_t wait_ts) { wait_ts_ = wait_ts; }
  int64_t get_wait_ts() const { return wait_ts_; }

  int get_table_columns(const uint64_t table_id, ObIArray<ObColumnRefRawExpr *> &table_cols) const;
  int get_rowkey_exprs(const uint64_t table_id, ObIArray<ObColumnRefRawExpr *> &rowkey) const;
  int is_rowkey_nullable(const uint64_t table_id, bool &is_nullable) const;
  ObIArray<IndexDMLInfo *> &get_index_dml_infos(){ return index_dml_info_; }
  bool is_multi_table_skip_locked();
  bool is_multi_part_dml() const { return is_multi_part_dml_; }
  void set_is_multi_part_dml(bool is_multi_part_dml) { is_multi_part_dml_ = is_multi_part_dml; }
  bool is_gi_above() const { return gi_charged_; }
  void set_gi_above(bool gi_above) { gi_charged_ = gi_above; }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
protected:
  int generate_multi_part_partition_id_expr();
  int get_for_update_dependant_exprs(ObIArray<ObRawExpr*> &dep_exprs);
private:
  bool skip_locked_; // is not used now
  bool is_multi_part_dml_;
  bool gi_charged_;
  int64_t wait_ts_;
  ObRawExpr *lock_rownum_; // only used for skip locked
  ObSEArray<IndexDMLInfo*, 1, common::ModulePageAllocator, true> index_dml_info_;
  DISALLOW_COPY_AND_ASSIGN(ObLogForUpdate);
};
} // end of namespace sql
} // end of namespace oceanbase

#endif // _OB_LOG_FOR_UPDATE_H
