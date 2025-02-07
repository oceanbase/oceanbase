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

#ifndef _OB_LOG_UPDATE_H
#define _OB_LOG_UPDATE_H 1
#include "ob_log_del_upd.h"

namespace oceanbase
{
namespace sql
{
class ObUpdateStmt;
class ObLogUpdate : public ObLogDelUpd
{
public:
  ObLogUpdate(ObDelUpdLogPlan &plan)
      : ObLogDelUpd(plan)
  {}
  virtual ~ObLogUpdate() {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(double child_card, double &op_cost);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual const char *get_name() const override;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  virtual int op_is_update_pk_with_dop(bool &is_update) override;
private:
  virtual int generate_rowid_expr_for_trigger() override;
  virtual int generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int generate_multi_part_partition_id_expr() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogUpdate);
};
}
}
#endif
