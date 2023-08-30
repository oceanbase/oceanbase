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

#ifndef _OB_LOG_DELETE_H
#define _OB_LOG_DELETE_H 1
#include "ob_log_del_upd.h"

namespace oceanbase
{
namespace sql
{
class ObLogDelete: public ObLogDelUpd
{
public:
  ObLogDelete(ObDelUpdLogPlan &plan) :
    ObLogDelUpd(plan)
  { }
  virtual ~ObLogDelete()
  {
  }
  virtual int est_cost();
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(double child_card, double &op_cost);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual const char *get_name() const;
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
protected:
  virtual int generate_rowid_expr_for_trigger() override;
  virtual int generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int generate_multi_part_partition_id_expr() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDelete);
};
}
}
#endif
