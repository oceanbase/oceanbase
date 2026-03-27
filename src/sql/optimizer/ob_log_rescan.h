/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_LOG_RESCAN_H_
#define OCEANBASE_SQL_OB_LOG_RESCAN_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogRescan : public ObLogicalOperator
{
public:
  ObLogRescan(ObLogPlan &plan) :
    ObLogicalOperator(plan), rescan_cnt_(0)
  {}
  virtual ~ObLogRescan() {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  virtual bool is_block_op() const override { return true; }
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;

  inline uint64_t get_rescan_cnt() { return rescan_cnt_; }
  inline void set_rescan_cnt(uint64_t cnt) { rescan_cnt_ = cnt; }
private:
  uint64_t rescan_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObLogRescan);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_RESCAN_H_