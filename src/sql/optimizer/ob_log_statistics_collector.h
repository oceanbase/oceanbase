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

#ifndef OCEANBASE_SQL_OB_LOG_STATISTICS_COLLECTOR_H_
#define OCEANBASE_SQL_OB_LOG_STATISTICS_COLLECTOR_H_
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObLogStatisticsCollector : public ObLogicalOperator
{
public:
  ObLogStatisticsCollector(ObLogPlan &plan);
  virtual ~ObLogStatisticsCollector() {}
  virtual const char *get_name() const;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item);
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  virtual bool is_block_op() const override { return false; }

  int64_t get_adaptive_threshold() { return adaptive_threshold_; }
  void set_adaptive_threshold(int64_t threshold);
private:
  int64_t adaptive_threshold_;

  DISALLOW_COPY_AND_ASSIGN(ObLogStatisticsCollector);
};


}
}

#endif
