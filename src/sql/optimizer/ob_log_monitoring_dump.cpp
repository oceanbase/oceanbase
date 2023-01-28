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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const char *ObLogMonitoringDump::get_name() const
{
  static const char *monitoring_name_[1] =
  {
    "MONITORING DUMP",
  };
  return monitoring_name_[0];
}

int ObLogMonitoringDump::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    set_op_cost(0.0);
    set_cost(op_cost_ + child->get_cost());
    set_card(child->get_card());
  }
  return ret;
}

}
}
