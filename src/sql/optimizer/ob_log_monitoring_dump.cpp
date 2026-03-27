/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_monitoring_dump.h"

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
