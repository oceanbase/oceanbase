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

#include "sql/optimizer/ob_log_stat_collector.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


const char *ObLogStatCollector::get_name() const
{
  static const char *name[1] =
  {
    "STATISTICS COLLECTOR",
  };
  return name[0];
}

int ObLogStatCollector::set_sort_keys(const common::ObIArray<OrderItem> &order_keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_keys_.assign(order_keys))) {
    LOG_WARN("failed to set sort keys", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObLogStatCollector::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
    if (OB_ISNULL(sort_keys_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(all_exprs.push_back(sort_keys_.at(i).expr_))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
      LOG_WARN("failed to get op exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogStatCollector::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
    if (OB_ISNULL(sort_keys_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(replace_expr_action(replacer, sort_keys_.at(i).expr_))) {
      LOG_WARN("failed to replace sort key expr", K(ret));
    }
  }
  return ret;
}
