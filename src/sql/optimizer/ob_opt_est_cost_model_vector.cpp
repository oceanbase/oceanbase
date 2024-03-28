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

#include "sql/optimizer/ob_opt_est_cost_model_vector.h"
#include "ob_opt_cost_model_parameter.h"

using namespace oceanbase;
using namespace sql;

int ObOptEstVectorCostModel::cost_range_scan(const ObCostTableScanInfo &est_cost_info,
                                            bool is_scan_index,
                                            double row_count,
                                            double &cost)
{
  int ret = OB_SUCCESS;
  // 从memtable读取数据的代价，待提供
  double memtable_cost = 0;
  // memtable数据和基线数据合并的代价，待提供
  double memtable_merge_cost = 0;
  double io_cost = 0.0;
  double cpu_cost = 0.0;
  if (OB_FAIL(range_scan_io_cost(est_cost_info,
                                 is_scan_index,
                                 row_count,
                                 io_cost))) {
    LOG_WARN("failed to calc table scan io cost", K(ret));
  } else if (OB_FAIL(range_scan_cpu_cost(est_cost_info,
                                         is_scan_index,
                                         row_count,
                                         false,
                                         cpu_cost))) {
    LOG_WARN("failed to calc table scan cpu cost", K(ret));
  } else {
    cost = io_cost + cpu_cost + memtable_cost + memtable_merge_cost;
    LOG_TRACE("OPT:[COST RANGE SCAN]", K(is_scan_index), K(row_count), K(cost),
            K(io_cost), K(cpu_cost), K(memtable_cost), K(memtable_merge_cost));
  }
  return ret;
}