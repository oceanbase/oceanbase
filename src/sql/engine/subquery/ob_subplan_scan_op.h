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

#ifndef OCEANBASE_SUBQUERY_OB_SUBPLAN_SCAN_OP_H_
#define OCEANBASE_SUBQUERY_OB_SUBPLAN_SCAN_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{

class ObSubPlanScanSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSubPlanScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  // project child output to subplan scan column.
  // projector_is filled with [child output, scan column] pairs, even index is child output,
  // odd is scan column. e.g.:
  //   [child output, scan column, child output, scan column, ...]
  ExprFixedArray projector_;
};

class ObSubPlanScanOp : public ObOperator
{
public:
  ObSubPlanScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;

  virtual int inner_rescan() override;

  virtual int inner_get_next_row() override;

  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  virtual void destroy() override { ObOperator::destroy(); }
private:
  int init_monitor_info();
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_SUBPLAN_SCAN_OP_H_
