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
#ifndef OCEANBASE_BASIC_OB_GROUPBY_VEC_OP_H_
#define OCEANBASE_BASIC_OB_GROUPBY_VEC_OP_H_

#include "sql/engine/aggregate/ob_groupby_op.h"
#include "share/aggregate/processor.h"


namespace oceanbase
{
namespace sql
{
class ObCompactRow;
class RowMeta;
class ObGroupByVecOp: public ObOperator
{
public:
  ObGroupByVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input),
    aggr_processor_(eval_ctx_,
                    (static_cast<ObGroupBySpec &>(const_cast<ObOpSpec &>(spec))).aggr_infos_,
                    ObModIds::OB_SQL_AGGR_FUNC_ROW,
                    op_monitor_info_,
                    exec_ctx.get_my_session()->get_effective_tenant_id())
  {}

  inline aggregate::Processor &get_aggr_processor() { return aggr_processor_; }
  virtual int inner_open() override;
  virtual int inner_get_next_row() = 0;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_close() override;
  virtual void destroy() override;

  int calculate_3stage_agg_info(const ObCompactRow &row, const RowMeta &row_meta,
                                const int64_t batch_idx, int32_t &start_agg_id,
                                int32_t &end_agg_id);
  int calculate_3stage_agg_info(char *aggr_row, const RowMeta &row_meta,
                                const int64_t batch_idx, int32_t &start_agg_id,
                                int32_t &end_agg_id);

protected:
  aggregate::Processor aggr_processor_;
};
;
} // end sql
} // end oceanabse
#endif // OCEANBASE_BASIC_OB_GROUPBY_VEC_OP_H_