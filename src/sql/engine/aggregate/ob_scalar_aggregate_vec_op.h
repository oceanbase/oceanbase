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

#ifndef _OB_SCALAR_AGGREGATE_OP_H_
#define _OB_SCALAR_AGGREGATE_OP_H_

#include "src/sql/engine/aggregate/ob_groupby_vec_op.h"
#include "src/sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace sql
{

class ObScalarAggregateVecSpec: public ObScalarAggregateSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObScalarAggregateVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) :
    ObScalarAggregateSpec(alloc, type), can_return_empty_set_(false)
  {}
  void set_cant_return_empty_set()
  {
    can_return_empty_set_ = true;
  }

  bool can_return_empty_set() const
  {
    return can_return_empty_set_;
  }

private:
  bool can_return_empty_set_;
};

class ObScalarAggregateVecOp: public ObGroupByVecOp
{
public:
  ObScalarAggregateVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObGroupByVecOp(exec_ctx, spec, input), started_(false), dir_id_(-1), row_(nullptr),
    row_meta_(&exec_ctx.get_allocator()), mem_context_(nullptr)
  {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
private:
  int add_batch_rows_for_3stage(const ObBatchRows &brs, aggregate::AggrRowPtr row);
  DISALLOW_COPY_AND_ASSIGN(ObScalarAggregateVecOp);
  int init_mem_context();

private:
  bool started_;
  int64_t dir_id_;
  ObCompactRow *row_;
  RowMeta row_meta_;
  lib::MemoryContext mem_context_;
};
} // end sql
} // end namespace
#endif // _OB_SCALAR_AGGREGATE_OP_H_