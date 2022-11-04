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

#ifndef OCEANBASE_BASIC_OB_SCALAR_GROUPBY_OP_H_
#define OCEANBASE_BASIC_OB_SCALAR_GROUPBY_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"

namespace oceanbase
{
namespace sql
{
class ObScalarAggregateSpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObScalarAggregateSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObGroupBySpec(alloc, type)
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObScalarAggregateSpec);
};

class ObScalarAggregateOp : public ObGroupByOp
{
public:
  friend ObAggregateProcessor;
  ObScalarAggregateOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObGroupByOp(exec_ctx, spec, input), started_(false), dir_id_(-1)
  {
  }

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  // reset default value of %cur_rownum_ && %rownum_limit_
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObScalarAggregateOp);

private:
  bool started_;
  int64_t dir_id_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SCALAR_GROUPBY_OP_H_
