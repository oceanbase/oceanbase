/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_SET_OB_MERGE_EXCEPT_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_MERGE_EXCEPT_OP_H_

#include "sql/engine/set/ob_merge_set_op.h"

namespace oceanbase
{
namespace sql
{

class ObMergeExceptSpec : public ObMergeSetSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObMergeExceptSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObMergeExceptOp : public ObMergeSetOp
{
public:
  ObMergeExceptOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt);

private:
  bool right_iter_end_;
  bool first_got_right_row_;
  bool first_got_left_row_;
  const ObBatchRows *right_brs_;
  int64_t right_idx_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_SET_OB_MERGE_EXCEPT_OP_H_ */