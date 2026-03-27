/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_SET_OB_HASH_INTERSECT_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_HASH_INTERSECT_VEC_OP_H_

#include "sql/engine/set/ob_hash_set_vec_op.h"

namespace oceanbase
{
namespace sql
{

class ObHashIntersectVecSpec : public ObHashSetVecSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashIntersectVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObHashIntersectVecOp : public ObHashSetVecOp
{
public:
  ObHashIntersectVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashIntersectVecOp() {}

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;

private:
  int build_hash_table_by_part(int64_t batch_size);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_HASH_INTERSECT_VEC_OP_H_
