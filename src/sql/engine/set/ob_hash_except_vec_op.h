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

#ifndef OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_VEC_OP_H_

#include "sql/engine/set/ob_hash_set_vec_op.h"

namespace oceanbase
{
namespace sql
{

class ObHashExceptVecSpec : public ObHashSetVecSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashExceptVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObHashExceptVecOp : public ObHashSetVecOp
{
public:
  ObHashExceptVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashExceptVecOp() {}

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;

private:
  int build_hash_table_by_part(int64_t batch_size);
  //Get the processed data from the hash table
  int get_next_batch_from_hashtable(const int64_t batch_size);
  //Continue processing the row on the right or return the row directly from the hash table
  bool get_row_from_hash_table_;
  int batch_process_right_vectorize(const int64_t batch_size);
  const ObCompactRow **store_rows_ = nullptr;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_VEC_OP_H_
