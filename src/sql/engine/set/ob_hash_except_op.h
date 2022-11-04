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

#ifndef OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_OP_H_

#include "sql/engine/set/ob_hash_set_op.h"

namespace oceanbase
{
namespace sql
{

class ObHashExceptSpec : public ObHashSetSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashExceptSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObHashExceptOp : public ObHashSetOp
{
public:
  ObHashExceptOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashExceptOp() {}

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;

private:
  int build_hash_table_by_part(const int64_t batch_size);
  //从hash表中拿取处理完成的数据
  int get_next_row_from_hashtable(const ObChunkDatumStore::StoredRow *&store_row);
  int get_next_batch_from_hashtable(const int64_t batch_size);
  //继续处理右边行或者直接从hash表中返回行
  bool get_row_from_hash_table_;
  int batch_process_right();
  int batch_process_right_vectorize(const int64_t batch_size);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_HASH_EXCEPT_OP_H_
