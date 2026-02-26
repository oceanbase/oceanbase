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

#ifndef OCEANBASE_BASIC_OB_SET_OB_HASH_SET_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_HASH_SET_VEC_OP_H_

#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{

class ObHashSetVecSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashSetVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(sort_collations));
  ExprFixedArray set_exprs_;
  ObSortCollations sort_collations_;};

class ObHashSetVecOp : public ObOperator
{
public:
  ObHashSetVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashSetVecOp() {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;

protected:
  void reset();
  int get_left_batch(const int64_t batch_size, const ObBatchRows *&child_brs);
  int build_hash_table_from_left_batch(bool from_child, const int64_t batch_size);
  int init_hash_partition_infras();
  int init_hash_partition_infras_for_batch();
  int init_mem_context();
  int convert_vector(const common::ObIArray<ObExpr*> &src_exprs,
                    const common::ObIArray<ObExpr*> &dst_exprs,
                    const ObBatchRows *&child_brs);

protected:
    //used by intersect and except
    bool first_get_left_;
    bool has_got_part_;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
    ObHashPartInfrastructureVecImpl hp_infras_;
    uint64_t *hash_values_for_batch_;
    //for batch array init, not reset in rescan
    bool need_init_;
    const ObBatchRows *left_brs_;
    lib::MemoryContext mem_context_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_HASH_SET_VEC_OP_H_
