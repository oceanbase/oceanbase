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

#ifndef OCEANBASE_BASIC_OB_SET_OB_HASH_SET_OP_H_
#define OCEANBASE_BASIC_OB_SET_OB_HASH_SET_OP_H_

#include "sql/engine/set/ob_set_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"

namespace oceanbase
{
namespace sql
{

class ObHashSetSpec : public ObSetSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashSetSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObSetSpec, K_(hash_funcs));
  ObHashFuncs hash_funcs_;
};

class ObHashSetOp : public ObOperator
{
public:
  ObHashSetOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashSetOp() {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;

protected:
  void reset();
  int is_left_has_row(bool &left_has_row);
  int get_left_row();
  int get_left_batch(const int64_t batch_size, const ObBatchRows *&child_brs);
  int get_right_row();

  int build_hash_table(bool from_child);
  int build_hash_table_from_left(bool from_child);
  int build_hash_table_from_left_batch(bool from_child, const int64_t batch_size);
  int init_hash_partition_infras();
  int init_hash_partition_infras_for_batch();
  int convert_row(
    const common::ObIArray<ObExpr*> &src_exprs, const common::ObIArray<ObExpr*> &dst_exprs);
  int convert_batch(const common::ObIArray<ObExpr*> &src_exprs, 
                    const common::ObIArray<ObExpr*> &dst_exprs,
                    const int64_t batch_size,
                    const ObBitVector &skip);
  int init_mem_context();

protected:
    //used by intersect and except
    bool first_get_left_;
    bool has_got_part_;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
    ObHashPartInfrastructure<ObHashPartCols, ObHashPartStoredRow> hp_infras_;
    uint64_t *hash_values_for_batch_;
    //for batch array init, not reset in rescan
    bool need_init_;
    const ObBatchRows *left_brs_;
    lib::MemoryContext mem_context_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SET_OB_HASH_SET_OP_H_
