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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_

#include "ob_px_transmit_op.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"

namespace oceanbase
{
namespace sql
{

class ObPxDistTransmitOpInput : public ObPxTransmitOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxDistTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxTransmitOpInput(ctx, spec)
  {}
  virtual ~ObPxDistTransmitOpInput()
  {}
};

class ObPxDistTransmitSpec : public ObPxTransmitSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxDistTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxTransmitSpec(alloc, type),
    dist_exprs_(alloc),
    dist_hash_funcs_(alloc),
    sort_cmp_funs_(alloc),
    sort_collations_(alloc),
    popular_values_hash_(alloc),
    calc_tablet_id_expr_(NULL)
  {}
  ~ObPxDistTransmitSpec() {}
  virtual int register_to_datahub(ObExecContext &ctx) const override;
  ExprFixedArray dist_exprs_;
  common::ObHashFuncs dist_hash_funcs_;
  ObSortFuncs sort_cmp_funs_;
  ObSortCollations sort_collations_;
  common::ObFixedArray<uint64_t, ObIAllocator> popular_values_hash_; // for hybrid hash distribution
  ObExpr *calc_tablet_id_expr_;   // for slave mapping
};

class ObPxDistTransmitOp : public ObPxTransmitOp
{
public:
  ObPxDistTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObPxTransmitOp(exec_ctx, spec, input),
    mem_context_(NULL),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_)
  {}
  virtual ~ObPxDistTransmitOp() {}
public:

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_rescan() override { return ObPxTransmitOp::inner_rescan(); }
  virtual int inner_close() override;
  virtual void destroy() override;

  virtual int do_transmit() override;

private:
  int do_hash_dist();
  int do_bc2host_dist();
  int do_random_dist();
  int do_broadcast_dist();
  int do_sm_broadcast_dist();
  int do_sm_pkey_hash_dist();
  int do_range_dist();
  int do_hybrid_hash_broadcast_dist();
  int do_hybrid_hash_random_dist();
protected:

  // We need to send the stored input rows in random order in FULL_INPUT_SAMPLE mode,
  // to avoid all transmit send rows to one receiver at the same time if input order
  // and range distribute keys are the same.
  int setup_sampled_rows_output();

  virtual int build_ds_piece_msg(int64_t expected_range_count,
      ObDynamicSamplePieceMsg &piece_msg) override;
  int add_batch_row_for_piece_msg(ObChunkDatumStore &sample_store);
  int add_row_for_piece_msg(ObChunkDatumStore &sample_store);
private:
  int build_row_sample_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg);

  // for range distribution to backup && restore last row/batch
  ObChunkDatumStore::ShadowStoredRow last_row_;
  ObBatchResultHolder brs_holder_;


  // for auto memory manager of %sampled_input_rows_
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_DIST_TRANSMIT_OP_H_
