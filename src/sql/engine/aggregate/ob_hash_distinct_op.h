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

#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/aggregate/ob_distinct_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"

namespace oceanbase
{
namespace sql
{

class ObHashDistinctSpec : public ObDistinctSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObHashDistinctSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  ObSortCollations sort_collations_;
  ObHashFuncs hash_funcs_;
  bool is_push_down_;
};

class ObHashDistinctOp : public ObOperator
{
public:
  ObHashDistinctOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashDistinctOp() {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  typedef int (ObHashDistinctOp::*Build_distinct_data_func)(bool block);
  typedef int (ObHashDistinctOp::*Build_distinct_data_batch_func)(const int64_t batch_size, bool is_block);
  void reset();
  int do_unblock_distinct();
  int do_unblock_distinct_for_batch(const int64_t batch_size);
  int do_block_distinct();
  int do_block_distinct_for_batch(const int64_t batch_size);
  int init_hash_partition_infras();
  int init_hash_partition_infras_for_batch();
  int build_distinct_data(bool is_block);
  int build_distinct_data_by_pass(bool is_block);
  int build_distinct_data_for_batch(const int64_t batch_size, bool is_block);
  int build_distinct_data_for_batch_by_pass(const int64_t batch_size, bool is_block);
  int by_pass_get_next_batch(const int64_t batch_size);
  int process_state(int64_t probe_cnt, bool &can_insert);
  int init_mem_context();
private:
  const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / sizeof(ObHashPartCols);
  typedef int (ObHashDistinctOp::*GetNextRowFunc)();
  typedef int (ObHashDistinctOp::*GetNextRowBatchFunc)(const int64_t batch_size);
  static const int64_t MIN_BUCKET_COUNT = 1L << 14;  //16384;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19; //524288;
  bool enable_sql_dumped_;
  bool first_got_row_;
  bool has_got_part_;
  bool iter_end_;
  bool child_op_is_end_;
  bool need_init_; //init 3 arrays before got_row, do not reset in rescan()
  GetNextRowFunc get_next_row_func_;
  GetNextRowBatchFunc get_next_batch_func_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObHashPartInfrastructure<ObHashPartCols, ObHashPartStoredRow> hp_infras_;
  int64_t group_cnt_;
  uint64_t *hash_values_for_batch_;
  int64_t tenant_id_;
  Build_distinct_data_func build_distinct_data_func_;
  Build_distinct_data_batch_func build_distinct_data_batch_func_;
  ObAdaptiveByPassCtrl bypass_ctrl_;
  lib::MemoryContext mem_context_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_OP_H_ */