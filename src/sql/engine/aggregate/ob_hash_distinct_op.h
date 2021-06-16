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

namespace oceanbase {
namespace sql {

class ObHashDistinctSpec : public ObDistinctSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObHashDistinctSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  ObSortCollations sort_collations_;
  ObHashFuncs hash_funcs_;
};

class ObHashDistinctOp : public ObOperator {
public:
  ObHashDistinctOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObHashDistinctOp()
  {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

private:
  void reset();
  int do_unblock_distinct();
  int do_block_distinct();
  int init_hash_partition_infras();
  int build_distinct_data(bool is_block);

private:
  typedef int (ObHashDistinctOp::*GetNextRowFunc)();
  static const int64_t MIN_BUCKET_COUNT = 1L << 14;  // 16384;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19;  // 524288;
  bool enable_sql_dumped_;
  bool first_got_row_;
  bool has_got_part_;
  bool iter_end_;
  GetNextRowFunc get_next_row_func_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObHashPartInfrastructure<ObHashPartCols, ObHashPartStoredRow> hp_infras_;
  int64_t group_cnt_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_OP_H_ */