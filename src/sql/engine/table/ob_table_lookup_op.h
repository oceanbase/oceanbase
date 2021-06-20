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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_TABLE_LOOKUP_OP_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_TABLE_LOOKUP_OP_

#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/table/ob_lookup_task_builder.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/executor/ob_task.h"

namespace oceanbase {
namespace sql {
class ObTableLookupSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableLookupSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type), remote_tsc_spec_(NULL), lookup_info_(), calc_part_id_expr_(NULL)
  {}

  virtual ~ObTableLookupSpec(){};

public:
  ObOpSpec* remote_tsc_spec_;
  ObLookupInfo lookup_info_;

  ObExpr* calc_part_id_expr_;

public:
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLookupSpec);
};

class ObTableLookupOp : public ObOperator {
public:
  ObTableLookupOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObTableLookupOp()
  {
    destroy();
  };

  int inner_open() override;
  int rescan() override;
  int inner_get_next_row() override;
  int inner_close() override;
  void destroy() override;

private:
  enum LookupState { INDEX_SCAN, DISTRIBUTED_LOOKUP, OUTPUT_ROWS, EXECUTION_FINISHED };
  int process_row(int64_t& part_row_cnt);
  int store_row(int64_t part_id, const common::ObNewRow* row);
  int store_row(
      int64_t part_id, const common::ObIArray<ObExpr*>& row, const bool table_has_hidden_pk, int64_t& part_row_cnt);
  int execute();
  int get_store_next_row();
  void set_end(bool end)
  {
    end_ = end;
  }
  bool end()
  {
    return end_;
  }
  // int init_partition_ranges(int64_t part_cnt);
  int clean_mem();
  void reset();
  int set_partition_cnt(int64_t partition_cnt);
  bool is_target_partition(int64_t pid);
  int wait_all_task(ObPhysicalPlanCtx* plan_ctx);

private:
  const static int64_t DEFAULT_BATCH_ROW_COUNT = 1024l * 1024;
  const static int64_t DEFAULT_PARTITION_BATCH_ROW_COUNT = 1000L;

  common::ObArenaAllocator allocator_;
  ObMiniTaskResult result_;
  // common::ObRowStore::Iterator result_iter_;
  ObChunkDatumStore::Iterator result_iter_;
  LookupState state_;
  bool end_;
  ObLookupMiniTaskExecutor lookup_task_executor_;
  ObLookupTaskBuilder task_builder_;
  // ranges
  ObMultiPartitionsRangesWarpper partitions_ranges_;
  // partition count
  int64_t partition_cnt_;
  share::schema::ObSchemaGetterGuard* schema_guard_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLookupOp);
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
