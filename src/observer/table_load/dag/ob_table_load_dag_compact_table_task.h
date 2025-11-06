/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_parallel_heap_table_compactor.h"
#include "observer/table_load/dag/ob_table_load_dag_parallel_sstable_compactor.h"
#include "observer/table_load/dag/ob_table_load_dag_task.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadCompactDataOp;

class ObTableLoadDagCompactTableOpTask final : public share::ObITask,
                                               public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDagCompactTableOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagCompactTableOpTask() = default;
  int process() override;
};

class ObTableLoadDagCompactTableOpFinishTask final : public share::ObITask,
                                                     public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDagCompactTableOpFinishTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagCompactTableOpFinishTask() = default;
  int process() override;
  static void reset_op(ObTableLoadCompactDataOp *op);
};

class ObTableLoadSSTableSplitRangeTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadSSTableSplitRangeTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadSSTableSplitRangeTask() = default;
  int init(ObTableLoadTableOpCtx *op_ctx, ObTableLoadDagParallelCompactTabletCtx *compact_ctx);
  int process() override;

private:
  ObTableLoadTableOpCtx *op_ctx_;
  ObTableLoadDagParallelCompactTabletCtx *compact_ctx_;
  bool is_inited_;
};

class ObTableLoadSSTableMergeRangeTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadSSTableMergeRangeTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadSSTableMergeRangeTask() = default;
  int init(ObTableLoadTableOpCtx *op_ctx, ObTableLoadDagParallelCompactTabletCtx *compact_ctx,
           const int64_t total_thread_cnt, const int64_t curr_thread_idx);
  int process() override;
  int generate_next_task(share::ObITask *&next_task) override;

private:
  int init_scan_merge();
  int init_sstable_builder();

private:
  ObTableLoadTableOpCtx *op_ctx_;
  ObTableLoadDagParallelCompactTabletCtx *compact_ctx_;
  int64_t total_thread_cnt_;
  int64_t curr_thread_idx_;
  storage::ObDirectLoadTableHandleArray sstable_array_;
  storage::ObDirectLoadMultipleSSTableScanMerge scan_merge_;
  storage::ObDirectLoadMultipleSSTableBuilder sstable_builder_;
  bool is_inited_;
};

class ObTableLoadSSTableCompactTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadSSTableCompactTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadSSTableCompactTask() = default;
  int init(ObTableLoadTableOpCtx *op_ctx, ObTableLoadDagParallelCompactTabletCtx *compact_ctx);
  int process() override;

private:
  int compact_sstable(storage::ObDirectLoadTableHandle &result_sstable);
  int apply_merged_sstable(const storage::ObDirectLoadTableHandle &merged_sstable);

private:
  ObTableLoadTableOpCtx *op_ctx_;
  ObTableLoadDagParallelCompactTabletCtx *compact_ctx_;
  bool is_inited_;
};

class ObTableLoadCompactSSTableClearTask final : public share::ObITask,
                                                 public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadCompactSSTableClearTask(ObTableLoadDag *dag,
                                     ObTableLoadDagParallelCompactTabletCtx *compact_ctx,
                                     const int64_t thread_idx);
  virtual ~ObTableLoadCompactSSTableClearTask() = default;
  int generate_next_task(share::ObITask *&next_task) override;
  int process() override;

private:
  ObTableLoadDagParallelCompactTabletCtx *compact_ctx_;
  const int64_t thread_idx_;
};

class ObTableLoadCompactSSTableTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadCompactSSTableTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadCompactSSTableTask() = default;
  int init(ObTableLoadCompactDataOp *op);
  int post_generate_next_task(ObITask *&next_task) override;
  int process() override { return common::OB_SUCCESS; }

private:
  ObTableLoadCompactDataOp *op_;
  bool is_inited_;
};

class ObTableLoadHeapTableCompactTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadHeapTableCompactTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadHeapTableCompactTask() = default;
  int init(ObTableLoadCompactDataOp *op,
           const int64_t table_cnt, const int64_t total_thread_cnt, const int64_t curr_thread_idx);
  int process() override;
  int generate_next_task(share::ObITask *&next_task) override;

private:
  ObTableLoadCompactDataOp *op_;
  int64_t table_cnt_;
  int64_t total_thread_cnt_;
  int64_t curr_thread_idx_;
  bool is_inited_;
};

class ObTableLoadCompactHeapTableTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadCompactHeapTableTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadCompactHeapTableTask() = default;
  int init(ObTableLoadCompactDataOp *op);
  int post_generate_next_task(ObITask *&next_task) override;
  int process() override { return common::OB_SUCCESS; }

private:
  ObTableLoadCompactDataOp *op_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
