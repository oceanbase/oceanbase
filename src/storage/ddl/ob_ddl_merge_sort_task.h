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

#ifndef OCEANBASE_STORAGE_DDL_OB_DDL_MERGE_SORT_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_DDL_MERGE_SORT_TASK_H_

#include "share/scheduler/ob_independent_dag.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"

namespace oceanbase
{
namespace storage
{

class ObDDLIndependentDag;
class ObMergeSortPrepareTask;
class ObGroupWriteMacroBlockTask;
class ObDDLMergeSortTaskMonitorInfo;

class ObDDLMergeSortTask : public share::ObITaskWithMonitor
{
public:
  ObDDLMergeSortTask();
  virtual ~ObDDLMergeSortTask();
  int init(ObDDLIndependentDag *ddl_dag,
           ObDDLSlice *ddl_slice,
           const int64_t final_merge_ways);
protected:
  virtual int inner_add_monitor_info(storage::ObDDLDagMonitorNode &node) override;
public:
  virtual int process() override;
private:
  bool is_inited_;
  ObDDLIndependentDag *ddl_dag_;
  ObDDLSlice *ddl_slice_;
  int64_t final_merge_ways_;
};

class ObDDLMergeSortTaskMonitorInfo : public ObDDLDagMonitorInfo
{
public:
  ObDDLMergeSortTaskMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task);
  virtual ~ObDDLMergeSortTaskMonitorInfo();
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const override;
  // Init params: only set once when task is created.
  void init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx);
  // Accumulate merge results (does NOT track current round details).
  void add_merged_stats(const int64_t merged_chunk_count,
                        const int64_t merged_chunk_size,
                        const int64_t merged_row_count);

  INHERIT_TO_STRING_KV("BaseMonitorInfo", ObDDLDagMonitorInfo,
                       K_(tablet_id), K_(slice_idx),
                       K_(merged_chunk_count), K_(merged_chunk_size), K_(merged_row_count));

public:
  // Fixed params (set once).
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;

  // Accumulated stats across multiple executions (task may be suspended and scheduled again).
  int64_t merged_chunk_count_;
  int64_t merged_chunk_size_;
  int64_t merged_row_count_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_DDL_MERGE_SORT_TASK_H_
