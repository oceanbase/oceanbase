/*
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

#ifndef OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_WRITE_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_WRITE_TASK_H_

#include "storage/ddl/ob_ddl_pipeline.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_sort_provider.h"
#include "storage/ddl/ob_fts_macro_block_write_op.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

namespace oceanbase
{
namespace sql
{
template <typename Store_Row, bool has_addon>
class ObSortVecOpChunk;

template<bool has_addon>
class ObSortKeyStore;
class RowMeta;
} // namespace sql

namespace blocksstable
{
class ObMacroBlockWriter;
}
namespace storage
{

class ObDDLIndependentDag;
class ObDAGFtsMacroBlockWriteOp;
class ObDDLFinalMergeSortWriteTaskMonitorInfo;
class ObDDLFinalMergeSortOp : public ObPipelineOperator
{
public:
  using Compare = ObInvertedIndexCompare<StoreRow>;
public:
  ObDDLFinalMergeSortOp(ObPipeline *pipeline);
  virtual ~ObDDLFinalMergeSortOp();
  virtual bool is_valid() const override { return is_inited_; }
  virtual int execute(const ObChunk &input_chunk, ResultState &result_state, ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk) override
  {
    UNUSED(input_chunk);
    UNUSED(result_state);
    UNUSED(output_chunk);
    return OB_SUCCESS;
  }

  int init(ObDDLIndependentDag *ddl_dag,
           ObIAllocator *allocator);
  int build_row_meta(const ObDDLTableSchema &ddl_schema);
  VIRTUAL_TO_STRING_KV(K(is_inited_), KP(ddl_dag_), KP(sort_impl_), K(enable_encode_sortkey_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLFinalMergeSortOp);
  int try_create_sort_impl();

public:
  static const int64_t MAX_MERGE_WAY_LIMIT = 64;
  static const int64_t MAX_BATCH_ROW_CNT = 512;
private:
  bool is_inited_;
  ObArenaAllocator rowmeta_allocator_;
  ObDDLIndependentDag *ddl_dag_;
  ObIAllocator *allocator_;
  sql::RowMeta row_meta_;
  sql::RowMeta addon_row_meta_;
  Compare *compare_;
  ObDDLSortProvider::SortImpl *sort_impl_;
  lib::MemoryContext merge_sort_mem_ctx_;
  ObBatchDatumRows output_bdrs_;
  // tmp code for debug
  ObMonitorNode tmp_monitor_info_;
  bool enable_encode_sortkey_;
};

class ObDDLFinalMergeSortWriteTask : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObDDLFinalMergeSortWriteTask();
  virtual ~ObDDLFinalMergeSortWriteTask();
  int init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id, const int64_t slice_idx);
  virtual int get_next_chunk(ObChunk *&chunk) override;
protected:
  virtual int inner_add_monitor_info(storage::ObDDLDagMonitorNode &node) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLFinalMergeSortWriteTask);
private:
  bool is_inited_;
  int64_t slice_idx_;
  ObTabletID tablet_id_;
  ObDDLIndependentDag *ddl_dag_;
  ObDDLFinalMergeSortOp merge_sort_op_;
  ObDAGFtsMacroBlockWriteOp macro_block_write_op_;
  ObArenaAllocator allocator_;
};



class ObDDLFinalMergeSortWriteTaskMonitorInfo : public ObDDLDagMonitorInfo
{
public:
  ObDDLFinalMergeSortWriteTaskMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task);
  virtual ~ObDDLFinalMergeSortWriteTaskMonitorInfo();
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const override;
  // Init params: only set once when task is created.
  void init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx);
  // Accumulate stats.
  void add_row_count(const int64_t row_count) { row_count_ += MAX(0, row_count); }

  INHERIT_TO_STRING_KV("BaseMonitorInfo", ObDDLDagMonitorInfo,
                       K_(tablet_id), K_(slice_idx), K_(row_count));

public:
  // Fixed params (set once).
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;

  // Accumulated stats.
  int64_t row_count_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_WRITE_TASK_H_
