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

#ifndef OCEANBASE_STORAGE_DDL_OB_FULL_TEXT_INDEX_WRITE_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_FULL_TEXT_INDEX_WRITE_TASK_H_

#include "sql/engine/sort/ob_sort_op_impl.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "lib/charset/ob_charset.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/page_arena.h"
#include "storage/ddl/ob_fts_macro_block_write_op.h"
#include "storage/ddl/ob_ddl_sort_provider.h"

namespace oceanbase
{
namespace storage
{

// 倒排索引排序刷盘算子
class ObInvertedIndexSortFlushOperator : public ObPipelineOperator
{
public:
  explicit ObInvertedIndexSortFlushOperator(ObPipeline *pipeline)
  : ObPipelineOperator(pipeline),
    is_inited_(false),
    tablet_id_(),
    allocator_("InvertedIdxSort", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    enable_encode_sortkey_(false)
    {}
  virtual ~ObInvertedIndexSortFlushOperator() = default;
  virtual bool is_valid() const override { return is_inited_ && tablet_id_.is_valid(); }
  int init(const ObTabletID &tablet_id);
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk) override {
    UNUSED(input_chunk);
    UNUSED(result_state);
    UNUSED(output_chunk);
    return OB_SUCCESS;
  }

  TO_STRING_KV(K_(tablet_id), K_(enable_encode_sortkey));
private:
  int deliver_sorted_chunks(ObDDLIndependentDag *ddl_dag,
                            ObDDLSortProvider::SortImpl *sort_impl);
  int add_unsorted_chunks(const ObChunk &input_chunk,
                          ObDDLSortProvider::SortImpl *sort_impl);

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  common::ObArenaAllocator allocator_;  // 用于构造临时数组
  common::ObArray<uint16_t> selector_array_;  // 预分配的 selector 数组
  common::ObArray<sql::ObSortKeyStore<false> *> sk_rows_;  // sk_rows 数组
  bool enable_encode_sortkey_;
};

class ObFullTextIndexWritePipeline : public ObIDDLPipeline
{
public:
  ObFullTextIndexWritePipeline()
  : ObIDDLPipeline(ObITask::ObITaskType::TASK_TYPE_DDL_WRITE_PIPELINE),
    is_inited_(false),
    chunk_(),
    fts_macro_block_write_op_(this),
    inverted_index_sort_flush_op_(this),
    ddl_slice_(nullptr)
    {}
  virtual ~ObFullTextIndexWritePipeline() { reset(); }
  int init(ObDDLSlice *ddl_slice);
  virtual int get_next_chunk(ObChunk *&next_chunk) override;
  virtual int finish_chunk(ObChunk *chunk) override;
  virtual void postprocess(int &ret_code) override;
  virtual ObITask::ObITaskPriority get_priority() override;
  virtual int inner_add_monitor_info(storage::ObDDLDagMonitorNode &node) override;
  int get_slice_idx(int64_t &slice_idx) const;
  void reset();

private:
  bool is_inited_;
  ObChunk chunk_;
  ObDAGFtsMacroBlockWriteOp fts_macro_block_write_op_;
  ObInvertedIndexSortFlushOperator inverted_index_sort_flush_op_;
  ObDDLSlice *ddl_slice_;
};

class ObFullTextIndexWritePipelineMonitorInfo : public ObDDLDagMonitorInfo
{
public:
  ObFullTextIndexWritePipelineMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task);
  virtual ~ObFullTextIndexWritePipelineMonitorInfo() = default;
  void init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx);
  void add_row_count(const int64_t row_count);
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const override;
private:
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t row_count_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_DDL_OB_FULL_TEXT_INDEX_WRITE_TASK_H_
