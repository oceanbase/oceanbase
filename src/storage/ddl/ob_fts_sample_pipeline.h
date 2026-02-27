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

#ifndef OB_OCEANBASE_STORAGE_DDL_FTS_SAMPLE_PIPELINE_H
#define OB_OCEANBASE_STORAGE_DDL_FTS_SAMPLE_PIPELINE_H

#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "common/ob_tablet_id.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/rc/context.h"
#include "share/datum/ob_datum_funcs.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "share/vector/ob_i_vector.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "lib/container/ob_fixed_array.h"
#include "storage/ddl/ob_ddl_sort_provider.h"

namespace oceanbase
{
namespace sql
{
class ObPxTabletRange;
}
namespace rootserver
{
struct ObDDLSliceInfo;
class ObDDLTaskRecordOperator;
}
namespace storage
{

constexpr int64_t FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE = 1024;
using StoreRow = sql::ObSortKeyStore<false>;
using Compare = storage::ObInvertedIndexCompare<StoreRow>;
using SortImpl = sql::ObStorageVecSortImpl<Compare, StoreRow, false>;

// ==================== FTS Sample ====================
class ObFtsSampleBaseOperator : public ObPipelineOperator
{
public:
  explicit ObFtsSampleBaseOperator(ObPipeline *pipeline)
    : ObPipelineOperator(pipeline), is_inited_(false), tablet_id_()
  {}
  virtual ~ObFtsSampleBaseOperator() = default;
  int init(const ObTabletID &tablet_id);
  virtual bool is_valid() const override { return is_inited_; }
protected:
  int get_ddl_tablet_context(ObDDLTabletContext *&tablet_context);
protected:
  bool is_inited_;
  ObTabletID tablet_id_;
};

class ObFtsForwardInvertSampleOperator : public ObFtsSampleBaseOperator
{
public:
  explicit ObFtsForwardInvertSampleOperator(ObPipeline *pipeline)
    : ObFtsSampleBaseOperator(pipeline),
      mem_ctx_(nullptr),
      op_monitor_info_(),
      ddl_table_schema_(),
      fts_word_doc_ddl_table_schema_(),
      selector_(),
      sk_rows_(),
      forward_compare_(),
      inverted_compare_(),
      sort_impl_forward_(nullptr),
      sort_impl_inverted_(nullptr)
  {}
  int init(const ObTabletID &tablet_id,
           const ObDDLTableSchema &ddl_table_schema,
           const ObDDLTableSchema &fts_word_doc_ddl_table_schema,
           const RowMeta *sk_row_meta,
           const RowMeta *sk_row_meta_inverted);
  virtual ~ObFtsForwardInvertSampleOperator();
protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;
  TO_STRING_KV(K_(tablet_id));
private:
  int build_final_samples(
      const common::Ob2DArray<sql::ObPxTabletRange> &local_ranges,
      const bool is_inverted,
      ObDDLTabletContext &tablet_ctx);
  int convert_keys_to_vectors(
      const common::ObIArray<const sql::ObPxTabletRange::DatumKey *> &key_refs,
      const bool is_inverted,
      const int64_t total_rows,
      common::ObFixedArray<common::ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
      common::ObIAllocator &allocator);
  int create_output_vectors_for_sort(
      const bool is_inverted,
      const int64_t batch_size,
      common::ObIAllocator &allocator,
      common::ObIArray<common::ObIVector *> &output_vectors);
  int sample_and_merge_sorted_data(
      SortImpl *sort_impl,
      const int64_t total_rows,
      const int64_t expect_sampling_count,
      const bool is_inverted,
      ObDDLTabletContext &tablet_ctx,
      const common::Ob2DArray<sql::ObPxTabletRange> &local_ranges);
private:
  lib::MemoryContext mem_ctx_;
  ObMonitorNode op_monitor_info_;
  ObDDLTableSchema ddl_table_schema_;
  ObDDLTableSchema fts_word_doc_ddl_table_schema_;
  common::ObArray<uint16_t> selector_;  // selector 数组
  common::ObArray<sql::ObSortKeyStore<false> *> sk_rows_;  // sk_rows 数组
  Compare forward_compare_;
  Compare inverted_compare_;
  SortImpl *sort_impl_forward_;
  SortImpl *sort_impl_inverted_;
};

class ObFtsWriteInnerTableOperator : public ObFtsSampleBaseOperator
{
public:
  explicit ObFtsWriteInnerTableOperator(ObPipeline *pipeline)
    : ObFtsSampleBaseOperator(pipeline), allocator_(ObMemAttr(MTL_ID(), "fts_write_inner"))
  {}
  virtual ~ObFtsWriteInnerTableOperator() = default;
protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;
  int build_ddl_slice_info(ObDDLTabletContext *tablet_context,
                           const int64_t ddl_task_id,
                           rootserver::ObDDLSliceInfo &ddl_slice_info);
  TO_STRING_KV(K_(tablet_id));
private:
  common::ObArenaAllocator allocator_;
};

class ObFtsSamplePipeline : public ObIDDLPipeline
{
public:
  explicit ObFtsSamplePipeline()
    : ObIDDLPipeline(TASK_TYPE_DDL_FTS_SAMPLE_TASK),
      is_chunk_generated_(false),
      tablet_id_(),
      chunk_(),
      final_sample_op_(this),
      write_inner_table_op_(this),
      sk_row_meta_(),
      sk_row_meta_inverted_(),
      row_meta_alloc_("FtsSample", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  virtual ~ObFtsSamplePipeline() = default;
  virtual share::ObITask::ObITaskPriority get_priority() override;
  int init(const ObTabletID &tablet_id);
  TO_STRING_KV(K_(tablet_id), K_(is_chunk_generated));
protected:
  virtual int get_next_chunk(ObChunk *&next_chunk) override;
  virtual void postprocess(int &ret_code) override;
private:
  int init_sk_row_meta(const bool is_inverted);
private:
  bool is_chunk_generated_;
  ObTabletID tablet_id_;
  ObChunk chunk_;
  ObFtsForwardInvertSampleOperator final_sample_op_;
  ObFtsWriteInnerTableOperator write_inner_table_op_;
  sql::RowMeta sk_row_meta_;
  sql::RowMeta sk_row_meta_inverted_;
  common::ObArenaAllocator row_meta_alloc_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_OCEANBASE_STORAGE_DDL_FTS_SAMPLE_PIPELINE_H
