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

#define USING_LOG_PREFIX STORAGE

#include "ob_final_merge_sort_write_task.h"

#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_sort_provider.h"
#include "storage/ob_storage_schema.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/vector/type_traits.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/vector_basic_op.h"
#include "lib/container/ob_heap.h"
#include "storage/ddl/ob_fts_macro_block_write_op.h"
#include "lib/rc/ob_rc.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{
using SortImpl = ObDDLSortProvider::SortImpl;
ObDDLFinalMergeSortOp::ObDDLFinalMergeSortOp(ObPipeline *pipeline)
  : ObPipelineOperator(pipeline),
    is_inited_(false),
    rowmeta_allocator_(ObMemAttr(MTL_ID(), "DDLSortRowMeta")),
    ddl_dag_(nullptr),
    allocator_(nullptr),
    compare_(nullptr),
    sort_impl_(nullptr),
    merge_sort_mem_ctx_(nullptr),
    enable_encode_sortkey_(false)
{
}


ObDDLFinalMergeSortOp::~ObDDLFinalMergeSortOp()
{
  row_meta_.reset();
  addon_row_meta_.reset();
  allocator_ = nullptr;
  ddl_dag_ = nullptr;
  if (nullptr != sort_impl_) {
    sort_impl_->~SortImpl();
    merge_sort_mem_ctx_->get_malloc_allocator().free(sort_impl_);
    sort_impl_ = nullptr;
  }
  if (nullptr != compare_) {
    compare_->~Compare();
    merge_sort_mem_ctx_->get_malloc_allocator().free(compare_);
    compare_ = nullptr;
  }
  if (nullptr != merge_sort_mem_ctx_) {
    DESTROY_CONTEXT(merge_sort_mem_ctx_);
    merge_sort_mem_ctx_ = nullptr;
  }
  rowmeta_allocator_.reset();
  enable_encode_sortkey_ = false;
  is_inited_ = false;
}

typedef sql::ObSortVecOpChunk<ObDDLSortProvider::StoreRow, false> SqlSortChunk; /*have addon*/
int ObDDLFinalMergeSortOp::init(ObDDLIndependentDag *ddl_dag,
                           ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init arguments", K(ret), KP(ddl_dag),  KP(allocator));
  } else {
    ddl_dag_ = ddl_dag;
    allocator_ = allocator;
    const ObDDLTableSchema *sort_schema = nullptr;
    if (OB_FAIL(ddl_dag_->get_sort_ddl_table_schema(sort_schema))) {
      LOG_WARN("get sort ddl table schema failed", K(ret));
    } else if (OB_ISNULL(sort_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort ddl table schema is null", K(ret));
    } else if (OB_FAIL(ddl_dag->check_enable_encode_sortkey(enable_encode_sortkey_))) {
      LOG_WARN("failed to check can encode sortkey", K(ret));
    } else if (OB_FAIL(ObDDLSortProvider::build_full_row_meta_from_schema(*sort_schema, enable_encode_sortkey_, *allocator_, row_meta_))) {
      LOG_WARN("build row meta failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}


int ObDDLFinalMergeSortOp::try_create_sort_impl()
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  const ObDDLTableSchema *sort_schema = nullptr;
  param.set_mem_attr(ObMemAttr(MTL_ID(), "DDLMergeSort", ObCtxIds::WORK_AREA))
       .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == ddl_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (nullptr != merge_sort_mem_ctx_) {
    // reuse existing memory context
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(merge_sort_mem_ctx_, param))) {
    LOG_WARN("create merge sort ctx failed", K(ret));
  }

  /* init compare */
  if (OB_FAIL(ret)) {
  } else if (nullptr != sort_impl_) {
    /* skip */
  } else if (OB_ISNULL(compare_ = OB_NEWx(Compare, &merge_sort_mem_ctx_->get_malloc_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc compare failed", K(ret));
  } else if (OB_FAIL(ddl_dag_->get_sort_ddl_table_schema(sort_schema))) {
    LOG_WARN("get sort ddl table schema failed", K(ret));
  } else if (OB_ISNULL(sort_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort schema is null", K(ret));
  } else if (OB_FAIL(compare_->init(sort_schema, &row_meta_, enable_encode_sortkey_))) {
    LOG_WARN("init compare failed", K(ret), KP(sort_schema));
  } else if (OB_ISNULL(sort_impl_ = OB_NEWx(SortImpl, &merge_sort_mem_ctx_->get_malloc_allocator(), merge_sort_mem_ctx_, tmp_monitor_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc sort impl failed", K(ret));
  }

  /* init sort impl */
  if (OB_FAIL(ret)) {
  } else if (nullptr != sort_impl_ && sort_impl_->is_inited()) {
    /* skip */
  } else {
    const int64_t tempstore_read_alignment_size = ObTempBlockStore::get_read_alignment_size_config(MTL_ID());
    const int64_t max_batch_size = ddl_dag_->get_ddl_task_param().max_batch_size_;
    const ObCompressorType compressor_type = sort_schema->table_item_.compress_type_;
    if (OB_FAIL(sort_impl_->init(compare_,
                                 &row_meta_,
                                 &addon_row_meta_,
                                 max_batch_size,
                                 ObMemAttr(MTL_ID(), "DDLMergeSort", ObCtxIds::WORK_AREA),
                                 compressor_type,
                                 false /*enable_trunc*/,
                                 tempstore_read_alignment_size,
                                 MTL_ID(),
                                 enable_encode_sortkey_,
                                 nullptr /*slice_decider*/))) {
      LOG_WARN("merge sort impl init failed", K(ret));
    }
  }
  return ret;
}

int ObDDLFinalMergeSortOp::execute(const ObChunk &input_chunk,
                              ResultState &result_state,
                              ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = ResultState::INVALID_VALUE;
  int64_t output_row_cnt = 0;

  ObSEArray<ObIVector *, 6> output_sk_rows_no_encode;
  ObIArray<ObIVector *> *output_sk_rows = nullptr;
  ObIArray<ObIVector *> *output_addon_rows = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == ddl_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (OB_ISNULL(input_chunk.ddl_sort_chunk_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql sort chunk array is null", K(ret));
  } else if (OB_FAIL(try_create_sort_impl())) {
    LOG_WARN("try create sort impl failed", K(ret));
  } else {
    output_bdrs_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  }

  /* create sort impl if needed */
  if (OB_FAIL(ret)) {
  } else if (nullptr == sort_impl_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort impl is null", K(ret));
  } else if (0 == input_chunk.ddl_sort_chunk_array_->size() && 0 == sort_impl_->get_sort_chunks_size()) {
    ret = OB_ITER_END;
    LOG_WARN("invalid input chunk size", K(ret), K(input_chunk.ddl_sort_chunk_array_->size()), K(sort_impl_->get_sort_chunks_size()));
  } else if (0 == sort_impl_->get_sort_chunks_size()) {
    SqlSortChunk *sql_sort_chunk = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < input_chunk.ddl_sort_chunk_array_->size(); ++i) {
      ObDDLSortChunk &ddl_chunk = input_chunk.ddl_sort_chunk_array_->at(i);
      if (OB_ISNULL(sql_sort_chunk = reinterpret_cast<SqlSortChunk *>(ddl_chunk.get_sort_op_chunk()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql sort chunk is null", K(ret));
      } else {
        // add_sort_chunk takes ChunkType *& and will set it to nullptr on success (ownership transferred).
        // We must also clear ddl_chunk's pointer to avoid double-free in ObChunk::~ObChunk().
        SqlSortChunk *chunk = sql_sort_chunk;
        if (OB_FAIL(sort_impl_->add_sort_chunk(0 /* merge_sort_level*/, chunk))) {
          LOG_WARN("add sort chunk failed", K(ret));
        } else {
          ddl_chunk.reset();
        }
      }
    }
  }

  /* output chunk */
  if (OB_FAIL(ret)) {
  } else if (nullptr == sort_impl_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort impl is null", K(ret));
  } else if (OB_FAIL(sort_impl_->get_next_batch(output_row_cnt, output_sk_rows, output_addon_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("merge sort chunks to vector failed", K(ret));
    } else {
      ret = OB_SUCCESS;
      result_state = ResultState::NEED_MORE_INPUT;
      sort_impl_->reset_for_merge_sort();
      output_chunk.type_ = ObChunk::ITER_END_TYPE;
    }
  } else {
    result_state = ResultState::HAVE_MORE_OUTPUT;
    output_chunk.type_ = ObChunk::BATCH_DATUM_ROWS;
    output_chunk.bdrs_ = &output_bdrs_;
    if (enable_encode_sortkey_) {
      for (int64_t i = 1; OB_SUCC(ret) && i < output_sk_rows->count(); i++) {
        if (OB_FAIL(output_sk_rows_no_encode.push_back(output_sk_rows->at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        output_sk_rows = &output_sk_rows_no_encode;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(output_sk_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_sk_rows is null", K(ret), K(output_row_cnt));
    } else if (OB_FAIL(output_bdrs_.vectors_.assign(*output_sk_rows))) {
      LOG_WARN("assign output bdrs failed", K(ret));
    } else if (OB_NOT_NULL(output_addon_rows) && OB_FAIL(append(output_bdrs_.vectors_, *output_addon_rows))) {
      LOG_WARN("append output addon rows failed", K(ret));
    } else {
      output_bdrs_.row_count_ = output_row_cnt;
      if (OB_NOT_NULL(pipeline_)) {
        ObDDLFinalMergeSortWriteTaskMonitorInfo *info = static_cast<ObDDLFinalMergeSortWriteTaskMonitorInfo *>(pipeline_->get_monitor_info());
        if (OB_NOT_NULL(info)) {
          info->add_row_count(output_row_cnt);
        }
      }
    }
  }
  return ret;
}

ObDDLFinalMergeSortWriteTask::ObDDLFinalMergeSortWriteTask()
  : ObDDLWriteMacroBlockBasePipeline(share::ObITask::TASK_TYPE_DDL_MERGE_SORT_WRITE_TASK),
  is_inited_(false),
  slice_idx_(-1),
  tablet_id_(),
  ddl_dag_(nullptr),
  merge_sort_op_(this),
  macro_block_write_op_(this),
  allocator_(ObMemAttr(MTL_ID(), "DDLMergeSort"))
{
}

ObDDLFinalMergeSortWriteTask::~ObDDLFinalMergeSortWriteTask()
{
  ddl_dag_ = nullptr;
  slice_idx_ = -1;
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  is_inited_ = false;
}

int ObDDLFinalMergeSortWriteTask::init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (nullptr == ddl_dag) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl dag should not be null", K(ret));
  } else if (OB_FAIL(merge_sort_op_.init(ddl_dag, &allocator_))) {
    LOG_WARN("init merge sort op failed", K(ret));
  } else if (OB_FAIL(add_op(&merge_sort_op_))) {
    LOG_WARN("init merge sort op failed", K(ret));
  } else if (OB_FAIL(macro_block_write_op_.init(tablet_id, slice_idx))) {
    LOG_WARN("failed to init macro block write op", K(ret));
  } else if (OB_FAIL(add_op(&macro_block_write_op_))) {
    LOG_WARN("init macro block write op failed", K(ret));
  } else {
    slice_idx_ = slice_idx;
    tablet_id_ = tablet_id;
    ddl_dag_ = ddl_dag;
    is_inited_ = true;
  }
  return ret;
}
/*
 * this func only schedule once, when task execute
*/


int create_sort_chunk_array(ObChunk *&chunk)
{
  int ret = OB_SUCCESS;
  if (nullptr != chunk) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk should be null", K(ret));
  } else {
    ObChunk *new_chunk = OB_NEW(ObChunk, ObMemAttr(MTL_ID(), "DDLChunk"));
    ObArray<ObDDLSortChunk> *sql_sort_chunk_array = OB_NEW(ObArray<ObDDLSortChunk>, ObMemAttr(MTL_ID(), "DDLSortChunkArr"));

    if (OB_ISNULL(new_chunk) || OB_ISNULL(sql_sort_chunk_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), KP(new_chunk), KP(sql_sort_chunk_array));
      if (nullptr != new_chunk) {
        new_chunk->~ObChunk();
        ob_free(new_chunk);
        new_chunk = nullptr;
      }
      if (nullptr != sql_sort_chunk_array) {
        sql_sort_chunk_array->~ObArray<ObDDLSortChunk>();
        ob_free(sql_sort_chunk_array);
        sql_sort_chunk_array = nullptr;
      }
    } else {
      chunk = new_chunk;
      chunk->type_ = ObChunk::DDL_SORT_CHUNK_ARRAY;
      chunk->ddl_sort_chunk_array_ = sql_sort_chunk_array;
    }
  }
  return ret;
}



int ObDDLFinalMergeSortWriteTask::get_next_chunk(ObChunk *&chunk)
{
  int ret = OB_SUCCESS;
  ObDDLSlice *ddl_slice = nullptr;
  bool is_new_slice = false;
  ObDDLTabletContext *tablet_context = nullptr;
  chunk = nullptr; /* this func*/
  if (OB_FAIL(create_sort_chunk_array(chunk))) {
    LOG_WARN("failed to create sort chunk array", K(ret));
  } else if (OB_FAIL(ddl_dag_->get_tablet_context(tablet_id_, tablet_context))) {
    LOG_WARN("get tablet context failed", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet context is null", K(ret), K(tablet_id_));
  } else if (OB_FAIL(tablet_context->get_or_create_slice(slice_idx_, ddl_slice, is_new_slice))) {
    LOG_WARN("failed to get ddl slice", K(ret));
  } else if (OB_ISNULL(ddl_slice)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl slice is null", K(ret));
  } else if (OB_FAIL(ddl_slice->pop_all_sorted_chunks(*(chunk->ddl_sort_chunk_array_)))) {
    LOG_WARN("failed to pop chunk", K(ret));
  } else {
    // For fulltext inverted index, external merge sort is completed when all sorted chunks are ready for final merge write.
    // Record the final sorted row count once here (task is scheduled once).
    ObFTSBuildStat fts_stat;
    for (int64_t i = 0; i < chunk->ddl_sort_chunk_array_->count(); ++i) {
      const ObDDLSortChunk &ddl_chunk = chunk->ddl_sort_chunk_array_->at(i);
      if (OB_NOT_NULL(ddl_chunk.get_sort_op_chunk())) {
        using SqlSortChunk = sql::ObSortVecOpChunk<ObDDLSortProvider::StoreRow, false>;
        const SqlSortChunk *sort_chunk = reinterpret_cast<const SqlSortChunk *>(ddl_chunk.get_sort_op_chunk());
        fts_stat.inverted_sorted_row_cnt_ += MAX(0, sort_chunk->get_row_count());
      }
    }
    ddl_dag_->update_fts_build_stat(fts_stat);
  }

  // get_next_chunk() failure path cleanup:
  // ObIDDLPipeline::process() only calls finish_chunk() when get_next_chunk() succeeds,
  // so we must release allocated chunk here if we are going to return an error.
  if (OB_FAIL(ret) && OB_NOT_NULL(chunk)) {
    chunk->~ObChunk();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}

int ObDDLFinalMergeSortWriteTask::inner_add_monitor_info(storage::ObDDLDagMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObDDLFinalMergeSortWriteTaskMonitorInfo *info = nullptr;
  if (OB_FAIL(node.alloc_monitor_info(this, info))) {
    LOG_WARN("alloc monitor info failed", K(ret));
  } else if (OB_NOT_NULL(info)) {
    monitor_info_ = info;
    info->init_task_params(tablet_id_, slice_idx_);
  }
  return ret;
}

ObDDLFinalMergeSortWriteTaskMonitorInfo::ObDDLFinalMergeSortWriteTaskMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task)
  : ObDDLDagMonitorInfo(allocator, task),
    tablet_id_(),
    slice_idx_(-1),
    row_count_(0)
{
}

ObDDLFinalMergeSortWriteTaskMonitorInfo::~ObDDLFinalMergeSortWriteTaskMonitorInfo()
{
}

void ObDDLFinalMergeSortWriteTaskMonitorInfo::init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx)
{
  // Init once. Do not override if already set.
  if (!tablet_id_.is_valid() && slice_idx_ < 0) {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
  }
}

int ObDDLFinalMergeSortWriteTaskMonitorInfo::convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLDagMonitorInfo::convert_to_monitor_entry(entry))) {
    LOG_WARN("convert base monitor entry failed", K(ret));
  } else {
    ObJsonObject *json_obj = nullptr;
    ObJsonInt *tablet_id_node = nullptr;
    ObJsonInt *slice_idx_node = nullptr;
    ObJsonInt *row_cnt_node = nullptr;
    ObString json_str = entry.get_message();
    common::ObArenaAllocator &allocator = entry.get_allocator();
    if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_object_form_str(allocator, json_str, json_obj))) {
      LOG_WARN("failed to get json object from message", K(ret), K(json_str));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, tablet_id_.id(), tablet_id_node))
               || OB_FAIL(json_obj->add("tablet_id", tablet_id_node))) {
      LOG_WARN("failed to add tablet_id to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, slice_idx_, slice_idx_node))
               || OB_FAIL(json_obj->add("slice_idx", slice_idx_node))) {
      LOG_WARN("failed to add slice_idx to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, row_count_, row_cnt_node))
               || OB_FAIL(json_obj->add("row_count", row_cnt_node))) {
      LOG_WARN("failed to add row_count to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::print_json_to_str(allocator, json_obj, json_str))) {
      LOG_WARN("failed to print json to string", K(ret));
    } else if (OB_FAIL(entry.set_message(json_str))) {
      LOG_WARN("failed to set message", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
