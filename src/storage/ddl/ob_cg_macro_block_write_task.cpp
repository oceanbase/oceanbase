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
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "storage/ddl/ob_tablet_slice_row_iterator.h"
#include "src/storage/ddl/ob_column_clustered_dag.h"
#include "storage/blocksstable/ob_logic_macro_id.h" // for ObMacroDataSeq
#include "storage/direct_load/ob_direct_load_batch_datum_rows.h"
#include "src/storage/ddl/ob_tablet_ddl_kv_mgr.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

/**
* -----------------------------------ObCgMacroBlockWriteTask-----------------------------------
*/

ObCgMacroBlockWriteTask::ObCgMacroBlockWriteTask(const ObITaskType type) :
    ObITask(type),
    is_inited_(false),
    allocator_(ObMemAttr(MTL_ID(), "CGMBWriteTask")),
    storage_schema_(nullptr),
    row_iter_(nullptr)
{
}

ObCgMacroBlockWriteTask::ObCgMacroBlockWriteTask()
  : ObCgMacroBlockWriteTask(TASK_TYPE_DDL_WRITE_CG_MACRO_BLOCK)
{
}

ObCgMacroBlockWriteTask::~ObCgMacroBlockWriteTask()
{
  for (int64_t i = 0; i < cg_macro_block_writers_.count(); ++i) {
    ObCgMacroBlockWriter *cur_writer = cg_macro_block_writers_.at(i);
    if (OB_NOT_NULL(cur_writer)) {
      cur_writer->~ObCgMacroBlockWriter();
    }
  }
  cg_macro_block_writers_.reset();
}

int ObCgMacroBlockWriteTask::init(
    ObDDLIndependentDag *ddl_dag,
    ObITabletSliceRowIterator *row_iter,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == ddl_dag
        || nullptr == row_iter
        || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), KP(ddl_dag), KP(row_iter), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    row_iter_ = row_iter;
    const int64_t slice_idx = row_iter_->get_slice_idx();
    ObWriteMacroParam param;
    if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id, slice_idx, -1/*cg_idx*/, ddl_dag, 0/*max_batch_size*/, param))) {
      LOG_WARN("fill writer param common failed", K(ret), K(tablet_id), K(param));
    } else if (OB_FAIL(ObDDLUtil::init_cg_macro_block_writers(param, allocator_, storage_schema_, cg_macro_block_writers_))) {
      LOG_WARN("init cg macro block writer failed", K(ret), K(tablet_id_), K(slice_idx), KPC(ddl_dag));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCgMacroBlockWriteTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    const ObDatumRow *row = nullptr;
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema_->get_column_groups();
    ObDatumRow cg_row;
    cg_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(row_iter_->get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else {
        for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
          ObCgMacroBlockWriter *cg_macro_block_writer = cg_macro_block_writers_.at(cg_idx);
          const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(cg_idx);
          if (OB_ISNULL(cg_macro_block_writer)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cg macro block writer is null", K(ret), K(cg_idx));
          } else if (OB_FAIL(project_cg_row(cg_schema, *row, cg_row))) {
            LOG_WARN("fail to project cg row", K(ret), K(row), K(cg_schema));
          } else if (OB_FAIL(cg_macro_block_writer->append_row(cg_row))) {
            LOG_WARN("fail to append row", K(ret), K(cg_row), KPC(cg_macro_block_writer));
          }
        }
      }
    }
    // TODO@wenqu: report_unique_key_dumplicated

    for (int64_t i = 0; OB_SUCC(ret) && i < cg_macro_block_writers_.count(); ++i) {
      ObCgMacroBlockWriter *cg_macro_block_writer = cg_macro_block_writers_.at(i);
      if (OB_FAIL(cg_macro_block_writer->close())) {
        LOG_WARN("fail to close macro block writer", K(ret), KPC(cg_macro_block_writer));
      }
    }
  }
  FLOG_INFO("cg macro block write task finished", K(ret), K(tablet_id_), K(cg_macro_block_writers_.count()));
  return ret;
}

int ObCgMacroBlockWriteTask::project_cg_row(
    const ObStorageColumnGroupSchema &cg_schema,
    const ObDatumRow &row,
    ObDatumRow &cg_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cg_schema.is_valid() || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(cg_schema), K(row));
  } else {
    int64_t column_idx = cg_schema.get_column_idx(0);
    cg_row.storage_datums_ = row.storage_datums_ + column_idx;
    cg_row.count_ = cg_schema.get_column_count();
  }
  return ret;
}

ObDDLScanTask::ObDDLScanTask(const ObITaskType type)
  : ObITask(type),
    ddl_dag_(nullptr)
{
}

ObDDLScanTask::ObDDLScanTask()
  : ObDDLScanTask(TASK_TYPE_DDL_PREPARE_SCAN)
{
}

ObDDLScanTask::~ObDDLScanTask()
{

}

int ObDDLScanTask::init(ObDDLIndependentDag *ddl_dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ddl_dag_ = ddl_dag;
  }
  return ret;
}

ObITask::ObITaskPriority ObDDLScanTask::get_priority()
{
  int ret = OB_SUCCESS;
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else {
    priority = ddl_dag_->is_scan_finished() && 0 == ddl_dag_->get_pipeline_count() ? ObITask::TASK_PRIO_2 : ObITask::TASK_PRIO_0;
  }
  return priority;
}

int ObDDLScanTask::process()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

ObDDLTabletScanTask::ObDDLTabletScanTask()
  : ObITask(TASK_TYPE_DDL_PREPARE_SCAN)
{
}

bool ObWriteMacroBaseOperator::is_valid() const
{
  return is_inited_;
}

int ObWriteMacroBaseOperator::init(const ObWriteMacroParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(slice_writer_.init(param))) {
    LOG_WARN("init slice writer failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

/**
* -----------------------------------ObDDLWriteMacroBlockOperator-----------------------------------
*/
int ObDDLWriteMacroBlockOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObDDLWriteMacroBlockOperator has been not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid() ||
                         (!input_chunk.is_end_chunk() && !input_chunk.is_direct_load_batch_datum_rows_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(input_chunk));
  } else if (input_chunk.is_end_chunk()) {
    if (OB_FAIL(slice_writer_.close())) {
      LOG_WARN("fail to close slice writer", K(ret));
    }
  } else if (OB_FAIL(slice_writer_.append_batch(input_chunk.direct_load_batch_rows_->datum_rows_))) {
    LOG_WARN("fail to append batch into slice writer", K(ret), K(input_chunk));
  }
  return ret;
}


/**
* -----------------------------------ObCGRowFileWriterOp-----------------------------------
*/
int ObCGRowFileWriterOp::init(const ObTabletID &tablet_id,
                              const int64_t slice_idx,
                              const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg micro block write Op has been initialized", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0 || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(tablet_id), K(slice_idx), K(max_batch_size));
  } else if (OB_UNLIKELY(nullptr == get_dag() ||
                         share::ObDagType::DAG_TYPE_DDL != get_dag()->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the dag is null or dag type is not ddl dag", K(ret), KP(get_dag()));
  } else {
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    ObDDLTabletContext *tablet_context = nullptr;
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    if (OB_UNLIKELY(nullptr == ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret));
    } else if (OB_FAIL(ddl_dag->get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("fail to get tablet context", K(ret), K(tablet_id_));
    } else if (OB_UNLIKELY(nullptr == tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret));
    } else {
      ObStorageSchema *storage_schema = tablet_context->tablet_param_.with_cs_replica_ ?
                                        tablet_context->tablet_param_.cs_replica_storage_schema_ :
                                        tablet_context->tablet_param_.storage_schema_;
      if (OB_UNLIKELY(nullptr == storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is null", K(ret));
      } else if (OB_FAIL(cg_row_files_generater_.init(tablet_id_,
                                                      slice_idx_,
                                                      storage_schema,
                                                      max_batch_size,
                                                      ObCGRowFilesGenerater::CG_ROW_FILE_MEMORY_LIMIT,
                                                      ddl_dag->get_ddl_table_schema().column_items_,
                                                      true/*is_generation_sync_output*/,
                                                      false/*is_sorted_table_load_with_column_store_replica*/))) {
        LOG_WARN("fail to initialize cg row files generater",
            K(ret), K(tablet_id_), K(slice_idx_),KP(storage_schema), K(max_batch_size));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObCGRowFileWriterOp::execute(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObCGRowFileWriterOp has been not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid() ||
                         (!input_chunk.is_batch_datum_rows_type() && !input_chunk.is_end_chunk()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(input_chunk));
  } else {
    ObDDLChunk ddl_output_chunk;
    const ObBatchDatumRows &bdrs = input_chunk.is_end_chunk() ? ObBatchDatumRows() : *input_chunk.bdrs_;
    if (OB_FAIL(cg_row_files_generater_.append_batch(bdrs, input_chunk.is_end_chunk(), ddl_output_chunk))) {
      LOG_WARN("fail to append batch into cg row files generater", K(ret), K(bdrs));
    } else if (ddl_output_chunk.is_valid()) {
      if (OB_LIKELY(ddl_output_chunk.has_chunk_data())) {
        output_chunk = *ddl_output_chunk.chunk_data_;
        ddl_output_chunk.chunk_data_->reset();
      }
    }
  }
  return ret;
}

/**
* -----------------------------------ObDDLWriteMacroBlockBasePipeline-----------------------------------
*/
int ObDDLWriteMacroBlockBasePipeline::get_next_chunk(ObChunk *&next_chunk)
{
  int ret = OB_SUCCESS;
  next_chunk = nullptr;
  static const int64_t timeout_us = 1000L; // 1ms
  if (OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl slice is null", K(ret), KPC(ddl_slice_));
  } else if (OB_FAIL(ddl_slice_->pop_chunk(next_chunk))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pop ddl chunk failed", K(ret));
    }
  }
  return ret;
}

ObITask::ObITaskPriority ObDDLWriteMacroBlockBasePipeline::get_priority()
{
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  if (nullptr != ddl_slice_ && nullptr != dag_) {
    if (ddl_slice_->get_queue_size() > 0 || ddl_slice_->has_end_chunk()) {
      priority = ObITask::TASK_PRIO_1;
    } else {
      priority = ObITask::TASK_PRIO_0;
    }
  }
  return priority;
}

int ObDDLWriteMacroBlockBasePipeline::finish_chunk(ObChunk *chunk)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(chunk)) {
    chunk->~ObChunk();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}

int ObDDLWriteMacroBlockBasePipeline::fill_writer_param(ObWriteMacroParam &param)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = nullptr;
  if (OB_ISNULL(dag = static_cast<ObDDLIndependentDag *>(get_dag())) || OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, dag must not be nullptr", K(ret), KPC(get_dag()), KPC(ddl_slice_));
  } else if (OB_FAIL(ObDDLUtil::fill_writer_param(ddl_slice_->get_tablet_id(),
                                                  ddl_slice_->get_slice_idx(),
                                                  -1/*cg_idx*/,
                                                  dag,
                                                  0/*max_batch_size*/,
                                                  param))) {
    LOG_WARN("fill writer param common failed", K(ret));
  }
  return ret;
}

void ObDDLWriteMacroBlockBasePipeline::postprocess(int &ret_code)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
  if (OB_ITER_END != ret_code
      && OB_DAG_TASK_IS_SUSPENDED != ret_code) {
    FLOG_INFO("ret code not expected", K(ret_code), KPC(this), KPC(dag));
  } else if (OB_ITER_END == ret_code) {
    ret_code = OB_SUCCESS;
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_ISNULL(dag) || OB_ISNULL(ddl_slice_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("get dag failed", K(ret), KPC(get_dag()), KPC(ddl_slice_));
    } else if (OB_FAIL(dag->get_tablet_context(ddl_slice_->get_tablet_id(), tablet_context))) {
      LOG_WARN("get tablet context failed", K(ret), KPC(ddl_slice_));
    } else if (OB_FAIL(set_remain_block())) {
      LOG_WARN("set remain block failed", K(ret));
    } else {
      LOG_INFO("not data any more, change ret to be success", K(ret), K(dag->get_ddl_task_param()));
      ret_code = OB_SUCCESS;
    }
  }
  if (OB_DAG_TASK_IS_SUSPENDED != ret_code) {
    // pipeline exit
    if (OB_NOT_NULL(dag)) {
      dag->dec_pipeline_count();
    }
  }
}

/**
* -----------------------------------ObDDLMemoryFriendWriteMacroBlockPipeline-----------------------------------
*/
int ObDDLMemoryFriendWriteMacroBlockPipeline::init(ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_slice || !ddl_slice->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KPC(ddl_slice));
  } else {
    ddl_slice_ = ddl_slice;
    if (OB_FAIL(fill_writer_param(write_param_))) {
      LOG_WARN("fill writer param failed", K(ret));
    } else if (OB_FAIL(micro_write_op_.init(ddl_slice_->get_tablet_id(),
                                            ddl_slice_->get_slice_idx()))) {
      LOG_WARN("fail to initialize micro write operator", K(ret));
    } else if (OB_FAIL(add_op(&micro_write_op_))) {
      LOG_WARN("fail to add micro write op", K(ret));
    } else if (OB_FAIL(macro_write_op_.init(ddl_slice_->get_tablet_id(),
                                            ddl_slice_->get_slice_idx()))) {
      LOG_WARN("fail to initialize macro write operator", K(ret));
    } else if (OB_FAIL(add_op(&macro_write_op_))) {
      LOG_WARN("fail to add macro write op", K(ret));
    }
  }
  return ret;
}

int ObDDLMemoryFriendWriteMacroBlockPipeline::set_remain_block()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl slice is null", K(ret));
  } else if (OB_FAIL(macro_write_op_.set_remain_block(ddl_slice_))) {
    LOG_WARN("set remain block failed", K(ret));
  }
  return ret;
}

/**
* -----------------------------------ObBatchDatumRowsWriteOp-----------------------------------
*/
int ObBatchDatumRowsWriteOp::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(tablet_id), K(slice_idx));
  } else if (OB_UNLIKELY(nullptr == get_dag() ||
                         share::ObDagType::DAG_TYPE_DDL != get_dag()->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the dag is null or dag type is not ddl dag", K(ret), KP(get_dag()));
  } else {
    ObDirectLoadRowFlag row_flag;
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    if (OB_UNLIKELY(nullptr == ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret));
    } else if (OB_FAIL(buffer_.init(ddl_dag->get_ddl_table_schema().column_items_,
                                    MAX_BATCH_SIZE,
                                    row_flag))) {
      LOG_WARN("fail to initialize buffer", K(ret));
    } else {
      const ObIArray<ObDirectLoadVector *> &vectors = buffer_.get_vectors();
      for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
        if (OB_FAIL(bdrs_.vectors_.push_back(vectors.at(i)->get_vector()))) {
          LOG_WARN("fail to push back vector", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObBatchDatumRowsWriteOp::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObBatchDatumRowsWriteOp is not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid() ||
                         (!input_chunk.is_end_chunk() && !input_chunk.is_datum_row_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the input chunk is invalid", K(ret), K(input_chunk));
  } else {
    if (buffer_need_reuse_) {
      buffer_.reuse();
      buffer_need_reuse_ = false;
    }
    if (input_chunk.is_end_chunk()) {
      if (OB_FAIL(generate_data_chunk(output_chunk))) {
        LOG_WARN("fail to generate output chunk when receive end chunk", K(ret));
      }
    } else {
      if (OB_FAIL(buffer_.append_row(*input_chunk.datum_row_))) {
        LOG_WARN("fail to append row", K(ret));
      } else if (buffer_.full() && OB_FAIL(generate_data_chunk(output_chunk))) {
        LOG_WARN("fail to generate output chunk", K(ret));
      }
    }
  }
  return ret;
}

int ObBatchDatumRowsWriteOp::generate_data_chunk(ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  if (OB_LIKELY(buffer_.size() > 0)) {
    buffer_need_reuse_ = true;
    bdrs_.row_count_ = buffer_.size();
    output_chunk.type_ = ObChunk::BATCH_DATUM_ROWS;
    output_chunk.bdrs_ = &bdrs_;
  }
  return ret;
}
