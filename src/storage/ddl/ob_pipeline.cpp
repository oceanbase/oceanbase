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

#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_ddl_pipeline.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::table;

ObChunk::~ObChunk()
{
  if (type_ == DIRECT_LOAD_BATCH_DATUM_ROWS && direct_load_batch_rows_ != nullptr) {
    direct_load_batch_rows_->~ObDirectLoadBatchDatumRows();
    ob_free(direct_load_batch_rows_);
    direct_load_batch_rows_ = nullptr;
    type_ = INVALID_TYPE;
  } else if (CG_ROW_TMP_FILES == type_ && nullptr != cg_row_file_arr_) {
    for (int64_t i = 0; i < cg_row_file_arr_->count(); ++i) {
      ObCGRowFile *&cg_row_file = cg_row_file_arr_->at(i);
      if (nullptr != cg_row_file) {
        cg_row_file->~ObCGRowFile();
        ob_free(cg_row_file);
        cg_row_file = nullptr;
      }
    }
    cg_row_file_arr_->~ObArray<ObCGRowFile *>();
    ob_free(cg_row_file_arr_);
    cg_row_file_arr_ = nullptr;
    type_ = INVALID_TYPE;
  } else if (DIRECT_LOAD_ROW_ARRAY == type_) {
    OB_DELETE(ObTableLoadTabletObjRowArray, ObMemAttr(MTL_ID(), "TLD_RowArray"), row_array_);
  }
}

void ObChunk::reset()
{
  type_ = INVALID_TYPE;
  data_ptr_ = nullptr;
}

bool ObChunk::is_valid() const
{
  bool bret = type_ > ChunkType::INVALID_TYPE && type_ < ChunkType::MAX_TYPE;
  if (bret) {
    switch (type_) {
      case ChunkType::DATUM_ROW:
      case ChunkType::MACRO_BUFFER:
      case ChunkType::DIRECT_LOAD_BATCH_DATUM_ROWS:
      case ChunkType::CG_ROW_TMP_FILES:
      case ChunkType::BATCH_DATUM_ROWS:
      case ChunkType::DIRECT_LOAD_ROW_ARRAY:
      case ChunkType::TASK_BATCH_INFO:
        bret = nullptr != data_ptr_;
        break;
      default:
        break;
    }
  }
  return bret;
}

int ObChunk::get_dag_tablet_context(ObDDLTabletContext *&tablet_context) const
{
  int ret = OB_SUCCESS;
  tablet_context = nullptr;
  if (ObChunk::DAG_TABLET_CONTEXT != type_ || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(this));
  } else {
    tablet_context = reinterpret_cast<ObDDLTabletContext *>(data_ptr_);
  }
  return ret;
}

int ObPipelineOperator::execute_op(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  if (OB_UNLIKELY(!input_chunk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(input_chunk));
  } else if (OB_FAIL(execute(input_chunk, result_state, output_chunk))) {
    LOG_WARN("fail to execute operator", K(ret));
  } else if (OB_FAIL(try_execute_finish(input_chunk, result_state, output_chunk))) {
    LOG_WARN("fail to execute finish", K(ret));
  }
  return ret;
}

int ObPipelineOperator::try_execute_finish(const ObChunk &input_chunk,
                                           ResultState &result_state,
                                           ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input_chunk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input chunk is not valid", K(ret), K(input_chunk));
  } else if (OB_UNLIKELY(input_chunk.is_end_chunk() &&
                         ObPipelineOperator::HAVE_MORE_OUTPUT == result_state)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("the operator state is not matching", K(ret));
  } else if (!input_chunk.is_end_chunk()) {
    // by pass
  } else if (output_chunk.is_valid()) { // has output data
    result_state = ObPipelineOperator::HAVE_MORE_OUTPUT;
  } else {
    output_chunk.set_end_chunk();
  }
  return ret;
}

int ObPipeline::add_op(ObPipelineOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == op || !op->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(op));
  } else if (OB_FAIL(ops_.push_back(op))) {
    LOG_WARN("push back operator failed", K(ret), KPC(op));
  }
  return ret;
}

int ObPipeline::push(const ObChunk &chunk_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!chunk_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(chunk_data));
  } else if (OB_FAIL(execute_ops(0, chunk_data))) {
    LOG_WARN("execute operators from start failed", K(ret), K(chunk_data));
  }
  return ret;
}

int ObPipeline::execute_ops(const int64_t start_pos, const ObChunk &chunk_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ops_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(ops_.count()));
  } else if (start_pos == ops_.count()) {
    /* reach boudnary, quit */
  } else if (OB_UNLIKELY(start_pos < 0 || start_pos >= ops_.count() || !chunk_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid  argument", K(ret), K(start_pos), K(ops_.count()), K(chunk_data), K(chunk_data.is_valid()));
  } else {
    ObChunk input_chunk = chunk_data;
    ObChunk output_chunk;
    int64_t op_idx = start_pos;
    while (OB_SUCC(ret) && op_idx < ops_.count()) {
      ObPipelineOperator *curr_op = ops_.at(op_idx);
      if (OB_ISNULL(curr_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current op is null", K(ret), KP(curr_op), K(op_idx));
      } else if (input_chunk.is_valid()) {
        ObPipelineOperator::ResultState result_state = ObPipelineOperator::ResultState::INVALID_VALUE;
        if (OB_FAIL(curr_op->execute_op(input_chunk, result_state, output_chunk))) {
          LOG_WARN("current op execute failed", K(ret), K(input_chunk));
        }

        if (OB_FAIL(ret)) {
        } else if (ObPipelineOperator::NEED_MORE_INPUT == result_state) {
          if (output_chunk.is_valid()) {
            input_chunk = output_chunk;
            ++op_idx;
          } else {
            break;
          }
        } else if (ObPipelineOperator::HAVE_MORE_OUTPUT == result_state) {
          if (!output_chunk.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("output chunk is null", K(ret));
          } else {
            int64_t next_op_idx = op_idx + 1;
            const ObChunk &tmp_input_chunk = output_chunk;
            if (OB_FAIL(execute_ops(next_op_idx, tmp_input_chunk))) {
              LOG_WARN("execute ops failed", K(ret), K(next_op_idx), K(tmp_input_chunk));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid result state", K(ret), K(result_state));
        }
      }
    }
    input_chunk.reset();
    output_chunk.reset();
  }
  return ret;
}

ObIDag *ObPipelineOperator::get_dag()
{
  return nullptr == pipeline_ ? nullptr : pipeline_->get_dag();
}
