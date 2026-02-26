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

#include "storage/ddl/ob_cg_micro_block_write_op.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
/**
* -----------------------------------ObCGMicroBlockWriter-----------------------------------
*/
int ObCGMicroBlockWriter::open(const ObWriteMacroParam &param,
                               ObCGBlockFile *cg_block_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg micro block writer has been initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() ||
                         nullptr == cg_block_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(param), KP(cg_block_file));
  } else if (OB_UNLIKELY(!cg_block_file->is_opened())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block file is not opened", K(ret));
  } else {
    if (OB_FAIL(cg_block_file_writer_.init(cg_block_file))) {
      LOG_WARN("fail to initialize cg block file", K(ret), KPC(cg_block_file));
    } else if (OB_FAIL(writer_args_.init(param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE))) {
      LOG_WARN("fail to initialize writer args", K(ret), K(param));
    } else if (OB_UNLIKELY(nullptr == writer_args_.object_cleaner_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object cleaner is null", K(ret));
    } else if (OB_FAIL(cg_micro_block_writer_.open(writer_args_.data_desc_.get_desc(),
                                                   writer_args_.parallel_idx_,
                                                   writer_args_.macro_seq_param_,
                                                   *writer_args_.object_cleaner_,
                                                   &cg_block_file_writer_))) {
      LOG_WARN("fail to open macro block writer", K(ret), K(writer_args_), K(cg_block_file_writer_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCGMicroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg micro block writer has not been initialized", K(ret));
  } else if (OB_FAIL(cg_micro_block_writer_.close())) {
    LOG_WARN("fail to close cg macro block writer", K(ret), K(writer_args_));
  }
  return ret;
}

void ObCGMicroBlockWriter::reset()
{
  is_inited_ = false;
  cg_block_file_writer_.reset();
  writer_args_.reset();
  cg_micro_block_writer_.reset();
}

int ObCGMicroBlockWriter::append_row(const ObDatumRow &row,
                                     const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg micro block writer has not been initialized", K(ret));
  } else if (OB_FAIL(cg_micro_block_writer_.append_row(row, curr_macro_desc))) {
    LOG_WARN("fail to append batch", K(ret));
  }
  return ret;
}

int ObCGMicroBlockWriter::append_batch(const ObBatchDatumRows &datum_rows,
                                       const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg micro block writer has not been initialized", K(ret));
  } else if (OB_FAIL(cg_micro_block_writer_.append_batch(datum_rows,
                                                         curr_macro_desc))) {
    LOG_WARN("fail to append batch", K(ret));
  }
  return ret;
}

/**
* -----------------------------------ObCGMicroBlockWriteOp-----------------------------------
*/
bool ObCGMicroBlockWriteOp::is_valid() const
{
  return is_inited_ && tablet_id_.is_valid() && slice_idx_ >= 0;
}

int ObCGMicroBlockWriteOp::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg micro block write Op has been initialized", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(tablet_id), K(slice_idx));
  } else {
    output_data_ = OB_NEWx(ObArray<ObCGBlockFile *>, &allocator_);
    if (OB_UNLIKELY(nullptr == output_data_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new output data", K(ret));
    } else {
      output_data_->set_block_allocator(ModulePageAllocator(allocator_));
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
        if (is_full_direct_load(ddl_dag->get_direct_load_type()) &&
                                ddl_dag->get_ddl_table_schema().table_item_.is_unique_index_) {
          unique_index_id_ = ddl_dag->get_ddl_table_schema().table_id_;
        }
        if (OB_UNLIKELY(nullptr == storage_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("storage schema is null", K(ret));
        } else {
          const int64_t cg_count = storage_schema->get_column_group_count();
          if (cg_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cg count is invalid", K(ret), K(cg_count));
          } else if (OB_FAIL(cg_block_file_arr_.prepare_allocate(cg_count))) {
            LOG_WARN("fail to prepare allocate cg block files", K(ret), K(cg_count));
          } else {
            for (int i = 0; i < cg_count; ++i) {
              cg_block_file_arr_.at(i) = nullptr;
            }
            is_inited_ = true;
          }
        }
      }
    }
  }
  return ret;
}

void ObCGMicroBlockWriteOp::reset()
{
  is_inited_ = false;
  tablet_id_.reset();
  slice_idx_ = -1;
  if (nullptr != output_data_) {
    for (int i = 0; i < output_data_->count(); ++i) {
      ObCGBlockFile *&cg_block_file = output_data_->at(i);
      if (nullptr != cg_block_file) {
        cg_block_file->~ObCGBlockFile();
        ob_free(cg_block_file);
        cg_block_file = nullptr;
      }
    }
    output_data_->reset();
  }
  for (int i = 0; i < cg_block_file_arr_.count(); ++i) {
    ObCGBlockFile *&cg_block_file = cg_block_file_arr_.at(i);
    if (nullptr != cg_block_file) {
      cg_block_file->~ObCGBlockFile();
      ob_free(cg_block_file);
      cg_block_file = nullptr;
    }
  }
  cg_block_file_arr_.reset();
  cg_micro_block_writer_.reset();
  unique_index_id_ = 0;
  allocator_.reset();
}

// start protected func
int ObCGMicroBlockWriteOp::execute(const ObChunk &input_chunk,
                                   ResultState &result_state,
                                   ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  cg_micro_block_writer_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the micro block write Op is not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid()) ||
                         (!input_chunk.is_end_chunk() && !input_chunk.is_cg_row_tmp_files_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input chunk is not valid", K(ret), K(input_chunk));
  } else if (input_chunk.is_end_chunk()) {
    // by pass
  } else {
    ObArray<ObCGRowFile *> *cg_row_file_arr = input_chunk.cg_row_file_arr_;
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_UNLIKELY(nullptr == ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_row_file_arr->count(); ++i) {
      ObCGBlockFile *cg_block_file = nullptr;
      ObCGRowFile *&cg_row_file = cg_row_file_arr->at(i);
      if (OB_UNLIKELY(nullptr == cg_row_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg row file is null", K(ret), K(i));
      } else {
        ObWriteMacroParam write_macro_param;
        const int64_t cg_idx = cg_row_file->get_cg_idx();
        if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= cg_block_file_arr_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cg idx is invalid", K(ret), K(cg_idx), K(cg_block_file_arr_.count()));
        } else {
          cg_block_file = cg_block_file_arr_.at(cg_idx);
          if (nullptr == cg_block_file) {
            cg_block_file = OB_NEW(ObCGBlockFile, ObMemAttr(MTL_ID(), "CGBlockFile"));
            if (OB_UNLIKELY(nullptr == cg_block_file)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to new cg block file", K(ret));
            } else if (OB_FAIL(cg_block_file->open(tablet_id_, slice_idx_, 0/*scan_idx*/, cg_idx))) {
              LOG_WARN("fail to open cg block file", K(ret), K(tablet_id_), K(slice_idx_), K(cg_idx));
            } else {
              cg_block_file_arr_.at(cg_idx) = cg_block_file;
            }
            if (OB_FAIL(ret) && nullptr != cg_block_file) {
              cg_block_file->~ObCGBlockFile();
              ob_free(cg_block_file);
              cg_block_file = nullptr;
            }
          }
          if (FAILEDx(ObDDLUtil::fill_writer_param(tablet_id_,
                                                   slice_idx_,
                                                   cg_idx,
                                                   ddl_dag,
                                                   0/*max_batch_size*/,
                                                   write_macro_param))) {
            LOG_WARN("fail to fill write macro param",
                K(ret), KPC(ddl_dag), K(tablet_id_), K(slice_idx_), K(cg_idx));
          } else if (OB_FAIL(cg_micro_block_writer_.open(write_macro_param, cg_block_file))) {
            LOG_WARN("fail to initialize cg macro block writer", K(ret), K(write_macro_param), KPC(cg_block_file));
          }
        }
      }
      if (FAILEDx(append_cg_row_file_to_writer(cg_row_file))) {
        LOG_WARN("fail to append cg row file to writer", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < cg_row_file_arr->count(); ++i) {
        ObCGRowFile *&cg_row_file = cg_row_file_arr->at(i);
        if (nullptr != cg_row_file) {
          cg_row_file->~ObCGRowFile();
          ob_free(cg_row_file);
          cg_row_file = nullptr;
        }
      }
    }
  }
  if (FAILEDx(try_generate_output_chunk(input_chunk, result_state, output_chunk))) {
    LOG_WARN("fail to try generate output chunk", K(ret));
  }
  return ret;
}

// start private func
int ObCGMicroBlockWriteOp::append_cg_row_file_to_writer(ObCGRowFile *&cg_row_file)
{
  int ret = OB_SUCCESS;
  ObBatchDatumRows *cg_bdrs_ptr = nullptr;
  if (OB_UNLIKELY(nullptr == cg_row_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg row file is null", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(cg_row_file->get_next_batch(cg_bdrs_ptr))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cg_row_file->close())) {
          LOG_WARN("fail to close cg row file", K(ret), KPC(cg_row_file));
        } else {
          cg_row_file->~ObCGRowFile();
          ob_free(cg_row_file);
          cg_row_file = nullptr;
        }
        break;
      } else {
        LOG_WARN("fail to get next batch", K(ret), KPC(cg_row_file));
      }
    } else if (OB_FAIL(cg_micro_block_writer_.append_batch(*cg_bdrs_ptr))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && unique_index_id_ > 0) {
        int report_ret_code = OB_SUCCESS;
        LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
        (void) ObDirectLoadSliceWriter::report_unique_key_dumplicated(ret, unique_index_id_, *cg_bdrs_ptr, tablet_id_, report_ret_code); // ignore ret
        if (OB_ERR_DUPLICATED_UNIQUE_KEY == report_ret_code) {
          //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
          ret = OB_ERR_DUPLICATED_UNIQUE_KEY;
        }
      } else {
        LOG_WARN("fail to append batch", K(ret), KPC(cg_bdrs_ptr));
      }
    }
  }
  if (FAILEDx(cg_micro_block_writer_.close())) {
    LOG_WARN("fail to close cg macro block writer", K(ret));
  } else {
    cg_micro_block_writer_.reset();
  }
  return ret;
}

int ObCGMicroBlockWriteOp::try_generate_output_chunk(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_data_->reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_block_file_arr_.count(); ++i) {
    ObCGBlockFile *&cg_block_file = cg_block_file_arr_.at(i);
    if (nullptr != cg_block_file &&
        (input_chunk.is_end_chunk() || cg_block_file->get_data_size() >= WRITE_MACRO_THRESHOLD)) {
      if (OB_FAIL(output_data_->push_back(cg_block_file))) {
        LOG_WARN("fail to push back cg block files", K(ret), KPC(cg_block_file));
      } else {
        cg_block_file = nullptr;
      }
    }
  }
  if (OB_SUCC(ret)) { // set output chunk
    if (!output_data_->empty()) {
      output_chunk.type_ = ObChunk::MACRO_BUFFER;
      output_chunk.cg_block_file_arr_ = output_data_;
    }
  }
  return ret;
}

} //end storage
} // end oceanbase
