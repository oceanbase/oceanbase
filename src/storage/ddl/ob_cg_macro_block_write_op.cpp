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

#include "storage/ddl/ob_cg_macro_block_write_op.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_cg_micro_block_write_op.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

 namespace oceanbase
{
using namespace oceanbase::blocksstable;
namespace storage
{
/**
* -----------------------------------ObDAGCGMacroBlockWriter-----------------------------------
*/
int ObDAGCGMacroBlockWriter::open(const ObWriteMacroParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the obj has been initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(param));
  } else if (OB_FAIL(writer_args_.init(param, ObWriterType::DAG_CG_MACRO_BLOCK_WRITER_TYPE))) {
    LOG_WARN("fail to initialize writer args", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == writer_args_.object_cleaner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object cleaner is null", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(cg_macro_block_writer_.open_for_ss_ddl(writer_args_.data_desc_.get_desc(),
                                                       writer_args_.parallel_idx_,
                                                       writer_args_.macro_seq_param_,
                                                       *writer_args_.object_cleaner_,
                                                       writer_args_.ddl_redo_callback_))) {
      LOG_WARN("fail to open macro block writer in ss mode", K(ret), K(writer_args_));
    }
#endif
  } else if (OB_FAIL(cg_macro_block_writer_.open(writer_args_.data_desc_.get_desc(),
                                                  writer_args_.parallel_idx_,
                                                  writer_args_.macro_seq_param_,
                                                  *writer_args_.object_cleaner_,
                                                  writer_args_.ddl_redo_callback_))) {
    LOG_WARN("fail to open macro block writer", K(ret), K(writer_args_));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObDAGCGMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObDAGCGMacroBlockWriter has been not inilialized", K(ret));
  } else if (OB_FAIL(cg_macro_block_writer_.close())) {
    LOG_WARN("fail to close cg macro block writer", K(ret));
  }
  return ret;
}

void ObDAGCGMacroBlockWriter::reset()
{
  is_inited_ = false;
  writer_args_.reset();
  cg_macro_block_writer_.reset();
}

int ObDAGCGMacroBlockWriter::append_cg_block(ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObDAGCGMacroBlockWriter has been not inilialized", K(ret));
  } else if (OB_FAIL(cg_macro_block_writer_.append_cg_block(cg_block, MACRO_BLOCK_REUSE_THRESHOLD))) {
    LOG_WARN("fail to append cg block", K(ret), K(cg_block));
  }
  return ret;
}

/**
* -----------------------------------ObDAGCGMacroBlockWriteOp-----------------------------------
*/
bool ObDAGCGMacroBlockWriteOp::is_valid() const
{
  return is_inited_ && tablet_id_.is_valid() && slice_idx_ >= 0;
}

int ObDAGCGMacroBlockWriteOp::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initialized twice", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) || slice_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_UNLIKELY(nullptr == ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret));
    } else if (OB_FAIL(ddl_dag->get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("fail to get tablet context", K(ret), K(tablet_id_));
    } else if (OB_UNLIKELY(nullptr == tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret), K(tablet_id_));
    } else {
      ObStorageSchema *storage_schema = tablet_context->tablet_param_.with_cs_replica_ ?
                                        tablet_context->tablet_param_.cs_replica_storage_schema_ :
                                        tablet_context->tablet_param_.storage_schema_;
      if (OB_UNLIKELY(nullptr == storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage schema is null", K(ret), K(tablet_id_), K(slice_idx));
      } else {
        const int64_t cg_count = storage_schema->get_column_group_count();
        if (OB_FAIL(flushed_bitmap_.init(cg_count, false))) {
          LOG_WARN("init flush bitmap failed", K(ret));
        } else if (OB_FAIL(cg_block_files_iter_arr_.prepare_allocate(cg_count))) {
          LOG_WARN("fail to prepare allocate cg block files iterator array", K(ret), K(cg_count));
        } else if (OB_FAIL(start_seqences_.prepare_allocate(cg_count))) {
          LOG_WARN("fail to prepare allocate start seqences", K(ret), K(cg_count));
        } else if (OB_FAIL(row_offsets_.prepare_allocate(cg_count))) {
          LOG_WARN("fail to prepare allocate row offsets", K(ret), K(cg_count));
        } else {
          for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_count; ++cg_idx) {
            ObCGBlockFilesIterator *cg_block_files_iter = OB_NEWx(ObCGBlockFilesIterator, &allocator_);
            if (nullptr == cg_block_files_iter) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate cg block files iterator", K(ret), K(cg_idx));
            } else {
              cg_block_files_iter_arr_.at(cg_idx) = cg_block_files_iter;
              row_offsets_.at(cg_idx) = 0;
            }
            if (FAILEDx(ObDDLUtil::init_macro_block_seq(ddl_dag->get_direct_load_type(),
                                                        tablet_id,
                                                        slice_idx_,
                                                        start_seqences_.at(cg_idx)))) {
              LOG_WARN("fail to initialize start seqence", K(ret), K(ddl_dag->get_direct_load_type()),
                                                           K(tablet_id), K(slice_idx_));
            }
          }
          if (OB_SUCC(ret)) {
            is_inited_ = true;
          }
        }
      }
    }
  }
  return ret;
}

void ObDAGCGMacroBlockWriteOp::reset()
{
  is_inited_ = false;
  tablet_id_.reset();
  slice_idx_ = -1;
  for (int64_t i = 0; i < cg_block_files_iter_arr_.count(); ++i) {
    ObCGBlockFilesIterator *&cg_block_files_iter = cg_block_files_iter_arr_.at(i);
    if (OB_UNLIKELY(nullptr != cg_block_files_iter)) {
      cg_block_files_iter->~ObCGBlockFilesIterator();
      cg_block_files_iter = nullptr;
    }
  }
  cg_block_files_iter_arr_.reset();
  cg_macro_block_writer_.reset();
  flushed_bitmap_.reuse(false);
  allocator_.reset();
}

// start protected func
int ObDAGCGMacroBlockWriteOp::execute(const ObChunk &input_chunk,
                                      ResultState &result_state,
                                      ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  cg_macro_block_writer_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDAGCGMacroBlockWriteOp is not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid()) ||
                         (!input_chunk.is_end_chunk() && !input_chunk.is_macro_buffer_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input chunk is not valid", K(ret), K(input_chunk));
  } else {
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_UNLIKELY(nullptr == ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret));
    } else if (!input_chunk.is_end_chunk()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < input_chunk.cg_block_file_arr_->count(); ++i) {
        ObCGBlockFile *&cg_block_file = input_chunk.cg_block_file_arr_->at(i);
        if (OB_UNLIKELY(nullptr == cg_block_file)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cg block file is null", K(ret));
        } else {
          int64_t cg_idx = cg_block_file->get_cg_idx();
          if (OB_UNLIKELY(cg_idx < 0 ||
                          cg_idx >= cg_block_files_iter_arr_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cg idx is invalid",
                K(ret), K(cg_idx), K(cg_block_files_iter_arr_.count()));
          } else if (OB_FAIL(cg_block_files_iter_arr_.at(cg_idx)->push_back_cg_block_file(cg_block_file))) {
            LOG_WARN("fail to push back cg block files", K(ret), KPC(cg_block_file), K(cg_idx));
          } else {
            cg_block_file = nullptr;
          }
        }
      }
    }
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_block_files_iter_arr_.count(); ++cg_idx) {
      ObCGBlockFilesIterator *cg_block_files_iter = cg_block_files_iter_arr_.at(cg_idx);
      while (OB_SUCC(ret) &&
             ((input_chunk.is_end_chunk() && cg_block_files_iter->get_total_data_size() > 0) ||
             cg_block_files_iter->get_total_data_size() >= ObCGMicroBlockWriteOp::WRITE_MACRO_THRESHOLD)) {
        const bool have_flushed_macro_block = flushed_bitmap_.test(cg_idx);
        ObWriteMacroParam write_macro_param;
        write_macro_param.start_sequence_ = start_seqences_.at(cg_idx);
        write_macro_param.row_offset_ = row_offsets_.at(cg_idx);
        if (input_chunk.is_end_chunk() && !have_flushed_macro_block) {
          LOG_TRACE("remain small cg block for group write", K(tablet_id_), K(slice_idx_), K(cg_idx), "remain_size", cg_block_files_iter->get_total_data_size());
          break;
        } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_,
                                                 slice_idx_,
                                                 cg_idx,
                                                 ddl_dag,
                                                 0/*max_batch_size*/,
                                                 write_macro_param))) {
          LOG_WARN("fail to fill write macro param",
              K(ret), KPC(ddl_dag), K(tablet_id_), K(slice_idx_), K(cg_idx));
        } else if (OB_FAIL(cg_macro_block_writer_.open(write_macro_param))) {
          LOG_WARN("fail to open cg macro block writer", K(ret), K(write_macro_param));
        } else if (OB_FAIL(append_cg_block_files_to_writer(cg_block_files_iter))) {
          LOG_WARN("fail to append cg block files into writer", K(ret));
        } else {
          row_offsets_.at(cg_idx) += cg_macro_block_writer_.get_written_row_count();
          if (OB_SUCC(ret)) {
            if (OB_FAIL(cg_macro_block_writer_.close())) {
              LOG_WARN("fail to close cg macro block writer", K(ret));
            } else if (OB_FAIL(flushed_bitmap_.set(cg_idx))) {
              LOG_WARN("flush bitmap set failed", K(ret));
            } else {
              start_seqences_.at(cg_idx) = cg_macro_block_writer_.get_last_macro_seq();
              cg_macro_block_writer_.reset();
            }
          }
        }
      }
    }
  }
  return ret;
}

// start private func
int ObDAGCGMacroBlockWriteOp::append_cg_block_files_to_writer(ObCGBlockFilesIterator *cg_block_files_iter)
{
  int ret = OB_SUCCESS;
  ObCGBlock cg_block;
  if (OB_UNLIKELY(nullptr == cg_block_files_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block files iterator is null", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(cg_block_files_iter->get_next_cg_block(cg_block))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_UNLIKELY(0 != cg_block_files_iter->get_total_data_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("total data size is not zero", K(ret), KPC(cg_block_files_iter));
        } else {
          cg_block_files_iter->reset(); // free cg block files here better than free them when dag task destruct
        }
        break;
      } else {
        LOG_WARN("fail to get next cg block", K(ret), KPC(cg_block_files_iter));
      }
    } else if (OB_FAIL(cg_macro_block_writer_.append_cg_block(cg_block))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cg_block_files_iter->put_cg_block_back(cg_block))) {
          LOG_WARN("fail to put cg block back", K(ret), K(cg_block), KPC(cg_block_files_iter));
        }
        break;
      } else {
        LOG_WARN("fail to append cg block", K(ret), K(cg_block));
      }
    }
  }
  return ret;
}

int ObDAGCGMacroBlockWriteOp::set_remain_block(ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDAGCGMacroBlockWriteOp is not initialized", K(ret));
  } else if (OB_UNLIKELY(nullptr == ddl_slice)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), KP(ddl_slice));
  }
  for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_block_files_iter_arr_.count(); ++cg_idx) {
    ObCGBlockFilesIterator *cg_block_files_iter = cg_block_files_iter_arr_.at(cg_idx);
    if (OB_ISNULL(cg_block_files_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current cg block file iterator is null", K(ret), KP(cg_idx), KP(cg_block_files_iter));
    } else if (flushed_bitmap_.test(cg_idx)) {
      if (OB_UNLIKELY(cg_block_files_iter->get_total_data_size() > 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg block file iterator remain unexpected data", K(ret), K(cg_idx), KPC(cg_block_files_iter));
      } else if (OB_FAIL(ddl_slice->set_block_flushed(cg_idx))) {
        LOG_WARN("set block flushed failed", K(ret));
      }
    } else {
      // move ownership of cg block file to ddl slice
      ObArray<ObCGBlockFile *> remain_block_files;
      if (OB_UNLIKELY(cg_block_files_iter->get_total_data_size() <= 0)) {
        LOG_INFO("the slice does not include the data", K(ret), K(cg_idx), KPC(cg_block_files_iter));
        if (OB_FAIL(ddl_slice->set_block_flushed(cg_idx))) {
          LOG_WARN("set block flushed failed", K(ret));
        }
      } else if (OB_FAIL(cg_block_files_iter->get_remain_block_files(remain_block_files))) {
        LOG_WARN("get remain block files", K(ret));
      } else if (OB_UNLIKELY(remain_block_files.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should only remain one block file", K(ret), K(remain_block_files.count()));
      } else if (OB_FAIL(ddl_slice->set_remain_block(cg_idx, remain_block_files.at(0)))) {
        LOG_WARN("set remain block file into ddl slice failed", K(ret), K(cg_idx), K(remain_block_files));
      }
      if (OB_FAIL(ret) && remain_block_files.count() > 0) {
        for (int64_t i = 0; i < remain_block_files.count(); ++i) {
          ObCGBlockFile *cg_block_file = remain_block_files.at(i);
          if (OB_LIKELY(nullptr != cg_block_file)) {
            cg_block_file->~ObCGBlockFile();
            ob_free(cg_block_file);
            cg_block_file = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
