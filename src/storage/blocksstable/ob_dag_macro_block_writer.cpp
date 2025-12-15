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

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_dag_macro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

/**
* ---------------------------------------ObDagSliceMacroBlockWriter-----------------------------------
*/
ObDagSliceMacroBlockWriter::ObDagSliceMacroBlockWriter()
  : ObMacroBlockWriter(true/*is_need_macro_buffer*/)
{
}

ObDagSliceMacroBlockWriter::~ObDagSliceMacroBlockWriter()
{
  reset();
}

void ObDagSliceMacroBlockWriter::reset()
{
  ObMacroBlockWriter::reset();
}

int ObDagSliceMacroBlockWriter::try_finish_last_micro_block()
{
  int ret = OB_SUCCESS;
  if (micro_writer_->get_row_count() > 0) {
    if (OB_FAIL(build_micro_block())) {
      STORAGE_LOG(WARN, "fail to build last micro block", K(ret));
    }
  }
  return ret;
}

int ObDagSliceMacroBlockWriter::close(ObDagSliceMacroFlusher *macro_block_flusher)
{
  int ret = OB_SUCCESS;
  if (nullptr != macro_block_flusher) {
    if (OB_UNLIKELY(merge_block_info_.macro_block_count_ != 0 || micro_writer_->get_row_count() > 0)) {
      // When macro_block_flusher is not null, the following checks are required:
      // 1. merge_block_info_.macro_block_count_ != 0: ensure the macro block writer has not flushed any macro blocks before close
      // 2. micro_writer_->get_row_count() > 0: ensure try_finish_last_micro_block has been called before close()
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected macro block count or micro writer row count", K(ret), K(merge_block_info_), K(micro_writer_->get_row_count()));
    } else {
      custom_macro_flusher_ = static_cast<ObIMacroBlockFlusher *>(macro_block_flusher);
    }
  } else {
    // For default_flusher, it is allowed to flush more than one macro block
    // skip
  }
  if (FAILEDx(ObMacroBlockWriter::close())) {
    LOG_WARN("failed to close macro block writer", K(ret));
  }
  return ret;
}

/**
* ---------------------------------------ObDagTempMacroBlockWriter-----------------------------------
*/
ObDagTempMacroBlockWriter::ObDagTempMacroBlockWriter()
  : ObMacroBlockWriter(true/*is_need_macro_buffer*/),
    dag_temp_macro_flusher_()
{
}

ObDagTempMacroBlockWriter::~ObDagTempMacroBlockWriter()
{
  reset();
}

void ObDagTempMacroBlockWriter::reset()
{
  dag_temp_macro_flusher_.reset();
  ObMacroBlockWriter::reset();
}

int ObDagTempMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    ObSSTablePrivateObjectCleaner &object_cleaner,
    ObCGBlockFileWriter *cg_block_writer)
{
  int ret = OB_SUCCESS;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  ObMacroBlockWriter::reset_for_open();
  // No need pre_warmer in dag_macro_block_writer.
  share::ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  if (OB_UNLIKELY(OB_ISNULL(cg_block_writer))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid cg_block_writer", K(ret), KP(cg_block_writer));
  } else if (OB_FAIL(ObMacroBlockWriter::inner_init(
      data_store_desc,
      parallel_idx,
      macro_seq_param,
      pre_warm_param,
      false,
      object_cleaner,
      nullptr,
      nullptr,
      nullptr))) {
    LOG_WARN("failed to inner init macro block writer", K(ret));
  } else {
    dag_temp_macro_flusher_.set_temp_file_writer(*cg_block_writer);
    custom_macro_flusher_ = static_cast<ObIMacroBlockFlusher *>(&dag_temp_macro_flusher_);
  }
  return ret;
}

/**
* -----------------------------------------ObDagMacroBlockWriter----------------------------------------
*/

ObDagMacroBlockWriter::ObDagMacroBlockWriter()
  : ObMacroBlockWriter(true/*is_need_macro_buffer*/),
    dag_stage_(ObDagMacroWriterStage::NOT_INITED)
{
}

ObDagMacroBlockWriter::~ObDagMacroBlockWriter()
{
  reset();
}

void ObDagMacroBlockWriter::reset()
{
  ObMacroBlockWriter::reset();
  dag_stage_ = ObDagMacroWriterStage::NOT_INITED;
}

int ObDagMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    ObSSTablePrivateObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  // No need pre_warmer in dag_macro_block_writer.
  share::ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  if (OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid callback", K(ret), KP(callback));
  } else if (OB_FAIL(ObMacroBlockWriter::open(
      data_store_desc, parallel_idx, macro_seq_param, pre_warm_param, object_cleaner, callback))) {
    STORAGE_LOG(WARN, "Fail to open macro block writer", K(ret));
  } else {
    set_dag_stage(ObDagMacroWriterStage::WAITTING_APPEND_CG_BLOCK);
  }
  return ret;
}

int ObDagMacroBlockWriter::open_for_ss_ddl(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    ObSSTablePrivateObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  // No need pre_warmer in dag_macro_block_writer.
  share::ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  ObMacroBlockWriter::reset_for_open();
  if (OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to open macro writer for ss ddl", K(ret));
  } else if (OB_FAIL(ObMacroBlockWriter::inner_init(
      data_store_desc,
      parallel_idx,
      macro_seq_param,
      pre_warm_param,
      false,
      object_cleaner,
      callback,
      nullptr, /* validator */
      nullptr /* device handle */))) {
    LOG_WARN("failed to inner init macro block writer", K(ret));
  } else {
    set_dag_stage(ObDagMacroWriterStage::WAITTING_APPEND_CG_BLOCK);
  }
  return ret;
}

int ObDagMacroBlockWriter::on_buffer_not_enough()
{
  int ret = OB_SUCCESS;
  if (ObDagMacroWriterStage::NOT_INITED == get_dag_stage()) {
    ret = OB_NOT_INIT;
  } else {
    set_dag_stage(ObDagMacroWriterStage::WAITTING_CLOSE);
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

/**
 * @brief Appends a CGBlock to the macro block and controls reusability based on the fill threshold.
 *
 * @param cg_block              [in] The CGBlock data to be appended to the macro block.
 * @param macro_block_fill_threshold [in] The fill ratio threshold (0-100) for reusing the macro block.
 *                                  If the current macro block's data volume is greater than or equal to this threshold,
 *                                  the macro block in cg_block can be reused; otherwise, have to iterate and reuse
 *                                  the micro block rather than reuse the macro block.
 * @return int                  Returns 0 on success, non-zero on failure.
 */
int ObDagMacroBlockWriter::append_cg_block(ObCGBlock &cg_block, const int64_t macro_block_fill_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid cg_block", K(ret), K(cg_block));
  } else if (macro_block_fill_threshold < 0 || macro_block_fill_threshold > 100) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro_block_fill_threshold", K(ret), K(macro_block_fill_threshold));
  } else if (get_dag_stage() != ObDagMacroWriterStage::WAITTING_APPEND_CG_BLOCK) {
    //If current stage is WAITTING_CLOSE, you should close the writer;
    //If current stage is NOT_INITED, it means not initialized and should do open().
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "Append_cg_block is only supported during DAG_WRITE_MACRO_BLOCK stage. ", K(ret), K(get_dag_stage()));
  } else if (is_reuse_macro_block(cg_block, macro_block_fill_threshold)) {
    if (OB_FAIL(append_macro_block(cg_block))) {
      STORAGE_LOG(WARN, "fail to reuse macro block", K(ret), K(cg_block));
    }
  } else {
    // have to iterate and reuse the micro block
    ObDagMicroBlockIterator micro_block_iter;
    compaction::ObLocalArena iter_allocator_temp("DagMaBlkWriter");
    if (OB_FAIL(micro_block_iter.open_cg_block(&cg_block))) {
      STORAGE_LOG(WARN, "fail to open cg block", K(ret), K(cg_block), K(micro_block_iter));
    } else if (OB_FAIL(reuse_micro_blocks(micro_block_iter, iter_allocator_temp))) {
      if (ret == OB_BUF_NOT_ENOUGH) {
        STORAGE_LOG(INFO, "The macro block is full and cannot accommodate more micro blocks.", K(ret));
      } else {
        STORAGE_LOG(WARN, "fail to reuse micro block", K(ret));
      }
    }
  }
  return ret;
}

int ObDagMacroBlockWriter::reuse_micro_blocks(ObDagMicroBlockIterator &micro_block_iter, ObIAllocator &rowkey_allocator)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc micro_block_desc;
  ObMicroIndexData micro_index_data;

  while (OB_SUCC(ret)) {
    rowkey_allocator.reuse();
    if (OB_FAIL(micro_block_iter.get_next_micro_block_desc(micro_block_desc, micro_index_data, rowkey_allocator))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        break;
      } else {
        STORAGE_LOG(WARN, "fail to get next micro block", K(ret), K(micro_block_iter));
      }
    } else if (!micro_block_desc.is_valid() || !micro_index_data.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid micro_block_desc or micro_index_data", K(ret), K(micro_block_desc), K(micro_index_data));
    } else if (FALSE_IT(micro_block_desc.logic_micro_id_.reset())) {
      // The logic micro id generated in the first stage of dag writer cannot guarantee uniqueness.
      // So the logic id obtained directly from the middle layer cannot be directly used for
      // append_micro_block in the second stage. Therefore, it needs to be reset and regenerated
      // as a unique micro block logic id in the second stage.
    } else if (OB_FAIL(ObMacroBlockWriter::append_micro_block(micro_block_desc, micro_index_data))) {
      STORAGE_LOG(WARN, "fail to append micro", K(ret), K(micro_block_desc), K(micro_index_data));
    } else if (OB_FAIL(micro_block_iter.update_cg_block_offset_and_micro_idx())) {
      STORAGE_LOG(WARN, "fail to update offset and micro block idx", K(ret));
    }
  }

  return ret;
}

int ObDagMacroBlockWriter::append_macro_block(const ObCGBlock &cg_block)
{
  int ret = OB_NOT_SUPPORTED;
  // TODO(zs475329): ObDagMacroBlockWriter reuse macro block
  return ret;
}


bool ObDagMacroBlockWriter::is_reuse_macro_block(const ObCGBlock &cg_block, const int64_t macro_block_fill_threshold) const
{
  return false;
  // return cg_block.is_complete_macro_block() &&
  //        cg_block.is_all_micro_blocks_unconsumed() &&
  //        is_almost_full_macro_block(macro_block_fill_threshold);
}

bool ObDagMacroBlockWriter::is_almost_full_macro_block(const int64_t macro_block_fill_threshold) const
{
  int64_t almost_full_size = data_store_desc_->get_macro_block_size() * macro_block_fill_threshold / 100;
  return macro_blocks_[current_index_].get_data_size() >= almost_full_size;
}


}//end namespace blocksstable
}//end namespace oceanbase
