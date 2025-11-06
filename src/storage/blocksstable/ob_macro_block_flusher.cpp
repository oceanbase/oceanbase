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

#include "storage/blocksstable/ob_macro_block_flusher.h"
namespace oceanbase
{
namespace blocksstable
{

/**
 * -------------------------------------------------------------------ObIMacroBlockFlusher-------------------------------------------------------------------
 */

ObIMacroBlockFlusher::ObIMacroBlockFlusher()
{
}

ObIMacroBlockFlusher::~ObIMacroBlockFlusher()
{
}

/**
 * --------------------------------------------------------------ObDagTempMacroFlusher------------------------------------------------------------
 */
ObDagTempMacroFlusher::ObDagTempMacroFlusher()
  : temp_file_writer_(nullptr)
{
}

ObDagTempMacroFlusher::~ObDagTempMacroFlusher()
{
  reset();
}

void ObDagTempMacroFlusher::reset()
{
  temp_file_writer_ = nullptr;
}

int ObDagTempMacroFlusher::write_disk(ObMacroBlock& macro_block, const bool is_close_flush)
{
  int ret = OB_SUCCESS;
  const char *simplified_buffer = nullptr;
  int64_t simplified_buffer_size = 0;
  if (OB_UNLIKELY(OB_ISNULL(temp_file_writer_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null temp_file_writer_", K(ret), KP(temp_file_writer_));
  } else if (OB_FAIL(ObSimplifiedSSTableMacroBlockHeader::simplify_macro_block(
              macro_block.get_data_buf(),
              macro_block.get_data_size(),
              simplified_buffer,
              simplified_buffer_size))) {
    STORAGE_LOG(WARN, "fail to simplify macro block", K(ret));
  } else if (OB_FAIL(temp_file_writer_->write(simplified_buffer, !is_close_flush, simplified_buffer_size))) {
    STORAGE_LOG(WARN, "fail to write macro block to temp file", K(ret));
  }
  return ret;
}

/**
 * --------------------------------------------------------------ObDagSliceMacroFlusher------------------------------------------------------------
 */
ObDagSliceMacroFlusher::ObDagSliceMacroFlusher(ObCGBlockFileWriter &temp_file_writer)
  : temp_file_writer_(&temp_file_writer)
{
}

ObDagSliceMacroFlusher::~ObDagSliceMacroFlusher()
{
  reset();
}

void ObDagSliceMacroFlusher::reset()
{
  temp_file_writer_ = nullptr;
}

int ObDagSliceMacroFlusher::write_disk(ObMacroBlock& macro_block, const bool is_close_flush)
{
  int ret = OB_SUCCESS;
  const char *simplified_buffer = nullptr;
  int64_t simplified_buffer_size = 0;
  if (OB_UNLIKELY(OB_ISNULL(temp_file_writer_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null temp_file_writer_", K(ret), KP(temp_file_writer_));
  } else if (OB_FAIL(ObSimplifiedSSTableMacroBlockHeader::simplify_macro_block(
        macro_block.get_data_buf(),
        macro_block.get_data_size(),
        simplified_buffer,
        simplified_buffer_size))) {
    STORAGE_LOG(WARN, "fail to simplify macro block", K(ret));
  } else if (OB_FAIL(temp_file_writer_->write(simplified_buffer, !is_close_flush, simplified_buffer_size))) {
    STORAGE_LOG(WARN, "fail to write macro block to temp file", K(ret));
  }
  return ret;
}


}//end namespace blocksstable
}//end namespace oceanbase