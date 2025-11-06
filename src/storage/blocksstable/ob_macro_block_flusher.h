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
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_

namespace oceanbase
{
namespace blocksstable
{
class ObMacroBlock;
/**
 * -----------------------------------------------------------------ObIMacroBlockFlusher-------------------------------------------------------------------
 */
class ObIMacroBlockFlusher
{
public:
  ObIMacroBlockFlusher();
  virtual ~ObIMacroBlockFlusher();
  virtual int write_disk(ObMacroBlock& macro_block, const bool is_close_flush) = 0;
};

/**
 * --------------------------------------------------------------ObDagTempMacroFlusher------------------------------------------------------------
 */
class ObDagTempMacroFlusher : public ObIMacroBlockFlusher
{
public:
  ObDagTempMacroFlusher();
  virtual ~ObDagTempMacroFlusher();
  void reset();
  virtual int write_disk(ObMacroBlock& macro_block, const bool is_close_flush) override;
  void set_temp_file_writer(ObCGBlockFileWriter &temp_file_writer) { temp_file_writer_ = &temp_file_writer; }
private:
  ObCGBlockFileWriter *temp_file_writer_;
};


/**
 * --------------------------------------------------------------ObDagSliceMacroFlusher------------------------------------------------------------
 */
class ObDagSliceMacroFlusher : public ObIMacroBlockFlusher
{
public:
  explicit ObDagSliceMacroFlusher(ObCGBlockFileWriter &temp_file_writer);
  virtual ~ObDagSliceMacroFlusher();
  void reset();
  virtual int write_disk(ObMacroBlock& macro_block, const bool is_close_flush) override;
private:
  ObCGBlockFileWriter *temp_file_writer_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif //OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_