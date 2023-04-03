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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_DATA_MACRO_BLOCK_MERGE_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_DATA_MACRO_BLOCK_MERGE_WRITER_H_
#include "ob_macro_block_writer.h"
#include "ob_macro_block.h"
#include "ob_index_block_macro_iterator.h"
#include "ob_macro_block.h"
#include "ob_datum_row.h"


namespace oceanbase
{
namespace blocksstable
{
class ObDataMacroBlockMergeWriter : public ObMacroBlockWriter
{
public:
  ObDataMacroBlockMergeWriter();
  virtual ~ObDataMacroBlockMergeWriter();
  virtual void reset() override;
  virtual int open(
      ObDataStoreDesc &data_store_desc,
      const ObMacroDataSeq &start_seq,
      ObIMacroBlockFlushCallback *callback = nullptr) override;
  virtual int append_row(const ObDatumRow &row, const ObMacroBlockDesc *curr_macro_desc = nullptr) override;
  virtual int append_micro_block(const ObMicroBlock &micro_block, const ObMacroBlockDesc *curr_macro_desc = nullptr) override;
  virtual int append_macro_block(const ObMacroBlockDesc &macro_desc) override;
protected:
  virtual int build_micro_block() override;
  virtual int try_switch_macro_block() override;
  virtual bool is_keep_freespace() const override {return !is_use_freespace_; }

private:
  void adjust_freespace(const ObMacroBlockDesc *curr_macro_desc);
  bool check_need_switch_macro_block();
private:
  ObLogicMacroBlockId curr_macro_logic_id_;
  bool is_use_freespace_;
  bool next_block_use_freespace_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
