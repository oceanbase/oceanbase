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
#include "storage/blocksstable/ob_data_macro_block_merge_writer.h"
#include "lib/utility/ob_macro_utils.h"


namespace oceanbase
{
namespace blocksstable
{


ObDataMacroBlockMergeWriter::ObDataMacroBlockMergeWriter()
    : ObMacroBlockWriter(true),
      curr_macro_logic_id_(),
      is_use_freespace_(false),
      next_block_use_freespace_(false)
{
}

ObDataMacroBlockMergeWriter::~ObDataMacroBlockMergeWriter()
{
}

void ObDataMacroBlockMergeWriter::reset()
{
  ObMacroBlockWriter::reset();
  curr_macro_logic_id_.reset();
  is_use_freespace_ = false;
  next_block_use_freespace_ = false;
}

int ObDataMacroBlockMergeWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const ObMacroDataSeq &start_seq,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;

  curr_macro_logic_id_.reset();
  is_use_freespace_ = false;
  next_block_use_freespace_ = false;
  if (OB_FAIL(ObMacroBlockWriter::open(data_store_desc, start_seq, callback))) {
    STORAGE_LOG(WARN, "Fail to open macro block writer", K(ret));
  }

  return ret;
}

int ObDataMacroBlockMergeWriter::append_row(
    const ObDatumRow &row,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(curr_macro_desc) && OB_UNLIKELY(!curr_macro_desc->is_valid_with_macro_meta())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro desc", K(ret));
  } else {
    adjust_freespace(curr_macro_desc);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMacroBlockWriter::append_row(row))) {
    STORAGE_LOG(WARN, "ObMacroBlockWriter fail to append_row", K(ret));
  }

  return ret;
}

int ObDataMacroBlockMergeWriter::append_micro_block(
    const ObMicroBlock &micro_block,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(curr_macro_desc) && OB_UNLIKELY(!curr_macro_desc->is_valid_with_macro_meta())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro desc", K(ret));
  } else {
    adjust_freespace(curr_macro_desc);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMacroBlockWriter::append_micro_block(micro_block))) {
    STORAGE_LOG(WARN, "ObMacroBlockWriter fail to append_micro_block", K(ret));
  } else if (check_need_switch_macro_block()) {
    if (OB_FAIL(try_switch_macro_block())) {
      STORAGE_LOG(WARN, "fail to try switch macro block", K(ret));
    }
  }

  return ret;
}


int ObDataMacroBlockMergeWriter::append_macro_block(const ObMacroBlockDesc &macro_desc)
{
  next_block_use_freespace_ = false;
  return ObMacroBlockWriter::append_macro_block(macro_desc);
}

void ObDataMacroBlockMergeWriter::adjust_freespace(const ObMacroBlockDesc *curr_macro_desc)
{
  if (OB_NOT_NULL(curr_macro_desc) && curr_macro_logic_id_ != curr_macro_desc->macro_meta_->get_logic_id()) {
    curr_macro_logic_id_ = curr_macro_desc->macro_meta_->get_logic_id();
    is_use_freespace_ = static_cast<blocksstable::ObDataMacroBlockMeta *>(curr_macro_desc->macro_meta_)->val_.data_zsize_
                        + get_macro_data_size() <= data_store_desc_->get_macro_block_size();
    next_block_use_freespace_ = !is_use_freespace_;
  }
}

bool ObDataMacroBlockMergeWriter::check_need_switch_macro_block()
{
  bool need_switch_macro_block = false;

  if (get_curr_micro_writer_row_count() > 0) {
    need_switch_macro_block = false;
  } else if (!is_use_freespace_ && get_macro_data_size() >= data_store_desc_->get_macro_store_size()) {
    need_switch_macro_block = true;
  }

  return need_switch_macro_block;
}

int ObDataMacroBlockMergeWriter::build_micro_block()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObMacroBlockWriter::build_micro_block())) {
    STORAGE_LOG(WARN, "ObMacroBlockWriter fail to build_micro_block", K(ret));
  } else if (check_need_switch_macro_block()) {
    if (OB_FAIL(try_switch_macro_block())) {
      STORAGE_LOG(WARN, "fail to try switch macro block", K(ret));
    }
  }

  return ret;
}

int ObDataMacroBlockMergeWriter::try_switch_macro_block()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObMacroBlockWriter::try_switch_macro_block())) {
    STORAGE_LOG(WARN, "ObMacroBlockWriter fail to try switch macro block", K(ret));
  } else {
    is_use_freespace_ = next_block_use_freespace_;
    next_block_use_freespace_ = false;
  }

  return ret;
}
}//end namespace blocksstable
}//end namespace oceanbase