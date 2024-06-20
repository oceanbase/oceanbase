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
#include "ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"

namespace oceanbase {
using namespace storage;
namespace blocksstable {
void ObMacroBlockDesc::reuse()
{
  macro_block_id_.reset();
  if (nullptr != macro_meta_) {
    macro_meta_->reset();
  }
  range_.reset();
  start_row_offset_ = 0;
  row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
  schema_version_ = 0;
  snapshot_version_ = 0;
  max_merged_trans_version_ = 0;
  row_count_ = 0;
  row_count_delta_ = 0;
  contain_uncommitted_row_ = false;
  is_deleted_ = false;
}

int ObMicroIndexRowItem::init(ObIAllocator &allocator,
                           const ObIndexBlockRowHeader *idx_row_header,
                           const ObDatumRowkey *endkey,
                           const ObIndexBlockRowMinorMetaInfo *idx_minor_info,
                           const char *agg_row_buf,
                           const int64_t agg_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(idx_row_header) || OB_ISNULL(endkey)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemen", K(ret), KP(idx_row_header), KP(endkey), KP(idx_minor_info), KP(agg_row_buf));
  } else {
    allocator_ = &allocator;
    agg_buf_size_ = agg_buf_size;

    void *key_buf = nullptr;
    if (OB_ISNULL(key_buf = allocator_->alloc(sizeof(ObDatumRowkey)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObDatumRowkey)));
    } else if (FALSE_IT(endkey_ = new (key_buf) ObDatumRowkey())) {
    } else if (OB_FAIL(endkey->deep_copy(*endkey_, allocator))) {
      LOG_WARN("fail to deep copy rowkey", K(ret), KPC(endkey_), KPC(endkey));
    }

    void *header_buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(header_buf = allocator_->alloc(sizeof(ObIndexBlockRowHeader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIndexBlockRowHeader)));
    } else if (FALSE_IT(idx_row_header_ = new (header_buf) ObIndexBlockRowHeader())) {
    } else {
      *idx_row_header_ =*idx_row_header;
    }

    void *minor_info_buf = nullptr;
    if (OB_FAIL(ret) || OB_ISNULL(idx_minor_info)) {
    } else if (OB_ISNULL(minor_info_buf = allocator_->alloc(sizeof(ObIndexBlockRowMinorMetaInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIndexBlockRowMinorMetaInfo)));
    } else if (FALSE_IT(idx_minor_info_ = new (minor_info_buf) ObIndexBlockRowMinorMetaInfo())) {
    } else {
      *idx_minor_info_ = *idx_minor_info;
    }

    void *agg_buf = nullptr;
    if (OB_FAIL(ret) || OB_ISNULL(agg_row_buf)) {
    } else if (OB_ISNULL(agg_buf = allocator_->alloc(agg_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(agg_buf_size));
    } else {
      MEMCPY(agg_buf, agg_row_buf, agg_buf_size);
      agg_row_buf_ = reinterpret_cast<char *>(agg_buf);
    }
  }
  return ret;
}

void ObMicroIndexRowItem::reset()
{
  int ret = OB_SUCCESS;
  agg_buf_size_ = 0;
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(endkey_)){
      endkey_->~ObDatumRowkey();
      allocator_->free(endkey_);
      endkey_ = nullptr;
    }
    if (OB_NOT_NULL(idx_row_header_)){
      idx_row_header_->~ObIndexBlockRowHeader();
      allocator_->free(idx_row_header_);
      idx_row_header_ = nullptr;
    }
    if (OB_NOT_NULL(idx_minor_info_)){
      idx_minor_info_->~ObIndexBlockRowMinorMetaInfo();
      allocator_->free(idx_minor_info_);
      idx_minor_info_ = nullptr;
    }
    if (OB_NOT_NULL(agg_row_buf_)){
      allocator_->free(agg_row_buf_);
      agg_row_buf_ = nullptr;
    }
  }
  allocator_ = nullptr;
}

void ObMicroIndexRowItem::reuse()
{
  int ret = OB_SUCCESS;
  endkey_ = nullptr;
  agg_buf_size_ = 0;
  idx_row_header_ = nullptr;
  idx_minor_info_ = nullptr;
  agg_row_buf_ = nullptr;
  allocator_ = nullptr;
}

ObIndexBlockMacroIterator::ObIndexBlockMacroIterator()
  : sstable_(nullptr), iter_range_(nullptr),
    tree_cursor_(), allocator_(nullptr),
    cur_idx_(-1), begin_(),end_(), curr_key_(), prev_key_(),
    curr_key_buf_(nullptr), prev_key_buf_(nullptr),
    begin_block_start_row_offset_(0), end_block_start_row_offset_(0), micro_index_infos_(),
    micro_endkeys_(), micro_endkey_allocator_(), hold_item_(), need_record_micro_info_(false),
    is_iter_end_(false), is_reverse_scan_(false), is_inited_(false) {}

ObIndexBlockMacroIterator::~ObIndexBlockMacroIterator()
{
  reset();
}

void ObIndexBlockMacroIterator::reset() {
  sstable_ = nullptr;
  iter_range_ = nullptr;
  if (need_record_micro_info_ && hold_item_.is_block_allocated_) {
    tree_cursor_.release_held_path_item(hold_item_);
  }
  tree_cursor_.reset();
  if (allocator_ != nullptr) {
    if (nullptr != curr_key_buf_) {
      allocator_->free(curr_key_buf_);
    }
    curr_key_buf_ = nullptr;
    if (nullptr != prev_key_buf_) {
      allocator_->free(prev_key_buf_);
    }
    prev_key_buf_ = nullptr;
  }
  allocator_ = nullptr;
  cur_idx_ = -1;
  begin_.reset();
  end_.reset();
  curr_key_.reset();
  prev_key_.reset();
  micro_index_infos_.reset();
  micro_endkeys_.reset();
  micro_endkey_allocator_.reset();
  begin_block_start_row_offset_ = 0;
  end_block_start_row_offset_ = 0;
  need_record_micro_info_ = false;
  is_iter_end_ = false;
  is_reverse_scan_ = false;
  is_inited_ = false;
}

int ObIndexBlockMacroIterator::open(
    ObSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse,
    const bool need_record_micro_info) {
  int ret = OB_SUCCESS;
  bool start_beyond_range = false;
  bool end_beyond_range = false;
  ObDatumRowkey sstable_endkey;
  int cmp_ret = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Need reset before iterate on a new sstable", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("SSTable is not valid", K(ret), K(sstable), K(range));
  } else if (sstable.no_data_to_read()) {
    is_iter_end_ = true;
  } else if (OB_FAIL(sstable.get_last_rowkey(allocator, sstable_endkey))) {
    LOG_WARN("Fail to get last rowkey of sstable", K(ret));
  } else if (OB_FAIL(sstable_endkey.compare(
      range.get_start_key(), rowkey_read_info.get_datum_utils(), cmp_ret))) {
    LOG_WARN("Fail to compare sstable endkey and range start key", K(ret));
  } else if (cmp_ret < 0 || (0 == cmp_ret && !range.get_border_flag().inclusive_start())) {
    is_iter_end_ = true;
  } else if (OB_FAIL(tree_cursor_.init(sstable, allocator, &rowkey_read_info))) {
    LOG_WARN("Fail to init tree cursor", K(ret), K(sstable));
  } else {
    const int64_t store_rowkey_cnt = sstable.is_normal_cg_sstable() ? 1
        : rowkey_read_info.get_schema_rowkey_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(); // include multi-version
    const bool is_percise_rowkey = store_rowkey_cnt == range.get_end_key().get_datum_cnt();
    if (is_reverse) {
      if (OB_FAIL(locate_macro_block(
          false,
          true,
          (range.get_border_flag().inclusive_start()),
          range.get_start_key(),
          begin_,
          begin_block_start_row_offset_,
          start_beyond_range))) {
        LOG_WARN("Fail to locate begin macro block", K(ret), K(range));
      } else if (OB_FAIL(locate_macro_block(
          true,
          false,
          (!range.get_border_flag().inclusive_end() || is_percise_rowkey),
          range.get_end_key(),
          end_,
          end_block_start_row_offset_,
          end_beyond_range))) {
        LOG_WARN("Fail to locate end macro block", K(ret), K(range));
      } else {
        curr_key_ = range.get_end_key();
        if (start_beyond_range) {
          is_iter_end_ = true;
        } else if (end_beyond_range) {
          // endkey beyond the open range
        }
      }
    } else {
      if (OB_FAIL(locate_macro_block(
          true,
          false,
          (!range.get_border_flag().inclusive_end() || is_percise_rowkey),
          range.get_end_key(),
          end_,
          end_block_start_row_offset_,
          end_beyond_range))) {
        LOG_WARN("Fail to locate end macro block", K(ret), K(range));
      } else if (OB_FAIL(locate_macro_block(
          false,
          true,
          (range.get_border_flag().inclusive_start()),
          range.get_start_key(),
          begin_,
          begin_block_start_row_offset_,
          start_beyond_range))) {
        LOG_WARN("Fail to locate begin macro block", K(ret), K(range));
      } else {
        // Always return MIN_ROWKEY as start_key of first iterated macro block
        // for check reuse boundary during compaction
        curr_key_.set_min_rowkey();
        if (start_beyond_range) {
          is_iter_end_ = true;
        } else if (end_beyond_range) {
        }
      }
    }
  }


  if (OB_SUCC(ret) && !is_iter_end_) {
    sstable_ = &sstable;
    iter_range_ = &range;
    allocator_ = &allocator;
    is_reverse_scan_ = is_reverse;
    need_record_micro_info_ = need_record_micro_info;
    is_iter_end_ = false;
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

int ObIndexBlockMacroIterator::deep_copy_rowkey(const ObDatumRowkey &src_key, ObDatumRowkey &dest_key, char *&key_buf)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexBlockMacroIterator is not inited", K(ret), KP(allocator_));
  } else if (OB_UNLIKELY(!src_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum rowkey", K(ret), K(src_key));
  } else {
    if (key_buf != nullptr) {
      allocator_->free(key_buf);
    }
    dest_key.reset();
    const int64_t copy_size = src_key.get_deep_copy_size();
    if (OB_ISNULL(key_buf = reinterpret_cast<char *>(allocator_->alloc(copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(copy_size));
    } else if (OB_FAIL(src_key.deep_copy(dest_key, key_buf, copy_size))) {
      STORAGE_LOG(WARN, "Failed to deep copy rowkey", K(ret), K(src_key), K(copy_size));
      allocator_->free(key_buf);
      key_buf = nullptr;
    }
  }

  return ret;
}

int ObIndexBlockMacroIterator::get_next_idx_row(ObIAllocator &item_allocator, ObMicroIndexRowItem &macro_index_item, int64_t &row_offset, bool &reach_cursor_end)
{
  int ret = OB_SUCCESS;
  row_offset = 0;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  const ObIndexBlockRowMinorMetaInfo *minor_meta_info = nullptr;
  const char *agg_row_buf = nullptr;
  int64_t agg_buf_size = 0;
  MacroBlockId macro_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(macro_id))) {
    LOG_WARN("Fail to get macro row block id", K(ret), K(macro_id));
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get idx row parser", K(ret), K_(tree_cursor));
  } else if (OB_ISNULL(idx_row_parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx_row_parser", K(ret));
  } else if (OB_FAIL(idx_row_parser->get_header(idx_row_header))) {
    LOG_WARN("Fail to get idx row header", K(ret), KPC(idx_row_parser));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index row header", K(ret));
  } else {
    if ((macro_id == begin_ && is_reverse_scan_) || (macro_id == end_ && !is_reverse_scan_)) {
      is_iter_end_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    // traverse all node of this macro block and collect index info
    ObDatumRowkey rowkey;
    row_offset = idx_row_parser->get_row_offset();
    if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
      if (OB_FAIL(idx_row_parser->get_minor_meta(minor_meta_info))) {
        LOG_WARN("Fail to get minor meta info", K(ret));
      }
    } else if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
      // Do not have aggregate data
    } else if (OB_FAIL(idx_row_parser->get_agg_row(agg_row_buf, agg_buf_size))) {
      LOG_WARN("Fail to get aggregate", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (need_record_micro_info_) {
        micro_endkey_allocator_.reuse();
        reuse_micro_info_array();
        if (OB_FAIL(tree_cursor_.get_child_micro_infos(
                    *iter_range_,
                    micro_endkey_allocator_,
                    micro_endkeys_,
                    micro_index_infos_,
                    hold_item_))) {
          LOG_WARN("Fail to record child micro block info", K(ret), KPC(iter_range_));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_rowkey(curr_key_, prev_key_, prev_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save prev key", K(ret), K_(curr_key));
    } else if (OB_FAIL(tree_cursor_.get_current_endkey(rowkey))) {
      LOG_WARN("Fail to get current endkey", K(ret), K_(tree_cursor));
    } else if (OB_FAIL(deep_copy_rowkey(rowkey, curr_key_, curr_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save curr key", K(ret), K(rowkey));
    } else if (FALSE_IT(macro_index_item.reuse())) {
    } else if (OB_FAIL(macro_index_item.init(item_allocator, idx_row_header, &curr_key_, minor_meta_info, agg_row_buf, agg_buf_size))) {
      STORAGE_LOG(WARN, "Failed to init macro index item", K(ret), K(macro_index_item));
    } else if (OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to move cursor to next macro node", K(ret), K_(tree_cursor));
      }
    }
    if (OB_SUCC(ret)) {
      reach_cursor_end = is_iter_end_;
    }
  }
  return ret;
}

int ObIndexBlockMacroIterator::get_next_macro_block(
    MacroBlockId &macro_id, int64_t &start_row_offset)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(macro_id))) {
    LOG_WARN("Fail to get macro row block id", K(ret), K(macro_id));
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get idx row parser", K(ret), K_(tree_cursor));
  } else if (OB_ISNULL(idx_row_parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx_row_parser", K(ret));
  } else if (OB_FAIL(idx_row_parser->get_header(idx_row_header))) {
    LOG_WARN("Fail to get idx row header", K(ret), KPC(idx_row_parser));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index row header", K(ret));
  } else {
    if ((macro_id == begin_ && is_reverse_scan_) || (macro_id == end_ && !is_reverse_scan_)) {
      is_iter_end_ = true;
    }
    start_row_offset = idx_row_parser->get_row_offset() - idx_row_header->get_row_count() + 1;
  }

  if (OB_SUCC(ret)) {
    // traverse all node of this macro block and collect index info
    ObDatumRowkey rowkey;
    if (need_record_micro_info_) {
      micro_endkey_allocator_.reuse();
      reuse_micro_info_array();
      if (OB_FAIL(tree_cursor_.get_child_micro_infos(
                  *iter_range_,
                  micro_endkey_allocator_,
                  micro_endkeys_,
                  micro_index_infos_,
                  hold_item_))) {
        LOG_WARN("Fail to record child micro block info", K(ret), KPC(iter_range_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_rowkey(curr_key_, prev_key_, prev_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save prev key", K(ret), K_(curr_key));
    } else if (OB_FAIL(tree_cursor_.get_current_endkey(rowkey))) {
      LOG_WARN("Fail to get current endkey", K(ret), K_(tree_cursor));
    } else if (OB_FAIL(deep_copy_rowkey(rowkey, curr_key_, curr_key_buf_))) {
      STORAGE_LOG(WARN, "Failed to save curr key", K(ret), K(rowkey));
    } else if (OB_FAIL(tree_cursor_.move_forward(is_reverse_scan_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to move cursor to next macro node", K(ret), K_(tree_cursor));
      }
    }
  }
  return ret;
}

// Sequential iter range: [prev_key, curr_key]
// Reverse iter range: [curr_key, prev_key]
int ObIndexBlockMacroIterator::get_next_macro_block(ObMacroBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  block_desc.reuse();
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get idx row parser", K(ret), K_(tree_cursor));
  } else if (OB_ISNULL(idx_row_parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx_row_parser", K(ret));
  } else if (OB_FAIL(idx_row_parser->get_header(idx_row_header))) {
    LOG_WARN("Fail to get idx row header", K(ret), KPC(idx_row_parser));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index row header", K(ret));
  } else {
    block_desc.range_.border_flag_.unset_inclusive_start();
    block_desc.range_.border_flag_.set_inclusive_end();
    block_desc.row_store_type_ = idx_row_header->row_store_type_;
    block_desc.schema_version_ = idx_row_header->schema_version_;
    block_desc.snapshot_version_ = idx_row_parser->get_snapshot_version();
    block_desc.max_merged_trans_version_ = idx_row_parser->get_max_merged_trans_version();
    block_desc.row_count_ = idx_row_header->get_row_count();
    block_desc.row_count_delta_ = idx_row_parser->get_row_count_delta();
    block_desc.contain_uncommitted_row_ = idx_row_header->contain_uncommitted_row();
    block_desc.is_deleted_ = idx_row_header->is_deleted();

    if (OB_FAIL(get_next_macro_block(block_desc.macro_block_id_,
                                     block_desc.start_row_offset_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        if (is_reverse_scan_) {
          block_desc.range_.start_key_.set_min_rowkey();
        } else {
          block_desc.range_.end_key_.set_max_rowkey();
        }
      } else {
        LOG_WARN("Fail to get next macro block id", K(ret), K(block_desc));
      }
    } else if (is_reverse_scan_) {
      block_desc.range_.start_key_ = curr_key_;
      block_desc.range_.end_key_ = prev_key_;;
    } else {
      block_desc.range_.start_key_ = prev_key_;
      block_desc.range_.end_key_ = curr_key_;;
    }
  }
  return ret;
}

int ObIndexBlockMacroIterator::locate_macro_block(
    const bool need_move_to_bound,
    const bool cursor_at_begin_bound,
    const bool lower_bound,
    const ObDatumRowkey &rowkey,
    MacroBlockId &macro_id,
    int64_t &start_row_offset,
    bool &is_beyond_the_range)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  const ObIndexBlockRowParser *parser = nullptr;
  if (OB_FAIL(tree_cursor_.pull_up_to_root())) {
    LOG_WARN("Fail to pull up sstable index tree cursor to root", K(ret));
  } else if (OB_FAIL(tree_cursor_.drill_down(
      rowkey, ObIndexBlockTreeCursor::MACRO, lower_bound, equal, is_beyond_the_range))) {
    LOG_WARN("Fail to drill down to macro level", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_macro_block_id(macro_id))) {
    LOG_WARN("Fail to get macro block logic id", K(ret));
  } else if (OB_FAIL(tree_cursor_.get_idx_parser(parser))) {
    LOG_WARN("Fail to get macro block idx parser", K(ret));
  } else if (OB_ISNULL(parser)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null parser", K(ret));
  } else if (OB_FAIL(parser->get_start_row_offset(start_row_offset))) {
    LOG_WARN("Fail to get prev row offset", K(ret), KPC(parser));
  }
  return ret;
}

int ObIndexBlockMacroIterator::get_cs_range(
    const ObITableReadInfo &rowkey_read_info,
    const bool is_start,
    ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &block_id = is_start ? begin_ : end_;
  ObCSRowId &cs_key = is_start ? cs_range.start_row_id_ : cs_range.end_row_id_;
  int64_t macro_start_row_offset = is_start ? begin_block_start_row_offset_ : end_block_start_row_offset_;

  ObMicroBlockBareIterator micro_block_iter;
  ObMicroBlockData data_block;
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *reader = nullptr;
  int64_t data_begin = 0;
  int64_t data_end = 0;
  int64_t micro_start_row_offset = 0;
  ObMacroBlockHandle macro_handle;
  blocksstable::ObMacroBlockReadInfo read_info;
  common::ObArenaAllocator io_allocator("cs_range");
  read_info.macro_block_id_ = block_id;
  read_info.offset_ = sstable_->get_macro_offset();
  read_info.size_ = sstable_->get_macro_read_size();
  read_info.io_desc_.set_wait_event(common::ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = std::max(DEFAULT_IO_WAIT_TIME_MS, GCONF._data_storage_io_timeout / 1000);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Macro iter not init", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
    LOG_WARN("Macro iter end", K(ret), KPC_(iter_range), K_(begin), K_(end));
  } else if (OB_ISNULL(read_info.buf_ =
      reinterpret_cast<char*>(io_allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle))) {
    LOG_WARN("Fail to read macro block, ", K(ret), K(read_info));
  } else if (OB_FAIL(macro_handle.wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), K(macro_handle), K(read_info));
  } else if (OB_FAIL(micro_block_iter.open(
      macro_handle.get_buffer(), macro_handle.get_data_size(), *iter_range_,
      rowkey_read_info, true/*is_left_border*/, true/*is_right_border*/))) {
    LOG_WARN("Fail to open micro block bare iter", K(ret), K(macro_handle));
  } else if (OB_FAIL(micro_block_iter.set_end_iter_idx(is_start))) {
    STORAGE_LOG(WARN, "failed to set_end_iter_idx", K(ret), K(micro_block_iter));
  } else if (OB_FAIL(micro_block_iter.get_curr_start_row_offset(micro_start_row_offset))) {
    LOG_WARN("Fail to get prev row offset", K(ret), K(macro_handle));
  } else if (OB_FAIL(micro_block_iter.get_next_micro_block_data(data_block))) {
    LOG_WARN("Fail to get get data block", K(ret), K(macro_handle));
  } else if (OB_FAIL(reader_helper.init(*allocator_))) {
    LOG_WARN("Fail to init reader helper", K(ret));
  } else if (OB_FAIL(reader_helper.get_reader(micro_block_iter.get_row_type(), reader))) {
    LOG_WARN("Fail to get reader", K(ret));
  } else if (OB_FAIL(reader->init(data_block, rowkey_read_info))) {
    LOG_WARN("Fail to init data block reader", K(ret), K(rowkey_read_info));
  } else if (OB_FAIL(reader->locate_range(
                            *iter_range_,
                            true/*is_left_border*/,
                            true/*is_right_border*/,
                            data_begin,
                            data_end))) {
    if (OB_LIKELY(ret == OB_BEYOND_THE_RANGE)) {
      if (is_start) {
        data_begin = 0;
      } else {
        data_end = -1;
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to locate range", K(ret), KPC_(iter_range), K(is_start), K(cs_range));
    }
  }
  if (OB_SUCC(ret)) {
    cs_key = macro_start_row_offset
           + micro_start_row_offset
           + (is_start ? data_begin : data_end);
  }
  return ret;
}

void ObIndexBlockMacroIterator::reuse_micro_info_array()
{
  micro_index_infos_.reuse();
  micro_endkeys_.reuse();
  tree_cursor_.release_held_path_item(hold_item_);
}


} // namespace blocksstable
} // namespace oceanbase
