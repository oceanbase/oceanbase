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

#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_micro_block_info.h"
#include "storage/blocksstable/cs_encoding/ob_cs_micro_block_transformer.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_index_block_tree_cursor.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{
ObCGRowKeyTransHelper::ObCGRowKeyTransHelper()
  : datums_() {}

int ObCGRowKeyTransHelper::trans_to_cg_range(
    const int64_t start_row_offset,
    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(trans_to_cg_rowkey(start_row_offset, range.start_key_, true/*is_start*/))) {
    LOG_WARN("Fail to trans start key", K(ret), K(range));
  } else if (OB_FAIL(trans_to_cg_rowkey(start_row_offset, range.end_key_, false/*is_start*/))) {
    LOG_WARN("Fail to trans end key", K(ret), K(range));
  } else {
    result_range_.border_flag_ = range.border_flag_;
  }

  return ret;
}

int ObCGRowKeyTransHelper::trans_to_cg_rowkey(
    const int64_t start_row_offset,
    const ObDatumRowkey &rowkey,
    const bool is_start_key)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey &cg_rowkey = is_start_key ? result_range_.start_key_ : result_range_.end_key_;
  if (rowkey.is_min_rowkey() || rowkey.is_max_rowkey()) {
    cg_rowkey = rowkey;
  } else if (OB_UNLIKELY(rowkey.get_datum_cnt() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey column cnt of cg sstable", K(ret), K(rowkey));
  } else {
    ObStorageDatum &datum = is_start_key ? datums_[0] : datums_[1];
    const int64_t row_idx = rowkey.datums_[0].get_int();
    datum.set_int(row_idx - start_row_offset);
    if (OB_FAIL(cg_rowkey.assign(&datum, 1))) {
      LOG_WARN("Fail to assign cg rowkey", K(ret), K(datum), K(rowkey), K(start_row_offset));
    }
  }
  return ret;
}

/*-------------------------------------ObIndexBlockTreePathItem------------------------------------*/
void ObIndexBlockTreePathItem::reset()
{
  macro_block_id_.reset();
  curr_row_idx_ = 0;
  curr_row_idx_ = 0;
  row_count_ = 0;
  block_data_.reset();
  if (block_from_cache_) {
    cache_handle_.reset();
  }
  is_root_micro_block_ = false;
  is_block_data_held_ = false;
  is_block_allocated_ = false;
  is_block_transformed_ = false;
  block_from_cache_ = false;
}

ObIndexBlockTreePath::ObIndexBlockTreePath()
  : allocator_(), item_stack_(allocator_), next_item_(nullptr), path_() {}

ObIndexBlockTreePath::~ObIndexBlockTreePath()
{
  reset();
}

void ObIndexBlockTreePath::reset()
{
  item_stack_.reset();
  next_item_ = nullptr;
  path_.reset();
  allocator_.reset();
}

int ObIndexBlockTreePath::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "IdxBlkTreePath");
  if (OB_FAIL(allocator_.init(
      lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      mem_attr))) {
    LOG_WARN("Fail to init allocator", K(ret));
  } else if (OB_FAIL(item_stack_.acquire_next_item_ptr(next_item_))) {
    LOG_WARN("Fail to init tree path", K(ret));
  }
  return ret;
}

int ObIndexBlockTreePath::push(ObIndexBlockTreePathItem *item_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_ptr != next_item_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid item pointer", K(ret), K(item_ptr), K_(next_item));
  } else if (OB_FAIL(path_.push_back(item_ptr))) {
    LOG_WARN("Fail to push an item in path", K(ret), KPC(item_ptr));
  } else if (OB_FAIL(item_stack_.acquire_next_item_ptr(next_item_))) {
    LOG_WARN("Fail to acquire next item pointer", K(ret), KPC(next_item_));
  }
  return ret;
}

int ObIndexBlockTreePath::pop(ObIndexBlockTreePathItem *&item_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(path_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Path is empty", K(ret));
  } else if (OB_FAIL(item_stack_.release_item(next_item_))) {
    LOG_WARN("Fail to release next item", K(ret), K_(item_stack), KPC(next_item_), KPC(item_ptr));
  } else if (OB_FAIL(path_.pop_back(item_ptr))) {
    LOG_WARN("Fail to pop current path item", K(ret));
  } else if (OB_FAIL(item_stack_.top(next_item_))) {
    LOG_WARN("Fail to get top pointer", K(ret), K_(item_stack));
  }

  return ret;
}

int ObIndexBlockTreePath::at(int64_t depth, const ObIndexBlockTreePathItem *&item_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(path_.at(depth, const_cast<ObIndexBlockTreePathItem *&>(item_ptr)))) {
    LOG_WARN("Fail to random get item pointer in path", K(ret), K(path_.count()));
  }
  return ret;
}

int ObIndexBlockTreePath::get_next_item_ptr(ObIndexBlockTreePathItem *&next_item_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_item_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to get next item pointer", K(ret));
  } else {
    next_item_ptr = next_item_;
  }
  return ret;
}

ObIndexBlockTreePath::PathItemStack::PathItemStack(ObIAllocator &allocator)
  : allocator_(&allocator), fix_buf_(), var_buf_(nullptr),
    buf_capacity_(MAX_TREE_FIX_BUF_LENGTH), idx_(-1) {}

void ObIndexBlockTreePath::PathItemStack::reset()
{
  if (idx_ < MAX_TREE_FIX_BUF_LENGTH) {
    for (int64_t i = 0; i <= idx_; ++i) {
      release_item_memory(fix_buf_[i]);
      fix_buf_[i].reset();
    }
  } else {
    for (int64_t i = 0; i < MAX_TREE_FIX_BUF_LENGTH; ++i) {
      release_item_memory(fix_buf_[i]);
      fix_buf_[i].reset();
    }
    for (int64_t i = 0; i < idx_ - MAX_TREE_FIX_BUF_LENGTH; ++i) {
      release_item_memory(var_buf_[i]);
      var_buf_[i].reset();
    }
  }
  if (nullptr != var_buf_) {
    allocator_->free(var_buf_);
    var_buf_ = nullptr;
  }
  buf_capacity_ = MAX_TREE_FIX_BUF_LENGTH;
  idx_ = -1;
}

int ObIndexBlockTreePath::PathItemStack::acquire_next_item_ptr(
    ObIndexBlockTreePathItem *&next_item_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx_ >= buf_capacity_ - 1)) {
    if (OB_FAIL(expand())) {
      LOG_WARN("Tree path fail to expand", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ++idx_;
    if (OB_FAIL(get_curr_item(next_item_ptr))) {
      LOG_WARN("Fail to get current item", K(ret));
    }
  }
  return ret;
}

int ObIndexBlockTreePath::PathItemStack::release_item(
    ObIndexBlockTreePathItem *release_item_ptr)
{
  int ret = OB_SUCCESS;
  ObIndexBlockTreePathItem *curr_item = nullptr;
  if (OB_FAIL(get_curr_item(curr_item))) {
    LOG_WARN("Fail to get curr item", K(ret));
  } else if (OB_UNLIKELY(release_item_ptr != curr_item) || OB_ISNULL(release_item_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected path behavior",
        K(ret), K_(idx), K_(buf_capacity), KPC(curr_item), K(release_item_ptr));
  } else {
    release_item_memory(*release_item_ptr);
    release_item_ptr->reset();
    --idx_;
  }
  return ret;
}

int ObIndexBlockTreePath::PathItemStack::top(ObIndexBlockTreePathItem *&curr_top_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx_ < 0 || idx_ >= buf_capacity_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Item stack is not inited", K(ret));
  } else if (OB_FAIL(get_curr_item(curr_top_item))) {
    LOG_WARN("Fail to get current item", K(ret));
  }
  return ret;
}


int ObIndexBlockTreePath::PathItemStack::get_curr_item(ObIndexBlockTreePathItem *&curr_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Fail to get current item", K(ret), K_(idx), K_(buf_capacity));
  } else if (idx_ >= MAX_TREE_FIX_BUF_LENGTH) {
    if (OB_ISNULL(var_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null pointer for var buf", K(ret), K_(idx), K_(buf_capacity));
    } else {
      curr_item = var_buf_ + (idx_ - MAX_TREE_FIX_BUF_LENGTH);
    }
  } else {
    curr_item = &fix_buf_[idx_];
  }
  return ret;
}

void ObIndexBlockTreePath::PathItemStack::release_item_memory(
    ObIndexBlockTreePathItem &release_item)
{
  if (!release_item.is_block_data_held_ && release_item.is_block_allocated_) {
    allocator_->free(const_cast<char *>(release_item.block_data_.buf_));
    release_item.block_data_.buf_ = nullptr;
  }
}

int ObIndexBlockTreePath::PathItemStack::expand()
{
  int ret = OB_SUCCESS;
  int64_t new_capacity = buf_capacity_ * 2;
  int64_t new_alloc_size
      = (new_capacity - MAX_TREE_FIX_BUF_LENGTH) * sizeof(ObIndexBlockTreePathItem);
  void *new_var_buf = nullptr;
  if (OB_ISNULL(new_var_buf = allocator_->alloc(new_alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory", K(ret), K(new_alloc_size));
  } else {
    MEMSET(new_var_buf, 0, new_alloc_size);
    for (int64_t i = 0; i < buf_capacity_ - MAX_TREE_FIX_BUF_LENGTH; ++i) {
      reinterpret_cast<ObIndexBlockTreePathItem *>(new_var_buf)[i] = var_buf_[i];
    }
    if (nullptr != var_buf_) {
      allocator_->free(var_buf_);
    }
    var_buf_ = reinterpret_cast<ObIndexBlockTreePathItem *>(new_var_buf);
    buf_capacity_ = new_capacity;
  }
  return ret;
}

/*-------------------------------------IndexBlockTreeCursor------------------------------------*/

ObIndexBlockTreeCursor::ObIndexBlockTreeCursor()
  : cursor_path_(), index_block_cache_(nullptr), reader_(nullptr), micro_reader_helper_(),
    tree_type_(TreeType::TREE_TYPE_MAX), rowkey_helper_(),
    tenant_id_(OB_INVALID_TENANT_ID), rowkey_column_cnt_(0),
    curr_path_item_(), row_(),
    idx_row_parser_(), read_info_(nullptr), sstable_meta_handle_(),
    is_normal_cg_sstable_(false), is_inited_(false) {}

ObIndexBlockTreeCursor::~ObIndexBlockTreeCursor()
{
  reset();
}

void ObIndexBlockTreeCursor::reset()
{
  if (is_inited_) {
    row_.reset();
    vector_endkey_.reset();
    cursor_path_.reset();
    micro_reader_helper_.reset();
    reader_ = nullptr;
    read_info_ = nullptr;
    sstable_meta_handle_.reset();
    is_normal_cg_sstable_ = false;
    is_inited_ = false;
  }
}

int ObIndexBlockTreeCursor::init(
    const ObSSTable &sstable,
    ObIAllocator &allocator,
    const ObITableReadInfo *read_info,
    const TreeType tree_type)
{
  int ret = OB_SUCCESS;
  int64_t sstable_rowkey_col_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Double init for index block tree cursor", K(ret));
  } else if (OB_ISNULL(read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null read info", K(ret));
  } else if (OB_FAIL(cursor_path_.init())) {
    LOG_WARN("Fail to init cursor path", K(ret));
  } else if (OB_FAIL(cursor_path_.get_next_item_ptr(curr_path_item_))) {
    LOG_WARN("Fail to init curr path item pointer", K(ret));
  } else if (OB_FAIL(micro_reader_helper_.init(allocator))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("Fail to get sstable meta handle", K(ret));
  } else if (FALSE_IT(sstable_rowkey_col_cnt = sstable_meta_handle_.get_sstable_meta().get_basic_meta().rowkey_column_count_)) {
  } else if (OB_UNLIKELY(!sstable.is_normal_cg_sstable() && sstable_rowkey_col_cnt != read_info->get_rowkey_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Rowkey column count not match between read info and sstable",
        K(ret), KPC(read_info), K(sstable_rowkey_col_cnt));
  } else {
    tenant_id_ = MTL_ID();
    ObRowStoreType root_row_store_type
        = static_cast<ObRowStoreType>(sstable_meta_handle_.get_sstable_meta().get_basic_meta().root_row_store_type_);
    curr_path_item_->row_store_type_ = root_row_store_type;
    read_info_ = read_info;
    rowkey_column_cnt_ = read_info_->get_rowkey_count();
    tree_type_ = tree_type;
    is_normal_cg_sstable_ = sstable.is_normal_cg_sstable();
    switch (tree_type) {
    case TreeType::INDEX_BLOCK: {
      curr_path_item_->block_data_ =
          sstable_meta_handle_.get_sstable_meta().get_root_info().get_block_data();
      break;
    }
    case TreeType::DATA_MACRO_META: {
      curr_path_item_->block_data_ =
          sstable_meta_handle_.get_sstable_meta().get_macro_info().get_macro_meta_data();
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected meta type", K(ret), K(tree_type));
    }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(root_row_store_type = curr_path_item_->block_data_.get_store_type())) {
    } else if (FALSE_IT(curr_path_item_->row_store_type_ = root_row_store_type)) {
    } else if (OB_FAIL(row_.init(allocator, rowkey_column_cnt_ + 1))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(init_curr_endkey(row_, rowkey_column_cnt_ + 1))) {
       STORAGE_LOG(WARN, "Failed to init curr endkey", K(ret));
    } else if (nullptr != curr_path_item_->block_data_.get_extra_buf()) {
      curr_path_item_->is_block_transformed_ = true;
    } else if (OB_FAIL(set_reader(root_row_store_type))) {
      LOG_WARN("Fail to set micro block reader", K(ret));
    } else if (OB_FAIL(reader_->init(curr_path_item_->block_data_, &(read_info_->get_datum_utils())))) {
      LOG_WARN("Fail to init micro block reader", K(ret));
    }

    if (OB_SUCC(ret)) {
      index_block_cache_ = &(ObStorageCacheSuite::get_instance().get_index_block_cache());
      is_inited_ = true;
    }
  }
  return ret;
}

// TODO: @saitong.zst support better upper bound semantic
int ObIndexBlockTreeCursor::drill_down(
    const ObDatumRowkey &rowkey,
    const MoveDepth depth,
    const bool is_lower_bound,
    bool &equal,
    bool &is_beyond_the_range)
{
  int ret = OB_SUCCESS;
  equal = false;
  ObDatumRowkey tmp_endkey;
  bool compare_schema_rowkey = false;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tree cursor not inited", K(ret));
  } else if (OB_FAIL(drill_down(rowkey, depth, is_beyond_the_range))) {
    LOG_WARN("Fail to do lower bound drill down", K(ret));
  } else if (FALSE_IT(
      compare_schema_rowkey = rowkey.datum_cnt_ == read_info_->get_schema_rowkey_count())) {
  } else if (is_lower_bound || rowkey.is_min_rowkey() || rowkey.is_max_rowkey()) {
  } else {
    // move to upper bound
    while (OB_SUCC(ret) && 0 == cmp_ret) {
      if (OB_FAIL(get_current_endkey(tmp_endkey, compare_schema_rowkey))) {
        LOG_WARN("Fail to get current endkey", K(ret));
      } else if (FALSE_IT(tmp_endkey.datum_cnt_ = rowkey.datum_cnt_)) {
      } else if (OB_FAIL(tmp_endkey.compare(
          rowkey,
          read_info_->get_datum_utils(),
          cmp_ret))) {
        LOG_WARN("Fail to compare datum rowkey", K(ret), K(tmp_endkey), K(rowkey));
      } else if (0 == cmp_ret && OB_FAIL(move_forward(false/*sequential*/))) {
        LOG_WARN("Fail to move forward tree cursor", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
    } else if (OB_ITER_END == ret) {
      if (OB_FAIL(pull_up_to_root())) {
        LOG_WARN("Fail to pull up sstable index tree cursor to root", K(ret));
      } else if (OB_FAIL(drill_down(ObDatumRowkey::MAX_ROWKEY, depth, is_beyond_the_range))) {
        LOG_WARN("Fail to drill down to macro level with max rowkey", K(ret));
      }
    } else {
      LOG_WARN("Fail to find last index block with tree cursor", K(ret), K(rowkey));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (rowkey.is_min_rowkey() || rowkey.is_max_rowkey()) {
    equal = false;
  } else if (OB_FAIL(get_current_endkey(tmp_endkey, compare_schema_rowkey))) {
    LOG_WARN("Fail to get current endkey", K(ret));
  } else if (OB_FAIL(tmp_endkey.compare(
      rowkey,
      read_info_->get_datum_utils(),
      cmp_ret))) {
    LOG_WARN("Fail to compare datum rowkey", K(ret), K(tmp_endkey), K(rowkey));
  } else {
    equal = cmp_ret == 0;
  }
  return ret;
}

int ObIndexBlockTreeCursor::drill_down(
    const ObDatumRowkey &rowkey,
    const MoveDepth depth,
    bool &is_beyond_the_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tree cursor not inited", K(ret));
  } else {
    switch (depth) {
      case ONE_LEVEL: {
        if (!cursor_path_.empty()) {
          // Not an empty path
        } else if (OB_FAIL(get_next_level_row_cnt(curr_path_item_->row_count_))) {
          LOG_WARN("Fail to get row count for current index micro block", K(ret));
        } else if (OB_FAIL(locate_rowkey_in_curr_block(rowkey, is_beyond_the_range))) {
          LOG_WARN("Fail to locate rowkey in current block",
              K(ret), K(rowkey), KPC(curr_path_item_), K(is_beyond_the_range));
        } else {
          curr_path_item_->is_root_micro_block_ = true;
          curr_path_item_->row_store_type_
              = static_cast<ObRowStoreType>(sstable_meta_handle_.get_sstable_meta().get_basic_meta().root_row_store_type_);
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(drill_down())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to drill down to next level block", K(ret));
          }
        } else if (OB_FAIL(locate_rowkey_in_curr_block(rowkey, is_beyond_the_range))) {
          LOG_WARN("Fail to locate rowkey in block", K(ret), K(rowkey), KPC(curr_path_item_));
        }
        break;
      }
      case MACRO:
      case LEAF: {
        bool reach_target_depth = false;
        if (OB_UNLIKELY(!cursor_path_.empty())) {
          // Not an empty path
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Drill to specific level has to start from root",
              K(ret), K(depth), K_(cursor_path));
        } else if (OB_FAIL(get_next_level_row_cnt(curr_path_item_->row_count_))) {
          LOG_WARN("Fail to get row count for current index micro block", K(ret));
        } else if (OB_FAIL(locate_rowkey_in_curr_block(rowkey, is_beyond_the_range))) {
          LOG_WARN("Fail to locate rowkey in curr block", K(ret), K(rowkey), KPC(curr_path_item_));
        } else if (OB_FAIL(check_reach_target_depth(depth, reach_target_depth))) {
          LOG_WARN("Fail to check if cursor reach target depth", K(ret), K(depth));
        } else {
          curr_path_item_->is_root_micro_block_ = true;
          curr_path_item_->row_store_type_
              = static_cast<ObRowStoreType>(sstable_meta_handle_.get_sstable_meta().get_basic_meta().root_row_store_type_);
        }

        while (OB_SUCC(ret) && !reach_target_depth) {
          if (OB_FAIL(drill_down())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("Fail to drill down to next level", K(ret));
            }
          } else if (OB_FAIL(locate_rowkey_in_curr_block(rowkey, is_beyond_the_range))) {
            LOG_WARN("Fail to locate rowkey in block", K(ret), K(rowkey), KPC(curr_path_item_));
          } else if (OB_FAIL(check_reach_target_depth(depth, reach_target_depth))) {
            LOG_WARN("Fail to check if cursor reach target depth", K(ret), K(depth));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Drill down to depth is not supported", K(ret), K(depth));
      }
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::drill_down()
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const int64_t curr_row_offset = idx_row_parser_.get_row_offset();
  if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
    LOG_WARN("Fail to get index row header", K(ret));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null idx row header", K(ret));
  } else if (OB_UNLIKELY(idx_row_header->is_data_block())) {
    ret = OB_ITER_END;
    LOG_DEBUG("Cursor on leaf node, can not drill down", K(ret), KPC(idx_row_header));
  } else if (OB_FAIL(cursor_path_.push(curr_path_item_))) {
    LOG_WARN("Fail to push tree item to path", K(ret), KPC(curr_path_item_));
  } else if (OB_FAIL(cursor_path_.get_next_item_ptr(curr_path_item_))) {
    LOG_WARN("Fail to get next item pointer", K(ret));
  } else {
    ObRowStoreType new_row_store_type = static_cast<ObRowStoreType>(idx_row_header->get_row_store_type());
    curr_path_item_->reset();
    if (OB_FAIL(get_next_level_block(idx_row_header->get_macro_id(), *idx_row_header, curr_row_offset))) {
      LOG_WARN("Fail to get micro block data handle", K(ret), KPC(idx_row_header), K(curr_row_offset));
    } else if (curr_path_item_->is_block_transformed_) {
      // No need to init micro reader
    } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(new_row_store_type)))) {
      LOG_WARN("Fail to set row reader", K(ret), K(new_row_store_type));
    } else if (OB_FAIL(reader_->init(
        curr_path_item_->block_data_, &(read_info_->get_datum_utils())))) {
      LOG_WARN("Fail to get micro block buffer handle", K(ret));
    }

    if (OB_FAIL(ret)){
    } else if (OB_FAIL(get_next_level_row_cnt(curr_path_item_->row_count_))) {
      LOG_WARN("Fail to get row count of this index micro block", K(ret), K_(cursor_path));
    } else {
      curr_path_item_->row_store_type_ = new_row_store_type;
      curr_path_item_->macro_block_id_ = idx_row_header->get_macro_id();
      curr_path_item_->curr_row_idx_ = 0;
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::locate_rowkey_in_curr_block(
    const ObDatumRowkey &ori_rowkey,
    bool &is_beyond_the_range)
{
  int ret = OB_SUCCESS;
  is_beyond_the_range = false;
  ObStoreRange range;
  ObDatumRowkey rowkey;
  bool equal = false;
  const bool is_need_trans_range = is_normal_cg_sstable_ && 0 != curr_path_item_->start_row_offset_;
  if (OB_UNLIKELY(!ori_rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Rowkey is not valid", K(ret), K(ori_rowkey));
  } else if (is_need_trans_range && OB_FAIL(rowkey_helper_.trans_to_cg_rowkey(curr_path_item_->start_row_offset_,
                         ori_rowkey, true/* is start */))) {
    LOG_WARN("Fail to trans to cg rowkey", K(ret), K(ori_rowkey));
  } else if (FALSE_IT(rowkey = is_need_trans_range ? rowkey_helper_.get_result_start_key() : ori_rowkey)) {
  } else if (rowkey.is_min_rowkey()) {
    curr_path_item_->curr_row_idx_ = 0;
  } else if (rowkey.is_max_rowkey()) {
    is_beyond_the_range = true;
    curr_path_item_->curr_row_idx_ = curr_path_item_->row_count_ - 1;
  } else if (curr_path_item_->is_block_transformed_) {
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret), KPC(curr_path_item_));
    } else if (OB_FAIL(search_rowkey_in_transformed_block(
        rowkey, *idx_data_header, curr_path_item_->curr_row_idx_, equal))) {
      if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
        is_beyond_the_range = true;
        curr_path_item_->curr_row_idx_ = curr_path_item_->row_count_ - 1;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to locate rowkey in transformed index block", K(ret), KPC(idx_data_header));
      }
    }
  } else {
    // Locate with micro reader
    int64_t begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    int64_t end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    ObDatumRange range;
    range.start_key_ = rowkey;
    range.set_left_closed();
    range.end_key_.set_max_rowkey();
    range.set_right_open();
    if (OB_FAIL(reader_->locate_range(range, true, false, begin_idx, end_idx, true))) {
      if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
        curr_path_item_->curr_row_idx_ = begin_idx - 1;
        is_beyond_the_range = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to locate row block", K(ret), K(rowkey), KPC(curr_path_item_));
      }
    } else {
      curr_path_item_->curr_row_idx_ = begin_idx;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
    LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_), K(rowkey));
  }

  LOG_DEBUG("[INDEX_BLOCK] Index block tree search: ",
      K(cursor_path_.depth()), KPC(curr_path_item_), K(idx_row_parser_), K_(row));
  return ret;
}

int ObIndexBlockTreeCursor::search_rowkey_in_transformed_block(
    const ObDatumRowkey &rowkey,
    const ObIndexBlockDataHeader &idx_data_header,
    int64_t &row_idx,
    bool &equal,
    const bool lower_bound)
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = read_info_->get_datum_utils();
  if (OB_UNLIKELY(!idx_data_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid idx data header", K(ret), K(idx_data_header));
  } else if (OB_FAIL(idx_data_header.rowkey_vector_->locate_key(0,
                                                                idx_data_header.row_cnt_,
                                                                rowkey,
                                                                datum_utils,
                                                                row_idx,
                                                                lower_bound))) {
    LOG_WARN("Failed to locate key in rowkey vector", K(ret), K(rowkey), K(idx_data_header));
  } else if (row_idx == idx_data_header.row_cnt_) {
    ret = OB_BEYOND_THE_RANGE;
  }
  return ret;
}

int ObIndexBlockTreeCursor::locate_range_in_curr_block(
    const ObDatumRange &ori_range,
    int64_t &begin_idx,
    int64_t &end_idx)
{
  int ret = OB_SUCCESS;
  begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  ObDatumRange range;
  const bool is_need_trans_range = is_normal_cg_sstable_ && 0 != curr_path_item_->start_row_offset_;
  if (OB_UNLIKELY(!ori_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid range", K(ret), K(ori_range));
  } else if (is_need_trans_range && OB_FAIL(rowkey_helper_.trans_to_cg_range(curr_path_item_->start_row_offset_, ori_range))) {
    LOG_WARN("Fail to trans to cg range", K(ret), K(ori_range));
  } else if (FALSE_IT(range = is_need_trans_range ? rowkey_helper_.get_result_range() : ori_range)) {
  } else if (!curr_path_item_->is_block_transformed_) {
    if (OB_FAIL(reader_->locate_range(range, true, true, begin_idx, end_idx, true))) {
      LOG_WARN("Fail to locate range with micro block reader", K(ret), K(range), KPC(curr_path_item_));
    }
  } else {
    bool equal = false;
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret), KPC(curr_path_item_));
    } else if (OB_FAIL(search_rowkey_in_transformed_block(
        range.get_start_key(), *idx_data_header, begin_idx, equal, true/*lower_bound*/))) {
      if (OB_UNLIKELY(ret != OB_BEYOND_THE_RANGE)) {
        LOG_WARN("Fail to search start key in transformed block", K(ret));
      }
    } else if (!range.get_border_flag().inclusive_start() && equal) {
      ++begin_idx;
      if (begin_idx == curr_path_item_->row_count_) {
        ret = OB_BEYOND_THE_RANGE;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(search_rowkey_in_transformed_block(
        range.get_end_key(), *idx_data_header, end_idx, equal, !range.get_border_flag().inclusive_end()))) {
      if (OB_LIKELY(ret == OB_BEYOND_THE_RANGE)) {
        end_idx = idx_data_header->row_cnt_ - 1;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to serach end key in transformed block", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::pull_up(const bool cascade, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  bool skip_read_block = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tree cursor not inited", K(ret));
  } else if (OB_UNLIKELY(cursor_path_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tree query path is empty, can not pull up", K(ret));
  } else if (OB_FAIL(cursor_path_.pop(curr_path_item_))) {
    LOG_WARN("Fail to pop previous path item", K(ret), K(cursor_path_.depth()));
  } else if (cascade) {
    skip_read_block = (is_reverse_scan && curr_path_item_->curr_row_idx_ == 0)
        || (!is_reverse_scan && curr_path_item_->curr_row_idx_ == curr_path_item_->row_count_);
  }

  if (OB_FAIL(ret) || skip_read_block) {
  } else if (curr_path_item_->is_block_transformed_) {
    if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
      LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_));
    }
  } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(curr_path_item_->row_store_type_)))) {
    LOG_WARN("Fail to set row reader", K(ret));
  } else if (OB_FAIL(reader_->init(
      curr_path_item_->block_data_, &(read_info_->get_datum_utils())))) {
    LOG_WARN("Fail to init micro block row reader", K(ret), KPC(curr_path_item_));
  } else if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
    LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_));
  }
  return ret;
}

int ObIndexBlockTreeCursor::pull_up_to_root()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tree cursor not inited", K(ret));
  }
  while (OB_SUCC(ret) && !cursor_path_.empty()) {
    if (OB_FAIL(cursor_path_.pop(curr_path_item_))) {
      LOG_WARN("Fail to pop from tree path", K(ret));
    }
  }
  if (OB_FAIL(ret) || curr_path_item_->is_block_transformed_) {
  } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(curr_path_item_->row_store_type_)))) {
    LOG_WARN("Fail to set reader");
  } else if (OB_FAIL(reader_->init(
      curr_path_item_->block_data_, &(read_info_->get_datum_utils())))) {
    LOG_WARN("Fail to init reader", K(ret));
  }
  return ret;
}

int ObIndexBlockTreeCursor::move_forward(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  int64_t step = is_reverse_scan ? -1 : 1;
  int64_t next_idx = curr_path_item_->curr_row_idx_ + step;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tree cursor not inited", K(ret));
  } else if (next_idx < 0 || next_idx >= curr_path_item_->row_count_) {
    // Currnet block iterate finish, pull up and find next node
    const ObIndexBlockRowHeader *idx_row_header = nullptr;
    bool reach_target_depth = false;
    MoveDepth depth = MoveDepth::DEPTH_MAX;
    if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
      LOG_WARN("Fail to get index block row header", K(ret));
    } else if (OB_ISNULL(idx_row_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index row header", K(ret));
    } else if (OB_UNLIKELY(!idx_row_header->is_macro_node() && !idx_row_header->is_data_block())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid depth for cursor to move forward", K(ret));
    } else {
      depth = idx_row_header->is_macro_node() ? MACRO : LEAF;
    }

    while (OB_SUCC(ret)
        && !cursor_path_.empty()
        && (next_idx < 0 || next_idx >= curr_path_item_->row_count_)) {
      // pull up and calculate next
      if (OB_FAIL(pull_up(true /* cascade */, is_reverse_scan))) {
        LOG_WARN("Fail to pull up index block tree cursor", K(ret), KPC(curr_path_item_));
      } else {
        next_idx = curr_path_item_->curr_row_idx_ + step;
      }
      LOG_DEBUG("[INDEX_BLOCK] Pull up", K(ret), KPC(curr_path_item_));
    }

    if (OB_FAIL(ret)) {
    } else if (cursor_path_.empty() && (next_idx < 0 || next_idx >= curr_path_item_->row_count_)) {
      // Scan finished
      ret = OB_ITER_END;
      LOG_DEBUG("[INDEX_BLOCK] Same level scan end", K(ret), K(next_idx), KPC(curr_path_item_));
    } else if (FALSE_IT(curr_path_item_->curr_row_idx_ = next_idx)) {
    } else if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
      LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_));
    } else if (OB_FAIL(check_reach_target_depth(depth, reach_target_depth))) {
      LOG_WARN("Fail to check if reach the target depth", K(ret));
    }

    while (OB_SUCC(ret) && !reach_target_depth) {
      if (OB_FAIL(drill_down())) {
        LOG_WARN("Fail to drill down to next level", K(ret), KPC(curr_path_item_), K_(cursor_path));
      } else {
        curr_path_item_->curr_row_idx_ = is_reverse_scan ? (curr_path_item_->row_count_ - 1) : 0;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
        LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_));
      } else if (OB_FAIL(check_reach_target_depth(depth, reach_target_depth))) {
        LOG_WARN("Fail to check if reach the target depth", K(ret));
      }
      LOG_DEBUG("[INDEX_BLOCK] Drill down", K(ret), KPC(curr_path_item_));
    }
  } else if (FALSE_IT(curr_path_item_->curr_row_idx_ = next_idx)) {
  } else if (OB_FAIL(read_next_level_row(curr_path_item_->curr_row_idx_))) {
    LOG_WARN("Fail to read next level row", K(ret), KPC(curr_path_item_));
  } else {
    LOG_DEBUG("[INDEX_BLOCK] Move forward", K(ret), KPC(curr_path_item_));
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_idx_parser(const ObIndexBlockRowParser *&parser)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    parser = &idx_row_parser_;
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_idx_row_header(const ObIndexBlockRowHeader *&idx_header)
{
  int ret = OB_SUCCESS;
  idx_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else if (OB_ISNULL(idx_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Got null pointer for index block row header", K(ret));
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_macro_block_id(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else if (OB_ISNULL(idx_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Got null pointer for index block row header", K(ret));
  } else if (idx_header->is_data_block()) {
    macro_id = curr_path_item_->macro_block_id_;
  } else {
    macro_id = idx_header->get_macro_id();
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_child_micro_infos(
    const ObDatumRange &range,
    ObArenaAllocator &endkey_allocator,
    ObIArray<ObDatumRowkey> &endkeys,
    ObIArray<ObMicroIndexInfo> &micro_index_infos,
    ObIndexBlockTreePathItem &hold_item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_FAIL(drill_down())) {
    LOG_WARN("Failed to drill down to next level intermediate micro block",
        K(ret), K(cursor_path_.depth()));
  } else if (OB_FAIL(get_micro_block_infos(range, micro_index_infos))) {
    LOG_WARN("Failed to get micro block infos", K(ret));
  } else if (OB_FAIL(get_micro_block_endkeys(range, endkey_allocator, micro_index_infos, endkeys))) {
    LOG_WARN("Failed to get micro block endkeys", K(ret));
  } else if (FALSE_IT(hold_item = *curr_path_item_)) {
  } else if (FALSE_IT(curr_path_item_->is_block_data_held_ = true)) {
  } else if (OB_FAIL(pull_up())) {
    LOG_WARN("Failed to pull up to previous micro block", K(ret));
  }
  return ret;
}

int ObIndexBlockTreeCursor::release_held_path_item(ObIndexBlockTreePathItem &held_item)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    if (held_item.is_block_allocated_) {
      cursor_path_.get_allocator()->free(const_cast<char *>(held_item.block_data_.buf_));
    }
    held_item.reset();
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_current_endkey(ObDatumRowkey &endkey, const bool get_schema_rowkey)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_datum_cnt = get_schema_rowkey ?
      read_info_->get_schema_rowkey_count() : rowkey_column_cnt_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to get current endkey", K(ret));
  } else if (curr_path_item_->is_block_transformed_) {
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret));
    } else if (OB_FAIL(endkey.assign(vector_endkey_.datums_, rowkey_datum_cnt))) {
      LOG_WARN("Failed to assign endkey", K(ret), K(rowkey_datum_cnt), K(vector_endkey_));
    }
  } else if (OB_FAIL(endkey.assign(row_.storage_datums_, rowkey_datum_cnt))) {
    LOG_WARN("Failed to assign endkey", K(ret), K(rowkey_datum_cnt), K(row_));
  }
  return ret;
}

int ObIndexBlockTreeCursor::estimate_range_macro_count(const ObDatumRange &range, int64_t &macro_count, int64_t &micro_count)
{
  int ret = OB_SUCCESS;

  bool is_beyond_range = false;
  bool is_reach_macro = false;
  bool is_reach_leaf = false;
  int64_t begin_idx = 0;
  int64_t end_idx = 0;
  macro_count = 0;
  micro_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_FAIL(pull_up_to_root())) {
    STORAGE_LOG(WARN, "Fail to pull up tree cursor back to root", K(ret));
  } else if (OB_FAIL(read_next_level_row(0))) {
    LOG_WARN("Fail to get row count for current index micro block", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (0 == micro_count) {
        if (OB_FAIL(locate_range_in_curr_block(range, begin_idx, end_idx))) {
          LOG_WARN("Fail to locate range in current block", K(ret), K(range));
        } else {
          micro_count = end_idx - begin_idx + 1;
        }
      } else {
        micro_count = micro_count * curr_path_item_->row_count_;
      }
      if (OB_FAIL(ret)) {
      } else if (!is_reach_macro) {
        macro_count = micro_count;
      }
      if (OB_FAIL(ret)) {
      } else if (!is_reach_macro && OB_FAIL(check_reach_target_depth(MACRO, is_reach_macro))) {
        LOG_WARN("Fail to check if cursor reach macro depth", K(ret));
      } else if (is_reach_macro && OB_FAIL(check_reach_target_depth(LEAF, is_reach_leaf))) {
        LOG_WARN("Fail to check if cursor reach macro depth", K(ret));
      } else if (is_reach_leaf) {
        break;
      } else if (OB_FAIL(drill_down(range.get_start_key(), ONE_LEVEL, is_beyond_range))) {
        LOG_WARN("Fail to drill down one level", K(ret), K(range.get_start_key()));
      } else if (is_beyond_range) {
        macro_count = 0;
        micro_count = 0;
        break;
      }
    }
  }

  if (OB_FAIL(ret) || is_beyond_range) {
  } else {
    if (0 == macro_count) {
      macro_count = 1;
    }
    if (0 == micro_count) {
      micro_count = 1;
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::set_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_reader_helper_.get_reader(store_type, reader_))) {
    LOG_WARN("Fail to get micro block reader", K(ret), K(store_type));
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_next_level_block(
    const MacroBlockId &macro_block_id,
    const ObIndexBlockRowHeader &idx_row_header,
    const int64_t curr_row_offset)
{
  int ret = OB_SUCCESS;
  int64_t absolute_offset = 0;
  if (OB_UNLIKELY(!macro_block_id.is_valid() || !idx_row_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block id or index block row data",
        K(ret), K(macro_block_id), K(idx_row_header));
  } else {
    absolute_offset = sstable_meta_handle_.get_sstable_meta().get_macro_info().get_nested_offset()
        + idx_row_header.get_block_offset();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(index_block_cache_->get_cache_block(
      tenant_id_,
      macro_block_id,
      absolute_offset,
      idx_row_header.get_block_size(),
      curr_path_item_->cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("Fail to get micro block handle from block cache",
        K(ret), K(macro_block_id), K(idx_row_header));
    } else if (OB_FAIL(load_micro_block_data(macro_block_id, absolute_offset, idx_row_header))) {
      LOG_WARN("fail to load micro block data",
          K(ret), K(macro_block_id), K(absolute_offset), K(idx_row_header));
    }
  } else {
    curr_path_item_->block_data_ = *curr_path_item_->cache_handle_.get_block_data();
    curr_path_item_->block_from_cache_ = true;
    curr_path_item_->is_block_transformed_ = curr_path_item_->block_data_.get_extra_buf() != nullptr;
    // curr_path_item_->is_block_transformed_ = false;
  }
  curr_path_item_->start_row_offset_ = 0;
  if (OB_SUCC(ret) && TreeType::INDEX_BLOCK == tree_type_ && (idx_row_header.is_leaf_block() || idx_row_header.is_data_block())) {
    curr_path_item_->start_row_offset_ = curr_row_offset - idx_row_header.row_count_ + 1;
  }
  return ret;
}

int ObIndexBlockTreeCursor::load_micro_block_data(const MacroBlockId &macro_block_id,
                                                  const int64_t block_offset,
                                                  const ObIndexBlockRowHeader &idx_row_header)
{
  // TODO: optimize with prefetch
  // Cache miss, read in sync IO
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObMacroBlockReader macro_reader;
  ObMacroBlockReadInfo read_info;
  ObMicroBlockDesMeta block_des_meta;
  read_info.macro_block_id_ = macro_block_id;
  read_info.offset_ = block_offset;
  read_info.size_ = idx_row_header.get_block_size();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::INDEX_BLOCK_TREE_CURSOR_IO);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  idx_row_header.fill_deserialize_meta(block_des_meta);
  ObArenaAllocator io_allocator("IBTC_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_ISNULL(read_info.buf_ =
      reinterpret_cast<char*>(io_allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else {
    if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
      LOG_WARN("Fail to read micro block from sync io", K(ret));
    } else {
      const char *src_block_buf = macro_handle.get_buffer();
      const int64_t src_buf_size = idx_row_header.get_block_size();
      ObMicroBlockHeader header;
      if (OB_FAIL(header.deserialize_and_check_header(src_block_buf, src_buf_size))) {
        LOG_WARN("fail to deserialize_and_check_header", K(ret), KP(src_block_buf), K(src_buf_size));
      } else if (ObStoreFormat::is_row_store_type_with_cs_encoding(static_cast<ObRowStoreType>(header.row_store_type_))) {
        if (OB_FAIL(macro_reader.decrypt_and_full_transform_data(
            header, block_des_meta, src_block_buf, src_buf_size,
            curr_path_item_->block_data_.get_buf(), curr_path_item_->block_data_.get_buf_size(),
            cursor_path_.get_allocator()))) {
          LOG_WARN("fail to decrypt_and_full_transform_data", K(ret), K(header), K(block_des_meta));
        } else {
          curr_path_item_->is_block_allocated_ = true;
        }
      } else { // not cs_encoding
        bool is_compressed = false;
        if (OB_FAIL(macro_reader.do_decrypt_and_decompress_data(
            header, block_des_meta,
            src_block_buf, src_buf_size,
            curr_path_item_->block_data_.get_buf(),
            curr_path_item_->block_data_.get_buf_size(),
            is_compressed,
            true, /* need deep copy */
            cursor_path_.get_allocator()))) {
          LOG_WARN("Fail to decrypt and decompress data", K(ret));
        } else {
          curr_path_item_->is_block_allocated_ = true;
        }
      }
    }
  }
  macro_handle.reset();
  io_allocator.reset();
  return ret;
}


int ObIndexBlockTreeCursor::get_transformed_data_header(
    const ObIndexBlockTreePathItem &path_item,
    const ObIndexBlockDataHeader *&idx_data_header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(idx_data_header = reinterpret_cast<const ObIndexBlockDataHeader *>(
      path_item.block_data_.get_extra_buf()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null pointer to micro block data", K(ret), KPC(curr_path_item_));
  } else if (OB_UNLIKELY(!idx_data_header->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid extra index data header", K(ret), KPC(idx_data_header));
  }
  return ret;
}

int ObIndexBlockTreeCursor::read_next_level_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  idx_row_parser_.reset();
  if (curr_path_item_->is_block_transformed_) {
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    const char *idx_data_buf = nullptr;
    int64_t idx_data_len = 0;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret), KPC(curr_path_item_));
    } else if (OB_UNLIKELY(row_idx >= idx_data_header->row_cnt_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid row idx", K(ret), K(row_idx), KPC(idx_data_header));
    } else if (OB_FAIL(idx_data_header->get_index_data(row_idx, idx_data_buf, idx_data_len))) {
      LOG_WARN("Fail to get index data", K(ret), KPC(idx_data_header), K(row_idx));
    } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf, idx_data_len))) {
      LOG_WARN("Fail to init index row parser with transformed index data",
          K(ret), K(row_idx), KPC(idx_data_header));
    } else if (nullptr != idx_data_header->rowkey_vector_) {
      if (OB_FAIL(idx_data_header->rowkey_vector_->get_rowkey(row_idx, vector_endkey_))) {
        LOG_WARN("Failed to get rowkey", K(ret));
      }
    }
  } else if (FALSE_IT(row_.reuse())) {
  } else if (OB_FAIL(reader_->get_row(row_idx, row_))) {
    LOG_WARN("Fail to read row", K(ret), K(row_idx), KPC(curr_path_item_));
  } else if (OB_FAIL(idx_row_parser_.init(rowkey_column_cnt_, row_))) {
    LOG_WARN("Fail to init index block row parser", K(ret), K_(rowkey_column_cnt), K_(row));
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_next_level_row_cnt(int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  if (curr_path_item_->is_block_transformed_) {
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret), KPC(curr_path_item_));
    } else {
      row_cnt = idx_data_header->row_cnt_;
    }
  } else if (OB_FAIL(reader_->get_row_count(row_cnt))) {
    LOG_WARN("Fail to get row count", K(ret));
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_micro_block_infos(
    const ObDatumRange &range,
    ObIArray<ObMicroIndexInfo> &micro_index_infos)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = 0;
  int64_t end_idx = 0;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  ObMicroIndexInfo index_info;
  if (OB_FAIL(locate_range_in_curr_block(range, begin_idx, end_idx))) {
    LOG_WARN("Fail to locate range in micro block", K(ret), K(range), K(begin_idx), K(end_idx));
  }
  for (int64_t i = begin_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
    if (OB_FAIL(read_next_level_row(i))) {
      LOG_WARN("Fail to read next level row", K(ret), K(i));
    } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
      LOG_WARN("Fail to get index row header", K(ret));
    } else if (OB_ISNULL(idx_row_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index row header", K(ret));
    } else {
      index_info.row_header_ = idx_row_header;
      index_info.parent_macro_id_ = curr_path_item_->macro_block_id_;
      if (!idx_row_header->is_data_index() || idx_row_header->is_major_node()) {
      } else if (OB_FAIL(idx_row_parser_.get_minor_meta(index_info.minor_meta_info_))) {
        LOG_WARN("Fail to get minor meta info", K(ret));
      }

      if (OB_SUCC(ret) && idx_row_header->is_pre_aggregated()) {
        if (OB_FAIL(idx_row_parser_.get_agg_row(index_info.agg_row_buf_, index_info.agg_buf_size_))) {
          LOG_WARN("Fail to get aggregated row", K(ret), K(index_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(micro_index_infos.push_back(index_info))) {
        LOG_WARN("Fail to push index micro block info into array", K(ret), K(index_info));
      }
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::get_micro_block_endkeys(
    const ObDatumRange &range,
    ObArenaAllocator &endkey_allocator,
    ObIArray<ObMicroIndexInfo> &micro_index_infos,
    ObIArray<ObDatumRowkey> &end_keys)
{
  int ret = OB_SUCCESS;
  int64_t orig_end_key_cnt = end_keys.count();
  int64_t begin_idx = 0;
  int64_t end_idx = 0;
  if (OB_FAIL(locate_range_in_curr_block(range, begin_idx, end_idx))) {
    LOG_WARN("Fail to locate range in micro block", K(ret), K(range), K(begin_idx), K(end_idx));
  } else if (curr_path_item_->is_block_transformed_) {
    const ObIndexBlockDataHeader *idx_data_header = nullptr;
    if (OB_FAIL(get_transformed_data_header(*curr_path_item_, idx_data_header))) {
      LOG_WARN("Fail to get transformed data header", K(ret), KPC(curr_path_item_));
    } else if (OB_UNLIKELY(begin_idx < 0 || end_idx >= idx_data_header->row_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid range idx located", K(ret), K(begin_idx), K(end_idx));
    } else {
      ObDatumRowkey rowkey, endkey;
      for (int64_t i = begin_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
        if (OB_FAIL(idx_data_header->rowkey_vector_->get_rowkey(i, vector_endkey_))) {
          LOG_WARN("Failed to get rowkey", K(ret));
        } else if (OB_FAIL(rowkey.assign(vector_endkey_.datums_, rowkey_column_cnt_))) {
          STORAGE_LOG(WARN, "Failed to assign datum rowkey", K(ret), K_(vector_endkey), K_(rowkey_column_cnt));
        } else if (OB_FAIL(rowkey.deep_copy(endkey, endkey_allocator))) {
          STORAGE_LOG(WARN, "Failed to deep copy endkey", K(ret), K(rowkey));
        } else if (OB_FAIL(end_keys.push_back(endkey))) {
          LOG_WARN("Fail to push rowkey into array", K(ret));
        }
      }
    }
  } else {
    for (int64_t i = begin_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
      ObDatumRowkey rowkey, endkey;
      row_.reuse();
      if (OB_FAIL(reader_->get_row(i, row_))) {
        LOG_WARN("Fail to get row from micro block", K(ret), K(i));
      } else if (OB_FAIL(rowkey.assign(row_.storage_datums_, rowkey_column_cnt_))) {
        STORAGE_LOG(WARN, "Failed to assign datum rowkey", K(ret), K_(row), K_(rowkey_column_cnt));
      } else if (OB_FAIL(rowkey.deep_copy(endkey, endkey_allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy endkey", K(ret), K(rowkey));
      } else if (OB_FAIL(end_keys.push_back(endkey))) {
        LOG_WARN("Fail to push end key into array", K(ret), K(rowkey));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = begin_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
      int64_t it = i - begin_idx + orig_end_key_cnt;
      if (OB_UNLIKELY(it > end_keys.count() || it > micro_index_infos.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected endkey index overflow", K(ret),
            K(it), K(end_keys.count()), K(micro_index_infos.count()));
      } else {
        micro_index_infos.at(it).endkey_.set_compact_rowkey(&end_keys.at(it));
        if (OB_UNLIKELY(!micro_index_infos.at(it).is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid micro index info", K(ret), K(it), K(micro_index_infos.at(it)));
        }
      }
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::check_reach_target_depth(
  const MoveDepth target_depth,
  bool &reach_target_depth)
{
  int ret = OB_SUCCESS;
  reach_target_depth = false;
  switch (target_depth) {
    case ROOT: {
      reach_target_depth = cursor_path_.empty();
      break;
    }
    case MACRO:
    case LEAF: {
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
        LOG_WARN("Fail to get index block row header", K(ret));
      } else if (OB_ISNULL(idx_row_header)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null index row header", K(ret));
      } else if (MACRO == target_depth) {
        reach_target_depth = idx_row_header->is_macro_node();
      } else {
        reach_target_depth = idx_row_header->is_data_block();
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("No semantic for depth to reach", K(ret), K(target_depth));
    }
  }
  return ret;
}

int ObIndexBlockTreeCursor::init_curr_endkey(ObDatumRow &row_buf, const int64_t datum_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum_cnt > row_buf.get_capacity())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row buf", K(ret), K(datum_cnt), K(row_buf.get_capacity()));
  } else if (!vector_endkey_.is_valid()) {
    if (OB_FAIL(vector_endkey_.assign(row_buf.storage_datums_, datum_cnt))) {
      LOG_WARN("Failed to assign", K(ret));
    }
  }
  return ret;
}

} // namespace oceanbase
} // namespace oceanbase
