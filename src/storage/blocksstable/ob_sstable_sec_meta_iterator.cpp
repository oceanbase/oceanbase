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

#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_param.h"
#include "ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
namespace blocksstable
{

ObSSTableSecMetaIterator::ObSSTableSecMetaIterator()
  : tenant_id_(OB_INVALID_TENANT_ID), rowkey_read_info_(nullptr), sstable_meta_hdl_(),
    prefetch_flag_(), idx_cursor_(), macro_reader_(), block_cache_(nullptr),
    micro_reader_(nullptr), micro_reader_helper_(), block_meta_tree_(nullptr),
    query_range_(nullptr), start_bound_micro_block_(), end_bound_micro_block_(),
    micro_handles_(), row_(), curr_handle_idx_(0), prefetch_handle_idx_(0), prev_block_row_cnt_(0),
    curr_block_start_idx_(0), curr_block_end_idx_(0), curr_block_idx_(0), step_cnt_(0),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE), is_reverse_scan_(false), is_prefetch_end_(false),
    is_range_end_key_multi_version_(false), is_inited_(false) {}

void ObSSTableSecMetaIterator::reset()
{
  rowkey_read_info_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sstable_meta_hdl_.reset();
  prefetch_flag_.reset();
  idx_cursor_.reset();
  block_cache_ = nullptr;
  micro_reader_ = nullptr;
  micro_reader_helper_.reset();
  block_meta_tree_ = nullptr;
  row_.reset();
  query_range_ = nullptr;
  curr_handle_idx_ = 0;
  prefetch_handle_idx_ = 0;
  prev_block_row_cnt_ = 0;
  curr_block_start_idx_ = 0;
  curr_block_end_idx_ = 0;
  curr_block_idx_ = 0;
  step_cnt_ = 0;
  row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
  is_reverse_scan_ = false;
  is_prefetch_end_ = false;
  is_range_end_key_multi_version_ = false;
  is_inited_ = false;
}

int ObSSTableSecMetaIterator::open(
    const ObDatumRange &query_range,
    const ObMacroBlockMetaType meta_type,
    const ObSSTable &sstable,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan,
    const int64_t sample_step)
{
  int ret = OB_SUCCESS;
  bool is_meta_root = false;
  bool is_ddl_mem_sstable = false;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Fail to open sstable secondary meta iterator", K(ret));
  } else if (OB_UNLIKELY(!query_range.is_valid()
      || !sstable.is_valid()
      || meta_type == ObMacroBlockMetaType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open sstable secondary meta iterator",
        K(ret), K(query_range), K(sstable), K(meta_type));
  } else if (sstable.is_empty()) {
    set_iter_end();
    is_inited_ = true;
    LOG_DEBUG("Empty sstable secondary meta", K(ret), K(meta_type), K(sstable));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_hdl_))) {
    LOG_WARN("get meta handle fail", K(ret), K(sstable));
  } else {
    rowkey_read_info_ = &rowkey_read_info;
    tenant_id_ = MTL_ID();
    prefetch_flag_.set_not_use_block_cache();
    query_range_ = &query_range;
    is_reverse_scan_ = is_reverse_scan;
    block_cache_ = &ObStorageCacheSuite::get_instance().get_block_cache();
    is_meta_root = sstable_meta_hdl_.get_sstable_meta().get_macro_info().is_meta_root();
  }

  if (OB_FAIL(ret) || is_prefetch_end_) {
  } else if (sstable.is_ddl_mem_sstable()) {
    is_ddl_mem_sstable = true;
    const ObMicroBlockData &root_block = sstable_meta_hdl_.get_sstable_meta().get_root_info().get_block_data();
    if (ObMicroBlockData::DDL_BLOCK_TREE != root_block.type_ || nullptr == root_block.buf_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block type is not ddl block tree", K(ret), K(root_block));
    } else {
      block_meta_tree_ = reinterpret_cast<ObBlockMetaTree *>(const_cast<char *>(root_block.buf_));
      if (OB_FAIL(block_meta_tree_->locate_range(query_range,
                                                 rowkey_read_info.get_datum_utils(),
                                                 true, //is_left_border
                                                 true, //is_right_border,
                                                 curr_block_start_idx_,
                                                 curr_block_end_idx_))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("locate range failed", K(ret), K(query_range));
        } else {
          curr_block_idx_ = curr_block_end_idx_ + 1;
          ret = OB_SUCCESS; // return OB_ITER_END on get_next() for get
        }
      } else {
        const int64_t step = max(1, sample_step);
        step_cnt_ = !is_reverse_scan ? step : -step;
        curr_block_idx_ = !is_reverse_scan ? curr_block_start_idx_ : curr_block_end_idx_;
        is_inited_ = true;
      }
    }
  } else if (OB_FAIL(idx_cursor_.init(sstable, allocator, rowkey_read_info_,
      get_index_tree_type_map()[meta_type]))) {
    LOG_WARN("Fail to init index block tree cursor", K(ret), K(meta_type));
  } else if (OB_FAIL(micro_reader_helper_.init(allocator))) {
    LOG_WARN("Fail to init micro reader helper", K(ret), K(sstable));
  } else {
    const int64_t schema_rowkey_cnt = rowkey_read_info.get_schema_rowkey_count();

    is_range_end_key_multi_version_ =
        schema_rowkey_cnt < query_range.get_end_key().get_datum_cnt();
  }

  const int64_t request_col_cnt = rowkey_read_info.get_schema_rowkey_count()
           + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() + 1;

  if (OB_SUCC(ret) && !is_prefetch_end_ && !is_meta_root && !is_ddl_mem_sstable) {
    bool start_key_beyond_range = false;
    bool end_key_beyond_range = false;
    if (is_reverse_scan) {
      if (OB_FAIL(locate_bound_micro_block(
          query_range.get_start_key(),
          query_range.get_border_flag().inclusive_start(),
          start_bound_micro_block_,
          start_key_beyond_range))) {
        LOG_WARN("Fail to locate start bound micro block", K(ret));
      } else if (OB_FAIL(locate_bound_micro_block(
          query_range.get_end_key(),
          (!query_range.get_border_flag().inclusive_end() || is_range_end_key_multi_version_),
          end_bound_micro_block_,
          end_key_beyond_range))) {
        LOG_WARN("Fail to locate end bound micro block", K(ret));
      }
    } else {
      if (OB_FAIL(locate_bound_micro_block(
          query_range.get_end_key(),
          (!query_range.get_border_flag().inclusive_end() || is_range_end_key_multi_version_),
          end_bound_micro_block_,
          end_key_beyond_range))) {
        LOG_WARN("Fail to locate end bound micro block", K(ret));
      } else if (OB_FAIL(locate_bound_micro_block(
          query_range.get_start_key(),
          query_range.get_border_flag().inclusive_start(),
          start_bound_micro_block_,
          start_key_beyond_range))) {
        LOG_WARN("Fail to locate start bound micro block", K(ret));
      }
    }

    if (OB_FAIL(ret) || is_prefetch_end_) {
    } else if (OB_UNLIKELY(start_key_beyond_range)) {
      set_iter_end();
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret) || is_ddl_mem_sstable) {
    // do nothing
  } else if (is_prefetch_end_) {
    is_inited_ = true;
  } else if (!is_meta_root && OB_FAIL(prefetch_micro_block(1 /* fetch first micro block */))) {
    LOG_WARN("Fail to prefetch next micro block", K(ret), K_(is_prefetch_end));
  } else if (OB_FAIL(row_.init(allocator, request_col_cnt))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else {
    if (sample_step != 0) {
      // is sample scan
      const int64_t start_offset = sample_step > 1 ? (sample_step / 2 - 1) : 0;
      step_cnt_ = is_reverse_scan ? (-sample_step) : sample_step;
      curr_block_idx_ = is_reverse_scan ? (-1 - start_offset) : start_offset;
    } else {
      step_cnt_ = is_reverse_scan ? -1 : 1;
      curr_block_idx_ = is_reverse_scan ? -1 : 0;
    }
    curr_block_start_idx_ = 1;
    curr_block_end_idx_ = -1;
    is_inited_ = true;
    LOG_DEBUG("Open secondary meta iterator", K(ret), K(meta_type), K(is_reverse_scan),
        K(sample_step), K_(step_cnt), K_(curr_block_idx), K_(tenant_id), KPC_(query_range));
  }
  return ret;
}


int ObSSTableSecMetaIterator::get_next(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  row_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Secondary meta iterator not inited", K(ret));
  } else if (nullptr != block_meta_tree_) {
    if (!is_target_row_in_curr_block()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(block_meta_tree_->get_macro_block_meta(curr_block_idx_, macro_meta))) {
      LOG_WARN("get next macro block meta failed", K(ret), K(curr_block_idx_));
    } else {
      curr_block_idx_ += step_cnt_;
    }
  } else {
    while (OB_SUCC(ret) && !is_target_row_in_curr_block()) {
      if (is_prefetch_end_ && is_handle_buffer_empty()) {
        ret = OB_ITER_END;
      } else {
        const bool is_data_block = sstable_meta_hdl_.get_sstable_meta().get_macro_info().is_meta_root();
        if (!is_data_block && OB_FAIL(open_next_micro_block(macro_id))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to open next micro block", K(ret));
          }
        } else if (is_data_block && OB_FAIL(open_meta_root_block())) {
          LOG_WARN("Fail to open data root block", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(micro_reader_->get_row(curr_block_idx_, row_))) {
      LOG_WARN("Fail to get secondary meta row from block", K(ret));
    } else if (OB_FAIL(macro_meta.parse_row(row_))) {
      LOG_WARN("Fail to parse macro meta", K(ret));
    } else {
      const ObSSTableMacroInfo &macro_info = sstable_meta_hdl_.get_sstable_meta().get_macro_info();
      if (!macro_info.is_meta_root() && 0 == macro_info.get_other_block_count()) {
        // this means macro meta root block is larger than 16KB but read from the end of data block
        // So the macro id parsed from macro meta is empty, which actually should be same to the
        // data block id read in open_next_micro_block
        macro_meta.val_.macro_id_ = macro_id;
      }
      curr_block_idx_ += step_cnt_;
      macro_meta.nested_size_ = macro_info.get_nested_size();
      macro_meta.nested_offset_ = macro_info.get_nested_offset();
    }
  }
  return ret;
}


void ObSSTableSecMetaIterator::set_iter_end()
{
  is_prefetch_end_ = true;
  curr_block_start_idx_ = 1;
  curr_block_end_idx_ = -1;
  curr_block_idx_ = 0;
  prefetch_handle_idx_ = 0;
  curr_handle_idx_ = 0;
}

int ObSSTableSecMetaIterator::locate_bound_micro_block(
    const ObDatumRowkey &rowkey,
    const bool lower_bound,
    ObMicroBlockId &bound_block,
    bool &is_beyond_range)
{
  int ret = OB_SUCCESS;
  is_beyond_range = false;
  const ObIndexBlockRowHeader *idx_header = nullptr;
  MacroBlockId macro_id;
  ObLogicMacroBlockId logic_id;
  bool equal = false;
  if (OB_FAIL(idx_cursor_.pull_up_to_root())) {
    LOG_WARN("Fail to pull up tree cursor back to root", K(ret));
  } else if (OB_FAIL(idx_cursor_.drill_down(
      rowkey,
      ObIndexBlockTreeCursor::LEAF,
      lower_bound,
      equal,
      is_beyond_range))) {
    LOG_WARN("Fail to locate micro block address in index tree", K(ret));
  } else if (OB_FAIL(idx_cursor_.get_idx_row_header(idx_header))) {
    LOG_WARN("Fail to get index block row header", K(ret));
  } else {
    bound_block.macro_id_ = idx_header->get_macro_id();
    bound_block.offset_ = idx_header->get_block_offset();
    bound_block.size_ = idx_header->get_block_size();
  }
  return ret;
}

int ObSSTableSecMetaIterator::open_next_micro_block(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  int64_t row_cnt = 0;
  int64_t begin_idx = 0;
  int64_t end_idx = 0;
  ObMicroBlockData micro_data;
  ObMicroBlockDataHandle &micro_handle = micro_handles_[curr_handle_idx_ % HANDLE_BUFFER_SIZE];
  if (OB_FAIL(prefetch_micro_block(HANDLE_BUFFER_SIZE - handle_buffer_count()))) {
    LOG_WARN("Fail to prefetch micro blocks", K(ret), K(handle_buffer_count()));
  } else if (OB_FAIL(micro_handle.get_data_block_data(macro_reader_, micro_data))) {
    LOG_WARN("Fail to get micro block data", K(ret), K_(curr_handle_idx), K(micro_handle));
  } else if (OB_UNLIKELY(!micro_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid micro block data", K(ret), K(micro_data));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(micro_data.get_store_type(), micro_reader_))) {
    LOG_WARN("fail to get micro block reader", K(ret), K(micro_data.get_store_type()));
  } else if (OB_FAIL(micro_reader_->init(micro_data, &(rowkey_read_info_->get_datum_utils())))) {
    LOG_WARN("Fail to init micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader_->get_row_count(row_cnt))) {
    LOG_WARN("Fail to get end index", K(ret));
  } else {
    end_idx = row_cnt;
    macro_id = micro_handle.macro_block_id_;
    ObMicroBlockId block_id(
        micro_handle.macro_block_id_,
        micro_handle.micro_info_.offset_,
        micro_handle.micro_info_.size_);
    const bool is_start_bound = block_id == start_bound_micro_block_;
    const bool is_end_bound = block_id == end_bound_micro_block_;
    const bool is_index_scan = true;
    if (!is_start_bound && !is_end_bound) {
      --end_idx;
      // full scan
    } else if (OB_FAIL(micro_reader_->locate_range(
        *query_range_,
        is_start_bound,
        is_end_bound,
        begin_idx,
        end_idx,
        is_index_scan))) {
      LOG_WARN("Fail to locate range", K(ret), KPC(query_range_),K(is_start_bound), K(is_end_bound),
          K_(start_bound_micro_block), K_(end_bound_micro_block));
    }
    LOG_DEBUG("Open next micro block", K(ret), K(is_start_bound), K(is_end_bound),
        K(begin_idx), K(end_idx), K_(curr_block_idx), K(is_index_scan), K(block_id));
  }

  if (OB_SUCC(ret) && OB_FAIL(adjust_index(begin_idx, end_idx, row_cnt))) {
    LOG_WARN("fail to move index", K(ret));
  } else {
    ++curr_handle_idx_;
  }

  return ret;
}

int ObSSTableSecMetaIterator::open_meta_root_block()
{
  int ret = OB_SUCCESS;
  const ObMicroBlockData &micro_data = sstable_meta_hdl_.get_sstable_meta().get_macro_info().get_macro_meta_data();
  int64_t row_cnt = 0;
  int64_t begin_idx = 0;
  int64_t end_idx = 0;
  if (OB_UNLIKELY(!micro_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid micro block data", K(ret), K(micro_data));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(micro_data.get_store_type(), micro_reader_))) {
    LOG_WARN("fail to get micro block reader", K(ret), K(micro_data.get_store_type()));
  } else if (OB_FAIL(micro_reader_->init(micro_data, &(rowkey_read_info_->get_datum_utils())))) {
    LOG_WARN("Fail to init micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader_->get_row_count(row_cnt))) {
    LOG_WARN("Fail to get end index", K(ret));
  } else {
    end_idx = row_cnt;
    const bool is_index_scan = true;
    if (OB_FAIL(micro_reader_->locate_range(
        *query_range_,
        true,
        true,
        begin_idx,
        end_idx,
        is_index_scan))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        set_iter_end();
        ret = OB_ITER_END;
        FLOG_INFO("this special sstable only locates range during iteration, so beyong range err should be transformed into iter end", K(ret));
      } else {
        LOG_WARN("Fail to locate range", K(ret), KPC(query_range_));
      }
    }
    LOG_DEBUG("Open next micro block", K(ret), K(begin_idx), K(end_idx), K(is_index_scan));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(adjust_index(begin_idx, end_idx, row_cnt))) {
    LOG_WARN("fail to move index", K(ret));
  } else {
    is_prefetch_end_ = true;
  }
  return ret;
}

int ObSSTableSecMetaIterator::adjust_index(const int64_t begin_idx, const int64_t end_idx, const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t curr_block_row_cnt = end_idx - begin_idx + 1;
  if (is_reverse_scan_) {
    if (OB_UNLIKELY(curr_block_idx_ >= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid current block index on reverse scan", K(ret), K_(curr_block_idx),
          K_(curr_block_start_idx), K_(curr_block_end_idx), K(begin_idx), K(end_idx));
    } else if (curr_block_idx_ + curr_block_row_cnt >= 0) {
      // next row in this block
      curr_block_idx_ = end_idx + curr_block_idx_ + 1;
    } else {
      curr_block_idx_ += curr_block_row_cnt;
    }
  } else {
    if (OB_UNLIKELY(curr_block_idx_ < prev_block_row_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid current block index on sequential scan", K(ret), K_(curr_block_idx),
          K_(curr_block_start_idx), K_(curr_block_end_idx), K(begin_idx), K(end_idx), K_(prev_block_row_cnt));
    } else if (curr_block_idx_ - prev_block_row_cnt_ < row_cnt) {
      // First block in scan : begin_idx may larger than 0, update curr_block_idx_
      // Non-first block : next row in this block
      curr_block_idx_ = begin_idx + (curr_block_idx_ - prev_block_row_cnt_);
    } else {
      curr_block_idx_ -= prev_block_row_cnt_;
    }
  }
  if (OB_SUCC(ret)) {
    prev_block_row_cnt_ = row_cnt;
    curr_block_start_idx_ = begin_idx;
    curr_block_end_idx_ = end_idx;
  }
  return ret;
}

int ObSSTableSecMetaIterator::prefetch_micro_block(int64_t prefetch_depth)
{
  int ret = OB_SUCCESS;
  if (is_prefetch_end_) {
    //prefetch end
  } else if (OB_UNLIKELY(prefetch_depth + handle_buffer_count() > HANDLE_BUFFER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Prefetch depth larger than available buffer", K(ret));
  } else {
    int64_t prefetch_count = 0;
    const ObIndexBlockRowHeader *idx_row_header = nullptr;
    ObLogicMacroBlockId logic_id;
    ObMicroBlockId micro_block_id;
    while (OB_SUCC(ret) && prefetch_count < prefetch_depth && !is_prefetch_end_) {
      if (OB_FAIL(idx_cursor_.get_idx_row_header(idx_row_header))) {
        LOG_WARN("Fail to get index block row header", K(ret));
      } else if (OB_UNLIKELY(!idx_row_header->is_data_block())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected non-leaf node when prefetch sec meta micro block", K(ret));
      } else {
        micro_block_id.macro_id_ = idx_row_header->get_macro_id();
        micro_block_id.offset_ = idx_row_header->get_block_offset();
        micro_block_id.size_ = idx_row_header->get_block_size();
        is_prefetch_end_ = is_reverse_scan_
            ? start_bound_micro_block_ == micro_block_id
            : end_bound_micro_block_ == micro_block_id;

        LOG_DEBUG("Prefetch next micro block",
            K(ret), K_(prefetch_handle_idx), K(micro_block_id), KPC(idx_row_header));
        if (OB_FAIL(get_micro_block(
            micro_block_id.macro_id_,
            *idx_row_header,
            micro_handles_[prefetch_handle_idx_ % HANDLE_BUFFER_SIZE]))) {
          LOG_WARN("Fail to prefetch next micro block",
              K(ret), K(micro_block_id), KPC(idx_row_header), K_(prefetch_handle_idx));
        } else {
          ++prefetch_handle_idx_;
          ++prefetch_count;
          if (!is_prefetch_end_ && OB_FAIL(idx_cursor_.move_forward(is_reverse_scan_))) {
            LOG_WARN("Index tree cursor fail to move forward", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// TODO: Always async io for now, opt with cache
int ObSSTableSecMetaIterator::get_micro_block(
    const MacroBlockId &macro_id,
    const ObIndexBlockRowHeader &idx_row_header,
    ObMicroBlockDataHandle &data_handle)
{
  int ret = OB_SUCCESS;
  data_handle.reset();
  ObTabletHandle tablet_handle;
  const int64_t nested_offset = sstable_meta_hdl_.get_sstable_meta().get_macro_info().get_nested_offset();
  if (OB_UNLIKELY(!macro_id.is_valid() || !idx_row_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid parameters to locate micro block", K(ret), K(macro_id), K(idx_row_header));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(block_cache_->get_cache_block(
      tenant_id_,
      macro_id,
      idx_row_header.get_block_offset() + nested_offset,
      idx_row_header.get_block_size(),
      data_handle.cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("Fail to get micro block handle from cache", K(ret), K(idx_row_header));
    } else {
      // Cache miss, async IO
      ObMicroIndexInfo idx_info;
      idx_info.row_header_ = &idx_row_header;
      idx_info.nested_offset_ = nested_offset;
      // TODO: @saitong.zst not safe here, remove tablet_handle from SecMeta prefetch interface, disable cache decoders
      if (OB_FAIL(block_cache_->prefetch(
          tenant_id_,
          macro_id,
          idx_info,
          prefetch_flag_,
          data_handle.io_handle_))) {
        LOG_WARN("Fail to prefetch with async io", K(ret));
      } else {
        data_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      }
    }
  } else {
    data_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
  }

  if (OB_SUCC(ret)) {
    data_handle.macro_block_id_ = macro_id;
    data_handle.micro_info_.offset_ = idx_row_header.get_block_offset() + nested_offset;
    data_handle.micro_info_.size_ = idx_row_header.get_block_size();
    const bool deep_copy_key = true;
    if (OB_FAIL(idx_row_header.fill_micro_des_meta(deep_copy_key, data_handle.des_meta_))) {
      LOG_WARN("Fail to fill deserialize meta", K(ret));
    }
  }
  return ret;
}

} // blocksstable
} // oceanbase
