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

#include "ob_index_block_micro_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
namespace compaction
{

ObMacroBlockDataIterator::ObMacroBlockDataIterator()
  : macro_buf_(nullptr), macro_buf_size_(0), range_(),
    micro_block_infos_(nullptr), endkeys_(nullptr),
    cur_micro_cursor_(0), is_inited_(false) {}

ObMacroBlockDataIterator::~ObMacroBlockDataIterator()
{
}

void ObMacroBlockDataIterator::reset()
{
  is_inited_ = false;
  macro_buf_ = nullptr;
  macro_buf_size_ = 0;
  cur_micro_cursor_ = 0;
  range_.reset();
}

int ObMacroBlockDataIterator::init(
    const char *macro_block_buf,
    const int64_t macro_block_buf_size,
    const common::ObIArray<blocksstable::ObMicroIndexInfo> &micro_block_infos,
    const common::ObIArray<ObDatumRowkey> &endkeys,
    const ObDatumRange *range)
{
  int ret = OB_SUCCESS;
  int64_t read_pos = 0;
  ObMacroBlockCommonHeader common_header;
  ObSSTableMacroBlockHeader macro_header;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(macro_block_buf_size <= 0)
      || OB_UNLIKELY(micro_block_infos.count() != endkeys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid init parameter for macro block data iterator",
        K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(micro_block_infos), K(endkeys));
  } else if (OB_FAIL(common_header.deserialize(macro_block_buf, macro_block_buf_size, read_pos))) {
    LOG_ERROR("Fail to deserialize common header", K(ret), K(macro_block_buf_size), K(read_pos));
  } else if (OB_FAIL(common_header.check_integrity())) {
    LOG_ERROR("Invalid common header", K(ret), K(common_header));
  } else if (OB_UNLIKELY(!common_header.is_sstable_data_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Macro block type not supported for data iterator", K(ret));
  } else if (OB_FAIL(macro_header.deserialize(macro_block_buf, macro_block_buf_size, read_pos))) {
    LOG_WARN("fail to deserialize macro block header", K(ret), K(macro_header));
  } else {
    macro_buf_ = macro_block_buf;
    macro_buf_size_ = macro_block_buf_size;
    if (nullptr == range) {
      range_.set_whole_range();
    } else {
      range_ = *range;
    }
    micro_block_infos_ = &micro_block_infos;
    endkeys_ = &endkeys;
    is_inited_ = true;
  }
  return ret;
}

int ObMacroBlockDataIterator::next_micro_block(ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Macro block not loaded, ", K(ret));
  } else if (cur_micro_cursor_ < 0 || cur_micro_cursor_ > micro_block_infos_->count()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong cursor, ", K(ret), K(cur_micro_cursor_));
  } else if (micro_block_infos_->count() == cur_micro_cursor_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!micro_block_infos_->at(cur_micro_cursor_).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    const ObMicroIndexInfo &index_info = micro_block_infos_->at(cur_micro_cursor_);
    LOG_WARN("Unexpected invalid micro block index info",
        K(ret), K(micro_block_infos_->at(cur_micro_cursor_)), K_(cur_micro_cursor));
  } else {
    const ObMicroIndexInfo &micro_block_info = micro_block_infos_->at(cur_micro_cursor_);
    const char *micro_buf = macro_buf_ + micro_block_info.get_block_offset();
    const int64_t micro_buf_size = micro_block_info.get_block_size();
    int64_t pos = 0;
    const char *payload_buf = nullptr;
    int64_t payload_size = 0;
    LOG_DEBUG("next micro block", K(micro_block_info));
    if (OB_FAIL(micro_block.header_.deserialize(micro_buf, micro_buf_size, pos))) {
      LOG_WARN("fail to deserialize record header", K(ret));
    } else if (OB_FAIL(micro_block.header_.check_and_get_record(
        micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
      LOG_ERROR("micro block data is corrupted", K(ret),
        KP(micro_buf), K(micro_buf_size), K(cur_micro_cursor_), K(micro_block_infos_->count()));
    } else {
      ObDatumRange &micro_range = micro_block.range_;
      micro_block.data_.get_buf() = micro_buf;
      micro_block.data_.get_buf_size() = micro_buf_size;
      micro_block.payload_data_.get_buf() = payload_buf;
      micro_block.payload_data_.get_buf_size() = payload_size;
      if (0 == cur_micro_cursor_) {
        micro_range.start_key_ = range_.get_start_key();
      } else {
        micro_range.start_key_ = endkeys_->at(cur_micro_cursor_ - 1);
      }
      micro_range.end_key_ = endkeys_->at(cur_micro_cursor_);
      micro_range.border_flag_.unset_inclusive_start();
      micro_range.border_flag_.set_inclusive_end();
      micro_block_info.copy_lob_out_row_flag();
      micro_block.micro_index_info_ = &micro_block_info;
    }
    ++cur_micro_cursor_;
  }
  return ret;
}

ObIndexBlockMicroIterator::ObIndexBlockMicroIterator()
  : data_iter_(), range_(), micro_block_(),
    macro_handle_(), allocator_("IBMI_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), is_inited_(false) {}

void ObIndexBlockMicroIterator::reset()
{
  data_iter_.reset();
  range_.reset();
  macro_handle_.reset();
  allocator_.reset();
  is_inited_ = false;
}

int ObIndexBlockMicroIterator::init(
    const blocksstable::ObMacroBlockDesc &macro_desc,
    const ObITableReadInfo &table_read_info,
    const common::ObIArray<blocksstable::ObMicroIndexInfo> &micro_block_infos,
    const common::ObIArray<blocksstable::ObDatumRowkey> &endkeys,
    const ObRowStoreType row_store_type,
    const ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  const bool is_normal_cg = sstable->is_normal_cg_sstable();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_UNLIKELY(!table_read_info.is_valid() || !macro_desc.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected read info", K(ret), K(table_read_info), K(macro_desc));
  } else if (is_normal_cg && OB_FAIL(rowkey_helper_.trans_to_cg_range(macro_desc.start_row_offset_, macro_desc.range_))) {
      STORAGE_LOG(WARN, "failed to trans cg range", K(ret), K(macro_desc));
  } else {
    range_ = is_normal_cg ? rowkey_helper_.get_result_range() : macro_desc.range_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_range_include_rowkey_array(range_, endkeys, table_read_info.get_datum_utils()))) {
    STORAGE_LOG(WARN, "Failed to check range include rowkey", K(ret), K(range_), K(endkeys));
  } else {
    ObMacroBlockReadInfo read_info;
    read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    read_info.macro_block_id_ = macro_desc.macro_block_id_;
    read_info.offset_ = sstable->get_macro_offset();
    read_info.size_ = sstable->get_macro_read_size();
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    read_info.io_desc_.set_sys_module_id(ObIOModule::INDEX_BLOCK_MICRO_ITER_IO);
    if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(allocator_.alloc(read_info.size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret));
    } else if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle_))) {
      LOG_WARN("async read block failed, ", K(ret), K(read_info), K(macro_desc));
    } else if (OB_FAIL(macro_handle_.wait())) {
      LOG_WARN("io wait failed", K(ret), K(macro_desc), K(read_info));
    } else if (OB_ISNULL(macro_handle_.get_buffer())
        || OB_UNLIKELY(macro_handle_.get_data_size() != read_info.size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf is null or buf size is too small, ",
          K(ret), K(macro_desc), KP(macro_handle_.get_buffer()),
          K(macro_handle_.get_data_size()), K(read_info.size_));
    } else if (OB_FAIL(data_iter_.init(
        macro_handle_.get_buffer(),
        macro_handle_.get_data_size(),
        micro_block_infos,
        endkeys,
        &range_))){
      LOG_WARN("Fail to init data iterator, ", K(ret), K(macro_desc),
          KP(macro_handle_.get_buffer()), K(macro_handle_.get_data_size()), K(range_));
    } else {
      micro_block_.read_info_ = &table_read_info;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIndexBlockMicroIterator::check_range_include_rowkey_array(
      const blocksstable::ObDatumRange range,
      const common::ObIArray<blocksstable::ObDatumRowkey> &endkeys,
      const blocksstable::ObStorageDatumUtils &datum_utils)
{
  int ret = OB_SUCCESS;

  if (endkeys.empty()) {
  } else {
    int cmp_ret = 0;
    const blocksstable::ObDatumRowkey &array_start_key = endkeys.at(0);
    const blocksstable::ObDatumRowkey &array_end_key = endkeys.at(endkeys.count() - 1);
    if (OB_FAIL(range.get_start_key().compare(array_start_key, datum_utils, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare start key", K(ret), K(range), K(array_start_key));
    } else if (cmp_ret > 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected range start key", K(ret), K(range), K(array_start_key));
    } else if (OB_FAIL(range.get_end_key().compare(array_end_key, datum_utils, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare start key", K(ret), K(range), K(array_end_key));
    } else if (cmp_ret < 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected range end key", K(ret), K(range), K(array_end_key));
    }
  }

  return ret;
}

int ObIndexBlockMicroIterator::next(const blocksstable::ObMicroBlock *&micro_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_FAIL(data_iter_.next_micro_block(micro_block_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("Fail to get next micro block data", K(ret));
    }
  } else {
    micro_block = &micro_block_;
    LOG_DEBUG("Iterate micro block", KPC(micro_block));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
