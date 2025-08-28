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

#include "ob_macro_meta_temp_store.h"

namespace oceanbase
{
namespace blocksstable
{

ObMacroMetaTempStore::StoreItem::StoreItem()
  : macro_id_(),
    macro_header_(),
    index_block_buf_(nullptr),
    index_block_buf_size_(0),
    macro_meta_block_buf_(nullptr),
    macro_meta_block_size_(0)
{}

bool ObMacroMetaTempStore::StoreItem::is_valid() const
{
  return macro_id_.is_valid()
      && macro_header_.is_valid()
      && nullptr != index_block_buf_
      && 0 != index_block_buf_size_
      && nullptr != macro_meta_block_buf_
      && 0 != macro_meta_block_size_;
}

void ObMacroMetaTempStore::StoreItem::reset()
{
  macro_id_.reset();
  macro_header_.reset();
  index_block_buf_ = nullptr;
  index_block_buf_size_ = 0;
  macro_meta_block_buf_ = nullptr;
  macro_meta_block_size_ = 0;
}

OB_DEF_SERIALIZE(ObMacroMetaTempStore::StoreItem)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid store item", K(ret), K_(macro_id), K_(macro_header), KP_(index_block_buf),
        K_(index_block_buf_size), KP_(macro_meta_block_buf), K_(macro_meta_block_size));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        macro_id_,
        macro_header_,
        index_block_buf_size_,
        macro_meta_block_size_);
  }
  
  if (OB_SUCC(ret)) {
    MEMCPY(buf + pos, index_block_buf_, index_block_buf_size_);
    pos += index_block_buf_size_;
    MEMCPY(buf + pos, macro_meta_block_buf_, macro_meta_block_size_);
    pos += macro_meta_block_size_;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObMacroMetaTempStore::StoreItem)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      macro_id_,
      macro_header_,
      index_block_buf_size_,
      macro_meta_block_size_);
  if (OB_SUCC(ret)) {
    index_block_buf_ = buf + pos;
    pos += index_block_buf_size_;
    macro_meta_block_buf_ = buf + pos;
    pos += macro_meta_block_size_;
    if (OB_UNLIKELY(!is_valid() || pos > data_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid store item", K(ret), K_(macro_id), K_(macro_header), KP_(index_block_buf),
          K_(index_block_buf_size), KP_(macro_meta_block_buf), K_(macro_meta_block_size), K(data_len));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMacroMetaTempStore::StoreItem)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      macro_id_,
      macro_header_,
      index_block_buf_size_,
      macro_meta_block_size_);
  len += index_block_buf_size_;
  len += macro_meta_block_size_;
  return len;
}

ObMacroMetaTempStore::ObMacroMetaTempStore()
  : count_(0),
    io_(),
    io_handle_(),
    buffer_("MacroTmpStore"),
    item_size_arr_(),
    is_inited_(false)
{
  item_size_arr_.set_attr(ObMemAttr(MTL_ID(), "MacroTmpIdxArr"));
}

int ObMacroMetaTempStore::init(const int64_t dir_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(MTL_ID(), io_.fd_, io_.dir_id_))) {
    LOG_WARN("open tmp file failed", K(ret));
  } else {
    count_ = 0;
    is_inited_ = true;
  }

  return ret;
}

void ObMacroMetaTempStore::reset()
{
  int ret = OB_SUCCESS;
  if (io_.fd_ > 0) {
    io_handle_.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(MTL_ID(), io_.fd_))) {
      LOG_WARN("remove tmp file failed", K(ret), K_(io));
    } else {
      LOG_INFO("remove tmp file success", K(ret), K_(io));
    }
    io_.reset();
  }
  buffer_.reset();
  item_size_arr_.reset();
  is_inited_ = false;
}

int ObMacroMetaTempStore::append(
    const char *block_buf,
    const int64_t block_size,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  buffer_.reuse();
  StoreItem item;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(block_buf) || OB_UNLIKELY(block_size < 0 || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_size), KP(block_buf), K(macro_id));
  } else if (io_handle_.is_valid() && OB_FAIL(io_handle_.wait())) {
    LOG_WARN("failed to wait previous write", K(ret));
  } else if (OB_FAIL(get_macro_block_header(block_buf, block_size, item.macro_header_))) {
    LOG_WARN("failed to get macro block header", K(ret));
  } else {
    item.macro_id_ = macro_id;
    item.index_block_buf_ = block_buf + item.macro_header_.fixed_header_.idx_block_offset_;
    item.index_block_buf_size_ = item.macro_header_.fixed_header_.idx_block_size_;
    item.macro_meta_block_buf_ = block_buf + item.macro_header_.fixed_header_.meta_block_offset_;
    item.macro_meta_block_size_ = item.macro_header_.fixed_header_.meta_block_size_;
    int64_t serialize_size = item.get_serialize_size();
    if (OB_FAIL(buffer_.write_serialize(item))) {
      LOG_WARN("failed to serialize item to buffer", K(ret));
    } else if (OB_UNLIKELY(buffer_.pos() != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected serialize size", K(ret), K(serialize_size), K_(buffer));
    } else {
      io_.buf_ = buffer_.data();
      io_.size_ = buffer_.pos();
      const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
      io_.io_timeout_ms_ = timeout_us <= 0 ? 0 : timeout_us / 1000;
      io_.io_desc_.set_wait_event(ObWaitEventIds::INTERM_RESULT_DISK_WRITE);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.aio_write(MTL_ID(), io_, io_handle_))) {
        LOG_WARN("failed to write store item to tmp file", K(ret), K_(io));
      } else if (OB_FAIL(item_size_arr_.push_back(serialize_size))) {
        LOG_WARN("failed to append size to item size array", K(ret));
      } else {
        ++count_;
      }
    }
  }
  return ret;
}

int ObMacroMetaTempStore::wait()
{
  int ret = OB_SUCCESS;
  if (io_handle_.is_valid() && OB_FAIL(io_handle_.wait())) {
    LOG_WARN("failed to wait write io finish", K(ret), K_(io));
  } else {
    // free buffer memory after caller actively wait previous io finished
    buffer_.reset();
  }
  return ret;
}

int ObMacroMetaTempStore::get_macro_block_header(
    const char *buf, const int64_t buf_size, ObSSTableMacroBlockHeader &macro_header)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to deserialize common header", K(ret));
  } else if (OB_FAIL(common_header.check_integrity())) {
    LOG_WARN("macro common header invalid", K(ret), K(common_header));
  } else if (OB_FAIL(macro_header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to deserialize sstable macro header", K(ret));
  } else if (OB_UNLIKELY(macro_header.is_valid())) {
    LOG_WARN("invalid sstable macro header", K(ret), K(macro_header));
  }
  return ret;
}

ObMacroMetaTempStoreIter::ObMacroMetaTempStoreIter()
  : meta_store_(nullptr),
    io_allocator_(),
    allocator_(),
    micro_reader_helper_(),
    macro_reader_(),
    curr_read_item_(),
    datum_row_(),
    curr_read_file_pos_(0),
    read_item_cnt_(0)
{}

void ObMacroMetaTempStoreIter::reset()
{
  meta_store_ = nullptr;
  micro_reader_helper_.reset();
  curr_read_item_.reset();
  datum_row_.reset();
  curr_read_file_pos_ = 0;
  read_item_cnt_ = 0;
  io_allocator_.reset();
  allocator_.reset();
}

int ObMacroMetaTempStoreIter::init(ObMacroMetaTempStore &temp_meta_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != meta_store_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initializetion", K(ret));
  } else if (OB_FAIL(micro_reader_helper_.init(allocator_))) {
    LOG_WARN("failed to init micro reader helper", K(ret));
  } else {
    curr_read_file_pos_ = 0;
    read_item_cnt_ = 0;
    meta_store_ = &temp_meta_store;
  }
  return ret;
}

int ObMacroMetaTempStoreIter::get_next(ObDataMacroBlockMeta &macro_meta, ObMicroBlockData &micro_block_data)
{
  int ret = OB_SUCCESS;
  macro_meta.reset();
  micro_block_data.reset();
  if (OB_ISNULL(meta_store_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("temp store iter not inited", K(ret));
  } else if (read_item_cnt_ >= meta_store_->count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(read_next_item())) {
    LOG_WARN("failed to read next item from temp store", K(ret));
  } else if (OB_FAIL(get_macro_meta_from_block_buf(macro_meta))) {
    LOG_WARN("failed to get macro meta from block buf", K(ret));
  } else {
    micro_block_data.buf_ = curr_read_item_.index_block_buf_;
    micro_block_data.size_ = curr_read_item_.index_block_buf_size_;
  }
  return ret;
}

int ObMacroMetaTempStoreIter::read_next_item()
{
  int ret = OB_SUCCESS;
  curr_read_item_.reset();
  io_allocator_.reuse();
  if (OB_FAIL(meta_store_->wait())) {
    LOG_WARN("failed to wait meta store io finish", K(ret));
  } else {
    tmp_file::ObTmpFileIOHandle read_handle;
    tmp_file::ObTmpFileIOInfo io_info = meta_store_->io_;
    const int64_t read_size = meta_store_->item_size_arr_.at(read_item_cnt_);
    char *read_buf = static_cast<char *>(io_allocator_.alloc(read_size));
    if (OB_ISNULL(read_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for tmp file io", K(ret));
    } else {
      io_info.buf_ = read_buf;
      io_info.size_ = read_size;
      io_info.io_desc_.set_wait_event(ObWaitEventIds::INTERM_RESULT_DISK_READ);
      const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
      io_info.io_timeout_ms_ = timeout_us <= 0 ? 0 : timeout_us / 1000;
      int64_t deserialize_pos = 0;
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, curr_read_file_pos_, read_handle))) {
        LOG_WARN("failed to do tmp file pread", K(ret));
      } else if (OB_FAIL(curr_read_item_.deserialize(read_buf, read_size, deserialize_pos))) {
        LOG_WARN("failed to deserialize macro meta temp store item", K(ret));
      } else {
        curr_read_file_pos_ += read_size;
        ++read_item_cnt_;
      }
    }
  }
  return ret;
}

int ObMacroMetaTempStoreIter::get_macro_meta_from_block_buf(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  ObMicroBlockData meta_block;
  ObIMicroBlockReader *micro_reader = nullptr;
  if (OB_UNLIKELY(!curr_read_item_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid current read item", K(ret));
  } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(
      curr_read_item_.macro_header_,
      curr_read_item_.macro_meta_block_buf_,
      curr_read_item_.macro_meta_block_size_,
      meta_block.get_buf(),
      meta_block.get_buf_size(),
      is_compressed))) {
    LOG_WARN("failed to decrypt and decompress meta block", K(ret));
  } else if (OB_UNLIKELY(!meta_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta block invalid", K(ret), K(meta_block));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(meta_block.get_store_type(), micro_reader))) {
    LOG_WARN("failed to get micro reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(meta_block, nullptr))) {
    LOG_WARN("failed to init micro reader", K(ret));
  } else if (datum_row_.get_column_count() != meta_block.get_micro_header()->column_count_) {
    if (OB_FAIL(datum_row_.init(allocator_, meta_block.get_micro_header()->column_count_))) {
      LOG_WARN("failed to init datum row", K(ret));
    }
  } else {
    datum_row_.reuse();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_reader->get_row(0, datum_row_))) {
    LOG_WARN("failed to get row from micro reader", K(ret));
  } else if (OB_FAIL(macro_meta.parse_row(datum_row_))) {
    LOG_WARN("failed to parse macro meta from datum row", K(ret));
  } else {
    macro_meta.val_.macro_id_ = curr_read_item_.macro_id_;
  }
  return ret;
}

} // blocksstable
} // oceanbase
