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
  : index_block_buf_(nullptr),
    index_block_buf_size_(0),
    macro_meta_block_buf_(nullptr),
    macro_meta_block_size_(0)
{}

bool ObMacroMetaTempStore::StoreItem::is_valid() const
{
  return StoreItem::STORE_ITEM_VERSION == header_.version_
         && ((0 == index_block_buf_size_ && nullptr == index_block_buf_)
             || (0 != index_block_buf_size_ && nullptr != index_block_buf_))
         && nullptr != macro_meta_block_buf_
         && 0 != macro_meta_block_size_;
}

void ObMacroMetaTempStore::StoreItem::reset()
{
  index_block_buf_ = nullptr;
  index_block_buf_size_ = 0;
  macro_meta_block_buf_ = nullptr;
  macro_meta_block_size_ = 0;
}

int ObMacroMetaTempStore::StoreItem::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t init_pos = pos;
  StoreItemHeader *item_header = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid store item", K(ret), KP_(index_block_buf),
        K_(index_block_buf_size), KP_(macro_meta_block_buf), K_(macro_meta_block_size));
  } else {
    item_header = reinterpret_cast<StoreItemHeader *>(buf + pos);
    *item_header = header_;
    pos += sizeof(StoreItemHeader);
    LST_DO_CODE(OB_UNIS_ENCODE,
                index_block_buf_size_,
                macro_meta_block_size_);
  }

  if (OB_SUCC(ret)) {
    MEMCPY(buf + pos, index_block_buf_, index_block_buf_size_);
    pos += index_block_buf_size_;
    MEMCPY(buf + pos, macro_meta_block_buf_, macro_meta_block_size_);
    pos += macro_meta_block_size_;
    // calculate checksum and set length
    item_header->checksum_
        = ob_crc64_sse42(0, buf + init_pos + sizeof(StoreItemHeader), pos - init_pos - sizeof(StoreItemHeader));
    item_header->total_length_ = get_serialize_size();
  }
  return ret;
}

int ObMacroMetaTempStore::StoreItem::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t init_pos = pos;
  const StoreItemHeader *item_header = reinterpret_cast<const StoreItemHeader *>(buf + pos);
  header_ = *item_header;
  pos += sizeof(StoreItemHeader);
  // verify checksum.
  const int64_t new_checksum
      = ob_crc64_sse42(0, buf + init_pos + sizeof(StoreItemHeader), header_.total_length_ - sizeof(StoreItemHeader));
  if (OB_UNLIKELY(new_checksum != header_.checksum_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to deserialize store item, un-expected checksum", K(ret), K(new_checksum), K(header_.checksum_));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
                index_block_buf_size_,
                macro_meta_block_size_);
    if (OB_SUCC(ret)) {
      index_block_buf_ = buf + pos;
      pos += index_block_buf_size_;
      macro_meta_block_buf_ = buf + pos;
      pos += macro_meta_block_size_;
      if (OB_UNLIKELY(!is_valid() || pos > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid store item", K(ret), KP_(index_block_buf),
            K_(index_block_buf_size), KP_(macro_meta_block_buf), K_(macro_meta_block_size), K(data_len));
      }
    }
  }
  return ret;
}

int64_t ObMacroMetaTempStore::StoreItem::get_serialize_size() const
{
  int64_t len = 0;
  len += sizeof(StoreItemHeader);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              index_block_buf_size_,
              macro_meta_block_size_);
  len += index_block_buf_size_;
  len += macro_meta_block_size_;
  return len;
}

ObMacroMetaTempStore::ObMacroMetaTempStore()
  : dir_id_(0),
    io_(),
    io_handle_(),
    buffer_("MaTmpStore"),
    allocator_("MaTmpStoreRead"),
    datum_allocator_("MaTmpStoreIO"),
    macro_meta_allocator_("MaTmpStoreMeta"),
    macro_reader_(),
    datum_row_(),
    is_inited_(false),
    is_empty_(true)
{
}

int ObMacroMetaTempStore::init(const int64_t dir_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(MTL_ID(), io_.fd_, dir_id_))) {
    LOG_WARN("open tmp file failed", K(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    datum_allocator_.set_tenant_id(MTL_ID());
    macro_meta_allocator_.set_tenant_id(MTL_ID());
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
  allocator_.reset();
  datum_allocator_.reset();
  datum_row_.reset();
  macro_meta_allocator_.reset(); // must reset after datum_row_
  is_inited_ = false;
  is_empty_ = true;
}

bool ObMacroMetaTempStore::is_valid() const
{
  return io_.is_valid() && is_inited_;
}

bool ObMacroMetaTempStore::is_empty() const
{
  return is_empty_;
}

int ObMacroMetaTempStore::append(const char *block_buf, const int64_t block_size, const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSTableMacroBlockHeader macro_header;
  ObDataMacroBlockMeta macro_meta;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(block_buf) || OB_UNLIKELY(block_size < 0 || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_size), KP(block_buf), K(macro_id));
  } else if (OB_FAIL(get_macro_block_header(block_buf, block_size, macro_header))) {
    LOG_WARN("failed to get macro block header", K(ret));
  } else if (OB_FAIL(get_macro_meta_from_block_buf(macro_header,
                                                   macro_id,
                                                   block_buf + macro_header.fixed_header_.meta_block_offset_,
                                                   macro_header.fixed_header_.meta_block_size_,
                                                   macro_meta))) {
    LOG_WARN("fail to get macro meta from block buf", K(ret), K(macro_header), K(block_size), K(macro_id));
  } else {
    ObMicroBlockData leaf_index_block;
    leaf_index_block.buf_ = block_buf + macro_header.fixed_header_.idx_block_offset_;
    leaf_index_block.size_ = macro_header.fixed_header_.idx_block_size_;
    if (OB_FAIL(inner_append(macro_meta, &leaf_index_block))) {
      LOG_WARN("fail to inner append macro meta and leaf index block to temp store",
               K(ret), K(macro_meta), K(leaf_index_block));
    } else {
      is_empty_ = false;
    }
  }
  return ret;
}

int ObMacroMetaTempStore::append(const ObDataMacroBlockMeta &macro_meta, const ObMicroBlockData *leaf_index_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(inner_append(macro_meta, leaf_index_block))) {
    LOG_WARN("fail to inner append macro meta and leaf index block to temp store",
             K(ret), K(macro_meta), K(leaf_index_block));
  } else {
    is_empty_ = false;
  }
  return ret;
}

int ObMacroMetaTempStore::inner_append(const ObDataMacroBlockMeta &macro_meta, const ObMicroBlockData *leaf_index_block)
{
  int ret = OB_SUCCESS;
  StoreItem item;
  int64_t macro_meta_serialize_size = 0;
  char *macro_meta_buf = nullptr;
  int64_t pos = 0;
  const uint64_t data_version = CLUSTER_CURRENT_VERSION;
  if (OB_UNLIKELY(!macro_meta.is_valid() || !macro_meta.get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, un-expected macro meta", K(ret), K(macro_meta));
  } else if (OB_UNLIKELY(leaf_index_block != nullptr && !leaf_index_block->is_valid())) {
    /* leaf index block can be a nullptr, otherwise it should pointer to a valid index block */
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt, un-expected leaf index block", K(ret), KPC(leaf_index_block));
  } else if (io_handle_.is_valid() && OB_FAIL(io_handle_.wait())) {
    LOG_WARN("failed to wait previous write", K(ret));
  } else if (FALSE_IT(macro_meta_serialize_size = macro_meta.get_serialize_size(data_version))) {
  } else if (OB_ISNULL(macro_meta_buf = static_cast<char *>(datum_allocator_.alloc(macro_meta_serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for macro meta buffer", K(ret));
  } else if (OB_FAIL(macro_meta.serialize(macro_meta_buf, macro_meta_serialize_size, pos, data_version))) {
    LOG_WARN("fail to serialize macro meta", K(ret), K(macro_meta_serialize_size), K(macro_meta));
  } else {
    // leaf index block might be nullptr (compaction reuse macro block without clustered index block).
    item.index_block_buf_ = (leaf_index_block == nullptr ? nullptr : leaf_index_block->buf_);
    item.index_block_buf_size_ = (leaf_index_block == nullptr ? 0 : leaf_index_block->size_);
    item.macro_meta_block_buf_ = macro_meta_buf;
    item.macro_meta_block_size_ = macro_meta_serialize_size;
    int64_t serialize_size = item.get_serialize_size();
    buffer_.reuse();
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
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(MTL_ID(), io_))) {
        LOG_WARN("failed to write store item to tmp file", K(ret), K_(io));
      }
    }
  }
  // release temp file io buffer
  datum_allocator_.reuse();
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
  } else if (OB_UNLIKELY(!macro_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sstable macro header", K(ret), K(macro_header));
  }
  return ret;
}

int ObMacroMetaTempStore::get_macro_meta_from_block_buf(const ObSSTableMacroBlockHeader &macro_header,
                                                        const MacroBlockId &macro_id,
                                                        const char *buf,
                                                        const int64_t buf_size,
                                                        ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  ObMicroBlockData meta_block;
  ObIMicroBlockReader *micro_reader = nullptr;
  ObMicroBlockReaderHelper micro_reader_helper;
  ObDatumRow tmp_datum_row;
  allocator_.reuse();
  if (OB_UNLIKELY(buf == nullptr || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for macro meta buffer and size", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(macro_header,
                                                              buf,
                                                              buf_size,
                                                              false,
                                                              meta_block.get_buf(),
                                                              meta_block.get_buf_size(),
                                                              is_compressed))) {
    LOG_WARN("failed to decrypt and decompress meta block", K(ret));
  } else if (OB_UNLIKELY(!meta_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta block invalid", K(ret), K(meta_block));
  } else if (OB_FAIL(micro_reader_helper.init(allocator_))) {
    LOG_WARN("fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper.get_reader(meta_block.get_store_type(), micro_reader))) {
    LOG_WARN("failed to get micro reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(meta_block, nullptr))) {
    LOG_WARN("failed to init micro reader", K(ret));
  } else if (OB_FAIL(tmp_datum_row.init(allocator_, meta_block.get_micro_header()->column_count_))) {
      LOG_WARN("failed to init temp datum row", K(ret));
  } else if (datum_row_.get_column_count() != meta_block.get_micro_header()->column_count_) {
    datum_row_.reset();
    macro_meta_allocator_.reuse();
    if (OB_FAIL(datum_row_.init(macro_meta_allocator_, meta_block.get_micro_header()->column_count_))) {
      LOG_WARN("failed to init datum row", K(ret));
    }
  } else {
    datum_row_.reuse();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_reader->get_row(0, tmp_datum_row))) {
    LOG_WARN("failed to get row from micro reader", K(ret));
  } else if (OB_FAIL(datum_row_.deep_copy(tmp_datum_row, macro_meta_allocator_))) {
    LOG_WARN("failed to deep copy datum row", K(ret), K(tmp_datum_row));
  } else if (OB_FAIL(macro_meta.parse_row(datum_row_))) {
    LOG_WARN("failed to parse macro meta from datum row", K(ret));
  } else {
    macro_meta.val_.macro_id_ = macro_id;
  }
  return ret;
}

ObMacroMetaTempStoreIter::ObMacroMetaTempStoreIter()
  : io_info_(),
    meta_store_file_length_(0),
    io_allocator_("MaTmpStoreIO"),
    curr_read_item_(),
    submit_io_size_(ObMacroMetaTempStoreIter::META_TEMP_STORE_INITIAL_READ_SIZE),
    meta_store_read_offset_(0),
    fragment_offset_(0),
    fragment_size_(0),
    meta_store_fragment_(nullptr),
    is_iter_end_(false)
{
}

void ObMacroMetaTempStoreIter::reset()
{
  io_info_.reset();
  curr_read_item_.reset();
  submit_io_size_ = ObMacroMetaTempStoreIter::META_TEMP_STORE_INITIAL_READ_SIZE;
  meta_store_read_offset_ = 0;
  fragment_offset_ = 0;
  fragment_size_ = 0;
  meta_store_fragment_ = nullptr;
  io_allocator_.reset();
  is_iter_end_ = false;
}

int ObMacroMetaTempStoreIter::init(ObMacroMetaTempStore &temp_meta_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_info_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initializetion", K(ret));
  } else if (temp_meta_store.is_empty()) {
    is_iter_end_ = true;
  } else if (OB_UNLIKELY(!temp_meta_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init macro meta temp store iter, invalid argument", K(ret), K(temp_meta_store));
  } else if (OB_FAIL(temp_meta_store.wait())) {
    LOG_WARN("fail to wait temp meta store write finish", K(ret), K(temp_meta_store));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(MTL_ID(),
                                                                             temp_meta_store.io_.fd_,
                                                                             meta_store_file_length_))) {
    LOG_WARN("fail to get temp file length", K(ret), K(temp_meta_store), K(temp_meta_store.io_.fd_));
  } else {
    io_info_ = temp_meta_store.io_;
  }
  return ret;
}

int ObMacroMetaTempStoreIter::get_next(ObDataMacroBlockMeta &macro_meta, ObMicroBlockData &micro_block_data)
{
  int ret = OB_SUCCESS;
  macro_meta.reset();
  micro_block_data.reset();
  int64_t pos = 0;
  StoreItemHeader *item_header  = nullptr;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!io_info_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("temp store iter not inited", K(ret), K(io_info_));
  } else if (meta_store_read_offset_ >= meta_store_file_length_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(try_submit_io())) {
    LOG_WARN("fail to try submit io", K(ret));
  } else if (FALSE_IT(pos = meta_store_read_offset_ - fragment_offset_)) {
  } else if (OB_FAIL(curr_read_item_.deserialize(meta_store_fragment_, fragment_size_, pos))) {
    LOG_WARN("fail to deserialize macro meta from store item",
             K(ret), KP(meta_store_fragment_), K(fragment_size_), K(pos));
  } else {
    int64_t macro_meta_pos = 0;
    if (OB_FAIL(macro_meta.deserialize(curr_read_item_.macro_meta_block_buf_,
                                       curr_read_item_.macro_meta_block_size_,
                                       io_allocator_,
                                       macro_meta_pos))) {
      LOG_WARN("fail to deserialize macro meta", K(ret), K(curr_read_item_.macro_meta_block_size_));
    } else {
      micro_block_data.buf_ = curr_read_item_.index_block_buf_;
      micro_block_data.size_ = curr_read_item_.index_block_buf_size_;
      // update read offset
      meta_store_read_offset_ += (pos - (meta_store_read_offset_ - fragment_offset_));
    }
  }
  return ret;
}

int ObMacroMetaTempStoreIter::try_submit_io()
{
  int ret = OB_SUCCESS;
  bool header_in_mem = meta_store_read_offset_ + sizeof(StoreItemHeader) <= fragment_offset_ + fragment_size_;
  bool data_in_mem = false;
  if (header_in_mem) {
    const StoreItemHeader *item_header
        = reinterpret_cast<const StoreItemHeader *>(meta_store_fragment_ + meta_store_read_offset_ - fragment_offset_);
    data_in_mem = meta_store_read_offset_ + item_header->total_length_ <= fragment_offset_ + fragment_size_;
    submit_io_size_ = MAX(2 * item_header->total_length_, submit_io_size_);
  }
  if (!header_in_mem || !data_in_mem) {
    // free current fragment and submit io.
    meta_store_fragment_ = nullptr;
    io_allocator_.reuse();
    // allocate memory for new fragment.
    const int64_t curr_read_size
        = MIN(meta_store_file_length_ - meta_store_read_offset_, submit_io_size_);
    if (OB_ISNULL(meta_store_fragment_ = static_cast<char *>(io_allocator_.alloc(curr_read_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for new fragment", K(ret), K(curr_read_size));
    } else {
      // submit io and update fragment info.
      tmp_file::ObTmpFileIOHandle read_handle;
      tmp_file::ObTmpFileIOInfo io_info = io_info_;
      io_info.buf_ = meta_store_fragment_;
      io_info.size_ = curr_read_size;
      io_info.io_desc_.set_wait_event(ObWaitEventIds::INTERM_RESULT_DISK_READ);
      const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
      int64_t deserialize_pos = 0;
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret), K(timeout_us));
      } else {
        io_info.io_timeout_ms_ = timeout_us / 1000;
      }
      if (OB_FAIL(ret)) {
        // pass
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, meta_store_read_offset_, read_handle))) {
        LOG_WARN("failed to do tmp file pread", K(ret));
      } else if (OB_FAIL(read_handle.wait())) {
        LOG_WARN("failed to wait read io finish", K(ret));
      } else {
        fragment_offset_ = meta_store_read_offset_;
        fragment_size_ = curr_read_size;
      }
    }
    // double check because data may not in current fragment.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_submit_io())) {
      LOG_WARN("fail to try submit io");
    }
  }
  return ret;
}

} // blocksstable
} // oceanbase
