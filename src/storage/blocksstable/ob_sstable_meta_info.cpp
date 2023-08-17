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

#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/ob_sstable_meta_info.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_index_block_row_scanner.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{

ObRootBlockInfo::ObRootBlockInfo()
  : addr_(),
    block_data_()
{
}

ObRootBlockInfo::~ObRootBlockInfo()
{
  reset();
}

bool ObRootBlockInfo::is_valid() const
{
  return addr_.is_valid()
      && (!addr_.is_memory() || (block_data_.is_valid() && block_data_.size_ == addr_.size()));
}

void ObRootBlockInfo::reset()
{
  block_data_.reset();
  addr_.reset();
}

int ObRootBlockInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t tmp_pos = 0;
    const int64_t len = get_serialize_size_();
    OB_UNIS_ENCODE(ROOT_BLOCK_INFO_VERSION);
    OB_UNIS_ENCODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialize_(buf + pos, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize address and dump", K(ret), KP(buf), K(buf_len), K(pos), K(addr_));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObRootBlockInfo::deserialize(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  int64_t len = 0;
  int64_t version = 0;
  if (OB_UNLIKELY(!des_meta.is_valid())
      || OB_ISNULL(buf)
      || OB_UNLIKELY(data_len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(des_meta), KP(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(version != ROOT_BLOCK_INFO_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("object version mismatch", K(ret), K(version));
    } else if (OB_FAIL(deserialize_(allocator, des_meta, buf + pos, data_len, tmp_pos))) {
      LOG_WARN("fail to deserialize address and load", K(ret), K(des_meta), KP(buf),
          K(data_len), K(pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int64_t ObRootBlockInfo::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t payload_size = get_serialize_size_();
  OB_UNIS_ADD_LEN(ROOT_BLOCK_INFO_VERSION);
  OB_UNIS_ADD_LEN(payload_size);
  len += get_serialize_size_();
  return len;
}

int64_t ObRootBlockInfo::get_serialize_size_() const
{
  int64_t len = 0;
  len += addr_.get_serialize_size();
  if (addr_.is_memory()) {
    len += addr_.size();
  }
  return len;
}

int ObRootBlockInfo::init_root_block_info(
    common::ObArenaAllocator &allocator,
    const ObMetaDiskAddr &addr,
    const ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  char *dst_buf = nullptr;
  int64_t size = 0;
  int64_t offset = 0;
  if (OB_UNLIKELY(!addr.is_valid())
      || OB_UNLIKELY(!addr.is_memory() && !addr.is_block() && !addr.is_none())
      || (OB_UNLIKELY(addr.is_memory()) && OB_ISNULL(block_data.buf_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), K(block_data));
  } else if (FALSE_IT(addr_ = addr)) {
  } else if (!addr.is_memory()) {
    block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
  } else if (OB_FAIL(addr.get_mem_addr(offset, size))) {
    LOG_WARN("fail to get memory address", K(ret), K(addr));
  } else {
    LOG_DEBUG("block data type", K(block_data.type_));
    if (ObMicroBlockData::DDL_BLOCK_TREE == block_data.type_) {
      block_data_ = block_data;
    } else if (size > 0 && OB_ISNULL(dst_buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(size));
    } else {
      MEMCPY(dst_buf, block_data.buf_, size);
      block_data_.buf_ = dst_buf;
      block_data_.size_ = size;
      block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(dst_buf)) {
      allocator.free(dst_buf);
    }
  }
  return ret;
}

int ObRootBlockInfo::load_root_block_data(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!des_meta.is_valid())
      || OB_UNLIKELY(!addr_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(des_meta), K(addr_));
  } else if (addr_.is_block()) {
    char *dst_buf = nullptr;
    ObMacroBlockReader reader;
    bool is_compressed = false;
    const ObMemAttr mem_attr(MTL_ID(), "RootBlkInfo");
    if (OB_ISNULL(dst_buf = static_cast<char *>(ob_malloc(addr_.size(), mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(addr_));
    } else if (OB_FAIL(read_block_data(addr_, dst_buf, addr_.size()))) {
      LOG_WARN("fail to read block data", K(ret), K(addr_));
    } else if (OB_FAIL(reader.decrypt_and_decompress_data(des_meta, dst_buf, addr_.size(),
        block_data_.buf_, block_data_.size_, is_compressed, true, &allocator))) {
      LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), K(addr_), K_(block_data));
    } else {
      block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
    }
    if (OB_NOT_NULL(dst_buf)) {
      ob_free(dst_buf);
      dst_buf = nullptr;
    }
  }
  return ret;
}

int ObRootBlockInfo::transform_root_block_data(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (ObMicroBlockData::INDEX_BLOCK == block_data_.type_
      && OB_ISNULL(block_data_.get_extra_buf())
      && OB_NOT_NULL(block_data_.get_buf())) {
    ObIndexBlockDataTransformer transformer;
    char *extra_buf = nullptr;
    int64_t extra_size = ObIndexBlockDataTransformer::get_transformed_block_mem_size(block_data_);
    if (extra_size > 0 && OB_ISNULL(extra_buf = static_cast<char *>(allocator.alloc(extra_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(extra_size));
    } else if (OB_FAIL(transformer.transform(block_data_, extra_buf, extra_size))) {
      LOG_WARN("Fail to transform root block to memory format", K(ret),
          K(block_data_), KP(extra_buf), K(extra_size));
    } else {
      block_data_.get_extra_buf() = extra_buf;
      block_data_.get_extra_size() = extra_size;
      block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(extra_buf)) {
      allocator.free(extra_buf);
    } else {
      LOG_DEBUG("succeed to transform root block data", K(addr_), KPC(this));
    }
  }
  return ret;
}

int ObRootBlockInfo::read_block_data(
    const storage::ObMetaDiskAddr &addr,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())
      || OB_UNLIKELY(!addr.is_block())
      || OB_UNLIKELY(buf_len < addr.size())
      || OB_UNLIKELY(addr.offset() >= OB_SERVER_BLOCK_MGR.get_macro_block_size())
      || OB_UNLIKELY(0 == addr.size() || addr.size() > OB_SERVER_BLOCK_MGR.get_macro_block_size())
      || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), KP(buf), K(buf_len));
  } else {
    blocksstable::ObMacroBlockHandle handle;
    blocksstable::ObMacroBlockReadInfo read_info;
    handle.reset();
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_desc_.set_group_id(ObIOModule::ROOT_BLOCK_IO);
    if (OB_FAIL(addr.get_block_addr(read_info.macro_block_id_, read_info.offset_, read_info.size_))) {
      LOG_WARN("fail to get block address", K(ret), K(addr));
    } else if (OB_FAIL(ObBlockManager::read_block(read_info, handle))) {
      LOG_WARN("fail to read block from macro block", K(ret), K(read_info));
    } else {
      MEMCPY(buf, handle.get_buffer(), addr.size());
    }
  }
  return ret;
}

int ObRootBlockInfo::serialize_(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const storage::ObMetaDiskAddr &addr = addr_;
  const char *data_buf = block_data_.buf_;
  if (OB_UNLIKELY(!addr.is_valid())
      || OB_UNLIKELY(!addr.is_memory() && !addr.is_block() && !addr.is_none())
      || (OB_UNLIKELY(addr.is_memory()) && OB_ISNULL(data_buf))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < pos)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(addr), KP(data_buf), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(addr.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize root block address", K(ret), KP(buf), K(buf_len), K(pos), K(addr));
  } else if (addr.is_memory()) {
    MEMCPY(buf + pos, data_buf, addr.size());
    pos += addr.size();
  }
  return ret;
}

int ObRootBlockInfo::deserialize_(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  char *data_buf = nullptr;
  int64_t block_size = 0;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(!des_meta.is_valid())
      || OB_UNLIKELY(pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(des_meta), K(data_len), K(pos));
  } else if (OB_FAIL(addr_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize address", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(!addr_.is_valid())
          || OB_UNLIKELY(!addr_.is_memory() && !addr_.is_block() && !addr_.is_none())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid address", K(ret), K(addr_));
  } else if (addr_.is_none()) {
    // do nothing
  } else if (addr_.size() > 0 && OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(addr_.size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc data buffer", K(ret), K(addr_));
  } else if (addr_.is_block()) {
    ObMacroBlockReader reader;
    const char *decomp_buf = nullptr;
    int64_t decomp_size = 0;
    bool is_compressed = false;
    if (OB_FAIL(read_block_data(addr_, data_buf, addr_.size()))) {
      LOG_WARN("fail to read block data", K(ret), K(addr_));
    } else if (OB_FAIL(reader.decrypt_and_decompress_data(des_meta, data_buf, addr_.size(),
        decomp_buf, decomp_size, is_compressed, true, &allocator))) {
      LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), KP(data_buf), K_(addr));
    } else {
      allocator.free(data_buf);
      data_buf = const_cast<char *>(decomp_buf);
      block_size = decomp_size;
    }
  } else if (OB_UNLIKELY(pos + addr_.size() > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr_), K(data_len), K(pos));
  } else {
    MEMCPY(data_buf, buf + pos, addr_.size());
    block_size = addr_.size();
    pos += addr_.size();
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(data_buf)) {
      allocator.free(data_buf);
      data_buf = nullptr;
    }
  } else {
    block_data_.buf_  = data_buf;
    block_data_.type_ = ObMicroBlockData::Type::INDEX_BLOCK;
    block_data_.size_ = block_size;
  }
  return ret;
}

int ObRootBlockInfo::deep_copy(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObRootBlockInfo &dest) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  const int64_t variable_size = get_variable_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < variable_size + pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(variable_size), K(pos));
  } else {
    dest.addr_ = addr_;
    if (OB_NOT_NULL(block_data_.buf_)) {
      MEMCPY(buf + pos, block_data_.buf_, block_data_.size_);
      dest.block_data_.buf_ = buf + pos;
      dest.block_data_.size_ = block_data_.size_;
      pos += block_data_.size_;
    }
    if (OB_NOT_NULL(block_data_.extra_buf_)) {
      const ObIndexBlockDataHeader *src_idx_header
          = reinterpret_cast<const ObIndexBlockDataHeader *>(block_data_.get_extra_buf());
      ObIndexBlockDataTransformer transformer;
      if (OB_FAIL(transformer.update_index_block(*src_idx_header,
                                                         dest.block_data_.buf_,
                                                         block_data_.get_buf_size(),
                                                         buf + pos,
                                                         block_data_.get_extra_size()))) {
        LOG_WARN("fail to update transformed index block", K(ret));
      } else {
        dest.block_data_.extra_buf_ = buf + pos;
        dest.block_data_.extra_size_ = block_data_.extra_size_;
        pos += block_data_.extra_size_;
      }
    }
    dest.block_data_.type_ = block_data_.type_;
  }
  return ret;
}

ObMacroIdIterator::ObMacroIdIterator()
  : value_ptr_(nullptr),
    pos_(0),
    count_(0),
    is_inited_(false),
    allocator_("MacroIdIter")
{
}

int ObMacroIdIterator::init(const Type type, const MacroBlockId &entry_id, const int64_t pos)
{
  int ret = OB_SUCCESS;
  MacroBlockId *data_blk_ids = nullptr;
  int64_t data_blk_cnt = 0;
  MacroBlockId *other_blk_ids = nullptr;
  int64_t other_blk_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(type >= Type::MAX || !entry_id.is_valid() || pos < 0 || count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), K(entry_id), K(pos), K(count_));
  } else if (OB_FAIL(ObSSTableMacroInfo::read_block_ids(entry_id, allocator_, data_blk_ids,
      data_blk_cnt, other_blk_ids, other_blk_cnt))) {
    LOG_WARN("fail to read block ids", K(ret), K(entry_id));
  } else {
    if (Type::DATA_BLOCK == type) {
      value_ptr_ = data_blk_ids;
      count_ = data_blk_cnt;
    } else if (Type::OTHER_BLOCK == type) {
      value_ptr_ = other_blk_ids;
      count_ = other_blk_cnt;
    }
    pos_ = pos;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObMacroIdIterator::init(MacroBlockId *ptr, const int64_t count, const int64_t pos)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(pos < 0 || count < 0 || pos > count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ptr), K(count), K(pos));
  } else {
    value_ptr_ = ptr;
    pos_ = pos;
    count_ = count;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObMacroIdIterator::reset()
{
  value_ptr_ = nullptr;
  pos_ = 0;
  count_ = 0;
  is_inited_ = false;
  allocator_.reuse();
}

int ObMacroIdIterator::get_next_macro_id(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (0 == count_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else if (pos_ < count_) {
    macro_id = value_ptr_[pos_++];
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

ObSSTableMacroInfo::ObSSTableMacroInfo()
  : macro_meta_info_(),
    data_block_ids_(nullptr),
    other_block_ids_(nullptr),
    linked_block_ids_(nullptr),
    data_block_count_(0),
    other_block_count_(0),
    linked_block_count_(0),
    entry_id_(),
    is_meta_root_(false),
    nested_offset_(0),
    nested_size_(0)
{
}

ObSSTableMacroInfo::~ObSSTableMacroInfo()
{
  reset();
}

int ObSSTableMacroInfo::init_macro_info(
    common::ObArenaAllocator &allocator,
    const storage::ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(macro_meta_info_.init_root_block_info(allocator,
      param.data_block_macro_meta_addr_, param.data_block_macro_meta_))) {
    LOG_WARN("fail to init macro meta info", K(ret), K(param));
  } else if (FALSE_IT(data_block_count_ = param.data_block_ids_.count())) {
  } else if (FALSE_IT(other_block_count_ = param.other_block_ids_.count())) {
  } else if (data_block_count_ + other_block_count_ >= BLOCK_CNT_THRESHOLD) {
    if (OB_FAIL(persist_block_ids(param.data_block_ids_, param.other_block_ids_, allocator))) {
      LOG_WARN("fail to persist block ids", K(ret), K(param));
    }
  } else if (param.data_block_ids_.count() > 0
      && OB_ISNULL(data_block_ids_ = static_cast<MacroBlockId *>(allocator.alloc(
      sizeof(MacroBlockId) * param.data_block_ids_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(param.data_block_ids_.count()));
  } else if (param.other_block_ids_.count() >0
      && OB_ISNULL(other_block_ids_ = static_cast<MacroBlockId *>(allocator.alloc(
      sizeof(MacroBlockId) * param.other_block_ids_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(param.other_block_ids_.count()));
  } else {
    entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
    for (int64_t i = 0; OB_SUCC(ret) && i < param.data_block_ids_.count(); ++i) {
      new (data_block_ids_ + i) MacroBlockId(param.data_block_ids_.at(i));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.other_block_ids_.count(); ++i) {
      new (other_block_ids_ + i) MacroBlockId(param.other_block_ids_.at(i));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_meta_root_ = param.is_meta_root_;
    nested_offset_ = param.nested_offset_;
    nested_size_ = 0 == param.nested_size_ ? OB_DEFAULT_MACRO_BLOCK_SIZE : param.nested_size_;
  }
  return ret;
}

int ObSSTableMacroInfo::load_root_block_data(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta)
{
  return macro_meta_info_.load_root_block_data(allocator, des_meta);
}

bool ObSSTableMacroInfo::is_valid() const
{
  return macro_meta_info_.is_valid();
}

void ObSSTableMacroInfo::reset()
{
  macro_meta_info_.reset();
  if (nullptr != data_block_ids_) {
    for (int64_t i = 0; i < data_block_count_; i++) {
      data_block_ids_[i].~MacroBlockId();
    }
    data_block_ids_ = nullptr;
  }
  data_block_count_ = 0;
  if (nullptr != other_block_ids_) {
    for (int64_t i = 0; i < other_block_count_; i++) {
      other_block_ids_[i].~MacroBlockId();
    }
    other_block_ids_ = nullptr;
  }
  other_block_count_ = 0;
  if (nullptr != linked_block_ids_) {
    for (int64_t i = 0; i < linked_block_count_; i++) {
      linked_block_ids_[i].~MacroBlockId();
    }
    linked_block_ids_ = nullptr;
  }
  linked_block_count_ = 0;
  entry_id_.reset();
  is_meta_root_ = false;
  nested_offset_ = 0;
  nested_size_ = 0;
}

int ObSSTableMacroInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t tmp_pos = 0;
    const int64_t len = get_serialize_size_();
    OB_UNIS_ENCODE(MACRO_INFO_VERSION);
    OB_UNIS_ENCODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialize_(buf + pos, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize_", K(ret), K(buf_len), K(pos), K(tmp_pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObSSTableMacroInfo::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_meta_info_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize root block info", K(ret), K(buf_len), K(pos), K_(macro_meta_info));
  } else if (OB_FAIL(entry_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize data block ids' entry", K(ret), K(buf_len), K(pos), K_(entry_id));
  } else if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_){
    OB_UNIS_ENCODE_ARRAY(data_block_ids_, data_block_count_);
    OB_UNIS_ENCODE_ARRAY(other_block_ids_, other_block_count_);
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_meta_root_))) {
    LOG_WARN("fail to serialize is_meta_root_", K(ret), K(is_meta_root_), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, nested_offset_))) {
    LOG_WARN("fail to serialize nested_offset_", K(ret), K(buf_len), K(pos), K(nested_offset_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, nested_size_))) {
    LOG_WARN("fail to serialize nested_size_", K(ret), K(buf_len), K(pos), K(nested_size_));
  }

  return ret;
}

int ObSSTableMacroInfo::persist_block_ids(
    const common::ObIArray<MacroBlockId> &data_ids,
    const common::ObIArray<MacroBlockId> &other_ids,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemWriter block_writer;
  if (OB_FAIL(write_block_ids(data_ids, other_ids, block_writer, entry_id_))) {
    LOG_WARN("fail to write other block ids", K(ret));
  } else if (OB_FAIL(save_linked_block_list(block_writer.get_meta_block_list(), allocator))) {
    LOG_WARN("fail to save linked block ids", K(ret));
  } else if (OB_FAIL(inc_linked_block_ref_cnt(allocator))) {
    LOG_WARN("fail to increase linked block ref cnt", K(ret));
  }
  return ret;
}

void ObSSTableMacroInfo::dec_linked_block_ref_cnt()
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (0 == linked_block_count_) {
    // skip the decrease
  } else if (OB_ISNULL(linked_block_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("linked_block_ids is null, but linked_block_count_ is not 0", K(linked_block_count_));
  } else {
    for (; idx < linked_block_count_; idx++) {
      const MacroBlockId &macro_id = linked_block_ids_[idx];
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
        LOG_ERROR("fail to decrease macro block ref cnt", K(ret), K(macro_id));
      }
    }
  }
}

int ObSSTableMacroInfo::inc_linked_block_ref_cnt(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (0 == linked_block_count_) {
    // skip the decrease
  } else if (OB_ISNULL(linked_block_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("linked_block_ids is null, but linked_block_count_ is not 0", K(linked_block_count_));
  } else {
    for (; OB_SUCC(ret) && idx < linked_block_count_; idx++) {
      const MacroBlockId &macro_id = linked_block_ids_[idx];
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
        LOG_ERROR("fail to increase macro block ref cnt", K(ret), K(macro_id));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < idx; i++) {
        const MacroBlockId &macro_id = linked_block_ids_[idx];
        if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to decrease macro block ref cnt", K(tmp_ret), K(macro_id));
        }
      }
      allocator.free(linked_block_ids_);
      linked_block_ids_ = nullptr;
    }
  }
  return ret;
}

int ObSSTableMacroInfo::save_linked_block_list(
    const common::ObIArray<MacroBlockId> &list,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t ids_cnt = list.count();
  if (ids_cnt > 0 && OB_ISNULL(linked_block_ids_ = static_cast<MacroBlockId *>(allocator.alloc(
      sizeof(MacroBlockId) * ids_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(ids_cnt));
  } else {
    int64_t idx = 0;
    for (int64_t idx = 0; idx < ids_cnt; ++idx) {
      new (linked_block_ids_ + idx) MacroBlockId(list.at(idx));
    }
    linked_block_count_ = ids_cnt;
  }
  return ret;
}

int ObSSTableMacroInfo::deserialize(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  int64_t len = 0;
  int64_t version = 0;
  if (OB_UNLIKELY(!des_meta.is_valid())
      || OB_ISNULL(buf)
      || OB_UNLIKELY(data_len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(des_meta), KP(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(version != MACRO_INFO_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("object version mismatch", K(ret), K(version));
    } else if (OB_UNLIKELY(data_len - pos < len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("payload is out of the buf's boundary", K(ret), K(data_len), K(pos), K(len));
    } else if (OB_FAIL(deserialize_(allocator, des_meta, buf + pos, len, tmp_pos))) {
      LOG_WARN("fail to deserialize_", K(ret), K(des_meta), KP(buf), K(len), K(tmp_pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), K(*this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObSSTableMacroInfo::deserialize_(
    common::ObArenaAllocator &allocator,
    const ObMicroBlockDesMeta &des_meta,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;

  if (OB_FAIL(macro_meta_info_.deserialize(allocator, des_meta, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize macro meta info", K(ret), K(des_meta), K(data_len), K(pos));
  } else if (OB_FAIL(entry_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize entry block macro id", K(ret), KP(buf), K(data_len), K(pos));
  } else if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK != entry_id_) {
    ObLinkedMacroBlockItemReader block_reader;
    ObMetaDiskAddr addr;
    char *reader_buf = nullptr;
    int64_t pos = 0;
    int64_t reader_len = 0;
    if (OB_FAIL(block_reader.init(entry_id_))) {
      LOG_WARN("fail to initialize reader", K(ret), K(entry_id_));
    } else if (OB_FAIL(block_reader.get_next_item(reader_buf, reader_len, addr))) {// read data ids
      LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
    } else if (OB_FAIL(serialization::decode(reader_buf, reader_len, pos, data_block_count_))) {
      LOG_WARN("fail to deserialize data block ids", K(ret));
    } else if (OB_FAIL(block_reader.get_next_item(reader_buf, reader_len, addr))) {// read other ids
      LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(serialization::decode(reader_buf, reader_len, pos, other_block_count_))) {
      LOG_WARN("fail to deserialize other block ids", K(ret));
    } else if (OB_FAIL(save_linked_block_list(block_reader.get_meta_block_list(), allocator))) {
      LOG_WARN("fail to save linked block ids", K(ret), K_(linked_block_ids));
    }
  } else {
    if (pos < data_len && OB_FAIL(deserialize_block_ids(allocator, buf, data_len, pos,
        data_block_ids_, data_block_count_))) {
      LOG_WARN("fail to deserialize data block ids", K(ret), KP(buf), K(data_len), K(pos));
    } else if (pos < data_len && OB_FAIL(deserialize_block_ids(allocator, buf, data_len, pos,
        other_block_ids_, other_block_count_))) {
      LOG_WARN("fail to deserialize other block ids", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (pos < data_len && OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_meta_root_))) {
    LOG_WARN("fail to deserialize is_meta_root_", K(ret));
  } else if (pos < data_len && OB_FAIL(serialization::decode_i64(buf, data_len, pos, &nested_offset_))) {
    LOG_WARN("fail to deserialize nested_offset_", K(ret));
  } else if (pos < data_len && OB_FAIL(serialization::decode_i64(buf, data_len, pos, &nested_size_))) {
    LOG_WARN("fail to deserialize nested_size_", K(ret));
  }

  return ret;
}

int ObSSTableMacroInfo::read_block_ids(
    const MacroBlockId &entry_id,
    common::ObArenaAllocator &allocator,
    MacroBlockId *&data_blk_ids,
    int64_t &data_blk_cnt,
    MacroBlockId *&other_blk_ids,
    int64_t &other_blk_cnt)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader block_reader;
  if (OB_UNLIKELY(!entry_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(entry_id));
  } else if (OB_FAIL(block_reader.init(entry_id))) {
    LOG_WARN("fail to initialize reader", K(ret), K(entry_id));
  } else if (OB_FAIL(read_block_ids(allocator, block_reader, data_blk_ids, data_blk_cnt,
      other_blk_ids, other_blk_cnt))) {
    LOG_WARN("fail to read block ids", K(ret), K(entry_id), K(data_blk_cnt), K(other_blk_cnt));
  }
  return ret;
}

int ObSSTableMacroInfo::read_block_ids(
    common::ObArenaAllocator &allocator,
    storage::ObLinkedMacroBlockItemReader &reader,
    MacroBlockId *&data_block_ids,
    int64_t &data_block_count,
    MacroBlockId *&other_block_ids,
    int64_t &other_block_count)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr addr;
  char *reader_buf = nullptr;
  int64_t reader_len = 0;
  int64_t reader_pos = 0;
  if (OB_FAIL(reader.get_next_item(reader_buf, reader_len, addr))) {
    LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
  } else if (OB_FAIL(deserialize_block_ids(allocator, reader_buf, reader_len, reader_pos,
      data_block_ids, data_block_count))) {
    LOG_WARN("fail to deserialize data block id array", K(ret), K(reader_len), K(reader_pos));
  } else if (OB_FAIL(reader.get_next_item(reader_buf, reader_len, addr))) {
    LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
  } else if (FALSE_IT(reader_pos = 0)) {
  } else if (OB_FAIL(deserialize_block_ids(allocator, reader_buf, reader_len, reader_pos,
      other_block_ids, other_block_count))) {
    LOG_WARN("fail to deserialize other block id array", K(ret), K(reader_len), K(reader_pos));
  }
  return ret;
}

int64_t ObSSTableMacroInfo::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t payload_size = get_serialize_size_();
  OB_UNIS_ADD_LEN(MACRO_INFO_VERSION);
  OB_UNIS_ADD_LEN(payload_size);
  len += get_serialize_size_();
  return len;
}

int ObSSTableMacroInfo::get_data_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    if (OB_FAIL(iterator.init(data_block_ids_, data_block_count_))) {
      LOG_WARN("fail to init data block iterator", K(ret), K(data_block_count_));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::DATA_BLOCK, entry_id_))) {
    LOG_WARN("fail to init data block iterator", K(ret), K(entry_id_));
  }
  return ret;
}

int ObSSTableMacroInfo::get_other_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    if (OB_FAIL(iterator.init(other_block_ids_, other_block_count_))) {
      LOG_WARN("fail to init other block iterator", K(ret), K(other_block_count_));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::OTHER_BLOCK, entry_id_))) {
    LOG_WARN("fail to init other block iterator", K(ret), K(entry_id_));
  }
  return ret;
}

int64_t ObSSTableMacroInfo::get_serialize_size_() const
{
  int64_t len = 0;
  len += macro_meta_info_.get_serialize_size();
  len += entry_id_.get_serialize_size();
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    OB_UNIS_ADD_LEN_ARRAY(data_block_ids_, data_block_count_);
    OB_UNIS_ADD_LEN_ARRAY(other_block_ids_, other_block_count_);
  }
  len += serialization::encoded_length_bool(is_meta_root_);
  len += serialization::encoded_length_i64(nested_offset_);
  len += serialization::encoded_length_i64(nested_size_);
  return len;
}

DEF_TO_STRING(ObSSTableMacroInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(macro_meta_info),
       K_(data_block_count),
       K_(other_block_count),
       K_(linked_block_count),
       KP_(data_block_ids),
       KP_(other_block_ids),
       KP_(linked_block_ids),
       K_(entry_id),
       K_(is_meta_root),
       K_(nested_offset),
       K_(nested_size));
  J_OBJ_END();
  return pos;
}

int ObSSTableMacroInfo::write_block_ids(
    const common::ObIArray<MacroBlockId> &data_ids,
    const common::ObIArray<MacroBlockId> &other_ids,
    storage::ObLinkedMacroBlockItemWriter &writer,
    MacroBlockId &entry_id) const
{
  int ret = OB_SUCCESS;
  const bool need_disk_addr = false;
  const int64_t data_blk_cnt = data_ids.count();
  const int64_t other_blk_cnt = other_ids.count();
  if (OB_UNLIKELY(0 == data_blk_cnt && 0 == other_blk_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_blk_cnt and other_blk_cnt shouldn't be both 0", K(ret), K(data_blk_cnt),
        K(other_blk_cnt));
  } else if (OB_FAIL(writer.init(need_disk_addr))) {
    LOG_WARN("fail to initialize item writer", K(ret), K(need_disk_addr));
  } else if (OB_FAIL(flush_ids(data_ids, writer))) {
    LOG_WARN("fail to flush data block ids", K(ret), K(data_blk_cnt));
  } else if (OB_FAIL(flush_ids(other_ids, writer))) {
    LOG_WARN("fail to flush other block ids", KP(ret), K(other_blk_cnt));
  } else if (OB_FAIL(writer.close())) {
    LOG_WARN("fail to close block id writer", K(ret));
  } else {
    const ObIArray<MacroBlockId> &linked_block = writer.get_meta_block_list();
    entry_id = linked_block.at(linked_block.count() - 1);
  }
  return ret;
}

int ObSSTableMacroInfo::flush_ids(
    const common::ObIArray<MacroBlockId> &blk_ids,
    storage::ObLinkedMacroBlockItemWriter &writer)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = serialize_size_of_block_ids(blk_ids.get_data(), blk_ids.count());
  const ObMemAttr attr(MTL_ID(), ObModIds::OB_BUFFER);
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_len, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for writer buf", K(ret), K(buf_len));
  } else {
    OB_UNIS_ENCODE_ARRAY(blk_ids.get_data(), blk_ids.count());
    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer.write_item(buf, buf_len))) {
        LOG_WARN("fail to write block ids", K(ret), KP(buf), K(buf_len));
      }
    }
  }
  if (OB_NOT_NULL(buf)) {
    ob_free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObSSTableMacroInfo::deserialize_block_ids(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    MacroBlockId *&blk_ids,
    int64_t &blk_cnt)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_UNLIKELY(nullptr != blk_ids && 0 != blk_cnt)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("block id may be initialized", K(ret), KP(blk_ids), K(blk_cnt));
  } else {
    if (count > 0 && OB_ISNULL(blk_ids = static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId) * count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate block id", K(ret), K(count));
    } else {
      OB_UNIS_DECODE_ARRAY(blk_ids, count);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(blk_ids)) {
      allocator.free(blk_ids);
      blk_ids = nullptr;
    } else {
      blk_cnt = count;
    }
  }
  return ret;
}

int ObSSTableMacroInfo::deep_copy(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObSSTableMacroInfo &dest) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  const int64_t deep_size = get_variable_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_size + pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_size), K(pos));
  } else if (OB_FAIL(macro_meta_info_.deep_copy(buf, buf_len, pos, dest.macro_meta_info_))) {
    LOG_WARN("fail to deep copy macro meta info", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    if (OB_NOT_NULL(data_block_ids_)) {
      dest.data_block_ids_ = reinterpret_cast<MacroBlockId *>(buf + pos);
      MEMCPY(dest.data_block_ids_, data_block_ids_, sizeof(MacroBlockId) * data_block_count_);
      pos += sizeof(MacroBlockId) * data_block_count_;
    } else {
      dest.data_block_ids_ = nullptr;
    }
    dest.data_block_count_ = data_block_count_;
    if (OB_NOT_NULL(other_block_ids_)) {
      dest.other_block_ids_ = reinterpret_cast<MacroBlockId *>(buf + pos);
      MEMCPY(dest.other_block_ids_, other_block_ids_, sizeof(MacroBlockId) * other_block_count_);
      pos += sizeof(MacroBlockId) * other_block_count_;
    } else {
      dest.other_block_ids_ = nullptr;
    }
    dest.other_block_count_ = other_block_count_;
    if (OB_NOT_NULL(linked_block_ids_)) {
      dest.linked_block_ids_ = reinterpret_cast<MacroBlockId *>(buf + pos);
      MEMCPY(dest.linked_block_ids_, linked_block_ids_, sizeof(MacroBlockId) * linked_block_count_);
      pos += sizeof(MacroBlockId) * linked_block_count_;
    } else {
      dest.linked_block_ids_ = nullptr;
    }
    dest.linked_block_count_ = linked_block_count_;
    dest.entry_id_ = entry_id_;
    dest.is_meta_root_ = is_meta_root_;
    dest.nested_offset_ = nested_offset_;
    dest.nested_size_ = nested_size_;
  }
  return ret;
}

}
} // end namespace oceanbase
