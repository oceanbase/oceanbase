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

#include "ob_sstable_meta_info.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"

namespace oceanbase
{
namespace blocksstable
{

ObRootBlockInfo::ObRootBlockInfo()
  : addr_(),
    orig_block_buf_(nullptr),
    block_data_()
{
}

ObRootBlockInfo::~ObRootBlockInfo()
{
  reset();
}

bool ObRootBlockInfo::is_valid() const
{
  return addr_.is_valid() && (!addr_.is_memory() || block_data_.is_valid());
}

void ObRootBlockInfo::reset()
{
  orig_block_buf_ = nullptr;
  block_data_.reset();
  addr_.reset();
}

int ObRootBlockInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(ObMicroBlockData::DDL_BLOCK_TREE == block_data_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not serialize a ddl block tree", K(ret), K_(block_data));
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
    } else if (OB_UNLIKELY(len < tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else if (len > tmp_pos) {
      LOG_WARN("old server may deserialize value written by new server", K(ret), K(len), K(tmp_pos), KPC(this));
      pos += len;
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
    const ObMicroBlockData &block_data,
    const common::ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t offset = 0;
  char *orig_buf = nullptr;
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
  } else if (ObMicroBlockData::DDL_BLOCK_TREE == block_data.type_) {
    block_data_ = block_data;
    orig_block_buf_ = block_data_.buf_;
  } else if (size > 0 && OB_ISNULL(orig_buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(deep_copy_micro_buf(block_data.get_buf(), block_data.get_buf_size(), orig_buf, size, true/* need_deserialize_header*/))) {
    LOG_WARN("fail to deserialize micro header", K(ret), K(block_data));
  } else {
    orig_block_buf_ = orig_buf;
    block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
    if (ObStoreFormat::is_row_store_type_with_cs_encoding(row_store_type)) {
      if (OB_FAIL(transform_cs_encoding_data_buf_(&allocator, orig_block_buf_, size, block_data_.buf_, block_data_.size_))) {
        LOG_WARN("fail to transform_cs_encoding_data_buf_", K(ret), K(block_data));
      } else if (OB_FAIL(block_data_.prepare_micro_header())) {
        LOG_WARN("fail to prepare micro header", K(ret), K_(block_data));
      }
    } else if (OB_FAIL(block_data_.init_with_prepare_micro_header(orig_block_buf_, size))){
      LOG_WARN("failed to init block data", KR(ret), KP_(orig_block_buf), K(size));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(orig_buf)) {
      allocator.free(orig_buf);
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
    char *orig_buf = nullptr;
    const char *dst_buf = nullptr;
    int64_t dst_buf_size = 0;
    const ObMemAttr mem_attr(MTL_ID(), "RootBlkInfo");
    if (OB_ISNULL(orig_buf = static_cast<char *>(ob_malloc(addr_.size(), mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(addr_));
    } else if (OB_FAIL(read_block_data(addr_, orig_buf, addr_.size()))) {
      LOG_WARN("fail to read block data", K(ret), K(addr_));
    } else {
      ObMacroBlockReader reader;
      bool is_compressed = false;
      ObMicroBlockData decomp_micro_data;
      if (ObStoreFormat::is_row_store_type_with_cs_encoding(des_meta.row_store_type_)) {
        if (OB_FAIL(reader.decrypt_and_decompress_data(
            des_meta, orig_buf, addr_.size(), decomp_micro_data, is_compressed))) {
          LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), K_(addr));
        } else if (OB_FAIL(transform_cs_encoding_data_buf_(&allocator, decomp_micro_data.get_buf(), decomp_micro_data.get_buf_size(),
            dst_buf, dst_buf_size))) {
          LOG_WARN("fail to transform_cs_encoding_data_buf_", K(ret), K(des_meta));
        }
      } else if (OB_FAIL(reader.decrypt_and_decompress_data(des_meta, orig_buf,  // not cs encoding
          addr_.size(), decomp_micro_data, is_compressed, true, &allocator))) {
        LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), K_(addr));
      } else {
        dst_buf = decomp_micro_data.get_buf();
        dst_buf_size = decomp_micro_data.get_buf_size();
      }

      if (FAILEDx(block_data_.init_with_prepare_micro_header(dst_buf, dst_buf_size))) {
        LOG_WARN("fail to init block data", K(ret), KP(dst_buf), K(dst_buf_size));
      } else {
        block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
      }
    }
    if (OB_NOT_NULL(orig_buf)) {
      ob_free(orig_buf);
      orig_buf = nullptr;
    }
  }

  return ret;
}

int ObRootBlockInfo::transform_root_block_extra_buf(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (ObMicroBlockData::INDEX_BLOCK == block_data_.type_
      && OB_ISNULL(block_data_.get_extra_buf())
      && OB_NOT_NULL(block_data_.get_buf())) {
    ObIndexBlockDataTransformer transformer;
    char *allocated_buf = nullptr;
    if (OB_ISNULL(block_data_.get_micro_header())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null micro header", KR(ret), K_(block_data));
    } else if (addr_.is_memory()
        && 0 == block_data_.get_micro_header()->original_length_
        && 0 == block_data_.get_micro_header()->data_zlength_
        && 0 == block_data_.get_micro_header()->data_length_) {
      // For micro header bug in version before 4.3, when root block serialized in sstable metas
      // data length related fileds was lefted to be filled
      if (OB_FAIL(transformer.fix_micro_header_and_transform(block_data_, block_data_, allocator, allocated_buf))) {
        LOG_WARN("Fail to fix micro header and transform root block", K(ret), K_(block_data));
      }
    } else if (OB_FAIL(transformer.transform(block_data_, block_data_, allocator, allocated_buf))) {
      LOG_WARN("Fail to transform root block to memory format", K(ret), K_(block_data));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(allocated_buf)) {
        allocator.free(allocated_buf);
      }
    } else {
      block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
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
      || OB_UNLIKELY(addr.offset() >=OB_STORAGE_OBJECT_MGR.get_macro_block_size())
      || OB_UNLIKELY(0 == addr.size() || addr.size() >OB_STORAGE_OBJECT_MGR.get_macro_block_size())
      || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), KP(buf), K(buf_len));
  } else {
    blocksstable::ObStorageObjectHandle handle;
    blocksstable::ObStorageObjectReadInfo read_info;

    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.set_bypass_micro_cache(true);
    read_info.io_desc_.set_sys_module_id(ObIOModule::ROOT_BLOCK_IO);
    if (OB_FAIL(addr.get_block_addr(read_info.macro_block_id_, read_info.offset_, read_info.size_))) {
      LOG_WARN("fail to get block address", K(ret), K(addr));
    } else if (OB_FAIL(ObObjectManager::read_object(read_info, handle))) {
      LOG_WARN("fail to read block from macro block", K(ret), K(read_info));
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
  if (OB_UNLIKELY(!addr.is_valid())
      || OB_UNLIKELY(!addr.is_memory() && !addr.is_block() && !addr.is_none())
      || (OB_UNLIKELY(addr.is_memory()) && OB_ISNULL(orig_block_buf_))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < pos)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(addr), KP_(orig_block_buf), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(addr.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize root block address", K(ret), KP(buf), K(buf_len), K(pos), K(addr));
  } else if (addr.is_memory()) {
    ObMicroBlockHeader micro_header;
    int64_t tmp_pos = 0;
    if (OB_FAIL(micro_header.deserialize(orig_block_buf_, addr.size(), tmp_pos))) {
      LOG_WARN("fail to deserialize micro header", K(ret), KP(orig_block_buf_), K(addr.size()));
    } else {
      const char *data_buf = orig_block_buf_ + micro_header.header_size_;
      const int64_t data_size = addr.size() - micro_header.header_size_;
      if (OB_UNLIKELY(data_size <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data size", K(ret));
      } else if (OB_FAIL(micro_header.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize micro header", K(ret));
      } else {
        MEMCPY(buf + pos, data_buf, data_size);
        pos += data_size;
      }
    }
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
  const char *dst_buf = nullptr;
  char *orig_buf = nullptr;
  int64_t dst_buf_size = 0;
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
  } else if (addr_.is_block()) {
    const ObMemAttr mem_attr(MTL_ID(), "RootBlkInfo");
    char *orig_buf = nullptr;
    if (OB_ISNULL(orig_buf = static_cast<char *>(ob_malloc(addr_.size(), mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(addr_));
    } else if (OB_FAIL(read_block_data(addr_, orig_buf, addr_.size()))) {
      LOG_WARN("fail to read block data", K(ret), K(addr_));
    } else {
      ObMacroBlockReader reader;
      ObMicroBlockData decomp_micro_data;
      bool is_compressed = false;
      if (ObStoreFormat::is_row_store_type_with_cs_encoding(des_meta.row_store_type_)) {
        if (OB_FAIL(reader.decrypt_and_decompress_data(
            des_meta, orig_buf, addr_.size(), decomp_micro_data, is_compressed))) {
          LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), K_(addr));
        } else if (OB_FAIL(transform_cs_encoding_data_buf_(&allocator, decomp_micro_data.get_buf(), decomp_micro_data.get_buf_size(), dst_buf, dst_buf_size))) {
          LOG_WARN("fail to transform_cs_encoding_data_buf_", K(ret), K(des_meta));
        }
      } else if (OB_FAIL(reader.decrypt_and_decompress_data(des_meta, orig_buf,    // not cs encoding
          addr_.size(), block_data_, is_compressed, true, &allocator))) {
        LOG_WARN("fail to decrypt and decomp block", K(ret), K(des_meta), K_(addr));
      }
    }
    if (OB_NOT_NULL(orig_buf)) {
      ob_free(orig_buf);
      orig_buf = nullptr;
    }
  } else if (OB_UNLIKELY(pos + addr_.size() > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr_), K(data_len), K(pos));
  } else if (addr_.size() > 0 && OB_ISNULL(orig_buf = static_cast<char *>(allocator.alloc(addr_.size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc data buffer", K(ret), K(addr_));
  } else { // is mem addr
    if (OB_FAIL(deep_copy_micro_buf(buf + pos, addr_.size(), orig_buf, addr_.size(), true/* need_deserialize_header*/))) {
      LOG_WARN("failed to deep copy micro buf", K(ret), KP(buf), K(pos), KP(orig_buf), K_(addr));
    } else {
      orig_block_buf_ = orig_buf;
      if (ObStoreFormat::is_row_store_type_with_cs_encoding(des_meta.row_store_type_)) {
        if (OB_FAIL(transform_cs_encoding_data_buf_(&allocator, orig_block_buf_, addr_.size(), dst_buf, dst_buf_size))) {
          LOG_WARN("fail to transform_cs_encoding_data_buf_", K(ret), K(addr_), K(pos));
        } else {
          pos += addr_.size();
        }
      } else {
        dst_buf = orig_block_buf_;
        dst_buf_size = addr_.size();
        pos += addr_.size();
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(orig_buf)) {
        allocator.free(orig_buf);
        orig_block_buf_ = nullptr;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!block_data_.is_valid() && OB_FAIL(block_data_.init_with_prepare_micro_header(dst_buf, dst_buf_size))) {
    LOG_WARN("failed to init block data", KR(ret), KP(dst_buf), K(dst_buf_size));
  } else {
    block_data_.type_ = ObMicroBlockData::Type::INDEX_BLOCK;
  }

  return ret;
}

int ObRootBlockInfo::transform_cs_encoding_data_buf_(
    common::ObIAllocator *allocator,
    const char *buf,
    const int64_t buf_size,
    const char *&dst_buf,
    int64_t &dst_buf_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMicroBlockHeader header;
  if (OB_FAIL(header.deserialize_and_check_header(buf, buf_size))) {
    LOG_WARN("Fail to deserialize record header", K(ret));
  } else if (OB_UNLIKELY(!ObStoreFormat::is_row_store_type_with_cs_encoding(static_cast<ObRowStoreType>(header.row_store_type_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row_store_type mismatch", K(ret), K(header));
  } else {
    const char *payload_buf = buf + header.header_size_;
    const int64_t payload_size = buf_size - header.header_size_;
    if (OB_FAIL(ObCSMicroBlockTransformer::full_transform_block_data(
        header, payload_buf, payload_size, dst_buf, dst_buf_size, allocator))) {
      LOG_WARN("fail to full_transform_block_data", K(ret), K(header), KP(payload_buf), K(payload_size));
    }
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
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(variable_size), K(pos), K(block_data_));
  } else if (ObMicroBlockData::DDL_BLOCK_TREE == block_data_.type_) {
    dest.block_data_ = block_data_;
    dest.addr_ = addr_;
    dest.orig_block_buf_ = nullptr;
  } else {
    dest.addr_ = addr_;
    if (OB_NOT_NULL(block_data_.buf_)) {
      if (OB_FAIL(deep_copy_micro_buf(block_data_.buf_, block_data_.size_, buf + pos, buf_len - pos, false))) {
        LOG_WARN("failed to deep copy micro block buf", K(ret), K_(block_data), KP(buf), K(pos));
      } else {
        dest.block_data_.buf_ = buf + pos;
        dest.block_data_.size_ = block_data_.size_;
        pos += block_data_.size_;
      }

      if (OB_FAIL(ret)) {
      } else if (orig_block_buf_ == block_data_.buf_) {
        dest.orig_block_buf_ = dest.block_data_.buf_ ;
      } else if (orig_block_buf_ != nullptr) {
        if (!addr_.is_memory()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("must be memory addr", K(ret), K(addr_), KP(orig_block_buf_), K(block_data_));
        } else if (OB_FAIL(deep_copy_micro_buf(orig_block_buf_, addr_.size(), buf + pos, buf_len - pos, false))) {
          LOG_WARN("failed to deep copy orig micro block buf", K(ret));
        } else {
          dest.orig_block_buf_ = buf + pos;
          pos += addr_.size();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(block_data_.extra_buf_)) {
        ObIndexBlockDataTransformer transformer;
        const ObIndexBlockDataHeader *src_idx_header
            = reinterpret_cast<const ObIndexBlockDataHeader *>(block_data_.get_extra_buf());
        char *extra_buf = buf + pos;
        int64_t extra_start_pos = pos;
        ObIndexBlockDataHeader *dst_idx_header = new (extra_buf) ObIndexBlockDataHeader();
        pos += sizeof(ObIndexBlockDataHeader);
        if (OB_FAIL(dst_idx_header->deep_copy_transformed_index_block(
            *src_idx_header, buf_len, buf, pos))) {
          LOG_WARN("fail to deep copy transformed index block", K(ret));
        } else {
          dest.block_data_.extra_buf_ = extra_buf;
          dest.block_data_.extra_size_ = pos - extra_start_pos;
        }
      }
      dest.block_data_.type_ = block_data_.type_;
    }
  }
  if (OB_FAIL(ret) || !dest.block_data_.is_valid()) {
  } else if (OB_FAIL(dest.block_data_.prepare_micro_header())) {
    LOG_WARN("fail to prepare micro header", K(ret));
  }
  return ret;
}

int ObRootBlockInfo::deep_copy_micro_buf(
  const char *src_buf,
  const int64_t src_buf_len,
  char *dst_buf,
  const int64_t dst_buf_len,
  bool need_deserialize_header) const
{
  int ret = OB_SUCCESS;
  int64_t copy_pos = 0;
  if (OB_ISNULL(src_buf) || OB_ISNULL(dst_buf) || OB_UNLIKELY(dst_buf_len < src_buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_buf), KP(dst_buf), K(dst_buf_len), K(src_buf_len));
  } else {
    ObMicroBlockHeader deserialized_header;
    const ObMicroBlockHeader *micro_header = nullptr;
    ObMicroBlockHeader *copied_micro_haeder = nullptr;
    int64_t header_offset = 0;
    if (need_deserialize_header) {
      int64_t des_pos = 0;
      if (OB_FAIL(deserialized_header.deserialize(src_buf, src_buf_len, des_pos))) {
        LOG_WARN("failed to deserialize micro block header", K(ret));
      } else {
        micro_header = &deserialized_header;
        header_offset = des_pos;
      }
    } else {
      micro_header = reinterpret_cast<const ObMicroBlockHeader *>(src_buf);
      header_offset = micro_header->get_serialize_size();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(micro_header) || OB_UNLIKELY(!micro_header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid micro block header", K(ret), KPC(micro_header));
    } else if (OB_FAIL(micro_header->deep_copy(dst_buf, dst_buf_len, copy_pos, copied_micro_haeder))) {
      LOG_WARN("failed to deep copy micro block header", K(ret));
    } else if (OB_UNLIKELY(copy_pos != header_offset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected micro block header copy offset not match", K(ret));
    } else {
      MEMCPY(dst_buf + copy_pos, src_buf + header_offset, src_buf_len - header_offset);
    }
  }
  return ret;
}

ObMacroIdIterator::ObMacroIdIterator()
  : target_type_(Type::NONE),
    iter_elements_(),
    element_idx_(0),
    element_pos_(0),
    iter_cnt_(0),
    total_count_(0),
    is_inited_(false),
    allocator_("MacroIdIter")
{
  iter_elements_.set_attr(ObMemAttr(MTL_ID(), "MacroIdIter"));
}

int ObMacroIdIterator::init(const Type type, const MacroBlockId &entry_id)
{
  int ret = OB_SUCCESS;
  MacroBlockId *data_blk_ids = nullptr;
  int64_t data_blk_cnt = 0;
  MacroBlockId *other_blk_ids = nullptr;
  int64_t other_blk_cnt = 0;
  MacroBlockId *linked_blk_ids = nullptr;
  int64_t linked_blk_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(type > Type::MAX || !entry_id.is_valid() || total_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), K(entry_id), K(total_count_));
  } else if (OB_FAIL(ObSSTableMacroInfo::read_block_ids(entry_id,
                                                        allocator_,
                                                        data_blk_ids, data_blk_cnt,
                                                        other_blk_ids, other_blk_cnt,
                                                        linked_blk_ids, linked_blk_cnt))) {
    LOG_WARN("fail to read block ids", K(ret), K(entry_id));
  } else {
    switch (type) {
      case Type::DATA_BLOCK: {
        if (data_blk_cnt > 0
            && OB_FAIL(iter_elements_.push_back(IterElement(data_blk_ids, data_blk_cnt, Type::DATA_BLOCK)))) {
          LOG_WARN("fail to push back iter element", K(ret));
        } else {
          total_count_ = data_blk_cnt;
        }
      } break;
      case Type::OTHER_BLOCK: {
        if (other_blk_cnt > 0
            && OB_FAIL(iter_elements_.push_back(IterElement(other_blk_ids, other_blk_cnt, Type::OTHER_BLOCK)))) {
          LOG_WARN("fail to push back iter element", K(ret));
        } else {
          total_count_ = other_blk_cnt;
        }
      } break;
      case Type::LINKED_BLOCK: {
        if (linked_blk_cnt <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("linked_blk_cnt <=0 is unexpected when macro info is not inlined", K(ret), K(linked_blk_cnt),
            KP(linked_blk_ids), K(entry_id));
        } else if (OB_ISNULL(linked_blk_ids)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unepxected null linked_blk_ids", K(ret), K(linked_blk_cnt),
              KP(linked_blk_ids), K(entry_id));
         } else if (OB_FAIL(iter_elements_.push_back(IterElement(linked_blk_ids, linked_blk_cnt, Type::LINKED_BLOCK)))) {
            LOG_WARN("fail to push back iter element", K(ret));
         } else {
            total_count_ = linked_blk_cnt;
         }
      } break;
      case Type::MAX: {
         if (data_blk_cnt > 0
             && OB_FAIL(iter_elements_.push_back(IterElement(data_blk_ids, data_blk_cnt, Type::DATA_BLOCK)))) {
            LOG_WARN("fail to push back iter element", K(ret));
         } else if (other_blk_cnt > 0
                    && OB_FAIL(iter_elements_.push_back(IterElement(other_blk_ids, other_blk_cnt, Type::OTHER_BLOCK)))) {
            LOG_WARN("fail to push back iter element", K(ret));
         } else if (linked_blk_cnt <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("linked_blk_cnt <=0 is unexpected when macro info is not inlined", K(ret), K(linked_blk_cnt),
              KP(linked_blk_ids), K(entry_id));
         } else if (OB_ISNULL(linked_blk_ids)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unepxected null linked_blk_ids", K(ret), K(linked_blk_cnt),
              KP(linked_blk_ids), K(entry_id));
         } else if (OB_FAIL(iter_elements_.push_back(IterElement(linked_blk_ids, linked_blk_cnt, Type::LINKED_BLOCK)))) {
            LOG_WARN("fail to push back iter element", K(ret));
         } else {
            total_count_ = data_blk_cnt + other_blk_cnt + linked_blk_cnt;
         }
      } break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid block type", K(ret), K(type));
      }
    }

    if (OB_SUCC(ret)) {
      target_type_ = type;
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObMacroIdIterator::init(
    const MacroBlockId *ptr,
    const int64_t count,
    const Type block_type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (OB_UNLIKELY(count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid count", K(ret), KP(ptr), K(count));
  } else if (OB_UNLIKELY(!is_block_type_valid(block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block type", K(ret), K(block_type));
  } else if (count > 0 && OB_ISNULL(ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null ptr", K(ret), KP(ptr), K(count));
  } else if (count > 0 && OB_FAIL(iter_elements_.push_back(IterElement(ptr, count, block_type)))) {
    LOG_WARN("fail to push back iter element", K(ret));
  } else {
    target_type_ = block_type;
    total_count_ = count;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObMacroIdIterator::init(
    MacroBlockId *data_blk_ids,
    const int64_t data_blk_cnt,
    MacroBlockId *other_blk_ids,
    const int64_t other_blk_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret));
  } else if (data_blk_cnt > 0) {
    if (OB_ISNULL(data_blk_ids)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("data_blk_ids can not be nullptr if data_blk_cnt > 0", K(ret),
        K(data_blk_cnt), KP(data_blk_ids));
    } else if (OB_FAIL(iter_elements_.push_back(IterElement(data_blk_ids, data_blk_cnt, Type::DATA_BLOCK)))) {
      LOG_WARN("fail to push back iter element", K(ret));
    } else {
      total_count_ += data_blk_cnt;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (other_blk_cnt > 0) {
    if (OB_ISNULL(other_blk_ids)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("other_blk_ids can not be nullptr if other_blk_cnt > 0", K(ret),
        K(other_blk_cnt), KP(other_blk_ids));
    } else if (OB_FAIL(iter_elements_.push_back(IterElement(other_blk_ids, other_blk_cnt, Type::OTHER_BLOCK)))) {
      LOG_WARN("fail to push back iter element", K(ret));
    } else {
      total_count_ += other_blk_cnt;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    target_type_ = Type::MAX;
    is_inited_ = true;
  }
  return ret;
}

void ObMacroIdIterator::reset()
{
  target_type_ = Type::NONE;
  iter_elements_.reset();
  element_idx_ = 0;
  element_pos_ = 0;
  iter_cnt_ = 0;
  total_count_ = 0;
  is_inited_ = false;
  allocator_.reuse();
}

int ObMacroIdIterator::get_next_macro_id(
    MacroBlockId &macro_id,
    Type &block_type)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  block_type = Type::MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (iter_cnt_ == total_count_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(this));
  } else {
    const IterElement *element_ptr = nullptr;
    if (element_pos_ < iter_elements_.at(element_idx_).count_) {
      element_ptr = &iter_elements_.at(element_idx_);
    } else if (OB_FAIL(get_next_element_(element_ptr))) {
      LOG_WARN("fail to get next element", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(element_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null element_ptr", K(ret), KPC(this));
    } else if (OB_UNLIKELY(!element_ptr->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid element", K(ret), KPC(element_ptr));
    } else if (OB_UNLIKELY(element_pos_ >= element_ptr->count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("element pos out of range", K(ret), K_(element_pos), KPC(element_ptr));
    } else {
      macro_id = element_ptr->value_ptr_[element_pos_];
      block_type = element_ptr->block_type_;
      ++element_pos_;
      ++iter_cnt_;
    }
  }
  return ret;
}

int ObMacroIdIterator::get_next_macro_id(MacroBlockId &macro_id)
{
  Type unused_block_type = Type::MAX;
  return get_next_macro_id(macro_id, unused_block_type);
}

int ObMacroIdIterator::get_block_count(const Type block_type, int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_block_type_valid(block_type)
                         && Type::MAX != block_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block type", K(ret), K(block_type));
  } else if (block_type == target_type_) {
    count = total_count_;
  } else if (Type::MAX == target_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter_elements_.count(); ++i) {
      const IterElement &element = iter_elements_.at(i);
      if (OB_UNLIKELY(!element.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid element", K(ret), K(element));
      } else if (element.block_type_ == block_type) {
        count = element.count_;
        break;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iterator doesn't contain specified block type", K(ret), K(block_type),
      K_(target_type));
  }
  return ret;
}

int ObMacroIdIterator::get_next_element_(/*out*/const IterElement *&element_ptr)
{
  int ret = OB_SUCCESS;
  element_ptr = nullptr;
  ++element_idx_;

  if (OB_UNLIKELY(element_idx_ >= iter_elements_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("element idx out of range", K(ret), K_(element_idx), K(iter_elements_.count()));
  } else {
    element_ptr = &iter_elements_.at(element_idx_);
    element_pos_ = 0;
  }
  return ret;
}

ObSSTableMacroInfo::ObSSTableMacroInfo()
  : macro_meta_info_(),
    data_block_ids_(nullptr),
    other_block_ids_(nullptr),
    linked_block_ids_(nullptr),
    data_block_count_(-1),
    other_block_count_(-1),
    linked_block_count_(-1),
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
  // NOTE:
  //  If the allocator were not ObArenaAllocator any more,
  //  other_block_ids_ and data_block_ids_ need be free manually, such as persist_block_ids(...)
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(macro_meta_info_.init_root_block_info(allocator,
      param.data_block_macro_meta_addr_, param.data_block_macro_meta_, param.root_row_store_type_))) {
    LOG_WARN("fail to init macro meta info", K(ret), K(param));
  } else if (FALSE_IT(data_block_count_ = param.data_block_ids_.count())) {
  } else if (FALSE_IT(other_block_count_ = param.other_block_ids_.count())) {
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
    linked_block_count_ = 0;
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
  data_block_count_ = -1;
  if (nullptr != other_block_ids_) {
    for (int64_t i = 0; i < other_block_count_; i++) {
      other_block_ids_[i].~MacroBlockId();
    }
    other_block_ids_ = nullptr;
  }
  other_block_count_ = -1;
  if (nullptr != linked_block_ids_) {
    for (int64_t i = 0; i < linked_block_count_; i++) {
      linked_block_ids_[i].~MacroBlockId();
    }
    linked_block_ids_ = nullptr;
  }
  linked_block_count_ = -1;
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
    if (OB_UNLIKELY(data_block_count_ < 0 || other_block_count_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block count", K(ret), K_(data_block_count), K_(other_block_count));
    } else {
      OB_UNIS_ENCODE_ARRAY(data_block_ids_, data_block_count_);
      OB_UNIS_ENCODE_ARRAY(other_block_ids_, other_block_count_);
    }
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
    common::ObArenaAllocator &allocator,
    const ObLinkedMacroInfoWriteParam &param,
    int64_t &macro_start_seq,
    ObSharedObjectsWriteCtx &linked_block_write_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemWriter block_writer;
  if (OB_FAIL(write_block_ids(param, block_writer, entry_id_, macro_start_seq))) {
    LOG_WARN("fail to write other block ids", K(ret), K(macro_start_seq));
  } else if (OB_FAIL(save_linked_block_list(block_writer.get_meta_block_list(), allocator))) {
    LOG_WARN("fail to save linked block ids", K(ret));
  } else {
    ObMetaDiskAddr addr;
    addr.set_block_addr(ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK,
                        0, /*offset*/
                        1, /*size*/
                        ObMetaDiskAddr::DiskType::BLOCK); // unused;
    linked_block_write_ctx.set_addr(addr);
    for (int64_t i = 0;
         OB_SUCC(ret) && i < block_writer.get_meta_block_list().count();
         i++) {
      if (OB_FAIL(linked_block_write_ctx.add_object_id(block_writer.get_meta_block_list().at(i)))) {
        LOG_WARN("fail to push_back macro_block", K(ret), K(i), K(block_writer.get_meta_block_list()));
      }
    }
    if (OB_SUCC(ret)) {
      // data_block_ids_ and other_block_ids are allocated by ArenaAllocator, need not free
      data_block_ids_ = nullptr;
      other_block_ids_ = nullptr;
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (!block_writer.is_closed() && OB_TMP_FAIL(block_writer.close())) {
      LOG_WARN("fail to close block_writer", K(tmp_ret));
    }
    for (int64_t i = 0; i < block_writer.get_meta_block_list().count(); ++i) {
      if (OB_TMP_FAIL(linked_block_write_ctx.add_object_id(block_writer.get_meta_block_list().at(i)))) {
        LOG_WARN("fail to push_back macro_block", K(tmp_ret), K(i));
      }
    }
  }
  return ret;
}

/// COMMENT: this method is called right after ObSSTable::init(which will also init macro info),
/// and there is no linked block when macro info is just inited.
/// Maybe this method is meaningless?
void ObSSTableMacroInfo::dec_linked_block_ref_cnt()
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (GCTX.is_shared_storage_mode()) {
    // do nothing in SS mode
  } else if (OB_UNLIKELY(linked_block_count_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected linked block count", K(ret), K(linked_block_count_));
  } else if (0 == linked_block_count_) {
    // skip the decrease
  } else if (OB_ISNULL(linked_block_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("linked_block_ids is null, but linked_block_count_ is not 0", K(linked_block_count_));
  } else {
    for (; idx < linked_block_count_; idx++) {
      const MacroBlockId &macro_id = linked_block_ids_[idx];
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_id))) {
        LOG_ERROR("fail to decrease macro block ref cnt", K(ret), K(macro_id));
      }
    }
  }
}

/*static*/int ObSSTableMacroInfo::save_linked_block_list_(
    const common::ObIArray<MacroBlockId> &list,
    common::ObArenaAllocator &allocator,
    /*out*/MacroBlockId *&linked_block_ids,
    /*out*/int64_t &linked_block_count)
{
  int ret = OB_SUCCESS;
  linked_block_ids = nullptr;
  linked_block_count = 0;
  const int64_t ids_cnt = list.count();
  if (ids_cnt > 0 && OB_ISNULL(linked_block_ids = static_cast<MacroBlockId *>(allocator.alloc(
      sizeof(MacroBlockId) * ids_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(ids_cnt));
  } else {
    int64_t idx = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < ids_cnt; ++idx) {
      new (linked_block_ids + idx) MacroBlockId(list.at(idx));
    }
    linked_block_count = ids_cnt;
  }
  return ret;
}

int ObSSTableMacroInfo::save_linked_block_list(
    const common::ObIArray<MacroBlockId> &list,
    common::ObArenaAllocator &allocator)
{
  return save_linked_block_list_(list, allocator, linked_block_ids_, linked_block_count_);
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
    } else if (OB_UNLIKELY(len < tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), K(*this));
    } else if (len > tmp_pos) {
      LOG_WARN("old server may deserialize value written by new server", K(ret), K(len), K(tmp_pos), K(*this));
      pos += len;
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
    // do nothing
    LOG_DEBUG("some of macro info members are not inlined, skip them", K(ret), K_(entry_id));
  } else {
    if (pos < data_len && OB_FAIL(deserialize_block_ids(allocator, buf, data_len, pos,
        data_block_ids_, data_block_count_))) {
      LOG_WARN("fail to deserialize data block ids", K(ret), KP(buf), K(data_len), K(pos));
    } else if (pos < data_len && OB_FAIL(deserialize_block_ids(allocator, buf, data_len, pos,
        other_block_ids_, other_block_count_))) {
      LOG_WARN("fail to deserialize other block ids", K(ret), KP(buf), K(data_len), K(pos));
    } else {
      linked_block_count_ = 0;
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
    int64_t &other_blk_cnt,
    MacroBlockId *&linked_blk_ids,
    int64_t &linked_blk_cnt)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader block_reader;
  ObMemAttr mem_attr(MTL_ID(), "SSTableBlockId");
  if (OB_UNLIKELY(!entry_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(entry_id));
  } else if (OB_FAIL(block_reader.init(entry_id, mem_attr))) {
    LOG_WARN("fail to initialize reader", K(ret), K(entry_id));
  } else if (OB_FAIL(read_block_ids(allocator,
                                    block_reader,
                                    data_blk_ids, data_blk_cnt,
                                    other_blk_ids, other_blk_cnt,
                                    linked_blk_ids, linked_blk_cnt))) {
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
    int64_t &other_block_count,
    MacroBlockId *&linked_block_ids,
    int64_t &linked_block_count)
{
  int ret = OB_SUCCESS;
  data_block_ids = other_block_ids = linked_block_ids =  nullptr;
  data_block_count = other_block_count = linked_block_count = 0;
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
  } else if (OB_FAIL(save_linked_block_list_(reader.get_meta_block_list(),
                                             allocator,
                                             linked_block_ids,
                                             linked_block_count))) {
    LOG_WARN("fail to get link block list", K(ret));
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

int ObSSTableMacroInfo::get_block_count(
    /*out*/int64_t &data_blk_cnt,
    /*out*/int64_t &other_blk_cnt,
    /*out*/int64_t &linked_blk_cnt) const
{
  int ret = OB_SUCCESS;
  data_blk_cnt = other_blk_cnt = linked_blk_cnt = -1;

  if (IS_EMPTY_BLOCK_LIST(entry_id_)) {
    if (OB_UNLIKELY(data_block_count_ < 0 || other_block_count_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block count", K(ret), K_(data_block_count), K_(other_block_count));
    } else {
      data_blk_cnt = data_block_count_;
      other_blk_cnt = other_block_count_;
      linked_blk_cnt = 0;
    }
  } else if (OB_FAIL(load_members_by_entry_block_(entry_id_,
                                                  /*out*/data_blk_cnt,
                                                  /*out*/other_blk_cnt,
                                                  /*out*/linked_blk_cnt))) {
    LOG_WARN("fail to load block counts", K(ret), K_(entry_id));
  }
  return ret;
}

int ObSSTableMacroInfo::get_data_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    if (OB_FAIL(iterator.init(data_block_ids_, data_block_count_, ObMacroIdIterator::Type::DATA_BLOCK))) {
      LOG_WARN("fail to init data block iterator", K(ret), K_(data_block_count),
        K_(data_block_ids));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::DATA_BLOCK, entry_id_))) {
    LOG_WARN("fail to init data block iterator", K(ret), K_(entry_id));
  }
  return ret;
}

int ObSSTableMacroInfo::get_other_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    if (OB_FAIL(iterator.init(other_block_ids_, other_block_count_, ObMacroIdIterator::Type::OTHER_BLOCK))) {
      LOG_WARN("fail to init other block iterator", K(ret), K_(other_block_count),
        KP_(other_block_ids));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::OTHER_BLOCK, entry_id_))) {
    LOG_WARN("fail to init other block iterator", K(ret), K_(entry_id));
  }
  return ret;
}

int ObSSTableMacroInfo::get_linked_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    // inline macro info has no linked blocks
    if (linked_block_count_ > 0 || OB_NOT_NULL(linked_block_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inline macro info has no linked blocks", K(ret), K_(linked_block_count),
        KP_(linked_block_ids));
    } else if (OB_FAIL(iterator.init(linked_block_ids_, linked_block_count_, ObMacroIdIterator::Type::LINKED_BLOCK))) {
      LOG_WARN("fail to init linked block iterator", K(ret), K_(linked_block_count),
        KP_(linked_block_ids));
    }
  } else if (OB_NOT_NULL(linked_block_ids_)) {
    // this may happens when macro info has just been persisted.
    if (OB_UNLIKELY(linked_block_count_ <= 0)) {
      LOG_WARN("linked_block_ids_ is not nullptr but linked block count is less than 0",
        K(ret), K_(linked_block_count), KP_(linked_block_ids));
    } else if (OB_FAIL(iterator.init(linked_block_ids_, linked_block_count_, ObMacroIdIterator::Type::LINKED_BLOCK))) {
      LOG_WARN("fail to init linked block iterator", K(ret), K_(linked_block_count),
        K_(linked_block_ids));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::LINKED_BLOCK, entry_id_))) {
    LOG_WARN("fail to init all block iterator", K(ret), K_(entry_id));
  }
  return ret;
}

int ObSSTableMacroInfo::get_all_block_iter(ObMacroIdIterator &iterator) const
{
  int ret = OB_SUCCESS;
  if (ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK == entry_id_) {
    if (OB_FAIL(iterator.init(data_block_ids_, data_block_count_,
                                     other_block_ids_, other_block_count_))) {
      LOG_WARN("fail to init all block iterator", K(ret), KP_(data_block_ids),
        K_(data_block_count), KP_(other_block_ids), K_(other_block_count));
    }
  } else if (OB_FAIL(iterator.init(ObMacroIdIterator::MAX, entry_id_))) {
    LOG_WARN("fail to init all block iterator", K(ret), K_(entry_id));
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
    const ObLinkedMacroInfoWriteParam &param,
    storage::ObLinkedMacroBlockItemWriter &writer,
    MacroBlockId &entry_id,
    int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "SSTableBlockId");
  if (OB_UNLIKELY(data_block_count_ <= 0 && other_block_count_ <= 0) ||
      OB_UNLIKELY((0 != data_block_count_ && OB_ISNULL(data_block_ids_)) ||
      OB_UNLIKELY((0 != other_block_count_ && OB_ISNULL(other_block_ids_)))) ||
      OB_UNLIKELY(!param.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_block_count_ and other_block_count_ shouldn't be both 0", K(ret), K(data_block_count_),
        K(other_block_count_), K(param));
  } else if (OB_FAIL(writer.init_for_macro_info(param))) {
    LOG_WARN("fail to initialize item writer", K(ret), K(param));
  } else if (OB_FAIL(flush_ids(data_block_ids_, data_block_count_, writer))) {
    LOG_WARN("fail to flush data block ids", K(ret), K(data_block_count_));
  } else if (OB_FAIL(flush_ids(other_block_ids_, other_block_count_, writer))) {
    LOG_WARN("fail to flush other block ids", KP(ret), K(other_block_count_));
  } else if (OB_FAIL(writer.close())) {
    LOG_WARN("fail to close block id writer", K(ret));
  } else {
    macro_start_seq += writer.get_written_macro_cnt();
    const ObIArray<MacroBlockId> &linked_block = writer.get_meta_block_list();
    entry_id = linked_block.at(linked_block.count() - 1);
  }
  return ret;
}

int ObSSTableMacroInfo::flush_ids(
    const MacroBlockId *blk_ids,
    const int64_t blk_cnt,
    storage::ObLinkedMacroBlockItemWriter &writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == blk_ids && 0 != blk_cnt) || OB_UNLIKELY(nullptr != blk_ids && blk_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("blk_ids should not be nullptr", KR(ret), KP(blk_ids), K(blk_cnt));
  } else {
    int64_t len = 0;
    OB_UNIS_ADD_LEN_ARRAY(blk_ids, blk_cnt);
    const ObMemAttr attr(MTL_ID(), ObModIds::OB_BUFFER);
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(len, attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for writer buf", K(ret), K(len));
    } else {
      int64_t buf_len = len;
      OB_UNIS_ENCODE_ARRAY(blk_ids, blk_cnt);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(writer.write_item(buf, buf_len))) {
          LOG_WARN("fail to write block ids", K(ret), KP(buf), K(len));
        }
      }
    }
    if (OB_NOT_NULL(buf)) {
      ob_free(buf);
      buf = nullptr;
    }
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

int ObSSTableMacroInfo::expand_block_ids(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (entry_id_ != ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK) {
    ObMacroIdIterator data_block_iter;
    ObMacroIdIterator other_block_iter;
    if (OB_FAIL(get_data_block_iter(data_block_iter))) {
      LOG_WARN("fail to get data block iterator", K(ret));
    } else if (OB_FAIL(get_other_block_iter(other_block_iter))) {
      LOG_WARN("fail to get other block iterator", K(ret));
    } else {
      MacroBlockId *data_block_ids = static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId) * data_block_count_));
      MacroBlockId *other_block_ids = static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId) * other_block_count_));
      if (OB_ISNULL(data_block_ids) || OB_ISNULL(other_block_ids)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for data block ids", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_block_count_; ++i) {
          new (data_block_ids + i) MacroBlockId();
          if (OB_FAIL(data_block_iter.get_next_macro_id(*(data_block_ids + i)))) {
            LOG_WARN("fail to get next macro id", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < other_block_count_; ++i) {
          new (other_block_ids + i) MacroBlockId();
          if (OB_FAIL(other_block_iter.get_next_macro_id(*(other_block_ids + i)))) {
            LOG_WARN("fail to get next macro id", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(data_block_ids)) {
          allocator.free(data_block_ids);
        }
        if (OB_NOT_NULL(other_block_ids)) {
          allocator.free(other_block_ids);
        }
      } else {
        data_block_ids_ = data_block_ids;
        other_block_ids_ = other_block_ids;
        linked_block_ids_ = nullptr;
        linked_block_count_ = 0;
        entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
      }
    }
  }
  return ret;
}

/*static*/int ObSSTableMacroInfo::load_members_by_entry_block_(
    const MacroBlockId &entry_block_id,
    /*out*/int64_t &data_blk_cnt,
    /*out*/int64_t &other_blk_cnt,
    /*out*/int64_t &linked_blk_cnt)
{
  int ret = OB_SUCCESS;
  data_blk_cnt = other_blk_cnt = linked_blk_cnt = -1;
  ObLinkedMacroBlockItemReader block_reader;
  ObMetaDiskAddr addr;
  char *reader_buf = nullptr;
  int64_t pos = 0;
  int64_t reader_len = 0;
  ObMemAttr mem_attr(MTL_ID(), "SSTableBlockId");

  if (OB_UNLIKELY(!entry_block_id.is_valid() || IS_EMPTY_BLOCK_LIST(entry_block_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry block id", K(ret), K(entry_block_id));
  } else if (OB_FAIL(block_reader.init(entry_block_id, mem_attr))) {
    LOG_WARN("fail to initialize reader", K(ret), K(entry_block_id));
  } else if (OB_FAIL(block_reader.get_next_item(reader_buf, reader_len, addr))) { // read data ids
    LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
  } else if (OB_FAIL(serialization::decode(reader_buf, reader_len, pos, data_blk_cnt))) {
    LOG_WARN("fail to deserialize data block ids", K(ret));
  } else if (OB_FAIL(block_reader.get_next_item(reader_buf, reader_len, addr))) { // read other ids
    LOG_WARN("fail to get next item", K(ret), K(reader_len), K(addr));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(serialization::decode(reader_buf, reader_len, pos, other_blk_cnt))) {
    LOG_WARN("fail to deserialize other block ids", K(ret));
  } else {
    linked_blk_cnt = block_reader.get_meta_block_list().count();
  }
  return ret;
}
} // end namespace blocksstable
} // end namespace oceanbase
