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

#include "ob_pg_meta_block_reader.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_pg_meta_block_writer.h"
#include "blocksstable/ob_store_file.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGMetaBlockReader::ObPGMetaBlockReader()
    : is_inited_(false),
      io_desc_(),
      handle_pos_(0),
      macros_handle_(),
      prefetch_macro_block_idx_(0),
      read_macro_block_idx_(0),
      file_handle_()
{
  handles_[0].reset();
  handles_[1].reset();
}

int ObPGMetaBlockReader::init(const MacroBlockId& entry_block, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaBlockReader has been inited twice", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret), K(ret));
  } else if (FALSE_IT(handles_[0].set_file(file_handle.get_storage_file()))) {
  } else if (FALSE_IT(handles_[1].set_file(file_handle.get_storage_file()))) {
  } else if (FALSE_IT(macros_handle_.set_storage_file(file_handle.get_storage_file()))) {
    LOG_WARN("fail to set file handle", K(ret), K(ret));
  } else if (OB_FAIL(get_meta_blocks(entry_block))) {
    LOG_WARN("fail to get meta blocks", K(ret));
  } else if (OB_FAIL(prefetch_block())) {
    LOG_WARN("fail to prefetch block", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaBlockReader::get_meta_blocks(const MacroBlockId& entry_block)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  ObMacroBlockReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ = common_header.get_serialize_size() + sizeof(ObLinkedMacroBlockHeaderV2);
  read_info.io_desc_ = io_desc_;

  if (entry_block.second_id() > 0) {
    blocksstable::ObStorageFile* file = NULL;
    ObMacroBlockCtx macro_block_ctx;
    macro_block_ctx.sstable_block_id_.macro_block_id_ = entry_block;
    read_info.macro_block_ctx_ = &macro_block_ctx;
    int64_t handle_pos = 0;
    MacroBlockId previous_block_id;
    handles_[handle_pos].reset();
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret), K_(file_handle));
    } else if (FALSE_IT(handles_[handle_pos_].set_file(file))) {
    } else if (OB_FAIL(file->async_read_block(read_info, handles_[handle_pos]))) {
      LOG_WARN("fail to async read block", K(ret));
    } else {
      const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(handles_[handle_pos].wait(io_timeout_ms))) {
          LOG_WARN("fail to wait io finish", K(ret));
        } else if (OB_FAIL(macros_handle_.add(macro_block_ctx.get_macro_block_id()))) {
          LOG_WARN("fail to push macro block id", K(ret));
        } else if (OB_FAIL(get_previous_block_id(
                       handles_[handle_pos].get_buffer(), handles_[handle_pos].get_data_size(), previous_block_id))) {
          LOG_WARN("fail to get previous block index", K(ret));
        } else {
          if (previous_block_id.second_id() > 0) {
            handle_pos = 1 - handle_pos;
            macro_block_ctx.sstable_block_id_.macro_block_id_ = previous_block_id;
            handles_[handle_pos].reset();
            handles_[handle_pos].set_file(file);
            if (OB_FAIL(file->async_read_block(read_info, handles_[handle_pos]))) {
              LOG_WARN("fail to async read block", K(ret), K(previous_block_id));
            }
          } else {
            break;
          }
        }
      }
    }
  }
  handles_[0].reset();
  handles_[1].reset();
  prefetch_macro_block_idx_ = macros_handle_.count() - 1;
  LOG_INFO("get meta blocks", K(macros_handle_));
  return ret;
}

int ObPGMetaBlockReader::prefetch_block()
{
  int ret = OB_SUCCESS;
  if (prefetch_macro_block_idx_ < 0) {
    ret = OB_SUCCESS;
  } else {
    ObMacroBlockReadInfo read_info;
    ObMacroBlockCtx macro_block_ctx;
    blocksstable::ObStorageFile* file = NULL;
    read_info.offset_ = 0;
    read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
    read_info.io_desc_ = io_desc_;
    macro_block_ctx.sstable_block_id_.macro_block_id_ = macros_handle_.at(prefetch_macro_block_idx_);
    read_info.macro_block_ctx_ = &macro_block_ctx;
    handles_[handle_pos_].reset();
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret), K_(file_handle));
    } else if (FALSE_IT(handles_[handle_pos_].set_file(file))) {
    } else if (OB_FAIL(file->async_read_block(read_info, handles_[handle_pos_]))) {
      LOG_WARN("fail to async read block", K(ret));
    } else {
      handle_pos_ = 1 - handle_pos_;
      --prefetch_macro_block_idx_;
    }
  }
  return ret;
}

int ObPGMetaBlockReader::read_block(char*& buf, int64_t& buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t read_handle_pos = 1 - handle_pos_;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (read_macro_block_idx_ >= macros_handle_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(handles_[read_handle_pos].wait(io_timeout_ms))) {
    LOG_WARN("fail to wait io finish", K(ret));
  } else if (OB_FAIL(prefetch_block())) {
    LOG_WARN("fail to prefetch block", K(ret));
  } else {
    buf = const_cast<char*>(handles_[read_handle_pos].get_buffer());
    buf_len = handles_[read_handle_pos].get_data_size();
    if (OB_FAIL(check_data_checksum(buf, buf_len))) {
      LOG_WARN("fail to check data checksum", K(ret), K(handles_[read_handle_pos].get_macro_id()));
    } else {
      ++read_macro_block_idx_;
    }
  }
  return ret;
}

int ObPGMetaBlockReader::check_data_checksum(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  if (OB_UNLIKELY(nullptr == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize common header fail", K(ret), K(buf_len), K(pos), KP(buf), K(common_header));
  } else {
    const int32_t expected_payload_checksum = common_header.get_payload_checksum();
    const int64_t common_header_size = common_header.get_serialize_size();
    const int32_t calc_payload_checksum = ob_crc64(buf + common_header_size, common_header.get_payload_size());
    if (expected_payload_checksum != calc_payload_checksum) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("meta blocks has checksum error",
          K(ret),
          K(common_header),
          K(expected_payload_checksum),
          K(calc_payload_checksum));
    }
  }
  return ret;
}

ObIArray<MacroBlockId>& ObPGMetaBlockReader::get_meta_block_list()
{
  return macros_handle_.get_macro_id_list();
}

void ObPGMetaBlockReader::reset()
{
  is_inited_ = false;
  handles_[0].reset();
  handles_[1].reset();
  handle_pos_ = 0;
  macros_handle_.reset();
  prefetch_macro_block_idx_ = 0;
  read_macro_block_idx_ = 0;
  file_handle_.reset();
}

int ObPGMetaBlockReader::get_previous_block_id(
    const char* buf, const int64_t buf_len, blocksstable::MacroBlockId& previous_block_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  ObLinkedMacroBlockHeaderV2 linked_header;
  int64_t pos = 0;

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize common header fail", K(ret), K(buf_len), K(pos), KP(buf));
  } else if (OB_FAIL(linked_header.deserialize(buf, buf_len, pos))) {
    LOG_WARN("deserialize linked header fail", K(ret), K(buf_len), K(pos), KP(buf));
  } else {
    previous_block_id = linked_header.get_previous_block_id();
  }
  return ret;
}

ObPGMetaItemReader::ObPGMetaItemReader()
    : is_inited_(false),
      common_header_(nullptr),
      linked_header_(nullptr),
      reader_(),
      buf_(nullptr),
      buf_pos_(0),
      buf_len_(0),
      allocator_(ObModIds::OB_CHECKPOINT)
{}

int ObPGMetaItemReader::init(const MacroBlockId& entry_block, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaItemReader has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!entry_block.is_valid() || !file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(entry_block), K(file_handle));
  } else if (OB_FAIL(reader_.init(entry_block, file_handle))) {
    LOG_WARN("fail to init ObPGMetaBlockReader", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaItemReader::get_next_item(ObPGMetaItemBuffer& item)
{
  int ret = OB_SUCCESS;
  item.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemReader has not been inited", K(ret));
  } else if (buf_pos_ >= buf_len_) {
    if (OB_FAIL(read_item_block())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to read item block", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_item(item))) {
      LOG_WARN("fail to parse item", K(ret));
    }
  }

  return ret;
}

int ObPGMetaItemReader::read_item_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemReader has not been inited", K(ret));
  } else if (OB_FAIL(reader_.read_block(buf_, buf_len_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to read block", K(ret));
    }
  } else {
    char* buf = buf_;
    buf_pos_ = 0;
    common_header_ = reinterpret_cast<const ObMacroBlockCommonHeader*>(buf);
    buf += common_header_->get_serialize_size();
    buf_pos_ += common_header_->get_serialize_size();
    linked_header_ = reinterpret_cast<const ObLinkedMacroBlockHeaderV2*>(buf);
    buf += sizeof(*linked_header_);
    buf_pos_ += sizeof(*linked_header_);
    buf_len_ = common_header_->get_payload_size() + common_header_->get_serialize_size();
    int64_t item_count = linked_header_->item_count_;
    int64_t buf_len = 0;
    int64_t copy_buf_pos = 0;
    if (0 == item_count) {
      // when current item's size is larger than 2M bytes, we must keep read macro block until it reaches condition 1 ==
      // item_count_ namely the end block of current item.
      allocator_.reuse();
      ObPGMetaItemHeader* item_header = reinterpret_cast<ObPGMetaItemHeader*>(buf_ + buf_pos_);
      const int64_t item_size = item_header->size_;
      const int64_t request_size = item_size + sizeof(ObPGMetaItemHeader);
      char* big_buf = nullptr;
      if (OB_ISNULL(big_buf = static_cast<char*>(allocator_.alloc(request_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(item_size));
      } else {
        const int64_t data_len = common_header_->get_payload_size() - sizeof(ObLinkedMacroBlockHeaderV2);
        MEMCPY(big_buf, buf_ + buf_pos_, data_len);
        buf_ = big_buf;
        copy_buf_pos += data_len;
      }
      while (OB_SUCC(ret) && 0 == item_count) {
        if (OB_FAIL(reader_.read_block(buf, buf_len))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to read block", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          const char* tmp_buf = buf;
          common_header_ = reinterpret_cast<const ObMacroBlockCommonHeader*>(tmp_buf);
          tmp_buf += common_header_->get_serialize_size();
          linked_header_ = reinterpret_cast<const ObLinkedMacroBlockHeaderV2*>(tmp_buf);
          tmp_buf += sizeof(*linked_header_);
          MEMCPY(buf_ + copy_buf_pos, tmp_buf, common_header_->get_payload_size() - sizeof(ObLinkedMacroBlockHeaderV2));
          copy_buf_pos += common_header_->get_payload_size() - sizeof(ObLinkedMacroBlockHeaderV2);
          item_count = linked_header_->item_count_;
        }
      }

      if (OB_SUCC(ret)) {
        buf_pos_ = 0;
        buf_len_ = request_size;
      }
    }
  }
  return ret;
}

int ObPGMetaItemReader::parse_item(ObPGMetaItemBuffer& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemReader has not been inited", K(ret));
  } else {
    ObPGMetaItemHeader* item_header = reinterpret_cast<ObPGMetaItemHeader*>(buf_ + buf_pos_);
    buf_pos_ += sizeof(ObPGMetaItemHeader);
    item.item_type_ = item_header->type_;
    item.buf_len_ = item_header->size_;
    item.buf_ = buf_ + buf_pos_;
    buf_pos_ += item.buf_len_;
  }
  return ret;
}

void ObPGMetaItemReader::reset()
{
  is_inited_ = false;
  common_header_ = nullptr;
  linked_header_ = nullptr;
  reader_.reset();
  buf_ = nullptr;
  buf_pos_ = 0;
  buf_len_ = 0;
  allocator_.reset();
}

ObIArray<MacroBlockId>& ObPGMetaItemReader::get_meta_block_list()
{
  return reader_.get_meta_block_list();
}
