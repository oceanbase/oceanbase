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

#include "storage/ob_pg_meta_block_writer.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/ob_store_file.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGMetaBlockWriter::ObPGMetaBlockWriter()
    : is_inited_(false),
      allocator_(ObModIds::OB_CHECKPOINT),
      io_desc_(),
      write_ctx_(),
      handle_(),
      macro_block_id_(-1),
      meta_()
{}

int ObPGMetaBlockWriter::init(ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_handle));
  } else if (!write_ctx_.file_handle_.is_valid() && OB_FAIL(write_ctx_.file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret), K(file_handle));
  } else if (FALSE_IT(handle_.set_file(file_handle.get_storage_file()))) {
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret), K(file_handle));
  } else {
    io_desc_.category_ = SYS_IO;
    io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
    io_desc_.req_deadline_time_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaBlockWriter::write_block(const char* buf, const int64_t buf_len,
    blocksstable::ObMacroBlockCommonHeader& common_header, blocksstable::ObLinkedMacroBlockHeaderV2& linked_header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf));
  } else {
    const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
    if (buf_len != macro_block_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(macro_block_size), K(buf_len));
    } else {
      ObMacroBlockWriteInfo write_info;
      write_info.size_ = macro_block_size;
      write_info.io_desc_ = io_desc_;
      write_info.buffer_ = buf;
      write_info.meta_.meta_ = &meta_;
      write_info.meta_.schema_ = &schema_;
      write_info.block_write_ctx_ = &write_ctx_;
      MacroBlockId previous_block_id(-1);
      if (!handle_.is_empty()) {
        const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
        if (OB_FAIL(handle_.wait(io_timeout_ms))) {
          LOG_WARN("fail to wait io finish", K(ret));
        } else {
          previous_block_id = handle_.get_macro_id();
          macro_block_id_ = previous_block_id;
        }
      }

      if (OB_SUCC(ret)) {
        linked_header.set_previous_block_id(previous_block_id);
        common_header.set_payload_checksum(
            static_cast<int32_t>(ob_crc64(buf + common_header.get_serialize_size(), common_header.get_payload_size())));
      }

      if (OB_SUCC(ret)) {
        handle_.reset();
        blocksstable::ObStorageFile* file = NULL;
        if (OB_ISNULL(file = file_handle_.get_storage_file())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to get file", K(ret), K_(file_handle));
        } else if (FALSE_IT(handle_.set_file(file))) {
        } else if (OB_FAIL(file->async_write_block(write_info, handle_))) {
          LOG_WARN("fail to async write block", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPGMetaBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaBlockWriter has not been inited", K(ret));
  } else if (handle_.is_empty()) {
    // do nothing
  } else {
    const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
    if (OB_FAIL(handle_.wait(io_timeout_ms))) {
      LOG_WARN("fail to wait io finish", K(ret));
    } else {
      macro_block_id_ = handle_.get_macro_id();
    }
  }
  return ret;
}

const MacroBlockId& ObPGMetaBlockWriter::get_entry_block() const
{
  return macro_block_id_;
}

ObIArray<MacroBlockId>& ObPGMetaBlockWriter::get_meta_block_list()
{
  return write_ctx_.get_macro_block_list();
}

void ObPGMetaBlockWriter::reset()
{
  is_inited_ = false;
  allocator_.reset();
  write_ctx_.reset();
  handle_.reset();
  file_handle_.reset();
  macro_block_id_.reset();
  macro_block_id_.set_second_id(-1);
  MEMSET(&meta_, 0, sizeof(meta_));
}

ObPGMetaItemWriter::ObPGMetaItemWriter()
    : is_inited_(false),
      iter_(nullptr),
      allocator_(ObModIds::OB_CHECKPOINT),
      writer_(),
      io_buf_(nullptr),
      io_buf_size_(0),
      io_buf_pos_(0),
      common_header_(nullptr),
      linked_header_(nullptr)
{}

int ObPGMetaItemWriter::init(ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaWriter has already been inited", K(ret));
  } else if (OB_FAIL(writer_.init(file_handle))) {
    LOG_WARN("fail to init meta block writer", K(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char*>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else {
    io_buf_size_ = macro_block_size;
    common_header_ = reinterpret_cast<ObMacroBlockCommonHeader*>(io_buf_);
    common_header_->reset();
    linked_header_ = reinterpret_cast<ObLinkedMacroBlockHeaderV2*>(io_buf_ + common_header_->get_serialize_size());
    linked_header_->reset();
    io_buf_pos_ = common_header_->get_serialize_size() + linked_header_->get_serialize_size();
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaItemWriter::write_item(ObIPGMetaItem* item, ObIPGWriteMetaItemCallback* callback)
{
  int ret = OB_SUCCESS;
  const char* item_buf = nullptr;
  int64_t item_buf_len = 0;
  int64_t item_buf_pos = 0;
  UNUSED(callback);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, item must not be null", K(ret));
  } else if (OB_FAIL(item->serialize(item_buf, item_buf_len))) {
    LOG_WARN("fail to serialize item to buffer", K(ret));
  } else {
    const int64_t real_item_size = sizeof(ObPGMetaItemHeader) + item_buf_len;
    int64_t remain_size = io_buf_size_ - io_buf_pos_;
    if (real_item_size < remain_size) {
      if (OB_FAIL(write_item_header(item, item_buf_len))) {
        LOG_WARN("fail to write item header", K(ret));
      } else if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
        LOG_WARN("fail to write item buffer", K(ret));
      } else {
        ++linked_header_->item_count_;
      }
    } else {
      if (0 != linked_header_->item_count_) {
        if (OB_FAIL(write_block())) {
          LOG_WARN("fail to write block", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        // remain size after write block
        remain_size = io_buf_size_ - io_buf_pos_;
        if (real_item_size > remain_size) {
          // write big meta which larger than 2M
          if (OB_FAIL(write_item_header(item, item_buf_len))) {
            LOG_WARN("fail to write item header", K(ret));
          }
          while (OB_SUCC(ret) && item_buf_pos < item_buf_len) {
            remain_size = io_buf_size_ - io_buf_pos_;
            const bool is_last_block = item_buf_len - item_buf_pos < remain_size;
            linked_header_->item_count_ = is_last_block ? 1 : 0;
            linked_header_->fragment_offset_ = item_buf_pos;
            if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
              LOG_WARN("fail to write item buffer", K(ret));
            } else if (io_buf_pos_ == io_buf_size_) {
              if (OB_FAIL(write_block())) {
                LOG_WARN("fail to write block", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (io_buf_pos_ > HEADER_SIZE) {
              if (OB_FAIL(write_block())) {
                LOG_WARN("fail to write block", K(ret));
              }
            }
          }
        } else {
          if (OB_FAIL(write_item_header(item, item_buf_len))) {
            LOG_WARN("fail to write item header", K(ret));
          } else if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to write item buffer", K(ret));
          } else {
            ++linked_header_->item_count_;
          }
        }
      }
    }
  }
  return ret;
}

int ObPGMetaItemWriter::write_item_header(const ObIPGMetaItem* item, const int64_t item_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObPGMetaItemHeader* item_header = reinterpret_cast<ObPGMetaItemHeader*>(io_buf_ + io_buf_pos_);
    item_header->type_ = item->get_item_type();
    item_header->size_ = item_len;
    item_header->reserved_ = 0;
    io_buf_pos_ += sizeof(ObPGMetaItemHeader);
  }
  return ret;
}

int ObPGMetaItemWriter::write_item_content(const char* item_buf, const int64_t item_buf_len, int64_t& item_buf_pos)
{
  int ret = OB_SUCCESS;
  const int64_t buffer_remain_size = io_buf_size_ - io_buf_pos_;
  const int64_t item_remain_size = item_buf_len - item_buf_pos;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else if (OB_ISNULL(item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(item_buf));
  } else {
    const int64_t copy_size = std::min(buffer_remain_size, item_remain_size);
    MEMCPY(io_buf_ + io_buf_pos_, item_buf + item_buf_pos, copy_size);
    io_buf_pos_ += copy_size;
    item_buf_pos += copy_size;
  }
  return ret;
}

int ObPGMetaItemWriter::write_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else {
    common_header_->set_payload_size(static_cast<int32_t>(io_buf_pos_ - common_header_->get_serialize_size()));
    if (OB_FAIL(writer_.write_block(io_buf_, io_buf_size_, *common_header_, *linked_header_))) {
      LOG_WARN("fail to write block", K(ret));
    } else {
      io_buf_pos_ = common_header_->get_serialize_size() + linked_header_->get_serialize_size();
      linked_header_->item_count_ = 0;
      linked_header_->fragment_offset_ = 0;
    }
  }
  return ret;
}

int ObPGMetaItemWriter::get_entry_block_index(blocksstable::MacroBlockId& entry_block) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else {
    entry_block = writer_.get_entry_block();
  }
  return ret;
}

ObIArray<MacroBlockId>& ObPGMetaItemWriter::get_meta_block_list()
{
  return writer_.get_meta_block_list();
}

int ObPGMetaItemWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaItemWriter has not been inited", K(ret));
  } else if (io_buf_pos_ > HEADER_SIZE && OB_FAIL(write_block())) {
    LOG_WARN("fail to write block", K(ret));
  } else if (OB_FAIL(writer_.close())) {
    LOG_WARN("fail to close writer", K(ret));
  }
  return ret;
}

void ObPGMetaItemWriter::reset()
{
  is_inited_ = false;
  iter_ = nullptr;
  allocator_.reset();
  writer_.reset();
  io_buf_ = nullptr;
  io_buf_size_ = 0;
  io_buf_pos_ = 0;
  common_header_ = nullptr;
  linked_header_ = nullptr;
}
