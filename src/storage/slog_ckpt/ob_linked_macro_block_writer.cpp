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

#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObLinkedMacroBlockWriter::ObLinkedMacroBlockWriter()
  : is_inited_(false), io_desc_(), write_ctx_(), handle_(), entry_block_id_()
{
}

int ObLinkedMacroBlockWriter::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else {
    io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockWriter::write_block(const char *buf, const int64_t buf_len,
  blocksstable::ObMacroBlockCommonHeader &common_header, ObLinkedMacroBlockHeader &linked_header,
  MacroBlockId &pre_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObMacroBlockWriteInfo write_info;
    write_info.size_ = buf_len;
    write_info.io_desc_ = io_desc_;
    write_info.buffer_ = buf;
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    write_info.io_desc_.set_sys_module_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
    MacroBlockId previous_block_id;
    previous_block_id.set_block_index(MacroBlockId::EMPTY_ENTRY_BLOCK_INDEX);
    if (!handle_.is_empty()) {
      if (OB_FAIL(handle_.wait())) {
        LOG_WARN("fail to wait io finish", K(ret));
      } else {
        previous_block_id = handle_.get_macro_id();
        pre_block_id = previous_block_id;
      }
    }

    if (OB_SUCC(ret)) {
      linked_header.set_previous_block_id(previous_block_id);
      common_header.set_payload_checksum(static_cast<int32_t>(
        ob_crc64(buf + common_header.get_serialize_size(), common_header.get_payload_size())));
    }

    if (OB_SUCC(ret)) {
      handle_.reset();
      if (OB_FAIL(ObBlockManager::async_write_block(write_info, handle_))) {
        LOG_WARN("fail to async write block", K(ret), K(write_info), K(handle_));
      } else if (OB_FAIL(write_ctx_.add_macro_block_id(handle_.get_macro_id()))) {
        LOG_WARN("fail to add macro id", K(ret), "macro id", handle_.get_macro_id());
      }
    }
  }
  return ret;
}

int ObLinkedMacroBlockWriter::close(MacroBlockId &pre_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (handle_.is_empty()) {
    // do nothing
  } else {
    if (OB_FAIL(handle_.wait())) {
      LOG_WARN("fail to wait io finish", K(ret));
    } else {
      entry_block_id_ = handle_.get_macro_id();
      pre_block_id = entry_block_id_;
    }
  }
  return ret;
}

const MacroBlockId &ObLinkedMacroBlockWriter::get_entry_block() const
{
  return entry_block_id_;
}

ObIArray<MacroBlockId> &ObLinkedMacroBlockWriter::get_meta_block_list()
{
  return write_ctx_.get_macro_block_list();
}

void ObLinkedMacroBlockWriter::reset()
{
  is_inited_ = false;
  write_ctx_.reset();
  handle_.reset();
  entry_block_id_.reset();
  io_desc_.reset();
}

void ObLinkedMacroBlockWriter::reuse_for_next_round()
{
  is_inited_ = false;
  handle_.reset();
  entry_block_id_.reset();
  io_desc_.reset();
}

//================== ObLinkedMacroBlockItemWriter =============================

ObLinkedMacroBlockItemWriter::ObLinkedMacroBlockItemWriter()
  : is_inited_(false), is_closed_(false), written_items_cnt_(0),
    need_disk_addr_(false), first_inflight_item_idx_(0),
    pre_block_inflight_items_cnt_(0), curr_block_inflight_items_cnt_(0),
    allocator_(), block_writer_(), io_buf_(nullptr), io_buf_size_(0),
    io_buf_pos_(0), common_header_(nullptr), linked_header_(nullptr)
{
}

int ObLinkedMacroBlockItemWriter::init(const bool need_disk_addr, const ObMemAttr &mem_attr)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaWriter has already been inited", K(ret));
  } else if (OB_FAIL(block_writer_.init())) {
    LOG_WARN("fail to init meta block writer", K(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else {
    allocator_.set_attr(mem_attr);
    need_disk_addr_ = need_disk_addr;
    io_buf_size_ = macro_block_size;
    common_header_ = reinterpret_cast<ObMacroBlockCommonHeader *>(io_buf_);
    common_header_->reset();
    common_header_->set_attr(ObMacroBlockCommonHeader::LinkedBlock);
    linked_header_ = reinterpret_cast<ObLinkedMacroBlockHeader *>(
      io_buf_ + common_header_->get_serialize_size());
    linked_header_->reset();
    io_buf_pos_ = common_header_->get_serialize_size() + linked_header_->get_serialize_size();
    written_items_cnt_ = 0;
    is_inited_ = true;
    is_closed_ = false;
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::write_item(
  const char *item_buf, const int64_t item_buf_len, int64_t *item_idx)
{
  int ret = OB_SUCCESS;
  int64_t item_buf_pos = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockItemWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObLinkedMacroBlockItemWriter has been closed", K(ret));
  } else if (OB_ISNULL(item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const int64_t real_item_size = sizeof(ObLinkedMacroBlockItemHeader) + item_buf_len;
    int64_t remain_size = io_buf_size_ - io_buf_pos_;
    if (real_item_size <= remain_size) {
      if (OB_FAIL(write_item_header(item_buf, item_buf_len))) {
        LOG_WARN("fail to write item header", K(ret));
      } else if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
        LOG_WARN("fail to write item buffer", K(ret));
      } else if (OB_FAIL(record_inflight_item(item_buf_len, item_idx))) {
        LOG_WARN("fail to record inflight item info", K(ret));
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
          if (OB_FAIL(write_item_header(item_buf, item_buf_len))) {
            LOG_WARN("fail to write item header", K(ret));
          } else if (OB_FAIL(record_inflight_item(item_buf_len, item_idx))) {
            LOG_WARN("fail to record inflight item info", K(ret));
          }
          while (OB_SUCC(ret) && item_buf_pos < item_buf_len) {
            remain_size = io_buf_size_ - io_buf_pos_;
            const bool is_last_block = (item_buf_len - item_buf_pos < remain_size);
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
            if (io_buf_pos_ > BLOCK_HEADER_SIZE) {
              if (OB_FAIL(write_block())) {
                LOG_WARN("fail to write block", K(ret));
              }
            }
          }
        } else {
          if (OB_FAIL(write_item_header(item_buf, item_buf_len))) {
            LOG_WARN("fail to write item header", K(ret));
          } else if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to write item buffer", K(ret));
          } else if (OB_FAIL(record_inflight_item(item_buf_len, item_idx))) {
            LOG_WARN("fail to record inflight item info", K(ret));
          } else {
            ++linked_header_->item_count_;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      written_items_cnt_++;
    }
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::record_inflight_item(
  const int64_t item_buf_len, int64_t *item_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
        (need_disk_addr_ && nullptr == item_idx) || (!need_disk_addr_ && nullptr != item_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(need_disk_addr), KP(item_idx));
  } else if (need_disk_addr_) {
    if (OB_FAIL(item_size_arr_.push_back(item_buf_len + sizeof(ObLinkedMacroBlockItemHeader)))) {
      LOG_WARN("fail to push back addr", K(ret));
    } else {
      *item_idx = item_size_arr_.count() - 1;
      ++curr_block_inflight_items_cnt_;
    }
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::set_pre_block_inflight_items_addr(
  const MacroBlockId &pre_block_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == pre_block_inflight_items_cnt_)) {
    // 1. this is the first block, so has no pre block
    // 2. this is the second or later block of a large item
    // 3. need_disk_addr_ == false
    // 4. no item to write
  } else if (OB_UNLIKELY(!pre_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pre_block_id is invalid", K(ret), K(pre_block_id), K_(pre_block_inflight_items_cnt));
  } else {
    // item which cross the boundary do not share block with other items
    // which is ensure by write_item, so first item's offset in the block is
    // fixed
    int64_t offset = BLOCK_HEADER_SIZE;
    for (int64_t idx = first_inflight_item_idx_;
        OB_SUCC(ret) && idx < first_inflight_item_idx_ + pre_block_inflight_items_cnt_; idx++) {
      ObMetaDiskAddr addr;
      if (OB_FAIL(addr.set_block_addr(pre_block_id, offset, item_size_arr_.at(idx), ObMetaDiskAddr::DiskType::BLOCK))) {
        LOG_WARN("fail to push back address", K(ret), K(addr));
      } else if (OB_FAIL(item_disk_addr_arr_.push_back(addr))) {
        LOG_WARN("fail to push back address", K(ret), K(addr));
      } else {
        offset += item_size_arr_.at(idx);
      }
    }
    if (OB_SUCC(ret)) {
      first_inflight_item_idx_ += pre_block_inflight_items_cnt_;
    }
  }

  if (OB_SUCC(ret)) {
    pre_block_inflight_items_cnt_ = curr_block_inflight_items_cnt_;
    curr_block_inflight_items_cnt_ = 0;
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::write_item_header(
  const char *item_buf, const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObLinkedMacroBlockItemHeader *header =
      reinterpret_cast<ObLinkedMacroBlockItemHeader *>(io_buf_ + io_buf_pos_);
    header->version_ = ObLinkedMacroBlockItemHeader::LINKED_MACRO_BLOCK_ITEM_HEADER_VERSION;
    header->magic_ = ObLinkedMacroBlockItemHeader::LINKED_MACRO_BLOCK_ITEM_MAGIC;
    header->payload_size_ = item_buf_len;
    header->payload_crc_ = static_cast<int32_t>(ob_crc64(item_buf, item_buf_len));
    io_buf_pos_ += sizeof(ObLinkedMacroBlockItemHeader);
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::write_item_content(
  const char *item_buf, const int64_t item_buf_len, int64_t &item_buf_pos)
{
  int ret = OB_SUCCESS;
  const int64_t buffer_remain_size = io_buf_size_ - io_buf_pos_;
  const int64_t item_remain_size = item_buf_len - item_buf_pos;
  if (OB_ISNULL(item_buf)) {
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

int ObLinkedMacroBlockItemWriter::write_block()
{
  int ret = OB_SUCCESS;
  common_header_->set_payload_size(
    static_cast<int32_t>(io_buf_pos_ - common_header_->get_serialize_size()));
  MacroBlockId pre_block_id;
  const int64_t upper_align_size = upper_align(io_buf_pos_, DIO_ALIGN_SIZE);
  if (OB_FAIL(block_writer_.write_block(
        io_buf_, upper_align_size, *common_header_, *linked_header_, pre_block_id))) {
    LOG_WARN("fail to write block", K(ret));
  } else if (OB_FAIL(set_pre_block_inflight_items_addr(pre_block_id))) {
    LOG_WARN("fail to set pre block inflight items addr", K(ret));
  } else {
    io_buf_pos_ = common_header_->get_serialize_size() + linked_header_->get_serialize_size();
    linked_header_->item_count_ = 0;
    linked_header_->fragment_offset_ = 0;
  }

  return ret;
}

int ObLinkedMacroBlockItemWriter::get_entry_block(MacroBlockId &entry_block) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockItemWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObLinkedMacroBlockItemWriter must be closed when get entry block", K(ret));
  } else {
    if (0 == written_items_cnt_) {
      LOG_INFO("no block items has been write");
      entry_block = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
    } else {
      entry_block = block_writer_.get_entry_block();
    }
  }
  return ret;
}

int64_t ObLinkedMacroBlockItemWriter::get_item_disk_addr(
  const int64_t item_idx, ObMetaDiskAddr &addr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObLinkedMacroBlockItemWriter must be closed when get item addr", K(ret));
  } else if (OB_UNLIKELY(item_idx > item_disk_addr_arr_.count() - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid item idx", K(ret), K(item_idx), K_(need_disk_addr));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObLinkedMacroBlockItemWriter must be closed when get item addr", K(ret));
  } else {
    addr = item_disk_addr_arr_.at(item_idx);
  }
  return ret;
}

ObIArray<MacroBlockId> &ObLinkedMacroBlockItemWriter::get_meta_block_list()
{
  return block_writer_.get_meta_block_list();
}

int ObLinkedMacroBlockItemWriter::close()
{
  int ret = OB_SUCCESS;
  MacroBlockId pre_block_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockItemWriter has not been inited", K(ret));
  } else if (io_buf_pos_ > BLOCK_HEADER_SIZE && OB_FAIL(write_block())) {
    LOG_WARN("fail to write block", K(ret));
  } else if (OB_FAIL(block_writer_.close(pre_block_id))) {
    LOG_WARN("fail to close writer", K(ret));
  } else if (OB_FAIL(set_pre_block_inflight_items_addr(pre_block_id))) {
    LOG_WARN("fail to set pre block inflight items addr", K(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

void ObLinkedMacroBlockItemWriter::reset()
{
  inner_reset();
  block_writer_.reset();
}

void ObLinkedMacroBlockItemWriter::reuse_for_next_round()
{
  inner_reset();
  block_writer_.reuse_for_next_round();
}

void ObLinkedMacroBlockItemWriter::inner_reset()
{
  is_inited_ = false;
  is_closed_ = false;
  written_items_cnt_ = 0;
  need_disk_addr_ = false;
  first_inflight_item_idx_ = 0;
  pre_block_inflight_items_cnt_ = 0;
  curr_block_inflight_items_cnt_ = 0;
  allocator_.reset();
  item_size_arr_.reset();
  item_disk_addr_arr_.reset();
  io_buf_ = nullptr;
  io_buf_size_ = 0;
  io_buf_pos_ = 0;
  common_header_ = nullptr;
  linked_header_ = nullptr;
}

}  // end namespace storage
}  // end namespace oceanbase
