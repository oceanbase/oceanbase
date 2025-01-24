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

#include "observer/ob_server_struct.h"

#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObLinkedMacroBlockWriter::ObLinkedMacroBlockWriter()
  : is_inited_(false), write_ctx_(), handle_(), entry_block_id_(),
    tablet_id_(0), tablet_transfer_seq_(0), snapshot_version_(0), cur_macro_seq_(-1)
{
}

int ObLinkedMacroBlockWriter::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else {
    snapshot_version_ = 0;
    cur_macro_seq_ = -1;
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockWriter::init_for_object(
  const uint64_t tablet_id,
  const int64_t tablet_transfer_seq,
  const int64_t snapshot_version,
  const int64_t start_macro_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(0 == tablet_id || (snapshot_version > 0 && start_macro_seq < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(snapshot_version), K(start_macro_seq));
  } else {
    tablet_id_ = tablet_id;
    tablet_transfer_seq_ = tablet_transfer_seq;
    snapshot_version_ = snapshot_version;
    cur_macro_seq_ = start_macro_seq;
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockWriter::write_block(
    char *buf, const int64_t buf_len,
    blocksstable::ObMacroBlockCommonHeader &common_header, ObLinkedMacroBlockHeader &linked_header,
    MacroBlockId &pre_block_id,
    blocksstable::ObIMacroBlockFlushCallback *write_callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (GCTX.is_shared_storage_mode() && 0 == tablet_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in shared_storage_mode, tablet_id should not be 0 (0 is default)", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObStorageObjectOpt opt;
    if (snapshot_version_ > 0) {
      opt.set_ss_share_meta_macro_object_opt(tablet_id_, cur_macro_seq_++, 0);
    } else {
      opt.set_private_meta_macro_object_opt(tablet_id_, tablet_transfer_seq_);
    }
    const uint64_t tenant_id = MTL_ID();
    ObStorageObjectWriteInfo write_info;
    write_info.size_ = buf_len;
    write_info.buffer_ = buf;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sys_module_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = is_valid_tenant_id(tenant_id) ? tenant_id : OB_SERVER_TENANT_ID;
    write_info.offset_ = 0;

    MacroBlockId previous_block_id(0, MacroBlockId::EMPTY_ENTRY_BLOCK_INDEX, 0);
    int64_t pos = 0;
    if (!handle_.is_empty()) {
      if (OB_FAIL(handle_.wait())) {
        LOG_WARN("fail to wait io finish", K(ret));
      } else {
        previous_block_id = handle_.get_macro_id();
        pre_block_id = previous_block_id;
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(write_callback)) {
        if (OB_FAIL(write_callback->wait())) {
          LOG_WARN("failed to wait write call back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      pos = common_header.get_serialize_size();
      linked_header.set_previous_block_id(previous_block_id);
      if (OB_FAIL(linked_header.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize linked header", K(ret));
      } else {
        common_header.set_payload_checksum(static_cast<int32_t>(
            ob_crc64(buf + common_header.get_serialize_size(), common_header.get_payload_size())));
      }
    }
    if (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(common_header.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize common header", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      handle_.reset();
      if (OB_FAIL(ObObjectManager::async_write_object(opt, write_info, handle_))) {
        LOG_WARN("fail to async write block", K(ret), K(write_info), K(handle_));
      } else if (OB_FAIL(write_ctx_.add_macro_block_id(handle_.get_macro_id()))) {
        LOG_WARN("fail to add macro id", K(ret), "macro id", handle_.get_macro_id());
      }

      ObLogicMacroBlockId unused_logic_id;
      if (OB_SUCC(ret) && OB_NOT_NULL(write_callback)) {
        if (OB_FAIL(write_callback->write(handle_, unused_logic_id, const_cast<char*>(write_info.buffer_), write_info.size_, 0 /* unused row count*/))) {
          LOG_WARN("fail to write buf into write callback", K(ret), K(handle_.get_macro_id()), K(write_info.size_));
        }
      }
    }
  }
  return ret;
}

int ObLinkedMacroBlockWriter::close(blocksstable::ObIMacroBlockFlushCallback *write_callback, MacroBlockId &pre_block_id)
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
    if (OB_SUCC(ret) && OB_NOT_NULL(write_callback)) {
      if (OB_FAIL(write_callback->wait())) {
        LOG_WARN("failed to wait redo callback", K(ret));
      }
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
int64_t ObLinkedMacroBlockWriter::get_meta_block_cnt() const
{
  return write_ctx_.get_macro_block_count();
}

void ObLinkedMacroBlockWriter::reset()
{
  is_inited_ = false;
  write_ctx_.reset();
  handle_.reset();
  entry_block_id_.reset();
  tablet_id_ = 0;
  snapshot_version_ = 0;
  cur_macro_seq_ = -1;
}

void ObLinkedMacroBlockWriter::reuse_for_next_round()
{
  is_inited_ = false;
  handle_.reset();
  entry_block_id_.reset();
  OB_ASSERT(-1 == cur_macro_seq_); // for shared macro, can't reuse sequence
}

//================== ObLinkedMacroBlockItemWriter =============================

ObLinkedMacroBlockItemWriter::ObLinkedMacroBlockItemWriter()
  : is_inited_(false), is_closed_(false), written_items_cnt_(0),
    need_disk_addr_(false), first_inflight_item_idx_(0),
    pre_block_inflight_items_cnt_(0), curr_block_inflight_items_cnt_(0),
    allocator_(), block_writer_(), io_buf_(nullptr), io_buf_size_(0),
    io_buf_pos_(0), common_header_(), linked_header_(), write_callback_(nullptr)
{
}

int ObLinkedMacroBlockItemWriter::init(const bool need_disk_addr, const ObMemAttr &mem_attr)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockItemWriter has already been inited", K(ret));
  } else if (FALSE_IT(allocator_.set_attr(mem_attr))) {
  } else if (OB_FAIL(block_writer_.init())) {
    LOG_WARN("fail to init meta block writer", K(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else if (FALSE_IT(MEMSET(io_buf_, 0, macro_block_size))) {
  } else if (OB_FAIL(common_header_.set_attr(ObMacroBlockCommonHeader::LinkedBlock))) {
    LOG_WARN("fail to set type for common header", K(ret), K(common_header_));
  } else {
    need_disk_addr_ = need_disk_addr;
    io_buf_size_ = macro_block_size;
    io_buf_pos_ = ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size();
    written_items_cnt_ = 0;
    is_inited_ = true;
    is_closed_ = false;
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::init_for_object(
  const uint64_t tablet_id,
  const int64_t tablet_transfer_seq,
  const int64_t snapshot_version,
  const int64_t start_macro_seq,
  ObIMacroBlockFlushCallback *write_callback)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  const uint64_t tenant_id = MTL_ID();
  const ObMemAttr mem_attr(is_valid_tenant_id(tenant_id) ? tenant_id : OB_SERVER_TENANT_ID, "ObjLinkWriter");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockItemWriter has already been inited", K(ret));
  } else if (FALSE_IT(allocator_.set_attr(mem_attr))) {
  } else if (OB_UNLIKELY(0 == tablet_id || (snapshot_version > 0 && start_macro_seq < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(snapshot_version), K(start_macro_seq));
  } else if (OB_FAIL(block_writer_.init_for_object(tablet_id, tablet_transfer_seq, snapshot_version, start_macro_seq))) {
    LOG_WARN("fail to init meta block writer", K(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else if (FALSE_IT(MEMSET(io_buf_, 0, macro_block_size))) {
  } else if (OB_FAIL(common_header_.set_attr(ObMacroBlockCommonHeader::LinkedBlock))) {
    LOG_WARN("fail to set type for common header", K(ret), K(common_header_));
  } else {
    need_disk_addr_ = false;
    io_buf_size_ = macro_block_size;
    io_buf_pos_ = ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size();
    written_items_cnt_ = 0;
    write_callback_ = write_callback;
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
        ++linked_header_.item_count_;
      }
    } else {
      if (0 != linked_header_.item_count_) {
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
            linked_header_.item_count_ = is_last_block ? 1 : 0;
            linked_header_.fragment_offset_ = item_buf_pos;
            if (OB_FAIL(write_item_content(item_buf, item_buf_len, item_buf_pos))) {
              LOG_WARN("fail to write item buffer", K(ret));
            } else if (io_buf_pos_ == io_buf_size_) {
              if (OB_FAIL(write_block())) {
                LOG_WARN("fail to write block", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (io_buf_pos_ > ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size()) {
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
            ++linked_header_.item_count_;
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
    int64_t offset = common_header_.get_serialize_size() + linked_header_.get_serialize_size();
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
  common_header_.set_payload_size(
    static_cast<int32_t>(io_buf_pos_ - common_header_.get_serialize_size()));
  MacroBlockId pre_block_id;
  const int64_t upper_align_size = upper_align(io_buf_pos_, DIO_ALIGN_SIZE);
  if (OB_FAIL(block_writer_.write_block(
        io_buf_, upper_align_size, common_header_, linked_header_, pre_block_id, write_callback_))) {
    LOG_WARN("fail to write block", K(ret));
  } else if (OB_FAIL(set_pre_block_inflight_items_addr(pre_block_id))) {
    LOG_WARN("fail to set pre block inflight items addr", K(ret));
  } else {
    io_buf_pos_ = common_header_.get_serialize_size() + linked_header_.get_serialize_size();
    linked_header_.item_count_ = 0;
    linked_header_.fragment_offset_ = 0;
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
  } else if ((io_buf_pos_ > ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size())
      && OB_FAIL(write_block())) {
    LOG_WARN("fail to write block", K(ret));
  } else if (OB_FAIL(block_writer_.close(write_callback_, pre_block_id))) {
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
  common_header_.reset();
  linked_header_.reset();
}

}  // end namespace storage
}  // end namespace oceanbase
