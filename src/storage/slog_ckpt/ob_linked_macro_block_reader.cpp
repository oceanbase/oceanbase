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

#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObLinkedMacroBlockReader::ObLinkedMacroBlockReader()
  : is_inited_(false), handle_pos_(0), macros_handle_(), prefetch_macro_block_idx_(0),
    read_macro_block_cnt_(0)
{
  handles_[0].reset();
  handles_[1].reset();
}

int ObLinkedMacroBlockReader::init(const MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockReader has been inited twice", K(ret));
  } else if (OB_FAIL(get_meta_blocks(entry_block))) {
    LOG_WARN("fail to get meta blocks", K(ret));
  } else if (OB_FAIL(prefetch_block())) {
    LOG_WARN("fail to prefetch block", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockReader::get_meta_blocks(const MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  ObMacroBlockReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ = sizeof(ObMacroBlockCommonHeader) + sizeof(ObLinkedMacroBlockHeader);
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_group_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
  if (entry_block.second_id() >= 0) {
    read_info.macro_block_id_ = entry_block;
    int64_t handle_pos = 0;
    MacroBlockId previous_block_id;
    handles_[handle_pos].reset();
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, handles_[handle_pos]))) {
      LOG_WARN("fail to async read block", K(ret));
    } else {
      const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(handles_[handle_pos].wait(io_timeout_ms))) {
          LOG_WARN("fail to wait io finish", K(ret));
        } else if (OB_FAIL(macros_handle_.add(read_info.macro_block_id_))) {
          LOG_WARN("fail to push macro block id", K(ret));
        } else if (OB_FAIL(get_previous_block_id(handles_[handle_pos].get_buffer(),
                     handles_[handle_pos].get_data_size(), previous_block_id))) {
          LOG_WARN("fail to get previous block index", K(ret), K(entry_block));
        } else {
          if (previous_block_id.second_id() >= 0) {
            handle_pos = 1 - handle_pos;
            read_info.macro_block_id_ = previous_block_id;
            handles_[handle_pos].reset();
            if (OB_FAIL(ObBlockManager::async_read_block(read_info, handles_[handle_pos]))) {
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

int ObLinkedMacroBlockReader::prefetch_block()
{
  int ret = OB_SUCCESS;
  if (prefetch_macro_block_idx_ < 0) {
    ret = OB_SUCCESS;
  } else {
    ObMacroBlockReadInfo read_info;
    read_info.offset_ = 0;
    read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_desc_.set_group_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
    read_info.macro_block_id_ = macros_handle_.at(prefetch_macro_block_idx_);
    handles_[handle_pos_].reset();
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, handles_[handle_pos_]))) {
      LOG_WARN("fail to async read block", K(ret));
    } else {
      handle_pos_ = 1 - handle_pos_;
      --prefetch_macro_block_idx_;
    }
  }
  return ret;
}

int ObLinkedMacroBlockReader::iter_read_block(char *&buf, int64_t &buf_len, MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  const int64_t read_handle_pos = 1 - handle_pos_;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (read_macro_block_cnt_ >= macros_handle_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(handles_[read_handle_pos].wait(io_timeout_ms))) {
    LOG_WARN("fail to wait io finish", K(ret));
  } else if (OB_FAIL(prefetch_block())) {
    LOG_WARN("fail to prefetch block", K(ret));
  } else {
    buf = const_cast<char *>(handles_[read_handle_pos].get_buffer());
    buf_len = handles_[read_handle_pos].get_data_size();
    block_id = handles_[read_handle_pos].get_macro_id();
    if (OB_FAIL(check_data_checksum(buf, buf_len))) {
      LOG_WARN("fail to check data checksum", K(ret));
    } else {
      ++read_macro_block_cnt_;
    }
  }
  return ret;
}

int ObLinkedMacroBlockReader::pread_block(const ObMetaDiskAddr &addr, ObMacroBlockHandle &handler)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  handler.reset();
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_group_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
  if (OB_FAIL(addr.get_block_addr(read_info.macro_block_id_, read_info.offset_, read_info.size_))) {
    LOG_WARN("fail to get block address", K(ret), K(addr));
  } else if (OB_FAIL(ObBlockManager::async_read_block(read_info, handler))) {
    LOG_WARN("fail to async read block", K(ret));
  } else if (OB_FAIL(handler.wait(GCONF._data_storage_io_timeout / 1000L))) {
    LOG_WARN("fail to wait io finish", K(ret));
  }
  return ret;
}

int ObLinkedMacroBlockReader::read_block_by_id(
  const MacroBlockId &block_id, ObMacroBlockHandle &handler)
{
  int ret = OB_SUCCESS;

  ObMacroBlockReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_group_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
  read_info.macro_block_id_ = block_id;
  handler.reset();
  if (OB_FAIL(ObBlockManager::async_read_block(read_info, handler))) {
    LOG_WARN("fail to async read block", K(ret));
  } else if (OB_FAIL(handler.wait(GCONF._data_storage_io_timeout / 1000L))) {
    LOG_WARN("fail to wait io finish", K(ret));
  } else if (OB_FAIL(check_data_checksum(handler.get_buffer(), handler.get_data_size()))) {
    LOG_WARN("fail to check data crc", K(ret));
  }
  return ret;
}

int ObLinkedMacroBlockReader::check_data_checksum(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const ObMacroBlockCommonHeader *common_header =
    reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
  if (OB_UNLIKELY(nullptr == buf || buf_len < sizeof(ObMacroBlockCommonHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    const int32_t expected_payload_checksum = common_header->get_payload_checksum();
    const int32_t calc_payload_checksum =
      ob_crc64(buf + sizeof(ObMacroBlockCommonHeader), common_header->get_payload_size());
    if (expected_payload_checksum != calc_payload_checksum) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("common header checksum error", K(ret), KPC(common_header),
        K(expected_payload_checksum), K(calc_payload_checksum));
    }
  }
  return ret;
}

ObIArray<MacroBlockId> &ObLinkedMacroBlockReader::get_meta_block_list()
{
  return macros_handle_.get_macro_id_list();
}

void ObLinkedMacroBlockReader::reset()
{
  is_inited_ = false;
  handles_[0].reset();
  handles_[1].reset();
  handle_pos_ = 0;
  macros_handle_.reset();
  prefetch_macro_block_idx_ = 0;
  read_macro_block_cnt_ = 0;
}

int ObLinkedMacroBlockReader::get_previous_block_id(
  const char *buf, const int64_t buf_len, MacroBlockId &previous_block_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  ObLinkedMacroBlockHeader linked_header;
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

//================== ObLinkedMacroBlockItemReader =============================

ObLinkedMacroBlockItemReader::ObLinkedMacroBlockItemReader()
  : is_inited_(false), common_header_(nullptr), linked_header_(nullptr), block_reader_(),
    buf_(nullptr), buf_pos_(0), buf_len_(0), allocator_(ObModIds::OB_CHECKPOINT)
{
}

int ObLinkedMacroBlockItemReader::init(const MacroBlockId &entry_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockItemReader has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!entry_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(entry_block));
  } else if (OB_FAIL(block_reader_.init(entry_block))) {
    LOG_WARN("fail to init ObLinkedMacroBlockReader", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockItemReader::get_next_item(
  char *&item_buf, int64_t &item_buf_len, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  item_buf = nullptr;
  item_buf_len = 0;
  addr.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockItemReader has not been inited", K(ret));
  } else if (buf_pos_ >= buf_len_) {
    if (OB_FAIL(read_item_block())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to read item block", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_item(item_buf, item_buf_len, addr))) {
      LOG_WARN("fail to parse item", K(ret));
    }
  }

  return ret;
}

int ObLinkedMacroBlockItemReader::read_item_block()
{
  int ret = OB_SUCCESS;
  MacroBlockId tmp_block_id;
  if (OB_FAIL(block_reader_.iter_read_block(buf_, buf_len_, tmp_block_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to read block", K(ret));
    }
  } else {
    // record the macro block that the buf belongs to,
    // if it is a big buf for a large item, only record the first block
    buf_block_id_ = tmp_block_id;
    char *buf = buf_;
    buf_pos_ = 0;
    common_header_ = reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
    buf += sizeof(ObMacroBlockCommonHeader);
    buf_pos_ += sizeof(ObMacroBlockCommonHeader);
    linked_header_ = reinterpret_cast<const ObLinkedMacroBlockHeader *>(buf);
    buf += sizeof(ObLinkedMacroBlockHeader);
    buf_pos_ += sizeof(ObLinkedMacroBlockHeader);
    buf_len_ = sizeof(ObMacroBlockCommonHeader) + common_header_->get_payload_size();

    int64_t item_count = linked_header_->item_count_;
    int64_t buf_len = 0;
    int64_t copy_buf_pos = 0;
    int64_t data_len = 0;
    if (0 == item_count) {
      // when current item's size is larger than 2M bytes, we must keep read
      // macro block until it reaches condition 1 == item_count_ namely the end
      // block of current item.
      allocator_.reuse();
      ObLinkedMacroBlockItemHeader *item_header =
        reinterpret_cast<ObLinkedMacroBlockItemHeader *>(buf_ + buf_pos_);
      const int64_t item_size = item_header->payload_size_;
      const int64_t request_size = item_size + sizeof(ObLinkedMacroBlockItemHeader);
      char *big_buf = nullptr;
      if (OB_ISNULL(big_buf = static_cast<char *>(allocator_.alloc(request_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(item_size));
      } else {
        data_len = common_header_->get_payload_size() - sizeof(ObLinkedMacroBlockHeader);
        MEMCPY(big_buf, buf_ + buf_pos_, data_len);
        buf_ = big_buf;
        copy_buf_pos += data_len;
      }
      while (OB_SUCC(ret) && 0 == item_count) {
        if (OB_FAIL(block_reader_.iter_read_block(buf, buf_len, tmp_block_id))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to read block", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          common_header_ = reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
          buf += sizeof(ObMacroBlockCommonHeader);
          linked_header_ = reinterpret_cast<const ObLinkedMacroBlockHeader *>(buf);
          buf += sizeof(ObLinkedMacroBlockHeader);
          data_len = common_header_->get_payload_size() - sizeof(ObLinkedMacroBlockHeader);
          MEMCPY(buf_ + copy_buf_pos, buf, data_len);
          copy_buf_pos += data_len;
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

int ObLinkedMacroBlockItemReader::parse_item(
  char *&item_buf, int64_t &item_buf_len, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemHeader *item_header =
    reinterpret_cast<ObLinkedMacroBlockItemHeader *>(buf_ + buf_pos_);
  if (OB_UNLIKELY(!item_header->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("item header is invalid", K(ret));
  } else {
    const int64_t size = item_header->payload_size_ + sizeof(ObLinkedMacroBlockItemHeader);
    int64_t offset = 0;
    if (OB_UNLIKELY(0 == buf_pos_)) {  // represent a big buf for a large item
      offset = sizeof(ObMacroBlockCommonHeader) + sizeof(ObLinkedMacroBlockHeader);
    } else {
      offset = buf_pos_;
    }
    if (OB_FAIL(addr.set_block_addr(buf_block_id_, offset, size))) {
      LOG_WARN("fail to set block address", K(ret), K(buf_block_id_), K(offset), K(size));
    } else {
      buf_pos_ += sizeof(ObLinkedMacroBlockItemHeader);
      item_buf = buf_ + buf_pos_;
      item_buf_len = item_header->payload_size_;
      buf_pos_ += item_buf_len;

      if (OB_FAIL(check_item_crc(item_header->payload_crc_, item_buf, item_buf_len))) {
        LOG_WARN("item checksum error", K(ret), KPC(item_header));
      }
    }
  }
  return ret;
}

int ObLinkedMacroBlockItemReader::check_item_crc(
  const int32_t crc, const char *item_buf, const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  const int32_t calc_item_crc = static_cast<int32_t>(ob_crc64(item_buf, item_buf_len));
  if (crc != calc_item_crc) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("item checksum error", K(ret), K(crc), K(calc_item_crc));
  }
  return ret;
}

int ObLinkedMacroBlockItemReader::read_item(const ObIArray<MacroBlockId> &block_list,
  const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!addr.is_valid() || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("disk addr is invalid", K(ret), K(addr));
  } else {
    // item not cross the boundary of macro block
    if (OB_LIKELY(addr.offset() + addr.size() <= OB_SERVER_BLOCK_MGR.get_macro_block_size())) {
      blocksstable::ObMacroBlockHandle handler;
      if (OB_FAIL(ObLinkedMacroBlockReader::pread_block(addr, handler))) {
        LOG_WARN("failed to pread block", K(ret), K(addr));
      } else {
        const char *item_buf_with_head = handler.get_buffer();
        const ObLinkedMacroBlockItemHeader *item_header =
          reinterpret_cast<const ObLinkedMacroBlockItemHeader *>(item_buf_with_head);
        // addr.size_ include the item header size
        item_buf_len = addr.size() - sizeof(ObLinkedMacroBlockItemHeader);
        if (OB_UNLIKELY(!item_header->is_valid())) {
          ret = OB_ERR_SYS;
          LOG_WARN("item header is invalid", K(ret), KPC(item_header));
        } else if (OB_FAIL(check_item_crc(item_header->payload_crc_,
                     item_buf_with_head + sizeof(ObLinkedMacroBlockItemHeader), item_buf_len))) {
          LOG_WARN("item checksum error", K(ret), KPC(item_header));
        } else {
          MEMCPY(item_buf, item_buf_with_head + sizeof(ObLinkedMacroBlockItemHeader), item_buf_len);
        }
      }
    } else if (OB_FAIL(read_large_item(block_list, addr, item_buf, item_buf_len))) {
      LOG_WARN("failed to read the item which cross the boundary of macro block", K(ret), K(addr));
    }
  }

  return ret;
}

// TODO(fenggu.yh) The current method need traversal, so the performance
// is poor when many items cross block boundary. A simple optimization is
// recording the index of the macro block in the linked list in the
// ObMetaDiskaddr, so that the next macro block can be quickly located in the
// array
int ObLinkedMacroBlockItemReader::get_next_block_id(const ObIArray<MacroBlockId> &block_list,
  const MacroBlockId &block_id, blocksstable::MacroBlockId &next_block_id)
{
  int ret = OB_SUCCESS;

  int64_t i = 0;
  for (; i < block_list.count(); ++i) {
    if (block_list.at(i) == block_id) {
      break;
    }
  }
  if (OB_UNLIKELY(i >= block_list.count())) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("block id not exist", K(ret), K(block_id), K(i));
  } else if (OB_UNLIKELY(i == block_list.count() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this is last block", K(ret), K(block_id), K(i));
  } else {
    next_block_id = block_list.at(i + 1);
  }

  return ret;
}

int ObLinkedMacroBlockItemReader::read_large_item(const ObIArray<MacroBlockId> &block_list,
  const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len)
{
  LOG_INFO("reading item cross the block boundary is inefficient", K(addr));

  int ret = OB_SUCCESS;
  ObMacroBlockHandle handler;
  MacroBlockId block_id;
  int64_t offset = 0;
  int64_t size = 0;
  if (OB_FAIL(addr.get_block_addr(block_id, offset, size))) {
    LOG_WARN("fail to get block address", K(ret), K(addr));
  } else if (OB_FAIL(ObLinkedMacroBlockReader::read_block_by_id(block_id, handler))) {
    LOG_WARN("fail to read block by id", K(ret));
  } else {
    // item_buf not include the item header
    item_buf_len = size - sizeof(ObLinkedMacroBlockItemHeader);
    const char *buf = handler.get_buffer();
    const ObMacroBlockCommonHeader *common_header =
      reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
    buf += sizeof(ObMacroBlockCommonHeader);
    const ObLinkedMacroBlockHeader *linked_header =
      reinterpret_cast<const ObLinkedMacroBlockHeader *>(buf);
    buf += sizeof(ObLinkedMacroBlockHeader);
    const ObLinkedMacroBlockItemHeader *item_header =
      reinterpret_cast<const ObLinkedMacroBlockItemHeader *>(buf);
    buf += sizeof(ObLinkedMacroBlockItemHeader);

    int64_t data_len = common_header->get_payload_size() - sizeof(ObLinkedMacroBlockHeader) -
      sizeof(ObLinkedMacroBlockItemHeader);
    int64_t item_count = linked_header->item_count_;
    int64_t copy_item_buf_pos = 0;
    int64_t left_item_buf_len = item_buf_len;

    if (OB_UNLIKELY(0 != item_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first item count of large item must be zero", K(ret), K(item_count));
    } else if (OB_UNLIKELY(item_header->payload_size_ != item_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item_buf_len mismatch with header payload_size", K(ret), KPC(item_header),
        K(item_buf_len));
    } else if (OB_UNLIKELY(data_len > left_item_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data len too large", K(ret), K(data_len), K(left_item_buf_len));
    } else {
      MEMCPY(item_buf, buf, data_len);
      copy_item_buf_pos += data_len;
      left_item_buf_len -= data_len;
      // when item's size is larger than 2M bytes, we must keep read macro block
      // until it reaches condition 1 == item_count_ namely the end block of
      // current item.
      MacroBlockId next_block_id;
      while (OB_SUCC(ret) && 0 == item_count) {
        if (OB_FAIL(get_next_block_id(block_list, block_id, next_block_id))) {
          LOG_WARN("fail to get next block id", K(ret));
        } else if (OB_FAIL(ObLinkedMacroBlockReader::read_block_by_id(next_block_id, handler))) {
          LOG_WARN("fail to read block by id", K(ret));
        } else {
          buf = handler.get_buffer();
          common_header = reinterpret_cast<const ObMacroBlockCommonHeader *>(buf);
          buf += sizeof(ObMacroBlockCommonHeader);
          linked_header = reinterpret_cast<const ObLinkedMacroBlockHeader *>(buf);
          buf += sizeof(ObLinkedMacroBlockHeader);
          data_len = common_header->get_payload_size() - sizeof(ObLinkedMacroBlockHeader);

          if (OB_UNLIKELY(data_len > left_item_buf_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data len too large", K(ret), K(data_len), K(left_item_buf_len));
          } else {
            MEMCPY(item_buf + copy_item_buf_pos, buf, data_len);
            copy_item_buf_pos += data_len;
            left_item_buf_len -= data_len;
            item_count = linked_header->item_count_;
            block_id = next_block_id;
          }
        }
      }
    }
  }
  return ret;
}

void ObLinkedMacroBlockItemReader::reset()
{
  is_inited_ = false;
  common_header_ = nullptr;
  linked_header_ = nullptr;
  block_reader_.reset();
  buf_ = nullptr;
  buf_pos_ = 0;
  buf_len_ = 0;
  allocator_.reset();
}

ObIArray<MacroBlockId> &ObLinkedMacroBlockItemReader::get_meta_block_list()
{
  return block_reader_.get_meta_block_list();
}

}  // end namespace storage
}  // end namespace oceanbase
