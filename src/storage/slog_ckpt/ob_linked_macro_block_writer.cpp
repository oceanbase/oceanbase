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
  : is_inited_(false),
    type_(WriteType::LMI_MAX_TYPE),
    write_ctx_(),
    handle_(),
    entry_block_id_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    tenant_epoch_id_(0),
    fd_dispenser_(nullptr),
    macro_info_param_()
{
}

int ObLinkedMacroBlockWriter::init_for_slog_ckpt(
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(tenant_epoch_id), KPC(fd_dispenser));
  } else {
    tenant_id_ = tenant_id;
    tenant_epoch_id_ = tenant_epoch_id;
    fd_dispenser_ = fd_dispenser;
    type_ = WriteType::PRIV_SLOG_CKPT;
    is_inited_ = true;
  }
  return ret;
}

int ObLinkedMacroBlockWriter::init_for_macro_info(const ObLinkedMacroInfoWriteParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    tenant_id_        = is_valid_tenant_id(MTL_ID()) ? MTL_ID() : OB_SERVER_TENANT_ID;
    type_             = param.type_;
    macro_info_param_ = param;
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
  ObStorageObjectOpt opt;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockWriter has not been inited", K(ret));
  } else if (GCTX.is_shared_storage_mode() && (WriteType::PRIV_SLOG_CKPT == type_ && nullptr == fd_dispenser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in shared_storage_mode, type is private slog checkpoint and fd dispenser is nullptr", K(ret), K(type_));
  } else if (OB_UNLIKELY(nullptr == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    switch (type_) {
      case WriteType::PRIV_SLOG_CKPT: {
        if (OB_NOT_NULL(fd_dispenser_)) {
          opt.set_private_ckpt_opt(tenant_id_, tenant_epoch_id_, fd_dispenser_->acquire_new_file_id());
        } else {
          opt.set_private_ckpt_opt(tenant_id_, tenant_epoch_id_, 0);
        }
        break;
      }
      case WriteType::SHARED_MAJOR_MACRO_INFO: {
        opt.set_ss_share_meta_macro_object_opt(macro_info_param_.tablet_id_.id(), macro_info_param_.start_macro_seq_++, 0);
        break;
      }
      case WriteType::SHARED_INC_MACRO_INFO : {
        opt.set_ss_tablet_sub_meta_opt(macro_info_param_.ls_id_.id(),
                                       macro_info_param_.tablet_id_.id(),
                                       macro_info_param_.op_id_,
                                       macro_info_param_.start_macro_seq_++,
                                       macro_info_param_.tablet_id_.is_ls_inner_tablet(),
                                       macro_info_param_.reorganization_scn_);
        break;
      }
      case WriteType::PRIV_MACRO_INFO: {
        opt.set_private_meta_macro_object_opt(macro_info_param_.tablet_id_.id(), macro_info_param_.tablet_transfer_seq_);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unknown type", K(ret), K(type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObStorageObjectWriteInfo write_info;
    write_info.size_ = buf_len;
    write_info.buffer_ = buf;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sys_module_id(ObIOModule::LINKED_MACRO_BLOCK_IO);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = tenant_id_;
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
  type_ = WriteType::LMI_MAX_TYPE;
  write_ctx_.reset();
  handle_.reset();
  entry_block_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_epoch_id_ = 0;
  fd_dispenser_ = nullptr;
  macro_info_param_.reset();
}

void ObLinkedMacroBlockWriter::reuse_for_next_round()
{
  is_inited_ = false;
  handle_.reset();
  entry_block_id_.reset();
  OB_ASSERT(-1 == macro_info_param_.start_macro_seq_); // for shared macro, can't reuse sequence
}

//================== ObLinkedMacroBlockItemWriter =============================

ObLinkedMacroBlockItemWriter::ObLinkedMacroBlockItemWriter()
  : is_inited_(false), is_closed_(false), written_items_cnt_(0),
    allocator_(), block_writer_(), io_buf_(nullptr), io_buf_size_(0),
    io_buf_pos_(0), common_header_(), linked_header_(), write_callback_(nullptr)
{
}

int ObLinkedMacroBlockItemWriter::init_for_slog_ckpt(
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id,
    const ObMemAttr &mem_attr,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockItemWriter has already been inited", K(ret));
  } else if (FALSE_IT(allocator_.set_attr(mem_attr))) {
  } else if (OB_FAIL(block_writer_.init_for_slog_ckpt(tenant_id, tenant_epoch_id, fd_dispenser))) {
    LOG_WARN("fail to init meta block writer", K(ret), K(tenant_id), K(tenant_epoch_id), K(fd_dispenser));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else if (FALSE_IT(MEMSET(io_buf_, 0, macro_block_size))) {
  } else if (OB_FAIL(common_header_.set_attr(ObMacroBlockCommonHeader::LinkedBlock))) {
    LOG_WARN("fail to set type for common header", K(ret), K(common_header_));
  } else {
    io_buf_size_ = macro_block_size;
    io_buf_pos_ = ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size();
    written_items_cnt_ = 0;
    is_inited_ = true;
    is_closed_ = false;
  }
  return ret;
}

int ObLinkedMacroBlockItemWriter::init_for_macro_info(
  const ObLinkedMacroInfoWriteParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  const uint64_t tenant_id = MTL_ID();
  const ObMemAttr mem_attr(is_valid_tenant_id(tenant_id) ? tenant_id : OB_SERVER_TENANT_ID, "ObjLinkWriter");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLinkedMacroBlockItemWriter has already been inited", K(ret));
  } else if (FALSE_IT(allocator_.set_attr(mem_attr))) {
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(block_writer_.init_for_macro_info(param))) {
    LOG_WARN("fail to init meta block writer", K(ret), K(param));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(macro_block_size));
  } else if (FALSE_IT(MEMSET(io_buf_, 0, macro_block_size))) {
  } else if (OB_FAIL(common_header_.set_attr(ObMacroBlockCommonHeader::LinkedBlock))) {
    LOG_WARN("fail to set type for common header", K(ret), K(common_header_));
  } else {
    io_buf_size_ = macro_block_size;
    io_buf_pos_ = ObMacroBlockCommonHeader::get_serialize_size() + linked_header_.get_serialize_size();
    written_items_cnt_ = 0;
    write_callback_ = param.write_callback();
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
  allocator_.reset();
  io_buf_ = nullptr;
  io_buf_size_ = 0;
  io_buf_pos_ = 0;
  common_header_.reset();
  linked_header_.reset();
}

}  // end namespace storage
}  // end namespace oceanbase
