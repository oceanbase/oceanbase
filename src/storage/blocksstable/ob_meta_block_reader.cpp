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

#include "ob_meta_block_reader.h"
#include "ob_store_file.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase {
namespace blocksstable {

ObMetaBlockReader::ObMetaBlockReader()
    : allocator_(ObModIds::OB_META_BLOCK_READER),
      io_desc_(),
      buf_(NULL),
      linked_header_(NULL),
      macro_blocks_(),
      file_ctx_(allocator_),
      sstable_macro_block_count_(0),
      is_inited_(false)
{
  io_desc_.category_ = SYS_IO;
  io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
  io_desc_.req_deadline_time_ = 0;
}

ObMetaBlockReader::~ObMetaBlockReader()
{}

int ObMetaBlockReader::init(const int64_t file_id, const int64_t file_size)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_META_BLOCK, file_ctx_))) {
    LOG_WARN("Failed to init file ctx", K(ret));
  } else {
    ObStoreFileInfo file_info;
    file_info.file_id_ = file_id;
    file_info.file_size_ = file_size;

    if (file_ctx_.need_file_id_list() && OB_FAIL(file_ctx_.file_list_.push_back(file_info))) {
      LOG_WARN("failed to add file info", K(ret), K(file_info));
    } else if (!file_ctx_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(file_ctx_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMetaBlockReader::read_old_macro_block(const int64_t entry_block)
{
  int ret = OB_SUCCESS;
  ObSuperBlockV2::MetaEntry fake_meta_entry;
  fake_meta_entry.block_index_ = entry_block;
  fake_meta_entry.log_seq_ = 0;
  fake_meta_entry.file_id_ = 0;
  fake_meta_entry.file_size_ = INT64_MAX;

  if (OB_FAIL(read(fake_meta_entry))) {
    LOG_WARN("failed to read", K(ret), K(fake_meta_entry));
  }

  return ret;
}

int ObMetaBlockReader::read(const ObSuperBlockV2::MetaEntry& meta_entry)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  int64_t header_size = 0;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  ObMacroBlockHandleV1 block_handle[2];
  ObMacroBlockReadInfo read_info;

  read_info.offset_ = 0;
  read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
  read_info.io_desc_ = io_desc_;

  if (OB_FAIL(init(meta_entry.file_id_, meta_entry.file_size_))) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(get_meta_blocks(meta_entry.block_index_))) {
    STORAGE_LOG(WARN, "Fail to get meta blocks", K(ret), K(meta_entry));
  } else {
    if (macro_blocks_.count() > 0) {
      ObMacroBlockCtx macro_block_ctx;
      macro_block_ctx.file_ctx_ = &file_ctx_;
      macro_block_ctx.sstable_block_id_.macro_block_id_ = macro_blocks_.at(macro_blocks_.count() - 1);
      macro_block_ctx.sstable_block_id_.macro_block_id_in_files_ = 0;
      read_info.macro_block_ctx_ = &macro_block_ctx;
      const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
      if (OB_FAIL(OB_STORE_FILE.async_read_block_v1(read_info, block_handle[cur_pos]))) {
        STORAGE_LOG(WARN, "Fail to async read meta block, ", K(ret), K(macro_blocks_.at(macro_blocks_.count() - 1)));
      }

      for (int64_t i = macro_blocks_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        if (0 != i) {
          macro_block_ctx.sstable_block_id_.macro_block_id_ = macro_blocks_.at(i - 1);
          macro_block_ctx.sstable_block_id_.macro_block_id_in_files_++;
          if (OB_FAIL(OB_STORE_FILE.async_read_block_v1(read_info, block_handle[1 - cur_pos]))) {
            STORAGE_LOG(WARN, "Fail to async read meta block, ", K(ret), K(macro_blocks_.at(i - 1)));
          }
        }
        pos = 0;
        common_header.reset();
        if (OB_FAIL(block_handle[cur_pos].wait(io_timeout_ms))) {
          STORAGE_LOG(WARN, "Fail to wait meta block io, ", K(ret));
        } else if (OB_FAIL(common_header.deserialize(
                       block_handle[cur_pos].get_buffer(), block_handle[cur_pos].get_data_size(), pos))) {
          STORAGE_LOG(
              WARN, "Fail deserialize common header, ", K(ret), K(pos), K(block_handle[cur_pos]), K(common_header));
        } else {
          buf_ = block_handle[cur_pos].get_buffer();
          cur_pos = 1 - cur_pos;
          linked_header_ = reinterpret_cast<const ObLinkedMacroBlockHeader*>(buf_ + common_header.get_serialize_size());
          header_size = common_header.get_serialize_size() + linked_header_->header_size_;

          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(!common_header.is_valid()) || OB_UNLIKELY(!linked_header_->is_valid())) {
              ret = OB_INVALID_DATA;
              STORAGE_LOG(WARN, "Invalid data, ", K(ret), K(common_header), K(*linked_header_));
            } else if (OB_FAIL(
                           parse(common_header, *linked_header_, buf_ + header_size, read_info.size_ - header_size))) {
              STORAGE_LOG(WARN, "Fail to parse meta block, ", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObMetaBlockReader::get_meta_blocks(const int64_t entry_block)
{
  int ret = OB_SUCCESS;
  int64_t cur_pos = 0;
  int64_t block_idx = entry_block;
  int64_t pos = 0;
  ObMacroBlockCommonHeader common_header;
  ObMacroBlockHandleV1 block_handle[2];
  ObMacroBlockReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ = common_header.get_serialize_size();
  read_info.io_desc_ = io_desc_;

  if (block_idx > 0) {
    ObMacroBlockCtx macro_block_ctx;
    macro_block_ctx.file_ctx_ = &file_ctx_;
    macro_block_ctx.sstable_block_id_.macro_block_id_.set_local_block_id(block_idx);
    macro_block_ctx.sstable_block_id_.macro_block_id_in_files_ = sstable_macro_block_count_ - 1;
    read_info.macro_block_ctx_ = &macro_block_ctx;
    const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
    if (OB_FAIL(OB_STORE_FILE.async_read_block_v1(read_info, block_handle[cur_pos]))) {
      STORAGE_LOG(WARN, "Fail to async read meta block, ", K(ret), K(block_idx));
    } else {
      do {
        common_header.reset();
        pos = 0;
        if (OB_FAIL(block_handle[cur_pos].wait(io_timeout_ms))) {
          STORAGE_LOG(WARN, "Fail to wait meta block io, ", K(ret));
        } else if (OB_FAIL(macro_blocks_.push_back(macro_block_ctx.get_macro_block_id()))) {
          STORAGE_LOG(WARN, "Fail to push block index", K(ret), K(block_idx));
        } else if (OB_FAIL(common_header.deserialize(
                       block_handle[cur_pos].get_buffer(), block_handle[cur_pos].get_data_size(), pos))) {
          STORAGE_LOG(
              WARN, "Fail to deserialize common header", K(ret), K(pos), K(block_handle[cur_pos]), K(common_header));
        } else {
          buf_ = block_handle[cur_pos].get_buffer();
          block_idx = common_header.get_previous_block_index();

          if (block_idx > 0) {
            cur_pos = 1 - cur_pos;
            macro_block_ctx.sstable_block_id_.macro_block_id_.set_local_block_id(block_idx);
            macro_block_ctx.sstable_block_id_.macro_block_id_in_files_--;
            if (OB_FAIL(OB_STORE_FILE.async_read_block_v1(read_info, block_handle[cur_pos]))) {
              STORAGE_LOG(WARN, "Fail to async read meta block, ", K(ret), K(block_idx));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(!common_header.is_valid())) {
              ret = OB_INVALID_DATA;
              STORAGE_LOG(WARN, "Invalid data, ", K(ret), K(common_header));
            }
          }
        }
      } while (OB_SUCC(ret) && block_idx > 0);
    }
  }
  return ret;
}

void ObMetaBlockReader::reset()
{
  buf_ = NULL;
  linked_header_ = NULL;
  macro_blocks_.reset();
  file_ctx_.reset();
  is_inited_ = false;
  sstable_macro_block_count_ = 0;
}

}  // namespace blocksstable
}  // namespace oceanbase
