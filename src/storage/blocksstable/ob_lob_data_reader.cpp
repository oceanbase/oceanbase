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

#include "ob_lob_data_reader.h"
#include "ob_sstable_printer.h"
#include "storage/ob_sstable.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace blocksstable {

ObLobMicroBlockReader::ObLobMicroBlockReader()
{}

int ObLobMicroBlockReader::read(const char* buf, const int64_t buf_len, const char*& out_buf, int64_t& out_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    const ObLobMicroBlockHeader* header = reinterpret_cast<const ObLobMicroBlockHeader*>(buf);
    if (OB_FAIL(check_micro_header(header))) {
      LOG_WARN("fail to check micro block header", K(ret));
    } else {
      out_buf = buf + sizeof(ObLobMicroBlockHeader);
      out_buf_len = buf_len - sizeof(ObLobMicroBlockHeader);
    }
  }
  return ret;
}

int ObLobMicroBlockReader::check_micro_header(const ObLobMicroBlockHeader* header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(header));
  } else if (OB_UNLIKELY(header->header_size_ != sizeof(ObLobMicroBlockHeader))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "invalid header size", K(ret), "correct_header_size", sizeof(ObLobMicroBlockHeader), K(header->header_size_));
  } else if (OB_UNLIKELY(LOB_MICRO_BLOCK_HEADER_VERSION != header->version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob version", K(ret), LITERAL_K(LOB_MICRO_BLOCK_HEADER_VERSION), K(header->version_));
  } else if (OB_UNLIKELY(LOB_MICRO_BLOCK_HEADER_MAGIC != header->magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid header magic", K(ret), LITERAL_K(LOB_MICRO_BLOCK_HEADER_MAGIC), K(header->magic_));
  }
  return ret;
}

ObLobMacroBlockReader::ObLobMacroBlockReader()
    : is_inited_(false),
      curr_block_id_(),
      micro_index_reader_(),
      micro_block_reader_(),
      macro_reader_(),
      macro_handle_(),
      data_buf_(NULL),
      full_meta_()
{}

int ObLobMacroBlockReader::init()
{
  int ret = OB_SUCCESS;
  is_inited_ = true;
  return ret;
}

int ObLobMacroBlockReader::open(const MacroBlockId& block_id, const bool is_sys_read,
    const ObFullMacroBlockMeta& full_meta, const common::ObPartitionKey& pkey, ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaHandle meta_handle;
  ObStorageFile* file = NULL;
  UNUSED(pkey);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!block_id.is_valid() || !full_meta.is_valid() || !file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(block_id), K(full_meta), K(file_handle));
  } else if (!full_meta.meta_->is_lob_data_block()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Metaimage fail to get lob meta", K(block_id), K(ret));
  } else if (full_meta.meta_->rowkey_column_number_ >= full_meta.meta_->column_number_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, macro meta column num less than rowkey column num",
        K(ret),
        "column_num",
        full_meta.meta_->column_number_,
        "rowkey_obj_cnt",
        full_meta.meta_->rowkey_column_number_);
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle));
  } else {
    const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
    ObMacroBlockReadInfo read_info;
    macro_handle_.reset();
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER);
    ObStoreFileCtx file_ctx(allocator);
    full_meta_ = full_meta;
    ObMacroBlockCtx macro_block_ctx;  // TODO(): fix it for ofs later
    file_ctx.file_system_type_ = STORE_FILE_SYSTEM_LOCAL;
    macro_block_ctx.file_ctx_ = &file_ctx;
    macro_block_ctx.sstable_block_id_.macro_block_id_ = block_id;

    curr_block_id_ = block_id;
    read_info.macro_block_ctx_ = &macro_block_ctx;
    read_info.io_desc_.category_ = is_sys_read ? SYS_IO : USER_IO;
    read_info.io_desc_.wait_event_no_ =
        is_sys_read ? ObWaitEventIds::DB_FILE_COMPACT_READ : ObWaitEventIds::DB_FILE_DATA_READ;
    read_info.offset_ = 0;
    read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
    macro_handle_.set_file(file);
    if (OB_FAIL(file->read_block(read_info, macro_handle_))) {
      STORAGE_LOG(WARN, "Fail to sync read lob macro block, ", K(ret));
    } else {
      const char* index_buf = macro_handle_.get_buffer() + macro_meta->micro_block_index_offset_;
      const int64_t index_size = macro_meta->micro_block_endkey_offset_ - macro_meta->micro_block_index_offset_;
      if (OB_FAIL(micro_index_reader_.transform(
              index_buf, static_cast<int32_t>(index_size), macro_meta->micro_block_count_))) {
        LOG_WARN("fail to transform micro block index reader",
            K(ret),
            K(index_size),
            "size_array_offset",
            macro_meta->micro_block_endkey_offset_,
            "index_offset",
            macro_meta->micro_block_index_offset_);
      } else {
        data_buf_ = macro_handle_.get_buffer() + macro_meta->micro_block_data_offset_;
      }
    }
  }
  return ret;
}

int ObLobMacroBlockReader::open(const ObStoreRowkey& rowkey, const uint16_t column_id, const MacroBlockId& block_id,
    const bool is_sys_read, const ObFullMacroBlockMeta& full_meta, const common::ObPartitionKey& partition_key,
    ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!block_id.is_valid() || !full_meta.is_valid() || !file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(block_id), K(full_meta), K(file_handle));
  } else if (OB_FAIL(open(block_id, is_sys_read, full_meta, partition_key, file_handle))) {
    LOG_WARN("fail to open macro block", K(ret));
  } else if (rowkey.is_valid() && OB_FAIL(check_macro_meta(rowkey, column_id))) {
    LOG_WARN("fail to check macro meta", K(ret));
  }
  return ret;
}

int ObLobMacroBlockReader::check_macro_meta(const ObStoreRowkey& rowkey, const uint16_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid() || 0 == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(rowkey), K(column_id));
  } else {
    ObStoreRowkey endkey;
    endkey.assign(full_meta_.meta_->endkey_, full_meta_.meta_->rowkey_column_number_);
    if (rowkey != endkey) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid rowkey", K(ret), K(rowkey), K(endkey));
    }
  }
  return ret;
}

int ObLobMacroBlockReader::read_next_micro_block(const char*& out_buf, int64_t& out_buf_len)
{
  int ret = OB_SUCCESS;
  ObLobMicroIndexItem index_item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockReader has not been inited", K(ret));
  } else if (OB_FAIL(micro_index_reader_.get_next_index_item(index_item))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next index item", K(ret));
    }
  } else {
    const char* buf = data_buf_ + index_item.offset_;
    const int64_t size = index_item.data_size_;
    bool is_compressed = false;
    const char* decompress_buf = NULL;
    int64_t decompress_len = 0;
    LOG_DEBUG("next_index_item", K(index_item));
    if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(buf, size, LOB_MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_WARN("fail to check record", K(ret));
    } else if (OB_FAIL(macro_reader_.decompress_data(
                   full_meta_, buf, size, decompress_buf, decompress_len, is_compressed))) {
      LOG_WARN("fail to decompress data", K(ret));
    } else if (OB_FAIL(micro_block_reader_.read(decompress_buf, decompress_len, out_buf, out_buf_len))) {
      LOG_WARN("fail to read micro block data", K(ret));
    }
  }
  return ret;
}

void ObLobMacroBlockReader::reset()
{
  is_inited_ = false;
  curr_block_id_.reset();
  data_buf_ = NULL;
  full_meta_.reset();
}

ObLobDataReader::ObLobDataReader()
    : is_inited_(false),
      reader_(),
      logic2phy_map_(NULL),
      sstable_(nullptr),
      allocator_(ObModIds::OB_LOB_READER),
      buf_(NULL),
      pos_(0),
      byte_size_(0),
      is_sys_read_(false),
      rowkey_(),
      column_id_(0),
      file_handle_()
{}

ObLobDataReader::~ObLobDataReader()
{}

int ObLobDataReader::init(const bool is_sys_read, const ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobDataReader has already been inited", K(ret));
  } else if (OB_FAIL(reader_.init())) {
    LOG_WARN("fail to init ObLobMacroBlockReader", K(ret));
  } else if (OB_FAIL(file_handle_.assign(sstable.get_storage_file_handle()))) {
    LOG_WARN("fail to get pg_file", K(ret), K(sstable.get_storage_file_handle()));
  } else {
    logic2phy_map_ = &sstable.get_logic_block_map();
    is_sys_read_ = is_sys_read;
    sstable_ = &sstable;
    is_inited_ = true;
  }
  return ret;
}

void ObLobDataReader::reuse()
{
  allocator_.reuse();
  inner_reuse();
}

void ObLobDataReader::inner_reuse()
{
  buf_ = NULL;
  pos_ = 0;
  byte_size_ = 0;
  rowkey_.reset();
  column_id_ = 0;
}

void ObLobDataReader::reset()
{
  is_inited_ = false;
  reader_.reset();
  logic2phy_map_ = NULL;
  allocator_.reset();
  buf_ = NULL;
  pos_ = 0;
  byte_size_ = 0;
  is_sys_read_ = false;
  rowkey_.reset();
  column_id_ = 0;
  file_handle_.reset();
}

int ObLobDataReader::read_lob_data_impl(
    const ObStoreRowkey& rowkey, const uint16_t column_id, const ObObj& src_obj, ObObj& dst_obj)
{
  int ret = OB_SUCCESS;
  const ObLobData* index_data = src_obj.get_lob_value();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_ISNULL(index_data) || !src_obj.is_lob_outrow()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(index_data), K(src_obj.is_lob_outrow()));
  } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(index_data->byte_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(index_data->byte_size_));
  } else {
    const int64_t direct_block_cnt = std::min(index_data->get_direct_cnt(), static_cast<int64_t>(index_data->idx_cnt_));
    byte_size_ = index_data->byte_size_;
    rowkey_ = rowkey;
    column_id_ = column_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < direct_block_cnt; ++i) {
      const ObLobIndex& index = index_data->lob_idx_[i];
      if (OB_FAIL(read_data_macro(index))) {
        LOG_WARN("fail to read data macro", K(ret), K(index));
      }
    }

    if (OB_SUCC(ret) && direct_block_cnt < index_data->idx_cnt_) {
      const ObLobIndex& index = index_data->lob_idx_[direct_block_cnt];
      ObArray<ObLobIndex> lob_indexes;
      if (OB_FAIL(read_all_direct_index(index, lob_indexes))) {
        LOG_WARN("fail to read all direct index", K(ret), K(index));
      } else if (0 == lob_indexes.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, lob index count must not be zero", K(ret), K(lob_indexes.count()));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < lob_indexes.count(); ++i) {
        if (OB_FAIL(read_data_macro(lob_indexes.at(i)))) {
          LOG_WARN("fail to read data macro", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (pos_ != byte_size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, read size is not as expected", K(ret), K(pos_), K(byte_size_));
      } else {
        dst_obj.copy_meta_type(src_obj.get_meta());
        dst_obj.set_lob_value(src_obj.get_type(), buf_, static_cast<int32_t>(pos_));
      }
    }
  }
  return ret;
}

int ObLobDataReader::read_lob_data(const ObObj& src_obj, ObObj& dst_obj)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey rowkey;
  uint16_t column_id = 0;
  inner_reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!src_obj.is_lob_outrow())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(src_obj.is_lob_outrow()), K(src_obj));
  } else if (OB_FAIL(read_lob_data_impl(rowkey, column_id, src_obj, dst_obj))) {
    LOG_WARN("fail to read lob data", K(ret));
  }
  return ret;
}

int ObLobDataReader::read_lob_data(
    const ObStoreRowkey& rowkey, const uint16_t column_id, const ObObj& src_obj, ObObj& dst_obj)
{
  int ret = OB_SUCCESS;
  inner_reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid() || 0 == column_id || !src_obj.is_lob_outrow())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(rowkey), K(column_id), K(src_obj.is_lob_outrow()));
  } else if (OB_FAIL(read_lob_data_impl(rowkey, column_id, src_obj, dst_obj))) {
    LOG_WARN("fail to read lob data", K(ret));
  }
  return ret;
}

int ObLobDataReader::read_data_macro(const ObLobIndex& lob_index)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  MacroBlockId block_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_FAIL(logic2phy_map_->get(lob_index.logic_macro_id_, block_id))) {
    LOG_WARN("fail to get from map", K(lob_index), K(ret));
  } else if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(block_id));
  } else if (OB_FAIL(sstable_->get_meta(block_id, full_meta))) {
    LOG_WARN("fail to get meta", K(ret), K(block_id));
  } else if (OB_FAIL(reader_.open(rowkey_,
                 column_id_,
                 block_id,
                 is_sys_read_,
                 full_meta,
                 sstable_->get_partition_key(),
                 file_handle_))) {
    LOG_WARN("fail to open ObLobMacroBlockReader", K(ret), K(block_id));
  } else {
    const char* out_buf = NULL;
    int64_t out_buf_len = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(reader_.read_next_micro_block(out_buf, out_buf_len))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to read next micro block", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_UNLIKELY(out_buf_len + pos_ > byte_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, read data is more than expected", K(ret), K(out_buf_len), K(pos_), K(byte_size_));
      } else {
        LOG_DEBUG("current micro offset", K(pos_), K(block_id));
        MEMCPY(buf_ + pos_, out_buf, out_buf_len);
        pos_ += out_buf_len;
      }
    }
  }
  return ret;
}

int ObLobDataReader::read_index_macro(const MacroBlockId& block_id, ObIArray<ObLobIndex>& lob_indexes)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  lob_indexes.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(block_id));
  } else if (OB_FAIL(sstable_->get_meta(block_id, full_meta))) {
    LOG_WARN("fail to get meta", K(ret), K(block_id));
  } else if (OB_FAIL(reader_.open(rowkey_,
                 column_id_,
                 block_id,
                 is_sys_read_,
                 full_meta,
                 sstable_->get_partition_key(),
                 file_handle_))) {
    LOG_WARN("fail to open ObLobMacroBlockReader", K(ret), K(block_id));
  } else {
    const char* out_buf = NULL;
    int64_t out_buf_len = 0;
    LOG_DEBUG("lob index macro id", K(block_id));
    while (OB_SUCC(ret)) {
      if (OB_FAIL(reader_.read_next_micro_block(out_buf, out_buf_len))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to read next micro block", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(parse_lob_index(out_buf, out_buf_len, lob_indexes))) {
        LOG_WARN("fail to parse lob index", K(ret));
      }
    }
  }
  return ret;
}

int ObLobDataReader::parse_lob_index(const char* buf, const int64_t buf_len, ObIArray<ObLobIndex>& lob_indexes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    const char* buf_curr = buf;
    const char* buf_end = buf + buf_len;
    int64_t left_size = buf_len;
    while (OB_SUCC(ret) && buf_curr < buf_end) {
      ObLobIndex index;
      int64_t pos = 0;
      if (OB_FAIL(index.deserialize(buf_curr, left_size, pos))) {
        LOG_WARN("fail to deserialize lob index", K(ret));
      } else if (OB_FAIL(lob_indexes.push_back(index))) {
        LOG_WARN("fail to push back lob index", K(ret));
      } else {
        buf_curr += pos;
        left_size -= pos;
      }
    }
  }
  return ret;
}

int ObLobDataReader::read_all_direct_index(const ObLobIndex& index, ObIArray<ObLobIndex>& lob_indexes)
{
  int ret = OB_SUCCESS;
  ObArray<ObLobIndex> curr_level_index;
  ObArray<ObLobIndex> next_level_index;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataReader has not been inited", K(ret));
  } else if (OB_FAIL(curr_level_index.push_back(index))) {
    LOG_WARN("fail to push back curr level index", K(ret));
  } else {
    while (OB_SUCC(ret) && curr_level_index.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < curr_level_index.count(); ++i) {
        MacroBlockId block_id;
        if (OB_FAIL(logic2phy_map_->get(curr_level_index.at(i).logic_macro_id_, block_id))) {
          LOG_WARN("fail to get from map", K(curr_level_index.at(i)), K(i), K(ret));
        } else {
          if (0 == i) {
            ObFullMacroBlockMeta full_meta;
            if (OB_FAIL(sstable_->get_meta(block_id, full_meta))) {
              LOG_WARN("fail to get meta", K(ret));
            } else if (OB_UNLIKELY(!full_meta.is_valid())) {
              ret = OB_ERR_SYS;
              LOG_WARN("ObMacroBlockMeta is not valid", K(ret));
            } else if (!full_meta.meta_->is_lob_data_block()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "Metaimage fail to get lob meta.", K(block_id), K(ret));
            } else if (ObMacroBlockCommonHeader::LobIndex != full_meta.meta_->attr_) {
              if (OB_FAIL(lob_indexes.assign(curr_level_index))) {
                LOG_WARN("fail to assign lob index", K(ret));
              }
              LOG_DEBUG("read all direct indexes", K(lob_indexes));
              curr_level_index.reset();
              break;
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(read_index_macro(block_id, next_level_index))) {
              LOG_WARN("fail to read index macro block", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(curr_level_index.assign(next_level_index))) {
          LOG_WARN("fail to assign lob index", K(ret));
        } else {
          next_level_index.reuse();
        }
      }
    }
  }
  return ret;
}

} /* namespace blocksstable*/
} /* namespace oceanbase */
