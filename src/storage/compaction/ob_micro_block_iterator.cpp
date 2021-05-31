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

#include "ob_micro_block_iterator.h"

#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_range.h"
#include "share/schema/ob_table_schema.h"
#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/blocksstable/ob_micro_block_index_transformer.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_index_reader.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;
namespace compaction {
/*
 * -------------------------------------------------------ObMacroBlockLoader-----------------------------------------------
 */
ObMacroBlockLoader::ObMacroBlockLoader()
    : is_inited_(false),
      macro_buf_(NULL),
      macro_buf_size_(0),
      range_(),
      micro_block_infos_(),
      end_keys_(),
      allocator_(ObModIds::ObModIds::OB_CS_COMMON),
      cur_micro_cursor_(0)

{}

ObMacroBlockLoader::~ObMacroBlockLoader()
{}

void ObMacroBlockLoader::reset()
{
  is_inited_ = false;
  macro_buf_ = NULL;
  macro_buf_size_ = 0;
  range_.reset();
  micro_block_infos_.reset();
  end_keys_.reset();
  allocator_.reset();
  cur_micro_cursor_ = 0;
}
int ObMacroBlockLoader::transform_block_header(
    const ObSSTableMacroBlockHeader*& block_header, const ObObjMeta*& column_types)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;

  if (OB_ISNULL(macro_buf_) || macro_buf_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null macro block buf", KP_(macro_buf), K_(macro_buf_size), K(ret));
  } else if (OB_FAIL(common_header.deserialize(macro_buf_, macro_buf_size_, pos))) {
    STORAGE_LOG(ERROR, "deserialize common header fail", KP_(macro_buf), K_(macro_buf_size), K(pos), K(ret));
  } else if (OB_FAIL(common_header.check_integrity())) {
    STORAGE_LOG(ERROR, "invalid common header", K(ret), K(common_header));
  } else if (!common_header.is_sstable_data_block()) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "Unsupported macro block type", K(common_header), K(ret));
  } else {
    block_header = reinterpret_cast<const ObSSTableMacroBlockHeader*>(macro_buf_ + pos);
    // skip column ids
    pos += sizeof(ObSSTableMacroBlockHeader) + sizeof(uint16_t) * block_header->column_count_;
    if (!block_header->is_valid() || pos >= macro_buf_size_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected block header or pos", K(*block_header), K(pos), K(ret));
    } else {
      column_types = reinterpret_cast<const ObObjMeta*>(macro_buf_ + pos);
    }
  }
  return ret;
}

int ObMacroBlockLoader::init(const char* macro_block_buf, const int64_t macro_block_buf_size,
    const ObFullMacroBlockMeta* meta, const ObStoreRange* range)
{
  int ret = OB_SUCCESS;
  // get macro meta
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMacroBlockLoader has been inited, ", K(ret));
  } else if (NULL == macro_block_buf || macro_block_buf_size <= 0 || (OB_NOT_NULL(meta) && !meta->is_valid()) ||
             (NULL != range && !range->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), KP(macro_block_buf), K(macro_block_buf_size), K(meta), KP(range));
  } else if (OB_NOT_NULL(meta) && ObMacroBlockCommonHeader::MacroBlockType::SSTableData != meta->meta_->attr_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "The macro block is NOT a data block, ", K(ret), K(meta));
  } else {
    const ObSSTableMacroBlockHeader* block_header = NULL;
    const ObObjMeta* column_types = NULL;
    macro_buf_ = macro_block_buf;
    macro_buf_size_ = macro_block_buf_size;
    if (NULL == range) {
      range_.set_whole_range();
    } else {
      range_ = *range;
    }
    // build micro block infos and endkeys
    ObMicroBlockIndexReader index_reader;
    if (OB_NOT_NULL(meta)) {
      if (OB_FAIL(index_reader.init(*meta, macro_buf_ + meta->meta_->micro_block_index_offset_))) {
        STORAGE_LOG(WARN,
            "micro index reader init failed, ",
            K(ret),
            K(meta),
            KP(macro_buf_),
            K(meta->meta_->micro_block_index_offset_));
      }
    } else {
      if (OB_FAIL(transform_block_header(block_header, column_types))) {
        STORAGE_LOG(WARN, "Failed to transform macro block header", K(ret));
      } else if (OB_ISNULL(block_header) || OB_ISNULL(column_types)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpecte null block header", KP(block_header), KP(column_types), K(ret));
      } else if (OB_FAIL(index_reader.init(
                     *block_header, macro_buf_ + block_header->micro_block_index_offset_, column_types))) {
        STORAGE_LOG(WARN,
            "micro index reader init failed, ",
            K(ret),
            K(*block_header),
            KP(macro_buf_),
            K(block_header->micro_block_index_offset_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_reader.get_micro_block_infos(range_, micro_block_infos_))) {
        if (OB_BEYOND_THE_RANGE != ret) {
          STORAGE_LOG(WARN, "get_micro_block_infos failed, ", K(ret), K(range_));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(index_reader.get_end_keys(range_, allocator_, end_keys_))) {
        STORAGE_LOG(WARN, "get_end_keys failed, ", K(ret), K(range_));
      } else if (OB_ISNULL(meta)) {  // mandatory parse mark deletion flag
        if (block_header->micro_block_endkey_offset_ + block_header->micro_block_endkey_size_ +
                micro_block_infos_.count() * sizeof(uint8_t) <=
            macro_buf_size_) {
          const uint8_t* deletion_ptr = (uint8_t*)(macro_buf_ + block_header->micro_block_endkey_offset_ +
                                                   block_header->micro_block_endkey_size_);
          for (int64_t i = 0; i < micro_block_infos_.count(); ++i) {
            micro_block_infos_.at(i).mark_deletion_ = (deletion_ptr[i] > 0);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObMacroBlockLoader::next_micro_block(blocksstable::ObMicroBlock& micro_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Macro block not loaded, ", K(ret));
  } else if (cur_micro_cursor_ < 0 || cur_micro_cursor_ > micro_block_infos_.size()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Wrong cursor, ", K(ret), K(cur_micro_cursor_));
  } else if (micro_block_infos_.size() == cur_micro_cursor_) {
    ret = OB_ITER_END;
  } else {
    ObRecordHeaderV3 header;
    const ObMicroBlockInfo& micro_block_info = micro_block_infos_[cur_micro_cursor_];
    const char* micro_buf = macro_buf_ + micro_block_info.offset_;
    const int64_t micro_buf_size = micro_block_info.size_;
    int64_t pos = 0;
    const char* payload_buf = nullptr;
    int64_t payload_size = 0;
    STORAGE_LOG(DEBUG, "next micro block", K(micro_block_info.offset_), K(micro_block_info.size_));
    if (OB_FAIL(header.deserialize(micro_buf, micro_buf_size, pos))) {
      STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
    } else if (OB_FAIL(header.check_and_get_record(
                   micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
      STORAGE_LOG(ERROR, "micro block data is corrupted", K(ret), KP(micro_buf), K(micro_buf_size));
    } else {
      ObStoreRange& micro_range = micro_block.range_;
      micro_block.data_.get_buf() = micro_buf;
      micro_block.data_.get_buf_size() = micro_buf_size;
      micro_block.payload_data_.get_buf() = payload_buf;
      micro_block.payload_data_.get_buf_size() = payload_size;
      if (0 == cur_micro_cursor_) {
        micro_range.get_start_key() = range_.get_start_key();
      } else {
        micro_range.get_start_key() = end_keys_[cur_micro_cursor_ - 1];
      }
      micro_range.get_end_key() = end_keys_[cur_micro_cursor_];
      micro_range.get_border_flag().unset_inclusive_start();
      micro_range.get_border_flag().set_inclusive_end();
      micro_block.row_count_ = header.row_count_;
      micro_block.column_cnt_ = header.column_cnt_;
      micro_block.column_checksums_ = header.column_checksums_;
      micro_block.origin_data_size_ = header.data_length_;
      micro_block.header_version_ = header.version_;
    }
    ++cur_micro_cursor_;
  }
  return ret;
}

/*
 * ----------------------------------------------ObMicroBlockIterator------------------------------------------------
 */
ObMicroBlockIterator::ObMicroBlockIterator()
    : inited_(false),
      table_id_(OB_INVALID_ID),
      columns_(NULL),
      range_(),
      micro_block_(),
      loader_(),
      column_map_allocator_(ObModIds::ObModIds::OB_CS_MERGER),
      full_meta_(),
      macro_handle_()
{}

ObMicroBlockIterator::~ObMicroBlockIterator()
{}

void ObMicroBlockIterator::reset()
{
  // annotated fields no need to reset
  inited_ = false;
  table_id_ = OB_INVALID_ID;
  columns_ = NULL;
  range_.reset();
  // micro_block_
  loader_.reset();
  column_map_.reset();
  column_map_allocator_.reset();
  full_meta_.reset();
  macro_handle_.reset();
}

int ObMicroBlockIterator::init(const uint64_t table_id, const ObIArray<ObColDesc>& columns, const ObStoreRange& range,
    const blocksstable::ObMacroBlockCtx& macro_block_ctx, blocksstable::ObStorageFile* pg_file)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice, ", K(ret));
  } else if (!macro_block_ctx.is_valid() || OB_ISNULL(pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block_ctx), KP(pg_file));
  } else {
    table_id_ = table_id;
    columns_ = &columns;
    range_ = range;

    if (OB_FAIL(macro_block_ctx.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), full_meta_))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret));
    } else if (!full_meta_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid macro block meta", K(ret), K(macro_block_ctx));
    } else if (OB_FAIL(column_map_.init(column_map_allocator_,
                   full_meta_.meta_->schema_version_,
                   full_meta_.meta_->rowkey_column_number_,
                   full_meta_.meta_->column_number_,
                   columns))) {
      LOG_WARN("Failed to init column map, ", K(ret));
    } else {
      macro_handle_.set_file(pg_file);
      ObMacroBlockReadInfo read_info;
      const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      read_info.macro_block_ctx_ = &macro_block_ctx;
      read_info.offset_ = 0;
      read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
      read_info.io_desc_.category_ = SYS_IO;
      read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;

      if (OB_FAIL(pg_file->async_read_block(read_info, macro_handle_))) {
        LOG_WARN("aysnc read block failed, ", K(ret), K(read_info), K(macro_block_ctx));
      } else if (OB_FAIL(macro_handle_.wait(io_timeout_ms))) {
        LOG_WARN("io wait failed", K(ret), K(macro_block_ctx), K(io_timeout_ms));
      } else if (NULL == macro_handle_.get_buffer() || macro_handle_.get_data_size() != read_info.size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buf is null or bufsize is too small, ",
            K(ret),
            K(macro_block_ctx),
            KP(macro_handle_.get_buffer()),
            K(macro_handle_.get_data_size()),
            K(read_info.size_));
      } else if (OB_FAIL(
                     loader_.init(macro_handle_.get_buffer(), macro_handle_.get_data_size(), &full_meta_, &range_))) {
        STORAGE_LOG(WARN,
            "Fail to load macro block, ",
            K(ret),
            K(macro_block_ctx),
            KP(macro_handle_.get_buffer()),
            K(macro_handle_.get_data_size()),
            K(range_));
      } else {
        micro_block_.row_store_type_ = static_cast<ObRowStoreType>(full_meta_.meta_->row_store_type_);
        micro_block_.column_map_ = &column_map_;
        inited_ = true;
      }
    }
  }
  return ret;
}

int ObMicroBlockIterator::next(const ObMicroBlock*& micro_block)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(loader_.next_micro_block(micro_block_))) {
      if (ret != OB_ITER_END) {
        STORAGE_LOG(WARN, "get next micro block buf failed", K(ret));
      }
    } else {
      micro_block_.meta_ = full_meta_;
      micro_block = &micro_block_;
      STORAGE_LOG(DEBUG, "iterator micro block", K(*micro_block));
    }
  }
  return ret;
}

}  // end namespace compaction
}  // end namespace oceanbase
