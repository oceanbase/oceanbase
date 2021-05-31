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

#include "storage/ob_partition_meta_redo_module.h"
#include "ob_lob_data_writer.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_task_define.h"
#include "storage/ob_sstable.h"
#include "storage/ob_file_system_util.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

ObLobMicroBlockWriter::ObLobMicroBlockWriter()
    : is_inited_(false), header_(NULL), data_buffer_(0, ObModIds::OB_LOB_WRITER, false /*aligned*/)
{}

int ObLobMicroBlockWriter::init(const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobMicroBlockWriter has already been inited", K(ret));
  } else if (OB_UNLIKELY(macro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(macro_block_size));
  } else if (OB_FAIL(data_buffer_.ensure_space(macro_block_size))) {
    STORAGE_LOG(WARN, "fail to ensure space for data buffer", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobMicroBlockWriter::reserve_header()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobMicroBlockWriter has not been inited", K(ret));
  } else {
    const int32_t header_size = static_cast<int32_t>(sizeof(ObLobMicroBlockHeader));
    header_ = reinterpret_cast<ObLobMicroBlockHeader*>(data_buffer_.data());
    if (OB_FAIL(data_buffer_.advance(header_size))) {
      STORAGE_LOG(WARN, "fail to advance buffer", K(ret), K(header_size));
    } else {
      MEMSET(header_, 0, header_size);
      header_->header_size_ = header_size;
      header_->version_ = LOB_MICRO_BLOCK_HEADER_VERSION;
      header_->magic_ = LOB_MICRO_BLOCK_HEADER_MAGIC;
    }
  }
  return ret;
}

int ObLobMicroBlockWriter::write(const char* buf, const int64_t buf_len, const char*& out_buf, int64_t& out_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    data_buffer_.reuse();
    if (OB_FAIL(reserve_header())) {
      STORAGE_LOG(WARN, "fail to reserve header", K(ret));
    } else if (OB_FAIL(data_buffer_.write(buf, buf_len))) {
      STORAGE_LOG(WARN, "fail to write micro block data", K(ret));
    } else {
      out_buf = data_buffer_.data();
      out_buf_len = data_buffer_.length();
    }
  }
  return ret;
}

void ObLobMicroBlockWriter::reset()
{
  is_inited_ = false;
  header_ = NULL;
  data_buffer_.reuse();
}

void ObLobMicroBlockWriter::reuse()
{
  header_ = NULL;
  data_buffer_.reuse();
}

ObLobMacroBlockWriter::ObLobMacroBlockWriter()
    : is_inited_(false),
      data_(0, ObModIds::OB_LOB_WRITER, false /*aligned*/),
      index_(),
      header_(NULL),
      common_header_(),
      writer_(NULL),
      column_ids_(NULL),
      column_types_(NULL),
      column_orders_(NULL),
      column_checksum_(NULL),
      data_base_offset_(0),
      is_dirty_(false),
      micro_block_writer_(),
      compressor_(),
      byte_size_(0),
      char_size_(0),
      desc_(),
      rowkey_(),
      current_seq_(0),
      use_old_micro_block_cnt_(0),
      column_id_array_(),
      column_type_array_(),
      lob_type_(0),
      lob_data_version_(0)
{}

void ObLobMacroBlockWriter::reuse()
{
  data_.reuse();
  index_.reuse();
  header_ = NULL;
  common_header_.reset();
  column_ids_ = NULL;
  column_types_ = NULL;
  column_orders_ = NULL;
  column_checksum_ = NULL;
  data_base_offset_ = 0;
  is_dirty_ = false;
  micro_block_writer_.reuse();
  byte_size_ = 0;
  char_size_ = 0;
  use_old_micro_block_cnt_ = 0;
}

void ObLobMacroBlockWriter::reset()
{
  is_inited_ = false;
  index_.reset();
  header_ = NULL;
  common_header_.reset();
  writer_ = NULL;
  column_ids_ = NULL;
  column_types_ = NULL;
  column_orders_ = NULL;
  column_checksum_ = NULL;
  data_base_offset_ = 0;
  is_dirty_ = false;
  micro_block_writer_.reset();
  byte_size_ = 0;
  char_size_ = 0;
  desc_.reset();
  rowkey_.reset();
  current_seq_ = 0;
  use_old_micro_block_cnt_ = 0;
  column_id_array_.reset();
  column_type_array_.reset();
  lob_type_ = 0;
  lob_data_version_ = 0;
  block_write_ctx_.reset();
}

int ObLobMacroBlockWriter::init(const ObDataStoreDesc& desc, const int64_t start_seq, ObLobDataWriter* writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobMacroBlockWriter has already been inited", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid() || start_seq < 0 || NULL == writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(desc), K(start_seq), KP(writer));
  } else if (OB_UNLIKELY(!desc.is_major_ && desc.snapshot_version_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for lob macro writer", K(desc), K(ret));
  } else if (OB_FAIL(desc_.assign(desc))) {
    LOG_WARN("Failed to assign ObDataStoreDesc", K(ret));
  } else if (OB_FAIL(data_.ensure_space(desc_.macro_block_size_))) {
    LOG_WARN("Failed to ensure space", K(ret));
  } else if (OB_FAIL(index_.init(desc_.macro_block_size_))) {
    LOG_WARN("Failed to init lob micro block index writer", K(ret));
  } else if (OB_FAIL(micro_block_writer_.init(desc_.macro_block_size_))) {
    LOG_WARN("Failed to init lob micro block writer", K(ret));
  } else if (OB_FAIL(compressor_.init(desc.micro_block_size_, desc.compressor_name_))) {
    LOG_WARN("Failed to get compressor", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, block_write_ctx_.file_ctx_))) {
    LOG_WARN("failed to init write ctx", K(ret));
  } else {
    current_seq_ = start_seq;
    writer_ = writer;
    lob_data_version_ = desc.is_major_ ? desc.data_version_ : desc.snapshot_version_;
    is_inited_ = true;
  }
  return ret;
}

int ObLobMacroBlockWriter::open(const int16_t lob_type, const ObStoreRowkey& rowkey,
    const ObIArray<uint16_t>& column_ids, const ObIArray<ObObjMeta>& column_types)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(
                 (ObMacroBlockCommonHeader::LobData != lob_type && ObMacroBlockCommonHeader::LobIndex != lob_type) ||
                 0 == column_ids.count() || 0 == column_types.count() || column_ids.count() != column_types.count() ||
                 column_ids.count() != (rowkey.get_obj_cnt() + 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(lob_type), K(rowkey), K(column_ids.count()), K(column_types.count()));
  } else if (!rowkey.is_valid() || rowkey.get_obj_cnt() != desc_.rowkey_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey), K(desc_.rowkey_column_count_));
  } else if (OB_FAIL(column_id_array_.assign(column_ids))) {
    LOG_WARN("fail to assign column ids", K(ret));
  } else if (OB_FAIL(column_type_array_.assign(column_types))) {
    LOG_WARN("fail to assign column types", K(ret));
  } else {
    rowkey_ = rowkey;
    lob_type_ = lob_type;
    if (OB_FAIL(init_header(column_ids, column_types))) {
      LOG_WARN("fail to init header", K(ret));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::init_header(
    const common::ObIArray<uint16_t>& column_ids, const common::ObIArray<ObObjMeta>& column_types)
{
  int ret = OB_SUCCESS;
  int64_t common_header_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(0 == column_ids.count() || 0 == column_types.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_UNLIKELY(0 != data_.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data length", K(ret), K(data_.length()));
  } else {
    MEMSET(data_.data(), 0, data_.capacity());
    common_header_.reset();
    common_header_size = common_header_.get_serialize_size();
    if (OB_FAIL(data_.advance(common_header_size))) {
      LOG_WARN("fail to advance data buffer", K(ret));
    } else {
      common_header_.set_attr(static_cast<ObMacroBlockCommonHeader::MacroBlockType>(lob_type_));
      common_header_.set_data_version(desc_.data_version_);
      common_header_.set_reserved(0);
    }

    // set info of ObLobMacroBlockHeader
    if (OB_SUCC(ret)) {
      const int64_t column_cnt = column_ids.count();
      const int64_t column_id_size = sizeof(uint16_t) * column_cnt;
      const int64_t column_type_size = sizeof(ObObjMeta) * column_cnt;
      const int64_t column_checksum_size = sizeof(int64_t) * column_cnt;
      const int64_t column_order_size = sizeof(ObOrderType) * column_cnt;
      int64_t lob_header_size = sizeof(ObLobMacroBlockHeader);
      header_ = reinterpret_cast<ObLobMacroBlockHeader*>(data_.current());
      column_ids_ = reinterpret_cast<uint16_t*>(data_.current() + lob_header_size);
      column_types_ = reinterpret_cast<ObObjMeta*>(data_.current() + lob_header_size + column_id_size);
      column_checksum_ =
          reinterpret_cast<int64_t*>(data_.current() + lob_header_size + column_id_size + column_type_size);
      column_orders_ = reinterpret_cast<ObOrderType*>(
          data_.current() + lob_header_size + column_id_size + column_type_size + column_checksum_size);
      lob_header_size += column_id_size + column_type_size + column_checksum_size + column_order_size;
      if (OB_FAIL(data_.advance(lob_header_size))) {
        LOG_WARN("fail to advance data buffer", K(ret));
      } else {
        MEMSET(header_, 0, lob_header_size);
        header_->header_size_ = static_cast<int32_t>(lob_header_size);
        header_->version_ = LOB_MACRO_BLOCK_HEADER_VERSION_V2;
        header_->magic_ = LOB_MACRO_BLOCK_HEADER_MAGIC;
        header_->attr_ = 0;
        header_->table_id_ = desc_.table_id_;
        header_->data_version_ = desc_.data_version_;
        header_->column_count_ = static_cast<int32_t>(column_cnt);
        header_->rowkey_column_count_ = static_cast<int32_t>(desc_.rowkey_column_count_);
        header_->column_index_scale_ = 1;
        header_->row_store_type_ = 0;
        header_->row_count_ = 1;
        header_->micro_block_size_ = static_cast<int32_t>(desc_.micro_block_size_);
        header_->micro_block_data_offset_ = header_->header_size_ + static_cast<int32_t>(common_header_size);
        header_->micro_block_count_ = 0;
        MEMSET(header_->compressor_name_, 0, OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH);
        MEMCPY(header_->compressor_name_, desc_.compressor_name_, strlen(desc_.compressor_name_));
        header_->data_checksum_ = 0;
        header_->data_seq_ = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < header_->column_count_; ++i) {
          column_ids_[i] = column_ids.at(i);
          column_types_[i] = column_types.at(i);
          column_checksum_[i] = 0;
          if (i < header_->rowkey_column_count_) {
            column_orders_[i] = desc_.column_orders_[i];
          } else {
            column_orders_[i] = ObOrderType::ASC;
          }
        }
        data_base_offset_ = header_->header_size_ + common_header_size;
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::write_micro_block(ObLobMicroBlockDesc& block_desc)
{
  int ret = OB_SUCCESS;
  const char* writer_out_buf = NULL;
  int64_t writer_out_len = 0;
  const char* comp_buf = NULL;
  int64_t comp_len = 0;
  bool need_switch = false;
  const char* buf = block_desc.buf_;
  const int64_t buf_size = block_desc.buf_size_;
  const int64_t byte_size = block_desc.byte_size_;
  const int64_t char_size = block_desc.char_size_;
  LOG_DEBUG("before write micro block", K(data_.length()));
  if (OB_UNLIKELY(NULL == buf || buf_size <= 0 || byte_size <= 0 || char_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size), K(byte_size), K(char_size));
  } else if (OB_FAIL(micro_block_writer_.write(buf, buf_size, writer_out_buf, writer_out_len))) {
    LOG_WARN("fail to write data to micro block", K(ret));
  } else if (OB_FAIL(compressor_.compress(writer_out_buf, writer_out_len, comp_buf, comp_len))) {
    LOG_WARN("fail to compress micro block", K(ret));
  } else if (OB_FAIL(check_need_switch(comp_len, need_switch))) {
    LOG_WARN("fail to check need switch", K(ret), K(comp_len));
  } else if (need_switch && OB_FAIL(flush())) {
    LOG_WARN("fail to flush data", K(ret));
  } else {
    const int64_t data_offset = data_.length() - data_base_offset_;
    ObRecordHeaderV3 record_header;
    LOG_DEBUG("add index entry", K(data_offset), K(byte_size), K(char_size));
    if (OB_FAIL(index_.add_entry(data_offset, byte_size, char_size))) {
      LOG_WARN("fail to add entry", K(ret));
    } else {
      int64_t pos = 0;
      record_header.magic_ = LOB_MICRO_BLOCK_HEADER_MAGIC;
      record_header.header_length_ =
          static_cast<int8_t>(ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V2, 0 /*column cnt*/));
      record_header.version_ = RECORD_HEADER_VERSION_V2;
      record_header.header_checksum_ = 0;
      record_header.reserved16_ = 0;
      record_header.data_length_ = writer_out_len;
      record_header.data_zlength_ = comp_len;
      record_header.data_checksum_ = ob_crc64_sse42(comp_buf, comp_len);
      record_header.set_header_checksum();

      if (OB_FAIL(record_header.serialize(data_.current(), get_remain_size(), pos))) {
        LOG_WARN("fail to serialize header", K(ret));
      } else if (OB_FAIL(data_.advance(record_header.get_serialize_size()))) {
        LOG_WARN("fail to advance record header size", K(ret));
      } else {
        is_dirty_ = true;
        MEMCPY(data_.current(), comp_buf, static_cast<size_t>(comp_len));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(data_.advance(comp_len))) {
        LOG_WARN("fail to advance data", K(ret));
      } else {
        use_old_micro_block_cnt_ += block_desc.old_micro_block_ ? 1 : 0;
        ++header_->micro_block_count_;
        header_->occupy_size_ = static_cast<int32_t>(get_data_size());
        header_->data_checksum_ = ob_crc64_sse42(
            header_->data_checksum_, &record_header.data_checksum_, sizeof(record_header.data_checksum_));
        byte_size_ += byte_size;
        char_size_ += char_size;
        if (OB_FAIL(add_column_checksum(block_desc.column_id_, block_desc.column_checksum_))) {
          LOG_WARN("fail to merge column checksum", K(ret));
        }
      }
    }
  }
  LOG_DEBUG("after write micro block", K(data_.length()), K(need_switch));
  return ret;
}

int ObLobMacroBlockWriter::build_macro_meta(ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  if (nullptr == full_meta.meta_ || nullptr == full_meta.schema_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(full_meta));
  } else {
    ObMacroBlockMetaV2& meta = const_cast<ObMacroBlockMetaV2&>(*full_meta.meta_);
    ObMacroBlockSchemaInfo& schema = const_cast<ObMacroBlockSchemaInfo&>(*full_meta.schema_);
    meta.attr_ = lob_type_;
    meta.create_timestamp_ = 0;
    meta.data_version_ = header_->data_version_;
    meta.column_number_ = static_cast<int16_t>(header_->column_count_);
    meta.rowkey_column_number_ = static_cast<uint16_t>(header_->rowkey_column_count_);
    meta.column_index_scale_ = static_cast<uint16_t>(header_->column_index_scale_);
    meta.row_store_type_ = static_cast<uint16_t>(header_->row_store_type_);
    meta.row_count_ = static_cast<uint16_t>(header_->row_count_);
    meta.occupy_size_ = header_->occupy_size_;
    meta.data_checksum_ = header_->data_checksum_;
    meta.micro_block_count_ = header_->micro_block_count_;
    meta.micro_block_data_offset_ = header_->micro_block_data_offset_;
    meta.micro_block_index_offset_ = header_->micro_block_index_offset_;
    meta.micro_block_endkey_offset_ = header_->micro_block_size_array_offset_;
    meta.column_checksum_ = column_checksum_;
    meta.table_id_ = header_->table_id_;
    meta.data_seq_ = header_->data_seq_;
    meta.schema_version_ = desc_.schema_version_;
    meta.snapshot_version_ = desc_.snapshot_version_;
    meta.schema_rowkey_col_cnt_ = static_cast<int16_t>(desc_.schema_rowkey_col_cnt_);
    meta.row_count_delta_ = 0;
    meta.micro_block_mark_deletion_offset_ = 0;
    meta.macro_block_deletion_flag_ = false;
    meta.micro_block_delta_offset_ = 0;
    meta.partition_id_ = desc_.partition_id_;
    meta.column_checksum_method_ = CCM_IGNORE;
    meta.endkey_ = const_cast<ObObj*>(rowkey_.get_obj_ptr());

    schema.column_number_ = static_cast<int16_t>(header_->column_count_);
    schema.rowkey_column_number_ = static_cast<uint16_t>(header_->rowkey_column_count_);
    schema.schema_version_ = desc_.schema_version_;
    schema.schema_rowkey_col_cnt_ = static_cast<int16_t>(desc_.schema_rowkey_col_cnt_);
    schema.compressor_ = header_->compressor_name_;
    schema.column_id_array_ = column_ids_;
    schema.column_type_array_ = column_types_;
    schema.column_order_array_ = column_orders_;
    if (OB_UNLIKELY(!meta.is_valid() || !schema.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(meta), K(*header_), K(schema));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::add_column_checksum(const uint16_t column_id, const int64_t column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockWriter has not been inited", K(ret), K(column_id));
  } else if (OB_UNLIKELY(header_->column_count_ <= header_->rowkey_column_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected column count", K(ret), K(header_->column_count_), K_(rowkey));
  } else if (column_ids_[header_->rowkey_column_count_] != column_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, column can not be found",
        K(ret),
        "column_ids",
        ObArrayWrap<uint16_t>(column_ids_, header_->column_count_),
        K(column_id));
  } else {
    column_checksum_[header_->rowkey_column_count_] += column_checksum;
  }
  return ret;
}

int ObLobMacroBlockWriter::flush()
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  ObMacroBlockMetaV2 macro_meta;
  ObMacroBlockSchemaInfo macro_schema;
  full_meta.meta_ = &macro_meta;
  full_meta.schema_ = &macro_schema;
  const int64_t data_len = data_.length() - data_base_offset_;
  if (index_.get_block_size() > data_.remain()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("micro block index size is larger than macro block data remain size",
        K(ret),
        "micro_block_size",
        index_.get_block_size(),
        "data_remain_size",
        data_.remain());
  } else if (OB_FAIL(index_.add_last_entry(data_len))) {
    LOG_WARN("fail to add last entry", K(ret));
  } else {
    MacroBlockId block_id;
    const int64_t index_len = index_.get_index().length();
    const int64_t size_array_len = index_.get_size_array().length();
    LOG_DEBUG("add last entry", K(data_len), K(data_.length()));
    if (OB_FAIL(finish_header(data_len, index_len, size_array_len))) {
      LOG_WARN("fail to finish header", K(ret));
    } else if (OB_FAIL(data_.write(index_.get_index().data(), index_.get_index().length()))) {
      LOG_WARN("fail to copy index to macro block data buffer", K(ret));
    } else if (OB_FAIL(data_.write(index_.get_size_array().data(), index_.get_size_array().length()))) {
      LOG_WARN("fail to copy size array to macro block data buffer", K(ret));
    } else if (OB_FAIL(build_macro_meta(full_meta))) {
      LOG_WARN("fail to build macro meta", K(ret));
    } else {
      ObMacroBlockHandle macro_handle;
      ObMacroBlockWriteInfo write_info;
      ObStorageFile* file = desc_.file_handle_.get_storage_file();
      if (OB_ISNULL(file)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "storage file is null", K(ret), KP(file), K_(desc));
      } else if (!block_write_ctx_.file_handle_.is_valid() &&
                 OB_FAIL(block_write_ctx_.file_handle_.assign(desc_.file_handle_))) {
        STORAGE_LOG(WARN, "fail to set file handle", K(ret), K(desc_.file_handle_));
      } else {
        write_info.buffer_ = data_.data();
        write_info.size_ = data_.capacity();
        write_info.meta_ = full_meta;
        write_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
        write_info.io_desc_.category_ = SYS_IO;
        write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
        write_info.block_write_ctx_ = &block_write_ctx_;
        macro_handle.set_file(file);
        if (desc_.need_calc_physical_checksum_) {
          const int64_t common_header_size = common_header_.get_serialize_size();
          const char* payload_buf = data_.data() + common_header_size;
          const int64_t payload_size = data_.length() - common_header_size;
          common_header_.set_payload_size(static_cast<int32_t>(payload_size));
          common_header_.set_payload_checksum(static_cast<int32_t>(ob_crc64(payload_buf, payload_size)));
        }
        if (OB_FAIL(common_header_.build_serialized_header(data_.data(), data_.capacity()))) {
          LOG_WARN("failed to build common header", K(ret), K_(common_header), K_(data));
        } else if (OB_FAIL(file->write_block(write_info, macro_handle))) {
          LOG_WARN("fail to write block", K(ret));
        } else {
          ObLobIndex index;
          index.logic_macro_id_.data_seq_ = header_->data_seq_;
          index.logic_macro_id_.data_version_ = lob_data_version_;
          index.byte_size_ = byte_size_;
          index.char_size_ = char_size_;
          ObFullMacroBlockMeta meta;
          const int64_t macro_meta_cnt = block_write_ctx_.macro_block_meta_list_.count();
          if (OB_FAIL(block_write_ctx_.macro_block_meta_list_.at(macro_meta_cnt - 1, meta))) {
            LOG_WARN("fail to get macro meta", K(ret));
          } else if (OB_FAIL(
                         writer_->add_macro_block(index, ObMacroBlockInfoPair(macro_handle.get_macro_id(), meta)))) {
            LOG_WARN("fail to add macro block", K(ret));
          } else {
            ObTaskController::get().allow_next_syslog();
            LOG_INFO("[LOB], succeed to flush macro block of lob",
                K(ret),
                "macro_block_id",
                macro_handle.get_macro_id(),
                K(index),
                K(data_.length()),
                K(*header_),
                K_(lob_data_version));

            // reset context
            reuse();
            if (OB_FAIL(init_header(column_id_array_, column_type_array_))) {
              LOG_WARN("fail to init header", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (is_dirty_) {
    if (OB_FAIL(flush())) {
      LOG_WARN("fail to flush macro block", K(ret));
    }
  }
  return ret;
}

int ObLobMacroBlockWriter::check_need_switch(const int64_t size, bool& need_switch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMacroBlockWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(size));
  } else {
    const int64_t request_size = size +
                                 ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V2, 0 /*column cnt*/) +
                                 ObLobMicroBlockIndexWriter::INDEX_ENTRY_SIZE;
    need_switch = request_size > get_remain_size();
  }
  return ret;
}

int ObLobMacroBlockWriter::finish_header(const int64_t data_len, const int64_t index_len, const int64_t size_array_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(data_len <= 0 || index_len <= 0 || size_array_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_len), K(index_len), K(size_array_len));
  } else {
    header_->micro_block_data_size_ = static_cast<int32_t>(data_len);
    header_->micro_block_index_offset_ = static_cast<int32_t>(data_len + data_base_offset_);
    header_->micro_block_index_size_ = static_cast<int32_t>(index_len);
    header_->micro_block_size_array_offset_ =
        static_cast<int32_t>(header_->micro_block_index_offset_ + header_->micro_block_index_size_);
    header_->micro_block_size_array_size_ = static_cast<int32_t>(size_array_len);
    header_->data_seq_ = current_seq_++;
    LOG_DEBUG("finish header", K(data_base_offset_), K(data_len), K(index_len), K(size_array_len), K(*header_));
  }
  return ret;
}

ObLobDataWriter::ObLobDataWriter()
    : is_inited_(false),
      macro_blocks_(),
      lob_indexes_(),
      writer_(),
      lob_index_buffer_(0, ObModIds::OB_LOB_WRITER, false /*aligned*/),
      rowkey_column_cnt_(0),
      micro_block_size_(0),
      column_id_(0),
      allocator_(ObModIds::OB_LOB_WRITER),
      column_ids_(),
      column_types_(),
      rowkey_(),
      file_handle_()
{}

ObLobDataWriter::~ObLobDataWriter()
{
  reset();
}

void ObLobDataWriter::clear_macro_block_ref()
{
  if (macro_blocks_.empty()) {
  } else {
    int ret = OB_SUCCESS;
    blocksstable::ObStorageFile* file = file_handle_.get_storage_file();
    if (OB_ISNULL(file)) {
      ret = OB_ERR_SYS;
      LOG_WARN("storage file is null", K(ret), KP(file), K_(file_handle));
    } else {
      for (int64_t i = 0; i < macro_blocks_.count(); i++) {
        if (OB_FAIL(file->dec_ref(macro_blocks_.at(i).block_id_))) {
          LOG_WARN("fail to inc ref for pg_file", K(ret), KPC(file), K(macro_blocks_.at(i)));
        }
      }
    }
  }
}

void ObLobDataWriter::reset()
{
  is_inited_ = false;
  clear_macro_block_ref();
  macro_blocks_.reset();
  lob_indexes_.reset();
  writer_.reset();
  rowkey_column_cnt_ = 0;
  micro_block_size_ = 0;
  column_id_ = 0;
  allocator_.reset();
  column_ids_.reset();
  column_types_.reset();
  rowkey_.reset();
  lob_index_buffer_.reuse();
  file_handle_.reset();
}

int ObLobDataWriter::init(const ObDataStoreDesc& desc, const int64_t start_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobDataWriter has already been inited", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid() || start_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(desc), K(start_seq));
  } else {
    reset();
    if (OB_FAIL(writer_.init(desc, start_seq, this))) {
      LOG_WARN("fail to init ObLobMacroBlockWriter", K(ret));
    } else if (OB_FAIL(file_handle_.assign(desc.file_handle_))) {
      LOG_WARN("failed to assign file handle", K(ret), K(desc.file_handle_));
    } else {
      rowkey_column_cnt_ = desc.rowkey_column_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_cnt_; i++) {
        if (OB_FAIL(column_ids_.push_back(static_cast<uint16_t>(desc.column_ids_[i])))) {
          STORAGE_LOG(WARN, "Failed to push back rowkey column id", "column_id", desc.column_ids_[i], K(ret));
        } else if (OB_FAIL(column_types_.push_back(desc.column_types_[i]))) {
          STORAGE_LOG(WARN, "Failed to push back rowkey column type", "column_type", desc.column_types_[i], K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        micro_block_size_ = desc.micro_block_size_;
      }
    }
  }
  return ret;
}

void ObLobDataWriter::reuse()
{
  inner_reuse();
  allocator_.reuse();
}

void ObLobDataWriter::inner_reuse()
{
  clear_macro_block_ref();
  macro_blocks_.reuse();
  lob_indexes_.reuse();
  lob_index_buffer_.reuse();
  column_id_ = 0;
  while (column_ids_.count() > rowkey_column_cnt_) {
    column_ids_.pop_back();
  }
  while (column_types_.count() > rowkey_column_cnt_) {
    column_types_.pop_back();
  }
  rowkey_.reset();
}

int ObLobDataWriter::check_rowkey(const ObStoreRowkey& rowkey, bool& check_ret) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(rowkey));
  } else if (rowkey.get_obj_cnt() != rowkey_column_cnt_) {
    check_ret = false;
  } else {
    check_ret = true;
    for (int64_t i = 0; check_ret && i < rowkey_column_cnt_; i++) {
      if (column_types_.at(i).get_type() != rowkey.get_obj_ptr()[i].get_type()) {
        check_ret = false;
      }
    }
  }

  return ret;
}

int ObLobDataWriter::write_lob_data(const ObStoreRowkey& rowkey, const uint16_t column_id, const ObObj& src, ObObj& dst,
    ObIArray<ObMacroBlockInfoPair>& macro_blocks)
{
  int ret = OB_SUCCESS;
  bool check_ret = false;
  inner_reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {  // TODO: we should check whether the src is out row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(rowkey));
  } else if (OB_FAIL(check_rowkey(rowkey, check_ret))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to check rowkey", K(ret), K(rowkey));
  } else if (OB_UNLIKELY(!check_ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Rowkey objects schema mismatch", K(ret), K(rowkey), K(rowkey_column_cnt_), K(column_types_));
  } else if (OB_FAIL(column_ids_.push_back(column_id))) {
    LOG_WARN("fail to push back column id", K(ret));
  } else if (OB_FAIL(column_types_.push_back(src.get_meta()))) {
    LOG_WARN("fail to push back column type", K(ret));
  } else if (OB_FAIL(writer_.open(ObMacroBlockCommonHeader::LobData, rowkey, column_ids_, column_types_))) {
    LOG_WARN("fail to open ObLobMacroBlockWriter", K(ret));
  } else {
    const char* pos = src.get_string_ptr();
    int32_t left_bytes = src.get_string_len();
    const char* end = pos + left_bytes;
    const ObCollationType coll_type = src.get_collation_type();
    int64_t char_size = 0;
    int64_t current_offset = 0;
    column_id_ = column_id;
    LOG_DEBUG("begin write lob data", K(left_bytes), K(micro_block_size_), K(column_id_));
    rowkey_ = rowkey;
    while (OB_SUCC(ret) && pos < end) {
      ObLobMicroBlockDesc block_desc;
      const int64_t request_size = std::min(static_cast<int64_t>(left_bytes), micro_block_size_);
      const int64_t real_bytes = ObCharset::max_bytes_charpos(coll_type, pos, left_bytes, request_size, char_size);
      block_desc.buf_ = pos;
      block_desc.buf_size_ = real_bytes;
      block_desc.byte_size_ = real_bytes;
      block_desc.char_size_ = char_size;
      block_desc.old_micro_block_ = false;
      block_desc.column_id_ = column_id;
      block_desc.column_checksum_ = ob_crc64_sse42(pos, real_bytes);

      LOG_DEBUG("write lob micro byte", K(current_offset), K(left_bytes), K(real_bytes), K(request_size));
      if (OB_FAIL(writer_.write_micro_block(block_desc))) {
        LOG_WARN("fail to write micro block", K(ret));
      } else {
        left_bytes -= static_cast<int32_t>(real_bytes);
        pos += real_bytes;
        current_offset += real_bytes;
      }
    }

    if (OB_SUCC(ret)) {
      ObLobData* lob_data = NULL;
      void* buf = NULL;
      if (OB_FAIL(writer_.close())) {
        LOG_WARN("fail to close writer", K(ret));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobData)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for LobData", K(ret));
      } else {
        lob_data = new (buf) ObLobData();
        if (OB_FAIL(write_lob_index(*lob_data))) {
          LOG_WARN("fail to write lob index", K(ret));
        } else if (OB_FAIL(get_physical_macro_blocks(macro_blocks))) {
          LOG_WARN("fail to get physical macro blocks", K(ret));
        } else {
          for (int64_t i = 0; i < lob_data->idx_cnt_; ++i) {
            lob_data->byte_size_ += lob_data->lob_idx_[i].byte_size_;
            lob_data->char_size_ += lob_data->lob_idx_[i].char_size_;
          }
          dst.copy_meta_type(src.get_meta());
          dst.set_lob_value(src.get_type(), lob_data, lob_data->get_handle_size());
        }
      }
    }
  }
  return ret;
}

int ObLobDataWriter::write_lob_index(ObLobData& lob_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else {
    int64_t i = 0;
    int64_t origin_count = lob_indexes_.count();
    int64_t direct_count = std::min(lob_data.get_direct_cnt(), origin_count);
    int64_t add_cnt = 0;
    bool need_indirect_index = 0;
    lob_data.reset();
    for (; OB_SUCC(ret) && i < direct_count; ++i) {
      lob_data.lob_idx_[i] = lob_indexes_.at(i);
      ++lob_data.idx_cnt_;
    }
    if (OB_SUCC(ret) && i < origin_count) {
      if (OB_FAIL(lob_index_buffer_.ensure_space(micro_block_size_))) {
        LOG_WARN("fail to ensure space for indirect buffer", K(ret));
      } else if (OB_FAIL(write_lob_index_impl(i, origin_count - 1, add_cnt))) {
        LOG_WARN("fail to write lob index impl", K(ret));
      } else {
        need_indirect_index = true;
      }
    }

    LOG_DEBUG("need_indirect_index", K(need_indirect_index), K(origin_count), K(add_cnt));
    if (OB_SUCC(ret) && need_indirect_index) {
      if (add_cnt < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid add cnt", K(ret), K(add_cnt));
      } else if (1 == add_cnt) {
        lob_data.lob_idx_[lob_data.get_direct_cnt()] = lob_indexes_.at(lob_indexes_.count() - 1);
        ++lob_data.idx_cnt_;
      } else {
        // need second level lob index
        const int64_t start_idx = origin_count;
        const int64_t end_idx = lob_indexes_.count() - 1;
        if (OB_FAIL(write_lob_index_impl(start_idx, end_idx, add_cnt))) {
          LOG_WARN("fail to write lob index impl", K(ret), K(start_idx), K(end_idx));
        } else {
          if (add_cnt < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid add cnt", K(ret), K(add_cnt));
          } else if (1 == add_cnt) {
            lob_data.lob_idx_[lob_data.get_direct_cnt()] = lob_indexes_.at(lob_indexes_.count() - 1);
            ++lob_data.idx_cnt_;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("lob data size overflow", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLobDataWriter::write_lob_index_impl(const int64_t start_idx, const int64_t end_idx, int64_t& add_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t origin_cnt = lob_indexes_.count();
  add_cnt = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_FAIL(start_idx < 0 || start_idx >= origin_cnt || end_idx < 0 || end_idx >= origin_cnt ||
                     start_idx > end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(start_idx), K(end_idx), K(origin_cnt));
  } else if (OB_FAIL(writer_.open(ObMacroBlockCommonHeader::LobIndex, rowkey_, column_ids_, column_types_))) {
    LOG_WARN("fail to open ObLobMacroBlockWriter", K(ret));
  } else {
    lob_index_buffer_.reuse();
    int64_t pos = 0;
    int64_t byte_size = 0;
    int64_t char_size = 0;
    for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; ++i) {
      const int64_t serialize_size = lob_indexes_.at(i).get_serialize_size();
      if (serialize_size > lob_index_buffer_.remain()) {
        // need flush micro block
        ObLobMicroBlockDesc block_desc;
        if (OB_FAIL(fill_micro_block_desc(lob_index_buffer_, byte_size, char_size, block_desc))) {
          LOG_WARN("fail to fill micro block desc", K(ret));
        } else if (OB_FAIL(writer_.write_micro_block(block_desc))) {
          LOG_WARN("fail to write micro block", K(ret));
        } else {
          lob_index_buffer_.reuse();
          pos = 0;
          byte_size = 0;
          char_size = 0;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(lob_indexes_.at(i).serialize(lob_index_buffer_.data(), lob_index_buffer_.capacity(), pos))) {
          LOG_WARN("fail to serialize lob index", K(ret));
        } else if (OB_FAIL(lob_index_buffer_.advance(serialize_size))) {
          LOG_WARN("fail to advance lob index buffer", K(ret));
        } else {
          byte_size += lob_indexes_.at(i).byte_size_;
          char_size += lob_indexes_.at(i).char_size_;
        }
      }
    }

    if (OB_SUCC(ret) && lob_index_buffer_.length() > 0) {
      ObLobMicroBlockDesc block_desc;
      if (OB_FAIL(fill_micro_block_desc(lob_index_buffer_, byte_size, char_size, block_desc))) {
        LOG_WARN("fail to fill micro block desc", K(ret));
      } else if (OB_FAIL(writer_.write_micro_block(block_desc))) {
        LOG_WARN("fail to write micro block", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer_.close())) {
        LOG_WARN("fail to close writer", K(ret));
      } else {
        add_cnt += lob_indexes_.count() - origin_cnt;
      }
    }
  }
  return ret;
}

int ObLobDataWriter::fill_micro_block_desc(
    const ObSelfBufferWriter& data, const int64_t byte_size, const int64_t char_size, ObLobMicroBlockDesc& block_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(0 == data.length() || byte_size <= 0 || char_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data.length()), K(byte_size), K(char_size));
  } else {
    block_desc.buf_ = data.data();
    block_desc.buf_size_ = data.length();
    block_desc.byte_size_ = byte_size;
    block_desc.char_size_ = char_size;
    block_desc.old_micro_block_ = false;
    block_desc.column_id_ = column_id_;
    block_desc.column_checksum_ = ob_crc64_sse42(data.data(), data.length());
  }
  return ret;
}

int ObLobDataWriter::get_physical_macro_blocks(ObIArray<ObMacroBlockInfoPair>& macro_blocks)
{
  int ret = OB_SUCCESS;
  macro_blocks.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_FAIL(macro_blocks.assign(macro_blocks_))) {
    LOG_WARN("fail to assign macro blocks", K(ret));
  }
  return ret;
}

int ObLobDataWriter::add_macro_block(const ObLobIndex& index, const ObMacroBlockInfoPair& block_info)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pg_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobDataWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!block_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index), K(block_info));
  } else if (OB_FAIL(lob_indexes_.push_back(index))) {
    LOG_WARN("fail to push back lob index", K(ret));
  } else if (OB_FAIL(macro_blocks_.push_back(block_info))) {
    LOG_WARN("fail to push back macro block", K(ret));
  } else {
    blocksstable::ObStorageFile* file = file_handle_.get_storage_file();
    if (OB_ISNULL(file)) {
      ret = OB_ERR_SYS;
      LOG_WARN("storage file is null", K(ret), KP(file), K_(file_handle));
    } else if (OB_FAIL(file->inc_ref(block_info.block_id_))) {
      LOG_WARN("fail to inc ref for pg_file", K(ret), K(*file), K(block_info.block_id_));
    }
    if (OB_FAIL(ret)) {
      lob_indexes_.pop_back();
      macro_blocks_.pop_back();
    }
  }
  return ret;
}
