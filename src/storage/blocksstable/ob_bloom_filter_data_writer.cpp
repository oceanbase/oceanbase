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
#include "ob_bloom_filter_data_writer.h"
#include "share/ob_task_define.h"
#include "storage/ob_file_system_util.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
ObBloomFilterMicroBlockWriter::ObBloomFilterMicroBlockWriter()
    : bf_micro_header_(NULL), data_buffer_(0, ObModIds::OB_BF_DATA_WRITER, false), is_inited_(false)
{}

ObBloomFilterMicroBlockWriter::~ObBloomFilterMicroBlockWriter()
{}

void ObBloomFilterMicroBlockWriter::reset()
{
  bf_micro_header_ = NULL;
  data_buffer_.reuse();
  is_inited_ = false;
}

void ObBloomFilterMicroBlockWriter::reuse()
{
  bf_micro_header_ = NULL;
  data_buffer_.reuse();
}

int ObBloomFilterMicroBlockWriter::init(const int64_t micro_block_size)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBloomFilterMicroBlockWriter init twice", K(ret));
  } else if (micro_block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block size", K(micro_block_size), K(ret));
  } else if (OB_FAIL(data_buffer_.ensure_space(micro_block_size))) {
    STORAGE_LOG(WARN, "Failed to ensure space", K(micro_block_size), K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObBloomFilterMicroBlockWriter::build_micro_block_header(const int64_t rowkey_column_count, const int64_t row_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMicroBlockWriter is not init", K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count <= 0 || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "Invalid argument to build bloomfilter micro block header", K(rowkey_column_count), K(row_count), K(ret));
  } else {
    data_buffer_.reuse();
    bf_micro_header_ = reinterpret_cast<ObBloomFilterMicroBlockHeader*>(data_buffer_.data());
    bf_micro_header_->reset();
    bf_micro_header_->header_size_ = sizeof(ObBloomFilterMicroBlockHeader);
    bf_micro_header_->version_ = BF_MICRO_BLOCK_HEADER_VERSION;
    bf_micro_header_->magic_ = BF_MICRO_BLOCK_HEADER_MAGIC;
    bf_micro_header_->rowkey_column_count_ = static_cast<int16_t>(rowkey_column_count);
    bf_micro_header_->row_count_ = static_cast<int32_t>(row_count);
    if (OB_FAIL(data_buffer_.advance(bf_micro_header_->header_size_))) {
      STORAGE_LOG(WARN, "Failed to advance bf data buffer", K(bf_micro_header_->header_size_), K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMicroBlockWriter::write(
    const ObBloomFilterCacheValue& bf_cache_value, const char*& block_buf, int64_t& block_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMicroBlockWriter is not init", K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build bloomfilter micro block header", K(bf_cache_value), K(ret));
  } else if (OB_FAIL(build_micro_block_header(bf_cache_value.get_prefix_len(), bf_cache_value.get_row_count()))) {
    STORAGE_LOG(WARN, "Failed to build bf micro block header", K(ret));
  } else if (OB_FAIL(data_buffer_.write_serialize(bf_cache_value))) {
    STORAGE_LOG(WARN, "Failed to serialize bloom filter cache value", K_(data_buffer), K(bf_cache_value), K(ret));
  } else {
    block_buf = data_buffer_.data();
    block_size = data_buffer_.length();
  }

  return ret;
}

ObBloomFilterMacroBlockWriter::ObBloomFilterMacroBlockWriter()
    : data_buffer_(0, ObModIds::OB_BF_DATA_WRITER, false),
      bf_macro_header_(NULL),
      common_header_(),
      compressor_(),
      bf_micro_writer_(),
      block_write_ctx_(),
      desc_(NULL),
      file_handle_(),
      is_inited_(false)
{}

ObBloomFilterMacroBlockWriter::~ObBloomFilterMacroBlockWriter()
{}

void ObBloomFilterMacroBlockWriter::reset()
{
  data_buffer_.reuse();
  bf_macro_header_ = NULL;
  common_header_.reset();
  compressor_.reset();
  bf_micro_writer_.reset();
  block_write_ctx_.reset();
  desc_ = NULL;
  file_handle_.reset();
  is_inited_ = false;
}

void ObBloomFilterMacroBlockWriter::reuse()
{
  data_buffer_.reuse();
  bf_macro_header_ = NULL;
  common_header_.reset();
  compressor_.reset();
  bf_micro_writer_.reuse();
  block_write_ctx_.clear();
}

int ObBloomFilterMacroBlockWriter::init(const ObDataStoreDesc& desc)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter init twice", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObBloomFilterMacroBlockWriter", K(desc), K(ret));
  } else if (OB_UNLIKELY(desc.is_major_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Major freeze would not build bloomfilter macro data", K(ret));
  } else if (OB_FAIL(data_buffer_.ensure_space(desc.macro_block_size_))) {
    STORAGE_LOG(WARN, "Failed to ensure space", K(desc.macro_block_size_), K(ret));
  } else if (OB_FAIL(compressor_.init(desc.macro_block_size_, desc.compressor_name_))) {
    STORAGE_LOG(WARN, "Failed to init compressor", K(ret));
  } else if (OB_FAIL(bf_micro_writer_.init(desc.macro_block_size_))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter micro writer", K(ret));
  } else if (OB_FAIL(file_handle_.assign(desc.file_handle_))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(desc.file_handle_));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, block_write_ctx_.file_ctx_))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter block write ctx", K(ret));
  } else if (!block_write_ctx_.file_handle_.is_valid() &&
             OB_FAIL(block_write_ctx_.file_handle_.assign(desc.file_handle_))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret), K(desc.file_handle_));
  } else {
    if (OB_FAIL(ret)) {
    } else {
      desc_ = &desc;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockWriter::write(const ObBloomFilterCacheValue& bf_cache_value)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter not init", K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value.is_valid() || bf_cache_value.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to write bloomfilter cache", K(bf_cache_value), K(ret));
  } else {
    const char* block_buf = NULL;
    const char* comp_block_buf = NULL;
    int64_t block_size = 0;
    int64_t comp_block_size = 0;
    data_buffer_.reuse();
    if (OB_FAIL(init_headers(bf_cache_value.get_row_count()))) {
      STORAGE_LOG(WARN, "Failed to build bloomfilter macro block header", K(ret));
    } else if (OB_FAIL(bf_micro_writer_.write(bf_cache_value, block_buf, block_size))) {
      STORAGE_LOG(WARN, "Failed to write bloomfilter micro block", K(bf_cache_value), K(ret));
    } else if (OB_ISNULL(block_buf) || block_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected micro blcok buf", KP(block_buf), K(block_size), K(ret));
    } else if (OB_FAIL(compressor_.compress(block_buf, block_size, comp_block_buf, comp_block_size))) {
      STORAGE_LOG(WARN, "Failed to compress bloomfilter micro block", K(ret));
    } else if (OB_FAIL(write_micro_block(comp_block_buf, comp_block_size, block_size))) {
      STORAGE_LOG(WARN, "Failed to write bloomfilter micro block", K(ret));
    } else if (OB_FAIL(flush_macro_block())) {
      STORAGE_LOG(WARN, "Failed to flush bloomfilter macro block", K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockWriter::write_micro_block(
    const char* comp_block_buf, const int64_t comp_block_size, const int64_t orig_block_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter not init", K(ret));
  } else if (OB_ISNULL(comp_block_buf) || comp_block_size <= 0 || orig_block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument to write bloomfilter micro block",
        KP(comp_block_buf),
        K(comp_block_size),
        K(orig_block_size),
        K(ret));
  } else if (comp_block_size + ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V3, 0) >
             data_buffer_.remain()) {
    ret = OB_ERR_UNEXPECTED;
    uint64_t table_id = bf_macro_header_->table_id_;
    STORAGE_LOG(WARN,
        "Unexpected too large bloomfilter data to write in one macroblock",
        K(table_id),
        K(orig_block_size),
        K(comp_block_size),
        K_(data_buffer),
        K(ret));
  } else {
    ObRecordHeaderV3 micro_record_header;
    micro_record_header.magic_ = BF_MICRO_BLOCK_HEADER_MAGIC;
    micro_record_header.header_length_ =
        static_cast<int8_t>(ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V3, 0));
    micro_record_header.version_ = RECORD_HEADER_VERSION_V3;
    micro_record_header.header_checksum_ = 0;
    micro_record_header.data_length_ = orig_block_size;
    micro_record_header.data_zlength_ = comp_block_size;
    micro_record_header.data_checksum_ = ob_crc64_sse42(comp_block_buf, comp_block_size);
    micro_record_header.set_header_checksum();
    if (OB_FAIL(data_buffer_.write_serialize(micro_record_header))) {
      STORAGE_LOG(WARN, "Failed to serialize bloomfilter micro block header", K(ret));
    } else if (OB_FAIL(data_buffer_.write(comp_block_buf, comp_block_size))) {
      STORAGE_LOG(WARN, "Failed to write bloomfilter compress block to buffer", K(comp_block_size), K(ret));
    } else {
      int64_t payload_size = data_buffer_.length() - common_header_.get_serialize_size();
      const char* payload_buf = data_buffer_.data() + common_header_.get_serialize_size();
      bf_macro_header_->micro_block_count_ += 1;
      bf_macro_header_->occupy_size_ = static_cast<int32_t>(data_buffer_.length());
      bf_macro_header_->micro_block_data_size_ = static_cast<int32_t>(
          data_buffer_.length() - bf_macro_header_->header_size_ - common_header_.get_serialize_size());
      bf_macro_header_->data_checksum_ = ob_crc64_sse42(bf_macro_header_->data_checksum_,
          &micro_record_header.data_checksum_,
          sizeof(micro_record_header.data_checksum_));

      if (OB_NOT_NULL(desc_) && desc_->need_calc_physical_checksum_) {
        common_header_.set_payload_size(static_cast<int32_t>(payload_size));
        common_header_.set_payload_checksum(static_cast<int32_t>(ob_crc64_sse42(payload_buf, payload_size)));
      }

      if (OB_FAIL(common_header_.build_serialized_header(data_buffer_.data(), common_header_.get_serialize_size()))) {
        STORAGE_LOG(WARN, "Failed to serialize macro block common header", K(ret));
      }
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockWriter::init_headers(const int64_t row_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter not init", K(ret));
  } else if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init bloomfilter headers", K(row_count), K(ret));
  } else if (OB_UNLIKELY(data_buffer_.pos() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected init data buffer to init header", K_(data_buffer), K(ret));
  } else {
    int64_t common_header_size = common_header_.get_serialize_size();
    int64_t bf_macro_header_size = sizeof(ObBloomFilterMacroBlockHeader);
    common_header_.reset();
    common_header_.set_attr(ObMacroBlockCommonHeader::BloomFilterData);
    common_header_.set_data_version(desc_->data_version_);
    common_header_.set_reserved(0);
    if (OB_FAIL(data_buffer_.advance(common_header_size))) {
      STORAGE_LOG(WARN, "Failed to advance data buffer for common header", K(common_header_size), K(ret));
    } else {
      bf_macro_header_ = reinterpret_cast<ObBloomFilterMacroBlockHeader*>(data_buffer_.current());
      bf_macro_header_->reset();
      bf_macro_header_->header_size_ = static_cast<int32_t>(bf_macro_header_size);
      bf_macro_header_->version_ = BF_MACRO_BLOCK_HEADER_VERSION;
      bf_macro_header_->magic_ = BF_MACRO_BLOCK_HEADER_MAGIC;
      bf_macro_header_->attr_ = ObMacroBlockCommonHeader::BloomFilterData;
      bf_macro_header_->table_id_ = desc_->table_id_;
      bf_macro_header_->partition_id_ = desc_->partition_id_;
      bf_macro_header_->data_version_ = desc_->data_version_;
      bf_macro_header_->rowkey_column_count_ = static_cast<int32_t>(desc_->schema_rowkey_col_cnt_);
      bf_macro_header_->micro_block_count_ = 0;
      bf_macro_header_->micro_block_data_offset_ = static_cast<int32_t>(common_header_size + bf_macro_header_size);
      bf_macro_header_->row_count_ = static_cast<int32_t>(row_count);
      MEMSET(bf_macro_header_->compressor_name_, 0, OB_MAX_COMPRESSOR_NAME_LENGTH);
      MEMCPY(bf_macro_header_->compressor_name_, desc_->compressor_name_, strlen(desc_->compressor_name_));
      if (OB_FAIL(data_buffer_.advance(bf_macro_header_size))) {
        STORAGE_LOG(
            WARN, "Failed to advance data buffer for bloomfilter macro header", K(bf_macro_header_size), K(ret));
      }
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockWriter::build_macro_meta(ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter not init", K(ret));
  } else if (nullptr == full_meta.meta_ || nullptr == full_meta.schema_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else if (OB_ISNULL(bf_macro_header_) || OB_ISNULL(desc_)) {
    STORAGE_LOG(WARN, "Unexpected null macro header or datastore desc", KP_(bf_macro_header), KP_(desc), K(ret));
  } else {
    ObMacroBlockMetaV2& meta = const_cast<ObMacroBlockMetaV2&>(*full_meta.meta_);
    ObMacroBlockSchemaInfo& schema = const_cast<ObMacroBlockSchemaInfo&>(*full_meta.schema_);
    meta.attr_ = ObMacroBlockCommonHeader::BloomFilterData;
    meta.data_version_ = bf_macro_header_->data_version_;
    meta.column_number_ = 0;
    meta.rowkey_column_number_ = static_cast<int16_t>(desc_->schema_rowkey_col_cnt_);
    meta.column_index_scale_ = 0;
    meta.row_store_type_ = desc_->row_store_type_;
    meta.row_count_ = bf_macro_header_->row_count_;
    meta.occupy_size_ = bf_macro_header_->occupy_size_;
    meta.data_checksum_ = bf_macro_header_->data_checksum_;
    meta.micro_block_count_ = bf_macro_header_->micro_block_count_;
    meta.micro_block_data_offset_ = bf_macro_header_->micro_block_data_offset_;
    meta.micro_block_index_offset_ =
        bf_macro_header_->micro_block_data_offset_ + bf_macro_header_->micro_block_data_size_;
    meta.micro_block_endkey_offset_ = 0;
    meta.column_checksum_ = NULL;
    meta.endkey_ = NULL;
    meta.table_id_ = bf_macro_header_->table_id_;
    meta.data_seq_ = -1;
    meta.schema_version_ = desc_->schema_version_;
    meta.snapshot_version_ = 0;
    meta.row_count_delta_ = 0;
    meta.micro_block_mark_deletion_offset_ = 0;
    meta.macro_block_deletion_flag_ = false;
    meta.micro_block_delta_offset_ = 0;
    meta.partition_id_ = bf_macro_header_->partition_id_;
    meta.column_checksum_method_ = CCM_UNKOWN;

    schema.column_number_ = 0;
    schema.rowkey_column_number_ = meta.rowkey_column_number_;
    schema.schema_version_ = meta.schema_version_;
    schema.schema_rowkey_col_cnt_ = static_cast<int16_t>(desc_->schema_rowkey_col_cnt_);
    schema.compressor_ = bf_macro_header_->compressor_name_;
    schema.column_id_array_ = NULL;
    schema.column_type_array_ = NULL;
    schema.column_order_array_ = NULL;
    if (OB_UNLIKELY(!full_meta.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected bloomfilter macro meta", K(full_meta), K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockWriter::flush_macro_block()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterMacroBlockWriter not init", K(ret));
  } else {
    ObFullMacroBlockMeta full_meta;
    ObMacroBlockMetaV2 macro_meta;
    ObMacroBlockSchemaInfo macro_schema;
    full_meta.schema_ = &macro_schema;
    full_meta.meta_ = &macro_meta;
    ObStorageFile* file = NULL;
    if (OB_FAIL(build_macro_meta(full_meta))) {
      STORAGE_LOG(WARN, "Failed to generate bloomfilter macro meta", K(ret));
    } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(file_handle_));
    } else {
      ObMacroBlockHandle macro_handle;
      ObMacroBlockWriteInfo macro_write_info;
      macro_write_info.buffer_ = data_buffer_.data();
      macro_write_info.size_ = data_buffer_.capacity();
      macro_write_info.meta_ = full_meta;
      macro_write_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
      macro_write_info.io_desc_.category_ = SYS_IO;
      macro_write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
      macro_write_info.block_write_ctx_ = &block_write_ctx_;
      macro_handle.set_file(file);
      if (OB_FAIL(file->write_block(macro_write_info, macro_handle))) {
        STORAGE_LOG(WARN, "Failed to write bloomfilter macro block", K(ret));
      } else {
        share::ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO,
            "Successed to flush bloomfilter macro block",
            "macro_block_id",
            macro_handle.get_macro_id(),
            K(data_buffer_.length()),
            K(*bf_macro_header_),
            K(ret));
      }
    }
  }

  return ret;
}

ObBloomFilterDataWriter::ObBloomFilterDataWriter()
    : bf_cache_value_(), bf_macro_writer_(), rowkey_column_count_(), is_inited_(false), file_handle_()
{}

ObBloomFilterDataWriter::~ObBloomFilterDataWriter()
{}

void ObBloomFilterDataWriter::reset()
{
  bf_cache_value_.reset();
  bf_macro_writer_.reset();
  rowkey_column_count_ = 0;
  is_inited_ = false;
  file_handle_.reset();
}

void ObBloomFilterDataWriter::reuse()
{
  bf_cache_value_.reuse();
  bf_macro_writer_.reuse();
}

int ObBloomFilterDataWriter::init(const ObDataStoreDesc& desc)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBloomFilterDataWriter init twice", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObBloomFilterDataWriter", K(desc), K(ret));
  } else if (OB_FAIL(bf_cache_value_.init(desc.schema_rowkey_col_cnt_, BLOOM_FILTER_MAX_ROW_COUNT))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter cache value", K(desc), K(ret));
  } else if (bf_cache_value_.get_serialize_size() > desc.macro_block_size_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "Unexpected large bloomfilter rowcount or small macro block size", K(desc), K_(bf_cache_value), K(ret));
  } else if (OB_FAIL(bf_macro_writer_.init(desc))) {
    STORAGE_LOG(WARN, "Failed to init bloomfilter macro block writer", K(desc), K(ret));
  } else if (OB_FAIL(file_handle_.assign(desc.file_handle_))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(desc.file_handle_));
  } else {
    rowkey_column_count_ = desc.schema_rowkey_col_cnt_;
    is_inited_ = true;
  }

  return ret;
}

int ObBloomFilterDataWriter::append(const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterDataWriter not init", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid() || row.row_val_.count_ < rowkey_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid row to append to bloomfilter", K(row), K(ret));
  } else if (get_row_count() >= BLOOM_FILTER_MAX_ROW_COUNT) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Too many row for bloomfilter", K_(bf_cache_value));
  } else {
    const ObStoreRowkey rowkey(row.row_val_.cells_, rowkey_column_count_);
    ret = append(rowkey);
  }
  return ret;
}

int ObBloomFilterDataWriter::append(const ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterDataWriter not init", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid rowkey to append to bloomfilter", K(rowkey), K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count_ != rowkey.get_obj_cnt())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Different rowkey count to append to bloomfilter", K_(rowkey_column_count), K(rowkey), K(ret));
  } else if (get_row_count() >= BLOOM_FILTER_MAX_ROW_COUNT) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Too many row for bloomfilter", K_(bf_cache_value));
  } else if (OB_FAIL(bf_cache_value_.insert(rowkey))) {
    STORAGE_LOG(WARN, "Failed to insert rowkey to bloomfilter cache", K(rowkey), K(ret));
  }

  return ret;
}

int ObBloomFilterDataWriter::append(const ObBloomFilterCacheValue& bf_cache_value)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterDataWriter not init", K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid rowkey to append to bloomfilter", K(bf_cache_value), K(ret));
  } else if (OB_UNLIKELY(rowkey_column_count_ != bf_cache_value.get_prefix_len())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Different rowkey count to append to bloomfilter", K_(rowkey_column_count), K(bf_cache_value));
  } else if (get_row_count() + bf_cache_value.get_row_count() > BLOOM_FILTER_MAX_ROW_COUNT) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Too many row for bloomfilter", K_(bf_cache_value), K(bf_cache_value));
  } else if (!bf_cache_value_.could_merge_bloom_filter(bf_cache_value)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(INFO, "Could not merge bloomfilter", K_(bf_cache_value), K(bf_cache_value));
  } else if (OB_FAIL(bf_cache_value_.merge_bloom_filter(bf_cache_value))) {
    STORAGE_LOG(WARN, "Failed to merge bloomfilter value", K(ret));
  }

  return ret;
}

int ObBloomFilterDataWriter::flush_bloom_filter()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBloomFilterDataWriter not init", K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value_.is_valid() || bf_cache_value_.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bloomfilter cache value to flush", K_(bf_cache_value), K(ret));
  } else if (OB_FAIL(bf_macro_writer_.write(bf_cache_value_))) {
    STORAGE_LOG(WARN, "Failed to write bloomfilter cache value to macro block", K(ret));
  } else {
    STORAGE_LOG(INFO, "Succ to flush bloomfilter cache value to macro block", K(ret));
  }

  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
