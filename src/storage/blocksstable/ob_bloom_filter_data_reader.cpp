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
#include "ob_bloom_filter_data_reader.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

ObBloomFilterMacroBlockReader::ObBloomFilterMacroBlockReader(const bool is_sys_read)
    : macro_reader_(), macro_handle_(), is_sys_read_(is_sys_read)
{}

ObBloomFilterMacroBlockReader::~ObBloomFilterMacroBlockReader()
{}

void ObBloomFilterMacroBlockReader::reset()
{
  macro_handle_.reset();
  is_sys_read_ = false;
}

int ObBloomFilterMacroBlockReader::read_macro_block(
    const ObMacroBlockCtx& macro_block_ctx, ObStorageFile* pg_file, const char*& bf_buf, int64_t& bf_size)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  const char* block_buf = NULL;
  int64_t block_size = 0;

  if (OB_UNLIKELY(!macro_block_ctx.is_valid() || NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block id to read bloomfilter", K(ret), K(macro_block_ctx), KP(pg_file));
    STORAGE_LOG(WARN, "Invalid macro block id to read bloomfilter", K(macro_block_ctx), K(ret));
  } else if (OB_FAIL(macro_block_ctx.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(check_macro_meta(full_meta))) {
    STORAGE_LOG(WARN, "Failed to check bloomfilter macro block meta", K(ret));
  } else if (OB_FAIL(read_macro_block(macro_block_ctx, pg_file))) {
    STORAGE_LOG(WARN, "Failed to read bloomfilter macro block", K(ret));
  } else if (OB_FAIL(decompress_micro_block(full_meta, block_buf, block_size))) {
    STORAGE_LOG(WARN, "Failed to decompress micro block", K(ret));
  } else if (OB_FAIL(read_micro_block(block_buf, block_size, bf_buf, bf_size))) {
    STORAGE_LOG(WARN, "Failed to read micro block to bloom filter", K(ret));
  }

  return ret;
}

int ObBloomFilterMacroBlockReader::check_macro_meta(const ObFullMacroBlockMeta& full_meta) const
{
  int ret = OB_SUCCESS;
  const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;

  if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid macro meta", K(macro_meta), K(ret));
  } else if (macro_meta->attr_ != ObMacroBlockCommonHeader::BloomFilterData) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected macro meta type", K(macro_meta), K(ret));
  } else if (macro_meta->column_number_ != 0 || macro_meta->rowkey_column_number_ <= 0 || macro_meta->row_count_ <= 0 ||
             macro_meta->micro_block_count_ != 1 || macro_meta->micro_block_data_offset_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected macro meta for bloomfilter", K(*macro_meta), K(ret));
  }

  return ret;
}

int ObBloomFilterMacroBlockReader::read_macro_block(const ObMacroBlockCtx& macro_block_ctx, ObStorageFile* pg_file)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!macro_block_ctx.is_valid() || NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block id to read bloomfilter", K(ret), K(macro_block_ctx), KP(pg_file));
  } else {
    ObMacroBlockReadInfo macro_read_info;
    macro_read_info.macro_block_ctx_ = &macro_block_ctx;
    macro_read_info.io_desc_.category_ = is_sys_read_ ? SYS_IO : USER_IO;
    macro_read_info.io_desc_.wait_event_no_ =
        is_sys_read_ ? ObWaitEventIds::DB_FILE_COMPACT_READ : ObWaitEventIds::DB_FILE_DATA_READ;
    macro_read_info.offset_ = 0;
    macro_read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
    macro_handle_.reset();
    macro_handle_.set_file(pg_file);
    if (OB_FAIL(pg_file->read_block(macro_read_info, macro_handle_))) {
      STORAGE_LOG(WARN, "Failed to read bloom filter macro block", K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockReader::decompress_micro_block(
    const ObFullMacroBlockMeta& full_meta, const char*& block_buf, int64_t& block_size)
{
  int ret = OB_SUCCESS;
  const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
  const char* data_buffer = NULL;

  if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else if (OB_ISNULL(macro_handle_.get_buffer()) || 0 == macro_meta->micro_block_data_offset_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Unexpected null data buffer", K(ret));
  } else if (FALSE_IT(data_buffer = macro_handle_.get_buffer() + macro_meta->micro_block_data_offset_)) {
  } else {
    const int64_t data_size = macro_meta->micro_block_index_offset_ - macro_meta->micro_block_data_offset_;
    bool is_compressed = false;
    if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(data_buffer, data_size, BF_MICRO_BLOCK_HEADER_MAGIC))) {
      STORAGE_LOG(WARN, "Failed to check record header", K(ret));
    } else if (OB_FAIL(macro_reader_.decompress_data(
                   full_meta, data_buffer, data_size, block_buf, block_size, is_compressed))) {
      STORAGE_LOG(WARN, "Failed to decompress micro block data", K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockReader::read_micro_block(
    const char* buf, const int64_t buf_size, const char*& bf_buf, int64_t& bf_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_size <= static_cast<int64_t>(sizeof(ObBloomFilterMicroBlockHeader))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid buf to read bloomfilter micro block", KP(buf), K(buf_size), K(ret));
  } else {
    const ObBloomFilterMicroBlockHeader* bf_micro_header = reinterpret_cast<const ObBloomFilterMicroBlockHeader*>(buf);
    if (OB_UNLIKELY(!bf_micro_header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected bloomfilter micro block header", K(*bf_micro_header), K(ret));
    } else {
      bf_buf = buf + bf_micro_header->header_size_;
      bf_size = buf_size - bf_micro_header->header_size_;
    }
  }

  return ret;
}

ObBloomFilterDataReader::ObBloomFilterDataReader(const bool is_sys_read) : bf_macro_reader_(is_sys_read)
{}

ObBloomFilterDataReader::~ObBloomFilterDataReader()
{}

int ObBloomFilterDataReader::read_bloom_filter(
    const ObMacroBlockCtx& macro_block_ctx, ObStorageFile* pg_file, ObBloomFilterCacheValue& bf_cache_value)
{
  int ret = OB_SUCCESS;
  const char* bf_buf = NULL;
  int64_t bf_size = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(!macro_block_ctx.is_valid() || NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to read bloomfilter", K(ret), K(macro_block_ctx), KP(pg_file));
  } else if (OB_FAIL(bf_macro_reader_.read_macro_block(macro_block_ctx, pg_file, bf_buf, bf_size))) {
    STORAGE_LOG(WARN, "Failed to read bloomfilter macro block", K(macro_block_ctx), K(ret));
  } else if (OB_ISNULL(bf_buf) || bf_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexppected bloomfilter data", KP(bf_buf), K(bf_size), K(ret));
  } else if (OB_FAIL(bf_cache_value.deserialize(bf_buf, bf_size, pos))) {
    STORAGE_LOG(WARN, "Failed to deserialize bloomfilter cache", K(ret));
  } else if (OB_UNLIKELY(!bf_cache_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid bloomfilter cache", K(bf_cache_value), K(ret));
  }
  bf_macro_reader_.reset();

  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
