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
#include "ob_bloom_filter_cache.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{

ObBloomFilterMacroBlockReader::ObBloomFilterMacroBlockReader(const bool is_sys_read)
  : macro_reader_(),
    macro_handle_(),
    common_header_(),
    bf_macro_header_(nullptr),
    is_sys_read_(is_sys_read),
    io_allocator_("BFR_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    io_buf_(nullptr)
{
}

ObBloomFilterMacroBlockReader::~ObBloomFilterMacroBlockReader()
{
}

void ObBloomFilterMacroBlockReader::reset()
{
  macro_handle_.reset();
  common_header_.reset();
  bf_macro_header_ = nullptr;
  is_sys_read_ = false;
  io_allocator_.reset();
  io_buf_ = nullptr;
}

int ObBloomFilterMacroBlockReader::read_macro_block(
    const MacroBlockId &macro_id,
    const char *&bf_buf,
    int64_t &bf_size)
{
 int ret = OB_SUCCESS;
 const char *block_buf = nullptr;
 int64_t block_size = 0;

 if (OB_UNLIKELY(!macro_id.is_valid())) {
   ret = OB_INVALID_ARGUMENT;
   STORAGE_LOG(WARN, "Invalid macro block id to read bloomfilter", K(ret), K(macro_id));
 } else if (OB_FAIL(read_macro_block(macro_id))) {
   STORAGE_LOG(WARN, "Failed to read bloomfilter macro block", K(ret));
 } else if (OB_FAIL(decompress_micro_block(block_buf, block_size))) {
   STORAGE_LOG(WARN, "Failed to decompress micro block", K(ret));
 } else if (OB_FAIL(read_micro_block(block_buf, block_size, bf_buf, bf_size))) {
   STORAGE_LOG(WARN, "Failed to read micro block to bloom filter", K(ret));
 }

 return ret;
}

int ObBloomFilterMacroBlockReader::read_macro_block(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro block id to read bloomfilter", K(ret), K(macro_id));
  } else {
    macro_handle_.reset();
    ObMacroBlockReadInfo macro_read_info;
    macro_read_info.macro_block_id_ = macro_id;
    macro_read_info.io_desc_.set_wait_event(is_sys_read_ ? ObWaitEventIds::DB_FILE_COMPACT_READ : ObWaitEventIds::DB_FILE_DATA_READ);
    macro_read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    macro_read_info.io_desc_.set_sys_module_id(ObIOModule::BLOOM_FILTER_IO);
    macro_read_info.offset_ = 0;
    macro_read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
    if (OB_ISNULL(io_buf_) && OB_ISNULL(io_buf_ =
        reinterpret_cast<char*>(io_allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret));
    } else {
      macro_read_info.buf_ = io_buf_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle_))) {
      STORAGE_LOG(WARN, "Failed to read bloom filter macro block", K(ret));
    }
  }

  return ret;
}

int ObBloomFilterMacroBlockReader::decompress_micro_block(
    const char *&block_buf,
    int64_t &block_size)
{
  int ret = OB_SUCCESS;
  const char *data_buf = nullptr;

  int64_t pos = 0;
  if (OB_ISNULL(data_buf = macro_handle_.get_buffer())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null macro block data", K(ret), K_(macro_handle));
  } else if (OB_FAIL(common_header_.deserialize(data_buf, macro_handle_.get_data_size(), pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize common header", K(ret), K_(macro_handle));
  } else if (OB_FAIL(common_header_.check_integrity())) {
    STORAGE_LOG(ERROR, "macro block common header corrupted", K(ret));
  } else if (FALSE_IT(bf_macro_header_ = reinterpret_cast<const ObBloomFilterMacroBlockHeader *>(
      data_buf + pos))) {
  } else if (OB_UNLIKELY(!bf_macro_header_->is_valid()
      || !common_header_.is_bloom_filter_data_block())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid bloom filter macro block header",
        K(ret), K_(common_header), KPC(bf_macro_header_));
  } else {
    const char *micro_data_buf = data_buf + bf_macro_header_->micro_block_data_offset_;
    const int64_t micro_data_size = bf_macro_header_->micro_block_data_size_;
    bool is_compressed = false;
    if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(
        micro_data_buf, micro_data_size, BF_MICRO_BLOCK_HEADER_MAGIC))) {
      STORAGE_LOG(WARN, "Fail to check record header", K(ret));
    } else if (OB_FAIL(macro_reader_.decompress_data(
        bf_macro_header_->compressor_type_,
        micro_data_buf,
        micro_data_size,
        block_buf,
        block_size,
        is_compressed))) {
      STORAGE_LOG(WARN, "Fail to decompress micro block data", K(ret));
    }
  }
  return ret;
}

int ObBloomFilterMacroBlockReader::read_micro_block(const char *buf, const int64_t buf_size,
                                                    const char *&bf_buf, int64_t &bf_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_size <= static_cast<int64_t>(sizeof(ObBloomFilterMicroBlockHeader))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid buf to read bloomfilter micro block", KP(buf), K(buf_size), K(ret));
  } else {
    const ObBloomFilterMicroBlockHeader *bf_micro_header =
        reinterpret_cast<const ObBloomFilterMicroBlockHeader *>(buf);
    if (OB_UNLIKELY(!bf_micro_header->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexcepted bloomfilter micro block header", K(*bf_micro_header), K(ret));
    } else {
      bf_buf = buf + bf_micro_header->header_size_;
      bf_size = buf_size - bf_micro_header->header_size_;
    }
  }

  return ret;
}

ObBloomFilterDataReader::ObBloomFilterDataReader(const bool is_sys_read)
  : bf_macro_reader_(is_sys_read)
{
}

ObBloomFilterDataReader::~ObBloomFilterDataReader()
{
}

int ObBloomFilterDataReader::read_bloom_filter(
    const MacroBlockId &macro_id,
    ObBloomFilterCacheValue &bf_cache_value)
{
  int ret = OB_SUCCESS;
  const char *bf_buf = NULL;
  int64_t bf_size = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to read bloomfilter", K(ret), K(macro_id));
  } else if (OB_FAIL(bf_macro_reader_.read_macro_block(
      macro_id, bf_buf, bf_size))) {
    STORAGE_LOG(WARN, "Failed to read bloomfilter macro block", K(macro_id), K(ret));
  } else if (OB_ISNULL(bf_buf) || OB_UNLIKELY(bf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bloomfilter data", KP(bf_buf), K(bf_size), K(ret));
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
