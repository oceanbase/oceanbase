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
#include "lib/file/ob_file.h"
#include "share/io/ob_io_manager.h"
#include "share/schema/ob_table_param.h"
#include "lib/stat/ob_diagnose_info.h"
#include "encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{
/**
 * -----------------------------------------------------ObMicroBlockCacheKey--------------------------------------------------
 */
ObMicroBlockCacheKey::ObMicroBlockCacheKey(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size)
    : tenant_id_(tenant_id),
      block_id_(macro_id, offset, size)
{
}

ObMicroBlockCacheKey::ObMicroBlockCacheKey(
    const uint64_t tenant_id,
    const ObMicroBlockId &block_id)
    : tenant_id_(tenant_id), block_id_(block_id)
{
}

ObMicroBlockCacheKey::ObMicroBlockCacheKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID), block_id_()
{
}

ObMicroBlockCacheKey::ObMicroBlockCacheKey(const ObMicroBlockCacheKey &other)
{
  tenant_id_ = other.tenant_id_;
  block_id_ = other.block_id_;
}



ObMicroBlockCacheKey::~ObMicroBlockCacheKey()
{
}


void ObMicroBlockCacheKey::set(const uint64_t tenant_id,
                               const MacroBlockId &block_id,
                               const int64_t offset,
                               const int64_t size)
{
  tenant_id_ = tenant_id;
  block_id_.macro_id_ = block_id;
  block_id_.offset_ = offset;
  block_id_.size_ = size;
}

bool ObMicroBlockCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObMicroBlockCacheKey &other_key = reinterpret_cast<const ObMicroBlockCacheKey &> (other);
  return tenant_id_ == other_key.tenant_id_  && block_id_ == other_key.block_id_;
}

uint64_t ObMicroBlockCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObMicroBlockCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObMicroBlockCacheKey), 0);
}

int64_t ObMicroBlockCacheKey::size() const
{
  return sizeof(*this);
}

int ObMicroBlockCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(
      0 == tenant_id_ || OB_INVALID_TENANT_ID == tenant_id_ || !block_id_.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block cache key is invalid, ", K(*this), K(ret));
  } else {
    key = new (buf) ObMicroBlockCacheKey(tenant_id_, block_id_);
  }
  return ret;
}

/**
 * -----------------------------------------------------ObMicroBlockCacheValue--------------------------------------------------
 */
ObMicroBlockCacheValue::ObMicroBlockCacheValue(
    const char *buf,
    const int64_t size,
    const char *extra_buf /* = NULL */,
    const int64_t extra_size /* = 0 */,
    const ObMicroBlockData::Type block_type /* = DATA_BLOCK */)
    : block_data_(buf, size, extra_buf, extra_size, block_type)
{
}

ObMicroBlockCacheValue::~ObMicroBlockCacheValue()
{
}

int64_t ObMicroBlockCacheValue::size() const
{
  return sizeof(blocksstable::ObMicroBlockCacheValue) + block_data_.total_size();
}

int ObMicroBlockCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObMicroBlockCacheValue *pvalue = NULL;

  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(!block_data_.is_valid())) {
    //buffer_ is allowed to be NULL
    ret = OB_INVALID_DATA;
    LOG_WARN("The micro block cache value is not valid, ", K(*this), K(ret));
  } else {
    char *new_buf = buf + sizeof(blocksstable::ObMicroBlockCacheValue);
    MEMCPY(new_buf, block_data_.get_buf(), block_data_.get_buf_size());
    if (NULL != block_data_.get_extra_buf() && block_data_.get_extra_size() > 0) {
      switch (block_data_.type_) {
      case ObMicroBlockData::Type::DATA_BLOCK: {
        MEMCPY(new_buf + block_data_.get_buf_size(), block_data_.get_extra_buf(), block_data_.get_extra_size());
        if (OB_FAIL(ObMicroBlockDecoder::update_cached_decoders(
            new_buf + block_data_.get_buf_size(),
            block_data_.get_extra_size(),
            block_data_.get_buf(),
            new_buf,
            block_data_.get_buf_size()))) {
          LOG_WARN(" Update cached pointer failed", K(ret), K_(block_data), KP(new_buf));
        }
        break;
      }
      case ObMicroBlockData::Type::INDEX_BLOCK: {
        const ObIndexBlockDataHeader *src_idx_header
            = reinterpret_cast<const ObIndexBlockDataHeader *>(block_data_.get_extra_buf());
        ObIndexBlockDataTransformer *transformer = nullptr;
        ObDecoderAllocator* allocator = nullptr;
        if (OB_ISNULL(allocator = get_decoder_allocator())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate decoder allocator", K(ret));
        } else if (OB_ISNULL(transformer = GET_TSI_MULT(ObIndexBlockDataTransformer, 1))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to get thread local index block data transformer", K(ret));
        } else if (OB_FAIL(transformer->update_index_block(
            *src_idx_header,
            new_buf,
            block_data_.get_buf_size(),
            new_buf + block_data_.get_buf_size(),
            block_data_.get_extra_size()))) {
          LOG_WARN("Fail to update transformed index block", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Not Supported block data type", K(ret), K_(block_data));
      }
      if (OB_SUCC(ret)) {
        pvalue = new (buf) ObMicroBlockCacheValue(
            new_buf, block_data_.get_buf_size(),
            new_buf + block_data_.get_buf_size(), block_data_.get_extra_size(), block_data_.type_);
      }
    } else {
      pvalue = new (buf) ObMicroBlockCacheValue(
          new_buf, block_data_.get_buf_size(), nullptr, 0, block_data_.type_);
    }
    value = pvalue;
  }
  return ret;
}

/*---------------------------------Multi Block IO parameters--------------------------------------*/
ObMultiBlockIOResult::ObMultiBlockIOResult()
{
  reset();
}

ObMultiBlockIOResult::~ObMultiBlockIOResult()
{
}

void ObMultiBlockIOResult::reset()
{
  micro_blocks_ = NULL;
  handles_ = NULL;
  block_count_ = 0;
  ret_code_ = OB_SUCCESS;
}

int ObMultiBlockIOResult::get_block_data(
    const int64_t index, ObMicroBlockData &block_data) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != ret_code_) {
    ret = ret_code_;
    STORAGE_LOG(WARN, "async process block failed", K(ret));
  } else if (NULL == micro_blocks_ || NULL == handles_ || block_count_ <= 0) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "inner stat error", K(ret), KP(micro_blocks_), KP(handles_), K_(block_count));
  } else if (index >= block_count_ || index < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid index", K(ret), K(index), K_(block_count));
  } else if (NULL == micro_blocks_[index]) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "micro_block is null", K(ret),
        "handle validity", handles_[index].is_valid(), K(index));
  } else {
    block_data = micro_blocks_[index]->get_block_data();
  }
  return ret;
}

void ObMultiBlockIOParam::reset()
{
  micro_index_infos_ = nullptr;
  start_index_ = -1;
  block_count_ = -1;
}

bool ObMultiBlockIOParam::is_valid() const
{
  bool is_same_block = false;
  const bool basic_valid = nullptr != micro_index_infos_
      && start_index_ >= 0
      && block_count_ > 0
      && micro_index_infos_->count() >= start_index_ + block_count_;
  if (basic_valid) {
    const ObMicroIndexInfo &first_micro = micro_index_infos_->at(start_index_);
    const ObMicroIndexInfo &last_micro = micro_index_infos_->at(start_index_ + block_count_ - 1);
    is_same_block = first_micro.get_macro_id() == last_micro.get_macro_id();
  }
  return basic_valid && is_same_block;
}

void ObMultiBlockIOParam::get_io_range(int64_t &offset, int64_t &size) const
{
  offset = 0;
  size = 0;
  if (block_count_ > 0) {
    const int64_t end_index = start_index_ + block_count_ - 1;
    offset = micro_index_infos_->at(start_index_).get_block_offset();
    ObMicroIndexInfo &end_micro_index = micro_index_infos_->at(end_index);
    size = end_micro_index.get_block_offset() - offset + end_micro_index.get_block_size();
  }
}

int ObMultiBlockIOParam::get_block_des_info(
    ObMicroBlockDesMeta &des_meta,
    common::ObRowStoreType &row_store_type) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid multi block io parameter", K(ret), K(*this));
  } else {
    ObMicroIndexInfo &start_info = micro_index_infos_->at(start_index_);
    des_meta.compressor_type_ = start_info.row_header_->get_compressor_type();
    des_meta.encrypt_id_ = start_info.row_header_->get_encrypt_id();
    des_meta.master_key_id_ = start_info.row_header_->get_master_key_id();
    des_meta.encrypt_key_ = start_info.row_header_->get_encrypt_key();
    row_store_type = start_info.row_header_->get_row_store_type();
  }
  return ret;
}

void ObMultiBlockIOCtx::reset()
{
  micro_index_infos_ = nullptr;
  block_count_ = 0;
}

bool ObMultiBlockIOCtx::is_valid() const
{
  return OB_NOT_NULL(micro_index_infos_) && block_count_ > 0;
}

/*----------------------------------------ObIMicroBlockCache--------------------------------------*/
int ObIMicroBlockCache::get_cache_block(
    const uint64_t tenant_id,
    const MacroBlockId block_id,
    const int64_t offset,
    const int64_t size,
    ObMicroBlockBufferHandle &handle)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = NULL;
  if (OB_UNLIKELY(0 == tenant_id || OB_INVALID_TENANT_ID == tenant_id || !block_id.is_valid()
      || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret),
        KP(cache), K(tenant_id), K(block_id), K(offset), K(size));
  } else if (OB_FAIL(get_cache(cache))) {
    STORAGE_LOG(WARN, "get_cache failed", K(ret));
  } else {
    ObMicroBlockCacheKey key(tenant_id, block_id, offset, size);
    if (OB_FAIL(cache->get(key, handle.micro_block_, handle.handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to get micro block from block cache, ", K(ret));
      }
      EVENT_INC(ObStatEventIds::BLOCK_CACHE_MISS);
    } else {
      EVENT_INC(ObStatEventIds::BLOCK_CACHE_HIT);
    }
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMicroIndexInfo& idx_row,
    const common::ObQueryFlag &flag,
    const ObTableReadInfo &full_read_info,
    const ObTabletHandle &tablet_handle,
    ObMacroBlockHandle &macro_handle)
{
  UNUSEDx(tenant_id, macro_id, idx_row, flag, full_read_info, tablet_handle, macro_handle);
  int ret = OB_SUCCESS;
  ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObIMicroBlockCache::load_block(
    const ObMicroBlockId &micro_block_id,
    const ObMicroBlockDesMeta &des_meta,
    const ObTableReadInfo *read_info,
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    ObIAllocator *allocator)
{
  UNUSEDx(micro_block_id, des_meta, read_info, macro_reader, block_data, allocator);
  return OB_NOT_IMPLEMENT;
}

int ObIMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObIndexBlockRowHeader& idx_row_header,
    const common::ObQueryFlag &flag,
    ObMacroBlockHandle &macro_handle,
    ObIMicroBlockIOCallback &callback)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = nullptr;
  ObIAllocator *allocator = nullptr;
  if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("Fail to get base cache", K(ret));
  } else if (OB_FAIL(get_allocator(allocator))) {
    LOG_WARN("Fail to get allocator", K(ret));
  } else {
    // fill callback
    callback.cache_ = cache;
    callback.allocator_ = allocator;
    callback.put_size_stat_ = this;
    callback.tenant_id_ = tenant_id;
    callback.block_id_ = macro_id;
    callback.offset_ = idx_row_header.get_block_offset();
    callback.size_ = idx_row_header.get_block_size();
    callback.row_store_type_ = idx_row_header.get_row_store_type();
    callback.block_des_meta_.compressor_type_ = idx_row_header.get_compressor_type();
    callback.block_des_meta_.encrypt_id_ = idx_row_header.get_encrypt_id();
    callback.block_des_meta_.master_key_id_ = idx_row_header.get_master_key_id();
    callback.block_des_meta_.encrypt_key_ = idx_row_header.get_encrypt_key();
    callback.use_block_cache_ = flag.is_use_block_cache();
    // fill read info
    ObMacroBlockReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.io_desc_.set_category(flag.is_prewarm() ? ObIOCategory::PREWARM_IO
        : flag.is_large_query() && GCONF._large_query_io_percentage.get_value() > 0
        ? ObIOCategory::LARGE_QUERY_IO : ObIOCategory::USER_IO);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_callback_ = &callback;
    common::align_offset_size(
        idx_row_header.get_block_offset(),
        idx_row_header.get_block_size(),
        read_info.offset_,
        read_info.size_);
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    } else {
      EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, idx_row_header.get_block_size());
    }
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMultiBlockIOParam &io_param,
    const ObQueryFlag &flag,
    ObMacroBlockHandle &macro_handle,
    ObIMicroBlockIOCallback &callback)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t size = 0;
  BaseBlockCache *cache = nullptr;
  ObIAllocator *allocator = nullptr;
  if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("Fail to get base cache", K(ret));
  } else if (OB_FAIL(get_allocator(allocator))) {
    LOG_WARN("Fail to get allocator", K(ret));
  } else if (OB_FAIL(io_param.get_block_des_info(
      callback.block_des_meta_, callback.row_store_type_))) {
    LOG_WARN("Fail to get meta data for deserializing block data", K(ret), K(io_param));
  } else {
    // fill callback
    io_param.get_io_range(offset, size);
    callback.cache_ = cache;
    callback.allocator_ = allocator;
    callback.put_size_stat_ = this;
    callback.tenant_id_ = tenant_id;
    callback.block_id_ = macro_id;
    callback.offset_ = offset;
    callback.size_ = size;
    callback.use_block_cache_ = flag.is_use_block_cache();
    // fill read info
    ObMacroBlockReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.io_desc_.set_category(flag.is_prewarm() ? ObIOCategory::PREWARM_IO
        : flag.is_large_query() && GCONF._large_query_io_percentage.get_value() > 0
        ? ObIOCategory::LARGE_QUERY_IO : ObIOCategory::USER_IO);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_callback_ = &callback;
    common::align_offset_size(offset, size, read_info.offset_, read_info.size_);

    if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    } else {
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT, io_param.block_count_);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, size);
    }
  }
  return ret;
}

int ObIMicroBlockCache::add_put_size(const int64_t put_size)
{
  UNUSED(put_size);
  return OB_SUCCESS;
}

/*---------------------------------------MicroBlockIOCallback-------------------------------------*/
ObIMicroBlockCache::ObIMicroBlockIOCallback::ObIMicroBlockIOCallback()
  : cache_(nullptr),
    put_size_stat_(nullptr),
    allocator_(nullptr),
    io_buffer_(nullptr),
    data_buffer_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    block_id_(),
    offset_(0),
    size_(0),
    row_store_type_(MAX_ROW_STORE),
    block_des_meta_(),
    use_block_cache_(true)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIMicroBlockCache::ObIMicroBlockIOCallback::~ObIMicroBlockIOCallback()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(io_buffer_)) {
    allocator_->free(io_buffer_);
    io_buffer_ = nullptr;
  }
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::alloc_io_buf(
    char *&io_buf, int64_t &align_size, int64_t &align_offset)
{
  int ret = OB_SUCCESS;
  align_size = 0;
  align_offset = 0;
  common::align_offset_size(offset_, size_, align_offset, align_size);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error, the allocator is NULL, ", KP_(allocator), K(ret));
  } else {
    io_buffer_ = static_cast<char *>(allocator_->alloc(align_size + DIO_READ_ALIGN_SIZE));
    for (int64_t i = 1; OB_ISNULL(io_buffer_) && i <= ALLOC_BUF_RETRY_TIMES; i++) {
        ob_usleep(ALLOC_BUF_RETRY_INTERVAL * i);
        io_buffer_ = static_cast<char *>(allocator_->alloc(align_size + DIO_READ_ALIGN_SIZE));
    }
    if (OB_NOT_NULL(io_buffer_)) {
      io_buf = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(io_buffer_),
                                                    DIO_READ_ALIGN_SIZE));
      data_buffer_ = io_buf + (offset_ - align_offset);
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory",
                      K(ret), K_(offset), K_(size), K(align_offset), K(align_size));
    }
  }

  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::process_block(
    ObMacroBlockReader *reader,
    char *buffer,
    const int64_t offset,
    const int64_t size,
    const ObMicroBlockCacheValue *&micro_block,
    common::ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  int64_t payload_size = 0;
  const char *payload_buf = nullptr;
  if (OB_UNLIKELY(NULL == reader || NULL == buffer || offset < 0 || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(reader), KP(buffer), K(offset), K(size));
  } else if (OB_FAIL(header.deserialize(buffer, size, pos))) {
    LOG_ERROR("Fail to deserialize record header", K(ret), K_(block_id), K(offset));
  } else if (OB_FAIL(header.check_and_get_record(
        buffer, size, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
    LOG_ERROR("Micro block data is corrupted", K(ret), K_(block_id), K(offset),
        K(size), K_(tenant_id), KP(buffer), KP(io_buffer_), KP(data_buffer_), KP(this));
  } else {
    if (OB_UNLIKELY(!use_block_cache_)) {
      // Won't put in cache
    } else {
      ObKVCachePair *kvpair = nullptr;
      ObKVCacheInstHandle inst_handle;
      const bool overwrite = false;
      ObMicroBlockCacheKey key(tenant_id_, block_id_, offset, size);
      const int64_t buf_size = header.header_size_ + header.data_length_;
      int64_t value_size = calc_value_size(buf_size, header.row_count_);
      if (OB_UNLIKELY(OB_SUCCESS == (ret = cache_->get(key, micro_block, handle)))) {
        // entry exist, no need to put
      } else if (OB_FAIL(cache_->alloc(
          tenant_id_,
          sizeof(ObMicroBlockCacheKey),
          value_size,
          kvpair,
          handle,
          inst_handle))) {
        LOG_WARN("Fail to alloc cache buf", K(ret), K_(tenant_id), K(value_size));
      } else {
        char *block_buf = reinterpret_cast<char *>(kvpair->value_)
            + sizeof(ObMicroBlockCacheValue);
        new (kvpair->key_) ObMicroBlockCacheKey(tenant_id_, block_id_, offset, size);
        ObMicroBlockCacheValue *cache_value
          = new (kvpair->value_) ObMicroBlockCacheValue(block_buf, buf_size);
        ObMicroBlockData &micro_data = cache_value->get_block_data();
        micro_data.type_ = get_type();
        int64_t pos = 0;
        if (OB_FAIL(header.serialize(block_buf, header.header_size_, pos))) {
          LOG_WARN("Fail to serialize header", K(ret), K(header));
        } else if (FALSE_IT(payload_buf = payload_buf + pos)) {
        } else if (FALSE_IT(payload_size = payload_size - pos)) {
        } else if (OB_FAIL(reader->decompress_data_with_prealloc_buf(
            block_des_meta_.compressor_type_,
            payload_buf,
            payload_size,
            block_buf + pos,
            buf_size - pos))) {
          LOG_WARN("Fail to decompress data with preallocated buffer", K(ret));
        } else if (OB_FAIL(write_extra_buf_on_demand(
            buf_size, micro_data, block_buf))) {
          LOG_WARN("Fail to writer extra buffer of block data",
              K(ret), K(header), KPC(cache_value));
        } else if (FALSE_IT(micro_block = cache_value)) {
        } else if (OB_FAIL(cache_->put_kvpair(inst_handle, kvpair, handle, overwrite))) {
          if (OB_ENTRY_EXIST != ret) {
            LOG_WARN("Fail to put micro block cache", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, *cache_value);
          if (OB_FAIL(put_size_stat_->add_put_size(put_size))) {
            LOG_WARN("add_put_size failed", K(ret), K(put_size));
          }
        }
        if (OB_FAIL(ret)) {
          handle.reset();
          micro_block = nullptr;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (use_block_cache_) {
      // block already in cache
    } else if (OB_FAIL(read_block_and_copy(*reader, buffer, size, block_data, micro_block, handle))) {
      LOG_WARN("Fail to read micro block and copy to cache value", K(ret));
    }
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::read_block_and_copy(
    ObMacroBlockReader &reader,
    char *buffer,
    const int64_t size,
    ObMicroBlockData &block_data,
    const ObMicroBlockCacheValue *&micro_block,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null pointer to data buffer", K(ret), KP(buffer));
  } else if (OB_FAIL(reader.decrypt_and_decompress_data(
      block_des_meta_, buffer, size,
      block_data.get_buf(), block_data.get_buf_size(), is_compressed))) {
    LOG_WARN("Fail to decrypt and decompress data", K(ret));
  } else {
    block_data.type_ = get_type();
    ObMicroBlockCacheValue value(
        block_data.get_buf(), block_data.get_buf_size(), nullptr, 0, block_data.type_);
    char *buf = nullptr;
    const int64_t buf_len = value.size();
    handle.reset();
    micro_block = nullptr;
    ObIKVCacheValue *value_copy = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate value", K(ret), K(buf_len));
    } else if (OB_FAIL(value.deep_copy(buf, buf_len, value_copy))) {
      LOG_WARN("Failed to deep copy value", K(ret));
      allocator_->free(buf);
    } else {
      micro_block = static_cast<const ObMicroBlockCacheValue *>(value_copy);
    }
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::cache_decoders(
    const ObColDescIArray &full_col_descs,
    const int64_t data_length,
    ObMicroBlockData &micro_data,
    char *block_buf)
{
  int ret = OB_SUCCESS;
  int64_t decoder_buf_size = 0;
  char *decoder_buf = block_buf + data_length;
  if (OB_FAIL(ObMicroBlockDecoder::get_decoder_cache_size(
              block_buf,
              data_length,
              decoder_buf_size))) {
    LOG_WARN("get decoder cache size failed", K(ret));
  } else if (OB_FAIL(ObMicroBlockDecoder::cache_decoders(
              decoder_buf,
              decoder_buf_size,
              block_buf,
              data_length,
              full_col_descs))) {
    LOG_WARN("cache decoder failed", K(ret));
  } else {
    micro_data.get_extra_buf() = decoder_buf;
    micro_data.get_extra_size() = decoder_buf_size;
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::transform_index_block(
    const ObTableReadInfo &index_read_info,
    const int64_t data_length,
    ObMicroBlockData &micro_data,
    char *block_buf,
    ObIndexBlockDataTransformer &transformer)
{
  int ret = OB_SUCCESS;
  char *extra_buf = block_buf + data_length;
  int64_t extra_size = ObIndexBlockDataTransformer::get_transformed_block_mem_size(micro_data);
  if (OB_UNLIKELY(!index_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index column read info", K(ret), K(index_read_info));
  } else if (OB_FAIL(transformer.transform(index_read_info, micro_data, extra_buf, extra_size))) {
    LOG_WARN("Fail to transform index block format",
        K(ret), K(index_read_info), K(micro_data), K(extra_size));
  } else {
    micro_data.get_extra_buf() = extra_buf;
    micro_data.get_extra_size() = extra_size;
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::assign(const ObIMicroBlockIOCallback &other)
{
  int ret = OB_SUCCESS;
  cache_ = other.cache_;
  put_size_stat_ = other.put_size_stat_;
  allocator_ = other.allocator_;
  io_buffer_ = other.io_buffer_;
  data_buffer_ = other.data_buffer_;
  tenant_id_ = other.tenant_id_;
  block_id_ = other.block_id_;
  offset_ = other.offset_;
  size_ = other.size_;
  row_store_type_ = other.row_store_type_;
  block_des_meta_ = other.block_des_meta_;
  use_block_cache_ = other.use_block_cache_;
  return ret;
}

/*-------------------------------------ObDataMicroBlockCache--------------------------------------*/
int ObDataMicroBlockCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 4 * 1024 * 1024 * 1024LL;
  if (OB_SUCCESS != (ret = common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::init(
      cache_name, priority))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_SSTABLE_MICRO_BLOCK_ALLOCATOR);
  }
  return ret;
}

int ObDataMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMicroIndexInfo& idx_row,
    const common::ObQueryFlag &flag,
    const ObTableReadInfo &full_read_info,
    const ObTabletHandle &tablet_handle,
    ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_header = idx_row.row_header_;
  if (OB_ISNULL(idx_header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null index block row header", K(ret), K(idx_row));
  } else if (OB_UNLIKELY(
          !idx_header->is_valid()
          || 0 >= idx_header->get_block_size()
          || !idx_header->is_data_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid data index block row header ", K(ret), K(idx_row));
  } else {
    ObDataMicroBlockIOCallback callback;
    callback.full_cols_ = &full_read_info.get_columns_desc();
    callback.tablet_handle_ = tablet_handle;
    callback.need_write_extra_buf_ = idx_header->is_data_index()
        && ObStoreFormat::is_row_store_type_with_encoding(idx_header->get_row_store_type());
    if (OB_FAIL(ObIMicroBlockCache::prefetch(
        tenant_id, macro_id, *idx_header, flag, macro_handle, callback))) {
      LOG_WARN("Fail to prefetch data micro block", K(ret));
    }
  }
  return ret;
}

int ObDataMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMultiBlockIOParam &io_param,
    const ObQueryFlag &flag,
    const ObTableReadInfo &full_read_info,
    ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  ObMultiDataBlockIOCallback callback;
  if (OB_UNLIKELY(!io_param.is_valid() || 0 == tenant_id || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid input parameters", K(ret), K(tenant_id));
  } else if (OB_FAIL(callback.set_io_ctx(io_param))) {
    LOG_WARN("Set io context failed", K(ret), K(io_param));
  } else if (FALSE_IT(callback.full_cols_ = &full_read_info.get_columns_desc())) {
  } else if (OB_FAIL(ObIMicroBlockCache::prefetch(
      tenant_id, macro_id, io_param, flag, macro_handle, callback))) {
    LOG_WARN("Fail to prefetch multi data blocks", K(ret));
  }
  return ret;
}

int ObDataMicroBlockCache::load_block(
    const ObMicroBlockId &micro_block_id,
    const ObMicroBlockDesMeta &des_meta,
    const ObTableReadInfo *read_info,
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    ObIAllocator *allocator)
{
  UNUSEDx(read_info, allocator);
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo macro_read_info;
  ObMacroBlockHandle macro_handle;
  bool is_compressed = false;
  const bool need_deep_copy = true;
  if (OB_UNLIKELY(!micro_block_id.is_valid()) || OB_ISNULL(macro_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(micro_block_id), KP(macro_reader));
  } else {
    macro_read_info.macro_block_id_ = micro_block_id.macro_id_;
    macro_read_info.io_desc_.set_category(ObIOCategory::USER_IO);
    macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    macro_read_info.offset_ = micro_block_id.offset_;
    macro_read_info.size_ = micro_block_id.size_;
    if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle))) {
      LOG_WARN("Fail to sync read block", K(ret), K(macro_read_info));
    } else if (OB_FAIL(macro_reader->decrypt_and_decompress_data(
        des_meta, macro_handle.get_buffer(), micro_block_id.size_, block_data.get_buf(),
        block_data.get_buf_size(), is_compressed, need_deep_copy))) {
      LOG_WARN("Fail to decrypt and decompress micro block data buf", K(ret));
    } else {
      block_data.type_ = ObMicroBlockData::DATA_BLOCK;
      EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, micro_block_id.size_);
    }
  }
  return ret;
}

int ObDataMicroBlockCache::get_cache(BaseBlockCache *&cache)
{
  int ret = OB_SUCCESS;
  cache = this;
  return ret;
}

int ObDataMicroBlockCache::get_allocator(common::ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = &allocator_;
  return ret;
}

void ObDataMicroBlockCache::destroy()
{
  common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::destroy();
  allocator_.destroy();
}

/*-----------------------------------ObDataMicroBlockIOCallback-----------------------------------*/
ObDataMicroBlockCache::ObDataMicroBlockIOCallback::ObDataMicroBlockIOCallback()
  : ObIMicroBlockIOCallback(),
    full_cols_(nullptr),
    micro_block_(nullptr),
    tablet_handle_(),
    handle_(),
    need_write_extra_buf_(false)
{
  STATIC_ASSERT(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObDataMicroBlockCache::ObDataMicroBlockIOCallback::~ObDataMicroBlockIOCallback()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(micro_block_) && !handle_.is_valid()) {
    allocator_->free(const_cast<ObMicroBlockCacheValue *>(micro_block_));
    micro_block_ = nullptr;
  }
  tablet_handle_.reset();
}

int64_t ObDataMicroBlockCache::ObDataMicroBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObDataMicroBlockCache::ObDataMicroBlockIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObMacroBlockReader *reader = nullptr;
    if (OB_ISNULL(reader = GET_TSI_MULT(ObMacroBlockReader, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(process_block(reader, data_buffer_, offset_, size_, micro_block_, handle_))) {
      LOG_WARN("process_block failed", K(ret));
    }
  }

  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(io_buffer_)) {
    allocator_->free(io_buffer_);
    io_buffer_ = nullptr;
  }
  return ret;
}

int ObDataMicroBlockCache::ObDataMicroBlockIOCallback::inner_deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIOCallback *&callback) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The micro block io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObDataMicroBlockIOCallback *pcallback = new (buf) ObDataMicroBlockIOCallback();
    if (OB_FAIL(pcallback->assign(*this))) {
      LOG_WARN("fail to assign callback", K(ret));
    } else {
      pcallback->full_cols_ = full_cols_;
      pcallback->micro_block_ = micro_block_;
      pcallback->tablet_handle_ = tablet_handle_;
      pcallback->handle_ = handle_;
      pcallback->need_write_extra_buf_ = need_write_extra_buf_;
      callback = pcallback;
    }
  }
  return ret;
}

const char *ObDataMicroBlockCache::ObDataMicroBlockIOCallback::get_data()
{
  const char *data = nullptr;
  if (OB_NOT_NULL(micro_block_)) {
    data = reinterpret_cast<const char*> (&(micro_block_->get_block_data()));
  }
  return data;
}
int ObDataMicroBlockCache::ObDataMicroBlockIOCallback::write_extra_buf_on_demand(
    const int64_t data_length,
    ObMicroBlockData &micro_data,
    char *block_buf)
{
  int ret = OB_SUCCESS;
  if (!need_write_extra_buf_
      || !ObStoreFormat::is_row_store_type_with_encoding(row_store_type_)
      || !tablet_handle_.is_valid()) {
    // Skip
  } else if (OB_ISNULL(full_cols_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null full column desc", K(ret), KP_(full_cols));
  } else if (OB_FAIL(ObIMicroBlockIOCallback::cache_decoders(
      *full_cols_, data_length, micro_data, block_buf))) {
    LOG_WARN("Fail to cache decoders", K(ret));
  }
  return ret;
}

int64_t ObDataMicroBlockCache::ObDataMicroBlockIOCallback::calc_value_size(
    int64_t data_length,
    int64_t row_count)
{
  UNUSED(row_count);
  int64_t value_size = sizeof(ObMicroBlockCacheValue) + data_length;
  if (ObStoreFormat::is_row_store_type_with_encoding(row_store_type_)) {
    value_size += ObMicroBlockDecoder::MAX_CACHED_DECODER_BUF_SIZE;
  }
  return value_size;
}

ObMicroBlockData::Type ObDataMicroBlockCache::ObDataMicroBlockIOCallback::get_type()
{
  return ObMicroBlockData::DATA_BLOCK;
}

/*-----------------------------------ObMultiDataBlockIOCallback-----------------------------------*/
ObDataMicroBlockCache::ObMultiDataBlockIOCallback::ObMultiDataBlockIOCallback()
  : ObIMicroBlockIOCallback(),
    full_cols_(nullptr),
    io_ctx_(),
    io_result_()
{
  STATIC_ASSERT(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObDataMicroBlockCache::ObMultiDataBlockIOCallback::~ObMultiDataBlockIOCallback()
{
  free_result();
}

int64_t ObDataMicroBlockCache::ObMultiDataBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObMacroBlockReader *reader = nullptr;
    if (OB_ISNULL(reader = GET_TSI_MULT(ObMacroBlockReader, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(alloc_result())) {
      LOG_WARN("alloc_result failed", K(ret));
    }

    const int64_t block_count = io_ctx_.block_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      const int64_t data_size = io_ctx_.micro_index_infos_[i].get_block_size();
      const int64_t data_offset = io_ctx_.micro_index_infos_[i].get_block_offset() - offset_;
      if (OB_FAIL(process_block(
          reader,
          data_buffer_ + data_offset,
          offset_ + data_offset,
          data_size,
          io_result_.micro_blocks_[i],
          io_result_.handles_[i]))) {
        LOG_WARN("process_block failed", K(ret));
      }
    }
  }

  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(io_buffer_)) {
    allocator_->free(io_buffer_);
    io_buffer_ = nullptr;
  }

  if (OB_FAIL(ret)) {
    io_result_.ret_code_ = ret;
  }
  return ret;
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::inner_deep_copy(char *buf,
  const int64_t buf_len, ObIOCallback *&callback) const
{
  int ret = OB_SUCCESS;
  callback = nullptr;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The micro block io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObMultiDataBlockIOCallback *pcallback = new (buf) ObMultiDataBlockIOCallback();
    pcallback->io_ctx_.reset();
    if (OB_FAIL(pcallback->assign(*this))) {
      LOG_WARN("fail to assign callback", K(ret));
    } else if (OB_FAIL(pcallback->deep_copy_ctx(io_ctx_))) {
      LOG_WARN("deep_copy_ctx failed", K(ret));
    } else {
      pcallback->full_cols_ = full_cols_;
      pcallback->io_result_ = io_result_;
      callback = pcallback;
    }
  }
  return ret;
}

const char *ObDataMicroBlockCache::ObMultiDataBlockIOCallback::get_data()
{
  const char *data = nullptr;
  data = reinterpret_cast<const char*>(&io_result_);
  return data;
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::set_io_ctx(
    const ObMultiBlockIOParam &io_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io_param", K(ret), K(io_param));
  } else {
    io_ctx_.micro_index_infos_ = &io_param.micro_index_infos_->at(io_param.start_index_);
    io_ctx_.block_count_ = io_param.block_count_;
  }
  return ret;
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::deep_copy_ctx(
    const ObMultiBlockIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io_ctx", K(ret), K(io_ctx));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("allocator_ is null", K(ret), KP(allocator_));
  } else {
    void *ptr = nullptr;
    int64_t alloc_size = sizeof(ObMicroIndexInfo) * io_ctx.block_count_;
    if (OB_ISNULL(ptr = allocator_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
    } else {
      io_ctx_.micro_index_infos_ = reinterpret_cast<ObMicroIndexInfo *>(ptr);
      MEMCPY(io_ctx_.micro_index_infos_, io_ctx.micro_index_infos_, alloc_size);
    }

    if (OB_SUCC(ret)) {
      io_ctx_.block_count_ = io_ctx.block_count_;
    }
  }
  return ret;
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::alloc_result()
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  const int64_t block_count = io_ctx_.block_count_;
  if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObMicroBlockCacheValue *) * block_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    io_result_.micro_blocks_ = reinterpret_cast<const ObMicroBlockCacheValue **>(ptr);
    MEMSET(io_result_.micro_blocks_, 0, sizeof(ObMicroBlockCacheValue *) * block_count);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObKVCacheHandle) * block_count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else {
      io_result_.handles_ = new (ptr) ObKVCacheHandle[block_count];
      io_result_.block_count_ = block_count;
    }
  }
  return ret;
}

void ObDataMicroBlockCache::ObMultiDataBlockIOCallback::free_result()
{
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(io_result_.micro_blocks_)) {
      if (OB_NOT_NULL(io_result_.handles_)) {
        for (int64_t i = 0; i < io_result_.block_count_; ++i) {
          if (!io_result_.handles_[i].is_valid()
              && OB_NOT_NULL(io_result_.micro_blocks_[i])) {
            allocator_->free(const_cast<ObMicroBlockCacheValue *>(io_result_.micro_blocks_[i]));
          }
        }
      }
      allocator_->free(io_result_.micro_blocks_);
      io_result_.micro_blocks_ = nullptr;
    }
    if (OB_NOT_NULL(io_result_.handles_)) {
      for (int64_t i = 0; i < io_result_.block_count_; ++i) {
        io_result_.handles_[i].~ObKVCacheHandle();
      }
      allocator_->free(io_result_.handles_);
      io_result_.handles_ = nullptr;
      io_result_.block_count_ = 0;
    }
  }
}

int ObDataMicroBlockCache::ObMultiDataBlockIOCallback::write_extra_buf_on_demand(
    const int64_t data_length,
    ObMicroBlockData &micro_data,
    char *block_buf)
{
  int ret = OB_SUCCESS;
  if (!ObStoreFormat::is_row_store_type_with_encoding(row_store_type_)) {
    // Skip
  } else if (OB_ISNULL(full_cols_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null full column desc", K(ret), KP_(full_cols));
  } else if (OB_FAIL(ObIMicroBlockIOCallback::cache_decoders(
      *full_cols_, data_length, micro_data, block_buf))) {
    LOG_WARN("Fail to cache decoders", K(ret));
  }
  return ret;
}

int64_t ObDataMicroBlockCache::ObMultiDataBlockIOCallback::calc_value_size(
    int64_t data_length,
    int64_t row_count)
{
  UNUSED(row_count);
  int64_t value_size = sizeof(ObMicroBlockCacheValue) + data_length;
  if (ObStoreFormat::is_row_store_type_with_encoding(row_store_type_)) {
    value_size += ObMicroBlockDecoder::MAX_CACHED_DECODER_BUF_SIZE;
  }
  return value_size;
}

ObMicroBlockData::Type ObDataMicroBlockCache::ObMultiDataBlockIOCallback::get_type()
{
  return ObMicroBlockData::DATA_BLOCK;
}

/*-------------------------------------ObIndexMicroBlockCache-------------------------------------*/
int ObIndexMicroBlockCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 4 * 1024 * 1024 * 1024LL;
  if (OB_SUCCESS != (ret = common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::init(
      cache_name, priority))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_SSTABLE_MICRO_BLOCK_ALLOCATOR);
  }
  return ret;
}

int ObIndexMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMicroIndexInfo& idx_row,
    const common::ObQueryFlag &flag,
    const ObTableReadInfo &index_read_info,
    const ObTabletHandle &tablet_handle,
    ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_header = idx_row.row_header_;
  if (OB_ISNULL(idx_header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null index block row header", K(ret), K(idx_row));
  } else if (OB_UNLIKELY(
      !idx_header->is_valid()
      || 0 >= idx_header->get_block_size()
      || idx_header->is_data_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid data index block row header ", K(ret), K(idx_row));
  } else {
    ObIndexMicroBlockIOCallback callback;
    callback.index_read_info_ = &index_read_info;
    callback.tablet_handle_ = tablet_handle;
    if (OB_FAIL(ObIMicroBlockCache::prefetch(
        tenant_id, macro_id, *idx_header, flag, macro_handle, callback))) {
      LOG_WARN("Fail to prefetch data micro block", K(ret));
    }
  }
  return ret;
}

int ObIndexMicroBlockCache::load_block(
    const ObMicroBlockId &micro_block_id,
    const ObMicroBlockDesMeta &des_meta,
    const ObTableReadInfo *read_info,
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    ObIAllocator *allocator)
{
  UNUSED(macro_reader);
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo macro_read_info;
  ObMacroBlockHandle macro_handle;
  // TODO: make deserialize micro block with allocator static and remove tmp inner_macro_reader
  ObMacroBlockReader inner_macro_reader;
  ObIndexBlockDataTransformer idx_transformer;
  bool is_compressed = false;
  const bool need_deep_copy = true;
  if (OB_UNLIKELY(!micro_block_id.is_valid())
      || OB_ISNULL(read_info)
      || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(micro_block_id), KP(read_info), KP(allocator));
  } else {
    macro_read_info.macro_block_id_ = micro_block_id.macro_id_;
    macro_read_info.io_desc_.set_category(ObIOCategory::USER_IO);
    macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    macro_read_info.offset_ = micro_block_id.offset_;
    macro_read_info.size_ = micro_block_id.size_;
    if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle))) {
      LOG_WARN("Fail to sync read block", K(ret), K(macro_read_info));
    } else if (OB_FAIL(inner_macro_reader.decrypt_and_decompress_data(
        des_meta, macro_handle.get_buffer(), micro_block_id.size_, block_data.get_buf(),
        block_data.get_buf_size(), is_compressed, need_deep_copy, allocator))) {
      LOG_WARN("Fail to decrypt and decompress micro block data buf", K(ret));
    } else {
      char *extra_buf = nullptr;
      const int64_t extra_buf_size
          = ObIndexBlockDataTransformer::get_transformed_block_mem_size(block_data);
      if (OB_ISNULL(extra_buf = reinterpret_cast<char *>(allocator->alloc(extra_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for transformed index block", K(ret));
      } else if (OB_FAIL(idx_transformer.transform(
          *read_info, block_data, extra_buf, extra_buf_size))) {
        LOG_WARN("Failed to transform index block", K(ret));
      } else {
        block_data.extra_buf_ = extra_buf;
        block_data.extra_size_ = extra_buf_size;
        block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
        EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
        EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, micro_block_id.size_);
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(extra_buf)) {
        allocator->free(extra_buf);
        block_data.extra_buf_ = nullptr;
        block_data.extra_size_ = 0;
      }
    }
  }
  return ret;
}

int ObIndexMicroBlockCache::get_cache(BaseBlockCache *&cache)
{
  int ret = OB_SUCCESS;
  cache = this;
  return ret;
}

int ObIndexMicroBlockCache::get_allocator(common::ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = &allocator_;
  return ret;
}

void ObIndexMicroBlockCache::destroy()
{
  common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::destroy();
  allocator_.destroy();
}

/*-----------------------------------ObIndexMicroBlockIOCallback----------------------------------*/
ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::ObIndexMicroBlockIOCallback()
  : ObIMicroBlockIOCallback(),
    index_read_info_(nullptr),
    micro_block_(nullptr),
    tablet_handle_(),
    handle_()
{
  STATIC_ASSERT(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::~ObIndexMicroBlockIOCallback()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(micro_block_) && !handle_.is_valid()) {
    allocator_->free(const_cast<ObMicroBlockCacheValue *>(micro_block_));
    micro_block_ = nullptr;
  }
  tablet_handle_.reset();
}

int64_t ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObMacroBlockReader *reader = nullptr;
    if (OB_ISNULL(reader = GET_TSI_MULT(ObMacroBlockReader, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(process_block(reader, data_buffer_, offset_, size_, micro_block_, handle_))) {
      LOG_WARN("process_block failed", K(ret));
    }
  }

  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(io_buffer_)) {
    allocator_->free(io_buffer_);
    io_buffer_ = nullptr;
  }
  return ret;
}

int ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::inner_deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIOCallback *&callback) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("The micro block io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObIndexMicroBlockIOCallback *pcallback = new (buf) ObIndexMicroBlockIOCallback();
    if (OB_FAIL(pcallback->assign(*this))) {
      LOG_WARN("fail to assign callback", K(ret));
    } else {
      pcallback->index_read_info_ = index_read_info_;
      pcallback->micro_block_ = micro_block_;
      pcallback->tablet_handle_ = tablet_handle_;
      pcallback->handle_ = handle_;
      callback = pcallback;
    }
  }
  return ret;
}

const char *ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::get_data()
{
  const char *data = NULL;
  if (OB_NOT_NULL(micro_block_)) {
    data = reinterpret_cast<const char*> (&(micro_block_->get_block_data()));
  }
  return data;
}
int ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::write_extra_buf_on_demand(
    const int64_t data_length,
    ObMicroBlockData &micro_data,
    char *block_buf)
{
  int ret = OB_SUCCESS;
  ObIndexBlockDataTransformer *transformer = nullptr;
  ObDecoderAllocator* allocator = nullptr;
  if (OB_ISNULL(index_read_info_)
      || OB_UNLIKELY(!index_read_info_->is_valid() || !micro_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(index_read_info_), K(micro_data));
  } else if (OB_ISNULL(allocator = get_decoder_allocator())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate decoder allocator", K(ret));
  } else if (OB_ISNULL(transformer = GET_TSI_MULT(ObIndexBlockDataTransformer, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate ObIndexBlockDataTransformer", K(ret));
  } else if (OB_FAIL(ObIMicroBlockIOCallback::transform_index_block(
      *index_read_info_,
      data_length,
      micro_data,
      block_buf,
      *transformer))) {
    LOG_WARN("Failed to transform index block",
        K(ret), KPC(index_read_info_), K(data_length), K(micro_data));
  }
  return ret;
}

int64_t ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::calc_value_size(
    int64_t data_length,
    int64_t row_count)
{
  int64_t value_size = sizeof(ObMicroBlockCacheValue) + data_length;
  value_size += ObIndexBlockDataTransformer::get_transformed_block_mem_size(
      row_count, index_read_info_->get_request_count());
  return value_size;
}

ObMicroBlockData::Type ObIndexMicroBlockCache::ObIndexMicroBlockIOCallback::get_type()
{
  return ObMicroBlockData::INDEX_BLOCK;
}

}//end namespace blocksstable
}//end namespace oceanbase
