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

#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/blocksstable/cs_encoding/ob_cs_micro_block_transformer.h"

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
ObMicroBlockCacheValue::ObMicroBlockCacheValue() : block_data_(), alloc_by_block_io_(false)
{
}

ObMicroBlockCacheValue::ObMicroBlockCacheValue(
    const char *buf,
    const int64_t size,
    const char *extra_buf /* = NULL */,
    const int64_t extra_size /* = 0 */,
    const ObMicroBlockData::Type block_type /* = DATA_BLOCK */)
    : block_data_(buf, size, extra_buf, extra_size, block_type),
      alloc_by_block_io_(false)
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
        if (block_data_.get_store_type() == ObRowStoreType::CS_ENCODING_ROW_STORE) {
          // no need update cached coder for CS_ENCODING_ROW_STORE
        } else if (OB_FAIL(ObMicroBlockDecoder::update_cached_decoders(
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
        char *new_extra_buf = new_buf + block_data_.get_buf_size();
        const int64_t new_extra_buf_size = buf_len - block_data_.get_buf_size();
        ObIndexBlockDataHeader *dst_idx_header = reinterpret_cast<ObIndexBlockDataHeader *>(new_extra_buf);
        int64_t pos = sizeof(ObIndexBlockDataHeader);
        if (OB_FAIL(dst_idx_header->deep_copy_transformed_index_block(
            *src_idx_header, new_extra_buf_size, new_extra_buf, pos))) {
          LOG_WARN("Fail to deep copy transformed index block", K(ret));
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
        pvalue->alloc_by_block_io_ = alloc_by_block_io_;
      }
    } else {
      pvalue = new (buf) ObMicroBlockCacheValue(new_buf, block_data_.get_buf_size(),
          nullptr, 0, block_data_.type_);
      pvalue->alloc_by_block_io_ = alloc_by_block_io_;
    }
    value = pvalue;
  }
  return ret;
}

/*---------------------------------Multi Block IO parameters--------------------------------------*/
ObMultiBlockIOResult::ObMultiBlockIOResult() :
    micro_blocks_(nullptr),
    handles_(nullptr),
    micro_infos_(nullptr),
    block_count_(0),
    ret_code_(OB_SUCCESS)
{
}

ObMultiBlockIOResult::~ObMultiBlockIOResult()
{
}

int ObMultiBlockIOResult::get_block_data(
    const int64_t index, const ObMicroBlockInfo &micro_info, ObMicroBlockData &block_data) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != ret_code_)) {
    ret = ret_code_;
    LOG_WARN("async process block failed", K(ret));
  } else if (OB_UNLIKELY(NULL == micro_blocks_ || NULL == handles_ || NULL == micro_infos_ || block_count_ <= 0)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret), KP(micro_blocks_), KP(handles_), KP_(micro_infos), K_(block_count));
  } else if (OB_UNLIKELY(index >= block_count_ || index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(index), K_(block_count));
  } else if (OB_UNLIKELY(NULL == micro_blocks_[index] ||
                         micro_infos_[index].offset_ != micro_info.offset_ ||
                         micro_infos_[index].size_ != micro_info.size_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("micro_block is null or invalid", K(ret),
             "handle validity", handles_[index].is_valid(), K(index), K(micro_info), K(micro_infos_[index]));
  } else {
    block_data = micro_blocks_[index]->get_block_data();
  }
  return ret;
}

void ObMultiBlockIOParam::reset()
{
  is_reverse_ = false;
  data_cache_size_ = 0;
  micro_block_count_ = 0;
  io_read_batch_size_ = 0;
  io_read_gap_size_ = 0;
  row_header_ = nullptr;
  prefetch_idx_.reset();
  micro_infos_.reset();
}

void ObMultiBlockIOParam::reuse()
{
  data_cache_size_ = 0;
  micro_block_count_ = 0;
  row_header_ = nullptr;
}

bool ObMultiBlockIOParam::is_valid() const
{
  return nullptr != row_header_ && data_cache_size_ > 0 && micro_block_count_ > 0;
}

int ObMultiBlockIOParam::init(
    const ObTableIterParam &iter_param,
    const int64_t micro_count_cap,
    const bool is_reverse,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  is_reverse_ = is_reverse;
  io_read_batch_size_ = MAX(iter_param.get_io_read_batch_size(), 0);
  io_read_gap_size_ = MAX(iter_param.get_io_read_gap_size(), 0);
  prefetch_idx_.set_allocator(&allocator);
  micro_infos_.set_allocator(&allocator);
  if (OB_UNLIKELY(0 >= micro_count_cap || MAX_MICRO_BLOCK_READ_COUNT < micro_count_cap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected micro count cap", K(ret), K(micro_count_cap));
  } else if (OB_FAIL(prefetch_idx_.prepare_reallocate(micro_count_cap))) {
    LOG_WARN("Fail to init prefetch idx array", K(ret), K(micro_count_cap));
  } else if (OB_FAIL(micro_infos_.prepare_reallocate(micro_count_cap))) {
    LOG_WARN("Fail to init micro info array", K(ret), K(micro_count_cap));
  }
  return ret;
}

bool ObMultiBlockIOParam::add_micro_data(const ObMicroIndexInfo &index_info,
                                         const int64_t micro_data_prefetch_idx,
                                         storage::ObMicroBlockDataHandle &micro_handle,
                                         bool &need_split)
{
  int64_t size = 0;
  bool need_prefetch = false;
  need_split = false;
  if (0 == micro_block_count_) {
    row_header_ = index_info.row_header_;
    size = index_info.get_block_size();
  } else if (!is_reverse_) {
    size = index_info.get_block_offset() + index_info.get_block_size() - micro_infos_[0].offset_;
  } else {
    size = micro_infos_[0].offset_ + micro_infos_[0].size_ - index_info.get_block_offset();
  }

  if ((data_cache_size_ + index_info.get_block_size() + io_read_gap_size_) < size) {
    need_prefetch = true;
    need_split = true;
  } else {
    prefetch_idx_[micro_block_count_] = micro_data_prefetch_idx;
    micro_infos_[micro_block_count_].set(index_info.get_block_offset(), index_info.get_block_size());
    data_cache_size_ += index_info.get_block_size();
    micro_block_count_++;
    need_prefetch = (micro_infos_.cap() == micro_block_count_) || (size > io_read_batch_size_);
  }
  return need_prefetch;
}

/*---------------------------------------ObIMicroBlockIOCallback-------------------------------------*/
ObIMicroBlockIOCallback::ObIMicroBlockIOCallback()
  : cache_(nullptr),
    put_size_stat_(nullptr),
    allocator_(nullptr),
    data_buffer_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    block_id_(),
    offset_(0),
    block_des_meta_(),
    use_block_cache_(true),
    rowkey_col_descs_(nullptr)
{
  MEMSET(encrypt_key_, 0, sizeof(encrypt_key_));
  block_des_meta_.encrypt_key_ = encrypt_key_;
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIMicroBlockIOCallback::~ObIMicroBlockIOCallback()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(data_buffer_)) {
    allocator_->free(data_buffer_);
    data_buffer_ = nullptr;
  }
  allocator_ = nullptr;
  rowkey_col_descs_ = nullptr;
}

void ObIMicroBlockIOCallback::set_micro_des_meta(const ObIndexBlockRowHeader *idx_row_header)
{
  OB_ASSERT(nullptr != idx_row_header);
  block_des_meta_.compressor_type_ = idx_row_header->get_compressor_type();
  block_des_meta_.encrypt_id_ = idx_row_header->get_encrypt_id();
  block_des_meta_.master_key_id_ = idx_row_header->get_master_key_id();
  block_des_meta_.row_store_type_ = idx_row_header->get_row_store_type();
  MEMCPY(encrypt_key_, idx_row_header->get_encrypt_key(), share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

int ObIMicroBlockIOCallback::alloc_data_buf(const char *io_data_buffer, const int64_t data_size)
{
  //UNUSED NOW
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error, the allocator is NULL, ", KP_(allocator), K(ret));
  } else if (OB_UNLIKELY(data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", K(ret), K(data_size));
  } else {
    data_buffer_ = static_cast<char *>(allocator_->alloc(data_size));
    for (int64_t i = 1; OB_ISNULL(data_buffer_) && i <= ALLOC_BUF_RETRY_TIMES; i++) {
        ob_usleep(ALLOC_BUF_RETRY_INTERVAL * i);
        data_buffer_ = static_cast<char *>(allocator_->alloc(data_size));
    }
    if (OB_ISNULL(data_buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory", K(ret), K_(offset), K(data_size), KP(data_buffer_));
    } else {
      MEMCPY(data_buffer_, io_data_buffer, data_size);
    }
  }
  return ret;
}

int ObIMicroBlockIOCallback::process_block(
    ObMacroBlockReader *reader,
    const char *buffer,
    const int64_t offset,
    const int64_t size,
    const ObMicroBlockCacheValue *&micro_block,
    common::ObKVCacheHandle &cache_handle)
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
        K(size), K_(tenant_id), KP(buffer), KP(this));
  } else {
    if (OB_UNLIKELY(!use_block_cache_)) {
      // Won't put in cache
      if (OB_FAIL(read_block_and_copy(header, *reader, buffer, size, block_data, micro_block, cache_handle))) {
        LOG_WARN("Fail to read micro block and copy to cache value", K(ret));
      }
    } else {
      ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
      ObMicroBlockCacheKey key(tenant_id_, block_id_, offset, size);
      if (OB_FAIL(cache_->get_cache(kvcache))) {
        LOG_WARN("Fail to get kvcache", K(ret));
      } else if (OB_UNLIKELY(OB_SUCCESS == (ret = kvcache->get(key, micro_block, cache_handle)))) {
        // entry exist, no need to put
      } else if (OB_FAIL(cache_->put_cache_block(
          block_des_meta_, buffer, key, *reader, *allocator_, micro_block, cache_handle, rowkey_col_descs_))) {
        LOG_WARN("Failed to put block to cache", K(ret));
      }
    }

    // NOTE: if block has column level compress and don't use kvcache,
    // the block not be full transformed here and left to be part transformed in
    // cs micro block decoder.
  }
  return ret;
}

int ObIMicroBlockIOCallback::read_block_and_copy(
    const ObMicroBlockHeader &header,
    ObMacroBlockReader &reader,
    const char *buffer,
    const int64_t size,
    ObMicroBlockData &block_data,
    const ObMicroBlockCacheValue *&micro_block,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  block_data.type_ = cache_->get_type();
  // In normal cases, full_transform is not required if not using block cache,
  // The ObMicroBlockCSDecoder can do part_transform and use the memory whose life cycle
  // is consistent with decoder. However, at present, there are cases where rows obtained from
  // index block or data block are continued to be used after decoder deconstruction,
  // so the full_transform is also done here.
  if (ObStoreFormat::is_row_store_type_with_cs_encoding(static_cast<ObRowStoreType>(header.row_store_type_))) {
    if (OB_FAIL(reader.decrypt_and_full_transform_data(header, block_des_meta_,
        buffer, size, block_data.get_buf(), block_data.get_buf_size(), nullptr))) {
      LOG_WARN("fail to decrypt_and_full_transform_data", K(ret), K(header), K_(block_des_meta));
    }
  } else {
    bool is_compressed = false;
    if (OB_FAIL(reader.do_decrypt_and_decompress_data(
        header, block_des_meta_, buffer, size,
        block_data.get_buf(), block_data.get_buf_size(),
        is_compressed, false/*need deep copy*/, nullptr))) {
      LOG_WARN("fail to do decrypt and decompress data", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObMicroBlockCacheValue value(
        block_data.get_buf(), block_data.get_buf_size(), nullptr, 0, block_data.type_);
    value.set_alloc_by_block_io();
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

/*-----------------------------------ObAsyncSingleMicroBlockIOCallback-----------------------------------*/
ObAsyncSingleMicroBlockIOCallback::ObAsyncSingleMicroBlockIOCallback()
  : ObIMicroBlockIOCallback(),
    micro_block_(nullptr),
    cache_handle_()
{
  STATIC_ASSERT(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObAsyncSingleMicroBlockIOCallback::~ObAsyncSingleMicroBlockIOCallback()
{
  // release micro_block_ outside
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(micro_block_) && !cache_handle_.is_valid()) {
    allocator_->free(const_cast<ObMicroBlockCacheValue *>(micro_block_));
    micro_block_ = nullptr;
  }
}

int64_t ObAsyncSingleMicroBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObAsyncSingleMicroBlockIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("AsyncSingle_Callback_Process", 100000); //100ms
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else {
    ObMacroBlockReader *reader = nullptr;
    if (OB_ISNULL(reader = GET_TSI_MULT(ObMacroBlockReader, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(process_block(reader, data_buffer, offset_, size, micro_block_, cache_handle_))) {
      LOG_WARN("process_block failed", K(ret));
    }
  }

  return ret;
}

const char *ObAsyncSingleMicroBlockIOCallback::get_data()
{
  return reinterpret_cast<const char *>(micro_block_);
}

/*-----------------------------------ObMultiDataBlockIOCallback-----------------------------------*/
ObMultiDataBlockIOCallback::ObMultiDataBlockIOCallback()
  : ObIMicroBlockIOCallback(),
    io_ctx_(),
    io_result_()
{
  STATIC_ASSERT(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObMultiDataBlockIOCallback::~ObMultiDataBlockIOCallback()
{
  if (nullptr != allocator_) {
    if (nullptr != io_ctx_.micro_infos_) {
      allocator_->free(io_ctx_.micro_infos_);
      io_ctx_.micro_infos_ = nullptr;
    }
  }

  free_result();
}

int64_t ObMultiDataBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObMultiDataBlockIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("MultiData_Callback_Process", 100000); //100ms
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else {
    ObMacroBlockReader *reader = nullptr;
    if (OB_ISNULL(reader = GET_TSI_MULT(ObMacroBlockReader, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(alloc_result())) {
      LOG_WARN("alloc_result failed", K(ret));
    }

    const int64_t block_count = io_ctx_.micro_block_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      const int64_t data_size = io_ctx_.micro_infos_[i].get_block_size();
      const int64_t data_offset = io_ctx_.micro_infos_[i].get_block_offset() - offset_;
      if (OB_FAIL(process_block(
          reader,
          data_buffer + data_offset,
          offset_ + data_offset,
          data_size,
          io_result_.micro_blocks_[i],
          io_result_.handles_[i]))) {
        LOG_WARN("process_block failed", K(ret));
      } else {
        io_result_.micro_infos_[i] = io_ctx_.micro_infos_[i];
      }
    }
  }

  if (OB_FAIL(ret)) {
    io_result_.ret_code_ = ret;
  }
  return ret;
}

const char *ObMultiDataBlockIOCallback::get_data()
{
  const char *data = nullptr;
  data = reinterpret_cast<const char*>(&io_result_);
  return data;
}

int ObMultiDataBlockIOCallback::set_io_ctx(
    const ObMultiBlockIOParam &io_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io_param", K(ret), K(io_param));
  } else {
    void *ptr = NULL;
    int64_t alloc_size = sizeof(ObMicroBlockInfo) * io_param.count();
    if (OB_UNLIKELY(nullptr == (ptr = allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(alloc_size));
    } else {
      io_ctx_.micro_infos_ = reinterpret_cast<ObMicroBlockInfo *>(ptr);
      MEMCPY(io_ctx_.micro_infos_, io_param.micro_infos_.get_data(), alloc_size);
      io_ctx_.micro_block_count_ = io_param.count();
    }
  }
  return ret;
}

int ObMultiDataBlockIOCallback::alloc_result()
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  const int64_t block_count = io_ctx_.micro_block_count_;
  if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObMicroBlockCacheValue *) * block_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else {
    io_result_.micro_blocks_ = reinterpret_cast<const ObMicroBlockCacheValue **>(ptr);
    MEMSET(io_result_.micro_blocks_, 0, sizeof(ObMicroBlockCacheValue *) * block_count);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObMicroBlockInfo) * block_count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else {
      io_result_.micro_infos_ = new (ptr) ObMicroBlockInfo[block_count];
    }
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

void ObMultiDataBlockIOCallback::free_result()
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
    if (OB_NOT_NULL(io_result_.micro_infos_)) {
      allocator_->free(io_result_.micro_infos_);
      io_result_.micro_infos_ = nullptr;
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

/* ---------------------------------------- ObSyncSingleMicroBLockIOCallback ---------------------------------------- */
ObSyncSingleMicroBLockIOCallback::ObSyncSingleMicroBLockIOCallback()
  : ObIMicroBlockIOCallback(),
    macro_reader_(nullptr),
    block_data_(nullptr),
    is_data_block_(true)
{
}

ObSyncSingleMicroBLockIOCallback::~ObSyncSingleMicroBLockIOCallback()
{
}

int64_t ObSyncSingleMicroBLockIOCallback::size() const
{
  return sizeof(*this);
}

int ObSyncSingleMicroBLockIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  ObTimeGuard time_guard("SyncSingle_Callback_Process", 100000); //100ms
  if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else {
    if (OB_UNLIKELY(nullptr == macro_reader_ || nullptr == block_data_ || nullptr == allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected reader or block data", K(ret), KP(macro_reader_), KP(block_data_), KP_(allocator));
    } else {
      const char *src_block_buf = data_buffer;
      const int64_t src_buf_size = size;
      ObMicroBlockHeader header;
      if (OB_FAIL(header.deserialize_and_check_header(src_block_buf, src_buf_size))) {
        LOG_WARN("fail to deserialize_and_check_header", K(ret), KP(src_block_buf), K(src_buf_size));
      } else if (ObStoreFormat::is_row_store_type_with_cs_encoding(static_cast<ObRowStoreType>(header.row_store_type_))) {
        if (OB_FAIL(macro_reader_->decrypt_and_full_transform_data(
            header, block_des_meta_, src_block_buf, src_buf_size,
            block_data_->get_buf(), block_data_->get_buf_size(), allocator_))) {
          LOG_WARN("fail to decrypt_and_full_transform_data", K(ret), K(header), K(block_des_meta_), K(is_data_block_));
        }
      } else { // not cs_encoding
        if (OB_FAIL(macro_reader_->do_decrypt_and_decompress_data(
            header, block_des_meta_, src_block_buf, src_buf_size, block_data_->get_buf(),
            block_data_->get_buf_size(), is_compressed, true /* need_deep_copy */, allocator_))) {
          LOG_WARN("Fail to decrypt and decompress micro block data buf", K(ret));
        }
      }
    }
  }

  return ret;
}

const char *ObSyncSingleMicroBLockIOCallback::get_data()
{
  return reinterpret_cast<char *>(block_data_);
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
    const bool use_cache,
    ObMacroBlockHandle &macro_handle,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_header = idx_row.row_header_;
  if (OB_ISNULL(idx_header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null index block row header", K(ret), K(idx_row));
  } else if (OB_UNLIKELY(!idx_header->is_valid() || 0 >= idx_header->get_block_size() || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid data index block row header ", K(ret), K(idx_row), KP(allocator));
  } else {
    void *buf = nullptr;
    ObAsyncSingleMicroBlockIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObAsyncSingleMicroBlockIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObAsyncSingleMicroBlockIOCallback;
      callback->allocator_ = allocator;
      callback->use_block_cache_ = use_cache;
            if (OB_FAIL(prefetch(tenant_id, macro_id, idx_row, macro_handle, *callback))) {
        LOG_WARN("Fail to prefetch data micro block", K(ret));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(callback->get_allocator())) { //Avoid double_free with io_handle
        callback->~ObAsyncSingleMicroBlockIOCallback();
        allocator->free(callback);
      }
    }
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMicroIndexInfo& idx_row,
    ObMacroBlockHandle &macro_handle,
    ObIMicroBlockIOCallback &callback)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = idx_row.row_header_;
  if (OB_ISNULL(idx_row_header)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    // fill callback
    callback.cache_ = this;
    callback.put_size_stat_ = this;
    callback.tenant_id_ = tenant_id;
    callback.block_id_ = macro_id;
    callback.offset_ = idx_row.get_block_offset();
    callback.set_rowkey_col_descs(idx_row.get_rowkey_col_descs());
    callback.set_micro_des_meta(idx_row_header);
    // fill read info
    ObMacroBlockReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    read_info.io_desc_.set_sys_module_id(ObIOModule::MICRO_BLOCK_CACHE_IO);
    read_info.io_callback_ = &callback;
    read_info.offset_ = idx_row.get_block_offset();
    read_info.size_ = idx_row.get_block_size();
    read_info.io_timeout_ms_ = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
    } else {
      EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, idx_row.get_block_size());
    }
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMultiBlockIOParam &io_param,
    const bool use_cache,
    ObMacroBlockHandle &macro_handle,
    ObIMicroBlockIOCallback &callback)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t size = 0;
  // fill callback
  io_param.get_io_range(offset, size);
  callback.cache_ = this;
  callback.put_size_stat_ = this;
  callback.tenant_id_ = tenant_id;
  callback.block_id_ = macro_id;
  callback.offset_ = offset;
  callback.use_block_cache_ = use_cache;
  callback.set_micro_des_meta(io_param.row_header_);
  // fill read info
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::MICRO_BLOCK_CACHE_IO);
  read_info.io_callback_ = &callback;
  read_info.offset_ = offset;
  read_info.size_ = size;
  read_info.io_timeout_ms_ = max(THIS_WORKER.get_timeout_remain() / 1000, 0);
  if (OB_FAIL(ObBlockManager::async_read_block(read_info, macro_handle))) {
    STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
  } else {
    EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT, io_param.micro_block_count_);
    EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, size);
  }
  return ret;
}

int ObIMicroBlockCache::add_put_size(const int64_t put_size)
{
  UNUSED(put_size);
  return OB_SUCCESS;
}

/*-----------------------------------ObMicroBlockBufTranformer-----------------------------------*/
ObMicroBlockBufTransformer::ObMicroBlockBufTransformer(
                                        const ObMicroBlockDesMeta &block_des_meta,
                                        ObMacroBlockReader *reader,
                                        ObMicroBlockHeader &header,
                                        const char *buf,
                                        const int64_t buf_size)
  : is_inited_(false),
    is_cs_full_transfrom_(ObStoreFormat::is_row_store_type_with_cs_encoding(
      static_cast<ObRowStoreType>(header.row_store_type_))),
    block_des_meta_(block_des_meta),
    reader_(reader), header_(header),
    payload_buf_(buf + header.header_size_),
    payload_size_(buf_size - header.header_size_)
{
}

int ObMicroBlockBufTransformer::init()
{
  int ret = OB_SUCCESS;
  if (is_cs_full_transfrom_) {
    bool is_compressed = false;
#ifdef OB_BUILD_TDE_SECURITY
    if (share::ObEncryptionUtil::need_encrypt(static_cast<ObCipherOpMode>(block_des_meta_.encrypt_id_))) {
      const char *decrypt_buf = NULL;
      int64_t decrypt_len = 0;
      if (OB_FAIL(reader_->decrypt_buf(
          block_des_meta_, payload_buf_, payload_size_, decrypt_buf, decrypt_len))) {
        LOG_WARN("Fail to decrypt data", K(ret));
      } else {
        payload_buf_ = decrypt_buf;
        payload_size_ = decrypt_len;
        is_compressed = (header_.data_length_ != decrypt_len);
      }
    }
#else
    is_compressed = header_.is_compressed_data();
#endif
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_compressed)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cs encoding must has no block-level compression", K(ret), K(header_));
      } else if (OB_FAIL(transformer_.init(&header_, payload_buf_, payload_size_))) {
        LOG_WARN("fail to init cs micro block transformer", K(ret), K_(header));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

int ObMicroBlockBufTransformer::get_buf_size(int64_t &buf_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!is_cs_full_transfrom_) {
    buf_size = header_.header_size_ + header_.data_length_;
  } else if (OB_FAIL(transformer_.calc_full_transform_size(buf_size))) {
    LOG_WARN("fail to calc transformed size", K(ret), K_(header));
  }
  return ret;
}

int ObMicroBlockBufTransformer::transfrom(char *block_buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == block_buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(block_buf), K(buf_size));
  } else if (!is_cs_full_transfrom_) {
    ObMicroBlockHeader *micro_header = nullptr;
    int64_t pos = 0;
    if (OB_FAIL(header_.deep_copy(block_buf, buf_size, pos, micro_header))) {
      LOG_WARN("Fail to deep copy header", K(ret), K_(header));
    } else {
#ifdef OB_BUILD_TDE_SECURITY
      const char *decrypt_buf = NULL;
      int64_t decrypt_len = 0;
      if (OB_FAIL(reader_->decrypt_buf(
        block_des_meta_, payload_buf_, payload_size_, decrypt_buf, decrypt_len))) {
        LOG_WARN("fail to decrypt data", K(ret), K_(header));
      } else {
        payload_buf_ = decrypt_buf;
        payload_size_ = decrypt_len;
      }
#endif
      if (OB_SUCC(ret)) {
        if (OB_FAIL(reader_->decompress_data_with_prealloc_buf(
            block_des_meta_.compressor_type_, payload_buf_, payload_size_,
            block_buf + pos, buf_size - pos))) {
          LOG_WARN("Fail to decompress data with preallocated buffer", K(ret), K_(header));
        }
      }
    }
  } else { // is_cs_full_transfrom_
    int64_t pos = 0;
    if (OB_FAIL(transformer_.full_transform(block_buf, buf_size, pos))) {
      LOG_WARN("fail to transfrom cs encoding mirco blcok", K(ret));
    } else if (OB_UNLIKELY(pos != buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pos should equal to buf_size", K(ret), K(pos), K(buf_size));
    }
  }

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
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_MIDDLE_BLOCK_SIZE, OB_MALLOC_MIDDLE_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else {
    allocator_.set_attr(SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_SSTABLE_MICRO_BLOCK_ALLOCATOR)));
  }
  return ret;
}

void ObDataMicroBlockCache::destroy()
{
  common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::destroy();
  allocator_.destroy();
}

int ObDataMicroBlockCache::prefetch_multi_block(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMultiBlockIOParam &io_param,
    const bool use_cache,
    ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  ObMultiDataBlockIOCallback *callback = nullptr;
  ObIAllocator *allocator = nullptr;
  if (OB_FAIL(get_allocator(allocator))) {
    LOG_WARN("Fail to get allocator", K(ret));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObMultiDataBlockIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObMultiDataBlockIOCallback;
      callback->allocator_ = allocator;
      if (OB_UNLIKELY(!io_param.is_valid() || 0 == tenant_id || OB_INVALID_TENANT_ID == tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid input parameters", K(ret), K(tenant_id));
      } else if (OB_FAIL(callback->set_io_ctx(io_param))) {
        LOG_WARN("Set io context failed", K(ret), K(io_param));
      } else if (OB_FAIL(ObIMicroBlockCache::prefetch(
          tenant_id, macro_id, io_param, use_cache, macro_handle, *callback))) {
        LOG_WARN("Fail to prefetch multi data blocks", K(ret));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(callback->get_allocator())) { //Avoid double_free with io_handle
        callback->~ObMultiDataBlockIOCallback();
        allocator->free(callback);
      }
    }
  }
  return ret;
}

int ObDataMicroBlockCache::load_block(
    const ObMicroBlockId &micro_block_id,
    const ObMicroBlockDesMeta &des_meta,
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo macro_read_info;
  ObMacroBlockHandle macro_handle;
  bool is_compressed = false;
  if (OB_UNLIKELY(!micro_block_id.is_valid() || nullptr == macro_reader || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(micro_block_id), KP(macro_reader), KP(allocator));
  } else {
    void *buf = nullptr;
    ObSyncSingleMicroBLockIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObSyncSingleMicroBLockIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObSyncSingleMicroBLockIOCallback;
      callback->allocator_ = allocator;
      callback->offset_ = micro_block_id.offset_;
      callback->block_des_meta_ = des_meta;
      callback->block_data_ = &block_data;
      callback->macro_reader_ = macro_reader;
      callback->is_data_block_ = true;

      macro_read_info.macro_block_id_ = micro_block_id.macro_id_;
      macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      macro_read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
      macro_read_info.io_desc_.set_sys_module_id(ObIOModule::MICRO_BLOCK_CACHE_IO);
      macro_read_info.io_callback_ = callback;
      macro_read_info.offset_ = micro_block_id.offset_;
      macro_read_info.size_ = micro_block_id.size_;
      macro_read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle))) {
        LOG_WARN("Fail to sync read block", K(ret), K(macro_read_info));
      } else {
        block_data.type_ = ObMicroBlockData::DATA_BLOCK;
        EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
        EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, micro_block_id.size_);
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(callback->get_allocator())) { //Avoid double_free with io_handle
        callback->~ObSyncSingleMicroBLockIOCallback();
        allocator->free(callback);
      }
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

int64_t ObDataMicroBlockCache::calc_value_size(const int64_t data_length,
                                               const ObRowStoreType &type,
                                               bool &need_decoder)
{
  need_decoder = false;
  int64_t value_size = sizeof(ObMicroBlockCacheValue) + data_length;
  if (ObStoreFormat::is_row_store_type_with_encoding(type)) {
    need_decoder = true;
    value_size += ObMicroBlockDecoder::MAX_CACHED_DECODER_BUF_SIZE;
  }
  return value_size;
}

int ObDataMicroBlockCache::write_extra_buf(const ObRowStoreType row_store_type,
                                           const char *block_buf,
                                           const int64_t block_size,
                                           char *extra_buf,
                                           ObMicroBlockData &micro_data)
{
  int ret = OB_SUCCESS;
  int64_t decoder_size = 0;

  if (ObStoreFormat::is_row_store_type_with_cs_encoding(row_store_type)) {
    if (OB_FAIL(ObMicroBlockCSDecoder::get_decoder_cache_size(block_buf, block_size, decoder_size))) {
      LOG_WARN("Fail to get decoder cache size", K(ret));
    } else if (OB_FAIL(ObMicroBlockCSDecoder::cache_decoders(extra_buf, decoder_size, block_buf, block_size))) {
      LOG_WARN("Fail to set cache decoder", K(ret));
    }
  } else if (OB_FAIL(ObMicroBlockDecoder::get_decoder_cache_size(block_buf, block_size, decoder_size))) {
    LOG_WARN("Fail to get decoder cache size", K(ret));
  } else if (OB_FAIL(ObMicroBlockDecoder::cache_decoders(extra_buf, decoder_size, block_buf, block_size))) {
    LOG_WARN("Fail to set cache decoder", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else {
    micro_data.get_extra_buf() = extra_buf;
    micro_data.get_extra_size() = decoder_size;
  }

  return ret;
}

int ObDataMicroBlockCache::put_cache_block(
    const ObMicroBlockDesMeta &des_meta,
    const char *raw_block_buf,
    const ObMicroBlockCacheKey &key,
    ObMacroBlockReader &reader,
    ObIAllocator &allocator,
    const ObMicroBlockCacheValue *&micro_block,
    common::ObKVCacheHandle &cache_handle,
    const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  int64_t payload_size = 0;
  const char *payload_buf = nullptr;
  const ObMicroBlockId &micro_id = key.get_micro_block_id();
  ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
  if (OB_UNLIKELY(!des_meta.is_valid() || !micro_id.is_valid()) || OB_ISNULL(raw_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(key), K(des_meta));
  } else if (OB_FAIL(header.deserialize(raw_block_buf, micro_id.size_, pos))) {
    LOG_ERROR("Fail to deserialize record header", K(ret), K(micro_id));
  } else if (OB_FAIL(header.check_and_get_record(
        raw_block_buf, micro_id.size_, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
    LOG_ERROR("Micro block data is corrupted", K(ret), K(key), KP(raw_block_buf), KP(this));
  } else if (OB_FAIL(get_cache(kvcache))) {
    LOG_WARN("Fail to get kvcache", K(ret));
  } else {
    ObKVCacheInstHandle inst_handle;
    ObKVCachePair *kvpair = nullptr;
    int64_t block_size = 0;
    int64_t value_size = 0;
    int64_t extra_size = 0;
    bool need_decoder = false;
    ObMicroBlockBufTransformer buf_transformer(des_meta, &reader, header, payload_buf, payload_size);
    if (OB_FAIL(buf_transformer.init())) {
      LOG_WARN("Fail to init buf transformer", K(ret));
    } else if (OB_FAIL(buf_transformer.get_buf_size(block_size))) {
      LOG_WARN("Fail to get block buf size", K(ret));
    } else if (FALSE_IT(value_size = calc_value_size(block_size, des_meta.row_store_type_, need_decoder))) {
    } else if (OB_FAIL(alloc(
        key.get_tenant_id(), sizeof(ObMicroBlockCacheKey), value_size, kvpair, cache_handle, inst_handle))) {
      LOG_WARN("Fail to allocate kvpair from kvcache", K(ret), K(value_size), K(key));
    } else {
      char *block_buf = reinterpret_cast<char *>(kvpair->value_) + sizeof(ObMicroBlockCacheValue);
      kvpair->key_ = new (kvpair->key_) ObMicroBlockCacheKey(key);
      ObMicroBlockCacheValue *cache_value = new (kvpair->value_) ObMicroBlockCacheValue(block_buf, block_size);
      ObMicroBlockData &micro_data = cache_value->get_block_data();
      micro_data.type_ = get_type();
      if (OB_FAIL(buf_transformer.transfrom(block_buf, block_size))) {
        LOG_WARN("fail to transfrom", K(ret));
      } else if (need_decoder && OB_FAIL(write_extra_buf(
          des_meta.row_store_type_, block_buf, block_size, block_buf + block_size, micro_data))) {
        LOG_WARN("Fail to cache decoder on extra buffer for data block", K(ret), K(header), KPC(cache_value));
      } else if (FALSE_IT(micro_block = cache_value)) {
      } else if (OB_FAIL(put_kvpair(inst_handle, kvpair, cache_handle, false /* overwrite */))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("Fail to put micro block cache", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, *cache_value);
        if (OB_FAIL(add_put_size(put_size))) {
          LOG_WARN("add_put_size failed", K(ret), K(put_size));
        }
      }
      if (OB_FAIL(ret)) {
        cache_handle.reset();
        micro_block = nullptr;
      }
    }
  }
  return ret;
}

int ObDataMicroBlockCache::reserve_kvpair(
    const ObMicroBlockDesc &micro_block_desc,
    ObKVCacheInstHandle &inst_handle,
    ObKVCacheHandle &cache_handle,
    ObKVCachePair *&kvpair,
    int64_t &kvpair_size)
{
  int ret = OB_SUCCESS;

  kvpair_size = 0;
  int64_t block_size = 0;
  int64_t extra_size = 0;
  int64_t value_size = 0;
  bool need_decoder = false;
  const int64_t key_size = sizeof(ObMicroBlockCacheKey);
  const ObRowStoreType row_store_type = static_cast<ObRowStoreType>(micro_block_desc.header_->row_store_type_);
  ObCSMicroBlockTransformer transformer;
  if (OB_UNLIKELY(!micro_block_desc.is_valid() || inst_handle.is_valid()
                  || cache_handle.is_valid() || nullptr != kvpair)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(micro_block_desc), K(inst_handle), K(cache_handle), KP(kvpair));
  } else if (ObStoreFormat::is_row_store_type_with_cs_encoding(row_store_type)) {
    if (OB_FAIL(transformer.init(micro_block_desc.header_, micro_block_desc.buf_, micro_block_desc.buf_size_))) {
      LOG_WARN("fail to init transformer", K(ret), K(micro_block_desc));
    } else if (OB_FAIL(transformer.calc_full_transform_size(block_size))) {
      LOG_WARN("fail to calc_full_transform_size", K(ret), K(micro_block_desc));
    }
  } else {
    block_size = micro_block_desc.header_->header_size_ + micro_block_desc.data_size_;
  }

  if (OB_SUCC(ret)) {
    value_size = calc_value_size(block_size, row_store_type, need_decoder);
    BaseBlockCache *kvcache = nullptr;
    if (OB_FAIL(get_cache(kvcache))) {
      LOG_WARN("Fail to get cache", K(ret));
    } else if (OB_FAIL(kvcache->alloc(MTL_ID(), key_size, value_size, kvpair, cache_handle, inst_handle))) {
      LOG_WARN("Fail to alloc cache buf", K(ret), K(key_size), K(value_size));
    } else {
      char *block_buf = reinterpret_cast<char *>(kvpair->value_) + sizeof(ObMicroBlockCacheValue);
      kvpair->key_ = new (kvpair->key_) ObMicroBlockCacheKey();
      ObMicroBlockCacheValue *cache_value = new (kvpair->value_) ObMicroBlockCacheValue(block_buf, block_size);
      int64_t pos = 0;
      if (ObStoreFormat::is_row_store_type_with_cs_encoding(row_store_type)) {
        if (OB_FAIL(transformer.full_transform(block_buf, block_size, pos))) {
          LOG_WARN("fail to full transform", K(ret), KP(block_buf), K(block_size));
        }
      } else {
        MEMCPY(block_buf, micro_block_desc.header_, micro_block_desc.header_->header_size_);
        MEMCPY(block_buf + micro_block_desc.header_->header_size_, micro_block_desc.buf_, micro_block_desc.buf_size_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_decoder && OB_FAIL(write_extra_buf(
      static_cast<ObRowStoreType>(micro_block_desc.header_->row_store_type_),
      reinterpret_cast<char *>(kvpair->value_) + sizeof(ObMicroBlockCacheValue),
      block_size,
      reinterpret_cast<char *>(kvpair->value_) + sizeof(ObMicroBlockCacheValue) + block_size,
      static_cast<ObMicroBlockCacheValue *>(kvpair->value_)->get_block_data()))) {
    LOG_WARN("Fail to write decoder in extra buf", K(ret));
  } else {
    kvpair_size = sizeof(ObMicroBlockCacheKey) + value_size;
  }

  return ret;
}

ObMicroBlockData::Type ObDataMicroBlockCache::get_type()
{
  return ObMicroBlockData::DATA_BLOCK;
}

void ObDataMicroBlockCache::cache_bypass()
{
  EVENT_INC(ObStatEventIds::DATA_BLOCK_READ_CNT);
}

void ObDataMicroBlockCache::cache_hit(int64_t &hit_cnt)
{
  ++hit_cnt;
  EVENT_INC(ObStatEventIds::DATA_BLOCK_CACHE_HIT);
}

void ObDataMicroBlockCache::cache_miss(int64_t &miss_cnt)
{
  ++miss_cnt;
  EVENT_INC(ObStatEventIds::DATA_BLOCK_READ_CNT);
}

/*-------------------------------------ObIndexMicroBlockCache-------------------------------------*/
ObIndexMicroBlockCache::ObIndexMicroBlockCache()
  : ObDataMicroBlockCache()
{
}

ObIndexMicroBlockCache::~ObIndexMicroBlockCache()
{
}

int ObIndexMicroBlockCache::init(const char *cache_name, const int64_t priority)
{
  return ObDataMicroBlockCache::init(cache_name, priority);
}

int ObIndexMicroBlockCache::load_block(
    const ObMicroBlockId &micro_block_id,
    const ObMicroBlockDesMeta &des_meta,
    ObMacroBlockReader *macro_reader,
    ObMicroBlockData &block_data,
    ObIAllocator *allocator)
{
  UNUSED(macro_reader);
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo macro_read_info;
  ObMacroBlockHandle macro_handle;
  // TODO: @chengji make deserialize micro block with allocator static and remove tmp inner_macro_reader
  ObMacroBlockReader inner_macro_reader;
  bool is_compressed = false;
  if (OB_UNLIKELY(!micro_block_id.is_valid()) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(micro_block_id), KP(allocator));
  } else {
    void *buf = nullptr;
    ObSyncSingleMicroBLockIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObSyncSingleMicroBLockIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObSyncSingleMicroBLockIOCallback;
      callback->allocator_ = allocator;
      callback->offset_ = micro_block_id.offset_;
      callback->block_des_meta_ = des_meta;
      callback->block_data_ = &block_data;
      callback->macro_reader_ = &inner_macro_reader;
      callback->is_data_block_ = false;

      macro_read_info.macro_block_id_ = micro_block_id.macro_id_;
      macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      macro_read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
      macro_read_info.io_desc_.set_sys_module_id(ObIOModule::MICRO_BLOCK_CACHE_IO);
      macro_read_info.io_callback_ = callback;
      macro_read_info.offset_ = micro_block_id.offset_;
      macro_read_info.size_ = micro_block_id.size_;
      macro_read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      ObIndexBlockDataTransformer idx_transformer;
      char *transform_buf = nullptr;
      char *raw_idx_block_buf = nullptr;
      if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle))) {
        LOG_WARN("Fail to sync read block", K(ret), K(macro_read_info));
      }
      // the reason why block_data.get_buf() can be released by this allocator directly is that the
      // memory is deep coiped in ObSyncSingleMicroBLockIOCallback. Maybe we should deep copy the memory in any case.
      raw_idx_block_buf = const_cast<char *>(block_data.get_buf());
      if (FAILEDx(idx_transformer.transform(block_data, block_data, *allocator, transform_buf))) {
        LOG_WARN("Fail to transform index block to memory format", K(ret));
      } else {
        EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
        EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, micro_block_id.size_);
      }
      if (nullptr != raw_idx_block_buf) {
        allocator->free(raw_idx_block_buf);
      }
      if (OB_FAIL(ret)) {
        if (nullptr != callback->get_allocator()) { //Avoid double_free with io_handle
          callback->~ObSyncSingleMicroBLockIOCallback();
          allocator->free(callback);
        }
        if (nullptr != transform_buf) {
          allocator->free(transform_buf);
        }
        block_data.reset();
      }
    }
  }
  return ret;
}

int ObIndexMicroBlockCache::put_cache_block(
    const ObMicroBlockDesMeta &des_meta,
    const char *raw_block_buf,
    const ObMicroBlockCacheKey &key,
    ObMacroBlockReader &reader,
    ObIAllocator &allocator,
    const ObMicroBlockCacheValue *&micro_block,
    common::ObKVCacheHandle &cache_handle,
    const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
{
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  int64_t payload_size = 0;
  const char *payload_buf = nullptr;
  const ObMicroBlockId &micro_id = key.get_micro_block_id();
  ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
  if (OB_UNLIKELY(!des_meta.is_valid() || !micro_id.is_valid()) || OB_ISNULL(raw_block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(key), K(des_meta));
  } else if (OB_FAIL(header.deserialize(raw_block_buf, micro_id.size_, pos))) {
    LOG_ERROR("Fail to deserialize record header", K(ret), K(micro_id));
  } else if (OB_FAIL(header.check_and_get_record(
        raw_block_buf, micro_id.size_, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
    LOG_ERROR("Micro block data is corrupted", K(ret), K(key), KP(raw_block_buf), KP(this));
  } else if (OB_FAIL(get_cache(kvcache))) {
    LOG_WARN("Fail to get kvcache", K(ret));
  } else {
    int64_t block_size = 0;
    int64_t value_size = 0;
    char *block_buf = nullptr;
    ObMicroBlockBufTransformer buf_transformer(des_meta, &reader, header, payload_buf, payload_size);
    ObIndexBlockDataTransformer idx_transformer;
    if (OB_FAIL(buf_transformer.init())) {
      LOG_WARN("Fail to init block buf transformer", K(ret));
    } else if (OB_FAIL(buf_transformer.get_buf_size(block_size))) {
      LOG_WARN("Fail to get transformed buf size", K(ret));
    } else if (OB_ISNULL(block_buf = static_cast<char *>(allocator.alloc(block_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory", K(ret), K(block_size));
    } else if (OB_FAIL(buf_transformer.transfrom(block_buf, block_size))) {
      LOG_WARN("fail to transfrom block buf", K(ret));
    } else {
      ObMicroBlockCacheValue cache_value(block_buf, block_size);
      ObMicroBlockData &block_data = cache_value.get_block_data();
      block_data.type_ = get_type();
      char *allocated_buf = nullptr;
      if (OB_FAIL(idx_transformer.transform(block_data, block_data, allocator, allocated_buf, rowkey_col_descs))) {
        LOG_WARN("Fail to transform index block to memory format", K(ret));
      } else if (OB_FAIL(put_and_fetch(key, cache_value, micro_block, cache_handle, false /* overwrite */))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("Fail to put micro block cache", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, cache_value);
        if (OB_FAIL(add_put_size(put_size))) {
          LOG_WARN("add_put_size failed", K(ret), K(put_size));
        }
      }

      if (nullptr != allocated_buf) {
        allocator.free(allocated_buf);
      }
    }
    if (nullptr != block_buf) {
      allocator.free(block_buf);
    }
    if (OB_FAIL(ret)) {
      cache_handle.reset();
      micro_block = nullptr;
    }
  }
  return ret;
}

ObMicroBlockData::Type ObIndexMicroBlockCache::get_type()
{
  return ObMicroBlockData::INDEX_BLOCK;
}

void ObIndexMicroBlockCache::cache_bypass()
{
  EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
}

void ObIndexMicroBlockCache::cache_hit(int64_t &hit_cnt)
{
  ++hit_cnt;
  EVENT_INC(ObStatEventIds::INDEX_BLOCK_CACHE_HIT);
}

void ObIndexMicroBlockCache::cache_miss(int64_t &miss_cnt)
{
  ++miss_cnt;
  EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
}

}//end namespace blocksstable
}//end namespace oceanbase
