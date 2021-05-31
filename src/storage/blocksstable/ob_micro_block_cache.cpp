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
#include "ob_micro_block_cache.h"
#include "lib/file/ob_file.h"
#include "lib/io/ob_io_manager.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_sstable.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
/**
 * -----------------------------------------------------ObMicroBlockCacheKey--------------------------------------------------
 */
ObMicroBlockCacheKey::ObMicroBlockCacheKey(const uint64_t table_id, const MacroBlockId& block_id, const int64_t file_id,
    const int64_t offset, const int64_t size)
    : table_id_(table_id), block_id_(block_id), file_id_(file_id), offset_(offset), size_(size)
{}

ObMicroBlockCacheKey::ObMicroBlockCacheKey() : table_id_(0), block_id_(), file_id_(0), offset_(0), size_()
{}

ObMicroBlockCacheKey::ObMicroBlockCacheKey(const ObMicroBlockCacheKey& other)
{
  table_id_ = other.table_id_;
  block_id_ = other.block_id_;
  file_id_ = other.file_id_;
  offset_ = other.offset_;
  size_ = other.size_;
}

ObMicroBlockCacheKey::~ObMicroBlockCacheKey()
{}

void ObMicroBlockCacheKey::set(const uint64_t table_id, const MacroBlockId& block_id, const int64_t file_id,
    const int64_t offset, const int64_t size)
{
  table_id_ = table_id;
  block_id_ = block_id;
  file_id_ = file_id;
  offset_ = offset;
  size_ = size;
}

bool ObMicroBlockCacheKey::operator==(const ObIKVCacheKey& other) const
{
  const ObMicroBlockCacheKey& other_key = reinterpret_cast<const ObMicroBlockCacheKey&>(other);
  return table_id_ == other_key.table_id_ && block_id_ == other_key.block_id_ && file_id_ == other_key.file_id_ &&
         offset_ == other_key.offset_ && size_ == other_key.size_;
}

uint64_t ObMicroBlockCacheKey::get_tenant_id() const
{
  return extract_tenant_id(table_id_);
}

uint64_t ObMicroBlockCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObMicroBlockCacheKey), 0);
}

int64_t ObMicroBlockCacheKey::size() const
{
  return sizeof(*this);
}

int ObMicroBlockCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(
                 0 == table_id_ || OB_INVALID_ID == table_id_ || !block_id_.is_valid() || offset_ < 0 || size_ <= 0)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block cache key is invalid, ", K(*this), K(ret));
  } else {
    key = new (buf) ObMicroBlockCacheKey(table_id_, block_id_, file_id_, offset_, size_);
  }
  return ret;
}

/**
 * -----------------------------------------------------ObMicroBlockCacheValue--------------------------------------------------
 */
ObMicroBlockCacheValue::ObMicroBlockCacheValue(
    const char* buf, const int64_t size, const char* extra_buf /* = NULL */, const int64_t extra_size /* = 0 */)
    : block_data_(buf, size, extra_buf, extra_size)
{}

ObMicroBlockCacheValue::~ObMicroBlockCacheValue()
{}

int64_t ObMicroBlockCacheValue::size() const
{
  return sizeof(blocksstable::ObMicroBlockCacheValue) + block_data_.total_size();
}

int ObMicroBlockCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  ObMicroBlockCacheValue* pvalue = NULL;

  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(!block_data_.is_valid())) {
    // buffer_ is allowed to be NULL
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block cache value is not valid, ", K(*this), K(ret));
  } else {
    char* new_buf = buf + sizeof(blocksstable::ObMicroBlockCacheValue);
    MEMCPY(new_buf, block_data_.get_buf(), block_data_.get_buf_size());
    if (NULL != block_data_.get_extra_buf() && block_data_.get_extra_size() > 0) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "Unexpected encoding micro block cache", K(ret), K_(block_data));
    } else {
      pvalue = new (buf) ObMicroBlockCacheValue(new_buf, block_data_.get_buf_size());
    }
    value = pvalue;
  }
  return ret;
}

ObMultiBlockIOParam::ObMultiBlockIOParam()
{
  reset();
}

ObMultiBlockIOParam::~ObMultiBlockIOParam()
{}

void ObMultiBlockIOParam::reset()
{
  micro_block_infos_ = NULL;
  start_index_ = -1;
  block_count_ = -1;
}

bool ObMultiBlockIOParam::is_valid() const
{
  return NULL != micro_block_infos_ && start_index_ >= 0 && block_count_ > 0 &&
         micro_block_infos_->count() >= start_index_ + block_count_;
}

int ObMultiBlockIOParam::get_io_range(int64_t& offset, int64_t& size) const
{
  int ret = OB_SUCCESS;
  offset = 0;
  size = 0;
  if (block_count_ > 0) {
    const int64_t end_index = start_index_ + block_count_ - 1;
    offset = micro_block_infos_->at(start_index_).offset_;
    size = micro_block_infos_->at(end_index).offset_ - offset + micro_block_infos_->at(end_index).size_;
  }
  return ret;
}

DEF_TO_STRING(ObMultiBlockIOParam)
{
  int64_t pos = 0;
  if (NULL == micro_block_infos_) {
    J_KV(KP(micro_block_infos_), K_(start_index), K_(block_count));
  } else {
    J_KV("micro_block_infos", *micro_block_infos_, K_(start_index), K_(block_count));
  }
  return pos;
}

void ObMultiBlockIOCtx::reset()
{
  micro_block_infos_ = NULL;
  block_count_ = 0;
}

bool ObMultiBlockIOCtx::is_valid() const
{
  return NULL != micro_block_infos_ && block_count_ > 0;
}

ObMultiBlockIOResult::ObMultiBlockIOResult()
{
  reset();
}

ObMultiBlockIOResult::~ObMultiBlockIOResult()
{}

void ObMultiBlockIOResult::reset()
{
  micro_blocks_ = NULL;
  handles_ = NULL;
  block_count_ = 0;
  ret_code_ = OB_SUCCESS;
}

int ObMultiBlockIOResult::get_block_data(const int64_t index, ObMicroBlockData& block_data) const
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
    STORAGE_LOG(WARN, "micro_block is null", K(ret), "handle validity", handles_[index].is_valid(), K(index));
  } else {
    block_data = micro_blocks_[index]->get_block_data();
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(const uint64_t table_id, const ObMacroBlockCtx& block_ctx, const int64_t offset,
    const int64_t size, const ObQueryFlag& flag, ObStorageFile* pg_file, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  BaseBlockCache* cache = NULL;
  ObIAllocator* allocator = NULL;
  if (OB_UNLIKELY(0 == table_id || OB_INVALID_ID == table_id || !block_ctx.is_valid() || offset < 0 || size <= 0 ||
                  NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(table_id), K(block_ctx), K(offset), K(size), K(pg_file));
  } else if (OB_FAIL(get_cache(cache))) {
    STORAGE_LOG(WARN, "get_cache failed", K(ret));
  } else if (OB_FAIL(get_allocator(allocator))) {
    STORAGE_LOG(WARN, "get_allocator failed", K(ret));
  } else {
    // fill callback
    ObMicroBlockIOCallback callback;
    callback.cache_ = cache;
    callback.put_size_stat_ = this;
    callback.allocator_ = allocator;
    callback.table_id_ = table_id;
    callback.block_id_ = block_ctx.get_macro_block_id();
    callback.file_id_ = pg_file->get_file_id();
    callback.offset_ = offset;
    callback.size_ = size;
    callback.use_block_cache_ = flag.is_use_block_cache();
    // fill read info
    read_info.macro_block_ctx_ = &block_ctx;
    read_info.io_desc_.category_ = flag.is_prewarm() ? PREWARM_IO
                                   : flag.is_large_query() && GCONF._large_query_io_percentage.get_value() > 0
                                       ? LARGE_QUERY_IO
                                       : USER_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    read_info.io_callback_ = &callback;
    common::align_offset_size(offset, size, read_info.offset_, read_info.size_);

    macro_handle.set_file(pg_file);
    if (OB_FAIL(callback.table_handle_.set_table(block_ctx.sstable_))) {
      STORAGE_LOG(WARN, "fail to set table", K(ret));
    } else if (OB_FAIL(pg_file->async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    } else {
      EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, size);
    }
  }
  return ret;
}

int ObIMicroBlockCache::prefetch(const uint64_t table_id, const ObMacroBlockCtx& block_ctx,
    const ObMultiBlockIOParam& io_param, const ObQueryFlag& flag, ObStorageFile* pg_file,
    ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t size = 0;
  BaseBlockCache* cache = NULL;
  ObIAllocator* allocator = NULL;
  if (OB_UNLIKELY(0 == table_id || OB_INVALID_ID == table_id || !block_ctx.is_valid() || !io_param.is_valid() ||
                  NULL == pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(table_id), K(block_ctx), K(io_param), K(pg_file));
  } else if (OB_FAIL(get_cache(cache))) {
    STORAGE_LOG(WARN, "get_cache failed", K(ret));
  } else if (OB_FAIL(get_allocator(allocator))) {
    STORAGE_LOG(WARN, "get_allocator failed", K(ret));
  } else if (OB_FAIL(io_param.get_io_range(offset, size))) {
    STORAGE_LOG(WARN, "get_io_range failed", K(ret));
  } else {
    // fill callback
    ObMultiBlockIOCallback callback;
    callback.cache_ = cache;
    callback.put_size_stat_ = this;
    callback.allocator_ = allocator;
    callback.table_id_ = table_id;
    callback.block_id_ = block_ctx.get_macro_block_id();
    callback.file_id_ = pg_file->get_file_id();
    callback.offset_ = offset;
    callback.size_ = size;
    callback.use_block_cache_ = flag.is_use_block_cache();
    // fill read info
    ObMacroBlockReadInfo read_info;
    read_info.io_callback_ = &callback;
    read_info.io_desc_.category_ = flag.is_prewarm() ? PREWARM_IO
                                   : flag.is_large_query() && GCONF._large_query_io_percentage.get_value() > 0
                                       ? LARGE_QUERY_IO
                                       : USER_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    read_info.macro_block_ctx_ = &block_ctx;
    common::align_offset_size(offset, size, read_info.offset_, read_info.size_);

    macro_handle.set_file(pg_file);
    if (OB_FAIL(callback.set_io_ctx(io_param))) {
      STORAGE_LOG(WARN, "set_io_ctx failed", K(ret), K(io_param));
    } else if (OB_FAIL(callback.table_handle_.set_table(block_ctx.sstable_))) {
      STORAGE_LOG(WARN, "fail to set table", K(ret));
    } else if (OB_FAIL(pg_file->async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    } else {
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT, io_param.block_count_);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, size);
    }

    // io_ctx is not deep copied, reset it
    callback.reset_io_ctx();
  }
  return ret;
}

int ObIMicroBlockCache::get_cache_block(const uint64_t table_id, const MacroBlockId block_id, const int64_t file_id,
    const int64_t offset, const int64_t size, ObMicroBlockBufferHandle& handle)
{
  int ret = OB_SUCCESS;
  BaseBlockCache* cache = NULL;
  if (OB_UNLIKELY(0 == table_id || OB_INVALID_ID == table_id || !block_id.is_valid() ||
                  OB_INVALID_DATA_FILE_ID == file_id || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(cache), K(table_id), K(block_id), K(offset), K(size));
  } else if (OB_FAIL(get_cache(cache))) {
    STORAGE_LOG(WARN, "get_cache failed", K(ret));
  } else {
    ObMicroBlockCacheKey key(table_id, block_id, file_id, offset, size);
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

int ObIMicroBlockCache::load_cache_block(ObMacroBlockReader& reader, const uint64_t table_id,
    const ObMacroBlockCtx& block_ctx, const int64_t offset, const int64_t size, ObStorageFile* storage_file,
    ObMicroBlockData& block_data)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  ObMacroBlockHandle macro_handle;
  ObFullMacroBlockMeta full_meta;

  bool is_compressed = false;
  const bool need_deep_copy = true;
  block_data.reset();
  if (OB_UNLIKELY(0 == table_id || OB_INVALID_ID == table_id || !block_ctx.is_valid() || offset < 0 || size <= 0) ||
      OB_ISNULL(storage_file)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(table_id), K(block_ctx), K(offset), K(size), KP(storage_file), K(ret));
  } else {
    // fill read info
    read_info.macro_block_ctx_ = &block_ctx;
    read_info.io_desc_.category_ = USER_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    read_info.offset_ = offset;
    read_info.size_ = size;
    macro_handle.set_file(storage_file);
    if (OB_FAIL(storage_file->read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async read block, ", K(ret));
    } else if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(
                   macro_handle.get_buffer(), size, MICRO_BLOCK_HEADER_MAGIC))) {
      STORAGE_LOG(ERROR,
          "micro block data is corrupted.",
          K(ret),
          K(table_id),
          K(block_ctx),
          K(offset),
          K(size),
          "buf",
          OB_P(macro_handle.get_buffer()));
    } else if (OB_FAIL(block_ctx.sstable_->get_meta(block_ctx.get_macro_block_id(), full_meta))) {
      STORAGE_LOG(WARN, "Fail to get meta, ", K(block_ctx), K(ret));
    } else if (OB_FAIL(reader.decompress_data(full_meta,
                   macro_handle.get_buffer(),
                   size,
                   block_data.get_buf(),
                   block_data.get_buf_size(),
                   is_compressed,
                   need_deep_copy))) {
      STORAGE_LOG(WARN, "Fail to decompress data, ", K(ret));
    } else {
      EVENT_INC(ObStatEventIds::IO_READ_PREFETCH_MICRO_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_PREFETCH_MICRO_BYTES, size);
    }
  }
  return ret;
}

/**
 * ----------------------------------------------------ObMicroBlockIOCallback------------------------------------------------------
 */
ObIMicroBlockCache::ObIMicroBlockIOCallback::ObIMicroBlockIOCallback()
    : cache_(NULL),
      put_size_stat_(NULL),
      allocator_(NULL),
      io_buffer_(NULL),
      data_buffer_(NULL),
      table_id_(0),
      block_id_(),
      file_id_(0),
      offset_(0),
      size_(0),
      use_block_cache_(true)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIMicroBlockCache::ObIMicroBlockIOCallback::~ObIMicroBlockIOCallback()
{
  if (NULL != allocator_ && NULL != io_buffer_) {
    allocator_->free(io_buffer_);
    io_buffer_ = NULL;
  }
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::alloc_io_buf(char*& io_buf, int64_t& align_size, int64_t& align_offset)
{
  int ret = OB_SUCCESS;
  align_size = 0;
  align_offset = 0;
  common::align_offset_size(offset_, size_, align_offset, align_size);
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the allocator is NULL, ", KP_(allocator), K(ret));
  } else {
    io_buffer_ = (char*)allocator_->alloc(align_size + DIO_READ_ALIGN_SIZE);
    for (int64_t i = 1; OB_ISNULL(io_buffer_) && i <= ALLOC_BUF_RETRY_TIMES; i++) {
      usleep(ALLOC_BUF_RETRY_INTERVAL * i);
      io_buffer_ = (char*)allocator_->alloc(align_size + DIO_READ_ALIGN_SIZE);
    }
    if (OB_NOT_NULL(io_buffer_)) {
      io_buf = reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(io_buffer_), DIO_READ_ALIGN_SIZE));
      data_buffer_ = io_buf + (offset_ - align_offset);
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory", K(ret), K_(offset), K_(size), K(align_offset), K(align_size));
    }
  }

  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::put_cache_and_fetch(const ObFullMacroBlockMeta& meta,
    ObMacroBlockReader& reader, const char* buffer,
    const int64_t offset,  // offset means offset in macro_block
    const int64_t size, const char* payload_buf, const int64_t payload_size, const ObMicroBlockCacheValue*& micro_block,
    common::ObKVCacheHandle& handle)
{
  int ret = OB_SUCCESS;
  ObKVCachePair* kvpair = nullptr;
  ObKVCacheInstHandle inst_handle;
  const bool overwrite = false;
  const ObRecordHeaderV3::ObRecordCommonHeader* common_header =
      reinterpret_cast<const ObRecordHeaderV3::ObRecordCommonHeader*>(buffer);
  ObMicroBlockCacheKey key(table_id_, block_id_, file_id_, offset, size);
  int64_t value_size = sizeof(ObMicroBlockCacheValue) + common_header->data_length_;
  if (OB_UNLIKELY(OB_SUCCESS == (ret = cache_->get(key, micro_block, handle)))) {
    // entry exist, no need to put
  } else if (OB_FAIL(cache_->alloc(extract_tenant_id(table_id_),
                 sizeof(ObMicroBlockCacheKey),
                 value_size,
                 kvpair,
                 handle,
                 inst_handle))) {
    LOG_WARN("failed to alloc cache buf", K(ret));
  } else {
    char* block_buf = reinterpret_cast<char*>(kvpair->value_) + sizeof(ObMicroBlockCacheValue);
    new (kvpair->key_) ObMicroBlockCacheKey(table_id_, block_id_, file_id_, offset, size);
    ObMicroBlockCacheValue* cache_value =
        new (kvpair->value_) ObMicroBlockCacheValue(block_buf, common_header->data_length_);
    if (OB_FAIL(reader.decompress_data_with_prealloc_buf(
            meta.schema_->compressor_, payload_buf, payload_size, block_buf, common_header->data_length_))) {
      LOG_WARN("failed to decompress_data_with_prealloc_buf", K(ret));
    } else {
      micro_block = cache_value;
      if (OB_FAIL(cache_->put_kvpair(inst_handle, kvpair, handle, overwrite))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_WARN("failed to put micro block cache", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, *cache_value);
        if (OB_FAIL(put_size_stat_->add_put_size(put_size))) {
          STORAGE_LOG(WARN, "add_put_size failed", K(ret), K(put_size));
        }
      }
    }
    if (OB_FAIL(ret)) {
      handle.reset();
      micro_block = nullptr;
    }
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::process_block(ObMacroBlockReader* reader, char* buffer,
    const int64_t offset, const int64_t size, const ObMicroBlockCacheValue*& micro_block, ObKVCacheHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  bool is_compressed = false;
  micro_block = NULL;
  int64_t payload_size = 0;
  const char* payload_buf = nullptr;
  ObFullMacroBlockMeta full_meta;
  ObTableHandle table_handle;
  ObSSTable* sstable = nullptr;
  if (NULL == reader || NULL == buffer || offset < 0 || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(reader), KP(buffer), K(offset), K(size));
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "fail to get sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "sstable must not be null", K(ret), K(table_handle_));
  } else if (OB_FAIL(sstable->get_meta(block_id_, full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret), K(block_id_));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret), K(full_meta));
  } else if (OB_FAIL(ObRecordHeaderV3::deserialize_and_check_record(
                 buffer, size, MICRO_BLOCK_HEADER_MAGIC, payload_buf, payload_size))) {
    STORAGE_LOG(ERROR,
        "micro block data is corrupted.",
        K(ret),
        K_(block_id),
        K(offset),
        K(size),
        K_(table_id),
        KP(buffer),
        KP(io_buffer_),
        KP(data_buffer_),
        KP(this));
  } else {
    if (OB_UNLIKELY(!use_block_cache_) ||
        OB_FAIL(put_cache_and_fetch(
            full_meta, *reader, buffer, offset, size, payload_buf, payload_size, micro_block, handle))) {
      if (OB_FAIL(reader->decompress_data(
              full_meta, buffer, size, block_data.get_buf(), block_data.get_buf_size(), is_compressed))) {
        STORAGE_LOG(WARN, "Fail to decompress data, ", K(ret));
      } else {
        ObMicroBlockCacheValue value(block_data.get_buf(), block_data.get_buf_size());
        char* buf = NULL;
        const int64_t buf_len = value.size();
        handle.reset();
        micro_block = NULL;
        ObIKVCacheValue* value_copy = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to allocate value", K(ret), K(buf_len));
        } else if (OB_FAIL(value.deep_copy(buf, buf_len, value_copy))) {
          STORAGE_LOG(WARN, "failed to deep copy value", K(ret));
          allocator_->free(buf);
        } else {
          micro_block = static_cast<const ObMicroBlockCacheValue*>(value_copy);
        }
      }
    }
  }
  return ret;
}

int ObIMicroBlockCache::ObIMicroBlockIOCallback::assign(const ObIMicroBlockIOCallback& other)
{
  int ret = OB_SUCCESS;
  cache_ = other.cache_;
  put_size_stat_ = other.put_size_stat_;
  allocator_ = other.allocator_;
  io_buffer_ = other.io_buffer_;
  data_buffer_ = other.data_buffer_;
  table_id_ = other.table_id_;
  block_id_ = other.block_id_;
  file_id_ = other.file_id_;
  offset_ = other.offset_;
  size_ = other.size_;
  use_block_cache_ = other.use_block_cache_;
  if (OB_FAIL(table_handle_.assign(other.table_handle_))) {
    STORAGE_LOG(WARN, "fail to assign table handle", K(ret));
  }
  return ret;
}

ObIMicroBlockCache::ObMicroBlockIOCallback::ObMicroBlockIOCallback()
    : ObIMicroBlockIOCallback(), micro_block_(NULL), handle_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIMicroBlockCache::ObMicroBlockIOCallback::~ObMicroBlockIOCallback()
{
  if (NULL != allocator_ && !handle_.is_valid() && NULL != micro_block_) {
    allocator_->free(const_cast<ObMicroBlockCacheValue*>(micro_block_));
    micro_block_ = NULL;
  }
}

int64_t ObIMicroBlockCache::ObMicroBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObIMicroBlockCache::ObMicroBlockIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObMacroBlockReader* reader = NULL;
    ObMacroBlockMetaHandle meta_handle;
    if (OB_UNLIKELY(NULL == (reader = GET_TSI_MULT(ObMacroBlockReader, 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(process_block(reader, data_buffer_, offset_, size_, micro_block_, handle_))) {
      STORAGE_LOG(WARN, "process_block failed", K(ret));
    }
  }

  if (NULL != allocator_ && NULL != io_buffer_) {
    allocator_->free(io_buffer_);
    io_buffer_ = NULL;
  }
  return ret;
}

int ObIMicroBlockCache::ObMicroBlockIOCallback::inner_deep_copy(
    char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(NULL == cache_) || OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObMicroBlockIOCallback* pcallback = new (buf) ObMicroBlockIOCallback();
    if (OB_FAIL(pcallback->assign(*this))) {
      STORAGE_LOG(WARN, "fail to assign callback", K(ret));
    } else {
      pcallback->micro_block_ = micro_block_;
      pcallback->handle_ = handle_;
      callback = pcallback;
    }
  }
  return ret;
}

const char* ObIMicroBlockCache::ObMicroBlockIOCallback::get_data()
{
  const char* data = NULL;
  if (NULL != micro_block_) {
    data = reinterpret_cast<const char*>(&(micro_block_->get_block_data()));
  }
  return data;
}

ObIMicroBlockCache::ObMultiBlockIOCallback::ObMultiBlockIOCallback()
    : ObIMicroBlockIOCallback(), io_ctx_(), io_result_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObIMicroBlockCache::ObMultiBlockIOCallback::~ObMultiBlockIOCallback()
{
  if (NULL != allocator_) {
    if (NULL != io_ctx_.micro_block_infos_) {
      allocator_->free(io_ctx_.micro_block_infos_);
      io_ctx_.micro_block_infos_ = NULL;
    }

    free_result();
  }
}

int64_t ObIMicroBlockCache::ObMultiBlockIOCallback::size() const
{
  return sizeof(*this);
}

int ObIMicroBlockCache::ObMultiBlockIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid micro block cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObMacroBlockReader* reader = NULL;
    if (OB_UNLIKELY(NULL == (reader = GET_TSI_MULT(ObMacroBlockReader, 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate ObMacroBlockReader, ", K(ret));
    } else if (OB_FAIL(alloc_result())) {
      STORAGE_LOG(WARN, "alloc_result failed", K(ret));
    } else {
      const int64_t block_count = io_ctx_.block_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
        const int64_t data_size = io_ctx_.micro_block_infos_[i].size_;
        const int64_t data_offset = io_ctx_.micro_block_infos_[i].offset_ - offset_;
        if (OB_FAIL(process_block(reader,
                data_buffer_ + data_offset,
                offset_ + data_offset,
                data_size,
                io_result_.micro_blocks_[i],
                io_result_.handles_[i]))) {
          STORAGE_LOG(WARN, "process_block failed", K(ret));
        }
      }
    }
  }

  if (NULL != allocator_ && NULL != io_buffer_) {
    allocator_->free(io_buffer_);
    io_buffer_ = NULL;
  }

  if (OB_FAIL(ret)) {
    io_result_.ret_code_ = ret;
  }
  return ret;
}

int ObIMicroBlockCache::ObMultiBlockIOCallback::inner_deep_copy(
    char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  callback = nullptr;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(NULL == cache_) || OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The micro block io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObMultiBlockIOCallback* pcallback = new (buf) ObMultiBlockIOCallback();
    pcallback->io_ctx_.reset();
    if (OB_FAIL(pcallback->assign(*this))) {
      STORAGE_LOG(WARN, "fail to assign callback", K(ret));
    } else if (OB_FAIL(pcallback->deep_copy_ctx(io_ctx_))) {
      STORAGE_LOG(WARN, "deep_copy_ctx failed", K(ret));
    } else {
      pcallback->io_result_ = io_result_;
      callback = pcallback;
    }
  }
  return ret;
}

const char* ObIMicroBlockCache::ObMultiBlockIOCallback::get_data()
{
  const char* data = NULL;
  data = reinterpret_cast<const char*>(&io_result_);
  return data;
}

int ObIMicroBlockCache::ObMultiBlockIOCallback::set_io_ctx(const ObMultiBlockIOParam& io_param)
{
  int ret = OB_SUCCESS;
  if (!io_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid io_param", K(ret), K(io_param));
  } else {
    io_ctx_.micro_block_infos_ = &io_param.micro_block_infos_->at(io_param.start_index_);
    io_ctx_.block_count_ = io_param.block_count_;
  }
  return ret;
}

int ObIMicroBlockCache::ObMultiBlockIOCallback::deep_copy_ctx(const ObMultiBlockIOCtx& io_ctx)
{
  int ret = OB_SUCCESS;
  if (!io_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid io_ctx", K(ret), K(io_ctx));
  } else if (NULL == allocator_) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "allocator_ is null", K(ret), KP(allocator_));
  } else {
    void* ptr = NULL;
    int64_t alloc_size = sizeof(ObMicroBlockInfo) * io_ctx.block_count_;
    if (NULL == (ptr = allocator_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc memory failed", K(ret), K(alloc_size));
    } else {
      io_ctx_.micro_block_infos_ = reinterpret_cast<ObMicroBlockInfo*>(ptr);
      MEMCPY(io_ctx_.micro_block_infos_, io_ctx.micro_block_infos_, alloc_size);
    }

    if (OB_SUCC(ret)) {
      io_ctx_.block_count_ = io_ctx.block_count_;
    }
  }
  return ret;
}

int ObIMicroBlockCache::ObMultiBlockIOCallback::alloc_result()
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  const int64_t block_count = io_ctx_.block_count_;
  if (NULL == (ptr = allocator_->alloc(sizeof(ObMicroBlockCacheValue*) * block_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc failed", K(ret));
  } else {
    io_result_.micro_blocks_ = reinterpret_cast<const ObMicroBlockCacheValue**>(ptr);
    MEMSET(io_result_.micro_blocks_, 0, sizeof(ObMicroBlockCacheValue*) * block_count);
  }

  if (OB_SUCC(ret)) {
    if (NULL == (ptr = allocator_->alloc(sizeof(ObKVCacheHandle) * block_count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc failed", K(ret));
    } else {
      io_result_.handles_ = new (ptr) ObKVCacheHandle[block_count];
      io_result_.block_count_ = block_count;
    }
  }
  return ret;
}

void ObIMicroBlockCache::ObMultiBlockIOCallback::free_result()
{
  if (NULL != allocator_) {
    if (NULL != io_result_.micro_blocks_) {
      if (NULL != io_result_.handles_) {
        for (int64_t i = 0; i < io_result_.block_count_; ++i) {
          if (!io_result_.handles_[i].is_valid() && NULL != io_result_.micro_blocks_[i]) {
            allocator_->free(const_cast<ObMicroBlockCacheValue*>(io_result_.micro_blocks_[i]));
          }
        }
      }
      allocator_->free(io_result_.micro_blocks_);
      io_result_.micro_blocks_ = NULL;
    }
    if (NULL != io_result_.handles_) {
      for (int64_t i = 0; i < io_result_.block_count_; ++i) {
        io_result_.handles_[i].~ObKVCacheHandle();
      }
      allocator_->free(io_result_.handles_);
      io_result_.handles_ = NULL;
      io_result_.block_count_ = 0;
    }
  }
}

/**
 * -----------------------------------------------------ObMicroBlockCache--------------------------------------------------
 */
int ObMicroBlockCache::init(const char* cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 4 * 1024 * 1024 * 1024LL;
  if (OB_SUCCESS !=
      (ret = common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::init(cache_name, priority))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_SSTABLE_MICRO_BLOCK_ALLOCATOR);
  }
  return ret;
}

void ObMicroBlockCache::destroy()
{
  common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>::destroy();
  allocator_.destroy();
}

int ObMicroBlockCache::get_cache(BaseBlockCache*& cache)
{
  int ret = OB_SUCCESS;
  cache = this;
  return ret;
}

int ObMicroBlockCache::get_allocator(ObIAllocator*& allocator)
{
  int ret = OB_SUCCESS;
  allocator = &allocator_;
  return ret;
}

int ObMicroBlockCache::add_put_size(const int64_t put_size)
{
  UNUSED(put_size);
  return OB_SUCCESS;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
