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

#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/ob_smart_var.h"
#include "ob_tmp_file_cache.h"
#include "ob_tmp_file.h"
#include "ob_tmp_file_store.h"
#include "ob_store_file.h"

using namespace oceanbase::storage;

namespace oceanbase {
namespace blocksstable {

ObTmpPageCacheKey::ObTmpPageCacheKey() : block_id_(-1), page_id_(-1), tenant_id_(0)
{}

ObTmpPageCacheKey::ObTmpPageCacheKey(const int64_t block_id, const int64_t page_id, const uint64_t tenant_id)
    : block_id_(block_id), page_id_(page_id), tenant_id_(tenant_id)
{}

ObTmpPageCacheKey::~ObTmpPageCacheKey()
{}

bool ObTmpPageCacheKey::operator==(const ObIKVCacheKey& other) const
{
  const ObTmpPageCacheKey& other_key = reinterpret_cast<const ObTmpPageCacheKey&>(other);
  return block_id_ == other_key.block_id_ && page_id_ == other_key.page_id_ && tenant_id_ == other_key.tenant_id_;
}

uint64_t ObTmpPageCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObTmpPageCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObTmpPageCacheKey), 0);
}

int64_t ObTmpPageCacheKey::size() const
{
  return sizeof(*this);
}

int ObTmpPageCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp page cache key, ", K(*this), K(ret));
  } else {
    key = new (buf) ObTmpPageCacheKey(block_id_, page_id_, tenant_id_);
  }
  return ret;
}

bool ObTmpPageCacheKey::is_valid() const
{
  return OB_LIKELY(block_id_ > 0 && page_id_ >= 0 && tenant_id_ > 0 && size() > 0);
}

ObTmpPageCacheValue::ObTmpPageCacheValue(char* buf) : buf_(buf), size_(ObTmpMacroBlock::get_default_page_size())
{}

ObTmpPageCacheValue::~ObTmpPageCacheValue()
{}

int64_t ObTmpPageCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObTmpPageCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp page cache value", K(ret));
  } else {
    ObTmpPageCacheValue* pblk_value = new (buf) ObTmpPageCacheValue(buf + sizeof(*this));
    MEMCPY(buf + sizeof(*this), buf_, size() - sizeof(*this));
    pblk_value->size_ = size_;
    value = pblk_value;
  }
  return ret;
}

int ObTmpPageCache::prefetch(const ObTmpPageCacheKey& key, const ObTmpBlockIOInfo& info, ObTmpFileIOHandle& handle,
    ObMacroBlockHandle& mb_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(key), K(handle));
  } else {
    // fill the callback
    ObTmpPageIOCallback callback;
    callback.cache_ = this;
    callback.offset_ = info.offset_;
    callback.buf_size_ = info.size_;
    callback.allocator_ = &allocator_;
    callback.key_ = key;
    if (OB_FAIL(read_io(info, callback, mb_handle))) {
      if (mb_handle.get_io_handle().is_empty()) {
        // TODO: After the continuous IO has been optimized, this should
        // not happen.
        if (OB_FAIL(handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
          STORAGE_LOG(WARN, "fail to wait tmp page io", K(ret));
        } else if (OB_FAIL(read_io(info, callback, mb_handle))) {
          STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret));
        }
      } else {
        STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpPageCache::prefetch(const ObTmpBlockIOInfo& info, const common::ObIArray<ObTmpPageIOInfo>& page_io_infos,
    ObTmpFileIOHandle& handle, ObMacroBlockHandle& mb_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_io_infos.count() <= 0 || !handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(page_io_infos.count()), K(info), K(handle));
  } else {
    ObTmpMultiPageIOCallback callback;
    callback.cache_ = this;
    callback.offset_ = info.offset_;
    callback.buf_size_ = info.size_;
    callback.allocator_ = &allocator_;
    void* buf = allocator_.alloc(sizeof(common::ObSEArray<ObTmpPageIOInfo, 255>));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc a buf", K(ret), K(info));
    } else {
      callback.page_io_infos_ = new (buf) common::ObSEArray<ObTmpPageIOInfo, 255>();
      callback.page_io_infos_->assign(page_io_infos);
      if (OB_FAIL(read_io(info, callback, mb_handle))) {
        if (mb_handle.get_io_handle().is_empty()) {
          // TODO: After the continuous IO has been optimized, this should
          // not happen.
          if (OB_FAIL(handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
            STORAGE_LOG(WARN, "fail to wait tmp page io", K(ret));
          } else if (OB_FAIL(read_io(info, callback, mb_handle))) {
            STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret));
          }
        } else {
          STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTmpPageCache::get_cache_page(const ObTmpPageCacheKey& key, ObTmpPageValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else {
    const ObTmpPageCacheValue* value = NULL;
    if (OB_FAIL(get(key, value, handle.handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        STORAGE_LOG(WARN, "fail to get key from page cache", K(ret));
      }
      EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_MISS);
    } else {
      if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", K(ret));
      } else {
        handle.value_ = const_cast<ObTmpPageCacheValue*>(value);
        EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_HIT);
      }
    }
  }
  return ret;
}

ObTmpPageCache::ObITmpPageIOCallback::ObITmpPageIOCallback()
    : cache_(NULL), allocator_(NULL), offset_(0), buf_size_(0), io_buf_(NULL), io_buf_size_(0), data_buf_(NULL)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObITmpPageIOCallback::~ObITmpPageIOCallback()
{
  if (NULL != allocator_ && NULL != io_buf_) {
    allocator_->free(io_buf_);
    io_buf_ = NULL;
    data_buf_ = NULL;
    allocator_ = NULL;
  }
}

int ObTmpPageCache::ObITmpPageIOCallback::alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
{
  int ret = OB_SUCCESS;
  io_buf_size = 0;
  aligned_offset = 0;
  common::align_offset_size(offset_, buf_size_, aligned_offset, io_buf_size);
  io_buf_size_ = io_buf_size + DIO_READ_ALIGN_SIZE;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid data, the allocator is NULL, ", K(ret));
  } else if (OB_UNLIKELY(NULL == (io_buf_ = (char*)(allocator_->alloc(io_buf_size_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else {
    io_buf = upper_align_buf(io_buf_, DIO_READ_ALIGN_SIZE);
    data_buf_ = io_buf + (offset_ - aligned_offset);
  }
  return ret;
}

int ObTmpPageCache::ObITmpPageIOCallback::process_page(const ObTmpPageCacheKey& key, const ObTmpPageCacheValue& value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(cache_->put(key, value, true /*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put row to row cache", K(ret), K(key), K(value));
  }
  return ret;
}

ObTmpPageCache::ObTmpPageIOCallback::ObTmpPageIOCallback() : key_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObTmpPageIOCallback::~ObTmpPageIOCallback()
{}

int ObTmpPageCache::ObTmpPageIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObTmpPageCacheValue value(const_cast<char*>(get_data()));
    if (OB_FAIL(process_page(key_, value))) {
      STORAGE_LOG(WARN, "fail to process tmp page cache in callback", K(ret));
    }
  }
  if (OB_FAIL(ret) && NULL != allocator_ && NULL != io_buf_) {
    allocator_->free(io_buf_);
    io_buf_ = NULL;
    data_buf_ = NULL;
  }
  return ret;
}

int64_t ObTmpPageCache::ObTmpPageIOCallback::size() const
{
  return sizeof(*this);
}

int ObTmpPageCache::ObTmpPageIOCallback::inner_deep_copy(
    char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(NULL == cache_) || OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The tmp page io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObTmpPageIOCallback* pcallback = new (buf) ObTmpPageIOCallback();
    *pcallback = *this;
    callback = pcallback;
  }
  return ret;
}

const char* ObTmpPageCache::ObTmpPageIOCallback::get_data()
{
  return data_buf_;
}

ObTmpPageCache::ObTmpMultiPageIOCallback::ObTmpMultiPageIOCallback() : page_io_infos_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObTmpMultiPageIOCallback::~ObTmpMultiPageIOCallback()
{}

int ObTmpPageCache::ObTmpMultiPageIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    char* buf = const_cast<char*>(get_data());
    for (int32_t i = 0; OB_SUCC(ret) && i < page_io_infos_->count(); i++) {
      int64_t offset = page_io_infos_->at(i).key_.get_page_id() * ObTmpMacroBlock::get_default_page_size() - offset_;
      ObTmpPageCacheValue value(buf + offset);
      if (OB_FAIL(process_page(page_io_infos_->at(i).key_, value))) {
        STORAGE_LOG(WARN, "fail to process tmp page cache in callback", K(ret));
      }
    }
    page_io_infos_->reset();
    allocator_->free(page_io_infos_);
  }
  if (OB_FAIL(ret) && NULL != allocator_ && NULL != io_buf_) {
    allocator_->free(io_buf_);
    io_buf_ = NULL;
    data_buf_ = NULL;
  }
  return ret;
}

int64_t ObTmpPageCache::ObTmpMultiPageIOCallback::size() const
{
  return sizeof(*this);
}

int ObTmpPageCache::ObTmpMultiPageIOCallback::inner_deep_copy(
    char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(NULL == cache_) || OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The tmp page io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObTmpMultiPageIOCallback* pcallback = new (buf) ObTmpMultiPageIOCallback();
    *pcallback = *this;
    callback = pcallback;
  }
  return ret;
}

const char* ObTmpPageCache::ObTmpMultiPageIOCallback::get_data()
{
  return data_buf_;
}

int ObTmpPageCache::read_io(const ObTmpBlockIOInfo& io_info, ObITmpPageIOCallback& callback, ObMacroBlockHandle& handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_MACRO_FILE);
  ObStoreFileCtx file_ctx(allocator);
  ObMacroBlockCtx macro_block_ctx;  // TODO(): fix it for ofs later
  file_ctx.file_system_type_ = STORE_FILE_SYSTEM_LOCAL;
  macro_block_ctx.file_ctx_ = &file_ctx;
  // fill the read info
  ObMacroBlockReadInfo read_info;
  read_info.io_desc_ = io_info.io_desc_;
  macro_block_ctx.sstable_block_id_.macro_block_id_ = io_info.macro_block_id_;
  read_info.macro_block_ctx_ = &macro_block_ctx;
  read_info.io_callback_ = &callback;
  common::align_offset_size(io_info.offset_, io_info.size_, read_info.offset_, read_info.size_);
  handle.set_file(file_handle_.get_storage_file());
  if (OB_FAIL(file_handle_.get_storage_file()->async_read_block(read_info, handle))) {
    STORAGE_LOG(WARN, "fail to async read block", K(ret));
  }
  return ret;
}

ObTmpPageCache& ObTmpPageCache::get_instance()
{
  static ObTmpPageCache instance;
  return instance;
}

ObTmpPageCache::ObTmpPageCache() : file_handle_()
{}

ObTmpPageCache::~ObTmpPageCache()
{}

int ObTmpPageCache::init(const char* cache_name, const int64_t priority, const ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 4 * 1024 * 1024 * 1024LL;
  if (OB_FAIL((common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>::init(cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  } else if (OB_FAIL(allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    STORAGE_LOG(WARN, "Fail to assign file handle", K(ret), K(file_handle));
  } else {
    allocator_.set_label(ObModIds::OB_TMP_PAGE_CACHE);
  }
  return ret;
}

void ObTmpPageCache::destroy()
{
  common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>::destroy();
  allocator_.destroy();
  file_handle_.reset();
}

int ObTmpPageCache::put_page(const ObTmpPageCacheKey& key, const ObTmpPageCacheValue& value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put page to page cache", K(ret), K(key), K(value));
  }
  return ret;
}

int ObTmpPageCache::get_page(const ObTmpPageCacheKey& key, ObTmpPageValueHandle& handle)
{
  int ret = OB_SUCCESS;
  const ObTmpPageCacheValue* value = NULL;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to get key from page cache", K(ret), K(key));
    } else {
      EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_MISS);
    }
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", K(ret));
    } else {
      handle.value_ = const_cast<ObTmpPageCacheValue*>(value);
      EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_HIT);
    }
  }
  return ret;
}

ObTmpBlockCacheKey::ObTmpBlockCacheKey(const int64_t block_id, const uint64_t tenant_id)
    : block_id_(block_id), tenant_id_(tenant_id)
{}

bool ObTmpBlockCacheKey::operator==(const ObIKVCacheKey& other) const
{
  const ObTmpBlockCacheKey& other_key = reinterpret_cast<const ObTmpBlockCacheKey&>(other);
  return block_id_ == other_key.block_id_ && tenant_id_ == other_key.tenant_id_;
}

uint64_t ObTmpBlockCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObTmpBlockCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObTmpBlockCacheKey), 0);
}

int64_t ObTmpBlockCacheKey::size() const
{
  return sizeof(*this);
}

int ObTmpBlockCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp block cache key, ", K(*this), K(ret));
  } else {
    key = new (buf) ObTmpBlockCacheKey(block_id_, tenant_id_);
  }
  return ret;
}

ObTmpBlockCacheValue::ObTmpBlockCacheValue(char* buf) : buf_(buf), size_(OB_TMP_FILE_STORE.get_block_size())
{}

int64_t ObTmpBlockCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObTmpBlockCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp block cache value", K(ret));
  } else {
    ObTmpBlockCacheValue* pblk_value = new (buf) ObTmpBlockCacheValue(buf + sizeof(*this));
    MEMCPY(buf + sizeof(*this), buf_, size() - sizeof(*this));
    pblk_value->size_ = size_;
    value = pblk_value;
  }
  return ret;
}

ObTmpBlockCache& ObTmpBlockCache::get_instance()
{
  static ObTmpBlockCache instance;
  return instance;
}

int ObTmpBlockCache::init(const char* cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cache_name) || OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(cache_name), K(priority));
  } else if (OB_FAIL((common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue>::init(cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  }
  return ret;
}

int ObTmpBlockCache::get_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle)
{
  int ret = OB_SUCCESS;
  const ObTmpBlockCacheValue* value = NULL;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to get key-value from block cache", K(ret));
    }
    EVENT_INC(ObStatEventIds::TMP_BLOCK_CACHE_MISS);
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", K(ret));
    } else {
      handle.value_ = const_cast<ObTmpBlockCacheValue*>(value);
      EVENT_INC(ObStatEventIds::TMP_BLOCK_CACHE_HIT);
    }
  }
  return ret;
}

int ObTmpBlockCache::alloc_buf(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc(key.get_tenant_id(),
          key.size(),
          sizeof(ObTmpBlockCacheValue) + OB_TMP_FILE_STORE.get_block_size(),
          handle.kvpair_,
          handle.handle_,
          handle.inst_handle_))) {
    STORAGE_LOG(WARN, "failed to alloc kvcache buf", K(ret));
  } else if (OB_FAIL(key.deep_copy(reinterpret_cast<char*>(handle.kvpair_->key_), key.size(), handle.kvpair_->key_))) {
    STORAGE_LOG(WARN, "failed to deep copy key", K(ret), K(key));
  } else {
    char* buf = reinterpret_cast<char*>(handle.kvpair_->value_);
    handle.value_ = new (buf) ObTmpBlockCacheValue(buf + sizeof(ObTmpBlockCacheValue));
  }
  return ret;
}

int ObTmpBlockCache::put_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(put_kvpair(handle.inst_handle_, handle.kvpair_, handle.handle_, true /*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put tmp block to block cache", K(ret));
  } else {
    handle.reset();
  }
  return ret;
}

void ObTmpBlockCache::destory()
{
  common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue>::destroy();
}

ObTmpTenantMemBlockManager::ObTmpTenantMemBlockManager()
    : write_handles_(),
      t_mblk_map_(),
      dir_to_blk_map_(),
      mblk_page_nums_(OB_FILE_SYSTEM.get_macro_block_size() / ObTmpMacroBlock::get_default_page_size() - 1),
      free_page_nums_(0),
      blk_nums_threshold_(0),
      block_cache_(NULL),
      allocator_(NULL),
      file_handle_(),
      tenant_id_(0),
      block_write_ctx_(),
      last_access_tenant_config_ts_(0),
      last_tenant_mem_block_num_(1),
      is_inited_(false)
{}

ObTmpTenantMemBlockManager::~ObTmpTenantMemBlockManager()
{}

int ObTmpTenantMemBlockManager::init(const uint64_t tenant_id, common::ObIAllocator& allocator,
    const ObStorageFileHandle& file_handle, double blk_nums_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpBlockCache has been inited", K(ret));
  } else if (OB_UNLIKELY(blk_nums_threshold <= 0) || OB_UNLIKELY(blk_nums_threshold > 1) ||
             OB_UNLIKELY(!file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(blk_nums_threshold), K(file_handle));
  } else if (OB_FAIL(t_mblk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_BLOCK_MAP))) {
    STORAGE_LOG(WARN, "Fail to create allocating block map, ", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_MAP))) {
    STORAGE_LOG(WARN, "Fail to create tmp dir map, ", K(ret));
  } else if (OB_ISNULL(block_cache_ = &ObTmpBlockCache::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get the block cache", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, block_write_ctx_.file_ctx_))) {
    STORAGE_LOG(WARN, "failed to init write ctx", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret), K(file_handle));
  } else if (!block_write_ctx_.file_handle_.is_valid() && OB_FAIL(block_write_ctx_.file_handle_.assign(file_handle))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret), K(file_handle));
  } else {
    blk_nums_threshold_ = blk_nums_threshold;
    tenant_id_ = tenant_id;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTmpTenantMemBlockManager::destroy()
{
  int ret = OB_SUCCESS;
  TmpMacroBlockMap::iterator iter;
  ObTmpMacroBlock* tmp = NULL;
  if (is_inited_ && OB_FAIL(wait_write_io_finish())) {
    ObMacroBlockHandle* mb_handle = NULL;
    while (write_handles_.count() > 0) {
      if (OB_FAIL(write_handles_.pop_back(mb_handle))) {
        STORAGE_LOG(WARN, "fail to pop write handle", K(ret));
      }
    }
  }
  write_handles_.reset();
  for (iter = t_mblk_map_.begin(); iter != t_mblk_map_.end(); ++iter) {
    tmp = iter->second;
    if (!tmp->is_disked()) {
      if (OB_FAIL(tmp->give_back_buf_into_cache())) {
        STORAGE_LOG(WARN, "fail to put block", K(ret));
      }
    }
  }
  blk_nums_threshold_ = 0;
  free_page_nums_ = 0;
  t_mblk_map_.destroy();
  dir_to_blk_map_.destroy();
  if (NULL != block_cache_) {
    block_cache_ = NULL;
  }
  file_handle_.reset();
  allocator_ = NULL;
  block_write_ctx_.reset();
  is_inited_ = false;
}

int ObTmpTenantMemBlockManager::get_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle)
{
  return block_cache_->get_block(key, handle);
}

int ObTmpTenantMemBlockManager::alloc_buf(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle)
{
  return block_cache_->alloc_buf(key, handle);
}

int ObTmpTenantMemBlockManager::try_wash(const uint64_t tenant_id, common::ObIArray<ObTmpMacroBlock*>& free_blocks)
{
  int ret = OB_SUCCESS;
  const int64_t count = t_mblk_map_.size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (get_tenant_mem_block_num() <= count) {
    int64_t wash_nums = 1;
    if (OB_FAIL(wash(tenant_id, std::max(wash_nums, count - get_tenant_mem_block_num() + 1), free_blocks))) {
      STORAGE_LOG(WARN, "cannot wash a tmp macro block", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::free_macro_block(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (block_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(t_mblk_map_.erase_refactored(block_id))) {
    STORAGE_LOG(WARN, "fail to erase tmp macro block", K(ret));
  }
  return ret;
}

int ObTmpTenantMemBlockManager::alloc_extent(const int64_t dir_id, const uint64_t tenant_id, const int64_t size,
    ObTmpFileExtent& extent, common::ObIArray<ObTmpMacroBlock*>& free_blocks)
{
  int ret = OB_SUCCESS;
  int64_t block_id = -1;
  int64_t page_nums = std::ceil(size * 1.0 / ObTmpMacroBlock::get_default_page_size());
  ObTmpMacroBlock* t_mblk = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.get_refactored(dir_id, block_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(get_macro_block(dir_id, tenant_id, page_nums, t_mblk, free_blocks))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to get macro block", K(ret));
        }
      }
    }
  } else if (OB_FAIL(t_mblk_map_.get_refactored(block_id, t_mblk))) {
    STORAGE_LOG(INFO, "the tmp macro block has been washed", K(ret), K(block_id));
    if (OB_FAIL(get_macro_block(dir_id, tenant_id, page_nums, t_mblk, free_blocks))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get macro block", K(ret));
      }
    } else if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1))) {
      STORAGE_LOG(WARN, "fail to set dir_to_blk_map", K(ret));
    }
  } else if (t_mblk->get_max_cont_page_nums() < page_nums || t_mblk->get_tenant_id() != tenant_id) {
    if (OB_FAIL(get_macro_block(dir_id, tenant_id, page_nums, t_mblk, free_blocks))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get macro block", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(t_mblk->alloc(page_nums, extent))) {
      STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
    } else if (OB_FAIL(refresh_dir_to_blk_map(t_mblk->get_dir_id(), t_mblk))) {
      STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
    } else {
      free_page_nums_ -= extent.get_page_nums();
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::free_extent(const int64_t free_page_nums, const ObTmpMacroBlock* t_mblk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (free_page_nums < 0 || free_page_nums > mblk_page_nums_ || NULL == t_mblk) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(free_page_nums), K(*t_mblk));
  } else if (OB_FAIL(refresh_dir_to_blk_map(t_mblk->get_dir_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
  } else {
    free_page_nums_ += free_page_nums;
  }
  return ret;
}

int ObTmpTenantMemBlockManager::get_macro_block(const int64_t dir_id, const uint64_t tenant_id, const int64_t page_nums,
    ObTmpMacroBlock*& t_mblk, common::ObIArray<ObTmpMacroBlock*>& free_blocks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else {
    bool is_found = false;
    TmpMacroBlockMap::iterator iter;
    for (iter = t_mblk_map_.begin(); !is_found && iter != t_mblk_map_.end(); ++iter) {
      if (tenant_id != iter->second->get_tenant_id() || dir_id != iter->second->get_dir_id()) {
        continue;
      } else {
        if (iter->second->get_max_cont_page_nums() < page_nums) {
          continue;
        } else {
          t_mblk = iter->second;
          is_found = true;
        }
      }
    }
    if (!is_found) {
      const int64_t count = t_mblk_map_.size();
      if (OB_UNLIKELY(t_mblk_map_.size() == 0)) {
        // nothing to do.
      } else if (get_tenant_mem_block_num() <= count ||
                 blk_nums_threshold_ > (free_page_nums_ * 1.0) / (t_mblk_map_.size() * mblk_page_nums_)) {
        int64_t wash_nums = 1;
        if (OB_FAIL(wash(tenant_id, std::max(wash_nums, count - get_tenant_mem_block_num() + 1), free_blocks))) {
          STORAGE_LOG(WARN, "cannot wash a tmp macro block", K(ret), K(dir_id), K(tenant_id));
        }
      }
      if (OB_SUCC(ret)) {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::wash(
    const uint64_t tenant_id, int64_t block_nums, common::ObIArray<ObTmpMacroBlock*>& free_blocks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(wait_write_io_finish())) {
    STORAGE_LOG(WARN, "fail to wait previous write io", K(ret));
  } else {
    while (OB_SUCC(ret) && block_nums--) {
      int64_t count = t_mblk_map_.size();
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(count < get_tenant_mem_block_num())) {
          STORAGE_LOG(WARN, "Tenant memory has not been used up, not need to wash ", K(ret), K(count));
        } else {
          TmpMacroBlockMap::iterator iter;
          ObTmpMacroBlock* wash_block = NULL;
          for (iter = t_mblk_map_.begin(); count > 0 && iter != t_mblk_map_.end(); ++iter) {
            if (iter->second->get_tenant_id() == tenant_id) {
              if (!iter->second->is_washing()) {
                if (NULL == wash_block || wash_block->get_free_page_nums() > iter->second->get_free_page_nums()) {
                  if (NULL != wash_block) {
                    wash_block->set_washing_status(false);
                  }
                  wash_block = iter->second;
                  wash_block->set_washing_status(true);
                }
              }
              count--;
            }
          }
          if (NULL != wash_block && wash_block->is_inited() && !wash_block->is_disked()) {
            bool is_empty = false;
            if (OB_FAIL(wash_with_no_wait(tenant_id, wash_block, is_empty))) {
              STORAGE_LOG(WARN, "fail to wash", K(ret), K(tenant_id), K(*wash_block));
            } else if (is_empty && OB_FAIL(free_blocks.push_back(wash_block))) {
              STORAGE_LOG(WARN, "fail to push back to free_blocks", K(ret), K(tenant_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::add_macro_block(const uint64_t tenant_id, ObTmpMacroBlock*& t_mblk)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  int64_t count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(t_mblk_map_.set_refactored(t_mblk->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to set tmp macro block map", K(ret), K(t_mblk));
  } else {
    free_page_nums_ += mblk_page_nums_;
  }
  return ret;
}

int ObTmpTenantMemBlockManager::refresh_dir_to_blk_map(const int64_t dir_id, const ObTmpMacroBlock* t_mblk)
{
  int ret = OB_SUCCESS;
  int64_t block_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.get_refactored(dir_id, block_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to set dir_to_blk_map_", K(ret));
      }
    }
  } else {
    ObTmpMacroBlock* dir_mblk = NULL;
    if (OB_FAIL(t_mblk_map_.get_refactored(block_id, dir_mblk))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1);
        STORAGE_LOG(INFO, "the tmp macro block has been removed or washed", K(ret), K(block_id));
      } else {
        STORAGE_LOG(WARN, "fail to get block", K(ret), K(block_id));
      }
    } else if (dir_mblk->get_max_cont_page_nums() < t_mblk->get_max_cont_page_nums()) {
      ret = dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1);
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::wash(const uint64_t tenant_id, ObTmpMacroBlock* wash_block, bool& is_empty)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(wait_write_io_finish())) {
    STORAGE_LOG(WARN, "fail to wait previous write io", K(ret));
  } else if (wash_block->is_disked()) {
    // nothing to do
  } else if (OB_FAIL(wash_with_no_wait(tenant_id, wash_block, is_empty))) {
    STORAGE_LOG(WARN, "fail to wash", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTmpTenantMemBlockManager::wash_with_no_wait(const uint64_t tenant_id, ObTmpMacroBlock* wash_block, bool& is_empty)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  // close all of extents in this block.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (NULL == wash_block) {
    STORAGE_LOG(WARN, "The washing block is null", K(ret));
  } else {
    int64_t free_page_nums = wash_block->get_free_page_nums();
    bool is_all_close = false;
    if (OB_FAIL(wash_block->close(is_all_close))) {
      STORAGE_LOG(WARN, "fail to close the wash block", K(ret));
    } else if (is_all_close) {
      free_page_nums_ = free_page_nums_ + wash_block->get_free_page_nums() - free_page_nums;
      if (wash_block->is_empty()) {
        // this block don't need to wash.
        if (OB_FAIL(refresh_dir_to_blk_map(wash_block->get_dir_id(), wash_block))) {
          STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*wash_block));
        } else {
          is_empty = true;
        }
      } else if (wash_block->is_inited() && !wash_block->is_disked()) {
        ObTmpBlockIOInfo info;
        info.tenant_id_ = tenant_id;
        info.io_desc_ = wash_block->get_io_desc();
        info.buf_ = wash_block->get_buffer();
        info.size_ = mblk_page_nums_ * ObTmpMacroBlock::get_default_page_size();
        ObMacroBlockHandle& mb_handle = wash_block->get_macro_block_handle();
        ObTmpBlockCacheKey key(wash_block->get_block_id(), tenant_id);
        mb_handle.set_file(file_handle_.get_storage_file());
        if (OB_FAIL(write_io(info, mb_handle))) {
          STORAGE_LOG(WARN, "fail to write tmp block", K(ret), K(tenant_id));
        } else if (OB_FAIL(write_handles_.push_back(&mb_handle))) {
          STORAGE_LOG(WARN, "fail to push back into write_handles", K(ret));
        } else if (wash_block->is_disked()) {
          // nothing to do
        } else if (OB_FAIL(wash_block->give_back_buf_into_cache(true /*is_wash*/))) {
          STORAGE_LOG(WARN, "fail to put tmp block cache", K(ret), K(tenant_id));
        } else {
          free_page_nums_ -= wash_block->get_free_page_nums();
          if (OB_FAIL(t_mblk_map_.erase_refactored(wash_block->get_block_id(), &wash_block))) {
            STORAGE_LOG(WARN, "fail to erase t_mblk_map", K(ret));
          } else {
            STORAGE_LOG(INFO, "succeed to wash a block", K(*wash_block));
          }
        }
      } else {
        STORAGE_LOG(WARN, "this block has been destoryed", K(*wash_block));
      }
    } else {
      STORAGE_LOG(INFO, "this block has some the unclosed extent", K(*wash_block));
    }
  }
  wash_block->set_washing_status(false);
  return ret;
}

int ObTmpTenantMemBlockManager::wait_write_io_finish()
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (write_handles_.count() > 0) {
    ObMacroBlockHandle* mb_handle = NULL;
    while (OB_SUCC(ret) && write_handles_.count() > 0) {
      if (OB_FAIL(write_handles_.pop_back(mb_handle))) {
        STORAGE_LOG(WARN, "fail to pop write handle", K(ret));
      } else if (OB_FAIL(mb_handle->wait(io_timeout_ms))) {
        STORAGE_LOG(WARN, "fail to wait tmp write io", K(ret));
      }
      mb_handle->get_io_handle().reset();
    }
    block_write_ctx_.clear();
    int tmp_ret = OB_SUCCESS;
    if (!block_write_ctx_.file_handle_.is_valid() &&
        OB_SUCCESS != (tmp_ret = block_write_ctx_.file_handle_.assign(file_handle_))) {
      STORAGE_LOG(WARN, "fail to assign file handle", K(ret), K(tmp_ret), K(file_handle_));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::write_io(const ObTmpBlockIOInfo& io_info, ObMacroBlockHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    ObMacroBlockWriteInfo write_info;
    ObFullMacroBlockMeta full_meta;
    ObMacroBlockMetaV2 macro_block_meta;
    ObMacroBlockSchemaInfo macro_schema;
    full_meta.meta_ = &macro_block_meta;
    full_meta.schema_ = &macro_schema;
    if (OB_FAIL(build_macro_meta(io_info.tenant_id_, full_meta))) {
      STORAGE_LOG(WARN, "fail to build macro meta", K(ret));
    } else {
      write_info.io_desc_ = io_info.io_desc_;
      write_info.meta_ = full_meta;
      write_info.block_write_ctx_ = &block_write_ctx_;
      write_info.buffer_ = io_info.buf_;
      write_info.size_ = io_info.size_;
      if (OB_FAIL(file_handle_.get_storage_file()->async_write_block(write_info, handle))) {
        STORAGE_LOG(WARN, "Fail to async write block", K(ret), K(full_meta));
      }
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::build_macro_meta(const uint64_t tenant_id, ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || nullptr == full_meta.meta_ || nullptr == full_meta.schema_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(full_meta));
  } else {
    ObMacroBlockMetaV2* macro_meta = const_cast<ObMacroBlockMetaV2*>(full_meta.meta_);
    macro_meta->attr_ = ObMacroBlockCommonHeader::SortTempData;
    macro_meta->table_id_ = combine_id(tenant_id, 1);
  }
  return ret;
}

int64_t ObTmpTenantMemBlockManager::get_tenant_mem_block_num()
{
  int64_t tenant_mem_block_num = TENANT_MEM_BLOCK_NUM;
  int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  if (last_access_ts > 0 && common::ObClockGenerator::getClock() - last_access_ts < 10000000) {
    tenant_mem_block_num = ATOMIC_LOAD(&last_tenant_mem_block_num_);
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (!tenant_config.is_valid()) {
      COMMON_LOG(INFO, "failed to get tenant config", K_(tenant_id));
    } else if (0 == tenant_config->_temporary_file_io_area_size) {
      tenant_mem_block_num = 1L;
    } else {
      const int64_t bytes = common::upper_align(
          lib::get_tenant_memory_limit(tenant_id_) * tenant_config->_temporary_file_io_area_size / 100,
          OB_TMP_FILE_STORE.get_block_size());
      tenant_mem_block_num = bytes / OB_TMP_FILE_STORE.get_block_size();
    }
    ATOMIC_STORE(&last_tenant_mem_block_num_, tenant_mem_block_num);
    ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
  }
  return tenant_mem_block_num;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
