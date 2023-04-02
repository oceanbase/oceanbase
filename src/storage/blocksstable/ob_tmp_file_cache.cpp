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
#include "storage/ob_file_system_router.h"
#include "share/ob_task_define.h"
#include "ob_tmp_file_cache.h"
#include "ob_tmp_file.h"
#include "ob_tmp_file_store.h"
#include "ob_block_manager.h"

using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase
{
namespace blocksstable
{

ObTmpPageCacheKey::ObTmpPageCacheKey()
  : block_id_(-1), page_id_(-1), tenant_id_(0)
{
}

ObTmpPageCacheKey::ObTmpPageCacheKey(const int64_t block_id, const int64_t page_id,
    const uint64_t tenant_id)
  : block_id_(block_id), page_id_(page_id), tenant_id_(tenant_id)
{
}

ObTmpPageCacheKey::~ObTmpPageCacheKey()
{
}

bool ObTmpPageCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObTmpPageCacheKey &other_key = reinterpret_cast<const ObTmpPageCacheKey &> (other);
  return block_id_ == other_key.block_id_
    && page_id_ == other_key.page_id_
    && tenant_id_ == other_key.tenant_id_;
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

int ObTmpPageCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
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

ObTmpPageCacheValue::ObTmpPageCacheValue(char *buf)
  : buf_(buf), size_(ObTmpMacroBlock::get_default_page_size())
{
}

ObTmpPageCacheValue::~ObTmpPageCacheValue()
{
}

int64_t ObTmpPageCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObTmpPageCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len),
                      "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp page cache value", K(ret));
  } else {
    ObTmpPageCacheValue *pblk_value = new (buf) ObTmpPageCacheValue(buf + sizeof(*this));
    MEMCPY(buf + sizeof(*this), buf_, size() - sizeof(*this));
    pblk_value->size_ = size_;
    value = pblk_value;
  }
  return ret;
}

int ObTmpPageCache::prefetch(
    const ObTmpPageCacheKey &key,
    const ObTmpBlockIOInfo &info,
    ObMacroBlockHandle &mb_handle,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() )) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(key));
  } else {
    // fill the callback
    ObTmpPageIOCallback callback;
    callback.cache_ = this;
    callback.offset_ = info.offset_;
    callback.buf_size_ = info.size_;
    callback.allocator_ = &allocator;
    callback.key_ = key;
    if (OB_FAIL(read_io(info, callback, mb_handle))) {
      if (mb_handle.get_io_handle().is_empty()) {
        // TODO: After the continuous IO has been optimized, this should
        // not happen.
        if (OB_FAIL(mb_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
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

int ObTmpPageCache::prefetch(
    const ObTmpBlockIOInfo &info,
    const common::ObIArray<ObTmpPageIOInfo> &page_io_infos,
    ObMacroBlockHandle &mb_handle,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_io_infos.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(page_io_infos.count()), K(info));
  } else {
    ObTmpMultiPageIOCallback callback;
    callback.cache_ = this;
    callback.offset_ = info.offset_;
    callback.buf_size_ = info.size_;
    callback.allocator_ = &allocator;
    if (OB_FAIL(callback.page_io_infos_.assign(page_io_infos))) {
      STORAGE_LOG(WARN, "fail to assign page io infos", K(ret), K(page_io_infos.count()), K(info));
    } else if (OB_FAIL(read_io(info, callback, mb_handle))) {
      if (mb_handle.get_io_handle().is_empty()) {
        // TODO: After the continuous IO has been optimized, this should
        // not happen.
        if (OB_FAIL(mb_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
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

int ObTmpPageCache::get_cache_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else {
    const ObTmpPageCacheValue *value = NULL;
    if (OB_FAIL(get(key, value, handle.handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        STORAGE_LOG(WARN, "fail to get key from page cache", K(ret));
      } else {
        EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_MISS);
      }
    } else {
      if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", K(ret));
      } else {
        handle.value_ = const_cast<ObTmpPageCacheValue *>(value);
        EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_HIT);
      }
    }
  }
  return ret;
}

ObTmpPageCache::ObITmpPageIOCallback::ObITmpPageIOCallback()
  : cache_(NULL), allocator_(NULL), offset_(0), buf_size_(0), io_buf_(NULL),
    io_buf_size_(0), data_buf_(NULL)
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

int ObTmpPageCache::ObITmpPageIOCallback::alloc_io_buf(
    char *&io_buf, int64_t &io_buf_size, int64_t &aligned_offset)
{
  int ret = OB_SUCCESS;
  io_buf_size = 0;
  aligned_offset = 0;
  common::align_offset_size(offset_, buf_size_, aligned_offset, io_buf_size);
  io_buf_size_ = io_buf_size + DIO_READ_ALIGN_SIZE;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid data, the allocator is NULL, ", K(ret));
  } else if (OB_UNLIKELY(NULL == (io_buf_ = (char*) (allocator_->alloc(io_buf_size_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "Fail to allocate memory, ", K(ret));
  } else {
    io_buf = upper_align_buf(io_buf_, DIO_READ_ALIGN_SIZE);
    data_buf_ = io_buf + (offset_ - aligned_offset);
  }
  return ret;
}

int ObTmpPageCache::ObITmpPageIOCallback::process_page(
    const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(cache_->put(key, value, true/*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put tmp page into cache", K(ret), K(key), K(value));
  }
  return ret;
}

ObTmpPageCache::ObTmpPageIOCallback::ObTmpPageIOCallback()
  : key_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObTmpPageIOCallback::~ObTmpPageIOCallback()
{
}

int ObTmpPageCache::ObTmpPageIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    ObTmpPageCacheValue value(const_cast<char *>(get_data()));
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

int ObTmpPageCache::ObTmpPageIOCallback::inner_deep_copy(char *buf,
    const int64_t buf_len, ObIOCallback *&callback) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The tmp page io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObTmpPageIOCallback *pcallback = new (buf) ObTmpPageIOCallback();
    *pcallback = *this;
    callback = pcallback;
  }
  return ret;
}

const char *ObTmpPageCache::ObTmpPageIOCallback::get_data()
{
  return data_buf_;
}

ObTmpPageCache::ObTmpMultiPageIOCallback::ObTmpMultiPageIOCallback()
  : page_io_infos_()
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObTmpMultiPageIOCallback::~ObTmpMultiPageIOCallback()
{
  page_io_infos_.reset();
  page_io_infos_.~ObIArray<ObTmpPageIOInfo>();
}

int ObTmpPageCache::ObTmpMultiPageIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback, ", KP_(cache), K(ret));
  } else if (is_success) {
    char *buf = const_cast<char *>(get_data());
    for (int32_t i = 0; OB_SUCC(ret) && i < page_io_infos_.count(); i++) {
      int64_t offset = page_io_infos_.at(i).key_.get_page_id()
          * ObTmpMacroBlock::get_default_page_size() - offset_;
      offset += ObTmpMacroBlock::get_header_padding();
      ObTmpPageCacheValue value(buf + offset);
      if (OB_FAIL(process_page(page_io_infos_.at(i).key_, value))) {
        STORAGE_LOG(WARN, "fail to process tmp page cache in callback", K(ret));
      }
    }
    page_io_infos_.reset();
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

int ObTmpPageCache::ObTmpMultiPageIOCallback::inner_deep_copy(char *buf,
    const int64_t buf_len, ObIOCallback *&callback) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "The tmp page io callback is not valid, ", KP_(cache), KP_(allocator), K(ret));
  } else {
    ObTmpMultiPageIOCallback *pcallback = new (buf) ObTmpMultiPageIOCallback();
    *pcallback = *this;
    if (OB_FAIL(pcallback->page_io_infos_.assign(page_io_infos_))) {
      STORAGE_LOG(WARN, "The tmp page io assign failed", K(ret));
    } else {
      callback = pcallback;
    }
  }
  return ret;
}

const char *ObTmpPageCache::ObTmpMultiPageIOCallback::get_data()
{
  return data_buf_;
}

int ObTmpPageCache::read_io(const ObTmpBlockIOInfo &io_info, ObITmpPageIOCallback &callback,
    ObMacroBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_MACRO_FILE);
  // fill the read info
  ObMacroBlockReadInfo read_info;
  read_info.io_desc_ = io_info.io_desc_;
  read_info.macro_block_id_ = io_info.macro_block_id_;
  read_info.io_callback_ = &callback;
  common::align_offset_size(io_info.offset_, io_info.size_, read_info.offset_, read_info.size_);
  if (OB_FAIL(ObBlockManager::async_read_block(read_info, handle))) {
    STORAGE_LOG(WARN, "fail to async read block", K(ret));
  }
  return ret;
}

ObTmpPageCache &ObTmpPageCache::get_instance()
{
  static ObTmpPageCache instance;
  return instance;
}

ObTmpPageCache::ObTmpPageCache()
{
}

ObTmpPageCache::~ObTmpPageCache()
{
}

int ObTmpPageCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>::init(
      cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  }
  return ret;
}

void ObTmpPageCache::destroy()
{
  common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>::destroy();
}

int ObTmpPageCache::put_page(const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, true/*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put page to page cache", K(ret), K(key), K(value));
  }
  return ret;
}

int ObTmpPageCache::get_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTmpPageCacheValue *value = NULL;
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
      handle.value_ = const_cast<ObTmpPageCacheValue *>(value);
      EVENT_INC(ObStatEventIds::TMP_PAGE_CACHE_HIT);
    }
  }
  return ret;
}

ObTmpBlockCacheKey::ObTmpBlockCacheKey(const int64_t block_id, const uint64_t tenant_id)
  : block_id_(block_id), tenant_id_(tenant_id)
{
}

bool ObTmpBlockCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const ObTmpBlockCacheKey &other_key = reinterpret_cast<const ObTmpBlockCacheKey &> (other);
  return block_id_ == other_key.block_id_
    && tenant_id_ == other_key.tenant_id_;
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

int ObTmpBlockCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp block cache key, ", K(*this), K(ret));
  } else {
    key = new(buf) ObTmpBlockCacheKey(block_id_, tenant_id_);
  }
  return ret;
}

ObTmpBlockCacheValue::ObTmpBlockCacheValue(char *buf)
      : buf_(buf), size_(ObTmpMacroBlock::get_block_size())
{
}

int64_t ObTmpBlockCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObTmpBlockCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(ret), KP(buf), K(buf_len),
        "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tmp block cache value", K(ret));
  } else {
    ObTmpBlockCacheValue *pblk_value = new (buf) ObTmpBlockCacheValue(buf + sizeof(*this));
    MEMCPY(buf + sizeof(*this), buf_, size() - sizeof(*this));
    pblk_value->size_ = size_;
    value = pblk_value;
  }
  return ret;
}

ObTmpBlockCache &ObTmpBlockCache::get_instance()
{
  static ObTmpBlockCache instance;
  return instance;
}

int ObTmpBlockCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_name) || OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(cache_name), K(priority));
  } else if (OB_FAIL((common::ObKVCache<ObTmpBlockCacheKey,
      ObTmpBlockCacheValue>::init(cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", K(ret));
  }
  return ret;
}

int ObTmpBlockCache::get_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTmpBlockCacheValue *value = NULL;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to get key-value from block cache", K(ret));
    } else {
      EVENT_INC(ObStatEventIds::TMP_BLOCK_CACHE_MISS);
    }
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", K(ret));
    } else {
      handle.value_ = const_cast<ObTmpBlockCacheValue *>(value);
      EVENT_INC(ObStatEventIds::TMP_BLOCK_CACHE_HIT);
    }
  }
  return ret;
}

int ObTmpBlockCache::alloc_buf(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc(key.get_tenant_id(), key.size(),
      sizeof(ObTmpBlockCacheValue) + ObTmpMacroBlock::get_block_size(),
      handle.kvpair_, handle.handle_, handle.inst_handle_))) {
    STORAGE_LOG(WARN, "failed to alloc kvcache buf", K(ret));
  } else if (OB_FAIL(key.deep_copy(reinterpret_cast<char *>(handle.kvpair_->key_),
      key.size(), handle.kvpair_->key_))) {
    STORAGE_LOG(WARN, "failed to deep copy key", K(ret), K(key));
  } else {
    char *buf = reinterpret_cast<char *>(handle.kvpair_->value_);
    handle.value_ = new (buf) ObTmpBlockCacheValue(buf + sizeof(ObTmpBlockCacheValue));
  }
  return ret;
}

int ObTmpBlockCache::put_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(put_kvpair(handle.inst_handle_, handle.kvpair_,
      handle.handle_, true/*overwrite*/))) {
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
  : is_inited_(false),
    last_access_tenant_config_ts_(0),
    last_tenant_mem_block_num_(1),
    free_page_nums_(0),
    tenant_id_(0),
    blk_nums_threshold_(0),
    compare_(),
    block_cache_(NULL),
    allocator_(NULL),
    write_handles_(),
    t_mblk_map_(),
    dir_to_blk_map_(),
    block_write_ctx_()
{
}

ObTmpTenantMemBlockManager::~ObTmpTenantMemBlockManager()
{
}

int ObTmpTenantMemBlockManager::init(const uint64_t tenant_id,
                                     common::ObIAllocator &allocator,
                                     double blk_nums_threshold)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpBlockCache has been inited", K(ret));
  } else if (OB_UNLIKELY(blk_nums_threshold <= 0) || OB_UNLIKELY(blk_nums_threshold > 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(blk_nums_threshold));
  } else if (OB_FAIL(t_mblk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_BLOCK_MAP))) {
    STORAGE_LOG(WARN, "Fail to create allocating block map, ", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_MAP))) {
    STORAGE_LOG(WARN, "Fail to create tmp dir map, ", K(ret));
  } else if (OB_ISNULL(block_cache_ = &ObTmpBlockCache::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get the block cache", K(ret));
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
  ObTmpMacroBlock *tmp = NULL;
  if (is_inited_ && OB_FAIL(wait_write_io_finish())) {
    ObMacroBlockHandle *mb_handle = NULL;
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
  allocator_ = NULL;
  block_write_ctx_.reset();
  is_inited_ = false;
}

int ObTmpTenantMemBlockManager::get_block(const ObTmpBlockCacheKey &key,
                                          ObTmpBlockValueHandle &handle)
{
  return block_cache_->get_block(key, handle);
}

int ObTmpTenantMemBlockManager::alloc_buf(const ObTmpBlockCacheKey &key,
                                          ObTmpBlockValueHandle &handle)
{
  return block_cache_->alloc_buf(key, handle);
}

int ObTmpTenantMemBlockManager::try_sync(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(t_mblk_map_.get_refactored(block_id, t_mblk))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "t_mblk_map get block failed", K(ret), K(block_id));
    } else {
      STORAGE_LOG(DEBUG, "the tmp macro block has been washed", K(ret), K(block_id));
    }
  } else if (t_mblk->is_washing()) {
    STORAGE_LOG(WARN, "the tmp macro block is washing", K(ret), K(block_id));
  } else if (t_mblk->is_disked()) {
    STORAGE_LOG(WARN, "the tmp macro block has been disked", K(ret), K(block_id));
  } else if (0 == t_mblk->get_used_page_nums()) {
    STORAGE_LOG(WARN, "the tmp macro block has not been written", K(ret), K(block_id));
  } else {
    t_mblk->set_washing_status(true);
    common::ObIArray<ObTmpFileExtent* > &extents = t_mblk->get_extents();
    for (int64_t i=0; OB_SUCC(ret) && i< extents.count(); i++){
      if (!extents.at(i)->is_closed()) {
        ret = OB_STATE_NOT_MATCH;
        STORAGE_LOG(WARN, "the tmp macro block's extents is not all closed", K(ret), K(block_id));
      }
    }
    bool is_empty = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(wash_block(t_mblk, is_empty))) {
        STORAGE_LOG(WARN, "fail to wash", K(ret), K(tenant_id_), K(*t_mblk));
      } else if (is_empty) {
        STORAGE_LOG(ERROR, "block to sync is empty", K(ret), K(tenant_id_), K(block_id));
      } else if (OB_FAIL(wait_write_io_finish())) {
        STORAGE_LOG(WARN, "wait sync finish failed", K(ret), K(tenant_id_), K(block_id));
      }
    }
    t_mblk->set_washing_status(false);
  }
  return ret;
}

int ObTmpTenantMemBlockManager::try_wash(common::ObIArray<ObTmpMacroBlock *> &free_blocks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantMemBlockManager has not been inited", K(ret));
  } else {
    const int64_t count = t_mblk_map_.size();
    const int64_t wash_threshold = get_tenant_mem_block_num();
    const int64_t oversize = count - wash_threshold + 1;
    const int64_t clean_nums = oversize > 1 ? oversize : 1;
    if (OB_FAIL(wash(clean_nums, free_blocks))) {
      STORAGE_LOG(WARN, "Wash tmp macro blocks failed ", K(ret), K(clean_nums));
    }
  }

  return ret;

}

int ObTmpTenantMemBlockManager::free_macro_block(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(t_mblk_map_.get_refactored(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "the tmp macro block has been washed", K(ret), K(block_id));
  } else if (OB_FAIL(t_mblk_map_.erase_refactored(block_id))) {
    STORAGE_LOG(WARN, "fail to erase tmp macro block", K(ret));
  } else if (OB_FAIL(erase_block_from_dir_map(block_id))) {
    STORAGE_LOG(WARN, "fail to erase block from dir map", K(ret));
  } else {
    free_page_nums_ -= t_mblk->get_free_page_nums();
  }
  return ret;
}

int ObTmpTenantMemBlockManager::erase_block_from_dir_map(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  Map::iterator iter;
  int64_t dir_id = -1;
  for (iter = dir_to_blk_map_.begin(); iter != dir_to_blk_map_.end(); ++iter) {
    const int64_t to_erase_blk_id = iter->second;
    if (to_erase_blk_id == block_id) {
      dir_id = iter->first;
      break;
    }
  }

  if (OB_UNLIKELY(-1 ==  dir_id)) {
    // do nothing
  } else if (OB_FAIL(dir_to_blk_map_.erase_refactored(dir_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "erase block from dir map failed", K(ret), K(dir_id));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::alloc_extent(const int64_t dir_id, const uint64_t tenant_id,
    const int64_t size, ObTmpFileExtent &extent, common::ObIArray<ObTmpMacroBlock *> &free_blocks)
{
  int ret = OB_SUCCESS;
  int64_t block_id = -1;
  const int64_t page_nums = std::ceil(size * 1.0 / ObTmpMacroBlock::get_default_page_size());
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
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
    STORAGE_LOG(DEBUG, "the tmp macro block has been washed", K(ret), K(block_id));
    if (OB_FAIL(get_macro_block(dir_id, tenant_id, page_nums, t_mblk, free_blocks))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get macro block", K(ret));
      }
    } else if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1))) {
      STORAGE_LOG(WARN, "fail to set dir_to_blk_map", K(ret));
    }
  } else if (t_mblk->get_max_cont_page_nums() < page_nums
      || t_mblk->get_tenant_id() != tenant_id_) {
    if (OB_FAIL(get_macro_block(dir_id, tenant_id, page_nums, t_mblk, free_blocks))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get macro block", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(t_mblk)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, t_mblk is nullptr", K(ret), KP(t_mblk));
    } else if (OB_FAIL(t_mblk->alloc(page_nums, extent))){
      STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
    } else if (OB_FAIL(refresh_dir_to_blk_map(t_mblk->get_dir_id(), t_mblk))) {
      STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
    } else {
      free_page_nums_ -= extent.get_page_nums();
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::alloc_block_all_pages(ObTmpMacroBlock *t_mblk, ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t_mblk)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, t_mblk is nullptr", K(ret), KP(t_mblk));
  } else if (OB_FAIL(t_mblk->alloc_all_pages(extent))){
    STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
  } else {
    free_page_nums_ -= extent.get_page_nums();
  }

  return ret;
}

int ObTmpTenantMemBlockManager::free_extent(const int64_t free_page_nums,
    const ObTmpMacroBlock *t_mblk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_UNLIKELY(free_page_nums < 0 || free_page_nums > ObTmpFilePageBuddy::MAX_PAGE_NUMS)
             || OB_ISNULL(t_mblk)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(free_page_nums), KPC(t_mblk));
  } else if (OB_FAIL(refresh_dir_to_blk_map(t_mblk->get_dir_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
  } else {
    free_page_nums_ += free_page_nums;
  }
  return ret;
}

int ObTmpTenantMemBlockManager::get_macro_block(const int64_t dir_id,
                                                const uint64_t tenant_id,
                                                const int64_t page_nums,
                                                ObTmpMacroBlock *&t_mblk,
                                                common::ObIArray<ObTmpMacroBlock *> &free_blocks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
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
      const int64_t wash_threshold = get_tenant_mem_block_num();
      if (OB_UNLIKELY(t_mblk_map_.size() == 0)) {
        // nothing to do.
      } else if (OB_UNLIKELY(free_page_nums_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "free page nums can not be negative", K(ret), K(free_page_nums_));
      } else if ( get_tenant_mem_block_num() <= count ||
          blk_nums_threshold_ > (free_page_nums_ * 1.0) / (t_mblk_map_.size() * ObTmpFilePageBuddy::MAX_PAGE_NUMS)) {
        int64_t wash_nums = 1;
        if (OB_FAIL(wash(std::max(wash_nums, count - wash_threshold + 1), free_blocks))) {
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

ObTmpTenantMemBlockManager::BlockWashScoreCompare::BlockWashScoreCompare()
{
}

bool ObTmpTenantMemBlockManager::BlockWashScoreCompare::operator() (
    const ObTmpTenantMemBlockManager::BlockInfo &left,
    const ObTmpTenantMemBlockManager::BlockInfo &right)
{
  return left.wash_score_ < right.wash_score_;
}

int ObTmpTenantMemBlockManager::wash(const int64_t block_nums,
    common::ObIArray<ObTmpMacroBlock *> &free_blocks)
{
  int ret = OB_SUCCESS;
  TmpMacroBlockMap::iterator iter;
  common::ObArray<ObTmpMacroBlock*> blks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantMemBlockManager has not been inited", K(ret));
  } else if (OB_FAIL(wait_write_io_finish())) {
    STORAGE_LOG(WARN, "fail to wait previous write io", K(ret));
  } else {
    Heap heap(compare_, allocator_);
    int64_t cur_time = ObTimeUtility::fast_current_time();
    for (iter = t_mblk_map_.begin(); OB_SUCC(ret) && iter != t_mblk_map_.end(); ++iter) {
      ObTmpMacroBlock *m_blk = iter->second;
      if (OB_UNLIKELY(NULL != m_blk) && OB_UNLIKELY(m_blk->is_inited()) && OB_UNLIKELY(!m_blk->is_disked()) &&
          OB_UNLIKELY(0 != m_blk->get_used_page_nums())) {
        BlockInfo info;
        info.block_id_ = m_blk->get_block_id();
        info.wash_score_ = m_blk->get_wash_score(cur_time);
        if(OB_FAIL(heap.push(info))) {
          STORAGE_LOG(WARN, "insert block to array failed", K(ret));
        }
      }
    }
    for (int64_t wash_count = 0; OB_SUCC(ret) && wash_count < block_nums && heap.count() > 0;) {
      const BlockInfo info = heap.top();
      ObTmpMacroBlock *m_blk = NULL;
      if (OB_FAIL(t_mblk_map_.get_refactored(info.block_id_, m_blk))) {
        STORAGE_LOG(WARN, "get block failed", K(ret));
      } else if(OB_UNLIKELY(NULL == m_blk)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "block is NULL ", K(ret), K(*m_blk));
      } else if (m_blk->is_washing()) {
        // do nothing
      } else {
        m_blk->set_washing_status(true);
        bool is_empty = false;
        if (m_blk->is_empty()) {
          if (OB_FAIL(refresh_dir_to_blk_map(m_blk->get_dir_id(), m_blk))) {
            STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*m_blk));
          }
        } else if (OB_FAIL(wash_block(m_blk, is_empty))) {
          STORAGE_LOG(WARN, "fail to wash", K(ret), K_(tenant_id), K(*m_blk));
        } else if (OB_FAIL(free_blocks.push_back(m_blk))) {
          STORAGE_LOG(WARN, "fail to push back to free_blocks", K(ret), K_(tenant_id));
        } else {
          wash_count++;
        }
        m_blk->set_washing_status(false);
      }
      if(OB_SUCC(ret)) {
        if (OB_FAIL(heap.pop())) {
          STORAGE_LOG(WARN, "pop info from heap failed", K(ret), K_(tenant_id));
        }
      }
    }
  }

  return ret;
}

int ObTmpTenantMemBlockManager::add_macro_block(ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(t_mblk_map_.set_refactored(t_mblk->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to set tmp macro block map", K(ret), K(t_mblk));
  } else {
    free_page_nums_ += ObTmpFilePageBuddy::MAX_PAGE_NUMS;
  }
  return ret;
}

int ObTmpTenantMemBlockManager::refresh_dir_to_blk_map(const int64_t dir_id,
    const ObTmpMacroBlock *t_mblk)
{
  int ret = OB_SUCCESS;
  int64_t block_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.get_refactored(dir_id, block_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to set dir_to_blk_map_", K(ret));
      }
    }
  } else {
    ObTmpMacroBlock *dir_mblk = NULL;
    if (OB_FAIL(t_mblk_map_.get_refactored(block_id, dir_mblk))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1);
        STORAGE_LOG(DEBUG, "the tmp macro block has been removed or washed", K(ret), K(block_id));
      } else {
        STORAGE_LOG(WARN, "fail to get block", K(ret), K(block_id));
      }
    } else if (dir_mblk->get_max_cont_page_nums() < t_mblk->get_max_cont_page_nums()) {
      ret = dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1);
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::wash_block(ObTmpMacroBlock *wash_block, bool &is_empty)
{
  int ret = OB_SUCCESS;
  // close all of extents in this block.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (NULL == wash_block) {
    STORAGE_LOG(WARN, "The washing block is null", K(ret));
  } else {
    bool is_all_close = false;
    uint8_t free_page_nums = 0;
    if (OB_FAIL(wash_block->close(is_all_close, free_page_nums)) ) {
      STORAGE_LOG(WARN, "fail to close the wash block", K(ret));
    } else if (FALSE_IT(free_page_nums_ = free_page_nums_ + free_page_nums)){
    } else if (is_all_close) {
      if (wash_block->is_empty()) {
        // this block don't need to wash.
        if (OB_FAIL(refresh_dir_to_blk_map(wash_block->get_dir_id(), wash_block))) {
          STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*wash_block));
        } else {
          is_empty = true;
        }
      } else if (wash_block->is_inited() && !wash_block->is_disked()) {
        ObTmpBlockIOInfo info;
        ObMacroBlockHandle &mb_handle = wash_block->get_macro_block_handle();
        if (OB_FAIL(wash_block->get_wash_io_info(info))) {
          STORAGE_LOG(WARN, "fail to get wash io info", K(ret), K_(tenant_id));
        } else if (OB_FAIL(write_io(info, mb_handle))) {
          STORAGE_LOG(WARN, "fail to write tmp block", K(ret), K_(tenant_id));
        } else if (OB_FAIL(write_handles_.push_back(&mb_handle))) {
          STORAGE_LOG(WARN, "fail to push back into write_handles", K(ret));
        } else if (wash_block->is_disked()) {
          // nothing to do
        } else if (OB_FAIL(wash_block->give_back_buf_into_cache(true/*is_wash*/))) {
          STORAGE_LOG(WARN, "fail to put tmp block cache", K(ret), K_(tenant_id));
        } else {
          OB_TMP_FILE_STORE.dec_block_cache_num(tenant_id_, 1);
          free_page_nums_ -= wash_block->get_free_page_nums();
          if (OB_FAIL(t_mblk_map_.erase_refactored(wash_block->get_block_id(), &wash_block))) {
            STORAGE_LOG(WARN, "fail to erase t_mblk_map", K(ret));
          } else if(OB_FAIL(erase_block_from_dir_map(wash_block->get_block_id()))){
            STORAGE_LOG(WARN, "fail to erase block from dir map", K(ret));
          } else {
            ObTaskController::get().allow_next_syslog();
            STORAGE_LOG(INFO, "succeed to wash a block", K(*wash_block));
          }
        }
      }
    } else {
      STORAGE_LOG(INFO, "this block has some the unclosed extent", K(*wash_block));
    }
  }
  wash_block->set_washing_status(false);
  return ret;
}

int ObTmpTenantMemBlockManager::free_empty_blocks(common::ObIArray<ObTmpMacroBlock *> &free_blocks)
{
  int ret = OB_SUCCESS;
  if (free_blocks.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < free_blocks.count(); ++i) {
      ObTmpMacroBlock* blk = free_blocks.at(i);
      if (blk->is_empty()) {
        if (OB_FAIL(free_macro_block(blk->get_block_id()))) {
          STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
        }
      }
      free_blocks.at(i) = NULL;
    }
    free_blocks.reset();
  }
  return ret;
}

int ObTmpTenantMemBlockManager::wait_write_io_finish()
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (write_handles_.count() > 0) {
    ObMacroBlockHandle *mb_handle = NULL;
    while (OB_SUCC(ret) && write_handles_.count() > 0) {
      if (OB_FAIL(write_handles_.pop_back(mb_handle))) {
        STORAGE_LOG(WARN, "fail to pop write handle", K(ret));
      } else if(OB_FAIL(mb_handle->wait(io_timeout_ms))) {
        STORAGE_LOG(WARN, "fail to wait tmp write io", K(ret));
      }
      mb_handle->get_io_handle().reset();
    }
    block_write_ctx_.clear();
  }
  return ret;
}

int ObTmpTenantMemBlockManager::write_io(
    const ObTmpBlockIOInfo &io_info,
    ObMacroBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  const int64_t page_size = ObTmpMacroBlock::get_default_page_size();
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (OB_FAIL(THE_IO_DEVICE->check_space_full(OB_SERVER_BLOCK_MGR.get_macro_block_size()))) {
    STORAGE_LOG(WARN, "fail to check space full", K(ret));
  } else {
    ObMacroBlockWriteInfo write_info;
    write_info.io_desc_ = io_info.io_desc_;
    write_info.buffer_ = io_info.buf_;
    write_info.offset_ = ObTmpMacroBlock::get_header_padding();
    write_info.size_ = io_info.size_;
    if (OB_FAIL(ObBlockManager::async_write_block(write_info, handle))) {
      STORAGE_LOG(WARN, "Fail to async write block", K(ret), K(write_info), K(handle));
    } else if (OB_FAIL(block_write_ctx_.add_macro_block_id(handle.get_macro_id()))) {
      STORAGE_LOG(WARN, "fail to add macro id", K(ret), "macro id", handle.get_macro_id());
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.update_write_time(handle.get_macro_id(),
        true/*update_to_max_time*/))) { //just to skip bad block inspect
      STORAGE_LOG(WARN, "fail to update macro id write time", K(ret), "macro id", handle.get_macro_id());
    }
  }
  return ret;
}

int64_t ObTmpTenantMemBlockManager::get_tenant_mem_block_num()
{
  int64_t tenant_mem_block_num = TENANT_MEM_BLOCK_NUM;
  int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  if (last_access_ts > 0
      && common::ObClockGenerator::getClock() - last_access_ts < 10000000) {
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
        ObTmpMacroBlock::get_block_size());
      tenant_mem_block_num = bytes / ObTmpMacroBlock::get_block_size();
    }
    ATOMIC_STORE(&last_tenant_mem_block_num_, tenant_mem_block_num);
    ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
  }
  return tenant_mem_block_num;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
