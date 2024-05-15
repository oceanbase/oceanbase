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

int ObTmpPageCache::inner_read_io(const ObTmpBlockIOInfo &io_info,
                                  ObITmpPageIOCallback *callback,
                                  ObMacroBlockHandle &macro_block_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(read_io(io_info, callback, macro_block_handle))) {
    if (macro_block_handle.get_io_handle().is_empty()) {
      // TODO: After the continuous IO has been optimized, this should
      // not happen.
      if (OB_FAIL(macro_block_handle.wait())) {
        STORAGE_LOG(WARN, "fail to wait tmp page io", K(ret), KP(callback));
      } else if (OB_FAIL(read_io(io_info, callback, macro_block_handle))) {
        STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret), KP(callback));
      }
    } else {
      STORAGE_LOG(WARN, "fail to read tmp page from io", K(ret), KP(callback));
    }
  }
  // Avoid double_free with io_handle
  if (OB_FAIL(ret) && OB_NOT_NULL(callback) && OB_NOT_NULL(callback->get_allocator())) {
    common::ObIAllocator *allocator = callback->get_allocator();
    callback->~ObITmpPageIOCallback();
    allocator->free(callback);
  }
  return ret;
}

int ObTmpPageCache::direct_read(const ObTmpBlockIOInfo &info,
                                ObMacroBlockHandle &mb_handle,
                                common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTmpDirectReadPageIOCallback *callback = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTmpDirectReadPageIOCallback)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate callback memory failed", K(ret));
  } else {
    // fill the callback
    callback = new (buf) ObTmpDirectReadPageIOCallback;
    callback->cache_ = this;
    callback->offset_ = info.offset_;
    callback->allocator_ = &allocator;
    if (OB_FAIL(inner_read_io(info, callback, mb_handle))) {
      STORAGE_LOG(WARN, "fail to inner read io", K(ret), K(mb_handle));
    }
    // There is no need to handle error cases (freeing the memory of the
    // callback) because inner_read_io will handle error cases and free the
    // memory of the callback.
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
    void *buf = nullptr;
    ObTmpPageIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTmpPageIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObTmpPageIOCallback;
      callback->cache_ = this;
      callback->offset_ = info.offset_;
      callback->allocator_ = &allocator;
      callback->key_ = key;
      if (OB_FAIL(inner_read_io(info, callback, mb_handle))) {
        STORAGE_LOG(WARN, "fail to inner read io", K(ret), K(mb_handle));
      }
      // There is no need to handle error cases (freeing the memory of the
      // callback) because inner_read_io will handle error cases and free the
      // memory of the callback.
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
    void *buf = nullptr;
    ObTmpMultiPageIOCallback *callback = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTmpMultiPageIOCallback)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate callback memory failed", K(ret));
    } else {
      callback = new (buf) ObTmpMultiPageIOCallback;
      callback->cache_ = this;
      callback->offset_ = info.offset_;
      callback->allocator_ = &allocator;
      if (OB_FAIL(callback->page_io_infos_.assign(page_io_infos))) {
        STORAGE_LOG(WARN, "fail to assign page io infos", K(ret), K(page_io_infos.count()), K(info));
        if (OB_NOT_NULL(callback)) {  // handle ObArray assign fail case and free callback
          callback->~ObTmpMultiPageIOCallback();
          allocator.free(callback);
          callback = nullptr;
        }
      } else if (OB_FAIL(inner_read_io(info, callback, mb_handle))) {
        STORAGE_LOG(WARN, "fail to inner read io", K(ret), K(mb_handle));
      }
      // inner_read_io will handle error cases and free the memory of callback.
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
  : cache_(NULL), allocator_(NULL), offset_(0), data_buf_(NULL)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObTmpPageCache::ObITmpPageIOCallback::~ObITmpPageIOCallback()
{
  if (NULL != allocator_ && NULL != data_buf_) {
    allocator_->free(data_buf_);
    data_buf_ = NULL;
  }
  allocator_ = NULL;
}

int ObTmpPageCache::ObITmpPageIOCallback::alloc_data_buf(const char *io_data_buffer, const int64_t data_size)
{
  int ret = alloc_and_copy_data(io_data_buffer, data_size, allocator_, data_buf_);
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

int ObTmpPageCache::ObTmpPageIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("TmpPage_Callback_Process", 100000); //100ms
  if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback or allocator", KP_(cache), KP_(allocator), K(ret));
  } else if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else if (OB_FAIL(alloc_data_buf(data_buffer, size))) {
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(size));
  } else if (FALSE_IT(time_guard.click("alloc_data_buf"))) {
  } else {
    ObTmpPageCacheValue value(data_buf_);
    if (OB_FAIL(process_page(key_, value))) {
      STORAGE_LOG(WARN, "fail to process tmp page cache in callback", K(ret));
    }
    time_guard.click("process_page");
  }
  if (OB_FAIL(ret) && NULL != allocator_ && NULL != data_buf_) {
    allocator_->free(data_buf_);
    data_buf_ = NULL;
  }
  return ret;
}

int64_t ObTmpPageCache::ObTmpPageIOCallback::size() const
{
  return sizeof(*this);
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

int ObTmpPageCache::ObTmpMultiPageIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("TmpMultiPage_Callback_Process", 100000); //100ms
  if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callbackor allocator", KP_(cache), KP_(allocator), K(ret));
  } else if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else if (OB_FAIL(alloc_data_buf(data_buffer, size))) {
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(size));
  } else if (FALSE_IT(time_guard.click("alloc_data_buf"))) {
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < page_io_infos_.count(); i++) {
      int64_t cur_offset = page_io_infos_.at(i).key_.get_page_id()
          * ObTmpMacroBlock::get_default_page_size() - offset_;
      cur_offset += ObTmpMacroBlock::get_header_padding();
      ObTmpPageCacheValue value(data_buf_ + cur_offset);
      if (OB_FAIL(process_page(page_io_infos_.at(i).key_, value))) {
        STORAGE_LOG(WARN, "fail to process tmp page cache in callback", K(ret));
      }
    }
    time_guard.click("process_page");
    page_io_infos_.reset();
  }
  if (OB_FAIL(ret) && NULL != allocator_ && NULL != data_buf_) {
    allocator_->free(data_buf_);
    data_buf_ = NULL;
  }
  return ret;
}

int64_t ObTmpPageCache::ObTmpMultiPageIOCallback::size() const
{
  return sizeof(*this);
}

const char *ObTmpPageCache::ObTmpMultiPageIOCallback::get_data()
{
  return data_buf_;
}

int64_t ObTmpPageCache::ObTmpDirectReadPageIOCallback::size() const
{
  return sizeof(*this);
}

const char * ObTmpPageCache::ObTmpDirectReadPageIOCallback::get_data()
{
  return data_buf_;
}

int ObTmpPageCache::ObTmpDirectReadPageIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTmpDirectReadPageIOCallback", 100000); //100ms
  if (OB_ISNULL(cache_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid tmp page cache callback allocator", KP_(cache), KP_(allocator), K(ret));
  } else if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else if (OB_FAIL(alloc_data_buf(data_buffer, size))) {
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(size));
  } else if (FALSE_IT(time_guard.click("alloc_data_buf"))) {
  }
  if (OB_FAIL(ret) && NULL != allocator_ && NULL != data_buf_) {
    allocator_->free(data_buf_);
    data_buf_ = NULL;
  }
  return ret;
}

int ObTmpPageCache::read_io(const ObTmpBlockIOInfo &io_info, ObITmpPageIOCallback *callback,
    ObMacroBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_MACRO_FILE);
  // fill the read info
  ObMacroBlockReadInfo read_info;
  read_info.io_desc_ = io_info.io_desc_;
  read_info.macro_block_id_ = io_info.macro_block_id_;
  read_info.io_timeout_ms_ = io_info.io_timeout_ms_;
  read_info.io_callback_ = callback;
  read_info.offset_ = io_info.offset_;
  read_info.size_ = io_info.size_;
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::TMP_PAGE_CACHE_IO);
  if (OB_FAIL(ObBlockManager::async_read_block(read_info, handle))) {
    STORAGE_LOG(WARN, "fail to async read block", K(ret), K(read_info), KP(callback));
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
      : buf_(buf), size_(ObTmpFileStore::get_block_size())
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
      sizeof(ObTmpBlockCacheValue) + ObTmpFileStore::get_block_size(),
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

void ObTmpBlockCache::destroy()
{
  common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue>::destroy();
}

ObTmpFileWaitTask::ObTmpFileWaitTask(ObTmpTenantMemBlockManager &mgr)
    : mgr_(mgr)
{
}

void ObTmpFileWaitTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mgr_.exec_wait())) {
    STORAGE_LOG(WARN, "fail to wait block", K(ret));
  }
}

ObTmpFileMemTask::ObTmpFileMemTask(ObTmpTenantMemBlockManager &mgr)
    : mgr_(mgr)
{
}

void ObTmpFileMemTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mgr_.change_mem())) {
    if (OB_EAGAIN != ret){
      STORAGE_LOG(WARN, "fail to wait block", K(ret));
    }
  }
}


ObTmpTenantMemBlockManager::IOWaitInfo::IOWaitInfo(
    ObMacroBlockHandle &block_handle, ObTmpMacroBlock &block, ObIAllocator &allocator)
  : block_handle_(&block_handle), block_(block), allocator_(allocator), ref_cnt_(0), ret_code_(OB_SUCCESS)
{
}

void ObTmpTenantMemBlockManager::IOWaitInfo::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObTmpTenantMemBlockManager::IOWaitInfo::dec_ref()
{
  int ret = OB_SUCCESS;
  const int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "bug: ref_cnt < 0", K(ret), K(tmp_ref), K(lbt()));
    ob_abort();
  } else if (0 == tmp_ref) {
    this->~IOWaitInfo();
    allocator_.free(this);
  }
}

int ObTmpTenantMemBlockManager::IOWaitInfo::wait(int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (block_.is_washing()) {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      STORAGE_LOG(ERROR, "fail to guard request condition", K(ret), K(block_.get_block_id()));
    } else {
      int64_t begin_us = ObTimeUtility::fast_current_time();
      int64_t wait_ms = timeout_ms;
      while (OB_SUCC(ret) && block_.is_washing() && wait_ms > 0) {
        if (OB_FAIL(cond_.wait(wait_ms))) {
          STORAGE_LOG(WARN, "fail to wait block write condition", K(ret), K(wait_ms), K(block_.get_block_id()));
        } else if (OB_FAIL(ret = ret_code_)) {
          STORAGE_LOG(WARN, "fail to wait io info", K(ret), KPC(this));
        } else if (block_.is_washing()) {
          wait_ms = timeout_ms - (ObTimeUtility::fast_current_time() - begin_us) / 1000;
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(wait_ms <= 0)) { // rarely happen
        ret = OB_TIMEOUT;
        STORAGE_LOG(WARN, "fail to wait block io condition due to spurious wakeup",
            K(ret), K(wait_ms), K(block_.get_block_id()));
      }
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::IOWaitInfo::exec_wait()
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  if (OB_FAIL(guard.get_ret())) {
    STORAGE_LOG(ERROR, "lock io request condition failed", K(ret), K(block_.get_block_id()));
  } else if (OB_NOT_NULL(block_handle_) && OB_FAIL(block_handle_->wait())) {
    STORAGE_LOG(WARN, "wait handle wait io failed", K(ret), K(block_.get_block_id()));
    block_handle_->reset_macro_id();
  }
  reset_io();
  return ret;
}

int ObTmpTenantMemBlockManager::IOWaitInfo::broadcast()
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  if (OB_FAIL(guard.get_ret())) {
    STORAGE_LOG(ERROR, "lock io request condition failed", K(ret), K(block_.get_block_id()));
  } else if (OB_FAIL(cond_.broadcast())) {
    STORAGE_LOG(WARN, "wait handle wait io failed", K(ret), K(block_.get_block_id()));
  }
  return ret;
}

ObTmpTenantMemBlockManager::IOWaitInfo::~IOWaitInfo()
{
  destroy();
}

void ObTmpTenantMemBlockManager::IOWaitInfo::destroy()
{
  ret_code_ = OB_SUCCESS;
  reset_io();
  if (0 != ATOMIC_LOAD(&ref_cnt_)) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, ref cnt isn't zero", K(ret), KPC(this));
  }
}

void ObTmpTenantMemBlockManager::IOWaitInfo::reset_io()
{
  if (OB_NOT_NULL(block_handle_)) {
    block_handle_->get_io_handle().reset();
    block_handle_ = nullptr;
  }
}

ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::ObIOWaitInfoHandle()
  : wait_info_(nullptr)
{
}

ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::ObIOWaitInfoHandle(const ObIOWaitInfoHandle &other)
  : wait_info_(nullptr)
{
  *this = other;
}

ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::~ObIOWaitInfoHandle()
{
  reset();
}

void ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::set_wait_info(IOWaitInfo *wait_info)
{
  if (OB_NOT_NULL(wait_info)) {
    reset();
    wait_info->inc_ref(); // ref for handle
    wait_info_ = wait_info;
  }
}

ObTmpTenantMemBlockManager::ObIOWaitInfoHandle&
ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::operator=(const ObTmpTenantMemBlockManager::ObIOWaitInfoHandle &other)
{
  if (&other != this) {
    set_wait_info(other.wait_info_);
  }
  return *this;
}

bool ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::is_empty() const
{
  return nullptr == wait_info_;
}

bool ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::is_valid() const
{
  return nullptr != wait_info_;
}

void ObTmpTenantMemBlockManager::ObIOWaitInfoHandle::reset()
{
  if (OB_NOT_NULL(wait_info_)) {
    wait_info_->dec_ref(); // ref for handle
    wait_info_ = nullptr;
  }
}

ObTmpTenantMemBlockManager::ObTmpTenantMemBlockManager(ObTmpTenantFileStore &tenant_store)
  : tenant_store_(tenant_store),
    wait_info_queue_(),
    t_mblk_map_(),
    dir_to_blk_map_(),
    blk_nums_threshold_(0),
    block_cache_(NULL),
    allocator_(NULL),
    tenant_id_(0),
    last_access_tenant_config_ts_(0),
    last_tenant_mem_block_num_(1),
    is_inited_(false),
    tg_id_(OB_INVALID_INDEX),
    stopped_(true),
    washing_count_(0),
    wait_task_(*this),
    mem_task_(*this),
    io_lock_(),
    map_lock_(),
    cond_(),
    compare_()
{
}

ObTmpTenantMemBlockManager::~ObTmpTenantMemBlockManager()
{
}

int ObTmpTenantMemBlockManager::init(const uint64_t tenant_id,
                                     common::ObConcurrentFIFOAllocator &allocator,
                                     double blk_nums_threshold)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpBlockCache has been inited", K(ret));
  } else if (OB_UNLIKELY(blk_nums_threshold <= 0) || OB_UNLIKELY(blk_nums_threshold > 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(blk_nums_threshold));
  } else if (OB_FAIL(wait_handles_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_BLOCK_MAP,
          "WaitHdl", tenant_id))) {
    STORAGE_LOG(WARN, "fail to create wait handles map", K(ret));
  } else if (OB_FAIL(t_mblk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_BLOCK_MAP,
          "TmpMBlk", tenant_id))) {
    STORAGE_LOG(WARN, "Fail to create allocating block map, ", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_MAP,
          "DirToBlk", tenant_id))) {
    STORAGE_LOG(WARN, "Fail to create tmp dir map, ", K(ret));
  } else if (OB_FAIL(map_lock_.init(MBLK_HASH_BUCKET_NUM, ObLatchIds::TMP_FILE_MEM_BLOCK_LOCK, "TmpMemBlkMgr", MTL_ID()))) {
    STORAGE_LOG(WARN, "Fail to create tmp dir map, ", K(ret));
  } else if (OB_ISNULL(block_cache_ = &ObTmpBlockCache::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get the block cache", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    STORAGE_LOG(WARN, "fail to init condition", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::COMMON_TIMER_THREAD, tg_id_))) {
    STORAGE_LOG(WARN, "TG_CREATE_TENANT failed", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    STORAGE_LOG(WARN, "TG_START failed", KR(ret), K_(tg_id));
  } else if (FALSE_IT(stopped_ = false)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, wait_task_, TASK_INTERVAL, true/*repeat*/))) {
    STORAGE_LOG(WARN, "TG_SCHEDULE task failed", KR(ret), K_(tg_id), K(TASK_INTERVAL));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, mem_task_, MEMORY_TASK_INTERVAL, true/*repeat*/))) {
    STORAGE_LOG(WARN, "TG_SCHEDULE task failed", KR(ret), K_(tg_id), K(MEMORY_TASK_INTERVAL));
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

int ObTmpTenantMemBlockManager::DestroyBlockMapOp::operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *blk = entry.second;
  if (OB_ISNULL(blk)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "block is null", K(ret));
  } else if (blk->is_memory()) {
    if (OB_FAIL(blk->check_and_set_status(
            ObTmpMacroBlock::BlockStatus::MEMORY, ObTmpMacroBlock::BlockStatus::DISKED))) {
      if (OB_STATE_NOT_MATCH == ret) {
        STORAGE_LOG(DEBUG, "this block is washing", K(ret), K(*blk));
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "check and set status failed", K(ret), K(*blk));
      }
    } else if (OB_FAIL(blk->give_back_buf_into_cache())) {
      STORAGE_LOG(WARN, "fail to put tmp block cache", K(ret), K(blk));
    } else {
      tenant_store_.dec_block_cache_num(1);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "block status not correct", K(ret), K(blk));
  }
  return ret;
}

void ObTmpTenantMemBlockManager::destroy()
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *tmp = NULL;
  stopped_ = true;
  if (OB_INVALID_INDEX != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = OB_INVALID_INDEX;
  }
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  ObSpLinkQueue::Link *node = NULL;
  while (!wait_info_queue_.is_empty()) {
    IOWaitInfo *wait_info = NULL;
    if (OB_FAIL(wait_info_queue_.pop(node))) {
      STORAGE_LOG(WARN, "pop wait handle failed", K(ret));
    } else if (FALSE_IT(wait_info = static_cast<IOWaitInfo*>(node))) {
    } else if (OB_FAIL(wait_info->exec_wait())) {
      // overwrite ret
      STORAGE_LOG(WARN, "fail to exec iohandle wait", K(ret), K_(tenant_id));
    }
  }
  ATOMIC_STORE(&washing_count_, 0);
  DestroyBlockMapOp op(tenant_store_);
  if (OB_FAIL(t_mblk_map_.foreach_refactored(op))) {
    // overwrite ret
    STORAGE_LOG(WARN, "destroy mblk map failed", K(ret));
  }
  t_mblk_map_.destroy();
  blk_nums_threshold_ = 0;
  dir_to_blk_map_.destroy();
  if (NULL != block_cache_) {
    block_cache_ = NULL;
  }
  map_lock_.destroy();
  allocator_ = NULL;
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
  } else if (OB_FAIL(t_mblk_map_.erase_refactored(block_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to erase tmp macro block", K(ret));
    } else {
      ret = OB_SUCCESS;
      STORAGE_LOG(DEBUG, "macro block has been erased", K(ret));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::alloc_extent(const int64_t dir_id, const uint64_t tenant_id,
    const int64_t size, ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  const int64_t page_nums = std::ceil(size * 1.0 / ObTmpMacroBlock::get_default_page_size());
  int64_t block_id = -1;
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(get_block_from_dir_cache(dir_id, tenant_id, page_nums, t_mblk))) {
    bool is_found = false;
    if (OB_FAIL(get_available_macro_block(dir_id, tenant_id, page_nums, t_mblk, is_found))) {
    } else if (!is_found) {
      ret = OB_STATE_NOT_MATCH;
      STORAGE_LOG(DEBUG, "cannot find available macro block", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(t_mblk)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, t_mblk is nullptr", K(ret), KP(t_mblk));
    } else if (OB_FAIL(t_mblk->alloc(page_nums, extent))){
      STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
    } else if (OB_FAIL(refresh_dir_to_blk_map(dir_id, t_mblk))) {
      STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::get_block_from_dir_cache(const int64_t dir_id, const int64_t tenant_id,
                                                         const int64_t page_nums, ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  int64_t block_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(dir_to_blk_map_.get_refactored(dir_id, block_id))) {
    STORAGE_LOG(DEBUG, "fail to get macro block from dir cache", K(ret), K(dir_id), K(block_id), K(dir_to_blk_map_.size()));
  } else if (OB_FAIL(t_mblk_map_.get_refactored(block_id, t_mblk))) {
    STORAGE_LOG(DEBUG, "the tmp macro block has been washed", K(ret), K(block_id));
  } else if (OB_ISNULL(t_mblk)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "block is null", K(ret));
  } else if (t_mblk->get_max_cont_page_nums() < page_nums
      || t_mblk->get_tenant_id() != tenant_id
      || !t_mblk->is_memory()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(DEBUG, "the block is not suitable", K(ret), K(block_id),
                "block_page_nums", t_mblk->get_max_cont_page_nums(), K(page_nums),
                "block_tenant_id", t_mblk->get_tenant_id(), K(tenant_id),
                "block_status", t_mblk->get_block_status());
  }
  return ret;
}

int ObTmpTenantMemBlockManager::GetAvailableBlockMapOp::operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *blk = entry.second;
  if (!is_found_) {
    if (OB_ISNULL(blk)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, blk is nullptr", K(ret), KP(blk));
    }  else if (tenant_id_ != blk->get_tenant_id() || dir_id_ != blk->get_dir_id()) {
      // do nothing
    } else {
      if (blk->get_max_cont_page_nums() < page_nums_
          || blk->get_block_status() != ObTmpMacroBlock::BlockStatus::MEMORY) {
        // do nothing
      } else {
        block_ = blk;
        is_found_ = true;
      }
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::get_available_macro_block(const int64_t dir_id, const uint64_t tenant_id,
                                                          const int64_t page_nums, ObTmpMacroBlock *&t_mblk,
                                                          bool &is_found)
{
  int ret = OB_SUCCESS;
  is_found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else {
    GetAvailableBlockMapOp op(dir_id, tenant_id, page_nums, t_mblk, is_found);
    if (OB_FAIL(t_mblk_map_.foreach_refactored(op))) {
      STORAGE_LOG(WARN, "get available macro block failed", K(ret));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::check_memory_limit()
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
  while (OB_SUCC(ret) && get_tenant_mem_block_num() < t_mblk_map_.size() && !wait_info_queue_.is_empty()) {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      STORAGE_LOG(ERROR, "fail to guard request condition", K(ret));
    } else {
      int64_t wait_ms = (timeout_ts - ObTimeUtility::current_time()) / 1000;
      while (OB_SUCC(ret)
          && get_tenant_mem_block_num() < t_mblk_map_.size()
          && !wait_info_queue_.is_empty()
          && wait_ms > 0) {
        if (OB_FAIL(cond_.wait(wait_ms))) {
          STORAGE_LOG(WARN, "fail to wait block write condition", K(ret), K(wait_ms));
        } else if (get_tenant_mem_block_num() < t_mblk_map_.size()) {
          wait_ms = (timeout_ts - ObTimeUtility::current_time()) / 1000;
        }
      }

      if (OB_SUCC(ret) && OB_UNLIKELY(wait_ms <= 0)) {
        ret = OB_TIMEOUT;
        STORAGE_LOG(WARN, "fail to wait block io condition due to spurious wakeup",
            K(ret), K(wait_ms), K(timeout_ts), K(ObTimeUtility::current_time()));
      }
    }
  }
  return ret;
}
bool ObTmpTenantMemBlockManager::check_block_full()
{
  return get_tenant_mem_block_num() < t_mblk_map_.size();
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

int ObTmpTenantMemBlockManager::cleanup()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "TmpFileRank"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpBlockCache has not been inited", K(ret));
  } else if (OB_FAIL(check_memory_limit())) {
    STORAGE_LOG(WARN, "fail to check memory limit", K(ret), K(t_mblk_map_.size()), K(get_tenant_mem_block_num()));
  } else {
    const int64_t wash_threshold = get_tenant_mem_block_num() * 0.8;
    Heap heap(compare_, &allocator);
    ChooseBlocksMapOp op(heap, ObTimeUtility::fast_current_time());
    const int64_t clean_nums = t_mblk_map_.size() - wash_threshold - ATOMIC_LOAD(&washing_count_);
    if (clean_nums <= 0) {
      STORAGE_LOG(DEBUG, "there is no need to wash blocks", K(ret), K(clean_nums));
    } else if (OB_FAIL(t_mblk_map_.foreach_refactored(op))) {
      STORAGE_LOG(WARN, "choose blks failed", K(ret));
    } else {
      const int64_t candidate_cnt = heap.count();
      bool wash_success = false;
      while (OB_SUCC(ret) && heap.count() > 0 && !wash_success) {
        const BlockInfo info = heap.top();
        ObIOWaitInfoHandle handle;
        if (OB_FAIL(wash_block(info.block_id_, handle))) {
          STORAGE_LOG(WARN, "fail to wash", K(ret), K_(tenant_id), K(info.block_id_));
        } else {
          wash_success = handle.is_valid();
          STORAGE_LOG(DEBUG, "succeed to wash block for cleanup", K(info));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(heap.pop())) {
            STORAGE_LOG(WARN, "pop info from heap failed", K(ret), K_(tenant_id));
          }
        }
      }
      if (OB_SUCC(ret) && !wash_success) {
        ret = OB_STATE_NOT_MATCH;
        STORAGE_LOG(WARN, "fail to cleanup", K(ret), K(t_mblk_map_.size()), K(candidate_cnt));
      }
    }
  }
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

int ObTmpTenantMemBlockManager::check_and_free_mem_block(ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  int64_t block_id = t_mblk->get_block_id();
  hash_val = murmurhash(&block_id, sizeof(block_id), hash_val);
  ObBucketHashWLockGuard lock_guard(map_lock_, hash_val);
  if (OB_FAIL(t_mblk->check_and_set_status(
          ObTmpMacroBlock::BlockStatus::MEMORY, ObTmpMacroBlock::BlockStatus::DISKED))) {
    if (OB_STATE_NOT_MATCH == ret) {
      STORAGE_LOG(DEBUG, "this block is washing", K(ret), K(*t_mblk));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "check and set status failed", K(ret), K(*t_mblk));
    }
  } else if (OB_FAIL(t_mblk->give_back_buf_into_cache())) {
    STORAGE_LOG(WARN, "fail to put tmp block cache", K(ret), K(t_mblk));
  } else if (OB_FAIL(free_macro_block(t_mblk->get_block_id()))) {
    STORAGE_LOG(WARN, "fail to free tmp macro block for block cache", K(ret));
  } else {
    tenant_store_.dec_block_cache_num(1);
  }
  return ret;
}

int ObTmpTenantMemBlockManager::ChooseBlocksMapOp::operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *blk = entry.second;
  if (OB_LIKELY(NULL != blk) && OB_LIKELY(blk->is_inited()) && OB_LIKELY(blk->is_memory())
      && OB_LIKELY(0 != blk->get_used_page_nums())) {
    BlockInfo info;
    info.block_id_ = blk->get_block_id();
    info.wash_score_ = blk->get_wash_score(cur_time_);
    if(OB_FAIL(heap_.push(info))) {
      STORAGE_LOG(WARN, "insert block to array failed", K(ret));
    }
  }
  STORAGE_LOG(DEBUG, "choose one block", K(ret), KPC(blk));
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
      if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1))) {
        STORAGE_LOG(WARN, "fail to set dir_to_blk_map_", K(ret), K(dir_id), K(t_mblk->get_block_id()));
      }
    }
  } else {
    ObTmpMacroBlock *dir_mblk = NULL;
    if (OB_FAIL(t_mblk_map_.get_refactored(block_id, dir_mblk))) {
      if (OB_HASH_NOT_EXIST == ret) {
        STORAGE_LOG(DEBUG, "the tmp macro block has been removed or washed", K(ret), K(block_id));
        if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1))) {
          STORAGE_LOG(WARN, "fail to set dir_to_blk_map_", K(ret), K(dir_id), K(t_mblk->get_block_id()));
        }
      } else {
        STORAGE_LOG(WARN, "fail to get block", K(ret), K(block_id));
      }
    } else if (dir_mblk->get_max_cont_page_nums() < t_mblk->get_max_cont_page_nums()) {
      if (OB_FAIL(dir_to_blk_map_.set_refactored(dir_id, t_mblk->get_block_id(), 1))) {
        STORAGE_LOG(WARN, "fail to set dir_to_blk_map_", K(ret), K(dir_id), K(t_mblk->get_block_id()));
      }
    }
  }

  return ret;
}

int ObTmpTenantMemBlockManager::get_block_and_set_washing(int64_t block_id, ObTmpMacroBlock *&m_blk)
{
  int ret = OB_SUCCESS;
  bool is_sealed = false;
  uint64_t hash_val = 0;
  hash_val = murmurhash(&block_id, sizeof(block_id), hash_val);
  ObBucketHashRLockGuard lock_guard(map_lock_, hash_val);
  if (OB_FAIL(t_mblk_map_.get_refactored(block_id, m_blk))) {
    STORAGE_LOG(DEBUG, "tenant mem block manager get block failed", K(ret), K(block_id));
  }  else if (OB_ISNULL(m_blk)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is null", K(ret), K(*m_blk));
  } else if (OB_UNLIKELY(!m_blk->is_inited())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "the block has not been inited", K(ret), K(*m_blk));
  } else if (OB_UNLIKELY(!m_blk->is_memory())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(DEBUG, "the block has been disked or washing", K(ret), K(*m_blk));
  } else if (m_blk->is_empty()) {
    if (OB_FAIL(refresh_dir_to_blk_map(m_blk->get_dir_id(), m_blk))) {
      STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*m_blk));
    }
    // refresh ret can be ignored. overwrite the ret.
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(m_blk->seal(is_sealed))) {
    STORAGE_LOG(WARN, "fail to seal block", K(ret), K(*m_blk));
  } else if (OB_UNLIKELY(!is_sealed)) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "the block has some unclosed extents", K(ret), K(is_sealed), K(*m_blk));
  } else if (OB_UNLIKELY(0 == m_blk->get_used_page_nums())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "the block write has not been finished", K(ret), K(*m_blk));
  } else if (OB_FAIL(m_blk->check_and_set_status(
              ObTmpMacroBlock::BlockStatus::MEMORY, ObTmpMacroBlock::BlockStatus::WASHING))) {
    if (OB_STATE_NOT_MATCH != ret) {
      STORAGE_LOG(WARN, "check and set status failed", K(ret), K(*m_blk));
    }
  }

  return ret;
}

int ObTmpTenantMemBlockManager::wash_block(const int64_t block_id, ObIOWaitInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  IOWaitInfo *wait_info = NULL;
  ObTmpMacroBlock *m_blk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the tenant mem block manager not been inited", K(ret));
  } else if (OB_FAIL(get_block_and_set_washing(block_id, m_blk))) {
    if (OB_HASH_NOT_EXIST == ret || OB_STATE_NOT_MATCH == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(DEBUG, "this block may be removed, washed or disked", K(ret), K(block_id));
    } else {
      STORAGE_LOG(WARN, "check and set washing failed", K(ret), K(block_id));
    }
  } else {
    ObTmpBlockIOInfo info;
    char *buf = NULL;
    SpinWLockGuard io_guard(io_lock_);
    ObMacroBlockHandle &mb_handle = m_blk->get_macro_block_handle();
    if (OB_FAIL(m_blk->get_wash_io_info(info))) {
      STORAGE_LOG(WARN, "fail to get wash io info", K(ret), K_(tenant_id), K(m_blk));
    } else if (OB_FAIL(write_io(info, mb_handle))) {
      STORAGE_LOG(WARN, "fail to write tmp block", K(ret), K_(tenant_id), K(info), K(*m_blk));
    } else if(OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(sizeof(IOWaitInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc io wait info memory", K(ret), K_(tenant_id));
    } else if (FALSE_IT(wait_info = new (buf) IOWaitInfo(mb_handle, *m_blk, *allocator_))) {
    } else if (OB_FAIL(wait_info->cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
      STORAGE_LOG(WARN, "fail to init condition", K(ret), K_(tenant_id));
    } else if (FALSE_IT(handle.set_wait_info(wait_info))) {
    } else if (OB_FAIL(wait_handles_map_.set_refactored(m_blk->get_block_id(), handle))) {
      STORAGE_LOG(WARN, "fail to set block into write_handles_map", K(ret), "block_id", m_blk->get_block_id());
    } else if (OB_FAIL(wait_info_queue_.push(wait_info))) {
      STORAGE_LOG(WARN, "fail to push back into write_handles", K(ret), K(wait_info));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(wait_handles_map_.erase_refactored(m_blk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to erase block from wait handles map", K(tmp_ret), K(m_blk->get_block_id()));
      }
    } else {
      ATOMIC_INC(&washing_count_);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(m_blk)) {
      mb_handle.reset();
      // don't release wait info unless ObIOWaitInfoHandle doesn't hold its ref
      if (OB_NOT_NULL(wait_info) && OB_ISNULL(handle.get_wait_info())) {
        wait_info->~IOWaitInfo();
        allocator_->free(wait_info);
        wait_info = nullptr;
      }
      handle.reset();
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(m_blk->check_and_set_status(ObTmpMacroBlock::BlockStatus::WASHING,
                                                  ObTmpMacroBlock::BlockStatus::MEMORY))) {
        STORAGE_LOG(ERROR, "fail to rollback block status", K(ret), K(tmp_ret));
      }
    }
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
    write_info.io_timeout_ms_ = io_info.io_timeout_ms_;
    write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    write_info.io_desc_.set_sys_module_id(ObIOModule::TMP_TENANT_MEM_BLOCK_IO);
    if (OB_FAIL(ObBlockManager::async_write_block(write_info, handle))) {
      STORAGE_LOG(WARN, "Fail to async write block", K(ret), K(write_info), K(handle));
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
        ObTmpFileStore::get_block_size());
      tenant_mem_block_num = bytes / ObTmpFileStore::get_block_size();
    }
    ATOMIC_STORE(&last_tenant_mem_block_num_, tenant_mem_block_num);
    ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
  }
  return tenant_mem_block_num;
}

int ObTmpTenantMemBlockManager::exec_wait()
{
  int ret = OB_SUCCESS;
  int64_t wait_io_cnt = 0;
  int64_t loop_nums = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (!stopped_) {
    common::ObSpLinkQueue::Link *node = NULL;
    SpinWLockGuard io_guard(io_lock_);
    const int64_t begin_us = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret) && (ObTimeUtility::fast_current_time() - begin_us)/1000 < TASK_INTERVAL) {
      IOWaitInfo *wait_info = NULL;
      if (OB_FAIL(wait_info_queue_.pop(node))) {
        if (OB_EAGAIN != ret) {
          STORAGE_LOG(WARN, "fail to pop wait info from queue", K(ret));
        }
      } else if (FALSE_IT(++loop_nums)) {
      } else if (OB_ISNULL(wait_info = static_cast<IOWaitInfo*>(node))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error, wait info is nullptr", K(ret), KP(node));
      } else if (OB_ISNULL(wait_info->block_handle_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error, macro handle in wait info is nullptr", K(ret), KPC(wait_info));
      } else {
        ObTmpMacroBlock &blk = wait_info->get_block();
        const MacroBlockId macro_id = wait_info->block_handle_->get_macro_id();
        const int64_t block_id = blk.get_block_id();
        const int64_t free_page_nums = blk.get_free_page_nums();
        if (OB_FAIL(wait_info->exec_wait())) {
          STORAGE_LOG(WARN, "fail to exec io handle wait", K(ret), K_(tenant_id), KPC(wait_info));
          ATOMIC_DEC(&washing_count_);
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(blk.check_and_set_status(ObTmpMacroBlock::WASHING, ObTmpMacroBlock::MEMORY))) {
            STORAGE_LOG(ERROR, "fail to rollback block status", K(ret), K(tmp_ret), K(block_id), K(blk));
          }
        } else {
          STORAGE_LOG(INFO, "start to wash a block", K(block_id), KPC(&blk));
          ObThreadCondGuard cond_guard(cond_);
          if (OB_FAIL(cond_guard.get_ret())) {
            STORAGE_LOG(WARN, "fail to guard request condition", K(ret));
          } else {
            ATOMIC_DEC(&washing_count_);
            if (OB_FAIL(blk.give_back_buf_into_cache(true/*set block disked for washed block*/))) {
              STORAGE_LOG(WARN, "fail to give back buf into cache", K(ret), K(block_id));
            } else if (OB_FAIL(t_mblk_map_.erase_refactored(block_id))) {
              if (OB_HASH_NOT_EXIST != ret) {
                STORAGE_LOG(WARN, "fail to erase t_mblk_map", K(ret), K(block_id));
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              ++wait_io_cnt;
              tenant_store_.dec_block_cache_num(1);
              ObTaskController::get().allow_next_syslog();
              STORAGE_LOG(INFO, "succeed to wash a block", K(block_id), K(macro_id),
                  K(free_page_nums), K(t_mblk_map_.size()));
            }
          }
        }
        wait_info->ret_code_ = ret;
        int64_t tmp_ret = OB_SUCCESS;
        // The broadcast() is executed regardless of success or failure, and the error code is ignored
        // so that the next request can be executed.
        if (OB_TMP_FAIL(wait_info->broadcast())) {
          STORAGE_LOG(ERROR, "signal io request condition failed", K(ret), K(tmp_ret), K(block_id));
        }
        // Regardless of success or failure, need to erase wait info handle from map.
        if (OB_TMP_FAIL(wait_handles_map_.erase_refactored(block_id))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            STORAGE_LOG(ERROR, "fail to erase wait handles map", K(ret), K(tmp_ret), K(block_id));
          }
        }
      }
    }
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(cond_.broadcast())) {
      STORAGE_LOG(ERROR, "signal wash condition failed", K(ret), K(tmp_ret));
    }
    if (loop_nums > 0 || REACH_TIME_INTERVAL(1000 * 1000L)/*1s*/) {
      const int64_t washing_count = ATOMIC_LOAD(&washing_count_);
      int64_t block_cache_num = -1;
      int64_t page_cache_num = -1;
      block_cache_num = tenant_store_.get_block_cache_num();
      page_cache_num = tenant_store_.get_page_cache_num();
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to do one round of tmp block io", K(ret), K(loop_nums),
          K(wait_io_cnt), K(washing_count), K(block_cache_num), K(page_cache_num));
    }
  }
  return ret;
}

int ObTmpTenantMemBlockManager::change_mem()
{
  int ret = OB_SUCCESS;
  // Here, this memory is used to store temporary file block metadata, which is related to the
  // datafile size. So, we set the upper limit of memory to be percentage (default, 70%) of tenant memory to
  // avoid excessive tenant memory, and affecting system stability. In theory, the limit
  // will be reached only when the tenant's memory is extremely small and the disk is extremely
  // large.
  tenant_store_.refresh_memory_limit(tenant_id_);
  return ret;
}

int ObTmpTenantMemBlockManager::wait_write_finish(const int64_t block_id, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  const int64_t wash_to_flush_wait_interval = 1000; // 1ms
  ObIOWaitInfoHandle handle;
  ObTmpMacroBlock *blk = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(timeout_ms), K(ret));
  } else if (OB_FAIL(t_mblk_map_.get_refactored(block_id, blk))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "cannot find tmp block", K(ret), K(block_id));
    }
  } else {
    bool is_found = false;
    while (OB_SUCC(ret) && blk->is_washing() && !is_found) {
      if (OB_FAIL(wait_handles_map_.get_refactored(block_id, handle))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "cannot find wait hanlde", K(ret), K(block_id));
        }
      } else {
        is_found = true;
      }
    }

    if (OB_SUCC(ret) && is_found) {
      if (OB_FAIL(handle.get_wait_info()->wait(timeout_ms))) {
        STORAGE_LOG(WARN, "wait write io finish failed", K(ret), K(block_id));
      }
    }
  }

  return ret;
}


}  // end namespace blocksstable
}  // end namespace oceanbase
