/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "ob_external_file_access.h"
#include "ob_external_data_page_cache.h"

namespace oceanbase
{
namespace sql
{


/***************** ObExternalDataPageCacheKey ****************/
bool ObExternalDataPageCacheKey::operator== (const ObIKVCacheKey &other) const
{
  const ObExternalDataPageCacheKey &key = reinterpret_cast<const ObExternalDataPageCacheKey &>(other);
  return ObString(url_size_, url_) == ObString(key.url_size_, key.url_)
         && ObString(content_digest_size_, content_digest_)
              == ObString(key.content_digest_size_, key.content_digest_)
         && modify_time_ == key.modify_time_ && page_size_ == key.page_size_
         && offset_ == key.offset_ && tenant_id_ == key.tenant_id_;
}

uint64_t ObExternalDataPageCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObExternalDataPageCacheKey::hash() const
{
  uint64_t hash_val = ObString(url_size_, url_).hash();
  hash_val = murmurhash(content_digest_, content_digest_size_, hash_val);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&modify_time_, sizeof(modify_time_), hash_val);
  hash_val = murmurhash(&page_size_, sizeof(page_size_), hash_val);
  hash_val = murmurhash(&offset_, sizeof(offset_), hash_val);
  return hash_val;
}

int64_t ObExternalDataPageCacheKey::size() const
{
  return sizeof(*this) + url_size_ + content_digest_size_;
}

int ObExternalDataPageCacheKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid page cache key, ", KPC(this), KR(ret));
  } else {
    char *url = buf + sizeof(*this);
    MEMCPY(url, url_, url_size_); // copy url_
    char *content_digest = url + url_size_;
    MEMCPY(content_digest, content_digest_, content_digest_size_);
    key = new (buf) ObExternalDataPageCacheKey(url, url_size_, content_digest, content_digest_size_,
                                               modify_time_, page_size_, offset_, tenant_id_);
  }
  return ret;
}


bool ObExternalDataPageCacheKey::is_valid_() const
{
  return OB_LIKELY(offset_ >= 0 &&
                   0 == offset_ % page_size_ &&
                   page_size_ > 0 &&
                   tenant_id_ != OB_INVALID_TENANT_ID &&
                   url_ != nullptr &&
                   url_size_ > 0 &&
                   size() > 0);
}

bool ObExternalDataPageCacheKey::is_valid() const
{
  return OB_LIKELY(is_valid_());
}

DEF_TO_STRING(ObExternalDataPageCacheKey) {
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(offset), K_(modify_time), K_(page_size), K_(tenant_id));
  J_COMMA();
  if (nullptr != url_ && url_size_ > 0) {
    ObString url(url_size_, url_);
    J_KV(K(url));
  } else {
    databuff_printf(buf, buf_len, pos, "null url");
  }
  J_OBJ_END();
  return pos;
};

/* -------------------------- ObExternalDataPageCacheValue --------------------------- */
ObExternalDataPageCacheValue::ObExternalDataPageCacheValue(char *buf,
                                                           const int64_t valid_data_size) :
  buf_(buf),
  valid_data_size_(valid_data_size)
{}

ObExternalDataPageCacheValue::~ObExternalDataPageCacheValue()
{
}

int64_t ObExternalDataPageCacheValue::size() const
{
  return sizeof(*this) + valid_data_size_;
}

int ObExternalDataPageCacheValue::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), KP(buf), K(buf_len),
                      "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid external page cache value", KR(ret));
  } else {
    ObExternalDataPageCacheValue *page_cache_value =
      new (buf) ObExternalDataPageCacheValue(buf + sizeof(*this), valid_data_size_);
    MEMCPY(buf + sizeof(*this), buf_, size() - sizeof(*this));
    value = page_cache_value;
  }
  return ret;
}


/***************** ObExternalDataPageCache ****************/

int ObExternalDataPageCache::init(
    const char *cache_name,
    const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((BaseExPageCache::init(
      cache_name, priority)))) {
    STORAGE_LOG(WARN, "Fail to init kv cache, ", KR(ret));
  }
  return ret;
}

ObExternalDataPageCache &ObExternalDataPageCache::get_instance()
{
  static ObExternalDataPageCache instance_;
  return instance_;
}

int ObExternalDataPageCache::get_page(
    const ObExternalDataPageCacheKey &key,
    ObExternalDataPageCacheValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObExternalDataPageCacheValue *value = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      EVENT_INC(ObStatEventIds::EXDATA_PAGE_CACHE_MISS);
    } else {
      STORAGE_LOG(WARN, "fail to get key from page cache", KR(ret), K(key));
    }
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, the value must not be NULL", KR(ret));
    } else {
      handle.value_ = const_cast<ObExternalDataPageCacheValue *>(value);
      EVENT_INC(ObStatEventIds::EXDATA_PAGE_CACHE_HIT);
    }
  }
  return ret;
}

void ObExternalDataPageCache::try_put_page_to_cache(
    const ObExternalDataPageCacheKey &key,
    const ObExternalDataPageCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, false/*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put tmp page into cache", KR(ret), K(key), K(value));
  } else {
    // refresh the page cache score by calling get_page() to prevent eviction,
    // otherwise its score is 0 and may be evicted immediately
    ObExternalDataPageCacheValueHandle handle;
    get_page(key, handle);
  }
}

void ObExternalDataPageCache::destroy()
{
  common::ObKVCache<ObExternalDataPageCacheKey, ObExternalDataPageCacheValue>::destroy();
}

int64_t ObExternalDataPageCache::get_page_size()
{
  int64_t page_size = ObExternalDataPageCache::DEFAULT_PAGE_SIZE;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    page_size = tenant_config->_external_table_mem_cache_page_size;
    const size_t mask = 4095; // 0xFFF
    page_size = (page_size + mask) & ~mask;
    if (page_size < ObExternalDataPageCache::MIN_PAGE_SIZE) {
      page_size = ObExternalDataPageCache::MIN_PAGE_SIZE;
    } else if (page_size > ObExternalDataPageCache::MAX_PAGE_SIZE) {
      page_size = ObExternalDataPageCache::MAX_PAGE_SIZE;
    }
  }
  return page_size;
}

/***************** ObExCachedReadPageIOCallback ****************/
int ObExCachedReadPageIOCallback::alloc_data_buf(
    const char *io_data_buffer,
    const int64_t data_size)
{
  // should not call this function;
  return OB_ERR_UNEXPECTED;
}

int ObExCachedReadPageIOCallback::set_allocator(ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(allocator_) && allocator_ != alloc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("double set allocator", K(ret), K(allocator_), K(alloc));
  } else {
    allocator_ = alloc;
  }
  return ret;
}

int ObExCachedReadPageIOCallback::process_kv_(
    const ObExternalDataPageCacheKey &key,
    const ObExternalDataPageCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", KR(ret), K(key), K(value));
  } else if (OB_FAIL(cache_->put(key, value, true/*overwrite*/))) {
    STORAGE_LOG(WARN, "fail to put external page into cache", KR(ret), K(key), K(value));
  } else {
    // refresh the page cache score by calling get_page() to prevent eviction,
    // otherwise its score is 0 and may be evicted immediately
    ObExternalDataPageCacheValueHandle handle;
    cache_->get_page(key, handle);
  }
  return ret;
}

ObExCachedReadPageIOCallback::ObExCachedReadPageIOCallback(
    const ObExternalDataPageCacheKey &key,
    char *user_buffer,
    void *self_buffer,
    const int64_t user_buf_offset,
    const int64_t user_data_len,
    ObExternalDataPageCache *cache,
    ObIAllocator *alloc)
  : common::ObIOCallback(ObIOCallbackType::EXTERNAL_DATA_CACHED_READ_CALLBACK), page_key_(key),
    cache_(cache), allocator_(alloc), self_buf_(self_buffer),
    data_buf_(user_buffer), data_offset_(user_buf_offset), data_length_(user_data_len)
  {}

ObExCachedReadPageIOCallback::~ObExCachedReadPageIOCallback()
{
  if (nullptr != allocator_ && nullptr != self_buf_) {
    allocator_->free(self_buf_);
    self_buf_ = nullptr;
  }
  allocator_ = nullptr;
  self_buf_ = nullptr;
  data_buf_ = nullptr;
}


int ObExCachedReadPageIOCallback::inner_process(
    const char *data_buffer, // this buffer is allocate by io_manager, should not free by callback_self
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObExCachedReadPageIOCallback", 100000); //100ms
  if (OB_ISNULL(cache_) || OB_ISNULL(allocator_) || OB_ISNULL(data_buf_) || data_offset_ < 0 || data_length_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid external page cache callback allocator", KR(ret), KP(cache_), KP(allocator_));
  } else if (OB_UNLIKELY(size <= 0 || OB_ISNULL(data_buffer) || data_length_ + data_offset_ > size)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", KR(ret), K(size), KP(data_buffer));
  } else if (OB_FAIL(inner_process_user_bufer_(data_buffer, size))) {
    LOG_WARN("failed to process kv", K(ret), KP(data_buffer), K(size));
  } else if (FALSE_IT(time_guard.click("process_user_buf_"))) {
  } else if (OB_FAIL(inner_process_cache_pages_(data_buffer, size, page_key_.get_page_size()))) {
    LOG_WARN("failed to process kv", K(ret), KP(data_buffer), K(size));
  } else if (FALSE_IT(time_guard.click("process_kv_"))) {
  }

  return ret;
}

int ObExCachedReadPageIOCallback::inner_process_cache_pages_(
    const char *data_buffer,
    const int64_t valid_size,
    const int64_t page_size)
{
  int ret = OB_SUCCESS;
  ObExternalDataPageCacheValue value(nullptr, 0);
  int64_t page_count = valid_size / page_size + (valid_size % page_size == 0 ? 0 : 1);
  if (OB_UNLIKELY(valid_size <= 0 || OB_ISNULL(data_buffer))) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", KR(ret), K(valid_size), KP(data_buffer));
  } else if (OB_UNLIKELY(data_length_ + data_offset_ > valid_size)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", KR(ret), K(valid_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < page_count; i++) {
      const int64_t cur_valid_size = i == (page_count - 1) ?
                                    (valid_size % page_size == 0 ? page_size : valid_size % page_size) :
                                    (page_size);
      if (FALSE_IT(value.set_buffer(const_cast<char*>(data_buffer + (i * page_size)), cur_valid_size))) {
      } else if (OB_FAIL(process_kv_(page_key_, value))) {
        STORAGE_LOG(WARN, "fail to process external page cache in callback", KR(ret));
      } else if (FALSE_IT(page_key_.advance_offset(page_size))) {
      }
    }
  }
  return ret;
}

int ObExCachedReadPageIOCallback::inner_process_user_bufer_(
    const char *data_buffer,
    const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0 && OB_ISNULL(data_buffer))) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", KR(ret), K(size), KP(data_buffer));
  } else if (OB_UNLIKELY(data_length_ + data_offset_ > size || OB_ISNULL(data_buf_))) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data buffer size", KR(ret), K(size));
  } else {
    MEMCPY(data_buf_, data_buffer + data_offset_, data_length_);
    STORAGE_LOG(DEBUG, "inner_process user_buf", K(ret), KP(data_buf_), K(data_offset_), K(data_length_));
  }
  return ret;
}

/***************** ObExtCacheMissSegment ****************/
int ObExtCacheMissSegment::push_piece(
    char *buf,
    const int64_t offset,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || offset < 0 || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(offset), K(len));
  } else if (is_valid() && buf_ + len_ != buf) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", K(ret), KP(buf), KP(buf_), K(offset), K(len));
  } else if (is_valid() && buf_ + len_ == buf) {
    len_ += len;
  } else if (!is_valid()) {
    buf_ = buf;
    offset_ = offset;
    len_ = len;
  }
  return ret;
}

int64_t ObExtCacheMissSegment::get_page_count(const int64_t page_size) const
{
  int64_t len = (offset_ % page_size) + len_;
  int64_t page_count = len % page_size == 0 ? (len / page_size) : (len / page_size + 1);
  return page_count;
}

int64_t ObExtCacheMissSegment::get_page_offset(const int64_t page_size) const
{
  return offset_ - (offset_ % page_size);
}


} // namespace sql
} // namespace oceanbase
