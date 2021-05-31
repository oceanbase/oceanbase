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

#include "ob_log_cache.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/ob_running_mode.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
using namespace common;
namespace clog {

// ---------------- ObLogKVCache ----------------
bool ObLogKVCacheKey::operator==(const ObIKVCacheKey& other) const
{
  const ObLogKVCacheKey& other_key = reinterpret_cast<const ObLogKVCacheKey&>(other);
  return (addr_ == other_key.addr_) && (seq_ == other_key.seq_) && (file_id_ == other_key.file_id_) &&
         (offset_ == other_key.offset_);
}

uint64_t ObLogKVCacheKey::get_tenant_id() const
{
  return common::OB_SYS_TENANT_ID;
}

uint64_t ObLogKVCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObLogKVCacheKey), 0);
}

int64_t ObLogKVCacheKey::size() const
{
  return sizeof(*this);
}

int ObLogKVCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObLogKVCacheKey* key_pointer = new (buf) ObLogKVCacheKey();
    *key_pointer = *this;
    key = key_pointer;
  }
  return ret;
}

int64_t ObLogKVCacheValue::size() const
{
  return sizeof(*this) + CLOG_CACHE_SIZE;
}

int ObLogKVCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf)) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "buf_len is not enough", K(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObLogKVCacheValue* value_ptr = new (buf) ObLogKVCacheValue();
    value_ptr->buf_ = buf + sizeof(*this);
    if (NULL != buf_) {
      MEMCPY(value_ptr->buf_, buf_, CLOG_CACHE_SIZE);
    }
    value = value_ptr;
  }
  return ret;
}

int ObLogKVCache::init(const char* cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  // args validation is done by KVCache
  if (OB_SUCCESS != (ret = ObKVCache<ObLogKVCacheKey, ObLogKVCacheValue>::init(cache_name, priority))) {
    CLOG_LOG(WARN, "ObKVCache init error", K(ret), K(cache_name), K(priority));
  } else {
    if (0 == STRNCMP("clog_cache", cache_name, MAX_CACHE_NAME_LENGTH)) {
      log_type_ = CLOG_TYPE;
    } else if (0 == STRNCMP("index_clog_cache", cache_name, MAX_CACHE_NAME_LENGTH)) {
      log_type_ = ILOG_TYPE;
    } else {
      log_type_ = INVALID_TYPE;
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "ObLogKVCache init failed", K(ret), K(cache_name), K(priority));
    }
    (void)snprintf(cache_name_, MAX_CACHE_NAME_LENGTH, "%s", cache_name);
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogKVCache init success", K(cache_name), K(priority), K(log_type_), K(this));
  }
  return ret;
}

void ObLogKVCache::destroy()
{
  ObKVCache<ObLogKVCacheKey, ObLogKVCacheValue>::destroy();
}

int ObLogKVCache::put_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id,
    const offset_t line_key_offset, const char* data_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(0 > seq) || OB_UNLIKELY(!is_valid_file_id(file_id)) ||
             OB_UNLIKELY(CLOG_TYPE == log_type_ && !is_valid_offset(line_key_offset)) ||
             OB_UNLIKELY(ILOG_TYPE == log_type_ && line_key_offset < 0) ||
             OB_UNLIKELY(!is_offset_align(line_key_offset, CLOG_CACHE_SIZE)) || OB_UNLIKELY(NULL == data_buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument when put line",
        K(ret),
        K(file_id),
        K(addr),
        K(log_type_),
        K(seq),
        K(line_key_offset),
        KP(data_buf));
  } else {
    const bool overwrite = false;
    ObLogKVCacheKey key(addr, seq, file_id, line_key_offset);
    ObLogKVCacheValue value(const_cast<char*>(data_buf));
    if (OB_FAIL(put(key, value, overwrite))) {
      if (OB_ENTRY_EXIST != ret) {
        CLOG_LOG(WARN, "KVCache put line error", K(ret), K(key), K(value));
      }
    } else {
      CLOG_LOG(TRACE, "KVCache put line success", K(ret), K(key), K(value));
    }
  }
  return ret;
}

int ObLogKVCache::get_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id,
    const offset_t line_key_offset, ObKVCacheHandle& handle, const char*& line_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(0 > seq) || OB_UNLIKELY(!is_valid_file_id(file_id)) ||
             OB_UNLIKELY(log_type_ == CLOG_TYPE && !is_valid_offset(line_key_offset)) ||
             OB_UNLIKELY(log_type_ == ILOG_TYPE && line_key_offset < 0) ||
             OB_UNLIKELY(!is_offset_align(line_key_offset, CLOG_CACHE_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument when get line",
        K(ret),
        K(file_id),
        K(addr),
        K(log_type_),
        K(seq),
        K(line_key_offset),
        K(this));
  } else {
    ObLogKVCacheKey key(addr, seq, file_id, line_key_offset);
    const ObLogKVCacheValue* value = NULL;
    if (OB_SUCC(get(key, value, handle))) {
      line_buf = value->get_buf();
      switch (log_type_) {
        case CLOG_TYPE:
          EVENT_INC(ObStatEventIds::CLOG_CACHE_HIT);
          break;
        case ILOG_TYPE:
          EVENT_INC(ObStatEventIds::INDEX_CLOG_CACHE_HIT);
          break;
        default:
          CLOG_LOG(WARN, "unknown log cache type", K_(log_type));
          break;
      }
    } else if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      CLOG_LOG(TRACE, "KVCache do not contain", K(ret), K(key), K(handle));
      switch (log_type_) {
        case CLOG_TYPE:
          EVENT_INC(ObStatEventIds::CLOG_CACHE_MISS);
          break;
        case ILOG_TYPE:
          EVENT_INC(ObStatEventIds::INDEX_CLOG_CACHE_MISS);
          break;
        default:
          CLOG_LOG(WARN, "unknown log cache type", K_(log_type));
          break;
      }
    } else {
      CLOG_LOG(WARN, "KVCache get error", K(ret), K(key), K(handle));
    }
  }
  return ret;
}

// ---------------- ObLogHotCache ----------------
int ObLogHotCache::alloc_byte_arr(const int64_t hot_cache_size)
{
  int ret = OB_SUCCESS;

  const int64_t server_hot_cache_limit =
      ObMallocAllocator::get_instance()->get_tenant_limit(OB_SERVER_TENANT_ID) / 100 * HOT_CACHE_MEM_PERCENT;
  int64_t hot_cache_upper_limit = std::min(HOT_CACHE_UPPER_HARD_LIMIT, server_hot_cache_limit);
  // hot_cache_size is calculated according to 5% of 500 tenant memory, min 256M, max 1G
  hot_cache_size_ = std::max(HOT_CACHE_LOWER_HARD_LIMIT, hot_cache_upper_limit);

  if (lib::is_mini_mode()) {
    const int64_t MINI_MODE_HOT_CACHE_HARD_LIMIT = 1L << 26;  // 64M
    hot_cache_size_ = MINI_MODE_HOT_CACHE_HARD_LIMIT;
  } else {
    hot_cache_size_ = hot_cache_size;
  }
  hot_cache_mask_ = (hot_cache_size_ - 1);
  ObMemAttr hot_cache_mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_LOG_HOT_CACHE);
  byte_arr_ = (char*)ob_malloc(hot_cache_size_, hot_cache_mem_attr);
  if (NULL == byte_arr_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc log hot cache error", K(ret), LITERAL_K(hot_cache_size_));
  } else {
    // memset(byte_arr_, 'C', hot_cache_size_);
    is_inited_ = false;  // true after warm up
    CLOG_LOG(INFO, "hot cache alloc succ", K(hot_cache_mem_attr), LITERAL_K(hot_cache_size_));
  }
  return ret;
}

void ObLogHotCache::destroy()
{
  is_inited_ = false;
  if (NULL != byte_arr_) {
    ob_free(byte_arr_);
    byte_arr_ = NULL;
  }
  base_offset_ = 0;
  head_offset_ = 0;
  tail_offset_ = 0;
  CLOG_LOG(INFO, "ObLogHotCache destroy");
}

// After log_writer flushed successfully and updated the cache, it returned to the caller.
// It is guaranteed by log_writer that when the cache is updated, it will always continue to append from the last offset
// without holes Agreed at most 1.875M data each time
int ObLogHotCache::append_data(
    const file_id_t file_id, const offset_t offset, const char* new_data, const int64_t write_len)
{
  // Except for warmup, only append_data function will change head_offset_/tail_offset_ (single thread)
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id)) || OB_UNLIKELY(!is_valid_offset(offset)) ||
             OB_UNLIKELY(NULL == new_data) || OB_UNLIKELY(write_len <= 0) ||
             OB_UNLIKELY(write_len > OB_MAX_LOG_BUFFER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "HotCache append data error", K(ret), K(file_id), K(offset), KP(new_data), K(write_len));
  } else if (get_long_offset(file_id, offset) != ATOMIC_LOAD(&tail_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "tail_offset_ not match log writer",
        K(ret),
        K(file_id),
        K(offset),
        K(write_len),
        "tail_offset_",
        ATOMIC_LOAD(&tail_offset_));
  } else {
    // op seq:
    // 1. make victim byte range unavailable
    // 2. copy data
    // 3. make new byte range available

    if (OB_LIKELY(ATOMIC_LOAD(&tail_offset_) - ATOMIC_LOAD(&head_offset_) + write_len > hot_cache_size_)) {
      // head_offset_ overlapped
      // just advance head_offset_ by write_len(definitely enough),
      // in which case, a whole may left in byte_arr_, it does not matter
      ATOMIC_FAA(&head_offset_, write_len);
    } else {
      // head_offset_ not overlapped, no victim range,
      // do nothing, just keep head_offset_ unchanged
    }
    const int64_t old_tail_pos = get_pos(ATOMIC_LOAD(&tail_offset_));
    if (OB_LIKELY(old_tail_pos + write_len <= hot_cache_size_)) {
      // not wrapped
      MEMCPY(byte_arr_ + old_tail_pos, new_data, write_len);
    } else {
      // wrapped
      const int64_t size_before_wrap = hot_cache_size_ - old_tail_pos;
      MEMCPY(byte_arr_ + old_tail_pos, new_data, size_before_wrap);
      MEMCPY(byte_arr_, new_data + size_before_wrap, write_len - size_before_wrap);
    }
    ATOMIC_FAA(&tail_offset_, write_len);
    CLOG_LOG(TRACE,
        "HotCache cache append data",
        K(file_id),
        K(offset),
        K(write_len),
        K(base_offset_),
        K(head_offset_),
        K(tail_offset_),
        K(base_offset_),
        K(old_tail_pos));
  }
  return ret;
}

// All read requests are restricted by log_writer.tail at the direct_reader level
int ObLogHotCache::read(const file_id_t file_id, const offset_t offset, const int64_t size, char* user_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id)) || OB_UNLIKELY(!is_valid_offset(offset)) ||
             OB_UNLIKELY(size <= 0) || OB_UNLIKELY(size > OB_MAX_LOG_BUFFER_SIZE) || OB_UNLIKELY(NULL == user_buf)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument when log hot cache read", K(ret), K(file_id), K(offset), K(size), KP(user_buf));
  } else {
    const int64_t user_offset = get_long_offset(file_id, offset);
    const int64_t user_pos = get_pos(user_offset);
    if (user_offset < ATOMIC_LOAD(&head_offset_)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      if (user_pos + size <= hot_cache_size_) {
        // not wrapped
        MEMCPY(user_buf, byte_arr_ + user_pos, size);
        // CLOG_LOG(TRACE, "HotCache read copy", K(user_offset), K(user_pos), K(size));
      } else {
        // wrapped
        const int64_t front_half_size = hot_cache_size_ - user_pos;
        MEMCPY(user_buf, byte_arr_ + user_pos, front_half_size);
        MEMCPY(user_buf + front_half_size, byte_arr_, size - front_half_size);
        // CLOG_LOG(TRACE, "HotCache read copy", K(user_offset), K(front_half_size), K(user_pos),
        // K(size), K(head_offset_), K(tail_offset_), K(base_offset_));
      }
      if (user_offset < ATOMIC_LOAD(&head_offset_)) {
        // double check the wanted range is still available
        ret = OB_ENTRY_NOT_EXIST;
        CLOG_LOG(TRACE,
            "HotCache read double check fail",
            K(user_offset),
            K(size),
            K(head_offset_),
            K(tail_offset_),
            K(base_offset_));
      }
    }
    CLOG_LOG(TRACE,
        "HotCache read",
        K(ret),
        K(file_id),
        K(offset),
        K(user_offset),
        K(user_pos),
        KP(user_buf),
        K(head_offset_),
        K(tail_offset_),
        K(base_offset_));
  }
  return ret;
}

int ObLogCache::init(
    const common::ObAddr& addr, const char* cache_name, const int64_t priority, const int64_t hot_cache_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid()) || OB_ISNULL(cache_name)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KR(ret), KP(cache_name), K(priority), K(hot_cache_size));
  } else if (OB_FAIL(kvcache_.init(cache_name, priority))) {
    CLOG_LOG(WARN, "log kvcache init error", K(ret), K(cache_name), K(priority));
  } else if (OB_FAIL(hot_cache_.alloc_byte_arr(hot_cache_size))) {
    CLOG_LOG(WARN, "hot_cache_ alloc byte arr error", K(ret));
  } else {
    self_addr_ = addr;
    CLOG_LOG(INFO, "ObLogCache init success", K(addr), K(cache_name), K(priority), K(hot_cache_size));
  }
  return ret;
}

int ObLogCache::append_data(const common::ObAddr& addr, const char* new_data, const file_id_t file_id,
    const offset_t offset, const int64_t write_len)
{
  int ret = OB_SUCCESS;
  if (addr != self_addr_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected addr", KR(ret), K(addr), K(self_addr_));
  } else {
    ret = hot_cache_.append_data(file_id, offset, new_data, write_len);
  }
  return ret;
}

int ObLogCache::hot_read(
    const ObAddr& addr, const file_id_t file_id, const offset_t offset, const int64_t size, char* user_buf)
{
  int ret = OB_SUCCESS;
  if (addr != self_addr_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = hot_cache_.read(file_id, offset, size, user_buf);
  }
  return ret;
}

void ObLogCache::destroy()
{
  hot_cache_.destroy();
  kvcache_.destroy();
}

}  // end namespace clog
}  // end namespace oceanbase
