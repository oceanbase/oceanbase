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

#ifndef OCEANBASE_CLOG_OB_LOG_CACHE_
#define OCEANBASE_CLOG_OB_LOG_CACHE_

#include "share/cache/ob_kv_storecache.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace common {
class ObAddr;
}
namespace clog {

// ---------------- ObLogKVCache ----------------
class ObLogKVCacheKey : public common::ObIKVCacheKey {
public:
  ObLogKVCacheKey() : addr_(), seq_(0), file_id_(common::OB_INVALID_FILE_ID), offset_(clog::OB_INVALID_OFFSET)
  {}
  ObLogKVCacheKey(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id, const offset_t offset)
      : addr_(addr), seq_(seq), file_id_(file_id), offset_(offset)
  {}
  ~ObLogKVCacheKey()
  {}
  file_id_t get_file_id() const
  {
    return file_id_;
  }
  offset_t get_offset() const
  {
    return offset_;
  }
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const;
  TO_STRING_KV(K(addr_), K(seq_), K(file_id_), K(offset_));
  // Intentionally copyable and assignable
private:
  common::ObAddr addr_;
  int64_t seq_;
  file_id_t file_id_;
  offset_t offset_;
};

class ObLogKVCacheValue : public common::ObIKVCacheValue {
public:
  ObLogKVCacheValue() : buf_(NULL)
  {}
  explicit ObLogKVCacheValue(char* buf) : buf_(buf)
  {}
  ~ObLogKVCacheValue(){};
  const char* get_buf() const
  {
    return buf_;
  }
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const;
  TO_STRING_KV(KP(buf_));

private:
  char* buf_;
  DISALLOW_COPY_AND_ASSIGN(ObLogKVCacheValue);
};

class ObLogKVCache : public common::ObKVCache<ObLogKVCacheKey, ObLogKVCacheValue> {
public:
  ObLogKVCache() : is_inited_(false), log_type_(INVALID_TYPE), cache_name_()
  {}
  virtual ~ObLogKVCache()
  {
    destroy();
  }
  virtual int init(const char* cache_name, const int64_t priority);
  void destroy();
  int put_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id, const offset_t line_key_offset,
      const char* data_buf);
  int get_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id, const offset_t line_key_offset,
      common::ObKVCacheHandle& handle, const char*& line_buf);

private:
  enum LogType { CLOG_TYPE = 0, ILOG_TYPE = 1, INVALID_TYPE = 2 };
  bool is_inited_;
  LogType log_type_;
  char cache_name_[common::MAX_CACHE_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObLogKVCache);
};

// ---------------- ObLogHotCache ----------------
// HotCache maintains three offsets (take file_id into account):
//    base_offset_, head_offset_, tail_offset_
// [head_offset_ ~ tail_offset_] is the valid byte range of HotCache.
// When the offset of the read request is converted to pos on the RingBuffer, only base_offset_ is required.
// base_offset_ is set once in the initial HotCache, and will not change.
class ObLogHotCache {
  friend class ObHotCacheWarmUpHelper;

public:
  ObLogHotCache()
      : is_inited_(false),
        hot_cache_size_(0),
        hot_cache_mask_(0),
        byte_arr_(NULL),
        base_offset_(0),
        head_offset_(0),
        tail_offset_(0)
  {}
  ~ObLogHotCache()
  {
    destroy();
  }
  void destroy();
  int alloc_byte_arr(const int64_t hot_cache_size);
  int append_data(const file_id_t file_id, const offset_t offset, const char* new_data, const int64_t write_len);
  int read(const file_id_t file_id, const offset_t offset, const int64_t size, char* user_buf);
  TO_STRING_KV(K(is_inited_), KP(byte_arr_), K(base_offset_), K(head_offset_), K(tail_offset_));

private:
  inline int64_t get_pos(const int64_t offset) const
  {
    return (offset - base_offset_) & hot_cache_mask_;
  }
  // not used now
  inline int64_t get_pos_after_size(const offset_t offset, const int64_t size) const
  {
    return (offset + size - base_offset_) & hot_cache_mask_;
  }

private:
  bool is_inited_;
  // DirectReader requires hot cache at least one file size 64M
  int64_t hot_cache_size_;
  int64_t hot_cache_mask_;
  char* byte_arr_;       // Ring Buffer
  int64_t base_offset_;  // offset for byte_arr_[0] after warm up
  int64_t head_offset_;  // correspond to first byte of valid byte range
  int64_t tail_offset_;  // correspond to last byte of valid byte range
  DISALLOW_COPY_AND_ASSIGN(ObLogHotCache);
};

// ---------------- ObLogCache ----------------
class ObLogCache {
  friend class ObHotCacheWarmUpHelper;

public:
  ObLogCache() : hot_cache_(), kvcache_()
  {}
  ~ObLogCache()
  {
    destroy();
  }
  int init(const common::ObAddr& addr, const char* cache_name, const int64_t priority, const int64_t hot_cache_size);
  void destroy();
  int append_data(const common::ObAddr& addr, const char* new_data, const file_id_t file_id, const offset_t offset,
      const int64_t write_len);
  int hot_read(
      const common::ObAddr& addr, const file_id_t file_id, const offset_t offset, const int64_t size, char* user_buf);
  int put_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id, const offset_t line_key_offset,
      const char* data_buf)
  {
    return kvcache_.put_line(addr, seq, file_id, line_key_offset, data_buf);
  }

  int get_line(const common::ObAddr& addr, const int64_t seq, const file_id_t file_id, const offset_t line_key_offset,
      common::ObKVCacheHandle& handle, const char*& line_buf)
  {
    return kvcache_.get_line(addr, seq, file_id, line_key_offset, handle, line_buf);
  }

private:
  // Only the local log will enter the hot_cache_, currently only the local clog will use the hot cache, there will be
  // no others, hot_cache does not do special processing temporarily
  common::ObAddr self_addr_;
  ObLogHotCache hot_cache_;
  ObLogKVCache kvcache_;
  DISALLOW_COPY_AND_ASSIGN(ObLogCache);
};

}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CACHE_
