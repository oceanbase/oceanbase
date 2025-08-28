/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OB_EXTERNAL_DATA_PAGE_CACHE_H_
#define OB_EXTERNAL_DATA_PAGE_CACHE_H_

#include "share/ob_i_tablet_scan.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace sql
{

class ObExternalReadInfo;

class ObExternalDataPageCacheKey : public common::ObIKVCacheKey
{
public:
  ObExternalDataPageCacheKey(const char *url, const int64_t url_size, const char *content_digest,
                             const int64_t content_digest_size, const int64_t modify_time,
                             const int64_t page_size, const int64_t offset,
                             const uint64_t tenant_id) :
    url_(url),
    url_size_(url_size), content_digest_(content_digest), content_digest_size_(content_digest_size),
    modify_time_(modify_time), page_size_(page_size), offset_(offset), tenant_id_(tenant_id)
  {}
  ~ObExternalDataPageCacheKey() = default;
  bool is_valid() const;

  DECLARE_TO_STRING;
public: // override
  bool operator== (const ObIKVCacheKey &other) const override;
  uint64_t get_tenant_id() const override;
  uint64_t hash() const override;
  int64_t size() const override;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      ObIKVCacheKey *&key) const override;
  void advance_offset(const int64_t inc_count) { offset_ += inc_count; }
  int64_t get_page_size() const { return page_size_; }
private:
  bool is_valid_() const;
private:
  const char *url_;
  const int64_t url_size_;
  const char *content_digest_;
  const int64_t content_digest_size_;
  int64_t modify_time_;
  int64_t page_size_;
  int64_t offset_;
  uint64_t tenant_id_;
};

class ObExternalDataPageCacheValue : public common::ObIKVCacheValue
{
public:
  explicit ObExternalDataPageCacheValue(char *buf, const int64_t valid_data_size);
  ~ObExternalDataPageCacheValue();
  int64_t size() const override;
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      ObIKVCacheValue *&value) const override;
  bool is_valid() const { return NULL != buf_ && size() > 0; }
  char *get_buffer() { return buf_; }
  int64_t get_valid_data_size() const { return valid_data_size_; }
  void set_buffer(char *buf, const int64_t valid_data_size) { buf_ = buf; valid_data_size_ = valid_data_size; }
  TO_STRING_KV(KP(buf_), K(valid_data_size_));

private:
  char *buf_;
  int64_t valid_data_size_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalDataPageCacheValue);
};



class ObExternalDataPageCacheValueHandle final
{
public:
  ObExternalDataPageCacheValueHandle() : value_(NULL), handle_() {}
  ~ObExternalDataPageCacheValueHandle() = default;
  void reset()
  {
    handle_.reset();
    value_ = NULL;
  }
  TO_STRING_KV(KP(value_), K(handle_));
  ObExternalDataPageCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class ObExternalDataPageCache : public common::ObKVCache<ObExternalDataPageCacheKey, ObExternalDataPageCacheValue>
{
  typedef common::ObKVCache<ObExternalDataPageCacheKey, ObExternalDataPageCacheValue> BaseExPageCache;
  ObExternalDataPageCache() {}
  ~ObExternalDataPageCache() {}
public:
  const static int64_t MIN_PAGE_SIZE = 4 * 1024; // 4KB
  const static int64_t MAX_PAGE_SIZE = 2 * 1024 * 1024; // 2MB
  const static int64_t DEFAULT_PAGE_SIZE = 512 * 1024; // 512KB
  int init(
      const char *cache_name,
      const int64_t priority);
  static ObExternalDataPageCache &get_instance();
  int get_page(
      const ObExternalDataPageCacheKey &key,
      ObExternalDataPageCacheValueHandle &handle);
  void try_put_page_to_cache(
      const ObExternalDataPageCacheKey &key,
      const ObExternalDataPageCacheValue &value);
  void destroy();
  static int64_t get_page_size();
private:
  DISALLOW_COPY_AND_ASSIGN(ObExternalDataPageCache);
};

class ObExtCacheMissSegment final
{
public:
  ObExtCacheMissSegment():buf_(nullptr), offset_(-1), len_(-1) {}
  ~ObExtCacheMissSegment() { reset(); }
  int push_piece(
      char *buf,
      int64_t offset,
      int64_t len);
  int is_valid() const { return OB_NOT_NULL(buf_) && offset_ >=0 && len_ > 0; }
  bool reach_2MB_boundary() const { return (offset_ + len_) % DEFAULT_MACRO_BLOCK_SIZE == 0; }
  void reset() { buf_ = nullptr; offset_ = -1; len_ = -1; }
  TO_STRING_KV(KP_(buf), K_(offset), K_(len));
public:
  const char* get_buf() const { return buf_; }
  int64_t get_rd_offset() const { return  offset_; }
  int64_t get_rd_len() const { return len_; }
  int64_t get_page_count(const int64_t page_size) const;
  int64_t get_page_offset(const int64_t page_size) const;
private:
  char *buf_;
  int64_t offset_;
  int64_t len_;
};

class ObExCachedReadPageIOCallback final : public common::ObIOCallback
{
public:
  ObExCachedReadPageIOCallback(
      const ObExternalDataPageCacheKey &key,
      char *user_buffer,
      void *self_buffer,
      const int64_t user_buf_offset,
      const int64_t user_data_len,
      ObExternalDataPageCache *cache,
      ObIAllocator *alloc);
  ~ObExCachedReadPageIOCallback();
  int64_t size() const override { return sizeof(*this); }
  int inner_process(const char *data_buffer, const int64_t size) override;
  const char *get_cb_name() const { return "ObExCachedReadPageIOCallback"; }
  int alloc_data_buf(
      const char *io_data_buffer,
      const int64_t data_size) override;
  const char *get_data() override { return data_buf_; }
  int set_allocator(ObIAllocator *alloc);
  ObIAllocator *get_allocator() { return allocator_; }

  TO_STRING_KV(KP_(cache), KP_(allocator), KP_(data_buf), KP_(self_buf), K_(page_key));
  DISALLOW_COPY_AND_ASSIGN(ObExCachedReadPageIOCallback);
private:
  int inner_process_cache_pages_(
      const char *data_buffer,
      const int64_t size,
      const int64_t page_size);
  int inner_process_user_bufer_(
      const char *data_buffer,
      const int64_t size);
  int process_kv_(
      const ObExternalDataPageCacheKey &key,
      const ObExternalDataPageCacheValue &value);
private:
  ObExternalDataPageCacheKey page_key_;
  ObExternalDataPageCache *cache_; // kv_cache
  common::ObIAllocator *allocator_; // self_allocator, not user_buf allocator
  void *self_buf_; // alloc for page_cache_read, useless in the whole process but notify io_manager how long the buffer should be allocate
  char *data_buf_; // user buffer pointer
  int64_t data_offset_; // user_buffer start offset
  int64_t data_length_; // user_buffer length
};


} // namespace sql
} // namespace oceanbase
#endif // OB_EXTERNAL_DATA_PAGE_CACHE_H_
