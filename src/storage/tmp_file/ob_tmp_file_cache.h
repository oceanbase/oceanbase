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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_CACHE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_CACHE_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase
{
namespace blocksstable
{
  class ObStorageObjectHandle;
  class MacroBlockId;
}
namespace tmp_file
{
class ObTmpPageCacheReadInfo;

class ObTmpPageCacheKey final : public common::ObIKVCacheKey
{
public:
  ObTmpPageCacheKey();
  ObTmpPageCacheKey(const ObTmpFileWriteCacheKey &page_key, const uint64_t tenant_id);
  ObTmpPageCacheKey(const int64_t fd,
                    const int64_t virtual_page_id,
                    const int64_t tenant_id);
  ObTmpPageCacheKey(const int64_t fd,
                    const int64_t tree_level,
                    const int64_t level_page_index,
                    const uint64_t tenant_id);
  ~ObTmpPageCacheKey();
  bool operator ==(const ObIKVCacheKey &other) const override;
  uint64_t get_tenant_id() const override;
  uint64_t hash() const override;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  OB_INLINE ObTmpFileWriteCacheKey get_page_key() const { return page_key_; }
  OB_INLINE void set_page_key(const ObTmpFileWriteCacheKey &page_key) { page_key_ = page_key; }
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
private:
  ObTmpFileWriteCacheKey page_key_;
  uint64_t tenant_id_;

};

class ObTmpPageCacheValue final : public common::ObIKVCacheValue
{
public:
  explicit ObTmpPageCacheValue(char *buf);
  ~ObTmpPageCacheValue();
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const { return NULL != buf_ && size() > 0; }
  char *get_buffer() { return buf_; }
  void set_buffer(char *buf) { buf_ = buf;}
  TO_STRING_KV(KP(buf_), K(size_));

private:
  char *buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCacheValue);
};

struct ObTmpPageValueHandle final
{
public:
  ObTmpPageValueHandle() : value_(NULL), handle_() {}
  ~ObTmpPageValueHandle() = default;
  void reset()
  {
    handle_.reset();
    value_ = NULL;
  }
  void move_from(ObTmpPageValueHandle& other)
  {
    this->handle_.move_from(other.handle_);
    this->value_ = other.value_;
    other.reset();
  }
  int assign(const ObTmpPageValueHandle& other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "failed to assign handle", K(ret));
      this->value_ = nullptr;
    } else {
      this->value_ = other.value_;
    }
    return ret;
  }
  TO_STRING_KV(KP(value_), K(handle_));
  ObTmpPageCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class ObTmpPageCache final : public common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>
{
public:
  typedef common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue> BasePageCache;
  static ObTmpPageCache &get_instance();
  int init(const char *cache_name, const int64_t priority);
  // only read disk pages
  int direct_read(ObTmpPageCacheReadInfo &read_info,
                  common::ObIAllocator &callback_allocator);
  // read disk pages and put all of them into cache
  int cached_read(const common::ObIArray<ObTmpPageCacheKey> &page_keys,
                  ObTmpPageCacheReadInfo &read_info,
                  common::ObIAllocator &callback_allocator);
  // read disk pages and put some of them into cache
  int aggregate_read(const common::ObIArray<std::pair<ObTmpPageCacheKey, int64_t>> &page_infos,
                     ObTmpPageCacheReadInfo &read_info,
                     common::ObIAllocator &callback_allocator);
  int get_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle);
  int load_page(const ObTmpPageCacheKey &key,
                const int64_t block_index,
                const int64_t physical_page_id,
                ObIAllocator *callback_allocator,
                ObTmpPageValueHandle &p_handle,
                const int64_t timeout_ms);
  void try_put_page_to_cache(const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value);
  void destroy();
public:
  class ObITmpPageIOCallback : public common::ObIOCallback
  {
  public:
    ObITmpPageIOCallback(const common::ObIOCallbackType type);
    virtual ~ObITmpPageIOCallback();
    virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override;
    const char *get_data() override { return data_buf_; }

  protected:
    friend class ObTmpPageCache;
    virtual int process_page(const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value);
    virtual ObIAllocator *get_allocator() { return allocator_; }

  protected:
    BasePageCache *cache_;
    common::ObIAllocator *allocator_;
    char *data_buf_;   // actual data buffer
  };

  class ObTmpCachedReadPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpCachedReadPageIOCallback();
    ~ObTmpCachedReadPageIOCallback();
    int64_t size() const override { return sizeof(*this); }
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_cb_name() const override { return "ObTmpCachedReadPageIOCallback"; }
    TO_STRING_KV("callback_type:", "ObTmpCachedReadPageIOCallback", KP(data_buf_));
    DISALLOW_COPY_AND_ASSIGN(ObTmpCachedReadPageIOCallback);
  private:
    friend class ObTmpPageCache;
    common::ObArray<ObTmpPageCacheKey> page_keys_;
  };

  class ObTmpAggregatePageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpAggregatePageIOCallback();
    ~ObTmpAggregatePageIOCallback();
    int64_t size() const override { return sizeof(*this); }
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_cb_name() const override { return "ObTmpAggregatePageIOCallback"; }
    TO_STRING_KV("callback_type:", "ObTmpAggregatePageIOCallback", KP(data_buf_));
    DISALLOW_COPY_AND_ASSIGN(ObTmpAggregatePageIOCallback);
  private:
    friend class ObTmpPageCache;
    // each pair is (page key, page offset in io buffer)
    common::ObArray<std::pair<ObTmpPageCacheKey, int64_t>> page_infos_;
  };

  class ObTmpDirectReadPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpDirectReadPageIOCallback() : ObITmpPageIOCallback(ObIOCallbackType::TMP_DIRECT_READ_PAGE_CALLBACK) {}
    ~ObTmpDirectReadPageIOCallback() {}
    int64_t size() const override { return sizeof(*this); }
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_cb_name() const override { return "ObTmpDirectReadPageIOCallback"; }
    TO_STRING_KV("callback_type:", "ObTmpDirectReadPageIOCallback", KP(data_buf_));
    DISALLOW_COPY_AND_ASSIGN(ObTmpDirectReadPageIOCallback);
  };
private:
  ObTmpPageCache() {}
  ~ObTmpPageCache() {}
  int inner_read_io_(ObTmpPageCacheReadInfo &read_info,
                     ObITmpPageIOCallback *callback);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCache);
};

struct ObTmpPageCacheReadInfo final
{
public:
  ObTmpPageCacheReadInfo()
    : is_inited_(false),
      macro_block_id_(),
      read_size_(0),
      begin_offset_(-1),
      io_desc_(),
      io_timeout_ms_(-1),
      object_handle_(nullptr) {}
  ~ObTmpPageCacheReadInfo() { reset(); }
  void reset();
  int init_read(const blocksstable::MacroBlockId &macro_block_id, const int64_t read_size, const int64_t begin_offset,
                common::ObIOFlag io_flag, const int64_t io_timeout_ms, blocksstable::ObStorageObjectHandle *handle);
  bool is_valid();
  int async_read(ObTmpPageCache::ObITmpPageIOCallback *callback);
  OB_INLINE int64_t get_begin_offset() const { return begin_offset_; }
  TO_STRING_KV(K(is_inited_), K(macro_block_id_), K(read_size_), K(begin_offset_),
               K(io_desc_), K(io_timeout_ms_), KP(object_handle_));
private:
  bool is_inited_;
  blocksstable::MacroBlockId macro_block_id_;
  int64_t read_size_;
  // the begin_offset is in the block
  int64_t begin_offset_;
  common::ObIOFlag io_desc_;
  int64_t io_timeout_ms_;
  blocksstable::ObStorageObjectHandle *object_handle_;
};
}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_CACHE_H_
