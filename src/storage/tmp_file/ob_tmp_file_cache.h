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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_CACHE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/queue/ob_link_queue.h"
#include "share/io/ob_io_manager.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpBlockCacheKey final : public common::ObIKVCacheKey
{
public:
  ObTmpBlockCacheKey();
  ObTmpBlockCacheKey(const int64_t block_id, const uint64_t tenant_id);
  ~ObTmpBlockCacheKey();
  bool operator ==(const ObIKVCacheKey &other) const override;
  uint64_t get_tenant_id() const override;
  uint64_t hash() const override;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  int64_t get_block_id() const { return block_id_; }
  TO_STRING_KV(K(block_id_), K(tenant_id_));

private:
  int64_t block_id_;
  uint64_t tenant_id_;
};

class ObTmpBlockCacheValue final : public common::ObIKVCacheValue
{
public:
  explicit ObTmpBlockCacheValue(char *buf);
  ~ObTmpBlockCacheValue();
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const { return NULL != buf_ && size() > 0; }
  char *get_buffer() { return buf_; }
  void set_buffer(char *buf) { buf_ = buf;}
  TO_STRING_KV(KP(buf_), K(size_));

private:
  char *buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCacheValue);
};

struct ObTmpBlockValueHandle final
{
public:
  ObTmpBlockValueHandle() : value_(NULL), handle_() {}
  ~ObTmpBlockValueHandle() = default;
  bool is_valid() const { return NULL != value_ && handle_.is_valid(); }
  void reset()
  {
    handle_.reset();
    value_ = NULL;
  }
  TO_STRING_KV(KP(value_), K(handle_));
  ObTmpBlockCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class ObTmpBlockCache final : public common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue>
{
public:
  typedef common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue> BasePageCache;
  static ObTmpBlockCache &get_instance();
  int init(const char *cache_name, const int64_t priority);
  void destroy();
  int get_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  int put_block(ObKVCacheInstHandle &inst_handle,
                ObKVCachePair *&kvpair,
                ObTmpBlockValueHandle &block_handle);
  int prealloc_block(const ObTmpBlockCacheKey &key,
                     ObKVCacheInstHandle &inst_handle,
                     ObKVCachePair *&kvpair,
                     ObTmpBlockValueHandle &block_handle);
private:
  ObTmpBlockCache() {}
  ~ObTmpBlockCache() {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCache);
};

class ObTmpPageCacheKey final : public common::ObIKVCacheKey
{
public:
  ObTmpPageCacheKey();
  ObTmpPageCacheKey(const int64_t block_id, const int64_t page_id, const uint64_t tenant_id);
  ~ObTmpPageCacheKey();
  bool operator ==(const ObIKVCacheKey &other) const override;
  uint64_t get_tenant_id() const override;
  uint64_t hash() const override;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  int64_t get_page_id() const { return page_id_; }
  int64_t get_block_id() const { return block_id_; }
  TO_STRING_KV(K(block_id_), K(page_id_), K(tenant_id_));

private:
  int64_t block_id_;
  int64_t page_id_;
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
  int direct_read(const blocksstable::MacroBlockId macro_block_id,
                  const int64_t read_size,
                  const int64_t begin_offset_in_block,
                  const common::ObIOFlag io_desc,
                  const int64_t io_timeout_ms,
                  common::ObIAllocator &callback_allocator,
                  blocksstable::ObMacroBlockHandle &mb_handle);
  // multi page cached_read
  int cached_read(const common::ObIArray<ObTmpPageCacheKey> &page_keys,
                  const blocksstable::MacroBlockId macro_block_id,
                  const int64_t begin_offset_in_block,
                  const common::ObIOFlag io_desc,
                  const int64_t io_timeout_ms,
                  common::ObIAllocator &callback_allocator,
                  blocksstable::ObMacroBlockHandle &mb_handle);
  int get_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle);
  int load_page(const ObTmpPageCacheKey &key,
                ObIAllocator *callback_allocator,
                ObTmpPageValueHandle &p_handle);
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
    TO_STRING_KV("callback_type:", "ObTmpCachedReadPageIOCallback", KP(data_buf_));
    DISALLOW_COPY_AND_ASSIGN(ObTmpCachedReadPageIOCallback);
  private:
    friend class ObTmpPageCache;
    common::ObArray<ObTmpPageCacheKey> page_keys_;
  };
  class ObTmpDirectReadPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpDirectReadPageIOCallback() : ObITmpPageIOCallback(ObIOCallbackType::TMP_DIRECT_READ_PAGE_CALLBACK) {}
    ~ObTmpDirectReadPageIOCallback() {}
    int64_t size() const override { return sizeof(*this); }
    int inner_process(const char *data_buffer, const int64_t size) override;
    TO_STRING_KV("callback_type:", "ObTmpDirectReadPageIOCallback", KP(data_buf_));
    DISALLOW_COPY_AND_ASSIGN(ObTmpDirectReadPageIOCallback);
  };
private:
  ObTmpPageCache() {}
  ~ObTmpPageCache() {}
  int inner_read_io_(const blocksstable::MacroBlockId macro_block_id,
                     const int64_t read_size,
                     const int64_t offset_in_block,
                     const common::ObIOFlag io_desc,
                     const int64_t io_timeout_ms,
                     ObITmpPageIOCallback *callback,
                     blocksstable::ObMacroBlockHandle &handle);
  int async_read_(const blocksstable::MacroBlockId macro_block_id,
                  const int64_t read_size,
                  const int64_t offset_in_block,
                  const common::ObIOFlag io_desc,
                  const int64_t io_timeout_ms,
                  ObITmpPageIOCallback *callback,
                  blocksstable::ObMacroBlockHandle &handle);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCache);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_CACHE_H_
