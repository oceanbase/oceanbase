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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_META_CACHE_H_
#define OCEANBASE_STORAGE_OB_STORAGE_META_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{

namespace blocksstable
{
class ObSSTable;
}

namespace share
{
class ObTabletAutoincSeq;
}

namespace storage
{

class ObTablet;
class ObTabletTableStore;
class ObStorageMetaCache;
class ObStorageMetaValueHandle;

class ObStorageMetaKey final : public common::ObIKVCacheKey
{
public:
  ObStorageMetaKey();
  ObStorageMetaKey(
      const uint64_t tenant_id,
      const ObMetaDiskAddr &phy_addr);
  ObStorageMetaKey(
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &block_id,
      const int64_t offset,
      const int64_t size);
  virtual ~ObStorageMetaKey();
  virtual bool operator ==(const ObIKVCacheKey &other) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  const ObMetaDiskAddr &get_meta_addr() const;
  TO_STRING_KV(K_(tenant_id), K_(phy_addr));
private:
  uint64_t tenant_id_;
  ObMetaDiskAddr phy_addr_;
};

class ObStorageMetaValue final : public common::ObIKVCacheValue
{
public:
  enum MetaType : uint16_t
  {
    SSTABLE        = 0,
    TABLE_STORE    = 1,
    AUTO_INC_SEQ   = 2,
    MAX            = 3,
  };
public:
  ObStorageMetaValue();
  ObStorageMetaValue(const MetaType type, ObIStorageMetaObj *buf);
  virtual ~ObStorageMetaValue();
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  int get_sstable(const blocksstable::ObSSTable *&sstable) const;
  int get_sstable(blocksstable::ObSSTable *&sstable) const;
  int get_table_store(const ObTabletTableStore *&store) const;
  int get_autoinc_seq(const share::ObTabletAutoincSeq *&seq) const;
  bool is_valid() const;
  void reset()
  {
    type_ = MetaType::MAX;
    if (OB_LIKELY(nullptr != obj_)) {
      obj_->~ObIStorageMetaObj();
    }
    obj_ = nullptr;
  }
  static int process_sstable(
      ObStorageMetaValueHandle &handle,
      const ObStorageMetaKey &key,
      const char *buf,
      const int64_t size,
      const ObTablet *tablet);
  static int process_table_store(
      ObStorageMetaValueHandle &handle,
      const ObStorageMetaKey &key,
      const char *buf,
      const int64_t size,
      const ObTablet *tablet);
  static int process_autoinc_seq(
      ObStorageMetaValueHandle &handle,
      const ObStorageMetaKey &key,
      const char *buf,
      const int64_t size,
      const ObTablet *tablet);
  TO_STRING_KV(K_(type), KP_(obj));
public:
  typedef int (*StorageMetaProcessor)(
      ObStorageMetaValueHandle &,
      const ObStorageMetaKey &,
      const char*,
      const int64_t,
      const ObTablet *);
  typedef int (*StorageMetaBypassProcessor)(
      const MetaType,
      common::ObSafeArenaAllocator &,
      ObStorageMetaValueHandle &,
      const char *,
      const int64_t);
  static StorageMetaProcessor processor[MetaType::MAX];
  static StorageMetaBypassProcessor bypass_processor[MetaType::MAX];
private:
  template <typename T>
  static int bypass_process_storage_meta(
      const MetaType type,
      common::ObSafeArenaAllocator &allocator,
      ObStorageMetaValueHandle &handle,
      const char *buf,
      const int64_t size);
private:
  MetaType type_;
  ObIStorageMetaObj *obj_;
};

class ObStorageMetaCacheValue final
{
public:
  ObStorageMetaCacheValue() : ref_cnt_(0), value_(nullptr), cache_handle_() {}
  ~ObStorageMetaCacheValue() = default;
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  TO_STRING_KV(K_(ref_cnt), KPC_(value), K_(cache_handle));
public:
  int64_t ref_cnt_;
  const ObStorageMetaValue *value_;
  common::ObKVCacheHandle cache_handle_;
};

class ObStorageMetaValueHandle final
{
public:
  ObStorageMetaValueHandle() : cache_value_(nullptr), allocator_(nullptr) {}
  ~ObStorageMetaValueHandle() { reset(); }
  ObStorageMetaValueHandle(const ObStorageMetaValueHandle &other);
  ObStorageMetaValueHandle &operator=(const ObStorageMetaValueHandle &other);
  bool is_valid() const
  {
    return nullptr != cache_value_ && nullptr != allocator_;
  }
  int new_value(common::ObIAllocator &allocator);
  void reset();
  OB_INLINE ObStorageMetaCacheValue *get_cache_value() { return cache_value_; }
  OB_INLINE ObStorageMetaCacheValue *get_cache_value() const { return cache_value_; }
  TO_STRING_KV(KPC_(cache_value), KP_(allocator));
private:
  int set_cache_value(ObStorageMetaCacheValue *value, common::ObIAllocator *allocator);
private:
  ObStorageMetaCacheValue *cache_value_;
  common::ObIAllocator *allocator_;
};

class ObStorageMetaHandle final
{
public:
  ObStorageMetaHandle();
  ~ObStorageMetaHandle();
  void reset();
  bool is_valid() const;
  const ObMetaDiskAddr &get_phy_addr() const
  {
    return phy_addr_;
  }
  int get_value(const ObStorageMetaValue *&value);
  int get_sstable(blocksstable::ObSSTable *&sstable);
  TO_STRING_KV(K_(phy_addr), K_(io_handle), K_(cache_handle));
private:
  int wait(const int64_t timeout_ms);
private:
  friend class ObStorageMetaCache;
  ObMetaDiskAddr phy_addr_;
  ObSharedBlockReadHandle io_handle_;
  ObStorageMetaValueHandle cache_handle_;
};

class ObStorageMetaCache final
  : public common::ObKVCache<ObStorageMetaKey, ObStorageMetaValue>
{
public:
  typedef common::ObKVCache<ObStorageMetaKey, ObStorageMetaValue> BaseSecondaryMetaCache;
public:
  ObStorageMetaCache();
  virtual ~ObStorageMetaCache();
  int init(const char *cache_name, const int64_t priority);
  void destory();
  int get_meta(
      const ObStorageMetaValue::MetaType type,
      const ObStorageMetaKey &key,
      ObStorageMetaHandle &meta_handle,
      const ObTablet *tablet);
  int bypass_get_meta(
      const ObStorageMetaValue::MetaType type,
      const ObStorageMetaKey &key,
      common::ObSafeArenaAllocator &allocator,
      ObStorageMetaHandle &meta_handle);
  int batch_get_meta_and_bypass_cache(
      const ObStorageMetaValue::MetaType type,
      const common::ObIArray<ObStorageMetaKey> &keys,
      common::ObSafeArenaAllocator &allocator,
      common::ObIArray<ObStorageMetaHandle> &meta_handles);
private:
  class ObStorageMetaIOCallback : public common::ObIOCallback
  {
  public:
    ObStorageMetaIOCallback();
    ObStorageMetaIOCallback(
        const ObStorageMetaValue::MetaType type,
        const ObStorageMetaKey &key,
        ObStorageMetaValueHandle &handle,
        common::ObIAllocator *allocator,
        const ObTablet *tablet,
        common::ObSafeArenaAllocator *arena_allocator = nullptr);
    virtual ~ObStorageMetaIOCallback();
    virtual int inner_deep_copy(
        char *buf, const
        int64_t buf_len,
        ObIOCallback *&callback) const override;
    virtual int inner_process(const char *data_buffer, const int64_t size) override;
    virtual int64_t size() const override;
    virtual const char *get_data() override;
    bool is_valid() const;
    TO_STRING_KV(K_(offset), K_(buf_size), KP_(data_buf), K_(key), KP_(tablet), KP_(arena_allocator));
  private:
    ObStorageMetaValue::MetaType meta_type_;
    int64_t offset_;   // offset in block.
    int64_t buf_size_; // read size in block.
    char *data_buf_;   // actual data buffer
    ObStorageMetaValueHandle handle_;
    common::ObIAllocator *allocator_;
    ObStorageMetaKey key_;
    const ObTablet *tablet_;
    common::ObSafeArenaAllocator *arena_allocator_;
  };
private:
  int prefetch(
      const ObStorageMetaValue::MetaType type,
      const ObStorageMetaKey &key,
      ObStorageMetaHandle &meta_handle,
      const ObTablet *tablet);
  int get_meta_and_bypass_cache(
      const ObStorageMetaValue::MetaType type,
      const ObStorageMetaKey &key,
      common::ObSafeArenaAllocator &allocator,
      ObStorageMetaHandle &handle);
  int read_io(
      const ObMetaDiskAddr &meta_addr,
      ObStorageMetaIOCallback &callback,
      ObStorageMetaHandle &handle);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageMetaCache);
};

template <typename T>
int ObStorageMetaValue::bypass_process_storage_meta(
    const MetaType type,
    common::ObSafeArenaAllocator &allocator,
    ObStorageMetaValueHandle &handle,
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "ProcMetaVaule"));
  int64_t pos = 0;
  T t;
  char *buff = nullptr;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size), K(handle));
  } else if (OB_FAIL(t.deserialize(tmp_allocator, buf, size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize ", K(ret), KP(buf), K(size));
  } else {
    ObIStorageMetaObj *tiny_meta = nullptr;
    const int64_t buff_pos = sizeof(ObStorageMetaValue);
    const int64_t buff_size = sizeof(ObStorageMetaValue) + t.get_deep_copy_size();
    if (OB_ISNULL(buff = static_cast<char *>(allocator.alloc(buff_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(buff_size));
    } else {
      if (OB_FAIL(t.deep_copy(buff + buff_pos, t.get_deep_copy_size(), tiny_meta))) {
        STORAGE_LOG(WARN, "fail to deserialize sstable", K(ret), KP(buf), K(size));
      } else {
        handle.get_cache_value()->value_ = new (buff) ObStorageMetaValue(type, tiny_meta);
      }
    }
  }
  return ret;
}

} // end storage
} // end oceanbase

#endif /* OCEANBASE_STORAGE_OB_STORAGE_META_CACHE_H_ */
