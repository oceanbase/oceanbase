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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_CACHE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/io/ob_io_manager.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_store.h"
#include "ob_store_file.h"
#include "ob_tmp_file_cache.h"

namespace oceanbase {
namespace blocksstable {

class ObTmpBlockIOInfo;
class ObTmpFileIOHandle;
class ObTmpMacroBlock;
class ObTmpFileExtent;

class ObTmpPageCacheKey : public common::ObIKVCacheKey {
public:
  ObTmpPageCacheKey();
  ObTmpPageCacheKey(const int64_t block_id, const int64_t page_id, const uint64_t tenant_id);
  virtual ~ObTmpPageCacheKey();
  virtual bool operator==(const ObIKVCacheKey& other) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const override;
  bool is_valid() const;
  int64_t get_page_id() const
  {
    return page_id_;
  }
  TO_STRING_KV(K_(block_id), K_(page_id), K_(tenant_id));

private:
  int64_t block_id_;
  int64_t page_id_;
  uint64_t tenant_id_;
};

class ObTmpPageCacheValue : public common::ObIKVCacheValue {
public:
  explicit ObTmpPageCacheValue(char* buf);
  virtual ~ObTmpPageCacheValue();
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const override;
  bool is_valid() const
  {
    return NULL != buf_ && size() > 0;
  }
  char* get_buffer()
  {
    return buf_;
  }
  void set_buffer(char* buf)
  {
    buf_ = buf;
  }
  TO_STRING_KV(K_(size));

private:
  char* buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCacheValue);
};

struct ObTmpPageValueHandle {
public:
  ObTmpPageValueHandle() : value_(NULL), handle_()
  {}
  virtual ~ObTmpPageValueHandle() = default;
  void reset()
  {
    handle_.reset();
    value_ = NULL;
  }
  TO_STRING_KV(KP_(value), K_(handle));
  ObTmpPageCacheValue* value_;
  common::ObKVCacheHandle handle_;
};

struct ObTmpPageIOInfo {
public:
  ObTmpPageIOInfo() : key_(), offset_(0), size_(0)
  {}
  virtual ~ObTmpPageIOInfo()
  {}
  TO_STRING_KV(K_(key), K_(offset), K_(size));

  ObTmpPageCacheKey key_;
  int32_t offset_;
  int32_t size_;
};

class ObTmpPageCache : public common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue> {
public:
  typedef common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue> BasePageCache;
  static ObTmpPageCache& get_instance();
  int init(const char* cache_name, const int64_t priority, const ObStorageFileHandle& file_handle);
  int prefetch(const ObTmpPageCacheKey& key, const ObTmpBlockIOInfo& info, ObTmpFileIOHandle& handle,
      ObMacroBlockHandle& mb_handle);
  // multi page prefetch
  int prefetch(const ObTmpBlockIOInfo& info, const common::ObIArray<ObTmpPageIOInfo>& page_io_infos,
      ObTmpFileIOHandle& handle, ObMacroBlockHandle& mb_handle);
  int get_cache_page(const ObTmpPageCacheKey& key, ObTmpPageValueHandle& handle);
  int get_page(const ObTmpPageCacheKey& key, ObTmpPageValueHandle& handle);
  int put_page(const ObTmpPageCacheKey& key, const ObTmpPageCacheValue& value);
  void destroy();

public:
  class ObITmpPageIOCallback : public common::ObIOCallback {
  public:
    ObITmpPageIOCallback();
    virtual ~ObITmpPageIOCallback();
    virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset) override;

  protected:
    friend class ObTmpPageCache;
    virtual int process_page(const ObTmpPageCacheKey& key, const ObTmpPageCacheValue& value);

  protected:
    BasePageCache* cache_;
    common::ObIAllocator* allocator_;
    int64_t offset_;    // offset in block
    int64_t buf_size_;  // read size in block
    char* io_buf_;      // for io assign
    int64_t io_buf_size_;
    char* data_buf_;  // actual data buffer
  };
  class ObTmpPageIOCallback : public ObITmpPageIOCallback {
  public:
    ObTmpPageIOCallback();
    virtual ~ObTmpPageIOCallback();
    virtual int64_t size() const override;
    virtual int inner_process(const bool is_success) override;
    virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const override;
    virtual const char* get_data() override;
    TO_STRING_KV(KP_(data_buf));

  private:
    friend class ObTmpPageCache;
    ObTmpPageCacheKey key_;
  };
  class ObTmpMultiPageIOCallback : public ObITmpPageIOCallback {
  public:
    ObTmpMultiPageIOCallback();
    virtual ~ObTmpMultiPageIOCallback();
    virtual int64_t size() const override;
    virtual int inner_process(const bool is_success) override;
    virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const override;
    virtual const char* get_data() override;
    TO_STRING_KV(KP_(data_buf));

  private:
    friend class ObTmpPageCache;
    common::ObIArray<ObTmpPageIOInfo>* page_io_infos_;
  };

private:
  ObTmpPageCache();
  virtual ~ObTmpPageCache();
  int read_io(const ObTmpBlockIOInfo& io_info, ObITmpPageIOCallback& callback, ObMacroBlockHandle& handle);

private:
  common::ObConcurrentFIFOAllocator allocator_;
  ObStorageFileHandle file_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCache);
};

class ObTmpBlockCacheKey : public common::ObIKVCacheKey {
public:
  ObTmpBlockCacheKey(const int64_t block_id, const uint64_t tenant_id);
  virtual ~ObTmpBlockCacheKey()
  {}
  virtual bool operator==(const ObIKVCacheKey& other) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const override;
  virtual int64_t get_block_id() const
  {
    return block_id_;
  }
  bool is_valid() const
  {
    return block_id_ > 0 && tenant_id_ > 0 && size() > 0;
  }
  TO_STRING_KV(K_(block_id), K_(tenant_id));

private:
  int64_t block_id_;
  uint64_t tenant_id_;
  friend class ObTmpBlockCache;
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCacheKey);
};

class ObTmpBlockCacheValue : public common::ObIKVCacheValue {
public:
  explicit ObTmpBlockCacheValue(char* buf);
  virtual ~ObTmpBlockCacheValue()
  {}
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const override;
  bool is_valid() const
  {
    return NULL != buf_ && size() > 0;
  }
  char* get_buffer()
  {
    return buf_;
  }
  TO_STRING_KV(K_(size));

private:
  char* buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCacheValue);
};

struct ObTmpBlockValueHandle {
public:
  ObTmpBlockValueHandle() : value_(NULL), inst_handle_(), kvpair_(NULL), handle_()
  {}
  virtual ~ObTmpBlockValueHandle() = default;
  void reset()
  {
    handle_.reset();
    inst_handle_.reset();
    value_ = NULL;
    kvpair_ = NULL;
  }
  TO_STRING_KV(KP_(value), K_(inst_handle), KP_(kvpair), K_(handle));
  ObTmpBlockCacheValue* value_;
  ObKVCacheInstHandle inst_handle_;
  ObKVCachePair* kvpair_;
  common::ObKVCacheHandle handle_;
};

class ObTmpBlockCache : public common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue> {
public:
  static ObTmpBlockCache& get_instance();
  int init(const char* cache_name, const int64_t priority);
  int get_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle);
  int alloc_buf(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle);
  int put_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle);
  void destory();

private:
  ObTmpBlockCache()
  {}
  virtual ~ObTmpBlockCache()
  {}

  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCache);
};

class ObTmpTenantMemBlockManager {
public:
  ObTmpTenantMemBlockManager();
  virtual ~ObTmpTenantMemBlockManager();
  int init(const uint64_t tenant_id, common::ObIAllocator& allocator, const ObStorageFileHandle& file_handle,
      double blk_nums_threshold = DEFAULT_MIN_FREE_BLOCK_RATIO);
  void destroy();
  int get_block(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle);
  int alloc_buf(const ObTmpBlockCacheKey& key, ObTmpBlockValueHandle& handle);
  int alloc_extent(const int64_t dir_id, const uint64_t tenant_id, const int64_t size, ObTmpFileExtent& extent,
      common::ObIArray<ObTmpMacroBlock*>& free_blocks);
  int free_macro_block(const int64_t block_id);
  int try_wash(const uint64_t tenant_id, common::ObIArray<ObTmpMacroBlock*>& free_blocks);
  int add_macro_block(const uint64_t tenant_id, ObTmpMacroBlock*& t_mblk);
  int wait_write_io_finish();
  OB_INLINE bool check_need_wait_write()
  {
    return write_handles_.count() > 0;
  }
  int free_extent(const int64_t free_page_nums, const ObTmpMacroBlock* t_mblk);

private:
  int get_macro_block(const int64_t dir_id, const uint64_t tenant_id, const int64_t page_nums, ObTmpMacroBlock*& t_mblk,
      common::ObIArray<ObTmpMacroBlock*>& free_blocks);
  int wash(const uint64_t tenant_id, int64_t block_nums, common::ObIArray<ObTmpMacroBlock*>& free_blocks);
  int wash(const uint64_t tenant_id, ObTmpMacroBlock* wash_block, bool& is_empty);
  int wash_with_no_wait(const uint64_t tenant_id, ObTmpMacroBlock* wash_block, bool& is_empty);
  int write_io(const ObTmpBlockIOInfo& io_info, ObMacroBlockHandle& handle);
  int refresh_dir_to_blk_map(const int64_t dir_id, const ObTmpMacroBlock* t_mblk);
  int build_macro_meta(const uint64_t tenant_id, ObFullMacroBlockMeta& meta);
  int64_t get_tenant_mem_block_num();

private:
  // 1/256, only one free block each 256 block.
  static constexpr double DEFAULT_MIN_FREE_BLOCK_RATIO = 0.00390625;
  static const uint64_t DEFAULT_BUCKET_NUM = 1543L;
  static const uint64_t MBLK_HASH_BUCKET_NUM = 10243L;
  static const int64_t TENANT_MEM_BLOCK_NUM = 64L;
  typedef common::hash::ObHashMap<int64_t, ObTmpMacroBlock*, common::hash::SpinReadWriteDefendMode> TmpMacroBlockMap;
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::SpinReadWriteDefendMode> Map;

  common::ObSEArray<ObMacroBlockHandle*, 1> write_handles_;
  TmpMacroBlockMap t_mblk_map_;  // <block id, tmp macro block>
  Map dir_to_blk_map_;           // <dir id, block id>
  int64_t mblk_page_nums_;
  int64_t free_page_nums_;
  double blk_nums_threshold_;  // free_page_nums / total_page_nums
  ObTmpBlockCache* block_cache_;
  common::ObIAllocator* allocator_;
  ObStorageFileHandle file_handle_;
  uint64_t tenant_id_;
  ObMacroBlocksWriteCtx block_write_ctx_;
  int64_t last_access_tenant_config_ts_;
  int64_t last_tenant_mem_block_num_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpTenantMemBlockManager);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_CACHE_H_
