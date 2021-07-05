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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_CACHE_H_
#include "lib/io/ob_io_manager.h"
#include "share/cache/ob_kv_storecache.h"
#include "ob_block_sstable_struct.h"
#include "ob_imicro_block_reader.h"
#include "ob_macro_block_reader.h"
#include "ob_micro_block_index_mgr.h"
#include "ob_store_file.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace blocksstable {
class ObMicroBlockCacheKey : public common::ObIKVCacheKey {
public:
  ObMicroBlockCacheKey(const uint64_t table_id, const MacroBlockId& block_id, const int64_t file_id,
      const int64_t offset, const int64_t size);
  ObMicroBlockCacheKey();
  ObMicroBlockCacheKey(const ObMicroBlockCacheKey& other);
  virtual ~ObMicroBlockCacheKey();
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const;
  void set(const uint64_t table_id, const MacroBlockId& block_id, const int64_t file_id, const int64_t offset,
      const int64_t size);
  TO_STRING_KV(K_(table_id), K_(block_id), K_(file_id), K_(offset), K_(size));

private:
  uint64_t table_id_;
  MacroBlockId block_id_;
  int64_t file_id_;
  int64_t offset_;
  int64_t size_;
};

class ObMicroBlockCacheValue : public common::ObIKVCacheValue {
public:
  ObMicroBlockCacheValue(
      const char* buf, const int64_t size, const char* extra_buf = NULL, const int64_t extra_size = 0);
  virtual ~ObMicroBlockCacheValue();
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const;
  inline const ObMicroBlockData& get_block_data() const
  {
    return block_data_;
  }
  inline ObMicroBlockData& get_block_data()
  {
    return block_data_;
  }
  TO_STRING_KV(K_(block_data));

private:
  ObMicroBlockData block_data_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockCacheValue);
};

class ObIMicroBlockCache;

class ObMicroBlockBufferHandle {
public:
  ObMicroBlockBufferHandle() : micro_block_(NULL)
  {}
  ~ObMicroBlockBufferHandle()
  {}
  void reset()
  {
    micro_block_ = NULL;
    handle_.reset();
  }
  inline const ObMicroBlockData* get_block_data() const
  {
    return is_valid() ? &(micro_block_->get_block_data()) : NULL;
  }
  inline bool is_valid() const
  {
    return NULL != micro_block_ && handle_.is_valid();
  }
  TO_STRING_KV(K_(handle), KP_(micro_block));

private:
  friend class ObIMicroBlockCache;
  common::ObKVCacheHandle handle_;
  const ObMicroBlockCacheValue* micro_block_;
};

struct ObMultiBlockIOParam {
  ObMultiBlockIOParam();
  virtual ~ObMultiBlockIOParam();
  void reset();
  bool is_valid() const;
  inline int get_io_range(int64_t& offset, int64_t& size) const;
  inline int64_t first_micro_index()
  {
    return start_index_;
  }
  inline int64_t last_micro_index()
  {
    return start_index_ + block_count_ - 1;
  }
  inline bool in_range(const int64_t index)
  {
    return index >= start_index_ && index < start_index_ + block_count_;
  }
  common::ObIArray<ObMicroBlockInfo>* micro_block_infos_;
  int64_t start_index_;
  int64_t block_count_;
  DECLARE_TO_STRING;
};

struct ObMultiBlockIOCtx {
  ObMultiBlockIOCtx() : micro_block_infos_(NULL), hit_cache_bitmap_(NULL), block_count_(0)
  {}
  virtual ~ObMultiBlockIOCtx()
  {}
  void reset();
  bool is_valid() const;
  ObMicroBlockInfo* micro_block_infos_;
  bool* hit_cache_bitmap_;
  int64_t block_count_;
  TO_STRING_KV(KP(micro_block_infos_), KP(hit_cache_bitmap_), K_(block_count));
};

struct ObMultiBlockIOResult {
  ObMultiBlockIOResult();
  virtual ~ObMultiBlockIOResult();

  int get_block_data(const int64_t index, ObMicroBlockData& block_data) const;
  void reset();
  const ObMicroBlockCacheValue** micro_blocks_;
  common::ObKVCacheHandle* handles_;
  int64_t block_count_;
  int ret_code_;
};

class ObIPutSizeStat {
public:
  virtual int add_put_size(const int64_t put_size) = 0;
};

class ObIMicroBlockCache : public ObIPutSizeStat {
public:
  typedef common::ObIKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue> BaseBlockCache;
  virtual int prefetch(const uint64_t table_id, const ObMacroBlockCtx& block_ctx, const int64_t offset,
      const int64_t size, const common::ObQueryFlag& flag, ObStorageFile* pg_file, ObMacroBlockHandle& handle);
  // multi micro block prefetch
  virtual int prefetch(const uint64_t table_id, const ObMacroBlockCtx& block_ctx, const ObMultiBlockIOParam& io_param,
      const common::ObQueryFlag& flag, ObStorageFile* pg_file, ObMacroBlockHandle& io_handle);
  virtual int get_cache_block(const uint64_t table_id, const MacroBlockId block_id, const int64_t file_id,
      const int64_t offset, const int64_t size, ObMicroBlockBufferHandle& handle);
  virtual int load_cache_block(ObMacroBlockReader& reader, const uint64_t table_id, const ObMacroBlockCtx& block_ctx,
      const int64_t offset, const int64_t size, ObStorageFile* storage_file, ObMicroBlockData& block_data);

  virtual int get_cache(BaseBlockCache*& cache) = 0;
  virtual int get_allocator(common::ObIAllocator*& allocator) = 0;

public:
  class ObIMicroBlockIOCallback : public common::ObIOCallback {
  public:
    ObIMicroBlockIOCallback();
    virtual ~ObIMicroBlockIOCallback();
    virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset);

  protected:
    friend class ObIMicroBlockCache;
    virtual int process_block(ObMacroBlockReader* reader, char* buffer,
        const int64_t offset,  // offset means offset in macro_block
        const int64_t size, const ObMicroBlockCacheValue*& micro_block, common::ObKVCacheHandle& handle);

  private:
    int put_cache_and_fetch(const ObFullMacroBlockMeta& meta, ObMacroBlockReader& reader, const char* buffer,
        const int64_t offset,  // offset means offset in macro_block
        const int64_t size, const char* payload_buf, const int64_t payload_size,
        const ObMicroBlockCacheValue*& micro_block, common::ObKVCacheHandle& handle);
    static const int64_t ALLOC_BUF_RETRY_INTERVAL = 100 * 1000;
    static const int64_t ALLOC_BUF_RETRY_TIMES = 3;

  protected:
    int assign(const ObIMicroBlockIOCallback& other);

  protected:
    BaseBlockCache* cache_;
    ObIPutSizeStat* put_size_stat_;
    common::ObIAllocator* allocator_;
    char* io_buffer_;
    char* data_buffer_;
    uint64_t table_id_;
    MacroBlockId block_id_;
    int64_t file_id_;
    int64_t offset_;
    int64_t size_;
    bool use_block_cache_;
    storage::ObTableHandle table_handle_;
  };
  class ObMicroBlockIOCallback : public ObIMicroBlockIOCallback {
  public:
    ObMicroBlockIOCallback();
    virtual ~ObMicroBlockIOCallback();
    virtual int64_t size() const;
    virtual int inner_process(const bool is_success);
    virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const;
    virtual const char* get_data();
    TO_STRING_KV(KP_(micro_block));

  private:
    friend class ObIMicroBlockCache;
    const ObMicroBlockCacheValue* micro_block_;
    common::ObKVCacheHandle handle_;
  };

  class ObMultiBlockIOCallback : public ObIMicroBlockIOCallback {
  public:
    ObMultiBlockIOCallback();
    virtual ~ObMultiBlockIOCallback();
    virtual int64_t size() const;
    virtual int inner_process(const bool is_success);
    virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const;
    virtual const char* get_data();
    TO_STRING_KV(KP_(&io_result));

  private:
    friend class ObIMicroBlockCache;
    int set_io_ctx(const ObMultiBlockIOParam& io_param);
    void reset_io_ctx()
    {
      io_ctx_.reset();
    }
    int deep_copy_ctx(const ObMultiBlockIOCtx& io_ctx);
    int alloc_result();
    void free_result();
    ObMultiBlockIOCtx io_ctx_;
    ObMultiBlockIOResult io_result_;
  };
};

class ObMicroBlockCache : public common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>,
                          public ObIMicroBlockCache {
public:
  typedef ObIKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue> BaseBlockCache;

  ObMicroBlockCache()
  {}
  virtual ~ObMicroBlockCache()
  {}
  int init(const char* cache_name, const int64_t priority = 1);
  void destroy();

  virtual int get_cache(BaseBlockCache*& cache);
  virtual int get_allocator(common::ObIAllocator*& allocator);
  virtual int add_put_size(const int64_t put_size);

private:
  common::ObConcurrentFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockCache);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
