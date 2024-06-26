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
#include "share/io/ob_io_manager.h"
#include "share/cache/ob_kv_storecache.h"
#include "ob_block_sstable_struct.h"
#include "ob_macro_block_reader.h"
#include "index_block/ob_index_block_row_scanner.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_micro_block_info.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/blocksstable/ob_block_manager.h"


namespace oceanbase
{
namespace storage
{
class ObMicroBlockDataHandle;
}

namespace blocksstable
{

class ObIMicroBlockIOCallback;
class ObMicroBlockCacheKey : public common::ObIKVCacheKey
{
public:
  ObMicroBlockCacheKey(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const int64_t offset,
      const int64_t size);
  ObMicroBlockCacheKey(
      const uint64_t tenant_id,
      const ObMicroBlockId &block_id);
  ObMicroBlockCacheKey();
  ObMicroBlockCacheKey(const ObMicroBlockCacheKey &other);
  virtual ~ObMicroBlockCacheKey();
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  void set(const uint64_t tenant_id,
           const MacroBlockId &block_id,
           const int64_t offset,
           const int64_t size);
  const ObMicroBlockId &get_micro_block_id() const { return block_id_; }
  TO_STRING_KV(K_(tenant_id), K_(block_id));
private:
  uint64_t tenant_id_;
  ObMicroBlockId block_id_;
};

class ObMicroBlockCacheValue : public common::ObIKVCacheValue
{
public:
  ObMicroBlockCacheValue();
  ObMicroBlockCacheValue(
      const char *buf,
      const int64_t size,
      const char *extra_buf = NULL,
      const int64_t extra_size = 0,
      const ObMicroBlockData::Type block_type = ObMicroBlockData::DATA_BLOCK);
  virtual ~ObMicroBlockCacheValue();
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  inline const ObMicroBlockData& get_block_data() const { return block_data_; }
  inline ObMicroBlockData& get_block_data() { return block_data_; }
  bool need_free() const {return alloc_by_block_io_; }
  void set_alloc_by_block_io() { alloc_by_block_io_ = true; }
  TO_STRING_KV(K_(block_data));
private:
  ObMicroBlockData block_data_;
  bool alloc_by_block_io_;  // TODO: @lvling to be removed
private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockCacheValue);
};

class ObIMicroBlockCache;

class ObMicroBlockBufferHandle
{
public:
  ObMicroBlockBufferHandle() : micro_block_(NULL) {}
  ~ObMicroBlockBufferHandle() {}
  void reset() { micro_block_ = NULL; handle_.reset(); }
  inline const ObMicroBlockData* get_block_data() const
  { return is_valid() ? &(micro_block_->get_block_data()) : NULL; }
  int64_t get_block_size() const { return is_valid() ? micro_block_->get_block_data().total_size() : 0; }
  inline bool is_valid() const { return NULL != micro_block_ && handle_.is_valid(); }
  inline ObKVMemBlockHandle* get_mb_handle() const { return handle_.get_mb_handle(); }
  inline const ObMicroBlockCacheValue* get_micro_block() const { return micro_block_; }
  inline void set_mb_handle(ObKVMemBlockHandle *mb_handle) { handle_.set_mb_handle(mb_handle); }
  inline void set_micro_block(const ObMicroBlockCacheValue *micro_block) { micro_block_ = micro_block; }
  TO_STRING_KV(K_(handle), KP_(micro_block));
private:
  friend class ObIMicroBlockCache;
  common::ObKVCacheHandle handle_;
  const ObMicroBlockCacheValue *micro_block_;
};

struct ObMultiBlockIOResult
{
  ObMultiBlockIOResult();
  virtual ~ObMultiBlockIOResult();

  int get_block_data(const int64_t index, const ObMicroBlockInfo &micro_info, ObMicroBlockData &block_data) const;
  const ObMicroBlockCacheValue **micro_blocks_;
  common::ObKVCacheHandle *handles_;
  ObMicroBlockInfo *micro_infos_;
  int64_t block_count_;
  int ret_code_;
};

struct ObMultiBlockIOParam
{
public:
  static const int64_t MAX_MICRO_BLOCK_READ_COUNT = 1 << 12;
  ObMultiBlockIOParam() :
      is_reverse_(false),
      data_cache_size_(0),
      micro_block_count_(0),
      io_read_batch_size_(0),
      io_read_gap_size_(0),
      row_header_(nullptr),
      prefetch_idx_(),
      micro_infos_()
  {}
  virtual ~ObMultiBlockIOParam() {}
  void reset();
  void reuse();
  bool is_valid() const;
  int init(
      const ObTableIterParam &iter_param,
      const int64_t micro_count_cap,
      const bool is_reverse,
      common::ObIAllocator &allocator);
  int64_t count() const
  { return micro_block_count_; }
  bool add_micro_data(
      const ObMicroIndexInfo &index_info,
      const int64_t micro_data_prefetch_idx,
      storage::ObMicroBlockDataHandle &micro_handle,
      bool &need_split);
  inline void get_io_range(int64_t &offset, int64_t &size) const
  {
    offset = 0;
    size = 0;
    if (1 == micro_block_count_) {
      offset = micro_infos_[0].offset_;
      size = micro_infos_[0].size_;
    } else if (!is_reverse_) {
      offset = micro_infos_[0].offset_;
      size = micro_infos_[micro_block_count_ - 1].offset_ + micro_infos_[micro_block_count_ - 1].size_ - offset;
    } else {
      offset = micro_infos_[micro_block_count_ - 1].offset_;
      size = micro_infos_[0].offset_ + micro_infos_[0].size_ - offset;
    }
  }
  inline int64_t get_data_cache_size() const
  { return data_cache_size_; }
  TO_STRING_KV(K_(is_reverse), K_(data_cache_size), K_(io_read_batch_size),
               K_(io_read_gap_size), K_(micro_block_count));

  bool is_reverse_;
  int64_t data_cache_size_;
  int64_t micro_block_count_;
  int64_t io_read_batch_size_;
  int64_t io_read_gap_size_;
  const ObIndexBlockRowHeader *row_header_;
  ObReallocatedFixedArray<int64_t> prefetch_idx_;
  ObReallocatedFixedArray<ObMicroBlockInfo> micro_infos_;
};

struct ObMultiBlockIOCtx
{
  ObMultiBlockIOCtx()
      : micro_block_count_(0), micro_infos_(nullptr) {}
  virtual ~ObMultiBlockIOCtx() {}
  bool is_valid() const
  { return micro_block_count_ > 0; }
  int64_t micro_block_count_;
  ObMicroBlockInfo *micro_infos_;

  TO_STRING_KV(K_(micro_block_count), KP_(micro_infos));
};

class ObIPutSizeStat
{
public:
  ObIPutSizeStat() {}
  virtual ~ObIPutSizeStat() {}
  virtual int add_put_size(const int64_t put_size) = 0;
};

// New Block IO Callbacks for version 4.0
class ObIMicroBlockIOCallback : public common::ObIOCallback
{
public:
  ObIMicroBlockIOCallback();
  virtual ~ObIMicroBlockIOCallback();
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size);
  virtual ObIAllocator *get_allocator() { return allocator_; }
  void set_micro_des_meta(const ObIndexBlockRowHeader *idx_row_header);
  OB_INLINE void set_rowkey_col_descs(const ObIArray<share::schema::ObColDesc> *rowkey_col_descs)
  {
    rowkey_col_descs_ = rowkey_col_descs;
  }
protected:
  friend class ObIMicroBlockCache;
  friend class ObDataMicroBlockCache;
  int process_block(
      ObMacroBlockReader *reader,
      const char *buffer,
      const int64_t offset,
      const int64_t size,
      const ObMicroBlockCacheValue *&micro_block,
      common::ObKVCacheHandle &cache_handle);
private:
  int read_block_and_copy(
      const ObMicroBlockHeader &header,
      ObMacroBlockReader &reader,
      const char *buffer,
      const int64_t size,
      ObMicroBlockData &block_data,
      const ObMicroBlockCacheValue *&micro_block,
      common::ObKVCacheHandle &handle);
  static const int64_t ALLOC_BUF_RETRY_INTERVAL = 100 * 1000;
  static const int64_t ALLOC_BUF_RETRY_TIMES = 3;
protected:
  ObIMicroBlockCache *cache_;
  ObIPutSizeStat *put_size_stat_;
  common::ObIAllocator *allocator_;
  char *data_buffer_;
  uint64_t tenant_id_;
  MacroBlockId block_id_;
  int64_t offset_;
  ObMicroBlockDesMeta block_des_meta_;
  bool use_block_cache_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  const ObIArray<share::schema::ObColDesc> *rowkey_col_descs_;
  DISALLOW_COPY_AND_ASSIGN(ObIMicroBlockIOCallback);
};

class ObAsyncSingleMicroBlockIOCallback : public ObIMicroBlockIOCallback
{
public:
  ObAsyncSingleMicroBlockIOCallback();
  virtual ~ObAsyncSingleMicroBlockIOCallback();
  virtual int64_t size() const;
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_data() override;
  TO_STRING_KV("callback_type:", "ObAsyncSingleMicroBlockIOCallback", KP_(micro_block), K_(cache_handle), K_(offset), K_(block_des_meta));
private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncSingleMicroBlockIOCallback);
  friend class ObIMicroBlockCache;
  // Notice: lifetime shoule be longer than AIO or deep copy here
  const ObMicroBlockCacheValue *micro_block_;
  common::ObKVCacheHandle cache_handle_;
};

class ObMultiDataBlockIOCallback : public ObIMicroBlockIOCallback
{
public:
  ObMultiDataBlockIOCallback();
  virtual ~ObMultiDataBlockIOCallback();
  virtual int64_t size() const;
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_data() override;
  TO_STRING_KV("callback_type:", "ObMultiDataBlockIOCallback", K_(io_ctx), K_(offset));
private:
  friend class ObDataMicroBlockCache;
  int set_io_ctx(const ObMultiBlockIOParam &io_param);
  int alloc_result();
  void free_result();
  DISALLOW_COPY_AND_ASSIGN(ObMultiDataBlockIOCallback);
  ObMultiBlockIOCtx io_ctx_;
  ObMultiBlockIOResult io_result_;
};

class ObSyncSingleMicroBLockIOCallback : public ObIMicroBlockIOCallback
{
public:
  ObSyncSingleMicroBLockIOCallback();
  virtual ~ObSyncSingleMicroBLockIOCallback();
  virtual int64_t size() const;
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_data() override;
  TO_STRING_KV("callback_type:", "ObSyncSingleMicroBLockIOCallback", KP_(macro_reader), KP_(block_data), K_(is_data_block));
  DISALLOW_COPY_AND_ASSIGN(ObSyncSingleMicroBLockIOCallback);
protected:
  friend class ObDataMicroBlockCache;
  friend class ObIndexMicroBlockCache;
  ObMacroBlockReader *macro_reader_;
  ObMicroBlockData *block_data_;
  bool is_data_block_;
};

class ObMicroBlockBufTransformer final
{
 public:
   ObMicroBlockBufTransformer(const ObMicroBlockDesMeta &block_des_meta,
                              ObMacroBlockReader *reader,
                              ObMicroBlockHeader &header,
                              const char *buf,
                              const int64_t buf_size);
   int init();
   int get_buf_size(int64_t &buf_size) const;
   int transfrom(char *block_buf, const int64_t buf_size);

 private:
   bool is_inited_;
   bool is_cs_full_transfrom_;
   const ObMicroBlockDesMeta &block_des_meta_;
   ObMacroBlockReader *reader_;
   ObMicroBlockHeader &header_;
   const char *payload_buf_;
   int64_t payload_size_;
   ObCSMicroBlockTransformer transformer_;
};


class ObIMicroBlockCache : public ObIPutSizeStat
{
public:
  typedef common::ObIKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue> BaseBlockCache;
  ObIMicroBlockCache() {}
  virtual ~ObIMicroBlockCache() {}
  int get_cache_block(
      const uint64_t tenant_id,
      const MacroBlockId block_id,
      const int64_t offset,
      const int64_t size,
      ObMicroBlockBufferHandle &handle);
  int prefetch(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const ObMicroIndexInfo& idx_row,
      const bool use_cache,
      ObMacroBlockHandle &macro_handle,
      ObIAllocator *allocator);
  virtual int load_block(
      const ObMicroBlockId &micro_block_id,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader *macro_reader,
      ObMicroBlockData &block_data,
      ObIAllocator *allocator) = 0;
  virtual void destroy() = 0;
  virtual int get_cache(BaseBlockCache *&cache) = 0;
  virtual int get_allocator(common::ObIAllocator *&allocator) = 0;
  virtual int put_cache_block(
      const ObMicroBlockDesMeta &des_meta,
      const char *raw_block_buf,
      const ObMicroBlockCacheKey &key,
      ObMacroBlockReader &reader,
      ObIAllocator &allocator,
      const ObMicroBlockCacheValue *&micro_block,
      common::ObKVCacheHandle &cache_handle,
      const ObIArray<share::schema::ObColDesc> *rowkey_col_descs = nullptr) = 0;
  virtual int reserve_kvpair(
      const ObMicroBlockDesc &micro_block_desc,
      ObKVCacheInstHandle &inst_handle,
      ObKVCacheHandle &cache_handle,
      ObKVCachePair *&kvpair,
      int64_t &kvpair_size) = 0;
  virtual ObMicroBlockData::Type get_type() = 0;
  virtual int add_put_size(const int64_t put_size) override;
  virtual void cache_bypass() = 0;
  virtual void cache_hit(int64_t &hit_cnt) = 0;
  virtual void cache_miss(int64_t &miss_cnt) = 0;

protected:
  int prefetch(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const ObMicroIndexInfo& idx_row,
      ObMacroBlockHandle &macro_handle,
      ObIMicroBlockIOCallback &callback);
  int prefetch(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const ObMultiBlockIOParam &io_param,
      const bool use_cache,
      ObMacroBlockHandle &macro_handle,
      ObIMicroBlockIOCallback &callback);
};

class ObDataMicroBlockCache
  : public common::ObKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>,
    public ObIMicroBlockCache
{
public:
  ObDataMicroBlockCache() {}
  virtual ~ObDataMicroBlockCache() {}
  int init(const char *cache_name, const int64_t priority = 1);
  virtual void destroy() override;
  using ObIMicroBlockCache::prefetch;
  int prefetch_multi_block(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const ObMultiBlockIOParam &io_param,
      const bool use_cache,
      ObMacroBlockHandle &macro_handle);
  int load_block(
      const ObMicroBlockId &micro_block_id,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader *macro_reader,
      ObMicroBlockData &block_data,
      ObIAllocator *allocator) override;
  virtual int get_cache(BaseBlockCache *&cache) override;
  virtual int get_allocator(common::ObIAllocator *&allocator) override;
  virtual int put_cache_block(
      const ObMicroBlockDesMeta &des_meta,
      const char *raw_block_buf,
      const ObMicroBlockCacheKey &key,
      ObMacroBlockReader &reader,
      ObIAllocator &allocator,
      const ObMicroBlockCacheValue *&micro_block,
      common::ObKVCacheHandle &cache_handle,
      const ObIArray<share::schema::ObColDesc> *rowkey_col_descs = nullptr) override;
  virtual int reserve_kvpair(
      const ObMicroBlockDesc &micro_block_desc,
      ObKVCacheInstHandle &inst_handle,
      ObKVCacheHandle &cache_handle,
      ObKVCachePair *&kvpair,
      int64_t &kvpair_size) override;
  virtual ObMicroBlockData::Type get_type() override;
  virtual void cache_bypass();
  virtual void cache_hit(int64_t &hit_cnt);
  virtual void cache_miss(int64_t &miss_cnt);
private:
  int64_t calc_value_size(const int64_t data_length, const ObRowStoreType &type, bool &need_decoder);
  int write_extra_buf(
      const ObRowStoreType row_store_type,
      const char *block_buf,
      const int64_t block_size,
      char *extra_buf,
      ObMicroBlockData &micro_data);
private:
  common::ObConcurrentFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObDataMicroBlockCache);
};

class ObIndexMicroBlockCache : public ObDataMicroBlockCache
{
public:
  ObIndexMicroBlockCache();
  virtual ~ObIndexMicroBlockCache();
  int init(const char *cache_name, const int64_t priority = 10);
  int load_block(
      const ObMicroBlockId &micro_block_id,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader *macro_reader,
      ObMicroBlockData &block_data,
      ObIAllocator *allocator) override;
  virtual int put_cache_block(
      const ObMicroBlockDesMeta &des_meta,
      const char *raw_block_buf,
      const ObMicroBlockCacheKey &key,
      ObMacroBlockReader &reader,
      ObIAllocator &allocator,
      const ObMicroBlockCacheValue *&micro_block,
      common::ObKVCacheHandle &cache_handle,
      const ObIArray<share::schema::ObColDesc> *rowkey_col_descs = nullptr) override;
  virtual int reserve_kvpair(
      const ObMicroBlockDesc &micro_block_desc,
      ObKVCacheInstHandle &inst_handle,
      ObKVCacheHandle &cache_handle,
      ObKVCachePair *&kvpair,
      int64_t &kvpair_size) override
  {
    // not support pre-reserve kvpair interface for index block
    return OB_NOT_SUPPORTED;
  }
  virtual ObMicroBlockData::Type get_type() override;
  virtual void cache_bypass();
  virtual void cache_hit(int64_t &hit_cnt);
  virtual void cache_miss(int64_t &miss_cnt);
};


}//end namespace blocksstable
}//end namespace oceanbase
#endif
