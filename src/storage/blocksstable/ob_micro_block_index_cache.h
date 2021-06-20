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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_CACHE_H_
#include "share/cache/ob_kv_storecache.h"
#include "share/schema/ob_table_schema.h"
#include "ob_micro_block_index_transformer.h"
#include "ob_micro_block_index_mgr.h"
#include "ob_store_file.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace blocksstable {
class ObMicroBlockIndexInfo : public common::ObIKVCacheKey {
public:
  ObMicroBlockIndexInfo();
  ObMicroBlockIndexInfo(const MacroBlockId& block_id, const uint64_t table_id, const int64_t file_id);
  virtual ~ObMicroBlockIndexInfo();
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const;
  bool is_valid() const;
  MacroBlockId get_block_id() const
  {
    return block_id_;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  void set(const MacroBlockId& block_id, const uint64_t table_id, const int64_t file_id);
  TO_STRING_KV(K_(block_id), K_(table_id));

private:
  MacroBlockId block_id_;
  uint64_t table_id_;
  int64_t file_id_;
};

class ObMicroBlockIndexCache;

class ObMicroBlockIndexBufferHandle {
public:
  ObMicroBlockIndexBufferHandle() : index_mgr_(NULL)
  {}
  ~ObMicroBlockIndexBufferHandle()
  {}
  void reset()
  {
    index_mgr_ = NULL;
    handle_.reset();
  }
  const ObMicroBlockIndexMgr* get_index_mgr() const
  {
    return index_mgr_;
  }
  inline bool is_valid() const
  {
    return handle_.is_valid() && NULL != index_mgr_;
  }
  TO_STRING_KV(K_(handle), KP_(index_mgr));

private:
  friend class ObMicroBlockIndexCache;
  common::ObKVCacheHandle handle_;
  const ObMicroBlockIndexMgr* index_mgr_;
};

class ObMicroBlockIndexCache : public common::ObKVCache<ObMicroBlockIndexInfo, ObMicroBlockIndexMgr> {
public:
  ObMicroBlockIndexCache();
  virtual ~ObMicroBlockIndexCache();
  int init(const char* cache_name, const int64_t priority = 1);
  void destroy();
  int get_micro_infos(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
      const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      common::ObIArray<ObMicroBlockInfo>& micro_infos, const bool is_prewarm = false);
  int get_micro_infos(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
      const common::ObStoreRowkey& rowkey, ObMicroBlockInfo& micro_info, const bool is_prewarm = false);
  int get_micro_endkey(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx, const int32_t micro_block_index,
      common::ObIAllocator& alloctor, common::ObStoreRowkey& endkey);
  int get_micro_endkeys(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx, common::ObIAllocator& alloctor,
      common::ObIArray<common::ObStoreRowkey>& endkeys);
  int get_cache_block_index(const uint64_t table_id, const MacroBlockId block_id, const int64_t file_id,
      ObMicroBlockIndexBufferHandle& handle);
  int prefetch(const uint64_t table_id, const blocksstable::ObMacroBlockCtx& macro_block_ctx,
      blocksstable::ObStorageFile* pg_file, ObMacroBlockHandle& macro_handle, const common::ObQueryFlag& flag);
  int cal_border_row_count(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
      const common::ObStoreRange& range, const bool is_left_border, const bool is_right_border,
      int64_t& logical_row_count, int64_t& physical_row_count, bool& need_check_micro_block);
  int cal_macro_purged_row_count(
      const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx, int64_t& purged_row_count);

private:
  class ObMicroBlockIndexIOCallback : public common::ObIOCallback {
  public:
    ObMicroBlockIndexIOCallback();
    virtual ~ObMicroBlockIndexIOCallback();
    virtual int64_t size() const;
    virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset);
    virtual int inner_process(const bool is_success);
    virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const;
    virtual const char* get_data();
    TO_STRING_KV(K_(key), KP_(buffer), K_(offset), K_(buf_size), KP_(cache), KP_(idx_mgr));

  private:
    int put_cache_and_fetch(const ObFullMacroBlockMeta& meta, ObMicroBlockIndexTransformer& transformer);

  private:
    friend class ObMicroBlockIndexCache;
    ObMicroBlockIndexInfo key_;
    char* buffer_;
    int64_t offset_;
    int64_t buf_size_;
    ObMicroBlockIndexCache* cache_;
    common::ObIAllocator* allocator_;
    const ObMicroBlockIndexMgr* idx_mgr_;
    common::ObKVCacheHandle handle_;
    bool use_index_cache_;
    int64_t io_buf_size_;
    int64_t aligned_offset_;
    storage::ObTableHandle table_handle_;
  };
  int read_micro_index(const uint64_t table_id, const ObMacroBlockCtx& macro_block_ctx,
      ObMicroBlockIndexTransformer& transformer, const ObMicroBlockIndexMgr*& index_mgr,
      common::ObKVCacheHandle& handle, const bool is_prewarm);

private:
  common::ObConcurrentFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndexCache);
};
}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
