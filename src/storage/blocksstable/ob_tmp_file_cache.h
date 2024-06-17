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
#include "lib/queue/ob_link_queue.h"
#include "share/io/ob_io_manager.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObTmpBlockIOInfo;
struct ObTmpFileMacroBlockHeader;
class ObTmpFileIOHandle;
class ObTmpMacroBlock;
class ObTmpFileExtent;
class ObMacroBlockHandle;
class ObTmpTenantFileStore;

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
  TO_STRING_KV(K_(block_id), K_(page_id), K_(tenant_id));

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
  TO_STRING_KV(KP_(buf), K_(size));

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
  TO_STRING_KV(KP_(value), K_(handle));
  ObTmpPageCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

struct ObTmpPageIOInfo final
{
public:
  ObTmpPageIOInfo() : offset_(0), size_(0), key_() {}
  ~ObTmpPageIOInfo() {}
  TO_STRING_KV(K_(key), K_(offset), K_(size));

  int32_t offset_;
  int32_t size_;
  ObTmpPageCacheKey key_;
};

class ObTmpPageCache final : public common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue>
{
public:
  typedef common::ObKVCache<ObTmpPageCacheKey, ObTmpPageCacheValue> BasePageCache;
  static ObTmpPageCache &get_instance();
  int init(const char *cache_name, const int64_t priority);
  int direct_read(const ObTmpBlockIOInfo &info, ObMacroBlockHandle &mb_handle, common::ObIAllocator &allocator);
  int prefetch(
      const ObTmpPageCacheKey &key,
      const ObTmpBlockIOInfo &info,
      ObMacroBlockHandle &mb_handle,
      common::ObIAllocator &allocator);
  // multi page prefetch
  int prefetch(
      const ObTmpBlockIOInfo &info,
      const common::ObIArray<ObTmpPageIOInfo> &page_io_infos,
      ObMacroBlockHandle &mb_handle,
      common::ObIAllocator &allocator);
  int get_cache_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle);
  int get_page(const ObTmpPageCacheKey &key, ObTmpPageValueHandle &handle);
  int put_page(const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value);
  void destroy();
public:
  class ObITmpPageIOCallback : public common::ObIOCallback
  {
  public:
    ObITmpPageIOCallback();
    virtual ~ObITmpPageIOCallback();
    virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override;
  protected:
    friend class ObTmpPageCache;
    virtual int process_page(const ObTmpPageCacheKey &key, const ObTmpPageCacheValue &value);
    virtual ObIAllocator *get_allocator() { return allocator_; }

  protected:
    BasePageCache *cache_;
    common::ObIAllocator *allocator_;
    int64_t offset_;   // offset in block
    char *data_buf_;   // actual data buffer
  };

  class ObTmpPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpPageIOCallback();
    ~ObTmpPageIOCallback() override;
    int64_t size() const override;
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_data() override;
    TO_STRING_KV("callback_type:", "ObTmpPageIOCallback", KP_(data_buf));
    DISALLOW_COPY_AND_ASSIGN(ObTmpPageIOCallback);
  private:
    friend class ObTmpPageCache;
    ObTmpPageCacheKey key_;
  };
  class ObTmpMultiPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpMultiPageIOCallback();
    ~ObTmpMultiPageIOCallback() override;
    int64_t size() const override;
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_data() override;
    TO_STRING_KV("callback_type:", "ObTmpMultiPageIOCallback", KP_(data_buf));
    DISALLOW_COPY_AND_ASSIGN(ObTmpMultiPageIOCallback);
  private:
    friend class ObTmpPageCache;
    common::ObArray<ObTmpPageIOInfo> page_io_infos_;
  };
  class ObTmpDirectReadPageIOCallback final : public ObITmpPageIOCallback
  {
  public:
    ObTmpDirectReadPageIOCallback() {}
    ~ObTmpDirectReadPageIOCallback() override {}
    int64_t size() const override;
    int inner_process(const char *data_buffer, const int64_t size) override;
    const char *get_data() override;
    TO_STRING_KV("callback_type:", "ObTmpDirectReadPageIOCallback", KP_(data_buf));
    DISALLOW_COPY_AND_ASSIGN(ObTmpDirectReadPageIOCallback);
  };
private:
  ObTmpPageCache();
  ~ObTmpPageCache();
  int inner_read_io(const ObTmpBlockIOInfo &io_info,
                    ObITmpPageIOCallback *callback,
                    ObMacroBlockHandle &handle);
  int read_io(const ObTmpBlockIOInfo &io_info,
              ObITmpPageIOCallback *callback,
              ObMacroBlockHandle &handle);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpPageCache);
};

class ObTmpBlockCacheKey final : public common::ObIKVCacheKey
{
public:
  ObTmpBlockCacheKey(const int64_t block_id, const uint64_t tenant_id);
  ~ObTmpBlockCacheKey() {}
  bool operator ==(const ObIKVCacheKey &other) const override;
  uint64_t get_tenant_id() const override;
  uint64_t hash() const override;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  int64_t get_block_id() const { return block_id_; }
  bool is_valid() const { return block_id_ > 0 && tenant_id_ > 0 && size() > 0; }
  TO_STRING_KV(K_(block_id), K_(tenant_id));

private:
  int64_t block_id_;
  uint64_t tenant_id_;
  friend class ObTmpBlockCache;
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCacheKey);
};

class ObTmpBlockCacheValue final : public common::ObIKVCacheValue
{
public:
  explicit ObTmpBlockCacheValue(char *buf);
  ~ObTmpBlockCacheValue() {}
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const { return NULL != buf_ && size() > 0; }
  char *get_buffer() { return buf_; }
  TO_STRING_KV(K_(size));

private:
  char *buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCacheValue);
};

struct ObTmpBlockValueHandle final
{
public:
  ObTmpBlockValueHandle()
    : value_(NULL), inst_handle_(), kvpair_(NULL), handle_(){}
  ~ObTmpBlockValueHandle() = default;
  void reset()
  {
    handle_.reset();
    inst_handle_.reset();
    value_ = NULL;
    kvpair_ = NULL;
  }
  TO_STRING_KV(KP_(value), K_(inst_handle), KP_(kvpair), K_(handle));
  ObTmpBlockCacheValue *value_;
  ObKVCacheInstHandle inst_handle_;
  ObKVCachePair *kvpair_;
  common::ObKVCacheHandle handle_;
};

class ObTmpBlockCache final: public common::ObKVCache<ObTmpBlockCacheKey, ObTmpBlockCacheValue>
{
public:
  static ObTmpBlockCache &get_instance();
  int init(const char *cache_name, const int64_t priority);
  int get_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  int alloc_buf(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  int put_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  void destroy();

private:
  ObTmpBlockCache() {}
  ~ObTmpBlockCache() {}

  DISALLOW_COPY_AND_ASSIGN(ObTmpBlockCache);
};

class ObTmpTenantMemBlockManager;

class ObTmpFileWaitTask : public common::ObTimerTask
{
public:
  explicit ObTmpFileWaitTask(ObTmpTenantMemBlockManager &mgr);
  virtual ~ObTmpFileWaitTask() {}
  virtual void runTimerTask() override;
private:
  ObTmpTenantMemBlockManager &mgr_;
};

class ObTmpFileMemTask : public common::ObTimerTask
{
public:
  explicit ObTmpFileMemTask(ObTmpTenantMemBlockManager &mgr);
  virtual ~ObTmpFileMemTask() {}
  virtual void runTimerTask() override;
private:
  ObTmpTenantMemBlockManager &mgr_;
};

class ObTmpTenantMemBlockManager final
{
public:
  struct IOWaitInfo :public common::ObSpLinkQueue::Link
  {
  public:
    IOWaitInfo(ObMacroBlockHandle &block_handle, ObTmpMacroBlock &block, ObIAllocator &allocator);
    virtual ~IOWaitInfo();
    void inc_ref();
    void dec_ref();
    int wait(const int64_t timout_ms);
    int exec_wait();
    void reset_io();
    int broadcast();
    OB_INLINE ObTmpMacroBlock& get_block() { return block_; };
    TO_STRING_KV(K_(block), KPC_(block_handle), K_(ref_cnt), K_(ret_code));

  private:
    void destroy();

  public:
    ObMacroBlockHandle *block_handle_;
    ObTmpMacroBlock &block_;
    ObThreadCond cond_;
    ObIAllocator &allocator_;
    volatile int64_t ref_cnt_;
    int64_t ret_code_;
  private:
    DISALLOW_COPY_AND_ASSIGN(IOWaitInfo);
  };

  struct ObIOWaitInfoHandle final
  {
  public:
    ObIOWaitInfoHandle();
    ~ObIOWaitInfoHandle();
    ObIOWaitInfoHandle(const ObIOWaitInfoHandle &other);
    ObIOWaitInfoHandle &operator=(const ObIOWaitInfoHandle &other);
    void set_wait_info(IOWaitInfo *wait_info);
    bool is_empty() const;
    bool is_valid() const;
    void reset();
    OB_INLINE IOWaitInfo* get_wait_info() const { return wait_info_; };
    TO_STRING_KV(KPC_(wait_info));

  private:
    IOWaitInfo *wait_info_;
  };

  explicit ObTmpTenantMemBlockManager(ObTmpTenantFileStore &tenant_store);
  ~ObTmpTenantMemBlockManager();
  int init(const uint64_t tenant_id,
           common::ObConcurrentFIFOAllocator &allocator,
           double blk_nums_threshold = DEFAULT_MIN_FREE_BLOCK_RATIO);
  void destroy();
  int get_block(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  int alloc_buf(const ObTmpBlockCacheKey &key, ObTmpBlockValueHandle &handle);
  int alloc_extent(const int64_t dir_id, const uint64_t tenant_id, const int64_t size, ObTmpFileExtent &extent);
  int alloc_block_all_pages(ObTmpMacroBlock *t_mblk, ObTmpFileExtent &extent);
  int free_macro_block(const int64_t block_id);
  int wash_block(const int64_t block_id, ObIOWaitInfoHandle &handle);
  int cleanup();
  int add_macro_block(ObTmpMacroBlock *&t_mblk);
  int wait_write_finish(const int64_t block_id, const int64_t timeout_ms);
  static int64_t get_default_timeout_ms() {return GCONF._data_storage_io_timeout / 1000L;}
  int free_empty_blocks(common::ObIArray<ObTmpMacroBlock *> &free_blocks);
  int refresh_dir_to_blk_map(const int64_t dir_id, const ObTmpMacroBlock *t_mblk);
  int check_and_free_mem_block(ObTmpMacroBlock *&blk);
  bool check_block_full();

  int exec_wait();
  int change_mem();

private:
  // 1/256, only one free block each 256 block.
  static constexpr double DEFAULT_MIN_FREE_BLOCK_RATIO = 0.00390625;
  static const uint64_t DEFAULT_BUCKET_NUM = 1543L;
  static const uint64_t MBLK_HASH_BUCKET_NUM = 10243L;
  static const int64_t TENANT_MEM_BLOCK_NUM = 64L;
  const int64_t TASK_INTERVAL = 10 * 1000; // 10 ms
  const int64_t MEMORY_TASK_INTERVAL = 1000 * 1000; // 1 s
  typedef common::hash::ObHashMap<int64_t, ObTmpMacroBlock*, common::hash::SpinReadWriteDefendMode>
      TmpMacroBlockMap;
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::SpinReadWriteDefendMode> Map;
  typedef common::hash::ObHashMap<int64_t, ObIOWaitInfoHandle, common::hash::SpinReadWriteDefendMode> WaitHandleMap;

  struct BlockInfo final
  {
  public:
    BlockInfo() :block_id_(0), wash_score_(INT64_MIN) {};
    ~BlockInfo() = default;

    TO_STRING_KV(K_(block_id), K_(wash_score));

    int64_t block_id_;
    double wash_score_;
  };

  class BlockWashScoreCompare final
  {
  public:
    BlockWashScoreCompare();
    ~BlockWashScoreCompare() = default;
    bool operator() (const BlockInfo &a, const BlockInfo &b);
    int get_error_code() { return OB_SUCCESS; }
  };

  typedef common::ObBinaryHeap<BlockInfo, BlockWashScoreCompare, TENANT_MEM_BLOCK_NUM> Heap;
  struct ChooseBlocksMapOp final
  {
  public:
    ChooseBlocksMapOp(Heap &heap, int64_t cur_time) : heap_(heap), cur_time_(cur_time) {}
    int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry);
  private:
    Heap &heap_;
    int64_t cur_time_;
  };

  struct GetAvailableBlockMapOp final
  {
  public:
    GetAvailableBlockMapOp(const int64_t dir_id, const int64_t tenant_id, const int64_t page_nums,
                           ObTmpMacroBlock *&block, bool &is_found)
        : dir_id_(dir_id), tenant_id_(tenant_id), page_nums_(page_nums),  block_(block), is_found_(is_found){}
    int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry);
  private:
    int64_t dir_id_;
    int64_t tenant_id_;
    int64_t page_nums_;
    ObTmpMacroBlock *&block_;
    bool &is_found_;
  };

  struct DestroyBlockMapOp final
  {
  public:
    DestroyBlockMapOp(ObTmpTenantFileStore &tenant_store) : tenant_store_(tenant_store) {}
    int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObTmpMacroBlock*> &entry);
  private:
    ObTmpTenantFileStore &tenant_store_;  // reference to tenant store from ObTmpTenantMemBlockManager
  };

private:
  int get_available_macro_block(const int64_t dir_id, const uint64_t tenant_id, const int64_t page_nums,
      ObTmpMacroBlock *&t_mblk, bool &is_found);
  int write_io(
      const ObTmpBlockIOInfo &io_info,
      ObMacroBlockHandle &handle);
  int64_t get_tenant_mem_block_num();
  int check_memory_limit();
  int get_block_from_dir_cache(const int64_t dir_id, const int64_t tenant_id,
                               const int64_t page_nums, ObTmpMacroBlock *&t_mblk);
  int get_block_and_set_washing(int64_t block_id, ObTmpMacroBlock *&m_blk);

  ObTmpTenantFileStore &tenant_store_;
  ObSpLinkQueue wait_info_queue_;
  WaitHandleMap wait_handles_map_;
  TmpMacroBlockMap t_mblk_map_;  // <block id, tmp macro block>
  Map dir_to_blk_map_;           // <dir id, block id>
  double blk_nums_threshold_;    // free_page_nums / total_page_nums
  ObTmpBlockCache *block_cache_;
  common::ObConcurrentFIFOAllocator *allocator_;
  uint64_t tenant_id_;
  int64_t last_access_tenant_config_ts_;
  int64_t last_tenant_mem_block_num_;
  bool is_inited_;
  int tg_id_;
  bool stopped_;
  int64_t washing_count_;
  ObTmpFileWaitTask wait_task_;
  ObTmpFileMemTask mem_task_;
  SpinRWLock io_lock_;
  common::ObBucketLock map_lock_;
  ObThreadCond cond_;
  BlockWashScoreCompare compare_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpTenantMemBlockManager);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_CACHE_H_


