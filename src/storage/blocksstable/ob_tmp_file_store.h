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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_STORE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_STORE_H_

#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_tmp_file_cache.h"

namespace oceanbase
{
namespace blocksstable
{

class ObTmpFile;
class ObTmpFileExtent;
class ObTmpFileIOHandle;
class ObTmpTenantMacroBlockManager;
struct ObTmpBlockValueHandle;
class ObTmpTenantBlockCache;
class ObTmpPageCache;

typedef common::ObSEArray<ObTmpFileExtent *, 32> ExtentArray;

struct ObTmpFileArea final
{
public:
  ObTmpFileArea(const uint8_t &start_page_id, const uint8_t &page_nums)
    : start_page_id_(start_page_id), page_nums_(page_nums), next_(NULL) {}
  ~ObTmpFileArea()
  {
    start_page_id_ = 0;
    page_nums_ = 0;
    next_ = NULL;
  }
  TO_STRING_KV(K_(start_page_id), K_(page_nums));
  uint8_t start_page_id_;
  uint8_t page_nums_;
  ObTmpFileArea *next_;
} __attribute__((packed, __aligned__(1)));;

class ObTmpFilePageBuddy final
{
public:
  ObTmpFilePageBuddy();
  ~ObTmpFilePageBuddy();
  int init(common::ObIAllocator &allocator);
  void destroy();
  int alloc_all_pages();
  int alloc(const uint8_t page_nums, uint8_t &start_page_id, uint8_t &alloced_page_nums);
  void free(const int32_t start_page_id, const int32_t page_nums);
  OB_INLINE uint8_t get_max_cont_page_nums() const { return max_cont_page_nums_; }
  bool is_inited() { return is_inited_; }
  bool is_empty() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;

  static const uint8_t MAX_PAGE_NUMS = 252; // 2^MAX_ORDER - 2^MIN_ORDER, uint8_t is only for < 256

private:
  void free_align(const int32_t start_page_id, const int32_t page_nums, ObTmpFileArea *&area);
  ObTmpFileArea *find_buddy(const int32_t page_nums, const int32_t start_page_id);

private:
  static const uint8_t MIN_ORDER = 2;
  static const uint8_t MAX_ORDER = 8;
  bool is_inited_;
  uint8_t max_cont_page_nums_;
  ObTmpFileArea *buf_;
  common::ObIAllocator *allocator_;
  ObTmpFileArea *free_area_[ObTmpFilePageBuddy::MAX_ORDER];
  DISALLOW_COPY_AND_ASSIGN(ObTmpFilePageBuddy);
};

struct ObTmpBlockIOInfo final
{
public:
  ObTmpBlockIOInfo()
    : block_id_(0), offset_(0), size_(0), tenant_id_(0),
      buf_(NULL), io_desc_(), macro_block_id_()  {}
  ObTmpBlockIOInfo(const int64_t block_id, const int64_t offset, const int64_t size,
      const uint64_t tenant_id, const MacroBlockId macro_block_id, char *buf,
      const common::ObIOFlag io_desc)
    : block_id_(block_id), offset_(offset), size_(size), tenant_id_(tenant_id),
      buf_(buf), io_desc_(io_desc), macro_block_id_(macro_block_id) {}
  TO_STRING_KV(K_(block_id), K_(offset), K_(size), K_(tenant_id), K_(macro_block_id), KP_(buf),
      K_(io_desc));
  int64_t block_id_;
  int64_t offset_;
  int64_t size_;
  uint64_t tenant_id_;
  char *buf_;
  common::ObIOFlag io_desc_;
  MacroBlockId macro_block_id_;
};

class ObTmpMacroBlock final
{
public:
  ObTmpMacroBlock();
  ~ObTmpMacroBlock();
  int init(const int64_t block_id, const int64_t dir_id, const uint64_t tenant_id,
      common::ObIAllocator &allocator);
  void destroy();
  int alloc_all_pages(ObTmpFileExtent &extent);
  int alloc(const uint8_t page_nums, ObTmpFileExtent &extent);
  int free(ObTmpFileExtent &extent);
  int free(const int32_t start_page_id, const int32_t page_nums);
  OB_INLINE void set_buffer(char *buf) { buffer_ = buf; }
  OB_INLINE char *get_buffer() { return buffer_; }
  OB_INLINE uint8_t get_max_cont_page_nums() const { return page_buddy_.get_max_cont_page_nums(); }
  OB_INLINE uint8_t get_free_page_nums() const { return free_page_nums_; }
  int64_t get_used_page_nums() const;
  int get_block_cache_handle(ObTmpBlockValueHandle &handle);
  int get_wash_io_info(ObTmpBlockIOInfo &info);
  void set_io_desc(const common::ObIOFlag &io_desc);
  OB_INLINE bool is_washing() const { return ATOMIC_LOAD(&is_washing_); }
  OB_INLINE void set_washing_status(bool is_washing) { ATOMIC_SET(&is_washing_, is_washing); }
  OB_INLINE bool is_inited() const { return is_inited_; }
  OB_INLINE bool is_disked() const { return ATOMIC_LOAD(&is_disked_); }
  OB_INLINE void set_disked() { ATOMIC_SET(&is_disked_, true); }
  static int64_t get_default_page_size() { return DEFAULT_PAGE_SIZE; }
  static int64_t calculate_offset(const int64_t page_start_id, const int64_t offset)
  {
    return page_start_id * ObTmpMacroBlock::DEFAULT_PAGE_SIZE + offset;
  }
  static int64_t get_header_padding()
  {
    return 4 * DEFAULT_PAGE_SIZE;
  }
  OB_INLINE int64_t get_block_id() const { return block_id_; }
  OB_INLINE const MacroBlockId& get_macro_block_id() const { return macro_block_handle_.get_macro_id(); }
  OB_INLINE ObMacroBlockHandle &get_macro_block_handle() { return macro_block_handle_; }
  OB_INLINE int64_t get_alloc_time() const { return alloc_time_; }
  OB_INLINE int64_t get_access_time() const { return ATOMIC_LOAD(&access_time_); }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE int64_t get_dir_id() const { return dir_id_; }
  common::ObIArray<ObTmpFileExtent *> &get_extents() { return using_extents_; }
  ObTmpBlockValueHandle &get_handle() { return handle_; }
  bool is_empty() const { return page_buddy_.is_empty(); }
  int close(bool &is_all_close, uint8_t &free_page_nums);
  int give_back_buf_into_cache(bool is_wash = false);
  OB_INLINE double get_wash_score(int64_t cur_time) const {
    if (ObTmpFilePageBuddy::MAX_PAGE_NUMS == get_used_page_nums()) {
      return INT64_MAX;
    }
    return (double) get_used_page_nums() * (cur_time - get_alloc_time()) / (get_access_time() - get_alloc_time());
  }

  static int64_t get_mblk_page_nums()
  {
    return ObTmpFilePageBuddy::MAX_PAGE_NUMS;
  }
  static int64_t get_block_size()
  {
    return get_mblk_page_nums() * ObTmpMacroBlock::get_default_page_size();
  }

  TO_STRING_KV(KP_(buffer), K_(page_buddy), K_(handle), K_(macro_block_handle), K_(block_id), K_(dir_id), K_(tenant_id),
      K_(free_page_nums), K_(io_desc), K_(is_washing), K_(is_disked), K_(is_inited), K_(alloc_time), K_(access_time));
private:
  static const int64_t DEFAULT_PAGE_SIZE;
  bool is_inited_;
  bool is_washing_;
  bool is_disked_;
  uint8_t free_page_nums_;
  char *buffer_;
  int64_t block_id_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  int64_t alloc_time_;
  int64_t access_time_;
  common::ObIOFlag io_desc_;
  common::SpinRWLock lock_;
  ObMacroBlockHandle macro_block_handle_;
  ObTmpBlockValueHandle handle_;
  ObTmpFilePageBuddy page_buddy_;
  ExtentArray using_extents_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpMacroBlock);
};

class ObTmpTenantMacroBlockManager final
{
public:
  ObTmpTenantMacroBlockManager();
  ~ObTmpTenantMacroBlockManager();
  int init(common::ObIAllocator &allocator);
  void destroy();
  int alloc_macro_block(const int64_t dir_id, const uint64_t tenant_id, ObTmpMacroBlock *&t_mblk);
  int free_macro_block(const int64_t block_id);
  int get_macro_block(const int64_t block_id, ObTmpMacroBlock *&t_mblk);
  OB_INLINE int64_t get_block_size() const {
    return ObTmpFilePageBuddy::MAX_PAGE_NUMS * ObTmpMacroBlock::get_default_page_size();
  }
  int get_disk_macro_block_list(common::ObIArray<MacroBlockId> &macro_id_list);
  void print_block_usage();

private:
  int64_t get_next_blk_id();

private:
  static const uint64_t MBLK_HASH_BUCKET_NUM = 10243L;
  typedef common::hash::ObHashMap<int64_t, ObTmpMacroBlock*, common::hash::SpinReadWriteDefendMode>
      TmpMacroBlockMap;
  bool is_inited_;
  int64_t next_blk_id_;
  common::ObIAllocator *allocator_;
  TmpMacroBlockMap blocks_;  // all of block meta.

  DISALLOW_COPY_AND_ASSIGN(ObTmpTenantMacroBlockManager);
};

class ObTmpTenantFileStore final
{
public:
  ObTmpTenantFileStore();
  ~ObTmpTenantFileStore();
  int init(const uint64_t tenant_id);
  void destroy();
  int alloc(const int64_t dir_id, const uint64_t tenant_id, const int64_t alloc_size,
      ObTmpFileExtent &extent);
  int free(ObTmpFileExtent *extent);
  int free(const int64_t block_id, const int32_t start_page_id, const int32_t page_nums);
  int read(ObTmpBlockIOInfo &io_info, ObTmpFileIOHandle &handle);
  int write(const ObTmpBlockIOInfo &io_info);
  int sync(const int64_t block_id);
  int get_disk_macro_block_list(common::ObIArray<MacroBlockId> &macro_id_list);
  void print_block_usage() { tmp_block_manager_.print_block_usage(); }
  OB_INLINE int64_t get_block_size() const { return tmp_block_manager_.get_block_size(); }
  OB_INLINE void inc_page_cache_num(const int64_t num) {
    ATOMIC_FAA(&page_cache_num_, num);
  };
  OB_INLINE void dec_page_cache_num(const int64_t num) {
    ATOMIC_FAS(&page_cache_num_, num);
  };
  OB_INLINE void inc_block_cache_num(const int64_t num) {
    ATOMIC_FAA(&block_cache_num_, num);
  };
  OB_INLINE void dec_block_cache_num(const int64_t num) {
    ATOMIC_FAS(&block_cache_num_, num);
  };
  void inc_ref();
  int64_t dec_ref();

private:
  int read_page(ObTmpMacroBlock *block, ObTmpBlockIOInfo &io_info, ObTmpFileIOHandle &handle);
  int free_extent(ObTmpFileExtent *extent);
  int free_extent(const int64_t block_id, const int32_t start_page_id, const int32_t page_nums);
  int free_macro_block(ObTmpMacroBlock *&t_mblk);
  int alloc_macro_block(const int64_t dir_id, const uint64_t tenant_id, ObTmpMacroBlock *&t_mblk);
  int wait_write_io_finish_if_need();
  int64_t get_memory_limit(const uint64_t tenant_id) const;

private:
  static const uint64_t IO_LIMIT = 4 * 1024L * 1024L * 1024L;
  static const uint64_t TOTAL_LIMIT = 15 * 1024L * 1024L * 1024L;
  static const uint64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const uint64_t BLOCK_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE;
  static constexpr double DEFAULT_PAGE_IO_MERGE_RATIO = 0.5;

  bool is_inited_;
  int64_t page_cache_num_;
  int64_t block_cache_num_;
  volatile int64_t ref_cnt_;
  ObTmpPageCache *page_cache_;
  common::SpinRWLock lock_;
  ObTmpTenantMacroBlockManager tmp_block_manager_;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObConcurrentFIFOAllocator io_allocator_;
  ObTmpTenantMemBlockManager tmp_mem_block_manager_;

  DISALLOW_COPY_AND_ASSIGN(ObTmpTenantFileStore);
};

struct ObTmpTenantFileStoreHandle final
{
public:
  ObTmpTenantFileStoreHandle();
  ~ObTmpTenantFileStoreHandle();
  ObTmpTenantFileStoreHandle(const ObTmpTenantFileStoreHandle &other);
  ObTmpTenantFileStoreHandle &operator=(const ObTmpTenantFileStoreHandle &other);
  void set_tenant_store(ObTmpTenantFileStore *store, common::ObConcurrentFIFOAllocator *allocator);
  bool is_empty() const;
  bool is_valid() const;
  void reset();
  OB_INLINE ObTmpTenantFileStore* get_tenant_store() const { return tenant_store_; }
  TO_STRING_KV(KP_(tenant_store), KP_(allocator));
private:
  ObTmpTenantFileStore *tenant_store_;
  common::ObConcurrentFIFOAllocator *allocator_;
};

class ObTmpFileStore final
{
public:
  typedef common::hash::HashMapPair<uint64_t, int64_t> TenantTmpBlockCntPair;

  static ObTmpFileStore &get_instance();
  int init();
  void destroy();

  int alloc(const int64_t dir_id, const uint64_t tenant_id, const int64_t size,
      ObTmpFileExtent &extent);
  int read(const uint64_t tenant_id, ObTmpBlockIOInfo &io_info, ObTmpFileIOHandle &handle);
  int write(const uint64_t tenant_id, const ObTmpBlockIOInfo &io_info);
  int sync(const uint64_t tenant_id, const int64_t block_id);
  int free(const uint64_t tenant_id, ObTmpFileExtent *extent);
  int free(const uint64_t tenant_id, const int64_t block_id, const int32_t start_page_id,
      const int32_t page_nums);
  int free_tenant_file_store(const uint64_t tenant_id);
  int get_macro_block_list(common::ObIArray<MacroBlockId> &macro_id_list);
  int get_macro_block_list(common::ObIArray<TenantTmpBlockCntPair> &tmp_block_cnt_pairs);
  int get_all_tenant_id(common::ObIArray<uint64_t> &tenant_ids);

  static int64_t get_block_size()
  {
    return ObTmpFilePageBuddy::MAX_PAGE_NUMS * ObTmpMacroBlock::get_default_page_size();
  }
  int inc_page_cache_num(const uint64_t tenant_id, const int64_t num);
  int dec_page_cache_num(const uint64_t tenant_id, const int64_t num);
  int inc_block_cache_num(const uint64_t tenant_id, const int64_t num);
  int dec_block_cache_num(const uint64_t tenant_id, const int64_t num);
private:
  ObTmpFileStore();
  ~ObTmpFileStore();
  int get_store(const uint64_t tenant_id, ObTmpTenantFileStoreHandle &handle);

private:
  static const uint64_t STORE_HASH_BUCKET_NUM = 1543L;
  static const int64_t TOTAL_LIMIT = 512L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t BLOCK_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int TMP_FILE_PAGE_CACHE_PRIORITY = 1;
  static const int TMP_FILE_BLOCK_CACHE_PRIORITY = 1;
  typedef common::hash::ObHashMap<uint64_t, ObTmpTenantFileStoreHandle,
                                  common::hash::SpinReadWriteDefendMode> TenantFileStoreMap;
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObConcurrentFIFOAllocator allocator_;
  TenantFileStoreMap tenant_file_stores_;
};

#define OB_TMP_FILE_STORE (::oceanbase::blocksstable::ObTmpFileStore::get_instance())

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_TMP_FILE_STORE_H_
