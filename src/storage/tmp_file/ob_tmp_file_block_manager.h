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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileBlockPageBitmap;
class ObTmpFileBlockManager;

struct ObTmpFileBlockKey final
{
  explicit ObTmpFileBlockKey(const int64_t block_index) : block_index_(block_index) {}
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = murmurhash(&block_index_, sizeof(int64_t), 0);
    return OB_SUCCESS;
  }
  OB_INLINE bool operator==(const ObTmpFileBlockKey &other) const
  {
    return block_index_ == other.block_index_;
  }
  TO_STRING_KV(K(block_index_));
  int64_t block_index_;
};

// search every set of continuous existence or non-existence pages in the range of [start_idx, end_idx_]
class ObTmpFileBlockPageBitmapIterator
{
public:
  ObTmpFileBlockPageBitmapIterator() :
      bitmap_(nullptr), is_inited_(false), cur_idx_(OB_INVALID_INDEX), end_idx_(OB_INVALID_INDEX) {}
  ~ObTmpFileBlockPageBitmapIterator() { reset(); }
  int init(const ObTmpFileBlockPageBitmap *bitmap, const int64_t start_idx, int64_t end_idx);
  void reset();
  int next_range(bool &value, int64_t &start_page_id, int64_t &end_page_id);
  OB_INLINE bool has_next() const { return is_inited_ && cur_idx_ <= end_idx_; }
private:
  const ObTmpFileBlockPageBitmap *bitmap_;
  bool is_inited_;
  int64_t cur_idx_;
  int64_t end_idx_;
};

class ObTmpFileBlockPageBitmap
{
public:
  ObTmpFileBlockPageBitmap() { reset(); }
  ~ObTmpFileBlockPageBitmap() { reset(); }
  OB_INLINE void reset() { MEMSET(bitmap_, 0, PAGE_CAPACITY / 8); }
  int get_value(const int64_t offset, bool &value) const;
  int set_bitmap(const int64_t offset, const bool value);
  int set_bitmap_batch(const int64_t offset, const int64_t count, const bool value);
  // [start, end]
  int is_all_true(const int64_t start, const int64_t end, bool &is_all_true) const;
  int is_all_false(const int64_t start, const int64_t end, bool &is_all_false) const;
  int is_all_true(bool &b_ret) const;
  int is_all_false(bool &b_ret) const;
  OB_INLINE static int64_t get_capacity() { return PAGE_CAPACITY; }
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  static constexpr int64_t PAGE_CAPACITY = ObTmpFileGlobal::BLOCK_PAGE_NUMS;
  // upper_align(PAGE_CAPACITY/8, 8)
  static constexpr int64_t PAGE_BYTE_CAPACITY = (PAGE_CAPACITY / 8 + 7) & ~7;

private:
  uint8_t bitmap_[PAGE_BYTE_CAPACITY];
};

// ObTmpFileBlock records the usage of a macro block.
// each macro block could be referenced by one or more tmp files of a tenant.
class ObTmpFileBlock final
{
  enum BlockState {
    INVALID = -1,
    IN_MEMORY = 0,
    WRITE_BACK = 1,
    ON_DISK = 2,
  };
public:
  ObTmpFileBlock(): block_state_(BlockState::INVALID), lock_(common::ObLatchIds::TMP_FILE_LOCK),
                    page_bitmap_(), block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
                    macro_block_id_(), ref_cnt_(0) {}
  ~ObTmpFileBlock();

  OB_INLINE bool is_valid() const {
    bool b_ret = false;

    if (block_state_ < BlockState::INVALID || block_state_ > BlockState::ON_DISK) {
      b_ret = false;
    } else {
      b_ret = block_index_ != ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
      if (BlockState::ON_DISK == block_state_) {
        b_ret = b_ret && macro_block_id_.is_valid();
      }
    }
    return b_ret;
  }

  int reset();
  int init_block(const int64_t block_index, const int64_t begin_page_id, const int64_t page_num);
  int write_back_start();
  int write_back_failed();
  int write_back_succ(const blocksstable::MacroBlockId macro_block_id);
  int release_pages(const int64_t begin_page_id, const int64_t page_num);
  int get_page_usage(int64_t &page_num) const;
  int inc_ref_cnt();
  int dec_ref_cnt(int64_t &ref_cnt);
  bool on_disk() const;
  int can_remove(bool &can_remove) const;
  blocksstable::MacroBlockId get_macro_block_id() const;
  int64_t get_block_index() const;

  TO_STRING_KV(K(block_index_), K(macro_block_id_),
               K(block_state_),  K(page_bitmap_),
               K(ref_cnt_));

private:
  BlockState block_state_;
  common::SpinRWLock lock_;
  ObTmpFileBlockPageBitmap page_bitmap_; // records the page usage of this block
  int64_t block_index_;
  blocksstable::MacroBlockId macro_block_id_;
  int64_t ref_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileBlock);
};

class ObTmpFileBlockHandle final
{
public:
  ObTmpFileBlockHandle() : ptr_(nullptr) {}
  ObTmpFileBlockHandle(ObTmpFileBlock *block, ObTmpFileBlockManager *tmp_file_blk_mgr);
  ObTmpFileBlockHandle(const ObTmpFileBlockHandle &handle);
  ObTmpFileBlockHandle & operator=(const ObTmpFileBlockHandle &other);
  ~ObTmpFileBlockHandle() { reset(); }
  OB_INLINE ObTmpFileBlock * get() const {return ptr_; }
  bool is_inited() { return nullptr != ptr_ && nullptr != tmp_file_blk_mgr_; }
  void reset();
  int init(ObTmpFileBlock *block, ObTmpFileBlockManager *tmp_file_blk_mgr);
  TO_STRING_KV(KPC(ptr_));
private:
  ObTmpFileBlock *ptr_;
  ObTmpFileBlockManager *tmp_file_blk_mgr_;
};

class ObTmpFileBlockManager final
{
public:
  ObTmpFileBlockManager();
  ~ObTmpFileBlockManager();
  int init(const uint64_t tenant_id, const int64_t block_mem_limit);
  void destroy();
  int create_tmp_file_block(const int64_t begin_page_id, const int64_t page_num,
                            int64_t &block_index);

  int write_back_start(const int64_t block_index);
  int write_back_failed(const int64_t block_index);
  int write_back_succ(const int64_t block_index, const blocksstable::MacroBlockId macro_block_id);
  int release_tmp_file_page(const int64_t block_index,
                            const int64_t begin_page_id, const int64_t page_num);
  int get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list);
  int get_macro_block_count(int64_t &macro_block_count);
  int get_tmp_file_block_handle(const int64_t block_index, ObTmpFileBlockHandle &handle);
  int get_macro_block_id(const int64_t block_index, blocksstable::MacroBlockId &macro_block_id);
  int get_block_usage_stat(int64_t &used_page_num, int64_t &macro_block_count);
  void print_block_usage();
  OB_INLINE common::ObConcurrentFIFOAllocator &get_block_allocator() { return block_allocator_; }
  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(used_page_num_),
               K(physical_block_num_), K(block_index_generator_));
private:
  int remove_tmp_file_block_(const int64_t block_index);
private:
  typedef common::ObLinearHashMap<ObTmpFileBlockKey, ObTmpFileBlockHandle> ObTmpFileBlockMap;
  typedef SpinWLockGuard ExclusiveLockGuard;
  typedef SpinRLockGuard SharedLockGuard;
private:
  class CollectMacroBlockIdFunctor final
  {
  public:
    CollectMacroBlockIdFunctor(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
        : macro_id_list_(macro_id_list) {}
    bool operator()(const ObTmpFileBlockKey &block_index, const ObTmpFileBlockHandle &block);

  private:
    common::ObIArray<blocksstable::MacroBlockId> &macro_id_list_;
  };
private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t used_page_num_;
  uint64_t physical_block_num_;
  uint64_t block_index_generator_;
  ObTmpFileBlockMap block_map_;
  common::ObConcurrentFIFOAllocator block_allocator_;
  common::SpinRWLock stat_lock_; // to protect the consistency of used_page_num_ and physical_block_num_
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileBlockManager);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_
