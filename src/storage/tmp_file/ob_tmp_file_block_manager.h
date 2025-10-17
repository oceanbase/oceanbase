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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_block.h"
#include "storage/tmp_file/ob_tmp_file_block_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_block_allocating_priority_manager.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace tmp_file
{
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

class ObTmpFileBlockManager final
{
public:
  ObTmpFileBlockManager();
  ~ObTmpFileBlockManager();
  int init(const uint64_t tenant_id);
  void destroy();
  int alloc_block(int64_t &block_index);
  int alloc_page_range(const int64_t necessary_page_num,
                       const int64_t expected_page_num,
                       ObIArray<ObTmpFileBlockRange>& page_ranges);
  int release_page(const int64_t block_index,
                   const int64_t begin_page_id,
                   const int64_t page_num);
  int acquire_flush_iterator(ObTmpFileBlockFlushIterator &flush_iter);

  void print_blocks();  // for DEBUG
public:
  // for block calling
  // please make sure the calling is under the protection of block lock
  int insert_block_into_alloc_priority_mgr(const int64_t free_page_num, ObTmpFileBlock &block);
  int remove_block_from_alloc_priority_mgr(const int64_t free_page_num, ObTmpFileBlock &block);
  int adjust_block_alloc_priority(const int64_t old_free_page_num, const int64_t free_page_num, ObTmpFileBlock &block);
  int insert_block_into_flush_priority_mgr(const int64_t flushing_page_num, ObTmpFileBlock &block);
  int remove_block_from_flush_priority_mgr(const int64_t flushing_page_num, ObTmpFileBlock &block);
  int adjust_block_flush_priority(const int64_t old_flushing_page_num, const int64_t flushing_page_num, ObTmpFileBlock &block);

public:
  int get_tmp_file_block_handle(const int64_t block_index, ObTmpFileBlockHandle &handle);
  int get_macro_block_id(const int64_t block_index, blocksstable::MacroBlockId &macro_block_id);
  int get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list);
  int get_block_usage_stat(int64_t &flushed_page_num, int64_t &macro_block_count);
  void print_block_usage();
  OB_INLINE common::ObConcurrentFIFOAllocator &get_block_allocator() { return block_allocator_; }
  OB_INLINE ObTmpFileBlockFlushPriorityManager &get_flush_priority_mgr() { return flush_priority_mgr_; }
  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(block_index_generator_));

private:
  int alloc_block_(ObTmpFileBlock *&block, ObTmpFileBlock::BlockType block_type);
  struct PrintBlockOp {
    bool operator()(const ObTmpFileBlockKey &key, const ObTmpFileBlockHandle &value);
  };
private:
  typedef common::ObLinearHashMap<ObTmpFileBlockKey, ObTmpFileBlockHandle> ObTmpFileBlockMap;
  typedef SpinWLockGuard ExclusiveLockGuard;
  typedef SpinRLockGuard SharedLockGuard;
  static const int64_t MAX_PRE_ALLOC_INTERVAL_NUM = 5;

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

  class CollectDiskUsageFunctor final
  {
  public:
    CollectDiskUsageFunctor() : flushed_page_num_(0), macro_block_count_(0) {}
    bool operator()(const ObTmpFileBlockKey &block_index, const ObTmpFileBlockHandle &block);
    OB_INLINE int64_t get_flushed_page_num() const { return flushed_page_num_; }
    OB_INLINE int64_t get_macro_block_count() const { return macro_block_count_; }
  private:
    int64_t flushed_page_num_;
    int64_t macro_block_count_;
  };
private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t block_index_generator_;
  ObTmpFileBlockMap block_map_;
  common::ObConcurrentFIFOAllocator block_allocator_;
  ObTmpFileBlockFlushPriorityManager flush_priority_mgr_;
  ObTmpFileBlockAllocatingPriorityManager alloc_priority_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileBlockManager);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_MANAGER_H_
