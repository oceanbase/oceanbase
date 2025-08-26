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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileBlockPageBitmap;
class ObTmpFileBlockManager;
class ObTmpFileBlock;
class ObTmpFileBlockFlushingPageIterator;

class ObTmpFileBlockPageBitmap
{
public:
  ObTmpFileBlockPageBitmap() { reset(); }
  ~ObTmpFileBlockPageBitmap() {}
  ObTmpFileBlockPageBitmap &operator=(const ObTmpFileBlockPageBitmap &bitmap);
  OB_INLINE void reset() { MEMSET(bitmap_, 0, PAGE_BYTE_CAPACITY); }
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

struct ObTmpFileBlkNode : public ObDLinkBase<ObTmpFileBlkNode>
{
  ObTmpFileBlkNode(ObTmpFileBlock &block) : block_(block) {}
  ObTmpFileBlock &block_;
};

// ObTmpFileBlock records the usage of a macro block.
// each macro block could be referenced by one or more tmp files of a tenant.
class ObTmpFileBlock final
{
public:
  const static int32_t MAGIC_CODE = 0x1F6095F1;
  typedef common::ObDList<ObTmpFilePage::PageListNode> PageList;
  enum BlockType {
    INVALID = -1,
    SHARED = 0,
    EXCLUSIVE = 1,
  };
public:
  ObTmpFileBlock(): magic_code_(MAGIC_CODE),
                    lock_(common::ObLatchIds::TMP_FILE_LOCK),
                    tmp_file_blk_mgr_(nullptr),
                    block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
                    macro_block_id_(), is_in_deleting_(false), is_in_flushing_(false),
                    ref_cnt_(0), type_(INVALID),
                    free_page_num_(0), prealloc_blk_node_(*this), flush_blk_node_(*this),
                    alloc_page_bitmap_(),
                    flushed_page_bitmap_(), flushing_page_bitmap_(),
                    flushing_page_list_() {}
  ~ObTmpFileBlock();

  int destroy();
  int init(const int64_t block_index, BlockType type, ObTmpFileBlockManager *tmp_file_blk_mgr);
  int alloc_pages(const int64_t page_num);
  int release_pages(const int64_t begin_page_id, const int64_t page_num, bool &can_remove);
  int set_macro_block_id(const blocksstable::MacroBlockId &macro_block_id);
  blocksstable::MacroBlockId get_macro_block_id() const;

public:
  int reinsert_into_flush_prio_mgr();
  int insert_page_into_flushing_list(ObTmpFilePageHandle &page_handle);
  int insert_pages_into_flushing_list(ObIArray<ObTmpFilePageHandle> &page_arr);
  int init_flushing_page_iterator(ObTmpFileBlockFlushingPageIterator &iter,
                                  int64_t &flushing_page_num);
  int set_flushing_status(bool &lock_succ);
  int flush_pages_succ(const int64_t begin_page_id, const int64_t page_num);
  int is_page_flushed(const ObTmpFilePageId &page_id, bool &is_flushed) const;
public:
  bool is_deleting() const;
  bool is_valid() const;
  bool is_valid_without_lock() const;
  int get_disk_usage(int64_t &flushed_page_num) const;
  OB_INLINE int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE int64_t dec_ref_cnt() { return ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE void dec_ref_cnt(int64_t &ref_cnt)
  {
    ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  }
  OB_INLINE int64_t get_block_index() const { return block_index_; }
  OB_INLINE int64_t get_free_page_num_without_lock() const { return free_page_num_; }
  OB_INLINE int64_t get_begin_page_id_without_lock() const { return ObTmpFileGlobal::BLOCK_PAGE_NUMS - free_page_num_; }
  OB_INLINE bool is_exclusive_block() const { return EXCLUSIVE == type_; }
  OB_INLINE bool is_shared_block() const { return SHARED == type_; }
  OB_INLINE ObTmpFileBlkNode &get_prealloc_blk_node() { return prealloc_blk_node_; }
  OB_INLINE ObTmpFileBlkNode &get_flush_blk_node() { return flush_blk_node_; }
  OB_INLINE ObTmpFileBlockManager *get_tmp_file_blk_mgr() { return tmp_file_blk_mgr_; }
  TO_STRING_KV(KP(this), K(block_index_), K(macro_block_id_),
               K(is_in_deleting_), K(is_in_flushing_),
               K(ref_cnt_), K(type_), K(free_page_num_),
               KP(&prealloc_blk_node_), KP(prealloc_blk_node_.get_prev()), KP(prealloc_blk_node_.get_next()),
               KP(&flush_blk_node_), KP(flush_blk_node_.get_prev()), KP(flush_blk_node_.get_next()),
               K(flushing_page_list_.get_size()),
               K(alloc_page_bitmap_),
               K(flushed_page_bitmap_), K(flushing_page_bitmap_),
               KP(tmp_file_blk_mgr_));

private:
  int can_remove_(bool &can_remove) const;
  int remove_page_from_flushing_status_(const int64_t page_id);
  int insert_page_into_flushing_list_(ObTmpFilePageHandle &page_handle);
  int reinsert_into_flush_prio_mgr_();
  int update_block_flush_level_(const int64_t old_flushing_page_num);
private:
  int32_t magic_code_;
  common::SpinRWLock lock_;
  ObTmpFileBlockManager *tmp_file_blk_mgr_;
  int64_t block_index_;
  blocksstable::MacroBlockId macro_block_id_;
  bool is_in_deleting_;
  bool is_in_flushing_;
  int64_t ref_cnt_;
  BlockType type_;
  int64_t free_page_num_;
  ObTmpFileBlkNode prealloc_blk_node_;
  ObTmpFileBlkNode flush_blk_node_;

  // For simplifying logic, each page can only be allocated once.
  ObTmpFileBlockPageBitmap alloc_page_bitmap_; // records the usage of pages that have be allocated
  ObTmpFileBlockPageBitmap flushed_page_bitmap_; // records the usage of pages which have been flushed
  ObTmpFileBlockPageBitmap flushing_page_bitmap_; // records the pages which are in flushing list
  PageList flushing_page_list_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileBlock);
};

class ObTmpFileBlockHandle final
{
public:
  ObTmpFileBlockHandle() : ptr_(nullptr) {}
  ObTmpFileBlockHandle(ObTmpFileBlock *block);
  ObTmpFileBlockHandle(const ObTmpFileBlockHandle &handle);
  ObTmpFileBlockHandle & operator=(const ObTmpFileBlockHandle &other);
  ~ObTmpFileBlockHandle() { reset(); }
  OB_INLINE ObTmpFileBlock * get() const {return ptr_; }
  OB_INLINE bool is_inited() const { return nullptr != ptr_; }
  void reset();
  int init(ObTmpFileBlock *block);
  OB_INLINE ObTmpFileBlock *operator->() const { return ptr_; }
  TO_STRING_KV(KPC(ptr_));
private:
  ObTmpFileBlock *ptr_;
};

// search every set of continuous existence or non-existence pages in the range of [start_idx, end_idx_]
class ObTmpFileBlockPageBitmapIterator final
{
public:
  ObTmpFileBlockPageBitmapIterator() :
      bitmap_(), is_inited_(false), cur_idx_(OB_INVALID_INDEX), end_idx_(OB_INVALID_INDEX) {}
  ~ObTmpFileBlockPageBitmapIterator() { reset(); }
  int init(const ObTmpFileBlockPageBitmap &bitmap, const int64_t start_idx, int64_t end_idx);
  void reset();
  int next_range(bool &value, int64_t &start_page_id, int64_t &end_page_id);
  OB_INLINE bool has_next() const { return is_inited_ && cur_idx_ <= end_idx_; }
  TO_STRING_KV(K(is_inited_), K(cur_idx_), K(end_idx_), K(bitmap_));
private:
  ObTmpFileBlockPageBitmap bitmap_;
  bool is_inited_;
  int64_t cur_idx_;
  int64_t end_idx_;
};

// sort the flushing list of a block and return each set of continuous pages
class ObTmpFileBlockFlushingPageIterator final
{
  friend class ObTmpFileBlock;
public:
  ObTmpFileBlockFlushingPageIterator() : is_inited_(false),
                                         block_handle_(),
                                         flushing_bitmap_iter_(), page_arr_(),
                                         cur_range_start_idx_(-1), cur_range_end_idx_(-1),
                                         cur_idx_(0) {}
  ~ObTmpFileBlockFlushingPageIterator() { destroy(); }
  int init(ObTmpFileBlock &block);
  void reset();
  void destroy();
  int next_range();
  int next_page_in_range(ObTmpFilePageHandle& page_handle);

  TO_STRING_KV(K(is_inited_), KP(this), K(block_handle_), K(cur_range_start_idx_),
               K(cur_range_end_idx_), K(cur_idx_), K(flushing_bitmap_iter_),
               K(page_arr_.count()));
private:
  bool is_inited_;
  ObTmpFileBlockHandle block_handle_;
  ObTmpFileBlockPageBitmapIterator flushing_bitmap_iter_;
  ObSEArray<ObTmpFilePageHandle, ObTmpFileGlobal::BLOCK_PAGE_NUMS> page_arr_;
  int64_t cur_range_start_idx_;
  int64_t cur_range_end_idx_;
  int64_t cur_idx_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_H_
