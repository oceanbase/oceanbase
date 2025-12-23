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

#ifndef _OCEABASE_LIB_ALLOC_OBJECT_SET_H_
#define _OCEABASE_LIB_ALLOC_OBJECT_SET_H_

#include "alloc_struct.h"
#include "lib/lock/ob_simple_lock.h"

namespace oceanbase
{
namespace common
{
class ObAllocator;
}
namespace lib
{
class IBlockMgr;
class ObjectSet
{
public:
  ObjectSet() {
    memset(scs, 0, sizeof(scs));
  }
  ~ObjectSet();
  static bool check_has_unfree(ABlock* block, char *first_label, char *first_bt);
  static int calc_sc_idx(const int64_t x);
  void reset();
  AObject *realloc_object(AObject *obj, const uint64_t size, const ObMemAttr &attr);
  void set_block_mgr(IBlockMgr *blk_mgr) { blk_mgr_ = blk_mgr; }
  IBlockMgr *get_block_mgr() { return blk_mgr_; }
  AObject *alloc_object(const uint64_t size, const ObMemAttr &attr);
  void free_object(AObject *obj, ABlock *block);
  ABlock *alloc_block(const uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock *block);
  void do_cleanup();
  void do_free_object(AObject *obj, ABlock *block);
  bool check_has_unfree(char *first_label, char *first_bt);
  struct SizeClass
  {
    common::ObSimpleLock lock_;
    AObject *local_free_;
    ABlock *avail_blist_;
    ABlock *full_blist_;
  public:
    void push_avail(ABlock *blk) { push(avail_blist_, blk); }
    ABlock *pop_avail() { return pop(avail_blist_); }
    void remove_avail(ABlock *blk) { remove(avail_blist_, blk); }
    void push_full(ABlock *blk) { push(full_blist_, blk); }
    ABlock *pop_full() { return pop(full_blist_); }
    void remove_full(ABlock *blk) { remove(full_blist_, blk); }
  private:
    void push(ABlock *&blist, ABlock *blk)
    {
      if (NULL == blist) {
        blk->prev_ = blk;
        blk->next_ = blk;
      } else {
        blk->prev_ = blist->prev_;
        blist->prev_->next_ = blk;
        blk->next_ = blist;
        blist->prev_ = blk;
      }
      blist = blk;
    }
    ABlock *pop(ABlock *&blist)
    {
      ABlock *blk = blist;
      if (NULL != blk) {
        remove(blist, blk);
      }
      return blk;
    }
    void remove(ABlock *&blist, ABlock *blk)
    {
      if (blk == blk->next_) {
        blist = NULL;
      } else {
        blk->prev_->next_ = blk->next_;
        blk->next_->prev_ = blk->prev_;
        if (blk == blist) { blist = blist->next_; }
      }
    }
  };
#define B_SIZE 64
#define B_MID_SIZE B_SIZE*16
  static constexpr int SIZE_CLASS_MAP[][2] = {
    {B_SIZE, ABLOCK_SIZE * 2},
    {B_SIZE * 2, ABLOCK_SIZE * 2},
    {B_SIZE * 3, ABLOCK_SIZE * 2},
    {B_SIZE * 4, ABLOCK_SIZE * 2},
    {B_SIZE * 5, ABLOCK_SIZE * 2},
    {B_SIZE * 6, ABLOCK_SIZE * 2},
    {B_SIZE * 7, ABLOCK_SIZE * 2},
    {B_SIZE * 8, ABLOCK_SIZE * 2},
    {B_SIZE * 9, ABLOCK_SIZE * 2},
    {B_SIZE * 10, ABLOCK_SIZE * 2},
    {B_SIZE * 11, ABLOCK_SIZE * 2},
    {B_SIZE * 12, ABLOCK_SIZE * 2},
    {B_SIZE * 13, ABLOCK_SIZE * 2},
    {B_SIZE * 14, ABLOCK_SIZE * 2},
    {B_SIZE * 15, ABLOCK_SIZE * 2},
    {B_MID_SIZE, ABLOCK_SIZE * 2},
    {B_MID_SIZE*2, ABLOCK_SIZE * 4},
    {B_MID_SIZE*4, ABLOCK_SIZE * 8},
    {B_MID_SIZE*6, ABLOCK_SIZE * 12},
    {B_MID_SIZE*8, ABLOCK_SIZE * 16},
    {B_MID_SIZE*10, ABLOCK_SIZE * 5},
    {B_MID_SIZE*12, ABLOCK_SIZE * 6},
    {B_MID_SIZE*14, ABLOCK_SIZE * 7},
    {B_MID_SIZE*16, ABLOCK_SIZE * 8},
    {64<<10, (64<<10) * 4},
    {240<<10, (240<<10) * 4}
  };
  static constexpr int SIZE_CLASS_CNT = ARRAYSIZEOF(SIZE_CLASS_MAP);
  static constexpr int MIN_LARGE_IDX = SIZE_CLASS_CNT - 2;
  static constexpr int MAX_LARGE_IDX = MIN_LARGE_IDX + 1;
  static constexpr int OTHER_SC_IDX = SIZE_CLASS_CNT;
#define BIN_SIZE_MAP(sc_idx) SIZE_CLASS_MAP[sc_idx][0]
#define BLOCK_SIZE_MAP(sc_idx) SIZE_CLASS_MAP[sc_idx][1]
  SizeClass scs[SIZE_CLASS_CNT + 1];

  IBlockMgr *blk_mgr_;
} __attribute__((aligned(16)));

} // end of namespace lib
} // end of namespace oceanbase
extern void has_unfree_callback(char *info);
#endif /* _OCEABASE_LIB_ALLOC_OBJECT_SET_H_ */
