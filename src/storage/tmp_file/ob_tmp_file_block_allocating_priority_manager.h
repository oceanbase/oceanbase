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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_ALLOCATING_PRIORITY_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_ALLOCATING_PRIORITY_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_block_handle_list.h"
#include "storage/tmp_file/ob_tmp_file_block.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace tmp_file
{
struct ObTmpFileBlockRange
{
public:
  ObTmpFileBlockRange() : block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX),
                          page_index_(0), page_cnt_(0) {}
  ObTmpFileBlockRange(int64_t block_index, int16_t page_index, int16_t page_cnt) :
                      block_index_(block_index),
                      page_index_(page_index), page_cnt_(page_cnt) {}
  ~ObTmpFileBlockRange() { reset(); }
  int init(const int64_t block_index, const int16_t page_index, const int16_t page_cnt);
  void reset();
  bool is_valid() const;

public:
  int64_t block_index_;
  int16_t page_index_;
  int16_t page_cnt_;
  TO_STRING_KV(K(block_index_), K(page_index_), K(page_cnt_));
};

class ObTmpFileBlockAllocatingPriorityManager final
{
public:
  ObTmpFileBlockAllocatingPriorityManager() : is_inited_(false), alloc_lists_() {};
  ~ObTmpFileBlockAllocatingPriorityManager() {};
  int init();
  void destroy();
  int alloc_page_range(const int64_t necessary_page_num,
                       const int64_t expected_page_num,
                       int64_t &remain_page_num,
                       ObIArray<ObTmpFileBlockRange>& page_ranges);
  // for block calling
  // please make sure the calling is under the protection of block lock
  int insert_block_into_alloc_priority_list(const int64_t free_page_num, ObTmpFileBlock &block);
  int remove_block_from_alloc_priority_list(const int64_t free_page_num, ObTmpFileBlock &block);
  int adjust_block_alloc_priority(const int64_t old_free_page_num,
                                  const int64_t free_page_num,
                                  ObTmpFileBlock &block);
  int64_t get_block_count();
  void print_blocks(); // for DEBUG

private:
  typedef common::ObDList<ObTmpFileBlkNode> ObTmpFileBlockAllocList;
  enum BlockPreAllocLevel {
    INVALID = -1,
    L1 = 0, // free page num is in (128, 256)
    L2,     // free page num is in (64, 128]
    L3,     // free page num is in (0, 64]
    MAX
  };
  struct GetAllocatableBlockOp {
    GetAllocatableBlockOp(int64_t candidate_page_num,
                          int64_t necessary_page_num,
                          ObIArray<ObTmpFileBlockHandle> &candidate_blocks);
    bool operator()(ObTmpFileBlkNode *node);
    int64_t candidate_page_num_;
    int64_t necessary_page_num_;
    ObIArray<ObTmpFileBlockHandle> &candidate_blocks_;
  };
  BlockPreAllocLevel get_block_list_level_(const int64_t free_page_num) const;
  BlockPreAllocLevel get_next_level_(const BlockPreAllocLevel level) const;
private:
  bool is_inited_;
  ObTmpFileBlockHandleList alloc_lists_[BlockPreAllocLevel::MAX];
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_ALLOCATING_PRIORITY_MANAGER_H_
