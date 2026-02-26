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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_FLUSH_PRIORITY_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_FLUSH_PRIORITY_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_block_handle_list.h"
#include "storage/tmp_file/ob_tmp_file_block.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileBlockFlushPriorityManager final
{
public:
  ObTmpFileBlockFlushPriorityManager() : is_inited_(false) {}
  ~ObTmpFileBlockFlushPriorityManager() { destroy(); }
  int init();
  void destroy();
  // for block calling
  // please make sure the calling is under the protection of block lock
  int insert_block_into_flush_priority_list(const int64_t flushing_page_num, ObTmpFileBlock &block);
  int remove_block_from_flush_priority_list(const int64_t flushing_page_num, ObTmpFileBlock &block);
  int adjust_block_flush_priority(const int64_t old_flushing_page_num,
                                  const int64_t flushing_page_num,
                                  ObTmpFileBlock &block);
  int64_t get_block_count();
  void print_blocks(); // for DEBUG
private:
  friend class ObTmpFileBlockFlushIterator;
  typedef common::ObDList<ObTmpFileBlkNode> ObTmpFileBlockFlushList;
  enum BlockFlushLevel {
    INVALID = -1,
    L1 = 0, // for exclusive block, flushing page num is in (64, 256)
    L2,     // for exclusive block, free page num is in (0, 64]
    L3,     // for shared block, free page num is in (64, 256)
    L4,     // for shared block, free page num is in (0, 64]
    MAX
  };
  BlockFlushLevel get_block_list_level_(const int64_t flushing_page_num, const bool is_exclusive_block) const;
  BlockFlushLevel get_next_level_(const BlockFlushLevel level) const;
  int popN_from_block_list_(const BlockFlushLevel flush_level,
                            const int64_t expected_count, int64_t &actual_count,
                            ObIArray<ObTmpFileBlockHandle> &block_handles);
private:
  bool is_inited_;
  ObTmpFileBlockHandleList flush_lists_[BlockFlushLevel::MAX];
};

struct PopBlockOperator {
  PopBlockOperator(int64_t expected_count,
                    ObTmpFileBlockHandleList &list,
                    ObIArray<ObTmpFileBlockHandle> &block_handles)
    : expected_count_(expected_count), actual_count_(0), list_(list), block_handles_(block_handles) {}
  bool operator()(ObTmpFileBlkNode *node);
private:
  int64_t expected_count_;
  int64_t actual_count_;
  ObTmpFileBlockHandleList &list_;
  ObIArray<ObTmpFileBlockHandle> &block_handles_;
};

class ObTmpFileBlockFlushIterator
{
public:
  ObTmpFileBlockFlushIterator();
  ~ObTmpFileBlockFlushIterator();

  int init(ObTmpFileBlockFlushPriorityManager *prio_mgr);
  void destroy();
  int next(ObTmpFileBlockHandle &block_handle);

  TO_STRING_KV(K(is_inited_), KP(prio_mgr_), K(blocks_.count()), K(cur_level_))
private:
  int reinsert_block_into_flush_priority_list_();
  int cache_blocks_();
private:
  typedef ObTmpFileBlockFlushPriorityManager::BlockFlushLevel BlockFlushLevel;
  static constexpr int64_t MAX_CACHE_NUM = 64;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileBlockFlushIterator);
private:
  bool is_inited_;
  ObTmpFileBlockFlushPriorityManager *prio_mgr_;
  ObSEArray<ObTmpFileBlockHandle, MAX_CACHE_NUM> blocks_;
  BlockFlushLevel cur_level_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_FLUSH_PRIORITY_MANAGER_H_
