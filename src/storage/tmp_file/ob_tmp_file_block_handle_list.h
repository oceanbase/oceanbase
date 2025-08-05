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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_HANDLE_LIST_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_HANDLE_LIST_H_

#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/container/ob_array.h"
#include "storage/tmp_file/ob_tmp_file_block.h"

namespace oceanbase
{
namespace tmp_file
{

class PopBlockOperator;
class ObTmpFileBlockHandle;

struct PrintOperator {
  PrintOperator(const char *hint) : hint_(hint) {}
  PrintOperator() : hint_(NULL) {}
  int operator()(ObTmpFileBlkNode *node);
  const char *hint_;
};

class ObTmpFileBlockHandleList
{
  friend class PopBlockOperator;
public:
  enum ListType {
    INVALID = -1,
    PREALLOC_NODE = 0,
    FLUSH_NODE = 1
  };
public:
  ObTmpFileBlockHandleList() : type_(INVALID), lock_(), list_() {}
  ~ObTmpFileBlockHandleList() { destroy(); }
  void destroy();
  int init(ListType type);
  int append(ObTmpFileBlockHandle handle);
  int remove(ObTmpFileBlockHandle handle);
  int remove(ObTmpFileBlockHandle handle, bool &is_exist);
  ObTmpFileBlockHandle pop_first();
  int64_t size();
  bool is_empty();
  // Note: this function is remove-safe.
  // applies the given function to each block in the list with the lock held
  template <typename Function> int for_each(Function& fn)
  {
    bool loop_continue = true;
    ObSpinLockGuard guard(lock_);
    DLIST_FOREACH_REMOVESAFE_X(node, list_, loop_continue) {
      loop_continue = fn(node);
    }
    return OB_SUCCESS;
  }
private:
  // Note: This function should only be called within the for_each operator, where lock_ is already held
  int remove_without_lock_(ObTmpFileBlockHandle handle);
  ObTmpFileBlkNode *get_blk_node_(ObTmpFileBlockHandle handle);
private:
  ListType type_;
  ObSpinLock lock_;
  common::ObDList<ObTmpFileBlkNode> list_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_BLOCK_HANDLE_LIST_H_
