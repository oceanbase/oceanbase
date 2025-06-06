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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_FREE_PAGE_LIST_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_FREE_PAGE_LIST_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/function/ob_function.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileWriteCacheFreePageList
{
public:
  static const int64_t MAX_FREE_LIST_NUM = 1024;
  typedef ObFunction<bool(PageNode *)> Function;

  ObTmpFileWriteCacheFreePageList();
  ~ObTmpFileWriteCacheFreePageList() { destroy(); }
  void destroy();
  void reset();
  int init();
  int push_back(PageNode *node);
  PageNode *pop_front();
  void push_range(ObDList<PageNode> &list);
  int remove_if(Function &in_shrink_range, ObDList<PageNode> &target_list);
  OB_INLINE int64_t size() const { return ATOMIC_LOAD(&size_); }
private:
  bool is_inited_;
  int32_t alloc_idx_;
  int32_t free_idx_;
  int64_t size_;
  ObBucketLock lock_;
  ObDList<PageNode> lists_[MAX_FREE_LIST_NUM];
  // ObDList requires locking when checking its size
  // To minimize lock contention, we maintain the sublist size here
  int list_size_[MAX_FREE_LIST_NUM];
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
