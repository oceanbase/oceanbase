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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_write_cache_free_page_list.h"

namespace oceanbase
{
namespace tmp_file
{

ObTmpFileWriteCacheFreePageList::ObTmpFileWriteCacheFreePageList()
  : is_inited_(false),
    alloc_idx_(0),
    free_idx_(0),
    size_(0),
    lock_(),
    lists_()
{
}

void ObTmpFileWriteCacheFreePageList::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int sum = 0;
    for (int32_t i = 0; i < MAX_FREE_LIST_NUM; ++i) {
      sum += lists_[i].get_size();
    }
    if (size_ != sum) {
      LOG_ERROR("free list size is not equal to size", K(sum), K(size_));
    }
    reset();
  }
}

void ObTmpFileWriteCacheFreePageList::reset()
{
  if (IS_INIT) {
    alloc_idx_ = 0;
    free_idx_ = 0;
    size_ = 0;
    lock_.destroy();
    for (int32_t i = 0; i < MAX_FREE_LIST_NUM; ++i) {
      lists_[i].reset();
    }
    for (int32_t i = 0; i < MAX_FREE_LIST_NUM; ++i) {
      list_size_[i] = 0;
    }
    is_inited_ = false;
  }
}

int ObTmpFileWriteCacheFreePageList::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(lock_.init(MAX_FREE_LIST_NUM))) {
    LOG_WARN("failed to init lock", K(ret));
  } else {
    for (int32_t i = 0; i < MAX_FREE_LIST_NUM; ++i) {
      list_size_[i] = 0;
    }
    is_inited_ = true;
  }
  return ret;
}

int ObTmpFileWriteCacheFreePageList::push_back(PageNode *node)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT)  {
    ret = OB_NOT_INIT;
    LOG_WARN("page list is not init", KR(ret));
  } else {
    int32_t idx = ATOMIC_LOAD(&free_idx_);
    ATOMIC_SET(&free_idx_, (idx + 1) % MAX_FREE_LIST_NUM);
    {
      ObBucketWLockGuard bucket_guard(lock_, idx);
      if (OB_UNLIKELY(!lists_[idx].add_last(node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to add free page", KR(ret), K(size()), K(idx), KPC(node));
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&size_, 1);
      ATOMIC_AAF(&list_size_[idx], 1);
    }
  }
  return ret;
}

PageNode* ObTmpFileWriteCacheFreePageList::pop_front()
{
  int ret = OB_SUCCESS;
  PageNode *node = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("page list is not init", KR(ret));
  } else if (ATOMIC_LOAD(&size_) == 0) {
    // do nothing
  } else {
    int32_t cnt = 0;
    int32_t idx = ATOMIC_LOAD(&alloc_idx_);
    while (cnt <= MAX_FREE_LIST_NUM && ATOMIC_LOAD(&list_size_[idx]) == 0) {
      idx = (idx + 1) % MAX_FREE_LIST_NUM;
      cnt += 1;
    }
    ATOMIC_SET(&alloc_idx_, (idx + 1) % MAX_FREE_LIST_NUM);
    if (cnt > MAX_FREE_LIST_NUM || idx >= MAX_FREE_LIST_NUM) {
      LOG_DEBUG("can not get free page, list is empty", K(size()));
    } else{
      {
        ObBucketWLockGuard bucket_guard(lock_, idx);
        node = lists_[idx].remove_first();
      }
      if (OB_NOT_NULL(node)) {
        ATOMIC_DEC(&list_size_[idx]);
        ATOMIC_DEC(&size_);
      }
    }
  }
  return node;
}

void ObTmpFileWriteCacheFreePageList::push_range(ObDList<PageNode> &list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("page list is not init", KR(ret));
  } else {
    int32_t idx = ATOMIC_LOAD(&free_idx_);
    int64_t sub_list_size = list.get_size();
    ATOMIC_SET(&free_idx_, (idx + 1) % MAX_FREE_LIST_NUM);
    {
      ObBucketWLockGuard bucket_guard(lock_, idx);
      lists_[idx].push_range(list);
    }
    ATOMIC_AAF(&size_, sub_list_size);
    ATOMIC_AAF(&list_size_[idx], sub_list_size);
    LOG_DEBUG("free page list push range", KR(ret), K(size()));
  }
}

int ObTmpFileWriteCacheFreePageList::remove_if(Function &in_shrink_range, ObDList<PageNode> &target_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("page list is not init", KR(ret));
  }
  int64_t purge_cnt = 0;
  for (int32_t i = 0; OB_SUCC(ret) && i < MAX_FREE_LIST_NUM; ++i) {
    ObBucketWLockGuard bucket_guard(lock_, i);
    PageNode *head = lists_[i].get_header();
    PageNode *curr = lists_[i].get_first();
    PageNode *prev = nullptr;
    while (OB_SUCC(ret) && OB_NOT_NULL(curr) && head != curr) {
      PageNode *next = curr->get_next();
      PageNode *tmp = nullptr;
      if (!in_shrink_range(curr)) {
        prev = curr;
      } else if (OB_NOT_NULL(tmp = lists_[i].remove(curr))) {
        if (!target_list.add_last(tmp)) {
          LOG_ERROR("fail to add page", KR(ret), K(tmp->page_));
        } else {
          ATOMIC_DEC(&size_);
          ATOMIC_DEC(&list_size_[i]);
          purge_cnt += 1;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to remove page", KR(ret), K(curr->page_));
      }
      curr = next;
    }
  }
  LOG_DEBUG("free page list purge pages", KR(ret), K(purge_cnt));
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
