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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_hj_batch_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace join;

ObHJBatchMgr::~ObHJBatchMgr()
{
  reset();
}

void ObHJBatchMgr::reset()
{
  FOREACH(p, batch_list_) {
    if (NULL != p->left_) {
      free(p->left_);
      p->left_ = NULL;
    }
    if (NULL != p->right_) {
      free(p->right_);
      p->right_ = NULL;
    }
  }
  batch_list_.clear();
}

int ObHJBatchMgr::next_batch(ObHJBatchPair &batch_pair) {
  int ret = batch_list_.pop_front(batch_pair);
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ITER_END;
  } else if (OB_SUCCESS != ret) {
    LOG_WARN("fail to pop front", K(ret));
  }
  return ret;
}

int ObHJBatchMgr::remove_undumped_batch()
{
  int ret = OB_SUCCESS;
  int64_t size = batch_list_.size();
  int64_t erase_cnt = 0;
  hj_batch_pair_list_type::iterator iter = batch_list_.begin();
  hj_batch_pair_list_type::iterator pre_iter = batch_list_.end();
  while (OB_SUCC(ret) && iter != batch_list_.end()) {
    ObHJBatch *left = iter->left_;
    ObHJBatch *right = iter->right_;
    bool erased = false;
    if (nullptr != left && nullptr != right) {
      if (left->is_dumped()) {
        // right maybe empty
        // if (!right->is_dumped()) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpect batch is not match", K(ret), K(left), K(right));
        // }
      } else if (right->is_dumped()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect batch is not match", K(ret), K(left), K(right));
      } else {
        ++erase_cnt;
        erased = true;
        if (OB_FAIL(batch_list_.erase(iter))) {
          LOG_WARN("failed to remove iter", K(left->get_part_level()), K(left->get_batchno()));
        } else {
          free(left);
          free(right);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect batch is null", K(ret), K(left), K(right));
    }
    if (!erased) {
      pre_iter = iter;
      ++iter;
    } else if (pre_iter != batch_list_.end()) {
      iter = pre_iter;
    } else {
      iter = batch_list_.begin();
    }
  }
  if (0 < erase_cnt) {
    LOG_TRACE("trace remove undumped batch", K(ret), K(erase_cnt));
  }
  if (OB_SUCC(ret) && erase_cnt + batch_list_.size() != size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to remove", K(ret));
  }
  return ret;
}

int ObHJBatchMgr::get_or_create_batch(int32_t level, int64_t part_shift, int32_t batchno, bool is_left, ObHJBatch *&batch,
    bool only_get)
{
  int ret = OB_SUCCESS;
  bool flag = false;
  for (hj_batch_pair_list_type::iterator iter = batch_list_.begin(); iter != batch_list_.end(); iter ++) {
    if (is_left) {
      if (iter->left_->get_part_level() == level && iter->left_->get_batchno() == batchno) {
        batch = iter->left_;
        flag = true;
        break;
      }
    } else {
      if (iter->right_->get_part_level() == level && iter->right_->get_batchno() == batchno) {
        batch = iter->right_;
        flag = true;
        break;
      }
    }
  }

  if (!flag && !only_get) { //not found, should new one
    ObHJBatchPair batch_pair;
    void *buf = alloc_.alloc(sizeof(ObHJBatch));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      batch_count_ ++;
      batch_pair.left_ = new (buf) ObHJBatch(alloc_, buf_mgr_, tenant_id_, level, part_shift, batchno);
    }
    if (OB_SUCC(ret)) {
      buf = alloc_.alloc(sizeof(ObHJBatch));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        batch_count_ ++;
        batch_pair.right_ = new (buf) ObHJBatch(alloc_, buf_mgr_, tenant_id_, level, part_shift, batchno);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_list_.push_front(batch_pair))) {
        LOG_WARN("fail to push batch pair to batch list", K(ret));
      } else {
        if (is_left) {
          batch = batch_pair.left_;
        } else {
          batch = batch_pair.right_;
        }
      }
    }
  }

  return ret;
}
