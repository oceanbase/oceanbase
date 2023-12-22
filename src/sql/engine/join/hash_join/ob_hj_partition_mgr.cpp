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
#include "sql/engine/join/hash_join/ob_hj_partition_mgr.h"

namespace oceanbase
{
namespace sql
{

ObHJPartitionMgr::~ObHJPartitionMgr()
{
  reset();
}

void ObHJPartitionMgr::reset()
{
  FOREACH(p, part_pair_list_) {
    if (NULL != p->left_) {
      free(p->left_);
      p->left_ = NULL;
    }
    if (NULL != p->right_) {
      free(p->right_);
      p->right_ = NULL;
    }
  }
  part_pair_list_.clear();
}

int ObHJPartitionMgr::next_part_pair(ObHJPartitionPair &part_pair) {
  int ret = part_pair_list_.pop_front(part_pair);
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ITER_END;
  } else if (OB_SUCCESS != ret) {
    LOG_WARN("fail to pop front", K(ret));
  }
  return ret;
}

int ObHJPartitionMgr::remove_undumped_part(int64_t cur_dumped_partition, int32_t part_round)
{
  int ret = OB_SUCCESS;
  int64_t size = part_pair_list_.size();
  int64_t erase_cnt = 0;
  ObHJPartitionPairList::iterator iter = part_pair_list_.begin();
  ObHJPartitionPairList::iterator pre_iter = part_pair_list_.end();
  while (OB_SUCC(ret) && iter != part_pair_list_.end()) {
    ObHJPartition *left = iter->left_;
    ObHJPartition *right = iter->right_;
    bool erased = false;
    if (nullptr != left && nullptr != right) {
      if (left->is_dumped()) {
        // right maybe empty
        // if (!right->is_dumped()) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpect part is not match", K(ret), K(left), K(right));
        // }
        if ((part_round == left->get_partno() >> 32) &&
            (INT64_MAX != cur_dumped_partition &&
             (left->get_partno() & PARTITION_IDX_MASK) <= cur_dumped_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpect part is not match", K(ret), K(left), K(right),
            K(left->get_partno()), K(cur_dumped_partition), K(part_round));
        }
      } else if (right->is_dumped()) {
        // left maybe empty
        if (0 != left->get_row_count_in_memory()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect part is not match", K(ret), K(left), K(right));
        }
        if ((part_round == right->get_partno() >> 32) &&
            (INT64_MAX != cur_dumped_partition && (right->get_partno() & PARTITION_IDX_MASK) <= cur_dumped_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpect part is not match", K(ret), K(left), K(right),
            K(right->get_partno()), K(cur_dumped_partition));
        }
      } else if (INT64_MAX == cur_dumped_partition
              || (part_round == (left->get_partno() >> 32) && (left->get_partno() & PARTITION_IDX_MASK) <= cur_dumped_partition)) {
        ++erase_cnt;
        erased = true;
        LOG_DEBUG("debug remove undumped partition", K(ret), K(left), K(right),
          K(left->get_partno()), K(left->get_part_level()), K(left->get_partno() >> 32),
          K(left->get_partno() & PARTITION_IDX_MASK));
        if (OB_FAIL(part_pair_list_.erase(iter))) {
          LOG_WARN("failed to remove iter", K(left->get_part_level()), K(left->get_partno()));
        } else {
          free(left);
          free(right);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect part is null", K(ret), K(left), K(right));
    }
    if (!erased) {
      pre_iter = iter;
      ++iter;
    } else if (pre_iter != part_pair_list_.end()) {
      iter = pre_iter;
    } else {
      iter = part_pair_list_.begin();
    }
  }
  if (0 < erase_cnt) {
    LOG_TRACE("trace remove undumped part", K(ret), K(erase_cnt));
  }
  if (OB_SUCC(ret) && erase_cnt + part_pair_list_.size() != size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to remove", K(ret));
  }
  return ret;
}

int ObHJPartitionMgr::get_or_create_part(int32_t level,
                                         int64_t part_shift,
                                         int64_t partno,
                                         bool is_left,
                                         ObHJPartition *&part,
                                         bool only_get)
{
  int ret = OB_SUCCESS;
  bool flag = false;
  for (ObHJPartitionPairList::iterator iter = part_pair_list_.begin();
       iter != part_pair_list_.end();
       iter ++) {
    if (is_left) {
      if (iter->left_->get_part_level() == level && iter->left_->get_partno() == partno) {
        part = iter->left_;
        flag = true;
        break;
      }
    } else {
      if (iter->right_->get_part_level() == level && iter->right_->get_partno() == partno) {
        part = iter->right_;
        flag = true;
        break;
      }
    }
  }

  if (!flag && !only_get) { //not found, should new one
    ObHJPartitionPair part_pair;
    void *buf1 = alloc_.alloc(sizeof(ObHJPartition));
    void *buf2 = nullptr;
    if (NULL == buf1) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      part_count_ ++;
      part_pair.left_ = new (buf1) ObHJPartition(alloc_, tenant_id_, level, part_shift, partno);
    }
    if (OB_SUCC(ret)) {
      buf2 = alloc_.alloc(sizeof(ObHJPartition));
      if (NULL == buf2) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        part_count_ ++;
        part_pair.right_ = new (buf2) ObHJPartition(alloc_, tenant_id_, level, part_shift, partno);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_pair_list_.push_front(part_pair))) {
        LOG_WARN("fail to push part pair to part list", K(ret));
      } else {
        LOG_DEBUG("push front part", K(partno), K(is_left));
        if (is_left) {
          part = part_pair.left_;
        } else {
          part = part_pair.right_;
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != buf1) {
        alloc_.free(buf1);
      }
      if (nullptr != buf2) {
        alloc_.free(buf2);
      }
    }
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
