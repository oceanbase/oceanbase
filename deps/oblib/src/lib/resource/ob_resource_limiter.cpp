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

#include "ob_resource_limiter.h"

using namespace oceanbase::lib;
ObResourceLimiter::ObResourceLimiter(int64_t max, int64_t min)
  : max_(max), min_(min),
    hold_(0), cache_(0), mutex_(),
    parent_(NULL), child_(NULL), next_(NULL)
{}

ObResourceLimiter::~ObResourceLimiter()
{
  if (NULL != parent_) {
    parent_->del_child(this);
    parent_ = NULL;
  }
}
void ObResourceLimiter::add_child(ObResourceLimiter *node)
{
  ObMutexGuard guard(mutex_);
  node->next_ = child_;
  child_ = node;
  node->parent_ = this;
  if (node->hold_ > 0) {
    acquire(node->hold_);
  }
  node->recalculate();
}

void ObResourceLimiter::del_child(ObResourceLimiter *node)
{
  ObMutexGuard guard(mutex_);
  ObResourceLimiter **cur = &child_;
  while (*cur != node && NULL != *cur) {
    cur = &(*cur)->next_;
  }
  if (NULL != *cur) {
    *cur = (*cur)->next_;
  }
  node->parent_ = NULL;
  if (node->hold_ > 0) {
    acquire(-node->hold_);
  }
}

bool ObResourceLimiter::acquire(int64_t quota)
{
  bool bret = true;
  if (quota <= 0) {
    if (ATOMIC_AAF(&hold_, quota) < min_) {
      ATOMIC_AAF(&hold_, -quota);
      ATOMIC_AAF(&cache_, -quota);
    } else if (NULL != parent_) {
      parent_->acquire(quota);
    }
  } else {
    if (ATOMIC_AAF(&cache_, -quota) < 0) {
      ATOMIC_AAF(&cache_, quota);
      if (ATOMIC_AAF(&hold_, quota) > max_) {
        bret = false;
      } else if (NULL != parent_) {
        bret = parent_->acquire(quota);
      }
      if (!bret) {
        ATOMIC_AAF(&hold_, -quota);
      }
    }
  }
  return bret;
}

void ObResourceLimiter::recalculate()
{
  if (NULL != parent_) {
    int64_t delta_hold = MAX(min_ - hold_, -cache_);
    ATOMIC_AAF(&hold_, delta_hold);
    if (!parent_->acquire(delta_hold)) {
      ATOMIC_AAF(&hold_, -delta_hold);
    } else {
      ATOMIC_AAF(&cache_, delta_hold);
    }
  }
}