/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_union_find.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int UnionFind::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_FAIL(parent_.push_back(i))) {
        LOG_WARN("failed to push back node", K(ret));
      } else if (OB_FAIL(tree_size_.push_back(1))) {
        LOG_WARN("failed to push back size", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int UnionFind::connect(int64_t p, int64_t q)
{
  int ret = OB_SUCCESS;
  int64_t root_p = OB_INVALID_INDEX;
  int64_t root_q = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("union find is not initialized", K(ret));
  } else if (OB_FAIL(find_root(p, root_p))) {
    LOG_WARN("failed to find node", K(ret));
  } else if (OB_FAIL(find_root(q, root_q))) {
    LOG_WARN("failed to find node", K(ret));
  } else if (root_p != root_q) {
    if (tree_size_.at(root_p) > tree_size_.at(root_q)) {
      parent_.at(root_q) = root_p;
      tree_size_.at(root_p) += tree_size_.at(root_q);
    } else {
      parent_.at(root_p) = root_q;
      tree_size_.at(root_q) += tree_size_.at(root_p);
    }
    --count_;
  }
  return ret;
}

int UnionFind::find_root(int64_t x, int64_t &root)
{
  int ret = OB_SUCCESS;
  root = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("union find is not initialized", K(ret));
  } else if (x < 0 || x >= parent_.count()) {
    ret = OB_INVALID_INDEX;
    LOG_WARN("invalid index", K(ret));
  } else {
    // compress the height of the tree
    while (parent_.at(x) != x) {
      parent_.at(x) = parent_.at(parent_.at(x));
      x = parent_.at(x);
    }
    root = x;
  }
  return ret;
}

int UnionFind::is_connected(int64_t p,
                            int64_t q,
                            bool &connected)
{
  int ret = OB_SUCCESS;
  connected = false;
  int64_t root_p = OB_INVALID_INDEX;
  int64_t root_q = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("union find is not initialized", K(ret));
  } else if (OB_FAIL(find_root(p, root_p))) {
    LOG_WARN("failed to find node", K(ret));
  } else if (OB_FAIL(find_root(q, root_q))) {
    LOG_WARN("failed to find node", K(ret));
  } else {
    connected = (root_p == root_q);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase