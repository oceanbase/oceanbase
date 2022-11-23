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

#ifndef OCEANBASE_HASH_OB_FIXED_HASH2_H_
#define OCEANBASE_HASH_OB_FIXED_HASH2_H_

#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_hash.h"

namespace oceanbase
{
namespace common
{
struct SpHashNode: public ObLink
{
  SpHashNode(uint64_t hash=0): hash_(hash) {}
  ~SpHashNode() {}
  bool is_dummy() { return 0 == (hash() & 1); }
  //int compare(SpHashNode* that);
  uint64_t hash() const { return hash_; }
  uint64_t hash_;
};

template<typename Node>
class FixedHash2
{
public:
  typedef SpHashNode BaseNode;
  FixedHash2(void* buf, int64_t size):
      nodes_((Node*)buf), limit_(last2n(size/sizeof(BaseNode))), shift_(__builtin_clzll(limit_) + 1) {
    init_buckets(nodes_, limit_);
  }
  ~FixedHash2() {}

  int insert(Node* node) { return ol_insert(get_pre(node), node); }
  int del(Node* key, Node*& node) { return ol_del(get_pre(key), key, node); }
  template<typename Comparator>
  int del(uint64_t hash, Comparator &cmp, Node*& node) { return ol_del(get_pre(hash), cmp, node); }
  int get(Node* key, Node*& node) { return ol_get(get_pre(key), key, node); }
  template<typename Comparator>
  int get(uint64_t hash, Comparator &cmp, Node*& node) { return ol_get(get_pre(hash), cmp, node); }
  Node* get_next_internal(uint64_t hash) { return (Node*)link_next(get_pre(hash)); }
  Node* quick_next(Node* cur) {
    while(NULL != (cur = quick_next_node(cur))
          && cur->is_dummy())
      ;
    return cur;
  }
  Node* next(Node* cur) {
    while(NULL != (cur = next_node(cur))
          && cur->is_dummy())
      ;
    return cur;
  }
  bool is_empty() {
    Node* iter = NULL;
    bool is_empty = false;
    while (NULL != (iter = quick_next_node(iter))
          && iter->is_dummy())
      ;
    if (NULL != iter && !iter->is_dummy()) {
      is_empty = false;
    } else {
      is_empty = true;
    }
    return is_empty;
  }
  Node *get(uint64_t hash, Node*& node) {
    node = get_pre(hash);
    while (NULL != node  && node->hash() != hash) {
      node = quick_next(node);
    }
    return node;
  }

private:
  Node* quick_next_node(Node* node) {
    Node* next = NULL;
    if (NULL == node) {
      next = (Node*)nodes_;
    } else if (is_last_bit_set((uint64_t)(next = (Node*)ATOMIC_LOAD(&node->next_)))) {
      next = get_pre(node);
    }
    return next;
  }
  Node* next_node(Node* key) {
    Node* prev = NULL;
    return NULL != key? ol_search_next(get_pre(key), key, prev): (Node*)nodes_;
  }
  static uint64_t last2n(uint64_t x) { return x == 0? 0 : (1UL << 63) >> (__builtin_clzll(x)); }
  Node* get_pre(uint64_t hash) { return (Node*)(nodes_ + (hash>>shift_)); }
  Node* get_pre(Node* node) { return get_pre(node->hash()); }
  static void init_buckets(BaseNode* nodes, int64_t limit) {
    uint64_t shift = __builtin_clzll(limit) + 1;
    new(nodes)BaseNode(0);
    for(uint64_t i = 1; i < limit; i++) {
      new(nodes + i)BaseNode(i << shift);
      nodes[i-1].next_ = &nodes[i];
    }
  }

private:
  BaseNode* nodes_;
  int64_t limit_;
  int64_t shift_;
};

}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_HASH_OB_FIXED_HASH2_H_ */
