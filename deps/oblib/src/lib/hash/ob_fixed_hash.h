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

#ifndef OCEANBASE_HASH_OB_FIXED_HASH_H_
#define OCEANBASE_HASH_OB_FIXED_HASH_H_

#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_hash.h"

namespace oceanbase {
namespace common {
template <typename key_t>
class FixedHash {
public:
  typedef ObLink Link;
  struct node_t : public Link {
    node_t() : hash_(0), key_()
    {}
    explicit node_t(key_t key) : key_(key)
    {
      hash_ = hash_map_calc_hash(key_);
    }
    ~node_t()
    {}
    static uint64_t hash_map_calc_hash(key_t key)
    {
      return calc_hash(key) | 1;
    }

    node_t* set(key_t key)
    {
      hash_ = hash_map_calc_hash(key);
      key_ = key;
      return this;
    }

    bool is_dummy_node()
    {
      return 0 == (hash_ & 1);
    }

    int compare(node_t* that)
    {
      int ret = 0;
      if (this->hash_ > that->hash_) {
        ret = 1;
      } else if (this->hash_ < that->hash_) {
        ret = -1;
      } else if (this->is_dummy_node()) {
        ret = 0;
      } else {
        ret = common::compare(this->key_, that->key_);
      }
      return ret;
    }

    node_t* set_as_bucket(uint64_t idx)
    {
      hash_ = bitrev(idx);
      return this;
    }

    uint64_t get_spk()
    {
      return bitrev(hash_);
    }
    uint64_t hash_;
    key_t key_;
  };

  FixedHash(void* buf, int64_t size) : nodes_((node_t*)buf), limit_(last2n(size / sizeof(node_t)))
  {
    init_buckets(nodes_, limit_);
  }
  ~FixedHash()
  {}

  int insert(key_t key, node_t* node)
  {
    node_t key_node(key);
    return ol_insert(get_pre(&key_node), node->set(key));
  }

  int del(key_t key, node_t*& node)
  {
    node_t key_node(key);
    return ol_del(get_pre(&key_node), &key_node, node);
  }

  int get(key_t key, node_t*& node)
  {
    node_t key_node(key);
    return ol_get(get_pre(&key_node), &key_node, node);
  }

  node_t* next(node_t* node)
  {
    while (NULL != (node = next_node(node)) && node->is_dummy_node())
      ;
    return node;
  }

private:
  node_t* next_node(node_t* node)
  {
    node_t* next = NULL;
    if (NULL == node) {
      next = nodes_;
    } else if (is_last_bit_set((uint64_t)(next = (node_t*)ATOMIC_LOAD(&node->next_)))) {
      next = get_pre(node);
    }
    return next;
  }

  static uint64_t last2n(const uint64_t x)
  {
    return x == 0 ? 0 : (1UL << 63) >> (__builtin_clzll(x));
  }

  static uint64_t bitrev(uint64_t x)
  {
    x = (((x & 0xaaaaaaaaaaaaaaaaUL) >> 1) | ((x & 0x5555555555555555UL) << 1));
    x = (((x & 0xccccccccccccccccUL) >> 2) | ((x & 0x3333333333333333UL) << 2));
    x = (((x & 0xf0f0f0f0f0f0f0f0UL) >> 4) | ((x & 0x0f0f0f0f0f0f0f0fUL) << 4));
    x = (((x & 0xff00ff00ff00ff00UL) >> 8) | ((x & 0x00ff00ff00ff00ffUL) << 8));
    x = (((x & 0xffff0000ffff0000UL) >> 16) | ((x & 0x0000ffff0000ffff) << 16));
    return ((x >> 32) | (x << 32));
  }

  static uint64_t get_idx(uint64_t spk, uint64_t bcnt)
  {
    return bcnt == 0 ? 0 : (spk & (bcnt - 1));
  }

  node_t* get_pre(node_t* key_node)
  {
    return nodes_ + get_idx(key_node->get_spk(), limit_);
  }

  static uint64_t get_bucket_pre_idx(uint64_t idx)
  {
    return idx & ~last2n(idx);
  }

  static int init_buckets(node_t* nodes, int64_t limit)
  {
    int err = 0;
    new (nodes) node_t[limit];
    nodes[0].set_as_bucket(0);
    for (int64_t i = 1; 0 == err && i < limit; i++) {
      node_t* node = nodes + i;
      node->set_as_bucket(i);
      err = ol_insert(nodes + get_bucket_pre_idx(i), node);
    }
    return err;
  }

private:
  node_t* nodes_;
  int64_t limit_;
};

};     // end namespace common
};     // end namespace oceanbase
#endif /* OCEANBASE_HASH_OB_FIXED_HASH_H_ */
