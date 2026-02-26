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

#ifndef OB_CONSISTENT_HASHING_LOAD_BALANCER_H_
#define OB_CONSISTENT_HASHING_LOAD_BALANCER_H_

#include "lib/container/ob_vector.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{

struct MurmurHashFunc
{
  uint64_t operator()(const void *data, int32_t len, uint64_t hash) const
  {
    return common::murmurhash(data, len, hash);
  }
};

template<int64_t virtual_node_cnt = 50,
         typename HashFunc = MurmurHashFunc>
class ObConsistentHashingLoadBalancer
{
private:
  struct VirtualNode
  {
    VirtualNode(const ObAddr &addr, const int64_t node_id) :
      svr_addr_(addr), virtual_node_id_(node_id), hash_val_(0)
    {}
    uint64_t hash()
    {
      hash_val_ = svr_addr_.hash();
      hash_val_ = common::murmurhash(&virtual_node_id_, sizeof(int64_t), hash_val_);
      return hash_val_;
    }
    VirtualNode &operator=(const VirtualNode &other)
    {
      if (this != &other) {
        svr_addr_ = other.svr_addr_;
        virtual_node_id_ = other.virtual_node_id_;
        hash_val_ = other.hash_val_;
      }
      return *this;
    }
    bool operator ==(const VirtualNode &r) const
    {
      bool is_eq = false;
      if (hash_val_ != r.hash_val_) {
        is_eq = true;
      } else if (virtual_node_id_ != r.virtual_node_id_) {
        is_eq = true;
      } else {
        is_eq = (svr_addr_ != r.svr_addr_);
      }
      return is_eq;
    }
    bool operator < (const VirtualNode &r) const
    {
      bool is_less = false;
      if (hash_val_ != r.hash_val_) {
        is_less = (hash_val_ < r.hash_val_);
      } else if (virtual_node_id_ != r.virtual_node_id_) {
        is_less = (virtual_node_id_ < r.virtual_node_id_);
      } else {
        is_less = (svr_addr_ < r.svr_addr_);
      }
      return is_less;
    }
    TO_STRING_KV(K_(svr_addr),
                 K_(virtual_node_id),
                 K_(hash_val));

    ObAddr svr_addr_;
    int64_t virtual_node_id_;
    uint64_t hash_val_;
  };

  struct VirtualNodeCmp
  {
    bool operator()(const VirtualNode &l, const VirtualNode &r)
    {
      return l < r;
    }
  };

  struct VirtualNodeUnique
  {
    bool operator()(const VirtualNode &l, const VirtualNode &r)
    {
      return l == r;
    }
  };

  struct VirtualNodeHashCmp
  {
    bool operator()(const VirtualNode &l, const uint64_t hash_val)
    {
      return l.hash_val_ < hash_val;
    }
  };

  typedef common::ObSortedVector<VirtualNode> NodeList;

public:
  ObConsistentHashingLoadBalancer() : hash_func_(), node_list_()
  {}

  ~ObConsistentHashingLoadBalancer()
  {}
  int add_server_list(const common::ObIArray<ObAddr> &target_servers)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); ++i) {
      if (OB_FAIL(add_server(target_servers.at(i)))) {
        SQL_ENG_LOG(WARN, "failed to add server", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      node_list_.sort(VirtualNodeCmp());
    }
    return ret;
  }
  // select server by file url
  int select_server(const ObString &file_url, ObAddr &addr)
  {
    int ret = OB_SUCCESS;
    if (file_url.empty()) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "empty file url", K(ret));
    } else {
      uint64_t hash_val = 0;
      hash_val = hash_func_(file_url.ptr(), file_url.length(), hash_val);
      typename NodeList::iterator iter =
        std::lower_bound(node_list_.begin(), node_list_.end(), hash_val, VirtualNodeHashCmp());
      if (iter == node_list_.end()) {
        iter = node_list_.begin();
      }
      addr = iter->svr_addr_;
#ifndef NDEBUG
      SQL_ENG_LOG(INFO, "select server", K(file_url), K(addr), KPC(iter));
#endif
    }
    return ret;
  }
  // select server by hash part idx
  int select_server(const int32_t part_idx, ObAddr &addr)
  {
    int ret = OB_SUCCESS;
    if (part_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "get invalid part idx", K(ret), K(part_idx));
    } else {
      uint64_t hash_val = 0;
      hash_val = hash_func_(&part_idx, sizeof(int32_t), hash_val);
      typename NodeList::iterator iter =
        std::lower_bound(node_list_.begin(), node_list_.end(), hash_val, VirtualNodeHashCmp());
      if (iter == node_list_.end()) {
        iter = node_list_.begin();
      }
      addr = iter->svr_addr_;
#ifndef NDEBUG
      SQL_ENG_LOG(INFO, "select server", K(part_idx), K(addr), KPC(iter));
#endif
    }
    return ret;
  }
  int remove_server(const ObAddr &addr)
  {
    int ret = OB_SUCCESS;
    // there is no such demand, and this interface is not supported yet.
    ret = OB_NOT_SUPPORTED;
    return ret;
  }

private:
  int add_server(const ObAddr &addr)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < virtual_node_cnt; ++i) {
      VirtualNode v_node(addr, i);
      v_node.hash();
      if (OB_FAIL(node_list_.push_back(v_node))) {
        SQL_ENG_LOG(WARN, "failed to add virtual node", K(ret));
      }
    }
    return ret;
  }

private:
  HashFunc hash_func_;
  NodeList node_list_;
};

using ObDefaultLoadBalancer = ObConsistentHashingLoadBalancer<50, MurmurHashFunc>;

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_CONSISTENT_HASHING_LOAD_BALANCER_H_ */