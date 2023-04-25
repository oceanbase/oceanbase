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

#ifndef OCEANBASE_HASH_OB_LINK_HASHMAP_DEPS_H_
#define OCEANBASE_HASH_OB_LINK_HASHMAP_DEPS_H_

#include "lib/allocator/ob_retire_station.h"
#include "lib/hash/ob_dchash.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase
{
namespace common
{
class DCArrayAlloc: public IAlloc
{
public:
  DCArrayAlloc(): attr_(OB_SERVER_TENANT_ID, ObModIds::OB_CONCURRENT_HASH_MAP) {}
  virtual ~DCArrayAlloc() {}
  int init(const lib::ObMemAttr &attr)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(attr.tenant_id_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(attr));
    } else {
      attr_ = attr;
    }
    return ret;
  }

  void* alloc(int64_t sz) override
  {
    void *ptr = nullptr;
    if (sz <= 0) {
      COMMON_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", K(sz));
    } else {
      ptr = ob_malloc(sz, attr_);
    }
    return ptr;
  }

  void free(void* p) override
  {
    if (OB_NOT_NULL(p)) {
      ob_free(p);
      p = nullptr;
    }
  }
private:
  DISALLOW_COPY_AND_ASSIGN(DCArrayAlloc);
private:
  lib::ObMemAttr attr_;
};

struct RefNode
{
  RefNode(): retire_link_(), uref_(0), href_(0), host_(nullptr) {}
  ~RefNode() {}
  OB_INLINE int32_t xhref(int32_t x) { return ATOMIC_AAF(&href_, x); }
  ObLink retire_link_;
  int32_t uref_;
  int32_t href_;
  void* host_;
private:
  DISALLOW_COPY_AND_ASSIGN(RefNode);
};

template<typename key_t>
struct LinkHashNode final: RefNode
{
  LinkHashNode(): RefNode(), hash_link_(), hash_val_(nullptr) {}
  ~LinkHashNode() {}
  KeyHashNode<key_t> hash_link_;
  void *hash_val_;
private:
  DISALLOW_COPY_AND_ASSIGN(LinkHashNode);
};

template<typename key_t>
struct LinkHashValue
{
  LinkHashValue(): hash_node_(nullptr) {}
  ~LinkHashValue() {}
  int32_t get_uref() const { return hash_node_->uref_; }
  int32_t get_href() const { return hash_node_->href_; }
  LinkHashNode<key_t> *hash_node_;
private:
  DISALLOW_COPY_AND_ASSIGN(LinkHashValue);
};

// alloc must return constructed Node/Value.
template<typename Key, typename Value>
class AllocHandle
{
public:
  typedef LinkHashNode<Key> Node;
  static Value* alloc_value() { return op_reclaim_alloc(Value); }
  static void free_value(Value* val) { op_reclaim_free(val); val = nullptr; }
  static Node* alloc_node(Value* val) { UNUSED(val); return op_reclaim_alloc(Node); }
  static void free_node(Node* node) { op_reclaim_free(node); node = nullptr; }
};

class CountHandle
{
public:
  CountHandle(): count_(0) {}
  ~CountHandle() {}
  int64_t size() const { return ATOMIC_LOAD(&count_); }
  int64_t add(int64_t x) { return ATOMIC_FAA(&count_, x); }
private:
  int64_t count_;
};
} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_HASH_OB_LINK_HASHMAP_DEPS_H_ */
