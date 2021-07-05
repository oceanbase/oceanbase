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

#ifndef OCEANBASE_HASH_OB_LINK_HASHMAP_WITH_HAZARD_VALUE_H_
#define OCEANBASE_HASH_OB_LINK_HASHMAP_WITH_HAZARD_VALUE_H_

#include "ob_link_hashmap_deps.h"

#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {
// In each thread context, there is one hashmap for each type at most.
// And Holding more than one node are also not supported.
template <typename Key, typename Value, typename AllocHandle = AllocHandle<Key, Value>, int64_t SHRINK_THRESHOLD = 8>
class ObLinkHashMapWithHazardValue {
  enum { RETIRE_LIMIT = 1024 };

protected:
  typedef DCArrayAlloc ArrayAlloc;
  typedef DCHash<Key, SHRINK_THRESHOLD> Hash;
  typedef typename Hash::Node Node;
  typedef LinkHashNode<Key> HashNode;
  class Iterator {
  public:
    explicit Iterator(ObLinkHashMapWithHazardValue& hash) : hash_(hash), next_(hash_.next(NULL))
    {}
    ~Iterator()
    {}
    Value* next(HashNode*& node)
    {
      Value* ret = nullptr;
      node = next_;
      ret = node ? (Value*)node->hash_val_ : nullptr;
      if (OB_NOT_NULL(node)) {
        next_ = hash_.next(node);
      }
      return ret;
    }

  private:
    ObLinkHashMapWithHazardValue& hash_;
    HashNode* next_;
  };

public:
  explicit ObLinkHashMapWithHazardValue(int64_t min_size = 1 << 16) : alloc_handle_(), hash_(array_alloc_, min_size)
  {}
  explicit ObLinkHashMapWithHazardValue(AllocHandle& alloc_handle, int64_t min_size = 1 << 16)
      : alloc_handle_(alloc_handle), hash_(array_alloc_, min_size)
  {}
  ~ObLinkHashMapWithHazardValue()
  {
    destroy();
  }
  int init(const lib::ObLabel& label = ObModIds::OB_CONCURRENT_HASH_MAP, const uint64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(label), K(tenant_id));
    } else if (OB_FAIL(array_alloc_.init(label, tenant_id))) {
      COMMON_LOG(ERROR, "array_alloc_ init error", K(ret), K(label), K(tenant_id));
    }
    return ret;
  }

  void reset()
  {
    (void)remove_if(always_true);
  }

  void destroy()
  {
    ObTimeGuard tg("link hash map destroyed", 1000000);
    reset();
    tg.click();
    purge();
    tg.click();
    hash_.destroy();
  }

  void purge()
  {
    HazardList reclaim_list;
    get_retire_station().purge(reclaim_list);
    reclaim_nodes(reclaim_list);
  }
  int64_t count() const
  {
    return hash_.count();
  }

  HashNode* next(HashNode* node)
  {
    return next_(node);
  }

  int64_t size() const
  {
    return count_handle_.size();
  }
  int alloc_value(Value*& value)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value = alloc_handle_.alloc_value())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  void free_value(Value* value)
  {
    alloc_handle_.free_value(value);
  }
  int create(const Key& key, Value*& value)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(alloc_value(value)) || OB_ISNULL(value)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(insert_and_get(key, value))) {
      free_value(value);
      value = nullptr;
    }
    return ret;
  }
  // insert value and get value
  // @note if succeed, revert should be called after using.
  int insert_and_get(const Key& key, Value* value)
  {
    int hash_ret = 0;
    HashNode* node = nullptr;
    if (OB_ISNULL(node = alloc_node(value))) {
      hash_ret = -ENOMEM;
    } else {
      enter_critical();
      node->hash_link_.set(key);
      while (-EAGAIN == (hash_ret = hash_.insert(key, &node->hash_link_)))
        ;
      if (OB_LIKELY(0 == hash_ret)) {
        count_handle_.add(1);
      } else {
        leave_critical();
        alloc_handle_.free_node(node);
      }
    }
    return err_code_map(hash_ret);
  }

  int del(const Key& key)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    enter_critical();
    while (-EAGAIN == (hash_ret = hash_.del(key, hash_link)))
      ;
    leave_critical();
    if (OB_LIKELY(0 == hash_ret)) {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      retire_and_reclaim_node(node);
      count_handle_.add(-1);
    }
    return err_code_map(hash_ret);
  }

  int get(const Key& key, Value*& value)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    enter_critical();
    while (-EAGAIN == (hash_ret = hash_.get(key, hash_link)))
      ;
    if (OB_UNLIKELY(0 != hash_ret)) {
      leave_critical();
    } else {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      if (OB_UNLIKELY(!try_acquire(node))) {
        revert(node);
        hash_ret = -ENOENT;
      } else {
        value = (Value*)node->hash_val_;
      }
    }
    return err_code_map(hash_ret);
  }

  // faster operate, no need to revert.
  template <typename Function>
  int operate(const Key& key, Function& fn)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    enter_critical();
    while (-EAGAIN == (hash_ret = hash_.get(key, hash_link)))
      ;
    if (OB_LIKELY(0 == hash_ret)) {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      fn(key, (Value*)node->hash_val_);
    }
    leave_critical();
    return err_code_map(hash_ret);
  }

  void revert(HashNode* node)
  {
    UNUSED(node);
    leave_critical();
  }

  void revert(Value* value)
  {
    if (OB_NOT_NULL(value)) {
      HashNode* node = (HashNode*)value->hash_node_;
      revert(node);
    }
  }

  int contains_key(const Key& key)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    while (-EAGAIN == (hash_ret = const_cast<Hash&>(hash_).get(key, hash_link)))
      ;
    if (OB_LIKELY(0 == hash_ret)) {
      hash_ret = -EEXIST;
    }
    return err_code_map(hash_ret);
  }

  template <typename Function>
  int map(Function& fn)
  {
    int ret = OB_SUCCESS;
    if (0 != size()) {
      HashNode* node = nullptr;
      Value* value = nullptr;
      enter_critical();
      Iterator iter(*this);
      while (OB_SUCC(ret) && OB_NOT_NULL(value = iter.next(node))) {
        if (OB_NOT_NULL(node)) {
          if (!fn(node->hash_link_.key_, value)) {
            ret = OB_EAGAIN;
          }
          enter_critical();
        }
      }
      leave_critical();
    }
    return ret;
  }

  template <typename Function>
  int for_each(Function& fn)
  {
    HandleOn<Function> handle_on(*this, fn);
    return map(handle_on);
  }
  template <typename Function>
  int remove_if(Function& fn)
  {
    RemoveIf<Function> remove_if(*this, fn);
    return map(remove_if);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkHashMapWithHazardValue);
  static int err_code_map(int err)
  {
    int ret = OB_SUCCESS;
    switch (err) {
      case 0:
        ret = OB_SUCCESS;
        break;
      case -ENOENT:
        ret = OB_ENTRY_NOT_EXIST;
        break;
      case -EEXIST:
        ret = OB_ENTRY_EXIST;
        break;
      case -ENOMEM:
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      case -EOVERFLOW:
        ret = OB_SIZE_OVERFLOW;
        break;
      default:
        ret = OB_ERROR;
    }
    return ret;
  }

  HashNode* alloc_node(Value* value)
  {
    HashNode* node = nullptr;
    if (OB_ISNULL(value)) {
      // do nothing
    } else if (OB_ISNULL(node = alloc_handle_.alloc_node(value))) {
      value->hash_node_ = nullptr;
    } else {
      value->hash_node_ = node;
      node->hash_val_ = value;
    }
    return node;
  }
  bool try_acquire(HashNode* node)
  {
    return !is_last_bit_set((uint64_t)node->hash_link_.next_);
  }
  HashNode* next_(HashNode* node)
  {
    Node* iter = nullptr;
    HashNode* next_node = nullptr;
    while (OB_NOT_NULL(iter = hash_.next(OB_NOT_NULL(node) ? &node->hash_link_ : nullptr)) &&
           OB_UNLIKELY(!try_acquire(next_node = CONTAINER_OF(iter, HashNode, hash_link_))))
      ;
    return OB_ISNULL(iter) ? nullptr : next_node;
  }

private:
  template <typename Function>
  class HandleOn {
  public:
    HandleOn(ObLinkHashMapWithHazardValue& hash, Function& fn) : hash_(hash), handle_(fn)
    {}
    bool operator()(Key& key, Value* value)
    {
      bool need_continue = handle_(key, value);
      hash_.revert(value);
      return need_continue;
    }

  private:
    ObLinkHashMapWithHazardValue& hash_;
    Function& handle_;
  };
  template <typename Function>
  class RemoveIf {
  public:
    RemoveIf(ObLinkHashMapWithHazardValue& hash, Function& fn) : hash_(hash), predicate_(fn)
    {}
    bool operator()(Key& key, Value* value)
    {
      bool need_remove = predicate_(key, value);
      hash_.revert(value);
      if (need_remove) {
        (void)hash_.del(key);
      }
      // always return true
      return true;
    }

  private:
    ObLinkHashMapWithHazardValue& hash_;
    Function& predicate_;
  };
  static bool always_true(Key& key, Value* value)
  {
    UNUSED(key);
    UNUSED(value);
    return true;
  }
  void enter_critical()
  {
    if (OB_UNLIKELY(get_slot_id() != UINT64_MAX)) {
      COMMON_LOG(ERROR, "enter_critical twice");
    } else {
      get_slot_id() = get_qclock().enter_critical();
    }
  }
  void leave_critical()
  {
    if (OB_UNLIKELY(get_slot_id() == UINT64_MAX)) {
      COMMON_LOG(ERROR, "leave_critical twice");
    } else {
      get_qclock().leave_critical(get_slot_id());
      get_slot_id() = UINT64_MAX;
    }
  }
  void retire_and_reclaim_node(HashNode* node)
  {
    HazardList retire_list;
    HazardList reclaim_list;
    retire_list.push(&node->retire_link_);
    get_retire_station().retire(reclaim_list, retire_list);
    reclaim_nodes(reclaim_list);
  }
  void reclaim_nodes(HazardList& list)
  {
    if (list.size() > 0) {
      ObLink* p = nullptr;
      while (OB_NOT_NULL(p = list.pop())) {
        HashNode* node = CONTAINER_OF(p, HashNode, retire_link_);
        free_value((Value*)node->hash_val_);
        alloc_handle_.free_node(node);
        node = nullptr;
      }
    }
  }
  static uint64_t& get_slot_id()
  {
    static __thread uint64_t slot_id = UINT64_MAX;
    return slot_id;
  }
  static QClock& get_qclock()
  {
    static QClock qclock;
    return qclock;
  }
  static RetireStation& get_retire_station()
  {
    static RetireStation retire_station(get_qclock(), RETIRE_LIMIT);
    return retire_station;
  }

private:
  CountHandle count_handle_;
  AllocHandle alloc_handle_;
  ArrayAlloc array_alloc_;
  Hash hash_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OCEANBASE_HASH_OB_LINK_HASHMAP_WITH_HAZARD_VALUE_H_ */
