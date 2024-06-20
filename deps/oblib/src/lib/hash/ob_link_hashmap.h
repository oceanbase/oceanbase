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

#ifndef OCEANBASE_HASH_OB_LINK_HASHMAP_H_
#define OCEANBASE_HASH_OB_LINK_HASHMAP_H_

#include "ob_link_hashmap_deps.h"

#include "lib/lock/ob_tc_ref.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
inline int32_t faa_if_ge(int32_t* addr, int32_t x, int32_t cmp)
{
  int32_t ov = ATOMIC_LOAD(addr);
  int32_t nv = 0;
  while (ov >= cmp && ov != (nv = ATOMIC_VCAS(addr, ov, ov + x))) {
    ov = nv;
  }
  return ov;
}

static ObQSync& get_global_link_hashmap_qsync()
{
  static ObQSync qsync;
  return qsync;
}

// DO NOT use me
class BaseRefHandle
{
public:
  typedef RefNode Node;
  explicit BaseRefHandle(RetireStation& retire_station): qclock_(get_global_qclock()), retire_station_(retire_station), qc_slot_(UINT64_MAX) {}
  ~BaseRefHandle() {}
  virtual void enter_critical() { qc_slot_ = qclock_.enter_critical(); }
  virtual void leave_critical() { qclock_.leave_critical(qc_slot_); }
  virtual void retire(Node* node, HazardList& reclaim_list)
  {
    HazardList retire_list;
    retire_list.push(&node->retire_link_);
    retire_station_.retire(reclaim_list, retire_list);
  }
  virtual void purge(HazardList& reclaim_list) { retire_station_.purge(reclaim_list); }
protected:
  QClock& qclock_;
  RetireStation& retire_station_;
  uint64_t qc_slot_;
};

// Different from RefHandle, ZeroRefHandle will init ref count of node to 1,
// when call revert and ref count is 0, will free_value,
// In RefHandle, ref count of node is initialized to INT32_MAX, before del,
// revert operator will not free_value
class ZeroRefHandle final : public BaseRefHandle
{
public:
  enum { BORN_REF = 1 };
  typedef RefNode Node;
  explicit ZeroRefHandle(RetireStation& retire_station) : BaseRefHandle(retire_station) {}
  void born(Node* node) { (void)ATOMIC_AAF(&node->uref_, BORN_REF); }
  int32_t end(Node* node) { return ATOMIC_AAF(&node->uref_, -BORN_REF); }
  bool inc(Node* node) { return faa_if_ge(&node->uref_, 1, BORN_REF) >= BORN_REF; }
  int32_t dec(Node* node) { return ATOMIC_AAF(&node->uref_, -1); }
};

// balanced read/del performance, realtime reclaim Value, batch/delay reclaim Node.
class RefHandle final : public BaseRefHandle
{
public:
  enum { BORN_REF = INT32_MAX/2 };
  typedef RefNode Node;
  explicit RefHandle(RetireStation& retire_station) : BaseRefHandle(retire_station) {}
  void born(Node* node) { (void)ATOMIC_AAF(&node->uref_, BORN_REF); }
  int32_t end(Node* node) { return ATOMIC_AAF(&node->uref_, -BORN_REF); }
  bool inc(Node* node) { return faa_if_ge(&node->uref_, 1, BORN_REF) >= BORN_REF; }
  int32_t dec(Node* node) { return ATOMIC_AAF(&node->uref_, -1); }
};

// Doc:
// class Key must implement compare() and hash() function.
// class Value must derived from LinkHashValue<Key>
template<typename Key, typename Value, typename AllocHandle=AllocHandle<Key, Value>, typename RefHandle=RefHandle, int64_t SHRINK_THRESHOLD = 8>
class ObLinkHashMap
{
protected:
  typedef DCArrayAlloc ArrayAlloc;
  typedef DCHash<Key, SHRINK_THRESHOLD> Hash;
  typedef typename Hash::Node Node;
  typedef LinkHashNode<Key> HashNode;
  typedef LinkHashValue<Key> HashValue;
  struct Guard
  {
    explicit Guard(RetireStation& retire_station): ref_handle_(retire_station) { ref_handle_.enter_critical(); }
    ~Guard() { ref_handle_.leave_critical(); }
    RefHandle ref_handle_;
  };
  enum { RETIRE_LIMIT = 1024 };
public:
  class Iterator
  {
  public:
    explicit Iterator(ObLinkHashMap& hash): hash_(hash), next_(hash_.next(nullptr)) {}
    ~Iterator() { destroy(); }
    void destroy() {
      if (OB_NOT_NULL(next_)) {
        hash_.revert(next_);
        next_ = nullptr;
      }
    }
    Value* next(Value*& node) {
      if (OB_ISNULL(next_)) {
        node = nullptr;
      } else {
        node = (Value*)next_->hash_val_;
        next_ = hash_.next(next_);
      }
      return node;
    }
    void revert(Value* value)
    {
      if (OB_NOT_NULL(value)) {
        hash_.revert(value);
      }
    }
  private:
    ObLinkHashMap& hash_;
    HashNode* next_;
  };
  static constexpr uint64 MAGIC_CODE = 0x0ceaba5e0ceaba5e;
public:
  explicit ObLinkHashMap(int64_t min_size = 1<<16, int64_t max_size = INT64_MAX)
    : ref_handle_(get_retire_station()),
      hash_(array_alloc_, min_size, max_size),
      magic_code_(0)
  {}
  ObLinkHashMap(AllocHandle alloc_handle, int64_t min_size = 1<<16, int64_t max_size = INT64_MAX)
    : alloc_handle_(alloc_handle),
      ref_handle_(get_retire_station()),
      hash_(array_alloc_, min_size, max_size),
      magic_code_(0) {}
  ~ObLinkHashMap()
  {
    destroy();
  }
  int init(const lib::ObLabel &label = ObModIds::OB_CONCURRENT_HASH_MAP,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    return init(lib::ObMemAttr(tenant_id, label));
  }
  int init(const lib::ObMemAttr &attr)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_tenant_id(attr.tenant_id_))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(attr));
    } else if (OB_FAIL(array_alloc_.init(attr))) {
      COMMON_LOG(ERROR, "array_alloc_ init error", K(ret), K(attr));
    } else {
      magic_code_ = MAGIC_CODE;
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
    ATOMIC_STORE(&magic_code_, 0xffffffffffffffff);
    tg.click();
    purge();
    tg.click();
    hash_.destroy();
  }

  void purge()
  {
    HazardList reclaim_list;
    {
      CriticalGuard(get_global_link_hashmap_qsync());
      get_retire_station().purge(reclaim_list);
      reclaim_nodes(reclaim_list);
    }
    WaitQuiescent(get_global_link_hashmap_qsync());
  }
  int64_t count() const { return hash_.count(); }
  HashNode* next(HashNode* node)
  {
    Guard guard(get_retire_station());
    return next_(node);
  }
  int64_t size() const { return count_handle_.size(); }
  int alloc_value(Value *&value)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value = alloc_handle_.alloc_value())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  void free_value(Value *value)
  {
    static_cast<decltype(this)>(value->hash_node_->host_)->alloc_handle_.free_value(value);
  }
  int create(const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(alloc_value(value)) || OB_ISNULL(value)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(insert_and_get(key, value))) {
      // free directly, cause value->hash_node_ is nullptr.
      alloc_handle_.free_value(value);
      value = nullptr;
    }
    return ret;
  }
  // insert value and get value
  // @note must call hashmap.revert(value) after successful insert_and_get()
  int insert_and_get(const Key &key, Value* value)
  {
    common::ObTimeGuard tg("link_hash::insert_and_get", 100 * 1000);
    int hash_ret = 0;
    Guard guard(get_retire_station());
    HashNode* node = nullptr;
    tg.click();
    if (OB_ISNULL(node = alloc_node(value))) {
      hash_ret = -ENOMEM;
    } else {
      tg.click();
      ref_handle_.born(node);
      (void)ref_handle_.inc(node);
      node->xhref(2);
      node->hash_link_.set(key);
      tg.click();
      while (-EAGAIN == (hash_ret = hash_.insert(key, &node->hash_link_)))
        ;
      tg.click();
      if (OB_LIKELY(0 == hash_ret)) {
        count_handle_.add(1);
      } else {
        node->xhref(-2);
        (void)ref_handle_.dec(node);
        tg.click();
        (void)ref_handle_.end(node);
        tg.click();
        static_cast<decltype(this)>(node->host_)->alloc_handle_.free_node(node);
        tg.click();
      }
    }
    if (tg.get_diff() > 100000) {
      COMMON_LOG(INFO, "ObLinkHashMap insert and get cost too much time", K(tg));
    }
    return err_code_map(hash_ret);
  }

  int del(const Key &key)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    {
      Guard guard(get_retire_station());
      while (-EAGAIN == (hash_ret = hash_.del(key, hash_link)))
        ;
    }
    if (OB_LIKELY(0 == hash_ret)) {
      HazardList reclaim_list;
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      end_uref(node);
      CriticalGuard(get_global_link_hashmap_qsync());
      ref_handle_.retire(node, reclaim_list);
      reclaim_nodes(reclaim_list);
      count_handle_.add(-1);
    }
    return err_code_map(hash_ret);
  }

  int get(const Key &key, Value*& value)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    Guard guard(get_retire_station());
    while (-EAGAIN == (hash_ret = hash_.get(key, hash_link)))
      ;
    if (OB_LIKELY(0 == hash_ret)) {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      if (!try_inc_ref(node)) {
        if (node->uref_ >= 0) {
          hash_ret = -ENOENT; // get after del
        } else {
          hash_ret = -EPERM; // uref < 0, maybe revert too much time.
        }
        COMMON_LOG_RET(WARN, err_code_map(hash_ret), "inc ref error", K(node->uref_), K(lbt()));
      } else {
        value = (Value*)node->hash_val_;
        if (node->uref_ - RefHandle::BORN_REF >= 100000 && (node->uref_ - RefHandle::BORN_REF) % 10000 == 0) {
          COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "uref leak check", K(node->uref_), K(lbt()));
        }
      }
    }
    return err_code_map(hash_ret);
  }
  // Used to get some very small values ​​(by copy), see test_link_hashmap for usage details, no need to revert
  template <typename Function> int operate(const Key &key, Function &fn)
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    Guard guard(get_retire_station());
    while (-EAGAIN == (hash_ret = hash_.get(key, hash_link)))
      ;
    if (OB_LIKELY(0 == hash_ret)) {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      fn(key, (Value*)node->hash_val_);
    }
    return err_code_map(hash_ret);
  }
  void revert(HashNode* node)
  {
#ifndef NDEBUG
    abort_unless(node->host_ == this);
    abort_unless(magic_code_ == MAGIC_CODE);
#else
    if (OB_UNLIKELY(node->host_ != this || magic_code_ != MAGIC_CODE)) {
      COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error", K(node->host_), K(this), K(magic_code_));
    }
#endif
    if (OB_NOT_NULL(node)) {
      dec_uref(node);
    }
  }
  void revert(Value* value)
  {
    if (OB_NOT_NULL(value)) {
      revert(value->hash_node_);
    }
  }
  int contains_key(const Key &key) const
  {
    int hash_ret = 0;
    Node* hash_link = nullptr;
    while (-EAGAIN == (hash_ret = const_cast<Hash&>(hash_).get(key, hash_link)))
      ;
    hash_ret = 0 == hash_ret ? -EEXIST : hash_ret;
    return err_code_map(hash_ret);
  }

  template <typename Function> int map(Function &fn)
  {
    int ret = OB_SUCCESS;
    if (0 != size()) {
      Value* value = nullptr;
      Iterator iter(*this);
      while(OB_SUCC(ret) && OB_NOT_NULL(value = iter.next(value))) {
        if (!fn(value->hash_node_->hash_link_.key_, value)) {
          ret = OB_EAGAIN;
        }
      }
    }
    return ret;
  }

  template <typename Function> int for_each(Function &fn)
  {
    HandleOn<Function> handle_on(*this, fn);
    return map(handle_on);
  }
  template <typename Function> int remove_if(Function &fn)
  {
    RemoveIf<Function> remove_if(*this, fn);
    return map(remove_if);
  }

  const AllocHandle& get_alloc_handle() const { return alloc_handle_; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkHashMap);
  static int err_code_map(int err)
  {
    int ret = OB_SUCCESS;
    switch (err) {
      case 0:           ret = OB_SUCCESS; break;
      case -ENOENT:     ret = OB_ENTRY_NOT_EXIST; break;
      case -EEXIST:     ret = OB_ENTRY_EXIST; break;
      case -ENOMEM:     ret = OB_ALLOCATE_MEMORY_FAILED; break;
      case -EOVERFLOW:  ret = OB_SIZE_OVERFLOW; break;
      case -EPERM:      ret = OB_ERR_UNEXPECTED; break;
      default:          ret = OB_ERROR;
    }
    return ret;
  }

  HashNode* alloc_node(Value* value)
  {
    HashNode* node = nullptr;
    if (OB_ISNULL(value)) {
    } else if (OB_ISNULL(node = alloc_handle_.alloc_node(value))) {
      value->hash_node_ = nullptr;
    } else {
      value->hash_node_ = node;
      node->hash_val_ = value;
      node->host_ = this;
    }
    return node;
  }
  bool try_inc_ref(HashNode* node)
  {
    bool ret = true;
    if (OB_NOT_NULL(node)) {
      if (!ref_handle_.inc(node)) {
        ret = false;
      } else if(is_last_bit_set((uint64_t)node->hash_link_.next_)) {
        dec_uref(node);
        ret = false;
      }
    }
    return ret;
  }
  void dec_uref(HashNode* node)
  {
    if (OB_NOT_NULL(node)) {
      if (0 == ref_handle_.dec(node)) {
        on_uref_clean(node);
      }
    }
  }
  void end_uref(HashNode* node)
  {
    if (OB_NOT_NULL(node)) {
      if (0 == ref_handle_.end(node)) {
        on_uref_clean(node);
      }
    }
  }
  void on_uref_clean(HashNode* node)
  {
    if (OB_NOT_NULL(node)) {
      free_value((Value*)node->hash_val_);
      dec_href(node);
    }
  }
  void dec_href(HashNode* node)
  {
    if (OB_NOT_NULL(node)) {
      if (0 == node->xhref(-1)) {
        static_cast<decltype(this)>(node->host_)->alloc_handle_.free_node(node);
        node = nullptr;
      }
    }
  }
  void reclaim_nodes(HazardList& list)
  {
    ObLink* p = nullptr;
    if (list.size() > 0) {
      while(OB_NOT_NULL(p = list.pop())) {
        HashNode* node = CONTAINER_OF(p, HashNode, retire_link_);
        dec_href(node);
        node = nullptr;
      }
    }
  }
  HashNode* next_(HashNode* node)
  {
    Node* iter = nullptr;
    HashNode* next_node = nullptr;
    while (OB_NOT_NULL(iter = hash_.next(OB_NOT_NULL(node)? &node->hash_link_: nullptr))
           && !try_inc_ref(next_node = CONTAINER_OF(iter, HashNode, hash_link_)))
      ;
    return OB_ISNULL(iter)? nullptr: next_node;
  }

protected:
  template <typename Function>
  class HandleOn
  {
  public:
    HandleOn(ObLinkHashMap &hash, Function &fn) : hash_(hash), handle_(fn) {}
    bool operator()(Key &key, Value* value)
    {
      bool need_continue = handle_(key, value);
      hash_.revert(value);
      return need_continue;
    }
  private:
    ObLinkHashMap &hash_;
    Function &handle_;
  };
  template <typename Function>
  class RemoveIf
  {
  public:
    RemoveIf(ObLinkHashMap &hash, Function &fn) : hash_(hash), predicate_(fn) {}
    bool operator()(Key &key, Value* value)
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
    ObLinkHashMap &hash_;
    Function &predicate_;
  };
  static bool always_true(Key &key, Value *value) { UNUSED(key); UNUSED(value); return true; }
protected:
  static RetireStation& get_retire_station() {
    static RetireStation retire_station(get_global_qclock(), RETIRE_LIMIT);
    return retire_station;
  }
protected:
  CountHandle count_handle_;
  AllocHandle alloc_handle_;
  RefHandle ref_handle_;
  ArrayAlloc array_alloc_;
  Hash hash_;
  uint64_t magic_code_;
};

template<typename Key, typename Value, typename AllocHandle, typename RefHandle=RefHandle>
class ObTenantLinkHashMap : public ObLinkHashMap<Key, Value, AllocHandle, RefHandle>
{
public:
  int create(uint64_t tenant_id, const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;
    if (NULL == (value = this->alloc_handle_.alloc_value(tenant_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_SUCCESS != (ret = this->insert_and_get(key, value))) {
      this->alloc_handle_.free_value(value);
      value = NULL;
    }
    return ret;
  }
  int clean_tenant(uint64_t tenant_id)
  {
    return this->alloc_handle_.clean_tenant(tenant_id);
  }
};

} // namespace common
} // namespace oceanbase


#endif /* OCEANBASE_HASH_OB_LINK_HASHMAP_H_ */
