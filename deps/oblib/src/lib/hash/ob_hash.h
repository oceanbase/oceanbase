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

#ifndef OCEANBASE_HASH_OB_HASH_
#define OCEANBASE_HASH_OB_HASH_

#include "lib/hash/ob_darray.h"
namespace oceanbase
{
namespace common
{
typedef IAlloc IHashAlloc;

inline uint64_t next2n(const uint64_t x)
{
  return x <= 2 ? x : (1UL << 63) >> (__builtin_clzll(x - 1) - 1);
}

// b63 b62 .. b1 b0 => b0 b1 .. b62 b63
inline uint64_t bitrev(uint64_t x)
{
  x = (((x & 0xaaaaaaaaaaaaaaaaUL) >> 1) | ((x & 0x5555555555555555UL) << 1));
  x = (((x & 0xccccccccccccccccUL) >> 2) | ((x & 0x3333333333333333UL) << 2));
  x = (((x & 0xf0f0f0f0f0f0f0f0UL) >> 4) | ((x & 0x0f0f0f0f0f0f0f0fUL) << 4));
  x = (((x & 0xff00ff00ff00ff00UL) >> 8) | ((x & 0x00ff00ff00ff00ffUL) << 8));
  x = (((x & 0xffff0000ffff0000UL) >> 16) | ((x & 0x0000ffff0000ffff) << 16));
  return ((x >> 32) | (x << 32));
}

inline uint64_t rand64(uint64_t h)
{
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccd;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53;
  h ^= h >> 33;
  return h;
}

inline int compare(uint64_t k1, uint64_t k2)
{
  return k1 > k2 ? +1 : (k1 < k2 ? -1 : 0);
}

inline int compare(int64_t k1, int64_t k2)
{
  return k1 > k2 ? +1 : (k1 < k2 ? -1 : 0);
}

inline int compare(uint32_t k1, uint32_t k2)
{
  return k1 > k2 ? +1 : (k1 < k2 ? -1 : 0);
}

inline uint64_t calc_hash(uint64_t hash)
{
  return rand64(hash);
}

inline uint64_t calc_hash(int64_t hash)
{
  return rand64(hash);
}

template<typename key_t>
uint64_t calc_hash(key_t &key)
{
  return key.hash();
}

template<typename key_t>
int compare(key_t &k1, key_t &k2)
{
  return k1.compare(k2);
}

template<typename key_t>
uint64_t hash_map_calc_hash(key_t &key)
{
  return calc_hash(key) | 1ULL;
}

struct HashRoot
{
  HashRoot() : root_(NULL), node_cnt_(), slot_limit_(0), filled_slot_(0)
  {
    node_cnt_.set_precision(0);
  }
  ~HashRoot() {}

  uint64_t get_slot_limit()
  {
    return ATOMIC_LOAD(&slot_limit_);
  }
  void update_size(int64_t x)
  {
    int64_t node_cnt = 0;
    if (node_cnt_.inc(x, node_cnt)) {
      if (node_cnt < 1) {
        node_cnt = 1;
      }
      int64_t slot_limit = ATOMIC_LOAD(&slot_limit_);
      if (node_cnt > 2 * slot_limit) {
        ATOMIC_STORE(&slot_limit_, node_cnt);
        set_filled(node_cnt);
      } else if (node_cnt < slot_limit / 2) {
        ATOMIC_STORE(&slot_limit_, node_cnt);
      }
    }
  }
  void set_filled(int64_t idx)
  {
    inc_update(&filled_slot_, idx);
  }
  int64_t try_purge(int64_t batch)
  {
    int64_t purge_idx = -1;
    int64_t slot_limit = ATOMIC_LOAD(&slot_limit_);
    int64_t filled_slot = ATOMIC_LOAD(&filled_slot_);
    int64_t idx = 0;
    if (filled_slot - batch < slot_limit) {
    } else if ((idx = ATOMIC_FAA(&filled_slot_, -batch)) - batch >= slot_limit) {
      purge_idx = idx;
    } else {
      set_filled(filled_slot);
    }
    return purge_idx;
  }

  ArrayHead *root_;
  EstimateCounter node_cnt_;
  int64_t slot_limit_ CACHE_ALIGNED;
  int64_t filled_slot_ CACHE_ALIGNED;
};

template<typename key_t, typename val_t>
class HashBase
{
public:
  class IKVRCallback
  {
  public:
    IKVRCallback() {}
    virtual ~IKVRCallback() {}
    virtual void reclaim_key_value(key_t &key, val_t &value) = 0;
    virtual void ref_key_value(key_t &key, val_t &value)
    {
      UNUSED(key);
      UNUSED(value);
    }
    virtual void release_key_value(key_t &key, val_t &value)
    {
      UNUSED(key);
      UNUSED(value);
    }
  };

  //typedef void* val_t;
  typedef ArrayBase::Handle ArrayHandle;
  struct DMark
  {
    const static uint64_t DELETE_MASK = 1ULL;
    static bool is_set(void *node)
    {
      return 0 != ((uint64_t)node & DELETE_MASK);
    }
    static void *set(void *node)
    {
      return reinterpret_cast<void *>((uint64_t)node | DELETE_MASK);
    }
    static void *unset(void *node)
    {
      return reinterpret_cast<void *>((uint64_t)node & ~DELETE_MASK);
    }
  };

  struct Node: public HazardNode
  {
    Node(): HazardNode(), next_(NULL), is_deleted_(false), hash_(0), key_(), val_() {}
    ~Node() {}
    Node *set_dummy(uint64_t idx)
    {
      hash_ = bitrev(idx);
      return this;
    }
    inline void set_deleted() { is_deleted_ = true; }
    bool is_deleted() const { return is_deleted_; }
    uint64_t get_spk() const { return bitrev(hash_); }
    Node *set(key_t key)
    {
      hash_ = hash_map_calc_hash(key);
      key_ = key;
      return this;
    }
    Node *set(key_t key, val_t val)
    {
      hash_ = hash_map_calc_hash(key);
      key_ = key;
      val_ = val;
      return this;
    }
    bool is_dummy_node()
    {
      return 0 == (hash_ & 1);
    }
    int compare(Node *that)
    {
      int ret = 0;
      if (OB_ISNULL(that)) {
        COMMON_LOG(ERROR, "node compare error, null node", K(lbt()));
      } else if (this->hash_ > that->hash_) {
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
    Node *get_next()
    {
      return static_cast<Node *>(DMark::unset(ATOMIC_LOAD(&next_)));
    }
    Node *get_next(bool &is_mark)
    {
      Node *next = ATOMIC_LOAD(&next_);
      is_mark = DMark::is_set(next);
      return static_cast<Node *>(DMark::unset((void *)next));
    }
    bool is_delete_mark_set() const
    {
      return DMark::is_set(ATOMIC_LOAD(&next_));
    }
    bool set_delete_mark()
    {
      Node *next = ATOMIC_LOAD(&next_);
      return !DMark::is_set(next) && ATOMIC_BCAS(&next_, next, (Node*)DMark::set(next));
    }
    void clear_delete_mark()
    {
      ATOMIC_STORE(&next_, static_cast<Node *>(DMark::unset(ATOMIC_LOAD(&next_))));
    }
    bool cas_next(Node *oldv, Node *newv)
    {
      return ATOMIC_BCAS(&next_, oldv, newv);
    }

    Node *next_;
    bool is_deleted_;
    uint64_t hash_;
    key_t key_;
    val_t val_;
  }; // end struct Node

  class Iterator
  {
  public:
    Iterator(): head_(NULL) {}
    ~Iterator() {}
    void set_head(Node *head) { head_ = head; }
    int next(key_t &key, val_t &val)
    {
      int err = 0;
      while (0 == err) {
        if (NULL == head_) {
          err = -ENOENT;
        } else if (head_->is_dummy_node()) {
          head_ = head_->get_next();
        } else {
          key = head_->key_;
          val = head_->val_;
          head_ = head_->get_next();
          break;
        }
      }
      return err;
    }
  private:
    Node *head_;
  };
  class BaseHandle
  {
  public:
    explicit BaseHandle(HashBase &host):
        alloc_(host.get_alloc()),
        kvr_callback_(host.get_kvr_callback()),
        hazard_handle_(host.get_hazard_ref()),
        retire_list_handle_(host.get_hazard_ref(), host.get_retire_list())
    {
      acquire_ref();
    }
    virtual ~BaseHandle() { release_ref(); }
    bool is_hold_ref() { return hazard_handle_.is_hold_ref(); }
    void retire(int errcode)
    {
      Node *p = NULL;
      retire_list_handle_.retire(errcode, 0);
      while (NULL != (p = static_cast<Node *>(retire_list_handle_.reclaim()))) {
        free_node(p);
      }
    }
    Node *alloc_node()
    {
      Node *node = NULL;
      if (NULL != (node = static_cast<Node *>(alloc_.alloc(sizeof(Node))))) {
        new(node)Node();
      }
      return node;
    }
    void free_node(Node *p)
    {
      if (OB_LIKELY(NULL != p)) {
        if (p->is_deleted()) {
          kvr_callback_.reclaim_key_value(p->key_, p->val_);
        }
        p->~Node();
        alloc_.free(p);
        p = NULL;
      }
    }
    void add_to_alloc_list(Node *node)
    {
      retire_list_handle_.add_alloc(node);
    }
    void add_to_del_list(Node *node)
    {
      retire_list_handle_.add_del(node);
    }
  private:
    int acquire_ref()
    {
      return (NULL == hazard_handle_.acquire_ref()) ? -EOVERFLOW : 0;
    }
    void release_ref()
    {
      hazard_handle_.release_ref();
    }
  private:
    IHashAlloc &alloc_;
    IKVRCallback &kvr_callback_;
    HazardHandle hazard_handle_;
    RetireListHandle retire_list_handle_;
  };

  class Handle: public BaseHandle
  {
  public:
    Handle(HashBase &host, HashRoot &root) : BaseHandle(host),
                                             array_(host.get_array_base()),
                                             root_(root) {}
    ~Handle() {}

    Node *get_head()
    {
      return get_from_array(0);
    }

    void print_list(FILE *fp)
    {
      if (OB_UNLIKELY(NULL == fp)) {
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "print list error, fp is null", K(lbt()));
      } else {
        Node *head = get_head();
        fprintf(fp, "Hash: ");
        while (NULL != head) {
          fprintf(fp, "%lx[%lx]->", head->hash_, head->key_);
          head = head->next_;
        }
        fprintf(fp, "\n");
      }
    }
    int get(key_t key, val_t &val)
    {
      int ret = 0;
      Node target;
      Node *ret_node = NULL;
      Node *pre = NULL;
      if (0 != (ret = get_pre(key, pre))) {
      } else if (0 != (ret = get_from_list(pre, target.set(key), ret_node))) {
      } else {
        if (OB_ISNULL(ret_node)) {
          COMMON_LOG(ERROR, "get null node", KP(ret_node), K(lbt()));
          ret = -EINVAL;
        } else {
          val = ret_node->val_;
        }
      }
      return ret;
    }
    int insert(key_t key, val_t val)
    {
      int err = 0;
      Node *node = NULL;
      Node *pre = NULL;
      if (0 != (err = get_pre(key, pre))) {
      } else if (NULL == (node = this->alloc_node())) {
        err = -ENOMEM;
      } else {
        this->add_to_alloc_list(node);
        err = insert_to_list(pre, node->set(key, val));
      }
      if (0 == err) {
        root_.update_size(1);
      }
      return err;
    }

    int del(key_t key, val_t &val)
    {
      int ret = 0;
      Node target;
      Node *ret_node = NULL;
      Node *pre = NULL;
      if (0 != (ret = get_pre(key, pre, false))) {
      } else if (NULL == pre) {
        ret = -ENOENT;
      } else if (0 == (ret = del_from_list(pre, target.set(key), ret_node))) {
        if (OB_ISNULL(ret_node)) {
          COMMON_LOG(ERROR, "get null node", KP(ret_node), K(lbt()));
          ret = -EINVAL;
        } else {
          ret_node->set_deleted();
          this->add_to_del_list(ret_node);
          val = ret_node->val_;
        }
      }
      if (0 == ret) {
        root_.update_size(-1);
        purge();
      }
      return ret;
    }

  private:
    int get_pre(key_t key, Node *&pre, bool need_add_bucket = true)
    {
      int err  = 0;
      uint64_t hash = hash_map_calc_hash(key);
      uint64_t spk = bitrev(hash);
      uint64_t slot2fill = 0;
      if (0 == (err = get_pre_(spk, pre, slot2fill))) {
      } else if (!need_add_bucket) {
        err = 0;
      } else {
        add_bucket(pre, slot2fill);
      }
      return err;
    }

    void purge()
    {
      int64_t batch = 64;
      int64_t purge_idx = 0;
      while ((purge_idx = root_.try_purge(batch)) > 0) {
        for (int64_t i = 0; i < batch; i++) {
          purge_bucket(purge_idx - i - 1);
        }
      }
    }

    static uint64_t get_idx(uint64_t spk, uint64_t bcnt)
    {
      return bcnt == 0 ? 0 : (spk & (bcnt - 1));
    }

    int get_pre_(uint64_t spk, Node *&pre, uint64_t &slot2fill)
    {
      int err = -EAGAIN;
      uint64_t limit = root_.get_slot_limit() ? : 1;
      uint64_t bcnt = next2n(limit);
      uint64_t target_idx = 0;
      uint64_t find_idx = 0;
      uint64_t find_bcnt = 0;
      if ((target_idx = get_idx(spk, bcnt)) >= limit) {
        bcnt >>= 1;
        target_idx = get_idx(spk, bcnt);
      }
      search_pre_(spk, bcnt, pre, find_idx, find_bcnt);
      if (find_bcnt <= 0) {
        slot2fill = 0;
      } else if (find_idx < target_idx) {
        slot2fill = get_idx(spk, find_bcnt << 1);
      } else {
        err = 0;
      }
      return err;
    }

    void search_pre_(uint64_t spk, uint64_t start_bcnt, Node *&item, uint64_t &find_idx, uint64_t &bcnt)
    {
      int tmp_ret = -EAGAIN;
      bcnt = start_bcnt;
      while (-EAGAIN == tmp_ret && bcnt > 0) {
        uint64_t idx = get_idx(spk, bcnt);
        ArrayHandle handle(array_); // WHITESCAN: OB_CUSTOM_LOOP_DECLARE
        if (-EAGAIN == (tmp_ret = array_.get(handle, &root_.root_, idx, (ArrayBase::val_t &)item))) {
        } else if (0 != tmp_ret) {
          bcnt >>= 1;
          tmp_ret = -EAGAIN;
        } else {
          find_idx = idx;
        }
      }
    }

    Node *get_from_array(int64_t idx)
    {
      Node *node = NULL;
      ArrayHandle handle(array_);
      array_.get(handle, &root_.root_, idx, (void *&)node);
      return node;
    }

    int insert_to_array(uint64_t idx, Node *node)
    {
      ArrayHandle handle(array_);
      return array_.insert(handle, &root_.root_, idx, (void *)node);
    }

    int del_from_array(uint64_t idx, Node *&node)
    {
      ArrayHandle handle(array_);
      return array_.del(handle, &root_.root_, idx, (void *&)node);
    }

    int del_bucket_from_list(Node *node)
    {
      int ret = 0;
      if (OB_ISNULL(node)) {
        ret = -EINVAL;
        COMMON_LOG(ERROR, "del bucket error, node is null", K(ret), K(lbt()));
      } else {
        Node *pre = NULL;
        uint64_t spk = node->get_spk();
        uint64_t slot2fill = 0;
        Node *ret_node = NULL;
        (void)get_pre_(spk, pre, slot2fill);
        if (NULL != pre) {
          ret = del_from_list(pre, node, ret_node);
        }
      }
      return ret;
    }

    int add_bucket(Node *pre, uint64_t idx)
    {
      int err = 0;
      Node *node = NULL;
      if (NULL == (node = this->alloc_node())) {
        err = -ENOMEM;
      } else if (NULL == node->set_dummy(idx)) {
        err = -ENOMEM;
      } else if (NULL != pre && 0 != (err = insert_to_list(pre, node))) {
        this->free_node(node);
      } else if (0 != (err = insert_to_array(idx, node))) {
        if (NULL != pre) {
          while (0 != del_bucket_from_list(node))
            ;
        }
        this->add_to_del_list(node);
      }
      if (0 == err) {
        root_.set_filled(idx + 1);
      }
      return err;
    }

    int del_bucket(uint64_t idx)
    {
      int err = 0;
      Node target;
      Node *node = NULL;
      if (0 != (err = del_from_array(idx, node))) {
      } else {
        uint64_t spk = bitrev(bitrev(idx) - 1);
        uint64_t slot2fill = 0;
        Node *pre = NULL;
        while (true) {
          get_pre_(spk, pre, slot2fill);
          if (NULL == pre) {
            break;
          } else if (0 == del_from_list(pre, target.set_dummy(idx), node)) {
            break;
          }
        }
      }
      if (0 == err) {
        this->add_to_del_list(node);
      }
      return err;
    }

    void purge_bucket(uint64_t idx)
    {
      int err = -EAGAIN;
      while (0 != err) {
        if (-ENOENT == (err = del_bucket(idx))) {
          err = 0;
        }
      }
    }

    static int search_in_list(Node *start, Node *target, Node *&pre, Node *&next)
    {
      int err = 0;
      bool is_deleted = false;
      if (OB_ISNULL(start) || OB_ISNULL(target)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "search in list error, start is null", K(err), K(lbt()));
      } else {
        pre = start;
        next = NULL;
        while (NULL != (next = pre->get_next(is_deleted))
               && target->compare(next) > 0) {
          pre = next;
        }
        if (is_deleted) {
          err = -EAGAIN;
        }
      }
      return err;
    }
    static int get_from_list(Node *start, Node *target, Node *&ret_node)
    {
      int err = 0;
      Node *pre = NULL;
      Node *next = NULL;
      if (OB_ISNULL(target)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "get from list error, target is null", K(err), K(lbt()));
      } else if (0 != (err = search_in_list(start, target, pre, next))) {
      } else if (NULL == next || 0 != target->compare(next)) {
        err = -ENOENT;
      } else {
        ret_node = next;
      }
      return err;
    }
    static int insert_to_list(Node *start, Node *target)
    {
      int err = 0;
      Node *pre = NULL;
      Node *next = NULL;
      if (OB_ISNULL(target)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "get from list error, target is null", K(err), K(lbt()));
      } else if (0 != (err = search_in_list(start, target, pre, next))) {
      } else if (NULL != next && 0 == target->compare(next)) {
        err = -EEXIST;
      } else if (OB_ISNULL(pre)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "pre is null", K(err), K(lbt()));
      } else if (!pre->cas_next(target->next_ = next, target)) {
        err = -EAGAIN;
      }
      return err;
    }
    static int del_from_list(Node *start, Node *target, Node *&ret_node)
    {
      int err = 0;
      Node *pre = NULL;
      Node *next = NULL;
      if (OB_ISNULL(target)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "get from list error, target is null", K(err), K(lbt()));
      } else if (0 != (err = search_in_list(start, target, pre, next))) {
      } else if (NULL == next || 0 != target->compare(next)) {
        err = -ENOENT;
      } else if (!next->set_delete_mark()) {
        err = -EAGAIN;
      } else if (OB_ISNULL(pre)) {
        err = -EINVAL;
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "pre is null", K(err), K(lbt()));
      } else if (!pre->cas_next(next, next->get_next())) {
        err = -EAGAIN;
        next->clear_delete_mark();
      } else {
        ret_node = next;
      }
      return err;
    }
  private:
    ArrayBase &array_;
    HashRoot &root_;
  };

public:
  HashBase(IHashAlloc &hash_alloc, IArrayAlloc &array_alloc, IKVRCallback &kvr_callback,
           int64_t array_node_size):
      alloc_(hash_alloc), kvr_callback_(kvr_callback), array_base_(array_alloc, array_node_size) {}
  ~HashBase() { clean_retire_list(); }
  IHashAlloc &get_alloc() { return alloc_; }
  IKVRCallback &get_kvr_callback() { return kvr_callback_; }
  HazardRef &get_hazard_ref() { return hazard_ref_; }
  RetireList &get_retire_list() { return retire_list_; }
  ArrayBase &get_array_base() { return array_base_; }
  void print(FILE *fp, HashRoot &root)
  {
    //fprintf(fp, "%s\n", repr(root));
    if (OB_ISNULL(root.root_)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "hash root is null", K(lbt()));
    } else {
      root.root_->print(fp);
    }
  }
  int get_iter(Handle &handle, Iterator &iter)
  {
    int err = 0;
    if (!handle.is_hold_ref()) {
      err = -EOVERFLOW;
    } else {
      iter.set_head(handle.get_head());
    }
    return err;
  }
  int get(Handle &handle, key_t key, val_t &val)
  {
    int err = 0;
    if (!handle.is_hold_ref()) {
      err = -EOVERFLOW;
    } else {
      err = handle.get(key, val);
    }
    handle.retire(err);
    return err;
  }
  int insert(Handle &handle, key_t key, val_t val)
  {
    int err = 0;
    if (!handle.is_hold_ref()) {
      err = -EOVERFLOW;
    } else {
      err = handle.insert(key, val);
    }
    handle.retire(err);
    return err;
  }
  int del(Handle &handle, key_t key, val_t &val)
  {
    int err = 0;
    if (!handle.is_hold_ref()) {
      err = -EOVERFLOW;
    } else {
      err = handle.del(key, val);
    }
    handle.retire(err);
    return err;
  }
private:
  void clean_retire_list()
  {
    LIB_LOG(INFO, "clean retire list");
    RetireList::RetireNodeIterator iter(&retire_list_);
    Node *node = NULL;
    while (NULL != (node = static_cast<Node *>(iter.get_next()))) {
      if (node->is_deleted()) {
        kvr_callback_.reclaim_key_value(node->key_, node->val_);
      }
      node->~Node();
      alloc_.free(node);
      node = NULL;
    }
  }
private:
  IHashAlloc &alloc_;
  IKVRCallback &kvr_callback_;
  HazardRef hazard_ref_;
  RetireList retire_list_;
  ArrayBase array_base_;
}; // end class HashBase

}; // end namespace hash
}; // end namespace oceanbase


#endif // OCEANBASE_HASH_OB_HASH_
