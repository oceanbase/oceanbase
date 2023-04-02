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

#ifndef OCEANBASE_HASH_OB_DARRAY_H_
#define OCEANBASE_HASH_OB_DARRAY_H_

#include "lib/allocator/ob_hazard_ref.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
class IAlloc
{
public:
  IAlloc() {}
  virtual ~IAlloc() {}
  virtual void *alloc(int64_t size) = 0;
  virtual void free(void *p) = 0;
};

typedef IAlloc IArrayAlloc;
// it's dangerous to change CPU_NUM, maybe oom
enum { MAX_THREAD_NUM = OB_MAX_THREAD_NUM, MAX_CPU_NUM = 64};

struct EstimateCounter
{
  struct Item
  {
    int64_t value_;
  } CACHE_ALIGNED;
  EstimateCounter(): precision_(64), value_(0)
  {
    memset(items_, 0, sizeof(items_));
  }
  ~EstimateCounter() {}
  void set_precision(int64_t precision)
  {
    precision_ = precision;
  }
  bool inc(int64_t x, int64_t &cur_val)
  {
    bool bool_ret = false;
    int64_t *local_pvalue = &items_[icpu_id() % MAX_CPU_NUM].value_;
    int64_t local_value = ATOMIC_AAF(local_pvalue, x);
    if ((bool_ret = (local_value > precision_ || local_value < -precision_))) {
      cur_val = ATOMIC_AAF(&value_, local_value);
      (void)ATOMIC_AAF(local_pvalue, -local_value);
    }
    return bool_ret;
  }
  int64_t value() const
  {
    int64_t sum = 0;
    for (int64_t i = 0; i < MAX_CPU_NUM; i++) {
      sum += ATOMIC_LOAD(&items_[i].value_);
    }
    return sum + ATOMIC_LOAD(&value_);
  }
  int64_t precision_;
  int64_t value_ CACHE_ALIGNED;
  Item items_[MAX_CPU_NUM];
};

// thread cached ref_cnt
class TcRef
{
public:
  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  struct Item
  {
    Item(): lock_(common::ObLatchIds::HASH_MAP_LOCK), addr_(NULL), val_(0) {}
    ~Item() {}
    void flush()
    {
      if (NULL != addr_) {
        (void)ATOMIC_AAF(addr_, val_);
        addr_ = NULL;
        val_ = 0;
      }
    }
    Lock lock_;
    int32_t *addr_;
    int32_t val_;
  } CACHE_ALIGNED;
  TcRef() {}
  ~TcRef() {}
  int32_t get_ref(int32_t *addr)
  {
    int64_t thread_count = min(get_max_itid(), MAX_THREAD_NUM);
    for (int64_t i = 0; i < thread_count; i++) {
      Item *item = ref_ + i;
      LockGuard guard(item->lock_); // WHITESCAN: OB_CUSTOM_LOOP_DECLARE
      if (item->addr_ == addr) {
        item->flush();
      }
    }
    return ATOMIC_LOAD(addr);
  }
  int ref(int32_t *addr, int32_t x)
  {
    int ret = 0;
    int64_t tid = get_itid();
    if (OB_UNLIKELY(tid >= MAX_THREAD_NUM)) {
      COMMON_LOG(ERROR, "TcRef do ref error");
      ret = -ERANGE;
    } else {
      Item *item = ref_ + tid;
      LockGuard guard(item->lock_);
      if (item->addr_ != addr) {
        item->flush();
        item->addr_ = addr;
      }
      item->val_ += x;
    }
    return ret;
  }
private:
  static int64_t min(int64_t x, int64_t y)
  {
    return x < y ? x : y;
  }
private:
  Item ref_[MAX_THREAD_NUM];
};

class ReadRef
{
public:
  ReadRef() {}
  ~ReadRef() {}

  // current thread declares locking at some @wlock
  void add_ref(void *wlock)
  {
    set_ref(wlock);
  }

  // current thread declares unlocking the wlock it holding
  void del_ref()
  {
    set_ref(NULL);
  }

  // test if some thread is wlocking at @wlock
  bool is_refed(void *wlock) const
  {
    bool ret = false;
    int64_t thread_num = min(get_max_itid(), MAX_THREAD_NUM);
    for (int64_t i = 0; !ret && i < thread_num; i++) {
      if (get_ref(i) == wlock) {
        ret = true;
      }
    }
    return ret;
  }

  // wait at @wlock until no thread refer to.
  // do not guarantee: no thread is refering to @wlock at a certain time snapshot
  // wait threads that refer to @wlock before start wait
  void wait_ref_free(void *wlock) const
  {
    int64_t thread_num = min(get_max_itid(), MAX_THREAD_NUM);
    for (int64_t i = 0; i < thread_num; i++) {
      while (get_ref(i) == wlock) {
        PAUSE();
      }
    }
  }
private:
  static int64_t min(int64_t x, int64_t y)
  {
    return x < y ? x : y;
  }
  void *get_ref(int64_t i) const
  {
    return ATOMIC_LOAD(&read_ref_[i].value_);
  }
  void set_ref(void *x)
  {
    int64_t tid = get_itid();
    if (tid >= MAX_THREAD_NUM) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "set_ref error", K(tid));
    } else {
      ATOMIC_STORE(&read_ref_[tid].value_, x);
    }
  }
private:
  struct RefMarker
  {
    RefMarker(): value_(NULL) {}
    ~RefMarker() {}
    void *value_ CACHE_ALIGNED;
  };
  RefMarker read_ref_[MAX_THREAD_NUM];
};

struct NestLessRWLockHandler
{
private:
  enum
  {
    UNLOCKED = 0,
    WR_LOCKING = 1,
    WR_LOCKED = 2,
  };
public:
  static bool try_rdlock(ReadRef &ref, uint32_t *write_uid)
  {
    bool lock_succ = false;
    if (UNLOCKED == ATOMIC_LOAD(write_uid)) {
      ref.add_ref(write_uid);
      if (UNLOCKED == ATOMIC_LOAD(write_uid)) {
        lock_succ = true;
      } else {
        // some writer locking
        ref.del_ref();
      }
    }
    return lock_succ;
  }

  static void rdlock(ReadRef &ref, uint32_t *write_uid)
  {
    while (!try_rdlock(ref, write_uid)) {
      PAUSE();
    }
  }

  static void rdunlock(ReadRef &ref)
  {
    ref.del_ref();
  }

  // retry unless another thread acquire wrlock
  static bool try_wrlock_hard(ReadRef &ref, uint32_t *write_uid)
  {
    // mark-wait-done
    bool lock_succ = false;
    if (!ATOMIC_BCAS(write_uid, UNLOCKED, WR_LOCKING)) {
      // already locking or locked
    } else {
      ref.wait_ref_free(write_uid);
      ATOMIC_STORE(write_uid, WR_LOCKED);
      lock_succ = true;
    }
    return lock_succ;
  }

  static bool try_wrlock(ReadRef &ref, uint32_t *write_uid)
  {
    bool lock_succ = false;
    if (!ATOMIC_BCAS(write_uid, UNLOCKED, WR_LOCKING)) {
    } else if (ref.is_refed(write_uid)) {
      // backoff if some reader exist
      ATOMIC_STORE(write_uid, UNLOCKED);
    } else {
      ATOMIC_STORE(write_uid, WR_LOCKED);
      lock_succ = true;
    }
    return lock_succ;
  }

  // retry until current thread acquire wrlock
  static void wrlock(ReadRef &ref, uint32_t *write_uid)
  {
    while (!ATOMIC_BCAS(write_uid, UNLOCKED, WR_LOCKING)) {
      PAUSE();
    }
    ref.wait_ref_free(write_uid);
    ATOMIC_STORE(write_uid, WR_LOCKED);
  }

  static void wrunlock(ReadRef &ref, uint32_t *write_uid)
  {
    UNUSED(ref);
    ATOMIC_STORE(write_uid, UNLOCKED);
  }
};

class ArrayHeadHandler
{
public:
  typedef NestLessRWLockHandler LockHandler;
  bool try_rdlock(uint32_t *lock)
  {
    return LockHandler::try_rdlock(read_ref_, lock);
  }
  bool try_wrlock(uint32_t *lock)
  {
    bool bool_ret = false;
    if (OB_ISNULL(lock)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "ArrayHeadHandler try_wrlock error, null lock addr");
    } else {
      bool_ret = LockHandler::try_wrlock_hard(read_ref_, lock);
    }
    return bool_ret;
  }
  void rdunlock(uint32_t *lock)
  {
    UNUSED(lock);
    LockHandler::rdunlock(read_ref_);
  }
  void wrunlock(uint32_t *lock)
  {
    if (OB_ISNULL(lock)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "ArrayHeadHandler wrunlock error, null lock addr");
    } else {
      LockHandler::wrunlock(read_ref_, lock);
    }
  }
  int ref(int32_t *addr, uint32_t x)
  {
    return tc_ref_.ref(addr, x);
  }
  int32_t get_ref(int32_t *addr)
  {
    return tc_ref_.get_ref(addr);
  }
private:
  ReadRef read_ref_;
  TcRef tc_ref_;
};

class ArrayHead: public HazardNode
{
public:
  typedef void *val_t;
  ArrayHead(uint64_t limit, int level, ArrayHeadHandler &head_handler)
      : HazardNode(),
        capacity_(static_cast<uint32_t>((limit - sizeof(*this)) / sizeof(val_t))),
        level_(level),
        lock_(0),
        ref_(0),
        head_handler_(head_handler)
  {
    memset(ptr_, 0, capacity_ * 8);
  }
  ~ArrayHead() { (void)head_handler_.get_ref(&ref_); }
  int get_level() { return level_; }

  // for debug
  void print(FILE *fp, int64_t slot_idx = 0, int indent = 0)
  {
    if (OB_ISNULL(this) || OB_ISNULL(fp)) {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "print error, invalid argument or null this", KP(this), KP(fp));
    } else {
      fprintf(fp, "%*s%ldL%d: ", indent * 4, "C", slot_idx, level_);
      if (level_ != 1) {
        fprintf(fp, "\n");
      }
      for (int64_t i = 0; i < capacity_; i++) {
        ArrayHead *child = NULL;
        if (1 == level_) {
          fprintf(fp, "%p ", get(i));
        } else if (NULL != (child = static_cast<ArrayHead *>(get(i)))) {
          child->print(fp, i, indent + 1);
        }
      }
      if (1 == level_) {
        fprintf(fp, "\n");
      }
    }
  }
  val_t get(uint64_t idx)
  {
    return ATOMIC_LOAD(ptr_ + idx);
  }
  int insert(ArrayHeadHandler &handler, uint64_t idx, val_t val)
  {
    int err = 0;
    bool lock_succ = false;
    if (!(lock_succ = handler.try_rdlock(&lock_))) {
      err = -EAGAIN;
    } else if (!ATOMIC_BCAS(ptr_ + idx, NULL, val)) {
      err = -EEXIST;
    } else {
      err = handler.ref(&ref_, 1);
    }
    if (lock_succ) {
      handler.rdunlock(&lock_);
    }
    return err;
  }
  int del(ArrayHeadHandler &handler, uint64_t idx, val_t &val, bool &is_clear)
  {
    int err = 0;
    bool lock_succ = false;
    if (NULL == get(idx)) {
      err = -ENOENT;
    } else if (!(lock_succ = handler.try_rdlock(&lock_))) {
      err = -EAGAIN;
    } else if (NULL == (val = ATOMIC_TAS(ptr_ + idx, NULL))) {
      err = -ENOENT;
    } else {
      err = handler.ref(&ref_, -1);
      is_clear = (NULL == get(0)) && 0 == handler.get_ref(&ref_);
    }
    if (lock_succ) {
      handler.rdunlock(&lock_);
    }
    return err;
  }
  int reclaim(ArrayHeadHandler &handler)
  {
    int err = 0;
    bool need_unlock = false;
    if (!(need_unlock = handler.try_wrlock(&lock_))) {
      err = -EAGAIN;
    } else if (0 != handler.get_ref(&ref_)) {
      err = -EAGAIN;
    } else {
      need_unlock = false;
    }
    if (need_unlock) {
      handler.wrunlock(&lock_);
    }
    return err;
  }
  int reclaim_root(ArrayHeadHandler &handler, ArrayHead **root)
  {
    int err = -EAGAIN;
    int32_t ref = 0;
    bool need_unlock = false;
    if (NULL != get(1)) {
      err = -EINVAL;
    } else if (!(need_unlock = handler.try_wrlock(&lock_))) {
      err = -EAGAIN;
    } else if (NULL != get(1) || (ref = handler.get_ref(&ref_)) > 1) {
      err = -EINVAL;
    } else if (1 == ref && (NULL == get(0) || level_ <= 1)) {
      err = -EINVAL;
    } else if (!ATOMIC_BCAS(root, this, static_cast<ArrayHead*>(get(0)))) {
      err = -EAGAIN;
    } else {
      need_unlock = false;
    }
    if (need_unlock) {
      handler.wrunlock(&lock_);
    }
    return err;
  }
private:
  uint32_t capacity_;
  int32_t level_;
  uint32_t lock_;
  int32_t ref_;
  ArrayHeadHandler &head_handler_;
  val_t ptr_[0];
};

class ArrayBase
{
public:
  typedef void *val_t;
  typedef ArrayHead Node;
  class Path
  {
  public:
    enum { MAX_LEVEL = 32 };

    // record position of this level
    struct Item
    {
      Item() : node_(NULL), node_idx_(0), slot_idx_(0) {}

      Node *node_;
      uint64_t node_idx_; // count from the very left one
      uint64_t slot_idx_;
    };
    Path(): root_node_(NULL), root_level_(0), pop_idx_(0), path_size_(0) {}
    ~Path() {}
    bool is_empty() { return pop_idx_ >= path_size_; }
    Node *get_root_node() { return root_node_; }
    uint64_t get_root_level() { return root_level_; }
    // build path of idx
    int search(Node *root, uint64_t idx, uint64_t node_size)
    {
      int err = 0;
      root_level_ = (NULL == root) ? 0 : root->get_level();
      do {
        path_[path_size_].slot_idx_ = (idx % node_size);
        path_[path_size_].node_idx_ = (idx /= node_size);
        path_size_++;
        if (OB_UNLIKELY(path_size_ >= MAX_LEVEL)) {
          err = -EOVERFLOW;
          COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "path size over flow, idx is too large",
                     K(err), K(idx), K(path_size_));
        }
      } while ((idx > 0 || path_size_ < root_level_) && OB_LIKELY(0 == err));
      if (0 == err) {
        Node *node = NULL;
        for (int i = path_size_ - 1; i >= 0; i--) {
          Item *item = path_ + i;
          int level = i + 1;
          if (level > root_level_) {
            item->node_ = NULL;
          } else if (level == root_level_) {
            if (0 == item->node_idx_) {
              item->node_ = root;
              node = (Node *)root->get(item->slot_idx_);
            } else {
              item->node_ = NULL;
            }
          } else if (NULL == node) {
            item->node_ = NULL;
          } else {
            item->node_ = node;
            node = (Node *)node->get(item->slot_idx_);
          }
        }
        root_node_ = root;
      }
      return err;
    }
    int pop(Node *&node, uint64_t &node_idx, uint64_t &slot_idx, uint64_t &level)
    {
      int err = 0;
      if (pop_idx_ >= path_size_) {
        err = -ENOENT;
      } else {
        Item *item = path_ + (pop_idx_);
        node = item->node_;
        node_idx = item->node_idx_;
        slot_idx = item->slot_idx_;
        level = ++pop_idx_;
      }
      return err;
    }
  private:
    Node *root_node_;
    int root_level_;
    int pop_idx_;
    int path_size_;
    Item path_[MAX_LEVEL];
  };

  class BaseHandle
  {
  public:
    explicit BaseHandle(ArrayBase &host):
        node_size_(host.get_node_size()),
        alloc_(host.get_alloc()),
        array_head_handler_(host.get_array_handler()),
        hazard_handle_(host.get_hazard_ref()),
        retire_list_handle_(host.get_hazard_ref(), host.get_retire_list())
    {}
    virtual ~BaseHandle() { release_ref(); }
    int64_t get_node_size() { return node_size_; }
    int acquire_ref()
    {
      return (NULL == hazard_handle_.acquire_ref()) ? -EOVERFLOW : 0;
    }
    void release_ref()
    {
      hazard_handle_.release_ref();
    }
    int retire(int errcode)
    {
      int err = 0;
      Node *p = NULL;
      retire_list_handle_.retire(errcode, 0);
      while (NULL != (p = (Node *)retire_list_handle_.reclaim())) {
        free_node(p);
      }
      return err;
    }
    Node *alloc_node(uint64_t level)
    {
      Node *node = NULL;
      int64_t alloc_size = node_size_ * 8 + sizeof(Node);
      if (NULL != (node = (Node *)alloc_.alloc(alloc_size))) {
        retire_list_handle_.add_alloc(new(node)Node(alloc_size, (int)level, array_head_handler_));
      }
      return node;
    }
    void add_to_del_list(Node *node)
    {
      retire_list_handle_.add_del(node);
    }
  private:
    void free_node(Node *p)
    {
      if (NULL != p) {
        p->~Node();
        alloc_.free(p);
        p = NULL;
      }
    }
  private:
    int64_t node_size_;
    IArrayAlloc &alloc_;
    ArrayHeadHandler &array_head_handler_;
    HazardHandle hazard_handle_;
    RetireListHandle retire_list_handle_;
  };

  class Handle: public BaseHandle
  {
  public:
    explicit Handle(ArrayBase &host): BaseHandle(host), handler_(host.get_array_handler()) {}
    ~Handle() {}
    int search(Node *root, uint64_t idx)
    {
      return path_.search(root, idx, get_node_size());
    }
    int get(val_t &val)
    {
      int err = 0;
      Node *leaf = NULL;
      uint64_t node_idx = 0;
      uint64_t slot_idx = 0;
      uint64_t level = 0;
      if (0 != path_.pop(leaf, node_idx, slot_idx, level)
          || NULL == leaf
          || NULL == (val = leaf->get(slot_idx))) {
        err = -ENOENT;
      }
      return err;
    }
    int insert(val_t val, Node *&root)
    {
      int err = 0;
      Node *old_node = NULL;
      Node *new_node = NULL;
      uint64_t node_idx = 0;
      uint64_t slot_idx = 0;
      uint64_t level = 0;
      root = path_.get_root_node();
      while (0 == err && 0 == path_.pop(old_node, node_idx, slot_idx, level)) {
        // insert a new node, alloc on-demand
        if (NULL == (new_node = (old_node ? : alloc_node(level)))) {
          err = -ENOMEM;
        } else if (0 != (err = new_node->insert(handler_, slot_idx, val))) {
          err = -EAGAIN;
        } else if (old_node == new_node) {
          break;
        } else {
          Node *new_root = NULL;
          if (level <= path_.get_root_level()) {
            // do not extend tree to a higher level.
            // Otherwise:
            //   we are inserting a node outside right of the tree,
            //   so extend the tree and link the origin tree as a subtree
          } else if (0 == node_idx) {
            // the final root
            if (NULL != root) {
              new_node->insert(handler_, 0, root); // can not fail
            }
            root = new_node;
          } else if (OB_UNLIKELY(NULL == root)) {
            // avoid alloc unnecessary node, just continue
          } else if (NULL == (new_root = alloc_node(level))) {
            err = -ENOMEM;
          } else {
            // compensate the very left path to link origin tree as subtree
            (void)new_root->insert(handler_, 0, root); // can not fail
            root = new_root;
          }
        }
        if (0 == err) {
          // recursively complete inserting node from bottom of the path
          // now, we are trying to insert a child node instead of user data
          val = new_node;
        }
      } // end while
      return err;
    }

    // either success or failed at level 1
    int del(val_t &ret_val, bool &need_reclaim_root)
    {
      int err = 0;
      Node *node = NULL;
      uint64_t node_idx = 0;
      uint64_t slot_idx = 0;
      uint64_t level = 0;
      ret_val = NULL;
      // delete node from botton of the path if can
      while (0 == err && 0 == path_.pop(node, node_idx, slot_idx, level)) {
        if (NULL == node) {
          err = -ENOENT;
        } else {
          val_t val = NULL;
          bool is_clear = false;
          while (-EAGAIN == (err = node->del(handler_, slot_idx, val, is_clear)))
            ;
          if (0 != err) {
          } else if (NULL == (ret_val = ret_val ? : val)) {
            err = -EFAULT;
          } else if (!is_clear || path_.is_empty() || 0 != node->reclaim(handler_)) {
            break;
          } else {
            // node can be reclaimed, no reader or writer accessing it
            add_to_del_list(node);
          }
        }
      }
      if (NULL != ret_val) {
        err = 0;
      }
      need_reclaim_root = path_.is_empty();
      return err;
    }
    void reclaim_root(Node **root)
    {
      int err = 0;
      Node *old_root = NULL;
      while (NULL != (old_root = ATOMIC_LOAD(root))) {
        if (0  == (err = old_root->reclaim_root(handler_, root))) {
          add_to_del_list(old_root);
        } else if (-EAGAIN != err) {
          break;
        }
      }
    }
  private:
    Path path_;
    ArrayHeadHandler &handler_;
  };

public:
  ArrayBase(IArrayAlloc &alloc, int64_t node_size) : alloc_(alloc),
                                                     node_size_(node_size),
                                                     hazard_ref_(false) {}
  ~ArrayBase() {}
  IArrayAlloc &get_alloc() { return alloc_; }
  int64_t get_node_size() { return node_size_; }
  RetireList &get_retire_list() { return retire_list_; }
  HazardRef &get_hazard_ref() { return hazard_ref_; }
  ArrayHeadHandler &get_array_handler() { return array_handler_; }
  int get(Handle &handle, Node **root, uint64_t idx, val_t &val)
  {
    int err = 0;
    Node *old_root = NULL;
    if (OB_ISNULL(root)) {
      err = -EINVAL;
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "root is null");
    } else if (0 != (err = handle.acquire_ref())) {
    } else if (0 != (err = handle.search(old_root = ATOMIC_LOAD(root), idx))) {
    } else if (0 != (err = handle.get(val))) {
    }
    return err;
  }
  int insert(Handle &handle, Node **root, uint64_t idx, val_t val)
  {
    int err = 0;
    Node *old_root = NULL;
    Node *new_root = NULL;
    if (OB_ISNULL(root) || OB_ISNULL(val)) {
      err = -EINVAL;
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "root is null");
    } else if (0 != (err = handle.acquire_ref())) {
    } else if (0 != (err = handle.search(old_root = ATOMIC_LOAD(root), idx))) {
    } else if (0 != (err = handle.insert(val, new_root))) {
    } else if (old_root == new_root) {
    } else if (!ATOMIC_BCAS(root, old_root, new_root)) {
      err = -EAGAIN;
    }
    handle.release_ref();
    handle.retire(err);
    return err;
  }
  int del(Handle &handle, Node **root, uint64_t idx, val_t &val)
  {
    int err = 0;
    bool need_reclaim_root = false;
    Node *old_root = NULL;
    if (OB_ISNULL(root)) {
      err = -EINVAL;
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "root is null");
    } else if (0 != (err = handle.acquire_ref())) {
    } else if (0 != (err = handle.search(old_root = ATOMIC_LOAD(root), idx))) {
    } else if (0 != (err = handle.del(val, need_reclaim_root))) {
    } else if (need_reclaim_root) {
      handle.reclaim_root(root);
    }
    handle.release_ref();
    handle.retire(err);
    return err;
  }
private:
  IArrayAlloc &alloc_;
  int64_t node_size_;
  HazardRef hazard_ref_;
  RetireList retire_list_;
  ArrayHeadHandler array_handler_;
};

}; // end namespace hash
}; // end namespace oceanbase

#endif /* OCEANBASE_HASH_OB_DARRAY_H_ */
