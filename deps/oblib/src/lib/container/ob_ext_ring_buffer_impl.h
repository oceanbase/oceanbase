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

#ifndef OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_H_
#define OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_IMPL_H_
#endif

#ifndef OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_IMPL_H_
#define OCEANBASE_LIB_CONTAINER_OB_EXT_RING_BUFFER_IMPL_H_

#include <algorithm>

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/allocator/ob_external_ref.h"
#include "lib/allocator/ob_heavy_ref.h"

namespace oceanbase
{
namespace common
{
namespace erb
{
static inline void on_fatal_error()
{
  while(true) {
    LIB_LOG_RET(ERROR, common::OB_ERROR, "on_fatal_error");
    sleep(1);
  }
}

// alloc
class RingBufferAlloc : public ObIAllocator
{
public:
  RingBufferAlloc() : mem_attr_()
  {
    mem_attr_.label_ = ObModIds::OB_RING_BUFFER;
    mem_attr_.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  virtual ~RingBufferAlloc() { }
  virtual void* alloc(const int64_t size) { return alloc(size, mem_attr_); }
  virtual void* alloc(const int64_t size, const ObMemAttr& attr)
  { return ob_malloc(size, attr); }
  virtual void free(void* ptr) { ob_free(ptr); }
  virtual void set_attr(const ObMemAttr& attr) { mem_attr_ = attr; }
private:
  ObMemAttr mem_attr_;
  DISALLOW_COPY_AND_ASSIGN(RingBufferAlloc);
};

// Slot.
// It holds a lock which synchronizes set() and pop().

// Ptr slot.
template <typename T>
struct PtrSlot
{
  typedef ObPtrSpinLock<T> Lock;
  T *val_;
};

// Slot operation.
namespace SlotOp
{
  // PtrSlot.
  template <typename T> void init(PtrSlot<T> &slot)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    lock.init();
  }
  template <typename T> void lock(PtrSlot<T> &slot)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    lock.lock();
  }
  template <typename T> bool try_lock(PtrSlot<T> &slot)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    return lock.try_lock();
  }
  template <typename T> void unlock(PtrSlot<T> &slot)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    lock.unlock();
  }
  template <typename T> T* get(PtrSlot<T> &slot)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    return lock.get_ptr();
  }
  template <typename T> void set(PtrSlot<T> &slot, T* const &val)
  {
    typename PtrSlot<T>::Lock &lock = reinterpret_cast<typename PtrSlot<T>::Lock&>(slot.val_);
    lock.set_ptr(val);
  }
  template <typename T> T* default_val(PtrSlot<T> &slot)
  {
    UNUSED(slot);
    return NULL;
  }
}

template <typename T>
class DummyRef
{
public:
  DummyRef() {}
  virtual ~DummyRef() {}
  T* acquire(T** paddr) { return load_ptr(paddr); }
  void release(T* addr) { UNUSED(addr); }
  void retire(T* addr) { UNUSED(addr); }
private:
  T* load_ptr(T** paddr) { return (T*)(((uint64_t)ATOMIC_LOAD(paddr)) & ~1ULL); }
};

// ring buffer base of T ptr
// allocator is iallcoator interface
// add all kinds of data access interface that can take functor to modify its behaviour

// data type should have its default value. Because, user may pop an entry that has not been initialized.


// Just write function definitions inside the class
template <typename ValT, typename SlotT, typename RefT = DummyRef<ValT>>
class ObExtendibleRingBufferBase
{
  // Segment.
  struct Segment : public ObExternalRef::IERefPtr
  {
    ObIAllocator *allocator_;
    SlotT slots_[0];
    void reset(const int64_t capacity, ObIAllocator *alloc)
    {
      allocator_ = alloc;
      for (int64_t idx = 0; idx < capacity; ++idx) {
        SlotOp::init(slots_[idx]);
      }
    }
    virtual void purge()
    {
      int ret = OB_SUCCESS;
      if (NULL == allocator_) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "err alloc", K(ret), K(allocator_));
      } else {
        allocator_->free(this);
      }
    }
    virtual void on_quiescent()
    {
      purge();
    }
  };
  // Dir.
  struct Dir : public ObExternalRef::IERefPtr
  {
    ObIAllocator *allocator_;
    int64_t seg_cnt_;
    int64_t mod_val_;
    Segment *segs_[0];
    void reset(const int64_t seg_cnt, const int64_t seg_capacity, ObIAllocator *alloc)
    {
      allocator_ = alloc;
      seg_cnt_ = seg_cnt;
      mod_val_ = seg_cnt * seg_capacity;
      for (int64_t idx = 0; idx < seg_cnt; ++idx) {
        segs_[idx] = NULL;
      }
    }
    virtual void purge() {
      int ret = OB_SUCCESS;
      for (int64_t idx = 0; idx < seg_cnt_; ++idx) {
        segs_[idx] = NULL;
      }
      seg_cnt_ = 0;
      mod_val_ = 0;
      if (NULL == allocator_) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "err alloc", K(ret), K(allocator_));
      } else {
        allocator_->free(this);
      }
    }
    void on_quiescent() {
      purge();
    }
  };
  struct TrueCond
  {
    bool operator()(const ValT &val) { UNUSED(val); return true; }
    bool operator()(const ValT &oldval, const ValT &newval) { UNUSED(oldval); UNUSED(newval); return true; }
  };
  // Defines.
  typedef ObExtendibleRingBufferBase<ValT, SlotT, RefT> MyType;
  static const int64_t MIN_SEG_CNT = 2;
  static const int64_t INIT_SEG_CNT = MIN_SEG_CNT;
  typedef std::pair<int64_t, int64_t> SlotIdx; // <Segment index, Slot index on Segment>
public:
  static const int64_t DEFAULT_SEG_SIZE = (1LL << 9);    // 512 bytes
  static const int64_t DEFAULT_SEG_CAPACITY = static_cast<int64_t>((DEFAULT_SEG_SIZE - sizeof(Segment)) / sizeof(SlotT));

  // Interface for subclass.
  ObExtendibleRingBufferBase() :
    inited_(false),
    seg_size_(0),
    seg_capacity_(0),
    begin_sn_(0),
    end_sn_(0),
    dir_(0),
    es_lock_(common::ObLatchIds::ALLOC_ESS_LOCK),
    allocator_(NULL)
  { }
  virtual ~ObExtendibleRingBufferBase() { }

  bool is_inited() const { return inited_; }

  int init(const int64_t begin_sn, ObIAllocator *allocator,
      const int64_t seg_size = DEFAULT_SEG_SIZE)
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_INIT_TWICE;
      LIB_LOG(WARN, "init twice", K(ret));
    } else {
      // Init param.
      seg_size_ = seg_size;
      seg_capacity_ = static_cast<int64_t>((seg_size_ - sizeof(Segment)) / sizeof(SlotT));
      update_begin_sn_(begin_sn);
      update_end_sn_(begin_sn);
      allocator_ = allocator;
      // Init Dir & Segment.
      if (OB_SUCCESS != (ret = dir_seg_init_())) {
        LIB_LOG(WARN, "failed to init Dir Segment", K(ret));
      } else {
        inited_ = true;
      }
    }
    return ret;
  }
  int destroy()
  {
    int ret = OB_SUCCESS;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LIB_LOG(WARN, "not init", K(ret));
    } else if (OB_SUCCESS != (ret = dir_seg_destroy_())) {
      LIB_LOG(WARN, "failed to destroy Dir Segment", K(ret));
    } else {
      inited_ = false;
    }
    return ret;
  }
  int get(const int64_t sn, ValT &val) const
  {
    return const_cast<MyType*>(this)->do_get_(sn, val);
  }
  int set(const int64_t sn, const ValT &val)
  {
    TrueCond cond;
    bool set = false;
    return do_set_(sn, val, cond, set);
  }
  template <typename Func> int set(const int64_t sn, const ValT &val, Func &cond, bool &set)
  {
    return do_set_(sn, val, cond, set);
  }
  int pop(ValT &val)
  {
    TrueCond cond;
    bool popped = false;
    return do_pop_(cond, val, popped, false/*use_lock*/);
  }
  template <typename Func> int pop(Func &cond, ValT &val, bool &popped, bool use_lock = false)
  {
    return do_pop_(cond, val, popped, use_lock);
  }
  int64_t begin_sn() const
  {
    return const_cast<MyType*>(this)->load_begin_sn_();
  }
  int64_t end_sn() const
  {
    return const_cast<MyType*>(this)->load_end_sn_();
  }

  void set_range(int64_t start_id, int64_t end_id)
  {
    update_begin_sn_(start_id);
    update_end_sn_(end_id);
  }

protected:
  ObExternalRef& get_dir_ref()
  {
    static ObExternalRef dir_ref;
    return dir_ref;
  }
  ObExternalRef& get_seg_ref()
  {
    static ObExternalRef seg_ref;
    return seg_ref;
  }
  RefT& get_val_ref()
  {
    static RefT val_ref;
    return val_ref;
  }
private:
  // Atomic loader & setter.
  int64_t load_begin_sn_() { return ATOMIC_LOAD(&(begin_sn_)); }
  int64_t load_end_sn_() { return ATOMIC_LOAD(&(end_sn_)); }
  Dir* load_dir_() { return ATOMIC_LOAD(&(dir_)); }
  void set_dir_(Dir *dir) { UNUSED(ATOMIC_STORE(&(dir_), dir)); }
  bool is_dir_changed(Dir* dir) { RLockGuard guard(sn_dir_lock_); return dir != load_dir_(); }
  void update_begin_sn_(const int64_t begin_sn)
  {
    UNUSED(ATOMIC_STORE(&begin_sn_, begin_sn));
  }
  void update_end_sn_(const int64_t end_sn)
  {
    bool updated = false;
    int64_t cur_end_sn = 0;
    while (!updated && ((cur_end_sn = load_end_sn_()) < end_sn)) {
      updated = ATOMIC_BCAS(&(end_sn_), cur_end_sn, end_sn);
    }
  }
  // Pop lock
  // Expand, shrink, Segment alloc lock.
  bool estrylock_() { return OB_SUCCESS == es_lock_.trylock(); }
  void eslock_() { int ret = es_lock_.lock(); if (OB_SUCCESS != ret) { LIB_LOG(ERROR, "err lock", K(ret)); } }
  void esunlock_() { int ret = es_lock_.unlock(); if (OB_SUCCESS != ret) { LIB_LOG(ERROR, "err unlock", K(ret)); }}
  // Calc.
  SlotIdx calc_slot_idx_(const int64_t sn, Dir *dir)
  {
    int64_t idx = sn % dir->mod_val_;
    return SlotIdx(idx / seg_capacity_, idx % seg_capacity_);
  }
  int64_t calc_upper_lmt_sn_(const int64_t begin_sn, Dir *dir)
  {
    return (dir->mod_val_ + (begin_sn - (begin_sn % seg_capacity_)));
  }
  int64_t calc_ctrl_sn_(const int64_t begin_sn, Dir *dir)
  {
    return (begin_sn - (calc_slot_idx_(begin_sn, dir).second) + seg_capacity_ - 1);
  }
  // Segment - New & Delete.
  Segment* new_segment_()
  {
    Segment *pret = NULL;
    if (NULL == allocator_) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err alloc");
    }
    else if (NULL == (pret = static_cast<Segment*>(allocator_->alloc(seg_size_)))) {
      LIB_LOG_RET(WARN, common::OB_ALLOCATE_MEMORY_FAILED, "failed to alloc Segment", K(seg_size_));
    } else {
      new(pret)Segment();
      pret->reset(seg_capacity_, allocator_);
    }
    return pret;
  }
  void delete_segment_(Segment *seg)
  {
    if (NULL != seg) {
      seg->purge();
      seg = NULL;
    }
  }
  // Ensure Segment. Hazard pointer for new Segment is acquired.
  int ensure_segment_(Dir *dir, const int64_t seg_idx, Segment *&seg)
  {
    int ret = OB_SUCCESS;
    if (estrylock_()) {
      if (dir != load_dir_()) {
        ret = OB_EAGAIN; // Dir changed.
      } else if (NULL != dir->segs_[seg_idx]) {
        // Segment not NULL.
      } else if (NULL == (dir->segs_[seg_idx] = new_segment_())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "failed to new Segment", K(ret), K(seg_idx));
      }
      if (OB_SUCC(ret)) {
        // Acquire haz, so Segment is safe.
        seg = (Segment*)get_seg_ref().acquire((void**)&dir->segs_[seg_idx]);
      }
      esunlock_();
    } else {
      ret = OB_EAGAIN; // Dir under modification.
    }
    return ret;
  }
  // Dir - New & Delete.
  Dir* new_dir_(const int64_t seg_cnt)
  {
    Dir *pret = NULL;
    int64_t size = static_cast<int64_t>(sizeof(Dir) + (seg_cnt * sizeof(Segment*)));
    if (NULL == allocator_) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err alloc");
    } else if (NULL == (pret = static_cast<Dir*>(allocator_->alloc(size)))) {
      LIB_LOG_RET(WARN, common::OB_ALLOCATE_MEMORY_FAILED, "failed to alloc Dir", K(size));
    } else {
      new(pret)Dir();
      pret->reset(seg_cnt, seg_capacity_, allocator_);
    }
    return pret;
  }
  void delete_dir_(Dir *dir)
  {
    if (NULL != dir) {
      dir->purge();
      dir = NULL;
    }
  }
  // Dir & Segment init & destroy.
  int dir_seg_init_()
  {
    int ret = OB_SUCCESS;
    int64_t init_seg_cnt = INIT_SEG_CNT;
    Dir *dir = NULL;
    if (NULL == (dir = new_dir_(init_seg_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "failed to init Dir", K(ret), K(init_seg_cnt));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < dir->seg_cnt_; ++idx) {
        Segment *seg = NULL;
        if (NULL == (seg = new_segment_())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "failed to init Segment", K(ret), K(idx));
        } else {
          dir->segs_[idx] = seg;
        }
      }
    }
    if (OB_SUCC(ret)) {
      set_dir_(dir);
    } else if (NULL != dir) {
      for (int64_t idx = 0; idx < dir->seg_cnt_; ++idx) {
        Segment *&seg = dir->segs_[idx];
        if (NULL != seg) {
          delete_segment_(seg);
        }
      }
      delete_dir_(dir);
    }
    return ret;
  }
  int dir_seg_destroy_()
  {
    int ret = OB_SUCCESS;
    Dir *dir = load_dir_();
    for (int64_t idx = 0; idx < dir->seg_cnt_; ++idx) {
      Segment *&seg = dir->segs_[idx];
      if (NULL != seg) {
        get_seg_ref().retire(seg);
      }
    }
    get_dir_ref().retire(dir);
    return ret;
  }
  // Dir - Expand.
  // Control upper_lmt_sn by locking the last Slot on begin_sn's Segment.
  int ctrl_upper_lmt_sn_(Dir *dir, int64_t &ctrl_sn)
  {
    int ret = OB_SUCCESS;
    bool ctrl = false;
    if (NULL == dir) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "err dir", K(ret));
    }
    while (OB_SUCCESS == ret && !ctrl) {
      int64_t begin_sn = load_begin_sn_();
      ctrl_sn = calc_ctrl_sn_(begin_sn, dir);
      SlotIdx slot_idx = calc_slot_idx_(ctrl_sn, dir);
      Segment *&seg = dir->segs_[slot_idx.first];
      if (NULL != seg || (NULL != (seg = new_segment_()))) {
        SlotT &slot = seg->slots_[slot_idx.second];
        SlotOp::lock(slot);
        // Double check.
        if (load_begin_sn_() <= ctrl_sn) {
          ctrl = true;
        } else {
          SlotOp::unlock(slot);
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "failed to new Segment", K(ret), K(slot_idx.first));
      }
    }
    return ret;
  }
  void unctrl_upper_lmt_sn_(const int64_t ctrl_sn, Dir *dir)
  {
    SlotIdx slot_idx = calc_slot_idx_(ctrl_sn, dir);
    SlotT &slot = dir->segs_[slot_idx.first]->slots_[slot_idx.second];
    SlotOp::unlock(slot);
  }
  // Rearrange Segments in new Dir.
  void expand_rearrange_(const int64_t ctrl_sn, Dir *dir, Dir *new_dir)
  {
    if (NULL == dir || NULL == new_dir) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err dir", K(dir), K(new_dir));
    }
    else {
      SlotIdx slot_idx = calc_slot_idx_(ctrl_sn, dir);
      SlotIdx new_slot_idx = calc_slot_idx_(ctrl_sn, new_dir);
      for (int64_t idx = 0; idx < dir->seg_cnt_; ++idx) {
        int64_t seg_idx = (slot_idx.first + idx) % dir->seg_cnt_;
        int64_t new_seg_idx = (new_slot_idx.first + idx) % new_dir->seg_cnt_;
        new_dir->segs_[new_seg_idx] = dir->segs_[seg_idx];
      }
    }
  }
  // Do expand.
  int do_expand_(const int64_t seg_cnt)
  {
    int ret = OB_SUCCESS;
    Dir* dir2retire = NULL;
    if (estrylock_()) {
      Dir *old_dir = load_dir_();
      if (NULL == old_dir) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "err dir", K(ret));
      }
      else if (old_dir->seg_cnt_ < seg_cnt) {
        Dir *new_dir = NULL;
        int64_t ctrl_sn = -1;
        if (NULL == (new_dir = new_dir_(seg_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "failed to new Dir", K(ret), K(seg_cnt));
        } else if (OB_SUCCESS != (ret = ctrl_upper_lmt_sn_(old_dir, ctrl_sn))) {
          dir2retire = new_dir;
          LIB_LOG(WARN, "failed to control upper lmt sn", K(ret), K(ctrl_sn));
        } else {
          expand_rearrange_(ctrl_sn, old_dir, new_dir);
          set_dir_(new_dir); // Replace.
          unctrl_upper_lmt_sn_(ctrl_sn, old_dir);
          dir2retire = old_dir;
        }
      } else {
        ret = OB_EAGAIN; // Seg cnt enough.
      }
      esunlock_();
    } else {
      ret = OB_EAGAIN; // Dir under modification.
    }
    if (NULL != dir2retire) {
      get_dir_ref().retire(dir2retire);
    }
    return ret;
  }
  // Dir - Shrink.
  bool need_shrink_(const SlotIdx &slot_idx, Dir *dir)
  {
    bool bret = false;
    if (seg_capacity_ - 1 == slot_idx.second) {
      int64_t begin_sn = load_begin_sn_();
      int64_t end_sn = load_end_sn_();
      int64_t upper_lmt_sn = calc_upper_lmt_sn_(begin_sn, dir);
      // Condition: more than half empty && enough Segments.
      bret = ((end_sn - begin_sn) < (upper_lmt_sn - end_sn)) && (MIN_SEG_CNT < dir->seg_cnt_);
    }
    return bret;
  }
  // Rearrange Segments in new Dir.
  void shrink_rearrange_(const int64_t ctrl_sn, Dir *dir, Dir *new_dir)
  {
    if (NULL == dir || NULL == new_dir) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err dir", K(dir), K(new_dir));
    }
    else {
      SlotIdx slot_idx = calc_slot_idx_(ctrl_sn + 1, dir); // Slot index of the next begin slot.
      SlotIdx new_slot_idx = calc_slot_idx_(ctrl_sn + 1, new_dir);
      for (int64_t idx = 0; idx < (dir->seg_cnt_ - 1); ++idx) {
        int64_t seg_idx = (slot_idx.first + idx) % dir->seg_cnt_;
        int64_t new_seg_idx = (new_slot_idx.first + idx) % new_dir->seg_cnt_;
        new_dir->segs_[new_seg_idx] = dir->segs_[seg_idx];
      }
    }
  }
  // Do shrink.
  int do_shrink_(const int64_t ctrl_sn, const Dir *dir)
  {
    int ret = OB_SUCCESS;
    if (estrylock_()) {
      Dir *old_dir = load_dir_();
      Dir *new_dir = NULL;
      int64_t seg_cnt = 0;
      if (NULL == old_dir) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "err dir", K(old_dir));
        // double check, need_shrink may be based on a stale dir
      } else if (dir != old_dir) {
        ret = OB_EAGAIN;
      } else if (NULL == (new_dir = new_dir_(seg_cnt = (old_dir->seg_cnt_ - 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "failed to new Dir", K(ret), K(seg_cnt));
      } else {
        shrink_rearrange_(ctrl_sn, old_dir, new_dir);
        // Order is Vital!!!
        WLockGuard guard(sn_dir_lock_);
        set_dir_(new_dir); // Replace. It's caller's duty to retire Dir & Segment.
        // Ensure atomicity of updating begin_sn_ & Dir
        //
        update_begin_sn_(ctrl_sn+1);
      }
      esunlock_();
    } else {
      ret = OB_EAGAIN; // Dir under modification.
    }
    return ret;
  }
  // Data access - Get.
  int do_get_(const int64_t sn, ValT &val)
  {
    int ret = OB_EAGAIN;
    while(OB_EAGAIN == ret) {
      // Check sn.
      if (sn < load_begin_sn_()) {
        ret = OB_ERR_OUT_OF_LOWER_BOUND;
      } else if (load_end_sn_() <= sn) {
        ret = OB_ERR_OUT_OF_UPPER_BOUND;
      }
      // Do get.
      else {
        Dir *dir = (Dir*)get_dir_ref().acquire((void**)&dir_);
        if (NULL == dir) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(ERROR, "err dir", K(ret));
        }
        else {
          SlotIdx slot_idx = calc_slot_idx_(sn, dir);
          Segment *seg = (Segment*)get_seg_ref().acquire((void**)&dir->segs_[slot_idx.first]);
          if (is_dir_changed(dir)) {
          } else if (NULL == seg) {
            // NULL Segment, get default val of T.
            SlotT fake_slot;
            val = SlotOp::default_val(fake_slot);
            ret = OB_SUCCESS;
          }
          else {
            SlotT &slot = seg->slots_[slot_idx.second];
            SlotOp::lock(slot);
            val = (ValT)get_val_ref().acquire((ValT*)&slot.val_);
            SlotOp::unlock(slot);
            ret = OB_SUCCESS;
          }
          get_seg_ref().release(seg);
          get_dir_ref().release(dir);
          // Double check sn.
          if (sn < load_begin_sn_()) {
            ret = OB_ERR_OUT_OF_LOWER_BOUND;
          }
        }
      }
    }
    return ret;
  }
  // Data access - Set.
  template <typename Func> int do_set_(const int64_t sn, const ValT &val, Func &cond, bool &set)
  {
    int ret = OB_EAGAIN;
    set = false;
    while (OB_EAGAIN == ret) {
      int64_t begin_sn = load_begin_sn_();
      // Check sn.
      if (begin_sn <= sn) {
        Dir *dir = (Dir*)get_dir_ref().acquire((void**)&dir_);
        if (NULL == dir) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(ERROR, "err dir", K(ret));
        }
        else {
          // upper limit sn sets a limit on slot, only slots of
          // sn < upper limit sn are writable.
          // upper limit sn increases monotonically, user figures if out
          // by begin sn and dir.
          // In shrink, begin sn is updated after dir change, this makes
          // upper limit sn always valid.
          if (sn < calc_upper_lmt_sn_(begin_sn, dir)) {
            SlotIdx slot_idx = calc_slot_idx_(sn, dir);
            Segment *seg = (Segment*)get_seg_ref().acquire((void**)&dir->segs_[slot_idx.first]);
            // Ensure Segment.
            if (is_dir_changed(dir)) {
            } else if (NULL != seg || (OB_SUCCESS == (ret = ensure_segment_(dir, slot_idx.first, seg)))) {
              SlotT &slot = seg->slots_[slot_idx.second];
              SlotOp::lock(slot);
              // Double check.
              if (load_begin_sn_() <= sn) {
                if (cond(SlotOp::get(slot), val)) {
                  // Set.
                  SlotOp::set(slot, val);
                  update_end_sn_(sn + 1);
                  set = true;
                }
                ret = OB_SUCCESS;
              }
              else {
                ret = OB_ERROR_OUT_OF_RANGE;
              }
              SlotOp::unlock(slot);
            }
            get_seg_ref().release(seg);
            get_dir_ref().release(dir);
          }
          else {
            // Capacity not enough. Expand and retry on success OR when Dir modified.
            int64_t expect_seg_cnt = (2 + ((sn - begin_sn + 1) / seg_capacity_));
            get_dir_ref().release(dir);
            ret = do_expand_(expect_seg_cnt);
            ret = (ret == OB_SUCCESS) ? OB_EAGAIN : ret;
          }
        }
      } else {
        ret = OB_ERROR_OUT_OF_RANGE;
      }

    }
    return ret;
  }
  // Data access - Pop.
  template <typename Func> int do_pop_(Func &cond, ValT &val, bool &popped, bool use_lock)
  {
    int ret = OB_EAGAIN;
    popped = false;
    bool shrink = false;
    while (OB_EAGAIN == ret) {
      int64_t begin_sn = load_begin_sn_();
      // Check sn.
      if (begin_sn < load_end_sn_()) {
        Dir *dir = (Dir*)get_dir_ref().acquire((void**)&dir_);
        if (NULL == dir) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(ERROR, "err dir", K(ret));
        }
        else {
          SlotIdx slot_idx = calc_slot_idx_(begin_sn, dir);
          Segment *seg = (Segment*)get_seg_ref().acquire((void**)&dir->segs_[slot_idx.first]);
          // Ensure Segment.
          if (is_dir_changed(dir)) {
          } else if (NULL != seg || (OB_SUCCESS == (ret = ensure_segment_(dir, slot_idx.first, seg)))) {
            SlotT &slot = seg->slots_[slot_idx.second];
            bool lock_succ = false;
            if (use_lock) {
              SlotOp::lock(slot);
              lock_succ = true;
            } else {
              lock_succ = SlotOp::try_lock(slot);
            }
            if (lock_succ) {
              // Double check.
              if (begin_sn == load_begin_sn_()) {
                // Get slot value and test condition.
                if (cond(begin_sn, SlotOp::get(slot))) {
                  // Order is Vital!!!
                  // 1. Return popped value.
                  // 2. Do shrink if necessary.
                  // 3. Update begin_sn.
                  // 4. Clean slot.
                  // Note: Do shrink before updating begin sn, so that the
                  //       upper limit sn is properly controlled, and another
                  //       thread won't set() on the to-be-freed segment.
                  //       Update begin sn before cleaning slot, so that
                  //       user read valid value when sn >= begin sn.
                  // Return value.
                  popped = true;
                  val = SlotOp::get(slot);
                  // Shrink. Errors are printed to log & swallowed.
                  if (need_shrink_(slot_idx, dir)) {
                    ret = do_shrink_(begin_sn, dir);
                    if (OB_SUCCESS == ret) {
                      shrink = true;
                    }
                    else if (OB_EAGAIN == ret) {
                      // Pass.
                    }
                    else {
                      // Err happened, print err code.
                      LIB_LOG(WARN, "failed to do shrink", K(ret));
                    }
                    // Not need to return do_shrink_()'s err code.
                    ret = OB_SUCCESS;
                  }
                  // Update begin sn.
                  if (begin_sn == load_begin_sn_()) {
                    update_begin_sn_(begin_sn + 1);
                  }
                  // Clean slot.
                  ValT deft_val = SlotOp::default_val(slot);
                  SlotOp::set(slot, deft_val);
                }
                ret = OB_SUCCESS; // OB_SUCCESS no matter popped or not.
              }
              else {
                ret = OB_EAGAIN;
              }
              SlotOp::unlock(slot);
            } else {
              ret = OB_ENTRY_NOT_EXIST;
            }
          }
          get_seg_ref().release(seg);
          get_dir_ref().release(dir);
          if (shrink) {
            get_seg_ref().retire(seg);
            get_dir_ref().retire(dir);
          }
        }
      } else {
        ret = OB_ENTRY_NOT_EXIST; // No available Slot.
      }
    }
    return ret;
  }
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  bool inited_;
  int64_t seg_size_;
  int64_t seg_capacity_;
  int64_t begin_sn_;
  int64_t end_sn_ CACHE_ALIGNED; // Trade mem for speed.
  // Dir.
  Dir *dir_;
  // Expand, shrink, Segment alloc lock.
  ObSpinLock es_lock_;
  RWLock sn_dir_lock_;
  // Pop lock;
  // Dir & Segment allocator.
  ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObExtendibleRingBufferBase);
};


}
}
}

#endif
