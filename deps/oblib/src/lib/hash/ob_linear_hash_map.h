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

/**
 * ObLinearHashMap
 *
 * A high performance extendible hash map. Supports insert(), erase(), get(),
 * for_each(), remove_if(), trivial iterating and more. Memory is managed
 * properly by hash map itself. Can grow up to today's hardware memory limit,
 * don't worry about its capacity. Performance is irrelevant to its size.
 *
 * When to use:
 *   - need high performance random access to a lot of Key Value pairs.
 *   - Key Value pair count grows from 0 to 1,000,000,000.
 *
 * When not to use:
 *   - highly concurrent access to a few Key Value pairs (lock based).
 *   - memory in shortage (trade mem for speed).
 *   - Key Value pair of size >= 512B (perf degrades).
 *
 * Performance(2.4GHz CPU):
 *   - Insert <16B, 8B>
 *     + 16 threads: > 5,000,000 per sec
 *     + 48 threads: > 7,000,000 per sec
 *   - Get <16B, 8B>
 *       thread count | load factor | get speed (per sec)
 *     +            8           10%          > 20,000,000
 *     +            8           50%          > 20,000,000
 *     +            8          100%          > 19,000,000
 *     +            8          500%          > 11,000,000
 *     +            8         1000%           > 6,000,000
 *     +           16           10%          > 35,000,000
 *     +           16           50%          > 35,000,000
 *     +           16          100%          > 35,000,000
 *     +           16          500%          > 21,000,000
 *     +           16         1000%          > 11,000,000
 *   - Concurrent Get on 1 Key <16B, 8B>
 *     + 8  threads: around 2,000,000
 *     + 16 threads: around 2,000,000
 *       Note: caused by lock contention on buckets.
 *
 * Memory usage:
 *   - HashMap object size: 712B.
 *   - Dynamic alloc mem size after init: 16KB (by default settings).
 *     (4KB counter + 4KB directory + 8KB segment)
 *   - HashMap of the same Key, Value and MemMrgTag share the same
 *     memory management facilities. This memory consumption is amortized.
 *
 * Manual:
 *   - Key, Value, MemMgrTag
 *     + Key and Value should be copyable.
 *     + Key: a class or a pointer to class which owns:
 *         uint64_t hash() const;
 *         bool operator==(const Key &other) const;
 *     + MemMgrTag: a tag used to specify which memory manager to use.
 *       Its default value is ShareMemMgrTag, which means HashMap of
 *       the same Key, Value share one memory manager.
 *     + When specified as UniqueMemMgrTag, each HashMap owns an individual
 *       memory manager.
 *     + You can set your own tag to manage it more refinedly.
 *
 *   - Interface
 *     + Declaration:
 *         template <typename Key, typename Value,
 *                   typename MemMgrTag = ShareMemMgrTag>
 *         class ObLinearHashMap;
 *     + Init & Destroy:
 *         int init(); // Don't set any argument.
 *         int destroy();
 *     + Insert:
 *         int insert(const Key &key, const Value &value);
 *     + Get:
 *         int get(const Key &key, Value &value) const;
 *         int get(const Key &key, Value &value, Key &copy_inner_key) const;
 *         // Special get which returns a copy of key stored in hash map.
 *         // It's useful when 2 different keys are "equal" by operator==().
 *     + Erase:
 *         int erase(const Key &key);
 *         int erase(const Key &key, Value &value); // Return a copy of value.
 *     + Count:
 *         uint64_t count() const; // It's an expensive call.
 *     + Foreach:
 *         template <typename Function> int for_each(Function &fn);
 *         // Call Function on every Key Value.
 *         // Function: bool operator()(const Key &key, Value &value);
 *         // Returning false stops for_each() and for_each() returns OB_EAGAIN.
 *     + RemoveIf:
 *         template <typename Function> int remove_if(Function &fn);
 *         // Call Function on every element of this hash map.
 *         // Function: bool operator()(const Key &key, Value &value);
 *         // Returning true removes current Key Value from hash map.
 *     + Operate:
 *         template <typename Function>
 *         int operate(const Key &key, Function &fn);
 *         // Call Function on certain Value of this hash map.
 *         // Useful when user make small changes on Value.
 *         // Function: bool operator()(const Key &key, Value &value);
 *         // Returning false causes operate() returning OB_EAGAIN.
 *     + Trivial Iterating.
 *         class BlurredIterator {
 *             BlurredIterator(ObLinearHashMap &map);
 *             int next(Key &key, Value &value); // Get next till OB_ITER_END.
 *             void rewind(); // Rewind to first Key.
 *         };
 *         It's a light weight iterator used to do trivial scanning, like
 *         periodically background scan.
 *         Some Key Value pairs might be skipped or returned multiple times
 *         during a full iterating.
 *
 *  - CAUTION:
 *      + DONNOT call any function of the same HashMap in the Function
 *        supplied to for_each(), remove_if() and operate().
 *      + DONNOT call any function of another HashMap who shares the
 *        same memory manager in the Function supplied to for_each(),
 *        remove_if() and operate(). If this might happen, set a special
 *        MemMgrTag to this HashMap so a different memory manager is used.
 *
 */

#ifndef OCEANBASE_LIB_HASH_OB_LINEAR_HASH_MAP_H_
#define OCEANBASE_LIB_HASH_OB_LINEAR_HASH_MAP_H_

// Debug macro.
#ifndef OB_LINEAR_HASH_MAP_UNITTEST_FRIEND
#define OB_LINEAR_HASH_MAP_UNITTEST_FRIEND
#endif

#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_external_ref.h"
#include "lib/ob_running_mode.h"

namespace oceanbase
{
namespace common
{

template <typename T> const T* __ptr__(const T &ref) { return &ref; }
template <typename T> const T* __ptr__(T *ptr) { return ptr; }
template <typename T> const T* __ptr__(const T *ptr) { return ptr; }

template <typename T>
struct Hash
{
  int operator()(const T &t, uint64_t &hash_val)
  {
    return __ptr__(t)->hash(hash_val);
  }
};

template <typename T>
struct Equal
{
  bool operator()(const T &t1, const T &t2)
  {
    return __ptr__(t1) != NULL
        && __ptr__(t2) != NULL
        && *(__ptr__(t1)) == *(__ptr__(t2));
  }
};


/* Share Memory Manager Mode. Default. */
/* In this mode, all Hash Maps with the same Key and Value share */
/* the same Memory Manager. */
struct ShareMemMgrTag { };
/* Unique Memory Manager Mode. */
/* Don't use this mode unless you know what it means. */
struct UniqueMemMgrTag { };

// avoid allocator destructed before HashMap
template <typename Key, typename Value, typename MemMgrTag>
struct ConstructGuard
{
public:
  ConstructGuard();
};

template <typename Key, typename Value, typename MemMgrTag = ShareMemMgrTag>
class ObLinearHashMap : public ConstructGuard<Key, Value, MemMgrTag>
{
  friend class ConstructGuard<Key, Value, MemMgrTag>;
private:
  /* Entry. */
  struct Node
  {
    Key key_;
    Value value_;
    Node *next_;
  };
  /* Bucket. */
  struct Bucket
  {
    Key key_;
    Value value_;
    Node *next_;
    uint8_t bkt_L_; // L of this bucket
    uint8_t flag_; // nonempty|active|lock
  };
  /* Hazard Pointer struct. Cache line aligned and padded. */
  /* Hash Map memory manager. */
  /* Memory alloc here uses mod id: LINEAR_HASH_MAP. */
  /* One combination of Key, Value and MemMgrTag corresponds to one Core class. */
  class HashMapMemMgrCore
  {
  public:
    HashMapMemMgrCore();
    ~HashMapMemMgrCore();
    static HashMapMemMgrCore& get_instance();
    ObExternalRef* get_external_ref();
    ObSmallAllocator& get_node_alloc();
    ObConcurrentFIFOAllocator& get_dir_alloc();
    ObConcurrentFIFOAllocator& get_cnter_alloc();
    void add_map(void *ptr);
    void rm_map(void *ptr);
  private:
    ObExternalRef hash_ref_;
    ObSmallAllocator node_alloc_;
    ObConcurrentFIFOAllocator dir_alloc_;
    ObConcurrentFIFOAllocator cnter_alloc_;
    typedef common::ObArray<void*> MapArray;
    MapArray map_array_;
    ObSpinLock map_array_lock_;
    DISALLOW_COPY_AND_ASSIGN(HashMapMemMgrCore);
  };
  /* Mem Mgr uses template in order to specialize an unique mem mgr class. */
  template <typename Tag, typename Dummy = void>
  class HashMapMemMgr
  {
  public:
    typedef HashMapMemMgrCore Core;
    HashMapMemMgr() {}
    virtual ~HashMapMemMgr() {}
    ObExternalRef* get_external_ref();
    ObSmallAllocator& get_node_alloc();
    ObConcurrentFIFOAllocator& get_dir_alloc();
    ObConcurrentFIFOAllocator& get_cnter_alloc();
    void add_map(void *ptr) { Core::get_instance().add_map(ptr); }
    void rm_map(void *ptr) { Core::get_instance().rm_map(ptr); }
  private:
    DISALLOW_COPY_AND_ASSIGN(HashMapMemMgr);
  };
  /* Specialization. */
  /* Dummy helps partially specialize HashMapMemMgr in this non-specialized class. */
  template <typename Dummy>
  class HashMapMemMgr<UniqueMemMgrTag, Dummy>
  {
  public:
    HashMapMemMgr() : core_() {}
    virtual ~HashMapMemMgr() {}
    typedef HashMapMemMgrCore Core;
    ObExternalRef* get_external_ref();
    ObSmallAllocator& get_node_alloc();
    ObConcurrentFIFOAllocator& get_dir_alloc();
    ObConcurrentFIFOAllocator& get_cnter_alloc();
    void add_map(void *ptr) { Core::get_instance().add_map(ptr); }
    void rm_map(void *ptr) { Core::get_instance().rm_map(ptr); }
  private:
    Core core_;
  };
  /* A fast counter. Several threads share one counter. */
  /* It is used to count key value number, and operation number. */
  /* In this impl, thread id mod counter cnt is used to assign counters. */
  class Cnter
  {
    static const int64_t CNTER_CNT = 64L;
    struct Counter
    {
      int64_t cnt_;
      int64_t op_cnt_;
      int64_t padding_[6];
    } CACHE_ALIGNED;
  public:
    Cnter();
    virtual ~Cnter() {}
    int init(HashMapMemMgr<MemMgrTag> &mem_mgr);
    int destroy();
    void add(const int64_t cnt, const int64_t th_id = 0);
    int64_t count() const;
    bool op_and_test_lmt(const int64_t lmt, const int64_t th_id = 0);
  private:
    HashMapMemMgr<MemMgrTag> *mem_mgr_;
    Counter *cnter_;
  };
public:
  /* BlurredIterator.
   * CAUTION: it's not a C++ STL style iterator.
   *
   * It covers every element before returning OB_ITER_END if none of
   * get(), insert() or erase() is called during iterating. Otherwise,
   * some elements might be skipped, or be returned multiple times.
   *
   * Use for_each(fn) to iterate on every element of this hashmap.
   */
  class BlurredIterator
  {
  public:
    explicit BlurredIterator(ObLinearHashMap &map) : map_(&map) { rewind(); }
    virtual ~BlurredIterator() {}
    int next(Key &key, Value &value);
    void rewind();
  private:
    ObLinearHashMap *map_;
    uint64_t bkt_idx_;
    uint64_t key_idx_;
  };

private:
  /* Parameters L & P. High-order 8 bits for L, low-order 56 bits for p. */
  static const uint64_t LP_P_BIT_NUM = 56;
  static const uint64_t LP_L_BIT_NUM = 8;
  static const uint64_t LP_L_CNT = (1 << LP_L_BIT_NUM); // [0, 255]
  static const uint64_t LP_P_MASK = ((uint64_t)1 << LP_P_BIT_NUM) - 1;
  static const uint64_t LP_L_MASK = ((uint64_t)1 << LP_L_BIT_NUM) - 1;
  /* Dynamic array: Directory, Micro-Segment and Standard-Segment. */
  static const uint64_t M_SEG_SZ_L_LMT = (uint64_t)1 << 12; // 4KB
  static const uint64_t M_SEG_SZ_U_LMT = (uint64_t)1 << 17; // 128KB
  static const uint64_t S_SEG_SZ_L_LMT = (uint64_t)1 << 14; // 16KB
  static const uint64_t S_SEG_SZ_U_LMT = (uint64_t)1 << 27; // 128MB
  static const uint64_t S_M_SEG_RATIO_U_LMT = (uint64_t)1 << 10; // 1024
  static const uint64_t S_M_SEG_RATIO_L_LMT = (uint64_t)1 << 4; // 16
  static const uint64_t DIR_SZ_L_LMT = (uint64_t)1 << 12; // 4KB
  static const uint64_t DIR_EXPAND_RATE = (uint64_t)2; // Expand by 2.
  /* Bucket. */
  //static const uint64_t BKT_SZ_LMT = 512; // 512B, can't be too large.
  static const uint64_t BKT_SZ_LMT = 1024; // 512B, can't be too large.
  static const uint8_t BKT_LOCK_MASK = 0x01;
  static const uint8_t BKT_ACTIVE_MASK = 0x02;
  static const uint8_t BKT_NONEMPTY_MASK = 0x04;
  /* Status code for expansion & shrinking. */
  static const int ES_INVALID = -1;
  static const int ES_SUCCESS = 0;
  static const int ES_OUT_OF_MEM = 1;
  static const int ES_TRYLOCK_FAILED = 2;
  static const int ES_REACH_LIMIT = 3;
  static const int ES_REACH_FOREACH_LIMIT = 4;
  /* Load factor. */
  static const double LOAD_FCT_DEF_U_LMT;
  static const double LOAD_FCT_DEF_L_LMT;
  static const uint64_t LOAD_FCT_REFR_LMT = 100; // Refresh load factor periodically. Worst case: 6400 ops trigger one update.
  /* Mod id for ob_malloc. */
  static const int TENANT_ID = OB_SERVER_TENANT_ID;
  static constexpr const char *LABEL = ObModIds::OB_LINEAR_HASH_MAP;

public:
  ObLinearHashMap()
    : init_(false),
    load_factor_(0.0),
    load_factor_u_limit_(0.0),
    load_factor_l_limit_(0.0),
    Lp_(0),
    eslock_(common::ObLatchIds::HASH_MAP_LOCK),
    m_seg_sz_(0),
    m_seg_bkt_n_(0),
    m_seg_n_lmt_(0),
    m_seg_bkt_n_lmt_(0),
    s_seg_sz_(0),
    s_seg_bkt_n_(0),
    L0_bkt_n_(0),
    dir_sz_(0),
    dir_seg_n_lmt_(0),
    dir_(NULL),
    cnter_(),
    mem_mgr_(),
    seg_ref_(NULL),
    foreach_L_lmt_(0),
    memattr_()
    {}
  virtual ~ObLinearHashMap()
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = destroy())) {
      LIB_LOG(WARN, "failed to destroy hash map", K(ret));
    }
  }
  // Initialization & destruction.
  // m_seg_sz and s_seg_sz are the sizes of micro-segment and standard-segment.
  // Set m_seg_sz = 0 to disable micro-segment.
  // dir_init_sz is the initial size of directory, it doubles when overflows.
  int init(const lib::ObLabel &label = LABEL, uint64_t tenant_id = TENANT_ID);
  int init(uint64_t m_seg_sz, uint64_t s_seg_sz, uint64_t dir_init_sz,
      const lib::ObLabel &label = LABEL, uint64_t tenant_id = TENANT_ID);
  int destroy();
  // Load factor control.
  int set_load_factor_lmt(double lower_lmt, double upper_lmt);
  double get_load_factor() const;
  // Erase all keys.
  int clear();
  // Reset. Erase all keys.
  int reset();
  // Only functions below are thread safe.
  // Key value access methods.
  int insert(const Key &key, const Value &value);
  int insert_or_update(const Key &key, const Value &value);
  int get(const Key &key, Value &value) const;
  int get(const Key &key, Value &value, Key &copy_inner_key) const; // Only for memtable.
  int erase(const Key &key);
  int erase(const Key &key, Value &value);
  uint64_t count() const;
  int resize(const uint64_t bkt_cnt);
  uint64_t get_bkt_cnt() const { return bkt_cnt_(); }
  // For each.
  // Call fn on every element of this hash map.
  // fn: bool operator()(const Key &key, Value &value);
  // If operator() returns false, for_each() would stop immediately and returns OB_EAGAIN.
  template <typename Function> int for_each(Function &fn);
  // Remove if.
  // Call fn on every element of this hash map.
  // fn: bool operator()(const Key &key, Value &value);
  // If operator() returns true, for_each() would remove this key value pair immediately.
  template <typename Function> int remove_if(Function &fn);
  // Erase if.
  // Call fn on the specified Value under lock protection, erase the element when
  // fn returns true.
  // Useful when you need to test value and then decide to erase or not.
  // fn: bool operator()(const Key &key, Value &value);
  // If operator() returns true, erase this key value pair, returns OB_SUCCESS.
  // If operator() return false, it returns OB_EAGAIN.
  template <typename Function> int erase_if(const Key &key, Function &fn);
  // Operate.
  // Call fn on the specified Value under lock protection.
  // Useful when you do small changes on value.
  // fn: bool operator()(const Key &key, Value &value);
  // If operator() returns false, operate() return OB_EAGAIN.
  template <typename Function> int operate(const Key &key, Function &fn);

  bool is_inited() const { return init_; }

private:
  // Parameters L & P.
  void load_Lp_(uint64_t &L, uint64_t &p) const;
  void set_Lp_(uint64_t L, uint64_t p);
  uint64_t bkt_cnt_() const;
  // Expand & Shrink lock.
  bool es_trylock_();
  void es_lock_();
  void es_unlock_();
  // Dynamic array: Directory, Micro-Segment and Standard-Segment.
  int init_d_arr_(uint64_t m_seg_sz, uint64_t s_seg_sz, uint64_t dir_init_sz);
  void des_d_arr_();
  Bucket* cons_seg_(uint64_t seg_sz);
  void des_seg_(Bucket *seg);
  Bucket** cons_dir_(uint64_t dir_sz, Bucket **old_dir, uint64_t old_dir_sz);
  void des_dir_(Bucket **dir);
  uint64_t seg_idx_(uint64_t bkt_idx);
  uint64_t seg_bkt_idx_(uint64_t bkt_idx);
  // Buckets.
  void set_bkt_lock_(Bucket *bkt, bool to_lock);
  void set_bkt_active_(Bucket *bkt, bool to_activate);
  bool is_bkt_active_(const Bucket *bkt);
  void set_bkt_nonempty_(Bucket *bkt, bool to_nonempty);
  bool is_bkt_nonempty_(const Bucket *bkt);
  // Expand & Shrink.
  void load_factor_ctrl_(const uint64_t seed);
  int expand_();
  int shrink_();
  void load_expand_d_seg_bkts_(uint64_t L, uint64_t p, Bucket* &src_bkt, Bucket* &dst_bkt);
  void split_expand_d_seg_bkts_(uint64_t L, uint64_t p, Bucket *src_bkt, Bucket *dst_bkt);
  void unload_expand_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt);
  void load_shrink_d_seg_bkts_(uint64_t L, uint64_t p, Bucket* &src_bkt, Bucket* &dst_bkt);
  int unite_shrink_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt);
  void unload_shrink_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt, bool succ);
  // Hazard pointer.
  void init_haz_();
  Bucket* load_haz_seg_(Bucket* &seg_ref);
  void unload_haz_seg_(Bucket* seg);
  Bucket* wait_evict_haz_seg_(Bucket* &to_evict_seg_ref);
  Bucket** load_haz_dir_(Bucket** &dir_ref);
  void unload_haz_dir_(Bucket** dir);
  Bucket** wait_replace_haz_dir_(Bucket** &to_replace_dir_ref, Bucket **new_dir);
  int64_t get_thread_id_();
  // Bucket access.
  Bucket* load_access_bkt_(const Key &key, uint64_t &hash_v, Bucket*& seg);
  Bucket* load_access_bkt_(uint64_t bkt_idx, Bucket*& seg);
  void unload_access_bkt_(Bucket *bkt, Bucket* seg);
  int do_clear_();
  uint64_t do_clear_seg_(Bucket *seg, uint64_t bkt_n);
  int do_insert_(const Key &key, const Value &value);
  int do_insert_or_update_(const Key &key, const Value &value);
  int do_get_(const Key &key, Value &value);
  int do_get_(const Key &key, Value &value, Key &copy_inner_key); // Only for memtable.
  int do_erase_(const Key &key, Value *value);
  uint64_t do_cnt_() const;
  void add_cnt_(int64_t cnt);
  // BlurredIterator.
  int blurred_iter_get_(uint64_t bkt_idx, uint64_t key_idx, Key &key, Value &value);
  // For each.
  void init_foreach_();
  uint64_t set_foreach_L_lmt_();
  void unset_foreach_L_lmt_(const uint64_t L_lmt);
  template <typename Function>
  struct DoForeachOnBkt
  {
    bool operator()(ObLinearHashMap<Key, Value, MemMgrTag> &host, Bucket *bkt, Function &fn);
  };
  template <typename Function>
  struct DoRemoveIfOnBkt
  {
    bool operator()(ObLinearHashMap<Key, Value, MemMgrTag> &host, Bucket *bkt, Function &fn);
  };
  template <typename Function, typename DoOp> bool do_foreach_(Function &fn, DoOp &op);
  template <typename Function, typename DoOp> bool do_foreach_scan_(uint64_t bkt_idx,
      uint64_t bkt_L, Function &fn, DoOp &op);
  template <typename Function> int do_erase_if_(const Key &key, Function &fn);
  template <typename Function> int do_operate_(const Key &key, Function &fn);

private:
  bool init_;
  double load_factor_;
  double load_factor_u_limit_;
  double load_factor_l_limit_;
  // Parameters L & P. High-order 8 bits for L, low-order 56 bits for p.
  uint64_t Lp_;
  // Expand & Shrink lock.
  ObSpinLock eslock_;
  // Dynamic array: Directory, Micro-Segment and Standard-Segment.
  uint64_t m_seg_sz_;
  uint64_t m_seg_bkt_n_;
  uint64_t m_seg_n_lmt_;
  uint64_t m_seg_bkt_n_lmt_;
  uint64_t s_seg_sz_;
  uint64_t s_seg_bkt_n_;
  uint64_t L0_bkt_n_;
  uint64_t dir_sz_;
  uint64_t dir_seg_n_lmt_;
  Bucket **dir_;
  // Counter.
  Cnter cnter_;
  // Memory mgr.
  HashMapMemMgr<MemMgrTag> mem_mgr_;
  // ObExternalRef.
  ObExternalRef* seg_ref_;
  static int64_t next_thread_id_;
  // Functors.
  Hash<Key> hash_func_;
  Equal<Key> equal_func_;
  // For each. Synced by es_lock_. Supports 30000 threads.
  int16_t foreach_L_lmt_arr_[LP_L_CNT];
  uint64_t foreach_L_lmt_;
  // Memory attr.
  ObMemAttr memattr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLinearHashMap);
  // For unit test.
  OB_LINEAR_HASH_MAP_UNITTEST_FRIEND;
};

template <typename Key, typename Value, typename MemMgrTag>
constexpr const char *ObLinearHashMap<Key, Value, MemMgrTag>::LABEL;

template <typename Key, typename Value, typename MemMgrTag>
template <typename Dummy>
ObExternalRef* ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<UniqueMemMgrTag, Dummy>::get_external_ref()
{
  return core_.get_external_ref();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Dummy>
ObSmallAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<UniqueMemMgrTag, Dummy>::get_node_alloc()
{
  return core_.get_node_alloc();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Dummy>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<UniqueMemMgrTag, Dummy>::get_dir_alloc()
{
  return core_.get_dir_alloc();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Dummy>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<UniqueMemMgrTag, Dummy>::get_cnter_alloc()
{
  return core_.get_cnter_alloc();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Tag, typename Dummy>
ObExternalRef* ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<Tag, Dummy>::get_external_ref()
{
  return Core::get_instance().get_external_ref();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Tag, typename Dummy>
ObSmallAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<Tag, Dummy>::get_node_alloc()
{
  return Core::get_instance().get_node_alloc();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Tag, typename Dummy>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<Tag, Dummy>::get_dir_alloc()
{
  return Core::get_instance().get_dir_alloc();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Tag, typename Dummy>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgr<Tag, Dummy>::get_cnter_alloc()
{
  return Core::get_instance().get_cnter_alloc();
}

/* Hash Map memory manager. */
template <typename Key, typename Value, typename MemMgrTag>
ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::HashMapMemMgrCore()
  : map_array_lock_(common::ObLatchIds::HASH_MAP_LOCK)
{
  // Init node alloc.
  int ret = node_alloc_.init(static_cast<int64_t>(sizeof(Node)), SET_USE_500("LinearHashMapNo"));
  if (OB_FAIL(ret)) {
    LIB_LOG(WARN, "failed to init node alloc", K(ret));
  }
  int64_t total_limit = 128 * (1L << 30); // 128GB
  int64_t page_size = 0;
  if (lib::is_mini_mode()) {
    total_limit *= lib::mini_mode_resource_ratio();
  }
  page_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
  // Init dir alloc.
  ret = dir_alloc_.init(total_limit, 2 * page_size, page_size);
  dir_alloc_.set_attr(SET_USE_500("LinearHashMapDi"));
  if (OB_FAIL(ret)) {
    LIB_LOG(WARN, "failed to init dir alloc", K(ret));
  }
  // Init counter alloc.
  ret = cnter_alloc_.init(total_limit, 2 * page_size, page_size);
  cnter_alloc_.set_attr(SET_USE_500("LinearHashMapCn"));
  if (OB_FAIL(ret)) {
    LIB_LOG(WARN, "failed to init cnter alloc", K(ret));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::~HashMapMemMgrCore()
{
  int ret = OB_SUCCESS;
  if (0 < map_array_.count()) {
    for (int64_t i = 0; (i < map_array_.count()); ++i) {
      LIB_LOG(WARN, "hash map not destroy", "map_ptr", map_array_.at(i));
    }
  }
  cnter_alloc_.destroy();
  dir_alloc_.destroy();
  if (OB_SUCCESS != (ret = node_alloc_.destroy())) {
    LIB_LOG(ERROR, "failed to destroy node alloc", K(ret));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore&
ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_instance()
{
  static HashMapMemMgrCore core;
  return core;
}

template <typename Key, typename Value, typename MemMgrTag>
ObExternalRef*
ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_external_ref()
{
  return &hash_ref_;
}

template <typename Key, typename Value, typename MemMgrTag>
ObSmallAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_node_alloc()
{
  return node_alloc_;
}

template <typename Key, typename Value, typename MemMgrTag>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_dir_alloc()
{
  return dir_alloc_;
}

template <typename Key, typename Value, typename MemMgrTag>
ObConcurrentFIFOAllocator& ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_cnter_alloc()
{
  return cnter_alloc_;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::add_map(void *ptr)
{
  int ret = OB_SUCCESS;
  int lock_ret = OB_SUCCESS;
  if (OB_SUCCESS != (lock_ret = map_array_lock_.lock())) {
    LIB_LOG(ERROR, "err lock map array lock", K(lock_ret));
  }
  if (OB_SUCCESS != (ret = map_array_.push_back(ptr))) {
    LIB_LOG(WARN, "failed to push back map array", K(ret), K(ptr));
  }
  if (OB_SUCCESS != (lock_ret = map_array_lock_.unlock())) {
    LIB_LOG(ERROR, "err unlock map array lock", K(lock_ret));
  }
};

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::rm_map(void *ptr)
{
  int ret = OB_SUCCESS;
  int lock_ret = OB_SUCCESS;
  if (OB_SUCCESS != (lock_ret = map_array_lock_.lock())) {
    LIB_LOG(ERROR, "err lock map array lock", K(lock_ret));
  }
  int64_t idx = -1;
  for (int64_t i = 0; (i < map_array_.count()) && (-1 == idx); ++i) {
    if (ptr == map_array_.at(i)) {
      idx = i;
    }
  }
  if ((-1 != idx) && OB_SUCCESS != (ret = map_array_.remove(idx))) {
    LIB_LOG(WARN, "failed to remove map array", K(ret), K(idx), K(ptr));
  }
  if (OB_SUCCESS != (lock_ret = map_array_lock_.unlock())) {
    LIB_LOG(ERROR, "err unlock map array lock", K(lock_ret));
  }
};

// KV Counter.
template <typename Key, typename Value, typename MemMgrTag>
ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::Cnter() :
  mem_mgr_(NULL),
  cnter_(NULL)
{ }

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::init(HashMapMemMgr<MemMgrTag> &mem_mgr)
{
  int ret = OB_SUCCESS;
  if (NULL != cnter_) {
    LIB_LOG(ERROR, "init twice", K(cnter_));
    ret = OB_INIT_TWICE;
  } else {
    mem_mgr_ = &mem_mgr;
    int64_t sz = static_cast<int64_t>(CNTER_CNT * sizeof(Counter));
    if (NULL == (cnter_ = static_cast<Counter*>(mem_mgr_->get_cnter_alloc().alloc(sz)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "failed to alloc counter", K(sz));
    } else {
      for (int64_t idx = 0; idx < CNTER_CNT; ++idx) {
        Counter &cnter = cnter_[idx];
        cnter.cnt_ = 0;
        cnter.op_cnt_ = 0;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::destroy()
{
  if (NULL != mem_mgr_ && NULL != cnter_) {
    mem_mgr_->get_cnter_alloc().free(cnter_);
  }

  cnter_ = NULL;
  mem_mgr_ = NULL;
  return OB_SUCCESS;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::add(const int64_t cnt,
                                               const int64_t th_id)
{
  if (NULL == cnter_) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid cnter, not init", K(cnter_));
  } else {
    // Use thread id mod counter cnt to assign counter.
    // get cpu id may be used in the future.
    Counter &cnter = cnter_[(th_id % CNTER_CNT)];
    UNUSED(ATOMIC_AAF(&(cnter.cnt_), cnt));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
int64_t ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::count() const
{
  int64_t ret = 0;
  if (NULL == cnter_) {
    ret = 0;
  } else {
    for (int64_t idx = 0; idx < CNTER_CNT; ++idx) {
      const Counter &cnter = cnter_[idx];
      ret += (ATOMIC_LOAD(&(cnter.cnt_)));
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
bool ObLinearHashMap<Key, Value, MemMgrTag>::Cnter::op_and_test_lmt(
  const int64_t lmt, const int64_t th_id)
{
  bool reach_lmt = false;

  if (NULL == cnter_) {
    reach_lmt = false;
  } else {
    Counter &cnter = cnter_[(th_id % CNTER_CNT)];
    // Not atomic.
    reach_lmt = (lmt < ++cnter.op_cnt_);
    if (reach_lmt) {
      cnter.op_cnt_ = 0;
    }
  }
  return reach_lmt;
}

// BlurredIterator.
template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::BlurredIterator::next(Key &key, Value &value)
{
  int ret = OB_SUCCESS;
  if (NULL == map_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "err map", K(ret), K(map_));
  } else {
    while (OB_EAGAIN == (ret = map_->blurred_iter_get_(bkt_idx_, key_idx_, key, value))) {
      bkt_idx_ += 1;
      key_idx_ = 0;
    }
    if (OB_SUCC(ret)) {
      key_idx_ += 1;
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::BlurredIterator::rewind()
{
  bkt_idx_ = 0;
  key_idx_ = 0;
}

// Public functions.
template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::init(const lib::ObLabel &label /*= LABEL*/,
                                                 uint64_t tenant_id /*=TENANT_ID*/)
{
  return init(OB_MALLOC_NORMAL_BLOCK_SIZE, /* Small segment. */
              OB_MALLOC_BIG_BLOCK_SIZE, /* Large segment. */
              DIR_SZ_L_LMT, /* Dir size, small when init, expand * 2. */
              label,
              tenant_id);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::init(uint64_t m_seg_sz, uint64_t s_seg_sz, uint64_t dir_init_sz,
    const lib::ObLabel &label /*= LABEL*/, uint64_t tenant_id /*=TENANT_ID*/)
{
  const double LOAD_FCT_DEF_U_LMT = 1;
  const double LOAD_FCT_DEF_L_LMT = 0.01;

  int ret = OB_SUCCESS;
  /* Memory alloc from MemMgr, and its static, so label and tenant_id no longer used. */
  memattr_.tenant_id_ = tenant_id;
  memattr_.label_ = label;
  load_factor_u_limit_ = LOAD_FCT_DEF_U_LMT;
  load_factor_l_limit_ = LOAD_FCT_DEF_L_LMT;
  load_factor_ = 0.0;
  set_Lp_(0, 0);
  init_haz_();
  init_foreach_();
  if (OB_SUCCESS != (ret = init_d_arr_(m_seg_sz, s_seg_sz, dir_init_sz)))
  { }
  else if (OB_SUCCESS != (ret = cnter_.init(mem_mgr_)))
  { }
  else {
    init_ = true;
    mem_mgr_.add_map(this);
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::destroy()
{
  int ret = OB_SUCCESS;
  if (!init_) {
    // Double destroy. Support it.
  } else if (OB_SUCCESS == (ret = do_clear_())) {
    mem_mgr_.rm_map(this);
    cnter_.destroy();
    des_d_arr_();
    init_ = false;
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::set_load_factor_lmt(double lower_lmt, double upper_lmt)
{
  int ret = OB_SUCCESS;
  if (lower_lmt < 0 || upper_lmt <= 0 || lower_lmt >= upper_lmt) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    load_factor_l_limit_ = lower_lmt;
    load_factor_u_limit_ = upper_lmt;
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
double ObLinearHashMap<Key, Value, MemMgrTag>::get_load_factor() const
{
  uint64_t cnt = do_cnt_();
  uint64_t bkt_cnt = const_cast<ObLinearHashMap&>(*this).bkt_cnt_();

  return (static_cast<double>(cnt) / static_cast<double>(bkt_cnt));
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::clear()
{
  return do_clear_();
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::reset()
{
  return clear();
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::insert(const Key &key, const Value &value)
{
  return do_insert_(key, value);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::insert_or_update(const Key &key, const Value &value)
{
  return do_insert_or_update_(key, value);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::get(const Key &key, Value &value) const
{
  return const_cast<ObLinearHashMap<Key, Value, MemMgrTag>*>(this)->do_get_(key, value);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::get(const Key& key, Value& value, Key& copy_inner_key) const
{
  return const_cast<ObLinearHashMap<Key, Value, MemMgrTag>*>(this)->do_get_(key, value, copy_inner_key);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::erase(const Key &key)
{
  return do_erase_(key, NULL);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::erase(const Key &key, Value &value)
{
  return do_erase_(key, &value);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::resize(const uint64_t bkt_cnt)
{
  int ret = OB_SUCCESS;
  while (bkt_cnt > bkt_cnt_()) {
    if (ES_OUT_OF_MEM == expand_()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::count() const
{
  return do_cnt_();
}

// For each.
template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  DoForeachOnBkt<Function> op;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else if (!do_foreach_(fn, op)) {
    ret = OB_EAGAIN;
  }
  return ret;
}
// Remove if.
template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::remove_if(Function &fn)
{
  int ret = OB_SUCCESS;
  DoRemoveIfOnBkt<Function> op;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else if (!do_foreach_(fn, op)) {
    ret = OB_EAGAIN;
  }
  return ret;
}

// Erase if.
template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::erase_if(const Key &key,  Function &fn)
{
  return do_erase_if_(key, fn);
}

// Operate.
template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::operate(const Key& key, Function& fn)
{
  return do_operate_(key, fn);
}

// Parameters L & P.
template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::load_Lp_(uint64_t &L, uint64_t &p) const
{
  uint64_t Lp = ATOMIC_LOAD(&Lp_);
  L = (Lp >> LP_P_BIT_NUM) & LP_L_MASK;
  p = Lp & LP_P_MASK;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::set_Lp_(uint64_t L, uint64_t p)
{
  uint64_t Lp = ((L & LP_L_MASK) << LP_P_BIT_NUM) | (p & LP_P_MASK);
  ATOMIC_STORE(&Lp_, Lp);
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::bkt_cnt_() const
{
  uint64_t L = 0;
  uint64_t p = 0;
  load_Lp_(L, p);
  return (L0_bkt_n_ << L) + p;
}

// Expand & Shrink lock.
template <typename Key, typename Value, typename MemMgrTag>
bool ObLinearHashMap<Key, Value, MemMgrTag>::es_trylock_()
{
  return (eslock_.trylock() == OB_SUCCESS) ? true : false;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::es_lock_()
{
  int ret = eslock_.lock();
  if (OB_SUCCESS != ret) {
    LIB_LOG(ERROR, "err lock eslock", K(ret));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::es_unlock_()
{
  int ret = eslock_.unlock();
  if (OB_SUCCESS != ret) {
    LIB_LOG(ERROR, "err unlock eslock", K(ret));
  }
}

// Dynamic array: Directory, Micro-Segment and Standard-Segment.
template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::init_d_arr_(uint64_t m_seg_sz, uint64_t s_seg_sz, uint64_t dir_init_sz)
{
  int ret = OB_SUCCESS;
  // Param validation.
  if (sizeof(Bucket) > BKT_SZ_LMT) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "can't support large key value pair");
  } else if (!(m_seg_sz == 0 || (m_seg_sz >= M_SEG_SZ_L_LMT && m_seg_sz <= M_SEG_SZ_U_LMT))) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid micro-segment size", K(m_seg_sz));
  } else if (!(s_seg_sz >= S_SEG_SZ_L_LMT && s_seg_sz <= S_SEG_SZ_U_LMT)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid standard-segment size", K(s_seg_sz));
  } else if (!(dir_init_sz >= DIR_SZ_L_LMT)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid initial directory size", K(dir_init_sz));
  } else if (m_seg_sz != 0
        && (s_seg_sz / m_seg_sz > S_M_SEG_RATIO_U_LMT || s_seg_sz / m_seg_sz < S_M_SEG_RATIO_L_LMT)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid standard-micro segment size ratio", K(s_seg_sz), K(m_seg_sz));
  }
  // Settings.
  if (OB_SUCC(ret)) {
    s_seg_sz_ = s_seg_sz;
    s_seg_bkt_n_ = s_seg_sz_ / sizeof(Bucket);
    if (m_seg_sz == 0) {
      m_seg_sz_ = 0;
      m_seg_bkt_n_ = 0;
      m_seg_n_lmt_ = 0;
      m_seg_bkt_n_lmt_ = 0;
      L0_bkt_n_ = s_seg_bkt_n_;
    } else {
      m_seg_sz_ = m_seg_sz;
      m_seg_bkt_n_ = m_seg_sz_ / sizeof(Bucket);
      m_seg_n_lmt_ = s_seg_sz_ / m_seg_sz_;
      m_seg_bkt_n_lmt_ = m_seg_n_lmt_ * m_seg_bkt_n_;
      L0_bkt_n_ = m_seg_bkt_n_;
    }
    dir_sz_ = dir_init_sz;
    dir_seg_n_lmt_ = dir_sz_ / sizeof(Bucket*);
  }
  // Init dynamic array.
  if (OB_SUCC(ret)) {
    if (NULL == (dir_ = cons_dir_(dir_sz_, NULL, 0))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (NULL == (dir_[0] = cons_seg_((m_seg_sz_ != 0) ? m_seg_sz_ : s_seg_sz_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (uint64_t idx = 0; idx < L0_bkt_n_; ++idx) {
        set_bkt_active_(&dir_[0][idx], true);
        dir_[0][idx].bkt_L_ = 0;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::des_d_arr_()
{
  if (dir_ != NULL) {
    for (uint64_t idx = 0; idx < dir_seg_n_lmt_; ++idx) {
      Bucket *seg = dir_[idx];
      if (seg != NULL) { des_seg_(seg);  }
    }
    des_dir_(dir_);
  }
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket*
ObLinearHashMap<Key, Value, MemMgrTag>::cons_seg_(uint64_t seg_sz)
{
  Bucket *ret = NULL;
  if (NULL == (ret = static_cast<Bucket*>(ob_malloc(seg_sz, memattr_)))) {
    LIB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "failed to alloc segment", K(seg_sz));
  } else {
DISABLE_WARNING_GCC_PUSH
#ifdef __clang__
DISABLE_WARNING_GCC("-Wdynamic-class-memaccess")
#endif
    memset(ret, 0x00, seg_sz);
DISABLE_WARNING_GCC_POP
    for (uint64_t idx = 0; idx < seg_sz / sizeof(Bucket); ++idx) {
      Bucket *bkt = &ret[idx];
      bkt->next_ = NULL;
      bkt->bkt_L_ = 0;
      set_bkt_nonempty_(bkt, false);
      set_bkt_active_(bkt, false);
      set_bkt_lock_(bkt, false);
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::des_seg_(Bucket *seg)
{
  if (NULL != seg) {
    ob_free(seg);
    seg = NULL;
  }
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket**
ObLinearHashMap<Key, Value, MemMgrTag>::cons_dir_(uint64_t dir_sz, Bucket **old_dir, uint64_t old_dir_sz)
{
  Bucket **ret = NULL;
  uint64_t old_seg_n = (old_dir == NULL) ? 0 : (old_dir_sz / sizeof(Bucket*));
  uint64_t dir_seg_n_lmt = dir_sz / sizeof(Bucket*);
  if (dir_seg_n_lmt <= old_seg_n) {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "err seg n", K(dir_seg_n_lmt), K(old_seg_n));
  }
  else if (NULL == (ret = static_cast<Bucket**>(mem_mgr_.get_dir_alloc().alloc(dir_sz)))) {
    LIB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "failed to alloc directory", K(dir_sz));
  } else {
    for (uint64_t idx = 0; idx < old_seg_n; ++idx) {
      ret[idx] = old_dir[idx];
    }
    for (uint64_t idx = old_seg_n; idx < dir_seg_n_lmt; ++idx) {
      ret[idx] = NULL;
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::des_dir_(Bucket **dir)
{
  if (NULL != dir) {
    mem_mgr_.get_dir_alloc().free(dir);
    dir = NULL;
  }
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::seg_idx_(uint64_t bkt_idx)
{
  return (bkt_idx < m_seg_bkt_n_lmt_) ? (bkt_idx / m_seg_bkt_n_)
      : (m_seg_n_lmt_ + (bkt_idx - m_seg_bkt_n_lmt_) / s_seg_bkt_n_);
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::seg_bkt_idx_(uint64_t bkt_idx)
{
  return (bkt_idx < m_seg_bkt_n_lmt_) ? (bkt_idx % m_seg_bkt_n_)
      : ((bkt_idx - m_seg_bkt_n_lmt_) % s_seg_bkt_n_);
}

// Bucket.
template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::set_bkt_lock_(Bucket *bkt, bool to_lock)
{
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err set bkt lock");
  } else {
    if (to_lock) {
      bool locked = false;
      while (!locked) {
        uint8_t oldv = ATOMIC_LOAD(&bkt->flag_);
        if ((oldv & BKT_LOCK_MASK) == 0) {
          uint8_t newv = oldv | BKT_LOCK_MASK;
          if (oldv == ATOMIC_CAS(&(bkt->flag_), oldv, newv)) {
            locked = true;
          }
          else {
            PAUSE();
          }
        }
        else {
          PAUSE();
        }
      }
    }
    else {
      ATOMIC_STORE(&bkt->flag_, static_cast<uint8_t>(bkt->flag_ & ~BKT_LOCK_MASK));
    }
  }
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::set_bkt_active_(Bucket *bkt, bool to_activate)
{
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err set bkt active");
  } else {
    bkt->flag_ = static_cast<uint8_t>(to_activate ? (bkt->flag_ | BKT_ACTIVE_MASK) : (bkt->flag_ & ~BKT_ACTIVE_MASK));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
bool ObLinearHashMap<Key, Value, MemMgrTag>::is_bkt_active_(const Bucket *bkt)
{
  bool bret = false;
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err set bkt nonempty");
  } else {
    bret = (bkt->flag_ & BKT_ACTIVE_MASK) != 0;
  }
  return bret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::set_bkt_nonempty_(Bucket *bkt, bool to_nonempty)
{
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err set bkt nonempty");
  } else {
    bkt->flag_ = static_cast<uint8_t>(to_nonempty ? (bkt->flag_ | BKT_NONEMPTY_MASK) : (bkt->flag_ & ~BKT_NONEMPTY_MASK));
  }
}

template <typename Key, typename Value, typename MemMgrTag>
bool ObLinearHashMap<Key, Value, MemMgrTag>::is_bkt_nonempty_(const Bucket *bkt)
{
  return (NULL != bkt) && ((bkt->flag_ & BKT_NONEMPTY_MASK) != 0);
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::load_factor_ctrl_(const uint64_t seed)
{
  int err = ES_INVALID;
  // Refresh load factor in 1/LOAD_FCT_REFR_LMT odd.

  UNUSED(seed);
  int64_t thread_id = get_thread_id_();
  if (cnter_.op_and_test_lmt(LOAD_FCT_REFR_LMT, thread_id)) {
    load_factor_ = get_load_factor();
  }

  if (load_factor_ > load_factor_u_limit_) {
    err = expand_();
  } else if (load_factor_ < load_factor_l_limit_) {
    err = shrink_();
  }
  UNUSED(err);
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::expand_()
{
  int ret = ES_SUCCESS;
  if (es_trylock_()) {
    uint64_t L = 0;
    uint64_t p = 0;
    load_Lp_(L, p);
    uint64_t dst_seg_idx = seg_idx_((L0_bkt_n_ << L) + p);
    // Ensure dir.
    if (dst_seg_idx >= dir_seg_n_lmt_) {
      uint64_t new_dir_sz = dir_sz_ * DIR_EXPAND_RATE;
      Bucket **new_dir = cons_dir_(new_dir_sz, dir_, dir_sz_);
      if (new_dir == NULL) {
        ret = ES_OUT_OF_MEM;
      } else {
        // Should refresh dir size, construct new segment, create new bucket and
        // refresh L & p after the old dir has been freed, in case a thread accesses
        // a non-existent segment on the old dir.
        des_dir_(wait_replace_haz_dir_(dir_, new_dir));
        ATOMIC_STORE(&dir_sz_, new_dir_sz);
        ATOMIC_STORE(&dir_seg_n_lmt_, dir_sz_ / sizeof(Bucket*));
      }
    }
    // Ensure seg.
    if (ret == ES_SUCCESS && dir_[dst_seg_idx] == NULL) {
      uint64_t seg_sz = (dst_seg_idx < m_seg_n_lmt_) ? m_seg_sz_ : s_seg_sz_;
      if (NULL == (dir_[dst_seg_idx] = cons_seg_(seg_sz))) {
        ret = ES_OUT_OF_MEM;
      }
    }
    // Expand.
    if (ret == ES_SUCCESS) {
      Bucket *src_bkt = NULL;
      Bucket *dst_bkt = NULL;
      load_expand_d_seg_bkts_(L, p, src_bkt, dst_bkt);
      ((L0_bkt_n_ << L) == p + 1) ? set_Lp_(L + 1, 0) : set_Lp_(L, p + 1);
      es_unlock_();
      split_expand_d_seg_bkts_(L, p, src_bkt, dst_bkt);
      unload_expand_d_seg_bkts_(src_bkt, dst_bkt);
    } else {
      es_unlock_();
    }
  } else {
    ret = ES_TRYLOCK_FAILED;
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::shrink_()
{
  int ret = ES_SUCCESS;
  if (es_trylock_()) {
    uint64_t bucket_cnt = bkt_cnt_();
    uint64_t cnt = load_factor_ * bucket_cnt;
    uint64_t target_bucket_cnt = cnt / load_factor_l_limit_ + 1;
    uint64_t L = 0;
    uint64_t p = 0;
    load_Lp_(L, p);
    uint64_t newL = L;
    uint64_t newp = p;
    uint64_t shrink_cnt = 0;
    // shrink bkt_cnt / 16 bucket at most, 1 bucket at least.
    do {
      if (newL == 0 && newp == 0) {
        ret = ES_REACH_LIMIT;
        break;
      } else if (newL == foreach_L_lmt_ && newp == 0) {
        // Give up so that for_each() has at least N*(2^foreach_L_lmt_) buckets to work with.
        ret = ES_REACH_FOREACH_LIMIT;
        break;
      } else {
        newL = (newp == 0) ? (newL - 1) : newL;
        newp = (newp == 0) ? ((L0_bkt_n_ << newL) - 1) : (newp - 1);
        Bucket* src_bkt = nullptr;
        Bucket* dst_bkt = nullptr;
        // Shrink.
        load_shrink_d_seg_bkts_(newL, newp, src_bkt, dst_bkt);
        if (ES_SUCCESS == (ret = unite_shrink_d_seg_bkts_(src_bkt, dst_bkt))) {
          set_Lp_(newL, newp);
          unload_shrink_d_seg_bkts_(src_bkt, dst_bkt, true);
          // Evict empty seg.
          if (seg_bkt_idx_((L0_bkt_n_ << newL) + newp) == 0) {
            des_seg_(wait_evict_haz_seg_(dir_[seg_idx_((L0_bkt_n_ << newL) + newp)]));
          }
        } else {
          unload_shrink_d_seg_bkts_(src_bkt, dst_bkt, false);
        }
        ++shrink_cnt;
      }
    } while (target_bucket_cnt + shrink_cnt < bucket_cnt && shrink_cnt < (bucket_cnt >> 4));
    load_factor_ = (static_cast<double>(cnt) / static_cast<double>(bucket_cnt - shrink_cnt));
    es_unlock_();
  } else {
    ret = ES_TRYLOCK_FAILED;
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::load_expand_d_seg_bkts_(uint64_t L, uint64_t p, Bucket* &src_bkt,
    Bucket* &dst_bkt)
{
  if (NULL == dir_) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err dir", K(dir_));
  } else {
    Bucket *src_seg = dir_[seg_idx_(p)];
    Bucket *dst_seg = dir_[seg_idx_((L0_bkt_n_ << L) + p)];
    if (NULL == src_seg || NULL == dst_seg) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err seg", K(src_seg), K(dst_seg));
    } else {
      src_bkt = &src_seg[seg_bkt_idx_(p)];
      dst_bkt = &dst_seg[seg_bkt_idx_((L0_bkt_n_ << L) + p)];
      set_bkt_lock_(src_bkt, true);
      set_bkt_lock_(dst_bkt, true);
      set_bkt_active_(dst_bkt, true);
    }
  }
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::split_expand_d_seg_bkts_(uint64_t L, uint64_t p,
    Bucket *src_bkt, Bucket *dst_bkt)
{
  if (NULL == src_bkt || NULL == dst_bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err bkt ptr", K(src_bkt), K(dst_bkt));
  }
  else if (is_bkt_nonempty_(src_bkt)) {
    Node *split_nodes[2] = { NULL, NULL };
    Node *iter = src_bkt->next_;
    src_bkt->next_ = NULL;
    while (iter != NULL) {
      Node *cur = iter;
      iter = iter->next_;
      uint64_t new_idx = 0;
      (void)hash_func_(cur->key_, new_idx);
      new_idx = new_idx % (L0_bkt_n_ << (L + 1));
      Node* &split_node = (new_idx == p) ? split_nodes[0] : split_nodes[1];
      cur->next_ = split_node;
      split_node = cur;
    }
    uint64_t new_idx = 0;
    (void)hash_func_(src_bkt->key_, new_idx);
    new_idx = new_idx % (L0_bkt_n_ << (L + 1));
    if (new_idx != p) {
      new (&dst_bkt->key_) Key(src_bkt->key_);
      new (&dst_bkt->value_) Value(src_bkt->value_);
      set_bkt_nonempty_(dst_bkt, true);
      src_bkt->key_.~Key();
      src_bkt->value_.~Value();
      set_bkt_nonempty_(src_bkt, false);
    }
    Bucket *split_bkts[2] = { src_bkt, dst_bkt };
    for (int idx = 0; idx < 2; ++idx) {
      Bucket* &split_bkt = split_bkts[idx];
      Node* &split_node = split_nodes[idx];
      if (is_bkt_nonempty_(split_bkt)) {
        split_bkt->next_ = split_node;
      } else {
        if (split_node != NULL) {
          new (&split_bkt->key_) Key(split_node->key_);
          new (&split_bkt->value_) Value(split_node->value_);
          split_bkt->next_ = split_node->next_;
          set_bkt_nonempty_(split_bkt, true);
          split_node->key_.~Key();
          split_node->value_.~Value();
          mem_mgr_.get_node_alloc().free(split_node);
          split_node = NULL;
        }
      }
    }
  }
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unload_expand_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt)
{
  if (NULL == src_bkt || NULL == dst_bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err bkt ptr", K(src_bkt), K(dst_bkt));
  } else {
    src_bkt->bkt_L_ = (uint8_t) (src_bkt->bkt_L_ + 1);
    dst_bkt->bkt_L_ = src_bkt->bkt_L_;
    set_bkt_lock_(dst_bkt, false);
    set_bkt_lock_(src_bkt, false);
  }
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::load_shrink_d_seg_bkts_(uint64_t L, uint64_t p, Bucket* &src_bkt,
    Bucket* &dst_bkt)
{
  if (NULL == dir_) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err dir", K(dir_));
  } else {
    Bucket *src_seg = dir_[seg_idx_((L0_bkt_n_ << L) + p)];
    Bucket *dst_seg = dir_[seg_idx_(p)];
    if (NULL == src_seg || NULL == dst_seg) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err seg", K(src_seg), K(dst_seg));
    } else {
      src_bkt = &src_seg[seg_bkt_idx_((L0_bkt_n_ << L) + p)];
      dst_bkt = &dst_seg[seg_bkt_idx_(p)];
      set_bkt_lock_(dst_bkt, true);
      set_bkt_lock_(src_bkt, true);
    }
  }
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::unite_shrink_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt)
{
  int ret = ES_SUCCESS;
  Node *node = NULL;
  if (NULL == src_bkt || NULL == dst_bkt) {
    LIB_LOG(ERROR, "err bkt ptr", K(src_bkt), K(dst_bkt));
  } else {
    if (is_bkt_nonempty_(src_bkt) && is_bkt_nonempty_(dst_bkt)
        && NULL == (node = static_cast<Node *>(mem_mgr_.get_node_alloc().alloc()))) {
      ret = ES_OUT_OF_MEM;
    } else if (is_bkt_nonempty_(src_bkt)) {
      if (!is_bkt_nonempty_(dst_bkt)) {
        new(&dst_bkt->key_) Key(src_bkt->key_);
        new(&dst_bkt->value_) Value(src_bkt->value_);
        dst_bkt->next_ = src_bkt->next_;
        set_bkt_nonempty_(dst_bkt, true);
      }
      else {
        new(&node->key_) Key(src_bkt->key_);
        new(&node->value_) Value(src_bkt->value_);
        node->next_ = src_bkt->next_;
        Node *tail = node;
        while (tail->next_ != NULL) { tail = tail->next_; }
        tail->next_ = dst_bkt->next_;
        dst_bkt->next_ = node;
      }
      src_bkt->key_.~Key();
      src_bkt->value_.~Value();
      src_bkt->next_ = NULL;
      set_bkt_nonempty_(src_bkt, false);
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unload_shrink_d_seg_bkts_(Bucket *src_bkt, Bucket *dst_bkt,
    bool succ)
{
  if (NULL == src_bkt || NULL == dst_bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err bkt ptr", K(src_bkt), K(dst_bkt));
  } else {
    if (succ) {
      dst_bkt->bkt_L_ = (uint8_t) (dst_bkt->bkt_L_ - 1);
      src_bkt->bkt_L_ = 0;
      set_bkt_active_(src_bkt, false);
    }
    set_bkt_lock_(src_bkt, false);
    set_bkt_lock_(dst_bkt, false);
  }
}

// Hazard pointer.
template <typename Key, typename Value, typename MemMgrTag>
int64_t ObLinearHashMap<Key, Value, MemMgrTag>::next_thread_id_ = 0;

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::init_haz_()
{
  seg_ref_ = mem_mgr_.get_external_ref();
}


template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket*
ObLinearHashMap<Key, Value, MemMgrTag>::load_haz_seg_(Bucket* &seg_ref)
{
  return (Bucket*)seg_ref_->acquire((void**)&seg_ref);
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unload_haz_seg_(Bucket* seg)
{
  seg_ref_->release((void*)seg);
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket*
ObLinearHashMap<Key, Value, MemMgrTag>::wait_evict_haz_seg_(Bucket* &to_evict_seg_ref)
{
  Bucket *to_evict_seg = to_evict_seg_ref;
  ATOMIC_STORE(&to_evict_seg_ref, NULL);
  seg_ref_->wait_quiescent((void*)to_evict_seg);
  return to_evict_seg;
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket**
ObLinearHashMap<Key, Value, MemMgrTag>::load_haz_dir_(Bucket** &dir_ref)
{
  return (Bucket**)seg_ref_->acquire((void**)&dir_ref);
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unload_haz_dir_(Bucket** dir)
{
  seg_ref_->release((void*)dir);
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket**
ObLinearHashMap<Key, Value, MemMgrTag>::wait_replace_haz_dir_(Bucket** &to_replace_dir_ref, Bucket **new_dir)
{
  Bucket **to_evict_dir = to_replace_dir_ref;
  // Replace dir ref, then no thread can load the old one anymore.
  ATOMIC_STORE(&to_replace_dir_ref, new_dir);
  // Wait till every thread unloads the dir.
  seg_ref_->wait_quiescent((void*)to_evict_dir);
  return to_evict_dir;
}

template <typename Key, typename Value, typename MemMgrTag>
int64_t ObLinearHashMap<Key, Value, MemMgrTag>::get_thread_id_()
{
  int64_t thread_id = get_itid();
  return thread_id;
}

// Bucket access.
template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket*
ObLinearHashMap<Key, Value, MemMgrTag>::load_access_bkt_(const Key &key, uint64_t &hash_v, Bucket*& bkt_seg)
{
  (void)hash_func_(key, hash_v); // Return hash value as seed of load factor ctrl.
  uint64_t L = 0;
  uint64_t p = 0;
  uint64_t bkt_L = 0;
  uint64_t bkt_idx = 0;
  Bucket *bkt = NULL;
  bool load = false;
  while (!load) {
    load_Lp_(L, p);
    bkt_L = L;
    if ((bkt_idx = hash_v % (L0_bkt_n_ << L)) < p) {
      bkt_L += 1;
      bkt_idx = hash_v % (L0_bkt_n_ << (L + 1));
    }
    Bucket **dir = load_haz_dir_(dir_);
    if (dir != NULL) {
      bkt_seg = load_haz_seg_(dir[seg_idx_(bkt_idx)]);
      // The dir may change between load dir & load seg, but nothing bad happens
      // because any further operations on the new dir would not happen before
      // the old one has been freed.
      if (bkt_seg == NULL) {
        // Shrank, retry.
      } else {
        bkt = &bkt_seg[seg_bkt_idx_(bkt_idx)];
        set_bkt_lock_(bkt, true);
        if (!is_bkt_active_(bkt)) {
          // Shrank, retry.
          set_bkt_lock_(bkt, false);
        } else {
          if ((uint64_t)bkt->bkt_L_ != bkt_L) {
          //if (is_bkt_expanded_(bkt_idx, L, p)) {
            // Expanded, retry.
            set_bkt_lock_(bkt, false);
          } else {
            // Finally got it.
            load = true;
          }
        }
      }
      if (!load) {
        unload_haz_seg_(bkt_seg);
      }
    } else { /* Fatal err.*/}
    unload_haz_dir_(dir);
  }
  return bkt;
}

template <typename Key, typename Value, typename MemMgrTag>
typename ObLinearHashMap<Key, Value, MemMgrTag>::Bucket*
ObLinearHashMap<Key, Value, MemMgrTag>::load_access_bkt_(uint64_t bkt_idx, Bucket*& bkt_seg)
{
  Bucket *bkt = NULL;
  Bucket **dir = load_haz_dir_(dir_);
  if (dir != NULL) {
    bkt_seg = load_haz_seg_(dir[seg_idx_(bkt_idx)]);
    // The dir is safe as mentioned in load_access_bkt_(const Key &key).
    if (seg_idx_(bkt_idx) >= dir_seg_n_lmt_) {
      // Access an invalid location of this dir. It seems:
      // 1. bkt_idx is invalid -- it's possible, and returning NULL is proper;
      // 2. bkt_idx is valid, but the loaded dir has been replaced by a new one
      //    with a new segment under construction -- it's impossible because:
      //    a. dir size only increase monotonously, it doesn't shrink when
      //       segment number decreases and ABA problem won't happen here,
      //       thus the bkt_idx is not generated by an old but valid L & p;
      //    b. the new segment is added to the new dir with L & p refreshed only
      //       after the old dir had been freed, so the L & p can't generate such
      //       a 'valid' bkt_idx.
      // If this situation happens, the bkt_idx is surely invalid, and the NULL
      // is a proper value to be returned.
    } else if (bkt_seg == NULL) {
      // Target bucket has shrank.
    } else {
      bkt = &bkt_seg[seg_bkt_idx_(bkt_idx)];
      set_bkt_lock_(bkt, true);
    }
    if (NULL == bkt) {
      unload_haz_seg_(bkt_seg);
    }
  } else { /* Fatal err.*/ }
  unload_haz_dir_(dir);
  return bkt;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unload_access_bkt_(Bucket *bkt, Bucket* bkt_seg)
{
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err bkt", K(bkt));
  } else {
    set_bkt_lock_(bkt, false);
    unload_haz_seg_(bkt_seg);
  }
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_clear_()
{
  int ret = OB_SUCCESS;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    if (dir_ != NULL) {
      uint64_t seg_idx = 0;
      while (seg_idx < dir_seg_n_lmt_ && dir_[seg_idx] != NULL) {
        uint64_t bkt_n = (seg_idx < m_seg_n_lmt_) ? m_seg_bkt_n_ : s_seg_bkt_n_;
        uint64_t clear_cnt = do_clear_seg_(dir_[seg_idx], bkt_n);
        add_cnt_(-1 * (int64_t)clear_cnt);
        seg_idx += 1;
      }
    } else { /*Fatal err.*/}
    while (ES_SUCCESS == shrink_()) { }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::do_clear_seg_(Bucket *seg, uint64_t bkt_n)
{
  uint64_t cnt = 0;
  if (NULL == seg) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err seg", K(seg));
  } else {
    for (uint64_t idx = 0; idx < bkt_n; ++idx) {
      Bucket *bkt = &seg[idx];
      if (is_bkt_active_(bkt)) {
        if (is_bkt_nonempty_(bkt)) {
          Node *iter = bkt->next_;
          while (iter != NULL) {
            Node *cur = iter;
            iter = iter->next_;
            cur->key_.~Key();
            cur->value_.~Value();
            mem_mgr_.get_node_alloc().free(cur);
            cur = NULL;
            cnt += 1;
          }
          bkt->next_ = NULL;
          bkt->key_.~Key();
          bkt->value_.~Value();
          set_bkt_nonempty_(bkt, false);
          cnt += 1;
        }
      }
    }
  }
  return cnt;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_insert_(const Key &key, const Value &value)
{
  int ret = OB_SUCCESS;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (!is_bkt_nonempty_(bkt)) {
      new (&bkt->key_) Key(key);
      new (&bkt->value_) Value(value);
      set_bkt_nonempty_(bkt, true);
    } else {
      if (equal_func_(bkt->key_, key)) {
        ret = OB_ENTRY_EXIST;
      } else {
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_SUCCESS == ret)) {
          if (equal_func_(iter->key_, key)) {
            ret = OB_ENTRY_EXIST;
            break;
          }
          iter = iter->next_;
        }
      }
      if (OB_SUCC(ret)) {
        Node *node = static_cast<Node*>(mem_mgr_.get_node_alloc().alloc());
        if (node == NULL) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          new (&node->key_) Key(key);
          new (&node->value_) Value(value);
          node->next_ = bkt->next_;
          bkt->next_ = node;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  if (OB_SUCC(ret)) {
    add_cnt_(1);
    load_factor_ctrl_(hash_v);
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_insert_or_update_(const Key &key, const Value &value)
{
  int ret = OB_SUCCESS;
  bool update = false;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (!is_bkt_nonempty_(bkt)) {
      new (&bkt->key_) Key(key);
      new (&bkt->value_) Value(value);
      set_bkt_nonempty_(bkt, true);
    } else {
      if (equal_func_(bkt->key_, key)) {
        bkt->value_ = value;
        update = true;
      } else {
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_SUCCESS == ret)) {
          if (equal_func_(iter->key_, key)) {
            iter->value_ = value;
            update = true;
            break;
          }
          iter = iter->next_;
        }

        if (!update && OB_SUCC(ret)) {
          Node *node = static_cast<Node*>(mem_mgr_.get_node_alloc().alloc());
          if (node == NULL) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            new (&node->key_) Key(key);
            new (&node->value_) Value(value);
            node->next_ = bkt->next_;
            bkt->next_ = node;
          }
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  if (OB_SUCC(ret)) {
    if (!update) {
      add_cnt_(1);
    }
    load_factor_ctrl_(hash_v);
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_get_(const Key& key, Value& value)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (is_bkt_nonempty_(bkt)) {
      if (equal_func_(bkt->key_, key)) {
        value = bkt->value_;
        ret = OB_SUCCESS;
      } else {
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_ENTRY_NOT_EXIST == ret)) {
          if (equal_func_(iter->key_, key)) {
            value = iter->value_;
            ret = OB_SUCCESS;
            break;
          }
          iter = iter->next_;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  load_factor_ctrl_(hash_v);
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_get_(const Key &key, Value &value, Key &copy_inner_key)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (is_bkt_nonempty_(bkt)) {
      if (equal_func_(bkt->key_, key)) {
        value = bkt->value_;
        copy_inner_key = bkt->key_;
        ret = OB_SUCCESS;
      } else {
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_ENTRY_NOT_EXIST == ret)) {
          if (equal_func_(iter->key_, key)) {
            value = iter->value_;
            copy_inner_key = iter->key_;
            ret = OB_SUCCESS;
            break;
          }
          iter = iter->next_;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  load_factor_ctrl_(hash_v);
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_erase_(const Key &key, Value *value)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (is_bkt_nonempty_(bkt)) {
      if (equal_func_(bkt->key_, key)) {
        bkt->key_.~Key();
        if (value != NULL) {
          *value = bkt->value_;
        }
        bkt->value_.~Value();
        Node *next = bkt->next_;
        if (next == NULL) {
          set_bkt_nonempty_(bkt, false);
        } else {
          new (&bkt->key_) Key(next->key_);
          new (&bkt->value_) Value(next->value_);
          bkt->next_ = next->next_;
          next->key_.~Key();
          next->value_.~Value();
          mem_mgr_.get_node_alloc().free(next);
          next = NULL;
        }
        ret = OB_SUCCESS;
      } else {
        void *prev = bkt;
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_ENTRY_NOT_EXIST == ret)) {
          if (equal_func_(iter->key_, key)) {
            iter->key_.~Key();
            if (value != NULL) {
              *value = iter->value_;
            }
            iter->value_.~Value();
            if (static_cast<Bucket*>(prev) == bkt) {
              bkt->next_ = iter->next_;
            } else {
              static_cast<Node*>(prev)->next_ = iter->next_;
            }
            mem_mgr_.get_node_alloc().free(iter);
            iter = NULL;
            ret = OB_SUCCESS;
            break;
          }
          prev = iter;
          iter = iter->next_;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  if (OB_SUCC(ret)) {
    add_cnt_(-1);
    load_factor_ctrl_(hash_v);
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_erase_if_(const Key &key, Function &fn)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket* bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (is_bkt_nonempty_(bkt)) {
      if (equal_func_(bkt->key_, key)) {
        if (fn(bkt->key_, bkt->value_)) {
          bkt->key_.~Key();
          bkt->value_.~Value();
          Node *next = bkt->next_;
          if (next == NULL) {
            set_bkt_nonempty_(bkt, false);
          } else {
            new (&bkt->key_) Key(next->key_);
            new (&bkt->value_) Value(next->value_);
            bkt->next_ = next->next_;
            next->key_.~Key();
            next->value_.~Value();
            mem_mgr_.get_node_alloc().free(next);
            next = NULL;
          }
          ret = OB_SUCCESS;
        } else {
          ret = OB_EAGAIN;
        }
      } else {
        void *prev = bkt;
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_ENTRY_NOT_EXIST == ret)) {
          if (equal_func_(iter->key_, key)) {
            if (fn(iter->key_, iter->value_)) {
              iter->key_.~Key();
              iter->value_.~Value();
              if (static_cast<Bucket*>(prev) == bkt) {
                bkt->next_ = iter->next_;
              } else {
                static_cast<Node*>(prev)->next_ = iter->next_;
              }
              mem_mgr_.get_node_alloc().free(iter);
              iter = NULL;
              ret = OB_SUCCESS;
            } else {
              ret = OB_EAGAIN;
            }
            break;
          }
          prev = iter;
          iter = iter->next_;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  if (OB_SUCC(ret)) {
    add_cnt_(-1);
    load_factor_ctrl_(hash_v);
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::do_cnt_() const
{
  int64_t ret = 0;
  ret = cnter_.count();
  return static_cast<uint64_t>(ret);
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::add_cnt_(int64_t cnt)
{
  int64_t thread_id = get_thread_id_();
  cnter_.add(cnt, thread_id);
}

// Iterator.
template <typename Key, typename Value, typename MemMgrTag>
int ObLinearHashMap<Key, Value, MemMgrTag>::blurred_iter_get_(uint64_t bkt_idx, uint64_t key_idx,
    Key &key, Value &value)
{
  int ret = OB_SUCCESS;
  Bucket **dir = load_haz_dir_(dir_);
  Bucket *bkt_seg = NULL;
  Bucket *bkt = NULL;
  if (dir != NULL) {
    bkt_seg = load_haz_seg_(dir[seg_idx_(bkt_idx)]);
    // The dir is safe as mentioned in load_access_bkt_(const Key &key).
    if (bkt_seg == NULL) {
      // bkt_idx became invalid, iter ends.
      ret = OB_ITER_END;
    } else {
      bkt = &bkt_seg[seg_bkt_idx_(bkt_idx)];
      set_bkt_lock_(bkt, true);
      if (!is_bkt_active_(bkt)) {
        // bkt_idx became invalid, iter ends.
        ret = OB_ITER_END;
      } else {
        ret = OB_EAGAIN; // if key_idx became invalid, should try the next bucket.
        if (is_bkt_nonempty_(bkt)) {
          if (key_idx == 0) {
            key = bkt->key_;
            value = bkt->value_;
            ret = OB_SUCCESS;
          } else {
            Node *cur = bkt->next_;
            uint64_t cur_idx = 0;
            while ((cur != NULL) && (OB_EAGAIN == ret)) {
              cur_idx += 1;
              if (cur_idx == key_idx) {
                key = cur->key_;
                value = cur->value_;
                ret = OB_SUCCESS;
              }
              cur = cur->next_;
            }
          }
        }
      }
      set_bkt_lock_(bkt, false);
    }
    unload_haz_seg_(bkt_seg);
  } else { /* Fatal err.*/ }
  unload_haz_dir_(dir);
  return ret;
}

// For each.
template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::init_foreach_()
{
  // Global lmt init as 0.
  foreach_L_lmt_ = 0;
  // lmt array init as all 0.
  for (uint64_t idx = 0; idx < LP_L_CNT; ++idx) {
    foreach_L_lmt_arr_[idx] = static_cast<int16_t>(0);
  }
}

template <typename Key, typename Value, typename MemMgrTag>
uint64_t ObLinearHashMap<Key, Value, MemMgrTag>::set_foreach_L_lmt_()
{
  // Set a proper limit on shrink, so hash map won't shrink to
  // a L that is smaller than the return L_lmt.
  // The return value is the limit set by this thread, user can
  // use it as a safe L.
  uint64_t L_lmt = 0;
  // Op under lock protection.
  es_lock_();
  uint64_t L = 0;
  uint64_t p = 0;
  load_Lp_(L, p);
  L_lmt = (L > 1) ? (L - 2) : 0;
  // Add a ref cnt on L_lmt. So other threads know that one
  // thread requres a safe L which is L_lmt.
  foreach_L_lmt_arr_[L_lmt] = static_cast<int16_t>(foreach_L_lmt_arr_[L_lmt] + 1);
  // Update global limit, which is the largest local limit.
  foreach_L_lmt_ = 0;
  for (uint64_t idx = 0; idx < LP_L_CNT; ++idx) {
    if (foreach_L_lmt_arr_[idx] > 0) {
      foreach_L_lmt_ = idx;
    }
  }
  es_unlock_();
  return L_lmt;
}

template <typename Key, typename Value, typename MemMgrTag>
void ObLinearHashMap<Key, Value, MemMgrTag>::unset_foreach_L_lmt_(const uint64_t L_lmt)
{
  // Decrease ref cnt on L_lmt.
  // Op under lock protection.
  es_lock_();
  foreach_L_lmt_arr_[L_lmt] = static_cast<int16_t>(foreach_L_lmt_arr_[L_lmt] - 1);
  // Update global limit, which is the largest local limit.
  foreach_L_lmt_ = 0;
  for (uint64_t idx = 0; idx < LP_L_CNT; ++idx) {
    if (foreach_L_lmt_arr_[idx] > 0) {
      foreach_L_lmt_ = idx;
    }
  }
  es_unlock_();
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function, typename DoOp>
bool ObLinearHashMap<Key, Value, MemMgrTag>::do_foreach_(Function &fn, DoOp &op)
{
  bool ret = true;
  uint64_t scan_L = set_foreach_L_lmt_();
  for (uint64_t bkt_idx = 0; ret == true && bkt_idx < (L0_bkt_n_ << scan_L); ++bkt_idx) {
    ret = do_foreach_scan_(bkt_idx, scan_L, fn, op);
  }
  unset_foreach_L_lmt_(scan_L);
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function, typename DoOp>
bool ObLinearHashMap<Key, Value, MemMgrTag>::do_foreach_scan_(uint64_t bkt_idx, uint64_t bkt_L, Function &fn, DoOp &op)
{
  bool ret = true;
  Bucket *bkt_seg = NULL;
  Bucket *bkt = load_access_bkt_(bkt_idx, bkt_seg);
  ret = op(*this, bkt, fn);
  for (uint64_t cur_L = bkt_L; ret == true && cur_L < bkt->bkt_L_; ++cur_L) {
    uint64_t cur_bkt_idx = bkt_idx + (L0_bkt_n_ << cur_L);
    ret = do_foreach_scan_(cur_bkt_idx, cur_L + 1, fn, op);
  }
  unload_access_bkt_(bkt, bkt_seg);
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
int ObLinearHashMap<Key, Value, MemMgrTag>::do_operate_(const Key &key, Function &fn)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash_v = 0;
  if (!init_) {
    ret = OB_NOT_INIT;
  } else {
    Bucket *bkt_seg = NULL;
    Bucket *bkt = load_access_bkt_(key, hash_v, bkt_seg);
    if (is_bkt_nonempty_(bkt)) {
      if (equal_func_(bkt->key_, key)) {
        if (fn(bkt->key_, bkt->value_)) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_EAGAIN;
        }
      } else {
        Node *iter = bkt->next_;
        while ((iter != NULL) && (OB_ENTRY_NOT_EXIST == ret)) {
          if (equal_func_(iter->key_, key)) {
            if (fn(iter->key_, iter->value_)) {
              ret = OB_SUCCESS;
            } else {
              ret = OB_EAGAIN;
            }
            break;
          }
          iter = iter->next_;
        }
      }
    }
    unload_access_bkt_(bkt, bkt_seg);
  }
  load_factor_ctrl_(hash_v);
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
bool ObLinearHashMap<Key, Value, MemMgrTag>::DoForeachOnBkt<Function>::operator()(ObLinearHashMap<Key, Value, MemMgrTag> &host, Bucket *bkt, Function &fn)
{
  bool ret = true;
  if (NULL == bkt) {
    LIB_LOG(ERROR, "err bkt", K(bkt));
  } else {
    if (host.is_bkt_nonempty_(bkt)) {
      ret = fn(const_cast<const Key &>(bkt->key_), bkt->value_);
      Node *cur = bkt->next_;
      while (ret == true && cur != NULL) {
        ret = fn(const_cast<const Key &>(cur->key_), cur->value_);
        cur = cur->next_;
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename MemMgrTag>
template <typename Function>
bool ObLinearHashMap<Key, Value, MemMgrTag>::DoRemoveIfOnBkt<Function>::operator()(ObLinearHashMap<Key, Value, MemMgrTag> &host, Bucket *bkt, Function &fn)
{
  if (NULL == bkt) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "err bkt", K(bkt));
  } else {
    Node *remove_list = NULL;
    Node **next_ptr = NULL;
    if (host.is_bkt_nonempty_(bkt)) {
      // Scan Nodes.
      next_ptr = &(bkt->next_);
      while (NULL != *next_ptr) {
        Node *cur = *next_ptr;
        if (fn(const_cast<const Key &>(cur->key_), cur->value_)) {
          *next_ptr = cur->next_;
          cur->next_ = remove_list;
          remove_list = cur;
          host.add_cnt_(-1);
        }
        else {
          next_ptr = &(cur->next_);
        }
      }
      // Scan Bucket.
      if (fn(const_cast<const Key &>(bkt->key_), bkt->value_)) {
        bkt->key_.~Key();
        bkt->value_.~Value();
        host.add_cnt_(-1);
        if (NULL != bkt->next_) {
          Node *cur = bkt->next_;
          new(&bkt->key_) Key(cur->key_);
          new(&bkt->value_) Value(cur->value_);
          bkt->next_ = cur->next_;
          cur->next_ = remove_list;
          remove_list = cur;
        }
        else {
          host.set_bkt_nonempty_(bkt, false);
        }
      }
      // Remove.
      while (NULL != remove_list) {
        Node *cur = remove_list;
        cur->key_.~Key();
        cur->value_.~Value();
        remove_list = cur->next_;
        host.mem_mgr_.get_node_alloc().free(cur);
        cur = NULL;
      }
    }
  }
  return true;
}

template <typename Key, typename Value, typename MemMgrTag>
ConstructGuard<Key, Value, MemMgrTag>::ConstructGuard()
{
  auto& t = ObLinearHashMap<Key, Value, MemMgrTag>::HashMapMemMgrCore::get_instance();
}

}
}

#endif
