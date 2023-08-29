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

#ifndef OCEANBASE_CACHE_OB_WORKING_SET_MGR_H_
#define OCEANBASE_CACHE_OB_WORKING_SET_MGR_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/queue/ob_link.h"
#include "lib/allocator/ob_retire_station.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kvcache_store.h"

namespace oceanbase
{
namespace common
{

typedef ObKVCacheInstKey WSListKey;
class ObWorkingSetMgr;
class ObWorkingSet;

struct WorkingSetMB
{
  WorkingSetMB() { reset(); }
  virtual ~WorkingSetMB() { reset(); }
  void reset();
  bool is_valid() const { return NULL != mb_handle_ && block_size_ >= 0; }
  bool is_mark_delete() const { return is_last_bit_set((uint64_t)ATOMIC_LOAD(&dlink_.next_)); }
  bool try_inc_ref() { return ATOMIC_BCAS(&is_refed_, false, true); }
  void dec_ref() { ATOMIC_BCAS(&is_refed_, true, false); }
  ObKVMemBlockHandle *get_mb_handle() { return mb_handle_; }
  int store(const ObIKVCacheKey &key, const ObIKVCacheValue &value, ObKVCachePair *&kvpair);
  int alloc(const int64_t key_size, const int64_t value_size, const int64_t align_kv_size, ObKVCachePair *&kvpair);
  void set_full(const double base_mb_score);
  TO_STRING_KV(KP_(mb_handle), K_(seq_num), K_(block_size));
  ObKVMemBlockHandle *mb_handle_;
  uint32_t seq_num_; // used to check whether mb_handle_ still store working set's data
  int64_t block_size_;
  bool is_refed_;
  ObDLink dlink_;
  ObLink retire_link_;
};

class ObWorkingSet : public ObDLinkBase<ObWorkingSet>,
    public ObIKVCacheStore<WorkingSetMB>
{
private:
  friend class ObWorkingSetMgr;

public:
  ObWorkingSet();
  virtual ~ObWorkingSet();

  int init(const WSListKey &ws_list_key,
           int64_t limit, ObKVMemBlockHandle *mb_handle,
           ObFixedQueue<WorkingSetMB> &ws_mb_pool,
           ObIMBHandleAllocator &mb_handle_allocator);
  void reset();
  bool is_valid() const { return inited_; }

  // implemnt functions of ObIKVCacheStore<WorkingSetMB>
  virtual bool add_handle_ref(WorkingSetMB *ws_mb);
  virtual uint32_t de_handle_ref(WorkingSetMB *ws_mb, const bool do_retire);
  virtual int alloc(ObKVCacheInst &inst, const enum ObKVCachePolicy policy,
      const int64_t block_size, WorkingSetMB *&ws_mb);
  virtual int free(WorkingSetMB *ws_mb);
  virtual WorkingSetMB *&get_curr_mb(ObKVCacheInst &inst, const enum ObKVCachePolicy policy);
  virtual bool mb_status_match(ObKVCacheInst &inst, const enum ObKVCachePolicy policy, WorkingSetMB *ws_mb);
  virtual int64_t get_block_size() const { return mb_handle_allocator_->get_block_size(); }

  const WSListKey &get_ws_list_key() const { return ws_list_key_; }
  int64_t get_cache_id() const { return ws_list_key_.cache_id_; }
  ObKVMemBlockHandle *get_curr_mb() { return cur_->mb_handle_; }
  int64_t get_limit() const { return limit_; }
  int64_t get_used() const { return used_; }
  TO_STRING_KV(K_(inited), K_(ws_list_key), K_(used), K_(limit));
private:
  int build_ws_mb(ObKVMemBlockHandle *mb_handle, WorkingSetMB *&ws_mb);
  int reuse_mb(ObKVCacheInst &inst, const int64_t block_size, ObKVMemBlockHandle *&mb_handle);
  bool try_reuse_mb(WorkingSetMB *ws_mb, ObKVMemBlockHandle *&mb_handle);
  void clear_mbs();

  static const int64_t RETIRE_LIMIT = 10;
  static QClock &get_qclock()
  {
    static QClock qclock;
    return qclock;
  }
  static RetireStation &get_retire_station()
  {
    static RetireStation retire_station(get_qclock(), RETIRE_LIMIT);
    return retire_station;
  }
  static int insert_ws_mb(ObDLink &head, WorkingSetMB *ws_mb);
  static int delete_ws_mb(ObFixedQueue<WorkingSetMB> &ws_mb_pool, WorkingSetMB *ws_mb);

  static void retire_ws_mbs(ObFixedQueue<WorkingSetMB> &ws_mb_pool, HazardList &retire_list);
  static void reuse_ws_mbs(ObFixedQueue<WorkingSetMB> &ws_mb_pool, HazardList &reclaim_list);
private:
  bool inited_;
  WSListKey ws_list_key_;
  int64_t used_;
  int64_t limit_;
  ObDLink head_;         // list of ws mb
  WorkingSetMB *cur_;    // current mb in use
  ObFixedQueue<WorkingSetMB> *ws_mb_pool_;
  ObIMBHandleAllocator *mb_handle_allocator_;
  lib::ObMutex mutex_;
};

class ObWorkingSetMgr
{
public:
  ObWorkingSetMgr();
  virtual ~ObWorkingSetMgr();

  int init(ObIMBHandleAllocator &mb_handle_allocator);
  void destroy();
  int create_working_set(const WSListKey &ws_list_key,
      const int64_t limit, ObWorkingSet *&working_set);
  int delete_working_set(ObWorkingSet *working_set);
private:
  static const int64_t MAX_WORKING_SET_COUNT = 10000;
  static const int64_t MAX_WORKING_SET_MB_COUNT = 10 * 1024 * 512;
  struct FreeArrayMB
  {
    FreeArrayMB() { reset(); }
    void reset()
    {
      mb_handle_ = NULL;
      seq_num_ = 0;
      in_array_ = false;
    }
    TO_STRING_KV(KP_(mb_handle), K_(seq_num), K_(in_array));
    ObKVMemBlockHandle *mb_handle_;
    uint32_t seq_num_;
    bool in_array_;    // indicate whether already in free array
  };
  struct WorkingSetList
  {
    static const int64_t FREE_ARRAY_SIZE = 10;
    WorkingSetList();
    ~WorkingSetList();

    int init(const WSListKey &key, ObIMBHandleAllocator &mb_handle_allocator);
    void reset();
    int add_ws(ObWorkingSet *ws);
    int del_ws(ObWorkingSet *ws);
    int pop_mb_handle(ObKVMemBlockHandle *&mb_handle);
    int push_mb_handle(ObKVMemBlockHandle *mb_handle);

    inline const WSListKey &get_key() const { return key_; }
    bool inited_;
    WSListKey key_;
    ObDList<ObWorkingSet> list_;
    // used to cache last mb of working set for reuse
    ObSEArray<FreeArrayMB, FREE_ARRAY_SIZE> free_array_mbs_;
    ObSEArray<FreeArrayMB *, FREE_ARRAY_SIZE> free_array_;
    int64_t limit_sum_;
    ObIMBHandleAllocator *mb_handle_allocator_;
  };
  typedef hash::ObHashMap<WSListKey, WorkingSetList *, hash::NoPthreadDefendMode> WSListMap;

private:
  int get_ws_list(const WSListKey &key, const bool create_not_exist, WorkingSetList *&list);
  int create_ws_list(const WSListKey &key, WorkingSetList *&list);
  int alloc_mb(WorkingSetList *list, ObKVMemBlockHandle *&mb_handle);
  int free_mb(WorkingSetList *list, ObKVMemBlockHandle *mb_handle);
private:
  bool inited_;
  DRWLock lock_;
  WSListMap ws_list_map_;
  ObFixedQueue<WorkingSetList> list_pool_;
  ObFixedQueue<ObWorkingSet> ws_pool_;
  ObFixedQueue<WorkingSetMB> ws_mb_pool_;
  ObIMBHandleAllocator *mb_handle_allocator_;
  ObArenaAllocator allocator_;
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_WORKING_SET_MGR_H_
