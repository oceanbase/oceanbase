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

#ifndef OCEANBASE_STORAGE_OB_TENANT_META_MEM_MGR_H_
#define OCEANBASE_STORAGE_OB_TENANT_META_MEM_MGR_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/queue/ob_link_queue.h"
#include "share/ob_ls_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_meta_pointer_map.h"
#include "storage/meta_mem/ob_meta_pointer.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "lib/signal/ob_signal_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace transaction
{
namespace tablelock
{
class ObLockMemtable;
}
}

namespace storage
{
class ObTenantMetaMemMgr;
class ObTxDataMemtable;
class ObTxCtxMemtable;
class ObLSMemberMemtable;
class ObTabletTxMultiSourceDataUnit;

struct ObTenantMetaMemStatus
{
public:
  ObTenantMetaMemStatus();
public:
  static const int64_t STRING_LEN = 127;
  char name_[STRING_LEN + 1];
  int64_t total_size_;
  int64_t used_size_;
  int64_t used_obj_cnt_;
  int64_t free_obj_cnt_;
  int64_t each_obj_size_;
  TO_STRING_KV(K_(name), K(total_size_), K(used_size_), K(used_obj_cnt_), K(free_obj_cnt_), K(each_obj_size_));
};

class ObTenantMetaMemMgr final
{
public:
  typedef ObMetaPointerHandle<ObTabletMapKey, ObTablet> ObTabletPointerHandle;
private:
  static const int64_t MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL = 10000;
  static const int64_t MIN_MODE_MAX_SSTABLE_CNT_IN_OBJ_POOL = 5 * MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL;
  static const int64_t MIN_MODE_MAX_MEMTABLE_CNT_IN_OBJ_POOL = 2 * MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL;

  static const int64_t MAX_TABLET_CNT_IN_OBJ_POOL = 50000;
  static const int64_t MAX_SSTABLE_CNT_IN_OBJ_POOL = 5 * MAX_TABLET_CNT_IN_OBJ_POOL;
  static const int64_t MAX_MEMTABLE_CNT_IN_OBJ_POOL = 2 * MAX_TABLET_CNT_IN_OBJ_POOL;
  static const int64_t MAX_TX_DATA_MEMTABLE_CNT_IN_OBJ_POOL = MAX_MEMSTORE_CNT * OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  static const int64_t MAX_TX_CTX_MEMTABLE_CNT_IN_OBJ_POOL = OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  static const int64_t MAX_LOCK_MEMTABLE_CNT_IN_OBJ_POOL = OB_MINI_MODE_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  static const int64_t MAX_DDL_KV_IN_OBJ_POOL = 5000;

  static int64_t get_default_tablet_pool_count()
  {
    return lib::is_mini_mode() ? MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL : MAX_TABLET_CNT_IN_OBJ_POOL;
  }
  static int64_t get_default_sstable_pool_count()
  {
    return lib::is_mini_mode() ? MIN_MODE_MAX_SSTABLE_CNT_IN_OBJ_POOL : MAX_SSTABLE_CNT_IN_OBJ_POOL;
  }
  static int64_t get_default_memtable_pool_count()
  {
    return lib::is_mini_mode() ? MIN_MODE_MAX_MEMTABLE_CNT_IN_OBJ_POOL : MAX_MEMTABLE_CNT_IN_OBJ_POOL;
  }

private:
  explicit ObTenantMetaMemMgr(const uint64_t tenant_id);
public:
  ~ObTenantMetaMemMgr();
  static int mtl_new(ObTenantMetaMemMgr *&meta_mem_mgr);
  static void mtl_destroy(ObTenantMetaMemMgr *&meta_mem_mgr);

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  // TIPS:
  //  - only for tx data table to find min log ts.
  int get_min_end_scn_for_ls(const share::ObLSID &ls_id, share::SCN &end_scn);

  // garbage collector for sstable and memtable.
  int push_table_into_gc_queue(ObITable *table, const ObITable::TableType table_type);
  int gc_tables_in_queue(bool &all_table_cleaned);
  void gc_sstable(blocksstable::ObSSTable *sstable);

  // sstable interface
  int acquire_sstable(ObTableHandleV2 &handle);
  int acquire_sstable(ObTableHandleV2 &handle, common::ObIAllocator &allocator);

  // ddl kv interface
  int acquire_ddl_kv(ObTableHandleV2 &handle);

  // memtable interfaces
  int acquire_memtable(ObTableHandleV2 &handle);
  int acquire_tx_data_memtable(ObTableHandleV2 &handle);
  int acquire_tx_ctx_memtable(ObTableHandleV2 &handle);
  int acquire_lock_memtable(ObTableHandleV2 &handle);

  // tablet interfaces
  int acquire_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      const bool only_acquire);
  int acquire_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObIAllocator &allocator,
      ObTabletHandle &tablet_handle,
      const bool only_acquire);
  int get_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  // NOTE: This interface return tablet handle, which couldn't be used by compare_and_swap_tablet.
  int get_tablet_with_allocator(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObIAllocator &allocator,
      ObTabletHandle &handle,
      const bool force_alloc_new = false);
  int get_tablet_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr);
  int has_tablet(const ObTabletMapKey &key, bool &is_exist);
  int del_tablet(const ObTabletMapKey &key);
  int check_all_meta_mem_released(
      ObLSService &ls_service,
      bool &is_released,
      const char *module);

  int compare_and_swap_tablet(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &new_addr,
      const ObTabletHandle &old_handle,
      ObTabletHandle &new_handle);
  // TIPS:
  //  - only compare and swap pure address, but no reset object.
  //  - only for checkpoint writer.
  int compare_and_swap_tablet_pure_address_without_object(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &old_addr,
      const ObMetaDiskAddr &new_addr);
  int try_wash_tablet();
  int get_meta_mem_status(common::ObIArray<ObTenantMetaMemStatus> &info) const;

  int get_tablet_pointer_tx_data(const ObTabletMapKey &key, ObTabletTxMultiSourceDataUnit &tx_data);
  int insert_pinned_tablet(const ObTabletMapKey &key);
  int erase_pinned_tablet(const ObTabletMapKey &key);
  int get_tablet_ddl_kv_mgr(const ObTabletMapKey &key, ObDDLKvMgrHandle &ddl_kv_mgr_handle);

  // TIPS:
  //  - only for allocating variable meta object in storage meta.
  OB_INLINE common::ObIAllocator &get_tenant_allocator() { return allocator_; }
  OB_INLINE bool is_used_obj_pool(common::ObIAllocator *allocator) const
  {
    return &allocator_ == allocator;
  }
  OB_INLINE int64_t get_total_tablet_cnt() const { return tablet_map_.count(); }

  TO_STRING_KV(K_(tenant_id), K_(is_inited));
private:
  int64_t cal_adaptive_bucket_num();

  typedef ObResourceValueStore<ObMetaPointer<ObTablet>> TabletValueStore;

  struct CandidateTabletInfo final
  {
  public:
    CandidateTabletInfo() :ls_id_(0), tablet_id_(0), wash_score_(INT64_MIN) {}
    ~CandidateTabletInfo() = default;
    bool is_valid() const { return ls_id_ > 0 && tablet_id_ > 0; }

    TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(wash_score));

    int64_t ls_id_; // to use ObBinaryHeap, the type here must is_trivially_copyable
    uint64_t tablet_id_; // to use ObBinaryHeap, the type here must is_trivially_copyable
    int64_t wash_score_;
  };
  struct InMemoryPinnedTabletInfo final
  {
  public:
    InMemoryPinnedTabletInfo() : key_(), addr_(), wash_score_(INT64_MIN) {}
    ~InMemoryPinnedTabletInfo() = default;
    bool operator < (const InMemoryPinnedTabletInfo &info)
    {
      return wash_score_ < info.wash_score_;
    }
    TO_STRING_KV(K_(key), K_(addr), K_(wash_score));
  public:
    ObTabletMapKey key_;
    ObMetaDiskAddr addr_;
    int64_t wash_score_;
  };
  class HeapCompare final
  {
  public:
    explicit HeapCompare(int &ret);
    ~HeapCompare() = default;
    bool operator() (const CandidateTabletInfo &left, const CandidateTabletInfo &right) const;
    int get_error_code() { return ret_; }
  private:
    int &ret_;
  };
  class TableGCItem : public common::ObLink
  {
  public:
    TableGCItem() : table_(nullptr), table_type_(ObITable::MAX_TABLE_TYPE) {}
    virtual ~TableGCItem() = default;

    TO_STRING_KV(KP_(table), K_(table_type));
  public:
    ObITable *table_;
    ObITable::TableType table_type_;
  };
  class TableGCTask : public common::ObTimerTask
  {
  public:
    explicit TableGCTask(ObTenantMetaMemMgr *t3m) : t3m_(t3m)
    {
      // TODO(handora.qc): enable it again after optimization
      disable_timeout_check();
    }
    virtual ~TableGCTask() = default;
    virtual void runTimerTask() override;
  private:
    ObTenantMetaMemMgr *t3m_;
  };
  class MinMinorSSTableGCTask : public common::ObTimerTask
  {
  public:
    explicit MinMinorSSTableGCTask(ObTenantMetaMemMgr *t3m) : t3m_(t3m) {}
    virtual ~MinMinorSSTableGCTask() = default;
    virtual void runTimerTask() override;
  private:
    ObTenantMetaMemMgr *t3m_;
  };
  class RefreshConfigTask : public common::ObTimerTask
  {
  public:
    RefreshConfigTask() = default;
    virtual ~RefreshConfigTask() = default;
    virtual void runTimerTask() override;
  };
  class MinMinorSSTableInfo final
  {
  public:
    MinMinorSSTableInfo() : ls_id_(), table_key_(), sstable_handle_() {}
    MinMinorSSTableInfo(
        const share::ObLSID &ls_id,
        const ObITable::TableKey &table_key,
        const ObTableHandleV2 &sstable_handle);
    ~MinMinorSSTableInfo();
    OB_INLINE bool is_valid() const
    {
      return ls_id_.is_valid()
          && table_key_.is_valid()
          && sstable_handle_.is_valid()
          && sstable_handle_.get_table()->is_minor_sstable();
    }
    OB_INLINE bool operator ==(const MinMinorSSTableInfo &other) const
    {
      return sstable_handle_.get_table() == other.sstable_handle_.get_table();
    }
    OB_INLINE bool operator !=(const MinMinorSSTableInfo &other) const { return !(*this == other); }
    OB_INLINE uint64_t hash() const
    {
      const ObITable *table = sstable_handle_.get_table();
      return common::murmurhash(&table, sizeof(table), 0);
    }
    OB_INLINE int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    TO_STRING_KV(K_(ls_id), K_(table_key), K_(sstable_handle));
  public:
    share::ObLSID ls_id_;
    ObITable::TableKey table_key_;
    ObTableHandleV2 sstable_handle_;
  };

private:
  friend class ObT3mTabletMapIterator;
  friend class GetWashTabletCandidate;
  friend class TableGCTask;
  friend class ObTabletPointer;
  static const int64_t DEFAULT_BUCKET_NUM = 10243L;
  static const int64_t TOTAL_LIMIT = 15 * 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t TABLE_GC_INTERVAL_US = 20 * 1000L; // 20ms
  static const int64_t MIN_MINOR_SSTABLE_GC_INTERVAL_US = 1 * 1000 * 1000L; // 1s
  static const int64_t REFRESH_CONFIG_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  static const int64_t ONE_ROUND_RECYCLE_COUNT_THRESHOLD = 20000L;
  static const int64_t BATCH_MEMTABLE_GC_THRESHOLD = 100L;
  static const int64_t DEFAULT_TABLET_WASH_HEAP_COUNT = 16;
  static const int64_t DEFAULT_MINOR_SSTABLE_SET_COUNT = 49999;
  static const int64_t SSTABLE_GC_MAX_TIME = 500; // 500us
  typedef common::ObBinaryHeap<CandidateTabletInfo, HeapCompare, DEFAULT_TABLET_WASH_HEAP_COUNT> Heap;
  typedef common::hash::ObHashSet<MinMinorSSTableInfo, common::hash::NoPthreadDefendMode> SSTableSet;
  typedef common::hash::ObHashSet<ObTabletMapKey, hash::NoPthreadDefendMode> PinnedTabletSet;

  class GetWashTabletCandidate final
  {
  public:
    GetWashTabletCandidate(
        Heap &heap,
        common::ObIArray<InMemoryPinnedTabletInfo> &mem_addr_tablets_,
        ObTenantMetaMemMgr &t3m,
        common::ObIAllocator &allocator);
    ~GetWashTabletCandidate() = default;
    int operator()(common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> &entry);

    TO_STRING_KV(K_(none_addr_tablet_cnt),
                 K_(mem_addr_tablet_cnt),
                 K_(in_memory_tablet_cnt),
                 K_(inner_tablet_cnt),
                 K_(candidate_tablet_cnt));
  private:
    Heap &heap_;
    common::ObIArray<InMemoryPinnedTabletInfo> &mem_addr_tablets_;
    ObTenantMetaMemMgr &t3m_;
    common::ObIAllocator &allocator_;
    int64_t none_addr_tablet_cnt_; // tablet whose meta disk addr is NONE, should not be washed
    int64_t mem_addr_tablet_cnt_; // tablet whose meta disk addr is MEM, should not be washed
    int64_t in_memory_tablet_cnt_; // tablet which exists in memory, may be washing candidate
    int64_t inner_tablet_cnt_;
    int64_t candidate_tablet_cnt_;
  };

  class TenantMetaAllocator : public common::ObFIFOAllocator
  {
  public:
    TenantMetaAllocator(const uint64_t tenant_id, TryWashTabletFunc &wash_func)
    : common::ObFIFOAllocator(tenant_id), wash_func_(wash_func) {};
    virtual ~TenantMetaAllocator() = default;
    TO_STRING_KV("used", used(), "total", total());
  protected:
    virtual void *alloc_align(const int64_t size, const int64_t align) override;
  private:
    TryWashTabletFunc &wash_func_;
  };

private:
  int acquire_tablet_ddl_kv_mgr(ObDDLKvMgrHandle &handle);
  int acquire_tablet_memtable_mgr(ObMemtableMgrHandle &handle);
  int create_tablet(const ObTabletMapKey &key, ObLSHandle &ls_handle, ObTabletHandle &tablet_handle);
  int gc_min_minor_sstable_in_set();
  int record_min_minor_sstable(const share::ObLSID &ls_id, const ObTableHandleV2 &table_handle);
  int try_wash_tablet(const int64_t expect_wash_cnt);
  int do_wash_candidate_tablet(
      const int64_t expect_wash_cnt,
      Heap &heap,
      int64_t &wash_inner_cnt,
      int64_t &wash_user_cnt);
  int do_wash_mem_addr_tablet(
      const int64_t expect_wash_cnt,
      const common::ObIArray<InMemoryPinnedTabletInfo> &mem_addr_tablet_info,
      int64_t &wash_inner_cnt,
      int64_t &wash_user_cnt,
      int64_t &wash_mem_addr_cnt);
  int write_slog_and_wash_tablet(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &old_addr,
      bool &is_wash);
  int64_t calc_wash_tablet_cnt() const;
  void dump_tablet();
  void dump_pinned_tablet();
  void dump_ls(ObLSService &ls_service) const;
  void init_pool_arr();
  void release_memtable(memtable::ObMemtable *memtable);
  void release_sstable(blocksstable::ObSSTable *sstable);
  void release_ddl_kv(ObDDLKV *ddl_kv);
  void release_tablet(ObTablet *tablet);
  void release_tablet_ddl_kv_mgr(ObTabletDDLKvMgr *ddl_kv_mgr);
  void release_tablet_memtable_mgr(ObTabletMemtableMgr *memtable_mgr);
  void release_tx_data_memtable_(ObTxDataMemtable *memtable);
  void release_tx_ctx_memtable_(ObTxCtxMemtable *memtable);
  void release_lock_memtable_(transaction::tablelock::ObLockMemtable *memtable);

  template <typename T>
  int get_obj_pool_info(
      const ObTenantMetaObjPool<T> &obj_pool,
      const char *name,
      common::ObIArray<ObTenantMetaMemStatus> &info) const;
  int get_allocator_info(common::ObIArray<ObTenantMetaMemStatus> &info) const;
  int exist_pinned_tablet(const ObTabletMapKey &key);
  int push_memtable_into_gc_map_(memtable::ObMemtable *memtable);
  void batch_gc_memtable_();

private:
  int cmp_ret_;
  HeapCompare compare_;
  common::SpinRWLock wash_lock_;
  TryWashTabletFunc wash_func_;
  const uint64_t tenant_id_;
  ObBucketLock bucket_lock_;
  TenantMetaAllocator allocator_;
  ObMetaPointerMap<ObTabletMapKey, ObTablet> tablet_map_;
  int tg_id_;
  TableGCTask table_gc_task_;
  MinMinorSSTableGCTask min_minor_sstable_gc_task_;
  RefreshConfigTask refresh_config_task_;
  common::ObLinkQueue free_tables_queue_;
  common::ObSpinLock gc_queue_lock_;
  SSTableSet last_min_minor_sstable_set_;
  common::SpinRWLock sstable_set_lock_;
  ObBucketLock pin_set_lock_;
  PinnedTabletSet pinned_tablet_set_; // tablets which are in multi source data transaction procedure

  common::hash::ObHashMap<share::ObLSID, memtable::ObMemtableSet*> gc_memtable_map_;

  ObTenantMetaObjPool<memtable::ObMemtable> memtable_pool_;
  ObTenantMetaObjPool<blocksstable::ObSSTable> sstable_pool_;
  ObTenantMetaObjPool<ObDDLKV> ddl_kv_pool_;
  ObTenantMetaObjPool<ObTablet> tablet_pool_;
  ObTenantMetaObjPool<ObTabletDDLKvMgr> tablet_ddl_kv_mgr_pool_;
  ObTenantMetaObjPool<ObTabletMemtableMgr> tablet_memtable_mgr_pool_;
  ObTenantMetaObjPool<ObTxDataMemtable> tx_data_memtable_pool_;
  ObTenantMetaObjPool<ObTxCtxMemtable> tx_ctx_memtable_pool_;
  ObTenantMetaObjPool<transaction::tablelock::ObLockMemtable> lock_memtable_pool_;
  ObITenantMetaObjPool *pool_arr_[ObITable::TableType::MAX_TABLE_TYPE];

  bool is_inited_;
};

class ObITenantTabletIterator
{
public:
  ObITenantTabletIterator() = default;
  virtual ~ObITenantTabletIterator() = default;
  virtual int get_next_tablet(ObTabletHandle &handle) = 0;
};

class ObITenantTabletPointerIterator
{
public:
  typedef ObMetaPointerHandle<ObTabletMapKey, ObTablet> ObTabletPointerHandle;
public:
  ObITenantTabletPointerIterator() = default;
  virtual ~ObITenantTabletPointerIterator() = default;
  virtual int get_next_tablet_pointer(
      ObTabletMapKey &key,
      ObTabletPointerHandle &pointer_handle,
      ObTabletHandle &tablet_handle) = 0;
};

class ObT3mTabletMapIterator
{
public:
  explicit ObT3mTabletMapIterator(ObTenantMetaMemMgr &t3m);
  ObT3mTabletMapIterator(ObTenantMetaMemMgr &t3m, common::ObIAllocator &allocator);
  virtual ~ObT3mTabletMapIterator();
  void reset();
protected:
  typedef ObResourceValueStore<ObMetaPointer<ObTablet>> TabletValueStore;
  typedef ObMetaPointerMap<ObTabletMapKey, ObTablet> TabletMap;
  typedef common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> TabletPair;

  int fetch_tablet_item();
  static bool ignore_err_code(const int ret) { return OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret; }
private:
  class FetchTabletItemOp final
  {
  public:
    FetchTabletItemOp(TabletMap &tablet_map, common::ObIArray<ObTabletMapKey> &items);
    ~FetchTabletItemOp() = default;
    int operator()(TabletPair &pair);
  private:
    TabletMap &tablet_map_;
    common::ObIArray<ObTabletMapKey> &items_;
  };
protected:
  static const int64_t DEFAULT_TABLET_ITEM_CNT = 8;

  TabletMap &tablet_map_;
  common::ObIAllocator &allocator_;
  common::ObSEArray<ObTabletMapKey, DEFAULT_TABLET_ITEM_CNT> tablet_items_;
  int64_t idx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObT3mTabletMapIterator);
};

class ObTenantTabletIterator : public ObT3mTabletMapIterator,
                               public ObITenantTabletIterator
{
public:
  explicit ObTenantTabletIterator(ObTenantMetaMemMgr &t3m);
  ObTenantTabletIterator(ObTenantMetaMemMgr &t3m, common::ObIAllocator &allocator);
  virtual ~ObTenantTabletIterator() = default;
  virtual int get_next_tablet(ObTabletHandle &handle) override;

private:
  const bool is_used_obj_pool_;
};

class ObTenantInMemoryTabletIterator : public ObT3mTabletMapIterator,
                                       public ObITenantTabletIterator
{
public:
  explicit ObTenantInMemoryTabletIterator(ObTenantMetaMemMgr &t3m);
  virtual ~ObTenantInMemoryTabletIterator() = default;
  virtual int get_next_tablet(ObTabletHandle &handle) override;
};

class ObTenantTabletPtrWithInMemObjIterator : public ObT3mTabletMapIterator,
                                              public ObITenantTabletPointerIterator
{
public:
  explicit ObTenantTabletPtrWithInMemObjIterator(ObTenantMetaMemMgr &t3m);
  virtual ~ObTenantTabletPtrWithInMemObjIterator() = default;
  virtual int get_next_tablet_pointer(
      ObTabletMapKey &key,
      ObTabletPointerHandle &pointer_handle,
      ObTabletHandle &in_memory_tablet_handle) override;
};

template <typename T>
int ObTenantMetaMemMgr::get_obj_pool_info(
    const ObTenantMetaObjPool<T> &obj_pool,
    const char *name,
    common::ObIArray<ObTenantMetaMemStatus> &info) const
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemStatus mem_status;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(name));
  } else if (OB_UNLIKELY(STRLEN(name) >= ObTenantMetaMemStatus::STRING_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "string length exceeds max buffer length", K(ret), K(name));
  } else {
    STRNCPY(mem_status.name_, name, mem_status.STRING_LEN);
    mem_status.each_obj_size_ = obj_pool.get_obj_size();
    mem_status.free_obj_cnt_ = obj_pool.get_free_obj_cnt();
    mem_status.used_obj_cnt_ = obj_pool.get_used_obj_cnt();
    mem_status.total_size_ = obj_pool.total();
    mem_status.used_size_ = obj_pool.used();
    if (OB_FAIL(info.push_back(mem_status))) {
      STORAGE_LOG(WARN, "fail to push mem status to info array", K(ret), K(mem_status));
    }
  }

  return ret;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TENANT_META_MEM_MGR_H_ */
