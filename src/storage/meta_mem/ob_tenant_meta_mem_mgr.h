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
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_pointer_map.h"
#include "storage/meta_mem/ob_tablet_pointer_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"
#include "storage/meta_mem/ob_tablet_leak_checker.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_full_tablet_creator.h"
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
class ObTabletCreateDeleteMdsUserData;

enum class ObTabletPoolType : uint8_t
{
  TP_NORMAL = 0,
  TP_LARGE  = 1,
  TP_MAX
};

struct ObTenantMetaMemStatus final
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

struct ObTabletBufferInfo final
{
public:
  ObTabletBufferInfo()
    : ls_id_(), tablet_id_(),
      tablet_buffer_ptr_(nullptr), tablet_(nullptr),
      pool_type_(), in_map_(false), last_access_time_(-1)
  {
  }
  ~ObTabletBufferInfo()
  {
    reset();
  }

  void reset();
  int fill_info(const ObTabletPoolType &pool_type, ObMetaObjBufferNode *node);
  TO_STRING_KV(K_(ls_id), K_(tablet_id), KP_(tablet_buffer_ptr), KP_(tablet), K_(pool_type), K_(in_map), K_(last_access_time));

public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  char *tablet_buffer_ptr_;
  ObTablet *tablet_;
  ObTabletPoolType pool_type_;
  bool in_map_;
  int64_t last_access_time_;
};

class ObTenantMetaMemMgr final
{
public:
  static const int64_t THE_SIZE_OF_HEADERS = sizeof(ObFIFOAllocator::NormalPageHeader) + sizeof(ObMetaObjBufferNode);
  static const int64_t NORMAL_TABLET_POOL_SIZE = (ABLOCK_SIZE - ABLOCK_HEADER_SIZE) / 2 - AOBJECT_META_SIZE - AOBJECT_EXTRA_INFO_SIZE - THE_SIZE_OF_HEADERS; // 3824B
  static const int64_t LARGE_TABLET_POOL_SIZE = 64 * 1024L - THE_SIZE_OF_HEADERS; // 65,480B

  static const int64_t MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL = 10000;
  static const int64_t MIN_MODE_MAX_MEMTABLE_CNT_IN_OBJ_POOL = 2 * MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL;

  static const int64_t MAX_TABLET_CNT_IN_OBJ_POOL = 50000;
  static const int64_t MAX_MEMTABLE_CNT_IN_OBJ_POOL = 2 * MAX_TABLET_CNT_IN_OBJ_POOL;
  static const int64_t MAX_TX_DATA_MEMTABLE_CNT_IN_OBJ_POOL = MAX_MEMSTORE_CNT * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT;
  static const int64_t MAX_TX_CTX_MEMTABLE_CNT_IN_OBJ_POOL = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT;
  static const int64_t MAX_LOCK_MEMTABLE_CNT_IN_OBJ_POOL = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT;
  static const int64_t MAX_DDL_KV_IN_OBJ_POOL = 5000;
  static const int64_t TABLET_TRANSFORM_INTERVAL_US = 2 * 1000 * 1000L; // 2s

  static int64_t get_default_tablet_pool_count()
  {
    const bool is_small_tenant = MTL_CTX() != nullptr && MTL_MEM_SIZE() <= 2 * 1024 * 1024; // memory is less than 2GB;
    return lib::is_mini_mode() || is_small_tenant ? MIN_MODE_MAX_TABLET_CNT_IN_OBJ_POOL : MAX_TABLET_CNT_IN_OBJ_POOL;
  }
  static int64_t get_default_normal_tablet_pool_count()
  {
    return static_cast<int64_t>(get_default_tablet_pool_count() * 0.96);
  }
  static int64_t get_default_large_tablet_pool_count()
  {
    return get_default_tablet_pool_count() - get_default_normal_tablet_pool_count();
  }
  static int64_t get_default_memtable_pool_count()
  {
    return lib::is_mini_mode() ? MIN_MODE_MAX_MEMTABLE_CNT_IN_OBJ_POOL : MAX_MEMTABLE_CNT_IN_OBJ_POOL;
  }

  static int register_into_tb_map(const char *file, const int line, const char *func, int32_t &index);

  static int get_tablet_pool_type(const int64_t tablet_size, ObTabletPoolType &type)
  {
    int ret = OB_SUCCESS;
    if (tablet_size <= NORMAL_TABLET_POOL_SIZE) {
      type = ObTabletPoolType::TP_NORMAL;
    } else if (tablet_size <= LARGE_TABLET_POOL_SIZE) {
      type = ObTabletPoolType::TP_LARGE;
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "tablet size is too large", K(ret), K(tablet_size));
    }
    return ret;
  }

private:
  static const int64_t DEFAULT_TABLET_CNT_PER_GB = 20000;

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
  int print_old_chain(
      const ObTabletMapKey &key,
      const ObTabletPointer &tablet_ptr,
      const int64_t buf_len,
      char *buf);
  // TIPS:
  //  - only for tx data table to find min log ts.
  int get_min_end_scn_for_ls(
      const ObTabletMapKey &key,
      const share::SCN &ls_checkpoint,
      share::SCN &min_end_scn_from_latest,
      share::SCN &min_end_scn_from_old);
  int get_min_mds_ckpt_scn(const ObTabletMapKey &key, share::SCN &scn);

  // garbage collector for sstable and memtable.
  int push_table_into_gc_queue(ObITable *table, const ObITable::TableType table_type);
  int gc_tables_in_queue(bool &all_table_cleaned);
  int push_tablet_into_gc_queue(ObTablet *tablet); // add tablet into gc queue
  int gc_tablets_in_queue(bool &all_tablet_cleaned); // trigger to gc tablets

  // ddl kv interface
  int acquire_ddl_kv(ObDDLKVHandle &handle);
  int acquire_direct_load_memtable(ObTableHandleV2 &handle);
  int acquire_data_memtable(ObTableHandleV2 &handle);
  int acquire_tx_data_memtable(ObTableHandleV2 &handle);
  int acquire_tx_ctx_memtable(ObTableHandleV2 &handle);
  int acquire_lock_memtable(ObTableHandleV2 &handle);
  void release_ddl_kv(ObDDLKV *ddl_kv);

  // tablet create and acquire interfaces
  //  - create_xx_tablet() is used for the first time to construct a tablet object and create a
  //    tablet pointer.
  //  - acquire_xx_tablet() is used for non-first time, and only constructs a tablet object.
  int create_msd_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  int create_tmp_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  int acquire_tmp_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle);
  int acquire_tablet_from_pool(
      const ObTabletPoolType &type,
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      ObTabletHandle &tablet_handle);
  int get_tablet(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  int get_tablet_with_filter(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObITabletFilterOp &op,
    ObTabletHandle &handle);
  int build_tablet_handle_for_mds_scan(
      ObTablet *tablet,
      ObTabletHandle &tablet_handle);

  // NOTE: This interface return tablet handle, which couldn't be used by compare_and_swap_tablet.
  int get_tablet_with_allocator(
      const WashTabletPriority &priority,
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle,
      const bool force_alloc_new = false);
  int get_tablet_buffer_infos(ObIArray<ObTabletBufferInfo> &buffer_infos);
  int get_tablet_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr);
  int has_tablet(const ObTabletMapKey &key, bool &is_exist);
  int del_tablet(const ObTabletMapKey &key);
  int check_all_meta_mem_released(bool &is_released, const char *module);
  // only used for replay and compat, others mustn't call this func
  int compare_and_swap_tablet(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &old_addr,
      const ObMetaDiskAddr &new_addr,
      const ObTabletPoolType &pool_type = ObTabletPoolType::TP_MAX,
      const bool set_pool = false /* whether to set tablet pool */);
  int compare_and_swap_tablet(
      const ObTabletMapKey &key,
      const ObTabletHandle &old_handle,
      const ObTabletHandle &new_handle,
      const ObUpdateTabletPointerParam &update_tablet_pointer_param);
  int update_tablet_buffer_header(ObTablet *old_obj, ObTablet *new_obj);
  int try_wash_tablet(const std::type_info &type_info, void *&obj);
  int get_meta_mem_status(common::ObIArray<ObTenantMetaMemStatus> &info) const;

  int get_tablet_pointer_initial_state(const ObTabletMapKey &key, bool &initial_state);
  int get_tablet_ddl_kv_mgr(const ObTabletMapKey &key, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  ObFullTabletCreator &get_mstx_tablet_creator() { return full_tablet_creator_; }
  common::ObIAllocator &get_meta_cache_io_allocator() { return meta_cache_io_allocator_; }
  OB_INLINE int64_t get_total_tablet_cnt() const { return tablet_map_.count(); }

  int has_meta_wait_gc(bool &is_wait);
  int dump_tablet_info();
  int release_memtable_and_mds_table_for_ls_offline(const ObTabletMapKey &key);
  OB_INLINE share::ObIResourceLimitCalculatorHandler * get_t3m_limit_calculator() { return &t3m_limit_calculator_; }

  TO_STRING_KV(K_(tenant_id), K_(is_inited), "tablet count", tablet_map_.count());

  int inc_ref_in_leak_checker(const int32_t index);
  int dec_ref_in_leak_checker(const int32_t index);
public:
  class ObT3MResourceLimitCalculatorHandler final : public share::ObIResourceLimitCalculatorHandler
  {
  public:
    explicit ObT3MResourceLimitCalculatorHandler(ObTenantMetaMemMgr &t3m) : t3m_(t3m) {}
    int get_current_info(share::ObResourceInfo &info) override;
    int get_resource_constraint_value(share::ObResoureConstraintValue &constraint_value) override;
    int cal_min_phy_resource_needed(share::ObMinPhyResourceResult &min_phy_res) override;
    int cal_min_phy_resource_needed(const int64_t num, share::ObMinPhyResourceResult &min_phy_res) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObT3MResourceLimitCalculatorHandler);
  private:
    ObTenantMetaMemMgr &t3m_;
  };
private:
  int fill_buffer_infos(
      const ObTabletPoolType pool_type,
      ObMetaObjBufferNode *tablet_buffer_node,
      ObIArray<ObTabletBufferInfo> &buffer_infos);
  int64_t cal_adaptive_bucket_num();
  int inner_push_tablet_into_gc_queue(ObTablet *tablet);
  int get_min_end_scn_from_single_tablet(ObTablet *tablet,
                                         const bool is_old,
                                         const share::SCN &ls_checkpoint,
                                         share::SCN &min_end_scn);
  int fetch_tenant_config();

private:
  typedef ObResourceValueStore<ObTabletPointer> TabletValueStore;
  typedef ObMetaObjBuffer<ObTablet, NORMAL_TABLET_POOL_SIZE> ObNormalTabletBuffer;
  typedef ObMetaObjBuffer<ObTablet, LARGE_TABLET_POOL_SIZE> ObLargeTabletBuffer;
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
  class TabletGCTask : public common::ObTimerTask
  {
  public:
    explicit TabletGCTask(ObTenantMetaMemMgr *t3m) : t3m_(t3m) {}
    virtual ~TabletGCTask() = default;
    virtual void runTimerTask() override;
  private:
    ObTenantMetaMemMgr *t3m_;
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
  class TabletMapDumpOperator
  {
  public:
    int operator()(common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> &entry);
  };
  class TabletGCQueue final
  {
  public:
    TabletGCQueue()
      : gc_head_(nullptr),
        gc_tail_(nullptr),
        tablet_count_(0),
        queue_lock_(common::ObLatchIds::TENANT_META_MEM_MGR_LOCK)
    {}
    ~TabletGCQueue() = default;
    OB_INLINE int64_t count() const { return ATOMIC_LOAD(&tablet_count_); }
    OB_INLINE bool is_empty() const { return 0 == count(); }
    int push(ObTablet *tablet);
    ObTablet *pop();
  private:
    ObTablet *gc_head_;
    ObTablet *gc_tail_;
    int64_t tablet_count_;
    common::SpinRWLock queue_lock_;
    DISALLOW_COPY_AND_ASSIGN(TabletGCQueue);
  };
private:
  friend class ObT3mTabletMapIterator;
  friend class TableGCTask;
  friend class ObTabletPointer;
  static const int64_t DEFAULT_BUCKET_NUM = 10243L;
  static const int64_t TOTAL_LIMIT = 15 * 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t TABLE_GC_INTERVAL_US = 20 * 1000L; // 20ms
  static const int64_t REFRESH_CONFIG_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  static const int64_t ONE_ROUND_RECYCLE_COUNT_THRESHOLD = 20000L;
  static const int64_t ONE_ROUND_TABLET_GC_COUNT_THRESHOLD = 200L;
  static const int64_t BATCH_MEMTABLE_GC_THRESHOLD = 100L;
  static const int64_t DEFAULT_TABLET_WASH_HEAP_COUNT = 16;
  static const int64_t DEFAULT_MINOR_SSTABLE_SET_COUNT = 49999;
  static const int64_t SSTABLE_GC_MAX_TIME = 500; // 500us
  static const int64_t LEAK_CHECKER_CONFIG_REFRESH_TIMEOUT = 10000000 * 10; // 10s
  typedef common::ObBinaryHeap<CandidateTabletInfo, HeapCompare, DEFAULT_TABLET_WASH_HEAP_COUNT> Heap;
  typedef common::ObDList<ObMetaObjBufferNode> TabletBufferList;
  typedef common::hash::ObHashSet<MinMinorSSTableInfo, common::hash::NoPthreadDefendMode> SSTableSet;
  typedef common::hash::ObHashSet<ObTabletMapKey, hash::NoPthreadDefendMode> PinnedTabletSet;

private:
  int acquire_tablet(const ObTabletPoolType type, ObTabletHandle &tablet_handle);
  int acquire_tablet(ObITenantMetaObjPool *pool, ObTablet *&tablet);
  int acquire_tablet_ddl_kv_mgr(ObDDLKvMgrHandle &handle);
  int create_tablet(const ObTabletMapKey &key, ObLSHandle &ls_handle, ObTabletHandle &tablet_handle);
  int do_wash_candidate_tablet(
      const CandidateTabletInfo &info,
      ObTabletHandle &tablet_handle,
      void *& free_obj);
  int try_wash_tablet_from_gc_queue(
      const int64_t buf_len,
      TabletBufferList &header,
      void *&free_obj);
  void init_pool_arr();
  void *release_tablet(ObTablet *tablet, const bool return_buf_ptr_after_release);
  void release_tablet_from_pool(ObTablet *tablet, const bool give_back_tablet_into_pool);
  void release_memtable(memtable::ObMemtable *memtable);
  void release_tablet_ddl_kv_mgr(ObTabletDDLKvMgr *ddl_kv_mgr);
  void release_tx_data_memtable_(ObTxDataMemtable *memtable);
  void release_tx_ctx_memtable_(ObTxCtxMemtable *memtable);
  void release_lock_memtable_(transaction::tablelock::ObLockMemtable *memtable);
  void mark_mds_table_deleted_(const ObTabletMapKey &key);

  template <typename T>
  int get_obj_pool_info(
      const ObTenantMetaObjPool<T> &obj_pool,
      const char *name,
      common::ObIArray<ObTenantMetaMemStatus> &info) const;
  int get_full_tablets_info(
      const ObFullTabletCreator &creator,
      const char *name,
      common::ObIArray<ObTenantMetaMemStatus> &info) const;
  int get_wash_tablet_candidate(const std::type_info &type_info, CandidateTabletInfo &info);
  int push_memtable_into_gc_map_(memtable::ObMemtable *memtable);
  void batch_gc_memtable_();
  void batch_destroy_memtable_(memtable::ObMemtableSet *memtable_set);
  bool is_tablet_handle_leak_checker_enabled();

private:
  common::SpinRWLock wash_lock_;
  TryWashTabletFunc wash_func_;
  const uint64_t tenant_id_;
  ObBucketLock bucket_lock_;
  ObFullTabletCreator full_tablet_creator_;
  ObTabletPointerMap tablet_map_;
  int tg_id_;
  int persist_tg_id_; // since persist task may cost too much time, we use another thread to exec.
  TableGCTask table_gc_task_;
  RefreshConfigTask refresh_config_task_;
  TabletGCTask tablet_gc_task_;
  TabletGCQueue tablet_gc_queue_;
  common::ObLinkQueue free_tables_queue_;
  common::ObSpinLock gc_queue_lock_;
  common::hash::ObHashMap<share::ObLSID, memtable::ObMemtableSet*> gc_memtable_map_;
  ObTabletLeakChecker leak_checker_;

  ObTenantMetaObjPool<memtable::ObMemtable> memtable_pool_;
  ObTenantMetaObjPool<ObNormalTabletBuffer> tablet_buffer_pool_;
  ObTenantMetaObjPool<ObLargeTabletBuffer> large_tablet_buffer_pool_;
  ObTenantMetaObjPool<ObDDLKV> ddl_kv_pool_;
  ObTenantMetaObjPool<ObTabletDDLKvMgr> tablet_ddl_kv_mgr_pool_;
  ObTenantMetaObjPool<ObTxDataMemtable> tx_data_memtable_pool_;
  ObTenantMetaObjPool<ObTxCtxMemtable> tx_ctx_memtable_pool_;
  ObTenantMetaObjPool<transaction::tablelock::ObLockMemtable> lock_memtable_pool_;
  ObITenantMetaObjPool *pool_arr_[ObITable::TableType::MAX_TABLE_TYPE];

  // for washing
  TabletBufferList normal_tablet_header_;
  TabletBufferList large_tablet_header_;

  common::ObConcurrentFIFOAllocator meta_cache_io_allocator_;
  int64_t last_access_tenant_config_ts_;
  ObT3MResourceLimitCalculatorHandler t3m_limit_calculator_;

  bool is_tablet_leak_checker_enabled_;
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
  ObITenantTabletPointerIterator() = default;
  virtual ~ObITenantTabletPointerIterator() = default;
  virtual int get_next_tablet_pointer(
      ObTabletMapKey &key,
      ObTabletPointerHandle &pointer_handle,
      ObTabletHandle &tablet_handle) = 0;
};

class ObT3mTabletMapIterator
{
  friend class ObTenantMetaMemMgr;
public:
  explicit ObT3mTabletMapIterator(ObTenantMetaMemMgr &t3m);
  virtual ~ObT3mTabletMapIterator();
  void reset();
protected:
  typedef ObResourceValueStore<ObTabletPointer> TabletValueStore;
  typedef ObTabletPointerMap TabletMap;
  typedef common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> TabletPair;

  int fetch_tablet_item();
  static bool ignore_err_code(const int ret) { return OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret || OB_NOT_THE_OBJECT == ret; }
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
  common::ObSEArray<ObTabletMapKey, DEFAULT_TABLET_ITEM_CNT> tablet_items_;
  int64_t idx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObT3mTabletMapIterator);
};

class ObTenantTabletIterator : public ObT3mTabletMapIterator,
                               public ObITenantTabletIterator
{
public:
  ObTenantTabletIterator(
      ObTenantMetaMemMgr &t3m,
      common::ObArenaAllocator &allocator,
      ObITabletFilterOp *op);
  virtual ~ObTenantTabletIterator() = default;
  virtual int get_next_tablet(ObTabletHandle &handle) override;

private:
  common::ObArenaAllocator *allocator_;
  storage::ObITabletFilterOp *op_;
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
