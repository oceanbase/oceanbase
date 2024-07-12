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


#define USING_LOG_PREFIX STORAGE

#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/multi_data_source/mds_table_mgr.h"
#include "lib/function/ob_function.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tx_table/ob_tx_data_memtable.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "share/ob_thread_define.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/ob_disk_usage_reporter.h"

namespace oceanbase
{
using namespace common::hash;
using namespace share;
using namespace memtable;
using namespace transaction::tablelock;
namespace storage
{
ObTenantMetaMemStatus::ObTenantMetaMemStatus()
  : total_size_(0),
    used_size_(0),
    used_obj_cnt_(0),
    free_obj_cnt_(0),
    each_obj_size_(0)
{
  name_[0] = '\0';
}

void ObTabletBufferInfo::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  tablet_buffer_ptr_ = nullptr;
  tablet_ = nullptr;
  pool_type_ = ObTabletPoolType::TP_MAX;
  in_map_ = false;
  last_access_time_ = -1;
}

int ObTabletBufferInfo::fill_info(const ObTabletPoolType &pool_type, ObMetaObjBufferNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || ObTabletPoolType::TP_MAX == pool_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(node), K(pool_type));
  } else {
    char *obj_buffer = ObMetaObjBufferHelper::get_obj_buffer(node);
    tablet_ = reinterpret_cast<ObTablet *>(obj_buffer);
    const ObTabletMeta &tablet_meta = tablet_->get_tablet_meta();
    tablet_id_ = tablet_meta.tablet_id_;
    ls_id_ = tablet_meta.ls_id_;
    tablet_buffer_ptr_ = reinterpret_cast<char *>(node);
    in_map_ = ObMetaObjBufferHelper::is_in_map(obj_buffer);
    pool_type_ = pool_type;
    const int64_t wash_score = tablet_->get_wash_score();
    last_access_time_ = wash_score > 0 ? wash_score : INT64_MAX + wash_score;
  }
  return ret;
}

void ObTenantMetaMemMgr::TabletGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool all_tablet_cleaned = false;
  if (OB_FAIL(t3m_->gc_tablets_in_queue(all_tablet_cleaned))) {
    LOG_WARN("fail to gc tablets in queue", K(ret));
  }
}

void ObTenantMetaMemMgr::TableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool all_table_cleaned = false; // no use
  if (OB_FAIL(t3m_->gc_tables_in_queue(all_table_cleaned))) {
    LOG_WARN("fail to gc tables in queue", K(ret));
  }
}

void ObTenantMetaMemMgr::RefreshConfigTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  const int64_t mem_limit = tenant_config.is_valid()
      ? tenant_config->_storage_meta_memory_limit_percentage : OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT;
  if (OB_FAIL(lib::set_meta_obj_limit(tenant_id, mem_limit))) {
    LOG_WARN("fail to set meta object memory limit", K(ret), K(tenant_id), K(mem_limit));
  }
}

ObTablet *ObTenantMetaMemMgr::TabletGCQueue::pop()
{
  SpinWLockGuard lock(queue_lock_);
  ObTablet *tablet = gc_head_;
  const int64_t tablet_cnt = count();
  OB_ASSERT(tablet_cnt >= 0);
  if (0 == tablet_cnt) {
    OB_ASSERT(nullptr == gc_head_);
    OB_ASSERT(nullptr == gc_tail_);
  } else if (tablet_cnt > 1) {
    OB_ASSERT(nullptr != gc_head_);
    OB_ASSERT(nullptr != gc_tail_);
    OB_ASSERT(gc_head_ != gc_tail_);
    gc_head_ = gc_head_->get_next_tablet();
  } else if (tablet_cnt == 1) {
    OB_ASSERT(gc_head_ == gc_tail_);
    OB_ASSERT(gc_head_ != nullptr);
    gc_head_ = gc_tail_ = nullptr;
  }
  if (OB_NOT_NULL(tablet)) {
    tablet->set_next_tablet(nullptr);
    ATOMIC_DEC(&tablet_count_);
  }
  return tablet;
}

int ObTenantMetaMemMgr::TabletGCQueue::push(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is nullptr", K(ret), KP(tablet));
  } else {
    SpinWLockGuard lock(queue_lock_);
    const int64_t tablet_cnt = count();
    OB_ASSERT(tablet_cnt >= 0);
    tablet->set_next_tablet(nullptr);
    if (0 == tablet_cnt) {
      OB_ASSERT(nullptr == gc_head_);
      OB_ASSERT(nullptr == gc_tail_);
      gc_head_ = gc_tail_ = tablet;
    } else if (tablet_cnt >= 1) {
      OB_ASSERT(nullptr != gc_head_);
      OB_ASSERT(nullptr != gc_tail_);
      OB_ASSERT(1 == tablet_cnt ? gc_head_ == gc_tail_ : gc_head_ != gc_tail_);
      gc_tail_->set_next_tablet(tablet);
      gc_tail_ = tablet;
    }
    ATOMIC_INC(&tablet_count_);
  }
  return ret;
}

ObTenantMetaMemMgr::ObTenantMetaMemMgr(const uint64_t tenant_id)
  : wash_lock_(common::ObLatchIds::TENANT_META_MEM_MGR_LOCK),
    wash_func_(*this),
    tenant_id_(tenant_id),
    bucket_lock_(),
    full_tablet_creator_(),
    tablet_map_(),
    tg_id_(-1),
    table_gc_task_(this),
    refresh_config_task_(),
    tablet_gc_task_(this),
    tablet_gc_queue_(),
    free_tables_queue_(),
    gc_queue_lock_(common::ObLatchIds::TENANT_META_MEM_MGR_LOCK),
    memtable_pool_(tenant_id, get_default_memtable_pool_count(), "MemTblObj", ObCtxIds::DEFAULT_CTX_ID),
    tablet_buffer_pool_(tenant_id, get_default_normal_tablet_pool_count(), "N_TabletPool", ObCtxIds::META_OBJ_CTX_ID, &wash_func_),
    large_tablet_buffer_pool_(tenant_id, get_default_large_tablet_pool_count(), "L_TabletPool", ObCtxIds::META_OBJ_CTX_ID, &wash_func_, false/*allow_over_max_free_num*/),
    ddl_kv_pool_(tenant_id, MAX_DDL_KV_IN_OBJ_POOL, "DDLKVObj", ObCtxIds::DEFAULT_CTX_ID),
    tablet_ddl_kv_mgr_pool_(tenant_id, get_default_tablet_pool_count(), "DDLKvMgrObj", ObCtxIds::DEFAULT_CTX_ID),
    tx_data_memtable_pool_(tenant_id, MAX_TX_DATA_MEMTABLE_CNT_IN_OBJ_POOL, "TxDataMemObj", ObCtxIds::DEFAULT_CTX_ID),
    tx_ctx_memtable_pool_(tenant_id, MAX_TX_CTX_MEMTABLE_CNT_IN_OBJ_POOL, "TxCtxMemObj", ObCtxIds::DEFAULT_CTX_ID),
    lock_memtable_pool_(tenant_id, MAX_LOCK_MEMTABLE_CNT_IN_OBJ_POOL, "LockMemObj", ObCtxIds::DEFAULT_CTX_ID),
    meta_cache_io_allocator_(),
    last_access_tenant_config_ts_(-1),
    t3m_limit_calculator_(*this),
    is_tablet_leak_checker_enabled_(false),
    is_inited_(false)
{
  for (int64_t i = 0; i < ObITable::TableType::MAX_TABLE_TYPE; i++) {
    pool_arr_[i] = nullptr;
  }
}

ObTenantMetaMemMgr::~ObTenantMetaMemMgr()
{
  destroy();
}

int ObTenantMetaMemMgr::mtl_new(ObTenantMetaMemMgr *&meta_mem_mgr)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();
  meta_mem_mgr = OB_NEW(ObTenantMetaMemMgr, ObMemAttr(tenant_id, "MetaMemMgr"), tenant_id);
  if (OB_ISNULL(meta_mem_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantMetaMemMgr::init()
{
  int ret = OB_SUCCESS;
  const lib::ObMemAttr map_attr(tenant_id_, "TabletMap");
  const int64_t mem_limit = 4 * 1024 * 1024 * 1024LL;
  const int64_t bucket_num = cal_adaptive_bucket_num();
  const int64_t pin_set_bucket_num = common::hash::cal_next_prime(DEFAULT_BUCKET_NUM);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantMetaMemMgr has been initialized", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::BLOCK_MANAGER_LOCK, "T3MBucket",
      tenant_id_))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else if (OB_FAIL(full_tablet_creator_.init(tenant_id_))) {
    LOG_WARN("fail to init full tablet creator", K(ret), K(tenant_id_));
  } else if (OB_FAIL(tablet_map_.init(bucket_num, map_attr, TOTAL_LIMIT, HOLD_LIMIT,
        common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("fail to initialize tablet map", K(ret), K(bucket_num));
  } else if (OB_FAIL(gc_memtable_map_.create(10, "GCMemtableMap", "GCMemtableMap", tenant_id_))) {
    LOG_WARN("fail to initialize gc memtable map", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantMetaMemMgr, tg_id_))) {
    LOG_WARN("fail to create thread for t3m", K(ret));
  } else if (OB_FAIL(meta_cache_io_allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE, "StorMetaCacheIO", tenant_id_, mem_limit))) {
    LOG_WARN("fail to init storage meta cache io allocator", K(ret), K_(tenant_id), K(mem_limit));
  } else if (OB_FAIL(fetch_tenant_config())) {
    LOG_WARN("fail to fetch tenant config", K(ret));
  } else {
    init_pool_arr();
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObTenantMetaMemMgr::fetch_tenant_config()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant config", K(ret), K(tenant_id_));
  } else {
    is_tablet_leak_checker_enabled_ = tenant_config->_enable_trace_tablet_leak;
    LOG_INFO("fetch tenant config", K(tenant_id_), K(is_tablet_leak_checker_enabled_));
  }
  return ret;
}

void ObTenantMetaMemMgr::init_pool_arr()
{
  pool_arr_[static_cast<int>(ObITable::TableType::DATA_MEMTABLE)] = &memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::TX_DATA_MEMTABLE)] = &tx_data_memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::TX_CTX_MEMTABLE)] = &tx_ctx_memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::LOCK_MEMTABLE)] = &lock_memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::DIRECT_LOAD_MEMTABLE)] = &ddl_kv_pool_;
}

int ObTenantMetaMemMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been inited", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start thread for t3m", K(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, table_gc_task_, TABLE_GC_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule itables gc task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
      tg_id_, refresh_config_task_, REFRESH_CONFIG_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule refresh config task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
      tg_id_, tablet_gc_task_, TABLE_GC_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule tablet gc task", K(ret));
  } else {
    LOG_INFO("successfully to start t3m's three tasks", K(ret), K(tg_id_));
  }
  return ret;
}

void ObTenantMetaMemMgr::stop()
{
  // When the observer exits by kill -15, the release of meta is triggered by prepare_safe_destory
  // called by ObLSService::stop(), then the ObLSService::wait() infinitely check if all meta release completed,
  // so t3m can't stop gc thread until check_all_meta_mem_released is true in ObTenantMetaMemMgr::wait()
}

void ObTenantMetaMemMgr::wait()
{
  if (OB_LIKELY(is_inited_)) {
    int ret = OB_SUCCESS;
    bool is_all_meta_released = false;
    while (!is_all_meta_released) {
      if (OB_FAIL(check_all_meta_mem_released(is_all_meta_released, "t3m_wait"))) {
        is_all_meta_released = false;
        LOG_WARN("fail to check_all_meta_mem_released", K(ret));
      }
      if (!is_all_meta_released) {
        if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          if (OB_FAIL(dump_tablet_info())) {
            LOG_WARN("fail to dump tablet info", K(ret));
          }
        }
        LOG_WARN("wait all meta released in t3m", K(ret));
        ob_usleep(1 * 1000 * 1000); // 1s
      }
    }

    TG_STOP(tg_id_);

    TG_WAIT(tg_id_);
  }
}

void ObTenantMetaMemMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (tg_id_ != -1) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  full_tablet_creator_.reset(); // must reset after gc_tablets
  tablet_map_.destroy();
  for (common::hash::ObHashMap<share::ObLSID, memtable::ObMemtableSet*>::iterator iter = gc_memtable_map_.begin();
      OB_SUCC(ret) && iter != gc_memtable_map_.end(); ++iter) {
    memtable::ObMemtableSet *memtable_set = iter->second;
    if (OB_NOT_NULL(memtable_set)) {
      if (0 != memtable_set->size()) {
        LOG_ERROR("leaked memtable", KPC(memtable_set));
      }
      if (OB_FAIL(memtable_set->destroy())) {
        LOG_ERROR("memtable set destroy failed", K(ret));
      }
      ob_free(memtable_set);
    }
  }
  gc_memtable_map_.destroy();
  bucket_lock_.destroy();
  for (int64_t i = 0; i <= ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE; i++) {
    pool_arr_[i] = nullptr;
  }
  meta_cache_io_allocator_.destroy();
  is_inited_ = false;
}

void ObTenantMetaMemMgr::mtl_destroy(ObTenantMetaMemMgr *&meta_mem_mgr)
{
  if (OB_UNLIKELY(nullptr == meta_mem_mgr)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "meta mem mgr is nullptr", KP(meta_mem_mgr));
  } else {
    OB_DELETE(ObTenantMetaMemMgr, oceanbase::ObModIds::OMT_TENANT, meta_mem_mgr);
    meta_mem_mgr = nullptr;
  }
}

int ObTenantMetaMemMgr::print_old_chain(
    const ObTabletMapKey &key,
    const ObTabletPointer &tablet_ptr,
    const int64_t buf_len,
    char *buf)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
  ObTablet *tablet = tablet_ptr.old_version_chain_;
  while (nullptr != tablet && OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{%p} ", static_cast<void*>(tablet)))) {
      if (OB_SIZE_OVERFLOW != ret) {
        SERVER_LOG(WARN, "fail to print tablet on old chain", K(ret), KP(tablet));
      }
    } else {
      tablet = tablet->get_next_tablet();
    }
  }
  if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTenantMetaMemMgr::push_table_into_gc_queue(ObITable *table, const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  const int64_t size = sizeof(TableGCItem);
  const ObMemAttr attr(tenant_id_, "TableGCItem");
  TableGCItem *item = nullptr;
  static const int64_t SLEEP_TS = 100_ms;
  static const int64_t MAX_RETRY_TIMES = 1000; // about 100s
  int64_t retry_cnt = 0;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(table));
  } else if (OB_UNLIKELY(!ObITable::is_table_type_valid(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid table key", K(ret), K(table_type), KPC(table));
  } else if (OB_UNLIKELY(ObITable::is_sstable(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("should not recycle sstable", K(ret), K(table_type), KPC(table));
  } else {
    do {
      if (OB_ISNULL(item = (TableGCItem *)ob_malloc(size, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for TableGCItem", K(ret), K(size));
      } else {
        if (ObITable::TableType::DATA_MEMTABLE == table_type) {
          ObMemtable *memtable = static_cast<ObMemtable *>(table);
          ObMtStat& mt_stat = memtable->get_mt_stat();
          if (0 == mt_stat.push_table_into_gc_queue_time_) {
            mt_stat.push_table_into_gc_queue_time_ = ObTimeUtility::current_time();
            if (0 != mt_stat.release_time_
                && mt_stat.push_table_into_gc_queue_time_ -
                   mt_stat.release_time_ >= 10 * 1000 * 1000 /*10s*/) {
              LOG_WARN("It cost too much time to dec ref cnt", K(ret), KPC(memtable), K(lbt()));
            }
          }
        }

        item->table_ = table;
        item->table_type_ = table_type;
        if (OB_FAIL(free_tables_queue_.push((ObLink *)item))) {
          LOG_ERROR("fail to push back into free_tables_queue_", K(ret), KPC(item));
        }
      }

      if (OB_FAIL(ret) && nullptr != item) {
        ob_free(item);
      }
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        ob_usleep(SLEEP_TS);
        retry_cnt++;               // retry some times
        if (retry_cnt % 100 == 0) {
          LOG_ERROR("push table into gc queue retry too many times", K(retry_cnt));
        }
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret
             && retry_cnt < MAX_RETRY_TIMES);
  }
  LOG_DEBUG("push table into gc queue", K(ret), KP(table), K(table_type), K(common::lbt()));
  return ret;
}

int ObTenantMetaMemMgr::gc_tables_in_queue(bool &all_table_cleaned)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  all_table_cleaned = false;
  const int64_t pending_cnt = free_tables_queue_.size();
  int64_t left_recycle_cnt = MIN(pending_cnt, ONE_ROUND_RECYCLE_COUNT_THRESHOLD);
  int64_t table_cnt_arr[ObITable::TableType::MAX_TABLE_TYPE] = { 0 };

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m has not been inited", K(ret), KP(this));
  } else {
    lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_queue_lock_);
    while(OB_SUCC(ret) && left_recycle_cnt-- > 0 && free_tables_queue_.size() > 0) {
      ObLink *ptr = nullptr;
      if (OB_FAIL(free_tables_queue_.pop(ptr))) {
        LOG_WARN("fail to pop itable gc item", K(ret));
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("itable gc item is nullptr", K(ret), KP(ptr));
      } else {
        TableGCItem *item = static_cast<TableGCItem *>(ptr);
        ObITable *table = item->table_;
        bool is_safe = false;
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the table in gc item is nullptr", K(ret), KP(item));
        } else if (OB_FAIL(table->safe_to_destroy(is_safe))) {
          LOG_WARN("fail to check safe_to_destroy", K(ret), KPC(table));
        } else if (is_safe) {
          ObITable::TableType table_type = item->table_type_;
          int64_t index = static_cast<int>(table_type);
          if (OB_UNLIKELY(!ObITable::is_table_type_valid(table_type) || nullptr == pool_arr_[index])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("the table type is invalid", K(ret), KP(item), KPC(item), KPC(table), K(table->get_key()));
          } else {
            if (ObITable::TableType::DATA_MEMTABLE == table_type) {
              if (OB_FAIL(push_memtable_into_gc_map_(static_cast<memtable::ObMemtable *>(table)))) {
                LOG_WARN("push memtable into gc map failed", K(ret));
              }
            } else {
              pool_arr_[index]->free_obj(static_cast<void *>(table));
            }

            if (OB_SUCC(ret)) {
              table_cnt_arr[index]++;
            }
          }
        } else if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
          LOG_INFO("the table is unsafe to destroy", KPC(table));
        }

        if (OB_FAIL(ret)) {
          if (OB_TMP_FAIL(free_tables_queue_.push(item))) {
            LOG_ERROR("fail to push itable gc item into queue", K(tmp_ret));
          }
        } else {
          if (is_safe) {
            ob_free(item);
          } else {
            if (OB_TMP_FAIL(free_tables_queue_.push(item))) {
              LOG_ERROR("fail to push itable gc item into queue", K(tmp_ret));
            }
          }
        }
      }
    }

    // batch gc memtable will handle error gracefully
    batch_gc_memtable_();

    if (OB_SUCC(ret)) {
      all_table_cleaned = (0 == free_tables_queue_.size());
    }
  }

  // no matter success or failure, print GC result
  {
    int64_t sstable_cnt = 0;
    const int64_t data_memtable_cnt = table_cnt_arr[static_cast<int>(ObITable::TableType::DATA_MEMTABLE)];
    const int64_t tx_data_memtable_cnt = table_cnt_arr[static_cast<int>(ObITable::TableType::TX_DATA_MEMTABLE)];
    const int64_t tx_ctx_memtable_cnt = table_cnt_arr[static_cast<int>(ObITable::TableType::TX_CTX_MEMTABLE)];
    const int64_t lock_memtable_cnt = table_cnt_arr[static_cast<int>(ObITable::TableType::LOCK_MEMTABLE)];
    for (int64_t i = static_cast<int>(ObITable::TableType::MAJOR_SSTABLE);
        i < static_cast<int>(ObITable::TableType::MAX_TABLE_TYPE);
        i++) {
      if (ObITable::is_sstable((ObITable::TableType)i)) {
        sstable_cnt += table_cnt_arr[i];
      }
    }
    const int64_t recycled_cnt = sstable_cnt + data_memtable_cnt + tx_data_memtable_cnt + tx_ctx_memtable_cnt +
        lock_memtable_cnt;
    const int64_t tablets_mem = tablet_buffer_pool_.total() + large_tablet_buffer_pool_.total() + full_tablet_creator_.total();
    int64_t tablets_mem_limit = 0;
    ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
    if (OB_NOT_NULL(alloc)) {
      const ObTenantCtxAllocatorGuard &ag = alloc->get_tenant_ctx_allocator(MTL_ID(), ObCtxIds::META_OBJ_CTX_ID);
      if (OB_NOT_NULL(ag)) {
        tablets_mem_limit = ag->get_limit();
      }
    }

    if (recycled_cnt > 0) {
      FLOG_INFO("gc tables in queue", K(sstable_cnt), K(data_memtable_cnt),
        K(tx_data_memtable_cnt), K(tx_ctx_memtable_cnt), K(lock_memtable_cnt), K(pending_cnt), K(recycled_cnt),
        K(tablet_buffer_pool_), K(large_tablet_buffer_pool_), K(full_tablet_creator_), K(tablets_mem), K(tablets_mem_limit),
        K(ddl_kv_pool_), K(memtable_pool_), "wait_gc_count", free_tables_queue_.size(),
        "tablet count", tablet_map_.count());
    } else if (REACH_COUNT_INTERVAL(100)) {
      FLOG_INFO("gc tables in queue: recycle 0 table", K(ret),
          K(tablet_buffer_pool_), K(large_tablet_buffer_pool_), K(full_tablet_creator_), K(tablets_mem), K(tablets_mem_limit),
          K(ddl_kv_pool_), K(memtable_pool_), K(pending_cnt), "wait_gc_count", free_tables_queue_.size(),
          "tablet count", tablet_map_.count());
    }
  }

  return ret;
}

void ObTenantMetaMemMgr::batch_destroy_memtable_(memtable::ObMemtableSet *memtable_set)
{
  for (common::hash::ObHashSet<uint64_t>::iterator set_iter = memtable_set->begin();
       set_iter != memtable_set->end();
       ++set_iter) {
    (void)(((memtable::ObMemtable *)set_iter->first)->pre_batch_destroy_keybtree());
  }
  memtable::ObMemtableKeyBtree::batch_destroy();
}

void ObTenantMetaMemMgr::batch_gc_memtable_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  for (common::hash::ObHashMap<share::ObLSID, memtable::ObMemtableSet*>::iterator iter = gc_memtable_map_.begin();
       iter != gc_memtable_map_.end(); ++iter) {
    const ObLSID &ls_id = iter->first;
    memtable::ObMemtableSet *memtable_set = iter->second;
    if (OB_NOT_NULL(memtable_set)
        && 0 != memtable_set->size()) {
      if (ls_id.id() != 0) {
        if (OB_TMP_FAIL(ObMemtable::batch_remove_unused_callback_for_uncommited_txn(ls_id,
                                                                                    memtable_set))) {
          if (OB_NOT_RUNNING != tmp_ret) {
            LOG_ERROR("batch remove memtable set failed", K(tmp_ret), KPC(memtable_set));
          } else {
            LOG_WARN("batch remove memtable set failed", K(tmp_ret), KPC(memtable_set));
          }
          for (common::hash::ObHashSet<uint64_t>::iterator set_iter = memtable_set->begin();
               set_iter != memtable_set->end();
               ++set_iter) {
            if (OB_TMP_FAIL(push_table_into_gc_queue((ObITable *)(set_iter->first), ObITable::TableType::DATA_MEMTABLE))) {
              LOG_ERROR("push table into gc queue failed, maybe there will be leak",
                        K(tmp_ret), KPC(memtable_set));
            }
          }

          LOG_INFO("batch gc memtable failed and push into gc queue again", K(memtable_set->size()));
          while (OB_TMP_FAIL(memtable_set->clear())) {
            LOG_ERROR("clear memtable set failed", K(tmp_ret), KPC(memtable_set));
          }
        } else {
          (void)batch_destroy_memtable_(memtable_set);

          int64_t batch_destroyed_occupy_size = 0;
          for (common::hash::ObHashSet<uint64_t>::iterator set_iter = memtable_set->begin();
               set_iter != memtable_set->end();
               ++set_iter) {
            batch_destroyed_occupy_size += ((ObMemtable *)(set_iter->first))->get_occupied_size();
            pool_arr_[static_cast<int>(ObITable::TableType::DATA_MEMTABLE)]->free_obj((void *)(set_iter->first));
          }

          FLOG_INFO("batch gc memtable successfully", K(memtable_set->size()), K(batch_destroyed_occupy_size));
          while (OB_TMP_FAIL(memtable_set->clear())) {
            LOG_ERROR("clear memtable set failed", K(tmp_ret), KPC(memtable_set));
          }
        }
      } else {
        for (common::hash::ObHashSet<uint64_t>::iterator set_iter = memtable_set->begin();
             set_iter != memtable_set->end();
             ++set_iter) {
          pool_arr_[static_cast<int>(ObITable::TableType::DATA_MEMTABLE)]->free_obj((void *)(set_iter->first));
        }

        LOG_INFO("batch gc memtable successfully", K(memtable_set->size()));
        while (OB_TMP_FAIL(memtable_set->clear())) {
          LOG_ERROR("clear memtable set failed", K(tmp_ret), KPC(memtable_set));
        }
      }
    }
  }

  if (REACH_TENANT_TIME_INTERVAL(1_hour)) {
    for (common::hash::ObHashMap<share::ObLSID, memtable::ObMemtableSet*>::iterator iter = gc_memtable_map_.begin();
         iter != gc_memtable_map_.end(); ++iter) {
      memtable::ObMemtableSet *memtable_set = iter->second;
      if (OB_NOT_NULL(memtable_set)) {
        if (0 != memtable_set->size()) {
          LOG_ERROR("leaked memtable", KPC(memtable_set));
        }
        if (OB_FAIL(memtable_set->destroy())) {
          LOG_ERROR("memtable set destroy failed", K(ret));
        }
        ob_free(memtable_set);
      }
    }

    if (OB_TMP_FAIL(gc_memtable_map_.clear())) {
      LOG_ERROR("clear gc memtable map failed", K(tmp_ret));
    }
  }
}

int ObTenantMetaMemMgr::push_memtable_into_gc_map_(memtable::ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id;
  const ObMemAttr attr(tenant_id_, "memtable_set");
  memtable::ObMemtableSet *memtable_set = nullptr;

  if (OB_FAIL(memtable->get_ls_id(ls_id))) {
    LOG_WARN("get memtable ls id failed", K(ret), KPC(memtable));
    if (OB_NOT_INIT == ret) {
      // special ls id for memtable which fail to init
      ls_id = 0;
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gc_memtable_map_.get_refactored(ls_id, memtable_set))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      memtable::ObMemtableSet *tmp_memtable_set;
      void *buf = NULL;
      if (OB_ISNULL(buf = ob_malloc(sizeof(memtable::ObMemtableSet), attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for hash set", K(ret));
      } else if (OB_ISNULL(tmp_memtable_set = new (buf) ObMemtableSet())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for hash set", K(ret));
      } else if (OB_FAIL(tmp_memtable_set->create(1024,
                                                  "MemtableSetBkt",
                                                  "MemtableSetNode",
                                                  MTL_ID()))) {
        LOG_WARN("fail to create", K(ret));
      } else if (OB_FAIL(gc_memtable_map_.set_refactored(ls_id, tmp_memtable_set))) {
        LOG_WARN("fail to set hash set", K(ret));
      } else {
        memtable_set = tmp_memtable_set;
      }

      if (NULL != buf && OB_FAIL(ret)) {
        ob_free(tmp_memtable_set);
      }
    } else {
      LOG_WARN("map get failed", K(ret), KPC(memtable));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(memtable_set->set_refactored((uint64_t)(memtable), 0/*flag, not overwrite*/))) {
      LOG_WARN("map set failed", K(ret), KPC(memtable));
    }
  }

  return ret;
}

int ObTenantMetaMemMgr::inner_push_tablet_into_gc_queue(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObTabletHandle empty_handle;
  if (OB_UNLIKELY(nullptr == tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to push invalid tablet into gc queue", K(ret), KP(tablet));
  } else if (FALSE_IT(tablet->set_next_tablet_guard(empty_handle))) { // release the ref_cnt of next_tablet_guard_
  } else if (OB_FAIL(tablet_gc_queue_.push(tablet))) {
    LOG_WARN("fail to push tablet into gc queue", K(ret), KP(tablet));
  } else {
    LOG_DEBUG("inner push tablet into gc queue", K(ret), KP(tablet));
  }
  return ret;
}

int ObTenantMetaMemMgr::push_tablet_into_gc_queue(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m has not been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == tablet || tablet->get_ref() != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("push invalid tablet into gc queue", K(ret), KPC(tablet));
  } else if (!tablet->is_valid()) {
    // If a tablet is invalid, it is not expensive to recycle it. Recycle it directly, to avoid asynchronous
    // tablet gc queue backlog。
    LOG_INFO("release an invalid tablet", KP(tablet));
    release_tablet(tablet, false/*return tablet buffer ptr after release*/);
  } else {
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());

    const ObTabletPointerHandle &ptr_handle = tablet->get_pointer_handle();
    ObTabletPointer *tablet_ptr = nullptr;
    if (OB_ISNULL(tablet_ptr = static_cast<ObTabletPointer *>(ptr_handle.get_resource_ptr()))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null tablet pointer", K(ret), K(key), K(ptr_handle));
    } else if (OB_FAIL(tablet_ptr->remove_tablet_from_old_version_chain(tablet))) {
      LOG_WARN("fail to remove tablet from old version chain", K(ret), K(key), KPC(tablet));
    } else if (FALSE_IT(tablet->reset_memtable())) {
    } else if (OB_FAIL(inner_push_tablet_into_gc_queue(tablet))) {
      LOG_WARN("fail to push tablet into gc queue", K(ret), KPC(tablet));
    }
  }
#ifndef OB_BUILD_PACKAGE
  FLOG_INFO("push tablet into gc queue", K(ret), KP(tablet), K(common::lbt()));
#endif
  return ret;
}

int ObTenantMetaMemMgr::gc_tablets_in_queue(bool &all_tablet_cleaned)
{
  int ret = OB_SUCCESS;
  all_tablet_cleaned = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m has not been inited", K(ret));
  } else {
    int64_t gc_tablets_cnt = 0;
    int64_t err_tablets_cnt = 0;
    int64_t left_recycle_cnt = ONE_ROUND_TABLET_GC_COUNT_THRESHOLD;
    ObTablet *tablet = nullptr;

    while(OB_SUCC(ret) && left_recycle_cnt > 0) {
      if (OB_ISNULL(tablet = tablet_gc_queue_.pop())) {
        break; // tablet gc queue is empty.
      } else if (OB_UNLIKELY(tablet->get_ref() != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet in gc queue", K(ret), KPC(tablet));
      } else {
        release_tablet(tablet, false/* return tablet buffer ptr after release*/);
      }
      if (OB_FAIL(ret)) {
        ++err_tablets_cnt;
        FLOG_INFO("fail to gc tablet", K(ret), KP(tablet), KPC(tablet));
        if (OB_FAIL(inner_push_tablet_into_gc_queue(tablet))) {
          LOG_ERROR("fail to push tablet into gc queue, tablet may be leaked", K(ret), KP(tablet));
        }
        ret = OB_SUCCESS; // continue to gc other tablet
      } else {
        ++gc_tablets_cnt;
        FLOG_INFO("succeed to gc tablet", K(ret), KP(tablet));
      }
      --left_recycle_cnt;
    }
    if (left_recycle_cnt < ONE_ROUND_TABLET_GC_COUNT_THRESHOLD) {
      FLOG_INFO("gc tablets in queue", K(gc_tablets_cnt), K(err_tablets_cnt), K(tablet_gc_queue_.count()));
    }
    all_tablet_cleaned = tablet_gc_queue_.is_empty();
  }
  return ret;
}

int ObTenantMetaMemMgr::has_meta_wait_gc(bool &is_wait)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m has not been inited", K(ret));
  } else {
    const int64_t wait_gc_tablets_cnt = tablet_gc_queue_.count();
    int64_t wait_gc_tables_cnt = 0;
    {
      lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_queue_lock_);
      wait_gc_tables_cnt = free_tables_queue_.size();
    }
    is_wait = (0 != wait_gc_tablets_cnt || 0 != wait_gc_tables_cnt);
    LOG_INFO("check has meta wait gc in t3m", K(wait_gc_tablets_cnt), K(wait_gc_tables_cnt));
  }
  return ret;
}

void *ObTenantMetaMemMgr::release_tablet(ObTablet *tablet, const bool return_buf_ptr_after_release)
{
  void *free_obj = nullptr;
  tablet->dec_macro_ref_cnt();
  ObIAllocator *allocator = tablet->get_allocator();
  if (OB_ISNULL(allocator)) {
    if (return_buf_ptr_after_release) {
      ObMetaObjBufferHelper::del_meta_obj(tablet);
      free_obj = ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char*>(tablet));
      release_tablet_from_pool(tablet, false/*give_back_tablet_into_pool*/);
    } else {
      release_tablet_from_pool(tablet, true/*give_back_tablet_into_pool*/);
    }
  } else {
    full_tablet_creator_.free_tablet(tablet);
  }
  return free_obj;
}

int ObTenantMetaMemMgr::get_min_end_scn_for_ls(
    const ObTabletMapKey &key,
    const SCN &ls_checkpoint,
    SCN &min_end_scn_from_latest,
    SCN &min_end_scn_from_old)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetMinScn"));
  ObTabletHandle handle;
  min_end_scn_from_latest.set_max();
  min_end_scn_from_old.set_max();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(get_tablet_with_allocator(WashTabletPriority::WTP_LOW, key, allocator, handle))) {
    LOG_WARN("fail to get latest tablet", K(ret), K(key));
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tablet handle", K(ret), K(key), K(handle));
  } else if (OB_FAIL(get_min_end_scn_from_single_tablet(
      handle.get_obj(), false/*is_old*/, ls_checkpoint, min_end_scn_from_latest))) {
    LOG_WARN("fail to get min end scn from latest tablet", K(ret), K(key), K(handle));
  } else {
    // get_ls_min_end_scn_in_latest_tablets must before get_ls_min_end_scn_in_old_tablets
    const ObTabletPointerHandle &ptr_handle = handle.get_obj()->get_pointer_handle();
    ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(ptr_handle.get_resource_ptr());
    ObTablet *tablet = nullptr;
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash()); // lock old_version_chain
    if (OB_ISNULL(tablet_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet ptr is NULL", K(ret), K(ptr_handle));
    } else if (OB_ISNULL(tablet = tablet_ptr->old_version_chain_)) { // skip
    } else {
      // since the last tablet may not be the oldest, we traverse the whole chain
      while (OB_SUCC(ret) && OB_NOT_NULL(tablet)) {
        if (OB_FAIL(get_min_end_scn_from_single_tablet(tablet, true/*is_old*/, ls_checkpoint, min_end_scn_from_old))) {
          LOG_WARN("fail to get min end scn from old tablet", K(ret), KP(tablet));
        } else {
          tablet = tablet->get_next_tablet();
        }
      }
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::get_min_end_scn_from_single_tablet(ObTablet *tablet,
                                                           const bool is_old,
                                                           const SCN &ls_checkpoint,
                                                           SCN &min_end_scn)
{
  int ret = OB_SUCCESS;
  bool is_committed = false;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tablet is nullptr.", K(ret), KP(this));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
    if (OB_EMPTY_RESULT == ret) {
      // When OB_EMPTY_RESULT is returned, there are two situations that need to be eaten, as follows:
      // - The one is that transfer transaction is aborted.
      // - The second is that the tablet is just been created and the tablet status has been also written.
      //
      // In the above two cases, the old version tablet is not allowed and will not exist, and it
      // is not allowed to be read without being queried, so it is safe to return max scn here.
      min_end_scn.set_max();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tablet status failed", KR(ret), KP(tablet));
    }
  } else if (ObTabletStatus::TRANSFER_IN == user_data.tablet_status_) {
    /* when tablet transfer with active tx, dest_ls may recycle active transaction tx_data
     * because no uncommitted data depend it, but src_ls's tablet may has uncommitted data depend this tx_data
     * so we must concern src_ls's tablet boundary to stop recycle tx_data
     */
    if (!user_data.transfer_scn_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer_scn is invalid", K(ret), K(user_data));
    } else {
      min_end_scn = SCN::scn_dec(user_data.transfer_scn_);
    }
  } else {
    ObITable *first_minor_mini_sstable =
        table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(false /*is_last*/);

    SCN end_scn = SCN::max_scn();
    if (OB_NOT_NULL(first_minor_mini_sstable)) {
      // step 1 : get end_scn if minor/mini sstable exist
      end_scn = first_minor_mini_sstable->get_end_scn();
    } else if (is_old) {
      // step 2 :
      /* If an old tablet has no minor sstable, it means that all the data inside it has been assigned version numbers,
         and therefore it does not depend on trx data.
         Thus, it's only necessary to focus on the recycle scn provided by the latest tablet. */
    } else {
      // step 3 : if minor sstable do not exist, us max{tablet_clog_checkpoint, ls_clog_checkpoint} as end_scn
      end_scn = SCN::max(tablet->get_tablet_meta().clog_checkpoint_scn_, ls_checkpoint);
      // the clog with scn of checkpoint scn may depend on the tx data with a commit scn of checkpoint scn
      end_scn = SCN::max(SCN::scn_dec(end_scn), SCN::min_scn());
    }

    if (end_scn < min_end_scn) {
      min_end_scn = end_scn;
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::get_min_mds_ckpt_scn(const ObTabletMapKey &key, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  SCN min_scn_from_old_tablets = SCN::max_scn();
  SCN min_scn_from_cur_tablet = SCN::max_scn();
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetMinMdsScn"));
  ObTabletHandle handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(get_tablet_with_allocator(WashTabletPriority::WTP_LOW, key, allocator, handle))) {
    LOG_WARN("fail to get latest tablet", K(ret), K(key));
  } else {
    // since cur_tablet may be added into old_chain, we must get it at first
    min_scn_from_cur_tablet = handle.get_obj()->get_tablet_meta().mds_checkpoint_scn_;
    const ObTabletPointerHandle &ptr_handle = handle.get_obj()->get_pointer_handle();
    ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(ptr_handle.get_resource_ptr());
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.hash()); // lock old_version_chain
    if (OB_ISNULL(tablet_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet ptr is NULL", K(ret), K(ptr_handle));
    } else if (OB_FAIL(tablet_ptr->get_min_mds_ckpt_scn(min_scn_from_old_tablets))) {
      LOG_WARN("fail to get min mds ckpt scn from old tablets", K(ret), K(key));
    } else {
      scn = MIN(min_scn_from_cur_tablet, min_scn_from_old_tablets);
      LOG_TRACE("get min mds ckpt scn", K(ret), K(key),
                K(scn), K(min_scn_from_cur_tablet), K(min_scn_from_old_tablets));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_ddl_kv(ObDDLKVHandle &handle)
{
  int ret = OB_SUCCESS;
  ObDDLKV *ddl_kv = nullptr;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(ddl_kv_pool_.acquire(ddl_kv))) {
    LOG_WARN("fail to acquire ddl kv object", K(ret));
  } else if (OB_FAIL(handle.set_obj(ddl_kv))) {
    LOG_WARN("fail to set table", K(ret), KP(ddl_kv));
  } else {
    ddl_kv = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(ddl_kv)) {
      release_ddl_kv(ddl_kv);
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::release_ddl_kv(ObDDLKV *ddl_kv)
{
  if (OB_NOT_NULL(ddl_kv)) {
    if (0 != ddl_kv->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ddl kv reference count may be leak", KPC(ddl_kv));
    } else {
      ddl_kv_pool_.release(ddl_kv);
    }
  }
}

int ObTenantMetaMemMgr::acquire_direct_load_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObDDLKV *ddl_kv = nullptr;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(ddl_kv_pool_.acquire(ddl_kv))) {
    STORAGE_LOG(WARN, "fail to acquire ddl kv object", K(ret));
  } else {
    handle.set_table(ddl_kv, this, ObITable::TableType::DIRECT_LOAD_MEMTABLE);
    ddl_kv = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(ddl_kv)) {
      release_ddl_kv(ddl_kv);
    }
  }
  return ret;

}
int ObTenantMetaMemMgr::acquire_data_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObMemtable *memtable = nullptr;

  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(memtable_pool_.acquire(memtable))) {
    LOG_WARN("fail to acquire memtable object", K(ret));
  } else {
    handle.set_table(memtable, this, ObITable::TableType::DATA_MEMTABLE);
    memtable = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(memtable)) {
      release_memtable(memtable);
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tx_data_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObTxDataMemtable *tx_data_memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else if (OB_FAIL(tx_data_memtable_pool_.acquire(tx_data_memtable))) {
    LOG_WARN("fail to acquire tx data memtable object", K(ret));
  } else {
    handle.set_table(tx_data_memtable, this, ObITable::TableType::TX_DATA_MEMTABLE);
    tx_data_memtable = nullptr;
  }

  if (OB_SUCCESS != ret) {
    handle.reset();
    if (OB_NOT_NULL(tx_data_memtable)) {
      release_tx_data_memtable_(tx_data_memtable);
    }
  }
  return  ret;
}

int ObTenantMetaMemMgr::acquire_tx_ctx_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObTxCtxMemtable *tx_ctx_memtable = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else if (OB_FAIL(tx_ctx_memtable_pool_.acquire(tx_ctx_memtable))) {
    LOG_WARN("fail to acquire tx ctx memtable object", K(ret));
  } else {
    handle.set_table(tx_ctx_memtable, this, ObITable::TableType::TX_CTX_MEMTABLE);
    tx_ctx_memtable = nullptr;
  }

  if (OB_SUCCESS != ret) {
    handle.reset();
    if (OB_NOT_NULL(tx_ctx_memtable)) {
      release_tx_ctx_memtable_(tx_ctx_memtable);
    }
  }
  return  ret;
}

int ObTenantMetaMemMgr::acquire_lock_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else if (OB_FAIL(lock_memtable_pool_.acquire(memtable))) {
    LOG_WARN("fail to acquire lock memtable object", K(ret));
  } else {
    handle.set_table(memtable, this, ObITable::TableType::LOCK_MEMTABLE);
    memtable = nullptr;
  }

  if (OB_SUCCESS != ret) {
    handle.reset();
    if (OB_NOT_NULL(memtable)) {
      release_lock_memtable_(memtable);
    }
  }
  return  ret;
}

void ObTenantMetaMemMgr::release_memtable(ObMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObSSTable reference count may be leak", KPC(memtable));
    } else {
      memtable_pool_.release(memtable);
    }
  }
}

void ObTenantMetaMemMgr::release_tx_data_memtable_(ObTxDataMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObTxDataMemtable reference count may be leak", KP(memtable));
    } else {
      tx_data_memtable_pool_.release(memtable);
    }
  }
}

int ObTenantMetaMemMgr::acquire_tablet_ddl_kv_mgr(ObDDLKvMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  ObMetaObj<ObTabletDDLKvMgr> meta_obj;
  meta_obj.pool_ = &tablet_ddl_kv_mgr_pool_;
  meta_obj.t3m_ = this;

  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(tablet_ddl_kv_mgr_pool_.acquire(meta_obj.ptr_))) {
    LOG_WARN("fail to acquire ddl kv mgr object", K(ret));
  } else if (OB_ISNULL(meta_obj.ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv mgr is nullptr", K(ret), K(meta_obj));
  } else {
    handle.set_obj(meta_obj);
    meta_obj.ptr_ = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(meta_obj.ptr_)) {
      release_tablet_ddl_kv_mgr(meta_obj.ptr_);
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::release_tablet_ddl_kv_mgr(ObTabletDDLKvMgr *ddl_kv_mgr)
{
  if (OB_NOT_NULL(ddl_kv_mgr)) {
    if (0 != ddl_kv_mgr->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObTabletDDLKvMgr reference count may be leak", KPC(ddl_kv_mgr));
    } else {
      tablet_ddl_kv_mgr_pool_.release(ddl_kv_mgr);
    }
  }
}

void ObTenantMetaMemMgr::release_tx_ctx_memtable_(ObTxCtxMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObTxCtxMemtable reference count may be leak", KPC(memtable));
    } else {
      tx_ctx_memtable_pool_.release(memtable);
    }
  }
}

void ObTenantMetaMemMgr::release_lock_memtable_(ObLockMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObLockMemtable reference count may be leak", KPC(memtable));
    } else {
      lock_memtable_pool_.release(memtable);
    }
  }
}

void ObTenantMetaMemMgr::mark_mds_table_deleted_(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  ObTabletPointerHandle ptr_handle(tablet_map_);
  if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // do nothing
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to get pointer from map", KR(ret), K(key), "resource_ptr", ptr_handle.get_resource_ptr());
    }
  } else if (OB_ISNULL(ptr_handle.get_resource_ptr())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ptr_handle is null", KPC(ptr_handle.get_resource_ptr()));
  } else {
    ObTabletPointer *tablet_pointer = static_cast<ObTabletPointer *>(ptr_handle.get_resource_ptr());
    tablet_pointer->mark_mds_table_deleted();
  }
}

int ObTenantMetaMemMgr::release_memtable_and_mds_table_for_ls_offline(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  ObTabletPointerHandle ptr_handle(tablet_map_);
  if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to get pointer from map", KR(ret), K(key), "resource_ptr", ptr_handle.get_resource_ptr());
    }
  } else if (OB_ISNULL(ptr_handle.get_resource_ptr())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ptr_handle is null", KPC(ptr_handle.get_resource_ptr()));
  } else {
    ObTabletPointer *tablet_pointer = static_cast<ObTabletPointer *>(ptr_handle.get_resource_ptr());
    if (OB_FAIL(tablet_pointer->release_memtable_and_mds_table_for_ls_offline(key.tablet_id_))) {
      LOG_WARN("failed to release memtable and mds table", K(ret), K(key));
    }
  }

  return ret;
}

int ObTenantMetaMemMgr::create_tmp_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  tablet_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTablet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), KP(buf));
  } else {
    tablet_handle.set_obj(new (buf) ObTablet(), &allocator, this);
    tablet_handle.set_wash_priority(priority);
    tablet_handle.get_obj()->set_allocator(&allocator);
    tablet_handle.disallow_copy_and_assign();
    bool is_exist = false;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (is_exist) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("This tablet pointer has exist, and don't create again", K(ret), K(key), K(is_exist));
    } else if (OB_FAIL(create_tablet(key, ls_handle, tablet_handle))) {
      LOG_WARN("fail to create tablet in map", K(ret), K(key), K(tablet_handle));
    }
  }
  if (OB_FAIL(ret)) {
    tablet_handle.reset();
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tablet_from_pool(
    const ObTabletPoolType &type,
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || type >= ObTabletPoolType::TP_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(type));
  } else if (OB_FAIL(acquire_tablet(type, tablet_handle))) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret) {
      LOG_WARN("fail to acquire tablet", K(ret), K(type));
    }
  } else {
    tablet_handle.set_wash_priority(priority);
    LOG_DEBUG("acquire tablet from pool", K(tablet_handle));
    bool is_exist = false;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (is_exist) {
      ObTabletPointerHandle ptr_handle(tablet_map_);
      if (OB_FAIL(tablet_map_.get_attr_for_obj(key, tablet_handle))) {
        LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
        LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
        LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("The tablet pointer isn't exist, don't support to acquire", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    tablet_handle.reset();
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tablet(
    const ObTabletPoolType type,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  TabletBufferList *header = nullptr;
  ObMetaObj<ObTablet> meta_obj;
  void *buf = nullptr;
  meta_obj.t3m_ = this;
  if (ObTabletPoolType::TP_NORMAL == type) {
    meta_obj.pool_ = static_cast<ObITenantMetaObjPool *>(&tablet_buffer_pool_);
    header = &normal_tablet_header_;
  } else if (ObTabletPoolType::TP_LARGE == type) {
    meta_obj.pool_ = static_cast<ObITenantMetaObjPool *>(&large_tablet_buffer_pool_);
    header = &large_tablet_header_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to wash", K(ret), K(type));
  }
  if (FAILEDx(meta_obj.pool_->alloc_obj(buf))) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret) {
      LOG_WARN("fail to acquire tablet buffer", K(ret));
    }
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet buffer is nullptr", K(ret), KP(buf));
  } else {
    ObMetaObjBufferHelper::new_meta_obj(buf, meta_obj.ptr_);
    tablet_handle.set_obj(meta_obj);
    SpinWLockGuard guard(wash_lock_);
    if (OB_UNLIKELY(!header->add_last(static_cast<ObMetaObjBufferNode *>(buf)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add last normal tablet", K(ret), KP(buf));
    }
    LOG_DEBUG("acquire tablet", K(type), K(meta_obj));
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tablet(ObITenantMetaObjPool *pool, ObTablet *&tablet)
{
  int ret = OB_SUCCESS;
  TabletBufferList *header = nullptr;
  void *buf = nullptr;
  tablet = nullptr;
  if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(pool));
  } else if (static_cast<ObTenantMetaObjPool<ObNormalTabletBuffer> *>(pool) == &tablet_buffer_pool_) {
    header = &normal_tablet_header_;
  } else if (static_cast<ObTenantMetaObjPool<ObLargeTabletBuffer> *>(pool) == &large_tablet_buffer_pool_) {
    header = &large_tablet_header_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to wash", K(ret), KP(pool));
  }
  if (FAILEDx(pool->alloc_obj(buf))) {
    LOG_WARN("fail to acquire tablet buffer", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet buffer is nullptr", K(ret), KP(buf));
  } else {
    ObMetaObjBufferHelper::new_meta_obj(buf, tablet);
    SpinWLockGuard guard(wash_lock_);
    if (OB_UNLIKELY(!header->add_last(static_cast<ObMetaObjBufferNode *>(buf)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add last normal tablet", K(ret), KP(buf));
    }
    LOG_DEBUG("acquire tablet", KP(pool), KP(tablet));
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tmp_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  tablet_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTablet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), KP(buf));
  } else {
    tablet_handle.set_obj(new (buf) ObTablet(), &allocator, this);
    tablet_handle.set_wash_priority(priority);
    tablet_handle.get_obj()->set_allocator(&allocator);
    tablet_handle.disallow_copy_and_assign();
    bool is_exist = false;
    CLICK();
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (CLICK_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (is_exist) {
      ObTabletPointerHandle ptr_handle(tablet_map_);
      if (CLICK_FAIL(tablet_map_.get_attr_for_obj(key, tablet_handle))) {
        LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
      } else if (CLICK_FAIL(tablet_map_.get(key, ptr_handle))) {
        LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
      } else if (CLICK_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
        LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("The tablet pointer doesn't exist, not supported to acquire", K(ret), K(key));
    }
  }
  if (CLICK_FAIL(ret)) {
    tablet_handle.reset();
  }
  return ret;
}

int ObTenantMetaMemMgr::create_msd_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(full_tablet_creator_.create_tablet(tablet_handle))) {
    LOG_WARN("fail to create tablet", K(ret), K(key), K(tablet_handle));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tablet handle isn't valid", K(ret), K(tablet_handle));
  } else {
    tablet_handle.set_wash_priority(priority);
    bool is_exist = false;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (OB_UNLIKELY(is_exist)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("This tablet pointer has exist, and don't create again", K(ret), K(key), K(is_exist));
    } else if (OB_FAIL(create_tablet(key, ls_handle, tablet_handle))) {
      LOG_WARN("fail to create tablet in map", K(ret), K(key), K(tablet_handle));
    }
  }
  if (OB_FAIL(ret)) {
    tablet_handle.reset();
  }
  return ret;
}

int ObTenantMetaMemMgr::create_tablet(
    const ObTabletMapKey &key,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMemtableMgrHandle memtable_mgr_hdl;
  ObLS *ls = ls_handle.get_ls();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !ls_handle.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(ls_handle), K(tablet_handle));
  } else if (key.tablet_id_.is_ls_tx_data_tablet()) {
    if (OB_FAIL(ls->get_tablet_svr()->get_tx_data_memtable_mgr(memtable_mgr_hdl))) {
      LOG_WARN("fail to get tx data memtable mgr", K(ret));
    }
  } else if (key.tablet_id_.is_ls_tx_ctx_tablet()) {
    if (OB_FAIL(ls->get_tablet_svr()->get_tx_ctx_memtable_mgr(memtable_mgr_hdl))) {
      LOG_WARN("fail to get tx data memtable mgr", K(ret));
    }
  } else if (key.tablet_id_.is_ls_lock_tablet()) {
    if (OB_FAIL(ls->get_tablet_svr()->get_lock_memtable_mgr(memtable_mgr_hdl))) {
      LOG_WARN("fail to get lock memtable mgr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObTabletPointerHandle ptr_handle(tablet_map_);
    ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
    ObMetaDiskAddr addr;
    addr.set_none_addr();
    tablet_ptr.set_addr_with_reset_obj(addr);
    // After restarting and replaying the slog, the tablet will load the pool when it is accessed for the first time.
    tablet_ptr.obj_.pool_ = &tablet_buffer_pool_;
    if (OB_FAIL(tablet_map_.set(key, tablet_ptr))) {
      LOG_WARN("fail to set tablet pointer", K(ret), K(key));
    } else if (OB_FAIL(tablet_map_.get_attr_for_obj(key, tablet_handle))) {
      LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
      LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
      LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObTabletHandle handle;
      if (OB_TMP_FAIL(tablet_map_.erase(key, handle))) {
        LOG_WARN("fail to erase tablet pointer", K(tmp_ret), K(key));
      }
      handle.set_wash_priority(WashTabletPriority::WTP_LOW);
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.get_meta_obj(key, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_ITEM_NOT_SETTED != ret) {
      LOG_WARN("fail to get tablet", K(ret), K(key));
    }
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(key), K(handle));
  } else {
    handle.set_wash_priority(priority);
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet_with_filter(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObITabletFilterOp &op,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.get_meta_obj_with_filter(key, op, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_ITEM_NOT_SETTED != ret && OB_NOT_THE_OBJECT != ret) {
      LOG_WARN("fail to get tablet", K(ret), K(key));
    }
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(key), K(handle));
  } else {
    handle.set_wash_priority(priority);
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet_with_allocator(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle,
    const bool force_alloc_new)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), KP(&allocator));
  } else if (OB_FAIL(tablet_map_.get_meta_obj_with_external_memory(key, allocator, handle, force_alloc_new, nullptr/*no_op*/))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get tablet", K(ret), K(key));
    }
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(key), K(handle));
  } else {
    handle.set_wash_priority(priority);
    if (&allocator == handle.get_allocator()) {
      handle.disallow_copy_and_assign();
      handle.get_obj()->set_allocator(handle.get_allocator());
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::build_tablet_handle_for_mds_scan(
    ObTablet *tablet,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObMetaObj<ObTablet> meta_obj;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, tablet is nullptr", K(ret), KP(tablet));
  } else if (nullptr == tablet->get_allocator()) {
    ObTabletPoolType type;
    ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char *>(tablet));
    if (OB_FAIL(get_tablet_pool_type(buf_header.buf_len_, type))) {
      LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header.buf_len_));
    } else if (ObTabletPoolType::TP_NORMAL == type) {
      meta_obj.pool_ = static_cast<ObITenantMetaObjPool *>(&tablet_buffer_pool_);
    } else if (ObTabletPoolType::TP_LARGE == type) {
      meta_obj.pool_ = static_cast<ObITenantMetaObjPool *>(&large_tablet_buffer_pool_);
    }
  }
  if (OB_SUCC(ret)) {
    meta_obj.t3m_ = this;
    meta_obj.allocator_ = tablet->get_allocator();
    meta_obj.ptr_ = tablet;
    handle.set_obj(meta_obj);
    handle.set_wash_priority(WashTabletPriority::WTP_HIGH);
  }
  return ret;
}


int ObTenantMetaMemMgr::get_tablet_buffer_infos(ObIArray<ObTabletBufferInfo> &buffer_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else {
    SpinRLockGuard guard(wash_lock_);
    const int64_t size = normal_tablet_header_.get_size() + large_tablet_header_.get_size();
    if (OB_FAIL(buffer_infos.reserve(size))) {
      LOG_WARN("fail to reserve memory for buffer_infos", K(ret), K(size));
    } else if (OB_FAIL(fill_buffer_infos(
        ObTabletPoolType::TP_NORMAL, normal_tablet_header_.get_header(), buffer_infos))) {
      LOG_WARN("fail to fill normal buffer infos", K(ret), KP(normal_tablet_header_.get_first()));
    } else if (OB_FAIL(fill_buffer_infos(
        ObTabletPoolType::TP_LARGE, large_tablet_header_.get_header(), buffer_infos))) {
      LOG_WARN("fail to fill large buffer infos", K(ret), KP(large_tablet_header_.get_header()));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::fill_buffer_infos(
    const ObTabletPoolType pool_type,
    ObMetaObjBufferNode *tablet_buffer_node,
    ObIArray<ObTabletBufferInfo> &buffer_infos)
{
  int ret = OB_SUCCESS;
  ObMetaObjBufferNode *cur_node = tablet_buffer_node->get_next();
  while (OB_SUCC(ret) && cur_node != tablet_buffer_node) {
    ObTabletBufferInfo buffer_info;
    if (OB_FAIL(buffer_info.fill_info(pool_type, cur_node))) {
      LOG_WARN("fail to fill tablet buffer info", K(ret), K(pool_type), KP(tablet_buffer_node), KP(cur_node));
    } else if (OB_FAIL(buffer_infos.push_back(buffer_info))) {
      LOG_WARN("fail to push back buffer info", K(ret), K(buffer_info));
    } else {
      cur_node = cur_node->get_next();
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.get_meta_addr(key, addr))) {
    LOG_WARN("failed to get tablet addr", K(ret), K(key));
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet_pointer_initial_state(const ObTabletMapKey &key, bool &initial_state)
{
  int ret = OB_SUCCESS;
  ObTabletPointerHandle ptr_handle(tablet_map_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
    LOG_WARN("failed to get ptr handle", K(ret), K(key));
  } else {
    ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(ptr_handle.get_resource_ptr());
    if (OB_ISNULL(tablet_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet ptr is NULL", K(ret), K(ptr_handle));
    } else {
      initial_state = tablet_ptr->get_initial_state();
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::get_tablet_ddl_kv_mgr(
    const ObTabletMapKey &key,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObTabletPointerHandle ptr_handle(tablet_map_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
    LOG_WARN("failed to get ptr handle", K(ret), K(key));
  } else {
    ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(ptr_handle.get_resource_ptr());
    if (OB_ISNULL(tablet_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet ptr is NULL", K(ret), K(ptr_handle));
    } else {
      tablet_ptr->get_ddl_kv_mgr(ddl_kv_mgr_handle);
      if (!ddl_kv_mgr_handle.is_valid()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_DEBUG("ddl kv mgr not exist", K(ret), K(ddl_kv_mgr_handle));
      }
    }
  }
  return ret;
}

int64_t ObTenantMetaMemMgr::cal_adaptive_bucket_num()
{
  const int64_t mem_limit = get_tenant_memory_limit(tenant_id_);
  const int64_t min_bkt_cnt = DEFAULT_BUCKET_NUM;
  const int64_t max_bkt_cnt = 1000000L;
  const int64_t tablet_bucket_num = std::min(std::max((mem_limit / (1024 * 1024 * 1024)) * 50000, min_bkt_cnt), max_bkt_cnt);
  const int64_t bucket_num = common::hash::cal_next_prime(tablet_bucket_num);
  LOG_INFO("cal adaptive bucket num", K(mem_limit), K(min_bkt_cnt), K(max_bkt_cnt), K(tablet_bucket_num), K(bucket_num));
  return bucket_num;
}

int ObTenantMetaMemMgr::get_meta_mem_status(common::ObIArray<ObTenantMetaMemStatus> &info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_obj_pool_info(memtable_pool_, "MEMTABLE POOL", info))) {
    LOG_WARN("fail to get memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(ddl_kv_pool_, "DDL KV POOL", info))) {
    LOG_WARN("fail to get ddl kv pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tablet_buffer_pool_, "NORMAL TABLET BUF POOL", info))) {
    LOG_WARN("fail to get tablet pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(large_tablet_buffer_pool_, "LARGE TABLET BUF POOL", info))) {
    LOG_WARN("fail to get large tablet pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tablet_ddl_kv_mgr_pool_, "KV MGR POOL", info))) {
    LOG_WARN("fail to get kv mgr pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tx_data_memtable_pool_, "TX DATA MEMTABLE POOL", info))) {
    LOG_WARN("fail to get tx data memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tx_ctx_memtable_pool_, "TX CTX MEMTABLE POOL", info))) {
    LOG_WARN("fail to get tx ctx memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(lock_memtable_pool_, "LOCK MEMTABLE POOL", info))) {
    LOG_WARN("fail to get lock memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_full_tablets_info(full_tablet_creator_, "MSTX TABLET ALLOCATOR", info))) {
    LOG_WARN("fail to get mstx tablet allocator", K(ret), K(info));
  }
  return ret;
}

int ObTenantMetaMemMgr::register_into_tb_map(const char *file, const int line,
                                             const char *func, int32_t &index) {
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  if (OB_NOT_NULL(t3m) && !t3m->is_tablet_handle_leak_checker_enabled()) {
    // do nothing
  } else if (OB_FAIL(ObTabletHandleIndexMap::get_instance()->register_handle(file, line, func, index))) {
    LOG_WARN("failed to register handle", KP(t3m), K(file), K(line), K(func), K(index));
  }

  return ret;
}

int ObTenantMetaMemMgr::inc_ref_in_leak_checker(const int32_t index)
{
  int ret = OB_SUCCESS;

  if (!is_tablet_handle_leak_checker_enabled()) {
    // do nothing
  } else if (OB_FAIL(leak_checker_.inc_ref(index))) {
    LOG_WARN("failed to inc ref in leak checker", K(index));
  }

  return ret;
}

int ObTenantMetaMemMgr::dec_ref_in_leak_checker(const int32_t index)
{
  int ret = OB_SUCCESS;

  if (!is_tablet_handle_leak_checker_enabled()) {
    // do nothing
  } else if (OB_FAIL(leak_checker_.dec_ref(index))) {
    LOG_WARN("failed to dec ref in leak checker", K(ret), K(index));
  }

  return ret;
}

bool ObTenantMetaMemMgr::is_tablet_handle_leak_checker_enabled()
{
  return is_tablet_leak_checker_enabled_;
}

int ObTenantMetaMemMgr::del_tablet(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("prepare to delete tablet", K(key));
  // use tmp_handle to ensure the tablet is finally released by handle,
  // since ObMetaObjGuard && ObMetaObj is not adapted to gc_tablet()
  ObTabletHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    // unregister step need ahead of erase, cause a new tablet maybe created after erase but before unregister
    // and that new tablet's register step will failed
    mark_mds_table_deleted_(key);
    if (OB_FAIL(tablet_map_.erase(key, handle))) {
      LOG_WARN("fail to erase tablet pointer", K(ret), K(key));
    } else {
      if (OB_NOT_NULL(handle.get_obj()) && OB_ISNULL(handle.get_obj()->get_allocator())) {
        ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(handle.get_obj()), false/*in_map*/);
      }
      LOG_DEBUG("succeed to delete tablet", K(ret), K(key));
    }
    handle.set_wash_priority(WashTabletPriority::WTP_LOW);
  }
  return ret;
}

int ObTenantMetaMemMgr::compare_and_swap_tablet(
    const ObTabletMapKey &key,
    const ObTabletHandle &old_handle,
    const ObTabletHandle &new_handle,
    const ObUpdateTabletPointerParam &update_pointer_param)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  const ObMetaDiskAddr &new_addr = new_handle.get_obj()->get_tablet_addr();
  const ObTablet *old_tablet = old_handle.get_obj();
  const ObTablet *new_tablet = new_handle.get_obj();

#ifdef ERRSIM
  ErrsimModuleGuard guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
#endif

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid()
                      || !new_addr.is_valid()
                      || !new_handle.is_valid()
                      || !old_handle.is_valid()
                      || new_handle.is_tmp_tablet()
                      || !update_pointer_param.is_valid()
                      )) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(new_addr), K(old_handle), K(new_handle), K(update_pointer_param));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (CLICK_FAIL(tablet_map_.compare_and_swap_addr_and_object(key, old_handle, new_handle, update_pointer_param))) {
      LOG_WARN("fail to compare and swap tablet", K(ret), K(key), K(old_handle), K(new_handle), K(update_pointer_param));
    } else if (CLICK_FAIL(update_tablet_buffer_header(old_handle.get_obj(), new_handle.get_obj()))) {
      LOG_WARN("fail to update tablet buffer header", K(ret), K(old_handle), K(new_handle));
    } else if (old_handle.get_obj() != new_handle.get_obj()) { // skip first init, old_handle == new_handle
      // TODO yunshan.tys update min minor sstable by link
      const ObTabletPointerHandle &ptr_hdl = old_handle.get_obj()->get_pointer_handle();
      ObTabletPointer *t_ptr = nullptr;
      if (OB_ISNULL(t_ptr = reinterpret_cast<ObTabletPointer *>(ptr_hdl.get_resource_ptr()))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet pointer", K(ret), K(key), K(ptr_hdl));
      } else if (CLICK_FAIL(t_ptr->add_tablet_to_old_version_chain(old_handle.get_obj()))) {
        LOG_WARN("fail to add tablet to old version chain", K(ret), K(key), KPC(old_tablet));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    // TODO(@bowen.gbw): Currently, the order is:
    // 1. CAS operation, let the newest tablet be visible
    // 2. check and set initial state on tablet pointer
    // But upper layer may get tablet between step 1 and 2, which will cause that upper layer cannot
    // read the newest initial state.
    // Maybe we should let the two steps, CAS opereation and set initial state, be an atomic operation.
    // The same issue exists on all 4.x version, and should be solved in future.
    if (OB_FAIL(new_handle.get_obj()->check_and_set_initial_state())) {
      LOG_WARN("failed to check and set initial state", K(ret), K(key));
    }
  }
  LOG_DEBUG("compare and swap object", K(ret), KPC(new_handle.get_obj()), K(lbt()));
  return ret;
}

int ObTenantMetaMemMgr::compare_and_swap_tablet(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const ObTabletPoolType &pool_type,
    const bool set_pool /* whether to set tablet pool */)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !old_addr.is_valid() || !(new_addr.is_disked()
      || new_addr.is_memory()) || (set_pool && ObTabletPoolType::TP_MAX == pool_type))) {
    ret = OB_INVALID_ARGUMENT;
    FLOG_WARN("invalid argument", K(ret), K(key), K(old_addr), K(new_addr), K(set_pool), K(pool_type));
  } else {
    ObITenantMetaObjPool *pool = nullptr;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet is exist", K(ret), K(key));
    } else if (OB_UNLIKELY(!is_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("this tablet isn't exist in map", K(ret), K(key), K(is_exist));
    } else if (set_pool) {
      if (ObTabletPoolType::TP_NORMAL == pool_type) {
        pool = &tablet_buffer_pool_;
      } else if (ObTabletPoolType::TP_LARGE == pool_type) {
        pool = &large_tablet_buffer_pool_;
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(tablet_map_.compare_and_swap_address_without_object(key,
                                                                           old_addr,
                                                                           new_addr,
                                                                           set_pool,
                                                                           pool))) {
      LOG_WARN("fail to compare and swap tablet address in map", K(ret), K(key), K(old_addr),
          K(new_addr), K(set_pool), KP(pool));
    }
  }
  LOG_DEBUG("compare and swap object", K(ret), K(old_addr), K(new_addr), K(lbt()));
  return ret;
}

void ObTenantMetaMemMgr::release_tablet_from_pool(ObTablet *tablet, const bool give_back_buf_into_pool)
{
  if (OB_NOT_NULL(tablet)) {
    ObMetaObjBufferNode &linked_node = ObMetaObjBufferHelper::get_linked_node(reinterpret_cast<char *>(tablet));
    ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char *>(tablet));
    void *buf = ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char *>(tablet));
    LOG_DEBUG("release tablet", K(buf_header), KP(buf), KP(tablet));
    SpinWLockGuard guard(wash_lock_);
    if (0 != tablet->get_ref()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObTablet reference count may be leak", KP(tablet));
    } else if (NORMAL_TABLET_POOL_SIZE == buf_header.buf_len_) {
      normal_tablet_header_.remove(&linked_node);
      if (give_back_buf_into_pool) {
        tablet_buffer_pool_.free_obj(buf);
      }
    } else if (LARGE_TABLET_POOL_SIZE == buf_header.buf_len_) {
      large_tablet_header_.remove(&linked_node);
      if (give_back_buf_into_pool) {
        large_tablet_buffer_pool_.free_obj(buf);
      }
    }
  }
}

int ObTenantMetaMemMgr::has_tablet(const ObTabletMapKey &key, bool &is_exist)
{
  int ret = OB_SUCCESS;

  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.exist(key, is_exist))) {
    LOG_WARN("fail to check tablet exist", K(ret), K(key));
  }
  return ret;
}

int ObTenantMetaMemMgr::check_all_meta_mem_released(bool &is_released, const char *module)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else {
    const int64_t memtable_cnt = memtable_pool_.get_used_obj_cnt();
    const int64_t ddl_kv_cnt = ddl_kv_pool_.get_used_obj_cnt();
    const int64_t tablet_cnt = tablet_buffer_pool_.get_used_obj_cnt();
    const int64_t large_tablet_cnt = large_tablet_buffer_pool_.get_used_obj_cnt();
    const int64_t ddl_kv_mgr_cnt = tablet_ddl_kv_mgr_pool_.get_used_obj_cnt();
    const int64_t tx_data_memtable_cnt_ = tx_data_memtable_pool_.get_used_obj_cnt();
    const int64_t tx_ctx_memtable_cnt_ = tx_ctx_memtable_pool_.get_used_obj_cnt();
    const int64_t lock_memtable_cnt_ = lock_memtable_pool_.get_used_obj_cnt();
    const int64_t full_tablet_cnt = full_tablet_creator_.get_used_obj_cnt();
    if (memtable_cnt != 0 || ddl_kv_cnt != 0 || tablet_cnt != 0 || 0 != large_tablet_cnt || ddl_kv_mgr_cnt != 0
        || tx_data_memtable_cnt_ != 0 || tx_ctx_memtable_cnt_ != 0
        || lock_memtable_cnt_ != 0 || full_tablet_cnt != 0) {
      is_released = false;
    } else {
      is_released = true;
    }
    const int64_t wait_gc_tablets_cnt = tablet_gc_queue_.count();
    const int64_t wait_gc_tables_cnt = free_tables_queue_.size();
    const int64_t tablet_cnt_in_map = tablet_map_.count();
    LOG_INFO("check all meta mem in t3m", K(module), K(is_released), K(memtable_cnt), K(ddl_kv_cnt),
        K(tablet_cnt), K(large_tablet_cnt), K(ddl_kv_mgr_cnt), K(tx_data_memtable_cnt_),
        K(tx_ctx_memtable_cnt_), K(lock_memtable_cnt_), K(full_tablet_cnt),
        K(wait_gc_tablets_cnt), K(wait_gc_tables_cnt), K(tablet_cnt_in_map));
  }
  return ret;
}

int ObTenantMetaMemMgr::dump_tablet_info()
{
  int ret = OB_SUCCESS;
  TabletMapDumpOperator op;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(tablet_map_.for_each_value_store(op))) {
    LOG_WARN("fail to traverse tablet map", K(ret));
  } else {
    SpinWLockGuard guard(wash_lock_);
    for (ObMetaObjBufferNode *node = normal_tablet_header_.get_first();
         node != normal_tablet_header_.get_header(); node = node->get_next()) {
      FLOG_INFO("dump normal tablet buffer", "buffer", static_cast<const void*>(ObMetaObjBufferHelper::get_obj_buffer(node)), KP(node));
    }
    for (ObMetaObjBufferNode *node = large_tablet_header_.get_first();
         node != large_tablet_header_.get_header(); node = node->get_next()) {
      FLOG_INFO("dump large tablet buffer", "buffer", static_cast<const void*>(ObMetaObjBufferHelper::get_obj_buffer(node)), KP(node));
    }
  }

  if (is_tablet_handle_leak_checker_enabled()) {
    leak_checker_.dump_pinned_tablet_info();
  }

  return ret;
}

int ObTenantMetaMemMgr::ObT3MResourceLimitCalculatorHandler::
    get_current_info(share::ObResourceInfo &info)
{
  int ret = OB_SUCCESS;
  ObResoureConstraintValue constraint_value;
  if (!t3m_.is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m not inited, the resource info may not right.", K(ret));
  } else if (OB_FAIL(get_resource_constraint_value(constraint_value))) {
    LOG_WARN("get resource constraint value failed", K(ret));
  } else {
    info.curr_utilization_ = t3m_.tablet_map_.count();
    info.max_utilization_ = t3m_.tablet_map_.max_count();
    info.reserved_value_ = 0;  // reserve value will be used later
    constraint_value.get_min_constraint(info.min_constraint_type_, info.min_constraint_value_);
  }
  return ret;
}

int ObTenantMetaMemMgr::ObT3MResourceLimitCalculatorHandler::
    get_resource_constraint_value(share::ObResoureConstraintValue &constraint_value)
{
  int ret = OB_SUCCESS;
  // Get tenant config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t config_tablet_per_gb = tenant_config.is_valid() ?
                                          tenant_config->_max_tablet_cnt_per_gb :
                                          DEFAULT_TABLET_CNT_PER_GB;
  const int64_t config_mem_percentage = tenant_config.is_valid() ?
                                          tenant_config->_storage_meta_memory_limit_percentage :
                                          OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT;
  const int64_t tenant_mem = lib::get_tenant_memory_limit(MTL_ID());
  // Calculate config constraint : (tenant_mem / 1GB) * config_tablet_per_gb
  const int64_t config_constraint = tenant_mem / (1.0 * 1024 * 1024 * 1024 /* 1GB */) * config_tablet_per_gb;
  // Calculate memory constraint : (tenant_mem * config_mem_percentage) / 200MB * 20000
  const int64_t memory_constraint = tenant_mem * (config_mem_percentage / 100.0) /
                                    (200.0 * 1024 * 1024 /* 200MB */) *
                                    DEFAULT_TABLET_CNT_PER_GB;
  // Set into constraint value
  if (OB_FAIL(constraint_value.set_type_value(CONFIGURATION_CONSTRAINT, config_constraint))) {
    LOG_WARN("set type value failed", K(ret), K(CONFIGURATION_CONSTRAINT),
             K(config_tablet_per_gb), K(tenant_mem), K(config_constraint));
  } else if (OB_FAIL(constraint_value.set_type_value(MEMORY_CONSTRAINT, memory_constraint))) {
    LOG_WARN("set type value failed", K(ret), K(MEMORY_CONSTRAINT),
             K(config_mem_percentage), K(tenant_mem), K(memory_constraint));
  }
  return ret;
}

int ObTenantMetaMemMgr::ObT3MResourceLimitCalculatorHandler::
    cal_min_phy_resource_needed(const int64_t num, share::ObMinPhyResourceResult &min_phy_res)
{
  int ret = OB_SUCCESS;
  int64_t cal_num = num >= 0 ? num : 0;  // We treat unexpected negative input numbers as zero.
  // Get tenant memory
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t config_tablet_per_gb = tenant_config.is_valid() ?
                                          tenant_config->_max_tablet_cnt_per_gb :
                                          DEFAULT_TABLET_CNT_PER_GB;
  const int64_t config_mem_percentage = tenant_config.is_valid() ?
                                          tenant_config->_storage_meta_memory_limit_percentage :
                                          OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT;
  // Inverse calculate through config formula and memory formula
  const int64_t memory_constraint_formula_inverse =
      cal_num * (200.0 * 1024 * 1024 /* 200MB */) / DEFAULT_TABLET_CNT_PER_GB / (config_mem_percentage / 100.0);
  const int64_t config_constraint_formula_inverse =
      cal_num * (1.0 * 1024 * 1024 * 1024 /* 1GB */) / config_tablet_per_gb;
  // Set into MinPhyResourceResult
  const int64_t minimum_physics_needed = std::max(memory_constraint_formula_inverse, config_constraint_formula_inverse);
  LOG_INFO("t3m resource limit calculator, cal_min_phy_resource_needed", K(num),
           K(cal_num), K(memory_constraint_formula_inverse),
           K(config_constraint_formula_inverse), K(minimum_physics_needed),
           K(config_tablet_per_gb), K(config_mem_percentage));
  if (OB_FAIL(min_phy_res.set_type_value(PHY_RESOURCE_MEMORY, minimum_physics_needed))) {
    LOG_WARN("set type value failed", K(PHY_RESOURCE_MEMORY),
             K(memory_constraint_formula_inverse),
             K(config_constraint_formula_inverse), K(minimum_physics_needed));
  }
  return ret;
}

int ObTenantMetaMemMgr::ObT3MResourceLimitCalculatorHandler::
    cal_min_phy_resource_needed(share::ObMinPhyResourceResult &min_phy_res) {
  int ret = OB_SUCCESS;
  // Get current tablet count
  const int64_t current_tablet_count = t3m_.tablet_map_.count();
  if (OB_FAIL(cal_min_phy_resource_needed(current_tablet_count, min_phy_res))) {
    LOG_WARN("cal_min_phy_resource_needed failed", K(ret), K(current_tablet_count));
  }
  return ret;
}

int ObTenantMetaMemMgr::TabletMapDumpOperator::operator()(common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> &entry)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey &key = entry.first;
  FLOG_INFO("dump tablet in map", K(key));
  return ret;
}

ObTenantMetaMemMgr::HeapCompare::HeapCompare(int &ret)
  : ret_(ret)
{
}

bool ObTenantMetaMemMgr::HeapCompare::operator() (
    const CandidateTabletInfo &left,
    const CandidateTabletInfo &right) const
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_)) {
    ret = ret_;
  } else {
    bret = left.wash_score_ > right.wash_score_;
  }
  ret_ = ret;
  return bret;
}

int ObTenantMetaMemMgr::get_wash_tablet_candidate(const std::type_info &type_info, CandidateTabletInfo &info)
{
  int ret = OB_SUCCESS;
  TabletBufferList *header = nullptr;
  ObMetaObjBufferNode *curr = nullptr;
  if (type_info == typeid(ObNormalTabletBuffer)) {
    header = &normal_tablet_header_;
  } else if (type_info == typeid(ObLargeTabletBuffer)) {
    header = &large_tablet_header_;
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported to wash", K(ret), "type:", type_info.name());
  }
  if (OB_SUCC(ret)) {
    CandidateTabletInfo min;
    min.wash_score_ = INT64_MAX;
    for (curr = header->get_first();
         OB_SUCC(ret) && curr != header->get_header() && curr != nullptr;
         curr = curr->get_next()) {
      if (curr->get_data().has_new_) {
        ObTablet *tablet = reinterpret_cast<ObTablet *>(ObMetaObjBufferHelper::get_obj_buffer(curr));
        const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
        const ObTabletMapKey tablet_key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
        if (1 == tablet->get_ref() && ObMetaObjBufferHelper::is_in_map(reinterpret_cast<char*>(tablet))) {
          // While hope to wash normal buffer pool, we need to skip rebuild next tablet, which may be oversub macro ref cnt.
          const bool need_skip = type_info == typeid(ObNormalTabletBuffer) && tablet->get_next_tablet_guard().is_valid();
          // this tablet is only hold by tablet map.
          CandidateTabletInfo candidate;
          candidate.ls_id_ = tablet_key.ls_id_.id();
          candidate.tablet_id_ = tablet_key.tablet_id_.id();
          candidate.wash_score_ = tablet->get_wash_score();
          LOG_DEBUG("get wash candidate", K(candidate));
          if (need_skip) {
            // just skip, nothing to do.
          } else if (candidate.wash_score_ < 0
              || (candidate.wash_score_ > 0 && (ObTimeUtility::current_time_ns() - candidate.wash_score_ > 1000000000))) {
            info = candidate;
            break;
          } else if (candidate.wash_score_ < min.wash_score_) {
            min = candidate;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (info.is_valid()) {
        // do nothing.
      } else if (!info.is_valid() && min.is_valid()) {
        info = min;
      } else {
        ret = OB_ITER_END;
        LOG_WARN("Don't find one candidate", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::do_wash_candidate_tablet(
    const CandidateTabletInfo &candidate,
    ObTabletHandle &handle,
    void *&free_obj)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(candidate.ls_id_);
  const ObTabletID tablet_id(candidate.tablet_id_);
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletPointerHandle ptr_handle(tablet_map_);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_NOT_NULL(handle.get_obj())) {
    if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
      LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(handle));
    } else if (OB_FAIL(handle.get_obj()->assign_pointer_handle(ptr_handle))) {
      LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
    }
  }
  if (FAILEDx(tablet_map_.wash_meta_obj(key, handle, free_obj))) {
    LOG_WARN("wash tablet obj fail", K(ret), K(key));
  }
  return ret;
}

int ObTenantMetaMemMgr::try_wash_tablet_from_gc_queue(
    const int64_t buf_len,
    TabletBufferList &header,
    void *&free_obj)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  const int64_t loop_cnt_limit = LARGE_TABLET_POOL_SIZE == buf_len ? 1 : INT64_MAX;
  int64_t loop_cnt = 0;
  free_obj = nullptr;
  while (loop_cnt < loop_cnt_limit
      && OB_ISNULL(free_obj)
      && OB_SUCC(ret)
      && OB_NOT_NULL(tablet = tablet_gc_queue_.pop())) {
    if (OB_UNLIKELY(tablet->get_ref() != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet in gc queue", K(ret), KPC(tablet));
    } else {
      if (OB_ISNULL(tablet->get_allocator())
          && buf_len == ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char *>(tablet)).buf_len_) {
        free_obj = release_tablet(tablet, true/*return tablet buffer ptr after release*/);
      } else {
        release_tablet(tablet, false/*return tablet buffer ptr after release*/);
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(inner_push_tablet_into_gc_queue(tablet))) {
        LOG_ERROR("fail to push tablet into gc queue, tablet may be leaked", K(tmp_ret), KP(tablet));
      }
      tablet = nullptr;
    }
    ++loop_cnt;
  }
  return ret;
}

int ObTenantMetaMemMgr::update_tablet_buffer_header(ObTablet *old_obj, ObTablet *new_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_obj) || OB_ISNULL(new_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_obj), KP(new_obj));
  } else if (old_obj == new_obj) {
    if (OB_ISNULL(new_obj->get_allocator())) { // from tablet buffer pool
      ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(new_obj), true/*in_map*/);
    }
  } else if (old_obj != new_obj) {
    if (OB_ISNULL(old_obj->get_allocator())) { // from tablet buffer pool
      ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(old_obj), false/*in_map*/);
    }
    if (OB_ISNULL(new_obj->get_allocator())) { // from tablet buffer pool
      ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(new_obj), true/*in_map*/);
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::try_wash_tablet(const std::type_info &type_info, void *&free_obj)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("try_wash_tablet", 1000 * 1000);
  ObNormalTabletBuffer *normal_buf = nullptr;
  ObTabletHandle tablet_handle;
  const bool is_large = typeid(ObLargeTabletBuffer) == type_info;
  const int64_t buf_len = is_large ? LARGE_TABLET_POOL_SIZE : NORMAL_TABLET_POOL_SIZE;
  TabletBufferList &header = is_large ? large_tablet_header_ : normal_tablet_header_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else if (OB_FAIL(try_wash_tablet_from_gc_queue(buf_len, header, free_obj))) {
    LOG_WARN("fail to try wash tablet from gc queue", K(ret), K(buf_len), K(header));
  } else if (FALSE_IT(time_guard.click("wash_queue"))) {
  } else if (OB_NOT_NULL(free_obj)) {
    LOG_INFO("succeed to wash tablet from gc queue", K(ret), KP(free_obj));
  } else if (is_large && OB_FAIL(acquire_tablet(ObTabletPoolType::TP_NORMAL, tablet_handle))) {
    LOG_WARN("fail to acquire tablet", K(ret));
  } else {
    tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "WashTablet"));
    CandidateTabletInfo info;
    time_guard.click("prepare");
    SpinWLockGuard guard(wash_lock_);
    time_guard.click("wait_lock");
    if (OB_FAIL(get_wash_tablet_candidate(type_info, info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get candidate tablet for wash", K(ret));
      }
    } else {
      time_guard.click("get_candidate");
      if (OB_FAIL(do_wash_candidate_tablet(info, tablet_handle, free_obj))) {
        LOG_WARN("fail to do wash candidate tablet", K(ret));
      } else if (OB_NOT_NULL(free_obj)) {
        header.remove(static_cast<ObMetaObjBufferNode *>(free_obj));
      }
      time_guard.click("wash_tablet");
    }
  }
  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    if (OB_ISNULL(free_obj)) {
      // ignore ret
      LOG_WARN("no object can be washed", K(ret), K(is_large),
          "tablet count", tablet_map_.count(), K(tablet_buffer_pool_), K(large_tablet_buffer_pool_),
          K(time_guard), K(sizeof(ObTablet)), K(sizeof(ObTabletPointer)), K(lbt()));
    } else {
      FLOG_INFO("succeed to wash tablet", K(is_large), "tablet count", tablet_map_.count(),
          K(tablet_buffer_pool_), K(large_tablet_buffer_pool_),
          K(sizeof(ObTablet)), K(sizeof(ObTabletPointer)), K(time_guard));
    }
  }
  return ret;
}

ObTenantMetaMemMgr::MinMinorSSTableInfo::MinMinorSSTableInfo(
    const share::ObLSID &ls_id,
    const ObITable::TableKey &table_key,
    const ObTableHandleV2 &sstable_handle)
  : ls_id_(ls_id),
    table_key_(table_key),
    sstable_handle_(sstable_handle)
{
}

ObTenantMetaMemMgr::MinMinorSSTableInfo::~MinMinorSSTableInfo()
{
  ls_id_.reset();
  table_key_.reset();
  sstable_handle_.reset();
}

ObT3mTabletMapIterator::ObT3mTabletMapIterator(ObTenantMetaMemMgr &t3m)
  : tablet_map_(t3m.tablet_map_),
    tablet_items_(),
    idx_(0)
{
  tablet_items_.set_attr(SET_USE_500("TabletItems"));
}

ObT3mTabletMapIterator::~ObT3mTabletMapIterator()
{
  reset();
}

void ObT3mTabletMapIterator::reset()
{
  tablet_items_.reset();
  idx_ = 0;
}

int ObT3mTabletMapIterator::fetch_tablet_item()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr = SET_USE_500(ObMemAttr(MTL_ID(), "TabletIterSE"));
  tablet_items_.set_attr(attr);
  FetchTabletItemOp fetch_op(tablet_map_, tablet_items_);
  if (OB_UNLIKELY(tablet_items_.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator use again, may be not reset", K(ret));
  } else if (OB_FAIL(tablet_map_.for_each_value_store(fetch_op))) {
    LOG_WARN("fail to fetch value store for each in map", K(ret));
  } else {
    idx_ = 0;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

ObT3mTabletMapIterator::FetchTabletItemOp::FetchTabletItemOp(
    TabletMap &tablet_map,
    common::ObIArray<ObTabletMapKey> &items)
  : tablet_map_(tablet_map), items_(items)
{
}

int ObT3mTabletMapIterator::FetchTabletItemOp::operator()(TabletPair &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(items_.push_back(pair.first))) {
    LOG_WARN("fail to push back tablet map key", K(ret), K(pair.first));
  }
  return ret;
}

ObTenantTabletIterator::ObTenantTabletIterator(
    ObTenantMetaMemMgr &t3m,
    common::ObArenaAllocator &allocator,
    ObITabletFilterOp *op)
  : ObT3mTabletMapIterator(t3m),
    allocator_(&allocator),
    op_(op)
{
}

int ObTenantTabletIterator::get_next_tablet(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (0 == tablet_items_.count() && OB_FAIL(fetch_tablet_item())) {
    LOG_WARN("fail to fetch value store pointers", K(ret));
  } else {
    do {
      if (tablet_items_.count() == idx_) {
        ret = OB_ITER_END;
      } else {
        const ObTabletMapKey &key = tablet_items_.at(idx_);
        if (OB_ISNULL(allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocator_ is nullptr, which is not allowed", K(ret));
        } else if (OB_FAIL(tablet_map_.get_meta_obj_with_external_memory(
            key, *allocator_, handle, false/*force*/, op_)) && !ignore_err_code(ret)) {
          LOG_WARN("fail to get tablet handle", K(ret), K(key));
        }
        if (OB_SUCC(ret) || ignore_err_code(ret)) {
          handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          if (allocator_ == handle.get_allocator()) {
            handle.disallow_copy_and_assign();
            handle.get_obj()->set_allocator(handle.get_allocator());
          }
          ++idx_;
        }
      }
    } while (ignore_err_code(ret)); // ignore deleted tablet or skipped tablet
  }
  return ret;
}

ObTenantTabletPtrWithInMemObjIterator::ObTenantTabletPtrWithInMemObjIterator(ObTenantMetaMemMgr &t3m)
  : ObT3mTabletMapIterator(t3m)
{
}

int ObTenantTabletPtrWithInMemObjIterator::get_next_tablet_pointer(
    ObTabletMapKey &tablet_key,
    ObTabletPointerHandle &pointer_handle,
    ObTabletHandle &in_memory_tablet_handle)
{
  int ret = OB_SUCCESS;
  pointer_handle.reset();
  in_memory_tablet_handle.reset();
  if (0 == tablet_items_.count() && OB_FAIL(fetch_tablet_item())) {
    LOG_WARN("fail to fetch value store pointers", K(ret));
  } else {
    do {
      if (tablet_items_.count() == idx_) {
        ret = OB_ITER_END;
      } else {
        bool success = false;
        ObTabletPointerHandle ptr_hdl(tablet_map_);
        const ObTabletMapKey &key = tablet_items_.at(idx_);
        if (OB_FAIL(tablet_map_.get(key, ptr_hdl))) {
          if (OB_ENTRY_NOT_EXIST != ret){
            LOG_WARN("fail to get tablet pointer handle", K(ret), K(key));
          }
        } else if (OB_FAIL(pointer_handle.assign(ptr_hdl))) {
          LOG_WARN("fail to assign pointer handle", K(ret), K(key), K(ptr_hdl));
        } else if (OB_FAIL(tablet_map_.try_get_in_memory_meta_obj(key, success,
            in_memory_tablet_handle))) {
          if (OB_ENTRY_NOT_EXIST != ret){
            LOG_WARN("fail to get in memory tablet handle", K(ret), K(key));
          }
        } else if (success) {
          in_memory_tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
        }
        if (OB_SUCC(ret) || ignore_err_code(ret)) {
          ++idx_;
          tablet_key = key;
        }
      }
    } while (ignore_err_code(ret)); // ignore deleted tablet
  }
  return ret;
}

ObTenantInMemoryTabletIterator::ObTenantInMemoryTabletIterator(ObTenantMetaMemMgr &t3m)
  : ObT3mTabletMapIterator(t3m)
{
}

int ObTenantInMemoryTabletIterator::get_next_tablet(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (0 == tablet_items_.count() && OB_FAIL(fetch_tablet_item())) {
    LOG_WARN("fail to fetch value store pointers", K(ret));
  } else {
    handle.reset();
    bool success = false;
    do {
      if (tablet_items_.count() == idx_) {
        ret = OB_ITER_END;
      } else {
        const ObTabletMapKey &key = tablet_items_.at(idx_++);
        if (OB_FAIL(tablet_map_.try_get_in_memory_meta_obj(key, success, handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get tablet handle", K(ret), K(key));
          }
        } else if (success) {
          handle.set_wash_priority(WashTabletPriority::WTP_LOW);
        }
      }
    } while (OB_SUCC(ret) && !success);
  }
  return ret;
}

int ObTenantMetaMemMgr::get_full_tablets_info(
    const ObFullTabletCreator &creator,
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
    mem_status.each_obj_size_ = -1; // the size of full tablet varies
    mem_status.free_obj_cnt_ = -1;
    mem_status.used_obj_cnt_ = creator.get_used_obj_cnt();
    mem_status.total_size_ = creator.total();
    mem_status.used_size_ = creator.used();
    if (OB_FAIL(info.push_back(mem_status))) {
      STORAGE_LOG(WARN, "fail to push mem status to info array", K(ret), K(mem_status));
    }
  }

  return ret;
}

} // end namespace storage
} // end namespace oceanbase
