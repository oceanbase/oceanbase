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

#include "lib/function/ob_function.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tx_table/ob_tx_data_memtable.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"

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

void ObTenantMetaMemMgr::TableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool all_table_cleaned = false; // no use
  if (OB_FAIL(t3m_->gc_tables_in_queue(all_table_cleaned))) {
    LOG_WARN("fail to gc tables in queue", K(ret));
  }
}

void ObTenantMetaMemMgr::MinMinorSSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(t3m_->gc_min_minor_sstable_in_set())) {
    LOG_WARN("fail to gc min minor sstable in set", K(ret));
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

ObTenantMetaMemMgr::ObTenantMetaMemMgr(const uint64_t tenant_id)
  : cmp_ret_(OB_SUCCESS),
    compare_(cmp_ret_),
    wash_func_(*this),
    tenant_id_(tenant_id),
    bucket_lock_(),
    allocator_(tenant_id, wash_func_),
    tablet_map_(),
    timer_(),
    table_gc_task_(this),
    min_minor_sstable_gc_task_(this),
    refresh_config_task_(),
    free_tables_queue_(),
    gc_queue_lock_(),
    last_min_minor_sstable_set_(),
    sstable_set_lock_(),
    pinned_tablet_set_(),
    memtable_pool_(tenant_id, get_default_memtable_pool_count(), "MemTblObj", ObCtxIds::DEFAULT_CTX_ID, wash_func_),
    sstable_pool_(tenant_id, get_default_sstable_pool_count(), "SSTblObj", ObCtxIds::META_OBJ_CTX_ID, wash_func_),
    tablet_pool_(tenant_id, get_default_tablet_pool_count(), "TabletObj", ObCtxIds::META_OBJ_CTX_ID, wash_func_),
    tablet_ddl_kv_mgr_pool_(tenant_id, get_default_tablet_pool_count(), "DDLKvMgrObj", ObCtxIds::DEFAULT_CTX_ID, wash_func_),
    tablet_memtable_mgr_pool_(tenant_id, get_default_tablet_pool_count(), "MemTblMgrObj", ObCtxIds::DEFAULT_CTX_ID,wash_func_),
    tx_data_memtable_pool_(tenant_id, MAX_TX_DATA_MEMTABLE_CNT_IN_OBJ_POOL, "TxDataMemObj", ObCtxIds::DEFAULT_CTX_ID,wash_func_),
    tx_ctx_memtable_pool_(tenant_id, MAX_TX_CTX_MEMTABLE_CNT_IN_OBJ_POOL, "TxCtxMemObj", ObCtxIds::DEFAULT_CTX_ID,wash_func_),
    lock_memtable_pool_(tenant_id, MAX_LOCK_MEMTABLE_CNT_IN_OBJ_POOL, "LockMemObj", ObCtxIds::DEFAULT_CTX_ID,wash_func_),
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
  meta_mem_mgr = OB_NEW(ObTenantMetaMemMgr, oceanbase::ObModIds::OMT_TENANT, tenant_id);
  if (OB_ISNULL(meta_mem_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantMetaMemMgr::init()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(tenant_id_, "MetaAllocator", ObCtxIds::META_OBJ_CTX_ID);
  const int64_t mem_limit = get_tenant_memory_limit(tenant_id_);
  const int64_t min_bkt_cnt = DEFAULT_BUCKET_NUM;
  const int64_t max_bkt_cnt = 10000000L;
  const int64_t tablet_bucket_num = std::min(std::max((mem_limit / (1024 * 1024 * 1024)) * 50000, min_bkt_cnt), max_bkt_cnt);
  const int64_t bucket_num = common::hash::cal_next_prime(tablet_bucket_num);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantMetaMemMgr has been initialized", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::BLOCK_MANAGER_LOCK))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("fail to init tenant fifo allocator", K(ret));
  } else if (OB_FAIL(tablet_map_.init(bucket_num, "TabletMap", TOTAL_LIMIT, HOLD_LIMIT,
        common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("fail to initialize tablet map", K(ret));
  } else if (OB_FAIL(last_min_minor_sstable_set_.create(DEFAULT_MINOR_SSTABLE_SET_COUNT))) {
    LOG_WARN("fail to create last min minor sstable set", K(ret));
  } else if (pinned_tablet_set_.create(DEFAULT_BUCKET_NUM)) {
    LOG_WARN("fail to create pinned tablet set", K(ret));
  } else {
    timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(timer_.init("T3mGC"))) {
      LOG_WARN("fail to init itable gc timer", K(ret));
    } else {
      init_pool_arr();
      is_inited_ = true;
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObTenantMetaMemMgr::init_pool_arr()
{
  pool_arr_[static_cast<int>(ObITable::TableType::DATA_MEMTABLE)] = &memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::TX_DATA_MEMTABLE)] = &tx_data_memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::TX_CTX_MEMTABLE)] = &tx_ctx_memtable_pool_;
  pool_arr_[static_cast<int>(ObITable::TableType::LOCK_MEMTABLE)] = &lock_memtable_pool_;

  for (int64_t i = ObITable::TableType::MAJOR_SSTABLE; i < ObITable::TableType::MAX_TABLE_TYPE; i++) {
    if (ObITable::is_sstable((ObITable::TableType)i)) {
      pool_arr_[i] = &sstable_pool_;
    }
  }
}

int ObTenantMetaMemMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(timer_.schedule(table_gc_task_, TABLE_GC_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule itables gc task", K(ret));
  } else if (OB_FAIL(timer_.schedule(min_minor_sstable_gc_task_, MIN_MINOR_SSTABLE_GC_INTERVAL_US,
      true/*repeat*/))) {
    LOG_WARN("fail to schedule min minor sstable gc task", K(ret));
  } else if (OB_FAIL(timer_.schedule(refresh_config_task_, REFRESH_CONFIG_INTERVAL_US,
      true/*repeat*/))) {
    LOG_WARN("fail to schedule refresh config task", K(ret));
  }
  return ret;
}

void ObTenantMetaMemMgr::stop()
{
  timer_.stop();
}

void ObTenantMetaMemMgr::wait()
{
  timer_.wait();
}

void ObTenantMetaMemMgr::destroy()
{
  int ret = OB_SUCCESS;
  bool is_all_clean = false;
  tablet_map_.destroy();
  last_min_minor_sstable_set_.destroy();
  pinned_tablet_set_.destroy();
  while (!is_all_clean && OB_SUCC(gc_tables_in_queue(is_all_clean)));
  bucket_lock_.destroy();
  allocator_.reset();
  timer_.destroy();
  for (int64_t i = 0; i <= ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE; i++) {
    pool_arr_[i] = nullptr;
  }
  is_inited_ = false;
}

void ObTenantMetaMemMgr::mtl_destroy(ObTenantMetaMemMgr *&meta_mem_mgr)
{
  if (OB_UNLIKELY(nullptr == meta_mem_mgr)) {
    LOG_WARN("meta mem mgr is nullptr", KP(meta_mem_mgr));
  } else {
    OB_DELETE(ObTenantMetaMemMgr, oceanbase::ObModIds::OMT_TENANT, meta_mem_mgr);
    meta_mem_mgr = nullptr;
  }
}

int ObTenantMetaMemMgr::push_table_into_gc_queue(ObITable *table, const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  const int64_t size = sizeof(TableGCItem);
  const ObMemAttr attr(tenant_id_, "TableGCItem");
  TableGCItem *item = nullptr;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(table));
  } else if (OB_UNLIKELY(!ObITable::is_table_type_valid(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid table key", K(ret), K(table_type), KP(table), K(*table));
  } else if (OB_ISNULL(item = (TableGCItem *)ob_malloc(size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory for TableGCItem", K(ret), K(size));
  } else {
    item->table_ = table;
    item->table_type_ = table_type;
    if (OB_FAIL(free_tables_queue_.push((ObLink *)item))) {
      LOG_ERROR("fail to push back into free_tables_queue_", K(ret), KPC(item));
    } else {
      LOG_DEBUG("succeed to push table into gc queue", KP(table), K(table_type), K(lbt()));
    }
  }

  if (OB_FAIL(ret) && nullptr != item) {
    ob_free(item);
  }
  return ret;
}

int ObTenantMetaMemMgr::gc_tables_in_queue(bool &all_table_cleaned)
{
  int ret = OB_SUCCESS;
  all_table_cleaned = false;
  int64_t left_recycle_cnt = ONE_ROUND_RECYCLE_COUNT_THRESHOLD;
  int64_t table_cnt_arr[ObITable::TableType::MAX_TABLE_TYPE] = { 0 };
  const int64_t pending_cnt = free_tables_queue_.size();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("t3m has not been inited", K(ret));
  } else {
    lib::ObLockGuard<common::ObSpinLock> lock_guard(gc_queue_lock_);
    while(OB_SUCC(ret) && left_recycle_cnt > 0 && free_tables_queue_.size() > 0) {
      ObLink *ptr = nullptr;
      if (OB_FAIL(free_tables_queue_.pop(ptr))) {
        LOG_WARN("fail to pop itable gc item", K(ret));
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("itable gc item is nullptr", K(ret), KP(ptr));
      } else {
        TableGCItem *item = static_cast<TableGCItem *>(ptr);
        ObITable *table = item->table_;
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the table in gc item is nullptr", K(ret), KP(item));
        } else {
          ObITable::TableType table_type = item->table_type_;
          int64_t index = static_cast<int>(table_type);
          if (OB_UNLIKELY(!ObITable::is_table_type_valid(table_type) || nullptr == pool_arr_[index])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("the table type is invalid", K(ret), KP(item), KPC(item), KPC(table), K(table->get_key()));
          } else {
            pool_arr_[index]->free_obj(static_cast<void *>(table));
            table_cnt_arr[index]++;
          }
        }

        if (OB_SUCC(ret)) {
          left_recycle_cnt--;
          ob_free(item);
          item = nullptr;
        } else if (OB_NOT_NULL(item)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(free_tables_queue_.push(item))) {
            LOG_ERROR("fail to push itable gc item into queue", K(tmp_ret));
          } else {
            item = nullptr;
          }
        }
      }
    }

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
    if (recycled_cnt > 0) {
      FLOG_INFO("Successfully finish table gc", K(sstable_cnt), K(data_memtable_cnt),
        K(tx_data_memtable_cnt), K(tx_ctx_memtable_cnt), K(lock_memtable_cnt),
        K(pending_cnt), K(recycled_cnt), K(allocator_),
        K(tablet_pool_), K(sstable_pool_), K(memtable_pool_),
        "tablet count", tablet_map_.count(),
        "min_minor_cnt", last_min_minor_sstable_set_.size(),
        "pinned_tablet_cnt", pinned_tablet_set_.size());
    } else if (REACH_COUNT_INTERVAL(100)) {
      FLOG_INFO("Recycle 0 table", K(ret), K(allocator_),
          K(tablet_pool_), K(sstable_pool_), K(memtable_pool_),
          "tablet count", tablet_map_.count(),
          "min_minor_cnt", last_min_minor_sstable_set_.size(),
          "pinned_tablet_cnt", pinned_tablet_set_.size());
    }
  }

  return ret;
}

void ObTenantMetaMemMgr::gc_sstable(ObSSTable *sstable)
{
  if (OB_UNLIKELY(nullptr == sstable)) {
    LOG_WARN("invalid argument");
  } else {
    const int64_t block_cnt = sstable->get_meta().get_macro_info().get_total_block_cnt();
    const int64_t start_time = ObTimeUtility::current_time();
    sstable_pool_.free_obj(sstable);
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > SSTABLE_GC_MAX_TIME) {
      LOG_WARN("sstable gc costs too much time", K(start_time), K(end_time), K(block_cnt));
    }
  }
}

int ObTenantMetaMemMgr::get_min_end_log_ts_for_ls(const share::ObLSID &ls_id, int64_t &end_log_ts)
{
  int ret = OB_SUCCESS;
  end_log_ts = INT64_MAX;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id));
  } else {
    SpinRLockGuard guard(sstable_set_lock_);
    SSTableSet::const_iterator iter = last_min_minor_sstable_set_.begin();
    while (OB_SUCC(ret) && iter != last_min_minor_sstable_set_.end()) {
      const MinMinorSSTableInfo &info = iter->first;
      if (info.ls_id_ != ls_id || info.table_key_.tablet_id_.is_ls_inner_tablet()) {
        // just skip
      } else if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, info is invalid", K(ret), K(info));
      } else if (info.sstable_handle_.get_table()->get_end_log_ts() < end_log_ts) {
        end_log_ts = info.sstable_handle_.get_table()->get_end_log_ts();
      }
      ++iter;
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::record_min_minor_sstable(
    const share::ObLSID &ls_id,
    const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(table_handle));
  } else if (OB_UNLIKELY(!table_handle.get_table()->is_minor_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type, this table must be minor sstable", K(ret), K(table_handle));
  } else {
    MinMinorSSTableInfo info(ls_id, table_handle.get_table()->get_key(), table_handle);
    SpinWLockGuard guard(sstable_set_lock_);
    if (OB_FAIL(last_min_minor_sstable_set_.set_refactored(info, 0/*flag, not overwrite*/))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to set refactored min minor sstable", K(ret), K(info));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::gc_min_minor_sstable_in_set()
{
  int ret = OB_SUCCESS;
  common::ObArray<const MinMinorSSTableInfo *> need_remove_infos;
  int64_t gc_table_cnt = 0;
  {
    SpinWLockGuard guard(sstable_set_lock_);
    SSTableSet::const_iterator iter = last_min_minor_sstable_set_.begin();
    while (OB_SUCC(ret) && iter != last_min_minor_sstable_set_.end()) {
      const MinMinorSSTableInfo &info = iter->first;
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error, info is invalid", K(ret), K(info));
      } else if (1 == info.sstable_handle_.get_table()->get_ref()) {
        if (OB_FAIL(need_remove_infos.push_back(&info))) {
          LOG_WARN("fail to push back info need to remove", K(ret), K(info));
        }
      }
      ++iter;
    }
  }
  if (OB_SUCC(ret) && need_remove_infos.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_remove_infos.count(); ++i) {
      SpinWLockGuard guard(sstable_set_lock_);
      if (OB_FAIL(last_min_minor_sstable_set_.erase_refactored(*need_remove_infos.at(i)))) {
        LOG_WARN("fail to erase info from last min minor sstable set", K(ret), K(i), "info",
            *need_remove_infos.at(i));
      } else {
        gc_table_cnt++;
      }
    }
    if (gc_table_cnt > 0) {
       FLOG_INFO("gc min minor sstable in set", K(ret), K(gc_table_cnt));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_sstable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(sstable_pool_.acquire(sstable))) {
    LOG_WARN("fail to acquire sstable object", K(ret));
  // sstable may not be major, table_type here is only used to distinguish obj pool
  } else if (OB_FAIL(handle.set_table(sstable, this, ObITable::TableType::MAJOR_SSTABLE))) {
    LOG_WARN("fail to set table", K(ret), KP(sstable));
  } else {
    sstable = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(sstable)) {
      release_sstable(sstable);
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_sstable(ObTableHandleV2 &handle, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(is_used_obj_pool(&allocator))) {
    if (OB_FAIL(acquire_sstable(handle))) {
      LOG_WARN("fail to acquire sstable", K(ret));
    }
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSTable)))) {
      LOG_WARN("fail to acquire sstable object", K(ret));
    } else {
      ObSSTable *sstable = new (buf) ObSSTable();
      if (OB_FAIL(handle.set_table(sstable, &allocator))) {
        LOG_WARN("sstable is null", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      handle.reset();
      if (OB_NOT_NULL(buf)) {
        static_cast<ObSSTable *>(buf)->~ObSSTable();
        allocator.free(buf);
        buf = nullptr;
      }
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::release_sstable(ObSSTable *sstable)
{
  if (OB_NOT_NULL(sstable)) {
    if (0 != sstable->get_ref()) {
      LOG_ERROR("ObSSTable reference count may be leak", KPC(sstable));
    } else {
      sstable_pool_.release(sstable);
    }
  }
}

int ObTenantMetaMemMgr::acquire_memtable(ObTableHandleV2 &handle)
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
    handle.set_table(memtable, this,  ObITable::TableType::DATA_MEMTABLE);
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
    handle.set_table(tx_data_memtable, this,  ObITable::TableType::TX_DATA_MEMTABLE);
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
    handle.set_table(tx_ctx_memtable, this,  ObITable::TableType::TX_CTX_MEMTABLE);
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
      LOG_ERROR("ObSSTable reference count may be leak", KPC(memtable));
    } else {
      memtable_pool_.release(memtable);
    }
  }
}

void ObTenantMetaMemMgr::release_tx_data_memtable_(ObTxDataMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR("ObTxDataMemtable reference count may be leak", KPC(memtable));
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
      LOG_ERROR("ObTabletDDLKvMgr reference count may be leak", KPC(ddl_kv_mgr));
    } else {
      tablet_ddl_kv_mgr_pool_.release(ddl_kv_mgr);
    }
  }
}

int ObTenantMetaMemMgr::acquire_tablet_memtable_mgr(ObMemtableMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *memtable_mgr = nullptr;

  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_FAIL(tablet_memtable_mgr_pool_.acquire(memtable_mgr))) {
    LOG_WARN("fail to acquire memtable manager object", K(ret));
  } else if (OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable manager is null", K(ret), KP(memtable_mgr));
  } else {
    handle.set_memtable_mgr(memtable_mgr, &tablet_memtable_mgr_pool_);
    memtable_mgr = nullptr;
  }

  if (OB_FAIL(ret)) {
    handle.reset();
    if (OB_NOT_NULL(memtable_mgr)) {
      release_tablet_memtable_mgr(memtable_mgr);
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::release_tablet_memtable_mgr(ObTabletMemtableMgr *memtable_mgr)
{
  if (OB_NOT_NULL(memtable_mgr)) {
    if (0 != memtable_mgr->get_ref()) {
      LOG_ERROR("ObTabletMemtableMgr reference count may be leak", KPC(memtable_mgr));
    } else {
      tablet_memtable_mgr_pool_.release(memtable_mgr);
    }
  }
}

void ObTenantMetaMemMgr::release_tx_ctx_memtable_(ObTxCtxMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR("ObTxCtxMemtable reference count may be leak", KPC(memtable));
    } else {
      tx_ctx_memtable_pool_.release(memtable);
    }
  }
}

void ObTenantMetaMemMgr::release_lock_memtable_(ObLockMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    if (0 != memtable->get_ref()) {
      LOG_ERROR("ObLockMemtable reference count may be leak", KPC(memtable));
    } else {
      lock_memtable_pool_.release(memtable);
    }
  }
}

int ObTenantMetaMemMgr::get_allocator_info(common::ObIArray<ObTenantMetaMemStatus> &info) const
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemStatus mem_status;
  STRNCPY(mem_status.name_, "ALLOCATOR", mem_status.STRING_LEN);
  mem_status.each_obj_size_ = 0;
  mem_status.free_obj_cnt_ = 0;
  mem_status.used_obj_cnt_ = 0;
  mem_status.total_size_ = allocator_.total();
  mem_status.used_size_ = allocator_.used();
  if (OB_FAIL(info.push_back(mem_status))) {
    LOG_WARN("fail to push mem status to info array", K(ret), K(mem_status));
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const bool only_acquire)
{
  int ret = OB_SUCCESS;
  ObMetaObj<ObTablet> meta_obj;
  meta_obj.pool_ = &tablet_pool_;
  tablet_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_pool_.acquire(meta_obj.ptr_))) {
    LOG_WARN("fail to acquire object", K(ret));
  } else if (OB_ISNULL(meta_obj.ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr", K(ret), K(meta_obj));
  } else {
    tablet_handle.set_obj(meta_obj);
    tablet_handle.set_wash_priority(priority);
    meta_obj.ptr_ = nullptr;
    bool is_exist = false;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (is_exist || only_acquire) {
      ObTabletPointerHandle ptr_handle(tablet_map_);
      if (OB_FAIL(tablet_map_.set_attr_for_obj(key, tablet_handle))) {
        LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
        LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
        LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
      }
    } else if (OB_FAIL(create_tablet(key, ls_handle, tablet_handle))) {
      LOG_WARN("fail to create tablet in map", K(ret), K(key), K(tablet_handle));
    }
  }
  if (OB_FAIL(ret)) {
    tablet_handle.reset();
    if (OB_NOT_NULL(meta_obj.ptr_)) {
      release_tablet(meta_obj.ptr_);
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::acquire_tablet(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObIAllocator &allocator,
    ObTabletHandle &tablet_handle,
    const bool only_acquire)
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
    tablet_handle.set_obj(new (buf) ObTablet(), &allocator);
    tablet_handle.set_wash_priority(priority);
    bool is_exist = false;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet existence", K(ret), K(key));
    } else if (is_exist || only_acquire) {
      ObTabletPointerHandle ptr_handle(tablet_map_);
      if (OB_FAIL(tablet_map_.set_attr_for_obj(key, tablet_handle))) {
        LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
        LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
      } else if (OB_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
        LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("don't support to acquire for creating tablet", K(ret), K(key));
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
  } else if (OB_FAIL(acquire_tablet_memtable_mgr(memtable_mgr_hdl))) {
    LOG_WARN("fail to acquire tablet memtable mgr", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObTabletPointerHandle ptr_handle(tablet_map_);
    ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
    ObMetaDiskAddr addr;
    addr.set_none_addr();
    tablet_ptr.set_addr_with_reset_obj(addr);

    if (OB_FAIL(tablet_map_.set(key, tablet_ptr))) {
      LOG_WARN("fail to set tablet pointer", K(ret), K(key));
    } else if (OB_FAIL(tablet_map_.set_attr_for_obj(key, tablet_handle))) {
      LOG_WARN("fail to set attribute for tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet_map_.get(key, ptr_handle))) {
      LOG_WARN("fail to get tablet pointer handle", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet_handle.get_obj()->assign_pointer_handle(ptr_handle))) {
      LOG_WARN("fail to set tablet pointer handle for tablet", K(ret), K(key));
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(tablet_map_.erase(key, allocator_))) {
        LOG_WARN("fail to erase tablet pointer", K(tmp_ret), K(key));
      }
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
  } else if (OB_FAIL(tablet_map_.get_meta_obj(key, allocator_, handle))) {
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

int ObTenantMetaMemMgr::get_tablet_with_allocator(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObIAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || is_used_obj_pool(&allocator))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), KP(&allocator), KP(&allocator_));
  } else if (OB_FAIL(tablet_map_.get_meta_obj_with_external_memory(key, allocator, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
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

int ObTenantMetaMemMgr::get_tablet_pointer_tx_data(
    const ObTabletMapKey &key,
    ObTabletTxMultiSourceDataUnit &tx_data)
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
    } else if (OB_FAIL(tablet_ptr->get_tx_data(tx_data))) {
      LOG_WARN("failed to get tx data in tablet pointer", K(ret), K(key));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::set_tablet_pointer_tx_data(
    const ObTabletMapKey &key,
    const ObTabletTxMultiSourceDataUnit &tx_data)
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
    } else if (OB_FAIL(tablet_ptr->set_tx_data(tx_data))) {
      LOG_WARN("failed to set tx data in tablet pointer", K(ret), K(key), K(tx_data));
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

int ObTenantMetaMemMgr::get_meta_mem_status(common::ObIArray<ObTenantMetaMemStatus> &info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_obj_pool_info(memtable_pool_, "MEMTABLE POOL", info))) {
    LOG_WARN("fail to get memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(sstable_pool_, "SSTABLE POOL", info))) {
    LOG_WARN("fail to get sstable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tablet_pool_, "TABLET POOL", info))) {
    LOG_WARN("fail to get tablet pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tablet_ddl_kv_mgr_pool_, "KV MGR POOL", info))) {
    LOG_WARN("fail to get kv mgr pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tablet_memtable_mgr_pool_, "MEMTABLE MGR POOL", info))) {
    LOG_WARN("fail to get memtable mgr pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tx_data_memtable_pool_, "TX DATA MEMTABLE POOL", info))) {
    LOG_WARN("fail to get tx data memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(tx_ctx_memtable_pool_, "TX CTX MEMTABLE POOL", info))) {
    LOG_WARN("fail to get tx ctx memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_obj_pool_info(lock_memtable_pool_, "LOCK MEMTABLE POOL", info))) {
    LOG_WARN("fail to get lock memtable pool's info", K(ret), K(info));
  } else if (OB_FAIL(get_allocator_info(info))) {
    LOG_WARN("fail to get allocator's info", K(ret), K(info));
  }
  return ret;
}

int ObTenantMetaMemMgr::del_tablet(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(tablet_map_.erase(key, allocator_))) {
    LOG_WARN("fail to erase tablet pointer", K(ret), K(key));
  } else if (OB_FAIL(erase_pinned_tablet(key))) {
    LOG_WARN("failed to erase from pinned tablet set", K(ret), K(key));
  }
  return ret;
}

int ObTenantMetaMemMgr::compare_and_swap_tablet(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &new_addr,
    const ObTabletHandle &old_handle,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid()
                      || !new_addr.is_valid()
                      || new_addr.is_none()
                      || !old_handle.is_valid()
                      || !new_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(new_addr), K(old_handle), K(new_handle));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet is exist", K(ret), K(key));
    } else if (OB_UNLIKELY(!is_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("this tablet does not exist in map", K(ret), K(key), K(is_exist));
    } else if (OB_FAIL(tablet_map_.compare_and_swap_address_and_object(key,
                                                                       new_addr,
                                                                       old_handle,
                                                                       new_handle))) {
      LOG_WARN("fail to replace tablet in map", K(ret), K(key), K(new_addr), K(old_handle),
          K(new_handle));
    } else if (old_handle.get_obj() != new_handle.get_obj()) {
      ObTabletTableStore &table_store = old_handle.get_obj()->get_table_store();
      ObITable *sstable = table_store.get_minor_sstables().get_boundary_table(false /*is_last*/);
      if (OB_NOT_NULL(sstable)) {
        ObTableHandleV2 table_handle(sstable, this, ObITable::TableType::MAJOR_SSTABLE);
        if (OB_FAIL(record_min_minor_sstable(key.ls_id_, table_handle))) {
          LOG_WARN("fail to record min minor sstable", K(ret), K(key), K(table_handle));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_handle.get_obj()->set_tx_data_in_tablet_pointer())) {
    LOG_WARN("failed to set tx data in tablet pointer", K(ret), K(key));
  }
  return ret;
}

int ObTenantMetaMemMgr::compare_and_swap_tablet_pure_address_without_object(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !old_addr.is_disked() || !new_addr.is_disked())) {
    ret = OB_INVALID_ARGUMENT;
    FLOG_WARN("invalid argument", K(ret), K(key), K(old_addr), K(new_addr));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.hash());
    if (OB_FAIL(has_tablet(key, is_exist))) {
      LOG_WARN("fail to check tablet is exist", K(ret), K(key));
    } else if (OB_UNLIKELY(!is_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("this tablet isn't exist in map", K(ret), K(key), K(is_exist));
    } else if (OB_FAIL(tablet_map_.compare_and_swap_address_without_object(key,
                                                                           old_addr,
                                                                           new_addr))) {
      LOG_WARN("fail to compare and swap tablet address in map", K(ret), K(key), K(old_addr),
          K(new_addr));
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::release_tablet(ObTablet *tablet)
{
  if (OB_NOT_NULL(tablet)) {
    if (0 != tablet->get_ref()) {
      LOG_ERROR("ObTablet reference count may be leak", KPC(tablet));
    } else {
      tablet_pool_.release(tablet);
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

int ObTenantMetaMemMgr::check_all_meta_mem_released(ObLSService &ls_service, bool &is_released,
    const char *module)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantMetaMemMgr hasn't been initialized", K(ret));
  } else {
    int64_t memtable_cnt = memtable_pool_.get_used_obj_cnt();
    int64_t sstable_cnt = sstable_pool_.get_used_obj_cnt();
    int64_t tablet_cnt = tablet_pool_.get_used_obj_cnt();
    int64_t ddl_kv_mgr_cnt = tablet_ddl_kv_mgr_pool_.get_used_obj_cnt();
    int64_t tablet_memtable_mgr_cnt = tablet_memtable_mgr_pool_.get_used_obj_cnt();
    int64_t tx_data_memtable_cnt_ = tx_data_memtable_pool_.get_used_obj_cnt();
    int64_t tx_ctx_memtable_cnt_ = tx_ctx_memtable_pool_.get_used_obj_cnt();
    int64_t lock_memtable_cnt_ = lock_memtable_pool_.get_used_obj_cnt();
    if (memtable_cnt != 0 || sstable_cnt != 0 || tablet_cnt != 0 || ddl_kv_mgr_cnt != 0
        || tablet_memtable_mgr_cnt != 0 || tx_data_memtable_cnt_ != 0 || tx_ctx_memtable_cnt_ != 0
        || lock_memtable_cnt_ != 0) {
      is_released = false;
    } else {
      is_released = true;
    }
    LOG_INFO("check all meta mem in t3m", K(module), K(is_released), K(memtable_cnt), K(sstable_cnt),
        K(tablet_cnt), K(ddl_kv_mgr_cnt), K(tablet_memtable_mgr_cnt), K(tx_data_memtable_cnt_),
        K(tx_ctx_memtable_cnt_), K(lock_memtable_cnt_),
        "min_minor_cnt", last_min_minor_sstable_set_.size(),
        "pinned_tablet_cnt", pinned_tablet_set_.size());
    const uint64_t interval = 180 * 1000 * 1000; // 180s
    if (!is_released && REACH_TIME_INTERVAL(interval)) {
      dump_tablet();
      dump_pinned_tablet();
      dump_ls(ls_service);
      PRINT_OBJ_LEAK(MTL_ID(), share::LEAK_CHECK_OBJ_MAX_NUM);
    }
  }
  return ret;
}

void ObTenantMetaMemMgr::dump_tablet()
{
  int ret = OB_SUCCESS;
  common::ObFunction<int(common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *>&)> func =
      [](common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> &entry) {
    const ObTabletMapKey &key = entry.first;
    FLOG_INFO("dump tablet", K(key));
    return OB_SUCCESS;
  };
  if (OB_FAIL(tablet_map_.for_each_value_store(func))) {
    LOG_WARN("fail to traverse tablet map", K(ret));
  }
}

void ObTenantMetaMemMgr::dump_pinned_tablet() const
{
  PinnedTabletSet::const_iterator iter = pinned_tablet_set_.begin();
  for (; iter != pinned_tablet_set_.end(); ++iter) {
    const ObTabletMapKey &key = iter->first;
    FLOG_INFO("dump pinned tablet", K(key));
  }
}

void ObTenantMetaMemMgr::dump_ls(ObLSService &ls_service) const
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  ObLSMeta ls_meta;
  if (OB_FAIL(ls_service.get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_iter->get_next(ls))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next ls", K(ret));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret));
    } else if (OB_FAIL(ls->get_ls_meta(ls_meta))) {
      LOG_WARN("fail to get ls meta", K(ret));
    } else {
      FLOG_INFO("dump ls", K(ls_meta));
    }
  }
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

ObTenantMetaMemMgr::GetWashTabletCandidate::GetWashTabletCandidate(
    Heap &heap,
    common::ObIArray<InMemoryPinnedTabletInfo> &mem_addr_tablets,
    ObTenantMetaMemMgr &t3m,
    ObIAllocator &allocator)
  : heap_(heap),
    mem_addr_tablets_(mem_addr_tablets),
    t3m_(t3m),
    allocator_(allocator),
    none_addr_tablet_cnt_(0),
    mem_addr_tablet_cnt_(0),
    in_memory_tablet_cnt_(0),
    inner_tablet_cnt_(0),
    candidate_tablet_cnt_(0)
{
}

int ObTenantMetaMemMgr::GetWashTabletCandidate::operator()(
    common::hash::HashMapPair<ObTabletMapKey, TabletValueStore *> &entry)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey &tablet_key = entry.first;
  TabletValueStore *value_store = entry.second;
  ObTabletPointer *tablet_ptr = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletHandle tablet_handle;
  bool need_check = true;

  if (OB_ISNULL(value_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value store is nullptr", K(ret), KP(value_store));
  } else if (OB_ISNULL(tablet_ptr = static_cast<ObTabletPointer*>(value_store->get_value_ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is nullptr", K(ret), KP(tablet_ptr));
  } else if (tablet_ptr->get_addr().is_none()) {
    ++none_addr_tablet_cnt_;
    LOG_DEBUG("NONE addr tablet should not be washed", K(tablet_key));
  } else {
    if (tablet_key.tablet_id_.is_inner_tablet()) {
      ++inner_tablet_cnt_;
    } else if (tablet_ptr->get_addr().is_memory()) {
      ++mem_addr_tablet_cnt_;
    }
    if (tablet_ptr->is_in_memory()) {
      ++in_memory_tablet_cnt_;
      if (OB_FAIL(tablet_ptr->get_in_memory_obj(tablet_handle))) {
        LOG_WARN("fail to get object", K(ret));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is nullptr", K(ret), KP(tablet));
      } else {
        int tmp_ret = t3m_.pinned_tablet_set_.exist_refactored(tablet_key);
        if (OB_HASH_EXIST == tmp_ret) {
          need_check = false;
          LOG_DEBUG("tablet is in tx, should no be washed", K(tmp_ret), K(tablet_key));
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
        } else {
          ret = tmp_ret;
          LOG_WARN("failed to check whether tablet is in tx", K(ret), K(tablet_key));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!need_check) {
        // no need to check
      } else if (2 == tablet->get_ref()) {
        if (tablet_ptr->get_addr().is_memory()) {
          InMemoryPinnedTabletInfo info;
          info.key_ = tablet_key;
          info.addr_ = tablet_ptr->get_addr();
          info.wash_score_ = tablet_handle.get_obj()->get_wash_score();
          if (OB_FAIL(mem_addr_tablets_.push_back(info))) {
            LOG_WARN("fail to push back into in memory pinned tablet info", K(ret), K(info), K(tablet_key));
          }
        } else {
          // this tablet is hold by tablet_handle here and the tablet_map_ only
          CandidateTabletInfo candidate;
          candidate.ls_id_ = tablet_key.ls_id_.id();
          candidate.tablet_id_ = tablet_key.tablet_id_.id();
          candidate.wash_score_ = tablet->get_wash_score();
          if (OB_FAIL(heap_.push(candidate))) {
            LOG_WARN("fail to push candidate tablet", K(ret), K(candidate));
          } else if (OB_SUCCESS != t3m_.cmp_ret_) {
            ret = t3m_.cmp_ret_;
            LOG_WARN("fail to add tablet pointer into heap", K(ret), K(candidate));
          } else {
            ++candidate_tablet_cnt_;
          }
        }
      }
    }
  }
  return ret;
}

void *ObTenantMetaMemMgr::TenantMetaAllocator::alloc_align(
    const int64_t size,
    const int64_t align,
    const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  const int64_t max_wait_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 3L; // 3s
  while (OB_ISNULL(ptr = ObFIFOAllocator::alloc_align(size, align, attr))
        && OB_SUCC(ret)
        && max_wait_ts - ObTimeUtility::fast_current_time() >= 0) {
    ob_usleep(1);
    if (OB_FAIL(wash_func_())) {
      LOG_WARN("wash function fail", K(ret), K(size), K(align));
    }
  }
  return ptr;
}

int ObTenantMetaMemMgr::try_wash_tablet()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else {
    ObTimeGuard wash_time("wash_tablet_profiling");
    SpinWLockGuard guard(wash_lock_);
    wash_time.click("wait_lock");
    const int64_t wash_cnt = max(1, calc_wash_tablet_cnt());
    if (OB_FAIL(try_wash_tablet(wash_cnt))) {
      LOG_WARN("fail to try wash tablet", K(ret), K(wash_cnt));
    }
    wash_time.click("do_wash");
    if (wash_time.get_diff() > 1 * 1000 * 1000) {// 1s
      LOG_INFO("try_wash_tablet too much time", K(ret), K(wash_cnt), K(wash_time));
    }
  }
  return ret;
}

int64_t ObTenantMetaMemMgr::calc_wash_tablet_cnt() const
{
  const int64_t used_tablet_cnt = tablet_pool_.get_used_obj_cnt();
  const double variable_mem_fragment_ratio = std::round(((allocator_.total() * 1.0) / allocator_.used()) * 10000) / 10000;
  const int64_t wash_cnt = min(variable_mem_fragment_ratio > 3.75 ? INT64_MAX : static_cast<int64_t>(used_tablet_cnt * 0.2), 30000);
  FLOG_INFO("calculate wash tablet count", K(wash_cnt), K(variable_mem_fragment_ratio), K(used_tablet_cnt));
  return wash_cnt;
}

int ObTenantMetaMemMgr::write_slog_and_wash_tablet(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    bool &is_wash)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ObTenantMetaMemMgr", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !old_addr.is_valid() || !old_addr.is_memory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(old_addr));
  } else {
    common::ObFunction<int(const ObMetaObjGuard<ObTablet> &, ObMetaDiskAddr &)> dump =
        [](const ObMetaObjGuard<ObTablet> &handle, ObMetaDiskAddr &disk_addr) -> int {
      ObTabletHandle tablet_handle = handle;
      tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
      return ObTabletSlogHelper::write_create_tablet_slog(handle, disk_addr);
    };
    if (OB_FAIL(tablet_map_.wash_meta_obj_with_func(key, old_addr, dump, is_wash))) {
      LOG_WARN("fail to wash meta object with function", K(ret), K(key), K(old_addr));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::do_wash_candidate_tablet(
    const int64_t expect_wash_cnt,
    Heap &heap,
    int64_t &wash_inner_cnt,
    int64_t &wash_user_cnt)
{
  int ret = OB_SUCCESS;
  bool is_wash = false;
  while (OB_SUCC(ret) && !heap.empty() && wash_user_cnt + wash_inner_cnt < expect_wash_cnt) {
    const CandidateTabletInfo &candidate = heap.top();
    const ObLSID ls_id(candidate.ls_id_);
    const ObTabletID tablet_id(candidate.tablet_id_);
    const ObTabletMapKey key(ls_id, tablet_id);
    if (OB_FAIL(tablet_map_.wash_meta_obj(key, is_wash))) {
      LOG_WARN("wash tablet obj fail", K(ret), K(candidate));
    } else if (is_wash) {
      if (!key.tablet_id_.is_inner_tablet()) {
        ++wash_user_cnt;
      } else {
        ++wash_inner_cnt;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(heap.pop())) {
      LOG_WARN("fail to pop heap", K(ret));
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::do_wash_mem_addr_tablet(
    const int64_t expect_wash_cnt,
    const common::ObIArray<InMemoryPinnedTabletInfo> &mem_addr_tablet_info,
    int64_t &wash_inner_cnt,
    int64_t &wash_user_cnt,
    int64_t &wash_mem_addr_cnt)
{
  int ret = OB_SUCCESS;
  bool is_wash = false;
  for (int64_t i = 0;
       OB_SUCC(ret) && i < mem_addr_tablet_info.count() && wash_user_cnt + wash_inner_cnt < expect_wash_cnt;
       ++i) {
    const InMemoryPinnedTabletInfo &info = mem_addr_tablet_info.at(i);
    if (OB_FAIL(write_slog_and_wash_tablet(info.key_, info.addr_, is_wash))) {
      if (OB_NOT_THE_OBJECT == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to write slog and wash tablet", K(ret), K(info));
      }
    } else if (is_wash) {
      ++wash_mem_addr_cnt;
      if (!info.key_.tablet_id_.is_inner_tablet()) {
        ++wash_user_cnt;
      } else {
        ++wash_inner_cnt;
      }
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::try_wash_tablet(const int64_t expect_wash_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expect_wash_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expect_wash_cnt));
  } else {
    ObArray<InMemoryPinnedTabletInfo> mem_addr_tablet_info;
    ObArenaAllocator allocator;
    Heap heap(compare_, &allocator);
    GetWashTabletCandidate op(heap, mem_addr_tablet_info, *this, allocator);
    ObTimeGuard time_guard("try_wash_tablet");
    if (OB_FAIL(tablet_map_.for_each_value_store(op))) {
      LOG_WARN("fail to get candidate tablet for wash", K(ret));
    } else {
      FLOG_INFO("candidate info", "tablet count", tablet_map_.count(), "candidate count", heap.count(),
          "memory address count", mem_addr_tablet_info.count(), K(expect_wash_cnt), K(op));
      time_guard.click("get_candidate_cost");
      int64_t wash_user_cnt = 0;
      int64_t wash_inner_cnt = 0;
      int64_t wash_mem_addr_cnt = 0;
      if (OB_FAIL(do_wash_candidate_tablet(expect_wash_cnt, heap, wash_inner_cnt, wash_user_cnt))) {
        LOG_WARN("fail to do wash candidate tablet", K(ret), K(expect_wash_cnt));
      } else if (FALSE_IT(time_guard.click("wash_candidate_tablet_cost"))) {
      }
      /* do_wash_mem_addr_tablet has concurrent issue with normal tablet update, disable it
      else if (wash_inner_cnt + wash_user_cnt < expect_wash_cnt) {
        std::sort(mem_addr_tablet_info.begin(), mem_addr_tablet_info.end());
        if (OB_FAIL(do_wash_mem_addr_tablet(expect_wash_cnt, mem_addr_tablet_info, wash_inner_cnt,
            wash_user_cnt, wash_mem_addr_cnt))) {
          LOG_WARN("fail to do wash memory address tablet", K(ret), K(expect_wash_cnt));
        } else {
          time_guard.click("wash_mem_addr_tablet_cost");
        }
      }
      */
      if (OB_SUCC(ret)) {
        if (0 == wash_inner_cnt + wash_user_cnt) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("no object can be washed", K(ret), K(wash_inner_cnt), K(wash_user_cnt),
              K(wash_mem_addr_cnt), K(expect_wash_cnt),
              "tablet count", tablet_map_.count(), K(allocator_), K(tablet_pool_),
              K(sstable_pool_), K(memtable_pool_), K(tablet_ddl_kv_mgr_pool_),
              K(tablet_memtable_mgr_pool_), K(tx_data_memtable_pool_), K(tx_ctx_memtable_pool_),
              K(lock_memtable_pool_), K(op),
              "min_minor_cnt", last_min_minor_sstable_set_.size(),
              "pinned_tablet_cnt", pinned_tablet_set_.size(),
              K(time_guard), K(lbt()));
        } else {
          FLOG_INFO("succeed to wash tablet", K(wash_inner_cnt), K(wash_user_cnt),
              K(wash_mem_addr_cnt), K(expect_wash_cnt), "tablet count", tablet_map_.count(),
              K(allocator_), K(tablet_pool_), K(sstable_pool_), K(memtable_pool_), K(op),
              "min_minor_cnt", last_min_minor_sstable_set_.size(),
              "pinned_tablet_cnt", pinned_tablet_set_.size(),
              K(time_guard));
        }
      }
    }
  }
  return ret;
}

int ObTenantMetaMemMgr::insert_pinned_tablet(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    LOG_WARN("invalid args", K(ret), K(key));
  } else if (OB_FAIL(pinned_tablet_set_.set_refactored(key, 0/*flag, not overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      LOG_DEBUG("tablet already exists", K(ret), K(key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to insert into hash set", K(ret), K(key));
    }
  }

  return ret;
}

int ObTenantMetaMemMgr::erase_pinned_tablet(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    LOG_WARN("invalid args", K(ret), K(key));
  } else if (OB_FAIL(pinned_tablet_set_.erase_refactored(key))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_DEBUG("tablet does not exist in t3m pinned set", K(ret), K(key));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase from hash set", K(ret), K(key));
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
    allocator_(t3m.allocator_),
    tablet_items_(),
    idx_(0)
{
}

ObT3mTabletMapIterator::ObT3mTabletMapIterator(
    ObTenantMetaMemMgr &t3m,
    common::ObIAllocator &allocator)
  : tablet_map_(t3m.tablet_map_),
    allocator_(allocator),
    tablet_items_(),
    idx_(0)
{
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

ObTenantTabletIterator::ObTenantTabletIterator(ObTenantMetaMemMgr &t3m)
  : ObT3mTabletMapIterator(t3m),
    is_used_obj_pool_(true)
{
}

ObTenantTabletIterator::ObTenantTabletIterator(
    ObTenantMetaMemMgr &t3m,
    common::ObIAllocator &allocator)
  : ObT3mTabletMapIterator(t3m, allocator),
    is_used_obj_pool_(t3m.is_used_obj_pool(&allocator))
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
        if (!is_used_obj_pool_) {
          if (OB_FAIL(tablet_map_.get_meta_obj_with_external_memory(key, allocator_, handle))
              && OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("fail to get tablet handle", K(ret), K(key));
          }
        } else if (OB_FAIL(tablet_map_.get_meta_obj(key, allocator_, handle))
            && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet handle", K(ret), K(key));
        }
        if (OB_SUCC(ret) || ignore_err_code(ret)) {
          handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          ++idx_;
        }
      }
    } while (ignore_err_code(ret)); // ignore deleted tablet
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

} // end namespace storage
} // end namespace oceanbase
