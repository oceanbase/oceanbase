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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_tenant_tablet_scheduler.h"
#include "storage/ob_bloom_filter_task.h"
#include "ob_tablet_merge_task.h"
#include "ob_partition_merge_policy.h"
#include "ob_schedule_dag_func.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_debug_sync.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/ob_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_tenant_freeze_info_mgr.h"
#include "ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_tablet_merge_checker.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "share/scn.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/ob_gc_upper_trans_helper.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;

namespace compaction
{
/********************************************ObFastFreezeChecker impl******************************************/
ObFastFreezeChecker::ObFastFreezeChecker()
  : store_map_(),
    enable_fast_freeze_(false)
{
}

ObFastFreezeChecker::~ObFastFreezeChecker()
{
  reset();
}

int ObFastFreezeChecker::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_map_.create(FAST_FREEZE_TABLET_STAT_KEY_BUCKET_NUM, "FastFrezCkr", "FastFrezCkr", MTL_ID()))) {
    LOG_WARN("failed to init fast freeze checker", K(ret));
  }
  return ret;
}

void ObFastFreezeChecker::reset()
{
  enable_fast_freeze_ = false;
  store_map_.destroy();
}

void ObFastFreezeChecker::reload_config(const bool enable_fast_freeze)
{
  enable_fast_freeze_ = enable_fast_freeze;
}

int ObFastFreezeChecker::check_need_fast_freeze(
    const ObTablet &tablet,
    bool &need_fast_freeze)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_fast_freeze = false;
  ObTableHandleV2 table_handle;
  ObITabletMemtable *memtable = nullptr;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTableQueuingModeCfg queuing_cfg;
  if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_queuing_cfg(ls_id, tablet_id, queuing_cfg))) {
    LOG_WARN_RET(tmp_ret, "[FastFreeze] failed to get table queuing mode, treat it as normal table", K(ls_id), K(tablet_id));
  }
  const int64_t memtable_alive_threshold = queuing_cfg.get_memtable_alive_threshold(FAST_FREEZE_INTERVAL_US);
  if (OB_FAIL(tablet.get_active_memtable(table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FastFreeze] failed to get active memtable", K(ret));
    }
  } else if (OB_FAIL(table_handle.get_tablet_memtable(memtable))) {
    LOG_WARN("[FastFreeze] failed to get memtalbe", K(ret), K(table_handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FastFreeze] get unexpected null memtable", K(ret), KPC(memtable));
  } else if (!memtable->is_active_memtable()) {
    // do nothing
  } else if (!memtable->is_data_memtable()) {
    // do nothing
  } else if (ObTimeUtility::current_time() < memtable->get_timestamp() + memtable_alive_threshold) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("[FastFreeze] memtable is just created, no need to check", K(memtable_alive_threshold), K(ls_id), K(tablet_id), KPC(memtable));
    }
  } else {
    memtable::ObMemtable *mt = static_cast<memtable::ObMemtable *>(memtable);
    check_hotspot_need_fast_freeze(*mt, need_fast_freeze);
    if (need_fast_freeze) {
      FLOG_INFO("[FastFreeze] tablet detects hotspot row, need fast freeze", K(ls_id), K(tablet_id));
    } else {
      // Only queuing table need tombstone fast freeze in 4.2.x, but 4.3.0 has this before, so open it
      check_tombstone_need_fast_freeze(tablet, queuing_cfg, *mt, need_fast_freeze);
      if (need_fast_freeze) {
        FLOG_INFO("[FastFreeze] tablet detects tombstone, need fast freeze", K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

void ObFastFreezeChecker::check_hotspot_need_fast_freeze(
    memtable::ObMemtable &memtable,
    bool &need_fast_freeze)
{
  need_fast_freeze = false;
  if (memtable.is_active_memtable()) {
    need_fast_freeze = memtable.has_hotspot_row();
  }
}

void ObFastFreezeChecker::check_tombstone_need_fast_freeze(
    const ObTablet &tablet,
    const ObTableQueuingModeCfg &queuing_cfg,
    memtable::ObMemtable &memtable,
    bool &need_fast_freeze)
{
  need_fast_freeze = false;
  if (memtable.is_active_memtable()) {
    const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
    const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
    const ObMtStat &mt_stat = memtable.get_mt_stat(); // dirty read
    int64_t adaptive_threshold = queuing_cfg.get_tombstone_row_threshold(TOMBSTONE_DEFAULT_ROW_COUNT);
    if (!queuing_cfg.is_queuing_mode()) {
      // dynamically change adaptive_threshold by merge cnt in recent 10 mins
      try_update_tablet_threshold(ObTabletStatKey(ls_id, tablet_id), mt_stat, memtable.get_timestamp(), queuing_cfg, adaptive_threshold);
    }
    need_fast_freeze = (mt_stat.update_row_count_ + mt_stat.delete_row_count_) >= adaptive_threshold
                     || mt_stat.delete_row_count_ > queuing_cfg.total_delete_row_cnt_;

    if (!need_fast_freeze) {
      need_fast_freeze =
        // tombstoned row count(empty ObMvccRow) is larger than 1000(hardcoded)
        (mt_stat.empty_mvcc_row_count_ >= EMPTY_MVCC_ROW_COUNT)
        // tombstoned row precentage(empty ObMvccRow) is larger than 50%(hardcoded)
        && (mt_stat.empty_mvcc_row_count_ >= INT64_MAX / 100 // prevent numerical overflow
            || mt_stat.empty_mvcc_row_count_ * 100 / memtable.get_physical_row_cnt()
               >= EMPTY_MVCC_ROW_PERCENTAGE);
      if (need_fast_freeze) {
        LOG_INFO("[FastFreeze] trigger by empty mvcc row tombstone", K(memtable), K(mt_stat),
                 K(memtable.get_physical_row_cnt()));
      } else {
        ObAdaptiveMergePolicy::AdaptiveMergeReason adaptive_merge_reason = ObAdaptiveMergePolicy::NONE;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObAdaptiveMergePolicy::check_tombstone_reason(tablet, adaptive_merge_reason))) {
          LOG_WARN_RET(tmp_ret, "failed to check tombstone by historical stats");
        } else if (ObAdaptiveMergePolicy::NONE != adaptive_merge_reason) {
          need_fast_freeze = true;
        }
      }
    }
  }
}

void ObFastFreezeChecker::try_update_tablet_threshold(
    const ObTabletStatKey &key,
    const ObMtStat &mt_stat,
    const int64_t memtable_create_timestamp,
    const ObTableQueuingModeCfg &queuing_cfg,
    int64_t &adaptive_threshold)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t base_adaptive_threshold = queuing_cfg.get_tombstone_row_threshold(TOMBSTONE_DEFAULT_ROW_COUNT);
  adaptive_threshold = base_adaptive_threshold;
  int64_t old_threshold = adaptive_threshold;

  if (OB_TMP_FAIL(store_map_.get_refactored(key, adaptive_threshold))) {
    // use default threshold at first
    if (OB_HASH_NOT_EXIST != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "[FastFreeze] failed to find store map", K(key));
    }
  } else {
    old_threshold = adaptive_threshold;
  }

  ObTabletStat tablet_stat;
  ObTabletStat total_stat;
  ObTableModeFlag mode = ObTableModeFlag::TABLE_MODE_NORMAL;
  if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_latest_tablet_stat(key.ls_id_, key.tablet_id_, tablet_stat, total_stat, mode))) {
    if (OB_HASH_NOT_EXIST != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "[FastFreeze] failed to get tablet stat", K(key));
    }
    // not hot tablet, reset threshold
    adaptive_threshold = base_adaptive_threshold;
  } else if (tablet_stat.merge_cnt_ >= 2) {
    // too many mini compaction occurs during the past 10 mins, inc threshold to dec mini merge count
    adaptive_threshold = MIN(adaptive_threshold + TOMBSTONE_STEP_ROW_COUNT, TOMBSTONE_MAX_ROW_COUNT);
  } else if (0 == tablet_stat.merge_cnt_) {
    const int64_t inc_row_cnt = mt_stat.update_row_count_ + mt_stat.delete_row_count_;
    if (inc_row_cnt >= adaptive_threshold) {
      // do nothing
    } else if (inc_row_cnt >= TOMBSTONE_DEFAULT_ROW_COUNT && ObTimeUtility::fast_current_time() - memtable_create_timestamp >= FAST_FREEZE_INTERVAL_US * 4) {
      adaptive_threshold = base_adaptive_threshold;
    }
  }

  if (old_threshold != adaptive_threshold) {
    if (base_adaptive_threshold == adaptive_threshold) {
      (void) store_map_.erase_refactored(key);
    } else {
      (void) store_map_.set_refactored(key, adaptive_threshold);
    }
  }
}


/********************************************ObTenantTabletScheduler impl******************************************/
constexpr ObMergeType ObTenantTabletScheduler::MERGE_TYPES[];

ObTenantTabletScheduler::ObTenantTabletScheduler()
 : is_inited_(false),
   major_merge_status_(false),
   is_stop_(false),
   is_restore_(false),
   enable_adaptive_compaction_(false),
   enable_adaptive_merge_schedule_(false),
   bf_queue_(),
   frozen_version_lock_(),
   frozen_version_(INIT_COMPACTION_SCN),
   merged_version_(INIT_COMPACTION_SCN),
   inner_table_merged_scn_(INIT_COMPACTION_SCN),
   min_data_version_(0),
   time_guard_(),
   schedule_stats_(),
   fast_freeze_checker_(),
   minor_ls_tablet_iter_(false/*is_major*/),
   medium_ls_tablet_iter_(true/*is_major*/),
   gc_sst_tablet_iter_(false/*is_major*/),
   error_tablet_cnt_(0),
   loop_cnt_(0),
   prohibit_medium_map_(),
   timer_task_mgr_(),
   batch_size_mgr_()
{
  STATIC_ASSERT(static_cast<int64_t>(NO_MAJOR_MERGE_TYPE_CNT) == ARRAYSIZEOF(MERGE_TYPES), "merge type array len is mismatch");
}

ObTenantTabletScheduler::~ObTenantTabletScheduler()
{
  destroy();
}

void ObTenantTabletScheduler::destroy()
{
  if (IS_INIT) {
    reset();
  }
}
void ObTenantTabletScheduler::reset()
{
  stop();
  wait();

  is_inited_ = false;
  bf_queue_.destroy();
  frozen_version_ = 0;
  merged_version_ = 0;
  inner_table_merged_scn_ = 0;
  min_data_version_ = 0;
  time_guard_.reuse();
  schedule_stats_.reset();
  minor_ls_tablet_iter_.reset();
  medium_ls_tablet_iter_.reset();
  gc_sst_tablet_iter_.reset();
  prohibit_medium_map_.destroy();
  LOG_INFO("The ObTenantTabletScheduler destroy");
}

int ObTenantTabletScheduler::init()
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval = ObTenantTabletSchedulerTaskMgr::DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  int64_t schedule_batch_size = ObScheduleBatchSizeMgr::DEFAULT_TABLET_BATCH_CNT;

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
      enable_adaptive_merge_schedule_ = tenant_config->_enable_adaptive_merge_schedule;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
      schedule_batch_size = tenant_config->compaction_schedule_tablet_batch_cnt;
    }
  } // end of ObTenantConfigGuard
#ifdef ERRSIM
  schedule_interval = 1000L * 1000L; // 1s
#endif
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantTabletScheduler has inited", K(ret));
  } else if (FALSE_IT(bf_queue_.set_run_wrapper(MTL_CTX()))) {
  } else if (OB_FAIL(bf_queue_.init(BLOOM_FILTER_LOAD_BUILD_THREAD_CNT,
                                    "BFBuildTask",
                                    BF_TASK_QUEUE_SIZE,
                                    BF_TASK_MAP_SIZE,
                                    BF_TASK_TOTAL_LIMIT,
                                    BF_TASK_HOLD_LIMIT,
                                    BF_TASK_PAGE_SIZE,
                                    MTL_ID(),
                                    "bf_queue"))) {
    LOG_WARN("Fail to init bloom filter queue", K(ret));
  } else if (OB_FAIL(fast_freeze_checker_.init())) {
    LOG_WARN("Fail to create fast freeze checker", K(ret));
  } else if (OB_FAIL(prohibit_medium_map_.init())) {
    LOG_WARN("Fail to create prohibit medium ls id map", K(ret));
  } else {
    timer_task_mgr_.set_scheduler_interval(schedule_interval);
    batch_size_mgr_.set_tablet_batch_size(schedule_batch_size);
    is_inited_ = true;
  }
  return ret;
}

int ObTenantTabletScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else {
    ret = timer_task_mgr_.start();
  }
  return ret;
}

int ObTenantTabletScheduler::reload_tenant_config()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else {
    uint64_t compat_version = 0;
    const uint64_t cached_data_version = ATOMIC_LOAD(&min_data_version_);
    if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN_RET(tmp_ret, "fail to get data version", K(tmp_ret));
    } else if (OB_UNLIKELY(compat_version < cached_data_version)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "data version is unexpected smaller", K(tmp_ret), K(compat_version), K(cached_data_version));
    } else if (compat_version > cached_data_version) {
      ATOMIC_STORE(&min_data_version_, compat_version);
      LOG_INFO("cache min data version", "old_data_version", cached_data_version, "new_data_version", compat_version);
    }
    int64_t merge_schedule_interval = ObTenantTabletSchedulerTaskMgr::DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
    int64_t schedule_batch_size = ObScheduleBatchSizeMgr::DEFAULT_TABLET_BATCH_CNT;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        merge_schedule_interval = tenant_config->ob_compaction_schedule_interval;
        enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
        enable_adaptive_merge_schedule_ = tenant_config->_enable_adaptive_merge_schedule;
        fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
        schedule_batch_size = tenant_config->compaction_schedule_tablet_batch_cnt;
      }
    } // end of ObTenantConfigGuard
    if (OB_FAIL(timer_task_mgr_.restart_scheduler_timer_task(merge_schedule_interval))) {
      LOG_WARN("failed to restart scheduler timer", K(ret));
    } else {
      batch_size_mgr_.set_tablet_batch_size(schedule_batch_size);
    }
  }
  return ret;
}

int ObTenantTabletScheduler::get_min_data_version(uint64_t &min_data_version)
{
  int ret = OB_SUCCESS;
  min_data_version = ATOMIC_LOAD(&min_data_version_);
  if (0 == min_data_version) { // force call GET_MIN_DATA_VERSION
    uint64_t compat_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN("fail to get data version", KR(ret));
    } else {
      uint64_t old_version = ATOMIC_LOAD(&min_data_version_);
      while (old_version < compat_version) {
        if (ATOMIC_BCAS(&min_data_version_, old_version, compat_version)) {
          // success to assign data version
          break;
        } else {
          old_version = ATOMIC_LOAD(&min_data_version_);
        }
      } // end of while
    }
    if (OB_SUCC(ret)) {
      min_data_version = ATOMIC_LOAD(&min_data_version_);
    }
  }
  return ret;
}

int ObTenantTabletScheduler::mtl_init(ObTenantTabletScheduler* &scheduler)
{
  return scheduler->init();
}

void ObTenantTabletScheduler::stop()
{
  is_stop_ = true;
  timer_task_mgr_.stop();
  stop_major_merge();
}

int ObTenantTabletScheduler::update_upper_trans_version_and_gc_sstable()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler not init", K(ret));
  } else if (OB_FAIL(gc_sst_tablet_iter_.build_iter(get_schedule_batch_size()))) {
    LOG_WARN("failed to init iterator", K(ret));
  }

  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(gc_sst_tablet_iter_.get_next_ls(ls_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), K(gc_sst_tablet_iter_));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else if (ls->is_stopped()) {
    } else if (OB_TMP_FAIL(try_update_upper_trans_version_and_gc_sstable(*ls, gc_sst_tablet_iter_))) {
      gc_sst_tablet_iter_.skip_cur_ls();
      LOG_WARN("failed to update upper trans version", K(tmp_ret), "ls_id", ls->get_ls_id());
    }
  }
  return ret;
}

int ObTenantTabletScheduler::try_update_upper_trans_version_and_gc_sstable(
    ObLS &ls,
    ObCompactionScheduleIterator &iter)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  common::ObTabletID tablet_id;
  bool ls_is_migration = false;
  int64_t rebuild_seq = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_handle));
      }
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
    } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
    } else if (tablet_id.is_special_merge_tablet()) {
    } else if (!tablet->get_tablet_meta().ha_status_.is_none()) {
    } else if (OB_FAIL(ls.check_ls_migration_status(ls_is_migration, rebuild_seq))) {
      LOG_WARN("failed to check ls migration status", K(ret), K(ls_id));
    } else if (ls_is_migration) {
    } else {
      int64_t multi_version_start = 0;
      int tmp_ret = OB_SUCCESS;
      bool need_update = false; // need update table store
      /*
       * 1. upper_trans_version calculated from ls is invalid when ls is rebuilding, use rebuild_seq to prevent concurrency bug.
       * 2. new_upper_trans array comes from old table store, use end_scn of last minor to check if table store is updated by other thread.
       */
      ObSEArray<int64_t, 8> new_upper_trans;
      new_upper_trans.set_attr(ObMemAttr(MTL_ID(), "NewUpTxnVer"));
      UpdateUpperTransParam upper_trans_param;
      upper_trans_param.new_upper_trans_ = &new_upper_trans;
      if (OB_TMP_FAIL(ObGCUpperTransHelper::check_need_gc_or_update_upper_trans_version(ls, *tablet, multi_version_start, upper_trans_param, need_update))) {
        LOG_WARN("faild to check need gc or update", K(tmp_ret), K(ls_id), K(tablet_id));
      } else if (need_update) {
        ObArenaAllocator tmp_arena("RmOldTblTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        ObStorageSchema *storage_schema = nullptr;
        if (OB_TMP_FAIL(tablet->load_storage_schema(tmp_arena, storage_schema))) {
          LOG_WARN("failed to load storage schema", K(tmp_ret), K(tablet));
        } else {
          ObUpdateTableStoreParam param(tablet->get_snapshot_version(), multi_version_start, storage_schema, rebuild_seq, upper_trans_param);
          ObTabletHandle new_tablet_handle; // no use here
          if (OB_TMP_FAIL(ls.update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
            LOG_WARN("failed to update table store", K(tmp_ret), K(param), K(ls_id), K(tablet_id));
          } else {
            FLOG_INFO("success to remove old table in table store", K(tmp_ret), K(ls_id),
                K(tablet_id), K(multi_version_start), KPC(tablet));
          }
        }
        ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
      }
    }
  } // end while
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets_minor()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(minor_ls_tablet_iter_.build_iter(get_schedule_batch_size()))) {
    LOG_WARN("failed to init iterator", K(ret));
  } else {
    LOG_INFO("start schedule all tablet minor merge", K(minor_ls_tablet_iter_));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(minor_ls_tablet_iter_.get_next_ls(ls_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), K(minor_ls_tablet_iter_));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else {
      const ObLSID &ls_id = ls->get_ls_id();
      if (OB_TMP_FAIL(schedule_ls_minor_merge(ls_handle))) {
        LOG_TRACE("meet error when schedule", K(tmp_ret), K(minor_ls_tablet_iter_));
        minor_ls_tablet_iter_.skip_cur_ls();
        if (!schedule_ignore_error(tmp_ret)) {
          LOG_WARN("failed to schedule ls minor merge", K(tmp_ret), K(ls_id));
        }
      }
    }
  } // end of while
  return ret;
}

int ObTenantTabletScheduler::check_ls_compaction_finish(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->check_ls_compaction_dag_exist_with_cancel(ls_id, exist))) {
    LOG_WARN("failed to check ls compaction dag", K(ret), K(ls_id));
  } else if (exist) {
    // the compaction dag exists, need retry later.
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTenantTabletScheduler::gc_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->gc_info())) {
    LOG_WARN("failed to gc in ObScheduleSuspectInfoMgr", K(ret));
  } else if (OB_FAIL(MTL(ObDagWarningHistoryManager *)->gc_info())) {
    LOG_WARN("failed to gc in ObDagWarningHistoryManager", K(ret));
  } else if (OB_FAIL(MTL(ObTenantSSTableMergeInfoMgr *)->gc_info())) {
    LOG_WARN("failed to gc in ObTenantSSTableMergeInfoMgr", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::set_max()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->set_max(ObScheduleSuspectInfoMgr::cal_max()))) {
    LOG_WARN("failed to set_max int ObScheduleSuspectInfoMgr", K(ret));
  } else if (OB_FAIL(MTL(ObDagWarningHistoryManager *)->set_max(ObDagWarningHistoryManager::cal_max()))) {
    LOG_WARN("failed to set_max in ObDagWarningHistoryManager", K(ret));
  } else if (OB_FAIL(MTL(ObTenantSSTableMergeInfoMgr *)->set_max(ObTenantSSTableMergeInfoMgr::cal_max()))) {
    LOG_WARN("failed to set_max int ObTenantSSTableMergeInfoMgr", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::refresh_tenant_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else {
    // refresh is_restore
    if (REACH_TENANT_TIME_INTERVAL(REFRESH_TENANT_STATUS_INTERVAL)) {
      ATOMIC_SET(&is_restore_, false);
      ObSchemaGetterGuard schema_guard;
      const ObSimpleTenantSchema *tenant_schema = nullptr;
      const int64_t tenant_id = MTL_ID();
      if (OB_TMP_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(tmp_ret), K(tenant_id));
      } else if (OB_TMP_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(tmp_ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        tmp_ret = OB_SCHEMA_ERROR;
        LOG_WARN("tenant schema is null", K(tmp_ret));
      } else if (tenant_schema->is_restore()) {
        ATOMIC_SET(&is_restore_, true);
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_build_bloomfilter(
    const uint64_t table_id,
    const blocksstable::MacroBlockId &macro_id,
    const int64_t prefix_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || prefix_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(macro_id), K(prefix_len));
  } else {
    ObBloomFilterBuildTask task(MTL_ID(), table_id, macro_id, prefix_len);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_LIKELY(OB_EAGAIN == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to add bloomfilter build task", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge(const int64_t broadcast_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(broadcast_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(broadcast_version), K(ret));
  } else if (broadcast_version <= get_frozen_version()) {
  } else {
    int tmp_ret = OB_SUCCESS;
    // add progress first before change frozen_version_
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->add_progress(broadcast_version))) {
      LOG_WARN("failed to add progress", K(tmp_ret), K(broadcast_version));
    }
    {
      obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version_ = broadcast_version;
    }

    LOG_INFO("schedule merge major version", K(broadcast_version));
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->init_progress(broadcast_version))) {
      LOG_WARN("failed to init progress", K(tmp_ret), K(broadcast_version));
    }
    loop_cnt_ = 0;
    clear_error_tablet_cnt();

    schedule_stats_.start_merge(); // set all statistics
    ADD_COMPACTION_EVENT(
        broadcast_version,
        ObServerCompactionEvent::RECEIVE_BROADCAST_SCN,
        schedule_stats_.start_timestamp_,
        "last_merged_version",
        merged_version_);
  }
  return ret;
}

bool ObTenantTabletScheduler::check_weak_read_ts_ready(
    const int64_t &merge_version,
    ObLS &ls)
{
  bool is_ready_for_compaction = false;
  SCN weak_read_scn;

  if (FALSE_IT(weak_read_scn = ls.get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  } else if (weak_read_scn.get_val_for_tx() < merge_version) {
    FLOG_INFO("current slave_read_ts is smaller than freeze_ts, try later",
              "ls_id", ls.get_ls_id(), K(merge_version), K(weak_read_scn));
  } else {
    is_ready_for_compaction = true;
  }
  return is_ready_for_compaction;
}

void ObTenantTabletScheduler::stop_major_merge()
{
  ATOMIC_SET(&major_merge_status_, false);
  LOG_INFO("major merge has been paused!");
}

void ObTenantTabletScheduler::resume_major_merge()
{
  if (!could_major_merge_start()) {
    ATOMIC_SET(&major_merge_status_, true);
    LOG_INFO("major merge has been resumed!");
  }
}

const char *ObProhibitScheduleMediumMap::ProhibitFlagStr[] = {
  "TRANSFER",
  "MEDIUM",
};
ObProhibitScheduleMediumMap::ObProhibitScheduleMediumMap()
  : transfer_flag_cnt_(0),
    lock_(),
    tablet_id_map_()
{
  STATIC_ASSERT(static_cast<int64_t>(ProhibitFlag::FLAG_MAX) == ARRAYSIZEOF(ProhibitFlagStr), "flag str len is mismatch");
}

int ObProhibitScheduleMediumMap::init()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard lock_guard(lock_);
  if (OB_FAIL(tablet_id_map_.create(TABLET_ID_MAP_BUCKET_NUM, "MediumTabletMap", "MediumTabletMap", MTL_ID()))) {
    LOG_WARN("Fail to create prohibit medium tablet id map", K(ret));
  }
  return ret;
}

int ObProhibitScheduleMediumMap::add_flag(const ObTabletID &tablet_id, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  ProhibitFlag tmp_flag = ProhibitFlag::FLAG_MAX;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !is_valid_flag(input_flag))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(input_flag));
  } else {
    obsys::ObWLockGuard lock_guard(lock_);
    if (OB_FAIL(tablet_id_map_.get_refactored(tablet_id, tmp_flag))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(tablet_id_map_.set_refactored(tablet_id, input_flag))) {
          LOG_WARN("failed to stop tablet schedule medium", K(ret), K(tablet_id), K(input_flag));
        }
      } else {
        LOG_WARN("failed to get map", K(ret), K(tablet_id), K(tmp_flag));
      }
    } else if (tmp_flag != input_flag) {
      ret = OB_EAGAIN;
      if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
        LOG_INFO("flag in conflict", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
      }
    } else { // tmp_flag == input_flag
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flag in already exist", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
    }
  }
  return ret;
}

int ObProhibitScheduleMediumMap::clear_flag(const ObTabletID &tablet_id, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !is_valid_flag(input_flag))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(input_flag));
  } else {
    obsys::ObWLockGuard lock_guard(lock_);
    if (OB_FAIL(inner_clear_flag_(tablet_id, input_flag))) {
      LOG_WARN("failed to inner clear flag", K(ret), K(tablet_id), K(input_flag));
    }
  }
  return ret;
}

int ObProhibitScheduleMediumMap::batch_add_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(ProhibitFlag::TRANSFER != input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, batch_add_flags only support TRANSFER now", K(ret), K(input_flag));
  } else if (OB_FAIL(inner_batch_check_tablets_not_prohibited_(tablet_ids))) {
    LOG_WARN("failed to check all tablets not prohibited", K(ret), K(tablet_ids));
  } else if (OB_FAIL(inner_batch_add_tablets_prohibit_flags_(tablet_ids, input_flag))){
    LOG_WARN("failed to add tablets prohibit_flags", K(ret), K(tablet_ids), K(input_flag));
  } else if (ProhibitFlag::TRANSFER == input_flag){
    ++transfer_flag_cnt_;
  }
  return ret;
}

int ObProhibitScheduleMediumMap::batch_clear_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ProhibitFlag::TRANSFER != input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, batch_clear_flags only support TRANSFER now", K(ret), K(input_flag));
  } else {
    const int64_t tablets_cnt = tablet_ids.count();
    obsys::ObWLockGuard lock_guard(lock_);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablets_cnt; idx++) {
      const ObTabletID &tablet_id = tablet_ids.at(idx);
      if (OB_FAIL(inner_clear_flag_(tablet_id, input_flag))) {
        LOG_WARN("failed to clear transfer flag", K(ret), K(tablet_id), K(input_flag));
      }
    }
    if (OB_SUCC(ret) && ProhibitFlag::TRANSFER == input_flag) {
      --transfer_flag_cnt_;
    }
  }
  return ret;
}

void ObProhibitScheduleMediumMap::destroy()
{
  obsys::ObWLockGuard lock_guard(lock_);
  transfer_flag_cnt_ = 0;
  if (tablet_id_map_.created()) {
    tablet_id_map_.destroy();
  }
}

int64_t ObProhibitScheduleMediumMap::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  obsys::ObRLockGuard lock_guard(lock_);
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (0 == tablet_id_map_.size()) {
    // do nothing
  } else {
    J_ARRAY_START();
    int64_t idx = 0;
    FOREACH_X(it, tablet_id_map_, OB_SUCC(ret)) {
      const ObTabletID &tablet_id = it->first;
      if (OB_UNLIKELY(!is_valid_flag(it->second))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prihibit medium flag is not expected", K(ret), "flag", it->second);
      } else {
        J_OBJ_START();
        J_KV(K(idx), "tablet_id", tablet_id.id(), "flag", ProhibitFlagStr[static_cast<int64_t>(it->second)]);
        J_OBJ_END();
        ++idx;
      }
    }
    J_ARRAY_END();
  }
  return pos;
}

// ATTENTION: hold lock outside !!
int ObProhibitScheduleMediumMap::inner_batch_check_tablets_not_prohibited_(const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const int64_t tablets_cnt = tablet_ids.count();
  ProhibitFlag tmp_flag = ProhibitFlag::FLAG_MAX;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tablets_cnt; idx++) {
    const ObTabletID &tablet_id = tablet_ids.at(idx);
    if (OB_UNLIKELY(!tablet_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_id_map_.get_refactored(tablet_id, tmp_flag))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get flag from tablet_id_map", K(ret), K(tablet_id), K(tmp_flag));
      }
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("tablet is already set flag", K(ret), K(tablet_id), K(tmp_flag));
    }
  }
  return ret;
}

// ATTENTION: hold lock outside !!
int ObProhibitScheduleMediumMap::inner_batch_add_tablets_prohibit_flags_(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t tablets_cnt = tablet_ids.count();
  int64_t idx = 0;
  for (idx = 0; OB_SUCC(ret) && idx < tablets_cnt; idx++) {
    const ObTabletID &tablet_id = tablet_ids.at(idx);
    if (OB_UNLIKELY(!tablet_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_id_map_.set_refactored(tablet_id, input_flag))) {
      LOG_WARN("failed to add flag for tablet", K(ret), K(tablet_id), K(input_flag));
      // set partially, rollback. suppose tablet_ids.at(idx) not set input_flag
      for (idx--; idx >= 0; idx--) {
        const ObTabletID &tmp_tablet_id = tablet_ids.at(idx); // checked valid before
        if (OB_TMP_FAIL(inner_clear_flag_(tmp_tablet_id, input_flag))) {
          LOG_ERROR("failed clear transfer flag", K(tmp_ret), K(tmp_tablet_id), K(input_flag));
        }
      }
    }
  }
  return ret;
}

// ATTENTION: hold lock outside !!
int ObProhibitScheduleMediumMap::inner_clear_flag_(const ObTabletID &tablet_id, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  ProhibitFlag tmp_flag = ProhibitFlag::FLAG_MAX;
  if (OB_FAIL(tablet_id_map_.get_refactored(tablet_id, tmp_flag))) {
    LOG_ERROR("failed to get from map", K(ret), K(tablet_id), K(tmp_flag));
  } else if (OB_UNLIKELY(tmp_flag != input_flag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task do not match", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
  } else if (OB_FAIL(tablet_id_map_.erase_refactored(tablet_id))) {
    LOG_ERROR("failed to resume tablet schedule medium", K(ret), K(tablet_id), K(input_flag));
  }
  return ret;
}

int64_t ObProhibitScheduleMediumMap::get_transfer_flag_cnt() const
{
  obsys::ObRLockGuard lock_guard(lock_);
  return transfer_flag_cnt_;
}

int ObTenantTabletScheduler::stop_tablets_schedule_medium(const ObIArray<ObTabletID> &tablet_ids, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObProhibitScheduleMediumMap::ProhibitFlag::MEDIUM == input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, input flag could not be MEDIUM", K(ret), K(input_flag));
  } else if (OB_FAIL(prohibit_medium_map_.batch_add_flags(tablet_ids, input_flag))) {
    LOG_WARN("failed to add flag for stopping medium", K(ret), K(tablet_ids), K(input_flag));
  } else {
    LOG_INFO("stopped tablets schedule medium", K(ret), K(tablet_ids), K(input_flag));
  }
  return ret;
}

// When executing the medium task, set the flag in the normal task process of the log stream
int ObTenantTabletScheduler::tablet_start_schedule_medium(const ObTabletID &tablet_id, bool &tablet_could_schedule_medium)
{
  int ret = OB_SUCCESS;
  tablet_could_schedule_medium = false;
  if (OB_FAIL(prohibit_medium_map_.add_flag(tablet_id, ObProhibitScheduleMediumMap::ProhibitFlag::MEDIUM))) {
    if (OB_EAGAIN == ret) {
      tablet_could_schedule_medium = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to add flag for tablet schedule medium", K(ret), K(tablet_id));
    }
  } else {
    tablet_could_schedule_medium = true;
  }
  return ret;
}

int ObTenantTabletScheduler::clear_tablets_prohibit_medium_flag(const ObIArray<ObTabletID> &tablet_ids, const ObProhibitScheduleMediumMap::ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prohibit_medium_map_.batch_clear_flags(tablet_ids, input_flag))) {
    LOG_WARN("failed to clear tablets prohibit medium flag", K(ret), K(tablet_ids), K(input_flag));
  } else {
    LOG_INFO("allow tablets schedule medium", K(ret), K(input_flag));
  }
  return ret;
}

int64_t ObTenantTabletScheduler::get_frozen_version() const
{
  obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_;
}

bool ObTenantTabletScheduler::check_tx_table_ready(ObLS &ls, const SCN &check_scn)
{
  int ret = OB_SUCCESS;
  bool tx_table_ready = false;
  SCN max_decided_scn;
  if (OB_FAIL(ls.get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get max decided log_ts", K(ret), "ls_id", ls.get_ls_id());
  } else if (check_scn <= max_decided_scn) {
    tx_table_ready = true;
    LOG_INFO("tx table ready", "sstable_end_scn", check_scn, K(max_decided_scn));
  }

  return tx_table_ready;
}

int ObTenantTabletScheduler::schedule_merge_dag(
    const ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (is_major_merge_type(merge_type) && !tablet.is_row_store()) {
    ObCOMergeDagParam param;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet.get_tablet_meta().tablet_id_;
    param.merge_type_ = merge_type;
    param.merge_version_ = merge_snapshot_version;
    param.compat_mode_ = tablet.get_tablet_meta().compat_mode_;
    param.transfer_seq_ = tablet.get_tablet_meta().transfer_info_.transfer_seq_;
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_co_merge_dag_net(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(ret));
      }
    }
    FLOG_INFO("chaser debug schedule co merge dag", K(ret), K(param), K(tablet.is_row_store()));
  } else {
    ObTabletMergeDagParam param;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet.get_tablet_meta().tablet_id_;
    param.merge_type_ = merge_type;
    param.merge_version_ = merge_snapshot_version;
    param.transfer_seq_ = tablet.get_tablet_meta().transfer_info_.transfer_seq_;
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_meta_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    bool &has_created_dag)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  has_created_dag = false;

  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else {
    const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const int64_t last_major_snapshot_version = tablet->get_last_major_snapshot_version();
    int64_t max_sync_medium_scn = 0;
    ObArenaAllocator allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;

    // check medium list
    if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_list))) {
      LOG_WARN("failed to read medium info list", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
        *tablet, *medium_list, max_sync_medium_scn))) {
      LOG_WARN("failed to get max sync medium snapshot", K(ret), K(ls_id), K(tablet_id));
    } else if ((nullptr != medium_list && medium_list->size() > 0)
             || max_sync_medium_scn > last_major_snapshot_version) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("tablet exists unfinished medium info, no need to do meta merge", K(ret), K(ls_id), K(tablet_id),
          K(last_major_snapshot_version), K(max_sync_medium_scn), KPC(medium_list));
    } else {
      LOG_INFO("start schedule meta merge", K(ls_id), K(tablet_id), KPC(tablet)); // tmp log, remove later
      ObGetMergeTablesParam param;
      ObGetMergeTablesResult result;
      param.merge_type_ = META_MAJOR_MERGE;
      if (OB_FAIL(ObAdaptiveMergePolicy::get_meta_merge_tables(
              param,
              *ls_handle.get_ls(),
              *tablet,
              result))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to get meta merge tables", K(ret), K(param), K(tablet_id));
        }
      } else if (FALSE_IT(result.merge_version_ = result.version_range_.snapshot_version_)) {
      } else if (OB_UNLIKELY(tablet->get_multi_version_start() > result.merge_version_)) {
        ret = OB_SNAPSHOT_DISCARDED;
        LOG_WARN("multi version data is discarded, should not compaction now", K(ret), K(ls_id), K(tablet_id),
          K(result.merge_version_));
      } else if (!tablet->is_row_store()) {
        ObCOMergeDagParam dag_param;
        dag_param.ls_id_ = ls_id;
        dag_param.tablet_id_ = tablet->get_tablet_meta().tablet_id_;
        dag_param.merge_type_ = META_MAJOR_MERGE;
        dag_param.merge_version_ = result.merge_version_;
        dag_param.compat_mode_ = tablet->get_tablet_meta().compat_mode_;
        dag_param.transfer_seq_ = tablet->get_tablet_meta().transfer_info_.transfer_seq_;
        if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_co_merge_dag_net(dag_param))) {
          if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
            LOG_WARN("failed to schedule tablet merge dag", K(ret), K(dag_param));
          }
        }
        FLOG_INFO("chaser debug schedule co merge dag", K(ret), K(dag_param), K(tablet->is_row_store()));
      } else {
        ObTabletMergeDagParam dag_param(META_MAJOR_MERGE, ls_id, tablet_id,
            tablet->get_tablet_meta().transfer_info_.transfer_seq_);
        dag_param.merge_version_ = result.merge_version_;
        ObTabletMergeExecuteDag *schedule_dag = nullptr;
        if (OB_FAIL(schedule_merge_execute_dag<ObTabletMergeExecuteDag>(dag_param, ls_handle, tablet_handle, result, schedule_dag))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule tablet meta merge dag", K(ret), K(dag_param));
          }
        }
      }

      if (OB_SUCC(ret)) {
        MTL(ObTenantTabletStatMgr *)->clear_tablet_stat(ls_id, tablet_id);
        has_created_dag = true;
        LOG_INFO("success to schedule meta merge", K(ret), K(tablet_id), "is_row_store", tablet->is_row_store());
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_medium_merge(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    bool &succ_create_dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_id));
  } else {
    bool need_merge = false;
    ObLS &ls = *ls_handle.get_ls();
    if (OB_FAIL(ObTabletMergeChecker::check_ls_state_in_major(ls, need_merge))) {
      LOG_WARN("failed to check ls state", K(ret), K(ls));
    } else if (!need_merge) {
      // no need to merge, do nothing
      ret = OB_STATE_NOT_MATCH;
    } else {
      ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
      int64_t merge_version = scheduler->get_frozen_version();
      if (ObTenantTabletScheduler::INIT_COMPACTION_SCN == merge_version) {
        merge_version = 0;
      } else {
        // TODO(chengkong): if merge_version > scheduler->merge_version_, whether to update progress?
      }
      bool unused_all_ls_weak_read_ts_ready = false;
      ObTenantTabletMediumParam medium_param(merge_version, true /*is_tombstone_*/);
      if (OB_FAIL(scheduler->prepare_ls_medium_merge(ls, medium_param, unused_all_ls_weak_read_ts_ready))) {
        LOG_WARN("failed to prepare ls medium merge", K(ret), K(medium_param), K(unused_all_ls_weak_read_ts_ready));
      } else {
        ObCompactionScheduleTimeGuard tablet_time_guard;
        ObTabletSchedulePair schedule_pair;
        bool unused_tablet_merge_finish = false;
        bool medium_clog_submitted = false;
        // ATTENTION!!! load weak ts before get tablet
        const share::SCN &weak_read_ts = ls.get_ls_wrs_handler()->get_ls_weak_read_ts();
        const ObLSID &ls_id = ls.get_ls_id();
        ObTabletHandle tablet_handle; // catch up latest ls status
        if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle))) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        } else if (FALSE_IT(tablet_time_guard.click(ObCompactionScheduleTimeGuard::GET_TABLET))) {
        } else if (OB_FAIL(scheduler->try_schedule_tablet_medium(
                                        ls,
                                        ls_id,
                                        tablet_handle,
                                        weak_read_ts,
                                        medium_param,
                                        false /*scheduler_called*/,
                                        unused_tablet_merge_finish,
                                        medium_clog_submitted,
                                        succ_create_dag,
                                        schedule_pair,
                                        tablet_time_guard))) {
            LOG_WARN("failed to try schedule tablet medium", K(ret), K(ls_handle), K(ls_id), K(tablet_handle),
                K(weak_read_ts), K(medium_param), K(unused_tablet_merge_finish));
        } else if (schedule_pair.need_force_freeze()) {
          // do not trigger force freeze since this function is called by mini merge dag, may casuse deadlock.
          // ls not support async tablet force freeze now.
          LOG_TRACE("tablet need force freeze", K(ret), K(ls_id), K(tablet_id));
        }
        if (medium_clog_submitted && OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->clear_tablet_stat(ls_id, tablet_id))) {
          LOG_WARN("failed to clear tablet stats", K(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::fill_minor_compaction_param(
    const ObTabletHandle &tablet_handle,
    const ObGetMergeTablesResult &result,
    const int64_t total_sstable_cnt,
    const int64_t parallel_dag_cnt,
    const int64_t create_time,
    ObTabletMergeDagParam &param)
{
  int ret = OB_SUCCESS;
  ObCompactionParam &compaction_param = param.compaction_param_;
  compaction_param.add_time_ = create_time;
  compaction_param.sstable_cnt_ = total_sstable_cnt;
  compaction_param.parallel_dag_cnt_ = parallel_dag_cnt;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;

  ObITable *table = nullptr;
  int64_t row_count = 0;
  int64_t macro_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
    table = result.handle_.get_table(i);
    if (OB_UNLIKELY(NULL == table || !table->is_multi_version_minor_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table", K(ret), KPC(table), K(result));
    } else {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      compaction_param.occupy_size_ += sstable->get_occupy_size();
      row_count += sstable->get_row_count();
      macro_count += sstable->get_data_macro_block_count();
      compaction_param.parallel_sstable_cnt_++;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle))) {
      LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(tablet_handle.get_obj()));
    } else {
      compaction_param.estimate_concurrent_count(MINOR_MERGE);
      param.need_swap_tablet_flag_ = ObBasicTabletMergeCtx::need_swap_tablet(*protected_handle, row_count, macro_count);
    }
  }
  return ret;
}

template <class T>
int ObTenantTabletScheduler::schedule_tablet_minor_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
  const int64_t schedule_type_cnt = tablet_id.is_special_merge_tablet() ? TX_TABLE_NO_MAJOR_MERGE_TYPE_CNT : NO_MAJOR_MERGE_TYPE_CNT;
  for (int i = 0; OB_SUCC(ret) && i < schedule_type_cnt; ++i) {
    if (OB_FAIL(schedule_tablet_minor_merge<T>(MERGE_TYPES[i], ls_handle, tablet_handle))) {
      LOG_WARN("fail to schdule minor merge", K(ret), "merge_type", MERGE_TYPES[i], K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

template <class T>
int ObTenantTabletScheduler::schedule_tablet_minor_merge(
    const ObMergeType &merge_type,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
  ObGetMergeTablesParam param;
  ObGetMergeTablesResult result;
  param.merge_type_ = merge_type;
  if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[merge_type](
          param,
          *ls_handle.get_ls(),
          *tablet_handle.get_obj(),
          result))) {
    if (OB_NO_NEED_MERGE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("tablet no need merge", K(ret), K(merge_type), K(tablet_id), K(tablet_handle));
    } else {
      LOG_WARN("failed to check need merge", K(ret), K(merge_type), K(tablet_id), K(tablet_handle));
    }
  } else {
    int64_t minor_compact_trigger = ObPartitionMergePolicy::DEFAULT_MINOR_COMPACT_TRIGGER;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        minor_compact_trigger = tenant_config->minor_compact_trigger;
      }
    }

    ObMinorExecuteRangeMgr minor_range_mgr;
    MinorParallelResultArray parallel_results;
    if (result.handle_.get_count() < minor_compact_trigger) {
      ret = OB_NO_NEED_MERGE;
    } else if (OB_FAIL(minor_range_mgr.get_merge_ranges(ls_id, tablet_id))) {
      LOG_WARN("failed to get merge range", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ObPartitionMergePolicy::generate_parallel_minor_interval(param.merge_type_, minor_compact_trigger, result, minor_range_mgr, parallel_results))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to generate parallel minor dag", K(ret), K(result));
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("tablet no need merge", K(ret), K(merge_type), K(ls_id), K(tablet_id), K(result));
      }
    } else if (parallel_results.empty()) {
      LOG_DEBUG("parallel results is empty, cannot schedule parallel minor merge", K(ls_id), K(tablet_id),
          K(result), K(minor_range_mgr.exe_range_array_));
    } else {
      const int64_t parallel_dag_cnt = minor_range_mgr.exe_range_array_.count() + parallel_results.count();
      const int64_t total_sstable_cnt = result.handle_.get_count();
      const int64_t create_time = common::ObTimeUtility::fast_current_time();
      ObTabletMergeDagParam dag_param(merge_type, ls_id, tablet_id,
          tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_);
      T *schedule_dag = nullptr;
      for (int64_t k = 0; OB_SUCC(ret) && k < parallel_results.count(); ++k) {
        if (OB_UNLIKELY(parallel_results.at(k).handle_.get_count() <= 1)) {
          LOG_WARN("invalid parallel result", K(ret), K(k), K(parallel_results));
        } else if (OB_FAIL(fill_minor_compaction_param(tablet_handle, parallel_results.at(k), total_sstable_cnt, parallel_dag_cnt, create_time, dag_param))) {
          LOG_WARN("failed to fill compaction param for ranking dags later", K(ret), K(k), K(parallel_results.at(k)));
        } else if (OB_FAIL(schedule_merge_execute_dag(dag_param, ls_handle, tablet_handle, parallel_results.at(k), schedule_dag))) {
          LOG_WARN("failed to schedule minor execute dag", K(ret), K(k), K(parallel_results.at(k)));
        } else {
          LOG_INFO("success to schedule tablet minor merge", K(ret), K(ls_id), K(tablet_id),
            "table_cnt", parallel_results.at(k).handle_.get_count(),
            "merge_scn_range", parallel_results.at(k).scn_range_, K(merge_type), KP(schedule_dag));
        }
      } // end of for
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_ddl_major_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLTableMergeDagParam param;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  bool is_major_sstable_exist = false;
  bool has_freezed_ddl_kv = false;
  SCN ddl_commit_scn;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle));
    }
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
          ls_handle.get_ls()->get_ls_id(),
          tablet_handle.get_obj()->get_tablet_meta().tablet_id_,
          true, /* is_full_direct_load */
          direct_load_mgr_handle,
          is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tablet direct load mgr failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
  } else if (FALSE_IT(ddl_commit_scn = direct_load_mgr_handle.get_full_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()))) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->try_flush_ddl_commit_scn(ls_handle, tablet_handle, direct_load_mgr_handle, ddl_commit_scn))) {
    LOG_WARN("try flush ddl commit scn failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->check_has_freezed_ddl_kv(has_freezed_ddl_kv))) {
    LOG_WARN("check has freezed ddl kv failed", K(ret));
  } else if (OB_FAIL(direct_load_mgr_handle.get_full_obj()->prepare_ddl_merge_param(*tablet_handle.get_obj(), param))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("prepare major merge param failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
    }
  } else if (has_freezed_ddl_kv || param.is_commit_) {
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    }
  }
  return ret;
}

// for minor dag, only hold tables_handle(sstable + ref), should not hold tablet(memtable)
template <class T>
int ObTenantTabletScheduler::schedule_merge_execute_dag(
    const ObTabletMergeDagParam &param,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const ObGetMergeTablesResult &result,
    T *&merge_exe_dag,
    const bool add_into_scheduler/* = true*/)
{
  int ret = OB_SUCCESS;
  merge_exe_dag = nullptr;
  const bool emergency = tablet_handle.get_obj()->get_tablet_meta().tablet_id_.is_ls_inner_tablet();

  if (result.handle_.get_count() > 1
        && !ObTenantTabletScheduler::check_tx_table_ready(
        *ls_handle.get_ls(),
        result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...", KR(ret),
        "merge_scn", result.scn_range_.end_scn_);
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->alloc_dag(merge_exe_dag))) {
    LOG_WARN("failed to alloc dag", K(ret));
  } else if (OB_FAIL(merge_exe_dag->prepare_init(
          param,
          tablet_handle.get_obj()->get_tablet_meta().compat_mode_,
          result,
          ls_handle))) {
    LOG_WARN("failed to init dag", K(ret), K(result));
  } else if (add_into_scheduler && OB_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(merge_exe_dag, emergency))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(merge_exe_dag));
    }
  } else {
    LOG_INFO("success to scheudle tablet minor execute dag", K(ret), KP(merge_exe_dag), K(emergency), K(add_into_scheduler));
  }
  if (OB_FAIL(ret) && nullptr != merge_exe_dag) {
    MTL(share::ObTenantDagScheduler *)->free_dag(*merge_exe_dag);
    merge_exe_dag = nullptr;
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_minor_merge(
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  bool need_merge = false;
  bool need_fast_freeze = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  if (OB_FAIL(ObTabletMergeChecker::check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    int tmp_ret = OB_SUCCESS;
    bool schedule_minor_flag = true;
    ObSEArray<ObTabletID, MERGE_BACTH_FREEZE_CNT> need_fast_freeze_tablets;
    need_fast_freeze_tablets.set_attr(ObMemAttr(MTL_ID(), "MinorBatch"));
    int64_t start_time_us = 0;
    while (OB_SUCC(ret)) { // loop all tablet in ls
      bool tablet_merge_finish = false;
      bool need_fast_freeze_flag = false;
      if (OB_FAIL(minor_ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_)) {
      } else if (OB_TMP_FAIL(schedule_tablet_minor(ls_handle, tablet_handle, schedule_minor_flag, need_fast_freeze_flag))) {
        LOG_WARN("failed to schedule tablet minor", KR(tmp_ret), K(ls_id), K(tablet_id));
      }
      if (need_fast_freeze_flag) {
        if (OB_TMP_FAIL(need_fast_freeze_tablets.push_back(tablet_id))) {
          LOG_WARN("failed to push back tablet_id for batch_freeze", KR(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
    } // end of while

    const bool is_sync = true;
    start_time_us = ObClockGenerator::getClock();
    if (need_fast_freeze_tablets.empty()) {
      // empty array. do not need freeze
    } else if (OB_TMP_FAIL(ls.tablet_freeze(checkpoint::INVALID_TRACE_ID, need_fast_freeze_tablets, is_sync))) {
      LOG_WARN("failt to batch freeze tablet", KR(tmp_ret), K(ls_id), K(need_fast_freeze_tablets));
    } else {
      LOG_INFO("fast freeze by batch_tablet_freeze finish",
               KR(tmp_ret),
               "freeze cnt",
               need_fast_freeze_tablets.count(),
               "cost time(ns)",
               common::ObTimeUtility::current_time() - start_time_us);
    }
  } // else
  return ret;
}

// schedule_minor_flag = false means minor dag array is full
// but still need to loop tablet for ddl major & fast freeze
int ObTenantTabletScheduler::schedule_tablet_minor(
  ObLSHandle &ls_handle,
  ObTabletHandle tablet_handle,
  bool &schedule_minor_flag,
  bool &need_fast_freeze_flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_fast_freeze_flag = false;

  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  const ObTablet &tablet = *tablet_handle.get_obj();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ObMergeType::MINOR_MERGE, tablet))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (schedule_minor_flag
      && OB_TMP_FAIL(schedule_tablet_minor_merge<ObTabletMergeExecuteDag>(ls_handle, tablet_handle))) {
    if (OB_SIZE_OVERFLOW == tmp_ret) {
      schedule_minor_flag = false;
    } else if (OB_EAGAIN != tmp_ret) {
      LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  }
  if (!tablet_id.is_ls_inner_tablet()) { // data tablet
    if (OB_TMP_FAIL(schedule_tablet_ddl_major_merge(ls_handle, tablet_handle))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule tablet ddl merge", K(tmp_ret), K(ls_id), K(tablet_handle));
      }
    }

    if (!fast_freeze_checker_.need_check() || tablet_id.is_inner_tablet() || tablet_id.is_ls_inner_tablet()) {
    } else if (OB_TMP_FAIL(fast_freeze_checker_.check_need_fast_freeze(tablet, need_fast_freeze_flag))) {
      LOG_WARN("failed to check need fast freeze", K(tmp_ret), K(tablet_handle));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::get_ls_tablet_medium_list(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    common::ObArenaAllocator &allocator,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const compaction::ObMediumCompactionInfoList *&medium_list,
    share::SCN &weak_read_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE)))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_TRACE("ls not exist", K(ret), K(ls_id));
    } else {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    }
  } else if (FALSE_IT(weak_read_ts = ls_handle.get_ls()->get_ls_wrs_handler()->get_ls_weak_read_ts())) {
    // must get ls weak_read_ts before get tablet
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
        tablet_id,
        tablet_handle,
        0/*timeout_us*/))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->read_medium_info_list(allocator, medium_list))) {
    LOG_WARN("failed to load medium info list", K(ret), K(tablet_id));
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_next_medium_for_leader(
    ObLS &ls,
    ObTabletHandle &tablet_handle,
    const SCN &weak_read_ts,
    const ObMediumCompactionInfoList *medium_info_list,
    const int64_t major_merge_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool tablet_merge_finish = false;

  ObTablet &tablet = *tablet_handle.get_obj();
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool tablet_could_schedule_merge = false;
  bool unused_medium_clog_submitted = false;
  ObMediumCompactionScheduleFunc func(ls, tablet_handle, weak_read_ts, *medium_info_list, &schedule_stats_);
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  if (last_major_snapshot_version > 0 && last_major_snapshot_version >= major_merge_version) {
    tablet_merge_finish = true;
  }
  if (OB_TMP_FAIL(ObTabletMergeChecker::check_could_merge_for_medium(tablet, tablet_could_schedule_merge))) {
    LOG_WARN("failed to check tablet counld schedule merge", K(tmp_ret), K(tablet_id));
  }
  if ((!tablet_merge_finish || get_enable_adaptive_compaction()) // schedule major or adaptive compaction
      && tablet_could_schedule_merge) {
    if (OB_FAIL(func.schedule_next_medium_for_leader(
        tablet_merge_finish ? 0 : major_merge_version, false /*is_tombstone*/, unused_medium_clog_submitted))) { // schedule another round
      LOG_WARN("failed to schedule next medium", K(ret), K(ls_id), K(tablet_id));
      if (OB_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(ls_id, tablet_id,
          share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
        LOG_WARN("failed to add diagnose tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_next_round_for_leader(
    const ObIArray<compaction::ObTabletCheckInfo> &tablet_ls_infos,
    const ObIArray<compaction::ObTabletCheckInfo> &finish_tablet_ls_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool could_major_merge = could_major_merge_start();
  const int64_t major_merge_version = get_frozen_version();
  share::SCN weak_read_ts;
  weak_read_ts.set_invalid();
  for (int64_t i = 0, idx = 0; i < tablet_ls_infos.count(); ++i) {
    const ObLSID &ls_id = tablet_ls_infos.at(i).get_ls_id();
    const ObTabletID &tablet_id = tablet_ls_infos.at(i).get_tablet_id();
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObArenaAllocator tmp_allocator("MediumChecker", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    bool tablet_could_schedule_medium = false;
    //#TODO @jingshui sort tablet_ls_info with ls id
    if (OB_FAIL(tablet_start_schedule_medium(tablet_id, tablet_could_schedule_medium))) {
      LOG_WARN("failed to set start schedule medium", K(ret), K(tmp_ret), K(ls_id));
    } else if (!tablet_could_schedule_medium) {
      if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
        LOG_INFO("tenant is blocking schedule medium", KR(ret), K(MTL_ID()), K(ls_id));
      }
    } else if (idx < finish_tablet_ls_infos.count() && tablet_ls_infos.at(i) == finish_tablet_ls_infos.at(idx)) {
      if (!could_major_merge) {
        // do nothing
      } else if (OB_TMP_FAIL(get_ls_tablet_medium_list(ls_id, tablet_id, tmp_allocator, ls_handle, tablet_handle, medium_list, weak_read_ts))) {
        LOG_WARN("failed to get_ls_tablet_medium_list", K(tmp_ret), K(ls_handle), K(tablet_handle), KPC(medium_list));
      } else if (OB_TMP_FAIL(schedule_next_medium_for_leader(*ls_handle.get_ls(), tablet_handle, weak_read_ts, medium_list, major_merge_version))) {
        LOG_WARN("failed to schedule_next_medium_for_leader", K(tmp_ret), K(ls_handle), K(tablet_handle), KPC(medium_list));
      }
      ++idx;
    }
    // clear flags set by ls_start_schedule_medium
    //#TODO @jingshui sort tablet_ls_info with ls id
    if (tablet_could_schedule_medium
        && OB_TMP_FAIL(clear_prohibit_medium_flag(tablet_id, ObProhibitScheduleMediumMap::ProhibitFlag::MEDIUM))) {
      LOG_WARN("failed to clear prohibit schedule medium flag", K(tmp_ret), K(ret), K(ls_id));
    }
  } // end of for
  return ret;
}

bool ObTenantTabletScheduler::get_enable_adaptive_compaction()
{
  int ret = OB_SUCCESS;
  bool enable_adaptive_compaction = enable_adaptive_compaction_;
  if (!enable_adaptive_compaction || !enable_adaptive_merge_schedule_) {
    // do nothing
#ifdef ENABLE_DEBUG_LOG
  } else if (GCONF.enable_crazy_medium_compaction) {
    enable_adaptive_compaction = true;
    LOG_DEBUG("set crazy medium, set enable_adaptive_compaction = true");
#endif
  } else if (MTL(ObTenantTabletStatMgr *)->is_high_tenant_cpu_load()) {
    enable_adaptive_compaction = false;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      FLOG_INFO("disable adaptive compaction due to the high load CPU", K(ret));
    }
  }
  return enable_adaptive_compaction;
}

int ObTenantTabletScheduler::try_schedule_tablet_medium(
    ObLS &ls,
    const share::ObLSID &ls_id,
    ObTabletHandle &tablet_handle,
    const share::SCN &weak_read_ts,
    ObTenantTabletMediumParam &param,
    const bool scheduler_called,
    bool &tablet_merge_finish,
    bool &medium_clog_submitted,
    bool &succ_create_dag,
    ObTabletSchedulePair &schedule_pair,
    ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletID tablet_id;
  bool tablet_could_schedule_medium = false;

  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
  } else if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ObMergeType::MEDIUM_MERGE, *tablet))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (param.is_leader_ && param.could_major_merge_
          && OB_TMP_FAIL(tablet_start_schedule_medium(tablet_id, tablet_could_schedule_medium))) {
    LOG_WARN("failed to set start schedule medium", K(ret), K(tmp_ret), K(ls_id), K(tablet_id));
  } else if (FALSE_IT(report_blocking_medium(param.is_leader_, tablet_could_schedule_medium, param.could_major_merge_, ls_id))) {
  } else if (OB_TMP_FAIL(schedule_tablet_medium(
                          ls,
                          tablet_handle,
                          weak_read_ts,
                          param,
                          tablet_could_schedule_medium,
                          scheduler_called,
                          tablet_merge_finish,
                          medium_clog_submitted,
                          succ_create_dag,
                          schedule_pair,
                          time_guard))) {
    LOG_WARN("failed to schedule tablet medium", KR(tmp_ret), K(ls_id), K(tablet_id));
  }
  if (tablet_could_schedule_medium
          && OB_TMP_FAIL(clear_prohibit_medium_flag(tablet_id, ObProhibitScheduleMediumMap::ProhibitFlag::MEDIUM))) {
    // clear flags set by tablet_start_schedule_medium
    LOG_WARN("failed to clear prohibit schedule medium flag", K(tmp_ret), K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTenantTabletScheduler::prepare_ls_medium_merge(
    ObLS &ls,
    ObTenantTabletMediumParam &param,
    bool &all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  if (could_major_merge_start()) {
    param.could_major_merge_ = true;
  } else if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
    LOG_INFO("major merge should not schedule", K(ret), K(param));
  }
  // check weak_read_ts
  if (param.merge_version_ >= 0) {
    // the check here does not affect scheduling // diagnose info will be added in check_need_medium_merge
    if (check_weak_read_ts_ready(param.merge_version_, ls)) { // weak read ts ready
      if (OB_FAIL(ObMediumCompactionScheduleFunc::is_election_leader(ls_id, param.is_leader_))) {
        if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
        }
      }
    } else {
      all_ls_weak_read_ts_ready = false;
    }
  }
  param.enable_adaptive_compaction_ = get_enable_adaptive_compaction();
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_medium_merge(
    const int64_t merge_version,
    ObLSHandle &ls_handle,
    bool &all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_merge = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  ObTenantTabletMediumParam medium_param(merge_version);
  if (OB_FAIL(ObTabletMergeChecker::check_ls_state_in_major(ls, need_merge))) {
    LOG_WARN("failed to check ls can schedule medium", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing // TODO(@jingshui): add diagnose info
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("could not to merge now", K(ret), K(need_merge), K(ls_id));
  } else if (OB_FAIL(prepare_ls_medium_merge(ls, medium_param, all_ls_weak_read_ts_ready))) {
    LOG_WARN("failed to prepare ls medium merge", K(ret), K(ls), K(medium_param), K(all_ls_weak_read_ts_ready));
  } else {
    ObCompactionScheduleTimeGuard ls_time_guard;
    ObCompactionScheduleTimeGuard tablet_time_guard;
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    ObSEArray<ObTabletSchedulePair, MERGE_BACTH_FREEZE_CNT> need_freeze_tablets;
    need_freeze_tablets.set_attr(ObMemAttr(MTL_ID(), "MediumBatch"));
    ObSEArray<ObTabletID, 64> batched_clear_stat_tablets;
    batched_clear_stat_tablets.set_attr(ObMemAttr(MTL_ID(), "BatchClearTblts"));
    ObTabletSchedulePair schedule_pair;

    while (OB_SUCC(ret)) { // loop all tablet in ls
      tablet_time_guard.reuse();
      bool tablet_merge_finish = false;
      bool medium_clog_submitted = false;
      bool succ_create_dag = false;
      // ATTENTION!!! load weak ts before get tablet
      const share::SCN &weak_read_ts = ls.get_ls_wrs_handler()->get_ls_weak_read_ts();
      schedule_pair.reset();

      if (OB_FAIL(medium_ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (FALSE_IT(tablet_time_guard.click(ObCompactionScheduleTimeGuard::GET_TABLET))) {
      } else if (OB_FAIL(try_schedule_tablet_medium(
                          ls,
                          ls_id,
                          tablet_handle,
                          weak_read_ts,
                          medium_param,
                          true /*scheduler_called*/,
                          tablet_merge_finish,
                          medium_clog_submitted,
                          succ_create_dag,
                          schedule_pair,
                          tablet_time_guard))) {
        LOG_WARN("failed to try schedule tablet medium", K(ret), K(ls_handle), K(ls_id),
            K(tablet_handle), K(weak_read_ts), K(medium_param), K(tablet_merge_finish));
      } else if (medium_clog_submitted) {
        tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
        if (OB_TMP_FAIL(batched_clear_stat_tablets.push_back(tablet_id))) {
          LOG_WARN("failed to add tablet to clear stat", K(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
      medium_ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
      if (schedule_pair.need_force_freeze()) {
        if (OB_TMP_FAIL(need_freeze_tablets.push_back(schedule_pair))) {
          LOG_WARN("failed to push back tablet_id for batch_freeze", KR(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
      ls_time_guard.add_time_guard(tablet_time_guard);
    } // end of while

    // most tablets will clear failed since the capacity of ObTenantTabletStatMgr is limited
    if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->batch_clear_tablet_stat(ls_id, batched_clear_stat_tablets))) {
      LOG_WARN("failed to batch clear tablet stats", K(tmp_ret), K(ls_id));
    }
    if (OB_FAIL(ret) || need_freeze_tablets.empty()) {
    } else if (OB_TMP_FAIL(schedule_batch_freeze_dag(ls_id,
                                                     need_freeze_tablets))) {
      LOG_WARN("failed to schedule batch force freeze tablets dag", K(tmp_ret), K(ls_id),
               "tablet_count", need_freeze_tablets.count());
    }

    ls_time_guard.click(ObCompactionScheduleTimeGuard::FAST_FREEZE);
    time_guard_.add_time_guard(ls_time_guard);
  } // else
  return ret;
}

int ObTenantTabletScheduler::update_tablet_report_status(
  const bool tablet_merge_finish,
  ObLS &ls,
  ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  if (OB_UNLIKELY(tablet.get_tablet_meta().report_status_.found_cg_checksum_error_)) {
    //TODO(@DanLing) solve this situation, but how to deal with the COSSTable that without the all column group?
    ret = OB_CHECKSUM_ERROR;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_ERROR("tablet found cg checksum error, skip to schedule merge", K(ret), K(tablet));
    }
  } else if (tablet_merge_finish) {
    int tmp_ret = OB_SUCCESS;
    if (tablet.get_tablet_meta().report_status_.need_report()) {
      if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater *)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
        LOG_WARN("failed to submit tablet update task to report", K(tmp_ret), K(tablet_id), K(ls_id));
      } else if (OB_TMP_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
        LOG_WARN("failed to update tablet report status", K(tmp_ret), K(MTL_ID()), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_medium(
  ObLS &ls,
  ObTabletHandle &tablet_handle,
  const share::SCN &weak_read_ts,
  ObTenantTabletMediumParam &param,
  const bool tablet_could_schedule_medium,
  const bool scheduler_called,
  bool &tablet_merge_finish,
  bool &medium_clog_submitted,
  bool &succ_create_dag,
  ObTabletSchedulePair &schedule_pair,
  ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTablet &tablet = *tablet_handle.get_obj();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool need_diagnose = false;
  bool tablet_could_schedule_merge = false;
  succ_create_dag = false;
  schedule_pair.reset();

  if (tablet_could_schedule_medium
      && OB_TMP_FAIL(ObTabletMergeChecker::check_could_merge_for_medium(tablet, tablet_could_schedule_merge))) {
    LOG_WARN("failed to check tablet counld schedule merge", K(tmp_ret), K(tablet_id));
  }

  ObArenaAllocator tmp_allocator("MediumLoop", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const compaction::ObMediumCompactionInfoList *medium_list = nullptr;

  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  if (last_major_snapshot_version > 0 && last_major_snapshot_version >= param.merge_version_) { // merge_version can be zero here.
    tablet_merge_finish = true;
    schedule_stats_.finish_cnt_++;
  }
  if (OB_TMP_FAIL(update_tablet_report_status(tablet_merge_finish, ls, tablet))) {
    LOG_WARN("failed to update tablet report status", K(tmp_ret), K(MTL_ID()), K(tablet_id));
    if (OB_CHECKSUM_ERROR == tmp_ret) {
      ret = tmp_ret;
    }
  } else if (FALSE_IT(time_guard.click(ObCompactionScheduleTimeGuard::UPDATE_TABLET_REPORT_STATUS))){
  }
  LOG_TRACE("schedule tablet medium", K(ret), K(ls_id), K(tablet_id),
            K(tablet_merge_finish), K(last_major_snapshot_version), K(param), K(tablet_could_schedule_merge));
  if (OB_FAIL(ret) || !param.is_leader_ || 0 >= last_major_snapshot_version) {
    // follower or no major: do nothing
    need_diagnose = true;
  } else if (OB_FAIL(tablet.read_medium_info_list(tmp_allocator, medium_list))) {
    LOG_WARN("failed to load medium info list", K(ret), K(tablet_id));
  } else if (FALSE_IT(time_guard.click(ObCompactionScheduleTimeGuard::READ_MEDIUM_INFO))){
  } else if (medium_list->need_check_finish()) { // need check finished
    schedule_stats_.wait_rs_validate_cnt_++;
    if (OB_TMP_FAIL(MTL(ObTenantMediumChecker *)->add_tablet_ls(
        tablet_id, ls_id, medium_list->get_wait_check_medium_scn()))) {
      LOG_WARN("failed to add tablet", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  } else if (param.could_major_merge_
    && (!tablet_merge_finish || param.enable_adaptive_compaction_)
    && tablet_could_schedule_merge) {
    // schedule another round
    ObMediumCompactionScheduleFunc func(ls, tablet_handle, weak_read_ts, *medium_list, &schedule_stats_);
    if (OB_TMP_FAIL(func.schedule_next_medium_for_leader(tablet_merge_finish ? 0 : param.merge_version_, param.is_tombstone_, medium_clog_submitted))) {
      if (OB_NOT_MASTER == tmp_ret) {
        param.is_leader_ = false;
      } else {
        LOG_WARN("failed to schedule next medium", K(tmp_ret), K(ls_id), K(tablet_id));
      }
      need_diagnose = true;
    } else {
      time_guard.click(ObCompactionScheduleTimeGuard::SCHEDULE_NEXT_MEDIUM);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (param.could_major_merge_) {
    if (OB_TMP_FAIL(ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
                ls, tablet, schedule_pair, succ_create_dag,
                param.merge_version_, scheduler_called))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule medium", K(tmp_ret), K(ls_id), K(tablet_id));
      }
      need_diagnose = true;
    } else if (succ_create_dag) {
      ++schedule_stats_.schedule_dag_cnt_;
    } else if (FALSE_IT(time_guard.click(ObCompactionScheduleTimeGuard::SCHEDULE_TABLET_MEDIUM))){
    }
  } else if (param.merge_version_ > merged_version_ // could_major_merge = false
    && OB_TMP_FAIL(ADD_SUSPECT_INFO(
                 MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                 ls_id, tablet_id, ObSuspectInfoType::SUSPECT_SUSPEND_MERGE,
                 param.merge_version_,
                 static_cast<int64_t>(tablet.is_row_store())))) {
    LOG_WARN("failed to add suspect info", K(tmp_ret));
  }

  if (need_diagnose
      && OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(ls_id, tablet_id,
                          share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
    LOG_WARN("failed to add diagnose tablet", K(tmp_ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTenantTabletScheduler::update_major_progress(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  const int64_t major_merged_scn = get_inner_table_merged_scn();
  if (major_merged_scn > merged_version_) {
    FLOG_INFO("last major merge finish", K(merge_version), K(major_merged_scn), K(merged_version_));
    merged_version_ = major_merged_scn;
    if (OB_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress_status(
        merged_version_, share::ObIDag::DAG_STATUS_FINISH))) {
      LOG_WARN("failed to finish progress", KR(ret), K(merge_version));
    }
  }
  if (FAILEDx(MTL(ObTenantCompactionProgressMgr *)->update_progress_status(
      merge_version, share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
    LOG_WARN("failed to update progress", KR(ret), K(merge_version));
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets_medium()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(get_min_data_version(compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing, should not loop tablets
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("compat_version is smaller than DATA_VERSION_4_1_0_0, cannot schedule medium", K(compat_version));
      if (OB_TMP_FAIL(ADD_COMMON_SUSPECT_INFO(MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
              ObSuspectInfoType::SUSPECT_INVALID_DATA_VERSION, compat_version, DATA_VERSION_4_1_0_0))) {
        LOG_WARN("failed to add suspect info", K(tmp_ret));
      }
    }
  } else if (OB_FAIL(medium_ls_tablet_iter_.build_iter(get_schedule_batch_size()))) {
    LOG_WARN("failed to init ls iterator", K(ret));
  } else {
    bool all_ls_weak_read_ts_ready = true;
    int64_t merge_version = get_frozen_version();
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    LOG_INFO("start schedule all tablet merge", K(merge_version), K(medium_ls_tablet_iter_));
    time_guard_.reuse();
    if (INIT_COMPACTION_SCN == merge_version) {
      merge_version = 0;
    } else if (merge_version > merged_version_) {
      (void) update_major_progress(merge_version);
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(medium_ls_tablet_iter_.get_next_ls(ls_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get ls", K(ret), K(ls_handle));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), K(ls));
      } else if (OB_TMP_FAIL(schedule_ls_medium_merge(
                     merge_version, ls_handle,
                     all_ls_weak_read_ts_ready))) {
        medium_ls_tablet_iter_.skip_cur_ls(); // for any errno, skip cur ls
        medium_ls_tablet_iter_.update_merge_finish(false);
        if (OB_SIZE_OVERFLOW == tmp_ret) {
          break;
        } else if (!schedule_ignore_error(tmp_ret)) {
          LOG_WARN("failed to schedule ls merge", K(tmp_ret), KPC(ls));
        }
      }

      // loop tablet_meta table to update smaller report_scn because of migration
      if (OB_SUCC(ret) && medium_ls_tablet_iter_.need_report_scn()) {
        tmp_ret = update_report_scn_as_ls_leader(*ls);

#ifndef ERRSIM
        LOG_INFO("try to update report scn as ls leader", K(tmp_ret), "ls_id", ls->get_ls_id()); // low printing frequency
#endif
      }
      LOG_TRACE("finish schedule ls medium merge", K(tmp_ret), K(ret), K_(medium_ls_tablet_iter), "ls_id", ls->get_ls_id());
    } // end while
    if (OB_TMP_FAIL(after_schedule_tenant_medium(merge_version, all_ls_weak_read_ts_ready))) {
      LOG_WARN("failed to update status after schedule medium", KR(ret));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::after_schedule_tenant_medium(
  const int64_t merge_version,
  bool all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!medium_ls_tablet_iter_.tenant_merge_finish() && merge_version > INIT_COMPACTION_SCN) { // not finish cur merge_version
    if (all_ls_weak_read_ts_ready) { // check schedule Timer Task
      if (schedule_stats_.add_weak_read_ts_event_flag_ && medium_ls_tablet_iter_.is_scan_finish()) { // all ls scan finish
        schedule_stats_.add_weak_read_ts_event_flag_ = false;
        ADD_COMPACTION_EVENT(
            merge_version,
            ObServerCompactionEvent::WEAK_READ_TS_READY,
            ObTimeUtility::fast_current_time(),
            "check_weak_read_ts_cnt", schedule_stats_.check_weak_read_ts_cnt_ + 1);
      }
    } else {
      schedule_stats_.check_weak_read_ts_cnt_++;
    }

    if (medium_ls_tablet_iter_.is_scan_finish()) {
      loop_cnt_++;
      if (REACH_TENANT_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
          merge_version,
          ObServerCompactionEvent::SCHEDULER_LOOP,
          ObTimeUtility::fast_current_time(),
          "schedule_stats",
          schedule_stats_);
      }
    }
  }

  if (OB_SUCC(ret) && medium_ls_tablet_iter_.tenant_merge_finish() && merge_version > merged_version_) {
    merged_version_ = merge_version;
    LOG_INFO("all tablet major merge finish", K(merged_version_), K_(loop_cnt));
    loop_cnt_ = 0;
    DEL_SUSPECT_INFO(MEDIUM_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE);
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress_status(
        merge_version,
        share::ObIDag::DAG_STATUS_FINISH))) {
      LOG_WARN("failed to finish progress", K(tmp_ret), K(merge_version));
    }

    const int64_t current_time = ObTimeUtility::fast_current_time();
    ADD_COMPACTION_EVENT(
          merge_version,
          ObServerCompactionEvent::TABLET_COMPACTION_FINISHED,
          current_time,
          "cost_time",
          current_time - schedule_stats_.start_timestamp_);
  }
  if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL) && prohibit_medium_map_.get_transfer_flag_cnt() > 0) {
    LOG_INFO("tenant is blocking schedule medium", KR(ret), K_(prohibit_medium_map));
  }

  LOG_INFO("finish schedule all tablet merge", K(merge_version), K(schedule_stats_),
      "tenant_merge_finish", medium_ls_tablet_iter_.tenant_merge_finish(),
      K(merged_version_), "is_scan_all_tablet_finish", medium_ls_tablet_iter_.is_scan_finish(), K_(time_guard));
  if (medium_ls_tablet_iter_.is_scan_finish()) {
    schedule_stats_.clear_tablet_cnt();
  }
  return ret;
}

int ObTenantTabletScheduler::try_schedule_tablet_medium_merge(
  const ObLSID &ls_id,
  const common::ObTabletID &tablet_id,
  const bool is_rebuild_column_group)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  bool can_merge = false;
  bool is_election_leader = false;
  ObArenaAllocator tmp_allocator("TabletFreeze", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;

  LOG_INFO("try_schedule_tablet_medium_merge", K(ret), K(tablet_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(tablet_id.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to schedule medium for ls inner tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_min_data_version(compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in compat, can't schedule medium", K(ret), K(compat_version), K(tablet_id));
  } else if (OB_TMP_FAIL(ObMediumCompactionScheduleFunc::is_election_leader(ls_id, is_election_leader))) {
    if (OB_LS_NOT_EXIST == tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
    }
  } else if (!is_election_leader) {
    // not leader, can't schedule
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("not ls leader, can't schedule medium", K(ret), K(ls_id), K(tablet_id), K(is_election_leader));
  } else if (!could_major_merge_start()) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major compaction is suspended", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_FAIL(ObTabletMergeChecker::check_ls_state_in_major(*ls_handle.get_ls(), can_merge))) {
    LOG_WARN("failed to check ls can schedule medium", K(ret), K(ls_handle));
  } else if (!can_merge) {
    // can't merge, do nothing
    LOG_WARN("not support schedule medium for ls", K(ret), K(ls_id), K(can_merge));
  } else {
    const share::SCN &weak_read_ts = ls_handle.get_ls()->get_ls_wrs_handler()->get_ls_weak_read_ts();
    if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 tablet_id, tablet_handle, 0 /*timeout_us*/))) {
      LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->read_medium_info_list(tmp_allocator, medium_info_list))) {
      LOG_WARN("fail to load medium info list", K(ret), K(tablet_handle));
    } else {
      ObMediumCompactionScheduleFunc func(
          *ls_handle.get_ls(), tablet_handle, weak_read_ts, *medium_info_list,
          nullptr /*schedule_stat*/,
          is_rebuild_column_group ? ObAdaptiveMergePolicy::REBUILD_COLUMN_GROUP : ObAdaptiveMergePolicy::USER_REQUEST);
      bool unused_medium_clog_submitted = false;
      const int64_t merge_version = get_frozen_version();
      const int64_t last_major_snapshot_version = tablet_handle.get_obj()->get_last_major_snapshot_version();

      if (OB_UNLIKELY(last_major_snapshot_version <= 0 || last_major_snapshot_version < merge_version)) {
        ret = OB_MAJOR_FREEZE_NOT_FINISHED;
        LOG_WARN("no major sstable or not finish tenant major compaction, can't schedule another medium",
          K(ret), K(ls_id), K(tablet_id), K(last_major_snapshot_version), K(merge_version));
      } else if (medium_info_list->need_check_finish()) {
        ret = OB_MAJOR_FREEZE_NOT_FINISHED;
        LOG_WARN("tablet need check finish, can't schedule another medium", K(ret), K(ls_id), K(tablet_id),
          "wait_check_medium_scn", medium_info_list->get_wait_check_medium_scn());
      } else if (OB_TMP_FAIL(func.schedule_next_medium_for_leader(0/*major_snapshot*/, false/*is_tombstone*/, unused_medium_clog_submitted))) {
        if (OB_EAGAIN != tmp_ret) {
          LOG_WARN("failed to schedule medium", K(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::get_min_dependent_schema_version(int64_t &min_schema_version)
{
  int ret = OB_SUCCESS;
  min_schema_version = OB_INVALID_VERSION;
  share::ObFreezeInfo freeze_info;
  if (OB_FAIL(MTL(storage::ObTenantFreezeInfoMgr*)->get_min_dependent_freeze_info(freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("freeze info is not exist", K(ret));
    } else {
      LOG_WARN("failed to get freeze info", K(ret));
    }
  } else {
    min_schema_version = freeze_info.schema_version_;
  }
  return ret;
}

int ObTenantTabletScheduler::update_report_scn_as_ls_leader(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  bool is_election_leader = false;
  const int64_t major_merged_scn = get_inner_table_merged_scn();
  bool need_merge = false;
  if (OB_FAIL(ObTabletMergeChecker::check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls_id));
  } else if (!need_merge) {
    ret = OB_STATE_NOT_MATCH; // do nothing
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::is_election_leader(ls_id, is_election_leader))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
    }
  } else if (is_election_leader) {
    ObSEArray<ObTabletID, 200> tablet_id_array;
    if (OB_FAIL(ls.get_tablet_svr()->get_all_tablet_ids(true/*except_ls_inner_tablet*/, tablet_id_array))) {
      LOG_WARN("failed to get tablet id", K(ret), K(ls_id));
    } else if (major_merged_scn > INIT_COMPACTION_SCN
        && OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_unequal_report_scn_tablet(
          MTL_ID(), ls_id, major_merged_scn, tablet_id_array))) {
      LOG_WARN("failed to get unequal report scn", K(ret), K(ls_id), K(major_merged_scn));
    }
  } else {
    ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  }
  return ret;
}

void ObTenantTabletScheduler::report_blocking_medium(
  const bool &is_leader,
  const bool &tablet_could_schedule_medium,
  const bool &could_major_merge,
  const share::ObLSID &ls_id)
{
  if (is_leader && !tablet_could_schedule_medium) { // not allow schedule medium
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("tenant is blocking schedule medium", K(MTL_ID()), K(ls_id), K(is_leader), K(could_major_merge));
    }
  }
}

int ObTenantTabletScheduler::schedule_batch_freeze_dag(
    const share::ObLSID &ls_id,
    const common::ObIArray<ObTabletSchedulePair> &tablet_pairs)
{
  int ret = OB_SUCCESS;
  ObBatchFreezeTabletsParam param;

  if (OB_UNLIKELY(!ls_id.is_valid() || tablet_pairs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(ls_id), K(tablet_pairs));
  } else if (FALSE_IT(param.ls_id_ = ls_id)) {
  } else if (OB_FAIL(param.tablet_pairs_.assign(tablet_pairs))) {
    LOG_WARN("failed to assign tablet ids", K(ret));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->create_and_add_dag<ObBatchFreezeTabletsDag>(&param, true/*is_emergency*/))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to create merge dag", K(ret), K(param));
    } else if (OB_EAGAIN == ret) {
      LOG_WARN("curr ls exists batch freeze dag, wait the dag to finish", K(ret), K(ls_id));
    }
  } else {
    LOG_INFO("Succ to create tablet batch freeze dag", K(ret), K(param));
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
