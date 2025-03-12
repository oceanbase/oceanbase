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
#include "ob_schedule_dag_func.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "storage/ob_gc_upper_trans_helper.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"

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
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
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


/*******************************************ObCSReplicaChecksumHelper impl*****************************************/
int ObCSReplicaChecksumHelper::check_column_type(
    const common::ObTabletID &tablet_id,
    const share::ObFreezeInfo &freeze_info,
    const common::ObIArray<int64_t> &column_idxs,
    bool &is_all_large_text_column)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  schema::ObMultiVersionSchemaService *schema_service = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  ObSEArray<ObColDesc, 16> column_descs;
  int64_t save_schema_version = freeze_info.schema_version_;
  const ObTableSchema *table_schema = nullptr;
  is_all_large_text_column = true;

  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet_id), K(column_idxs));
  } else if (OB_UNLIKELY(column_idxs.empty())) {
    // do nothing
  } else if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null schema service", K(ret));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_table_id(*schema_service,
                                                                  tablet_id,
                                                                  freeze_info.schema_version_,
                                                                  table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(tablet_id));
  } else if (OB_FAIL(MTL(ObTenantSchemaService *)->get_schema_service()->retry_get_schema_guard(MTL_ID(),
                                                                                                freeze_info.schema_version_,
                                                                                                table_id,
                                                                                                schema_guard,
                                                                                                save_schema_version))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_UNLIKELY(save_schema_version < freeze_info.schema_version_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("can not use older schema version", K(ret), K(freeze_info), K(save_schema_version), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("Fail to get table schema", K(ret), K(table_id));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K(table_id));
  } else if (OB_FAIL(table_schema->get_multi_version_column_descs(column_descs))) {
    LOG_WARN("failed to get multi version column descs", K(ret), K(tablet_id), KPC(table_schema));
  } else {
    for (int64_t idx = 0; is_all_large_text_column && OB_SUCC(ret) && idx < column_idxs.count(); ++idx) {
      const int64_t cur_col_idx = column_idxs.at(idx);
      const int64_t column_id = column_descs.at(cur_col_idx).col_id_;
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(column_id);

      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null col schema", K(ret));
      } else if (is_lob_storage(col_schema->get_data_type())) {
        LOG_DEBUG("check column type for cs replica", K(cur_col_idx), KPC(col_schema),
                  "rowkey_cnt", table_schema->get_rowkey_column_num(), KPC(table_schema));
      } else {
        is_all_large_text_column = false;
      }
    }
  }
  return ret;
}


/********************************************ObTenantTabletScheduler impl******************************************/
constexpr ObMergeType ObTenantTabletScheduler::MERGE_TYPES[];

ObTenantTabletScheduler::ObTenantTabletScheduler()
 : ObBasicMergeScheduler(),
   is_inited_(false),
   bf_queue_(),
   fast_freeze_checker_(),
   minor_ls_tablet_iter_(false/*is_major*/),
   gc_sst_tablet_iter_(false/*is_major*/),
   prohibit_medium_map_(),
   timer_task_mgr_(),
   batch_size_mgr_(),
   mview_validation_()
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
  ObBasicMergeScheduler::reset();
  bf_queue_.destroy();
  minor_ls_tablet_iter_.reset();
  gc_sst_tablet_iter_.reset();
  prohibit_medium_map_.destroy();
  LOG_INFO("The ObTenantTabletScheduler destroy");
}

int ObTenantTabletScheduler::init()
{
  int ret = OB_SUCCESS;
  bool enable_adaptive_compaction = false;
  bool enable_adaptive_merge_schedule = false;
  int64_t schedule_interval = ObTenantTabletSchedulerTaskMgr::DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  int64_t schedule_batch_size = ObScheduleBatchSizeMgr::DEFAULT_TABLET_BATCH_CNT;

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction = tenant_config->_enable_adaptive_compaction;
      enable_adaptive_merge_schedule = tenant_config->_enable_adaptive_merge_schedule;
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
    IGNORE_RETURN tenant_status_.refresh_tenant_config(enable_adaptive_compaction, enable_adaptive_merge_schedule);
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
    bool enable_adaptive_compaction = false;
    bool enable_adaptive_merge_schedule = false;
    int64_t merge_schedule_interval = ObTenantTabletSchedulerTaskMgr::DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
    int64_t schedule_batch_size = ObScheduleBatchSizeMgr::DEFAULT_TABLET_BATCH_CNT;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        merge_schedule_interval = tenant_config->ob_compaction_schedule_interval;
        enable_adaptive_compaction = tenant_config->_enable_adaptive_compaction;
        enable_adaptive_merge_schedule = tenant_config->_enable_adaptive_merge_schedule;
        fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
        schedule_batch_size = tenant_config->compaction_schedule_tablet_batch_cnt;
      }
    } // end of ObTenantConfigGuard
    (void) tenant_status_.refresh_tenant_config(
      enable_adaptive_compaction,
      enable_adaptive_merge_schedule);

    if (OB_FAIL(timer_task_mgr_.restart_scheduler_timer_task(merge_schedule_interval))) {
      LOG_WARN("failed to restart scheduler timer", K(ret));
    } else {
      batch_size_mgr_.set_tablet_batch_size(schedule_batch_size);
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
    } else if (!tablet->get_tablet_meta().ha_status_.check_allow_read()) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else {
    IGNORE_RETURN tenant_status_.init_or_refresh();
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
  } else if (broadcast_version > get_frozen_version()) {
    update_frozen_version_and_merge_progress(broadcast_version);
    LOG_INFO("schedule merge major version", K(broadcast_version));

    MTL(ObTenantMediumChecker*)->clear_error_tablet_cnt();

    medium_loop_.start_merge(broadcast_version); // set all statistics
  }
  return ret;
}

const char *ObProhibitScheduleMediumMap::ProhibitFlagStr[] = {
  "TRANSFER",
  "MEDIUM",
  "SPLIT",
};
ObProhibitScheduleMediumMap::ObProhibitScheduleMediumMap()
  : transfer_flag_cnt_(0),
    split_flag_cnt_(0),
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
      if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        LOG_INFO("flag in conflict", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
      }
    } else { // tmp_flag == input_flag
      ret = OB_ENTRY_EXIST;
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
  if (OB_UNLIKELY(ProhibitFlag::TRANSFER != input_flag && ProhibitFlag::SPLIT != input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, batch_add_flags only support TRANSFER or SPLIT now", K(ret), K(input_flag));
  } else if (OB_FAIL(inner_batch_check_tablets_not_prohibited_(tablet_ids))) {
    LOG_WARN("failed to check all tablets not prohibited", K(ret), K(tablet_ids));
  } else if (OB_FAIL(inner_batch_add_tablets_prohibit_flags_(tablet_ids, input_flag))){
    LOG_WARN("failed to add tablets prohibit_flags", K(ret), K(tablet_ids), K(input_flag));
  } else if (ProhibitFlag::TRANSFER == input_flag){
    ++transfer_flag_cnt_;
  } else if (ProhibitFlag::SPLIT == input_flag) {
    ++split_flag_cnt_;
  }
  return ret;
}

int ObProhibitScheduleMediumMap::batch_clear_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ProhibitFlag::TRANSFER != input_flag && ProhibitFlag::SPLIT != input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, batch_clear_flags only support TRANSFER or SPLIT now", K(ret), K(input_flag));
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
    if (OB_SUCC(ret) && ProhibitFlag::SPLIT == input_flag) {
      --split_flag_cnt_;
    }
  }
  return ret;
}

void ObProhibitScheduleMediumMap::destroy()
{
  obsys::ObWLockGuard lock_guard(lock_);
  transfer_flag_cnt_ = 0;
  split_flag_cnt_ = 0;
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

int64_t ObProhibitScheduleMediumMap::get_split_flag_cnt() const
{
  obsys::ObRLockGuard lock_guard(lock_);
  return split_flag_cnt_;
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
    } else if (OB_ENTRY_EXIST != ret) {
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

int ObTenantTabletScheduler::check_ready_for_major_merge(
    const ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const ObMergeType merge_type,
    ObCSReplicaTabletStatus &cs_replica_status)
{
  int ret = OB_SUCCESS;
  if (is_medium_merge(merge_type) || is_major_merge(merge_type)) {
    cs_replica_status = ObCSReplicaTabletStatus::NORMAL;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    bool need_wait_major_convert = false;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is invalid", K(ret), K(ls_id), K(ls_handle));
    } else if (FALSE_IT(ls = ls_handle.get_ls())) {
    } else if (!ls->is_cs_replica()) {
    } else if (OB_FAIL(ObCSReplicaUtil::init_cs_replica_tablet_status(*ls, tablet, cs_replica_status))) {
      LOG_WARN("fail to init cs replica tablet status", K(ret), KPC(ls), K(tablet));
    } else if (OB_UNLIKELY(!is_valid_cs_replica_status(cs_replica_status))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cs replica status is invalid", K(ret), KPC(ls), K(tablet));
    } else if (is_normal_status(cs_replica_status)) {
    } else if (is_need_wait_status(cs_replica_status)) {
      ret = OB_EAGAIN;
      LOG_WARN("tablet is not complete or no major", K(ret), K(cs_replica_status), K(tablet));
    } else if (is_need_major_convert_status(cs_replica_status)) {
      ret = OB_EAGAIN;
      LOG_WARN("need wait major convert in cs replica", K(ret), K(cs_replica_status), K(tablet));
      int tmp_ret = OB_SUCCESS;
      ObDagId co_dag_net_id;
      int schedule_ret = OB_SUCCESS;
      co_dag_net_id.init(GCTX.self_addr());
      if (OB_TMP_FAIL(schedule_convert_co_merge_dag_net(ls_id, tablet, 0 /*retry_times*/, co_dag_net_id, schedule_ret))) {
        LOG_WARN("failed to schedule convert co merge for cs replica", K(tmp_ret), K(ls_id), K(tablet), K(schedule_ret));
      }
    } else if (is_need_cs_storage_schema_status(cs_replica_status)) {
      ret = OB_EAGAIN;
      LOG_WARN("need construct column store stroage schema", K(ret), K(cs_replica_status), K(tablet));
      int tmp_ret = OB_SUCCESS;
      const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
      if (OB_TMP_FAIL(ls->update_tablet_ha_data_status(tablet.get_tablet_id(), data_status))) {
        LOG_WARN("failed to update tablet data status", K(tmp_ret), K(ls_id), K(tablet));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected new cs replica tablet status", K(ret), K(cs_replica_status));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge_dag(
    const ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const ObExecMode exec_mode,
    const ObDagId *dag_net_id /*= nullptr*/,
    const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type /*= ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE*/)
{
  int ret = OB_SUCCESS;
  ObCSReplicaTabletStatus cs_replica_status = ObCSReplicaTabletStatus::NORMAL;
  if (OB_FAIL(check_ready_for_major_merge(ls_id, tablet, merge_type, cs_replica_status))) {
    LOG_WARN("failed to check ready for major merge", K(ret), K(ls_id), K(tablet), K(merge_type));
  } else if (is_major_merge_type(merge_type)
             && (ObCSReplicaTabletStatus::NORMAL_CS_REPLICA == cs_replica_status
                 || ObCOMajorMergePolicy::is_valid_major_merge_type(co_major_merge_type)
                 || is_convert_co_major_merge(merge_type))) {
    ObCOMergeDagParam param;
    if (OB_FAIL(ObDagParamFunc::fill_param(ls_id, tablet, merge_type, merge_snapshot_version, exec_mode, dag_net_id, param))) {
      LOG_WARN("failed to fill param", KR(ret));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_co_merge_dag_net(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(ret));
      }
    }
    FLOG_INFO("schedule co merge dag", K(ret), K(param), K(tablet.is_row_store()), K(merge_type), K(co_major_merge_type), K(cs_replica_status));
  } else {
    ObTabletMergeDagParam param;
    if (OB_FAIL(ObDagParamFunc::fill_param(
      ls_id, tablet, merge_type, merge_snapshot_version, exec_mode, param))) {
      LOG_WARN("failed to fill param", KR(ret));
    } else if (OB_FAIL(ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(ret));
      }
    }
    FLOG_INFO("schedule merge dag", K(ret), K(param), K(tablet.is_row_store()), K(merge_type), K(co_major_merge_type), K(cs_replica_status));
  }
  return ret;
}

int ObTenantTabletScheduler::get_co_merge_type_for_compaction(
    const int64_t merge_version,
    const storage::ObTablet &tablet,
    ObCOMajorMergePolicy::ObCOMajorMergeType &co_major_merge_type)
{
  int ret = OB_SUCCESS;
  co_major_merge_type = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  ObArenaAllocator temp_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
  ObMediumCompactionInfo *medium_info = nullptr;
  if (OB_UNLIKELY(tablet.get_multi_version_start() > merge_version)) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_ERROR("multi version data is discarded, should not execute compaction now", K(ret), K(tablet), K(merge_version));
  } else if (OB_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(merge_version,
                                                                                  tablet,
                                                                                  temp_allocator,
                                                                                  medium_info))) {
    LOG_WARN("fail to get medium info with merge version", K(ret), K(merge_version), K(tablet));
  } else {
    co_major_merge_type = static_cast<ObCOMajorMergePolicy::ObCOMajorMergeType>(medium_info->co_major_merge_type_);
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_convert_co_merge_dag_net(
      const ObLSID &ls_id,
      const ObTablet &tablet,
      const int64_t retry_times,
      const ObDagId& curr_dag_net_id,
      int &schedule_ret)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  schedule_ret = OB_SUCCESS;
  // do not reply on co_major_merge_type in cs replica
  ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  if (OB_TMP_FAIL(compaction::ObTenantTabletScheduler::schedule_merge_dag(ls_id,
                                                                          tablet,
                                                                          compaction::ObMergeType::CONVERT_CO_MAJOR_MERGE,
                                                                          tablet.get_last_major_snapshot_version(),
                                                                          EXEC_MODE_LOCAL,
                                                                          &curr_dag_net_id,
                                                                          co_major_merge_type))) {
    if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to schedule co merge dag net for cs replica", K(ret), K(ls_id), "tablet_id", tablet.get_tablet_id());
    } else {
      schedule_ret = tmp_ret;
    }
  } else {
    LOG_INFO("[CS-Replica] schedule COMergeDagNet to convert row store to column store", K(retry_times), K(ls_id), "tablet_id", tablet.get_tablet_id(), K(curr_dag_net_id));
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_meta_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    bool &has_created_dag)
{
  int ret = OB_SUCCESS;
  has_created_dag = false;

  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else if (ls_handle.get_ls()->is_cs_replica()) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("meta merge is not supported in cs replica now", K(ret), K(ls_handle), K(tablet_handle));
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const int64_t last_major_snapshot_version = tablet->get_last_major_snapshot_version();
    int64_t max_sync_medium_scn = 0;
    ObArenaAllocator allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    ObGetMergeTablesParam param;
    param.merge_type_ = META_MAJOR_MERGE;
    ObGetMergeTablesResult result;

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
      } else if (tablet->is_row_store()) {
        ObTabletMergeDagParam dag_param;
        if (OB_FAIL(ObDagParamFunc::fill_param(
          ls_id, *tablet, META_MAJOR_MERGE, result.merge_version_, EXEC_MODE_LOCAL, dag_param))) {
          LOG_WARN("failed to fill param", KR(ret));
        } else if (OB_FAIL(schedule_merge_execute_dag<ObTabletMergeExecuteDag>(
                dag_param, ls_handle, tablet_handle, result))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule tablet meta merge dag", K(ret), K(dag_param));
          }
        } else {
          has_created_dag = true;
        }
      } else if (GCTX.is_shared_storage_mode()) {
        ObCOMergeDagParam dag_param;
        if (OB_FAIL(ObDagParamFunc::fill_param(
          ls_id, *tablet, META_MAJOR_MERGE, result.merge_version_, EXEC_MODE_LOCAL, nullptr/*ObDagId*/, dag_param))) {
          LOG_WARN("failed to fill param", KR(ret));
        } else if (OB_FAIL(ObScheduleDagFunc::schedule_tablet_co_merge_dag_net(dag_param))) {
          if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
            LOG_WARN("failed to schedule tablet merge dag", K(ret), K(dag_param));
          }
        } else {
          has_created_dag = true;
        }
      }

      if (OB_SUCC(ret) && has_created_dag) {
        MTL(ObTenantTabletStatMgr *)->clear_tablet_stat(ls_id, tablet_id);
        LOG_INFO("success to schedule meta merge", K(ret), K(tablet_id), "is_row_store", tablet->is_row_store());
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
      param.need_swap_tablet_flag_ = ObBasicTabletMergeCtx::need_swap_tablet(*protected_handle, row_count, macro_count, 0/*cg_count*/);
      param.merge_version_ = result.handle_.get_table(result.handle_.get_count() - 1)->get_end_scn().get_val_for_tx();
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
    result.transfer_seq_ = tablet_handle.get_obj()->get_transfer_seq();
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
      for (int64_t k = 0; OB_SUCC(ret) && k < parallel_results.count(); ++k) {
        if (OB_UNLIKELY(parallel_results.at(k).handle_.get_count() <= 1)) {
          LOG_WARN("invalid parallel result", K(ret), K(k), K(parallel_results));
        } else if (OB_FAIL(fill_minor_compaction_param(tablet_handle, parallel_results.at(k), total_sstable_cnt, parallel_dag_cnt, create_time, dag_param))) {
          LOG_WARN("failed to fill compaction param for ranking dags later", K(ret), K(k), K(parallel_results.at(k)));
        } else if (OB_FAIL(schedule_merge_execute_dag<T>(dag_param, ls_handle, tablet_handle, parallel_results.at(k)))) {
          LOG_WARN("failed to schedule minor execute dag", K(ret), K(k), K(parallel_results.at(k)));
        } else {
          LOG_INFO("success to schedule tablet minor merge", K(ret), K(ls_id), K(tablet_id),
            "table_cnt", parallel_results.at(k).handle_.get_count(),
            "merge_scn_range", parallel_results.at(k).scn_range_, K(merge_type));
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
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_handle));
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle));
    }
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
          ls_id,
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

// for minor dag, only hold table key array, should not hold tablet(memtable)
template <class T>
int ObTenantTabletScheduler::schedule_merge_execute_dag(
    const ObTabletMergeDagParam &param,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const bool emergency = tablet_handle.get_obj()->get_tablet_meta().tablet_id_.is_ls_inner_tablet();
  T *merge_exe_dag = nullptr;

  if (result.handle_.get_count() > 1 &&
      !ObTenantTabletScheduler::check_tx_table_ready(*ls_handle.get_ls(), result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...", KR(ret),
             "merge_scn", result.scn_range_.end_scn_);
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->alloc_dag(merge_exe_dag))) {
    LOG_WARN("failed to alloc dag", K(ret), K(param));
  } else if (OB_FAIL(merge_exe_dag->prepare_init(param,
                                                 tablet_handle.get_obj()->get_tablet_meta().compat_mode_,
                                                 result,
                                                 ls_handle))) {
    LOG_WARN("failed to init dag", K(ret), K(result));
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(merge_exe_dag, emergency))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(merge_exe_dag));
    }
  } else {
    LOG_INFO("success to scheudle merge execute dag", K(ret), KP(merge_exe_dag), K(emergency));
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
  ObLSStatusCache::LSState state = ObLSStatusCache::STATE_MAX;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  (void) ObLSStatusCache::check_ls_state(ls, state);
  if (ObLSStatusCache::CAN_MERGE != state) {
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

    // ATTENTION! : do not use sync freeze because cyclic dependencies exist
    const bool is_sync = false;
    start_time_us = ObClockGenerator::getClock();
    if (need_fast_freeze_tablets.empty()) {
      // empty array. do not need freeze
    } else if (OB_TMP_FAIL(ls.tablet_freeze(checkpoint::INVALID_TRACE_ID,
                                            need_fast_freeze_tablets,
                                            is_sync,
                                            0, /*timeout, useless for async one*/
                                            false, /*need_rewrite_meta*/
                                            ObFreezeSourceFlag::FAST_FREEZE))) {
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
  if (tablet.is_empty_shell()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("can't schedule minor for empty shell tablet", K(ret), K(ls_id), K(tablet_id));
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
    if (OB_TMP_FAIL(schedule_ddl_tablet_merge(ls_handle, tablet_handle))) {
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

int ObTenantTabletScheduler::schedule_ddl_tablet_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  const ObTabletID tablet_id = tablet_handle.is_valid() ? tablet_handle.get_obj()->get_tablet_meta().tablet_id_ : ObTabletID();
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_TRACE("kv mgr not exist", K(ret), K(ls_id), K(tablet_id));
      ret = OB_SUCCESS; /* for empty table, ddl kv may not exist*/
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ls_id), K(tablet_id));
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ObTabletDDLUtil::schedule_ddl_minor_merge_on_demand(false/*need_freeze*/, ls_id, ddl_kv_mgr_handle))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
      } else {
        LOG_TRACE("schedule ddl minor merge failed", K(ret), K(ls_id), K(tablet_id));
      }
    }
#endif
  } else {
    if (OB_FAIL(schedule_tablet_ddl_major_merge(ls_handle, tablet_handle))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
      } else {
        LOG_TRACE("schedule ddl major merge failed", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  LOG_TRACE("schedule ddl tablet merge", K(ret), K(ls_id), K(tablet_id));
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets_medium()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (!tenant_status_.is_inited() && OB_FAIL(tenant_status_.init_or_refresh())) {
    if (OB_NEED_WAIT != ret) {
      LOG_WARN("failed to init tenant_status", KR(ret), K_(tenant_status));
    }
  } else {
    const int64_t merge_version = get_frozen_version();
    if (merge_version > merged_version_) {
      try_finish_merge_progress(merge_version);
      mview_validation_.refresh(merge_version);
    }
    if (OB_FAIL(medium_loop_.init(get_schedule_batch_size()))) {
      LOG_WARN("failed to init medium loop", K(ret));
    } else {
      LOG_INFO("start schedule all tablet merge", K(merge_version));
      if (OB_FAIL(medium_loop_.loop())) {
        LOG_WARN("failed to medium loop", K(ret));
      }
    }
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL) &&
        (prohibit_medium_map_.get_transfer_flag_cnt() > 0 || prohibit_medium_map_.get_split_flag_cnt() > 0)) {
      LOG_INFO("tenant is blocking schedule medium", KR(ret), K_(prohibit_medium_map));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::user_request_schedule_medium_merge(
  const ObLSID &ls_id,
  const common::ObTabletID &tablet_id,
  const bool is_rebuild_column_group)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  LOG_INFO("user_request_schedule_medium_merge", K(ret), K(ls_id), K(tablet_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(tablet_id.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to schedule medium for ls inner tablet", K(ret), K(tablet_id));
  } else if (!could_major_merge_start()) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major compaction is suspended", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else {
    const int64_t merge_version = get_frozen_version();
    const ObAdaptiveMergePolicy::AdaptiveMergeReason reason = is_rebuild_column_group ? ObAdaptiveMergePolicy::REBUILD_COLUMN_GROUP : ObAdaptiveMergePolicy::USER_REQUEST;
    ObScheduleTabletFunc func(merge_version, reason);
    if (OB_FAIL(func.switch_ls(ls_handle))) {
      if (OB_STATE_NOT_MATCH != ret) {
        LOG_WARN("failed to switch ls", KR(ret), K(ls_id), K(func));
      } else {
        LOG_WARN("not support schedule medium for ls", K(ret), K(ls_id), K(tablet_id), K(func));
      }
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 tablet_id, tablet_handle, 0 /*timeout_us*/))) {
      LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(func.request_schedule_new_round(tablet_handle, true/*user_request*/))) {
      LOG_WARN("failed to request schedule new round", K(ret), K(ls_id), K(tablet_id));
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

} // namespace storage
} // namespace oceanbase
