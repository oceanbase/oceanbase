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
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_tenant_freeze_info_mgr.h"
#include "ob_tenant_compaction_progress.h"
#include "ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_tablet_merge_checker.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "share/scn.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/ob_gc_upper_trans_helper.h"
#include "share/schema/ob_tenant_schema_service.h"

namespace oceanbase
{
using namespace compaction;
using namespace common;
using namespace share;

namespace storage
{
ERRSIM_POINT_DEF(EN_COMPACTION_DISABLE_META_MERGE_AFTER_MINI);
ObFastFreezeChecker::ObFastFreezeChecker()
  : enable_fast_freeze_(false)
{
}

ObFastFreezeChecker::~ObFastFreezeChecker()
{
  reset();
}

void ObFastFreezeChecker::reset()
{
  enable_fast_freeze_ = false;
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
  memtable::ObMemtable *memtable = nullptr;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const bool need_check_fast_freeze = enable_fast_freeze_ && !tablet_id.is_inner_tablet() && !tablet_id.is_ls_inner_tablet();
  ObTableModeFlag queuing_mode = ObTableModeFlag::TABLE_MODE_NORMAL;
  int64_t memtable_alive_threshold = 0;
  if (need_check_fast_freeze && OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_queuing_mode(ls_id, tablet_id, queuing_mode))) {
    LOG_WARN_RET(tmp_ret, "[FastFreeze] failed to get table queuing mode, treat it as normal table", K(ls_id), K(tablet_id));
  }
  if (!need_check_fast_freeze) {
    // do nothing
  } else if (OB_FAIL(tablet.get_active_memtable(table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FastFreeze] failed to get active memtable", K(ret));
    }
  } else if (OB_FAIL(table_handle.get_data_memtable(memtable))) {
    LOG_WARN("[FastFreeze] failed to get memtalbe", K(ret), K(table_handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FastFreeze] get unexpected null memtable", K(ret), KPC(memtable));
  } else if (!memtable->is_active_memtable()) {
    // do nothing
  } else if (FALSE_IT(memtable_alive_threshold = FAST_FREEZE_INTERVAL_US * ObTableQueuingModeCfg::get_basic_config(queuing_mode).queuing_factor_)) {
  } else if (ObTimeUtility::current_time() < memtable->get_timestamp() + memtable_alive_threshold) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("[FastFreeze] memtable is just created, no need to check", K(memtable_alive_threshold), K(ls_id), K(tablet_id), KPC(memtable));
    }
  } else {
    check_hotspot_need_fast_freeze(*memtable, need_fast_freeze);
    if (need_fast_freeze) {
      FLOG_INFO("[FastFreeze] tablet detects hotspot row, need fast freeze", K(ls_id), K(tablet_id));
    } else {
      // only queuing table need tombstone fast freeze
      check_tombstone_need_fast_freeze(tablet, *memtable, queuing_mode, need_fast_freeze);
      if (need_fast_freeze) {
        FLOG_INFO("[FastFreeze] tablet detects tombstone, need fast freeze", K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

void ObFastFreezeChecker::check_hotspot_need_fast_freeze(
    const memtable::ObMemtable &memtable,
    bool &need_fast_freeze)
{
  need_fast_freeze = false;
  if (memtable.is_active_memtable()) {
    need_fast_freeze = memtable.has_hotspot_row();
  }
}

void ObFastFreezeChecker::check_tombstone_need_fast_freeze(
    const ObTablet &tablet,
    const memtable::ObMemtable &memtable,
    const ObTableModeFlag &queuing_mode,
    bool &need_fast_freeze)
{
  const ObTableQueuingModeCfg &queuing_cfg = ObTableQueuingModeCfg::get_basic_config(queuing_mode);
  need_fast_freeze = false;
  if (memtable.is_active_memtable()) {
    const memtable::ObMtStat &mt_stat = memtable.get_mt_stat(); // dirty read
    int64_t adaptive_threshold = TOMBSTONE_DEFAULT_ROW_COUNT * queuing_cfg.queuing_factor_;
    need_fast_freeze = (mt_stat.update_row_count_ + mt_stat.delete_row_count_) >= adaptive_threshold
                     || mt_stat.delete_row_count_ > queuing_cfg.total_delete_row_cnt_;
    if (!need_fast_freeze) {
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


void ObTenantTabletScheduler::MergeLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_minor())) {
      LOG_WARN("Fail to merge all partition", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("MergeLoopTask", K(cost_ts));
  }
}

void ObTenantTabletScheduler::MediumLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_UNLIKELY(!ObServerCheckpointSlogHandler::get_instance().is_started())) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_medium())) {
      LOG_WARN("Fail to merge all partition", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("MediumLoopTask", K(cost_ts));
  }
}

void ObTenantTabletScheduler::SSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    // use tenant config to loop minor && medium task
    MTL(ObTenantTabletScheduler *)->reload_tenant_config();

    int64_t cost_ts = ObTimeUtility::fast_current_time();
    ObCurTraceId::init(GCONF.self_addr_);
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->update_upper_trans_version_and_gc_sstable())) {
      LOG_WARN("Fail to update upper_trans_version and gc sstable", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("SSTableGCTask", K(cost_ts));
  }
}

void ObTenantTabletScheduler::InfoPoolResizeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->set_max())) {
    LOG_WARN("Fail to resize info pool", K(ret));
  }
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->gc_info())) {
    LOG_WARN("Fail to gc info", K(ret));
  }
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->refresh_tenant_status())) {
    LOG_WARN("Fail to refresh tenant status", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("InfoPoolResizeTask", K(cost_ts));
}

constexpr ObMergeType ObTenantTabletScheduler::MERGE_TYPES[];

ObTenantTabletScheduler::ObTenantTabletScheduler()
 : is_inited_(false),
   major_merge_status_(false),
   is_stop_(false),
   is_restore_(false),
   merge_loop_tg_id_(0),
   medium_loop_tg_id_(0),
   sstable_gc_tg_id_(0),
   info_pool_resize_tg_id_(0),
   schedule_interval_(0),
   bf_queue_(),
   frozen_version_lock_(),
   frozen_version_(INIT_COMPACTION_SCN),
   merged_version_(INIT_COMPACTION_SCN),
   inner_table_merged_scn_(INIT_COMPACTION_SCN),
   schedule_stats_(),
   merge_loop_task_(),
   medium_loop_task_(),
   sstable_gc_task_(),
   info_pool_resize_task_(),
   fast_freeze_checker_(),
   enable_adaptive_compaction_(false),
   minor_ls_tablet_iter_(false/*is_major*/),
   medium_ls_tablet_iter_(true/*is_major*/),
   error_tablet_cnt_(0),
   prohibit_medium_map_(),
   ls_locality_cache_()
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
  TG_DESTROY(merge_loop_tg_id_);
  TG_DESTROY(medium_loop_tg_id_);
  TG_DESTROY(sstable_gc_tg_id_);
  TG_DESTROY(info_pool_resize_tg_id_);

  is_inited_ = false;
  bf_queue_.destroy();
  frozen_version_ = 0;
  merged_version_ = 0;
  inner_table_merged_scn_ = 0;
  schedule_stats_.reset();
  merge_loop_tg_id_ = 0;
  medium_loop_tg_id_ = 0;
  sstable_gc_tg_id_ = 0;
  info_pool_resize_tg_id_ = 0;
  schedule_interval_ = 0;
  minor_ls_tablet_iter_.reset();
  medium_ls_tablet_iter_.reset();
  ls_locality_cache_.reset();
  prohibit_medium_map_.destroy();
  LOG_INFO("The ObTenantTabletScheduler destroy");
}

int ObTenantTabletScheduler::init()
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  int64_t schedule_batch_size = DEFAULT_COMPACTION_SCHEDULE_BATCH_SIZE;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
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
  } else if (OB_FAIL(ls_locality_cache_.init(MTL_ID(), GCTX.sql_proxy_))) {
    LOG_WARN("failed to init ls locality cache", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(prohibit_medium_map_.init())) {
    LOG_WARN("Fail to create prohibit medium ls id map", K(ret));
  } else {
    schedule_interval_ = schedule_interval;
    schedule_tablet_batch_size_ = schedule_batch_size;
    is_inited_ = true;
  }
  if (!is_inited_) {
    reset();
  }
  return ret;
}

int ObTenantTabletScheduler::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MergeLoop, merge_loop_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(merge_loop_tg_id_))) {
    LOG_WARN("failed to start minor merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule minor merge scan task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MediumLoop, medium_loop_tg_id_))) {
    LOG_WARN("failed to create medium loop thread", K(ret));
  } else if (OB_FAIL(TG_START(medium_loop_tg_id_))) {
    LOG_WARN("failed to start medium merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(medium_loop_tg_id_, medium_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule medium merge scan task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTableGC, sstable_gc_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(sstable_gc_tg_id_))) {
    LOG_WARN("failed to start sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(sstable_gc_tg_id_, sstable_gc_task_, SSTABLE_GC_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule sstable gc task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::InfoPoolResize, info_pool_resize_tg_id_))) {
    LOG_WARN("failed to create info pool resize thread", K(ret));
  } else if (OB_FAIL(TG_START(info_pool_resize_tg_id_))) {
    LOG_WARN("failed to start info pool resize thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(info_pool_resize_tg_id_, info_pool_resize_task_, INFO_POOL_RESIZE_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule info pool resize task", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::reload_tenant_config()
{
  int ret = OB_SUCCESS;
  int64_t merge_schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  int64_t schedule_batch_size = DEFAULT_COMPACTION_SCHEDULE_BATCH_SIZE;
  bool tenant_config_valid = false;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      tenant_config_valid = true;
      merge_schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
      schedule_batch_size = tenant_config->compaction_schedule_tablet_batch_cnt;
    }
  } // end of ObTenantConfigGuard
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else if (schedule_interval_ != merge_schedule_interval) {
    if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, medium_loop_tg_id_, medium_loop_task_))) {
      LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
    } else if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, merge_loop_tg_id_, merge_loop_task_))) {
      LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
    } else {
      schedule_interval_ = merge_schedule_interval;
      LOG_INFO("succeeded to reload new merge schedule interval", K(merge_schedule_interval), K(tenant_config_valid));
    }
  }
  if (OB_SUCC(ret) && schedule_tablet_batch_size_ != schedule_batch_size) {
    schedule_tablet_batch_size_ = schedule_batch_size;
    LOG_INFO("succeeded to reload new merge schedule tablet batch cnt", K(schedule_tablet_batch_size_), K(tenant_config_valid));
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
  TG_STOP(merge_loop_tg_id_);
  TG_STOP(medium_loop_tg_id_);
  TG_STOP(sstable_gc_tg_id_);
  TG_STOP(info_pool_resize_tg_id_);
  stop_major_merge();
}

void ObTenantTabletScheduler::wait()
{
  TG_WAIT(merge_loop_tg_id_);
  TG_WAIT(medium_loop_tg_id_);
  TG_WAIT(sstable_gc_tg_id_);
  TG_WAIT(info_pool_resize_tg_id_);
}

int ObTenantTabletScheduler::try_update_upper_trans_version_and_gc_sstable(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &ls_id = ls.get_ls_id();
  // only need NORMAL tablet here
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_ALL_COMMITED);

  if (OB_FAIL(ls.build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
  }

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  common::ObTabletID tablet_id;
  bool ls_is_migration = false;
  int64_t rebuild_seq = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get tablet", K(ret), K(tenant_id), K(ls_id), K(tablet_handle));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
    } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
    } else if (tablet_id.is_special_merge_tablet()) {
    } else if (!tablet->get_tablet_meta().ha_status_.is_none()) {
    } else if (OB_FAIL(ls.check_ls_migration_status(ls_is_migration, rebuild_seq))) {
      LOG_WARN("failed to check ls migration status", K(ret), K(tenant_id), K(ls_id));
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
            LOG_WARN("failed to update table store", K(tmp_ret), K(param), K(tenant_id), K(ls_id), K(tablet_id));
          } else {
            FLOG_INFO("success to remove old table in table store", K(tmp_ret), K(tenant_id), K(ls_id),
                K(tablet_id), K(multi_version_start), KPC(tablet));
          }
        }
        ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::update_upper_trans_version_and_gc_sstable()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), KP(ls_iter_guard.get_ptr()));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else if (ls->is_stopped()) {
    } else if (OB_TMP_FAIL(try_update_upper_trans_version_and_gc_sstable(*ls))) {
      LOG_WARN("failed to update upper trans version", K(tmp_ret), "ls_id", ls->get_ls_id());
    }
  }
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
  } else if (OB_FAIL(minor_ls_tablet_iter_.build_iter(schedule_tablet_batch_size_))) {
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
  }
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
      } else {
        ATOMIC_SET(&is_restore_, false);
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
    {
      obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version_ = broadcast_version;
    }

    LOG_INFO("schedule merge major version", K(broadcast_version));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->add_progress(broadcast_version))) {
      LOG_WARN("failed to add progress", K(tmp_ret), K(broadcast_version));
    }
    clear_error_tablet_cnt();

    schedule_stats_.start_merge(); // set all statistics
    ADD_COMPACTION_EVENT(
        MTL_ID(),
        MAJOR_MERGE,
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
  if (ATOMIC_BCAS(&major_merge_status_, true, false)) {
    LOG_INFO("major merge has been paused!");
  }
}

void ObTenantTabletScheduler::resume_major_merge()
{
  if (ATOMIC_BCAS(&major_merge_status_, false, true)) {
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
  STATIC_ASSERT(static_cast<int64_t>(FLAG_MAX) == ARRAYSIZEOF(ProhibitFlagStr), "flag str len is mismatch");
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
      LOG_TRACE("flag in conflict", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
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
  if (OB_UNLIKELY(TRANSFER != input_flag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, batch_add_flags only support TRANSFER now", K(ret), K(input_flag));
  } else if (OB_FAIL(inner_batch_check_tablets_not_prohibited_(tablet_ids))) {
    LOG_WARN("failed to check all tablets not prohibited", K(ret), K(tablet_ids));
  } else if (OB_FAIL(inner_batch_add_tablets_prohibit_flags_(tablet_ids, input_flag))){
    LOG_WARN("failed to add tablets prohibit_flags", K(ret), K(tablet_ids), K(input_flag));
  } else if (TRANSFER == input_flag){
    ++transfer_flag_cnt_;
  }
  return ret;
}

int ObProhibitScheduleMediumMap::batch_clear_flags(const ObIArray<ObTabletID> &tablet_ids, const ProhibitFlag &input_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(TRANSFER != input_flag)) {
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
    if (OB_SUCC(ret) && TRANSFER == input_flag) {
      --transfer_flag_cnt_;
    }
  }
  return ret;
}



void ObProhibitScheduleMediumMap::destroy()
{
  obsys::ObWLockGuard lock_guard(lock_);
  transfer_flag_cnt_ = 0;
  tablet_id_map_.destroy();
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
        J_KV(K(idx), "tablet_id", tablet_id.id(), "flag", ProhibitFlagStr[it->second]);
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
    LOG_ERROR("flag in conflict", K(ret), K(tablet_id), K(tmp_flag), K(input_flag));
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
  if (OB_UNLIKELY(ObProhibitScheduleMediumMap::MEDIUM == input_flag)) {
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
  if (OB_FAIL(prohibit_medium_map_.add_flag(tablet_id, ObProhibitScheduleMediumMap::MEDIUM))) {
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

int ObTenantTabletScheduler::check_ls_state(
  ObLS &ls,
  bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  if (ls.is_deleted()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("ls is deleted", K(ret), K(ls));
    }
  } else if (ls.is_offline()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("ls is offline", K(ret), K(ls));
    }
  } else if (ObReplicaTypeCheck::is_log_replica(ls.get_replica_type())) {
    // skip logonly replica
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("ls is logonly", K(ret), K(ls));
    }
  } else {
    need_merge = true;
  }
  return ret;
}

int ObTenantTabletScheduler::check_ls_state_in_major(
  ObLS &ls,
  bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", KR(ret));
  } else if (!need_merge) {
    // do nothing
  } else if (OB_FAIL(ls.get_ls_meta().get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ls));
  } else if (OB_UNLIKELY(!restore_status.is_restore_none())) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("ls is in restore status, should not loop tablet to schedule", K(ret), K(ls));
    }
  } else {
    need_merge = true;
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge_dag(
    const ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const bool is_tenant_major_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.merge_type_ = merge_type;
  param.merge_version_ = merge_snapshot_version;
  param.is_tenant_major_merge_ = is_tenant_major_merge;
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to schedule tablet merge dag", K(ret));
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
    if (OB_FAIL(check_ls_state(ls, need_merge))) {
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
        bool unused_tablet_merge_finish = false;
        bool medium_clog_submitted = false;
        // ATTENTION!!! load weak ts before get tablet
        const share::SCN &weak_read_ts = ls.get_ls_wrs_handler()->get_ls_weak_read_ts();
        const ObLSID &ls_id = ls.get_ls_id();
        ObTabletHandle tablet_handle; // catch up latest ls status
        if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle))) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        } else if (OB_FAIL(scheduler->try_schedule_tablet_medium(
                                        ls_handle, ls_id,
                                        tablet_handle,
                                        weak_read_ts,
                                        medium_param,
                                        false /*scheduler_called*/,
                                        unused_tablet_merge_finish,
                                        medium_clog_submitted,
                                        succ_create_dag))) {
            LOG_WARN("failed to try schedule tablet medium", K(ret), K(ls_handle), K(ls_id), K(tablet_handle),
                K(weak_read_ts), K(medium_param), K(unused_tablet_merge_finish));
        } else if (medium_clog_submitted && OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->clear_tablet_stat(ls_id, tablet_id))) {
          LOG_WARN("failed to clear tablet stats", K(tmp_ret), K(ls_id), K(tablet_id));
        }
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
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;

  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const int64_t last_major_snapshot_version = wrapper.get_member()->get_major_sstables().get_last_snapshot_version(); // is no major, return 0
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
      } else {
        ObTabletMergeDagParam dag_param(META_MAJOR_MERGE, ls_id, tablet_id);
        dag_param.merge_version_ = result.merge_version_;
        if (OB_FAIL(schedule_merge_execute_dag<ObTabletMergeExecuteDag>(dag_param, ls_handle, tablet_handle, result))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule tablet meta merge dag", K(ret), K(dag_param));
          }
        } else {
          MTL(ObTenantTabletStatMgr *)->clear_tablet_stat(ls_id, tablet_id);
          has_created_dag = true;
          LOG_INFO("success to schedule meta merge", K(ret), K(tablet_id));
        }
      }
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
  ObGetMergeTablesParam param;
  ObGetMergeTablesResult result;
  for (int i = 0; OB_SUCC(ret) && i < schedule_type_cnt; ++i) {
    param.merge_type_ = MERGE_TYPES[i];
    if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[MERGE_TYPES[i]](
            param,
            *ls_handle.get_ls(),
            *tablet_handle.get_obj(),
            result))) {
      if (OB_NO_NEED_MERGE == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("tablet no need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet_id), K(tablet_handle));
      } else {
        LOG_WARN("failed to check need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet_handle));
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
      if (OB_FAIL(minor_range_mgr.get_merge_ranges(ls_id, tablet_id))) {
        LOG_WARN("failed to get merge range", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(ObPartitionMergePolicy::generate_parallel_minor_interval(param.merge_type_, minor_compact_trigger, result, minor_range_mgr, parallel_results))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to generate parallel minor dag", K(ret), K(result));
        } else {
          ret = OB_SUCCESS;
          LOG_DEBUG("tablet no need merge", K(ret), "merge_type", MERGE_TYPES[i], K(ls_id), K(tablet_id), K(result));
        }
      } else if (parallel_results.empty()) {
        LOG_DEBUG("parallel results is empty, cannot schedule parallel minor merge", K(ls_id), K(tablet_id),
            K(result), K(minor_range_mgr.exe_range_array_));
      } else {
        ObTabletMergeDagParam dag_param(MERGE_TYPES[i], ls_id, tablet_id);
        for (int k = 0; OB_SUCC(ret) && k < parallel_results.count(); ++k) {
          if (OB_UNLIKELY(parallel_results.at(k).handle_.get_count() <= 1)) {
            LOG_WARN("invalid parallel result", K(ret), K(k), K(parallel_results));
          } else if (OB_FAIL(schedule_merge_execute_dag<T>(dag_param, ls_handle, tablet_handle, parallel_results.at(k)))) {
            LOG_WARN("failed to schedule minor execute dag", K(ret), K(k), K(parallel_results.at(k)));
          } else {
            LOG_INFO("success to schedule tablet minor merge", K(ret), K(ls_id), K(tablet_id),
              "table_cnt", parallel_results.at(k).handle_.get_count(),
              "merge_scn_range", parallel_results.at(k).scn_range_, "merge_type", MERGE_TYPES[i]);
          }
        } // end of for
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_ddl_major_merge(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle kv_mgr_handle;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(tablet_handle));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (kv_mgr_handle.is_valid()) {
    ObDDLTableMergeDagParam param;
    if (OB_FAIL(kv_mgr_handle.get_obj()->get_ddl_major_merge_param(*tablet_handle.get_obj(), param))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to get ddl major merge param", K(ret));
      }
    } else if (!param.is_commit_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl merge param not a committed one", K(ret), K(param));
    } else if (OB_FAIL(kv_mgr_handle.get_obj()->freeze_ddl_kv(*tablet_handle.get_obj(), param.rec_scn_))) {
      LOG_WARN("failed to freeze ddl kv", K(ret));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    } else {
      LOG_INFO("schedule ddl merge task for major sstable success", K(param));
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
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  T *merge_exe_dag = nullptr;
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
  } else if (OB_FAIL(merge_exe_dag->direct_init_ctx(
          param,
          tablet_handle.get_obj()->get_tablet_meta().compat_mode_,
          result,
          ls_handle))) {
    LOG_WARN("failed to init dag", K(ret), K(result));
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(merge_exe_dag, emergency))) {
    LOG_WARN("failed to add dag", K(ret), KPC(merge_exe_dag));
  } else {
    LOG_INFO("success to scheudle tablet minor execute dag", K(ret), KP(merge_exe_dag), K(emergency));
  }
  if (OB_FAIL(ret) && nullptr != merge_exe_dag) {
    MTL(share::ObTenantDagScheduler *)->free_dag(*merge_exe_dag, nullptr/*parent_dag*/);
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
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    int tmp_ret = OB_SUCCESS;
    bool schedule_minor_flag = true;
    while (OB_SUCC(ret) && schedule_minor_flag) { // loop all tablet in ls
      bool tablet_merge_finish = false;
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
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
      } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ObMergeType::MINOR_MERGE, *tablet))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to check need merge", K(ret));
        }
      } else if (tablet_id.is_special_merge_tablet()) {
        // schedule minor merge for special tablet
        if (tablet_id.is_mini_and_minor_merge_tablet()
            && OB_TMP_FAIL(schedule_tablet_minor_merge<ObTxTableMinorExecuteDag>(
                ls_handle,
                tablet_handle))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_minor_flag = false;
          } else if (OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }
      } else { // data tablet
        if (OB_TMP_FAIL(schedule_tablet_minor_merge<ObTabletMergeExecuteDag>(ls_handle, tablet_handle))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_minor_flag = false;
          } else if (OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }

        if (OB_TMP_FAIL(schedule_tablet_ddl_major_merge(tablet_handle))) {
          if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet ddl merge", K(tmp_ret), K(ls_id), K(tablet_handle));
          }
        }

        if (MTL(ObTenantTabletStatMgr *)->is_extreme_tablet(ls_id, tablet_id)) {
          bool unused_create_dag = false; // unused
          if (OB_TMP_FAIL(ObTenantTabletScheduler::try_schedule_adaptive_merge(ls_handle, tablet_handle,
                ObAdaptiveMergePolicy::SCHEDULE_META, 0 /*update_cnt*/, 0 /*delete_cnt*/, unused_create_dag))) {
            LOG_WARN("failed to schedule tablet meta merge", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }

        if (OB_SUCC(ret)) {
          need_fast_freeze = false;
          if (!fast_freeze_checker_.need_check()) {
          } else if (OB_TMP_FAIL(
                         fast_freeze_checker_.check_need_fast_freeze(*tablet_handle.get_obj(), need_fast_freeze))) {
            LOG_WARN("failed to check need fast freeze", K(tmp_ret), K(tablet_handle));
          } else if (need_fast_freeze) {
            const bool is_sync = false;
            if (OB_TMP_FAIL(ls.tablet_freeze(tablet_id, is_sync))) {
              LOG_WARN("failt to freeze tablet", K(tmp_ret), K(tablet_id));
            }

            ObTxTableGuard tx_table_guard;
            LOG_INFO("Trigger tx data freeze by fast freeze", K(ls_id), K(tablet_id));
            if (OB_TMP_FAIL(ls.get_tx_table()->get_tx_table_guard(tx_table_guard))) {
              LOG_WARN("get tx table guard failed", KR(tmp_ret), K(tx_table_guard));
            } else {
              (void)tx_table_guard.self_freeze_task();
            }
          }
        }
      }
    }  // end of while
  } // else
  return ret;
}

int ObTenantTabletScheduler::check_tablet_could_schedule_by_status(const ObTablet &tablet, bool &could_schedule_merge)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;// will be removed later
  mds::TwoPhaseCommitState trans_stat;// will be removed later
  share::SCN trans_version;// will be removed later
  could_schedule_merge = true;
  if (OB_FAIL(tablet.get_latest(user_data, writer, trans_stat, trans_version))) {
    LOG_WARN("failed to get tablet status", K(ret), K(tablet), K(user_data));
  } else if (ObTabletStatus::TRANSFER_OUT == user_data.tablet_status_
    || ObTabletStatus::TRANSFER_OUT_DELETED == user_data.tablet_status_) {
    could_schedule_merge = false;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("tablet status is TRANSFER_OUT or TRANSFER_OUT_DELETED, merging is not allowed", K(user_data), K(tablet));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::prepare_ls_medium_merge(
    ObLS &ls,
    ObTenantTabletMediumParam &param,
    bool &all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObRole role = INVALID_ROLE;
  bool locality_bad_case = false;
  param.ls_locality_.set_attr(MTL_ID());
  if (MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
    param.could_major_merge_ = true;
  } else if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
    LOG_INFO("major merge should not schedule", K(ret), K(param));
  }
  // check weak_read_ts
  if (param.merge_version_ >= 0) {
    if (check_weak_read_ts_ready(param.merge_version_, ls)) { // weak read ts ready
      if (OB_FAIL(ObMediumCompactionScheduleFunc::get_palf_role(ls_id, role))) {
        if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
        }
      } else if (is_leader_by_election(role)) {
        param.is_leader_ = true;
        if (OB_FAIL(ls_locality_cache_.get_ls_locality(ls_id, param.ls_locality_))) {
          LOG_WARN("failed to get ls locality", K(ret), K(ls_id));
        } else if (param.ls_locality_.is_valid()) {
          if (!param.ls_locality_.check_exist(GCTX.self_addr())) {
            locality_bad_case = true;
            const int64_t buf_len = ObScheduleSuspectInfoMgr::EXTRA_INFO_LEN;
            char tmp_str[buf_len] = "\0";
            ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, "leader_addr", GCTX.self_addr(), K(param.ls_locality_));
            ADD_SUSPECT_INFO(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX),
              ObSuspectInfoType::SUSPECT_LOCALITY_CHANGE, static_cast<int64_t>(0), "locality", tmp_str);
          } else {
            DEL_SUSPECT_INFO(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX));
          }
        }
      }
    } else {
      all_ls_weak_read_ts_ready = false;
    }
  }
  if (!locality_bad_case) {
    DEL_SUSPECT_INFO(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX));
  }

  param.enable_adaptive_compaction_ = enable_adaptive_compaction_;
  ObTenantSysStat cur_sys_stat;
  if (!enable_adaptive_compaction_) {
    // do nothing
  } else if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_sys_stat(cur_sys_stat))) {
    LOG_WARN("failed to get tenant sys stat", K(tmp_ret), K(cur_sys_stat));
  } else if (cur_sys_stat.is_full_cpu_usage()) {
    param.enable_adaptive_compaction_ = false;
    FLOG_INFO("disable adaptive compaction due to the high load CPU", K(ret), K(cur_sys_stat));
  }

  return ret;
}

int ObTenantTabletScheduler::try_schedule_tablet_medium(
    ObLSHandle &ls_handle,
    const share::ObLSID &ls_id,
    ObTabletHandle &tablet_handle,
    const share::SCN &weak_read_ts,
    ObTenantTabletMediumParam &param,
    const bool scheduler_called,
    bool &tablet_merge_finish,
    bool &medium_clog_submitted,
    bool &succ_create_dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletID tablet_id;
  bool tablet_could_schedule_medium = false;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ObMergeType::MEDIUM_MERGE, *tablet))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
  } else if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (param.is_leader_ && param.could_major_merge_
          && OB_TMP_FAIL(tablet_start_schedule_medium(tablet_id, tablet_could_schedule_medium))) {
    LOG_WARN("failed to set start schedule medium", K(ret), K(tmp_ret), K(ls_id));
  } else if (OB_TMP_FAIL(schedule_tablet_medium(
                          ls_handle,
                          tablet_handle,
                          weak_read_ts,
                          param,
                          tablet_could_schedule_medium,
                          scheduler_called,
                          tablet_merge_finish,
                          medium_clog_submitted,
                          succ_create_dag))) {
    LOG_WARN("fail to schedule tablet medium", K(tmp_ret), K(tablet_id));
  }
  if (tablet_could_schedule_medium
          && OB_TMP_FAIL(clear_prohibit_medium_flag(tablet_id, ObProhibitScheduleMediumMap::MEDIUM))) {
    // clear flags set by tablet_start_schedule_medium
    LOG_WARN("failed to clear prohibit schedule medium flag", K(tmp_ret), K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_medium_merge(
    int64_t &merge_version,
    ObLSHandle &ls_handle,
    bool &all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_merge = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTenantTabletMediumParam medium_param(merge_version);
  if (OB_FAIL(check_ls_state_in_major(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(prepare_ls_medium_merge(ls, medium_param, all_ls_weak_read_ts_ready))) {
    LOG_WARN("failed to prepare ls medium merge", K(ret), K(medium_param), K(all_ls_weak_read_ts_ready));
  } else {
    ObSEArray<ObTabletID, 64> batched_clear_stat_tablets;
    batched_clear_stat_tablets.set_attr(ObMemAttr(MTL_ID(), "BatchClearTblts"));
    ObTabletID tablet_id;
    while (OB_SUCC(ret)) { // loop all tablet in ls
      bool tablet_merge_finish = false;
      bool medium_clog_submitted = false;
      bool succ_create_dag = false;
      // ATTENTION!!! load weak ts before get tablet
      const share::SCN &weak_read_ts = ls.get_ls_wrs_handler()->get_ls_weak_read_ts();
      if (OB_FAIL(medium_ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_FAIL(try_schedule_tablet_medium(
                          ls_handle,
                          ls_id,
                          tablet_handle,
                          weak_read_ts,
                          medium_param,
                          true /*scheduler_called*/,
                          tablet_merge_finish,
                          medium_clog_submitted,
                          succ_create_dag))) {
        LOG_WARN("failed to try schedule tablet medium", K(ret), K(ls_handle), K(ls_id),
            K(tablet_handle), K(weak_read_ts), K(medium_param), K(tablet_merge_finish));
      } else if (medium_clog_submitted) {
        tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
        if (OB_TMP_FAIL(batched_clear_stat_tablets.push_back(tablet_id))) {
          LOG_WARN("failed to add tablet to clear stat", K(tmp_ret), K(ls_id), K(tablet_id));
        }
      }
      medium_ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
    } // end of while
    // most tablets will clear failed since the capacity of ObTenantTabletStatMgr is limited
    if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->batch_clear_tablet_stat(ls_id, batched_clear_stat_tablets))) {
      LOG_WARN("failed to batch clear tablet stats", K(tmp_ret), K(ls_id));
    }
  } // else
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_medium(
  ObLSHandle &ls_handle,
  ObTabletHandle &tablet_handle,
  const share::SCN &weak_read_ts,
  ObTenantTabletMediumParam &param,
  const bool tablet_could_schedule_medium,
  const bool scheduler_called,
  bool &tablet_merge_finish,
  bool &medium_clog_submitted,
  bool &succ_create_dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  tablet_merge_finish = false;
  bool tablet_could_schedule_merge = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  ObTablet &tablet = *tablet_handle.get_obj();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool need_diagnose = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    if (tablet_could_schedule_medium && OB_TMP_FAIL(check_tablet_could_schedule_by_status(tablet, tablet_could_schedule_merge))) {
      LOG_WARN("failed to check tablet counld schedule merge", K(tmp_ret), K(tablet_id));
    }
    ObMediumCompactionScheduleFunc func(ls, tablet_handle, weak_read_ts);
    ObITable *latest_major = table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/);
    if (OB_NOT_NULL(latest_major) && latest_major->get_snapshot_version() >= param.merge_version_) {
      tablet_merge_finish = true;
      schedule_stats_.finish_cnt_++;

      if (tablet.get_tablet_meta().report_status_.need_report()) {
        if (OB_TMP_FAIL(GCTX.ob_service_->submit_tablet_update_task(MTL_ID(), ls_id, tablet_id))) {
          LOG_WARN("failed to submit tablet update task to report", K(tmp_ret), K(MTL_ID()), K(tablet_id));
        } else if (OB_TMP_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
          LOG_WARN("failed to update tablet report status", K(tmp_ret), K(MTL_ID()), K(tablet_id));
        }
      }
    }
    ObArenaAllocator allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    LOG_DEBUG("schedule tablet medium", K(ret), K(ls_id), K(tablet_id), K(tablet_merge_finish),
      KPC(latest_major), K(param));
    bool could_schedule_next_medium = true;
    bool check_medium_finish = false;
    if (!param.is_leader_ || OB_ISNULL(latest_major)) {
      // follower or no major: do nothing
      could_schedule_next_medium = false;
    } else if (OB_TMP_FAIL(tablet_handle.get_obj()->read_medium_info_list(
                   allocator, medium_list))) {
      LOG_WARN("failed to read medium info list", K(tmp_ret), K(tablet_id));
    } else if (medium_list->need_check_finish()) { // need check finished
      if (OB_TMP_FAIL(func.check_medium_finish(param.ls_locality_))) {
        LOG_WARN("failed to check medium finish", K(tmp_ret), K(ls_id), K(tablet_id), KPC(medium_list));
      } else {
        check_medium_finish = true;
      }
    }

    if (could_schedule_next_medium && param.could_major_merge_
        && (!tablet_merge_finish || param.enable_adaptive_compaction_ || check_medium_finish)
        && tablet_could_schedule_merge) {
      if (OB_TMP_FAIL(func.schedule_next_medium_for_leader(
          tablet_merge_finish ? 0 : param.merge_version_, param.is_tombstone_, schedule_stats_, medium_clog_submitted))) { // schedule another round
        if (OB_NOT_MASTER == tmp_ret) {
          param.is_leader_ = false;
        } else {
          LOG_WARN("failed to schedule next medium", K(tmp_ret), K(ls_id), K(tablet_id));
        }
      } else {
        schedule_stats_.schedule_cnt_++;
      }
    }

    if (param.could_major_merge_ && OB_TMP_FAIL(ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
        ls, tablet, succ_create_dag, param.merge_version_, scheduler_called))) {
      if (OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule medium", K(tmp_ret), K(ret), K(ls_id), K(tablet_id));
      }
    }
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing, should not loop tablets
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("compat_version is smaller than DATA_VERSION_4_1_0_0, cannot schedule medium", K(compat_version));
      if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MEDIUM_MERGE, ObLSID(INT64_MAX), ObTabletID(INT64_MAX),
              ObSuspectInfoType::SUSPECT_INVALID_DATA_VERSION, compat_version, DATA_VERSION_4_1_0_0))) {
        LOG_WARN("failed to add suspect info", K(tmp_ret));
      }
    }
  } else if (OB_FAIL(medium_ls_tablet_iter_.build_iter(schedule_tablet_batch_size_))) {
    LOG_WARN("failed to init iterator", K(ret));
  } else {
    bool all_ls_weak_read_ts_ready = true;
    int64_t merge_version = get_frozen_version();
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    LOG_INFO("start schedule all tablet merge", K(merge_version), K(medium_ls_tablet_iter_));

    if (INIT_COMPACTION_SCN == merge_version) {
      merge_version = 0;
    } else if (merge_version > merged_version_) {
      if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress(merge_version,
          share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
        LOG_WARN("failed to update progress", K(tmp_ret), K(merge_version));
      }
    }

    if (REACH_TENANT_TIME_INTERVAL(CHECK_LS_LOCALITY_INTERVAL)) {
      if (OB_TMP_FAIL(ls_locality_cache_.refresh_ls_locality())) {
        LOG_WARN("failed to refresh ls locality", K(tmp_ret));
        ADD_SUSPECT_INFO(MEDIUM_MERGE, ObLSID(INT64_MAX), ObTabletID(INT64_MAX),
          ObSuspectInfoType::SUSPECT_FAILED_TO_REFRESH_LS_LOCALITY, tmp_ret);
      }
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

    if (!medium_ls_tablet_iter_.tenant_merge_finish()) { // wait major compaction
      if (all_ls_weak_read_ts_ready) { // check schedule Timer Task
        if (schedule_stats_.add_weak_read_ts_event_flag_ && medium_ls_tablet_iter_.is_scan_finish()) { // all ls scan finish
          schedule_stats_.add_weak_read_ts_event_flag_ = false;
          ADD_COMPACTION_EVENT(
              MTL_ID(),
              MAJOR_MERGE,
              merge_version,
              ObServerCompactionEvent::WEAK_READ_TS_READY,
              ObTimeUtility::fast_current_time(),
              "check_weak_read_ts_cnt", schedule_stats_.check_weak_read_ts_cnt_ + 1);
        }
      } else {
        schedule_stats_.check_weak_read_ts_cnt_++;
      }

      if (medium_ls_tablet_iter_.is_scan_finish() && REACH_TENANT_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
            MTL_ID(),
            MAJOR_MERGE,
            merge_version,
            ObServerCompactionEvent::SCHEDULER_LOOP,
            ObTimeUtility::fast_current_time(),
            "schedule_stats",
            schedule_stats_);
      }
    }

    if (OB_SUCC(ret) && medium_ls_tablet_iter_.tenant_merge_finish() && merge_version > merged_version_) {
      merged_version_ = merge_version;
      LOG_INFO("all tablet major merge finish", K(merged_version_), K(merge_version));
      DEL_SUSPECT_INFO(MEDIUM_MERGE, ObLSID(INT64_MAX), ObTabletID(INT64_MAX));
      if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress(
          merge_version,
          share::ObIDag::DAG_STATUS_FINISH))) {
        LOG_WARN("failed to finish progress", K(tmp_ret), K(merge_version));
      }

      const int64_t current_time = ObTimeUtility::fast_current_time();
      ADD_COMPACTION_EVENT(
          MTL_ID(),
          MAJOR_MERGE,
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
        K(merged_version_));
    if (medium_ls_tablet_iter_.is_scan_finish()) {
      schedule_stats_.clear_tablet_cnt();
    }
  }
  return ret;
}

int ObTenantTabletScheduler::restart_schedule_timer_task(
  const int64_t schedule_interval,
  const int64_t tg_id,
  common::ObTimerTask &timer_task)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(tg_id, timer_task, is_exist))) {
    LOG_ERROR("failed to check merge schedule task exist", K(ret));
  } else if (is_exist && OB_FAIL(TG_CANCEL_R(tg_id, timer_task))) {
    LOG_WARN("failed to cancel task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, timer_task, schedule_interval, true/*repeat*/))) {
    LOG_WARN("Fail to schedule timer task", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::get_min_dependent_schema_version(int64_t &min_schema_version)
{
  int ret = OB_SUCCESS;
  min_schema_version = OB_INVALID_VERSION;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  if (OB_FAIL(MTL(storage::ObTenantFreezeInfoMgr*)->get_min_dependent_freeze_info(freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("freeze info is not exist", K(ret));
    } else {
      LOG_WARN("failed to get freeze info", K(ret));
    }
  } else {
    min_schema_version = freeze_info.schema_version;
  }
  return ret;
}

int ObTenantTabletScheduler::update_report_scn_as_ls_leader(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObRole role = INVALID_ROLE;
  const int64_t major_merged_scn = get_inner_table_merged_scn();
  bool need_merge = false;
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls_id));
  } else if (!need_merge) {
    ret = OB_STATE_NOT_MATCH; // do nothing
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_palf_role(ls_id, role))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
    }
  } else if (is_leader_by_election(role)) {
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

#ifdef ERRSIM
void ObTenantTabletScheduler::errsim_after_mini_schedule_adaptive(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObAdaptiveMergePolicy::AdaptiveCompactionEvent &event,
    bool &medium_is_cooling_down,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  // ATTENTION !!!: 2 tracepoint can only hit one at once
  #define SCHEDULE_META_MEDIUM_ERRSIM(tracepoint, cooling_down)              \
    do {                                                                     \
      if (OB_SUCC(ret)) {                                                    \
        ret = OB_E((EventTable::tracepoint)) OB_SUCCESS;                     \
        if (OB_FAIL(ret)) {                                                  \
          ret = OB_SUCCESS;                                                  \
          STORAGE_LOG(INFO, "ERRSIM " #tracepoint);                          \
          reason = ObAdaptiveMergePolicy::TOMBSTONE_SCENE;                   \
          medium_is_cooling_down = cooling_down;                             \
        }                                                                    \
      }                                                                      \
    } while(0);
  SCHEDULE_META_MEDIUM_ERRSIM(EN_COMPACTION_SCHEDULE_MEDIUM_MERGE_AFTER_MINI, false /*cooling_down*/);
  SCHEDULE_META_MEDIUM_ERRSIM(EN_COMPACTION_SCHEDULE_META_MERGE, true /*cooling_down*/);
  #undef SCHEDULE_META_MEDIUM_ERRSIM

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(EN_COMPACTION_DISABLE_META_MERGE_AFTER_MINI)) {
    reason = ObAdaptiveMergePolicy::NONE;
    LOG_INFO("ERRSIM EN_COMPACTION_DISABLE_META_MERGE_AFTER_MINI: disable meta merge after mini", K(ret), K(ls_id), K(tablet_id));
  }

  bool is_tombstone_scene = ObAdaptiveMergePolicy::NONE != reason;
  STORAGE_LOG(INFO, "try_schedule_adaptive_merge hit errsim", K(ret), K(is_tombstone_scene), K(medium_is_cooling_down));
}
#endif

int ObTenantTabletScheduler::try_schedule_adaptive_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const ObAdaptiveMergePolicy::AdaptiveCompactionEvent &event,
    const int64_t update_row_cnt,
    const int64_t delete_row_cnt,
    bool &create_dag)
{
  int ret = OB_SUCCESS;
  create_dag = false;
  uint64_t compat_version = 0;
  const ObTablet *tablet = nullptr;
  bool medium_is_cooling_down = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN("failed to get data version", K(ret));
  } else if (not_compat_for_queuing_mode(compat_version)) {
    // do nothing, buffer table opt (including meta major merge) only supported in version [4.2.1.5, 4.2.2) union [4.2.3, 4.3)
    if (REACH_TENANT_TIME_INTERVAL(2 * 60 * 1000 * 1000L /*120s*/)) {
      LOG_INFO("compat_version < 4.2.1.5 or 4.2.2 <= compat_version < 4.2.3, no need to schedule adaptive merge after mini", K(compat_version));
    }
  } else if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_handle.is_valid() || !ObAdaptiveMergePolicy::need_schedule_meta(event))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle), K(event));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (ObAdaptiveMergePolicy::need_schedule_medium(event) && OB_FAIL(ObAdaptiveMergePolicy::check_medium_cooling_down(*tablet, medium_is_cooling_down))) {
    LOG_WARN("failed to check medium cooling down", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    ObTableModeFlag mode = ObTableModeFlag::TABLE_MODE_NORMAL;
    ObAdaptiveMergePolicy::AdaptiveMergeReason reason = ObAdaptiveMergePolicy::NONE;

    if (OB_FAIL(ObAdaptiveMergePolicy::check_adaptive_merge_reason_for_event(
        *ls_handle.get_ls(),
        *tablet,
        event,
        update_row_cnt,
        delete_row_cnt,
        mode,
        reason))) {
      LOG_WARN("failed to check adaptive merge reason", K(ret), K(ls_handle), K(tablet_handle));
#ifdef ERRSIM
    } else if (ObAdaptiveMergePolicy::AdaptiveCompactionEvent::SCHEDULE_AFTER_MINI == event
            && FALSE_IT(errsim_after_mini_schedule_adaptive(ls_id, tablet_id, event, medium_is_cooling_down, reason))) {
#endif
    } else if (ObAdaptiveMergePolicy::NONE == reason) {
    } else if (ObAdaptiveMergePolicy::is_schedule_medium(mode) && ObAdaptiveMergePolicy::need_schedule_medium(event) && !medium_is_cooling_down) {
      if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_medium_merge(ls_handle, tablet_id, create_dag))) {
        LOG_WARN_RET(tmp_ret, "failed to schedule medium merge for tablet after mini", K(ls_id), K(tablet_id));
      }
    } else if (ObAdaptiveMergePolicy::is_schedule_meta(mode)) {
      if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_meta_merge(ls_handle, tablet_handle, create_dag))) {
        LOG_WARN_RET(tmp_ret, "failed to schedule meta merge for tablet", K(ls_id), K(tablet_id));
      } else if (create_dag && ObAdaptiveMergePolicy::AdaptiveCompactionEvent::SCHEDULE_META == event) {
         LOG_INFO("[Buffer-Opt] Try to schedule tablet meta merge background", K(ret), K(ls_id), K(tablet_id));
      }
    }

    if (ObAdaptiveMergePolicy::AdaptiveCompactionEvent::SCHEDULE_AFTER_MINI == event) {
      LOG_INFO("[Buffer-Opt] Try to schedule tablet medium/meta after mini", K(ret), K(tmp_ret), K(ls_id), K(tablet_id), "is_tombstone_scene", ObAdaptiveMergePolicy::NONE != reason,
               "mode", table_mode_flag_to_str(mode), K(medium_is_cooling_down), K(event), K(update_row_cnt), K(delete_row_cnt), K(create_dag));
    } else if (REACH_TIME_INTERVAL(30 * 1000 * 1000 /*30s*/)) {
      LOG_INFO("Try schedule tablet adaptive merge", K(ret), K(tmp_ret), K(ls_id), K(tablet_id), "is_tombstone_scene", ObAdaptiveMergePolicy::NONE != reason, K(event), K(create_dag));
    }
  }
  return ret;
}

// ------------------- ObCompactionScheduleIterator -------------------- //
int ObCompactionScheduleIterator::build_iter(const int64_t batch_tablet_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_tablet_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(batch_tablet_cnt));
  } else if (!is_valid()) {
    ls_ids_.reuse();
    if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids_))) {
      LOG_WARN("failed to get all ls id", K(ret));
    } else {
      ls_idx_ = -1;
      tablet_idx_ = 0;
      tablet_ids_.reuse();
      scan_finish_ = false;
      merge_finish_ = true;
      ls_tablet_svr_ = nullptr;
      schedule_tablet_cnt_ = 0;
      report_scn_flag_ = false;
      // check every time start loop all tablet
      if (REACH_TENANT_TIME_INTERVAL(CHECK_REPORT_SCN_INTERVAL)) {
        report_scn_flag_ = true;
      }
#ifdef ERRSIM
      report_scn_flag_ = true;
#endif
      LOG_TRACE("build iter", K(ret), KPC(this));
    }
  } else { // iter is invalid, no need to build, just set var to start cur batch
    (void) start_cur_batch();
  }
  if (OB_SUCC(ret)) {
    max_batch_tablet_cnt_ = batch_tablet_cnt;
  }
  return ret;
}

int ObCompactionScheduleIterator::get_next_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (-1 == ls_idx_
    || tablet_idx_ >= tablet_ids_.count()) { // tablet iter end, need get next ls
    ++ls_idx_;
    ls_tablet_svr_ = nullptr;
    tablet_ids_.reuse();
    LOG_TRACE("tablet iter end", K(ret), K(ls_idx_), K(tablet_idx_), "tablet_cnt", tablet_ids_.count(), K_(ls_ids));
  }
  do {
     if (finish_cur_batch_) {
      ret = OB_ITER_END;
     } else if (ls_idx_ >= ls_ids_.count()) {
      scan_finish_ = true;
      ret = OB_ITER_END;
    } else if (OB_FAIL(get_cur_ls_handle(ls_handle))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_TRACE("ls not exist", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
      } else {
        LOG_WARN("failed to get ls", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
        ret = OB_LS_NOT_EXIST;
      }
       skip_cur_ls();
    } else {
      ls_tablet_svr_ = ls_handle.get_ls()->get_tablet_svr();
    }
  } while (OB_LS_NOT_EXIST == ret);
  return ret;
}

void ObCompactionScheduleIterator::reset()
{
  scan_finish_ = false;
  merge_finish_ = false;
  finish_cur_batch_ = false;
  ls_idx_ = 0;
  tablet_idx_ = 0;
  schedule_tablet_cnt_ = 0;
  ls_ids_.reuse();
  tablet_ids_.reuse();
  ls_tablet_svr_ = nullptr;
  report_scn_flag_ = false;
}

bool ObCompactionScheduleIterator::is_valid() const
{
  return ls_ids_.count() > 0 && ls_idx_ >= 0 && nullptr != ls_tablet_svr_
    && (ls_idx_ < ls_ids_.count() - 1
      || (ls_idx_ == ls_ids_.count() - 1 && tablet_idx_ < tablet_ids_.count()));
    // have remain ls or have remain tablet
}

int ObCompactionScheduleIterator::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_tablet_svr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet svr is unexpected null", K(ret), KPC(this));
  } else if (tablet_ids_.empty()) {
    if (OB_FAIL(get_tablet_ids())) {
      LOG_WARN("failed to get tablet ids", K(ret));
    } else {
      tablet_idx_ = 0; // for new ls, set tablet_idx_ = 0
      LOG_TRACE("build iter", K(ret), K_(ls_idx), "ls_id", ls_ids_[ls_idx_], K(tablet_ids_));
    }
  }
  if (OB_SUCC(ret)) {
    do {
      if (tablet_idx_ >= tablet_ids_.count()) {
        ret = OB_ITER_END;
      } else if (schedule_tablet_cnt_ >= max_batch_tablet_cnt_) {
        finish_cur_batch_ = true;
        ret = OB_ITER_END;
      } else {
        const common::ObTabletID &tablet_id = tablet_ids_.at(tablet_idx_);
        if (OB_FAIL(get_tablet_handle(tablet_id, tablet_handle))) {
          if (OB_TABLET_NOT_EXIST != ret) {
            LOG_WARN("fail to get tablet", K(ret), K(tablet_idx_), K(tablet_id));
            ret = OB_TABLET_NOT_EXIST;
          }
          tablet_idx_++;
        } else {
          tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          tablet_idx_++;
          schedule_tablet_cnt_++;
        }
      }
    } while (OB_TABLET_NOT_EXIST == ret);
  }
  return ret;
}
// TODO(@lixia.yq) add errsim obtest here
int ObCompactionScheduleIterator::get_cur_ls_handle(ObLSHandle &ls_handle)
{
  return MTL(storage::ObLSService *)->get_ls(ls_ids_[ls_idx_], ls_handle, mod_);
}

int ObCompactionScheduleIterator::get_tablet_ids()
{
  return ls_tablet_svr_->get_all_tablet_ids(is_major_/*except_ls_inner_tablet*/, tablet_ids_);
}

int ObCompactionScheduleIterator::get_tablet_handle(
  const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  return ls_tablet_svr_->get_tablet(tablet_id, tablet_handle, 0/*timeout*/, ObMDSGetTabletMode::READ_ALL_COMMITED);
}

int64_t ObCompactionScheduleIterator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_idx), K_(ls_ids), K_(tablet_idx), K(tablet_ids_.count()), K_(schedule_tablet_cnt), K_(max_batch_tablet_cnt),
      K_(report_scn_flag));
  if (is_valid()) {
    J_COMMA();
    J_KV("cur_ls", ls_ids_.at(ls_idx_));
    if (!tablet_ids_.empty() && tablet_idx_ > 0 && tablet_idx_ < tablet_ids_.count()) {
      J_COMMA();
      J_KV("next_tablet", tablet_ids_.at(tablet_idx_));
    }
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage
} // namespace oceanbase
