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

namespace oceanbase
{
using namespace compaction;
using namespace common;
using namespace share;

namespace storage
{

ObFastFreezeChecker::ObFastFreezeChecker()
  : enable_fast_freeze_(false)
{
}

ObFastFreezeChecker::~ObFastFreezeChecker()
{
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
  need_fast_freeze = false;
  ObTableHandleV2 table_handle;
  memtable::ObMemtable *memtable = nullptr;

  if (!enable_fast_freeze_) {
  } else if (OB_FAIL(tablet.get_active_memtable(table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get active memtable", K(ret));
    }
  } else if (OB_FAIL(table_handle.get_data_memtable(memtable))) {
    LOG_WARN("failed to get memtalbe", K(ret), K(table_handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable", K(ret), KPC(memtable));
  } else if (!memtable->is_active_memtable()) {
  } else if (ObTimeUtility::current_time() < memtable->get_mt_stat().frozen_time_ + FAST_FREEZE_INTERVAL_US) {
    if (REACH_TENANT_TIME_INTERVAL(120 * 1000 * 1000)) {
      LOG_INFO("[FastFreeze] no need to check fast freeze now", K(tablet));
    }
  } else if (OB_FAIL(check_hotspot_need_fast_freeze(*memtable, need_fast_freeze))) {
    LOG_WARN("failed to check hotspot need fast freeze", K(ret));
  }
  return ret;
}

int ObFastFreezeChecker::check_hotspot_need_fast_freeze(
    const memtable::ObMemtable &memtable,
    bool &need_fast_freeze)
{
  int ret = OB_SUCCESS;
  need_fast_freeze = false;
  if (!memtable.is_active_memtable()) {
  } else {
    need_fast_freeze = memtable.has_hotspot_row();
    if (need_fast_freeze && REACH_TENANT_TIME_INTERVAL(120 * 1000 * 1000)) {
      LOG_INFO("[FastFreeze] current tablet has hotspot row, need fast freeze", K(memtable));
    }
  }
  return ret;
}


void ObTenantTabletScheduler::MergeLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->merge_all())) {
    LOG_WARN("Fail to merge all partition", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("MergeLoopTask", K(cost_ts));
}

void ObTenantTabletScheduler::SSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->update_upper_trans_version_and_gc_sstable())) {
    LOG_WARN("Fail to update upper_trans_version and gc sstable", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("SSTableGCTask", K(cost_ts));
}

constexpr ObMergeType ObTenantTabletScheduler::MERGE_TYPES[];

ObTenantTabletScheduler::ObTenantTabletScheduler()
 : is_inited_(false),
   major_merge_status_(true),
   is_stop_(true),
   merge_loop_tg_id_(0),
   sstable_gc_tg_id_(0),
   schedule_interval_(0),
   bf_queue_(),
   frozen_version_lock_(),
   frozen_version_(INIT_COMPACTION_SCN),
   merged_version_(INIT_COMPACTION_SCN),
   schedule_stats_(),
   merge_loop_task_(),
   sstable_gc_task_(),
   fast_freeze_checker_()
{
  STATIC_ASSERT(static_cast<int64_t>(NO_MAJOR_MERGE_TYPE_CNT) == ARRAYSIZEOF(MERGE_TYPES), "merge type array len is mismatch");
}

ObTenantTabletScheduler::~ObTenantTabletScheduler()
{
  destroy();
}

void ObTenantTabletScheduler::destroy()
{
  stop();
  wait();
  TG_DESTROY(merge_loop_tg_id_);
  TG_DESTROY(sstable_gc_tg_id_);
  bf_queue_.destroy();
  frozen_version_ = 0;
  merged_version_ = 0;
  schedule_stats_.reset();
  merge_loop_tg_id_ = 0;
  sstable_gc_tg_id_ = 0;
  schedule_interval_ = 0;
  is_inited_ = false;
  LOG_INFO("The ObTenantTabletScheduler destroy");
}

int ObTenantTabletScheduler::init()
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval = tenant_config->ob_compaction_schedule_interval;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
    }
  } // end of ObTenantConfigGuard
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
  } else {
    schedule_interval_ = schedule_interval;
    is_inited_ = true;
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
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTableGC, sstable_gc_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(sstable_gc_tg_id_))) {
    LOG_WARN("failed to start sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(sstable_gc_tg_id_, sstable_gc_task_, SSTABLE_GC_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule sstable gc task", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::reload_tenant_config()
{
  int ret = OB_SUCCESS;
  int64_t merge_schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      merge_schedule_interval = tenant_config->ob_compaction_schedule_interval;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
    }
  } // end of ObTenantConfigGuard
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else if (schedule_interval_ != merge_schedule_interval) {
    if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval))) {
      LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
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
  TG_STOP(merge_loop_tg_id_);
  TG_STOP(sstable_gc_tg_id_);
  stop_major_merge();
}

void ObTenantTabletScheduler::wait()
{
  TG_WAIT(merge_loop_tg_id_);
  TG_WAIT(sstable_gc_tg_id_);
}

int ObTenantTabletScheduler::try_remove_old_table(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls.get_ls_id();
  // only need NORMAL tablet here
  ObLSTabletIterator tablet_iter(ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US);

  if (OB_FAIL(ls.build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
  }

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  common::ObTabletID tablet_id;
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
    } else {
      int64_t multi_version_start = 0;
      int64_t min_reserved_snapshot = 0;
      bool need_remove = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(tablet->get_kept_multi_version_start(multi_version_start, min_reserved_snapshot))) {
        LOG_WARN("failed to get multi version start", K(tmp_ret), K(tablet_id));
      } else if (OB_TMP_FAIL(tablet->check_need_remove_old_table(multi_version_start, need_remove))) {
        LOG_WARN("failed to check need remove old store", K(tmp_ret), K(multi_version_start), K(tablet_id));
      } else if (need_remove) {
        const ObStorageSchema &storage_schema = tablet->get_storage_schema();
        const int64_t rebuild_seq = ls.get_rebuild_seq();
        ObUpdateTableStoreParam param(tablet->get_snapshot_version(), multi_version_start, &storage_schema, rebuild_seq);
        ObTabletHandle new_tablet_handle; // no use here
        if (OB_TMP_FAIL(ls.update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
          LOG_WARN("failed to update table store", K(tmp_ret), K(param), K(tenant_id), K(ls_id), K(tablet_id));
        } else {
          FLOG_INFO("success to remove old table in table store", K(tmp_ret), K(tenant_id), K(ls_id),
              K(tablet_id), K(multi_version_start), K(min_reserved_snapshot), KPC(tablet));
        }
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
    } else {
      (void) ls->try_update_uppder_trans_version();
      (void) try_remove_old_table(*ls);
    }
  }
  return ret;
}

int ObTenantTabletScheduler::check_ls_compaction_finish(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->check_ls_compaction_dag_exist(ls_id, exist))) {
    LOG_WARN("failed to check ls compaction dag", K(ret), K(ls_id));
  } else if (exist) {
    // the compaction dag exists, need retry later.
    ret = OB_EAGAIN;
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

int ObTenantTabletScheduler::schedule_load_bloomfilter(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block id", K(ret), K(macro_id));
  } else {
    ObBloomFilterLoadTask task(MTL_ID(), macro_id);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to add bloomfilter load task", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::merge_all()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(schedule_all_tablets())) {
    LOG_WARN("failed to schedule all tablet major merge", K(ret));
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
      obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version_ = broadcast_version;
    }

    LOG_INFO("schedule merge major version", K(broadcast_version));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->add_progress(broadcast_version))) {
      LOG_WARN("failed to add progress", K(tmp_ret), K(broadcast_version));
    }

    schedule_stats_.start_merge(); // set all statistics
    ADD_COMPACTION_EVENT(
        MTL_ID(),
        MAJOR_MERGE,
        broadcast_version,
        ObServerCompactionEvent::GET_COMPACTION_INFO,
        schedule_stats_.start_timestamp_,
        "last_merged_version",
        merged_version_);

    if (OB_FAIL(restart_schedule_timer_task(CHECK_WEAK_READ_TS_SCHEDULE_INTERVAL))) {
      LOG_WARN("failed to restart schedule timer task", K(ret));
    }
  }
  return ret;
}

bool ObTenantTabletScheduler::check_weak_read_ts_ready(
    const int64_t &merge_version,
    ObLS &ls)
{
  bool is_ready_for_compaction = false;
  int64_t weak_read_ts = 0;

  if (FALSE_IT(weak_read_ts = ls.get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  } else if (weak_read_ts < merge_version) {
    FLOG_INFO("current slave_read_ts is smaller than freeze_ts, try later",
              "ls_id", ls.get_ls_id(), K(merge_version), K(weak_read_ts));
  } else {
    is_ready_for_compaction = true;
  }
  return is_ready_for_compaction;
}

int ObTenantTabletScheduler::check_and_freeze_for_major(
    const common::ObTabletID &tablet_id,
    const int64_t &merge_version,
    ObLS &ls)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls.get_ls_id();

  // TODO: @dengzhi.ldz opt force freeze when no inc data in active memtable
  if (OB_UNLIKELY(merge_version > MTL(ObTenantTabletScheduler *)->get_frozen_version())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(merge_version));
  } else if (OB_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(tablet_id, true/*force_freeze*/))) {
    LOG_WARN("failed to force freeze tablet", K(ret), K(ls_id), K(tablet_id));
  } else {
    LOG_DEBUG("succeed to freeze tablet before merge", K(ret), K(ls_id), K(tablet_id),
        K(merge_version));
  }

  return ret;
}

void ObTenantTabletScheduler::stop_major_merge()
{
  if (major_merge_status_) {
    LOG_INFO("major merge has been paused!");
    major_merge_status_ = false;
  }
}

void ObTenantTabletScheduler::resume_major_merge()
{
  if (!major_merge_status_) {
    LOG_INFO("major merge has been resumed!");
    major_merge_status_ = true;
  }
}

int64_t ObTenantTabletScheduler::get_frozen_version() const
{
  obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_;
}

bool ObTenantTabletScheduler::check_tx_table_ready(ObLS &ls, const int64_t check_log_ts)
{
  int ret = OB_SUCCESS;
  bool tx_table_ready = false;
  int64_t max_replay_log_ts = 0;
  if (OB_FAIL(ls.get_max_decided_log_ts_ns(max_replay_log_ts))) {
    LOG_WARN("failed to get max decided log_ts", K(ret), "ls_id", ls.get_ls_id());
  } else if (check_log_ts <= max_replay_log_ts) {
    tx_table_ready = true;
    LOG_INFO("tx table ready", "sstable_end_log_ts", check_log_ts, K(max_replay_log_ts));
  }

  return tx_table_ready;
}

int ObTenantTabletScheduler::check_ls_state(ObLS &ls, bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  if (ls.is_deleted()) {
    LOG_INFO("ls is deleted", K(ret), K(ls));
  } else if (ls.is_offline()) {
    LOG_INFO("ls is offline", K(ret), K(ls));
  } else {
    need_merge = true;
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge_dag(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.merge_type_ = merge_type;
  param.merge_version_ = merge_snapshot_version;
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to schedule tablet merge dag", K(ret));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tx_table_merge(
    const ObLSID &ls_id,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletTableStore &table_store = tablet.get_table_store();
  bool need_merge = false;
  if (OB_FAIL(ObPartitionMergePolicy::check_need_mini_minor_merge(tablet, need_merge))) {
    LOG_WARN("failed to check need merge", K(ret), K(table_store));
  } else if (need_merge) {
    ObTabletMergeDagParam param;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet.get_tablet_meta().tablet_id_;
    param.merge_type_ = MINI_MINOR_MERGE;
    param.merge_version_ = ObVersionRange::MIN_VERSION;
    if (OB_TMP_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW == tmp_ret) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule tx tablet merge dag", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_minor_merge(const ObLSID ls_id, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  for (int i = 0; OB_SUCC(ret) && i < NO_MAJOR_MERGE_TYPE_CNT; ++i) {
    bool need_merge = false;
    if (OB_FAIL(ObPartitionMergePolicy::check_need_minor_merge[MERGE_TYPES[i]](tablet, need_merge))) {
      if (OB_NO_NEED_MERGE == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("tablet no need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet_id), K(tablet));
      } else {
        LOG_WARN("failed to check need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet));
      }
    } else if (need_merge && OB_TMP_FAIL(schedule_merge_dag(ls_id, tablet_id, MERGE_TYPES[i], ObVersionRange::MIN_VERSION))) {
      if (OB_SIZE_OVERFLOW == tmp_ret) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to schedule tablet merge dag", K(tmp_ret));
      }
    } else if (need_merge) {
      LOG_DEBUG("success to schedule tablet minor merge", K(tmp_ret), K(ls_id), K(tablet_id), "merge_type", MERGE_TYPES[i]);
    }
  } // end of for
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_major_merge(
    int64_t &merge_version,
    ObLS &ls,
    ObTablet &tablet,
    bool &tablet_merge_finish,
    ObScheduleStatistics &schedule_stats,
    const bool enable_force_freeze)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  tablet_merge_finish = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObLSID ls_id = ls.get_ls_id();
  bool need_merge = false;
  bool can_merge = false;
  bool need_force_freeze = false;
  if (!check_weak_read_ts_ready(merge_version, ls)) {
    // ls weak read ts is not ready
  } else if (OB_FAIL(ObPartitionMergePolicy::check_need_major_merge(
          tablet,
          merge_version,
          need_merge,
          can_merge,
          need_force_freeze))) {
    LOG_WARN("failed to check need major merge", K(ret), K(ls_id), K(tablet_id));
  } else if (!need_merge) { // no need merge
    tablet_merge_finish = true;
    schedule_stats.finish_cnt_++;
    LOG_DEBUG("tablet no need to merge now", K(ret), K(ls_id), K(tablet_id), K(merge_version));
  } else if (can_merge) {
    if (OB_TMP_FAIL(schedule_merge_dag(ls_id, tablet_id, MAJOR_MERGE, merge_version))) {
      if (OB_SIZE_OVERFLOW == tmp_ret) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_EAGAIN != tmp_ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(tmp_ret));
      }
    } else {
      schedule_stats.schedule_cnt_++;
      LOG_DEBUG("success to schedule tablet major merge", K(tmp_ret), K(ls_id), K(tablet_id),
          K(need_force_freeze));
    }
  } else if (need_force_freeze && enable_force_freeze) {
    schedule_stats.force_freeze_cnt_++;
    if (OB_TMP_FAIL(check_and_freeze_for_major(tablet_id, merge_version, ls))) {
      LOG_WARN("failed to check and freeze for major", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  }

  if (OB_SUCC(ret) && tablet_merge_finish && tablet.get_tablet_meta().report_status_.need_report()) {
    if (OB_TMP_FAIL(GCTX.ob_service_->submit_tablet_update_task(MTL_ID(), ls_id, tablet_id))) {
      LOG_WARN("failed to submit tablet update task to report", K(tmp_ret), K(MTL_ID()), K(tablet_id));
    } else if (OB_TMP_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
      LOG_WARN("failed to update tablet report status", K(tmp_ret), K(MTL_ID()), K(tablet_id));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_merge(
    int64_t &merge_version,
    ObLS &ls,
    bool &ls_merge_finish,
    bool &all_ls_weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US);
  bool need_merge = false;
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
  } else if (OB_FAIL(ls.build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
  } else {
    const ObLSID &ls_id = ls.get_ls_id();
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    int tmp_ret = OB_SUCCESS;
    bool schedule_minor_flag = true;
    bool schedule_major_flag = merge_version > ObVersionRange::MIN_VERSION;
    bool need_fast_freeze = false;

    // check weak_read_ts
    bool weak_read_ts_ready = false;
    if (schedule_major_flag) {
      if (check_weak_read_ts_ready(merge_version, ls)) {
        weak_read_ts_ready = true;
      } else {
        all_ls_weak_read_ts_ready = false;
      }
    }

    while (OB_SUCC(ret)) { // loop all tablet in ls
      bool tablet_merge_finish = false;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_)) {
      } else if (tablet_id.is_special_merge_tablet()) {
        // schedule minor merge for special tablet
        if (tablet_id.is_mini_and_minor_merge_tablet()
            && OB_TMP_FAIL(schedule_tx_table_merge(
                ls_id,
                *tablet_handle.get_obj()))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            ret = OB_SIZE_OVERFLOW;
          } else {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_handle));
          }
        }
      } else { // data tablet
        if (schedule_minor_flag
            && OB_TMP_FAIL(schedule_tablet_minor_merge(ls_id, *tablet_handle.get_obj()))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_minor_flag = false;
          } else {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_handle));
          }
        }
        if (schedule_major_flag && weak_read_ts_ready
            && could_major_merge_start() && OB_TMP_FAIL(schedule_tablet_major_merge(
                    merge_version,
                    ls,
                    *tablet_handle.get_obj(),
                    tablet_merge_finish,
                    schedule_stats_))) {
          ls_merge_finish = false;
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_major_flag = false;
          } else {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_handle));
          }
        } else {
          ls_merge_finish &= tablet_merge_finish;
        }

        if (OB_SUCC(ret)) {
          need_fast_freeze = false;
          if (!fast_freeze_checker_.need_check()) {
          } else if (OB_TMP_FAIL(fast_freeze_checker_.check_need_fast_freeze(*tablet_handle.get_obj(), need_fast_freeze))) {
            LOG_WARN("failed to check need fast freeze", K(tmp_ret), K(tablet_handle));
          } else if (need_fast_freeze) {
            if (OB_TMP_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(tablet_id, false/*force_freeze*/))) {
              LOG_WARN("failt to freeze tablet", K(tmp_ret), K(tablet_id));
            }
          }
        }

      }
    } // end of while
  } // else
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  } else {
    bool tenant_merge_finish = true;
    bool all_ls_weak_read_ts_ready = true;
    int64_t merge_version = get_frozen_version();
    LOG_INFO("start schedule all tablet merge", K(merge_version));

    if (merge_version > merged_version_) {
      if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress(merge_version,
          share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
        LOG_WARN("failed to update progress", K(tmp_ret), K(merge_version));
      }
    }

    while (OB_SUCC(ret)) {
      bool ls_merge_finish = true;
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
      } else if (OB_TMP_FAIL(schedule_ls_merge(merge_version, *ls, ls_merge_finish, all_ls_weak_read_ts_ready))) {
        tenant_merge_finish = false;
        if (OB_SIZE_OVERFLOW == tmp_ret) {
          break;
        } else {
          LOG_WARN("failed to schedule ls merge", K(tmp_ret), KPC(ls));
        }
      } else {
        tenant_merge_finish &= ls_merge_finish;
        // TODO(DanLing) Optimize: check whether the slave_read_ts of tablet that failed to merge is bigger than frozen ts
      }
    } // end while

    if (!tenant_merge_finish) { // wait major compaction
      if (all_ls_weak_read_ts_ready) { // check schedule Timer Task
        if (OB_FAIL(reload_tenant_config())) {
          LOG_WARN("failed to restart schedule timer task", K(ret));
        }
        if (schedule_stats_.add_weak_read_ts_event_flag_) {
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
        if (OB_FAIL(restart_schedule_timer_task(CHECK_WEAK_READ_TS_SCHEDULE_INTERVAL))) {
          LOG_WARN("failed to restart schedule timer task", K(ret));
        }
      }

      if (REACH_TENANT_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
            MTL_ID(),
            MAJOR_MERGE,
            merge_version,
            ObServerCompactionEvent::SCHEDULER_LOOP,
            ObTimeUtility::fast_current_time(),
            "schedule_stats",
            schedule_stats_);
        schedule_stats_.clear_tablet_cnt();
      }
    }

    if (OB_SUCC(ret) && tenant_merge_finish && merge_version > merged_version_) {
      merged_version_ = merge_version;
      LOG_INFO("all tablet major merge finish", K(merged_version_), K(merge_version));

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

    LOG_INFO("finish schedule all tablet merge", K(merge_version), K(schedule_stats_), K(tenant_merge_finish),
        K(merged_version_));
  }
  return ret;
}

int ObTenantTabletScheduler::restart_schedule_timer_task(const int64_t schedule_interval)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(merge_loop_tg_id_, merge_loop_task_, is_exist))) {
    LOG_ERROR("failed to check merge schedule task exist", K(ret));
  } else if (is_exist) {
    TG_CANCEL(merge_loop_tg_id_, merge_loop_task_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_loop_task_, schedule_interval, true/*repeat*/))) {
    LOG_WARN("Fail to schedule minor merge scan task", K(ret));
  } else {
    schedule_interval_ = schedule_interval;
    LOG_INFO("succeeded to reload new merge schedule interval", K(schedule_interval));
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

} // namespace storage
} // namespace oceanbase
