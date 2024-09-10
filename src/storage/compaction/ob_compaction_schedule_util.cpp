/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#endif

namespace oceanbase
{
namespace compaction
{

/**
 * -------------------------------------------------------------------ObBasicMergeScheduler-------------------------------------------------------------------
 */
ObBasicMergeScheduler::ObBasicMergeScheduler()
  : frozen_version_lock_(),
    frozen_version_(INIT_COMPACTION_SCN),
    inner_table_merged_scn_(INIT_COMPACTION_SCN),
    merged_version_(INIT_COMPACTION_SCN),
    min_data_version_(0),
    major_merge_status_(false)
  {}

ObBasicMergeScheduler::~ObBasicMergeScheduler()
{
}

void ObBasicMergeScheduler::reset()
{
  obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
  frozen_version_ = 0;
  inner_table_merged_scn_ = 0;
  merged_version_ = 0;
  min_data_version_ = 0;
  major_merge_status_ = false;
}

ObBasicMergeScheduler* ObBasicMergeScheduler::get_merge_scheduler()
{
  ObBasicMergeScheduler *scheduler = nullptr;
  scheduler = MTL(ObTenantTabletScheduler *);

#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    scheduler = MTL(ObTenantLSMergeScheduler *);
  }
#endif

  return scheduler;
}

bool ObBasicMergeScheduler::could_start_loop_task()
{
  bool can_start = true;
  if (!SERVER_STORAGE_META_SERVICE.is_started()) {
    can_start = false;
    if (REACH_TENANT_TIME_INTERVAL(PRINT_SLOG_REPLAY_INVERVAL)) {
      LOG_INFO("slog replay hasn't finished, cannot start loop task");
    }
  }
  return can_start;
}

void ObBasicMergeScheduler::stop_major_merge()
{
  ATOMIC_SET(&major_merge_status_, false);
  LOG_INFO("major merge has been paused!");
}

void ObBasicMergeScheduler::resume_major_merge()
{
  if (!could_major_merge_start()) {
    ATOMIC_SET(&major_merge_status_, true);
    LOG_INFO("major merge has been resumed!");
  }
}

int64_t ObBasicMergeScheduler::get_frozen_version() const
{
  obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_;
}

bool ObBasicMergeScheduler::is_compacting() const
{
  obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_ > get_inner_table_merged_scn();
}

int ObBasicMergeScheduler::get_min_data_version(uint64_t &min_data_version)
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

int ObBasicMergeScheduler::refresh_data_version()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  const uint64_t cached_data_version = ATOMIC_LOAD(&min_data_version_);
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN_RET(ret, "fail to get data version");
  } else if (OB_UNLIKELY(compat_version < cached_data_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data version is unexpected smaller", KR(ret), K(compat_version), K(cached_data_version));
  } else if (compat_version > cached_data_version) {
    ATOMIC_STORE(&min_data_version_, compat_version);
    LOG_INFO("cache min data version", "old_data_version", cached_data_version,
             "new_data_version", compat_version);
  }
  return ret;
}

void ObBasicMergeScheduler::update_frozen_version(const int64_t broadcast_version)
{
  if (broadcast_version > get_frozen_version()) {
    obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
    frozen_version_ = broadcast_version;
  }
}

void ObBasicMergeScheduler::init_merge_progress(const int64_t broadcast_version)
{
  if (broadcast_version > get_frozen_version()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->add_progress(broadcast_version))) {
      LOG_WARN_RET(tmp_ret, "failed to add progress", K(broadcast_version));
    }
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->init_progress(broadcast_version))) {
      LOG_WARN_RET(tmp_ret, "failed to init progress", K(broadcast_version));
    }
  }
}

void ObBasicMergeScheduler::update_merge_progress(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  const int64_t merged_scn = get_inner_table_merged_scn();

  if (merged_scn > merged_version_) {
    FLOG_INFO("last major merge finish", K(merge_version), K(merged_scn), K(merged_version_));
    merged_version_ = merged_scn;
  }

  const share::ObIDag::ObDagStatus dag_status = merged_version_ >= merge_version
                                              ? share::ObIDag::DAG_STATUS_FINISH
                                              : share::ObIDag::DAG_STATUS_NODE_RUNNING;
  if (OB_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress_status(merged_version_, dag_status))) {
    LOG_WARN("failed to finish progress", KR(ret), K(merge_version), K(merged_version_));
  }
}

} // namespace compaction
} // namespace oceanbase
