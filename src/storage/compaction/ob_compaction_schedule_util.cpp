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
#include "ob_compaction_schedule_util.h"
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
    tenant_status_(),
    major_merge_status_(false),
    is_stop_(false)
  {}

ObBasicMergeScheduler::~ObBasicMergeScheduler()
{
}

void ObBasicMergeScheduler::reset()
{
  is_stop_ = true;
  obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
  frozen_version_ = 0;
  inner_table_merged_scn_ = 0;
  merged_version_ = 0;
  tenant_status_.reset();
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
    if (REACH_THREAD_TIME_INTERVAL(PRINT_SLOG_REPLAY_INVERVAL)) {
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

void ObBasicMergeScheduler::update_frozen_version_and_merge_progress(const int64_t broadcast_version)
{
  if (broadcast_version > get_frozen_version()) {
    {
      obsys::ObWLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version_ = broadcast_version;
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->init_progress(broadcast_version))) {
      LOG_WARN_RET(tmp_ret, "failed to init progress", K(broadcast_version));
    }
  }
}

void ObBasicMergeScheduler::update_merged_version(const int64_t merged_version)
{
   merged_version_ = merged_version;
}

void ObBasicMergeScheduler::try_finish_merge_progress(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  const int64_t merged_scn = get_inner_table_merged_scn();

  if (merged_scn > merged_version_) {
    FLOG_INFO("last major merge finish", K(merge_version), K(merged_scn), K(merged_version_));
    merged_version_ = merged_scn;
  }

  if (merged_version_ >= merge_version && OB_FAIL(MTL(ObTenantCompactionProgressMgr *)->finish_progress(merged_version_))) {
    LOG_WARN("failed to finish progress", KR(ret), K(merge_version), K(merged_version_));
  }
}

} // namespace compaction
} // namespace oceanbase
