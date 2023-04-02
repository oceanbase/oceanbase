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
#include "storage/compaction/ob_merge_schedule_info.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

/**
 * -----------------------------------------------ObMajorMergeHistory--------------------------------------------------------
 */

ObMergeStatEntry::ObMergeStatEntry()
  : frozen_version_(0),
    start_time_(0),
    finish_time_(0)
{
}

void ObMergeStatEntry::reset()
{
  frozen_version_ = 0;
  start_time_ = 0;
  finish_time_ = 0;
}

ObMajorMergeHistory::ObMajorMergeHistory()
  : lock_()
{
}

ObMajorMergeHistory::~ObMajorMergeHistory()
{
}

int ObMajorMergeHistory::notify_major_merge_start(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry *pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(frozen_version), K(ret));
  } else {
    obsys::ObWLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        pentry = &(stats_[frozen_version % MAX_KEPT_HISTORY]);
        pentry->reset();
        pentry->frozen_version_ = frozen_version;
        pentry->start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      } else {
        LOG_WARN("Fail to search entry", K(ret));
      }
    } else if (OB_ISNULL(pentry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected error, the pentry is NULL", K(ret));
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMajorMergeHistory::notify_major_merge_finish(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry *pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(frozen_version), K(ret));
  } else {
    obsys::ObWLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      LOG_WARN("Fail to search entry", K(ret));
    } else if (OB_ISNULL(pentry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected error, the pentry is NULL", K(ret));
    } else {
      if (0 == pentry->finish_time_) {
        pentry->finish_time_ = ::oceanbase::common::ObTimeUtility::current_time();
        LOG_INFO("set merge finish time",
                 K(frozen_version),
                 "cost_time", (pentry->finish_time_ - pentry->start_time_) / 1000000,
                 "start_time", time2str(pentry->start_time_),
                 "finish_time", time2str(pentry->finish_time_));
      }
    }
  }
  return ret;
}

int ObMajorMergeHistory::get_entry(const int64_t frozen_version, ObMergeStatEntry &entry)
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry *pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(frozen_version), K(ret));
  } else {
    obsys::ObRLockGuard guard(lock_);
    if (OB_FAIL(search_entry(frozen_version, pentry))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        entry.reset();
      } else {
        LOG_WARN("Fail to search entry", K(frozen_version), K(ret));
      }
    } else {
      entry = *pentry;
    }
  }
  return ret;
}

int ObMajorMergeHistory::search_entry(const int64_t frozen_version, ObMergeStatEntry *&pentry)
{
  int ret = OB_SUCCESS;
  pentry = NULL;
  if (OB_UNLIKELY(frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(frozen_version), K(ret));
  } else if (stats_[frozen_version % MAX_KEPT_HISTORY].frozen_version_ != frozen_version) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    pentry = &(stats_[frozen_version % MAX_KEPT_HISTORY]);
  }
  return ret;
}

/*
 *  ----------------------------------------------ObMinorMergeHistory--------------------------------------------------
 */

ObMinorMergeHistory::ObMinorMergeHistory(const uint64_t tenant_id)
  : mutex_(common::ObLatchIds::INFO_MGR_LOCK),
    count_(0),
    tenant_id_(tenant_id)
{
}

ObMinorMergeHistory::~ObMinorMergeHistory()
{
}

int ObMinorMergeHistory::notify_minor_merge_start(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (MAX_MINOR_HISTORY == count_) {
    MEMMOVE(snapshot_history_, &snapshot_history_[1], sizeof(int64_t) * (count_ - 1));
    --count_;
  }
  int64_t insert_pos = 0;
  for (int64_t i = count_ - 1; -1 == insert_pos && i >= 0; --i) {
    if (snapshot_history_[i] < snapshot_version) {
      insert_pos = i + 1;
    }
  }
  MEMMOVE(&snapshot_history_[insert_pos + 1], &snapshot_history_[insert_pos],
          sizeof(int64_t) * (count_ - insert_pos));
  snapshot_history_[insert_pos] = snapshot_version;
  ++count_;
  SERVER_EVENT_ADD(
      "minor_merge", "minor merge start",
      "tenant_id", tenant_id_,
      "snapshot_version", snapshot_version,
      "checkpoint_type", "DATA_CKPT",
      "checkpoint_cluster_version", GET_MIN_CLUSTER_VERSION());

  FLOG_INFO("[MINOR_MERGE_HISTORY] minor merge start", K(tenant_id_), K(snapshot_version), K_(count));
  return ret;
}

int ObMinorMergeHistory::notify_minor_merge_finish(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (count_ > 0) {
    int64_t i = 0;
    bool is_found = false;
    for (; i < count_ ; ++i) {
      if (snapshot_version >= snapshot_history_[i]) {
        SERVER_EVENT_ADD(
            "minor_merge", "minor merge finish",
            "tenant_id", tenant_id_,
            "snapshot_version", snapshot_history_[i],
            "checkpoint_type", "DATA_CKPT",
            "checkpoint_cluster_version", GET_MIN_CLUSTER_VERSION());
        FLOG_INFO("[MINOR_MERGE_HISTORY] minor merge finish", K(tenant_id_), K(snapshot_version),
                  K_(count));
        is_found = true;
      } else {
        break;
      }
    }
    if (is_found) {
      if (i < count_) {
        MEMMOVE(snapshot_history_, &snapshot_history_[i], sizeof(int64_t) * (count_ - i));
      }
      count_ -= i;
      LOG_INFO("[MINOR_MERGE_HISTORY] notify minor merge finish", K(tenant_id_), K_(count));
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase

