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

#include "ob_log_archive_and_restore_driver.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "ob_partition_log_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;

namespace clog {
ObLogArchiveAndRestoreDriver::ObLogArchiveAndRestoreDriver()
    : is_inited_(false),
      partition_service_(NULL),
      last_check_time_for_sync_log_archive_progress_(OB_INVALID_TIMESTAMP),
      last_check_time_for_confirm_clog_(OB_INVALID_TIMESTAMP),
      last_check_time_for_archive_checkpoint_(OB_INVALID_TIMESTAMP),
      last_check_time_for_check_restore_progress_(OB_INVALID_TIMESTAMP),
      last_check_time_for_clear_trans_after_restore_(OB_INVALID_TIMESTAMP),
      last_run_time_(OB_INVALID_TIMESTAMP)
{}

ObLogArchiveAndRestoreDriver::~ObLogArchiveAndRestoreDriver()
{
  destroy();
}

int ObLogArchiveAndRestoreDriver::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(partition_service));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogArchiveAndRestoreDriver has already been inited", K(ret));
  } else {
    partition_service_ = partition_service;
    if (OB_FAIL(start())) {
      CLOG_LOG(ERROR, "ObLogArchiveAndRestoreDriver thread failed to start", KR(ret));
    } else {
      last_check_time_for_sync_log_archive_progress_ = OB_INVALID_TIMESTAMP;
      last_check_time_for_confirm_clog_ = OB_INVALID_TIMESTAMP;
      last_check_time_for_archive_checkpoint_ = OB_INVALID_TIMESTAMP;
      last_check_time_for_check_restore_progress_ = OB_INVALID_TIMESTAMP;
      last_run_time_ = ObTimeUtility::current_time();
      is_inited_ = true;
      CLOG_LOG(INFO, "ObLogArchiveAndRestoreDriver init finished");
    }
  }

  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  return ret;
}

void ObLogArchiveAndRestoreDriver::destroy()
{
  stop();
  wait();
  partition_service_ = NULL;
  is_inited_ = false;
}

void ObLogArchiveAndRestoreDriver::run1()
{
  lib::set_thread_name("LogArchiveDri");
  ObCurTraceId::init(GCONF.self_addr_);
  state_driver_loop();
  CLOG_LOG(INFO, "ob_log_archvie_and_restore_driver will stop");
}

void ObLogArchiveAndRestoreDriver::state_driver_loop()
{
  int64_t cycle_num = 0;
  bool update_flag_for_sync_log_archive_progress = false;
  bool update_flag_for_confirm_clog = false;
  bool update_flag_for_archive_checkpoint = false;
  bool update_flag_for_check_restore_progress = false;
  bool update_flag_for_clear_trans_after_restore = false;

  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    const int64_t start_ts = ObTimeUtility::current_time();
    last_run_time_ = start_ts;

    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    update_flag_for_sync_log_archive_progress =
        start_ts - last_check_time_for_sync_log_archive_progress_ > SYNC_LOG_ARCHIVE_PROGRESS_INTERVAL;
    update_flag_for_confirm_clog = start_ts - last_check_time_for_confirm_clog_ > CONFIRM_CLOG_INTERVAL;

    update_flag_for_archive_checkpoint =
        start_ts - last_check_time_for_archive_checkpoint_ > CHECK_LOG_ARCHIVE_CHECKPOINT_INTERVAL;

    update_flag_for_check_restore_progress =
        start_ts - last_check_time_for_check_restore_progress_ > CLOG_CHECK_RESTORE_PROGRESS_INTERVAL;

    update_flag_for_clear_trans_after_restore =
        start_ts - last_check_time_for_check_restore_progress_ > CLEAR_TRANS_AFTER_RESTORE_INTERVAL;
    if (NULL == partition_service_) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "partition_service_ is not inited", K(ret));
    } else if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "alloc_scan_iter failed", K(ret));
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      while (!has_set_stop() && OB_SUCC(ret)) {
        if (OB_FAIL(partition_iter->get_next(partition))) {
          // do nothing
        } else if (NULL == partition) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "Unexpected error, partition is NULL", KR(ret));
        } else if (OB_SYS_TENANT_ID == partition->get_partition_key().get_tenant_id()) {
          // skip, sys
        } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
          // do nothing
        } else {
          int tmp_ret = OB_SUCCESS;
          bool is_backup_stopped = true;
          bool need_archive_checkpoint = false;
          bool is_backup_info_valid = false;

          const ObPartitionKey& pkey = partition->get_partition_key();
          const bool is_restore = partition->get_pg_storage().is_restore();
          ObLogArchiveBackupInfo info;
          if (OB_SUCCESS != (tmp_ret = ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
            if (OB_EAGAIN == tmp_ret) {
              if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
                CLOG_LOG(WARN, "failed to get_log_archive_backup_info", K(pkey), KR(tmp_ret));
              }
            } else {
              CLOG_LOG(WARN, "failed to get_log_archive_backup_info", K(pkey), KR(tmp_ret));
            }
          } else {
            is_backup_info_valid = true;
            is_backup_stopped = ObLogArchiveStatus::STATUS::STOP == info.status_.status_;
            need_archive_checkpoint = (ObLogArchiveStatus::STATUS::BEGINNING == info.status_.status_ ||
                                       ObLogArchiveStatus::STATUS::DOING == info.status_.status_);
          }

          if (!is_restore && update_flag_for_sync_log_archive_progress && is_backup_info_valid && !is_backup_stopped) {
            if (OB_SUCCESS != (tmp_ret = pls->sync_log_archive_progress())) {
              CLOG_LOG(WARN, "sync_log_archive_progress failed", K(pkey), K(tmp_ret));
            }
          }

          if (update_flag_for_confirm_clog) {
            if (OB_SUCCESS != (tmp_ret = pls->restore_leader_try_confirm_log())) {
              CLOG_LOG(WARN, "restore_leader_try_confirm_log failed", K(pkey), K(tmp_ret));
            }
          }

          if (!is_restore && update_flag_for_archive_checkpoint && is_backup_info_valid && need_archive_checkpoint) {
            const bool enable_log_archive = common::ObServerConfig::get_instance().enable_log_archive;
            if (enable_log_archive) {
              const int64_t archive_checkpoint_interval = GCONF.log_archive_checkpoint_interval;
              if (OB_SUCCESS != (tmp_ret = pls->archive_checkpoint(archive_checkpoint_interval))) {
                if (OB_ENTRY_NOT_EXIST == tmp_ret || OB_LOG_ARCHIVE_NOT_RUNNING == tmp_ret || OB_EAGAIN == ret) {
                  if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                    CLOG_LOG(WARN, "archive_checkpoint failed", K(pkey), K(archive_checkpoint_interval), K(tmp_ret));
                  }
                } else {
                  CLOG_LOG(WARN, "archive_checkpoint failed", K(pkey), K(archive_checkpoint_interval), K(tmp_ret));
                }
              }
            }
          }

          if (update_flag_for_check_restore_progress) {
            if (OB_SUCCESS != (tmp_ret = pls->check_and_set_restore_progress())) {
              CLOG_LOG(WARN, "failed to check_and_set_restore_progress", K(pkey), KR(tmp_ret));
            }
          }
        }
      }  // end of while
    }

    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }

    if (update_flag_for_sync_log_archive_progress) {
      last_check_time_for_sync_log_archive_progress_ = start_ts;
    }
    if (update_flag_for_confirm_clog) {
      last_check_time_for_confirm_clog_ = start_ts;
    }
    if (update_flag_for_archive_checkpoint) {
      last_check_time_for_archive_checkpoint_ = start_ts;
    }
    if (update_flag_for_check_restore_progress) {
      last_check_time_for_check_restore_progress_ = start_ts;
    }
    if (update_flag_for_check_restore_progress) {
      last_check_time_for_clear_trans_after_restore_ = start_ts;
    }

    int tmp_ret = OB_SUCCESS;
    if ((NULL != partition_service_) && OB_SUCCESS != (tmp_ret = partition_service_->try_advance_restoring_clog())) {
      CLOG_LOG(WARN, "ObLogArchiveAndRestoreDriver failed to try_advance_restoring_clog", K(cycle_num), K(tmp_ret));
    }

    const int64_t round_cost_time = ObTimeUtility::current_time() - start_ts;
    int32_t sleep_ts = ARCHVIE_AND_RESTORE_STATE_DRIVER_INTERVAL - static_cast<const int32_t>(round_cost_time);
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    usleep(sleep_ts);
    ++cycle_num;

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(INFO, "ObLogArchiveAndRestoreDriver round_cost_time", K(round_cost_time), K(sleep_ts), K(cycle_num));
    }
  }
}
}  // namespace clog
}  // namespace oceanbase
