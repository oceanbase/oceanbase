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

#ifndef OCEANBASE_LOG_ARCHIVE_AND_RESOTRE_DRIVER_RUNNABLE_
#define OCEANBASE_LOG_ARCHIVE_AND_RESOTRE_DRIVER_RUNNABLE_

#include "share/ob_thread_pool.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObLogArchiveAndRestoreDriver : public share::ObThreadPool {
public:
  ObLogArchiveAndRestoreDriver();
  virtual ~ObLogArchiveAndRestoreDriver();

public:
  int init(storage::ObPartitionService* partition_service);
  void destroy();
  void run1();
  int64_t get_last_run_time() const
  {
    return last_run_time_;
  }

private:
  void state_driver_loop();
  const int64_t SYNC_LOG_ARCHIVE_PROGRESS_INTERVAL = 2 * 1000 * 1000LL;  // 2s
  const int64_t CONFIRM_CLOG_INTERVAL = 1 * 1000 * 1000LL;
  const int64_t CLOG_CHECK_RESTORE_PROGRESS_INTERVAL = 2L * 1000 * 1000LL;
  const int64_t CHECK_LOG_ARCHIVE_CHECKPOINT_INTERVAL = 1 * 1000 * 1000LL;  // 1s
  const int64_t CLEAR_TRANS_AFTER_RESTORE_INTERVAL = 5 * 1000 * 1000LL;     // 5s
  const int64_t ARCHVIE_AND_RESTORE_STATE_DRIVER_INTERVAL = 500 * 1000LL;   // 500ms
private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  int64_t last_check_time_for_sync_log_archive_progress_;
  int64_t last_check_time_for_confirm_clog_;
  int64_t last_check_time_for_archive_checkpoint_;
  int64_t last_check_time_for_check_restore_progress_;
  int64_t last_check_time_for_clear_trans_after_restore_;
  int64_t last_run_time_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveAndRestoreDriver);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_LOG_ARCHIVE_AND_RESOTRE_DRIVER_RUNNABLE_H_
