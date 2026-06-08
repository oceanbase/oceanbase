/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "rootserver/mview/ob_mview_timer_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewPendingTaskManager;

class ObMViewPendingTaskInspectionTask : public ObMViewTimerTask
{
public:
  ObMViewPendingTaskInspectionTask();
  virtual ~ObMViewPendingTaskInspectionTask();
  DISALLOW_COPY_AND_ASSIGN(ObMViewPendingTaskInspectionTask);

  int init(ObMViewPendingTaskManager &manager, uint64_t tenant_id);
  int start();
  int stop();
  int wait();
  int destroy();
  void runTimerTask() override; // must stay void: virtual override of ObTimerTask

  int register_for_recovery(uint64_t tenant_id, int64_t refresh_id, uint64_t mview_id,
                             uint64_t target_data_sync_scn);
  int register_for_retry(uint64_t tenant_id, int64_t refresh_id, uint64_t mview_id,
                         int64_t next_retry_ts);

private:
  struct RecoveryEntry {
    uint64_t tenant_id_;
    int64_t  refresh_id_;
    uint64_t mview_id_;
    uint64_t target_data_sync_scn_;
    TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(mview_id), K_(target_data_sync_scn));
  };

  struct RetryEntry {
    uint64_t tenant_id_;
    int64_t  refresh_id_;
    uint64_t mview_id_;
    int64_t  next_retry_ts_;
    TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(mview_id), K_(next_retry_ts));
  };

  void clear_lists();

  int process_recovery_list();
  int drain_recovery_list(common::ObIArray<RecoveryEntry> &snapshot);
  int recover_single_entry(const RecoveryEntry &entry,
                           common::ObIArray<RecoveryEntry> &requeue);
  int requeue_recovery_entries(const common::ObIArray<RecoveryEntry> &requeue);

  int process_retry_list();
  int drain_retry_list(common::ObIArray<RetryEntry> &snapshot);
  int process_single_retry_entry(const RetryEntry &entry, int64_t now,
                                 common::ObIArray<RetryEntry> &remaining,
                                 bool &need_wakeup);
  int restore_retry_list(const common::ObIArray<RetryEntry> &remaining);

private:
  static const int64_t INSPECTION_INTERVAL = 1LL * 1000 * 1000; // 1s
  static const int64_t RECOVERY_TICK_RATIO = 10; // process_recovery_list runs every 10 ticks (~10s)
  static const int64_t CLEANUP_DROP_BLOCK_RATIO = 60; // 60s
  ObMViewPendingTaskManager *manager_;
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  int64_t tick_count_;

  // Protects both recovery_list_ and retry_list_.
  common::ObSpinLock list_lock_;
  common::ObArray<RecoveryEntry> recovery_list_;
  common::ObArray<RetryEntry>    retry_list_;
};

} // namespace rootserver
} // namespace oceanbase
