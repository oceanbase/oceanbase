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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_DETECTOR_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_DETECTOR_

#include "rootserver/freeze/ob_freeze_reentrant_thread.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace rootserver
{
class ObMajorMergeInfoManager;
class ObThreadIdling;

class ObMajorMergeInfoDetector : public ObFreezeReentrantThread
{
public:
  ObMajorMergeInfoDetector(const uint64_t tenant_id);
  virtual ~ObMajorMergeInfoDetector() {}
  int init(const bool is_primary_service,
           common::ObMySQLProxy &sql_proxy,
           ObMajorMergeInfoManager &major_merge_info_mgr,
           ObThreadIdling &major_scheduler_idling);

  virtual void run3() override;
  virtual int blocking_run() override { BLOCKING_RUN_IMPLEMENT(); }
  virtual int64_t get_schedule_interval() const override;

  virtual int start() override;

  int signal();

private:
  int check_need_broadcast(bool &need_broadcast, const int64_t expected_epoch);
  int try_broadcast_freeze_info(const int64_t expected_epoch);
  int try_renew_snapshot_gc_scn();
  int try_minor_freeze();
  int try_update_zone_info(const int64_t expected_epoch);

  int can_start_work(bool &can_work);
  bool is_primary_service() { return is_primary_service_; }
  int check_tenant_is_restore(const uint64_t tenant_id, bool &is_restore);
  int try_reload_freeze_info(const int64_t expected_epoch);
  // adjust global_merge_info in memory to avoid useless major freezes on restore major_freeze_service
  int try_adjust_global_merge_info(const int64_t expected_epoch);
  int check_global_merge_info(bool &is_initial) const;
  // For backup-restore tenant that switchover to primary tenant, FreezeInfoDetector is not able to
  // has write access immediately when it starts. Thus, FreezeInfoDetector can not renew
  // snapshot_gc_scn immediately. Therefore, let FreezeInfoDetector to check snapshot_gc_scn
  // after it has started for a period of time (e.g., 10 min).
  //
  bool need_check_snapshot_gc_scn(const int64_t start_time_us);

private:
  static const int64_t FREEZE_INFO_DETECTOR_THREAD_CNT = 1;
  static const int64_t UPDATER_INTERVAL_US = 10 * 1000 * 1000; // 10s

  bool is_inited_;
  bool is_primary_service_;  // identify ObMajorFreezeServiceType::SERVICE_TYPE_PRIMARY
  bool is_global_merge_info_adjusted_;
  bool is_gc_scn_inited_;
  int64_t last_gc_timestamp_;
  ObMajorMergeInfoManager *major_merge_info_mgr_;
  ObThreadIdling *major_scheduler_idling_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeInfoDetector);
};

}
}
#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_DETECTOR_
