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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MERGE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MERGE_SCHEDULER_H_

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_mutex.h"

#include "share/ob_zone_info.h"
#include "share/ob_zone_merge_info.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/freeze/ob_tenant_all_zone_merge_strategy.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_checksum_validator.h"
#include "rootserver/freeze/ob_freeze_reentrant_thread.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace common
{
class ObServerConfig;
};

namespace rootserver
{
class ObZoneMergeManager;
class ObFreezeInfoManager;
class ObTenantMajorMergeStrategy;

class ObMergeErrorCallback
{
public:
  ObMergeErrorCallback() 
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), 
      zone_merge_mgr_(nullptr)
  {}
  virtual ~ObMergeErrorCallback() {}

  int init(const uint64_t tenant_id, ObZoneMergeManager &zone_merge_mgr);

  int handle_merge_error(const int64_t error_type, const int64_t expected_epoch);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObZoneMergeManager *zone_merge_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeErrorCallback);
};

class ObFullChecksumValidator
{
public:
  ObFullChecksumValidator() 
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), last_check_time_(0),
      tablet_validator_(), cross_cluster_validator_(), index_validator_(), 
      merge_err_cb_()
  {}
  virtual ~ObFullChecksumValidator() {}

  int init(const uint64_t tenant_id,
           common::ObMySQLProxy &sql_proxy,
           ObZoneMergeManager &zone_merge_mgr);

  int execute_check(const volatile bool &stop,
                    const share::ObSimpleFrozenStatus &frozen_status,
                    const int64_t expected_epoch);

  // sync tablet checksum data from __all_tablet_replica_checksum to __all_tablet_checksum
  int sync_tablet_checksum();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t last_check_time_;
  ObTabletChecksumValidator tablet_validator_;
  ObCrossClusterTableteChecksumValidator cross_cluster_validator_;
  ObIndexChecksumValidator index_validator_;
  ObMergeErrorCallback merge_err_cb_;
};

class ObMajorMergeIdling : public ObThreadIdling
{
public:
  explicit ObMajorMergeIdling(volatile bool &stop) 
    : ObThreadIdling(stop), tenant_id_(OB_INVALID_TENANT_ID) {}
  int init(const uint64_t tenant_id);
  virtual int64_t get_idle_interval_us() override;

public:
  const static int64_t DEFAULT_SCHEDULE_IDLE_US = 10 * 60 * 1000L * 1000L; // 10m

private:
  uint64_t tenant_id_;
};

class ObMajorMergeScheduler : public ObFreezeReentrantThread
{
public:
  ObMajorMergeScheduler();
  virtual ~ObMajorMergeScheduler() {}

  int init(const uint64_t tenant_id,
           ObZoneMergeManager &zone_merge_mgr,
           ObFreezeInfoManager &freeze_info_mgr,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObIServerTrace &server_trace,
           common::ObServerConfig &config, 
           common::ObMySQLProxy &sql_proxy);

  virtual int start() override;
  virtual void run3() override;

  virtual int blocking_run() override { BLOCKING_RUN_IMPLEMENT(); }

  ObMajorMergeIdling &get_major_scheduler_idling() { return idling_; }

  int try_update_epoch_and_reload();

protected:
  virtual int try_idle(const int64_t ori_idle_time_us, const int work_ret) override;

private:
  int do_work();

  int do_one_round_major_merge(const int64_t expected_epoch);

  int generate_next_global_broadcast_scn(const int64_t expected_epoch);
  int get_next_merge_zones(share::ObZoneArray &to_merge);
  int schedule_zones_to_merge(const share::ObZoneArray &to_merge, const int64_t expected_epoch);
  int start_zones_merge(const share::ObZoneArray &to_merge, const int64_t expected_epoch);
  int set_zone_merging(const ObZone &zone, const int64_t expected_epoch);

  int update_merge_status(const int64_t expected_epoch);
  int try_update_global_merged_scn(const int64_t expected_epoch);
  int update_global_merge_info_after_merge(const int64_t expected_epoch);

  int do_update_freeze_service_epoch(const int64_t latest_epoch);
  int update_epoch_in_memory_and_reload();
  int get_epoch_with_retry(int64_t &freeze_service_epoch);
  int do_update_and_reload(const int64_t epoch);

  void check_merge_interval_time(const bool is_merging);

private:
  const static int64_t DEFAULT_IDLE_US = 10 * 1000L * 1000L; // 10s
  static const int64_t MAJOR_MERGE_SCHEDULER_THREAD_CNT = 1;

  bool is_inited_;
  int64_t fail_count_;
  int64_t first_check_merge_us_;

  mutable lib::ObMutex epoch_update_lock_;
  mutable ObMajorMergeIdling idling_;

  ObZoneMergeManager *zone_merge_mgr_;
  ObFreezeInfoManager *freeze_info_mgr_;
  common::ObServerConfig *config_;
  ObTenantAllZoneMergeStrategy merge_strategy_;
  common::ObMySQLProxy *sql_proxy_;
  ObMajorMergeProgressChecker progress_checker_;
  ObFullChecksumValidator checksum_validator_;

  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeScheduler);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MERGE_SCHEDULER_H_
