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

#ifndef LOGSERVICE_COORDINATOR_INTERFACE_OB_LEADER_COORDINATOR_H
#define LOGSERVICE_COORDINATOR_INTERFACE_OB_LEADER_COORDINATOR_H
#include "failure_event.h"
#include "lib/function/ob_function.h"
#include "lib/container/ob_array.h"
#include "share/ob_delegate.h"
#include "ob_failure_detector.h"
#include "share/ob_occam_timer.h"
#include "share/ob_table_access_helper.h"

namespace oceanbase
{
namespace unittest
{
class TestTabletAccessor;
class TestElectionPriority;
}
namespace logservice
{
namespace coordinator
{

// 该结构存储的信息是周期性刷新的, 非本机的优先级信息缓存在该结构里
typedef common::ObTuple<int64_t/*0. ls_id*/,
                        int64_t/*1. self zone priority*/,
                        bool/*2. is_manual_leader*/,
                        ObTuple<bool/*3.1 is_removed*/,
                                common::ObStringHolder/*3.2 removed_reason*/>,
                        bool/*4. is_zone_stopped*/,
                        bool/*5. is_server_stopped*/,
                        bool/*6. is_primary_region*/> LsElectionReferenceInfo;

class ObLeaderCoordinator
{
  friend class ObFailureDetector;
  friend class unittest::TestTabletAccessor;
  friend class unittest::TestElectionPriority;
public:
  ObLeaderCoordinator();
  ~ObLeaderCoordinator();
  void destroy();
  ObLeaderCoordinator(const ObLeaderCoordinator &rhs) = delete;
  ObLeaderCoordinator& operator=(const ObLeaderCoordinator &rhs) = delete;
  static int mtl_init(ObLeaderCoordinator *&p_coordinator);
  static int mtl_start(ObLeaderCoordinator *&p_coordinator);
  static void mtl_stop(ObLeaderCoordinator *&p_coordinator);
  static void mtl_wait(ObLeaderCoordinator *&p_coordinator);
  static void mtl_destroy(ObLeaderCoordinator *&p_coordinator);
  /**
   * @description: 当内部表更新的时候，可以通过该接口主动触发LeaderCoordinator的刷新流程，以便切主动作可以尽快完成
   * @param {*}
   * @return {*}
   * @Date: 2021-12-27 20:30:39
   */
  void refresh();
  int get_ls_election_reference_info(const share::ObLSID &ls_id, LsElectionReferenceInfo &reference_info) const;
  int schedule_refresh_priority_task();
private:
  common::ObArray<LsElectionReferenceInfo> *all_ls_election_reference_info_;
  // refresh priority and detect recovery from failure
  common::ObOccamTimer recovery_detect_timer_;
  // detect whether failure has occured
  common::ObOccamTimer failure_detect_timer_;
  common::ObOccamTimerTaskRAIIHandle refresh_priority_task_handle_;
  bool is_running_;
  mutable ObSpinLock lock_;
};

}
}
}

#endif