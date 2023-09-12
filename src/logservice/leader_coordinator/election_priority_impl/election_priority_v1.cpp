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

#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_log_service.h"
#include "share/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "election_priority_impl.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "logservice/leader_coordinator/ob_leader_coordinator.h"
#include "logservice/leader_coordinator/common_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_table_access_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "logservice/palf/lsn.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace share;
namespace logservice
{
namespace coordinator
{

#define COMPARE_OUT(stmt) (0 != (result = stmt))

int PriorityV1::compare(const AbstractPriority &rhs, int &result, ObStringHolder &reason) const
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(*this), K(rhs), K(result), K(reason)
  int ret = OB_SUCCESS;
  // 这里如果转型失败直接抛异常，但设计上转型不会失败
  const PriorityV1 &rhs_impl = dynamic_cast<const PriorityV1 &>(rhs);
  if (COMPARE_OUT(compare_observer_stopped_(ret, rhs_impl))) {// kill -15 导致observer stop
    (void) reason.assign("OBSERVER STOP");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from is_observer_stopped_");
  } else if (COMPARE_OUT(compare_server_stopped_flag_(ret, rhs_impl))) {// 比较server是否被stop
    (void) reason.assign("SERVER STOPPED");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from server_stopped_flag_");
  } else if (COMPARE_OUT(compare_zone_stopped_flag_(ret, rhs_impl))) {// 比较zone是否被stop
    (void) reason.assign("ZONE STOPPED");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from zone_stopped_flag_");
  } else if (COMPARE_OUT(compare_fatal_failures_(ret, rhs_impl))) {// 比较致命的异常
    (void) reason.assign("FATAL FAILURE");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from fatal_failures_");
  } else if (COMPARE_OUT(compare_scn_(ret, rhs_impl))) {// 避免切换至回放位点过小的副本
    (void) reason.assign("LOG TS");
    COORDINATOR_LOG_(TRACE, "compare done! get compared resultfrom scn_");
  } else if (COMPARE_OUT(compare_in_blacklist_flag_(ret, rhs_impl, reason))) {// 比较是否被标记删除
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from in_blacklist_flag_");
  } else if (COMPARE_OUT(compare_manual_leader_flag_(ret, rhs_impl))) {// 比较是否存在用户指定的leader
    (void) reason.assign("MANUAL LEADER");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from manual_leader_flag_");
  } else if (COMPARE_OUT(compare_primary_region_(ret, rhs_impl))) {// 通常Leader不能选出primary region
    (void) reason.assign("PRIMARY REGION");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from primary_region_");
  } else if (COMPARE_OUT(compare_serious_failures_(ret, rhs_impl))) {// 比较会导致切主的异常
    (void) reason.assign("SERIOUS FAILURE");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from serious_failures_");
  } else if (COMPARE_OUT(compare_zone_priority_(ret, rhs_impl))) {// 比较RS设置的zone priority
    (void) reason.assign("ZONE PRIORITY");
    COORDINATOR_LOG_(TRACE, "compare done! get compared result from zone_priority_");
  } else if (CLICK_FAIL(ret)) {
    COORDINATOR_LOG(ERROR, "error occure when compare priority");
  }
  COORDINATOR_LOG(DEBUG, "debug", K(*this), K(rhs), KR(ret), K(MTL_ID()));
  return ret;
  #undef PRINT_WRAPPER
}

//                 |           Leader             | Follower
// ----------------|------------------------------|-----------------
//  APPEND         |           max_scn            | max_replayed_scn
// ----------------|------------------------------|-----------------
// RAW_WRITE v4.1  | min(replayable_scn, max_scn) | max_replayed_scn
// ----------------|------------------------------|-----------------
// RAW_WRITE v4.2  |         SCN::max_scn         |  SCN::max_scn
// ----------------|------------------------------|-----------------
// OTHER           |          like RAW_WRITE
int PriorityV1::get_scn_(const share::ObLSID &ls_id, SCN &scn)
{
  LC_TIME_GUARD(100_ms);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(ls_id), K(*this)
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
  palf::AccessMode access_mode = palf::AccessMode::INVALID_ACCESS_MODE;
  ObLogService* log_service = MTL(ObLogService*);
  common::ObRole role = FOLLOWER;
  int64_t unused_pid = -1;
  const bool is_cluster_already_4200 = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0;
  if (OB_ISNULL(log_service)) {
    COORDINATOR_LOG_(ERROR, "ObLogService is nullptr");
  } else if (CLICK_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
    COORDINATOR_LOG_(WARN, "open_palf failed");
  } else if (CLICK_FAIL(palf_handle_guard.get_palf_handle()->get_access_mode(access_mode))) {
    COORDINATOR_LOG_(WARN, "get_access_mode failed");
  } else if (false == is_cluster_already_4200) {
    // Note: A(PZ, 4.1), B(4.1), and C(4.2).
    // C's scn is SCN::MAX_SCN, and A's scn is meaningful scn, therefore, A will change
    // leadership to C; After the leadership has been transfered to C, C regards the SCN::MAX_SCN
    // and a meaningful scn as the same value, further, A is in the primary zone, therefore, C
    // will transfer leadership to A back. The leadership will be switched between A and
    // C repetitively.
    // Solution: we activate new get_scn logic after a physical standby tenant has been upgraded
    // to v4.1. A risk is that if the MIN_CLSUTER_VERSIONs of (A,B,C) are (4.1, 4.1, 4.2) within
    // a small time window, unreasonable leadership transfer may occur, we think it is tolerable.
    SCN replayable_scn;
    SCN max_scn;
    if (CLICK_FAIL(palf_handle_guard.get_role(role, unused_pid))) {
      COORDINATOR_LOG_(WARN, "get_role failed");
    } else if (FOLLOWER == role) {
      if (CLICK_FAIL(log_service->get_log_replay_service()->get_max_replayed_scn(ls_id, scn))) {
        COORDINATOR_LOG_(WARN, "failed to get_max_replayed_scn");
        ret = OB_SUCCESS;
      }
    } else if (palf::AccessMode::APPEND == access_mode) {
      if (CLICK_FAIL(palf_handle_guard.get_max_scn(scn))) {
        COORDINATOR_LOG_(WARN, "get_max_scn failed");
      }
    } else if (CLICK_FAIL(log_service->get_log_replay_service()->get_replayable_point(replayable_scn))) {
      COORDINATOR_LOG_(WARN, "failed to get_replayable_point");
      ret = OB_SUCCESS;
    } else if (CLICK_FAIL(palf_handle_guard.get_max_scn(max_scn))) {
      COORDINATOR_LOG_(WARN, "get_max_scn failed");
    } else {
      // For LEADER in RAW_WRITE mode, scn = min(replayable_scn, max_scn)
      if (max_scn < replayable_scn) {
        scn = max_scn;
      } else {
        scn = replayable_scn;
      }
    }
  } else if (palf::AccessMode::APPEND != access_mode) {
    // Note: set scn to max_scn when access mode is not APPEND.
    // 1. A possible risk is when LS is switched from RAW_WRITE to APPEND, the leader
    // may be APPEND and followers may be RAW_WRITE within a very short time window,
    // the leader's scn (log sync max_scn) may be smaller than followers' scns (SCN::max_scn).
    // To avoid the problem, if scn of a election priority is max_scn, we think the
    // priority is equivalent to any priorities whose scn is any values.
    // 2. Do not set scn to min_scn, if a follower do not replay any logs, its replayed_scn
    // may be SCN::min_scn, and the leadership may be switched to the follower.
    scn = SCN::max_scn();
  } else if (CLICK_FAIL(get_role_(ls_id, role))) {
    COORDINATOR_LOG_(WARN, "get_role failed");
  } else if (LEADER != role) {
    if (CLICK_FAIL(log_service->get_log_replay_service()->get_max_replayed_scn(ls_id, scn))) {
      COORDINATOR_LOG_(WARN, "failed to get_max_replayed_scn");
      ret = OB_SUCCESS;
    }
  } else if (CLICK_FAIL(palf_handle_guard.get_max_scn(scn))) {
    COORDINATOR_LOG_(WARN, "get_max_scn failed");
  }
  // scn may fallback because palf's role may be different with apply_service.
  // So we need check it here to keep inc update semantic.
  // Note: scn is always max_scn when access mode is not APPEND, so we just
  // keep its inc update semantic when cached scn_ is not SCN::max_scn
  if (scn < scn_ && SCN::max_scn() != scn_) {
    COORDINATOR_LOG_(TRACE, "new scn is smaller than current, no need update", K(role), K(access_mode), K(scn));
    scn = scn_;
  }
  COORDINATOR_LOG_(TRACE, "get_scn_ finished", K(role), K(access_mode), K(scn));
  if (OB_SUCC(ret) && !scn.is_valid()) {
    scn.set_min();
  }
  return ret;
  #undef PRINT_WRAPPER
}

int PriorityV1::get_role_(const share::ObLSID &ls_id, common::ObRole &role) const
{
  LC_TIME_GUARD(100_ms);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(ls_id), K(*this)
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService* ls_srv = MTL(ObLSService*);
  role = FOLLOWER;

  if (OB_ISNULL(ls_srv)) {
    COORDINATOR_LOG_(ERROR, "ObLSService is nullptr");
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::LOG_MOD))) {
    COORDINATOR_LOG_(WARN, "get_ls failed", K(ls_id));
  } else if (OB_UNLIKELY(false == ls_handle.is_valid())) {
    COORDINATOR_LOG_(WARN, "ls_handler is invalid", K(ls_id), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_log_handler()->get_role_atomically(role))) {
    COORDINATOR_LOG_(WARN, "get_role_atomically failed", K(ls_id));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int PriorityV1::refresh_(const share::ObLSID &ls_id)
{
  LC_TIME_GUARD(100_ms);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(*this)
  int ret = OB_SUCCESS;
  ObLeaderCoordinator* coordinator = MTL(ObLeaderCoordinator*);
  ObFailureDetector* detector = MTL(ObFailureDetector*);
  LsElectionReferenceInfo election_reference_info;
  SCN scn = SCN::min_scn();
  if (observer::ObServer::get_instance().is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    ret = OB_NO_NEED_UPDATE;
#endif
  } else if (OB_ISNULL(coordinator) || OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(ERROR, "unexpected nullptr");
  } else if (CLICK_FAIL(detector->get_specified_level_event(FailureLevel::FATAL, fatal_failures_))) {
    COORDINATOR_LOG_(WARN, "get fatal failures failed");
  } else if (CLICK_FAIL(detector->get_specified_level_event(FailureLevel::SERIOUS, serious_failures_))) {
    COORDINATOR_LOG_(WARN, "get serious failures failed");
  } else if (CLICK_FAIL(coordinator->get_ls_election_reference_info(ls_id, election_reference_info))) {
    COORDINATOR_LOG_(WARN, "fail to get ls election reference info");
  } else if (CLICK_FAIL(in_blacklist_reason_.assign(election_reference_info.element<3>().element<1>()))) {
    COORDINATOR_LOG_(WARN, "fail to copy removed reason string");
  } else if (CLICK_FAIL(get_scn_(ls_id, scn))) {
    COORDINATOR_LOG_(WARN, "get_scn failed");
  } else {
    zone_priority_ = election_reference_info.element<1>();
    is_manual_leader_ = election_reference_info.element<2>();
    is_in_blacklist_ = election_reference_info.element<3>().element<0>();
    is_zone_stopped_ = election_reference_info.element<4>();
    is_server_stopped_ = election_reference_info.element<5>();
    is_primary_region_ = election_reference_info.element<6>();
    is_observer_stopped_ = (observer::ObServer::get_instance().is_stopped()
        || observer::ObServer::get_instance().is_prepare_stopped());
    scn_ = scn;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int PriorityV1::compare_fatal_failures_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (fatal_failures_.count() == rhs.fatal_failures_.count()) {
      compare_result = 0;
    } else if (fatal_failures_.count() < rhs.fatal_failures_.count()) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_serious_failures_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (serious_failures_.count() == rhs.serious_failures_.count()) {
      compare_result = 0;
    } else if (serious_failures_.count() < rhs.serious_failures_.count()) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_in_blacklist_flag_(int &ret, const PriorityV1&rhs, ObStringHolder &reason) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    char remove_reason[64] = { 0 };
    int64_t pos = 0;
    if (is_in_blacklist_ == rhs.is_in_blacklist_) {
      compare_result = 0;
    } else if (!is_in_blacklist_ && rhs.is_in_blacklist_) {
      if (CLICK_FAIL(databuff_printf(remove_reason, 64, pos, "IN BLACKLIST(%s)", to_cstring(rhs.in_blacklist_reason_)))) {
        COORDINATOR_LOG(WARN, "data buf printf failed");
      } else if (CLICK_FAIL(reason.assign(remove_reason))) {
        COORDINATOR_LOG(WARN, "assign reason failed");
      }
      compare_result = 1;
    } else {
      if (CLICK_FAIL(databuff_printf(remove_reason, 64, pos, "IN BLACKLIST(%s)", to_cstring(in_blacklist_reason_)))) {
        COORDINATOR_LOG(WARN, "data buf printf failed");
      } else if (CLICK_FAIL(reason.assign(remove_reason))) {
        COORDINATOR_LOG(WARN, "assign reason failed");
      }
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_server_stopped_flag_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (is_server_stopped_ == rhs.is_server_stopped_) {
      compare_result = 0;
    } else if (!is_server_stopped_ && rhs.is_server_stopped_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_zone_stopped_flag_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (is_zone_stopped_ == rhs.is_zone_stopped_) {
      compare_result = 0;
    } else if (!is_zone_stopped_ && rhs.is_zone_stopped_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_manual_leader_flag_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (is_manual_leader_ == rhs.is_manual_leader_) {
      compare_result = 0;
    } else if (is_manual_leader_ && !rhs.is_manual_leader_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_zone_priority_(int &ret, const PriorityV1&rhs) const
{
  LC_TIME_GUARD(1_s);
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (zone_priority_ == rhs.zone_priority_) {
      compare_result = 0;
    } else if (zone_priority_ < rhs.zone_priority_) {// CAUTION: smaller zone_priority means higher election priority
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_observer_stopped_(int &ret, const PriorityV1&rhs) const
{
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (is_observer_stopped_ == rhs.is_observer_stopped_) {
      compare_result = 0;
    } else if (!is_observer_stopped_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_primary_region_(int &ret, const PriorityV1&rhs) const
{
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (is_primary_region_ == rhs.is_primary_region_) {
      compare_result = 0;
    } else if (is_primary_region_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

int PriorityV1::compare_scn_(int &ret, const PriorityV1&rhs) const
{
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    // If scn of a election priority is max_scn, we think the priority is
    // equivalent to any priorities whose scn is any values.
    // See detailed reason in PriorityV1::get_scn_
    if (scn_ == rhs.scn_ || scn_.is_max() || rhs.scn_.is_max()) {
      compare_result = 0;
    } else if (scn_.is_valid() && rhs.scn_.is_valid()) {
      if (std::max(scn_, rhs.scn_).convert_to_ts() - std::min(scn_, rhs.scn_).convert_to_ts() <= MAX_UNREPLAYED_LOG_TS_DIFF_THRESHOLD_US) {
        compare_result = 0;
      } else if (std::max(scn_, rhs.scn_) == scn_) {
        compare_result = 1;
      } else {
        compare_result = -1;
      }
    } else if (scn_.is_valid() && (!rhs.scn_.is_valid())) {
      compare_result = 1;
    } else if ((!scn_.is_valid()) && rhs.scn_.is_valid()) {
      compare_result = -1;
    }
  }
  return compare_result;
}

bool PriorityV1::has_fatal_failure_() const
{
  return !fatal_failures_.empty();
}

}
}
}
