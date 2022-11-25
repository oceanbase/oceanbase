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
  } else if (COMPARE_OUT(compare_log_ts_(ret, rhs_impl))) {// 避免切换至回放位点过小的副本
    (void) reason.assign("LOG TS");
    COORDINATOR_LOG_(TRACE, "compare done! get compared resultfrom log_ts_");
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

int PriorityV1::get_log_ts_(const share::ObLSID &ls_id, int64_t &log_ts)
{
  LC_TIME_GUARD(100_ms);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(ls_id), K(*this)
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard palf_handle_guard;
//  palf::AccessMode access_mode = palf::AccessMode::INVALID_ACCESS_MODE;
  if (OB_ISNULL(MTL(ObLogService*))) {
    COORDINATOR_LOG_(ERROR, "ObLogService is nullptr");
  } else if (CLICK_FAIL(MTL(ObLogService*)->open_palf(ls_id, palf_handle_guard))) {
    COORDINATOR_LOG_(WARN, "open_palf failed");
//  } else if (CLICK_FAIL(palf_handle_guard.get_palf_handle()->get_access_mode(access_mode))) {
//    COORDINATOR_LOG_(WARN, "get_access_mode failed");
//  } else if (palf::AccessMode::APPEND != access_mode) {
//    // Set log_ts to 0 when current access mode is not APPEND.
//    log_ts = 0;
  } else {
    common::ObRole role;
    int64_t unused_pid = -1;
    int64_t min_unreplay_log_ts_ns = OB_INVALID_TIMESTAMP;
    if (CLICK_FAIL(palf_handle_guard.get_role(role, unused_pid))) {
      COORDINATOR_LOG_(WARN, "get_role failed");
    } else if (CLICK_FAIL(palf_handle_guard.get_max_ts_ns(log_ts))) {
      COORDINATOR_LOG_(WARN, "get_max_ts_ns failed");
    } else if (FOLLOWER == role) {
      if (CLICK_FAIL(MTL(ObLogService*)->get_log_replay_service()->get_min_unreplayed_log_ts_ns(ls_id, min_unreplay_log_ts_ns))) {
        COORDINATOR_LOG_(WARN, "failed to get_min_unreplayed_log_ts_ns");
        ret = OB_SUCCESS;
      } else if (min_unreplay_log_ts_ns - 1 < log_ts) {
        // For restore case, min_unreplay_log_ts_ns may be larger than max_ts.
        log_ts = min_unreplay_log_ts_ns - 1;
      } else {}
    } else {}
    // log_ts may fallback because palf's role may be different with apply_service.
    // So we need check it here to keep inc update semantic.
    if (log_ts < log_ts_) {
      COORDINATOR_LOG_(TRACE, "new log_ts is smaller than current, no need update", K(role), K(min_unreplay_log_ts_ns), K(log_ts));
      log_ts = log_ts_;
    }
    COORDINATOR_LOG_(TRACE, "get_log_ts_ finished", K(role), K(min_unreplay_log_ts_ns), K(log_ts));
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
  int64_t log_ts = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(coordinator) || OB_ISNULL(detector)) {
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
  } else if (CLICK_FAIL(get_log_ts_(ls_id, log_ts))) {
    COORDINATOR_LOG_(WARN, "get_log_ts failed");
  } else {
    zone_priority_ = election_reference_info.element<1>();
    is_manual_leader_ = election_reference_info.element<2>();
    is_in_blacklist_ = election_reference_info.element<3>().element<0>();
    is_zone_stopped_ = election_reference_info.element<4>();
    is_server_stopped_ = election_reference_info.element<5>();
    is_primary_region_ = election_reference_info.element<6>();
    is_observer_stopped_ = (observer::ObServer::get_instance().is_stopped()
        || observer::ObServer::get_instance().is_prepare_stopped());
    log_ts_ = log_ts;
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

int PriorityV1::compare_log_ts_(int &ret, const PriorityV1&rhs) const
{
  int compare_result = 0;
  if (OB_SUCC(ret)) {
    if (std::max(log_ts_, rhs.log_ts_) - std::min(log_ts_, rhs.log_ts_) <= MAX_UNREPLAYED_LOG_TS_DIFF_THRESHOLD_NS) {
      compare_result = 0;
    } else if (std::max(log_ts_, rhs.log_ts_) == log_ts_) {
      compare_result = 1;
    } else {
      compare_result = -1;
    }
  }
  return compare_result;
}

}
}
}
