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

#include "lib/list/ob_dlist.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_occam_time_guard.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_time_fmt.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ls_id.h"
#include "share/ob_occam_timer.h"
#include "common/ob_clock_generator.h"
#include "util/easy_time.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "election_event_recorder.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

static constexpr int INFO_MAX_LEN = 128;
#define DO_IF_SUCC(stmt) if (OB_SUCC(ret) && CLICK_FAIL(stmt)) {}

int64_t EventRecorder::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "{ls_id:{id:%ld}, ", ls_id_);
  databuff_printf(buf, len, pos, "self_addr:%s}", to_cstring(self_addr_));
  return pos;
}

int EventRecorder::report_event_(ElectionEventType type, const common::ObString &info)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  int retry_times = 0;
  int64_t report_ts = ObClockGenerator::getRealClock();
  int64_t mtl_id = MTL_ID();
  int64_t ls_id = ls_id_;
  ObAddr self_addr = self_addr_;
  ObUniqueGuard<ObStringHolder> uniq_holder;
  #define PRINT_WRAPPER KR(ret), K(*this), K(type), K(info)
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "EventReHolder");
  SET_USE_500(attr);
  if (CLICK_FAIL(ob_make_unique(uniq_holder, attr))) {
    LOG_EVENT(WARN, "fail to make unique guard");
  } else if (CLICK_FAIL(uniq_holder->assign(info))) {
    LOG_EVENT(WARN, "fail to create unique ownership of string");
  #undef PRINT_WRAPPER
  #define PRINT_WRAPPER KR(ret), K(retry_times), K(report_ts), K(self_addr), K(mtl_id), K(obj_to_string(type)), K(ls_id), KPC(uniq_holder.get_ptr())
  } else if (!need_report_) {
    LOG_EVENT(INFO, "event happened, but no need do report");
  } else if (CLICK_FAIL(GLOBAL_REPORT_TIMER.schedule_task_ignore_handle_repeat_and_immediately(15_s, [retry_times, report_ts, self_addr, mtl_id, type, ls_id, uniq_holder]() mutable -> bool {
    ELECT_TIME_GUARD(500_ms);
    int ret = OB_SUCCESS;
    bool stop_flag = false;
    char ip[64] = {0};
    static const char *columns[12] = {"svr_ip", "svr_port", "module", "event", "name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4"};
    if (!self_addr.ip_to_string(ip, sizeof(ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EVENT(WARN, "ip to string failed");
    } else if (CLICK_FAIL(SERVER_EVENT_SYNC_ADD("ELECTION",
                                                obj_to_string(type),
                                                "TENANT_ID",
                                                mtl_id,
                                                "LS_ID",
                                                ls_id,
                                                 "INFO",
                                                to_cstring(uniq_holder->get_ob_string()),
                                                "HAPPENED_TIME",
                                                ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(report_ts)))) {
      LOG_EVENT(WARN, "fail to insert row");
    } else {
      LOG_EVENT(INFO, "insert history row success");
    }
    if (++retry_times > 12 || OB_SUCC(ret)) {
      if (retry_times > 12) {// this may happened cause inner sql may not work for a long time, but i have tried my very best, so let it miss.
        LOG_EVENT_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fail to schedule report event task cause retry too much times");
      }
      stop_flag = true;
    }
    return stop_flag;
  }))) {
    LOG_EVENT(ERROR, "fail to schedule report event task");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_vote_event(const ObAddr &dest_svr, const ObStringHolder &reason) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  if (CLICK_FAIL(databuff_printf(info, INFO_MAX_LEN, pos, "vote for %s, reason:%s", to_cstring(dest_svr), to_cstring(reason)))) {
    LOG_EVENT(WARN, "fail to format string");
  } else if (CLICK_FAIL(report_event_(ElectionEventType::VOTE, info))) {
    LOG_EVENT(WARN, "report vote event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_decentralized_to_be_leader_event(const MemberListWithStates &member_list_with_states) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  bool last_print_flag = false;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "server|lease:["));
  for (int idx = 0; idx < member_list_with_states.get_member_list().get_addr_list().count(); ++idx) {
    if (member_list_with_states.p_impl_->accept_ok_promise_not_vote_before_local_ts_[idx] > 0) {
      int64_t lease_end_time = TimeSpanWrapper(member_list_with_states.p_impl_->accept_ok_promise_not_vote_before_local_ts_[idx]).get_current_ts_likely();
      if (!last_print_flag) {
        DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "%s|%s",
                                    to_cstring(member_list_with_states.p_impl_->member_list_.get_addr_list()[idx]),
                                    ObTime2Str::ob_timestamp_str_range<YEAR, MSECOND>(lease_end_time)));
        last_print_flag = true;
      } else {
        DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, ",%s|%s",
                                    to_cstring(member_list_with_states.p_impl_->member_list_.get_addr_list()[idx]),
                                    ObTime2Str::ob_timestamp_str_range<YEAR, MSECOND>(lease_end_time)));
      }
    }
  }
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "]"));
  DO_IF_SUCC(report_event_(ElectionEventType::DECENTRALIZED_TO_BE_LEADER, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report DECENTRALIZED_TO_BE_LEADER event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_directly_change_leader_event(const ObAddr &dest_svr, const ObStringHolder &reason) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "directly change leader : %s -> %s, reason : %s", to_cstring(self_addr_), to_cstring(dest_svr), to_cstring(reason)));
  DO_IF_SUCC(report_event_(ElectionEventType::DIRECTLY_CHANGE_LEADER, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report DIRECTLY_CHANGE_LEADER event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_prepare_change_leader_event(const ObAddr &dest_svr, const ObStringHolder &reason) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "leader prepare to change leader : %s -> %s, reason : %s", to_cstring(self_addr_), to_cstring(dest_svr), to_cstring(reason)));
  DO_IF_SUCC(report_event_(ElectionEventType::PREPARE_CHANGE_LEADER, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report PREPARE_CHANGE_LEADER event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_change_leader_to_revoke_event(const ObAddr &dest_svr) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "old leader revoke : %s -> %s", to_cstring(self_addr_), to_cstring(dest_svr)));
  DO_IF_SUCC(report_event_(ElectionEventType::CHANGE_LEADER_TO_REVOKE, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report CHANGE_LEADER_TO_REVOKE event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_change_leader_to_takeover_event(const ObAddr &addr) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "new leader takeover : %s -> %s", to_cstring(addr), to_cstring(self_addr_)));
  DO_IF_SUCC(report_event_(ElectionEventType::CHANGE_LEADER_TO_TAKEOVER, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report CHANGE_LEADER_TO_TAKEOVER event failed", KR(ret), K(*this), K(info));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_leader_lease_expired_event(const MemberListWithStates &member_list_with_states) {
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(member_list_with_states)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "leader lease expired, server|lease:["));
  bool last_print_flag = false;
  for (int idx = 0; idx < member_list_with_states.get_member_list().get_addr_list().count(); ++idx) {
    int64_t lease_end_time = TimeSpanWrapper(member_list_with_states.p_impl_->accept_ok_promise_not_vote_before_local_ts_[idx]).get_current_ts_likely();
    if (!last_print_flag) {
      DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "%s",
                                 ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(lease_end_time)));
      last_print_flag = true;
    } else {
      DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "|%s",
                                 ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(lease_end_time)));
    }
  }
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "]"));
  DO_IF_SUCC(report_event_(ElectionEventType::LEASE_EXPIRED_TO_REVOKE, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report LEASE_EXPIRED_TO_REVOKE event failed", KR(ret), K(*this), K(info));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_member_list_changed_event(const MemberList &old_list, const MemberList &new_list)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(info), K(old_list), K(new_list)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  bool last_print_flag = false;
  if (old_list.only_membership_version_different(new_list)) {
    LOG_EVENT(ERROR, "only membership version different");
  } else {
    DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "new : "));
    last_print_flag = false;
    for (int64_t idx = 0; idx < new_list.get_addr_list().count(); ++idx) {
      if (!last_print_flag) {
        DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "%s", to_cstring(new_list.get_addr_list()[idx])));
        last_print_flag = true;
      } else {
        DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, ", %s", to_cstring(new_list.get_addr_list()[idx])));
      }
    }
    DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, ", %s", to_cstring(new_list.get_membership_version())));
    DO_IF_SUCC(report_event_(ElectionEventType::CHANGE_MEMBERLIST, info));
    if (CLICK_FAIL(ret)) {
      LOG_EVENT(WARN, "report CHANGE_MEMBERLIST event failed");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_acceptor_lease_expired_event(const Lease &lease)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(lease)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  bool last_print_flag = false;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "owner:%s, expired time:%s, ballot number:%ld",
                             to_cstring(lease.get_owner()),
                             common::ObTime2Str::ob_timestamp_str_range<HOUR, USECOND>(TimeSpanWrapper(lease.get_lease_end_ts()).get_current_ts_likely()),
                             lease.get_ballot_number()));
  DO_IF_SUCC(report_event_(ElectionEventType::LEASE_EXPIRED, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report LEASE_EXPIRED event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int EventRecorder::report_acceptor_witness_change_leader_event(const ObAddr &old_leader, const ObAddr &new_leader)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(*this), K(old_leader), K(new_leader)
  int ret = OB_SUCCESS;
  char info[INFO_MAX_LEN] = {0};
  int64_t pos = 0;
  bool last_print_flag = false;
  DO_IF_SUCC(databuff_printf(info, INFO_MAX_LEN, pos, "witness change leader : %s -> %s", to_cstring(old_leader), to_cstring(new_leader)));
  DO_IF_SUCC(report_event_(ElectionEventType::WITNESS_CHANGE_LEADER, info));
  if (CLICK_FAIL(ret)) {
    LOG_EVENT(WARN, "report WITNESS_CHANGE_LEADER event failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}
//#endif
