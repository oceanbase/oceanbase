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

#ifndef LOGSERVICE_PALF_ELECTION_UTILS_ELECTION_EVENT_HISTORY_ACCESSOR_H
#define LOGSERVICE_PALF_ELECTION_UTILS_ELECTION_EVENT_HISTORY_ACCESSOR_H

#include "lib/list/ob_dlist.h"
#include "lib/oblog/ob_log_time_fmt.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/utils/election_member_list.h"
#include "logservice/palf/election/utils/election_utils.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

enum class ElectionEventType
{
  VOTE,
  DECENTRALIZED_TO_BE_LEADER,
  DIRECTLY_CHANGE_LEADER,
  PREPARE_CHANGE_LEADER,
  CHANGE_LEADER_TO_REVOKE,
  LEASE_EXPIRED_TO_REVOKE,
  CHANGE_LEADER_TO_TAKEOVER,
  WITNESS_CHANGE_LEADER,
  CHANGE_MEMBERLIST,
  CHANGE_LEASE_TIME,
  LEASE_EXPIRED,
};

inline const char *obj_to_string(ElectionEventType type)
{
  switch (type) {
    case ElectionEventType::VOTE:
      return "vote";
    case ElectionEventType::DECENTRALIZED_TO_BE_LEADER:
      return "decentralized to be leader";
    case ElectionEventType::DIRECTLY_CHANGE_LEADER:
      return "directly change leader";
    case ElectionEventType::PREPARE_CHANGE_LEADER:
      return "prepare change leader";
    case ElectionEventType::CHANGE_LEADER_TO_REVOKE:
      return "change leader to revoke";
    case ElectionEventType::LEASE_EXPIRED_TO_REVOKE:
      return "lease expired to revoke";
    case ElectionEventType::CHANGE_LEADER_TO_TAKEOVER:
      return "change leader to takeover";
    case ElectionEventType::WITNESS_CHANGE_LEADER:
      return "witness change leader";
    case ElectionEventType::CHANGE_MEMBERLIST:
      return "change memberlist";
    case ElectionEventType::CHANGE_LEASE_TIME:
      return "change lease time";
    case ElectionEventType::LEASE_EXPIRED:
      return "lease expired";
    default:
      return "unknown type";
  }
}

class EventRecorder
{
public:
  EventRecorder(const int64_t &ls_id, const ObAddr &self_addr, ObOccamTimer *&timer) :
  ls_id_(ls_id), self_addr_(self_addr), timer_(timer), need_report_(true) {}
  void set_need_report(const bool need_report) { need_report_ = need_report; }
  // proposer event
  int report_decentralized_to_be_leader_event(const MemberListWithStates &member_list_with_states);// 当选Leader
  int report_leader_lease_expired_event(const MemberListWithStates &member_list_with_states);// Leader续约失败卸任
  int report_directly_change_leader_event(const ObAddr &dest_svr, const ObStringHolder &reason);// Leader直接切主，不走RCS流程
  int report_prepare_change_leader_event(const ObAddr &dest_svr, const ObStringHolder &reason);// 旧Leader准备切主
  int report_change_leader_to_revoke_event(const ObAddr &dest_svr);// 旧Leader卸任
  int report_change_leader_to_takeover_event(const ObAddr &addr);// 新Leader上任
  int report_member_list_changed_event(const MemberList &old_list, const MemberList &new_list);// 修改成员列表
  // acceptor event
  int report_vote_event(const ObAddr &dest_svr, const ObStringHolder &reason);// 一呼百应prepare阶段投票
  int report_acceptor_lease_expired_event(const Lease &lease);// Lease到期
  int report_acceptor_witness_change_leader_event(const ObAddr &old_leader, const ObAddr &new_leader);// 见证切主事件
  int64_t to_string(char *buf, const int64_t len) const;
private:
  int report_event_(ElectionEventType type, const common::ObString &info);
  const int64_t &ls_id_;
  const ObAddr &self_addr_;
  ObOccamTimer *&timer_;
  bool need_report_;
};

}
}
}

#endif