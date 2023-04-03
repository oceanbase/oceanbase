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

#ifndef LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_ACCEPTOR_H
#define LOGSERVICE_PALF_ELECTION_ALGORITHM_OB_ELECTION_ACCEPTOR_H

#include "lib/net/ob_addr.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/election/utils/election_utils.h"
#include "logservice/palf/election/message/election_message.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class RequestChecker;
class ElectionPrepareRequestMsg;
class ElectionAcceptRequestMsg;
class ElectionImpl;

class ElectionAcceptor
{
  friend class RequestChecker;
  friend class ElectionImpl;
public:
  ElectionAcceptor(ElectionImpl *p_election);
  int start();
  void stop();
  void on_prepare_request(const ElectionPrepareRequestMsg &prepare_req);
  void on_accept_request(const ElectionAcceptRequestMsg &accept_req, int64_t *us_to_expired);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void reset_time_window_states_(const LogPhase phase);
  void advance_ballot_number_and_reset_related_states_(const int64_t new_ballot_number, const LogPhase phase);
private:
  int64_t ballot_number_;// 选举的轮次
  int64_t ballot_of_time_window_;// 已经prepare响应成功的轮次
  Lease lease_;// 租约
  ElectionPrepareRequestMsg highest_priority_prepare_req_;// 缓存的最高优先级的prepare请求
  ObStringHolder vote_reason_;
  bool is_time_window_opened_;// 时间窗口的打开状态
  ElectionImpl * const p_election_;// 指向election的指针
  common::ObOccamTimerTaskRAIIHandle time_window_task_handle_;// 用于在时间窗口关闭时投票
  int64_t last_time_window_open_ts_;// 添加该状态的理由与proposer.last_do_prepare_ts_相同
  int64_t last_dump_acceptor_info_ts_;
};

}// namespace election
}// namespace palf
}// namesapce oceanbase

#endif