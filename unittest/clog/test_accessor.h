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

#include "clog/ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace clog;

namespace unittest {

class ObLogReconfirmAccessor {
public:
  void set_assert_on(ObLogReconfirm& reconfirm, bool assert)
  {
    reconfirm.set_assert_on_ = assert;
  }
  void set_state(ObLogReconfirm& reconfirm, ObLogReconfirm::State state)
  {
    reconfirm.state_ = state;
  }
  ObLogSimpleBitMap& get_max_log_ack_map(ObLogReconfirm& reconfirm)
  {
    return reconfirm.max_log_ack_map_;
  }
  ObProposalID& get_new_proposal_id(ObLogReconfirm& reconfirm)
  {
    return reconfirm.new_proposal_id_;
  }
  uint64_t& get_max_flushed_id(ObLogReconfirm& reconfirm)
  {
    return reconfirm.max_flushed_id_;
  }
  uint64_t& get_start_id(ObLogReconfirm& reconfirm)
  {
    return reconfirm.start_id_;
  }
  uint64_t& get_next_id(ObLogReconfirm& reconfirm)
  {
    return reconfirm.next_id_;
  }
  int64_t& get_leader_ts(ObLogReconfirm& reconfirm)
  {
    return reconfirm.leader_ts_;
  }
  ObLogEntry*& get_log_array(ObLogReconfirm& reconfirm)
  {
    return reconfirm.log_array_;
  }
  void prepare_map(ObLogReconfirm& reconfirm)
  {
    reconfirm.prepare_log_map_();
  }
  int execute_try_filter_invalid_log(ObLogReconfirm& reconfirm)
  {
    return reconfirm.try_filter_invalid_log_();
  }
  int execute_init_reconfirm(ObLogReconfirm& reconfirm)
  {
    return reconfirm.init_reconfirm_();
  }
  int execute_get_start_id_and_leader_ts(ObLogReconfirm& reconfirm)
  {
    return reconfirm.get_start_id_and_leader_ts_();
  }
  int execute_try_fetch_log(ObLogReconfirm& reconfirm)
  {
    return reconfirm.try_fetch_log_();
  }
};
}  // namespace unittest
}  // namespace oceanbase
