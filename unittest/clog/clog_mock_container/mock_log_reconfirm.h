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

#ifndef _OCEANBASE_UNITTEST_CLOG_CLOG_MOCK_UTILS_MOCK_LOG_RECONFIRM_H_
#define _OCEANBASE_UNITTEST_CLOG_CLOG_MOCK_UTILS_MOCK_LOG_RECONFIRM_H_

#include "lib/lock/ob_spin_lock.h"
#include "clog/ob_log_define.h"
#include "clog/ob_log_sliding_window.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_log_entry_header.h"
#include "clog/ob_log_task.h"
#include "clog/ob_log_checksum_V2.h"
#include "clog/ob_log_reconfirm.h"

namespace oceanbase {
using namespace common;
namespace clog {
class ObILogMembershipMgr;

class MockObLogReconfirm : public ObLogReconfirm {
public:
  MockObLogReconfirm()
  {}
  virtual ~MockObLogReconfirm()
  {}
  int init(ObILogSWForReconfirm* sw, ObILogStateMgrForReconfirm* state_mgr, ObILogMembershipMgr* mm,
      ObILogEngine* log_engine, ObILogAllocator* alloc_mgr, const common::ObPartitionKey& partition_key,
      const common::ObAddr self)
  {
    UNUSED(sw);
    UNUSED(log_engine);
    UNUSED(partition_key);
    UNUSED(state_mgr);
    UNUSED(mm);
    UNUSED(alloc_mgr);
    UNUSED(self);
    return OB_SUCCESS;
  }
  int reconfirm()
  {
    return OB_SUCCESS;
  }
  int receive_log(const ObLogEntry& log_entry, const int64_t server_id)
  {
    UNUSED(log_entry);
    UNUSED(server_id);
    return OB_SUCCESS;
  }
  int receive_max_log_id(const int64_t server_id, const uint64_t log_id)
  {
    UNUSED(server_id);
    UNUSED(log_id);
    return OB_SUCCESS;
  }
  void clear()
  {}
  bool need_start_up()
  {
    return true;
  }

  enum State {
    INITED = 0,
    FLUSHING_PREPARE_LOG = 1,
    FETCH_MAX_LSN = 2,
    RECONFIRMING = 3,
    START_WORKING = 4,
    FINISHED = 5,
  };

  static const int64_t MAJORITY_TAG_BIT = 31;
  static const int64_t CONFIRMED_TAG_BIT = 30;
  static const int64_t SUBMITED_TAG_BIT = 29;
  static const int64_t LOG_EXIST_TAG_BIT = 28;

private:
  int init_reconfirm_()
  {
    return OB_SUCCESS;
  }
  bool is_new_proposal_id_flushed_()
  {
    return true;
  }
  int fetch_max_log_id_()
  {
    return OB_SUCCESS;
  }
  int try_set_majority_ack_tag_of_max_log_id_()
  {
    return OB_SUCCESS;
  }
  int get_start_id_and_leader_ts_()
  {
    return OB_SUCCESS;
  }
  int prepare_log_map_()
  {
    return OB_SUCCESS;
  }
  int reconfirm_log_()
  {
    return OB_SUCCESS;
  }
  int try_fetch_log_()
  {
    return OB_SUCCESS;
  }
  int confirm_log_()
  {
    return OB_SUCCESS;
  }
  int retry_confirm_log_()
  {
    return OB_SUCCESS;
  }
  int try_filter_invalid_log_()
  {
    return OB_SUCCESS;
  }

  bool is_first_time_()
  {
    return true;
  }
  bool need_retry_reconfirm_()
  {
    return true;
  }
  const char* get_state_str_(const State state) const
  {
    UNUSED(state);
    return NULL;
  }
  void generate_nop_log_(const uint64_t log_id, const int64_t idx)
  {
    UNUSED(log_id);
    UNUSED(idx);
  }
};
}  // namespace clog
}  // namespace oceanbase
#endif  // _OCEANBASE_UNITTEST_CLOG_CLOG_MOCK_UTILS_MOCK_LOG_RECONFIRM_H_
