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

#ifndef OCEANBASE_LOGSERVICE_LOG_RECONFIRM_
#define OCEANBASE_LOGSERVICE_LOG_RECONFIRM_

#include "lib/lock/ob_spin_lock.h"
#include "common/ob_member_list.h"
#include "log_ack_info.h"
#include "log_simple_member_list.h"
#include "log_define.h"
#include "log_task.h"
#include "log_meta_info.h"

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace palf
{
class LogSlidingWindow;
class LogStateMgr;
class LogConfigMgr;
class LogEngine;
class LogGroupEntry;
class LogModeMgr;

class LogReconfirm
{
public:
  LogReconfirm();
  virtual ~LogReconfirm() { destroy(); }
public:
  virtual int init(const int64_t palf_id,
           const ObAddr &self,
           LogSlidingWindow *sw,
           LogStateMgr *state_mgr,
           LogConfigMgr *mm,
           LogModeMgr *mode_mgr,
           LogEngine *log_engine);
  virtual void destroy();
  virtual void reset_state();
  virtual bool need_wlock();
  virtual bool can_receive_log() const;
  virtual bool can_do_degrade() const;
  virtual int reconfirm();
  virtual int handle_prepare_response(const common::ObAddr &server,
                                      const int64_t &src_proposal_id,
                                      const int64_t &accept_proposal_id,
                                      const LSN &last_lsn,
                                      const LSN &committed_end_lsn);
  TO_STRING_KV(K_(palf_id), K_(self), "state", state_to_string(state_), K_(new_proposal_id), K_(prepare_log_ack_list), \
  K_(curr_paxos_follower_list), K_(majority_cnt), K_(majority_max_log_server),       \
  K_(majority_max_accept_pid), K_(majority_max_lsn), K_(saved_end_lsn), K_(last_submit_prepare_req_time_us),         \
  K_(last_fetch_log_time_us), K_(last_record_sw_start_id), K_(last_notify_fetch_time_us), K_(last_purge_throttling_time_us), KP(this));
private:
  int init_reconfirm_();
  int submit_prepare_log_();
  int wait_all_log_flushed_();
  bool is_fetch_log_finished_();
  bool is_confirm_log_finished_() const;
  int try_fetch_log_();
  int update_follower_end_lsn_(const common::ObAddr &server, const LSN &committed_end_lsn);
  int ack_log_with_end_lsn_();
  bool is_majority_catch_up_();
  int purge_throttling_();
private:
  enum State
  {
    INITED = 0,
	// this state is not correctness guarantee, if you do not wait all log flused,
	// consensus protocol still works, because when START_WORKING has committed,
	// all previous logs have been confirmed, the only effect is to avoid unnecessary
	// pulling logs.
    WAITING_LOG_FLUSHED = 1,
	// this state is used to query who is the newest server, records it as 'newest_server_',
    FETCH_MAX_LOG_LSN = 2,
	// this state is used to reconfirm mode_meta to majority.
    RECONFIRM_MODE_META = 3,
	// this state is used to fetch all logs from the 'newest_server_'.
    RECONFIRM_FETCH_LOG = 4,
	// this state is used to wait fetch log finished, and then write START_WORKING log.
    RECONFIRMING = 5,
	// this state is used to wait write START_WORKING finished.
    START_WORKING = 6,
    FINISHED = 7,
  };

  const char *state_to_string(const State &state) const
  {
    switch (state)
    {
      case(State::INITED): return "INITED";
      case(State::WAITING_LOG_FLUSHED): return "WAITING_LOG_FLUSHED";
      case(State::FETCH_MAX_LOG_LSN): return "FETCH_MAX_LOG_LSN";
      case(State::RECONFIRM_FETCH_LOG): return "RECONFIRM_FETCH_LOG";
      case(State::RECONFIRM_MODE_META): return "RECONFIRM_MODE_META";
      case(State::RECONFIRMING): return "RECONFIRMING";
      case(State::START_WORKING): return "START_WORKING";
      case(State::FINISHED): return "FINISHED";
      default:
        return "INVALID_RECONFIRM_STATE";
    }
  }

  static const int64_t RECONFIRM_PREPARE_RETRY_INTERVAL_US = 2 * 1000 * 1000; // 2s
private:
  State state_;
  int64_t palf_id_;
  common::ObAddr self_;
  int64_t new_proposal_id_;

	//
	//
  //                                  cur fetch id
  //                                       │
  //                  │un committed id│    ▼
  //    ┌─────────────┼───────────────┼───────────────────────┐
  //    │             │               │                       │
  //    └─────────────┴───────────────┴───────────────────────┘
  //    ▲             ▲               ▲                       ▲
  //    │             │               │                       │
  // start id     committed id    max flushed id             end id
	//
	// After reconfirm, election leader will be the newest server of majority.
	// 'max_lsn_' is max flushed id;
	// 'max_log_proposal_id_' is the proposal of 'max_lsn_';
	// 'fetching_lsn_' is current fetch id, will be updated by receive_log;
	// end id is the max flushed id of 'majority_max_log_server_'.
	// NB: before reconfirm, election leader will behind others too much,
	//     however, the maximum numbers of uncommitted logs for any replica can
	//     not exceed the max size of ObGroupBuffer, we just only advance the
	//     start id of sliding window, the sliding window will not be full.
	//
  // current lsn for fetching which is inited by local max log flushed offset
  // NB: The correctness of using local max flushed log offset as 'fetch_lsn_'
  // 1. If replica receives config log, need forward check;
  // 2. Only after start working, can submit new log.
	// int64_t fetching_lsn_;

  // prepare response server list
  LogAckList prepare_log_ack_list_;
  // all paxos folowers, not include self
  common::ObMemberList curr_paxos_follower_list_;
  int64_t majority_cnt_;
  // the server whose log is newest among majority
  common::ObAddr majority_max_log_server_;
  // the accept_proposal_id of that server
  int64_t majority_max_accept_pid_;
  // the max_lsn of that server
  LSN majority_max_lsn_;
  // the max lsn when generate start_working log
  LSN saved_end_lsn_;
  LogMemberAckInfoList follower_end_lsn_list_;
  LogConfigVersion sw_config_version_;

  LogSlidingWindow *sw_;
  LogStateMgr *state_mgr_;
  LogConfigMgr *mm_;
  LogModeMgr *mode_mgr_;
  LogEngine *log_engine_;

  mutable common::ObSpinLock lock_;
  // last time of sending prepare log
  int64_t last_submit_prepare_req_time_us_;
  // last time of fetching log
  int64_t last_fetch_log_time_us_;
  int64_t last_record_sw_start_id_;
  int64_t wait_slide_print_time_us_;
  int64_t wait_majority_time_us_;
  int64_t last_notify_fetch_time_us_;
  int64_t last_purge_throttling_time_us_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogReconfirm);
};
} // namespace palf
} // namespace oceanbase
#endif //_OCEANBASE_LOGSERVICE_LOG_RECONFIRM_
