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

#ifndef OCEANBASE_LOGSERVICE_LOG_SLIDING_WINDOW_
#define OCEANBASE_LOGSERVICE_LOG_SLIDING_WINDOW_

#include <stdint.h>
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/ob_thread_lease.h"
#include "log_ack_info.h"
#include "share/scn.h"
#include "log_group_entry.h"
#include "log_group_buffer.h"
#include "log_checksum.h"
#include "log_req.h"
#include "lsn.h"
#include "lsn_allocator.h"
#include "log_task.h"
#include "fixed_sliding_window.h"
#include "palf_base_info.h"
#include "palf_callback_wrapper.h"

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace palf
{
class PalfFSCbWrapper;
class LogEntryHeader;
}
namespace palf
{
class LogEngine;
class FlushLogCbCtx;
class LogGroupEntryHeader;
class LogTaskHeaderInfo;
class LogStateMgr;
class LogConfigMgr;
class LogModeMgr;
class LogTask;
class LogGroupEntry;
class TruncateLogCbCtx;

enum FetchTriggerType
{
  LOG_LOOP_TH = 0,
  SLIDING_CB = 1,
  LEADER_RECONFIRM = 2,
  NOTIFY_REBUILD = 3,
  LEARNER_REGISTER = 4,
  CLEAN_CACHED_LOG = 5,
  MODE_META_BARRIER = 6,
  RECONFIRM_NOTIFY_FETCH = 7,
  ADD_MEMBER_PRE_CHECK = 8,
};

inline const char *fetch_trigger_type_2_str(const FetchTriggerType type)
{
#define EXTRACT_TRIGGER_TYPE(type_var) ({ case(type_var): return #type_var; })
  switch(type)
  {
    EXTRACT_TRIGGER_TYPE(LOG_LOOP_TH);
    EXTRACT_TRIGGER_TYPE(SLIDING_CB);
    EXTRACT_TRIGGER_TYPE(LEADER_RECONFIRM);
    EXTRACT_TRIGGER_TYPE(NOTIFY_REBUILD);
    EXTRACT_TRIGGER_TYPE(LEARNER_REGISTER);
    EXTRACT_TRIGGER_TYPE(CLEAN_CACHED_LOG);
    EXTRACT_TRIGGER_TYPE(MODE_META_BARRIER);
    EXTRACT_TRIGGER_TYPE(RECONFIRM_NOTIFY_FETCH);
    EXTRACT_TRIGGER_TYPE(ADD_MEMBER_PRE_CHECK);

    default:
      return "Invalid Type";
  }
#undef EXTRACT_TRIGGER_TYPE
}

enum TruncateType
{
  INVALID_TRUNCATE_TYPE = 0,
  TRUNCATE_CACHED_LOG_TASK = 1,
  TRUNCATE_LOG = 2,
};

inline const char *truncate_type_2_str(const TruncateType type)
{
#define EXTRACT_TRUNCATE_TYPE(type_var) ({ case(type_var): return #type_var; })
  switch(type)
  {
    EXTRACT_TRUNCATE_TYPE(INVALID_TRUNCATE_TYPE);
    EXTRACT_TRUNCATE_TYPE(TRUNCATE_CACHED_LOG_TASK);
    EXTRACT_TRUNCATE_TYPE(TRUNCATE_LOG);

    default:
      return "Invalid Type";
  }
#undef EXTRACT_TRUNCATE_TYPE
}

enum FreezeMode
{
  PERIOD_FREEZE_MODE = 0,
  FEEDBACK_FREEZE_MODE,
};

inline const char *freeze_mode_2_str(const FreezeMode mode)
{
#define EXTRACT_FREEZE_MODE(type_var) ({ case(type_var): return #type_var; })
  switch(mode)
  {
    EXTRACT_FREEZE_MODE(PERIOD_FREEZE_MODE);
    EXTRACT_FREEZE_MODE(FEEDBACK_FREEZE_MODE);

    default:
      return "Invalid Mode";
  }
#undef EXTRACT_FREEZE_MODE
}

struct TruncateLogInfo
{
  TruncateType truncate_type_;
  int64_t truncate_log_id_;
  LSN truncate_begin_lsn_;
  int64_t truncate_log_proposal_id_;
  TruncateLogInfo() { reset(); }
  bool is_valid() const {
    return (OB_INVALID_LOG_ID != truncate_log_id_
      && INVALID_PROPOSAL_ID != truncate_log_proposal_id_
      && truncate_begin_lsn_.is_valid());
  }
  void reset() {
    truncate_type_ = INVALID_TRUNCATE_TYPE;
    truncate_log_id_ = OB_INVALID_LOG_ID;
    truncate_begin_lsn_.reset();
    truncate_log_proposal_id_ = INVALID_PROPOSAL_ID;
  }
  TO_STRING_KV("truncate_type", truncate_type_2_str(truncate_type_), K_(truncate_log_id), K_(truncate_begin_lsn), K_(truncate_log_proposal_id));
};

class UpdateMatchLsnFunc
{
public:
  UpdateMatchLsnFunc(const LSN &end_lsn, const int64_t new_ack_time_us)
      : new_end_lsn_(end_lsn), old_end_lsn_(), new_ack_time_us_(new_ack_time_us), old_advance_time_us_(OB_INVALID_TIMESTAMP)
  {}
  ~UpdateMatchLsnFunc() {}
  bool operator()(const common::ObAddr &server, LsnTsInfo &value);
  bool is_advance_delay_too_long() const {
    bool bool_ret = false;
    if (old_end_lsn_ < new_end_lsn_
        && (new_ack_time_us_ - old_advance_time_us_) > MATCH_LSN_ADVANCE_DELAY_THRESHOLD_US) {
      // Return true when advance delay exceeds 1s.
      bool_ret = true;
    }
    return bool_ret;
  }
  TO_STRING_KV(K_(old_end_lsn), K_(new_end_lsn), K_(old_advance_time_us), K_(new_ack_time_us),
      "advance delay(us)", new_ack_time_us_ - old_advance_time_us_);
private:
  LSN new_end_lsn_;
  LSN old_end_lsn_;
  int64_t new_ack_time_us_;
  int64_t old_advance_time_us_;
};

class GetLaggedListFunc
{
public:
  GetLaggedListFunc(const LSN &dst_lsn)
      : dst_lsn_(dst_lsn), lagged_list_()
  {}
  ~GetLaggedListFunc() {}
  bool operator()(const common::ObAddr &server, LsnTsInfo &value);
  int get_lagged_list(common::ObMemberList &out_list) const
  { return out_list.deep_copy(lagged_list_); }
  TO_STRING_KV(K_(dst_lsn), K_(lagged_list));
private:
  LSN dst_lsn_;
  common::ObMemberList lagged_list_;
};

inline bool is_need_rebuild(const LSN &end_lsn, const LSN &last_rebuild_lsn)
{
  return (end_lsn.is_valid() &&
          last_rebuild_lsn.is_valid() &&
          end_lsn < last_rebuild_lsn);
}

class LogSlidingWindow : public ISlidingCallBack
{
public:
  LogSlidingWindow();
  virtual ~LogSlidingWindow() { destroy(); }
public:
  virtual void destroy();
  virtual int flashback(const PalfBaseInfo &palf_base_info, const int64_t palf_id, common::ObILogAllocator *alloc_mgr);
  virtual int init(const int64_t palf_id,
                   const common::ObAddr &self,
                   LogStateMgr *state_mgr,
                   LogConfigMgr *mm,
                   LogModeMgr *mode_mgr,
                   LogEngine *log_engine,
                   palf::PalfFSCbWrapper *palf_fs_cb,
                   common::ObILogAllocator *alloc_mgr,
                   LogPlugins *plugins,
                   const PalfBaseInfo &palf_base_info,
                   const bool is_normal_replica);
  virtual int sliding_cb(const int64_t sn, const FixedSlidingWindowSlot *data);
  virtual int64_t get_max_log_id() const;
  virtual const share::SCN get_max_scn() const;
  virtual LSN get_max_lsn() const;
  virtual int64_t get_start_id() const;
  virtual int get_committed_end_lsn(LSN &committed_end_lsn) const;
  virtual int try_fetch_log(const FetchTriggerType &fetch_log_type,
                            const LSN prev_lsn = LSN(),
                            const LSN fetch_start_lsn = LSN(),
                            const int64_t fetch_start_log_id = OB_INVALID_LOG_ID);
  virtual int try_fetch_log_for_reconfirm(const common::ObAddr &dest, const LSN &fetch_end_lsn, bool &is_fetched);
  virtual int submit_push_log_resp(const common::ObAddr &server);
  virtual bool is_empty() const;
  virtual bool check_all_log_has_flushed();
  virtual int get_majority_match_lsn(LSN &majority_match_lsn);
  virtual int get_lagged_member_list(const LSN &dst_lsn, ObMemberList &lagged_list);
  virtual bool is_all_committed_log_slided_out(LSN &prev_lsn, int64_t &prev_log_id, LSN &committed_end_lsn) const;
  // ================= log sync part begin
  virtual int submit_log(const char *buf,
                 const int64_t buf_len,
                 const share::SCN &ref_scn,
                 LSN &lsn,
                 share::SCN &scn);
  virtual int submit_group_log(const LSN &lsn,
                       const char *buf,
                       const int64_t buf_len);
  virtual int receive_log(const common::ObAddr &src_server,
                  const PushLogType push_log_type,
                  const LSN &prev_lsn,
                  const int64_t &prev_log_pid,
                  const LSN &lsn,
                  const char *buf,
                  const int64_t buf_len,
                  const bool need_check_clean_log,
                  TruncateLogInfo &truncate_log_info);
  virtual int after_flush_log(const FlushLogCbCtx &flush_cb_ctx);
  virtual int after_truncate(const TruncateLogCbCtx &truncate_cb_ctx);
  virtual int after_rebuild(const LSN &lsn);
  virtual int ack_log(const common::ObAddr &src_server, const LSN &end_lsn);
  virtual int truncate(const TruncateLogInfo &truncate_log_info, const LSN &expected_prev_lsn,
      const int64_t expected_prev_log_pid);
  virtual bool is_allow_rebuild() const;
  virtual int truncate_for_rebuild(const PalfBaseInfo &palf_base_info);
  virtual bool is_prev_log_pid_match(const int64_t log_id,
                                     const LSN &lsn,
                                     const LSN &prev_lsn,
                                     const int64_t &prev_log_pid,
                                     bool &is_prev_log_exist);
  virtual bool pre_check_for_config_log(const int64_t &msg_proposal_id,
                                        const LSN &lsn,
                                        const int64_t &log_proposal_id,
                                        TruncateLogInfo &truncate_log_info);
  virtual int handle_committed_info(const common::ObAddr &server,
                                    const int64_t prev_log_id,
                                    const int64_t &prev_log_proposal_id,
                                    const LSN &committed_end_lsn);
  virtual int config_change_update_match_lsn_map(const ObMemberList &added_memberlist,
                                                 const ObMemberList &removed_memberlist,
                                                 const ObMemberList &new_log_sync_memberlist,
                                                 const int64_t new_replica_num);
  // ================= log sync part end
  virtual int append_disk_log(const LSN &lsn, const LogGroupEntry &group_entry);
  virtual int report_log_task_trace(const int64_t log_id);
  virtual void get_max_flushed_end_lsn(LSN &end_lsn) const;
  virtual int get_max_flushed_log_info(LSN &lsn,
                               LSN &end_lsn,
                               int64_t &log_proposal_id) const;
  virtual int clean_log();
  virtual int clean_cached_log(const int64_t begin_log_id,
                               const LSN &lsn,
                               const LSN &prev_lsn,
                               const int64_t prev_log_pid);
  virtual int to_follower_pending(LSN &last_lsn);
  virtual int to_leader_reconfirm();
  virtual int to_leader_active();
  virtual int try_advance_committed_end_lsn(const LSN &end_lsn);
  virtual int64_t get_last_submit_log_id_() const;
  virtual void get_last_submit_end_lsn_(LSN &end_lsn) const;
  virtual int get_last_submit_log_info(LSN &last_submit_lsn, int64_t &log_id, int64_t &log_proposal_id) const;
  virtual int get_last_submit_log_info(LSN &last_submit_lsn,
      LSN &last_submit_end_lsn, int64_t &log_id, int64_t &log_proposal_id) const;
  virtual int get_last_slide_end_lsn(LSN &out_end_lsn) const;
  virtual const share::SCN get_last_slide_scn() const;
  virtual int check_and_switch_freeze_mode();
  virtual bool is_in_period_freeze_mode() const;
  virtual int period_freeze_last_log();
  virtual int inc_update_scn_base(const share::SCN &scn);
  virtual int get_server_ack_info(const common::ObAddr &server, LsnTsInfo &ack_info) const;
  virtual int get_ack_info_array(LogMemberAckInfoList &ack_info_array) const;
  virtual int pre_check_before_degrade_upgrade(const LogMemberAckInfoList &servers,
                                               bool is_degrade);
  virtual int advance_reuse_lsn(const LSN &flush_log_end_lsn);
  virtual int try_send_committed_info(const common::ObAddr &server,
                                      const LSN &log_lsn,
                                      const LSN &log_end_lsn,
                                      const int64_t &log_proposal_id);

  virtual int get_leader_from_cache(common::ObAddr &leader) const;
  virtual int read_data_from_buffer(const LSN &read_begin_lsn,
                                    const int64_t in_read_size,
                                    char *buf,
                                    int64_t &out_read_size) const;
  int64_t get_last_slide_log_id() const;
  TO_STRING_KV(K_(palf_id), K_(self), K_(lsn_allocator), K_(group_buffer),                         \
  K_(last_submit_lsn), K_(last_submit_end_lsn), K_(last_submit_log_id), K_(last_submit_log_pid),   \
  K_(max_flushed_lsn), K_(max_flushed_end_lsn), K_(max_flushed_log_pid), K_(committed_end_lsn),    \
  K_(last_slide_log_id), K_(last_slide_scn), K_(last_slide_lsn), K_(last_slide_end_lsn),        \
  K_(last_slide_log_pid), K_(last_slide_log_accum_checksum), K_(last_fetch_end_lsn),               \
  K_(last_fetch_max_log_id), K_(last_fetch_committed_end_lsn), K_(last_truncate_lsn),           \
  K_(last_fetch_req_time), K_(is_truncating), K_(is_rebuilding), K_(last_rebuild_lsn),          \
  "freeze_mode", freeze_mode_2_str(freeze_mode_), \
  "last_fetch_trigger_type", fetch_trigger_type_2_str(last_fetch_trigger_type_), KP(this));
private:
  int do_init_mem_(const int64_t palf_id,
                   const PalfBaseInfo &palf_base_info,
                   common::ObILogAllocator *alloc_mgr);
  int get_fetch_log_dst_(common::ObAddr &leader) const;
  int get_leader_from_cache_(common::ObAddr &leader) const;
  int clean_log_();
  int reset_match_lsn_map_();
  bool is_all_log_flushed_();
  int leader_wait_sw_slot_ready_(const int64_t log_id);
  bool can_receive_larger_log_(const int64_t log_id) const;
  bool leader_can_submit_larger_log_(const int64_t log_id) const;
  bool leader_can_submit_new_log_(const int64_t valid_log_size, LSN &lsn_upper_bound);
  bool leader_can_submit_group_log_(const LSN &lsn, const int64_t group_log_size);
  void get_committed_end_lsn_(LSN &out_lsn) const;
  int get_max_flushed_log_info_(LSN &lsn,
                                LSN &end_lsn,
                                int64_t &log_proposal_id) const;
  int get_prev_log_info_(const int64_t log_id,
                         LSN &prev_lsn,
                         LSN &prev_end_lsn,
                         share::SCN &prev_log_iscn,
                         int64_t &prev_log_pid,
                         int64_t &prev_log_accum_checksum);
  int inc_update_max_flushed_log_info_(const LSN &lsn,
                                       const LSN &end_lsn,
                                       const int64_t &proposal_id);
  int truncate_max_flushed_log_info_(const LSN &lsn,
                                     const LSN &end_lsn,
                                     const int64_t &log_proposal_id);
  void get_last_slide_end_lsn_(LSN &out_end_lsn) const;
  int64_t get_last_slide_log_id_() const;
  void get_last_slide_log_info_(int64_t &log_id,
                                share::SCN &scn,
                                LSN &lsn,
                                LSN &end_lsn,
                                int64_t &log_proposal_id,
                                int64_t &accum_checksum) const;
  int try_update_last_slide_log_info_(const int64_t log_id,
                                      const share::SCN &scn,
                                      const LSN &lsn,
                                      const LSN &end_lsn,
                                      const int64_t &proposal_id,
                                      const int64_t accum_checksum);
  int try_advance_committed_lsn_(const LSN &end_lsn);
  void get_last_submit_log_info_(LSN &lsn,
                                 LSN &end_lsn,
                                 int64_t &log_id,
                                 int64_t &log_proposal_id) const;
  int set_last_submit_log_info_(const LSN &lsn,
                                const LSN &end_lsn,
                                const int64_t log_id,
                                const int64_t &log_proposal_id);
  int try_freeze_prev_log_(const int64_t next_log_id, const LSN &lsn, bool &is_need_handle);
  int feedback_freeze_last_log_();
  int try_freeze_last_log_task_(const int64_t expected_log_id, const LSN &expected_end_lsn, bool &is_need_handle);
  int generate_new_group_log_(const LSN &lsn,
                              const int64_t log_id,
                              const share::SCN &scn,
                              const int64_t log_body_size,
                              const LogType &log_type,
                              const char *log_data,
                              const int64_t data_len,
                              bool &is_need_handle);
  int append_to_group_log_(const LSN &lsn,
                           const int64_t log_id,
                           const share::SCN &scn,
                           const int64_t log_entry_size,
                           const char *log_data,
                           const int64_t data_len,
                           bool &is_need_handle);
  int handle_next_submit_log_(bool &is_committed_lsn_updated);
  int handle_committed_log_();
  int apply_committed_log_();
  int generate_group_entry_header_(const int64_t log_id,
                                   LogTask *log_task,
                                   LogGroupEntryHeader &header,
                                   int64_t &group_log_checksum,
                                   bool &is_accum_checksum_acquired);
  int gen_committed_end_lsn_(LSN &new_committed_end_lsn);
  int gen_committed_end_lsn_with_memberlist_(
    const ObMemberList &member_list,
    const int64_t replica_num);
  int get_majority_lsn_(const ObMemberList &member_list,
                        const int64_t replica_num,
                        LSN &result_lsn) const;
  int inc_ref_(LogTask *log_task, const int64_t inc_val, int64_t &result);
  bool need_update_log_task_(LogGroupEntryHeader &header,
                             LogTask *log_task,
                             bool &need_send_ack,
                             bool &is_local_log_valid,
                             bool &is_log_pid_match) const;
  int try_update_match_lsn_map_(const common::ObAddr &server, const LSN &end_lsn);
  int wait_group_buffer_ready_(const LSN &lsn, const int64_t data_len);
  int append_disk_log_to_sw_(const LSN &lsn, const LogGroupEntry &group_entry);
  int try_update_max_lsn_(const LSN &lsn, const LogGroupEntryHeader &header);
  int truncate_lsn_allocator_(const LSN &last_lsn, const int64_t last_log_id, const share::SCN &last_scn);
  bool is_all_committed_log_slided_out_(LSN &prev_lsn,
                                        int64_t &prev_log_id,
                                        LSN &start_lsn,
                                        LSN &committed_end_lsn) const;
  void get_last_fetch_info_(LSN &last_fetch_end_lsn,
                            LSN &last_committed_end_lsn,
                            int64_t &last_fetch_max_log_id) const;
  void try_reset_last_fetch_log_info_(const LSN &expected_end_lsn, const int64_t log_id);
  void try_update_committed_lsn_for_fetch_(const LSN &expected_end_lsn,
      const int64_t &expected_log_id,
      const LSN &log_committed_end_lsn,
      bool &is_need_fetch);
  void try_fetch_log_streamingly_(const LSN &log_end_lsn);
  int do_fetch_log_(const FetchTriggerType &trigger_type,
                    const common::ObAddr &dest,
                    const LSN &prev_lsn,
                    const LSN &fetch_start_lsn,
                    const int64_t fetch_log_size,
                    const int64_t fetch_start_log_id);
  int freeze_pending_log_(LSN &last_lsn);
  int check_all_log_task_freezed_(bool &is_all_freezed);
  int get_min_scn_from_buf_(const LogGroupEntryHeader &group_entry_header,
                                  const char *buf,
                                  const int64_t buf_len,
                                  share::SCN &min_scn);
  int leader_get_committed_log_info_(const LSN &committed_end_lsn,
                                     int64_t &log_id,
                                     int64_t &log_proposal_id);
  int leader_broadcast_committed_info_(const LSN &committed_end_lsn);
  int submit_push_log_resp_(const common::ObAddr &server, const int64_t &msg_proposal_id, const LSN &lsn);
  inline int try_push_log_to_paxos_follower_(const int64_t curr_proposal_id,
                                             const int64_t prev_log_pid,
                                             const LSN &prev_lsn,
                                             const LSN &lsn,
                                             const LogWriteBuf &log_write_buf);
  int try_push_log_to_children_(const int64_t curr_proposal_id,
                                const int64_t prev_log_pid,
                                const LSN &prev_lsn,
                                const LSN &lsn,
                                const LogWriteBuf &log_write_buf);
  bool need_execute_fetch_(const FetchTriggerType &fetch_trigger_type);
public:
  typedef common::ObLinearHashMap<common::ObAddr, LsnTsInfo> SvrMatchOffsetMap;
  static const int64_t TMP_HEADER_SER_BUF_LEN = 256; // log header序列化的临时buffer大小
  static const int64_t APPEND_CNT_ARRAY_SIZE = 32;   // append次数统计数组的size
  static const uint64_t APPEND_CNT_ARRAY_MASK = APPEND_CNT_ARRAY_SIZE - 1;
  static const int64_t APPEND_CNT_LB_FOR_PERIOD_FREEZE = 140000;   // 切为PERIOD_FREEZE_MODE的append count下界
private:
  struct LogTaskGuard
  {
  public:
    explicit LogTaskGuard(LogSlidingWindow *sw): sw_(sw), log_id_(common::OB_INVALID_LOG_ID) { }
    // this function should be called only once for a guard object
    int get_log_task(const int64_t log_id, LogTask *&log_task);
    // revert log task manually, usually do not need call this function
    // destructor will revert it automatically
    void revert_log_task();

    ~LogTaskGuard()
    {
      revert_log_task();
    }
  private:
    LogSlidingWindow *sw_;
    int64_t log_id_;
  };
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
private:
  int64_t  palf_id_;
  common::ObAddr self_;
  FixedSlidingWindow<LogTask> sw_;
  LogChecksum checksum_;
  LogStateMgr *state_mgr_;
  LogConfigMgr *mm_;
  LogModeMgr *mode_mgr_;
  LogEngine *log_engine_;
  LogPlugins *plugins_;
  palf::PalfFSCbWrapper *palf_fs_cb_;
  LSNAllocator lsn_allocator_;
  mutable RWLock group_buffer_lock_;
  LogGroupBuffer group_buffer_;
  // Record the last submit log info.
  // It is used to submit logs sequentially, for restarting, set it as last_replay_log_id.
  mutable common::ObSpinLock last_submit_info_lock_;
  LSN last_submit_lsn_;
  LSN last_submit_end_lsn_;
  int64_t last_submit_log_id_;
  int64_t last_submit_log_pid_;
  // Record the max flushed log info.
  // max_flushed_lsn_: start lsn of max flushed log, it can be used as prev_lsn for fetching log.
  // max_flushed_end_lsn_: end lsn of max flushed log, it can be used as start_lsn for fetching log.
  // max_flushed_log_pid_: the proposal_id of max flushed log.
  mutable RWLock max_flushed_info_lock_;
  LSN max_flushed_lsn_;
  LSN max_flushed_end_lsn_;
  int64_t max_flushed_log_pid_;
  // Record committed end lsn.
  mutable common::ObSpinLock committed_info_lock_;
  LSN committed_end_lsn_;
  // Record the last log which has slided out.
  // last_slide_lsn_: it is used as prev_lsn for fetching log
  // last_slide_end_lsn_: it is used for checking all committed log slided out when fetching log.
  // last_slide_log_pid_: it is used for forward checking when receive log.
  mutable common::ObSpinLock last_slide_info_lock_;
  int64_t last_slide_log_id_;   // used by clean log
  share::SCN last_slide_scn_;
  LSN last_slide_lsn_;
  LSN last_slide_end_lsn_;
  int64_t last_slide_log_pid_;
  int64_t last_slide_log_accum_checksum_;
  // ---------------- fetch log info begin --------------------------
  // last_fetch_req_time_:
  //    record the request time of the last fetch operation.
  // last_fetch_end_lsn_:
  //    记录本轮fetch的lsn终点，根据group_buffer容量算出
  // last_fetch_max_log_id_:
  //    记录本轮fetch的log_id终点，根据sw容量算出
  // 一轮fetch的日志数量不定，达到上述任意一个条件就结束.
  //
  // last_fetch_committed_end_lsn_:
  //    记录本轮fetch拉到所有日志后的committed_end_lsn, handle_next_submit_log_()时
  //    检查如果处理到本轮fetch的最后一条日志(log_id与last_fetch_max_log_id_相等或者
  //    log_end_lsn越过了last_fetch_end_lsn_), 则更新该值.
  //
  // 流式fetch机制:
  //    日志滑出时检查自己的end_lsn是否与last_fetch_committed_end_lsn_相等，是则触发下一轮fetch,
  //    下一轮fetch的起点是(last_submit_log_id + 1).
  //
  mutable common::ObSpinLock fetch_info_lock_;
  int64_t last_fetch_req_time_;
  LSN last_fetch_end_lsn_;
  int64_t last_fetch_max_log_id_;
  LSN last_fetch_committed_end_lsn_;
  FetchTriggerType last_fetch_trigger_type_;
  // ---------------- fetch log info end --------------------------
  // used to record synchronization points for each replica
  mutable common::ObSpinLock match_lsn_map_lock_;
  SvrMatchOffsetMap match_lsn_map_;
  // last truncate lsn, protected by palf_handle_impl's wlock
  LSN last_truncate_lsn_;
  mutable int64_t cannot_fetch_log_warn_time_;
  mutable int64_t cannot_freeze_log_warn_time_;
  mutable int64_t larger_log_warn_time_;
  mutable int64_t log_life_long_warn_time_;
  mutable int64_t lc_cb_get_warn_time_;
  mutable int64_t fetch_failure_print_time_;
  common::ObThreadLease commit_log_handling_lease_;  // thread lease for handling committed logs
  common::ObThreadLease submit_log_handling_lease_;  // thread lease for handling committed logs
  // last_renew_leader_ts in fetch_log
  mutable int64_t last_fetch_log_renew_leader_ts_us_;
  int64_t end_lsn_stat_time_us_;
  // fetch log dest server for reconfirm
  common::ObAddr reconfirm_fetch_dest_;
  // whether sw is executing trucnation
  bool is_truncating_;
  // whether this replica is rebuilding
  bool is_rebuilding_;
  LSN last_rebuild_lsn_;
  LSN last_record_end_lsn_;
  ObMiniStat::ObStatItem fs_cb_cost_stat_;
  ObMiniStat::ObStatItem log_life_time_stat_;
  int64_t accum_slide_log_cnt_;
  int64_t accum_log_gen_to_freeze_cost_;
  int64_t accum_log_gen_to_submit_cost_;
  int64_t accum_log_submit_to_flush_cost_;
  int64_t accum_log_submit_to_first_ack_cost_;
  int64_t accum_log_submit_to_commit_cost_;
  int64_t accum_log_submit_to_slide_cost_;
  mutable int64_t log_slide_stat_time_;
  int64_t group_log_stat_time_us_;
  int64_t accum_log_cnt_;
  int64_t accum_group_log_size_;
  int64_t last_record_group_log_id_;
  int64_t append_cnt_array_[APPEND_CNT_ARRAY_SIZE];
  FreezeMode freeze_mode_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogSlidingWindow);
};

} // namespace palf
} // namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_LOG_SLIDING_WINDOW_
