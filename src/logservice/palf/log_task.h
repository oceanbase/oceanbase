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

#ifndef OCEABASE_LOGSERVICE_LOG_TASK_
#define OCEABASE_LOGSERVICE_LOG_TASK_
#include "lib/ob_define.h"                      // Serialization
#include "lib/utility/ob_print_utils.h"         // Print*
#include "share/scn.h"
#include "fixed_sliding_window.h"
#include "log_define.h"                         // block_id_t
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
class LogGroupEntryHeader;
class LogSlidingWindow;

struct LogTaskHeaderInfo
{
  LSN begin_lsn_;
  LSN end_lsn_;
  int64_t log_id_;
  share::SCN min_scn_;
  share::SCN max_scn_;
  int64_t data_len_;             // total len without log_group_entry_header
  int64_t proposal_id_;  // leader's proposal_id when generate this log
  LSN prev_lsn_;
  int64_t prev_proposal_id_;  // 乱序收日志需要校验该值
  LSN committed_end_lsn_;     // 乱序收日志时前面空洞补齐后需要该值
  int64_t data_checksum_;        // follower收日志时校验, 本地计算accum_checksum也需要用该值
  int64_t accum_checksum_;       // follower乱序收日志时前面空洞补齐后与本地值做校验
  bool is_padding_log_;
  bool is_raw_write_;

  LogTaskHeaderInfo() { reset(); }
  ~LogTaskHeaderInfo() { reset(); }
  LogTaskHeaderInfo& operator=(const LogTaskHeaderInfo &rval);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(begin_lsn), K_(end_lsn), K_(log_id), K_(min_scn), K_(max_scn), K_(data_len), K_(proposal_id),
      K_(prev_lsn), K_(prev_proposal_id), K_(committed_end_lsn),
      K_(data_checksum), K_(accum_checksum), K_(is_padding_log), K_(is_raw_write));
};

class LogSimpleBitMap
{
public:
  LogSimpleBitMap() : val_(0) {}
  ~LogSimpleBitMap() {}
  void reset_all();
  void reset_map(const int64_t idx);
  void set_map(const int64_t idx);
  bool test_map(const int64_t idx) const;
  void reset_map_unsafe(const int64_t idx);
  void set_map_unsafe(const int64_t idx);
  bool test_map_unsafe(const int64_t idx) const;
  bool test_and_set(const int64_t idx);
  TO_STRING_KV(K_(val));
private:
  uint16_t val_;
};

class LogTask : public FixedSlidingWindowSlot
{
  enum STATE_BIT_INDEX
  {
    PRE_FILL = 0,  // only one thread can set this tag and fill group buffer
    IS_VALID = 1,    // whether this log_task is valid
    SUBMIT_LOG_EXIST = 2, // whether this log_task's data is in group_buffer
    IS_FREEZED = 3,  // whether this log_task is freezed
    PRE_SUBMIT = 4,  // only one thread can set this tag and calculate accum_checksum
    PRE_SLIDE = 5,     // only one therad can slide a log_task
  };
public:
  LogTask();
  virtual ~LogTask();
  void destroy();
  virtual void reset();
  virtual bool can_be_slid();
public:
  void lock() const {
    lock_.wrlock(common::ObLatchIds::CLOG_TASK_LOCK);
  }
  void unlock() const {
    lock_.unlock();
  }
  void inc_update_max_scn(const share::SCN &scn);
  void update_data_len(const int64_t data_len);
  void set_end_lsn(const LSN &end_lsn);
  int try_freeze(const LSN &end_lsn);
  int try_freeze_by_myself();
  void set_freezed();
  bool is_freezed() const;
  bool try_pre_fill();
  void reset_pre_fill();
  bool try_pre_submit();
  bool is_pre_submit() const;
  void set_valid();
  bool is_valid() const;
  void reset_pre_submit();
  void reset_submit_log_exist();
  bool set_submit_log_exist();
  bool is_submit_log_exist() const;
  bool try_pre_slide();
  bool is_pre_slide();
  void reset_pre_slide();
  int64_t ref(const int64_t val, const bool is_append_log = true);
  int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  int64_t get_log_cnt() const { return ATOMIC_LOAD(&log_cnt_); }
  int set_initial_header_info(const LogTaskHeaderInfo &header_info);
  int update_header_info(const LSN &committed_end_lsn, const int64_t accum_checksum);
  int set_group_header(const LSN &lsn, const share::SCN &scn, const LogGroupEntryHeader &group_entry_header);
  // update group log data_checksum
  void set_group_log_checksum(const int64_t data_checksum);
  void set_prev_lsn(const LSN &prev_lsn);
  LSN get_prev_lsn() const { return header_.prev_lsn_; }
  void set_prev_log_proposal_id(const int64_t &prev_log_proposal_id);
  int64_t get_prev_proposal_id() const { return header_.prev_proposal_id_; }
  LogTaskHeaderInfo get_header_info() const { return header_; }
  bool is_padding_log() const { return header_.is_padding_log_; }
  bool is_raw_write() const { return header_.is_raw_write_; }
  int64_t get_data_len() const { return ATOMIC_LOAD(&(header_.data_len_)); }
  int64_t get_log_id() const { return header_.log_id_; }
  const share::SCN get_min_scn() const {return header_.min_scn_; }
  const share::SCN get_max_scn() const { return header_.max_scn_; }
  int64_t get_proposal_id() const { return header_.proposal_id_; }
  LSN get_begin_lsn() const { return header_.begin_lsn_; }
  LSN get_end_lsn() const { return header_.end_lsn_; }
  int64_t get_accum_checksum() const { return header_.accum_checksum_; }
  void set_freeze_ts(const int64_t ts);
  void set_submit_ts(const int64_t ts);
  void set_flushed_ts(const int64_t ts);
  int64_t get_gen_ts() const { return ATOMIC_LOAD(&(gen_ts_)); }
  int64_t get_freeze_ts() const { return ATOMIC_LOAD(&(freeze_ts_)); }
  int64_t get_submit_ts() const { return ATOMIC_LOAD(&(submit_ts_)); }
  int64_t get_flushed_ts() const { return ATOMIC_LOAD(&(flushed_ts_)); }
  TO_STRING_KV(K_(header), K_(state_map), K_(ref_cnt),
      K_(gen_ts), K_(freeze_ts), K_(submit_ts), K_(flushed_ts),
      "gen_to_freeze cost time", freeze_ts_ - gen_ts_,
      "gen_to_submit cost time", submit_ts_ - gen_ts_,
      "submit_to_flush cost time", ((flushed_ts_ - submit_ts_) < 0 ? 0 : (flushed_ts_ - submit_ts_))
  );
private:
  int try_freeze_(const LSN &end_lsn);
private:
  LogSimpleBitMap state_map_;
  LogTaskHeaderInfo header_;
  int64_t ref_cnt_;
  int64_t log_cnt_;  // log_entry count
  mutable int64_t gen_ts_;
  mutable int64_t freeze_ts_;
  mutable int64_t submit_ts_;
  mutable int64_t flushed_ts_;
  mutable common::ObLatch lock_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
