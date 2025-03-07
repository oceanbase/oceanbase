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

#ifndef _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_
#define _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_

#include "lib/profile/ob_trace_id.h"
#include "lib/wait_event/ob_inner_sql_wait_type.h"
#include "lib/guard/ob_shared_guard.h"          // ObShareGuard
#include "lib/ash/ob_ash_bkgd_sess_inactive_guard.h"
#include "lib/container/ob_array.h"

#define ASH_PROGRAM_STR_LEN 64
#define ASH_MODULE_STR_LEN 32
#define ASH_CLIENT_ID_STR_LEN 32
#define ASH_ACTION_STR_LEN 32
#define ASH_BACKTRACE_STR_LEN 256
#define WR_ASH_SAMPLE_INTERVAL 10

namespace oceanbase
{
namespace sql
{
class ObQueryRetryASHDiagInfo;
}

namespace observer
{
class ObVirtualASH;
}

namespace common
{
class ObDiagnoseSessionInfo;
class ObDiagnoseTenantInfo;
class ObAshBuffer;
struct ObActiveSessionStat;
class ObDiagnosticInfo;
class ObRetryWaitEventInfoGuard;

// historical ob active session stat for ash.
struct ObActiveSessionStatItem
{
public:
  friend struct ObActiveSessionStat;
  friend class observer::ObVirtualASH;
  ObActiveSessionStatItem()
      : id_(0),
        tenant_id_(0),
        user_id_(0),
        session_id_(0),
        plan_id_(0),
        sample_time_(0),
        event_no_(0),
        wait_time_(0),
        p1_(0),
        p2_(0),
        p3_(0),
        plsql_entry_object_id_(OB_INVALID_ID),
        plsql_entry_subprogram_id_(OB_INVALID_ID),
        plsql_object_id_(OB_INVALID_ID),
        plsql_subprogram_id_(OB_INVALID_ID),
        time_model_(0),
        trace_id_(),
        plan_line_id_(-1),
        session_type_(BACKGROUND),
        is_wr_sample_(false),
        delta_time_(0),
        delta_cpu_time_(0),
        delta_db_time_(0),
        group_id_(0),
        tid_(0),
        plan_hash_(0),
        tx_id_(0),
        stmt_type_(0),
        tablet_id_(0),
        block_sessid_(0),
        proxy_sid_(0)
  {
    sql_id_[0] = '\0';
    top_level_sql_id_[0] = '\0';
    plsql_entry_subprogram_name_[0] = '\0';
    plsql_subprogram_name_[0] = '\0';
#if !defined(NDEBUG)
    bt_[0] = '\0';
#endif
    program_[0] = '\0';
    module_[0] = '\0';
    action_[0] = '\0';
    client_id_[0] = '\0';
  }
  ~ObActiveSessionStatItem() = default;
public:
  enum SessionType : bool
  {
    FOREGROUND = 0,
    BACKGROUND = 1
  };
public:
  uint64_t id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t session_id_;
  uint64_t plan_id_;
  int64_t sample_time_; // sample time
  int64_t event_no_; // wait event no
  uint64_t wait_time_; // how long the event waited in total
  uint64_t p1_; // event parameter 1
  uint64_t p2_; // event parameter 2
  uint64_t p3_; // event parameter 3
  int64_t plsql_entry_object_id_; // top pl object id
  int64_t plsql_entry_subprogram_id_; // top pl subprogram id
  int64_t plsql_object_id_; // current object id
  int64_t plsql_subprogram_id_; // current subprogram id
  union {
    uint64_t time_model_;  // phase of execution bitmap
    struct
    {
      uint64_t in_parse_ : 1;
      uint64_t in_pl_parse_ : 1;
      uint64_t in_get_plan_cache_ : 1;
      uint64_t in_sql_optimize_ : 1;
      uint64_t in_sql_execution_ : 1;
      uint64_t in_px_execution_ : 1;
      uint64_t in_sequence_load_ : 1;
      uint64_t in_committing_ : 1;
      uint64_t in_storage_read_ : 1;
      uint64_t in_storage_write_ : 1;
      uint64_t in_das_remote_exec_ : 1;
      uint64_t in_plsql_compilation_ : 1;
      uint64_t in_plsql_execution_ : 1;
      uint64_t in_filter_rows_ : 1;
      uint64_t in_rpc_encode_ : 1;
      uint64_t in_rpc_decode_ : 1;
      uint64_t in_connection_mgr_ : 1;
      uint64_t in_check_row_confliction_: 1;
      uint64_t in_deadlock_row_register_ : 1; //register rowkey info to deadlock manager
      uint64_t in_check_tx_status_ : 1;
    };
  };

public:
  common::ObCurTraceId::TraceId trace_id_;
  int32_t plan_line_id_; // which SQL operater the session is processing when sampling
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char top_level_sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char plsql_entry_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  char plsql_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  SessionType session_type_; // false=0, FOREGROUND, true=1, BACKGROUND
  bool is_wr_sample_;  // true represents this node should be sampled into wr.
  int64_t delta_time_;
  int64_t delta_cpu_time_;
  int64_t delta_db_time_;
  int32_t group_id_;
  int64_t tid_;  // record current tid for cpu time verification
  int64_t plan_hash_;
  int64_t tx_id_;
  int64_t stmt_type_;
  int64_t tablet_id_;
  int64_t block_sessid_;
  uint64_t proxy_sid_; //proxy session id
  char program_[ASH_PROGRAM_STR_LEN];
  char module_[ASH_MODULE_STR_LEN];
  char action_[ASH_ACTION_STR_LEN];
  char client_id_[ASH_CLIENT_ID_STR_LEN];
#if !defined(NDEBUG)
  char bt_[ASH_BACKTRACE_STR_LEN];
#endif
  TO_STRING_KV(K_(tenant_id), K_(session_id), "event id", OB_WAIT_EVENTS[event_no_].event_id_,
      "event", OB_WAIT_EVENTS[event_no_].event_name_, K_(wait_time), K_(time_model), K_(trace_id),
      K_(plan_line_id), K_(sql_id), K_(top_level_sql_id), K_(plsql_entry_subprogram_name),
      K_(plsql_subprogram_name), K_(session_type), K_(is_wr_sample), K_(delta_time),
      K_(delta_cpu_time), K_(delta_db_time), K_(program), K_(module), K_(action), K_(client_id),
#if !defined(NDEBUG)
      K_(bt),
#endif
      K_(group_id), K_(tid), K_(plan_hash), K_(tx_id), K_(stmt_type), K_(tablet_id), K_(block_sessid));
};

// record run-time stat for each OB session
struct ObActiveSessionStat : public ObActiveSessionStatItem
{
  friend class ObRetryWaitEventInfoGuard;
public:
  ObActiveSessionStat()
      : ObActiveSessionStatItem(),
        last_touch_ts_(0),
        last_inactive_ts_(0),
        wait_event_begin_ts_(0),
        total_idle_wait_time_(0),
        total_non_idle_wait_time_(0),
        prev_idle_wait_time_(0),
        prev_non_idle_wait_time_(0),
        total_cpu_time_(0),
        is_active_session_(false),
        inner_sql_wait_type_id_(ObInnerSqlWaitTypeId::NULL_INNER_SQL),
        pcode_(0),
        tm_idle_time_(0),
        retry_wait_event_no_(0),
        retry_wait_event_p1_(0),
        retry_wait_event_p2_(0),
        retry_wait_event_p3_(0),
        retry_plan_line_id_(-1),
        need_calc_wait_event_end_(false),
        last_query_exec_use_time_us_(0),
        curr_query_start_time_(0),
        last_das_task_exec_use_time_us_(0),
        curr_das_task_start_time_(0),
        fixup_index_(-1),
        fixup_ash_buffer_(),
        query_retry_ash_diag_info_ptr_(nullptr)
  {}
  ~ObActiveSessionStat() = default;
  void fixup_last_stat(ObWaitEventDesc &desc);
  void set_fixup_buffer(common::ObSharedGuard<ObAshBuffer> &ash_buffer);
  void set_fixup_index(int64_t index)
  {
    fixup_index_ = index;
  }

  void set_event(int64_t event_no, uint64_t p1, uint64_t p2, uint64_t p3)
  {
    event_no_ = event_no;
    p1_ = p1;
    p2_ = p2;
    p3_ = p3;
#if !defined(NDEBUG)
    const char *bt = lbt();
    int64_t size = std::min(sizeof(bt_) - 1, STRLEN(bt));
    MEMCPY(bt_, bt, size);
    bt_[size] = '\0';
#endif
  }
  void reset_event()
  {
    event_no_ = 0;
    p1_ = 0;
    p2_ = 0;
    p3_ = 0;
    wait_event_begin_ts_ = 0;
#if !defined(NDEBUG)
    MEMSET(bt_, 0, sizeof(bt_));
#endif
  }
  void set_sess_active();
  void set_sess_inactive();
  void accumulate_tm_idle_time();
  static void calc_db_time(
      ObDiagnosticInfo *di, const int64_t sample_time, const int64_t tsc_sample_time);
  // timestamp for last ash sample taken place. could be optimized to rdtsc()
  // FIXME:but should check rdtsc_is_supported on bootstrap.
  void set_ash_waiting(const int64_t event_no,
                       const int64_t p1 = 0 ,
                       const int64_t p2 = 0,
                       const int64_t p3 = 0);
  void finish_ash_waiting();
  inline sql::ObQueryRetryASHDiagInfo* get_retry_ash_diag_info_ptr() { return query_retry_ash_diag_info_ptr_; }
  void begin_retry_wait_event(const int64_t retry_wait_event_no,
                              const int64_t retry_wait_event_p1,
                              const int64_t retry_wait_event_p2,
                              const int64_t retry_wait_event_p3);
  void end_retry_wait_event();

  void begin_row_lock_wait_event();
  void end_row_lock_wait_event();
  static void calc_retry_wait_event(ObActiveSessionStat &stat, const int64_t sample_time);
  inline void record_cur_query_start_ts(bool is_in_retry)
  {
    curr_query_start_time_ = common::ObTimeUtility::current_time();
    need_calc_wait_event_end_ = is_in_retry;
  }
  inline void record_last_query_exec_use_time_us()
  {
    last_query_exec_use_time_us_ = common::ObTimeUtility::current_time() - curr_query_start_time_;
    need_calc_wait_event_end_ = false;
  }
  inline bool can_start_das_retry() const {
    return retry_wait_event_no_ > 0;
  }
  inline void record_cur_das_test_start_ts(const int64_t last_das_task_exec_use_time_us, bool is_in_retry) {
    curr_das_task_start_time_ = common::ObTimeUtility::current_time();
    last_das_task_exec_use_time_us_ = last_das_task_exec_use_time_us;
    need_calc_wait_event_end_ = is_in_retry;
  }
  inline void stop_das_retry_wait_event() {
    end_retry_wait_event();
  }
  inline bool is_in_row_lock_wait() {
    return retry_wait_event_no_ == ObWaitEventIds::ROW_LOCK_WAIT;
  }

private:
  inline void set_retry_ash_diag_info_ptr(sql::ObQueryRetryASHDiagInfo* query_retry_ash_diag_info_ptr) {
    query_retry_ash_diag_info_ptr_ = query_retry_ash_diag_info_ptr;
  }
public:
  int64_t last_touch_ts_; CACHE_ALIGNED // the timestamp of the last sampling or creation
  int64_t last_inactive_ts_ CACHE_ALIGNED; //the timestamp when the session was last in an inactive state
  int64_t wait_event_begin_ts_;
  uint64_t total_idle_wait_time_; // idle wait time in total
  uint64_t total_non_idle_wait_time_; // total non-idle wait time in total
  uint64_t prev_idle_wait_time_;
  uint64_t prev_non_idle_wait_time_;
  uint64_t total_cpu_time_;  // total cpu time since last ash sample. for cpu-time verification.
  bool is_active_session_ CACHE_ALIGNED;
  ObInnerSqlWaitTypeId inner_sql_wait_type_id_;
  int pcode_ CACHE_ALIGNED;
  int64_t tm_idle_time_; // the idle time between two sampling intervals.
  int64_t retry_wait_event_no_;
  int64_t retry_wait_event_p1_;
  int64_t retry_wait_event_p2_;
  int64_t retry_wait_event_p3_;
  int64_t retry_plan_line_id_;
  bool need_calc_wait_event_end_;
  int64_t last_query_exec_use_time_us_;
  int64_t curr_query_start_time_; //only used to calc retry wait event time
  int64_t last_das_task_exec_use_time_us_;
  int64_t curr_das_task_start_time_;

  INHERIT_TO_STRING_KV("ObActiveSessionStatItem", ObActiveSessionStatItem, K_(last_touch_ts),
      K_(wait_event_begin_ts), K_(total_idle_wait_time), K_(total_non_idle_wait_time),
      K_(prev_idle_wait_time), K_(prev_non_idle_wait_time), K_(total_cpu_time),
      K_(is_active_session), K_(inner_sql_wait_type_id), K_(pcode), K_(tm_idle_time),
      K_(fixup_index), K_(fixup_ash_buffer), K_(retry_wait_event_no), K_(retry_plan_line_id));

private:

  // for wait time fix-up.
  // Fixes-up values unknown at sampling time
  // So we collect the wait time after the event finish
  int64_t fixup_index_;
  common::ObSharedGuard<ObAshBuffer> fixup_ash_buffer_;
  sql::ObQueryRetryASHDiagInfo* query_retry_ash_diag_info_ptr_;
};

class ObAshBuffer
{
public:
  ObAshBuffer() : write_pos_(0), read_pos_(0), buffer_() {}
  ~ObAshBuffer()
  {
    OB_LOG(INFO, "successfully released one ash buffer", K(buffer_.count()), "size", buffer_.count() * sizeof(sizeof(ObActiveSessionStatItem)), K(this));
  }
  const ObActiveSessionStatItem &get(int64_t pos) const;
  int64_t copy_from_ash_buffer(const ObActiveSessionStatItem &stat);
  int64_t append(const ObActiveSessionStat &stat);
  void fixup_stat(int64_t index, const ObWaitEventDesc &desc);
  int64_t write_pos() const { return write_pos_; }
  int64_t read_pos() const { return read_pos_; }
  int64_t size() const { return buffer_.size(); }
  int64_t free_slots_num() const { return buffer_.size() - min(buffer_.size(), write_pos_ - read_pos_); }
  inline void set_label(const lib::ObLabel &label) { return buffer_.set_label(label); }
  inline void set_tenant_id(const uint64_t &tenant_id) { return buffer_.set_tenant_id(tenant_id); }
  void set_read_pos(int64_t pos);
  int prepare_allocate(int64_t capacity) { return buffer_.prepare_allocate(capacity); }
  TO_STRING_KV(K_(write_pos), K(buffer_.count()));
private:
  int64_t write_pos_; // current write index for ash stat item. Not written yet.
  // When copy one ash buffer to another, read_pos_ will not be copied.
  int64_t read_pos_;  // current read index for wr snapshot. Not read yet.
  common::ObArray<ObActiveSessionStatItem> buffer_;
};

class ObActiveSessionGuard
{
public:
  ObActiveSessionGuard() = default;
  ~ObActiveSessionGuard() = default;
  // reset ashstat to another thread local variable durning session active period.
  static ObActiveSessionStat *get_stat() __attribute__ ((deprecated));
  static void set_sess_active();
  static void set_sess_inactive();
private:
  static ObActiveSessionStat *&get_stat_ptr();
  DISALLOW_COPY_AND_ASSIGN(ObActiveSessionGuard);
};

class ObBackgroundSessionIdGenerator {
public:
  ObBackgroundSessionIdGenerator() : local_seq_(1) {}
  ~ObBackgroundSessionIdGenerator() = default;

  static ObBackgroundSessionIdGenerator &get_instance();
  static bool is_background_session_id(uint64_t session_id);
  uint64_t get_next_sess_id();
private:
  ObBackgroundSessionIdGenerator *generator_;
  volatile uint64_t local_seq_;
};

#define ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(filed, value)                                 \
  do {                                                                                      \
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                                    \
    if (OB_NOT_NULL(di) && OB_NOT_NULL(di->get_ash_stat().get_retry_ash_diag_info_ptr())) { \
      di->get_ash_stat().get_retry_ash_diag_info_ptr()->filed = value;                      \
    }                                                                                       \
  } while (0)
}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_ */
//// end of header file
