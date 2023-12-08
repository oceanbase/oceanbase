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
#define WR_ASH_SAMPLE_INTERVAL 10

namespace oceanbase
{
namespace common
{
class ObDiagnoseSessionInfo;
class ObDiagnoseTenantInfo;

class ObAshBuffer;

// historical ob active session stat for ash.
struct ObActiveSessionStatItem
{
public:
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
        session_type_(false),
        is_wr_sample_(false),
        delta_time_(0),
        delta_cpu_time_(0),
        delta_db_time_(0)
  {
    sql_id_[0] = '\0';
    top_level_sql_id_[0] = '\0';
    plsql_entry_subprogram_name_[0] = '\0';
    plsql_subprogram_name_[0] = '\0';
#ifndef NDEBUG
    bt_[0] = '\0';
#endif
    program_[0] = '\0';
    module_[0] = '\0';
  }
  ~ObActiveSessionStatItem() = default;
  void reuse()
  {
    user_id_ = 0;
    session_id_ = 0;
    plan_id_ = 0;
    sql_id_[0] = '\0';
    top_level_sql_id_[0] = '\0';
    time_model_ = 0;
    plsql_entry_object_id_ = OB_INVALID_ID;
    plsql_entry_subprogram_id_ = OB_INVALID_ID;
    plsql_object_id_ = OB_INVALID_ID;
    plsql_subprogram_id_ = OB_INVALID_ID;
    plsql_entry_subprogram_name_[0] = '\0';
    plsql_subprogram_name_[0] = '\0';
#ifndef NDEBUG
    bt_[0] = '\0';
#endif
    delta_time_ = 0;
    delta_cpu_time_ = 0;
    delta_db_time_ = 0;
    program_[0] = '\0';
    module_[0] = '\0';
  }
public:
  enum SessionType
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
    uint64_t time_model_; // phase of execution bitmap
    struct {
      uint64_t in_parse_          : 1;
      uint64_t in_pl_parse_       : 1;
      uint64_t in_get_plan_cache_ : 1;
      uint64_t in_sql_optimize_   : 1;
      uint64_t in_sql_execution_  : 1;
      uint64_t in_px_execution_   : 1;
      uint64_t in_sequence_load_  : 1;
      uint64_t in_committing_     : 1;
      uint64_t in_storage_read_   : 1;
      uint64_t in_storage_write_  : 1;
      uint64_t in_das_remote_exec_: 1;
      uint64_t in_plsql_compilation_  : 1;
      uint64_t in_plsql_execution_  : 1;
      uint64_t in_filter_rows_: 1;
    };
  };

  common::ObCurTraceId::TraceId trace_id_;
  int32_t plan_line_id_; // which SQL operater the session is processing when sampling
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char top_level_sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char plsql_entry_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  char plsql_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  bool session_type_; // false=0, FOREGROUND, true=1, BACKGROUND
  bool is_wr_sample_;  // true represents this node should be sampled into wr.
  int64_t delta_time_;
  int64_t delta_cpu_time_;
  int64_t delta_db_time_;
  char program_[ASH_PROGRAM_STR_LEN];
  char module_[64];
#ifndef NDEBUG
  char bt_[256];
#endif
  TO_STRING_KV(K_(tenant_id), K_(session_id), "event id", OB_WAIT_EVENTS[event_no_].event_id_,
      "event", OB_WAIT_EVENTS[event_no_].event_name_, K_(wait_time), K_(time_model), K_(trace_id), K_(delta_time),
      K_(delta_cpu_time), K_(delta_db_time), K_(program));
};

// record run-time stat for each OB session
struct ObActiveSessionStat : public ObActiveSessionStatItem
{
public:
  ObActiveSessionStat()
      : ObActiveSessionStatItem(),
        last_ts_(0),
        wait_event_begin_ts_(0),
        total_idle_wait_time_(0),
        total_non_idle_wait_time_(0),
        prev_idle_wait_time_(0),
        prev_non_idle_wait_time_(0),
        total_cpu_time_(0),
        tid_(0),
        di_(nullptr),
        is_background_(false),
        is_bkgd_active_(true),
        inner_sql_wait_type_id_(ObInnerSqlWaitTypeId::NULL_INNER_SQL),
        is_remote_inner_sql_(false),
        pcode_(0),
        bkgd_elapse_time_(0),
        last_stat_(nullptr),
        fixup_index_(-1),
        fixup_ash_buffer_(),
        prev_stat_(nullptr)
  {}
  ~ObActiveSessionStat() = default;
  void fixup_last_stat(ObWaitEventDesc &desc);
  void set_fixup_buffer(common::ObSharedGuard<ObAshBuffer> &ash_buffer);
  void set_fixup_index(int64_t index)
  {
    fixup_index_ = index;
  }
  int set_prev_stat(ObActiveSessionStat *stat)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(stat)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "Unexpected null prev stat", K(ret));
    } else {
      prev_stat_ = stat;
    }
    return ret;
  }

  ObActiveSessionStat* get_prev_stat() {
    return prev_stat_;
  }

  void reuse()
  {
    ObActiveSessionStatItem::reuse();
    last_ts_ = 0;
    wait_event_begin_ts_ = 0;
    total_idle_wait_time_ = 0;
    total_non_idle_wait_time_ = 0;
    prev_idle_wait_time_ = 0;
    prev_non_idle_wait_time_ = 0;
    total_cpu_time_ = 0;
    tid_ = 0;
    di_ = nullptr;
    fixup_index_ = -1;
    // NOTICE: reset of fixup_ash_buffer_ is in ObActiveSessionStat::fixup_last_stat
    // fixup_ash_buffer_.reset();
  }
  /**
   * Add ASH flags to async commit of transactions
   * In the end of async commit in func named ` ObEndTransAsyncCallback::callback() `,
   * set the ash flag named  `in_committing_` to false.
   */
  void set_async_committing();
  void finish_async_commiting();
  void set_event(int64_t event_no, uint64_t p1, uint64_t p2, uint64_t p3)
  {
    event_no_ = event_no;
    p1_ = p1;
    p2_ = p2;
    p3_ = p3;
  }
  void reset_event()
  {
    event_no_ = 0;
    p1_ = 0;
    p2_ = 0;
    p3_ = 0;
  }
  void set_bkgd_sess_active();
  void set_bkgd_sess_inactive();
  static void calc_db_time_for_background_session(ObActiveSessionStat &stat, const int64_t sample_time);
  static void calc_db_time(ObActiveSessionStat &stat, const int64_t sample_time);
  // timestamp for last ash sample taken place. could be optimized to rdtsc()
  // FIXME:but should check rdtsc_is_supported on bootstrap.
  int64_t last_ts_;
  int64_t wait_event_begin_ts_;
  uint64_t total_idle_wait_time_; // idle wait time in total
  uint64_t total_non_idle_wait_time_; // total non-idle wait time in total
  uint64_t prev_idle_wait_time_;
  uint64_t prev_non_idle_wait_time_;
  uint64_t total_cpu_time_;  // total cpu time since last ash sample. for cpu-time verification.
  int64_t tid_;  // record current tid for cpu time verification
  common::ObDiagnoseSessionInfo *di_;
  bool is_background_;
  bool is_bkgd_active_; // Identifies whether the status of the background session is active.
                        // Inactive background thread session will not be collected in ASH.
  ObInnerSqlWaitTypeId inner_sql_wait_type_id_;
  bool is_remote_inner_sql_;
  int pcode_;
  int64_t bkgd_elapse_time_; // for backgorund elapse time.

  INHERIT_TO_STRING_KV("ObActiveSessionStatItem", ObActiveSessionStatItem, K_(last_ts),
      K_(wait_event_begin_ts), K_(total_idle_wait_time), K_(total_non_idle_wait_time),
      K_(prev_idle_wait_time), K_(prev_non_idle_wait_time), K_(total_cpu_time), K_(is_background),
      K_(is_bkgd_active), K_(inner_sql_wait_type_id), K_(is_remote_inner_sql));

private:
  // for wait time fix-up.
  // Fixes-up values unknown at sampling time
  // So we collect the wait time after the event finish
  ObActiveSessionStatItem *last_stat_;
  int64_t fixup_index_;
  common::ObSharedGuard<ObAshBuffer> fixup_ash_buffer_;
  // `prev_stat_` is for inner session nesting
  ObActiveSessionStat *prev_stat_;
};

class ObAshBuffer
{
public:
  ObAshBuffer() : write_pos_(0), buffer_() {}
  ~ObAshBuffer()
  {
    OB_LOG(INFO, "successfully released one ash buffer", K(buffer_.count()), "size", buffer_.count() * sizeof(sizeof(ObActiveSessionStatItem)), K(this));
  }
  const ObActiveSessionStatItem &get(int64_t pos) const;
  int64_t copy_from_ash_buffer(const ObActiveSessionStatItem &stat);
  int64_t append(const ObActiveSessionStatItem &stat);
  void fixup_stat(int64_t index, const ObWaitEventDesc &desc);
  int64_t write_pos() const { return write_pos_; };
  int64_t size() const { return buffer_.size(); };
  inline void set_label(const lib::ObLabel &label) { return buffer_.set_label(label); }
  inline void set_tenant_id(const uint64_t &tenant_id) { return buffer_.set_tenant_id(tenant_id); }
  int prepare_allocate(int64_t capacity) { return buffer_.prepare_allocate(capacity); }
  TO_STRING_KV(K_(write_pos), K(buffer_.count()));
private:
  int64_t write_pos_; // current write index for ash stat item. Not written yet.
  common::ObArray<ObActiveSessionStatItem> buffer_;
};

class ObActiveSessionGuard
{
public:
  ObActiveSessionGuard() = default;
  ~ObActiveSessionGuard() = default;
  // When Worker execution done, reset the thread local ash_stat_ to dummy_stat_
  // so that we can ensure the session stat will not be accessed after this thread switch
  // to any other task
  static void setup_default_ash();
  // set ash_stat in session to the thread local ash_stat_
  static void setup_ash(ObActiveSessionStat &stat);
  // reset ashstat to another thread local variable durning session active period.
  static void resetup_ash(ObActiveSessionStat &stat);
  static ObActiveSessionStat &get_stat();
  static void setup_thread_local_ash();
  static void set_bkgd_sess_active();
  static void set_bkgd_sess_inactive();
  static thread_local ObActiveSessionStat thread_local_stat_;
  static ObActiveSessionStat dummy_stat_;
private:
  static ObActiveSessionStat *&get_stat_ptr();
  DISALLOW_COPY_AND_ASSIGN(ObActiveSessionGuard);
};

class ObRPCActiveGuard
{
public:
  ObRPCActiveGuard(int pcode);
  ~ObRPCActiveGuard();
private:
  int pcode_;
};

#define DEF_ASH_FLAGS_SETTER_GUARD(ash_flag_type)                                                  \
  class ObActiveSession_##ash_flag_type##_FlagSetterGuard                                          \
  {                                                                                                \
    public:                                                                                        \
      ObActiveSession_##ash_flag_type##_FlagSetterGuard() {                                        \
        ObActiveSessionGuard::get_stat().ash_flag_type##_ = true;                                  \
      }                                                                                            \
      ~ObActiveSession_##ash_flag_type##_FlagSetterGuard() {                                       \
        ObActiveSessionGuard::get_stat().ash_flag_type##_ = false;                                 \
      }                                                                                            \
    private:                                                                                       \
      DISALLOW_COPY_AND_ASSIGN(ObActiveSession_##ash_flag_type##_FlagSetterGuard);                 \
  };

DEF_ASH_FLAGS_SETTER_GUARD(in_parse)
DEF_ASH_FLAGS_SETTER_GUARD(in_pl_parse)
DEF_ASH_FLAGS_SETTER_GUARD(in_get_plan_cache)
DEF_ASH_FLAGS_SETTER_GUARD(in_sql_optimize)
DEF_ASH_FLAGS_SETTER_GUARD(in_sql_execution)
DEF_ASH_FLAGS_SETTER_GUARD(in_px_execution)
DEF_ASH_FLAGS_SETTER_GUARD(in_sequence_load)
DEF_ASH_FLAGS_SETTER_GUARD(in_committing)
DEF_ASH_FLAGS_SETTER_GUARD(in_storage_read)
DEF_ASH_FLAGS_SETTER_GUARD(in_storage_write)
DEF_ASH_FLAGS_SETTER_GUARD(in_filter_rows)

#undef DEF_ASH_FLAGS_SETTER_GUARD

#define ACTIVE_SESSION_FLAG_SETTER_GUARD(ash_flag_type)                                            \
  ObActiveSession_##ash_flag_type##_FlagSetterGuard _ash_flag_setter_guard;

class ObASHSetInnerSqlWaitGuard {
public:
  ObASHSetInnerSqlWaitGuard(ObInnerSqlWaitTypeId id)
  {
    prev_id_ = ObActiveSessionGuard().get_stat().inner_sql_wait_type_id_;
    ObActiveSessionGuard().get_stat().inner_sql_wait_type_id_ = id;
  };
  ~ObASHSetInnerSqlWaitGuard()
  {
    ObActiveSessionGuard().get_stat().inner_sql_wait_type_id_ = prev_id_;
  };

private:
  ObInnerSqlWaitTypeId prev_id_;
};

class ObBackgroundSessionIdGenerator {
public:
  ObBackgroundSessionIdGenerator() : local_seq_(1) {}
  ~ObBackgroundSessionIdGenerator() = default;

  static ObBackgroundSessionIdGenerator &get_instance();
  uint64_t get_next_sess_id();
private:
  ObBackgroundSessionIdGenerator *generator_;
  uint64_t local_seq_;
};

}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_GUARD_H_ */
//// end of header file
