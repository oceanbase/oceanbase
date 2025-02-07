/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DIAGNOSTIC_INFO_GUARD_H_
#define OB_DIAGNOSTIC_INFO_GUARD_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnostic_info.h"
#include "lib/stat/ob_diagnostic_info_summary.h"
#include "lib/wait_event/ob_inner_sql_wait_type.h"

namespace oceanbase
{

#define GET_DIAGNOSTIC_INFO                                       \
  if (oceanbase::common::ObLocalDiagnosticInfo::get() != nullptr) \
  oceanbase::common::ObLocalDiagnosticInfo::get()

namespace observer
{
class ObInnerSqlWaitGuard;
}
namespace sql
{
class ObEndTransAsyncCallback;
}

namespace common
{

class ObDiagnosticInfo;
class ObDiagnosticInfoSlot;
class ObDiagnosticInfoContainer;
class ObDiagnosticInfoSwitchGuard;
class ObLatchStat;

class ObBackGroundSessionGuard
{
public:
  ObBackGroundSessionGuard(int64_t tenant_id, int64_t group_id);
  ~ObBackGroundSessionGuard();

private:
  ObDiagnosticInfo *di_;
  bool prev_value_;
  ObDiagnosticInfoContainer *dic_;
};

class ObLocalDiagnosticInfo
{
public:
  friend class ObTenantDiagnosticInfoSummaryGuard;
  friend class ObDiagnosticInfoSwitchGuard;
  friend class ObBackGroundSessionGuard;
  friend class observer::ObInnerSqlWaitGuard;
  friend class sql::ObEndTransAsyncCallback;
  DISABLE_COPY_ASSIGN(ObLocalDiagnosticInfo);
  // WARN: should check nullptr when call ObDiagnosticInfo::get().
  static inline ObDiagnosticInfo *get()
  {
    return get_instance().get_diagnostic_ptr();
  }
  static int aggregate_diagnostic_info_summary(ObDiagnosticInfo *di);
  static int revert_diagnostic_info(ObDiagnosticInfo *di);
  static int return_diagnostic_info(ObDiagnosticInfo *di);
  static inline void add_stat(ObStatEventIds::ObStatEventIdEnum stat_no, int64_t value)
      __attribute__((always_inline))
  {
    ObLocalDiagnosticInfo &instance = get_instance();
    ObDiagnosticInfoSlot *slot = instance.slot_;
    if (OB_UNLIKELY(nullptr != slot)) {
      slot->atomic_add_stat(stat_no, value);
    } else {
      ObDiagnosticInfo *di = instance.get_diagnostic_ptr();
      if (OB_NOT_NULL(di)) {
        di->add_stat(stat_no, value);
      }
    }
  }
  static ObLatchStat *get_latch_stat(int64_t latch_id);
  static void set_thread_name(const char *name);
  static void set_thread_name(uint64_t tenant_id, const char *name);
  static void set_service_action(const char *program, const char *module, const char *action);
  static inline int inc_ref(ObDiagnosticInfo *di) __attribute__((always_inline))
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(di)) {
      ATOMIC_INC(&di->ref_cnt_);
    }
    return ret;
  }
  static int dec_ref(ObDiagnosticInfo *di);
private:
  ObLocalDiagnosticInfo();
  ~ObLocalDiagnosticInfo() = default;
  static inline ObLocalDiagnosticInfo &get_instance()
  {
    static thread_local ObLocalDiagnosticInfo di;
    return di;
  }
  inline ObDiagnosticInfo *&get_diagnostic_ptr()
  {
    return di_ptr_;
  }
  static void setup_di_slot(ObDiagnosticInfoSlot *di_slot)
  {
    get_instance().slot_ = di_slot;
  }
  static void reset_di_slot()
  {
    get_instance().slot_ = nullptr;
  }
  static inline void reset_diagnostic_info()
  {
    get_instance().get_diagnostic_ptr() = nullptr;
  }
  static inline void setup_diagnostic_info(ObDiagnosticInfo *di)
  {
    if (OB_NOT_NULL(di)) {
      get_instance().get_diagnostic_ptr() = di;
      di->get_ash_stat().tid_ = GETTID();
    } else {
      COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "set nullptr to local diagnostic info", K(lbt()));
    }
  }
  ObDiagnosticInfo *di_ptr_ CACHE_ALIGNED;
  ObDiagnosticInfoSlot *slot_ CACHE_ALIGNED;
};

class ObTenantDiagnosticInfoSummaryGuard
{
public:
  explicit ObTenantDiagnosticInfoSummaryGuard(int64_t tenant_id, int64_t group_id = 0,
      bool using_global = false /*ATTENTION: do not set this unless you know what you are doing*/);
  explicit ObTenantDiagnosticInfoSummaryGuard(ObDiagnosticInfoSlot *slot);
  ~ObTenantDiagnosticInfoSummaryGuard();
  DISABLE_COPY_ASSIGN(ObTenantDiagnosticInfoSummaryGuard);

private:
  bool need_restore_slot_;
  ObDiagnosticInfoSlot *prev_slot_;
};

class ObDiagnosticInfoSwitchGuard
{
public:
  explicit ObDiagnosticInfoSwitchGuard(ObDiagnosticInfo *di);
  ~ObDiagnosticInfoSwitchGuard();
  DISABLE_COPY_ASSIGN(ObDiagnosticInfoSwitchGuard);

private:
  ObDiagnosticInfo *prev_di_;
  ObDiagnosticInfo *cur_di_;
  bool di_switch_success_;
#ifdef ENABLE_DEBUG_LOG
  bool leak_check_;
#endif
  bool prev_value_;
};

class ObRetryWaitEventInfoGuard {
public:
  ObRetryWaitEventInfoGuard(sql::ObSQLSessionInfo &session);
  ~ObRetryWaitEventInfoGuard();

private:
  bool is_switch_;
  sql::ObQueryRetryASHDiagInfo *parent_ptr_;
};

#define DEF_ASH_FLAGS_SETTER_GUARD(ash_flag_type)                                \
  class ObActiveSession_##ash_flag_type##_FlagSetterGuard                        \
  {                                                                              \
  public:                                                                        \
    ObActiveSession_##ash_flag_type##_FlagSetterGuard()                          \
    {                                                                            \
      GET_DIAGNOSTIC_INFO->get_ash_stat().ash_flag_type##_ = true;               \
    }                                                                            \
    ~ObActiveSession_##ash_flag_type##_FlagSetterGuard()                         \
    {                                                                            \
      GET_DIAGNOSTIC_INFO->get_ash_stat().ash_flag_type##_ = false;              \
    }                                                                            \
                                                                                 \
  private:                                                                       \
    DISALLOW_COPY_AND_ASSIGN(ObActiveSession_##ash_flag_type##_FlagSetterGuard); \
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
DEF_ASH_FLAGS_SETTER_GUARD(in_rpc_encode)
DEF_ASH_FLAGS_SETTER_GUARD(in_rpc_decode)
DEF_ASH_FLAGS_SETTER_GUARD(in_connection_mgr)
DEF_ASH_FLAGS_SETTER_GUARD(in_check_row_confliction)


#undef DEF_ASH_FLAGS_SETTER_GUARD

#define ACTIVE_SESSION_FLAG_SETTER_GUARD(ash_flag_type)                                            \
  ObActiveSession_##ash_flag_type##_FlagSetterGuard _ash_flag_setter_guard;

class ObASHSetInnerSqlWaitGuard {
public:
  ObASHSetInnerSqlWaitGuard(ObInnerSqlWaitTypeId id)
  {
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
    if (OB_NOT_NULL(di)) {
      prev_id_ = di->get_ash_stat().inner_sql_wait_type_id_;
      di->get_ash_stat().inner_sql_wait_type_id_ = id;
    }
  };
  ~ObASHSetInnerSqlWaitGuard()
  {
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
    if (OB_NOT_NULL(di)) {
      di->get_ash_stat().inner_sql_wait_type_id_ = prev_id_;
    }
  };

private:
  ObInnerSqlWaitTypeId prev_id_;
};

} /* namespace common */
} /* namespace oceanbase */

#define EVENT_ADD(stat_no, value)                                                         \
  do {                                                                                    \
    if (oceanbase::lib::is_diagnose_info_enabled()) {                                     \
      ObLocalDiagnosticInfo::add_stat(oceanbase::common::ObStatEventIds::stat_no, value); \
    }                                                                                     \
  } while (0)

#define EVENT_TENANT_ADD(stat_no, value, tenant_id)                \
  oceanbase::common::ObTenantDiagnosticInfoSummaryGuard g(tenant_id, 0); \
  EVENT_ADD(stat_no, value);

#define EVENT_INC(stat_no) EVENT_ADD(stat_no, 1)

#define EVENT_TENANT_INC(stat_no, tenant_id) EVENT_TENANT_ADD(stat_no, 1, tenant_id)

#define EVENT_DEC(stat_no) EVENT_ADD(stat_no, -1)

#define WAIT_BEGIN(event_no, timeout_ms, p1, p2, p3, is_atomic)          \
  do {                                                                   \
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                 \
    if (oceanbase::lib::is_diagnose_info_enabled() && OB_NOT_NULL(di)) { \
      need_record_ = true;                                               \
      di->begin_wait_event(event_no, timeout_ms, p1, p2, p3);            \
    } else {                                                             \
      need_record_ = false;                                              \
    }                                                                    \
  } while (0)

#define WAIT_END(event_no)                                                           \
  do {                                                                               \
    if (need_record_) {                                                              \
      ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                           \
      if (OB_NOT_NULL(di)) {                                                         \
        di->end_wait_event(                                                          \
            event_no, OB_WAIT_EVENTS[event_no].wait_class_ == ObWaitClassIds::IDLE); \
      }                                                                              \
    }                                                                                \
  } while (0)

#endif /* OB_DIAGNOSTIC_INFO_GUARD_H_ */