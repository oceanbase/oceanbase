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

class ObDIActionGuard
{
public:
  enum ActionNameSpace
  {
    NS_INVALID = 0,
    NS_PROGRAM = 1,
    NS_MODULE = 2,
    NS_ACTION = 3,
    NS_MAX
  };
public:
  ObDIActionGuard(const char *program, const char *module, const char *action);
  ObDIActionGuard(const char *module, const char *action);
  ObDIActionGuard(const char *action);
  ObDIActionGuard(const ObString &action);
  ObDIActionGuard(const std::type_info &type_info);
  ObDIActionGuard(ActionNameSpace action_ns, const char *action_format, ...);
  ~ObDIActionGuard();
private:
  void save_prev_action_info(bool has_program, bool has_module, bool has_action);
  struct SaveInfo
  {
    char program_[ASH_PROGRAM_STR_LEN];
    char module_[ASH_MODULE_STR_LEN];
    char action_[ASH_ACTION_STR_LEN];
  };
private:
  SaveInfo *prev_info_;
  union {
    uint64_t flags_;
    struct {
      uint64_t pre_guard_ref_                 :1;
      uint64_t reset_program_                 :1;
      uint64_t save_program_                  :1;
      uint64_t reset_module_                  :1;
      uint64_t save_module_                   :1;
      uint64_t reset_action_                  :1;
      uint64_t save_action_                   :1;
    };
  };
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
  static int revert_diagnostic_info(ObDiagnosticInfo *di)
      __attribute__((deprecated("pls use inc_ref/dec_ref instead")));
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
  static void set_service_name(uint64_t tenant_id, const char *name);
  static void set_program_name(const char *name);
  static void set_service_module(const char *module);
  static void set_service_action(const char *service, const char *module, const char *action);
  static void set_service_action(const char *action);
  static inline int inc_ref(ObDiagnosticInfo *di) __attribute__((always_inline))
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(di)) {
      const int64_t cur_ref = ATOMIC_AAF(&di->ref_cnt_, 1);
      // 1 for rpc layer and 1 for this function
      if (2 == cur_ref) {
        di->get_ash_stat().set_sess_active();
      }
      if (OB_UNLIKELY(cur_ref >= 10)) {
        COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "inc di ref overflow", K(cur_ref), KPC(di));
      }
    }
    return ret;
  }
  static int dec_ref(ObDiagnosticInfo *&di);
  static void set_io_time(int64_t enqueue_time, int64_t device_time, int64_t callback_time);
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
      get_instance().get_diagnostic_ptr() = nullptr;
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

#define DEF_ASH_ITEM_ATTACH_GUARD(item_name, item_type)                           \
class ObAshStat_##item_name##_AttachGuard                                         \
{                                                                                 \
public:                                                                           \
  ObAshStat_##item_name##_AttachGuard(const item_type &item_val)                  \
  {                                                                               \
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                          \
    if (OB_NOT_NULL(di)) {                                                        \
      pre_item_val_ = di->get_ash_stat().item_name##_;                            \
      di->get_ash_stat().item_name##_ = item_val;                                 \
    }                                                                             \
  }                                                                               \
  ~ObAshStat_##item_name##_AttachGuard()                                          \
  {                                                                               \
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();                          \
    if (OB_NOT_NULL(di)) {                                                        \
      di->get_ash_stat().item_name##_ = pre_item_val_;                            \
    }                                                                             \
  }                                                                               \
private:                                                                          \
  item_type pre_item_val_;                                                        \
  DISALLOW_COPY_AND_ASSIGN(ObAshStat_##item_name##_AttachGuard);                  \
};

DEF_ASH_ITEM_ATTACH_GUARD(plan_line_id, int32_t);

#define ASH_ITEM_ATTACH_GUARD(item_name, item_val)                                \
  ObAshStat_##item_name##_AttachGuard _ash_item_attach_guard(item_val);

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
DEF_ASH_FLAGS_SETTER_GUARD(in_das_remote_exec)
DEF_ASH_FLAGS_SETTER_GUARD(in_filter_rows)
DEF_ASH_FLAGS_SETTER_GUARD(in_rpc_encode)
DEF_ASH_FLAGS_SETTER_GUARD(in_rpc_decode)
DEF_ASH_FLAGS_SETTER_GUARD(in_connection_mgr)
DEF_ASH_FLAGS_SETTER_GUARD(in_check_row_confliction)
DEF_ASH_FLAGS_SETTER_GUARD(in_deadlock_row_register)
DEF_ASH_FLAGS_SETTER_GUARD(in_check_tx_status)
DEF_ASH_FLAGS_SETTER_GUARD(in_resolve)
DEF_ASH_FLAGS_SETTER_GUARD(in_rewrite)
DEF_ASH_FLAGS_SETTER_GUARD(in_foreign_key_cascading)
DEF_ASH_FLAGS_SETTER_GUARD(in_extract_query_range)


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
