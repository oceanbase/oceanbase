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

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/ob_lib_config.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/stat/ob_diagnostic_info_util.h"

namespace oceanbase
{
namespace common
{

ObBackGroundSessionGuard::ObBackGroundSessionGuard(int64_t tenant_id, int64_t group_id)
    : di_(nullptr), prev_value_(lib::ObPerfModeGuard::get_tl_instance()), dic_(nullptr)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    if (tenant_id == OB_INVALID_TENANT_ID || tenant_id == OB_SERVER_TENANT_ID ||
        tenant_id == OB_DTL_TENANT_ID) {
      tenant_id = OB_SYS_TENANT_ID;
    }
    int ret = OB_SUCCESS;
    const int64_t bg_sess_id = ObBackgroundSessionIdGenerator::get_instance().get_next_sess_id();
    if (ObDiagnosticInfoContainer::get_global_di_container()->is_inited()) {
      // using global di for all background sessions. because when the tenant is dropped there could
      // be some tenant thread is left. like TXXXX_UnitGC thread when mtl_wait is called.
      dic_ = ObDiagnosticInfoContainer::get_global_di_container();
    } else {
      ret = OB_ERR_UNEXPECTED;
      lib::ObPerfModeGuard::get_tl_instance() = true;
      LOG_ERROR_RET(OB_SUCCESS, "no diagnostic info resource for background session", K(tenant_id),
          K(group_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dic_->acquire_diagnostic_info(tenant_id, group_id, bg_sess_id, di_))) {
      LOG_ERROR("failed to allocate diagnostic info object", K(ret), K(tenant_id), K(bg_sess_id),
          K(group_id));
      di_ = nullptr;
      lib::ObPerfModeGuard::get_tl_instance() = true;
    } else if (FALSE_IT(ObLocalDiagnosticInfo::inc_ref(di_))) {
    } else {
      OB_ASSERT(di_ != nullptr);
      di_->get_ash_stat().session_id_ = bg_sess_id;
      di_->get_ash_stat().tenant_id_ = tenant_id;
      di_->get_ash_stat().session_type_ = ObActiveSessionStatItem::SessionType::BACKGROUND;
      di_->get_ash_stat()
          .set_sess_active();  // for background di. It's ref cnt is 1 throughout its life cycle.
      ObLocalDiagnosticInfo::setup_diagnostic_info(di_);
      ObLocalDiagnosticInfo::set_thread_name(ob_get_tname());
      LOG_DEBUG("acquired new diagnostic info for background session", K(tenant_id), K(group_id),
          K(bg_sess_id), K(lbt()));
    }
  } else {
    di_ = nullptr;
    lib::ObPerfModeGuard::get_tl_instance() = true;
  }
}

ObBackGroundSessionGuard::~ObBackGroundSessionGuard()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di_)) {
    di_->get_ash_stat().set_sess_inactive();
    ObLocalDiagnosticInfo::reset_diagnostic_info();
    // background diagnostic info only attach to current thread.
    ObLocalDiagnosticInfo::dec_ref(di_);
  }

  lib::ObPerfModeGuard::get_tl_instance() = prev_value_;
}

ObDIActionGuard::ObDIActionGuard(const char *program, const char *module, const char *action)
  : prev_info_(nullptr),
    flags_(0)
{
  save_prev_action_info(program != nullptr, module != nullptr, action != nullptr);
  ObLocalDiagnosticInfo::set_service_action(program, module, action);
}

void ObDIActionGuard::save_prev_action_info(bool has_program, bool has_module, bool has_action)
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (di != nullptr) {
    pre_guard_ref_ = di->action_guard_ref_;
    di->action_guard_ref_ = true;
    save_program_ = has_program && di->get_ash_stat().program_[0] != '\0' ? true : false;
    reset_program_ = !save_program_ && has_program ? true : false;
    save_module_ = has_module && di->get_ash_stat().module_[0] != '\0' ? true : false;
    reset_module_ = !save_module_ && has_module ? true : false;
    save_action_ = has_action && di->get_ash_stat().action_[0] != '\0' ? true : false;
    reset_action_ = !save_action_ && has_action ? true : false;
    if (save_program_ || save_module_ || save_action_) {
      prev_info_ = op_alloc(ObDIActionGuard::SaveInfo);
      if (OB_ISNULL(prev_info_)) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate action buffer failed", K(sizeof(ObDIActionGuard::SaveInfo)));
      } else {
        di->action_guard_ref_ = true;
        if (save_program_) {
          MEMCPY(prev_info_->program_, di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN);
        }
        if (save_module_) {
          MEMCPY(prev_info_->module_, di->get_ash_stat().module_, ASH_MODULE_STR_LEN);
        }
        if (save_action_) {
          MEMCPY(prev_info_->action_, di->get_ash_stat().action_, ASH_ACTION_STR_LEN);
        }
      }
    }
  }
}

ObDIActionGuard::ObDIActionGuard(const char *module, const char *action)
  : ObDIActionGuard(nullptr, module, action)
{
}

ObDIActionGuard::ObDIActionGuard(const char *action)
  : ObDIActionGuard(nullptr, nullptr, action)
{
}

ObDIActionGuard::ObDIActionGuard(const ObString &action)
  : prev_info_(nullptr),
    flags_(0)
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (di != nullptr) {
    save_prev_action_info(false, false, true);
    snprintf(di->get_ash_stat().action_, ASH_ACTION_STR_LEN, "%.*s", action.length(), action.ptr());
  }
}

ObDIActionGuard::ObDIActionGuard(const std::type_info &type_info)
  : prev_info_(nullptr),
    flags_(0)
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (di != nullptr) {
    save_prev_action_info(false, false, true);
    int64_t len = ASH_ACTION_STR_LEN;
    extract_demangled_class_name(type_info.name(), "Ob", di->get_ash_stat().action_, len);
  }
}

ObDIActionGuard::ObDIActionGuard(ActionNameSpace action_ns, const char *action_format, ...)
  : prev_info_(nullptr),
    flags_(0)
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (action_ns > NS_INVALID && action_ns < NS_MAX && action_format != nullptr && di != nullptr) {
    save_prev_action_info(NS_PROGRAM == action_ns, NS_MODULE == action_ns, NS_ACTION == action_ns);
    char *buffer = nullptr;
    int64_t buffer_len = 0;
    switch (action_ns) {
      case NS_PROGRAM:
        buffer = di->get_ash_stat().program_;
        buffer_len = ASH_PROGRAM_STR_LEN;
        break;
      case NS_MODULE:
        buffer = di->get_ash_stat().module_;
        buffer_len = ASH_MODULE_STR_LEN;
        break;
      case NS_ACTION:
        buffer = di->get_ash_stat().action_;
        buffer_len = ASH_ACTION_STR_LEN;
        break;
      default: break;
    }
    va_list args;
    va_start(args, action_format);
    vsnprintf(buffer, buffer_len, action_format, args);
    va_end(args);
  }
}

ObDIActionGuard::~ObDIActionGuard()
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (di != nullptr) {
    di->action_guard_ref_ = pre_guard_ref_;
    if (reset_program_) {
      di->get_ash_stat().program_[0] = '\0';
    } else if (save_program_ && prev_info_ != nullptr) {
      MEMCPY(di->get_ash_stat().program_, prev_info_->program_, ASH_PROGRAM_STR_LEN);
    }
    if (reset_module_) {
      di->get_ash_stat().module_[0] = '\0';
    } else if (save_module_ && prev_info_ != nullptr) {
      MEMCPY(di->get_ash_stat().module_, prev_info_->module_, ASH_MODULE_STR_LEN);
    }
    if (reset_action_) {
      di->get_ash_stat().action_[0] = '\0';
    } else if (save_action_ && prev_info_ != nullptr) {
      MEMCPY(di->get_ash_stat().action_, prev_info_->action_, ASH_ACTION_STR_LEN);
    }
  }
  if (prev_info_ != nullptr) {
    op_free(prev_info_);
    prev_info_ = nullptr;
  }
}

ObLocalDiagnosticInfo::ObLocalDiagnosticInfo()
    : di_ptr_(nullptr), slot_(nullptr)
{}

int ObLocalDiagnosticInfo::aggregate_diagnostic_info_summary(ObDiagnosticInfo *di)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di)) {
    if (OB_NOT_NULL(di->get_summary_slot())) {
      di->get_summary_slot()->accumulate_diagnostic_info(*di);
    } else {
      const int64_t tenant_id = di->get_tenant_id();
      lib_mtl_switch(tenant_id, [&ret, &di, tenant_id](int switch_ret) -> void {
        if (OB_SUCC(switch_ret)) {
          if (OB_NOT_NULL(MTL_DI_CONTAINER()) && OB_FAIL(MTL_DI_CONTAINER()->aggregate_diagnostic_info_summary(di))) {
            LOG_WARN("failed to aggregate diagnostic info", KPC(di));
          } else {
            // do noting
          }
        } else {
          LOG_ERROR("aggregate diagnostic info failed", K(tenant_id), KPC(di));
        }
      });
    }
  }
  return ret;
}

int ObLocalDiagnosticInfo::return_diagnostic_info(ObDiagnosticInfo *di)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di)) {
    const int64_t tenant_id = di->get_tenant_id();
    const int64_t session_id = di->get_session_id();
    lib_mtl_switch(tenant_id, [&ret, &di, tenant_id, session_id](int switch_ret) -> void {
      if (OB_SUCC(switch_ret)) {
        ObDiagnosticInfoContainer *c = MTL_DI_CONTAINER();
        if (OB_FAIL(c->return_diagnostic_info(di))) {
          LOG_WARN("failed to return diagnostic info", KPC(di));
        } else {
          // do noting
          LOG_DEBUG("return diagnostic info object", K(session_id), K(tenant_id));
        }
      } else {
        LOG_ERROR("return diagnostic info failed", K(tenant_id), KPC(di));
      }
    });
  }
  return ret;
}

int ObLocalDiagnosticInfo::dec_ref(ObDiagnosticInfo *&di)
{
  int ret = OB_SUCCESS;
  const bool is_global = di->is_acquired_from_global();
  if (OB_NOT_NULL(di)) {
    di->inc_release_holder_cnt();
    const int64_t cur_ref = ATOMIC_SAF(&di->ref_cnt_, 1);
    if (OB_UNLIKELY(cur_ref < 0)) {
      di->dec_release_holder_cnt();
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dec di ref overflow", K(cur_ref), KPC(di));
    } else if (1 == cur_ref) {
      // left 1 for rpc layer
      di->get_ash_stat().set_sess_inactive();
      if (di->is_using_cache()) {
        di->dec_release_holder_cnt();
        if (OB_UNLIKELY(is_global)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid use of global container cache", K(ret), KPC(di));
          ObDiagnosticInfoContainer::get_global_di_container()->return_di_to_cache(di);
        } else {
          const int64_t tenant_id = di->get_tenant_id();
          lib_mtl_switch(tenant_id, [&ret, &di, tenant_id](int switch_ret) -> void {
            if (OB_SUCC(switch_ret)) {
              MTL_DI_CONTAINER()->return_di_to_cache(di);
              // ATTENTION: cannot touch di after return it to di cache. It could be released in clear_di_cache
              di = nullptr;
            } else {
              LOG_ERROR("dec diagnostic info ref failed", K(tenant_id), KPC(di));
            }
          });
        }
      } else {
        di->dec_release_holder_cnt();
      }
    } else if (0 == cur_ref) {
      // only last caller dec ref.
      lib::ObDisableDiagnoseGuard disable_guard;
      const int64_t tenant_id = di->get_tenant_id();
      if (OB_UNLIKELY(is_global)) {
        // wait till other thread end call of ObLocalDiagnosticInfo::dec_ref
        int64_t cur_time = ObClockGenerator::getClock();
        while (di->get_release_holder_cnt() != 1) {
          // this while case would be entered in very rare condition.
          PAUSE();
          if (cur_time + 10 * 1000 * 1000 < ObClockGenerator::getClock()) {  // 10 seconds
            cur_time = ObClockGenerator::getClock();
            LOG_WARN("dec ref hold too long", KPC(di));
          }
        }
        di->dec_release_holder_cnt();
        ObDiagnosticInfoContainer::get_global_di_container()->dec_ref(di);
      } else {
        lib_mtl_switch(tenant_id, [&ret, &di, tenant_id](int switch_ret) -> void {
          // wait till other thread end call of ObLocalDiagnosticInfo::dec_ref
          int64_t cur_time = ObClockGenerator::getClock();
          while (di->get_release_holder_cnt() != 1) {
            // this while case would be entered in very rare condition.
            PAUSE();
            if (cur_time + 10 * 1000 * 1000 < ObClockGenerator::getClock()) {  // 10 seconds
              cur_time = ObClockGenerator::getClock();
              LOG_WARN("dec ref hold too long", KPC(di));
            }
          }
          di->dec_release_holder_cnt();
          if (OB_SUCC(switch_ret)) {
            MTL_DI_CONTAINER()->dec_ref(di);
          } else {
            LOG_ERROR("dec diagnostic info ref failed", K(tenant_id), KPC(di));
          }
        });
      }
    } else {
      di->dec_release_holder_cnt();
    }
  }
  return ret;
}

void ObLocalDiagnosticInfo::set_thread_name(const char *name)
{
  ObDiagnosticInfo *di = get();
  if (OB_NOT_NULL(di)) {
    MEMMOVE(di->get_ash_stat().program_, name,
        oceanbase::OB_THREAD_NAME_BUF_LEN);
    di->get_ash_stat().program_[oceanbase::OB_THREAD_NAME_BUF_LEN] = '\0';
  }

}

void ObLocalDiagnosticInfo::set_thread_name(uint64_t tenant_id, const char *name)
{
  ObDiagnosticInfo *di = get();
  if (OB_NOT_NULL(di) && !di->action_guard_ref_) {
    //If the diagnostic info is held by DIActionGuard,
    //it means that the program,module,action fields will be filled in by DIActionGuard
    //and cannot be modified by other interfaces.
    set_service_name(tenant_id, name);
    set_service_module(name);
  }
}

void ObLocalDiagnosticInfo::set_service_name(uint64_t tenant_id, const char *name)
{
  ObDiagnosticInfo *di = get();
  if (name != nullptr && OB_NOT_NULL(di)) {
    if (tenant_id == 0) {
      snprintf(di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN, "%s", name);
    } else {
      snprintf(di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN,
              "T%ld_%s", tenant_id, name);
    }
  }
}

void ObLocalDiagnosticInfo::set_program_name(const char *program)
{
  ObDiagnosticInfo *di = get();
  if (program != nullptr && OB_NOT_NULL(di)) {
    snprintf(di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN, "%s", program);
  }
}

void ObLocalDiagnosticInfo::set_service_module(const char *module)
{
  ObDiagnosticInfo *di = get();
  if (module != nullptr && OB_NOT_NULL(di)) {
    snprintf(di->get_ash_stat().module_, ASH_MODULE_STR_LEN, "%s", module);
  }
}

void ObLocalDiagnosticInfo::set_service_action(const char *action)
{
  ObDiagnosticInfo *di = get();
  if (action != nullptr && OB_NOT_NULL(di)) {
    snprintf(di->get_ash_stat().action_, ASH_ACTION_STR_LEN, "%s", action);
  }
}

void ObLocalDiagnosticInfo::set_service_action(const char *service, const char *module, const char *action)
{
  set_service_name(ob_get_tenant_id(), service);
  set_service_module(module);
  set_service_action(action);
}

void ObLocalDiagnosticInfo::set_io_time(int64_t enqueue_time, int64_t device_time, int64_t callback_time) {
  ObDiagnosticInfo *di = get_instance().get_diagnostic_ptr();
  if (OB_NOT_NULL(di)) {
    di->get_ash_stat().p1_ = enqueue_time;
    di->get_ash_stat().p2_ = device_time;
    di->get_ash_stat().p3_ = callback_time;
    di->get_curr_wait().p1_ = enqueue_time;
    di->get_curr_wait().p2_ = device_time;
    di->get_curr_wait().p3_ = callback_time;
  }
}

ObLatchStat *ObLocalDiagnosticInfo::get_latch_stat(int64_t latch_id)
{
  ObLocalDiagnosticInfo &instance = get_instance();
  ObDiagnosticInfoSlot *slot = instance.slot_;
  ObLatchStat *ptr = nullptr;
  if (OB_UNLIKELY(nullptr != slot)) {
    ptr = slot->get_latch_stat(latch_id);
  }
  return ptr;
}

ObTenantDiagnosticInfoSummaryGuard::ObTenantDiagnosticInfoSummaryGuard(
    int64_t tenant_id, int64_t group_id, bool using_global)
    : need_restore_slot_(false), prev_slot_(nullptr)
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (oceanbase::lib::is_diagnose_info_enabled() && OB_NOT_NULL(di)) {
    int ret = OB_SUCCESS;
    ObDiagnosticInfoSlot *slot = nullptr;
    lib::ObDisableDiagnoseGuard disable_guard;
    if (di->get_tenant_id() == tenant_id &&
        di->get_group_id() == group_id) {
      slot = di->get_summary_slot();
    }
    if (slot == nullptr) {
      if (tenant_id != OB_INVALID_TENANT_ID && tenant_id != OB_SERVER_TENANT_ID &&
          tenant_id != OB_DTL_TENANT_ID) {
        if (using_global) {
          ObDiagnosticInfoContainer *c = ObDiagnosticInfoContainer::get_global_di_container();
          if (OB_FAIL(c->summarys_.get_di_slot(
                  tenant_id, group_id, di->get_session_id(), slot))) {
            LOG_WARN("failed to get summary slot", K(ret), K(tenant_id), K(group_id));
          } else {
            // get slot successfully
          }
        } else {
          lib_mtl_switch(
              tenant_id, [&ret, &di, &slot, tenant_id, group_id](int switch_ret) -> void {
                if (OB_SUCC(switch_ret)) {
                  if (OB_FAIL(MTL_DI_CONTAINER()->summarys_.get_di_slot(
                          tenant_id, group_id, di->get_session_id(), slot))) {
                    LOG_WARN("failed to get summary slot", K(ret), K(tenant_id), K(group_id));
                  } else {
                    // get slot successfully
                  }
                } else {
                  LOG_WARN("failed to switch to tenant diagnostic info summary mode", K(ret),
                      K(tenant_id), K(group_id), K(lbt()));
                }
              });
        }
      } else {
        ret = OB_ERROR;
      }
    }
    if (OB_SUCC(ret)) {
      need_restore_slot_ = true;
      prev_slot_ = ObLocalDiagnosticInfo::get_instance().slot_;
      ObLocalDiagnosticInfo::setup_di_slot(slot);
    }
  }
}

ObTenantDiagnosticInfoSummaryGuard::ObTenantDiagnosticInfoSummaryGuard(ObDiagnosticInfoSlot *slot)
    : need_restore_slot_(false), prev_slot_(nullptr)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    if (OB_NOT_NULL(slot)) {
      need_restore_slot_ = true;
      prev_slot_ = ObLocalDiagnosticInfo::get_instance().slot_;
      ObLocalDiagnosticInfo::setup_di_slot(slot);
    }
  }
}

ObTenantDiagnosticInfoSummaryGuard::~ObTenantDiagnosticInfoSummaryGuard()
{
  if (need_restore_slot_) {
    ObLocalDiagnosticInfo::setup_di_slot(prev_slot_);
  }
}

ObDiagnosticInfoSwitchGuard::ObDiagnosticInfoSwitchGuard(ObDiagnosticInfo *di)
    : prev_di_(nullptr),
      cur_di_(di),
      di_switch_success_(false),
#ifdef ENABLE_DEBUG_LOG
      leak_check_(false),
#endif
      prev_value_(false)
{
  int ret = OB_SUCCESS;
  if (oceanbase::lib::is_diagnose_info_enabled() && OB_NOT_NULL(di)) {
    common::ObLocalDiagnosticInfo::inc_ref(di);
    di_switch_success_ = true;
    di->get_ash_stat().tid_ = GETTID();
    prev_di_ = ObLocalDiagnosticInfo::get();
    ObLocalDiagnosticInfo::setup_diagnostic_info(di);
    cur_di_ = di;
#ifdef ENABLE_DEBUG_LOG
    if (di->get_uref() - INT32_MAX / 2 > 100) {
      LOG_ERROR_RET(OB_SUCCESS, "di switch after inc leak!", KPC(di), K(di));
      leak_check_ = true;
    }
#endif
  } else {
    LOG_DEBUG("disable diagnostic info recording", K(ret),
        K(oceanbase::lib::is_diagnose_info_enabled()), KPC(di));
    prev_value_ = lib::ObPerfModeGuard::get_tl_instance();
    lib::ObPerfModeGuard::get_tl_instance() = true;
  }
}

ObDiagnosticInfoSwitchGuard::~ObDiagnosticInfoSwitchGuard()
{
  int ret = OB_SUCCESS;
  if (di_switch_success_) {
    if (OB_NOT_NULL(prev_di_)) {
      ObLocalDiagnosticInfo::setup_diagnostic_info(prev_di_);
    } else {
      ObLocalDiagnosticInfo::reset_diagnostic_info();
    }
#ifdef ENABLE_DEBUG_LOG
    if (leak_check_) {
      LOG_ERROR("di switch before dec leak!", KPC(cur_di_), K(cur_di_));
    }
#endif
    common::ObLocalDiagnosticInfo::dec_ref(cur_di_);
  } else {
    lib::ObPerfModeGuard::get_tl_instance() = prev_value_;
  }
}

} /* namespace common */
} /* namespace oceanbase */
