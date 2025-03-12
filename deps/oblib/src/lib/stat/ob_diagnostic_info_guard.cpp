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
    } else {
      OB_ASSERT(di_ != nullptr);
      di_->get_ash_stat().session_id_ = bg_sess_id;
      di_->get_ash_stat().tenant_id_ = tenant_id;
      di_->get_ash_stat().session_type_ = ObActiveSessionStatItem::SessionType::BACKGROUND;
      di_->get_ash_stat().set_sess_active();
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
    dic_->revert_diagnostic_info(di_);
    // event if aggregate failed, still try to return it.
    if (OB_FAIL(dic_->return_diagnostic_info(di_))) {
      LOG_ERROR("failed to return diagnostic info", KPC(di_), K(dic_));
    }
  }

  lib::ObPerfModeGuard::get_tl_instance() = prev_value_;
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

int ObLocalDiagnosticInfo::revert_diagnostic_info(ObDiagnosticInfo *di)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di)) {
    const int64_t tenant_id = di->get_tenant_id();
    lib_mtl_switch(tenant_id, [&ret, &di, tenant_id](int switch_ret) -> void {
      if (OB_SUCC(switch_ret)) {
        ObDiagnosticInfoContainer *c = MTL_DI_CONTAINER();
        c->revert_diagnostic_info(di);
      } else {
        LOG_ERROR("return diagnostic info failed", K(tenant_id), KPC(di));
      }
    });
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

int ObLocalDiagnosticInfo::dec_ref(ObDiagnosticInfo *di)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(di)) {
    if (0 == ATOMIC_SAF(&di->ref_cnt_, 1)) {
      // only last caller dec ref.
      lib::ObDisableDiagnoseGuard disable_guard;
      const int64_t tenant_id = di->get_tenant_id();
      lib_mtl_switch(tenant_id, [&ret, &di, tenant_id](int switch_ret) -> void {
        if (OB_SUCC(switch_ret)) {
          MTL_DI_CONTAINER()->dec_ref(di);
        } else {
          LOG_ERROR("dec diagnostic info ref failed", K(tenant_id), KPC(di));
        }
      });
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
  if (OB_NOT_NULL(di)) {
    if (tenant_id == 0) {
      snprintf(di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN, "%s", name);
    } else {
      snprintf(di->get_ash_stat().program_, ASH_PROGRAM_STR_LEN,
              "T%ld_%s", tenant_id, name);
    }
  }
}

void ObLocalDiagnosticInfo::set_service_action(const char *program, const char *module, const char *action)
{
  ObDiagnosticInfo *di = get();
  if (OB_NOT_NULL(di)) {
    set_thread_name(ob_get_tenant_id(), program);
    snprintf(di->get_ash_stat().module_, ASH_MODULE_STR_LEN, "%s", module);
    snprintf(di->get_ash_stat().action_, ASH_ACTION_STR_LEN, "%s", action);
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
    // 1 for rpc layer and 1 for this guard
    if (2 == di->ref_cnt_) {
      // first guard set session active.
      cur_di_->get_ash_stat().set_sess_active();
    }
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
    // 1 for rpc and 1 for this guard
    if (2 == cur_di_->ref_cnt_) {
      // last guard set session inactive
      cur_di_->get_ash_stat().set_sess_inactive();
    }
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

ObRetryWaitEventInfoGuard::ObRetryWaitEventInfoGuard(sql::ObSQLSessionInfo &session)
  : is_switch_(false),
    parent_ptr_(nullptr)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
    if (OB_NOT_NULL(di)) {
      sql::ObQueryRetryASHDiagInfo * cur_ptr = session.get_retry_info_for_update().get_query_retry_ash_diag_info_ptr();
      if (di->get_ash_stat().get_retry_ash_diag_info_ptr() != cur_ptr) {
        sql::ObQueryRetryASHDiagInfo *parent_ptr_ = di->get_ash_stat().get_retry_ash_diag_info_ptr();
        di->get_ash_stat().set_retry_ash_diag_info_ptr(cur_ptr);
        is_switch_ = true;
      }
    }
  }
}
ObRetryWaitEventInfoGuard::~ObRetryWaitEventInfoGuard()
{
  if (is_switch_) {
    ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
    if (OB_NOT_NULL(di)) {
      di->get_ash_stat().set_retry_ash_diag_info_ptr(parent_ptr_);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "(SHOULD NEVER HAPPEN) di point to nullptr", K(di));
    }
  }
}

} /* namespace common */
} /* namespace oceanbase */