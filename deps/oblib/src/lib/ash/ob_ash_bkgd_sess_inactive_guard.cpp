/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "lib/ash/ob_ash_bkgd_sess_inactive_guard.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_util.h"

using namespace oceanbase::common;

ObBKGDSessInActiveGuard::ObBKGDSessInActiveGuard()
{
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di)) {
    need_record_ = true;
    prev_stat_ = di->get_ash_stat().is_active_session_;
    di->get_ash_stat().set_sess_inactive();
  } else {
    need_record_ = false;
  }
}
ObBKGDSessInActiveGuard::~ObBKGDSessInActiveGuard()
{
  if (need_record_) {
    if (prev_stat_) {
      GET_DIAGNOSTIC_INFO->get_ash_stat().set_sess_active();
    } else {
      GET_DIAGNOSTIC_INFO->get_ash_stat().set_sess_inactive();
    }
  }
}
