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

#define USING_LOG_PREFIX SHARE

#include "lib/ash/ob_ash_bkgd_sess_inactive_guard.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

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
    GET_DIAGNOSTIC_INFO->get_ash_stat().is_active_session_ = prev_stat_;
  }
}
