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

#include "lib/ash/ob_active_session_guard.h"

using namespace oceanbase::common;

ActiveSessionStat ObActiveSessionGuard::dummy_stat_;
thread_local ActiveSessionStat ObActiveSessionGuard::thread_local_stat_;

ActiveSessionStat *&ObActiveSessionGuard::get_stat_ptr()
{
  // before ObActiveSessionGuard constructed,
  // ensure it can get the dummy value if anyone call get_stat
  RLOCAL_INIT(ActiveSessionStat *, stat, &dummy_stat_);
  return stat;
}

ActiveSessionStat &ObActiveSessionGuard::get_stat()
{
  return *get_stat_ptr();
}

void ObActiveSessionGuard::setup_default_ash()
{
  get_stat_ptr() = &dummy_stat_;
}

void ObActiveSessionGuard::setup_ash(ActiveSessionStat &stat)
{
  get_stat_ptr() = &stat;
}

void ObActiveSessionGuard::setup_thread_local_ash()
{
  get_stat_ptr() = &thread_local_stat_;
}
