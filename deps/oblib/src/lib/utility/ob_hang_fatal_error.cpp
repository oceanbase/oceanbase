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

#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_common_utility.h"

extern "C" {
void right_to_die_or_duty_to_live_c()
{
  ::oceanbase::common::right_to_die_or_duty_to_live();
}
}

namespace oceanbase
{
namespace common
{
_RLOCAL(bool, in_try_stmt);
int64_t g_fatal_error_thread_id = -1;

int64_t get_fatal_error_thread_id()
{
  return g_fatal_error_thread_id;
}
void set_fatal_error_thread_id(int64_t thread_id)
{
  g_fatal_error_thread_id = thread_id;
}

// To die or to live, it's a problem.
void right_to_die_or_duty_to_live()
{
  const ObFatalErrExtraInfoGuard *extra_info = ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
  set_fatal_error_thread_id(GETTID());
  while (true) {
    const char *info = (NULL == extra_info) ? NULL : to_cstring(*extra_info);
    LOG_DBA_ERROR(OB_ERR_THREAD_PANIC, "msg", "Trying so hard to die", KCSTRING(info), KCSTRING(lbt()));
  #ifndef FATAL_ERROR_HANG
    if (in_try_stmt) {
      throw OB_EXCEPTION<OB_ERR_UNEXPECTED>();
    }
  #endif
    sleep(60);
  }
}

} //common
} //oceanbase
