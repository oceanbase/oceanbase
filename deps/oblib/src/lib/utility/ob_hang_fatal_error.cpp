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
#include "lib/utility/ob_print_utils.h"
#include "common/ob_common_utility.h"

namespace oceanbase {
namespace common {

void right_to_die_or_duty_to_live_c()
{
  right_to_die_or_duty_to_live();
}

void right_to_die_or_duty_to_live()
{
  const ObFatalErrExtraInfoGuard* extra_info = ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
  BACKTRACE(
      ERROR, true, "Trying so hard to die, extra_info=(%s)", (NULL == extra_info) ? NULL : to_cstring(*extra_info));
  throw OB_EXCEPTION<OB_ERR_UNEXPECTED>();
  _OB_LOG(ERROR, "Trying very hard to live");
}

}  // namespace common
}  // namespace oceanbase
