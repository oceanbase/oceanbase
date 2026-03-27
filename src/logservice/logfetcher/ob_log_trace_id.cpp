/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_trace_id.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace logfetcher
{
common::ObAddr& get_self_addr()
{
  static common::ObAddr s_self_addr;
  return s_self_addr;
}

}
}
