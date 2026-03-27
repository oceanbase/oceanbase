/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/time/ob_cur_time.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase
{
namespace common
{
volatile int64_t g_cur_time = 0;

void TimeUpdateDuty::runTimerTask()
{
  g_cur_time = ::oceanbase::common::ObTimeUtility::current_time();
}
}
}
