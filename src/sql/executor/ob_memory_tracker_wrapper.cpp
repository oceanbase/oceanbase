/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/executor/ob_memory_tracker_wrapper.h"
#include "sql/executor/ob_memory_tracker.h"

int check_mem_status()
{
  return oceanbase::lib::ObMemTrackerGuard::check_status();
}

int try_check_mem_status(int64_t check_try_times)
{
  return oceanbase::lib::ObMemTrackerGuard::try_check_status(check_try_times);
}
