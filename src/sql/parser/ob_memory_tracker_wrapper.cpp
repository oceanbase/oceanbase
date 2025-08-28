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


