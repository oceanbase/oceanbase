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
 *
 * KV store stat
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_store_service_stat.h"
#include "ob_log_utils.h"                     // _G_
#include "lib/oblog/ob_log.h"                 // ObLogger
#include "lib/oblog/ob_log_module.h"          // LOG_*
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
void StoreServiceStatInfo::reset()
{
  total_data_size_ = 0;
  last_total_data_size_ = 0;
}

void StoreServiceStatInfo::do_data_stat(int64_t record_len)
{
  ATOMIC_AAF(&total_data_size_, record_len);
}

double StoreServiceStatInfo::calc_rate(const int64_t delta_time)
{
  double rate = 0.0;
  const int64_t local_totol_data_size = ATOMIC_LOAD(&total_data_size_);
  const int64_t local_last_totol_data_size = ATOMIC_LOAD(&last_total_data_size_);
  const int64_t delta_data_size = local_totol_data_size - local_last_totol_data_size;
  double delta_data_size_formatted = (double)delta_data_size / (double)_M_;

  if (delta_time > 0) {
    rate = (double)(delta_data_size_formatted) * 1000000.0 / (double)delta_time;
  }

  // Update the last statistics
  last_total_data_size_ = local_totol_data_size;

  return rate;
}

double StoreServiceStatInfo::get_total_data_size() const
{
  double total_size = 0.0;
  const int64_t local_totol_data_size = ATOMIC_LOAD(&total_data_size_);
  total_size = (double)local_totol_data_size / (double)_G_;

  return total_size;
}

}
}
