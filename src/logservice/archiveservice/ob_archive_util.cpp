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

#include "ob_archive_util.h"
#include "src/logservice/palf/lsn.h"
#include "observer/omt/ob_tenant_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"
namespace oceanbase
{
namespace archive
{
using namespace oceanbase::palf;

int64_t cal_archive_file_id(const LSN &lsn, const int64_t N)
{
  return lsn.val_ / N + 1;
}

/*
  0 <= archive_lag_target <= 100ms  ----> interval = 10ms
  100ms < archive_lag_target <= 1s  ----> interval = archive_lag_target / 10
  archive_lag_target > 1s           ----> interval = 100ms
*/
int64_t cal_thread_run_interval()
{
  constexpr int64_t min_thread_run_interval = 10 * 1000L; // 10ms
  const int64_t default_thread_run_interval = 100 * 1000L;  // default interval 100ms
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t lag_target = tenant_config.is_valid() ? tenant_config->archive_lag_target : 0L;
  int64_t interval = max(min_thread_run_interval, lag_target > 1 * 1000 * 1000L ? default_thread_run_interval : lag_target / 10);

  return interval;
}

}
}
