/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_WR_OB_WR_STAT_GUARD_H_
#define OCEANBASE_WR_OB_WR_STAT_GUARD_H_
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace share
{

#define WR_STAT_GUARD(STAT_PREFIX)                                            \
  WrStatGuard<::oceanbase::common::ObStatEventIds::STAT_PREFIX##_ELAPSE_TIME, \
      ::oceanbase::common::ObStatEventIds::STAT_PREFIX##_CPU_TIME>            \
      guard;

template <ObStatEventIds::ObStatEventIdEnum elapse_time_id,
    ObStatEventIds::ObStatEventIdEnum cpu_time_id>
class WrStatGuard
{
public:
  WrStatGuard()
  {
    begin_ts_ = ::oceanbase::common::ObTimeUtility::current_time();
    begin_ru_cputime_ = get_ru_utime();
  }
  ~WrStatGuard()
  {
    int64_t elapse_time = ::oceanbase::common::ObTimeUtility::current_time() - begin_ts_;
    int64_t cpu_time = get_ru_utime() - begin_ru_cputime_;
    oceanbase::common::ObDiagnoseTenantInfo *tenant_info =
        oceanbase::common::ObDiagnoseTenantInfo::get_local_diagnose_info();
    if (NULL != tenant_info) {
      tenant_info->update_stat(elapse_time_id, elapse_time);
      tenant_info->update_stat(cpu_time_id, cpu_time);
    }
  }

private:
  int64_t get_ru_utime()
  {
    struct rusage ru;
    getrusage(RUSAGE_THREAD, &ru);
    int64_t ru_utime = ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec +
                       ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec;
    return ru_utime;
  }
  int64_t begin_ts_;
  int64_t begin_ru_cputime_;
};

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_WR_OB_WR_STAT_GUARD_H_
