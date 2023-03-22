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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_UTIL_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_UTIL_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
  class ObISQLClient;
}
namespace rootserver
{
class ObPrimaryMajorFreezeService;
class ObRestoreMajorFreezeService;
class ObMajorFreezeService;

class ObMajorFreezeUtil
{
public:
  static int get_major_freeze_service(ObPrimaryMajorFreezeService *primary_major_freeze_service,
                                      ObRestoreMajorFreezeService *restore_major_freeze_service,
                                      ObMajorFreezeService *&major_freeze_service,
                                      bool &is_primary_service);
  static int check_epoch_periodically(common::ObISQLClient &sql_proxy,
                                      const uint64_t tenant_id,
                                      const int64_t expected_epoch,
                                      int64_t &last_check_us);
private:
  static const int64_t CHECK_EPOCH_INTERVAL_US = 60 * 1000L * 1000L; // 60s
};

#define FREEZE_TIME_GUARD \
  rootserver::ObFreezeTimeGuard freeze_time_guard(__FILE__, __LINE__, __FUNCTION__, "[RS] ")

class ObFreezeTimeGuard
{
public:
  ObFreezeTimeGuard(const char *file,
                    const int64_t line,
                    const char *func,
                    const char *mod);
  virtual ~ObFreezeTimeGuard();

private:
  const int64_t FREEZE_WARN_THRESHOLD_US = 10 * 1000L * 1000L; // 10s

private:
  const int64_t warn_threshold_us_;
  const int64_t start_time_us_;
  const char * const file_;
  const int64_t line_;
  const char * const func_name_;
  const char * const log_mod_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_UTIL_H_
