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

#ifndef OCEANBASE_TRANSACTION_OB_WEAK_READ_UTIL_H_
#define OCEANBASE_TRANSACTION_OB_WEAK_READ_UTIL_H_

#include "stdint.h"
namespace oceanbase
{
namespace share
{
class SCN;
}
namespace transaction
{
class ObWeakReadUtil
{
public:
  static const int64_t DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY = 5 * 1000 * 1000L;
  static const int64_t DEFAULT_WEAK_READ_VERSION_REFRESH_INTERVAL = 50 * 1000L;
  static const int64_t DEFAULT_MAX_STALE_BUFFER_TIME = 500 * 1000L;
  static const int64_t DEFAULT_REPLICA_KEEPALIVE_INTERVAL = 3000 * 1000L;
  static const int64_t IGNORE_TENANT_EXIST_WARN = 1;
  static int64_t replica_keepalive_interval();
  static int generate_min_weak_read_version(const uint64_t tenant_id, share::SCN &scn);
  static bool enable_monotonic_weak_read(const uint64_t tenant_id);
  static int64_t max_stale_time_for_weak_consistency(const uint64_t tenant_id, int64_t ignore_warn = 0);
  static bool check_weak_read_service_available();
  static int64_t default_max_stale_time_for_weak_consistency() {
    return DEFAULT_MAX_STALE_TIME_FOR_WEAK_CONSISTENCY; };

private:
  ObWeakReadUtil(){};
  virtual ~ObWeakReadUtil(){
  };
};
}// transaction
}// oceanbase

#endif /* OCEANBASE_TRANSACTION_OB_WEAK_READ_UTIL_H_ */
