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

#ifndef __OCEANBASE_COMMON_OB_LEASE_COMMON_H__
#define __OCEANBASE_COMMON_OB_LEASE_COMMON_H__
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {
struct ObLease {
  int64_t lease_time;      // lease start time
  int64_t lease_interval;  // lease interval, lease valid time [lease_time, lease_time + lease_interval]
  int64_t renew_interval;  // renew interval, slave will renew lease when lease expiration time is close

  ObLease() : lease_time(0), lease_interval(0), renew_interval(0)
  {}

  bool is_lease_valid(int64_t redun_time);

  NEED_SERIALIZE_AND_DESERIALIZE;
};
}  // namespace common
}  // namespace oceanbase

#endif  //__OCEANBASE_COMMON_OB_LEASE_COMMON_H__
