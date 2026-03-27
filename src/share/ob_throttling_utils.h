/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_WRITE_THROTTLING_UTILS_H_
#define OCEANBASE_SHARE_OB_WRITE_THROTTLING_UTILS_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
class ObThrottlingUtils
{
public:
  ObThrottlingUtils(){};
  ~ObThrottlingUtils(){};
public:
  static int calc_decay_factor(const int64_t available_size,
                               const int64_t duration_us,
                               const int64_t chunk_size,
                               double &decay_fatctor);
  static int get_throttling_interval(const int64_t chunk_size,
                                     const int64_t request_size,
                                     const int64_t trigger_limit,
                                     const int64_t cur_hold,
                                     const double decay_factor,
                                     int64_t &interval_us);

};

}//end of namespace share
}//end of namespace oceanbase
#endif
