/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_UTIL_H_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_UTIL_H_

#include <stdint.h>
#include "ob_cdc_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace cdc
{
class ObExtRpcQit
{
public:
  ObExtRpcQit() : deadline_(common::OB_INVALID_TIMESTAMP) {}
  int init(const int64_t deadline);
  // check should_hurry_quit when start to perform a time-consuming operation
  bool should_hurry_quit() const;

  TO_STRING_KV(K(deadline_));
private:
  static const int64_t RESERVED_INTERVAL = 1 * 1000 * 1000; // 1 second
  int64_t deadline_;
};

} // namespace cd
} // namespace oceanbase

#endif

