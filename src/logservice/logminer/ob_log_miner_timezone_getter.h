/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_TIMEZONE_GETTER_H_
#define OCEANBASE_LOG_MINER_TIMEZONE_GETTER_H_

#include "lib/timezone/ob_timezone_info.h"            // ObTimeZoneInfo

namespace oceanbase
{
namespace oblogminer
{
#define LOGMINER_TZ \
::oceanbase::oblogminer::ObLogMinerTimeZoneGetter::get_instance()

class ObLogMinerTimeZoneGetter {
public:
  static ObLogMinerTimeZoneGetter &get_instance();

  ObLogMinerTimeZoneGetter();

  int set_timezone(const char *timezone);

  const ObTimeZoneInfo &get_tz_info() const {
    return tz_info_;
  }

private:
  ObTimeZoneInfo tz_info_;
};
}
}

#endif
