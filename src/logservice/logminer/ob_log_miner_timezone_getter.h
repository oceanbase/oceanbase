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
