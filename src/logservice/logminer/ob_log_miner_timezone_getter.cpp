/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_timezone_getter.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace oblogminer
{

ObLogMinerTimeZoneGetter &ObLogMinerTimeZoneGetter::get_instance()
{
  static ObLogMinerTimeZoneGetter tz_getter_instance;
  return tz_getter_instance;
}

ObLogMinerTimeZoneGetter::ObLogMinerTimeZoneGetter():
    tz_info_() { }

int ObLogMinerTimeZoneGetter::set_timezone(const char *timezone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.set_timezone(ObString(timezone)))) {
    LOG_ERROR("parse timezone failed", K(ret), KCSTRING(timezone));
  }
  return ret;
}

}
}