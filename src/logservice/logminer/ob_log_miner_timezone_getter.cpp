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