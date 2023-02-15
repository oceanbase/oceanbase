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

#ifndef OCEANBASE_SHARE_OB_ZONE_STATUS_H_
#define OCEANBASE_SHARE_OB_ZONE_STATUS_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

struct ObZoneStatus
{
  enum Status
  {
    INACTIVE = 1,
    ACTIVE = 2, // zone is working
    UNKNOWN, // not a status, just the max limit value
  };
  static const char *get_status_str(const Status status);
  static Status get_status(const common::ObString &status_str);
};

inline ObZoneStatus::Status ObZoneStatus::get_status(const common::ObString &status_str)
{
  Status ret_status = UNKNOWN;
  if (status_str == common::ObString::make_string(get_status_str(INACTIVE))) {
    ret_status = INACTIVE;
  } else if (status_str == common::ObString::make_string(get_status_str(ACTIVE))) {
    ret_status = ACTIVE;
  } else {
    SERVER_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "invalid status_str, return UNKNOWN status", K(status_str));
  }
  return ret_status;
}

inline const char *ObZoneStatus::get_status_str(const ObZoneStatus::Status status)
{
  const char *str = "UNKNOWN";
  switch (status) {
    case ACTIVE:
      str = "ACTIVE";
      break;
    case INACTIVE:
      str = "INACTIVE";
      break;
    default:
      SERVER_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "unknown zone status, fatal error", K(status));
      break;
  }

  return str;
}

} // end namespace share
} // end namespace oceanbase
#endif  //OCEANBASE_SHARE_OB_ZONE_STATUS_H_
