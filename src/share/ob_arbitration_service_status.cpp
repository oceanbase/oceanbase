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

#define USING_LOG_PREFIX SERVER

#include "share/ob_errno.h"                       // for KR(ret)
#include "share/ob_arbitration_service_status.h"  // for ObArbitrationServiceStatus
#include "lib/utility/ob_print_utils.h"           // for J_OBJ_START

namespace oceanbase
{
namespace share
{

static const char* arbitration_service_status_strs[] = {
  "ENABLING",
  "ENABLED",
  "DISABLING",
  "DISABLED"
};

const char* ObArbitrationServiceStatus::get_status_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_status_strs) == (int64_t)MAX,
                "arbitration_service_status string array size mismatch enum ArbitrationServiceStatus count");
  const char *str = NULL;
  if (status_ > INVALID
      && status_ < MAX) {
    str = arbitration_service_status_strs[static_cast<int64_t>(status_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ArbitrationServiceStatus", K_(status));
  }
  return str;
}

int64_t ObArbitrationServiceStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(status));
  J_OBJ_END();
  return pos;
}

int ObArbitrationServiceStatus::parse_from_string(const ObString &status)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_status_strs) == (int64_t)MAX,
                "arbitration_service_status string array size mismatch enum ArbitrationServiceStatus count");
  for (int i = 0; i < ARRAYSIZEOF(arbitration_service_status_strs) && !found; i++) {
    if (status.case_compare(ObString::make_string(arbitration_service_status_strs[i])) == 0) {
      status_ = static_cast<ArbitrationServiceStatus>(i);
      found = true;
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse status from string", KR(ret), K(status), K_(status));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObArbitrationServiceStatus, status_);
} // namespace share
} // namespace oceanbase
