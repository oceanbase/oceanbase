/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_log_policy.h"

namespace oceanbase
{
namespace common
{
int ObLogPolicyParser::parse_retry_write_policy(const char *str, ObLogRetryWritePolicy &policy)
{
  int ret = OB_SUCCESS;
  if (0 == STRCMP(str, "normal")) {
    policy = ObLogRetryWritePolicy::NORMAL_WRITE;
  } else if (0 == STRCMP(str, "switch_file")) {
    policy = ObLogRetryWritePolicy::SWITCH_FILE_WRITE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(str));
  }
  return ret;
}

int ObLogPolicyParser::parse_create_policy(const char *str, ObLogCreatePolicy &policy)
{
  int ret = OB_SUCCESS;
  if (0 == STRCMP(str, "normal")) {
    policy = ObLogCreatePolicy::NORMAL_CREATE;
  } else if (0 == STRCMP(str, "pre_create")) {
    policy = ObLogCreatePolicy::PRE_CREATE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(str));
  }
  return ret;
}

int ObLogPolicyParser::parse_log_write_policy(const char *str, ObLogWritePolicy &policy)
{
  int ret = OB_SUCCESS;
  if (0 == STRCMP(str, "append")) {
    policy = ObLogWritePolicy::LOG_APPEND_WRITE;
  } else if (0 == STRCMP(str, "truncate")) {
    policy = ObLogWritePolicy::LOG_TRUNCATE_WRITE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(str));
  }
  return ret;
}
} // namespace common
} // namespace oceanbase