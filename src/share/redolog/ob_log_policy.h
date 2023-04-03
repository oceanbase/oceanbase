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

#ifndef OCEANBASE_COMMON_OB_LOG_POLICY_H_
#define OCEANBASE_COMMON_OB_LOG_POLICY_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
enum ObLogRetryWritePolicy
{
  INVALID_RETRY_WRITE,
  NORMAL_WRITE,
  SWITCH_FILE_WRITE // append only
};

enum ObLogCreatePolicy
{
  INVALID_CREATE,
  NORMAL_CREATE,
  PRE_CREATE
};

enum ObLogWritePolicy
{
  INVALID_WRITE,
  LOG_APPEND_WRITE,
  LOG_TRUNCATE_WRITE
};

struct ObLogPolicyCollection
{
  ObLogRetryWritePolicy retry_write_policy_;
  ObLogCreatePolicy log_create_policy_;
  ObLogWritePolicy log_write_policy_;

  TO_STRING_KV(K_(retry_write_policy), K_(log_create_policy), K_(log_write_policy));
};

class ObLogPolicyParser
{
public:
  static int parse_retry_write_policy(const char *str, ObLogRetryWritePolicy &policy);
  static int parse_create_policy(const char *str, ObLogCreatePolicy &policy);
  static int parse_log_write_policy(const char *str, ObLogWritePolicy &policy);
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_POLICY_H_
