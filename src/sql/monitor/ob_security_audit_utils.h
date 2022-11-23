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

#ifndef OCEANBASE_SQL_SECURITY_AUDIT_UTILS_H
#define OCEANBASE_SQL_SECURITY_AUDIT_UTILS_H

#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "sql/monitor/ob_audit_action_type.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
enum class ObAuditTrailType{
  INVALID = 0,
  NONE,
};

ObAuditTrailType get_audit_trail_type_from_string(const common::ObString &string);

struct ObAuditUnit
{
  ObAuditUnit() {}
  ~ObAuditUnit() {}
  TO_STRING_KV(K(""));
};

class ObSecurityAuditUtils final
{
public:
  static int check_allow_audit(ObSQLSessionInfo &session, ObAuditTrailType &at_type);
  static int get_audit_file_name(char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos);
};
}
} //namespace oceanbase
#endif


