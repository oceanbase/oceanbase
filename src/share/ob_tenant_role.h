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

#ifndef OCEANBASE_SHARE_OB_TENANT_ROLE_H_
#define OCEANBASE_SHARE_OB_TENANT_ROLE_H_

#include "lib/string/ob_string.h" // ObString
#include "lib/utility/ob_print_utils.h"             // TO_STRING_KV
#include "lib/utility/ob_unify_serialize.h"   // serialize
#include "lib/oblog/ob_log_module.h"      // LOG*

namespace oceanbase {
namespace share {

class ObTenantRole
{
  OB_UNIS_VERSION(1);
public:
  // Tenant Role
  static const int64_t INVALID_TENANT = 0;
  static const int64_t PRIMARY_TENANT = 1;
  static const int64_t STANDBY_TENANT = 2;
  static const int64_t RESTORE_TENANT = 3;
  static const int64_t MAX_TENANT = 4;
public:
  ObTenantRole() : value_(INVALID_TENANT) {}
  explicit ObTenantRole(const int64_t value) : value_(value) {}
  explicit ObTenantRole(const ObString &str);
  ~ObTenantRole() { reset(); }

public:
  void reset() { value_ = INVALID_TENANT; }
  bool is_valid() const { return INVALID_TENANT != value_; }
  int64_t value() const { return value_; }
  const char* to_str() const;

  // assignment
  ObTenantRole &operator=(const int64_t value) { value_ = value; return *this; }

  // compare operator
  bool operator == (const ObTenantRole &other) const { return value_ == other.value_; }
  bool operator != (const ObTenantRole &other) const { return value_ != other.value_; }

  // ObTenantRole attribute interface
  bool is_primary() const { return PRIMARY_TENANT == value_; }
  bool is_standby() const { return STANDBY_TENANT == value_; }
  bool is_restore() const { return RESTORE_TENANT == value_; }

  TO_STRING_KV(K_(value));
private:
  int64_t value_;
};

static const ObTenantRole INVALID_TENANT_ROLE(ObTenantRole::INVALID_TENANT);
static const ObTenantRole PRIMARY_TENANT_ROLE(ObTenantRole::PRIMARY_TENANT);
static const ObTenantRole STANDBY_TENANT_ROLE(ObTenantRole::STANDBY_TENANT);
static const ObTenantRole RESTORE_TENANT_ROLE(ObTenantRole::RESTORE_TENANT);

}  // share
}  // oceanbase

#endif /* OCEANBASE_SHARE_OB_TENANT_ROLE_H_ */
