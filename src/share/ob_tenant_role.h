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
  enum Role
  {
    INVALID_TENANT = 0,
    PRIMARY_TENANT = 1,
    STANDBY_TENANT = 2,
    RESTORE_TENANT = 3,
    CLONE_TENANT   = 4,
    MAX_TENANT = 5,
  };
public:
  ObTenantRole() : value_(INVALID_TENANT) {}
  explicit ObTenantRole(const Role value) : value_(value) {}
  explicit ObTenantRole(const ObString &str);
  ~ObTenantRole() { reset(); }

public:
  void reset() { value_ = INVALID_TENANT; }
  bool is_valid() const { return INVALID_TENANT != value_; }
  Role value() const { return value_; }
  const char* to_str() const;

  // assignment
  ObTenantRole &operator=(const Role value) { value_ = value; return *this; }

  // compare operator
  bool operator == (const ObTenantRole &other) const { return value_ == other.value_; }
  bool operator != (const ObTenantRole &other) const { return value_ != other.value_; }

  // ObTenantRole attribute interface
  bool is_primary() const { return PRIMARY_TENANT == value_; }
  bool is_standby() const { return STANDBY_TENANT == value_; }
  bool is_restore() const { return RESTORE_TENANT == value_; }
  bool is_clone() const { return CLONE_TENANT == value_; }

  TO_STRING_KV(K_(value));
  DECLARE_TO_YSON_KV;
private:
  Role value_;
};

#define GEN_IS_TENANT_ROLE_DECLARE(TENANT_ROLE_VALUE, TENANT_ROLE) \
  bool is_##TENANT_ROLE##_tenant(const ObTenantRole::Role value);

GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::INVALID_TENANT, invalid)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::PRIMARY_TENANT, primary)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::STANDBY_TENANT, standby)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::RESTORE_TENANT, restore)
GEN_IS_TENANT_ROLE_DECLARE(ObTenantRole::Role::CLONE_TENANT, clone)
#undef GEN_IS_TENANT_ROLE_DECLARE

static const ObTenantRole INVALID_TENANT_ROLE(ObTenantRole::INVALID_TENANT);
static const ObTenantRole PRIMARY_TENANT_ROLE(ObTenantRole::PRIMARY_TENANT);
static const ObTenantRole STANDBY_TENANT_ROLE(ObTenantRole::STANDBY_TENANT);
static const ObTenantRole RESTORE_TENANT_ROLE(ObTenantRole::RESTORE_TENANT);
static const ObTenantRole CLONE_TENANT_ROLE(ObTenantRole::CLONE_TENANT);

}  // share
}  // oceanbase

#endif /* OCEANBASE_SHARE_OB_TENANT_ROLE_H_ */
