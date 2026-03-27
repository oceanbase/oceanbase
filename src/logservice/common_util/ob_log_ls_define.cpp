/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_ls_define.h"

namespace oceanbase
{
namespace logservice
{
int TenantLSID::compare(const TenantLSID &other) const
{
  int cmp_ret = 0;

  if (tenant_id_ > other.tenant_id_) {
    cmp_ret = 1;
  } else if (tenant_id_ < other.tenant_id_) {
    cmp_ret = -1;
  } else if (ls_id_ > other.ls_id_) {
    cmp_ret = 1;
  } else if (ls_id_ < other.ls_id_) {
    cmp_ret = -1;
  } else {
    cmp_ret = 0;
  }

  return cmp_ret;
}

TenantLSID &TenantLSID::operator=(const TenantLSID &other)
{
  this->tenant_id_ = other.get_tenant_id();
  this->ls_id_ = other.get_ls_id();
  return *this;
}

} // namespace logservice
} // namespace oceanbase
