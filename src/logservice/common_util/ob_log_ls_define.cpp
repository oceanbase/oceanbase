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
