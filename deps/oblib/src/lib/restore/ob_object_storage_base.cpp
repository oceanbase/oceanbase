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

#include "ob_object_storage_base.h"

namespace oceanbase
{
namespace common
{

thread_local uint64_t ObObjectStorageTenantGuard::tl_tenant_id_ = OB_SERVER_TENANT_ID;
thread_local int64_t ObObjectStorageTenantGuard::tl_timeout_us_ = OB_STORAGE_MAX_IO_TIMEOUT_US;
thread_local uint64_t ObObjectStorageTenantGuard::tl_io_group_id_ = USER_RESOURCE_OTHER_GROUP_ID;

ObObjectStorageTenantGuard::ObObjectStorageTenantGuard(
    const uint64_t tenant_id, const int64_t timeout_us)
    : old_tenant_id_(tl_tenant_id_),
      old_timeout_us_(tl_timeout_us_),
      old_io_group_id_(tl_io_group_id_)
{
  tl_tenant_id_ = tenant_id;
  tl_timeout_us_ = timeout_us;
}

ObObjectStorageTenantGuard::ObObjectStorageTenantGuard(
    const uint64_t tenant_id, const int64_t timeout_us, const uint64_t group_id)
    : old_tenant_id_(tl_tenant_id_),
      old_timeout_us_(tl_timeout_us_),
      old_io_group_id_(tl_io_group_id_)
{
  tl_tenant_id_ = tenant_id;
  tl_timeout_us_ = timeout_us;
  tl_io_group_id_ = group_id;
}

ObObjectStorageTenantGuard::~ObObjectStorageTenantGuard()
{
  tl_tenant_id_ = old_tenant_id_;
  tl_timeout_us_ = old_timeout_us_;
  tl_io_group_id_ = old_io_group_id_;
}

uint64_t ObObjectStorageTenantGuard::get_tenant_id()
{
  if (OB_UNLIKELY(tl_tenant_id_ == OB_SERVER_TENANT_ID)
      && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    // log once at a minimum interval of 1s
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tenant id is 500", K(tl_tenant_id_), K(lbt()));
  }
  return tl_tenant_id_;
}

int64_t ObObjectStorageTenantGuard::get_timeout_us()
{
  return tl_timeout_us_;
}

uint64_t ObObjectStorageTenantGuard::get_io_group_id()
{
  return tl_io_group_id_;
}

int64_t ObObjectStorageTenantGuard::get_timeout_ms()
{
  return tl_timeout_us_ / 1000;
}

} // common
} // oceanbase
