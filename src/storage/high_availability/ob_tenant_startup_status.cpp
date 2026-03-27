/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_tenant_startup_status.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ObTenantStartupStatus::ObTenantStartupStatus()
  : is_in_service_(false)
{
}

int ObTenantStartupStatus::mtl_init(ObTenantStartupStatus *&tenant_startup_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_startup_status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant startup status should not be NULL", K(ret));
  }
  return ret;
}

bool ObTenantStartupStatus::is_in_service() const
{
  return ATOMIC_LOAD(&is_in_service_) && SERVER_STORAGE_META_SERVICE.is_started();
}

void ObTenantStartupStatus::start_service()
{
  ATOMIC_STORE(&is_in_service_, true);
}

void ObTenantStartupStatus::stop_service()
{
  ATOMIC_STORE(&is_in_service_, false);
}
