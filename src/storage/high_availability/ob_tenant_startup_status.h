/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_TENANT_STARTUP_STATUS_
#define OCEABASE_STORAGE_TENANT_STARTUP_STATUS_
#include "share/ob_define.h"

namespace oceanbase
{
namespace storage
{

class ObTenantStartupStatus final
{
public:
  ObTenantStartupStatus();
  ~ObTenantStartupStatus() = default;
  static int mtl_init(ObTenantStartupStatus *&tenant_startup_status);
  bool is_in_service() const;
  void start_service();
  void stop_service();
  void destroy() {}
private:
  bool is_in_service_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantStartupStatus);
};

#define TENANT_STARTUP_STATUS (*MTL(ObTenantStartupStatus *))

} //storage
} //ocenabase

#endif
