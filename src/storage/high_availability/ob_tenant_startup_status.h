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
