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

#define USING_LOG_PREFIX SERVER_OMT

#include "common/ob_common_utility.h"
#include "lib/net/ob_net_util.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_timezone.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_time_zone_info_manager.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {
ObTenantTimezone::ObTenantTimezone(common::ObMySQLProxy &sql_proxy, uint64_t tenant_id)
    : is_inited_(false), tenant_id_(tenant_id),
    tz_info_mgr_(sql_proxy, tenant_id), update_task_not_exist_(false)
{
}
ObTenantTimezone::~ObTenantTimezone()
{
}

int ObTenantTimezone::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(tenant_id_));
  } else if (OB_FAIL(tz_info_mgr_.init())) {
    LOG_WARN("fail to init tz_info_mgr_", K(ret));
  } else {
    is_inited_ = true;
  }
  LOG_INFO("tenant timezone init", K(ret), K(tenant_id_), K(sizeof(ObTimeZoneInfoManager)));
  return ret;
}

} // omt
} // oceanbase
