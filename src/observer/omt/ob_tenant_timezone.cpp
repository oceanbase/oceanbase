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
#include "observer/ob_server.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {

ObTenantTimezone::ObTenantTimezone() : tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObTenantTimezone::ObTenantTimezone(uint64_t tenant_id)
    : is_inited_(false), tenant_id_(tenant_id), tenant_tz_mgr_(nullptr),
    tz_info_mgr_(nullptr), tz_info_map_(nullptr), update_task_not_exist_(false)
{
}
ObTenantTimezone::~ObTenantTimezone()
{
}

int ObTenantTimezone::init(ObTenantTimezoneMgr *tz_mgr)
{
  int ret = OB_SUCCESS;
  tenant_tz_mgr_ = tz_mgr;
  is_inited_ = true;
  tz_info_map_ = OB_NEW(ObTZInfoMap, "TZInfoMap");
  tz_info_mgr_ = OB_NEW(ObTimeZoneInfoManager, "TZInfoMgr", OBSERVER.get_common_rpc_proxy(),
                    OBSERVER.get_mysql_proxy(), OBSERVER.get_root_service(),
                    *tz_info_map_, tenant_id_);
  if (OB_ISNULL(tz_info_map_) || OB_ISNULL(tz_info_mgr_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mem for tz_info", K(ret), K(tz_info_map_), K(tz_info_mgr_));
  } else if (OB_FAIL(tz_info_map_->init("TZInfoMap"))) {
    LOG_WARN("fail to init tz_info_map_", K(ret));
  } else if (OB_FAIL(tz_info_mgr_->init())) {
    LOG_WARN("fail to init tz_info_mgr_", K(ret));
  } else {
    LOG_INFO("tenant timezone init", K(tz_info_map_), K(tenant_id_));
  }
  return ret;
}

void ObTenantTimezone::destroy()
{
  if (NULL != tz_info_map_) {
    tz_info_map_->destroy();
  }
}

} // omt
} // oceanbase
