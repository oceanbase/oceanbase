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
#include "lib/net/tbnetutil.h"
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
{}

ObTenantTimezone::ObTenantTimezone(uint64_t tenant_id)
    : is_inited_(false),
      tenant_id_(tenant_id),
      tenant_tz_mgr_(nullptr),
      update_tz_task_(),
      tz_info_mgr_(nullptr),
      tz_info_map_(nullptr),
      ref_count_(0),
      update_task_not_exist_(false)
{}
ObTenantTimezone::~ObTenantTimezone()
{
  if (is_inited_) {
    ob_delete(tz_info_map_);
    ob_delete(tz_info_mgr_);
  }
}

int ObTenantTimezone::init(ObTenantTimezoneMgr* tz_mgr)
{
  int ret = OB_SUCCESS;
  tenant_tz_mgr_ = tz_mgr;
  if (OB_FAIL(update_tz_task_.init(tz_mgr, this))) {
    LOG_ERROR("init tenant time zone updata task failed", K_(tenant_id), K(ret));
  } else {
    is_inited_ = true;
    tz_info_map_ = OB_NEW(ObTZInfoMap, ObModIds::OMT);
    tz_info_mgr_ = OB_NEW(ObTimeZoneInfoManager,
        ObModIds::OMT,
        OBSERVER.get_common_rpc_proxy(),
        OBSERVER.get_mysql_proxy(),
        OBSERVER.get_root_service(),
        *tz_info_map_,
        tenant_id_);
    if (OB_ISNULL(tz_info_map_) || OB_ISNULL(tz_info_mgr_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate mem for tz_info", K(ret), K(tz_info_map_), K(tz_info_mgr_));
    } else if (OB_FAIL(tz_info_map_->init(ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP))) {
      LOG_WARN("fail to init tz_info_map_", K(ret));
    } else if (OB_FAIL(tz_info_mgr_->init())) {
      LOG_WARN("fail to init tz_info_mgr_", K(ret));
    } else {
      LOG_INFO("tenant timezone init", K(tz_info_map_), K(tenant_id_));
    }
  }
  return ret;
}

void ObTenantTimezone::TenantTZUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (task_lock_.tryLock()) {
    if (!tenant_tz_mgr_->get_start_refresh()) {
      const int64_t delay = 1 * 1000 * 1000;
      const bool repeat = false;
      if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, *this, delay, repeat))) {
        LOG_WARN("schedule timezone update task failed", K(ret));
      }
    } else {
      const int64_t delay = 5 * 1000 * 1000;
      const bool repeat = false;
      if (OB_FAIL(tenant_tz_->get_tz_mgr()->fetch_time_zone_info())) {
        LOG_WARN("fail to update time zone info", K(ret));
      }
      if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, *this, delay, repeat))) {
        LOG_WARN("schedule timezone update task failed", K(ret));
      }
    }
    task_lock_.unlock();
  } else {
    // deleted already
  }
}

int ObTenantTimezone::get_ref_count(int64_t& ref_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tz_info_map_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ref_count = tz_info_map_->get_ref_count();
  }
  return ret;
}

}  // namespace omt
}  // namespace oceanbase
