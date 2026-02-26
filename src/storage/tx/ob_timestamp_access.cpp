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

#include "ob_timestamp_access.h"
#include "ob_timestamp_service.h"
#include "ob_standby_timestamp_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/sslog/ob_sslog_gts_service.h"
#include "storage/incremental/sslog/ob_sslog_uid_service.h"
#endif
 
namespace oceanbase
{
namespace transaction
{

int ObTimestampAccess::handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
{
  int ret = OB_SUCCESS;
  if (request.is_sslog_request()) {
    if (!is_sslog_leader_) {
      ret = OB_NOT_MASTER;
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(INFO, "sslog gts service is FOLLOWER", K(ret), K_(is_sslog_leader));
      }
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      ret = MTL(ObSSLogGTSService *)->handle_request(request, result);
#else
      ret = OB_ERR_UNEXPECTED;
#endif
    }
  } else {
    if (GTS_LEADER == service_type_) {
      ret = MTL(ObTimestampService *)->handle_request(request, result);
    } else if (STS_LEADER == service_type_) {
      ret = MTL(ObStandbyTimestampService *)->handle_request(request, result);
    } else {
      ret = OB_NOT_MASTER;
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(INFO, "ObTimestampAccess service type is FOLLOWER", K(ret), K_(service_type));
      }
    }
  }
  return ret;
}

int ObTimestampAccess::get_number(int64_t &gts, const bool sslog_gts)
{
  int ret = OB_SUCCESS;
  if (sslog_gts) {
    ret = get_number_from_sslog(gts);
  } else {
    if (GTS_LEADER == service_type_) {
      ret = MTL(ObTimestampService *)->get_timestamp(gts);
    } else if (STS_LEADER == service_type_) {
      ret = MTL(ObStandbyTimestampService *)->get_number(gts);
    } else {
      ret = OB_NOT_MASTER;
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "ObTimestampAccess service type is FOLLOWER", K(ret), K_(service_type));
      }
    }
  }
  return ret;
}

int ObTimestampAccess::get_number_from_sslog(int64_t &gts)
{
  int ret = OB_SUCCESS;
  if (is_sslog_leader_) {
#ifdef OB_BUILD_SHARED_STORAGE
    ret = MTL(ObSSLogGTSService *)->get_timestamp(gts);
#else
    ret = OB_ERR_UNEXPECTED;
#endif
  } else {
    ret = OB_NOT_MASTER;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "sslog gts service type is FOLLOWER", K(ret), K_(is_sslog_leader));
    }
  }
  return ret;
}

void ObTimestampAccess::get_virtual_info(int64_t &ts_value,
                                         ServiceType &service_type,
                                         common::ObRole &role,
                                         int64_t &proposal_id)
{
  service_type = service_type_;
  if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
    MTL(ObTimestampService *)->get_virtual_info(ts_value, role, proposal_id);
  } else {
    MTL(ObStandbyTimestampService *)->get_virtual_info(ts_value, role, proposal_id);
  }
}

void ObTimestampAccess::set_sslog_leader(const bool is_sslog_leader)
{
  is_sslog_leader_ = is_sslog_leader;
}

} // transaction
} // oceanbase
