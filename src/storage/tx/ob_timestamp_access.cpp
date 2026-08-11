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
 
namespace oceanbase
{
namespace transaction
{

int ObTimestampAccess::handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
{
  int ret = OB_SUCCESS;
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
  return ret;
}

int ObTimestampAccess::get_number(int64_t &gts)
{
  int ret = OB_SUCCESS;
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
  return ret;
}

int ObTimestampAccess::get_number_with_target(const int64_t target_gts,
                                              int64_t &gts,
                                              bool &is_target_reached)
{
  int ret = OB_SUCCESS;
  is_target_reached = false;
  if (OB_UNLIKELY(target_gts <= 0 || target_gts >= INT64_MAX / 2)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid target gts", KR(ret), K(target_gts));
  } else if (GTS_LEADER == service_type_) {
    ret = MTL(ObTimestampService *)->get_timestamp_with_target(
        target_gts, gts, is_target_reached);
  } else if (STS_LEADER == service_type_) {
    if (OB_FAIL(MTL(ObStandbyTimestampService *)->get_number(gts))) {
      // Propagate the STS allocation failure unchanged.
    } else {
      // STS has no targeted allocation API, so preserve its normal allocation
      // and report separately whether the returned value reached the target.
      is_target_reached = gts >= target_gts;
    }
  } else {
    ret = OB_NOT_MASTER;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "ObTimestampAccess service type is FOLLOWER",
                K(ret), K_(service_type), K(target_gts));
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

}
}
