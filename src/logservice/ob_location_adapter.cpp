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

#include "ob_location_adapter.h"
#include "share/location_cache/ob_location_service.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace share;
namespace logservice
{

ObLocationAdapter::ObLocationAdapter() :
    is_inited_(false),
    location_service_(NULL)
  {}

ObLocationAdapter::~ObLocationAdapter()
{
  destroy();
}

int ObLocationAdapter::init(ObLocationService *location_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLocationAdapter init twice", K(ret));
  } else if (OB_ISNULL(location_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(location_service), K(ret));
  } else {
    location_service_ = location_service;
    is_inited_ = true;
    PALF_LOG(INFO, "ObLocationAdapter init success", K(ret), K(location_service_));
  }
  return ret;
}

void ObLocationAdapter::destroy()
{
  is_inited_ = false;
  location_service_ = NULL;
}

int ObLocationAdapter::get_leader(const int64_t id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  uint64_t tenant_id = MTL_ID();
  ObLSID ls_id(id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "location adapter is not inited", K(ret));
  } else if (OB_FAIL(location_service_->get_leader(
                     cluster_id, tenant_id, ls_id, false, leader))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "location adapter get_leader failed", K(ret), K(id), K(leader));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLocationAdapter::nonblock_get_leader(int64_t id,
                                           common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObLSID ls_id(id);
  ret = nonblock_get_leader(tenant_id, id, leader);
  return ret;
}

int ObLocationAdapter::nonblock_renew_leader(int64_t id)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ret = nonblock_renew_leader(tenant_id, id);
  return ret;
}

int ObLocationAdapter::nonblock_get_leader(const uint64_t tenant_id,
                                           int64_t id,
                                           common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  ObLSID ls_id(id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "location adapter is not inited", K(ret));
  } else if (OB_FAIL(location_service_->nonblock_get_leader(
                     cluster_id, tenant_id, ls_id, leader))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "location adapter nonblock_get_leader failed", K(ret), K(id), K(leader));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLocationAdapter::nonblock_renew_leader(const uint64_t tenant_id, int64_t id)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  ObLSID ls_id(id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "location adapter is not inited", K(ret));
  } else if (OB_FAIL(location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(WARN, "location adapter nonblock_renew_leader failed", K(ret), K(id));
    }
  } else {
    // do nothing
  }
  return ret;
}
}
}
