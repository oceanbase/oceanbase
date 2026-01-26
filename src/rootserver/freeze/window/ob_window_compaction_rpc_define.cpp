/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "rootserver/freeze/window/ob_window_compaction_rpc_define.h"
#include "rootserver/freeze/ob_major_freeze_service.h"

namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObWindowCompactionRequest, tenant_id_, param_);
OB_SERIALIZE_MEMBER(ObWindowCompactionResponse, err_code_);

int ObWindowCompactionRequest::init(
    const uint64_t tenant_id,
    const rootserver::ObWindowCompactionParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(param));
  } else {
    tenant_id_ = tenant_id;
    param_ = param;
  }
  return ret;
}

int ObTenantWindowCompactionP::process()
{
  int ret = OB_SUCCESS;
  ObWindowCompactionRequest &req = arg_;
  ObWindowCompactionResponse &res = result_;
  rootserver::ObMajorFreezeService *major_freeze_service = nullptr;
  bool is_primary_service = true;

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", KR(ret), K(req));
  } else if (OB_UNLIKELY(req.tenant_id() != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "mtl_id not match", KR(ret), K(req), "mtl_id", MTL_ID());
  } else {
    if (nullptr == primary_major_freeze_service_) {
      if (OB_ISNULL(primary_major_freeze_service_ = MTL(rootserver::ObPrimaryMajorFreezeService*))) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(ERROR, "primary_major_freeze_service is nullptr", KR(ret), K(req));
      }
    }
    if (nullptr == restore_major_freeze_service_) {
      if (OB_ISNULL(restore_major_freeze_service_ = MTL(rootserver::ObRestoreMajorFreezeService*))) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(ERROR, "restore_major_freeze_service is nullptr", KR(ret), K(req));
      }
    }
  }

  if (FAILEDx(rootserver::ObMajorFreezeUtil::get_major_freeze_service(primary_major_freeze_service_,
        restore_major_freeze_service_, major_freeze_service, is_primary_service))) {
    RS_LOG(WARN, "fail to get major freeze service", KR(ret), K(req));
  } else if (OB_ISNULL(major_freeze_service)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "major_freeze_service is null", KR(ret), K(req));
  } else if (is_primary_service) {
    if (OB_UNLIKELY(req.tenant_id() != major_freeze_service->get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "tenant_id does not match", K(req),
            "local_tenant_id", major_freeze_service->get_tenant_id(), K(is_primary_service));
    } else if (OB_FAIL(major_freeze_service->launch_window_compaction(req.param()))) {
      if (OB_MAJOR_FREEZE_NOT_FINISHED != ret && (OB_FROZEN_INFO_ALREADY_EXIST != ret)) {
        RS_LOG(WARN, "fail to launch_window_compaction", KR(ret), K(req), K(is_primary_service));
      }
    } else {
      RS_LOG(INFO, "launch_window_compaction succ", K(req), K(is_primary_service), K(req));
    }
  } else {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    RS_LOG(WARN, "fail to launch_window_compaction, forbidden in restore_major_freeze_service",
           KR(ret), K(req), K(is_primary_service));
  }
  res.err_code_ = ret;
  ret = OB_SUCCESS;
  return ret;
}

} // end namespace obrpc
} // end namespace rootserver