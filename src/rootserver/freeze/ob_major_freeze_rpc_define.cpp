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

#include "rootserver/freeze/ob_major_freeze_rpc_define.h"
#include "share/rc/ob_tenant_base.h"
#include "rootserver/freeze/ob_major_freeze_service.h"

namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObSimpleFreezeInfo, tenant_id_);

OB_SERIALIZE_MEMBER(ObMajorFreezeRequest, info_);

OB_SERIALIZE_MEMBER(ObMajorFreezeResponse, err_code_);

OB_SERIALIZE_MEMBER(ObTenantAdminMergeRequest, tenant_id_, type_);

OB_SERIALIZE_MEMBER(ObTenantAdminMergeResponse, err_code_);

int ObTenantMajorFreezeP::process()
{
  int ret = OB_SUCCESS;
  ObMajorFreezeRequest &req = arg_;
  ObMajorFreezeResponse &res = result_;

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (OB_UNLIKELY(req.tenant_id() != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "mtl_id not match", K(ret), K(req), "mtl_id", MTL_ID());
  } else if (nullptr == major_freeze_service_) {
    if (OB_ISNULL(major_freeze_service_ = MTL(rootserver::ObMajorFreezeService*))) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(ERROR, "major_freeze_service is nullptr", KR(ret), K(req));
    }
  }

  if (OB_SUCC(ret) && (nullptr != major_freeze_service_)) {
    if (OB_UNLIKELY(req.tenant_id() != major_freeze_service_->get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "tenant_id does not match", K(req), 
             "local_tenant_id", major_freeze_service_->get_tenant_id());
    } else if (OB_FAIL(major_freeze_service_->launch_major_freeze())) {
      if ((OB_MAJOR_FREEZE_NOT_FINISHED != ret) && (OB_FROZEN_INFO_ALREADY_EXIST != ret)) {
        RS_LOG(WARN, "fail to launch_major_freeze", KR(ret), K(req));
      }
    } else {
      RS_LOG(INFO, "launch_major_freeze succ", K(req));
    }
  }
  res.err_code_ = ret;
  ret = OB_SUCCESS;
  return ret;
}

int ObTenantAdminMergeP::process()
{
  int ret = OB_SUCCESS;
  ObTenantAdminMergeRequest &req = arg_;
  ObTenantAdminMergeResponse &res = result_;

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (OB_UNLIKELY(req.tenant_id() != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "mtl_id not match", K(ret), K(req), "mtl_id", MTL_ID());
  } else if (nullptr == major_freeze_service_) {
    if (OB_ISNULL(major_freeze_service_ = MTL(rootserver::ObMajorFreezeService*))) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(ERROR, "major_freeze_service is nullptr", K(ret), K(req));
    }
  }
  
  if (OB_SUCC(ret) && (nullptr != major_freeze_service_)) {
    if (OB_UNLIKELY(req.tenant_id() != major_freeze_service_->get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "tenant_id does not match", K(req), 
             "local_tenant_id", major_freeze_service_->get_tenant_id());
    } else {
      switch(req.get_type()) {
        case ObTenantAdminMergeType::SUSPEND_MERGE:
          if (OB_FAIL(major_freeze_service_->suspend_merge())) {
            RS_LOG(WARN, "fail to suspend merge", KR(ret), K(req));
          }
          break;
        case ObTenantAdminMergeType::RESUME_MERGE:
          if (OB_FAIL(major_freeze_service_->resume_merge())) {
            RS_LOG(WARN, "fail to resume merge", KR(ret), K(req));
          }
          break;
        case ObTenantAdminMergeType::CLEAR_MERGE_ERROR:
          if (OB_FAIL(major_freeze_service_->clear_merge_error())) {
            RS_LOG(WARN, "fail to clear merge error", KR(ret), K(req));
          }
          break;
        default:
          break;
      }
      if (OB_SUCC(ret)) {
        RS_LOG(INFO, "succ to execute tenant admin merge", K(req));
      }
    }
  }
  res.err_code_ = ret;
  ret = OB_SUCCESS;
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
