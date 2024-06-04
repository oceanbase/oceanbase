/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_resource_rpc_executor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/resource/ob_table_load_resource_service.h"
#include "observer/table_load/resource/ob_table_load_resource_manager.h"

namespace oceanbase
{
namespace observer
{

// apply_resource
int ObDirectLoadResourceApplyExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(arg_.tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl_id not match", KR(ret), "mtl_id", MTL_ID());
  }

  return ret;
}

int ObDirectLoadResourceApplyExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadResourceService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(ObTableLoadResourceService::local_apply_resource(arg_, res_))) {
    LOG_WARN("fail to apply resource", KR(ret));
  }

  return ret;
}

// release_resource
int ObDirectLoadResourceReleaseExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(arg_.tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl_id not match", KR(ret), "mtl_id", MTL_ID());
  }

  return ret;
}

int ObDirectLoadResourceReleaseExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadResourceService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(ObTableLoadResourceService::local_release_resource(arg_))) {
    LOG_WARN("fail to release resource", KR(ret));
  }

  return ret;
}

// update_resource
int ObDirectLoadResourceUpdateExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(arg_.tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl_id not match", KR(ret), "mtl_id", MTL_ID());
  }

  return ret;
}

int ObDirectLoadResourceUpdateExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadResourceService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(ObTableLoadResourceService::local_update_resource(arg_))) {
    LOG_WARN("fail to update resource", KR(ret));
  }

  return ret;
}

// check_resource
int ObDirectLoadResourceCheckExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(arg_.tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl_id not match", KR(ret), "mtl_id", MTL_ID());
  }

  return ret;
}

int ObDirectLoadResourceCheckExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadResourceService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::refresh_and_check_resource(arg_, res_))) {
    LOG_WARN("fail to refresh_and_check_resource", KR(ret));
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
