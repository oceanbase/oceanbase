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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_ls_op_processor.h"
#include "logservice/common_util/ob_log_ls_define.h"
#include "ob_log_instance.h"        // TCTX

namespace oceanbase
{
namespace libobcdc
{
int ObLogLSOpProcessor::process_ls_op(
    const uint64_t tenant_id,
    const palf::LSN &lsn,
    const int64_t start_tstamp_ns,
    const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;

  if (OB_UNLIKELY(! ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls attr is invalid", KR(ret), K(ls_attr));
  } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_WARN("get_tenant fail", KR(ret), K(tenant_id), K(guard));
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get tenant fail, tenant is NULL", KR(ret), K(tenant_id));
  } else if (share::is_ls_create_pre_op(ls_attr.get_ls_operation_type())) {
    LOG_INFO("[LS_PROCESSOR] ls create pre operation", K(tenant_id), K(lsn), K(start_tstamp_ns), K(ls_attr));
  } else if (share::is_ls_create_end_op(ls_attr.get_ls_operation_type())) {
    LOG_INFO("[LS_PROCESSOR] ls create end operation", K(tenant_id), K(lsn), K(start_tstamp_ns), K(ls_attr));
    //create new ls;
    if (TCONF.test_mode_on) {
      const int64_t block_add_ls_usec = TCONF.test_mode_block_add_ls_sec * _SEC_;
      if (0 < block_add_ls_usec) {
        ob_usleep(block_add_ls_usec);
        LOG_INFO("[TEST_MODE] block add ls", K(ls_attr), K(block_add_ls_usec));
      }
    }
    if (OB_FAIL(create_new_ls_(tenant, start_tstamp_ns, ls_attr))) {
      LOG_ERROR("failed to create new ls", KR(ret), K(tenant_id), K(lsn), K(start_tstamp_ns), K(ls_attr));
    }
  } else if (share::is_ls_create_abort_op(ls_attr.get_ls_operation_type())) {
  } else if (share::is_ls_drop_pre_op(ls_attr.get_ls_operation_type())) {
  } else if (share::is_ls_drop_end_op(ls_attr.get_ls_operation_type())) {
  } else if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operation_type())) {
    //only sys ls
  } else if (share::is_ls_tenant_drop_op(ls_attr.get_ls_operation_type())) {
  } else if (share::is_ls_alter_ls_group_op(ls_attr.get_ls_operation_type())) {
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation type", KR(ret), K(tenant_id), K(lsn), K(start_tstamp_ns), K(ls_attr));
  }

  return ret;
}

int ObLogLSOpProcessor::create_new_ls_(
    ObLogTenant *tenant,
    const int64_t start_tstamp_ns,
    const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! ls_attr.is_valid()
        || ! ls_attr.ls_is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ls_attr is invalid", KR(ret), K(ls_attr));
  } else {
    logservice::TenantLSID tls_id(tenant->get_tenant_id(), ls_attr.get_ls_id());
    const bool is_create_ls = true;

    if (OB_FAIL(tenant->get_ls_mgr().add_ls(tls_id, start_tstamp_ns, is_create_ls))) {
      LOG_ERROR("LsMgr add_ls failed", KR(ret), K(tls_id), K(start_tstamp_ns), K(is_create_ls));
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
