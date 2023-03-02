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

#define USING_LOG_PREFIX RS
#include "ob_tenant_info_report.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ob_share_util.h"//ObShareUtil
#include "observer/ob_server_struct.h"//GCTX
#include "share/ls/ob_ls_recovery_stat_operator.h"//ObLSRecoveryStatOperator

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{
//////////////ObTenantInfoReportor

int ObTenantInfoReportor::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("TenInfoRep",
          lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObTenantInfoReportor::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
}

void ObTenantInfoReportor::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t idle_time_us = 500 * 1000L;
    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      if (is_sys_tenant(tenant_id_)) {
        idle_time_us = 3600 * 1000 * 1000L;
      } else if (OB_FAIL(gather_tenant_recovery_stat_())) {
        LOG_WARN("failed to gather tenant recovery stat", KR(ret), K(tenant_id_));
      }
      idle(idle_time_us);
    }// end while
  }
}

int ObTenantInfoReportor::gather_tenant_recovery_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_meta_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys or user tenant no need gather tenant recovery stat", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObLSRecoveryStatOperator ls_recovery_op;
    ObAllTenantInfoProxy info_proxy;
    ObAllTenantInfo tenant_info;
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    SCN sync_scn;
    SCN readable_scn;
    DEBUG_SYNC(BLOCK_TENANT_SYNC_SNAPSHOT_INC);
    if (OB_FAIL(ls_recovery_op.get_tenant_recovery_stat(
            user_tenant_id, *GCTX.sql_proxy_, sync_scn, readable_scn))) {
      LOG_WARN("failed to get tenant recovery stat", KR(ret), K(user_tenant_id));
      //TODO replayable_scn is equal to sync_scn
    } else if (OB_FAIL(info_proxy.update_tenant_recovery_status(
                   user_tenant_id, GCTX.sql_proxy_, sync_scn,
                   sync_scn, readable_scn))) {
      LOG_WARN("failed to update tenant recovery stat", KR(ret),
               K(user_tenant_id), K(sync_scn), K(readable_scn));
    }
  }
  return ret;
}
}
}
