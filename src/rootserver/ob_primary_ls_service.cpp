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
#include "ob_primary_ls_service.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ls/ob_ls_creator.h" //ObLSCreator
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "share/ob_share_util.h"//ObShareUtil
#include "observer/ob_server_struct.h"//GCTX
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo
#include "rootserver/ob_ls_service_helper.h"//ObTenantLSInfo

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{

//////////////ObPrimaryLSService
int ObPrimaryLSService::mtl_init(ObPrimaryLSService *&ka)
{
  return ka->init();
}

int ObPrimaryLSService::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("PLSSer", 
          lib::TGDefIDs::LSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObPrimaryLSService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
}

void ObPrimaryLSService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(tenant_id_, DATA_VERSION_4_1_0_0))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(tenant_id_), K(DATA_CURRENT_VERSION));
  } else {
    int64_t idle_time_us = 100 * 1000L;
    int tmp_ret = OB_SUCCESS;
    const uint64_t thread_idx = get_thread_idx();
    share::schema::ObTenantSchema tenant_schema;
    palf::PalfBufferIterator iterator;
    SCN start_scn;
    while (!has_set_stop()) {
      tenant_schema.reset();
      ObCurTraceId::init(GCONF.self_addr_);
      if (0 == thread_idx) {
        if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
          LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
        } else if (OB_TMP_FAIL(process_user_tenant_thread0_(tenant_schema))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to process user tenant thread0", KR(ret),
                   KR(tmp_ret), K(tenant_id_));
        }
      } else if (OB_TMP_FAIL(
                     process_user_tenant_thread1_(iterator, start_scn))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to process user tenant thread1", KR(ret), KR(tmp_ret),
                 K(tenant_id_), K(start_scn));
      }

      tenant_schema.reset();
      idle(idle_time_us);
      LOG_INFO("[PRIMARY_LS_SERVICE] finish one round", KR(ret), K(idle_time_us), K(thread_idx));
    }  // end while
  }
}


int ObPrimaryLSService::process_user_tenant_thread0_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, &tenant_schema, tenant_id,
                             GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
  int64_t task_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (tenant_schema.is_creating()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("tenant schema not ready, no need process", KR(ret), K(tenant_schema));
  } else if (tenant_schema.is_dropping()) {
    //if tenant schema is in dropping
    //set the creating ls to create_abort,
    //set the normal or dropping tenant to drop_tennat_pre
    if (OB_FAIL(tenant_stat.drop_tenant())) {
      LOG_WARN("failed to drop tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_stat.process_next_ls_status(task_cnt))) {
      LOG_WARN("failed to process next ls status", KR(ret), KR(tmp_ret));
    }
  } else {
    //normal tenant
    //some ls may failed to create ls, but can continue
    if (OB_SUCCESS != (tmp_ret = tenant_stat.process_next_ls_status(task_cnt))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to process next ls status", KR(ret), KR(tmp_ret));
    } else if (0 == task_cnt) {
      //process each ls group and primary zone is matching
      //process each unit group has the right ls group
      //overwrite ret
      if (OB_SUCCESS != (tmp_ret = tenant_stat.gather_stat(false))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to gather stat", KR(ret), KR(tmp_ret), K(tenant_id));
      } else {
        if (OB_SUCCESS != (tmp_ret = tenant_stat.check_ls_match_primary_zone())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to check ls match with primary zone", KR(ret), KR(tmp_ret));
        }
        if (OB_SUCCESS != (tmp_ret = tenant_stat.check_ls_group_match_unitnum())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to check ls group match unitnum", KR(ret), KR(tmp_ret));
        }
      }
    }
  }

  LOG_INFO("[PRIMARY_LS_SERVICE_THREAD0] finish process tenant",
      KR(ret), KR(tmp_ret), K(tenant_id), K(task_cnt), K(tenant_schema));
  return ret;
}

int ObPrimaryLSService::process_user_tenant_thread1_(palf::PalfBufferIterator &iterator,
                                  share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls recovery thread must run on user tenant", KR(ret),
             K(tenant_id_));
  } else {
    ObRecoveryLSHelper recovery_ls(tenant_id_, GCTX.sql_proxy_);
    if (OB_FAIL(recovery_ls.do_work(iterator, start_scn))) {
      LOG_WARN("failed to recovery ls", KR(ret), K(start_scn));
    }
  }
  LOG_INFO("[PRIMARY_LS_SERVICE_THREAD1] finish process tenant", KR(ret), K(start_scn));

  return ret;
}

}//end of rootserver
}

