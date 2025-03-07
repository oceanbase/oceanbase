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
#include "src/storage/tx_storage/ob_ls_map.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_common_ls_service.h"
#include "share/ob_srv_rpc_proxy.h" // ObSrvRpcProxy
#include "share/location_cache/ob_location_service.h" //ObLocationService

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{

//////////////ObPrimaryLSService
int ObPrimaryLSService::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("PLSSer", 
          lib::TGDefIDs::SimpleLSService, *this))) {
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
    int64_t idle_time_us = 1000 * 1000L;
    int tmp_ret = OB_SUCCESS;
    share::schema::ObTenantSchema tenant_schema;
    while (!has_set_stop()) {
      tenant_schema.reset();
      ObCurTraceId::init(GCONF.self_addr_);
      DEBUG_SYNC(STOP_PRIMARY_LS_THREAD);
      if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
        LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
      } else {
        if (OB_TMP_FAIL(process_all_ls(tenant_schema))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to process user tenant thread0", KR(ret),
              KR(tmp_ret), K(tenant_id_));
        }
        if (OB_TMP_FAIL(process_all_ls_status_to_steady_(tenant_schema))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to process user tenant thread1", KR(ret), KR(tmp_ret),
              K(tenant_id_));
        }
      }

      LOG_INFO("[PRIMARY_LS_SERVICE] finish one round", KR(ret), K(tenant_schema));
      tenant_schema.reset();
      idle(idle_time_us);
    }// end while
  }
}


int ObPrimaryLSService::process_all_ls(const share::schema::ObTenantSchema &tenant_schema,
    bool in_creating_tenant)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  common::ObArray<ObLSStatusMachineParameter> machine_array;
  int64_t task_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (tenant_schema.is_creating() && !in_creating_tenant) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("tenant schema not ready, no need process", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(ObLSServiceHelper::construct_ls_status_machine(false, tenant_id,
             GCTX.sql_proxy_, machine_array))) {
    LOG_WARN("failed to construct ls status machine", KR(ret), K(tenant_id));
  } else if (tenant_schema.is_dropping()) {
    //if tenant schema is in dropping
    //set the creating ls to create_abort,
    //set the normal or dropping tenant to drop_tennat_pre
    if (OB_FAIL(set_tenant_dropping_status_(machine_array, task_cnt))) {
      LOG_WARN("failed to set tenant dropping status", KR(ret), K(task_cnt), K(machine_array));
    }
  }
  if (OB_SUCC(ret) && 0 == task_cnt) {
    if (OB_FAIL(try_set_next_ls_status_(machine_array))) {
      LOG_WARN("failed to set next ls status", KR(ret), K(machine_array));
    }
  }

  LOG_INFO("[PRIMARY_LS_SERVICE] finish process tenant",
      KR(ret), K(tenant_id), K(task_cnt), K(machine_array), K(tenant_schema));
  return ret;
}

int ObPrimaryLSService::set_tenant_dropping_status_(
    const common::ObIArray<ObLSStatusMachineParameter> &status_machine_array, int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_info_loader is null", KR(ret), KP(tenant_info_loader));
  } else {
    share::ObLSAttrOperator ls_operator(MTL_ID(), GCTX.sql_proxy_);
    const ObTenantSwitchoverStatus working_sw_status = share::NORMAL_SWITCHOVER_STATUS;
    share::SCN tenant_sync_scn, sys_ls_target_scn;
    tenant_sync_scn.set_invalid();
    sys_ls_target_scn.set_invalid();
    for (int64_t i = 0; OB_SUCC(ret) && i < status_machine_array.count() && !has_set_stop(); ++i) {
      const share::ObLSAttr &attr = status_machine_array.at(i).ls_info_;
      if (attr.get_ls_id().is_sys_ls()) {
        if (attr.ls_is_normal()) {
          if (OB_FAIL(ls_operator.update_ls_status(attr.get_ls_id(),
          attr.get_ls_status(), share::OB_LS_PRE_TENANT_DROPPING, working_sw_status))) {
            LOG_WARN("failed to update ls status", KR(ret), K(attr));
          }
          task_cnt++;
          LOG_INFO("[PRIMARY_LS_SERVICE] set sys ls to pre tenant dropping", KR(ret), K(attr));
        }
        if (OB_FAIL(ret)) {
        } else if (!attr.ls_is_normal() && !attr.ls_is_pre_tenant_dropping()) {
          // if attr is normal, it means that the status has been switched to pre_tenant_dropping in this round
          // if attr is pre_tenant_dropping, it means that the status has been changed in a previous round
          // the other attr is tenant_dropping, we should skip checking
        } else if (OB_FAIL(ls_operator.get_pre_tenant_dropping_ora_rowscn(sys_ls_target_scn))) {
          LOG_WARN("fail to get sys_ls_end_scn", KR(ret), K(tenant_id_));
        }
        // find SYS LS
        break;
      }
    }//end for set sys ls change to pre tenant dropping

    //before check tenant_info sync scn larger than sys_ls pre tenant dropping scn
    //set creating ls to create_abort
    for (int64_t i = 0; OB_SUCC(ret) && i < status_machine_array.count() && !has_set_stop(); ++i) {
      const share::ObLSAttr &attr = status_machine_array.at(i).ls_info_;
      if (attr.ls_is_creating()) {
        task_cnt++;
        if (OB_FAIL(ls_operator.delete_ls(attr.get_ls_id(), attr.get_ls_status(), working_sw_status))) {
          LOG_WARN("failed to remove ls not normal", KR(ret), K(attr));
        }
        LOG_INFO("[PRIMARY_LS_SERVICE] tenant is dropping, delete ls in creating", KR(ret),
            K(attr));
      }
    }//end for process creating

    if (OB_SUCC(ret) && sys_ls_target_scn.is_valid()) {
      if (OB_FAIL(tenant_info_loader->get_sync_scn(tenant_sync_scn))) {
        LOG_WARN("get tenant_sync_scn failed", KR(ret));
      } else if (OB_UNLIKELY(!tenant_sync_scn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_sync_scn not valid", KR(ret), K(tenant_sync_scn));
      } else if (tenant_sync_scn < sys_ls_target_scn) {
        ret = OB_NEED_WAIT;
        LOG_WARN("wait some time, tenant_sync_scn cannot be smaller than sys_ls_target_scn", KR(ret),
            K(tenant_id_), K(tenant_sync_scn), K(sys_ls_target_scn));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_machine_array.count() && !has_set_stop(); ++i) {
      const share::ObLSAttr &attr = status_machine_array.at(i).ls_info_;
      if (OB_UNLIKELY(!attr.is_valid()) || attr.get_ls_id().is_sys_ls() || attr.ls_is_creating()) {
        // invalid attr might happens if the ls is deleted in __all_ls table but still exists in __all_ls_status table
        // no need process sys ls and creating ls
      } else if (!attr.ls_is_tenant_dropping()) {
        task_cnt++;
        //no matter the status is in normal or dropping
        //may be the status in status info is created
        if (OB_FAIL(ls_operator.update_ls_status(
                attr.get_ls_id(), attr.get_ls_status(),
                share::OB_LS_TENANT_DROPPING, working_sw_status))) {
          LOG_WARN("failed to update ls status", KR(ret), K(attr));
        }
        LOG_INFO("[PRIMARY_LS_SERVICE] set ls to tenant dropping", KR(ret), K(attr), K(i),
            K(tenant_sync_scn), K(sys_ls_target_scn));
      }
    }//end for
  }
  if (OB_SUCC(ret) && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("[PRIMARY_LS_SERVICE] thread stop", KR(ret));
  }
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_BEFORE_LS_STATUS_NORMAL);
int ObPrimaryLSService::try_set_next_ls_status_(
    const common::ObIArray<ObLSStatusMachineParameter> &status_machine_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    share::ObLSAttrOperator ls_operator(MTL_ID(), GCTX.sql_proxy_);
    const ObTenantSwitchoverStatus working_sw_status =
        share::NORMAL_SWITCHOVER_STATUS;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_machine_array.count() && !has_set_stop(); ++i) {
      const ObLSStatusMachineParameter &machine = status_machine_array.at(i);
      const share::ObLSStatusInfo &status_info = machine.status_info_;
      const share::ObLSAttr &ls_info =  machine.ls_info_;
      const uint64_t tenant_id = status_info.tenant_id_;
      if (OB_UNLIKELY(!machine.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("machine is invalid", KR(ret), K(machine));
      } else if (!ls_info.is_valid()) {
        if (status_info.ls_is_wait_offline()) {
        } else if (status_info.ls_is_create_abort()
            || status_info.ls_is_creating()
            || status_info.ls_is_created()) {
          //in switchover/failover, need create abort ls
          //in drop tenant, __all_ls will be deleted while status is creating
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status info is invalid", KR(ret), K(machine));
        }
      } else if (ls_info.ls_is_creating()) {
        if (status_info.ls_is_create_abort()) {
          //delete ls, the ls must is creating
          if (OB_FAIL(ls_operator.delete_ls(
                  machine.ls_id_, share::OB_LS_CREATING, working_sw_status))) {
            LOG_WARN("failed to process creating info", KR(ret), K(machine));
          }
        } else if (status_info.ls_is_created()) {
          //set ls to normal
          if (OB_UNLIKELY(ERRSIM_BEFORE_LS_STATUS_NORMAL)) {
            FLOG_INFO("ERRSIM_BEFORE_LS_STATUS_NORMAL, skip update ls status to normal", K(tenant_id_), K(status_info));
          } else if (OB_FAIL(ls_operator.update_ls_status(
                  machine.ls_id_, ls_info.get_ls_status(), share::OB_LS_NORMAL, working_sw_status))) {
            LOG_WARN("failed to update ls status", KR(ret), K(machine));
          }
        } else if (status_info.ls_is_creating()) {
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status info is invalid", KR(ret), K(machine));
        }
      } else if (ls_info.ls_is_normal()) {
        if (status_info.ls_is_normal()) {
        } else if (status_info.ls_is_created()) {
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status info is invalid", KR(ret), K(machine));
        }
      } else if (ls_info.ls_is_dropping()) {
        if (!status_info.ls_is_dropping()) {
        } else if (OB_FAIL(try_delete_ls_(status_info))) {
          LOG_WARN("failed to try delete ls", KR(ret), K(status_info));
        }
      } else if (ls_info.ls_is_pre_tenant_dropping()) {
        if (!machine.ls_id_.is_sys_ls()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("normal ls can not in pre tenant dropping status", KR(ret), K(machine));
        } else if (!status_info.ls_is_pre_tenant_dropping()) {
        } else if (OB_FAIL(sys_ls_tenant_drop_(status_info))) {
          LOG_WARN("failed to process sys ls", KR(ret), K(status_info));
        }
      } else if (ls_info.ls_is_tenant_dropping()) {
        if (!status_info.ls_is_tenant_dropping()) {
          // __all_ls_status should also be tenant_dropping to notify GC module to offline LS
        } else if (OB_FAIL(try_delete_ls_(status_info))) {
          LOG_WARN("failed to try delete ls", KR(ret), K(machine), K(status_info));
        }
      } else {
        //other status can not be in __all_ls
        //such as created, wait_offline
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ls not expected in all_ls", KR(ret), K(machine));
      }
    }
  }
  if (OB_SUCC(ret) && has_set_stop()) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("[PRIMARY_LS_SERVICE] thread stop", KR(ret));
  }
  return ret;
}

int ObPrimaryLSService::try_delete_ls_(const share::ObLSStatusInfo &status_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  bool can_offline = false;
  const ObTenantSwitchoverStatus working_sw_status = share::NORMAL_SWITCHOVER_STATUS;
  if (OB_UNLIKELY(!status_info.is_valid()
      || (!status_info.ls_is_dropping() && !status_info.ls_is_tenant_dropping())
      || (status_info.ls_id_.is_sys_ls() && !status_info.ls_is_tenant_dropping()))) {
    // SYS LS only can be in tenant_dropping, can not be in DROPPING
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid or not in dropping status or sys ls", KR(ret), K(status_info));
  } else {
    // send rpc to observer
    share::ObLSAttrOperator ls_operator(MTL_ID(), GCTX.sql_proxy_);
    if (OB_FAIL(check_ls_can_offline_by_rpc_(status_info, can_offline))) {
      LOG_WARN("failed to check ls can offline", KR(ret), K(status_info));
    } else if (can_offline) {
      // User LS should be deleted from __all_ls
      if (!status_info.ls_id_.is_sys_ls()) {
        if (OB_FAIL(ls_operator.delete_ls(status_info.ls_id_, status_info.status_, working_sw_status))) {
          LOG_WARN("failed to delete ls", KR(ret), K(status_info));
        }
      } else {
        // SYS LS can not be deleted from __all_ls, as SYS LS is blocked by GC module.
        // So, SYS LS should change __all_ls_status to WAIT_OFFLINE to end its status.
        if (OB_FAIL(ObLSServiceHelper::offline_ls(status_info.tenant_id_,
            status_info.ls_id_, status_info.status_, working_sw_status))) {
          LOG_WARN("failed to offline ls", KR(ret), K(status_info), K(working_sw_status));
        }
      }
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[PRIMARY_LS_SERVICE] finish to try delete LS", KR(ret), K(status_info), K(cost), K(can_offline));
  return ret;
}

int ObPrimaryLSService::sys_ls_tenant_drop_(const share::ObLSStatusInfo &info)
{
  int ret = OB_SUCCESS;
  const ObLSStatus target_status = share::OB_LS_TENANT_DROPPING;
  const ObLSStatus pre_status = share::OB_LS_PRE_TENANT_DROPPING;
  const ObTenantSwitchoverStatus working_sw_status = share::NORMAL_SWITCHOVER_STATUS;
  bool can_offline = false;
  if (OB_UNLIKELY(!info.is_valid()
                  || !info.ls_id_.is_sys_ls())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (pre_status != info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls can not in other status", KR(ret), K(info));
  } else if (OB_FAIL(check_sys_ls_can_offline_(can_offline))) {
    LOG_WARN("failed to check sys ls can offline", KR(ret));
  } else if (can_offline) {
    share::ObLSAttrOperator ls_operator(MTL_ID(), GCTX.sql_proxy_);
    if (OB_FAIL(ls_operator.update_ls_status(info.ls_id_, pre_status, target_status, working_sw_status))) {
      LOG_WARN("failed to update ls status", KR(ret), K(info), K(pre_status), K(target_status));
    }
  }
  LOG_INFO("[PRIMARY_LS_SERVICE] set sys ls tenant dropping", KR(ret), K(info), K(can_offline));
  return ret;
}

int ObPrimaryLSService::check_sys_ls_can_offline_(bool &can_offline)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  can_offline = true;
  const uint64_t tenant_id = MTL_ID();
  share::ObLSStatusOperator status_operator;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(status_operator.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
  } else if (0 == status_info_array.count()) {
    //sys ls not exist
    can_offline = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count() && can_offline; ++i) {
    const share::ObLSStatusInfo &status_info = status_info_array.at(i);
    if (status_info.ls_id_.is_sys_ls()) {
    } else {
      can_offline = false;
      LOG_INFO("[PRIMARY_LS_SERVICE] sys ls can not offline", K(status_info));
    }
  }
  if (OB_SUCC(ret) && can_offline) {
    LOG_INFO("[PRIMARY_LS_SERVICE] sys ls can offline", K(status_info_array));
  }
  return ret;
}

int ObPrimaryLSService::check_ls_can_offline_by_rpc_(const share::ObLSStatusInfo &info, bool &can_offline)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service or proxy is null", KR(ret), KP(GCTX.location_service_),
             KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, info.tenant_id_,
             info.ls_id_, false, leader))) {
    LOG_WARN("failed to get ls leader", KR(ret), K(info));
  } else {
    const int64_t timeout = GCONF.rpc_timeout;
    obrpc::ObCheckLSCanOfflineArg arg;
    can_offline = false;
    const uint64_t group_id = info.ls_is_tenant_dropping() ? OBCG_DBA_COMMAND : OBCG_DEFAULT;
    if (OB_FAIL(arg.init(info.tenant_id_, info.ls_id_, info.status_))) {
      LOG_WARN("failed to init arg", KR(ret), K(arg));
    } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader)
                           .by(info.tenant_id_)
                           .timeout(timeout)
                           .group_id(group_id)
                           .check_ls_can_offline(arg))) {
      can_offline = false;
      LOG_WARN("failed to check ls can offline", KR(ret), K(arg), K(info),
               K(timeout), K(leader));
    } else {
      can_offline = true;
    }
  }
  return ret;
}

int ObPrimaryLSService::process_all_ls_status_to_steady_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls recovery thread must run on user tenant", KR(ret),
             K(tenant_id_));
  } else {
    ObTenantLSInfo tenant_info(GCTX.sql_proxy_, &tenant_schema, tenant_id_);
    if (OB_FAIL(ObLSServiceHelper::process_status_to_steady(false, share::NORMAL_SWITCHOVER_STATUS, tenant_info))) {
      LOG_WARN("failed to process status to steady", KR(ret));
    }
  }
  LOG_INFO("[PRIMARY_LS_SERVICE] finish process all ls status to steady", KR(ret), K(tenant_id_));
  return ret;
}

int ObPrimaryLSService::check_mini_mode_create_ls()
{
  int ret = OB_SUCCESS;
  bool is_created = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.schema_service_));
  } else if (!MTL_IS_MINI_MODE()) {
  } else if (OB_FAIL(GCTX.schema_service_->check_if_tenant_created_for_creating_tenant(tenant_id_,
          true/*auto_update*/, is_created))) {
    LOG_WARN("failed to check if tenant created", KR(ret), K_(tenant_id));
  } else if (!is_created) {
    ret = OB_NEED_WAIT;
    LOG_WARN("schema is not ready, need wait", KR(ret), K_(tenant_id));
  }
  return ret;
}

//the interface may reentry
int ObPrimaryLSService::create_ls_for_create_tenant()
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  ObArray<ObZone> primary_zone;
  ObArray<share::ObSimpleUnitGroup> unit_group_array;
  share::ObLSAttrOperator ls_operator(tenant_id_, GCTX.sql_proxy_);
  if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
  } else if (!tenant_schema.is_creating()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only creating tenant can create user ls", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(check_mini_mode_create_ls())) {
    LOG_WARN("failed to check mini mode create ls", KR(ret));
  } else if (OB_FAIL(ObLSServiceHelper::get_primary_zone_unit_array(&tenant_schema,
          primary_zone, unit_group_array))) {
    LOG_WARN("failed to get primary zone unit array", KR(ret), K(tenant_schema));
  } else {
    // ensure __all_ls is emptry
    START_TRANSACTION(GCTX.sql_proxy_, tenant_id_)
    ObArray<share::ObLSAttr> ls_array;
    share::ObLSAttr sys_ls;
    if (FAILEDx(ls_operator.get_ls_attr(SYS_LS, true, trans, sys_ls))) {
      LOG_WARN("failed to get SYS_LS attr", KR(ret));
    } else if (OB_FAIL(ls_operator.get_all_ls_by_order(ls_array))) {
      LOG_WARN("failed to get all_ls by order", KR(ret));
    } else if (ls_array.count() > 1) {
      //nothing
    } else {
      uint64_t ls_group_id = OB_INVALID_ID;
      ObLSID ls_id;
      share::ObLSAttr new_ls;
      share::ObLSFlag flag;
      SCN create_scn;
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array.count(); ++i) {
        if (unit_group_array.at(i).is_active()) {
          //create ls
          if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(GCTX.sql_proxy_, tenant_id_, ls_group_id))) {
            LOG_WARN("failed to fetch new LS group id", KR(ret), K(tenant_id_));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < primary_zone.count(); j++) {
            if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(GCTX.sql_proxy_, tenant_id_, ls_id))) {
              LOG_WARN("failed to fetch new LS id", KR(ret), K(tenant_id_));
            } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id_, create_scn))) {
              LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
            } else if (OB_FAIL(new_ls.init(ls_id, ls_group_id, flag, share::OB_LS_CREATING,
                           share::OB_LS_OP_CREATE_PRE, create_scn))) {
              LOG_WARN("failed to init new operation", KR(ret), K(create_scn),
                       K(ls_id), K(ls_group_id));
            } else if (OB_FAIL(ls_operator.insert_ls(
                           new_ls, share::NORMAL_SWITCHOVER_STATUS, &trans))) {
              LOG_WARN("failed to insert new operation", KR(ret), K(new_ls));
            }
          }//end for each ls group
        }
      }//end for each unit group
    }
    END_TRANSACTION(trans)
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_AFTER_CREATE_ONE_LS);
int ObPrimaryLSService::create_all_user_ls_for_creating_tenant_(
    const uint64_t tenant_id,
    const ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  uint64_t ls_created_count = 0;
  if (tenant_id != tenant_schema.get_tenant_id() || !is_valid_tenant_id(tenant_id) ||
      !tenant_schema.is_creating() || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(tenant_schema), KP(GCTX.sql_proxy_));
  } else {
    share::ObLSAttrOperator ls_operator(tenant_id, GCTX.sql_proxy_);
    share::ObLSAttrArray ls_array;
    if (OB_FAIL(ls_operator.get_all_ls_by_order(false/*lock_sys_ls*/, ls_array))) {
      LOG_WARN("failed to get get all ls", KR(ret));
    } else if (OB_UNLIKELY(0 == ls_array.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls array can not be empty", KR(ret), K(ls_array));
    } else {
      ObLSRecoveryStat recovery_stat;
      ObLSRecoveryStatOperator ls_recovery_operator;
      share::ObLSStatusOperator status_op;
      ObLSStatusInfo status_info;
      palf::PalfBaseInfo palf_base_info;
      FOREACH_X(ls, ls_array, OB_SUCC(ret)) {
        ObLSID ls_id = ls->get_ls_id();
        if (!ls_id.is_user_ls()) {
        } else if (OB_FAIL(status_op.get_ls_status_info(tenant_id, ls_id, status_info,
                *GCTX.sql_proxy_))) {
          LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id));
        } else if (!status_info.ls_is_creating()) {
        } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, ls_id,
                false /*need_for_update*/, recovery_stat, *GCTX.sql_proxy_))) {
          LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ObCommonLSService::do_create_user_ls(tenant_schema, status_info,
                recovery_stat.get_create_scn(), false/*create_with_palf*/, palf_base_info,
                OB_INVALID_TENANT_ID/*source_tenant_id*/))) {
          // create ls
          // ls status == CREATED, ls == CREATING
          LOG_WARN("failed to create user ls", KR(ret), K(tenant_schema), K(status_info), K(recovery_stat));
        } else if (OB_FAIL(ERRSIM_AFTER_CREATE_ONE_LS)) {
          LOG_WARN("ERRSIM_AFTER_CREATE_ONE_LS", KR(ret));
        } else {
          ls_created_count++;
        }
      }
    }
  }
  LOG_INFO("created user ls count", KR(ret), K(tenant_id), K(ls_created_count));
  return ret;
}

int ObPrimaryLSService::check_has_one_normal_ls()
{
  int ret = OB_SUCCESS;
  ObLSStatusInfoArray ls_status_array;
  share::ObLSStatusOperator status_op;
  bool has_normal_ls = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id_, ls_status_array, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get get all ls status", KR(ret));
  } else if (OB_UNLIKELY(0 == ls_status_array.count())) {
    ret = OB_NEED_RETRY;
    LOG_WARN("ls array can not be empty", KR(ret), K(ls_status_array));
  } else {
    FOREACH_X(status, ls_status_array, !has_normal_ls && OB_SUCC(ret)) {
      ObLSID ls_id = status->get_ls_id();
      if (!ls_id.is_user_ls()) {
      } else if (status->is_normal()) {
        has_normal_ls = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_normal_ls) {
    ret = OB_NEED_RETRY;
    LOG_WARN("has no normal ls, need retry", KR(ret), K(ls_status_array));
  }
  return ret;
}

int ObPrimaryLSService::advance_user_ls_status_for_creating_tenant()
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("primary ls service not inited", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy  is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
  } else if (!tenant_schema.is_creating()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("function should be called when tenant is creating", K(tenant_id_));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(process_all_ls_status_to_steady_(tenant_schema))) {
      // init ls status
      // ls status == CREATING, ls == CREATING
      LOG_WARN("failed to process_all_ls_status_to_steady_, make ls status to creating failed",
          KR(tmp_ret), K(tenant_id_));
    }
    if (OB_TMP_FAIL(create_all_user_ls_for_creating_tenant_(tenant_id_, tenant_schema))) {
      // create ls
      // ls status == CREATED, ls == CREATING
      LOG_WARN("failed to create user ls", KR(tmp_ret), K(tenant_id_));
    }
    if (OB_TMP_FAIL(process_all_ls(tenant_schema, true/*in_creating_tenant*/))) {
      // ls status == CREATED, ls == NORMAL
      LOG_WARN("failed to process all ls", KR(tmp_ret), K(tenant_schema));
    }
    if (OB_TMP_FAIL(process_all_ls_status_to_steady_(tenant_schema))) {
      // ls status == NORMAL, ls == NORMAL
      LOG_WARN("failed to process_all_ls_status_to_steady_, make ls to normal failed", KR(tmp_ret),
          K(tenant_id_));
    }
    if (OB_FAIL(check_has_one_normal_ls())) {
      LOG_WARN("failed to check has one normal ls", KR(ret));
    } else {
      LOG_INFO("succeed to advance user ls status for creating tenant", KR(ret), K(tenant_id_));
    }
  }
  return ret;
}

int ObPrimaryLSService::create_duplicate_ls()
{
  int ret = OB_SUCCESS;
  share::ObLSAttrOperator ls_operator(tenant_id_, GCTX.sql_proxy_);
  share::ObLSID ls_id;
  SCN create_scn;
  const uint64_t ls_group_id = 0;
  ObArray<share::ObLSAttr> existing_ls;
  share::ObLSAttr new_ls;
  ObLSFlag flag(ObLSFlag::DUPLICATE_FLAG);
  bool duplicate_ls_exist = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ls_operator.get_duplicate_ls_attr(false, *GCTX.sql_proxy_, existing_ls))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed get duplicate ls", KR(ret));
    }
  } else {
    duplicate_ls_exist = true;
  }
  if (duplicate_ls_exist) {
    // check duplicate ls exist or not to avoid max used ls id to be large
  } else if (FAILEDx(ObLSServiceHelper::fetch_new_ls_id(GCTX.sql_proxy_, tenant_id_, ls_id))) {
    LOG_WARN("failed to fetch new LS id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id_, create_scn))) {
    LOG_WARN("failed to get tenant gts", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(new_ls.init(ls_id, ls_group_id, flag, share::OB_LS_CREATING,
                                 share::OB_LS_OP_CREATE_PRE, create_scn))) {
    LOG_WARN("failed to init new operation", KR(ret), K(create_scn),
             K(ls_id), K(ls_group_id));
  } else if (OB_FAIL(ls_operator.insert_ls(
              new_ls, share::NORMAL_SWITCHOVER_STATUS))) {
    LOG_WARN("failed to insert new operation", KR(ret), K(new_ls));
  }
  LOG_INFO("[LS_MGR] create duplicate ls", KR(ret), K(new_ls));
  return ret;
}

int ObDupLSCreateHelper::init(
    const uint64_t tenant_id,
    common::ObISQLClient *sql_proxy,
    obrpc::ObSrvRpcProxy *srv_rpc_proxy,
    share::ObLocationService *location_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_ISNULL(sql_proxy)
      || OB_ISNULL(srv_rpc_proxy)
      || OB_ISNULL(location_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), KP(sql_proxy), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("duplicate ls can be only created in user tenant", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    srv_rpc_proxy_ = srv_rpc_proxy;
    location_service_ = location_service;
    inited_ = true;
  }
  return ret;
}

int ObDupLSCreateHelper::check_and_create_duplicate_ls_if_needed(
    share::ObLSID &dup_ls_id)
{
  int ret = OB_SUCCESS;
  dup_ls_id.reset();
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)
      || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(srv_rpc_proxy_)
      || OB_ISNULL(location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret), K(ctx));
  } else {
    ObLSStatusOperator ls_status_operator;
    ObLSStatusInfo duplicate_ls_status_info;
    obrpc::ObCreateDupLSArg arg;
    obrpc::ObCreateDupLSResult result;
    ObAddr dup_ls_leader;
    while (OB_SUCC(ret) && !ctx.is_timeouted()) {
      int tmp_ret = OB_SUCCESS;
      duplicate_ls_status_info.reset();
      if (OB_TMP_FAIL(ls_status_operator.get_duplicate_ls_status_info(
          tenant_id_,
          *sql_proxy_,
          duplicate_ls_status_info))) {
        if (OB_ENTRY_NOT_EXIST == tmp_ret) {
          LOG_INFO("duplicate ls not exist, need create one", K_(tenant_id));
          tmp_ret = OB_SUCCESS;
          // create duplicate ls
          ObAddr sys_ls_leader;
          const int64_t timeout = ctx.get_timeout();
          if (OB_TMP_FAIL(location_service_->get_leader(
              GCONF.cluster_id,
              tenant_id_,
              SYS_LS,
              false/*force_renew*/,
              sys_ls_leader))) {
            LOG_WARN("failed to get leader", KR(tmp_ret), K_(tenant_id));
          } else if (OB_TMP_FAIL(arg.init(tenant_id_))) {
            LOG_WARN("failed to init arg", KR(ret), K_(tenant_id));
          } else if (OB_TMP_FAIL(srv_rpc_proxy_->to(sys_ls_leader).timeout(timeout).notify_create_duplicate_ls(arg, result))) {
            LOG_WARN("failed to create tenant duplicate ls", KR(tmp_ret), K_(tenant_id), K(sys_ls_leader), K(arg), K(timeout));
            if (OB_CONFLICT_WITH_CLONE == tmp_ret) {
              ret = tmp_ret;
              LOG_WARN("tenant is in clone procedure, can not create new log stream for now", KR(ret), K_(tenant_id), K(arg));
            }
          }
        } else {
          LOG_WARN("fail to get duplicate log stream from table", KR(tmp_ret), K_(tenant_id));
        }
      } else if (!duplicate_ls_status_info.ls_is_normal()) { // need wait
        LOG_INFO("duplicate ls is not in normal status, need wait", K(duplicate_ls_status_info));
      } else if (OB_TMP_FAIL(location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id,
          tenant_id_,
          duplicate_ls_status_info.get_ls_id(),
          dup_ls_leader,
          ctx.get_abs_timeout()))) {
        LOG_WARN("get_leader_with_retry_until_timeout failed", KR(tmp_ret),
            K_(tenant_id), K(duplicate_ls_status_info), K(ctx));
      } else if (dup_ls_leader.is_valid()) {
        dup_ls_id = duplicate_ls_status_info.get_ls_id();
        break;
      }
      ob_usleep(1_s);
    } // end while
    if (OB_FAIL(ret)) {
    } else if (ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait creating duplicate ls timeout", KR(ret), K_(tenant_id), K(ctx));
    } else if (OB_UNLIKELY(!dup_ls_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid dup_ls_id", KR(ret), K_(tenant_id), K(ctx), K(dup_ls_id), K(dup_ls_leader));
    }
  }
  return ret;
}

}//end of rootserver
}
