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
#include "ob_ls_service_helper.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ob_max_id_fetcher.h"
#include "share/schema/ob_schema_struct.h"//ObTenantInfo
#include "share/schema/ob_schema_service.h"//ObMultiSchemaService
#include "share/ls/ob_ls_creator.h" //ObLSCreator
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "share/ls/ob_ls_table_operator.h"//ObLSTableOpertor
#include "share/ob_primary_zone_util.h"//ObPrimaryZoneUtil
#include "share/ob_unit_table_operator.h"//ObUnitTableOperator
#include "share/ob_zone_table_operation.h" //ObZoneTableOperation
#include "share/ob_share_util.h"//ObShareUtil
#include "share/restore/ob_physical_restore_table_operator.h"//ObTenantRestoreTableOperator
#include "share/ob_standby_upgrade.h"//ObStandbyUpgrade
#include "share/ob_upgrade_utils.h"//ObUpgradeChecker
#include "share/rc/ob_tenant_base.h"//MTL_SWITCH
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/ob_recovery_ls_service.h"//ObRecoveryLSHelper
#include "rootserver/ob_tenant_thread_helper.h"//get_zone_priority
#include "rootserver/ob_tenant_role_transition_service.h"//get_checkpoint_by_rpc
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo
#include "logservice/ob_log_service.h"//ObLogService

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{
/////////ObUnitGroupInfo
bool ObUnitGroupInfo::is_valid() const
{
  return OB_INVALID_ID != unit_group_id_
         && share::ObUnit::UNIT_STATUS_MAX != unit_status_;
}

int ObUnitGroupInfo::init(const uint64_t unit_group_id,
                          const share::ObUnit::Status &unit_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id
                  || share::ObUnit::UNIT_STATUS_MAX == unit_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_group_id), K(unit_status));
  } else {
    unit_group_id_ = unit_group_id;
    unit_status_ = unit_status;
  }
  return ret;
}

void ObUnitGroupInfo::reset()
{
  unit_group_id_ = OB_INVALID_ID;
  unit_status_ = share::ObUnit::UNIT_STATUS_MAX;
  ls_group_ids_.reset();
}

int ObUnitGroupInfo::assign(const ObUnitGroupInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ls_group_ids_.assign(other.ls_group_ids_))) {
      LOG_WARN("failed to assign", KR(ret), K(other));
    } else {
      unit_group_id_ = other.unit_group_id_;
      unit_status_ = other.unit_status_;
    }
  }
  return ret;
}

int ObUnitGroupInfo::remove_ls_group(const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObLSID::MIN_USER_LS_GROUP_ID >= ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group id is invalid", KR(ret), K(ls_group_id));
  } else {
    bool remove = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_group_ids_.count(); ++i) {
      if (ls_group_ids_.at(i) == ls_group_id) {
        remove = true;
        if (OB_FAIL(ls_group_ids_.remove(i))) {
          LOG_WARN("failed to remove from array", KR(ret), K(i),
                   K(ls_group_id), "this", *this);
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !remove) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find ls group id", KR(ret),
               K(ls_group_id), "this", *this);
    }
  }
  return ret;
}

bool ObUnitGroupInfo::operator==(const ObUnitGroupInfo &other) const
{
  return unit_group_id_ == other.unit_group_id_
         && unit_status_ == other.unit_status_;
}

////////////ObLSGroupInfo
bool ObLSGroupInfo::is_valid() const
{
  return OB_INVALID_ID != unit_group_id_
         && OB_INVALID_ID != ls_group_id_;
}

int ObLSGroupInfo::init(const uint64_t unit_group_id,
                               const uint64_t ls_group_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id
                  || OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_group_id), K(ls_group_id));
  } else {
    unit_group_id_ = unit_group_id;
    ls_group_id_ = ls_group_id;
  }
  return ret;
}

int ObLSGroupInfo::assign(const ObLSGroupInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ls_ids_.assign(other.ls_ids_))) {
      LOG_WARN("failed to assign ls ids", KR(ret), K(other));
    } else {
      unit_group_id_ = other.unit_group_id_;
      ls_group_id_ = other.ls_group_id_;
    }
  }
  return ret;
}

void ObLSGroupInfo::reset()
{
  ls_group_id_ = OB_INVALID_ID;
  unit_group_id_ = OB_INVALID_ID;
  ls_ids_.reset();
}

int ObLSGroupInfo::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id));
  } else {
    bool remove = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids_.count(); ++i) {
      if (ls_ids_.at(i) == ls_id) {
        remove = true;
        if (OB_FAIL(ls_ids_.remove(i))) {
          LOG_WARN("failed to remove from array", KR(ret), K(i),
                   K(ls_id), "this", *this);
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !remove) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find ls id", KR(ret),
               K(ls_id), "this", *this);
    }
  }
  return ret;

}

///////////////ObLSStatusMachineParameter
//no need check status valid or ls_status is valid
int ObLSStatusMachineParameter::init(const share::ObLSID &id,
                            const share::ObLSStatusInfo &status_info,
                            const share::ObLSAttr &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("id is invalid", KR(ret), K(id));
  } else if (OB_FAIL(status_info_.assign(status_info))) {
    LOG_WARN("failed to assign status info", KR(ret), K(status_info));
  } else if (OB_FAIL(ls_info_.assign(ls_info))) {
    LOG_WARN("failed to assign ls info", KR(ret), K(ls_info));
  } else {
    ls_id_ = id;
  }
  return ret;
}

void ObLSStatusMachineParameter::reset()
{
  ls_id_.reset();
  ls_info_.reset();
  status_info_.reset();
}

//////ObLSServiceHelper////////////
int ObLSServiceHelper::construct_ls_status_machine(
    const bool lock_sys_ls,
    const uint64_t tenant_id,
    ObMySQLProxy *sql_proxy,
    common::ObIArray<ObLSStatusMachineParameter> &status_machine_array)
{
  int ret = OB_SUCCESS;
  status_machine_array.reset();
  share::ObLSStatusInfoArray status_info_array;
  share::ObLSAttrArray ls_array;
  share::ObLSAttrOperator ls_operator(tenant_id, sql_proxy);
  ObLSStatusMachineParameter status_machine;
  ObLSStatusOperator status_op;
  if (OB_ISNULL(sql_proxy) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null or tenant id is invalid", KR(ret), KP(sql_proxy), K(tenant_id));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *sql_proxy))) {
    LOG_WARN("failed to get all ls status by order", KR(ret));
  } else if (1 == status_info_array.count() && status_info_array.at(0).ls_is_tenant_dropping()
      && status_info_array.at(0).ls_id_.is_sys_ls()) {
    //Due to SYS_LS being tenant_dropping, it cannot be read,
    //so it cannot be read during the construction of the state machine of __all_ls,
    //it needs to be mocked the content of the ls table
    ObLSAttr ls_info;
    const share::ObLSStatusInfo &status_info = status_info_array.at(0);
    if (OB_FAIL(ls_info.init(SYS_LS, status_info.ls_group_id_, status_info.flag_,
            status_info.status_, OB_LS_OP_TENANT_DROP, SCN::base_scn()))) {
      LOG_WARN("failed to mock ls info", KR(ret), K(status_info));
    } else if (OB_FAIL(status_machine.init(SYS_LS, status_info, ls_info))) {
        LOG_WARN("failed to init status machine", KR(ret), K(ls_info.get_ls_id()), K(status_info), K(ls_info));
    } else if (OB_FAIL(status_machine_array.push_back(status_machine))) {
      LOG_WARN("failed to push back status machine", KR(ret), K(status_machine));
    }
  } else if (OB_FAIL(ls_operator.get_all_ls_by_order(lock_sys_ls, ls_array))) {
    LOG_WARN("failed to get get all ls", KR(ret), K(lock_sys_ls));
  } else if (OB_UNLIKELY(0 == ls_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls array can not be empty", KR(ret), K(ls_array), K(status_info_array));
  } else {
    // merge by sort
    int64_t status_index = 0;
    int64_t ls_index = 0;
    const int64_t status_count = status_info_array.count();
    const int64_t ls_count = ls_array.count();
    share::ObLSStatusInfo status_info;
    ObLSID ls_id;
    ObLSAttr ls_info;
    while ((status_index < status_count || ls_index < ls_count) && OB_SUCC(ret)) {
      status_machine.reset();
      if (status_index == status_count) {
        //status already end
        if (OB_FAIL(ls_info.assign(ls_array.at(ls_index)))) {
          LOG_WARN("failed to assign ls info", KR(ret), K(ls_index), K(ls_array));
        } else {
          ls_id = ls_array.at(ls_index).get_ls_id();
          status_info.reset();
          ls_index++;
        }
      } else if (ls_index == ls_count) {
        //ls already end
        if (OB_FAIL(status_info.assign(status_info_array.at(status_index)))) {
          LOG_WARN("failed to assign status info", KR(ret), K(status_index));
        } else {
          status_index++;
          ls_id = status_info.ls_id_;
          ls_info.reset();
        }
      } else {
        const share::ObLSStatusInfo &tmp_status_info = status_info_array.at(status_index);
        const share::ObLSAttr &tmp_ls_info = ls_array.at(ls_index);
        if (tmp_status_info.ls_id_ == tmp_ls_info.get_ls_id()) {
          status_index++;
          ls_index++;
          ls_id = tmp_status_info.ls_id_;
          if (OB_FAIL(ls_info.assign(tmp_ls_info))) {
            LOG_WARN("failed to assign tmp ls info", KR(ret), K(tmp_ls_info));
          } else if (OB_FAIL(status_info.assign(tmp_status_info))) {
            LOG_WARN("failed to assign status info", KR(ret), K(tmp_status_info));
          }
        } else if (tmp_status_info.ls_id_ > tmp_ls_info.get_ls_id()) {
          ls_index++;
          ls_id = tmp_ls_info.get_ls_id();
          status_info.reset();
          if (OB_FAIL(ls_info.assign(tmp_ls_info))) {
            LOG_WARN("failed to assign tmp ls info", KR(ret), K(tmp_ls_info));
          }
        } else {
          status_index++;
          ls_id = tmp_status_info.ls_id_;
          ls_info.reset();
          if (OB_FAIL(status_info.assign(tmp_status_info))) {
            LOG_WARN("failed to assign status info", KR(ret), K(tmp_status_info));
          }
        }
      }
      if (FAILEDx(status_machine.init(ls_id, status_info, ls_info))) {
        LOG_WARN("failed to init status machine", KR(ret), K(ls_id), K(status_info), K(ls_info));
      } else if (OB_FAIL(status_machine_array.push_back(status_machine))) {
        LOG_WARN("failed to push back status machine", KR(ret), K(status_machine));
      }
    } // end while

  }
  return ret;
}

int ObLSServiceHelper::fetch_new_ls_group_id(
     ObMySQLProxy *sql_proxy,
     const uint64_t tenant_id,
     uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ls_group_id = OB_INVALID_ID;
    share::ObMaxIdFetcher id_fetcher(*sql_proxy);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        tenant_id, share::OB_MAX_USED_LS_GROUP_ID_TYPE, ls_group_id))) {
      LOG_WARN("fetch_max_id failed", KR(ret), K(tenant_id), "id_type",
               share::OB_MAX_USED_LS_GROUP_ID_TYPE);
    }
  }
  return ret;
}

int ObLSServiceHelper::fetch_new_ls_id(ObMySQLProxy *sql_proxy, const uint64_t tenant_id, share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    share::ObMaxIdFetcher id_fetcher(*sql_proxy);
    uint64_t id_value = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        tenant_id, share::OB_MAX_USED_LS_ID_TYPE, id_value))) {
      LOG_WARN("fetch_max_id failed", KR(ret), K(tenant_id), "id_type",
               share::OB_MAX_USED_LS_ID_TYPE);
    } else {
      share::ObLSID new_id(id_value);
      id = new_id;
    }
  }
  return ret;
}

int ObLSServiceHelper::get_primary_zone_unit_array(const share::schema::ObTenantSchema *tenant_schema,
      ObIArray<ObZone> &primary_zone, ObIArray<share::ObSimpleUnitGroup> &unit_group_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is null", KR(ret), K(tenant_schema));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
            *tenant_schema, primary_zone))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret), KPC(tenant_schema));
  }
  if (OB_SUCC(ret)) {
    //get unit_group
    ObUnitTableOperator unit_operator;
    const uint64_t tenant_id = tenant_schema->get_tenant_id();
    if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init unit operator", KR(ret));
    } else if (OB_FAIL(unit_operator.get_unit_groups_by_tenant(
            tenant_id, unit_group_array))) {
      LOG_WARN("failed to get unit group array", KR(ret), K(tenant_id));
    }
  }

  return ret;
}
int ObLSServiceHelper::update_ls_recover_in_trans(
     const share::ObLSRecoveryStat &ls_recovery_stat,
     const bool only_update_readable_scn,
     common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_recovery_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls recovery stat is invalid", KR(ret), K(ls_recovery_stat));
  } else {
    share::ObLSRecoveryStatOperator ls_recovery;
    SCN new_scn;
    if (OB_FAIL(ls_recovery.update_ls_recovery_stat_in_trans(ls_recovery_stat,
            only_update_readable_scn, trans))) {
      LOG_WARN("failed to update ls recovery stat", KR(ret), K(ls_recovery_stat));
    } else if (only_update_readable_scn) {
      //only update readable_scn, no need check sync_scn is fallback
      //for restore tenant, sync_scn of sys_ls maybe larger than end_scn.
      //before sys_ls can iterator log, no need update sync_scn, only need update readable_scn
      //so do as after flashback, sys_ls need update readable_scn too
    } else if (OB_FAIL(get_ls_replica_sync_scn(ls_recovery_stat.get_tenant_id(),
            ls_recovery_stat.get_ls_id(), new_scn))) {
      LOG_WARN("failed to get ls sync scn", KR(ret), K(ls_recovery_stat));
    } else if (new_scn < ls_recovery_stat.get_sync_scn()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("maybe flashback, can not report sync scn", KR(ret), K(ls_recovery_stat), K(new_scn));
    }
  }
  return ret;
}

int ObLSServiceHelper::get_ls_replica_sync_scn(const uint64_t tenant_id,
      const ObLSID &ls_id, share::SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      palf::PalfHandleGuard palf_handle_guard;
      SCN end_scn;
      SCN checkpoint_scn;
      logservice::ObLogService *log_svr = MTL(logservice::ObLogService*);
      ObLSService *ls_svr = MTL(ObLSService *);
      ObLSHandle ls_handle;

      if (OB_ISNULL(ls_svr) || OB_ISNULL(log_svr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ls log service is null", KR(ret), K(tenant_id), KP(ls_svr), KP(log_svr));
      } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, storage::ObLSGetMod::RS_MOD))) {
        LOG_WARN("failed to get ls", KR(ret));
      } else {
        ObLS *ls = NULL;
        if (OB_ISNULL(ls = ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is null", KR(ret), K(ls_id));
        } else if (FALSE_IT(checkpoint_scn = ls->get_clog_checkpoint_scn())) {
        } else if (OB_FAIL(log_svr->open_palf(ls_id, palf_handle_guard))) {
          LOG_WARN("failed to open palf", KR(ret), K(ls_id));
        } else if (OB_FAIL(palf_handle_guard.get_end_scn(end_scn))) {
          LOG_WARN("failed to get end scn", KR(ret));
        } else {
          //The end_scn of PALF will be set to the SCN corresponding to the LSN in the checkpoint information,
          //which is smaller than the SCN recorded in the checkpoint information.
          //Therefore, in the recovery scenario, the end_scn of the LS will be smaller than readable_scn
          //(readable_scn is provided by get_max_decided_scn of ObLogHandler, and this value is checkpoint at this time. SCN recorded in the information)
          //set sync_scn = max(end_scn, checkpoint_scn);
          sync_scn = SCN::max(end_scn, checkpoint_scn);
          LOG_DEBUG("get sync scn", K(tenant_id), K(ls_id), K(sync_scn), K(end_scn), K(checkpoint_scn));
        }
      }
    }
  }
  return ret;
}


//Regardless of the tenant being dropped,
//handle the asynchronous operation of status and history table
int ObLSServiceHelper::process_status_to_steady(
    const bool lock_sys_ls,
    const share::ObTenantSwitchoverStatus &working_sw_status,
    ObTenantLSInfo& tenant_ls_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSStatusMachineParameter> status_machine_array;
  const uint64_t tenant_id = tenant_ls_info.get_tenant_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(construct_ls_status_machine(
          lock_sys_ls, tenant_id, GCTX.sql_proxy_, status_machine_array))) {
    LOG_WARN("failed to construct ls status machine array", KR(ret), K(lock_sys_ls), K(tenant_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    ARRAY_FOREACH_NORET(status_machine_array, idx) {
      const ObLSStatusMachineParameter &machine = status_machine_array.at(idx);
      //ignore error of each ls
      //may ls can not create success
      if (OB_SUCCESS != (tmp_ret = revision_to_equal_status_(machine, working_sw_status, tenant_ls_info))) {
        LOG_WARN("failed to fix ls status", KR(ret), KR(tmp_ret), K(machine), K(tenant_ls_info));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (machine.ls_info_.get_ls_status() != machine.status_info_.status_) {
        //no need do next, or ls is normal, no need process next
        //or ls status not equal, no need to next
      } else if (machine.ls_info_.ls_is_normal() && machine.ls_info_.get_ls_group_id() != machine.status_info_.ls_group_id_) {
        // ***TODO(linqiucen.lqc) wait tenant_sync_scn > sys_ls_end_scn
        if (OB_TMP_FAIL(process_alter_ls(machine.ls_id_, machine.status_info_.ls_group_id_,
                machine.ls_info_.get_ls_group_id(), machine.status_info_.unit_group_id_,
                tenant_ls_info, *GCTX.sql_proxy_))) {
          LOG_WARN("failed to process alter ls", KR(ret), KR(tmp_ret), K(machine));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }

  return ret;
}

int ObLSServiceHelper::offline_ls(const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObLSStatus &cur_ls_status,
    const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  SCN drop_scn;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is NULL", KP(GCTX.sql_proxy_), KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else {
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);

    if (ls_id.is_sys_ls()) {
      // For SYS LS, drop scn can not be generated, as GTS service is down, SYS LS is blocked
      // drop_scn is meaningless for SYS LS, so set it to min_scn
      // base_scn is dafault value, if set default, __all_ls_recovery_stat affected_row is zero
      drop_scn.set_min();
    }
    // For user LS, drop_scn should be GTS.
    else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id, drop_scn))) {
      LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_life_agent.set_ls_offline(tenant_id, ls_id, cur_ls_status, drop_scn,
        working_sw_status))) {
      LOG_WARN("failed to set ls offline", KR(ret), K(tenant_id), K(ls_id), K(cur_ls_status),
          K(drop_scn), K(working_sw_status));
    }
  }
  return ret;
}

int ObLSServiceHelper::wait_all_tenants_user_ls_sync_scn(
    common::hash::ObHashMap<uint64_t, share::SCN> &tenants_sys_ls_target_scn)
{
  int ret = OB_SUCCESS;
  share::SCN sys_ls_target_scn;
  ObTimeoutCtx timeout_ctx;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout * 10;
  sys_ls_target_scn.set_invalid();
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to get default timeout", KR(ret));
  }
  while(OB_SUCC(ret) && !tenants_sys_ls_target_scn.empty()) {
    hash::ObHashMap<uint64_t, share::SCN>::iterator iter = tenants_sys_ls_target_scn.begin();
    while (OB_SUCC(ret) && iter != tenants_sys_ls_target_scn.end()) {
      sys_ls_target_scn.set_invalid();
      const uint64_t tenant_id = iter->first;
      sys_ls_target_scn = iter->second;
      iter++;
      if (OB_UNLIKELY(timeout_ctx.is_timeouted())) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait tenant_sync_scn timeout", KR(ret), K(timeout_ctx));
      } else if (OB_FAIL(check_if_need_wait_user_ls_sync_scn_(tenant_id, sys_ls_target_scn))) {
        if (OB_NEED_WAIT != ret) {
          LOG_WARN("fail to check tenant_sync_scn", KR(ret), K(tenant_id), K(sys_ls_target_scn));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(tenants_sys_ls_target_scn.erase_refactored(tenant_id))) {
        LOG_WARN("fail to remove the tenant from tenants_sys_ls_target_scn", KR(ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret) && OB_LIKELY(!tenants_sys_ls_target_scn.empty())) {
      ob_usleep(200*1000);
    }
  }
  return ret;
}

int ObLSServiceHelper::check_if_need_wait_user_ls_sync_scn_(
    const uint64_t tenant_id,
    const share::SCN &sys_ls_target_scn)
{
  int ret = OB_SUCCESS;
  share::SCN user_ls_sync_scn;
  user_ls_sync_scn.set_invalid();
  ObAllTenantInfo tenant_info;
  ObLSRecoveryStatOperator ls_recovery;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_user_tenant(tenant_id)) {
    // skip
  } else if (OB_UNLIKELY(!sys_ls_target_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys_ls_target_scn", KR(ret), K(sys_ls_target_scn));
  } else if (OB_FAIL(ls_recovery.get_user_ls_sync_scn(tenant_id, *GCTX.sql_proxy_, user_ls_sync_scn))) {
    LOG_WARN("failed to get user scn", KR(ret), K(tenant_id));
  } else {
    if (user_ls_sync_scn < sys_ls_target_scn) {
      ret = OB_NEED_WAIT;
      LOG_WARN("wait some time, user_ls_sync_scn cannot be smaller than sys_ls_target_scn",
          KR(ret), K(tenant_id), K(user_ls_sync_scn), K(sys_ls_target_scn));
    } else {
      LOG_INFO("user_ls_sync_scn >= sys_ls_target_scn now", K(tenant_id), K(user_ls_sync_scn), K(sys_ls_target_scn));
    }
  }
  return ret;
}

int ObLSServiceHelper::revision_to_equal_status_(const ObLSStatusMachineParameter &machine,
                                      const share::ObTenantSwitchoverStatus &working_sw_status,
                                      ObTenantLSInfo& tenant_ls_info)
{
  int ret = OB_SUCCESS;
  const share::ObLSStatusInfo &status_info = machine.status_info_;
  const share::ObLSAttr &ls_info = machine.ls_info_;
  const uint64_t tenant_id = tenant_ls_info.get_tenant_id();
  ObLSStatusOperator status_op;
  if (OB_UNLIKELY(!machine.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status machine is valid", KR(ret), K(machine));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (ls_info.get_ls_status() == status_info.status_) {
    //if ls and ls status is equal, need to process next ls status
  } else if (!ls_info.is_valid()) {
    // LS has beed deleted in __all_ls
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    if (status_info.ls_is_wait_offline()) {
      //already offline, status will be deleted by GC
    } else if (status_info.ls_is_dropping()
               || status_info.ls_is_tenant_dropping()) {
      // LS has been in dropping or tenant_dropping, should be offlined
      // NOTE: SYS LS will not be HERE, as SYS LS can not be deleted in __all_ls when tenant dropping.
      //       See ObPrimaryLSService::try_delete_ls_() for SYS LS offline operation.
      if (OB_FAIL(offline_ls(tenant_id, status_info.ls_id_, status_info.status_, working_sw_status))) {
        LOG_WARN("failed to offline ls", KR(ret), K(status_info), K(tenant_id), K(working_sw_status));
      }
    } else if (status_info.ls_is_creating()
               || status_info.ls_is_created()
               || status_info.ls_is_create_abort()) {
      // ls may create_abort, it should be dropped in __all_ls_status
      if (OB_FAIL(ls_life_agent.drop_ls(tenant_id, machine.ls_id_, working_sw_status))) {
        LOG_WARN("failed to delete ls", KR(ret), K(machine));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    }
  } else if (ls_info.ls_is_creating()) {
    if (!status_info.is_valid()) {
      //create ls
      START_TRANSACTION(GCTX.sql_proxy_, ObLSLifeIAgent::get_exec_tenant_id(tenant_id));
      if (FAILEDx(create_new_ls_in_trans(ls_info.get_ls_id(),
                                         ls_info.get_ls_group_id(),
                                         ls_info.get_create_scn(),
                                         working_sw_status,
                                         tenant_ls_info, trans, ls_info.get_ls_flag(),
                                         OB_INVALID_TENANT_ID/*source_tenant_id*/))) {
        LOG_WARN("failed to create new ls in trans", KR(ret), K(ls_info), K(tenant_ls_info), K(working_sw_status));
      }
      END_TRANSACTION(trans);
    } else if (status_info.ls_is_created() || status_info.ls_is_create_abort()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    }
  } else if (ls_info.ls_is_normal()) {
    if (!status_info.ls_is_created()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    } else if (OB_FAIL(status_op.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), working_sw_status, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id),
          K(ls_info), K(working_sw_status));
    }
  } else if (ls_info.ls_is_pre_tenant_dropping()) {
    if (!status_info.ls_is_normal() || !status_info.ls_id_.is_sys_ls()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    } else if (OB_FAIL(status_op.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), working_sw_status, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id),
          K(ls_info), K(working_sw_status));
    }
  } else if (ls_info.ls_is_dropping()) {
    if (!status_info.ls_is_normal()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    } else if (OB_FAIL(status_op.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), working_sw_status, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id),
          K(ls_info), K(working_sw_status));
    }
  } else if (ls_info.ls_is_tenant_dropping()) {
    if (status_info.ls_id_.is_sys_ls()) {
      if (status_info.ls_is_wait_offline()) {
        //sys ls maybe in tenant dropping and wait offline
      } else if (!status_info.ls_is_pre_tenant_dropping()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status machine not expected", KR(ret), K(machine));
      }
      // Change SYS LS status to TENANT_DROPPING
      else if (OB_FAIL(status_op.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), working_sw_status, *GCTX.sql_proxy_))) {
        LOG_WARN("failed to update ls status to tenant_dropping", KR(ret), K(status_info), K(tenant_id),
            K(ls_info), K(working_sw_status));
      }
    } else if (OB_UNLIKELY(status_info.ls_is_creating())) {
      //status_info may in created, normal, dropping, tenant_dropping
      //while ls in dropping, it must be change to tenant_dropping, no need to transfer tablet
      //if status in __all_ls is creating, while tenant_dropping, it will be create abort directly
      //the status in __all_ls_status maybe created or creating, but status is created, status in
      //__all_ls maybe normal, so while tenant_dropping, status cannot be creating.
      //while status_info is wait_offline, ls_info is been drop_end, so cannot be tenant_dropping
      //sys ls is special, sys ls no drop_end
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status info not expected", KR(ret), K(machine));
    } else if (OB_FAIL(status_op.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), working_sw_status, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id),
          K(ls_info), K(working_sw_status));
    }
  } else {
    //other status can not be in __all_ls
    //such as created, wait_offline
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls not expected in all_ls", KR(ret), K(machine), K(working_sw_status));
  }
  return ret;
}



int ObLSServiceHelper::process_alter_ls(const share::ObLSID &ls_id,
                       const uint64_t &old_ls_group_id,
                       const uint64_t &new_ls_group_id,
                       const uint64_t &old_unit_group_id,
                       ObTenantLSInfo& tenant_info,
                       ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_info.get_tenant_id();;
  uint64_t unit_group_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!ls_id.is_valid() || OB_INVALID_ID == old_ls_group_id || OB_INVALID_ID == new_ls_group_id
        || old_ls_group_id == new_ls_group_id || OB_INVALID_ID == old_unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(old_ls_group_id),
        K(new_ls_group_id), K(old_unit_group_id));
  } else if (OB_FAIL(tenant_info.gather_stat())) {
    LOG_WARN("failed to gather stat", KR(ret));
  } else {
    // update ls group id in status
    int64_t unit_group_index = OB_INVALID_INDEX_INT64;
    ObLSGroupInfo ls_group_info;
    if (OB_SUCC(tenant_info.get_ls_group_info(new_ls_group_id, ls_group_info))) {
       unit_group_id = ls_group_info.unit_group_id_;
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get ls group info", KR(ret), K(new_ls_group_id));
    } else {
      ret = OB_SUCCESS;  // try to get new unit group id
      if (OB_FAIL(tenant_info.get_next_unit_group(unit_group_index))) {
        LOG_WARN("failed to get next unit group", KR(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 == unit_group_index ||
                             unit_group_index >= tenant_info.get_unit_group_array().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to next unit group", KR(ret), K(unit_group_index));
      } else {
        unit_group_id = tenant_info.get_unit_group_array().at(unit_group_index).unit_group_id_;
      }
    }
    ObLSStatusOperator status_op;
    if (FAILEDx(status_op.alter_ls_group_id(
            tenant_id, ls_id, old_ls_group_id, new_ls_group_id,
            old_unit_group_id, unit_group_id, sql_proxy))) {
      LOG_WARN("failed to update ls group id", KR(ret), K(new_ls_group_id), K(old_ls_group_id),
               K(unit_group_id), K(tenant_info), K(old_unit_group_id));
    }
    LOG_INFO("[LS_MGR] alter ls group id", KR(ret), K(old_ls_group_id),
        K(new_ls_group_id), K(unit_group_id), K(old_unit_group_id));
  }
  return ret;
}

int ObLSServiceHelper::create_new_ls_in_trans(
    const share::ObLSID &ls_id,
    const uint64_t ls_group_id,
    const SCN &create_scn,
    const share::ObTenantSwitchoverStatus &working_sw_status,
    ObTenantLSInfo& tenant_ls_info,
    ObMySQLTransaction &trans,
    const share::ObLSFlag &ls_flag,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t info_index = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || OB_INVALID_ID == ls_group_id
                  || !create_scn.is_valid()
                  || !ls_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id), K(ls_group_id), K(create_scn), K(ls_flag));
  } else if (OB_ISNULL(tenant_ls_info.get_tenant_schema())
             || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret), KP(tenant_ls_info.get_tenant_schema()), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(tenant_ls_info.gather_stat())) {
    LOG_WARN("failed to gather stat", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_ls_info.get_tenant_id();
    share::ObLSStatusInfo new_info;
    ObLSGroupInfo group_info;
    ObZone primary_zone;
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    ObSqlString zone_priority;
    uint64_t unit_group_id = 0;
    if (OB_INVALID_TENANT_ID == source_tenant_id) {
      if (OB_FAIL(construct_unit_group_id_and_primary_zone_(
                      ls_id,
                      ls_group_id,
                      ls_flag,
                      tenant_ls_info,
                      unit_group_id,
                      primary_zone))) {
        LOG_WARN("fail to construct unit group id and primary zone", KR(ret), K(ls_id), K(ls_group_id), K(ls_flag), K(tenant_ls_info));
      }
    } else {
      // for clone tenant
      if (OB_FAIL(construct_unit_group_id_and_primary_zone_for_clone_tenant_(ls_id, source_tenant_id, tenant_id, unit_group_id, primary_zone))) {
        LOG_WARN("fail to construct unit group id and primary zone for clone tenant", KR(ret), K(ls_id), K(source_tenant_id), K(tenant_id));
      }
    }

    if (FAILEDx(new_info.init(tenant_id, ls_id,
                              ls_group_id,
                              share::OB_LS_CREATING,
                              unit_group_id,
                              primary_zone, ls_flag))) {
      LOG_WARN("failed to init new info", KR(ret), K(tenant_id), K(unit_group_id),
               K(ls_id), K(ls_group_id), K(group_info), K(primary_zone), K(ls_flag));
    } else if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(primary_zone,
            *tenant_ls_info.get_tenant_schema(), zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(primary_zone), K(zone_priority));
    } else if (OB_FAIL(ls_life_agent.create_new_ls_in_trans(new_info, create_scn,
            zone_priority.string(), working_sw_status, trans))) {
      LOG_WARN("failed to insert ls info", KR(ret), K(new_info), K(create_scn), K(zone_priority));
    }
  }
  return ret;
}

int ObLSServiceHelper::construct_unit_group_id_and_primary_zone_for_clone_tenant_(
    const share::ObLSID &ls_id,
    const uint64_t source_tenant_id,
    const uint64_t tenant_id,
    uint64_t &unit_group_id,
    ObZone &primary_zone)
{
  int ret = OB_SUCCESS;
  uint64_t source_unit_group_id = OB_INVALID_ID;
  unit_group_id = OB_INVALID_ID;
  primary_zone.reset();
  if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == source_tenant_id)
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id), K(source_tenant_id), K(tenant_id), KP(GCTX.sql_proxy_));
  } else {
    // construct primary zone
    share::ObLSStatusInfo ls_status_info;
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      share::ObLSStatusOperator ls_status_operator;
      if (OB_FAIL(ls_status_operator.get_ls_status_info(source_tenant_id, ls_id, ls_status_info, *GCTX.sql_proxy_))) {
        LOG_WARN("fail to get all ls status", KR(ret), K(source_tenant_id), K(ls_id), K(ls_status_info));
      } else if (OB_UNLIKELY(!ls_status_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("source ls status info is invalid", KR(ret), K(ls_status_info));
      } else if (OB_FAIL(primary_zone.assign(ls_status_info.primary_zone_))) {
        LOG_WARN("fail to assign primary zone", KR(ret), K(ls_status_info));
      } else {
        source_unit_group_id = ls_status_info.unit_group_id_;
      }
    }
    // construct sql to fetch unit group id
    ObSqlString sql;
    const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
    if (OB_FAIL(ret)) {
    } else if (0 == source_unit_group_id) {
      unit_group_id = 0;
    } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT b.unit_group_id FROM "
                                      "(SELECT svr_ip, svr_port FROM %s WHERE tenant_id = %ld AND unit_group_id = %ld) as a "
                                      "INNER JOIN "
                                      "(SELECT * FROM %s WHERE tenant_id = %ld) as b "
                                      "ON a.svr_ip = b.svr_ip AND a.svr_port = b.svr_port",
                                      OB_DBA_OB_UNITS_TNAME, source_tenant_id, source_unit_group_id,
                                      OB_DBA_OB_UNITS_TNAME, tenant_id))) {
      LOG_WARN("fail to construct sql to fetch unit group id for new log stream", KR(ret),
               K(source_tenant_id), K(tenant_id), K(source_unit_group_id));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret), K(sql));
        } else {
          ret = result->next();
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("unit group id not found", KR(ret), K(sql));
          } else if (OB_FAIL(ret)) {
            LOG_WARN("failed to get unit group id", KR(ret), K(sql));
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result, "unit_group_id", unit_group_id, uint64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("fail to get unit group id from result", KR(ret));
            } else if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id)) {
              ret = OB_STATE_NOT_MATCH;
              LOG_WARN("unexpected unit group id", KR(ret), K(sql), K(unit_group_id));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ITER_END != result->next()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expect only one row", KR(ret), K(sql));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLSServiceHelper::construct_unit_group_id_and_primary_zone_(
    const share::ObLSID &ls_id,
    const uint64_t ls_group_id,
    const share::ObLSFlag &ls_flag,
    ObTenantLSInfo &tenant_ls_info,
    uint64_t &unit_group_id,
    ObZone &primary_zone)
{
  int ret = OB_SUCCESS;
  unit_group_id = 0;
  primary_zone.reset();
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || OB_INVALID_ID == ls_group_id
                  || !ls_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id), K(ls_group_id), K(ls_flag));
  } else {
    ObLSGroupInfo group_info;
    if (0 == ls_group_id) {
      unit_group_id = 0;
      if (!ls_flag.is_duplicate_ls()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls without ls group must be duplate", KR(ret),
                 K(ls_group_id), K(ls_id), K(ls_flag));
      } else if (OB_FAIL(primary_zone.assign(tenant_ls_info.get_primary_zone().at(0)))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(tenant_ls_info));
      }
    } else if (OB_SUCC(tenant_ls_info.get_ls_group_info(
            ls_group_id, group_info))) {
      // need a new primary zone
      unit_group_id = group_info.unit_group_id_;
      if (OB_FAIL(tenant_ls_info.get_next_primary_zone(group_info, primary_zone))) {
        LOG_WARN("failed to get next primary zone", KR(ret), K(group_info));
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // need a new unit group
      ret = OB_SUCCESS;
      int64_t unit_group_index = OB_INVALID_INDEX_INT64;
      if (OB_FAIL(tenant_ls_info.get_next_unit_group(unit_group_index))) {
        LOG_WARN("failed to get next unit group", KR(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 == unit_group_index
                             || unit_group_index >= tenant_ls_info.get_unit_group_array().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to next unit group", KR(ret), K(unit_group_index), K(tenant_ls_info));
      } else {
        unit_group_id = tenant_ls_info.get_unit_group_array().at(unit_group_index).unit_group_id_;
        if (OB_FAIL(primary_zone.assign(tenant_ls_info.get_primary_zone().at(0)))) {
          LOG_WARN("failed to assign primary zone", KR(ret), K(tenant_ls_info));
        }
      }
    } else {
      LOG_WARN("failed to get ls group info", KR(ret), K(ls_group_id), K(tenant_ls_info));
    }
  }
  return ret;
}

int ObLSServiceHelper::balance_ls_group(
    const bool need_execute_balance,
    ObTenantLSInfo& tenant_ls_info,
    bool &is_balanced)
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  is_balanced = true;
  if (OB_FAIL(tenant_ls_info.gather_stat())) {
    LOG_WARN("failed to gather stat", KR(ret));
  } else if (OB_FAIL(try_shrink_standby_unit_group_(tenant_ls_info, task_cnt))) {
    LOG_WARN("failed to shrink standby unit group", KR(ret), K(tenant_ls_info));
  } else if (0 != task_cnt) {
    LOG_INFO("has unit group need deleting, can not balance", K(task_cnt));
  } else {
    int64_t min_count = INT64_MAX, min_index = OB_INVALID_INDEX_INT64;
    int64_t max_count = 0, max_index = OB_INVALID_INDEX_INT64;
    do {
      min_count = INT64_MAX;
      max_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ls_info.get_unit_group_array().count(); ++i) {
        const ObUnitGroupInfo &unit_group_info = tenant_ls_info.get_unit_group_array().at(i);
        if (share::ObUnit::UNIT_STATUS_ACTIVE != unit_group_info.unit_status_) {
          ret = OB_NEED_WAIT;
          LOG_INFO("has unit group need deleting, can not balance", KR(ret), K(unit_group_info));
        } else {
          int64_t curr_ls_group_count = unit_group_info.ls_group_ids_.count();
          if (max_count < curr_ls_group_count) {
            max_count = curr_ls_group_count;
            max_index = i;
          }
          if (min_count > curr_ls_group_count) {
            min_count = curr_ls_group_count;
            min_index = i;
          }
        }
      }//end for
      if (OB_SUCC(ret) && max_count - min_count > 1) {
        is_balanced = false;
        if (need_execute_balance && OB_FAIL(balance_ls_group_between_unit_group_(tenant_ls_info, min_index, max_index))) {
          LOG_WARN("failed to balance ls group between unit group", KR(ret), K(tenant_ls_info),
              K(min_index), K(max_index));
        }
      }
    } while (OB_SUCC(ret) && (max_count - min_count) > 1 && need_execute_balance);
  }
  return ret;
}

int ObLSServiceHelper::balance_ls_group_between_unit_group_(ObTenantLSInfo& tenant_ls_info,
                                           const int64_t min_index, const int64_t max_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(min_index == max_index
                         || min_index < 0 || min_index > tenant_ls_info.get_unit_group_array().count()
                         || max_index < 0 || max_index > tenant_ls_info.get_unit_group_array().count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(min_index), K(max_index), K(tenant_ls_info));
  } else {
    ObUnitGroupInfo &dest_info = tenant_ls_info.get_unit_group_array().at(min_index);
    ObUnitGroupInfo &src_info = tenant_ls_info.get_unit_group_array().at(max_index);
    const int64_t ls_group_count = src_info.ls_group_ids_.count();
    if (ls_group_count < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src info is unexpected", KR(ret), K(ls_group_count), K(src_info));
    } else {
      const uint64_t ls_group_id = src_info.ls_group_ids_.at(ls_group_count - 1);
      if (OB_FAIL(try_update_ls_unit_group_(tenant_ls_info, ls_group_id, src_info, dest_info))) {
        LOG_WARN("failed to update ls unit group", KR(ret), K(tenant_ls_info), K(ls_group_id), K(src_info), K(dest_info));
      }
    }
  }
  return ret;
}

int ObLSServiceHelper::try_update_ls_unit_group_(
    ObTenantLSInfo& tenant_ls_info,
      const uint64_t ls_group_id,
      ObUnitGroupInfo &src_info,
      ObUnitGroupInfo &dest_info)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX_INT64;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!tenant_ls_info.is_valid()
        || OB_INVALID_ID == ls_group_id || !src_info.is_valid() || !dest_info.is_valid()
        || src_info == dest_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_ls_info), K(ls_group_id), K(src_info), K(dest_info));
  } else if (!has_exist_in_array(src_info.ls_group_ids_, ls_group_id, &index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group not in src unit group", KR(ret), K(ls_group_id), K(src_info));
  } else {
    ObLSGroupInfo ls_group_info;
    const uint64_t tenant_id = tenant_ls_info.get_tenant_id();
    if (OB_FAIL(tenant_ls_info.get_ls_group_info(ls_group_id, ls_group_info))) {
      LOG_WARN("failed to get ls group_info", KR(ret), K(ls_group_id));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < ls_group_info.ls_ids_.count(); ++j) {
        share::ObLSStatusOperator status_op;
        ObLSID ls_id = ls_group_info.ls_ids_.at(j);
        if (OB_FAIL(status_op.alter_unit_group_id(tenant_id, ls_id,
                ls_group_id, src_info.unit_group_id_,
                dest_info.unit_group_id_, *GCTX.sql_proxy_))) {
          LOG_WARN("failed to alter unit group", KR(ret), K(tenant_id), K(ls_id), K(ls_group_id),
              K(src_info), K(dest_info));
        }
      }
      LOG_INFO("[LS_MGR]balance ls group to unit group", KR(ret), K(ls_group_id), K(src_info), K(dest_info));
    }
    if (FAILEDx(src_info.ls_group_ids_.remove(index))) {
      LOG_WARN("failed to remove", KR(ret), K(index), K(src_info));
    } else if (OB_FAIL(dest_info.ls_group_ids_.push_back(ls_group_id))) {
      LOG_WARN("failed to push back", KR(ret), K(ls_group_id), K(src_info));
    }
  }
  return ret;

}

int ObLSServiceHelper::try_shrink_standby_unit_group_(
    ObTenantLSInfo& tenant_ls_info, int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  task_cnt = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!tenant_ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is invalid", KR(ret), K(tenant_ls_info));
  } else {
    ObUnitGroupInfoArray &unit_group_array = tenant_ls_info.get_unit_group_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array.count() && task_cnt == 0; ++i) {
      ObUnitGroupInfo &unit_group_info = unit_group_array.at(i);
      if (share::ObUnit::UNIT_STATUS_ACTIVE != unit_group_info.unit_status_) {
        FLOG_INFO("has unit is in deleting status, need alter unit group id", K(unit_group_info));
        if (unit_group_info.ls_group_ids_.count() > 0) {
          task_cnt = 1;
          int64_t unit_group_index = OB_INVALID_INDEX_INT64;
          //for each ls group, get new unit group, process ls group one by one
          for (int64_t j = 0; OB_SUCC(ret) && j < unit_group_info.ls_group_ids_.count(); ++j) {
            if (OB_FAIL(tenant_ls_info.get_next_unit_group(unit_group_index))) {
              LOG_WARN("failed to get next unit group", KR(ret), K(tenant_ls_info));
            } else if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 == unit_group_index
                  || unit_group_index >= unit_group_array.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to next unit group", KR(ret), K(unit_group_index), K(tenant_ls_info));
            } else {
              ObUnitGroupInfo &dest_info = unit_group_array.at(unit_group_index);
              const uint64_t ls_group_id = unit_group_info.ls_group_ids_.at(j);
              if (OB_FAIL(try_update_ls_unit_group_(tenant_ls_info, ls_group_id,
                      unit_group_info, dest_info))) {
                LOG_WARN("failed to update ls unit group", KR(ret), K(ls_group_id),
                    K(unit_group_info), K(dest_info), K(tenant_ls_info));
              }
            }
          }//end for j
        }
      }
    }//end for i
  }
  return ret;
}

/////////////ObTenantLSInfo
void ObTenantLSInfo::reset()
{
  is_load_ = false;
  status_array_.reset();
  unit_group_array_.reset();
  ls_group_array_.reset();
  primary_zone_.reset();
  status_map_.reuse();
}
bool ObTenantLSInfo::is_valid() const
{
  return ATOMIC_LOAD(&is_load_);
}

int ObTenantLSInfo::gather_stat()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(tenant_schema_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    ObArray<share::ObSimpleUnitGroup> unit_groups;
    if (OB_FAIL(ObLSServiceHelper::get_primary_zone_unit_array(
            tenant_schema_, primary_zone_, unit_groups))) {
      LOG_WARN("failed to get primary zone unit", KR(ret), KPC(tenant_schema_));
    } else {
      ObUnitGroupInfo info;
      for (int64_t j = 0; OB_SUCC(ret) && j < unit_groups.count(); ++j) {
        info.reset();
        const ObSimpleUnitGroup &unit_group = unit_groups.at(j);
        if (OB_FAIL(info.init(unit_group.get_unit_group_id(), unit_group.get_status()))) {
          LOG_WARN("failed to init unit info", KR(ret), K(unit_group));
        } else if (OB_FAIL(unit_group_array_.push_back(info))) {
          LOG_WARN("fail to push back", KR(ret), K(info));
        }
      }
    }

    if (FAILEDx(gather_all_ls_info_())) {
      LOG_WARN("failed to get all ls info", KR(ret), K(tenant_id_));
    } else {
      is_load_ = true;
    }
  }
  LOG_INFO("[LS_MGR] gather stat", KR(ret), K(primary_zone_), K(unit_group_array_));
  return ret;
}

int ObTenantLSInfo::gather_all_ls_info_()
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else if (OB_ISNULL(trans_)) {
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
                 tenant_id_, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to get all ls status by order", KR(ret), K(tenant_id_), KP(trans_));
    }
  } else if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
          tenant_id_, status_info_array, *trans_))) {
    LOG_WARN("failed to get all ls status by order", KR(ret), K(tenant_id_), KP(trans_));
  }
  if (OB_FAIL(ret)) {
  } else {
    const int64_t count = max(1, hash::cal_next_prime(status_info_array.count()));
    if (!status_map_.created() && OB_FAIL(status_map_.create(count, "LogStrInfo", "LogStrInfo"))) {
      LOG_WARN("failed to create ls map", KR(ret), K(count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        const share::ObLSStatusInfo &info = status_info_array.at(i);
        if (OB_FAIL(add_ls_status_info_(info))) {
          LOG_WARN("failed to add ls status info", KR(ret), K(i), K(info));
        }
      }// end for
    }
  }
  return ret;
}

int ObTenantLSInfo::add_ls_to_ls_group_(const share::ObLSStatusInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls status is invalid", KR(ret), K(info));
  } else {
    int64_t group_index = OB_INVALID_INDEX_INT64;
    for (int64_t j = 0; OB_SUCC(ret) && j < ls_group_array_.count();
         ++j) {
      const ObLSGroupInfo &group = ls_group_array_.at(j);
      if (info.ls_group_id_ == group.ls_group_id_) {
        group_index = j;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 == group_index) {
      ObLSGroupInfo group;
      group_index = ls_group_array_.count();
      if (OB_FAIL(group.init(info.unit_group_id_, info.ls_group_id_))) {
        LOG_WARN("failed to init ls group", KR(ret), K(info));
      } else if (OB_FAIL(ls_group_array_.push_back(group))) {
        LOG_WARN("failed to pushback group", KR(ret), K(group));
      } else if (0 == info.ls_group_id_) {
        //duplate ls, no need to set to unit group
      } else if (OB_FAIL(add_ls_group_to_unit_group_(group))) {
        LOG_WARN("failed to add ls group to unit group", KR(ret), K(group));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(group_index >= ls_group_array_.count()
            || OB_INVALID_INDEX_INT64 == group_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group index is invalid", KR(ret), K(group_index), "count",
                 ls_group_array_.count());
      } else if (OB_FAIL(ls_group_array_.at(group_index)
                             .ls_ids_.push_back(info.ls_id_))) {
        LOG_WARN("failed to push back ls id", KR(ret), K(group_index),
                 K(info));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::add_ls_group_to_unit_group_(const ObLSGroupInfo &group_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_UNLIKELY(!group_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_info is invalid", KR(ret), K(group_info));
  } else {
    found = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < unit_group_array_.count(); ++j) {
      ObUnitGroupInfo &unit_info = unit_group_array_.at(j);
      if (group_info.unit_group_id_ == unit_info.unit_group_id_) {
        found = true;
        if (OB_FAIL(unit_info.ls_group_ids_.push_back(
                group_info.ls_group_id_))) {
          LOG_WARN("failed to push back ls group id", KR(ret),
                   K(group_info), K(unit_info));
        }
      }
    }  // end j
    if (OB_SUCC(ret) && !found) {
      //can found unit_group for a ls group
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find unit group for ls group", KR(ret),
               K(group_info), K(unit_group_array_));
    }
  }

  return ret;
}

int ObTenantLSInfo::add_ls_status_info_(
    const share::ObLSStatusInfo &ls_info)
{
  int ret = OB_SUCCESS;
  const int64_t index = status_array_.count();
  if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info is invalid", KR(ret), K(ls_info));
  } else if (OB_FAIL(status_array_.push_back(ls_info))) {
    LOG_WARN("failed to remove status", KR(ret), K(ls_info));
  } else if (OB_FAIL(status_map_.set_refactored(ls_info.ls_id_, index))) {
    LOG_WARN("failed to remove ls from map", KR(ret), K(ls_info), K(index));
  } else if (ls_info.ls_id_.is_sys_ls()) {
    //sys ls no ls group
  } else if (OB_FAIL(add_ls_to_ls_group_(ls_info))) {
    LOG_WARN("failed to add ls info", KR(ret), K(ls_info));
  }
  return ret;
}

int ObTenantLSInfo::get_ls_group_info(
    const uint64_t ls_group_id, ObLSGroupInfo &info) const
{
  int ret = OB_SUCCESS;
  info.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group id is invalid", KR(ret), K(ls_group_id));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_group_array_.count(); ++i) {
      const ObLSGroupInfo group_info = ls_group_array_.at(i);
      if (ls_group_id == group_info.ls_group_id_) {
        found = true;
        if (OB_FAIL(info.assign(group_info))) {
          LOG_WARN("failed to assign group info", KR(ret), K(group_info));
        }
        break;
      }
    }//end for
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find ls group info", KR(ret),
              K(ls_group_id));
    }
  }
  return ret;
}

int ObTenantLSInfo::get_ls_status_info(
    const share::ObLSID &ls_id,
    share::ObLSStatusInfo &info,
    int64_t &info_index) const
{
  int ret = OB_SUCCESS;
  info.reset();
  info_index = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else if (OB_FAIL(status_map_.get_refactored(ls_id, info_index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("failed to find ls index", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(info_index < 0 || info_index >= status_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info index not valid", KR(ret), K(info_index), K(ls_id),
            "status_count", status_array_.count());
  } else if (OB_FAIL(info.assign(status_array_.at(info_index)))) {
    LOG_WARN("failed to assign ls info", KR(ret), K(info_index),
             "status", status_array_.at(info_index));
  }
  return ret;
}

int ObTenantLSInfo::get_next_unit_group(int64_t &group_index)
{
  int ret = OB_SUCCESS;
  group_index = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else {
    int64_t ls_count = OB_INVALID_COUNT;
    // Find the unit group with the least number of log streams
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array_.count(); ++i) {
      const ObUnitGroupInfo &info = unit_group_array_.at(i);
      const int64_t count = info.ls_group_ids_.count();
      if (share::ObUnit::UNIT_STATUS_ACTIVE == info.unit_status_) {
        if (OB_INVALID_COUNT == ls_count || ls_count > count) {
          ls_count = count;
          group_index = i;
          if (0 == count) {
            //the first has no ls group unit group
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_INDEX_INT64 == group_index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to find next unit group", KR(ret),
                 K(unit_group_array_), K(ls_count));
      }
    }
  }
  LOG_INFO("get next primary zone", KR(ret), K(group_index));
  return ret;
}

int ObTenantLSInfo::get_next_primary_zone(
  const ObLSGroupInfo &group_info,
  ObZone &primary_zone)
{
  int ret = OB_SUCCESS;
  primary_zone.reset();
  if (OB_UNLIKELY(!group_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("group info is invalid", KR(ret), K(group_info));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant ls info not valid", KR(ret));
  } else {
    share::ObLSStatusInfo info;
    int64_t ls_count = OB_INVALID_COUNT;
    int64_t info_index = OB_INVALID_INDEX_INT64;
    for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_.count(); ++i) {
      const ObZone &zone = primary_zone_.at(i);
      int64_t primary_zone_count = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < group_info.ls_ids_.count(); ++j) {
        const share::ObLSID &id = group_info.ls_ids_.at(j);
        if (OB_FAIL(get_ls_status_info(id, info, info_index))) {
          LOG_WARN("failed to find ls info", KR(ret), K(id), K(j));
        } else if (zone == info.primary_zone_) {
          primary_zone_count++;
        }
      }//end for j
      if (OB_SUCC(ret)) {
        if (OB_INVALID_COUNT == ls_count || ls_count > primary_zone_count) {
          ls_count = primary_zone_count;
          if (OB_FAIL(primary_zone.assign(zone))) {
            LOG_WARN("failed to assign zone", KR(ret), K(zone));
          } else if (0 == primary_zone_count) {
            //the first zone has no primary zone
            break;
          }
        }
      }
    }//end for i
    if (OB_SUCC(ret)) {
      if (primary_zone.is_empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to find next primary zone", KR(ret), K(primary_zone_),
                 K(group_info), K(ls_count));
      }
    }
    LOG_INFO("get next primary zone", KR(ret), K(group_info), K(primary_zone));
  }
  return ret;
}
int ObLSServiceHelper::check_transfer_task_replay(const uint64_t tenant_id,
      const share::ObLSID &src_ls,
      const share::ObLSID &dest_ls,
      const share::SCN &transfer_scn,
      bool &replay_finish)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator ls_operator;
  share::ObLSStatusInfo ls_status;
  SCN readble_scn;
  replay_finish = true;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !src_ls.is_valid() || !dest_ls.is_valid()
        || !transfer_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(src_ls),
        K(dest_ls), K(transfer_scn));
  } else if (OB_FAIL(check_ls_transfer_replay_(tenant_id, src_ls, transfer_scn, replay_finish))) {
    LOG_WARN("failed to check ls transfer replay", KR(ret), K(tenant_id), K(src_ls), K(transfer_scn));
  } else if (!replay_finish) {
    LOG_WARN("src ls has not replay transfer finish", K(tenant_id), K(src_ls));
  } else if (OB_FAIL(check_ls_transfer_replay_(tenant_id, dest_ls, transfer_scn, replay_finish))) {
    LOG_WARN("failed to check ls transfer replay", KR(ret), K(tenant_id), K(dest_ls), K(transfer_scn));
  } else if (!replay_finish) {
    LOG_WARN("dest ls has not replay transfer finish", K(tenant_id), K(dest_ls));
  }
  return ret;
}

int ObLSServiceHelper::check_ls_transfer_replay_(const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &transfer_scn,
      bool &replay_finish)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator ls_operator;
  share::ObLSStatusInfo ls_status;
  SCN readable_scn;
  replay_finish = true;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !ls_id.is_valid()
        || !transfer_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(transfer_scn));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ls_operator.get_ls_status_info(tenant_id, ls_id,
          ls_status, *GCTX.sql_proxy_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("src ls not exist, no need check", K(tenant_id), K(ls_id));
    } else {
      LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id), K(ls_id));
    }
  } else if (OB_FAIL(get_ls_all_replica_readable_scn_(tenant_id, ls_id, readable_scn))) {
    LOG_WARN("failed to get ls all replica readable scn", KR(ret), K(tenant_id), K(ls_id));
  } else if (readable_scn < transfer_scn) {
    replay_finish = false;
    LOG_INFO("need wait, ls has not replay finish transfer", K(tenant_id),
        K(ls_id), K(readable_scn), K(transfer_scn));
  }
  return ret;
}

int ObLSServiceHelper::get_ls_all_replica_readable_scn_(const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!ls_id.is_valid() || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret), KP(GCTX.location_service_),
        KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    obrpc::ObGetLSReplayedScnArg arg;
    ObAddr leader;
    ObGetLSReplayedScnRes result;
    const int64_t timeout = ctx.get_timeout();
    ObGetLSReplayedScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_replayed_scn);
    if (OB_FAIL(arg.init(tenant_id, ls_id, true))) {
      LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(GCTX.location_service_->get_leader(
            GCONF.cluster_id, tenant_id, ls_id, false, leader))) {
      LOG_WARN("failed to get leader", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(proxy.call(leader, timeout, tenant_id, arg))) {
      LOG_WARN("failed to get ls repalyed scn", KR(ret), K(leader), K(tenant_id),
          K(timeout), K(arg));
    } else if (OB_FAIL(proxy.wait())) {
      LOG_WARN("failed to get wait all rpc", KR(ret), K(tenant_id), K(ls_id), K(leader));
    } else if (OB_ISNULL(proxy.get_results().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret), K(tenant_id), K(leader), K(ls_id));
    } else {
      readable_scn = proxy.get_results().at(0)->get_cur_readable_scn();
      LOG_INFO("get all replica readable scn", K(ls_id), K(readable_scn));
    }
  }
  return ret;

}

}//end of rootserver
}
