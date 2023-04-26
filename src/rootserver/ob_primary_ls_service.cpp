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
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/ob_tenant_recovery_reportor.h"//update_ls_recovery
#include "rootserver/ob_tenant_role_transition_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo

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
                            const share::ObLSStatus &ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("id is invalid", KR(ret), K(id));
  } else if (OB_FAIL(status_info_.assign(status_info))) {
    LOG_WARN("failed to assign status info", KR(ret), K(status_info));
  } else {
    ls_info_ = ls_info;
    ls_id_ = id;
  }
  return ret;
}

void ObLSStatusMachineParameter::reset()
{
  ls_id_.reset();
  ls_info_ = share::OB_LS_EMPTY;
  status_info_.reset();
}

/////////////ObTenantLSInfo

void ObTenantLSInfo::reset()
{
  is_load_ = false;
  status_array_.reset();
  unit_group_array_.reset();
  ls_group_array_.reset();
  primary_zone_.reset();
  max_ls_id_ = OB_INVALID_ID;
  max_ls_group_id_ = OB_INVALID_ID;
}


bool ObTenantLSInfo::is_valid() const
{
  return ATOMIC_LOAD(&is_load_);
}

//Regardless of the tenant being dropped,
//handle the asynchronous operation of status and history table
int ObTenantLSInfo::process_ls_status_missmatch(
    const bool lock_sys_ls,
    const share::ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  share::ObLSAttrArray ls_array;
  ObArray<ObLSStatusMachineParameter> status_machine_array;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else if (OB_FAIL(ls_operator_.get_all_ls_by_order(lock_sys_ls, ls_array))) {
    LOG_WARN("failed to insert ls operation", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret));
    }
  }
  if (FAILEDx(construct_ls_status_machine_(status_info_array, ls_array,
                                                  status_machine_array))) {
    LOG_WARN("failed to construct ls status machine array", KR(ret),
             K(status_info_array), K(ls_array));
  } else {
    int tmp_ret = OB_SUCCESS;
    ARRAY_FOREACH_NORET(status_machine_array, idx) {
      const ObLSStatusMachineParameter &machine = status_machine_array.at(idx);
      //ignore error of each ls
      //may ls can not create success
      if (OB_SUCCESS != (tmp_ret = fix_ls_status_(machine, working_sw_status))) {
        LOG_WARN("failed to fix ls status", KR(ret), KR(tmp_ret), K(machine));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }

  return ret;
}

int ObTenantLSInfo::construct_ls_status_machine_(
    const share::ObLSStatusInfoIArray &status_info_array,
    const share::ObLSAttrIArray &ls_array,
    common::ObIArray<ObLSStatusMachineParameter> &status_machine_array)
{
  int ret = OB_SUCCESS;
  status_machine_array.reset();
  if (OB_UNLIKELY(0 == status_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls array can not be empty", KR(ret), K(ls_array), K(status_info_array));
  } else {
    // merge by sort
    int64_t status_index = 0;
    int64_t ls_index = 0;
    ObLSStatusMachineParameter status_machine;
    const int64_t status_count = status_info_array.count();
    const int64_t ls_count = ls_array.count();
    share::ObLSStatusInfo status_info;
    ObLSID ls_id;
    ObLSStatus ls_info_status = share::OB_LS_EMPTY;
    while ((status_index < status_count || ls_index < ls_count) && OB_SUCC(ret)) {
      status_machine.reset();
      if (status_index == status_count) {
        //status already end
        ls_id = ls_array.at(ls_index).get_ls_id();
        ls_info_status = ls_array.at(ls_index).get_ls_status();
        status_info.reset();
        ls_index++;
      } else if (ls_index == ls_count) {
        //ls already end
        if (OB_FAIL(status_info.assign(status_info_array.at(status_index)))) {
          LOG_WARN("failed to assign status info", KR(ret), K(status_index));
        } else {
          status_index++;
          ls_id = status_info.ls_id_;
          ls_info_status = share::OB_LS_EMPTY;
        }
      } else {
        const share::ObLSStatusInfo &tmp_status_info = status_info_array.at(status_index);
        const share::ObLSAttr &tmp_ls_info = ls_array.at(ls_index);
        if (tmp_status_info.ls_id_ == tmp_ls_info.get_ls_id()) {
          status_index++;
          ls_index++;
          ls_info_status = tmp_ls_info.get_ls_status();
          ls_id = tmp_status_info.ls_id_;
          if (OB_FAIL(status_info.assign(tmp_status_info))) {
            LOG_WARN("failed to assign status info", KR(ret), K(tmp_status_info));
          }
        } else if (tmp_status_info.ls_id_ > tmp_ls_info.get_ls_id()) {
          ls_index++;
          ls_id = tmp_ls_info.get_ls_id();
          ls_info_status = tmp_ls_info.get_ls_status();
          status_info.reset();
        } else {
          status_index++;
          ls_id = tmp_status_info.ls_id_;
          ls_info_status = share::OB_LS_EMPTY;
          if (OB_FAIL(status_info.assign(tmp_status_info))) {
            LOG_WARN("failed to assign status info", KR(ret), K(tmp_status_info));
          }
        }
      }
      if (FAILEDx(status_machine.init(ls_id, status_info, ls_info_status))) {
        LOG_WARN("failed to push back status", KR(ret), K(ls_id), K(status_info), K(ls_info_status));
      } else if (OB_FAIL(status_machine_array.push_back(status_machine))) {
        LOG_WARN("failed to push back status machine", KR(ret), K(status_machine));
      }
    } // end while

  }
  return ret;
}

//for create new ls
//ls_info:creating, status_info:creating, status_info:created, ls_info:normal, status_info:normal

//for drop ls
//ls_info:dropping, status_info:dropping, ls_info:empty, status_info:wait_offline

//for drop tenant
//ls_info:tenant_dropping, status_info:tenant_dropping, ls_info:empty, status_info:wait_offline
//the sys ls can be tenant_dropping in status_info after all_users ls were deleted in ls_info
int ObTenantLSInfo::fix_ls_status_(const ObLSStatusMachineParameter &machine,
                                   const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  const share::ObLSStatusInfo &status_info = machine.status_info_;
  const uint64_t tenant_id = status_info.tenant_id_;
  ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
  if (OB_UNLIKELY(!machine.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status machine is valid", KR(ret), K(machine));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (ls_is_empty_status(machine.ls_info_)) {
    //the ls not in ls and in status
    if (status_info.ls_is_wait_offline()) {
      //already offline, status will be deleted by GC
    } else if (status_info.ls_is_dropping()
               || status_info.ls_is_tenant_dropping()) {
      SCN drop_scn;
      if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(tenant_id, drop_scn))) {
        LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ls_life_agent.set_ls_offline(tenant_id,
                     status_info.ls_id_, status_info.status_,
                     drop_scn, working_sw_status))) {
        LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id), K(drop_scn));
      }
    } else if (status_info.ls_is_creating()
               || status_info.ls_is_created()
               || status_info.ls_is_create_abort()) {
      //ls may create_abort
      if (OB_FAIL(ls_life_agent.drop_ls(tenant_id, machine.ls_id_, working_sw_status))) {
        LOG_WARN("failed to delete ls", KR(ret), K(machine));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    }
  } else if (ls_is_creating_status(machine.ls_info_)) {
    //the ls may is in creating
    if (!status_info.is_valid()) {
      // TODO fix later
      //in primary cluster, can create continue, but no need
      if (OB_FAIL(ls_operator_.delete_ls(machine.ls_id_, machine.ls_info_, working_sw_status))) {
        LOG_WARN("failed to delete ls", KR(ret), K(machine));
      }
    } else if (status_info.ls_is_creating() && working_sw_status.is_normal_status()) {
      //TODO check the primary zone or unit group id is valid
      ObLSRecoveryStat recovery_stat;
      ObLSRecoveryStatOperator ls_recovery_operator;
      if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, status_info.ls_id_,
              false/*for_update*/, recovery_stat, *sql_proxy_))) {
        LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id), K(status_info));
      } else if (OB_FAIL(do_create_ls_(status_info, recovery_stat.get_create_scn()))) {
        LOG_WARN("failed to create new ls", KR(ret), K(status_info),
            K(recovery_stat));
      }
    } else if (status_info.ls_is_created()) {
      if (OB_FAIL(process_ls_status_after_created_(status_info, working_sw_status))) {
        LOG_WARN("failed to create ls", KR(ret), K(status_info));
      }
    } else if (status_info.ls_is_create_abort()) {
      //drop ls
      if (OB_FAIL(ls_operator_.delete_ls(status_info.ls_id_, machine.ls_info_, working_sw_status))) {
        LOG_WARN("failed to remove ls not normal", KR(ret), K(machine));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    }
  } else if (ls_is_normal_status(machine.ls_info_)) {
    //the ls is normal
    if (status_info.ls_is_normal()) {
      //nothing
    } else if (status_info.ls_is_created()) {
      if (OB_FAIL(status_operator_.update_ls_status(
              tenant_id, status_info.ls_id_, status_info.status_,
              share::OB_LS_NORMAL, working_sw_status, *sql_proxy_))) {
        LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status machine not expected", KR(ret), K(machine));
    }
  } else if (ls_is_dropping_status(machine.ls_info_)) {
    if (FAILEDx(do_drop_ls_(status_info, working_sw_status))) {
      LOG_WARN("failed to drop ls", KR(ret), K(status_info));
    }
  } else if (ls_is_pre_tenant_dropping_status(machine.ls_info_)) {
    if (!machine.ls_id_.is_sys_ls()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("normal ls can not in pre tenant dropping status", KR(ret), K(machine));
    } else if (OB_FAIL(sys_ls_tenant_drop_(status_info, working_sw_status))) {
      LOG_WARN("failed to process sys ls", KR(ret), K(status_info));
    }
  } else if (ls_is_tenant_dropping_status(machine.ls_info_)) {
    if (status_info.ls_is_wait_offline()) {
      //sys ls will tenant_dropping and wait offline
    } else if (OB_FAIL(do_tenant_drop_ls_(status_info, working_sw_status))) {
      LOG_WARN("failed to drop ls", KR(ret), K(machine));
    }
  } else {
    //other status can not be in __all_ls
    //such as created, wait_offline
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls not expected in all_ls", KR(ret), K(machine));
  }
  return ret;
}

int ObTenantLSInfo::drop_tenant(const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  share::ObLSAttrArray ls_array;
  if (OB_FAIL(ls_operator_.get_all_ls_by_order(ls_array))) {
    LOG_WARN("failed to get all ls", KR(ret));
  } else {
    //1. set sys_ls to tenant_dropping in __all_ls
    //2. set user_ls to tenant_dropping or abort in __all_ls
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
      share::ObLSAttr &attr = ls_array.at(i);
      if (attr.get_ls_id().is_sys_ls()) {
        if (attr.ls_is_normal()) {
          if (OB_FAIL(ls_operator_.update_ls_status(attr.get_ls_id(),
          attr.get_ls_status(), share::OB_LS_PRE_TENANT_DROPPING, working_sw_status))) {
            LOG_WARN("failed to update ls status", KR(ret), K(attr));
          }
        }
      } else if (attr.ls_is_creating()) {
        //drop the status
        if (OB_FAIL(ls_operator_.delete_ls(attr.get_ls_id(), attr.get_ls_status(), working_sw_status))) {
          LOG_WARN("failed to remove ls not normal", KR(ret), K(attr));
        }
      } else {
        //no matter the status is in normal or dropping
        //may be the status in status info is created
        if (!attr.ls_is_tenant_dropping()) {
          if (OB_FAIL(ls_operator_.update_ls_status(
                  attr.get_ls_id(), attr.get_ls_status(),
                  share::OB_LS_TENANT_DROPPING, working_sw_status))) {
            LOG_WARN("failed to update ls status", KR(ret), K(attr));
          } else {
            LOG_INFO("[LS_MGR] set ls to tenant dropping", KR(ret), K(attr));
          }
        }
      }
    }//end for
  }
  return ret;
}
int ObTenantLSInfo::gather_stat(bool for_recovery)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(tenant_schema_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    ObUnitTableOperator unit_operator;
    common::ObArray<ObUnit> units;
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
      LOG_WARN("failed to init unit operator", KR(ret));
    } else if (OB_FAIL(unit_operator.get_units_by_tenant(
            tenant_id, units))) {
      LOG_WARN("failed to get all tenant unit", KR(ret), K(tenant_id));
    } else {
      ObUnitGroupInfo info;
      for (int64_t j = 0; OB_SUCC(ret) && j < units.count(); ++j) {
        info.reset();
        const ObUnit &unit = units.at(j);
        if (OB_FAIL(info.init(unit.unit_group_id_, unit.status_))) {
          LOG_WARN("failed to init unit info", KR(ret), K(unit));
        } else if (has_exist_in_array(unit_group_array_, info)) {
          //nothing
        } else if (OB_FAIL(unit_group_array_.push_back(info))) {
          LOG_WARN("fail to push back", KR(ret), K(info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (for_recovery) {
      } else {
        //get max ls id and max ls group id
        share::ObMaxIdFetcher id_fetcher(*sql_proxy_);
        if (OB_FAIL(id_fetcher.fetch_max_id(*sql_proxy_, tenant_id,
                share::OB_MAX_USED_LS_GROUP_ID_TYPE, max_ls_group_id_))) {
          LOG_WARN("failed to fetch max ls group id", KR(ret), K(tenant_id));
        } else if (OB_FAIL(id_fetcher.fetch_max_id(*sql_proxy_, tenant_id,
                share::OB_MAX_USED_LS_ID_TYPE, max_ls_id_))) {
          LOG_WARN("failed to fetch max ls id", KR(ret), K(tenant_id));
        }
      }
    }
    if (FAILEDx(gather_all_ls_info_())) {
      LOG_WARN("failed to get all ls info", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
                   *tenant_schema_, primary_zone_))) {
      LOG_WARN("failed to get tenant primary zone array", KR(ret), KPC(tenant_schema_));
    } else {
      is_load_ = true;
    }
  }
  LOG_INFO("[LS_MGR] gather stat", KR(ret), K(primary_zone_), K(unit_group_array_),
      K(max_ls_id_), K(max_ls_group_id_));
  return ret;
}

int ObTenantLSInfo::gather_all_ls_info_()
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    const int64_t count = max(1, hash::cal_next_prime(status_info_array.count()));
    if (OB_FAIL(status_map_.create(count, "LogStrInfo", "LogStrInfo"))) {
      LOG_WARN("failed to create ls map", KR(ret), K(count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        const share::ObLSStatusInfo &info = status_info_array.at(i);
        if (info.ls_is_wait_offline()) {
          //ls is already offline, no need to process
        } else if (FAILEDx(add_ls_status_info_(info))) {
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

int ObTenantLSInfo::check_unit_group_valid_(
    const uint64_t unit_group_id, bool &is_valid_info)
{
  int ret = OB_SUCCESS;
  is_valid_info = true;
  if (OB_UNLIKELY(OB_INVALID_VERSION == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(unit_group_id));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else {
    //check unit group status is valid
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_FAIL(ret) && i < unit_group_array_.count(); ++i) {
      const ObUnitGroupInfo &group_info = unit_group_array_.at(i);
      if (unit_group_id == group_info.unit_group_id_) {
        ret = OB_SUCCESS;
        if (share::ObUnit::UNIT_STATUS_ACTIVE != group_info.unit_status_) {
          is_valid_info = false;
        }
        break;
      }
    }//end for
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to found unit group status", KR(ret), K(unit_group_id),
               K(unit_group_array_));
    }
  }
  return ret;
}

//for standby
int ObTenantLSInfo::create_new_ls_for_recovery(
    const share::ObLSID &ls_id,
    const uint64_t ls_group_id,
    const SCN &create_scn,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const bool is_recovery = true;
  int64_t info_index = OB_INVALID_INDEX_INT64;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || OB_INVALID_ID == ls_group_id
                  || !create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id), K(ls_group_id), K(create_scn));
  } else if (OB_UNLIKELY(!is_valid())
             || OB_ISNULL(tenant_schema_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat not valid", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    share::ObLSStatusInfo new_info;
    ObLSGroupInfo group_info;
    ObZone primary_zone;
    ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    ObSqlString zone_priority;
    if (OB_SUCC(get_ls_group_info(
            ls_group_id, group_info))) {
      // need a new primary zone
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // need a new unit group
      ret = OB_SUCCESS;
      int64_t unit_group_index = OB_INVALID_INDEX_INT64;
      if (OB_FAIL(get_next_unit_group_(is_recovery, unit_group_index))) {
        LOG_WARN("failed to get next unit group", KR(ret), K(is_recovery));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 == unit_group_index
                             || unit_group_index >= unit_group_array_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to next unit group", KR(ret), K(unit_group_index));
      } else if (OB_FAIL(group_info.init(
                     unit_group_array_.at(unit_group_index).unit_group_id_,
                     ls_group_id))) {
        LOG_WARN("failed to init group info", KR(ret), K(unit_group_index), K(ls_group_id));
      }
    } else {
      LOG_WARN("failed to get ls group info", KR(ret),
               K(ls_group_id));
    }

    if (FAILEDx(get_next_primary_zone_(is_recovery, group_info, primary_zone))) {
      LOG_WARN("failed to get next primary zone", KR(ret), K(group_info));
    } else if (OB_FAIL(new_info.init(tenant_id, ls_id,
                              ls_group_id,
                              share::OB_LS_CREATING,
                              group_info.unit_group_id_, primary_zone))) {
      LOG_WARN("failed to init new info", KR(ret), K(tenant_id),
               K(ls_id), K(ls_group_id), K(group_info), K(primary_zone));
    } else if (OB_FAIL(get_zone_priority(primary_zone, *tenant_schema_, zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(primary_zone), K(zone_priority));
    } else if (OB_FAIL(ls_life_agent.create_new_ls_in_trans(new_info, create_scn,
            zone_priority.string(), share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to insert ls info", KR(ret), K(new_info), K(create_scn), K(zone_priority));
    } else if (OB_FAIL(add_ls_status_info_(new_info))) {
      LOG_WARN("failed to add new ls info to memory", KR(ret), K(new_info));
    }
  }
  return ret;
}

int ObTenantLSInfo::process_ls_stats_for_recovery()
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  ObLSRecoveryStat recovery_stat;
  ObLSRecoveryStatOperator ls_recovery_operator;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
            tenant_id, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &status_info = status_info_array.at(i);
      recovery_stat.reset();
      if (status_info.ls_is_creating()) {
        if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, status_info.ls_id_,
                false/*for_update*/, recovery_stat, *sql_proxy_))) {
          LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id), K(status_info));
        } else if (OB_FAIL(do_create_ls_(status_info, recovery_stat.get_create_scn()))) {
          LOG_WARN("failed to create new ls", KR(ret), K(status_info),
              K(recovery_stat));
        }
      }
    }//end for
    //adjust primary zone
    if (OB_SUCCESS != (tmp_ret = adjust_user_tenant_primary_zone())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to adjust user tenant primary zone", KR(ret),
          KR(tmp_ret), K(tenant_schema_));
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

int ObTenantLSInfo::get_next_unit_group_(const bool is_recovery, int64_t &group_index)
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
      if (OB_INVALID_INDEX_INT64 == group_index
          || (!is_recovery && 0 != ls_count)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to find next unit group", KR(ret),
                 K(unit_group_array_), K(is_recovery), K(ls_count));
      }
    }
  }
  LOG_INFO("get next primary zone", KR(ret), K(group_index), K(is_recovery));
  return ret;
}

int ObTenantLSInfo::get_next_primary_zone_(
  const bool is_recovery,
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
      if (primary_zone.is_empty() || (!is_recovery && 0 != ls_count)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("failed to find next primary zone", KR(ret), K(primary_zone_),
                 K(group_info), K(is_recovery), K(ls_count));
      }
    }
    LOG_INFO("get next primary zone", KR(ret), K(group_info), K(primary_zone));
  }
  return ret;
}

//need create or delete new ls group
int ObTenantLSInfo::check_ls_group_match_unitnum()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant stat not valid", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array_.count(); ++i) {
    const ObUnitGroupInfo &info = unit_group_array_.at(i);
    if (share::ObUnit::UNIT_STATUS_MAX == info.unit_status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit status not valid", KR(ret), K(info));
    } else if (share::ObUnit::UNIT_STATUS_DELETING == info.unit_status_) {
      // drop logstream,
      if (0 == info.ls_group_ids_.count()) {
        //nothing
      } else if (OB_FAIL(try_drop_ls_of_deleting_unit_group_(info))) {
        LOG_WARN("failed to drop ls of the unit group", KR(ret), K(info));
      }
    } else if (0 == info.ls_group_ids_.count()) {
      //create ls
      if (OB_FAIL(create_new_ls_for_empty_unit_group_(info.unit_group_id_))) {
        LOG_WARN("failed to create new ls for empty unit_group", KR(ret),
                 K(info));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::create_new_ls_for_empty_unit_group_(const uint64_t unit_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_INDEX_INT64 == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("group id is invalid", KR(ret), K(unit_group_id));
  } else if (OB_UNLIKELY(!is_valid())
             || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant ls info not valid", KR(ret), KP(tenant_schema_));
  } else if (OB_UNLIKELY(0 == primary_zone_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary zone is invalid", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    share::ObLSID new_id;
    share::ObLSStatusInfo new_info;
    uint64_t new_ls_group_id = OB_INVALID_INDEX_INT64;
    if (OB_FAIL(fetch_new_ls_group_id(tenant_id, new_ls_group_id))) {
      LOG_WARN("failed to fetch new id", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_.count(); ++i) {
      const ObZone &zone = primary_zone_.at(i);
      new_info.reset();
      if (OB_FAIL(fetch_new_ls_id(tenant_id, new_id))) {
        LOG_WARN("failed to get new id", KR(ret), K(tenant_id));
      } else if (OB_FAIL(new_info.init(tenant_id, new_id, new_ls_group_id,
                                       share::OB_LS_CREATING, unit_group_id,
                                       zone))) {
        LOG_WARN("failed to init new info", KR(ret), K(new_id),
                 K(new_ls_group_id), K(unit_group_id), K(zone), K(tenant_id));
      } else if (OB_FAIL(create_new_ls_(new_info, share::NORMAL_SWITCHOVER_STATUS))) {
        LOG_WARN("failed to add ls info", KR(ret), K(new_info));
      }
      LOG_INFO("[LS_MGR] create new ls for empty unit group", KR(ret), K(new_info));
    }//end for
  }
  return ret;
}

int ObTenantLSInfo::try_drop_ls_of_deleting_unit_group_(const ObUnitGroupInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(share::ObUnit::UNIT_STATUS_DELETING != info.unit_status_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group status is invalid", KR(ret), K(info));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant ls info not valid", KR(ret));
  } else {
    ARRAY_FOREACH_X(info.ls_group_ids_, idx, cnt, OB_SUCC(ret)) {
      ObLSGroupInfo ls_group_info;
      const int64_t ls_group_id = info.ls_group_ids_.at(idx);
      if (OB_FAIL(get_ls_group_info(ls_group_id, ls_group_info))) {
        LOG_WARN("failed to get ls group info", KR(ret), K(idx),
                 K(ls_group_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_group_info.ls_ids_.count(); ++i) {
          const share::ObLSID &ls_id = ls_group_info.ls_ids_.at(i);
          share::ObLSStatusInfo info;
          int64_t info_index = 0;
          if (OB_FAIL(get_ls_status_info(ls_id, info, info_index))) {
            LOG_WARN("failed to get ls status info", KR(ret), K(ls_id));
          } else if (OB_FAIL(drop_ls_(info, share::NORMAL_SWITCHOVER_STATUS))) {
            LOG_WARN("failed to drop ls", KR(ret), K(info));
          }
          LOG_INFO("[LS_MGR] drop ls for deleting unit group", KR(ret),
                   K(ls_id), K(info));
        }//end for each ls in ls group info
      }
    }//end for each ls group in unit group info
  }
  return ret;
}

int ObTenantLSInfo::drop_ls_(const share::ObLSStatusInfo &info,
                             const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info is invalid", KR(ret), K(info));
  } else if (info.ls_is_normal()) {
    //try to set ls to dropping
    if (OB_FAIL(ls_operator_.update_ls_status(info.ls_id_,
            share::OB_LS_NORMAL, share::OB_LS_DROPPING, working_sw_status))) {
      LOG_WARN("failed to update ls status", KR(ret), K(info));
    }
  } else if (info.ls_is_created()) {
    //if ls_status is created, the ls_info may creating or normal
    //creating can not to dropping, so need retry after fix_status_machine.
    ret = OB_NEED_RETRY;
    LOG_WARN("not certain status in ls info, need retry", KR(ret), K(info));
  } else if (info.ls_is_dropping()
             || info.ls_is_wait_offline()) {
    //nothing
  } else if (info.ls_is_creating()) {
    //if status is in creating, the ls must in creating too
    if (OB_FAIL(ls_operator_.delete_ls(info.ls_id_, share::OB_LS_CREATING, working_sw_status))) {
      LOG_WARN("failed to process creating info", KR(ret), K(info));
    }
  } else {
    //other ls status not expected
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls status not expected for dropping ls", KR(ret), K(info));
  }
  return ret;
}

//need create or delete new ls
//TODO for primary cluster, The primary_zone of multiple LSs may be the same,
//the redundant ones need to be deleted, and only one is kept.
int ObTenantLSInfo::check_ls_match_primary_zone()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid()) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant ls info not valid", KR(ret), KP(tenant_schema_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    ObZone zone;
    share::ObLSID new_id;
    share::ObLSStatusInfo new_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_group_array_.count(); ++i) {
      ObLSGroupInfo &group_info = ls_group_array_.at(i);
      //check the unit group is active
      //if unit group is in deleting, no need check primary zone
      bool is_valid = false;
      int64_t group_id_count = group_info.ls_ids_.count();
      if (OB_FAIL(check_unit_group_valid_(group_info.unit_group_id_, is_valid))) {
        LOG_WARN("failed to check unit group valid", KR(ret), K(group_info));
      }
      while (OB_SUCC(ret) && is_valid
             && primary_zone_.count() > group_id_count) {
        group_id_count++;
        if (OB_FAIL(get_next_primary_zone_(false/*is_recovery*/, group_info, zone))) {
          LOG_WARN("failed to get next primary zone", K(group_info));
        } else if (OB_FAIL(fetch_new_ls_id(tenant_id, new_id))) {
          LOG_WARN("failed to get new id", KR(ret), K(tenant_id));
        } else if (OB_FAIL(new_info.init(tenant_id, new_id, group_info.ls_group_id_,
                                         share::OB_LS_CREATING,
                                         group_info.unit_group_id_, zone))) {
          LOG_WARN("failed to init new info", KR(ret), K(new_id),
                   K(group_info), K(zone), K(tenant_id));
        } else if (OB_FAIL(create_new_ls_(new_info, share::NORMAL_SWITCHOVER_STATUS))) {
          LOG_WARN("failed to create new ls", KR(ret), K(new_info));
        }
        LOG_INFO("[LS_MGR] create new ls of primary zone", KR(ret), K(new_info),
            K(group_id_count));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::get_zone_priority(const ObZone &primary_zone,
                                 const share::schema::ObTenantSchema &tenant_schema,
                                 ObSqlString &primary_zone_str)
{
  int ret = OB_SUCCESS;
  primary_zone_str.reset();
  if (OB_UNLIKELY(!tenant_schema.is_valid() || primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone), K(tenant_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_ls_primary_zone_priority(primary_zone,
          tenant_schema, primary_zone_str))) {
    LOG_WARN("failed to get ls primary zone priority", KR(ret), K(primary_zone), K(tenant_schema));
  }
  LOG_DEBUG("get zone priority", KR(ret), K(primary_zone_str), K(tenant_schema));
  return ret;
}


int ObTenantLSInfo::create_new_ls_(const share::ObLSStatusInfo &status_info,
                                   const share::ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant stat not valid", KR(ret));
  } else if (OB_UNLIKELY(!status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info is invalid", KR(ret), K(status_info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    share::ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    share::ObLSAttr ls_info;
    share::ObLSFlag flag = share::OB_LS_FLAG_NORMAL;//TODO
    SCN create_scn;
    ObSqlString zone_priority;
    if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(status_info.tenant_id_, create_scn))) {
      LOG_WARN("failed to get tenant gts", KR(ret), K(status_info));
    } else if (OB_FAIL(ls_info.init(status_info.ls_id_, status_info.ls_group_id_, flag,
                             share::OB_LS_CREATING, share::OB_LS_OP_CREATE_PRE, create_scn))) {
      LOG_WARN("failed to init new operation", KR(ret), K(status_info), K(create_scn));
    } else if (OB_FAIL(ls_operator_.insert_ls(ls_info, max_ls_group_id_, working_sw_status))) {
      LOG_WARN("failed to insert new operation", KR(ret), K(ls_info), K(max_ls_group_id_));
    } else if (OB_FAIL(get_zone_priority(status_info.primary_zone_,
            *tenant_schema_, zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(status_info),
          K(zone_priority), K(tenant_schema_));
    } else if (OB_FAIL(ls_life_agent.create_new_ls(status_info, create_scn,
            zone_priority.string(), working_sw_status))) {
      LOG_WARN("failed to create new ls", KR(ret), K(status_info), K(create_scn),
          K(zone_priority));
    } else if (OB_FAIL(do_create_ls_(status_info, create_scn))) {
      LOG_WARN("failed to create ls", KR(ret), K(status_info), K(create_scn));
    } else if (OB_FAIL(process_ls_status_after_created_(status_info, working_sw_status))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info));
    }
    LOG_INFO("[LS_MGR] create new ls", KR(ret), K(status_info));
  }
  return ret;
}

int ObTenantLSInfo::fetch_new_ls_group_id(
     const uint64_t tenant_id,
     uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ls_group_id = OB_INVALID_ID;
    share::ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        tenant_id, share::OB_MAX_USED_LS_GROUP_ID_TYPE, ls_group_id))) {
      LOG_WARN("fetch_max_id failed", KR(ret), K(tenant_id), "id_type",
               share::OB_MAX_USED_LS_GROUP_ID_TYPE);
    } else {
      if (ls_group_id != ++max_ls_group_id_) {
        ret = OB_EAGAIN;
        LOG_WARN("ls may create concurrency, need retry", KR(ret), K(ls_group_id),
            K(max_ls_group_id_));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::fetch_new_ls_id(const uint64_t tenant_id, share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    share::ObMaxIdFetcher id_fetcher(*sql_proxy_);
    uint64_t id_value = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        tenant_id, share::OB_MAX_USED_LS_ID_TYPE, id_value))) {
      LOG_WARN("fetch_max_id failed", KR(ret), K(tenant_id), "id_type",
               share::OB_MAX_USED_LS_ID_TYPE);
    } else {
      if (id_value != ++max_ls_id_) {
        ret = OB_EAGAIN;
        LOG_WARN("ls may create concurrency, need retry", KR(ret), K(id_value),
            K(max_ls_id_));
      } else {
        share::ObLSID new_id(id_value);
        id = new_id;
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::create_ls_with_palf(
    const share::ObLSStatusInfo &info,
    const SCN &create_scn,
    const bool create_ls_with_palf,
    const palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LS_EXEC] start to create ls", K(info));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!info.ls_is_created() && !info.ls_is_creating())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info status not expected", KR(ret), K(info));
  } else if (info.ls_is_creating()) {
    common::ObArray<share::ObZoneReplicaAttrSet> locality_array;
    int64_t paxos_replica_num = 0;
    ObSchemaGetterGuard guard;//nothing
    if (OB_ISNULL(tenant_schema_)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(info));
    } else if (tenant_schema_->is_dropping()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant is in dropping, no need create ls", KR(ret), K(tenant_schema_));
    } else if (OB_FAIL(tenant_schema_->get_zone_replica_attr_array(
                   locality_array))) {
      LOG_WARN("failed to get zone locality array", KR(ret));
    } else if (OB_FAIL(tenant_schema_->get_paxos_replica_num(
                   guard, paxos_replica_num))) {
      LOG_WARN("failed to get paxos replica num", KR(ret));
    } else {
      ObLSCreator creator(*rpc_proxy_, info.tenant_id_,
                          info.ls_id_, sql_proxy_);
      if (OB_FAIL(creator.create_user_ls(info, paxos_replica_num,
                                         locality_array, create_scn,
                                         tenant_schema_->get_compatibility_mode(),
                                         create_ls_with_palf,
                                         palf_base_info))) {
        LOG_WARN("failed to create user ls", KR(ret), K(info), K(locality_array), K(create_scn),
                                             K(palf_base_info), K(create_ls_with_palf));
      }
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[LS_EXEC] end to create ls", KR(ret), K(info), K(cost));
  return ret;
}

int ObTenantLSInfo::do_create_ls_(const share::ObLSStatusInfo &info,
                                  const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else {
    palf::PalfBaseInfo palf_base_info;
    if (OB_FAIL(create_ls_with_palf(info, create_scn, false, palf_base_info))) {
      LOG_WARN("failed to create ls with palf", KR(ret), K(info), K(create_scn), K(palf_base_info));
    }
  }
  return ret;
}

int ObTenantLSInfo::process_ls_status_after_created_(const share::ObLSStatusInfo &status_info,
                                                     const ObTenantSwitchoverStatus &working_sw_status)
{
  //do not check ls status
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(status_info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ls_operator_.update_ls_status(
                 status_info.ls_id_, share::OB_LS_CREATING, share::OB_LS_NORMAL, working_sw_status))) {
    LOG_WARN("failed to update ls status", KR(ret), K(status_info));
  } else if (OB_FAIL(status_operator_.update_ls_status(
                 status_info.tenant_id_, status_info.ls_id_, share::OB_LS_CREATED,
                 share::OB_LS_NORMAL, working_sw_status, *sql_proxy_))) {
    LOG_WARN("failed to update ls status", KR(ret), K(status_info));
  }
  return ret;
}

int ObTenantLSInfo::do_tenant_drop_ls_(const share::ObLSStatusInfo &status_info,
                                       const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LS_EXEC] start to tenant drop ls", K(status_info));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  bool can_offline = false;
  if (OB_UNLIKELY(!status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(status_info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_UNLIKELY(status_info.ls_is_creating() || status_info.ls_is_wait_offline())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status info not expected", KR(ret), K(status_info));
  } else if (!status_info.ls_is_tenant_dropping()) {
    //status_info may in created, normal, dropping, tenant_dropping
    if (OB_FAIL(status_operator_.update_ls_status(
              status_info.tenant_id_, status_info.ls_id_,
              status_info.status_, share::OB_LS_TENANT_DROPPING, working_sw_status,
              *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info));
    }
  }
  if (FAILEDx(check_ls_can_offline_by_rpc_(status_info, share::OB_LS_TENANT_DROPPING, can_offline))) {
    // send rpc to observer
    LOG_WARN("failed to check ls can offline", KR(ret), K(status_info));
  } else if (can_offline) {
    ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    SCN drop_scn;
    if (!status_info.ls_id_.is_sys_ls()) {
      //sys ls cannot delete ls, after ls is in tenant dropping
      if (OB_FAIL(ls_operator_.delete_ls(status_info.ls_id_, share::OB_LS_TENANT_DROPPING,
                                         working_sw_status))) {
        LOG_WARN("failed to delete ls", KR(ret), K(status_info));
      } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(status_info.tenant_id_, drop_scn))) {
        LOG_WARN("failed to get gts", KR(ret), K(status_info));
      }
    } else {
      //TODO sys ls can not get GTS after tenant_dropping
      drop_scn.set_base();
    }
    if (FAILEDx(ls_life_agent.set_ls_offline(status_info.tenant_id_,
            status_info.ls_id_, status_info.status_, drop_scn, working_sw_status))) {
      LOG_WARN("failed to update ls info", KR(ret), K(status_info), K(drop_scn));
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[LS_EXEC] end to tenant drop ls", KR(ret), K(status_info), K(cost));
  return ret;
}

int ObTenantLSInfo::do_drop_ls_(const share::ObLSStatusInfo &status_info,
                                const ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LS_EXEC] start to drop ls", K(status_info));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  bool tablet_empty = false;
  if (OB_UNLIKELY(!status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(status_info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (status_info.ls_is_normal()) {
    if (OB_FAIL(status_operator_.update_ls_status(
            status_info.tenant_id_, status_info.ls_id_,
            status_info.status_, share::OB_LS_DROPPING,
            working_sw_status,
            *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(status_info));
    }
  } else if (status_info.ls_is_dropping()) {
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status not expected", KR(ret), K(status_info));
  }
  if (FAILEDx(check_ls_empty_(status_info, tablet_empty))) {
    LOG_WARN("failed to check ls empty", KR(ret), K(status_info));
  }
  if (OB_SUCC(ret) && tablet_empty) {
    // send rpc to observer
    bool can_offline = false;
    if (OB_FAIL(check_ls_can_offline_by_rpc_(status_info, share::OB_LS_DROPPING, can_offline))) {
      LOG_WARN("failed to check ls can offline", KR(ret), K(status_info));
    } else if (can_offline) {
      ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
      SCN drop_scn;
      if (OB_FAIL(ls_operator_.delete_ls(status_info.ls_id_, share::OB_LS_DROPPING,
                                         working_sw_status))) {
        LOG_WARN("failed to delete ls", KR(ret), K(status_info));
      } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(status_info.tenant_id_, drop_scn))) {
        LOG_WARN("failed to get gts", KR(ret), K(status_info));
      } else if (OB_FAIL(ls_life_agent.set_ls_offline(status_info.tenant_id_,
              status_info.ls_id_, status_info.status_, drop_scn, working_sw_status))) {
        LOG_WARN("failed to update ls info", KR(ret), K(status_info), K(drop_scn));
      }
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[LS_EXEC] end to drop ls", KR(ret), K(status_info), K(cost), K(tablet_empty));
  return ret;
}


int ObTenantLSInfo::sys_ls_tenant_drop_(const share::ObLSStatusInfo &info,
                                        const share::ObTenantSwitchoverStatus &working_sw_status)
{
  int ret = OB_SUCCESS;
  const ObLSStatus target_status = share::OB_LS_TENANT_DROPPING;
  const ObLSStatus pre_status = share::OB_LS_PRE_TENANT_DROPPING;
  if (OB_UNLIKELY(!info.is_valid()
                  || !info.ls_id_.is_sys_ls())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (info.ls_is_normal()) {
    if (OB_FAIL(status_operator_.update_ls_status(info.tenant_id_,
            info.ls_id_, info.status_, pre_status, working_sw_status,
            *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(info), K(pre_status));
    }
  } else if (pre_status == info.status_) {
    //nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls can not in other status", KR(ret), K(info));
  }
  bool can_offline = false;
  if (FAILEDx(check_sys_ls_can_offline_(can_offline))) {
    LOG_WARN("failed to check sys ls can offline", KR(ret));
  } else if (can_offline) {
    if (OB_FAIL(ls_operator_.update_ls_status(info.ls_id_, pre_status, target_status,
                                              working_sw_status))) {
      LOG_WARN("failed to update ls status", KR(ret), K(info), K(pre_status), K(target_status));
    } else if (OB_FAIL(status_operator_.update_ls_status(info.tenant_id_,
            info.ls_id_, pre_status, target_status, working_sw_status, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(info), K(pre_status), K(target_status));
    }
  }
  return ret;
}

int ObTenantLSInfo::check_sys_ls_can_offline_(bool &can_offline)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray status_info_array;
  can_offline = true;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id));
    } else if (0 == status_info_array.count()) {
      //if has multi ls_mgr
      can_offline = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count() && can_offline; ++i) {
      const share::ObLSStatusInfo &status_info = status_info_array.at(i);
      if (status_info.ls_id_.is_sys_ls()) {
      } else if (status_info.ls_is_wait_offline()) {
        //nothing
      } else {
        can_offline = false;
        LOG_INFO("[LS_MGR] sys ls can not offline", K(status_info));
        break;
      }
    }
    if (OB_SUCC(ret) && can_offline) {
      LOG_INFO("[LS_MGR] sys ls can offline", K(status_info_array));
    }
  }
  return ret;
}

int ObTenantLSInfo::check_ls_empty_(const share::ObLSStatusInfo &info, bool &empty)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "SELECT * FROM %s where ls_id = %ld",
                   OB_ALL_TABLET_TO_LS_TNAME, info.ls_id_.id()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, info.tenant_id_, sql.ptr()))) {
          LOG_WARN("failed to read", KR(ret), K(info), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret));
        } else if (OB_SUCC(result->next())) {
          empty = false;
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          empty = true;
        } else {
          LOG_WARN("failed to get next", KR(ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::check_ls_can_offline_by_rpc_(const share::ObLSStatusInfo &info,
    const share::ObLSStatus &current_ls_status, bool &can_offline)
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  const share::ObLSReplica *replica = NULL;
  if (OB_UNLIKELY(!info.is_valid()
        || !ls_is_tenant_dropping_status(current_ls_status) && ! ls_is_dropping_status(current_ls_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info), K(current_ls_status));
  } else if (OB_ISNULL(lst_operator_) || OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst operator or proxy is null", KR(ret), KP(lst_operator_),
             KP(rpc_proxy_));
  } else if (OB_FAIL(lst_operator_->get(GCONF.cluster_id, info.tenant_id_,
             info.ls_id_, share::ObLSTable::DEFAULT_MODE, ls_info))) {
    LOG_WARN("failed to get ls info", KR(ret), K(info));
  } else if (OB_FAIL(ls_info.find_leader(replica))) {
    LOG_WARN("failed to find leader", KR(ret), K(ls_info));
  } else if (OB_ISNULL(replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica is null", KR(ret), K(ls_info));
  } else {
    const int64_t timeout = GCONF.rpc_timeout;
    obrpc::ObCheckLSCanOfflineArg arg;
    if (OB_FAIL(arg.init(info.tenant_id_, info.ls_id_, current_ls_status))) {
      LOG_WARN("failed to init arg", KR(ret), K(info), K(current_ls_status));
    } else if (OB_FAIL(rpc_proxy_->to(replica->get_server())
                           .by(info.tenant_id_)
                           .timeout(timeout)
                           .check_ls_can_offline(arg))) {
      can_offline = false;
      LOG_WARN("failed to check ls can offline", KR(ret), K(arg), K(info),
               K(timeout), K(replica));
    } else {
      can_offline = true;
    }
  }
  return ret;
}
int ObTenantLSInfo::adjust_user_tenant_primary_zone()
{
  int ret = OB_SUCCESS;
  share::ObLSStatusOperator status_op;
  share::ObLSPrimaryZoneInfoArray info_array;
  ObArray<common::ObZone> primary_zone;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(sql_proxy_), KP(tenant_schema_));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
                   *tenant_schema_, primary_zone))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret), KPC(tenant_schema_));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(status_op.get_tenant_primary_zone_info_array(
          tenant_id, info_array, *sql_proxy_))) {
      LOG_WARN("failed to get tenant primary zone info array", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t last_ls_group_id = OB_INVALID_ID;
    share::ObLSPrimaryZoneInfoArray tmp_info_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_array.count(); ++i) {
      const ObLSPrimaryZoneInfo &info = info_array.at(i);
      if (OB_INVALID_ID == last_ls_group_id) {
        last_ls_group_id = info.get_ls_group_id();
      }
      if (last_ls_group_id != info.get_ls_group_id()) {
        //process the ls group
        if (OB_FAIL(adjust_primary_zone_by_ls_group_(primary_zone, tmp_info_array, *tenant_schema_))) {
          LOG_WARN("failed to update primary zone of each ls group", KR(ret),
              K(tmp_info_array), K(primary_zone), KPC(tenant_schema_));
        } else {
          tmp_info_array.reset();
          last_ls_group_id = info.get_ls_group_id();
        }
      }
      if (FAILEDx(tmp_info_array.push_back(info))) {
        LOG_WARN("failed to push back primary info array", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && 0 < tmp_info_array.count()) {
      if (OB_FAIL(adjust_primary_zone_by_ls_group_(primary_zone, tmp_info_array, *tenant_schema_))) {
        LOG_WARN("failed to update primary zone of each ls group", KR(ret),
            K(tmp_info_array), K(primary_zone), KPC(tenant_schema_));
      }
    }
  }

  return ret;
}


int ObTenantLSInfo::try_update_ls_primary_zone_(
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!primary_zone_info.is_valid()
        || new_primary_zone.is_empty() || zone_priority.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("primary zone info is invalid", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(zone_priority));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (new_primary_zone != primary_zone_info.get_primary_zone()
      || zone_priority.string() != primary_zone_info.get_zone_priority_str()) {
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    ObLSInfo ls_info;
    const uint64_t tenant_id = primary_zone_info.get_tenant_id();
    const ObLSID ls_id = primary_zone_info.get_ls_id();
    if (OB_FAIL(ls_life_agent.update_ls_primary_zone(tenant_id, ls_id,
            new_primary_zone, zone_priority.string()))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
          K(new_primary_zone), K(zone_priority));
    } else if (OB_ISNULL(GCTX.lst_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.lst_operator_ is NULL", KR(ret));
    } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id,
            tenant_id, ls_id, share::ObLSTable::DEFAULT_MODE, ls_info))) {
      LOG_WARN("get ls info from GCTX.lst_operator_ failed", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_TMP_FAIL(ObRootUtils::try_notify_switch_ls_leader(GCTX.srv_rpc_proxy_, ls_info,
            obrpc::ObNotifySwitchLeaderArg::MODIFY_PRIMARY_ZONE))) {
      LOG_WARN("failed to switch ls leader", KR(ret), KR(tmp_ret), K(ls_id), K(ls_info));
    }
    LOG_INFO("[LS_MGR] update ls primary zone", KR(ret), K(new_primary_zone),
        K(zone_priority), K(primary_zone_info));
  } else {
    //no need update
  }
  return ret;
}

//check every ls has right primary zone in ls group
//if primary_zone of ls not in primary zone, try to choose a right zone,
//if can not find a zone to modify, modify the ls to exist zone,
//load_balancer will choose the right ls to drop
//eg:
//if ls group has z1, z2 and primary zone is : z1. need modify z2 to z1
//if ls group has z1, z2 and primary zone is : z1, z3. modify z2 to z3;
int ObTenantLSInfo::adjust_primary_zone_by_ls_group_(
    const common::ObIArray<common::ObZone> &primary_zone_array,
    const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
    const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t ls_count = primary_zone_infos.count();
  const int64_t primary_zone_count = primary_zone_array.count();
  if (OB_UNLIKELY(0 == primary_zone_count
        || 0 == ls_count || !tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone_infos),
        K(primary_zone_array), K(tenant_schema));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (1 == ls_count
      && primary_zone_infos.at(0).get_ls_id().is_sys_ls()) {
    //user sys ls is equal to meta sys ls
    share::ObLSStatusOperator status_op;
    share::ObLSPrimaryZoneInfo meta_primary_zone;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(status_op.get_ls_primary_zone_info(meta_tenant_id,
            SYS_LS, meta_primary_zone, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(try_update_ls_primary_zone_(primary_zone_infos.at(0),
            meta_primary_zone.get_primary_zone(), meta_primary_zone.get_zone_priority()))) {
      LOG_WARN("failed to update primary zone", KR(ret), K(primary_zone_infos), K(meta_primary_zone));
    }
  } else {
    //Algorithm Description:
    //Assumption: We have 5 ls, 3 primary_zones(z1, z2, z3).
    //1. Set the primary zone of ls to  tenant's primary zone,
    //choose the least number of log streams on the zone is selected for all zones
    //2. After all the primary_zone of the ls are in the primary_zonw of the tenant,
    //choose the primary_zone with the most and least ls. Adjust a certain number of ls to the smallest zone without exceeding the balance
    //while guaranteeing that the number of the zone with the most is no less than the average.
    ObArray<ObZone> ls_primary_zone;//is match with primary_zone_infos
    ObSEArray<uint64_t, 3> count_group_by_zone;//ls count of each primary zone
    ObSqlString new_zone_priority;
    if (OB_FAIL(set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone,
            count_group_by_zone))) {
      LOG_WARN("failed to set ls to primary zone", KR(ret), K(primary_zone_array), K(primary_zone_infos));
    } else if (OB_FAIL(balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone))) {
      LOG_WARN("failed to balance ls primary zone", KR(ret), K(ls_primary_zone), K(count_group_by_zone));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &new_primary_zone = ls_primary_zone.at(i);
      if (OB_FAIL(ObTenantLSInfo::get_zone_priority(new_primary_zone, tenant_schema, new_zone_priority))) {
        LOG_WARN("failed to get normalize primary zone", KR(ret), K(new_primary_zone));
      }
      if (FAILEDx(try_update_ls_primary_zone_(primary_zone_infos.at(i), new_primary_zone, new_zone_priority))) {
        LOG_WARN("failed to update ls primary zone", KR(ret), "primary_zone_info", primary_zone_infos.at(i),
            K(new_primary_zone), K(new_zone_priority));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::set_ls_to_primary_zone(
    const common::ObIArray<ObZone> &primary_zone_array,
    const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
    common::ObIArray<common::ObZone> &ls_primary_zone,
    common::ObIArray<uint64_t> &count_group_by_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == primary_zone_array.count()
        || 0 == primary_zone_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone_array), K(primary_zone_infos));
  } else {
    const int64_t primary_zone_count = primary_zone_array.count();
    const int64_t ls_count = primary_zone_infos.count();
    //ls may not in primary zone, record the index of primary_zone_infos not in primary zone
    ObSEArray<int64_t, 3> index_not_primary_zone;
    int64_t index = 0;
    ARRAY_FOREACH_X(primary_zone_array, idx, cnt, OB_SUCC(ret)) {
      if (OB_FAIL(count_group_by_zone.push_back(0))) {
        LOG_WARN("failed to push back", KR(ret), K(idx));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &current_zone = primary_zone_infos.at(i).get_primary_zone();
      if (has_exist_in_array(primary_zone_array, current_zone, &index)) {
        count_group_by_zone.at(index)++;
      } else if (OB_FAIL(index_not_primary_zone.push_back(i))) {
        LOG_WARN("failed to push back", KR(ret), K(i), K(current_zone));
      }
      if (FAILEDx(ls_primary_zone.push_back(current_zone))) {
        LOG_WARN("failed to push back current zone", KR(ret), K(i), K(current_zone));
      }
    }
    //1. take all ls primary zone to tenant primary zone, choose the less primary zone count
    int64_t min_count = INT64_MAX;
    int64_t min_index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_not_primary_zone.count(); ++i) {
      const int64_t ls_index = index_not_primary_zone.at(i);
      min_count = INT64_MAX;
      ARRAY_FOREACH_X(primary_zone_array, idx, cnt, OB_SUCC(ret)) {
        if (min_count > count_group_by_zone.at(idx)) {
          min_count = count_group_by_zone.at(idx);
          min_index = idx;
        }
      }//end for search min count
      if (OB_FAIL(ret)) {
      } else if (min_index >= primary_zone_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to found min count", KR(ret), K(min_index), K(primary_zone_array));
      } else if (OB_FAIL(ls_primary_zone.at(ls_index).assign(primary_zone_array.at(min_index)))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(min_index), K(min_count), K(ls_index));
      } else {
        count_group_by_zone.at(min_index)++;
      }
    }
  }
  return ret;

}

int ObTenantLSInfo::balance_ls_primary_zone(
    const common::ObIArray<common::ObZone> &primary_zone_array,
    common::ObIArray<common::ObZone> &ls_primary_zone,
    common::ObIArray<uint64_t> &count_group_by_zone)
{
  int ret = OB_SUCCESS;
  const int64_t ls_count = ls_primary_zone.count();
  const int64_t primary_zone_count = count_group_by_zone.count();
  if (OB_UNLIKELY(0 == primary_zone_count
        || 0 == ls_count
        || primary_zone_count != primary_zone_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(count_group_by_zone), K(ls_primary_zone));
  } else {
    int64_t max_count = -1, max_index = 0;
    int64_t min_count = INT64_MAX, min_index = 0;
    const int64_t each_ls_max_count = ceil((double)(ls_count)/primary_zone_count);
    do {
      max_count = -1, max_index = 0;
      min_count = INT64_MAX, min_index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_count; ++i) {
        const int64_t ls_count = count_group_by_zone.at(i);
        if (min_count > ls_count) {
          min_count = ls_count;
          min_index = i;
        }
        if (max_count < ls_count) {
          max_count = ls_count;
          max_index = i;
        }
      }//end for find min and max count
      if (OB_UNLIKELY(max_index >= primary_zone_count || min_index >= primary_zone_count
            || -1 == max_count || INT64_MAX == min_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get primary zone", KR(ret), K(max_index), K(min_index),
            K(min_count), K(max_count),
            K(primary_zone_array), K(primary_zone_count), K(ls_primary_zone));
      } else if (max_count - min_count > 1) {
        //choose the max count to min count
        int64_t need_change = min(each_ls_max_count - min_count, max_count - each_ls_max_count);
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_count && need_change > 0; ++i) {
          if (ls_primary_zone.at(i) == primary_zone_array.at(max_index)) {
            if (OB_FAIL(ls_primary_zone.at(i).assign(primary_zone_array.at(min_index)))) {
              LOG_WARN("failed to push back ls primary zone", KR(ret), K(min_index));
            } else {
              need_change--;
              count_group_by_zone.at(max_index)--;
              count_group_by_zone.at(min_index)++;
            }
          }
        }
      }
    } while (max_count - min_count > 1);
  }
  return ret;
}


//////////////ObTenantThreadHelper
int ObTenantThreadHelper::create(
    const char* thread_name, int tg_def_id, ObTenantThreadHelper &tenant_thread)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_created_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_ISNULL(thread_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("thread name is null", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_def_id, tg_id_))) {
    LOG_ERROR("create tg failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_ERROR("set thread runable fail", KR(ret));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::REENTRANT_THREAD_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", KR(ret));
  } else {
    thread_name_ = thread_name;
    is_created_ = true;
    is_first_time_to_start_ = true;
  }
  return ret;
}

int ObTenantThreadHelper::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_first_time_to_start_) {
    if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("fail ed to start at first time", KR(ret), K(tg_id_), K(thread_name_));
    } else {
      is_first_time_to_start_ = false;
    }
  } else if (OB_FAIL(TG_REENTRANT_LOGICAL_START(tg_id_))) {
    LOG_WARN("failed to start", KR(ret));
  }
  LOG_INFO("[TENANT THREAD] thread start", KR(ret), K(tg_id_), K(thread_name_));
  return ret;
}

void ObTenantThreadHelper::stop()
{
  LOG_INFO("[TENANT THREAD] thread stop start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_STOP(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread stop finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::wait()
{
  LOG_INFO("[TENANT THREAD] thread wait start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_WAIT(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread wait finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::mtl_thread_stop()
{
  LOG_INFO("[TENANT THREAD] thread stop start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread stop finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::mtl_thread_wait()
{
  LOG_INFO("[TENANT THREAD] thread wait start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
    is_first_time_to_start_ = true;
  }
  LOG_INFO("[TENANT THREAD] thread wait finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::destroy()
{
  LOG_INFO("[TENANT THREAD] thread destory start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_created_ = false;
  is_first_time_to_start_ = true;
  LOG_INFO("[TENANT THREAD] thread destory finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::switch_to_follower_forcedly()
{
  stop();
}
int ObTenantThreadHelper::switch_to_leader()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[TENANT THREAD] thread start", K(tg_id_), K(thread_name_));
  if (OB_FAIL(start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    ObThreadCondGuard guard(thread_cond_);
    if (OB_FAIL(thread_cond_.broadcast())) {
      LOG_WARN("failed to weakup thread cond", KR(ret));
    }
  }
  LOG_INFO("[TENANT THREAD] thread start finish", K(tg_id_), K(thread_name_));
  return ret;
}
void ObTenantThreadHelper::run1()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lib::set_thread_name(thread_name_);
    LOG_INFO("thread run", K(thread_name_));
    do_work();
  }
}
void ObTenantThreadHelper::idle(const int64_t idle_time_us)
{
  ObThreadCondGuard guard(thread_cond_);
  thread_cond_.wait_us(idle_time_us);
}

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
  } else {
    int64_t idle_time_us = 100 * 1000L;
    int tmp_ret = OB_SUCCESS;
    while (!has_set_stop()) {
      idle_time_us = 1000 * 1000L;
      {
        ObCurTraceId::init(GCONF.self_addr_);
        share::schema::ObSchemaGetterGuard schema_guard;
        const share::schema::ObTenantSchema *tenant_schema = NULL;
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", KR(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get schema guard", KR(ret));
        } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
          LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id_));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K(tenant_id_));
        } else if (!is_user_tenant(tenant_id_)) {
          if (OB_SUCCESS != (tmp_ret = process_meta_tenant_(*tenant_schema))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            LOG_WARN("failed to process tenant", KR(ret), KR(tmp_ret), K(tenant_id_));
          }
          //drop tenant, no need process sys tenant, ignore failuer
          if (!is_sys_tenant(tenant_id_)) {
            const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
            if (OB_SUCCESS != (tmp_ret = try_force_drop_tenant_(user_tenant_id))) {
              ret = OB_SUCC(ret) ? tmp_ret : ret;
              LOG_WARN("failed to drop tenant", KR(ret), KR(tmp_ret), K(user_tenant_id));
            }
          }
        } else {
          if (OB_SUCCESS !=( tmp_ret = process_user_tenant_(*tenant_schema))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            LOG_WARN("failed to process tenant", KR(ret), KR(tmp_ret), K(tenant_id_));
          }

          if (OB_SUCCESS != (tmp_ret = report_sys_ls_recovery_stat_())) {
            //ignore error of report, no need wakeup
            LOG_WARN("failed to report sys ls recovery stat", KR(ret), KR(tmp_ret), K(tenant_id_));
          }
        }
      }//for schema guard, must be free
      if (OB_FAIL(ret)) {
        idle_time_us = 100 * 1000;
      }
      idle(idle_time_us);
      LOG_INFO("[LS_SER] finish one round", KR(ret), K(idle_time_us));
    }// end while
  }
}


//meta tenant no need process create_new_ls or drop ls
//only need to agjust primary zone of sys_ls
int ObPrimaryLSService::process_meta_tenant_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (!share::schema::ObSchemaService::is_formal_version(tenant_schema.get_schema_version())
             || !tenant_schema.is_normal()) {
  } else if (tenant_schema.is_dropping()) {
  } else {
    share::ObLSPrimaryZoneInfo primary_zone_info;
    ObArray<common::ObZone> primary_zone;
    share::ObLSStatusOperator status_op;
    ObZone new_primary_zone;
    ObSqlString new_zone_priority;
    if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
                   tenant_schema, primary_zone))) {
      LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(status_op.get_ls_primary_zone_info(tenant_id_,
            SYS_LS, primary_zone_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(0 == primary_zone.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary zone is empty", KR(ret), K(tenant_schema));
    } else if (has_exist_in_array(primary_zone, primary_zone_info.get_primary_zone())) {
      if (OB_FAIL(new_primary_zone.assign(primary_zone_info.get_primary_zone()))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone_info));
      }
    } else if (OB_FAIL(new_primary_zone.assign(primary_zone.at(0)))) {
      LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone));
    }
    if (OB_FAIL(ret)) {
    } else if (is_sys_tenant(tenant_id_)) {
      //sys tenant use tenant normalize primary zone
      if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_zone_priority(
            tenant_schema, new_zone_priority))) {
        LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
      }
    } else if (OB_FAIL(ObTenantLSInfo::get_zone_priority(new_primary_zone, tenant_schema, new_zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(new_primary_zone));
    }
    if (FAILEDx(ObTenantLSInfo::try_update_ls_primary_zone_(
            primary_zone_info, new_primary_zone, new_zone_priority))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
          K(new_primary_zone), K(new_zone_priority));
    }

    if (OB_SUCC(ret) && !is_sys_tenant(tenant_id_)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = gather_tenant_recovery_stat_())) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to gather tenant recovery stat", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObPrimaryLSService::process_user_tenant_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, &tenant_schema, tenant_id,
                             GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
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
    if (OB_FAIL(tenant_stat.drop_tenant(share::NORMAL_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to drop tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_stat.process_ls_status_missmatch(false/* lock_sys_ls */,
                                                               share::NORMAL_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to process ls status missmatch", KR(ret), KR(tmp_ret));
    }
  } else {
    //normal tenant
    //some ls may failed to create ls, but can continue
    if (OB_SUCCESS != (tmp_ret = tenant_stat.process_ls_status_missmatch(false/* lock_sys_ls */,
                                                                share::NORMAL_SWITCHOVER_STATUS))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to process ls status missmatch", KR(ret), KR(tmp_ret));
    }

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
    if (OB_SUCCESS != (tmp_ret = tenant_stat.adjust_user_tenant_primary_zone())) {
      //ignore error
      LOG_WARN("failed to adjust user tenant primary zone", KR(ret), KR(tmp_ret));
    }
  }

  LOG_INFO("finish process tenant", KR(ret), KR(tmp_ret), K(tenant_id), K(tenant_schema));

  return ret;
}


int ObPrimaryLSService::report_sys_ls_recovery_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else {
    ObLSService *ls_svr = MTL(ObLSService *);
    ObLSHandle ls_handle;
    if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("failed to get ls", KR(ret));
    } else if (OB_FAIL(ObTenantRecoveryReportor::update_ls_recovery(ls_handle.get_ls(), GCTX.sql_proxy_))) {
      LOG_WARN("failed to update ls recovery", KR(ret));
    }
  }
  return ret;
}

int ObPrimaryLSService::try_force_drop_tenant_(
    const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)
             || OB_ISNULL(GCTX.rs_rpc_proxy_)
             || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.rs_rpc_proxy_),
        KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(!is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant ids", KR(ret), K(user_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(user_tenant_id));
  } else if (!tenant_schema->is_dropping()) {
  } else {
    LOG_INFO("try drop tenant", K(user_tenant_id));
    ObLSStatusOperator op;
    share::ObLSStatusInfoArray ls_array;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(op.get_all_ls_status_by_order(user_tenant_id, ls_array, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(user_tenant_id));
    } else if (ls_array.count() <= 0) {
      obrpc::ObDropTenantArg arg;
      arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      arg.tenant_name_ = tenant_schema->get_tenant_name();
      arg.tenant_id_ = tenant_schema->get_tenant_id();
      arg.if_exist_ = true;
      arg.delay_to_drop_ = false;
      ObSqlString sql;
      const int64_t timeout_ts = ctx.get_timeout();
      if (OB_FAIL(sql.append_fmt("DROP TENANT IF EXISTS %s FORCE", arg.tenant_name_.ptr()))) {
        LOG_WARN("fail to generate sql", KR(ret), K(arg));
      } else if (FALSE_IT(arg.ddl_stmt_str_ = sql.string())) {
      } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(timeout_ts).drop_tenant(arg))) {
        LOG_WARN("fail to drop tenant", KR(ret), K(arg), K(timeout_ts));
      }
    } else {
      // tenant's logstream is still dropping, check next round
    }
  }
  return ret;
}

int ObPrimaryLSService::gather_tenant_recovery_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_sys_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tenant no need gather tenant recovery stat", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObLSRecoveryStatOperator ls_recovery_op;
    ObAllTenantInfoProxy info_proxy;
    ObAllTenantInfo tenant_info;
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    SCN sync_scn;
    SCN min_wrs_scn;
    DEBUG_SYNC(BLOCK_TENANT_SYNC_SNAPSHOT_INC);
    if (OB_FAIL(ls_recovery_op.get_tenant_recovery_stat(
            user_tenant_id, *GCTX.sql_proxy_, sync_scn, min_wrs_scn))) {
      LOG_WARN("failed to get tenant recovery stat", KR(ret), K(user_tenant_id));
      //TODO replayable_scn is equal to sync_scn
    } else if (OB_FAIL(info_proxy.update_tenant_recovery_status(
                   user_tenant_id, GCTX.sql_proxy_, share::NORMAL_SWITCHOVER_STATUS, sync_scn,
                   sync_scn, min_wrs_scn))) {
      LOG_WARN("failed to update tenant recovery stat", KR(ret),
               K(user_tenant_id), K(sync_scn), K(min_wrs_scn));
    }
  }
  return ret;
}
}
}

