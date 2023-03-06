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
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/ob_tenant_info_loader.h"//get_tenant_info
#include "rootserver/ob_tenant_thread_helper.h"//get_zone_priority
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
    ls_info_ = ls_info;
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

int ObTenantLSInfo::revision_to_equal_status(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSStatusMachineParameter> status_machine_array;
  if (OB_FAIL(construct_ls_status_machine_(status_machine_array))) {
    LOG_WARN("failed to construct ls status machine array", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(sql_proxy_), KP(tenant_schema_));
  } else {
    ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    ARRAY_FOREACH_NORET(status_machine_array, idx) {
      const ObLSStatusMachineParameter &machine = status_machine_array.at(idx);
      const share::ObLSStatusInfo &status_info = machine.status_info_;
      const share::ObLSAttr &ls_info = machine.ls_info_;
      if (ls_info.get_ls_status() == status_info.status_) {
        //if ls and ls status is equal, need to process
      } else if (!ls_info.is_valid()) {
        if (status_info.ls_is_create_abort()) {
         //ls may create_abort
          if (OB_FAIL(ls_life_agent.drop_ls_in_trans(tenant_id, machine.ls_id_,
                      share::NORMAL_SWITCHOVER_STATUS, trans))) {
            LOG_WARN("failed to delete ls", KR(ret), K(machine));
          }
        } else {
          //ls can not be waitoffline or other status
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status machine not expected", KR(ret), K(machine));
        }
      } else if (ls_info.ls_is_creating()) {
        if (!status_info.is_valid()) {
          if (OB_FAIL(create_new_ls_in_trans(ls_info.get_ls_id(),
                                    ls_info.get_ls_group_id(),
                                    ls_info.get_create_scn(), trans))) {
            LOG_WARN("failed to create new ls", KR(ret), K(machine));
          }
        } else if (status_info.ls_is_created()
                   || status_info.ls_is_create_abort()) {
          //nothing todo
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status machine not expected", KR(ret), K(machine));
        }
      } else if (ls_info.ls_is_normal()) {
        if (status_info.ls_is_created()) {
              if (OB_FAIL(status_operator_.update_ls_status(
            tenant_id, status_info.ls_id_, status_info.status_,
            ls_info.get_ls_status(), share::NORMAL_SWITCHOVER_STATUS, trans))) {
             LOG_WARN("failed to update ls status", KR(ret), K(status_info), K(tenant_id), K(ls_info));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status machine not expected", KR(ret), K(machine));
        }
      } else {
        //in upgrade, tenant can not be dropping, so no need to take care of
        //pre_tenant_dropping, tenant_dropping, waitoffline
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ls not expected in all_ls", KR(ret), K(machine));
      }
    }
  }
  return ret;
}


//Regardless of the tenant being dropped,
//handle the asynchronous operation of status and history table
int ObTenantLSInfo::process_next_ls_status(int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSStatusMachineParameter> status_machine_array;

  if (OB_FAIL(construct_ls_status_machine_(status_machine_array))) {
    LOG_WARN("failed to construct ls status machine array", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_steady = false;
    ARRAY_FOREACH_NORET(status_machine_array, idx) {
      const ObLSStatusMachineParameter &machine = status_machine_array.at(idx);
      if (OB_TMP_FAIL(process_next_ls_status_(machine, is_steady))) {
        LOG_WARN("failed to do next ls process", KR(ret), KR(tmp_ret), K(machine));
      }
      if (OB_TMP_FAIL(tmp_ret) || !is_steady) {
        task_cnt++;
      }
    }
  }

  return ret;
}

int ObTenantLSInfo::construct_ls_status_machine_(
    common::ObIArray<ObLSStatusMachineParameter> &status_machine_array)
{
  int ret = OB_SUCCESS;
  status_machine_array.reset();
  share::ObLSStatusInfoArray status_info_array;
  share::ObLSAttrArray ls_array;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or tenant schema is null", KR(ret), KP(tenant_schema_), KP(sql_proxy_));
  } else if (OB_FAIL(ls_operator_.get_all_ls_by_order(ls_array))) {
    LOG_WARN("failed to get all ls by order", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_schema_->get_tenant_id();
    if (OB_FAIL(status_operator_.get_all_ls_status_by_order(
                 tenant_id, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to get all ls status by order", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == status_info_array.count())) {
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

int ObTenantLSInfo::drop_tenant()
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
          attr.get_ls_status(), share::OB_LS_PRE_TENANT_DROPPING))) {
            LOG_WARN("failed to update ls status", KR(ret), K(attr));
          }
        }
      } else if (attr.ls_is_creating()) {
        //drop the status
        if (OB_FAIL(ls_operator_.delete_ls(attr.get_ls_id(), attr.get_ls_status()))) {
          LOG_WARN("failed to remove ls not normal", KR(ret), K(attr));
        }
      } else {
        //no matter the status is in normal or dropping
        //may be the status in status info is created
        if (!attr.ls_is_tenant_dropping()) {
          if (OB_FAIL(ls_operator_.update_ls_status(
                  attr.get_ls_id(), attr.get_ls_status(),
                  share::OB_LS_TENANT_DROPPING))) {
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
      LOG_WARN("failed to get all ls status by order", KR(ret), K(tenant_id));
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

int ObTenantLSInfo::create_new_ls_in_trans(
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
    } else if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(primary_zone, *tenant_schema_, zone_priority))) {
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

//for create new ls
//ls_info:creating, status_info:creating, status_info:created, ls_info:normal, status_info:normal

//for drop ls, not supported

//for drop tenant
//ls_info:tenant_dropping, status_info:tenant_dropping, ls_info:empty, status_info:wait_offline
//the sys ls can be tenant_dropping in status_info after all_users ls were deleted in ls_info
int ObTenantLSInfo::process_next_ls_status_(const ObLSStatusMachineParameter &machine, bool &is_steady)
{
  int ret = OB_SUCCESS;
  const share::ObLSStatusInfo &status_info = machine.status_info_;
  const share::ObLSAttr &ls_info = machine.ls_info_;
  const uint64_t tenant_id = status_info.tenant_id_;
  is_steady = false;
  if (OB_UNLIKELY(!machine.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("machine is invalid", KR(ret), K(machine));
  } else if (!ls_info.is_valid()) {
    if (status_info.ls_is_wait_offline()) {
      is_steady = true;
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
      if (OB_FAIL(ls_operator_.delete_ls(
            machine.ls_id_, share::OB_LS_CREATING))) {
        LOG_WARN("failed to process creating info", KR(ret), K(machine));
      }
    } else if (status_info.ls_is_created()) {
      //set ls to normal
      if (OB_FAIL(ls_operator_.update_ls_status(
            machine.ls_id_, ls_info.get_ls_status(), share::OB_LS_NORMAL))) {
        LOG_WARN("failed to update ls status", KR(ret), K(machine));
      }
    } else if (status_info.ls_is_creating()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status info is invalid", KR(ret), K(machine));
    }
  } else if (ls_info.ls_is_normal()) {
    if (status_info.ls_is_normal()) {
      is_steady = true;
    } else if (status_info.ls_is_created()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status info is invalid", KR(ret), K(machine));
    }
  } else if (ls_info.ls_is_dropping()) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("drop ls not supported", KR(ret), K(machine));
  } else if (share::ls_is_pre_tenant_dropping_status(ls_info.get_ls_status())) {
    if (!machine.ls_id_.is_sys_ls()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("normal ls can not in pre tenant dropping status", KR(ret), K(machine));
    } else if (!status_info.ls_is_pre_tenant_dropping()) {
    } else if (OB_FAIL(sys_ls_tenant_drop_(status_info))) {
      LOG_WARN("failed to process sys ls", KR(ret), K(status_info));
    }
  } else if (ls_info.ls_is_tenant_dropping()) {
    if (ls_info.get_ls_id().is_sys_ls()) {
      //nothing
    } else if (!status_info.ls_is_tenant_dropping()) {
    } else if (OB_FAIL(do_tenant_drop_ls_(status_info))) {
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
    if (share::ObUnit::UNIT_STATUS_ACTIVE != info.unit_status_) {
      // drop logstream, not support
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit status not valid", KR(ret), K(info));
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
      } else if (OB_FAIL(create_new_ls_(new_info))) {
        LOG_WARN("failed to add ls info", KR(ret), K(new_info));
      }
      LOG_INFO("[LS_MGR] create new ls for empty unit group", KR(ret), K(new_info));
    }//end for
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
        } else if (OB_FAIL(create_new_ls_(new_info))) {
          LOG_WARN("failed to create new ls", KR(ret), K(new_info));
        }
        LOG_INFO("[LS_MGR] create new ls of primary zone", KR(ret), K(new_info),
            K(group_id_count));
      }
    }
  }
  return ret;
}

int ObTenantLSInfo::create_new_ls_(const share::ObLSStatusInfo &status_info)
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
    share::ObLSAttr ls_info;
    share::ObLSFlag flag = share::OB_LS_FLAG_NORMAL;//TODO
    SCN create_scn;
    if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(status_info.tenant_id_, create_scn))) {
      LOG_WARN("failed to get tenant gts", KR(ret), K(status_info));
    } else if (OB_FAIL(ls_info.init(status_info.ls_id_, status_info.ls_group_id_, flag,
                             share::OB_LS_CREATING, share::OB_LS_OP_CREATE_PRE, create_scn))) {
      LOG_WARN("failed to init new operation", KR(ret), K(status_info), K(create_scn));
    } else if (OB_FAIL(ls_operator_.insert_ls(ls_info, max_ls_group_id_))) {
      LOG_WARN("failed to insert new operation", KR(ret), K(ls_info), K(max_ls_group_id_));
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

int ObTenantLSInfo::do_tenant_drop_ls_(const share::ObLSStatusInfo &status_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LS_MGR] start to tenant drop ls", K(status_info));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  bool can_offline = false;
  if (OB_UNLIKELY(!status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(status_info));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (!status_info.ls_is_tenant_dropping()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status info not expected", KR(ret), K(status_info));
  }
  if (FAILEDx(check_ls_can_offline_by_rpc(status_info, can_offline))) {
    // send rpc to observer
    LOG_WARN("failed to check ls can offline", KR(ret), K(status_info));
  } else if (can_offline) {
    //sys ls already block, can not delete ls
    const uint64_t tenant_id = status_info.tenant_id_;
    if (OB_FAIL(ls_operator_.delete_ls(status_info.ls_id_, share::OB_LS_TENANT_DROPPING))) {
      LOG_WARN("failed to delete ls", KR(ret), K(status_info));
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[LS_MGR] end to tenant drop ls", KR(ret), K(status_info), K(can_offline), K(cost));
  return ret;
}

int ObTenantLSInfo::sys_ls_tenant_drop_(const share::ObLSStatusInfo &info)
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
  } else if (pre_status != info.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls can not in other status", KR(ret), K(info));
  }
  bool can_offline = false;
  if (FAILEDx(check_sys_ls_can_offline_(can_offline))) {
    LOG_WARN("failed to check sys ls can offline", KR(ret));
  } else if (can_offline) {
    if (OB_FAIL(ls_operator_.update_ls_status(info.ls_id_, pre_status, target_status))) {
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
        LOG_INFO("[LS_MGR] sys ls can not offline", K(status_info));
      }
    }
    if (OB_SUCC(ret) && can_offline) {
      LOG_INFO("[LS_MGR] sys ls can offline", K(status_info_array));
    }
  }
  return ret;
}

int ObTenantLSInfo::check_ls_can_offline_by_rpc(const share::ObLSStatusInfo &info, bool &can_offline)
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  const share::ObLSReplica *replica = NULL;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
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
    if (OB_FAIL(arg.init(info.tenant_id_, info.ls_id_, info.status_))) {
      LOG_WARN("failed to init arg", KR(ret), K(arg));
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

////////////ObRecoveryLSHelper
int ObRecoveryLSHelper::do_work(palf::PalfBufferIterator &iterator,
              share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObTenantInfoLoader *tenant_loader = MTL(ObTenantInfoLoader*);
  ObAllTenantInfo tenant_info;
  // two thread for seed log and recovery_ls_manager
  if (!is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls recovery thread must run on user tenant", KR(ret),
             K(tenant_id_));
  } else if (OB_ISNULL(tenant_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tenant_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::check_can_do_recovery(tenant_info))) {
    LOG_WARN("failed to check do recovery", KR(ret), K(tenant_info));
  } else if (!start_scn.is_valid()) {
    ObAllTenantInfo new_tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, proxy_,
                                                    false, new_tenant_info))) {
        LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (new_tenant_info.get_recovery_until_scn() == new_tenant_info.get_sys_recovery_scn()) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // every minute
        LOG_INFO("has recovered to recovery_until_scn", KR(ret),
                 K(new_tenant_info));
      }
    } else if (OB_FAIL(
                   seek_log_iterator_(new_tenant_info.get_sys_recovery_scn(), iterator))) {
      LOG_WARN("failed to seek log iterator", KR(ret), K(new_tenant_info));
    } else {
      start_scn = new_tenant_info.get_sys_recovery_scn();
      LOG_INFO("start to seek at", K(start_scn));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (start_scn.is_valid() &&
             OB_FAIL(process_ls_log_(tenant_info, start_scn, iterator))) {
    if (OB_ITER_STOP != ret) {
      LOG_WARN("failed to process ls log", KR(ret), K(start_scn),
               K(tenant_info));
    }
  }
  if (OB_FAIL(ret)) {
    start_scn.reset();
  }
  return ret;
}

int ObRecoveryLSHelper::seek_log_iterator_(const SCN &sys_recovery_scn, PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sys_recovery_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync scn is invalid", KR(ret), K(sys_recovery_scn));
  } else {
    ObLSService *ls_svr = MTL(ObLSService *);
    ObLSHandle ls_handle;
    if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("failed to get ls", KR(ret));
    } else {
      logservice::ObLogHandler *log_handler = NULL;
      ObLS *ls = NULL;
      palf::LSN start_lsn;
      if (OB_ISNULL(ls = ls_handle.get_ls())
          || OB_ISNULL(log_handler = ls->get_log_handler())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls or log handle is null", KR(ret), KP(ls), KP(log_handler));
      } else if (sys_recovery_scn.is_base_scn()) {
        //The recovery_scn on the primary tenant is initialized to base_scn,
        //but too small scn may cause locate to fail,
        //so when base_scn is encountered, iterates the log directly from the initialized LSN.
        start_lsn = palf::LSN(palf::PALF_INITIAL_LSN_VAL);
      } else if (OB_FAIL(log_handler->locate_by_scn_coarsely(sys_recovery_scn, start_lsn))) {
        LOG_WARN("failed to locate lsn", KR(ret), K(sys_recovery_scn));
      }

      if (FAILEDx(log_handler->seek(start_lsn, iterator))) {
        LOG_WARN("failed to seek iterator", KR(ret), K(sys_recovery_scn));
      }
    }
  }
  return ret;
}

int ObRecoveryLSHelper::process_ls_log_(
      const ObAllTenantInfo &tenant_info,
      SCN &start_scn,
      PalfBufferIterator &iterator)
{
  int ret = OB_SUCCESS;
  palf::LogEntry log_entry;
  palf::LSN target_lsn;
  SCN sys_recovery_scn;
  SCN last_sys_recovery_scn;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_info));
  }
  while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
    if (OB_FAIL(iterator.get_entry(log_entry, target_lsn))) {
      LOG_WARN("failed to get log", KR(ret), K(log_entry));
    } else if (OB_ISNULL(log_entry.get_data_buf())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log entry is null", KR(ret));
    } else {
      sys_recovery_scn = log_entry.get_scn();
      LOG_TRACE("get log", K(log_entry), K(target_lsn), K(start_scn), K(sys_recovery_scn));
      const char *log_buf = log_entry.get_data_buf();
      const int64_t log_length = log_entry.get_data_len();
      logservice::ObLogBaseHeader header;
      const int64_t HEADER_SIZE = header.get_serialize_size();
      int64_t log_pos = 0;
      if (OB_UNLIKELY(sys_recovery_scn <= start_scn)) {
        //通过scn定位的LSN是不准确的，可能会获取多余的数据，所以需要把小于等于sys_recovery_scn的过滤掉
        continue;
      } else if (tenant_info.get_recovery_until_scn() < sys_recovery_scn) {
        if (OB_FAIL(report_tenant_sys_recovery_scn_(tenant_info.get_recovery_until_scn(), false, proxy_))) {
          LOG_WARN("failed to report_tenant_sys_recovery_scn_", KR(ret), K(sys_recovery_scn), K(tenant_info),
                   K(log_entry), K(target_lsn), K(start_scn));
        // SYS LS has recovered to the recovery_until_scn, need stop iterate SYS LS log and reset start_scn
        } else if (OB_FAIL(seek_log_iterator_(tenant_info.get_recovery_until_scn(), iterator))) {
          LOG_WARN("failed to seek log iterator", KR(ret), K(sys_recovery_scn), K(tenant_info),
                    K(log_entry), K(target_lsn), K(start_scn));
        } else {
          ret = OB_ITER_STOP;
          LOG_WARN("SYS LS has recovered to the recovery_until_scn, need stop iterate SYS LS log",
                   KR(ret), K(sys_recovery_scn), K(tenant_info), K(log_entry), K(target_lsn), K(start_scn));
          start_scn.reset();
        }
      } else if (OB_FAIL(header.deserialize(log_buf, HEADER_SIZE, log_pos))) {
        LOG_WARN("failed to deserialize", KR(ret), K(HEADER_SIZE));
      } else if (OB_UNLIKELY(log_pos >= log_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log pos is not expected", KR(ret), K(log_pos), K(log_length));
        // nothing
      } else if (logservice::TRANS_SERVICE_LOG_BASE_TYPE == header.get_log_type()) {
        ObTxLogBlock tx_log_block;
        ObTxLogBlockHeader tx_block_header;
        if (OB_FAIL(tx_log_block.init(log_buf, log_length, log_pos, tx_block_header))) {
          LOG_WARN("failed to init tx log block", KR(ret), K(log_length));
        } else if (OB_FAIL(process_ls_tx_log_(tx_log_block, sys_recovery_scn))) {
          LOG_WARN("failed to process ls tx log", KR(ret), K(tx_log_block), K(sys_recovery_scn));
        }
      } else {}
      if (OB_SUCC(ret)) {
        last_sys_recovery_scn = sys_recovery_scn;
      }
      if ((OB_FAIL(ret) && last_sys_recovery_scn.is_valid())
          || (OB_SUCC(ret) && REACH_TIME_INTERVAL(100 * 1000))) {
        //if ls_operator can not process, need to report last sync scn
        //Forced reporting every second
        if (OB_TMP_FAIL(report_tenant_sys_recovery_scn_(last_sys_recovery_scn, false, proxy_))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to report ls recovery stat", KR(ret), KR(tmp_ret), K(last_sys_recovery_scn));
        }
      }
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      if (OB_TMP_FAIL(try_to_process_sys_ls_offline_())) {
        LOG_WARN("failed to process sys ls offline", KR(ret), KR(tmp_ret));
      }
    }
  }//end for each log
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (!sys_recovery_scn.is_valid()) {
    } else if (OB_FAIL(report_tenant_sys_recovery_scn_(sys_recovery_scn, false, proxy_))) {
      LOG_WARN("failed to report ls recovery stat", KR(ret), K(sys_recovery_scn));
    }
  } else if (OB_SUCC(ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator must be end", KR(ret));
  } else {
    LOG_WARN("failed to get next log", KR(ret));
  }

  return ret;
}

int ObRecoveryLSHelper::process_ls_tx_log_(ObTxLogBlock &tx_log_block, const SCN &sys_recovery_scn)
{
  int ret = OB_SUCCESS;
  bool has_operation = false;
  while (OB_SUCC(ret)) {
    transaction::ObTxLogHeader tx_header;
    if (OB_FAIL(tx_log_block.get_next_log(tx_header))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next log", KR(ret));
      }
    } else if (transaction::ObTxLogType::TX_COMMIT_LOG !=
               tx_header.get_tx_log_type()) {
      // nothing
    } else {
      ObTxCommitLogTempRef temp_ref;
      ObTxCommitLog commit_log(temp_ref);
      const int64_t COMMIT_SIZE = commit_log.get_serialize_size();
      //TODO commit log may too large
      if (OB_FAIL(tx_log_block.deserialize_log_body(commit_log))) {
        LOG_WARN("failed to deserialize", KR(ret));
      } else {
        const ObTxBufferNodeArray &source_data =
            commit_log.get_multi_source_data();
        for (int64_t i = 0; OB_SUCC(ret) && i < source_data.count(); ++i) {
          const ObTxBufferNode &node = source_data.at(i);
          if (ObTxDataSourceType::STANDBY_UPGRADE == node.get_data_source_type()) {
            if (OB_FAIL(process_upgrade_log_(node))) {
              LOG_WARN("failed to process_upgrade_log_", KR(ret), K(node));
            }
          } else if (ObTxDataSourceType::LS_TABLE != node.get_data_source_type()) {
            // nothing
          } else if (has_operation) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("one clog has more than one operation", KR(ret), K(commit_log));
          } else {
            has_operation = true;
            ObLSAttr ls_attr;
            const int64_t LS_SIZE = ls_attr.get_serialize_size();
            int64_t pos = 0;
            if (OB_FAIL(ls_attr.deserialize(node.get_data_buf().ptr(), LS_SIZE,
                                            pos))) {
              LOG_WARN("failed to deserialize", KR(ret), K(node), K(LS_SIZE));
            } else if (OB_UNLIKELY(pos > LS_SIZE)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get ls attr", KR(ret), K(pos), K(LS_SIZE));
            } else {
              LOG_INFO("get ls operation", K(ls_attr), K(sys_recovery_scn));
              //TODO ls recovery is too fast for ls manager, so it maybe failed, while change ls status
              //consider how to retry
              if (OB_FAIL(process_ls_operator_(ls_attr,
                                               sys_recovery_scn))) {
                LOG_WARN("failed to process ls operator", KR(ret), K(ls_attr),
                         K(sys_recovery_scn));
              }
            }
          }
        }// end for
      }
    }
  }  // end while for each tx_log

  return ret;
}

int ObRecoveryLSHelper::process_upgrade_log_(const ObTxBufferNode &node)
{
  int ret = OB_SUCCESS;
  uint64_t standby_data_version = 0;

  if (!node.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is invalid", KR(ret), K(node));
  } else {
    ObStandbyUpgrade primary_data_version;
    int64_t pos = 0;
    if (OB_FAIL(primary_data_version.deserialize(node.get_data_buf().ptr(), node.get_data_buf().length(), pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(node), KPHEX(node.get_data_buf().ptr(), node.get_data_buf().length()));
    } else if (OB_UNLIKELY(pos > node.get_data_buf().length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get primary_data_version", KR(ret), K(pos), K(node.get_data_buf().length()));
    } else {
      LOG_INFO("get primary_data_version", K(primary_data_version));
      if (!primary_data_version.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary_data_version not valid", KR(ret), K(primary_data_version));
      } else if (OB_FAIL(ObUpgradeChecker::get_data_version_by_cluster_version(
                                           GET_MIN_CLUSTER_VERSION(),
                                           standby_data_version))) {
        LOG_WARN("failed to get_data_version_by_cluster_version", KR(ret), K(GET_MIN_CLUSTER_VERSION()));
      } else if (primary_data_version.get_data_version() > standby_data_version) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(30 * 60 * 1000 * 1000)) { // 30min
          LOG_ERROR("standby version is not new enough to recover primary clog", KR(ret),
                   K(primary_data_version), K(standby_data_version));
        }
      }
    }
  }
  return ret;
}

int ObRecoveryLSHelper::process_ls_operator_(const share::ObLSAttr &ls_attr, const SCN &sys_recovery_scn)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_UNLIKELY(!ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_FAIL(check_valid_to_operator_ls_(ls_attr, sys_recovery_scn))) {
    LOG_WARN("failed to check valid to operator ls", KR(ret), K(sys_recovery_scn), K(ls_attr));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(process_ls_operator_in_trans_(ls_attr, sys_recovery_scn, trans))) {
    LOG_WARN("failed to process ls operator in trans", KR(ret), K(ls_attr), K(sys_recovery_scn));
  } else if (OB_FAIL(report_tenant_sys_recovery_scn_trans_(sys_recovery_scn, true, trans))) {
    LOG_WARN("failed to report tenant sys recovery scn", KR(ret), K(sys_recovery_scn));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObRecoveryLSHelper::check_valid_to_operator_ls_(
    const share::ObLSAttr &ls_attr, const SCN &sys_recovery_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_UNLIKELY(!sys_recovery_scn.is_valid() || !ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("syns scn is invalid", KR(ret), K(sys_recovery_scn), K(ls_attr));
  } else if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operation_type())) {
    ObTenantSchema tenant_schema;
    if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
    } else if (!tenant_schema.is_dropping()) {
      ret = OB_ITER_STOP;
      LOG_WARN("can not process ls operator before tenant dropping", K(ls_attr), K(tenant_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    SCN min_sys_recovery_scn;
    SCN min_wrs;
    ObLSRecoveryStatOperator ls_recovery;
    if (OB_FAIL(
            ls_recovery.get_tenant_recovery_stat(tenant_id_, *proxy_, min_sys_recovery_scn, min_wrs))) {
      LOG_WARN("failed to get user scn", KR(ret), K(tenant_id_));
    } else if (min_sys_recovery_scn >= sys_recovery_scn) {
      // other ls has larger sync scn, ls can operator
    } else {
      ret = OB_NEED_WAIT;
      LOG_WARN("can not process ls operator, need wait other ls sync", KR(ret),
               K(min_sys_recovery_scn), K(sys_recovery_scn));
    }
  }
  return ret;
}

int ObRecoveryLSHelper::process_ls_operator_in_trans_(
    const share::ObLSAttr &ls_attr, const SCN &sys_recovery_scn, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_UNLIKELY(!ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else {
    ObLSStatusOperator ls_operator;
    share::ObLSStatusInfo ls_status;
    ObLSLifeAgentManager ls_life_agent(*proxy_);
    if (OB_UNLIKELY(!ls_attr.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ls attr is invalid", KR(ret), K(ls_attr));
    } else if (share::is_ls_create_pre_op(ls_attr.get_ls_operation_type())) {
      //create new ls;
      if (OB_FAIL(create_new_ls_(ls_attr, sys_recovery_scn, trans))) {
        LOG_WARN("failed to create new ls", KR(ret), K(sys_recovery_scn), K(ls_attr));
      }
    } else if (share::is_ls_create_abort_op(ls_attr.get_ls_operation_type())) {
      if (OB_FAIL(ls_life_agent.drop_ls_in_trans(tenant_id_, ls_attr.get_ls_id(), share::NORMAL_SWITCHOVER_STATUS, trans))) {
        LOG_WARN("failed to drop ls", KR(ret), K(tenant_id_), K(ls_attr));
      }
    } else if (share::is_ls_drop_end_op(ls_attr.get_ls_operation_type())) {
      if (OB_FAIL(ls_life_agent.set_ls_offline_in_trans(tenant_id_, ls_attr.get_ls_id(),
              ls_attr.get_ls_status(), sys_recovery_scn, share::NORMAL_SWITCHOVER_STATUS, trans))) {
        LOG_WARN("failed to set offline", KR(ret), K(tenant_id_), K(ls_attr), K(sys_recovery_scn));
      }
    } else {
      ObLSStatus target_status = share::OB_LS_EMPTY;
      if (OB_FAIL(ls_operator.get_ls_status_info(tenant_id_, ls_attr.get_ls_id(),
              ls_status, trans))) {
        LOG_WARN("failed to get ls status", KR(ret), K(tenant_id_), K(ls_attr));
      } else if (ls_status.ls_is_creating()) {
        ret = OB_EAGAIN;
        LOG_WARN("ls not created, need wait", KR(ret), K(ls_status));
      } else if (share::is_ls_create_end_op(ls_attr.get_ls_operation_type())) {
        // set ls to normal
        target_status = share::OB_LS_NORMAL;
      } else if (share::is_ls_tenant_drop_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_TENANT_DROPPING;
      } else if (share::is_ls_drop_pre_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_DROPPING;
      } else if (share::is_ls_tenant_drop_pre_op(ls_attr.get_ls_operation_type())) {
        target_status = share::OB_LS_PRE_TENANT_DROPPING;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operation type", KR(ret), K(ls_attr));
      }
      if (FAILEDx(ls_operator.update_ls_status(tenant_id_, ls_attr.get_ls_id(),
              ls_status.status_, target_status,
              share::NORMAL_SWITCHOVER_STATUS, trans))) {
        LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_), K(ls_attr),
            K(ls_status), K(target_status));
      }
      LOG_INFO("[LS_RECOVERY] update ls status", KR(ret), K(ls_attr), K(target_status));
    }
  }

  return ret;
}
int ObRecoveryLSHelper::create_new_ls_(const share::ObLSAttr &ls_attr,
                                        const SCN &sys_recovery_scn,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!share::is_ls_create_pre_op(ls_attr.get_ls_operation_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls not create pre operation", KR(ret), K(ls_attr));
  } else {
    //create new ls;
    share::schema::ObTenantSchema tenant_schema;
    if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id_, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
    } else {
      ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, &tenant_schema, tenant_id_,
                                GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
      if (OB_FAIL(tenant_stat.gather_stat(true))) {
        LOG_WARN("failed to gather stat", KR(ret));
      } else if (OB_FAIL(tenant_stat.create_new_ls_in_trans(ls_attr.get_ls_id(),
              ls_attr.get_ls_group_id(), ls_attr.get_create_scn(), trans))) {
        LOG_WARN("failed to add new ls status info", KR(ret), K(ls_attr), K(sys_recovery_scn));
      }
    }
    LOG_INFO("[LS_RECOVERY] create new ls", KR(ret), K(ls_attr));
  }
  return ret;
}

int ObRecoveryLSHelper::report_tenant_sys_recovery_scn_(const SCN &sys_recovery_scn,
           const bool update_sys_recovery_scn, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), KP(proxy));
  } else if (SCN::base_scn() != sys_recovery_scn) {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    START_TRANSACTION(proxy, exec_tenant_id)
    if (FAILEDx(report_tenant_sys_recovery_scn_trans_(sys_recovery_scn, update_sys_recovery_scn, trans))) {
      LOG_WARN("failed to report tenant sys recovery scn in trans", KR(ret), K(sys_recovery_scn));
    }
    END_TRANSACTION(trans)
  }
  return ret;
}

int ObRecoveryLSHelper::report_tenant_sys_recovery_scn_trans_(const share::SCN &sys_recovery_scn,
               const bool update_sys_recovery_scn, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTenantInfoLoader *tenant_report = MTL(ObTenantInfoLoader*);
  ObAllTenantInfo tenant_info;
  if (OB_ISNULL(tenant_report)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tenant_report->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_sys_recovery_scn_in_trans(
             tenant_id_, sys_recovery_scn, update_sys_recovery_scn, trans))) {
    LOG_WARN("failed to update tenant sys recovery scn", KR(ret), K(tenant_id_),
             K(sys_recovery_scn), K(tenant_info), K(update_sys_recovery_scn));
  } else {
   //double check sync can not fallback
   //log will be truncated during the flashback.
   //If a certain iteration report crosses the flashback,
   //recovery scn too large may be reported. Therefore,
   //it is verified that the reported bit is not rolled back within the transaction.
    palf::PalfHandleGuard palf_handle_guard;
    logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
    SCN new_scn;
    if (OB_FAIL(ls_svr->open_palf(SYS_LS, palf_handle_guard))) {
      LOG_WARN("failed to open palf", KR(ret), K(ls_id));
    } else if (OB_FAIL(palf_handle_guard.get_end_scn(new_scn))) {
      LOG_WARN("failed to get end ts", KR(ret), K(ls_id));
    } else if (new_scn < sys_recovery_scn) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("maybe flashback, can not report sync scn", KR(ret), K(sys_recovery_scn), K(new_scn));
    }
  }
  return ret;
}

int ObRecoveryLSHelper::try_to_process_sys_ls_offline_()
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
  } else if (!tenant_schema.is_dropping()) {
    //not dropping, nothing
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else {
    //check sys ls is tenant dropping, and check can offline
    share::ObLSStatusInfo ls_status;
    share::ObLSStatusOperator ls_op;
    if (OB_FAIL(ls_op.get_ls_status_info(tenant_id_, SYS_LS, ls_status, *proxy_))) {
      LOG_WARN("failed to get ls status", KR(ret), K(tenant_id_));
    } else if (!ls_status.ls_is_tenant_dropping()) {
      //nothing todo
    } else {
      bool can_offline = false;
      ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, &tenant_schema, tenant_id_,
                                GCTX.srv_rpc_proxy_, GCTX.lst_operator_);
      if (OB_FAIL(tenant_stat.check_ls_can_offline_by_rpc(ls_status, can_offline))) {
        LOG_WARN("failed to check ls can offline", KR(ret), K(ls_status));
      } else if (!can_offline) {
        //nothing
      } else {
        ObLSLifeAgentManager ls_life_agent(*proxy_);
        if (OB_FAIL(ls_life_agent.set_ls_offline(tenant_id_, SYS_LS, ls_status.status_,
                         SCN::base_scn(), share::NORMAL_SWITCHOVER_STATUS))) {
          LOG_WARN("failed to set ls offline", KR(ret), K(ls_status));
        }
      }
    }
  }
  return ret;
}

}
}
