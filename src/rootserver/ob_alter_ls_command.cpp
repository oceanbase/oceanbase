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

#include "ob_alter_ls_command.h"

#include "lib/oblog/ob_log_module.h"
#include "observer/ob_server_struct.h"
#include "share/ob_errno.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/balance/ob_balance_task_table_operator.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "storage/tablelock/ob_lock_utils.h"
#include "rootserver/ob_tenant_event_def.h"
#include "share/ob_unit_table_operator.h"
#include "rootserver/ob_unit_manager.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction::tablelock;
using namespace oceanbase::tenant_event;
namespace rootserver
{
int ObAlterLSCommand::process(const ObAlterLSArg &arg, ObAlterLSRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_(arg))) {
    LOG_WARN("fail to init", KR(ret), K(arg));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K(arg_));
  } else if (arg_.is_create_service() && OB_FAIL(create_ls_())) {
    LOG_WARN("fail to create ls", KR(ret), K(arg_));
  } else if (arg_.is_modify_service() && OB_FAIL(modify_ls_())) {
    LOG_WARN("fail to modify ls", KR(ret), K(arg_));
  } else if (arg_.is_drop_service()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to drop ls", KR(ret), K(arg_));
  }
  res.ret_ = ret;
  res.ls_id_ = arg_.get_ls_id();
  FLOG_INFO("[ALTER LS] process", KR(ret), K(arg_));
  return ret;
}

int ObAlterLSCommand::init_(const ObAlterLSArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("fail to assign arg", KR(ret), K(arg));
  } else if (FALSE_IT(sql_proxy_ = GCTX.sql_proxy_)) {
  } else if (OB_FAIL(pre_check_for_zone_deploy_mode_())) {
    LOG_WARN("fail to execute pre_check_for_zone_deploy_mode_", KR(ret));
  } else if (!in_hetero_deploy_mode_ && OB_INVALID_ID != arg_.get_unit_group_id()
      && OB_FAIL(generate_unit_list_for_homo_zone_())) {
    LOG_WARN("fail to execute generate_unit_list_for_homo_zone", KR(ret));
  } else if (in_hetero_deploy_mode_ && arg.has_unit_list()
      && OB_FAIL(check_unit_list_locality_for_hetero_zone_())) {
    LOG_WARN("fail to execute check_unit_list_locality_for_hetero_zone", KR(ret));
  }
  return ret;
}

int ObAlterLSCommand::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_) || OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner stat error", KR(ret), KP(sql_proxy_), K(arg_));
  }
  return ret;
}

int ObAlterLSCommand::pre_check_for_zone_deploy_mode_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  uint64_t meta_tenant_data_version = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(meta_tenant_id, meta_tenant_data_version))) {
    LOG_WARN("fail to get the meta tenant's min data version", KR(ret), K(meta_tenant_id));
  } else if (!ObShareUtil::check_compat_version_for_hetero_zone(meta_tenant_data_version)) {
    is_unit_list_enabled_ = false;
    if (OB_UNLIKELY(arg_.has_unit_list())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("To set UNIT_LIST, meta_tenant_data_version should be [4.2.5.5, 4.3.0.0) or [4.4.2.0, +infinity)",
          KR(ret), K(meta_tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "meta_tenant_data_version is smaller than 4.4.2.0, setting UNIT_LIST is");
    }
  } else {
    is_unit_list_enabled_ = true;
    if (OB_FAIL(ObUnitManager::check_tenant_in_heterogeneous_deploy_mode(tenant_id, in_hetero_deploy_mode_))) {
      LOG_WARN("fail to execute check_tenant_in_heterogeneous_deploy_mode", KR(ret), K(tenant_id));
    } else if (!in_hetero_deploy_mode_ && arg_.has_unit_list()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ZONE_DEPLOY_MODE is HOMO, setting UNIT_LIST is not allowed", KR(ret), K(in_hetero_deploy_mode_), K(arg_));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ZONE_DEPLOY_MODE is homo_mode, setting UNIT_LIST is");
    } else if (in_hetero_deploy_mode_ && OB_INVALID_ID != arg_.get_unit_group_id()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ZONE_DEPLOY_MODE is HETERO, setting UNIT_GROUP is not allowed", KR(ret), K(in_hetero_deploy_mode_), K(arg_));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ZONE_DEPLOY_MODE is hetero_mode, setting UNIT_GROUP is");
    }
  }
  return ret;
}

int ObAlterLSCommand::generate_unit_list_for_homo_zone_()
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator unit_table_operator;
  common::ObArray<ObUnit> units;
  unit_list_.reset();
  const uint64_t unit_group_id = arg_.get_unit_group_id();
  const uint64_t tenant_id = arg_.get_tenant_id();
  ObArray<uint64_t> unit_id_list;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(unit_group_id));
  } else if (0 == unit_group_id) {
    // use empty unit_id_list to init unit_list_
  } else if (OB_FAIL(unit_table_operator.init(*sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", KR(ret));
  } else if (OB_FAIL(unit_table_operator.get_units_by_unit_group_id(tenant_id, unit_group_id, units))) {
    LOG_WARN("fail to execute get_units_by_unit_group_id", KR(ret), K(unit_group_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_OP_NOT_ALLOW;
      (void) print_unavailable_unit_group_error_(unit_group_id);
    }
  } else {
    for (int64_t i = 0; i < units.size() && OB_SUCC(ret); ++i) {
      const ObUnit &unit = units[i];
      if (share::ObUnit::UNIT_STATUS_DELETING == unit.status_) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("unit status should not be deleting", KR(ret), K(unit));
        (void) print_deleting_unit_error_(unit.unit_id_);
      } else if (OB_FAIL(unit_id_list.push_back(unit.unit_id_))) {
        LOG_WARN("fail to push_back", KR(ret), "unit_id", unit.unit_id_, K(unit_id_list));
      }
    }
  }
  if (FAILEDx(unit_list_.init(unit_id_list))) {
    LOG_WARN("fail to init unit_list", KR(ret), K(unit_id_list));
  }
  LOG_INFO("[ALTER LS] generate unit list for homo zone", KR(ret), K(unit_group_id), K(unit_list_));
  return ret;
}

int ObAlterLSCommand::check_unit_list_locality_for_hetero_zone_()
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  common::ObArray<ObUnit> units_in_unit_list;
  const uint64_t tenant_id = arg_.get_tenant_id();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!arg_.has_unit_list())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg_));
  } else if (OB_FAIL(unit_list_.assign(arg_.get_unit_list_arg()))) {
    LOG_WARN("fail to get unit list", KR(ret), K(arg_));
  } else if (unit_list_.has_empty_unit_id_list()) {
    // do nothing
  } else if (OB_FAIL(get_zone_list_and_units_(zone_list, units_in_unit_list))) {
    LOG_WARN("fail to execute get_zone_list_and_units_", KR(ret));
  } else {
    if (unit_list_.get_unit_id_list().count() != units_in_unit_list.count()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("there exists invalid unit_id", KR(ret), K(unit_list_), K(units_in_unit_list));
      (void) print_invalid_unit_error_(units_in_unit_list);
    } else {
      for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); ++i) {
        ObZone &zone = zone_list[i];
        bool zone_match = false;
        for (int64_t j = 0; j < units_in_unit_list.count() && OB_SUCC(ret); ++j) {
          const ObUnit &unit = units_in_unit_list[j];
          if (unit.zone_ == zone) {
            if (share::ObUnit::UNIT_STATUS_DELETING == unit.status_) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("unit status should not be deleting", KR(ret), K(unit));
              (void) print_deleting_unit_error_(unit.unit_id_);
            } else if (zone_match) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("duplicate zone match", KR(ret), K(units_in_unit_list), K(i), K(zone));
              (void) print_zone_error_(zone);
            } else {
              zone_match = true;
            }
          }
        }
      }
    }
    LOG_INFO("[ALTER LS] check unit list for hetero zone", KR(ret), K(zone_list), K(units_in_unit_list));
  }
  return ret;
}

int ObAlterLSCommand::get_zone_list_and_units_(
    ObIArray<ObZone> &zone_list,
    common::ObIArray<ObUnit> &units_in_unit_list)
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  ObArray<ObZone> primary_zone;
  common::ObArray<ObUnit> tenant_unit_array;
  const uint64_t tenant_id = arg_.get_tenant_id();
  zone_list.reset();
  units_in_unit_list.reset();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObLSServiceHelper::get_primary_zone_unit_array(&tenant_schema,
      primary_zone, tenant_unit_array, zone_list))) {
    LOG_WARN("fail to get primary zone unit array", KR(ret));
  } else {
    const ObSEArray<uint64_t, 24> &unit_id_list = unit_list_.get_unit_id_list();
    for (int64_t i = 0; i < tenant_unit_array.count() && OB_SUCC(ret); ++i) {
      const uint64_t uid = tenant_unit_array[i].unit_id_;
      if (has_exist_in_array(unit_id_list, uid) && OB_FAIL(units_in_unit_list.push_back(tenant_unit_array[i]))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(tenant_unit_array));
      }
    }
  }
  return ret;
}

int ObAlterLSCommand::modify_ls_()
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  ObTenantSchema tenant_schema;
  const uint64_t tenant_id = arg_.get_tenant_id();
  const ObZone ls_primary_zone = arg_.get_ls_primary_zone();
  const ObLSID ls_id = arg_.get_ls_id();
  ObLSStatusInfo ls_status_info;
  uint64_t ls_group_id = OB_INVALID_ID;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_ls_info(sql_proxy_, &tenant_schema, tenant_id);
    if (OB_FAIL(tenant_ls_info.gather_stat())) {
      LOG_WARN("fail to gather stat", KR(ret));
    } else if (OB_FAIL(check_modify_ls_(tenant_ls_info, ls_status_info))) {
      LOG_WARN("fail to execute check_modify_ls_", KR(ret), K(tenant_ls_info));
    } else {
      if (!ls_primary_zone.is_empty() && ls_primary_zone != ls_status_info.primary_zone_
          && OB_FAIL(modify_ls_primary_zone_(ls_status_info.primary_zone_, ls_primary_zone, &tenant_schema))) {
        LOG_WARN("fail to execute modify_ls_primary_zone_", KR(ret),
            "old_ls_primary_zone", ls_status_info.primary_zone_, "new_ls_primary_zone", ls_primary_zone);
      } else if (unit_list_.has_unit_id_list()) {
        bool need_update = true;
        if (OB_FAIL(check_if_need_update_unit_(ls_status_info, need_update))) {
          LOG_WARN("fail to execute check_if_need_update_unit_", KR(ret), K(ls_status_info));
        } else if (!need_update) {
          // do nothing
        } else if (OB_FAIL(modify_unit_(ls_status_info, tenant_ls_info, ls_group_id))) {
          LOG_WARN("fail to modify unit", KR(ret), K(ls_status_info));
        }
      }
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, LS_COMMAND, MODIFY_LS, end_ts, ret, end_ts - begin_ts,
      arg_, ls_id.id(), ls_group_id, unit_list_, ls_primary_zone);
  return ret;
}

int ObAlterLSCommand::check_modify_ls_(ObTenantLSInfo &tenant_ls_info, ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  int64_t info_index = 0;
  const ObLSID &ls_id = arg_.get_ls_id();
  const uint64_t unit_group_id = arg_.get_unit_group_id();
  const common::ObZone ls_primary_zone = arg_.get_ls_primary_zone();
  ObArray<common::ObZone> tenant_primary_zone_arr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!tenant_ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_ls_info));
  } else if (OB_ISNULL(tenant_ls_info.get_tenant_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", KR(ret), KP(tenant_ls_info.get_tenant_schema()));
  } else if (OB_UNLIKELY(!tenant_ls_info.get_tenant_schema()->is_normal())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's status is not normal", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant status is not NORMAL. MODIFY LS is");
  } else if (OB_FAIL(tenant_ls_info.get_ls_status_info(ls_id, ls_status_info, info_index))) {
    LOG_WARN("fail to execute get_ls_status_info", KR(ret), K(ls_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified LS does not exist, MODIFY LS is");
    }
  } else if (OB_UNLIKELY(!ls_status_info.is_normal())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the specified LS's status is not normal", KR(ret), K(ls_status_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified LS's status is not normal, MODIFY LS is");
  } else if (OB_FAIL(tenant_ls_info.get_tenant_schema()->get_zone_list(tenant_primary_zone_arr))) {
    LOG_WARN("fail to get tenant primary zone", KR(ret), K(tenant_ls_info));
  } else if (!ls_primary_zone.is_empty() && !is_ls_primary_zone_ok_(tenant_primary_zone_arr, ls_primary_zone)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ls_primary_zone is not in the tenant's zone list", KR(ret), K(tenant_primary_zone_arr), K(ls_primary_zone));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified primary_zone is not in the tenant's zone list, MODIFY LS is");
  } else if (unit_list_.has_empty_unit_id_list()
      && !ls_status_info.get_flag().is_duplicate_ls() && !ls_status_info.get_ls_id().is_sys_ls()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unit related arg is invalid", KR(ret), K(arg_), K(ls_status_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Unit group or unit list is invalid, MODIFY LS is");
  }
  return ret;
}

int ObAlterLSCommand::create_ls_()
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  ObTenantSchema tenant_schema;
  const uint64_t tenant_id = arg_.get_tenant_id();
  ObZone ls_primary_zone;
  uint64_t ls_group_id = OB_INVALID_ID;
  ObLSID new_ls_id;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_ls_info(sql_proxy_, &tenant_schema, tenant_id);
    if (OB_FAIL(tenant_ls_info.gather_stat())) {
      LOG_WARN("failed to gather stat", KR(ret));
    } else if (OB_FAIL((check_create_ls_(tenant_ls_info)))) {
      LOG_WARN("fail to execute check_create_ls_", KR(ret));
    } else if (OB_FAIL(gen_ls_group_id_(tenant_ls_info, ls_group_id))) {
      LOG_WARN("fail to execute gen_ls_group_id_", KR(ret));
    } else if (OB_FAIL(gen_ls_primary_zone_(ls_group_id, tenant_ls_info, ls_primary_zone))) {
      LOG_WARN("fail to execute gen_ls_primary_zone_", KR(ret), K(ls_group_id), K(tenant_ls_info));
    } else if (OB_FAIL(insert_ls_(ls_group_id, ls_primary_zone, tenant_ls_info, new_ls_id))) {
      LOG_WARN("fail to insert ls", KR(ret), K(arg_), K(ls_group_id), K(ls_primary_zone), K(tenant_ls_info));
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, LS_COMMAND, CREATE_LS, end_ts, ret, end_ts - begin_ts,
      arg_, new_ls_id.id(), ls_group_id, unit_list_, ls_primary_zone);
  return ret;
}

int ObAlterLSCommand::check_create_ls_(ObTenantLSInfo &tenant_ls_info)
{
  int ret = OB_SUCCESS;
  bool is_primary = false;
  ObArray<common::ObZone> tenant_primary_zone_arr;
  const uint64_t tenant_id = arg_.get_tenant_id();
  const common::ObZone &ls_primary_zone = arg_.get_ls_primary_zone();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObShareUtil::table_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
    LOG_WARN("fail to execute mtl_check_if_tenant_role_is_primary", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_primary)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is not primary", KR(ret), K(tenant_id), K(is_primary));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant role is not primary, CREATE LS is");
  } else if (OB_ISNULL(tenant_ls_info.get_tenant_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", KR(ret), KP(tenant_ls_info.get_tenant_schema()));
  } else if (OB_UNLIKELY(!tenant_ls_info.get_tenant_schema()->is_normal())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's status is not normal", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant status is not NORMAL. CREATE LS is");
  } else if (OB_FAIL(tenant_ls_info.get_tenant_schema()->get_zone_list(tenant_primary_zone_arr))) {
    LOG_WARN("fail to get tenant primary zone", KR(ret), K(tenant_ls_info));
  } else if (!ls_primary_zone.is_empty() && !is_ls_primary_zone_ok_(tenant_primary_zone_arr, ls_primary_zone)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ls_primary_zone is not in the tenant's zone list",
        KR(ret), K(tenant_ls_info), K(ls_primary_zone));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW,
        "The specified primary_zone is not in the tenant's zone list, CREATE LS is");
  } else if (unit_list_.has_empty_unit_id_list()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unit related arg is invalid", KR(ret), K(arg_));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Unit group or unit list is invalid, CREATE LS is");
  }
  return ret;
}

bool ObAlterLSCommand::is_ls_primary_zone_ok_(
    const ObIArray<common::ObZone> &tenant_primary_zone_arr,
    const common::ObZone &ls_primary_zone)
{
  bool bret = false;
  for (int64_t i = 0; i < tenant_primary_zone_arr.count() && !bret; ++i) {
    if (ls_primary_zone == tenant_primary_zone_arr.at(i)) {
      bret = true;
    }
  }
  return bret;
}

int ObAlterLSCommand::try_get_exist_ls_group_id_(ObTenantLSInfo &tenant_ls_info, uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  const ObLSGroupInfoArray &ls_group_info_arr = tenant_ls_info.get_ls_group_array();
  ls_group_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!is_unit_list_enabled_) {
    const uint64_t unit_group_id = arg_.get_unit_group_id();
    if (0 == unit_group_id) {
      ls_group_id = 0; // duplicate LS
    } else {
      for (int64_t i = 0; i < ls_group_info_arr.count() && OB_INVALID_ID == ls_group_id; ++i) {
        if (unit_group_id == ls_group_info_arr.at(i).unit_group_id_) {
          ls_group_id = ls_group_info_arr.at(i).ls_group_id_;
        }
      }
    }
  } else if (is_unit_list_enabled_) {
    if (unit_list_.has_empty_unit_id_list()) {
      ls_group_id = 0;
    } else {
      for (int64_t i = 0; i < ls_group_info_arr.count() && OB_INVALID_ID == ls_group_id; ++i) {
        const ObLSGroupInfo &lsg_info = ls_group_info_arr[i];
        if (is_same_unit_id_list_(lsg_info.get_unit_list())) {
          ls_group_id = lsg_info.get_ls_group_id();
          LOG_INFO("matched LS", KR(ret), K(lsg_info), K(unit_list_));
        }
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_GEN_LS_GROUP_ID);
int ObAlterLSCommand::gen_ls_group_id_(ObTenantLSInfo &tenant_ls_info, uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  bool has_fetched_new_ls_group_id = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!unit_list_.has_unit_id_list())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected unit_list_", KR(ret), K(unit_list_));
  } else if (OB_UNLIKELY(ERRSIM_GEN_LS_GROUP_ID)) {
    ls_group_id = -ERRSIM_GEN_LS_GROUP_ID;
    FLOG_INFO("ERRSIM_GEN_LS_GROUP_ID opened", K(tenant_id), K(arg_), K(ls_group_id));
  } else if (OB_FAIL(try_get_exist_ls_group_id_(tenant_ls_info, ls_group_id))) {
    LOG_WARN("fail to execute try_get_exist_ls_group_id", KR(ret));
  } else if (OB_INVALID_ID == ls_group_id) {
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_id, ls_group_id))) {
      LOG_WARN("fail to fetch new LS group id", KR(ret), K(tenant_id));
    }
    has_fetched_new_ls_group_id = true;
  }
  FLOG_INFO("[ALTER LS] generate ls group id", KR(ret), K(arg_),
      K(is_unit_list_enabled_), K(unit_list_), K(has_fetched_new_ls_group_id));
  return ret;
}

int ObAlterLSCommand::check_if_need_update_unit_(const ObLSStatusInfo &ls_status_info, bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = true;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_status_info.is_valid() || ls_status_info.get_ls_id() != arg_.get_ls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg_), K(ls_status_info));
  } else if (!is_unit_list_enabled_) {
    const uint64_t old_unit_group_id = ls_status_info.get_unit_group_id();
    const uint64_t new_unit_group_id = arg_.get_unit_group_id();
    if (old_unit_group_id == new_unit_group_id) {
      need_update = false;
    }
  } else if (is_same_unit_id_list_(ls_status_info.get_unit_list())) {
    need_update = false;
  }
  LOG_INFO("check if need update unit", KR(ret), K(need_update), K(arg_), K(ls_status_info), K(is_unit_list_enabled_));
  return ret;
}

int ObAlterLSCommand::modify_unit_(
    const ObLSStatusInfo &ls_status_info,
    ObTenantLSInfo &tenant_ls_info,
    uint64_t &new_ls_group_id)
{
  int ret = OB_SUCCESS;
  const uint64_t old_ls_group_id = ls_status_info.get_ls_group_id();
  const uint64_t tenant_id = arg_.get_tenant_id();
  const ObLSID &ls_id = arg_.get_ls_id();
  new_ls_group_id = old_ls_group_id; // no need to get new ls_group_id if the tenant is non-primary
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_status_info.is_valid() || ls_status_info.get_ls_id() != arg_.get_ls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg_), K(ls_status_info));
  } else {
    share::ObLSStatusOperator status_op;
    START_TRANSACTION(sql_proxy_, gen_meta_tenant_id(tenant_id));
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant's switchover status is not normal, MODIFY LS is");
    } else if (tenant_info.is_primary() && !ls_status_info.ls_id_.is_sys_ls()) {
      // only update ls_group_id of user ls
      if (OB_FAIL(gen_ls_group_id_(tenant_ls_info, new_ls_group_id))) {
        LOG_WARN("fail to execute gen_ls_group_id_", KR(ret), K(tenant_id));
      } else if (OB_FAIL(update_ls_group_id_in_all_ls_(old_ls_group_id, new_ls_group_id))) {
        LOG_WARN("fail to execute update_ls_group_id_in_all_ls_", KR(ret), K(old_ls_group_id), K(new_ls_group_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_unit_list_enabled_) {
      const uint64_t old_unit_group_id = ls_status_info.get_unit_group_id();
      const uint64_t new_unit_group_id = arg_.get_unit_group_id();
      if (OB_FAIL(status_op.alter_ls_group_id(tenant_id, ls_id, old_ls_group_id, new_ls_group_id,
          old_unit_group_id, new_unit_group_id, trans))) {
        LOG_WARN("fail to alter ls group", KR(ret), K(tenant_id), K(ls_id), K(new_ls_group_id),
            K(old_ls_group_id), K(new_unit_group_id), K(old_unit_group_id));
      }
    } else { // is_unit_list_enabled_ = true
      ObUnitIDList new_unit_list;
      if (OB_FAIL(get_unit_id_list_(new_unit_list))) {
        LOG_WARN("fail to get unit id list", KR(ret));
      } else if (OB_FAIL(status_op.alter_ls_group_id(tenant_id, ls_id, old_ls_group_id, new_ls_group_id,
          ls_status_info.get_unit_list(), new_unit_list, trans))) {
        LOG_WARN("fail to alter ls group", KR(ret), K(tenant_id), K(ls_id), K(new_ls_group_id),
            "old_unit_list", ls_status_info.get_unit_list(), K(new_unit_list));
      }
    }
    END_TRANSACTION(trans);
  }

  FLOG_INFO("[ALTER LS] modify unit_group_id", KR(ret), K(tenant_info),
      K(ls_status_info), K(arg_), K(new_ls_group_id));
  return ret;
}

int ObAlterLSCommand::update_ls_group_id_in_all_ls_(const uint64_t old_ls_group_id, const uint64_t new_ls_group_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  const share::ObLSID &ls_id = arg_.get_ls_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == old_ls_group_id || OB_INVALID_ID == new_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_), K(old_ls_group_id), K(new_ls_group_id));
  } else {
    START_TRANSACTION(sql_proxy_, tenant_id);
    ObLSAttrOperator ls_op(tenant_id, sql_proxy_);
    share::ObLSAttr ls_attr;
    int64_t task_cnt = 0;
    if (FAILEDx(ObInnerTableLockUtil::lock_inner_table_in_trans(
        trans,
        tenant_id,
        OB_ALL_BALANCE_JOB_TID,
        EXCLUSIVE, false))) {
      LOG_WARN("lock inner table failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObBalanceTaskTableOperator::get_ls_task_cnt(tenant_id, ls_id, task_cnt, trans))) {
      LOG_WARN("fail to execute get_ls_task_cnt", KR(ret), K(tenant_id), K(ls_id));
    } else if (task_cnt > 0) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the specified LS has balance task", KR(ret), K(tenant_id), K(ls_id), K(task_cnt));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified LS has balance tasks, MODIFY LS is");
    } else if (OB_FAIL(ls_op.get_ls_attr(ls_id, true, trans, ls_attr))) {
      LOG_WARN("fail to get ls attr", KR(ret), K(ls_id));
    } else if (OB_UNLIKELY(ls_attr.get_ls_group_id() != old_ls_group_id)) {
      ret = OB_NEED_RETRY;
      LOG_WARN("concurrent modification occured", KR(ret), K(ls_attr), K(old_ls_group_id));
    } else if (OB_FAIL(ls_op.alter_ls_group_in_trans(ls_attr, new_ls_group_id, trans))) {
      LOG_WARN("fail to alter ls group in trans", KR(ret), K(ls_attr), K(new_ls_group_id));
    }
    END_TRANSACTION(trans);
  }
  return ret;
}

int ObAlterLSCommand::modify_ls_primary_zone_(
    const common::ObZone &old_ls_primary_zone,
    const common::ObZone &new_ls_primary_zone,
    share::schema::ObTenantSchema *tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  const share::ObLSID &ls_id = arg_.get_ls_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(old_ls_primary_zone.is_empty() || new_ls_primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_), K(old_ls_primary_zone),  K(new_ls_primary_zone));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema  is null", KR(ret), KP(tenant_schema));
  } else {
    ObSqlString new_zone_priority;
    ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(new_ls_primary_zone, *tenant_schema, new_zone_priority))) {
      LOG_WARN("fail to get normalize primary zone", KR(ret), K(new_ls_primary_zone) ,KP(tenant_schema));
    } else if (OB_FAIL(ls_life_agent.update_ls_primary_zone(tenant_id, ls_id,
        new_ls_primary_zone, new_zone_priority.string()))) {
      LOG_WARN("fail to execute update_ls_primary_zone_in_trans", KR(ret), K(tenant_id),
          K(new_ls_primary_zone), K(new_zone_priority));
    }
  }
  FLOG_INFO("[ALTER LS] modify ls_primary_zone", KR(ret), K(arg_), K(new_ls_primary_zone), K(old_ls_primary_zone));
  return ret;
}

int ObAlterLSCommand::gen_ls_primary_zone_(const uint64_t &ls_group_id, ObTenantLSInfo &tenant_ls_info, ObZone &ls_primary_zone)
{
  int ret = OB_SUCCESS;
  ObLSGroupInfo group_info;
  if (OB_UNLIKELY(OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group_id));
  } else if (OB_FAIL(ls_primary_zone.assign(arg_.get_ls_primary_zone()))) {
    LOG_WARN("fail to assign ls_primary_zone", KR(ret));
  } else if (!ls_primary_zone.is_empty()) {
    // ls_primary_zone has been set
  } else if (OB_FAIL(tenant_ls_info.get_ls_group_info(ls_group_id, group_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      const ObZone &candidate_zone = tenant_ls_info.get_primary_zone().at(0);
      if (OB_FAIL(ls_primary_zone.assign(candidate_zone))) {
      LOG_WARN("fail to assign ls_primary_zone", KR(ret), K(candidate_zone));
      }
    } else {
      LOG_WARN("fail to get ls_group_info", KR(ret), K(ls_group_id), K(tenant_ls_info));
    }
  } else if (OB_FAIL(tenant_ls_info.get_next_primary_zone(group_info, ls_primary_zone))) {
    LOG_WARN("fail to execute get_next_primary_zone", KR(ret), K(group_info));
  }
  FLOG_INFO("[ALTER LS] generate ls_primary_zone", KR(ret), K(arg_), K(ls_group_id), K(ls_primary_zone), K(group_info));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_INSERT_LS);
int ObAlterLSCommand::insert_ls_(
    const uint64_t ls_group_id,
    const ObZone& ls_primary_zone,
    ObTenantLSInfo &tenant_ls_info,
    ObLSID &new_ls_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.get_tenant_id();
  ObAllTenantInfo tenant_info;
  ObMySQLTransaction trans;
  uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
  share::ObLSAttr new_ls;
  share::ObLSFlag flag;
  share::ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
  share::ObLSStatusInfo new_ls_info;
  const uint64_t unit_group_id = !is_unit_list_enabled_ ? arg_.get_unit_group_id() : 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY((OB_INVALID_ID == arg_.get_unit_group_id() && !unit_list_.has_unit_id_list())
      || OB_INVALID_ID == ls_group_id || ls_primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg_), K(unit_group_id), K(unit_list_), K(ls_group_id),
        K(ls_primary_zone));
  } else if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_info.is_primary())) {
    // double check
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is not primary", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant role is not primary, CREATE LS is");
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant's switchover status is not normal, CREATE LS is");
  } else if (OB_FAIL(ObLSServiceHelper::create_ls_in_user_tenant(tenant_id, ls_group_id, flag, ls_operator, new_ls))) {
    LOG_WARN("fail to insert into __all_ls table", KR(ret), K(tenant_id), K(ls_group_id));
  } else if (OB_UNLIKELY(ERRSIM_INSERT_LS)) {
    ret = ERRSIM_INSERT_LS;
    LOG_WARN("ERRSIM_INSERT_LS opened", KR(ret));
  } else {
    ObUnitIDList new_unit_list;
    if (is_unit_list_enabled_ && OB_FAIL(get_unit_id_list_(new_unit_list))) {
      LOG_WARN("fail to get unit id list", KR(ret));
    } else if (OB_FAIL(new_ls_info.init(tenant_id, new_ls.get_ls_id(), ls_group_id, share::OB_LS_CREATING,
          unit_group_id, ls_primary_zone, new_ls.get_ls_flag(), new_unit_list))) {
        LOG_WARN("failed to init new info", KR(ret), K(tenant_id), K(unit_group_id),
            K(new_ls), K(ls_group_id), K(ls_primary_zone));
    } else if (OB_FAIL(ObLSServiceHelper::life_agent_create_new_ls_in_trans(new_ls_info,
        new_ls.get_create_scn(), tenant_info.get_switchover_epoch(), tenant_ls_info, trans))) {
      LOG_WARN("fail to execute life_agent_create_new_ls_in_trans", KR(ret), K(new_ls),
          K(ls_group_id), K(unit_group_id), K(ls_primary_zone), K(tenant_ls_info));
    }
  }
  new_ls_id = new_ls.get_ls_id();
  END_TRANSACTION(trans);
  if (OB_FAIL(ret) && new_ls.is_valid()) {
    int original_ret = ret;
    ret = OB_PARTIAL_FAILED;
    (void) print_insert_ls_error_(new_ls_id, original_ret);
    LOG_WARN("fail to insert ls", KR(ret), K(original_ret));
  }
  return ret;
}

int ObAlterLSCommand::get_unit_id_list_(ObUnitIDList &unit_id_list)
{
  int ret = OB_SUCCESS;
  unit_id_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_list_.get_unit_id_list().count(); ++i) {
    ObDisplayUnitID uid(unit_list_.get_unit_id_list()[i]);
    if (OB_FAIL(unit_id_list.push_back(uid))) {
      LOG_WARN("fail to push back", KR(ret), K(uid), K(unit_id_list));
    }
  }
  return ret;
}

bool ObAlterLSCommand::is_same_unit_id_list_(const ObUnitIDList &old_unit_id_list)
{
  int same = true;
  const ObSEArray<uint64_t, 24> &new_unit_id_list = unit_list_.get_unit_id_list();
  if (new_unit_id_list.count() != old_unit_id_list.count()) {
    same = false;
  } else {
    for (int64_t i = 0; i < old_unit_id_list.count() && same; ++i) {
      const uint64_t uid = old_unit_id_list[i].id();
      if (!has_exist_in_array(new_unit_id_list, uid)) {
        same = false;
      }
    }
  }
  return same;
}

#define DECLARE_ERR_VARS() \
    const int64_t ERR_MSG_BUF_LEN = 1024; \
    char err_msg[ERR_MSG_BUF_LEN] = ""; \
    int64_t pos = 0; \
    const char * const op_str = arg_.alter_ls_op_to_str()

int ObAlterLSCommand::print_insert_ls_error_(const ObLSID &ls_id, const int orig_ret)
{
  int ret = OB_SUCCESS;
  DECLARE_ERR_VARS();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", KR(ret), K(ls_id));
  } else if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_BUF_LEN, pos,
      "Partially failed to execute CREATE LS (new_ls_id = %ld), original return code: %d", ls_id.id(), orig_ret))) {
    LOG_WARN("fail to execute databuff_printf", KR(ret), K(ls_id));
  } else {
    LOG_USER_ERROR(OB_PARTIAL_FAILED, err_msg);
  }
  return ret;
}

int ObAlterLSCommand::print_deleting_unit_error_(const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  DECLARE_ERR_VARS();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_BUF_LEN, pos,
      "Unit %lu's status is DELETING, %s is", unit_id, op_str))) {
    LOG_WARN("failed to printf to err_msg", KR(ret), K(unit_id), K(op_str));
  } else {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}

int ObAlterLSCommand::print_invalid_unit_error_(const common::ObIArray<ObUnit> &units_in_unit_list)
{
  int ret = OB_SUCCESS;
  DECLARE_ERR_VARS();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObSEArray<uint64_t, 24> &unit_id_list = unit_list_.get_unit_id_list();
    for (int64_t i = 0; i < unit_id_list.count() && OB_SUCC(ret); ++i) {
      const uint64_t uid = unit_id_list[i];
      bool found = false;
      for (int64_t j = 0; j < units_in_unit_list.count() && !found && OB_SUCC(ret); ++j) {
        const ObUnit &unit = units_in_unit_list.at(j);
        if (unit.unit_id_ == uid) {
          found = true;
        }
      }
      if (OB_SUCC(ret) && !found) {
        if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_BUF_LEN, pos, "Invalid unit %lu exists, %s is", uid, op_str))) {
          LOG_WARN("failed to printf to err_msg", KR(ret), K(op_str));
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
        }
      }
    }
  }
  return ret;
}

int ObAlterLSCommand::print_unavailable_unit_group_error_(const uint64_t unit_group_id)
{
  int ret = OB_SUCCESS;
  DECLARE_ERR_VARS();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_BUF_LEN, pos,
      "UNIT_GROUP %lu is not available for this tenant, %s is", unit_group_id, op_str))) {
    LOG_WARN("failed to printf to err_msg", KR(ret), K(arg_), K(op_str));
  } else {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}

int ObAlterLSCommand::print_zone_error_(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  DECLARE_ERR_VARS();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_BUF_LEN, pos,
      "Zone '%s' has too many units in the given unit list, %s is", zone.ptr(), op_str))) {
    LOG_WARN("failed to printf to err_msg", KR(ret), K(zone), K(op_str));
  } else {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
  return ret;
}
} // rootserver
} // oceanbase
