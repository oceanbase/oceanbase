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
  ObLSID ls_id = arg.get_ls_id();
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (arg.is_create_service() && OB_FAIL(create_ls_(arg))) {
    LOG_WARN("fail to create ls", KR(ret), K(arg));
  } else if (arg.is_modify_service() && OB_FAIL(modify_ls_(arg))) {
    LOG_WARN("fail to modify ls", KR(ret), K(arg));
  } else if (arg.is_drop_service()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("fail to drop ls", KR(ret), K(arg));
  }
  res.ret_ = ret;
  res.ls_id_ = ls_id;
  FLOG_INFO("[ALTER LS] process", KR(ret), K(arg));
  // TODO(linqiuce.lqc): add tenant event
  return ret;
}

int ObAlterLSCommand::modify_ls_(const ObAlterLSArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  ObTenantSchema tenant_schema;
  const uint64_t tenant_id = arg.get_tenant_id();
  const uint64_t unit_group_id = arg.get_unit_group_id();
  const ObZone ls_primary_zone = arg.get_ls_primary_zone();
  const ObLSID ls_id = arg.get_ls_id();
  ObLSStatusInfo ls_status_info;
  uint64_t ls_group_id = OB_INVALID_ID;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_ls_info(GCTX.sql_proxy_, &tenant_schema, tenant_id);
    if (OB_FAIL(tenant_ls_info.gather_stat())) {
      LOG_WARN("fail to gather stat", KR(ret));
    } else if (OB_FAIL(check_modify_ls_(ls_id, unit_group_id, ls_primary_zone, tenant_ls_info, ls_status_info))) {
      LOG_WARN("fail to execute  check_modify_ls_", KR(ret), K(unit_group_id), K(ls_primary_zone),
          K(tenant_ls_info));
    } else {
      if (OB_INVALID_ID != unit_group_id && unit_group_id != ls_status_info.unit_group_id_
          && OB_FAIL(modify_unit_group_id_(tenant_id, ls_id, ls_status_info.unit_group_id_, unit_group_id,
              ls_status_info.ls_group_id_, ls_group_id, tenant_ls_info.get_ls_group_array()))) {
        LOG_WARN("fail to execute modify_unit_group_id_", KR(ret),  K(tenant_id), K(ls_id),
            "old_unit_group_id", ls_status_info.unit_group_id_, "new_unit_group_id", unit_group_id);
      } else if (!ls_primary_zone.is_empty() && ls_primary_zone != ls_status_info.primary_zone_
          && OB_FAIL(modify_ls_primary_zone_(tenant_id, ls_id, ls_status_info.primary_zone_,
              ls_primary_zone, &tenant_schema))) {
          LOG_WARN("fail to execute modify_ls_primary_zone_", KR(ret), K(tenant_id), K(ls_id),
              "old_ls_primary_zone", ls_status_info.primary_zone_, "new_ls_primary_zone", ls_primary_zone);
      }
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, LS_COMMAND, MODIFY_LS, end_ts, ret, end_ts - begin_ts,
      arg, ls_id.id(), ls_group_id, unit_group_id, ls_primary_zone);
  return ret;
}

int ObAlterLSCommand::check_modify_ls_(
    const ObLSID &ls_id,
    const uint64_t unit_group_id,
    const common::ObZone &ls_primary_zone,
    ObTenantLSInfo &tenant_ls_info,
    ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  int64_t info_index = 0;
  ObArray<common::ObZone> tenant_primary_zone_arr;
  if (OB_UNLIKELY(!tenant_ls_info.is_valid()
      || !ls_id.is_valid_with_tenant(tenant_ls_info.get_tenant_id())
      || (OB_INVALID_ID == unit_group_id && ls_primary_zone.is_empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_ls_info),
        K(ls_id), K(unit_group_id), K(ls_primary_zone));
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
  } else if (OB_INVALID_ID != unit_group_id && !is_unit_group_ok_(tenant_ls_info.get_unit_group_array(), unit_group_id, ls_status_info)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unit_group does not exists or not active", KR(ret), K(tenant_ls_info), K(unit_group_id), K(ls_status_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified unit_group is not active or does not exist, MODIFY LS is");
  }
  return ret;
}

int ObAlterLSCommand::create_ls_(const ObAlterLSArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  ObTenantSchema tenant_schema;
  const uint64_t tenant_id = arg.get_tenant_id();
  const uint64_t unit_group_id = arg.get_unit_group_id();
  ObZone ls_primary_zone;
  uint64_t ls_group_id = OB_INVALID_ID;
  ObLSID new_ls_id;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_ls_info(GCTX.sql_proxy_, &tenant_schema, tenant_id);
    if (OB_FAIL(tenant_ls_info.gather_stat())) {
      LOG_WARN("failed to gather stat", KR(ret));
    } else if (OB_FAIL((check_create_ls_(tenant_id, unit_group_id, arg.get_ls_primary_zone(), tenant_ls_info)))) {
      LOG_WARN("fail to execute check_create_ls_", KR(ret), K(arg));
    } else if (OB_FAIL(gen_ls_group_id_(tenant_id, unit_group_id, tenant_ls_info.get_ls_group_array(), ls_group_id))) {
      LOG_WARN("fail to execute gen_ls_group_id_", KR(ret), K(tenant_id), K(unit_group_id), K(tenant_ls_info));
    } else if (!arg.get_ls_primary_zone().is_empty()
        && OB_FAIL(ls_primary_zone.assign(arg.get_ls_primary_zone()))) {
      LOG_WARN("fail to assign ls_primary_zone", KR(ret), K(arg));
    } else if (arg.get_ls_primary_zone().is_empty()
        &&OB_FAIL(gen_ls_primary_zone_(ls_group_id, tenant_ls_info, ls_primary_zone))) {
      LOG_WARN("fail to execute gen_ls_primary_zone_", KR(ret), K(ls_group_id), K(tenant_ls_info));
    } else if (OB_FAIL(insert_ls_(tenant_id, unit_group_id, ls_group_id,
        ls_primary_zone, tenant_ls_info, new_ls_id))) {
      LOG_WARN("fail to insert ls", KR(ret), K(tenant_id), K(unit_group_id), K(ls_group_id),
          K(ls_primary_zone), K(tenant_ls_info));
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, LS_COMMAND, CREATE_LS, end_ts, ret, end_ts - begin_ts,
      arg, new_ls_id.id(), ls_group_id, unit_group_id, ls_primary_zone);
  return ret;
}
int ObAlterLSCommand::check_create_ls_(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    const common::ObZone &ls_primary_zone,
    ObTenantLSInfo &tenant_ls_info)
{
  int ret = OB_SUCCESS;
  bool is_primary = false;
  ObArray<common::ObZone> tenant_primary_zone_arr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id)
      || OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(unit_group_id));
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
  } else if (!is_unit_group_ok_(tenant_ls_info.get_unit_group_array(), unit_group_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unit_group does not exists or not active", KR(ret), K(tenant_ls_info), K(unit_group_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The specified unit_group is not active or does not exist, CREATE LS is");
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

bool ObAlterLSCommand::is_unit_group_ok_(
    ObUnitGroupInfoArray &tenant_unit_group_arr,
    const uint64_t unit_group_id)
{
  bool bret = false;
  for (int64_t i = 0; i < tenant_unit_group_arr.count() && !bret; ++i) {
    if (unit_group_id == tenant_unit_group_arr.at(i).unit_group_id_
        && share::ObUnit::UNIT_STATUS_ACTIVE == tenant_unit_group_arr.at(i).unit_status_) {
      bret = true;
    }
  }
  return bret;
}
bool ObAlterLSCommand::is_unit_group_ok_(
    ObUnitGroupInfoArray &tenant_unit_group_arr,
    const uint64_t unit_group_id,
    const ObLSStatusInfo &ls_status_info)
{
  bool bret = false;
  if (0 == unit_group_id && (ls_status_info.get_flag().is_duplicate_ls() || ls_status_info.get_ls_id().is_sys_ls())) {
    bret = true;
  }
  if (!bret) {
    bret = is_unit_group_ok_(tenant_unit_group_arr, unit_group_id);
  }
  return bret;
}
ERRSIM_POINT_DEF(ERRSIM_GEN_LS_GROUP_ID);
int ObAlterLSCommand::gen_ls_group_id_(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    ObLSGroupInfoArray &ls_group_info_arr,
    uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  ls_group_id = OB_INVALID_ID;
  bool has_fetched_new_ls_group_id = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(unit_group_id));
  } else if (OB_UNLIKELY(ERRSIM_GEN_LS_GROUP_ID)) {
    ls_group_id = -ERRSIM_GEN_LS_GROUP_ID;
    FLOG_INFO("ERRSIM_GEN_LS_GROUP_ID opened", K(tenant_id), K(unit_group_id), K(ls_group_id));
  } else {
    if (0 == unit_group_id) {
      ls_group_id = 0; // duplicate LS
    }
    for (int64_t i = 0; i < ls_group_info_arr.count() && OB_INVALID_ID == ls_group_id; ++i) {
      if (ls_group_info_arr.at(i).unit_group_id_ == unit_group_id) {
        ls_group_id = ls_group_info_arr.at(i).ls_group_id_;
      }
    }
    if (OB_INVALID_ID == ls_group_id) {
      if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(GCTX.sql_proxy_, tenant_id, ls_group_id))) {
        // GCTX.sql_proxy_ has been checked whether it's null in the function
        LOG_WARN("failed to fetch new LS group id", KR(ret), K(tenant_id));
      }
      has_fetched_new_ls_group_id = true;
    }
  }
  FLOG_INFO("[ALTER LS] generate ls group id", KR(ret), K(tenant_id), K(unit_group_id),
      K(ls_group_id), K(has_fetched_new_ls_group_id));
  return ret;
}

int ObAlterLSCommand::modify_unit_group_id_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const uint64_t old_unit_group_id,
    const uint64_t new_unit_group_id,
    const uint64_t old_ls_group_id,
    uint64_t &new_ls_group_id,
    ObLSGroupInfoArray &ls_group_info_arr)
{
  int ret = OB_SUCCESS;
  new_ls_group_id = old_ls_group_id; // no need to get new ls_group_id if the tenant is non-primary
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid_with_tenant(tenant_id)
      || OB_INVALID_ID == old_unit_group_id || OB_INVALID_ID == new_unit_group_id
      || OB_INVALID_ID == old_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(old_unit_group_id),
        K(new_unit_group_id), K(old_ls_group_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    share::ObLSStatusOperator status_op;
    START_TRANSACTION(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id));
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the tenant's switchover status is not normal", KR(ret), K(tenant_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant's switchover status is not normal, MODIFY LS is");
    } else if (tenant_info.is_primary()) {
      if (OB_FAIL(gen_ls_group_id_(tenant_id, new_unit_group_id, ls_group_info_arr, new_ls_group_id))) {
        LOG_WARN("fail to execute gen_ls_group_id_", KR(ret), K(tenant_id), K(new_unit_group_id), K(ls_group_info_arr));
      } else if (OB_FAIL(update_ls_group_id_in_all_ls_(tenant_id, ls_id, old_ls_group_id, new_ls_group_id))) {
        LOG_WARN("fail to execute update_ls_group_id_in_all_ls_", KR(ret), K(ls_id), K(new_ls_group_id));
      }
    }
    if (FAILEDx(status_op.alter_ls_group_id(tenant_id, ls_id, old_ls_group_id, new_ls_group_id,
        old_unit_group_id, new_unit_group_id, trans))) {
      LOG_WARN("fail to alter ls group", KR(ret), K(tenant_id), K(ls_id), K(new_ls_group_id),
          K(old_ls_group_id), K(new_unit_group_id), K(old_unit_group_id));
    }
    END_TRANSACTION(trans);
  }

  FLOG_INFO("[ALTER LS] modify unit_group_id", KR(ret), K(tenant_id), K(tenant_info), K(ls_id),
      K(new_unit_group_id), K(old_unit_group_id), K(new_ls_group_id), K(old_ls_group_id));
  return ret;
}

int ObAlterLSCommand::update_ls_group_id_in_all_ls_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const uint64_t old_ls_group_id,
    const uint64_t new_ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid_with_tenant(tenant_id))
      || OB_INVALID_ID == old_ls_group_id || OB_INVALID_ID == new_ls_group_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(old_ls_group_id), K(new_ls_group_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    START_TRANSACTION(GCTX.sql_proxy_, tenant_id);
    ObLSAttrOperator ls_op(tenant_id, GCTX.sql_proxy_);
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
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObZone &old_ls_primary_zone,
    const common::ObZone &new_ls_primary_zone,
    share::schema::ObTenantSchema *tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid_with_tenant(tenant_id)
      || old_ls_primary_zone.is_empty() || new_ls_primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(old_ls_primary_zone),
        K(new_ls_primary_zone));
  } else if (OB_ISNULL(tenant_schema) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema or GCTX.sql_proxy_ is null", KR(ret), KP(tenant_schema), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString new_zone_priority;
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(new_ls_primary_zone, *tenant_schema, new_zone_priority))) {
      LOG_WARN("fail to get normalize primary zone", KR(ret), K(new_ls_primary_zone) ,KP(tenant_schema));
    } else if (OB_FAIL(ls_life_agent.update_ls_primary_zone(tenant_id, ls_id,
        new_ls_primary_zone, new_zone_priority.string()))) {
      LOG_WARN("fail to execute update_ls_primary_zone_in_trans", KR(ret), K(tenant_id),
          K(new_ls_primary_zone), K(new_zone_priority));
    }
  }
  FLOG_INFO("[ALTER LS] modify ls_primary_zone", KR(ret), K(tenant_id), K(ls_id), K(new_ls_primary_zone), K(old_ls_primary_zone));
  return ret;
}

int ObAlterLSCommand::gen_ls_primary_zone_(const uint64_t &ls_group_id, ObTenantLSInfo &tenant_ls_info, ObZone &ls_primary_zone)
{
  int ret = OB_SUCCESS;
  ObLSGroupInfo group_info;
  if (OB_UNLIKELY(OB_INVALID_ID == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group_id));
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
  FLOG_INFO("[ALTER LS] generate ls_primary_zone", KR(ret), K(ls_group_id), K(ls_primary_zone), K(group_info));
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_INSERT_LS);
int ObAlterLSCommand::insert_ls_(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    const uint64_t ls_group_id,
    const ObZone& ls_primary_zone,
    ObTenantLSInfo &tenant_ls_info,
    ObLSID &new_ls_id)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObMySQLTransaction trans;
  uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
  share::ObLSAttr new_ls;
  share::ObLSFlag flag;
  share::ObLSAttrOperator ls_operator(tenant_id, GCTX.sql_proxy_);
  share::ObLSStatusInfo new_ls_info;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id)
      || OB_INVALID_ID == unit_group_id || OB_INVALID_ID == ls_group_id
      || ls_primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(unit_group_id), K(ls_group_id),
        K(ls_primary_zone));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, exec_tenant_id))) {
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
  } else if (OB_FAIL(new_ls_info.init(tenant_id, new_ls.get_ls_id(), ls_group_id, share::OB_LS_CREATING,
        unit_group_id, ls_primary_zone, new_ls.get_ls_flag()))) {
      LOG_WARN("failed to init new info", KR(ret), K(tenant_id), K(unit_group_id),
          K(new_ls), K(ls_group_id), K(ls_primary_zone));
  } else if (OB_FAIL(ObLSServiceHelper::life_agent_create_new_ls_in_trans(new_ls_info,
      new_ls.get_create_scn(), tenant_info.get_switchover_epoch(), tenant_ls_info, trans))) {
    LOG_WARN("fail to execute life_agent_create_new_ls_in_trans", KR(ret), K(new_ls),
        K(ls_group_id), K(unit_group_id), K(ls_primary_zone), K(tenant_ls_info));
  }
  new_ls_id = new_ls.get_ls_id();
  END_TRANSACTION(trans);
  if (OB_FAIL(ret) && new_ls.is_valid()) {
    int original_ret = ret;
    ret = OB_PARTIAL_FAILED;
    (void) print_insert_ls_error(tenant_id, new_ls_id, original_ret);
    LOG_WARN("fail to insert ls", KR(ret), K(original_ret));
  }
  return ret;
}

int ObAlterLSCommand::print_insert_ls_error(const uint64_t tenant_id, const ObLSID &ls_id, const int orig_ret)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", KR(ret), K(tenant_id), K(ls_id));
  } else {
    const int64_t ERR_MSG_BUF_LEN = 256;
    char err_msg[ERR_MSG_BUF_LEN] = "";
    int64_t pos = 0;
    if (OB_FAIL(databuff_print_multi_objs(err_msg, ERR_MSG_BUF_LEN, pos,
        "fail to execute CREATE LS ", ls_id.id(), " for tenant ", tenant_id,
        ", original return code: ", orig_ret))) {
      LOG_WARN("fail to execute databuff_printf", KR(ret), K(tenant_id),  K(ls_id));
    } else {
      LOG_USER_ERROR(OB_PARTIAL_FAILED, err_msg);
    }
  }
  return ret;
}

} // rootserver
} // oceanbase