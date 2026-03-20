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

#define USING_LOG_PREFIX SHARE

#include "share/ob_sync_standby_dest_utils.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "share/ob_share_util.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/standby/ob_protection_mode_utils.h"
namespace oceanbase
{
namespace share
{
const char *const OB_STR_SYNC_STANDBY_DEST = "sync_standby_dest";

static int notify_clear_sync_standby_dest_cache(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObAddr leader;
  obrpc::ObClearSyncStandbyDestCacheArg arg;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service or srv rpc proxy is null", KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(
      GCONF.cluster_id, meta_tenant_id, SYS_LS, false, leader))) {
    LOG_WARN("failed to get leader", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(arg.init(user_tenant_id))) {
    LOG_WARN("failed to init clear sync standby dest cache arg", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader).timeout(ctx.get_timeout())
        .group_id(share::OBCG_DBA_COMMAND).clear_sync_standby_dest_cache(arg))) {
    LOG_WARN("failed to clear sync standby dest cache", KR(ret), K(user_tenant_id), K(leader), K(arg));
  }
  return ret;
}

int ObSyncStandbyDestUtils::check_set_sync_standby_dest(const obrpc::ObAdminSetConfigArg &arg,
    bool &is_set_sync_standby_dest, uint64_t &tenant_id_output)
{
  int ret = OB_SUCCESS;
  is_set_sync_standby_dest = false;
  const obrpc::ObAdminSetConfigItem *item = nullptr;
  if (!arg.is_valid() || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret), KP(GCTX.schema_service_));
  } else if (arg.items_.count() != 1) {
    // only one item is allowed to set when set sync standby dest config
  } else if (FALSE_IT(item = &arg.items_.at(0))) {
  } else if (0 != STRCASECMP(item->name_.ptr(), OB_STR_SYNC_STANDBY_DEST)) {
  } else if ((is_sys_tenant(item->exec_tenant_id_) && item->tenant_name_.is_empty())
      || is_meta_tenant(item->exec_tenant_id_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sync_standby_dest only support user tenant", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set sync_standby_dest for meta or sys tenant is");
  } else if (OB_FAIL(ObBackupConfigUtil::get_item_user_tenant_id(*item,
      *GCTX.schema_service_, tenant_id_output))) {
    LOG_WARN("failed to check config tenant valid", KR(ret), K(arg));
  } else {
    is_set_sync_standby_dest = true;
  }
  LOG_DEBUG("check set sync standby dest", KR(ret), K(is_set_sync_standby_dest), K(arg));
  return ret;
}

int ObSyncStandbyDestUtils::admin_set_sync_standby_dest_config(const obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  bool is_set_sync_standby_dest = false;
  bool is_empty = false;
  ObSyncStandbyDestStruct sync_standby_dest;
  uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
  bool protection_mode_enable = false;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set sync_standby_dest in logservice is not supported", KR(ret), K(user_tenant_id),
        K(GCONF.enable_logservice));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set sync_standby_dest in logservice");
  } else if (OB_FAIL(check_set_sync_standby_dest(arg, is_set_sync_standby_dest, user_tenant_id))) {
    LOG_WARN("failed to check set sync standby dest", KR(ret), K(arg));
  } else if (!is_set_sync_standby_dest) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only set sync standby dest is allowed in this function", KR(ret), K(arg));
  } else if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set sync standby dest for meta/sys tenant", KR(ret), K(user_tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sync_standby_dest for meta or sys tenant is");
  } else if (OB_FAIL(standby::ObProtectionModeUtils::check_tenant_data_version_for_protection_mode(
          user_tenant_id, protection_mode_enable))) {
    LOG_WARN("failed to check tenant data version for protection mode", KR(ret), K(user_tenant_id));
  } else if (!protection_mode_enable) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is not upgraded, set protection mode is not allowed", KR(ret), K(user_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant is not upgraded, set sync_standby_dest");
  } else if (arg.items_.at(0).value_.is_empty()) {
    is_empty = true;
  } else if (OB_FAIL(parse_sync_standby_dest_config(user_tenant_id, arg.items_.at(0).value_.ptr(), sync_standby_dest))) {
    LOG_WARN("failed to parse sync standby dest config", KR(ret), K(arg));
  } else if (!sync_standby_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync standby dest is not valid", KR(ret), K(sync_standby_dest));
  }
  if (FAILEDx(update_sync_standby_dest(user_tenant_id, is_empty, sync_standby_dest))) {
    LOG_WARN("failed to update sync standby dest", KR(ret), K(arg));
  }
  return ret;
}
int ObSyncStandbyDestUtils::parse_sync_standby_dest_config(const uint64_t user_tenant_id,
  const common::ObString &value, ObSyncStandbyDestStruct &sync_standby_dest)
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode compat_mode;
  lib::Worker::CompatMode lib_compat_mode;
  ObSyncStandbyDestParser parser(sync_standby_dest);
  if (value.length() == 0 || !is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(value), K(user_tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(user_tenant_id, lib_compat_mode))) {
    LOG_WARN("failed to get tenant mode", KR(ret), K(user_tenant_id));
  } else if (FALSE_IT(compat_mode = (lib_compat_mode == lib::Worker::CompatMode::ORACLE ?
     ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE))) {
  } else if (OB_FAIL(parser.parse_from(value, user_tenant_id, compat_mode))) {
    LOG_WARN("failed to parse sync standby dest", KR(ret), K(value), K(user_tenant_id), K(compat_mode));
  }
  return ret;
}
int ObSyncStandbyDestUtils::update_sync_standby_dest(const uint64_t user_tenant_id, const bool is_empty,
  const ObSyncStandbyDestStruct &sync_standby_dest)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfoProxy all_tenant_info_proxy;
  ObMySQLTransaction trans;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ObAllTenantInfo tenant_info;
  ObSyncStandbyDestStruct old_sync_standby_dest;
  bool change_tenant = false;
  bool is_old_empty = false;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)
    || (!is_empty && !sync_standby_dest.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(is_empty), K(sync_standby_dest));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(all_tenant_info_proxy.load_tenant_info(user_tenant_id, &trans, true/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(ObSyncStandbyDestOperator::read_sync_standby_dest(trans, meta_tenant_id,
      true/*for_update*/, is_old_empty, old_sync_standby_dest))) {
    LOG_WARN("failed to read sync standby dest", KR(ret), K(meta_tenant_id));
  } else if (is_old_empty == is_empty) {
    change_tenant = (!is_empty && !(old_sync_standby_dest.restore_source_service_attr_.user_
        .is_same_tenant(sync_standby_dest.restore_source_service_attr_.user_)));
  } else {
    change_tenant = true;
  }
  if (OB_FAIL(ret)) {
  } else if (change_tenant && OB_FAIL(check_can_change_sync_standby_dest(tenant_info))) {
    LOG_WARN("failed to check can change sync standby dest", KR(ret), K(tenant_info));
  } else if (is_empty) {
    // delete sync standby dest
    if (is_old_empty) {
    } else if (OB_FAIL(ObSyncStandbyDestOperator::delete_sync_standby_dest(trans, meta_tenant_id))) {
      LOG_WARN("failed to delete sync standby dest", KR(ret), K(meta_tenant_id));
    }
  } else {
    // update sync standby dest
    if (OB_FAIL(ObSyncStandbyDestOperator::write_sync_standby_dest(trans, meta_tenant_id, sync_standby_dest))) {
      LOG_WARN("failed to write sync standby dest", KR(ret), K(meta_tenant_id), K(sync_standby_dest));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (FAILEDx(notify_clear_sync_standby_dest_cache(user_tenant_id))) {
    LOG_WARN("failed to notify clear sync standby dest cache", KR(ret), K(user_tenant_id));
  }
  return ret;
}
int ObSyncStandbyDestUtils::check_can_change_sync_standby_dest(const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  if (!tenant_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant info", KR(ret), K(tenant_info));
  } else if (!tenant_info.is_primary()) {
    // standby tenant is allowed to change sync standby dest at any time
  } else if (tenant_info.get_protection_mode().is_sync_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("maximum protection or maximum availability tenant is not allowed to change sync standby dest",
       KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change sync_standby_dest for tenant in MPT or MA is");
  }
  return ret;
}
} // namespace share
} // namespace oceanbase