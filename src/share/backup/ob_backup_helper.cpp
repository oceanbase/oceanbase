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
#include "share/backup/ob_backup_helper.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_smart_var.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace sqlclient;


ObBackupHelper::ObBackupHelper()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(nullptr)
{

}

uint64_t ObBackupHelper::get_exec_tenant_id() const
{
  return gen_meta_tenant_id(tenant_id_);
}

int ObBackupHelper::init(const uint64_t tenant_id, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupHelper init twice", K(ret));
  } else if(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }

  return ret;
}

int ObBackupHelper::get_backup_dest(share::ObBackupPathString &backup_dest) const
{
  int ret = OB_SUCCESS;
  ObInnerKVTableOperator kv_table_operator;
  ObInnerKVItemStringValue kv_value;
  ObInnerKVItem item(&kv_value);
  ObInnerKVItemTenantIdWrapper item_with_tenant_id(&item);

  const bool need_lock = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupHelper not init", K(ret));
  } else if (OB_FAIL(init_backup_parameter_table_operator_(kv_table_operator))) {
    LOG_WARN("failed to init backup parameter table operator", K(ret), K(tenant_id_));
  } else if (OB_FAIL(item_with_tenant_id.set_tenant_id(tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(item_with_tenant_id.set_kv_name(OB_STR_DATA_BACKUP_DEST))) {
    LOG_WARN("failed to set kv name", K(ret));
  } else if (OB_FAIL(kv_table_operator.get_item(*sql_proxy_, need_lock, item_with_tenant_id))) {
    LOG_WARN("failed to get item", K(ret), K(need_lock), K(item_with_tenant_id));
  } else if (OB_FAIL(backup_dest.assign(kv_value.get_value()))) {
    LOG_WARN("failed to assign backup dest", K(ret), K(need_lock), K(item_with_tenant_id));
  }

  return ret;
}


int ObBackupHelper::set_backup_dest(const share::ObBackupPathString &backup_dest) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerKVTableOperator kv_table_operator;
  ObInnerKVItemStringValue kv_value;
  ObInnerKVItem item(&kv_value);
  ObInnerKVItemTenantIdWrapper item_with_tenant_id(&item);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupHelper not init", K(ret));
  } else if (OB_FAIL(init_backup_parameter_table_operator_(kv_table_operator))) {
    LOG_WARN("failed to init backup parameter table operator", K(ret), K(tenant_id_));
  } else if (OB_FAIL(kv_value.set_value(backup_dest.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(tenant_id_), K(backup_dest));
  } else if (OB_FAIL(item_with_tenant_id.set_tenant_id(tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(item_with_tenant_id.set_kv_name(OB_STR_DATA_BACKUP_DEST))) {
    LOG_WARN("failed to set kv name", K(ret));
  } else if (OB_FAIL(kv_table_operator.insert_or_update_item(*sql_proxy_, item_with_tenant_id, affected_rows))) {
    LOG_WARN("failed to set backup_dest", K(ret), K(item_with_tenant_id));
  } 

  return ret;
}


int ObBackupHelper::init_backup_parameter_table_operator_(ObInnerKVTableOperator &kv_table_operator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(kv_table_operator.init(OB_ALL_BACKUP_PARAMETER_TNAME, *this))) {
    LOG_WARN("failed to init backup parameter table operator", K(ret), K(this));
  }
  return ret;
}