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

#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "storage/tablelock/ob_lock_executor.h"
#include "share/ob_table_lock_compat_versions.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{

using namespace share::schema;
using namespace common;
using namespace transaction::tablelock;


namespace sql
{

ObRenameTableStmt::ObRenameTableStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_RENAME_TABLE)
{
}

ObRenameTableStmt::ObRenameTableStmt()
    : ObDDLStmt(stmt::T_RENAME_TABLE)
{
}

ObRenameTableStmt::~ObRenameTableStmt()
{
}

int ObRenameTableStmt::add_rename_table_item(const obrpc::ObRenameTableItem &rename_table_item){
  int ret = OB_SUCCESS;
  if (OB_FAIL(rename_table_arg_.rename_table_items_.push_back(rename_table_item))) {
    SQL_RESV_LOG(WARN, "failed to add rename table item to rename table arg!", K(ret));
  }
  return ret;
}

int ObRenameTableStmt::get_rename_table_table_ids(
    common::ObIArray<share::schema::ObObjectStruct> &object_ids) const
{
  int ret = OB_SUCCESS;
  const common::ObIArray<obrpc::ObRenameTableItem> &tmp_items=rename_table_arg_.rename_table_items_;
  for (int64_t i = 0; i < tmp_items.count() && OB_SUCC(ret); ++i) {
    share::schema::ObObjectStruct tmp_struct(share::schema::ObObjectType::TABLE,
                                             tmp_items.at(i).origin_table_id_);
    if (OB_FAIL(object_ids.push_back(tmp_struct))) {
      SQL_RESV_LOG(WARN, "failed to add rename table item to rename table arg!", K(ret));
    }
  }
  return ret;
}

int ObRenameTableStmt::set_lock_priority(sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session->get_effective_tenant_id();
  const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "tenant config invalid, can not do rename", K(ret), K(tenant_id));
  } else if (tenant_config->enable_lock_priority) {
    if (is_rename_cluster_version(min_cluster_version)) {
      if (!ObLockExecutor::proxy_is_support(session)) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "is in proxy_mode and not support rename", K(ret), KPC(session));
      } else {
        rename_table_arg_.lock_priority_ = ObTableLockPriority::HIGH1;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "the cluster version is not support rename", K(ret),
                   K(min_cluster_version));
    }
  }
  return ret;
}

} //namespace sql
}

