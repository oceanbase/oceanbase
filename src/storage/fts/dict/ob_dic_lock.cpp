/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/dict/ob_dic_lock.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

namespace oceanbase
{
namespace storage
{
int ObDicLock::lock_dic_tables_out_trans(
    const uint64_t tenant_id,
    const ObTenantDicLoader &dic_loader,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const ObArray<ObTenantDicLoader::ObDicTableInfo> &dic_tables_info = dic_loader.get_dic_tables_info();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dic_tables_info.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dic loader is invalid", K(ret), K(tenant_id), K(dic_tables_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
     LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dic_tables_info.count(); ++i) {
      const uint64_t table_id = dic_tables_info.at(i).table_id_;
      if (OB_FAIL(do_table_lock(tenant_id, table_id, lock_mode, lock_owner, DEFAULT_TIMEOUT, true/*is_lock*/, trans))) {
          LOG_WARN("fail to do lock table", K(ret), K(tenant_id));
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObDicLock::unlock_dic_tables(
    const uint64_t tenant_id,
    const ObTenantDicLoader &dic_loader,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObArray<ObTenantDicLoader::ObDicTableInfo> &dic_tables_info = dic_loader.get_dic_tables_info();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dic_tables_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dic loader is invalid", K(ret), K(tenant_id), K(dic_tables_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dic_tables_info.count(); ++i) {
      const uint64_t table_id = dic_tables_info.at(i).table_id_;
      if (OB_FAIL(do_table_lock(tenant_id, table_id, lock_mode, lock_owner, DEFAULT_TIMEOUT, false/*is_lock*/, trans))) {
        LOG_WARN("fail to do unlock table", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObDicLock::lock_dic_tables_in_trans(
    const int64_t tenant_id,
    const ObTenantDicLoader &dic_loader,
    const transaction::tablelock::ObTableLockMode lock_mode,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  const ObArray<ObTenantDicLoader::ObDicTableInfo> &dic_tables_info = dic_loader.get_dic_tables_info();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dic_tables_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dic loader is invalid", K(ret), K(tenant_id), K(dic_tables_info));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dic_tables_info.count(); ++i) {
      const uint64_t table_id = dic_tables_info.at(i).table_id_;
      LOG_INFO("lock table", KR(ret), K(table_id), K(tenant_id), KPC(conn));
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id, table_id, lock_mode, DEFAULT_TIMEOUT, conn))) {
        LOG_WARN("lock dest table failed", KR(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}
} // end storage
} // end oceanbase