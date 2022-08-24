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
#include "ob_snapshot_info_manager.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "ob_rs_event_history_table_operator.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace rootserver {
int ObSnapshotInfoManager::init(const ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  if (!self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(self_addr));
  } else {
    self_addr_ = self_addr;
  }
  return ret;
}

int ObSnapshotInfoManager::set_index_building_snapshot(
    common::ObMySQLProxy &proxy, const int64_t index_table_id, const int64_t snapshot_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tenant_id = extract_tenant_id(index_table_id);
  if (OB_INVALID_ID == index_table_id || snapshot_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_table_id), K(snapshot_ts));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET BUILDING_SNAPSHOT = %ld WHERE TABLE_ID = %lu AND TENANT_ID = %ld",
                 OB_ALL_ORI_SCHEMA_VERSION_TNAME,
                 snapshot_ts,
                 ObSchemaUtils::get_extract_schema_id(tenant_id, index_table_id),
                 ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
    LOG_WARN("fail to update index building snapshot", KR(ret), K(index_table_id), K(snapshot_ts));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update get invalid affected_rows, should be one or zero", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObSnapshotInfoManager::acquire_snapshot_for_building_index(
    common::ObMySQLTransaction& trans, const ObSnapshotInfo& snapshot, const int64_t index_table_id)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (!snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot));
  } else if (OB_FAIL(snapshot_proxy.add_snapshot(trans, snapshot))) {
    LOG_WARN("fail to add snapshot", K(ret));
  }
  ROOTSERVICE_EVENT_ADD("snapshot", "acquire_snapshot", K(ret), K(snapshot), K(index_table_id), "rs_addr", self_addr_);
  return ret;
}

int ObSnapshotInfoManager::acquire_snapshot(common::ObMySQLTransaction& trans, const ObSnapshotInfo& snapshot)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (!snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot));
  } else if (OB_FAIL(snapshot_proxy.add_snapshot(trans, snapshot))) {
    LOG_WARN("fail to add snapshot", K(ret));
  }
  ROOTSERVICE_EVENT_ADD("snapshot", "acquire_snapshot", K(ret), K(snapshot), "rs_addr", self_addr_);
  return ret;
}

int ObSnapshotInfoManager::release_snapshot(common::ObMySQLTransaction& trans, const ObSnapshotInfo& snapshot)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (!snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot));
  } else if (OB_FAIL(snapshot_proxy.remove_snapshot(trans, snapshot))) {
    LOG_WARN("fail to remove snapshot", K(ret), K(snapshot));
  }
  ROOTSERVICE_EVENT_ADD("snapshot", "release_snapshot", K(ret), K(snapshot), "rs_addr", self_addr_);
  return ret;
}

int ObSnapshotInfoManager::get_snapshot(common::ObMySQLProxy& proxy, const int64_t tenant_id,
    share::ObSnapShotType snapshot_type, const char* extra_info, ObSnapshotInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (OB_FAIL(snapshot_proxy.get_snapshot(proxy, tenant_id, snapshot_type, extra_info, snapshot_info))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("fail to get snapshot", K(ret));
    }
  }

  return ret;
}

int ObSnapshotInfoManager::get_snapshot(common::ObISQLClient &proxy, const int64_t tenant_id,
    share::ObSnapShotType snapshot_type, const int64_t snapshot_ts, share::ObSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (OB_FAIL(snapshot_proxy.get_snapshot(proxy, tenant_id, snapshot_type, snapshot_ts, snapshot_info))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("fail to get snapshot", K(ret));
    }
  }
  return ret;
}

int ObSnapshotInfoManager::check_restore_point(
    common::ObMySQLProxy& proxy, const int64_t tenant_id, const int64_t table_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObSnapshotTableProxy snapshot_proxy;
  if (OB_FAIL(snapshot_proxy.check_snapshot_exist(
          proxy, tenant_id, table_id, share::SNAPSHOT_FOR_RESTORE_POINT, is_exist))) {
    LOG_WARN("fail to check snapshot exist", K(ret), K(tenant_id), K(table_id));
  }

  return ret;
}

int ObSnapshotInfoManager::get_snapshot_count(
    common::ObMySQLProxy& proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type, int64_t& count)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  if (OB_FAIL(snapshot_proxy.get_snapshot_count(proxy, tenant_id, snapshot_type, count))) {
    LOG_WARN("fail to get snapshot count", K(ret), K(tenant_id), K(snapshot_type));
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
