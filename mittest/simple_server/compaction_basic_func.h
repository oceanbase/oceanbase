/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
namespace unittest
{
struct CompactionBasicFunc
{
  static int get_table_id(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    int64_t &table_id);
  static int get_tablet_and_ls_id(
    sqlclient::ObISQLConnection &conn,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id);
  static int check_major_finish(
    sqlclient::ObISQLConnection &conn,
    const uint64_t tenant_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCC(inner_check_major_finish(conn, tenant_id, 1/*expect_cnt*/))) {
        ret = inner_check_major_finish(conn, tenant_id, 0/*expect_cnt*/);
      }
      return ret;
    }
  static int check_minor_finish(
    sqlclient::ObISQLConnection &conn,
    const uint64_t tenant_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCC(inner_check_minor_finish(conn, tenant_id, true/*expect_exist*/))) {
        ret = inner_check_minor_finish(conn, tenant_id, false/*expect_exist*/);
      }
      return ret;
    }
private:
  static int inner_check_major_finish(
    sqlclient::ObISQLConnection &conn,
    const uint64_t tenant_id,
    const bool expect_cnt);
  static int inner_check_minor_finish(
    sqlclient::ObISQLConnection &conn,
    const uint64_t tenant_id,
    const bool expect_exist);
};

int CompactionBasicFunc::get_table_id(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    int64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table where table_name like '%c%s%c' limit 1", '%', table_name, '%');
  if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), table_id))) {
    COMMON_LOG(WARN, "failed to select table id", KR(ret), K(sql));
  } else if (is_index) {
    sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table where data_table_id = %ld and index_status = 2 limit 1", table_id);
    if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), table_id))) {
      COMMON_LOG(WARN, "failed to select index id", KR(ret), K(sql));
    }
  }
  return ret;
}

int CompactionBasicFunc::get_tablet_and_ls_id(
    sqlclient::ObISQLConnection &conn,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld limit 1", table_id);
  if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_tablet_id))) {
    sql.reuse();
    sql.assign_fmt("select ls_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld and tablet_id = %ld limit 1", table_id, tmp_tablet_id);
    if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_ls_id))) {
      tablet_id = ObTabletID(tmp_tablet_id);
      ls_id = ObLSID(tmp_ls_id);
    }
  }
  return ret;
}

int CompactionBasicFunc::inner_check_major_finish(
    sqlclient::ObISQLConnection &conn,
    const uint64_t tenant_id,
    const bool expect_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sql.assign_fmt("select count(*) as val from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = %lu and GLOBAL_BROADCAST_SCN != LAST_SCN", tenant_id);
  while (OB_SUCC(ret)) {
    int64_t unfinish = 0;
    if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), unfinish))) {
      COMMON_LOG(INFO, "check major finish", KR(ret), K(tenant_id), K(sql), K(unfinish), K(expect_cnt));
      if (unfinish == expect_cnt) {
        break;
      } else {
        sleep(10);
      }
    }
  }
  return ret;
}

int CompactionBasicFunc::inner_check_minor_finish(
  sqlclient::ObISQLConnection &conn,
  const uint64_t tenant_id,
  const bool expect_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sql.assign_fmt("select count(*) > 0 as val from oceanbase.__all_virtual_table_mgr where tenant_id = %lu and table_type = 0 and is_active like '%NO%'", tenant_id);
  while (OB_SUCC(ret)) {
    int64_t exist = false;
    if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), exist))) {
      COMMON_LOG(INFO, "check minor finish", KR(ret), K(tenant_id), K(sql), K(exist), K(expect_exist));
      if (exist == expect_exist) {
        break;
      } else {
        sleep(3);
      }
    }
  } // while
  return ret;
}

} // unittest
} // namespace oceanbase
