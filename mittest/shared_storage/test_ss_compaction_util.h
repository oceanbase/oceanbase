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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#ifndef OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SS_COMPACTION_UTIL_H_
#define OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SS_COMPACTION_UTIL_H_

#define private public
#define protected public
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_test_util.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace storage
{

class TestCompactionUtil
{
public:
  static int wait_reserved_snapshot_ready(const share::ObLSID &ls_id);
  static int medium_compact(const uint64_t tenant_id, const int64_t tablet_id, const share::ObLSID &ls_id);
  static int wait_policy_refresh_finish(const uint64_t tenant_id, const int64_t tablet_id,
                                        const char *storage_cache_policy);
};

int TestCompactionUtil::wait_reserved_snapshot_ready(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  const int64_t timeout_us = 60 * 1000 * 1000; // 60s
  int64_t min_reserved_snapshot = 0;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);

  FLOG_INFO("[TEST] start to wait reserved snapshot ready", K(ls_id));

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[TEST] ls_svr is null", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("[TEST] fail to get ls", KR(ret), K(ls_id));
  } else {
    while ((min_reserved_snapshot <= 0) && (ObTimeUtility::current_time_us() - start_time_us < timeout_us)) {
      min_reserved_snapshot = ls_handle.get_ls()->get_min_reserved_snapshot();
      FLOG_INFO("[TEST] current reserved snapshot value", K(ls_id), K(min_reserved_snapshot));
      usleep(1000 * 1000);  // 1s
    }
    if (min_reserved_snapshot <= 0) {
      ret = OB_TIMEOUT;
      FLOG_ERROR("[TEST] timeout waiting for reserved snapshot ready", K(ls_id), K(min_reserved_snapshot), K(timeout_us));
    }
  }
  return ret;
}

int TestCompactionUtil::medium_compact(const uint64_t tenant_id, const int64_t tablet_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  observer::ObSimpleServer &server = unittest::ObSimpleClusterTestBase::get_curr_simple_server();
  common::ObMySQLProxy &sql_proxy = server.get_sql_proxy2();  // tenant tt1

  int64_t last_major_snapshot = 0;
  int64_t finish_count = 0;
  ObSqlString get_last_major_snapshot_sql;
  ObSqlString wait_major_compaction_finish_sql;

  if (OB_FAIL(get_last_major_snapshot_sql.assign_fmt("select ifnull(max(end_log_scn),0) as mx from oceanbase.__all_virtual_table_mgr"
                                                     " where tenant_id = %lu and tablet_id = %lu and ls_id = %lu"
                                                     " and table_type = 10;",
                                                     tenant_id, tablet_id, ls_id.id()))) {
    LOG_WARN("[TEST] fail to select last_major_snapshot", KR(ret), K(get_last_major_snapshot_sql), K(tenant_id), K(tablet_id), K(ls_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_proxy.read(res, get_last_major_snapshot_sql.ptr()))) {
        LOG_WARN("[TEST] fail to read sql result", KR(ret), K(get_last_major_snapshot_sql));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[TEST] fail to get result", KR(ret), K(get_last_major_snapshot_sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("[TEST] fail to get next result", KR(ret), K(get_last_major_snapshot_sql));
        } else if (OB_FAIL(result->get_int("mx", last_major_snapshot))) {
          LOG_WARN("[TEST] fail to get int result", KR(ret), K(get_last_major_snapshot_sql));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // already logged
  } else if (OB_FAIL(wait_reserved_snapshot_ready(ls_id))) {
    LOG_WARN("[TEST] fail to wait reserved snapshot ready", KR(ret), K(ls_id));
  } else {
    FLOG_INFO("[TEST] start medium compact", K(tenant_id), K(tablet_id), K(ls_id), K(get_last_major_snapshot_sql));
    std::string medium_compact_str = "alter system major freeze tenant tt1 tablet_id=";
    medium_compact_str += std::to_string(tablet_id);
    int64_t affected_rows = 0;
    ObSqlString sql_alter;
    if (OB_FAIL(sql_alter.assign(medium_compact_str.c_str()))) {
      LOG_WARN("[TEST] fail to assign alter sql", KR(ret), K(medium_compact_str.c_str()));
    } else if (OB_FAIL(server.get_sql_proxy().write(sql_alter.ptr(), affected_rows))) {
      LOG_WARN("[TEST] fail to exe sys sql", KR(ret), K(medium_compact_str.c_str()));
    } else {
      FLOG_INFO("[TEST] wait medium compact finished", K(tenant_id), K(tablet_id), K(medium_compact_str.c_str()));
      do {
        if (OB_FAIL(wait_major_compaction_finish_sql.assign_fmt("select count(*) as count from oceanbase.__all_virtual_table_mgr"
                                                                " where tenant_id = %lu and tablet_id = %lu and ls_id = %lu"
                                                                " and table_type = 10 and end_log_scn > %ld;",
                                                                tenant_id, tablet_id, ls_id.id(), last_major_snapshot))) {
          LOG_WARN("[TEST] fail to select finish count", KR(ret), K(wait_major_compaction_finish_sql), K(last_major_snapshot));
        } else {
          SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
            if (OB_FAIL(sql_proxy.read(res2, wait_major_compaction_finish_sql.ptr()))) {
              LOG_WARN("[TEST] fail to read sql result", KR(ret), K(wait_major_compaction_finish_sql));
            } else {
              sqlclient::ObMySQLResult *result = res2.get_result();
              if (OB_ISNULL(result)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("[TEST] fail to get result", KR(ret), K(wait_major_compaction_finish_sql));
              } else if (OB_FAIL(result->next())) {
                LOG_WARN("[TEST] fail to get next result", KR(ret), K(wait_major_compaction_finish_sql));
              } else if (OB_FAIL(result->get_int("count", finish_count))) {
                LOG_WARN("[TEST] fail to get int result", KR(ret), K(wait_major_compaction_finish_sql));
              }
            }
          }
        }
      } while (OB_SUCC(ret) && 0 == finish_count);
      if (OB_SUCC(ret)) {
        FLOG_INFO("[TEST] medium compact finished", K(tenant_id), K(tablet_id), K(wait_major_compaction_finish_sql));
      }
    }
  }
  return ret;
}

int TestCompactionUtil::wait_policy_refresh_finish(const uint64_t tenant_id, const int64_t tablet_id,
                                                   const char *storage_cache_policy)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[TEST] wait policy refresh finish", K(tenant_id), K(tablet_id), K(storage_cache_policy));
  observer::ObSimpleServer &server = unittest::ObSimpleClusterTestBase::get_curr_simple_server();
  common::ObMySQLProxy &sql_proxy = server.get_sql_proxy2();  // tenant tt1

  ObSqlString sql;
  int64_t row_cnt = 0;

  do {
    if (OB_FAIL(sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_tablet_local_cache"
                               " where tenant_id=%lu and tablet_id=%lu and storage_cache_policy='%s';",
              tenant_id, tablet_id, storage_cache_policy))) {
      LOG_WARN("[TEST] fail to assign sql", KR(ret), K(sql));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
          LOG_WARN("[TEST] fail to read sql result", KR(ret), K(sql));
        } else {
          sqlclient::ObMySQLResult *result = res.get_result();
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[TEST] fail to get result", KR(ret), K(sql));
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("[TEST] fail to get next result", KR(ret), K(sql));
          } else if (OB_FAIL(result->get_int("row_cnt", row_cnt))) {
            LOG_WARN("[TEST] fail to get int result", KR(ret), K(sql));
          }
        }
      }
    }
    usleep(100 * 1000);
    FLOG_INFO("[TEST] policy refresh result", K(row_cnt));
  } while (row_cnt <= 0);

  if (OB_SUCC(ret)) {
    FLOG_INFO("[TEST] policy refresh finished", K(row_cnt));
  } else {
    LOG_WARN("[TEST] fail to wait policy refresh finish", KR(ret), K(tenant_id), K(tablet_id), K(storage_cache_policy));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SS_COMPACTION_UTIL_H_
