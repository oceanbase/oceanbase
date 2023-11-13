#/**
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

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "logservice/libobcdc/src/ob_log_systable_helper.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"
#include <cstdint>
#include <gtest/gtest.h>
#include "share/ob_log_restore_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#define private public
#include "simple_server/env/ob_simple_cluster_test_base.h"
#undef private
namespace oceanbase
{
namespace unittest
{

const char *ORACLE_TENANT_NAME = "";
const char *MYSQL_TENANT_NAME = "";
const char *USER_NAME = "";
const char *ORACLE_USER = "";
const char *MYSQL_USER = "";
const char *PASSWD = "";
const char *PASSWD_NEW = "";
const char *ORACLE_DB = "SYS";
const char *MYSQL_DB = "OCEANBASE";
const char *QUERY_SQL = "SELECT TENANT_ID, LS_ID from GV$OB_LOG_STAT";
const char *MEMORY_SIZE = "20G";
const char *LOG_DISK_SIZE = "20G";
const char *TENANT_MEMORY_SIZE = "5G";
const char *TENANT_LOG_DISK_SIZE = "5G";
static const char *TEST_FILE_NAME = "log_restore_proxy";
class ObLogRestoreProxyTest : public ObSimpleClusterTestBase
{
public:
  ObLogRestoreProxyTest() : ObSimpleClusterTestBase(TEST_FILE_NAME, MEMORY_SIZE, LOG_DISK_SIZE), oracle_mode_(false) {}

  void create_test_tenant(const bool oracle_mode)
  {
    oracle_mode_ = oracle_mode;
    const char *tenant_name = oracle_mode ? ORACLE_TENANT_NAME : MYSQL_TENANT_NAME;
    const char *db_name = oracle_mode ? ORACLE_DB : MYSQL_DB;
    CLOG_LOG(INFO, "create_tenant start", K(tenant_name));
    ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, TENANT_MEMORY_SIZE, TENANT_LOG_DISK_SIZE, oracle_mode));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name, db_name, oracle_mode));
    CLOG_LOG(INFO, "create_tenant end", K(tenant_name));
  }

  void create_user()
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (true == oracle_mode_) {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt("create user %s identified by %s", USER_NAME, PASSWD));
    } else {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt("create user %s identified by '%s'", USER_NAME, PASSWD));
    }
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
    CLOG_LOG(INFO, "create user end", K(sql));
  }

  void grant_user()
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (oracle_mode_) {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt("grant dba to %s", USER_NAME));
    } else {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt("GRANT SELECT ON *.* TO '%s'", USER_NAME));
    }
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
    CLOG_LOG(INFO, "grant user end", K(sql));
  }

  void modify_passwd()
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (oracle_mode_) {
    ASSERT_EQ(OB_SUCCESS, sql.append_fmt("alter user %s identified by %s", USER_NAME, PASSWD_NEW));
    } else {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt("alter user %s identified by '%s'", USER_NAME, PASSWD_NEW));
    }
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));
  }

  void get_server(common::ObAddr &addr)
  {
    addr.set_ip_addr(get_curr_simple_server().local_ip_.c_str(), get_curr_simple_server().mysql_port_);
  }

  void delete_test_tenant(const bool oracle_mode)
  {
    const char *tenant_name = oracle_mode_ ? ORACLE_TENANT_NAME : MYSQL_TENANT_NAME;
    ASSERT_EQ(OB_SUCCESS, delete_tenant(tenant_name));
    CLOG_LOG(INFO, "delete tenant end");
  }

  int query(common::ObMySQLProxy *mysql_proxy, int64_t &tenant_id)
  {
    int ret = OB_SUCCESS;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      const char *SELECT_ALL_LS = "SELECT TENANT_ID, LS_ID from %s ";
      ObMySQLResult *result = NULL;
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(SELECT_ALL_LS, share::OB_GV_OB_LOG_STAT_TNAME))) {
        SERVER_LOG(WARN, "failed to append table name", K(ret));
      } else if (OB_FAIL(mysql_proxy->read(res, OB_INVALID_TENANT_ID, sql.ptr()))) {
        SERVER_LOG(WARN, "read failed");
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t ls_id = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "TENANT_ID", tenant_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "LS_ID", ls_id, int64_t);
          SERVER_LOG(INFO, "query succ", K(tenant_id), K(ls_id));
        }
      }
    }
    return ret;
  }

private:
  bool oracle_mode_;
};
}
}
