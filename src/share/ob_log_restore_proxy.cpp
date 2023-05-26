/**
 * Copyright (c) 2023 OceanBase
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

#include "ob_log_restore_proxy.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/net/ob_addr.h"
#include "observer/ob_sql_client_decorator.h"  //ObSQLClientRetryWeak
#include "common/ob_smart_var.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/backup/ob_backup_struct.h"     // COMPATIBILITY_MODE
#include "share/ob_thread_mgr.h"
#include "logservice/palf/palf_options.h"
#include "share/oracle_errno.h"

namespace oceanbase
{
namespace share
{
#define RESTORE_PROXY_USER_ERROR(args)                                                                 \
  switch (ret) {                                                                                       \
    case -ER_TABLEACCESS_DENIED_ERROR:                                                                 \
    case -OER_TABLE_OR_VIEW_NOT_EXIST:                                                                 \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", privilege is insufficient");          \
      break;                                                                                           \
    case -ER_ACCESS_DENIED_ERROR:                                                                      \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the user and password");                      \
      break;                                                                                           \
    case -ER_DBACCESS_DENIED_ERROR:                                                                    \
    case OB_ERR_NO_LOGIN_PRIVILEGE:                                                                    \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the privileges");        \
      break;                                                                                           \
    case OB_ERR_NULL_VALUE:                                                                            \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", primary may not be ready");           \
      break;                                                                                           \
    case -ER_CONNECT_FAILED:                                                                           \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the network");                    \
      break;                                                                                           \
    default:                                                                                           \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args);                                        \
  }                                                                                                    \

ObLogRestoreMySQLProvider::ObLogRestoreMySQLProvider() : server_list_() {}

ObLogRestoreMySQLProvider::~ObLogRestoreMySQLProvider()
{
  server_list_.reset();
}

int ObLogRestoreMySQLProvider::init(const common::ObIArray<common::ObAddr> &server_list)
{
  return set_restore_source_server(server_list);
}

int ObLogRestoreMySQLProvider::set_restore_source_server(const common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(server_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server_list));
  } else if (OB_FAIL(server_list_.assign(server_list))) {
    LOG_WARN("server_list assign failed", K(server_list));
  }
  return ret;
}
void ObLogRestoreMySQLProvider::destroy()
{
  server_list_.reset();
}

int ObLogRestoreMySQLProvider::get_server(const int64_t svr_idx, common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(svr_idx < 0 || svr_idx >= server_list_.count())) {
    ret = OB_INVALID_INDEX;
    LOG_WARN("get_server invalid index", K(svr_idx));
  } else {
    server = server_list_.at(svr_idx);
  }
  return ret;
}

int64_t ObLogRestoreMySQLProvider::get_server_count() const
{
  return server_list_.count();
}

int ObLogRestoreMySQLProvider::get_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  return ret;
}

int ObLogRestoreMySQLProvider::get_tenant_servers(const uint64_t tenant_id, ObIArray<ObAddr> &tenant_servers)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  tenant_servers.reset();
  return ret;
}

int ObLogRestoreMySQLProvider::refresh_server_list(void)
{
  return OB_SUCCESS;
}

int ObLogRestoreMySQLProvider::prepare_refresh()
{
  return OB_SUCCESS;
}

int ObLogRestoreMySQLProvider::end_refresh()
{
  return OB_SUCCESS;
}

int ObLogRestoreConnectionPool::init(const common::ObIArray<common::ObAddr> &server_list,
      const char *user_name,
      const char *user_password,
      const char *db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(server_list.count() <= 0)
      || OB_ISNULL(user_name)
      || OB_ISNULL(user_password)
      || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server_list), K(user_name), K(user_password), K(db_name));
  } else if (OB_FAIL(set_db_param(user_name, user_password, db_name))) {
    LOG_WARN("set db param failed", K(user_name), K(user_password), K(db_name));
  } else {
    for (int64_t i = 0; i < server_list.count() && OB_SUCC(ret); i++) {
      ret = create_server_connection_pool(server_list.at(i));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObLogRestoreConnectionPool::destroy()
{
  common::sqlclient::ObMySQLConnectionPool::stop();
  common::sqlclient::ObMySQLConnectionPool::close_all_connection();
}

ObLogRestoreProxyUtil::ObLogRestoreProxyUtil() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  tg_id_(-1),
  server_prover_(),
  connection_(),
  user_name_(),
  user_password_(),
  db_name_(),
  sql_proxy_()
{}

ObLogRestoreProxyUtil::~ObLogRestoreProxyUtil()
{
  destroy();
}

int ObLogRestoreProxyUtil::init(const uint64_t tenant_id,
    const common::ObIArray<common::ObAddr> &server_list,
    const char *user_name,
    const char *user_password,
    const char *db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLogRestoreProxyUtil already init", K(inited_));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))
      || OB_UNLIKELY(server_list.count() <= 0)
      || OB_ISNULL(user_name)
      || OB_ISNULL(user_password)
      || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(server_list),
        K(user_name), K(user_password), K(db_name));
  } else if (OB_FAIL(server_prover_.init(server_list))) {
    LOG_WARN("server_prover_ init failed", K(tenant_id), K(server_list));
  } else if (FALSE_IT(connection_.set_server_provider(&server_prover_))) {
  } else if (TG_CREATE_TENANT(lib::TGDefIDs::LogMysqlPool, tg_id_)) {
    LOG_ERROR("create connection pool timer pool failed");
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_ERROR("TG_START failed");
  } else if (OB_FAIL(connection_.init(server_list, user_name, user_password, db_name))) {
    LOG_WARN("connection init failed");
  } else if (OB_FAIL(connection_.start(tg_id_))) {
    LOG_ERROR("start connection pool fail");
  } else if (!sql_proxy_.is_inited() && OB_FAIL(sql_proxy_.init(&connection_))) {
    LOG_WARN("sql_proxy_ init failed");
  } else if (OB_FAIL(user_name_.assign(user_name))) {
    LOG_WARN("user_name_ assign failed", K(user_name));
  } else if (OB_FAIL(user_password_.assign(user_password))) {
    LOG_WARN("user_password_ assign failed", K(user_password));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_sql_proxy(common::ObMySQLProxy *&proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    proxy = &sql_proxy_;
  }
  return ret;
}

void ObLogRestoreProxyUtil::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  connection_.stop();
  destroy_tg_();
  connection_.destroy();
  server_prover_.destroy();
  user_name_.reset();
  user_password_.reset();
  db_name_.reset();
}

int ObLogRestoreProxyUtil::refresh_conn(const common::ObIArray<common::ObAddr> &addr_array,
    const char *user_name,
    const char *user_password,
    const char *db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(addr_array.count() <= 0)
      || OB_ISNULL(user_name)
      || OB_ISNULL(user_password)
      || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_user_changed_(user_name, user_password, db_name)
      && OB_FAIL(connection_.set_db_param(user_name, user_password, db_name))) {
    LOG_WARN("set db param failed", K(user_name), K(user_password), K(db_name));
  } else if (OB_FAIL(server_prover_.set_restore_source_server(addr_array))) {
    LOG_WARN("set_restore_source_server failed", K(addr_array));
  } else {
    ObString name(user_name);
    ObString passwd(user_password);
    ObString db(db_name);
    LOG_INFO("print refresh_conn", K(name), K(passwd), K(db));
    connection_.signal_refresh();
  }
  return ret;
}

int ObLogRestoreProxyUtil::try_init(const uint64_t tenant_id,
    const common::ObIArray<common::ObAddr> &server_list,
    const char *user_name,
    const char *user_password)
{
  int ret = OB_SUCCESS;
  const char *MYSQL_DB = "OCEANBASE";
  const char *ORACLE_DB = "SYS";

  if (OB_SUCC(init(tenant_id, server_list, user_name, user_password, MYSQL_DB))) {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("SELECT 1"))) {
        LOG_WARN("fail to generate sql");
      } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result is null", K(tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get result next failed", K(sql));
      } else {
        LOG_INFO("proxy connect to primary oceanabse db success");
      }
    }
  }
  if (OB_FAIL(ret) && (-ER_BAD_DB_ERROR == ret)) {
    LOG_WARN("proxy connect to primary oceanbase db failed, then try connect to sys db");
    (void)destroy();
    if (OB_SUCC(init(tenant_id, server_list, user_name, user_password, ORACLE_DB))) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        ObSqlString sql;
        if (OB_FAIL(sql.assign_fmt("SELECT 1 FROM DUAL"))) {
          LOG_WARN("fail to generate sql");
        } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query result is null", K(tenant_id), K(sql));
        } else if (OB_FAIL(result.get_result()->next())) {
          LOG_WARN("get result next failed", K(sql));
        } else {
          LOG_INFO("proxy connect to sys db success");
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("proxy connect to primary db failed");
    RESTORE_PROXY_USER_ERROR("connection");
    destroy();
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_tenant_id(char *tenant_name, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT TENANT_ID FROM %s WHERE TENANT_NAME='%s'",
        OB_DBA_OB_TENANTS_TNAME, tenant_name))) {
      LOG_WARN("fail to generate sql");
    } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql), K(connection_.get_db_name()));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query result is null", K(tenant_name), K(sql));
    } else if (OB_FAIL(result.get_result()->next())) {
      LOG_WARN("get result next failed", K(tenant_name), K(sql));
    } else {
      EXTRACT_UINT_FIELD_MYSQL(*result.get_result(), "TENANT_ID", tenant_id, uint64_t);

      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get tenant id result", K(tenant_name), K(sql));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to get tenant id result");
    RESTORE_PROXY_USER_ERROR("tenant id");
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_cluster_id(uint64_t tenant_id, int64_t &cluster_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT VALUE FROM %s WHERE NAME='cluster_id'", OB_GV_OB_PARAMETERS_TNAME))) {
      LOG_WARN("fail to generate sql");
    } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query result is null", K(sql));
    } else if (OB_FAIL(result.get_result()->next())) {
      LOG_WARN("get result next failed", K(tenant_id), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result.get_result(), "VALUE", cluster_id, int64_t);

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get cluster id result", K(cluster_id), K(sql));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get cluster id result");
    RESTORE_PROXY_USER_ERROR("cluster id");
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_compatibility_mode(const uint64_t tenant_id, ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  const char *MYSQL_STR = "MYSQL";
  const char *ORACLE_STR = "ORACLE";
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT COMPATIBILITY_MODE FROM %s WHERE TENANT_ID=%ld",
         OB_DBA_OB_TENANTS_TNAME, tenant_id))) {
      LOG_WARN("fail to generate sql", K(tenant_id));
    } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
      LOG_WARN("read value from DBA_OB_TENANTS failed", K(tenant_id), K(sql.ptr()));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query result is null", K(tenant_id), K(sql));
    } else if (OB_FAIL(result.get_result()->next())) {
      LOG_WARN("get result next failed", K(tenant_id), K(sql));
    } else {
      ObString tmp_compat_mode;
      char compact[OB_MAX_COMPAT_MODE_STR_LEN + 1] = { 0 };
      EXTRACT_VARCHAR_FIELD_MYSQL(*result.get_result(), "COMPATIBILITY_MODE", tmp_compat_mode);

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get compact mode result", K(tenant_id), K(sql.ptr()));
        RESTORE_PROXY_USER_ERROR("compatibility mode");
      } else if (OB_FAIL(databuff_printf(compact, sizeof(compact), "%.*s",
          static_cast<int>(tmp_compat_mode.length()), tmp_compat_mode.ptr()))) {
        LOG_WARN("fail to print compact_mode", K(tmp_compat_mode.ptr()));
      } else if (0 == STRCASECMP(compact, ORACLE_STR)) {
        compat_mode = ObCompatibilityMode::ORACLE_MODE;
      } else if (0 == STRCASECMP(compact, MYSQL_STR)) {
        compat_mode = ObCompatibilityMode::MYSQL_MODE;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compatibility mode is not expected");
      }
    }
  }
  return ret;
}

int ObLogRestoreProxyUtil::check_begin_lsn(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS CNT FROM %s OB_LS LEFT JOIN"
                  "(SELECT TENANT_ID, LS_ID, BEGIN_LSN FROM %s WHERE ROLE= 'LEADER') LOG_STAT "
                  "ON OB_LS.LS_ID = LOG_STAT.LS_ID "
                  "WHERE LOG_STAT.TENANT_ID = %lu AND (BEGIN_LSN IS NULL OR BEGIN_LSN != 0)"
                      "AND OB_LS.STATUS NOT IN ('TENANT_DROPPING', 'CREATE_ABORT', 'PRE_TENANT_DROPPING')",
                      OB_DBA_OB_LS_TNAME, OB_GV_OB_LOG_STAT_ORA_TNAME, tenant_id))) {
        LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
        LOG_WARN("check_begin_lsn failed", KR(ret), K(tenant_id), K(sql));
        RESTORE_PROXY_USER_ERROR("tenant ls begin_lsn failed");
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config result is null", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get result next failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else {
        uint64_t cnt = 0;
        EXTRACT_INT_FIELD_MYSQL(*result.get_result(), "CNT", cnt, uint64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get result", KR(ret), K(tenant_id), K(sql));
        } else if (cnt > 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("primary tenant LS log may be recycled, create standby tenant is not allow", KR(ret), K(tenant_id), K(sql));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "primary tenant LS log may be recycled, create standby tenant is");
        }
      }
    }
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_server_ip_list(const uint64_t tenant_id, common::ObArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult ,result) {
    ObSqlString sql;
    ObMySQLResult *res = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT SVR_IP, SQL_PORT FROM %s WHERE TENANT_ID=%ld",
      OB_DBA_OB_ACCESS_POINT_TNAME, tenant_id))) {
      LOG_WARN("fail to generate sql");
    } else if (OB_FAIL(sql_proxy_.read(result, OB_INVALID_TENANT_ID, sql.ptr()))) {
      LOG_WARN("read value from DBA_OB_ACCESS_POINT failed", K(tenant_id), K(sql));
    } else if (OB_ISNULL(res = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query result is null", K(tenant_id), K(sql));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(res->next())) {
        ObString tmp_ip;
        int32_t tmp_port;
        ObAddr addr;

        EXTRACT_VARCHAR_FIELD_MYSQL(*res, "SVR_IP", tmp_ip);
        EXTRACT_INT_FIELD_MYSQL(*res, "SQL_PORT", tmp_port, int32_t);

        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get server ip and sql port", K(tmp_ip), K(tmp_port), K(tenant_id), K(sql));
        } else if (!addr.set_ip_addr(tmp_ip, tmp_port)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to set addr", K(tmp_ip), K(tmp_port), K(tenant_id), K(sql));
        } else if (!addr.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("addr is invalid", K(addr), K(tenant_id), K(sql));
        } else if (OB_FAIL(addrs.push_back(addr))) {
          LOG_WARN("fail to push back addr to addrs", K(addr), K(tenant_id), K(sql));
        }
      }
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get ip list", K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

bool ObLogRestoreProxyUtil::is_user_changed_(const char *user_name, const char *user_password, const char *db_name)
{
  return user_name_ != common::ObFixedLengthString<common::OB_MAX_USER_NAME_BUF_LENGTH>(user_name)
    || user_password_ != common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH + 1>(user_password)
    || db_name_ != common::ObFixedLengthString<common::OB_MAX_DATABASE_NAME_BUF_LENGTH>(db_name);
}

int ObLogRestoreProxyUtil::get_tenant_info(ObTenantRole &role, schema::ObTenantStatus &status)
{
  int ret = OB_SUCCESS;
  const char *TENANT_ROLE = "TENANT_ROLE";
  const char *TENANT_STATUS = "STATUS";
  common::ObMySQLProxy *proxy = &sql_proxy_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      common::ObSqlString sql;
      const char *GET_TENANT_INFO_SQL = "SELECT %s, %s FROM %s";
      if (OB_FAIL(sql.append_fmt(GET_TENANT_INFO_SQL, TENANT_ROLE, TENANT_STATUS, OB_DBA_OB_TENANTS_TNAME))) {
        LOG_WARN("append_fmt failed");
      } else if (OB_FAIL(proxy->read(res, sql.ptr()))) {
        LOG_WARN("excute sql failed", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("next failed");
      } else {
        ObString status_str;
        ObString role_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, TENANT_ROLE, role_str);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, TENANT_STATUS, status_str);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(schema::get_tenant_status(status_str, status))) {
            LOG_WARN("get tenant status failed");
          } else {
            role = ObTenantRole(role_str.ptr());
          }
        }
      }
    }
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_max_log_info(const ObLSID &id, palf::AccessMode &mode, SCN &scn)
{
  int ret = OB_SUCCESS;
  const char *LS_ID = "LS_ID";
  const char *MAX_SCN = "MAX_SCN";
  const char *ACCESS_MODE = "ACCESS_MODE";
  common::ObMySQLProxy *proxy = &sql_proxy_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(id));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      common::ObSqlString sql;
      const char *GET_MAX_LOG_INFO_SQL = "SELECT %s, %s, %s FROM %s WHERE %s=%ld and ROLE = 'LEADER'";
      if (OB_FAIL(sql.append_fmt(GET_MAX_LOG_INFO_SQL, LS_ID, MAX_SCN, ACCESS_MODE,
              OB_GV_OB_LOG_STAT_TNAME,  LS_ID, id.id()))) {
        LOG_WARN("append_fmt failed");
      } else if (OB_FAIL(proxy->read(res, sql.ptr()))) {
        LOG_WARN("excute sql failed", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("next failed");
      } else {
        uint64_t max_scn = 0;
        ObString access_mode_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, ACCESS_MODE, access_mode_str);
        EXTRACT_UINT_FIELD_MYSQL(*result, MAX_SCN, max_scn, uint64_t);
        if (OB_SUCC(ret) && OB_FAIL(palf::get_access_mode(access_mode_str, mode))) {
          LOG_WARN("get_access_mode failed", K(id), K(access_mode_str));
        }

        if (OB_SUCC(ret) && OB_FAIL(scn.convert_for_logservice(max_scn))) {
          LOG_WARN("convert_for_logservice failed", K(id), K(max_scn));
        }
      }
    }
  }
  return ret;
}

void ObLogRestoreProxyUtil::destroy_tg_()
{
  if (-1 != tg_id_) {
    int origin_tg_id = tg_id_;
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
    LOG_INFO("destroy_tg_ succ", K(origin_tg_id));
  }
}
} // namespace share
} // namespace oceanbase
