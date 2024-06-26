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
#include "observer/ob_server_struct.h"
#include "common/ob_smart_var.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/backup/ob_backup_struct.h"     // COMPATIBILITY_MODE
#include "share/ob_thread_mgr.h"
#include "share/config/ob_server_config.h"
#include "logservice/palf/palf_options.h"
#include "share/oracle_errno.h"
#include <mysql.h>

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
    case OB_PASSWORD_WRONG:                                                                            \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the user and password"); \
      break;                                                                                           \
    case -ER_DBACCESS_DENIED_ERROR:                                                                    \
    case OB_ERR_NO_LOGIN_PRIVILEGE:                                                                    \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the privileges");        \
      break;                                                                                           \
    case OB_ERR_NULL_VALUE:                                                                            \
    case OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH:                                                            \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", query primary failed");               \
      break;                                                                                           \
    case OB_IN_STOP_STATE:                                                                             \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", primary is in stop state");           \
      break;                                                                                           \
    case OB_TENANT_NOT_EXIST:                                                                          \
    case OB_TENANT_NOT_IN_SERVER:                                                                      \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", primary tenant is not avaliable");    \
      break;                                                                                           \
    case OB_SERVER_IS_INIT:                                                                            \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", primary server is initializing");     \
      break;                                                                                           \
    case -ER_CONNECT_FAILED:                                                                           \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", please check the network");           \
      break;                                                                                           \
    case OB_ERR_TENANT_IS_LOCKED:                                                                      \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", primary tenant is locked");           \
      break;                                                                                           \
    case OB_CONNECT_ERROR:                                                                             \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args ", all servers are unreachable");        \
      break;                                                                                           \
    default:                                                                                           \
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "get primary " args);                                        \
  }                                                                                                    \

#define RESTORE_RETRY(arg)                                                                             \
  int64_t run_time = 1;                                                                                \
  int tmp_ret = OB_SUCCESS;                                                                            \
  do {                                                                                                 \
    arg                                                                                                \
    if (OB_TMP_FAIL(ret) && run_time < server_prover_.get_server_count()) {                            \
      LOG_WARN("restore proxy query failed, switch to next server", K(run_time), K(tmp_ret), K(ret));  \
      ret = OB_SUCCESS;                                                                                \
    }                                                                                                  \
  } while (OB_TMP_FAIL(tmp_ret) && run_time++ < server_prover_.get_server_count());


ObLogRestoreMySQLProvider::ObLogRestoreMySQLProvider() : server_list_(), lock_() {}

ObLogRestoreMySQLProvider::~ObLogRestoreMySQLProvider()
{
  destroy();
}

int ObLogRestoreMySQLProvider::init(const common::ObIArray<common::ObAddr> &server_list)
{
  return set_restore_source_server(server_list);
}

int ObLogRestoreMySQLProvider::set_restore_source_server(const common::ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
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
  WLockGuard guard(lock_);
  server_list_.reset();
}

int ObLogRestoreMySQLProvider::get_server(const int64_t svr_idx, common::ObAddr &server)
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(svr_idx < 0)) {
    ret = OB_INVALID_INDEX;
    LOG_WARN("get_server invalid index", K(svr_idx));
  } else if (OB_UNLIKELY(svr_idx >= server_list_.count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("svr_idx out of range", K(svr_idx), K(server_list_));
  } else {
    server = server_list_.at(svr_idx);
  }
  return ret;
}

int64_t ObLogRestoreMySQLProvider::get_server_count() const
{
  RLockGuard guard(lock_);
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
  sql_proxy_(),
  is_oracle_mode_(false)
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
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LogMysqlPool, tg_id_))) {
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
  is_oracle_mode_ = false;
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
  } else if (OB_FAIL(server_prover_.set_restore_source_server(addr_array))) {
    LOG_WARN("set_restore_source_server failed", K(addr_array));
  } else if (!is_user_changed_(user_name, user_password)) {
    // do nothing
  } else if (OB_FAIL(connection_.set_db_param(user_name, user_password, db_name))) {
    LOG_WARN("set db param failed", K(user_name), K(user_password), K(db_name));
  }
  // fix string user_name_ and user_password_ is enough to hold these two params
  else if (OB_FAIL(user_name_.assign(user_name))) {
    LOG_ERROR("user_name_ assign failed", K(user_name));
  } else if (OB_FAIL(user_password_.assign(user_password))) {
    LOG_ERROR("user_password_ assign failed", K(user_password));
  } else {
    LOG_INFO("log restore proxy connection refresh", K(user_name_));
  }
  return ret;
}

int ObLogRestoreProxyUtil::try_init(const uint64_t tenant_id,
    const common::ObIArray<common::ObAddr> &server_list,
    const char *user_name,
    const char *user_password)
{
  int ret = OB_SUCCESS;
  const char *db_name = nullptr;
  ObLogRestoreMySQLProvider tmp_server_prover;
  common::ObArray<common::ObAddr> fixed_server_list;

  if (OB_FAIL(fixed_server_list.assign(server_list))) {
    LOG_WARN("fail to assign fixed server list", K(tenant_id), K(server_list));
  } else if (OB_FAIL(tmp_server_prover.init(server_list))) {
    LOG_WARN("tmp_server_prover_ init failed", K(tenant_id), K(server_list));
  } else if (OB_FAIL(detect_tenant_mode_(&tmp_server_prover, user_name, user_password, &fixed_server_list))) {
    LOG_WARN("detect tenant mode failed", KR(ret), K_(user_name));
  } else if (OB_FALSE_IT(db_name = is_oracle_mode_ ? OB_ORA_SYS_SCHEMA_NAME : OB_SYS_DATABASE_NAME)) {
  } else if (OB_FAIL(init(tenant_id, fixed_server_list, user_name, user_password, db_name))) {
    LOG_WARN("[RESTORE PROXY] fail to init restore proxy");
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("[RESTORE PROXY] proxy connect to primary db failed");
    RESTORE_PROXY_USER_ERROR("connection");
    destroy();
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_tenant_id(char *tenant_name, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  RESTORE_RETRY(
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
  )
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to get tenant id result");
    RESTORE_PROXY_USER_ERROR("tenant id");
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_cluster_id(uint64_t tenant_id, int64_t &cluster_id)
{
  int ret = OB_SUCCESS;
  RESTORE_RETRY(
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
        LOG_WARN("get result next failed", K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result.get_result(), "VALUE", cluster_id, int64_t);

        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get cluster id result", K(cluster_id), K(sql));
        }
      }
    }
  )
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get cluster id result");
    RESTORE_PROXY_USER_ERROR("cluster id");
  }
  return ret;
}

int ObLogRestoreProxyUtil::check_different_cluster_with_same_cluster_id(
    const int64_t source_cluster_id, bool &res)
{
  int ret = OB_SUCCESS;
  res = false;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (GCONF.cluster_id == source_cluster_id) {
    // get one machine ip from the source cluster, then check if that machine is in current
    // cluster's __all_server table
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObSqlString sql;
      char svr_ip_buf[common::OB_IP_STR_BUFF] = {0};
      common::ObAddr primary_server;
      if (OB_FAIL(server_prover_.get_server(0, primary_server))) {
        LOG_WARN("fail to get primary server", K(ret));
      } else if (!primary_server.ip_to_string(svr_ip_buf, common::OB_IP_STR_BUFF)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to convert ip to string", K(ret));
      } else if (OB_FAIL(sql.assign_fmt(
                     "SELECT COUNT(*) AS cnt FROM %s WHERE svr_ip='%s' AND inner_port=%d",
                     OB_ALL_SERVER_TNAME, svr_ip_buf, primary_server.get_port()))) {
        LOG_WARN("fail to generate sql", K(ret));
      } else if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql proxy is null", K(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql.ptr()))) {
        LOG_WARN("fail to get __all_server", K(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result is null", K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get result next failed", K(sql));
      } else {
        int64_t cnt = 0;
        EXTRACT_INT_FIELD_MYSQL(*result.get_result(), "cnt", cnt, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get sql result", K(ret), K(sql));
        } else if (0 == cnt) {
          res = true;
        }
        LOG_INFO("check if cluster_id duplicated", K(res), K(cnt), K(sql));
      }
    }
  }

  return ret;
}

int ObLogRestoreProxyUtil::get_compatibility_mode(const uint64_t tenant_id, ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  const char *MYSQL_STR = "MYSQL";
  const char *ORACLE_STR = "ORACLE";
  RESTORE_RETRY(
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
        LOG_WARN("get result next failed", K(sql));
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
  )
  return ret;
}

int ObLogRestoreProxyUtil::check_begin_lsn(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    RESTORE_RETRY(
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        ObSqlString sql;
        if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS CNT FROM %s OB_LS LEFT JOIN"
              "(SELECT TENANT_ID, LS_ID, BEGIN_LSN FROM %s WHERE ROLE= 'LEADER' AND TENANT_ID = %lu) LOG_STAT "
                    "ON OB_LS.LS_ID = LOG_STAT.LS_ID "
                    "WHERE (BEGIN_LSN IS NULL OR BEGIN_LSN != 0)"
                    "AND OB_LS.STATUS NOT IN ('TENANT_DROPPING', 'CREATE_ABORT', 'PRE_TENANT_DROPPING')",
                    OB_DBA_OB_LS_ORA_TNAME, OB_GV_OB_LOG_STAT_ORA_TNAME, tenant_id))) {
          LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
        } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
          LOG_WARN("check_begin_lsn failed", KR(ret), K(tenant_id), K(sql));
          RESTORE_PROXY_USER_ERROR("tenant ls begin_lsn failed");
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config result is null", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result.get_result()->next())) {
          LOG_WARN("get result next failed", K(sql));
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
          LOG_INFO("check begion lsn", K(cnt), K(sql));
        }
      }
    )
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_server_ip_list(const uint64_t tenant_id, common::ObArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(
          sql.assign_fmt("SELECT SVR_IP, SQL_PORT AS SVR_PORT FROM %s WHERE TENANT_ID=%ld",
                         OB_DBA_OB_ACCESS_POINT_TNAME, tenant_id))) {
    LOG_WARN("fail to generate sql");
  } else if (OB_FAIL(construct_server_ip_list(sql, addrs))) {
    LOG_WARN("failed to get server ip list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogRestoreProxyUtil::get_server_addr(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT distinct(SVR_IP), SVR_PORT FROM %s WHERE TENANT_ID=%ld",
                             OB_GV_OB_LOG_STAT_TNAME, tenant_id))) {
    LOG_WARN("fail to generate sql");
  } else if (OB_FAIL(construct_server_ip_list(sql, addrs))) {
    LOG_WARN("failed to get server ip list", KR(ret), K(sql));
  }
  return ret;
}

int ObLogRestoreProxyUtil::construct_server_ip_list(const common::ObSqlString &sql, common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql is empty", KR(ret), K(sql));
  } else {
    RESTORE_RETRY(SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObMySQLResult *res = NULL;
      if (OB_FAIL(sql_proxy_.read(result, OB_INVALID_TENANT_ID, sql.ptr()))) {
        LOG_WARN("read value from DBA_OB_ACCESS_POINT failed", K(tenant_id_), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query result is null", K(tenant_id_), K(sql));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(res->next())) {
          ObString tmp_ip;
          int32_t tmp_port;
          ObAddr addr;

          EXTRACT_VARCHAR_FIELD_MYSQL(*res, "SVR_IP", tmp_ip);
          EXTRACT_INT_FIELD_MYSQL(*res, "SVR_PORT", tmp_port, int32_t);

          if (OB_FAIL(ret)) {
            LOG_WARN("fail to get server ip and sql port", K(tmp_ip), K(tmp_port), K(tenant_id_), K(sql));
          } else if (!addr.set_ip_addr(tmp_ip, tmp_port)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to set addr", K(tmp_ip), K(tmp_port), K(tenant_id_), K(sql));
          } else if (!addr.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("addr is invalid", K(addr), K(tenant_id_), K(sql));
          } else if (OB_FAIL(addrs.push_back(addr))) {
            LOG_WARN("fail to push back addr to addrs", K(addr), K(tenant_id_), K(sql));
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get ip list", K(tenant_id_));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    )
  }
  return ret;
}

bool ObLogRestoreProxyUtil::is_user_changed_(const char *user_name, const char *user_password)
{
  bool changed =  user_name_ != common::ObFixedLengthString<common::OB_MAX_USER_NAME_BUF_LENGTH>(user_name)
    || user_password_ != common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH + 1>(user_password);

  if (changed) {
    LOG_INFO("restore proxy user info changed", K(user_name_),
        K(common::ObFixedLengthString<common::OB_MAX_USER_NAME_BUF_LENGTH>(user_name)));
  }
  return changed;
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
    RESTORE_RETRY(
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
    )
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
    RESTORE_RETRY(
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
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("get max log info failed", K(sql), K(id));
          } else {
            LOG_WARN("next failed", K(sql), K(id));
          }
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
    )
  }
  return ret;
}

int ObLogRestoreProxyUtil::is_ls_existing(const ObLSID &id)
{
  int ret = OB_SUCCESS;
  const char *LS_ID = "LS_ID";
  common::ObMySQLProxy *proxy = &sql_proxy_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(id));
  } else {
    RESTORE_RETRY(
      SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        common::ObSqlString sql;
        const char *GET_LS_SQL = "SELECT COUNT(1) as COUNT FROM %s WHERE %s=%ld";
        if (OB_FAIL(sql.append_fmt(GET_LS_SQL, OB_DBA_OB_LS_TNAME,  LS_ID, id.id()))) {
          LOG_WARN("append_fmt failed");
        } else if (OB_FAIL(proxy->read(res, sql.ptr()))) {
          LOG_WARN("excute sql failed", K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("next failed", K(id), K(sql));
        } else {
          uint64_t count = 0;
          EXTRACT_UINT_FIELD_MYSQL(*result, "COUNT", count, uint64_t);
          if (OB_SUCC(ret) && 0 == count) {
            ret = OB_LS_NOT_EXIST;
          }
        }
      }
    )
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

int ObLogRestoreProxyUtil::detect_tenant_mode_(common::sqlclient::ObMySQLServerProvider *server_provider,
                                               const char *user_name,
                                               const char *user_password,
                                               common::ObIArray<common::ObAddr> *fixed_server_list)
{
  LOG_INFO("start to detec tenant mode");
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> invalid_ip_array;
  if (OB_ISNULL(server_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[RESTORE PROXY] invalid server_provider", KR(ret));
  } else {
    const int64_t svr_cnt = server_provider->get_server_count();
    if (OB_UNLIKELY(0 >= svr_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[RESTORE PROXY] expect valid svr_cnt", K(svr_cnt));
    } else {
      bool detect_succ = false;
      for (int idx = 0; idx < svr_cnt; idx++) {
        MYSQL *mysql;
        common::ObAddr server;
        if (OB_FAIL(server_provider->get_server(idx, server))) {
          LOG_WARN("[RESTORE PROXY] failed to get server", KR(ret), K(idx), K(svr_cnt));
        } else {
          const static int MAX_IP_BUFFER_LEN = 32;
          char host[MAX_IP_BUFFER_LEN] = { 0 };
          const char *default_db_name = "";
          int32_t port = server.get_port();

          if (!server.ip_to_string(host, MAX_IP_BUFFER_LEN)) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("fail to get host.", K(server), K(ret));
          } else if (NULL == (mysql = mysql_init(NULL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("mysql_init fail", KR(ret));
          } else {
            int64_t final_timeout = cal_timeout_();
            mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &final_timeout);
            if (mysql != mysql_real_connect(mysql, host, user_name, user_password, default_db_name, port, NULL, 0)) {
              int mysql_error_code = mysql_errno(mysql);   // remove invalid ip if mysql error code is ER_CONNECT_FAILED
              if ((ER_CONNECT_FAILED == mysql_error_code) && OB_FAIL(invalid_ip_array.push_back(idx))) {
                LOG_WARN("[RESTORE PROXY] fail to push back invalid_ip_array", K(invalid_ip_array), K(idx));
              }
              LOG_WARN("[RESTORE PROXY] mysql connect failed", "mysql_error", mysql_error(mysql), K(mysql_error_code), K(host), K(port));
            } else {
 #ifdef OB_BUILD_ORACLE_PARSER
              is_oracle_mode_ = mysql->oracle_mode;
 #else
              is_oracle_mode_ = false;
 #endif
              detect_succ = true;
              LOG_INFO("[RESTORE PROXY] detect tenant mode success", KR(ret), K(host), K(port));
            }
            if (OB_NOT_NULL(mysql)) {
              mysql_close(mysql);
            }
          }
        }
        mysql = NULL;
      }
      if (OB_UNLIKELY(!detect_succ)) {
        ret = OB_CONNECT_ERROR;
        LOG_WARN("[RESTORE PROXY][DETECT_TENANT_MODE] connect to all server in tenant endpoint list failed", KR(ret), K(svr_cnt));
      } else if (!invalid_ip_array.empty()) {
        for (int64_t idx = invalid_ip_array.count() - 1; idx >= 0; idx--) {
          if (OB_FAIL(fixed_server_list->remove(invalid_ip_array.at(idx)))) {
            LOG_WARN("[RESTORE PROXY] fail to remove from fixed_server_list", K(invalid_ip_array), K(*fixed_server_list), K(idx));
          }
        }
        LOG_INFO("[RESTORE PROXY] fixed server list", K_(tenant_id), K(*fixed_server_list));
      }
      if (fixed_server_list->count() <= 0) {
        ret = OB_CONNECT_ERROR;
        LOG_WARN("[RESTORE PROXY] all servers are unreachable", K_(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int64_t ObLogRestoreProxyUtil::cal_timeout_() {
  const int64_t DEFAULT_MAX_TIMEOUT = 10; // default max time is 10s
  int64_t final_timeout = DEFAULT_MAX_TIMEOUT;
  int64_t abs_timeout_ts = THIS_WORKER.get_timeout_ts();
  int64_t curr_ts = ObTimeUtility::fast_current_time();
  int64_t abs_timeout = (abs_timeout_ts - curr_ts) /1000 /1000 /2; // half of the total timeout
  if (abs_timeout > 0) {
    final_timeout = abs_timeout < DEFAULT_MAX_TIMEOUT ? abs_timeout : DEFAULT_MAX_TIMEOUT;
  }
  LOG_INFO("[RESTORE PROXY] set time out,",
    K(abs_timeout_ts), K(curr_ts), K(abs_timeout), K(final_timeout), K(DEFAULT_MAX_TIMEOUT));
  return final_timeout;
}

} // namespace share
} // namespace oceanbase
