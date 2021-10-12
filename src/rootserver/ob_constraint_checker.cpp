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

#include "ob_constraint_checker.h"
#include "share/config/ob_server_config.h"
#include "share/ob_debug_sync.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase {
namespace rootserver {

ObConstraintChecker::ObConstraintChecker()
    : inited_(false),
      ddl_service_(NULL),
      server_manager_(NULL),
      schema_service_(NULL),
      sql_proxy_(NULL)
{}

ObConstraintChecker::~ObConstraintChecker()
{}

int ObConstraintChecker::init(ObRootService& root_service) 
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ddl_service_ = &root_service.get_ddl_service();
    schema_service_ = &root_service.get_schema_service();
    server_manager_ = &root_service.get_server_mgr();
    sql_proxy_ = &root_service.get_sql_proxy();
    inited_ = true;
  }
  return ret;
}

int ObConstraintChecker::check()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(CHECK_CONSTRAINTS);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(process_foreign_key())) {
    LOG_WARN("check foreign key failed", K(ret));
  } 

  return ret;
}

int ObConstraintChecker::process_foreign_key()
{
  int ret = OB_SUCCESS;
  common::ObArray<ConstraintCreatingInfo> creating_fks;

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(
          "SELECT tenant_id, foreign_key_id, child_table_id, svr_ip, svr_port, creating_time from %s"
          , OB_ALL_CREATING_FOREIGN_KEY_TNAME))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
      } else {
        ConstraintCreatingInfo cst_creating_info;

        if (OB_FAIL(result->get_int("tenant_id", cst_creating_info.tenant_id_))) {
          LOG_WARN("get tenant id failed", K(ret));
        } else if (OB_FAIL(result->get_int("foreign_key_id", cst_creating_info.constraint_id_))) {
          LOG_WARN("get foreign key id failed", K(ret));
        } else if (OB_FAIL(result->get_int("child_table_id", cst_creating_info.table_id_))) {
          LOG_WARN("get child table id failed", K(ret));
        } else if (OB_FAIL(result->get_varchar("svr_ip", cst_creating_info.ip_))) {
          LOG_WARN("get svr_ip failed", K(ret));
        } else if (OB_FAIL(result->get_int("svr_port", cst_creating_info.port_))) {
          LOG_WARN("get svr_port failed", K(ret));
        } else if (OB_FAIL(result->get_int("creating_time", cst_creating_info.creating_time_))) {
          LOG_WARN("get creating time failed", K(ret));
        } else {
          if (OB_FAIL(creating_fks.push_back(cst_creating_info))) {
            LOG_WARN("fail to push back element to array", K(ret));
          }
        }
      }
    }
  }

  for(int i = 0; OB_SUCC(ret) && i < creating_fks.size(); i++) {
    ConstraintCreatingInfo& creating_fk = creating_fks.at(i);
    bool invalid = false;

    if (OB_FAIL(check_fk_valid(creating_fk, invalid))) {
      LOG_WARN("fail to check fk valid", K(ret));
    } else if (invalid && OB_FAIL(rollback_foreign_key(
          creating_fk.tenant_id_, creating_fk.constraint_id_, creating_fk.table_id_))) {
      LOG_WARN("fail to rollback invalid foreign key", K(creating_fk.tenant_id_), K(creating_fk.constraint_id_), K(creating_fk.table_id_), K(ret));
    }
  }
  return ret;
}

int ObConstraintChecker::acquire_foreign_key_name(const uint64_t tenant_id, const uint64_t fk_id,
    const uint64_t child_table_id, common::ObString& foreign_key_name)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema* table_schema = NULL;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id ||OB_INVALID_ID == child_table_id || OB_INVALID_ID == fk_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(child_table_id), K(fk_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(child_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(child_table_id));
  } else if (OB_ISNULL(table_schema)) {
    LOG_WARN("table is dropped", K(tenant_id), K(child_table_id), K(fk_id));
  } else {
    const common::ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema->get_foreign_key_infos();
    bool find = false;
    for (int64_t i = 0; !find && i < foreign_key_infos.count(); i++) {
      const ObForeignKeyInfo& foreign_key_info = foreign_key_infos.at(i);
      if (foreign_key_info.foreign_key_id_ == fk_id) {
        find = true;
        foreign_key_name = foreign_key_info.foreign_key_name_;
      }
    }
  }

  return ret;
}

int ObConstraintChecker::check_fk_valid(ConstraintCreatingInfo& creating_fk, bool& invalid)
{
  int ret = OB_SUCCESS;
  invalid = false;
  
  common::ObAddr server;
  server.set_ip_addr(creating_fk.ip_, creating_fk.port_);
  bool permanent_offline = false;
  bool exist = false;
  bool alive = false;
    
  if (OB_FAIL(server_manager_->is_server_exist(server, exist))) {
    LOG_WARN("check server states failed", K(server), K(ret));
  } else if (!exist) {
    invalid = true;
  } else if (OB_FAIL(server_manager_->check_server_permanent_offline(server, permanent_offline))) {
    LOG_WARN("check server states failed", K(server), K(ret));
  } else if (permanent_offline) {
    invalid = true;
  } else if (OB_FAIL(server_manager_->check_server_alive(server, alive))) {
    LOG_WARN("check server states failed", K(server), K(ret));
  } else if (alive) {
    ObServerStatus status;
    if (OB_FAIL(server_manager_->get_server_status(server, status))) {
      LOG_WARN("fail to get server_status", K(server), K(ret));
    } else if (creating_fk.creating_time_ < status.start_service_time_) {
      invalid = true;
    }
  } else {
    // do nothing until the server status changes to alive or permanent offline
  }
    
  return ret;
}

int ObConstraintChecker::rollback_foreign_key(const uint64_t tenant_id, const uint64_t fk_id, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  common::ObString foreign_key_name;
  if (OB_FAIL(acquire_foreign_key_name(tenant_id, fk_id, table_id, foreign_key_name))) {
    LOG_WARN("fail to acquire foreign key name", K(tenant_id), K(fk_id), K(table_id), K(ret));
  } else {
    obrpc::ObDropForeignKeyArg drop_foreign_key_arg;
    drop_foreign_key_arg.foreign_key_name_ = foreign_key_name;
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);

    ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema* table_schema = NULL;
    ObDDLSQLTransaction trans(schema_service_);

    if (OB_FAIL(trans.start(sql_proxy_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      // ret = OB_SUCCESS;
      LOG_WARN("table is dropped", K(ret), K(fk_id));
    } else if (OB_FAIL(ddl_operator.alter_table_drop_foreign_key(*table_schema, drop_foreign_key_arg, trans))) {
      LOG_WARN("drop foreign key failed", K(ret), K(fk_id));
    } else {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
