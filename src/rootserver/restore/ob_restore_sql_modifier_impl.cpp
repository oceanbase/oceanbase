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

#include "ob_restore_sql_modifier_impl.h"
#include "share/ob_rpc_struct.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_array_iterator.h"
#include "sql/ob_result_set.h"
#include "share/ob_kv_parser.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/cmd/ob_resource_stmt.h"
#include "rootserver/ob_server_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::rootserver;

ObRestoreSQLModifierImpl::ObRestoreSQLModifierImpl(
    share::ObRestoreArgs& restore_args, hash::ObHashSet<uint64_t>& dropped_index_ids)
    : tenant_name_(), restore_args_(restore_args), dropped_index_ids_(dropped_index_ids)
{}

ObRestoreSQLModifierImpl::~ObRestoreSQLModifierImpl()
{}

/*
 * logical restore tenant will run in two different modes:
 * 1. Inplace restore:
 *    Run logical restore tenant cmd on backup cluster, using existed resource(unit_config, resource_pool)
 *    and backup tenant options(primary_zone, locality). User should drop tenant first before restore tenant in place.
 *
 * 2. Not inplace restore:
 *    Run logical restore tenant cmd on other cluster. Normally, we must specify parameters below
 *    to overwrite backup parameters while restore tenant. In most cases, we won't restore tenant in place.
 *    2.1 pool_list: must be specific, pool_list should be created first before restore tenant.
 *    2.2 locality, primary_zone: optional.
 */

int ObRestoreSQLModifierImpl::modify(ObResultSet& rs)
{
  int ret = OB_SUCCESS;
  ObICmd* cmd = const_cast<ObICmd*>(rs.get_cmd());
  if (NULL != cmd) {
    int cmd_type = cmd->get_cmd_type();
    switch (cmd_type) {
      case stmt::T_CREATE_TENANT: {
        ret = handle_create_tenant(dynamic_cast<ObCreateTenantStmt*>(cmd));
        break;
      }
      case stmt::T_CREATE_DATABASE: {
        ret = handle_create_database(dynamic_cast<ObCreateDatabaseStmt*>(cmd));
        break;
      }
      case stmt::T_CREATE_TABLE: {
        ret = handle_create_table(dynamic_cast<ObCreateTableStmt*>(cmd));
        break;
      }
      case stmt::T_CREATE_INDEX: {
        ret = handle_create_index(dynamic_cast<ObCreateIndexStmt*>(cmd));
        break;
      }
      case stmt::T_CREATE_TABLEGROUP: {
        ret = handle_create_tablegroup(dynamic_cast<ObCreateTablegroupStmt*>(cmd));
        break;
      }

      default:
        break;
    }
    LOG_INFO("modified cmd in result set", K(restore_args_), K(cmd_type), K(ret));
  }
  return ret;
}

int ObRestoreSQLModifierImpl::handle_create_tenant(ObCreateTenantStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail cast cmd to create_tenant_stmt", K(ret));
  } else {
    stmt->set_tenant_name(tenant_name_);
    if (OB_FAIL(stmt->set_tcp_invited_nodes(ObString(restore_args_.tcp_invited_nodes_)))) {
      LOG_WARN("fail to set tcp_invited_nodes",
          K_(tenant_name),
          "tcp_invited_nodes",
          restore_args_.tcp_invited_nodes_,
          K(ret));
    } else if (!restore_args_.is_inplace_restore()) {
      ObSEArray<ObString, 8> pools;
      ObString pool_list(restore_args_.pool_list_);
      if (OB_FAIL(split_on(pool_list, ',', pools))) {
        LOG_WARN("fail split pool list to pools", K(pool_list), K(ret));
      } else if (OB_FAIL(stmt->set_resource_pool(pools))) {
        LOG_WARN("fail set resource pool", K(pools), K(ret));
      } else {
        stmt->set_primary_zone(ObString(restore_args_.primary_zone_));
        stmt->set_locality(ObString(restore_args_.locality_));
      }
    }
  }
  return ret;
}

int ObRestoreSQLModifierImpl::handle_create_database(ObCreateDatabaseStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail cast cmd to create database stmt", K(ret));
  } else {
    if (!restore_args_.is_inplace_restore()) {
      stmt->set_primary_zone(ObString(restore_args_.primary_zone_));
    }
    const ObString& database_name = stmt->get_database_name();
    if (database_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX) || database_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX)) {
      stmt->set_in_recyclebin(true);
    }
  }
  return ret;
}

int ObRestoreSQLModifierImpl::handle_create_table(ObCreateTableStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail cast cmd to create table", K(ret));
  } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == stmt->get_database_name()) {
    ret = OB_IGNORE_SQL_IN_RESTORE;
  } else {
    if (!restore_args_.is_inplace_restore()) {
      stmt->set_primary_zone(ObString(restore_args_.primary_zone_));
      stmt->set_locality(ObString(restore_args_.locality_));
    }
    stmt->set_create_mode(obrpc::OB_CREATE_TABLE_MODE_RESTORE);
    // use backup table_id to address backup user table data from oss/nfs.
    restore_args_.sql_info_.set_backup_table_id(stmt->get_table_id());
    // backup_table_name/backup_db_name is used to get new table_id after create table while restore tenant.
    if (OB_FAIL(restore_args_.sql_info_.set_backup_table_name(stmt->get_table_name()))) {
      LOG_WARN("fail set_backup_table_name", "table_name", stmt->get_table_name(), K(ret));
    } else if (OB_FAIL(restore_args_.sql_info_.set_backup_db_name(stmt->get_database_name()))) {
      LOG_WARN("fail set_backup_db_name", "db_name", stmt->get_database_name(), K(ret));
    }
    // create table with new table_id instead of backup table_id.
    stmt->invalidate_backup_table_id();
  }
  return ret;
}

int ObRestoreSQLModifierImpl::handle_create_index(ObCreateIndexStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail cast cmd to create index", K(ret));
  } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == stmt->get_database_name()) {
    ret = OB_IGNORE_SQL_IN_RESTORE;
  } else {
    int hash_ret = dropped_index_ids_.exist_refactored(stmt->get_data_index_id());
    if (OB_HASH_EXIST == hash_ret) {
      stmt->set_index_status(share::schema::INDEX_STATUS_INDEX_ERROR);
      LOG_INFO("handle_create_index as error", K(ret), "index_id", stmt->get_data_index_id(), K(*stmt));
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      stmt->set_index_status(share::schema::INDEX_STATUS_AVAILABLE);
      LOG_INFO("handle_create_index as available", K(ret), "index_id", stmt->get_data_index_id(), K(*stmt));
    } else {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid hash ret", K(ret), K(hash_ret), K(*stmt));
    }

    stmt->set_create_mode(obrpc::OB_CREATE_TABLE_MODE_RESTORE);
    // use backup table_id to address backup user table data from oss/nfs.
    restore_args_.sql_info_.set_backup_table_id(stmt->get_data_table_id());
    restore_args_.sql_info_.set_backup_index_id(stmt->get_data_index_id());
    // backup_index_name/backup_table_name/backup_db_name is used to
    // get new table_id after create table while restore tenant.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(restore_args_.sql_info_.set_backup_table_name(stmt->get_table_name()))) {
      LOG_WARN("fail set_backup_table_name", "table_name", stmt->get_table_name(), K(ret));
    } else if (OB_FAIL(restore_args_.sql_info_.set_backup_index_name(stmt->get_index_name()))) {
      LOG_WARN("fail set_backup_index_name", "index_name", stmt->get_index_name(), K(ret));
    } else if (OB_FAIL(restore_args_.sql_info_.set_backup_db_name(stmt->get_database_name()))) {
      LOG_WARN("fail set_backup_db_name", "db_name", stmt->get_database_name(), K(ret));
    }
    // create table with new table_id instead of backup table_id.
    stmt->invalidate_backup_index_id();
  }
  return ret;
}

int ObRestoreSQLModifierImpl::handle_create_tablegroup(ObCreateTablegroupStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail cast cmd to create tablegroup stmt", K(ret));
  } else {
    if (!restore_args_.is_inplace_restore()) {
      stmt->set_primary_zone(ObString(restore_args_.primary_zone_));
      stmt->set_locality(ObString(restore_args_.locality_));
    }
    stmt->set_create_mode(obrpc::OB_CREATE_TABLE_MODE_RESTORE);
    restore_args_.sql_info_.set_backup_tablegroup_id(stmt->get_tablegroup_id());
    if (OB_FAIL(restore_args_.sql_info_.set_backup_tablegroup_name(stmt->get_tablegroup_name()))) {
      LOG_WARN("fail set_backup_tablegroup_name", "tablegroup_name", stmt->get_tablegroup_name(), K(ret));
    }
  }
  return ret;
}
