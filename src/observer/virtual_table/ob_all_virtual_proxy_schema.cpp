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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_proxy_schema.h"
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak
#include "observer/ob_server_struct.h" // GCTX
#include "observer/ob_inner_sql_result.h"
#include "share/ob_errno.h" // KR(ret)
#include "share/schema/ob_part_mgr_util.h" // ObPartitionIterator
#include "share/schema/ob_schema_mgr.h" // ObSimpleDatabaseSchema
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_ALL_XXX_TNAME
#include "share/location_cache/ob_location_service.h" // ObLocationService
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "sql/session/ob_sql_session_info.h" // ObSqlSessionInfo
#include "sql/parser/ob_parser.h" // ObParser
#include "sql/resolver/dml/ob_select_resolver.h" // ObSelectResolver
#include "sql/resolver/ob_resolver_utils.h" // ObResolverUtils

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace observer
{
void ObAllVirtualProxySchema::ObTenantServer::reset()
{
  virtual_tablet_id_.reset();
  location_.reset();
}

int ObAllVirtualProxySchema::ObTenantServer::set_location(
    const share::ObLSReplicaLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!location.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(location));
  } else if (OB_FAIL(location_.assign(location))) {
    LOG_WARN("fail to assign", KR(ret), K(location));
  }
  return ret;
}

ObAllVirtualProxySchema::ObAllVirtualProxySchema()
    : ObVirtualTableIterator(),
      is_inited_(false),
      inner_alloc_("ProxySchemSQL"),
      convert_alloc_("ConvertAlloc"),
      force_sql_refresh_(false),
      next_table_idx_(0),
      next_replica_idx_(0),
      next_server_idx_(-1),
      input_tenant_name_(),
      input_db_name_(),
      input_table_names_(),
      level1_decoded_db_name_(),
      level1_decoded_table_name_(),
      level2_decoded_db_name_(),
      level2_decoded_table_name_(),
      complex_table_type_(CT_DEFAULT),
      table_schemas_(),
      tablet_ids_(),
      tenant_servers_(),
      sql_res_(NULL),
      location_(),
      schema_guard_(share::schema::ObSchemaMgrItem::MOD_VIRTUAL_TABLE),
      schema_service_(NULL),
      location_service_(NULL),
      sql_proxy_(NULL)
{
  MEMSET(ip_buf_, 0 , sizeof(ip_buf_));
}

ObAllVirtualProxySchema::~ObAllVirtualProxySchema()
{
  if (sql_res_ != NULL) {
    sql_res_->~ReadResult();
    sql_res_ = NULL;
  }
}

int ObAllVirtualProxySchema::init(
    const bool force_sql_refresh,
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::ObLocationService *location_service,
    common::ObMySQLProxy *sql_proxy,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(location_service)
      || OB_ISNULL(sql_proxy)
      || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret),
        KP(location_service), KP(sql_proxy), KP(allocator));
  } else {
    force_sql_refresh_ = force_sql_refresh;
    schema_service_ = &schema_service;
    location_service_ = location_service;
    sql_proxy_ = sql_proxy;
    allocator_ = allocator;
    // inner variables init
    next_table_idx_ = 0;
    next_replica_idx_ = 0;
    next_server_idx_ = -1;
    input_tenant_name_.reset();
    input_db_name_.reset();
    input_table_names_.reset();
    level1_decoded_db_name_.reset();
    level1_decoded_table_name_.reset();
    level2_decoded_db_name_.reset();
    level2_decoded_table_name_.reset();
    complex_table_type_ = CT_DEFAULT;
    table_schemas_.reset();
    tablet_ids_.reset();
    tenant_servers_.reset();
    location_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualProxySchema::init_convert_ctx()
{
  int ret = OB_SUCCESS;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
  ObCastCtx cast_ctx(&convert_alloc_, &dtc_params, CM_NONE, table_schema_->get_collation_type());
  cast_ctx_ = cast_ctx;

  ObObj *cells = NULL;
  void *tmp_ptr = NULL;
  if (OB_UNLIKELY(NULL == allocator_ || NULL == table_schema_ || NULL == session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(allocator_));
  } else if (OB_ISNULL(tmp_ptr = allocator_->alloc(
      reserved_column_cnt_ <= 0 ? 1 * sizeof(ObObj): reserved_column_cnt_ * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cells", K(ret), K(reserved_column_cnt_));
  } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[reserved_column_cnt_ <= 0 ? 1 : reserved_column_cnt_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new cell array", K(ret), K(reserved_column_cnt_));
  } else {
    convert_row_.cells_ = cells;
    convert_row_.count_ = reserved_column_cnt_;
  }
  return ret;
}

int ObAllVirtualProxySchema::convert_output_row(ObNewRow *&cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is NULL", K(ret));
  } else {
    convert_alloc_.reuse();
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schema(OB_SYS_TENANT_ID,
                                               OB_ALL_VIRTUAL_PROXY_SCHEMA_TID,
                                               table_schema))) {
      LOG_WARN("get table schema failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(column_id);
      if (cur_row->get_cell(i).is_null() ||
          (cur_row->get_cell(i).is_string_type() && 0 == cur_row->get_cell(i).get_data_length())) {
        convert_row_.cells_[i].set_null();
      } else if (OB_FAIL(ObObjCaster::to_type(col_schema->get_data_type(),
                                              col_schema->get_collation_type(),
                                              cast_ctx_,
                                              cur_row->get_cell(i),
                                              convert_row_.cells_[i]))) {
        LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
      }
    }
    cur_row = &convert_row_;
  }
  return ret;
}

int ObAllVirtualProxySchema::gen_column_value(char *&buf, int64_t len,
                                              const ObString &str, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard guard(is_oracle_mode ? lib::Worker::CompatMode::ORACLE :
                             lib::Worker::CompatMode::MYSQL);
  ObObj col_obj;
  col_obj.set_varchar(str);
  int64_t pos = 0;
  ObObjPrintParams print_params;
  if (OB_FAIL(col_obj.print_sql_literal(buf, len, pos, print_params))) {
    LOG_WARN("failed to print column value", K(ret), K(col_obj));
  }
  return ret;
}


int ObAllVirtualProxySchema::inner_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP_(schema_service));
  //FIXME: should not use cluster level schema guard
  } else if (OB_FAIL(schema_service_->get_cluster_schema_guard(schema_guard_))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else {
    const int64_t ROW_KEY_COUNT = 6;
    int64_t exec_tenant_id = OB_INVALID_ID;
    ObRowkey start_key;
    ObRowkey end_key;
    ObString tenant_name;
    bool is_oracle_tenant = false;
    ObString database_name;
    ObString table_name;
    const ObObj *start_key_obj_ptr = NULL;
    const ObObj *end_key_obj_ptr = NULL;
    const ObTableSchema *table_schema = NULL;
    const ObRoutineInfo *routine_info = NULL;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    uint64_t database_id = OB_INVALID_ID;
    uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    const ObTenantSchema *tenant_schema = NULL;

    // get table schema
    for (int64_t i = 0; (OB_SUCC(ret)) && (i < key_ranges_.count()); ++i) {
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      if ((ROW_KEY_COUNT != start_key.get_obj_cnt()) || (ROW_KEY_COUNT != end_key.get_obj_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_name, database_name and table_name must all be specified");
        LOG_WARN("row key count is not correct", KR(ret), K(ROW_KEY_COUNT),
            "start key count", start_key.get_obj_cnt(), "end key count", end_key.get_obj_cnt());
      } else {
        start_key_obj_ptr = start_key.get_obj_ptr();
        end_key_obj_ptr = end_key.get_obj_ptr();

        for (int64_t j = 0; (OB_SUCC(ret)) && (j < 3); ++j) {
          if (start_key_obj_ptr[j].is_min_value()
              || end_key_obj_ptr[j].is_max_value()
              || !start_key_obj_ptr[j].is_varchar_or_char()
              || !end_key_obj_ptr[j].is_varchar_or_char()
              || (start_key_obj_ptr[j] != end_key_obj_ptr[j])) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name, database_name and table_name (must all be specified)");
            LOG_WARN("invalid keys", KR(ret),
                "start key obj", start_key_obj_ptr[j], "end key obj", end_key_obj_ptr[j]);
          } else {
            switch (j) {
              case 0: {// tenant_name
                tenant_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 1: {// database_name
                database_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 2: {// table_name
                table_name = end_key_obj_ptr[j].get_string();
                break;
              }
            }//end of switch
          }//end of else
        }//end of for

        if (OB_SUCC(ret)) {
          if ((start_key_obj_ptr[3].is_min_value()) && (end_key_obj_ptr[3].is_max_value())) {
            tablet_id = ObTabletID::INVALID_TABLET_ID;
          } else {
            tablet_id = start_key_obj_ptr[3].get_int(); // int64_t to uint64_t
          }
        }

        if (OB_SUCC(ret)) { // check tenant_name
          if (OB_UNLIKELY(!input_tenant_name_.empty() && (0 != input_tenant_name_.case_compare(tenant_name)))) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "different tenant_names in a batch query");
            LOG_WARN("unexpected different tenant_names", KR(ret), K(tenant_name), K_(input_tenant_name));
          } else if (OB_FAIL(schema_guard_.get_tenant_info(tenant_name, tenant_schema))) {
            LOG_WARN("fail to get tenant info", KR(ret), K(tenant_name));
          } else if (OB_ISNULL(tenant_schema)) {
            LOG_TRACE("tenant not exist", K(tenant_name)); // skip
          } else {
            tenant_id = tenant_schema->get_tenant_id();
            is_oracle_tenant = tenant_schema->is_oracle_tenant();
            if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid effective_tenant_id", KR(ret), K_(effective_tenant_id));
            } else if (!is_sys_tenant(effective_tenant_id_) && (tenant_id != effective_tenant_id_)) {
              LOG_TRACE("unprivileged tenant", K(tenant_name), K(tenant_id), K_(effective_tenant_id)); // skip
              tenant_id = OB_INVALID_TENANT_ID;  // vtable return nothing
            } else if (is_sys_tenant(effective_tenant_id_) && (tenant_id != effective_tenant_id_)) {
              exec_tenant_id = tenant_id;
            } else {
              // correct input tenant_name, do nothing. (all tenant's table is visible in sys tenant)
            }
          }
        }
      }

      if (OB_SUCC(ret)) { // check database_name
        if (OB_UNLIKELY(!input_db_name_.empty() && (0 != input_db_name_.case_compare(database_name)))) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "different database_names in a batch query");
        }
      }

      if (OB_SUCC(ret)) {
        table_schema = NULL;
        input_tenant_name_ = tenant_name;
        input_db_name_ = database_name;

        //1. try default
        if (!is_valid_tenant_id(tenant_id)) {
          // skip, vtable will return nothing
        } else if (OB_FAIL(schema_guard_.get_database_id(tenant_id, database_name, database_id))) {
          LOG_WARN("database is not exist", KR(ret), K(tenant_id), K(database_name));
        } else if (OB_INVALID_ID == database_id) {
          // unknown db, return succ
          ret = OB_SUCCESS;
        } else if (OB_FAIL(input_table_names_.push_back(table_name))) {
          LOG_WARN("fail to push back table name", KR(ret), K(table_name));
        } else if (OB_FAIL(tablet_ids_.push_back(ObTabletID(tablet_id)))) {
          LOG_WARN("fail to push back partition_ids", KR(ret), K(tablet_id));
        }
      }
    } // end for key_ranges_

    if (OB_FAIL(ret)) {
    } else if (0 == input_table_names_.count()) {
      // do-nothing
    } else if (OB_INVALID_ID == exec_tenant_id) {
      ret = init_data();
    } else {
      ObSEArray<ObString, 16> column_names;
      for (int col_idx = 0; OB_SUCC(ret) && col_idx < output_column_ids_.count(); ++col_idx) {
        int64_t column_id = output_column_ids_.at(col_idx);
        ObString column_name;
        bool is_column_exist = false;
        table_schema_->get_column_name_by_column_id(column_id, column_name, is_column_exist);
        if (!is_column_exist) {
          ret = OB_ERR_COLUMN_NOT_FOUND;
          LOG_WARN("get column name failed", KR(ret));
        } else if (OB_FAIL(column_names.push_back(column_name))) {
          LOG_WARN("add column name failed", KR(ret));
        }
      }
      ObSqlString sql;
      for (int col_idx = 0; OB_SUCC(ret) && col_idx < column_names.count(); ++col_idx) {
        if (0 == col_idx) {
          if (OB_FAIL(sql.append_fmt("SELECT %.*s", column_names[col_idx].length(), column_names[col_idx].ptr()))) {
            LOG_WARN("fail to append_fmt", K(ret));
          }
        } else {
          if (OB_FAIL(sql.append_fmt(", %.*s", column_names[col_idx].length(), column_names[col_idx].ptr()))) {
            LOG_WARN("fail to append_fmt", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.append_fmt(" FROM %s.%s WHERE",
          is_oracle_tenant ? OB_ORA_SYS_SCHEMA_NAME : OB_SYS_DATABASE_NAME,
          is_oracle_tenant ? OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TNAME : OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME))) {
        LOG_WARN("fail to append_fmt", K(ret));
      }
      char *value_buf = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(value_buf = (char*)inner_alloc_.alloc(OB_MAX_SQL_LENGTH))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < input_table_names_.count(); ++i) {
        const ObString &table_name = input_table_names_.at(i);
        const ObTabletID &tablet_id = tablet_ids_.at(i);
        if (i != 0) {
          if (OB_FAIL(sql.append_fmt(" OR"))) {
            LOG_WARN("fail to append_fmt", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (tablet_id.is_valid()) {
          if (OB_FAIL(sql.append_fmt(" (TENANT_NAME, DATABASE_NAME, TABLE_NAME, TABLET_ID) = ('%.*s'",
                                     input_tenant_name_.length(), input_tenant_name_.ptr()))) {
            LOG_WARN("fail to append_fmt", K(ret));
          } else if (OB_FAIL(gen_column_value(value_buf, OB_MAX_SQL_LENGTH, input_db_name_,
                                              is_oracle_tenant))) {
            LOG_WARN("fail to gen_column_value", K(ret));
          } else if (OB_FAIL(sql.append_fmt(", %s", value_buf))) {
            LOG_WARN("fail to append_fmt", K(ret));
          } else if (OB_FAIL(gen_column_value(value_buf, OB_MAX_SQL_LENGTH, table_name,
                                              is_oracle_tenant))) {
            LOG_WARN("fail to gen_column_value", K(ret));
          } else if (OB_FAIL(sql.append_fmt(", %s", value_buf))) {
            LOG_WARN("fail to append_fmt", K(ret));
          } else if (OB_FAIL(sql.append_fmt(", %lu)", tablet_id.id()))) {
            LOG_WARN("fail to append_fmt", K(ret));
          }
        } else {
          if (OB_FAIL(sql.append_fmt(" (TENANT_NAME, DATABASE_NAME, TABLE_NAME) = ('%.*s'",
                                     input_tenant_name_.length(), input_tenant_name_.ptr()))) {
            LOG_WARN("fail to append_fmt", K(ret));
          } else if (OB_FAIL(gen_column_value(value_buf, OB_MAX_SQL_LENGTH, input_db_name_,
                                              is_oracle_tenant))) {
            LOG_WARN("fail to gen_column_value", K(ret));
          } else if (OB_FAIL(sql.append_fmt(", %s", value_buf))) {
            LOG_WARN("fail to append_fmt", K(ret));
          } else if (OB_FAIL(gen_column_value(value_buf, OB_MAX_SQL_LENGTH, table_name,
                                              is_oracle_tenant))) {
            LOG_WARN("fail to gen_column_value", K(ret));
          } else if (OB_FAIL(sql.append_fmt(", %s)", value_buf))) {
            LOG_WARN("fail to append_fmt", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        common::ObCommonSqlProxy *user_sql_proxy = NULL;
        common::ObOracleSqlProxy oracle_sql_proxy;
        common::ObMySQLProxy::MySQLResult *sql_res = NULL;
        if (OB_FAIL(oracle_sql_proxy.init(GCTX.sql_proxy_->get_pool()))) {
          LOG_WARN("fail to init oracle sql proxy", K(ret));
        } else if (FALSE_IT(user_sql_proxy = is_oracle_tenant ?
                            (common::ObCommonSqlProxy*)&oracle_sql_proxy : GCTX.sql_proxy_)) {
        } else if (OB_ISNULL(sql_res = OB_NEWx(ObMySQLProxy::MySQLResult, (&inner_alloc_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate result failed", K(ret));
        } else if (OB_FAIL(user_sql_proxy->read(*sql_res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(sql_res->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL sql executing result", KR(ret), K(exec_tenant_id), K(sql));
        } else {
          sql_res_ = sql_res;
        }
        if (OB_FAIL(ret) && sql_res != NULL) {
          sql_res->~ReadResult();
          sql_res = NULL;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(init_convert_ctx())) {
        LOG_WARN("fail to init convert context", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::init_data()
{
  int ret = OB_SUCCESS;

  const ObString &tenant_name = input_tenant_name_;
  const ObString &database_name = input_db_name_;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  const ObTenantSchema *tenant_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  if (OB_FAIL(schema_guard_.get_tenant_info(tenant_name, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_name));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant not exists", KR(ret), K(tenant_name));
  } else if (FALSE_IT(tenant_id = tenant_schema->get_tenant_id())) {
  } else if (OB_FAIL(schema_guard_.get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("fail to get database id", KR(ret), K(tenant_name), K(database_name));
  } else if (OB_INVALID_ID == database_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database not exist", KR(ret), K(tenant_name), K(database_name));
  }

  for (int i = 0; OB_SUCC(ret) && i < input_table_names_.count(); i++) {
    const ObString &table_name = input_table_names_.at(i);
    if (OB_FAIL(schema_guard_.get_table_schema(
                                               tenant_id,
                                               database_id,
                                               table_name,
                                               false,
                                               table_schema))) {
      LOG_WARN("get table schema failed", KR(ret), K(tenant_name), K(database_name), K(table_name));
    } else if (OB_ISNULL(table_schema)) {
      if (OB_FAIL(schema_guard_.get_table_schema(
                                                 tenant_id,
                                                 database_id,
                                                 table_name,
                                                 true,
                                                 table_schema))) {
        LOG_WARN("get table schema failed", KR(ret), K(tenant_name), K(database_name), K(table_name));
      } else if (OB_ISNULL(table_schema)) {
        LOG_TRACE("table does not exist", KR(ret), K(tenant_name), K(database_name), K(table_name));
      } // unknown table, return succ
    }

    //2. try synonym
    if (OB_SUCC(ret)
        && OB_ISNULL(table_schema)
        && is_valid_tenant_id(tenant_id)
        && OB_INVALID_ID != database_id) {
      LOG_TRACE("try synonym", K(tenant_name), K(database_name), K(table_name));
      uint64_t object_database_id = OB_INVALID_ID;
      uint64_t synonym_id = OB_INVALID_ID;
      ObString object_table_name;
      const ObSimpleDatabaseSchema *database_schema = NULL;
      bool exist = false;
      sql::ObSchemaChecker schema_checker;
      sql::ObSynonymChecker synonym_checker;
      if (OB_FAIL(schema_checker.init(schema_guard_))) {
        LOG_WARN("failed to init schema checker", KR(ret), K(tenant_id), K(database_id), K(table_name));
      } else if (OB_FAIL(sql::ObResolverUtils::resolve_synonym_object_recursively(
          schema_checker,
          synonym_checker,
          tenant_id,
          database_id,
          table_name,
          object_database_id,
          object_table_name,
          exist))) {
        LOG_WARN("resolve_synonym_object_recursively failed", KR(ret), K(tenant_id),
            K(database_id), K(table_name), K(object_database_id), K(object_table_name), K(exist));
      } else if (!exist) {
        //break
      } else if (OB_FAIL(ob_write_string(*allocator_, object_table_name, level1_decoded_table_name_))) {
        LOG_WARN("ob_write_string failed", KR(ret), K(object_table_name));
      } else if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, object_database_id, database_schema))) {
        LOG_WARN("get_database_schema failed", KR(ret), K(tenant_id), K(object_database_id));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database schema is null", KR(ret), K(object_database_id));
      } else if (OB_FAIL(ob_write_string(
                                         *allocator_,
                                         database_schema->get_database_name_str(),
                                         level1_decoded_db_name_))) {
        LOG_WARN("ob_write_string failed", KR(ret), K(database_schema->get_database_name_str()));
      } else if (OB_FAIL(schema_guard_.get_table_schema(
                                                        tenant_id,
                                                        object_database_id,
                                                        object_table_name,
                                                        false,
                                                        table_schema))) {
        LOG_WARN("get table schema failed", KR(ret),
                 K(tenant_name), K(object_database_id), K(object_table_name));
      } else if (OB_ISNULL(table_schema)) {
        if (OB_FAIL(schema_guard_.get_table_schema(
                                                   tenant_id,
                                                   object_database_id,
                                                   object_table_name,
                                                   true,
                                                   table_schema))) {
          LOG_WARN("get table schema failed", KR(ret),
                   K(tenant_name), K(object_database_id), K(object_table_name));
        } else if (OB_ISNULL(table_schema)) {
          // unknown table, return succ
          LOG_TRACE("table does not exist", K(tenant_name), K(object_database_id), K(object_table_name));
        } else {
          complex_table_type_ = CT_SYNONYM;
        }
      } else {
        complex_table_type_ = CT_SYNONYM;
      }
    }

    //3. try view
    if (OB_SUCC(ret) && OB_NOT_NULL(table_schema) && table_schema->is_view_table()) {
      LOG_TRACE("try view", K(tenant_name), K(database_name), K(table_name), K_(complex_table_type),
                K_(level1_decoded_db_name), K_(level1_decoded_table_name), KPC(table_schema));
      const common::ObString &view_definition = table_schema->get_view_schema().get_view_definition_str();
      const ObTableSchema *new_table_schema = NULL;
      bool is_oracle_mode = false;
      if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check oracle mode", KR(ret), KPC(table_schema));
      } else if (OB_FAIL(get_view_decoded_schema_(
                                                  tenant_id,
                                                  tenant_name,
                                                  view_definition,
                                                  is_oracle_mode,
                                                  new_table_schema))) {
        LOG_WARN("get_view_decoded_schema failed", KR(ret));
      } else if (OB_NOT_NULL(new_table_schema)) {
        table_schema = new_table_schema;
      }
    }

    // if table schema doesn't exist, must confirm current schema manager's version
    // greater than received_broadcast_version
    if (OB_SUCC(ret)
        && OB_ISNULL(table_schema)
        && is_valid_tenant_id(tenant_id)
        && OB_INVALID_ID != database_id) {
      int64_t received_broadcast_version = OB_INVALID_VERSION;
      int64_t refreshed_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_->get_tenant_received_broadcast_version(
                                                                         tenant_id,
                                                                         received_broadcast_version))) {
        LOG_WARN("fail to get tenant received broadcast version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard_.get_schema_version(tenant_id, refreshed_version))) {
        LOG_WARN("fail to get schema guard version", KR(ret), K(tenant_id));
      } else if (OB_CORE_SCHEMA_VERSION >= received_broadcast_version
                 || refreshed_version < received_broadcast_version) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("schema not exist, need retry", KR(ret), K(tenant_id),
                 K(received_broadcast_version), K(refreshed_version));
      } else {
        // do noting
      }
    }

    if ((OB_SUCC(ret)) && OB_NOT_NULL(table_schema)) {
      LOG_TRACE("succ to get table_schema", K(tenant_name), K(database_name), K(table_name),
                K_(level1_decoded_db_name), K_(level1_decoded_table_name), K_(level2_decoded_db_name),
                K_(level2_decoded_table_name), KPC(table_schema));
      if (OB_FAIL(table_schemas_.push_back(table_schema))) {
        LOG_WARN("fail to push back table_schema", K(table_schema), KR(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_view_decoded_schema_(
    const uint64_t tenant_id,
    const common::ObString &tenant_name,
    const common::ObString &view_definition,
    const bool is_oracle_mode,
    const ObTableSchema *&new_table_schema)
{
  int ret = OB_SUCCESS;
  SMART_VAR(sql::ObSQLSessionInfo, empty_session) {
    new_table_schema = NULL;
    const ObComplexTableType orig_complex_table_type = complex_table_type_;
    if (OB_FAIL(empty_session.init(0, 0, allocator_))) {
      LOG_WARN("init empty session failed", KR(ret));
    } else if (OB_FAIL(empty_session.load_default_sys_variable(false, false))) {
      LOG_WARN("session load default system variable failed", KR(ret));
    } else if (OB_FAIL(empty_session.init_tenant(tenant_name, tenant_id))) {
      LOG_WARN("fail to set tenant", KR(ret), K(tenant_name), K(tenant_id));
    } else {
      lib::Worker::CompatMode compat_mode = is_oracle_mode ?
                                         lib::Worker::CompatMode::ORACLE :
                                         lib::Worker::CompatMode::MYSQL;
      empty_session.set_compatibility_mode(static_cast<ObCompatibilityMode>(compat_mode));
      empty_session.set_sql_mode(ob_compatibility_mode_to_sql_mode(static_cast<ObCompatibilityMode>(compat_mode)));
      ParseResult parse_result;
      sql::ObParser parser(*allocator_, empty_session.get_sql_mode());
      sql::ObSchemaChecker schema_checker;
      lib::CompatModeGuard tmp_guard(compat_mode);
      //FIXME: Resolve view definition directly may failed when sys views are involved.
      //       Select sql is needed here like int ObTableColumns::resolve_view_definition().
      if (OB_FAIL(parser.parse(view_definition, parse_result))) {
        LOG_WARN("parse view definition failed", KR(ret), K(view_definition));
      } else if (OB_FAIL(schema_checker.init(schema_guard_))) {
        LOG_WARN("fail to init schema checker", KR(ret));
      } else {
        sql::ObResolverParams resolver_ctx;
        sql::ObStmtFactory stmt_factory(*allocator_);
        sql::ObRawExprFactory expr_factory(*allocator_);
        resolver_ctx.allocator_ = allocator_;
        resolver_ctx.schema_checker_ = &schema_checker;
        resolver_ctx.session_info_ = &empty_session;
        resolver_ctx.expr_factory_ = &expr_factory;
        resolver_ctx.stmt_factory_ = &stmt_factory;
        if (OB_ISNULL(resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create query context failed", KR(ret));
        } else {
          // set # of question marks
          resolver_ctx.query_ctx_->question_marks_count_ = static_cast<int64_t> (parse_result.question_mark_ctx_.count_);
        }
        if (OB_SUCC(ret)
            && OB_NOT_NULL(parse_result.result_tree_)
            && OB_LIKELY(parse_result.result_tree_->num_child_ > 0)) {
          sql::ObSelectResolver select_resolver(resolver_ctx);
          ParseNode *select_stmt_node = parse_result.result_tree_->children_[0];
          if (OB_ISNULL(select_stmt_node) || OB_UNLIKELY(select_stmt_node->type_ != T_SELECT)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid select_stmt_node", KR(ret), K(select_stmt_node),
                     K(select_stmt_node->type_));
          } else if (OB_FAIL(select_resolver.resolve(*select_stmt_node))) {
            LOG_WARN("resolve view definition failed", KR(ret));
            ret = OB_ERR_VIEW_INVALID;
          } else if (OB_ISNULL(select_resolver.get_basic_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid stmt", KR(ret));
          } else {
            // Take out the stmt after the view is expanded
            const sql::ObSelectStmt *select_stmt = static_cast<sql::ObSelectStmt*>(select_resolver.get_basic_stmt());
            const sql::TableItem *view_item = NULL;
            for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(view_item) && i < select_stmt->get_table_size(); ++i) {
              if (OB_ISNULL(view_item = select_stmt->get_table_item(i))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("view item is null", KR(ret));
              } else if (!view_item->is_basic_table()) {
                LOG_TRACE("only support base table now, try next table", KPC(view_item));
                view_item = NULL;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(view_item)) {
              LOG_TRACE("only support base table now, do nothing here", KPC(select_stmt));
            } else if (CT_DEFAULT == complex_table_type_) {
              if (view_item->synonym_name_.empty()) {
                if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level1_decoded_db_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->database_name_));
                } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level1_decoded_table_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->table_name_));
                } else {
                  complex_table_type_ = CT_VIEW;
                }
              } else {
                if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level1_decoded_db_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->database_name_));
                } else if (OB_FAIL(ob_write_string(*allocator_, view_item->synonym_name_, level1_decoded_table_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->table_name_));
                } else if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level2_decoded_db_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->database_name_));
                } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level2_decoded_table_name_))) {
                  LOG_WARN("ob_write_string failed", KR(ret), K(view_item->synonym_name_));
                } else {
                  complex_table_type_ = CT_SYNONYM_VIEW;
                }
              }
            } else {
              if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level2_decoded_db_name_))) {
                LOG_WARN("ob_write_string failed", KR(ret), K(view_item->database_name_));
              } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level2_decoded_table_name_))) {
                LOG_WARN("ob_write_string failed", KR(ret), K(view_item->table_name_));
              } else {
                complex_table_type_ = CT_VIEW_SYNONYM;
              }
            }
          }
        }
      }//end of lib::CompatModeGuard
    }

    if (OB_SUCC(ret) && orig_complex_table_type != complex_table_type_) {
      LOG_TRACE("try get real table", K(tenant_name), K_(complex_table_type), K_(level1_decoded_db_name),
          K_(level1_decoded_table_name), K_(level2_decoded_db_name), K_(level2_decoded_table_name));
      const ObString &new_db_name = (CT_VIEW == complex_table_type_ ? level1_decoded_db_name_ : level2_decoded_db_name_);
      const ObString &new_table_name = (CT_VIEW == complex_table_type_ ? level1_decoded_table_name_ : level2_decoded_table_name_);
      if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, new_db_name, new_table_name, false, new_table_schema))) {
        LOG_WARN("get table schema failed", KR(ret), K(tenant_name), K(new_db_name), K(new_table_name));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(inner_get_next_row_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", KR(ret), K_(cur_row));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualProxySchema::inner_get_next_row_()
{
  int ret = OB_SUCCESS;
  if (NULL == sql_res_) {
    int64_t replica_count = 0;
    const ObTableSchema *table_schema = NULL;
    ObString table_name;
    ObTabletID tablet_id;
    DupReplicaType dup_replica_type = DupReplicaType::NON_DUP_REPLICA;
    if (OB_UNLIKELY(next_table_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("next_table_idx_ can't be smaller than 0", KR(ret), K_(next_table_idx));
    } else if (next_table_idx_ >= table_schemas_.count()) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(table_schema = table_schemas_.at(next_table_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is NULL", KR(ret), K_(next_table_idx));
    } else {
      table_name = input_table_names_.at(next_table_idx_);
      tablet_id = tablet_ids_.at(next_table_idx_);
      // we need get tenant servers, only all the following conditions are met
      // 1. tablet id was not specified_
      // 2. this is __all_dummy table
      if (!tablet_id.is_valid()
          && ObString::make_string(OB_ALL_DUMMY_TNAME) == table_schema->get_table_name_str()) {
        if (OB_FAIL(get_next_tenant_server_(table_name, table_schema))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next tenant server", KR(ret), KPC(table_schema));
          }
        }
      } else if (OB_FAIL(get_next_tablet_location_(table_name, table_schema, tablet_id))) {
        LOG_WARN("fail to get next tablet location", KR(ret), KPC(table_schema), K(tablet_id));
      }
    }
  } else {
    ObInnerSQLResult *inner_sql_res =
      static_cast<ObInnerSQLResult *>(sql_res_->get_result());
    if (OB_FAIL(inner_sql_res->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("next failed", K(ret));
      }
    } else {
      cur_row_ = *inner_sql_res->get_row();
      ObNewRow *row = &cur_row_;
      if (OB_FAIL(convert_output_row(row))) {
        LOG_WARN("failed to convert row", K(ret));
      } else {
        cur_row_ = *row;
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_next_tablet_location_(
    const common::ObString &table_name,
    const share::schema::ObTableSchema *table_schema,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t replica_count = 0;
  if (0 == next_replica_idx_) { // get next table location
    if (OB_FAIL(get_table_tablet_location_(next_table_idx_, tablet_id))) {
      LOG_WARN("fail to get tablet location", KR(ret), K_(next_table_idx), K(tablet_id));
    }
  }
  if (OB_SUCC(ret)) { // dump each location
    DupReplicaType dup_replica_type = ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
                                      == table_schema->get_duplicate_scope() ?
                                      DupReplicaType::DUP_REPLICA :
                                      DupReplicaType::NON_DUP_REPLICA;
    const uint64_t table_id = table_schema->get_table_id();
    replica_count = location_.get_replica_locations().count();
    if (next_replica_idx_ < replica_count) {
      const ObLSReplicaLocation &replica = location_.get_replica_locations().at(next_replica_idx_);
      if (OB_FAIL(fill_row_(
          schema_guard_,
          table_name,
          *table_schema,
          replica,
          tablet_id,
          dup_replica_type))) {
        LOG_WARN("fail to fill row", KR(ret));
      } else {
        LOG_TRACE("success to fill row", K(replica), K(tablet_id), K(dup_replica_type));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++next_replica_idx_;
    if (next_replica_idx_ >= replica_count) {
      next_replica_idx_ = 0;
      ++next_table_idx_;
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_next_tenant_server_(
    const common::ObString &table_name,
    const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (next_server_idx_ < 0) {
    tenant_servers_.reset();
    if (OB_FAIL(get_tenant_servers_(table_schema->get_tenant_id()))) {
      LOG_WARN("fail to get tenant servers", KR(ret), K_(next_table_idx));
    } else if (OB_UNLIKELY(tenant_servers_.empty())) {
      ret = OB_ITER_END;
      LOG_TRACE("empty tenant servers, but has available tenant id, return nothing", KR(ret),
          K(tenant_servers_), "tenant_id", table_schema->get_tenant_id());
    } else {
      next_server_idx_ = 0;
    }
  }
  if (OB_SUCC(ret) && (next_server_idx_ >= 0)) {
    DupReplicaType dup_replica_type = DupReplicaType::NON_DUP_REPLICA;
    if (next_server_idx_ < tenant_servers_.count()) { // dump each server
      const ObTenantServer &server = tenant_servers_[next_server_idx_];
      if (OB_FAIL(fill_row_(
          schema_guard_,
          table_name,
          *table_schema,
          server.get_location(),
          server.get_virtual_tablet_id(),
          dup_replica_type))) {
        LOG_WARN("fail to fill row", KR(ret), K_(next_server_idx), K(server));
      }
    }
    if (OB_SUCC(ret)) {
      ++next_server_idx_;
      if (next_server_idx_ >= tenant_servers_.count()) {
        ++next_table_idx_;
        next_server_idx_ = -1; //set to invalid value
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::fill_tenant_servers_(
    const uint64_t tenant_id,
    ObMySQLResult &result,
    ObIArray<int64_t> &first_idx_in_zone)
{
  int ret = OB_SUCCESS;
  const int32_t svr_port = UINT16_MAX;//invalid
  ObString svr_ip;
  int64_t sql_port = OB_INVALID_INDEX;
  ObReplicaType replica_type = REPLICA_TYPE_FULL;
  ObReplicaProperty property;
  ObLSRestoreStatus restore_status;
  ObAddr server;
  ObString zone;
  uint64_t zone_hash = 0;
  uint64_t last_zone_hash = 0;
  ObTenantServer tenant_server;
  ObLSReplicaLocation replica_location;
  int64_t svr_idx = 0;
  first_idx_in_zone.reset();
  tenant_servers_.reset();

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    tenant_server.reset();
    svr_ip.reset();
    zone.reset();
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "svr_ip", svr_ip);
    EXTRACT_INT_FIELD_MYSQL(result, "inner_port", sql_port, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "zone", zone);
    if (OB_UNLIKELY(!server.set_ip_addr(svr_ip, svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set_ip_addr", KR(ret), K(svr_ip), K(svr_port));
    } else if (OB_FAIL(replica_location.init(
        server,
        FOLLOWER,
        sql_port,
        replica_type,
        property,
        restore_status,
        0 /*proposal_id*/))) {
      LOG_WARN("fail to init replica location", KR(ret), K(server), K(sql_port), K(replica_type));
    } else if (OB_FAIL(tenant_server.set_location(replica_location))) {
      LOG_WARN("fail to init tenant_server", KR(ret), K(replica_location));
    } else if (OB_FAIL(tenant_servers_.push_back(tenant_server))) {
      LOG_WARN("failed to push back tenant_servers_", K(tenant_server), KR(ret));
    } else {
      zone_hash = zone.hash();
      if (last_zone_hash != zone_hash) {
        if (OB_FAIL(first_idx_in_zone.push_back(svr_idx))) {
          LOG_WARN("fail to push back first_idx_in_zone", K(svr_idx), KR(ret));
        } else {
          last_zone_hash = zone_hash;
        }
      }
      ++svr_idx;
    }
  }
  if (OB_ITER_END != ret) {
    LOG_WARN("failed to get tenant server info", KR(ret), K_(tenant_servers));
  } else {
    ret = OB_SUCCESS;
    //virtual invalid idx, just used for compute svr_count_in_zone
    if (OB_FAIL(first_idx_in_zone.push_back(svr_idx))) {
      LOG_WARN("fail to push back first_idx_in_zone", KR(ret), K(svr_idx));
    } else {
      LOG_INFO("succ to fill tenant servers", K_(tenant_servers), K(first_idx_in_zone));
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_servers_by_join_(
    const uint64_t tenant_id,
    ObIArray<int64_t> &first_idx_in_zone)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy_ is null", KR(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);

    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      ObSqlString sql;
      first_idx_in_zone.reset();
      tenant_servers_.reset();
      const static char *SELECT_TENANT_ACTIVE_SERVERS_SQL =
          "SELECT S.svr_ip, S.inner_port, S.zone "
          "FROM oceanbase.%s R, oceanbase.%s U, oceanbase.%s S "
          "WHERE R.tenant_id = %lu "
          "AND R.resource_pool_id = U.resource_pool_id "
          "AND ((U.svr_ip = S.svr_ip AND U.svr_port = S.svr_port) "
          "      OR (U.migrate_from_svr_port > 0 AND U.migrate_from_svr_ip = S.svr_ip AND U.migrate_from_svr_port = S.svr_port)) "
          "AND S.inner_port > 0 "
          "ORDER BY S.zone ASC "
          "LIMIT %ld";
      if (OB_FAIL(sql.append_fmt(
          SELECT_TENANT_ACTIVE_SERVERS_SQL,
          OB_ALL_RESOURCE_POOL_TNAME,
          OB_ALL_UNIT_TNAME,
          OB_ALL_SERVER_TNAME,
          tenant_id,
          INT64_MAX))) {
        LOG_WARN("failed to append sql info", KR(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", KR(ret), K(sql), K(result));
      } else if (OB_FAIL(fill_tenant_servers_(tenant_id, *result, first_idx_in_zone))) {
        LOG_WARN("failed to fill_tenant_servers", KR(ret), K(tenant_id));
      } else {
        LOG_INFO("succ to get tenant_servers by join", K_(tenant_servers));
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_servers_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // 1. get tenant servers
  ObSEArray<int64_t, 5> first_idx_in_zone;//save first svr_idx_idx in zone
  if (OB_FAIL(get_tenant_servers_by_join_(tenant_id, first_idx_in_zone))) {
    LOG_WARN("fail to get tenant servers by join", KR(ret));
  }

  // 2. order servers
  if (OB_SUCC(ret)
      && OB_LIKELY(tenant_servers_.count() > 0)
      && OB_LIKELY(first_idx_in_zone.count() > 1)) {
    const int64_t server_count = tenant_servers_.count();
    const int64_t zone_count = first_idx_in_zone.count() - 1;//NOTE:: the last one is virtual invalid;
    const int64_t replica_count = zone_count;
    const int64_t tablet_count = (server_count / replica_count + (0 == server_count % replica_count ? 0 : 1));
    ObSEArray<int64_t, 6> unused_server_count;
    int64_t svr_count_in_zone = 0;
    int64_t svr_idx = 0;
    uint64_t virtual_tablet_id = 0;

    for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
      if (OB_UNLIKELY(0 >= (svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it should not happened", K(first_idx_in_zone), K(svr_count_in_zone), KR(ret));
      } else if (OB_FAIL(unused_server_count.push_back(svr_count_in_zone))) {
        LOG_WARN("fail to push back unused_server_count", K(i), K(svr_count_in_zone), KR(ret));
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret)) {
      int64_t init_idx = (ObTimeUtility::current_time() + server_count) % server_count; // random to pick zone
      int64_t finish_count = 0;
      while (finish_count < server_count && OB_SUCC(ret)) {
        ++init_idx;
        for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
          if (unused_server_count.at(i) > 0) {
            svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i);
            svr_idx = first_idx_in_zone.at(i) + (init_idx + i) % svr_count_in_zone;
            virtual_tablet_id = finish_count / replica_count;
            tenant_servers_.at(svr_idx).set_virtual_tablet_id(ObTabletID(virtual_tablet_id));
            ++finish_count;
            --unused_server_count.at(i);
          } //end of unused_server_count > 0
        } //end of for zone_count
      } //end of while server_count

      //make the first one leader
      if (OB_SUCC(ret)) {
        for (int64_t tablet_id = 0; tablet_id < tablet_count && OB_SUCC(ret); ) {
          for (svr_idx = 0; svr_idx < server_count && tablet_id < tablet_count && OB_SUCC(ret); ++svr_idx) {
            //set the first location to leader
            if (tablet_id == tenant_servers_.at(svr_idx).get_virtual_tablet_id().id()) {
              tenant_servers_.at(svr_idx).set_role(LEADER);
              ++tablet_id;
            }
          }
        }
        LOG_INFO("succ to get tenant_servers_", K(tenant_servers_), K(tablet_count), K(replica_count));
      }
    }
  } //end of order servers
  return ret;
}

int ObAllVirtualProxySchema::fill_row_(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObString &table_name,
    const share::schema::ObTableSchema &table_schema,
    const share::ObLSReplicaLocation &replica,
    const common::ObTabletID &tablet_id,
    const DupReplicaType dup_replica_type)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  int64_t paxos_replica_num = OB_INVALID_COUNT;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", KR(ret));
  } else {
    uint64_t table_id = table_schema.get_table_id();
    int64_t part_num = 0;
    if (table_schema.is_index_local_storage()) {
      // if local index table, use it's main table's tablet num and replica num
      const ObTableSchema *main_table_schema = NULL;
      uint64_t data_table_id = table_schema.get_data_table_id();
      if (OB_FAIL(schema_guard.get_table_schema(table_schema.get_tenant_id(),
                  data_table_id, main_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(data_table_id), K(table_schema));
      } else if (OB_ISNULL(main_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table schema", KR(ret), K(data_table_id), K(table_schema));
      } else {
        part_num = main_table_schema->get_all_part_num();
        if (OB_FAIL(main_table_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
          LOG_WARN("fail to get table paxos replica_num", KR(ret), KPC(main_table_schema));
        }
      }
    } else {
      part_num = table_schema.get_all_part_num();
      if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
        LOG_WARN("fail to get table paxos replica_num", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(paxos_replica_num < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("paxos replica num error", KR(ret),
            K(paxos_replica_num), "table_id", table_schema.get_table_id());
      }
    }

    uint64_t cell_idx = 0;
    uint64_t col_id = OB_INVALID_ID;
    for (int64_t m = 0; (OB_SUCC(ret)) && (m < col_count); ++m) {
      col_id = output_column_ids_.at(m);
      switch (col_id) {
        case TENANT_NAME: {
          cells[cell_idx].set_varchar(input_tenant_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case DATABASE_NAME: {
          cells[cell_idx].set_varchar(input_db_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case TABLE_NAME: {
          cells[cell_idx].set_varchar(table_name);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case TABLET_ID: {
          cells[cell_idx].set_int(tablet_id.id());
          break;
        }
        case SERVER_IP: {
          if (OB_UNLIKELY(!replica.get_server().ip_to_string(ip_buf_, OB_IP_STR_BUFF))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to covert ip to string", KR(ret),
                K(cell_idx), K(output_column_ids_), K(col_id), K(replica));
          } else {
            cells[cell_idx].set_varchar(ip_buf_);
            cells[cell_idx].set_collation_type(coll_type);
          }
          break;
        }
        case SQL_PORT: {
          cells[cell_idx].set_int(replica.get_sql_port());
          break;
        }
        case TABLE_ID: {
          cells[cell_idx].set_int(static_cast<int64_t>(table_id));
          break;
        }
        case ROLE: {
          cells[cell_idx].set_int(static_cast<int64_t>(replica.get_role()));
          break;
        }
        case PART_NUM: {
          cells[cell_idx].set_int(part_num);
          break;
        }
        case REPLICA_NUM: {
          cells[cell_idx].set_int(paxos_replica_num);
          break;
        }
        case TABLE_TYPE: {
          cells[cell_idx].set_int(static_cast<int64_t>(table_schema.get_table_type()));
          break;
        }
        case SCHEMA_VERSION: {
          cells[cell_idx].set_int(static_cast<int64_t>(table_schema.get_schema_version()));
          break;
        }
        case REPLICA_TYPE: {
          cells[cell_idx].set_int(static_cast<int64_t>(replica.get_replica_type()));
          break;
        }
        case DUP_REPICA_TYPE: {
          cells[cell_idx].set_int(static_cast<int>(dup_replica_type));
          break;
        }
        case MEMSTORE_PERCENT: {
          cells[cell_idx].set_int(replica.get_property().get_memstore_percent());
          break;
        }
        case SPARE1: { // int, unused
          cells[cell_idx].set_int(0);
          break;
        }
        case SPARE2: { // int, unused
          cells[cell_idx].set_int(0);
          break;
        }
        case SPARE3: {// int, unused
          cells[cell_idx].set_int(0);
          break;
        }
        case SPARE4: { // varchar, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case SPARE5: { // string, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case SPARE6: { // string, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case COMPLEX_TABLE_TYPE: { // int
          cells[cell_idx].set_int(static_cast<int64_t>(complex_table_type_));
          break;
        }
        case LEVEL1_DECODED_DB_NAME: { // varchar
          cells[cell_idx].set_varchar(level1_decoded_db_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL1_DECODED_TABLE_NAME: { // varchar
          cells[cell_idx].set_varchar(level1_decoded_table_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL2_DECODED_DB_NAME: { // varchar
          cells[cell_idx].set_varchar(level2_decoded_db_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL2_DECODED_TABLE_NAME: { // varchar
          cells[cell_idx].set_varchar(level2_decoded_table_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", KR(ret),
              K(cell_idx), K(output_column_ids_), K(col_id), K(tablet_id), K(replica));
          break;
        }
      }

      if (OB_SUCC(ret)) {
        ObObj &cell = cells[cell_idx];
        if (cell.is_string_type() && 0 == cell.get_data_length()) {
          cell.set_null();
        }
        cell_idx++;
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_table_tablet_location_(
    const int64_t table_idx,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t table_count = table_schemas_.count();
  ObTabletID acutal_tablet_id = tablet_id;
  if (OB_UNLIKELY(table_idx < 0 || table_idx >= table_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table idx", KR(ret), K(table_idx), K(table_count));
  } else {
    const ObTableSchema *table_schema = table_schemas_.at(table_idx);
    int64_t table_id = 0;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is NULL", KR(ret), K(table_schema));
    } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
      // impossible
    } else if ((table_schema->is_vir_table())
        || (table_schema->is_tmp_table())
        || (table_schema->is_view_table())
        || (table_schema->is_external_table())) {
      // virtual table use virtual tablet
      if (OB_FAIL(location_.init_fake_location())) {
        LOG_WARN("fail to init fake location", KR(ret), KPC(table_schema));
      } else {
        LOG_TRACE("init fake location success", KR(ret), K_(location), KPC(table_schema));
      }
    } else if (FALSE_IT(table_id = table_schema->get_table_id())) { // fetch again, table_schema mybe transfered
      // impossible
    } else if (OB_FAIL(get_actual_tablet_id_(*table_schema, acutal_tablet_id))) {
      LOG_WARN("fail to get actual tablet idx", KR(ret), KPC(table_schema), K(tablet_id));
    } else if (OB_FAIL(fetch_tablet_location_(*table_schema, acutal_tablet_id))) {
      LOG_WARN("fail to fetch tablet location", KR(ret), KPC(table_schema), K(acutal_tablet_id));
    } else {
      LOG_TRACE("success to get location", K(tablet_id), K(acutal_tablet_id), K_(location));
    }
  }
  return ret;
}

//TODO: use force_sql_refresh_ if is necessary
int ObAllVirtualProxySchema::fetch_tablet_location_(
    const share::schema::ObTableSchema &table_schema,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  location_.reset();
  bool is_cache_hit = false;
  // only requires 100ms of effectiveness, no need for real-time.
  const int64_t expire_renew_time =
      ObTimeUtility::current_time() - GCONF.location_cache_refresh_min_interval;
  ObLSID ls_id;
  ObLSLocation ls_location;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  if (OB_ISNULL(location_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid location_service_", KR(ret), KP_(location_service));
  } else if (OB_FAIL(location_service_->get(
      tenant_id,
      tablet_id,
      expire_renew_time,
      is_cache_hit,
      ls_id))) {
    LOG_WARN("get tablet log stream id fail", KR(ret),
        K(tenant_id), K(tablet_id), K(expire_renew_time), K(is_cache_hit));
  } else if (OB_FAIL(location_service_->get(
      GCONF.cluster_id,
      tenant_id,
      ls_id,
      expire_renew_time,
      is_cache_hit,
      location_))) {
    LOG_WARN("get ls locaiton fail", KR(ret), K(tenant_id), K(ls_id), K(expire_renew_time));
  }
  if (is_cache_hit) {
    EVENT_INC(LOCATION_CACHE_PROXY_HIT);
  } else {
    EVENT_INC(LOCATION_CACHE_PROXY_MISS);
  }
  if (OB_LS_LOCATION_NOT_EXIST == ret || OB_GET_LOCATION_TIME_OUT == ret) {
    ret = OB_SUCCESS;
    location_.reset();
  }
  LOG_TRACE("finish get location", KR(ret), K(tenant_id), K(tablet_id),
      K(ls_id), K(expire_renew_time), K(is_cache_hit), K_(location));
  return ret;
}

// If the input tablet_id does not exist or is invalid, return first tablet_id of the table
int ObAllVirtualProxySchema::get_actual_tablet_id_(
    const ObTableSchema &table_schema,
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to get actual tablet id", K(tablet_id), K(table_schema));
  ObTabletID first_tablet_id;
  ObTabletID tmp_tablet_id;
  ObPartitionSchemaIter iter(table_schema, ObCheckPartitionMode::CHECK_PARTITION_MODE_NORMAL);
  if (OB_FAIL(iter.next_tablet_id(first_tablet_id))) {
    LOG_WARN("there must have at least one tablet", KR(ret), K(table_schema));
  } else if (!tablet_id.is_valid()) { // invalid input tablet_id means get first tablet
    tablet_id = first_tablet_id;
  } else if (first_tablet_id == tablet_id) {
    // find tablet, do nothing
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_tablet_id(tmp_tablet_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next tablet id by iter", KR(ret), K(iter));
        }
      } else if (tmp_tablet_id == tablet_id) {
        break; // find tablet
      }
    } // end while
    if (OB_ITER_END == ret) {
      tablet_id = first_tablet_id; // if tablet_id not exist, use first tablet
      ret = OB_SUCCESS;
    }
  }
  LOG_TRACE("finish to get actual tablet id", K(tablet_id), K(table_schema));
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
