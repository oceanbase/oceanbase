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
#include "lib/stat/ob_diagnose_info.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_all_virtual_proxy_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_dup_replica_checker.h"
#include "storage/ob_partition_service.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_select_resolver.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase {
namespace observer {

ObAllVirtualProxySchema::ObAllVirtualProxySchema()
    : ObVirtualTableIterator(),
      pl_cache_(NULL),
      sql_proxy_(NULL),
      tenant_id_(OB_INVALID_ID),
      table_schemas_(),
      partition_ids_(),
      tenant_servers_(),
      location_(),
      next_table_idx_(0),
      next_replica_idx_(0),
      next_server_idx_(-1),
      config_(NULL),
      routine_databases_(),
      routine_names_(),
      schema_service_(NULL),
      full_schema_guard_(),
      complex_table_type_(CT_DEFAULT),
      force_sql_refresh_(false)
{
  MEMSET(ip_buf, 0, sizeof(ip_buf));
}

ObAllVirtualProxySchema::~ObAllVirtualProxySchema()
{}

void ObAllVirtualProxySchema::set_schema_service(share::schema::ObMultiVersionSchemaService& schema_service)
{
  schema_service_ = &schema_service;
}

int ObAllVirtualProxySchema::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_) ||
      OB_ISNULL(pl_cache_) || OB_ISNULL(schema_service_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", KP_(schema_service), K_(pl_cache), K_(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(full_schema_guard_))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret));
  } else {
    const int64_t ROW_KEY_COUNT = 6;
    ObRowkey start_key;
    ObRowkey end_key;
    ObString tenant_name;
    ObString database_name;
    ObString table_name;
    const ObObj* start_key_obj_ptr = NULL;
    const ObObj* end_key_obj_ptr = NULL;
    const ObTableSchema* table_schema = NULL;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    uint64_t database_id = OB_INVALID_ID;
    int64_t partition_id = OB_INVALID_INDEX;
    // if svr_ip = '', svr_port=0, means used for routine
    const ObString routine_svr_ip("");
    const int64_t routine_svr_port = 0;
    const int64_t routine_partition_id = 0;

    // get table schema
    for (int64_t i = 0; (OB_SUCC(ret)) && (i < key_ranges_.count()); ++i) {
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      if ((ROW_KEY_COUNT != start_key.get_obj_cnt()) || (ROW_KEY_COUNT != end_key.get_obj_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_name, database_name and table_name must all be specified");
      }

      if (OB_SUCC(ret)) {
        start_key_obj_ptr = start_key.get_obj_ptr();
        end_key_obj_ptr = end_key.get_obj_ptr();

        for (int64_t j = 0; (OB_SUCC(ret)) && (j < 3); ++j) {
          if (start_key_obj_ptr[j].is_min_value() || end_key_obj_ptr[j].is_max_value() ||
              !start_key_obj_ptr[j].is_varchar_or_char() || !end_key_obj_ptr[j].is_varchar_or_char() ||
              (start_key_obj_ptr[j] != end_key_obj_ptr[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_USER_ERROR(OB_ERR_UNEXPECTED, "tenant_name, database_name and table_name must all be specified");
          } else {
            switch (j) {
              case 0: {  // tenant_name
                tenant_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 1: {  // database_name
                database_name = end_key_obj_ptr[j].get_string();
                break;
              }
              case 2: {  // table_name
                table_name = end_key_obj_ptr[j].get_string();
                break;
              }
            }  // end of switch
          }    // end of else
        }      // end of for

        if (OB_SUCC(ret)) {
          // all users of sys tenant are permitted to access this virtual table
          if (OB_UNLIKELY(!session_->is_proxy_sys_user())) {
            ret = OB_ERR_NO_PRIVILEGE;
            SERVER_LOG(WARN, "Access denied for this table", KPC(session_), K(ret));
          } else if ((start_key_obj_ptr[3].is_min_value()) && (end_key_obj_ptr[3].is_max_value())) {
            partition_id = OB_INVALID_INDEX;
          } else {
            partition_id = start_key_obj_ptr[3].get_int();
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(partition_ids_.push_back(partition_id))) {
            SERVER_LOG(WARN, "fail to push back partition_ids", K(partition_id), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        table_schema = NULL;
        input_tenant_name_ = tenant_name;
        input_db_name_ = database_name;
        input_table_name_ = table_name;

        // 1. try default
        if (OB_FAIL(full_schema_guard_.get_tenant_id(tenant_name, tenant_id))) {
          SERVER_LOG(INFO, "tenant does not exist", K(tenant_name), K(ret));
          // unknown tenant, return succ
          ret = OB_SUCCESS;
        } else if (OB_FAIL(full_schema_guard_.get_database_id(tenant_id, database_name, database_id))) {
          SERVER_LOG(WARN, "database is not exist", K(tenant_id), K(database_name), K(ret));
        } else if (OB_INVALID_ID == database_id) {
          // unknown db, return succ
          ret = OB_SUCCESS;
        } else if (OB_FAIL(
                       full_schema_guard_.get_table_schema(tenant_id, database_id, table_name, false, table_schema))) {
          SERVER_LOG(WARN, "get table schema failed", K(tenant_name), K(database_name), K(table_name));
        } else if (NULL == table_schema) {
          if (OB_FAIL(full_schema_guard_.get_table_schema(tenant_id, database_id, table_name, true, table_schema))) {
            SERVER_LOG(WARN, "get table schema failed", K(tenant_name), K(database_name), K(table_name));
          } else if (NULL == table_schema) {
            SERVER_LOG(INFO, "table does not exist", K(tenant_name), K(database_name), K(table_name));
          }  // unknown table, return succ
        }

        // 2. try synonym
        if (OB_SUCC(ret) && NULL == table_schema && OB_INVALID_ID != tenant_id && OB_INVALID_TENANT_ID != tenant_id &&
            OB_INVALID_ID != database_id) {
          SERVER_LOG(DEBUG, "try synonym", K(tenant_name), K(database_name), K(table_name));
          uint64_t object_database_id = OB_INVALID_ID;
          uint64_t synonym_id = OB_INVALID_ID;
          ObString object_table_name;
          const ObSimpleDatabaseSchema* database_schema = NULL;
          bool exist = false;
          if (OB_FAIL(full_schema_guard_.get_object_with_synonym(
                  tenant_id, database_id, table_name, object_database_id, synonym_id, object_table_name, exist))) {
            LOG_WARN("get_object_with_synonym failed", K(tenant_id), K(database_id), K(table_name), K(ret));
          } else if (!exist) {
            // break
          } else if (OB_FAIL(ob_write_string(*allocator_, object_table_name, level1_decoded_table_name_))) {
            LOG_WARN("ob_write_string failed", K(object_table_name), K(ret));
          } else if (OB_FAIL(full_schema_guard_.get_database_schema(object_database_id, database_schema))) {
            LOG_WARN("get_database_schema failed", K(tenant_id), K(object_database_id), K(ret));
          } else if (OB_FAIL(ob_write_string(
                         *allocator_, database_schema->get_database_name_str(), level1_decoded_db_name_))) {
            LOG_WARN("ob_write_string failed", K(database_schema->get_database_name_str()), K(ret));
          } else if (OB_FAIL(full_schema_guard_.get_table_schema(
                         tenant_id, object_database_id, object_table_name, false, table_schema))) {
            SERVER_LOG(WARN, "get table schema failed", K(tenant_name), K(object_database_id), K(object_table_name));
          } else if (NULL == table_schema) {
            if (OB_FAIL(full_schema_guard_.get_table_schema(
                    tenant_id, object_database_id, object_table_name, true, table_schema))) {
              SERVER_LOG(WARN, "get table schema failed", K(tenant_name), K(object_database_id), K(object_table_name));
            } else if (NULL == table_schema) {
              // unknown table, return succ
              SERVER_LOG(INFO, "table does not exist", K(tenant_name), K(object_database_id), K(object_table_name));
            } else {
              complex_table_type_ = CT_SYNONYM;
            }
          } else {
            complex_table_type_ = CT_SYNONYM;
          }
        }

        // 3. try view
        if (OB_SUCC(ret) && NULL != table_schema && table_schema->is_view_table()) {
          SERVER_LOG(DEBUG,
              "try view",
              K(tenant_name),
              K(database_name),
              K(table_name),
              K_(complex_table_type),
              K_(level1_decoded_db_name),
              K_(level1_decoded_table_name),
              KPC(table_schema));
          const common::ObString& view_definition = table_schema->get_view_schema().get_view_definition_str();
          const ObTableSchema* new_table_schema = NULL;
          if (OB_FAIL(get_view_decoded_schema(tenant_id, tenant_name, view_definition, new_table_schema))) {
            LOG_WARN("get_view_decoded_schema failed", K(ret));
          } else if (NULL != new_table_schema) {
            table_schema = new_table_schema;
          }
        }

        // if table schema doesn't exist, must confirm current schema manager's version
        // greater than query_begin_schema_version_
        if (OB_SUCC(ret) && NULL == table_schema && OB_INVALID_ID != tenant_id && OB_INVALID_TENANT_ID != tenant_id &&
            OB_INVALID_ID != database_id) {
          uint64_t fetch_tenant_id = is_sys_database_id(database_id) ? OB_SYS_TENANT_ID : tenant_id;
          int64_t received_broadcast_version = OB_INVALID_VERSION;
          int64_t refreshed_version = OB_INVALID_VERSION;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(schema_service_->get_tenant_received_broadcast_version(
                         fetch_tenant_id, received_broadcast_version))) {
            SERVER_LOG(WARN, "fail to get tenant received broadcast version", K(ret), K(fetch_tenant_id));
          } else if (OB_FAIL(full_schema_guard_.get_schema_version(fetch_tenant_id, refreshed_version))) {
            SERVER_LOG(WARN, "fail to get schema guard version", K(ret), K(fetch_tenant_id));
          } else if (OB_CORE_SCHEMA_VERSION >= received_broadcast_version ||
                     refreshed_version < received_broadcast_version) {
            ret = OB_SCHEMA_ERROR;
            SERVER_LOG(INFO,
                "schema not exist, need retry",
                K(fetch_tenant_id),
                K(received_broadcast_version),
                K(refreshed_version),
                K(ret));
          } else {
            // do noting
          }
        }

        if ((OB_SUCC(ret)) && (NULL != table_schema)) {
          SERVER_LOG(DEBUG,
              "succ to get table_schema",
              K(tenant_name),
              K(database_name),
              K(table_name),
              K_(level1_decoded_db_name),
              K_(level1_decoded_table_name),
              K_(level2_decoded_db_name),
              K_(level2_decoded_table_name),
              KPC(table_schema));

          if (OB_FAIL(table_schemas_.push_back(table_schema))) {
            SERVER_LOG(WARN, "fail to push back table_schema", K(table_schema), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_view_decoded_schema(const uint64_t tenant_id, common::ObString& tenant_name,
    const common::ObString& view_definition, const ObTableSchema*& new_table_schema)
{
  int ret = OB_SUCCESS;
  new_table_schema = NULL;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  sql::ObSQLSessionInfo empty_session;
  const ObComplexTableType orig_complex_table_type = complex_table_type_;
  if (OB_FAIL(empty_session.init(0, 0, 0, allocator_))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(empty_session.load_default_sys_variable(false, false))) {
    LOG_WARN("session load default system variable failed", K(ret));
  } else if (OB_FAIL(empty_session.init_tenant(tenant_name, tenant_id))) {
    LOG_WARN("fail to set tenant", K(tenant_name), K(tenant_id), K(ret));
  } else if (OB_FAIL(full_schema_guard_.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("get_tenant_compat_mode failed", K(tenant_id), K(ret));
  } else {
    empty_session.set_compatibility_mode(static_cast<ObCompatibilityMode>(compat_mode));
    empty_session.set_sql_mode(ob_compatibility_mode_to_sql_mode(static_cast<ObCompatibilityMode>(compat_mode)));
    ParseResult parse_result;
    sql::ObParser parser(*allocator_, empty_session.get_sql_mode());
    sql::ObSchemaChecker schema_checker;
    CompatModeGuard tmp_guard(compat_mode);
    if (OB_FAIL(parser.parse(view_definition, parse_result))) {
      LOG_WARN("parse view definition failed", K(view_definition), K(ret));
    } else if (OB_FAIL(schema_checker.init(full_schema_guard_))) {
      LOG_WARN("fail to init schema checker", K(ret));
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
        LOG_WARN("create query context failed", K(ret));
      } else {
        // set # of question marks
        resolver_ctx.query_ctx_->question_marks_count_ = static_cast<int64_t>(parse_result.question_mark_ctx_.count_);
      }
      if (OB_SUCC(ret) && OB_LIKELY(NULL != parse_result.result_tree_) &&
          OB_LIKELY(parse_result.result_tree_->num_child_ > 0)) {
        sql::ObSelectResolver select_resolver(resolver_ctx);
        ParseNode* select_stmt_node = parse_result.result_tree_->children_[0];
        if (OB_UNLIKELY(NULL == select_stmt_node || select_stmt_node->type_ != T_SELECT)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid select_stmt_node", K(ret), K(select_stmt_node), K(select_stmt_node->type_));
        } else if (OB_FAIL(select_resolver.resolve(*select_stmt_node))) {
          LOG_WARN("resolve view definition failed", K(ret));
          ret = OB_ERR_VIEW_INVALID;
        } else if (OB_UNLIKELY(NULL == select_resolver.get_basic_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid stmt", K(ret));
        } else {
          // Take out the stmt after the view is expanded
          const sql::ObSelectStmt* select_stmt = static_cast<sql::ObSelectStmt*>(select_resolver.get_basic_stmt());
          const sql::TableItem* view_item = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && NULL == view_item && i < select_stmt->get_table_size(); ++i) {
            if (OB_ISNULL(view_item = select_stmt->get_table_item(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("view item is null", K(ret));
            } else if (!view_item->is_basic_table()) {
              LOG_DEBUG("only support base table now, try next table", KPC(view_item));
              view_item = NULL;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (NULL == view_item) {
            LOG_DEBUG("only support base table now, do nothing here", KPC(select_stmt));
          } else if (CT_DEFAULT == complex_table_type_) {
            if (view_item->synonym_name_.empty()) {
              if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level1_decoded_db_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->database_name_), K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level1_decoded_table_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->table_name_), K(ret));
              } else {
                complex_table_type_ = CT_VIEW;
              }
            } else {
              if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level1_decoded_db_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->database_name_), K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, view_item->synonym_name_, level1_decoded_table_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->table_name_), K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level2_decoded_db_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->database_name_), K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level2_decoded_table_name_))) {
                LOG_WARN("ob_write_string failed", K(view_item->synonym_name_), K(ret));
              } else {
                complex_table_type_ = CT_SYNONYM_VIEW;
              }
            }
          } else {
            if (OB_FAIL(ob_write_string(*allocator_, view_item->database_name_, level2_decoded_db_name_))) {
              LOG_WARN("ob_write_string failed", K(view_item->database_name_), K(ret));
            } else if (OB_FAIL(ob_write_string(*allocator_, view_item->table_name_, level2_decoded_table_name_))) {
              LOG_WARN("ob_write_string failed", K(view_item->table_name_), K(ret));
            } else {
              complex_table_type_ = CT_VIEW_SYNONYM;
            }
          }
        }
      }
    }  // end of CompatModeGuard
  }

  if (OB_SUCC(ret) && orig_complex_table_type != complex_table_type_) {
    SERVER_LOG(DEBUG,
        "try get real table",
        K(tenant_name),
        K_(complex_table_type),
        K_(level1_decoded_db_name),
        K_(level1_decoded_table_name),
        K_(level2_decoded_db_name),
        K_(level2_decoded_table_name));
    const ObString& new_db_name = (CT_VIEW == complex_table_type_ ? level1_decoded_db_name_ : level2_decoded_db_name_);
    const ObString& new_table_name =
        (CT_VIEW == complex_table_type_ ? level1_decoded_table_name_ : level2_decoded_table_name_);
    if (OB_FAIL(full_schema_guard_.get_table_schema(tenant_id, new_db_name, new_table_name, false, new_table_schema))) {
      SERVER_LOG(WARN, "get table schema failed", K(tenant_name), K(new_db_name), K(new_table_name), K(ret));
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K_(cur_row), K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualProxySchema::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema service is NULL", K(ret));
  } else if (OB_ISNULL(pl_cache_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "partition location is NULL", K(ret));
  } else {
    int64_t replica_count = 0;
    const ObTableSchema* table_schema = NULL;
    int64_t partition_id = OB_INVALID_INDEX;
    if (next_table_idx_ >= table_schemas_.count()) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(table_schema = table_schemas_.at(next_table_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table schema is NULL", K_(next_table_idx), K(ret));
    } else {
      partition_id = partition_ids_.at(next_table_idx_);
      // we need get tenant servers, only the follows all happened
      // 1. next_server_idx_ < 0, invalid value
      // 2. partition id was not specified_
      // 3. this is __all_dummy table
      if (next_server_idx_ < 0 && OB_INVALID_INDEX == partition_ids_[next_table_idx_] &&
          ObString::make_string(OB_ALL_DUMMY_TNAME) == table_schema->get_table_name_str()) {
        tenant_servers_.reset();
        if (OB_FAIL(get_tenant_servers(table_schema->get_tenant_id()))) {
          SERVER_LOG(WARN, "fail to get tenant servers", K_(next_table_idx), K(ret));
        } else if (OB_UNLIKELY(tenant_servers_.empty())) {
          ret = OB_ITER_END;
          SERVER_LOG(INFO,
              "empty tenant servers, but has avail tenant id, response nothing",
              K(tenant_servers_),
              "tenant_id",
              table_schema->get_tenant_id(),
              K(ret));
        } else {
          next_server_idx_ = 0;
        }
      }

      if (OB_SUCC(ret)) {
        DupReplicaType dup_replica_type = DupReplicaType::NON_DUP_REPLICA;
        if (next_server_idx_ >= 0) {                         // this is all_dumyy table
          if (next_server_idx_ < tenant_servers_.count()) {  // dump each server
            const ObTenantServer& server = tenant_servers_[next_server_idx_];
            if (OB_FAIL(fill_row(full_schema_guard_,
                    *table_schema,
                    server.location_,
                    server.virtual_partition_id_,
                    dup_replica_type))) {
              SERVER_LOG(WARN, "fail to fill row", K(next_server_idx_), K(server), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ++next_server_idx_;
            if (next_server_idx_ >= tenant_servers_.count()) {
              ++next_table_idx_;
              next_server_idx_ = -1;  // set to invalid value
            }
            SERVER_LOG(DEBUG, "find one row", K(next_server_idx_), K(next_table_idx_), K(tenant_servers_.count()));
          }
        } else {                         // this is not all_dummy table
          if (0 == next_replica_idx_) {  // get next partition location
            if (OB_FAIL(get_table_partition_location(next_table_idx_, partition_id))) {
              SERVER_LOG(WARN, "fail to get partition location", K_(next_table_idx), K(partition_id), K(ret));
            }
          }

          // dump each partition location
          if (OB_SUCC(ret)) {
            const uint64_t table_id = table_schema->get_table_id();
            // dump spec partition_id
            replica_count = location_.size();
            if (next_replica_idx_ < replica_count) {
              const ObReplicaLocation& replica = location_.get_replica_locations().at(next_replica_idx_);
              if (replica.is_valid() && NULL != GCTX.par_ser_ &&
                  OB_FAIL(GCTX.par_ser_->get_dup_replica_type(table_id, replica.server_, dup_replica_type))) {
                SERVER_LOG(WARN, "get dup replica type failed", K(ret));
              } else if (OB_FAIL(
                             fill_row(full_schema_guard_, *table_schema, replica, partition_id, dup_replica_type))) {
                SERVER_LOG(WARN, "fail to get fill row", K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            ++next_replica_idx_;
            if (next_replica_idx_ >= replica_count) {
              next_replica_idx_ = 0;
              ++next_table_idx_;
            }  // end of next_replica_idx_ >= replica_count
          }    // end of OB_SUCC
        }      // end of non all_dummy table
      }        // end of OB_SUCC(ret)
    }
  }
  return ret;
}

ObReplicaType ObAllVirtualProxySchema::get_locality_type(const ObZoneReplicaNumSet& zone_locality)
{
  ObReplicaType ret_type = REPLICA_TYPE_FULL;
  if (zone_locality.get_readonly_replica_num() > 0 && zone_locality.get_non_readonly_replica_num() == 0) {
    ret_type = REPLICA_TYPE_READONLY;
  } else if (zone_locality.get_full_replica_num() > 0) {
    ret_type = REPLICA_TYPE_FULL;
  } else if (zone_locality.get_logonly_replica_num() > 0) {
    ret_type = REPLICA_TYPE_LOGONLY;
  } else {
    // can not reach here
    ret_type = REPLICA_TYPE_FULL;
  }
  return ret_type;
}

int ObAllVirtualProxySchema::fill_tenant_servers(
    ObMySQLResult& result, ObIArray<int64_t>& first_idx_in_zone, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int32_t svr_port = UINT16_MAX;  // invalid
  ObString svr_ip;
  ObString zone;
  uint64_t zone_hash = 0;
  uint64_t last_zone_hash = 0;
  ObTenantServer tenant_server;
  int64_t svr_idx = 0;
  first_idx_in_zone.reset();
  tenant_servers_.reset();
  const ObTenantSchema* tenant_info = NULL;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema service is NULL", K(ret));
  } else if (OB_FAIL(full_schema_guard_.get_tenant_info(tenant_id, tenant_info))) {
    SERVER_LOG(WARN, "failed to get tenant_info", K(ret));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_DEBUG("succ to get tenant_info", KPC(tenant_info));
  }

  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    tenant_server.location_.server_.reset();
    svr_ip.reset();
    zone.reset();
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "svr_ip", svr_ip);
    EXTRACT_INT_FIELD_MYSQL(result, "inner_port", tenant_server.location_.sql_port_, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "zone", zone);
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(tenant_info->get_zone_replica_attr_array(zone_locality))) {
      SERVER_LOG(WARN, "fail to get zone replica num array", K(ret));
    } else {
      for (int i = 0; i < zone_locality.count(); ++i) {
        if (zone_locality.at(i).zone_.str() == zone) {
          tenant_server.location_.replica_type_ = get_locality_type(zone_locality.at(i));
        }
      }
      if (OB_UNLIKELY(!tenant_server.location_.server_.set_ip_addr(svr_ip, svr_port))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to set_ip_addr", K(svr_ip), K(svr_port), K(ret));
      } else if (OB_FAIL(tenant_servers_.push_back(tenant_server))) {
        SERVER_LOG(WARN, "failed to push back tenant_servers_", K(tenant_server), K(ret));
      } else {
        zone_hash = zone.hash();
        if (last_zone_hash != zone_hash) {
          if (OB_FAIL(first_idx_in_zone.push_back(svr_idx))) {
            SERVER_LOG(WARN, "fail to push back first_idx_in_zone", K(svr_idx), K(ret));
          } else {
            last_zone_hash = zone_hash;
          }
        }
        ++svr_idx;
      }
    }
  }
  if (OB_ITER_END != ret) {
    SERVER_LOG(WARN, "failed to get tenant server info", K(tenant_servers_), K(ret));
  } else {
    ret = OB_SUCCESS;
    // virtual invalid idx, just used for compute svr_count_in_zone
    if (OB_FAIL(first_idx_in_zone.push_back(svr_idx))) {
      SERVER_LOG(WARN, "fail to push back first_idx_in_zone", K(svr_idx), K(ret));
    } else {
      SERVER_LOG(INFO, "succ to fill tenant servers", K(tenant_servers_), K(first_idx_in_zone));
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_servers_by_join(
    const uint64_t tenant_id, ObIArray<int64_t>& first_idx_in_zone, bool& is_mysql_error)
{
  int ret = OB_SUCCESS;
  const bool did_retry_weak = GCTX.is_started_and_can_weak_read();
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_retry_weak);

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObSqlString sql;
    first_idx_in_zone.reset();
    tenant_servers_.reset();
    is_mysql_error = false;
    const static char* SELECT_TENANT_ACTIVE_SERVERS_SQL =
        "SELECT S.svr_ip, S.inner_port, S.zone "
        "FROM oceanbase.%s R, oceanbase.%s U, oceanbase.%s S "
        "WHERE R.tenant_id = %lu "
        "AND R.resource_pool_id = U.resource_pool_id "
        "AND ((U.svr_ip = S.svr_ip AND U.svr_port = S.svr_port) "
        "      OR (U.migrate_from_svr_port > 0 AND U.migrate_from_svr_ip = S.svr_ip AND U.migrate_from_svr_port = "
        "S.svr_port)) "
        "AND S.inner_port > 0 "
        "ORDER BY S.zone ASC "
        "LIMIT %ld";
    if (OB_FAIL(sql.append_fmt(SELECT_TENANT_ACTIVE_SERVERS_SQL,
            OB_ALL_RESOURCE_POOL_TNAME,
            OB_ALL_UNIT_TNAME,
            OB_ALL_SERVER_TNAME,
            tenant_id,
            INT64_MAX))) {
      SERVER_LOG(WARN, "failed to append sql info", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
      is_mysql_error = true;
      SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
    } else if (OB_FAIL(fill_tenant_servers(*result, first_idx_in_zone, tenant_id))) {
      SERVER_LOG(WARN, "failed to fill_tenant_servers", K(tenant_id), K(ret));
    } else {
      SERVER_LOG(INFO, "succ to get tenant_servers by join");
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_resource_pool_id(const uint64_t tenant_id, int64_t& resource_pool_id)
{
  int ret = OB_SUCCESS;
  resource_pool_id = -1;
  const bool did_retry_weak = GCTX.is_started_and_can_weak_read();
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_retry_weak);

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObSqlString sql;
    const static char* SELECT_RESOURCE_POOL_ID_SQL = "SELECT resource_pool_id "
                                                     "FROM oceanbase.%s "
                                                     "WHERE tenant_id = %lu "
                                                     "LIMIT 1";
    if (OB_FAIL(sql.append_fmt(SELECT_RESOURCE_POOL_ID_SQL, OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
      SERVER_LOG(WARN, "failed to append sql info", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
      SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
    } else {
      if (OB_FAIL(result->next())) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {  // no record
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          SERVER_LOG(INFO, "fail to get resource_pool_id", K(sql), K(ret));
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "resource_pool_id", resource_pool_id, int64_t);

        if (OB_SUCC(ret)) {
          // check if this is only one
          if (OB_UNLIKELY(OB_ITER_END != (ret = result->next()))) {
            SERVER_LOG(WARN, "fail to get tenant resource_pool_id, there is more than one record", K(ret));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_UNLIKELY(resource_pool_id < 0)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected resource_pool_id", K(resource_pool_id), K(ret));
          } else {
            ret = OB_SUCCESS;
            SERVER_LOG(INFO, "succ to get tenant resource_pool_id", K(resource_pool_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_server_addr(
    const int64_t resource_pool_id, ObIArray<ObAddr>& tenant_server_addr)
{
  int ret = OB_SUCCESS;
  if (resource_pool_id >= 0) {
    const bool did_retry_weak = GCTX.is_started_and_can_weak_read();
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_retry_weak);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      ObSqlString sql;
      tenant_server_addr.reset();
      const static char* SELECT_TENANT_SERVER_ADDR_SQL =
          "SELECT svr_ip, svr_port, migrate_from_svr_ip, migrate_from_svr_port "
          "FROM oceanbase.%s "
          "WHERE resource_pool_id = %ld "
          "LIMIT %ld";
      if (OB_FAIL(sql.append_fmt(SELECT_TENANT_SERVER_ADDR_SQL, OB_ALL_UNIT_TNAME, resource_pool_id, INT64_MAX))) {
        SERVER_LOG(WARN, "failed to append sql info", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
      } else {
        int32_t svr_port = UINT16_MAX;  // invalid
        ObString svr_ip;
        ObAddr servers_addr;

        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          servers_addr.reset();
          svr_ip.reset();
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int32_t);

          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(!servers_addr.set_ip_addr(svr_ip, svr_port))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "failed to set_ip_addr", K(svr_ip), K(svr_port), K(ret));
            } else if (OB_FAIL(tenant_server_addr.push_back(servers_addr))) {
              SERVER_LOG(WARN, "failed to push back tenant server addr", K(servers_addr), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            servers_addr.reset();
            svr_ip.reset();
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "migrate_from_svr_ip", svr_ip);
            EXTRACT_INT_FIELD_MYSQL(*result, "migrate_from_svr_port", svr_port, int32_t);

            if (OB_SUCC(ret) && servers_addr.set_ip_addr(svr_ip, svr_port)) {
              if (OB_FAIL(tenant_server_addr.push_back(servers_addr))) {
                SERVER_LOG(WARN, "failed to push back tenant server addr from migrate", K(servers_addr), K(ret));
              }
            }
          }
        }
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get tenant server addr", K(tenant_server_addr), K(ret));
        } else {
          ret = OB_SUCCESS;
          SERVER_LOG(INFO, "succ to get tenant server addr", K(tenant_server_addr));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_server_sql_port(
    const ObIArray<ObAddr>& tenant_server_addr, ObIArray<int64_t>& first_idx_in_zone, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!tenant_server_addr.empty()) {
    const bool did_retry_weak = GCTX.is_started_and_can_weak_read();
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_retry_weak);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      ObSqlString sql;
      first_idx_in_zone.reset();
      tenant_servers_.reset();
      const static char* SELECT_TENANT_SERVER_SQL_PORT_HEAD_SQL = "SELECT svr_ip, inner_port, zone "
                                                                  "FROM oceanbase.%s "
                                                                  "WHERE inner_port > 0 "
                                                                  "AND (svr_ip, svr_port) IN (";
      // body =  ('ip1', addr1), ('ip2', addr2), ('ip3', addr3)
      const static char* SELECT_TENANT_SERVER_SQL_PORT_TAIL_SQL = ") ORDER BY zone ASC LIMIT %ld";
      if (OB_FAIL(sql.append_fmt(SELECT_TENANT_SERVER_SQL_PORT_HEAD_SQL, OB_ALL_SERVER_TNAME))) {
        SERVER_LOG(WARN, "failed to append sql info", K(ret));
      } else {
        char ip_str[MAX_IP_ADDR_LENGTH] = {'\0'};
        int32_t port = tenant_server_addr.at(0).get_port();
        if (!tenant_server_addr.at(0).ip_to_string(ip_str, MAX_IP_ADDR_LENGTH) ||
            OB_FAIL(sql.append_fmt("('%s', %d)", ip_str, port))) {
          SERVER_LOG(WARN, "failed to append addr", "addr", tenant_servers_.at(0).location_.server_, K(ret));
        } else {
          for (int64_t i = 1; i < tenant_server_addr.count() && OB_SUCC(ret); ++i) {
            MEMSET(ip_str, 0, sizeof(ip_str));
            port = tenant_server_addr.at(i).get_port();
            if (!tenant_server_addr.at(i).ip_to_string(ip_str, MAX_IP_ADDR_LENGTH) ||
                OB_FAIL(sql.append_fmt(", ('%s',%d)", ip_str, port))) {
              SERVER_LOG(WARN, "failed to append addr", "addr", tenant_servers_.at(i).location_.server_, K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(SELECT_TENANT_SERVER_SQL_PORT_TAIL_SQL, INT64_MAX))) {
          SERVER_LOG(WARN, "failed to append tail sql", K(ret));
        } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
          SERVER_LOG(WARN, "failed to execute sql", K(sql), K(ret));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "failed to get result", "sql", sql.ptr(), K(result), K(ret));
        } else if (OB_FAIL(fill_tenant_servers(*result, first_idx_in_zone, tenant_id))) {
          SERVER_LOG(WARN, "failed to fill_tenant_servers", K(tenant_id), K(ret));
        } else {
          SERVER_LOG(INFO, "succ to get tenant_server sql_port");
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_servers_separately(
    const uint64_t tenant_id, ObIArray<int64_t>& first_idx_in_zone)
{
  int ret = OB_SUCCESS;
  int64_t resource_pool_id = -1;
  common::ObSEArray<ObAddr, 64> tenant_server_addr;
  first_idx_in_zone.reset();
  if (OB_FAIL(get_tenant_resource_pool_id(tenant_id, resource_pool_id))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get resource_pool_id", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_tenant_server_addr(resource_pool_id, tenant_server_addr))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get tenant_server_addr", K(tenant_server_addr), K(ret));
  } else if (OB_FAIL(get_tenant_server_sql_port(tenant_server_addr, first_idx_in_zone, tenant_id))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get resource_pool_id", K(first_idx_in_zone), K(tenant_id), K(ret));
  } else {
    SERVER_LOG(INFO, "succ to get tenant servers separately");
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_servers(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "sql proxy is null", K(ret));
  } else {
    // 1. get tenant servers
    bool is_mysql_error = false;
    ObSEArray<int64_t, 5> first_idx_in_zone;  // save first svr_idx_idx in zone
    if (OB_FAIL(get_tenant_servers_by_join(tenant_id, first_idx_in_zone, is_mysql_error))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get tenant servers by join", K(ret));
      if (is_mysql_error) {
        ret = OB_SUCCESS;
        SHARE_SCHEMA_LOG(WARN, "try to get tenant servers separately");
        if (OB_FAIL(get_tenant_servers_separately(tenant_id, first_idx_in_zone))) {
          SHARE_SCHEMA_LOG(WARN, "fail to get tenant servers separately", K(ret));
        }
      }
    }

    // 2. order servers
    if (OB_SUCC(ret) && OB_LIKELY(tenant_servers_.count() > 0) && OB_LIKELY(first_idx_in_zone.count() > 1)) {
      const int64_t server_count = tenant_servers_.count();
      const int64_t zone_count = first_idx_in_zone.count() - 1;  // NOTE:: the last one is virtual invalid;
      const int64_t replica_count = zone_count;
      const int64_t partition_count = (server_count / replica_count + (0 == server_count % replica_count ? 0 : 1));
      ObSEArray<int64_t, 6> unused_server_count;
      int64_t svr_count_in_zone = 0;
      int64_t svr_idx = 0;

      for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
        if (OB_UNLIKELY(0 >= (svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "it should not happened", K(first_idx_in_zone), K(svr_count_in_zone), K(ret));
        } else if (OB_FAIL(unused_server_count.push_back(svr_count_in_zone))) {
          SERVER_LOG(WARN, "fail to push back unused_server_count", K(i), K(svr_count_in_zone), K(ret));
        } else { /*do nothing*/
        }
      }

      if (OB_SUCC(ret)) {
        int64_t init_idx = (ObTimeUtility::current_time() + server_count) % server_count;  // random to pick zone
        int64_t finish_count = 0;
        while (finish_count < server_count && OB_SUCC(ret)) {
          ++init_idx;
          for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
            if (unused_server_count.at(i) > 0) {
              svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i);
              svr_idx = first_idx_in_zone.at(i) + (init_idx + i) % svr_count_in_zone;
              tenant_servers_.at(svr_idx).virtual_partition_id_ = finish_count / replica_count;
              ++finish_count;
              --unused_server_count.at(i);
            }  // end of unused_server_count > 0
          }    // end of for zone_count
        }      // end of while server_count

        // make the first one leader
        if (OB_SUCC(ret)) {
          for (int64_t partition_id = 0; partition_id < partition_count && OB_SUCC(ret);) {
            for (svr_idx = 0; svr_idx < server_count && partition_id < partition_count && OB_SUCC(ret); ++svr_idx) {
              // set the first partition location to leader
              if (partition_id == tenant_servers_.at(svr_idx).virtual_partition_id_) {
                tenant_servers_.at(svr_idx).location_.role_ = LEADER;
                ++partition_id;
              }
            }
          }
          SERVER_LOG(INFO, "succ to get tenant_servers_", K(tenant_servers_), K(partition_count), K(replica_count));
        }
      }
    }  // end of order servers
  }
  return ret;
}

int ObAllVirtualProxySchema::fill_row(share::schema::ObSchemaGetterGuard& schema_guard,
    const ObTableSchema& table_schema, const ObReplicaLocation& replica, const int64_t part_idx,
    const DupReplicaType dup_replica_type)
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  const int64_t col_count = output_column_ids_.count();

  int64_t paxos_replica_num = OB_INVALID_COUNT;
  const ObString table_name = table_schema.get_table_name_str();
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    uint64_t table_id = table_schema.get_table_id();

    int64_t part_num = 0;
    if (table_schema.is_index_local_storage()) {
      // if local index table, use it's main table's partition num and replica num
      const ObTableSchema* main_table_schema = NULL;
      uint64_t data_table_id = table_schema.get_data_table_id();
      if (OB_FAIL(schema_guard.get_table_schema(data_table_id, main_table_schema))) {
        SERVER_LOG(WARN, "fail to get table schema", K(data_table_id), K(table_schema), K(ret));
      } else if (OB_ISNULL(main_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to get table schema", K(data_table_id), K(table_schema), K(ret));
      } else {
        part_num = main_table_schema->get_all_part_num();
        if (OB_FAIL(main_table_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
          SERVER_LOG(WARN, "fail to get table paxos replica_num", KPC(main_table_schema), K(ret));
        }
      }
    } else {
      part_num = table_schema.get_all_part_num();
      if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
        SERVER_LOG(WARN, "fail to get table paxos replica_num", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(paxos_replica_num < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(
            WARN, "paxos replica num error", K(ret), K(paxos_replica_num), "table_id", table_schema.get_table_id());
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
          cells[cell_idx].set_varchar(input_table_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case PARTITION_ID: {
          cells[cell_idx].set_int(part_idx);
          break;
        }
        case SERVER_IP: {
          if (!replica.server_.ip_to_string(ip_buf, OB_IP_STR_BUFF)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(
                WARN, "fail to covert ip to string", K(cell_idx), K(output_column_ids_), K(col_id), K(replica), K(ret));
          } else {
            cells[cell_idx].set_varchar(ip_buf);
            cells[cell_idx].set_collation_type(coll_type);
          }
          break;
        }
        case SQL_PORT: {
          cells[cell_idx].set_int(replica.sql_port_);
          break;
        }
        case TABLE_ID: {
          cells[cell_idx].set_int(static_cast<int64_t>(table_id));
          break;
        }
        case ROLE: {
          cells[cell_idx].set_int(static_cast<int64_t>(replica.role_));
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
        case SPARE1: {  // int, used for replica type
          cells[cell_idx].set_int(static_cast<int64_t>(replica.replica_type_));
          break;
        }
        case SPARE2: {  // int, used for dup replica type
          cells[cell_idx].set_int(static_cast<int>(dup_replica_type));
          break;
        }
        case SPARE3: {  // int, replica_property_memstore_percent
          cells[cell_idx].set_int(replica.property_.get_memstore_percent());
          break;
        }
        case SPARE4: {  // varchar, for
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case SPARE5: {  // string, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case SPARE6: {  // string, unused
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case COMPLEX_TABLE_TYPE: {  // int
          cells[cell_idx].set_int(static_cast<int64_t>(complex_table_type_));
          break;
        }
        case LEVEL1_DECODED_DB_NAME: {  // varchar
          cells[cell_idx].set_varchar(level1_decoded_db_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL1_DECODED_TABLE_NAME: {  // varchar
          cells[cell_idx].set_varchar(level1_decoded_table_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL2_DECODED_DB_NAME: {  // varchar
          cells[cell_idx].set_varchar(level2_decoded_db_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case LEVEL2_DECODED_TABLE_NAME: {  // varchar
          cells[cell_idx].set_varchar(level2_decoded_table_name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(cell_idx), K(part_idx), K(output_column_ids_), K(col_id), K(ret));
          break;
        }
      }

      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }
  return ret;
}

int ObAllVirtualProxySchema::get_tenant_db_name(
    const uint64_t tenant_id, const uint64_t database_id, ObString& tenant_name, ObString& db_name)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
  const ObDatabaseSchema* db_schema = NULL;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema service is NULL", K(ret));
  } else if (OB_FAIL(full_schema_guard_.get_tenant_info(tenant_id, tenant_schema))) {
    SERVER_LOG(WARN, "failed to get tenant info", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    SERVER_LOG(WARN, "tenant not exist", K(tenant_id), K(ret));
  } else if (OB_FAIL(full_schema_guard_.get_database_schema(database_id, db_schema))) {
    SERVER_LOG(WARN, "failed to get database schema", K(ret), K(database_id));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    SERVER_LOG(WARN, "fail to get database name", K(tenant_id), K(database_id), K(ret));
  } else {
    tenant_name = tenant_schema->get_tenant_name_str();
    db_name = db_schema->get_database_name_str();
  }

  return ret;
}

int ObAllVirtualProxySchema::get_table_partition_location(const int64_t table_idx, const int64_t part_idx)
{
  return get_table_locations(table_idx, part_idx);
};

int ObAllVirtualProxySchema::get_table_locations(const int64_t table_idx, const int64_t part_idx)
{
  int ret = OB_SUCCESS;

  int64_t partition_idx = part_idx;
  int64_t table_count = table_schemas_.count();
  if (OB_UNLIKELY(table_idx < 0 || table_idx >= table_count)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid table idx", K(table_idx), K(table_count), K(ret));
  } else if (OB_UNLIKELY(part_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "partition id must be a positive number");
    SERVER_LOG(WARN, "partition id must be a positive number", K(table_idx), K(part_idx), K(ret));
  } else {
    const ObTableSchema* table_schema = table_schemas_.at(table_idx);
    int64_t table_id = 0;
    if (OB_ISNULL(table_schema) || OB_ISNULL(pl_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table_schema or pl_cache is NULL", K(table_schema), K_(pl_cache), K(ret));
    } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
      // impossible
    } else if ((table_schema->is_vir_table()) || (table_schema->is_tmp_table()) || (table_schema->is_view_table())) {
      // virtual table use virtual partition
      if (OB_FAIL(add_virtual_partition_location(table_id, partition_idx))) {
        SERVER_LOG(WARN, "fail to add fake partition location", K(ret));
      }
    } else if (OB_FAIL(transfer_table_schema(table_schema))) {
      SERVER_LOG(WARN, "fail to transfer table schema", KPC(table_schema), K(ret));
      // fetch again, table_schema mybe transfered
    } else if (FALSE_IT(table_id = table_schema->get_table_id())) {
      // impossible
    } else if (OB_FAIL(get_actual_partition_idx(*table_schema, partition_idx))) {
      SERVER_LOG(WARN, "fail to get actual partition idx", KPC(table_schema), K(partition_idx), K(ret));
    } else if (OB_FAIL(fetch_partition_location(*table_schema, table_id, partition_idx))) {
      SERVER_LOG(WARN, "fail to fetch partition location", K(table_id), K(partition_idx), K(ret));
    }
  }

  return ret;
}

inline int ObAllVirtualProxySchema::add_virtual_partition_location(const int64_t table_id, const int64_t partition_idx)
{
  INIT_SUCC(ret);
  // use virtual ip:port just tell client not fetch again
  const char* VIRTUAL_IP = GCONF.use_ipv6 ? "::" : "0.0.0.0";
  uint32_t VIRTUAL_PORT = 0;
  location_.reset();
  ObReplicaLocation replica_location;

  if (!replica_location.server_.set_ip_addr(VIRTUAL_IP, VIRTUAL_PORT)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to set ip addr", K(ret));
  } else {

    replica_location.sql_port_ = VIRTUAL_PORT;
    replica_location.role_ = FOLLOWER;
    replica_location.replica_type_ = REPLICA_TYPE_FULL;

    location_.set_table_id(table_id);
    location_.set_partition_id(partition_idx);
    location_.set_partition_cnt(0);

    if (OB_FAIL(location_.add_with_no_check(replica_location))) {
      SERVER_LOG(WARN, "fail to add replica location", K(ret));
    }
  }

  return ret;
}

inline int ObAllVirtualProxySchema::fetch_partition_location(
    const share::schema::ObTableSchema& table_schema, const int64_t table_id, const int64_t partition_idx)
{
  INIT_SUCC(ret);
  // fetch location
  bool is_cache_hit = false;
  // Only requires 100ms of effectiveness, no need for real-time
  const int64_t expire_renew_time = ObTimeUtility::current_time() - GCONF.location_cache_refresh_min_interval;
  /* 1. If it is a binding table, need to do a pkey->pgkey conversion
   * 2. If it is a standlone table, just use pg directly
   */
  uint64_t pe_id = OB_INVALID_ID;
  int64_t pe_partition_id = -1;
  if (!table_schema.get_binding()) {
    pe_id = table_id;
    pe_partition_id = partition_idx;
  } else {
    common::ObPGKey pg_key;
    if (OB_FAIL(table_schema.get_pg_key(table_id, partition_idx, pg_key))) {
      LOG_WARN("fail to get pg key", K(ret), K(ret), K(table_id), K(partition_idx));
    } else {
      pe_id = pg_key.get_tablegroup_id();
      pe_partition_id = pg_key.get_partition_id();
    }
  }
  if (OB_FAIL(ret)) {
    // fail
  } else if (!force_sql_refresh_ &&
             OB_FAIL(pl_cache_->get(pe_id, pe_partition_id, location_, expire_renew_time, is_cache_hit))) {
    SERVER_LOG(WARN, "fail to get table location", K(table_id), K(partition_idx), K(expire_renew_time), K(ret));
    if ((OB_LOCATION_NOT_EXIST == ret) || (OB_GET_LOCATION_TIME_OUT == ret)) {
      // if get location not exist or timeout, just return empty, do let upper layer retry
      ret = OB_SUCCESS;
      location_.reset();
    }
  } else if (force_sql_refresh_ &&
             OB_FAIL(pl_cache_->force_sql_get(pe_id, pe_partition_id, location_, expire_renew_time, is_cache_hit))) {
    SERVER_LOG(WARN, "fail to force get table location", K(table_id), K(partition_idx), K(expire_renew_time), K(ret));
    if ((OB_LOCATION_NOT_EXIST == ret) || (OB_GET_LOCATION_TIME_OUT == ret)) {
      // if get location not exist or timeout, just return empty, do let upper layer retry
      ret = OB_SUCCESS;
      location_.reset();
    }
  } else {
    if (is_cache_hit) {
      EVENT_INC(LOCATION_CACHE_PROXY_HIT);
    } else {
      EVENT_INC(LOCATION_CACHE_PROXY_MISS);
    }
  }

  return ret;
}

inline int ObAllVirtualProxySchema::get_actual_partition_idx(const ObTableSchema& table_schema, int64_t& partition_idx)
{
  INIT_SUCC(ret);
  // partition_idx has assign initial value outer

  // for partition table or non-partition table, use the first partition id,
  // if partition id is invalid.
  bool use_the_first_location = false;
  if (0 == partition_idx) {
    // if partition_idx is 0, we should use the first location
    use_the_first_location = true;
  } else {
    // the partition idx get from client should covnert to real partition id
    // (e.g. hash partition split)
    int64_t origin_part_idx = partition_idx;
    if (OB_FAIL(table_schema.convert_partition_idx_to_id(origin_part_idx, partition_idx))) {
      SERVER_LOG(
          WARN, "fail to convert partition idx to id", K(table_schema), K(origin_part_idx), K(partition_idx), K(ret));
      ret = OB_SUCCESS;
      use_the_first_location = true;
    } else {
      bool exist = false;
      bool check_dropped_partition = false;
      if (OB_FAIL(ObPartMgrUtils::check_part_exist(table_schema, partition_idx, check_dropped_partition, exist))) {
        SERVER_LOG(WARN, "failed to check part exist", K(table_schema), K(ret));
        // ignore this ret
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_DEBUG("check part exist", K(partition_idx), K(exist), K(table_schema));
      }
      if (!exist) {
        // if partition_idx not exist, we should use the first location
        use_the_first_location = true;
      }
    }
  }

  // generate the first partition id
  if (use_the_first_location) {
    bool check_dropped_schema = false;
    ObTablePartitionKeyIter iter(table_schema, check_dropped_schema);
    if (OB_FAIL(iter.next_partition_id_v2(partition_idx))) {
      // OB_ITER_END is unexpected too
      SERVER_LOG(WARN, "iter failed", K(table_schema), K(ret));
    }
  }
  return ret;
}

inline int ObAllVirtualProxySchema::transfer_table_schema(const ObTableSchema*& table_schema)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema service is NULL", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(table_schema), K(ret));
  }
  // local index table has the same partition location as it's main table.
  // local index doesn't has replica info in __all_meta_table(__all_tenant_meta_table in ob2.0),
  // and if we use local index's table_id to fetch location, will get nothing actually.
  // so here we should convert index schema to it's main table schema.
  else if (table_schema->is_index_local_storage()) {
    const ObTableSchema* local_index_schema = table_schema;
    table_schema = NULL;
    uint64_t data_table_id = local_index_schema->get_data_table_id();
    if (OB_FAIL(full_schema_guard_.get_table_schema(data_table_id, table_schema))) {
      SERVER_LOG(WARN, "fail to get table schema", K(data_table_id), KPC(local_index_schema), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get table schema", K(data_table_id), KPC(local_index_schema), K(ret));
    } else {
      LOG_TRACE("use data table location as local index table location", KPC(local_index_schema), KPC(table_schema));
    }
  } else {
    // nothing
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
