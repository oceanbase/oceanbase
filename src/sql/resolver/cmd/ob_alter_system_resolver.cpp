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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "common/ob_region.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_locality_parser.h"
#include "share/ob_time_utility2.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"
#include "share/ob_encryption_util.h"
#include "share/ob_encrypt_kms.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_zone_table_operation.h"
#include "share/backup/ob_backup_struct.h"

#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "sql/resolver/cmd/ob_clear_balance_task_stmt.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "observer/ob_server.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "observer/mysql/ob_query_response_time.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql {
typedef ObAlterSystemResolverUtil Util;

int ObAlterSystemResolverUtil::sanity_check(const ParseNode* parse_tree, ObItemType item_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree should not be null");
  } else if (OB_UNLIKELY(item_type != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", "expect", get_type_name(item_type), "actual", get_type_name(parse_tree->type_));
  } else if (OB_UNLIKELY(parse_tree->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid num_child", "num_child", parse_tree->num_child_);
  } else if (OB_UNLIKELY(NULL == parse_tree->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  }

  return ret;
}

template <typename RPC_ARG>
int ObAlterSystemResolverUtil::resolve_server_or_zone(const ParseNode* parse_tree, RPC_ARG& arg)
{
  int ret = OB_SUCCESS;
  arg.server_.reset();
  arg.zone_.reset();
  if (NULL != parse_tree) {
    switch (parse_tree->type_) {
      case T_IP_PORT: {
        if (OB_FAIL(resolve_server(parse_tree, arg.server_))) {
          LOG_WARN("resolve server address failed", K(ret));
        }
        break;
      }
      case T_ZONE: {
        if (OB_FAIL(resolve_zone(parse_tree, arg.zone_))) {
          LOG_WARN("resolve zone failed", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only server or zone type allowed here", "type", get_type_name(parse_tree->type_));
        break;
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_server_value(const ParseNode* parse_tree, ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree should not be null");
  } else if (OB_UNLIKELY(T_VARCHAR != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_VARCHAR", "type", get_type_name(parse_tree->type_));
  } else {
    char ip_port[128] = {0};
    snprintf(ip_port, 128, "%.*s", static_cast<int32_t>(parse_tree->str_len_), parse_tree->str_value_);
    if (OB_FAIL(server.parse_from_cstring(ip_port))) {
      LOG_WARN("string not in server address format", K(ip_port), K(ret));
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_replica_type(const ParseNode* parse_tree, ObReplicaType& replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    replica_type = REPLICA_TYPE_FULL;
    LOG_INFO("resolve_replica_type without any value. default to FULL.");
  } else if (OB_UNLIKELY(T_VARCHAR != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_VARCHAR", "type", get_type_name(parse_tree->type_));
  } else {
    int64_t len = parse_tree->str_len_;
    const char* str = parse_tree->str_value_;
    if (OB_FAIL(ObLocalityParser::parse_type(str, len, replica_type))) {
      if (OB_INVALID_ARGUMENT == ret) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "replica_type");
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_memstore_percent(
    const ParseNode* parse_tree, ObReplicaProperty& replica_property)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    replica_property.reset();
    LOG_INFO("resolve_memstore_percent without any value. default to 100");
  } else if (OB_UNLIKELY(T_INT != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_INT", "type", get_type_name(parse_tree->type_));
  } else {
    const ParseNode* node = parse_tree;
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else {
      int64_t temp_value = node->value_;
      if (temp_value > 100 || temp_value < 0) {
        LOG_WARN("err memstore percent");
      } else if (OB_FAIL(replica_property.set_memstore_percent(temp_value))) {
        LOG_WARN("set memstore percent failed");
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_server(const ParseNode* parse_tree, ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    server.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_IP_PORT))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode* node = parse_tree->children_[0];
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else if (OB_FAIL(resolve_server_value(node, server))) {
      LOG_WARN("resolve server value failed", K(ret));
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_zone(const ParseNode* parse_tree, ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    zone.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_ZONE))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode* node = parse_tree->children_[0];
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else if (node->value_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty zone string");
    } else {
      ObString zone_name(static_cast<int32_t>(node->str_len_), node->str_value_);
      if (OB_FAIL(zone.assign(zone_name))) {
        LOG_WARN("assign zone string failed", K(zone_name), K(ret));
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_tenant(
    const ParseNode* parse_tree, ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1>& tenant_name)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    tenant_name.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_TENANT_NAME))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode* node = parse_tree->children_[0];
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else if (node->value_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty tenant string");
    } else {
      ObString tenant(static_cast<int32_t>(node->str_len_), node->str_value_);
      if (OB_FAIL(tenant_name.assign(tenant))) {
        LOG_WARN("assign tenant string failed", K(tenant), K(ret));
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_tenant_id(const ParseNode* parse_tree, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  if (NULL == parse_tree) {
    tenant_id = OB_SYS_TENANT_ID;
  } else if (T_TENANT_NAME != parse_tree->type_ && T_TENANT_ID != parse_tree->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree));
  } else {
    if (T_TENANT_NAME == parse_tree->type_) {
      ObSchemaGetterGuard schema_guard;
      ObString tenant_name;
      tenant_name.assign_ptr(parse_tree->str_value_, static_cast<int32_t>(parse_tree->str_len_));
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("failed to get_tenant_schema_guard", KR(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
        LOG_WARN("failed to get tenant id from schema guard", KR(ret), K(tenant_name));
      }
    } else {
      tenant_id = parse_tree->value_;
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_partition_id(
    const uint64_t tenant_id, const ParseNode* parse_tree, ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sanity_check(parse_tree, T_PARTITION_ID_DESC))) {
    LOG_WARN("sanity check failed");
  } else {
    common::ObSqlString buf;
    const ParseNode* node = parse_tree->children_[0];
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else if (OB_FAIL(buf.append(node->str_value_, node->str_len_))) {
      SQL_RESV_LOG(WARN, "fail to assign str value to buf", K(ret));
    } else if (OB_FAIL(parse_partition_id_desc(tenant_id, buf.ptr(), partition_key))) {
      LOG_WARN("parse partition id desc failed", K(ret), "string", node->str_value_);
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_force_opt(const ParseNode* parse_tree, bool& force_cmd)
{
  int ret = OB_SUCCESS;
  force_cmd = false;
  if (OB_ISNULL(parse_tree)) {
    // nothing todo
  } else {
    force_cmd = true;
  }
  return ret;
}

int ObAlterSystemResolverUtil::parse_partition_id_desc(
    const uint64_t tenant_id, const char* partition_id, ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  if (NULL == partition_id || 0 == strlen(partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty partitin_id string", K(partition_id));
  } else {
    const char* p = partition_id;
    char* endptr = NULL;
    int64_t idx = strtol(p, &endptr, 0);
    int64_t cnt = 0;
    uint64_t table_id = 0;
    p = endptr;
    // skip space
    while ('\0' != *p && isspace(*p)) {
      p++;
    }
    if ('%' != *p) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      p++;
      cnt = strtol(p, &endptr, 0);
      p = endptr;
      // skip space
      while ('\0' != *p && isspace(*p)) {
        p++;
      }
      if ('@' != *p) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        p++;
        table_id = strtoul(p, &endptr, 0);
        p = endptr;
        // skip space
        while ('\0' != *p && isspace(*p)) {
          p++;
        }
        if ('\0' != *p) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }

    // for compatible
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema* table = NULL;
      if (OB_ISNULL(GCTX.schema_service_)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid argument", K(tmp_ret), K(GCTX.schema_service_));
      } else if (OB_SUCCESS !=
                 (tmp_ret = GCTX.schema_service_->get_tenant_schema_guard(extract_tenant_id(table_id), schema_guard))) {
        SERVER_LOG(WARN, "get_schema_guard failed", K(tenant_id), K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = schema_guard.get_table_schema(table_id, table))) {
        SERVER_LOG(WARN, "get_table_schema failed", K(table_id), K(tmp_ret));
      } else if (OB_ISNULL(table) || table->is_dropped_schema()) {
        tmp_ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "table not exist", K(table_id), K(tmp_ret));
      } else {
        cnt = table->get_partition_cnt();
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("invalid partition id format", K(partition_id), K(ret));
    } else {
      partition_key.init(table_id, idx, cnt);
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_string(const ParseNode* node, ObString& string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null");
  } else if (OB_UNLIKELY(T_VARCHAR != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_VARCHAR", "type", get_type_name(node->type_));
  } else if (OB_UNLIKELY(node->str_len_ <= 0)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("empty string");
  } else {
    string = ObString(node->str_len_, node->str_value_);
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_relation_name(const ParseNode* node, ObString& string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null");
  } else if (OB_UNLIKELY(T_IDENT != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_IDENT", "type", get_type_name(node->type_));
  } else if (OB_UNLIKELY(node->str_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty string");
  } else {
    string = ObString(node->str_len_, node->str_value_);
  }
  return ret;
}

int ObAlterSystemResolverUtil::check_same_with_gconf(const common::ObString& str, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  ObBackupDest dst1, dst2;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (str.empty()) {
    is_same = true;
  } else if (OB_FAIL(dst1.set(str.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest", KR(ret));
  } else if (0 == strlen(backup_dest_str)) {
    is_same = false;
  } else if (OB_FAIL(dst2.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret));
  } else {
    is_same = dst1.is_root_path_equal(dst2);
  }
  return ret;
}

int ObFreezeResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObFreezeStmt* freeze_stmt = NULL;
  ParseNode* servers_node = NULL;
  ParseNode* tenants_or_partition_node = NULL;
  ParseNode* zone_node = NULL;
  if (OB_UNLIKELY(NULL == parse_tree.children_) || OB_UNLIKELY(parse_tree.num_child_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong freeze parse tree", KP(parse_tree.children_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (NULL == (freeze_stmt = create_stmt<ObFreezeStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObFreezeStmt failed");
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong freeze type", KP(parse_tree.children_[0]));
  } else if (T_INT != parse_tree.children_[0]->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong freeze type", K(parse_tree.children_[0]->type_));
  } else {
    stmt_ = freeze_stmt;
    if (1 == parse_tree.children_[0]->value_) {  // MAJOR FREEZE
      if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong freeze parse tree", K(parse_tree.num_child_));
      } else {
        freeze_stmt->set_major_freeze(true);
        servers_node = parse_tree.children_[1];
      }
    } else if (2 == parse_tree.children_[0]->value_) {  // MINOR FREEZE
      if (OB_UNLIKELY(4 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong freeze parse tree", K(parse_tree.num_child_));
      } else {
        freeze_stmt->set_major_freeze(false);
        tenants_or_partition_node = parse_tree.children_[1];
        servers_node = parse_tree.children_[2];
        zone_node = parse_tree.children_[3];
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown freeze type", K(parse_tree.children_[0]->value_));
    }
  }

  // resolve zone
  if (OB_SUCC(ret) && NULL != zone_node) {
    if (OB_FAIL(Util::resolve_zone(zone_node, freeze_stmt->get_zone()))) {
      LOG_WARN("resolve zone failed", K(ret));
    }
  }

  // resolve tenants
  if (OB_SUCC(ret) && NULL != tenants_or_partition_node && T_TENANT_LIST == tenants_or_partition_node->type_) {
    const ParseNode* tenants_node = tenants_or_partition_node;
    ObSchemaGetterGuard schema_guard;
    if (OB_UNLIKELY(NULL == tenants_node->children_) || OB_UNLIKELY(0 == tenants_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of tenant should not be null", K(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is empty", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info_->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else {
      common::ObIArray<uint64_t>& tenant_ids = freeze_stmt->get_tenant_ids();
      uint64_t tenant_id = 0;
      ObString tenant_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenants_node->num_child_; ++i) {
        ParseNode* node = tenants_node->children_[i];
        if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children of server_list should not be null");
        } else {
          tenant_name.assign_ptr(node->str_value_, static_cast<ObString::obstr_size_t>(node->str_len_));
          if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
            LOG_WARN("tenant not exist", K(tenant_name));
          } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push tenant id ", K(tenant_name), K(tenant_id));
          }
        }
        tenant_name.reset();
      }
    }
  }

  // resolve partition
  if (OB_SUCC(ret) && NULL != tenants_or_partition_node && T_PARTITION_ID_DESC == tenants_or_partition_node->type_) {
    const ParseNode* partition_node = tenants_or_partition_node;
    if (OB_FAIL(Util::resolve_partition_id(
            session_info_->get_effective_tenant_id(), partition_node, freeze_stmt->get_partition_key()))) {
      LOG_WARN("resolve partition id failed");
    }
  }

  // resolve observer list
  if (OB_SUCC(ret) && NULL != servers_node) {
    if (OB_UNLIKELY(NULL == servers_node->children_) || OB_UNLIKELY(0 == servers_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of server_list should not be null");
    } else {
      ObString addr_str;
      ObAddr server;
      for (int64_t i = 0; OB_SUCC(ret) && i < servers_node->num_child_; ++i) {
        ParseNode* node = servers_node->children_[i];
        if (OB_ISNULL(node)) {
          LOG_WARN("children of server_list should not be null");
        } else {
          addr_str.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          if (OB_FAIL(server.parse_from_string(addr_str))) {
            LOG_WARN("parse server from cstring failed", K(addr_str));
          } else if (OB_FAIL(freeze_stmt->push_server(server))) {
            SQL_RESV_LOG(WARN, "push back failed", K(server));
          }
        }
        server.reset();
        addr_str.reset();
      }
    }
  }
  return ret;
}

  //
  // This node has five children_ and they are following:
  // cache_type_: parse_tree.children_[0]
  // opt_sql_id: parse_tree.children_[1]
  // opt_databases: parse_tree.children_[2]
  // opt_tenant_list: parse_tree.children_[3]
  // flush_scope: parse_tree.children_[4]
  //
int ObFlushCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushCacheStmt *stmt = NULL;
  ObSQLSessionInfo* sess = params_.session_info_;
  if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid session");
  } else if (OB_UNLIKELY(T_FLUSH_CACHE != parse_tree.type_ || parse_tree.num_child_ != 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument",
             "type", get_type_name(parse_tree.type_),
             "child_num", parse_tree.num_child_);
  } else if (NULL == parse_tree.children_[0]
             || NULL == parse_tree.children_[4]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (NULL == (stmt = create_stmt<ObFlushCacheStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObFlushCacheStmt failed");
  } else {
    ObSchemaGetterGuard schema_guard;

    // first child: resolve cache type
    stmt->flush_cache_arg_.cache_type_ = (ObCacheType)parse_tree.children_[0]->value_;
    // second child: resolve sql_id
    ParseNode *sql_id_node = parse_tree.children_[1];
    // for adds database id
    // third child: resolve db_list
    ParseNode *db_node = parse_tree.children_[2];
    // for adds tenant ids
    // fourth child: resolve tenant list
    ParseNode *t_node = parse_tree.children_[3];
    // fivth child: resolve application fields
    stmt->is_global_ = parse_tree.children_[4]->value_;
    // whether is coarse granularity plan cache evict.
    // tenant level(true) / pcv_set level(false)
    bool is_coarse_granularity = true;
    ObSEArray<common::ObString, 8> db_name_list;

    // sql_id
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(sql_id_node)) {
      // do nothing
    // currently, only support plan cache's fine-grained cache evict
    } else if (stmt->flush_cache_arg_.cache_type_ != CACHE_TYPE_PLAN) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support plan cache's fine-grained cache evict", K(stmt->flush_cache_arg_.cache_type_), K(ret));
    } else if (lib::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported plan cache's fine-grained cache evict in oracle mode", K(ret));
    } else if (OB_ISNULL(sql_id_node->children_)
               || OB_ISNULL(sql_id_node->children_[0])
               || T_SQL_ID != sql_id_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (sql_id_node->children_[0]->str_len_ > (OB_MAX_SQL_ID_LENGTH+1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      stmt->flush_cache_arg_.sql_id_.assign_ptr(
          sql_id_node->children_[0]->str_value_,
          static_cast<ObString::obstr_size_t>(sql_id_node->children_[0]->str_len_));
      stmt->flush_cache_arg_.is_fine_grained_ = true;
    }

    // retrive schema guard
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(GCTX.schema_service_));
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info_->get_effective_tenant_id(), schema_guard))) {
      SERVER_LOG(WARN, "get_schema_guard failed", K(ret));
    } else {
      // do nothing
    }

    // db names
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!stmt->flush_cache_arg_.is_fine_grained_) {
      if (OB_ISNULL(db_node)) {
        // tenant level plan cache evict
        // and not needs to specify db_name
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported flush cache in database level", K(ret));
      }
    } else if (NULL == db_node) { // db list is empty
      // empty db list means clear all db's in fine-grained cache evict
      // do nothing
    } else if (OB_ISNULL(db_node->children_)
               || OB_ISNULL(db_node->children_[0])
               || T_DATABASE_LIST != db_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      uint64_t db_id = 0;
      ObString db_names;
      ObString db_name;
      db_names.assign_ptr(db_node->children_[0]->str_value_,
                          static_cast<ObString::obstr_size_t>(db_node->children_[0]->str_len_));
      while (OB_SUCC(ret) && !db_names.empty()) {
        db_name = db_names.split_on(',').trim();
        if(db_name.empty() && NULL == db_names.find(',')) {
          db_name = db_names;
          db_names.reset();
        }
        if(!db_name.empty()) {
          if (OB_FAIL(db_name_list.push_back(db_name))) {
            SERVER_LOG(WARN, "failed to add database name", K(ret));
          }
        }
      } // for database name end
    }

    /*
     * different database belongs to different tenant,
     * and we will use following logics to retrive db_id:
     * for (tenant list) {
     *    for (database_name_list) {
     *      // find db_id from schema
     *      args_.push_back(db_id);
     *    }
     * }
     * */
    // tenant list
    if (OB_FAIL(ret)) {
    } else if (NULL == t_node) { //tenant list is empty
      if (!stmt->flush_cache_arg_.is_fine_grained_) { // coarse grained cache evict
        // Notes:
        // tenant level evict, and no tenant list specified means all tenant
        // for system tenant: empty means flush all tenant's 
        // for normal tenant: this node has been set as NULL in parse phase,
        //                    and already adds its tenant id to tenant list in above
        // Therefore, do nothing
        if (sess->get_effective_tenant_id() == OB_SYS_TENANT_ID) {
          //sys_tenant do nothing
        } else {
          //normal tenant add its tenant id
          stmt->flush_cache_arg_.push_tenant(sess->get_effective_tenant_id());
        }
      } else { // fine-grained cache evcit
        // for fine-grained plan evict, we must specify tenant list
        uint64_t t_id = OB_INVALID_ID;
        t_id = sess->get_effective_tenant_id();
        if (t_id <= OB_MAX_RESERVED_TENANT_ID) {// system tenant will use this path.
          // system tenant must specify tenant_list;
          ret = OB_EMPTY_TENANT;
          SERVER_LOG(WARN, "invalid argument, fine-grained plan evict must specify tenant_list", K(ret));
        } else { // normal tenant
          if (OB_FAIL(stmt->flush_cache_arg_.push_tenant(sess->get_effective_tenant_id()))) {
            LOG_WARN("failed  to adds tenant for normal tenant", K(sess->get_effective_tenant_id()), K(ret));
          } else {
            // normal tenant will use it's tenant_id when t_node is empty
            for (uint64_t j=0; OB_SUCC(ret) && j<db_name_list.count(); j++) {
              uint64_t db_id = 0;
              if (OB_FAIL(schema_guard.get_database_id(t_id, db_name_list.at(j).trim(), db_id))) {
                SERVER_LOG(WARN, "database not exist", K(db_name_list.at(j).trim()), K(ret));
              } else if (OB_FAIL(stmt->flush_cache_arg_.push_database(db_id))) {
                SERVER_LOG(WARN, "fail to push database id ",K(db_name_list.at(j).trim()), K(db_id), K(ret));
              }
            } // for get db_id ends
            LOG_INFO("normal tenant flush plan cache ends", K(t_id), K(db_name_list));
          }
        } // normal tenant ends
      } // fine-grained plan evcit ends
    } else if (sess->get_effective_tenant_id() != OB_SYS_TENANT_ID) {
    // tenant node is not null and current tenant is not sys tenant
    // due to normal tenant cannot specify tenant, and only can purge
    // their own plan cache
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do this operation", K(ret), K(sess->get_effective_tenant_id()));
    } else if (NULL == t_node->children_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(ret));
    } else {
      uint64_t tenant_id = 0;
      ObString tenant_name;
      // adds tenant_ids and get db_ids
      for (int64_t i = 0; OB_SUCC(ret) && i < t_node->num_child_; ++i) {
        if (OB_ISNULL(t_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid argument", K(t_node->children_[i]), K(ret));
        } else {
          tenant_name.assign_ptr(
              t_node->children_[i]->str_value_, static_cast<ObString::obstr_size_t>(t_node->children_[i]->str_len_));
          if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
            SERVER_LOG(WARN, "tenant not exist", K(tenant_name), K(ret));
          } else if (OB_FAIL(stmt->flush_cache_arg_.push_tenant(tenant_id))) {
            SERVER_LOG(WARN, "fail to push tenant id ",K(tenant_name), K(tenant_id), K(ret));
          } else {
            ObSchemaGetterGuard schema_guard_db;
            if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_db))) {
              SERVER_LOG(WARN, "get_schema_guard failed", K(ret), K(tenant_id));
            } else {
              for (uint64_t j = 0; OB_SUCC(ret) && j < db_name_list.count(); j++) {
                uint64_t db_id = 0;
                if (OB_FAIL(schema_guard_db.get_database_id(tenant_id, db_name_list.at(j), db_id))) {
                  SERVER_LOG(WARN, "database not exist", K(db_name_list.at(j)), K(ret));
                } else if ((int64_t)db_id == OB_INVALID_ID) {
                  ret = OB_ERR_BAD_DATABASE;
                  SERVER_LOG(WARN, "database not exist", K(db_name_list.at(j)), K(ret));
                } else if (OB_FAIL(stmt->flush_cache_arg_.push_database(db_id))) {
                  SERVER_LOG(WARN, "fail to push database id ",K(db_name_list.at(j)), K(db_id), K(ret));
                }
              } // for get db_id ends
            }
          }
        } // for get tenant_id ends
        tenant_name.reset();
      }  // for tenant end
    }
    LOG_INFO("resolve flush command finished!", K(ret), K(stmt->is_global_), K(stmt->flush_cache_arg_.cache_type_),
                K(stmt->flush_cache_arg_.sql_id_), K(stmt->flush_cache_arg_.is_fine_grained_),
                K(stmt->flush_cache_arg_.tenant_ids_), K(stmt->flush_cache_arg_.db_ids_));
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    if (OB_ISNULL(schema_checker_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(schema_checker_->check_ora_ddl_priv(
        session_info_->get_effective_tenant_id(),
        session_info_->get_priv_user_id(),
        ObString(""),
        // why use T_ALTER_SYSTEM_SET_PARAMETER?
        // because T_ALTER_SYSTEM_SET_PARAMETER has following traits:
        // T_ALTER_SYSTEM_SET_PARAMETER can allow dba to do an operation
        // and prohibit other user to do this operation
        // so we reuse this.
        stmt::T_ALTER_SYSTEM_SET_PARAMETER,
        session_info_->get_enable_role_array()))) {
      LOG_WARN("failed to check privilege", K(session_info_->get_effective_tenant_id()), K(session_info_->get_user_id()));
    }
  }
  return ret;
}

int ObFlushKVCacheResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FLUSH_KVCACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_FLUSH_KVCACHE", "type", get_type_name(parse_tree.type_));
  } else {
    ObFlushKVCacheStmt* stmt = create_stmt<ObFlushKVCacheStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObFlushKVCacheStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode* node = parse_tree.children_[0];
        if (NULL == node) {
          stmt->tenant_name_.reset();
        } else {
          if (OB_UNLIKELY(NULL == node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("children should not be null");
          } else {
            node = node->children_[0];
            if (OB_UNLIKELY(NULL == node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node should not be null");
            } else {
              if (node->str_len_ <= 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("empty tenant name");
              } else {
                ObString tenant_name(node->str_len_, node->str_value_);
                if (OB_FAIL(stmt->tenant_name_.assign(tenant_name))) {
                  LOG_WARN("assign tenant name failed", K(tenant_name), K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          node = parse_tree.children_[1];
          if (NULL == node) {
            stmt->cache_name_.reset();
          } else {
            if (OB_UNLIKELY(NULL == node->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children should not be null");
            } else {
              node = node->children_[0];
              if (OB_UNLIKELY(NULL == node)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("node should not be null");
              } else {
                if (node->str_len_ <= 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("empty cache name");
                } else {
                  ObString cache_name(node->str_len_, node->str_value_);
                  if (OB_FAIL(stmt->cache_name_.assign(cache_name))) {
                    LOG_WARN("assign cache name failed", K(cache_name), K(ret));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFlushIlogCacheResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushIlogCacheStmt* stmt = NULL;
  if (OB_UNLIKELY(T_FLUSH_ILOGCACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type not match T_FLUSH_ILOGCACHE", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObFlushIlogCacheStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ObFlushCacheStmt error", K(ret));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of parse tree is null", K(ret));
  } else {
    ParseNode* opt_file_id_node = parse_tree.children_[0];
    ParseNode* file_id_val_node = NULL;
    if (OB_ISNULL(opt_file_id_node)) {
      stmt->file_id_ = 0;
    } else if (OB_ISNULL(opt_file_id_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("opt_file_id_node.children is null", K(ret));
    } else if (OB_ISNULL(file_id_val_node = opt_file_id_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file_id_val_node is null", K(ret));
    } else {
      int64_t file_id_val = file_id_val_node->value_;  // type of value_ is int64_t
      if (file_id_val <= 0 || file_id_val >= INT32_MAX) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid file_id when flush ilogcache", K(ret), K(file_id_val));
      } else {
        stmt->file_id_ = (int32_t)file_id_val;
        stmt_ = stmt;
        LOG_INFO("flush ilogcache resolve succ", K(file_id_val));
      }
    }
  }
  return ret;
}

int ObFlushDagWarningsResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushDagWarningsStmt* stmt = NULL;
  if (OB_UNLIKELY(T_FLUSH_DAG_WARNINGS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type not match T_FLUSH_DAG_WARNINGS", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObFlushDagWarningsStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ObFlushDagWarningsStmt error", K(ret));
  }
  return ret;
}

int ObLoadBaselineResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObLoadBaselineStmt* stmt = NULL;
  if (OB_UNLIKELY(T_LOAD_BASELINE != parse_tree.type_ || NULL == parse_tree.children_ || parse_tree.num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", "type", get_type_name(parse_tree.type_), "child_num", parse_tree.num_child_);
  } else if (NULL == (stmt = create_stmt<ObLoadBaselineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObFlushCacheStmt failed");
  } else {
    // tenant id
    ObSchemaGetterGuard schema_guard;
    ParseNode* t_node = parse_tree.children_[0];
    if (NULL == t_node) {  // tenant list is empty
      // do nothing
    } else if (NULL == t_node->children_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid argument", K(GCTX.schema_service_));
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info_->get_effective_tenant_id(), schema_guard))) {
      SERVER_LOG(WARN, "get_schema_guard failed", K(ret));
    } else {
      uint64_t tenant_id = 0;
      ObString tenant_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < t_node->num_child_; ++i) {
        if (OB_ISNULL(t_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid argument", K(t_node->children_[i]), K(ret));
        } else {
          tenant_name.assign_ptr(
              t_node->children_[i]->str_value_, static_cast<ObString::obstr_size_t>(t_node->children_[i]->str_len_));
          if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
            SERVER_LOG(WARN, "tenant not exist", K(tenant_name), K(ret));
          } else if (OB_FAIL(stmt->load_baseline_arg_.push_tenant(tenant_id))) {
            SERVER_LOG(WARN, "fail to push tenant id ", K(tenant_name), K(tenant_id), K(ret));
          }
        }
        tenant_name.reset();
      }  // for tenant end
    }
    // sql_id
    ParseNode* sql_id_node = parse_tree.children_[1];
    if (OB_FAIL(ret)) {
      // do_nothing
    } else if (OB_ISNULL(sql_id_node)) {
      // do_nothing
    } else if (OB_ISNULL(sql_id_node->children_) || OB_ISNULL(sql_id_node->children_[0]) ||
               T_SQL_ID != sql_id_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      stmt->load_baseline_arg_.sql_id_.assign_ptr(sql_id_node->children_[0]->str_value_,
          static_cast<ObString::obstr_size_t>(sql_id_node->children_[0]->str_len_));
    }
  }

  return ret;
}

int ObAdminServerResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_SERVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_SERVER", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminServerStmt* admin_server_stmt = NULL;
    if (NULL == (admin_server_stmt = create_stmt<ObAdminServerStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "create ObAdminServerStmt failed");
    } else {
      stmt_ = admin_server_stmt;
      ParseNode* admin_op = NULL;
      ParseNode* server_list = NULL;
      ParseNode* zone_info = NULL;
      if (3 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "child num not right", "child num", parse_tree.num_child_, K(ret));
      } else {
        admin_op = parse_tree.children_[0];
        server_list = parse_tree.children_[1];
        zone_info = parse_tree.children_[2];
        if (OB_UNLIKELY(NULL == admin_op || NULL == server_list)) {
          SQL_RESV_LOG(WARN, "admin_op and server_list should not be null");
          ret = OB_ERR_UNEXPECTED;
        } else {
          admin_server_stmt->set_op(static_cast<ObAdminServerArg::AdminServerOp>(admin_op->value_));
          ObServerList& servers = admin_server_stmt->get_server_list();
          for (int64_t i = 0; OB_SUCC(ret) && i < server_list->num_child_; ++i) {
            ObString addr_str;
            ObAddr server;
            if (OB_UNLIKELY(NULL == server_list->children_)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "children of server_list should not be null");
            } else if (OB_UNLIKELY(NULL == server_list->children_[i])) {
              ret = OB_ERR_UNEXPECTED;
              _SQL_RESV_LOG(WARN, "children[%ld] of server_list should not be null", i);
            } else {
              addr_str.assign_ptr(
                  server_list->children_[i]->str_value_, static_cast<int32_t>(server_list->children_[i]->str_len_));
              if (OB_FAIL(server.parse_from_string(addr_str))) {
                SQL_RESV_LOG(WARN, "parse server from string failed", K(addr_str), K(ret));
              } else if (OB_FAIL(servers.push_back(server))) {
                SQL_RESV_LOG(WARN, "push back failed", K(server), K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (NULL != zone_info) {
              ObString zone_str;
              ObZone zone;
              if (OB_UNLIKELY(NULL == zone_info->children_)) {
                ret = OB_ERR_UNEXPECTED;
                SQL_RESV_LOG(WARN, "children of zone_info should not be null");
              } else if (OB_UNLIKELY(NULL == zone_info->children_[0])) {
                ret = OB_ERR_UNEXPECTED;
                SQL_RESV_LOG(WARN, "children[0] of zone_info should not be null");
              } else {
                zone_str.assign_ptr(
                    zone_info->children_[0]->str_value_, static_cast<int32_t>(zone_info->children_[0]->str_len_));
                if (OB_FAIL(zone.assign(zone_str))) {
                  SQL_RESV_LOG(WARN, "assign failed", K(zone_str), K(ret));
                } else {
                  admin_server_stmt->set_zone(zone);
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminZoneResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObAdminZoneStmt* admin_zone_stmt = NULL;
  if (OB_UNLIKELY(T_ADMIN_ZONE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_ZONE", "type", get_type_name(parse_tree.type_));
  } else if (NULL == (admin_zone_stmt = create_stmt<ObAdminZoneStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "create ObAdminZoneStmt failed");
  } else {
    stmt_ = admin_zone_stmt;
    if (3 != parse_tree.num_child_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "child num not right", "child num", parse_tree.num_child_, K(ret));
    } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "children should not be null");
    } else {
      ParseNode* admin_op = parse_tree.children_[0];
      ParseNode* zone_info = parse_tree.children_[1];
      ParseNode* zone_options = parse_tree.children_[2];

      ObZone zone;
      if (OB_UNLIKELY(NULL == admin_op || NULL == zone_info)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "admin_op and zone_info should not be null");
      } else if (admin_op->value_ < 1 || admin_op->value_ > 7) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid action code", "action", admin_op->value_);
      } else if (OB_FAIL(zone.assign(ObString(zone_info->str_len_, zone_info->str_value_)))) {
        SQL_RESV_LOG(WARN, "assign zone failed", K(zone), K(ret));
      } else {
        admin_zone_stmt->set_zone(zone);
        ObAdminZoneArg::AdminZoneOp op = static_cast<ObAdminZoneArg::AdminZoneOp>(admin_op->value_);
        admin_zone_stmt->set_op(op);
        if (NULL != zone_options) {
          for (int64_t i = 0; OB_SUCC(ret) && i < zone_options->num_child_; ++i) {
            if (NULL == zone_options->children_[i]) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "region info is null", K(ret));
            } else if (T_REGION == zone_options->children_[i]->type_) {
              ObRegion region;
              ParseNode* region_info = zone_options->children_[i];
              if (admin_zone_stmt->has_alter_region_option()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple region option");
              } else if (region_info->str_len_ <= 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty region");
              } else if (OB_FAIL(region.assign(ObString(region_info->str_len_, region_info->str_value_)))) {
                SQL_RESV_LOG(WARN, "assign region failed", K(ret));
              } else if (OB_FAIL(admin_zone_stmt->set_alter_region_option())) {
                SQL_RESV_LOG(WARN, "fail to set alter region option", K(ret));
              } else {
                admin_zone_stmt->set_region(region);
              }
            } else if (T_IDC == zone_options->children_[i]->type_) {
              ObIDC idc;
              ParseNode* idc_info = zone_options->children_[i];
              if (admin_zone_stmt->has_alter_idc_option()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple idc option");
              } else if (idc_info->str_len_ <= 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty idc");
              } else if (OB_FAIL(idc.assign(ObString(idc_info->str_len_, idc_info->str_value_)))) {
                SQL_RESV_LOG(WARN, "idc assign failed", K(ret));
              } else if (OB_FAIL(admin_zone_stmt->set_alter_idc_option())) {
                SQL_RESV_LOG(WARN, "fail to set alter idc option", K(ret));
              } else {
                admin_zone_stmt->set_idc(idc);
              }
            } else if (T_ZONE_TYPE == zone_options->children_[i]->type_) {
              ParseNode* zone_type_info = zone_options->children_[i];
              ObZoneType zone_type = str_to_zone_type(zone_type_info->str_value_);
              if (admin_zone_stmt->has_alter_zone_type_option()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple zone_type option");
              } else if (zone_type == ObZoneType::ZONE_TYPE_INVALID) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("unexpected zone_type info", "info", zone_type_info->value_);
              } else if (OB_FAIL(admin_zone_stmt->set_alter_zone_type_option())) {
                SQL_RESV_LOG(WARN, "fail to set alter zone_type option", K(ret));
              } else {
                admin_zone_stmt->set_zone_type(zone_type);
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected zone option", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSwitchReplicaRoleResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SWITCH_REPLICA_ROLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SWITCH_REPLICA_ROLE", "type", get_type_name(parse_tree.type_));
  } else {
    ObSwitchReplicaRoleStmt* stmt = create_stmt<ObSwitchReplicaRoleStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSwitchReplicaRoleStmt failed");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else {
      stmt_ = stmt;
      ObAdminSwitchReplicaRoleArg& rpc_arg = stmt->get_rpc_arg();
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      }

      if (OB_SUCC(ret)) {
        ParseNode* role = parse_tree.children_[0];
        if (OB_UNLIKELY(NULL == role)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("role should not be null");
        } else if (OB_UNLIKELY(T_INT != role->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", "type", get_type_name(role->type_));
        } else {
          switch (role->value_) {
            case 0: {
              rpc_arg.role_ = LEADER;
              break;
            }
            case 1: {
              rpc_arg.role_ = FOLLOWER;
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected role value", "value", role->value_);
              break;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        ParseNode* node = parse_tree.children_[1];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else if (OB_UNLIKELY(NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children should not be null");
        } else {
          switch (node->type_) {
            case T_PARTITION_ID_SERVER: {
              if (OB_FAIL(Util::resolve_partition_id(
                      session_info_->get_effective_tenant_id(), node->children_[0], rpc_arg.partition_key_))) {
                LOG_WARN("resolve partition id failed", K(ret));
              } else if (OB_FAIL(Util::resolve_server(node->children_[1], rpc_arg.server_))) {
                LOG_WARN("resolve server failed", K(ret));
              }
              break;
            }
            case T_SERVER_TENANT: {
              if (OB_FAIL(Util::resolve_server(node->children_[0], rpc_arg.server_))) {
                LOG_WARN("resolve server failed", K(ret));
              } else if (OB_FAIL(Util::resolve_tenant(node->children_[1], rpc_arg.tenant_name_))) {
                LOG_WARN("resolve tenant failed", K(ret));
              }
              break;
            }
            case T_ZONE_TENANT: {
              if (OB_FAIL(Util::resolve_zone(node->children_[0], rpc_arg.zone_))) {
                LOG_WARN("resolve zone failed", K(ret));
              } else if (OB_FAIL(Util::resolve_tenant(node->children_[1], rpc_arg.tenant_name_))) {
                LOG_WARN("resolve tenant failed", K(ret));
              }
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected type", "type", get_type_name(node->type_));
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObChangeReplicaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CHANGE_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_DROP_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObChangeReplicaStmt* stmt = create_stmt<ObChangeReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropReplicaStmt failed");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ObAddr server;
        ObReplicaType type;
        ObReplicaProperty replica_property;
        if (OB_FAIL(Util::resolve_partition_id(session_info_->get_effective_tenant_id(),
                parse_tree.children_[0],
                stmt->get_rpc_arg().partition_key_))) {
          LOG_WARN("resolve partition id failed", K(ret));
        } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[1], server))) {
          LOG_WARN("resolve server failed", K(ret));
        } else if (OB_FAIL(Util::resolve_force_opt(parse_tree.children_[3], stmt->get_rpc_arg().force_cmd_))) {
          LOG_WARN("fail to resolve force opt", K(ret));
        } else {
          ParseNode* node = parse_tree.children_[2];
          if (OB_ISNULL(node) || OB_UNLIKELY(0 == node->num_child_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("children of action node should not be null");
          }
          bool is_replica_exist = false;
          bool is_memstore_exist = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
            if (OB_ISNULL(node->children_[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children of action node should not be null");
            } else if (T_REPLICA_TYPE == node->children_[i]->type_) {
              if (true == is_replica_exist) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify replica type more than once");
                LOG_WARN("only enter replica type once", K(ret));
              } else if (OB_SUCC(ret)) {
                is_replica_exist = true;
                if (node->children_[i]->num_child_ != 1 || OB_ISNULL(node->children_[i]->children_[0])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid replica type node", K(ret), K(node), "child_num", node->children_[i]->num_child_);
                } else if (OB_FAIL(Util::resolve_replica_type(node->children_[i]->children_[0], type))) {
                  LOG_WARN("fail to resove repilca type", K(ret));
                }
              }
            } else if (T_MEMSTORE_PERCENT == node->children_[i]->type_) {
              if (true == is_memstore_exist) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify memstore percentage more than once");
                LOG_WARN("only enter memstore percent once", K(ret));
              } else if (OB_SUCC(ret)) {
                is_memstore_exist = true;
                if (node->children_[i]->num_child_ != 1 || OB_ISNULL(node->children_[i]->children_[0])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid replica type node", K(ret), K(node), "child_num", node->children_[i]->num_child_);
                } else if (OB_FAIL(
                               Util::resolve_memstore_percent(node->children_[i]->children_[0], replica_property))) {
                  LOG_WARN("fail to resolve memstore percent", K(ret));
                }
              }
            } else {
              LOG_WARN("invalid child", K(ret), K(node->type_), K(i), "type", node->children_[i]->type_);
            }
          }
          if (OB_SUCC(ret)) {
            if (((REPLICA_TYPE_FULL == type) &&
                    (0 == replica_property.get_memstore_percent() || 100 == replica_property.get_memstore_percent())) ||
                ((REPLICA_TYPE_LOGONLY == type) && (100 == replica_property.get_memstore_percent())) ||
                ((REPLICA_TYPE_READONLY == type) && (100 == replica_property.get_memstore_percent()))) {
              stmt->get_rpc_arg().member_ = ObReplicaMember(server, 0, type, replica_property.get_memstore_percent());
            } else if ((REPLICA_TYPE_FULL == type) && (0 != replica_property.get_memstore_percent() ||
                                                          100 != replica_property.get_memstore_percent())) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("if replica_type is FULL, memstor_percent must be 0 or 100", K(ret));
            } else if ((REPLICA_TYPE_LOGONLY == type) && (100 != replica_property.get_memstore_percent())) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("if replica_type is LOGONLY, memstor_percent must be 100", K(ret));
            } else if ((REPLICA_TYPE_READONLY == type) && (100 != replica_property.get_memstore_percent())) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("if replica_type is READONLY, memstor_percent must be 100", K(ret));
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("wrong input", K(ret));
            }
          }
        }  // end else
      }
    }
  }
  return ret;
}

int ObDropReplicaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_DROP_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_DROP_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObDropReplicaStmt* stmt = create_stmt<ObDropReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropReplicaStmt failed");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        if (OB_FAIL(Util::resolve_partition_id(session_info_->get_effective_tenant_id(),
                parse_tree.children_[0],
                stmt->get_rpc_arg().partition_key_))) {
          LOG_WARN("resolve partition id failed", K(ret));
        } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[1], stmt->get_rpc_arg().server_))) {
          LOG_WARN("resolve server failed", K(ret));
        } else if (OB_FAIL(Util::resolve_zone(parse_tree.children_[3], stmt->get_rpc_arg().zone_))) {
          LOG_WARN("resolve zone failed");
        } else if (OB_FAIL(Util::resolve_force_opt(parse_tree.children_[4], stmt->get_rpc_arg().force_cmd_))) {
          LOG_WARN("fail to resolve force opt", K(ret));
        } else {
          ParseNode* node = parse_tree.children_[2];
          if (NULL != node) {
            if (OB_UNLIKELY(NULL == node->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children should not be null");
            } else if (OB_UNLIKELY(NULL == node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children[0] should not be null");
            } else {
              stmt->get_rpc_arg().create_timestamp_ = node->children_[0]->value_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMigrateReplicaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_MIGRATE_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MIGRATE_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObMigrateReplicaStmt* stmt = create_stmt<ObMigrateReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObMigrateReplicaStmt failed");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode* node = parse_tree.children_[0];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else if (OB_UNLIKELY(T_INT != node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node type is not T_INT", "type", get_type_name(node->type_));
        } else {
          switch (node->value_) {
            case 1: {
              stmt->get_rpc_arg().is_copy_ = false;
              break;
            }
            case 2: {
              stmt->get_rpc_arg().is_copy_ = true;
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected migrate action value", "value", node->value_);
              break;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(Util::resolve_partition_id(session_info_->get_effective_tenant_id(),
                parse_tree.children_[1],
                stmt->get_rpc_arg().partition_key_))) {
          LOG_WARN("resolve partition id failed", K(ret));
        } else if (OB_FAIL(Util::resolve_server_value(parse_tree.children_[2], stmt->get_rpc_arg().src_))) {
          LOG_WARN("resolve server failed", K(ret));
        } else if (OB_FAIL(Util::resolve_server_value(parse_tree.children_[3], stmt->get_rpc_arg().dest_))) {
          LOG_WARN("resolve server failed", K(ret));
        } else if (OB_FAIL(Util::resolve_force_opt(parse_tree.children_[4], stmt->get_rpc_arg().force_cmd_))) {
          LOG_WARN("fail to resolve force opt", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReportReplicaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REPORT_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REPORT_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObReportReplicaStmt* stmt = create_stmt<ObReportReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReportReplicaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRecycleReplicaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RECYCLE_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RECYCLE_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObRecycleReplicaStmt* stmt = create_stmt<ObRecycleReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRecycleReplicaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAdminMergeResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_MERGE_CONTROL != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MERGE_CONTROL", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminMergeStmt* stmt = create_stmt<ObAdminMergeStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminMergeStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode* node = parse_tree.children_[0];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else if (OB_UNLIKELY(T_INT != node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", "type", get_type_name(node->type_));
        } else {
          switch (node->value_) {
            case 1: {
              stmt->get_rpc_arg().type_ = ObAdminMergeArg::START_MERGE;
              break;
            }
            case 2: {
              stmt->get_rpc_arg().type_ = ObAdminMergeArg::SUSPEND_MERGE;
              break;
            }
            case 3: {
              stmt->get_rpc_arg().type_ = ObAdminMergeArg::RESUME_MERGE;
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected merge admin type", "value", node->value_);
              break;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(Util::resolve_zone(parse_tree.children_[1], stmt->get_rpc_arg().zone_))) {
          LOG_WARN("resolve zone failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObClearRootTableResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CLEAR_ROOT_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_ROOT_TABLE", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearRoottableStmt* stmt = create_stmt<ObClearRoottableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearRoottableStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode* node = parse_tree.children_[0];
        if (NULL == node) {
          stmt->get_rpc_arg().tenant_name_.reset();
        } else {
          if (OB_UNLIKELY(NULL == node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("children should not be null");
          } else {
            node = node->children_[0];
            if (OB_UNLIKELY(NULL == node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node should not be null");
            } else {
              if (node->str_len_ <= 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("empty tenant name");
              } else {
                ObString tenant_name(node->str_len_, node->str_value_);
                if (OB_FAIL(stmt->get_rpc_arg().tenant_name_.assign(tenant_name))) {
                  LOG_WARN("assign tenant name failed", K(tenant_name), K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRefreshSchemaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_SCHEMA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_SCHEMA", "type", get_type_name(parse_tree.type_));
  } else {
    ObRefreshSchemaStmt* stmt = create_stmt<ObRefreshSchemaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRefreshSchemaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRefreshMemStatResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_MEMORY_STAT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_MEMORY_STAT", "type", get_type_name(parse_tree.type_));
  } else {
    ObRefreshMemStatStmt* stmt = create_stmt<ObRefreshMemStatStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRefreshMemStatStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int check_backup_dest(const ObString& backup_dest)
{
  int ret = OB_SUCCESS;
  bool is_doing_backup = false;
  share::ObBackupDest dest;
  char backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  char backup_backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  ObLogArchiveBackupInfoMgr backup_info_mgr;
  ObClusterBackupDest cluster_dest;
  ObTenantLogArchiveStatus last_status;

  if (OB_FAIL(share::ObBackupInfoMgr::get_instance().check_if_doing_backup(is_doing_backup))) {
    LOG_WARN("failed to check_if_doing_backup", K(ret));
  } else if (is_doing_backup) {
    ret = OB_BACKUP_IN_PROGRESS;
    LOG_WARN("cannot set backup dest during backup", K(ret), K(backup_dest_buf), K(is_doing_backup));
  } else if (backup_dest.length() == 0) {
    // not check empty dest
  } else if (OB_FAIL(databuff_printf(
                 backup_dest_buf, sizeof(backup_dest_buf), "%.*s", backup_dest.length(), backup_dest.ptr()))) {
    LOG_WARN("failed to print backup dest buf", K(ret), K(backup_dest));
  } else if (OB_FAIL(dest.set(backup_dest_buf))) {
    LOG_WARN("failed to set dest", K(ret), K(backup_dest_buf));
  } else if (dest.is_nfs_storage() && 0 != strlen(dest.storage_info_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup device is nfs, storage_info should be empty", K(ret), K_(dest.storage_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup device is nfs, additional parameters are");
  } else if (dest.is_nfs_storage() && strlen(dest.root_path_) != strlen(backup_dest_buf)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup device is nfs, backup dest should not set '?'", K(ret), K_(dest.storage_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup device is nfs, setting '?' is"); 
  } else if (OB_FAIL(cluster_dest.set(dest, OB_START_INCARNATION))) {
    LOG_WARN("Failed to set cluster dest", K(ret), K(dest));
  } else if (OB_FAIL(backup_info_mgr.get_last_extern_log_archive_backup_info(
                 cluster_dest, OB_SYS_TENANT_ID, last_status))) {
    if (OB_LOG_ARCHIVE_BACKUP_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get_last_extern_log_archive_backup_info", K(ret), K(cluster_dest));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("no last archive backup info exist", K(cluster_dest));
    }
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cannot set backup dest with old backup data", K(ret), K(backup_dest_buf), K(last_status));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_buf, sizeof(backup_backup_dest_buf)))) {
      LOG_WARN("failed to set backup dest buf", K(ret));
    } else if (strlen(backup_backup_dest_buf) > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot change backup dest when backup backup dest is not empty", K(ret), K(backup_backup_dest_buf));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "set backup dest with backup backup dest not empty is");
    }
  }

  if (OB_SUCC(ret)) {
    share::ObBackupDest old_dest;
    if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_buf, sizeof(backup_dest_buf)))) {
      LOG_WARN("failed to set backup dest buf", K(ret));
    } else if (0 == strlen(backup_dest_buf)) {
      LOG_INFO("[BACKUP_DEST]set new backup dest", K(dest));
      ROOTSERVICE_EVENT_ADD(
          "backup_dest", "backup_dest", "old_backup_dest", "null", "new_backup_dest", dest.root_path_);
    } else if (OB_FAIL(old_dest.set(backup_dest_buf))) {
      LOG_WARN("failed to set conf backup dest", K(ret), K(backup_dest_buf));
    } else {
      LOG_INFO("[BACKUP_DEST]set new backup dest", K(old_dest), K(dest));
      ROOTSERVICE_EVENT_ADD(
          "backup_dest", "backup_dest", "old_backup_dest", old_dest.root_path_, "new_backup_dest", dest.root_path_);
    }
  }

  return ret;
}

int init_backup_dest_opt(
    const bool is_backup_backup, const ObString& new_opt_str, ObBackupDestOpt& cur_opt, ObBackupDestOpt& new_opt)
{
  int ret = OB_SUCCESS;
  char cur_opt_buf[OB_MAX_CONFIG_VALUE_LEN] = "";
  char new_opt_buf[OB_MAX_CONFIG_VALUE_LEN] = "";
  const bool global_auto_delete_obsolete_backup = GCONF.auto_delete_expired_backup;
  const bool auto_update_reserved_backup_timestamp = GCONF._auto_update_reserved_backup_timestamp;
  const int64_t global_backup_recovery_window = GCONF.backup_recovery_window;
  const int64_t global_log_archive_checkount_interval = GCONF.log_archive_checkpoint_interval;

  if (OB_FAIL(databuff_printf(new_opt_buf, sizeof(new_opt_buf), "%.*s", new_opt_str.length(), new_opt_str.ptr()))) {
    LOG_WARN("failed to copy opt buf", K(ret), K(new_opt_str));
  } else if (!is_backup_backup && OB_FAIL(GCONF.backup_dest_option.copy(cur_opt_buf, sizeof(cur_opt_buf)))) {
    LOG_WARN("failed to copy backup dest option buf", K(ret));
  } else if (is_backup_backup && OB_FAIL(GCONF.backup_backup_dest_option.copy(cur_opt_buf, sizeof(cur_opt_buf)))) {
    LOG_WARN("failed to copy backup backup dest option buf", K(ret));
  } else if (OB_FAIL(cur_opt.init(is_backup_backup,
                 cur_opt_buf,
                 global_auto_delete_obsolete_backup,
                 global_backup_recovery_window,
                 global_log_archive_checkount_interval,
                 auto_update_reserved_backup_timestamp))) {
    LOG_WARN("failed to init cur opt", K(ret));
  } else if (OB_FAIL(new_opt.init(is_backup_backup,
                 new_opt_buf,
                 global_auto_delete_obsolete_backup,
                 global_backup_recovery_window,
                 global_log_archive_checkount_interval,
                 auto_update_reserved_backup_timestamp))) {
    LOG_WARN("failed to init new opt", K(ret));
  }

  return ret;
}

int check_backup_dest_opt(const bool is_backup_backup, const ObString& opt_str)
{
  int ret = OB_SUCCESS;
  bool is_doing_backup = false;
  ObLogArchiveBackupInfoMgr backup_info_mgr;
  ObBackupDestOpt cur_opt;
  ObBackupDestOpt new_opt;
  const int64_t MIN_LOG_ARCHIVE_CHECKPOINT_INTERVAL = 5 * 1000LL * 1000LL;                // 5s
  const int64_t MAX_LOG_ARCHIVE_CHECKPOINT_INTERVAL = 3600 * 1000LL * 1000LL;             // 1h
  const int64_t MIN_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL = 24 * 3600 * 1000LL * 1000LL;      // 1d
  const int64_t MAX_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL = 7 * 24 * 3600 * 1000LL * 1000LL;  // 7d
  const int64_t OB_MAX_BACKUP_COPIES = 8;

  if (OB_FAIL(init_backup_dest_opt(is_backup_backup, opt_str, cur_opt, new_opt))) {
    LOG_WARN("failed to init backup dest opt", K(ret), K(is_backup_backup), K(opt_str));
  } else {
    // check field range
    if (OB_SUCC(ret)) {
      if (new_opt.log_archive_checkpoint_interval_ < MIN_LOG_ARCHIVE_CHECKPOINT_INTERVAL ||
          new_opt.log_archive_checkpoint_interval_ > MAX_LOG_ARCHIVE_CHECKPOINT_INTERVAL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid log_archive_checkpoint_interval", K(ret), K(opt_str), K(new_opt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid log_archive_checkpoint_interval out of range [5s,1h] is");
      }
    }

    if (OB_SUCC(ret)) {
      if (new_opt.recovery_window_ < 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid recovery_window", K(ret), K(opt_str), K(new_opt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid recovery_window less than 0 is");
      }
    }

    if (OB_SUCC(ret)) {
      if (new_opt.auto_delete_obsolete_backup_ && new_opt.auto_touch_reserved_backup_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cannot auto delete and touch backup at same time", K(ret), K(opt_str), K(new_opt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto delete and touch backup at same time is");
      }
    }

    if (OB_SUCC(ret)) {
      if (0 != new_opt.piece_switch_interval_ &&
          (new_opt.piece_switch_interval_ < MIN_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL ||
              new_opt.piece_switch_interval_ > MAX_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL)) {
#ifndef ERRSIM
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid piece_switch_interval", K(ret), K(opt_str), K(new_opt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid piece_switch_interval out of range [1d,7d] is");
#endif
      }
    }
    if (OB_SUCC(ret)) {
      if (0 != new_opt.backup_copies_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid backup copies", K(ret), K(opt_str), K(new_opt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup_copies out of range [0,8] is");
      }
    }
  }

  if (OB_SUCC(ret) && is_backup_backup) {
    if (new_opt.backup_copies_ > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot set copies for backup backup dest", K(ret), K(opt_str), K(new_opt));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "set backup_copies for backup backup dest is");
    } else if (new_opt.piece_switch_interval_ != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot set piece_switch_interval for backup backup dest", K(ret), K(opt_str), K(new_opt));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "set piece_switch_interval for backup backup dest is");
    }
  }

  if (FAILEDx(share::ObBackupInfoMgr::get_instance().check_if_doing_backup(is_doing_backup))) {
    LOG_WARN("failed to check_if_doing_backup", K(ret));
  } else if (is_doing_backup) {
    if (cur_opt.is_switch_piece() != new_opt.is_switch_piece()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot change switch piece mode during log archive is running", K(ret), K(cur_opt), K(new_opt));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "change switch piece mode during log archive running is");
    }
  }

  return ret;
}

int check_backup_backup_dest(const ObString& backup_backup_dest)
{
  int ret = OB_SUCCESS;
  bool is_empty_dir = false;
  share::ObBackupDest src_dest;
  share::ObBackupDest dst_dest;
  ObStorageUtil util(false /*need_retry*/);
  char backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  char backup_backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  bool is_doing_backup_backup = false;
  if (OB_FAIL(share::ObBackupInfoMgr::get_instance().check_if_doing_backup_backup(is_doing_backup_backup))) {
    LOG_WARN("failed to check_if_doing_backup_backup", K(ret));
  } else if (is_doing_backup_backup) {
    ret = OB_BACKUP_IN_PROGRESS;
    LOG_WARN("cannot set backup dest during backup", K(ret), K(is_doing_backup_backup));
  } else if (backup_backup_dest.empty()) {
    // not check empty dest
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_buf, sizeof(backup_dest_buf)))) {
    LOG_WARN("failed to set backup dest buf", K(ret));
  } else if (OB_FAIL(databuff_printf(backup_backup_dest_buf,
                 sizeof(backup_backup_dest_buf),
                 "%.*s",
                 backup_backup_dest.length(),
                 backup_backup_dest.ptr()))) {
    LOG_WARN("failed to print backup dest buf", K(ret), K(backup_backup_dest));
  } else if (strlen(backup_dest_buf) == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set backup backup dest with empty backup dest is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set backup backup dest with empty backup dest is");
  } else if (OB_FAIL(src_dest.set(backup_dest_buf))) {
    LOG_WARN("failed to set dest", K(ret), K(backup_dest_buf));
  } else if (OB_FAIL(dst_dest.set(backup_backup_dest_buf))) {
    LOG_WARN("failed to set dest", K(ret), K(backup_backup_dest_buf));
  } else if (dst_dest.is_nfs_storage() && 0 != strlen(dst_dest.storage_info_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup backup device is nfs, storage_info should be empty", K(ret), K_(dst_dest.storage_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup backup device is nfs, additional parameters are"); 
  } else if (dst_dest.is_nfs_storage() && strlen(dst_dest.root_path_) != strlen(backup_backup_dest_buf)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup backup device is nfs, backup backup dest should not set '?'", K(ret), K_(dst_dest.storage_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup backup device is nfs, setting '?' is"); 
  } else if (dst_dest.is_cos_storage()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup backup do not support cos storage", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup backup on cos storage");
  } else if (0 == STRNCMP(src_dest.root_path_, dst_dest.root_path_, OB_MAX_BACKUP_PATH_LENGTH)) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("backup backup dest can not be same with backup dest", K(ret), K(backup_dest_buf));
  } else if (OB_FAIL(util.is_empty_directory(dst_dest.root_path_, dst_dest.storage_info_, is_empty_dir))) {
    LOG_WARN("failed to check is empty directory", K(ret), K(dst_dest));
  } else if (!is_empty_dir) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_ERROR("cannot use backup backup dest with non empty directory", K(ret));
  }

  if (OB_SUCC(ret)) {
    share::ObBackupDest old_dest;
    if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_buf, sizeof(backup_backup_dest_buf)))) {
      LOG_WARN("failed to set backup backup dest buf", K(ret));
    } else if (0 == strlen(backup_backup_dest_buf)) {
      LOG_INFO("[BACKUP_DEST]set new backup backup dest", K(dst_dest));
      ROOTSERVICE_EVENT_ADD("backup_dest",
          "backup_backup_dest",
          "old_backup_backup_dest",
          "null",
          "new_backup_backup_dest",
          dst_dest.root_path_);
    } else if (OB_FAIL(old_dest.set(backup_backup_dest_buf))) {
      LOG_WARN("failed to set conf backup backup dest", K(ret), K(backup_backup_dest_buf));
    } else {
      LOG_INFO("[BACKUP_DEST]set new backup backup dest", K(old_dest), K(dst_dest));
      ROOTSERVICE_EVENT_ADD("backup_dest",
          "backup_backup_dest",
          "old_backup_backup_dest",
          old_dest.root_path_,
          "new_backup_backup_dest",
          dst_dest.root_path_);
    }
  }
  return ret;
}

int check_enable_log_archive(const ObString& is_enable)
{
  int ret = OB_SUCCESS;
  bool is_doing_backup = false;
  share::ObBackupDest dest;
  char backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  ObLogArchiveBackupInfoMgr backup_info_mgr;
  ObClusterBackupDest cluster_dest;
  ObTenantLogArchiveStatus last_status;

  if (0 == is_enable.case_compare("false") || 0 == is_enable.case_compare("0")) {
    // not check for false
  } else if (OB_FAIL(share::ObBackupInfoMgr::get_instance().check_if_doing_backup(is_doing_backup))) {
    LOG_WARN("failed to check_if_doing_backup", K(ret));
  } else if (!is_doing_backup) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set enable log archive true without archivelog is");
    LOG_WARN("set enable log archive true without archivelog is not supported", K(ret), K(is_enable));
  }

  FLOG_INFO("check_enable_log_archive", K(ret), K(is_doing_backup), K(is_enable));
  return ret;
}

int check_backup_region(const ObString& backup_region)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupRegion> backup_region_array;
  ObArray<ObRegion> region_array;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_region, MAX_REGION_LENGTH, backup_region_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(backup_region));
  } else if (OB_FAIL(share::ObZoneTableOperation::get_region_list(*GCTX.sql_proxy_, region_array))) {
    LOG_WARN("failed to get region list", K(ret));
  } else {
    for (int64_t i = 0; i < backup_region_array.count(); ++i) {
      const ObRegion& tmp_region = backup_region_array.at(i).region_;
      bool found = false;
      for (int64_t j = 0; !found && j < region_array.count(); ++j) {
        const ObRegion& region = region_array.at(j);
        if (tmp_region == region) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_CANNOT_SET_BACKUP_REGION;
        LOG_WARN("backup region is not exist in region list", K(ret), K(backup_region), K(region_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup region do not exist in region list. backup region : %s.",
                               backup_region.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_REGION, error_msg);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObString backup_zone_str = GCONF.backup_zone.get_value_string();
      if (!backup_zone_str.empty()) {
        ret = OB_CANNOT_SET_BACKUP_REGION;
        LOG_WARN("backup zone str is not empty", K(ret), K(backup_zone_str));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup zone has been setup already. backup zone : %s.",
                               backup_zone_str.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_REGION, error_msg);
        }
      }
    }
  }
  return ret;
}

int check_backup_zone(const ObString& backup_zone)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupZone> backup_zone_array;
  ObArray<ObZone> zone_array;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_zone, MAX_REGION_LENGTH, backup_zone_array))) {
    LOG_WARN("failed to parse backup format input", K(ret), K(backup_zone));
  } else if (OB_FAIL(share::ObZoneTableOperation::get_zone_list(*GCTX.sql_proxy_, zone_array))) {
    LOG_WARN("failed to get region list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_zone_array.count(); ++i) {
      const ObZone& tmp_zone = backup_zone_array.at(i).zone_;
      bool found = false;
      for (int64_t j = 0; !found && j < zone_array.count(); ++j) {
        const ObZone& zone = zone_array.at(j);
        if (tmp_zone == zone) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_CANNOT_SET_BACKUP_ZONE;
        LOG_WARN("backup zone is not exist in zone list", K(ret), K(backup_zone), K(zone_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup zone do not exist in zone list. backup zone : %s.",
                               backup_zone.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_ZONE, error_msg);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObString backup_region_str = GCONF.backup_region.get_value_string();
      if (!backup_region_str.empty()) {
        ret = OB_CANNOT_SET_BACKUP_ZONE;
        LOG_WARN("backup region str is not empty", K(ret), K(backup_region_str));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup region has been setup already. backup region : %s.",
                               backup_region_str.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_ZONE, error_msg);
        }
      }
    }
  }
  return ret;
}

int check_auto_delete_expired_backup(const ObString& is_enable)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  const bool auto_update_reserved_backup_timestamp = GCONF._auto_update_reserved_backup_timestamp;
  bool is_doing = false;
  bool is_enable_value = false;
  bool is_valid = false;
  ObBackupDestOpt backup_dest_opt;
  ObBackupDestOpt backup_backup_dest_opt;

  is_enable_value = ObConfigBoolParser::get(is_enable.ptr(), is_valid);
  if (!is_valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bool str", K(ret), K(is_enable));
  } else if (!is_enable_value) {
    // not check for false
  } else if (OB_FAIL(backup_dest_opt.init(false /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup dest opt", K(ret));
  } else if (OB_FAIL(backup_backup_dest_opt.init(true /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup backup dest opt", K(ret));
  } else if (auto_update_reserved_backup_timestamp || backup_dest_opt.auto_touch_reserved_backup_ ||
             backup_backup_dest_opt.auto_touch_reserved_backup_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto delete and touch backup at same time is");
    LOG_WARN("set auto delete expired backup true when auto update reserved backup timestamp is true",
        K(ret),
        K(auto_update_reserved_backup_timestamp),
        K(backup_dest_opt),
        K(backup_backup_dest_opt));
  } else if (OB_FAIL(updater.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init clean info updater", K(ret));
  } else if (OB_FAIL(updater.check_clean_task_is_dong(is_doing))) {
    LOG_WARN("failed to check clean task status", K(ret));
  } else if (is_doing) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "can not set auto delete expired backup true when clean task is running");
    LOG_WARN("can not set auto delete expired backup true when clean task is running", K(ret), K(is_doing));
  }
  return ret;
}

int check_auto_update_reserved_backup_timestamp(const ObString& is_enable)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  const bool auto_delete_expired_backup = GCONF.auto_delete_expired_backup;
  bool is_doing = false;
  bool is_enable_value = false;
  bool is_valid = false;
  ObBackupDestOpt backup_dest_opt;
  ObBackupDestOpt backup_backup_dest_opt;

  is_enable_value = ObConfigBoolParser::get(is_enable.ptr(), is_valid);
  if (!is_valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bool str", K(ret), K(is_enable));
  } else if (!is_enable_value) {
    // not check for false
  } else if (OB_FAIL(backup_dest_opt.init(false /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup dest opt", K(ret));
  } else if (OB_FAIL(backup_backup_dest_opt.init(true /*is_backup_backup*/))) {
    LOG_WARN("failed to init backup backup dest opt", K(ret));
  } else if (auto_delete_expired_backup || backup_dest_opt.auto_delete_obsolete_backup_ ||
             backup_backup_dest_opt.auto_delete_obsolete_backup_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "auto delete and touch backup at same time is");
    LOG_WARN("set auto update reserved backup timestamp true when auto delete expired backup is true",
        K(ret),
        K(auto_delete_expired_backup),
        K(backup_dest_opt),
        K(backup_backup_dest_opt));
  } else if (OB_FAIL(updater.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init clean info updater", K(ret));
  } else if (OB_FAIL(updater.check_clean_task_is_dong(is_doing))) {
    LOG_WARN("failed to check clean task status", K(ret));
  } else if (is_doing) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "can not set auto update reserved backup timestamp true when clean is doing is ");
    LOG_WARN(
        "set auto update reserved backup timestamp true when clean is doing is not supported", K(ret), K(is_doing));
  }
  return ret;
}

/* for mysql mode */
int ObSetConfigResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_SET_PARAMETER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_SYSTEM_SET_PARAMETER", "type", get_type_name(parse_tree.type_));
  } else {
    if (OB_UNLIKELY(NULL == parse_tree.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children should not be null");
    } else {
      const ParseNode* list_node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == list_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list_node should not be null");
      } else {
        ObSetConfigStmt* stmt = create_stmt<ObSetConfigStmt>();
        if (OB_UNLIKELY(NULL == stmt)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("create stmt failed");
        } else {
          bool has_major_compact_trigger = false;
          bool has_perf_audit = false;
          ObCreateTableResolver ddl_resolver(params_);
          for (int64_t i = 0; OB_SUCC(ret) && i < list_node->num_child_; ++i) {
            if (OB_UNLIKELY(NULL == list_node->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children should not be null");
              break;
            }

            const ParseNode* action_node = list_node->children_[i];
            if (NULL == action_node) {
              continue;
            }

            // config name
            ObAdminSetConfigItem item;
            if (OB_LIKELY(session_info_ != NULL)) {
              item.exec_tenant_id_ = session_info_->get_effective_tenant_id();
            } else {
              LOG_WARN("session is null");
              item.exec_tenant_id_ = OB_INVALID_TENANT_ID;
            }

            if (OB_UNLIKELY(NULL == action_node->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children should not be null");
              break;
            }

            if (OB_UNLIKELY(NULL == action_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children[0] should not be null");
              break;
            }

            ObString name(action_node->children_[0]->str_len_, action_node->children_[0]->str_value_);
            ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
            if (OB_FAIL(item.name_.assign(name))) {
              LOG_WARN("assign config name failed", K(name), K(ret));
              break;
            }

            // config value
            ObObjParam val;
            if (OB_UNLIKELY(NULL == action_node->children_[1])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children[1] should not be null");
              break;
            } else if (OB_FAIL(ddl_resolver.resolve_default_value(action_node->children_[1], val))) {
              LOG_WARN("resolve config value failed", K(ret));
              break;
            }
            ObString str_val;
            ObCollationType cast_coll_type = CS_TYPE_INVALID;
            if (OB_LIKELY(session_info_ != NULL)) {
              if (OB_SUCCESS != session_info_->get_collation_connection(cast_coll_type)) {
                LOG_WARN("fail to get collation_connection");
                cast_coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
              } else {
              }
            } else {
              LOG_WARN("session is null");
              cast_coll_type = ObCharset::get_system_collation();
            }
            ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
            ObCastCtx cast_ctx(&allocator,
                NULL,  // to varchar. this field will not be used.
                0,     // to varchar. this field will not be used.
                CM_NONE,
                cast_coll_type,
                NULL);
            EXPR_GET_VARCHAR_V2(val, str_val);
            if (OB_FAIL(ret)) {
              LOG_WARN("get varchar value failed", K(ret), K(val));
              break;
            } else if (OB_FAIL(item.value_.assign(str_val))) {
              LOG_WARN("assign config value failed", K(ret), K(str_val));
              break;
            } else if (session_info_ != NULL && OB_FAIL(check_param_valid(session_info_->get_effective_tenant_id(),
                                                    ObString(item.name_.size(), item.name_.ptr()),
                                                    ObString(item.value_.size(), item.value_.ptr())))) {
              LOG_WARN("fail to check param valid", K(ret));
            } else if (NULL != action_node->children_[2]) {
              ObString comment(action_node->children_[2]->str_len_, action_node->children_[2]->str_value_);
              if (OB_FAIL(item.comment_.assign(comment))) {
                LOG_WARN("assign comment failed", K(comment), K(ret));
                break;
              }
            }

            // ignore config scope
            // server or zone
            if (NULL != action_node->children_[3]) {
              const ParseNode* n = action_node->children_[3];
              if (OB_FAIL(Util::resolve_server_or_zone(n, item))) {
                LOG_WARN("resolve server or zone failed", K(ret));
                break;
              }
            }  // if

            // tenant
            if (NULL != action_node->children_[4]) {
              const ParseNode* n = action_node->children_[4];
              if (T_TENANT_NAME == n->type_) {
                uint64_t tenant_id = item.exec_tenant_id_;
                if (OB_SYS_TENANT_ID != tenant_id) {
                  ret = OB_ERR_NO_PRIVILEGE;
                  LOG_WARN("non sys tenant", K(tenant_id), K(ret));
                  break;
                } else {
                  uint64_t tenant_node_id = OB_INVALID_ID;
                  ObString tenant_name(n->children_[0]->str_len_, n->children_[0]->str_value_);
                  ObString config_name(item.name_.size(), item.name_.ptr());
                  if (OB_FAIL(item.tenant_name_.assign(tenant_name))) {
                    LOG_WARN("assign tenant name failed", K(tenant_name), K(ret));
                    break;
                  } else if (0 == config_name.case_compare(TDE_MODE) ||
                             0 == config_name.case_compare(EXTERNAL_KMS_INFO)) {
                    if (OB_ISNULL(schema_checker_)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("schema checker is null", K(ret));
                    } else if (OB_FAIL(schema_checker_->get_tenant_id(tenant_name, tenant_node_id))) {
                      LOG_WARN("fail to get tenant id", K(ret));
                    } else if (OB_FAIL(check_param_valid(
                                   tenant_node_id, config_name, ObString(item.value_.size(), item.value_.ptr())))) {
                      LOG_WARN("fail to check param valid", K(ret));
                    }
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("resolve tenant name failed", K(ret));
                break;
              }
            }  // if

            if (OB_SUCC(ret)) {
              if (OB_FAIL(stmt->get_rpc_arg().items_.push_back(item))) {
                LOG_WARN("add config item failed", K(ret), K(item));
              } else if (0 == STRCMP(item.name_.ptr(), MINOR_FREEZE_TIMES) ||
                         0 == STRCMP(item.name_.ptr(), MAJOR_COMPACT_TRIGGER)) {
                if (has_major_compact_trigger) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "set minor_freeze_times and major_compact_trigger together");
                  LOG_WARN("minor_freeze_times and major_compact_trigger should not set together", K(ret));
                } else if (0 == STRCMP(item.name_.ptr(), MINOR_FREEZE_TIMES)) {
                  if (OB_FAIL(item.name_.assign(MAJOR_COMPACT_TRIGGER))) {
                    LOG_WARN("assign config name to major_compact_trigger failed", K(item), K(ret));
                  }
                } else if (OB_FAIL(item.name_.assign(MINOR_FREEZE_TIMES))) {
                  LOG_WARN("assign config name to minor_freeze_times failed", K(item), K(ret));
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(stmt->get_rpc_arg().items_.push_back(item))) {
                    LOG_WARN("add config item failed", K(ret), K(item));
                  } else {
                    has_major_compact_trigger = true;
                  }
                }
              } else if (0 == STRCMP(item.name_.ptr(), ENABLE_PERF_EVENT) ||
                         0 == STRCMP(item.name_.ptr(), ENABLE_SQL_AUDIT)) {
                if (has_perf_audit) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "set enable_perf_event and enable_sql_audit together");
                  LOG_WARN("enable_perf_event and enable_sql_audit should not set together", K(ret));
                } else if (0 == STRCMP(item.name_.ptr(), ENABLE_PERF_EVENT) &&
                           (0 == STRCASECMP(item.value_.ptr(), CONFIG_FALSE_VALUE_BOOL) ||
                               0 == STRCASECMP(item.value_.ptr(), CONFIG_FALSE_VALUE_STRING))) {
                  if (GCONF.enable_sql_audit) {
                    ret = OB_NOT_SUPPORTED;
                    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set enable_perf_event to false when enable_sql_audit is true");
                    LOG_WARN("enable_sql_audit cannot set true when enable_perf_event is false", K(ret));
                  } else if (OB_FAIL(item.name_.assign(ENABLE_SQL_AUDIT))) {
                    LOG_WARN("assign config name to enable_sql_audit failed", K(item), K(ret));
                  } else if (OB_FAIL(stmt->get_rpc_arg().items_.push_back(item))) {
                    LOG_WARN("add config item failed", K(ret), K(item));
                  }
                } else if (0 == STRCMP(item.name_.ptr(), ENABLE_SQL_AUDIT) &&
                           (0 == STRCASECMP(item.value_.ptr(), CONFIG_TRUE_VALUE_BOOL) ||
                               0 == STRCASECMP(item.value_.ptr(), CONFIG_TRUE_VALUE_STRING)) &&
                           !GCONF.enable_perf_event) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "set enable_sql_audit to true when enable_perf_event is false");
                  LOG_WARN("enable_sql_audit cannot set true when enable_perf_event is false", K(ret));
                }
                if (OB_SUCC(ret)) {
                  has_perf_audit = true;
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_DEST)) {
                if (!session_info_->is_inner() && OB_FAIL(check_backup_dest(item.value_.str()))) {
                  LOG_WARN("failed to check backup dest", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_BACKUP_DEST)) {
                if (!session_info_->is_inner() && OB_FAIL(check_backup_backup_dest(item.value_.str()))) {
                  LOG_WARN("failed to check backup backup dest", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_DEST_OPT)) {
                if (OB_FAIL(check_backup_dest_opt(false /*is backup backup*/, item.value_.str()))) {
                  LOG_WARN("failed to check backup dest opt", K(ret), K(item));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_BACKUP_DEST_OPT)) {
                if (OB_FAIL(check_backup_dest_opt(true /*is backup backup*/, item.value_.str()))) {
                  LOG_WARN("failed to check backup backup dest opt", K(ret), K(item));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_ENABLE_LOG_ARCHIVE)) {
                if (OB_FAIL(check_enable_log_archive(item.value_.str()))) {
                  LOG_WARN("cannot set enable log archive true", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), CLUSTER_ID)) {
                ret = OB_OP_NOT_ALLOW;
                LOG_WARN("cluster_id is not allowed to modify");
                LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter the parameter cluster_id");
              } else if (0 == STRCMP(item.name_.ptr(), Ob_STR_BACKUP_REGION)) {
                if (OB_FAIL(check_backup_region(item.value_.str()))) {
                  LOG_WARN("failed to check backup dest", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_ZONE)) {
                if (OB_FAIL(check_backup_zone(item.value_.str()))) {
                  LOG_WARN("failed to check backup dest", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_AUTO_DELETE_EXPIRED_BACKUP)) {
                if (OB_FAIL(check_auto_delete_expired_backup(item.value_.str()))) {
                  LOG_WARN("cannot set enable log archive true", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), OB_STR_AUTO_UPDATE_RESERVED_BACKUP_TIMESTAMP)) {
                if (OB_FAIL(check_auto_update_reserved_backup_timestamp(item.value_.str()))) {
                  LOG_WARN("cannot set enable log archive true", K(ret));
                }
              } else if (0 == STRCMP(item.name_.ptr(), QUERY_RESPPONSE_TIME_FLUSH)) {
                if(OB_FAIL(observer::ObRSTCollector::get_instance().flush_query_response_time(item.exec_tenant_id_, item.value_.str()))){
                  LOG_WARN("set query response time flush", K(ret));
                }  
              } else if (0 == STRCMP(item.name_.ptr(), QUERY_RESPPONSE_TIME_STATS)) {
                if(OB_FAIL(observer::ObRSTCollector::get_instance().control_query_response_time(item.exec_tenant_id_, item.value_.str()))){
                  LOG_WARN("set query response time stats", K(ret));
                }  
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSetConfigResolver::check_param_valid(int64_t tenant_id, const ObString& name, const ObString& value)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_SYS_TENANT_ID) {
    if (0 == name.case_compare(SSL_EXTERNAL_KMS_INFO)) {
      share::ObSSLClient client;
      if (OB_FAIL(client.init(value.ptr(), value.length()))) {
        OB_LOG(WARN, "ssl client init", K(ret), K(value));
      } else if (OB_FAIL(client.check_param_valid())) {
        OB_LOG(WARN, "ssl client param is not valid", K(ret));
      }
    } else if (0 == name.case_compare("ssl_client_authentication") &&
               (0 == value.case_compare("1") || 0 == value.case_compare("ON") || 0 == value.case_compare("TRUE"))) {
      ObString ssl_config(GCONF.ssl_external_kms_info.str());
      if (!ssl_config.empty()) {
        share::ObSSLClient client;
        if (OB_FAIL(client.init(ssl_config.ptr(), ssl_config.length()))) {
          OB_LOG(WARN, "kms client init", K(ret), K(ssl_config));
        } else if (OB_FAIL(client.check_param_valid())) {
          OB_LOG(WARN, "kms client param is not valid", K(ret));
        }
      } else {
        if (EASY_OK != easy_ssl_ob_config_check(OB_SSL_CA_FILE, OB_SSL_CERT_FILE, OB_SSL_KEY_FILE, true, false)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("key and cert not match", K(ret));
          LOG_USER_ERROR(OB_INVALID_CONFIG, "key and cert not match");
        }
      }
    } else if (0 == name.case_compare("_ob_ssl_invited_nodes")) {
      storage::ObPartitionService::get_instance().get_locality_manager()->set_ssl_invited_nodes(value);
#ifdef ERRSIM
      if (OB_SUCCESS != (ret = E(EventTable::EN_SSL_INVITE_NODES_FAILED) OB_SUCCESS)) {
        LOG_WARN("ERRSIM, fail to set ssl invite node", K(ret));
      }
#endif
    }
    else if (OB_FAIL(OB_STORE_FILE.validate_datafile_param(name, value.ptr()))) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("datafile_size is not valid", K(ret));
    }
  }
  return ret;
}

int ObSetTPResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_SETTP != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_SYSTEM", "type", get_type_name(parse_tree.type_));
  } else {
    if (OB_UNLIKELY(NULL == parse_tree.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children should not be null");
    } else {
      const ParseNode* list_node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == list_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list_node should not be null");
      } else {
        ObSetTPStmt* stmt = create_stmt<ObSetTPStmt>();
        if (OB_UNLIKELY(NULL == stmt)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("create stmt failed");
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < list_node->num_child_; ++i) {
            if (OB_UNLIKELY(NULL == list_node->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children should not be null");
              break;
            }

            const ParseNode* action_node = list_node->children_[i];
            if (OB_ISNULL(action_node) || OB_ISNULL(action_node->children_) || OB_ISNULL(action_node->children_[0])) {
              continue;
            }

            const ParseNode* value = action_node->children_[0];
            switch (action_node->type_) {
              case T_TP_NO: {  // event no
                stmt->get_rpc_arg().event_no_ = value->value_;
              } break;
              case T_TP_NAME: {
                stmt->get_rpc_arg().event_name_.assign_ptr(
                    value->str_value_, static_cast<ObString::obstr_size_t>(value->str_len_));
                break;
              }
              case T_OCCUR: {  // occurrence
                if (value->value_ > 0) {
                  stmt->get_rpc_arg().occur_ = value->value_;
                } else {
                  ret = OB_INVALID_ARGUMENT;
                }
              } break;
              case T_TRIGGER_MODE: {  // trigger frequency
                if (T_INT == value->type_) {
                  if (value->value_ < 0) {
                    ret = OB_INVALID_ARGUMENT;
                    SQL_RESV_LOG(WARN, "invalid argument", K(value->value_));
                  } else {
                    stmt->get_rpc_arg().trigger_freq_ = value->value_;
                  }
                }
              } break;
              case T_ERROR_CODE: {  // error code
                if (value->value_ > 0) {
                  stmt->get_rpc_arg().error_code_ = -value->value_;
                } else {
                  stmt->get_rpc_arg().error_code_ = value->value_;
                }
              } break;
              default:
                break;
            }
          }
          if (OB_SUCC(ret) && (2 <= parse_tree.num_child_) && (NULL != parse_tree.children_[1])) {
            if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[1], stmt->get_rpc_arg()))) {
              LOG_WARN("failed to resolve_server_or_zone", K(ret));
            }
          }
        }
        LOG_INFO("set tp", K(stmt->get_rpc_arg()));
      }
    }
  }

  return ret;
}

int ObClearLocationCacheResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CLEAR_LOCATION_CACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_LOCATION_CACHE", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearLocationCacheStmt* stmt = create_stmt<ObClearLocationCacheStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearLocationCacheStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dhildren should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObReloadGtsResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_GTS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_GTS", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadGtsStmt* stmt = create_stmt<ObReloadGtsStmt>();
    if (nullptr == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadGtsStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObReloadUnitResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_UNIT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_UNIT", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadUnitStmt* stmt = create_stmt<ObReloadUnitStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadUnitStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObReloadServerResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_SERVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_SERVER", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadServerStmt* stmt = create_stmt<ObReloadServerStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadServerStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObReloadZoneResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_ZONE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_ZONE", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadZoneStmt* stmt = create_stmt<ObReloadZoneStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadZoneStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObClearMergeErrorResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CLEAR_MERGE_ERROR != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_MERGE_ERROR", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearMergeErrorStmt* stmt = create_stmt<ObClearMergeErrorStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearMergeErrorStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObMigrateUnitResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_MIGRATE_UNIT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MIGRATE_UNIT", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObMigrateUnitStmt* stmt = create_stmt<ObMigrateUnitStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObMigrateUnitStmt failed");
    } else {
      stmt_ = stmt;
      ParseNode* node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node should not be null");
      } else {
        stmt->get_rpc_arg().unit_id_ = node->value_;
        if (NULL == parse_tree.children_[1]) {
          stmt->get_rpc_arg().is_cancel_ = true;
        } else if (OB_FAIL(Util::resolve_server_value(parse_tree.children_[1], stmt->get_rpc_arg().destination_))) {
          LOG_WARN("resolve server failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpgradeVirtualSchemaResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_UPGRADE_VIRTUAL_SCHEMA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_UPGRADE_VIRTUAL_SCHEMA", "type", get_type_name(parse_tree.type_));
  } else {
    ObUpgradeVirtualSchemaStmt* stmt = create_stmt<ObUpgradeVirtualSchemaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObUpgradeVirtualSchemaStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObAdminUpgradeCmdResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_UPGRADE_CMD != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_UPGRADE_CMD", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminUpgradeCmdStmt* stmt = create_stmt<ObAdminUpgradeCmdStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminUpgradeCmdStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_ISNULL(parse_tree.children_) || 1 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be NULL and number of children should be one", K(ret));
      } else {
        ParseNode* op = parse_tree.children_[0];
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmd should not be NULL", K(ret));
        } else if (OB_UNLIKELY(T_INT != op->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", K(ret), "type", get_type_name(op->type_));
        } else {
          stmt->set_op(static_cast<ObAdminUpgradeCmdStmt::AdminUpgradeOp>(op->value_));
        }
      }
    }
  }
  return ret;
}

int ObAdminRollingUpgradeCmdResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_ROLLING_UPGRADE_CMD != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ROLLING_UPGRADE_CMD", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminRollingUpgradeCmdStmt* stmt = create_stmt<ObAdminRollingUpgradeCmdStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminRollingUpgradeCmdStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_ISNULL(parse_tree.children_) || 1 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be NULL and number of children should be one", K(ret));
      } else {
        ParseNode* op = parse_tree.children_[0];
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmd should not be NULL", K(ret));
        } else if (OB_UNLIKELY(T_INT != op->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", K(ret), "type", get_type_name(op->type_));
        } else {
          stmt->set_op(static_cast<ObAdminRollingUpgradeCmdStmt::AdminUpgradeOp>(op->value_));
        }
      }
    }
  }
  return ret;
}

int ObRestoreTenantResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RESTORE_TENANT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RESTORE_TENANT", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObRestoreTenantStmt* stmt = create_stmt<ObRestoreTenantStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRestoreTenantStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_FAIL(Util::resolve_tenant(parse_tree.children_[0], stmt->get_rpc_arg().tenant_name_))) {
        LOG_WARN("resolve tenant_name failed", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], stmt->get_rpc_arg().oss_uri_))) {
        LOG_WARN("resolve string failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTenantResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_PHYSICAL_RESTORE_TENANT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_PHYSICAL_RESTORE_TENANT", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObPhysicalRestoreTenantStmt* stmt = create_stmt<ObPhysicalRestoreTenantStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObPhysicalRestoreTenantStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], stmt->get_rpc_arg().tenant_name_))) {
        LOG_WARN("resolve tenant_name failed", K(ret));
      } else if (OB_FAIL(
                     Util::resolve_relation_name(parse_tree.children_[1], stmt->get_rpc_arg().backup_tenant_name_))) {
        LOG_WARN("resolve tenant_name failed", K(ret));
      } else {
        if (OB_NOT_NULL(parse_tree.children_[2])) {
          if (session_info_->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid sql syntax", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "should not have backup_dest and restore_source at the same time");
          } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[2], stmt->get_rpc_arg().uri_))) {
            LOG_WARN("resolve string failed", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[4], stmt->get_rpc_arg().restore_option_))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(resolve_decryption_passwd(stmt->get_rpc_arg()))) {
        LOG_WARN("failed to resolve decrytion passwd", K(ret));
      } else if (OB_FAIL(resolve_restore_source_array(stmt->get_rpc_arg()))) {
        LOG_WARN("failed to resolve restore source array", K(ret));
      } else {
        // resolve datetime
        int64_t time_val = 0;
        ObString time_str;
        ObTimeConvertCtx ctx(TZ_INFO(session_info_), true);
        if (OB_FAIL(Util::resolve_string(parse_tree.children_[3], time_str))) {
          LOG_WARN("resolve string failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::str_to_datetime(time_str, ctx, time_val))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIMESTAMP", to_cstring(time_str));
        } else {
          stmt->get_rpc_arg().restore_timestamp_ = time_val;
        }
      }

      if (OB_SUCC(ret)) {
        if (6 == parse_tree.num_child_) {  // resolve table_list
          const ParseNode* node = parse_tree.children_[5];
          if (OB_ISNULL(node)) {
            stmt->set_is_preview(false);
          } else {
            if (T_TABLE_LIST == node->type_) {
              // store database_name/table_name with case sensitive.
              // compare database_name/table_name with tenant's name_case_mode.
              share::CompatModeGuard g(ObWorker::CompatMode::ORACLE);
              for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
                const ParseNode* table_node = node->children_[i];
                if (OB_ISNULL(table_node)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("table_node is null", KR(ret));
                } else {
                  ObString table_name;
                  ObString database_name;
                  obrpc::ObTableItem table_item;
                  if (OB_FAIL(resolve_table_relation_node(table_node, table_name, database_name))) {
                    LOG_WARN("failed to resolve table name", KR(ret), K(table_item));
                  } else {
                    table_item.table_name_ = table_name;
                    table_item.database_name_ = database_name;
                    if (OB_FAIL(stmt->get_rpc_arg().add_table_item(table_item))) {
                      LOG_WARN("failed to add table item", KR(ret), K(table_item));
                    }
                  }
                }
              }
            } else if (T_PREVIEW == node->type_) {
              stmt->set_is_preview(true);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node type is not right", K(ret), K(node));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTenantResolver::resolve_decryption_passwd(ObPhysicalRestoreTenantArg& arg)
{
  int ret = OB_SUCCESS;
  ObObj value;

  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (!session_info_->user_variable_exists(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR)) {
    LOG_INFO("no decryption passwd is specified");
    arg.passwd_array_.reset();
  } else if (OB_FAIL(session_info_->get_user_variable_value(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR, value))) {
    LOG_WARN("fail to get user variable", K(ret));
  } else {
    arg.passwd_array_ = value.get_varchar();
    LOG_INFO("succeed to resolve_decryption_passwd", "passwd", arg.passwd_array_);
  }

  return ret;
}

int ObPhysicalRestoreTenantResolver::resolve_restore_source_array(obrpc::ObPhysicalRestoreTenantArg& arg)
{
  int ret = OB_SUCCESS;
  ObObj value;

  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (!session_info_->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
    LOG_INFO("no restore source is specified");
    arg.multi_uri_.reset();
  } else if (OB_FAIL(session_info_->get_user_variable_value(OB_RESTORE_SOURCE_NAME_SESSION_STR, value))) {
    LOG_WARN("failed to get user variable", KR(ret));
  } else {
    arg.multi_uri_ = value.get_varchar();
    LOG_INFO("succeed to resolve_restore_source_array", "multi_uri", arg.multi_uri_);
  }
  return ret;
}

int ObRunJobResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RUN_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RUN_JOB", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObRunJobStmt* stmt = create_stmt<ObRunJobStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRunJobStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], stmt->get_rpc_arg().job_))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[1], stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRunUpgradeJobResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_RUN_UPGRADE_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_RUN_UPGRADE_JOB", KR(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else {
    ObRunUpgradeJobStmt* stmt = create_stmt<ObRunUpgradeJobStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRunUpgradeJobStmt failed", KR(ret));
    } else {
      stmt_ = stmt;
      ObString version_str;
      uint64_t version = OB_INVALID_VERSION;
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], version_str))) {
        LOG_WARN("resolve string failed", KR(ret));
      } else if (OB_FAIL(ObClusterVersion::get_version(version_str, version))) {
        LOG_WARN("fail to get version", KR(ret), K(version_str));
      } else {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::RUN_UPGRADE_JOB;
        stmt->get_rpc_arg().version_ = static_cast<int64_t>(version);
      }
    }
  }
  return ret;
}

int ObStopUpgradeJobResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_STOP_UPGRADE_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_STOP_UPGRADE_JOB", KR(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else {
    ObStopUpgradeJobStmt* stmt = create_stmt<ObStopUpgradeJobStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObStopUpgradeJobStmt failed", KR(ret));
    } else {
      stmt_ = stmt;
      stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::STOP_UPGRADE_JOB;
    }
  }
  return ret;
}

int ObSwitchRSRoleResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SWITCH_RS_ROLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SWITCH_RS_ROLE", K(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("childern should noto be null", K(ret));
  } else {
    ObSwitchRSRoleStmt* stmt = create_stmt<ObSwitchRSRoleStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSwitchRSRoleStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      ObAdminSwitchRSRoleArg& rpc_arg = stmt->get_rpc_arg();
      ParseNode* role = parse_tree.children_[0];
      if (OB_ISNULL(role)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("role should not be null", K(ret));
      } else if (OB_UNLIKELY(T_INT != role->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is not T_INT", K(ret), "type", get_type_name(role->type_));
      } else {
        switch (role->value_) {
          case 0: {
            rpc_arg.role_ = LEADER;
            break;
          }
          case 1: {
            rpc_arg.role_ = FOLLOWER;
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected role value", K(ret), "value", role->value_);
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ParseNode* node = parse_tree.children_[1];
        if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null", K(ret));
        } else {
          switch (node->type_) {
            case T_IP_PORT: {
              if (OB_FAIL(Util::resolve_server(node, rpc_arg.server_))) {
                LOG_WARN("resolve server failed", K(ret));
              }
              break;
            }
            case T_ZONE: {
              if (OB_FAIL(Util::resolve_zone(node, rpc_arg.zone_))) {
                LOG_WARN("resolve zone failed", K(ret));
              }
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected type", K(ret), "type", get_type_name(node->type_));
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRefreshTimeZoneInfoResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_TIME_ZONE_INFO != parse_tree.type_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_TIME_ZONE_INFO", "type", get_type_name(parse_tree.type_), K(session_info_));
  } else {
    ObRefreshTimeZoneInfoStmt* refresh_time_zone_info_stmt = create_stmt<ObRefreshTimeZoneInfoStmt>();
    if (OB_UNLIKELY(NULL == refresh_time_zone_info_stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRefreshTimeZoneInfoStmt failed", K(ret));
    } else {
      refresh_time_zone_info_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
      stmt_ = refresh_time_zone_info_stmt;
    }
  }
  return ret;
}

//
//                           /- T_INT(priority)
//                          /|
//  T_ENABLE_SQL_THROTTLE -<
//                          \|
//                           \- T_SQL_THROTTLE_METRICS -< [ T_RT -> (decimal)
//                                                        | T_CPU -> (decimal)
//                                                        | T_IO -> (int)
//                                                        | T_NETWORK -> (decimal)
//                                                        | T_LOGICAL_READS -> (int)
//                                                        ]+
//
int ObEnableSqlThrottleResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObEnableSqlThrottleStmt* stmt = nullptr;

  if (OB_UNLIKELY(T_ENABLE_SQL_THROTTLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ENABLE_SQL_THROTTLE", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree's children is null", K(ret));
  } else if (2 != parse_tree.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree's number of children doesn't match", K(ret));
  } else {
    stmt = create_stmt<ObEnableSqlThrottleStmt>();
    if (nullptr == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObEnableSqlThrottleStmt failed");
    }
  }
  if (OB_SUCC(ret)) {
    ParseNode* priority_node = parse_tree.children_[0];
    if (nullptr != priority_node) {
      stmt->set_priority(priority_node->value_);
    }
    ParseNode* metrics_node = parse_tree.children_[1];
    if (metrics_node != nullptr) {
      for (int i = 0; i < metrics_node->num_child_; i++) {
        ParseNode* node = metrics_node->children_[i];
        ParseNode* valNode = node->children_[0];
        switch (node->type_) {
          case T_RT:
            if (valNode->type_ == T_INT) {
              stmt->set_rt(static_cast<double>(valNode->value_));
            } else if (valNode->type_ == T_NUMBER) {
              stmt->set_rt(atof(valNode->str_value_));
            }
            break;
          case T_CPU:
            if (valNode->type_ == T_INT) {
              stmt->set_cpu(static_cast<double>(valNode->value_));
            } else if (valNode->type_ == T_NUMBER) {
              stmt->set_cpu(atof(valNode->str_value_));
            }
            break;
          case T_IO:
            stmt->set_io(valNode->value_);
            break;
          case T_NETWORK:
            if (valNode->type_ == T_INT) {
              stmt->set_network(static_cast<double>(valNode->value_));
            } else if (valNode->type_ == T_NUMBER) {
              stmt->set_network(atof(valNode->str_value_));
            }
            break;
          case T_LOGICAL_READS:
            stmt->set_logical_reads(valNode->value_);
            break;
          case T_QUEUE_TIME:
            if (valNode->type_ == T_INT) {
              stmt->set_queue_time(static_cast<double>(valNode->value_));
            } else if (valNode->type_ == T_NUMBER) {
              stmt->set_queue_time(atof(valNode->str_value_));
            }
            break;
          default:
            break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  return ret;
}

int ObDisableSqlThrottleResolver::resolve(const ParseNode& parse_tree)
{
  UNUSED(parse_tree);
  int ret = OB_SUCCESS;
  stmt_ = create_stmt<ObDisableSqlThrottleStmt>();
  return ret;
}

int ObCancelTaskResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CANCEL_TASK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CANCEL_TASK", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree's children is null", K(ret));
  } else {
    ObCancelTaskStmt* cancel_task = create_stmt<ObCancelTaskStmt>();
    if (NULL == cancel_task) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObCancelTaskStmt failed");
    } else {
      stmt_ = cancel_task;
      ParseNode* cancel_type_node = parse_tree.children_[0];
      ParseNode* task_id = parse_tree.children_[1];
      share::ObSysTaskType task_type = MAX_SYS_TASK_TYPE;
      ObString task_id_str;
      if (OB_ISNULL(task_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_id node is null", K(ret));
      } else if (OB_FAIL(Util::resolve_string(task_id, task_id_str))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (NULL == cancel_type_node) {
        task_type = MAX_SYS_TASK_TYPE;
      } else {
        switch (cancel_type_node->value_) {
          case 1: {
            task_type = GROUP_PARTITION_MIGRATION_TASK;
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cancel task type", K(ret), "value", cancel_type_node->value_);
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(cancel_task->set_param(task_type, task_id_str))) {
          LOG_WARN("failed to set cancel task param", K(ret), K(task_type), K(task_id_str));
        }
      }
    }
  }
  return ret;
}

int ObSetDiskValidResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SET_DISK_VALID != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SET_DISK_VALID", "type", get_type_name(parse_tree.type_));
  } else {
    ObSetDiskValidStmt* stmt = create_stmt<ObSetDiskValidStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSetDiskValidStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        if (OB_FAIL(Util::resolve_server(parse_tree.children_[0], stmt->server_))) {
          LOG_WARN("resolve server failed", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObAlterDiskgroupAddDiskResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_DISKGROUP_ADD_DISK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_DISKGROUP_ADD_DISK", "type", get_type_name(parse_tree.type_));
  } else {
    ObAddDiskStmt* stmt = create_stmt<ObAddDiskStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAddDiskStmt failed");
    } else {
      stmt_ = stmt;
      ObAdminAddDiskArg& arg = stmt->arg_;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], arg.diskgroup_name_))) {
        LOG_WARN("failed to resolve diskgroup_name", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], arg.disk_path_))) {
        LOG_WARN("failed to resolve disk_path", K(ret));
      } else if (NULL != parse_tree.children_[2] &&
                 OB_FAIL(Util::resolve_string(parse_tree.children_[2], arg.alias_name_))) {
        LOG_WARN("failed to resolve alias name", K(ret));
      } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[3], arg.server_))) {
        LOG_WARN("failed to resolve server", K(ret));
      } else if (NULL != parse_tree.children_[4] && OB_FAIL(Util::resolve_zone(parse_tree.children_[4], arg.zone_))) {
        LOG_WARN("failed to resolve zone", K(ret));
      } else {
        LOG_INFO("succeed to resolve add disk arg", K(arg));
      }
    }
  }
  return ret;
}

int ObAlterDiskgroupDropDiskResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_DISKGROUP_DROP_DISK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_DISKGROUP_DROP_DISK", "type", get_type_name(parse_tree.type_));
  } else {
    ObDropDiskStmt* stmt = create_stmt<ObDropDiskStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropDiskStmt failed");
    } else {
      stmt_ = stmt;
      ObAdminDropDiskArg& arg = stmt->arg_;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], arg.diskgroup_name_))) {
        LOG_WARN("failed to resolve diskgroup_name", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], arg.alias_name_))) {
        LOG_WARN("failed to resolve alias name", K(ret));
      } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[2], arg.server_))) {
        LOG_WARN("failed to resolve server", K(ret));
      } else if (NULL != parse_tree.children_[3] && OB_FAIL(Util::resolve_zone(parse_tree.children_[3], arg.zone_))) {
        LOG_WARN("failed to resolve zone", K(ret));
      } else {
        LOG_INFO("succeed to resolve drop disk arg", K(arg));
      }
    }
  }
  return ret;
}

int ObClearBalanceTaskResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* tenants_node = NULL;
  ParseNode* zone_node = NULL;
  ParseNode* task_type_node = NULL;
  ObClearBalanceTaskStmt* clear_task_stmt = NULL;
  if (OB_UNLIKELY(T_CLEAR_BALANCE_TASK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_BALANCE_TASK", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree's children is null", K(ret));
  } else {
    clear_task_stmt = create_stmt<ObClearBalanceTaskStmt>();
    if (NULL == clear_task_stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearBalanceTaskStmt failed");
    } else {
      stmt_ = clear_task_stmt;
      tenants_node = parse_tree.children_[0];
      zone_node = parse_tree.children_[1];
      task_type_node = parse_tree.children_[2];
    }
  }
  ObSchemaGetterGuard schema_guard;
  if (OB_SUCC(ret) && NULL != tenants_node) {
    if (T_TENANT_LIST != tenants_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tenant_name node", K(ret));
    } else if (OB_UNLIKELY(NULL == tenants_node->children_) || OB_UNLIKELY(0 == tenants_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of tenant should not be null");
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is empty");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   session_info_->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("get_schema_guard failed");
    } else {
      common::ObIArray<uint64_t>& tenant_ids = clear_task_stmt->get_tenant_ids();
      uint64_t tenant_id = 0;
      ObString tenant_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenants_node->num_child_; ++i) {
        ParseNode* node = tenants_node->children_[i];
        if (OB_ISNULL(node)) {
          LOG_WARN("children of server_list should not be null");
        } else {
          tenant_name.assign_ptr(node->str_value_, static_cast<ObString::obstr_size_t>(node->str_len_));
          if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
            LOG_WARN("tenant not exist", K(tenant_name));
          } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push tenant id ", K(tenant_name), K(tenant_id));
          }
        }
        tenant_name.reset();
      }  // end for
    }
  }  // end if (NULL != tenants_node)

  if (OB_SUCC(ret) && NULL != zone_node) {
    if (T_ZONE_LIST != zone_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid zone_name node", K(ret));
    } else if (OB_UNLIKELY(NULL == zone_node->children_) || OB_UNLIKELY(0 == zone_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of zone should not be null");
    } else {
      common::ObIArray<ObZone>& zone_names = clear_task_stmt->get_zone_names();
      ObZone zone_name;
      ObString parse_zone_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_node->num_child_; ++i) {
        ParseNode* node = zone_node->children_[i];
        if (OB_ISNULL(node)) {
          LOG_WARN("children of server_list should not be null");
        } else {
          parse_zone_name.assign_ptr(node->str_value_, static_cast<ObString::obstr_size_t>(node->str_len_));
          if (OB_FAIL(zone_name.assign(parse_zone_name))) {
            LOG_WARN("failed to assign zone name", K(ret), K(parse_zone_name));
          } else if (OB_FAIL(zone_names.push_back(zone_name))) {
            LOG_WARN("failed to push back zone name", K(ret), K(zone_name));
          }
        }
        parse_zone_name.reset();
        zone_name.reset();
      }  // end for
    }
  }  // end if (NULL != zone_node)

  if (OB_SUCC(ret)) {
    if (NULL == task_type_node) {
      clear_task_stmt->set_type(ObAdminClearBalanceTaskArg::ALL);
    } else if (T_BALANCE_TASK_TYPE != task_type_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid type node", K(ret));
    } else if (OB_UNLIKELY(NULL == task_type_node->children_) || OB_UNLIKELY(0 == task_type_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of node_type should not be null");
    } else {
      clear_task_stmt->set_type(
          static_cast<ObAdminClearBalanceTaskArg::TaskType>(task_type_node->children_[0]->value_));
    }
  }
  return ret;
}

int ObChangeTenantResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CHANGE_TENANT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CHANGE_TENANT", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(schema_checker_) || OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_checker or schema_guard is null", K(ret));
  } else {
    ObChangeTenantStmt* stmt = create_stmt<ObChangeTenantStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObChangeTenantStmt failed");
    } else {
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_UNLIKELY(T_TENANT_NAME != parse_tree.children_[0]->type_ &&
                             T_TENANT_ID != parse_tree.children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid parse_tree", K(ret));
      } else {
        if (T_TENANT_ID == parse_tree.children_[0]->type_) {
          uint64_t tenant_id = static_cast<uint64_t>(parse_tree.children_[0]->value_);
          bool is_exist = false;
          ObSchemaGetterGuard* schema_guard = schema_checker_->get_schema_guard();
          if (OB_FAIL(schema_guard->check_tenant_exist(tenant_id, is_exist))) {
            LOG_WARN("check tenant exist failed", K(ret), K(tenant_id));
          } else if (!is_exist) {
            ret = OB_TENANT_NOT_EXIST;
            LOG_WARN("tenant not exist", K(ret), K(tenant_id));
          } else {
            stmt->set_tenant_id(tenant_id);
            stmt_ = stmt;
          }
        } else if (T_TENANT_NAME == parse_tree.children_[0]->type_) {
          ObString tenant_name;
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          tenant_name.assign_ptr(
              (char*)(parse_tree.children_[0]->str_value_), static_cast<int32_t>(parse_tree.children_[0]->str_len_));
          if (tenant_name.length() >= OB_MAX_TENANT_NAME_LENGTH) {
            ret = OB_ERR_TOO_LONG_IDENT;
            LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, tenant_name.length(), tenant_name.ptr());
          } else if (OB_FAIL(schema_checker_->get_tenant_id(tenant_name, tenant_id))) {
            LOG_WARN("fail to get tenant_id", K(ret), K(tenant_name));
          } else {
            stmt->set_tenant_id(tenant_id);
            stmt_ = stmt;
          }
        }
      }
    }
  }
  return ret;
}

int ObDropTempTableResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_DROP_TEMP_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_SYSTEM_DROP_TEMP_TABLE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObDropTableStmt* stmt = create_stmt<ObDropTableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropTableStmt failed");
    } else {
      stmt_ = stmt;
      obrpc::ObDropTableArg& drop_table_arg = stmt->get_drop_table_arg();
      drop_table_arg.if_exist_ = true;
      drop_table_arg.to_recyclebin_ = false;
      drop_table_arg.tenant_id_ = session_info_->get_login_tenant_id();
      drop_table_arg.table_type_ = share::schema::TMP_TABLE_ALL;
      drop_table_arg.session_id_ = static_cast<uint64_t>(parse_tree.children_[0]->value_);
    }
  }
  return ret;
}

int ObRefreshTempTableResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_REFRESH_TEMP_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_SYSTEM_REFRESH_TEMP_TABLE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ should not be null");
  } else {
    ObAlterTableStmt* stmt = create_stmt<ObAlterTableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAlterTableStmt failed");
    } else if (OB_FAIL(stmt->set_tz_info_wrap(session_info_->get_tz_info_wrap()))) {
      LOG_WARN("failed to set_tz_info_wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(stmt->set_nls_formats(session_info_->get_local_nls_formats()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else {
      stmt_ = stmt;
      stmt->set_is_alter_system(true);
      obrpc::ObAlterTableArg& alter_table_arg = stmt->get_alter_table_arg();
      alter_table_arg.session_id_ = static_cast<uint64_t>(parse_tree.children_[0]->value_);
      alter_table_arg.alter_table_schema_.alter_type_ = OB_DDL_ALTER_TABLE;
      // compat for old server
      alter_table_arg.tz_info_ = session_info_->get_tz_info_wrap().get_tz_info_offset();
      if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(session_info_->get_tz_info_wrap()))) {
        LOG_WARN("failed to deep_copy tz info wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
      } else if (OB_FAIL(alter_table_arg.set_nls_formats(session_info_->get_local_nls_formats()))) {
        LOG_WARN("failed to set_nls_formats", K(ret));
      } else if (OB_FAIL(alter_table_arg.alter_table_schema_.alter_option_bitset_.add_member(
                     obrpc::ObAlterTableArg::SESSION_ACTIVE_TIME))) {
        LOG_WARN("failed to add member SESSION_ACTIVE_TIME for alter table schema", K(ret), K(alter_table_arg));
      }
    }
  }
  return ret;
}

// for oracle mode grammer: alter system set sys_var = val
int ObAlterSystemSetResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  bool set_parameters = false;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_SET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ must be T_ALTER_SYSTEM_SET", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session_info_ or allocator_ is NULL", K(ret), K(session_info_), K(allocator_));
  } else {
    tenant_id = session_info_->get_effective_tenant_id();
    /* first round: detect set variables or parameters */
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
      ParseNode *set_node = nullptr, *set_param_node = nullptr;
      if (OB_ISNULL(set_node = parse_tree.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node should not be null", K(ret));
      } else if (T_ALTER_SYSTEM_SET_PARAMETER != set_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER", K(ret), K(set_node->type_));
      } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node is null", K(ret));
      } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
      } else {
        ParseNode* var = nullptr;
        if (OB_ISNULL(var = set_param_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var is NULL", K(ret));
        } else if (T_IDENT != var->type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name isn't identifier type");
        } else {
          ObString name(var->str_len_, var->str_value_);
          omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
          if (tenant_config.is_valid() && nullptr != tenant_config->get_container().get(ObConfigStringKey(name))) {
            set_parameters = true;
            break;
          }
        }
      }
    }  // for
  }
  /* second round: gen stmt */
  if (OB_SUCC(ret)) {
    if (set_parameters) {
      ObSetConfigStmt* setconfig_stmt = create_stmt<ObSetConfigStmt>();
      if (OB_ISNULL(setconfig_stmt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("create set config stmt failed");
      } else {
        ObCreateTableResolver ddl_resolver(params_);
        for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
          ParseNode *set_node = nullptr, *set_param_node = nullptr;
          if (OB_ISNULL(set_node = parse_tree.children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node should not be null", K(ret));
          } else if (T_ALTER_SYSTEM_SET_PARAMETER != set_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER", K(ret), K(set_node->type_));
          } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node is null", K(ret));
          } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
          } else {
            ParseNode *name_node = nullptr, *value_node = nullptr;
            ObAdminSetConfigItem item;
            item.exec_tenant_id_ = tenant_id;
            /* name */
            if (OB_ISNULL(name_node = set_param_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("var is NULL", K(ret));
            } else if (T_IDENT != name_node->type_) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name isn't identifier type");
            } else {
              ObString name(name_node->str_len_, name_node->str_value_);
              ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
              if (OB_FAIL(item.name_.assign(name))) {
                LOG_WARN("assign config name failed", K(name), K(ret));
              }
            }
            if (OB_FAIL(ret)) {
              break;
            }
            /* value */
            if (OB_ISNULL(value_node = set_param_node->children_[1])) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("value node is NULL", K(ret));
            } else {
              ObObjParam val;
              if (OB_FAIL(ddl_resolver.resolve_default_value(value_node, val))) {
                LOG_WARN("resolve config value failed", K(ret));
                break;
              }
              ObString str_val;
              ObCollationType cast_coll_type = CS_TYPE_INVALID;
              if (OB_SUCCESS != session_info_->get_collation_connection(cast_coll_type)) {
                LOG_WARN("fail to get collation_connection");
                cast_coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
              }
              ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
              ObCastCtx cast_ctx(&allocator,
                  NULL,  // to varchar. this field will not be used.
                  0,     // to varchar. this field will not be used.
                  CM_NONE,
                  cast_coll_type,
                  NULL);
              EXPR_GET_VARCHAR_V2(val, str_val);
              if (OB_FAIL(ret)) {
                LOG_WARN("get varchar value failed", K(ret), K(val));
                break;
              } else if (OB_FAIL(item.value_.assign(str_val))) {
                LOG_WARN("assign config value failed", K(ret), K(str_val));
                break;
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(setconfig_stmt->get_rpc_arg().items_.push_back(item))) {
                LOG_WARN("add config item failed", K(ret), K(item));
              }
            }
          }
        }  // for
      }
    } else {
      ObVariableSetStmt* variable_set_stmt = create_stmt<ObVariableSetStmt>();
      if (OB_ISNULL(variable_set_stmt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("create variable set stmt failed", K(OB_ALLOCATE_MEMORY_FAILED));
      } else {
        variable_set_stmt->set_actual_tenant_id(session_info_->get_effective_tenant_id());
        ParseNode *set_node = nullptr, *set_param_node = NULL;
        ObVariableSetStmt::VariableSetNode var_node;
        for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
          if (OB_ISNULL(set_node = parse_tree.children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node should not be null", K(ret));
          } else if (T_ALTER_SYSTEM_SET_PARAMETER != set_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER", K(ret), K(set_node->type_));
          } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node is null", K(ret));
          } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
          } else {
            ParseNode* var = NULL;
            var_node.set_scope_ = ObSetVar::SET_SCOPE_GLOBAL;
            variable_set_stmt->set_has_global_variable(true);
            /* resolve var_name */
            if (OB_ISNULL(var = set_param_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("var is NULL", K(ret));
            } else {
              ObString var_name;
              if (T_IDENT != var->type_) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Variable name isn't identifier type");
              } else {
                var_node.is_system_variable_ = true;
                var_name.assign_ptr(var->str_value_, static_cast<int32_t>(var->str_len_));
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(ob_write_string(*allocator_, var_name, var_node.variable_name_))) {
                  LOG_WARN("Can not malloc space for variable name", K(ret));
                } else {
                  ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, var_node.variable_name_);
                }
              }
              /* resolve value */
              if (OB_SUCC(ret)) {
                if (OB_ISNULL(set_param_node->children_[1])) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("value node is NULL", K(ret));
                } else if (var_node.is_system_variable_) {
                  ParseNode value_node;
                  MEMCPY(&value_node, set_param_node->children_[1], sizeof(ParseNode));
                  if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, value_node, var_node.value_expr_, NULL))) {
                    LOG_WARN("resolve variable value failed", K(ret));
                  }
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(variable_set_stmt->add_variable_node(
                                  ObVariableSetStmt::make_variable_name_node(var_node)))) {
                LOG_WARN("Add set entry failed", K(ret));
              }
            }
          }  // end resolve variable and value
        }    // end for
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK(OB_NOT_NULL(schema_checker_));
      OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
             session_info_->get_priv_user_id(),
             ObString(""),
             stmt::T_ALTER_SYSTEM_SET_PARAMETER,
             session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(),
          session_info_->get_user_id());
    }

  }  // if

  return ret;
}

int ObArchiveLogResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ARCHIVE_LOG != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ARCHIVE_LOG", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObArchiveLogStmt* stmt = create_stmt<ObArchiveLogStmt>();
    const int64_t is_enable = parse_tree.children_[0]->value_;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObArchiveLogStmt failed");
    } else {
      stmt->set_is_enable(is_enable);
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupDatabaseResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_DATABASE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_DATABASE", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupDatabaseStmt* stmt = create_stmt<ObBackupDatabaseStmt>();
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t incremental = parse_tree.children_[0]->value_;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupDatabaseStmt failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, incremental))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(incremental));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObTableTTLResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_TABLE_TTL != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_TABLE_TTL", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObTableTTLStmt* stmt = create_stmt<ObTableTTLStmt>();
    const int64_t type = parse_tree.children_[0]->value_;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObArchiveLogStmt failed");
    } else if (OB_FAIL(stmt->set_param(type))) {
      LOG_WARN("Failed to set param", K(ret), K(type));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupManageResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_MANAGE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_MANAGE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_ && 2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupManageStmt* stmt = create_stmt<ObBackupManageStmt>();
    int64_t copy_id = 0;
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t type = parse_tree.children_[0]->value_;
    const int64_t value = parse_tree.children_[1]->value_;
    if (2 == parse_tree.num_child_) {
      // do nothing
    } else if (3 == parse_tree.num_child_) {
      copy_id = NULL == parse_tree.children_[2] ? 0 : parse_tree.children_[2]->children_[0]->value_;
    }
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupManageResolver failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, type, value, copy_id))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(type), K(value), K(copy_id));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupBackupsetResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_BACKUPSET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_BACKUPSET", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else if (4 != parse_tree.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupBackupsetStmt* stmt = create_stmt<ObBackupBackupsetStmt>();
    const int64_t backup_set_id = parse_tree.children_[0]->value_;
    const ObString backup_dest = OB_ISNULL(parse_tree.children_[2]) ? "" : parse_tree.children_[2]->str_value_;
    const int64_t max_backup_times = OB_ISNULL(parse_tree.children_[3]) ? -1 : parse_tree.children_[3]->value_;
    uint64_t tenant_id = OB_INVALID_ID;
    bool same_with_gconf = false;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupBackupsetStmt failed", KR(ret));
    } else if (OB_FAIL(Util::check_same_with_gconf(backup_dest, same_with_gconf))) {
      LOG_WARN("failed to check same with gconf", KR(ret), K(backup_dest));
    } else if (same_with_gconf && max_backup_times > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support not backed up N times sql with backup_backup_dest same with gconf");
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not backed up N times to the backup_backup_dest of sys parameter");
    } else if (OB_FAIL(Util::resolve_tenant_id(parse_tree.children_[1], tenant_id))) {
      LOG_WARN("failed to resolve tenant id", KR(ret));
    } else if (OB_FAIL(stmt->set_param(tenant_id, backup_set_id, max_backup_times, backup_dest))) {
      LOG_WARN("failed to set param", KR(ret), K(tenant_id), K(backup_set_id), K(max_backup_times));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupArchiveLogResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_ARCHIVELOG != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_ARCHIVELOG", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupArchiveLogStmt* stmt = create_stmt<ObBackupArchiveLogStmt>();
    const int64_t is_enable = parse_tree.children_[0]->value_;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupArchiveLogStmt failed");
    } else {
      stmt->set_is_enable(is_enable);
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupSetEncryptionResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObBackupSetEncryptionStmt* stmt = nullptr;

  if (OB_UNLIKELY(T_BACKUP_SET_ENCRYPTION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_SET_ENCRYPTION", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const int64_t mode = parse_tree.children_[0]->value_;
    const ObString passwd(parse_tree.children_[1]->str_len_, parse_tree.children_[1]->str_value_);
    const uint64_t tenant_id = session_info_->get_login_tenant_id();

    LOG_INFO("resolve set encryption", K(mode), K(passwd));

    if (tenant_id != OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only sys tenant can set encryption", K(ret), K(tenant_id));
    } else if (NULL == (stmt = create_stmt<ObBackupSetEncryptionStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupSetEncryptionStmt failed", K(ret));
    } else if (OB_FAIL(stmt->set_param(mode, passwd))) {
      LOG_WARN("Failed to set param", K(ret), K(mode), K(passwd), KP(session_info_));
    }
    stmt_ = stmt;
  }
  return ret;
}

int ObBackupSetDecryptionResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode* passwd_list = NULL;
  ObBackupSetDecryptionStmt* stmt = nullptr;

  if (OB_UNLIKELY(T_BACKUP_SET_DECRYPTION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_SET_DECRYPTION", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(passwd_list = parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("passwd_ist must not null", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (NULL == (stmt = create_stmt<ObBackupSetDecryptionStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObBackupSetEncryptionStmt failed", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    if (tenant_id != OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only sys tenant can set decryption", K(ret), K(tenant_id));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < passwd_list->num_child_; ++i) {
    ParseNode* node = passwd_list->children_[i];
    const ObString passwd(node->str_len_, node->str_value_);
    if (OB_FAIL(stmt->add_passwd(passwd))) {
      LOG_WARN("failed to add passwd", K(ret), K(i), K(passwd));
    }
  }

  stmt_ = stmt;
  return ret;
}

int ObBackupBackupPieceResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_BACKUPPIECE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_BACKUPPIECE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else if (7 != parse_tree.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupBackupPieceStmt* stmt = create_stmt<ObBackupBackupPieceStmt>();
    const int64_t piece_id = parse_tree.children_[0]->value_;
    const int64_t backup_all = parse_tree.children_[1]->value_;
    const int64_t max_times = parse_tree.children_[2]->value_;
    const bool with_active_piece = NULL == parse_tree.children_[3] ? false : (1 == parse_tree.children_[3]->value_);
    const ObString backup_dest = NULL == parse_tree.children_[5] ? "" : parse_tree.children_[5]->str_value_;
    const bool support_active_piece = 0 == parse_tree.children_[6]->value_;
    uint64_t tenant_id = OB_INVALID_ID;
    bool same_with_gconf = false;
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupBackupPieceStmt failed", KR(ret));
    } else if (OB_FAIL(Util::check_same_with_gconf(backup_dest, same_with_gconf))) {
      LOG_WARN("failed to check same with gconf", KR(ret), K(backup_dest));
    } else if (same_with_gconf && max_times > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support not backed up N times sql with backup_backup_dest same with gconf");
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not backed up N times to the backup_backup_dest of sys parameter");
    } else if (OB_FAIL(Util::resolve_tenant_id(parse_tree.children_[4], tenant_id))) {
      LOG_WARN("failed to resolve tenant id", KR(ret));
    } else if (!support_active_piece && with_active_piece) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cluster level backup backup do not support backup active piece",
          KR(ret),
          K(tenant_id),
          K(piece_id),
          K(max_times),
          K(with_active_piece),
          K(parse_tree.children_[5]->value_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cluster level backup backup active piece");
    } else if (OB_FAIL(stmt->set_param(tenant_id, piece_id, max_times, backup_all, with_active_piece, backup_dest))) {
      LOG_WARN("failed to set param", KR(ret), K(tenant_id), K(piece_id), K(max_times), K(backup_all));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObAddRestoreSourceResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObAddRestoreSourceStmt* stmt = NULL;

  if (OB_UNLIKELY(T_ADD_RESTORE_SOURCE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADD_RESTORE_SOURCE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else if (1 != parse_tree.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const ObString source(parse_tree.children_[0]->str_len_, parse_tree.children_[0]->str_value_);
    if (tenant_id != OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only sys tenant can set add restore source", K(ret), K(tenant_id));
    } else if (OB_ISNULL(stmt = create_stmt<ObAddRestoreSourceStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create ObAddRestoreSourceStmt", K(ret));
    } else if (OB_FAIL(stmt->add_restore_source(source))) {
      LOG_WARN("Failed to set param", K(ret), K(source));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObClearRestoreSourceResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObClearRestoreSourceStmt* stmt = NULL;
  if (OB_UNLIKELY(T_CLEAR_RESTORE_SOURCE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_RESTORE_SOURCE", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObClearRestoreSourceStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObClearRestoreSource failed", KR(ret));
  } else {
    stmt_ = stmt;
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
