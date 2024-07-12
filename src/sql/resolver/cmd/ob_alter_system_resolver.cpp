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
#include "share/ob_encryption_util.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_encrypt_kms.h"
#endif
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_zone_table_operation.h"
#include "share/backup/ob_backup_struct.h"
#include "share/restore/ob_import_table_struct.h"
#include "share/restore/ob_recover_table_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "sql/resolver/cmd/ob_clear_balance_task_stmt.h"
#include "sql/resolver/cmd/ob_switch_tenant_resolver.h"
#include "sql/resolver/ddl/ob_create_table_resolver.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "observer/ob_server.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_config.h"
#include "observer/mysql/ob_query_response_time.h"
#include "rootserver/ob_rs_job_table_operator.h"  //ObRsJobType
#include "sql/resolver/cmd/ob_kill_stmt.h"
#include "share/table/ob_table_config_util.h"
#include "share/restore/ob_import_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql
{
typedef ObAlterSystemResolverUtil Util;

int ObAlterSystemResolverUtil::sanity_check(const ParseNode *parse_tree, ObItemType item_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree should not be null");
  } else if (OB_UNLIKELY(item_type != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type",
             "expect", get_type_name(item_type),
             "actual", get_type_name(parse_tree->type_));
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
int ObAlterSystemResolverUtil::resolve_server_or_zone(const ParseNode *parse_tree, RPC_ARG &arg)
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

int ObAlterSystemResolverUtil::resolve_server_value(const ParseNode *parse_tree, ObAddr &server)
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

int ObAlterSystemResolverUtil::resolve_replica_type(const ParseNode *parse_tree,
                                                    ObReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    replica_type = REPLICA_TYPE_FULL; // 为了兼容早期命令，不指定replica_type时默认为FULL类型
    LOG_INFO("resolve_replica_type without any value. default to FULL.");
  } else if (OB_UNLIKELY(T_VARCHAR != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_VARCHAR", "type", get_type_name(parse_tree->type_));
  } else {
    int64_t len = parse_tree->str_len_;
    const char *str = parse_tree->str_value_;
    if (OB_FAIL(ObLocalityParser::parse_type(str, len, replica_type))) {
      // do nothing, error log will print inside parse_type
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_memstore_percent(const ParseNode *parse_tree,
                                                        ObReplicaProperty &replica_property)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == parse_tree)) {
    replica_property.reset();
    LOG_INFO("resolve_memstore_percent without any value. default to 100");
  } else if (OB_UNLIKELY(T_INT != parse_tree->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_INT", "type", get_type_name(parse_tree->type_));
  } else {
    const ParseNode *node = parse_tree;
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else {
      int64_t temp_value = node->value_;
      if(temp_value > 100 || temp_value < 0) {
        LOG_WARN("err memstore percent");
      } else if (OB_FAIL(replica_property.set_memstore_percent(temp_value))) {
        LOG_WARN("set memstore percent failed");
      }
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_server(const ParseNode *parse_tree, ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    server.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_IP_PORT))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode *node = parse_tree->children_[0];
    if (OB_UNLIKELY(NULL == node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node should not be null");
    } else if (OB_FAIL(resolve_server_value(node, server))) {
      LOG_WARN("resolve server value failed", K(ret));
    }
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_zone(const ParseNode *parse_tree, ObZone &zone)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    zone.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_ZONE))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode *node = parse_tree->children_[0];
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

int ObAlterSystemResolverUtil::resolve_tenant(const ParseNode *parse_tree,
                                              ObFixedLengthString < OB_MAX_TENANT_NAME_LENGTH + 1 > &tenant_name)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    tenant_name.reset();
  } else if (OB_FAIL(sanity_check(parse_tree, T_TENANT_NAME))) {
    LOG_WARN("sanity check failed");
  } else {
    const ParseNode *node = parse_tree->children_[0];
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

int ObAlterSystemResolverUtil::resolve_tenant_id(const ParseNode *parse_tree, uint64_t &tenant_id)
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
      tenant_name.assign_ptr(parse_tree->str_value_,
                              static_cast<int32_t>(parse_tree->str_len_));
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

int ObAlterSystemResolverUtil::resolve_ls_id(const ParseNode *parse_tree, int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  if (NULL == parse_tree) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("node should not be null");
  } else if (OB_FAIL(sanity_check(parse_tree, T_LS))) {
    LOG_WARN("sanity check failed");
  } else {
    ls_id = parse_tree->children_[0]->value_;
    FLOG_INFO("resolve ls id", K(ls_id));
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_tablet_id(const ParseNode *opt_tablet_id, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (NULL == opt_tablet_id) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("opt_tablet_id should not be null");
  } else if (OB_FAIL(sanity_check(opt_tablet_id, T_TABLET_ID))) {
    LOG_WARN("sanity check failed");
  } else {
    tablet_id = opt_tablet_id->children_[0]->value_;
    FLOG_INFO("resolve tablet_id", K(tablet_id));
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_force_opt(const ParseNode *parse_tree, bool &force_cmd)
{
  int ret = OB_SUCCESS;
  force_cmd = false;
  if (OB_ISNULL(parse_tree)) {
    //nothing todo
  } else {
    force_cmd = true;
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_string(const ParseNode *node, ObString &string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null");
  } else if (OB_UNLIKELY(T_VARCHAR != node->type_ && T_CHAR != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_VARCHAR/T_CHAR", "type", get_type_name(node->type_));
  } else if (OB_UNLIKELY(node->str_len_ <= 0)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("empty string");
  } else {
    string = ObString(node->str_len_, node->str_value_);
  }
  return ret;
}

int ObAlterSystemResolverUtil::resolve_relation_name(const ParseNode *node, ObString &string)
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

// resolve tenants
int ObAlterSystemResolverUtil::resolve_tenant(
    const ParseNode &tenants_node,
    const uint64_t tenant_id,
    common::ObSArray<uint64_t> &tenant_ids,
    bool &affect_all,
    bool &affect_all_user,
    bool &affect_all_meta)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  // Filter duplicate tenant names.
  hash::ObHashSet<uint64_t> tenant_id_set;
  const int64_t MAX_TENANT_BUCKET = 256;

  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";

  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is empty", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", KR(ret));
  } else if (OB_FAIL(tenant_id_set.create(MAX_TENANT_BUCKET))) {
    LOG_WARN("failed to create tenant id set", K(ret));
  } else {
    ObString tenant_name;
    uint64_t tmp_tenant_id = 0;
    affect_all = false;
    affect_all_user = false;
    affect_all_meta = false;

    for (int64_t i = 0; OB_SUCC(ret) && (i < tenants_node.num_child_); ++i) {
      ParseNode *node = tenants_node.children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children of server_list should not be null", KR(ret));
      } else {
        tenant_name.assign_ptr(node->str_value_,
                               static_cast<ObString::obstr_size_t>(node->str_len_));

        if (tenant_name.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret));
        } else if (0 == strcasecmp(tenant_name.ptr(), "all")) {
          affect_all = true;
        } else if (0 == strcasecmp(tenant_name.ptr(), "all_user")) {
          affect_all_user = true;
        } else if (0 == strcasecmp(tenant_name.ptr(), "all_meta")) {
          affect_all_meta = true;
        } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tmp_tenant_id))) {
          LOG_WARN("tenant not exist", K(tenant_name), KR(ret));
          if (OB_ERR_INVALID_TENANT_NAME == ret && OB_SYS_TENANT_ID != tenant_id) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("change error code, existence of tenant is only accessible to sys tenant", KR(ret));
          }
        } else {
          int hash_ret = tenant_id_set.exist_refactored(tmp_tenant_id);
          if (OB_HASH_EXIST == hash_ret) {
            // duplicate tenant name
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("duplicate tanant name", K(tenant_name), K(tmp_tenant_id), KR(ret));
            if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
                pos, "duplicate tenant %s is", tenant_name.ptr()))) {
              LOG_WARN("failed to set error msg", K(ret), K(tmp_ret), K(error_msg), K(pos));
            } else {
              LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
            }
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            if (OB_FAIL(tenant_ids.push_back(tmp_tenant_id))) {
              LOG_WARN("fail to push tenant id ", K(tenant_name), K(tmp_tenant_id), KR(ret));
            } else if (OB_FAIL(tenant_id_set.set_refactored(tmp_tenant_id))) {
              LOG_WARN("fail to push tenant id ", K(tenant_name), K(tmp_tenant_id), KR(ret));
            }
          } else {
            ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
            LOG_WARN("failed to check tenant exist", K(tenant_name), K(tmp_tenant_id), KR(ret));
          }
        }
      }
      tenant_name.reset();
    }

    if (OB_SUCC(ret) && (affect_all || affect_all_user || affect_all_meta)) {
      if (OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("Only sys tenant can operate all tenants", KR(ret), K(tenant_id));
      } else if (tenants_node.num_child_ > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("all/all_user/all_meta must be used separately",
                 KR(ret), "tenant list count", tenants_node.num_child_);
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "all/all_user/all_meta in combination with other names is");
      }
    }
    FLOG_INFO("resolve tenants", K(affect_all), K(affect_all_user),
              K(affect_all_meta), K(tenant_ids));
  }
  return ret;
}


int ObFreezeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFreezeStmt *freeze_stmt = NULL;
  if (OB_UNLIKELY(NULL == parse_tree.children_)
      || OB_UNLIKELY(parse_tree.num_child_ < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong freeze parse tree", KP(parse_tree.children_),
             K(parse_tree.num_child_));
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
    if (1 == parse_tree.children_[0]->value_) { // MAJOR FREEZE
      freeze_stmt->set_major_freeze(true);
      if (OB_UNLIKELY(3 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong freeze parse tree", K(parse_tree.num_child_));
      } else {
        ParseNode *opt_tenant_list_or_tablet_id = parse_tree.children_[1];
        const ParseNode *opt_rebuild_column_group = parse_tree.children_[2];
        if (OB_FAIL(resolve_major_freeze_(freeze_stmt, opt_tenant_list_or_tablet_id, opt_rebuild_column_group))) {
          LOG_WARN("resolve major freeze failed", KR(ret), KP(opt_tenant_list_or_tablet_id));
        }
      }
    } else if (2 == parse_tree.children_[0]->value_) {  // MINOR FREEZE
      freeze_stmt->set_major_freeze(false);
      if (OB_UNLIKELY(4 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong freeze parse tree", K(parse_tree.num_child_));
      } else {
        ParseNode *opt_tenant_list_or_ls_or_tablet_id = parse_tree.children_[1];
        ParseNode *opt_server_list = parse_tree.children_[2];
        ParseNode *opt_zone_desc = parse_tree.children_[3];
        if (OB_FAIL(resolve_minor_freeze_(
                freeze_stmt, opt_tenant_list_or_ls_or_tablet_id, opt_server_list, opt_zone_desc))) {
          LOG_WARN("resolve minor freeze failed",
                   KR(ret),
                   KP(opt_tenant_list_or_ls_or_tablet_id),
                   KP(opt_server_list),
                   KP(opt_zone_desc));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown freeze type", K(parse_tree.children_[0]->value_));
    }
  }

  return ret;
}

int ObFreezeResolver::resolve_major_freeze_(ObFreezeStmt *freeze_stmt, ParseNode *opt_tenant_list_or_tablet_id, const ParseNode *opt_rebuild_column_group)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();

  if (NULL == opt_tenant_list_or_tablet_id) {
    // if opt_tenant_list_or_tablet_id == NULL, add owned tenant_id
    if (OB_FAIL(freeze_stmt->get_tenant_ids().push_back(cur_tenant_id))) {
      LOG_WARN("fail to push owned tenant id ", KR(ret), "owned tenant_id", cur_tenant_id);
    }
  } else if (OB_UNLIKELY(nullptr == opt_tenant_list_or_tablet_id->children_ || 0 == opt_tenant_list_or_tablet_id->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of tenant should not be null", KR(ret), KP(opt_tenant_list_or_tablet_id));
  } else if (OB_FAIL(resolve_tenant_ls_tablet_(freeze_stmt, opt_tenant_list_or_tablet_id))) {
    LOG_WARN("fail to resolve tenant or tablet", KR(ret));
  } else if (OB_UNLIKELY(share::ObLSID::INVALID_LS_ID != freeze_stmt->get_ls_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to specify ls to major freeze", K(ret), "ls_id", freeze_stmt->get_ls_id());
  } else if (freeze_stmt->get_tablet_id().is_valid()) { // tablet major freeze
    if (T_TABLET_ID == opt_tenant_list_or_tablet_id->type_) {
      if (OB_UNLIKELY(0 != freeze_stmt->get_tenant_ids().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant ids should be empty for type T_TABLET_ID", K(ret));
      } else if (OB_FAIL(freeze_stmt->get_tenant_ids().push_back(cur_tenant_id))) { // if tenant is not explicitly specified, add owned tenant_id
        LOG_WARN("fail to push owned tenant id ", KR(ret), "owned tenant_id", cur_tenant_id);
      }
    } else if (OB_SYS_TENANT_ID != cur_tenant_id) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can add suffix opt of tablet_id after tenant name", KR(ret), K(cur_tenant_id));
    } else if (1 != freeze_stmt->get_tenant_ids().count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not suppport to specify several tenant ids or no tenant_id for tablet major freeze", K(ret),
        "tenant_ids", freeze_stmt->get_tenant_ids());
    }
  } else if (OB_SYS_TENANT_ID != cur_tenant_id && !freeze_stmt->get_tenant_ids().empty()) { // tenant major freeze
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can add suffix opt(tenant=name)", KR(ret), K(cur_tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (opt_rebuild_column_group != nullptr) {
    if (OB_UNLIKELY(!freeze_stmt->get_tablet_id().is_valid())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rebuild only supports tablet major freeze", KR(ret), K(cur_tenant_id));
    } else {
      freeze_stmt->set_rebuild_column_group(true);
    }
  }
  return ret;
}

int ObFreezeResolver::resolve_minor_freeze_(ObFreezeStmt *freeze_stmt,
                                            ParseNode *opt_tenant_list_or_ls_or_tablet_id,
                                            ParseNode *opt_server_list,
                                            ParseNode *opt_zone_desc)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();

  if (OB_NOT_NULL(opt_tenant_list_or_ls_or_tablet_id)) {
    if (OB_FAIL(resolve_tenant_ls_tablet_(freeze_stmt, opt_tenant_list_or_ls_or_tablet_id))) {
      LOG_WARN("resolve tenant ls table failed", KR(ret));
    } else if (T_TABLET_ID == opt_tenant_list_or_ls_or_tablet_id->type_) {
      freeze_stmt->get_tenant_ids().reuse();
      freeze_stmt->get_ls_id() = share::ObLSID::INVALID_LS_ID;
      if (OB_FAIL(freeze_stmt->get_tenant_ids().push_back(cur_tenant_id))) {  // if tenant is not explicitly
                                                                              // specified, add owned tenant_id
        LOG_WARN("fail to push owned tenant id ", KR(ret), "owned tenant_id", cur_tenant_id);
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(opt_server_list) && OB_FAIL(resolve_server_list_(freeze_stmt, opt_server_list))) {
    LOG_WARN("resolve server list failed", KR(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(opt_zone_desc) &&
      OB_FAIL(Util::resolve_zone(opt_zone_desc, freeze_stmt->get_zone()))) {
    LOG_WARN("resolve zone desc failed", KR(ret));
  }

  return ret;
}

int ObFreezeResolver::resolve_tenant_ls_tablet_(ObFreezeStmt *freeze_stmt,
                                                ParseNode *opt_tenant_list_or_ls_or_tablet_id)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();

  if (OB_ISNULL(opt_tenant_list_or_ls_or_tablet_id->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of tenant should not be null", KR(ret));
  } else {
    bool affect_all = false;
    bool affect_all_user = false;
    bool affect_all_meta = false;
    const ParseNode *tenant_list_tuple = nullptr;
    const ParseNode *opt_tablet_id = nullptr;
    const ParseNode *ls_id = nullptr;

    switch (opt_tenant_list_or_ls_or_tablet_id->type_) {
      case T_TENANT_TABLET:
        if (opt_tenant_list_or_ls_or_tablet_id->num_child_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child num", K(ret), K(opt_tenant_list_or_ls_or_tablet_id->num_child_));
        } else {
          tenant_list_tuple = opt_tenant_list_or_ls_or_tablet_id->children_[0];
          opt_tablet_id = opt_tenant_list_or_ls_or_tablet_id->children_[1];
          if (OB_ISNULL(tenant_list_tuple)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant list is nullptr", KR(ret), KP(tenant_list_tuple), KP(ls_id), KP(opt_tablet_id));
          }
        }
        break;
      case T_TENANT_LS_TABLET:
        if (opt_tenant_list_or_ls_or_tablet_id->num_child_ != 3) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child num", K(ret), K(opt_tenant_list_or_ls_or_tablet_id->num_child_));
        } else {
          tenant_list_tuple = opt_tenant_list_or_ls_or_tablet_id->children_[0];
          ls_id = opt_tenant_list_or_ls_or_tablet_id->children_[1];
          opt_tablet_id = opt_tenant_list_or_ls_or_tablet_id->children_[2];
          if (OB_ISNULL(tenant_list_tuple) || OB_ISNULL(ls_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant_list or ls_id is nullptr", KR(ret), KP(tenant_list_tuple), KP(ls_id), KP(opt_tablet_id));
          }
        }
        break;
      case T_TABLET_ID:
        if (opt_tenant_list_or_ls_or_tablet_id->num_child_ != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child num", K(ret), K(opt_tenant_list_or_ls_or_tablet_id->num_child_));
        } else {
          opt_tablet_id = opt_tenant_list_or_ls_or_tablet_id->children_[0];
          if (OB_ISNULL(opt_tablet_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant_list or ls_id is nullptr", KR(ret), KP(opt_tablet_id));
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse node type",
                 K(T_TENANT_TABLET),
                 K(T_TENANT_LS_TABLET),
                 K(opt_tenant_list_or_ls_or_tablet_id->type_));
        break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(tenant_list_tuple) &&
               OB_FAIL(Util::resolve_tenant(*tenant_list_tuple, cur_tenant_id,
                                            freeze_stmt->get_tenant_ids(),
                                            affect_all, affect_all_user, affect_all_meta))) {
      LOG_WARN("fail to resolve tenant", KR(ret));
    } else if (OB_NOT_NULL(ls_id) && OB_FAIL(Util::resolve_ls_id(ls_id, freeze_stmt->get_ls_id()))) {
      LOG_WARN("fail to resolve tablet id", KR(ret));
    } else if (OB_NOT_NULL(opt_tablet_id) &&
               OB_FAIL(Util::resolve_tablet_id(opt_tablet_id, freeze_stmt->get_tablet_id()))) {
      LOG_WARN("fail to resolve tablet id", KR(ret));
    } else if (affect_all || affect_all_user || affect_all_meta) {
      if ((true == affect_all && true == affect_all_user) ||
          (true == affect_all && true == affect_all_meta) ||
          (true == affect_all_user && true == affect_all_meta)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                KR(ret), K(affect_all), K(affect_all_user), K(affect_all_meta));
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "all/all_user/all_meta in combination with other names is");
      } else {
        if (affect_all) {
          freeze_stmt->set_freeze_all();
        } else if (affect_all_user) {
          freeze_stmt->set_freeze_all_user();
        } else {
          freeze_stmt->set_freeze_all_meta();
        }
      }
    }
  }

  return ret;
}

int ObFreezeResolver::resolve_server_list_(ObFreezeStmt *freeze_stmt, ParseNode *opt_server_list)
{
  int ret = OB_SUCCESS;
    if (OB_ISNULL(opt_server_list->children_)
        || OB_UNLIKELY(0 == opt_server_list->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of server_list should not be null");
    } else {
      ObString addr_str;
      ObAddr server;
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_server_list->num_child_; ++i) {
        ParseNode *node = opt_server_list->children_[i];
        if (OB_ISNULL(node)) {
          ret = OB_INVALID_ARGUMENT;
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
  return ret;
}

  //
  // This node has six children_ and they are following:
  // cache_type_: parse_tree.children_[0]
  // opt_namespace: parse_tree.children_[1]
  // opt_sql_id: parse_tree.children_[2]
  // opt_databases: parse_tree.children_[3]
  // opt_tenant_list: parse_tree.children_[4]
  // flush_scope: parse_tree.children_[5]
  //
int ObFlushCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushCacheStmt *stmt = NULL;
  ObSQLSessionInfo* sess = params_.session_info_;
  /* 无论设置租户级配置项，还是系统参数，都需要alter system权限。
       对租户级配置项的修改，算是一种扩展，借用alter system权限进行控制 */
  if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid session");
  } else if (OB_UNLIKELY(T_FLUSH_CACHE != parse_tree.type_ || parse_tree.num_child_ != 6)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument",
             "type", get_type_name(parse_tree.type_),
             "child_num", parse_tree.num_child_);
  } else if (NULL == parse_tree.children_[0]
             || NULL == parse_tree.children_[5]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (NULL == (stmt = create_stmt<ObFlushCacheStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObFlushCacheStmt failed");
  } else {
    ObSchemaGetterGuard schema_guard;

    // first child: resolve cache type
    ParseNode *cache_type_node = parse_tree.children_[0];
    if(T_IDENT == cache_type_node->type_) {
      common::ObString pltmp,plself("pl");
      pltmp.assign_ptr(cache_type_node->str_value_, static_cast<ObString::obstr_size_t>(cache_type_node->str_len_));
      if (0 == pltmp.case_compare(plself)) {
        stmt->flush_cache_arg_.cache_type_ = CACHE_TYPE_PL_OBJ;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only support pl cache's cache evict by identify as T_IDENT", K(ret));
      }
    } else {
      stmt->flush_cache_arg_.cache_type_ = (ObCacheType)cache_type_node->value_;
    }
    // second child: resolve namespace
    ParseNode *namespace_node = parse_tree.children_[1];
    // third child: resolve sql_id
    ParseNode *sql_id_node = parse_tree.children_[2];
    // for adds database id
    // fourth child: resolve db_list
    ParseNode *db_node = parse_tree.children_[3];
    // for adds tenant ids
    // fivth child: resolve tenant list
    ParseNode *t_node = parse_tree.children_[4];
    // sixth child: resolve application fields
    stmt->is_global_ = parse_tree.children_[5]->value_;
    // whether is coarse granularity plan cache evict.
    // tenant level(true) / pcv_set level(false)
    bool is_coarse_granularity = true;
    ObSEArray<common::ObString, 8> db_name_list;

    // namespace
    if (OB_FAIL(ret)) {
    } else if (NULL == namespace_node) {
      stmt->flush_cache_arg_.ns_type_ = ObLibCacheNameSpace::NS_INVALID;
    } else if (stmt->flush_cache_arg_.cache_type_ != CACHE_TYPE_LIB_CACHE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support lib cache's cache evict by namespace", K(stmt->flush_cache_arg_.cache_type_), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support lib cache's cache evict by namespace, other type");
    } else {
      if (OB_UNLIKELY(NULL == namespace_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode *node = namespace_node->children_[0];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else {
          if (node->str_len_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("empty namespace name");
          } else {
            ObString namespce_name(node->str_len_, node->str_value_);
            ObLibCacheNameSpace ns_type = ObLibCacheRegister::get_ns_type_by_name(namespce_name);
            if (ns_type <= ObLibCacheNameSpace::NS_INVALID || ns_type >= ObLibCacheNameSpace::NS_MAX) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid namespace type", K(ns_type));
            } else {
              stmt->flush_cache_arg_.ns_type_ = ns_type;
            }
          }
        }
      }
    }

    // sql_id
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(sql_id_node)) {
      // do nothing
    // currently, only support plan cache's fine-grained cache evict
    } else if (stmt->flush_cache_arg_.cache_type_ != CACHE_TYPE_PLAN &&
               stmt->flush_cache_arg_.cache_type_ != CACHE_TYPE_PL_OBJ) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support plan cache's fine-grained cache evict", K(stmt->flush_cache_arg_.cache_type_), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support plan cache's fine-grained cache evict, other type");
    } else if (lib::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported plan cache's fine-grained cache evict in oracle mode", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "plan cache's fine-grained cache evict in oracle mode is");
    } else if (OB_ISNULL(sql_id_node->children_)
               || OB_ISNULL(sql_id_node->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (T_SQL_ID == sql_id_node->type_) {
      if (sql_id_node->children_[0]->str_len_ > (OB_MAX_SQL_ID_LENGTH+1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        stmt->flush_cache_arg_.sql_id_.assign_ptr(
            sql_id_node->children_[0]->str_value_,
            static_cast<ObString::obstr_size_t>(sql_id_node->children_[0]->str_len_));
        stmt->flush_cache_arg_.is_fine_grained_ = true;
      }
    } else if (T_SCHEMA_ID == sql_id_node->type_) {
      stmt->flush_cache_arg_.schema_id_ = sql_id_node->children_[0]->value_;
      stmt->flush_cache_arg_.is_fine_grained_ = true;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
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
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "flushing cache in database level at coarse flushing");
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
        if (OB_SYS_TENANT_ID != sess->get_effective_tenant_id()
              && OB_FAIL(stmt->flush_cache_arg_.push_tenant(sess->get_effective_tenant_id()))) {
            LOG_WARN("failed  to adds tenant for normal tenant", K(sess->get_effective_tenant_id()), K(ret));
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
              if (OB_FAIL(schema_guard.get_database_id(t_id, db_name_list.at(j), db_id))
                  || (int64_t)db_id == OB_INVALID_ID) {
                ret = OB_ERR_BAD_DATABASE;
                SERVER_LOG(WARN, "database not exist", K(db_name_list.at(j)), K(ret));
              } else if (OB_FAIL(stmt->flush_cache_arg_.push_database(db_id))) {
                SERVER_LOG(WARN, "fail to push database id ",K(db_name_list.at(j)), K(db_id), K(ret));
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
          tenant_name.assign_ptr(t_node->children_[i]->str_value_,
                                 static_cast<ObString::obstr_size_t>(t_node->children_[i]->str_len_));
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
      } //for tenant end
    }
    LOG_INFO("resolve flush command finished!", K(ret), K(sess->get_effective_tenant_id()),
                K(stmt->is_global_), K(stmt->flush_cache_arg_.cache_type_),
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

int ObFlushKVCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FLUSH_KVCACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_FLUSH_KVCACHE", "type", get_type_name(parse_tree.type_));
  } else {
    ObFlushKVCacheStmt *stmt = create_stmt<ObFlushKVCacheStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObFlushKVCacheStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode *node = parse_tree.children_[0];
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

int ObFlushIlogCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushIlogCacheStmt *stmt = NULL;
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
    ParseNode *opt_file_id_node = parse_tree.children_[0];
    ParseNode *file_id_val_node = NULL;
    if (OB_ISNULL(opt_file_id_node)) {
      stmt->file_id_ = 0;
    } else if (OB_ISNULL(opt_file_id_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("opt_file_id_node.children is null", K(ret));
    } else if (OB_ISNULL(file_id_val_node = opt_file_id_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file_id_val_node is null", K(ret));
    } else {
      int64_t file_id_val = file_id_val_node->value_; // type of value_ is int64_t
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

int ObFlushDagWarningsResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlushDagWarningsStmt *stmt = NULL;
  if (OB_UNLIKELY(T_FLUSH_DAG_WARNINGS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type not match T_FLUSH_DAG_WARNINGS", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObFlushDagWarningsStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ObFlushDagWarningsStmt error", K(ret));
  }
  return ret;
}

int ObAdminServerResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_SERVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_SERVER", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminServerStmt *admin_server_stmt = NULL;
    if (NULL == (admin_server_stmt = create_stmt<ObAdminServerStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "create ObAdminServerStmt failed");
    } else {
      stmt_ = admin_server_stmt;
      ParseNode *admin_op = NULL;
      ParseNode *server_list = NULL;
      ParseNode *zone_info = NULL;
      if (3 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "child num not right", "child num", parse_tree.num_child_, K(ret));
      } else {
        admin_op = parse_tree.children_[0];
        server_list = parse_tree.children_[1];
        zone_info = parse_tree.children_[2];
        if (OB_UNLIKELY(NULL == admin_op || NULL == server_list)) {
          SQL_RESV_LOG(WARN, "admin_op and server_list should not be null");
          ret= OB_ERR_UNEXPECTED;
        } else {
          admin_server_stmt->set_op(
              static_cast<ObAdminServerArg::AdminServerOp>(admin_op->value_));
          ObServerList &servers = admin_server_stmt->get_server_list();
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
              addr_str.assign_ptr(server_list->children_[i]->str_value_,
                                static_cast<int32_t>(server_list->children_[i]->str_len_));
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
                zone_str.assign_ptr(zone_info->children_[0]->str_value_,
                                    static_cast<int32_t>(zone_info->children_[0]->str_len_));
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

int ObAdminZoneResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAdminZoneStmt *admin_zone_stmt = NULL;
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
      ParseNode *admin_op = parse_tree.children_[0];
      ParseNode *zone_info = parse_tree.children_[1];
      ParseNode *zone_options = parse_tree.children_[2];

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
              ParseNode *region_info = zone_options->children_[i];
              if (admin_zone_stmt->has_alter_region_option()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple region option");
              } else if (region_info->str_len_ <= 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty region");
              } else if (OB_FAIL(region.assign(ObString(
                        region_info->str_len_, region_info->str_value_)))) {
                SQL_RESV_LOG(WARN, "assign region failed", K(ret));
              } else if (OB_FAIL(admin_zone_stmt->set_alter_region_option())) {
                SQL_RESV_LOG(WARN, "fail to set alter region option", K(ret));
              } else {
                admin_zone_stmt->set_region(region);
              }
            } else if (T_IDC == zone_options->children_[i]->type_) {
              ObIDC idc;
              ParseNode *idc_info = zone_options->children_[i];
              if (admin_zone_stmt->has_alter_idc_option()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple idc option");
              } else if (idc_info->str_len_ <= 0) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty idc");
              } else if (OB_FAIL(idc.assign(ObString(
                        idc_info->str_len_, idc_info->str_value_)))) {
                SQL_RESV_LOG(WARN, "idc assign failed", K(ret));
              } else if (OB_FAIL(admin_zone_stmt->set_alter_idc_option())) {
                SQL_RESV_LOG(WARN, "fail to set alter idc option", K(ret));
              } else {
                admin_zone_stmt->set_idc(idc);
              }
            } else if (T_ZONE_TYPE == zone_options->children_[i]->type_) {
              ParseNode *zone_type_info = zone_options->children_[i];
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

int ObSwitchReplicaRoleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SWITCH_REPLICA_ROLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SWITCH_REPLICA_ROLE", "type", get_type_name(parse_tree.type_));
  } else {
    ObSwitchReplicaRoleStmt *stmt = create_stmt<ObSwitchReplicaRoleStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSwitchReplicaRoleStmt failed");
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info should not be null", K(ret));
    } else {
      stmt_ = stmt;
      ObAdminSwitchReplicaRoleArg &rpc_arg = stmt->get_rpc_arg();
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      }

      if (OB_SUCC(ret)) {
        ParseNode *role = parse_tree.children_[0];
        if (OB_UNLIKELY(NULL == role)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("role should not be null");
        } else if (OB_UNLIKELY(T_INT != role->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", "type", get_type_name(role->type_));
        } else {
          switch (role->value_) {
            case 0: {
              rpc_arg.role_ = ObRole::LEADER;
              LOG_INFO("switch role to LEADER", K(role->value_));
              break;
            }
            case 1: {
              rpc_arg.role_ = ObRole::FOLLOWER;
              LOG_INFO("switch role to FOLLOWER", K(role->value_));
              break;
            }
            case 2: {
              rpc_arg.role_ = ObRole::INVALID_ROLE;
              LOG_INFO("switch role to ARBITRARY", K(role->value_));
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
        ParseNode *node = parse_tree.children_[1];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else if (OB_UNLIKELY(NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children should not be null");
        } else {
          switch (node->type_) {
            case T_LS_SERVER_TENANT: {
              if (OB_FAIL(Util::resolve_ls_id(node->children_[0],
                                              rpc_arg.ls_id_))) {
                LOG_WARN("resolve partition id failed", K(ret));
              } else if (OB_FAIL(Util::resolve_server(node->children_[1], rpc_arg.server_))) {
                LOG_WARN("resolve server failed", K(ret));
              } else if (OB_FAIL(Util::resolve_tenant(node->children_[2], rpc_arg.tenant_name_))) {
                LOG_WARN("resolve tenant failed", K(ret));
              } else {
                if (rpc_arg.ls_id_ < 0 || !rpc_arg.server_.is_valid()) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("ls_id or server is invalid", K(rpc_arg));
                }
                LOG_INFO("resolve switch replica arg done", K_(rpc_arg.ls_id), K_(rpc_arg.server), K_(rpc_arg.tenant_name));
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

int ObReportReplicaResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REPORT_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REPORT_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObReportReplicaStmt *stmt = create_stmt<ObReportReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReportReplicaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                    stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRecycleReplicaResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RECYCLE_REPLICA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RECYCLE_REPLICA", "type", get_type_name(parse_tree.type_));
  } else {
    ObRecycleReplicaStmt *stmt = create_stmt<ObRecycleReplicaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRecycleReplicaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAdminMergeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* sess = params_.session_info_;
  if (OB_UNLIKELY(T_MERGE_CONTROL != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MERGE_CONTROL", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminMergeStmt *stmt = create_stmt<ObAdminMergeStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminMergeStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode *node = parse_tree.children_[0];
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
        const uint64_t cur_tenant_id = sess->get_effective_tenant_id();
        ParseNode *tenants_node = parse_tree.children_[1];
        if (NULL != tenants_node) {
          if (OB_SYS_TENANT_ID != cur_tenant_id) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("Only sys tenant can add suffix opt(tenant=name)", KR(ret), K(cur_tenant_id));
          } else if (T_TENANT_LIST != tenants_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_TENANT_LIST", "type", get_type_name(tenants_node->type_));
          } else {
            bool affect_all = false;
            bool affect_all_user = false;
            bool affect_all_meta = false;
            const int64_t child_num = tenants_node->num_child_;
            if (OB_UNLIKELY(nullptr == tenants_node->children_)
                || OB_UNLIKELY(0 == child_num)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children of tenant should not be null", KR(ret), K(child_num));
            } else if (OB_FAIL(Util::resolve_tenant(*tenants_node, cur_tenant_id,
                                                    stmt->get_rpc_arg().tenant_ids_, affect_all,
                                                    affect_all_user, affect_all_meta))) {
              LOG_WARN("fail to resolve tenant", KR(ret), K(cur_tenant_id));
            } else if (affect_all || affect_all_user || affect_all_meta) {
              if ((true == affect_all && true == affect_all_user) ||
                  (true == affect_all && true == affect_all_meta) ||
                  (true == affect_all_user && true == affect_all_meta)) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                        KR(ret), K(affect_all), K(affect_all_user), K(affect_all_meta));
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                               "all/all_user/all_meta in combination with other names is");
              } else {
                if (affect_all) {
                  stmt->get_rpc_arg().affect_all_ = true;
                } else if (affect_all_user) {
                  stmt->get_rpc_arg().affect_all_user_ = true;
                } else {
                  stmt->get_rpc_arg().affect_all_meta_ = true;
                }
              }
            }
          }
        } else {
          if (OB_FAIL(stmt->get_rpc_arg().tenant_ids_.push_back(cur_tenant_id))) {
            LOG_WARN("fail to push back own tenant id");
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminRecoveryResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RECOVERY_CONTROL != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RECOVERY_CONTROL", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminRecoveryStmt *stmt = create_stmt<ObAdminRecoveryStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminRecoveryStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode *node = parse_tree.children_[0];
        if (OB_UNLIKELY(NULL == node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node should not be null");
        } else if (OB_UNLIKELY(T_INT != node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_INT", "type", get_type_name(node->type_));
        } else {
          switch (node->value_) {
            case 2: {
              stmt->get_rpc_arg().type_ = ObAdminRecoveryArg::SUSPEND_RECOVERY;
              break;
            }
            case 3: {
              stmt->get_rpc_arg().type_ = ObAdminRecoveryArg::RESUME_RECOVERY;
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected recovery admin type", "value", node->value_);
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

int ObClearRootTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CLEAR_ROOT_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_ROOT_TABLE", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearRoottableStmt *stmt = create_stmt<ObClearRoottableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearRoottableStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ParseNode *node = parse_tree.children_[0];
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

int ObRefreshSchemaResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_SCHEMA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_SCHEMA", "type", get_type_name(parse_tree.type_));
  } else {
    ObRefreshSchemaStmt *stmt = create_stmt<ObRefreshSchemaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRefreshSchemaStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRefreshMemStatResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_MEMORY_STAT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_MEMORY_STAT", "type", get_type_name(parse_tree.type_));
  } else {
    ObRefreshMemStatStmt *stmt = create_stmt<ObRefreshMemStatStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRefreshMemStatStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWashMemFragmentationResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_WASH_MEMORY_FRAGMENTATION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_WASH_MEMORY_FRAGMENTATION", "type", get_type_name(parse_tree.type_));
  } else {
    ObWashMemFragmentationStmt *stmt = create_stmt<ObWashMemFragmentationStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObWashMemFragmentationStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRefreshIOCalibrationResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObRefreshIOCalibraitonStmt *stmt = nullptr;
  obrpc::ObAdminRefreshIOCalibrationArg *arg = nullptr;
  if (OB_UNLIKELY(T_REFRESH_IO_CALIBRATION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_IO_CALIBRATION", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObRefreshIOCalibraitonStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObRefreshIOCalibraitonStmt failed");
  } else if (FALSE_IT(stmt_ = stmt)) {
  } else if (OB_UNLIKELY(NULL == parse_tree.children_ || 3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree children is invalid", K(ret), K(parse_tree.num_child_));
  } else {
    arg = &stmt->get_rpc_arg();
  }
  if (OB_SUCC(ret)) {
    // parse storage_name from child[0]
    const ParseNode *storage_name_node = parse_tree.children_[0];
    if (OB_ISNULL(storage_name_node) || storage_name_node->num_child_ <= 0) {
      // allow null, do nothing
    } else if (OB_FAIL(Util::resolve_string(storage_name_node->children_[0], arg->storage_name_))) {
      LOG_WARN("resolve storage name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // parse calibration_list from child[1]
    const ParseNode *calibration_list_node = parse_tree.children_[1];
    if (OB_ISNULL(calibration_list_node)) {
      // null means refresh
      arg->only_refresh_ = true;
    } else if (nullptr == calibration_list_node->children_ || calibration_list_node->num_child_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("calibration list node has no children", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < calibration_list_node->num_child_; ++i) {
        common::ObIOBenchResult item;
        const ParseNode *calibration_info_node = calibration_list_node->children_[i];
        ObString calibration_string;
        if (OB_ISNULL(calibration_info_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children of calibration_list should not be null", K(ret), KP(calibration_info_node), K(i));
        } else if (OB_FAIL(Util::resolve_string(calibration_info_node, calibration_string))) {
          LOG_WARN("resolve calibration info node failed", K(ret));
          if (0 == i && calibration_info_node->str_len_ <= 0) {
            // empty means reset, do nothing
            arg->only_refresh_ = false;
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(ObIOCalibration::parse_calibration_string(calibration_string, item))) {
          LOG_WARN("parse calibration info failed", K(ret), K(calibration_string), K(i));
        } else if (OB_FAIL(arg->calibration_list_.push_back(item))) {
          LOG_WARN("push back calibration item failed", K(ret), K(i), K(item));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // parse server or zone from child[2]
    if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[2], *arg))) {
      LOG_WARN("resolve server or zone failed", K(ret));
    }
  }
  return ret;
}

int check_backup_region(const ObString &backup_region)
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
      const ObRegion &tmp_region = backup_region_array.at(i).region_;
      bool found = false;
      for (int64_t j = 0; !found && j < region_array.count(); ++j) {
        const ObRegion &region = region_array.at(j);
        if (tmp_region == region) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_CANNOT_SET_BACKUP_REGION;
        LOG_WARN("backup region is not exist in region list", K(ret), K(backup_region), K(region_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
            pos, "backup region do not exist in region list. backup region : %s.", backup_region.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_REGION, error_msg);
        }
      }
    }
  }
  return ret;
}

int check_backup_zone(const ObString &backup_zone)
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
      const ObZone &tmp_zone = backup_zone_array.at(i).zone_;
      bool found = false;
      for (int64_t j = 0; !found && j < zone_array.count(); ++j) {
        const ObZone &zone = zone_array.at(j);
        if (tmp_zone == zone) {
          found = true;
        }
      }

      if (!found) {
        ret = OB_CANNOT_SET_BACKUP_ZONE;
        LOG_WARN("backup zone is not exist in zone list", K(ret), K(backup_zone), K(zone_array));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
            pos, "backup zone do not exist in zone list. backup zone : %s.", backup_zone.ptr()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_CANNOT_SET_BACKUP_ZONE, error_msg);
        }
      }
    }
  }
  return ret;
}

static int alter_system_set_reset_constraint_check_and_add_item_mysql_mode(obrpc::ObAdminSetConfigArg &rpc_arg, ObAdminSetConfigItem &item, ObSQLSessionInfo *& session_info)
{
  int ret = OB_SUCCESS;
  bool is_backup_config = false;
  bool can_set_trace_control_info = false;
  int  tmp_ret = OB_SUCCESS;
  tmp_ret = OB_E(EventTable::EN_ENABLE_SET_TRACE_CONTROL_INFO) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    can_set_trace_control_info = true;
  }
  share::ObBackupConfigChecker backup_config_checker;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is NULL", KP(session_info), K(ret));
  } else if (OB_FAIL(backup_config_checker.check_config_name(item.name_.ptr(), is_backup_config))) {
    LOG_WARN("fail to check is valid backup config", K(ret), "config_name", item.name_.ptr(), "config value", item.value_.ptr());
  } else if (is_backup_config) {
    if (rpc_arg.is_valid() && !rpc_arg.is_backup_config_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup configuration items cannot be set together with non data backup configuration items");
      LOG_WARN("backup configuration items cannot be set together with non data backup configuration items", K(ret));
    } else {
      rpc_arg.is_backup_config_ = true;
    }
  } else if (rpc_arg.is_valid() && rpc_arg.is_backup_config_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup configuration items cannot be set together with non data backup configuration items");
    LOG_WARN("backup configuration items cannot be set together with non data backup configuration items", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rpc_arg.items_.push_back(item))) {
    LOG_WARN("add config item failed", K(ret), K(item));
  } else if (0 == STRCMP(item.name_.ptr(), Ob_STR_BACKUP_REGION)) {
    if (OB_FAIL(check_backup_region(item.value_.str()))) {
      LOG_WARN("failed to check backup dest", K(ret));
    }
  } else if (0 == STRCMP(item.name_.ptr(), OB_STR_BACKUP_ZONE)) {
    if (OB_FAIL(check_backup_zone(item.value_.str()))) {
      LOG_WARN("failed to check backup dest", K(ret));
    }
  } else if (0 == STRCMP(item.name_.ptr(), CLUSTER_ID)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cluster_id is not allowed to modify");
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter the parameter cluster_id");
  } else if (0 == STRCMP(item.name_.ptr(), QUERY_RESPPONSE_TIME_FLUSH)) {
    if(OB_FAIL(observer::ObRSTCollector::get_instance().flush_query_response_time(item.exec_tenant_id_, item.value_.str()))){
      LOG_WARN("set query response time flush", K(ret));
    }
  } else if (0 == STRCMP(item.name_.ptr(), QUERY_RESPPONSE_TIME_STATS)) {
    if(OB_FAIL(observer::ObRSTCollector::get_instance().control_query_response_time(item.exec_tenant_id_, item.value_.str()))){
      LOG_WARN("set query response time stats", K(ret));
    }
  } else if (!can_set_trace_control_info &&
              session_info != NULL &&
              0 == STRCMP(item.name_.ptr(), OB_STR_TRC_CONTROL_INFO) &&
              !session_info->is_inner()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("_trace_control_info is not allowed to modify");
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter the parameter _trace_control_info");
  }
  return ret;
}

static int set_reset_check_param_valid_oracle_mode(uint64_t tenant_id ,
    const ObString &name, const ObString &value, ObSchemaChecker *& schema_checker)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_ISNULL(schema_checker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_checker is NULL", K(ret));
  } else if (0 == name.case_compare("tde_method")) {
    ObString tde_method;
    uint64_t compat_version = 0;
    if (!ObTdeMethodUtil::is_valid(value)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported other method", K(value), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter invalid tde_method");
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (compat_version < DATA_VERSION_4_2_1_0
               && ObTdeMethodUtil::is_aes256_algorithm(value)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("encrypt table key with aes256 is not supported", K(ret), K(value));
    } else if (OB_FAIL(share::ObEncryptionUtil::get_tde_method(tenant_id, tde_method))) {
      LOG_WARN("fail to check tenant is method internal", K(ret));
    } else if (0 != tde_method.case_compare("none") && 0 != value.case_compare(tde_method)) {
      // tde_method修改规则
      // 当主密钥已经存在于租户内, 不允许修改为其他类型.
      // 主备库场景放开此限制, 检查主密钥的类型是否和要修改的类型一致.
      const share::schema::ObKeystoreSchema *keystore_schema = NULL;
      if (OB_FAIL(schema_checker->get_keystore_schema(tenant_id, keystore_schema))) {
        LOG_WARN("fail get keystore schema", K(ret));
      } else if (OB_ISNULL(keystore_schema)) {
        ret = OB_OBJECT_NAME_NOT_EXIST;
        LOG_WARN("fail to get keystore schema", K(ret));
      } else if (0 != keystore_schema->get_master_key_id()) {
        if (!GCTX.is_standby_cluster()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter tde method is not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tde method with master key exists");
        } else if (0 == keystore_schema->get_master_key().case_compare("kms")
            && 0 == value.case_compare("bkmi")) {
          /*do nothing*/
        } else if (0 == keystore_schema->get_master_key().case_compare(value)) {
          /*do nothing*/
        } else if (ObTdeMethodUtil::is_internal(value)) {
            /*do nothing*/
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter tde method is not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tde method with master key exists");
        }
      }
    }
  } else if (0 == name.case_compare(EXTERNAL_KMS_INFO)) {
    ObString tde_method;
    ObKmsClient *client = NULL;
    ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
    if (OB_FAIL(share::ObEncryptionUtil::get_tde_method(tenant_id, tde_method))) {
      LOG_WARN("fail to get method internal", K(ret));
    } else if (OB_FAIL(ObKmsClientUtil::get_kms_client(allocator, tde_method, client))) {
      LOG_WARN("fail to get kms client", K(tde_method), K(ret));
    } else if (OB_ISNULL(client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("kms_client is null", K(tde_method), K(ret));
    } else if (OB_FAIL(client->init(value.ptr(), value.length()))) {
      LOG_WARN("the json str is not valid", K(ret));
    }
  }
#endif
  return ret;
}

static int alter_system_set_reset_constraint_check_and_add_item_oracle_mode(obrpc::ObAdminSetConfigArg &rpc_arg, ObAdminSetConfigItem &item,
          uint64_t tenant_id, ObSchemaChecker *& schema_checker)
{
  int ret = OB_SUCCESS;
  share::ObBackupConfigChecker backup_config_checker;
  bool is_backup_config = false;
  if (OB_FAIL(backup_config_checker.check_config_name(item.name_.ptr(), is_backup_config))) {
    LOG_WARN("fail to check is valid backup config", K(ret), "config_name", item.name_.ptr(), "config value", item.value_.ptr());
  } else if (is_backup_config) {
    if (rpc_arg.is_valid() && !rpc_arg.is_backup_config_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup configuration items cannot be set together with non data backup configuration items");
      LOG_WARN("backup configuration items cannot be set together with non data backup configuration items", K(ret));
    } else {
      rpc_arg.is_backup_config_ = true;
    }
  } else if (rpc_arg.is_valid() && rpc_arg.is_backup_config_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup configuration items cannot be set together with non data backup configuration items");
    LOG_WARN("backup configuration items cannot be set together with non data backup configuration items", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_reset_check_param_valid_oracle_mode(
          tenant_id,
          ObString(item.name_.size(), item.name_.ptr()),
          ObString(item.value_.size(), item.value_.ptr()), schema_checker))) {
    LOG_WARN("fail to check param valid", K(ret));
  } else if (OB_FAIL(rpc_arg.items_.push_back(item))) {
    LOG_WARN("add config item failed", K(ret), K(item));
  }
  return ret;
}

/* for mysql mode */
int ObSetConfigResolver::resolve(const ParseNode &parse_tree)
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
      const ParseNode *list_node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == list_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list_node should not be null");
      } else {
        ObSetConfigStmt *stmt = create_stmt<ObSetConfigStmt>();
        if (OB_UNLIKELY(NULL == stmt)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("create stmt failed");
        } else {
          HEAP_VAR(ObCreateTableResolver, ddl_resolver, params_) {
            for (int64_t i = 0; OB_SUCC(ret) && i < list_node->num_child_; ++i) {
              if (OB_UNLIKELY(NULL == list_node->children_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("children should not be null");
                break;
              }

              const ParseNode *action_node = list_node->children_[i];
              if (NULL == action_node) {
                continue;
              }

              // config name
              HEAP_VAR(ObAdminSetConfigItem, item) {
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

                ObString name(action_node->children_[0]->str_len_,
                              action_node->children_[0]->str_value_);
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
                  } else {}
                } else {
                  LOG_WARN("session is null");
                  cast_coll_type = ObCharset::get_system_collation();
                }
                ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
                ObCastCtx cast_ctx(&allocator,
                                   NULL,//to varchar. this field will not be used.
                                   0,//to varchar. this field will not be used.
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
                } else if (session_info_ != NULL && action_node->children_[4] == NULL &&
                    OB_FAIL(check_param_valid(session_info_->get_effective_tenant_id(),
                    ObString(item.name_.size(), item.name_.ptr()),
                    ObString(item.value_.size(), item.value_.ptr())))) {
                  LOG_WARN("fail to check param valid", K(ret));
                } else if (NULL != action_node->children_[2]) {
                  ObString comment(action_node->children_[2]->str_len_,
                                   action_node->children_[2]->str_value_);
                  if (OB_FAIL(item.comment_.assign(comment))) {
                    LOG_WARN("assign comment failed", K(comment), K(ret));
                    break;
                  }
                }

                // ignore config scope
                // server or zone
                if (OB_SUCC(ret) && NULL != action_node->children_[3]) {
                  const ParseNode *n = action_node->children_[3];
                  if (OB_FAIL(Util::resolve_server_or_zone(n, item))) {
                    LOG_WARN("resolve server or zone failed", K(ret));
                    break;
                  }
                } // if

                // tenant
                if (OB_SUCC(ret) && NULL != action_node->children_[4]) {
                  const ParseNode *n = action_node->children_[4];
                  if (T_TENANT_NAME == n->type_) {
                    uint64_t tenant_id = item.exec_tenant_id_;
                    if (OB_SYS_TENANT_ID != tenant_id) {
                      ret = OB_ERR_NO_PRIVILEGE;
                      LOG_WARN("non sys tenant", K(tenant_id), K(ret));
                      break;
                    } else {
                      uint64_t tenant_node_id = OB_INVALID_ID;
                      ObString tenant_name(n->children_[0]->str_len_,
                                           n->children_[0]->str_value_);
                      ObString config_name(item.name_.size(), item.name_.ptr());
                      if (OB_FAIL(item.tenant_name_.assign(tenant_name))) {
                        LOG_WARN("assign tenant name failed", K(tenant_name), K(ret));
                        break;
#ifdef OB_BUILD_TDE_SECURITY
                      } else if (0 == config_name.case_compare(TDE_METHOD)
                                 || 0 == config_name.case_compare(EXTERNAL_KMS_INFO)) {
                        if (OB_ISNULL(schema_checker_)) {
                          ret = OB_ERR_UNEXPECTED;
                          LOG_WARN("schema checker is null", K(ret));
                        } else if (OB_FAIL(schema_checker_->get_tenant_id(tenant_name, tenant_node_id))) {
                          LOG_WARN("fail to get tenant id", K(ret));
                        } else if (OB_FAIL(check_param_valid(tenant_node_id,
                                   config_name,
                                   ObString(item.value_.size(), item.value_.ptr())))) {
                          LOG_WARN("fail to check param valid", K(ret));
                        }
#endif
                      } else if (0 == config_name.case_compare(ARCHIVE_LAG_TARGET)) {
                        ObSArray <uint64_t> tenant_ids;
                        bool affect_all;
                        bool affect_all_user;
                        bool affect_all_meta;
                        if (OB_FAIL(ObAlterSystemResolverUtil::resolve_tenant(*n,
                                                                              tenant_id,
                                                                              tenant_ids,
                                                                              affect_all,
                                                                              affect_all_user,
                                                                              affect_all_meta))) {
                          LOG_WARN("fail to get reslove tenant", K(ret), "exec_tenant_id", tenant_id);
                        } else if (affect_all || affect_all_meta) {
                          ret = OB_NOT_SUPPORTED;
                          LOG_WARN("all/all_meta is not supported by ALTER SYSTEM SET ARCHIVE_LAG_TARGET",
                                  KR(ret), K(affect_all), K(affect_all_user), K(affect_all_meta));
                          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                                        "use all/all_meta in 'ALTER SYSTEM SET ARCHIVE_LAG_TARGET' syntax is");
                        } else if (affect_all_user) {
                          ObSchemaGetterGuard schema_guard;
                          ObSArray <uint64_t> all_tenant_ids;
                          if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
                            LOG_WARN("get_schema_guard failed", K(ret));
                          } else if (OB_FAIL(schema_guard.get_tenant_ids(all_tenant_ids))) {
                            LOG_WARN("fail to get tenant id from schema guard", K(ret));
                          } else {
                            ARRAY_FOREACH_X(all_tenant_ids, i, cnt, OB_SUCC(ret)) {
                              const uint64_t tmp_tenant_id = all_tenant_ids.at(i);
                              if (is_user_tenant(tmp_tenant_id)) {
                                if (OB_FAIL(tenant_ids.push_back(tmp_tenant_id))) {
                                  LOG_WARN("fail to push back", K(ret), K(tmp_tenant_id));
                                }
                              }
                            }
                          }
                        } else if (tenant_ids.empty()) {
                          if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
                            LOG_WARN("fail to push back", K(ret), K(tenant_id));
                          }
                        }
                        if (OB_SUCC(ret) && !tenant_ids.empty()) {
                          bool valid = true;
                          for (int i = 0; i < tenant_ids.count() && valid; i++) {
                            const uint64_t tenant_id = tenant_ids.at(i);
                            valid = valid && ObConfigArchiveLagTargetChecker::check(tenant_id, item);
                            if (!valid) {
                              ret = OB_OP_NOT_ALLOW; //log_user_error is handled in checker
                              LOG_WARN("can not set archive_lag_target", "item", item, K(ret), K(i), K(tenant_id));
                            }
                          }
                        }
                      }
                    }
                  } else {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("resolve tenant name failed", K(ret));
                    break;
                  }
                } else if (OB_SUCC(ret) && (0 == STRCASECMP(item.name_.ptr(), ARCHIVE_LAG_TARGET))) {
                  bool valid = ObConfigArchiveLagTargetChecker::check(item.exec_tenant_id_, item);
                  if (!valid) {
                    ret = OB_OP_NOT_ALLOW;
                    LOG_WARN("can not set archive_lag_target", "item", item, K(ret), "tenant_id", item.exec_tenant_id_);
                  }
                }

                if (OB_SUCC(ret)) {
                  if (OB_FAIL(alter_system_set_reset_constraint_check_and_add_item_mysql_mode(stmt->get_rpc_arg(), item, session_info_))) {
                    LOG_WARN("constraint check failed", K(ret));
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

int ObSetConfigResolver::check_param_valid(int64_t tenant_id ,
    const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_SYS_TENANT_ID) {
    if (0 == name.case_compare(SSL_EXTERNAL_KMS_INFO)) {
#ifndef OB_BUILD_TDE_SECURITY
      ret = OB_NOT_SUPPORTED;
#else
      share::ObSSLClient client;
      if (OB_FAIL(client.init(value.ptr(), value.length()))) {
        OB_LOG(WARN, "ssl client init", K(ret), K(value));
      } else if (OB_FAIL(client.check_param_valid())) {
        OB_LOG(WARN, "ssl client param is not valid", K(ret));
      }
#endif
    } else if (0 == name.case_compare("ssl_client_authentication")
               && (0 == value.case_compare("1")
                   || 0 == value.case_compare("ON")
                   || 0 == value.case_compare("TRUE"))) {
      ObString ssl_config(GCONF.ssl_external_kms_info.str());
      if (!ssl_config.empty()) {
#ifndef OB_BUILD_TDE_SECURITY
        ret = OB_NOT_SUPPORTED;
#else
        share::ObSSLClient client;
        if (OB_FAIL(client.init(ssl_config.ptr(), ssl_config.length()))) {
          OB_LOG(WARN, "kms client init", K(ret), K(ssl_config));
        } else if (OB_FAIL(client.check_param_valid())) {
          OB_LOG(WARN, "kms client param is not valid", K(ret));
        }
#endif
      } else {
        if (EASY_OK != easy_ssl_ob_config_check(OB_SSL_CA_FILE, OB_SSL_CERT_FILE,
                                                OB_SSL_KEY_FILE, NULL, NULL, true, false)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("key and cert not match", K(ret));
          LOG_USER_ERROR(OB_INVALID_CONFIG, "key and cert not match");
        }
      }
    } else if (0 == name.case_compare("_ob_ssl_invited_nodes")) {
      if (OB_ISNULL(GCTX.locality_manager_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("locality manager is null", K(ret), KP(GCTX.locality_manager_));
      } else if (FALSE_IT(GCTX.locality_manager_->set_ssl_invited_nodes(value))) {
      }
      #ifdef ERRSIM
      if (OB_SUCCESS != (ret = OB_E(EventTable::EN_SSL_INVITE_NODES_FAILED) OB_SUCCESS)) {
        LOG_WARN("ERRSIM, fail to set ssl invite node", K(ret));
      }
      #endif
    } else if (0 == name.case_compare("_load_tde_encrypt_engine")) {
      if (OB_FAIL(share::ObTdeEncryptEngineLoader::get_instance().load(value))) {
        LOG_WARN("load antsm-engine failed", K(ret));
      }
    }
  }
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_SUCC(ret)) {
    if (0 == name.case_compare("tde_method")) {
      ObString tde_method;
      uint64_t compat_version = 0;
      if (!ObTdeMethodUtil::is_valid(value)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported other method", K(value), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter invalid tde_method");
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
      } else if (compat_version < DATA_VERSION_4_2_1_0
                 && ObTdeMethodUtil::is_aes256_algorithm(value)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("encrypt table key with aes256 is not supported", K(ret), K(value));
      } else if (OB_FAIL(share::ObEncryptionUtil::get_tde_method(tenant_id, tde_method))) {
        LOG_WARN("fail to check tenant is method internal", K(ret));
      } else if (0 != tde_method.case_compare("none") && 0 != value.case_compare(tde_method)) {
        // tde_method修改规则
        // 当主密钥已经存在于租户内, 不允许修改为其他类型.
        // 主备库场景放开此限制, 检查主密钥的类型是否和要修改的类型一致.
        ObSchemaGetterGuard schema_guard;
        const share::schema::ObKeystoreSchema *keystore_schema = NULL;
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service is empty");
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              tenant_id, schema_guard))) {
          LOG_WARN("get_schema_guard failed");
        } else if (OB_FAIL(schema_guard.get_keystore_schema(tenant_id, keystore_schema))) {
          LOG_WARN("fail get keystore schema", K(ret));
        } else if (OB_ISNULL(keystore_schema)) {
          ret = OB_OBJECT_NAME_NOT_EXIST;
          LOG_WARN("fail to get keystore schema", K(ret));
        } else if (0 != keystore_schema->get_master_key_id()) {
          if (!GCTX.is_standby_cluster()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("alter tde method is not support", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tde method with master key exists");
          } else if (0 == keystore_schema->get_master_key().case_compare("kms")
                     && 0 == value.case_compare("bkmi")) {
            /*do nothing*/
          } else if (0 == keystore_schema->get_master_key().case_compare(value)) {
            /*do nothing*/
          } else if (0 == value.case_compare("internal")) {
             /*do nothing*/
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("alter tde method is not support", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tde method with master key exists");
          }
        }
      }
    } else if (0 == name.case_compare(EXTERNAL_KMS_INFO)) {
      ObString tde_method;
      ObKmsClient *client = NULL;
      ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
      if (OB_FAIL(share::ObEncryptionUtil::get_tde_method(tenant_id, tde_method))) {
        LOG_WARN("fail to get method internal", K(ret));
      } else if (OB_FAIL(ObKmsClientUtil::get_kms_client(allocator, tde_method, client))) {
        LOG_WARN("fail to get kms client", K(tde_method), K(ret));
      } else if (OB_ISNULL(client)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("kms_client is null", K(tde_method), K(ret));
      } else if (OB_FAIL(client->init(value.ptr(), value.length()))) {
        LOG_WARN("the json str is not valid", K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObSetTPResolver::resolve(const ParseNode &parse_tree)
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
      const ParseNode *list_node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == list_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list_node should not be null");
      } else {
        ObSetTPStmt *stmt = create_stmt<ObSetTPStmt>();
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

            const ParseNode *action_node = list_node->children_[i];
            if (OB_ISNULL(action_node)
                || OB_ISNULL(action_node->children_)
                || OB_ISNULL(action_node->children_[0])) {
              continue;
            }

            const ParseNode *value = action_node->children_[0];
            switch (action_node->type_)
            {
            case T_TP_NO: {        // event no
              if (stmt->get_rpc_arg().event_name_ != "") {
                ret = OB_NOT_SUPPORTED;
                SQL_RESV_LOG(WARN, "Setting tp_no and tp_name simultaneously is not supported.");
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Setting tp_no and tp_name simultaneously is");
              } else {
                stmt->get_rpc_arg().event_no_ = value->value_;
              }
              break;
            }
            case T_TP_NAME: {     // event name
              if (stmt->get_rpc_arg().event_no_ != 0) {
                ret = OB_NOT_SUPPORTED;
                SQL_RESV_LOG(WARN, "Setting tp_no and tp_name simultaneously is not supported.");
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Setting tp_no and tp_name simultaneously is");
              } else {
                stmt->get_rpc_arg().event_name_.assign_ptr(
                  value->str_value_, static_cast<ObString::obstr_size_t>(value->str_len_));
              }
              break;
            }
            case T_OCCUR: {        // occurrence
              if (value->value_ > 0) {
                stmt->get_rpc_arg().occur_ = value->value_;
              } else {
                ret = OB_INVALID_ARGUMENT;
              }
            } break;
            case T_TRIGGER_MODE: {      // trigger frequency
              if (T_INT == value->type_) {
                if (value->value_ < 0) {
                  ret = OB_INVALID_ARGUMENT;
                  SQL_RESV_LOG(WARN, "invalid argument", K(value->value_));
                } else {
                  stmt->get_rpc_arg().trigger_freq_ = value->value_;
                }
              }
            } break;
            case T_ERROR_CODE: {        // error code
              if (value->value_ > 0) {
                stmt->get_rpc_arg().error_code_ = -value->value_;
              } else {
                stmt->get_rpc_arg().error_code_ = value->value_;
              }
            } break;
            case T_TP_COND: {        // condition
              stmt->get_rpc_arg().cond_ = value->value_;
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

int ObClearLocationCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CLEAR_LOCATION_CACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_LOCATION_CACHE", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearLocationCacheStmt *stmt = create_stmt<ObClearLocationCacheStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearLocationCacheStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dhildren should not be null");
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[0],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObReloadGtsResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_GTS != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_GTS", "type", get_type_name(parse_tree.type_));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("reload gts not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reload gts command");
  }
  return ret;
}

int ObReloadUnitResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_UNIT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_UNIT", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadUnitStmt *stmt = create_stmt<ObReloadUnitStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadUnitStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObReloadServerResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_SERVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_SERVER", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadServerStmt *stmt = create_stmt<ObReloadServerStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadServerStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObReloadZoneResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RELOAD_ZONE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RELOAD_ZONE", "type", get_type_name(parse_tree.type_));
  } else {
    ObReloadZoneStmt *stmt = create_stmt<ObReloadZoneStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReloadZoneStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObClearMergeErrorResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* sess = params_.session_info_;
  if (OB_UNLIKELY(T_CLEAR_MERGE_ERROR != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CLEAR_MERGE_ERROR", "type", get_type_name(parse_tree.type_));
  } else {
    ObClearMergeErrorStmt *stmt = create_stmt<ObClearMergeErrorStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObClearMergeErrorStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        const uint64_t cur_tenant_id = sess->get_effective_tenant_id();
        ParseNode *tenants_node = parse_tree.children_[0];
        if (NULL != tenants_node) {
          if (OB_SYS_TENANT_ID != cur_tenant_id) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("Only sys tenant can add suffix opt(tenant=name)", KR(ret), K(cur_tenant_id));
          } else if (T_TENANT_LIST != tenants_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_TENANT_LIST", "type", get_type_name(tenants_node->type_));
          } else {
            bool affect_all = false;
            bool affect_all_user = false;
            bool affect_all_meta = false;
            const int64_t child_num = tenants_node->num_child_;
            if (OB_UNLIKELY(nullptr == tenants_node->children_)
                || OB_UNLIKELY(0 == child_num)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("children of tenant should not be null", KR(ret), K(child_num));
            } else if (OB_FAIL(Util::resolve_tenant(*tenants_node, cur_tenant_id,
                                                    stmt->get_rpc_arg().tenant_ids_, affect_all,
                                                    affect_all_user, affect_all_meta))) {
              LOG_WARN("fail to resolve tenant", KR(ret), K(cur_tenant_id));
            } else if (affect_all || affect_all_user || affect_all_meta) {
              if ((true == affect_all && true == affect_all_user) ||
                  (true == affect_all && true == affect_all_meta) ||
                  (true == affect_all_user && true == affect_all_meta)) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("only one of affect_all,affect_all_user,affect_all_meta can be true",
                        KR(ret), K(affect_all), K(affect_all_user), K(affect_all_meta));
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                               "all/all_user/all_meta in combination with other names is");
              } else {
                if (affect_all) {
                  stmt->get_rpc_arg().affect_all_ = true;
                } else if (affect_all_user) {
                  stmt->get_rpc_arg().affect_all_user_ = true;
                } else {
                  stmt->get_rpc_arg().affect_all_meta_ = true;
                }
              }
            }
          }
        } else {
          if (OB_FAIL(stmt->get_rpc_arg().tenant_ids_.push_back(cur_tenant_id))) {
            LOG_WARN("fail to push back own tenant id");
          }
        }
      }
    }
  }
  return ret;
}

int ObMigrateUnitResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_MIGRATE_UNIT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_MIGRATE_UNIT", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObMigrateUnitStmt *stmt = create_stmt<ObMigrateUnitStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObMigrateUnitStmt failed");
    } else {
      stmt_ = stmt;
      ParseNode *node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node should not be null");
      } else {
        stmt->get_rpc_arg().unit_id_ = node->value_;
        if (NULL == parse_tree.children_[1] ) {
          stmt->get_rpc_arg().is_cancel_ = true ;
        } else if (OB_FAIL(Util::resolve_server_value(parse_tree.children_[1],
                                                      stmt->get_rpc_arg().destination_))) {
          LOG_WARN("resolve server failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAddArbitrationServiceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(parse_tree);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported in CE version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "add arbitration service in CE version");
#else
  if (OB_UNLIKELY(T_ADD_ARBITRATION_SERVICE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADD_ARBITRATION_SERVICE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObAddArbitrationServiceStmt *stmt = create_stmt<ObAddArbitrationServiceStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAddArbitrationServiceStmt failed");
    } else {
      stmt_ = stmt;
      ObString dest("");
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], dest))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(stmt->get_rpc_arg().init(dest))) {
        LOG_WARN("fail to init arg", K(ret), K(dest));
      }
    }
  }
#endif
  return ret;
}

int ObRemoveArbitrationServiceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(parse_tree);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported in CE version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "remove arbitration service in CE version");
#else
  if (OB_UNLIKELY(T_REMOVE_ARBITRATION_SERVICE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REMOVE_ARBITRATION_SERVICE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObRemoveArbitrationServiceStmt *stmt = create_stmt<ObRemoveArbitrationServiceStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRemoveArbitrationServiceStmt failed");
    } else {
      stmt_ = stmt;
      ObString dest("");
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], dest))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(stmt->get_rpc_arg().init(dest))) {
        LOG_WARN("fail to init arg", K(ret), K(dest));
      }
    }
  }
#endif
  return ret;
}

int ObReplaceArbitrationServiceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ARBITRATION
  UNUSEDx(parse_tree);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported in CE version", KR(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace arbitration service in CE version");
#else
  if (OB_UNLIKELY(T_REPLACE_ARBITRATION_SERVICE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REPLACE_ARBITRATION_SERVICE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObReplaceArbitrationServiceStmt *stmt = create_stmt<ObReplaceArbitrationServiceStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObReplaceArbitrationServiceStmt failed");
    } else {
      stmt_ = stmt;
      ObString arbitration_service("");
      ObString previous_arbitration_service("");
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], previous_arbitration_service))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], arbitration_service))) {
        LOG_WARN("resolve string for previous arb service failed", K(ret));
      } else if (OB_FAIL(stmt->get_rpc_arg().init(arbitration_service, previous_arbitration_service))) {
        LOG_WARN("fail to init arg", KR(ret), K(arbitration_service), K(previous_arbitration_service));
      }
    }
  }
#endif
  return ret;
}

int ObUpgradeVirtualSchemaResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_UPGRADE_VIRTUAL_SCHEMA != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_UPGRADE_VIRTUAL_SCHEMA", "type", get_type_name(parse_tree.type_));
  } else {
    ObUpgradeVirtualSchemaStmt *stmt = create_stmt<ObUpgradeVirtualSchemaStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObUpgradeVirtualSchemaStmt failed");
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObAdminUpgradeCmdResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_UPGRADE_CMD != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_UPGRADE_CMD", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminUpgradeCmdStmt *stmt = create_stmt<ObAdminUpgradeCmdStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminUpgradeCmdStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_ISNULL(parse_tree.children_) || 1 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be NULL and number of children should be one", K(ret));
      } else {
        ParseNode *op = parse_tree.children_[0];
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

int ObAdminRollingUpgradeCmdResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_ROLLING_UPGRADE_CMD != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ROLLING_UPGRADE_CMD", "type", get_type_name(parse_tree.type_));
  } else {
    ObAdminRollingUpgradeCmdStmt *stmt = create_stmt<ObAdminRollingUpgradeCmdStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAdminRollingUpgradeCmdStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      if (OB_ISNULL(parse_tree.children_) || 1 != parse_tree.num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be NULL and number of children should be one", K(ret));
      } else {
        ParseNode *op = parse_tree.children_[0];
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

int ObPhysicalRestoreTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTenantStmt *stmt = nullptr;
  if (OB_UNLIKELY(T_PHYSICAL_RESTORE_TENANT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_PHYSICAL_RESTORE_TENANT", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_UNLIKELY(7 != parse_tree.num_child_ && 2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of children not match", K(ret), "child_num", parse_tree.num_child_);
  } else if (OB_ISNULL(stmt = create_stmt<ObPhysicalRestoreTenantStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObPhysicalRestoreTenantStmt failed");
  } else if (OB_FALSE_IT(stmt_ = stmt)) {
  } else if (2 == parse_tree.num_child_) {
    // parse preview
    stmt->set_is_preview(true);
    stmt->get_rpc_arg().initiator_tenant_id_ = OB_SYS_TENANT_ID;
    const ParseNode *time_node = parse_tree.children_[1];
    if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], stmt->get_rpc_arg().uri_))) {
      LOG_WARN("resolve string failed", K(ret));
    } else if (OB_ISNULL(parse_tree.children_[1])) {
      stmt->get_rpc_arg().with_restore_scn_ = false;
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore preview must have a scn or timestamp, otherwise");
    } else if (0/*timestamp*/ == time_node->children_[0]->value_) {
      stmt->get_rpc_arg().restore_timestamp_.assign_ptr(time_node->children_[1]->str_value_, time_node->children_[1]->str_len_);
      stmt->get_rpc_arg().with_restore_scn_ = false;
    } else if (1/*timestamp*/ == time_node->children_[0]->value_) {
      if (share::OB_BASE_SCN_TS_NS >= time_node->children_[1]->value_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "until scn, it should be positive integer");
        LOG_WARN("until scn, it should be positive integer", KR(ret), K(time_node->children_[1]->value_));
      } else if (OB_FAIL(stmt->get_rpc_arg().restore_scn_.convert_for_sql(time_node->children_[1]->value_))) {
        LOG_WARN("failed to convert scn", K(ret));
      } else {
        stmt->get_rpc_arg().with_restore_scn_ = true;
      }
    }
  } else {
    stmt->set_is_preview(false);
    stmt->get_rpc_arg().initiator_tenant_id_ = OB_SYS_TENANT_ID;
      if(OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], stmt->get_rpc_arg().tenant_name_))) {
        LOG_WARN("resolve tenant_name failed", K(ret));
      } else {
        const ObString &tenant_name = stmt->get_rpc_arg().tenant_name_;
        if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(tenant_name))) {
          LOG_WARN("unsupported tenant name", KR(ret), K(tenant_name));
        } else if (OB_NOT_NULL(parse_tree.children_[1])) {
          if (session_info_->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid sql syntax", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "have backup_dest and restore_source at the same time");
          } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1],
                                          stmt->get_rpc_arg().uri_))) {
            LOG_WARN("resolve string failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_NOT_NULL(parse_tree.children_[4])
          && OB_FAIL(Util::resolve_string(parse_tree.children_[4],
                                          stmt->get_rpc_arg().encrypt_key_))) {
        LOG_WARN("failed to resolve encrypt key", K(ret));
      } else if (OB_NOT_NULL(parse_tree.children_[5])) {
        ParseNode *kms_node = parse_tree.children_[5];
        if (2 != kms_node->num_child_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("num of children not match", K(ret), "child_num", kms_node->num_child_);
        } else if (OB_ISNULL(kms_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("kms uri should not be NULL", K(ret));
        } else if (OB_FAIL(Util::resolve_string(kms_node->children_[0],
                                                stmt->get_rpc_arg().kms_uri_))) {
          LOG_WARN("failed to resolve kms uri", K(ret));
        } else if (OB_NOT_NULL(kms_node->children_[1])
            && OB_FAIL(Util::resolve_string(kms_node->children_[1],
                                            stmt->get_rpc_arg().kms_encrypt_key_))) {
          LOG_WARN("failed to resolve kms encrypt key", K(ret));
        }
      }

      ParseNode *description_node = parse_tree.children_[6];
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[3],
                                              stmt->get_rpc_arg().restore_option_))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_NOT_NULL(description_node)
          && OB_FAIL(Util::resolve_string(description_node, stmt->get_rpc_arg().description_))) {
        LOG_WARN("fail to resolve description", K(ret));
#ifdef OB_BUILD_TDE_SECURITY
      } else if (OB_FAIL(resolve_kms_encrypt_info(stmt->get_rpc_arg().restore_option_))) {
        LOG_WARN("fail to resolve encrypt info", K(ret));
#endif
      } else if (OB_FAIL(resolve_decryption_passwd(stmt->get_rpc_arg()))) {
        LOG_WARN("failed to resolve decrytion passwd", K(ret));
      } else if (OB_FAIL(resolve_restore_source_array(stmt->get_rpc_arg()))) {
        LOG_WARN("failed to resolve restore source array", K(ret));
      } else {
        // resolve datetime
        const ParseNode *time_node = parse_tree.children_[2];
        if (OB_ISNULL(parse_tree.children_[2])) {
          stmt->get_rpc_arg().with_restore_scn_ = false;
        } else if (0/*timestamp*/ == time_node->children_[0]->value_) {
          stmt->get_rpc_arg().restore_timestamp_.assign_ptr(time_node->children_[1]->str_value_, time_node->children_[1]->str_len_);
          stmt->get_rpc_arg().with_restore_scn_ = false;
        } else if (1/*timestamp*/ == time_node->children_[0]->value_) {
          if (share::OB_BASE_SCN_TS_NS >= time_node->children_[1]->value_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "until scn, it should be positive integer");
            LOG_WARN("until scn, it should be positive integer", KR(ret), K(time_node->children_[1]->value_));
          } else if (OB_FAIL(stmt->get_rpc_arg().restore_scn_.convert_for_sql(time_node->children_[1]->value_))) {
            LOG_WARN("failed to convert scn", K(ret));
          } else {
            stmt->get_rpc_arg().with_restore_scn_ = true;
          }
        }
      }
    }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObPhysicalRestoreTenantResolver::resolve_kms_encrypt_info(common::ObString store_option)
{
  int ret = OB_SUCCESS;
  const char *encrypt_option_str = "kms_encrypt=true";
  ObString kms_var("kms_encrypt_info");
  int64_t encrypt_opt_str_len = strlen(encrypt_option_str);
  bool is_kms_encrypt = false;
  for (int i = 0; i <= store_option.length() - encrypt_opt_str_len; ++i) {
    if (0 == STRNCASECMP(store_option.ptr() + i, encrypt_option_str, encrypt_opt_str_len)) {
      is_kms_encrypt = true;
      break;
    }
  }
  if (is_kms_encrypt) {
     ObObj value;
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null" , K(ret));
    } else if (OB_FAIL(session_info_->get_user_variable_value(kms_var, value))) {
      LOG_WARN("fail to get user variable", K(ret));
    } else {
      ObPhysicalRestoreTenantStmt *stmt = static_cast<ObPhysicalRestoreTenantStmt *>(stmt_);
      stmt->get_rpc_arg().kms_info_ = value.get_varchar(); //浅拷贝即可
      bool is_valid = false;
      if (OB_FAIL(check_kms_info_valid(stmt->get_rpc_arg().kms_info_, is_valid))) {
        LOG_WARN("fail to check kms info valid", K(ret));
      } else if (!is_valid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("kms info is not valid", K(ret));
      }
    }
  }

  return ret;
}

int ObPhysicalRestoreTenantResolver::check_kms_info_valid(const ObString &kms_info, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (kms_info.length() > 4000 || kms_info.length() < 0) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}
#endif

int ObPhysicalRestoreTenantResolver::resolve_decryption_passwd(
    ObPhysicalRestoreTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObObj value;

  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null" , K(ret));
  } else if (!session_info_->user_variable_exists(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR)) {
    LOG_INFO("no decryption passwd is specified");
    arg.passwd_array_.reset();
  } else if (OB_FAIL(session_info_->get_user_variable_value(
      OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR, value))) {
    LOG_WARN("fail to get user variable", K(ret));
  } else {
    arg.passwd_array_ = value.get_varchar();
    LOG_INFO("succeed to resolve_decryption_passwd", "passwd", arg.passwd_array_);
  }

  return ret;

}

int ObPhysicalRestoreTenantResolver::resolve_restore_source_array(
    obrpc::ObPhysicalRestoreTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObObj value;

  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null" , K(ret));
  } else if (!session_info_->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
    LOG_INFO("no restore source is specified");
    arg.multi_uri_.reset();
  } else if (OB_FAIL(session_info_->get_user_variable_value(
      OB_RESTORE_SOURCE_NAME_SESSION_STR, value))) {
    LOG_WARN("failed to get user variable", KR(ret));
  } else {
    arg.multi_uri_ = value.get_varchar();
    LOG_INFO("succeed to resolve_restore_source_array", "multi_uri", arg.multi_uri_);
  }
  return ret;
}

int ObRunJobResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_RUN_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RUN_JOB", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else {
    ObRunJobStmt *stmt = create_stmt<ObRunJobStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRunJobStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], stmt->get_rpc_arg().job_))) {
        LOG_WARN("resolve string failed", K(ret));
      } else if (OB_FAIL(Util::resolve_server_or_zone(parse_tree.children_[1],
                                                      stmt->get_rpc_arg()))) {
        LOG_WARN("resolve server or zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRunUpgradeJobResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_RUN_UPGRADE_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_RUN_UPGRADE_JOB",
             KR(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KR(ret));
  } else {
    ObRunUpgradeJobStmt *stmt = create_stmt<ObRunUpgradeJobStmt>();
    if (OB_ISNULL(stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRunUpgradeJobStmt failed", KR(ret));
    } else {
      stmt_ = stmt;
      // 1. parse action
      ObString str;
      uint64_t version = OB_INVALID_VERSION;
      if (OB_FAIL(Util::resolve_string(parse_tree.children_[0], str))) {
        LOG_WARN("resolve string failed", KR(ret));
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_BEGIN))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_BEGIN;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_SYSTEM_VARIABLE))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_VARIABLE;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_SYSTEM_TABLE))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_TABLE;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_VIRTUAL_SCHEMA))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_VIRTUAL_SCHEMA;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_SYSTEM_PACKAGE))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_PACKAGE;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_ALL_POST_ACTION))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_ALL_POST_ACTION;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_INSPECTION))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_INSPECTION;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_END))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_END;
      } else if (0 == str.case_compare(rootserver::ObRsJobTableOperator::get_job_type_str(
                 rootserver::JOB_TYPE_UPGRADE_ALL))) {
        stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_ALL;
      } else {
        // UPGRADE_POST_ACTION
        if (OB_FAIL(ObClusterVersion::get_version(str, version))) {
          LOG_WARN("fail to get version", KR(ret), K(str));
        } else {
          stmt->get_rpc_arg().action_ = obrpc::ObUpgradeJobArg::UPGRADE_POST_ACTION;
          stmt->get_rpc_arg().version_ = static_cast<int64_t>(version);
        }
      }
      // 2. parse tenant_ids
      if (OB_SUCC(ret)
          && parse_tree.num_child_ >= 2
          && OB_NOT_NULL(parse_tree.children_[1])) {
        ParseNode *tenants_node = parse_tree.children_[1];
        bool affect_all = false;
        bool affect_all_user = false;
        bool affect_all_meta = false;
        const int64_t child_num = tenants_node->num_child_;
        const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();
        ObSArray<uint64_t> &tenant_ids = stmt->get_rpc_arg().tenant_ids_;
        if (T_TENANT_LIST != tenants_node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type is not T_TENANT_LIST", KR(ret),
                   "type", get_type_name(tenants_node->type_));
        } else if (OB_ISNULL(tenants_node->children_)
                   || OB_UNLIKELY(0 == child_num)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children of tenant should not be null", KR(ret), K(child_num));
        } else if (OB_FAIL(Util::resolve_tenant(*tenants_node, cur_tenant_id, tenant_ids,
                                                affect_all, affect_all_user, affect_all_meta))) {
          LOG_WARN("fail to resolve tenant", KR(ret), K(cur_tenant_id));
        } else if (affect_all_user || affect_all_meta) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("all_user/all_meta is not supported by ALTER SYSTEM RUN UPGRADE JOB",
                  KR(ret), K(affect_all_user), K(affect_all_meta));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                        "use all_user/all_meta in 'ALTER SYSTEM RUN UPGRADE JOB' syntax is");
        } else if (affect_all && 0 != tenant_ids.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant_ids should be empty when specify tenant = all", KR(ret));
        } else if (obrpc::ObUpgradeJobArg::UPGRADE_SYSTEM_PACKAGE == stmt->get_rpc_arg().action_) {
          if ((tenant_ids.count() > 1)
               || (1 == tenant_ids.count() && !is_sys_tenant(tenant_ids.at(0)))) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Only system tenant can run UPGRADE_SYSTEM_PACKAGE Job", KR(ret), K(tenant_ids));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Non-sys tenant run UPGRADE_SYSTEM_PACKAGE job");
          }
        }
      }
    }
  }
  return ret;
}

int ObStopUpgradeJobResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ADMIN_STOP_UPGRADE_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ADMIN_STOP_UPGRADE_JOB",
             KR(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KR(ret));
  } else {
    ObStopUpgradeJobStmt *stmt = create_stmt<ObStopUpgradeJobStmt>();
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

int ObSwitchRSRoleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SWITCH_RS_ROLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SWITCH_RS_ROLE", K(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("childern should noto be null", K(ret));
  } else {
    ObSwitchRSRoleStmt *stmt = create_stmt<ObSwitchRSRoleStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSwitchRSRoleStmt failed", K(ret));
    } else {
      stmt_ = stmt;
      ObAdminSwitchRSRoleArg &rpc_arg = stmt->get_rpc_arg();
      ParseNode *role = parse_tree.children_[0];
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
        ParseNode *node = parse_tree.children_[1];
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

int ObRefreshTimeZoneInfoResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_REFRESH_TIME_ZONE_INFO != parse_tree.type_)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REFRESH_TIME_ZONE_INFO", "type", get_type_name(parse_tree.type_),
              K(session_info_));
  } else {
    ObRefreshTimeZoneInfoStmt *refresh_time_zone_info_stmt = create_stmt<ObRefreshTimeZoneInfoStmt>();
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
int ObEnableSqlThrottleResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObEnableSqlThrottleStmt *stmt = nullptr;

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
    ParseNode *priority_node = parse_tree.children_[0];
    if (nullptr != priority_node) {
      stmt->set_priority(priority_node->value_);
    }
    ParseNode *metrics_node = parse_tree.children_[1];
    if (metrics_node != nullptr) {
      for (int i = 0; i < metrics_node->num_child_; i++) {
        ParseNode *node = metrics_node->children_[i];
        ParseNode *valNode = node->children_[0];
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

int ObDisableSqlThrottleResolver::resolve(const ParseNode &parse_tree)
{
  UNUSED(parse_tree);
  int ret = OB_SUCCESS;
  stmt_ = create_stmt<ObDisableSqlThrottleStmt>();
  return ret;
}

int ObCancelTaskResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CANCEL_TASK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CANCEL_TASK", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree's children is null", K(ret));
  } else {
    ObCancelTaskStmt *cancel_task = create_stmt<ObCancelTaskStmt>();
    if (NULL == cancel_task) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObCancelTaskStmt failed");
    } else {
      stmt_ = cancel_task;
      ParseNode *cancel_type_node = parse_tree.children_[0];
      ParseNode *task_id = parse_tree.children_[1];
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
            task_type = GROUP_MIGRATION_TASK;
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

int ObSetDiskValidResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_SET_DISK_VALID != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SET_DISK_VALID", "type", get_type_name(parse_tree.type_));
  } else {
    ObSetDiskValidStmt *stmt = create_stmt<ObSetDiskValidStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObSetDiskValidStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        if (OB_FAIL(Util::resolve_server(parse_tree.children_[0],
                                         stmt->server_))) {
          LOG_WARN("resolve server failed", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObAlterDiskgroupAddDiskResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_DISKGROUP_ADD_DISK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_DISKGROUP_ADD_DISK", "type", get_type_name(parse_tree.type_));
  } else {
    ObAddDiskStmt *stmt = create_stmt<ObAddDiskStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAddDiskStmt failed");
    } else {
      stmt_ = stmt;
      ObAdminAddDiskArg &arg = stmt->arg_;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], arg.diskgroup_name_))) {
        LOG_WARN("failed to resolve diskgroup_name", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], arg.disk_path_))) {
        LOG_WARN("failed to resolve disk_path", K(ret));
      } else if (NULL != parse_tree.children_[2]
                    && OB_FAIL(Util::resolve_string(parse_tree.children_[2], arg.alias_name_))) {
        LOG_WARN("failed to resolve alias name", K(ret));
      } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[3], arg.server_))) {
        LOG_WARN("failed to resolve server", K(ret));
      } else if (NULL != parse_tree.children_[4]
                    && OB_FAIL(Util::resolve_zone(parse_tree.children_[4], arg.zone_))) {
        LOG_WARN("failed to resolve zone", K(ret));
      } else {
        LOG_INFO("succeed to resolve add disk arg", K(arg));
      }
    }
  }
  return ret;
}

int ObAlterDiskgroupDropDiskResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_DISKGROUP_DROP_DISK != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_DISKGROUP_DROP_DISK", "type", get_type_name(parse_tree.type_));
  } else {
    ObDropDiskStmt *stmt = create_stmt<ObDropDiskStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropDiskStmt failed");
    } else {
      stmt_ = stmt;
      ObAdminDropDiskArg &arg = stmt->arg_;
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_FAIL(Util::resolve_relation_name(parse_tree.children_[0], arg.diskgroup_name_))) {
        LOG_WARN("failed to resolve diskgroup_name", K(ret));
      } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], arg.alias_name_))) {
        LOG_WARN("failed to resolve alias name", K(ret));
      } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[2], arg.server_))) {
        LOG_WARN("failed to resolve server", K(ret));
      } else if (NULL != parse_tree.children_[3]
                    && OB_FAIL(Util::resolve_zone(parse_tree.children_[3], arg.zone_))) {
        LOG_WARN("failed to resolve zone", K(ret));
      } else {
        LOG_INFO("succeed to resolve drop disk arg", K(arg));
      }
    }
  }
  return ret;
}

int ObClearBalanceTaskResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *tenants_node = NULL;
  ParseNode *zone_node = NULL;
  ParseNode *task_type_node = NULL;
  ObClearBalanceTaskStmt *clear_task_stmt = NULL;
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
  if (OB_SUCC(ret) && NULL != tenants_node) {//以租户为单位
    if (T_TENANT_LIST != tenants_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tenant_name node", K(ret));
    } else if (OB_UNLIKELY(NULL == tenants_node->children_)
               || OB_UNLIKELY(0 == tenants_node->num_child_)) {
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
      common::ObIArray<uint64_t> &tenant_ids = clear_task_stmt->get_tenant_ids();
      uint64_t tenant_id = 0;
      ObString tenant_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenants_node->num_child_; ++i) {
        ParseNode *node = tenants_node->children_[i];
        if (OB_ISNULL(node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("children of server_list should not be null");
        } else {
          tenant_name.assign_ptr(node->str_value_,
                                 static_cast<ObString::obstr_size_t>(node->str_len_));
          if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
            LOG_WARN("tenant not exist", K(tenant_name));
          } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push tenant id ", K(tenant_name), K(tenant_id));
          }
        }
        tenant_name.reset();
      } // end for
    }
  } //end if (NULL != tenants_node)

  if (OB_SUCC(ret) && NULL != zone_node) {//以zone为单位
    if (T_ZONE_LIST != zone_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid zone_name node", K(ret));
    } else if (OB_UNLIKELY(NULL == zone_node->children_)
               || OB_UNLIKELY(0 == zone_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of zone should not be null");
    } else {
      common::ObIArray<ObZone> &zone_names = clear_task_stmt->get_zone_names();
      ObZone zone_name ;
      ObString parse_zone_name;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_node->num_child_; ++i) {
        ParseNode *node = zone_node->children_[i];
        if (OB_ISNULL(node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("children of server_list should not be null");
        } else {
          parse_zone_name.assign_ptr(node->str_value_,
                                     static_cast<ObString::obstr_size_t>(node->str_len_));
          if (OB_FAIL(zone_name.assign(parse_zone_name))) {
            LOG_WARN("failed to assign zone name", K(ret), K(parse_zone_name));
          } else if (OB_FAIL(zone_names.push_back(zone_name))) {
            LOG_WARN("failed to push back zone name", K(ret), K(zone_name));
          }
        }
        parse_zone_name.reset();
        zone_name.reset();
      }//end for
    }
  }// end if (NULL != zone_node)

  if (OB_SUCC(ret)) {
    if (NULL == task_type_node) {
      clear_task_stmt->set_type(ObAdminClearBalanceTaskArg::ALL);
    } else if (T_BALANCE_TASK_TYPE != task_type_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid type node", K(ret));
    } else if (OB_UNLIKELY(NULL == task_type_node->children_)
               || OB_UNLIKELY(0 == task_type_node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of node_type should not be null");
    } else {
      clear_task_stmt->set_type(static_cast<ObAdminClearBalanceTaskArg::TaskType>(task_type_node->children_[0]->value_));
    }
  }
  return ret;
}

int ObChangeTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CHANGE_TENANT != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CHANGE_TENANT", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(schema_checker_)
             || OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_checker or schema_guard is null", K(ret));
  } else {
    ObChangeTenantStmt *stmt = create_stmt<ObChangeTenantStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObChangeTenantStmt failed");
    } else {
      if (OB_UNLIKELY(NULL == parse_tree.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else if (OB_UNLIKELY(T_TENANT_NAME != parse_tree.children_[0]->type_
                             && T_TENANT_ID != parse_tree.children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid parse_tree", K(ret));
      } else {
        if (T_TENANT_ID == parse_tree.children_[0]->type_) {
          uint64_t tenant_id = static_cast<uint64_t>(parse_tree.children_[0]->value_);
          bool is_exist = false;
          ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
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
          tenant_name.assign_ptr((char *)(parse_tree.children_[0]->str_value_),
                                 static_cast<int32_t>(parse_tree.children_[0]->str_len_));
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

int ObDropTempTableResolver::resolve(const ParseNode &parse_tree)
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
    ObDropTableStmt *stmt = create_stmt<ObDropTableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDropTableStmt failed");
    } else {
      stmt_ = stmt;
      obrpc::ObDropTableArg &drop_table_arg = stmt->get_drop_table_arg();
      drop_table_arg.if_exist_ = true;
      drop_table_arg.to_recyclebin_ = false;
      drop_table_arg.tenant_id_ = session_info_->get_login_tenant_id();
      drop_table_arg.table_type_ = share::schema::TMP_TABLE_ALL;
      drop_table_arg.session_id_ = static_cast<uint64_t>(parse_tree.children_[0]->value_);
    }
  }
  return ret;
}

int ObRefreshTempTableResolver::resolve(const ParseNode &parse_tree)
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
    ObAlterTableStmt *stmt = create_stmt<ObAlterTableStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObAlterTableStmt failed");
    } else if (OB_FAIL(stmt->set_tz_info_wrap(session_info_->get_tz_info_wrap()))) {
      LOG_WARN("failed to set_tz_info_wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(stmt->set_nls_formats(
        session_info_->get_local_nls_date_format(),
        session_info_->get_local_nls_timestamp_format(),
        session_info_->get_local_nls_timestamp_tz_format()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else {
      stmt_ = stmt;
      stmt->set_is_alter_system(true);
      obrpc::ObAlterTableArg &alter_table_arg = stmt->get_alter_table_arg();
      alter_table_arg.session_id_ = static_cast<uint64_t>(parse_tree.children_[0]->value_);
      alter_table_arg.alter_table_schema_.alter_type_ = OB_DDL_ALTER_TABLE;
      //compat for old server
      alter_table_arg.tz_info_ = session_info_->get_tz_info_wrap().get_tz_info_offset();
      if (OB_FAIL(alter_table_arg.tz_info_wrap_.deep_copy(session_info_->get_tz_info_wrap()))) {
        LOG_WARN("failed to deep_copy tz info wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
      } else if (OB_FAIL(alter_table_arg.set_nls_formats(
          session_info_->get_local_nls_date_format(),
          session_info_->get_local_nls_timestamp_format(),
          session_info_->get_local_nls_timestamp_tz_format()))) {
        LOG_WARN("failed to set_nls_formats", K(ret));
      } else if (OB_FAIL(alter_table_arg.alter_table_schema_.alter_option_bitset_.add_member(obrpc::ObAlterTableArg::SESSION_ACTIVE_TIME))) {
        LOG_WARN("failed to add member SESSION_ACTIVE_TIME for alter table schema", K(ret), K(alter_table_arg));
      }
    }
  }
  return ret;
}

// for oracle mode grammer: alter system set sys_var = val
int ObAlterSystemSetResolver::resolve(const ParseNode &parse_tree)
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
        LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER", K(ret),
                 K(set_node->type_));
      } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node is null", K(ret));
      } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
      } else {
        ParseNode *var = nullptr;
        if (OB_ISNULL(var = set_param_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var is NULL", K(ret));
        } else if (T_IDENT != var->type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "Variable name isn't identifier type");
        } else {
          ObString name(var->str_len_, var->str_value_);
          share::ObBackupConfigChecker backup_config_checker;
          bool is_backup_config = false;
          if (OB_FAIL(backup_config_checker.check_config_name(name, is_backup_config))) {
            LOG_WARN("fail to check config name", K(ret), K(name));
          } else if (is_backup_config) {
            set_parameters = true;
            break;
          } else {
            omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
            if (tenant_config.is_valid() &&
                nullptr != tenant_config->get_container().get(ObConfigStringKey(name))) {
                set_parameters = true;
                break;
            }
          }
        }
      }
    } // for
  }
  /* second round: gen stmt */
  if (OB_SUCC(ret)) {
    if (set_parameters) {
      FLOG_WARN("set parameters");
      ObSetConfigStmt *setconfig_stmt = create_stmt<ObSetConfigStmt>();
      if (OB_ISNULL(setconfig_stmt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("create set config stmt failed", KR(ret));
      } else {
        HEAP_VAR(ObCreateTableResolver, ddl_resolver, params_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
            ParseNode *set_node = nullptr, *set_param_node = nullptr;
            if (OB_ISNULL(set_node = parse_tree.children_[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("set_node should not be null", K(ret));
            } else if (T_ALTER_SYSTEM_SET_PARAMETER != set_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER",
                       K(ret), K(set_node->type_));
            } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("set_node is null", K(ret));
            } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
            } else {
              ParseNode *name_node = nullptr, *value_node = nullptr;
              HEAP_VAR(ObAdminSetConfigItem, item) {
                item.exec_tenant_id_ = tenant_id;
                /* name */
                if (OB_ISNULL(name_node = set_param_node->children_[0])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("var is NULL", K(ret));
                } else if (T_IDENT != name_node->type_) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED,
                      "Variable name isn't identifier type");
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
                  if (OB_SUCCESS !=
                      session_info_->get_collation_connection(cast_coll_type)) {
                    LOG_WARN("fail to get collation_connection");
                    cast_coll_type = ObCharset::get_default_collation(
                        ObCharset::get_default_charset());
                  }
                  ObArenaAllocator allocator(ObModIds::OB_SQL_COMPILE);
                  ObCastCtx cast_ctx(&allocator,
                                     NULL, //to varchar. this field will not be used.
                                     0, //to varchar. this field will not be used.
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
                  if (OB_FAIL(alter_system_set_reset_constraint_check_and_add_item_oracle_mode(
                      setconfig_stmt->get_rpc_arg(), item, tenant_id, schema_checker_))) {
                    LOG_WARN("constraint check failed", K(ret));
                  } else if (OB_SUCC(ret) && (0 == STRCASECMP(item.name_.ptr(), ARCHIVE_LAG_TARGET))) {
                    bool valid = ObConfigArchiveLagTargetChecker::check(item.exec_tenant_id_, item);
                    if (!valid) {
                      ret = OB_OP_NOT_ALLOW;
                      LOG_WARN("can not set archive_lag_target", "item", item, K(ret), "tenant_id", item.exec_tenant_id_);
                    }
                  }
                }
              }
            }
          } // for
        }
      }
    } else {
      ObVariableSetStmt *variable_set_stmt = create_stmt<ObVariableSetStmt>();
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
            LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_SET_PARAMETER",
                     K(ret), K(set_node->type_));
          } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node is null", K(ret));
          } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
          } else {
            ParseNode *var = NULL;
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
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "Variable name isn't identifier type");
              } else {
                var_node.is_system_variable_ = true;
                var_name.assign_ptr(var->str_value_,
                    static_cast<int32_t>(var->str_len_));
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(ob_write_string(*allocator_, var_name,
                                            var_node.variable_name_))) {
                  LOG_WARN("Can not malloc space for variable name", K(ret));
                } else {
                  ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI,
                                    var_node.variable_name_);
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
              if (OB_SUCC(ret) && OB_FAIL(variable_set_stmt->add_variable_node(var_node))) {
                LOG_WARN("Add set entry failed", K(ret));
              }
            }
          } // end resolve variable and value
        } // end for
      }
    }
    /* 无论设置租户级配置项，还是系统参数，都需要alter system权限。
       对租户级配置项的修改，算是一种扩展，借用alter system权限进行控制 */
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_ALTER_SYSTEM_SET_PARAMETER,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }

  } // if

  return ret;
}


int ObArchiveLogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ARCHIVE_LOG != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ARCHIVE_LOG", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObArchiveLogStmt *stmt = create_stmt<ObArchiveLogStmt>();
    const int64_t is_enable = parse_tree.children_[0]->value_;
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    ObSArray<uint64_t> archive_tenant_ids;
    ParseNode *t_node = parse_tree.children_[1];
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObArchiveLogStmt failed");
    } else if (NULL != t_node) {
      if (OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("Only sys tenant can add suffix opt(tenant=name)", KR(ret), K(tenant_id));
      } else if (T_TENANT_LIST != t_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is not T_TENANT_LIST", "type", get_type_name(t_node->type_));
      } else {
        bool affect_all = false;
        bool affect_all_user = false;
        bool affect_all_meta = false;
        const int64_t child_num = t_node->num_child_;
        if (OB_UNLIKELY(nullptr == t_node->children_)
            || OB_UNLIKELY(0 == child_num)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("children of tenant should not be null", KR(ret), K(child_num));
        } else if (OB_FAIL(Util::resolve_tenant(*t_node, tenant_id, archive_tenant_ids,
                                                affect_all, affect_all_user, affect_all_meta))) {
          LOG_WARN("fail to resolve tenant", KR(ret), K(tenant_id));
        } else if (affect_all_user || affect_all_meta) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("all_user/all_meta is not supported by ALTER SYSTEM ARCHIVELOG",
                  KR(ret), K(affect_all_user), K(affect_all_meta));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                        "use all_user/all_meta in 'ALTER SYSTEM ARCHIVELOG' syntax is");
        } else if (affect_all) {
        } else if (archive_tenant_ids.empty()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("No valid tenant name given", K(ret), K(tenant_id));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "No valid tenant name given");
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->set_param(is_enable, tenant_id, archive_tenant_ids))) {
      LOG_WARN("Failed to set archive tenant ids", K(ret), K(is_enable), K(tenant_id), K(archive_tenant_ids));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_DATABASE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_DATABASE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(5 != parse_tree.num_child_ && 6 != parse_tree.num_child_ )) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t with_tenant = parse_tree.children_[0]->value_;
    common::ObSArray<uint64_t> backup_tenant_ids;
    ObBackupPathString backup_dest;
    ObBackupDescription backup_description;
    if (0 == with_tenant && OB_SYS_TENANT_ID != tenant_id) {
      // user tenant backup
      ParseNode *dest_node = parse_tree.children_[3];
      ParseNode *description_node = parse_tree.children_[4];
      if (nullptr != dest_node && OB_FAIL(backup_dest.assign(dest_node->str_value_))) {
        LOG_WARN("failed to assign backup_dest", K(ret));
      } else if (nullptr != description_node && OB_FAIL(backup_description.assign(description_node->str_value_))) {
        LOG_WARN("failed to assign backup_description", K(ret));
      }
    } else if (0 == with_tenant && OB_SYS_TENANT_ID == tenant_id) {
      ParseNode *dest_node = parse_tree.children_[3];
      ParseNode *description_node = parse_tree.children_[4];
      if (nullptr != dest_node && OB_FAIL(backup_dest.assign(dest_node->str_value_))) {
        LOG_WARN("failed to assign backup_dest", K(ret));
      } else if (nullptr != description_node && OB_FAIL(backup_description.assign(description_node->str_value_))) {
        LOG_WARN("failed to assign backup_description", K(ret));
      }
    } else if (1 == with_tenant && OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("user tenant cannot specify tenant names", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    } else if (1 == with_tenant/*tenant level*/ && OB_SYS_TENANT_ID == tenant_id) {
      ParseNode *t_node = parse_tree.children_[3];
      ParseNode *dest_node = parse_tree.children_[4];
      ParseNode *description_node = parse_tree.children_[5];
      if (nullptr == t_node) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("No tenant name specified", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "No tenant name specified");
      } else if (OB_FAIL(Util::get_tenant_ids(*t_node, backup_tenant_ids))) {
        LOG_WARN("failed to get tenant ids");
      } else if (nullptr != dest_node && OB_FAIL(backup_dest.assign(dest_node->str_value_))) {
        LOG_WARN("failed to assign backup_dest", K(ret));
      } else if (nullptr != description_node && OB_FAIL(backup_description.assign(description_node->str_value_))) {
        LOG_WARN("failed to assign backup_description", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!backup_dest.is_empty()) { // TODO(chognrong.th) support specify path in 4.1
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("backup do not support using specify path ", K(ret), K(backup_dest), K(backup_tenant_ids));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup do not support using specify path");
    }

    const int64_t compl_log = parse_tree.children_[1]->value_;
    const int64_t incremental = parse_tree.children_[2]->value_;
    ObBackupDatabaseStmt *stmt = create_stmt<ObBackupDatabaseStmt>();
    if (OB_FAIL(ret)) {
    } else if (nullptr == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupDatabaseStmt failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, incremental, compl_log, backup_dest, backup_description, backup_tenant_ids))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(incremental));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObCancelRestoreResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CANCEL_RESTORE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CANCEL_RESTORE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cancel restore is not supported under cluster version 4_2_0_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cancel restore is");
  } else {
    ObCancelRestoreStmt *stmt = NULL;
    if (OB_ISNULL(stmt = create_stmt<ObCancelRestoreStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt", K(ret));
    } else if (OB_UNLIKELY(T_IDENT != parse_tree.children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node", K(ret));
    } else {
      ObString tenant_name;
      tenant_name.assign_ptr(parse_tree.children_[0]->str_value_, parse_tree.children_[0]->str_len_);
      stmt->get_drop_tenant_arg().exec_tenant_id_ = OB_SYS_TENANT_ID;
      stmt->get_drop_tenant_arg().if_exist_ = false;
      stmt->get_drop_tenant_arg().force_drop_ = true;
      stmt->get_drop_tenant_arg().delay_to_drop_ = false;
      stmt->get_drop_tenant_arg().open_recyclebin_ = false;
      stmt->get_drop_tenant_arg().tenant_name_ = tenant_name;
      stmt->get_drop_tenant_arg().drop_only_in_restore_ = true;
    }
  }
  return ret;
}

int ObCancelRecoverTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  uint64_t session_tenant_id = session_info_->get_effective_tenant_id();
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObRecoverTableStmt *stmt = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObString tenant_name;
  if (OB_UNLIKELY(T_CANCEL_RECOVER_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CANCEL_RESTORE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(1 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (!is_sys_tenant(session_tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("user tenant cancel recover table is not allowed", K(ret), K(session_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "user tenant cancel recover table is");
  } else if (OB_ISNULL(stmt = create_stmt<ObRecoverTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create stmt", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != parse_tree.children_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session_tenant_id, schema_guard))) {
    LOG_WARN("failed to get_tenant_schema_guard", KR(ret));
  } else if (OB_FALSE_IT(tenant_name.assign_ptr(parse_tree.children_[0]->str_value_, parse_tree.children_[0]->str_len_))) {
  } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
    LOG_WARN("failed to get tenant id from schema guard", KR(ret), K(tenant_name));
  } else if (OB_FAIL(ObRecoverTableUtil::check_compatible(tenant_id))) {
    LOG_WARN("check recover table compatible failed", K(ret));
  } else {
    stmt->get_rpc_arg().tenant_id_ = tenant_id;
    stmt->get_rpc_arg().tenant_name_ = tenant_name;
    stmt->get_rpc_arg().action_ = ObRecoverTableArg::CANCEL;
  }

  return ret;
}

int ObAlterSystemResolverUtil::get_tenant_ids(const ParseNode &t_node, ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  hash::ObHashSet<uint64_t> tenant_id_set;
  const int64_t MAX_TENANT_BUCKET = 256;
  if (T_TENANT_LIST != t_node.type_ || NULL == t_node.children_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(t_node.type_), KP(t_node.children_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid argument", K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
             OB_SYS_TENANT_ID, schema_guard))) {
    SERVER_LOG(WARN, "get_schema_guard failed", K(ret));
  } else if (OB_FAIL(tenant_id_set.create(MAX_TENANT_BUCKET))) {
    SERVER_LOG(WARN, "failed to create tenant id set", K(ret));
  } else {
    uint64_t tenant_id = 0;
    ObString tenant_name;
    const char *const ALL_TENANT = "ALL";
    for (int64_t i = 0; OB_SUCC(ret) && i < t_node.num_child_; ++i) {
      if (OB_ISNULL(t_node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid argument", KP(t_node.children_[i]), K(ret));
      } else {
        tenant_name.assign_ptr(t_node.children_[i]->str_value_,
                               static_cast<ObString::obstr_size_t>(t_node.children_[i]->str_len_));
        if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id))) {
          SERVER_LOG(WARN, "tenant not exist", K(tenant_name), K(ret), K(i), K(t_node.num_child_));
        } else if (OB_SYS_TENANT_ID == tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant id list is unexpected", K(ret), K(tenant_id));
        } else {
          int hash_ret = tenant_id_set.exist_refactored(tenant_id);
          if (OB_HASH_EXIST == hash_ret) {
            //do nothing
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
              SERVER_LOG(WARN, "failed to push tenant id into array", K(ret));
            } else if (OB_FAIL(tenant_id_set.set_refactored(tenant_id))) {
              SERVER_LOG(WARN, "failed to push tenant id into set", K(ret));
            }
          } else {
            ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
            SERVER_LOG(WARN, "failed to check tenant exist", K(ret));
          }
        }
      }
      tenant_name.reset();
    } //for tenant end
  }
  return ret;
}

int ObTableTTLResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();
  if (OB_FAIL(GET_MIN_DATA_VERSION(cur_tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("TTL command is not supported in data version less than 4.2.1", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "TTL command is not supported in data version less than 4.2.1");
  } else if (!ObKVFeatureModeUitl::is_ttl_enable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ttl is disable", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ttl is disable, set by config item _obkv_feature_mode");
  } else if (OB_UNLIKELY(T_TABLE_TTL != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_TABLE_TTL", "type", get_type_name(parse_tree.type_));
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
    ObTableTTLStmt* ttl_stmt = create_stmt<ObTableTTLStmt>();
    const int64_t type = parse_tree.children_[0]->value_;
    ParseNode *opt_tenant_list_v2 = parse_tree.children_[1];
    if (NULL == ttl_stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObTableTTLStmt failed");
    } else if (OB_FAIL(ttl_stmt->set_type(type))) {
      LOG_WARN("fail to set param", K(ret), K(type));
    } else {
      if (NULL == opt_tenant_list_v2) {
        if (OB_SYS_TENANT_ID == cur_tenant_id) {
          ttl_stmt->set_ttl_all(true);
        } else if (OB_FAIL(ttl_stmt->get_tenant_ids().push_back(cur_tenant_id))) {
          LOG_WARN("fail to push owned tenant id ", KR(ret), "owned tenant_id", cur_tenant_id);
        }
      } else if (OB_SYS_TENANT_ID != cur_tenant_id) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("only sys tenant can add suffix opt(tenant=name)", KR(ret), K(cur_tenant_id));
      } else {
        bool affect_all = false;
        bool affect_all_user = false;
        bool affect_all_meta = false;
        if (OB_FAIL(Util::resolve_tenant(*opt_tenant_list_v2, cur_tenant_id, ttl_stmt->get_tenant_ids(), affect_all, affect_all_user, affect_all_meta))) {
          LOG_WARN("fail to resolve tenant", KR(ret), KP(opt_tenant_list_v2), K(cur_tenant_id));
        } else if (affect_all_meta) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("affect_all_meta and affect_all_user is not supported", KR(ret), K(affect_all_meta), K(affect_all_user));
          LOG_USER_WARN(OB_NOT_SUPPORTED, "affect_all_meta and affect_all_user");
        } else if (affect_all || affect_all_user) {
          ttl_stmt->set_ttl_all(true);
        }
      }
    }
    if (OB_SUCC(ret)) {
      stmt_ = ttl_stmt;
    }
  }

  return ret;
}

int ObBackupManageResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_MANAGE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_MANAGE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_ && 2 != parse_tree.num_child_
    && 4 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObBackupManageStmt *stmt = create_stmt<ObBackupManageStmt>();
    common::ObSArray<uint64_t> managed_tenants;
    int64_t copy_id = 0;
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t type = parse_tree.children_[0]->value_;
    const int64_t value = parse_tree.children_[1]->value_;
    if (2 == parse_tree.num_child_) {
      // do nothing
    } else if (3 == parse_tree.num_child_) {
      copy_id = NULL == parse_tree.children_[2]
                  ? 0 : parse_tree.children_[2]->children_[0]->value_;
    } else if (4 == parse_tree.num_child_) {
      const int64_t tenant_flag = parse_tree.children_[2]->value_;
      if (1 != tenant_flag) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant id or tenant flag", K(ret), K(tenant_flag));
      } else {
        ParseNode *t_node = parse_tree.children_[3];
        if (OB_ISNULL(t_node)) {
        } else if (!is_sys_tenant(tenant_id)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only sys tenant can cancel specify tenant backup", K(ret), K(tenant_id));
        } else if (OB_FAIL(Util::get_tenant_ids(*t_node, managed_tenants))) {
          LOG_WARN("failed to get tenant ids", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupManageResolver failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, type, value, copy_id, managed_tenants))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(type), K(value), K(copy_id));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupCleanResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_CLEAN != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_CLEAN", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(4 != parse_tree.num_child_ && 5 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t type = parse_tree.children_[0]->value_;
    const int64_t value = parse_tree.children_[1]->value_;
    ObBackupDescription description;
    ParseNode *t_node = NULL;
    common::ObSArray<uint64_t> clean_tenant_ids;
    int64_t copy_id = 0;
    ObBackupCleanStmt *stmt = create_stmt<ObBackupCleanStmt>();
    if (share::ObNewBackupCleanType::TYPE::CANCEL_DELETE != static_cast<share::ObNewBackupCleanType::TYPE>(type)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported function ", K(ret), K(type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "function is");
    } else if (5 == parse_tree.num_child_) {
      copy_id = NULL == parse_tree.children_[3]
                  ? 0 : parse_tree.children_[3]->children_[0]->value_;
      t_node = parse_tree.children_[4];
    } else {
      t_node = parse_tree.children_[3];
    }

    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupCleanResolver failed");
    } else if (NULL != parse_tree.children_[2] && OB_FAIL(description.assign(parse_tree.children_[2]->str_value_))) {
      LOG_WARN("failed to assign description", K(ret));
    } else if (NULL != t_node && OB_SYS_TENANT_ID != tenant_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("user tenant cannot specify tenant names", K(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    } else if (NULL != t_node && OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(Util::get_tenant_ids(*t_node, clean_tenant_ids))) {
        LOG_WARN("failed to get tenant ids", K(ret));
      } else if (clean_tenant_ids.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("No valid tenant name given", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "No valid tenant name given");
      } else if (5 == parse_tree.num_child_ && clean_tenant_ids.count() > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To initiate cleanup set or piece under the system tenant, only one tenant name can be specified", K(ret), K(tenant_id), K(clean_tenant_ids));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Too many tenant names specified, only one tenant name can be specified");
      }
    } else if (NULL == t_node && OB_SYS_TENANT_ID == tenant_id) {
      if (1 == type || 2 == type) { // type=1 is BACKUPSET and type=2 is BACKUPPIECE
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To initiate cleanup set or piece under the system tenant, need to specify one tenant name", K(ret), K(tenant_id), K(clean_tenant_ids));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "need to specify a tenant name");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->set_param(tenant_id, type, value, copy_id, description, clean_tenant_ids))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(type), K(value), K(copy_id));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}


int ObDeletePolicyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_DELETE_POLICY != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_DELETE_POLICY", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_ && 6 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t type = parse_tree.children_[0]->value_;
    const ObString policy_name = parse_tree.children_[2]->str_value_;
    common::ObSArray<uint64_t> clean_tenant_ids;
    ObDeletePolicyStmt *stmt = create_stmt<ObDeletePolicyStmt>();
    ParseNode *t_node = parse_tree.children_[1];
    if (0 != STRCMP(policy_name.ptr(), "default")) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the policy name is not \'default\'", K(ret), K(policy_name));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "The policy name is not \'default\', it is");
    } else if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObDeletePolicyStmt failed");
    } else if (NULL != t_node && OB_SYS_TENANT_ID != tenant_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("user tenant cannot specify tenant names", K(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    } else if (NULL != t_node && OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(Util::get_tenant_ids(*t_node, clean_tenant_ids))) {
        LOG_WARN("failed to get tenant ids", K(ret));
      } else if (clean_tenant_ids.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("No valid tenant name given", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "No valid tenant name given");
      } else if (clean_tenant_ids.count() > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To handle deleted policy under the system tenant, only one tenant name can be specified", K(ret), K(tenant_id), K(clean_tenant_ids));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Too many tenant names specified, only one tenant name can be specified");
      }
    } else if (NULL == t_node && OB_SYS_TENANT_ID == tenant_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("To handle deleted policy under the system tenant, only one tenant name can be specified", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Too little tenant names specified, only one tenant name can be specified");
    }

    if (OB_SUCC(ret) && 6 == parse_tree.num_child_) {
      const ObString recovery_window = OB_ISNULL(parse_tree.children_[3]) ? "" : parse_tree.children_[3]->str_value_;
      const int64_t redundancy = OB_ISNULL(parse_tree.children_[4]) ? 1 : parse_tree.children_[4]->value_;
      const int64_t backup_copies = OB_ISNULL(parse_tree.children_[5]) ? 0 : parse_tree.children_[5]->value_;
      if (OB_ISNULL(parse_tree.children_[3]) && OB_ISNULL(parse_tree.children_[4])) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("recovery_window and redundancy must set one", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Neither recovery_window nor redundancy is set");
      } else if (0 !=recovery_window.length()) {
        bool is_valid = true;
        int64_t val = 0;
        val = ObConfigTimeParser::get(recovery_window.ptr(), is_valid);
        if (!is_valid) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid time interval str", K(ret), K(recovery_window));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "recovery window is not be time string");
        }
      } else if (1 != redundancy) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("redundancy is not equal to 1", K(ret), K(redundancy));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Redundancy not equal to 1 is");
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(stmt->set_delete_policy(recovery_window, redundancy, backup_copies))) {
        LOG_WARN("failed to set delete policy", K(ret), K(recovery_window), K(redundancy), K(backup_copies));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->set_param(tenant_id, type, policy_name, clean_tenant_ids))) {
      LOG_WARN("failed to set param", K(ret), K(tenant_id), K(type), K(policy_name));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObBackupKeyResolver::resolve(const ParseNode &parse_tree)
{
#ifndef OB_BUILD_TDE_SECURITY
  int ret = OB_ERR_PARSE_SQL;
#else
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_BACKUP_KEY != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_KEY", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(4 != parse_tree.num_child_ && 3 != parse_tree.num_child_ )) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t with_tenant = parse_tree.children_[0]->value_;
    common::ObSArray<uint64_t> backup_tenant_ids;
    ObBackupPathString backup_dest;
    ObString encrypt_key;
    if (0 == with_tenant) {
      ParseNode *dest_node = parse_tree.children_[1];
      ParseNode *encrypt_key_node = parse_tree.children_[2];
      if (nullptr != dest_node && OB_FAIL(backup_dest.assign(dest_node->str_value_))) {
        LOG_WARN("failed to assign backup_dest", K(ret));
      } else if (nullptr != encrypt_key_node) {
        encrypt_key.assign_ptr(encrypt_key_node->str_value_,
                               static_cast<ObString::obstr_size_t>(encrypt_key_node->str_len_));
      }
    } else if (1 == with_tenant && OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("user tenant cannot specify tenant names", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    } else if (1 == with_tenant/*tenant level*/ && OB_SYS_TENANT_ID == tenant_id) {
      ParseNode *t_node = parse_tree.children_[1];
      ParseNode *dest_node = parse_tree.children_[2];
      ParseNode *encrypt_key_node = parse_tree.children_[3];
      if (nullptr == t_node) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("No tenant name specified", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "No tenant name specified");
      } else if (OB_FAIL(Util::get_tenant_ids(*t_node, backup_tenant_ids))) {
        LOG_WARN("failed to get tenant ids");
      } else if (backup_tenant_ids.count() != 1) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant list, count of tenants must be 1");
      } else if (FALSE_IT(tenant_id = backup_tenant_ids.at(0))) {
      } else if (nullptr != dest_node && OB_FAIL(backup_dest.assign(dest_node->str_value_))) {
        LOG_WARN("failed to assign backup_dest", K(ret));
      } else if (nullptr != encrypt_key_node) {
        encrypt_key.assign_ptr(encrypt_key_node->str_value_,
                               static_cast<ObString::obstr_size_t>(encrypt_key_node->str_len_));
      }
    }

    ObBackupKeyStmt *stmt = create_stmt<ObBackupKeyStmt>();
    if (OB_FAIL(ret)) {
    } else if (nullptr == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupKeyStmt failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, backup_dest, encrypt_key))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id));
    } else {
      stmt_ = stmt;
    }
  }
#endif
  return ret;
}

int ObBackupArchiveLogResolver::resolve(const ParseNode &parse_tree)
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
    ObBackupArchiveLogStmt *stmt = create_stmt<ObBackupArchiveLogStmt>();
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

int ObBackupSetEncryptionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObBackupSetEncryptionStmt *stmt = nullptr;

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

    if (NULL == (stmt = create_stmt<ObBackupSetEncryptionStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupSetEncryptionStmt failed", K(ret));
    } else if (OB_FAIL(stmt->set_param(mode, passwd))) {
      LOG_WARN("Failed to set param", K(ret), K(mode), K(passwd), KP(session_info_));
    }
    stmt_ = stmt;
  }
  return ret;
}

int ObBackupSetDecryptionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *passwd_list = NULL;
  ObBackupSetDecryptionStmt *stmt = nullptr;

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
    ret= OB_ERR_UNEXPECTED;
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
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-sys tenant set decryption is");
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < passwd_list->num_child_; ++i) {
    ParseNode *node = passwd_list->children_[i];
    const ObString passwd(node->str_len_, node->str_value_);
    if (OB_FAIL(stmt->add_passwd(passwd))) {
      LOG_WARN("failed to add passwd", K(ret), K(i), K(passwd));
    }
  }

  stmt_ = stmt;
  return ret;
}

int ObSetRegionBandwidthResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *src_region_node = NULL;
  const ParseNode *dst_region_node = NULL;
  const ParseNode *max_bw_node     = NULL;
  common::ObSqlString max_bw_str;
  int64_t max_bw = 0;
  bool valid = false;
  ObSetRegionBandwidthStmt *stmt = nullptr;

  // refer to ObResourceUnitOptionResolver<T>::resolve_option;
  if (OB_UNLIKELY(T_SET_REGION_NETWORK_BANDWIDTH != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_SET_REGION_NETWORK_BANDWIDTH", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (NULL == (stmt = create_stmt<ObSetRegionBandwidthStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObSetRegionBandwidthStmt failed", K(ret));
  } else {
    src_region_node = parse_tree.children_[0];
    dst_region_node = parse_tree.children_[1];
    max_bw_node     = parse_tree.children_[2];

    if (T_INT == max_bw_node->type_) {
      max_bw = static_cast<int64_t>(max_bw_node->value_);
      if (OB_UNLIKELY(0 == max_bw)) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param, max_bw can't be zero");
      }
    } else if (T_VARCHAR == max_bw_node->type_) {
      if (OB_FAIL(max_bw_str.append(max_bw_node->str_value_, max_bw_node->str_len_))) {
        SQL_RESV_LOG(WARN, "fail to assign child str", K(ret));
      } else {
        max_bw = common::ObConfigCapacityParser::get(max_bw_str.ptr(), valid);
        if (!valid) {
          ret = common::OB_ERR_PARSE_SQL;
        } else if (OB_UNLIKELY(0 == max_bw)) {
          ret = common::OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param, max_bw can't be zero");
        }
      }
    } /* else if (T_NUMBER == max_bw_node->type_) {
    } */ else {
      ret = common::OB_ERR_PARSE_SQL;
    }

    if (OB_SUCC(ret)) {
      stmt->set_param(src_region_node->str_value_, dst_region_node->str_value_, max_bw);
    }
  }
  return ret;
}

int ObAddRestoreSourceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAddRestoreSourceStmt *stmt = NULL;

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
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-sys tenant set add restore source is");
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

int ObClearRestoreSourceResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObClearRestoreSourceStmt *stmt = NULL;
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

int ObCheckpointSlogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CHECKPOINT_SLOG != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CHECKPOINT_SLOG", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should sepecify tenant", K(ret));
  } else if (OB_ISNULL(parse_tree.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should sepecify server", K(ret));
  } else {
    ObCheckpointSlogStmt *stmt = create_stmt<ObCheckpointSlogStmt>();
    if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObCheckpointSlogStmt failed");
    } else {
      stmt_ = stmt;
      if (OB_FAIL(Util::resolve_tenant_id(parse_tree.children_[0], stmt->tenant_id_))) {
        LOG_WARN("resolve tenant_id failed", K(ret));
      } else if (OB_FAIL(Util::resolve_server(parse_tree.children_[1], stmt->server_))) {
        LOG_WARN("resolve server failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_NOT_SUPPORTED;
  if (OB_UNLIKELY(T_RECOVER_TABLE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RECOVER_TABLE", "type", get_type_name(parse_tree.type_));
  } else if (OB_UNLIKELY(9 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else {
    ObRecoverTableStmt *stmt = create_stmt<ObRecoverTableStmt>();
    Worker::CompatMode compat_mode = Worker::CompatMode::INVALID;
    ObNameCaseMode case_mode = ObNameCaseMode::OB_NAME_CASE_INVALID;
    if (OB_ISNULL(stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRecoverTableStmt failed", K(ret));
    } else if (OB_FAIL(resolve_tenant_(
        parse_tree.children_[0], stmt->get_rpc_arg().tenant_id_, stmt->get_rpc_arg().tenant_name_, compat_mode, case_mode))) {
      LOG_WARN("failed to resolve tenant id", K(ret));
    } else if (OB_FAIL(ObRecoverTableUtil::check_compatible(stmt->get_rpc_arg().tenant_id_))) {
      LOG_WARN("check recover table compatible failed", K(ret));
    } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[1], stmt->get_rpc_arg().restore_tenant_arg_.uri_))) {
      LOG_WARN("failed to resolve backup dest", K(ret));
    } else if (OB_FAIL(resolve_scn_(parse_tree.children_[2], stmt->get_rpc_arg().restore_tenant_arg_))) {
      LOG_WARN("failed to resolve restore scn", K(ret));
    } else if (OB_FAIL(Util::resolve_string(parse_tree.children_[3], stmt->get_rpc_arg().restore_tenant_arg_.restore_option_))) {
      LOG_WARN("failed to resolve restore option", K(ret));
    } else if (OB_FAIL(resolve_recover_tables_(
        parse_tree.children_[4], compat_mode, case_mode, stmt->get_rpc_arg().import_arg_.get_import_table_arg()))) {
      LOG_WARN("failed to resolve recover table list", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[5])
        && OB_FAIL(Util::resolve_string(parse_tree.children_[5], stmt->get_rpc_arg().restore_tenant_arg_.encrypt_key_))) {
      LOG_WARN("failed to resolve encrypt key", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[6])) {
      ParseNode *kms_node = parse_tree.children_[6];
      if (2 != kms_node->num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("num of children not match", K(ret), "child_num", kms_node->num_child_);
      } else if (OB_ISNULL(kms_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("kms uri should not be NULL", K(ret));
      } else if (OB_FAIL(Util::resolve_string(kms_node->children_[0],
                                              stmt->get_rpc_arg().restore_tenant_arg_.kms_uri_))) {
        LOG_WARN("failed to resolve kms uri", K(ret));
      } else if (OB_NOT_NULL(kms_node->children_[1])
          && OB_FAIL(Util::resolve_string(kms_node->children_[1],
                                          stmt->get_rpc_arg().restore_tenant_arg_.kms_encrypt_key_))) {
        LOG_WARN("failed to resolve kms encrypt key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(parse_tree.children_[7])
        && OB_FAIL(resolve_remap_(parse_tree.children_[7], compat_mode, case_mode, stmt->get_rpc_arg().import_arg_.get_remap_table_arg()))) {
      LOG_WARN("failed to resolve remap", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[8])
        && OB_FAIL(Util::resolve_string(parse_tree.children_[8], stmt->get_rpc_arg().restore_tenant_arg_.description_))) {
      LOG_WARN("failed to resolve desc", K(ret));
#ifdef OB_BUILD_TDE_SECURITY
    } else if (OB_FAIL(resolve_kms_info_(
        stmt->get_rpc_arg().restore_tenant_arg_.restore_option_, stmt->get_rpc_arg().restore_tenant_arg_.kms_info_))) {
      LOG_WARN("failed to resolve kms info", K(ret));
#endif
    } else if (OB_FAIL(resolve_backup_set_pwd_(stmt->get_rpc_arg().restore_tenant_arg_.passwd_array_))) {
      LOG_WARN("failed to resolve backup set pwd", K(ret));
    } else if (OB_FAIL(resolve_restore_source_(stmt->get_rpc_arg().restore_tenant_arg_.multi_uri_))) {
      LOG_WARN("failed to resolve restore source", K(ret));
    }

    if (OB_SUCC(ret)) {
      stmt->get_rpc_arg().action_ = ObRecoverTableArg::INITIATE;
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_remap_(
    const ParseNode *node,
    const lib::Worker::CompatMode &compat_mode,
    const ObNameCaseMode &case_mode,
    share::ObImportRemapArg &remap_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node must not be null", K(ret));
  } else if (node->num_child_ > 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", node->num_child_);
  } else {
    bool parsed_remap_table = false;
    bool parsed_remap_tablegroup = false;
    bool parsed_remap_tablespace = false;
    const int DEFAULT_ERROR_MSG_LEN = 16;
    for (int64_t i = 0; i < node->num_child_ && OB_SUCC(ret); i++) {
      const ParseNode *child_node = node->children_[i];
      if (T_REMAP_TABLE == child_node->type_) {
        if (parsed_remap_table) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("duplicate remap table is not allowed", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "duplicate REMAP TABLE is");
        } else if (OB_FAIL(resolve_remap_tables_(child_node, compat_mode, case_mode, remap_arg))) {
          LOG_WARN("failed to resolve remap tables", K(ret));
        } else {
          parsed_remap_table = true;
        }
      } else if (T_REMAP_TABLEGROUP == child_node->type_) {
        if (parsed_remap_tablegroup) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("duplicate remap tablegroup is not allowed", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "duplicate REMAP TABLEGROUP is");
        } else if (OB_FAIL(resolve_remap_tablegroups_(child_node, remap_arg))) {
          LOG_WARN("failed to resolve remap tablegroups", K(ret));
        } else {
          parsed_remap_tablegroup = true;
        }
      } else if (T_REMAP_TABLESPACE == child_node->type_) {
        if (parsed_remap_tablespace) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("duplicate remap tablespace is not allowed", K(ret));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "duplicate REMAP TABLESPACE is");
        } else if (OB_FAIL(resolve_remap_tablespaces_(child_node, remap_arg))) {
          LOG_WARN("failed to resolve remap tablespaces", K(ret));
        } else {
          parsed_remap_tablespace = true;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid remap", K(ret));
      }
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_restore_source_(common::ObString &restore_source)
{
  int ret = OB_SUCCESS;
  ObObj value;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info must not be nullptr", K(ret));
  } else if (session_info_->user_variable_exists(OB_RESTORE_SOURCE_NAME_SESSION_STR)) {
    if (OB_FAIL(session_info_->get_user_variable_value(OB_RESTORE_SOURCE_NAME_SESSION_STR, value))) {
      LOG_WARN("failed to get user variable value", K(ret));
    } else {
      restore_source = value.get_char();
      LOG_INFO("succeed to resolve restore source", K(restore_source));
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_remap_tablespaces_(
    const ParseNode *node, share::ObImportRemapArg &remap_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {// no need to remap tablegroups
  } else if (OB_UNLIKELY(T_REMAP_TABLESPACE != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REMAP_TABLESPACE", "type", get_type_name(node->type_));
  } else {
    share::ObRemapTablespaceItem item;
    const ObNameCaseMode case_mode = OB_ORIGIN_AND_SENSITIVE;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ParseNode *remap_ts_node = node->children_[i];
      if (OB_ISNULL(remap_ts_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remap table group node must not be null", K(ret));
      } else if (OB_UNLIKELY(T_RELATION_FACTOR != remap_ts_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse node type", K(ret));
      } else if (OB_UNLIKELY(2 != remap_ts_node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child num", K(ret));
      } else {
        const ParseNode *src = remap_ts_node->children_[0];
        const ParseNode *dst = remap_ts_node->children_[1];
        if (OB_ISNULL(src) || OB_ISNULL(dst)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("remap tablegroup must not be null", K(ret));
        } else {
          ObString src_name(src->str_len_, src->str_value_), dst_name(dst->str_len_, dst->str_value_);
          item.reset();
          if (src_name.length() > OB_MAX_TABLESPACE_NAME_LENGTH || src_name.length() <= 0) {
            ret = OB_ERR_WRONG_VALUE;
            LOG_WARN("invalid src name or dst name", K(ret), K(src_name));
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "REMAP TABLESPACE", to_cstring(src_name));
          } else if (dst_name.length() > OB_MAX_TABLESPACE_NAME_LENGTH || dst_name.length() <= 0) {
            ret = OB_ERR_WRONG_VALUE;
            LOG_WARN("invalid src name or dst name", K(ret), K(dst_name));
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "REMAP TABLESPACE", to_cstring(dst_name));
          } else if (OB_FALSE_IT(item.src_.mode_ = case_mode)) {
          } else if (OB_FALSE_IT(item.target_.mode_ = case_mode)) {
          } else if (OB_FALSE_IT(item.src_.name_.assign_ptr(src_name.ptr(), src_name.length()))) {
          } else if (OB_FALSE_IT(item.target_.name_.assign_ptr(dst_name.ptr(), dst_name.length()))) {
            LOG_WARN("failed to assign", K(dst_name));
          } else if (OB_FAIL(remap_arg.add_remap_tablespace(item))) {
            LOG_WARN("failed to add remap tablespace", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_remap_tablegroups_(
    const ParseNode *node, share::ObImportRemapArg &remap_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {// no need to remap tablegroups
  } else if (OB_UNLIKELY(T_REMAP_TABLEGROUP != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REMAP_TABLEGROUP", "type", get_type_name(node->type_));
  } else {
    share::ObRemapTablegroupItem item;
    const ObNameCaseMode case_mode = OB_ORIGIN_AND_SENSITIVE;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ParseNode *remap_tg_node = node->children_[i];
      if (OB_ISNULL(remap_tg_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remap table group node must not be null", K(ret));
      } else if (OB_UNLIKELY(T_RELATION_FACTOR != remap_tg_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse node type", K(ret));
      } else if (OB_UNLIKELY(2 != remap_tg_node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child num", K(ret));
      } else {
        const ParseNode *src = remap_tg_node->children_[0];
        const ParseNode *dst = remap_tg_node->children_[1];
        if (OB_ISNULL(src) || OB_ISNULL(dst)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("remap tablegroup must not be null", K(ret));
        } else {
          ObString src_name(src->str_len_, src->str_value_), dst_name(dst->str_len_, dst->str_value_);
          item.reset();
          if (src_name.length() > OB_MAX_TABLEGROUP_NAME_LENGTH || src_name.length() <= 0) {
            ret = OB_ERR_WRONG_VALUE;
            LOG_WARN("invalid src name", K(ret), K(src_name));
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "REMAP TABLEGROUP", to_cstring(src_name));
          } else if (dst_name.length() > OB_MAX_TABLEGROUP_NAME_LENGTH || dst_name.length() <= 0) {
            ret = OB_ERR_WRONG_VALUE;
            LOG_WARN("invalid dst name", K(ret), K(dst_name));
            LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "REMAP TABLEGROUP", to_cstring(dst_name));
          } else if (OB_FALSE_IT(item.src_.mode_ = case_mode)) {
          } else if (OB_FALSE_IT(item.target_.mode_ = case_mode)) {
          } else if (OB_FALSE_IT(item.src_.name_.assign_ptr(src_name.ptr(), src_name.length()))) {
          } else if (OB_FALSE_IT(item.target_.name_.assign_ptr(dst_name.ptr(), dst_name.length()))) {
            LOG_WARN("failed to assign", K(dst_name));
          } else if (OB_FAIL(remap_arg.add_remap_tablegroup(item))) {
            LOG_WARN("failed to add remap tablegroup", K(ret), K(item));
          }
        }
      }
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_remap_tables_(
    const ParseNode *node, const lib::Worker::CompatMode &compat_mode, const ObNameCaseMode &case_mode,
    share::ObImportRemapArg &remap_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) { // no need to remap tables
  } else if (OB_UNLIKELY(T_REMAP_TABLE != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_REMAP_TABLE", "type", get_type_name(node->type_));
  } else {
    share::ObRemapDatabaseItem remap_db_item;
    share::ObRemapTableItem remap_table_item;
    ObCollationType cs_type = CS_TYPE_INVALID;
    bool perserve_lettercase = Worker::CompatMode::ORACLE == compat_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
    bool is_oracle_mode = Worker::CompatMode::ORACLE == compat_mode;
    // No matter what name case mode is of target tenant, the names of remap tables are case sensitive.
    const ObNameCaseMode sensitive_case_mode = OB_ORIGIN_AND_SENSITIVE;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      bool is_remap_db = false;

      remap_db_item.reset();
      remap_table_item.reset();
      remap_db_item.src_.mode_ = sensitive_case_mode;
      remap_db_item.target_.mode_ = sensitive_case_mode;
      remap_table_item.src_.mode_ = sensitive_case_mode;
      remap_table_item.target_.mode_ = sensitive_case_mode;
      const ParseNode *remap_table_node = node->children_[i];
      if (OB_ISNULL(remap_table_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remap table group node must not be null", K(ret));
      } else if (OB_UNLIKELY(T_RELATION_FACTOR != remap_table_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse node type", K(ret));
      } else if (5 == remap_table_node->num_child_) {
        // remap table
        const ParseNode *src_db_node = remap_table_node->children_[0];
        const ParseNode *src_tb_node = remap_table_node->children_[1];
        const ParseNode *src_pt_node = remap_table_node->children_[2];
        const ParseNode *dst_db_node = remap_table_node->children_[3];
        const ParseNode *dst_tb_node = remap_table_node->children_[4];

        ObString src_db_name, src_tb_name, src_pt_name, dst_db_name, dst_tb_name;

        if (OB_NOT_NULL(src_db_node) && OB_FALSE_IT(src_db_name.assign_ptr(src_db_node->str_value_, src_db_node->str_len_))) {
        } else if (OB_NOT_NULL(src_tb_node) && OB_FALSE_IT(src_tb_name.assign_ptr(src_tb_node->str_value_, src_tb_node->str_len_))) {
        } else if (OB_NOT_NULL(src_pt_node) && OB_FALSE_IT(src_pt_name.assign_ptr(src_pt_node->str_value_, src_pt_node->str_len_))) {
        } else if (OB_NOT_NULL(dst_db_node) && OB_FALSE_IT(dst_db_name.assign_ptr(dst_db_node->str_value_, dst_db_node->str_len_))) {
        } else if (OB_NOT_NULL(dst_tb_node) && OB_FALSE_IT(dst_tb_name.assign_ptr(dst_tb_node->str_value_, dst_tb_node->str_len_))) {
        }

        if (!src_db_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, src_db_name))) {
          LOG_WARN("failed to check and convert db name", K(ret), K(cs_type), K(perserve_lettercase), K(src_db_name));
        } else if (!src_tb_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, src_tb_name, is_oracle_mode))) {
          LOG_WARN("failed to check and convert table name", K(ret), K(cs_type), K(perserve_lettercase), K(src_tb_name));
        } else if (!dst_db_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, dst_db_name))) {
          LOG_WARN("failed to check and convert db name", K(ret), K(cs_type), K(perserve_lettercase), K(dst_db_name));
        } else if (!dst_tb_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, dst_tb_name, is_oracle_mode))) {
          LOG_WARN("failed to check and convert table name", K(ret), K(cs_type), K(perserve_lettercase), K(dst_tb_name));
        } else if (!src_pt_name.empty() && src_pt_name.length() > OB_MAX_PARTITION_NAME_LENGTH) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("invalid partition name", K(ret));
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "INVALID PARTITION NAME", to_cstring(src_pt_name));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_NOT_NULL(src_db_node) && OB_ISNULL(src_tb_node) && OB_ISNULL(src_pt_node) && OB_NOT_NULL(dst_db_node) && OB_ISNULL(dst_tb_node)) {
          // db_name.'*':new_db_name.'*';
          remap_db_item.src_.name_.assign_ptr(src_db_name.ptr(), src_db_name.length());
          remap_db_item.target_.name_.assign_ptr(dst_db_name.ptr(), dst_db_name.length());
          is_remap_db = true;
        } else if (OB_NOT_NULL(src_db_node) && OB_NOT_NULL(src_tb_node) && OB_ISNULL(src_pt_node) && OB_ISNULL(dst_db_node) && OB_NOT_NULL(dst_tb_node)) {
          // db_name.tb_name:new_tb_name;
          remap_table_item.src_.database_name_.assign_ptr(src_db_name.ptr(), src_db_name.length());
          remap_table_item.src_.table_name_.assign_ptr(src_tb_name.ptr(), src_tb_name.length());
          remap_table_item.target_.database_name_.assign_ptr(src_db_name.ptr(), src_db_name.length());
          remap_table_item.target_.table_name_.assign_ptr(dst_tb_name.ptr(), dst_tb_name.length());
        } else if (OB_NOT_NULL(src_db_node) && OB_NOT_NULL(src_tb_node) && OB_ISNULL(src_pt_node) && OB_NOT_NULL(dst_db_node) && OB_NOT_NULL(dst_tb_node)) {
          // db_name.tb_name:new_db_name.new_tb_name
          remap_table_item.src_.database_name_.assign_ptr(src_db_name.ptr(), src_db_name.length());
          remap_table_item.src_.table_name_.assign_ptr(src_tb_name.ptr(), src_tb_name.length());
          remap_table_item.target_.database_name_.assign_ptr(dst_db_name.ptr(), dst_db_name.length());
          remap_table_item.target_.table_name_.assign_ptr(dst_tb_name.ptr(), dst_tb_name.length());
        } else if (OB_NOT_NULL(src_db_node) && OB_NOT_NULL(src_tb_node) && OB_NOT_NULL(src_pt_node) && OB_ISNULL(dst_db_node) && OB_NOT_NULL(dst_tb_node)) {
          // db_name.tb_name:part_name:new_tb_name;
          int ret = OB_NOT_SUPPORTED;
          LOG_WARN("remap partition is not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "remap partition");
        } else if (OB_NOT_NULL(src_db_node) && OB_NOT_NULL(src_tb_node) && OB_NOT_NULL(src_pt_node) && OB_NOT_NULL(dst_db_node) && OB_NOT_NULL(dst_tb_node)) {
          int ret = OB_NOT_SUPPORTED;
          LOG_WARN("remap partition is not supported", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid remap tables", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid remap tables", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (!is_remap_db && OB_FAIL(remap_arg.add_remap_table(remap_table_item))) {
        LOG_WARN("fail to push backup", K(ret), K(remap_table_item));
      } else if (is_remap_db && OB_FAIL(remap_arg.add_remap_database(remap_db_item))) {
        LOG_WARN("fail to push backup", K(ret), K(remap_db_item));
      }
    }
  }
  return ret;
}

int ObRecoverTableResolver::resolve_backup_set_pwd_(common::ObString &pwd)
{
  int ret = OB_SUCCESS;
  ObObj value;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null" , K(ret));
  } else if (!session_info_->user_variable_exists(OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR)) {
    LOG_INFO("no decryption passwd is specified");
    pwd.reset();
  } else if (OB_FAIL(session_info_->get_user_variable_value(
      OB_BACKUP_DECRYPTION_PASSWD_ARRAY_SESSION_STR, value))) {
    LOG_WARN("fail to get user variable", K(ret));
  } else {
    pwd = value.get_varchar();
    LOG_INFO("succeed to resolve_decryption_passwd", "passwd", pwd);
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRecoverTableResolver::resolve_kms_info_(const common::ObString &restore_option, common::ObString &kms_info)
{
  int ret = OB_SUCCESS;
  const char *encrypt_option_str = "kms_encrypt=true";
  ObString kms_var("kms_encrypt_info");
  int64_t encrypt_opt_str_len = strlen(encrypt_option_str);
  bool is_kms_encrypt = false;
  for (int i = 0; i <= restore_option.length() - encrypt_opt_str_len; ++i) {
    if (0 == STRNCASECMP(restore_option.ptr() + i, encrypt_option_str, encrypt_opt_str_len)) {
      is_kms_encrypt = true;
      break;
    }
  }
  if (is_kms_encrypt) {
     ObObj value;
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null" , K(ret));
    } else if (OB_FAIL(session_info_->get_user_variable_value(kms_var, value))) {
      LOG_WARN("fail to get user variable", K(ret));
    } else {
      kms_info = value.get_varchar();
      if (kms_info.length() > 4000 || kms_info.length() < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("kms info is not valid", K(ret));
      }
    }
  }
  return ret;
}
#endif

int ObRecoverTableResolver::resolve_recover_tables_(
    const ParseNode *node, const lib::Worker::CompatMode &compat_mode, const ObNameCaseMode &case_mode,
    share::ObImportTableArg &import_arg)
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = CS_TYPE_INVALID;
  bool perserve_lettercase = Worker::CompatMode::ORACLE == compat_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
  bool is_oracle_mode = Worker::CompatMode::ORACLE == compat_mode;
  // No matter what name case mode is of target tenant, the names of recover tables are case sensitive.
    const ObNameCaseMode sensitive_case_mode = OB_ORIGIN_AND_SENSITIVE;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table list node must not be null", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get collation connection", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
    const ParseNode *table_relation_node = node->children_[i];
    if (OB_ISNULL(table_relation_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table relation node must not be null", K(ret));
    } else if (OB_UNLIKELY(T_RELATION_FACTOR != table_relation_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse node type", K(ret));
    } else if (3 == table_relation_node->num_child_) {
      const ParseNode *db_node = table_relation_node->children_[0];
      const ParseNode *tb_node = table_relation_node->children_[1];
      const ParseNode *pt_node = table_relation_node->children_[2];
      if (OB_ISNULL(db_node) && OB_ISNULL(tb_node) && OB_ISNULL(pt_node)) {
        // '*'.'*' recover all table of all all database.
        if (OB_FAIL(import_arg.set_import_all())) {
          LOG_WARN("failed to set import all", K(ret));
        }
      } else if (OB_NOT_NULL(db_node) && OB_ISNULL(tb_node) && OB_ISNULL(pt_node)) {
        // db_name.'*' recover all tables of db_name
        ObString db_name(db_node->str_len_, db_node->str_value_);
        share::ObImportDatabaseItem db_item(sensitive_case_mode, db_node->str_value_, db_node->str_len_);
        if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
          LOG_WARN("failed to check and convert db name", K(ret), K(cs_type), K(perserve_lettercase), K(db_name));
        } else if (OB_FAIL(import_arg.add_database(db_item))) {
          LOG_WARN("failed to add database", K(ret), K(db_item));
        }
      } else if (OB_NOT_NULL(db_node) && OB_NOT_NULL(tb_node) && OB_ISNULL(pt_node)) {
        // db_name.tb_name recover tb_name of db_name
        ObString db_name(db_node->str_len_, db_node->str_value_), tb_name(tb_node->str_len_, tb_node->str_value_);
        if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, tb_name, is_oracle_mode))) {
          LOG_WARN("failed to check and convert table name", K(ret), K(cs_type), K(perserve_lettercase), K(tb_name));
        } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
          LOG_WARN("failed to check and convert db name", K(ret), K(cs_type), K(perserve_lettercase), K(db_name));
        } else {
          share::ObImportTableItem table_item(sensitive_case_mode, db_name.ptr(), db_name.length(), tb_name.ptr(), tb_name.length());
          if (OB_FAIL(import_arg.add_table(table_item))) {
            LOG_WARN("failed to add table", K(ret), K(table_item));
          }
        }
      } else if (OB_NOT_NULL(db_node) && OB_NOT_NULL(tb_node) && OB_NOT_NULL(pt_node)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("recover partition is not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "recover partition");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid recover table list", K(ret));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "invalid recover table list");
      }
    } else if (5 == table_relation_node->num_child_) {
      const ParseNode *db_node = table_relation_node->children_[0];
      const ParseNode *tb_node = table_relation_node->children_[1];
      const ParseNode *pt_node = table_relation_node->children_[4];
      if (OB_NOT_NULL(db_node) && OB_NOT_NULL(tb_node) && OB_NOT_NULL(pt_node)) {
        // db_name.tb_name:pt_name reocver the partion of tb_name
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("recover partition is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "recover partition");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid recover table list", K(ret));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "invalid recover table list");
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid recover table list", K(ret));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "invalid recover table list");
    }
  }

  return ret;
}

int ObRecoverTableResolver::resolve_scn_(
    const ParseNode *node, obrpc::ObPhysicalRestoreTenantArg &arg)
{
  int ret = OB_SUCCESS;
  const ParseNode *time_node = nullptr;
  if (OB_ISNULL(node)) { //restore to latest, scn = 0;
  } else if (OB_FALSE_IT(time_node = node)) {
  } else if (OB_UNLIKELY(T_PHYSICAL_RESTORE_UNTIL != time_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_PHYSICAL_RESTORE_UNTIL", "type", get_type_name(time_node->type_));
  } else if (OB_UNLIKELY(2 != time_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of children not match", K(ret), "child_num", time_node->num_child_);
  } else if (0/*timestamp*/ == time_node->children_[0]->value_) {
    arg.restore_timestamp_.assign_ptr(time_node->children_[1]->str_value_, time_node->children_[1]->str_len_);
    arg.with_restore_scn_ = false;
  } else if (1/*scn*/ == time_node->children_[0]->value_) {
    if (share::OB_BASE_SCN_TS_NS >= time_node->children_[1]->value_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "until scn, it should be positive integer");
      LOG_WARN("until scn, it should be positive integer", KR(ret), K(time_node->children_[1]->value_));
    } else if (OB_FAIL(arg.restore_scn_.convert_for_sql(time_node->children_[1]->value_))) {
      LOG_WARN("failed to convert scn", K(ret));
    } else {
      arg.with_restore_scn_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("invalid until", K(ret));
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "invalid until type");
  }
  return ret;
}

int ObRecoverTableResolver::resolve_tenant_(
    const ParseNode *node, uint64_t &tenant_id, common::ObString &tenant_name, lib::Worker::CompatMode &compat_mode,
    ObNameCaseMode &case_mode)
{
  int ret = OB_SUCCESS;
  uint64_t session_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FALSE_IT(session_tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (is_sys_tenant(session_tenant_id) && OB_NOT_NULL(node)) {
    // System tenant initiates recover table.
    if (OB_UNLIKELY(T_TENANT_NAME != node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type is not T_TENANT_NAME", "type", get_type_name(node->type_));
    } else if (OB_UNLIKELY(1 != node->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant name node must not be null", K(ret));
    } else {
      ObSchemaGetterGuard schema_guard;
      ObAllTenantInfo tenant_info;
      ObString tmp_tenant_name(node->children_[0]->str_len_, node->children_[0]->str_value_);
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session_tenant_id, schema_guard))) {
        LOG_WARN("failed to get_tenant_schema_guard", KR(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_id(tmp_tenant_name, tenant_id))) {
        LOG_WARN("failed to get tenant id from schema guard", KR(ret), K(tmp_tenant_name));
      } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_, false/*for update*/, tenant_info))) {
        LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
      } else if (tenant_info.is_standby()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("dest tenant is standby", K(ret), "tenant_name", tmp_tenant_name);
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "recover table to standby tenant is");
      } else {
        tenant_name = tmp_tenant_name;
      }
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't initiate table recover", K(ret), K(session_tenant_id));
    if (is_sys_tenant(session_tenant_id)) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "no target tenant specified");
    } else {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "user tenant initiate recover table is");
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
      LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObImportTableUtil::get_tenant_name_case_mode(tenant_id, case_mode))) {
      LOG_WARN("failed to get name case mode", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRecoverTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  share::SCN recovery_until_scn = SCN::min_scn();
  obrpc::ObRecoverTenantArg::RecoverType type = obrpc::ObRecoverTenantArg::RecoverType::INVALID;
  ObString tenant_name;
  bool with_restore_scn = false;
  if (OB_UNLIKELY(T_RECOVER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_RECOVER", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of children not match", KR(ret), "child_num", parse_tree.num_child_);
  } else {
    ObRecoverTenantStmt *stmt = create_stmt<ObRecoverTenantStmt>();
    if (OB_ISNULL(stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObRecoverTenantStmt failed");
    } else {
      if (OB_FAIL(resolve_tenant_name(parse_tree.children_[0],
                                      session_info_->get_effective_tenant_id(), tenant_name))) {
        LOG_WARN("resolve_tenant_name", KR(ret), KP(parse_tree.children_[0]),
                                        K(session_info_->get_effective_tenant_id()), K(tenant_name));
      } else if (OB_ISNULL(parse_tree.children_[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "restore until, should specify restore until point");
        LOG_WARN("should specify restore until point", KR(ret));
      } else if (T_PHYSICAL_RESTORE_UNTIL == parse_tree.children_[1]->type_) {
        if (OB_FAIL(resolve_restore_until(*parse_tree.children_[1], session_info_, recovery_until_scn,
                                          with_restore_scn))) {
          LOG_WARN("failed to resolve restore until", KR(ret), KP(parse_tree.children_[1]),
                                                      K(recovery_until_scn), K(with_restore_scn));
        } else {
          type = obrpc::ObRecoverTenantArg::RecoverType::UNTIL;
        }
      } else if (T_RECOVER_UNLIMITED == parse_tree.children_[1]->type_) {
        //TODO(yaoying):need check  recovery_until_scn
        recovery_until_scn.set_max();
        type = obrpc::ObRecoverTenantArg::RecoverType::UNTIL;
      } else if (T_RECOVER_CANCEL == parse_tree.children_[1]->type_) {
        recovery_until_scn.set_min();
        type = obrpc::ObRecoverTenantArg::RecoverType::CANCEL;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node type", KR(ret), "type", get_type_name(parse_tree.children_[1]->type_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->get_rpc_arg().init(session_info_->get_effective_tenant_id(),
                                              tenant_name, type, recovery_until_scn))) {
        LOG_WARN("fail to init arg", KR(ret), K(stmt->get_rpc_arg()),
                 K(session_info_->get_effective_tenant_id()), K(tenant_name), K(type),
                 K(recovery_until_scn));
      } else {
        stmt_ = stmt;
      }
    }
  }
  return ret;
}

int resolve_restore_until(const ParseNode &time_node,
                          const ObSQLSessionInfo *session_info,
                          share::SCN &recovery_until_scn,
                          bool &with_restore_scn)
{
  int ret = OB_SUCCESS;
  recovery_until_scn.set_min();
  with_restore_scn = false;

  if (OB_UNLIKELY(T_PHYSICAL_RESTORE_UNTIL != time_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_PHYSICAL_RESTORE_UNTIL", "type", get_type_name(time_node.type_));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg should not be null", KP(session_info));
  } else if (OB_ISNULL(time_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_UNLIKELY(2 != time_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of children not match", K(ret), "child_num", time_node.num_child_);
  } else if (OB_ISNULL(time_node.children_[0]) || OB_ISNULL(time_node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", KP(time_node.children_[0]), KP(time_node.children_[1]));
  } else if (0/*timestamp*/ == time_node.children_[0]->value_) {
    uint64_t time_val = 0;
    ObString time_str;
    ObString sys_time_zone;
    ObTimeZoneInfoWrap tz_info_wrap;
    const ObTimeZoneInfo *time_zone_info = nullptr;
    const ObTimeZoneInfo *sys_tz_info = nullptr;
    if (OB_ISNULL(time_zone_info = session_info->get_timezone_info())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("time zone info is null", K(ret), KP(session_info), KP(time_zone_info));
    } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_SYSTEM_TIME_ZONE, sys_time_zone))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (OB_ISNULL(time_zone_info->get_tz_info_map())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", KP(time_zone_info->get_tz_info_map()));
    } else if (OB_FAIL(tz_info_wrap.init_time_zone(sys_time_zone, OB_INVALID_VERSION,
        *(const_cast<ObTZInfoMap *>(time_zone_info->get_tz_info_map()))))) {
      LOG_WARN("tz_info_wrap init_time_zone fail", KR(ret), K(sys_time_zone));
    } else if (OB_ISNULL(sys_tz_info = tz_info_wrap.get_time_zone_info())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sys time zone info is null", K(ret));
    } else if (OB_FAIL(ObAlterSystemResolverUtil::resolve_string(time_node.children_[1], time_str))) {
      LOG_WARN("resolve string failed", K(ret));
    } else if (OB_FAIL(ObTimeConverter::str_to_scn_value(time_str,
                                                         sys_tz_info,
                                                         time_zone_info,
                                                         ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT,
                                                         true/*oracle mode*/,
                                                         time_val))) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIMESTAMP", to_cstring(time_str));
    } else if (OB_FAIL(recovery_until_scn.convert_for_sql(time_val))) {
      LOG_WARN("fail to set scn", K(ret));
    } else {
      with_restore_scn = true;
    }
  } else if (1/*scn*/ == time_node.children_[0]->value_) {
    if (share::OB_BASE_SCN_TS_NS >= time_node.children_[1]->value_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "until scn, it should be positive integer");
        LOG_WARN("until scn, it should be positive integer", KR(ret), K(time_node.children_[1]->value_));
    } else if (OB_FAIL(recovery_until_scn.convert_for_sql(time_node.children_[1]->value_))) {
      LOG_WARN("fail to set scn", K(ret));
    } else {
      with_restore_scn = true;
    }
  }
  return ret;
}

//for mysql mode
int ObResetConfigResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_RESET_PARAMETER != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_ALTER_SYSTEM_RESET_PARAMETER", "type", get_type_name(parse_tree.type_));
  } else {
    if (OB_UNLIKELY(NULL == parse_tree.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children should not be null");
    } else {
      const ParseNode *list_node = parse_tree.children_[0];
      if (OB_UNLIKELY(NULL == list_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("list_node should not be null");
      } else if (OB_UNLIKELY(NULL == list_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null");
      } else {
        ObResetConfigStmt *stmt = create_stmt<ObResetConfigStmt>();
        if (OB_UNLIKELY(NULL == stmt)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("create stmt failed", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < list_node->num_child_; i++) {
            const ParseNode *action_node = list_node->children_[i];
            if (NULL == action_node) {
              continue;
            } else {
              HEAP_VAR(ObAdminSetConfigItem, item) {
                if (OB_LIKELY(NULL != session_info_)) {
                  item.exec_tenant_id_ = session_info_->get_effective_tenant_id();
                } else {
                  LOG_WARN("session is null");
                  item.exec_tenant_id_ = OB_INVALID_TENANT_ID;
                }
                if (OB_UNLIKELY(NULL == action_node->children_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("children should not be null");
                } else if (OB_UNLIKELY(NULL == action_node->children_[0])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("children[0] should not be null");
                } else {
                  // config name
                  ObString name(action_node->children_[0]->str_len_,
                                action_node->children_[0]->str_value_);
                  ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
                  if (OB_FAIL(item.name_.assign(name))) {
                    LOG_WARN("assign config name failed", K(name), K(ret));
                  } else {
                    ObConfigItem *ci = NULL;
                    ObConfigItem * const *sys_ci_ptr = NULL;
                    ObConfigItem * const *tenant_ci_ptr = NULL;
                    sys_ci_ptr = GCONF.get_container().get(ObConfigStringKey(item.name_.ptr()));
                    if (OB_NOT_NULL(sys_ci_ptr)) {
                      ci = *sys_ci_ptr;
                    } else {
                      int tmp_ret = OB_SUCCESS;
                      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
                      if (!tenant_config.is_valid()) {
                        tmp_ret = OB_ERR_UNEXPECTED;
                        LOG_WARN("failed to get tenant config", KR(tmp_ret));
                      } else if (OB_ISNULL(tenant_ci_ptr = (tenant_config->get_container().get(
                                                ObConfigStringKey(item.name_.ptr()))))) {
                        tmp_ret = OB_ERR_SYS_CONFIG_UNKNOWN;
                        LOG_WARN("can't found config item", KR(tmp_ret), "item", item);
                      } else {
                        ci = *tenant_ci_ptr;
                      }
                    }
                    if (OB_FAIL(ret)) {
                      LOG_WARN("error ret", KR(ret));
                    } else {
                      if (OB_NOT_NULL(ci)) {
                        if (OB_FAIL(item.value_.assign(ci->default_str()))) {
                          LOG_WARN("assign config value failed", K(ret));
                        } else {
                          //ignore config scope
                          //tenant
                          if (NULL != action_node->children_[1]) {
                            const ParseNode *n = action_node->children_[1];
                            if (T_TENANT_NAME == n->type_) {
                              uint64_t tenant_id = item.exec_tenant_id_;
                              if (OB_SYS_TENANT_ID != tenant_id) {
                                ret = OB_ERR_NO_PRIVILEGE;
                                LOG_WARN("non sys tenant", K(tenant_id), K(ret));
                              } else {
                                ObString tenant_name(n->children_[0]->str_len_,
                                                      n->children_[0]->str_value_);
                                if (OB_FAIL(item.tenant_name_.assign(tenant_name))) {
                                  LOG_WARN("assign tenant name failed", K(tenant_name), K(ret));
                                }
                              }
                            } else {
                              ret = OB_ERR_UNEXPECTED;
                              LOG_WARN("resolve tenant name failed", K(ret));
                            }
                          }
                          if (OB_SUCC(ret)) {
                            if (OB_FAIL(alter_system_set_reset_constraint_check_and_add_item_mysql_mode(stmt->get_rpc_arg(), item, session_info_))) {
                              LOG_WARN("constraint check failed", K(ret));
                            }
                          }
                        }
                      } else {
                        ret = OB_ERR_SYS_CONFIG_UNKNOWN;
                        LOG_WARN("unknown config", K(ret), K(item));
                      }
                    }
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

int resolve_part_info(const ParseNode &parse_node, uint64_t &table_id, ObObjectID &object_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  object_id = OB_INVALID_OBJECT_ID;
  if (OB_UNLIKELY(T_PARTITION_INFO != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_PARTITION_INFO", KR(ret), "type",
        get_type_name(parse_node.type_));
  } else if (2 != parse_node.num_child_
        || OB_ISNULL(parse_node.children_[0])
        || OB_ISNULL(parse_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", KR(ret), "num_child", parse_node.num_child_,
        KP(parse_node.children_[0]), KP(parse_node.children_[1]));
  } else {
    table_id = parse_node.children_[0]->value_;
    object_id = parse_node.children_[1]->value_;
  }
  return ret;
}

int ObAlterSystemResetResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  bool set_parameters = false;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_ALTER_SYSTEM_RESET != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ must be T_ALTER_SYSTEM_RESET", K(ret), K(parse_tree.type_));
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
      } else if (T_ALTER_SYSTEM_RESET_PARAMETER != set_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_RESET_PARAMETER", K(ret),
                 K(set_node->type_));
      } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_node is null", K(ret));
      } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
      } else {
        ParseNode *var = nullptr;
        if (OB_ISNULL(var = set_param_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var is NULL", K(ret));
        } else if (T_IDENT != var->type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "Variable name isn't identifier type");
        } else {
          ObString name(var->str_len_, var->str_value_);
          share::ObBackupConfigChecker backup_config_checker;
          bool is_backup_config = false;
          if (OB_FAIL(backup_config_checker.check_config_name(name, is_backup_config))) {
            LOG_WARN("fail to check config name", K(ret), K(name));
          } else if (is_backup_config) {
            set_parameters = true;
            break;
          } else {
            omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
            if (tenant_config.is_valid() &&
              nullptr != tenant_config->get_container().get(ObConfigStringKey(name))) {
              set_parameters = true;
              break;
            }
          }
        }
      }
    } // for
  }
  /* second round: gen stmt */
  if (OB_SUCC(ret)) {
    if (set_parameters) {
      ObSetConfigStmt *setconfig_stmt = create_stmt<ObSetConfigStmt>();
      if (OB_ISNULL(setconfig_stmt)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("create set config stmt failed", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
          ParseNode *set_node = nullptr, *set_param_node = nullptr;
          if (OB_ISNULL(set_node = parse_tree.children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node should not be null", K(ret));
          } else if (T_ALTER_SYSTEM_RESET_PARAMETER != set_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node->type_ must be T_ALTER_SYSTEM_RESET_PARAMETER",
                      K(ret), K(set_node->type_));
          } else if (OB_ISNULL(set_param_node = set_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("set_node is null", K(ret));
          } else if (OB_UNLIKELY(T_VAR_VAL != set_param_node->type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is not T_VAR_VAL", K(ret), K(set_param_node->type_));
          } else {
            ParseNode *name_node = nullptr, *value_node = nullptr;
            HEAP_VAR(ObAdminSetConfigItem, item) {
              item.exec_tenant_id_ = tenant_id;
              /* name */
              if (OB_ISNULL(name_node = set_param_node->children_[0])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("var is NULL", K(ret));
              } else if (T_IDENT != name_node->type_) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "Variable name isn't identifier type");
              } else {
                ObString name(name_node->str_len_, name_node->str_value_);
                ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
                if (OB_FAIL(item.name_.assign(name))) {
                  LOG_WARN("assign config name failed", K(name), K(ret));
                }
              }
              if (OB_FAIL(ret)) {
                continue;
              }
              //value
              ObConfigItem *ci = NULL;
              ObConfigItem * const *tenant_ci_ptr = NULL;
              {
                int tmp_ret = OB_SUCCESS;
                omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
                if (!tenant_config.is_valid()) {
                  tmp_ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("failed to get tenant config", KR(tmp_ret));
                } else if (OB_ISNULL(tenant_ci_ptr = (tenant_config->get_container().get(
                                          ObConfigStringKey(item.name_.ptr()))))) {
                  tmp_ret = OB_ERR_SYS_CONFIG_UNKNOWN;
                  LOG_WARN("can't found config item", KR(tmp_ret), "item", item);
                } else {
                  ci = *tenant_ci_ptr;
                }
              }
              if (OB_FAIL(ret)) {
                LOG_WARN("error ret", KR(ret));
              } else {
                if (OB_NOT_NULL(ci)) {
                  if (OB_FAIL(item.value_.assign(ci->default_str()))) {
                    LOG_WARN("assign config value failed", K(ret));
                  }
                } else {
                  ret = OB_ERR_SYS_CONFIG_UNKNOWN;
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(alter_system_set_reset_constraint_check_and_add_item_oracle_mode(
                    setconfig_stmt->get_rpc_arg(), item, tenant_id, schema_checker_))) {
                  LOG_WARN("constraint check failed", KR(ret));
                }
              }
            }
          }
        } // for
      }
    } else {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WARN("variables do not support reset or unknown config item", KR(ret));
    }
    /* 无论设置租户级配置项，还是系统参数，都需要alter system权限。
       对租户级配置项的修改，算是一种扩展，借用alter system权限进行控制 */
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_ALTER_SYSTEM_RESET_PARAMETER,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  } // if
  return ret;
}

int ObCancelCloneResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCancelCloneStmt *mystmt = NULL;
  ObString tenant_name;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_UNLIKELY(T_CANCEL_CLONE != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_UNLIKELY(1 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret));
  } else {
    bool is_compatible = false;
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    if (OB_FAIL(share::ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
                    tenant_id, is_compatible))) {
      LOG_WARN("fail to check compat version", KR(ret), K(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is below 4.3", KR(ret), K(tenant_id), K(is_compatible));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cancel tenant cloning below 4.3");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(mystmt = create_stmt<ObCancelCloneStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt", KR(ret));
    } else {
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else {
      tenant_name.assign_ptr((char *)(node->children_[0]->str_value_),
                                  static_cast<int32_t>(node->children_[0]->str_len_));
      if (OB_FAIL(mystmt->set_clone_tenant_name(tenant_name))) {
        LOG_WARN("set clone tenant name failed", KR(ret), K(tenant_name));
      }
    }
  }
  return ret;
}

int resolve_transfer_partition_to_ls(
    const ParseNode &parse_node,
    const uint64_t target_tenant_id,
    const uint64_t exec_tenant_id,
    ObTransferPartitionStmt *stmt)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObObjectID object_id = OB_INVALID_OBJECT_ID;
  if (OB_UNLIKELY(T_TRANSFER_PARTITION_TO_LS != parse_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION_TO_LS", KR(ret), "type",
        get_type_name(parse_node.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id) || !is_valid_tenant_id(target_tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exec_tenant_id or target_tenant_id", KR(ret), K(exec_tenant_id), K(target_tenant_id));
  } else if (2 != parse_node.num_child_
        || OB_ISNULL(parse_node.children_[0])
        || OB_ISNULL(parse_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", KR(ret), "num_child", parse_node.num_child_,
        KP(parse_node.children_[0]), KP(parse_node.children_[1]));
  } else if (OB_FAIL(resolve_part_info(*parse_node.children_[0], table_id, object_id))) {
    LOG_WARN("fail to resolve partition info", KR(ret), KP(parse_node.children_[0]));
  } else {
    int64_t id = parse_node.children_[1]->value_;
    ObLSID ls_id(id);
    if (OB_FAIL(stmt->get_arg().init_for_transfer_partition_to_ls(
        target_tenant_id,
        table_id,
        object_id,
        ls_id))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id),
          K(table_id), K(object_id), K(ls_id));
    }
  }
  return ret;
}

int get_and_verify_tenant_name(
    const ParseNode *parse_node,
    const uint64_t exec_tenant_id,
    uint64_t &target_tenant_id,
    const char * const op_str)
{
  int ret = OB_SUCCESS;
  ObString tenant_name;
  ObSchemaGetterGuard schema_guard;
  int tmp_ret = OB_SUCCESS;
  const int64_t COMMENT_LENGTH = 512;
  char comment[COMMENT_LENGTH] = {0};
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(exec_tenant_id))
      || OB_ISNULL(op_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec tenant id is invalid", KR(ret), K(exec_tenant_id), K(op_str));
  } else if (NULL == parse_node) {
    if (OB_SYS_TENANT_ID != exec_tenant_id) {
      target_tenant_id = exec_tenant_id;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("no specified tenant in the sys tenant's session", KR(ret));
      if (OB_TMP_FAIL(databuff_printf(comment, COMMENT_LENGTH, pos,
              "%s of SYS tenant is", op_str))) {
        LOG_WARN("failed to printf to comment", KR(ret), KR(tmp_ret), K(op_str));
      } else {
        LOG_USER_ERROR(OB_NOT_SUPPORTED, comment);
      }
    }
  } else if (OB_FAIL(resolve_tenant_name(parse_node, exec_tenant_id, tenant_name))) {
    LOG_WARN("fail to resolve target tenant id", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get_tenant_schema_guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, target_tenant_id))) {
    LOG_WARN("failed to get tenant id from schema guard", KR(ret), K(tenant_name));
    if (OB_TENANT_NOT_EXIST == ret || OB_ERR_INVALID_TENANT_NAME == ret) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
    }
  } else if (OB_UNLIKELY(!is_user_tenant(target_tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support user tenant", KR(ret), K(target_tenant_id));
    if (OB_TMP_FAIL(databuff_printf(comment, COMMENT_LENGTH, pos,
            "%s of META or SYS tenant is", op_str))) {
      LOG_WARN("failed to printf to comment", KR(ret), KR(tmp_ret), K(op_str));
    } else {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, comment);
    }
  } else if (OB_SYS_TENANT_ID != exec_tenant_id && target_tenant_id != exec_tenant_id) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("no support operating other user tenants", KR(ret), K(target_tenant_id), K(exec_tenant_id));
  }
  return ret;
}

int ObTransferPartitionResolver::resolve_transfer_partition_(const ParseNode &parse_tree)
{
int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_TRANSFER_PARTITION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_ || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse node is null", KR(ret),  KP(parse_tree.children_[0]));
  } else if (OB_FAIL(get_and_verify_tenant_name(
      parse_tree.children_[1],
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Transfer partition"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else {
    ParseNode *transfer_partition_node = parse_tree.children_[0];
    switch(transfer_partition_node->type_) {
      case T_TRANSFER_PARTITION_TO_LS:
        if (OB_FAIL(resolve_transfer_partition_to_ls(
            *transfer_partition_node,
            target_tenant_id,
            session_info_->get_effective_tenant_id(),
            stmt))) {
          LOG_WARN("fail to resolve transfer_partition_to_ls", KR(ret), K(target_tenant_id), K(session_info_->get_effective_tenant_id()));
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid transfer partition node type", KR(ret), "type", get_type_name(transfer_partition_node->type_));
    }
    if (OB_SUCC(ret)) {
      stmt_ = stmt;
    }
  }
  return ret;
}
int ObTransferPartitionResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (T_TRANSFER_PARTITION == parse_tree.type_) {
    if (OB_FAIL(resolve_transfer_partition_(parse_tree))) {
      LOG_WARN("failed to reslove transfer partition", KR(ret));
    }
  } else if (T_CANCEL_TRANSFER_PARTITION == parse_tree.type_) {
    if (OB_FAIL(resolve_cancel_transfer_partition_(parse_tree))) {
      LOG_WARN("failed to resolve cancel transfer partition", KR(ret));
    }
  } else if (T_CANCEL_BALANCE_JOB == parse_tree.type_) {
    if (OB_FAIL(resolve_cancel_balance_job_(parse_tree))) {
      LOG_WARN("failed to resolve cancel balance job", KR(ret));
    }
  } else  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
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

int ObTransferPartitionResolver::resolve_cancel_transfer_partition_(const ParseNode &parse_tree)
{
int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_CANCEL_TRANSFER_PARTITION != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_ || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse node is null", KR(ret),  KP(parse_tree.children_[0]));
  } else if (OB_FAIL(get_and_verify_tenant_name(
      parse_tree.children_[1],
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Cancel transfer partition"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else {
    ParseNode *transfer_partition_node = parse_tree.children_[0];
    uint64_t table_id = OB_INVALID_ID;
    ObObjectID object_id = OB_INVALID_OBJECT_ID;
    rootserver::ObTransferPartitionArg::ObTransferPartitionType type = rootserver::ObTransferPartitionArg::INVALID_TYPE;
    if (T_PARTITION_INFO == transfer_partition_node->type_) {
      type = rootserver::ObTransferPartitionArg::CANCEL_TRANSFER_PARTITION;
      if (OB_FAIL(resolve_part_info(*transfer_partition_node, table_id, object_id))) {
        LOG_WARN("failed to resolve part info", KR(ret));
      }
    } else if (T_ALL == transfer_partition_node->type_) {
      type = rootserver::ObTransferPartitionArg::CANCEL_TRANSFER_PARTITION_ALL;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid transfer partition node type", KR(ret), "type", get_type_name(transfer_partition_node->type_));
    }
    if (FAILEDx(stmt->get_arg().init_for_cancel_transfer_partition(
            target_tenant_id, type, table_id, object_id))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id), K(type),
          K(table_id), K(object_id));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}

int ObTransferPartitionResolver::resolve_cancel_balance_job_(const ParseNode &parse_tree)
{
int ret = OB_SUCCESS;
  ObTransferPartitionStmt *stmt = create_stmt<ObTransferPartitionStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_CANCEL_BALANCE_JOB != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_TRANSFER_PARTITION", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (1 != parse_tree.num_child_
        || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(session_info_));
  } else if (OB_FAIL(get_and_verify_tenant_name(
      parse_tree.children_[0],
      session_info_->get_effective_tenant_id(),
      target_tenant_id, "Cancel balance job"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[0]));
  } else if (OB_FAIL(stmt->get_arg().init_for_cancel_balance_job(
            target_tenant_id))) {
      LOG_WARN("fail to init stmt rpc arg", KR(ret), K(target_tenant_id));
  } else {
    stmt_ = stmt;
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
