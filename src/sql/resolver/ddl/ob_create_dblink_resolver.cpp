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
#include "sql/resolver/ddl/ob_create_dblink_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_dblink_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObCreateDbLinkResolver::ObCreateDbLinkResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateDbLinkResolver::~ObCreateDbLinkResolver()
{
}

int ObCreateDbLinkResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateDbLinkStmt *create_dblink_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_DBLINK)
      || OB_UNLIKELY(node->num_child_ != DBLINK_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), KP(node), K(node->type_), K(T_CREATE_DBLINK), K(node->num_child_));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(create_dblink_stmt = create_stmt<ObCreateDbLinkStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_dblink_stmt", K(ret));
  } else {
    stmt_ = create_dblink_stmt;
    create_dblink_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    create_dblink_stmt->set_user_id(session_info_->get_user_id());
    LOG_TRACE("debug dblink create", K(session_info_->get_database_id()));
  }
  if (!lib::is_oracle_mode() && OB_SUCC(ret)) {
    uint64_t compat_version = 0;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (compat_version < DATA_VERSION_4_2_0_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
    } else if (NULL != node->children_[IF_NOT_EXIST]) {
      if (T_IF_NOT_EXISTS != node->children_[IF_NOT_EXIST]->type_) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument.",
                      K(ret), K(node->children_[1]->type_));
      } else {
        create_dblink_stmt->set_if_not_exist(true);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString dblink_name;
    ParseNode *name_node = node->children_[DBLINK_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (name_node->str_len_ >= OB_MAX_DBLINK_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
    } else if (FALSE_IT(dblink_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(create_dblink_stmt->set_dblink_name(dblink_name))) {
      LOG_WARN("set dblink name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString cluster_name;
    ParseNode *name_node = node->children_[OPT_CLUSTER];
    if (!OB_ISNULL(name_node)) {
      cluster_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
    }
    if (OB_FAIL(create_dblink_stmt->set_cluster_name(cluster_name))) {
      LOG_WARN("set cluster name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString tenant_name;
    ParseNode *name_node = node->children_[TENANT_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(tenant_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(create_dblink_stmt->set_tenant_name(tenant_name))) {
      LOG_WARN("set tenant name failed", K(ret));
    }
  }
  if (!lib::is_oracle_mode() && OB_SUCC(ret)) {
    ObString database_name;
    ParseNode *name_node = node->children_[DATABASE_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(database_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(create_dblink_stmt->set_database_name(database_name))) {
      LOG_WARN("set database name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString user_name;
    ParseNode *name_node = node->children_[USER_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(user_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(create_dblink_stmt->set_user_name(user_name))) {
      LOG_WARN("set user name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString password;
    ParseNode *pwd_node = node->children_[PASSWORD];
    if (OB_ISNULL(pwd_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(password.assign_ptr(pwd_node->str_value_, static_cast<int32_t>(pwd_node->str_len_)))) {
      // do nothing
    } else if (password.empty()) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_MISSING_OR_INVALID_PASSWORD;
        LOG_USER_ERROR(OB_ERR_MISSING_OR_INVALID_PASSWORD);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create dblink with empty password");
      }
      LOG_WARN("create dblink with empty password", K(ret));
    } else if (OB_FAIL(create_dblink_stmt->set_password(password))) {
      LOG_WARN("set password failed", K(ret));
    }
  }
  DriverType drv_type = DRV_OB;
  if (OB_SUCC(ret)) {
    ParseNode *driver = node->children_[OPT_DRIVER];
    if (OB_ISNULL(driver)) {
      create_dblink_stmt->set_driver_proto(0); // default proto is ob.
    } else {
      drv_type = static_cast<DriverType>(driver->int16_values_[0]);
      if (DRV_OB == drv_type) {
        create_dblink_stmt->set_driver_proto(0); // ob.
      } else if (DRV_OCI == drv_type) {
        create_dblink_stmt->set_driver_proto(1); // oci.
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected driver type", K(drv_type));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObAddr host;
    ObString host_str;
    ObString ip_port, conn_str;
    ParseNode *host_node = NULL;
    if (OB_ISNULL(node->children_[IP_PORT]) || OB_ISNULL(node->children_[IP_PORT]->children_)
        || OB_ISNULL(host_node = node->children_[IP_PORT]->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(host_str.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_)))) {
    } else if (OB_FAIL(cut_host_string(host_str, ip_port, conn_str))) {
      LOG_WARN("failed to cut host string", K(ret), K(host_str));
    } else if (OB_FAIL(host.parse_from_string(ip_port))) {
      LOG_WARN("parse ip port failed", K(ret), K(host_str), K(ip_port));
    } else if (OB_FAIL(resolve_conn_string(conn_str, ip_port, *create_dblink_stmt))) {
      LOG_WARN("failed to resolve conn string", K(ret), K(conn_str));
    } else {
      create_dblink_stmt->set_host_addr(host);
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (lib::is_oracle_mode() && OB_FAIL(resolve_opt_reverse_link(node, create_dblink_stmt, drv_type))) {
    LOG_WARN("failed to resolve optional reverse link", K(ret));
  } else if (ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_CREATE_DBLINK,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }
  LOG_INFO("resolve create dblink finish", K(ret));
  return ret;
}

int ObCreateDbLinkResolver::resolve_opt_reverse_link(const ParseNode *node, sql::ObCreateDbLinkStmt *link_stmt, DriverType drv_type)
{
  int ret = OB_SUCCESS;
  ParseNode *reverse_link_node = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(link_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(node), K(link_stmt), K(ret));
  } else if (FALSE_IT(reverse_link_node = node->children_[OPT_REVERSE_LINK])) {
  } else if (OB_ISNULL(reverse_link_node)) {
    // nothing to do
  } else if (DRV_OB != drv_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only DBLINK_DRV_OB support reverse link", K(ret), K(drv_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "option reverse link");
  } else if (T_REVERSE_DBLINK != reverse_link_node->type_ ||
              REVERSE_DBLINK_NODE_COUNT != reverse_link_node->num_child_ ||
              OB_ISNULL(reverse_link_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(T_REVERSE_DBLINK), K(reverse_link_node->type_), K(reverse_link_node->num_child_));
  } else {
    ObString reverse_user_name;
    ParseNode *name_node = reverse_link_node->children_[REVERSE_LINK_USER_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(reverse_user_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      // do nothing
    } else {
      link_stmt->set_reverse_user_name(reverse_user_name);
    }

    ObString reverse_tenant_name;
    ParseNode *tenant_node = reverse_link_node->children_[REVERSE_LINK_TENANT_NAME];
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tenant_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(reverse_tenant_name.assign_ptr(tenant_node->str_value_, static_cast<int32_t>(tenant_node->str_len_)))) {
      // do nothing
    } else {
      link_stmt->set_reverse_tenant_name(reverse_tenant_name);
    }

    ObString reverse_link_password;
    ParseNode *pwd_node = reverse_link_node->children_[REVERSE_LINK_PASSWORD];
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(pwd_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(reverse_link_password.assign_ptr(pwd_node->str_value_, static_cast<int32_t>(pwd_node->str_len_)))) {
      // do nothing
    } else {
      link_stmt->set_reverse_password(reverse_link_password);
    }

    ObString reverse_link_cluster_name;
    ParseNode *cluster_node = reverse_link_node->children_[REVERSE_LINK_OPT_CLUSTER];
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cluster_node)) {
      // do nothing
    } else if (FALSE_IT(reverse_link_cluster_name.assign_ptr(cluster_node->str_value_, static_cast<int32_t>(cluster_node->str_len_)))) {
      // do nothing
    } else {
      link_stmt->set_reverse_cluster_name(reverse_link_cluster_name);
    }

    ObAddr host;
    ObString host_str;
    ObString ip_port, conn_str;
    ParseNode *host_node = NULL;
    if (OB_ISNULL(reverse_link_node->children_[REVERSE_LINK_IP_PORT]) || OB_ISNULL(reverse_link_node->children_[REVERSE_LINK_IP_PORT]->children_) || OB_ISNULL(host_node = reverse_link_node->children_[REVERSE_LINK_IP_PORT]->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(host_str.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_)))) {
    } else if (OB_FAIL(cut_host_string(host_str, ip_port, conn_str))) {
      LOG_WARN("failed to cut host string", K(ret), K(host_str));
    } else if (OB_FAIL(host.parse_from_string(ip_port))) {
      LOG_WARN("parse ip port failed", K(ret), K(host_str), K(ip_port));
    } else {
      link_stmt->set_reverse_host_addr(host);
    }
  }
  return ret;
}

int ObCreateDbLinkResolver::cut_host_string(const ObString &host_string,
                                            ObString &ip_port,
                                            ObString &conn_string)
{
  // ip:port/driver_name/sid -> ip_port, conn_string
  int ret = OB_SUCCESS;
  ObString tmp_str(host_string.length(), host_string.ptr());
  if (tmp_str.empty()) {
    // do nothing
  } else {
    ip_port = tmp_str.split_on('/');
    if (ip_port.empty()) {
      ip_port = tmp_str;
    } else {
      conn_string = tmp_str;
    }
  }
  return ret;
}

int ObCreateDbLinkResolver::resolve_conn_string(const ObString &conn_string,
                                                const ObString &ip_port_str,
                                                ObCreateDbLinkStmt &link_stmt)
{
  // something like : /sid
  int ret = OB_SUCCESS;
  DriverType drv_type = static_cast<DriverType>(link_stmt.get_driver_proto());
  if (DRV_OCI == drv_type) {
    // format conn_string
    ObSqlString cstr;
    if (OB_FAIL(cstr.append("//"))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(cstr.append(ip_port_str))) {
      LOG_WARN("failed to append", K(ret), K(ip_port_str));
    } else if (OB_FAIL(cstr.append("/"))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(cstr.append(conn_string))) {
      LOG_WARN("failed to append", K(ret), K(conn_string));
    } else if (OB_FAIL(link_stmt.set_conn_string(cstr.string()))) {
      LOG_WARN("failed to append", K(ret), K(cstr));
    } else if (OB_FAIL(link_stmt.set_service_name(conn_string))) {
      LOG_WARN("failed to append", K(ret), K(conn_string));
    } else {
      // do nothing
    }
  } else if (DRV_OB == drv_type) {
    link_stmt.set_driver_proto(0); // default proto is ob.
    if (OB_FAIL(link_stmt.set_conn_string(conn_string))) {
      LOG_WARN("failed to set link connect string", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid database link syntax", K(ret), K(conn_string));
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
