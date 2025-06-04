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
#include "sql/resolver/cmd/ob_bootstrap_resolver.h"
#include "sql/resolver/cmd/ob_bootstrap_stmt.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObBootstrapResolver::ObBootstrapResolver(ObResolverParams &params) : ObSystemCmdResolver(params)
{
}

ObBootstrapResolver::~ObBootstrapResolver()
{
}

int ObBootstrapResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObBootstrapStmt *bootstrap_stmt = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is NULL", K(ret));
  } else if (OB_UNLIKELY(T_BOOTSTRAP != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type_ of parse_tree is not T_BOOTSTRAP", K(parse_tree.type_), K(ret));
  } else if (OB_UNLIKELY(NULL == (bootstrap_stmt = create_stmt<ObBootstrapStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create ObBootstrapStmt failed", K(ret));
  } else {
    stmt_ = bootstrap_stmt;
  }
  if (OB_SUCC(ret)) {
    ParseNode *server_infos = parse_tree.children_[0];
    if (OB_ISNULL(server_infos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_infos is NULL", K(ret));
    } else if (OB_UNLIKELY(T_SERVER_INFO_LIST != server_infos->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_infos type is not T_SERVER_INFO_LIST", K(server_infos->type_), K(ret));
    } else if (OB_ISNULL(server_infos->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_infos children is NULL", K(ret));
    } else {
      obrpc::ObServerInfoList &server_list = bootstrap_stmt->get_server_info_list();
      for (int64_t i = 0; OB_SUCC(ret) && i < server_infos->num_child_; ++i) {
        ParseNode *server_info_node = server_infos->children_[i];
        if (OB_ISNULL(server_info_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_info_node is NULL", K(ret));
        } else if (OB_UNLIKELY(T_SERVER_INFO != server_info_node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_info_node type is not T_SERVER_INFO", K(server_info_node->type_), K(ret));
        } else if (OB_ISNULL(server_info_node->children_[1]) ) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first child of server_info_node is NULL", K(ret));
        } else if (OB_ISNULL(server_info_node->children_[2]) ) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("secord child of server_info_node is NULL", K(ret));
        } else {
          ObString zone_name;
          ObString zone_str;
          ObString region_name;
          ObString region_str;
          ObString ip_port;
          obrpc::ObServerInfo server_info;

          if (NULL == server_info_node->children_[0] ) {
            // ob 1.3 版本新增 REGION，方便工具渐进升级起见允许不指定REGION
            region_str.assign_ptr(DEFAULT_REGION_NAME, static_cast<int32_t>(strlen(DEFAULT_REGION_NAME)));
          } else {
            region_str.assign_ptr(server_info_node->children_[0]->str_value_,
                                  static_cast<int32_t>(server_info_node->children_[0]->str_len_));
          }
          zone_str.assign_ptr(server_info_node->children_[1]->str_value_,
                              static_cast<int32_t>(server_info_node->children_[1]->str_len_));
          ip_port.assign_ptr(server_info_node->children_[2]->str_value_,
                             static_cast<int32_t>(server_info_node->children_[2]->str_len_));
          if (OB_FAIL(ob_write_string(*allocator_, region_str, region_name))) {
            LOG_WARN("write region name failed", K(region_str), K(ret));
          } else if (OB_FAIL(server_info.region_.assign(region_name))) {
            LOG_WARN("assign region name failed", K(region_str), K(region_name), K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, zone_str, zone_name))) {
            LOG_WARN("write zone name failed", K(zone_str), K(ret));
          } else if (OB_FAIL(server_info.zone_.assign(zone_name))) {
            LOG_WARN("assign zone name failed", K(zone_str), K(zone_name), K(ret));
          } else if (OB_FAIL(server_info.server_.parse_from_string(ip_port))) {
            LOG_WARN("Invalid server address", "ip:port", ip_port, K(ret));
          } else if (OB_FAIL(server_list.push_back(server_info))) {
            LOG_WARN("push back server info failed", K(ret));
          } else {}
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ParseNode *bootstrap_info_node = parse_tree.children_[1];
    if (NULL == bootstrap_info_node) {
      LOG_INFO("bootstrap_info_node is NULL", KR(ret));
    } else if (0 > bootstrap_info_node->num_child_ || 2 < bootstrap_info_node->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid number of children", K(bootstrap_info_node->num_child_), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < bootstrap_info_node->num_child_; ++i) {
        ParseNode *iter_node = bootstrap_info_node->children_[i];
        if (OB_ISNULL(iter_node)) { // do nothing
        } else if (OB_FAIL(do_resolve_bootstrap_info_(*iter_node, bootstrap_stmt))) {
          LOG_WARN("resolve bootstrap info failed", K(ret));
        } else { } // do nothing
      }
    }
  }
  return ret;
}

int ObBootstrapResolver::do_resolve_bootstrap_info_(
  const ParseNode &parse_tree,
  ObBootstrapStmt* bootstrap_stmt)
{
  int ret = OB_SUCCESS;
  if (parse_tree.type_ == T_LOGSERVICE_ACCESS_POINT) {
    ret = do_resolve_logservice_access_point_(parse_tree, bootstrap_stmt);
  } else if (parse_tree.type_ == T_SHARED_STORAGE_INFO) {
    ret = do_resolve_shared_storage_info_(parse_tree, bootstrap_stmt);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ is invalid", K(parse_tree.type_));
  }
  return ret;
}

int ObBootstrapResolver::do_resolve_logservice_access_point_(
  const ParseNode &parse_tree,
  ObBootstrapStmt* bootstrap_stmt)
{
  int ret = OB_SUCCESS;
  if (parse_tree.type_ != T_LOGSERVICE_ACCESS_POINT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ is not T_LOGSERVICE_ACCESS_POINT", K(parse_tree.type_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is NULL", KR(ret));
  } else {
    ObString logservice_access_point;
    ObString logservice_access_point_str;
    logservice_access_point_str.assign_ptr(parse_tree.str_value_,
        static_cast<int32_t>(parse_tree.str_len_));
    if (OB_FAIL(ob_write_string(*allocator_, logservice_access_point_str,
                                logservice_access_point))) {
      LOG_WARN("write logservice access point failed", KR(ret), K(logservice_access_point_str));
    } else {
      bootstrap_stmt->set_logservice_access_point(logservice_access_point);
    }
  }
  return ret;
}

int ObBootstrapResolver::do_resolve_shared_storage_info_(
  const ParseNode &parse_tree,
  ObBootstrapStmt* bootstrap_stmt)
{
  int ret = OB_SUCCESS;
  if (parse_tree.type_ != T_SHARED_STORAGE_INFO) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree.type_ is not T_SHARED_STORAGE_INFO", K(parse_tree.type_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is NULL", KR(ret));
  } else {
    ObString shared_storage_info;
    ObString shared_storage_str;
    shared_storage_str.assign_ptr(parse_tree.str_value_,
        static_cast<int32_t>(parse_tree.str_len_));
    if (OB_FAIL(ob_write_string(*allocator_, shared_storage_str, shared_storage_info))) {
      LOG_WARN("write shared storage info failed", KR(ret), K(shared_storage_str));
    } else {
      bootstrap_stmt->set_shared_storage_info(shared_storage_info);
    }
  }
  return ret;
}


}// namespace sql
}// namespace oceanbase
