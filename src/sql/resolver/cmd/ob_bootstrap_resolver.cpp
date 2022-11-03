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
#include "objit/common/ob_item_type.h"
#include "share/ob_zone_info.h"

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

  return ret;
}
}// namespace sql
}// namespace oceanbase
