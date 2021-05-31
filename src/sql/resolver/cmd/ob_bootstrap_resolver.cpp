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
#include "sql/parser/ob_item_type.h"
#include "share/ob_zone_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
ObBootstrapResolver::ObBootstrapResolver(ObResolverParams& params) : ObSystemCmdResolver(params)
{}

ObBootstrapResolver::~ObBootstrapResolver()
{}

int ObBootstrapResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ObBootstrapStmt* bootstrap_stmt = NULL;
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
    ParseNode* server_infos = parse_tree.children_[0];
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
      obrpc::ObServerInfoList& server_list = bootstrap_stmt->get_server_info_list();
      for (int64_t i = 0; OB_SUCC(ret) && i < server_infos->num_child_; ++i) {
        ParseNode* server_info_node = server_infos->children_[i];
        if (OB_ISNULL(server_info_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_info_node is NULL", K(ret));
        } else if (OB_UNLIKELY(T_SERVER_INFO != server_info_node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_info_node type is not T_SERVER_INFO", K(server_info_node->type_), K(ret));
        } else if (OB_ISNULL(server_info_node->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first child of server_info_node is NULL", K(ret));
        } else if (OB_ISNULL(server_info_node->children_[2])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("secord child of server_info_node is NULL", K(ret));
        } else {
          ObString zone_name;
          ObString zone_str;
          ObString region_name;
          ObString region_str;
          ObString ip_port;
          obrpc::ObServerInfo server_info;

          if (NULL == server_info_node->children_[0]) {
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
          } else {
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ParseNode* cluster_type_node = parse_tree.children_[1];
    ObClusterType type = PRIMARY_CLUSTER;
    if (OB_ISNULL(cluster_type_node)) {
      // nothing todo
    } else if (T_CLUSTER_TYPE != cluster_type_node->type_ || 1 != cluster_type_node->num_child_ ||
               OB_ISNULL(cluster_type_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node", K(ret), K(cluster_type_node));
    } else {
      LOG_INFO("resolve cluster type", K(type));
      if (0 == cluster_type_node->children_[0]->value_) {
        type = PRIMARY_CLUSTER;
      } else {
        type = STANDBY_CLUSTER;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      bootstrap_stmt->set_cluster_type(type);
    }
  }

  if (OB_SUCC(ret)) {
    ParseNode* primary_rootservice_node = parse_tree.children_[2];
    if (PRIMARY_CLUSTER == bootstrap_stmt->get_cluster_type()) {
      if (OB_ISNULL(primary_rootservice_node)) {
        // nothing todo
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("parse error, primary cluster should not have primary_rootservice_node", KR(ret));
      }
    } else if (STANDBY_CLUSTER == bootstrap_stmt->get_cluster_type()) {
      ObString primary_rootservice_list;
      ObString primary_rootservice_list_str;
      ObArray<ObString> addr_string;
      ObArray<ObAddr> addr_array;
      int64_t primary_cluster_id = OB_INVALID_ID;
      if (OB_ISNULL(primary_rootservice_node) || 2 != primary_rootservice_node->num_child_ ||
          OB_ISNULL(primary_rootservice_node->children_[0]) || OB_ISNULL(primary_rootservice_node->children_[1])) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("parse error, standby cluster should have primary_rootservice_list node", KR(ret));
      } else {
        primary_cluster_id = primary_rootservice_node->children_[0]->value_;
        primary_rootservice_list_str.assign_ptr(primary_rootservice_node->children_[1]->str_value_,
            static_cast<int32_t>(primary_rootservice_node->children_[1]->str_len_));
        if (OB_FAIL(ob_write_string(*allocator_, primary_rootservice_list_str, primary_rootservice_list))) {
          LOG_WARN("fail to write string", KR(ret));
        } else {
          ObString trimed_string = primary_rootservice_list.trim();
          if (OB_FAIL(split_on(trimed_string, ',', addr_string))) {
            LOG_WARN("fail to split string", KR(ret), K(trimed_string));
          } else {
            for (int64_t i = 0; i < addr_string.count() && OB_SUCC(ret); i++) {
              ObAddr addr;
              if (OB_FAIL(addr.parse_from_string(addr_string.at(i)))) {
                LOG_WARN("fail to parse from string", KR(ret), K(i), "string", addr_string.at(i));
              } else if (OB_FAIL(addr_array.push_back(addr))) {
                LOG_WARN("fail to push back", KR(ret), K(addr));
              }
            }  // end for
          }    // end else
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(bootstrap_stmt->set_primary_rs_list(addr_array))) {
          LOG_WARN("fail to set primary rs list", KR(ret));
        } else {
          bootstrap_stmt->set_primary_cluster_id(primary_cluster_id);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid cluster type", KR(ret), "cluster_type", bootstrap_stmt->get_cluster_type());
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
