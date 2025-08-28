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

#include "sql/resolver/cmd/ob_backup_clean_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql
{
int ObAlterSystemResolverBackupCleanUtil::get_set_or_piece_ids(const ParseNode &id_node, ObIArray<int64_t> &backup_ids)
{
  int ret = OB_SUCCESS;
  const ParseNode *cur = &id_node;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur)) {
    if (T_INT == cur->type_) {
      if (OB_FAIL(backup_ids.push_back(cur->value_))) {
        LOG_WARN("failed to push id", K(ret), "id", cur->value_);
      }
      cur = NULL;
    } else if (T_LINK_NODE == cur->type_) {
      if (OB_ISNULL(cur->children_)
          || cur->num_child_ < 2
          || OB_ISNULL(cur->children_[1])
          || T_INT != cur->children_[1]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid T_LINK_NODE structure", K(ret));
      } else if (OB_FAIL(backup_ids.push_back(cur->children_[1]->value_))) {
        LOG_WARN("failed to push id", K(ret), "id", cur->children_[1]->value_);
      } else {
        cur = cur->children_[0];
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node type in id list", K(ret), "node_type", cur->type_);
    }
  }
  // Ensure the backup_ids count is not more than OB_MAX_BACKUP_DELETE_IDS_COUNT
  if (OB_SUCC(ret) && backup_ids.count() > OB_MAX_BACKUP_DELETE_IDS_COUNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("too many ids in the list", K(ret), "ids_num", backup_ids.count());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the number of ids to be deleted more than 32");
  }
  return ret;
}

int ObAlterSystemResolverBackupCleanUtil::get_clean_all_path_and_type(const ParseNode &path_node,
                                                           share::ObBackupPathString &backup_dest_str,
                                                           share::ObBackupDestType::TYPE &dest_type)
{
  int ret = OB_SUCCESS;
  if (T_LINK_NODE == path_node.type_
      && 1 == path_node.num_child_
      && NULL != path_node.children_[0]) {
    const char *dest_name = path_node.str_value_;
    int64_t dest_name_len = path_node.str_len_;
    if (OB_ISNULL(dest_name) || dest_name_len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup_dest name is null or empty", K(ret));
    } else {
      if (0 == strcasecmp(dest_name, OB_STR_DATA_BACKUP_DEST)) {
        dest_type = share::ObBackupDestType::DEST_TYPE_BACKUP_DATA;
      } else if (0 == strcasecmp(dest_name, OB_STR_LOG_ARCHIVE_DEST)) {
        dest_type = share::ObBackupDestType::DEST_TYPE_ARCHIVE_LOG;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Syntax error: expected 'data_backup_dest' or 'log_archive_dest', got", K(dest_name));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "Syntax error: expected 'data_backup_dest' or 'log_archive_dest'");
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(backup_dest_str.assign(path_node.children_[0]->str_value_))) {
        LOG_WARN("failed to assign backup_dest_str", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node structure for clean all path and type", K(ret),
              "node_type", path_node.type_, "num_child", path_node.num_child_);
  }
  LOG_INFO("get the backup_dest type and path finished", K(ret), K(backup_dest_str), K(dest_type));
  return ret;
}


int ObAlterSystemResolverBackupCleanUtil::handle_backup_clean_parameters(
    const ParseNode &parse_tree,
    share::ObNewBackupCleanType::TYPE clean_type,
    common::ObIArray<int64_t> &value,
    ObBackupPathString &backup_dest_str,
    share::ObBackupDestType::TYPE &dest_type)
{
  int ret = OB_SUCCESS;
  switch (clean_type) {
  case share::ObNewBackupCleanType::TYPE::CANCEL_DELETE: {
      // cancel has one parameter always be 0
      if (NULL == parse_tree.children_[1] || T_INT != parse_tree.children_[1]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the node is not a T_INT node", K(ret), "children_num", parse_tree.num_child_,
                "first_child_type", parse_tree.children_[1]->type_);
      } else if (OB_FAIL(value.push_back(parse_tree.children_[1]->value_))) {
        LOG_WARN("failed to push back value", K(ret), "value", parse_tree.children_[1]->value_);
      }
      break;
  }
  case share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_SET:
  case share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_PIECE: {
      if (NULL == parse_tree.children_[1]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the set_id or piece_id list node is null or missing", K(ret),
                  "children_num", parse_tree.num_child_, "first_child_type", parse_tree.children_[1]->type_);
      } else if (OB_FAIL(get_set_or_piece_ids(*parse_tree.children_[1], value))) {
        LOG_WARN("failed to get set or piece ids", K(ret), K(clean_type));
      }
      break;
  }
  case share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_ALL: {
      if (NULL == parse_tree.children_[1]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the dest_path node is null or not a string node", K(ret));
      } else if (OB_FAIL(get_clean_all_path_and_type(
                              *parse_tree.children_[1], backup_dest_str, dest_type))) {
        LOG_WARN("failed to get clean all path and type", K(ret));
      }
      break;
  }
  case share::ObNewBackupCleanType::TYPE::DELETE_OBSOLETE_BACKUP: {
      break;
  }
  default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported function ", K(ret), K(clean_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "function is");
      break;
  }
  }
  return ret;
}

int ObBackupCleanResolver::resolve(const ParseNode &parse_tree)
{
  // parse_tree.children_[0]: type (an INT node representing the clean operation type).
  // parse_tree.children_[1]: value, can be a single T_INT or a T_LINK_NODE representing a list IDs(in delete set and piece)
  //                                 and be a single T_INT representing the dest_id (in delete backup all)
  //                                 and be a single T_INT (always be 0 in cancel delete)
  // parse_tree.children_[2]: description (opt_description from grammar, can be NULL).
  // parse_tree.children_[3]: opt_backup_tenant_list
  int ret = OB_SUCCESS;
  LOG_INFO("ObBackupCleanResolver::resolve", K(parse_tree.type_), K(parse_tree.num_child_));
  if (OB_UNLIKELY(T_BACKUP_CLEAN != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_BACKUP_CLEAN", "type", parse_tree.type_);
  } else if (OB_UNLIKELY(NULL == parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_UNLIKELY(4 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    const int64_t type = parse_tree.children_[0]->value_;
    share::ObNewBackupCleanType::TYPE clean_type = static_cast<share::ObNewBackupCleanType::TYPE>(type);
    common::ObSArray<int64_t> value;
    common::ObSArray<uint64_t> clean_tenant_ids;
    ParseNode *t_node = parse_tree.children_[3];
    ObBackupDescription description;
    ObBackupPathString backup_dest_str;
    share::ObBackupDestType::TYPE dest_type = share::ObBackupDestType::DEST_TYPE_MAX;
    ObBackupCleanStmt *stmt = create_stmt<ObBackupCleanStmt>();

    // check if the clean type is supported, and get the parameters
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
      // before 4.4.1, only CANCEL_DELETE is supported
      if (share::ObNewBackupCleanType::TYPE::CANCEL_DELETE != clean_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("manual backup clean is not supported under cluster version 4_4_1_0",
                  K(ret), "cluster_version", GET_MIN_CLUSTER_VERSION());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "manual backup clean");
      }
    }

    // get clean parameters and tenant ids if needed
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAlterSystemResolverBackupCleanUtil::handle_backup_clean_parameters(
                              parse_tree, clean_type, value, backup_dest_str, dest_type))) {
      LOG_WARN("failed to handle backup clean parameters", K(ret), K(clean_type));
    } else if (NULL != parse_tree.children_[2]
              && OB_FAIL(description.assign(parse_tree.children_[2]->str_value_))) {
      LOG_WARN("failed to assign description", K(ret));
    } else if (NULL != t_node && OB_SYS_TENANT_ID != tenant_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("user tenant cannot specify tenant names", K(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    } else if (NULL != t_node && OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(ObAlterSystemResolverUtil::get_tenant_ids(*t_node, clean_tenant_ids))) {
        LOG_WARN("failed to get tenant ids", K(ret));
      } else if (clean_tenant_ids.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("No valid tenant name given", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "No valid tenant name given");
      } else if (clean_tenant_ids.count() > 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To initiate cleanup set or piece under the system tenant, only one tenant name can be specified",
                  K(ret), K(tenant_id), K(clean_tenant_ids));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Too many tenant names specified, only one tenant name can be specified");
      }
    } else if (NULL == t_node && OB_SYS_TENANT_ID == tenant_id) {
      if (share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_SET == clean_type
          || share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_PIECE == clean_type
          || share::ObNewBackupCleanType::TYPE::DELETE_BACKUP_ALL == clean_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To initiate cleanup set or piece or delete backup all under the system tenant, need to specify one tenant name",
                  K(ret), K(tenant_id), K(clean_tenant_ids));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "need to specify a tenant name");
      }
    }

    // set stmt parameters
    if (OB_FAIL(ret)) {
    } else if (NULL == stmt) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create ObBackupCleanResolver failed");
    } else if (OB_FAIL(stmt->set_param(tenant_id, type, value, description, clean_tenant_ids, backup_dest_str, dest_type))) {
      LOG_WARN("Failed to set param", K(ret), K(tenant_id), K(type), K(value), K(backup_dest_str), K(dest_type));
    } else {
      LOG_INFO("ObBackupCleanResolver::resolve done",
              K(ret), K(tenant_id), K(type), K(value), K(clean_tenant_ids), K(backup_dest_str), K(dest_type));
      stmt_ = stmt;
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
