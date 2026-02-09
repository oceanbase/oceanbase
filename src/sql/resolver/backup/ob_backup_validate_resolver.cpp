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

#include "sql/resolver/backup/ob_backup_validate_resolver.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_connectivity.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "lib/utility/utility.h"

#define USING_LOG_PREFIX SQL_RESV
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{
int ObBackupValidateResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  int32_t num_child = 0;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();
  ObBackupValidateStmt *stmt = create_stmt<ObBackupValidateStmt>();
  if (OB_UNLIKELY(T_BACKUP_VALIDATE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_VALIDATE_BACKUP", "type", get_type_name(parse_tree.type_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(cur_tenant_id, data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("BACKUP_VALIDATE command is not supported in data version less than 4.5.1", KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "BACKUP_VALIDATE command is not supported in data version less than 4.5.1");
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_FALSE_IT(num_child = parse_tree.num_child_)) {
  } else if (OB_UNLIKELY(num_child < 3 || num_child > 6)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", KR(ret), K(num_child));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("validate_type should not be null", KR(ret));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create validate backup set stmt failed", KR(ret));
  } else if (OB_FAIL(parse_and_set_stmt_(parse_tree, cur_tenant_id, stmt))) {
    LOG_WARN("fail to parse and set stmt", KR(ret), K(cur_tenant_id));
  } else {
    stmt_ = stmt;
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    if (OB_ISNULL(schema_checker_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(schema_checker_->check_ora_ddl_priv(
        session_info_->get_effective_tenant_id(),
        session_info_->get_priv_user_id(),
        ObString(""),
        stmt::T_BACKUP_VALIDATE,
        session_info_->get_enable_role_array()))) {
      LOG_WARN("failed to check privilege", K(session_info_->get_effective_tenant_id()), K(session_info_->get_user_id()));
    }
  }

  return ret;
}

int ObBackupValidateResolver::parse_and_set_stmt_(
    const ParseNode &parse_tree,
    const uint64_t tenant_id,
    ObBackupValidateStmt *stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t validate_type_value = parse_tree.children_[0]->value_;
  ParseNode *validate_level_node = parse_tree.children_[1];
  ParseNode *description_node = parse_tree.children_[2];
  int32_t num_child = parse_tree.num_child_;

  common::ObSArray<uint64_t> execute_tenant_ids;
  ObBackupPathString backup_dest;
  ObBackupDescription backup_description;
  share::ObBackupValidateLevel validate_level;
  validate_level.level_ = share::ObBackupValidateLevel::ValidateLevel::PHYSICAL;
  common::ObSArray<uint64_t> set_or_piece_ids;

  if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create validate backup set stmt failed", KR(ret));
  } else if (OB_NOT_NULL(validate_level_node) && OB_FAIL(validate_level.set(validate_level_node->str_value_))) {
    LOG_WARN("failed to set validate level", KR(ret));
  } else if (OB_NOT_NULL(description_node) && OB_FAIL(backup_description.assign(description_node->str_value_))) {
    LOG_WARN("failed to assign backup_description", KR(ret));
  }

  if (OB_SUCC(ret) && num_child > 4) {
    // may has set_or_piece_ids_node and dest
    ParseNode *set_or_piece_ids_node = parse_tree.children_[3];
    ParseNode *dest_node = parse_tree.children_[4];
    if (OB_NOT_NULL(dest_node) && OB_FAIL(get_dest_(*dest_node, validate_type_value, backup_dest))) {
      LOG_WARN("failed to assign backup dest", KR(ret));
    } else if (OB_NOT_NULL(set_or_piece_ids_node)
                  && OB_FAIL(get_set_or_piece_ids_(set_or_piece_ids_node->str_value_, set_or_piece_ids))) {
      LOG_WARN("failed to get set or piece ids", KR(ret));
    }
  }

  if (OB_SUCC(ret) && (4 == num_child || 6 == num_child)) {
    int tenant_list_pos = num_child - 1;
    ParseNode *execute_tenant_list_node = parse_tree.children_[tenant_list_pos];
    if (OB_NOT_NULL(execute_tenant_list_node)) {
      if (OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("user tenant cannot specify tenant names", KR(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify tenant under user tenant");
      } else if (OB_FAIL(ObAlterSystemResolverUtil::get_tenant_ids(*execute_tenant_list_node, execute_tenant_ids))) {
        LOG_WARN("failed to get tenant ids", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (static_cast<uint64_t>(share::ObBackupValidateType::ValidateType::DATABASE) == validate_type_value
              && !set_or_piece_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("validate type database can not set set or piece ids",
              KR(ret), K(set_or_piece_ids), K(validate_type_value));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate database with set set or piece ids");
  } else if (!backup_dest.is_empty() && OB_SYS_TENANT_ID == tenant_id && execute_tenant_ids.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("When validating data for a specified path, a unique user tenant must also be specified.",
              KR(ret), K(execute_tenant_ids), K(backup_dest));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                    "validate data for a specified path: A unique user tenant must also be specified.");
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt->set_param(tenant_id, validate_type_value, set_or_piece_ids, validate_level,
                                      backup_dest, backup_description, execute_tenant_ids))) {
      LOG_WARN("failed to init stmt", KR(ret), K(tenant_id), K(validate_type_value), K(set_or_piece_ids),
                K(validate_level), K(backup_dest), K(backup_description), K(execute_tenant_ids));
  }

  return ret;
}

//set_or_piece_ids_str = '1,2,3,4'
int ObBackupValidateResolver::get_set_or_piece_ids_(
  const char* set_or_piece_ids_str,
  ObIArray<uint64_t> &set_or_piece_ids)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_SQL_STRING_LEN = 512;
  if (OB_ISNULL(set_or_piece_ids_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set or piece ids string is null", KR(ret));
  } else {
    char tmp[MAX_SQL_STRING_LEN] = {0};
    char *token = NULL;
    char *saved_ptr = NULL;

    int64_t ids_len = strlen(set_or_piece_ids_str);
    if (ids_len >= MAX_SQL_STRING_LEN) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("set or piece ids string is too long", KR(ret), K(ids_len));
    } else {
      MEMCPY(tmp, set_or_piece_ids_str, ids_len);
      tmp[ids_len] = '\0';

      token = tmp;
      for (char *str = token; OB_SUCC(ret); str = NULL) {
        token = ::strtok_r(str, ",", &saved_ptr);
        if (NULL == token) {
          break;
        } else {
          uint64_t id = 0;
          if (OB_FAIL(ob_atoull(token, id))) {
            LOG_WARN("failed to convert string to uint64_t", KR(ret), K(token));
          } else if (id == 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("id is invalid", KR(ret), K(id));
          } else if (OB_FAIL(set_or_piece_ids.push_back(id))) {
            LOG_WARN("failed to add id to array", KR(ret), K(id));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "check set/piece ids string");
  }

  return ret;
}

int ObBackupValidateResolver::get_dest_(
    const ParseNode &path_node,
    const uint64_t validate_type_value,
    share::ObBackupPathString &backup_dest_str)
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
        if (static_cast<uint64_t>(share::ObBackupValidateType::ValidateType::BACKUPSET) != validate_type_value) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only validate type backupset can set data_backup_dest", KR(ret), K(validate_type_value));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "only validate type backupset can set data_backup_dest");
        }
      } else if (0 == strcasecmp(dest_name, OB_STR_LOG_ARCHIVE_DEST)) {
        if (static_cast<uint64_t>(share::ObBackupValidateType::ValidateType::ARCHIVELOG_PIECE) != validate_type_value) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only validate type archivelog_piece can set log_archive_dest", KR(ret), K(validate_type_value));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "only validate type archivelog_piece can set log_archive_dest");
        }
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
  LOG_INFO("get the backup_dest type and path finished", K(ret), K(backup_dest_str));
  return ret;
}

}//namespace sql
}//namespace oceanbase