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
#include "sql/resolver/backup/ob_backup_change_external_storage_dest_resolver.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_connectivity.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

int ObChangeExternalStorageDestResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("change external storage dest is not supported under cluster version 4_2_5_2 ", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change external storage dest is");
  } else if (OB_UNLIKELY(T_CHANGE_EXTERNAL_STORAGE_DEST != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_CHANGE_EXTERNAL_STORAGE_DEST", "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null");
  } else if (OB_UNLIKELY(3 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match", K(ret), "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(parse_tree.children_[0]) || (OB_ISNULL(parse_tree.children_[1]) && OB_ISNULL(parse_tree.children_[2])) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret), "children", parse_tree.children_);
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObChangeExternalStorageDestStmt *stmt = create_stmt<ObChangeExternalStorageDestStmt>();
    if (OB_ISNULL(stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create stmt failed", K(ret));
    } else {
      // path
      if (OB_SUCC(ret)) {
        HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
          item.exec_tenant_id_ = session_info_->get_effective_tenant_id();
          const ObString path_value = parse_tree.children_[0]->str_value_;
          if (OB_FAIL(item.name_.assign("path"))) {
            LOG_WARN("failed to assign attribute", K(ret));
          } else if (OB_FAIL(item.value_.assign(path_value))) {
            LOG_WARN("failed to assign config value", K(ret));
          } else if (OB_FAIL(stmt->get_rpc_arg().items_.push_back(item))) {
            LOG_WARN("add config item failed", K(ret), K(item));
          }
        }
      }
      if (OB_SUCC(ret)) {
        HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
          item.exec_tenant_id_ = session_info_->get_effective_tenant_id();
          if (OB_FAIL(item.name_.assign("access_info"))) {
            LOG_WARN("failed to assign attribute", K(ret));
          }
          // access info may be null
          if (OB_SUCC(ret) && OB_NOT_NULL(parse_tree.children_[1])) {
            const ObString access_info_value = parse_tree.children_[1]->str_value_;
            if (OB_FAIL(item.value_.assign(access_info_value))) {
              LOG_WARN("failed to assign config value", K(ret));
            }
          }
          if (FAILEDx(stmt->get_rpc_arg().items_.push_back(item))) {
            LOG_WARN("add config item failed", K(ret), K(item));
          }
        }
      }
      if (OB_SUCC(ret)) {
        HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
          item.exec_tenant_id_ = session_info_->get_effective_tenant_id();
          if (OB_FAIL(item.name_.assign("attribute"))) {
            LOG_WARN("failed to assign attribute", K(ret));
          }
          // attribute may be null
          if (OB_SUCC(ret) && OB_NOT_NULL(parse_tree.children_[2])) {
            const ObString attribute_value = parse_tree.children_[2]->str_value_;
            if (OB_FAIL(item.value_.assign(attribute_value))) {
              LOG_WARN("failed to assign config value", K(ret));
            }
          }
          if (FAILEDx(stmt->get_rpc_arg().items_.push_back(item))) {
            LOG_WARN("add config item failed", K(ret), K(item));
          }
        }
      }
    }
  }

  return ret;
}

}//namespace sql
}//namespace oceanbase