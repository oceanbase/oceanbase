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
#include "share/ob_define.h"
#include "sql/resolver/ddl/ob_flashback_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
/**
 * flashback table
 */
int ObFlashBackTableFromRecyclebinResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObFlashBackTableFromRecyclebinStmt *flashback_table_from_recyclebin_stmt = NULL;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else if (T_FLASHBACK_TABLE_FROM_RECYCLEBIN != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(parser_tree.type_));
  }
  //create flashback table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (flashback_table_from_recyclebin_stmt = create_stmt<ObFlashBackTableFromRecyclebinStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create rename table stmt", K(ret));
    } else {
      stmt_ = flashback_table_from_recyclebin_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    flashback_table_from_recyclebin_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    //flashback table
    ParseNode *table_node = parser_tree.children_[ORIGIN_TABLE_NODE];
    ObString origin_table_name;
    ObString origin_db_name;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(table_node,
                                                   origin_table_name,
                                                   origin_db_name,
                                                   true /*get origin db_name*/))) {
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else {
      OX (flashback_table_from_recyclebin_stmt->set_origin_table_name(origin_table_name));
      OX (flashback_table_from_recyclebin_stmt->set_origin_table_id(OB_INVALID_ID));
    }
    
    if (OB_SUCC(ret)) {
      //rename to new table_name
      ParseNode *rename_node = parser_tree.children_[NEW_TABLE_NODE];
      if (NULL != rename_node) {
        ObString new_table_name;
        ObString new_db_name;
        if (OB_FAIL(resolve_table_relation_node(rename_node,
                                                new_table_name,
                                                new_db_name))) {
          LOG_WARN("failed to resolve_table_relation_node", K(ret));
        } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == new_db_name
                   || ObString(OB_PUBLIC_SCHEMA_NAME) == new_db_name) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't not flashback table to recyclebin database", K(ret));
        } else {
          flashback_table_from_recyclebin_stmt->set_new_db_name(new_db_name);
          flashback_table_from_recyclebin_stmt->set_new_table_name(new_table_name);
        }
      }
    }

    // 现在支持了用原表名 flashback 回收站中的表
    // 复用一个以前 unused 的 origin_db_name, 指定需要 flashback 的表是从哪个库删除的
    if (OB_SUCC(ret)) {
      if (origin_db_name.empty()) {
        if (OB_ISNULL(session_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session_info_ is null", K(ret));
        } else if (OB_UNLIKELY(session_info_->get_database_name().empty())) {
          ret = OB_ERR_NO_DB_SELECTED;
          LOG_WARN("database not specified", K(ret));
        } else {
          flashback_table_from_recyclebin_stmt->set_origin_db_name(
              session_info_->get_database_name());
        }
      } else {
        flashback_table_from_recyclebin_stmt->set_origin_db_name(origin_db_name);
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
                            session_info_->get_effective_tenant_id(),
                            session_info_->get_priv_user_id(),
                            origin_db_name,
                            stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN,
                            session_info_->get_enable_role_array()));
    }
  }
  return ret;
}

int ObFlashBackTableToScnResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlashBackTableToScnStmt *stmt = nullptr;
  if (OB_ISNULL(session_info_) ||
      (T_FLASHBACK_TABLE_TO_TIMESTAMP != parse_tree.type_ && T_FLASHBACK_TABLE_TO_SCN != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (nullptr == (stmt = create_stmt<ObFlashBackTableToScnStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create flsahbck stmt", K(ret));
  } else {
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    stmt->set_tenant_id(tenant_id);
    obrpc::ObFlashBackTableToScnArg &arg = stmt->flashback_table_to_scn_arg_;
    ParseNode *table_node = parse_tree.children_[TABLE_NODES];
    ObString db_name;
    ObString table_name;
    obrpc::ObTableItem table_item;
    if (nullptr == table_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node is null", K(ret));
    } else {
      int64_t table_num = parse_tree.children_[TABLE_NODES]->num_child_;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_num; i++) {
        ParseNode *node = parse_tree.children_[TABLE_NODES]->children_[i];
        if (nullptr == node) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table node is null", K(ret));
        } else if (OB_FAIL(resolve_table_relation_node(node, table_name, db_name))) {
          LOG_WARN("failed to resolve table relaiton node", K(ret));
        } else if (OB_FAIL(session_info_->get_name_case_mode(table_item.mode_))) {
          LOG_WARN("failed to get name case mode", K(ret));
        } else {
          const share::schema::ObTableSchema *table_schema = NULL;
          table_item.database_name_ = db_name;
          table_item.table_name_ = table_name;
          OZ (arg.tables_.push_back(table_item));
          OZ (schema_checker_->get_table_schema(tenant_id,
                                                db_name,
                                                table_name,
                                                false, /*is_index*/
                                                table_schema));
          if (OB_SUCC(ret) && table_schema == NULL) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table_shema is null", K(ret), K(db_name), K(table_name));
          }
          if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
            OZ (schema_checker_->check_ora_ddl_priv(
                  tenant_id,
                  session_info_->get_priv_user_id(),
                  db_name,
                  table_schema->get_table_id(),
                  static_cast<uint64_t>(share::schema::ObObjectType::TABLE),
                  stmt::T_FLASHBACK_TABLE_TO_SCN,
                  session_info_->get_enable_role_array()));
          }
        }
      }
    } 

    if (OB_SUCC(ret)) {
      ParseNode *time_node = parse_tree.children_[TIME_NODE];
      ObRawExpr *expr = nullptr;
      if (nullptr == time_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("time_node is null", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_const_expr(
              params_, *time_node, expr, nullptr))) {
        LOG_WARN("resolve sql expr failed");
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else {
        stmt->set_time_expr(expr);
        if (T_FLASHBACK_TABLE_TO_SCN == parse_tree.type_) {
          stmt->set_time_type(ObFlashBackTableToScnStmt::TIME_SCN);
        } else if (T_FLASHBACK_TABLE_TO_TIMESTAMP == parse_tree.type_) {
          stmt->set_time_type(ObFlashBackTableToScnStmt::TIME_TIMESTAMP);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }

        if (OB_SUCC(ret)) {
          int64_t start_time = session_info_->get_query_start_time();
          int64_t query_timeout = 0;
          // get_query_timeout里面肯定返回OB_SUCCESS，所以后面代码不需要在判断
          // 返回值了
          OZ(session_info_->get_query_timeout(query_timeout));
          stmt->set_query_end_time(start_time + query_timeout);
        }
      }
    }
  }

  return ret;
}

/**
 * flashback index
 */
int ObFlashBackIndexResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObFlashBackIndexStmt *flashback_index_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info or schema_checker is null", K(ret), K(schema_checker_), K(session_info_));
  } else if (T_FLASHBACK_INDEX != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(parser_tree.type_));
  }
  //create flashback index stmt
  if (OB_SUCC(ret)) {
    if (NULL == (flashback_index_stmt = create_stmt<ObFlashBackIndexStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create rename index stmt", K(ret));
    } else {
      stmt_ = flashback_index_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    flashback_index_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    //flashback table
    ParseNode *table_node = parser_tree.children_[ORIGIN_TABLE_NODE];
    ObString origin_table_name;
    ObString origin_db_name;
    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(table_node,
                                                   origin_table_name,
                                                   origin_db_name,
                                                   true /*get origin db_name*/))) {
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else if (!origin_db_name.empty() && origin_db_name != OB_RECYCLEBIN_SCHEMA_NAME) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(origin_db_name), to_cstring(origin_table_name));
      LOG_WARN("flashback index db.xx should not specified with db name", K(ret));
    } else {
      UNUSED(schema_checker_->get_table_schema(flashback_index_stmt->get_tenant_id(),
                                               OB_RECYCLEBIN_SCHEMA_ID,
                                               origin_table_name,
                                               true, /*is_index*/
                                               false, /*cte_table_fisrt*/
                                               false/*is_hidden*/,
                                               table_schema));
      flashback_index_stmt->set_origin_table_name(origin_table_name);
      flashback_index_stmt->set_origin_table_id(OB_NOT_NULL(table_schema) ? table_schema->get_table_id() : OB_INVALID_ID);
    }

    if (OB_SUCC(ret)) {
      //rename to new table_name
      ParseNode *rename_node = parser_tree.children_[NEW_TABLE_NODE];
      if (NULL != rename_node) {
        ObString new_index_name;
        new_index_name.assign_ptr(rename_node->str_value_, static_cast<int32_t>(rename_node->str_len_));
        flashback_index_stmt->set_new_table_name(new_index_name);
      }
    }
  }
  return ret;
}

/**
 * flashback database
 */
int ObFlashBackDatabaseResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObFlashBackDatabaseStmt *flashback_database_stmt = NULL;
  int32_t max_database_name_length = OB_MAX_DATABASE_NAME_LENGTH;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else if (T_FLASHBACK_DATABASE != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(parser_tree.type_));
  }
  //create flashback table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (flashback_database_stmt = create_stmt<ObFlashBackDatabaseStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create rename table stmt", K(ret));
    } else {
      stmt_ = flashback_database_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    flashback_database_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    ObString origin_db_name;
    ParseNode *origin_dbname_node = parser_tree.children_[ORIGIN_DB_NODE];
    if (OB_ISNULL(origin_dbname_node) || OB_UNLIKELY(T_IDENT != origin_dbname_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(
            static_cast<int32_t>(origin_dbname_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)origin_dbname_node->str_len_, origin_dbname_node->str_value_);
    } else {
      origin_db_name.assign_ptr(origin_dbname_node->str_value_,
                                static_cast<int32_t>(origin_dbname_node->str_len_));
      flashback_database_stmt->set_origin_db_name(origin_db_name);
    }
  }

  if (OB_SUCC(ret)) {
    ParseNode *new_db_node = parser_tree.children_[NEW_DB_NODE];
    if (NULL != new_db_node) {
      ObString new_db_name;
      if (OB_UNLIKELY(T_IDENT != new_db_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret));
      } else if (OB_UNLIKELY(
          static_cast<int32_t>(new_db_node->str_len_) > max_database_name_length)) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)new_db_node->str_len_, new_db_node->str_value_);
      } else {
        new_db_name.assign_ptr(new_db_node->str_value_,
                               static_cast<int32_t>(new_db_node->str_len_));
        flashback_database_stmt->set_new_db_name(new_db_name);
      }
    }
  }
  return ret;
}


/**
 * flashback tenant
 */
int ObFlashBackTenantResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObFlashBackTenantStmt *flashback_tenant_stmt = NULL;
  int32_t max_database_name_length = OB_MAX_DATABASE_NAME_LENGTH;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else if (T_FLASHBACK_TENANT != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(parser_tree.type_));
  }
  //create flashback table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (flashback_tenant_stmt = create_stmt<ObFlashBackTenantStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create flashback tenant stmt", K(ret));
    } else {
      stmt_ = flashback_tenant_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    flashback_tenant_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    ObString origin_tenant_name;
    ParseNode *origin_tenant_node = parser_tree.children_[ORIGIN_TENANT_NODE];
    if (OB_ISNULL(origin_tenant_node) || OB_UNLIKELY(T_IDENT != origin_tenant_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(
            static_cast<int32_t>(origin_tenant_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)origin_tenant_node->str_len_, origin_tenant_node->str_value_);
    } else {
      origin_tenant_name.assign_ptr(origin_tenant_node->str_value_,
                               static_cast<int32_t>(origin_tenant_node->str_len_));
      flashback_tenant_stmt->set_origin_tenant_name(origin_tenant_name);
    }
  }

  if (OB_SUCC(ret)) {
    ParseNode *new_tenant_node = parser_tree.children_[NEW_TENANT_NODE];
    ObString new_tenant_name;
    if (NULL != new_tenant_node) {
      if (OB_UNLIKELY(
            static_cast<int32_t>(new_tenant_node->str_len_) > max_database_name_length)) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)new_tenant_node->str_len_, new_tenant_node->str_value_);
      } else {
        new_tenant_name.assign_ptr(new_tenant_node->str_value_,
                                   static_cast<int32_t>(new_tenant_node->str_len_));
        if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(new_tenant_name))) {
          LOG_WARN("unsupported tenant name", KR(ret), K(new_tenant_name));
        } else {
          flashback_tenant_stmt->set_new_tenant_name(new_tenant_name);
        }
      }
    }
  }
  return ret;
}
} //namespace common
}
