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

#include "sql/resolver/ddl/ob_drop_table_resolver.h"
#include "lib/hash/ob_placement_hashset.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;
using common::hash::ObPlacementHashSet;
using obrpc::ObTableItem;
namespace sql
{
ObDropTableResolver::ObDropTableResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObDropTableResolver::~ObDropTableResolver()
{
}

int ObDropTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObDropTableStmt *drop_table_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) ||
                (T_DROP_TABLE != parse_tree.type_ && T_DROP_VIEW != parse_tree.type_) ||
                MAX_NODE != parse_tree.num_child_ || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(drop_table_stmt = create_stmt<ObDropTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "create drop table stmt failed");
  } else {
    stmt_ = drop_table_stmt;
    drop_table_stmt->set_is_view_stmt(T_DROP_VIEW == parse_tree.type_);
  }
  if (OB_SUCC(ret)) {
    obrpc::ObDropTableArg &drop_table_arg = drop_table_stmt->get_drop_table_arg();
    ObObj is_recyclebin_open;
    if (OB_ISNULL(parse_tree.children_[TABLE_LIST_NODE]) ||
        parse_tree.children_[TABLE_LIST_NODE]->num_child_ <= 0){
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_RECYCLEBIN, is_recyclebin_open))){
      SQL_RESV_LOG(WARN, "get sys variable failed", K(ret));
    } else {
      if (lib::is_oracle_mode()) {
        drop_table_arg.if_exist_ = false;
      } else {
        drop_table_arg.if_exist_ = (NULL != parse_tree.children_[IF_EXIST_NODE]) ? true : false;
      }
      drop_table_arg.tenant_id_ = session_info_->get_effective_tenant_id();
      drop_table_arg.to_recyclebin_ = is_recyclebin_open.get_bool();
    }

    if (OB_FAIL(ret)) {
    } else if (T_DROP_TABLE == parse_tree.type_) {
      if (NULL != parse_tree.children_[MATERIALIZED_NODE]
          && T_TEMPORARY == parse_tree.children_[MATERIALIZED_NODE]->type_) {
        // mysql临时表特有用法
        drop_table_arg.table_type_ = share::schema::TMP_TABLE;
      } else {
        drop_table_arg.table_type_ = share::schema::USER_TABLE; //xiyu@TODO: SYSTEM_TABLE???
        if (lib::is_oracle_mode()) {
          if (NULL != parse_tree.children_[0]) {
            // oracle模式没有if exist
            // cascade constarints 借用 if_exist_标记位
            drop_table_arg.if_exist_ = true;
          }
          if (NULL != parse_tree.children_[1]) {
            // 指定purge，则不放入回收站
            drop_table_arg.to_recyclebin_ = false;
          }
        }
      }
    } else if (T_DROP_VIEW == parse_tree.type_) {
      drop_table_arg.table_type_ = share::schema::USER_VIEW; //xiyu@TODO: SYSTEM_VIEW???
      if (parse_tree.children_[MATERIALIZED_NODE]) {
        // transfer to materiaized view, RS will drop such view table
        drop_table_arg.table_type_ = share::schema::MATERIALIZED_VIEW;
      }
      if (lib::is_oracle_mode()) {
        if (NULL != parse_tree.children_[0]) {
          // FIXME: cascade constarints
          // 目前view是不可以创建任何的索引包括constraints，因此这个条件直接生效的
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "Unknown parse tree type", K_(parse_tree.type), K(ret));
    }
    ObPlacementHashSet<ObTableItem> *tmp_ptr = NULL;
    ObPlacementHashSet<ObTableItem> *table_item_set = NULL;
    if (NULL == (tmp_ptr = (ObPlacementHashSet<ObTableItem> *)
      allocator_->alloc(sizeof(ObPlacementHashSet<ObTableItem>)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
    } else {
      table_item_set = new(tmp_ptr) ObPlacementHashSet<ObTableItem>();
      ObString db_name;
      ObString table_name;
      obrpc::ObTableItem table_item;
      ParseNode *table_node = NULL;
      int64_t i = 0;
      int64_t max_table_num = 0;
      if (lib::is_oracle_mode()) {
        max_table_num = 1;
      } else {
        if (OB_UNLIKELY(!parse_tree.children_[TABLE_LIST_NODE])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "parse tree node is null", K(ret), K(TABLE_LIST_NODE));
        } else {
          max_table_num = parse_tree.children_[TABLE_LIST_NODE]->num_child_;
        }
      }
      for (i = 0; OB_SUCC(ret) && i < max_table_num; ++i) {
        table_node = lib::is_oracle_mode() ? parse_tree.children_[TABLE_LIST_NODE]
            : parse_tree.children_[TABLE_LIST_NODE]->children_[i];
        ObString dblink_name;
        bool has_reverse_link = false;
        bool has_dblink_node = false;
        if (NULL == table_node) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "table_node is null", K(ret));
        } else if (OB_FAIL(resolve_dblink_name(table_node, session_info_->get_effective_tenant_id(), dblink_name, has_reverse_link, has_dblink_node))) {
          SQL_RESV_LOG(WARN, "failed to resolv dblink name", K(ret));
        } else if (has_dblink_node) {
          if (has_reverse_link || !dblink_name.empty()) {
            ret = OB_ERR_DDL_ON_REMOTE_DATABASE;
            SQL_RESV_LOG(WARN, "drop table on remote database by dblink", K(ret));
            LOG_USER_ERROR(OB_ERR_DDL_ON_REMOTE_DATABASE);
          } else {
            ret = OB_ERR_DATABASE_LINK_EXPECTED;
            SQL_RESV_LOG(WARN, "database link name expected", K(ret));
            LOG_USER_ERROR(OB_ERR_DATABASE_LINK_EXPECTED);
          }
        } else {
          db_name.reset();
          table_name.reset();
          if (OB_FAIL(resolve_table_relation_node(table_node,
                                                  table_name,
                                                  db_name))) {
            SQL_RESV_LOG(WARN, "failed to resolve table relation node!", K(ret));
          } else {
            table_item.reset();
            if (OB_FAIL(session_info_->get_name_case_mode(table_item.mode_))) {
              SQL_RESV_LOG(WARN, "failed to get name case mode!", K(ret));
            } else {
              table_item.database_name_ = db_name;
              table_item.table_name_ = table_name;
              if (OB_HASH_EXIST == table_item_set->exist_refactored(table_item)) {
                ret = OB_ERR_NONUNIQ_TABLE;
                LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, table_item.table_name_.length(), table_item.table_name_.ptr());
              } else if (OB_FAIL(table_item_set->set_refactored(table_item))) {
                SQL_RESV_LOG(WARN, "failed to add table item!", K(table_item), K(ret));
              } else if (OB_FAIL(drop_table_stmt->add_table_item(table_item))) {
                SQL_RESV_LOG(WARN, "failed to add table item!", K(table_item), K(ret));
              } else if (ObSchemaChecker::is_ora_priv_check()) {
                uint64_t tenant_id = session_info_->get_effective_tenant_id();
                bool is_exists = false;
                if (OB_FAIL(schema_checker_->check_table_exists(tenant_id,
                                                        db_name,
                                                        table_name,
                                                        false,
                                                        false/*is_hidden*/,
                                                        is_exists))) {
                  SQL_RESV_LOG(WARN, "failed to check table or view exists", K(db_name),
                                K(table_name), K(ret));
                } else if (!is_exists) {
                  ret = OB_TABLE_NOT_EXIST;
                  LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_name), to_cstring(table_name));
                } else {
                  uint64_t db_id = OB_INVALID_ID;
                  const share::schema::ObSimpleTableSchemaV2 *table_view_schema = NULL;
                  if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, db_id))) {
                    SQL_RESV_LOG(WARN, "failed to get db id", K(db_name), K(ret));
                  } else if (OB_FAIL(schema_checker_->get_simple_table_schema(tenant_id, 
                                          db_id, table_name, false, table_view_schema))) {
                    SQL_RESV_LOG(WARN, "failed to get simple table schema", K(db_id),
                                  K(table_name), K(ret));
                  } else if (NULL == table_view_schema) {
                    SQL_RESV_LOG(WARN, "table_view_schema is NULL", K(ret), K(db_id), K(table_name));
                  } else if ((T_DROP_TABLE == parse_tree.type_
                              && table_view_schema->is_view_table())
                              || (T_DROP_VIEW == parse_tree.type_
                                  && !table_view_schema->is_view_table())) {
                    ret = OB_TABLE_NOT_EXIST;
                    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_name),
                        to_cstring(table_name));
                  } else if (OB_FAIL(schema_checker_->check_ora_ddl_priv(
                                tenant_id,
                                session_info_->get_priv_user_id(),
                                db_name,
                                table_view_schema->get_table_id(),
                                T_DROP_TABLE == parse_tree.type_ ?
                                static_cast<uint64_t>(share::schema::ObObjectType::TABLE) :
                                static_cast<uint64_t>(share::schema::ObObjectType::VIEW),
                                T_DROP_TABLE == parse_tree.type_ ?
                                stmt::T_DROP_TABLE : stmt::T_DROP_VIEW,
                                session_info_->get_enable_role_array()))) {
                    if (OB_TABLE_NOT_EXIST == ret) {
                      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_name),
                          to_cstring(table_name));
                    }
                    SQL_RESV_LOG(WARN, "failed to check ora ddl priv",
                                  K(db_name), K(parse_tree.type_),
                                  K(table_name), K(session_info_->get_priv_user_id()), K(ret));
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

}  // namespace sql
}  // namespace oceanbase
