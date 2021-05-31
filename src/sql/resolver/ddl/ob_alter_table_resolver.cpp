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
#include "sql/resolver/ddl/ob_alter_table_resolver.h"
#include "share/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_rpc_struct.h"
#include "sql/parser/ob_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase {
using namespace share::schema;
using obrpc::ObCreateIndexArg;
using obrpc::ObDropIndexArg;
using namespace common;
using namespace obrpc;
namespace sql {
ObAlterTableResolver::ObAlterTableResolver(ObResolverParams& params)
    : ObDDLResolver(params),
      table_schema_(NULL),
      index_schema_(NULL),
      current_index_name_set_(),
      add_or_modify_check_cst_times_(0),
      modify_constraint_times_(0)
{}

ObAlterTableResolver::~ObAlterTableResolver()
{}

int ObAlterTableResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session_info should not be null", K(ret));
  } else if (T_ALTER_TABLE != parse_tree.type_ || ALTER_TABLE_NODE_COUNT != parse_tree.num_child_ ||
             OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ObAlterTableStmt* alter_table_stmt = NULL;
    // create alter table stmt
    if (NULL == (alter_table_stmt = create_stmt<ObAlterTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create alter table stmt", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_tz_info_wrap(session_info_->get_tz_info_wrap()))) {
      SQL_RESV_LOG(WARN, "failed to set_tz_info_wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_nls_formats(session_info_->get_local_nls_formats()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else {
      stmt_ = alter_table_stmt;
    }

    // resolve table
    if (OB_SUCC(ret)) {
      // alter table database_name.table_name ...
      ObString database_name;
      ObString table_name;
      if (OB_ISNULL(parse_tree.children_[TABLE])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[TABLE], table_name, database_name))) {
        SQL_RESV_LOG(WARN, "failed to resolve table name.", K(table_name), K(database_name), K(ret));
      } else if (OB_FAIL(set_database_name(database_name))) {
        SQL_RESV_LOG(WARN, "set database name failes", K(ret));
      } else {
        alter_table_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
        if (OB_FAIL(alter_table_stmt->set_origin_table_name(table_name))) {
          SQL_RESV_LOG(WARN, "failed to set origin table name", K(ret));
        } else if (OB_FAIL(set_table_name(table_name))) {
          SQL_RESV_LOG(WARN, "fail to set table name", K(ret), K(table_name));
        } else if (OB_FAIL(alter_table_stmt->set_origin_database_name(database_name))) {
          SQL_RESV_LOG(WARN, "failed to set origin database name", K(ret));
        } else if (0 == parse_tree.value_ &&
                   OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                       database_name,
                       table_name,
                       false /*not index table*/,
                       table_schema_))) {
          if (OB_TABLE_NOT_EXIST == ret) {
            LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(table_name));
          }
          LOG_WARN("fail to get table schema", K(ret));
        } else if (1 == parse_tree.value_) {
          uint64_t db_id = OB_INVALID_ID;
          if (OB_FAIL(
                  schema_checker_->get_database_id(session_info_->get_effective_tenant_id(), database_name, db_id))) {
            LOG_WARN("fail to get db id", K(ret), K(database_name));
          } else if (OB_FAIL(schema_checker_->get_idx_schema_by_origin_idx_name(
                         session_info_->get_effective_tenant_id(), db_id, table_name, index_schema_))) {
            LOG_WARN("fail to get index table schema", K(ret), K(table_name));
          } else if (OB_ISNULL(index_schema_)) {
            ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
            LOG_WARN("index not exists", K(ret), K(database_name), K(table_name));
            LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, table_name.length(), table_name.ptr());
          } else if (OB_FAIL(schema_checker_->get_table_schema(index_schema_->get_data_table_id(), table_schema_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get table schema with data table id failed", K(ret));
          } else if (OB_ISNULL(table_schema_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret));
          } else if (OB_FAIL(alter_table_stmt->set_origin_table_name(table_schema_->get_table_name_str()))) {
            SQL_RESV_LOG(WARN, "failed to set origin table name", K(ret));
          } else if (OB_FAIL(set_table_name(table_schema_->get_table_name_str()))) {
            SQL_RESV_LOG(WARN, "fail to set table name", K(ret), K(table_name));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(table_schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", K(ret));
        } else if (1 == parse_tree.value_ && OB_ISNULL(index_schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", K(ret));
        } else if (ObSchemaChecker::is_ora_priv_check()) {
          OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
              session_info_->get_priv_user_id(),
              database_name,
              index_schema_ != NULL ? index_schema_->get_table_id() : table_schema_->get_table_id(),
              index_schema_ != NULL ? static_cast<uint64_t>(ObObjectType::INDEX)
                                    : static_cast<uint64_t>(ObObjectType::TABLE),
              stmt::T_ALTER_TABLE,
              session_info_->get_enable_role_array()));
        }
      }
    }

    // resolve action list
    if (OB_SUCCESS == ret && NULL != parse_tree.children_[ACTION_LIST]) {
      if (OB_FAIL(resolve_action_list(*(parse_tree.children_[ACTION_LIST])))) {
        SQL_RESV_LOG(WARN, "failed to resolve action list.", K(ret));
      } else if (alter_table_bitset_.has_member(obrpc::ObAlterTableArg::LOCALITY) &&
                 alter_table_bitset_.has_member(obrpc::ObAlterTableArg::TABLEGROUP_NAME)) {
        ret = OB_OP_NOT_ALLOW;
        SQL_RESV_LOG(WARN, "alter table localiy and tablegroup in the same time not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter table localiy and tablegroup at the same time");
      } else if (OB_FAIL(set_table_options())) {
        SQL_RESV_LOG(WARN, "failed to set table options.", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      alter_table_stmt->set_table_id(table_schema_->get_table_id());
    }
    if (OB_SUCC(ret)) {
      // alter table
      if (OB_SUCC(ret) && OB_NOT_NULL(table_schema_)) {
        if (OB_FAIL(
                alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(
                    table_schema_->get_table_id(), TABLE_SCHEMA, table_schema_->get_schema_version())))) {
          SQL_RESV_LOG(WARN,
              "failed to add based_schema_object_info to arg",
              K(ret),
              K(table_schema_->get_table_id()),
              K(table_schema_->get_schema_version()),
              K(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_));
        }
      }
      // alter index
      if (OB_SUCC(ret) && OB_NOT_NULL(index_schema_)) {
        if (OB_FAIL(
                alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(
                    index_schema_->get_table_id(), TABLE_SCHEMA, index_schema_->get_schema_version())))) {
          SQL_RESV_LOG(WARN,
              "failed to add based_schema_object_info to arg",
              K(ret),
              K(index_schema_->get_table_id()),
              K(index_schema_->get_schema_version()),
              K(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_));
        }
      }
    }
  }
  DEBUG_SYNC(HANG_BEFORE_RESOLVER_FINISH);
  return ret;
}

int ObAlterTableResolver::set_table_options()
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "stmt should not be null!", K(ret));
  } else {
    AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    // this can be set by alter table option
    alter_table_schema.set_auto_increment(auto_increment_);
    alter_table_schema.set_block_size(block_size_);
    alter_table_schema.set_charset_type(charset_type_);
    alter_table_schema.set_collation_type(collation_type_);
    alter_table_schema.set_replica_num(replica_num_);
    alter_table_schema.set_tablet_size(tablet_size_);
    alter_table_schema.set_pctfree(pctfree_);
    alter_table_schema.set_progressive_merge_num(progressive_merge_num_);
    alter_table_schema.set_is_use_bloomfilter(use_bloom_filter_);
    alter_table_schema.set_read_only(read_only_);
    alter_table_schema.set_row_store_type(row_store_type_);
    alter_table_schema.set_store_format(store_format_);
    alter_table_schema.set_duplicate_scope(duplicate_scope_);
    alter_table_schema.set_enable_row_movement(enable_row_movement_);
    alter_table_schema.set_storage_format_version(storage_format_version_);
    alter_table_schema.set_table_mode_struct(table_mode_);
    alter_table_schema.set_dop(table_dop_);
    // deep copy
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(alter_table_schema.set_compress_func_name(compress_method_))) {
      SQL_RESV_LOG(WARN, "Write compress_method_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_comment(comment_))) {
      SQL_RESV_LOG(WARN, "Write comment_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_expire_info(expire_info_))) {
      SQL_RESV_LOG(WARN, "Write expire_info_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_primary_zone(primary_zone_))) {
      SQL_RESV_LOG(WARN, "Write primary_zone_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_table_name(table_name_))) {  // new table name
      SQL_RESV_LOG(WARN, "Write table_name_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_tablegroup_name(tablegroup_name_))) {
      SQL_RESV_LOG(WARN, "Write tablegroup to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_database_name(database_name_))) {
      SQL_RESV_LOG(WARN, "Write database_name to alter_table_schema failed!", K(database_name_), K(ret));
    } else if (OB_FAIL(alter_table_schema.set_locality(locality_))) {
      SQL_RESV_LOG(WARN, "Write locality to alter_table_schema failed!", K(locality_), K(ret));
    } else {
      alter_table_schema.alter_option_bitset_ = alter_table_bitset_;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    }

    if (OB_FAIL(ret)) {
      alter_table_schema.reset();
      SQL_RESV_LOG(WARN, "Set table options error!", K(ret));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_action_list(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  if (OB_UNLIKELY(node.num_child_ <= 0) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema_->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema* index_table_schema = NULL;
      ObString index_name;
      if (OB_FAIL(schema_checker_->get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret));
      } else if (index_table_schema->is_materialized_view()) {
      } else if (OB_FAIL(index_table_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else {
        ObIndexNameHashWrapper index_key(index_name);
        if (OB_FAIL(current_index_name_set_.set_refactored(index_key))) {
          LOG_WARN("fail to push back current_index_name_set_", K(ret), K(index_name));
        }
      }
    }
    // only use in oracle mode
    bool is_modify_column_visibility = false;
    int64_t alter_column_times = 0;
    int64_t alter_column_visibility_times = 0;
    ObReducedVisibleColSet reduced_visible_col_set;
    int cnt_alter_table = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode* action_node = node.children_[i];
      if (OB_ISNULL(action_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else if (share::is_oracle_mode() && is_modify_column_visibility &&
                 (alter_column_times != alter_column_visibility_times)) {
        ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
        SQL_RESV_LOG(WARN,
            "Column visibility modifications can not be combined with any other modified column DDL option.",
            K(ret));
      } else {
        cnt_alter_table++;
        switch (action_node->type_) {
          // deal with alter table option
          case T_ALTER_TABLE_OPTION: {
            alter_table_stmt->set_alter_table_option();
            if (OB_ISNULL(action_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
            } else if (OB_FAIL(resolve_table_option(action_node->children_[0], false))) {
              SQL_RESV_LOG(WARN, "Resolve table option failed!", K(ret));
            }
            break;
          }
          case T_TABLE_OPTION_LIST: {
            alter_table_stmt->set_alter_table_option();
            if (OB_FAIL(resolve_alter_table_option_list(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve table option failed!", K(ret));
            }
            break;
          }
          // deal with add column, alter column, drop column, change column, modify column
          case T_ALTER_COLUMN_OPTION: {
            alter_table_stmt->set_alter_table_column();
            bool temp_is_modify_column_visibility = false;
            if (OB_FAIL(
                    resolve_column_options(*action_node, temp_is_modify_column_visibility, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve column option failed!", K(ret));
            } else {
              if (temp_is_modify_column_visibility) {
                is_modify_column_visibility = temp_is_modify_column_visibility;
                ++alter_column_visibility_times;
              }
              ++alter_column_times;
            }
            break;
          }
          case T_ALTER_INDEX_OPTION_ORACLE: {
            alter_table_stmt->set_alter_table_index();
            if (OB_FAIL(resolve_index_options_oracle(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve index option oracle failed!", K(ret));
            }
            break;
          }
          // deal with add index drop index rename index
          case T_ALTER_INDEX_OPTION: {
            alter_table_stmt->set_alter_table_index();
            if (OB_FAIL(resolve_index_options(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve index option failed!", K(ret));
            }
            break;
          }
          case T_ALTER_PARTITION_OPTION: {
            alter_table_stmt->set_alter_table_partition();
            if (OB_FAIL(resolve_partition_options(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve partition option failed!", K(ret));
            }
            break;
          }
          case T_ALTER_CHECK_CONSTRAINT_OPTION: {
            if (OB_FAIL(resolve_constraint_options(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve check constraint option in mysql mode failed!", K(ret));
            }
            break;
          }
          case T_ALTER_TABLEGROUP_OPTION: {
            alter_table_stmt->set_alter_table_option();
            if (OB_FAIL(resolve_tablegroup_options(*action_node))) {
              SQL_RESV_LOG(WARN, "failed to resolve tablegroup options!", K(ret));
            }
            break;
          }
          case T_ALTER_FOREIGN_KEY_OPTION: {
            alter_table_stmt->set_alter_table_index();
            if (OB_FAIL(resolve_foreign_key_options(*action_node))) {
              SQL_RESV_LOG(WARN, "failed to resolve foreign key options in mysql mode!", K(ret));
            }
            break;
          }
          case T_DROP_CONSTRAINT: {
            ObString constraint_name;
            uint64_t constraint_id = OB_INVALID_ID;
            bool is_constraint = false;
            bool is_foreign_key = false;
            bool is_unique_key = false;
            ObSchemaGetterGuard* schema_guard = schema_checker_->get_schema_guard();

            if (OB_ISNULL(action_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
            } else if (OB_ISNULL(action_node->children_[0]->str_value_) || action_node->children_[0]->str_len_ <= 0) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN,
                  "invalid parse tree",
                  K(ret),
                  KP(action_node->children_[0]->str_value_),
                  K(action_node->children_[0]->str_len_));
            } else {
              constraint_name.assign_ptr(
                  action_node->children_[0]->str_value_, static_cast<int32_t>(action_node->children_[0]->str_len_));
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(schema_guard->get_constraint_id(table_schema_->get_tenant_id(),
                      table_schema_->get_database_id(),
                      constraint_name,
                      constraint_id))) {
                LOG_WARN("get constraint id failed",
                    K(ret),
                    K(table_schema_->get_tenant_id()),
                    K(table_schema_->get_database_id()),
                    K(constraint_name));
              } else if (OB_INVALID_ID != constraint_id) {
                is_constraint = true;
                if (OB_FAIL(resolve_constraint_options(*action_node))) {
                  SQL_RESV_LOG(WARN, "Resolve check constraint option in mysql mode failed!", K(ret));
                }
              } else if (!is_constraint) {
                if (OB_FAIL(schema_guard->get_foreign_key_id(table_schema_->get_tenant_id(),
                        table_schema_->get_database_id(),
                        constraint_name,
                        constraint_id))) {
                  LOG_WARN("get foreign key id failed",
                      K(ret),
                      K(table_schema_->get_tenant_id()),
                      K(table_schema_->get_database_id()),
                      K(constraint_name));
                } else if (OB_INVALID_ID != constraint_id) {
                  is_foreign_key = true;
                  alter_table_stmt->set_alter_table_index();
                  if (OB_FAIL(resolve_foreign_key_options(*action_node))) {
                    SQL_RESV_LOG(WARN, "failed to resolve foreign key options in mysql mode!", K(ret));
                  }
                } else if (!is_foreign_key) {
                  const ObSimpleTableSchemaV2* simple_table_schema = nullptr;
                  ObString unique_index_name_with_prefix;
                  if (OB_FAIL(ObTableSchema::build_index_table_name(*allocator_,
                          table_schema_->get_table_id(),
                          constraint_name,
                          unique_index_name_with_prefix))) {
                    LOG_WARN(
                        "build_index_table_name failed", K(ret), K(table_schema_->get_table_id()), K(constraint_name));
                  } else if (OB_FAIL(schema_guard->get_simple_table_schema(table_schema_->get_tenant_id(),
                                 table_schema_->get_database_id(),
                                 unique_index_name_with_prefix,
                                 true,
                                 simple_table_schema))) {
                    LOG_WARN("failed to get simple table schema",
                        K(ret),
                        K(table_schema_->get_tenant_id()),
                        K(table_schema_->get_database_id()),
                        K(unique_index_name_with_prefix));
                  } else if (OB_NOT_NULL(simple_table_schema) && simple_table_schema->is_unique_index()) {
                    is_unique_key = true;
                    action_node->type_ = T_INDEX_DROP;
                    alter_table_stmt->set_alter_table_index();
                    if (OB_FAIL(resolve_drop_index(*action_node))) {
                      SQL_RESV_LOG(WARN, "Resolve drop index error!", K(ret));
                    }
                  }
                }
              }
            }
            if (OB_SUCC(ret) && !is_constraint && !is_foreign_key && !is_unique_key) {
              ret = OB_ERR_NONEXISTENT_CONSTRAINT;
              SQL_RESV_LOG(WARN,
                  "Cannot drop constraint  - nonexistent constraint",
                  K(ret),
                  K(*table_schema_),
                  K(constraint_name));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            SQL_RESV_LOG(WARN, "Unknown alter table action %d", K_(action_node->type), K(ret));
            /* won't be here */
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      if (is_modify_column_visibility && (alter_column_times != alter_column_visibility_times)) {
        ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
        SQL_RESV_LOG(WARN,
            "Column visibility modifications can not be combined with any other modified column DDL option.",
            K(ret));
      } else {
        bool has_visible_col = false;
        ObColumnIterByPrevNextID iter(*table_schema_);
        const ObColumnSchemaV2* column_schema = NULL;
        while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema)) && !has_visible_col) {
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "The column is null", K(ret));
          } else if (column_schema->is_shadow_column()) {
            // skip shadow column
            continue;
          } else if (column_schema->is_invisible_column()) {
            // skip invisible column
            continue;
          } else if (column_schema->is_hidden()) {
            // skip hidden column
            continue;
          } else {  // is visible column
            ObColumnNameHashWrapper col_key(column_schema->get_column_name_str());
            if (OB_HASH_NOT_EXIST == reduced_visible_col_set.exist_refactored(col_key)) {
              has_visible_col = true;
              ret = OB_SUCCESS;  // change ret from OB_HASH_NOT_EXIST to OB_SUCCESS
            } else {             // OB_HASH_EXIST
              ret = OB_SUCCESS;  // change ret from OB_HASH_EXIST to OB_SUCCESS
            }
          }
        }
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
          SQL_RESV_LOG(WARN, "failed to check column visibility", K(ret));
          if (NULL != column_schema) {
            SQL_RESV_LOG(WARN, "failed column schema", K(*column_schema), K(column_schema->is_hidden()));
          }
        } else {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          if (alter_column_visibility_times > reduced_visible_col_set.count()) {
          } else if (!has_visible_col) {
            ret = OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE;
            SQL_RESV_LOG(WARN, "table must have at least one column that is not invisible", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      if (cnt_alter_table > 1) {
        ObSArray<obrpc::ObCreateIndexArg*>& index_arg_list = alter_table_stmt->get_index_arg_list();
        for (int32_t i = 0; OB_SUCC(ret) && i < index_arg_list.count(); ++i) {
          const ObCreateIndexArg* index_arg = index_arg_list.at(i);
          if (NULL == index_arg) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "index arg is null", K(ret));
          } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "add index together with other DDLs");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (cnt_alter_table > 1) {
        if (0 != alter_table_stmt->get_foreign_key_arg_list().count()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "add/modify foreign key together with other DDLs");
        }
      }
    }
    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      if (cnt_alter_table > 1) {
        if (0 != add_or_modify_check_cst_times_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "add/modify check constraint together with other DDLs");
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_column_options(
    const ParseNode& node, bool& is_modify_column_visibility, ObReducedVisibleColSet& reduced_visible_col_set)
{
  int ret = OB_SUCCESS;

  if (T_ALTER_COLUMN_OPTION != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode* column_node = node.children_[i];
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else {
        switch (column_node->type_) {
          // add column
          case T_COLUMN_ADD: {
            if (OB_FAIL(resolve_add_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve add column error!", K(ret));
            }
            break;
          }
          // alter column attribute
          case T_COLUMN_ALTER: {
            if (OB_FAIL(resolve_alter_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve alter column error!", K(ret));
            }
            break;
          }
          // change column name
          case T_COLUMN_CHANGE: {
            if (OB_FAIL(resolve_change_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve change column error!", K(ret));
            }
            break;
          }
          // rename column name in oracle mode
          case T_COLUMN_RENAME: {
            if (OB_FAIL(resolve_rename_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve rename column error!", K(ret));
            }
            break;
          }
          // modify column attribute
          case T_COLUMN_MODIFY: {
            if (OB_FAIL(resolve_modify_column(*column_node, is_modify_column_visibility, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve modify column error!", K(ret));
            }
            break;
          }
          case T_COLUMN_DROP: {
            if (OB_FAIL(resolve_drop_column(*column_node, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve drop column error!", K(ret));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            SQL_RESV_LOG(WARN, "Unknown column option type!", "type", column_node->type_, K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_index_column_list(const ParseNode& node, obrpc::ObCreateIndexArg& index_arg,
    const ObTableSchema& tbl_schema, const bool is_fulltext_index, ParseNode* table_option_node,
    ObIArray<ObString>& input_index_columns_name)
{
  UNUSED(tbl_schema);
  UNUSED(table_option_node);
  int ret = OB_SUCCESS;
  if (T_INDEX_COLUMN_LIST != node.type_ || node.num_child_ <= 0 || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (!is_fulltext_index) {
  }

  if (OB_SUCC(ret)) {
    obrpc::ObColumnSortItem sort_item;
    // reset sort column set
    sort_column_array_.reset();
    for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode* sort_column_node = node.children_[i];
      if (OB_ISNULL(sort_column_node) || OB_UNLIKELY(T_SORT_COLUMN_KEY != sort_column_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else {
        // column_name
        sort_item.reset();
        if (OB_ISNULL(sort_column_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
        } else {
          sort_item.column_name_.assign_ptr(sort_column_node->children_[0]->str_value_,
              static_cast<int32_t>(sort_column_node->children_[0]->str_len_));
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (NULL != sort_column_node->children_[1]) {
          sort_item.prefix_len_ = static_cast<int32_t>(sort_column_node->children_[1]->value_);
          // can't not be zero
          if (0 == sort_item.prefix_len_) {
            ret = OB_KEY_PART_0;
            LOG_USER_ERROR(OB_KEY_PART_0, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            SQL_RESV_LOG(WARN, "Key part length cannot be 0", K(sort_item), K(ret));
          }
        } else {
          sort_item.prefix_len_ = 0;
        }

        // column_order
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (sort_column_node->children_[2] && T_SORT_DESC == sort_column_node->children_[2]->type_) {
          // sort_item.order_type_ = common::ObOrderType::DESC;
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support desc index now", K(ret));
        } else {
          sort_item.order_type_ = common::ObOrderType::ASC;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_sort_column(sort_item, index_arg))) {
            SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(input_index_columns_name.push_back(sort_item.column_name_))) {
            SQL_RESV_LOG(WARN, "add column name to input_index_columns_name failed", K(sort_item.column_name_), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::add_sort_column(
    const obrpc::ObColumnSortItem& sort_column, obrpc::ObCreateIndexArg& index_arg)
{
  int ret = OB_SUCCESS;
  const ObString& column_name = sort_column.column_name_;
  ObColumnNameWrapper column_key(column_name, sort_column.prefix_len_);
  bool check_prefix_len = false;
  if (is_column_exists(sort_column_array_, column_key, check_prefix_len)) {
    ret = OB_ERR_COLUMN_DUPLICATE;  // index (c1,c1) or index (c1(3), c1 (6))
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
  } else if (OB_FAIL(sort_column_array_.push_back(column_key))) {
    SQL_RESV_LOG(WARN, "failed to push back column key", K(ret));
  } else if (OB_FAIL(index_arg.index_columns_.push_back(sort_column))) {
    SQL_RESV_LOG(WARN, "add sort column to index arg failed", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::get_table_schema_for_check(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  const ObTableSchema* tbl_schema = NULL;
  if (OB_ISNULL(alter_table_stmt)) {
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                 alter_table_stmt->get_org_database_name(),
                 alter_table_stmt->get_org_table_name(),
                 false /*not index table*/,
                 tbl_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
          to_cstring(alter_table_stmt->get_org_database_name()),
          to_cstring(alter_table_stmt->get_org_table_name()));
    }
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema.assign(*tbl_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_index(const ParseNode& node)
{
  int ret = OB_SUCCESS;

  if (T_INDEX_ADD != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    bool is_unique_key = 1 == node.value_;
    ParseNode* index_name_node = nullptr;
    ParseNode* column_list_node = nullptr;
    ParseNode* table_option_node = nullptr;
    CHECK_COMPATIBILITY_MODE(session_info_);
    if (is_unique_key && lib::is_oracle_mode()) {
      // oracle mode
      if (node.num_child_ != 2) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else {
        index_name_node = node.children_[0];
        column_list_node = node.children_[1];
      }
    } else {
      // mysql mode
      index_name_node = node.children_[0];
      column_list_node = node.children_[1];
      table_option_node = node.children_[2];
    }
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      sort_column_array_.reset();
      storing_column_set_.reset();
      index_keyname_ = static_cast<INDEX_KEYNAME>(node.value_);

      // column_list node should be parse first in case the index name is not specified
      if (OB_SUCC(ret)) {
        ObCreateIndexStmt create_index_stmt(allocator_);
        obrpc::ObCreateIndexArg* create_index_arg = NULL;
        void* tmp_ptr = NULL;
        ObSEArray<ObString, 8> input_index_columns_name;
        if (NULL == (tmp_ptr = (ObCreateIndexArg*)allocator_->alloc(sizeof(obrpc::ObCreateIndexArg)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
        } else {
          create_index_arg = new (tmp_ptr) ObCreateIndexArg();
          if (OB_ISNULL(column_list_node)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
          } else if (OB_FAIL(resolve_index_column_list(*column_list_node,
                         *create_index_arg,
                         *table_schema_,
                         2 == node.value_,
                         table_option_node,
                         input_index_columns_name))) {
            SQL_RESV_LOG(WARN, "resolve index name failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (!lib::is_oracle_mode()) {
            if (NULL != node.children_[3]) {
              if (T_USING_BTREE == node.children_[3]->type_) {
                create_index_arg->index_using_type_ = USING_BTREE;
              } else {
                create_index_arg->index_using_type_ = USING_HASH;
              }
            }
          } else {
            // oracle mode
            // In oracle mode, we need to check if the new index is on the same cols with old indexes
            bool has_other_indexes_on_same_cols = false;
            if (OB_FAIL(check_indexes_on_same_cols(
                    *table_schema_, *create_index_arg, *schema_checker_, has_other_indexes_on_same_cols))) {
              SQL_RESV_LOG(WARN, "check indexes on same cols failed", K(ret));
            } else if (has_other_indexes_on_same_cols) {
              ret = OB_ERR_COLUMN_LIST_ALREADY_INDEXED;
              SQL_RESV_LOG(WARN, "has other indexes on the same cols", K(ret));
            }
            // In oracle mode, we need to check if the unique index is on the same cols with pk
            if (OB_SUCC(ret) && is_unique_key) {
              bool is_uk_pk_on_same_cols = false;
              if (OB_FAIL(ObResolverUtils::check_pk_idx_duplicate(
                      *table_schema_, *create_index_arg, input_index_columns_name, is_uk_pk_on_same_cols))) {
                SQL_RESV_LOG(WARN, "check if pk and uk on same cols failed", K(ret));
              } else if (is_uk_pk_on_same_cols) {
                ret = OB_ERR_UK_PK_DUPLICATE;
                SQL_RESV_LOG(WARN, "uk and pk is duplicate", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(index_name_node)) {
            // create_index_arg->index_name_.reset();
            // generate index name
            ObString index_name;
            if (input_index_columns_name.count() < 1) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "size of index columns is less than 1", K(ret));
            } else {
              ObString first_column_name = input_index_columns_name.at(0);
              if (lib::is_oracle_mode()) {
                if (OB_FAIL(ObTableSchema::create_cons_name_automatically(
                        index_name, table_name_, *allocator_, CONSTRAINT_TYPE_UNIQUE_KEY))) {
                  SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
                }
              } else {  // mysql mode
                if (OB_FAIL(generate_index_name(index_name, current_index_name_set_, first_column_name))) {
                  SQL_RESV_LOG(WARN, "failed to generate index name", K(first_column_name));
                }
              }
              if (OB_SUCC(ret)) {
                ObIndexNameHashWrapper index_key(index_name);
                if (OB_FAIL(current_index_name_set_.set_refactored(index_key))) {
                  LOG_WARN("fail to push back current_index_name_set_", K(ret), K(index_name));
                } else if (OB_FAIL(ob_write_string(*allocator_, index_name, create_index_arg->index_name_))) {
                  LOG_WARN("fail to wirte string", K(ret), K(index_name), K(first_column_name));
                }
              }
            }
          } else {
            if (T_IDENT != index_name_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
            } else {
              int32_t len = static_cast<int32_t>(index_name_node->str_len_);
              create_index_arg->index_name_.assign_ptr(index_name_node->str_value_, len);
              ObCollationType cs_type = CS_TYPE_INVALID;
              if (OB_UNLIKELY(NULL == session_info_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("session if NULL", K(ret));
              } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
                LOG_WARN("fail to get collation connection", K(ret));
              } else if (OB_FAIL(ObSQLUtils::check_index_name(cs_type, create_index_arg->index_name_))) {
                LOG_WARN("fail to check index name", K(ret), K(create_index_arg->index_name_));
              }
            }
          }
          if (OB_SUCCESS == ret) {
            if (NULL != table_option_node) {
              has_index_using_type_ = false;
              if (OB_FAIL(resolve_table_options(table_option_node, false))) {
                SQL_RESV_LOG(WARN, "failed to resolve table options!", K(ret));
              } else if (has_index_using_type_) {
                create_index_arg->index_using_type_ = index_using_type_;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (table_schema_->is_old_no_pk_table() && table_schema_->is_partitioned_table() &&
                OB_FAIL(store_part_key(*table_schema_, *create_index_arg))) {
              LOG_WARN("failed to store part key", K(ret));
            } else if (OB_FAIL(generate_index_arg(*create_index_arg, is_unique_key))) {
              SQL_RESV_LOG(WARN, "failed to generate index arg!", K(ret));
            } else {
              create_index_arg->index_schema_.set_table_type(USER_INDEX);
              create_index_arg->index_schema_.set_index_type(create_index_arg->index_type_);
              if (OB_FAIL(create_index_stmt.get_create_index_arg().assign(*create_index_arg))) {
                LOG_WARN("fail to assign create index arg", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            ObSArray<ObPartitionResolveResult>& resolve_results =
                alter_table_stmt->get_index_partition_resolve_results();
            ObSArray<obrpc::ObCreateIndexArg*>& index_arg_list = alter_table_stmt->get_index_arg_list();
            ObPartitionResolveResult resolve_result;
            resolve_result.get_part_fun_exprs() = create_index_stmt.get_part_fun_exprs();
            resolve_result.get_part_values_exprs() = create_index_stmt.get_part_values_exprs();
            resolve_result.get_subpart_fun_exprs() = create_index_stmt.get_subpart_fun_exprs();
            resolve_result.get_template_subpart_values_exprs() = create_index_stmt.get_template_subpart_values_exprs();
            resolve_result.get_individual_subpart_values_exprs() =
                create_index_stmt.get_individual_subpart_values_exprs();
            if (OB_FAIL((*create_index_arg).assign(create_index_stmt.get_create_index_arg()))) {
              LOG_WARN("fail to assign create index arg", K(ret));
            } else if (OB_FAIL(resolve_results.push_back(resolve_result))) {
              LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
            } else if (OB_FAIL(index_arg_list.push_back(create_index_arg))) {
              LOG_WARN("fail to push back index_arg", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            storing_column_set_.reset();  // storing column for each index
            sort_column_array_.reset();   // column for each index
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_constraint(const ParseNode& node)
{
  int ret = OB_SUCCESS;

  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
  ObSEArray<ObConstraint, 4> csts;
  for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
       OB_SUCC(ret) && iter != alter_table_schema.constraint_end();
       iter++) {
    ret = csts.push_back(**iter);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_check_constraint_node(node, csts))) {
      SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
    } else if (OB_FAIL(alter_table_schema.add_constraint(csts.at(csts.count() - 1)))) {
      SQL_RESV_LOG(WARN, "add constraint failed", K(ret));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_partition(const ParseNode& node, const ObTableSchema& orig_table_schema)
{
  int ret = OB_SUCCESS;
  AlterTableSchema& alter_table_schema = get_alter_table_stmt()->get_alter_table_arg().alter_table_schema_;
  ObTableStmt* alter_stmt = get_alter_table_stmt();
  ObSEArray<ObString, 8> dummy_part_keys;
  const ObPartitionOption& part_option = orig_table_schema.get_part_option();
  const ObPartitionFuncType part_func_type = part_option.get_part_func_type();
  ParseNode* part_func_node = NULL;
  ParseNode* part_elements_node = NULL;

  if (OB_ISNULL(node.children_[0]) || OB_ISNULL(part_elements_node = node.children_[0]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node.children_[0]), K(part_elements_node));
  } else if (OB_FAIL(mock_part_func_node(orig_table_schema, false /*is_sub_part*/, part_func_node))) {
    LOG_WARN("mock part func node failed", K(ret));
  } else if (OB_FAIL(resolve_part_func(params_,
                 part_func_node,
                 part_func_type,
                 orig_table_schema,
                 alter_stmt->get_part_fun_exprs(),
                 dummy_part_keys))) {
    LOG_WARN("resolve part func failed", K(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_elements_node->num_child_; ++i) {
      if (OB_ISNULL(part_elements_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL != part_elements_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE]) {
        ret = OB_ERR_NOT_COMPOSITE_PARTITION;
        LOG_WARN("table is not partitioned by composite partition method", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              inner_add_partition(part_elements_node, part_func_type, part_option, alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to inner add partition", K(ret));
      }
    }
  } else if (orig_table_schema.is_sub_part_template()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_elements_node->num_child_; ++i) {
      if (OB_ISNULL(part_elements_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL != part_elements_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE]) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "add partition with subpartition to sub part template table");
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              inner_add_partition(part_elements_node, part_func_type, part_option, alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to inner add partition", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_elements_node->num_child_; ++i) {
      if (OB_ISNULL(part_elements_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_ISNULL(part_elements_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE])) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "add partition without subpartition to non sub part template table");
      }
    }
    if (OB_SUCC(ret)) {
      const ObPartitionOption& subpart_option = orig_table_schema.get_sub_part_option();
      const ObPartitionFuncType subpart_type = subpart_option.get_part_func_type();
      ParseNode* subpart_func_node = NULL;
      alter_table_schema.set_is_sub_part_template(orig_table_schema.is_sub_part_template());
      alter_table_schema.get_sub_part_option() = orig_table_schema.get_sub_part_option();

      if (OB_FAIL(mock_part_func_node(orig_table_schema, true /*is_sub_part*/, subpart_func_node))) {
        LOG_WARN("mock part func node failed", K(ret));
      } else if (OB_FAIL(resolve_part_func(params_,
                     subpart_func_node,
                     subpart_type,
                     orig_table_schema,
                     alter_stmt->get_subpart_fun_exprs(),
                     dummy_part_keys))) {
        LOG_WARN("resolve part func failed", K(ret));
      } else if (OB_FAIL(inner_add_partition(
                     part_elements_node, part_func_type, part_option, alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to inner add partition", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to resolve partition elements",
        KPC(alter_stmt),
        K(alter_stmt->get_part_fun_exprs()),
        K(alter_stmt->get_part_values_exprs()),
        K(alter_stmt->get_subpart_fun_exprs()),
        K(alter_stmt->get_individual_subpart_values_exprs()));

    alter_table_schema.set_part_level(orig_table_schema.get_part_level());
    alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
    alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
    // check part_name and subpart_name duplicate
    if (OB_FAIL(check_and_set_partition_names(alter_stmt, alter_table_schema, false))) {
      LOG_WARN("failed to check and set partition names", K(ret));
    } else if (PARTITION_LEVEL_TWO == orig_table_schema.get_part_level()) {
      if (orig_table_schema.is_sub_part_template()) {
        // do nothing
      } else if (OB_FAIL(check_and_set_individual_subpartition_names(alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to check and set individual subpartition names", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < alter_table_schema.get_partition_num(); ++i) {
          ObPartition* cur_part = alter_table_schema.get_part_array()[i];
          if (OB_ISNULL(cur_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            cur_part->set_part_id(-1);
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::inner_add_partition(ParseNode* part_elements_node, const ObPartitionFuncType part_type,
    const ObPartitionOption& part_option, ObTableStmt* alter_stmt, ObTableSchema& alter_table_schema)
{
  int ret = OB_SUCCESS;
  int64_t max_used_part_id = OB_INVALID_ID;
  if (OB_ISNULL(part_elements_node) || OB_ISNULL(alter_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(part_elements_node), K(alter_stmt));
  } else if (part_option.is_range_part()) {
    if (T_LIST_PARTITION_LIST == part_elements_node->type_) {
      ret = OB_ERR_PARTITION_EXPECT_VALUES_LESS_THAN;
      LOG_WARN("Expecting VALUES LESS THAN  or AT clause", K(ret));
    } else if (OB_FAIL(resolve_range_partition_elements(alter_stmt,
                   part_elements_node,
                   alter_table_schema,
                   part_type,
                   alter_stmt->get_part_fun_exprs(),
                   alter_stmt->get_part_values_exprs(),
                   max_used_part_id))) {
      LOG_WARN("failed to resolve reange partition elements", K(ret));
    }
  } else if (part_option.is_list_part()) {
    if (T_RANGE_SUBPARTITION_LIST == part_elements_node->type_) {
      ret = OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN;
      LOG_WARN("VALUES LESS THAN or AT clause cannot be used with List partitioned tables", K(ret));
    } else if (OB_FAIL(resolve_list_partition_elements(alter_stmt,
                   part_elements_node,
                   alter_table_schema,
                   part_type,
                   alter_stmt->get_part_fun_exprs(),
                   alter_stmt->get_part_values_exprs(),
                   max_used_part_id))) {
      LOG_WARN("failed to resolve list partition elements", K(ret));
    }
  } else if (part_option.is_hash_like_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add hash partition");
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_subpartition(const ParseNode& node, const ObTableSchema& orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption& subpart_option = orig_table_schema.get_sub_part_option();
  const ObPartitionFuncType subpart_type = subpart_option.get_part_func_type();
  ParseNode* subpart_func_node = NULL;
  ParseNode* part_name_node = NULL;
  ParseNode* part_elements_node = NULL;

  if (OB_ISNULL(part_name_node = node.children_[0]) || OB_ISNULL(node.children_[1]) ||
      OB_ISNULL(part_elements_node = node.children_[1]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    ret = OB_ERR_NOT_COMPOSITE_PARTITION;
    LOG_WARN("table is not partitioned by composite partition method", K(ret));
  } else if (orig_table_schema.is_sub_part_template()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add subpartition on subpart template table");
  } else if (OB_FAIL(mock_part_func_node(orig_table_schema, true /*is_sub_part*/, subpart_func_node))) {
    LOG_WARN("mock part func node failed", K(ret));
  } else {
    AlterTableSchema& alter_table_schema = get_alter_table_stmt()->get_alter_table_arg().alter_table_schema_;
    ObTableStmt* alter_stmt = get_alter_table_stmt();
    ObSEArray<ObString, 8> dummy_part_keys;
    ObPartition dummy_part;
    ObPartition* cur_partition = NULL;
    alter_table_schema.set_is_sub_part_template(false);
    alter_table_schema.get_sub_part_option() = orig_table_schema.get_sub_part_option();
    // resolve partition name
    ObString partition_name(static_cast<int32_t>(part_name_node->str_len_), part_name_node->str_value_);
    int64_t part_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_partition_num(); ++i) {
      ObPartition* ori_partition = orig_table_schema.get_part_array()[i];
      if (OB_ISNULL(ori_partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (ori_partition->get_part_name() == partition_name) {
        part_id = ori_partition->get_part_id();
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == part_id) {
      ret = OB_UNKNOWN_PARTITION;
      LOG_USER_ERROR(OB_UNKNOWN_PARTITION,
          partition_name.length(),
          partition_name.ptr(),
          orig_table_schema.get_table_name_str().length(),
          orig_table_schema.get_table_name_str().ptr());
    } else if (OB_FAIL(dummy_part.set_part_name(partition_name))) {
      LOG_WARN("failed to set subpart name", K(ret), K(partition_name));
    } else if (FALSE_IT(dummy_part.set_part_id(part_id))) {
    } else if (OB_FAIL(alter_table_schema.add_partition(dummy_part))) {
      LOG_WARN("failed to add partition", K(ret));
    } else if (OB_ISNULL(cur_partition = alter_table_schema.get_part_array()[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
      // resolve subpartition define
    } else if (OB_FAIL(resolve_part_func(params_,
                   subpart_func_node,
                   subpart_type,
                   orig_table_schema,
                   alter_stmt->get_subpart_fun_exprs(),
                   dummy_part_keys))) {
      LOG_WARN("resolve part func failed", K(ret));
    } else if (subpart_option.is_range_part()) {
      if (T_LIST_SUBPARTITION_LIST == part_elements_node->type_) {
        ret = OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN;
        LOG_WARN("VALUES (<value list>) cannot be used for Range subpartitioned tables", K(ret));
      } else if (OB_FAIL(resolve_subpartition_elements(
                     alter_stmt, part_elements_node, alter_table_schema, cur_partition, false))) {
        LOG_WARN("failed to resolve subpartition elements", K(ret));
      }
    } else if (subpart_option.is_list_part()) {
      if (T_RANGE_SUBPARTITION_LIST == part_elements_node->type_) {
        ret = OB_ERR_SUBPARTITION_EXPECT_VALUES_IN;
        LOG_WARN("VALUES (<value list>) clause expected", K(ret));
      } else if (OB_FAIL(resolve_subpartition_elements(
                     alter_stmt, part_elements_node, alter_table_schema, cur_partition, false))) {
        LOG_WARN("failed to resolve subpartition elements", K(ret));
      }
    } else if (subpart_option.is_hash_like_part()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "add hash subpartition");
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to resolve subpartition elements",
          KPC(alter_stmt),
          K(alter_stmt->get_subpart_fun_exprs()),
          K(alter_stmt->get_individual_subpart_values_exprs()));

      alter_table_schema.set_part_level(orig_table_schema.get_part_level());
      alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
      alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
      cur_partition->set_sub_part_num(cur_partition->get_subpartition_num());
      // check subpart_name duplicate
      if (OB_FAIL(check_and_set_individual_subpartition_names(alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to check and set individual subpartition names", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < cur_partition->get_subpartition_num(); ++i) {
        ObSubPartition* cur_subpart = cur_partition->get_subpart_array()[i];
        if (OB_ISNULL(cur_subpart)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          cur_subpart->set_sub_part_id(-1);
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::mock_part_func_node(
    const ObTableSchema& table_schema, const bool is_sub_part, ParseNode*& part_expr_node)
{
  int ret = OB_SUCCESS;
  part_expr_node = NULL;

  ObSqlString sql_str;
  ParseResult parse_result;
  ParseNode* stmt_node = NULL;
  ParseNode* select_node = NULL;
  ParseNode* select_expr_list = NULL;
  ParseNode* select_expr_node = NULL;
  ObParser parser(*allocator_, params_.session_info_->get_sql_mode());
  const ObString& part_str = is_sub_part ? table_schema.get_sub_part_option().get_part_func_expr_str()
                                         : table_schema.get_part_option().get_part_func_expr_str();
  ObPartitionFuncType part_type = is_sub_part ? table_schema.get_sub_part_option().get_part_func_type()
                                              : table_schema.get_part_option().get_part_func_type();

  if (is_inner_table(table_schema.get_table_id())) {
    if (OB_FAIL(sql_str.append_fmt("SELECT partition_%.*s FROM DUAL", part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_KEY == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT partition_%.*s FROM DUAL", part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_KEY_V2 == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_KEY_V2, part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_KEY_V3 == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_KEY_V3, part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_HASH == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_HASH_V1, part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_HASH_V2 == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_HASH_V2, part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else {
    if (OB_FAIL(sql_str.append_fmt("SELECT (%.*s) FROM DUAL", part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    ret = OB_ERR_PARSE_SQL;
    _OB_LOG(WARN,
        "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d], sql[%.*s]",
        parse_result.yyscan_info_,
        parse_result.result_tree_,
        parse_result.malloc_pool_,
        parse_result.error_msg_,
        parse_result.start_col_,
        parse_result.end_col_,
        parse_result.line_,
        parse_result.yycolumn_,
        parse_result.yylineno_,
        static_cast<int>(sql_str.length()),
        sql_str.ptr());
  } else if (OB_ISNULL(stmt_node = parse_result.result_tree_) || OB_UNLIKELY(stmt_node->type_ != T_STMT_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt node is invalid", K(stmt_node));
  } else if (OB_ISNULL(select_node = stmt_node->children_[0]) || OB_UNLIKELY(select_node->type_ != T_SELECT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select node is invalid", K(select_node));
  } else if (OB_ISNULL(select_expr_list = select_node->children_[PARSE_SELECT_SELECT]) ||
             OB_UNLIKELY(select_expr_list->type_ != T_PROJECT_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr list is invalid", K(ret));
  } else if (OB_ISNULL(select_expr_node = select_expr_list->children_[0]) ||
             OB_UNLIKELY(select_expr_node->type_ != T_PROJECT_STRING)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr node is invalid", K(ret));
  } else if (OB_ISNULL(part_expr_node = select_expr_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part expr node is invalid", K(part_expr_node));
  }
  // no need destory parse tree right now
  return ret;
}

// int ObAlterTableResolver::check_alter_partition(const obrpc::ObAlterTableArg &arg)
//{
//  int ret = OB_SUCCESS;
//
//  if (arg.is_alter_partitions_) {
//    const AlterTableSchema &alter_table_schema = arg.alter_table_schema_;
//    //ObPartition **partition_array = table_schema.get_part_array();
//    //int64_t get_partition_num = alter_table_schema.get_partition_num();
//    //for (int64_t i = 1; OB_SUCC(ret) && i < partition_num; i++) {}
//    LOG_WARN("", K(alter_table_schema));
//  }
//
//  return ret;
//}

int ObAlterTableResolver::generate_index_arg(obrpc::ObCreateIndexArg& index_arg, const bool is_unique_key)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(session_info_) || OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info should not be null", K(session_info_), K(alter_table_stmt));
  } else {
    // add storing column
    for (int32_t i = 0; OB_SUCC(ret) && i < store_column_names_.count(); ++i) {
      if (OB_FAIL(index_arg.store_columns_.push_back(store_column_names_.at(i)))) {
        SQL_RESV_LOG(WARN, "failed to add storing column!", "column_name", store_column_names_.at(i), K(ret));
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < hidden_store_column_names_.count(); ++i) {
      if (OB_FAIL(index_arg.hidden_store_columns_.push_back(hidden_store_column_names_.at(i)))) {
        SQL_RESV_LOG(WARN, "failed to add storing column!", "column_name", hidden_store_column_names_.at(i), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_arg.fulltext_columns_.assign(fulltext_column_names_))) {
        LOG_WARN("assign fulltext columns failed", K(ret));
      }
    }
    index_arg.tenant_id_ = session_info_->get_effective_tenant_id();
    index_arg.table_name_ = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_table_name();
    index_arg.database_name_ = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_database_name();
    // set index option
    index_arg.index_option_.block_size_ = block_size_;
    index_arg.index_option_.use_bloom_filter_ = use_bloom_filter_;
    index_arg.index_option_.compress_method_ = compress_method_;
    index_arg.index_option_.row_store_type_ = row_store_type_;
    index_arg.index_option_.store_format_ = store_format_;
    index_arg.index_option_.storage_format_version_ = storage_format_version_;
    index_arg.index_option_.comment_ = comment_;
    index_arg.with_rowid_ = with_rowid_;
    if (OB_SUCC(ret)) {
      ObIndexType type = INDEX_TYPE_IS_NOT;
      if (!index_arg.fulltext_columns_.empty()) {
        if (is_unique_key) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "UNIQUE index option in domain CTXCAT index");
        } else if (index_keyname_ != DOMAIN_KEY) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "CTXCAT without domain index");
        } else if (index_scope_ != NOT_SPECIFIED) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "GLOBAL/LOCAL option in domain CTXCAT index");
        } else {
          type = INDEX_TYPE_DOMAIN_CTXCAT;
        }
        if (OB_SUCC(ret) && parser_name_.empty()) {
          index_arg.index_option_.parser_name_ = common::ObString::make_string(common::OB_DEFAULT_FULLTEXT_PARSER_NAME);
        }
      } else {
        global_ = (index_scope_ != LOCAL_INDEX);
        if (is_unique_key) {
          if (global_) {
            type = INDEX_TYPE_UNIQUE_GLOBAL;
          } else {
            type = INDEX_TYPE_UNIQUE_LOCAL;
          }
        } else {
          if (global_) {
            type = INDEX_TYPE_NORMAL_GLOBAL;
          } else {
            type = INDEX_TYPE_NORMAL_LOCAL;
          }
        }
      }
      index_arg.index_type_ = type;
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_index(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_DROP != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode* index_node = node.children_[0];
    if (OB_ISNULL(index_node) || T_IDENT != index_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObString drop_index_name;
      drop_index_name.assign_ptr(index_node->str_value_, static_cast<int32_t>(index_node->str_len_));
      // construct ObDropIndexArg
      ObDropIndexArg* drop_index_arg = NULL;
      void* tmp_ptr = NULL;
      if (NULL == (tmp_ptr = (ObDropIndexArg*)allocator_->alloc(sizeof(obrpc::ObDropIndexArg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        drop_index_arg = new (tmp_ptr) ObDropIndexArg();
        drop_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        drop_index_arg->index_name_ = drop_index_name;
        ObObj is_recyclebin;
        if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_RECYCLEBIN, is_recyclebin))) {
          SQL_RESV_LOG(WARN, "get sys variable SYS_VAR_RECYCLEBIN failed", K(ret));
        } else {
          drop_index_arg->to_recyclebin_ = is_recyclebin.get_bool();
        }
      }
      // push drop index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (table_schema_->is_parent_table() || table_schema_->is_child_table()) {
          const ObTableSchema* index_table_schema = NULL;
          ObString index_table_name;
          ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
          bool has_other_indexes_on_same_cols = false;
          if (OB_FAIL(ObTableSchema::build_index_table_name(
                  allocator, table_schema_->get_table_id(), drop_index_name, index_table_name))) {
            LOG_WARN("build_index_table_name failed", K(ret), K(table_schema_->get_table_id()), K(drop_index_name));
          } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                         alter_table_stmt->get_org_database_name(),
                         index_table_name,
                         true /* index table */,
                         index_table_schema))) {
            if (OB_TABLE_NOT_EXIST == ret) {
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                  to_cstring(alter_table_stmt->get_org_database_name()),
                  to_cstring(alter_table_stmt->get_org_table_name()));
            }
            LOG_WARN("fail to get index table schema", K(ret));
          } else if (OB_ISNULL(index_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret));
          } else if (OB_FAIL(check_indexes_on_same_cols(
                         *table_schema_, *index_table_schema, *schema_checker_, has_other_indexes_on_same_cols))) {
            LOG_WARN("check indexes on same cols failed", K(ret));
          } else if (!has_other_indexes_on_same_cols && share::is_mysql_mode()) {
            if (OB_FAIL(check_index_columns_equal_foreign_key(*table_schema_, *index_table_schema))) {
              LOG_WARN("failed to check_index_columns_equal_foreign_key", K(ret), K(index_table_name));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_stmt->add_index_arg(drop_index_arg))) {
            SQL_RESV_LOG(WARN, "add index to drop_index_list failed!", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_foreign_key(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ParseNode* name_node = NULL;
  ObDropForeignKeyArg* foreign_key_arg = NULL;
  ObString foreign_key_name;
  void* tmp_ptr = NULL;
  if ((share::is_mysql_mode() && (T_FOREIGN_KEY_DROP != node.type_ || OB_ISNULL(node.children_))) ||
      (share::is_oracle_mode() && (T_DROP_CONSTRAINT != node.type_ || OB_ISNULL(node.children_)))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(node.type_), K(ret));
  } else if (OB_ISNULL(name_node = node.children_[0]) || T_IDENT != name_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), KP(name_node), K(name_node->type_));
  } else if (OB_ISNULL(name_node->str_value_) || name_node->str_len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", KP(name_node->str_value_), K(name_node->str_len_), K(ret));
  } else if (OB_ISNULL(tmp_ptr = allocator_->alloc(sizeof(ObDropForeignKeyArg)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
  } else if (FALSE_IT(foreign_key_arg = new (tmp_ptr) ObDropForeignKeyArg())) {
  } else if (FALSE_IT(foreign_key_arg->foreign_key_name_.assign_ptr(
                 name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_FAIL(alter_table_stmt->add_index_arg(foreign_key_arg))) {
    SQL_RESV_LOG(WARN, "add index to drop_index_list failed!", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_constraint(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (share::is_mysql_mode()) {
    const ParseNode* name_list = node.children_[0];
    if (OB_ISNULL(name_list)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      }
      AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObConstraint cst;
        ObString constraint_name(
            static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
        if (OB_FAIL(cst.set_constraint_name(constraint_name))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "set constraint name failed", K(constraint_name), K(ret));
        } else if (OB_FAIL(alter_table_schema.add_constraint(cst))) {
          SQL_RESV_LOG(WARN, "add constraint failed!", K(cst), K(ret));
        }
      }
    }
  } else if (share::is_oracle_mode()) {
    const ParseNode* constraint_name = node.children_[0];
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      ObConstraint cst;
      ObString constraint_name_str(static_cast<int32_t>(constraint_name->str_len_), constraint_name->str_value_);
      ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
      for (; OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
        if (0 == constraint_name_str.case_compare((*iter)->get_constraint_name_str())) {
          if (OB_FAIL(cst.assign(**iter))) {
            SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
          }
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (table_schema_->constraint_end() == iter) {
        ret = OB_ERR_NONEXISTENT_CONSTRAINT;
        SQL_RESV_LOG(WARN,
            "Cannot drop check constraint - nonexistent constraint",
            K(ret),
            K(constraint_name_str),
            K(table_schema_->get_table_name_str()));
      } else if (OB_FAIL(alter_table_schema.add_constraint(cst))) {
        SQL_RESV_LOG(WARN, "add constraint failed!", K(cst), K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_partition(const ParseNode& node, const ObTableSchema& orig_table_schema)
{
  int ret = OB_SUCCESS;
  if ((T_ALTER_PARTITION_DROP != node.type_ && T_ALTER_PARTITION_TRUNCATE != node.type_) || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (orig_table_schema.is_hash_like_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop hash partition");
  } else {
    const ParseNode* name_list = node.children_[0];
    if (OB_ISNULL(name_list)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      }
      AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObPartition part;
        ObString partition_name(
            static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
        if (OB_FAIL(part.set_part_name(partition_name))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "set partition name failed", K(partition_name), K(ret));
        } else if (OB_FAIL(alter_table_schema.check_part_name(part))) {
          SQL_RESV_LOG(WARN, "check part name failed!", K(part), K(ret));
        } else if (OB_FAIL(alter_table_schema.add_partition(part))) {
          SQL_RESV_LOG(WARN, "add partition failed!", K(part), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        alter_table_schema.set_part_level(orig_table_schema.get_part_level());
        alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
        alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_subpartition(const ParseNode& node, const ObTableSchema& orig_table_schema)
{
  int ret = OB_SUCCESS;
  if ((T_ALTER_SUBPARTITION_DROP != node.type_ && T_ALTER_SUBPARTITION_TRUNCATE != node.type_) ||
      OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    ret = OB_ERR_NOT_COMPOSITE_PARTITION;
    LOG_WARN("table is not partitioned by composite partition method", K(ret));
  } else if (orig_table_schema.is_sub_part_template()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop subpartition on subpart template table");
  } else if (orig_table_schema.is_hash_like_subpart()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop hash subpartition");
  } else {
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    const ParseNode* name_list = node.children_[0];
    if (OB_ISNULL(name_list) || OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(name_list), K(alter_table_stmt));
    } else {
      AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      ObPartition dummy_part;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObSubPartition subpart;
        ObString partition_name(
            static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
        if (OB_FAIL(subpart.set_part_name(partition_name))) {
          LOG_WARN("failed to set subpart name", K(ret), K(partition_name));
        } else if (OB_FAIL(check_subpart_name(dummy_part, subpart))) {
          LOG_WARN("failed to check subpart name", K(subpart), K(ret));
        } else if (OB_FAIL(dummy_part.add_partition(subpart))) {
          LOG_WARN("failed to add partition", K(subpart), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        dummy_part.set_sub_part_num(name_list->num_child_);
        if (OB_FAIL(alter_table_schema.add_partition(dummy_part))) {
          LOG_WARN("failed to add partition", K(ret));
        } else {
          alter_table_schema.set_part_level(orig_table_schema.get_part_level());
          alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
          alter_table_schema.get_sub_part_option() = orig_table_schema.get_sub_part_option();
          alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_subpart_name(const ObPartition& partition, const ObSubPartition& subpartition)
{
  int ret = OB_SUCCESS;
  const ObString& subpart_name = subpartition.get_part_name();
  for (int64_t i = 0; OB_SUCC(ret) && i < partition.get_subpartition_num(); ++i) {
    if (common::ObCharset::case_insensitive_equal(subpart_name, partition.get_subpart_array()[i]->get_part_name())) {
      ret = OB_ERR_SAME_NAME_PARTITION;
      LOG_WARN("subpart name is duplicate",
          K(ret),
          K(subpartition),
          K(i),
          "exists partition",
          partition.get_subpart_array()[i]);
      LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, subpart_name.length(), subpart_name.ptr());
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_ALTER != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode* index_node = node.children_[0];
    ParseNode* visibility_node = node.children_[1];
    if (OB_ISNULL(index_node) || T_IDENT != index_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid index node", KP(index_node), K(ret));
    } else if (OB_ISNULL(visibility_node) ||
               (T_VISIBLE != visibility_node->type_ && T_INVISIBLE != visibility_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid visibility node", KP(visibility_node), K(ret));
    } else {
      ObString alter_index_name;
      alter_index_name.assign_ptr(index_node->str_value_, static_cast<int32_t>(index_node->str_len_));
      // construct ObAlterIndexArg
      ObAlterIndexArg* alter_index_arg = NULL;
      void* tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexArg*)allocator_->alloc(sizeof(obrpc::ObAlterIndexArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_arg = new (tmp_ptr) ObAlterIndexArg();
        alter_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_arg->index_name_ = alter_index_name;
        alter_index_arg->index_visibility_ = T_VISIBLE == visibility_node->type_ ? 0 : 1;
      }
      // push drop index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (table_schema_->is_parent_table() || table_schema_->is_child_table()) {
          ObString index_table_name;
          ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
          const ObTableSchema* index_table_schema = NULL;
          if (OB_FAIL(ObTableSchema::build_index_table_name(
                  allocator, table_schema_->get_table_id(), alter_index_name, index_table_name))) {
            LOG_WARN("build_index_table_name failed", K(table_schema_->get_table_id()), K(alter_index_name), K(ret));
          } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                         alter_table_stmt->get_org_database_name(),
                         index_table_name,
                         true /* index table */,
                         index_table_schema))) {
            if (OB_TABLE_NOT_EXIST == ret) {
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                  to_cstring(alter_table_stmt->get_org_database_name()),
                  to_cstring(alter_table_stmt->get_org_table_name()));
            }
            LOG_WARN("fail to get index table schema", K(ret), K(index_table_name));
          } else if (OB_ISNULL(index_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret), K(index_table_name));
          } else if (share::is_mysql_mode() &&
                     OB_FAIL(check_index_columns_equal_foreign_key(*table_schema_, *index_table_schema))) {
            LOG_WARN("failed to check_index_columns_equal_foreign_key", K(ret), K(index_table_schema));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_arg))) {
            SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index_parallel_oracle(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObString index_name;
  if (T_PARALLEL != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (OB_ISNULL(node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node is null", K(ret));
  } else if (OB_ISNULL(index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the index schema is null", K(ret));
  } else if (OB_FAIL(index_schema_->get_index_name(index_name))) {
    LOG_WARN("failed get index name", K(ret));
  } else {
    int64_t index_dop = node.children_[0]->value_;
    LOG_DEBUG("alter index table dop", K(ret), K(index_dop), K(index_name), K(index_schema_->get_table_name()));
    if (index_dop <= 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "The value of table dop should greater than 0");
    } else {
      ObAlterIndexParallelArg* alter_index_parallel_arg = NULL;
      void* tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexParallelArg*)allocator_->alloc(
                                   sizeof(obrpc::ObAlterIndexParallelArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_parallel_arg = new (tmp_ptr) ObAlterIndexParallelArg();
        alter_index_parallel_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_parallel_arg->new_parallel_ = index_dop;  // update index dop
        alter_index_parallel_arg->index_name_ = index_name;   // update index name
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_parallel_arg))) {
          SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index_parallel_mysql(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (!share::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the mode is not mysql", K(ret));
  } else if (node.type_ != T_INDEX_ALTER_PARALLEL || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(
        WARN, "the type is not right or the children is null", K(ret), K(node.type_), K(node.children_ == NULL));
  } else {
    ParseNode* mysql_index_name_node = node.children_[0];
    ParseNode* parallel_node = node.children_[1];

    if (OB_ISNULL(mysql_index_name_node) || T_IDENT != mysql_index_name_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid mysql index name node", K(ret), KP(mysql_index_name_node));
    } else if (OB_ISNULL(parallel_node) || T_PARALLEL != parallel_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid mysql parallel node", K(ret), KP(parallel_node));
    } else if (OB_ISNULL(parallel_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parallel_node children node", K(ret), KP(parallel_node->children_));
    } else if (OB_ISNULL(parallel_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "child node is null", K(ret));
    } else if (parallel_node->children_[0]->value_ < 1) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "the parallel is invalid", K(ret), K(parallel_node->children_[0]->value_));
    } else {
      ObString index_name;
      index_name.assign_ptr(mysql_index_name_node->str_value_, static_cast<int32_t>(mysql_index_name_node->str_len_));
      ObAlterIndexParallelArg* alter_index_parallel_arg = NULL;
      void* tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexParallelArg*)allocator_->alloc(
                                   sizeof(obrpc::ObAlterIndexParallelArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_parallel_arg = new (tmp_ptr) ObAlterIndexParallelArg();
        alter_index_parallel_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_parallel_arg->new_parallel_ = parallel_node->children_[0]->value_;
        alter_index_parallel_arg->index_name_ = index_name;  // update index name
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_parallel_arg))) {
          SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_rename_index(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_RENAME != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode* index_node = node.children_[0];
    ParseNode* new_name_node = node.children_[1];
    if (share::is_mysql_mode() && (OB_ISNULL(index_node) || T_IDENT != index_node->type_ || OB_ISNULL(new_name_node) ||
                                      T_IDENT != new_name_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid index node", K(ret), KP(index_node));
    } else if (share::is_oracle_mode() && (OB_ISNULL(index_schema_) || index_schema_->get_table_name_str().empty() ||
                                              OB_ISNULL(new_name_node) || T_IDENT != new_name_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "index_schema_ is null or invalid index node", K(ret), KP(index_node), KPC(index_schema_));
    } else {
      // should check new index name length
      int32_t len = static_cast<int32_t>(new_name_node->str_len_);
      ObString tmp_new_index_name(len, len, new_name_node->str_value_);
      ObCollationType cs_type = CS_TYPE_INVALID;
      ObRenameIndexArg* rename_index_arg = NULL;
      if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation connection", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_index_name(cs_type, tmp_new_index_name))) {
        LOG_WARN("fail to check index name", K(tmp_new_index_name), K(ret));
      } else {
        ObString ori_index_name;
        ObString new_index_name;
        if (share::is_mysql_mode()) {
          ori_index_name.assign_ptr(index_node->str_value_, static_cast<int32_t>(index_node->str_len_));
        } else if (share::is_oracle_mode()) {
          if (OB_FAIL(index_schema_->get_index_name(ori_index_name))) {
            LOG_WARN("fail to get origin index name", K(ret));
          }
        }
        new_index_name.assign_ptr(new_name_node->str_value_, static_cast<int32_t>(new_name_node->str_len_));
        void* tmp_ptr = NULL;

        if (OB_UNLIKELY(NULL == (tmp_ptr = (ObRenameIndexArg*)allocator_->alloc(sizeof(obrpc::ObRenameIndexArg))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
        } else {
          rename_index_arg = new (tmp_ptr) ObRenameIndexArg();
          rename_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
          rename_index_arg->origin_index_name_ = ori_index_name;
          rename_index_arg->new_index_name_ = new_index_name;
        }
      }

      // push index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(rename_index_arg))) {
          SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
        }
      }  // end for pushing index arg
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_primary(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_PRIMARY_KEY != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "table_schema is null", K(ret));
  } else if (table_schema_->is_no_pk_table()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "not support to add primary key!", K(ret));
  } else {
    ret = OB_ERR_MULTIPLE_PRI_KEY;
    SQL_RESV_LOG(WARN, "multiple primary key defined", K(ret));
  }
#if 0
  // TODO: uncomment following after support alter table with primary key
  else {
    obrpc::ObCreateIndexArg *create_index_arg = NULL;
    void *tmp_ptr = NULL;
    if (NULL == (tmp_ptr = (ObCreateIndexArg *)allocator_->alloc(sizeof(obrpc::ObCreateIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
    } else {
      create_index_arg = new (tmp_ptr) ObCreateIndexArg();
    }
    ParseNode *column_list = node.children_[0];
    if (OB_ISNULL(column_list) || T_COLUMN_LIST != column_list->type_ ||
        OB_ISNULL(column_list->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    }
    obrpc::ObColumnSortItem sort_item;
    for (int32_t i = 0; OB_SUCC(ret) && i < column_list->num_child_; ++i) {
      ParseNode *column_node = column_list->children_[i];
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else {
        sort_item.reset();
        sort_item.column_name_.assign_ptr(column_node->str_value_,
                                          static_cast<int32_t>(column_node->str_len_));
        sort_item.prefix_len_ = 0;
        sort_item.order_type_ = common::ObOrderType::ASC;
        if (OB_FAIL(add_sort_column(sort_item, *create_index_arg))) {
          SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      create_index_arg->index_type_ = INDEX_TYPE_PRIMARY;
      create_index_arg->index_name_.assign_ptr(common::OB_PRIMARY_INDEX_NAME,
                                               static_cast<int32_t>(strlen(common::OB_PRIMARY_INDEX_NAME)));
      create_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_index_arg(create_index_arg))) {
        SQL_RESV_LOG(WARN, "push back index arg failed", K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObAlterTableResolver::resolve_index_options_oracle(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_INDEX_OPTION_ORACLE != node.type_ || node.num_child_ <= 0 || OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    switch (node.children_[0]->type_) {
      case T_INDEX_RENAME: {
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_rename_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve rename index error!", K(ret));
        }
        break;
      }
      case T_PARALLEL: {
        // alter index parallel for oracle
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_alter_index_parallel_oracle(*index_node))) {
          SQL_RESV_LOG(WARN, "resolve alter index parallel error!", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Unknown index option type!", "option type", node.children_[0]->type_, K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_index_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_INDEX_OPTION != node.type_ || node.num_child_ <= 0 || OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    switch (node.children_[0]->type_) {
      case T_INDEX_ADD: {
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_add_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve add index error!", K(ret));
        }
        break;
      }
      case T_INDEX_DROP: {
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_drop_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve drop index error!", K(ret));
        }
        break;
      }
      case T_PRIMARY_KEY: {
        ParseNode* primary_key_node = node.children_[0];
        if (OB_FAIL(resolve_add_primary(*primary_key_node))) {
          SQL_RESV_LOG(WARN, "failed to resovle primary key!", K(ret));
        }
        break;
      }
      case T_INDEX_ALTER: {
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_alter_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve alter index error!", K(ret));
        }
        break;
      }
      case T_INDEX_RENAME: {
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_rename_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve rename index error!", K(ret));
        }
        break;
      }
      case T_INDEX_ALTER_PARALLEL: {
        // alter index parallel for mysql
        ParseNode* index_node = node.children_[0];
        if (OB_FAIL(resolve_alter_index_parallel_mysql(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve alter index parallel error!", K(ret));
        }
        break;
      }
      case T_CHECK_CONSTRAINT: {
        const ParseNode* check_cst_node = node.children_[0];
        if (OB_FAIL(resolve_constraint_options(*check_cst_node))) {
          SQL_RESV_LOG(WARN, "Resolve check constraint option in mysql mode failed!", K(ret));
        }
        break;
      }
      case T_FOREIGN_KEY: {
        ObCreateForeignKeyArg foreign_key_arg;
        ParseNode* foreign_key_action_node = node.children_[0];
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        ObSchemaGetterGuard* schema_guard = schema_checker_->get_schema_guard();
        if (OB_FAIL(resolve_foreign_key_node(foreign_key_action_node, foreign_key_arg, true))) {
          SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret));
        } else if (OB_FAIL(check_dup_foreign_keys_exist(schema_guard, foreign_key_arg))) {
          SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret));
        } else if (OB_FAIL(alter_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
          SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
        }
        break;
      }
      case T_MODIFY_CONSTRAINT_OPTION: {
        ObSchemaGetterGuard* schema_guard = schema_checker_->get_schema_guard();
        ParseNode* cons_state_node = node.children_[0];
        ParseNode* constraint_name_node = NULL;
        uint64_t constraint_id = OB_INVALID_ID;
        ObString constraint_name;
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
        } else if (OB_ISNULL(constraint_name_node = cons_state_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "constraint_name_node is null", K(ret));
        } else {
          constraint_name.assign_ptr(
              constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_guard->get_foreign_key_id(table_schema_->get_tenant_id(),
                       table_schema_->get_database_id(),
                       constraint_name,
                       constraint_id))) {
          LOG_WARN("get foreign key id failed",
              K(ret),
              K(table_schema_->get_tenant_id()),
              K(table_schema_->get_database_id()),
              K(constraint_name));
        } else if (OB_INVALID_ID != constraint_id) {
          ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
          if (OB_ISNULL(alter_table_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "alter_table_stmt is null", K(ret));
          } else if (OB_FAIL(resolve_modify_foreign_key_state(cons_state_node))) {
            LOG_WARN("modify foreign key state failed", K(ret));
          }
        } else {
          if (OB_FAIL(schema_guard->get_constraint_id(
                  table_schema_->get_tenant_id(), table_schema_->get_database_id(), constraint_name, constraint_id))) {
            LOG_WARN("get constraint id failed",
                K(ret),
                K(table_schema_->get_tenant_id()),
                K(table_schema_->get_database_id()),
                K(constraint_name));
          } else if (OB_INVALID_ID != constraint_id) {
            if (OB_FAIL(resolve_modify_check_constraint_state(cons_state_node))) {
              LOG_WARN("modify check constraint state failed", K(ret));
            }
          } else {  // OB_INVALID_ID == constraint_id
            ret = OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT;
            SQL_RESV_LOG(WARN,
                "Cannot modify constraint - nonexistent constraint",
                K(ret),
                K(constraint_name),
                K(table_schema_->get_table_name_str()));
            LOG_USER_ERROR(OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT, constraint_name.length(), constraint_name.ptr());
          }
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Unknown index option type!", "option type", node.children_[0]->type_, K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_dup_foreign_keys_exist(
    share::schema::ObSchemaGetterGuard* schema_guard, const obrpc::ObCreateForeignKeyArg& foreign_key_arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> child_column_ids;
  ObSEArray<uint64_t, 4> parent_column_ids;
  uint64_t parent_table_id = OB_INVALID_ID;
  const ObTableSchema* parent_table_schema = NULL;

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_schema_->get_tenant_id(),
                 foreign_key_arg.parent_database_,
                 foreign_key_arg.parent_table_,
                 false,
                 parent_table_schema))) {
    SQL_RESV_LOG(WARN, "get table schema failed", K(ret));
  } else if (OB_ISNULL(parent_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "parent_table_schema is null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < foreign_key_arg.child_columns_.count(); ++i) {
      const ObColumnSchemaV2* child_column_schema =
          table_schema_->get_column_schema(foreign_key_arg.child_columns_.at(i));
      const ObColumnSchemaV2* parent_column_schema =
          parent_table_schema->get_column_schema(foreign_key_arg.parent_columns_.at(i));
      if (OB_ISNULL(child_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "child_column_schema is null", K(ret));
      } else if (OB_ISNULL(parent_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "parent_column_schema is null", K(ret));
      } else if (OB_FAIL(child_column_ids.push_back(child_column_schema->get_column_id()))) {
        SQL_RESV_LOG(WARN, "push back to child_column_ids failed", K(ret));
      } else if (OB_FAIL(parent_column_ids.push_back(parent_column_schema->get_column_id()))) {
        SQL_RESV_LOG(WARN, "push back to child_column_ids failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObResolverUtils::check_dup_foreign_keys_exist(table_schema_->get_foreign_key_infos(),
                            child_column_ids,
                            parent_column_ids,
                            parent_table_schema->get_table_id()))) {
      SQL_RESV_LOG(WARN, "check dup foreign keys exist failed", K(ret));
    }
  }

  return ret;
}

int ObAlterTableResolver::resolve_modify_foreign_key_state(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* constraint_name_node = NULL;
  ParseNode* rely_option_node = NULL;
  ParseNode* enable_option_node = NULL;
  ParseNode* validate_option_node = NULL;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  obrpc::ObCreateForeignKeyArg foreign_key_arg;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "enable_cons_node is null", K(ret));
  } else if (T_MODIFY_CONSTRAINT_OPTION != node->type_ || 4 != node->num_child_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
  } else if (OB_ISNULL(constraint_name_node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "constraint_name_node is null", K(ret));
  } else {
    rely_option_node = node->children_[1];
    enable_option_node = node->children_[2];
    validate_option_node = node->children_[3];
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rely_option_node) && OB_ISNULL(enable_option_node) && OB_ISNULL(validate_option_node)) {
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_RESV_LOG(WARN, "all options are null", K(ret));
  } else {
    foreign_key_arg.is_modify_fk_state_ = true;
    foreign_key_arg.foreign_key_name_.assign_ptr(
        constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
    const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema_->get_foreign_key_infos();
    const ObForeignKeyInfo* foreign_key_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      if (0 == foreign_key_arg.foreign_key_name_.case_compare(foreign_key_infos.at(i).foreign_key_name_) &&
          table_schema_->get_table_id() == foreign_key_infos.at(i).child_table_id_) {
        foreign_key_info = &foreign_key_infos.at(i);
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(foreign_key_info)) {
      ret = OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT;
      SQL_RESV_LOG(WARN,
          "Cannot modify constraint - nonexistent constraint",
          K(ret),
          K(foreign_key_arg.foreign_key_name_),
          K(table_schema_->get_table_name_str()));
      LOG_USER_ERROR(OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT,
          static_cast<int32_t>(constraint_name_node->str_len_),
          constraint_name_node->str_value_);
    } else {
      ObSchemaGetterGuard* schema_guard = schema_checker_->get_schema_guard();
      const ObDatabaseSchema* parent_db_schema = NULL;
      const ObTableSchema* parent_table_schema = NULL;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(foreign_key_info->parent_table_id_, parent_table_schema))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "get parent_table_schema failed", K(ret), K(foreign_key_info->parent_table_id_));
      } else if (OB_ISNULL(parent_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "parent_table_schema is null", K(ret), K(foreign_key_info->parent_table_id_));
      } else if (OB_FAIL(schema_guard->get_database_schema(parent_table_schema->get_database_id(), parent_db_schema))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "get parent_database_schema failed", K(ret), K(parent_table_schema->get_database_id()));
      } else if (OB_ISNULL(parent_db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "parent_db_schema is null", K(ret), K(parent_table_schema->get_database_id()));
      } else {
        const ObColumnSchemaV2* child_col = NULL;
        const ObColumnSchemaV2* parent_col = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_info->child_column_ids_.count(); ++i) {
          if (OB_ISNULL(child_col = table_schema_->get_column_schema(foreign_key_info->child_column_ids_.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "child column schema not exists", K(ret), K(foreign_key_info->child_column_ids_.at(i)));
          } else if (OB_ISNULL(parent_col = parent_table_schema->get_column_schema(
                                   foreign_key_info->parent_column_ids_.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(
                WARN, "parent column schema not exists", K(ret), K(foreign_key_info->parent_column_ids_.at(i)));
          } else if (OB_FAIL(foreign_key_arg.child_columns_.push_back(child_col->get_column_name_str()))) {
            SQL_RESV_LOG(WARN, "push back failed", K(ret), K(child_col->get_column_name_str()));
          } else if (OB_FAIL(foreign_key_arg.parent_columns_.push_back(parent_col->get_column_name_str()))) {
            SQL_RESV_LOG(WARN, "push back failed", K(ret), K(parent_col->get_column_name_str()));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(
                       *allocator_, parent_db_schema->get_database_name_str(), foreign_key_arg.parent_database_))) {
          SQL_RESV_LOG(
              WARN, "deep copy parent database name failed", K(ret), K(parent_db_schema->get_database_name_str()));
        } else if (OB_FAIL(ob_write_string(
                       *allocator_, parent_table_schema->get_table_name_str(), foreign_key_arg.parent_table_))) {
          SQL_RESV_LOG(
              WARN, "deep copy parent table name failed", K(ret), K(parent_table_schema->get_table_name_str()));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        if (OB_NOT_NULL(rely_option_node)) {
          if (T_RELY_CONSTRAINT == rely_option_node->type_ && !foreign_key_info->rely_flag_) {
            foreign_key_arg.is_modify_rely_flag_ = true;
            foreign_key_arg.rely_flag_ = true;
          } else if (T_NORELY_CONSTRAINT == rely_option_node->type_ && foreign_key_info->rely_flag_) {
            foreign_key_arg.is_modify_rely_flag_ = true;
            foreign_key_arg.rely_flag_ = false;
          }
        }
        if (OB_NOT_NULL(validate_option_node)) {
          if (T_VALIDATE_CONSTRAINT == validate_option_node->type_ && !foreign_key_info->validate_flag_) {
            foreign_key_arg.is_modify_validate_flag_ = true;
            foreign_key_arg.validate_flag_ = true;
          } else if (T_NOVALIDATE_CONSTRAINT == validate_option_node->type_ && foreign_key_info->validate_flag_) {
            foreign_key_arg.is_modify_validate_flag_ = true;
            foreign_key_arg.validate_flag_ = false;
          }
        }
        if (OB_NOT_NULL(enable_option_node)) {
          if (T_ENABLE_CONSTRAINT == enable_option_node->type_) {
            if (!foreign_key_info->enable_flag_) {
              foreign_key_arg.is_modify_enable_flag_ = true;
              foreign_key_arg.enable_flag_ = true;
            }
            if (OB_ISNULL(validate_option_node) && !foreign_key_info->validate_flag_) {
              foreign_key_arg.is_modify_validate_flag_ = true;
              foreign_key_arg.validate_flag_ = true;
            }
          } else if (T_DISABLE_CONSTRAINT == enable_option_node->type_) {
            if (foreign_key_info->enable_flag_) {
              foreign_key_arg.is_modify_enable_flag_ = true;
              foreign_key_arg.enable_flag_ = false;
            }
            if (OB_ISNULL(validate_option_node) && foreign_key_info->validate_flag_) {
              foreign_key_arg.is_modify_validate_flag_ = true;
              foreign_key_arg.validate_flag_ = false;
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alter_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
      SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
    }
  }

  return ret;
}

int ObAlterTableResolver::resolve_modify_check_constraint_state(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* constraint_name_node = NULL;
  ParseNode* rely_option_node = NULL;
  ParseNode* enable_option_node = NULL;
  ParseNode* validate_option_node = NULL;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ObConstraint cst;
  ObString cst_name;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "enable_cons_node is null", K(ret));
  } else if (T_MODIFY_CONSTRAINT_OPTION != node->type_ || 4 != node->num_child_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
  } else if (OB_ISNULL(constraint_name_node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "constraint_name_node is null", K(ret));
  } else {
    rely_option_node = node->children_[1];
    enable_option_node = node->children_[2];
    validate_option_node = node->children_[3];
    cst_name.assign_ptr(constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
    ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
    for (; OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
      if (0 == cst_name.case_compare((*iter)->get_constraint_name_str())) {
        if (OB_FAIL(cst.assign(**iter))) {
          SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (table_schema_->constraint_end() == iter) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(
          WARN, "can't find check constraint in table", K(ret), K(cst_name), K(table_schema_->get_table_name_str()));
    } else if (OB_ISNULL(rely_option_node) && OB_ISNULL(enable_option_node) && OB_ISNULL(validate_option_node)) {
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "all options are null", K(ret));
    } else {
      if (OB_NOT_NULL(rely_option_node)) {
        if (T_RELY_CONSTRAINT == rely_option_node->type_ && !cst.get_rely_flag()) {
          cst.set_is_modify_rely_flag(true);
          cst.set_rely_flag(true);
        } else if (T_NORELY_CONSTRAINT == rely_option_node->type_ && cst.get_rely_flag()) {
          cst.set_is_modify_rely_flag(true);
          cst.set_rely_flag(false);
        }
      }
      if (OB_NOT_NULL(validate_option_node)) {
        if (T_VALIDATE_CONSTRAINT == validate_option_node->type_ && !cst.get_validate_flag()) {
          cst.set_is_modify_validate_flag(true);
          cst.set_validate_flag(true);
        } else if (T_NOVALIDATE_CONSTRAINT == validate_option_node->type_ && cst.get_validate_flag()) {
          cst.set_is_modify_validate_flag(true);
          cst.set_validate_flag(false);
        }
      }
      if (OB_NOT_NULL(enable_option_node)) {
        if (T_ENABLE_CONSTRAINT == enable_option_node->type_) {
          if (!cst.get_enable_flag()) {
            cst.set_is_modify_enable_flag(true);
            cst.set_enable_flag(true);
          }
          if (OB_ISNULL(validate_option_node) && !cst.get_validate_flag()) {
            cst.set_is_modify_validate_flag(true);
            cst.set_validate_flag(true);
          }
        } else if (T_DISABLE_CONSTRAINT == enable_option_node->type_) {
          if (cst.get_enable_flag()) {
            cst.set_is_modify_enable_flag(true);
            cst.set_enable_flag(false);
          }
          if (OB_ISNULL(validate_option_node) && cst.get_validate_flag()) {
            cst.set_is_modify_validate_flag(true);
            cst.set_validate_flag(false);
          }
        }
      }
      if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_constraint(cst))) {
        SQL_RESV_LOG(WARN, "add constraint failed", K(ret));
      } else {
        alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::ALTER_CONSTRAINT_STATE;
        ++add_or_modify_check_cst_times_;
      }
    }
  }

  return ret;
}

// parse add/drop check constraint
int ObAlterTableResolver::resolve_constraint_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          (share::is_mysql_mode() && (T_ALTER_CHECK_CONSTRAINT_OPTION != node.type_ || OB_ISNULL(node.children_))) ||
          (share::is_oracle_mode() && (((T_DROP_CONSTRAINT != node.type_) && (T_CHECK_CONSTRAINT != node.type_)) ||
                                          OB_ISNULL(node.children_))))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
      const ParseNode* constraint_node = NULL;
      if (share::is_oracle_mode()) {
        constraint_node = &node;
      } else if (share::is_mysql_mode()) {
        constraint_node = node.children_[0];
      }
      if (OB_ISNULL(constraint_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "constraint_node is null", K(ret), K(constraint_node));
      } else {
        switch (constraint_node->value_) {
          case 0: {
            if (OB_FAIL(resolve_drop_constraint(*constraint_node))) {
              SQL_RESV_LOG(WARN, "Resolve drop constraint error!", K(ret));
            } else {
              alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::DROP_CONSTRAINT;
            }
            break;
          }
          case 1: {
            if (OB_SUCC(ret)) {
              if (OB_FAIL(resolve_add_constraint(*constraint_node))) {
                SQL_RESV_LOG(WARN, "Resolve add constraint error!", K(ret));
              } else {
                alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::ADD_CONSTRAINT;
                ++add_or_modify_check_cst_times_;
              }
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            SQL_RESV_LOG(WARN, "Unknown alter constraint option!", "option type", node.children_[0]->value_, K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_partition_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_PARTITION_OPTION != node.type_ || node.num_child_ <= 0 || OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    }

    if (OB_SUCC(ret)) {
      const ObPartitionLevel part_level = table_schema_->get_part_level();
      if (T_ALTER_PARTITION_PARTITIONED != node.children_[0]->type_ && PARTITION_LEVEL_ZERO == part_level) {
        ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
        LOG_WARN("unsupport add/drop management on non-partition table", K(ret));

      } else if (T_ALTER_PARTITION_PARTITIONED == node.children_[0]->type_ && PARTITION_LEVEL_ZERO != part_level) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't re-partitioned a partitioned table", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "re-partition a patitioned table");
      }
    }
    if (OB_SUCC(ret)) {
      ParseNode* partition_node = node.children_[0];
      // if (T_ALTER_PARTITION_PARTITIONED != node.children_[0]->type_
      //     && T_ALTER_PARTITION_TRUNCATE != node.children_[0]->type_
      //     && !table_schema_->is_range_part()
      //     && !table_schema_->is_list_part()) {
      //   ret = OB_NOT_SUPPORTED;
      //   SQL_RESV_LOG(WARN, "just support add/drop partition for range part", K(ret));
      // }
      switch (partition_node->type_) {
        case T_ALTER_PARTITION_ADD: {
          if (OB_FAIL(resolve_add_partition(*partition_node, *table_schema_))) {
            SQL_RESV_LOG(WARN, "Resolve add partition error!", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::ADD_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_ADD: {
          if (OB_FAIL(resolve_add_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve add subpartition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::ADD_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_DROP: {
          if (partition_node->num_child_ == 2 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("drop partition with update global indexes option not support yet", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop partition with update global indexes option ");
          } else if (OB_FAIL(resolve_drop_partition(*partition_node, *table_schema_))) {
            SQL_RESV_LOG(WARN, "Resolve drop partition error!", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::DROP_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_DROP: {
          if (OB_FAIL(resolve_drop_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve drop subpartition", K(ret));
          } else if (partition_node->num_child_ == 2 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("drop subpartition with update global indexes not support yet", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop subpartition with update global indexes option ");
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::DROP_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_PARTITIONED: {
          bool enable_split_partition = false;
          if (OB_FAIL(get_enable_split_partition(session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("partitioned table not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partitioned table");
          } else if (OB_FAIL(resolve_partitioned_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve partition option", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::PARTITIONED_TABLE;
          }
          break;
        }
        case T_ALTER_PARTITION_REORGANIZE: {
          bool enable_split_partition = false;
          if (!table_schema_->is_range_part() && !table_schema_->is_list_part()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_WARN(OB_NOT_SUPPORTED, "reorganize hash partition");
          } else if (OB_FAIL(get_enable_split_partition(
                         session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("reorganize partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "reorganize partition");
          } else if (OB_FAIL(resolve_reorganize_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve reorganize partition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::REORGANIZE_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_SPLIT: {
          bool enable_split_partition = false;
          if (!table_schema_->is_range_part() && !table_schema_->is_list_part()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_WARN(OB_NOT_SUPPORTED, "split hash partition");
          } else if (OB_FAIL(get_enable_split_partition(
                         session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("split partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "split partition");
          } else if (OB_FAIL(resolve_split_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve reorganize partition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::SPLIT_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_TRUNCATE: {
          ParseNode* partition_node = node.children_[0];
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("truncate partition not support yet", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "truncate partition ");
          } else if (OB_FAIL(resolve_drop_partition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve truncate partition", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::TRUNCATE_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_TRUNCATE: {
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2276) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("truncate subpartition not support yet", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "truncate subpartition ");
          } else if (OB_FAIL(resolve_drop_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve drop subpartition", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::TRUNCATE_SUB_PARTITION;
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          SQL_RESV_LOG(WARN, "Unknown alter partition option %d!", "option type", node.children_[0]->type_, K(ret));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool strict_mode = true;
      if (OB_FAIL(session_info_->is_create_table_strict_mode(strict_mode))) {
        SQL_RESV_LOG(WARN, "failed to get variable ob_create_table_strict_mode");
      } else {
        obrpc::ObCreateTableMode create_mode =
            strict_mode ? obrpc::OB_CREATE_TABLE_MODE_STRICT : obrpc::OB_CREATE_TABLE_MODE_LOOSE;
        alter_table_stmt->get_alter_table_arg().create_mode_ = create_mode;
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_partitioned_partition(const ParseNode* node, const ObTableSchema& origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node) || T_ALTER_PARTITION_PARTITIONED != node->type_ || OB_ISNULL(node->children_) ||
      1 != node->num_child_ || OB_ISNULL(node->children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc,
                 alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_table_name(),
                 origin_table_name))) {
    LOG_WARN("fail to wirte string", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc,
                 alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_database_name(),
                 origin_database_name))) {
    LOG_WARN("fail to wirte string", K(ret));
  } else {
    AlterTableSchema& table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    if (OB_FAIL(table_schema.assign(origin_table_schema))) {
      LOG_WARN("fail to assign", K(ret));
    } else if (OB_FAIL(resolve_partition_node(alter_table_stmt, node->children_[0], table_schema))) {
      LOG_WARN("failed to resolve partition option", K(ret));
    } else if (OB_FAIL(table_schema.check_primary_key_cover_partition_column())) {
      LOG_WARN("fail to check primary key cover partition column", K(ret));
    } else if (OB_FAIL(table_schema.set_origin_table_name(origin_table_name))) {
      LOG_WARN("fail to set origin table name", K(ret), K(origin_table_name));
    } else if (OB_FAIL(table_schema.set_origin_database_name(origin_database_name))) {
      LOG_WARN("fail to set origin database name", K(ret), K(origin_database_name));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_split_partition(
    const ParseNode* node, const share::schema::ObTableSchema& origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node) || T_ALTER_PARTITION_SPLIT != node->type_ || 2 != node->num_child_ ||
      OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[1]) || T_SPLIT_ACTION != node->children_[1]->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", K(ret));
  } else {
    ParseNode* name_list = node->children_[0];
    AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    if (OB_FAIL(ob_write_string(alloc, alter_table_schema.get_origin_table_name(), origin_table_name))) {
      LOG_WARN("fail to wirte string", K(ret));
    } else if (OB_FAIL(ob_write_string(alloc, alter_table_schema.get_origin_database_name(), origin_database_name))) {
      LOG_WARN("fail to wirte string", K(ret));
    } else if (OB_FAIL(alter_table_schema.assign(origin_table_schema))) {
      LOG_WARN("failed to assign table schema", K(ret), K(alter_table_schema));
    } else {
      alter_table_schema.reset_partition_schema();
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(name_list)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(node));
      } else {
        ObString partition_name(static_cast<int32_t>(name_list->str_len_), name_list->str_value_);
        if (OB_FAIL(alter_table_schema.set_split_partition_name(partition_name))) {
          LOG_WARN("failed to set split partition name", K(ret));
        }
      }
    }

    int64_t expr_count = OB_INVALID_PARTITION_ID;
    const ParseNode* split_node = node->children_[1];
    ParseNode* part_func_node = NULL;
    PartitionInfo part_info;
    int64_t expr_num = OB_INVALID_COUNT;
    if (OB_FAIL(mock_part_func_node(origin_table_schema, false /*is_sub_part*/, part_func_node))) {
      LOG_WARN("mock part func node failed", K(ret));
    } else if (OB_FAIL(resolve_part_func(params_,
                   part_func_node,
                   origin_table_schema.get_part_option().get_part_func_type(),
                   origin_table_schema,
                   part_info.part_func_exprs_,
                   part_info.part_keys_))) {
      LOG_WARN("resolve part func failed", K(ret));
    } else if (origin_table_schema.get_part_option().is_range_part()) {
      if (OB_FAIL(alter_table_stmt->get_part_fun_exprs().assign(part_info.part_func_exprs_))) {
        LOG_WARN("failed to assign func expr", K(ret));
      }
    } else if (origin_table_schema.get_part_option().is_list_part()) {
      if (OB_FAIL(alter_table_stmt->get_part_fun_exprs().assign(part_info.part_func_exprs_))) {
        LOG_WARN("failed to assign func expr", K(ret));
      }
    }
    LOG_DEBUG("succ to resolve_part_func", KPC(alter_table_stmt), K(part_info.part_func_exprs_), K(ret));

    /*T_SPLIT_ACTION
     *  - T_PARTITION_LIST
     *  - T_EXPR_LIST
     *  - T_SPLIT_LIST/T_SPLIT_RANGE()
     * */
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   check_split_type_valid(split_node, origin_table_schema.get_part_option().get_part_func_type()))) {
      LOG_WARN("failed to check split type valid", K(ret), K(origin_table_schema));
    } else if (OB_NOT_NULL(split_node->children_[AT_VALUES_NODE]) &&
               OB_NOT_NULL(split_node->children_[SPLIT_PARTITION_TYPE_NODE])) {
      // split at [into ()]
      expr_count = 1;
      if (OB_FAIL(resolve_split_at_partition(alter_table_stmt,
              split_node,
              origin_table_schema.get_part_option().get_part_func_type(),
              part_info.part_func_exprs_,
              alter_table_schema,
              expr_num))) {
        LOG_WARN("failed to resolve split at partition", K(ret));
      }
    } else if (OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE]) &&
               OB_NOT_NULL(split_node->children_[SPLIT_PARTITION_TYPE_NODE]) &&
               OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE]->children_[0])) {
      // split into ()
      const ParseNode* range_element_node = split_node->children_[PARTITION_DEFINE_NODE]->children_[0];
      if (OB_FAIL(resolve_split_into_partition(alter_table_stmt,
              range_element_node,
              origin_table_schema.get_part_option().get_part_func_type(),
              part_info.part_func_exprs_,
              expr_count,
              expr_num,
              alter_table_schema))) {
        LOG_WARN("failed to resolve split at partition", K(ret));
      }
    } else {
      ret = OB_ERR_MISS_AT_VALUES;
      LOG_WARN("miss at and less than values", K(ret));
      LOG_USER_ERROR(OB_ERR_MISS_AT_VALUES);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alter_table_schema.set_origin_table_name(origin_table_name))) {
      LOG_WARN("fail to set origin table name", K(ret), K(origin_table_name));
    } else if (OB_FAIL(alter_table_schema.set_origin_database_name(origin_database_name))) {
      LOG_WARN("fail to set origin database name", K(ret), K(origin_database_name));
    } else {
      alter_table_schema.set_part_level(origin_table_schema.get_part_level());
      alter_table_schema.get_part_option() = origin_table_schema.get_part_option();
      alter_table_schema.get_part_option().set_part_num(expr_count);
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_reorganize_partition(
    const ParseNode* node, const share::schema::ObTableSchema& origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node) || T_ALTER_PARTITION_REORGANIZE != node->type_ || 2 != node->num_child_ ||
      OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else {
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      ParseNode* name_list = node->children_[1];
      AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      // save atble_name
      if (OB_FAIL(ob_write_string(alloc, alter_table_schema.get_origin_table_name(), origin_table_name))) {
        LOG_WARN("fail to wirte string", K(ret));
      } else if (OB_FAIL(ob_write_string(alloc, alter_table_schema.get_origin_database_name(), origin_database_name))) {
        LOG_WARN("fail to wirte string", K(ret));
      } else if (OB_FAIL(alter_table_schema.assign(origin_table_schema))) {
        LOG_WARN("failed to assign table schema", K(ret), K(alter_table_schema));
      } else {
        alter_table_schema.reset_partition_schema();
      }
      // parse newly added node
      if (OB_SUCC(ret)) {
        // parse splited patitions
        if (OB_ISNULL(name_list)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(node));
        } else if (name_list->num_child_ != 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter table reorganize multi partition not supported now", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table reorganize multiple partitions");
        } else {
          ObPartition part;
          ObString partition_name(
              static_cast<int32_t>(name_list->children_[0]->str_len_), name_list->children_[0]->str_value_);
          if (OB_FAIL(alter_table_schema.set_split_partition_name(partition_name))) {
            LOG_WARN("failed to set split partition name", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // nothing
      } else if (OB_FAIL(resolve_add_partition(*node, origin_table_schema))) {
        LOG_WARN("failed to add partition", K(ret), K(origin_table_name));
      } else if (OB_FAIL(alter_table_schema.set_origin_table_name(origin_table_name))) {
        LOG_WARN("fail to set origin table name", K(ret), K(origin_table_name));
      } else if (OB_FAIL(alter_table_schema.set_origin_database_name(origin_database_name))) {
        LOG_WARN("fail to set origin database name", K(ret), K(origin_database_name));
      } else if (1 == alter_table_schema.get_partition_num()) {
        ret = OB_ERR_SPLIT_INTO_ONE_PARTITION;
        LOG_USER_ERROR(OB_ERR_SPLIT_INTO_ONE_PARTITION);
        LOG_WARN("can not split partition into one partition", K(ret), K(alter_table_schema));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_tablegroup_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_TABLEGROUP_OPTION != node.type_ || NULL == node.children_ || node.num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    ParseNode* tablegroup_action_node = node.children_[0];
    if (OB_ISNULL(tablegroup_action_node) || OB_UNLIKELY(T_TABLEGROUP_DROP != tablegroup_action_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else if (OB_FAIL(alter_table_bitset_.add_member(obrpc::ObAlterTableArg::TABLEGROUP_NAME))) {
      SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
    } else {
      tablegroup_name_ = ObString("");
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_foreign_key_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;

  if (share::is_mysql_mode()) {
    ParseNode* foreign_key_action_node = NULL;
    if (OB_UNLIKELY(T_ALTER_FOREIGN_KEY_OPTION != node.type_ || OB_ISNULL(node.children_) || node.num_child_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(node.type_), KP(node.children_), K(node.num_child_), K(ret));
    } else if (OB_ISNULL(foreign_key_action_node = node.children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", KP(foreign_key_action_node), K(ret));
    } else {
      if (T_FOREIGN_KEY_DROP == foreign_key_action_node->type_) {
        if (OB_FAIL(resolve_drop_foreign_key(*foreign_key_action_node))) {
          SQL_RESV_LOG(WARN, "resolve drop foreign key failed", K(ret));
        }
      } else if (T_FOREIGN_KEY == foreign_key_action_node->type_) {
        ObCreateForeignKeyArg foreign_key_arg;
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_FAIL(resolve_foreign_key_node(foreign_key_action_node, foreign_key_arg, true))) {
          SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret));
        } else if (OB_FAIL(alter_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
          SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(foreign_key_action_node->type_), K(ret));
      }
    }
  } else if (share::is_oracle_mode()) {
    if (OB_UNLIKELY(T_DROP_CONSTRAINT != node.type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(node.type_), K(ret));
    } else {
      if (OB_FAIL(resolve_drop_foreign_key(node))) {
        SQL_RESV_LOG(WARN, "resolve drop foreign key failed", K(ret));
      }
    }
  }

  return ret;
}

int ObAlterTableResolver::resolve_alter_table_option_list(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (node.num_child_ <= 0 || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode* option_node = NULL;
    int32_t num = node.num_child_;
    for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      option_node = node.children_[i];
      if (OB_ISNULL(option_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else if (OB_FAIL(resolve_table_option(option_node, false))) {
        SQL_RESV_LOG(WARN, "resolve table option failed", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::process_timestamp_column(ObColumnResolveStat& stat, AlterColumnSchema& alter_column_schema)
{
  int ret = OB_SUCCESS;
  const ObObj& cur_default_value = alter_column_schema.get_cur_default_value();
  bool explicit_value = false;
  if (NULL == session_info_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info is NULL", K(ret));
  } else if (OB_FAIL(alter_column_schema.set_orig_default_value(cur_default_value))) {
    SQL_RESV_LOG(WARN, "fail to set orig default value for alter table", K(ret), K(cur_default_value));
  } else if (OB_FAIL(session_info_->get_explicit_defaults_for_timestamp(explicit_value))) {
    LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
  } else if (true == explicit_value) {
    // nothing to do
  } else {
    alter_column_schema.check_timestamp_column_order_ = true;
    alter_column_schema.is_no_zero_date_ = is_no_zero_date(session_info_->get_sql_mode());
    alter_column_schema.is_set_nullable_ = stat.is_set_null_;
    alter_column_schema.is_set_default_ = stat.is_set_default_value_;
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_column(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  if (OB_UNLIKELY(T_COLUMN_ADD != node.type_ || NULL == node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "stmt should not be null!", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    ObColumnResolveStat stat;
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      if (OB_ISNULL(node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else {
        // resolve column definition
        stat.reset();
        bool is_modify_column_visibility = false;
        if (OB_FAIL(resolve_alter_table_column_definition(alter_column_schema,
                node.children_[i],
                stat,
                is_modify_column_visibility,
                true /*is_add_column*/,
                false,
                table_schema_->is_oracle_tmp_table()))) {
          SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
        } else if (OB_FAIL(fill_column_schema_according_stat(stat, alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to fill column schema", K(alter_column_schema), K(stat), K(ret));
          // TODO():hanlde add column c2 int unique key; support unique key
        } else if (OB_FAIL(set_column_collation(alter_column_schema))) {
          SQL_RESV_LOG(
              WARN, "fail to set column collation", "column_name", alter_column_schema.get_column_name(), K(ret));
        }

        // do some check
        if (OB_SUCC(ret)) {
          // TODO,  add column unique
          if (alter_column_schema.is_autoincrement_) {
            LOG_USER_WARN(OB_WARN_ADD_AUTOINCREMENT_COLUMN,
                alter_table_stmt->get_org_table_name().length(),
                alter_table_stmt->get_org_table_name().ptr(),
                alter_column_schema.get_column_name());
          }
          // resolve index (unique key only) of column
          if (alter_column_schema.is_unique_key_) {
            if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
              SQL_RESV_LOG(
                  WARN, "failed to resolve column index", "column name", alter_column_schema.get_column_name(), K(ret));
            }
          }
        }
        // add column
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_pos_column(const ParseNode* pos_node, share::schema::AlterColumnSchema& column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node can not be null", K(ret));
  } else if ((T_COLUMN_ADD_AFTER != pos_node->type_) && (T_COLUMN_ADD_BEFORE != pos_node->type_) &&
             (T_COLUMN_ADD_FIRST != pos_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operation type", K(ret));
  } else if (T_COLUMN_ADD_FIRST == pos_node->type_) {
    column.is_first_ = true;
  } else if (OB_ISNULL(pos_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node can not be null", K(ret));
  } else {
    ObString pos_column_name(
        static_cast<int32_t>(pos_node->children_[0]->str_len_), pos_node->children_[0]->str_value_);
    if (pos_column_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("position column name is empty", K(ret));
    } else if (T_COLUMN_ADD_AFTER == pos_node->type_) {
      column.set_prev_column_name(pos_column_name);
    } else {
      // T_COLUMN_ADD_BEFORE == pos_node->type_
      column.set_next_column_name(pos_column_name);
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_table_column_definition(AlterColumnSchema& column, ParseNode* node,
    ObColumnResolveStat& stat, bool& is_modify_column_visibility, const bool is_add_column, const bool is_modify_column,
    const bool is_oracle_temp_table)
{
  int ret = OB_SUCCESS;
  common::ObString pk_name;
  if (OB_FAIL(resolve_column_definition(column,
          node,
          stat,
          is_modify_column_visibility,
          pk_name,
          is_add_column,
          is_modify_column,
          is_oracle_temp_table))) {
    SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
  }  // else if (OB_FAIL(process_default_value(stat, column))) {
  //   SQL_RESV_LOG(WARN, "failed to set default value", K(ret));
  //   //when add new column, the default value should saved in both origin default value
  //   //and cur_default value
  //   //in resvolve_column_definition default value is saved in cur_default_value
  //   //TODO():hanlde add column c2 int unique key; support unique key
  // }
  if (OB_SUCC(ret)) {
    ParseNode* pos_node = NULL;
    CHECK_COMPATIBILITY_MODE(session_info_);
    if (lib::is_mysql_mode()) {
      if (OB_UNLIKELY(GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_)) {
        // generated column with pos_column
        pos_node = node->children_[5];
      } else {
        // normal column with pos_column
        pos_node = node->children_[3];
      }
      if (NULL != pos_node) {
        if (!is_add_column) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupport for first, before or after column", K(ret));
        } else if (OB_FAIL(resolve_pos_column(pos_node, column))) {
          LOG_WARN("fail to resove position column", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // In oracle mode: disallow default 'NULL' column: alter table modify col not null
    // while allow default 'not null' column: alter table modify col not null
    if (share::is_oracle_mode() && stat.is_set_not_null_ && column.get_cur_default_value().is_null()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support add or modify col not null on default null col in oracle mode now",
          K(ret),
          K(column.get_table_id()),
          K(column.get_column_name_str()),
          K(stat.is_set_not_null_),
          K(column.get_cur_default_value().is_null()));
      LOG_USER_WARN(
          OB_NOT_SUPPORTED, "add or modify col not null on default null col in oracle mode is not supported now");
    }
  }
  return ret;
}

int ObAlterTableResolver::set_column_collation(AlterColumnSchema& alter_column_schema)
{
  int ret = OB_SUCCESS;
  if (alter_column_schema.get_meta_type().is_string_type()) {
    ObCharsetType charset_type = alter_column_schema.get_charset_type();
    ObCollationType collation_type = alter_column_schema.get_collation_type();
    if (CHARSET_INVALID == charset_type && CS_TYPE_INVALID == collation_type) {
      // do nothing
    } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))) {
      SQL_RESV_LOG(WARN, "fail to fill charset collation info", K(ret));
    } else {
      alter_column_schema.set_charset_type(charset_type);
      alter_column_schema.set_collation_type(collation_type);
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_column(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_ALTER != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode* column_definition_ref_node = node.children_[0];
    AlterColumnSchema alter_column_schema;
    if (OB_ISNULL(column_definition_ref_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, column_definition_ref_node, true))) {
      LOG_WARN("invalid column definition ref node", K(ret));
    } else {
      alter_column_schema.alter_type_ = OB_DDL_ALTER_COLUMN;
    }
    // resolve the default value
    if (OB_SUCC(ret)) {
      ParseNode* default_node = node.children_[1];
      // may be T_CONSTR_NOT_NULL
      // alter table t1 alter c1 not null
      if (NULL == default_node || (T_CONSTR_DEFAULT != default_node->type_ && T_CONSTR_NULL != default_node->type_)) {
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else {
        ObObjParam default_value;
        if (T_CONSTR_NULL == default_node->type_) {
          default_value.set_null();
          alter_column_schema.is_drop_default_ = true;
        } else if (!share::is_oracle_mode() && ob_is_text_tc(alter_column_schema.get_data_type())) {
          ret = OB_INVALID_DEFAULT;
          SQL_RESV_LOG(WARN, "BLOB/TEXT can't set default value!", K(ret));
        } else if (OB_FAIL(resolve_default_value(default_node, default_value))) {
          SQL_RESV_LOG(WARN, "failed to resolve default value!", K(ret));
        }
        if (OB_SUCCESS == ret && OB_FAIL(alter_column_schema.set_cur_default_value(default_value))) {
          SQL_RESV_LOG(WARN, "failed to set current default to alter column schema!", K(ret));
        }
      }
    }
    // add atler alter_column schema
    if (OB_SUCC(ret)) {
      ObColumnResolveStat stat;
      ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(process_timestamp_column(stat, alter_column_schema))) {
        SQL_RESV_LOG(WARN, "fail to process timestamp column", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
        SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_change_column(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_CHANGE != node.type_ || OB_ISNULL(node.children_) || OB_ISNULL(node.children_[0]) ||
      OB_ISNULL(node.children_[1]) || T_COLUMN_DEFINITION != node.children_[1]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    const ObColumnSchemaV2* origin_col_schema = NULL;
    if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[0], true))) {
      LOG_WARN("check column definition ref node failed", K(ret));
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    }
    const ObString& origin_column_name = alter_column_schema.get_origin_column_name();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_checker_->get_column_schema(
                   table_schema_->get_table_id(), origin_column_name, origin_col_schema, false))) {
      if (ret == OB_ERR_BAD_FIELD_ERROR) {
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
            origin_column_name.length(),
            origin_column_name.ptr(),
            table_schema_->get_table_name_str().length(),
            table_schema_->get_table_name_str().ptr());
      }
      LOG_WARN("fail to get origin column schema", K(ret));
    }
    // resolve new column definition
    if (OB_SUCC(ret)) {
      ObColumnResolveStat stat;
      alter_column_schema.set_column_flags(origin_col_schema->get_column_flags());
      alter_column_schema.erase_generated_column_flags();
      bool is_modify_column_visibility = false;
      if (OB_FAIL(resolve_alter_table_column_definition(
              alter_column_schema, node.children_[1], stat, is_modify_column_visibility))) {
        SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
      } else {
        // Todo():hanlde change column c2 c3 int unique key; support unique key
        alter_column_schema.alter_type_ = OB_DDL_CHANGE_COLUMN;
        alter_column_schema.is_primary_key_ = stat.is_primary_key_;
        alter_column_schema.is_unique_key_ = stat.is_unique_key_;
        alter_column_schema.is_autoincrement_ = stat.is_autoincrement_;
        alter_column_schema.is_set_nullable_ = stat.is_set_null_;
        alter_column_schema.is_set_default_ = stat.is_set_default_value_;
        if (OB_FAIL(set_column_collation(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to set column collation", K(alter_column_schema), K(ret));
        }
      }
    }

    // resolve index (unique key only) of column
    if (OB_SUCC(ret) && alter_column_schema.is_unique_key_) {
      if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
        SQL_RESV_LOG(WARN,
            "failed to resolve column index",
            "origin column name",
            alter_column_schema.get_origin_column_name(),
            "new column name",
            alter_column_schema.get_column_name(),
            K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret) && lib::is_mysql_mode()) {
        if (0 != origin_col_schema->get_rowkey_position() && alter_column_schema.is_set_default_ &&
            alter_column_schema.get_cur_default_value().is_null()) {
          ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
          LOG_USER_ERROR(OB_ERR_PRIMARY_CANT_HAVE_NULL);
        }
      }
      if (OB_SUCC(ret) && lib::is_mysql_mode()) {
        bool is_sync_ddl_user = false;
        if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_dll_user", K(ret));
        } else if (is_sync_ddl_user) {
          // skip
        } else if (origin_col_schema->get_data_type() == alter_column_schema.get_data_type() &&
                   (origin_col_schema->get_data_type() == ObNumberType ||
                       origin_col_schema->get_data_type() == ObUNumberType) &&
                   (origin_col_schema->get_accuracy().get_precision() >
                           alter_column_schema.get_accuracy().get_precision() ||
                       origin_col_schema->get_accuracy().get_scale() >
                           alter_column_schema.get_accuracy().get_scale())) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cannot decrease precision or scale");
          SQL_RESV_LOG(WARN,
              "Can not decrease precision or scale",
              K(ret),
              K(alter_column_schema.get_accuracy()),
              KPC(origin_col_schema));
        }
      }
      if (OB_SUCC(ret)) {
        if ((origin_col_schema->get_data_type()) != (alter_column_schema.get_data_type())) {
          if (OB_FAIL(check_column_in_foreign_key(*table_schema_, alter_column_schema.get_origin_column_name()))) {
            SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_modify_column(
    const ParseNode& node, bool& is_modify_column_visibility, ObReducedVisibleColSet& reduced_visible_col_set)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_MODIFY != node.type_ || OB_ISNULL(node.children_) || OB_ISNULL(node.children_[0]) ||
      T_COLUMN_DEFINITION != node.children_[0]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    // resolve new column defintion
    ObColumnResolveStat stat;
    // TODO():hanlde modify column c2 int unique key; support unique key
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      stat.reset();
      const ObColumnSchemaV2* origin_col_schema = NULL;
      ObString column_name;
      ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
      if (OB_FAIL(check_column_definition_node(node.children_[i]))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[i]->children_[0], false))) {
        SQL_RESV_LOG(WARN, "fail to resolve column name from parse node", K(ret));
      } else {
        column_name = alter_column_schema.get_column_name();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(schema_checker_->get_column_schema(
                       table_schema_->get_table_id(), column_name, origin_col_schema, false))) {
          if (ret == OB_ERR_BAD_FIELD_ERROR) {
            LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                column_name.length(),
                column_name.ptr(),
                table_schema_->get_table_name_str().length(),
                table_schema_->get_table_name_str().ptr());
          }
          LOG_WARN("fail to get origin column schema", K(ret));
        } else if (share::is_oracle_mode() && OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
          LOG_WARN("fail to copy column schema", K(ret));
        } else {
          alter_column_schema.set_column_flags(origin_col_schema->get_column_flags());
          alter_column_schema.erase_generated_column_flags();
        }
      }
      if (OB_FAIL(ret)) {
        /*nothing*/
      } else if (OB_FAIL(resolve_alter_table_column_definition(alter_column_schema,
                     node.children_[i],
                     stat,
                     is_modify_column_visibility,
                     false /*is_add_column*/,
                     true /*is_modify_column*/,
                     table_schema_->is_oracle_tmp_table()))) {
        SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
      } else {
        alter_column_schema.alter_type_ = OB_DDL_MODIFY_COLUMN;
        alter_column_schema.is_primary_key_ = stat.is_primary_key_;
        alter_column_schema.is_unique_key_ = stat.is_unique_key_;
        alter_column_schema.is_autoincrement_ = stat.is_autoincrement_;
        alter_column_schema.is_set_nullable_ = stat.is_set_null_;
        alter_column_schema.is_set_default_ = stat.is_set_default_value_;
        if (OB_FAIL(alter_column_schema.set_origin_column_name(column_name))) {
          SQL_RESV_LOG(WARN, "failed to set origin column name", K(column_name), K(ret));
        } else if (OB_FAIL(set_column_collation(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to set column collation", K(alter_column_schema), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        bool is_sync_ddl_user = false;
        if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_dll_user", K(ret));
        } else if (is_sync_ddl_user) {
          // skip
        } else if (origin_col_schema->get_data_type() == alter_column_schema.get_data_type() &&
                   (origin_col_schema->get_data_type() == ObNumberType ||
                       origin_col_schema->get_data_type() == ObUNumberType ||
                       origin_col_schema->get_data_type() == ObNumberFloatType) &&
                   (origin_col_schema->get_accuracy().get_precision() >
                           alter_column_schema.get_accuracy().get_precision() ||
                       origin_col_schema->get_accuracy().get_scale() >
                           alter_column_schema.get_accuracy().get_scale())) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cannot decrease precision or scale");
          SQL_RESV_LOG(WARN,
              "Can not decrease precision or scale",
              K(ret),
              K(alter_column_schema.get_accuracy()),
              KPC(origin_col_schema));
        } else if (share::is_oracle_mode() &&
                   ((origin_col_schema->get_data_type() != alter_column_schema.get_data_type()) ||
                       (origin_col_schema->get_data_length() != alter_column_schema.get_data_length()))) {
          if (origin_col_schema->is_part_key_column()) {
            ret = OB_ERR_MODIFY_PART_COLUMN_TYPE;
            SQL_RESV_LOG(WARN, "data type or len of a part column may not be changed", K(ret));
          } else if (origin_col_schema->is_subpart_key_column()) {
            ret = OB_ERR_MODIFY_SUBPART_COLUMN_TYPE;
            SQL_RESV_LOG(WARN, "data type or len of a subpart column may not be changed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if ((origin_col_schema->get_data_type()) != (alter_column_schema.get_data_type())) {
            if (OB_FAIL(check_column_in_foreign_key(*table_schema_, alter_column_schema.get_origin_column_name()))) {
              SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
          }
        }
        if (OB_SUCC(ret) && lib::is_mysql_mode()) {
          if (0 != origin_col_schema->get_rowkey_position() && alter_column_schema.is_set_default_ &&
              alter_column_schema.get_cur_default_value().is_null()) {
            ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
            LOG_USER_ERROR(OB_ERR_PRIMARY_CANT_HAVE_NULL);
          }
        }
      }

      // resolve index (unique key only) of column
      if (OB_SUCC(ret) && alter_column_schema.is_unique_key_) {
        if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
          SQL_RESV_LOG(
              WARN, "failed to resolve column index", "column_name", alter_column_schema.get_column_name(), K(ret));
        }
      }
      if (OB_SUCC(ret) && lib::is_oracle_mode() && alter_column_schema.is_invisible_column()) {
        ObString name(node.children_[i]->children_[0]->children_[2]->str_len_,
            node.children_[i]->children_[0]->children_[2]->str_value_);
        ObColumnSchemaHashWrapper col_key(name);
        if (OB_FAIL(reduced_visible_col_set.set_refactored(col_key))) {
          SQL_RESV_LOG(
              WARN, "set foreign key name to hash set failed", K(ret), K(alter_column_schema.get_column_name_str()));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_column_index(const ObString& column_name)
{
  int ret = OB_SUCCESS;
  UNUSED(column_name);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter column add index");
#if 0
  sort_column_array_.reset();
  storing_column_set_.reset();
  obrpc::ObCreateIndexArg *create_index_arg = NULL;
  void *tmp_ptr = NULL;
  if (OB_ISNULL(allocator_)) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "allocator is null");
  } else if (NULL == (tmp_ptr = (ObCreateIndexArg *)allocator_->alloc(sizeof(obrpc::ObCreateIndexArg)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
  } else {
    create_index_arg = new (tmp_ptr) ObCreateIndexArg();
    obrpc::ObColumnSortItem sort_item;
    if (OB_FAIL(ob_write_string(*allocator_, column_name, sort_item.column_name_))) {
      SQL_RESV_LOG(WARN, "write index name failed", K(ret));
    } else if (OB_FAIL(add_sort_column(sort_item, *create_index_arg))) {
      SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
    } else if (table_schema_->has_hidden_primary_key() && table_schema_->is_partitioned_table() &&
               OB_FAIL(store_part_key(*table_schema_, create_index_arg*))) {
      LOG_WARN("failed to store part key", K(ret));
    } else {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      bool is_unique_key = true;
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(generate_index_arg(*create_index_arg, is_unique_key))) {
        SQL_RESV_LOG(WARN, "failed to generate index arg!", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_index_arg(create_index_arg))) {
        SQL_RESV_LOG(WARN, "push back index arg failed", K(ret));
      } else {
        alter_table_stmt->set_alter_table_index();
        storing_column_set_.reset();  //storing column for each index
        sort_column_array_.reset();   //column for each index
      }
    }
  }
#endif

  return ret;
}

int ObAlterTableResolver::resolve_drop_column(const ParseNode& node, ObReducedVisibleColSet& reduced_visible_col_set)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_DROP != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      if (OB_ISNULL(node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[i], true))) {
        LOG_WARN("check column definition ref node failed", K(ret));
      } else {
        alter_column_schema.alter_type_ = OB_DDL_DROP_COLUMN;
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (!share::is_oracle_mode() &&
                   OB_FAIL(check_column_in_foreign_key(*table_schema_, alter_column_schema.get_origin_column_name()))) {
          SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
        } else if (share::is_oracle_mode() &&
                   OB_FAIL(check_column_in_foreign_key_for_oracle(
                       *table_schema_, alter_column_schema.get_origin_column_name(), alter_table_stmt))) {
          SQL_RESV_LOG(WARN, "failed to check column in foreign key for oracle mode", K(ret));
        } else if (share::is_oracle_mode() &&
                   OB_FAIL(check_column_in_check_constraint_for_oracle(
                       *table_schema_, alter_column_schema.get_origin_column_name(), alter_table_stmt))) {
          SQL_RESV_LOG(WARN, "failed to check column in foreign key for oracle mode", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
        }
      }
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        const ObString& column_name = alter_column_schema.get_origin_column_name();
        ObColumnSchemaHashWrapper col_key(column_name);
        if (OB_FAIL(reduced_visible_col_set.set_refactored(col_key))) {
          SQL_RESV_LOG(WARN, "set foreign key name to hash set failed", K(ret), K(column_name));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::fill_column_schema_according_stat(
    const ObColumnResolveStat& stat, AlterColumnSchema& alter_column_schema)
{
  int ret = OB_SUCCESS;
  bool explicit_value = false;
  if (OB_UNLIKELY(NULL == session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info is NULL", K(ret));
  } else if (ObTimestampType == alter_column_schema.get_data_type()) {
    if (OB_FAIL(session_info_->get_explicit_defaults_for_timestamp(explicit_value))) {
      LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
    } else if (!explicit_value) {
      alter_column_schema.check_timestamp_column_order_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    alter_column_schema.is_no_zero_date_ = is_no_zero_date(session_info_->get_sql_mode());
    alter_column_schema.is_set_nullable_ = stat.is_set_null_;
    alter_column_schema.is_set_default_ = stat.is_set_default_value_;
    alter_column_schema.is_primary_key_ = stat.is_primary_key_;
    alter_column_schema.is_unique_key_ = stat.is_unique_key_;
    alter_column_schema.is_autoincrement_ = stat.is_autoincrement_;
    alter_column_schema.alter_type_ = OB_DDL_ADD_COLUMN;
  }

  return ret;
}

int ObAlterTableResolver::check_column_definition_node(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || T_COLUMN_DEFINITION != node->type_ || node->num_child_ < COLUMN_DEFINITION_NUM_CHILD ||
      OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0]) || T_COLUMN_REF != node->children_[0]->type_ ||
      COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse node", K(ret), K(node->type_), K(node->num_child_));
  }
  return ret;
}

int ObAlterTableResolver::resolve_rename_column(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_RENAME != node.type_ || OB_ISNULL(node.children_) || OB_ISNULL(node.children_[0]) ||
      T_COLUMN_REF != node.children_[0]->type_ || OB_ISNULL(node.children_[1]) || T_IDENT != node.children_[1]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    ObString new_column_name(node.children_[1]->str_len_, node.children_[1]->str_value_);
    AlterColumnSchema alter_column_schema;
    if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[0], true))) {
      LOG_WARN("resolve column definition ref failed", K(ret));
    } else if (0 == new_column_name.case_compare(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
          new_column_name.length(),
          new_column_name.ptr(),
          table_name_.length(),
          table_name_.ptr());
      LOG_WARN("invalid rowid column for rename stmt", K(ret));
    }
    const ObString origin_column_name = alter_column_schema.get_origin_column_name();
    const ObColumnSchemaV2* origin_col_schema = NULL;
    ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_column_schema(
                   table_schema_->get_table_id(), origin_column_name, origin_col_schema, false))) {
      if (ret == OB_ERR_BAD_FIELD_ERROR) {
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
            origin_column_name.length(),
            origin_column_name.ptr(),
            table_schema_->get_table_name_str().length(),
            table_schema_->get_table_name_str().ptr());
      }
      SQL_RESV_LOG(WARN, "fail to get origin column schema", K(ret));
    } else if (ObCharset::case_insensitive_equal(origin_column_name, new_column_name)) {
      ret = OB_ERR_FIELD_SPECIFIED_TWICE;
      LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(origin_column_name));
    } else if (OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
      SQL_RESV_LOG(WARN, "fail to copy column schema", K(ret));
    } else if (OB_FAIL(alter_column_schema.set_origin_column_name(origin_column_name))) {
      SQL_RESV_LOG(WARN, "fail to set origin column name", K(origin_column_name), K(ret));
    } else if (OB_FAIL(alter_column_schema.set_column_name(new_column_name))) {
      SQL_RESV_LOG(WARN, "fail to set new column name", K(new_column_name), K(ret));
    } else {
      alter_column_schema.alter_type_ = OB_DDL_CHANGE_COLUMN;
      if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
        SQL_RESV_LOG(WARN, "add alter column schema failed", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
