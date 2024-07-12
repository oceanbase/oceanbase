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
#include "sql/resolver/expr/ob_raw_expr_part_expr_checker.h"
#include "share/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_rpc_struct.h"
#include "sql/parser/ob_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase
{
using namespace share::schema;
using obrpc::ObCreateIndexArg;
using obrpc::ObDropIndexArg;
using namespace common;
using namespace obrpc;
namespace sql
{
ObAlterTableResolver::ObAlterTableResolver(ObResolverParams &params)
    : ObDDLResolver(params),
      table_schema_(NULL),
      index_schema_(NULL),
      current_index_name_set_(),
      add_or_modify_check_cst_times_(0),
      modify_constraint_times_(0),
      add_not_null_constraint_(false),
      add_column_cnt_(0)
{
}

ObAlterTableResolver::~ObAlterTableResolver()
{
}


int ObAlterTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session_info should not be null", K(ret));
  } else if (T_ALTER_TABLE != parse_tree.type_ ||
      ALTER_TABLE_NODE_COUNT != parse_tree.num_child_ ||
      OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), K(parse_tree.num_child_));
  } else {
    ObAlterTableStmt *alter_table_stmt = NULL;
    //create alter table stmt
    if (NULL == (alter_table_stmt = create_stmt<ObAlterTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create alter table stmt", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_tz_info_wrap(session_info_->get_tz_info_wrap()))) {
      SQL_RESV_LOG(WARN, "failed to set_tz_info_wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_nls_formats(
        session_info_->get_local_nls_date_format(),
        session_info_->get_local_nls_timestamp_format(),
        session_info_->get_local_nls_timestamp_tz_format()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else if (OB_FAIL(alter_table_stmt->fill_session_vars(*session_info_))) {
      SQL_RESV_LOG(WARN, "failed to init local session vars with session", K(ret));
    } else {
      stmt_ = alter_table_stmt;
    }
    //resolve table
    if (OB_SUCC(ret)) {
      //alter table database_name.table_name ...
      ObString database_name;
      ObString table_name;
      char *dblink_name_ptr = NULL;
      int32_t dblink_name_len = 0;
      if (OB_ISNULL(parse_tree.children_[TABLE])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[TABLE],
                                                     table_name,
                                                     database_name,
                                                     false,
                                                     false,
                                                     &dblink_name_ptr,
                                                     &dblink_name_len))) {
        SQL_RESV_LOG(WARN, "failed to resolve table name.",
                     K(table_name), K(database_name), K(ret));
      } else if (NULL != dblink_name_ptr) {
        // Check whether the child nodes of table_node have dblink ParseNode,
        // If so, error will be reported.
        if (0 != dblink_name_len) { //To match the error reporting behavior of Oracle
          ret = OB_ERR_DDL_ON_REMOTE_DATABASE;
          SQL_RESV_LOG(WARN, "alter table on remote database by dblink.", K(ret));
          LOG_USER_ERROR(OB_ERR_DDL_ON_REMOTE_DATABASE);
        } else {
          ret = OB_ERR_DATABASE_LINK_EXPECTED;
          SQL_RESV_LOG(WARN, "miss database link.", K(ret));
          LOG_USER_ERROR(OB_ERR_DATABASE_LINK_EXPECTED);
        }
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
        } else if (0 == parse_tree.value_
                   && OB_FAIL(schema_checker_->get_table_schema(
                      session_info_->get_effective_tenant_id(),
                      database_name,
                      table_name,
                      false/*not index table*/,
                      table_schema_))) {
          if (OB_TABLE_NOT_EXIST == ret) {
            LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name),
                to_cstring(table_name));
          }
          LOG_WARN("fail to get table schema", K(ret));
        } else if (1 == parse_tree.value_) {
          uint64_t db_id = OB_INVALID_ID;
          if (OB_FAIL(schema_checker_->get_database_id(
                      session_info_->get_effective_tenant_id(),
                      database_name, db_id))) {
            LOG_WARN("fail to get db id", K(ret), K(database_name));
          } else if (OB_FAIL(schema_checker_->get_idx_schema_by_origin_idx_name(
                             session_info_->get_effective_tenant_id(),
                             db_id,
                             table_name,
                             index_schema_))) {
            LOG_WARN("fail to get index table schema", K(ret), K(table_name));
          } else if (OB_ISNULL(index_schema_)) {
            // 获取到的 index_schema_ 为空，则说明当前 db 下没有对应的 index
            ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
            LOG_WARN("index not exists", K(ret), K(database_name), K(table_name));
            LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY,
                           table_name.length(), table_name.ptr());
          } else if (OB_FAIL(schema_checker_->get_table_schema(
                             index_schema_->get_tenant_id(),
                             index_schema_->get_data_table_id(),
                             table_schema_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get table schema with data table id failed", K(ret));
          } else if (OB_ISNULL(table_schema_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret));
          } else if (table_schema_->is_mysql_tmp_table()) {
            // supported in mysql.
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("alter temporary table not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter temporary table");
          } else if (OB_FAIL(alter_table_stmt->set_origin_table_name(
                             table_schema_->get_table_name_str()))) {
            SQL_RESV_LOG(WARN, "failed to set origin table name", K(ret));
          } else if (OB_FAIL(set_table_name(table_schema_->get_table_name_str()))) {
            SQL_RESV_LOG(WARN, "fail to set table name", K(ret), K(table_name));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_NOT_NULL(parse_tree.children_[SPECIAL_TABLE_TYPE])
              && T_EXTERNAL == parse_tree.children_[SPECIAL_TABLE_TYPE]->type_) {
            is_external_table_ = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(table_schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", K(ret));
        } else if (1 == parse_tree.value_ && OB_ISNULL(index_schema_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", K(ret));
        } else if (table_schema_->is_external_table() != is_external_table_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table type");
          SQL_RESV_LOG(WARN, "assign external table failed", K(ret));
        } else if (ObSchemaChecker::is_ora_priv_check()) {
          OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                                  session_info_->get_priv_user_id(),
                                                  database_name,
                                                  index_schema_ != NULL ?
                                                    index_schema_->get_table_id() :
                                                    table_schema_->get_table_id(),
                                                  index_schema_ != NULL ?
                                                    static_cast<uint64_t>(ObObjectType::INDEX) :
                                                    static_cast<uint64_t>(ObObjectType::TABLE),
                                                  stmt::T_ALTER_TABLE,
                                                  session_info_->get_enable_role_array()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      alter_table_stmt->set_tenant_id(table_schema_->get_tenant_id());
      alter_table_stmt->set_table_id(table_schema_->get_table_id());
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_charset_type(table_schema_->get_charset_type());
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_collation_type(table_schema_->get_collation_type());
      if (table_schema_->is_external_table()) {
        ObTableSchema &alter_schema = alter_table_stmt->get_alter_table_schema();
        alter_schema.set_table_type(table_schema_->get_table_type());
        OZ (alter_schema.set_external_file_format(table_schema_->get_external_file_format()));
        OZ (alter_schema.set_external_file_location(table_schema_->get_external_file_location()));
        OZ (alter_schema.set_external_file_location_access_info(table_schema_->get_external_file_location_access_info()));
        OZ (alter_schema.set_external_file_pattern(table_schema_->get_external_file_pattern()));
        if (OB_SUCC(ret) && table_schema_->is_user_specified_partition_for_external_table()) {
          alter_schema.set_user_specified_partition_for_external_table();
        }
      }
    }
    //resolve action list
    if (OB_SUCCESS == ret && NULL != parse_tree.children_[ACTION_LIST]){
      if (OB_FAIL(resolve_action_list(*(parse_tree.children_[ACTION_LIST])))) {
        SQL_RESV_LOG(WARN, "failed to resolve action list.", K(ret));
      } else if (OB_FAIL(generate_index_arg_cascade())) {
        LOG_WARN("fail to generate alter index cascade", K(ret));
      } else if (alter_table_bitset_.has_member(obrpc::ObAlterTableArg::LOCALITY)
                 && alter_table_bitset_.has_member(obrpc::ObAlterTableArg::TABLEGROUP_NAME)) {
        ret = OB_OP_NOT_ALLOW;
        SQL_RESV_LOG(WARN, "alter table localiy and tablegroup in the same time not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter table localiy and tablegroup at the same time");
      } else if (OB_FAIL(set_table_options())) {
        SQL_RESV_LOG(WARN, "failed to set table options.", K(ret));
      } else if ((table_schema_->has_mlog_table() || table_schema_->is_mlog_table())
          && OB_FAIL(ObResolverUtils::check_allowed_alter_operations_for_mlog(
              alter_table_stmt->get_tenant_id(),
              alter_table_stmt->get_alter_table_arg(),
              *table_schema_))) {
        LOG_WARN("failed to check allowed alter operations for mlog", KR(ret));
      } else {
        // deal with alter table rename to mock_fk_parent_table_name
        if (is_mysql_mode()
            && alter_table_bitset_.has_member(obrpc::ObAlterTableArg::TABLE_NAME)) {
          ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
          const AlterTableSchema &alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
          const ObString &new_table_name = alter_table_schema.get_table_name_str();
          const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
          const ObString &new_database_name = alter_table_schema.get_database_name();
          const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
          const ObTableSchema *orig_table_schema = NULL;
          const ObMockFKParentTableSchema *mock_parent_table_schema = NULL;
          if (OB_ISNULL(schema_guard)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
          } else if (OB_FAIL(schema_guard->get_table_schema(alter_table_stmt->get_tenant_id(),
                             origin_database_name, origin_table_name, false, orig_table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(alter_table_stmt->get_tenant_id()), K(origin_table_name), K(origin_database_name));
          } else if (OB_ISNULL(orig_table_schema)) {
            ret = OB_ERR_TABLE_EXIST;
            LOG_WARN("table not exist", K(ret), K(alter_table_stmt->get_tenant_id()), K(origin_table_name), K(origin_database_name));
          } else {
            ObString database_name;
            uint64_t database_id = OB_INVALID_ID;
            ObNameCaseMode mode = OB_NAME_CASE_INVALID;
            if (OB_FAIL(schema_guard->get_tenant_name_case_mode(alter_table_stmt->get_tenant_id(), mode))) {
              LOG_WARN("fail to get tenant name case mode", K(ret), K(alter_table_stmt->get_tenant_id()));
            } else if (!new_database_name.empty() && !ObCharset::case_mode_equal(mode, new_database_name, origin_database_name)) {
              database_name = new_database_name;
            } else {
              database_name = origin_database_name;
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(schema_guard->get_database_id(alter_table_stmt->get_tenant_id(), database_name, database_id))) {
              LOG_WARN("fail to get database id", K(ret), K(alter_table_stmt->get_tenant_id()), K(database_name));
            } else if (OB_INVALID_ID == database_id) {
              // do nothing
            } else if (OB_FAIL(schema_checker_->get_mock_fk_parent_table_with_name(
                session_info_->get_effective_tenant_id(), database_id,
                new_table_name, mock_parent_table_schema))) {
              SQL_RESV_LOG(WARN, "failed to check_mock_fk_parent_table_exist_with_name");
            } else if (OB_NOT_NULL(mock_parent_table_schema)) {
              if (alter_table_stmt->get_alter_table_action_count() > 1) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("alter table rename to mock fk parent table name with other actions not supported", K(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter table rename to mock fk parent table name with other actions");
              }
            }
          }
        }
      }
    }
    // 检查 references 的权限
    if (OB_SUCC(ret)) {
      const ObSArray<ObCreateForeignKeyArg> &fka_list =
          alter_table_stmt->get_foreign_key_arg_list();
      for (int i = 0; OB_SUCC(ret) && i < fka_list.count(); ++i) {
        const ObCreateForeignKeyArg &fka = fka_list.at(i);
        // Oracle 官方文档关于 references 权限的描述非常少。
        // 文档中比较重要的一条是“references 权限不能被授予给角色”，
        // 再加上测试的结果，所以我们推测references权限进行检查时，
        // 检查的不是当前用户是否具有refernces权限，而是去检查子表所在的schema有没有references的权限。
        // 所以现在的逻辑是
        //   1. 当子表和父表相同时，无需检查
        //   2. 当子表和父表同属一个schema时，也无需检查，这一点已经在oracle上验证了。
        //   所以在代码里面，当database_name_和 fka.parent_database_相同时，就 skip 检查。
        if (0 == database_name_.case_compare(fka.parent_database_)) {
        } else if (true == fka.is_modify_fk_state_) {
          // Skip privilege check for alter table modify constraint fk enable/disable.
        } else {
          OZ (schema_checker_->check_ora_ddl_ref_priv(session_info_->get_effective_tenant_id(),
                                                    database_name_,
                                                    fka.parent_database_,
                                                    fka.parent_table_,
                                                    fka.parent_columns_,
                                                    static_cast<uint64_t>(ObObjectType::TABLE),
                                                    stmt::T_ALTER_TABLE,
                                                    session_info_->get_enable_role_array()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // alter table 路径
      if (OB_SUCC(ret) && OB_NOT_NULL(table_schema_)) {
        if (OB_FAIL(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.
                    push_back(ObBasedSchemaObjectInfo(
                              table_schema_->get_table_id(),
                              TABLE_SCHEMA,
                              table_schema_->get_schema_version())))) {
          SQL_RESV_LOG(WARN, "failed to add based_schema_object_info to arg",
                       K(ret), K(table_schema_->get_table_id()),
                       K(table_schema_->get_schema_version()),
                       K(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_));
        } else if (!table_schema_->is_interval_part()) {
        } else if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                   set_interval_range(table_schema_->get_interval_range()))) {
          LOG_WARN("fail to set interval range", K(ret));
        } else if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                   set_transition_point(table_schema_->get_transition_point()))) {
          LOG_WARN("fail to set transition point", K(ret));
        } else {
          alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                            set_schema_version(table_schema_->get_schema_version());
          alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                            get_part_option().set_part_func_type(table_schema_->get_part_option().get_part_func_type());
        }
      }
      // alter index 路径
      if (OB_SUCC(ret) && OB_NOT_NULL(index_schema_)) {
        if (OB_FAIL(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.
                    push_back(ObBasedSchemaObjectInfo(
                              index_schema_->get_table_id(),
                              TABLE_SCHEMA,
                              index_schema_->get_schema_version())))) {
          SQL_RESV_LOG(WARN, "failed to add based_schema_object_info to arg",
                       K(ret), K(index_schema_->get_table_id()),
                       K(index_schema_->get_schema_version()),
                       K(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_hints(parse_tree.children_[ALTER_HINT],
          *alter_table_stmt, nullptr == index_schema_ ? *table_schema_ : *index_schema_))) {
        LOG_WARN("resolve hints failed", K(ret));
      }
    }
    if (OB_SUCC(ret)){
      if (OB_FAIL(deep_copy_string_in_part_expr(get_alter_table_stmt()))) {
        LOG_WARN("failed to deep copy string in part expr");
      }
    }
  }
  DEBUG_SYNC(HANG_BEFORE_RESOLVER_FINISH);
  return ret;
}

int ObAlterTableResolver::set_table_options()
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "stmt should not be null!", K(ret));
  } else {
    AlterTableSchema &alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      //this can be set by alter table option
    alter_table_schema.set_auto_increment(auto_increment_);
    alter_table_schema.set_block_size(block_size_);
    alter_table_schema.set_charset_type(charset_type_);
    alter_table_schema.set_collation_type(collation_type_);
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
    alter_table_schema.set_tablespace_id(tablespace_id_);
    alter_table_schema.set_dop(table_dop_);
    alter_table_schema.set_lob_inrow_threshold(lob_inrow_threshold_);
    alter_table_schema.set_auto_increment_cache_size(auto_increment_cache_size_);
    //deep copy
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (compress_method_ == all_compressor_name[ZLIB_COMPRESSOR]) {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "Not allowed to use zlib compressor!", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "zlib compressor");
    } else if (OB_FAIL(alter_table_schema.set_compress_func_name(compress_method_))) {
      SQL_RESV_LOG(WARN, "Write compress_method_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_comment(comment_))) {
      SQL_RESV_LOG(WARN, "Write comment_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_expire_info(expire_info_))) {
      SQL_RESV_LOG(WARN, "Write expire_info_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_table_name(table_name_))) { //new table name
      SQL_RESV_LOG(WARN, "Write table_name_ to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_tablegroup_name(tablegroup_name_))) {
      SQL_RESV_LOG(WARN, "Write tablegroup to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_database_name(database_name_))) {
      SQL_RESV_LOG(WARN, "Write database_name to alter_table_schema failed!", K(database_name_), K(ret));
    } else if (OB_FAIL(alter_table_schema.set_encryption_str(encryption_))) {
      SQL_RESV_LOG(WARN, "Write encryption to alter_table_schema failed!", K(encryption_), K(ret));
    } else if (OB_FAIL(alter_table_schema.set_ttl_definition(ttl_definition_))) {
      SQL_RESV_LOG(WARN, "Write ttl_definition to alter_table_schema failed!", K(ret));
    } else if (OB_FAIL(alter_table_schema.set_kv_attributes(kv_attributes_))) {
      SQL_RESV_LOG(WARN, "Write kv_attributes to alter_table_schema failed!", K(ret));
    } else {
      alter_table_schema.alter_option_bitset_ = alter_table_bitset_;
    }

    if (OB_SUCC(ret) && alter_table_schema.get_compressor_type() == ObCompressorType::ZLIB_LITE_COMPRESSOR) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_3_0_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant version is less than 4.3, zlib_lite compress method is not supported",
                 K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.3, zlib_lite");
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    }

    if (OB_FAIL(ret)) {
      alter_table_schema.reset();
      SQL_RESV_LOG(WARN, "Set table options error!", K(ret));
    } else {
      LOG_DEBUG("alter table resolve end", K(alter_table_schema));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_set_interval(ObAlterTableStmt *stmt, const ParseNode &node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "table_schema_ should not be null", K(ret));
  } else if (!table_schema_->is_range_part()) {
    ret = OB_ERR_SET_INTERVAL_IS_NOT_LEGAL_ON_THIS_TABLE;
    SQL_RESV_LOG(WARN, "set interval on no range partitioned table", K(ret));
  } else if (OB_ISNULL(node.children_[0])) {
    /* set interval () */
    if (!table_schema_->is_interval_part()) {
      ret = OB_ERR_TABLE_IS_ALREADY_A_RANGE_PARTITIONED_TABLE;
      SQL_RESV_LOG(WARN, "table alreay a range partitionted table", K(ret));
    } else {
      /* 设置为interval -> range */
      stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::INTERVAL_TO_RANGE;
    }
  } else if (OB_INVALID_ID != table_schema_->get_tablegroup_id()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("set interval in tablegroup not allowed", K(ret), K(table_schema_->get_tablegroup_id()));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add/drop table partition in 2.0 tablegroup");
  } else {
    ObRawExpr *expr = NULL;

    /* set interval (expr) */
    if (false == table_schema_->is_interval_part()) {
      /* 设置为 range -> interval */
      stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::SET_INTERVAL;
    } else {
      stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::SET_INTERVAL;
    }
    const ObRowkey *rowkey_last =
        &table_schema_->get_part_array()[table_schema_->get_part_option().get_part_num()- 1]
        ->get_high_bound_val();

    if (OB_SUCC(ret) && NULL != rowkey_last) {
      if (rowkey_last->get_obj_cnt() != 1) {
        ret = OB_ERR_INTERVAL_CLAUSE_HAS_MORE_THAN_ONE_COLUMN;
        SQL_RESV_LOG(WARN, "interval clause has more then one column", K(ret));
      } else if (OB_ISNULL(rowkey_last->get_obj_ptr())) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "row key is null", K(ret));
      } else {
        ObObj transition_value = rowkey_last->get_obj_ptr()[0];
        ObItemType item_type;
        ObConstRawExpr *transition_expr = NULL;
        if (false == ObResolverUtils::is_valid_oracle_interval_data_type(
                                      transition_value.get_type(), item_type)) {
          ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
          SQL_RESV_LOG(WARN, "invalid interval column data type", K(ret));
        }
        OZ (params_.expr_factory_->create_raw_expr(item_type, transition_expr));
        OX (transition_expr->set_value(transition_value));
        OZ (ObDDLResolver::resolve_interval_expr_low(params_,
                                                  node.children_[0],
                                                  *table_schema_,
                                                  transition_expr,
                                                  expr));
        OX (stmt->set_transition_expr(transition_expr));
        OX (stmt->set_interval_expr(expr));
      }
    }
  }
  return ret;
}

// bug: 48644348
int ObAlterTableResolver::check_alter_column_schemas_valid(ObAlterTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (stmt.get_alter_table_arg().is_alter_columns_) {
    const AlterTableSchema &alter_table_schema = stmt.get_alter_table_arg().alter_table_schema_;
    ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    ObSEArray<ObString, 2> dependent_columns;
    ObSEArray<ObString, 2> drop_columns;
    const ObColumnSchemaV2 *col_schema = NULL;
    AlterColumnSchema *alter_column_schema = NULL;
    ObString alter_column_name;
    for (; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
      if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter_column_schema is NULL", K(ret), K(alter_table_schema));
      } else if (OB_DDL_DROP_COLUMN == alter_column_schema->alter_type_) {
        alter_column_name = alter_column_schema->get_origin_column_name();
        if (OB_FAIL(drop_columns.push_back(alter_column_name))) {
          LOG_WARN("fail to push back column id", K(ret));
        }
      } else if (OB_DDL_ADD_COLUMN == alter_column_schema->alter_type_ &&
                 alter_column_schema->is_generated_column()) {
        ObSEArray<ObString, 2> columns_names;
        ObItemType root_expr_type = T_INVALID;
        if (OB_FAIL(alter_column_schema->get_cur_default_value().get_string(alter_column_name))) {
          LOG_WARN("get expr string from default value failed", K(ret));
        } else if (OB_UNLIKELY(alter_column_name.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("got an empty column name", K(ret), K(alter_column_name));
        } else if (OB_FAIL(ObResolverUtils::resolve_generated_column_info(alter_column_name,
                                                                          *allocator_,
                                                                          root_expr_type,
                                                                          columns_names))) {
          LOG_WARN("failed to resolve generated column info", K(ret), K(alter_column_name));
        } else if (OB_FAIL(append(dependent_columns, columns_names))) {
          LOG_WARN("failed to append column names", K(ret), K(columns_names));
        }
      }
    }
    if (OB_SUCC(ret) && !drop_columns.empty() && !dependent_columns.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < drop_columns.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < dependent_columns.count(); ++j) {
          if (0 == drop_columns.at(i).compare(dependent_columns.at(j))) {
            const ObString &column_name = drop_columns.at(i);
            ObString scope_name = "generated column function";
            ret = OB_ERR_BAD_FIELD_ERROR;
            LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
                          scope_name.length(), scope_name.ptr());
            LOG_WARN("Dropping column has generated column deps", K(ret), K(column_name));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_external_partition(const ParseNode &part_element,
                                                         const ParseNode &location_element)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();

  if (OB_ISNULL(alter_table_stmt) || OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (part_element.type_ != T_ALTER_EXTERNAL_PARTITION_ADD ||
      (location_element.type_ != T_VARCHAR
      && location_element.type_ != T_CHAR
      && location_element.type_ != T_NCHAR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (part_element.num_child_ != table_schema_->get_partition_key_info().get_size()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "unsatisfactory partition element");
    LOG_WARN("invalid partition key num", K(ret), K(part_element.num_child_));
  } else {
    alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::ADD_PARTITION;
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_level(table_schema_->get_part_level());
    const common::ObPartitionKeyInfo &partition_keys = table_schema_->get_partition_key_info();
    ParseNode *part_func_node = NULL;
    ObSEArray<ObString, 4> dummy_part_keys;
    ObArray<ObRawExpr *> part_value_expr_array;
    if (OB_FAIL(mock_part_func_node(*table_schema_, false/*is_sub_part*/, part_func_node))) {
      LOG_WARN("mock part func node failed", K(ret));
    } else if (OB_FAIL(resolve_part_func(params_, part_func_node,
                                        table_schema_->get_part_option().get_part_func_type(),
                                        *table_schema_,
                                        alter_table_stmt->get_part_fun_exprs(), dummy_part_keys))) {
      LOG_WARN("resolve part func failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_keys.get_size(); i++) {
      ObString column_name;
      if (OB_ISNULL(part_element.children_[i]) || OB_ISNULL(part_element.children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_FAIL(resolve_column_name(column_name, part_element.children_[i]->children_[0]))) {
        LOG_WARN("failed to resolve new column name", K(ret));
      }
      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found && j < partition_keys.get_size(); j++) {
        const ObColumnSchemaV2 *part_column = table_schema_->get_column_schema(partition_keys.get_column(j)->column_id_);
        if (0 == part_column->get_column_name_str().case_compare(column_name)) {
          found = true;
          ObRawExpr *const_expr = NULL;
          if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *part_element.children_[i]->children_[1], const_expr, NULL))) {
            LOG_WARN("resolve const expr failed", K(ret));
          } else if (!const_expr->is_const_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("const expr is not const expr", K(ret));
          } else if (OB_FAIL(part_value_expr_array.push_back(const_expr))) {
            LOG_WARN("push back failed", K(ret));
          } else {
            ObExprResType col_type;
            col_type.set_meta(part_column->get_meta_type());
            col_type.set_accuracy(part_column->get_accuracy());
            if (OB_FAIL(ObResolverUtils::check_partition_range_value_result_type(table_schema_->get_part_option().get_part_func_type(),
                                                        col_type,
                                                        column_name,
                                                        static_cast<ObConstRawExpr *>(const_expr)->get_value()))) {
              LOG_WARN("check partition range value result type failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        ret = OB_ERR_SPECIFIY_PARTITION_DESCRIPTION;
        LOG_WARN("have not specifiy part column value", K(ret));
      }
    }
    ObOpRawExpr *row_expr = NULL;
    ObString external_location;
    if (OB_FAIL(ret)) {
    } else if (part_value_expr_array.count() > partition_keys.get_size()) {
      ret = OB_ERR_SPECIFIY_PARTITION_DESCRIPTION;
      LOG_WARN("may have redefine the part column value", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_ISNULL(row_expr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allcoate memory", K(ret));
    } else {
      ObPartition partition;
      ObString tmp_str = ObString(location_element.str_len_, location_element.str_value_);
      OZ (ob_write_string(*allocator_, tmp_str, external_location));
      ObSqlString full_path;
      OZ (full_path.append(table_schema_->get_external_file_location()));
      if (OB_SUCC(ret)) {
        if (full_path.length() == 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("full path length should not be zero", K(ret));
        } else if (*(full_path.ptr() + full_path.length() - 1) != '/') {
          OZ (full_path.append("/"));
        }
      }
      OZ (full_path.append(external_location));
      OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_external_file_location(full_path.string()));
      alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_table_type(EXTERNAL_TABLE);
      partition.set_external_location(external_location);
      partition.set_is_empty_partition_name(true);
      for (int64_t i = 0; OB_SUCC(ret) && i < part_value_expr_array.count(); i++) {
        if (part_value_expr_array.at(i)->is_const_expr()) {
          if (OB_FAIL(row_expr->add_param_expr(part_value_expr_array.at(i)))) {
            LOG_WARN("add param expr failed", K(ret));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not a valid const expr", K(ret));
        }
      }
      OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_partition(partition));
      OZ (alter_table_stmt->get_part_values_exprs().push_back(row_expr));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_external_partition(const ParseNode &location_node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt) || OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (location_node.type_ != T_ALTER_EXTERNAL_PARTITION_DROP
      || OB_UNLIKELY(location_node.num_child_ != 1)
      || OB_ISNULL(location_node.children_[0])
      || (location_node.children_[0]->type_ != T_VARCHAR && location_node.children_[0]->type_ != T_CHAR
          && location_node.children_[0]->type_ != T_NCHAR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::DROP_PARTITION;
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_level(table_schema_->get_part_level());
    const common::ObPartitionKeyInfo &partition_keys = table_schema_->get_partition_key_info();
    ObString tmp_loc(location_node.children_[0]->str_len_, location_node.children_[0]->str_value_);
    if (table_schema_->get_partition_num() > 0) {
      CK (OB_NOT_NULL(table_schema_->get_part_array()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_->get_partition_num(); i++) {
      ObPartition *partition = table_schema_->get_part_array()[i];
      CK (OB_NOT_NULL(partition));
      if (OB_SUCC(ret)) {
        if (0 == partition->get_external_location().compare(tmp_loc)) {
          OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_partition(*partition));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_partition_num() < 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "check the partition location, no partitions to drop");
      LOG_WARN("already no partitions existed, drop partition is not supported", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_level(table_schema_->get_part_level());
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_part_option() = table_schema_->get_part_option();
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_part_option().set_part_num(
            alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_partition_num());
    alter_table_stmt->get_alter_table_arg().is_add_to_scheduler_ = false;
  }
  return ret;
}

int ObAlterTableResolver::resolve_external_partition_options(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret), K(session_info_->get_effective_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_3_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter refresh ext table is not supported in data version less than 4.3.1", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh external table in data version less than 4.3.1");
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt is null", K(ret));
  } else if (!table_schema_->is_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter external partition for non external table");
    LOG_WARN("alter external partition for non external table is not supported", K(ret));
  } else if (!table_schema_->is_user_specified_partition_for_external_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter non user specified external table attributes");
    LOG_WARN("alter partition for non user specified partition or other attributes is not supported", K(ret));
  } else {
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.alter_type_ = OB_DDL_ALTER_TABLE;
    alter_table_stmt->set_tenant_id(table_schema_->get_tenant_id());
    alter_table_stmt->set_table_id(table_schema_->get_table_id());
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.
                      get_part_option().set_part_func_type(table_schema_->get_part_option().get_part_func_type());
    alter_table_stmt->set_alter_table_partition();
    alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_part_level(table_schema_->get_part_level());
    OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_external_file_location_access_info(table_schema_->get_external_file_location_access_info()));
    OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_external_file_pattern(table_schema_->get_external_file_pattern()));
    CK (OB_LIKELY(node.type_ == T_ALTER_EXTERNAL_PARTITION_OPTION));
    if (T_ALTER_EXTERNAL_PARTITION_ADD == node.children_[0]->type_) {
      CK (OB_LIKELY(node.num_child_ == 2));
      CK (OB_NOT_NULL(node.children_[0]));
      CK (OB_NOT_NULL(node.children_[1]));
      OZ (resolve_add_external_partition(*node.children_[0], *node.children_[1]));
    } else if (T_ALTER_EXTERNAL_PARTITION_DROP == node.children_[0]->type_) {
      CK (OB_LIKELY(node.num_child_ == 1));
      CK (OB_NOT_NULL(node.children_[0]));
      OZ (resolve_drop_external_partition(*node.children_[0]));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported alter partition action", K(ret));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_action_list(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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
    // 获取表中已有的index name, 并初始化current_index_name_set_
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    // drop_col_act_position_list is only used in mysql mode
    // to resolve drop_column_nodes after drop constraint nodes resolved
    ObArray<int> drop_col_act_position_list;
    if (OB_FAIL(table_schema_->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = NULL;
      ObString index_name;
      if (OB_FAIL(schema_checker_->get_table_schema(table_schema_->get_tenant_id(), simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret));
      } else if (index_table_schema->is_materialized_view()) {
        // bug:
        // index_tid_array: 包含index和mv, 这里只需要处理索引即可
        // so do-nothing for mv
      } else if (index_table_schema->is_built_in_fts_index()) {
        // skip built-in fts index
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
    bool has_alter_column_option = false;
    //in mysql mode, resolve add index after resolve column actions
    ObSEArray<int64_t, 4> add_index_action_idxs;
    int64_t external_table_accept_options[] = {
      T_ALTER_REFRESH_EXTERNAL_TABLE, T_ALTER_EXTERNAL_PARTITION_OPTION, T_TABLE_OPTION_LIST, T_ALTER_TABLE_OPTION};
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode *action_node = node.children_[i];
      if (OB_ISNULL(action_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else if (lib::is_oracle_mode() && is_modify_column_visibility && (alter_column_times != alter_column_visibility_times)) {
        ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
        SQL_RESV_LOG(WARN, "Column visibility modifications can not be combined with any other modified column DDL option.", K(ret));
      } else if (FALSE_IT(alter_table_stmt->inc_alter_table_action_count())) {
      } else if (is_external_table_ && !has_exist_in_array(external_table_accept_options,
                                                           array_elements(external_table_accept_options),
                                                           static_cast<int64_t>(action_node->type_))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter external table option is");
      } else {
        switch (action_node->type_) {
        //deal with alter table option
        case T_ALTER_TABLE_OPTION: {
            alter_table_stmt->set_alter_table_option();
            if (OB_ISNULL(action_node->children_)) {
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
        case T_CONVERT_TO_CHARACTER: {
          alter_table_stmt->set_convert_to_character();
          if (OB_FAIL(resolve_convert_to_character(*action_node))) {
            SQL_RESV_LOG(WARN, "Resolve convert to character failed!", K(ret));
          }
          break;
        }
        //deal with add column, alter column, drop column, change column, modify column
        case T_ALTER_COLUMN_OPTION: {
            alter_table_stmt->set_alter_table_column();
            bool temp_is_modify_column_visibility = false;
            bool is_drop_column = false;
            has_alter_column_option = true;
            if (OB_FAIL(resolve_column_options(*action_node, temp_is_modify_column_visibility, is_drop_column, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve column option failed!", K(ret));
            } else {
              if (temp_is_modify_column_visibility) {
                is_modify_column_visibility = temp_is_modify_column_visibility;
                ++alter_column_visibility_times;
              }
              if (is_drop_column) {
                drop_col_act_position_list.push_back(i);
              }
              ++alter_column_times;
            }
            break;
          }
        case T_ALTER_COLUMN_GROUP_OPTION: {
            if (OB_FAIL(resolve_alter_column_groups(*action_node))) {
                SQL_RESV_LOG(WARN, "Resolve column group option failed!", K(ret));
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
        //deal with add index drop index rename index
        case T_ALTER_INDEX_OPTION: {
            // mysql对应alter index
            bool is_add_index = false;
            alter_table_stmt->set_alter_table_index();
            if (OB_FAIL(resolve_index_options(node, *action_node, is_add_index))) {
              SQL_RESV_LOG(WARN, "Resolve index option failed!", K(ret));
            } else if (is_add_index) {
              if (OB_FAIL(add_index_action_idxs.push_back(i))) {
                LOG_WARN("push back add index failed", K(ret));
              }
            }
            break;
          }
        case T_ALTER_PARTITION_OPTION: {
            alter_table_stmt->set_alter_table_partition();
            if (lib::is_mysql_mode() && alter_table_stmt->get_alter_table_arg().is_alter_columns_) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(
                OB_NOT_SUPPORTED,
                "specify alter_column_action and alter_partition_action in a single alter table stmt");
              LOG_WARN(
                "alter_column_action and alter_partition_action in a single alter table stmt",
                K(ret));
            } else if (OB_FAIL(resolve_partition_options(*action_node))) {
              SQL_RESV_LOG(WARN, "Resolve partition option failed!", K(ret));
            }
            break;
          }
        // 仅处理 mysql 模式下的 alter table add check constraint
        // oracle 模式下的 alter table add check constraint 在 resolve_index_options 里面处理
        case T_ALTER_CHECK_CONSTRAINT_OPTION: {
            if (OB_FAIL(resolve_constraint_options(*action_node, node.num_child_ > 1))) {
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
            // drop check constraint/foreign key/index in oracle mode, drop check constraint/foreign key in mysql mode
            ObString constraint_name;
            uint64_t constraint_id = OB_INVALID_ID;
            bool is_constraint = false; // 表示除去外键及唯一键以外的其他 constraint
            bool is_foreign_key = false;
            bool is_unique_key = false;
            bool is_primary_key = false;
            ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();

            if (OB_ISNULL(action_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
            } else if (OB_ISNULL(action_node->children_[0]->str_value_) || action_node->children_[0]->str_len_ <= 0) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), KP(action_node->children_[0]->str_value_), K(action_node->children_[0]->str_len_));
            } else {
              constraint_name.assign_ptr(action_node->children_[0]->str_value_, static_cast<int32_t>(action_node->children_[0]->str_len_));
            }
            if (OB_SUCC(ret)) {
              if (!table_schema_->is_mysql_tmp_table()) {
                if (OB_FAIL(schema_guard->get_constraint_id(table_schema_->get_tenant_id(),
                                                            table_schema_->get_database_id(),
                                                            constraint_name,
                                                            constraint_id))) {
                  LOG_WARN("get constraint id failed", K(ret),
                                                       K(table_schema_->get_tenant_id()),
                                                       K(table_schema_->get_database_id()),
                                                       K(constraint_name));
                } else {
                  is_constraint = OB_INVALID_ID != constraint_id;
                  is_primary_key = lib::is_oracle_mode() && nullptr != table_schema_->get_constraint(constraint_id)
                    && CONSTRAINT_TYPE_PRIMARY_KEY == table_schema_->get_constraint(constraint_id)->get_constraint_type();
                }
              } else { // tmp table in mysql mode
                ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
                for (; OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
                  if (0 == constraint_name.case_compare((*iter)->get_constraint_name_str())) {
                    is_constraint = true;
                    break;
                  }
                }
              }
              if (OB_SUCC(ret) && (lib::is_mysql_mode() || !is_constraint)) { // drop foreign key
                // 在 drop constraint 的时候检查约束类型是否是 foreign key 或者 unique constraint
                if (OB_FAIL(schema_guard->get_foreign_key_id(table_schema_->get_tenant_id(),
                                                             table_schema_->get_database_id(),
                                                             constraint_name,
                                                             constraint_id))) {
                  LOG_WARN("get foreign key id failed", K(ret),
                                                        K(table_schema_->get_tenant_id()),
                                                        K(table_schema_->get_database_id()),
                                                        K(constraint_name));
                } else if (OB_INVALID_ID != constraint_id) {
                  if (is_constraint) {
                    ObString action("drop");
                    ret = OB_ERR_MULTIPLE_CONSTRAINTS_WITH_SAME_NAME;
                    LOG_USER_ERROR(OB_ERR_MULTIPLE_CONSTRAINTS_WITH_SAME_NAME,
                                   constraint_name.length(), constraint_name.ptr(),
                                   action.length(), action.ptr());
                    LOG_WARN("drop colum failed : muti-column constraint", K(ret), K(constraint_name));
                  }
                  is_foreign_key = true;
                }
              }
              if (OB_SUCC(ret) && lib::is_oracle_mode() && !is_constraint && !is_foreign_key) {
                // drop unique index (only in oracle mode)
                const ObSimpleTableSchemaV2* simple_table_schema = nullptr;
                ObString unique_index_name_with_prefix;
                if (OB_FAIL(ObTableSchema::build_index_table_name(*allocator_,
                            table_schema_->get_table_id(),
                            constraint_name,
                            unique_index_name_with_prefix))) {
                  LOG_WARN("build_index_table_name failed", K(ret), K(table_schema_->get_table_id()), K(constraint_name));
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
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (is_primary_key) {
                alter_table_stmt->set_alter_table_index();
                if (action_node->num_child_ <= 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected err", K(ret));
                } else if (OB_FALSE_IT(action_node->children_[0]->type_ = T_PRIMARY_KEY_DROP)) {
                } else if (OB_FAIL(resolve_drop_primary(node))) {
                  LOG_WARN("resolve drop primary key failed", K(ret), K(constraint_name));
                }
              } else if (is_constraint) {
                if (OB_FAIL(resolve_constraint_options(*action_node, node.num_child_ > 1))) {
                  SQL_RESV_LOG(WARN, "Resolve check constraint option in mysql mode failed!", K(ret));
                }
              } else if(is_foreign_key) {
                alter_table_stmt->set_alter_table_index();
                if (OB_FAIL(resolve_foreign_key_options(*action_node))) {
                  SQL_RESV_LOG(WARN, "failed to resolve foreign key options in mysql mode!", K(ret));
                }
              } else if (is_unique_key) {
                action_node->type_ = T_INDEX_DROP;
                alter_table_stmt->set_alter_table_index();
                if (OB_FAIL(resolve_drop_index(*action_node))) {
                  SQL_RESV_LOG(WARN, "Resolve drop index error!", K(ret));
                }
              } else {
                ret = OB_ERR_NONEXISTENT_CONSTRAINT;
                if (lib::is_mysql_mode()) {
                  LOG_USER_ERROR(OB_ERR_NONEXISTENT_CONSTRAINT, constraint_name.length(), constraint_name.ptr());
                }
                SQL_RESV_LOG(WARN,
                    "Cannot drop constraint  - nonexistent constraint",
                    K(ret),
                    K(*table_schema_),
                    K(constraint_name));
              }
            }
            break;
        }
        case T_MODIFY_ALL_TRIGGERS: {
            alter_table_stmt->set_is_alter_triggers(true);
            if (OB_FAIL(resolve_modify_all_trigger(*action_node))) {
              SQL_RESV_LOG(WARN, "failed to resolve trigger option!", K(ret));
            }
            break;
        }
        case T_SET_INTERVAL: {
            if (OB_FAIL(resolve_set_interval(alter_table_stmt, *action_node))) {
              SQL_RESV_LOG(WARN, "failed to resolve foreign key options in mysql mode!", K(ret));
            }
            break;
        }
        case T_REMOVE_TTL: {
          uint64_t tenant_data_version = 0;
          if (OB_ISNULL(session_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
            LOG_WARN("get tenant data version failed", K(ret), K(session_info_->get_effective_tenant_id()));
          } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("REMOVE TTL is not supported in data version less than 4.2.1", K(ret), K(tenant_data_version));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "REMOVE TTL in data version less than 4.2.1");
          } else {
            ttl_definition_.reset();
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TTL_DEFINITION))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
          break;
        }
        case T_ALTER_REFRESH_EXTERNAL_TABLE : {
          alter_table_stmt->set_alter_external_table_type(action_node->type_);
          if (table_schema_->is_external_table()) {
            if (table_schema_->is_user_specified_partition_for_external_table()) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter refresh user-specified partition external table");
              LOG_WARN("alter refresh user-specified partition external table not supported", K(ret), K(action_node->type_));
            } else {
              ObString origin_table_name = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_table_name();
              ObString origin_db_name = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_database_name();
              OZ (alter_table_stmt->get_alter_table_arg().alter_table_schema_.assign(*table_schema_));
              alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_origin_table_name(origin_table_name);
              alter_table_stmt->get_alter_table_arg().alter_table_schema_.set_origin_database_name(origin_db_name);
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid to alter non external table", K(ret));
          }
          break;
        }
        case T_ALTER_EXTERNAL_PARTITION_OPTION: {
          alter_table_stmt->set_alter_external_table_type(action_node->type_);
          OZ (resolve_external_partition_options(*action_node));
          break;
        }
        default: {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "Unknown alter table action %d", K_(action_node->type), K(ret));
            /* won't be here */
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resolve_column_group_for_column())) {
      LOG_WARN("failed to resolve column group", K(ret));
    } else if (OB_FAIL(check_skip_index(alter_table_stmt->get_alter_table_arg().alter_table_schema_))) {
      LOG_WARN("failed to resolve skip index", K(ret));
    }
    //deal with drop column affer drop constraint (mysql mode)
    if (OB_SUCC(ret) && lib::is_mysql_mode() && drop_col_act_position_list.count() > 0) {
      for (uint64_t i = 0; OB_SUCC(ret) && i < drop_col_act_position_list.count(); ++i) {
        if (OB_FAIL(resolve_drop_column_nodes_for_mysql(*node.children_[drop_col_act_position_list.at(i)], reduced_visible_col_set))) {
          SQL_RESV_LOG(WARN, "Resolve drop column error!", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < add_index_action_idxs.count(); ++i) {
        ParseNode *action_node = NULL;
        if (add_index_action_idxs.at(i) < 0 || add_index_action_idxs.at(i) > node.num_child_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid id", K(ret), K(node.num_child_), K(add_index_action_idxs));
        } else if (OB_ISNULL(action_node = node.children_[add_index_action_idxs.at(i)])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (action_node->num_child_ <= 0
                   || OB_ISNULL(action_node->children_)
                   || OB_ISNULL(action_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected action node", K(ret));
        } else if (OB_FAIL(resolve_add_index(*(action_node->children_[0])))) {
          LOG_WARN("resolve add index failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (lib::is_oracle_mode() && is_modify_column_visibility && (alter_column_times != alter_column_visibility_times)) {
        ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
        SQL_RESV_LOG(WARN, "Column visibility modifications can not be combined with any other modified column DDL option.", K(ret));
      } else {
        bool has_visible_col = false;
        bool has_hidden_gencol = false;
        ObColumnIterByPrevNextID iter(*table_schema_);
        const ObColumnSchemaV2 *column_schema = NULL;
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
            has_hidden_gencol |= column_schema->is_virtual_generated_column();
            continue;
          } else { // is visible column
            ObColumnNameHashWrapper col_key(column_schema->get_column_name_str());
            if (OB_HASH_NOT_EXIST == reduced_visible_col_set.exist_refactored(col_key)) {
              has_visible_col = true;
              ret = OB_SUCCESS; // change ret from OB_HASH_NOT_EXIST to OB_SUCCESS
            } else { // OB_HASH_EXIST
              ret = OB_SUCCESS; // change ret from OB_HASH_EXIST to OB_SUCCESS
            }
          }
        }
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
          SQL_RESV_LOG(WARN, "failed to check column visibility", K(ret));
          if (NULL != column_schema) {
            SQL_RESV_LOG(WARN, "failed column schema", K(*column_schema),
                                                       K(column_schema->is_hidden()));
          }
        } else {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          if (lib::is_oracle_mode() && alter_column_visibility_times > reduced_visible_col_set.count()) {
            // 走到这里说明存在 alter table modify column visible，则至少有一个 visible column，不应该报错
          } else if (!has_visible_col && (is_oracle_mode() || has_hidden_gencol)) {
            //If there's no hidden generated columns, OB will check if all fields are dropped on rootserver
            ret = OB_ERR_ONLY_HAVE_INVISIBLE_COL_IN_TABLE;
            SQL_RESV_LOG(WARN, "table must have at least one column that is not invisible", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      if (alter_table_stmt->get_alter_table_action_count() > 1) {
        // 由于alter table add index将会使用在observer端进行同步建索引
        // 而其他已经在rs端执行过的ddl无法rollback，
        // 因此add index前, 禁掉其他ddl
        ObSArray<obrpc::ObCreateIndexArg*> &index_arg_list = alter_table_stmt->get_index_arg_list();
        for (int32_t i = 0; OB_SUCC(ret) && i < index_arg_list.count(); ++i) {
          const ObCreateIndexArg *index_arg = index_arg_list.at(i);
          if (NULL == index_arg) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "index arg is null", K(ret));
          } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_) {
            // supported in mysql.
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("add index together with other ddls not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add index together with other DDLs");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (alter_table_stmt->get_alter_table_action_count() > 1) {
        if(0 != alter_table_stmt->get_foreign_key_arg_list().count()) {
          // suppored in mysql
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("add/modify foreign key together with other ddls not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify foreign key together with other DDLs");
        }
      }
    }
    LOG_DEBUG("check add/modify cst allowed", K(alter_table_stmt->get_alter_table_action_count()), K(add_or_modify_check_cst_times_),
              K(add_not_null_constraint_), K(add_column_cnt_));
    const AlterTableSchema &table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    if (OB_SUCC(ret)) {
      if (alter_table_stmt->get_alter_table_action_count() > 1) {
        if (0 != add_or_modify_check_cst_times_) {
          // suppored in mysql
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("add/modify constraint together with other ddls not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify constraint together with other DDLs");
        } else if (lib::is_oracle_mode() && add_not_null_constraint_ && alter_table_stmt->get_alter_table_action_count() != add_column_cnt_) {
          // A ddl can't contain "add/modify column not null" with other clauses, except
          // multiple "add column (not null)"
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("add/modify not null constraint together with other ddls not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify not null constraint together with other DDLs");
        }
      } else if (lib::is_oracle_mode()
              && OB_UNLIKELY(1 == alter_table_stmt->get_alter_table_action_count()
              && table_schema.get_column_count() > 1
              && OB_DDL_MODIFY_COLUMN ==
                  (static_cast<const AlterColumnSchema*>(*table_schema.column_begin()))->alter_type_
              && add_not_null_constraint_)) {
        // alter table t modify(c1 not null, c2 varchar(100));
        // supported in mysql && oracle
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("add/modify not null constraint together with other ddls", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify not null constraint together with other DDLs");
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(check_alter_column_schemas_valid(*alter_table_stmt))) {
      LOG_WARN("failed to check alter column schemas valid", K(ret));
    }

    if (OB_SUCC(ret)) {
      // modify/change definition ttl column is not allowed currently
      if (has_alter_column_option && alter_table_bitset_.has_member(ObAlterTableArg::TTL_DEFINITION)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "SET/REMOVE TTL together with other Alter Column DDL");
      } else if (has_alter_column_option) {
        ObTableSchema tbl_schema;
        ObSEArray<ObString, 8> ttl_columns;
        if (OB_FAIL(get_table_schema_for_check(tbl_schema))) {
          LOG_WARN("fail to get table schema", K(ret));
        } else if (OB_FAIL(ObTTLUtil::get_ttl_columns(tbl_schema.get_ttl_definition(), ttl_columns))) {
          LOG_WARN("fail to get ttl column", K(ret));
        } else if (ttl_columns.empty()) {
          // do nothing
        } else {
          AlterTableSchema &alter_table_schema = get_alter_table_stmt()->get_alter_table_arg().alter_table_schema_;
          ObTableSchema::const_column_iterator iter = alter_table_schema.column_begin();
          ObTableSchema::const_column_iterator end = alter_table_schema.column_end();
          for (; OB_SUCC(ret) && iter != end; ++iter) {
            const AlterColumnSchema *column = static_cast<AlterColumnSchema *>(*iter);
            if (OB_ISNULL(column)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null alter column", K(ret));
            } else if (ObTTLUtil::is_ttl_column(column->get_origin_column_name(), ttl_columns)) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("Modify/Change TTL column is not allowed", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify/Change TTL column");
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_column_options(const ParseNode &node,
                                                 bool &is_modify_column_visibility,
                                                 bool &is_drop_column,
                                                 ObReducedVisibleColSet &reduced_visible_col_set)
{
  int ret = OB_SUCCESS;

  if (T_ALTER_COLUMN_OPTION != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode *column_node = node.children_[i];
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else {
        switch(column_node->type_) {
        //add column
        case T_COLUMN_ADD: {
            if (OB_FAIL(resolve_add_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve add column error!", K(ret));
            }
            break;
          }
        //alter column attribute
        //用来修改列的默认值
        case T_COLUMN_ALTER: {
            if (OB_FAIL(resolve_alter_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve alter column error!", K(ret));
            }
            break;
          }
        //change column name
        //需要提供新旧两个column_name
        case T_COLUMN_CHANGE: {
            if (OB_FAIL(resolve_change_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve change column error!", K(ret));
            }
            break;
          }
        //rename column name in oracle mode
        case T_COLUMN_RENAME: {
            if (OB_FAIL(resolve_rename_column(*column_node))) {
              SQL_RESV_LOG(WARN, "Resolve rename column error!", K(ret));
            }
            break;
          }
        //modify column attribute
        case T_COLUMN_MODIFY: {
            if (OB_FAIL(resolve_modify_column(*column_node, is_modify_column_visibility, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve modify column error!", K(ret));
            }
            break;
          }
        case T_COLUMN_DROP: {
          if (lib::is_mysql_mode()) {
              is_drop_column = true;
            } else if (OB_FAIL(resolve_drop_column(*column_node, reduced_visible_col_set))) {
              SQL_RESV_LOG(WARN, "Resolve drop column error!", K(ret));
            }
            break;
          }
        case T_COLUMN_ADD_WITH_LOB_PARAMS: {
            if (OB_FAIL(resolve_add_column(*column_node->children_[0]))) {
              SQL_RESV_LOG(WARN, "Resolve column option error!", K(ret));
            } else if (OB_FAIL(resolve_lob_storage_parameters(column_node->children_[1]))) {
              SQL_RESV_LOG(WARN, "Resolve lob storage parameters error!", K(ret));
            }
            break;
          }
        default:{
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "Unknown column option type!",
                         "type", column_node->type_, K(ret));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_column_nodes_for_mysql(const ParseNode& node, ObReducedVisibleColSet &reduced_visible_col_set)
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
      } else if (column_node->type_ == T_COLUMN_DROP &&
                OB_FAIL(resolve_drop_column(*column_node, reduced_visible_col_set))) {
        SQL_RESV_LOG(WARN, "Resolve drop column error!", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_index_column_list(const ParseNode &node,
                                                    obrpc::ObCreateIndexArg &index_arg,
                                                    const int64_t index_name_value,
                                                    ObIArray<ObString> &input_index_columns_name,
                                                    bool &cnt_func_index)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_COLUMN_LIST != node.type_ || node.num_child_ <= 0 ||
      OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    obrpc::ObColumnSortItem sort_item;
    //reset sort column set
    sort_column_array_.reset();
    cnt_func_index = false;
    OZ (add_new_indexkey_for_oracle_temp_table(index_arg));
    for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode *sort_column_node = node.children_[i];
      if (OB_ISNULL(sort_column_node) ||
          OB_UNLIKELY(T_SORT_COLUMN_KEY != sort_column_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      } else {
        //column_name
        sort_item.reset();
        if (OB_ISNULL(sort_column_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
        } else {
          //if the type of node is not identifiter, the index is considered as a fuctional index
          if (is_mysql_mode() && sort_column_node->children_[0]->type_ != T_IDENT) {
            sort_item.is_func_index_ = true;
            cnt_func_index = true;
          }
          sort_item.column_name_.assign_ptr(sort_column_node->children_[0]->str_value_,
              static_cast<int32_t>(sort_column_node->children_[0]->str_len_));
          bool is_multi_value_index = false;
          if (OB_FAIL(ObMulValueIndexBuilderUtil::adjust_index_type(sort_item.column_name_,
                                                                    is_multi_value_index,
                                                                    reinterpret_cast<int*>(&index_keyname_)))) {
            LOG_WARN("failed to resolve index type", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          //do nothing
        } else if (NULL != sort_column_node->children_[1]) {
          sort_item.prefix_len_ = static_cast<int32_t>(sort_column_node->children_[1]->value_);
          //can't not be zero
          if (0 == sort_item.prefix_len_) {
            ret = OB_KEY_PART_0;
            LOG_USER_ERROR(OB_KEY_PART_0, sort_item.column_name_.length(), sort_item.column_name_.ptr());
            SQL_RESV_LOG(WARN, "Key part length cannot be 0", K(sort_item), K(ret));
          }
        } else {
          sort_item.prefix_len_ = 0;
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (index_keyname_ == FTS_KEY) {
          if (!GCONF._enable_add_fulltext_index_to_existing_table) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("experimental feature: build fulltext index afterward is experimental feature", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "build fulltext index afterward");
          } else if (OB_FAIL(resolve_fts_index_constraint(*table_schema_,
                                                   sort_item.column_name_,
                                                   index_name_value))) {
            SQL_RESV_LOG(WARN, "check fts index constraint fail",K(ret),
                K(sort_item.column_name_));
          }
        } else { // spatial index, NOTE resolve_spatial_index_constraint() will set index_keyname
          ObSEArray<ObColumnSchemaV2 *, 8> resolved_cols;
          ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
          bool is_explicit_order = (NULL != sort_column_node->children_[2]
              && 1 != sort_column_node->children_[2]->is_empty_);
          if (OB_ISNULL(alter_table_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
          } else if (OB_FAIL(get_table_schema_all_column_schema(resolved_cols, alter_table_stmt->get_alter_table_schema()))) {
            SQL_RESV_LOG(WARN, "failed to get table column schema", K(ret));
          } else if (OB_FAIL(resolve_spatial_index_constraint(*table_schema_, sort_item.column_name_,
              node.num_child_, index_name_value, is_explicit_order, sort_item.is_func_index_, &resolved_cols))) {
            SQL_RESV_LOG(WARN, "check spatial index constraint fail",K(ret),
                K(sort_item.column_name_), K(node.num_child_));
          }
        }

        //column_order
        if (OB_FAIL(ret)) {
          //do nothing
        } else if (is_oracle_mode() && sort_column_node->children_[2]
                   && T_SORT_DESC == sort_column_node->children_[2]->type_) {
          // sort_item.order_type_ = common::ObOrderType::DESC;
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support desc index now", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Desc index");
        } else {
          //兼容mysql5.7, 降序索引不生效且不报错
          sort_item.order_type_ = common::ObOrderType::ASC;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_sort_column(sort_item, index_arg))){
            SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
          } else { /*do nothing*/ }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(input_index_columns_name.push_back(sort_item.column_name_))) {
            SQL_RESV_LOG(WARN, "add column name to input_index_columns_name failed",K(sort_item.column_name_), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && lib::is_mysql_mode() && cnt_func_index) {
      uint64_t tenant_data_version = 0;
      if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_2_0_0){
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant version is less than 4.2, functional index is not supported in mysql mode", K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.2, functional index in mysql mode not supported");
      }
    }
  }
  return ret;
}


int ObAlterTableResolver::add_sort_column(const obrpc::ObColumnSortItem &sort_column,
                                          obrpc::ObCreateIndexArg &index_arg)
{
  int ret = OB_SUCCESS;
  const ObString &column_name = sort_column.column_name_;
  ObColumnNameWrapper column_key(column_name, sort_column.prefix_len_);
  bool check_prefix_len = false;
  if (is_column_exists(sort_column_array_, column_key, check_prefix_len)) {
    ret = OB_ERR_COLUMN_DUPLICATE;    //index (c1,c1) or index (c1(3), c1 (6))
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
  } else if (OB_FAIL(sort_column_array_.push_back(column_key))) {
    SQL_RESV_LOG(WARN, "failed to push back column key", K(ret));
  } else if (OB_FAIL(index_arg.index_columns_.push_back(sort_column))) {
    SQL_RESV_LOG(WARN, "add sort column to index arg failed", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::get_table_schema_for_check(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  const ObTableSchema *tbl_schema = NULL;
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                alter_table_stmt->get_org_database_name(),
                                                alter_table_stmt->get_org_table_name(),
                                                false/*not index table*/,
                                                tbl_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(alter_table_stmt->get_org_database_name()),
                     to_cstring(alter_table_stmt->get_org_table_name()));
    }
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(table_schema.assign(*tbl_schema))){
    LOG_WARN("fail to assign schema", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_index(const ParseNode &node)
{
  int ret = OB_SUCCESS;

  if (T_INDEX_ADD != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    bool is_unique_key = 1 == node.value_;
    bool is_fulltext_index = 3 == node.value_;
    ParseNode *index_name_node = nullptr;
    ParseNode *column_list_node = nullptr;
    ParseNode *table_option_node = nullptr;
    ParseNode *index_partition_option = nullptr;
    ParseNode *colulmn_group_node = nullptr;
    bool is_index_part_specified = false;
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
      if (OB_UNLIKELY(ALTER_INDEX_CHILD_NUM != node.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret), K(node.num_child_));
      } else {
        index_name_node = node.children_[0];
        column_list_node = node.children_[1];
        table_option_node = node.children_[2];
        if (!is_fulltext_index) {
          index_partition_option = node.children_[4];
        }
        colulmn_group_node = node.children_[5];
      }
    }
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      sort_column_array_.reset();
      storing_column_set_.reset();
      index_keyname_ = static_cast<INDEX_KEYNAME>(node.value_);

      //column_list node should be parse first in case the index name is not specified
      if (OB_SUCC(ret)) {
        HEAP_VAR(ObCreateIndexStmt, create_index_stmt ,allocator_) {
          obrpc::ObCreateIndexArg *create_index_arg = NULL;
          void *tmp_ptr = NULL;
          ObSEArray<ObString, 8> input_index_columns_name;
          bool cnt_func_index = false;
          if (NULL == (tmp_ptr = (ObCreateIndexArg *)allocator_->alloc(
                  sizeof(obrpc::ObCreateIndexArg)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
          } else {
            create_index_arg = new (tmp_ptr) ObCreateIndexArg();
            if (OB_ISNULL(column_list_node)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
            } else if (OB_FAIL(resolve_index_column_list(*column_list_node,
                                                         *create_index_arg,
                                                         node.value_,
                                                         input_index_columns_name,
                                                         cnt_func_index))) {
              SQL_RESV_LOG(WARN, "resolve index name failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (!lib::is_oracle_mode()) {
              if (NULL != node.children_[3]) {
                if (SPATIAL_KEY == index_keyname_) {
                  const char *method = T_USING_HASH == node.children_[3]->type_ ? "HASH" : "BTREE";
                  ret = OB_ERR_INDEX_TYPE_NOT_SUPPORTED_FOR_SPATIAL_INDEX;
                  LOG_USER_ERROR(OB_ERR_INDEX_TYPE_NOT_SUPPORTED_FOR_SPATIAL_INDEX, method);
                } else if (T_USING_BTREE == node.children_[3]->type_) {
                  create_index_arg->index_using_type_ = USING_BTREE;
                } else {
                  create_index_arg->index_using_type_ = USING_HASH;
                }
              }
            } else {
              // oracle mode
              // In oracle mode, we need to check if the new index is on the same cols with old indexes
              bool has_other_indexes_on_same_cols = false;
              if (OB_FAIL(check_indexes_on_same_cols(*table_schema_,
                                                     *create_index_arg,
                                                     *schema_checker_,
                                                     has_other_indexes_on_same_cols))) {
                SQL_RESV_LOG(WARN, "check indexes on same cols failed", K(ret));
              } else if (has_other_indexes_on_same_cols) {
                ret = OB_ERR_COLUMN_LIST_ALREADY_INDEXED;
                SQL_RESV_LOG(WARN, "has other indexes on the same cols", K(ret));
              }
              // In oracle mode, we need to check if the unique index is on the same cols with pk
              if (OB_SUCC(ret) && is_unique_key) {
                bool is_uk_pk_on_same_cols = false;
                if (OB_FAIL(ObResolverUtils::check_pk_idx_duplicate(*table_schema_,
                                                                    *create_index_arg,
                                                                    input_index_columns_name,
                                                                    is_uk_pk_on_same_cols))) {
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
                ObString first_column_name = cnt_func_index ? ObString::make_string("functional_index") : input_index_columns_name.at(0);
                if (lib::is_oracle_mode()) {
                  if (OB_FAIL(ObTableSchema::create_cons_name_automatically(index_name, table_name_, *allocator_, CONSTRAINT_TYPE_UNIQUE_KEY, lib::is_oracle_mode()))) {
                    SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
                  }
                } else { // mysql mode
                  if (OB_FAIL(generate_index_name(index_name, current_index_name_set_, first_column_name))) {
                    SQL_RESV_LOG(WARN, "failed to generate index name", K(first_column_name));
                  }
                }
                if (OB_SUCC(ret)) {
                  ObIndexNameHashWrapper index_key(index_name);
                  if (OB_FAIL(current_index_name_set_.set_refactored(index_key))) {
                    LOG_WARN("fail to push back current_index_name_set_", K(ret), K(index_name));
                  } else if (OB_FAIL(ob_write_string(
                          *allocator_,
                          index_name,
                          create_index_arg->index_name_))) {
                    LOG_WARN("fail to wirte string", K(ret), K(index_name), K(first_column_name));
                  } else {
                    create_index_arg->index_schema_.set_name_generated_type(GENERATED_TYPE_SYSTEM);
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
                } else {
                  create_index_arg->index_schema_.set_name_generated_type(GENERATED_TYPE_USER);
                }
              }
            }
            if (OB_SUCCESS == ret) {
              if (NULL != table_option_node) {
                has_index_using_type_ = false;
                if (OB_FAIL(resolve_table_options(table_option_node, true))) {
                  SQL_RESV_LOG(WARN, "failed to resolve table options!", K(ret));
                } else if (has_index_using_type_) {
                  create_index_arg->index_using_type_ = index_using_type_;
                }
              }
            }
            if (OB_SUCC(ret) && is_mysql_mode()) {
              if (NULL != index_partition_option) {
                if (1 != index_partition_option->num_child_ || T_PARTITION_OPTION != index_partition_option->type_) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("column vectical partition for index not supported", K(ret));
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Column vertical partition for index");
                } else if (OB_ISNULL(index_partition_option->children_[0])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("node is null", K(ret));
                } else if (LOCAL_INDEX == index_scope_) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("specify partition option of local index not supported", K(ret));
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Specify partition option of local index");
                } else if (NOT_SPECIFIED == index_scope_) {
                  index_scope_ = GLOBAL_INDEX;
                }
                is_index_part_specified = true;
              }
            }

            if (OB_SUCC(ret) && is_mysql_mode()) {
              if (OB_ISNULL(colulmn_group_node)) {
                // no cg, ignore
              } else if (T_COLUMN_GROUP != colulmn_group_node->type_ || colulmn_group_node->num_child_ <= 0) {
                ret = OB_INVALID_ARGUMENT;
                SQL_RESV_LOG(WARN, "invalid argument", KR(ret), K(colulmn_group_node->type_), K(colulmn_group_node->num_child_));
              } else if (OB_ISNULL(colulmn_group_node->children_[0])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("node is null", K(ret));
              } else if (OB_FAIL(resolve_index_column_group(colulmn_group_node, *create_index_arg))) {
                SQL_RESV_LOG(WARN, "resolve index column group failed", K(ret));
              }
            }

            if (OB_SUCC(ret) && lib::is_mysql_mode()) {
              if (OB_FAIL(set_index_tablespace(*table_schema_, *create_index_arg))) {
                LOG_WARN("fail to set index tablespace", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              uint64_t tenant_data_version = 0;
              create_index_arg->sql_mode_ = session_info_->get_sql_mode();
              if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
                LOG_WARN("get tenant data version failed", K(ret));
              } else if (tenant_data_version < DATA_VERSION_4_2_2_0) {
                //do nothing
              } else if (OB_FAIL(create_index_arg->local_session_var_.load_session_vars(session_info_))) {
                LOG_WARN("fail to fill session info into local_session_var", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(generate_index_arg(*create_index_arg, is_unique_key))) {
                SQL_RESV_LOG(WARN, "failed to generate index arg!", K(ret));
              } else if (table_schema_->is_partitioned_table()
                         && INDEX_TYPE_SPATIAL_GLOBAL == create_index_arg->index_type_) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "spatial global index");
              } else {
                create_index_arg->index_schema_.set_table_type(USER_INDEX);
                create_index_arg->index_schema_.set_index_type(create_index_arg->index_type_);
                create_index_arg->index_schema_.set_tenant_id(session_info_->get_effective_tenant_id());
                if (OB_FAIL(create_index_stmt.get_create_index_arg().assign(*create_index_arg))) {
                  LOG_WARN("fail to assign create index arg", K(ret));
                }
              }
            }
            if (OB_SUCC(ret)) {
              ObSArray<ObPartitionResolveResult> &resolve_results = alter_table_stmt->get_index_partition_resolve_results();
              ObSArray<obrpc::ObCreateIndexArg*> &index_arg_list = alter_table_stmt->get_index_arg_list();
              ObPartitionResolveResult resolve_result;
              ObCreateIndexArg &index_arg = create_index_stmt.get_create_index_arg();
              if (is_index_part_specified) {
                ObTableSchema &index_schema = index_arg.index_schema_;
                SMART_VAR(ObCreateIndexArg, my_create_index_arg) {
                  SMART_VAR(ObTableSchema, new_table_schema) {
                    ObArray<ObColumnSchemaV2 *> gen_columns;
                    if (OB_FAIL(new_table_schema.assign(*table_schema_))) {
                      LOG_WARN("fail to assign schema", K(ret));
                    } else if (OB_FAIL(my_create_index_arg.assign(index_arg))) {
                      LOG_WARN("fail to assign index arg", K(ret));
                    } else if (OB_FAIL(share::ObIndexBuilderUtil::adjust_expr_index_args(
                            my_create_index_arg, new_table_schema, *allocator_, gen_columns))) {
                      LOG_WARN("fail to adjust expr index args", K(ret));
                    } else if (OB_FAIL(share::ObIndexBuilderUtil::set_index_table_columns(
                              my_create_index_arg, new_table_schema, my_create_index_arg.index_schema_, false))) {
                      LOG_WARN("fail to set index table columns", K(ret));
                    } else if (OB_FAIL(index_schema.assign(my_create_index_arg.index_schema_))){
                      LOG_WARN("fail to assign schema", K(ret));
                    }
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(resolve_index_partition_node(index_partition_option->children_[0], &create_index_stmt))) {
                  LOG_WARN("fail to resolve partition option", K(ret));
                } else {
                  resolve_result.get_part_fun_exprs() = create_index_stmt.get_part_fun_exprs();
                  resolve_result.get_part_values_exprs() = create_index_stmt.get_part_values_exprs();
                  resolve_result.get_subpart_fun_exprs() = create_index_stmt.get_subpart_fun_exprs();
                  resolve_result.get_template_subpart_values_exprs() = create_index_stmt.get_template_subpart_values_exprs();
                  resolve_result.get_individual_subpart_values_exprs() = create_index_stmt.get_individual_subpart_values_exprs();
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(create_index_arg->assign(index_arg))) {
                  LOG_WARN("fail to assign create index arg", K(ret));
                } else if (share::schema::is_fts_index(index_arg.index_type_)) {
                  if (OB_FAIL(ObDDLResolver::append_domain_index_args(*table_schema_,
                                                                      resolve_result,
                                                                      create_index_arg,
                                                                      have_generate_fts_arg_,
                                                                      resolve_results,
                                                                      index_arg_list,
                                                                      allocator_))) {
                    LOG_WARN("failed to append domain index args", K(ret), K(index_arg));
                  } else {
                    // record allocator to free fts arg in desctructor
                    alter_table_stmt->set_fts_arg_allocator(allocator_);
                  }
                } else if (is_multivalue_index(index_arg.index_type_)) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("dynamic add multivalue index not supported yet", K(ret));
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic add multivalue index not supported yet");
                } else {
                  if (OB_FAIL(resolve_results.push_back(resolve_result))) {
                    LOG_WARN("fail to push back index_stmt_list", K(ret),
                        K(resolve_result));
                  } else if (OB_FAIL(index_arg_list.push_back(create_index_arg))) {
                    LOG_WARN("fail to push back index_arg", K(ret));
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              storing_column_set_.reset();  //storing column for each index
              sort_column_array_.reset();   //column for each index
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_constraint(const ParseNode &node)
{
  int ret = OB_SUCCESS;

  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  AlterTableSchema &alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
  ObSEArray<ObConstraint, 4> csts;
  for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin(); OB_SUCC(ret) &&
    iter != alter_table_schema.constraint_end(); iter ++) {
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

int ObAlterTableResolver::resolve_add_partition(const ParseNode &node,
                                                const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  AlterTableSchema &alter_table_schema = get_alter_table_stmt()->get_alter_table_arg().alter_table_schema_;
  ObTableStmt *alter_stmt = get_alter_table_stmt();
  ObSEArray<ObString, 8> dummy_part_keys;
  const ObPartitionOption &part_option = orig_table_schema.get_part_option();
  const ObPartitionFuncType part_func_type = part_option.get_part_func_type();
  ParseNode *part_func_node = NULL;
  ParseNode *part_elements_node = NULL;
  alter_table_schema.set_part_level(orig_table_schema.get_part_level());
  if (OB_ISNULL(node.children_[0]) ||
      OB_ISNULL(part_elements_node = node.children_[0]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node.children_[0]), K(part_elements_node));
  } else if (OB_NOT_NULL(params_.session_info_) &&
             !params_.session_info_->is_inner() &&
             orig_table_schema.is_interval_part()) {
    ret = OB_ERR_ADD_PARTITION_ON_INTERVAL;
    LOG_WARN("add partition on interval");
  } else if (OB_FAIL(mock_part_func_node(orig_table_schema, false/*is_sub_part*/, part_func_node))) {
    LOG_WARN("mock part func node failed", K(ret));
  } else if (OB_FAIL(resolve_part_func(params_, part_func_node,
                                       part_func_type, orig_table_schema,
                                       alter_stmt->get_part_fun_exprs(), dummy_part_keys))) {
    LOG_WARN("resolve part func failed", K(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    // 一级分区表加一级分区
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
      if (OB_FAIL(inner_add_partition(part_elements_node, part_func_type, part_option,
                                      alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to inner add partition", K(ret));
      }
    }
  } else {
    // 非模板化二级分区表加一级分区, 支持显示定义一级分区下的二级分区
    // 1. no_subpart == true: subpart info is the template of table.
    // 2. no_subpart == false: subpart info is specified by clause.
    bool no_subpart = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_elements_node->num_child_; ++i) {
      if (OB_ISNULL(part_elements_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      no_subpart = part_elements_node->children_[0]->children_[ELEMENT_SUBPARTITION_NODE] == NULL;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_elements_node->num_child_; ++i) {
      if ((!no_subpart && NULL == part_elements_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE])
          || (no_subpart && NULL != part_elements_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE])) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("add partition with subpartition and add another partition without subpartition not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add partition with subpartition and add another partition without subpartition ");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (no_subpart && orig_table_schema.has_sub_part_template_def()) {
      bool generated = false;
      if (OB_FAIL(inner_add_partition(part_elements_node, part_func_type, part_option,
                 alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to inner add partition", K(ret));
      } else if (OB_FAIL(alter_table_schema.try_assign_def_subpart_array(orig_table_schema))) {
        LOG_WARN("fail to assign def sub partition array", KR(ret), K(orig_table_schema));
      } else if (OB_FAIL(alter_table_schema.try_generate_subpart_by_template(generated))) {
        LOG_WARN("fail to expand_def_subpart", K(ret), K(alter_table_schema));
      } else if (!generated) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("generate subpart by template failed", KR(ret));
      }
      alter_stmt->set_use_def_sub_part(false);
    } else {
      const ObPartitionOption &subpart_option = orig_table_schema.get_sub_part_option();
      const ObPartitionFuncType subpart_type = subpart_option.get_part_func_type();
      ParseNode *subpart_func_node = NULL;
      alter_stmt->set_use_def_sub_part(false);
      // 先设置好sub part option, 解析二级分区的定义时依赖
      alter_table_schema.get_sub_part_option() = orig_table_schema.get_sub_part_option();
      alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
      if (no_subpart && orig_table_schema.is_hash_like_subpart()) {
        hash_subpart_num_ = 1;
      }
      /* set subpartition key info */
      OZ (alter_table_schema.assign_subpartition_key_info(
                            orig_table_schema.get_subpartition_key_info()));
      OZ (mock_part_func_node(orig_table_schema, true/*is_sub_part*/, subpart_func_node));
      OZ (resolve_part_func(params_, subpart_func_node,
                            subpart_type, orig_table_schema,
                            alter_stmt->get_subpart_fun_exprs(), dummy_part_keys));
      OZ (inner_add_partition(part_elements_node, part_func_type, part_option,
                              alter_stmt, alter_table_schema));
      if (OB_SUCC(ret) && no_subpart) {
        const int64_t part_num = alter_table_schema.get_partition_num();
        ObPartition **part_array = alter_table_schema.get_part_array();
        if (OB_ISNULL(part_array) || part_num <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("part_array is null or part_num is invalid", K(ret), KP(part_array), K(part_num));
        } else {
          const int64_t BUF_SIZE = OB_MAX_PARTITION_NAME_LENGTH;
          char buf[BUF_SIZE];
          ObSubPartition *subpart;
          for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
            ObPartition *part = part_array[i];
            MEMSET(buf, 0, BUF_SIZE);
            int64_t pos = 0;
            ObString sub_part_name;
            if (OB_ISNULL(part) || OB_ISNULL(part->get_subpart_array()) ||
               OB_ISNULL(subpart = part->get_subpart_array()[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret), K(i), K(part), K(subpart));
            } else if (OB_FAIL(databuff_printf(buf, BUF_SIZE, pos, "%s%s",
                      part->get_part_name().ptr(), lib::is_oracle_mode() ? "SP0" : "sp0"))) {
              LOG_WARN("part name is too long", K(ret), KPC(part), K(subpart));
            } else if (FALSE_IT(sub_part_name.assign_ptr(buf, static_cast<int32_t>(strlen(buf))))) {
            } else if (OB_FAIL(subpart->set_part_name(sub_part_name))) {
              LOG_WARN("set subpart name failed", K(ret), K(sub_part_name));
            } else {
              subpart->set_is_empty_partition_name(false);
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to resolve partition elements", KPC(alter_stmt),
          K(alter_stmt->get_part_fun_exprs()),
          K(alter_stmt->get_part_values_exprs()),
          K(alter_stmt->get_subpart_fun_exprs()),
          K(alter_stmt->get_individual_subpart_values_exprs()));

    alter_table_schema.get_part_option() = orig_table_schema.get_part_option();
    alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
    // check part_name and subpart_name duplicate
    if (OB_FAIL(check_and_set_partition_names(alter_stmt, alter_table_schema, false))) {
      LOG_WARN("failed to check and set partition names", K(ret));
    } else if (PARTITION_LEVEL_TWO == orig_table_schema.get_part_level()) {
      if (OB_FAIL(check_and_set_individual_subpartition_names(alter_stmt, alter_table_schema))) {
        LOG_WARN("failed to check and set individual subpartition names", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::inner_add_partition(ParseNode *part_elements_node,
                                              const ObPartitionFuncType part_type,
                                              const ObPartitionOption &part_option,
                                              ObTableStmt *alter_stmt,
                                              ObTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
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
                                                        alter_stmt->get_part_values_exprs()))) {
      LOG_WARN("failed to resolve reange partition elements", K(ret));
    }
  } else if (part_option.is_list_part()) {
    if (T_RANGE_PARTITION_LIST == part_elements_node->type_) {
      ret = OB_ERR_PARTITION_NOT_EXPECT_VALUES_LESS_THAN;
      LOG_WARN("VALUES LESS THAN or AT clause cannot be used with List partitioned tables", K(ret));
    } else if (OB_FAIL(resolve_list_partition_elements(alter_stmt,
                                                       part_elements_node,
                                                       alter_table_schema,
                                                       part_type,
                                                       alter_stmt->get_part_fun_exprs(),
                                                       alter_stmt->get_part_values_exprs()))) {
      LOG_WARN("failed to resolve list partition elements", K(ret));
    }
  } else if (part_option.is_hash_like_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("add hash partition not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add hash partition");
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_subpartition(const ParseNode &node,
                                                   const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption &subpart_option = orig_table_schema.get_sub_part_option();
  const ObPartitionFuncType subpart_type = subpart_option.get_part_func_type();
  ParseNode *subpart_func_node = NULL;
  ParseNode *part_name_node = NULL;
  ParseNode *part_elements_node = NULL;
  ObTableStmt *alter_stmt = get_alter_table_stmt();

  if (OB_ISNULL(part_name_node = node.children_[0]) || OB_ISNULL(node.children_[1]) ||
      OB_ISNULL(part_elements_node = node.children_[1]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(alter_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter_stmt is null", KR(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    ret = OB_ERR_NOT_COMPOSITE_PARTITION;
    LOG_WARN("table is not partitioned by composite partition method", K(ret));
  } else if (OB_FAIL(mock_part_func_node(orig_table_schema, true/*is_sub_part*/, subpart_func_node))) {
    LOG_WARN("mock part func node failed", K(ret));
  } else {
    AlterTableSchema &alter_table_schema = get_alter_table_stmt()->get_alter_table_arg().alter_table_schema_;
    ObSEArray<ObString, 8> dummy_part_keys;
    ObPartition dummy_part;
    ObPartition *cur_partition = NULL;
    alter_stmt->set_use_def_sub_part(false);
    // 先设置好sub part option, 解析二级分区的定义时依赖
    alter_table_schema.get_sub_part_option() = orig_table_schema.get_sub_part_option();
    // resolve partition name
    ObString partition_name(static_cast<int32_t>(part_name_node->str_len_),
                            part_name_node->str_value_);
    int64_t part_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_table_schema.get_partition_num(); ++i) {
      ObPartition *ori_partition = orig_table_schema.get_part_array()[i];
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
      LOG_USER_ERROR(OB_UNKNOWN_PARTITION, partition_name.length(), partition_name.ptr(),
                                           orig_table_schema.get_table_name_str().length(),
                                           orig_table_schema.get_table_name_str().ptr());
    } else if (OB_FAIL(dummy_part.set_part_name(partition_name))) {
      LOG_WARN("failed to set subpart name", K(ret), K(partition_name));
    } else if (OB_FAIL(alter_table_schema.add_partition(dummy_part))) {
      LOG_WARN("failed to add partition", K(ret));
    } else if (OB_ISNULL(cur_partition = alter_table_schema.get_part_array()[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    // resolve subpartition define
    } else if (OB_FAIL(resolve_part_func(params_, subpart_func_node,
                                         subpart_type, orig_table_schema,
                                         alter_stmt->get_subpart_fun_exprs(), dummy_part_keys))) {
      LOG_WARN("resolve part func failed", K(ret));
    } else if (subpart_option.is_range_part()) {
      if (T_LIST_SUBPARTITION_LIST == part_elements_node->type_) {
        ret = OB_ERR_SUBPARTITION_NOT_EXPECT_VALUES_IN;
        LOG_WARN("VALUES (<value list>) cannot be used for Range subpartitioned tables", K(ret));
      } else if (OB_FAIL(resolve_subpartition_elements(alter_stmt,
                                                       part_elements_node,
                                                       alter_table_schema,
                                                       cur_partition,
                                                       false))) {
        LOG_WARN("failed to resolve subpartition elements", K(ret));
      }
    } else if (subpart_option.is_list_part()) {
      if (T_RANGE_SUBPARTITION_LIST == part_elements_node->type_) {
        ret = OB_ERR_SUBPARTITION_EXPECT_VALUES_IN;
        LOG_WARN("VALUES (<value list>) clause expected", K(ret));
      } else if (OB_FAIL(resolve_subpartition_elements(alter_stmt,
                                                       part_elements_node,
                                                       alter_table_schema,
                                                       cur_partition,
                                                       false))) {
        LOG_WARN("failed to resolve subpartition elements", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to resolve subpartition elements", KPC(alter_stmt),
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
    }
  }
  return ret;
}

int ObAlterTableResolver::mock_part_func_node(const ObTableSchema &table_schema,
                                              const bool is_sub_part,
                                              ParseNode *&part_expr_node)
{
  int ret = OB_SUCCESS;
  part_expr_node = NULL;

  ObSqlString sql_str;
  ParseResult parse_result;
  ParseNode *stmt_node = NULL;
  ParseNode *select_node = NULL;
  ParseNode *select_expr_list = NULL;
  ParseNode *select_expr_node = NULL;
  ObParser parser(*allocator_, params_.session_info_->get_sql_mode());
  const ObString &part_str = is_sub_part ?
      table_schema.get_sub_part_option().get_part_func_expr_str() :
      table_schema.get_part_option().get_part_func_expr_str();
  ObPartitionFuncType part_type = is_sub_part ?
      table_schema.get_sub_part_option().get_part_func_type() :
      table_schema.get_part_option().get_part_func_type();

  if (is_inner_table(table_schema.get_table_id())) {
    if (OB_FAIL(sql_str.append_fmt("SELECT partition_%.*s FROM DUAL",
                                   part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_KEY == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_KEY,
                                   part_str.length(), part_str.ptr()))) {
      LOG_WARN("fail to concat string", K(part_str), K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_HASH == part_type) {
    if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_HASH,
                                   part_str.length(), part_str.ptr()))) {
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
    _OB_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d], sql[%.*s]",
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
  } else if (OB_ISNULL(stmt_node = parse_result.result_tree_) ||
             OB_UNLIKELY(stmt_node->type_ != T_STMT_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt node is invalid", K(stmt_node));
  } else if (OB_ISNULL(select_node = stmt_node->children_[0]) ||
             OB_UNLIKELY(select_node->type_ != T_SELECT)) {
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


int ObAlterTableResolver::generate_index_arg(obrpc::ObCreateIndexArg &index_arg, const bool is_unique_key)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(session_info_) || OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info should not be null", K(session_info_), K(alter_table_stmt));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else {
    //add storing column
    for (int32_t i = 0; OB_SUCC(ret) && i < store_column_names_.count(); ++i) {
      if (OB_FAIL(index_arg.store_columns_.push_back(store_column_names_.at(i)))) {
        SQL_RESV_LOG(WARN, "failed to add storing column!", "column_name",
                     store_column_names_.at(i), K(ret));
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < hidden_store_column_names_.count(); ++i) {
      if (OB_FAIL(index_arg.hidden_store_columns_.push_back(hidden_store_column_names_.at(i)))) {
        SQL_RESV_LOG(WARN, "failed to add storing column!", "column_name",
                     hidden_store_column_names_.at(i), K(ret));
      }
    }
    index_arg.tenant_id_ = session_info_->get_effective_tenant_id();
    index_arg.table_name_ = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_table_name();
    index_arg.database_name_ = alter_table_stmt->get_alter_table_arg().alter_table_schema_.get_origin_database_name();
    //set index option
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
      if (OB_NOT_NULL(table_schema_) && table_schema_->is_oracle_tmp_table()) {
        index_scope_ = LOCAL_INDEX;
      }
      if (NOT_SPECIFIED == index_scope_) {
        // MySQL default index mode is local,
        // and Oracle default index mode is global
        global_ = lib::is_oracle_mode();
      } else {
        global_ = (GLOBAL_INDEX == index_scope_);
      }
      if (is_unique_key) {
        if (global_) {
          type = INDEX_TYPE_UNIQUE_GLOBAL;
        } else {
          type = INDEX_TYPE_UNIQUE_LOCAL;
        }

        if (index_keyname_ == MULTI_KEY) {
          type = INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL;
          if (global_) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("global index, multivalue index not supported", K(ret), K(tenant_data_version));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "global multivalue index");
          }
        }
      } else {
        if (tenant_data_version < DATA_VERSION_4_1_0_0 && index_keyname_ == SPATIAL_KEY) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.1, spatial index is not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, spatial index");
        } else if (tenant_data_version < DATA_VERSION_4_3_1_0 && index_keyname_ == FTS_KEY) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, fulltext index not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, fulltext index");
        } else if (tenant_data_version < DATA_VERSION_4_3_1_0 && index_keyname_ == MULTI_KEY) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant data version is less than 4.3.1, multivalue index not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
        } else if (global_) {
          if (index_keyname_ == SPATIAL_KEY) {
            type = INDEX_TYPE_SPATIAL_GLOBAL;
          } else if (index_keyname_ == FTS_KEY) {
            type = INDEX_TYPE_DOC_ID_ROWKEY_GLOBAL;
          } else if (index_keyname_ == MULTI_KEY) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("global multivalue index not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, multivalue index");
          } else {
            type = INDEX_TYPE_NORMAL_GLOBAL;
          }
        } else {
          if (index_keyname_ == SPATIAL_KEY) {
            type = INDEX_TYPE_SPATIAL_LOCAL;
          } else if (index_keyname_ == FTS_KEY) {
            type = INDEX_TYPE_DOC_ID_ROWKEY_LOCAL;
          } else if (index_keyname_ == MULTI_KEY) {
            type = INDEX_TYPE_NORMAL_MULTIVALUE_LOCAL;
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

int ObAlterTableResolver::resolve_drop_index(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_DROP != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode *index_node = node.children_[0];
    if (OB_ISNULL(index_node) || T_IDENT != index_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObString drop_index_name;
      drop_index_name.assign_ptr(index_node->str_value_,
                                 static_cast<int32_t>(index_node->str_len_));
      //construct ObDropIndexArg
      ObDropIndexArg *drop_index_arg = NULL;
      void *tmp_ptr = NULL;
      if (NULL == (tmp_ptr = (ObDropIndexArg *)allocator_->alloc(sizeof(obrpc::ObDropIndexArg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        drop_index_arg = new (tmp_ptr)ObDropIndexArg();
        drop_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        drop_index_arg->index_name_ = drop_index_name;
      }
      //push drop index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
        // 删除索引列的时候需要检查索引相关的列是否是外键列,如果是外键列则不允许删除，包含情况：
        // 1. 索引的主表是父表
        // 2. 索引的主表是子表
        // 在 fetch_foreign_key_info 的时候，会把 parent/child table 的 foreign key info 都带过来
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (table_schema_->is_parent_table() || table_schema_->is_child_table()) {
          const ObTableSchema *index_table_schema = NULL;
          ObString index_table_name;
          ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
          bool has_other_indexes_on_same_cols = false;
          if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                            table_schema_->get_table_id(),
                                                            drop_index_name,
                                                            index_table_name))) {
            LOG_WARN("build_index_table_name failed", K(ret), K(table_schema_->get_table_id()), K(drop_index_name));
          } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                               alter_table_stmt->get_org_database_name(),
                                                               index_table_name,
                                                               true /* index table */,
                                                               index_table_schema))) {
            if (OB_TABLE_NOT_EXIST == ret) {
              if (is_mysql_mode()) {
                ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
                LOG_WARN("index does not exist", K(ret), K(drop_index_name));
                LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, drop_index_name.length(), drop_index_name.ptr());
              } else {
                LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(alter_table_stmt->get_org_database_name()),
                              to_cstring(alter_table_stmt->get_org_table_name()));
              }
            }
            LOG_WARN("fail to get index table schema", K(ret));
          } else if (OB_ISNULL(index_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret));
          } else if (OB_FAIL(check_indexes_on_same_cols(*table_schema_,
                                                        *index_table_schema,
                                                        *schema_checker_,
                                                        has_other_indexes_on_same_cols))) {
            LOG_WARN("check indexes on same cols failed", K(ret));
          } else if (!has_other_indexes_on_same_cols && lib::is_mysql_mode()) {
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

int ObAlterTableResolver::resolve_drop_foreign_key(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  ParseNode *name_node = NULL;
  ObDropForeignKeyArg *foreign_key_arg = NULL;
  ObString foreign_key_name;
  bool has_same_fk_arg = false;
  void *tmp_ptr = NULL;
  if ((lib::is_mysql_mode() && ((T_FOREIGN_KEY_DROP != node.type_ && T_DROP_CONSTRAINT != node.type_) || OB_ISNULL(node.children_)))
      || (lib::is_oracle_mode() && (T_DROP_CONSTRAINT != node.type_ || OB_ISNULL(node.children_)))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), K(node.type_));
  } else if (OB_ISNULL(name_node = node.children_[0]) || T_IDENT != name_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), KP(name_node), K(name_node->type_));
  } else if (OB_ISNULL(name_node->str_value_) || name_node->str_len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", KP(name_node->str_value_), K(name_node->str_len_), K(ret));
  } else if (OB_ISNULL(tmp_ptr = allocator_->alloc(sizeof(ObDropForeignKeyArg)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
  } else if (FALSE_IT(foreign_key_arg = new (tmp_ptr)ObDropForeignKeyArg())) {
  } else if (FALSE_IT(foreign_key_arg->foreign_key_name_.assign_ptr(name_node->str_value_,
                                                                    static_cast<int32_t>(name_node->str_len_)))) {
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  } else if (OB_FAIL(alter_table_stmt->check_drop_fk_arg_exist(foreign_key_arg, has_same_fk_arg))) {
    SQL_RESV_LOG(WARN, "check_drop_fk_arg_exist failed", K(ret), K(foreign_key_arg));
  } else if (has_same_fk_arg) {
    // do nothing
  } else if (OB_FAIL(alter_table_stmt->add_index_arg(foreign_key_arg))) {
    SQL_RESV_LOG(WARN, "add index to drop_index_list failed!", K(ret));
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_constraint(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (lib::is_mysql_mode()) {
    const ParseNode *name_list = node.children_[0];
    if (OB_ISNULL(name_list)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (T_NAME_LIST != name_list->type_) {
        name_list = &node;
      }
      AlterTableSchema &alter_table_schema =
          alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObConstraint cst;
        ObString constraint_name(static_cast<int32_t>(name_list->children_[i]->str_len_),
            name_list->children_[i]->str_value_);
        for (iter = table_schema_->constraint_begin(); OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
          if (0 == constraint_name.case_compare((*iter)->get_constraint_name_str())) {
            if (OB_FAIL(cst.assign(**iter))) {
              SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
            }
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (table_schema_->constraint_end() == iter) {
          if (T_DROP_CONSTRAINT == node.type_) { // drop constraint <name>
            ret = OB_ERR_NONEXISTENT_CONSTRAINT;
            LOG_USER_ERROR(OB_ERR_NONEXISTENT_CONSTRAINT, constraint_name.length(), constraint_name.ptr());
          } else { // drop check <name> | drop {constraint|check} '(' <name_list> ')'
            ret = OB_ERR_CHECK_CONSTRAINT_NOT_FOUND;
            LOG_USER_ERROR(OB_ERR_CHECK_CONSTRAINT_NOT_FOUND, constraint_name.length(), constraint_name.ptr());
          }
          SQL_RESV_LOG(WARN, "Cannot drop check constraint - nonexistent constraint",
                       K(ret),
                       K(constraint_name),
                       K(table_schema_->get_table_name_str()));
        } else if (OB_FAIL(alter_table_schema.add_constraint(cst))){
          SQL_RESV_LOG(WARN, "add constraint failed!", K(cst), K(ret));
        }
      }
    }
  } else if (lib::is_oracle_mode()) {
    const ParseNode *constraint_name = node.children_[0];
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      AlterTableSchema &alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      ObConstraint cst;
      ObString constraint_name_str(static_cast<int32_t>(constraint_name->str_len_),constraint_name->str_value_);
      ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
      for (;OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
        if (0 == constraint_name_str.case_compare((*iter)->get_constraint_name_str())) {
          if (OB_FAIL(cst.assign(**iter))) {
            SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
          }
          break;
        }
      }
      bool has_same_cst = false;
      if (OB_FAIL(ret)) {
      } else if (table_schema_->constraint_end() == iter) {
        ret = OB_ERR_NONEXISTENT_CONSTRAINT;
        SQL_RESV_LOG(WARN, "Cannot drop check constraint - nonexistent constraint", K(ret), K(constraint_name_str), K(table_schema_->get_table_name_str()));
      } else if (OB_FAIL(alter_table_stmt->check_drop_cst_exist(cst, has_same_cst))) {
        SQL_RESV_LOG(WARN, "check_drop_cst_exist failed", K(ret), K(cst), K(has_same_cst));
      } else if (has_same_cst) {
        // skip
      } else if (OB_FAIL(alter_table_schema.add_constraint(cst))){
        SQL_RESV_LOG(WARN, "add constraint failed!", K(cst), K(ret));
      } else if (CONSTRAINT_TYPE_NOT_NULL == cst.get_constraint_type()) {
        // need to add alter_column_schema when drop not null constraint, in order to modify column_flags.
        uint64_t column_id = OB_INVALID_ID;
        ObColumnSchemaV2 *column_schema = NULL;
        if (OB_UNLIKELY(0 == cst.get_column_cnt()) || OB_ISNULL(cst.cst_col_begin())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column ids", K(ret), K(cst));
        } else if (FALSE_IT(column_id = *cst.cst_col_begin())) {
        } else if (NULL == (column_schema = alter_table_schema.get_column_schema(column_id))) {
          AlterColumnSchema alter_column_schema;
          const ObColumnSchemaV2 *origin_col_schema = NULL;
          if (OB_FAIL(schema_checker_->get_column_schema(table_schema_->get_tenant_id(),
                                                         table_schema_->get_table_id(),
                                                         column_id, origin_col_schema, false))) {
            LOG_WARN("get not null origin col schema failed", K(ret));
          } else if (OB_UNLIKELY(origin_col_schema->is_identity_column())) {
            ret = origin_col_schema->is_default_on_null_identity_column()
                 ? OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_COLUMN
                 : OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
            LOG_WARN("can't drop not null constraint on an identity column", K(ret));
          } else if (OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
            LOG_WARN("copy column schema failed", K(ret));
          } else if (OB_FAIL(alter_column_schema.set_origin_column_name(alter_column_schema.get_column_name_str()))) {
            LOG_WARN("set origin column name faield", K(ret));
          } else {
            alter_column_schema.drop_not_null_cst();
            alter_column_schema.alter_type_ = OB_DDL_MODIFY_COLUMN;
            // drop not null constriant == modify column null
            alter_table_stmt->set_alter_table_column();
            if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
              LOG_WARN("add alter column schema failed", K(ret));
            }
            LOG_DEBUG("drop not null constraint", KPC(origin_col_schema), K(alter_column_schema));
          }
        } else {
          if (OB_UNLIKELY(column_schema->is_identity_column())) {
            ret = column_schema->is_default_on_null_identity_column()
                  ? OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_COLUMN
                  : OB_ERR_CANNOT_DROP_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
            LOG_WARN("can't drop not null constraint on an identity column", K(ret));
          } else {
            column_schema->drop_not_null_cst();
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_partition(const ParseNode &node,
                                                 const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  if ((T_ALTER_PARTITION_DROP != node.type_ && T_ALTER_PARTITION_TRUNCATE != node.type_)
      || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (orig_table_schema.is_hash_like_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Drop hash partition not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Drop hash partition");
  } else {
    const ParseNode *name_list = node.children_[0];
    if (OB_ISNULL(name_list)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      }
      AlterTableSchema &alter_table_schema =
          alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObPartition part;
        ObString partition_name(static_cast<int32_t>(name_list->children_[i]->str_len_),
            name_list->children_[i]->str_value_);
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
        alter_table_schema.get_part_option().set_part_num(
            alter_table_schema.get_partition_num());
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_subpartition(const ParseNode &node,
                                                    const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  if ((T_ALTER_SUBPARTITION_DROP != node.type_ && T_ALTER_SUBPARTITION_TRUNCATE != node.type_)
      || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (share::schema::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
    ret = OB_ERR_NOT_COMPOSITE_PARTITION;
    LOG_WARN("table is not partitioned by composite partition method", K(ret));
  } else if (orig_table_schema.is_hash_like_subpart()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop hash subpartition not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Drop hash subpartition");
  } else {
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    const ParseNode *name_list = node.children_[0];
    if (OB_ISNULL(name_list) || OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(name_list), K(alter_table_stmt));
    } else {
      AlterTableSchema &alter_table_schema =
          alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      ObPartition dummy_part;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObSubPartition subpart;
        ObString partition_name(static_cast<int32_t>(name_list->children_[i]->str_len_),
            name_list->children_[i]->str_value_);
        if (OB_FAIL(subpart.set_part_name(partition_name))) {
          LOG_WARN("failed to set subpart name", K(ret), K(partition_name));
        } else if (OB_FAIL(check_subpart_name(dummy_part, subpart))){
          LOG_WARN("failed to check subpart name", K(subpart), K(ret));
        } else if (OB_FAIL(dummy_part.add_partition(subpart))){
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

int ObAlterTableResolver::resolve_exchange_partition(const ParseNode &node,
                                                     const ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  if (OB_UNLIKELY(T_ALTER_PARTITION_EXCHANGE != node.type_ || OB_ISNULL(node.children_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(node.type_));
  } else if (OB_UNLIKELY(2 != node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree, num child != 2", K(ret), K(node.num_child_));
  } else if (OB_ISNULL(node.children_[0]) || OB_ISNULL(node.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(alter_table_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info and alter table stmt should not be null", K(ret), KPC(session_info_), KPC(alter_table_stmt));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get data version failed", K(ret), K(session_info_->get_effective_tenant_id()), K(tenant_data_version));
  } else if (OB_UNLIKELY(tenant_data_version < DATA_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version and feature mismatch", K(ret), K(tenant_data_version));
  } else if (OB_UNLIKELY(!orig_table_schema.is_user_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table type");
    LOG_WARN("unsupport behavior on not user table", K(ret), K(orig_table_schema));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO == part_level)) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non partitioned table", K(ret), K(part_level));
  } else {
    const ObPartition *part = nullptr;
    const ObSubPartition *subpart = nullptr;
    const ObTableSchema *exchange_table_schema = NULL;
    ObString exchange_table_name;
    ObString exchange_db_name;
    ParseNode *second_node = node.children_[1];
    ObString origin_partition_name(static_cast<int32_t>(node.children_[0]->str_len_), node.children_[0]->str_value_);
    if (OB_FAIL(resolve_table_relation_node(second_node,
                                            exchange_table_name,
                                            exchange_db_name))){
      LOG_WARN("failed to resolve exchange table node", K(ret), K(exchange_table_name), K(exchange_db_name));
    } else if (lib::is_oracle_mode() && 0 != exchange_db_name.compare(session_info_->get_database_name())) {
      ret = OB_TABLE_NOT_EXIST;//compatible with Oracle, reporting error table does not exist
      LOG_WARN("Swapping partitions between different database tables is not supported in oracle mode", K(ret), K(lib::is_oracle_mode()), K(exchange_db_name), K(session_info_->get_database_name()));
    } else if (0 == exchange_db_name.compare(session_info_->get_database_name()) && 0 == exchange_table_name.compare(orig_table_schema.get_table_name())) {
      ret = OB_ERR_NONUNIQ_TABLE;
      LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, exchange_table_name.length(), exchange_table_name.ptr());
      LOG_WARN("Not unique table/alias", K(ret), K(exchange_table_name), K(exchange_db_name), K(session_info_->get_database_name()), K(orig_table_schema.get_table_name()));
    } else if (OB_ISNULL(schema_checker_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("schema_checker should not be null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                         exchange_db_name,
                                                         exchange_table_name,
                                                         false,
                                                         exchange_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(exchange_db_name), to_cstring(exchange_table_name));
      }
      LOG_WARN("fail to get table schema", K(ret), KPC(exchange_table_schema), K(exchange_db_name), K(exchange_table_name));
    } else if (OB_ISNULL(exchange_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not found", K(ret), KPC(exchange_table_schema), K(exchange_db_name), K(exchange_table_name));
    } else if (OB_UNLIKELY(!exchange_table_schema->is_user_table())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table type");
      LOG_WARN("unsupport behavior on not user table", K(ret), KPC(exchange_table_schema));
    } else if (OB_UNLIKELY(exchange_table_schema->is_partitioned_table())) {
      ret = OB_ERR_PARTITION_EXCHANGE_PART_TABLE;
      LOG_USER_ERROR(OB_ERR_PARTITION_EXCHANGE_PART_TABLE, exchange_table_name.length(), exchange_table_name.ptr());
    } else if (share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE == orig_table_schema.get_part_level()) {
      if (OB_FAIL(orig_table_schema.get_partition_by_name(origin_partition_name, part))) {
        LOG_WARN("fail to get partition", K(ret), K(orig_table_schema), K(origin_partition_name));
      } else if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition not found", K(ret), K(orig_table_schema), K(origin_partition_name));
      } else {
        share::schema::ObPartitionFuncType part_type = orig_table_schema.get_part_option().get_part_func_type();
        if (OB_UNLIKELY(PARTITION_FUNC_TYPE_RANGE != part_type && PARTITION_FUNC_TYPE_RANGE_COLUMNS != part_type)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Only support exchanging range/range columns partitions currently", K(ret), K(part_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Exchange partition except range/range columns");
        }
      }
    } else if (share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO == orig_table_schema.get_part_level()) {
      if (OB_FAIL(orig_table_schema.get_partition_by_name(origin_partition_name, part))) {
        if (OB_UNKNOWN_PARTITION == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(orig_table_schema.get_subpartition_by_name(origin_partition_name, part, subpart))) {
            LOG_WARN("get subpartition by name failed", K(ret), K(orig_table_schema), K(origin_partition_name));
          } else if (OB_ISNULL(part) || OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition not found", K(ret), K(OB_ISNULL(part)), K(OB_ISNULL(subpart)));
          } else {
            share::schema::ObPartitionFuncType subpart_type = orig_table_schema.get_sub_part_option().get_part_func_type();
            if (OB_UNLIKELY(PARTITION_FUNC_TYPE_RANGE != subpart_type && PARTITION_FUNC_TYPE_RANGE_COLUMNS != subpart_type)) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("Only support exchanging range/range columns partitions currently", K(ret), K(subpart_type));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Exchange partition except range/range columns");
            }
          }
        } else {
          LOG_WARN("fail to get partition by name", K(ret), K(orig_table_schema), K(origin_partition_name));
        }
      } else {
        ret = OB_ERR_EXCHANGE_COMPOSITE_PARTITION;
        LOG_WARN("cannot EXCHANGE a composite partition with a non-partitioned table", K(ret), K(orig_table_schema), K(origin_partition_name));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition level is invalid", K(ret), K(orig_table_schema.get_part_level()));
    }
    if (OB_SUCC(ret)) {
      obrpc::ObExchangePartitionArg exchange_partition_arg;
      if (OB_FAIL(exchange_partition_arg.based_schema_object_infos_.assign(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_))) {
        LOG_WARN("fail to assign based_schema_object_infos", K(ret), K(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_));
      } else if (OB_FAIL(exchange_partition_arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(exchange_table_schema->get_table_id(), TABLE_SCHEMA, exchange_table_schema->get_schema_version())))) {
        LOG_WARN("failed to add exchange table info", K(ret), KPC(exchange_table_schema));
      } else {
        exchange_partition_arg.session_id_ = session_info_->get_sessid();
        exchange_partition_arg.tenant_id_  = session_info_->get_effective_tenant_id();
        exchange_partition_arg.exchange_partition_level_ = orig_table_schema.get_part_level();
        exchange_partition_arg.base_table_id_ = orig_table_schema.get_table_id();
        exchange_partition_arg.base_table_part_name_ = origin_partition_name;
        exchange_partition_arg.inc_table_id_ = exchange_table_schema->get_table_id();
        exchange_partition_arg.including_indexes_ = true;
        exchange_partition_arg.without_validation_ = true;
        exchange_partition_arg.update_global_indexes_ = false;
        exchange_partition_arg.exec_tenant_id_ = session_info_->get_effective_tenant_id();
        if (OB_FAIL(alter_table_stmt->set_exchange_partition_arg(exchange_partition_arg))) {
          LOG_WARN("fail to set exchange_partition_arg", K(ret), K(exchange_partition_arg));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_subpart_name(const ObPartition &partition,
                                             const ObSubPartition &subpartition)
{
  int ret = OB_SUCCESS;
  const ObString &subpart_name = subpartition.get_part_name();
  for (int64_t i = 0; OB_SUCC(ret) && i < partition.get_subpartition_num(); ++i) {
    if (common::ObCharset::case_insensitive_equal(subpart_name,
                                                  partition.get_subpart_array()[i]->get_part_name())) {
      ret = OB_ERR_SAME_NAME_PARTITION;
      LOG_WARN("subpart name is duplicate", K(ret), K(subpartition), K(i), "exists partition", partition.get_subpart_array()[i]);
      LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, subpart_name.length(), subpart_name.ptr());
    }
  }
  return ret;
}
int ObAlterTableResolver::resolve_rename_partition(const ParseNode &node,
                             const share::schema::ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  uint64_t tenant_data_version = 0;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (T_ALTER_PARTITION_RENAME != node.type_
      || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret));
  } else if (OB_UNLIKELY(2 != node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree, num child != 2", KR(ret), K(node.num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", KR(ret), KP(this));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get data version failed", KR(ret), K(session_info_->get_effective_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version and feature mismatch", KR(ret));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on non partitioned table", KR(ret), K(orig_table_schema));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", KR(ret));
  } else {
    AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    const ObPartition *part = nullptr;
    ObPartition inc_part;
    ObString origin_partition_name(static_cast<int32_t>(node.children_[0]->str_len_),
        node.children_[0]->str_value_);
    ObString new_partition_name(static_cast<int32_t>(node.children_[1]->str_len_),
        node.children_[1]->str_value_);
    const ObPartitionOption &ori_part_option = orig_table_schema.get_part_option();
    if (OB_UNLIKELY(ObCharset::case_insensitive_equal(origin_partition_name, new_partition_name))) {
      ret = OB_ERR_RENAME_PARTITION_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_RENAME_PARTITION_NAME_DUPLICATE, new_partition_name.length(), new_partition_name.ptr());
      LOG_WARN("origin part name equal to new part name", KR(ret), K(origin_partition_name), K(new_partition_name));
    } else if (OB_FAIL(orig_table_schema.check_partition_duplicate_with_name(new_partition_name))) {
      if (OB_DUPLICATE_OBJECT_NAME_EXIST == ret) {
        ret = OB_ERR_RENAME_PARTITION_NAME_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_RENAME_PARTITION_NAME_DUPLICATE, new_partition_name.length(), new_partition_name.ptr());
        LOG_WARN("new part name duplicate with existed partition", KR(ret), K(new_partition_name));
      } else {
        LOG_WARN("check new part name duplicate failed", KR(ret), K(new_partition_name));
      }
    } else if (OB_FAIL(orig_table_schema.get_partition_by_name(origin_partition_name, part))) {
      LOG_WARN("get part by name failed", KR(ret), K(origin_partition_name), K(orig_table_schema));
    } else if (OB_FAIL(alter_table_schema.set_new_part_name(new_partition_name))) {
      LOG_WARN("table set new partition name failed", KR(ret), K(alter_table_schema), K(new_partition_name));
    } else if (OB_FAIL(inc_part.set_part_name(origin_partition_name))) {
      LOG_WARN("inc part set name failed", KR(ret), K(inc_part), K(origin_partition_name));
    } else if (OB_FAIL(alter_table_schema.add_partition(inc_part))) {
      LOG_WARN("alter table add inc part failed", KR(ret), K(alter_table_schema), K(inc_part));
    } else if (OB_FAIL(alter_table_schema.get_part_option().assign(ori_part_option))) {
      LOG_WARN("alter table set part option failed", KR(ret), K(alter_table_schema), K(ori_part_option));
    } else {
      alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
      alter_table_schema.set_part_level(part_level);
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_rename_subpartition(const ParseNode &node,
                                                      const share::schema::ObTableSchema &orig_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  uint64_t tenant_data_version = 0;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (T_ALTER_SUBPARTITION_RENAME != node.type_
  || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret));
  } else if (OB_UNLIKELY(2 != node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree, num child != 2",KR(ret), K(node.num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", KR(ret), KP(this));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get data version failed", KR(ret), K(session_info_->get_effective_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version and feature mismatch", KR(ret));
  } else if (!orig_table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport behavior on not user table", KR(ret), K(orig_table_schema));
  } else if (PARTITION_LEVEL_ZERO == part_level) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_USER_ERROR(OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED);
    LOG_WARN("unsupport management on not partition table", KR(ret));
  } else if (PARTITION_LEVEL_ONE == part_level) {
    ret = OB_ERR_NOT_COMPOSITE_PARTITION;
    LOG_USER_ERROR(OB_ERR_NOT_COMPOSITE_PARTITION);
    LOG_WARN("unsupport management on not composite partition table", KR(ret));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", KR(ret));
  } else {
    AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    const ObPartition *part = nullptr;
    const ObSubPartition *subpart = nullptr;
    ObPartition inc_part;
    ObSubPartition inc_subpart;
    ObString origin_partition_name(static_cast<int32_t>(node.children_[0]->str_len_),
        node.children_[0]->str_value_);
    ObString new_partition_name(static_cast<int32_t>(node.children_[1]->str_len_),
        node.children_[1]->str_value_);
    const ObPartitionOption &ori_part_option = orig_table_schema.get_part_option();
    if (OB_UNLIKELY(ObCharset::case_insensitive_equal(origin_partition_name, new_partition_name))) {
      ret = OB_ERR_RENAME_SUBPARTITION_NAME_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_RENAME_SUBPARTITION_NAME_DUPLICATE, new_partition_name.length(), new_partition_name.ptr());
      LOG_WARN("origin subpart name equal to new subpart name", KR(ret), K(origin_partition_name), K(new_partition_name));
    } else if (OB_FAIL(orig_table_schema.check_partition_duplicate_with_name(new_partition_name))) {
      if (OB_DUPLICATE_OBJECT_NAME_EXIST == ret) {
        ret = OB_ERR_RENAME_SUBPARTITION_NAME_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_RENAME_SUBPARTITION_NAME_DUPLICATE, new_partition_name.length(), new_partition_name.ptr());
        LOG_WARN("new subpart name duplicate with existed partition", KR(ret), K(new_partition_name));
      } else {
        LOG_WARN("check new subpart name duplicate failed", KR(ret), K(new_partition_name));
      }
    } else if (OB_FAIL(orig_table_schema.get_subpartition_by_name(origin_partition_name, part, subpart))) {
      LOG_WARN("get part by name failed", KR(ret), K(origin_partition_name), K(orig_table_schema));
    } else if (OB_FAIL(inc_subpart.set_part_name(origin_partition_name))) {
      LOG_WARN("inc subpart set name failed", KR(ret), K(inc_subpart), K(origin_partition_name));
    } else if (OB_FAIL(inc_part.add_partition(inc_subpart))) {
      LOG_WARN("inc part add inc subpart failed", KR(ret), K(inc_part), K(inc_subpart));
    } else if (OB_FAIL(alter_table_schema.add_partition(inc_part))) {
      LOG_WARN("alter table add inc part failed", KR(ret), K(alter_table_schema), K(inc_part));
    } else if (OB_FAIL(alter_table_schema.set_new_part_name(new_partition_name))) {
      LOG_WARN("alter table schema set new part name failed", KR(ret), K(alter_table_schema), K(new_partition_name));
    } else if (OB_FAIL(alter_table_schema.get_part_option().assign(ori_part_option))) {
      LOG_WARN("alter table set part option failed", KR(ret), K(alter_table_schema), K(ori_part_option));
    } else {
      alter_table_schema.set_part_level(orig_table_schema.get_part_level());
      alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_ALTER != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode *index_node = node.children_[0];
    ParseNode *visibility_node = node.children_[1];
    if (OB_ISNULL(index_node) || T_IDENT != index_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid index node", KP(index_node), K(ret));
    } else if (OB_ISNULL(visibility_node) || (T_VISIBLE != visibility_node->type_
                                              && T_INVISIBLE != visibility_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid visibility node", KP(visibility_node), K(ret));
    } else {
      ObString alter_index_name;
      alter_index_name.assign_ptr(index_node->str_value_,
                                 static_cast<int32_t>(index_node->str_len_));
      //construct ObAlterIndexArg
      ObAlterIndexArg *alter_index_arg = NULL;
      void *tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexArg *)allocator_->alloc(sizeof(obrpc::ObAlterIndexArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_arg = new (tmp_ptr)ObAlterIndexArg();
        alter_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_arg->index_name_ = alter_index_name;
        alter_index_arg->index_visibility_ = T_VISIBLE == visibility_node->type_ ? 0 : 1 ;
      }
      //push drop index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (table_schema_->is_parent_table() || table_schema_->is_child_table()) {
          ObString index_table_name;
          ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
          const ObTableSchema *index_table_schema = NULL;
          if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                            table_schema_->get_table_id(),
                                                            alter_index_name,
                                                            index_table_name))) {
            LOG_WARN("build_index_table_name failed", K(table_schema_->get_table_id()), K(alter_index_name), K(ret));
          } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                               alter_table_stmt->get_org_database_name(),
                                                               index_table_name,
                                                               true /* index table */,
                                                               index_table_schema))) {
            if (OB_TABLE_NOT_EXIST == ret) {
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(alter_table_stmt->get_org_database_name()),
                              to_cstring(alter_table_stmt->get_org_table_name()));
            }
            LOG_WARN("fail to get index table schema", K(ret), K(index_table_name));
          } else if (OB_ISNULL(index_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is NULL", K(ret), K(index_table_name));
          } else if (lib::is_mysql_mode() && OB_FAIL(check_index_columns_equal_foreign_key(*table_schema_, *index_table_schema))) {
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

int ObAlterTableResolver::resolve_alter_index_parallel_oracle(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObString tmp_index_name;
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
  } else if (OB_FAIL(index_schema_->get_index_name(tmp_index_name))) {
    LOG_WARN("failed get index name", K(ret));
  } else if (OB_FAIL(deep_copy_str(tmp_index_name, index_name))) {
    LOG_WARN("failed to deep copy new_db_name", K(ret));
  } else {
    int64_t index_dop = node.children_[0]->value_;
    LOG_DEBUG("alter index table dop",
      K(ret), K(index_dop), K(index_name), K(index_schema_->get_table_name()));
    if (index_dop <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the value of table dop should greater than 0", K(ret));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "The value of table dop should greater than 0");
    } else {
      ObAlterIndexParallelArg *alter_index_parallel_arg = NULL;
      void *tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexParallelArg *)allocator_->alloc(sizeof(obrpc::ObAlterIndexParallelArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_parallel_arg = new (tmp_ptr)ObAlterIndexParallelArg();
        alter_index_parallel_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_parallel_arg->new_parallel_ = index_dop; // update以后的index dop
        alter_index_parallel_arg->index_name_ = index_name; // update的索引的name
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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

int ObAlterTableResolver::resolve_alter_index_parallel_mysql(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (!lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the mode is not mysql", K(ret));
  } else if (node.type_ != T_INDEX_ALTER_PARALLEL || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the type is not right or the children is null",
      K(ret), K(node.type_), K(node.children_==NULL));
  } else {
    ParseNode *mysql_index_name_node  = node.children_[0];
    ParseNode *parallel_node = node.children_[1];

    if (OB_ISNULL(mysql_index_name_node) || T_IDENT != mysql_index_name_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid mysql index name node", K(ret), KP(mysql_index_name_node));
    } else if (OB_ISNULL(parallel_node) || T_PARALLEL != parallel_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid mysql parallel node", K(ret), KP(parallel_node));
    } else if (OB_ISNULL(parallel_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parallel_node children node",
        K(ret), KP(parallel_node->children_));
    } else if (OB_ISNULL(parallel_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "child node is null", K(ret));
    } else if (parallel_node->children_[0]->value_ < 1) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "the parallel is invalid", K(ret), K(parallel_node->children_[0]->value_));
    } else {
      ObString index_name;
      index_name.assign_ptr(mysql_index_name_node->str_value_,
                            static_cast<int32_t>(mysql_index_name_node->str_len_));
      ObAlterIndexParallelArg *alter_index_parallel_arg = NULL;
      void *tmp_ptr = NULL;
      if (OB_UNLIKELY(NULL == (tmp_ptr = (ObAlterIndexParallelArg *)allocator_->alloc(sizeof(obrpc::ObAlterIndexParallelArg))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
      } else {
        alter_index_parallel_arg = new (tmp_ptr)ObAlterIndexParallelArg();
        alter_index_parallel_arg->tenant_id_ = session_info_->get_effective_tenant_id();
        alter_index_parallel_arg->new_parallel_ = parallel_node->children_[0]->value_;
        alter_index_parallel_arg->index_name_ = index_name; // update的索引的name
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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

int ObAlterTableResolver::resolve_rename_index(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_INDEX_RENAME != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode *index_node = node.children_[0];
    ParseNode *new_name_node = node.children_[1];
    if (lib::is_mysql_mode()
        && (OB_ISNULL(index_node)
            || T_IDENT != index_node->type_
            || OB_ISNULL(new_name_node) || T_IDENT != new_name_node ->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid index node", K(ret), KP(index_node));
    } else if (lib::is_oracle_mode()
               && (OB_ISNULL(index_schema_)
                   || index_schema_->get_table_name_str().empty()
                   || OB_ISNULL(new_name_node)
                   || T_IDENT != new_name_node ->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "index_schema_ is null or invalid index node", K(ret), KP(index_node), KPC(index_schema_));
    } else {
      // should check new index name length
      int32_t len = static_cast<int32_t>(new_name_node->str_len_);
      ObString tmp_new_index_name(len, len, new_name_node->str_value_);
      ObCollationType cs_type = CS_TYPE_INVALID;
      ObRenameIndexArg *rename_index_arg = NULL;
      if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation connection", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_index_name(cs_type, tmp_new_index_name))) {
        LOG_WARN("fail to check index name", K(tmp_new_index_name), K(ret));
      } else {
        ObString tmp_index_name;
        ObString ori_index_name;
        ObString new_index_name;
        if (lib::is_mysql_mode()) {
          ori_index_name.assign_ptr(index_node->str_value_,
                                    static_cast<int32_t>(index_node->str_len_));
        } else if (lib::is_oracle_mode()) {
          if (OB_FAIL(index_schema_->get_index_name(tmp_index_name))) {
            LOG_WARN("fail to get origin index name", K(ret));
          } else if (OB_FAIL(deep_copy_str(tmp_index_name, ori_index_name))) {
            LOG_WARN("failed to deep copy new_db_name", K(ret));
          }
        }
        new_index_name.assign_ptr(new_name_node->str_value_, static_cast<int32_t>(new_name_node->str_len_));
        void *tmp_ptr = NULL;

        if (OB_UNLIKELY(NULL == (tmp_ptr = (ObRenameIndexArg *)allocator_->alloc(sizeof(obrpc::ObRenameIndexArg))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
        } else {
          rename_index_arg = new (tmp_ptr)ObRenameIndexArg();
          rename_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
          rename_index_arg->origin_index_name_ = ori_index_name;
          rename_index_arg->new_index_name_= new_index_name;
        }
      }

      //push index arg
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(rename_index_arg))) {
          SQL_RESV_LOG(WARN, "add index to alter_index_list failed!", K(ret));
        }
      } //end for pushing index arg
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_primary(const ParseNode &action_node_list,
                                                const ParseNode &node)
{
  int ret = OB_SUCCESS;
  bool is_exist_add_pk = false;
  bool is_exist_drop_pk = false;
  bool is_exist_alter_pk = false;
  if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY, is_exist_add_pk))
      || OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_DROP, is_exist_drop_pk))) {
    SQL_RESV_LOG(WARN, "failed to check item type!", K(ret));
  } else if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_ALTER, is_exist_alter_pk))) {
    SQL_RESV_LOG(WARN, "failed to check item type!", K(ret));
  } else if (OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (lib::is_mysql_mode() && (is_exist_alter_pk)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to alter primary key using modify syntax under Mysql mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter primary using MODIFY PRIMARY KEY under Mysql mode");
  } else if (lib::is_mysql_mode() && (!is_exist_add_pk || !is_exist_drop_pk)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "Mysql mode, invalid parse tree!", K(ret));
  } else if (lib::is_oracle_mode() && (is_exist_drop_pk || is_exist_add_pk)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to modify primary key and other DDLs on primary key in single stmt is disallowed", K(ret), K(is_exist_drop_pk), K(is_exist_add_pk));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify primary and other DDLs on primary key in single stmt");
  } else if (lib::is_oracle_mode() && !is_exist_alter_pk) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "Oracle mode, invalid parse tree!", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "table_schema is null", K(ret));
  } else if (table_schema_->is_heap_table()) {
    const ObString pk_name = "PRIMAY";
    ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
    LOG_WARN("can't DROP 'PRIMARY', check primary key exists", K(ret), KPC(table_schema_));
    LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, pk_name.length(), pk_name.ptr());
  } else {
    obrpc::ObAlterPrimaryArg *alter_pk_arg = NULL;
    void *tmp_ptr = NULL;
    if (NULL == (tmp_ptr = (ObAlterPrimaryArg *)allocator_->alloc(sizeof(obrpc::ObAlterPrimaryArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
    } else {
      alter_pk_arg = new (tmp_ptr) ObAlterPrimaryArg();
      alter_pk_arg->set_index_action_type(ObIndexArg::ALTER_PRIMARY_KEY);
      ParseNode *column_list = node.children_[0];
      if (OB_ISNULL(column_list) || T_COLUMN_LIST != column_list->type_ ||
          OB_ISNULL(column_list->children_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
      }
      obrpc::ObColumnSortItem sort_item;
      const ObColumnSchemaV2 *col = NULL;
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
          if (OB_ISNULL(col = table_schema_->get_column_schema(sort_item.column_name_))) {
            ret = OB_ERR_KEY_DOES_NOT_EXISTS;
            SQL_RESV_LOG(WARN, "col is null", K(ret), "column_name", sort_item.column_name_);
            LOG_USER_ERROR(OB_ERR_KEY_DOES_NOT_EXISTS,
            sort_item.column_name_.length(), sort_item.column_name_.ptr(),
            table_schema_->get_table_name_str().length(), table_schema_->get_table_name_str().ptr());
          } else if (OB_FAIL(check_add_column_as_pk_allowed(*col))) {
            LOG_WARN("the column can not be primary key", K(ret));
          } else if (OB_FAIL(add_sort_column(sort_item, *alter_pk_arg))) {
            SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      alter_pk_arg->index_type_ = INDEX_TYPE_PRIMARY;
      alter_pk_arg->index_name_.assign_ptr(
      common::OB_PRIMARY_INDEX_NAME, static_cast<int32_t>(strlen(common::OB_PRIMARY_INDEX_NAME)));
      alter_pk_arg->tenant_id_ = session_info_->get_effective_tenant_id();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_pk_arg))) {
        SQL_RESV_LOG(WARN, "push back index arg failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_primary(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_PRIMARY_KEY != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "table_schema is null", K(ret));
  } else if (!table_schema_->is_heap_table()) {
    ret = OB_ERR_MULTIPLE_PRI_KEY;
    SQL_RESV_LOG(WARN, "multiple primary key defined", K(ret));
  } else {
    obrpc::ObCreateIndexArg *create_index_arg = NULL;
    void *tmp_ptr = NULL;
    if (NULL == (tmp_ptr = (ObCreateIndexArg *)allocator_->alloc(sizeof(obrpc::ObCreateIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
    } else {
      create_index_arg = new (tmp_ptr) ObCreateIndexArg();
      create_index_arg->set_index_action_type(ObIndexArg::ADD_PRIMARY_KEY);
    }
    ParseNode *column_list = node.children_[0];
    if (OB_ISNULL(column_list) || T_COLUMN_LIST != column_list->type_ ||
        OB_ISNULL(column_list->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    }
    obrpc::ObColumnSortItem sort_item;
    const ObColumnSchemaV2 *col = NULL;
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
        if (OB_ISNULL(col = table_schema_->get_column_schema(sort_item.column_name_))) {
          ret = OB_ERR_KEY_DOES_NOT_EXISTS;
          SQL_RESV_LOG(WARN, "col is null", K(ret), "column_name", sort_item.column_name_);
          LOG_USER_ERROR(OB_ERR_KEY_DOES_NOT_EXISTS,
          sort_item.column_name_.length(), sort_item.column_name_.ptr(),
          table_schema_->get_table_name_str().length(), table_schema_->get_table_name_str().ptr());
        } else if (OB_FAIL(check_add_column_as_pk_allowed(*col))) {
          LOG_WARN("the column can not be primary key", K(ret));
        } else if (OB_FAIL(add_sort_column(sort_item, *create_index_arg))) {
          SQL_RESV_LOG(WARN, "failed to add sort column to index arg", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      create_index_arg->index_type_ = INDEX_TYPE_PRIMARY;
      if (lib::is_oracle_mode()) {
        if (node.num_child_ == 2 && OB_NOT_NULL(node.children_[1])
            && node.children_[1]->str_len_ != 0) {
          create_index_arg->index_name_.assign_ptr(node.children_[1]->str_value_,
                                                   static_cast<int32_t>(node.children_[1]->str_len_));
          create_index_arg->index_schema_.set_name_generated_type(GENERATED_TYPE_USER);
        }
      } else {
        create_index_arg->index_name_.assign_ptr(common::OB_PRIMARY_INDEX_NAME,
                                                 static_cast<int32_t>(strlen(common::OB_PRIMARY_INDEX_NAME)));
      }
      create_index_arg->tenant_id_ = session_info_->get_effective_tenant_id();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_index_arg(create_index_arg))) {
        SQL_RESV_LOG(WARN, "push back index arg failed", K(ret));
      }
    }
  }
  return ret;
}

// to check whether to drop primary key only.
// to check whether to drop primary key legally.
int ObAlterTableResolver::check_is_drop_primary_key(const ParseNode &node,
                                                    bool &is_drop_primary_key)
{
  int ret = OB_SUCCESS;
  is_drop_primary_key = false;
  if (lib::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode *action_node = node.children_[i];
      if (OB_ISNULL(action_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree!", K(ret));
      } else if (T_PRIMARY_KEY_DROP != action_node->children_[0]->type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported to drop primary key and other DDLs in single stmt", K(ret), K(action_node->children_[0]->type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Drop primary and other DDLs in single stmt");
      }
    }
    if (OB_SUCC(ret)) {
      is_drop_primary_key = true;
    }
  } else {
    // Not supported is reported when,
    // there are multiple add pk nodes and at least one drop pk node,
    // or there are at least one other action node and at least one drop pk node,
    // or there are multiple drop pk nodes.
    int64_t drop_pk_node_cnt = 0;
    int64_t add_pk_node_cnt = 0;
    int64_t other_action_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      ParseNode *action_node = node.children_[i];
      if (OB_ISNULL(action_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree!", K(ret));
      } else if (T_PRIMARY_KEY == action_node->children_[0]->type_) {
        add_pk_node_cnt++;
      } else if (T_PRIMARY_KEY_DROP == action_node->children_[0]->type_) {
        drop_pk_node_cnt++;
      } else {
        other_action_cnt++;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (drop_pk_node_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, there is no drop primary key node",
        K(ret), K(drop_pk_node_cnt), K(add_pk_node_cnt), K(other_action_cnt));
    } else if (drop_pk_node_cnt == 1 && add_pk_node_cnt == 1 && other_action_cnt == 0) {
      // is modify primary key operation.
      is_drop_primary_key = false;
    } else if (drop_pk_node_cnt == 1 && add_pk_node_cnt == 0 && other_action_cnt == 0) {
      // is drop primary key operation.
      is_drop_primary_key = true;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Multiple complex DDLs about primary key in single stmt is not supported now",
        K(ret), K(drop_pk_node_cnt), K(add_pk_node_cnt), K(other_action_cnt));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Multiple complex DDLs about primary in single stmt");
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_drop_primary(const ParseNode &action_node_list)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = nullptr;
  bool is_exist_drop_pk = false;
  bool is_drop_primary_key = false;
  ObAlterPrimaryArg *drop_pk_arg = nullptr;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret));
  } else if (table_schema_->is_heap_table()) {
    const ObString pk_name = "PRIMAY";
    ret = OB_ERR_CANT_DROP_FIELD_OR_KEY;
    LOG_WARN("can't DROP 'PRIMARY', check primary key exists", K(ret), KPC(table_schema_));
    LOG_USER_ERROR(OB_ERR_CANT_DROP_FIELD_OR_KEY, pk_name.length(), pk_name.ptr());
  } else if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_DROP, is_exist_drop_pk))) {
    LOG_WARN("failed to check item type", K(ret));
  } else if (!is_exist_drop_pk) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, invalid parser tree", K(ret));
  } else if (OB_FAIL(check_is_drop_primary_key(action_node_list, is_drop_primary_key))) {
    LOG_WARN("fail to check whether to drop primary key only", K(ret));
  } else if (!is_drop_primary_key) {
    // modify primary key, thus skip to add drop primary key arg.
  } else if (OB_ISNULL(tmp_ptr = (ObAlterPrimaryArg *)allocator_->alloc(sizeof(ObAlterPrimaryArg)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    drop_pk_arg = new (tmp_ptr) ObAlterPrimaryArg();
    drop_pk_arg->set_index_action_type(ObIndexArg::DROP_PRIMARY_KEY);
    drop_pk_arg->index_type_ = INDEX_TYPE_PRIMARY;
    drop_pk_arg->tenant_id_ = session_info_->get_effective_tenant_id();
    if (OB_FAIL(alter_table_stmt->add_index_arg(drop_pk_arg))) {
      LOG_WARN("failed to add index arg", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != drop_pk_arg) {
      drop_pk_arg->~ObAlterPrimaryArg();
      drop_pk_arg = nullptr;
    }
    if (nullptr != tmp_ptr) {
      allocator_->free(tmp_ptr);
      tmp_ptr = nullptr;
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_index_tablespace_oracle(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObString tmp_index_name;
  ObString index_name;
  if (T_TABLESPACE != node.type_|| OB_ISNULL(node.children_[0]) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (OB_ISNULL(index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the index schema is null", K(ret));
  } else if (OB_FAIL(index_schema_->get_index_name(tmp_index_name))) {
    LOG_WARN("failed to get index name", K(ret));
  } else if (OB_FAIL(deep_copy_str(tmp_index_name, index_name))) {
    LOG_WARN("failed to deep copy new_db_name", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    const ObTablespaceSchema *tablespace_schema = NULL;
    const ParseNode *tablespace_node = node.children_[0];
    const ObString tablespace_name(tablespace_node->str_len_, tablespace_node->str_value_);
    ObAlterIndexTablespaceArg *alter_index_tablespace_arg = NULL;
    void *tmp_ptr = NULL;
    if (OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker ptr is null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_tablespace_schema(tenant_id, tablespace_name, tablespace_schema))) {
      LOG_WARN("fail to get tablespace schema", K(ret), K(tablespace_name));
    } else if (OB_ISNULL(tablespace_schema)) {
      ret = OB_TABLESPACE_NOT_EXIST;
      LOG_WARN("tablespace schema is not exist", K(ret), K(tenant_id), K(tablespace_name));
    } else if (OB_ISNULL(tmp_ptr = (ObAlterIndexTablespaceArg *)allocator_->alloc(sizeof(obrpc::ObAlterIndexTablespaceArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      alter_index_tablespace_arg = new (tmp_ptr)ObAlterIndexTablespaceArg();
      alter_index_tablespace_arg->tenant_id_ = tenant_id;
      alter_index_tablespace_arg->tablespace_id_ = tablespace_schema->get_tablespace_id();
      if (OB_FAIL(ob_write_string(*allocator_, tablespace_schema->get_encryption_name(),
                                  alter_index_tablespace_arg->encryption_))) {
        LOG_WARN("deep copy tablespace encryption name failed", K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator_, index_name,
                                         alter_index_tablespace_arg->index_name_))) {
        LOG_WARN("failed to deep copy index name", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter table stmt should not be null", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_tablespace_arg))) {
        LOG_WARN("add index to alter_index_list failed!", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_index_options_oracle(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_INDEX_OPTION_ORACLE != node.type_ ||
                  node.num_child_ <= 0 ||
                  OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    switch(node.children_[0]->type_) {
    case T_INDEX_RENAME: {
        ParseNode *index_node = node.children_[0];
        if (OB_FAIL(resolve_rename_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve rename index error!", K(ret));
        }
        break;
      }
    case T_PARALLEL: {
      // alter index parallel for oracle
      ParseNode *index_node = node.children_[0];
      if (OB_FAIL(resolve_alter_index_parallel_oracle(*index_node))) {
          SQL_RESV_LOG(WARN, "resolve alter index parallel error!", K(ret));
      }
      break;
    }
    case T_TABLESPACE: {
      ParseNode *index_node = node.children_[0];
      if (OB_FAIL(resolve_alter_index_tablespace_oracle(*index_node))) {
          SQL_RESV_LOG(WARN, "resolve alter index tablespace error!", K(ret));
      }
      break;
    }
    default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Unknown index option type!",
                     "option type", node.children_[0]->type_, K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unknown index option type");
        break;
      }
    }
  }
  return ret;
}

// 这里不只处理 index，还会处理 oracle 模式下 alter table 时追加约束
int ObAlterTableResolver::resolve_index_options(const ParseNode &action_node_list,
                                                const ParseNode &node,
                                                bool &is_add_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_INDEX_OPTION != node.type_ ||
                  node.num_child_ <= 0 ||
                  OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    switch(node.children_[0]->type_) {
    case T_INDEX_ADD: {
        ParseNode *index_node = node.children_[0];
        if (is_mysql_mode()) {
          is_add_index = true;
        } else if (OB_FAIL(resolve_add_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve add index error!", K(ret));
        }
        break;
      }
    case T_INDEX_DROP: {
        ParseNode *index_node = node.children_[0];
        if (OB_FAIL(resolve_drop_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve drop index error!", K(ret));
        }
        break;
      }
    case T_PRIMARY_KEY:
    case T_PRIMARY_KEY_ALTER: {
        ParseNode *primary_key_node = node.children_[0];
        bool is_exist_drop = false;
        bool is_exist_alter = false;
        if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_DROP, is_exist_drop))) {
          SQL_RESV_LOG(WARN, "failed to check item type!", K(ret));
        } else if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_ALTER, is_exist_alter))) {
          SQL_RESV_LOG(WARN, "failed to check item type!", K(ret));
        } else if (!is_exist_drop && !is_exist_alter) {
          if (OB_FAIL(resolve_add_primary(*primary_key_node))) {
            SQL_RESV_LOG(WARN, "failed to resovle primary key!", K(ret));
          }
        } else {
          if (OB_FAIL(resolve_alter_primary(action_node_list, *primary_key_node))) {
            SQL_RESV_LOG(WARN, "failed to resovle alter primary key!", K(ret));
          }
        }
        break;
      }
    case T_PRIMARY_KEY_DROP: {
        bool is_exist_drop = false;
        ParseNode *primary_key_node = node.children_[0];
        if (OB_FAIL(is_exist_item_type(action_node_list, T_PRIMARY_KEY_DROP, is_exist_drop))) {
          SQL_RESV_LOG(WARN, "failed to check item type!", K(ret));
        } else if (!is_exist_drop) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "4.0 support to drop primary key", K(ret));
        } else if (OB_FAIL(resolve_drop_primary(action_node_list))) {
          SQL_RESV_LOG(WARN, "failed to resolve drop primary key", K(ret));
        }
        break;
      }
    case T_INDEX_ALTER: {
        ParseNode *index_node = node.children_[0];
        if (OB_FAIL(resolve_alter_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve alter index error!", K(ret));
        }
        break;
      }
    case T_INDEX_RENAME: {
        ParseNode *index_node = node.children_[0];
        if (OB_FAIL(resolve_rename_index(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve rename index error!", K(ret));
        }
        break;
      }
    case T_INDEX_ALTER_PARALLEL: {
      // alter index parallel for mysql
      ParseNode *index_node = node.children_[0];
      if (OB_FAIL(resolve_alter_index_parallel_mysql(*index_node))) {
          SQL_RESV_LOG(WARN, "Resolve alter index parallel error!", K(ret));
      }
      break;
    }
    case T_CHECK_CONSTRAINT: {
      const ParseNode *check_cst_node = node.children_[0];
      if (OB_FAIL(resolve_constraint_options(*check_cst_node, action_node_list.num_child_ > 1))) {
        SQL_RESV_LOG(WARN, "Resolve check constraint option failed!", K(ret));
      }
      break;
    }
    case T_FOREIGN_KEY: {
      ObCreateForeignKeyArg foreign_key_arg;
      ParseNode *foreign_key_action_node = node.children_[0];
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
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
      ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
      ParseNode *cons_state_node = node.children_[0];
      ParseNode *constraint_name_node = NULL;
      uint64_t constraint_id = OB_INVALID_ID;
      ObString constraint_name;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
      } else if (OB_ISNULL(constraint_name_node = cons_state_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "constraint_name_node is null", K(ret));
      } else {
        constraint_name.assign_ptr(constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
      }
      if (OB_FAIL(ret)) {
      } else if (lib::is_mysql_mode()) {
        if (cons_state_node->value_ == 0) { // 0 : alter state of a check constraint or foreign key
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
            // check if check constraint exist
            bool is_constraint = false;
            if (!table_schema_->is_mysql_tmp_table()
                && OB_FAIL(schema_guard->get_constraint_id(table_schema_->get_tenant_id(),
                           table_schema_->get_database_id(),
                           constraint_name,
                           constraint_id))) {
              LOG_WARN("get constraint id failed", K(ret),
                        K(table_schema_->get_tenant_id()),
                        K(table_schema_->get_database_id()),
                        K(constraint_name));
            } else if (OB_INVALID_ID != constraint_id) {
              is_constraint = true;
            } else if (table_schema_->is_mysql_tmp_table()) {
              ObTableSchema::const_constraint_iterator iter = table_schema_->constraint_begin();
              for (; OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
                if (0 == constraint_name.case_compare((*iter)->get_constraint_name_str())) {
                  is_constraint = true;
                  break;
                }
              }
            } else {
              // check cst not exist
            }
            if (is_constraint) {
              // alter constraint, check foreign key with same name
              ret = OB_ERR_MULTIPLE_CONSTRAINTS_WITH_SAME_NAME;
              ObString action("alter");
              LOG_USER_ERROR(OB_ERR_MULTIPLE_CONSTRAINTS_WITH_SAME_NAME,
                            constraint_name.length(), constraint_name.ptr(),
                            action.length(), action.ptr());
            } else {
              ret = OB_ERR_ALTER_CONSTRAINT_ENFORCEMENT_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_ALTER_CONSTRAINT_ENFORCEMENT_NOT_SUPPORTED, constraint_name.length(), constraint_name.ptr());
              LOG_WARN("alter foreign key is not supported in mysql mode", K(ret), K(constraint_name));
            }
          }
        } else {
          // alter check, do nothing
          // cons_state_node->value_ == 1
          // 1 : alter state of a check constraint
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_modify_check_constraint_state_mysql(cons_state_node))) { // alter check constraint state
            LOG_WARN("modify check constraint state failed", K(ret));
          }
        }
      } else { // oracle mode
        if(OB_FAIL(schema_guard->get_foreign_key_id(table_schema_->get_tenant_id(),
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
            if (OB_FAIL(resolve_modify_check_constraint_state_oracle(cons_state_node))) {
              LOG_WARN("modify check constraint state failed", K(ret));
            }
          } else {  // OB_INVALID_ID == constraint_id
            const ObSimpleTableSchemaV2* simple_table_schema = nullptr;
            ObString unique_index_name_with_prefix;
            if (OB_FAIL(ObTableSchema::build_index_table_name(*allocator_,
                        table_schema_->get_table_id(),
                        constraint_name,
                        unique_index_name_with_prefix))) {
              LOG_WARN("build_index_table_name failed", K(ret), K(table_schema_->get_table_id()), K(constraint_name));
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
              ret = OB_NOT_SUPPORTED;
              SQL_RESV_LOG(WARN, "modify unique constraint is not supported", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify unique constraint");
            } else {
              ret = OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT;
              SQL_RESV_LOG(WARN,
                  "Cannot modify constraint - nonexistent constraint",
                  K(ret),
                  K(constraint_name),
                  K(table_schema_->get_table_name_str()));
              LOG_USER_ERROR(OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT, constraint_name.length(), constraint_name.ptr());
            }
          }
        }
      }
      break;
    }
    default: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Unknown index option type!",
                     "option type", node.children_[0]->type_, K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unknown index option type");
        break;
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_modify_check_constraint_state_mysql(const ParseNode* node) {
  int ret = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  ParseNode* constraint_state = NULL;
  ParseNode* constraint_name_node = NULL;
  ObConstraint cst;
  ObString cst_name;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "enable_cons_node is null", K(ret));
  } else if ((T_MODIFY_CONSTRAINT_OPTION != node->type_ && T_CHECK_CONSTRAINT != node->type_) ||
             2 != node->num_child_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
  } else if (OB_ISNULL(constraint_name_node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "constraint_name_node is null", K(ret));
  } else {
    cst_name.assign_ptr(constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
    constraint_state = node->children_[1];
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
      if (node->value_ == 0) {
        ret = OB_ERR_NONEXISTENT_CONSTRAINT;
        LOG_USER_ERROR(OB_ERR_NONEXISTENT_CONSTRAINT, cst_name.length(), cst_name.ptr());
      } else {
        ret = OB_ERR_CHECK_CONSTRAINT_NOT_FOUND;
        LOG_USER_ERROR(OB_ERR_CHECK_CONSTRAINT_NOT_FOUND, cst_name.length(), cst_name.ptr());
      }
      SQL_RESV_LOG(
          WARN, "can't find check constraint in table", K(ret), K(cst_name), K(table_schema_->get_table_name_str()));
    } else if (OB_UNLIKELY(OB_ISNULL(constraint_state))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "constraint state option is null", K(ret));
    } else if (T_ENFORCED_CONSTRAINT == constraint_state->type_ && !cst.get_enable_flag()) {
      cst.set_is_modify_enable_flag(true);
      cst.set_enable_flag(true);
      cst.set_is_modify_validate_flag(true);
      cst.set_validate_flag(CST_FK_VALIDATED);
    } else if (T_NOENFORCED_CONSTRAINT == constraint_state->type_ && cst.get_enable_flag()) {
      cst.set_is_modify_enable_flag(true);
      cst.set_enable_flag(false);
      cst.set_is_modify_validate_flag(true);
      cst.set_validate_flag(CST_FK_NO_VALIDATE);
    }
    if (OB_SUCC(ret)) {
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

int ObAlterTableResolver::check_dup_foreign_keys_exist(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const obrpc::ObCreateForeignKeyArg &foreign_key_arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> child_column_ids;
  ObSEArray<uint64_t, 4> parent_column_ids;
  const ObTableSchema *parent_table_schema = NULL;


  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             table_schema_->get_tenant_id(),
             foreign_key_arg.parent_database_, foreign_key_arg.parent_table_, false, parent_table_schema))) {
    SQL_RESV_LOG(WARN, "get table schema failed", K(ret));
  } else if (OB_ISNULL(parent_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "parent_table_schema is null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < foreign_key_arg.child_columns_.count(); ++i) {
      const ObColumnSchemaV2 *child_column_schema = table_schema_->get_column_schema(foreign_key_arg.child_columns_.at(i));
      const ObColumnSchemaV2 *parent_column_schema = parent_table_schema->get_column_schema(foreign_key_arg.parent_columns_.at(i));
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
    if (OB_SUCC(ret)
        && OB_FAIL(ObResolverUtils::check_dup_foreign_keys_exist(
                   table_schema_->get_foreign_key_infos(),
                   child_column_ids, parent_column_ids,
                   parent_table_schema->get_table_id(),
                   table_schema_->get_table_id()))) {
      SQL_RESV_LOG(WARN, "check dup foreign keys exist failed", K(ret));
    }
  }

  return ret;
}

int ObAlterTableResolver::resolve_modify_foreign_key_state(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ParseNode *constraint_name_node = NULL;
  ParseNode *rely_option_node = NULL;
  ParseNode *enable_option_node = NULL;
  ParseNode *validate_option_node = NULL;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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
  } else if (OB_ISNULL(rely_option_node)
             && OB_ISNULL(enable_option_node)
             && OB_ISNULL(validate_option_node)) {
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_RESV_LOG(WARN, "all options are null", K(ret));
  } else {
    foreign_key_arg.is_modify_fk_state_ = true;
    foreign_key_arg.foreign_key_name_.assign_ptr(constraint_name_node->str_value_, static_cast<int32_t>(constraint_name_node->str_len_));
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema_->get_foreign_key_infos();
    const ObForeignKeyInfo *foreign_key_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
      if (0 == foreign_key_arg.foreign_key_name_.case_compare(foreign_key_infos.at(i).foreign_key_name_)
          && table_schema_->get_table_id() == foreign_key_infos.at(i).child_table_id_) {
        foreign_key_info = &foreign_key_infos.at(i);
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(foreign_key_info)) {
      ret = OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT;
      SQL_RESV_LOG(WARN, "Cannot modify constraint - nonexistent constraint", K(ret), K(foreign_key_arg.foreign_key_name_), K(table_schema_->get_table_name_str()));
      LOG_USER_ERROR(OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT, static_cast<int32_t>(constraint_name_node->str_len_), constraint_name_node->str_value_);
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info is null", KR(ret));
    } else {
      const uint64_t tenant_id = session_info_->get_effective_tenant_id();
      ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
      const ObDatabaseSchema *parent_db_schema = NULL;
      const ObTableSchema *parent_table_schema = NULL;
      foreign_key_arg.name_generated_type_ = foreign_key_info->name_generated_type_;
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "schema_guard is null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(
                 tenant_id, foreign_key_info->parent_table_id_, parent_table_schema))) {
        SQL_RESV_LOG(WARN, "get parent_table_schema failed", K(ret), K(foreign_key_info->parent_table_id_));
      } else if (OB_ISNULL(parent_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "parent_table_schema is null", K(ret), K(foreign_key_info->parent_table_id_));
      } else if (OB_FAIL(schema_guard->get_database_schema(tenant_id,
                 parent_table_schema->get_database_id(), parent_db_schema))) {
        SQL_RESV_LOG(WARN, "get parent_database_schema failed", K(ret), K(parent_table_schema->get_database_id()));
      } else if (OB_ISNULL(parent_db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "parent_db_schema is null", K(ret), K(parent_table_schema->get_database_id()));
      } else {
        const ObColumnSchemaV2 *child_col = NULL;
        const ObColumnSchemaV2 *parent_col = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_info->child_column_ids_.count(); ++i) {
          ObString tmp_child_column_name;
          ObString tmp_parent_column_name;
          if (OB_ISNULL(child_col = table_schema_->get_column_schema(foreign_key_info->child_column_ids_.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "child column schema not exists", K(ret), K(foreign_key_info->child_column_ids_.at(i)));
          } else if (OB_ISNULL(parent_col = parent_table_schema->get_column_schema(foreign_key_info->parent_column_ids_.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "parent column schema not exists", K(ret), K(foreign_key_info->parent_column_ids_.at(i)));
          } else if (OB_FAIL(ob_write_string(*allocator_, child_col->get_column_name_str(), tmp_child_column_name))) {
            SQL_RESV_LOG(WARN, "deep copy parent database name failed", K(ret), K(child_col->get_column_name_str()));
          } else if (OB_FAIL(ob_write_string(*allocator_, parent_col->get_column_name_str(), tmp_parent_column_name))) {
            SQL_RESV_LOG(WARN, "deep copy parent table name failed", K(ret), K(parent_col->get_column_name_str()));
          } else if (OB_FAIL(foreign_key_arg.child_columns_.push_back(tmp_child_column_name))) {
            SQL_RESV_LOG(WARN, "push back failed", K(ret), K(tmp_child_column_name));
          } else if (OB_FAIL(foreign_key_arg.parent_columns_.push_back(tmp_parent_column_name))) {
            SQL_RESV_LOG(WARN, "push back failed", K(ret), K(tmp_parent_column_name));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(*allocator_, parent_db_schema->get_database_name_str(), foreign_key_arg.parent_database_))) {
          SQL_RESV_LOG(WARN, "deep copy parent database name failed", K(ret), K(parent_db_schema->get_database_name_str()));
        } else if (OB_FAIL(ob_write_string(*allocator_, parent_table_schema->get_table_name_str(), foreign_key_arg.parent_table_))) {
          SQL_RESV_LOG(WARN, "deep copy parent table name failed", K(ret), K(parent_table_schema->get_table_name_str()));
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
            foreign_key_arg.validate_flag_ = CST_FK_VALIDATED;
          } else if (T_NOVALIDATE_CONSTRAINT == validate_option_node->type_ && foreign_key_info->validate_flag_) {
            foreign_key_arg.is_modify_validate_flag_ = true;
            foreign_key_arg.validate_flag_ = CST_FK_NO_VALIDATE;
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
              foreign_key_arg.validate_flag_ = CST_FK_VALIDATED;
            }
          } else if (T_DISABLE_CONSTRAINT == enable_option_node->type_) {
            if (foreign_key_info->enable_flag_) {
              foreign_key_arg.is_modify_enable_flag_ = true;
              foreign_key_arg.enable_flag_ = false;
            }
            if (OB_ISNULL(validate_option_node) && foreign_key_info->validate_flag_) {
              foreign_key_arg.is_modify_validate_flag_ = true;
              foreign_key_arg.validate_flag_ = CST_FK_NO_VALIDATE;
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

int ObAlterTableResolver::resolve_modify_check_constraint_state_oracle(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ParseNode *constraint_name_node = NULL;
  ParseNode *rely_option_node = NULL;
  ParseNode *enable_option_node = NULL;
  ParseNode *validate_option_node = NULL;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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
    for (;OB_SUCC(ret) && iter != table_schema_->constraint_end(); ++iter) {
      if (0 == cst_name.case_compare((*iter)->get_constraint_name_str())) {
        if (OB_FAIL(cst.assign(**iter))) {
          SQL_RESV_LOG(WARN, "Fail to assign constraint", K(ret));
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (table_schema_->constraint_end() == iter) {
      ret = OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT;
      SQL_RESV_LOG(WARN,
          "Cannot modify constraint - nonexistent constraint",
          K(ret),
          K(cst_name),
          K(table_schema_->get_table_name_str()));
      LOG_USER_ERROR(OB_ERR_MODIFY_NONEXISTENT_CONSTRAINT, cst_name.length(), cst_name.ptr());
    } else if (OB_ISNULL(rely_option_node) && OB_ISNULL(enable_option_node) && OB_ISNULL(validate_option_node)) {
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "all options are null", K(ret));
    }
    if (OB_SUCC(ret)) {
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
        if (T_VALIDATE_CONSTRAINT == validate_option_node->type_ && cst.is_no_validate()) {
          cst.set_is_modify_validate_flag(true);
          cst.set_validate_flag(CST_FK_VALIDATED);
        } else if (T_NOVALIDATE_CONSTRAINT == validate_option_node->type_ && cst.is_validated()) {
          cst.set_is_modify_validate_flag(true);
          cst.set_validate_flag(CST_FK_NO_VALIDATE);
        }
      }
      if (OB_NOT_NULL(enable_option_node)) {
        if (T_ENABLE_CONSTRAINT == enable_option_node->type_) {
          if (!cst.get_enable_flag()) {
            cst.set_is_modify_enable_flag(true);
            cst.set_enable_flag(true);
          }
          if (OB_ISNULL(validate_option_node) && cst.is_no_validate()) {
            cst.set_is_modify_validate_flag(true);
            cst.set_validate_flag(CST_FK_VALIDATED);
          }
        } else if (T_DISABLE_CONSTRAINT == enable_option_node->type_) {
          if (cst.get_enable_flag()) {
            cst.set_is_modify_enable_flag(true);
            cst.set_enable_flag(false);
          }
          if (OB_ISNULL(validate_option_node) && cst.is_validated()) {
            cst.set_is_modify_validate_flag(true);
            cst.set_validate_flag(CST_FK_NO_VALIDATE);
          }
        }
      }
      if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
        AlterTableSchema &alter_table_schema =
          alter_table_stmt->get_alter_table_arg().alter_table_schema_;
        ObColumnSchemaV2 *col_schema = NULL;
        if (1 != (*iter)->get_column_cnt()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column count of not null constraint", K(ret), KPC(*iter));
        } else if (NULL == (col_schema = alter_table_schema.get_column_schema(*((*iter)->cst_col_begin())))) {
          AlterColumnSchema alter_column_schema;
          const ObColumnSchemaV2 *origin_col_schema = NULL;
          if (OB_FAIL(schema_checker_->get_column_schema(table_schema_->get_tenant_id(), table_schema_->get_table_id(),
                                                         *cst.cst_col_begin(), origin_col_schema, false))) {
            LOG_WARN("get not null origin col schema failed", K(ret));
          } else if (OB_UNLIKELY(origin_col_schema->is_identity_column())) {
            ret = origin_col_schema->is_default_on_null_identity_column()
                 ? OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_COLUMN
                  : OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
            LOG_WARN("can't modify not null constraint on an identity column", K(ret));
          } else if (OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
            LOG_WARN("copy column schema failed", K(ret));
          } else if (OB_FAIL(alter_column_schema.set_origin_column_name(
                        alter_column_schema.get_column_name_str()))) {
            LOG_WARN("set origin column name faield", K(ret));
          } else {
            // need to add alter_column_schema when modify not null constraint,
            // in order to modify column_flags.
            alter_column_schema.add_not_null_cst(cst.get_rely_flag(), cst.get_enable_flag(),
                                                 cst.is_validated());
            alter_column_schema.alter_type_ = OB_DDL_MODIFY_COLUMN;
            alter_table_stmt->set_alter_table_column();
            if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
              LOG_WARN("add alter column schema failed", K(ret));
            }
            LOG_DEBUG("modify not null constraint", KPC(origin_col_schema), K(alter_column_schema));
          }
        } else if (OB_UNLIKELY(col_schema->is_identity_column())) {
          ret = col_schema->is_default_on_null_identity_column()
                ? OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_COLUMN
                : OB_ERR_CANNOT_MODIFY_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
          LOG_WARN("can't modify not null constraint on an identity column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(alter_table_stmt->get_alter_table_arg().alter_table_schema_.add_constraint(cst))) {
        SQL_RESV_LOG(WARN, "add constraint failed", K(ret));
      } else {
        alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::ALTER_CONSTRAINT_STATE;
        ++add_or_modify_check_cst_times_;
      }
    }
  }

  return ret;
}

// 用于解析 add/drop check constraint
int ObAlterTableResolver::resolve_constraint_options(const ParseNode &node, const bool is_multi_actions)
{
  int ret = OB_SUCCESS;
  if ((lib::is_mysql_mode() && ((T_ALTER_CHECK_CONSTRAINT_OPTION != node.type_ && T_DROP_CONSTRAINT != node.type_) || OB_ISNULL(node.children_)))
      || (lib::is_oracle_mode() && (((T_DROP_CONSTRAINT != node.type_) && (T_CHECK_CONSTRAINT != node.type_)) || OB_ISNULL(node.children_)))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      const ParseNode *constraint_node = NULL;
      if (lib::is_mysql_mode() && T_ALTER_CHECK_CONSTRAINT_OPTION == node.type_) {
        constraint_node = node.children_[0];
      } else {
        constraint_node = &node;
      }
      if (OB_ISNULL(constraint_node)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "constraint_node is null", K(ret), K(constraint_node));
      } else {
        if (T_DROP_CONSTRAINT == constraint_node->type_ || (T_CHECK_CONSTRAINT == constraint_node->type_ && 0 == constraint_node->value_)) {
          if (OB_FAIL(resolve_drop_constraint(*constraint_node))) {
            SQL_RESV_LOG(WARN, "Resolve drop constraint error!", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::DROP_CONSTRAINT;
          }
        } else if (1 == constraint_node->value_) {
          if (is_multi_actions) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("add/modify constraint together with other ddls is not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify constraint together with other DDLs");
          } else if (OB_FAIL(resolve_add_constraint(*constraint_node))) {
            SQL_RESV_LOG(WARN, "Resolve add constraint error!", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::ADD_CONSTRAINT;
            ++add_or_modify_check_cst_times_;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "Unknown alter constraint option!", "option type", node.children_[0]->value_, K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_partition_options(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_PARTITION_OPTION != node.type_ ||
                  node.num_child_ <= 0 ||
                  OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else if (table_schema_->is_materialized_view()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("alter partition of materialized view is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter partition of materialized view is");
    }

    if (OB_SUCC(ret)) {
      const ObPartitionLevel part_level = table_schema_->get_part_level();
      if (T_ALTER_PARTITION_PARTITIONED != node.children_[0]->type_
          && PARTITION_LEVEL_ZERO == part_level) {
        ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
        LOG_WARN("unsupport add/drop management on non-partition table", K(ret));

      } else if (T_ALTER_PARTITION_PARTITIONED == node.children_[0]->type_
                 && PARTITION_LEVEL_ZERO != part_level
                 && lib::is_oracle_mode()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't re-partitioned a partitioned table", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Re-partition a patitioned table");
      }
    }
    if (OB_SUCC(ret)) {
      ParseNode *partition_node = node.children_[0];
      // if (T_ALTER_PARTITION_PARTITIONED != node.children_[0]->type_
      //     && T_ALTER_PARTITION_TRUNCATE != node.children_[0]->type_
      //     && !table_schema_->is_range_part()
      //     && !table_schema_->is_list_part()) {
      //   ret = OB_NOT_SUPPORTED;
      //   SQL_RESV_LOG(WARN, "just support add/drop partition for range part", K(ret));
      // }
      switch(partition_node->type_) {
        case T_ALTER_PARTITION_ADD: {
          if (OB_FAIL(resolve_add_partition(*partition_node, *table_schema_))) {
            SQL_RESV_LOG(WARN, "Resolve add partition error!", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ =
                ObAlterTableArg::ADD_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_ADD: {
          if (OB_FAIL(resolve_add_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve add subpartition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ =
                  ObAlterTableArg::ADD_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_RENAME:{
          if (OB_FAIL(resolve_rename_partition(*partition_node, *table_schema_))) {
            LOG_WARN("Resolve rename partition error!", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_=ObAlterTableArg::RENAME_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_RENAME:{
          if (OB_FAIL(resolve_rename_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("Resolve rename subpartition error!", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_=ObAlterTableArg::RENAME_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_DROP: {
          if (OB_FAIL(resolve_drop_partition(*partition_node, *table_schema_))) {
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
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::DROP_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_PARTITIONED: {
          bool enable_split_partition = false;
          const ObPartitionLevel part_level = table_schema_->get_part_level();
          alter_table_stmt->get_alter_table_arg().alter_part_type_ =
            ObAlterTableArg::REPARTITION_TABLE;
          if (OB_FAIL(get_enable_split_partition(session_info_->get_effective_tenant_id(),
                  enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config", K(ret),
                "tenant_id", session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition
                    && ObAlterTableArg::PARTITIONED_TABLE ==
                    alter_table_stmt->get_alter_table_arg().alter_part_type_) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("partitioned table not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partitioned table");
          } else if (OB_FAIL(resolve_partitioned_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve partition option", K(ret));
          }
          break;
        }
        case T_ALTER_PARTITION_REORGANIZE: {
          bool enable_split_partition = false;
          if (!table_schema_->is_range_part() && !table_schema_->is_list_part()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("reorganize hash partition not supported", K(ret));
            LOG_USER_WARN(OB_NOT_SUPPORTED, "Reorganize hash partition");
          } else if (OB_FAIL(get_enable_split_partition(session_info_->get_effective_tenant_id(),
                                                        enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config", K(ret),
                "tenant_id", session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("reorganize partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "reorganize partition");
          } else if (OB_FAIL(resolve_reorganize_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve reorganize partition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ =
              ObAlterTableArg::REORGANIZE_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_SPLIT: {
          bool enable_split_partition = false;
          if (!table_schema_->is_range_part() && !table_schema_->is_list_part()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Split hash partition not supported", K(ret));
            LOG_USER_WARN(OB_NOT_SUPPORTED, "Split hash partition");
          } else if (OB_FAIL(get_enable_split_partition(session_info_->get_effective_tenant_id(),
                  enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config", K(ret),
                "tenant_id", session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("split partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "split partition");
          } else if (OB_FAIL(resolve_split_partition(partition_node, *table_schema_))) {
            LOG_WARN("fail to resolve reorganize partition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ =
                ObAlterTableArg::SPLIT_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_TRUNCATE: {
          ParseNode *partition_node = node.children_[0];
          if (OB_FAIL(resolve_drop_partition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve truncate partition", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::TRUNCATE_PARTITION;
          }
          break;
        }
        case T_ALTER_SUBPARTITION_TRUNCATE: {
          if (OB_FAIL(resolve_drop_subpartition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve drop subpartition", KR(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().is_update_global_indexes_ = partition_node->num_child_ == 2;
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::TRUNCATE_SUB_PARTITION;
          }
          break;
        }
        case T_ALTER_PARTITION_EXCHANGE: {
          if (OB_FAIL(resolve_exchange_partition(*partition_node, *table_schema_))) {
            LOG_WARN("failed to resolve exchange partition", K(ret));
          } else {
            alter_table_stmt->get_alter_table_arg().alter_part_type_ = ObAlterTableArg::EXCHANGE_PARTITION;
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "Unknown alter partition option %d!",
                       "option type", node.children_[0]->type_, K(ret));
          break;
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_partitioned_partition(const ParseNode *node,
                                                        const ObTableSchema &origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node)
      || T_ALTER_PARTITION_PARTITIONED != node->type_
      || OB_ISNULL(node->children_)
      || 1 != node->num_child_
      || OB_ISNULL(node->children_[0])) {
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
    AlterTableSchema &table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    if (OB_FAIL(table_schema.assign(origin_table_schema))) {
      LOG_WARN("fail to assign", K(ret));
    } else {
      table_schema.reuse_partition_schema();
      table_schema.reset_column_part_key_info();
    }
    if (OB_FAIL(ret)) {
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

int ObAlterTableResolver::resolve_split_partition(const ParseNode *node,
                                                  const share::schema::ObTableSchema &origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node)
      || T_ALTER_PARTITION_SPLIT != node->type_
      || 2 != node->num_child_
      || OB_ISNULL(node->children_[0])
      || OB_ISNULL(node->children_[1])
      || T_SPLIT_ACTION != node->children_[1]->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", K(ret));
  } else {
    ParseNode *name_list = node->children_[0];
    AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    //保存原table_name在assign之前
    if (OB_FAIL(ob_write_string(alloc,
                                alter_table_schema.get_origin_table_name(),
                                origin_table_name))) {
      LOG_WARN("fail to wirte string", K(ret));
    } else if (OB_FAIL(ob_write_string(alloc,
                                       alter_table_schema.get_origin_database_name(),
                                       origin_database_name))) {
      LOG_WARN("fail to wirte string", K(ret));
    } else if (OB_FAIL(alter_table_schema.assign(origin_table_schema))) {
      LOG_WARN("failed to assign table schema", K(ret), K(alter_table_schema));
    } else {
      alter_table_schema.reset_partition_schema();
    }

    if (OB_SUCC(ret)) {
      //解析被分裂的分区名
      if (OB_ISNULL(name_list)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(node));
      } else {
        ObString partition_name(static_cast<int32_t>(name_list->str_len_),
                                name_list->str_value_);
        if (OB_FAIL(alter_table_schema.set_split_partition_name(partition_name))) {
          LOG_WARN("failed to set split partition name", K(ret));
        }
      }
    }

    int64_t expr_count = OB_INVALID_PARTITION_ID;
    const ParseNode *split_node = node->children_[1];
    ParseNode *part_func_node = NULL;
    PartitionInfo part_info;
    int64_t expr_num = OB_INVALID_COUNT;
    if (OB_FAIL(mock_part_func_node(origin_table_schema, false/*is_sub_part*/, part_func_node))) {
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
     *  - T_SPLIT_LIST/T_SPLIT_RANGE(标记必须有)
     * */
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_split_type_valid(split_node,
                                              origin_table_schema.get_part_option().get_part_func_type()))) {
      LOG_WARN("failed to check split type valid", K(ret), K(origin_table_schema));
    } else if (OB_NOT_NULL(split_node->children_[AT_VALUES_NODE])
               && OB_NOT_NULL(split_node->children_[SPLIT_PARTITION_TYPE_NODE])) {
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
    } else if (OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE])
               && OB_NOT_NULL(split_node->children_[SPLIT_PARTITION_TYPE_NODE])
               && OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE]->children_[0])) {
      // split into ()
      const ParseNode *range_element_node = split_node->children_[PARTITION_DEFINE_NODE]->children_[0];
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
      //不能即没有at也没有partition说明
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
      alter_table_schema.get_part_option().set_part_num(expr_count);//最后一个partition可能没有最大值，需要在rs端处理
    }
  }
  return ret;
}



int ObAlterTableResolver::resolve_reorganize_partition(const ParseNode *node,
                                                       const share::schema::ObTableSchema &origin_table_schema)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  ObArenaAllocator alloc;
  ObString origin_table_name;
  ObString origin_database_name;
  if (OB_ISNULL(node)
      || T_ALTER_PARTITION_REORGANIZE != node->type_
      || 2 != node->num_child_
      || OB_ISNULL(node->children_[0])
      || OB_ISNULL(node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else {
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else {
      // 处理第一个节点为分裂后的分区
      ParseNode *name_list = node->children_[1];
      AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      //保存原table_name在assign之前
      if (OB_FAIL(ob_write_string(alloc,
                                  alter_table_schema.get_origin_table_name(),
                                  origin_table_name))) {
        LOG_WARN("fail to wirte string", K(ret));
      } else if (OB_FAIL(ob_write_string(alloc,
                                         alter_table_schema.get_origin_database_name(),
                                         origin_database_name))) {
        LOG_WARN("fail to wirte string", K(ret));
      } else if (OB_FAIL(alter_table_schema.assign(origin_table_schema))) {
        LOG_WARN("failed to assign table schema", K(ret), K(alter_table_schema));
      } else {
        alter_table_schema.reset_partition_schema();
      }
      //解析添加的节点
      if (OB_SUCC(ret)) {
        //解析被分裂的分区名
        if (OB_ISNULL(name_list)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(node));
        } else if (name_list->num_child_ != 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter table reorganize multi partition not supported now", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter table reorganize multiple partitions");
        } else {
          ObPartition part;
          ObString partition_name(static_cast<int32_t>(name_list->children_[0]->str_len_),
                                  name_list->children_[0]->str_value_);
          if (OB_FAIL(alter_table_schema.set_split_partition_name(partition_name))) {
            LOG_WARN("failed to set split partition name", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        //nothing
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

int ObAlterTableResolver::resolve_convert_to_character(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_CONVERT_TO_CHARACTER != node.type_ ||
                  NULL == node.children_ ||
                  node.num_child_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    ParseNode *charset_node = node.children_[0];
    if (OB_ISNULL(charset_node) ||
        OB_UNLIKELY(T_CHAR_CHARSET != charset_node->type_ && T_VARCHAR != charset_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObString charset_node_value(charset_node->str_len_,
                                  charset_node->str_value_);
      ObString charset = charset_node_value.trim();
      ObCharsetType charset_type = ObCharset::charset_type(charset);
      if (CHARSET_INVALID == charset_type) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
      } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(charset_type, session_info_->get_effective_tenant_id()))) {
        LOG_WARN("failed to check charset data version valid", K(ret));
      } else {
        charset_type_ = charset_type;
      }
    }
  }
  if (OB_SUCC(ret) && 2 == node.num_child_ && OB_NOT_NULL(node.children_[1])) {
    ParseNode *collation_node = node.children_[1];
    ObString collation_node_value(collation_node->str_len_,
                                  collation_node->str_value_);
    ObString collation = collation_node_value.trim();
    ObCollationType collation_type = ObCharset::collation_type(collation);
    if (CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNKNOWN_COLLATION;
      LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
    } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(common::ObCharset::charset_type_by_coll(collation_type),
                                                                      session_info_->get_effective_tenant_id()))) {
      LOG_WARN("failed to check charset data version valid", K(ret));
    } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(collation_type,
                                                                        session_info_->get_effective_tenant_id()))) {
      LOG_WARN("failed to check collation data version valid", K(ret));
    } else {
      collation_type_ = collation_type;
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_tablegroup_options(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_TABLEGROUP_OPTION != node.type_ ||
                  NULL == node.children_ ||
                  node.num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    ParseNode *tablegroup_action_node = node.children_[0];
    if (OB_ISNULL(tablegroup_action_node) ||
        OB_UNLIKELY(T_TABLEGROUP_DROP != tablegroup_action_node->type_ )) {
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

int ObAlterTableResolver::resolve_foreign_key_options(const ParseNode &node)
{
  int ret = OB_SUCCESS;

  if (lib::is_mysql_mode()) {
    ParseNode *foreign_key_action_node = NULL;
    if (T_DROP_CONSTRAINT == node.type_) {
      if (OB_FAIL(resolve_drop_foreign_key(node))) {
        SQL_RESV_LOG(WARN, "resolve drop foreign key failed", K(ret));
      }
    } else if (OB_UNLIKELY(T_ALTER_FOREIGN_KEY_OPTION != node.type_ || OB_ISNULL(node.children_) || node.num_child_ <= 0)) {
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
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
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
  } else if (lib::is_oracle_mode()) {
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

int ObAlterTableResolver::resolve_alter_table_option_list(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (node.num_child_ <= 0 || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode *option_node = NULL;
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

int ObAlterTableResolver::process_timestamp_column(ObColumnResolveStat &stat,
                                                   AlterColumnSchema &alter_column_schema)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_default_value = alter_column_schema.get_cur_default_value();
  bool explicit_value = false;
  if (NULL == session_info_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info is NULL", K(ret));
  } else if (OB_FAIL(alter_column_schema.set_orig_default_value(cur_default_value))) {
    SQL_RESV_LOG(WARN, "fail to set orig default value for alter table", K(ret), K(cur_default_value));
  } else if (OB_FAIL(session_info_->get_explicit_defaults_for_timestamp(explicit_value))) {
    LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
  } else if (true == explicit_value || alter_column_schema.is_generated_column()) {
    //nothing to do
  } else {
    alter_column_schema.check_timestamp_column_order_ = true;
    alter_column_schema.is_no_zero_date_ = is_no_zero_date(session_info_->get_sql_mode());
    alter_column_schema.is_set_nullable_ = stat.is_set_null_;
    alter_column_schema.is_set_default_ = stat.is_set_default_value_;
  }
  return ret;
}

int ObAlterTableResolver::check_sdo_geom_default_value(ObAlterTableStmt *alter_table_stmt,
                                                       AlterColumnSchema &column_schema)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && column_schema.is_geometry()) {
    ObObj orig_default_value;
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_2_2_0
      || (tenant_data_version >= DATA_VERSION_4_3_0_0 && tenant_data_version < DATA_VERSION_4_3_1_0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sdo_geometry is not supported when data version is below 4.2.2.0 or data_version is above 4.3.0.0 but below 4.3.1.0", K(ret), K(tenant_data_version), K(column_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2.2 or data_version is above 4.3.0.0 but below 4.3.1.0, sdo_geometry");
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter table stmt not exist", K(ret));
    } else if (column_schema.get_cur_default_value().is_null()) {
    } else if (OB_FAIL(get_udt_column_default_values(column_schema.get_cur_default_value(),
                                                     session_info_->get_tz_info_wrap(),
                                                     *allocator_,
                                                     column_schema,
                                                     session_info_->get_sql_mode(),
                                                     session_info_,
                                                     schema_checker_,
                                                     orig_default_value,
                                                     alter_table_stmt->get_ddl_arg()))) {
      LOG_WARN("fail to calc xmltype default value expr", K(ret));
    } else if (orig_default_value.is_null()) {
    } else {
      // get rid of lob header
      ObObj default_val;
      ObString swkb;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator_, orig_default_value, swkb))) {
        LOG_WARN("fail to get real data.", K(ret));
      } else {
        default_val.set_common_value(swkb);
        default_val.set_meta_type(column_schema.get_meta_type());
        if (OB_FAIL(column_schema.set_orig_default_value(default_val))) {
          LOG_WARN("fail to set orig default value", K(default_val), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::add_udt_hidden_column(ObAlterTableStmt *alter_table_stmt,
                                                AlterColumnSchema &column_schema)
{
  int ret = OB_SUCCESS;
  if (column_schema.is_xmltype()) {
    AlterColumnSchema hidden_blob;
    hidden_blob.reset();
    hidden_blob.alter_type_ = OB_DDL_ADD_COLUMN;
    hidden_blob.set_data_type(ObLongTextType);
    hidden_blob.set_nullable(column_schema.is_nullable());
    hidden_blob.set_is_hidden(true);
    hidden_blob.set_charset_type(CHARSET_BINARY);
    hidden_blob.set_collation_type(CS_TYPE_BINARY);
    hidden_blob.set_udt_set_id(1);
    ObObj orig_default_value;
    column_schema.set_tenant_id(table_schema_->get_tenant_id());
    ObString nls_formats_str[ObNLSFormatEnum::NLS_MAX];
    nls_formats_str[ObNLSFormatEnum::NLS_DATE] = session_info_->get_local_nls_date_format();
    nls_formats_str[ObNLSFormatEnum::NLS_TIMESTAMP] = session_info_->get_local_nls_timestamp_format();
    nls_formats_str[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = session_info_->get_local_nls_timestamp_tz_format();

    // the only udt hidden column in 4.2 is the hidden blob of xmltype
    // if cur default value setted on xmltype column:
    // original default value on xmltype column is null
    // original default value on hidden blob is the calc result of xmltype cur default value (convert to blob)
    // cur default value on hidden blob is null
    if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter table stmt not exist", K(ret));
    } else if (column_schema.get_cur_default_value().is_null()) {
    } else if (OB_FAIL(get_udt_column_default_values(column_schema.get_cur_default_value(),
                                                     session_info_->get_tz_info_wrap(),
                                                     *allocator_,
                                                     column_schema,
                                                     session_info_->get_sql_mode(),
                                                     session_info_,
                                                     schema_checker_,
                                                     orig_default_value,
                                                     alter_table_stmt->get_ddl_arg()))) {
      LOG_WARN("fail to calc xmltype default value expr", K(ret));
    } else {
      // calc result is 1. string type (not lob) or 2. xmltype binary (need to remove lob header)
      ObString res_string;
      ObObj xml_default;
      xml_default.set_null();
      if (orig_default_value.is_character_type()) {
        // convert to xml binary
        ObXmlDocument *xml_doc = NULL;
        ObMulModeMemCtx* mem_ctx = nullptr;
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "XMLModule"));
        if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(allocator_, mem_ctx))) {
          LOG_WARN("fail to create tree memory context", K(ret));
        } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx,
                                                                  orig_default_value.get_string(),
                                                                  xml_doc))) {
          LOG_WARN("parse xml plain text as document failed.", K(ret), K(orig_default_value.get_string()));
          ret = OB_ERR_XML_PARSE;
          LOG_USER_ERROR(OB_ERR_XML_PARSE);
        } else if (OB_ISNULL(xml_doc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null xml document.", K(ret));
        } else {
          ObIMulModeBase *xml_root = static_cast<ObIMulModeBase *>(xml_doc);
          ObString res_string;
          if (OB_FAIL(xml_root->get_raw_binary(res_string, allocator_))) {
            LOG_WARN("failed to get xml binary", K(ret), K(orig_default_value.get_string()));
          } else {
            xml_default.set_lob_value(ObLongTextType, res_string.ptr(), res_string.length());
            xml_default.set_collation_type(CS_TYPE_BINARY);
          }
        }
      } else if (orig_default_value.is_xml_sql_type()) {
        // move lob header
        res_string = orig_default_value.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator_,
                                                              ObLongTextType,
                                                              CS_TYPE_BINARY,
                                                              true, res_string))) {
          LOG_WARN("failed to get xml binary data", K(ret), K(orig_default_value.get_string()));
        } else {
          xml_default.set_lob_value(ObLongTextType, res_string.ptr(), res_string.length());
          xml_default.set_collation_type(CS_TYPE_BINARY);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("error xml default type", K(ret), K(orig_default_value));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hidden_blob.set_orig_default_value(xml_default))) {
        LOG_WARN("fail to set orig default value", K(xml_default), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(hidden_blob.set_column_name("SYS_NC"))) {
      SQL_RESV_LOG(WARN, "failed to set column name", K(ret));
    } else if (OB_FAIL(alter_table_stmt->add_column(hidden_blob))) {
      SQL_RESV_LOG(WARN, "add column to table_schema failed", K(ret), K(hidden_blob));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_add_column(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_UNLIKELY(T_COLUMN_ADD != node.type_ || NULL == node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "stmt should not be null!", K(ret));
  } else {
    int64_t identity_column_count = 0;
    AlterColumnSchema alter_column_schema;
    ObColumnResolveStat stat;
    if (OB_FAIL(get_identity_column_count(*table_schema_, identity_column_count))) {
      SQL_RESV_LOG(WARN, "get identity column count fail", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      alter_column_schema.alter_type_ = OB_DDL_ADD_COLUMN;
      if (OB_ISNULL(node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else {
        //resolve column definition
        stat.reset();
        bool is_modify_column_visibility = false;
        ObSEArray<ObColumnSchemaV2 *, 8> resolved_cols;
        if (OB_FAIL(get_table_schema_all_column_schema(resolved_cols, alter_table_stmt->get_alter_table_schema()))) {
          SQL_RESV_LOG(WARN, "failed to get table column schema", K(ret));
        } else if (OB_FAIL(resolve_alter_table_column_definition(alter_column_schema,
                                                          node.children_[i],
                                                          stat,
                                                          is_modify_column_visibility,
                                                          resolved_cols,
                                                          table_schema_->is_oracle_tmp_table()))) {
          SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
        } else if (OB_FAIL(fill_column_schema_according_stat(stat, alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to fill column schema", K(alter_column_schema), K(stat), K(ret));
          //TODO(xiyu):hanlde add column c2 int unique key; support unique key
        } else if (OB_FAIL(set_column_collation(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to set column collation",
                       "column_name", alter_column_schema.get_column_name(), K(ret));
        }

        //do some check
        if (OB_SUCC(ret)) {
          // resolve index (unique key only) of column
          if (OB_SUCC(ret) && alter_column_schema.is_unique_key_) {
            if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
              SQL_RESV_LOG(WARN, "failed to resolve column index",
                           "column name", alter_column_schema.get_column_name(), K(ret));
            }
          }
          if (alter_column_schema.has_not_null_constraint()) {
            add_not_null_constraint_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (table_schema_->is_primary_vp_table() && alter_column_schema.is_generated_column()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("can't add generated column into vertical partition table", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add generated column into vertical partition table");
          } else if (alter_column_schema.is_identity_column()) {
            identity_column_count++;
            if (identity_column_count > 1) {
              ret = OB_ERR_IDENTITY_COLUMN_COUNT_EXCE_LIMIT;
              SQL_RESV_LOG(WARN, "each table can only have an identity column", K(ret));
            }
          }
        }
        //add column
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_sdo_geom_default_value(alter_table_stmt, alter_column_schema))) {
            SQL_RESV_LOG(WARN, "check sdo geometry default value failed!", K(ret));
          } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
          } else if (OB_FAIL(add_udt_hidden_column(alter_table_stmt, alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter udt hidden column schema failed!", K(ret));
          }
        }
      }
    }
    add_column_cnt_++;
  }
  return ret;
}

int ObAlterTableResolver::resolve_pos_column(const ParseNode *pos_node,
    share::schema::AlterColumnSchema &column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node can not be null", K(ret));
  } else if ((T_COLUMN_ADD_AFTER != pos_node->type_)
      && (T_COLUMN_ADD_BEFORE != pos_node->type_)
      && (T_COLUMN_ADD_FIRST != pos_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operation type", K(ret));
  } else if (T_COLUMN_ADD_FIRST == pos_node->type_) {
    column.is_first_ = true;
  } else if (OB_ISNULL(pos_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name node can not be null", K(ret));
  } else {
    ObString pos_column_name(static_cast<int32_t>(pos_node->children_[0]->str_len_),
        pos_node->children_[0]->str_value_);
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

int ObAlterTableResolver::get_table_schema_all_column_schema(
                                                    ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
    OZ (resolved_cols.push_back(table_schema.get_column_schema_by_idx(i)), i, table_schema);
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_table_column_definition(AlterColumnSchema &column,
                                                                ParseNode *node,
                                                                ObColumnResolveStat &stat,
                                                                bool &is_modify_column_visibility,
                                                        ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                                const bool is_oracle_temp_table,
                                                                const bool allow_has_default)
{
  int ret = OB_SUCCESS;
  common::ObString pk_name;
  ObAlterTableStmt* alter_table_stmt = get_alter_table_stmt();
  AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
  int64_t cst_cnt = alter_table_schema.get_constraint_count();
  ObSEArray<ObString, 4> gen_col_expr_arr;
  ObString tmp_str[ObNLSFormatEnum::NLS_MAX];
  tmp_str[ObNLSFormatEnum::NLS_DATE] = session_info_->get_local_nls_date_format();
  tmp_str[ObNLSFormatEnum::NLS_TIMESTAMP] = session_info_->get_local_nls_timestamp_format();
  tmp_str[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = session_info_->get_local_nls_timestamp_tz_format();
  AlterColumnSchema dummy_column(column.get_allocator());
  ObTableSchema tmp_table_schema; // check_default_value will change table_schema
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(node));
  } else if (OB_FAIL(tmp_table_schema.assign(*table_schema_))) {
    LOG_WARN("failed to assign a table schema", K(ret));
  } else if (OB_FAIL(resolve_column_definition(column, node, stat,
              is_modify_column_visibility, pk_name, *table_schema_,
              is_oracle_temp_table,
              false,
              allow_has_default))) {
    SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
  } else if (is_mysql_mode()){ // add column level constraint
    add_or_modify_check_cst_times_ += alter_table_schema.get_constraint_count() - cst_cnt;
  } else if (FALSE_IT(dummy_column = column)) {
  } else if (OB_FAIL(dummy_column.get_err_ret())) {
    LOG_WARN("failed to copy from column", K(ret));
  } else if (column.is_generated_column() && lib::is_oracle_mode()
          && OB_DDL_ADD_COLUMN == column.alter_type_
          && OB_FAIL(check_default_value(column.get_cur_default_value(),
                              session_info_->get_tz_info_wrap(),
                              tmp_str,
                              *allocator_,
                              tmp_table_schema,
                              resolved_cols,
                              dummy_column,
                              gen_col_expr_arr,
                              session_info_->get_sql_mode(),
                              session_info_,
                              false, /* allow_sequence*/
                              schema_checker_,
                              NULL == node->children_[1]))) {
    SQL_RESV_LOG(WARN, "failed to check default value", K(column), K(ret));
  } else if (OB_FAIL(column.set_cur_default_value(dummy_column.get_cur_default_value()))) {
    LOG_WARN("failed to set default value", K(ret));
  }
  // else if (OB_FAIL(process_default_value(stat, column))) {
  //   SQL_RESV_LOG(WARN, "failed to set default value", K(ret));
  //   //when add new column, the default value should saved in both origin default value
  //   //and cur_default value
  //   //in resvolve_column_definition default value is saved in cur_default_value
  //   //TODO(xiyu):hanlde add column c2 int unique key; support unique key
  // }
  if (OB_SUCC(ret)) {
    ParseNode *pos_node = NULL;
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
        if (OB_FAIL(resolve_pos_column(pos_node, column))) {
         LOG_WARN("fail to resove position column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::set_column_collation(AlterColumnSchema &alter_column_schema)
{
  int ret = OB_SUCCESS;
  if (alter_column_schema.get_meta_type().is_string_type()) {
    ObCharsetType charset_type = alter_column_schema.get_charset_type();
    ObCollationType collation_type = alter_column_schema.get_collation_type();
    if (CHARSET_INVALID == charset_type && CS_TYPE_INVALID == collation_type) {
      //do nothing
      //在rootserver会根据表的schema设置正确的collation和charset
    } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))){
      SQL_RESV_LOG(WARN, "fail to fill charset collation info", K(ret));
    } else {
      alter_column_schema.set_charset_type(charset_type);
      alter_column_schema.set_collation_type(collation_type);
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_column(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_ALTER != node.type_ || OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ParseNode *column_definition_ref_node  = node.children_[0];
    AlterColumnSchema alter_column_schema;
    if (OB_ISNULL(column_definition_ref_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else if (OB_FAIL(resolve_column_definition_ref(alter_column_schema,
                                                    column_definition_ref_node,
                                                    true))) {
      LOG_WARN("invalid column definition ref node", K(ret));
    } else {
      alter_column_schema.alter_type_ = OB_DDL_ALTER_COLUMN;
    }
    //resolve the default value
    if (OB_SUCC(ret)) {
      ParseNode *default_node = node.children_[1];
      //may be T_CONSTR_NOT_NULL
      //alter table t1 alter c1 not null
      if (NULL == default_node ||
        (T_CONSTR_DEFAULT != default_node->type_ && T_CONSTR_NULL != default_node->type_)) {
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else {
        ObObjParam default_value;
        if (T_CONSTR_NULL == default_node->type_) {
          default_value.set_null();
          alter_column_schema.is_drop_default_ = true;
        } else if (!lib::is_oracle_mode() && ob_is_text_tc(alter_column_schema.get_data_type())) {
          ret = OB_INVALID_DEFAULT;
          SQL_RESV_LOG(WARN, "BLOB/TEXT can't set default value!", K(ret));
        } else if (!lib::is_oracle_mode() && ob_is_json_tc(alter_column_schema.get_data_type())) {
          ret = OB_ERR_BLOB_CANT_HAVE_DEFAULT;
          SQL_RESV_LOG(WARN, "BLOB/TEXT or JSON can't set default value!", K(ret));
        } else if (!lib::is_oracle_mode() && ob_is_geometry_tc(alter_column_schema.get_data_type())) {
          ret = OB_ERR_BLOB_CANT_HAVE_DEFAULT;
          SQL_RESV_LOG(WARN, "GEOMETRY can't set default value!", K(ret));
        } else if (OB_FAIL(resolve_default_value(default_node, default_value))) {
          SQL_RESV_LOG(WARN, "failed to resolve default value!", K(ret));
        }
        if (OB_SUCCESS == ret &&
          OB_FAIL(alter_column_schema.set_cur_default_value(default_value))) {
            SQL_RESV_LOG(WARN, "failed to set current default to alter column schema!", K(ret));
        }
      }
    }
    //add atler alter_column schema
    if (OB_SUCC(ret)) {
      ObColumnResolveStat stat;
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      if (OB_ISNULL(alter_table_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
      } else if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "session_info_ should not be null", K(ret));
      } else if (OB_FAIL(process_timestamp_column(stat, alter_column_schema))) {
        SQL_RESV_LOG(WARN, "fail to process timestamp column", K(ret));
      } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))){
        SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
      } else if (FALSE_IT(alter_table_stmt->set_sql_mode(session_info_->get_sql_mode()))) {
      }
    }
  }
  return ret;
}

// To check whether modify/change column is allowed,
// should check it under main table schema and index table schemas.
int ObAlterTableResolver::check_column_in_part_key(const ObTableSchema &table_schema,
                                                   const ObColumnSchemaV2 &src_col_schema,
                                                   const ObColumnSchemaV2 &dst_col_schema)
{
  int ret = OB_SUCCESS;
  // 1. to get all check table schemas, including main table schema and its' index schemas.
  bool is_same = false;
  ObSArray<const ObTableSchema *> check_table_schemas;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret));
  } else if (OB_FAIL(ObTableSchema::check_is_exactly_same_type(src_col_schema,
                                                               dst_col_schema,
                                                               is_same))) {
    LOG_WARN("check same type alter failed", K(ret));
  } else if (table_schema.is_partitioned_table() && OB_FAIL(check_table_schemas.push_back(&table_schema))) {
    LOG_WARN("push back schema failed", K(ret));
  } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple index infos failed", K(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(table_schema.get_tenant_id(),
                                                 simple_index_infos.at(i).table_id_,
                                                 index_schema))) {
        LOG_WARN("get index schema failed", K(ret), K(simple_index_infos.at(i)), K(table_schema));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index schema", K(ret), K(simple_index_infos.at(i)));
      } else if (index_schema->is_partitioned_table() && OB_FAIL(check_table_schemas.push_back(index_schema))) {
        LOG_WARN("push back related index schema failed", K(ret));
      }
    }
  }

  // 2. to check whether change/modify column for each schema is allowed.
  if (OB_SUCC(ret)) {
    const ObString &alter_column_name = src_col_schema.get_column_name_str();
    for (int64_t i = 0; OB_SUCC(ret) && i < check_table_schemas.count(); i++) {
      const ObTableSchema &cur_table_schema = *check_table_schemas.at(i);
      const ObColumnSchemaV2 *column_schema = nullptr;
      if (OB_ISNULL(column_schema = cur_table_schema.get_column_schema(alter_column_name))) {
        // do nothing, bacause the column does not exist in the schema.
      } else if (column_schema->is_tbl_part_key_column()) {
        if (lib::is_oracle_mode() && !is_same) {
          ret = OB_ERR_MODIFY_PART_COLUMN_TYPE;
          SQL_RESV_LOG(WARN, "data type or len of a part column may not be changed", K(ret));
        } else if (cur_table_schema.is_global_index_table()) {
          // FIXME YIREN (20221019), allow to alter part key of global index table by refilling part info when rebuilding it.
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("alter part key column of global index table is disallowed", K(ret), KPC(column_schema), K(cur_table_schema));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter part key of global index is");
        } else if (OB_FAIL(check_alter_part_key_allowed(cur_table_schema, *column_schema, dst_col_schema))) {
          LOG_WARN("check alter partition key allowed failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::alter_column_expr_in_part_expr(
    const ObColumnSchemaV2 &src_col_schema,
    const ObColumnSchemaV2 &dst_col_schema,
    ObRawExpr *part_expr)
{
  int ret = OB_SUCCESS;
  if (part_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(part_expr);
    if (0 == column_ref->get_column_name().case_compare(src_col_schema.get_column_name())) {
      column_ref->set_data_type(dst_col_schema.get_data_type());
      column_ref->set_accuracy(dst_col_schema.get_accuracy());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
      ObRawExpr *sub_expr = part_expr->get_param_expr(i);
      if (OB_FAIL(alter_column_expr_in_part_expr(src_col_schema, dst_col_schema, sub_expr))) {
        LOG_WARN("alter column expr in part expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_alter_part_key_allowed(const ObTableSchema &table_schema,
                                                      const ObColumnSchemaV2 &src_col_schema,
                                                      const ObColumnSchemaV2 &dst_col_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table_schema.get_table_id();
  ObResolverParams resolver_ctx;
  ObRawExprFactory expr_factory(*allocator_);
  ObStmtFactory stmt_factory(*allocator_);
  TableItem table_item;
  resolver_ctx.allocator_ = allocator_;
  resolver_ctx.schema_checker_ = schema_checker_;
  resolver_ctx.session_info_ = session_info_;
  resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
  resolver_ctx.expr_factory_ = &expr_factory;
  resolver_ctx.stmt_factory_ = &stmt_factory;
  resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
  table_item.table_id_ = table_id;
  table_item.ref_id_ = table_id;
  table_item.type_ = TableItem::BASE_TABLE;
  // This is just to use the resolver to resolve the partition expr interface.
  // The resolver of any statement has this ability. The reason for using the delete
  // resolver is that the delete resolver is the simplest
  ObRawExpr *part_expr = NULL;
  ObPartitionFuncType part_type;
  SMART_VAR (ObDeleteResolver, delete_resolver, resolver_ctx) {
    ObDeleteStmt *delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
    CK (OB_NOT_NULL(delete_stmt));
    CK (OB_NOT_NULL(resolver_ctx.query_ctx_));
    OZ (delete_stmt->get_table_items().push_back(&table_item));
    OZ (delete_stmt->set_table_bit_index(table_id));
    if (src_col_schema.is_part_key_column()) {
      const ObString &part_str = table_schema.get_part_option().get_part_func_expr_str();
      part_type = table_schema.get_part_option().get_part_func_type();
      OZ (delete_resolver.resolve_partition_expr(table_item, table_schema,
      part_type, part_str, part_expr));
    } else {
      const ObString &part_str = table_schema.get_sub_part_option().get_part_func_expr_str();
      part_type = table_schema.get_sub_part_option().get_part_func_type();
      OZ (delete_resolver.resolve_partition_expr(table_item, table_schema,
      part_type, part_str, part_expr));
    }
    ObRawExprPartExprChecker part_expr_checker(part_type);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null part expr", K(ret));
    } else if (OB_FAIL(alter_column_expr_in_part_expr(src_col_schema, dst_col_schema, part_expr))) {
      LOG_WARN("fail to alter column expr in part expr", K(ret), KPC(part_expr));
    }
    OZ (part_expr->formalize(session_info_));
    if (OB_FAIL(ret)) {
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type ||
        PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type ||
        is_key_part(part_type)) {
      if (is_key_part(part_type) && part_expr->get_param_count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(*part_expr));
      }
      if (0 == part_expr->get_param_count()) {
        OZ (ObResolverUtils::check_column_valid_for_partition(*part_expr, part_type, table_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->get_param_count(); ++i) {
        ObRawExpr *param_expr = part_expr->get_param_expr(i);
        CK (OB_NOT_NULL(param_expr));
        OZ (ObResolverUtils::check_column_valid_for_partition(
          *param_expr, part_type, table_schema));
      }
    } else {
      OZ (ObResolverUtils::check_column_valid_for_partition(
        *part_expr, part_type, table_schema));
    }
    if (OB_SUCC(ret) && !is_key_part(part_type)) {
      OZ (part_expr->preorder_accept(part_expr_checker));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_change_column(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_CHANGE != node.type_ || OB_ISNULL(node.children_) ||
      OB_ISNULL(node.children_[0]) || OB_ISNULL(node.children_[1]) ||
      T_COLUMN_DEFINITION != node.children_[1]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    const ObColumnSchemaV2 *origin_col_schema = NULL;
    if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[0], true))) {
      LOG_WARN("check column definition ref node failed", K(ret));
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    }
    const ObString &origin_column_name = alter_column_schema.get_origin_column_name();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_checker_->get_column_schema(
               table_schema_->get_tenant_id(),
               table_schema_->get_table_id(),
               origin_column_name,
               origin_col_schema,
               false))) {
      if (ret == OB_ERR_BAD_FIELD_ERROR) {
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, origin_column_name.length(), origin_column_name.ptr(),
                       table_schema_->get_table_name_str().length(),
                       table_schema_->get_table_name_str().ptr());
      }
      LOG_WARN("fail to get origin column schema", K(ret));
    }
    //resolve new column definition
    if (OB_SUCC(ret)) {
      ObColumnResolveStat stat;
      alter_column_schema.set_column_flags(origin_col_schema->get_column_flags());
      //alter column的generated column flag应该自己解析，
      //所以需要清空掉自己以前拷贝的generated column flag
      alter_column_schema.erase_generated_column_flags();
      alter_column_schema.drop_not_null_cst();
      alter_column_schema.set_tenant_id(origin_col_schema->get_tenant_id());
      alter_column_schema.set_table_id(origin_col_schema->get_table_id());
      alter_column_schema.set_column_id(origin_col_schema->get_column_id());
      // alter table change col 是 mysql 模式下的语法，oracle 模式不会走到这里
      bool is_modify_column_visibility = false;
      alter_column_schema.alter_type_ = OB_DDL_CHANGE_COLUMN;
      ObSEArray<ObColumnSchemaV2 *, 8> resolved_cols;
      if (OB_FAIL(get_table_schema_all_column_schema(resolved_cols, alter_table_stmt->get_alter_table_schema()))) {
        SQL_RESV_LOG(WARN, "failed to get table column schema", K(ret));
      } else if (OB_FAIL(resolve_alter_table_column_definition(alter_column_schema, node.children_[1], stat, is_modify_column_visibility, resolved_cols))) {
        SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
      } else if (!stat.is_primary_key_ && OB_FAIL(resolve_alter_column_not_null(alter_column_schema, *origin_col_schema))) {
        LOG_WARN("resolve modify column not null failed", K(ret));
      } else {
        //TODO(xiyu):hanlde change column c2 c3 int unique key; support unique key
        alter_column_schema.is_primary_key_ = stat.is_primary_key_;
        alter_column_schema.is_unique_key_ = stat.is_unique_key_;
        alter_column_schema.is_autoincrement_ = stat.is_autoincrement_;
        alter_column_schema.is_set_nullable_ = stat.is_set_null_;
        alter_column_schema.is_set_default_ = stat.is_set_default_value_;
        add_not_null_constraint_ |= stat.is_set_not_null_;
        if (OB_FAIL(set_column_collation(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "fail to set column collation", K(alter_column_schema), K(ret));
        }
      }
    }

    // resolve index (unique key only) of column
    if (OB_SUCC(ret) && alter_column_schema.is_unique_key_) {
      if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
        SQL_RESV_LOG(WARN, "failed to resolve column index",
                     "origin column name", alter_column_schema.get_origin_column_name(),
                     "new column name", alter_column_schema.get_column_name(),
                     K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret) && lib::is_mysql_mode()) {
        if (0 != origin_col_schema->get_rowkey_position()
            && alter_column_schema.is_set_default_
            && alter_column_schema.get_cur_default_value().is_null()) {
          ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
          LOG_USER_ERROR(OB_ERR_PRIMARY_CANT_HAVE_NULL);
        } else if (0 != origin_col_schema->get_rowkey_position()
            && alter_column_schema.is_set_nullable_) {
          ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
          LOG_WARN("can't set primary key nullable", K(ret));
        } else if (OB_FAIL(check_alter_geo_column_allowed(alter_column_schema, *origin_col_schema))) {
          LOG_WARN("modify geo column not allowed", K(ret));
        } else if (OB_FAIL(check_alter_multivalue_depend_column_allowed(alter_column_schema, *origin_col_schema))) {
          LOG_WARN("modify geo column not allowed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if ((origin_col_schema->get_data_type()) != (alter_column_schema.get_data_type())) {
          // alter table change column 的时候，如果只改列名字，不改列类型，就无需检查外键约束，允许改成功
          // 如果改了列的类型，就需要检查外键约束
          if (OB_FAIL(check_column_in_foreign_key(*table_schema_,
                                                  alter_column_schema.get_origin_column_name(),
                                                  false /* is_drop_column */))) {
            SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_column_in_part_key(*table_schema_, *origin_col_schema, alter_column_schema))) {
          SQL_RESV_LOG(WARN, "check column in part key failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        alter_table_stmt->set_sql_mode(session_info_->get_sql_mode());
        if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
          SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_modify_column_allowed(
                                const share::schema::AlterColumnSchema &alter_column_schema,
                                const share::schema::ObColumnSchemaV2 &origin_col_schema,
                                const ObColumnResolveStat &stat)
{
  int ret = OB_SUCCESS;
  const ObObjType origin_col_type = origin_col_schema.get_data_type();
  const ObObjType alter_col_type = alter_column_schema.get_data_type();
  const ObAccuracy &origin_col_accuracy = origin_col_schema.get_accuracy();
  const ObAccuracy &alter_col_accuracy = alter_column_schema.get_accuracy();
  // The number type does not specify precision, which means that it is the largest range and requires special judgment
  if (lib::is_oracle_mode()
      && (origin_col_type == alter_col_type
          || (origin_col_type == ObDecimalIntType && alter_col_type == ObNumberType)
          || (origin_col_type == ObNumberType && alter_col_type == ObDecimalIntType))
      && (origin_col_type == ObNumberType
          || origin_col_type == ObUNumberType
          || origin_col_type == ObNumberFloatType
          || origin_col_type == ObDecimalIntType)
      && (origin_col_accuracy.get_precision() >
          alter_col_accuracy.get_precision()
          || origin_col_accuracy.get_scale() >
          alter_col_accuracy.get_scale())
      && !ObAccuracy::is_default_number(alter_column_schema.get_accuracy())) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "Can not decrease precision or scale",
                K(ret), K(alter_col_accuracy), K(origin_col_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Decrease precision or scale");
  } else if ((lib::is_oracle_mode())
                   && (ObTimestampNanoType == origin_col_type
                        || ObTimestampType == origin_col_type
                        || ObDateTimeType == origin_col_type
                        || ObTimeType == origin_col_type)
                   && origin_col_type == alter_col_type
                   && origin_col_accuracy.get_precision() >
                      alter_col_accuracy.get_precision()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Decrease scale of timestamp type not supported", K(ret),
            K(origin_col_accuracy), K(alter_col_accuracy));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Decrease scale of timestamp type");
  } else if (lib::is_oracle_mode() && stat.is_set_not_null_) {
    // can't modify other property of column when modify column not null,
    // otherwise we can't rollback to previous state.
    if (OB_UNLIKELY(origin_col_type != alter_col_type)) {
      // supported in oracle. t(c1 int): alter table t modify c1 varchar(100) not null;
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Modify column not null and change type not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify column not null and change type");
    } else if (ob_is_accuracy_length_valid_tc(origin_col_type)) {
      if (OB_UNLIKELY(origin_col_accuracy.length_ != alter_col_accuracy.length_
                || origin_col_accuracy.length_semantics_ != alter_col_accuracy.length_semantics_)) {
        // supported in oracle.
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("modify column not null and change data length is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify column not null and change data length");
      }
    } else {
      if (OB_UNLIKELY(origin_col_accuracy.precision_ != alter_col_accuracy.precision_
                      || origin_col_accuracy.scale_ != alter_col_accuracy.scale_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("modify column not null and change precision/scale not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify column not null and change precision/scale");
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (lib::is_oracle_mode()
      && ((origin_col_type != alter_col_type)
        || (origin_col_schema.get_data_length() != alter_column_schema.get_data_length()))) {
    // not support modify type or length of partition key in Oracle mode.
    if (origin_col_schema.is_part_key_column()) {
      ret = OB_ERR_MODIFY_PART_COLUMN_TYPE;
      SQL_RESV_LOG(WARN, "data type or len of a part column may not be changed", K(ret));
    } else if (origin_col_schema.is_subpart_key_column()) {
      ret = OB_ERR_MODIFY_SUBPART_COLUMN_TYPE;
      SQL_RESV_LOG(WARN, "data type or len of a subpart column may not be changed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if ((origin_col_type) != alter_col_type
         || (!origin_col_schema.is_autoincrement() && alter_column_schema.is_autoincrement())) {
      // when execute "alter table modify column",
      // don't need to check foreign constraint if type is not modified.
      if (OB_FAIL(check_column_in_foreign_key(*table_schema_,
                                              alter_column_schema.get_origin_column_name(),
                                              false /* is_drop_column */))) {
        SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_alter_multivalue_depend_column_allowed(
  const share::schema::AlterColumnSchema &alter_column_schema,
  const share::schema::ObColumnSchemaV2 &data_column_schema)
{
  int ret = OB_SUCCESS;
  bool has_func_idx_col_deps = false;
  bool is_oracle_mode = false;

  ObString column_name = data_column_schema.get_column_name_str();

  if (!data_column_schema.has_generated_column_deps()) {
  } else if (OB_FAIL(table_schema_->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check if oracle compat mode", K(ret));
  } else if (OB_FAIL(table_schema_->check_column_has_multivalue_index_depend(
    data_column_schema, has_func_idx_col_deps))) {
    LOG_WARN("fail to check if column has functional index dependency.", K(ret));
  } else if (has_func_idx_col_deps) {
    ret = OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX;
    LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX, column_name.length(), column_name.ptr());
    LOG_WARN("alter column has functional index column deps", K(ret), K(column_name));
  }

  return ret;
}


int ObAlterTableResolver::check_alter_geo_column_allowed(const share::schema::AlterColumnSchema &alter_column_schema,
                                                         const share::schema::ObColumnSchemaV2 &origin_col_schema)
{
  int ret = OB_SUCCESS;
  if (ObGeometryType != origin_col_schema.get_data_type()) {
    // do nothing
  } else if (origin_col_schema.is_spatial_index_column()
            && ObGeometryType != alter_column_schema.get_data_type()) {
    ret = OB_ERR_SPATIAL_MUST_HAVE_GEOM_COL;
    LOG_USER_ERROR(OB_ERR_SPATIAL_MUST_HAVE_GEOM_COL);
    LOG_WARN("can't not alter geometry col with spatial index", K(ret), K(origin_col_schema.get_geo_type()),
            K(alter_column_schema.get_geo_type()));
  } else if (ObGeometryType == alter_column_schema.get_data_type()
              && alter_column_schema.get_geo_type() != common::ObGeoType::GEOMETRY
              && origin_col_schema.get_geo_type() != common::ObGeoType::GEOMETRY
              && origin_col_schema.get_geo_type() != alter_column_schema.get_geo_type()) {
    ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
    LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
    LOG_WARN("can't not alter geometry type", K(ret), K(origin_col_schema.get_geo_type()),
            K(alter_column_schema.get_geo_type()));
  } else if (ObGeometryType == alter_column_schema.get_data_type()
            && origin_col_schema.get_srid() != alter_column_schema.get_srid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter geometry srid");
    LOG_WARN("can't not alter geometry srid", K(ret),
            K(origin_col_schema.get_srid()), K(alter_column_schema.get_srid()));
  }
  return ret;
}

int ObAlterTableResolver::resolve_modify_column(const ParseNode &node,
                                                bool &is_modify_column_visibility,
                                                ObReducedVisibleColSet &reduced_visible_col_set)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_MODIFY != node.type_ ||
      OB_ISNULL(node.children_) ||
      OB_ISNULL(node.children_[0]) ||
      T_COLUMN_DEFINITION != node.children_[0]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    //resolve new column defintion
    ObColumnResolveStat stat;
    //TODO(xiyu):hanlde modify column c2 int unique key; support unique key
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      stat.reset();
      const ObColumnSchemaV2 *origin_col_schema = NULL;
      ObString column_name;
      ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
      bool is_identity_column = false;
      if (OB_FAIL(check_column_definition_node(node.children_[i]))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_column_definition_ref(
                 alter_column_schema, node.children_[i]->children_[0], false))) {
        SQL_RESV_LOG(WARN, "fail to resolve column name from parse node", K(ret));
      } else {
        column_name = alter_column_schema.get_column_name();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (OB_FAIL(schema_checker_->get_column_schema(table_schema_->get_tenant_id(), table_schema_->get_table_id(),
                                                              column_name,
                                                              origin_col_schema,
                                                              false))) {
          if (ret == OB_ERR_BAD_FIELD_ERROR) {
            LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
                           table_schema_->get_table_name_str().length(),
                           table_schema_->get_table_name_str().ptr());
          }
          LOG_WARN("fail to get origin column schema", K(ret));
        } else if (lib::is_oracle_mode() && OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
          LOG_WARN("fail to copy column schema", K(ret));
        } else {
          //identity column->identity column,YES
          //identity column->normal column,YES,but still identity column
          //identity column->generated column,NO
          //other column->identity colum,NO
          is_identity_column = alter_column_schema.is_identity_column();

          //alter column的generated column flag应该自己解析，
          //所以需要清空掉自己以前拷贝的generated column flag
          alter_column_schema.set_column_flags(origin_col_schema->get_column_flags());
          alter_column_schema.erase_generated_column_flags();
          if (!is_oracle_mode()) {
            alter_column_schema.drop_not_null_cst();
          }
          alter_table_stmt->set_sql_mode(session_info_->get_sql_mode());
          alter_column_schema.alter_type_ = OB_DDL_MODIFY_COLUMN;
          alter_column_schema.set_tenant_id(origin_col_schema->get_tenant_id());
          alter_column_schema.set_table_id(origin_col_schema->get_table_id());
          alter_column_schema.set_column_id(origin_col_schema->get_column_id());
        }
      }

      if (OB_SUCC(ret)) {
        ObSEArray<ObColumnSchemaV2 *, 8> resolved_cols;
        bool allow_has_default = !(is_oracle_mode() && origin_col_schema->is_generated_column());
        if (OB_FAIL(get_table_schema_all_column_schema(resolved_cols, alter_table_stmt->get_alter_table_schema()))) {
          SQL_RESV_LOG(WARN, "failed to get table column schema", K(ret));
        } else if (OB_FAIL(resolve_alter_table_column_definition(
                    alter_column_schema, node.children_[i], stat, is_modify_column_visibility,
                    resolved_cols,
                    table_schema_->is_oracle_tmp_table(),
                    allow_has_default))) {
          SQL_RESV_LOG(WARN, "resolve column definition failed", K(ret));
        } else if (!is_oracle_mode() && !stat.is_primary_key_ &&
                  OB_FAIL(resolve_alter_column_not_null(alter_column_schema, *origin_col_schema))) {
          LOG_WARN("resolve modify column not null failed", K(ret));
        } else if (is_identity_column != alter_column_schema.is_identity_column()) {
          ret = OB_ERR_COLUMN_MODIFY_TO_IDENTITY_COLUMN;
          SQL_RESV_LOG(WARN, "other column could not modify to identity column", K(ret));
        }
        if (OB_SUCC(ret)) {
          alter_column_schema.is_primary_key_ = stat.is_primary_key_;
          alter_column_schema.is_unique_key_ = stat.is_unique_key_;
          alter_column_schema.is_autoincrement_ = stat.is_autoincrement_;
          alter_column_schema.is_set_nullable_ = stat.is_set_null_;
          alter_column_schema.is_set_default_ = stat.is_set_default_value_;
          add_not_null_constraint_ |= stat.is_set_not_null_;
          if (OB_FAIL(alter_column_schema.set_origin_column_name(column_name))) {
            SQL_RESV_LOG(WARN, "failed to set origin column name", K(column_name), K(ret));
          } else if (OB_FAIL(set_column_collation(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "fail to set column collation", K(alter_column_schema), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        bool is_sync_ddl_user = false;
        if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_dll_user", K(ret));
        } else if (is_sync_ddl_user) {
          // skip
        } else if (OB_FAIL(check_modify_column_allowed(alter_column_schema,
                                                       *origin_col_schema, stat))) {
          LOG_WARN("modify column not allowed", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_column_in_part_key(*table_schema_, *origin_col_schema, alter_column_schema))) {
            SQL_RESV_LOG(WARN, "check column in part key failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && alter_column_schema.is_udt_related_column(lib::is_oracle_mode())) {
          ObString tmp_str;
          if (OB_FAIL(ObDDLResolver::check_udt_default_value(alter_column_schema.get_cur_default_value(),
                                                             session_info_->get_tz_info_wrap(),
                                                             &tmp_str,    // useless
                                                             *allocator_,
                                                             *const_cast<ObTableSchema *>(table_schema_),
                                                             alter_column_schema,
                                                             session_info_->get_sql_mode(),
                                                             session_info_,
                                                             schema_checker_,
                                                             alter_table_stmt->get_ddl_arg()))) {
            SQL_RESV_LOG(WARN, "check udt column default value failed in alter table modify stmt",
                        K(ret), K(alter_column_schema.get_cur_default_value()));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
          }
        }
        if (OB_SUCC(ret) && lib::is_mysql_mode()) {
          if (0 != origin_col_schema->get_rowkey_position()
              && alter_column_schema.is_set_default_
              && alter_column_schema.get_cur_default_value().is_null()) {
            ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
            LOG_USER_ERROR(OB_ERR_PRIMARY_CANT_HAVE_NULL);
          } else if (0 != origin_col_schema->get_rowkey_position()
              && alter_column_schema.is_set_nullable_) {
            ret = OB_ERR_PRIMARY_CANT_HAVE_NULL;
            LOG_WARN("can't set primary key nullable", K(ret));
          } else if (OB_FAIL(check_alter_geo_column_allowed(alter_column_schema, *origin_col_schema))) {
            LOG_WARN("modify geo column not allowed", K(ret));
          } else if (ObGeometryType == origin_col_schema->get_data_type()
                     && ObGeometryType == alter_column_schema.get_data_type()
                     && alter_column_schema.get_geo_type() != common::ObGeoType::GEOMETRY
                     && origin_col_schema->get_geo_type() != common::ObGeoType::GEOMETRY
                     && origin_col_schema->get_geo_type() != alter_column_schema.get_geo_type()) {
            ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
            LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
            LOG_WARN("can't not modify geometry type", K(ret), K(origin_col_schema->get_geo_type()),
                    K(alter_column_schema.get_geo_type()));
          } else if (ObGeometryType == origin_col_schema->get_data_type()
                     && ObGeometryType == alter_column_schema.get_data_type()
                     && origin_col_schema->get_srid() != alter_column_schema.get_srid()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify geometry srid");
            LOG_WARN("can't not modify geometry srid", K(ret),
                    K(origin_col_schema->get_srid()), K(alter_column_schema.get_srid()));
          } else if (OB_FAIL(check_alter_multivalue_depend_column_allowed(alter_column_schema, *origin_col_schema))) {
            LOG_WARN("modify geo column not allowed", K(ret));
          } else if (origin_col_schema->get_data_type() == ObRoaringBitmapType
                     && alter_column_schema.get_data_type() != ObRoaringBitmapType
                     && !ob_is_string_type(alter_column_schema.get_data_type())) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify roaringbitmap to other type except string");
            LOG_WARN("can't not modify roaringbitmap type", K(ret),
                    K(origin_col_schema->get_data_type()), K(alter_column_schema.get_data_type()));
          } else if (alter_column_schema.get_data_type() == ObRoaringBitmapType
                     && origin_col_schema->get_data_type() != ObRoaringBitmapType) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify other type to roaringbitmap");
            LOG_WARN("can't not modify other type to roaringbitmap type", K(ret),
                    K(origin_col_schema->get_data_type()), K(alter_column_schema.get_data_type()));
          }
        }
      }

      // resolve index (unique key only) of column
      if (OB_SUCC(ret) && alter_column_schema.is_unique_key_) {
        if (OB_FAIL(resolve_column_index(alter_column_schema.get_column_name_str()))) {
          SQL_RESV_LOG(WARN, "failed to resolve column index",
              "column_name", alter_column_schema.get_column_name(), K(ret));
        }
      }
      if (OB_SUCC(ret) && lib::is_oracle_mode() && alter_column_schema.is_invisible_column()) {
        ObString name(node.children_[i]->children_[0]->children_[2]->str_len_, node.children_[i]->children_[0]->children_[2]->str_value_);
        ObColumnSchemaHashWrapper col_key(name);
        if (OB_FAIL(reduced_visible_col_set.set_refactored(col_key))) {
          SQL_RESV_LOG(WARN, "set foreign key name to hash set failed", K(ret), K(alter_column_schema.get_column_name_str()));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_column_not_null(share::schema::AlterColumnSchema &column,
                                                        const ObColumnSchemaV2 &ori_column)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
  } else if (ori_column.has_not_null_constraint()) {
    if (column.is_nullable()) {
      if (OB_FAIL(drop_not_null_constraint(column))) {
        LOG_WARN("drop not null constraint failed", K(ret));
      }
    } else {
      column.add_not_null_cst();
    }
  } else if (ori_column.is_nullable() && !column.is_nullable() && !column.is_autoincrement()) {
    column.set_nullable(true);
    if (OB_FAIL(resolve_not_null_constraint_node(column, NULL, false))) {
      LOG_WARN("resolve not null constraint not failed", K(ret));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_column_index(const ObString &column_name)
{
  int ret = OB_SUCCESS;
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

  return ret;
}

int ObAlterTableResolver::resolve_drop_column(const ParseNode &node, ObReducedVisibleColSet &reduced_visible_col_set)
{
  int ret = OB_SUCCESS;
  if (T_COLUMN_DROP != node.type_ ||
      OB_ISNULL(node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    AlterColumnSchema alter_column_schema;
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
      alter_column_schema.reset();
      if (OB_ISNULL(node.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
      } else if (OB_FAIL(resolve_column_definition_ref(alter_column_schema,
                                                        node.children_[i], true))) {
        LOG_WARN("check column definition ref node failed", K(ret));
      } else {
        alter_column_schema.alter_type_ = OB_DDL_DROP_COLUMN;
      }
      if (OB_SUCC(ret)) {
        ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
        if (OB_ISNULL(alter_table_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
        } else if (!lib::is_oracle_mode()
                   && OB_FAIL(check_column_in_foreign_key(
                              *table_schema_,
                              alter_column_schema.get_origin_column_name(),
                              true /* is_drop_column */))) {
          SQL_RESV_LOG(WARN, "failed to check_column_in_foreign_key", K(ret));
        } else if (lib::is_oracle_mode()
                   && OB_FAIL(check_column_in_foreign_key_for_oracle(
                              *table_schema_,
                              alter_column_schema.get_origin_column_name(),
                              alter_table_stmt))) {
          SQL_RESV_LOG(WARN, "failed to check column in foreign key for oracle mode", K(ret));
        } else if (OB_FAIL(check_drop_column_is_partition_key(*table_schema_,
                                                              alter_column_schema.get_origin_column_name()))) {
          SQL_RESV_LOG(WARN, "failed to check column in parition key", K(ret));
        } else if (OB_FAIL(check_column_in_check_constraint(
                           *table_schema_,
                           alter_column_schema.get_origin_column_name(),
                           alter_table_stmt))) {
          SQL_RESV_LOG(WARN, "failed to check column in foreign key for oracle mode", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))){
          SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const ObString &column_name = alter_column_schema.get_origin_column_name();
        ObColumnSchemaHashWrapper col_key(column_name);
        if (OB_FAIL(reduced_visible_col_set.set_refactored(col_key))) {
          if (OB_HASH_EXIST == ret) {
            if (is_mysql_mode()) {
              //In mysql mode, OB will check whether a column is dropped twice on rootserver
              //So don't return error here
              ret = OB_SUCCESS;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop the same column twice");
            }
          }
          if (OB_FAIL(ret)) {
            SQL_RESV_LOG(WARN, "set col_key to hash set failed", K(ret), K(column_name));
          }
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::fill_column_schema_according_stat(const ObColumnResolveStat &stat,
                                                            AlterColumnSchema &alter_column_schema)
{
  int ret = OB_SUCCESS;
  bool explicit_value = false;
  if (OB_UNLIKELY(NULL == session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info is NULL", K(ret));
  } else if (ObTimestampType == alter_column_schema.get_data_type()) {
    if (OB_FAIL(session_info_->get_explicit_defaults_for_timestamp(explicit_value))) {
      LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
    } else if (!explicit_value && !alter_column_schema.is_generated_column()) {
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

int ObAlterTableResolver::check_column_definition_node(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || T_COLUMN_DEFINITION != node->type_ ||
      node->num_child_ < COLUMN_DEFINITION_NUM_CHILD ||
      OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0]) ||
      T_COLUMN_REF != node->children_[0]->type_ ||
      COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse node",K(ret), K(node->type_), K(node->num_child_));
  }
  return ret;
}

int ObAlterTableResolver::is_exist_item_type(const ParseNode &node,
                                             const ObItemType type,
                                             bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    ParseNode *action_node = node.children_[i];
    if (OB_ISNULL(action_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
    } else if (type == action_node->children_[0]->type_) {
      is_exist = true;
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_rename_column(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode() && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "mysql rename column in current version");
  } else if (T_COLUMN_RENAME != node.type_ || OB_ISNULL(node.children_) ||
      OB_ISNULL(node.children_[0]) || T_COLUMN_REF != node.children_[0]->type_ ||
      OB_ISNULL(node.children_[1]) || T_IDENT != node.children_[1]->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else {
    ObString new_column_name;
    AlterColumnSchema alter_column_schema;
    if (OB_FAIL(resolve_column_definition_ref(alter_column_schema, node.children_[0], true))) {
      LOG_WARN("resolve column definition ref failed", K(ret));
    } else if (OB_FAIL(resolve_column_name(new_column_name, node.children_[1]))) {
      LOG_WARN("failed to resolve new column name", K(ret));
    } else if (0 == new_column_name.case_compare(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, new_column_name.length(), new_column_name.ptr(),
                                             table_name_.length(), table_name_.ptr());
      LOG_WARN("invalid rowid column for rename stmt", K(ret));
    }
    const ObString origin_column_name = alter_column_schema.get_origin_column_name();
    const ObColumnSchemaV2 *origin_col_schema = NULL;
    ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(alter_table_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_column_schema(table_schema_->get_tenant_id(), table_schema_->get_table_id(),
                                                          origin_column_name,
                                                          origin_col_schema,
                                                          false))) {
      if (ret == OB_ERR_BAD_FIELD_ERROR) {
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                       origin_column_name.length(), origin_column_name.ptr(),
                       table_schema_->get_table_name_str().length(),
                       table_schema_->get_table_name_str().ptr());
      }
      SQL_RESV_LOG(WARN, "fail to get origin column schema", K(ret));
    } else if (lib::is_oracle_mode()
               && ObCharset::case_sensitive_equal(origin_column_name, new_column_name)) {
      //先拿原schema再判断node中colname重名的这一顺序是兼容oracle的.
      ret = OB_ERR_FIELD_SPECIFIED_TWICE;
      LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(origin_column_name));
    } else if (OB_FAIL(alter_column_schema.assign(*origin_col_schema))) {
      SQL_RESV_LOG(WARN, "fail to copy column schema", K(ret));
    } else if (OB_FAIL(alter_column_schema.set_origin_column_name(origin_column_name))) {
      SQL_RESV_LOG(WARN, "fail to set origin column name", K(origin_column_name), K(ret));
    } else if (OB_FAIL(alter_column_schema.set_column_name(new_column_name))) {
      SQL_RESV_LOG(WARN, "fail to set new column name", K(new_column_name), K(ret));
    } else if (lib::is_mysql_mode()
               && OB_FAIL(check_mysql_rename_column(alter_column_schema, *table_schema_,
                                                    *alter_table_stmt))) {
      LOG_WARN("check rename mysql columns failed", K(ret));
    } else {
      //rs端复用ddl_change_column
      alter_column_schema.alter_type_ = OB_DDL_CHANGE_COLUMN;
      if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
        SQL_RESV_LOG(WARN, "add alter column schema failed", K(ret));
      }
      LOG_DEBUG("rename column", KPC(origin_col_schema), K(alter_column_schema));
    }
  }
  return ret;
}

int ObAlterTableResolver::generate_index_arg_cascade()
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
  }
  if (OB_SUCC(ret) && alter_table_bitset_.has_member(obrpc::ObAlterTableArg::TABLESPACE_ID)
      && lib::is_mysql_mode()) {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
    if (alter_table_stmt->get_index_arg_list().count() != 0 ||
        alter_table_stmt->get_alter_index_arg_list().count() != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Alter index together with other DDLs not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter index together with other DDLs");
    } else if (OB_FAIL(table_schema_->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    } else if (simple_index_infos.count() > 0) {
      alter_table_stmt->set_alter_table_index();
    }
    for (int i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = NULL;
      ObString index_name;
      ObAlterIndexTablespaceArg *alter_index_tablespace_arg = NULL;
      void *tmp_ptr = NULL;
      if (OB_FAIL(schema_guard->get_table_schema(table_schema_->get_tenant_id(),
                  simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema not exist", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else if (OB_ISNULL(tmp_ptr = (ObAlterIndexTablespaceArg *)allocator_->alloc(sizeof(obrpc::ObAlterIndexTablespaceArg)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        alter_index_tablespace_arg = new (tmp_ptr)ObAlterIndexTablespaceArg();
        alter_index_tablespace_arg->tenant_id_ = tenant_id;
        alter_index_tablespace_arg->tablespace_id_ = tablespace_id_;
        if (OB_FAIL(ob_write_string(*allocator_, encryption_,
                                    alter_index_tablespace_arg->encryption_))) {
          LOG_WARN("deep copy tablespace encryption name failed", K(ret));
        } else if (OB_FAIL(ob_write_string(*allocator_, index_name,
                                           alter_index_tablespace_arg->index_name_))) {
          LOG_WARN("failed to deep copy index name", K(ret));
        } else if (OB_FAIL(alter_table_stmt->add_index_arg(alter_index_tablespace_arg))) {
          LOG_WARN("add index to alter_index_list failed!", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_modify_all_trigger(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard;
  bool is_enable = (T_ENABLE_CONSTRAINT == node.children_[0]->type_);
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  CK (OB_NOT_NULL(alter_table_stmt) && OB_NOT_NULL(schema_checker_) && OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(schema_guard = schema_checker_->get_schema_guard()));
  if (OB_SUCC(ret)) {
    alter_table_stmt->get_tg_arg().is_set_status_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_->get_trigger_list().count(); ++i) {
      ObTriggerInfo new_tg_arg;
      OX (new_tg_arg.set_is_enable(is_enable));
      OX (new_tg_arg.set_trigger_id(table_schema_->get_trigger_list().at(i)));
      OZ (alter_table_stmt->get_tg_arg().trigger_infos_.push_back(new_tg_arg));
      LOG_DEBUG("alter table all triggers", K(new_tg_arg.get_trigger_id()), K(ret));
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_column_group_for_column()
{
  int ret = OB_SUCCESS;
  bool is_normal_column_store_table = false;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  uint64_t compat_version = 0;
  const uint64_t tenant_id = table_schema_->get_tenant_id();
  if (OB_ISNULL(alter_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null alter_table_stmt", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_0_0) { //skip resolve cg
  } else if (table_schema_->get_column_group_count() > 0) {
    // TODO, wait to support table update from 4.1 or less
    ObColumnGroupSchema column_group;
    char cg_name[OB_MAX_COLUMN_GROUP_NAME_LENGTH];
    ObArray<uint64_t> column_ids;
    const share::schema::AlterTableSchema &alter_table_schema =
        alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    uint64_t cur_column_group_id = table_schema_->get_max_used_column_group_id();
    ObTableSchema::const_column_iterator tmp_begin = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator tmp_end = alter_table_schema.column_end();
    for (; OB_SUCC(ret) && (tmp_begin != tmp_end); tmp_begin++) {
      column_group.reset();
      AlterColumnSchema *column = static_cast<AlterColumnSchema *>(*tmp_begin);
      MEMSET(cg_name, '\0', OB_MAX_COLUMN_GROUP_NAME_LENGTH);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column should not be null", KR(ret));
      } else if (column->alter_type_ != OB_DDL_ADD_COLUMN) { // if not add_column, skip
      } else if (column->is_virtual_generated_column()) { // skip virtual column
      } else if (0 >= snprintf(cg_name, OB_MAX_COLUMN_GROUP_NAME_LENGTH, "%s_%s",
          OB_COLUMN_GROUP_NAME_PREFIX, column->get_column_name_str().ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to snprintf", K(ret), KPC(column));
      // real column_id will be generated later, thus column_ids is empty here
      } else if (OB_FAIL(build_column_group(*table_schema_, ObColumnGroupType::SINGLE_COLUMN_GROUP,
            cg_name, column_ids, ++cur_column_group_id, column_group))) {
        LOG_WARN("failed to build column group", K(ret), KPC(column));
      } else if (OB_FAIL(alter_table_stmt->add_column_group(column_group))) {
        // only used for duplicate column in sql
        if (OB_HASH_EXIST == ret) {
          ret = OB_ERR_COLUMN_DUPLICATE;
          LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column->get_column_name_str().length(), column->get_column_name_str().ptr());
          LOG_WARN("duplicate column name", K(ret), K(column));
        } else {
          LOG_WARN("failed to add column group", K(ret));
        }
      } else if (OB_FAIL(column->set_column_group_name(cg_name))) {
        LOG_WARN("failed to set column group name", K(ret), KPC(column));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::resolve_alter_column_groups(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObAlterTableStmt *alter_table_stmt = get_alter_table_stmt();
  if (OB_ISNULL(alter_table_stmt) || OB_UNLIKELY(T_ALTER_COLUMN_GROUP_OPTION != node.type_ ||
                                                 OB_ISNULL(node.children_))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "get alter table stmt failed", K(ret), K(node.type_), KP(node.children_));
  } else {
    const ParseNode *column_group_node = node.children_[0];
    uint64_t compat_version = 0;

    ObAlterTableArg &alter_table_arg = alter_table_stmt->get_alter_table_arg();
    share::schema::AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    const uint64_t column_cnt = table_schema_->get_column_count();
    const uint64_t tenant_id  = table_schema_->get_tenant_id();

    if (OB_ISNULL(column_group_node) || column_group_node->num_child_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree, column group node is null or have no children!",
                   K(ret), KP(column_group_node));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      SQL_RESV_LOG(WARN, "fail to get min data version", K(ret));
    } else if (compat_version < DATA_VERSION_4_3_0_0) {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "data_version not support for altering column group", K(ret), K(compat_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3, alter column group");
    } else if (!need_column_group(*table_schema_)) {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "table don't support alter column group", K(ret));
    } else {
      alter_table_schema.set_column_store(true);
      if (column_group_node->type_ == T_COLUMN_GROUP_ADD) {
        alter_table_stmt->get_alter_table_arg().alter_table_schema_.alter_type_ = OB_DDL_ADD_COLUMN_GROUP;
      } else if (column_group_node->type_ == T_COLUMN_GROUP_DROP) {
        alter_table_stmt->get_alter_table_arg().alter_table_schema_.alter_type_ = OB_DDL_DROP_COLUMN_GROUP;
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree ", K(ret), K(column_group_node->type_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(parse_column_group(column_group_node, *table_schema_, alter_table_schema))) {
        LOG_WARN("fail to parse column gorup list", K(ret));
      }
    }
  }
  return ret;
}

int ObAlterTableResolver::check_mysql_rename_column(const AlterColumnSchema &alter_column_schema,
                                                    const ObTableSchema &origin_table_schema,
                                                    ObAlterTableStmt &alter_table_stmt)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = nullptr;
  const ObPartitionOption &part_opt = origin_table_schema.get_part_option();
  // check table part key deps
  if (alter_column_schema.is_tbl_part_key_column()
      && part_opt.get_part_func_type() != PARTITION_FUNC_TYPE_KEY_IMPLICIT) {
    ret = OB_ERR_DEPENDENT_BY_PARTITION_FUNC;
    LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_PARTITION_FUNC,
                   alter_column_schema.get_origin_column_name().length(),
                   alter_column_schema.get_origin_column_name().ptr());
    LOG_WARN("alter column has table part key deps", K(ret), K(alter_column_schema));
  }
  // generated column deps
  for (auto col_iter = origin_table_schema.column_begin();
       OB_SUCC(ret) && (col_iter != origin_table_schema.column_end()); col_iter++) {
    if (OB_ISNULL(column = *col_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null *col_iter", K(ret));
    } else if (column->has_cascaded_column_id(alter_column_schema.get_column_id())
               && column->is_generated_column()) {
      if (!column->is_hidden()) {
        ret = OB_ERR_DEPENDENT_BY_GENERATED_COLUMN;
        LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_GENERATED_COLUMN,
                       alter_column_schema.get_origin_column_name().length(),
                       alter_column_schema.get_origin_column_name().ptr());
        LOG_WARN("alter column has generated column deps", K(ret), K(alter_column_schema));
      } else if (column->is_func_idx_column()) { // renname column with func index deps is forbidden
        ret = OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX;
        LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
                       alter_column_schema.get_origin_column_name().length(),
                       alter_column_schema.get_origin_column_name().ptr());
        LOG_WARN("alter column has function index deps", K(ret), K(alter_column_schema));
      }
    }
  }

  // constraints deps
  for (auto cst_iter = origin_table_schema.constraint_begin();
       OB_SUCC(ret) && (cst_iter != origin_table_schema.constraint_end()); cst_iter++) {
    if ((*cst_iter)->get_column_cnt() == 0
        || (*cst_iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
      continue;
    } else {
      bool dropped_cst = false;
      AlterTableSchema &alter_table_schema = alter_table_stmt.get_alter_table_arg().alter_table_schema_;
      for (auto it = alter_table_schema.constraint_begin();
           OB_SUCC(ret)
           && !dropped_cst
           && (alter_table_stmt.get_alter_table_arg().alter_constraint_type_
                == ObAlterTableArg::DROP_CONSTRAINT)
           && it != alter_table_schema.constraint_end();
           it++) {
        if ((*it)->get_constraint_id() == (*cst_iter)->get_constraint_id()) {
          dropped_cst = true;
        }
      }
      for (auto it = (*cst_iter)->cst_col_begin();
           !dropped_cst && OB_SUCC(ret) && it != (*cst_iter)->cst_col_end(); it++) {
        if (*it == alter_column_schema.get_column_id()) {
          ret = OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT;
          LOG_USER_ERROR(OB_ERR_DROP_COL_REFERENCED_MULTI_COLS_CONSTRAINT,
                         (*cst_iter)->get_constraint_name_str().length(),
                         (*cst_iter)->get_constraint_name_str().ptr(),
                         alter_column_schema.get_origin_column_name().length(),
                         alter_column_schema.get_origin_column_name().ptr());
          LOG_WARN("column has contraint deps", K(ret), K(alter_column_schema));
        }
      }
    }
  }
  return ret;
}

/*
* chech the droped column is partition key,
* if ture, forbid the action
*/
int ObAlterTableResolver::check_drop_column_is_partition_key(const ObTableSchema &table_schema, const ObString &column_name)
{
  int ret=OB_SUCCESS;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid arguemnt", K(ret), K(table_schema));
  } else {
    const ObColumnSchemaV2 *origin_column = table_schema.get_column_schema(column_name);
    if (OB_ISNULL(origin_column)) {
      // do nothing
      // 根据列名查不到列是因为表中不存在该列，后面会在 RS 端再检查一遍表中是否存在该列，并在 RS 端根据操作的不同报不同的错误
    } else if (origin_column->is_tbl_part_key_column()){
      ret = OB_ERR_DEPENDENT_BY_PARTITION_FUNC;
      LOG_USER_ERROR(OB_ERR_DEPENDENT_BY_PARTITION_FUNC,
                     column_name.length(),
                     column_name.ptr());
      LOG_WARN("alter column has table part key deps", K(ret), K(origin_column));
    }
  }
  return ret;
}

int ObAlterTableResolver::add_new_indexkey_for_oracle_temp_table(obrpc::ObCreateIndexArg &index_arg)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_schema_) && table_schema_->is_oracle_tmp_table()) {
    ObColumnSortItem sort_item;
    sort_item.column_name_.assign_ptr(OB_HIDDEN_SESSION_ID_COLUMN_NAME,
                                      static_cast<int32_t>(strlen(OB_HIDDEN_SESSION_ID_COLUMN_NAME)));
    sort_item.prefix_len_ = 0;
    sort_item.order_type_ = common::ObOrderType::ASC;
    if (OB_FAIL(add_sort_column(sort_item, index_arg))) {
      SQL_RESV_LOG(WARN, "add sort column failed", K(ret), K(sort_item));
    } else {
      LOG_DEBUG("add __session_id as first index key succeed", K(sort_item));
    }
  }
  return ret;
}

} //namespace common
} //namespace oceanbase
