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
#include "sql/resolver/ddl/ob_create_table_resolver_base.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace omt;
namespace sql
{
ObCreateTableResolverBase::ObCreateTableResolverBase(ObResolverParams &params)
    : ObDDLResolver(params),
      cur_column_group_id_(COLUMN_GROUP_START_ID)
{
}

ObCreateTableResolverBase::~ObCreateTableResolverBase()
{
}

int ObCreateTableResolverBase::resolve_partition_option(
    ParseNode *node, ObTableSchema &table_schema, const bool is_partition_option_node_with_opt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "failed to build partition key info!", KR(ret), KP(session_info_));
  } else {
    if (NULL != node) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret), K(session_info_->get_effective_tenant_id()));
      } else if (tenant_data_version < DATA_VERSION_4_3_1_0) {
        if (table_schema.is_external_table()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("partition ext table is not supported in data version less than 4.3.1", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition external table in data version less than 4.3.1");
        }
      }
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt *>(stmt_);
      if (OB_FAIL(ret)) {
      } else if (!is_partition_option_node_with_opt) {
        if (OB_FAIL(resolve_partition_node(create_table_stmt, node, table_schema))) {
          LOG_WARN("failed to resolve partition option", KR(ret));
        }
      } else if (T_PARTITION_OPTION == node->type_) {
        if (node->num_child_ < 1 || node->num_child_ > 2) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "node number is invalid.", KR(ret), K(node->num_child_));
        } else if (NULL == node->children_[0]) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "partition node is null.", KR(ret));
        } else {
          ParseNode *partition_node = node->children_[0]; // 普通分区partition node
          if (OB_FAIL(resolve_partition_node(create_table_stmt, partition_node, table_schema))) {
            LOG_WARN("failed to resolve partition option", KR(ret));
          }
        }
        /*  vertical partition is not support in 4.x, remove its code here */
      } else {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "node type is invalid.", KR(ret), K(node->type_));
      }
    } else if (table_schema.is_external_table() && table_schema.is_user_specified_partition_for_external_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "USER SPECIFIED PARTITION TYPE for non partitioned external table");
      LOG_WARN("USER SPECIFIED PARTITION TYPE for non partitioned external table not supported");
    }
  }
  return ret;
}

int ObCreateTableResolverBase::set_table_option_to_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session_info is null.", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    bool is_oracle_mode = lib::is_oracle_mode();
    table_schema.set_block_size(block_size_);
    int64_t progressive_merge_round = 0;
    int64_t tablet_size = tablet_size_;
    if (-1 == tablet_size) {
      tablet_size = common::ObServerConfig::get_instance().tablet_size;
    }
    table_schema.set_tablet_size(tablet_size);
    table_schema.set_pctfree(pctfree_);
    table_schema.set_collation_type(collation_type_);
    table_schema.set_charset_type(charset_type_);
    table_schema.set_is_use_bloomfilter(use_bloom_filter_);
    table_schema.set_auto_increment(auto_increment_);
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_tablegroup_id(OB_INVALID_ID);
    table_schema.set_table_id(table_id_);
    table_schema.set_read_only(read_only_);
    table_schema.set_duplicate_attribute(duplicate_scope_, duplicate_read_consistency_);
    table_schema.set_enable_row_movement(enable_row_movement_);
    table_schema.set_table_mode_struct(table_mode_);
    table_schema.set_encryption_str(encryption_);
    table_schema.set_tablespace_id(tablespace_id_);
    table_schema.set_dop(table_dop_);
    if (0 == progressive_merge_num_) {
      ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      table_schema.set_progressive_merge_num(tenant_config.is_valid() ? tenant_config->default_progressive_merge_num : 0);
    } else {
      table_schema.set_progressive_merge_num(progressive_merge_num_);
    }
    // set store format
    if (store_format_ == OB_STORE_FORMAT_INVALID) {
      ObString default_format;
      if (is_oracle_mode) {
        if (OB_ISNULL(GCONF.default_compress.get_value())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default oracle compress option is not set in server config", K(ret));
        } else {
          default_format = ObString::make_string(GCONF.default_compress.str());
        }
      } else {
        if (NULL == GCONF.default_row_format.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default row format is not set in server config", K(ret));
        } else {
          default_format = ObString::make_string(GCONF.default_row_format.str());
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL((ObStoreFormat::find_store_format_type(default_format, store_format_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default compress not found!", K(ret), K_(store_format), K(default_format));
        } else if (!ObStoreFormat::is_store_format_valid(store_format_, is_oracle_mode)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected store format type", K_(store_format), K(is_oracle_mode), K(ret));
        } else if (OB_FAIL(ObDDLResolver::get_row_store_type(tenant_id, store_format_, row_store_type_))) {
          LOG_WARN("fail to get_row_store_type", K(ret), K(tenant_id), K(store_format_));
        }
      }
    } else if (OB_FAIL(ObDDLResolver::get_row_store_type(tenant_id, store_format_, row_store_type_))) {
      LOG_WARN("fail to get_row_store_type", K(ret),  K(tenant_id), K(store_format_), K(is_oracle_mode));
    }

    if (OB_SUCC(ret)) {
      if (0 == progressive_merge_round) {
        progressive_merge_round = 1;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_STORAGE_FORMAT_VERSION_INVALID == storage_format_version_) {
        storage_format_version_ = OB_STORAGE_FORMAT_VERSION_V4;
      }
    }

    // set compress method
    if (OB_SUCC(ret)) {
      if (is_oracle_mode) {
        const char* compress_name = NULL;
        if (OB_ISNULL(compress_name = ObStoreFormat::get_store_format_compress_name(store_format_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null compress name", K_(store_format), K(ret));
        } else {
          compress_method_ = ObString::make_string(compress_name);
        }
      } else if (compress_method_.empty()) {
        char compress_func_str[OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH] = "";
        if (NULL == GCONF.default_compress_func.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default compress func name is not set in server config", K(ret));
        } else if (OB_FAIL(GCONF.default_compress_func.copy(compress_func_str, sizeof(compress_func_str)))) {
          LOG_WARN("Failed to copy default compress func", K(ret));
        } else {
          bool found = false;
          for (int i = 0; i < ARRAYSIZEOF(common::compress_funcs) && !found; ++i) {
            //find again in case of case sensitive in server init parameters
            //all change to
            if (0 == ObString::make_string(common::compress_funcs[i]).case_compare(compress_func_str)) {
              found = true;
              compress_method_ = ObString::make_string(common::compress_funcs[i]);
            }
          }
          if (!found) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("compress method not found!", K(ret), K_(compress_method),
                "default_compress_func", compress_func_str);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (compress_method_ == all_compressor_name[ZLIB_COMPRESSOR]) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Not allowed to use zlib compressor!", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "zlib compressor");
      }
    }

    if (OB_SUCC(ret)) {
      table_schema.set_row_store_type(row_store_type_);
      table_schema.set_store_format(store_format_);
      table_schema.set_progressive_merge_round(progressive_merge_round);
      table_schema.set_storage_format_version(storage_format_version_);
      if (OB_FAIL(table_schema.set_expire_info(expire_info_)) ||
          OB_FAIL(table_schema.set_compress_func_name(compress_method_)) ||
          OB_FAIL(table_schema.set_comment(comment_)) ||
          OB_FAIL(table_schema.set_tablegroup_name(tablegroup_name_)) ||
          OB_FAIL(table_schema.set_ttl_definition(ttl_definition_)) ||
          OB_FAIL(table_schema.set_kv_attributes(kv_attributes_))) {
        SQL_RESV_LOG(WARN, "set table_options failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && table_schema.get_compressor_type() == ObCompressorType::ZLIB_LITE_COMPRESSOR) {
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

    if (OB_SUCC(ret)) {
      // if lob_inrow_threshold not set, used config default_lob_inrow_threshold
      uint64_t tenant_data_version = 0;
      if (is_set_lob_inrow_threshold_) {
        table_schema.set_lob_inrow_threshold(lob_inrow_threshold_);
      } else if (OB_ISNULL(session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session if NULL", K(ret));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_2_1_2){
        // lob_inrow_threshold is added in 421 bp2
        // so need ensure lob_inrow_threshold is 4096 before 421 bp2 for compat
        lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
        table_schema.set_lob_inrow_threshold(lob_inrow_threshold_);
      } else if (OB_FALSE_IT((lob_inrow_threshold_ = session_info_->get_default_lob_inrow_threshold()))) {
      } else if (lob_inrow_threshold_ < OB_MIN_LOB_INROW_THRESHOLD || lob_inrow_threshold_ > OB_MAX_LOB_INROW_THRESHOLD) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(ERROR, "invalid inrow threshold", K(ret), K(lob_inrow_threshold_));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid inrow threshold");
      } else {
        table_schema.set_lob_inrow_threshold(lob_inrow_threshold_);
      }
    }
    if (OB_SUCC(ret) && table_schema.is_external_table()) {
      if ((table_schema.get_external_file_format().empty()
          || table_schema.get_external_file_location().empty()) &&
           table_schema.get_external_properties().empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Default properties or format or location option for external table");
      }
    }
    if (OB_SUCC(ret) && auto_increment_cache_size_ != 0) {
      table_schema.set_auto_increment_cache_size(auto_increment_cache_size_);
    }
  }
  return ret;
}

int ObCreateTableResolverBase::add_primary_key_part(const ObString &column_name,
                                                    ObTableSchema &table_schema,
                                                    const int64_t cur_rowkey_size,
                                                    int64_t &pk_data_length,
                                                    ObColumnSchemaV2 *&col)
{
  int ret = OB_SUCCESS;
  col = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  int64_t length = 0;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "session is null", KP(session_info_), K(ret));
  } else if (static_cast<int64_t>(table_id_) > 0
             && OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
             session_info_->get_effective_tenant_id(), table_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(table_id));
  } else if (OB_ISNULL(col = table_schema.get_column_schema(column_name))) {
    ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
    LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
    SQL_RESV_LOG(WARN, "column does not exists", K(ret), K(column_name));
  } else if (OB_FAIL(check_add_column_as_pk_allowed(*col))) {
    LOG_WARN("the column can not be primary key", K(ret));
  } else if (col->get_rowkey_position() > 0) {
    ret = OB_ERR_COLUMN_DUPLICATE;
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
  } else if (OB_USER_MAX_ROWKEY_COLUMN_NUMBER == cur_rowkey_size) {
    ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
  } else if (OB_FALSE_IT(col->set_nullable(false))
             || OB_FALSE_IT(col->set_rowkey_position(cur_rowkey_size + 1))) {
  } else if (OB_FAIL(table_schema.set_rowkey_info(*col))) {
    LOG_WARN("failed to set rowkey info", K(ret));
  } else if (!col->is_string_type()) {
    /* do nothing */
  } else if (OB_FAIL(col->get_byte_length(length, is_oracle_mode, false))) {
    SQL_RESV_LOG(WARN, "fail to get byte length of column", KR(ret), K(is_oracle_mode));
  } else if (length <= 0) {
    ret = OB_ERR_WRONG_KEY_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
  } else {
    if (col->is_string_lob()) {
      length = 0;
    }
    if ((pk_data_length += length) > OB_MAX_USER_ROW_KEY_LENGTH) {
      ret = OB_ERR_TOO_LONG_KEY_LENGTH;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_USER_ROW_KEY_LENGTH);
    }
  }
  return ret;
}

uint64_t ObCreateTableResolverBase::gen_column_group_id()
{
  return ++cur_column_group_id_;
}

int ObCreateTableResolverBase::resolve_column_group_helper(const ParseNode *cg_node,
                                                            ObTableSchema &table_schema)
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  ObArray<uint64_t> column_ids; // not include virtual column
  uint64_t compat_version = 0;
  ObTableStoreType table_store_type = OB_TABLE_STORE_INVALID;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const int64_t column_cnt = table_schema.get_column_count();
  if (OB_FAIL(column_ids.reserve(column_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(column_cnt));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (!(compat_version >= DATA_VERSION_4_3_0_0)) {
      if (OB_NOT_NULL(cg_node) && (T_COLUMN_GROUP == cg_node->type_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't support column store if version less than 4_1_0_0", KR(ret), K(compat_version));
      }
  } else {
    table_schema.set_column_store(true);
    bool is_each_cg_exist = false;
    if (OB_NOT_NULL(cg_node)) {
      if (!is_column_group_supported()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("column group is not enabled", KR(ret));
      } else if (OB_FAIL(parse_column_group(cg_node, table_schema, table_schema))) {
        LOG_WARN("fail to parse column group", K(ret));
      }
    }

    /* build column group when cg node is null && tenant cg valid*/
    ObTenantConfigGuard tenant_config(TENANT_CONF(session_info_->get_effective_tenant_id()));
    if (OB_FAIL(ret)) {
    } else if ( OB_LIKELY(tenant_config.is_valid()) && nullptr == cg_node) {
      /* force to build each cg*/
      if (!ObSchemaUtils::can_add_column_group(table_schema)) {
      } else if (OB_FAIL(ObTableStoreFormat::find_table_store_type(
                  tenant_config->default_table_store_format.get_value_string(),
                  table_store_type))) {
        LOG_WARN("fail to get table store format", K(ret), K(table_store_type));
      } else if (ObTableStoreFormat::is_with_column(table_store_type)) {
        /* for default is column store, must add each column group*/
        if (OB_FAIL(ObSchemaUtils::build_add_each_column_group(table_schema, table_schema))) {
          LOG_WARN("fail to add each column group", K(ret));
        }
      }

      /* force to build all cg*/
      ObColumnGroupSchema all_cg;
      uint64_t all_cg_id = ALL_COLUMN_GROUP_ID;
#ifdef ERRSIM
      tmp_ret = OB_E(EventTable::EN_DDL_CREATE_OLD_VERSION_COLUMN_GROUP) OB_SUCCESS;
      if (OB_TMP_FAIL(tmp_ret)) {
        all_cg_id = table_schema.get_max_used_column_group_id() + 1;
      }
#endif
      if (OB_FAIL(ret)) {
      } else if (!ObSchemaUtils::can_add_column_group(table_schema)) {
      } else if (ObTableStoreFormat::is_row_with_column_store(table_store_type)) {
        if (OB_FAIL(ObSchemaUtils::build_all_column_group(table_schema, table_schema.get_tenant_id(),
                                                                  all_cg_id, all_cg))) {
          LOG_WARN("fail to add all column group", K(ret));
        } else if (OB_FAIL(table_schema.add_column_group(all_cg))) {
          LOG_WARN("fail to build all column group", K(ret));
        }
      }
    }

    // add default_type column_group, build a empty and then use alter_deafult_cg
    if (OB_SUCC(ret)) {
      ObColumnGroupSchema tmp_cg;
      column_ids.reuse();
      if (OB_FAIL(build_column_group(table_schema, ObColumnGroupType::DEFAULT_COLUMN_GROUP,
          OB_DEFAULT_COLUMN_GROUP_NAME, column_ids, DEFAULT_TYPE_COLUMN_GROUP_ID, tmp_cg))) {
        LOG_WARN("fail to build default type column_group", KR(ret), K(table_store_type),
                  "table_id", table_schema.get_table_id());
      } else if (OB_FAIL(table_schema.add_column_group(tmp_cg))) {
        LOG_WARN("fail to add default column group", KR(ret), "table_id", table_schema.get_table_id());
      } else if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(table_schema))) {
        LOG_WARN("fail to adjust rowkey column group when add column group", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(table_schema))) {
        LOG_WARN("fail to adjust default column group", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_SUCCESS == tmp_ret) {
      if (OB_FAIL(table_schema.adjust_column_group_array())) {
        LOG_WARN("fail to adjust column group array", K(ret), K(table_schema));
      }
    }
  }
  return ret;
}
/*
* only when default columns store is column_store
* have to add each column group
*/
int ObCreateTableResolverBase::resolve_column_group(const ParseNode *cg_node)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt *>(stmt_);

  if (OB_ISNULL(create_table_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "create_table_stmt should not be null", KR(ret));
  } else {
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    if (OB_FAIL(resolve_column_group_helper(cg_node, table_schema))) {
      LOG_WARN("fail to resolve column group helper", KR(ret));
    }
  }
  return ret;
}

int ObCreateTableResolverBase::resolve_table_organization(omt::ObTenantConfigGuard &tenant_config, ParseNode *node)
{
  int ret = OB_SUCCESS;
  // get the table organization from the tenant config
  if (OB_LIKELY(tenant_config.is_valid())) {
    const char *ptr = NULL;
    if (OB_ISNULL(ptr = tenant_config->default_table_organization.get_value())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default organization ptr is null", K(ret));
    } else {
      table_organization_ =
        (0 == ObString::make_string("HEAP").case_compare(ptr)) ?
          ObTableOrganizationType::OB_HEAP_ORGANIZATION : ObTableOrganizationType::OB_INDEX_ORGANIZATION;
    }
  }

  // get the table organization from the table options
  if (OB_FAIL(ret)) {
  } else if (NULL != node) {
    ParseNode *option_node = NULL;
    int32_t num = 0;
    if (T_TABLE_OPTION_LIST != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse node", KR(ret), K(node->type_), K(node->num_child_));
    } else {
      num = node->num_child_;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (OB_ISNULL(option_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node is null", K(ret));
      } else if (T_ORGANIZATION == option_node->type_) {
        if (lib::is_oracle_mode()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("oracle mode should not specify organization type", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify organization type in oracle mode");
        } else if (stmt_->get_stmt_type() == stmt::T_CREATE_TABLE) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            if (T_ORGANIZATION_HEAP == option_node->children_[0]->type_) {
              table_organization_ = ObTableOrganizationType::OB_HEAP_ORGANIZATION;
            } else if (T_ORGANIZATION_INDEX == option_node->children_[0]->type_) {
              table_organization_ = ObTableOrganizationType::OB_INDEX_ORGANIZATION;
            }
          }
        } else if (stmt_->get_stmt_type() == stmt::T_ALTER_TABLE) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter table statement should not specify organization type", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify organization type in alter table query");
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  return ret;
}
}//end namespace sql
}//end namespace oceanbase
