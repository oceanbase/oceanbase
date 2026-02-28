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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_load_inner_table_schema.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
enum class ObHardCodeInnerTableSchemaVersion : int64_t { //FARM COMPAT WHITELIST
  HARD_CODE_SCHEMA_VERSION_BEGIN = 131072,
  MAX_HARD_CODE_SCHEMA_VERSION,
};
int64_t get_hard_code_schema_count()
{
  return static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::MAX_HARD_CODE_SCHEMA_VERSION) -
    static_cast<int64_t>(ObHardCodeInnerTableSchemaVersion::HARD_CODE_SCHEMA_VERSION_BEGIN) - 1;
}

uint64_t ObLoadInnerTableSchemaInfo::get_checksum(const char *row, const uint64_t table_id)
{
  return ob_crc64(row, strlen(row)) ^ table_id;
}
int ObLoadInnerTableSchemaInfo::get_checksum(const int64_t idx, uint64_t &checksum) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of range", KR(ret), K(idx), K(row_count_));
  } else {
    checksum = row_checksum_[idx];
  }
  return ret;
}
int ObLoadInnerTableSchemaInfo::check_row_valid_(const int64_t idx) const
{
  int ret = OB_SUCCESS;
  uint64_t checksum = 0;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is out of range", KR(ret), K(idx), K(row_count_));
  } else if (FALSE_IT(checksum = get_checksum(inner_table_rows_[idx], inner_table_table_ids_[idx]))) {
  } else if (checksum != row_checksum_[idx]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checksum is not equal, may be observer binary is broken", KR(ret), K(idx), K(checksum),
        K(row_checksum_[idx]), K(inner_table_table_ids_[idx]), K(inner_table_rows_[idx]));
  }
  return ret;
}
int ObLoadInnerTableSchemaInfo::get_row(const int64_t idx, const char *&row, uint64_t &table_id) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is out of range", KR(ret), K(idx), K(row_count_));
  } else if (OB_FAIL(check_row_valid_(idx))) {
    LOG_WARN("failed to check row valid", KR(ret), K(idx));
  } else {
    row = inner_table_rows_[idx];
    table_id = inner_table_table_ids_[idx];
  }
  return ret;
}
int get_hard_code_schema_version_mapping(const uint64_t table_id, int64_t &schema_version)
{
  int ret = OB_SUCCESS;  return ret;
}
const int64_t INNER_TABLE_SYS_SCHEMA_VERSION = 0;
const int64_t INNER_TABLE_CORE_SCHEMA_VERSION = 0;
const char * INNER_TABLE_ALL_CORE_TABLE_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_CORE_TABLE_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_CORE_TABLE_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_CORE_TABLE_HEADER = "table_name,row_id,column_name,column_value";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_CORE_TABLE_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_CORE_TABLE_TABLE_IDS), "ALL_CORE_TABLE header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_CORE_TABLE_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_CORE_TABLE_CHECKSUMS), "ALL_CORE_TABLE header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_CORE_TABLE_LOAD_INFO(
  OB_ALL_CORE_TABLE_TID,
  OB_ALL_CORE_TABLE_TNAME,
  INNER_TABLE_ALL_CORE_TABLE_HEADER,
  INNER_TABLE_ALL_CORE_TABLE_TABLE_IDS,
  INNER_TABLE_ALL_CORE_TABLE_ROWS,
  INNER_TABLE_ALL_CORE_TABLE_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_CORE_TABLE_ROWS)
);
const char * INNER_TABLE_ALL_TABLE_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_TABLE_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_TABLE_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_TABLE_HEADER = "tenant_id,table_id,table_name,database_id,table_type,load_type,def_type,rowkey_column_num,index_column_num,max_used_column_id,session_id,sess_active_time,tablet_size,pctfree,autoinc_column_id,auto_increment,read_only,rowkey_split_pos,compress_func_name,expire_condition,is_use_bloomfilter,index_attributes_set,comment,block_size,collation_type,data_table_id,index_status,tablegroup_id,progressive_merge_num,index_type,index_using_type,part_level,part_func_type,part_func_expr,part_num,sub_part_func_type,sub_part_func_expr,sub_part_num,view_definition,view_check_option,view_is_updatable,parser_name,partition_status,partition_schema_version,pk_comment,row_store_type,store_format,duplicate_scope,progressive_merge_round,storage_format_version,table_mode,encryption,tablespace_id,sub_part_template_flags,dop,character_set_client,collation_connection,auto_part,auto_part_size,association_table_id,define_user_id,max_dependency_version,tablet_id,object_status,table_flags,truncate_version,external_file_location,external_file_location_access_info,external_file_format,external_file_pattern,ttl_definition,kv_attributes,name_generated_type,lob_inrow_threshold,max_used_column_group_id,column_store,auto_increment_cache_size,duplicate_read_consistency,mv_mode,external_properties,index_params,micro_index_clustered,local_session_vars,parser_properties,enable_macro_block_bloom_filter,storage_cache_policy,semistruct_encoding_type,dynamic_partition_policy,merge_engine_type";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_TABLE_IDS), "ALL_TABLE header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_CHECKSUMS), "ALL_TABLE header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_TABLE_LOAD_INFO(
  OB_ALL_TABLE_TID,
  OB_ALL_TABLE_TNAME,
  INNER_TABLE_ALL_TABLE_HEADER,
  INNER_TABLE_ALL_TABLE_TABLE_IDS,
  INNER_TABLE_ALL_TABLE_ROWS,
  INNER_TABLE_ALL_TABLE_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_ROWS)
);
const char * INNER_TABLE_ALL_TABLE_HISTORY_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_TABLE_HISTORY_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_TABLE_HISTORY_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_TABLE_HISTORY_HEADER = "tenant_id,table_id,table_name,database_id,table_type,load_type,def_type,rowkey_column_num,index_column_num,max_used_column_id,session_id,sess_active_time,tablet_size,pctfree,autoinc_column_id,auto_increment,read_only,rowkey_split_pos,compress_func_name,expire_condition,is_use_bloomfilter,index_attributes_set,comment,block_size,collation_type,data_table_id,index_status,tablegroup_id,progressive_merge_num,index_type,index_using_type,part_level,part_func_type,part_func_expr,part_num,sub_part_func_type,sub_part_func_expr,sub_part_num,view_definition,view_check_option,view_is_updatable,parser_name,partition_status,partition_schema_version,pk_comment,row_store_type,store_format,duplicate_scope,progressive_merge_round,storage_format_version,table_mode,encryption,tablespace_id,sub_part_template_flags,dop,character_set_client,collation_connection,auto_part,auto_part_size,association_table_id,define_user_id,max_dependency_version,tablet_id,object_status,table_flags,truncate_version,external_file_location,external_file_location_access_info,external_file_format,external_file_pattern,ttl_definition,kv_attributes,name_generated_type,lob_inrow_threshold,max_used_column_group_id,column_store,auto_increment_cache_size,duplicate_read_consistency,mv_mode,external_properties,index_params,micro_index_clustered,local_session_vars,parser_properties,enable_macro_block_bloom_filter,storage_cache_policy,semistruct_encoding_type,dynamic_partition_policy,merge_engine_type,is_deleted";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_HISTORY_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_HISTORY_TABLE_IDS), "ALL_TABLE_HISTORY header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_HISTORY_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_HISTORY_CHECKSUMS), "ALL_TABLE_HISTORY header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_TABLE_HISTORY_LOAD_INFO(
  OB_ALL_TABLE_HISTORY_TID,
  OB_ALL_TABLE_HISTORY_TNAME,
  INNER_TABLE_ALL_TABLE_HISTORY_HEADER,
  INNER_TABLE_ALL_TABLE_HISTORY_TABLE_IDS,
  INNER_TABLE_ALL_TABLE_HISTORY_ROWS,
  INNER_TABLE_ALL_TABLE_HISTORY_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_TABLE_HISTORY_ROWS)
);
const char * INNER_TABLE_ALL_COLUMN_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_COLUMN_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_COLUMN_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_COLUMN_HEADER = "tenant_id,table_id,column_id,column_name,rowkey_position,index_position,partition_key_position,data_type,data_length,data_precision,data_scale,zero_fill,nullable,autoincrement,is_hidden,on_update_current_timestamp,orig_default_value_v2,cur_default_value_v2,cur_default_value,order_in_rowkey,collation_type,comment,column_flags,extended_type_info,prev_column_id,srs_id,udt_set_id,sub_data_type,skip_index_attr,lob_chunk_size,local_session_vars,gmt_create,gmt_modified";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_TABLE_IDS), "ALL_COLUMN header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_CHECKSUMS), "ALL_COLUMN header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_COLUMN_LOAD_INFO(
  OB_ALL_COLUMN_TID,
  OB_ALL_COLUMN_TNAME,
  INNER_TABLE_ALL_COLUMN_HEADER,
  INNER_TABLE_ALL_COLUMN_TABLE_IDS,
  INNER_TABLE_ALL_COLUMN_ROWS,
  INNER_TABLE_ALL_COLUMN_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_ROWS)
);
const char * INNER_TABLE_ALL_COLUMN_HISTORY_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_COLUMN_HISTORY_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_COLUMN_HISTORY_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_COLUMN_HISTORY_HEADER = "tenant_id,table_id,column_id,column_name,rowkey_position,index_position,partition_key_position,data_type,data_length,data_precision,data_scale,zero_fill,nullable,autoincrement,is_hidden,on_update_current_timestamp,orig_default_value_v2,cur_default_value_v2,cur_default_value,order_in_rowkey,collation_type,comment,column_flags,extended_type_info,prev_column_id,srs_id,udt_set_id,sub_data_type,skip_index_attr,lob_chunk_size,local_session_vars,gmt_create,gmt_modified,is_deleted";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_HISTORY_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_HISTORY_TABLE_IDS), "ALL_COLUMN_HISTORY header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_HISTORY_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_HISTORY_CHECKSUMS), "ALL_COLUMN_HISTORY header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_COLUMN_HISTORY_LOAD_INFO(
  OB_ALL_COLUMN_HISTORY_TID,
  OB_ALL_COLUMN_HISTORY_TNAME,
  INNER_TABLE_ALL_COLUMN_HISTORY_HEADER,
  INNER_TABLE_ALL_COLUMN_HISTORY_TABLE_IDS,
  INNER_TABLE_ALL_COLUMN_HISTORY_ROWS,
  INNER_TABLE_ALL_COLUMN_HISTORY_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_COLUMN_HISTORY_ROWS)
);
const char * INNER_TABLE_ALL_DDL_OPERATION_ROWS[] = {
};
const uint64_t INNER_TABLE_ALL_DDL_OPERATION_TABLE_IDS[] = {
};
const uint64_t INNER_TABLE_ALL_DDL_OPERATION_CHECKSUMS[] = {
};
const char * INNER_TABLE_ALL_DDL_OPERATION_HEADER = "gmt_create,gmt_modified,tenant_id,user_id,database_id,database_name,tablegroup_id,table_id,table_name,operation_type,ddl_stmt_str,exec_tenant_id";
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_DDL_OPERATION_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_DDL_OPERATION_TABLE_IDS), "ALL_DDL_OPERATION header and table_ids length is not equal");
static_assert(ARRAYSIZEOF(INNER_TABLE_ALL_DDL_OPERATION_ROWS) == ARRAYSIZEOF(INNER_TABLE_ALL_DDL_OPERATION_CHECKSUMS), "ALL_DDL_OPERATION header and table_ids length is not equal");
const ObLoadInnerTableSchemaInfo ALL_DDL_OPERATION_LOAD_INFO(
  OB_ALL_DDL_OPERATION_TID,
  OB_ALL_DDL_OPERATION_TNAME,
  INNER_TABLE_ALL_DDL_OPERATION_HEADER,
  INNER_TABLE_ALL_DDL_OPERATION_TABLE_IDS,
  INNER_TABLE_ALL_DDL_OPERATION_ROWS,
  INNER_TABLE_ALL_DDL_OPERATION_CHECKSUMS,
  ARRAYSIZEOF(INNER_TABLE_ALL_DDL_OPERATION_ROWS)
);
} // end namespace share
} // end namespace oceanbase
