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
#include "share/schema/ob_schema_service.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_schema_macro_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace oceanbase::common;
DEFINE_ENUM_FUNC(ObSchemaOperationType, op_type, OP_TYPE_DEF);
//only liboblog will set it to true
//use to ignore column retrieve error of new added column in inner table
bool ObSchemaService::g_ignore_column_retrieve_error_ = false;
bool ObSchemaService::g_liboblog_mode_ = false;

ObSchemaOperation::ObSchemaOperation()
    : schema_version_(OB_INVALID_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID),
      user_id_(0),
      database_id_(0),
      database_name_(""),
      tablegroup_id_(0),
      table_id_(0),
      table_name_(""),
      op_type_(OB_INVALID_DDL_OP),
      ddl_stmt_str_("")
{
}

uint64_t ObSchemaOperation::get_obj_type() const 
{
  return ObFastAtoi<uint64_t>::atoi_positive_unchecked(table_name_.ptr(), 
                                                       table_name_.ptr() + table_name_.length());
}

void ObSchemaOperation::reset()
{
  schema_version_ = OB_INVALID_VERSION;
  tenant_id_ = OB_INVALID_TENANT_ID;
  user_id_ = 0;
  database_id_ = 0;
  database_name_.reset();
  tablegroup_id_ = 0;
  table_id_ = 0;
  table_name_.reset();
  op_type_ = OB_INVALID_DDL_OP;
  ddl_stmt_str_.reset();
}
// Shallow copy
ObSchemaOperation& ObSchemaOperation::operator=(const ObSchemaOperation &other)
{
  if (this != &other) {
    reset();
    schema_version_ = other.schema_version_;
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    database_id_ = other.database_id_;
    database_name_ = other.database_name_;
    tablegroup_id_ = other.tablegroup_id_;
    table_id_ = other.table_id_;
    table_name_ = other.table_name_;
    op_type_ = other.op_type_;
    ddl_stmt_str_ = other.ddl_stmt_str_;
  }
  return *this;
}
// Not all content is serialized, obstring is not serialized
OB_SERIALIZE_MEMBER(ObSchemaOperation,
                    schema_version_,
                    tenant_id_,
                    user_id_,
                    table_id_,
                    database_id_,
                    tablegroup_id_,
                    op_type_,
                    outline_id_,                // for compat
                    synonym_id_,                // for compat
                    sequence_id_,               // for compat
                    keystore_id_,               // for compat
                    label_se_policy_id_,        // for compat
                    label_se_component_id_,     // for compat
                    label_se_label_id_,         // for compat
                    label_se_user_level_id_,    // for compat
                    tablespace_id_,             // for compat
                    profile_id_,                // for compat
                    audit_id_,                  // for compat
                    grantee_id_,                // for compat
                    grantor_id_,                // for compat
                    dblink_id_,                 // for compat
                    directory_id_);             // for compat

bool ObSchemaOperation::is_valid() const
{
  return schema_version_ >= 0 && OB_INVALID_DDL_OP != op_type_;
}

const char *ObSchemaOperation::type_str(ObSchemaOperationType op_type)
{
  return get_op_type_string(op_type);
}

ObString ObSchemaOperation::type_semantic_str(ObSchemaOperationType op_type)
{
  ObString ddl_type_str(get_op_type_string(op_type));
  ddl_type_str.split_on('_');
  ddl_type_str.split_on('_');
  return ddl_type_str.split_on('_');
}

int64_t ObSchemaOperation::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(schema_version), K_(tenant_id), K_(database_id), K_(user_id), K_(database_name),
       K_(tablegroup_id), K_(table_id), "operation_type", type_str(op_type_),
       K_(label_se_policy_id), K_(label_se_component_id),
       K_(label_se_label_id), K_(label_se_user_level_id),
       K_(outline_id), K_(udf_name), K_(sequence_id), K_(keystore_id),
       K_(outline_id), K_(udf_name), K_(sequence_id),
       K_(tablespace_id), K_(profile_id), K_(audit_id),
       K_(grantee_id), K_(grantor_id),
       K_(ddl_stmt_str), K_(dblink_id), K_(directory_id));
  return pos;
}

int AlterTableSchema::deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  AlterColumnSchema column;
  int64_t count = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "buf should not be null", K(buf), K(data_len), K(pos), K(ret));
  } else if (pos == data_len) {
    //do nothing
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to decode column count, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      column.reset();
      if (OB_FAIL(column.deserialize(buf, data_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to deserialize", K(ret), K(column));
      } else if (OB_FAIL(add_alter_column(column, true))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column, ", K(ret));
      } else {
        SHARE_SCHEMA_LOG(DEBUG, "add alter column", K(column));
      }
    }
  }
  return ret;
}


void AlterColumnSchema::reset()
{
  ObColumnSchemaV2::reset();
  alter_type_ = OB_INVALID_DDL_OP;
  origin_column_name_.reset();
  is_primary_key_ = false;
  is_autoincrement_ = false;
  is_unique_key_ = false;
  is_drop_default_ = false;
  is_set_nullable_ = false;
  is_set_default_ = false;
  check_timestamp_column_order_ = false;
  is_no_zero_date_ = false;
  next_column_name_.reset();
  prev_column_name_.reset();
  is_first_ = false;
}


OB_SERIALIZE_MEMBER((AlterColumnSchema, ObColumnSchemaV2),
                    alter_type_,
                    origin_column_name_,
                    is_primary_key_,
                    is_autoincrement_,
                    is_unique_key_,
                    is_drop_default_,
                    is_set_nullable_,
                    is_set_default_,
                    check_timestamp_column_order_,
                    is_no_zero_date_,
                    next_column_name_,
                    prev_column_name_,
                    is_first_);

DEFINE_SERIALIZE(AlterTableSchema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableSchema::serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize ObTableSchema, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, alter_type_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize alter_type_, ", K(ret));
  } else if (OB_FAIL(origin_table_name_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize origin_table_name_, ", K(ret));
  } else if (OB_FAIL(new_database_name_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize new_datbase_name, ", K(ret));
  } else if (OB_FAIL(origin_database_name_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize origin_database_name, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, origin_tablegroup_id_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize origin_tablegroup_id_", K(ret));
  } else if (OB_FAIL(alter_option_bitset_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialized bitset", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, sql_mode_))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize sql_mode_", K(ret));
  } else if (OB_FAIL(split_partition_name_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize partition_name", K(ret));
  } else if (OB_FAIL(new_part_name_.serialize(buf, buf_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize new_part_name", K(ret));
  }
  return ret;
}
//
DEFINE_DESERIALIZE(AlterTableSchema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableSchema::deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize ObTableSchema", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, ((int32_t *)(&alter_type_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize alter_type_, ", K(ret));
  } else if (OB_FAIL(origin_table_name_.deserialize(buf, data_len, pos))) {
    //do not deep copy becasue AlterTableSchema are not used after this
    //alter table request
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize origin_table_name, ", K(ret));
  } else if (OB_FAIL(new_database_name_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize new_database_name, ", K(ret));
  } else if (OB_FAIL(origin_database_name_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize origin_database_name, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, ((int64_t *)(&origin_tablegroup_id_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize origin_tablegroup_id");
  } else if (OB_FAIL(alter_option_bitset_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize bitset", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, ((int64_t *)(&sql_mode_))))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize sql mode", K(ret));
  } else if (OB_FAIL(split_partition_name_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to deserialize split_partition_name", K(ret));
  } else if (OB_FAIL(new_part_name_.deserialize(buf, data_len, pos))) {
    SHARE_SCHEMA_LOG(WARN, "fail to serialize new_part_name", K(ret));
  }
  return ret;
}

void AlterTableSchema::reset()
{
  ObTableSchema::reset();
  alter_type_ = OB_INVALID_DDL_OP,
  origin_table_name_.reset();
  new_database_name_.reset();
  origin_database_name_.reset();
  origin_tablegroup_id_ = common::OB_INVALID_ID;
  alter_option_bitset_.reset();
  sql_mode_ = SMO_DEFAULT;
  split_partition_name_.reset();
  split_high_bound_val_.reset();
  split_list_row_values_.reset();
  new_part_name_.reset();
}

int64_t AlterTableSchema::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ALTER_TYPE, ObSchemaOperation::type_str(alter_type_),
       K_(origin_table_name),
       K_(new_database_name),
       K_(origin_database_name),
       K_(split_partition_name),
       K_(split_high_bound_val),
       K_(split_list_row_values),
       K_(new_part_name));
  J_COMMA();
  J_NAME(N_ALTER_TABLE_SCHEMA);
  J_COLON();
  pos += ObTableSchema::to_string(buf + pos, buf_len - pos);
  J_OBJ_END();

  return pos;

}

int64_t AlterColumnSchema::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ALTER_TYPE, ObSchemaOperation::type_str(alter_type_),
       K_(is_drop_default),
       K_(is_set_nullable),
       K_(is_set_default),
       K_(origin_column_name),
       K_(next_column_name),
       K_(prev_column_name),
       K_(is_unique_key));
  J_COMMA();
  J_NAME(N_ALTER_COLUMN_SCHEMA);
  J_COLON();
  pos += ObColumnSchemaV2::to_string(buf + pos, buf_len - pos);
  J_OBJ_END();
  return pos;
}

int AlterColumnSchema::assign(const ObColumnSchemaV2 &other)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2::operator = (other);
  return ret;
}

AlterColumnSchema &AlterColumnSchema::operator=(const AlterColumnSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    ObColumnSchemaV2::operator = (src_schema);
    alter_type_ = src_schema.alter_type_;
    is_primary_key_ = src_schema.is_primary_key_;
    is_autoincrement_ = src_schema.is_autoincrement_;
    is_unique_key_ = src_schema.is_unique_key_;
    is_drop_default_ = src_schema.is_drop_default_;
    is_set_nullable_ = src_schema.is_set_nullable_;
    is_set_default_ = src_schema.is_set_default_;
    check_timestamp_column_order_ = src_schema.check_timestamp_column_order_;
    is_no_zero_date_ = src_schema.is_no_zero_date_;
    if (OB_FAIL(deep_copy_str(src_schema.get_origin_column_name(), origin_column_name_))) {
      SHARE_LOG(WARN, "failed to deep copy origin_column_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.get_next_column_name(), next_column_name_))) {
      SHARE_LOG(WARN, "failed to deep copy next_column_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.get_prev_column_name(), prev_column_name_))) {
      SHARE_LOG(WARN, "failed to deep copy prev_column_name", K(ret));
    } else {
      is_first_ = src_schema.is_first_;
    }
  }
  if (OB_FAIL(ret)) {
    error_ret_ = ret;
  }
  return *this;
}


int AlterTableSchema::assign(const ObTableSchema &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    int ret = common::OB_SUCCESS;
    char *buf = NULL;
    int64_t column_cnt = 0;

    if (OB_FAIL(ObSimpleTableSchemaV2::assign(src_schema))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else {
      error_ret_ = src_schema.error_ret_;
      max_used_column_id_ = src_schema.max_used_column_id_;
      rowkey_column_num_ = src_schema.rowkey_column_num_;
      index_column_num_ = src_schema.index_column_num_;
      rowkey_split_pos_ = src_schema.rowkey_split_pos_;
      part_key_column_num_ = src_schema.part_key_column_num_;
      subpart_key_column_num_ = src_schema.subpart_key_column_num_;
      block_size_ = src_schema.block_size_;
      is_use_bloomfilter_ = src_schema.is_use_bloomfilter_;
      progressive_merge_num_ = src_schema.progressive_merge_num_;
      tablet_size_ = src_schema.tablet_size_;
      pctfree_ = src_schema.pctfree_;
      autoinc_column_id_ = src_schema.autoinc_column_id_;
      auto_increment_ = src_schema.auto_increment_;
      read_only_ = src_schema.read_only_;
      load_type_ = src_schema.load_type_;
      index_using_type_ = src_schema.index_using_type_;
      def_type_ = src_schema.def_type_;
      charset_type_ = src_schema.charset_type_;
      collation_type_ = src_schema.collation_type_;
      code_version_ = src_schema.code_version_;
      index_attributes_set_ = src_schema.index_attributes_set_;
      session_id_ = src_schema.session_id_;
      compressor_type_ = src_schema.compressor_type_;
      if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
        LOG_WARN("Fail to deep copy tablegroup_name", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
        LOG_WARN("Fail to deep copy comment", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.expire_info_, expire_info_))) {
        LOG_WARN("Fail to deep copy expire info string", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.parser_name_, parser_name_))) {
        LOG_WARN("deep copy parser name failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_location_, external_file_location_))) {
        LOG_WARN("deep copy external_file_location failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_location_access_info_, external_file_location_access_info_))) {
        LOG_WARN("deep copy external_file_location_access_info failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_format_, external_file_format_))) {
        LOG_WARN("deep copy external_file_format failed", K(ret));
      } else if (OB_FAIL(deep_copy_str(src_schema.external_file_pattern_, external_file_pattern_))) {
        LOG_WARN("deep copy external_file_pattern failed", K(ret));
      }

      //view schema
      view_schema_ = src_schema.view_schema_;

      mv_cnt_ = src_schema.mv_cnt_;
      MEMCPY(mv_tid_array_, src_schema.mv_tid_array_, sizeof(uint64_t) * src_schema.mv_cnt_);

      aux_vp_tid_array_ = src_schema.aux_vp_tid_array_;

      base_table_ids_ = src_schema.base_table_ids_;
      depend_table_ids_ = src_schema.depend_table_ids_;
      depend_mock_fk_parent_table_ids_ = src_schema.depend_mock_fk_parent_table_ids_;

      //copy columns
      column_cnt = src_schema.column_cnt_;

      aux_lob_meta_tid_ = src_schema.aux_lob_meta_tid_;
      aux_lob_piece_tid_ = src_schema.aux_lob_piece_tid_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_simple_index_infos(src_schema.get_simple_index_infos()))) {
        LOG_WARN("fail to set simple index infos", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_constraint(src_schema))) {
        LOG_WARN("failed to assign constraint", K(ret), K(src_schema), K(*this));
      }
    }

    //prepare memory
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_info_.reserve(src_schema.rowkey_info_.get_size()))) {
        LOG_WARN("Fail to reserve rowkey_info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(shadow_rowkey_info_.reserve(src_schema.shadow_rowkey_info_.get_size()))) {
        LOG_WARN("Fail to reserve shadow_rowkey_info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_info_.reserve(src_schema.index_info_.get_size()))) {
        LOG_WARN("Fail to reserve index_info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_key_info_.reserve(src_schema.partition_key_info_.get_size()))) {
        LOG_WARN("Fail to reserve partition_key_info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(subpartition_key_info_.reserve(src_schema.subpartition_key_info_.get_size()))) {
        LOG_WARN("Fail to reserve partition_key_info", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t id_hash_array_size = get_id_hash_array_mem_size(column_cnt);
      if (NULL == (buf = static_cast<char*>(alloc(id_hash_array_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for id_hash_array, ", K(id_hash_array_size), K(ret));
      } else if (NULL == (id_hash_array_ = new (buf) IdHashArray(id_hash_array_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new IdHashArray", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t name_hash_array_size = get_name_hash_array_mem_size(column_cnt);
      if (NULL == (buf = static_cast<char*>(alloc(name_hash_array_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (NULL == (name_hash_array_ = new (buf) NameHashArray(name_hash_array_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to new NameHashArray", K(ret));
      }
    }

    if (OB_SUCCESS == ret && column_cnt > 0) {
      column_array_ = static_cast<ObColumnSchemaV2**>(alloc(sizeof(AlterColumnSchema*)
                                                            * column_cnt));
      if (NULL == column_array_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Fail to allocate memory for column_array_", K(ret));
      } else {
        MEMSET(column_array_, 0, sizeof(AlterColumnSchema*) * column_cnt);
        column_array_capacity_ = column_cnt;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      AlterColumnSchema column;
      if (OB_FAIL(column.assign(*src_schema.column_array_[i]))) {
        LOG_WARN("fail to assign", K(ret));
      } else if (OB_FAIL(add_column<AlterColumnSchema>(column))) {
        LOG_WARN("Fail to add column", K(ret));
      } else {
        LOG_DEBUG("add column success", K(column));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.ttl_definition_, ttl_definition_))) {
    LOG_WARN("Fail to deep copy ttl definition string", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(deep_copy_str(src_schema.kv_attributes_, kv_attributes_))) {
    LOG_WARN("Fail to deep copy ttl definition string", K(ret));
  }

  return ret;
}

int AlterTableSchema::add_alter_column(const AlterColumnSchema &alter_column_schema,
                                       const bool need_allocate)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  AlterColumnSchema *local_column = NULL;
  if (!alter_column_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "The column is not valid, ", K(ret));
  } else if (need_allocate) {
    if (NULL == (buf = static_cast<char*>(alloc(sizeof(AlterColumnSchema))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(ERROR, "Fail to allocate memory, ", "size", sizeof(AlterColumnSchema), K(ret));
    } else {
      //deep copy
      if (NULL == (local_column = new (buf) AlterColumnSchema(allocator_))) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "Fail to new local_column", K(ret));
      } else {
        *local_column = alter_column_schema;
        if (!local_column->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "The local column is not valid, ", K(ret));
        } else if (OB_FAIL(add_col_to_column_array(local_column))) {
          SHARE_LOG(WARN, "Fail to push column to array, ", K(ret));
        } else {
          SHARE_SCHEMA_LOG(DEBUG, "add column", K(local_column));
        }
      }
    }
  } else {
    if (OB_FAIL(add_col_to_column_array(const_cast<AlterColumnSchema *>(&alter_column_schema)))) {
      SHARE_LOG(WARN, "Fail to push column to array, ", K(ret));
    }
  }
  return ret;
}

int AlterTableSchema::assign_subpartiton_key_info(const common::ObPartitionKeyInfo& src_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(subpartition_key_info_.reserve(src_info.get_size()))) {
    SHARE_SCHEMA_LOG(WARN, "reserve subpartition key info", K(ret));
  } else {
    const ObRowkeyColumn *ObRowkeyInfo = NULL;
    for (int i = 0; OB_SUCC(ret) && i < src_info.get_size(); i++) {
      ObRowkeyInfo = src_info.get_column(i);
      if (OB_FAIL(subpartition_key_info_.set_column(i, *ObRowkeyInfo))) {
        SHARE_SCHEMA_LOG(WARN, "set column fail", K(ret));
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(AlterTableSchema)
{
  int64_t size = 0;
  size += ObTableSchema::get_serialize_size();
  size += serialization::encoded_length_vi32(static_cast<int32_t>(alter_type_));
  size += origin_table_name_.get_serialize_size();
  size += new_database_name_.get_serialize_size();
  size += origin_database_name_.get_serialize_size();
  size += serialization::encoded_length_vi64(origin_tablegroup_id_);
  size += alter_option_bitset_.get_serialize_size();
  size += serialization::encoded_length_vi64(sql_mode_);
  size += split_partition_name_.get_serialize_size();
  size += new_part_name_.get_serialize_size();
  return size;
}

bool ObSchemaService::is_formal_version(const int64_t schema_version)
{
  return schema_version % ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP == 0;
}

bool ObSchemaService::is_sys_temp_version(const int64_t schema_version)
{
  return schema_version % ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP
         == (ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP - 1);
}

int ObSchemaService::gen_core_temp_version(const int64_t schema_version,
                                           int64_t &core_temp_version)
{
  int ret = OB_SUCCESS;
  if (!is_formal_version(schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema_version is not formal version", K(schema_version), K(ret));
  } else {
    core_temp_version = schema_version - 2;
  }
  return ret;
}

int ObSchemaService::gen_sys_temp_version(const int64_t schema_version,
                                          int64_t &sys_temp_version)
{
  int ret = OB_SUCCESS;
  if (!is_formal_version(schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema_version is not formal version", K(schema_version), K(ret));
  } else {
    sys_temp_version = schema_version - 1;
  }
  return ret;
}

int ObSchemaService::alloc_table_schema(const ObTableSchema &table,
                                        ObIAllocator &allocator,
                                        ObTableSchema *&allocated_table_schema)
{
  int ret = OB_SUCCESS;
  allocated_table_schema = NULL;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(ObTableSchema)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "alloc table schema failed", K(ret));
  } else if (NULL == (allocated_table_schema = new (buf) ObTableSchema(&allocator))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "placement new failed", K(ret));
  } else if (OB_FAIL((*allocated_table_schema).assign(table))){
    LOG_WARN("fail to assign schema", K(ret));
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
