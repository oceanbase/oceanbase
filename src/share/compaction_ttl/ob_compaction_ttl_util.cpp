// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#include "storage/tx/ob_ts_mgr.h"
#define USING_LOG_PREFIX SHARE
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase
{
using namespace common;
namespace share
{

/**
 * @brief This interface only check whether compaction ttl is valid at the specific data version.
 */
int ObCompactionTTLUtil::is_compaction_ttl_schema(const uint64_t tenant_data_version,
                                                  const ObTableSchema &table_schema,
                                                  bool &is_compaction_ttl)
{
  int ret = OB_SUCCESS;

  is_compaction_ttl = false;

  const ObTTLFlag &ttl_flag = table_schema.get_ttl_flag();

  if (OB_UNLIKELY(!ttl_flag.is_valid(tenant_data_version))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ttl flag", KR(ret), K(tenant_data_version), K(table_schema));
  } else if (ttl_flag.get_ttl_type() == ObTTLDefinition::COMPACTION) {
    is_compaction_ttl = true;
  }

  return ret;
}

int ObCompactionTTLUtil::check_exist_user_defined_rowscn_column(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator iter = table_schema.column_begin();
  ObColumnSchemaV2 *col = NULL;
  for ( ; OB_SUCC(ret) && iter != table_schema.column_end(); iter++) {
    if (OB_ISNULL(col = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The column is NULL", K(col));
    } else if (is_rowscn_column(col->get_column_name_str())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "exist user defined rowscn column", K(ret), K(table_schema));
    }
  }
  return ret;
}

int ObCompactionTTLUtil::check_table_hbase_valid(const uint64_t tenant_id,
                                                 const ObTTLFlag &ttl_flag,
                                                 const ObString &kv_attributes)
{
  int ret = OB_SUCCESS;

  ObKVAttr kv_attr;
  if (OB_FAIL(ObTTLUtil::parse_kv_attributes(tenant_id, kv_attributes, kv_attr))) {
    COMMON_LOG(WARN, "fail to parse kv attributes", KR(ret), K(kv_attributes));
  } else if (OB_FAIL(check_table_hbase_valid(ttl_flag, kv_attr))) {
    COMMON_LOG(WARN, "fail to check table hbase valid", KR(ret), K(ttl_flag), K(kv_attr));
  }

  return ret;
}

int ObCompactionTTLUtil::check_table_hbase_valid(const ObTTLFlag &ttl_flag,
                                                 const common::ObKVAttr &new_kv_attr)
{
  int ret = OB_SUCCESS;

  if (new_kv_attr.is_kv_hbase_table() && ttl_flag.get_ttl_type() == ObTTLDefinition::COMPACTION) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "compaction ttl in hbase table is not supported", KR(ret), K(ttl_flag), K(new_kv_attr));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "compaction ttl in hbase table is");
  }

  return ret;
}

int ObCompactionTTLUtil::check_ttl_column_valid(const ObTableSchema &table_schema,
                                                const ObString &ttl_definition,
                                                const ObTTLFlag &ttl_flag,
                                                const ObMergeEngineType &merge_engine_type,
                                                const uint64_t tenant_data_version,
                                                const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (ObTTLDefinition::COMPACTION == ttl_flag.get_ttl_type()) {
    bool is_hbase_table = false;

    // 1. don't support sql mode ttl table in old data version
    if (tenant_data_version < COMPACTION_TTL_CMP_DATA_VERSION) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "sql mode ttl is not supported in old data version", K(ret), K(tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql mode ttl under this version is");
    } else if (OB_FAIL(table_schema.is_hbase_table(is_hbase_table))) {
      COMMON_LOG(WARN, "fail to check is hbase table", KR(ret), K(table_schema));
    } else if (is_hbase_table && OB_FAIL(check_table_hbase_valid(tenant_id, ttl_flag, table_schema.get_kv_attributes()))) {
      COMMON_LOG(WARN, "fail to check table hbase valid", KR(ret), K(table_schema));
    } else {
      ObSimpleTableTTLChecker ttl_checker;
      if (OB_FAIL(ttl_checker.init(ttl_definition))) {
        COMMON_LOG(WARN, "Fail to parse ttl_definition");
      } else {
        const common::ObIArray<ObTableTTLExpr> &ttl_exprs = ttl_checker.get_ttl_definition();
        const ObColumnSchemaV2 *column_schema = nullptr;
        int64_t unused = 0;

        if (ttl_exprs.count() != 1) {
          // 3. don't support multi ttl columns in sql mode
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "multi ttl columns count is not supported", K(ret), K(ttl_definition));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "multi ttl columns count in sql mode is");
        } else if (!table_schema.is_aux_lob_table() && !ObCompactionTTLUtil::is_compaction_ttl_merge_engine(merge_engine_type)) {
          // 4. only support delete_insert and append_only merge engine
          //    skip check aux lob table
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "compaction ttl only support delete_insert and append_only merge engine", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "compaction ttl for partial update merge engine is");
        } else if (OB_FAIL(check_exist_user_defined_rowscn_column(table_schema))) {
          COMMON_LOG(WARN, "table have user defined rowscn column, can't be compaction ttl table", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "table with user defined rowscn column as compaction ttl table is");
        } else if (OB_FAIL(ttl_checker.get_ttl_filter_us(unused))) {
          COMMON_LOG(WARN, "fail to get ttl filter us", KR(ret), K(ttl_definition));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "negative or very large ttl time is");
        } else if (!is_rowscn_column(ttl_exprs.at(0).column_name_)) {
          if (tenant_data_version < COMPACTION_TTL_CMP_DATA_VERSION_V2) {
            // 5. 4.5.1 only support ora_rowscn column in sql mode
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "now, only support ora_rowscn column in sql mode", K(ret), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-ora_rowscn column as ttl column in sql mode in this version is");
          } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(ttl_exprs.at(0).column_name_))) {
            // 6. column must be in table schema
            ret = OB_TTL_COLUMN_NOT_EXIST;
            LOG_WARN("column schema is null", K(ret), K(ttl_exprs.at(0).column_name_), K(table_schema));
            LOG_USER_ERROR(OB_TTL_COLUMN_NOT_EXIST, ttl_exprs.at(0).column_name_.length(), ttl_exprs.at(0).column_name_.ptr());
          } else if (!(column_schema->get_data_type() == ObDateTimeType
                       || column_schema->get_data_type() == ObTimestampType
                       || column_schema->get_data_type() == ObTimestampNanoType
                       || column_schema->get_data_type() == ObMySQLDateTimeType
                       || column_schema->get_data_type() == ObMySQLDateType
                       || column_schema->get_data_type() == ObTimestampTZType
                       || column_schema->get_data_type() == ObTimestampLTZType
                       || (is_hbase_table && column_schema->get_data_type() == ObIntType))) {
            // 8. column must be datetime type
            ret = OB_TTL_COLUMN_TYPE_NOT_SUPPORTED;
            LOG_WARN("non-datetime/non-timestamp column is not supported as ttl column", K(ret), K(ttl_exprs.at(0).column_name_), K(table_schema));
            LOG_USER_ERROR(OB_TTL_COLUMN_TYPE_NOT_SUPPORTED, ttl_exprs.at(0).column_name_.length(), ttl_exprs.at(0).column_name_.ptr());
          } else if (column_schema->get_data_type() == ObMySQLDateType
                     && !(ttl_exprs.at(0).time_unit_ == ObTableTTLTimeUnit::DAY
                          || ttl_exprs.at(0).time_unit_ == ObTableTTLTimeUnit::MONTH
                          || ttl_exprs.at(0).time_unit_ == ObTableTTLTimeUnit::YEAR)) {
            // 9. mysql date type only support day, month, year interval
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "mysql date type only support day, month, year interval", K(ret), K(ttl_exprs.at(0).column_name_), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "date type without day, month, year interval is");
          } else if (ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE == merge_engine_type && table_schema.is_heap_organized_table()) {
            // 10. ttl table with partial update merge engine not support heap organized table
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "ttl table with partial update merge engine not support heap organized table", K(ret), K(ttl_exprs.at(0).column_name_), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "ttl table with partial update merge engine and heap organized table is");
          } else if (ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE == merge_engine_type && !column_schema->is_rowkey_column()) {
            // 11. partial update merge engine only support rowkey column ttl
            // TODO(menglan): we disable normal column ttl for partial update merge engine because of the primary key conflict check path.
            //                we should implement fuse logic in this path to support that.
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "partial update merge engine only support rowkey column ttl", K(ret), K(ttl_exprs.at(0).column_name_), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "partial update merge engine with non-rowkey column ttl is");
          }
        }
      }
    }
  }

  return ret;
}

int ObCompactionTTLUtil::check_alter_merge_engine_valid(const share::schema::ObTableSchema &table_schema,
                                                        const AlterTableSchema &alter_table_schema,
                                                        const uint64_t tenant_id,
                                                        share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    COMMON_LOG(WARN, "Fail to get data version", KR(ret));
  }

  if (OB_SUCC(ret) && table_schema.is_append_only_merge_engine() && alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::DYNAMIC_PARTITION_POLICY)) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "dynamic partition policy is not supported for append_only table", K(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition policy for append_only table is");
  }
  /*
   1. append_only table can not have delete/update event trigger
   2. append_only table can not be alter to other merge engine
   3. dynamic partition policy is not supported for append_only table
   4. Compaction TTL table with index or invalid ttl column can not alter to partial update merge engine
   5. Deleting TTL table can not alter to append_only merge engine
  */

  const bool is_alter_merge_engine = alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::MERGE_ENGINE_TYPE);
  if (OB_SUCC(ret) && is_alter_merge_engine) {
    const bool is_alter_ttl = alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::TTL_DEFINITION);
    const bool is_alter_dynamic_partition_policy = alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::DYNAMIC_PARTITION_POLICY);
    const bool is_alter_compaction_ttl = is_alter_ttl && alter_table_schema.get_ttl_flag().ttl_type_ == ObTTLDefinition::COMPACTION;
    const bool is_alter_deleting_ttl = is_alter_ttl && alter_table_schema.get_ttl_flag().ttl_type_ == ObTTLDefinition::DELETING;
    const bool is_compaction_ttl = table_schema.has_ttl_definition() && table_schema.get_ttl_flag().ttl_type_ == ObTTLDefinition::COMPACTION;
    const bool is_deleting_ttl = table_schema.has_ttl_definition() && table_schema.get_ttl_flag().ttl_type_ == ObTTLDefinition::DELETING;

    // 1. check alter table schema is valid
    // 1.1 append_only table does not support dynamic partition policy
    if (alter_table_schema.is_append_only_merge_engine() && is_alter_dynamic_partition_policy) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "append_only table does not support dynamic partition policy", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition policy for append_only table is");
    }
    // 1.2 compaction ttl table can not be alter to partial update merge engine
    if (OB_SUCC(ret) && is_alter_compaction_ttl && !is_compaction_ttl_merge_engine(alter_table_schema.get_merge_engine_type())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "invalid merge engine with compaction ttl definition", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table merge engine with compaction ttl definition is");
    }
    // 1.3 deleting ttl table can not be alter to append_only merge engine
    if (OB_SUCC(ret) && is_alter_deleting_ttl && !is_deleting_ttl_merge_engine(alter_table_schema.get_merge_engine_type())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "invalid merge engine with deleting ttl definition", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table merge engine with deleting ttl definition is");
    }

    // 2. check alter is valid
    // 2.1 append_only table can not be alter to other merge engine
    if (OB_SUCC(ret) && table_schema.is_append_only_merge_engine() && !alter_table_schema.is_append_only_merge_engine()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "append_only table can not be alter to other merge engine", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter append_only table to other merge engine is");
    }

    // 2.2 table with dynamic partition policy can not be alter to append_only table
    if (OB_SUCC(ret) && table_schema.with_dynamic_partition_policy() && alter_table_schema.is_append_only_merge_engine()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "table with dynamic partition policy can not be alter to append_only table", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table with dynamic partition policy to append_only table is");
    }

    // 2.3 table with trigger can not be alter to append_only table
    if (OB_SUCC(ret) && alter_table_schema.is_append_only_merge_engine() && table_schema.get_trigger_list().count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_trigger_list().count(); i++) {
        const ObTriggerInfo *trigger_info = nullptr;
        if (OB_FAIL(schema_guard.get_trigger_info(table_schema.get_tenant_id(), table_schema.get_trigger_list().at(i), trigger_info))) {
          COMMON_LOG(WARN, "fail to get trigger info", K(ret), K(table_schema), K(table_schema.get_trigger_list().at(i)));
        } else if (OB_ISNULL(trigger_info)) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "trigger info is null", K(ret), K(table_schema), K(table_schema.get_trigger_list().at(i)));
        } else if (trigger_info->has_delete_event() || trigger_info->has_update_event()) {
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "append_only table can not have delete or update event trigger", K(ret), K(table_schema), K(trigger_info));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table with delete or update event trigger to append_only table is");
        }
      }
    }

    // 2.4 compaction ttl support append_only/delete_insert/partial_update(no index and valid ttl column) merge engine, deleting ttl support partial_update/delete_insert merge engine
    // if alter ttl, we only need to check alter table schema is valid
    if (OB_SUCC(ret) && !is_alter_ttl) {
      if (is_compaction_ttl) {
        if (!is_compaction_ttl_merge_engine(alter_table_schema.get_merge_engine_type())) {
          ret = OB_NOT_SUPPORTED;
          COMMON_LOG(WARN, "invalid merge engine with compaction ttl definition", K(ret), K(table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table merge engine with compaction ttl definition is");
        } else if (alter_table_schema.is_partial_update_merge_engine()) {
          // 2.4.1 compaction ttl table with index can not alter to partial update merge engine
          if (table_schema.get_simple_index_infos().count() > 0) {
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "compaction ttl table with index can not alter to partial update merge engine", K(ret), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table merge engine with compaction ttl definition and index is");
          // 2.4.2 compaction ttl table with invalid ttl column can not alter to partial update merge engine
          } else if (OB_FAIL(check_ttl_column_valid(table_schema,
                                                    table_schema.get_ttl_definition(),
                                                    table_schema.get_ttl_flag(),
                                                    alter_table_schema.get_merge_engine_type(),
                                                    tenant_data_version,
                                                    tenant_id))) {
            COMMON_LOG(WARN, "fail to check ttl column valid", KR(ret), K(table_schema), K(alter_table_schema));
          }
        }
      }
      if (OB_SUCC(ret) && is_deleting_ttl && !is_deleting_ttl_merge_engine(alter_table_schema.get_merge_engine_type())) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "invalid merge engine with deleting ttl definition", K(ret), K(table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table merge engine with deleting ttl definition is");
      }
    }
  }
  return ret;
}

int ObCompactionTTLUtil::extract_lob_meta_ttl_column_from_schema(
    const schema::ObTableSchema &table_schema,
    schema::ObColumnSchemaV2 &ttl_column)
{
  int ret = OB_SUCCESS;

  if (table_schema.get_ttl_flag().is_lob_meta_has_ttl_column()) {
    const schema::ObColumnSchemaV2 *src_col = table_schema.get_column_schema(table_schema.get_ttl_flag().get_last_user_ttl_column_id());
    if (OB_ISNULL(src_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get ttl column, schema is not consistent", K(ret), K(table_schema));
    } else if (OB_FAIL(ttl_column.assign(*src_col))) {
      LOG_WARN("Fail to assign ttl column schema", K(ret), K(src_col));
    } else {
      // For aux lob table, we need reset some fields
      ObObj default_obj;
      default_obj.set_null();

      // Maybe more clear than if-else-OB_FAIL
      OZ(ttl_column.set_column_name(OB_LOB_META_TTL_COLUMN_NAME));
      OZ(ttl_column.set_orig_default_value_v2(default_obj));
      OZ(ttl_column.set_cur_default_value_v2(default_obj));

      OX(ttl_column.set_column_id(OB_LOB_META_TTL_COLUMN_ID));
      OX(ttl_column.set_nullable(true));
      OX(ttl_column.set_rowkey_position(0));
      OX(ttl_column.set_index_position(0));
      OX(ttl_column.set_not_part_key());
      OX(ttl_column.set_autoincrement(false));
      OX(ttl_column.set_has_used_as_ttl(true));
    }
  }

  return ret;
}

int ObCompactionTTLUtil::create_mock_ttl_column_for_aux_lob(const common::ObObjType obj_type,
                                                            schema::ObColumnSchemaV2 &ttl_column)
{
  int ret = OB_SUCCESS;

  ObObj default_obj;
  default_obj.set_null();

  OZ(ttl_column.set_column_name(OB_LOB_META_TTL_COLUMN_NAME));
  OZ(ttl_column.set_orig_default_value_v2(default_obj));
  OZ(ttl_column.set_cur_default_value_v2(default_obj));

  OX(ttl_column.set_column_id(OB_LOB_META_TTL_COLUMN_ID));
  OX(ttl_column.set_data_type(obj_type));
  OX(ttl_column.set_nullable(true));
  OX(ttl_column.set_rowkey_position(0));
  OX(ttl_column.set_index_position(0));
  OX(ttl_column.set_not_part_key());
  OX(ttl_column.set_autoincrement(false));
  OX(ttl_column.set_has_used_as_ttl(true));

  return ret;
}

int ObCompactionTTLUtil::build_aux_lob_table_schema(const schema::ObTableSchema &data_table_schema, schema::ObTableSchema &aux_lob_meta_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObInnerTableSchema::all_column_aux_lob_meta_schema(aux_lob_meta_schema))) {
    LOG_WARN("Fail to get all column aux lob meta schema", K(ret), K(data_table_schema));
  } else if (data_table_schema.get_ttl_flag().is_lob_meta_has_ttl_column()) {
    // Compaction TTL Table, should add ttl column to aux lob meta schema
    schema::ObColumnSchemaV2 dst_col;
    if (OB_FAIL(extract_lob_meta_ttl_column_from_schema(data_table_schema, dst_col))) {
      LOG_WARN("Fail to extract lob meta ttl column from schema", K(ret), K(data_table_schema));
    } else if (OB_FAIL(aux_lob_meta_schema.add_column(dst_col))) {
      LOG_WARN("Fail to add ttl column to aux lob meta schema", K(ret), K(dst_col));
    } else {
      aux_lob_meta_schema.set_max_used_column_id(dst_col.get_column_id());
    }
  }

  return ret;
}

int ObCompactionTTLUtil::build_aux_lob_table_schema(const ObTTLFilterColType ttl_type,
                                                    schema::ObTableSchema &aux_lob_meta_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObInnerTableSchema::all_column_aux_lob_meta_schema(aux_lob_meta_schema))) {
    LOG_WARN("Fail to get all column aux lob meta schema", K(ret));
  } else if (ObTTLFilterColTypeUtil::has_ttl_column_in_lob_meta(ttl_type)) {
    schema::ObColumnSchemaV2 ttl_col;
    common::ObObjType ttl_column_obj_type = common::ObMaxType;
    if (OB_FAIL(ObTTLFilterColTypeUtil::to_obj_type(ttl_type, ttl_column_obj_type))) {
      LOG_WARN("Fail to convert ttl type to obj type", K(ret), K(ttl_type));
    } else if (OB_FAIL(create_mock_ttl_column_for_aux_lob(ttl_column_obj_type, ttl_col))) {
      LOG_WARN("Fail to create mock ttl column for aux lob", K(ret), K(ttl_column_obj_type));
    } else if (OB_FAIL(aux_lob_meta_schema.add_column(ttl_col))) {
      LOG_WARN("Fail to add ttl column to aux lob meta schema", K(ret), K(ttl_col));
    } else {
      aux_lob_meta_schema.set_max_used_column_id(ttl_col.get_column_id());
    }
  }

  return ret;
}

int ObCompactionTTLUtil::adjust_ttl_flag_for_offline(ObTableSchema &new_table_schema, hash::ObHashMap<uint64_t, uint64_t> &id_map)
{
  int ret = OB_SUCCESS;

  if (new_table_schema.get_ttl_flag().version_ >= ObTTLFlag::TTL_FLAG_VERSION_V3) { // which means >= 4.6.1
    uint64_t *new_user_ttl_column_id = id_map.get(new_table_schema.get_ttl_flag().get_last_user_ttl_column_id());

    if (new_user_ttl_column_id == nullptr) {
      // This situation means user drop the ttl column.
      new_table_schema.get_ttl_flag().clear_user_ttl_column_id();
    } else {
      new_table_schema.get_ttl_flag().update_user_ttl_column_id(*new_user_ttl_column_id);
    }
  }

  return ret;
}


int ObCompactionTTLUtil::update_being_compaction_ttl_time(ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;

  ObTimeoutCtx ctx;
  share::SCN upper_version;
  uint64_t tenant_id = new_table_schema.get_tenant_id();
  uint64_t timestamp_us = 0;
  bool is_external_consistent = false;
  if (OB_UNLIKELY(common::OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id,
                                           ctx.get_timeout(),
                                           upper_version,
                                           is_external_consistent))) {
    LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id), K(ctx.get_timeout()), K(upper_version));
  } else if (FALSE_IT(timestamp_us = upper_version.convert_to_ts())) {
  } else if (OB_UNLIKELY(timestamp_us <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timestamp", KR(ret), K(timestamp_us), K(upper_version));
  } else {
    new_table_schema.update_being_compaction_ttl_time(timestamp_us);
  }

  return ret;
}

bool ObCompactionTTLUtil::check_change_to_compaction_ttl_table(
    const AlterTableSchema &alter_table_schema,
    const ObTableSchema &orig_table_schema)
{
  // If a table is transformed to a ora_rowscn ttl table,
  // we should lock the table to make sure all DML before this ddl to be committed, and all DML
  // after this ddl should use this new schema. DML that use the new shcema will update all aux
  // tables even if the index table is not affected by the DML itself. For example, main table: pk1,
  // c1, c2
  //             index table: c1, pk1
  // `update c2 = 2 if pk = 1` won't change the index table, but it actually changes the ora_rowscn
  // column value. Becuase TTL table depends on the ora_rowscn value, we must update all aux table
  // together. Therefore, all DML after this ddl should use this new schema.
  return alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::TTL_DEFINITION)
         && alter_table_schema.get_ttl_flag().ttl_type_ == share::ObTTLDefinition::COMPACTION
         && orig_table_schema.get_ttl_flag().ttl_type_ != share::ObTTLDefinition::COMPACTION;
}

int ObCompactionTTLUtil::adjust_ttl_related_field_for_offline(const uint64_t tenant_data_version,
                                                              ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;

  if (tenant_data_version >= ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2) {
    bool is_compaction_ttl = new_table_schema.get_ttl_flag().get_ttl_type() == ObTTLDefinition::COMPACTION;
    uint64_t current_user_ttl_column_id = new_table_schema.get_ttl_flag().get_curr_user_ttl_column_id();

    ObTableSchema::const_column_iterator tmp_begin = new_table_schema.column_begin();
    ObTableSchema::const_column_iterator tmp_end = new_table_schema.column_end();
    for (; OB_SUCC(ret) && tmp_begin != tmp_end; tmp_begin++) {
      ObColumnSchemaV2 *col = (*tmp_begin);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K(ret));
      } else if (is_compaction_ttl && col->get_column_id() == current_user_ttl_column_id) {
        // 1. If new table schema is compaction ttl, we should clear all has_used_ttl_flag except the current ttl column
        // 2. If new table schema isn't compaction ttl, we should clear all has_used_ttl_flag and reset ttl_flag
      } else {
        col->set_has_used_as_ttl(false);
      }
    }

    if (!is_compaction_ttl) {
      // Offline path remove compaction ttl, we can safely clear compaction ttl information
      new_table_schema.get_ttl_flag().reset_compaction_ttl();
    } else {
      // Don't need to adjust ttl_flag now. If column id has changed, we will adjust it in adjust_ttl_flag_for_offline
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase
