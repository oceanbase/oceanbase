//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_TTL_UTIL_H_
#define OB_SHARE_COMPACTION_TTL_UTIL_H_
#include "share/schema/ob_schema_struct.h"
#include "src/share/ob_rpc_struct.h"
#include "share/schema/ob_latest_schema_guard.h"
#include "lib/json/ob_json.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}

struct ObTTLDefinition final
{
  enum ObTTLType : uint8_t {
    NONE = 0, // no ttl definition on schema NOW or compat for old DELETING mode table
    DELETING = 1,
    COMPACTION = 2,
    INVALID,
  };
  static const char *ttl_type_to_string(const ObTTLType ttl_type) { // only for print schema
    const char *ret_str = "INVALID";
    switch (ttl_type) {
      case DELETING:
      case NONE:
        ret_str = "DELETING";
        break;
      case COMPACTION:
        ret_str = "COMPACTION";
        break;
      default:
        break;
    }
    return ret_str;
  }
  ObTTLDefinition()
      : ttl_definition_(), ttl_type_(INVALID) {}
  ObTTLDefinition(const ObString &ttl_definition, const uint64_t ttl_type)
      : ttl_definition_(ttl_definition), ttl_type_(static_cast<ObTTLType>(ttl_type)) {}
  bool is_valid() const { return ttl_type_ != INVALID && !ttl_definition_.empty(); }
  void reset() { ttl_definition_.reset(); ttl_type_ = INVALID; }
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV("ttl", ttl_definition_, "type", ttl_type_to_string(ttl_type_));

  ObString ttl_definition_;
  ObTTLType ttl_type_;
};


struct ObCompactionTTLUtil final
{
public:
  static const uint64_t COMPACTION_TTL_CMP_DATA_VERSION = DATA_VERSION_4_5_1_0;
  static bool is_enable_compaction_ttl(uint64_t tenant_id);
  static bool is_compaction_ttl_merge_engine(const ObMergeEngineType &merge_engine_type) {
    return ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT == merge_engine_type
      || ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY == merge_engine_type;
  }
  static bool is_rowscn_column(const ObString &column_name) {
    return 0 == column_name.case_compare(ObString::make_string(OB_ORA_ROWSCN_STR));
  }
  static bool is_vec_index_for_ttl(const share::schema::ObIndexType index_type)
  { // different from schema::is_vec_index(which contains doc_rowkey/rowkey_doc)
    return share::schema::is_vec_domain_index(index_type) || share::schema::is_hybrid_vec_index(index_type);
  }

  OB_INLINE static bool is_compaction_ttl_type(const share::schema::ObTableSchema &table_schema)
  {
    return ObTTLDefinition::COMPACTION == table_schema.get_ttl_flag().ttl_type_;
  }

  static int check_ttl_column_valid(const share::schema::ObTableSchema &table_schema,
                                    const ObString &ttl_definition,
                                    const ObTTLFlag &ttl_flag,
                                    const uint64_t tenant_data_version);

  OB_INLINE static int check_create_append_only_engine_valid(const share::schema::ObTableSchema &table_schema,
                                                             const uint64_t tenant_id);

  // alter merge engine feature will support in 4.5.2
  // this function now is just a placeholder for future feature support
  OB_INLINE static int check_alter_merge_engine_valid(const share::schema::ObTableSchema &table_schema,
                                            const AlterTableSchema &alter_table_schema);

  OB_INLINE static int check_create_ttl_schema_valid(const share::schema::ObTableSchema &table_schema,
                                           const uint64_t tenant_id);

  template<typename Guard>
  static int check_alter_ttl_schema_valid(const share::schema::ObTableSchema &table_schema,
                                          const AlterTableSchema &alter_table_schema,
                                          const uint64_t tenant_id,
                                          Guard &schema_guard);

  OB_INLINE static int check_create_index_for_ttl_valid(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObIndexType index_type,
    const share::schema::ObTableSchema &index_table_schema);

  OB_INLINE static int check_create_trigger_for_ttl_valid(const share::schema::ObTableSchema &table_schema,
                                                const ObTriggerInfo &trigger_info);

  OB_INLINE static int check_create_foreign_key_for_ttl_valid(const share::schema::ObTableSchema &parent_table);

  OB_INLINE static int check_create_mlog_for_ttl_valid(const share::schema::ObTableSchema &base_table);

  // Why we need ttl_definition parameter?
  //   1. in ob_create_view_resolver.cpp, the ttl_definition is not set in table_schema, so we need to pass it.
  //   2. in root_service, the ttl_definition is set in table_schema, so we need to pass an empty string.
  OB_INLINE static int check_create_mview_for_ttl_valid(const schema::ObTableSchema &view_table,
                                                        const ObString &ttl_definition = ObString());

  OB_INLINE static int check_alter_column_for_append_only_valid(const obrpc::ObAlterTableArg &alter_table_arg,
                                                                const share::schema::ObTableSchema &table_schema,
                                                                const bool is_oracle_mode);

  OB_INLINE static int check_alter_partition_for_append_only_valid(const schema::ObTableSchema &table_schema,
                                                                   const obrpc::ObAlterTableArg::AlterPartitionType alter_partition_type);

  OB_INLINE static int check_exchange_partition_for_append_only_valid(const share::schema::ObTableSchema &base_table_schema,
                                                                      const share::schema::ObTableSchema &inc_table_schema);

  OB_INLINE static int check_alter_table_force_valid(const share::schema::ObTableSchema &table_schema);

  static int is_ttl_schema(
    const schema::ObTableSchema &table_schema,
    bool &is_ttl_schema);

  static int is_compaction_ttl_schema(
    const uint64_t tenant_data_version,
    const schema::ObTableSchema &table_schema,
    bool &is_compaction_ttl);
private:
  static int check_exist_user_defined_rowscn_column(const ObTableSchema &table_schema);
  template<typename Guard>
  static int check_index_for_ttl_valid(const share::schema::ObTableSchema &table_schema, Guard &schema_guard);
};

OB_INLINE int ObCompactionTTLUtil::check_create_append_only_engine_valid(
    const schema::ObTableSchema &table_schema,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  // When creating append_only table, must check the following constraints:
  if (table_schema.is_append_only_merge_engine()) {

    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      COMMON_LOG(WARN, "Fail to get data version", KR(ret));
    }

    // 0. append_only merge engine is supported after 4.5.1
    //    [x] (CREATE TABLE) when you create append_only table
    if (OB_SUCC(ret) && tenant_data_version < COMPACTION_TTL_CMP_DATA_VERSION) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "append_only merge engine is not supported under this version", K(ret), K(tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "append_only merge engine under this version is");
    }

    // 1. dynamic partition is not supported for append_only table
    //    [x] (CREATE TABLE) when you create append_only table
    if (OB_SUCC(ret) && table_schema.with_dynamic_partition_policy()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "dynamic partition is not supported for append_only table", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition for append_only table is");
    }

    // 2. tmp table don't support append only merge engine
    //    [x] (CREATE TABLE) when you create append_only table
    if (OB_SUCC(ret) && table_schema.is_tmp_table()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "tmp table don't support append only merge engine", K(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create tmp table with append only merge engine is");
    }
  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_alter_merge_engine_valid(const share::schema::ObTableSchema &table_schema,
                                                        const AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;

  // 1. dynamic partition policy is not supported for append_only table
  //    [x] (ALTER TABLE) when you add dynamic partition policy to append_only table
  if (table_schema.is_append_only_merge_engine() && alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::DYNAMIC_PARTITION_POLICY)) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "dynamic partition policy is not supported for append_only table", K(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition policy for append_only table is");
  }

  //! Below is placeholder for future feature support (4.5.2 support alter merge engine)
  //! Some checks are as follows:
  //! Notice that if you alter merge engine, you should also ensure the above checks are passed.

  // 1. check data version
  //    [x] (ALTER merge engine) when you alter merge engine (for future merge_engine alter feature)

  // 2. append_only table can not have delete/update event trigger
  //    [x] (ALTER merge engine) when you alter merge engine (for future merge_engine alter feature)

  // 3. TTL table can not alter to partial update merge engine (notice that there may be alter_ttl and alter_merge_engine at the same time)
  //    [x] (ALTER merge engine) when you alter merge engine (for future merge_engine alter feature)

  // 4. append_only table can not be alter to other merge engine
  //    [x] (ALTER merge engine) when you alter merge engine (for future merge_engine alter feature)

  // 5. dynamic partition policy is not supported for append_only table
  //    [x] (ALTER merge engine) when you alter merge engine (for future merge_engine alter feature)

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_ttl_schema_valid(const share::schema::ObTableSchema &table_schema,
                                                       const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  // When creating ttl table, must check the following constraints:
  if (table_schema.has_ttl_definition()) {
    if (is_compaction_ttl_type(table_schema)) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        COMMON_LOG(WARN, "Fail to get data version", KR(ret));
      }

      // 0. sql mode support ttl is in 4.5.1
      if (OB_SUCC(ret) && tenant_data_version < COMPACTION_TTL_CMP_DATA_VERSION) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "ttl in sql mode is not supported under this version", K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ttl in sql mode under this version is");
      }

      // 1 tmp table don't support ttl
      if (OB_SUCC(ret) && table_schema.is_tmp_table()) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "tmp table don't support ttl", K(ret), K(table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create tmp table with ttl is");
      }

      // 2. ttl table can not have delete event trigger
      //    [x] (CREATE TABLE) tirgger is built after create table. Don't need check

      // 3. check ttl column valid
      //    [x] (CREATE TABLE) check ttl column valid when create table
      if (OB_SUCC(ret) && OB_FAIL(check_ttl_column_valid(
          table_schema,
          table_schema.get_ttl_definition(),
          table_schema.get_ttl_flag(),
          tenant_data_version))) {
        COMMON_LOG(WARN, "fail to check ttl column valid", KR(ret), K(table_schema));
      }
    }

  }

  return ret;
}

template<typename Guard>
int ObCompactionTTLUtil::check_alter_ttl_schema_valid(const share::schema::ObTableSchema &table_schema,
                                                      const AlterTableSchema &alter_table_schema,
                                                      const uint64_t tenant_id,
                                                      Guard &schema_guard)
{
  int ret = OB_SUCCESS;

  // When altering ttl table, must check the following constraints:
  if (alter_table_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::TTL_DEFINITION)) {
    if (is_compaction_ttl_type(alter_table_schema)) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        COMMON_LOG(WARN, "Fail to get data version", KR(ret));
      }

      // 0. only user table can be altered to have ttl definition
      //    [x] (ALTER TTL) when you alter non-user table to add ttl definition
      if (OB_SUCC(ret) && !table_schema.is_user_table() && alter_table_schema.has_ttl_definition()) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "only user table can be altered to have ttl definition", K(ret), K(table_schema), K(alter_table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter non-user table to have ttl definition is");
      }

      // 1. ttl table can not have delete event trigger
      //    [x] (ALTER TTL) when you add ttl definition to table with create trigger
      if (OB_SUCC(ret) && table_schema.get_trigger_list().count() > 0 && alter_table_schema.has_ttl_definition()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_trigger_list().count(); i++) {
          const ObTriggerInfo *trigger_info = nullptr;

          if constexpr (std::is_same<Guard, schema::ObSchemaGetterGuard>::value) {
            if (OB_FAIL(schema_guard.get_trigger_info(table_schema.get_tenant_id(), table_schema.get_trigger_list().at(i), trigger_info))) {
              COMMON_LOG(WARN, "fail to get trigger info", K(ret), K(table_schema), K(table_schema.get_trigger_list().at(i)));
            }
          } else if constexpr (std::is_same<Guard, schema::ObLatestSchemaGuard>::value) {
            if (OB_FAIL(schema_guard.get_trigger_info(table_schema.get_trigger_list().at(i), trigger_info))) {
              COMMON_LOG(WARN, "fail to get trigger info", K(ret), K(table_schema), K(table_schema.get_trigger_list().at(i)));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(trigger_info)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "trigger info is null", K(ret), K(table_schema), K(table_schema.get_trigger_list().at(i)));
          } else if (trigger_info->has_delete_event()) {
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "ttl table can not have trigger with delete event", K(ret), K(table_schema), KPC(trigger_info));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table with delete event trigger to be ttl table is");
          }
        }
      }

      // 2. ttl table cannot be referenced by foreign keys from other tables
      //    [x] (ALTER TTL) make a table referenced by other foreign keys be ttl table
      if (OB_SUCC(ret) && table_schema.is_parent_table() && alter_table_schema.has_ttl_definition()) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "ttl table cannot be referenced by foreign keys from other tables", K(ret), K(table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "add ttl to table referenced by foreign keys from other tables is");
      }

      // 3. ttl table don't support vector index
      //    [x] (ALTER TTL) make a table with vector index be ttl table
      if (OB_SUCC(ret) && alter_table_schema.has_ttl_definition()) {
        if (OB_FAIL(check_index_for_ttl_valid(table_schema, schema_guard))) {
          COMMON_LOG(WARN, "fail to check index for ttl valid", KR(ret), K(table_schema));
        }
      }

      // 4. ttl + materialized view table is not supported
      //    [x] (ALTER MATERIALIZED VIEW) oceanbase don't support alter table options of materialized view, so don't need check

      // 5. ttl base table can not create mlog table
      //    [x] (ALTER TTL) make a table with mlog table be ttl table
      if (OB_SUCC(ret) && table_schema.has_mlog_table() && alter_table_schema.has_ttl_definition()) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "ttl table cannot have mlog table", K(ret), K(table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "add ttl to table with mlog is");
      }

      // 6. check ttl column valid
      //    [x] (ALTER TTL) check ttl column valid when alter table
      if (OB_SUCC(ret) && alter_table_schema.has_ttl_definition()) {
        if (OB_FAIL(check_ttl_column_valid(table_schema, alter_table_schema.get_ttl_definition(), alter_table_schema.get_ttl_flag(), tenant_data_version))) {
          COMMON_LOG(WARN, "fail to check ttl column valid", KR(ret), K(table_schema), K(alter_table_schema));
        } else if (OB_FAIL(check_exist_user_defined_rowscn_column(alter_table_schema))) {
          COMMON_LOG(WARN, "table is compaction ttl table, can't add user defined rowscn column", K(ret), K(alter_table_schema));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "add user defined rowscn column to compaction ttl table is");
        }
      }
    }

  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_index_for_ttl_valid(
  const share::schema::ObTableSchema &table_schema,
  const share::schema::ObIndexType index_type,
  const share::schema::ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;

  // When creating index for ttl table, must check the following constraints:
  if (table_schema.has_ttl_definition()) {
    if (is_compaction_ttl_type(table_schema)) {

      // 1. ttl table don't support vector index
      //    [x] (WHEN CREATE TABLE) create a ttl table with vector index
      //    [x] (WHEN CREATE INDEX) create vector index for ttl table
      if (is_vec_index_for_ttl(index_type) || is_fts_index(index_type)) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "ttl table don't support vector index or fts index", K(ret), K(table_schema), K(index_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create vector index or fts index on ttl table is");
      } else if (!is_compaction_ttl_merge_engine(index_table_schema.get_merge_engine_type())) {
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "not support to create index for ttl table with non-compaction ttl merge engine", K(ret), K(table_schema), K(index_table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating index for ttl table with non-compaction ttl merge engine is");
      }
    }

  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_trigger_for_ttl_valid(const share::schema::ObTableSchema &table_schema,
                                                            const ObTriggerInfo &trigger_info)
{
  int ret = OB_SUCCESS;

  if (is_compaction_ttl_type(table_schema)) {

    // 1. ttl table can not have delete event trigger
    //    [x] (ALTER TRIGGER) oceanbase don't support alter trigger
    //    [x] (CREATE TRIGGER)
    if (table_schema.has_ttl_definition() && trigger_info.has_delete_event()) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "ttl table can not have delete event trigger", K(ret), K(table_schema), K(trigger_info));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create delete event trigger for ttl table is");
    }

    // 2. append_only table can not have delete/update event trigger
    //    [x] (ALTER TRIGGER) oceanbase don't support alter trigger
    //    [x] (CREATE TRIGGER)
    if (OB_SUCC(ret) && table_schema.is_append_only_merge_engine() && (trigger_info.has_delete_event() || trigger_info.has_update_event())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "append_only table can not have delete/update event trigger", K(ret), K(table_schema), K(trigger_info));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create delete/update event trigger for append_only table is");
    }
  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_foreign_key_for_ttl_valid(const share::schema::ObTableSchema &parent_table)
{
  int ret = OB_SUCCESS;

  // When creating foreign key for ttl table, must check the following constraints:
  if (parent_table.has_ttl_definition()) {
    if (is_compaction_ttl_type(parent_table)) {

      // 1. ttl table cannot be referenced by foreign keys from other tables
      //    [x] (CREATE TABLE) create a table that has foreign keys referencing to ttl table
      //    [x] (ALTER TABLE ADD CONSTRAINT FOREIGN KEY) add foreign key to ttl table
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "foreign key cannot be referenced on ttl table", K(ret), K(parent_table));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "foreign key referenced on ttl table is");
    }

  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_mlog_for_ttl_valid(const share::schema::ObTableSchema &base_table)
{
  int ret = OB_SUCCESS;

  // When creating mlog for ttl table, must check the following constraints:
  if (base_table.has_ttl_definition()) {
    if (is_compaction_ttl_type(base_table)) {
      // 1. ttl base table can not create mlog table
      //    [x] (CREATE MATERIALIZED VIEW LOG) create a mlog table with ttl base table
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "ttl base table can not create mlog table", K(ret), K(base_table));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on ttl base table is");
    }

  }

  return ret;
}

OB_INLINE int ObCompactionTTLUtil::check_create_mview_for_ttl_valid(const share::schema::ObTableSchema &view_table,
                                                          const ObString &ttl_definition)
{
  int ret = OB_SUCCESS;

  // When creating mview for ttl table, must check the following constraints:
  if (view_table.has_ttl_definition() || !ttl_definition.empty()) {

    // 1. ttl + materialized view table is not supported
    //    [x] (CREATE MATERIALIZED VIEW) create a ttl table with materialized view
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "ttl table don't support materialized view", K(ret), K(view_table));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view with ttl definition is");

  }

  return ret;
}

int ObCompactionTTLUtil::check_alter_column_for_append_only_valid(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const share::schema::ObTableSchema &table_schema,
    const bool is_oracle_mode)
{
  using ColumnIterator = schema::ObTableSchema::const_column_iterator;

  int ret = OB_SUCCESS;

  if (table_schema.is_append_only_merge_engine()) {
    const AlterColumnSchema *alter_column_schema = nullptr;
    ColumnIterator it_begin = alter_table_arg.alter_table_schema_.column_begin();
    ColumnIterator it_end = alter_table_arg.alter_table_schema_.column_end();
    for (ColumnIterator it = it_begin; OB_SUCC(ret) && it != it_end; ++it) {
      if (OB_ISNULL(alter_column_schema = static_cast<const AlterColumnSchema *>(*it))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "unexpected null alter column", KR(ret), K(alter_table_arg), K(table_schema));
      } else {
        switch (alter_column_schema->alter_type_) {
          case OB_DDL_ADD_COLUMN:
            // 1. add auto increment column is not supported
            if (alter_column_schema->is_autoincrement()) {
              ret = OB_NOT_SUPPORTED;
              COMMON_LOG(WARN, "add auto increment column for append_only table is not supported", KR(ret), KPC(alter_column_schema), K(table_schema));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "add auto increment column for append_only table is");
            }

            // 2. add column with default value is not supported
            if (OB_SUCC(ret) && alter_column_schema->is_set_default_) {
              ret = OB_NOT_SUPPORTED;
              COMMON_LOG(WARN, "add column with default value is not supported", KR(ret), KPC(alter_column_schema), K(table_schema));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "add column with default value for append_only table is");
            }
            break;
          case OB_DDL_DROP_COLUMN:
            ret = OB_NOT_SUPPORTED;
            COMMON_LOG(WARN, "drop column for append_only table is not supported", KR(ret), KPC(alter_column_schema), K(table_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop column for append_only table is");
            break;
          case OB_DDL_CHANGE_COLUMN:
          case OB_DDL_MODIFY_COLUMN: {
            // 3. reduce precision or scale is not supported
            //    [x] (ALTER TABLE) modify column type to reduce precision or scale
            //    [x] (ALTER TABLE) modify column precision or scale to reduce precision or scale
            const ObColumnSchemaV2 *orig_column_schema = table_schema.get_column_schema(alter_column_schema->get_origin_column_name());
            const int32_t src_col_byte_len = orig_column_schema->get_data_length();
            const int32_t dst_col_byte_len = alter_column_schema->get_data_length();
            bool is_same = false;
            bool is_offline = false;
            bool is_type_reduction = false;
            if (OB_ISNULL(orig_column_schema)) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "unexpected null column schema", KR(ret), KPC(alter_column_schema), K(table_schema));
            } else if (OB_FAIL(ObTableSchema::check_is_exactly_same_type(*orig_column_schema, *alter_column_schema, is_same))) {
              COMMON_LOG(WARN, "failed to check is exactly same type", KR(ret), KPC(orig_column_schema), KPC(alter_column_schema));
            } else if (is_same) {
              // unrestricted
            } else if (OB_FAIL(ObTableSchema::check_alter_column_type(*orig_column_schema,
                                                                      *alter_column_schema,
                                                                      src_col_byte_len,
                                                                      dst_col_byte_len,
                                                                      is_oracle_mode,
                                                                      is_offline,
                                                                      is_type_reduction))) {
              COMMON_LOG(WARN, "failed to check alter column type", KR(ret), KPC(orig_column_schema), KPC(alter_column_schema));
            } else if (is_type_reduction || is_offline) {
              ret = OB_NOT_SUPPORTED;
              COMMON_LOG(WARN, "modify column type to make precision or scale decrease is not supported", KR(ret), K(orig_column_schema), K(alter_column_schema));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify column type to make precision or scale decrease for append_only table is");
            } else if (OB_FAIL(ObTableSchema::check_alter_column_accuracy(*orig_column_schema,
                                                                           *alter_column_schema,
                                                                           src_col_byte_len,
                                                                           dst_col_byte_len,
                                                                           is_oracle_mode,
                                                                           is_offline,
                                                                           is_type_reduction))) {
              COMMON_LOG(WARN, "failed to check alter column accuracy", KR(ret), K(orig_column_schema), KPC(alter_column_schema));
            } else if (is_type_reduction) {
              ret = OB_NOT_SUPPORTED;
              COMMON_LOG(WARN, "reduce precision or scale is not supported", KR(ret), K(orig_column_schema), KPC(alter_column_schema));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "reduce precision or scale for append_only table is");
            }

            break;
          }
          case OB_DDL_ALTER_COLUMN:
            // unrestricted
            break;
          default:
            // other operation is not alter-column-related, unrestricted
            break;
        }
      }
    }
  }

  return ret;
}

int ObCompactionTTLUtil::check_alter_partition_for_append_only_valid(
    const share::schema::ObTableSchema &table_schema,
    const obrpc::ObAlterTableArg::AlterPartitionType alter_partition_type)
{
  int ret = OB_SUCCESS;

  if (table_schema.is_append_only_merge_engine()) {
    // 1. truncate or drop partition is not supported for append_only table
    if (obrpc::ObAlterTableArg::TRUNCATE_PARTITION == alter_partition_type
        || obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_partition_type
        || obrpc::ObAlterTableArg::DROP_PARTITION == alter_partition_type
        || obrpc::ObAlterTableArg::DROP_SUB_PARTITION == alter_partition_type) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "truncate or drop partition for append_only table is not supported", KR(ret), K(table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "truncate or drop partition for append_only table is");
    }
  }

  return ret;
}

int ObCompactionTTLUtil::check_exchange_partition_for_append_only_valid(
    const share::schema::ObTableSchema &base_table_schema,
    const share::schema::ObTableSchema &inc_table_schema)
{
  int ret = OB_SUCCESS;

  if (base_table_schema.is_append_only_merge_engine() || inc_table_schema.is_append_only_merge_engine()) {
    // 1. exchange partition is not supported for append_only table
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "exchange partition for append_only table is not supported", KR(ret), K(base_table_schema), K(inc_table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "exchange partition for append_only table is");
  }

  return ret;
}

int ObCompactionTTLUtil::check_alter_table_force_valid(const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (table_schema.is_append_only_merge_engine()) {
    // 1. alter table force is not supported for append_only table
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "alter table force is not supported for append_only table", KR(ret), K(table_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table force for append_only table is");
  }

  return ret;
}

template<typename Guard>
int ObCompactionTTLUtil::check_index_for_ttl_valid(
  const share::schema::ObTableSchema &table_schema,
  Guard &schema_guard)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos = table_schema.get_simple_index_infos();
  const ObTableSchema *index_table_schema = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const ObAuxTableMetaInfo &simple_index_info = simple_index_infos.at(i);
    if (is_vec_index_for_ttl(simple_index_info.index_type_) || is_fts_index(simple_index_info.index_type_)) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "ttl table don't support vector index or fts index", K(ret), K(table_schema), K(simple_index_info.index_type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create vector index or fts index on ttl table is");
    } else if (OB_FAIL(schema_guard.get_table_schema(
        table_schema.get_tenant_id(),
        simple_index_info.table_id_,
        index_table_schema))) {
      COMMON_LOG(WARN, "failed to get index table schema", KR(ret), K(table_schema), K(simple_index_info.table_id_));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      COMMON_LOG(WARN, "index table schema should not be null", KR(ret), K(simple_index_info.table_id_));
    } else if (!is_compaction_ttl_merge_engine(index_table_schema->get_merge_engine_type())) {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "not support to have index for ttl table with non-compaction ttl merge engine", K(ret), K(table_schema), KPC(index_table_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "index for ttl table with non-compaction ttl merge engine is");
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_TTL_UTIL_H_
