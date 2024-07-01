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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_mlog_builder.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace transaction::tablelock;
using namespace storage;

namespace rootserver
{
ObMLogBuilder::MLogColumnUtils::MLogColumnUtils()
  : mlog_table_column_array_(),
    allocator_("MlogColUtil"),
    rowkey_count_(0)
{

}

ObMLogBuilder::MLogColumnUtils::~MLogColumnUtils()
{
  for (int64_t i = 0; i < mlog_table_column_array_.count(); ++i) {
    ObColumnSchemaV2 *column = mlog_table_column_array_.at(i);
    if (OB_NOT_NULL(column)) {
      column->~ObColumnSchemaV2();
      allocator_.free(column);
    }
  }
}

int ObMLogBuilder::MLogColumnUtils::check_column_type(
    const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (column_schema.get_meta_type().is_lob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "create materialized view log on lob columns is");
    LOG_WARN("create materialized view log on lob columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  } else if (column_schema.is_generated_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "create materialized view log on generated columns is");
    LOG_WARN("create materialized view log on generated columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  } else if (column_schema.is_xmltype()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on xmltype columns is");
    LOG_WARN("create materialized view log on xmltype columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  } else if (column_schema.is_json()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on json columns is");
    LOG_WARN("create materialized view log on json columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  } else if (column_schema.is_geometry()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on geometry columns is");
    LOG_WARN("create materialized view log on geometry columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  } else if (column_schema.is_udt_related_column(lib::is_oracle_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on udt columns is");
    LOG_WARN("create materialized view log on udt columns is not supported",
        KR(ret), K(column_schema.get_column_name_str()));
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_special_columns()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_sequence_column())) {
    LOG_WARN("failed to add sequence column", KR(ret));
  } else if (OB_FAIL(add_dmltype_column())) {
    LOG_WARN("failed to add dmltype column", KR(ret));
  } else if (OB_FAIL(add_old_new_column())) {
    LOG_WARN("failed to add old new column", KR(ret));
  }
  return ret;
}

// sequence_no is part of mlog's rowkey
// its rowkey position will be set later
int ObMLogBuilder::MLogColumnUtils::add_sequence_column()
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *rowkey_column = nullptr;
  if (OB_FAIL(alloc_column(rowkey_column))) {
    LOG_WARN("failed to alloc column", KR(ret));
  } else {
    rowkey_column->set_autoincrement(false);
    rowkey_column->set_is_hidden(false);
    rowkey_column->set_nullable(false);
    rowkey_column->set_rowkey_position(0);
    rowkey_column->set_order_in_rowkey(ObOrderType::ASC);
    rowkey_column->set_column_id(OB_MLOG_SEQ_NO_COLUMN_ID);
    rowkey_column->set_data_type(ObIntType);
    rowkey_column->set_charset_type(CHARSET_BINARY);
    rowkey_column->set_collation_type(CS_TYPE_BINARY);
    rowkey_column->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    if (OB_FAIL(rowkey_column->set_column_name(OB_MLOG_SEQ_NO_COLUMN_NAME))) {
      LOG_WARN("failed to set column name", KR(ret));
    } else if (OB_FAIL(mlog_table_column_array_.push_back(rowkey_column))) {
      LOG_WARN("failed to push back column to mlog table column array", KR(ret), KPC(rowkey_column));
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_dmltype_column()
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = nullptr;
  if (OB_FAIL(alloc_column(column))) {
    LOG_WARN("failed to alloc column", KR(ret));
  } else {
    column->set_autoincrement(false);
    column->set_is_hidden(false);
    column->set_rowkey_position(0);
    column->set_index_position(0);
    column->set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    column->set_data_type(ColumnType::ObVarcharType);
    column->set_data_length(1);
    column->set_column_id(OB_MLOG_DML_TYPE_COLUMN_ID);
    if (OB_FAIL(column->set_column_name(OB_MLOG_DML_TYPE_COLUMN_NAME))) {
      LOG_WARN("failed to set column name", KR(ret));
    } else if (OB_FAIL(mlog_table_column_array_.push_back(column))) {
      LOG_WARN("failed to push back column to mlog table column array", KR(ret), KP(column));
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_old_new_column()
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *column = nullptr;
  if (OB_FAIL(alloc_column(column))) {
    LOG_WARN("failed to alloc column", KR(ret));
  } else {
    column->set_autoincrement(false);
    column->set_is_hidden(false);
    column->set_rowkey_position(0);
    column->set_index_position(0);
    column->set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    column->set_data_type(ColumnType::ObVarcharType);
    column->set_data_length(1);
    column->set_column_id(OB_MLOG_OLD_NEW_COLUMN_ID);
    if (OB_FAIL(column->set_column_name(OB_MLOG_OLD_NEW_COLUMN_NAME))) {
      LOG_WARN("failed to set column name", KR(ret));
    } else if (OB_FAIL(mlog_table_column_array_.push_back(column))) {
      LOG_WARN("failed to push back column to mlog table column array", KR(ret), KP(column));
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_base_table_pk_columns(
    ObRowDesc &row_desc,
    const ObTableSchema &base_table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_ids;
  if (OB_FAIL(base_table_schema.get_rowkey_column_ids(column_ids))) {
    LOG_WARN("failed to get rowkey column ids", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_ids.count()); ++i) {
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      ObColumnSchemaV2 *ref_column = nullptr;
      if (OB_FAIL(alloc_column(ref_column))) {
        LOG_WARN("failed to alloc column", KR(ret));
      } else if (OB_ISNULL(rowkey_column =
          base_table_schema.get_column_schema(column_ids.at(i).col_id_))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("column not exist", KR(ret));
      } else if (OB_FAIL(row_desc.add_column_desc(rowkey_column->get_table_id(),
                                          rowkey_column->get_column_id()))) {
        LOG_WARN("failed to add column desc to row desc", KR(ret),
            K(rowkey_column->get_table_id()), K(rowkey_column->get_column_id()));
      } else if (OB_FAIL(ref_column->assign(*rowkey_column))) {
        LOG_WARN("failed to assign rowkey_column to ref_column",
            KR(ret), KPC(rowkey_column));
      } else {
        // preserve the rowkey position of the column
        ref_column->set_autoincrement(false);
        ref_column->set_is_hidden(false);
        ref_column->set_index_position(0);
        ref_column->set_prev_column_id(UINT64_MAX);
        ref_column->set_next_column_id(UINT64_MAX);
        ref_column->set_column_id(ObTableSchema::gen_mlog_col_id_from_ref_col_id(
                                                rowkey_column->get_column_id()));
        if (base_table_schema.is_heap_table()
            && OB_FAIL(ref_column->set_column_name(OB_MLOG_ROWID_COLUMN_NAME))) {
          LOG_WARN("failed to set column name", KR(ret));
        } else if (OB_FAIL(mlog_table_column_array_.push_back(ref_column))) {
          LOG_WARN("failed to push back column to mlog table column array",
              KR(ret), KP(ref_column));
        } else {
          ++rowkey_count_;
        }
      }
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_base_table_columns(
    const ObCreateMLogArg &create_mlog_arg,
    ObRowDesc &row_desc,
    const ObTableSchema &base_table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < create_mlog_arg.store_columns_.count()); ++i) {
    const ObString &column_name = create_mlog_arg.store_columns_.at(i);
    const ObColumnSchemaV2 *data_column = nullptr;
    ObColumnSchemaV2 *ref_column = nullptr;
    if (OB_FAIL(alloc_column(ref_column))) {
      LOG_WARN("failed to alloc column", KR(ret));
    } else if (OB_ISNULL(data_column = base_table_schema.get_column_schema(column_name))) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("failed to get column schema", KR(ret), K(column_name));
    } else if (OB_FAIL(check_column_type(*data_column))) {
      LOG_WARN("failed to check column type", KR(ret), KPC(data_column));
    } else if (OB_INVALID_INDEX != row_desc.get_idx(
        data_column->get_table_id(),
        data_column->get_column_id())) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
    } else if (OB_FAIL(row_desc.add_column_desc(data_column->get_table_id(),
                                        data_column->get_column_id()))) {
      LOG_WARN("failed to add column desc to row desc", KR(ret),
          K(data_column->get_table_id()), K(data_column->get_column_id()));
    } else if (OB_FAIL(ref_column->assign(*data_column))) {
      LOG_WARN("failed to assign rowkey_column to ref_column",
          KR(ret), KPC(data_column));
    } else {
      ref_column->set_autoincrement(false);
      ref_column->set_is_hidden(false);
      ref_column->set_rowkey_position(0);
      ref_column->set_index_position(0);
      ref_column->set_prev_column_id(UINT64_MAX);
      ref_column->set_next_column_id(UINT64_MAX);
      ref_column->set_column_id(ObTableSchema::gen_mlog_col_id_from_ref_col_id(
                                                  data_column->get_column_id()));
      if (OB_FAIL(mlog_table_column_array_.push_back(ref_column))) {
        LOG_WARN("failed to push back column to mlog table column array",
            KR(ret), KP(ref_column));
      }
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::implicit_add_base_table_part_key_columns(
    const ObPartitionKeyInfo &part_key_info,
    ObRowDesc &row_desc,
    const ObTableSchema &base_table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < part_key_info.get_size()); ++i) {
    const ObRowkeyColumn *rowkey_column = nullptr;
    ObColumnSchemaV2 *ref_column = nullptr;
    if (OB_FAIL(alloc_column(ref_column))) {
      LOG_WARN("failed to alloc column", KR(ret));
    } else if (OB_ISNULL(rowkey_column = part_key_info.get_column(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey column is null", KR(ret));
    } else {
      const uint64_t column_id = rowkey_column->column_id_;
      const ObColumnSchemaV2 *part_key_column = nullptr;
      if (OB_ISNULL(part_key_column = base_table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("failed to get column schema", KR(ret), K(column_id));
      } else if (OB_FAIL(check_column_type(*part_key_column))) {
        LOG_WARN("failed to check column type", KR(ret), KPC(part_key_column));
      } else if (OB_INVALID_INDEX != row_desc.get_idx(
          part_key_column->get_table_id(), part_key_column->get_column_id())) {
        // bypass
      } else if (OB_FAIL(row_desc.add_column_desc(
          part_key_column->get_table_id(), part_key_column->get_column_id()))) {
        LOG_WARN("failed to add column desc to row desc", KR(ret),
            K(part_key_column->get_table_id()), K(part_key_column->get_column_id()));
      } else if (OB_FAIL(ref_column->assign(*part_key_column))) {
        LOG_WARN("failed to assign rowkey_column to ref_column",
            KR(ret), KPC(part_key_column));
      } else {
        ref_column->set_autoincrement(false);
        ref_column->set_is_hidden(false);
        ref_column->set_rowkey_position(0);
        ref_column->set_index_position(0);
        ref_column->set_nullable(false);
        ref_column->set_prev_column_id(UINT64_MAX);
        ref_column->set_next_column_id(UINT64_MAX);
        ref_column->set_column_id(ObTableSchema::gen_mlog_col_id_from_ref_col_id(
                                                    part_key_column->get_column_id()));
        if (OB_FAIL(mlog_table_column_array_.push_back(ref_column))) {
          LOG_WARN("failed to push back column to mlog table column array",
              KR(ret), KP(ref_column));
        }
      }
    }
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::add_base_table_part_key_columns(
    ObRowDesc &row_desc,
    const ObTableSchema &base_table_schema)
{
  int ret = OB_SUCCESS;
  const ObPartitionKeyInfo &part_key_info = base_table_schema.get_partition_key_info();
  const ObPartitionKeyInfo &sub_part_key_info = base_table_schema.get_subpartition_key_info();
  if (OB_FAIL(implicit_add_base_table_part_key_columns(
      part_key_info, row_desc, base_table_schema))) {
    LOG_WARN("failed to implicit add base table part key columns", KR(ret));
  } else if (OB_FAIL(implicit_add_base_table_part_key_columns(
      sub_part_key_info, row_desc, base_table_schema))) {
    LOG_WARN("failed to implicit add base table sub part key columns", KR(ret));
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::alloc_column(ObColumnSchemaV2 *&column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSchemaUtils::alloc_schema<ObColumnSchemaV2>(allocator_, column))) {
    LOG_WARN("failed to alloc column schema", KR(ret));
  }
  return ret;
}

int ObMLogBuilder::MLogColumnUtils::construct_mlog_table_columns(
    ObTableSchema &mlog_schema)
{
  int ret = OB_SUCCESS;
  // sort mlog_table_column_array first
  struct ColumnSchemaCmp {
    inline bool operator()(const ObColumnSchemaV2 *a, const ObColumnSchemaV2 *b) {
      return (a->get_column_id() < b->get_column_id());
    }
  };
  lib::ob_sort(mlog_table_column_array_.begin(), mlog_table_column_array_.end(), ColumnSchemaCmp());

  for (int64_t i = 0; OB_SUCC(ret) && (i < mlog_table_column_array_.count()); ++i) {
    ObColumnSchemaV2 *column = mlog_table_column_array_.at(i);
    // columns referencing base table primary keys have already been assigned rowkey position
    if ((column->is_part_key_column() || column->is_subpart_key_column())
        && (0 == column->get_rowkey_position())) {
      column->set_rowkey_position(++rowkey_count_);
    } else if (OB_MLOG_SEQ_NO_COLUMN_ID == column->get_column_id()) {
      // mlog seq_no column must be the last rowkey
      column->set_rowkey_position(++rowkey_count_);
    }
    if (OB_FAIL(mlog_schema.add_column(*column))) {
      LOG_WARN("failed to add column to mlog schema", KR(ret), KPC(column));
    }
  }
  return ret;
}

ObMLogBuilder::ObMLogBuilder(ObDDLService &ddl_service)
    : ddl_service_(ddl_service),
      mlog_column_utils_(),
      is_inited_(false)
{

}

ObMLogBuilder::~ObMLogBuilder()
{

}

int ObMLogBuilder::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMLogBuilder init twice", KR(ret));
  } else if (!ddl_service_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ddl service not init", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMLogBuilder::create_mlog(
    ObSchemaGetterGuard &schema_guard,
    const ObCreateMLogArg &create_mlog_arg,
    ObCreateMLogRes &create_mlog_res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = create_mlog_arg.tenant_id_;
  uint64_t compat_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogBuilder not init", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("materialized view log before version 4.3 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "materialized view log before version 4.3 is");
  } else if (!create_mlog_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_mlog_arg));
  } else {
    uint64_t base_table_id = OB_INVALID_ID;
    bool in_tenant_space = true;
    const ObTableSchema *base_table_schema = nullptr;
    schema_guard.set_session_id(create_mlog_arg.session_id_);
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                              create_mlog_arg.database_name_,
                                              create_mlog_arg.table_name_,
                                              false /* is_index */,
                                              base_table_schema))) {
      LOG_WARN("failed to get table schema", KR(ret), K(create_mlog_arg));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(create_mlog_arg.database_name_),
          to_cstring(create_mlog_arg.table_name_));
      LOG_WARN("table not exist", KR(ret), K(create_mlog_arg));
    } else if(!base_table_schema->is_user_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("create materialized view log on a non-user table is not supported",
          KR(ret), K(base_table_schema->get_table_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on a non-user table is");
    } else if (base_table_schema->has_mlog_table()) {
      ret = OB_ERR_MLOG_EXIST;
      LOG_WARN("a materialized view log already exists on table",
          K(create_mlog_arg.table_name_), K(base_table_schema->get_mlog_tid()));
      LOG_USER_ERROR(OB_ERR_MLOG_EXIST, to_cstring(create_mlog_arg.table_name_));
    } else if (FALSE_IT(base_table_id = base_table_schema->get_table_id())) {
    } else if (OB_FAIL(ObSysTableChecker::is_tenant_space_table_id(base_table_id, in_tenant_space))) {
      LOG_WARN("failed to check table in tenant space", KR(ret), K(base_table_id));
    } else if (is_inner_table(base_table_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("create mlog on inner table is not supported", KR(ret), K(base_table_id));
    } else if (base_table_schema->is_in_splitting()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("can not create mlog during splitting", KR(ret), K(create_mlog_arg));
    } else if (OB_FAIL(ddl_service_.check_restore_point_allow(
          tenant_id, *base_table_schema))) {
      LOG_WARN("failed to check restore point allow", KR(ret), K(tenant_id), K(base_table_id));
    } else if (OB_FAIL(ddl_service_.check_fk_related_table_ddl(
          *base_table_schema, ObDDLType::DDL_CREATE_INDEX))) {
      LOG_WARN("check whether the foreign key related table is executing ddl failed", KR(ret));
    } else if (OB_FAIL(do_create_mlog(schema_guard,
                                      create_mlog_arg,
                                      *base_table_schema,
                                      compat_version,
                                      create_mlog_res))) {
      LOG_WARN("failed to do create mlog", KR(ret), K(create_mlog_arg));
    }
  }
  return ret;
}

int ObMLogBuilder::do_create_mlog(
    ObSchemaGetterGuard &schema_guard,
    const ObCreateMLogArg &create_mlog_arg,
    const ObTableSchema &base_table_schema,
    const uint64_t tenant_data_version,
    ObCreateMLogRes &create_mlog_res)
{
  int ret = OB_SUCCESS;
  HEAP_VARS_3((ObTableSchema, mlog_schema),
              (ObTableSchema, src_table_schema),
              (obrpc::ObCreateIndexArg, create_index_arg)) {
    ObDDLTaskRecord task_record;
    ObArenaAllocator allocator("DdlTaskTmp");
    ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
    int64_t refreshed_schema_version = 0;
    const uint64_t tenant_id = base_table_schema.get_tenant_id();
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(),
                                   tenant_id,
                                   refreshed_schema_version))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(src_table_schema.assign(base_table_schema))) {
      LOG_WARN("failed to assign table schema", KR(ret));
    } else if (!src_table_schema.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to copy table schema", KR(ret));
    } else if (OB_FAIL(generate_mlog_schema(schema_guard, create_mlog_arg, src_table_schema, mlog_schema))) {
      LOG_WARN("failed to generate schema", KR(ret), K(create_mlog_arg), K(src_table_schema));
    } else if (OB_FAIL(ddl_service_.create_mlog_table(trans, create_mlog_arg, tenant_data_version, schema_guard, mlog_schema))) {
      LOG_WARN("failed to create mlog table", KR(ret), K(create_mlog_arg), K(mlog_schema));
    } else {
      // submit build mlog task
      // creating mlog reuses create_index ddl task
      create_index_arg.tenant_id_ = create_mlog_arg.tenant_id_;
      create_index_arg.data_table_id_ = create_mlog_arg.base_table_id_;
      create_index_arg.index_table_id_ = OB_INVALID_ID;
      create_index_arg.session_id_ = create_mlog_arg.session_id_;
      create_index_arg.index_action_type_ = ObIndexArg::ADD_MLOG;
      ObCreateDDLTaskParam param(mlog_schema.get_tenant_id(),
                                  ObDDLType::DDL_CREATE_MLOG,
                                  &src_table_schema,
                                  &mlog_schema,
                                  0 /*object_id*/,
                                  mlog_schema.get_schema_version(),
                                  create_mlog_arg.parallelism_,
                                  create_mlog_arg.consumer_group_id_,
                                  &allocator,
                                  &create_index_arg);
      param.tenant_data_version_ = tenant_data_version;
      ObTableLockOwnerID owner_id;
      if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().create_ddl_task(param, trans, task_record))) {
        LOG_WARN("failed to submit create mlog task", KR(ret));
      } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE,
                                              task_record.task_id_))) {
        LOG_WARN("failed to get owner id", K(ret), K(task_record.task_id_));
      } else if (OB_FAIL(ObDDLLock::lock_for_add_drop_index(
          src_table_schema,
          nullptr/*inc_data_tablet_ids*/,
          nullptr/*del_data_tablet_ids*/,
          mlog_schema,
          owner_id,
          trans))) {
        LOG_WARN("failed to lock online ddl lock", KR(ret));
      } else {
        create_mlog_res.mlog_table_id_ = mlog_schema.get_table_id();
        create_mlog_res.schema_version_ = mlog_schema.get_schema_version();
        create_mlog_res.task_id_ = task_record.task_id_;
      }
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("failed to publish schema", KR(ret));
      } else if (OB_FAIL(GCTX.root_service_->get_ddl_task_scheduler().schedule_ddl_task(task_record))) {
        LOG_WARN("failed to schedule ddl task", KR(ret), K(task_record));
      }
    }
  } // end HEAP_VAR()
  return ret;
}

int ObMLogBuilder::generate_mlog_schema(
    ObSchemaGetterGuard &schema_guard,
    const ObCreateMLogArg &create_mlog_arg,
    const ObTableSchema &base_table_schema,
    ObTableSchema &mlog_schema)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogBuilder not init", KR(ret));
  } else if (OB_UNLIKELY(!base_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data schema", KR(ret), K(base_table_schema));
  } else {
    if (OB_FAIL(set_basic_infos(schema_guard, create_mlog_arg, base_table_schema, mlog_schema))) {
      LOG_WARN("failed to set basic infos", KR(ret));
    } else if (OB_FAIL(set_table_columns(create_mlog_arg, base_table_schema, mlog_schema))) {
      LOG_WARN("failed to set table columns", KR(ret));
    } else if (OB_FAIL(set_table_options(create_mlog_arg, base_table_schema, mlog_schema))) {
      LOG_WARN("failed to set table options", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if ((base_table_schema.get_part_level() > 0)
        && OB_FAIL(mlog_schema.assign_partition_schema(base_table_schema))) {
      LOG_WARN("failed to assign partition schema", KR(ret), K(mlog_schema));
    } else if (OB_FAIL(ddl_service_.try_format_partition_schema(mlog_schema))) {
      LOG_WARN("failed to format partitionn schema", KR(ret));
    } else if (OB_FAIL(ddl_service_.generate_object_id_for_partition_schema(mlog_schema))) {
      LOG_WARN("failed to generate object id for patition schema", KR(ret), K(mlog_schema));
    } else if (OB_FAIL(ddl_service_.generate_tablet_id(mlog_schema))) {
      LOG_WARN("failed to generate new tablet id", KR(ret), K(mlog_schema));
    }
  }
  return ret;
}

int ObMLogBuilder::set_basic_infos(
    ObSchemaGetterGuard &schema_guard,
    const ObCreateMLogArg &create_mlog_arg,
    const ObTableSchema &base_table_schema,
    ObTableSchema &mlog_schema)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database = NULL;
  const uint64_t tenant_id = base_table_schema.get_tenant_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogBuilder not init", KR(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(
        tenant_id, base_table_schema.get_database_id(), database))) {
    LOG_WARN("failed to get database_schema", K(ret), K(tenant_id),
        "database_id", base_table_schema.get_database_id());
  } else if (OB_ISNULL(database)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database_schema is null", K(ret), "database_id", base_table_schema.get_database_id());
  } else if (OB_UNLIKELY(!base_table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data schema", KR(ret), K(base_table_schema));
  } else {
    ObString mlog_table_name;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    bool is_oracle_mode = false;
    if (OB_FAIL(base_table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("failed to check if oracle compat mode", KR(ret));
    } else if (OB_FAIL(ObTableSchema::build_mlog_table_name(allocator,
        create_mlog_arg.table_name_, mlog_table_name, is_oracle_mode))) {
      LOG_WARN("failed to build mlog table name", KR(ret), K(create_mlog_arg.table_name_));
    } else if (OB_FAIL(mlog_schema.set_table_name(mlog_table_name))) {
      LOG_WARN("failed to set table name", KR(ret), K(mlog_table_name));
    } else {
      mlog_schema.set_table_mode(base_table_schema.get_table_mode_flag());
      mlog_schema.set_table_type(MATERIALIZED_VIEW_LOG);
      mlog_schema.set_index_status(ObIndexStatus::INDEX_STATUS_UNAVAILABLE);
      mlog_schema.set_duplicate_scope(base_table_schema.get_duplicate_scope());
      mlog_schema.set_table_state_flag(base_table_schema.get_table_state_flag());
      mlog_schema.set_table_id(create_mlog_arg.mlog_table_id_);
      mlog_schema.set_data_table_id(base_table_schema.get_table_id());
      mlog_schema.set_tenant_id(base_table_schema.get_tenant_id());
      mlog_schema.set_database_id(base_table_schema.get_database_id());
      mlog_schema.set_tablegroup_id(OB_INVALID_ID);
      mlog_schema.set_def_type(base_table_schema.get_def_type());
      mlog_schema.set_part_level(base_table_schema.get_part_level());
      mlog_schema.set_charset_type(base_table_schema.get_charset_type());
      mlog_schema.set_collation_type(base_table_schema.get_collation_type());
      mlog_schema.set_row_store_type(base_table_schema.get_row_store_type());
      mlog_schema.set_store_format(base_table_schema.get_store_format());
      mlog_schema.set_storage_format_version(base_table_schema.get_storage_format_version());
      mlog_schema.set_tablet_size(base_table_schema.get_tablet_size());
      mlog_schema.set_autoinc_column_id(0);
      mlog_schema.set_progressive_merge_num(base_table_schema.get_progressive_merge_num());
      mlog_schema.set_progressive_merge_round(base_table_schema.get_progressive_merge_round());
      if (OB_FAIL(mlog_schema.set_compress_func_name(base_table_schema.get_compress_func_name()))) {
        LOG_WARN("failed to set compress func name", KR(ret));
      } else if (OB_INVALID_ID != mlog_schema.get_tablespace_id()) {
        const ObTablespaceSchema *tablespace_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tablespace_schema(
            mlog_schema.get_tenant_id(), mlog_schema.get_tablespace_id(), tablespace_schema))) {
          LOG_WARN("failed to get tablespace schema", KR(ret));
        } else if (OB_UNLIKELY(OB_ISNULL(tablespace_schema))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablespace schema is null", KR(ret));
        } else if (OB_FAIL(mlog_schema.set_encrypt_key(tablespace_schema->get_encrypt_key()))) {
          LOG_WARN("failed to set encrypt key", KR(ret));
        } else {
          mlog_schema.set_master_key_id(tablespace_schema->get_master_key_id());
        }
      }
    }
  }
  return ret;
}

int ObMLogBuilder::set_table_columns(
    const ObCreateMLogArg &create_mlog_arg,
    const ObTableSchema &base_table_schema,
    ObTableSchema &mlog_schema)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogBuilder not init", KR(ret));
  } else {
    HEAP_VAR(ObRowDesc, row_desc) {
      if (base_table_schema.is_heap_table()) {
        if (create_mlog_arg.with_primary_key_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create mlog on heap table cannot use with primary key option",
              KR(ret), K(base_table_schema.is_heap_table()), K(create_mlog_arg.with_primary_key_));
        } else if (!create_mlog_arg.with_rowid_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create mlog on heap table should use with rowid option",
              KR(ret), K(base_table_schema.is_heap_table()), K(create_mlog_arg.with_rowid_));
        }
      } else {
        if (create_mlog_arg.with_rowid_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create mlog on non-heap table cannot use with rowid option",
              KR(ret), K(base_table_schema.is_heap_table()), K(create_mlog_arg.with_rowid_));
        } else if (!create_mlog_arg.with_primary_key_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create mlog on non-heap table should use with primary key option",
              KR(ret), K(base_table_schema.is_heap_table()), K(create_mlog_arg.with_primary_key_));
        }
      }

      // the primary key of a mlog is consist of:
      // the base table pk + the base table part key + the mlog sequence no
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(mlog_column_utils_.add_base_table_pk_columns(
          row_desc, base_table_schema))) {
        LOG_WARN("failed to add base table pk columns", KR(ret));
      } else if (OB_FAIL(mlog_column_utils_.add_base_table_columns(
          create_mlog_arg, row_desc, base_table_schema))) {
        LOG_WARN("failed to add base table columns", KR(ret));
      } else if (OB_FAIL(mlog_column_utils_.add_base_table_part_key_columns(
          row_desc, base_table_schema))) {
        LOG_WARN("failed to add base table part key columns", KR(ret));
      } else if (OB_FAIL(mlog_column_utils_.add_special_columns())) {
        LOG_WARN("failed to add special columns", KR(ret));
      } else if (OB_FAIL(mlog_column_utils_.construct_mlog_table_columns(mlog_schema))) {
        LOG_WARN("failed to construct mlog table columns", KR(ret));
      }
    }
  }
  return ret;
}

int ObMLogBuilder::set_table_options(
    const ObCreateMLogArg &create_mlog_arg,
    const ObTableSchema &base_table_schema,
    ObTableSchema &mlog_schema)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogBuilder not init", KR(ret));
  } else {
    mlog_schema.set_block_size(create_mlog_arg.mlog_schema_.get_block_size());
    mlog_schema.set_pctfree(create_mlog_arg.mlog_schema_.get_pctfree());
    mlog_schema.set_dop(create_mlog_arg.mlog_schema_.get_dop());
    mlog_schema.set_tablespace_id(create_mlog_arg.mlog_schema_.get_tablespace_id());
    mlog_schema.set_comment(create_mlog_arg.mlog_schema_.get_comment());
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
