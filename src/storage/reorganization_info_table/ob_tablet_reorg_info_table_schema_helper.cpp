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

#include "lib/ob_errno.h"
#include "lib/worker.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "common/object/ob_object.h"
#include "ob_tablet_reorg_info_table_schema_helper.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_schema_macro_define.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{

ObTabletReorgInfoTableSchemaDef::ObTabletReorgInfoTableSchemaDef()
{
}

ObTabletReorgInfoTableSchemaDef::~ObTabletReorgInfoTableSchemaDef()
{
}

int ObTabletReorgInfoTableSchemaDef::get_table_column_info(
    const int64_t column_idx,
    int64_t &column_id,
    const char *&column_name,
    int64_t &column_length,
    ObObjMeta &meta,
    int64_t &rowkey_position)
{
  int ret = OB_SUCCESS;
  column_id = 0;
  column_name = nullptr;
  column_length = 0;
  meta.reset();
  rowkey_position = 0;

  if (column_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get table column info get invalid argument", K(ret), K(column_idx));
  } else if (column_idx >= REORG_INFO_ROW_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column idx is bigger than row column count, unexpected", K(ret), K(column_idx));
  } else {
    column_name = TABLE_COLUMN_NAMES[column_idx];
    column_length = COLUMN_DATA_LENGTH[column_idx];
    column_id = TABLE_COLUMN_ID[column_idx];
    const REORG_INFO_TABLE_SCHEMA_COL_IDX type = static_cast<REORG_INFO_TABLE_SCHEMA_COL_IDX>(column_idx);
    switch (type) {
    case REORG_INFO_TABLE_SCHEMA_COL_IDX::TABLET_ID : {
      meta.set_int();
      rowkey_position = 1;
      break;
    }
    case REORG_INFO_TABLE_SCHEMA_COL_IDX::REORGANIZATION_SCN : {
      meta.set_int();
      rowkey_position = 2;
      break;
    }
    case REORG_INFO_TABLE_SCHEMA_COL_IDX::DATA_TYPE : {
      meta.set_int();
      rowkey_position = 3;
      break;
    }
    case REORG_INFO_TABLE_SCHEMA_COL_IDX::DATA_VALUE : {
      meta.set_binary();
      rowkey_position = 0;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid type for fail", K(ret), K(type));
    }
    }
  }
  return ret;
}


ObTabletReorgInfoTableSchemaHelper::ObTabletReorgInfoTableSchemaHelper()
  : is_inited_(false),
    allocator_("MTSchema"),
    table_schema_(),
    storage_schema_(),
    rowkey_read_info_(),
    column_ids_()
{
}

ObTabletReorgInfoTableSchemaHelper::~ObTabletReorgInfoTableSchemaHelper()
{
  reset();
}

ObTabletReorgInfoTableSchemaHelper &ObTabletReorgInfoTableSchemaHelper::get_instance()
{
  static ObTabletReorgInfoTableSchemaHelper helper;
  return helper;
}

int ObTabletReorgInfoTableSchemaHelper::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    const uint64_t tenant_id = 1; // mock
    if (OB_FAIL(build_table_schema_(tenant_id, ObTabletReorgInfoTableSchemaDef::DATABASE_ID, ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_ID,
        ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_NAME, table_schema_))) {
      LOG_WARN("fail to build table schema", K(ret));
    } else if (OB_UNLIKELY(!table_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret), K_(table_schema));
    } else if (OB_FAIL(storage_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("fail to init storage schema", K(ret));
    } else if (OB_UNLIKELY(!storage_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage schema", K(ret), K_(storage_schema));
    } else if (OB_FAIL(build_rowkey_read_info_(allocator_, storage_schema_, rowkey_read_info_))) {
      LOG_WARN("fail to build rowkey read info", K(ret));
    } else if (OB_UNLIKELY(!rowkey_read_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey read info", K(ret), K_(rowkey_read_info));
    } else if (OB_FAIL(build_column_ids_(table_schema_))) {
      LOG_WARN("failed to build column ids", K(ret), K(table_schema_));
    } else {
      is_inited_ = true;
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

void ObTabletReorgInfoTableSchemaHelper::reset()
{
  rowkey_read_info_.reset();
  storage_schema_.reset();
  table_schema_.reset();
  allocator_.reset();
  is_inited_ = false;
}

const ObStorageSchema *ObTabletReorgInfoTableSchemaHelper::get_storage_schema() const
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &storage_schema_;
  }

  return ptr;
}

const share::schema::ObTableSchema *ObTabletReorgInfoTableSchemaHelper::get_table_schema() const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &table_schema_;
  }

  return ptr;
}

const ObRowkeyReadInfo *ObTabletReorgInfoTableSchemaHelper::get_rowkey_read_info() const
{
  int ret = OB_SUCCESS;
  const ObRowkeyReadInfo *ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &rowkey_read_info_;
  }

  return ptr;
}

int ObTabletReorgInfoTableSchemaHelper::build_table_schema_(
    const uint64_t tenant_id,
    const int64_t database_id,
    const uint64_t table_id,
    const char *table_name,
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.reset();

  int64_t column_id = 0;
  const char *column_name = nullptr;
  int64_t column_length = 0;
  ObObjMeta meta;
  int64_t rowkey_position = 0;

  table_schema.set_tenant_id(tenant_id);
  table_schema.set_database_id(database_id);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
  table_schema.set_table_name(ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_NAME);
  table_schema.set_schema_version(ObTabletReorgInfoTableSchemaDef::REORG_INFO_SCHEMA_VERSION);

  ObColumnSchemaV2 column_schema;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObTabletReorgInfoTableSchemaDef::REORG_INFO_ROW_COLUMN_CNT; ++i) {
    column_schema.reset();
    if (OB_FAIL(ObTabletReorgInfoTableSchemaDef::get_table_column_info(i, column_id, column_name, column_length, meta, rowkey_position))) {
      LOG_WARN("failed to get table column info", K(ret), K(i));
    } else if (OB_FAIL(build_column_schema_(
        tenant_id,
        table_id,
        column_id,
        column_name,
        ObTabletReorgInfoTableSchemaDef::COLUMN_SCHEMA_VERSION,
        rowkey_position,
        ObOrderType::ASC,
        meta,
        column_length,
        column_schema))) {
      LOG_WARN("failed to build column schema", K(ret), K(i));
    } else if (OB_FAIL(table_schema.add_column(column_schema))) {
      LOG_WARN("failed to add column", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
    table_schema.reset();
  }
  return ret;
}

int ObTabletReorgInfoTableSchemaHelper::build_rowkey_read_info_(
    common::ObIAllocator &allocator,
    const ObStorageSchema &storage_schema,
    ObRowkeyReadInfo &rowkey_read_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  int64_t full_stored_col_cnt = 0;

  if (OB_FAIL(storage_schema.get_mulit_version_rowkey_column_ids(cols_desc))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(storage_schema.get_store_column_count(full_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", K(ret));
  } else if (OB_FAIL(rowkey_read_info.init(
      allocator,
      full_stored_col_cnt,
      storage_schema.get_rowkey_column_num(),
      storage_schema.is_oracle_mode(),
      cols_desc,
      false/*is_cg_sstable*/,
      true/*use_default_compat_version*/,
      false/*is_cs_replica_compat*/))) {
    LOG_WARN("fail to init rowkey read info", K(ret));
  }

  return ret;
}

int ObTabletReorgInfoTableSchemaHelper::build_column_schema_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t column_id,
    const char *column_name,
    const int64_t schema_version,
    const int64_t rowkey_position,
    const common::ObOrderType &order_in_rowkey,
    const common::ObObjMeta &meta_type,
    const int64_t data_length,
    share::schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;

  column_schema.set_tenant_id(tenant_id);
  column_schema.set_table_id(table_id);
  column_schema.set_column_id(column_id);
  column_schema.set_schema_version(schema_version);
  column_schema.set_rowkey_position(rowkey_position);
  column_schema.set_order_in_rowkey(order_in_rowkey);
  column_schema.set_meta_type(meta_type);
  column_schema.set_data_length(data_length);
  column_schema.set_nullable(false/*is_nullable*/);

  if (OB_FAIL(column_schema.set_column_name(column_name))) {
    LOG_WARN("fail to set column name", K(ret), K(column_name));
  }
  return ret;
}

int ObTabletReorgInfoTableSchemaHelper::build_column_ids_(
    const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build column ids get invalid argument", K(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids_))) {
    LOG_WARN("failed to get column ids", K(ret), K(table_schema));
  }
  return ret;
}

int ObTabletReorgInfoTableSchemaHelper::get_column_ids(
    const bool is_query,
    common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (!is_query) {
    if (OB_FAIL(column_ids.assign(column_ids_))) {
      LOG_WARN("failed to get column ids", K(ret));
    }
  } else {
    //here get column id by storaged schema with hidden column
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM; ++i) {
      if (OB_FAIL(column_ids.push_back(column_ids_.at(i)))) {
        LOG_WARN("failed to push column id into array", K(ret), K(i), K(column_ids_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
      LOG_WARN("failed to add hidden trans version column id", K(ret), K(column_ids_));
    } else if (OB_FAIL(column_ids.push_back(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID))) {
      LOG_WARN("failed to add hidden sql sequence column id", K(ret), K(column_ids_));
    }

    for (; OB_SUCC(ret) && i < ObTabletReorgInfoTableSchemaDef::REORG_INFO_ROW_COLUMN_CNT; ++i) {
      if (OB_FAIL(column_ids.push_back(column_ids_.at(i)))) {
        LOG_WARN("failed to push column id into array", K(ret), K(i), K(column_ids_));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
