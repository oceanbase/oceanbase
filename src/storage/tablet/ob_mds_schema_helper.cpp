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

#include "storage/tablet/ob_mds_schema_helper.h"

#include "lib/ob_errno.h"
#include "lib/worker.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{
ObMdsSchemaHelper::ObMdsSchemaHelper()
  : allocator_(),
    table_schema_(),
    storage_schema_(),
    rowkey_read_info_()
{
}

ObMdsSchemaHelper::~ObMdsSchemaHelper()
{
  reset();
}

ObMdsSchemaHelper &ObMdsSchemaHelper::get_instance()
{
  static ObMdsSchemaHelper helper;
  return helper;
}

int ObMdsSchemaHelper::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    const uint64_t tenant_id = 1; // mock
    if (OB_FAIL(build_table_schema(tenant_id, DATABASE_ID, MDS_TABLE_ID, MDS_TABLE_NAME, table_schema_))) {
      LOG_WARN("fail to build table schema", K(ret));
    } else if (OB_UNLIKELY(!table_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret), K_(table_schema));
    } else if (OB_FAIL(storage_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::ORACLE))) {
      LOG_WARN("fail to init storage schema", K(ret));
    } else if (OB_UNLIKELY(!storage_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage schema", K(ret), K_(storage_schema));
    } else if (OB_FAIL(build_rowkey_read_info(allocator_, storage_schema_, rowkey_read_info_))) {
      LOG_WARN("fail to build rowkey read info", K(ret));
    } else if (OB_UNLIKELY(!rowkey_read_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey read info", K(ret), K_(rowkey_read_info));
    } else {
      is_inited_ = true;
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

void ObMdsSchemaHelper::reset()
{
  rowkey_read_info_.reset();
  storage_schema_.reset();
  table_schema_.reset();
  allocator_.reset();
  is_inited_ = false;
}

const ObStorageSchema *ObMdsSchemaHelper::get_storage_schema() const
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

const share::schema::ObTableSchema *ObMdsSchemaHelper::get_table_schema() const
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

const ObRowkeyReadInfo *ObMdsSchemaHelper::get_rowkey_read_info() const
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

int ObMdsSchemaHelper::build_table_schema(
    const uint64_t tenant_id,
    const int64_t database_id,
    const uint64_t table_id,
    const char *table_name,
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  /*
   * mds schema:
   *
   * MDS_TYPE UDF_KEY SNAPSHOT SEQ_NO META_INFO USER_DATA
   * tiny_int binary  int      int    binary    binary
   *
   * SNAPSHOT column and SEQ_NO column are multi version columns
   */
  ObObjMeta mds_type_meta;
  mds_type_meta.set_tinyint();
  ObObjMeta udf_key_meta;
  udf_key_meta.set_binary();
  ObObjMeta meta_info_meta;
  meta_info_meta.set_binary();
  ObObjMeta user_data_meta;
  user_data_meta.set_binary();

  ObColumnSchemaV2 mds_type_column_schema;
  ObColumnSchemaV2 udf_key_column_schema;
  ObColumnSchemaV2 meta_info_column_schema;
  ObColumnSchemaV2 user_data_column_schema;

  if (OB_FAIL(build_column_schema(
      tenant_id,
      table_id,
      MDS_TYPE_COLUMN_ID,
      MDS_TYPE_COLUMN_NAME,
      COLUMN_SCHEMA_VERSION,
      1/*rowkey_position*/,
      ObOrderType::ASC,
      mds_type_meta,
      MDS_TYPE_DATA_LENGTH,
      mds_type_column_schema))) {
    LOG_WARN("fail to build column schema", K(ret));
  } else if (OB_FAIL(build_column_schema(
      tenant_id,
      table_id,
      UDF_KEY_COLUMN_ID,
      UDF_KEY_COLUMN_NAME,
      COLUMN_SCHEMA_VERSION,
      2/*rowkey_position*/,
      ObOrderType::ASC,
      udf_key_meta,
      UDF_KEY_DATA_LENGTH,
      udf_key_column_schema))) {
    LOG_WARN("fail to build column schema", K(ret));
  } else if (OB_FAIL(build_column_schema(
      tenant_id,
      table_id,
      META_INFO_COLUMN_ID,
      META_INFO_COLUMN_NAME,
      COLUMN_SCHEMA_VERSION,
      0/*rowkey_position*/,
      ObOrderType::ASC,
      meta_info_meta,
      META_INFO_DATA_LENGTH,
      meta_info_column_schema))) {
    LOG_WARN("fail to build column schema", K(ret));
  } else if (OB_FAIL(build_column_schema(
      tenant_id,
      table_id,
      USER_DATA_COLUMN_ID,
      USER_DATA_COLUMN_NAME,
      COLUMN_SCHEMA_VERSION,
      0/*rowkey_position*/,
      ObOrderType::ASC,
      user_data_meta,
      USER_DATA_DATA_LENGTH,
      user_data_column_schema))) {
    LOG_WARN("fail to build column schema", K(ret));
  }

  if (OB_SUCC(ret)) {
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_database_id(database_id);
    table_schema.set_table_id(table_id);
    table_schema.set_rowkey_column_num(ROWKEY_COLUMN_NUM);
    table_schema.set_compress_func_name("none");
    table_schema.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
    table_schema.set_table_name(MDS_TABLE_NAME);
    table_schema.set_schema_version(MDS_SCHEMA_VERSION);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_schema.add_column(mds_type_column_schema))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(table_schema.add_column(udf_key_column_schema))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(table_schema.add_column(meta_info_column_schema))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(table_schema.add_column(user_data_column_schema))) {
    LOG_WARN("fail to add column", K(ret));
  }

  if (OB_FAIL(ret)) {
    table_schema.reset();
  }

  return ret;
}

int ObMdsSchemaHelper::build_rowkey_read_info(
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
      true/*use_default_compat_version*/))) {
    LOG_WARN("fail to init rowkey read info", K(ret));
  }

  return ret;
}

int ObMdsSchemaHelper::build_column_schema(
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

  if (OB_FAIL(column_schema.set_column_name(column_name))) {
    LOG_WARN("fail to set column name", K(ret), K(column_name));
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
