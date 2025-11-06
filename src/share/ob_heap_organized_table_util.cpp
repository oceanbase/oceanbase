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

#define USING_LOG_PREFIX SHARE

#include "share/ob_heap_organized_table_util.h"

namespace oceanbase
{
namespace share
{
using namespace schema;

int ObHeapTableUtil::generate_pk_increment_column(
  schema::ObTableSchema &table_schema,
  const uint64_t available_col_id,
  const uint64_t rowkey_position)
{
  int ret = OB_SUCCESS;

  // 参数边界检查
  if (rowkey_position > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
  } else if (OB_UNLIKELY(OB_INVALID_ID == available_col_id
      || rowkey_position <= 0
      || !table_schema.is_table_with_clustering_key()
      || table_schema.get_column_schema(available_col_id) != nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument(s)", K(ret), K(available_col_id), K(rowkey_position), K(OB_USER_MAX_ROWKEY_COLUMN_NUMBER),
             K(table_schema.is_table_with_clustering_key()), K(table_schema.get_column_schema(available_col_id)));
  } else {
    ObColumnSchemaV2 hidden_pk;
    hidden_pk.reset();
    hidden_pk.set_column_id(available_col_id);
    hidden_pk.set_data_type(ObVarcharType);
    hidden_pk.set_nullable(false);
    hidden_pk.set_is_hidden(true);
    hidden_pk.set_data_length(OB_CLUSTER_BY_TABLE_HIDDEN_PK_BYTE_LENGTH);
    hidden_pk.set_charset_type(CHARSET_BINARY);
    hidden_pk.set_collation_type(CS_TYPE_BINARY);
    hidden_pk.add_column_flag(HEAP_TABLE_CLUSTERING_KEY_FLAG);
    if (OB_FAIL(hidden_pk.set_column_name(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME))) {
      LOG_WARN("failed to set column name", K(ret));
    } else {
      hidden_pk.set_rowkey_position(rowkey_position);
      if (OB_FAIL(table_schema.add_column(hidden_pk))) {
        LOG_WARN("add column to table_schema failed", K(ret), K(hidden_pk));
      }
    }
  }
  return ret;
}

bool ObHeapTableUtil::is_table_with_clustering_key(
  const bool is_table_without_pk,
  const bool is_table_with_hidden_pk_column)
{
  return !is_table_without_pk && is_table_with_hidden_pk_column;
}

int ObHeapTableUtil::get_hidden_clustering_key_column_id(
  const ObTableSchema &table_schema,
  uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  column_id = OB_INVALID_ID;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema));
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
         OB_SUCC(ret) && iter != table_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(table_schema));
      } else if (column_schema->is_hidden_clustering_key_column()) {
        column_id = column_schema->get_column_id();
        break;
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID == column_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hidden clustering key column not found", K(ret), K(table_schema));
    }
  }
  return ret;
}
}
}