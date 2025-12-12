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
#include "share/ob_tablet_autoincrement_service.h"
#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_vector.h"

namespace oceanbase
{
namespace share
{
using namespace schema;
using namespace common;

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
  const schema::ObTableSchema &table_schema,
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

int ObHeapTableUtil::handle_hidden_clustering_key_column(ObArenaAllocator &allocator,
                                                         const ObTabletID &tablet_id,
                                                         ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else {
    ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
    uint64_t seq_id = 0;
    uint64_t buf_len = sizeof(ObHiddenClusteringKey);
    char *buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
    ObString hidden_clustering_key_str(buf_len, 0, buf);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), KP(buf));
    } else if (OB_FAIL(auto_inc.get_autoinc_seq(MTL_ID(), tablet_id, seq_id))) {
      LOG_WARN("fail to get tablet autoinc seq", K(ret), K(tablet_id));
    } else {
      ObHiddenClusteringKey hidden_clustering_key(tablet_id.id(), seq_id);
      if (OB_FAIL(ObHiddenClusteringKey::set_hidden_clustering_key_to_string(hidden_clustering_key, hidden_clustering_key_str))) {
        LOG_WARN("failed to set hidden clustering key to string", KR(ret), K(hidden_clustering_key), K(hidden_clustering_key_str));
      } else {
        datum.set_string(hidden_clustering_key_str);
      }
    }
  }
  return ret;
}
int ObHeapTableUtil::fill_hidden_clustering_key_for_vector(ObArenaAllocator &allocator,
                                                           storage::ObDirectLoadVector *hidden_pk_vector,
                                                           storage::ObDirectLoadVector *tablet_id_vector,
                                                           const bool is_single_part,
                                                           const ObTabletID &single_tablet_id,
                                                           const int64_t row_start,
                                                           const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hidden_pk_vector) || OB_UNLIKELY(row_start < 0 || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(hidden_pk_vector), K(row_start), K(count));
  } else if (!is_single_part && OB_ISNULL(tablet_id_vector)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet id vector is null in multi-part scene", KR(ret));
  } else if (is_single_part && OB_UNLIKELY(!single_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid single tablet id", KR(ret), K(single_tablet_id));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const int64_t row_idx = row_start + idx;
      uint64_t tablet_id_value = 0;
      if (is_single_part) {
        tablet_id_value = single_tablet_id.id();
      } else {
        ObDatum tablet_id_datum;
        if (OB_FAIL(tablet_id_vector->get_datum(row_idx, tablet_id_datum))) {
          LOG_WARN("fail to get tablet id datum", KR(ret), K(row_idx));
        } else {
          tablet_id_value = tablet_id_datum.get_uint();
        }
      }
      if (OB_SUCC(ret)) {
        ObDatum hidden_pk_datum;
        if (OB_FAIL(hidden_pk_vector->get_datum(row_idx, hidden_pk_datum))) {
          LOG_WARN("fail to get hidden pk datum", KR(ret), K(row_idx));
        } else if (OB_FAIL(handle_hidden_clustering_key_column(
                     allocator, ObTabletID(tablet_id_value), hidden_pk_datum))) {
          LOG_WARN("fail to handle hidden clustering key column", KR(ret), K(tablet_id_value), K(row_idx));
        } else if (OB_FAIL(hidden_pk_vector->set_datum(row_idx, hidden_pk_datum))) {
          LOG_WARN("fail to set hidden pk datum", KR(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}
}
}