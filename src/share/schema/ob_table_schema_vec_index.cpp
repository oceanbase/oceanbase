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
#include "ob_table_schema.h"
#include "ob_column_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace oceanbase::common;

int ObTableSchema::get_vec_index_column_id(uint64_t &with_cascaded_info_column_id) const
{
  int ret = OB_SUCCESS;
  with_cascaded_info_column_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == with_cascaded_info_column_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_vec_hnsw_vector_column() || column_schema->is_vec_ivf_center_id_column()) {
      with_cascaded_info_column_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == with_cascaded_info_column_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_sparse_vec_index_column_id(uint64_t &sparse_vec_col_id) const
{
  int ret = OB_SUCCESS;
  sparse_vec_col_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == sparse_vec_col_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_collection()) {
      sparse_vec_col_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == sparse_vec_col_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_vec_index_vid_col_id(uint64_t &vec_id_col_id, bool is_cid) const
{
  int ret = OB_SUCCESS;
  vec_id_col_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == vec_id_col_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (!is_cid && column_schema->is_vec_hnsw_vid_column()) {
      vec_id_col_id = column_schema->get_column_id();
    } else if (is_cid && column_schema->is_vec_ivf_center_id_column()) { // table schema must be index table here
      vec_id_col_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == vec_id_col_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_hybrid_vec_chunk_column_id(const ObTableSchema &data_table_schema,
                                                   uint64_t &hybrid_vec_chunk_col_id) const
{
  int ret = OB_SUCCESS;
  hybrid_vec_chunk_col_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == hybrid_vec_chunk_col_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->get_column_name_str().prefix_match(OB_HYBRID_VEC_CHUNK_VALUE_COLUMN_NAME_PREFIX)) {
      // must to check the column in the data table schema
      const ObColumnSchemaV2 *data_col = data_table_schema.get_column_schema(column_schema->get_column_id());
      if (OB_NOT_NULL(data_col) && data_col->is_hybrid_vec_index_chunk_column()) {
        hybrid_vec_chunk_col_id = column_schema->get_column_id();
      }
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == hybrid_vec_chunk_col_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_hybrid_vec_embedded_column_id(uint64_t &vec_id_col_id) const
{
  int ret = OB_SUCCESS;
  vec_id_col_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == vec_id_col_id && i < get_column_count(); ++i) {
    const ObColumnSchemaV2 *column_schema = get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
    } else if (column_schema->is_hybrid_embedded_vec_column()) {
      vec_id_col_id = column_schema->get_column_id();
    }
  }
  if (OB_FAIL(ret) || OB_INVALID_ID == vec_id_col_id) {
    ret = ret != OB_SUCCESS ? ret : OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

int ObTableSchema::get_rowkey_vid_tid(uint64_t &index_table_id) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  index_table_id = OB_INVALID_ID;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (share::schema::is_vec_rowkey_vid_type(simple_index_infos.at(i).index_type_)) {
      index_table_id = simple_index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
    LOG_DEBUG("not found rowkey vid index", K(ret), K(simple_index_infos));
  }
  return ret;
}

int ObTableSchema::get_rowkey_cid_tid(uint64_t &index_table_id, const ObIndexType index_type) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  index_table_id = OB_INVALID_ID;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (index_type == simple_index_infos.at(i).index_type_) {
      index_table_id = simple_index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
    LOG_DEBUG("not found rowkey cid index", K(ret), K(simple_index_infos));
  }
  return ret;
}

int ObTableSchema::get_embedded_vec_tid(uint64_t &index_table_id) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  index_table_id = OB_INVALID_ID;
  if (OB_FAIL(get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    if (share::schema::is_hybrid_vec_index_embedded_type(simple_index_infos.at(i).index_type_)) {
      index_table_id = simple_index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(OB_INVALID_ID == index_table_id)) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
    LOG_DEBUG("not found rowkey vid index", K(ret), K(simple_index_infos));
  }
  return ret;
}

int ObTableSchema::get_vec_id_rowkey_tid(uint64_t &vec_id_rowkey_tid) const
{
  int ret = OB_SUCCESS;
  vec_id_rowkey_tid = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos_.count(); ++i) {
    if (share::schema::is_vec_vid_rowkey_type(simple_index_infos_.at(i).index_type_)) {
      vec_id_rowkey_tid = simple_index_infos_.at(i).table_id_;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == vec_id_rowkey_tid) {
    ret = OB_ERR_INDEX_KEY_NOT_FOUND;
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
