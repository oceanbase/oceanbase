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

#include "ob_vector_index_util.h"
#include "storage/vector_index/ob_vector_index_sched_job_utils.h"

namespace oceanbase
{
namespace share
{
/*
  预期 index_param_str 是大写的字串
*/
int ObVectorIndexUtil::parser_params_from_string(
    const ObString &index_param_str, ObVectorIndexHNSWParam &param)
{
  int ret = OB_SUCCESS;
  ObString tmp_param_str = index_param_str;
  ObArray<ObString> tmp_param_strs;
  param.reset();
  if (tmp_param_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index param, is empty", K(ret));
  } else if (OB_FAIL(split_on(tmp_param_str, ',', tmp_param_strs))) {
    LOG_WARN("fail to split func expr", K(ret), K(tmp_param_str));
  } else if (tmp_param_strs.count() < 2) {  // at lease two params(distance, type) should be set
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index param count", K(tmp_param_strs.count()));
  } else {
    const int64_t default_m_value = 16;
    const int64_t default_ef_construction_value = 200;
    const int64_t default_ef_search_value = 64;
    const ObVectorIndexAlgorithmLib default_lib = ObVectorIndexAlgorithmLib::VIAL_VSAG;

    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_param_strs.count(); ++i) {
      ObString one_tmp_param_str = tmp_param_strs.at(i).trim();
      ObArray<ObString> one_tmp_param_strs;
      if (OB_FAIL(split_on(one_tmp_param_str, '=', one_tmp_param_strs))) {
        LOG_WARN("fail to split one param str", K(ret), K(one_tmp_param_str));
      } else if (one_tmp_param_strs.count() != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector index one param pair count", K(one_tmp_param_strs.count()));
      } else {
        ObString new_param_name = one_tmp_param_strs.at(0).trim();
        ObString new_param_value = one_tmp_param_strs.at(1).trim();

        if (new_param_name == "DISTANCE") {
          if (new_param_value == "INNER_PRODUCT") {
            param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_IP;
          } else if (new_param_value == "L2") {
            param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_L2;
          } else if (new_param_value == "COSINE") {
            param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_COS;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index dist algorithm", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "LIB") {
          if (new_param_value == "VSAG") {
            param.lib_ = ObVectorIndexAlgorithmLib::VIAL_VSAG;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index lib", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "TYPE") {
          if (new_param_value == "HNSW") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW;
          } else if (new_param_value == "HNSW_SQ") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW_SQ;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index type", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "M") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 5 && int_value <= 64) {
            param.m_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index m value", K(ret), K(int_value), K(new_param_value));
          }
        } else if (new_param_name == "EF_CONSTRUCTION") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 5 && int_value <= 1000) {
            param.ef_construction_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index ef_construction value", K(ret), K(int_value), K(new_param_value));
          }
        } else if (new_param_name == "EF_SEARCH") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 1 && int_value <= 1000) {
            param.ef_search_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index ef_search value", K(ret), K(int_value), K(new_param_value));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector index param name", K(ret), K(new_param_name));
        }
      }
    }
    if (OB_SUCC(ret)) {  // if vector parram not set, set default
      if (param.m_ == 0) { param.m_ = default_m_value; }
      if (param.ef_construction_ == 0) { param.ef_construction_ = default_ef_construction_value; }
      if (param.ef_search_ == 0) { param.ef_search_ = default_ef_search_value; }
      if (param.lib_ == ObVectorIndexAlgorithmLib::VIAL_MAX) { param.lib_ = default_lib; }
      param.dim_ = 0; // TODO@xiajin: fill dim
    }
    LOG_DEBUG("parser vector index param", K(ret), K(index_param_str), K(param));
  }
  return ret;
}

bool ObVectorIndexUtil::is_expr_type_and_distance_algorithm_match(
     const ObItemType expr_type, const ObVectorIndexDistAlgorithm algorithm)
{
  bool is_match = false;
  switch (expr_type) {
    case T_FUN_SYS_L2_DISTANCE: {
      if (ObVectorIndexDistAlgorithm::VIDA_L2 == algorithm) {
        is_match = true;
      }
      break;
    }
    case T_FUN_SYS_COSINE_DISTANCE: {
      if (ObVectorIndexDistAlgorithm::VIDA_COS == algorithm) {
        is_match = true;
      }
      break;
    }
    case T_FUN_SYS_INNER_PRODUCT:
    case T_FUN_SYS_NEGATIVE_INNER_PRODUCT: {
      if (ObVectorIndexDistAlgorithm::VIDA_IP == algorithm) {
        is_match = true;
      }
      break;
    }
    default: break;
  }
  return is_match;
}

int ObVectorIndexUtil::check_distance_algorithm_match(
    ObSchemaGetterGuard &schema_guard,
    const schema::ObTableSchema &table_schema,
    const ObString &index_column_name,
    const ObItemType type,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  const int64_t data_table_id = table_schema.get_table_id();
  const int64_t database_id = table_schema.get_database_id();
  const int64_t tenant_id = table_schema.get_tenant_id();
  const int64_t vector_index_column_cnt = 1;
  is_match = false;

  if (index_column_name.empty() ||
      OB_INVALID_ID == data_table_id || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(index_column_name), K(data_table_id), K(tenant_id), K(database_id));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    ObSEArray<ObString, 1> col_names;
    ObVectorIndexHNSWParam index_param;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("fail to get simple index infos failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_schema = nullptr;
        const int64_t table_id = simple_index_infos.at(i).table_id_;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
          LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (!index_schema->is_vec_index()) {
          // skip none vector index
        } else if (index_schema->is_built_in_vec_index()) {
          // skip built in vector index table
        } else if (OB_FAIL(get_vector_index_column_name(table_schema, *index_schema, col_names))) {
          LOG_WARN("fail to get vector index column name", K(ret), K(index_schema));
        } else if (col_names.count() != vector_index_column_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector index column cnt, should equal to one", K(ret), K(col_names.count()));
        } else if (ObColumnNameHashWrapper(col_names.at(0)) == ObColumnNameHashWrapper(index_column_name)) {
          if (OB_FAIL(parser_params_from_string(index_schema->get_index_params(), index_param))) {
            LOG_WARN("fail to parser params from string", K(ret), K(index_schema->get_index_params()));
          } else {
            is_match = is_expr_type_and_distance_algorithm_match(type, index_param.dist_algorithm_);
            LOG_INFO("has finish finding index according to column_name and check expr match",
              K(is_match), K(type), K(index_param.dist_algorithm_));
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_index_name_prefix(
  const schema::ObTableSchema &index_schema,
  ObString &prefix)
{
  int ret = OB_SUCCESS;
  if (!index_schema.is_vec_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, not vector index table", K(ret), K(index_schema));
  } else if (index_schema.is_vec_rowkey_vid_type() || index_schema.is_vec_vid_rowkey_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index type, only support get none share table prefix",
      K(ret), K(index_schema));
  } else {
    ObString tmp_table_name = index_schema.get_table_name();
    const int64_t table_name_len = tmp_table_name.length();
    const char* delta_buffer_table = "";
    const char* index_id_table = "_index_id_table";
    const char* index_snapshot_data_table = "_index_snapshot_data_table";
    int64_t assign_len = 0;

    if (index_schema.is_vec_delta_buffer_type()) {
      assign_len = table_name_len - strlen(delta_buffer_table);
    } else if (index_schema.is_vec_index_id_type()) {
      assign_len = table_name_len - strlen(index_id_table);
    } else if (index_schema.is_vec_index_snapshot_data_type()) {
      assign_len = table_name_len - strlen(index_snapshot_data_table);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector index type", K(ret), K(index_schema));
    }
    if (OB_SUCC(ret)) {
      prefix.assign_ptr(tmp_table_name.ptr(), assign_len);
    }
  }
  return ret;
}

int ObVectorIndexUtil::check_table_has_vector_of_fts_index(
    const ObTableSchema &data_table_schema, ObSchemaGetterGuard &schema_guard, bool &has_fts_index, bool &has_vec_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  has_fts_index = false;
  has_vec_index = false;

  if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (index_table_schema->is_vec_index()) {
        has_vec_index = true;
      } else if (index_table_schema->is_fts_index_aux() || index_table_schema->is_fts_doc_word_aux()) {
        has_fts_index = true;
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::check_column_has_vector_index(
    const ObTableSchema &data_table_schema, ObSchemaGetterGuard &schema_guard, const int64_t col_id, bool &is_column_has_vector_index)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16>simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  is_column_has_vector_index = false;

  if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_table_schema->is_vec_index()) {
        // skip none vector index
      } else if (index_table_schema->is_built_in_vec_index()) {
        // skip built in vector index table
      } else {
        // handle delta_buffer_table index table
        const ObRowkeyInfo &rowkey_info = index_table_schema->get_rowkey_info();
        for (int64_t j = 0; OB_SUCC(ret) && !is_column_has_vector_index && j < rowkey_info.get_size(); j++) {
          const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(j);
          const int64_t column_id = rowkey_column->column_id_;
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema(column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(column_id), KPC(index_table_schema));
          } else if (col_schema->is_vec_vid_column()) {
            // only need vec_type, here skip vec_vid column of delta_buffer_table rowkey column
          } else {
            // get generated column cascaded column id info
            // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
            ObArray<uint64_t> cascaded_column_ids;
            // get column_schema from data table using generate column id
            const ObColumnSchemaV2 *table_column = data_table_schema.get_column_schema(col_schema->get_column_id());
            if (OB_ISNULL(table_column)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected table column", K(ret));
            } else if (OB_FAIL(table_column->get_cascaded_column_ids(cascaded_column_ids))) {
              LOG_WARN("failed to get cascaded column ids", K(ret));
            } else {
              for (int64_t k = 0; OB_SUCC(ret) && !is_column_has_vector_index && k < cascaded_column_ids.count(); ++k) {
                const ObColumnSchemaV2 *cascaded_column = NULL;
                ObString new_col_name;
                if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(k)))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected cascaded column", K(ret));
                } else if (cascaded_column->get_column_id() == col_id) {
                  is_column_has_vector_index = true;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

bool ObVectorIndexUtil::has_multi_index_on_same_column(ObIArray<uint64_t> &vec_index_cols, const uint64_t col_id)
{
  bool has_same_column_index = false;
  for (int64_t i = 0; !has_same_column_index && i < vec_index_cols.count(); ++i) {
    if (vec_index_cols.at(i) == col_id) {
      has_same_column_index = true;
    }
  }
  return has_same_column_index;
}

/* need deep copy */
int ObVectorIndexUtil::insert_index_param_str(
  const ObString &new_add_param, ObIAllocator &allocator, ObString &current_index_param)
{
  int ret = OB_SUCCESS;
  ObString tmp_str = new_add_param;
  ObString tmp_new_str;

  if (new_add_param.empty() || !current_index_param.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index string",
      K(ret), K(new_add_param), K(current_index_param));
  } else if (OB_FAIL(ob_simple_low_to_up(allocator, tmp_str.trim(), tmp_new_str))) {
    LOG_WARN("string low to up failed", K(ret), K(tmp_str));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_new_str, current_index_param))){
    LOG_WARN("fail to write vector index param", K(ret), K(tmp_new_str));
  }

  return ret;
}

int ObVectorIndexUtil::get_vector_index_column_id(
    const ObTableSchema &data_table_schema, const ObTableSchema &index_table_schema, ObIArray<uint64_t> &col_ids)
{
  INIT_SUCC(ret);
  col_ids.reset();
  if (!index_table_schema.is_vec_index()) {
    // skip none vector index
  } else if (index_table_schema.is_vec_rowkey_vid_type() || index_table_schema.is_vec_vid_rowkey_type()) {
    // skip rowkey_vid and vid_rowkey table
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_schema.get_column_count(); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(index_table_schema));
      } else if (!col_schema->is_vec_vector_column()) {
        // only need vec_vector column, here skip other column
      } else {
        // get generated column cascaded column id info
        // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
        ObArray<uint64_t> cascaded_column_ids;
        // get column_schema from data table using generate column id
        const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(ori_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ori column", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
        } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
          LOG_WARN("failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < cascaded_column_ids.count(); ++j) {
            const ObColumnSchemaV2 *cascaded_column = NULL;
            uint64_t new_col_id;
            if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(j)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected cascaded column", K(ret));
            } else if (OB_FALSE_IT(new_col_id = cascaded_column->get_column_id())) {
            } else if (OB_FAIL(col_ids.push_back(new_col_id))) {
              LOG_WARN("fail to push back col names", K(ret), K(new_col_id));
            } else {
              LOG_DEBUG("success to get vector index col name", K(ret), K(new_col_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

/*
  目前只支持单列向量索引。
 */
int ObVectorIndexUtil::get_vector_dim_from_extend_type_info(const ObIArray<ObString> &extend_type_info, int64_t &dim)
{
  int ret = OB_SUCCESS;
  dim = 0;
  if (extend_type_info.count() != 1) {
    // Vector index columns currently only support single column vector indexes.
    // When building the vector column of the auxiliary table, only one column of extend_type_info is assigned.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unexpected extend type info, current only support one column vector index",
      K(ret), K(extend_type_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "vector column index only support build on one vector column");
  } else {
    ObString extend_type_info_str = extend_type_info.at(0);
    ObString spilt_str = extend_type_info_str.split_on('(').trim();
    if (0 == spilt_str.compare("ARRAY")) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unexpected column type", K(ret), K(spilt_str));
    } else if (0 != spilt_str.compare("VECTOR")) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column extend info type", K(ret), K(spilt_str));
    } else if (OB_FALSE_IT(spilt_str = extend_type_info_str.split_on(')').trim())) {
    } else {
      dim = std::atoi(spilt_str.ptr());
    }
  }
  return ret;
}

/*
  To obtain the dimension of the vector index。
  it is currently only supported to retrieve it from table 345, as only table 345 contains vector column information.
*/
int ObVectorIndexUtil::get_vector_index_column_dim(const ObTableSchema &index_table_schema, int64_t &dim)
{
  int ret = OB_SUCCESS;
  ObSArray<uint64_t> all_column_ids;
  dim = 0;
  if (!index_table_schema.is_vec_index()) {
    // skip none vector index
  } else if (!index_table_schema.is_vec_delta_buffer_type() &&
             !index_table_schema.is_vec_index_id_type() &&
             !index_table_schema.is_vec_index_snapshot_data_type()) {
    // skip has no vector column index table
  } else if (OB_FAIL(index_table_schema.get_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(index_table_schema));
  } else {
    // handle delta_buffer_table index table
    for (int64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
      const int64_t column_id = all_column_ids.at(i);
      const ObColumnSchemaV2 *col_schema = nullptr;
      ObArray<ObString> extend_type_info;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(column_id), K(index_table_schema));
      } else if (!col_schema->is_vec_vector_column()) {
        // only need vec_type, here skip vec_vid column of delta_buffer_table rowkey column
      } else if (OB_FAIL(get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(),
                                                              dim))) {
        LOG_WARN("fail to get vector dim", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_vector_index_tid(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const ObIndexType index_type,
    const int64_t col_id,
    uint64_t &tid)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16>simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  tid = OB_INVALID_ID;

  if (!share::schema::is_vec_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index type for vector index", K(index_type));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && tid == OB_INVALID_ID; ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_table_schema->is_vec_index()) {
        // skip none vector index
      } else if (index_table_schema->get_index_type() != index_type) {
        // skip not spec index type
      } else if (index_table_schema->is_vec_rowkey_vid_type() || index_table_schema->is_vec_vid_rowkey_type()) {
        // rowkey_vid and vid_rowkey is shared, only one, just return
        tid = simple_index_infos.at(i).table_id_;
      } else { // delta buffer, index id, index snapshot, we should check cascaded_column by vec_vector col
        for (int64_t j = 0; OB_SUCC(ret) && tid == OB_INVALID_ID && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_vector_column()) {
            // only need vec_vector column, here skip other column
          } else {
            // get generated column cascaded column id info
            // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
            ObArray<uint64_t> cascaded_column_ids;
            // get column_schema from data table using generate column id
            const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
            if (OB_ISNULL(ori_col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected table column", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
            } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
              LOG_WARN("failed to get cascaded column ids", K(ret));
            } else {
              for (int64_t k = 0; OB_SUCC(ret) && tid == OB_INVALID_ID && k < cascaded_column_ids.count(); ++k) {
                const ObColumnSchemaV2 *cascaded_column = NULL;
                ObString new_col_name;
                if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(k)))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected cascaded column", K(ret));
                } else if (cascaded_column->get_column_id() == col_id) {
                  tid = simple_index_infos.at(i).table_id_;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObVecIdxSnapshotDataWriteCtx::reset()
{
  ls_id_.reset();
  data_tablet_id_.reset();
  lob_meta_tablet_id_.reset();
  lob_piece_tablet_id_.reset();
  vals_.reset();
}

int ObVectorIndexUtil::generate_new_index_name(ObIAllocator &allocator, ObString &new_index_name)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(allocator.alloc(OB_MAX_TABLE_NAME_LENGTH));
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc new memory", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "idx_%lu", ObTimeUtility::current_time()))){
    LOG_WARN("fail to printf current time", K(ret));
  } else {
    new_index_name.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObVectorIndexUtil::generate_switch_index_names(
    const ObString &old_domain_index_name,
    const ObString &new_domain_index_name,
    ObIAllocator &allocator,
    ObIArray<ObString> &old_table_names,
    ObIArray<ObString> &new_table_names)
{
  int ret = OB_SUCCESS;
  ObString old_delta_buffer_table_name = old_domain_index_name;
  ObString new_delta_buffer_table_name = new_domain_index_name;
  ObString new_index_id_table_name;
  ObString new_snapshot_data_table_name;
  ObString old_index_id_table_name;
  ObString old_snapshot_data_table_name;

  if (OB_FAIL(new_table_names.push_back(new_delta_buffer_table_name))) {
    LOG_WARN("fail to push back new delta buffer table name", K(ret));
  } else if (OB_FAIL(old_table_names.push_back(old_delta_buffer_table_name))) {
    LOG_WARN("fail to push back old delta buffer table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_index_id_table_name))) {
    LOG_WARN("fail to generate delta buffer table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_index_id_table_name))) {
    LOG_WARN("fail to push back new index id table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_index_id_table_name))) {
    LOG_WARN("fail to generate index id table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_index_id_table_name))) {
    LOG_WARN("fail to push back new snapshot data table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_snapshot_data_table_name))) {
    LOG_WARN("fail to construct old snapshot data table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_snapshot_data_table_name))) {
    LOG_WARN("fail to push back old snapshot data table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_snapshot_data_table_name))) {
    LOG_WARN("fail to construct old snapshot data table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_snapshot_data_table_name))) {
    LOG_WARN("fail to push back old snapshot data table name", K(ret));
  }
  return ret;
}

int ObVectorIndexUtil::update_index_tables_status(
    const int64_t tenant_id,
    const int64_t database_id,
    const ObIArray<ObString> &old_table_names,
    const ObIArray<ObString> &new_table_names,
    rootserver::ObDDLOperator &ddl_operator,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLTransaction &trans,
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const bool is_index = true;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id ||
      old_table_names.count() <= 0 || new_table_names.count() <= 0 ||
     (old_table_names.count() != new_table_names.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(tenant_id), K(database_id), K(old_table_names), K(new_table_names));
  } else {
    // update old index status
    for (int64_t i = 0; OB_SUCC(ret) && i < old_table_names.count(); ++i) {
      const ObString *ddl_stmt_str = NULL;
      const ObTableSchema *index_schema = nullptr;
      bool in_offline_ddl_white_list = false;
      const bool is_built_in_index = i == 0 ? false : true;
      const ObString &old_index_name = old_table_names.at(i);
      const ObString &new_index_name = new_table_names.at(i);
      SMART_VAR(ObTableSchema, tmp_schema) {
      // ObTableSchema tmp_schema;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                old_index_name,
                                                is_index, /* is_index */
                                                index_schema,
                                                false,  /* is_hidden_table */
                                                is_built_in_index))) {
        LOG_WARN("fail to get table schema", K(ret), K(old_index_name));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(tenant_id), K(database_id), K(old_index_name));
      } else if (!index_schema->is_vec_index()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected, here should be vector index schema", K(ret), K(index_schema));
      } else if (index_schema->is_unavailable_index()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("switch name of unaveliable index is not support", KR(ret));
      } else if (OB_FALSE_IT(in_offline_ddl_white_list = index_schema->get_table_state_flag() != TABLE_STATE_NORMAL)) {
      } else if (OB_FAIL(ddl_operator.update_index_status(tenant_id,
                                                          index_schema->get_data_table_id(),
                                                          index_schema->get_table_id(),
                                                          INDEX_STATUS_UNAVAILABLE,
                                                          in_offline_ddl_white_list,
                                                          trans,
                                                          ddl_stmt_str))) {
        LOG_WARN("update_index_status failed", K(index_schema->get_data_table_id()));
      } else if (OB_FAIL(tmp_schema.assign(*index_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else if (OB_FALSE_IT(tmp_schema.set_index_status(INDEX_STATUS_UNAVAILABLE))) {
      } else if (OB_FAIL(tmp_schema.set_table_name(new_index_name))) {
        LOG_WARN("fail to set table name", K(ret), K(new_index_name));
      } else if (OB_FAIL(table_schemas.push_back(tmp_schema))) {
        LOG_WARN("fail to push back schema", K(ret));
      }
      } // end smart_var
    }
  }
  return ret;
}

int ObVectorIndexUtil::update_index_tables_attributes(
    const int64_t tenant_id,
    const int64_t database_id,
    const int64_t data_table_id,
    const int64_t expected_update_table_cnt,
    const ObIArray<ObString> &old_table_names,
    const ObIArray<ObString> &new_table_names,
    rootserver::ObDDLOperator &ddl_operator,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLTransaction &trans,
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const bool is_index = true;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id || OB_INVALID_ID == data_table_id ||
      old_table_names.count() <= 0 || new_table_names.count() <= 0 ||
     (table_schemas.count() != old_table_names.count()) ||
     (old_table_names.count() != new_table_names.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(tenant_id), K(database_id), K(data_table_id),
      K(table_schemas.count()), K(old_table_names.count()), K(new_table_names.count()));
  } else {
    // switch new/old index name
    for (int64_t i = 0; OB_SUCC(ret) && i < new_table_names.count(); i++) {
      const ObString *ddl_stmt_str = NULL;
      const ObTableSchema *index_schema = nullptr;
      const bool is_built_in_index = i == 0 ? false : true;
      const ObString &new_index_name = new_table_names.at(i);
      const ObString &old_index_name = old_table_names.at(i);
      SMART_VAR(ObTableSchema, tmp_schema) {
      // ObTableSchema tmp_schema;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                database_id,
                                                new_index_name,
                                                is_index,
                                                index_schema,
                                                false, /* is_hidden */
                                                is_built_in_index))) {
        LOG_WARN("fail to get table schema", K(ret), K(new_index_name));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(tenant_id), K(database_id), K(new_index_name));
      } else if (!index_schema->is_vec_index()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected, here should be vector index schema", K(ret), KPC(index_schema));
      } else if (index_schema->is_unavailable_index()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("switch name of unaveliable index is not support", KR(ret), KPC(index_schema));
      } else if (OB_FAIL(tmp_schema.assign(*index_schema))) {
        LOG_WARN("fail to assign index schema", K(ret));
      } else if (OB_FAIL(tmp_schema.set_table_name(old_index_name))) {
        LOG_WARN("fail to set new table name", K(ret), K(old_index_name));
      } else if (OB_FAIL(table_schemas.push_back(tmp_schema))) {
        LOG_WARN("fail to push back schema", K(ret));
      }
      } // end smart_var
    }
    if (OB_SUCC(ret)) { // get data table schema to update schema version
      SMART_VAR(ObTableSchema, tmp_schema) {
        const ObTableSchema *data_table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
          LOG_WARN("fail to get data table schema", K(ret), K(data_table_id));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret), KP(data_table_schema));
        } else if (OB_FAIL(tmp_schema.assign(*data_table_schema))) {
          LOG_WARN("fail to assign table schema", K(ret));
        } else if (OB_FAIL(table_schemas.push_back(tmp_schema))) {
          LOG_WARN("fail to push back table schema", K(ret));
        }
      }
    }
    // update table attribute
    if (OB_FAIL(ret)) {
    } else if (table_schemas.count() != expected_update_table_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected update table schema count", K(table_schemas.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        ObSchemaOperationType operation_type = OB_DDL_ALTER_TABLE;
        const ObString *ddl_stmt_str = NULL;
        if (OB_FAIL(ddl_operator.update_table_attribute(table_schemas.at(i),
                                                        trans,
                                                        operation_type,
                                                        ddl_stmt_str))) {
          LOG_WARN("failed to update index table schema attribute", K(ret), K(table_schemas.at(i)));
        }
      }
    }
  }
  return ret;
}


int ObVectorIndexUtil::generate_index_schema_from_exist_table(
    const int64_t tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLService &ddl_service,
    const obrpc::ObCreateIndexArg &create_index_arg,
    const ObTableSchema &data_table_schema,
    ObTableSchema &new_index_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *old_index_schema = nullptr;
  const ObTableSchema *old_domain_index_schema = nullptr;
  const int64_t old_domain_table_id = create_index_arg.index_table_id_;
  const ObString database_name = create_index_arg.database_name_;
  const ObString new_index_name_suffix = create_index_arg.index_name_;  // e.g: idx_xxx_delta_buffer_table, idx_xxx_index_id_table...
  ObString old_domain_index_name; // The name of the old table number 3.
  ObString old_index_table_name;  // The name of the old index table, is composed of the number 3 table and a suffix, and it used to obtain the schema of the old index table.
  ObString new_index_table_name;  // The name of the new index table
  uint64_t new_index_table_id = OB_INVALID_ID;
  ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
  ObSchemaService *schema_service = nullptr;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID || old_domain_table_id == OB_INVALID_ID ||
      new_index_name_suffix.empty() || database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
      K(tenant_id), K(old_domain_table_id), K(new_index_name_suffix), K(database_name), KP(schema_service));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, old_domain_table_id, old_domain_index_schema))) {
    LOG_WARN("fail to get old domain index schema", K(ret), K(tenant_id), K(old_domain_table_id));
  } else if (OB_ISNULL(old_domain_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FALSE_IT(old_domain_index_name = old_domain_index_schema->get_table_name())) {
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    create_index_arg.index_type_,
                                                                    old_domain_index_name,
                                                                    old_index_table_name))) {
    LOG_WARN("failed to generate index name", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   database_name,
                                                   old_index_table_name,
                                                   true, /* is_index */
                                                   old_index_schema,
                                                   false, /* with_hidden_flag */
                                                   share::schema::is_built_in_vec_index(create_index_arg.index_type_)))) {
    LOG_WARN("fail to get origin index schema", K(ret), K(tenant_id), K(old_domain_index_name), K(old_index_table_name));
  } else if (OB_ISNULL(old_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(old_index_table_name));
  } else if (OB_FAIL(new_index_schema.assign(*old_index_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                   data_table_schema.get_table_id(),
                                                   new_index_name_suffix,
                                                   new_index_table_name))) {
    LOG_WARN("fail to build index table name", K(ret), K(create_index_arg.index_name_));
  } else {
    if (FALSE_IT(new_index_schema.set_tenant_id(tenant_id))) {
    } else if (OB_FAIL(new_index_schema.set_table_name(new_index_table_name))) {
      LOG_WARN("set table name failed", K(ret), K(new_index_table_name));
    } else if (OB_FAIL(schema_service->fetch_new_table_id(tenant_id, new_index_table_id))) {
      LOG_WARN("failed to fetch_new_table_id", K(ret));
    } else if (OB_FAIL(ddl_service.generate_object_id_for_partition_schema(new_index_schema))) {
      LOG_WARN("fail to generate object_id for partition schema", KR(ret), K(new_index_schema));
    } else if (OB_FAIL(ddl_service.generate_tablet_id(new_index_schema))) {
      LOG_WARN("fail to generate tablet id for hidden table", K(ret), K(new_index_schema));
    } else {
      new_index_schema.set_max_used_column_id(max(
      new_index_schema.get_max_used_column_id(), data_table_schema.get_max_used_column_id()));
      new_index_schema.set_table_id(new_index_table_id);
      new_index_schema.set_index_status(INDEX_STATUS_UNAVAILABLE);
      new_index_schema.set_table_state_flag(data_table_schema.get_table_state_flag());
      new_index_schema.set_exec_env(create_index_arg.vidx_refresh_info_.exec_env_);
    }
  }
  LOG_DEBUG("generate_index_schema_from_exist_table", K(ret), K(new_index_table_name));
  return ret;
}

int ObVectorIndexUtil::add_dbms_vector_jobs(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                                            const uint64_t vidx_table_id,
                                            const common::ObString &exec_env)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObVectorIndexSchedJobUtils::add_vector_index_refresh_job(
                      sql_client, tenant_id,
                      vidx_table_id,
                      exec_env))) {
    LOG_WARN("fail to add vector index refresh job", KR(ret), K(tenant_id), K(vidx_table_id), K(exec_env));
  } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_vector_index_rebuild_job(
                      sql_client, tenant_id,
                      vidx_table_id,
                      exec_env))) {
    LOG_WARN("fail to add vector index rebuild job", KR(ret), K(tenant_id), K(vidx_table_id), K(exec_env));
  }
  return ret;
}

int ObVectorIndexUtil::remove_dbms_vector_jobs(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                                               const uint64_t vidx_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObVectorIndexSchedJobUtils::remove_vector_index_refresh_job(
                     sql_client, tenant_id, vidx_table_id))) {
    LOG_WARN("failed to remove vector index refresh job",
            KR(ret), K(tenant_id), K(vidx_table_id));
  } else if (OB_FAIL(ObVectorIndexSchedJobUtils::remove_vector_index_rebuild_job(
                     sql_client, tenant_id, vidx_table_id))) {
    LOG_WARN("failed to remove vector index rebuild job",
            KR(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexUtil::get_dbms_vector_job_info(common::ObISQLClient &sql_client,
                                                    const uint64_t tenant_id,
                                                    const uint64_t vidx_table_id,
                                                    common::ObIAllocator &allocator,
                                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                                    dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObVectorIndexSchedJobUtils::get_vector_index_job_info(sql_client, tenant_id,
                                                                    vidx_table_id,
                                                                    allocator,
                                                                    schema_guard,
                                                                    job_info))) {
    LOG_WARN("fail to get vector index job info", K(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexUtil::check_table_exist(
    const ObTableSchema &data_table_schema,
    const ObString &domain_index_name)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  bool is_exist = false;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  const int64_t database_id = data_table_schema.get_database_id();
  const int64_t data_table_id = data_table_schema.get_table_id();
  ObString index_table_name;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);

  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id || OB_INVALID_ID == data_table_id ||
      domain_index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(data_table_id), K(domain_index_name));
  } else if (OB_FAIL(ObTableSchema::build_index_table_name(
               allocator, data_table_id, domain_index_name, index_table_name))) {
    LOG_WARN("build_index_table_name failed", K(ret), K(data_table_id), K(domain_index_name));
  } else if (OB_FAIL(schema_service.check_table_exist(tenant_id,
                                                      database_id,
                                                      index_table_name,
                                                      true, /* is_index_table */
                                                      OB_INVALID_VERSION, /* latest version */
                                                      is_exist))) {
    LOG_WARN("failed to check is table exist", K(ret));
  } else if (is_exist) {
    ret = OB_ERR_TABLE_EXIST;
    LOG_WARN("table is exist, cannot create it twice", K(ret),
      K(tenant_id),  K(database_id), K(domain_index_name));
  }
  return ret;
}

int ObVectorIndexUtil::check_vec_aux_index_deleted(
    ObSchemaGetterGuard &schema_guard,
    const schema::ObTableSchema &table_schema,
    bool &is_all_deleted)
{
  int ret = OB_SUCCESS;
  const int64_t data_table_id = table_schema.get_table_id();
  const int64_t database_id = table_schema.get_database_id();
  const int64_t tenant_id = table_schema.get_tenant_id();
  bool delta_buffer_table_is_valid = false;
  bool index_id_table_is_valid = false;
  bool snapshot_table_is_valid = false;

  is_all_deleted = false;

  if (OB_INVALID_ID == data_table_id || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_id), K(tenant_id), K(database_id));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("fail to get simple index infos failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_schema = nullptr;
        const int64_t table_id = simple_index_infos.at(i).table_id_;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
          LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (!index_schema->is_vec_index()) {
          // skip none vector index
        } else if (index_schema->is_vec_rowkey_vid_type() || index_schema->is_vec_vid_rowkey_type()) {
          // skip
        } else if (index_schema->is_vec_delta_buffer_type()) {
          if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            delta_buffer_table_is_valid = true;
          }
        } else if (index_schema->is_vec_index_id_type()) {
          if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            index_id_table_is_valid = true;
          }
        } else if (index_schema->is_vec_index_snapshot_data_type()) {
          if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            snapshot_table_is_valid = true;
          }
        }
      }
      if (!delta_buffer_table_is_valid && !index_id_table_is_valid && !snapshot_table_is_valid) {
        is_all_deleted = true;
      } else {
        LOG_WARN("vector index is not all valid",
          K(delta_buffer_table_is_valid), K(index_id_table_is_valid), K(snapshot_table_is_valid));
      }
    }
  }
  LOG_INFO("check_vec_aux_index_deleted", K(ret), K(is_all_deleted));
  return ret;
}

int ObVectorIndexUtil::check_vector_index_by_column_name(
    ObSchemaGetterGuard &schema_guard,
    const schema::ObTableSchema &table_schema,
    const ObString &index_column_name,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  const int64_t data_table_id = table_schema.get_table_id();
  const int64_t database_id = table_schema.get_database_id();
  const int64_t tenant_id = table_schema.get_tenant_id();
  bool vid_rowkey_table_is_valid = false;
  bool rowkey_vid_table_is_valid = false;
  bool delta_buffer_table_is_valid = false;
  bool index_id_table_is_valid = false;
  bool snapshot_table_is_valid = false;

  is_valid = false;

  if (index_column_name.empty() ||
      OB_INVALID_ID == data_table_id || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(index_column_name), K(data_table_id), K(tenant_id), K(database_id));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("fail to get simple index infos failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_schema = nullptr;
        const int64_t table_id = simple_index_infos.at(i).table_id_;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
          LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (!index_schema->is_vec_index()) {
          // skip none vector index
        } else if (index_schema->is_vec_rowkey_vid_type()) {
          if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            rowkey_vid_table_is_valid = true;
          }
        } else if (index_schema->is_vec_vid_rowkey_type()) {
          if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            vid_rowkey_table_is_valid = true;
          }
        } else if (index_schema->is_vec_delta_buffer_type()) {
          if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
            // skip
          } else if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            delta_buffer_table_is_valid = true;
          }
        } else if (index_schema->is_vec_index_id_type()) {
          if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
            // skip
          } else if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            index_id_table_is_valid = true;
          }
        } else if (index_schema->is_vec_index_snapshot_data_type()) {
          if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
            // skip
          } else if (index_schema->can_read_index() && index_schema->is_index_visible()) {
            snapshot_table_is_valid = true;
          }
        }
      }
      if (rowkey_vid_table_is_valid && vid_rowkey_table_is_valid &&
          delta_buffer_table_is_valid && index_id_table_is_valid && snapshot_table_is_valid) {
        is_valid = true;
      } else {
        LOG_WARN("vector index is not all valid",
          K(rowkey_vid_table_is_valid), K(vid_rowkey_table_is_valid),
          K(delta_buffer_table_is_valid), K(index_id_table_is_valid), K(snapshot_table_is_valid));
      }
    }
  }
  LOG_INFO("check_vector_index_by_column_name", K(is_valid), K(ret));
  return ret;
}

int ObVectorIndexUtil::get_vector_index_column_name(
    const ObTableSchema &data_table_schema, const ObTableSchema &index_table_schema, ObIArray<ObString> &col_names)
{
  INIT_SUCC(ret);
  col_names.reset();
  if (!index_table_schema.is_vec_index()) {
    // skip none vector index
  } else if (index_table_schema.is_vec_rowkey_vid_type() || index_table_schema.is_vec_vid_rowkey_type()) {
    // skip rowkey_vid and vid_rowkey table
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_schema.get_column_count(); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(index_table_schema));
      } else if (!col_schema->is_vec_vector_column()) {
        // only need vec_vector column, here skip other column
      } else {
        // get generated column cascaded column id info
        // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
        ObArray<uint64_t> cascaded_column_ids;
        // get column_schema from data table using generate column id
        const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(ori_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ori column", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
        } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
          LOG_WARN("failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < cascaded_column_ids.count(); ++j) {
            const ObColumnSchemaV2 *cascaded_column = NULL;
            ObString new_col_name;
            if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(j)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected cascaded column", K(ret));
            } else if (OB_FALSE_IT(new_col_name = cascaded_column->get_column_name())) {
            } else if (OB_FAIL(col_names.push_back(new_col_name))) {
              LOG_WARN("fail to push back col names", K(ret), K(new_col_name));
            } else {
              LOG_DEBUG("success to get vector index col name", K(ret), K(new_col_name));
            }
          }
        }
      }
    }
  }
  return ret;
}

bool ObVectorIndexUtil::is_match_index_column_name(
    const schema::ObTableSchema &table_schema,
    const schema::ObTableSchema &index_schema,
    const ObString &index_column_name)
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  ObSEArray<ObString, 1> col_names;
  const int64_t vector_index_column_cnt = 1;
  if (OB_FAIL(get_vector_index_column_name(table_schema, index_schema, col_names))) {
    LOG_WARN("fail to get vector index column name", K(ret), K(index_schema));
  } else if (col_names.count() != vector_index_column_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index column cnt, should equal to one", K(ret), K(col_names.count()));
  } else if (ObColumnNameHashWrapper(col_names.at(0)) == ObColumnNameHashWrapper(index_column_name)) {
    is_match = true;
  }
  return is_match;
}

int ObVectorIndexUtil::get_rebuild_drop_index_id_and_name(share::schema::ObSchemaGetterGuard &schema_guard, obrpc::ObDropIndexArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const uint64_t old_index_id = arg.table_id_;
  const uint64_t new_index_id = arg.index_table_id_;
  const ObString old_index_name = arg.index_name_;
  const ObTableSchema *old_index_schema = nullptr;
  const ObTableSchema *new_index_schema = nullptr;
  if (!arg.is_add_to_scheduler_ || !arg.is_vec_inner_drop_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg", K(ret), K(arg));
  } else if (tenant_id == OB_INVALID_TENANT_ID ||
             old_index_id == OB_INVALID_ID || new_index_id == OB_INVALID_ID || old_index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(old_index_id), K(new_index_id), K(old_index_name));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, old_index_id, old_index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(old_index_id));
  } else if (OB_ISNULL(old_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, new_index_id, new_index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(new_index_id));
  } else if (OB_ISNULL(new_index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    // If the name of the old table has been changed, it means the rebuild was successful, otherwise, the rebuild failed. So:
    //    1. When the rebuild is successful, the old table needs to be deleted because the name of the old table has been replaced.
    //    2. Conversely, the new table needs to be deleted if the rebuild is unsuccessful.
    bool rebuild_succ = false;
    if (0 == old_index_schema->get_table_name_str().case_compare(old_index_name)) {
      rebuild_succ = false;
    } else if (0 == new_index_schema->get_table_name_str().case_compare(old_index_name)) {
      rebuild_succ = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rebuild old and new index table name", K(ret), K(old_index_name),
        K(old_index_schema->get_table_name()),
        K(new_index_schema->get_table_name()));
    }
    if (OB_FAIL(ret)) {
    } else if (rebuild_succ) { // drop old index
      arg.index_table_id_ = old_index_id;
      if (OB_FAIL(old_index_schema->get_index_name(arg.index_name_))) { // index name, like: idx1, not full index name
        LOG_WARN("fail to get index name", K(ret));
      }
    } else { // drop new index
      arg.index_table_id_ = new_index_id;
      if (OB_FAIL(new_index_schema->get_index_name(arg.index_name_))) { // index name, like: idx1, not full index name
        LOG_WARN("fail to get index name", K(ret));
      }
    }
    LOG_INFO("succ to get rebuild drop index id and name", K(ret),
      K(arg.index_table_id_), K(arg.index_name_),
      K(old_index_schema->get_table_name()), K(new_index_schema->get_table_name()));
  }
  return ret;
}

}
}
